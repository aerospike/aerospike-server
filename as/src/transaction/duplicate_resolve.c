/*
 * duplicate_resolve.c
 *
 * Copyright (C) 2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#include "transaction/duplicate_resolve.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

int fill_ack_w_pickle(as_storage_rd* rd, msg* m);
int old_fill_ack_w_pickle(as_storage_rd* rd, msg* m);
void done_handle_request(as_partition_reservation* rsv, as_index_ref* r_ref, as_storage_rd* rd);
void send_dup_res_ack(cf_node node, msg* m, uint32_t result, uint32_t info);
void send_dup_res_ack_preserved(cf_node node, msg* m, uint32_t result, uint32_t info);
uint32_t parse_dup_meta(msg* m, uint32_t* p_generation, uint64_t* p_last_update_time);
uint32_t old_parse_conflict_meta(msg* m, uint32_t* generation, uint64_t* lut);
void apply_winner(rw_request* rw);
bool old_parse_winner(rw_request* rw, uint32_t info, as_remote_record* rr);


//==========================================================
// Public API.
//

void
dup_res_make_message(rw_request* rw, as_transaction* tr)
{
	rw->dest_msg = as_fabric_msg_get(M_TYPE_RW);

	as_namespace* ns = tr->rsv.ns;
	msg* m = rw->dest_msg;

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_DUP);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_ID, ns->id);
	msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_TID, rw->tid);

	// TODO - JUMP - send this only because versions up to 3.14.x require it.
	msg_set_uint64(m, RW_FIELD_CLUSTER_KEY, as_exchange_cluster_key());

	as_index_ref r_ref;

	if (as_record_get(tr->rsv.tree, &tr->keyd, &r_ref) == 0) {
		as_record* r = r_ref.r;

		msg_set_uint32(m, RW_FIELD_GENERATION, r->generation);
		msg_set_uint64(m, RW_FIELD_LAST_UPDATE_TIME, r->last_update_time);

		as_record_done(&r_ref, ns);
	}
}


void
dup_res_setup_rw(rw_request* rw, as_transaction* tr, dup_res_done_cb dup_res_cb,
		timeout_done_cb timeout_cb)
{
	rw->msgp = tr->msgp;
	tr->msgp = NULL;

	rw->msg_fields = tr->msg_fields;
	rw->origin = tr->origin;
	rw->from_flags = tr->from_flags;

	rw->from.any = tr->from.any;
	rw->from_data.any = tr->from_data.any;
	tr->from.any = NULL;

	rw->start_time = tr->start_time;
	rw->benchmark_time = tr->benchmark_time;

	as_partition_reservation_copy(&rw->rsv, &tr->rsv);
	// Hereafter, rw must release the reservation - happens in destructor.

	rw->end_time = tr->end_time;
	// Note - don't need as_transaction's other 'container' members.

	rw->dup_res_cb = dup_res_cb;
	rw->timeout_cb = timeout_cb;

	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	rw->n_dest_nodes = tr->rsv.n_dupl;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
		rw->dest_nodes[i] = tr->rsv.dupl_nodes[i];
	}

	// Allow retransmit thread to destroy rw as soon as we unlock.
	rw->is_set_up = true;
}


void
dup_res_handle_request(cf_node node, msg* m)
{
	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "dup-res handler: no digest");
		send_dup_res_ack(node, m, AS_ERR_UNKNOWN, 0);
		return;
	}

	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "dup-res handler: no namespace");
		send_dup_res_ack(node, m, AS_ERR_UNKNOWN, 0);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_RW, "dup-res handler: invalid namespace");
		send_dup_res_ack(node, m, AS_ERR_UNKNOWN, 0);
		return;
	}

	uint32_t generation = 0;
	uint64_t last_update_time = 0;

	bool local_conflict_check =
			msg_get_uint32(m, RW_FIELD_GENERATION, &generation) == 0 &&
			msg_get_uint64(m, RW_FIELD_LAST_UPDATE_TIME,
					&last_update_time) == 0;

	as_partition_reservation rsv;

	as_partition_reserve(ns, as_partition_getid(keyd), &rsv);

	as_index_ref r_ref;

	if (as_record_get(rsv.tree, keyd, &r_ref) != 0) {
		done_handle_request(&rsv, NULL, NULL);
		send_dup_res_ack(node, m, AS_ERR_NOT_FOUND, 0);
		return;
	}

	as_record* r = r_ref.r;

	int result;

	if ((result = as_partition_check_source(ns, rsv.p, node, NULL)) != AS_OK) {
		done_handle_request(&rsv, &r_ref, NULL);
		send_dup_res_ack(node, m, result, 0);
		return;
	}

	if (local_conflict_check &&
			(result = as_record_resolve_conflict(ns->conflict_resolution_policy,
					generation, last_update_time, r->generation,
					r->last_update_time)) <= 0) {
		uint32_t info = dup_res_pack_repl_state_info(r, ns);

		done_handle_request(&rsv, &r_ref, NULL);
		send_dup_res_ack(node, m,
				result == 0 ? AS_ERR_RECORD_EXISTS : AS_ERR_GENERATION, info);
		return;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	// TODO - old pickle - remove old method in "six months".
	result = as_exchange_min_compatibility_id() >= 3 ?
			fill_ack_w_pickle(&rd, m) : old_fill_ack_w_pickle(&rd, m);

	if (result < 0) {
		done_handle_request(&rsv, &r_ref, &rd);
		send_dup_res_ack(node, m, (uint32_t)-result, 0);
		return;
	}

	uint32_t info = dup_res_pack_info(r, ns);

	done_handle_request(&rsv, &r_ref, &rd);
	send_dup_res_ack_preserved(node, m, AS_OK, info);
}


void
dup_res_handle_ack(cf_node node, msg* m)
{
	uint32_t ns_id;

	if (msg_get_uint32(m, RW_FIELD_NS_ID, &ns_id) != 0) {
		cf_warning(AS_RW, "dup-res ack: no ns-id");
		as_fabric_msg_put(m);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		uint8_t* pickle;
		size_t pickle_sz;

		if (msg_get_buf(m, RW_FIELD_RECORD, &pickle, &pickle_sz,
				MSG_GET_DIRECT) != 0 ||
						pickle_sz < sizeof(as_flat_record)) {
			cf_warning(AS_RW, "dup-res ack: no or bad digest");
			as_fabric_msg_put(m);
			return;
		}

		keyd = &((as_flat_record*)pickle)->keyd;
	}

	uint32_t tid;

	if (msg_get_uint32(m, RW_FIELD_TID, &tid) != 0) {
		cf_warning(AS_RW, "dup-res ack: no tid");
		as_fabric_msg_put(m);
		return;
	}

	rw_request_hkey hkey = { ns_id, *keyd };
	rw_request* rw = rw_request_hash_get(&hkey);

	if (! rw) {
		// Extra ack, after rw_request is already gone.
		as_fabric_msg_put(m);
		return;
	}

	cf_mutex_lock(&rw->lock);

	if (rw->tid != tid || rw->dup_res_complete) {
		// Extra ack - rw_request is newer transaction for same digest, or ack
		// is arriving after rw_request was aborted or finished dup-res.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	// Find remote node in duplicates list.
	int i = index_of_node(rw->dest_nodes, rw->n_dest_nodes, node);

	if (i == -1) {
		cf_warning(AS_RW, "dup-res ack: from non-dest node %lx", node);
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (rw->dest_complete[i]) {
		// Extra ack for this duplicate.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	rw->dest_complete[i] = true;

	uint32_t generation = 0;
	uint64_t last_update_time = 0;
	uint32_t result_code = parse_dup_meta(m, &generation, &last_update_time);

	// If it makes sense, retry transaction from the beginning.
	// TODO - is this retry too fast? Should there be a throttle? If so, how?
	if (dup_res_should_retry_transaction(rw, result_code)) {
		if (! rw->from.any) {
			// Lost race against timeout in retransmit thread.
			cf_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			as_fabric_msg_put(m);
			return;
		}

		as_transaction tr;
		as_transaction_init_head_from_rw(&tr, rw);

		// Note that tr now owns msgp - make sure rw destructor doesn't free it.
		// Note also that rw will release rsv - tr will get a new one.
		rw->msgp = NULL;

		tr.from_flags |= FROM_FLAG_RESTART;
		as_tsvc_enqueue(&tr);

		rw->dup_res_complete = true;

		cf_mutex_unlock(&rw->lock);
		rw_request_hash_delete(&hkey, rw);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	dup_res_handle_tie(rw, m, result_code);

	// Compare this duplicate with previous best, if any.
	bool keep_previous_best = rw->best_dup_msg &&
			as_record_resolve_conflict(rw->rsv.ns->conflict_resolution_policy,
					rw->best_dup_gen, rw->best_dup_lut,
					(uint16_t)generation, last_update_time) <= 0;

	if (keep_previous_best) {
		// This duplicate is no better than previous best - keep previous best.
		as_fabric_msg_put(m);
	}
	else {
		// No previous best, or this duplicate is better - keep this one.
		if (rw->best_dup_msg) {
			as_fabric_msg_put(rw->best_dup_msg);
		}

		msg_preserve_all_fields(m);
		rw->best_dup_msg = m;
		rw->best_dup_result_code = (uint8_t)result_code;
		rw->best_dup_gen = generation;
		rw->best_dup_lut = last_update_time;
	}

	// Saved or discarded m - from here down don't call as_fabric_msg_put(m)!

	for (uint32_t j = 0; j < rw->n_dest_nodes; j++) {
		if (! rw->dest_complete[j]) {
			// Still haven't heard from all duplicates.
			cf_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			return;
		}
	}

	if (rw->best_dup_result_code == AS_OK) {
		apply_winner(rw); // sets rw->result_code to pass along to callback
	}
	else {
		apply_if_tie(rw);
	}

	// Check for lost race against timeout in retransmit thread *after* applying
	// winner - may save a future transaction from re-fetching the duplicates.
	// Note - nsup deletes don't get here, so check using rw->from.any is ok.
	if (! rw->from.any) {
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		return;
	}

	dup_res_translate_result_code(rw);

	bool delete_from_hash = rw->dup_res_cb(rw);

	rw->dup_res_complete = true;

	cf_mutex_unlock(&rw->lock);

	if (delete_from_hash) {
		rw_request_hash_delete(&hkey, rw);
	}

	rw_request_release(rw);
}


//==========================================================
// Local helpers.
//

int
fill_ack_w_pickle(as_storage_rd* rd, msg* m)
{
	if (! as_storage_record_get_pickle(rd)) {
		return -AS_ERR_UNKNOWN;
	}

	msg_preserve_fields(m, 2, RW_FIELD_NS_ID, RW_FIELD_TID);

	// Can't fail from here on - ok to add message fields.

	msg_set_buf(m, RW_FIELD_RECORD, rd->pickle, rd->pickle_sz,
			MSG_SET_HANDOFF_MALLOC);

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
int
old_fill_ack_w_pickle(as_storage_rd* rd, msg* m)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	int result = as_storage_rd_load_n_bins(rd);

	if (result < 0) {
		return result;
	}

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd->n_bins];

	if ((result = as_storage_rd_load_bins(rd, stack_bins)) < 0) {
		return result;
	}

	if (! as_storage_record_get_key(rd)) {
		return -AS_ERR_UNKNOWN;
	}

	msg_preserve_fields(m, 3, RW_FIELD_NS_ID, RW_FIELD_DIGEST, RW_FIELD_TID);

	// Can't fail from here on - ok to add message fields.

	msg_set_uint32(m, RW_FIELD_GENERATION, r->generation);
	msg_set_uint64(m, RW_FIELD_LAST_UPDATE_TIME, r->last_update_time);

	if (r->void_time != 0) {
		msg_set_uint32(m, RW_FIELD_VOID_TIME, r->void_time);
	}

	const char* set_name = as_index_get_set_name(r, ns);

	if (set_name) {
		msg_set_buf(m, RW_FIELD_SET_NAME, (const uint8_t*)set_name,
				strlen(set_name), MSG_SET_COPY);
	}

	if (rd->key) {
		msg_set_buf(m, RW_FIELD_KEY, rd->key, rd->key_size, MSG_SET_COPY);
	}

	size_t buf_len;
	uint8_t* buf = as_record_pickle(rd, &buf_len);

	msg_set_buf(m, RW_FIELD_OLD_RECORD, buf, buf_len, MSG_SET_HANDOFF_MALLOC);

	return AS_OK;
}


void
done_handle_request(as_partition_reservation* rsv, as_index_ref* r_ref,
		as_storage_rd* rd)
{
	if (rd) {
		as_storage_record_close(rd);
	}

	if (r_ref) {
		as_record_done(r_ref, rsv->ns);
	}

	if (rsv) {
		as_partition_release(rsv);
	}
}


void
send_dup_res_ack(cf_node node, msg* m, uint32_t result, uint32_t info)
{
	msg_preserve_fields(m, 3, RW_FIELD_NS_ID, RW_FIELD_DIGEST, RW_FIELD_TID);

	send_dup_res_ack_preserved(node, m, result, info);
}


void
send_dup_res_ack_preserved(cf_node node, msg* m, uint32_t result, uint32_t info)
{
	msg_set_uint32(m, RW_FIELD_OP, RW_OP_DUP_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);

	if (info != 0) {
		msg_set_uint32(m, RW_FIELD_INFO, info);
	}

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


uint32_t
parse_dup_meta(msg* m, uint32_t* p_generation, uint64_t* p_last_update_time)
{
	uint32_t result_code;

	if (msg_get_uint32(m, RW_FIELD_RESULT, &result_code) != 0) {
		cf_warning(AS_RW, "dup-res ack: no result_code");
		return AS_ERR_UNKNOWN;
	}

	if (result_code != AS_OK) {
		return result_code;
	}

	uint8_t* pickle;
	size_t pickle_sz;

	if (msg_get_buf(m, RW_FIELD_RECORD, &pickle, &pickle_sz,
			MSG_GET_DIRECT) != 0) {
		// TODO - old pickle - remove in "six months".
		return old_parse_conflict_meta(m, p_generation, p_last_update_time);
	}

	*p_generation = ((as_flat_record*)pickle)->generation;

	if (*p_generation == 0) {
		cf_warning(AS_RW, "dup-res ack: generation 0");
		return AS_ERR_UNKNOWN;
	}

	*p_last_update_time = ((as_flat_record*)pickle)->last_update_time;

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
uint32_t
old_parse_conflict_meta(msg* m, uint32_t* generation, uint64_t* lut)
{
	// TODO - old pickle - remove in "six months".
	if (msg_get_uint32(m, RW_FIELD_GENERATION, generation) != 0 ||
			*generation == 0) {
		cf_warning(AS_RW, "dup-res ack: no or bad generation");
		return AS_ERR_UNKNOWN;
	}

	if (msg_get_uint64(m, RW_FIELD_LAST_UPDATE_TIME, lut) != 0) {
		cf_warning(AS_RW, "dup-res ack: no last-update-time");
		return AS_ERR_UNKNOWN;
	}

	return AS_OK;
}


void
apply_winner(rw_request* rw)
{
	msg* m = rw->best_dup_msg;

	as_remote_record rr = {
			// Skipping .src for now.
			.rsv = &rw->rsv,
			.keyd = &rw->keyd
	};

	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if (msg_get_buf(m, RW_FIELD_RECORD, &rr.pickle, &rr.pickle_sz,
			MSG_GET_DIRECT) != 0) {
		// TODO - old pickle - remove in "six months".
		if (! old_parse_winner(rw, info, &rr)) {
			rw->result_code = AS_ERR_UNKNOWN;
			return;
		}
	}
	else if (! as_flat_unpack_remote_record_meta(rr.rsv->ns, &rr)) {
		cf_warning_digest(AS_RW, &rw->keyd, "dup-res ack: bad record ");
		rw->result_code = AS_ERR_UNKNOWN;
		return;
	}

	dup_res_init_repl_state(&rr, info);

	rw->result_code = (uint8_t)as_record_replace_if_better(&rr, false, false,
			false);

	// Duplicate resolution just treats these errors as successful no-ops:
	if (rw->result_code == AS_ERR_RECORD_EXISTS ||
			rw->result_code == AS_ERR_GENERATION) {
		rw->result_code = AS_OK;
	}
}


// TODO - old pickle - remove in "six months".
bool
old_parse_winner(rw_request* rw, uint32_t info, as_remote_record* rr)
{
	rr->generation = rw->best_dup_gen;
	rr->last_update_time = rw->best_dup_lut;
	rr->is_old_pickle = true;

	msg* m = rw->best_dup_msg;

	if (msg_get_buf(m, RW_FIELD_OLD_RECORD, &rr->pickle, &rr->pickle_sz,
			MSG_GET_DIRECT) != 0 || rr->pickle_sz < 2) {
		cf_warning_digest(AS_RW, &rw->keyd, "dup-res ack: no or bad record ");
		return false;
	}

	if (dup_res_ignore_pickle(rr->pickle, info)) {
		cf_warning_digest(AS_RW, &rw->keyd, "dup-res ack: binless pickle ");
		return false;
	}

	msg_get_uint32(m, RW_FIELD_VOID_TIME, &rr->void_time);

	msg_get_buf(m, RW_FIELD_SET_NAME, (uint8_t **)&rr->set_name,
			&rr->set_name_len, MSG_GET_DIRECT);

	msg_get_buf(m, RW_FIELD_KEY, (uint8_t **)&rr->key, &rr->key_size,
			MSG_GET_DIRECT);

	return true;
}
