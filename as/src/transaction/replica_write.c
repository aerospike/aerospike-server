/*
 * replica_write.c
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

#include "transaction/replica_write.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/xdr_serverside.h"
#include "fabric/exchange.h" // TODO - old pickle - remove in "six months"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "transaction/delete.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

void old_make_message(rw_request* rw, as_transaction* tr);
uint32_t pack_info_bits(as_transaction* tr);
void send_repl_write_ack(cf_node node, msg* m, uint32_t result);
void send_repl_write_ack_w_digest(cf_node node, msg* m, uint32_t result,
		const cf_digest* keyd);
uint32_t parse_result_code(msg* m);
void drop_replica(as_partition_reservation* rsv, cf_digest* keyd,
		bool is_xdr_op, cf_node master);


//==========================================================
// Public API.
//

void
repl_write_make_message(rw_request* rw, as_transaction* tr)
{
	if (rw->dest_msg) {
		as_fabric_msg_put(rw->dest_msg);
	}

	rw->dest_msg = as_fabric_msg_get(M_TYPE_RW);

	// TODO - old pickle - remove in "six months".
	if (rw->is_old_pickle) {
		old_make_message(rw, tr);
		return;
	}

	as_namespace* ns = tr->rsv.ns;
	msg* m = rw->dest_msg;

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_ID, ns->id);
	msg_set_uint32(m, RW_FIELD_TID, rw->tid);

	if (rw->pickle != NULL) {
		msg_set_buf(m, RW_FIELD_RECORD, rw->pickle, rw->pickle_sz,
				MSG_SET_HANDOFF_MALLOC);
		rw->pickle = NULL; // make sure destructor doesn't free this
	}
	else { // drop
		msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
				MSG_SET_COPY);
	}

	uint32_t info = pack_info_bits(tr);

	if (info != 0) {
		msg_set_uint32(m, RW_FIELD_INFO, info);
	}
}


void
repl_write_setup_rw(rw_request* rw, as_transaction* tr,
		repl_write_done_cb repl_write_cb, timeout_done_cb timeout_cb)
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
	// Hereafter, rw_request must release reservation - happens in destructor.

	rw->end_time = tr->end_time;
	rw->flags = tr->flags;
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;
	rw->last_update_time = tr->last_update_time;

	rw->repl_write_cb = repl_write_cb;
	rw->timeout_cb = timeout_cb;

	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}

	// Allow retransmit thread to destroy rw_request as soon as we unlock.
	rw->is_set_up = true;

	if (as_health_sample_replica_write()) {
		rw->repl_start_us = cf_getus();
	}
}


void
repl_write_reset_rw(rw_request* rw, as_transaction* tr, repl_write_done_cb cb)
{
	// Reset rw->from.any which was set null in tr setup.
	rw->from.any = tr->from.any;

	// Needed for response to origin.
	rw->flags = tr->flags;
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;
	rw->last_update_time = tr->last_update_time;

	rw->repl_write_cb = cb;

	// TODO - is this better than not resetting? Note - xmit_ms not volatile.
	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}

	if (as_health_sample_replica_write()) {
		rw->repl_start_us = cf_getus();
	}
}


void
repl_write_handle_op(cf_node node, msg* m)
{
	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no namespace");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_RW, "repl_write_handle_op: invalid namespace");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	cf_digest* keyd;
	size_t keyd_size;

	// Handle drops.
	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, &keyd_size,
			MSG_GET_DIRECT) == 0) {
		if (keyd_size != CF_DIGEST_KEY_SZ) {
			cf_warning(AS_RW, "repl_write_handle_op: invalid digest");
			send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
			return;
		}

		as_partition_reservation rsv;
		uint32_t result = as_partition_reserve_replica(ns,
				as_partition_getid(keyd), &rsv);

		if (result == AS_OK) {
			drop_replica(&rsv, keyd, (info & RW_INFO_XDR) != 0, node);
			as_partition_release(&rsv);
		}

		send_repl_write_ack(node, m, result);

		return;
	}
	// else - flat record, including tombstone.

	as_remote_record rr = { .src = node };

	if (msg_get_buf(m, RW_FIELD_RECORD, &rr.pickle, &rr.pickle_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no record");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	if (! as_flat_unpack_remote_record_meta(ns, &rr)) {
		cf_warning(AS_RW, "repl_write_handle_op: bad record");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	as_partition_reservation rsv;
	uint32_t result = as_partition_reserve_replica(ns,
			as_partition_getid(rr.keyd), &rsv);

	if (result != AS_OK) {
		send_repl_write_ack_w_digest(node, m, result, rr.keyd);
		return;
	}

	rr.rsv = &rsv;

	// Do XDR write if the write is a non-XDR write or forwarding is enabled.
	bool do_xdr_write = (info & RW_INFO_XDR) == 0 ||
			is_xdr_forwarding_enabled() || ns->ns_forward_xdr_writes;

	// If source didn't touch sindex, may not need to touch it locally.
	bool skip_sindex = (info & RW_INFO_SINDEX_TOUCHED) == 0;

	result = (uint32_t)as_record_replace_if_better(&rr, true, skip_sindex,
			do_xdr_write);

	as_partition_release(&rsv);
	send_repl_write_ack_w_digest(node, m, result, rr.keyd);
}


// TODO - old pickle - remove in "six months".
void
repl_write_handle_old_op(cf_node node, msg* m)
{
	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no namespace");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_RW, "repl_write_handle_op: invalid namespace");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no digest");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	as_partition_reservation rsv;
	uint32_t result = as_partition_reserve_replica(ns, as_partition_getid(keyd),
			&rsv);

	if (result != AS_OK) {
		send_repl_write_ack(node, m, result);
		return;
	}

	as_remote_record rr = {
			.src = node,
			.rsv = &rsv,
			.keyd = keyd,
			.is_old_pickle = true
	};

	if (msg_get_buf(m, RW_FIELD_OLD_RECORD, &rr.pickle, &rr.pickle_sz,
			MSG_GET_DIRECT) != 0 || rr.pickle_sz < 2) {
		cf_warning(AS_RW, "repl_write_handle_op: no or bad record");
		as_partition_release(&rsv);
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if (repl_write_pickle_is_drop(rr.pickle, info)) {
		drop_replica(&rsv, keyd, (info & RW_INFO_XDR) != 0, node);

		as_partition_release(&rsv);
		send_repl_write_ack(node, m, AS_OK);

		return;
	}

	if (msg_get_uint32(m, RW_FIELD_GENERATION, &rr.generation) != 0 ||
			rr.generation == 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no or bad generation");
		as_partition_release(&rsv);
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	if (msg_get_uint64(m, RW_FIELD_LAST_UPDATE_TIME,
			&rr.last_update_time) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no last-update-time");
		as_partition_release(&rsv);
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	msg_get_uint32(m, RW_FIELD_VOID_TIME, &rr.void_time);

	msg_get_buf(m, RW_FIELD_SET_NAME, (uint8_t **)&rr.set_name,
			&rr.set_name_len, MSG_GET_DIRECT);

	msg_get_buf(m, RW_FIELD_KEY, (uint8_t **)&rr.key, &rr.key_size,
			MSG_GET_DIRECT);

	// Do XDR write if the write is a non-XDR write or forwarding is enabled.
	bool do_xdr_write = (info & RW_INFO_XDR) == 0 ||
			is_xdr_forwarding_enabled() || ns->ns_forward_xdr_writes;

	// If source didn't touch sindex, may not need to touch it locally.
	bool skip_sindex = (info & RW_INFO_SINDEX_TOUCHED) == 0;

	result = (uint32_t)as_record_replace_if_better(&rr, true, skip_sindex,
			do_xdr_write);

	as_partition_release(&rsv);
	send_repl_write_ack(node, m, result);
}


void
repl_write_handle_ack(cf_node node, msg* m)
{
	uint32_t ns_id;

	if (msg_get_uint32(m, RW_FIELD_NS_ID, &ns_id) != 0) {
		cf_warning(AS_RW, "repl-write ack: no ns-id");
		as_fabric_msg_put(m);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl-write ack: no digest");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t tid;

	if (msg_get_uint32(m, RW_FIELD_TID, &tid) != 0) {
		cf_warning(AS_RW, "repl-write ack: no tid");
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

	if (rw->tid != tid || rw->repl_write_complete) {
		// Extra ack - rw_request is newer transaction for same digest, or ack
		// is arriving after rw_request was aborted.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (! rw->from.any) {
		// Lost race against timeout in retransmit thread.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	// Find remote node in replicas list.
	int i = index_of_node(rw->dest_nodes, rw->n_dest_nodes, node);

	if (i == -1) {
		cf_warning(AS_RW, "repl-write ack: from non-dest node %lx", node);
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (rw->dest_complete[i]) {
		// Extra ack for this replica write.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	uint32_t result_code = parse_result_code(m);

	// If it makes sense, retransmit replicas. Note - rw->dest_complete[i] not
	// yet set true, so that retransmit will go to this remote node.
	if (repl_write_should_retransmit_replicas(rw, result_code)) {
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	rw->dest_complete[i] = true;

	as_health_add_ns_latency(node, ns_id, AS_HEALTH_NS_REPL_LAT,
			rw->repl_start_us);

	for (uint32_t j = 0; j < rw->n_dest_nodes; j++) {
		if (! rw->dest_complete[j]) {
			// Still haven't heard from all replicas.
			cf_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			as_fabric_msg_put(m);
			return;
		}
	}

	// Success for all replicas.
	rw->repl_write_cb(rw);
	repl_write_send_confirmation(rw);

	rw->repl_write_complete = true;

	cf_mutex_unlock(&rw->lock);
	rw_request_hash_delete(&hkey, rw);
	rw_request_release(rw);
	as_fabric_msg_put(m);
}


//==========================================================
// Local helpers.
//

// TODO - old pickle - remove in "six months".
void
old_make_message(rw_request* rw, as_transaction* tr)
{
	// TODO - remove this when we're comfortable:
	cf_assert(rw->pickle, AS_RW, "making repl-write msg with null pickle");

	as_namespace* ns = tr->rsv.ns;
	msg* m = rw->dest_msg;

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_ID, ns->id);
	msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_TID, rw->tid);
	msg_set_uint32(m, RW_FIELD_GENERATION, tr->generation);
	msg_set_uint64(m, RW_FIELD_LAST_UPDATE_TIME, tr->last_update_time);

	if (tr->void_time != 0) {
		msg_set_uint32(m, RW_FIELD_VOID_TIME, tr->void_time);
	}

	uint32_t info = pack_info_bits(tr);

	repl_write_flag_pickle(tr, rw->pickle, &info);

	msg_set_buf(m, RW_FIELD_OLD_RECORD, rw->pickle, rw->pickle_sz,
			MSG_SET_HANDOFF_MALLOC);
	rw->pickle = NULL; // make sure destructor doesn't free this

	if (rw->set_name) {
		msg_set_buf(m, RW_FIELD_SET_NAME, (const uint8_t*)rw->set_name,
				rw->set_name_len, MSG_SET_COPY);
		// rw->set_name points directly into vmap - never free it.
	}

	if (rw->key) {
		msg_set_buf(m, RW_FIELD_KEY, rw->key, rw->key_size,
				MSG_SET_HANDOFF_MALLOC);
		rw->key = NULL; // make sure destructor doesn't free this
	}

	if (info != 0) {
		msg_set_uint32(m, RW_FIELD_INFO, info);
	}
}


uint32_t
pack_info_bits(as_transaction* tr)
{
	uint32_t info = 0;

	if (as_transaction_is_xdr(tr)) {
		info |= RW_INFO_XDR;
	}

	if ((tr->flags & AS_TRANSACTION_FLAG_SINDEX_TOUCHED) != 0) {
		info |= RW_INFO_SINDEX_TOUCHED;
	}

	if (respond_on_master_complete(tr)) {
		info |= RW_INFO_NO_REPL_ACK;
	}

	return info;
}


void
send_repl_write_ack(cf_node node, msg* m, uint32_t result)
{
	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if ((info & RW_INFO_NO_REPL_ACK) != 0) {
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 3, RW_FIELD_NS_ID, RW_FIELD_DIGEST, RW_FIELD_TID);

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


void
send_repl_write_ack_w_digest(cf_node node, msg* m, uint32_t result,
		const cf_digest* keyd)
{
	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if ((info & RW_INFO_NO_REPL_ACK) != 0) {
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 2, RW_FIELD_NS_ID, RW_FIELD_TID);

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);
	msg_set_buf(m, RW_FIELD_DIGEST, (const uint8_t*)keyd, sizeof(cf_digest),
			MSG_SET_COPY);

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


uint32_t
parse_result_code(msg* m)
{
	uint32_t result_code;

	if (msg_get_uint32(m, RW_FIELD_RESULT, &result_code) != 0) {
		cf_warning(AS_RW, "repl-write ack: no result_code");
		return AS_ERR_UNKNOWN;
	}

	return result_code;
}


void
drop_replica(as_partition_reservation* rsv, cf_digest* keyd, bool is_xdr_op,
		cf_node master)
{
	// Shortcut pointers & flags.
	as_namespace* ns = rsv->ns;
	as_index_tree* tree = rsv->tree;

	as_index_ref r_ref;

	if (as_record_get(tree, keyd, &r_ref) != 0) {
		return; // not found is ok from master's perspective.
	}

	as_record* r = r_ref.r;

	if (ns->storage_data_in_memory) {
		record_delete_adjust_sindex(r, ns);
	}

	// Save the set-ID for XDR.
	uint16_t set_id = as_index_get_set_id(r);

	as_index_delete(tree, keyd);
	as_record_done(&r_ref, ns);

	if (xdr_must_ship_delete(ns, is_xdr_op)) {
		xdr_write(ns, keyd, 0, master, XDR_OP_TYPE_DROP, set_id, NULL);
	}
}
