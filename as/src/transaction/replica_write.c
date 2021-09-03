/*
 * replica_write.c
 *
 * Copyright (C) 2016-2021 Aerospike, Inc.
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
#include "log.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "sindex/secondary_index.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

uint32_t pack_info_bits(as_transaction* tr);
void send_repl_write_ack(cf_node node, msg* m, uint32_t result);
void send_repl_write_ack_w_digest(cf_node node, msg* m, uint32_t result,
		const cf_digest* keyd);
uint32_t parse_result_code(msg* m);
void drop_replica(as_partition_reservation* rsv, cf_digest* keyd);


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

	as_namespace* ns = tr->rsv.ns;
	msg* m = rw->dest_msg;

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_IX, ns->ix);
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
repl_write_reset_replicas(rw_request* rw)
{
	cf_node nodes[AS_CLUSTER_SZ];
	uint32_t n_nodes = as_partition_get_other_replicas(rw->rsv.p, nodes);

	if (n_nodes == rw->n_dest_nodes &&
			memcmp(nodes, rw->dest_nodes, n_nodes * sizeof(cf_node)) == 0) {
		return; // almost always - replica destinations unchanged
	}

	if (! sufficient_replica_destinations(rw->rsv.ns, n_nodes)) {
		return; // don't change destinations - time out if not enough replicas
	}
	// else - use new replica destinations. Note - could be same nodes just
	// reordered, but not worth detecting this.

	// Initialize or preserve completion status.

	bool complete[n_nodes];

	for (uint32_t n = 0; n < n_nodes; n++) {
		complete[n] = false;

		for (uint32_t old_n = 0; old_n < rw->n_dest_nodes; old_n++) {
			if (nodes[n] == rw->dest_nodes[old_n]) {
				complete[n] = rw->dest_complete[old_n];
				break;
			}
		}
	}

	// Install the new list of replica destinations.

	rw->n_dest_nodes = n_nodes;

	for (uint32_t n = 0; n < n_nodes; n++) {
		rw->dest_nodes[n] = nodes[n];
		rw->dest_complete[n] = complete[n];
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
			drop_replica(&rsv, keyd);
			as_partition_release(&rsv);
		}

		send_repl_write_ack(node, m, result);

		return;
	}
	// else - flat record, including tombstone.

	as_remote_record rr = { .via = VIA_REPLICATION, .src = node };

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

	// Replica writes are the last thing cut off when storage is backed up.
	if (as_storage_overloaded(ns, 192, "replica write")) {
		send_repl_write_ack_w_digest(node, m, AS_ERR_DEVICE_OVERLOAD, rr.keyd);
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

	result = (uint32_t)as_record_replace_if_better(&rr);

	as_partition_release(&rsv);
	send_repl_write_ack_w_digest(node, m, result, rr.keyd);
}


void
repl_write_handle_ack(cf_node node, msg* m)
{
	uint32_t ns_ix;

	if (msg_get_uint32(m, RW_FIELD_NS_IX, &ns_ix) != 0) {
		cf_warning(AS_RW, "repl-write ack: no ns-ix");
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

	rw_request_hkey hkey = { ns_ix, *keyd };
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
		cf_detail(AS_RW, "repl-write ack: from non-dest node %lx", node);
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

	as_health_add_ns_latency(node, ns_ix, AS_HEALTH_NS_REPL_LAT,
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

uint32_t
pack_info_bits(as_transaction* tr)
{
	uint32_t info = 0;

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

	msg_preserve_fields(m, 3, RW_FIELD_NS_IX, RW_FIELD_DIGEST, RW_FIELD_TID);

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

	msg_preserve_fields(m, 2, RW_FIELD_NS_IX, RW_FIELD_TID);

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
drop_replica(as_partition_reservation* rsv, cf_digest* keyd)
{
	// Shortcut pointers & flags.
	as_namespace* ns = rsv->ns;
	as_index_tree* tree = rsv->tree;

	as_index_ref r_ref;

	if (as_record_get(tree, keyd, &r_ref) != 0) {
		return; // not found is ok from master's perspective.
	}

	if (ns->storage_data_in_memory) {
		remove_from_sindex(ns, &r_ref);
	}

	// Note - may find a tombstone here if replica missed a generation.
	as_set_index_delete_live(ns, tree, r_ref.r, r_ref.r_h);
	as_index_delete(tree, keyd);
	as_record_done(&r_ref, ns);
}
