/*
 * delete.c
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

#include "transaction/delete.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/xdr_serverside.h"
#include "fabric/exchange.h" // TODO - old pickle - remove in "six months"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

void start_delete_dup_res(rw_request* rw, as_transaction* tr);
void start_delete_repl_write(rw_request* rw, as_transaction* tr);
void start_delete_repl_write_forget(rw_request* rw, as_transaction* tr);
bool delete_dup_res_cb(rw_request* rw);
void delete_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
void delete_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr);
void delete_repl_write_cb(rw_request* rw);

void send_delete_response(as_transaction* tr);
void delete_timeout_cb(rw_request* rw);


//==========================================================
// Inlines & macros.
//

static inline void
client_delete_update_stats(as_namespace* ns, uint8_t result_code,
		bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_client_delete_success);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_client_delete_success);
		}
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_delete_timeout);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_client_delete_timeout);
		}
		break;
	default:
		cf_atomic64_incr(&ns->n_client_delete_error);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_client_delete_error);
		}
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_client_delete_not_found);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_client_delete_not_found);
		}
		break;
	}
}

static inline void
from_proxy_delete_update_stats(as_namespace* ns, uint8_t result_code,
		bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_from_proxy_delete_success);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_from_proxy_delete_success);
		}
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_from_proxy_delete_timeout);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_from_proxy_delete_timeout);
		}
		break;
	default:
		cf_atomic64_incr(&ns->n_from_proxy_delete_error);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_from_proxy_delete_error);
		}
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_from_proxy_delete_not_found);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_from_proxy_delete_not_found);
		}
		break;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_delete_start(as_transaction* tr)
{
	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_ERR_ALWAYS_FORBIDDEN;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	if (! validate_delete_durability(tr)) {
		tr->result_code = AS_ERR_FORBIDDEN;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	if (delete_storage_overloaded(tr)) {
		tr->result_code = AS_ERR_DEVICE_OVERLOAD;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->id, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_delete_response(tr);
		}

		return status;
	}
	// else - rw_request is now in hash, continue...

	if (tr->rsv.ns->write_dup_res_disabled) {
		// Note - preventing duplicate resolution this way allows
		// rw_request_destroy() to handle dup_msg[] cleanup correctly.
		tr->rsv.n_dupl = 0;
	}

	// If there are duplicates to resolve, start doing so.
	// TODO - should we bother if there's no generation check?
	if (tr->rsv.n_dupl != 0) {
		start_delete_dup_res(rw, tr);

		// Started duplicate resolution.
		return TRANS_IN_PROGRESS;
	}
	// else - no duplicate resolution phase, apply operation to master.

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p,
			rw->dest_nodes);

	if (insufficient_replica_destinations(tr->rsv.ns, rw->n_dest_nodes)) {
		rw_request_hash_delete(&hkey, rw);
		tr->result_code = AS_ERR_UNAVAILABLE;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	// If error, transaction is finished.
	if ((status = delete_master(tr, rw)) != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);

		if (status != TRANS_WAITING) {
			send_delete_response(tr);
		}

		return status;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(tr);
		rw_request_hash_delete(&hkey, rw);
		send_delete_response(tr);
		return TRANS_DONE_SUCCESS;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(tr)) {
		start_delete_repl_write_forget(rw, tr);
		rw_request_hash_delete(&hkey, rw);
		send_delete_response(tr);
		return TRANS_DONE_SUCCESS;
	}

	start_delete_repl_write(rw, tr);

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

void
start_delete_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, delete_dup_res_cb, delete_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


void
start_delete_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-delete message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, delete_repl_write_cb, delete_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


void
start_delete_repl_write_forget(rw_request* rw, as_transaction* tr)
{
	// Construct and send repl-write message. No need to finish rw setup.

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}


bool
delete_dup_res_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		send_delete_response(&tr);
		return true;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr.rsv.p,
			rw->dest_nodes);

	if (insufficient_replica_destinations(tr.rsv.ns, rw->n_dest_nodes)) {
		tr.result_code = AS_ERR_UNAVAILABLE;
		send_delete_response(&tr);
		return true;
	}

	transaction_status status = delete_master(&tr, rw);

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
		return true;
	}

	if (status == TRANS_DONE_ERROR) {
		send_delete_response(&tr);
		return true;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(&tr);
		send_delete_response(&tr);
		return true;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	// (Remember that nsup deletes can't get here, so no need to check.)
	if (respond_on_master_complete(&tr)) {
		delete_repl_write_forget_after_dup_res(rw, &tr);
		send_delete_response(&tr);
		return true;
	}

	delete_repl_write_after_dup_res(rw, &tr);

	// Started replica write - don't delete rw_request from hash.
	return false;
}


void
delete_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, delete_repl_write_cb);
	send_rw_messages(rw);
}


void
delete_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Send replica writes. Not waiting for acks, so need to reset rw_request.
	// Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}


void
delete_repl_write_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	finished_replicated(&tr);
	send_delete_response(&tr);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

void
send_delete_response(as_transaction* tr)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	switch (tr->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(tr->from.proto_fd_h, tr->result_code, 0, 0, NULL,
				NULL, 0, tr->rsv.ns, as_transaction_trid(tr));
		client_delete_update_stats(tr->rsv.ns, tr->result_code,
				as_transaction_is_xdr(tr));
		break;
	case FROM_PROXY:
		as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
				tr->result_code, 0, 0, NULL, NULL, 0, tr->rsv.ns,
				as_transaction_trid(tr));
		from_proxy_delete_update_stats(tr->rsv.ns, tr->result_code,
				as_transaction_is_xdr(tr));
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}


void
delete_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	finished_not_replicated(rw);

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, rw_request_trid(rw));
		client_delete_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
				as_msg_is_xdr(&rw->msgp->msg));
		break;
	case FROM_PROXY:
		from_proxy_delete_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
				as_msg_is_xdr(&rw->msgp->msg));
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - delete master.
//

transaction_status
drop_master(as_transaction* tr, as_index_ref* r_ref, rw_request* rw)
{
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_index_tree* tree = tr->rsv.tree;
	as_record* r = r_ref->r;

	// Check generation requirement, if any.
	if (! generation_check(r, m, ns)) {
		as_record_done(r_ref, ns);
		cf_atomic64_incr(&ns->n_fail_generation);
		tr->result_code = AS_ERR_GENERATION;
		return TRANS_DONE_ERROR;
	}

	bool check_key = as_transaction_has_key(tr);

	if (ns->storage_data_in_memory || check_key) {
		as_storage_rd rd;
		as_storage_record_open(ns, r, &rd);

		// Check the key if required.
		// Note - for data-not-in-memory a key check is expensive!
		if (check_key && as_storage_record_get_key(&rd) &&
				! check_msg_key(m, &rd)) {
			as_storage_record_close(&rd);
			as_record_done(r_ref, ns);
			tr->result_code = AS_ERR_KEY_MISMATCH;
			return TRANS_DONE_ERROR;
		}

		if (ns->storage_data_in_memory) {
			delete_adjust_sindex(&rd);
		}

		as_storage_record_close(&rd);
	}

	// TODO - old pickle - remove in "six months".
	if (as_exchange_min_compatibility_id() < 3 && rw->n_dest_nodes != 0) {
		// Generate a binless pickle, but don't generate pickled rec-props -
		// these are useless for a drop.
		rw->is_old_pickle = true;
		rw->pickle_sz = sizeof(uint16_t);
		rw->pickle = cf_malloc(rw->pickle_sz);
		*(uint16_t*)rw->pickle = 0;
	}

	// Save the set-ID for XDR.
	uint16_t set_id = as_index_get_set_id(r);

	as_index_delete(tree, &tr->keyd);
	as_record_done(r_ref, ns);

	if (xdr_must_ship_delete(ns, as_msg_is_xdr(m))) {
		xdr_write(ns, &tr->keyd, 0, 0, XDR_OP_TYPE_DROP, set_id, NULL);
	}

	return TRANS_IN_PROGRESS;
}
