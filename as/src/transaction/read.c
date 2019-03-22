/*
 * read.c
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

#include "transaction/read.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_ping.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

void start_read_dup_res(rw_request* rw, as_transaction* tr);
void start_repl_ping(rw_request* rw, as_transaction* tr);
bool read_dup_res_cb(rw_request* rw);
void repl_ping_after_dup_res(rw_request* rw, as_transaction* tr);
void repl_ping_cb(rw_request* rw);

void send_read_response(as_transaction* tr, as_msg_op** ops,
		as_bin** response_bins, uint16_t n_bins, cf_dyn_buf* db);
void read_timeout_cb(rw_request* rw);

transaction_status read_local(as_transaction* tr);
void read_local_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		int result_code);


//==========================================================
// Inlines & macros.
//

static inline bool
read_must_duplicate_resolve(const as_transaction* tr)
{
	return tr->rsv.n_dupl != 0 &&
			TR_READ_CONSISTENCY_LEVEL(tr) == AS_READ_CONSISTENCY_LEVEL_ALL;
}

static inline bool
read_must_ping(const as_transaction *tr)
{
	return (tr->flags & AS_TRANSACTION_FLAG_MUST_PING) != 0;
}

static inline void
client_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_client_read_success);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_read_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_read_error);
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_client_read_not_found);
		break;
	}
}

static inline void
from_proxy_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_from_proxy_read_success);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_from_proxy_read_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_from_proxy_read_error);
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_from_proxy_read_not_found);
		break;
	}
}

static inline void
batch_sub_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_batch_sub_read_success);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_batch_sub_read_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_batch_sub_read_error);
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_batch_sub_read_not_found);
		break;
	}
}

static inline void
from_proxy_batch_sub_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_from_proxy_batch_sub_read_success);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_from_proxy_batch_sub_read_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_from_proxy_batch_sub_read_error);
		break;
	case AS_ERR_NOT_FOUND:
		cf_atomic64_incr(&ns->n_from_proxy_batch_sub_read_not_found);
		break;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_read_start(as_transaction* tr)
{
	BENCHMARK_START(tr, read, FROM_CLIENT);
	BENCHMARK_START(tr, batch_sub, FROM_BATCH);

	if (! repl_ping_check(tr)) {
		send_read_response(tr, NULL, NULL, 0, NULL);
		return TRANS_DONE_ERROR;
	}

	transaction_status status;
	bool must_duplicate_resolve = read_must_duplicate_resolve(tr);
	bool must_ping = read_must_ping(tr);

	if (! must_duplicate_resolve && ! must_ping) {
		// No network hops needed, try reading.
		if ((status = read_local(tr)) != TRANS_IN_PROGRESS) {
			return status;
		}
		// else - must try again under hash.
	}
	// else - there are duplicates, and we're configured to resolve them, or
	// we're required to ping replicas.

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->id, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);

	// If rw_request isn't inserted in hash, transaction is finished.
	if ((status = rw_request_hash_insert(&hkey, rw, tr)) != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_read_response(tr, NULL, NULL, 0, NULL);
		}

		return status;
	}
	// else - rw_request is now in hash, continue...

	if (must_duplicate_resolve) {
		start_read_dup_res(rw, tr);

		// Started duplicate resolution.
		return TRANS_IN_PROGRESS;
	}

	if (must_ping) {
		// Set up the nodes to which we'll ping.
		rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p,
				rw->dest_nodes);

		if (insufficient_replica_destinations(tr->rsv.ns, rw->n_dest_nodes)) {
			rw_request_hash_delete(&hkey, rw);
			tr->result_code = AS_ERR_UNAVAILABLE;
			send_read_response(tr, NULL, NULL, 0, NULL);
			return TRANS_DONE_ERROR;
		}

		start_repl_ping(rw, tr);

		// Started replica ping.
		return TRANS_IN_PROGRESS;
	}

	// Trying again under hash.
	status = read_local(tr);
	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");
	rw_request_hash_delete(&hkey, rw);

	return status;
}


//==========================================================
// Local helpers - transaction flow.
//

void
start_read_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw_request, construct and send dup-res message.

	dup_res_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, read_dup_res_cb, read_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


void
start_repl_ping(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-ping message.

	repl_ping_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_ping_setup_rw(rw, tr, repl_ping_cb, read_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


bool
read_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, read, FROM_CLIENT, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		send_read_response(&tr, NULL, NULL, 0, NULL);
		return true;
	}

	if (read_must_ping(&tr)) {
		// Set up the nodes to which we'll ping.
		rw->n_dest_nodes = as_partition_get_other_replicas(tr.rsv.p,
				rw->dest_nodes);

		if (insufficient_replica_destinations(tr.rsv.ns, rw->n_dest_nodes)) {
			tr.result_code = AS_ERR_UNAVAILABLE;
			send_read_response(&tr, NULL, NULL, 0, NULL);
			return true;
		}

		repl_ping_after_dup_res(rw, &tr);

		return false;
	}

	// Read the local copy and respond to origin.
	transaction_status status = read_local(&tr);

	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
	}

	// Finished transaction - rw_request cleans up reservation and msgp!
	return true;
}


void
repl_ping_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica pings. Note - we are under the rw_request lock here!

	repl_ping_make_message(rw, tr);
	repl_ping_reset_rw(rw, tr, repl_ping_cb);
	send_rw_messages(rw);
}


void
repl_ping_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, read, FROM_CLIENT, repl_ping);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, repl_ping);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	// Read the local copy and respond to origin.
	transaction_status status = read_local(&tr);

	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
	}
}


//==========================================================
// Local helpers - transaction end.
//

void
send_read_response(as_transaction* tr, as_msg_op** ops, as_bin** response_bins,
		uint16_t n_bins, cf_dyn_buf* db)
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
		BENCHMARK_NEXT_DATA_POINT(tr, read, local);
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, ops, response_bins, n_bins,
					tr->rsv.ns, as_transaction_trid(tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, read, response);
		HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(tr, read_hist);
		client_read_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, ops,
					response_bins, n_bins, tr->rsv.ns, as_transaction_trid(tr));
		}
		if (as_transaction_is_batch_sub(tr)) {
			from_proxy_batch_sub_read_update_stats(tr->rsv.ns, tr->result_code);
		}
		else {
			from_proxy_read_update_stats(tr->rsv.ns, tr->result_code);
		}
		break;
	case FROM_BATCH:
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, read_local);
		as_batch_add_result(tr, n_bins, response_bins, ops);
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, response);
		batch_sub_read_update_stats(tr->rsv.ns, tr->result_code);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}


void
read_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res callback
	}

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, rw_request_trid(rw));
		// Timeouts aren't included in histograms.
		client_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_PROXY:
		if (rw_request_is_batch_sub(rw)) {
			from_proxy_batch_sub_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		else {
			from_proxy_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		break;
	case FROM_BATCH:
		as_batch_add_error(rw->from.batch_shared, rw->from_data.batch_index,
				AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		batch_sub_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - read local.
//

transaction_status
read_local(as_transaction* tr)
{
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	as_index_ref r_ref;

	if (as_record_get(tr->rsv.tree, &tr->keyd, &r_ref) != 0) {
		read_local_done(tr, NULL, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	as_record* r = r_ref.r;

	// Check if it's an expired or truncated record.
	if (as_record_is_doomed(r, ns)) {
		read_local_done(tr, &r_ref, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	int result = repl_state_check(r, tr);

	if (result != 0) {
		if (result == -3) {
			read_local_done(tr, &r_ref, NULL, AS_ERR_UNAVAILABLE);
			return TRANS_DONE_ERROR;
		}

		// No response sent to origin.
		as_record_done(&r_ref, ns);
		return result == 1 ? TRANS_IN_PROGRESS : TRANS_WAITING;
	}

	// Check if it's a tombstone.
	if (! as_record_is_live(r)) {
		read_local_done(tr, &r_ref, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	// If configuration permits, allow reads to use page cache.
	rd.read_page_cache = ns->storage_read_page_cache;

	// Check the key if required.
	// Note - for data-not-in-memory "exists" ops, key check is expensive!
	if (as_transaction_has_key(tr) &&
			as_storage_record_get_key(&rd) && ! check_msg_key(m, &rd)) {
		read_local_done(tr, &r_ref, &rd, AS_ERR_KEY_MISMATCH);
		return TRANS_DONE_ERROR;
	}

	if ((m->info1 & AS_MSG_INFO1_GET_NO_BINS) != 0) {
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		read_local_done(tr, &r_ref, &rd, AS_OK);
		return TRANS_DONE_SUCCESS;
	}

	if ((result = as_storage_rd_load_n_bins(&rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: failed as_storage_rd_load_n_bins() ", ns->name);
		read_local_done(tr, &r_ref, &rd, -result);
		return TRANS_DONE_ERROR;
	}

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd.n_bins];

	if ((result = as_storage_rd_load_bins(&rd, stack_bins)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: failed as_storage_rd_load_bins() ", ns->name);
		read_local_done(tr, &r_ref, &rd, -result);
		return TRANS_DONE_ERROR;
	}

	if (! as_bin_inuse_has(&rd)) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: found record with no bins ", ns->name);
		read_local_done(tr, &r_ref, &rd, AS_ERR_UNKNOWN);
		return TRANS_DONE_ERROR;
	}

	uint32_t bin_count = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0 ?
			rd.n_bins : m->n_ops;

	as_msg_op* ops[bin_count];
	as_msg_op** p_ops = ops;
	as_bin* response_bins[bin_count];
	uint16_t n_bins = 0;

	as_bin result_bins[bin_count];
	uint32_t n_result_bins = 0;

	if ((m->info1 & AS_MSG_INFO1_GET_ALL) != 0) {
		p_ops = NULL;
		n_bins = rd.n_bins;
		as_bin_get_all_p(&rd, response_bins);
	}
	else {
		if (m->n_ops == 0) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: bin op(s) expected, none present ", ns->name);
			read_local_done(tr, &r_ref, &rd, AS_ERR_PARAMETER);
			return TRANS_DONE_ERROR;
		}

		bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

		as_msg_op* op = 0;
		int n = 0;

		while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
			if (op->op == AS_MSG_OP_READ) {
				as_bin* b = as_bin_get_from_buf(&rd, op->name, op->name_sz);

				if (b || respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = b;
				}
			}
			else if (op->op == AS_MSG_OP_CDT_READ) {
				as_bin* b = as_bin_get_from_buf(&rd, op->name, op->name_sz);

				if (b) {
					as_bin* rb = &result_bins[n_result_bins];
					as_bin_set_empty(rb);

					if ((result = as_bin_cdt_read_from_client(b, op, rb)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: failed as_bin_cdt_read_from_client() ", ns->name);
						destroy_stack_bins(result_bins, n_result_bins);
						read_local_done(tr, &r_ref, &rd, -result);
						return TRANS_DONE_ERROR;
					}

					if (as_bin_inuse(rb)) {
						n_result_bins++;
						ops[n_bins] = op;
						response_bins[n_bins++] = rb;
					}
					else if (respond_all_ops) {
						ops[n_bins] = op;
						response_bins[n_bins++] = NULL;
					}
				}
				else if (respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = NULL;
				}
			}
			else {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: unexpected bin op %u ", ns->name, op->op);
				destroy_stack_bins(result_bins, n_result_bins);
				read_local_done(tr, &r_ref, &rd, AS_ERR_PARAMETER);
				return TRANS_DONE_ERROR;
			}
		}
	}

	cf_dyn_buf_define_size(db, 16 * 1024);

	if (tr->origin != FROM_BATCH) {
		db.used_sz = db.alloc_sz;
		db.buf = (uint8_t*)as_msg_make_response_msg(tr->result_code,
				r->generation, r->void_time, p_ops, response_bins, n_bins, ns,
				(cl_msg*)dyn_bufdb, &db.used_sz, as_transaction_trid(tr));

		db.is_stack = db.buf == dyn_bufdb;
		// Note - not bothering to correct alloc_sz if buf was allocated.
	}
	else {
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		// Since as_batch_add_result() constructs response directly in shared
		// buffer to avoid extra copies, can't use db.
		send_read_response(tr, p_ops, response_bins, n_bins, NULL);
	}

	destroy_stack_bins(result_bins, n_result_bins);
	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	// Now that we're not under the record lock, send the message we just built.
	if (db.used_sz != 0) {
		send_read_response(tr, NULL, NULL, 0, &db);

		cf_dyn_buf_free(&db);
		tr->from.proto_fd_h = NULL;
	}

	return TRANS_DONE_SUCCESS;
}


void
read_local_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		int result_code)
{
	if (r_ref) {
		if (rd) {
			as_storage_record_close(rd);
		}

		as_record_done(r_ref, tr->rsv.ns);
	}

	tr->result_code = (uint8_t)result_code;

	send_read_response(tr, NULL, NULL, 0, NULL);
}
