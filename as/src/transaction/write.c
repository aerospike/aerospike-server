/*
 * write.c
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

#include "transaction/write.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/expop.h"
#include "base/index.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/mrt_utils.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

#define MAX_N_OPS (32 * 1024)

COMPILER_ASSERT(RECORD_MAX_BINS + MAX_N_OPS < 64 * 1024);


//==========================================================
// Forward declarations.
//

static void write_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r);
static void start_write_repl_write(rw_request* rw, as_transaction* tr);
static void start_write_repl_write_forget(rw_request* rw, as_transaction* tr);
static bool write_dup_res_cb(rw_request* rw);
static void write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
static void write_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr);
static void write_repl_write_cb(rw_request* rw);

static void send_write_response(as_transaction* tr, cf_dyn_buf* db);
static void write_timeout_cb(rw_request* rw);

static transaction_status write_master(rw_request* rw, as_transaction* tr);
static void write_master_failed(as_transaction* tr, as_index_ref* r_ref, as_index_tree* tree, as_storage_rd* rd, int result_code);
static int write_master_preprocessing(as_transaction* tr);
static int write_master_policies(as_transaction* tr, bool* p_must_not_create, bool* p_is_replace);
static int write_master_apply(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd, bool is_replace, rw_request* rw, bool* is_delete);
static int write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd, as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins, as_bin* result_bins, uint32_t* p_n_result_bins, cf_ll_buf* particles_llb);


//==========================================================
// Inlines & macros.
//

static inline void
client_write_update_stats(as_namespace* ns, uint8_t result_code, bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_client_write_success);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_success);
		}
		break;
	default:
		as_incr_uint64(&ns->n_client_write_error);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_error);
		}
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_client_write_timeout);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_timeout);
		}
		break;
	case AS_ERR_FILTERED_OUT:
		// Can't be an XDR write.
		as_incr_uint64(&ns->n_client_write_filtered_out);
		break;
	}
}

static inline void
from_proxy_write_update_stats(as_namespace* ns, uint8_t result_code,
		bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_from_proxy_write_success);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_success);
		}
		break;
	default:
		as_incr_uint64(&ns->n_from_proxy_write_error);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_error);
		}
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_from_proxy_write_timeout);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_timeout);
		}
		break;
	case AS_ERR_FILTERED_OUT:
		// Can't be an XDR write.
		as_incr_uint64(&ns->n_from_proxy_write_filtered_out);
		break;
	}
}

// Can't be an XDR write.
static inline void
batch_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_batch_sub_write_success);
		break;
	default:
		as_incr_uint64(&ns->n_batch_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_batch_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_batch_sub_write_filtered_out);
		break;
	}
}

// Can't be an XDR write.
static inline void
from_proxy_batch_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_success);
		break;
	default:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_filtered_out);
		break;
	}
}

static inline void
ops_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_ops_sub_write_success);
		break;
	default:
		as_incr_uint64(&ns->n_ops_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_ops_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT: // doesn't include those filtered out by metadata
		as_incr_uint64(&ns->n_ops_sub_write_filtered_out);
		break;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_write_start(as_transaction* tr)
{
	BENCHMARK_START(tr, write, FROM_CLIENT);
	BENCHMARK_START_FROM_BATCH(tr);
	BENCHMARK_START(tr, ops_sub, FROM_IOPS);

	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_ERR_FORBIDDEN;
		send_write_response(tr, NULL);
		return TRANS_DONE;
	}

	// Check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns, 0, "write")) {
		tr->result_code = AS_ERR_DEVICE_OVERLOAD;
		send_write_response(tr, NULL);
		return TRANS_DONE;
	}

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->ix, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_write_response(tr, NULL);
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
	if (tr->rsv.n_dupl != 0 && dup_res_start(rw, tr, write_dup_res_start_cb)) {
		return TRANS_IN_PROGRESS; // started duplicate resolution
	}
	// else - no duplicate resolution phase, apply operation to master.

	status = write_master(rw, tr);

	BENCHMARK_NEXT_DATA_POINT_FROM(tr, write, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, batch_sub, FROM_BATCH, write_master);
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, ops_sub, FROM_IOPS, master);

	// If error, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);

		if (status != TRANS_WAITING) {
			send_write_response(tr, NULL);
		}

		return status;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(tr);
		send_write_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(tr)) {
		start_write_repl_write_forget(rw, tr);
		send_write_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE;
	}

	start_write_repl_write(rw, tr);

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

static void
write_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr, r);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, write_dup_res_cb, write_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_write_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, write_repl_write_cb, write_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_write_repl_write_forget(rw_request* rw, as_transaction* tr)
{
	// Construct and send repl-write message. No need to finish rw setup.

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

static bool
write_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, write, FROM_CLIENT, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, ops_sub, FROM_IOPS, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		send_write_response(&tr, NULL);
		return true;
	}

	transaction_status status = write_master(rw, &tr);

	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), write, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), batch_sub, FROM_BATCH, write_master);
	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), ops_sub, FROM_IOPS, master);

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
		return true;
	}

	if (status == TRANS_DONE) {
		send_write_response(&tr, NULL);
		return true;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(&tr);
		send_write_response(&tr, &rw->response_db);
		return true;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(&tr)) {
		write_repl_write_forget_after_dup_res(rw, &tr);
		send_write_response(&tr, &rw->response_db);
		return true;
	}

	write_repl_write_after_dup_res(rw, &tr);

	// Started replica write - don't delete rw_request from hash.
	return false;
}

static void
write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, write_repl_write_cb);
	send_rw_messages(rw);
}

static void
write_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Send replica writes. Not waiting for acks, so need to reset rw_request.
	// Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

static void
write_repl_write_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, write, FROM_CLIENT, repl_write);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, repl_write);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, ops_sub, FROM_IOPS, repl_write);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	finished_replicated(&tr);
	send_write_response(&tr, &rw->response_db);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

static void
send_write_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	clear_delete_response_metadata(tr);

	// Note - doesn't conflict with clear_delete_response_metadata() since we
	// only flag as delete on success, where we don't return record version.
	as_record_version v;

	switch (tr->origin) {
	case FROM_CLIENT:
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, NULL, NULL, 0, tr->rsv.ns,
					mrt_write_fill_version(&v, tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, write, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, write_hist);
		client_write_update_stats(tr->rsv.ns, tr->result_code,
				as_transaction_is_xdr(tr));
		break;
	case FROM_PROXY:
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, NULL, NULL,
					0, tr->rsv.ns, mrt_write_fill_version(&v, tr));
		}
		if (as_transaction_is_batch_sub(tr)) {
			from_proxy_batch_sub_write_update_stats(tr->rsv.ns,
					tr->result_code);
		}
		else {
			from_proxy_write_update_stats(tr->rsv.ns, tr->result_code,
					as_transaction_is_xdr(tr));
		}
		break;
	case FROM_BATCH:
		if (db && db->used_sz != 0) {
			as_batch_add_made_result(tr->from.batch_shared,
					tr->from_data.batch_index, (cl_msg*)db->buf, db->used_sz);
		}
		else {
			as_batch_add_ack(tr, mrt_write_fill_version(&v, tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, batch_sub_write_hist);
		batch_sub_write_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_IOPS:
		tr->from.iops_orig->done_cb(tr->from.iops_orig->udata, tr->result_code);
		BENCHMARK_NEXT_DATA_POINT(tr, ops_sub, response);
		ops_sub_write_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_MONITOR_UPDATE:
		// Nothing needed.
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}

static void
write_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	finished_not_replicated(rw);

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, NULL);
		// Timeouts aren't included in histograms.
		client_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
				as_msg_is_xdr(&rw->msgp->msg));
		break;
	case FROM_PROXY:
		if (rw_request_is_batch_sub(rw)) {
			from_proxy_batch_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		else {
			from_proxy_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
					as_msg_is_xdr(&rw->msgp->msg));
		}
		break;
	case FROM_BATCH:
		as_batch_add_error(rw->from.batch_shared, rw->from_data.batch_index,
				AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		batch_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_IOPS:
		rw->from.iops_orig->done_cb(rw->from.iops_orig->udata, AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		ops_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_MONITOR_UPDATE:
		// Nothing needed.
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - write master.
//

static transaction_status
write_master(rw_request* rw, as_transaction* tr)
{
	//------------------------------------------------------
	// Perform checks that don't need to loop over ops, or
	// create or find (and lock) the as_index.
	//

	if (! write_master_preprocessing(tr)) {
		// Failure cases all call write_master_failed().
		return TRANS_DONE;
	}

	//------------------------------------------------------
	// Loop over ops to set some essential policy flags.
	//

	bool must_not_create;
	bool is_replace;

	int result = write_master_policies(tr, &must_not_create, &is_replace);

	if (result != 0) {
		write_master_failed(tr, NULL, NULL, NULL, result);
		return TRANS_DONE;
	}

	//------------------------------------------------------
	// Find or create the as_index and get a reference -
	// this locks the record. Perform all checks that don't
	// need the as_storage_rd.
	//

	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_index_tree* tree = tr->rsv.tree;

	bool is_mrt = as_transaction_has_mrt_id(tr);

	// Find or create as_index, populate as_index_ref, lock record.
	as_index_ref r_ref = { 0 };
	as_record* r = NULL;
	bool record_created = false;

	if (must_not_create) {
		int rv = as_record_get(tree, &tr->keyd, &r_ref);

		r = r_ref.r;

		if ((result = mrt_allow_write(tr, r)) != 0) {
			write_master_failed(tr, &r_ref, tree, NULL, result);
			return TRANS_DONE;
		}

		if (rv != 0) {
			write_master_failed(tr, NULL, tree, NULL, AS_ERR_NOT_FOUND);
			return TRANS_DONE;
		}

		// Set non-zero values - returns generation & void-time on errors - ok?
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		if (as_record_is_doomed(r, ns) && ! (is_mrt && is_mrt_provisional(r))) {
			write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_NOT_FOUND);
			return TRANS_DONE;
		}

		if (repl_state_check(r, tr) < 0) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		if (! as_record_is_live(r)) {
			write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_NOT_FOUND);
			return TRANS_DONE;
		}
	}
	else {
		int rv = as_record_get_create(tree, &tr->keyd, &r_ref, ns);

		if (rv < 0) {
			cf_detail(AS_RW, "{%s} write_master: fail as_record_get_create() %pD", ns->name, &tr->keyd);
			write_master_failed(tr, NULL, tree, NULL, AS_ERR_UNKNOWN);
			return TRANS_DONE;
		}

		r = r_ref.r;
		record_created = rv == 1; // also equivalent to r->generation == 0

		if ((result = mrt_allow_write(tr, r)) != 0) {
			write_master_failed(tr, &r_ref, tree, NULL, result);
			return TRANS_DONE;
		}

		// Set non-zero values - returns generation & void-time on errors - ok?
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		bool is_doomed = as_record_is_doomed(r, ns) &&
				! (is_mrt && is_mrt_provisional(r));

		if (! record_created && ! is_doomed && repl_state_check(r, tr) < 0) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		// If it's an expired or truncated record, pretend it's a fresh create.
		if (! record_created && is_doomed) {
			as_set_index_delete_live(ns, tree, r, r_ref.r_h);
			as_record_rescue(&r_ref, ns);
			record_created = true;
		}
	}

	// Enforce record-level create-only existence policy.
	if ((m->info2 & AS_MSG_INFO2_CREATE_ONLY) != 0 &&
			! record_created && as_record_is_live(r)) {
		write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_RECORD_EXISTS);
		return TRANS_DONE;
	}

	// Check MRT locking requirement, if any.
	if (! mrt_write_on_locking_only(tr, r)) {
		write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_MRT_ALREADY_LOCKED);
		return TRANS_DONE;
	}

	// Check generation requirement, if any.
	if (! generation_check(r, m, ns)) {
		write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_GENERATION);
		return TRANS_DONE;
	}

	// If creating record, write set-ID into index.
	if (record_created) {
		if (as_transaction_has_set(tr) &&
				(result = set_set_from_msg(r, ns, m)) != AS_OK) {
			write_master_failed(tr, &r_ref, tree, NULL, result);
			return TRANS_DONE;
		}

		// Don't write record if it would be truncated.
		if (! is_mrt &&
				as_truncate_now_is_truncated(ns, as_index_get_set_id(r))) {
			write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_FORBIDDEN);
			return TRANS_DONE;
		}
	}

	// If record existed, check that as_msg set name matches.
	if (! record_created && tr->origin != FROM_IOPS &&
			(result = set_name_check_on_update(tr, r)) != 0) {
		write_master_failed(tr, &r_ref, tree, NULL, result);
		return TRANS_DONE;
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	// Enforce set size limit, if any.
	if (as_set_size_stop_writes(p_set)) {
		cf_ticker_warning(AS_RW, "{%s|%s} at stop-writes-size - can't write", ns->name, p_set->name);
		write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_FORBIDDEN);
		return TRANS_DONE;
	}

	// Shortcut set name.
	const char* set_name = p_set == NULL ? NULL : p_set->name;

	if (! record_created) {
		as_xdr_ship_status ship_status = as_xdr_ship_check(r, tr);

		if (ship_status == XDR_SHIP_NEAR) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		if (ship_status == XDR_SHIP_FAR) {
			write_master_failed(tr, &r_ref, tree, NULL, AS_ERR_XDR_KEY_BUSY);
			return TRANS_DONE;
		}
	}

	as_exp* filter_exp = NULL;

	// Handle metadata filter if present.
	if (! record_created && as_record_is_live(r) &&
			(result = handle_meta_filter(tr, r, &filter_exp)) != 0) {
		write_master_failed(tr, &r_ref, tree, NULL, result);
		return TRANS_DONE;
	}

	//------------------------------------------------------
	// Open or create the as_storage_rd, and handle record
	// metadata.
	//

	as_storage_rd rd;

	if (record_created) {
		as_storage_record_create(ns, r, &rd);
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	// Add the MRT id, as appropriate.
	if ((result = set_mrt_id_from_msg(&rd, tr)) != 0) {
		write_master_failed(tr, &r_ref, tree, &rd, result);
		return TRANS_DONE;
	}

	if ((result = as_mrt_monitor_write_check(tr, &rd)) != 0) {
		write_master_failed(tr, &r_ref, tree, &rd, result);
		return TRANS_DONE;
	}

	// Apply record bins filter if present.
	if (filter_exp != NULL) {
		if ((result = read_and_filter_bins(&rd, filter_exp)) != 0) {
			destroy_filter_exp(tr, filter_exp);
			write_master_failed(tr, &r_ref, tree, &rd, result);
			return TRANS_DONE;
		}

		destroy_filter_exp(tr, filter_exp);
	}

	// Check background si-queries for false positives.
	if (tr->origin == FROM_IOPS) {
		iops_origin* origin = tr->from.iops_orig;

		if (origin->check_cb != NULL &&
				! origin->check_cb(origin->udata, &rd)) {
			write_master_failed(tr, &r_ref, tree, &rd, AS_ERR_NOT_FOUND);
			return TRANS_DONE;
		}
	}

	// Shortcut for set name storage.
	if (set_name) {
		rd.set_name = set_name;
		rd.set_name_len = strlen(set_name);
	}

	// Deal with key storage as needed.
	if ((result = handle_msg_key(tr, &rd)) != 0) {
		write_master_failed(tr, &r_ref, tree, &rd, result);
		return TRANS_DONE;
	}

	// Convert message TTL special value if appropriate.
	if (m->record_ttl == TTL_DONT_UPDATE &&
			(record_created || ! as_record_is_live(r))) {
		m->record_ttl = TTL_USE_DEFAULT;
	}

	if (! is_valid_ttl(m->record_ttl)) {
		cf_warning(AS_RW, "write_master: invalid ttl %u", m->record_ttl);
		write_master_failed(tr, &r_ref, tree, &rd, AS_ERR_PARAMETER);
		return TRANS_DONE;
	}

	if (is_ttl_disallowed(m->record_ttl, ns, p_set) &&
			! as_mrt_monitor_is_monitor_record(ns, r)) {
		cf_ticker_warning(AS_RW, "write_master: disallowed ttl with nsup-period 0");
		write_master_failed(tr, &r_ref, tree, &rd, AS_ERR_FORBIDDEN);
		return TRANS_DONE;
	}

	// Set up the nodes to which we'll write replicas.
	if (! set_replica_destinations(tr, rw)) {
		write_master_failed(tr, &r_ref, tree, &rd, AS_ERR_UNAVAILABLE);
		return TRANS_DONE;
	}

	// Fire and forget can overload the fabric send queues - check.
	if (respond_on_master_complete(tr) &&
			as_fabric_is_overloaded(rw->dest_nodes, rw->n_dest_nodes,
					AS_FABRIC_CHANNEL_RW, 0)) {
		tr->flags |= AS_TRANSACTION_FLAG_SWITCH_TO_COMMIT_ALL;
	}

	// Will we need a pickle?
	rd.keep_pickle = rw->n_dest_nodes != 0;

	// Save for XDR submit.
	uint64_t prev_lut = r->last_update_time;

	//------------------------------------------------------
	// Handle bin operations and commit master.
	//

	bool is_delete = false;

	result = write_master_apply(tr, &r_ref, &rd, is_replace, rw, &is_delete);

	if (result != 0) {
		write_master_failed(tr, &r_ref, tree, &rd, result);
		return TRANS_DONE;
	}

	//------------------------------------------------------
	// Done - complete function's output, release the record
	// lock, and do XDR write if appropriate.
	//

	tr->generation = r->generation;
	tr->void_time = r->void_time;
	tr->last_update_time = r->last_update_time;

	// Handle deletion if appropriate.
	if (is_delete) {
		write_delete_record(r_ref.r, tree);
		as_incr_uint64(&ns->n_deleted_last_bin);
		tr->flags |= AS_TRANSACTION_FLAG_IS_DELETE;
	}
	// Or (normally) adjust max void-time.
	else if (r->void_time != 0) {
		as_setmax_uint32(&tr->rsv.p->max_void_time, r->void_time);
	}

	will_replicate(r, ns);

	// Save for XDR submit outside record lock.
	as_xdr_submit_info submit_info;

	as_xdr_get_submit_info(r, prev_lut, &submit_info);

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	if (! write_is_full_drop(tr)) {
		as_xdr_submit(ns, &submit_info);
	}

	return TRANS_IN_PROGRESS;
}

static void
write_master_failed(as_transaction* tr, as_index_ref* r_ref,
		as_index_tree* tree, as_storage_rd* rd, int result_code)
{
	as_namespace* ns = tr->rsv.ns;

	if (r_ref != NULL && r_ref->r != NULL) {
		if (rd != NULL) {
			as_storage_record_close(rd);
		}

		if (r_ref->r->generation == 0) { // was created
			as_index_delete(tree, &tr->keyd);
		}

		as_record_done(r_ref, ns);
	}

	switch (result_code) {
	case AS_ERR_GENERATION:
		as_incr_uint64(&ns->n_fail_generation);
		break;
	case AS_ERR_RECORD_TOO_BIG:
		cf_detail(AS_RW, "{%s} write_master: record too big %pD", ns->name, &tr->keyd);
		as_incr_uint64(&ns->n_fail_record_too_big);
		break;
	case AS_ERR_MRT_BLOCKED:
		as_incr_uint64(&ns->n_fail_mrt_blocked);
		break;
	default:
		// These either log warnings or aren't interesting enough to count.
		break;
	}

	tr->result_code = (uint8_t)result_code;
}

static int
write_master_preprocessing(as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;
	as_msg* m = &tr->msgp->msg;

	if (ns->clock_skew_stop_writes) {
		// TODO - new error code?
		write_master_failed(tr, NULL, NULL, NULL, AS_ERR_FORBIDDEN);
		return false;
	}

	// ns->stop_writes is set by nsup if configured threshold is breached.
	if (ns->stop_writes) {
		write_master_failed(tr, NULL, NULL, NULL, AS_ERR_OUT_OF_SPACE);
		return false;
	}

	// Fail if disallow_null_setname is true and set name is absent or empty.
	if (ns->disallow_null_setname) {
		as_msg_field* f = as_transaction_has_set(tr) ?
				as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;

		if (! f || as_msg_field_get_value_sz(f) == 0) {
			cf_warning(AS_RW, "write_master: null/empty set name not allowed for namespace %s", ns->name);
			write_master_failed(tr, NULL, NULL, NULL, AS_ERR_PARAMETER);
			return false;
		}
	}

	return true;
}

static int
write_master_policies(as_transaction* tr, bool* p_must_not_create,
		bool* p_is_replace)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	if (m->n_ops == 0) {
		cf_warning(AS_RW, "{%s} write_master: bin op(s) expected, none present %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (m->n_ops > MAX_N_OPS) {
		cf_warning(AS_RW, "{%s} write_master: can't exceed %u bin ops %pD", ns->name, MAX_N_OPS, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	bool info1_get_all = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	bool must_not_create =
			(m->info3 & AS_MSG_INFO3_UPDATE_ONLY) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	bool is_replace =
			(m->info3 & AS_MSG_INFO3_CREATE_OR_REPLACE) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	if (is_replace && forbid_replace(ns)) {
		cf_warning(AS_RW, "{%s} write_master: can't replace record %pD if conflict resolving", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	bool has_read_op = false;
	bool has_read_all_op = false;
	bool generates_response_bin = false;

	// Loop over ops to check and modify flags.
	as_msg_op* op = NULL;
	uint16_t i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (op->op == AS_MSG_OP_TOUCH) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: touch op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			must_not_create = true;
			continue;
		}

		if (! as_bin_name_check(op->name, op->name_sz)) {
			cf_warning(AS_RW, "{%s} write_master: bad bin name %.*s (%u) %pD", ns->name, op->name_sz, op->name, op->name_sz, &tr->keyd);
			return AS_ERR_BIN_NAME;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			if (op->particle_type == AS_PARTICLE_TYPE_NULL &&
					is_replace) {
				cf_warning(AS_RW, "{%s} write_master: bin delete can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (OP_IS_MODIFY(op->op)) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (op->op == AS_MSG_OP_DELETE_ALL) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: delete-all op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			// Could forbid multiple delete-alls and delete-all being first op
			// (should use replace), but these are nonsensical, not unworkable.
		}
		else if (op_is_read_all(op, m)) {
			if (respond_all_ops) {
				cf_warning(AS_RW, "{%s} write_master: read-all op can't have respond-all-ops flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			if (has_read_all_op) {
				cf_warning(AS_RW, "{%s} write_master: can't have more than one read-all op %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			has_read_op = true;
			has_read_all_op = true;
		}
		else if (op->op == AS_MSG_OP_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_BITS_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: bits modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (op->op == AS_MSG_OP_BITS_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_HLL_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: hll modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			generates_response_bin = true; // HLL modify may generate a response bin
		}
		else if (op->op == AS_MSG_OP_HLL_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: cdt modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			generates_response_bin = true; // CDT modify may generate a response bin
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_EXP_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
	}

	if (has_read_op && (m->info1 & AS_MSG_INFO1_READ) == 0) {
		cf_warning(AS_RW, "{%s} write_master: has read op but read flag not set %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (has_read_all_op && generates_response_bin) {
		cf_warning(AS_RW, "{%s} write_master: read-all op can't mix with ops that generate response bins %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (info1_get_all && ! has_read_all_op) {
		cf_warning(AS_RW, "{%s} write_master: get-all flag set with no read-all op %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	*p_must_not_create = must_not_create;
	*p_is_replace = is_replace;

	return 0;
}

static int
write_master_apply(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		bool is_replace, rw_request* rw, bool* is_delete)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;

	bool do_indexes = rd->mrt_id == 0;
	bool set_has_si = false;
	bool si_needs_bins = false;

	if (do_indexes) {
		set_has_si = set_has_sindex(r, ns);
		si_needs_bins = set_has_si && r->in_sindex == 1;
	}

	// For sindex, we must read existing record even if replacing.
	rd->ignore_record_on_device = is_replace && ! si_needs_bins;

	as_bin stack_bins[RECORD_MAX_BINS + m->n_ops];

	int result = as_storage_rd_load_bins(rd, stack_bins);

	if (result < 0) {
		cf_warning(AS_RW, "{%s} write_master: failed as_storage_rd_load_bins() %pD", ns->name, &tr->keyd);
		return -result;
	}

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	as_bin old_bins[n_old_bins];

	//------------------------------------------------------
	// Copy old bins (if any) - which are currently in new
	// bins array - to old bins array, for sindex purposes.
	//

	uint32_t n_old_bins_saved = 0;

	if (si_needs_bins && n_old_bins != 0) {
		memcpy(old_bins, rd->bins, n_old_bins * sizeof(as_bin));
		n_old_bins_saved = n_old_bins;

		// If it's a replace, clear the new bins array.
		if (is_replace) {
			rd->n_bins = 0;
		}
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	prepare_bin_metadata(tr, rd);

	as_record old_r = *r;

	advance_record_version(tr, r);
	set_xdr_write(tr, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	if ((result = write_master_bin_ops(tr, rd, &particles_llb,
			&rw->response_db)) != 0) {
		cf_ll_buf_free(&particles_llb);
		unwind_index_metadata(&old_r, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	*is_delete = as_bin_empty_if_all_tombstones(rd,
			as_transaction_is_durable_delete(tr));

	if (*is_delete) {
		if (! as_transaction_is_xdr(tr) &&
				(n_old_bins == 0 || ! as_record_is_live(r))) {
			// Didn't exist or was bin cemetery (tombstone bit not yet updated).
			cf_ll_buf_free(&particles_llb);
			unwind_index_metadata(&old_r, r);
			return AS_ERR_NOT_FOUND;
		}

		if ((result = validate_delete_durability(tr)) != AS_OK) {
			cf_ll_buf_free(&particles_llb);
			unwind_index_metadata(&old_r, r);
			return result;
		}
	}

	transition_delete_metadata(tr, r, *is_delete,
			*is_delete && rd->n_bins != 0);

	if ((result = as_mrt_monitor_check_writes_limit(rd)) != 0) {
		cf_ll_buf_free(&particles_llb);
		unwind_index_metadata(&old_r, r);
		return result;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_detail(AS_RW, "{%s} write_master: failed as_storage_record_write() %pD", ns->name, &tr->keyd);
		cf_ll_buf_free(&particles_llb);
		unwind_index_metadata(&old_r, r);
		return -result;
	}

	as_mrt_monitor_update_hist(rd);

	as_record_transition_stats(r, ns, &old_r);
	pickle_all(rd, rw);

	//------------------------------------------------------
	// Success - adjust set index and sindex.
	//

	if (do_indexes) {
		as_record_transition_set_index(tr->rsv.tree, r_ref, ns, rd->n_bins,
				&old_r);

		if (set_has_si) {
			update_sindex(ns, r_ref, old_bins, n_old_bins_saved, rd->bins,
					rd->n_bins);
		}
		else {
			// Sindex drop will leave in_sindex bit. Good opportunity to clear.
			as_index_clear_in_sindex(r);
		}
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	finish_first_mrt(rd, &old_r, r_ref->puddle);

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (r->key_stored == 0 && rd->key) {
		r->key_stored = 1;
	}

	cf_ll_buf_free(&particles_llb);

	return 0;
}

// Not static - called by split function.
int
write_master_bin_ops(as_transaction* tr, as_storage_rd* rd,
		cf_ll_buf* particles_llb, cf_dyn_buf* db)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;
	bool has_read_all_op = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;

	as_msg_op* ops[m->n_ops];
	as_bin response_bins[has_read_all_op ? RECORD_MAX_BINS : m->n_ops];
	as_bin result_bins[m->n_ops];

	uint32_t n_response_bins = 0;
	uint32_t n_result_bins = 0;

	int result = write_master_bin_ops_loop(tr, rd, ops, response_bins,
			&n_response_bins, result_bins, &n_result_bins, particles_llb);

	if (result != 0) {
		as_bin_destroy_all(result_bins, n_result_bins);
		return result;
	}

	if (rd->n_bins > RECORD_MAX_BINS) {
		as_bin_destroy_all(result_bins, n_result_bins);
		return AS_ERR_PARAMETER;
	}

	if (n_response_bins == 0) {
		// If 'ordered-ops' flag was not set, and there were no read ops or CDT
		// ops with results, there's no response to build and send later.
		return 0;
	}

	as_bin* bins[n_response_bins];

	for (uint32_t i = 0; i < n_response_bins; i++) {
		as_bin* b = &response_bins[i];

		bins[i] = as_bin_is_used(b) ? b : NULL;
	}

	uint32_t generation = r->generation;
	uint32_t void_time = r->void_time;

	// Deletes don't return metadata.
	if (rd->n_bins == 0) {
		generation = 0;
		void_time = 0;
	}

	size_t msg_sz = 0;
	uint8_t* msgp = (uint8_t*)as_msg_make_response_msg(AS_OK, generation,
			void_time, has_read_all_op ? NULL : ops, bins,
			(uint16_t)n_response_bins, ns, NULL, &msg_sz, NULL,
			as_mrt_monitor_compute_deadline(tr));
	// Note - no record version - only for writes that don't touch.
	// Note - deadline because monitor record create response will have op.

	as_bin_destroy_all(result_bins, n_result_bins);

	// Stash the message, to be sent later.
	db->buf = msgp;
	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;

	return 0;
}

static int
write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd,
		as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins,
		as_bin* result_bins, uint32_t* p_n_result_bins,
		cf_ll_buf* particles_llb)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	uint64_t msg_lut = as_transaction_xdr_lut(tr);

	if (forbid_resolve(tr, rd, msg_lut)) {
		return AS_ERR_FORBIDDEN;
	}

	uint16_t n_won = m->n_ops;

	int result;

	as_msg_op* op = NULL;
	uint16_t i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (! resolve_bin(rd, op, msg_lut, m->n_ops, &n_won, &result)) {
			if (result != AS_OK) {
				return result;
			}

			continue;
		}

		if (op->op == AS_MSG_OP_TOUCH) {
			touch_bin_metadata(rd);
			continue;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			// AS_PARTICLE_TYPE_NULL means delete the bin.
			if (op->particle_type == AS_PARTICLE_TYPE_NULL) {
				delete_bin(rd, op, msg_lut);
			}
			// It's a regular bin write.
			else {
				as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

				write_resolved_bin(rd, op, msg_lut, b);

				if ((result = as_bin_particle_from_client(b, particles_llb, op)) < 0) {
					cf_warning(AS_RW, "{%s} write_master: failed as_bin_particle_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		// Modify an existing bin value.
		else if (OP_IS_MODIFY(op->op)) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			if ((result = as_bin_particle_modify_from_client(b, particles_llb, op)) < 0) {
				cf_warning(AS_RW, "{%s} write_master: failed as_bin_particle_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_DELETE_ALL) {
			delete_all_bins(rd);

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op_is_read_all(op, m)) {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin* b = &rd->bins[i];

				if (! as_bin_is_tombstone(b)) {
					// ops array will not be not used in this case.
					response_bins[(*p_n_response_bins)++] = *b;
				}
			}
		}
		else if (op->op == AS_MSG_OP_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = *b;
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_BITS_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			if ((result = as_bin_bits_modify_from_client(b, particles_llb, op)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_bits_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_BITS_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_bits_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_bits_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_HLL_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_hll_modify_from_client(b, particles_llb, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_hll_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops || as_bin_is_used(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_HLL_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_hll_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_hll_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_cdt_modify_from_client(b, particles_llb, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_cdt_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops || as_bin_is_used(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_cdt_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_cdt_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_EXP_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			bool created_bin = as_bin_is_unused(b);
			const as_exp_ctx exp_ctx = { .ns = ns, .rd = rd, .r = rd->r };
			const iops_expop* expop = tr->origin == FROM_IOPS ?
					&tr->from.iops_orig->expops[i] : NULL;

			if ((result = as_bin_exp_modify_from_client(&exp_ctx, b, particles_llb, op, expop)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_exp_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}

			if (as_bin_is_unused(b)) {
				if (created_bin) {
					rd->n_bins--;
				}
				else {
					delete_bin(rd, op, msg_lut);
				}
			}
		}
		else if (op->op == AS_MSG_OP_EXP_READ) {
			const as_exp_ctx exp_ctx = { .ns = ns, .rd = rd, .r = rd->r };

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_exp_read_from_client(&exp_ctx, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_exp_read_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			ops[*p_n_response_bins] = op;
			response_bins[(*p_n_response_bins)++] = result_bin;
			append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
		}
		else {
			cf_warning(AS_RW, "{%s} write_master: unknown bin op %u %pD", ns->name, op->op, &tr->keyd);
			return AS_ERR_PARAMETER;
		}
	}

	return 0;
}
