/*
 * udf.c
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

#include "transaction/udf.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_log.h"
#include "aerospike/as_list.h"
#include "aerospike/as_module.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_types.h"
#include "aerospike/as_udf_context.h"
#include "aerospike/mod_lua.h"

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/udf_aerospike.h"
#include "base/udf_arglist.h"
#include "base/udf_cask.h"
#include "base/udf_record.h"
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
// Typedefs & constants.
//

static const cf_fault_severity as_log_level_map[5] = {
	[AS_LOG_LEVEL_ERROR] = CF_WARNING,
	[AS_LOG_LEVEL_WARN]	= CF_WARNING,
	[AS_LOG_LEVEL_INFO]	= CF_INFO,
	[AS_LOG_LEVEL_DEBUG] = CF_DEBUG,
	[AS_LOG_LEVEL_TRACE] = CF_DETAIL
};

typedef struct udf_call_s {
	udf_def*		def;
	as_transaction* tr;
} udf_call;


//==========================================================
// Globals.
//

as_aerospike g_as_aerospike;

// Deadline per UDF.
static __thread uint64_t g_end_ns;


//==========================================================
// Forward declarations.
//

bool log_callback(as_log_level level, const char* func, const char* file,
		uint32_t line, const char* fmt, ...);

void start_udf_dup_res(rw_request* rw, as_transaction* tr);
void start_udf_repl_write(rw_request* rw, as_transaction* tr);
void start_udf_repl_write_forget(rw_request* rw, as_transaction* tr);
bool udf_dup_res_cb(rw_request* rw);
void udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
void udf_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr);
void udf_repl_write_cb(rw_request* rw);

void send_udf_response(as_transaction* tr, cf_dyn_buf* db);
void udf_timeout_cb(rw_request* rw);

transaction_status udf_master(rw_request* rw, as_transaction* tr);
udf_optype udf_master_apply(udf_call* call, rw_request* rw);
int udf_apply_record(udf_call* call, as_rec* rec, as_result* result);
void udf_finish(udf_record* urecord, rw_request* rw, udf_optype* record_op);
udf_optype udf_finish_op(udf_record* urecord);
void udf_post_processing(udf_record* urecord, rw_request* rw,
		udf_optype urecord_op);
bool udf_timer_timedout(const as_timer* timer);
uint64_t udf_timer_timeslice(const as_timer* timer);

void update_lua_complete_stats(uint8_t origin, as_namespace* ns, udf_optype op,
		int ret, bool is_success);

void process_failure_str(udf_call* call, const char* err_str, size_t len,
		cf_dyn_buf* db);
void process_result(const as_result* result, udf_call* call, cf_dyn_buf* db);
void process_response(udf_call* call, bool success, const as_val* val,
		cf_dyn_buf* db);


//==========================================================
// Inlines & macros.
//

static inline void
client_udf_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_client_udf_complete);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_udf_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_udf_error);
		break;
	}
}

static inline void
from_proxy_udf_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_from_proxy_udf_complete);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_from_proxy_udf_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_from_proxy_udf_error);
		break;
	}
}

static inline void
udf_sub_udf_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_udf_sub_udf_complete);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_udf_sub_udf_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_udf_sub_udf_error);
		break;
	}
}

static inline bool
udf_zero_bins_left(udf_record* urecord)
{
	return (urecord->flag & UDF_RECORD_FLAG_OPEN) != 0 &&
			! as_bin_inuse_has(urecord->rd);
}

static inline void
process_failure(udf_call* call, const as_val* val, cf_dyn_buf* db)
{
	process_response(call, false, val, db);
}

static inline void
process_success(udf_call* call, const as_val* val, cf_dyn_buf* db)
{
	process_response(call, true, val, db);
}


//==========================================================
// Public API.
//

void
as_udf_init()
{
	as_module_configure(&mod_lua, &g_config.mod_lua);
	as_log_set_callback(log_callback);
	udf_cask_init();
	as_aerospike_init(&g_as_aerospike, NULL, &udf_aerospike_hooks);
}


// Public API for udf_def class, not big enough for it's own file.
udf_def*
udf_def_init_from_msg(udf_def* def, const as_transaction* tr)
{
	def->arglist = NULL;

	as_msg* m = &tr->msgp->msg;
	as_msg_field* filename =
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_FILENAME);

	if (! filename) {
		return NULL;
	}

	as_msg_field* function =
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_FUNCTION);

	if (! function) {
		return NULL;
	}

	as_msg_field* arglist = as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_ARGLIST);

	if (! arglist) {
		return NULL;
	}

	uint32_t filename_len = as_msg_field_get_value_sz(filename);

	if (filename_len >= sizeof(def->filename)) {
		return NULL;
	}

	uint32_t function_len = as_msg_field_get_value_sz(function);

	if (function_len >= sizeof(def->function)) {
		return NULL;
	}

	memcpy(def->filename, filename->data, filename_len);
	def->filename[filename_len] = '\0';

	memcpy(def->function, function->data, function_len);
	def->function[function_len] = '\0';

	as_unpacker unpacker;

	unpacker.buffer = (const unsigned char*)arglist->data;
	unpacker.length = as_msg_field_get_value_sz(arglist);
	unpacker.offset = 0;

	if (unpacker.length > 0) {
		as_val* val = NULL;
		int ret = as_unpack_val(&unpacker, &val);

		if (ret == 0 && as_val_type(val) == AS_LIST) {
			def->arglist = (as_list*)val;
		}
	}

	as_msg_field* op = as_transaction_has_udf_op(tr) ?
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_OP) : NULL;

	def->type = op ? *op->data : AS_UDF_OP_KVS;

	return def;
}


transaction_status
as_udf_start(as_transaction* tr)
{
	BENCHMARK_START(tr, udf, FROM_CLIENT);
	BENCHMARK_START(tr, udf_sub, FROM_IUDF);

	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_ERR_ALWAYS_FORBIDDEN;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Don't know if UDF is read or delete - check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns)) {
		tr->result_code = AS_ERR_DEVICE_OVERLOAD;
		send_udf_response(tr, NULL);
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
			send_udf_response(tr, NULL);
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
	if (tr->rsv.n_dupl != 0) {
		start_udf_dup_res(rw, tr);

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
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	status = udf_master(rw, tr);

	BENCHMARK_NEXT_DATA_POINT_FROM(tr, udf, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, udf_sub, FROM_IUDF, master);

	// If error or UDF was a read, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		if (status != TRANS_WAITING) {
			send_udf_response(tr, &rw->response_db);
		}

		rw_request_hash_delete(&hkey, rw);
		return status;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(tr);
		send_udf_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE_SUCCESS;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(tr)) {
		start_udf_repl_write_forget(rw, tr);
		send_udf_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE_SUCCESS;
	}

	start_udf_repl_write(rw, tr);

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - initialization.
//

bool
log_callback(as_log_level level, const char* func, const char* file,
		uint32_t line, const char* fmt, ...)
{
	cf_fault_severity severity = as_log_level_map[level];

	if (severity > cf_fault_filter[AS_UDF]) {
		return true;
	}

	va_list ap;

	va_start(ap, fmt);
	char message[1024] = { '\0' };
	vsnprintf(message, 1024, fmt, ap);
	va_end(ap);

	cf_fault_event(AS_UDF, severity, file, line, "%s", message);

	return true;
}


//==========================================================
// Local helpers - transaction flow.
//

void
start_udf_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, udf_dup_res_cb, udf_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


void
start_udf_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, udf_repl_write_cb, udf_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}


void
start_udf_repl_write_forget(rw_request* rw, as_transaction* tr)
{
	// Construct and send repl-write message. No need to finish rw setup.

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}


bool
udf_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, udf, FROM_CLIENT, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, udf_sub, FROM_IUDF, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		send_udf_response(&tr, NULL);
		return true;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr.rsv.p,
			rw->dest_nodes);

	if (insufficient_replica_destinations(tr.rsv.ns, rw->n_dest_nodes)) {
		tr.result_code = AS_ERR_UNAVAILABLE;
		send_udf_response(&tr, NULL);
		return true;
	}

	transaction_status status = udf_master(rw, &tr);

	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), udf, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), udf_sub, FROM_IUDF, master);

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
		return true;
	}

	if (status != TRANS_IN_PROGRESS) {
		send_udf_response(&tr, &rw->response_db);
		return true;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(&tr);
		send_udf_response(&tr, &rw->response_db);
		return true;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(&tr)) {
		udf_repl_write_forget_after_dup_res(rw, &tr);
		send_udf_response(&tr, &rw->response_db);
		return true;
	}

	udf_repl_write_after_dup_res(rw, &tr);

	// Started replica write - don't delete rw_request from hash.
	return false;
}


void
udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, udf_repl_write_cb);
	send_rw_messages(rw);
}


void
udf_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Send replica writes. Not waiting for acks, so need to reset rw_request.
	// Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}


void
udf_repl_write_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, udf, FROM_CLIENT, repl_write);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, udf_sub, FROM_IUDF, repl_write);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	finished_replicated(&tr);
	send_udf_response(&tr, &rw->response_db);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

void
send_udf_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	clear_delete_response_metadata(tr);

	switch (tr->origin) {
	case FROM_CLIENT:
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, NULL, NULL, 0, tr->rsv.ns,
					as_transaction_trid(tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, udf, response);
		HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(tr, udf_hist);
		client_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, NULL, NULL,
					0, tr->rsv.ns, as_transaction_trid(tr));
		}
		from_proxy_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_IUDF:
		if (db && db->used_sz != 0) {
			cf_crash(AS_RW, "unexpected - internal udf has response");
		}
		tr->from.iudf_orig->cb(tr->from.iudf_orig->udata, tr->result_code);
		BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, response);
		udf_sub_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}


void
udf_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	finished_not_replicated(rw);

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, rw_request_trid(rw));
		// Timeouts aren't included in histograms.
		client_udf_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_PROXY:
		from_proxy_udf_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_IUDF:
		rw->from.iudf_orig->cb(rw->from.iudf_orig->udata, AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		udf_sub_udf_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - UDF.
//

transaction_status
udf_master(rw_request* rw, as_transaction* tr)
{
	CF_ALLOC_SET_NS_ARENA(tr->rsv.ns);

	udf_def def;
	udf_call call = { &def, tr };

	if (tr->origin == FROM_IUDF) {
		call.def = &tr->from.iudf_orig->def;
	}
	else if (! udf_def_init_from_msg(call.def, tr)) {
		cf_warning(AS_UDF, "failed udf_def_init_from_msg");
		tr->result_code = AS_ERR_PARAMETER;
		return TRANS_DONE_ERROR;
	}

	udf_optype optype = udf_master_apply(&call, rw);

	if (tr->origin != FROM_IUDF && call.def->arglist) {
		as_list_destroy(call.def->arglist);
	}

	if (optype == UDF_OPTYPE_READ || optype == UDF_OPTYPE_NONE) {
		// UDF is done, no replica writes needed.
		return TRANS_DONE_SUCCESS;
	}

	return optype == UDF_OPTYPE_WAITING ? TRANS_WAITING : TRANS_IN_PROGRESS;
}


udf_optype
udf_master_apply(udf_call* call, rw_request* rw)
{
	as_transaction* tr = call->tr;
	as_namespace* ns = tr->rsv.ns;

	// Find record in index.

	as_index_ref r_ref;
	int get_rv = as_record_get(tr->rsv.tree, &tr->keyd, &r_ref);

	if (get_rv == 0 && as_record_is_doomed(r_ref.r, ns)) {
		// If record is expired or truncated, pretend it was not found.
		as_record_done(&r_ref, ns);
		get_rv = -1;
	}

	if (get_rv == 0 && repl_state_check(r_ref.r, tr) < 0) {
		as_record_done(&r_ref, ns);
		return UDF_OPTYPE_WAITING;
	}

	if (tr->origin == FROM_IUDF &&
			(get_rv == -1 || ! as_record_is_live(r_ref.r))) {
		// Internal UDFs must not create records.
		tr->result_code = AS_ERR_NOT_FOUND;
		process_failure(call, NULL, &rw->response_db);
		return UDF_OPTYPE_NONE;
	}

	// Open storage record.

	as_storage_rd rd;

	udf_record urecord;
	udf_record_init(&urecord, true);

	xdr_dirty_bins dirty_bins;
	xdr_clear_dirty_bins(&dirty_bins);

	urecord.r_ref	= &r_ref;
	urecord.tr		= tr;
	urecord.rd		= &rd;
	urecord.dirty	= &dirty_bins;
	urecord.keyd	= tr->keyd;

	if (get_rv == 0) {
		urecord.flag |= (UDF_RECORD_FLAG_OPEN | UDF_RECORD_FLAG_PREEXISTS);

		if (udf_storage_record_open(&urecord) != 0) {
			udf_record_close(&urecord);
			tr->result_code = AS_ERR_BIN_NAME; // overloaded... add bin_count error?
			process_failure(call, NULL, &rw->response_db);
			return UDF_OPTYPE_NONE;
		}

		if (tr->origin == FROM_IUDF && tr->from.iudf_orig->predexp) {
			predexp_args_t predargs = {
					.ns = ns, .md = r_ref.r, .vl = NULL, .rd = &rd
			};

			if (! predexp_matches_record(tr->from.iudf_orig->predexp,
					&predargs)) {
				udf_record_close(&urecord);
				tr->result_code = AS_ERR_NOT_FOUND; // not ideal
				process_failure(call, NULL, &rw->response_db);
				return UDF_OPTYPE_NONE;
			}
		}

		as_msg* m = &tr->msgp->msg;

		// If both the record and the message have keys, check them.
		if (rd.key) {
			if (as_transaction_has_key(tr) && ! check_msg_key(m, &rd)) {
				udf_record_close(&urecord);
				tr->result_code = AS_ERR_KEY_MISMATCH;
				process_failure(call, NULL, &rw->response_db);
				return UDF_OPTYPE_NONE;
			}
		}
		else {
			// If the message has a key, apply it to the record.
			if (! get_msg_key(tr, &rd)) {
				udf_record_close(&urecord);
				tr->result_code = AS_ERR_UNSUPPORTED_FEATURE;
				process_failure(call, NULL, &rw->response_db);
				return UDF_OPTYPE_NONE;
			}

			urecord.flag |= UDF_RECORD_FLAG_METADATA_UPDATED;
		}
	}
	else {
		urecord.flag &= ~(UDF_RECORD_FLAG_OPEN |
				UDF_RECORD_FLAG_STORAGE_OPEN |
				UDF_RECORD_FLAG_PREEXISTS);
	}

	// Run UDF.

	// This as_rec needs to be in the heap - once passed into the lua scope it
	// gets garbage collected later. Also, the destroy hook is set to NULL so
	// garbage collection has nothing to do.
	as_rec* urec = as_rec_new(&urecord, &udf_record_hooks);

	as_val_reserve(urec); // for lua

	as_result result;
	as_result_init(&result);

	int apply_rv = udf_apply_record(call, urec, &result);

	udf_optype optype = UDF_OPTYPE_NONE;

	if (apply_rv == 0) {
		udf_finish(&urecord, rw, &optype);
		process_result(&result, call, &rw->response_db);
	}
	else {
		udf_record_close(&urecord);

		char* rs = as_module_err_string(apply_rv);

		tr->result_code = AS_ERR_UDF_EXECUTION;
		process_failure_str(call, rs, strlen(rs), &rw->response_db);
		cf_free(rs);
	}

	update_lua_complete_stats(tr->origin, ns, optype, apply_rv,
			result.is_success);

	as_result_destroy(&result);
	udf_record_destroy(urec);

	return optype;
}


int
udf_apply_record(udf_call* call, as_rec* rec, as_result* result)
{
	// timedout callback gives no 'udata' per UDF - use thread-local.
	g_end_ns = ((udf_record*)rec->data)->tr->end_time;

	as_timer timer;

	static const as_timer_hooks udf_timer_hooks = {
		.destroy	= NULL,
		.timedout	= udf_timer_timedout,
		.timeslice	= udf_timer_timeslice
	};

	as_timer_init(&timer, NULL, &udf_timer_hooks);

	as_udf_context ctx = {
		.as			= &g_as_aerospike,
		.timer		= &timer,
		.memtracker	= NULL
	};

	return as_module_apply_record(&mod_lua, &ctx, call->def->filename,
			call->def->function, rec, call->def->arglist, result);
}


void
udf_finish(udf_record* urecord, rw_request* rw, udf_optype* record_op)
{
	*record_op = UDF_OPTYPE_READ;

	udf_optype final_op = udf_finish_op(urecord);

	if (final_op == UDF_OPTYPE_DELETE) {
		*record_op = UDF_OPTYPE_DELETE;
		urecord->tr->flags |= AS_TRANSACTION_FLAG_IS_DELETE;
	}
	else if (final_op == UDF_OPTYPE_WRITE) {
		*record_op = UDF_OPTYPE_WRITE;
	}

	udf_post_processing(urecord, rw, final_op);
}


udf_optype
udf_finish_op(udf_record* urecord)
{
	if (udf_zero_bins_left(urecord)) {
		// Amazingly, with respect to stored key, memory statistics work out
		// correctly regardless of what this returns.
		return udf_finish_delete(urecord);
	}

	if ((urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) != 0) {
		if ((urecord->flag & UDF_RECORD_FLAG_OPEN) == 0) {
			cf_crash(AS_UDF, "updated record not open");
		}

		return UDF_OPTYPE_WRITE;
	}

	return UDF_OPTYPE_READ;
}


void
udf_post_processing(udf_record* urecord, rw_request* rw, udf_optype urecord_op)
{
	as_storage_rd* rd = urecord->rd;
	as_transaction* tr = urecord->tr;
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	uint16_t generation = 0;
	uint16_t set_id = 0;
	xdr_dirty_bins dirty_bins;

	if (urecord_op == UDF_OPTYPE_WRITE || urecord_op == UDF_OPTYPE_DELETE) {
		as_msg* m = &tr->msgp->msg;

		// Convert message TTL special value if appropriate.
		if (m->record_ttl == TTL_DONT_UPDATE &&
				(urecord->flag & UDF_RECORD_FLAG_PREEXISTS) == 0) {
			m->record_ttl = TTL_NAMESPACE_DEFAULT;
		}

		update_metadata_in_index(tr, r);

		// TODO - old pickle - remove in "six months".
		if (as_exchange_min_compatibility_id() < 3) {
			rw->is_old_pickle = true;
			pickle_all(rd, rw);
		}

		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		// Store or drop the key as appropriate.
		as_record_finalize_key(r, ns, rd->key, rd->key_size);

		as_storage_record_adjust_mem_stats(rd, urecord->starting_memory_bytes);

		will_replicate(r, ns);

		// Collect information for XDR before closing the record.
		generation = plain_generation(r->generation, ns);
		set_id = as_index_get_set_id(r);

		if (urecord->dirty && urecord_op == UDF_OPTYPE_WRITE) {
			xdr_clear_dirty_bins(&dirty_bins);
			xdr_copy_dirty_bins(urecord->dirty, &dirty_bins);
		}
	}

	// Will we need a pickle?
	// TODO - old pickle - remove condition in "six months".
	if (! rw->is_old_pickle) {
		rd->keep_pickle = rw->n_dest_nodes != 0;
	}

	// Close the record for all the cases.
	udf_record_close(urecord);

	// TODO - old pickle - remove condition in "six months".
	if (! rw->is_old_pickle) {
		// Yes, it's safe to use these urecord fields after udf_record_close().
		rw->pickle = urecord->pickle;
		rw->pickle_sz = urecord->pickle_sz;
	}

	// Write to XDR pipe.
	if (urecord_op == UDF_OPTYPE_WRITE) {
		xdr_write(tr->rsv.ns, &tr->keyd, generation, 0, XDR_OP_TYPE_WRITE,
				set_id, &dirty_bins);
	}
	else if (urecord_op == UDF_OPTYPE_DELETE) {
		xdr_write(tr->rsv.ns, &tr->keyd, 0, 0,
				as_transaction_is_durable_delete(tr) ?
						XDR_OP_TYPE_DURABLE_DELETE : XDR_OP_TYPE_DROP,
				set_id, NULL);
	}
}


bool
udf_timer_timedout(const as_timer* timer)
{
	uint64_t now = cf_getns();

	if (now < g_end_ns) {
		return false;
	}

	cf_warning(AS_UDF, "UDF timed out %lu ms ago", (now - g_end_ns) / 1000000);

	return true;
}


uint64_t
udf_timer_timeslice(const as_timer* timer)
{
	uint64_t now = cf_getns();

	return g_end_ns > now ? (g_end_ns - now) / 1000000 : 1;
}


//==========================================================
// Local helpers - statistics.
//

void
update_lua_complete_stats(uint8_t origin, as_namespace* ns, udf_optype op,
		int ret, bool is_success)
{
	switch (origin) {
	case FROM_CLIENT:
		if (ret == 0 && is_success) {
			if (op == UDF_OPTYPE_READ) {
				cf_atomic64_incr(&ns->n_client_lang_read_success);
			}
			else if (op == UDF_OPTYPE_DELETE) {
				cf_atomic64_incr(&ns->n_client_lang_delete_success);
			}
			else if (op == UDF_OPTYPE_WRITE) {
				cf_atomic64_incr(&ns->n_client_lang_write_success);
			}
		}
		else {
			cf_info(AS_UDF, "lua error, ret:%d", ret);
			cf_atomic64_incr(&ns->n_client_lang_error);
		}
		break;
	case FROM_PROXY:
		if (ret == 0 && is_success) {
			if (op == UDF_OPTYPE_READ) {
				cf_atomic64_incr(&ns->n_from_proxy_lang_read_success);
			}
			else if (op == UDF_OPTYPE_DELETE) {
				cf_atomic64_incr(&ns->n_from_proxy_lang_delete_success);
			}
			else if (op == UDF_OPTYPE_WRITE) {
				cf_atomic64_incr(&ns->n_from_proxy_lang_write_success);
			}
		}
		else {
			cf_info(AS_UDF, "lua error, ret:%d", ret);
			cf_atomic64_incr(&ns->n_from_proxy_lang_error);
		}
		break;
	case FROM_IUDF:
		if (ret == 0 && is_success) {
			if (op == UDF_OPTYPE_READ) {
				// Note - this would be weird, since there's nowhere for a
				// response to go in our current UDF scans & queries.
				cf_atomic64_incr(&ns->n_udf_sub_lang_read_success);
			}
			else if (op == UDF_OPTYPE_DELETE) {
				cf_atomic64_incr(&ns->n_udf_sub_lang_delete_success);
			}
			else if (op == UDF_OPTYPE_WRITE) {
				cf_atomic64_incr(&ns->n_udf_sub_lang_write_success);
			}
		}
		else {
			cf_info(AS_UDF, "lua error, ret:%d", ret);
			cf_atomic64_incr(&ns->n_udf_sub_lang_error);
		}
		break;
	default:
		cf_crash(AS_UDF, "unexpected transaction origin %u", origin);
		break;
	}
}


//==========================================================
// Local helpers - construct response to be sent to origin.
//

void
process_failure_str(udf_call* call, const char* err_str, size_t len,
		cf_dyn_buf* db)
{
	if (! err_str) {
		// Better than sending an as_string with null value.
		process_failure(call, NULL, db);
		return;
	}

	as_string stack_s;
	as_string_init_wlen(&stack_s, (char*)err_str, len, false);

	process_failure(call, as_string_toval(&stack_s), db);
}


void
process_result(const as_result* result, udf_call* call, cf_dyn_buf* db)
{
	as_val* val = result->value;

	if (result->is_success) {
		process_success(call, val, db);
		return;
	}

	// Failures...

	if (as_val_type(val) == AS_STRING) {
		call->tr->result_code = AS_ERR_UDF_EXECUTION;
		process_failure(call, val, db);
		return;
	}

	char lua_err_str[1024];
	size_t len = (size_t)sprintf(lua_err_str,
			"%s:0: in function %s() - error() argument type not handled",
			call->def->filename, call->def->function);

	call->tr->result_code = AS_ERR_UDF_EXECUTION;
	process_failure_str(call, lua_err_str, len, db);
}


void
process_response(udf_call* call, bool success, const as_val* val,
		cf_dyn_buf* db)
{
	// No response for background (internal) UDF.
	if (call->def->type == AS_UDF_OP_BACKGROUND) {
		return;
	}

	as_transaction* tr = call->tr;

	// Note - this function quietly handles a null val. The response call will
	// be given a bin with a name but not 'in use', and it does the right thing.

	size_t msg_sz = 0;

	db->buf = (uint8_t *)as_msg_make_val_response(success, val, tr->result_code,
			tr->generation, tr->void_time, as_transaction_trid(tr), &msg_sz);

	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;
}
