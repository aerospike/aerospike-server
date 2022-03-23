/*
 * udf.c
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

#include "transaction/udf.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_atomic.h"
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
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/udf_aerospike.h"
#include "base/udf_arglist.h"
#include "base/udf_cask.h"
#include "base/udf_record.h"
#include "base/xdr.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "sindex/secondary_index.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef enum {
	UDF_OPTYPE_NONE,
	UDF_OPTYPE_WAITING,
	UDF_OPTYPE_READ,
	UDF_OPTYPE_WRITE,
	UDF_OPTYPE_DELETE
} udf_optype;

typedef struct udf_call_s {
	udf_def* def;
	as_transaction* tr;
} udf_call;

static const cf_log_level as_log_level_map[] = {
	[AS_LOG_LEVEL_ERROR] = CF_WARNING,
	[AS_LOG_LEVEL_WARN] = CF_WARNING,
	[AS_LOG_LEVEL_INFO] = CF_INFO,
	[AS_LOG_LEVEL_DEBUG] = CF_DEBUG,
	[AS_LOG_LEVEL_TRACE] = CF_DETAIL
};


//==========================================================
// Globals.
//

static as_aerospike g_as_aerospike;

// Deadline per UDF.
static __thread uint64_t g_end_ns;


//==========================================================
// Forward declarations.
//

static bool log_callback(as_log_level level, const char* func, const char* file, uint32_t line, const char* fmt, ...);

static void start_udf_dup_res(rw_request* rw, as_transaction* tr);
static void start_udf_repl_write(rw_request* rw, as_transaction* tr);
static void start_udf_repl_write_forget(rw_request* rw, as_transaction* tr);
static bool udf_dup_res_cb(rw_request* rw);
static void udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
static void udf_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr);
static void udf_repl_write_cb(rw_request* rw);

static void send_udf_response(as_transaction* tr, cf_dyn_buf* db);
static void udf_timeout_cb(rw_request* rw);

static transaction_status udf_master(rw_request* rw, as_transaction* tr);
static udf_optype udf_master_apply(udf_call* call, rw_request* rw);
static uint8_t open_existing_record(udf_record* urecord);
static int udf_apply_record(udf_call* call, as_rec* rec, as_result* result);
static bool udf_timer_timedout(const as_timer* timer);
static uint64_t udf_timer_timeslice(const as_timer* timer);
static uint8_t udf_master_write(udf_record* urecord, rw_request* rw);
static void udf_update_sindex(udf_record* urecord);

static void udf_master_failed(udf_record* urecord, as_rec* urec, as_result* result, uint8_t result_code, cf_dyn_buf* db);
static void udf_master_done(udf_record* urecord, as_rec* urec, as_result* result, cf_dyn_buf* db);

static void update_lua_failure_stats(uint8_t origin, as_namespace* ns, const as_result* result);
static void update_lua_success_stats(uint8_t origin, as_namespace* ns, udf_optype op);

static void process_failure(as_transaction* tr, const as_result* result, cf_dyn_buf* db);
static void process_response(as_transaction* tr, bool success, const as_val* val, cf_dyn_buf* db);


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
	default:
		cf_atomic64_incr(&ns->n_client_udf_error);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_udf_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		cf_atomic64_incr(&ns->n_client_udf_filtered_out);
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
	default:
		cf_atomic64_incr(&ns->n_from_proxy_udf_error);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_from_proxy_udf_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		cf_atomic64_incr(&ns->n_from_proxy_udf_filtered_out);
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
	default:
		cf_atomic64_incr(&ns->n_udf_sub_udf_error);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_udf_sub_udf_timeout);
		break;
	case AS_ERR_FILTERED_OUT: // doesn't include those filtered out by metadata
		as_incr_uint64(&ns->n_udf_sub_udf_filtered_out);
		break;
	}
}

static inline bool
has_forbidden_policy(const as_msg* m)
{
	return	(m->info2 & (
					AS_MSG_INFO2_GENERATION |
					AS_MSG_INFO2_GENERATION_GT |
					AS_MSG_INFO2_CREATE_ONLY)) != 0 ||
			(m->info3 & (
					AS_MSG_INFO3_UPDATE_ONLY |
					AS_MSG_INFO3_CREATE_OR_REPLACE |
					AS_MSG_INFO3_REPLACE_ONLY)) != 0;
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

// Public API for udf_def class, not big enough for its own file.
bool
udf_def_init_from_msg(udf_def* def, const as_transaction* tr)
{
	def->arglist = NULL;

	as_msg* m = &tr->msgp->msg;
	as_msg_field* filename =
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_FILENAME);

	if (filename == NULL) {
		return false;
	}

	as_msg_field* function =
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_FUNCTION);

	if (function == NULL) {
		return false;
	}

	as_msg_field* arglist = as_msg_field_get(m, AS_MSG_FIELD_TYPE_UDF_ARGLIST);

	if (arglist == NULL) {
		return false;
	}

	uint32_t filename_len = as_msg_field_get_value_sz(filename);

	if (filename_len >= sizeof(def->filename)) {
		return false;
	}

	uint32_t function_len = as_msg_field_get_value_sz(function);

	if (function_len >= sizeof(def->function)) {
		return false;
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

	def->type = (uint8_t)(op != NULL ? *op->data : AS_UDF_OP_KVS);

	return true;
}

transaction_status
as_udf_start(as_transaction* tr)
{
	BENCHMARK_START(tr, udf, FROM_CLIENT);
	BENCHMARK_START(tr, udf_sub, FROM_IUDF);

	// Temporary security vulnerability protection.
	if (g_config.udf_execution_disabled) {
		tr->result_code = AS_ERR_FORBIDDEN;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_ERR_FORBIDDEN;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Don't know if UDF is read or delete - check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns, 0, "udf")) {
		tr->result_code = AS_ERR_DEVICE_OVERLOAD;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->ix, tr->keyd };
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

static bool
log_callback(as_log_level level, const char* func, const char* file,
		uint32_t line, const char* fmt, ...)
{
	(void)func;

	cf_log_level severity = as_log_level_map[level];

	if (! cf_log_check_level(AS_UDF, severity)) {
		return true;
	}

	va_list ap;

	va_start(ap, fmt);
	char message[1024] = { '\0' };
	vsnprintf(message, 1024, fmt, ap);
	va_end(ap);

	cf_log_write(AS_UDF, severity, file, (int)line, "%s", message);

	return true;
}


//==========================================================
// Local helpers - transaction flow.
//

static void
start_udf_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, udf_dup_res_cb, udf_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_udf_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, udf_repl_write_cb, udf_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_udf_repl_write_forget(rw_request* rw, as_transaction* tr)
{
	// Construct and send repl-write message. No need to finish rw setup.

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

static bool
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

static void
udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, udf_repl_write_cb);
	send_rw_messages(rw);
}

static void
udf_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Send replica writes. Not waiting for acks, so need to reset rw_request.
	// Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

static void
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

static void
send_udf_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (tr->from.any == NULL) {
		cf_warning(AS_UDF, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	clear_delete_response_metadata(tr);

	switch (tr->origin) {
	case FROM_CLIENT:
		if (db != NULL && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, NULL, NULL, 0, tr->rsv.ns,
					as_transaction_trid(tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, udf, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, udf_hist);
		client_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		if (db != NULL && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, NULL, NULL,
					0, tr->rsv.ns, as_transaction_trid(tr));
		}
		from_proxy_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_IUDF:
		if (db != NULL && db->used_sz != 0) {
			cf_crash(AS_UDF, "unexpected - internal udf has response");
		}
		tr->from.iudf_orig->cb(tr->from.iudf_orig->udata, tr->result_code);
		BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, response);
		udf_sub_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	default:
		cf_crash(AS_UDF, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}

static void
udf_timeout_cb(rw_request* rw)
{
	if (rw->from.any == NULL) {
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
		cf_crash(AS_UDF, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - apply UDF on master.
//

static transaction_status
udf_master(rw_request* rw, as_transaction* tr)
{
	CF_ALLOC_SET_NS_ARENA_DIM(tr->rsv.ns);

	udf_def def;
	udf_call call = { .def = &def, .tr = tr };

	if (tr->origin == FROM_IUDF) {
		call.def = &tr->from.iudf_orig->def;
	}
	else if (! udf_def_init_from_msg(call.def, tr)) {
		cf_warning(AS_UDF, "failed udf_def_init_from_msg");
		tr->result_code = AS_ERR_PARAMETER;
		return TRANS_DONE_ERROR;
	}

	udf_optype optype = udf_master_apply(&call, rw);

	if (tr->origin != FROM_IUDF && call.def->arglist != NULL) {
		as_list_destroy(call.def->arglist);
	}

	if (optype == UDF_OPTYPE_READ || optype == UDF_OPTYPE_NONE) {
		// UDF is done, no replica writes needed.
		// TODO - linearized reads need to start replica ping here.
		return TRANS_DONE_SUCCESS;
	}

	return optype == UDF_OPTYPE_WAITING ? TRANS_WAITING : TRANS_IN_PROGRESS;
}

static udf_optype
udf_master_apply(udf_call* call, rw_request* rw)
{
	as_transaction* tr = call->tr;
	as_namespace* ns = tr->rsv.ns;
	cf_dyn_buf* db = &rw->response_db;

	if (has_forbidden_policy(&tr->msgp->msg)) {
		cf_warning(AS_UDF, "udf applied with forbidden policy");
		tr->result_code = AS_ERR_PARAMETER;
		return UDF_OPTYPE_NONE;
	}

	as_index_ref r_ref;
	int get_rv = as_record_get(tr->rsv.tree, &tr->keyd, &r_ref);

	as_record* r = r_ref.r; // only valid if get_rv == 0

	// If record is expired or truncated, pretend it was not found.
	if (get_rv == 0 && as_record_is_doomed(r, ns)) {
		as_record_done(&r_ref, ns);
		get_rv = -1;
	}

	if (get_rv == 0 && repl_state_check(r, tr) < 0) {
		as_record_done(&r_ref, ns);
		return UDF_OPTYPE_WAITING;
	}

	// Internal UDFs must not create records.
	if (tr->origin == FROM_IUDF && (get_rv != 0 || ! as_record_is_live(r))) {
		if (get_rv == 0) {
			as_record_done(&r_ref, ns);
		}

		tr->result_code = AS_ERR_NOT_FOUND;
		return UDF_OPTYPE_NONE;
	}

	udf_record urecord;
	udf_record_init(&urecord);

	urecord.tr = tr;

	//------------------------------------------------------
	// Open storage record if the record exists.
	//

	as_storage_rd rd;

	urecord.r_ref = &r_ref;
	urecord.rd = &rd;

	if (get_rv == 0) {
		uint8_t open_rv = open_existing_record(&urecord);

		if (open_rv != AS_OK) {
			udf_master_failed(&urecord, NULL, NULL, open_rv, NULL);
			return UDF_OPTYPE_NONE;
		}
	}
	// else - may create record via UDF - urecord.is_open still false.

	//------------------------------------------------------
	// Apply the UDF function.
	//

	// This as_rec needs to be in the heap - once passed into the lua scope it
	// gets garbage collected later. Also, the destroy hook is set to NULL so
	// garbage collection has nothing to do.
	as_rec* urec = as_rec_new(&urecord, &udf_record_hooks);

	as_val_reserve(urec); // for lua

	as_result result;
	as_result_init(&result);

	int apply_rv = udf_apply_record(call, urec, &result);

	//------------------------------------------------------
	// Handle UDF execution failures or update failures.
	//

	// If we didn't even apply our UDF function - i.e. we couldn't set up the
	// UDF execution environment...
	if (apply_rv != 0) {
		char* rs = as_module_err_string(apply_rv);

		as_string stack_s;
		as_string_init_wlen(&stack_s, rs, strlen(rs), true);

		// Note - destroying result will free the string value.
		result.value = as_string_toval(&stack_s);

		update_lua_failure_stats(tr->origin, ns, &result);
		udf_master_failed(&urecord, urec, &result, AS_ERR_UDF_EXECUTION, db);
		return UDF_OPTYPE_NONE;
	}

	// If UDF environment setup succeeded, but as_result reports failure...
	if (! result.is_success) {
		update_lua_failure_stats(tr->origin, ns, &result);
		udf_master_failed(&urecord, urec, &result, AS_ERR_UDF_EXECUTION, db);
		return UDF_OPTYPE_NONE;
	}

	// If as_result reports success, but execute_updates() failed...
	if (urecord.result_code != AS_OK) {
		udf_master_failed(&urecord, urec, &result, urecord.result_code, db);
		return UDF_OPTYPE_NONE;
	}

	//------------------------------------------------------
	// UDF success - try to apply cached updates, if any.
	//

	udf_optype final_op;

	if (urecord.has_updates) {
		// Can't use original r shortcut - only good if get_rv == 0.
		as_record* safe_r = rd.r;

		// Save for XDR submit.
		uint64_t prev_lut = safe_r->last_update_time;

		// Write the record to storage.
		uint8_t write_rv = udf_master_write(&urecord, rw);

		if (write_rv != AS_OK) {
			udf_master_failed(&urecord, urec, &result, write_rv, db);
			return UDF_OPTYPE_NONE;
		}

		final_op = rd.n_bins == 0 ? UDF_OPTYPE_DELETE : UDF_OPTYPE_WRITE;

		// Save for XDR submit outside record lock.
		as_xdr_submit_info submit_info;

		as_xdr_get_submit_info(safe_r, prev_lut, &submit_info);

		udf_master_done(&urecord, urec, &result, db);

		if (! write_is_full_drop(tr)) {
			as_xdr_submit(ns, &submit_info);
		}
	}
	else {
		final_op = UDF_OPTYPE_READ;
		udf_master_done(&urecord, urec, &result, db);
	}

	update_lua_success_stats(tr->origin, ns, final_op);

	return final_op;
}

static uint8_t
open_existing_record(udf_record* urecord)
{
	urecord->is_open = true;

	as_transaction* tr = urecord->tr;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = urecord->r_ref->r;

	int rv;
	as_exp* filter_exp = NULL;

	// Handle metadata filter if present.
	if (as_record_is_live(r) &&
			(rv = handle_meta_filter(tr, r, &filter_exp)) != 0) {
		return (uint8_t)rv;
	}

	// Apply record bins filter if present.
	if (filter_exp != NULL) {
		if ((rv = udf_record_load(urecord)) != 0) {
			cf_warning(AS_UDF, "record failed load");
			destroy_filter_exp(tr, filter_exp);
			return (uint8_t)rv;
		}

		as_exp_ctx ctx = { .ns = ns, .r = r, .rd = urecord->rd };

		if (! as_exp_matches_record(filter_exp, &ctx)) {
			destroy_filter_exp(tr, filter_exp);
			return AS_ERR_FILTERED_OUT;
		}

		destroy_filter_exp(tr, filter_exp);
	}

	if (as_transaction_has_key(tr)) {
		if ((rv = udf_record_load(urecord)) != 0) {
			cf_warning(AS_UDF, "record failed load");
			return (uint8_t)rv;
		}

		as_storage_rd* rd = urecord->rd;

		if (rd->key != NULL) {
			// If both the record and the message have keys, check them.
			if (! check_msg_key(&tr->msgp->msg, rd)) {
				return AS_ERR_KEY_MISMATCH;
			}
		}
		else {
			// If the message has a key, it will now be stored with the record.
			if (! get_msg_key(tr, rd)) {
				return AS_ERR_UNSUPPORTED_FEATURE;
			}
		}
	}

	return AS_OK;
}

static int
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

static bool
udf_timer_timedout(const as_timer* timer)
{
	(void)timer;

	uint64_t now = cf_getns();

	if (now < g_end_ns) {
		return false;
	}

	cf_warning(AS_UDF, "UDF timed out %lu ms ago", (now - g_end_ns) / 1000000);

	return true;
}

static uint64_t
udf_timer_timeslice(const as_timer* timer)
{
	(void)timer;

	uint64_t now = cf_getns();

	return g_end_ns > now ? (g_end_ns - now) / 1000000 : 1;
}

static uint8_t
udf_master_write(udf_record* urecord, rw_request* rw)
{
	as_transaction* tr = urecord->tr;
	as_namespace* ns = tr->rsv.ns;
	as_storage_rd* rd = urecord->rd;
	as_record* r = rd->r;

	// Set up the nodes to which we'll write replicas.
	if (! set_replica_destinations(tr, rw)) {
		return AS_ERR_UNAVAILABLE;
	}

	// Fire and forget can overload the fabric send queues - check.
	if (respond_on_master_complete(tr) &&
			as_fabric_is_overloaded(rw->dest_nodes, rw->n_dest_nodes,
					AS_FABRIC_CHANNEL_RW, 0)) {
		tr->flags |= AS_TRANSACTION_FLAG_SWITCH_TO_COMMIT_ALL;
	}

	// Will we need a pickle?
	rd->keep_pickle = rw->n_dest_nodes != 0;

	as_msg* m = &tr->msgp->msg;

	// Convert message TTL special value if appropriate.
	if (m->record_ttl == TTL_DONT_UPDATE && urecord->n_old_bins == 0) {
		m->record_ttl = TTL_NAMESPACE_DEFAULT;
	}

	if (! is_valid_ttl(m->record_ttl)) {
		cf_warning(AS_UDF, "invalid ttl %u", m->record_ttl);
		return AS_ERR_PARAMETER;
	}

	if (is_ttl_disallowed(m->record_ttl, ns)) {
		cf_ticker_warning(AS_UDF, "disallowed ttl with nsup-period 0");
		return AS_ERR_FORBIDDEN;
	}

	int result;
	bool is_delete = as_bin_empty_if_all_tombstones(rd,
			as_transaction_is_durable_delete(tr));

	if (is_delete) {
		if (urecord->n_old_bins == 0 || ! as_record_is_live(r)) {
			// Didn't exist or was bin cemetery (tombstone bit not yet updated).
			return AS_ERR_NOT_FOUND;
		}

		if ((result = validate_delete_durability(tr)) != AS_OK) {
			return (uint8_t)result;
		}
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index.
	//

	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	advance_record_version(tr, r);
	set_xdr_write(tr, r);
	transition_delete_metadata(tr, r, is_delete, is_delete && rd->n_bins != 0);

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_detail(AS_UDF, "{%s} failed write %pD", ns->name, &tr->keyd);
		unwind_index_metadata(&old_metadata, r);
		return (uint8_t)(-result);
	}

	as_record_transition_stats(r, ns, &old_metadata);
	as_record_transition_set_index(tr->rsv.tree, urecord->r_ref, ns, rd->n_bins,
			&old_metadata);
	pickle_all(rd, rw);

	//------------------------------------------------------
	// Success - adjust sindex, etc.
	//

	// Store or drop the key as appropriate.
	as_record_finalize_key(r, ns, rd->key, rd->key_size);

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			as_bin_destroy_all(urecord->cleanup_bins, urecord->n_cleanup_bins);
			as_single_bin_copy(as_index_get_single_bin(r), rd->bins);
		}
		else {
			udf_update_sindex(urecord);
			as_bin_destroy_all(urecord->cleanup_bins, urecord->n_cleanup_bins);
			as_storage_rd_update_bin_space(rd);
		}

		as_storage_record_adjust_mem_stats(rd, urecord->old_memory_bytes);
	}
	else {
		udf_update_sindex(urecord);
	}

	tr->generation = r->generation;
	tr->void_time = r->void_time;
	tr->last_update_time = r->last_update_time;

	// Handle deletion if appropriate.
	if (is_delete) {
		write_delete_record(r, tr->rsv.tree);
		tr->flags |= AS_TRANSACTION_FLAG_IS_DELETE;
	}
	// Or (normally) adjust max void-time.
	else if (r->void_time != 0) {
		cf_atomic32_setmax(&tr->rsv.p->max_void_time, (int32_t)r->void_time);
	}

	will_replicate(r, ns);

	return AS_OK;
}

static void
udf_update_sindex(udf_record* urecord) {
	as_namespace* ns = urecord->tr->rsv.ns;
	as_storage_rd* rd = urecord->rd;
	as_record* r = rd->r;

	if (set_has_sindex(r, ns)) {
		update_sindex(ns, urecord->r_ref, urecord->old_bins,
				urecord->n_old_bins, rd->bins, rd->n_bins);
	}
	else {
		// Sindex drop will leave in_sindex bit. Good opportunity to clear.
		as_index_clear_in_sindex(r);
	}
}


//==========================================================
// Local helpers - cleanup after applying UDF.
//

static void
udf_master_failed(udf_record* urecord, as_rec* urec, as_result* result,
		uint8_t result_code, cf_dyn_buf* db)
{
	as_transaction* tr = urecord->tr;
	as_namespace* ns = tr->rsv.ns;

	if (urecord->is_open) {
		as_index_ref* r_ref = urecord->r_ref;

		if (urecord->is_loaded) {
			as_storage_rd* rd = urecord->rd;

			if (urecord->has_updates && ns->storage_data_in_memory) {
				if (ns->single_bin) {
					write_dim_single_bin_unwind(urecord->old_bins,
							urecord->n_old_bins, rd->bins, rd->n_bins,
							urecord->cleanup_bins, urecord->n_cleanup_bins);
				}
				else {
					write_dim_unwind(urecord->old_bins, urecord->n_old_bins,
							rd->bins, rd->n_bins, urecord->cleanup_bins,
							urecord->n_cleanup_bins);
				}
			}

			if ((urecord->result_code != AS_OK || urecord->has_updates) &&
					urecord->n_old_bins == 0) {
				write_delete_record(rd->r, tr->rsv.tree);
			}

			as_storage_record_close(rd);
		}

		as_record_done(r_ref, ns);
	}

	switch (result_code) {
	// FIXME - add generation check?
	case AS_ERR_RECORD_TOO_BIG:
		cf_detail(AS_UDF, "{%s} record too big %pD", ns->name, &tr->keyd);
		cf_atomic64_incr(&ns->n_fail_record_too_big);
		break;
	default:
		// These either log warnings or aren't interesting enough to count.
		break;
	}

	tr->result_code = result_code; // must set before process_failure()

	if (result != NULL) {
		process_failure(tr, result, db);
		as_result_destroy(result);
	}

	if (urec != NULL) {
		as_rec_destroy(urec);
	}

	if (urecord->particle_buf != NULL) {
		cf_free(urecord->particle_buf);
	}

	udf_record_cache_free(urecord);
}

static void
udf_master_done(udf_record* urecord, as_rec* urec, as_result* result,
		cf_dyn_buf* db)
{
	as_transaction* tr = urecord->tr;
	as_namespace* ns = tr->rsv.ns;

	if (urecord->is_open) {
		if (urecord->is_loaded) {
			as_storage_record_close(urecord->rd);
		}

		as_record_done(urecord->r_ref, ns);
	}

	process_response(tr, true, result->value, db);

	as_result_destroy(result);
	as_rec_destroy(urec);

	if (urecord->particle_buf != NULL) {
		cf_free(urecord->particle_buf);
	}

	udf_record_cache_free(urecord);
}


//==========================================================
// Local helpers - statistics.
//

static void
update_lua_failure_stats(uint8_t origin, as_namespace* ns,
		const as_result* result)
{
	char* val_str = NULL;

	if (as_val_type(result->value) == AS_STRING) {
		val_str = as_val_tostring(result->value);
	}

	cf_warning(AS_UDF, "lua-error: result %s", val_str != NULL ?
			val_str : "<unexpected>");

	if (val_str != NULL) {
		cf_free(val_str);
	}

	switch (origin) {
	case FROM_CLIENT:
		cf_atomic64_incr(&ns->n_client_lang_error);
		break;
	case FROM_PROXY:
		cf_atomic64_incr(&ns->n_from_proxy_lang_error);
		break;
	case FROM_IUDF:
		cf_atomic64_incr(&ns->n_udf_sub_lang_error);
		break;
	default:
		cf_crash(AS_UDF, "unexpected transaction origin %u", origin);
		break;
	}
}

static void
update_lua_success_stats(uint8_t origin, as_namespace* ns, udf_optype op)
{
	switch (origin) {
	case FROM_CLIENT:
		if (op == UDF_OPTYPE_READ) {
			cf_atomic64_incr(&ns->n_client_lang_read_success);
		}
		else if (op == UDF_OPTYPE_DELETE) {
			cf_atomic64_incr(&ns->n_client_lang_delete_success);
		}
		else if (op == UDF_OPTYPE_WRITE) {
			cf_atomic64_incr(&ns->n_client_lang_write_success);
		}
		break;
	case FROM_PROXY:
		if (op == UDF_OPTYPE_READ) {
			cf_atomic64_incr(&ns->n_from_proxy_lang_read_success);
		}
		else if (op == UDF_OPTYPE_DELETE) {
			cf_atomic64_incr(&ns->n_from_proxy_lang_delete_success);
		}
		else if (op == UDF_OPTYPE_WRITE) {
			cf_atomic64_incr(&ns->n_from_proxy_lang_write_success);
		}
		break;
	case FROM_IUDF:
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
		break;
	default:
		cf_crash(AS_UDF, "unexpected transaction origin %u", origin);
		break;
	}
}


//==========================================================
// Local helpers - construct response to be sent to origin.
//

static void
process_failure(as_transaction* tr, const as_result* result, cf_dyn_buf* db)
{
	if (result->is_success) { // yes - this can happen
		static const char IGNORED[] = "result value ignored";
		as_string stack_s;

		as_string_init_wlen(&stack_s, (char*)IGNORED, sizeof(IGNORED) - 1,
				false);

		process_response(tr, false, as_string_toval(&stack_s), db);
		return;
	}

	as_val* val = result->value;

	if (as_val_type(val) != AS_STRING) {
		// Lua returned failure but not string - unexpected.
		static const char UNEXPECTED[] = "result value type unexpected";
		as_string stack_s;

		as_string_init_wlen(&stack_s, (char*)UNEXPECTED, sizeof(UNEXPECTED) - 1,
				false);

		process_response(tr, false, as_string_toval(&stack_s), db);
		return;
	}

	process_response(tr, false, val, db);
}

static void
process_response(as_transaction* tr, bool success, const as_val* val,
		cf_dyn_buf* db)
{
	// No response for background (internal) UDF.
	if (tr->origin == FROM_IUDF) {
		return;
	}

	// Note - this function quietly handles a null val. The response call will
	// be given a bin with a name but not 'in use', and it does the right thing.

	size_t msg_sz = 0;

	db->buf = (uint8_t*)as_msg_make_val_response(success, val, tr->result_code,
			tr->generation, tr->void_time, as_transaction_trid(tr), &msg_sz);

	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;
}
