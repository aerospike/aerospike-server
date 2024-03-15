/*
 * transaction.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

/*
 * Operations on transactions
 */

#include "base/transaction.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "socket.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/service.h"
#include "base/stats.h"
#include "fabric/partition.h"
#include "transaction/proxy.h"
#include "transaction/rw_request.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"
#include "transaction/write.h"


void
as_transaction_init_head(as_transaction *tr, const cf_digest *keyd,
		cl_msg *msgp)
{
	tr->msgp				= msgp;
	tr->msg_fields			= 0;

	tr->origin				= 0;
	tr->from_flags			= 0;

	tr->from.any			= NULL;
	tr->from_data.any		= 0;

	tr->keyd				= keyd ? *keyd : cf_digest_zero;

	tr->start_time			= 0;
	tr->benchmark_time		= 0;
}

void
as_transaction_init_body(as_transaction *tr)
{
	AS_PARTITION_RESERVATION_INIT(tr->rsv);

	tr->end_time			= 0;
	tr->result_code			= AS_OK;
	tr->flags				= 0;
	tr->generation			= 0;
	tr->void_time			= 0;
	tr->last_update_time	= 0;
}

void
as_transaction_copy_head(as_transaction *to, const as_transaction *from)
{
	to->msgp				= from->msgp;
	to->msg_fields			= from->msg_fields;

	to->origin				= from->origin;
	to->from_flags			= from->from_flags;

	to->from.any			= from->from.any;
	to->from_data.any		= from->from_data.any;

	to->keyd				= from->keyd;

	to->start_time			= from->start_time;
	to->benchmark_time		= from->benchmark_time;
}

void
as_transaction_init_from_rw(as_transaction *tr, rw_request *rw)
{
	as_transaction_init_head_from_rw(tr, rw);
	// Note - we don't clear rw->msgp, destructor will free it.

	as_partition_reservation_copy(&tr->rsv, &rw->rsv);
	// Note - destructor will still release the reservation.

	tr->end_time = rw->end_time;
	tr->result_code = rw->result_code;
	tr->flags = rw->flags;
	tr->generation = rw->generation;
	tr->void_time = rw->void_time;
	tr->last_update_time = rw->last_update_time;
}

void
as_transaction_init_head_from_rw(as_transaction *tr, rw_request *rw)
{
	tr->msgp				= rw->msgp;
	tr->msg_fields			= rw->msg_fields;
	tr->origin				= rw->origin;
	tr->from_flags			= rw->from_flags;
	tr->from.any			= rw->from.any;
	tr->from_data.any		= rw->from_data.any;
	tr->keyd				= rw->keyd;
	tr->start_time			= rw->start_time;
	tr->benchmark_time		= rw->benchmark_time;

	rw->from.any = NULL;
	// Note - we don't clear rw->msgp, destructor will free it.
}

bool
as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type)
{
	switch (type) {
	case AS_MSG_FIELD_TYPE_NAMESPACE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_NAMESPACE;
		break;
	case AS_MSG_FIELD_TYPE_SET:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SET;
		break;
	case AS_MSG_FIELD_TYPE_KEY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_KEY;
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_RIPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_RIPE;
		break;
	case AS_MSG_FIELD_TYPE_TRID:
		tr->msg_fields |= AS_MSG_FIELD_BIT_TRID;
		break;
	case AS_MSG_FIELD_TYPE_SOCKET_TIMEOUT:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SOCKET_TIMEOUT;
		break;
	case AS_MSG_FIELD_TYPE_RECS_PER_SEC:
		tr->msg_fields |= AS_MSG_FIELD_BIT_RECS_PER_SEC;
		break;
	case AS_MSG_FIELD_TYPE_PID_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_PID_ARRAY;
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_ARRAY;
		break;
	case AS_MSG_FIELD_TYPE_SAMPLE_MAX:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SAMPLE_MAX;
		break;
	case AS_MSG_FIELD_TYPE_LUT:
		tr->msg_fields |= AS_MSG_FIELD_BIT_LUT;
		break;
	case AS_MSG_FIELD_TYPE_BVAL_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_BVAL_ARRAY;
		break;
	case AS_MSG_FIELD_TYPE_INDEX_RANGE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_RANGE;
		break;
	case AS_MSG_FIELD_TYPE_INDEX_TYPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_TYPE;
		break;
	case AS_MSG_FIELD_TYPE_UDF_FILENAME:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FILENAME;
		break;
	case AS_MSG_FIELD_TYPE_UDF_FUNCTION:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FUNCTION;
		break;
	case AS_MSG_FIELD_TYPE_UDF_ARGLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_ARGLIST;
		break;
	case AS_MSG_FIELD_TYPE_UDF_OP:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_OP;
		break;
	case AS_MSG_FIELD_TYPE_QUERY_BINLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_QUERY_BINLIST;
		break;
	case AS_MSG_FIELD_TYPE_BATCH: // shouldn't get here - batch parent handles this
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH;
		break;
	case AS_MSG_FIELD_TYPE_BATCH_WITH_SET: // shouldn't get here - batch parent handles this
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH_WITH_SET;
		break;
	case AS_MSG_FIELD_TYPE_PREDEXP:
		tr->msg_fields |= AS_MSG_FIELD_BIT_PREDEXP;
		break;
	default:
		return false;
	}

	return true;
}

bool
as_transaction_prepare(as_transaction *tr, bool swap)
{
	uint64_t size = tr->msgp->proto.sz;

	if (size < sizeof(as_msg)) {
		cf_warning(AS_PROTO, "proto body size %lu smaller than as_msg", size);
		return false;
	}

	// The proto data is not smaller than an as_msg - safe to swap header.
	as_msg *m = &tr->msgp->msg;

	if (swap) {
		as_msg_swap_header(m);
	}

	uint8_t* p_end = (uint8_t*)m + size;
	uint8_t* p_read = m->data;

	// Parse and swap fields first.
	for (uint16_t n = 0; n < m->n_fields; n++) {
		if (p_read + sizeof(as_msg_field) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_field");
			return false;
		}

		as_msg_field* p_field = (as_msg_field*)p_read;

		if (swap) {
			as_msg_swap_field(p_field);
		}

		if (! (p_read = as_msg_field_skip(p_field))) {
			cf_warning(AS_PROTO, "bad as_msg_field");
			return false;
		}

		// Store which message fields are present - prevents lots of re-parsing.
		if (! as_transaction_set_msg_field_flag(tr, p_field->type)) {
			cf_debug(AS_PROTO, "skipping as_msg_field type %u", p_field->type);
		}
	}

	// Parse and swap bin-ops, if any.
	for (uint16_t n = 0; n < m->n_ops; n++) {
		if (p_read + sizeof(as_msg_op) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg");
			return false;
		}

		as_msg_op* op = (as_msg_op*)p_read;

		if (swap) {
			// Swap can touch metadata bytes beyond as_msg_op struct.
			if (as_msg_op_get_value_p(op) > p_end) {
				cf_warning(AS_PROTO, "bad as_msg_op");
				return false;
			}

			as_msg_swap_op(op);
		}

		if (! (p_read = as_msg_op_skip(op))) {
			cf_warning(AS_PROTO, "bad as_msg_op");
			return false;
		}
	}

	if (p_read != p_end) {
		cf_warning(AS_PROTO, "extra bytes follow fields and bin-ops");
		return false;
	}

	return true;
}

// Initialize an internal UDF transaction (for a UDF query). Uses shared message
// with namespace but no digest, and no set for now since these transactions
// won't get security checked, and can't create a record.
void
as_transaction_init_iudf(as_transaction *tr, as_namespace *ns, cf_digest *keyd,
		iudf_origin* iudf_orig)
{
	// Note - digest is on transaction head before it's enqueued.
	as_transaction_init_head(tr, keyd, iudf_orig->msgp);

	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_NAMESPACE);

	tr->origin = FROM_IUDF;
	tr->from.iudf_orig = iudf_orig;

	tr->start_time = cf_getns();
}

// Initialize an internal ops transaction (for an ops query). Uses shared
// message with namespace but no digest, and no set for now since these
// transactions won't get security checked, and can't create a record.
void
as_transaction_init_iops(as_transaction *tr, as_namespace *ns, cf_digest *keyd,
		iops_origin* iops_orig)
{
	// Note - digest is on transaction head before it's enqueued.
	as_transaction_init_head(tr, keyd, iops_orig->msgp);

	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_NAMESPACE);

	tr->origin = FROM_IOPS;
	tr->from.iops_orig = iops_orig;

	tr->start_time = cf_getns();
}

void
as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code)
{
	as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, 0);
	tr->from.proto_fd_h = NULL;

	cf_free(tr->msgp);
	tr->msgp = NULL;

	as_incr_uint64(&g_stats.n_demarshal_error);
}

#define UPDATE_ERROR_STATS(name) \
	if (ns) { \
		if (error_code == AS_ERR_TIMEOUT) { \
			as_incr_uint64(&ns->n_##name##_tsvc_timeout); \
		} \
		else { \
			as_incr_uint64(&ns->n_##name##_tsvc_error); \
		} \
	} \
	else { \
		as_incr_uint64(&g_stats.n_tsvc_##name##_error); \
	}

void
as_transaction_error(as_transaction* tr, as_namespace* ns, uint32_t error_code)
{
	if (error_code == 0) {
		cf_warning(AS_PROTO, "converting error code 0 to 1 (unknown)");
		error_code = AS_ERR_UNKNOWN;
	}

	// The 'from' checks below are unnecessary, only paranoia.
	switch (tr->origin) {
	case FROM_CLIENT:
		if (tr->from.proto_fd_h) {
			as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proto_fd_h = NULL; // pattern, not needed
		}
		UPDATE_ERROR_STATS(client);
		break;
	case FROM_PROXY:
		if (tr->from.proxy_node != 0) {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proxy_node = 0; // pattern, not needed
		}
		if (as_transaction_is_batch_sub(tr)) {
			UPDATE_ERROR_STATS(from_proxy_batch_sub);
		}
		else {
			UPDATE_ERROR_STATS(from_proxy);
		}
		break;
	case FROM_BATCH:
		if (tr->from.batch_shared) {
			as_batch_add_error(tr->from.batch_shared, tr->from_data.batch_index, error_code);
			tr->from.batch_shared = NULL; // pattern, not needed
			tr->msgp = NULL; // pattern, not needed
		}
		UPDATE_ERROR_STATS(batch_sub);
		break;
	case FROM_IUDF:
		if (tr->from.iudf_orig) {
			tr->from.iudf_orig->done_cb(tr->from.iudf_orig->udata, error_code);
			tr->from.iudf_orig = NULL; // pattern, not needed
		}
		UPDATE_ERROR_STATS(udf_sub);
		break;
	case FROM_IOPS:
		if (tr->from.iops_orig) {
			tr->from.iops_orig->done_cb(tr->from.iops_orig->udata, error_code);
			tr->from.iops_orig = NULL; // pattern, not needed
		}
		UPDATE_ERROR_STATS(ops_sub);
		break;
	case FROM_READ_TOUCH:
		tr->from.read_touch_active = NULL; // pattern, not needed
		UPDATE_ERROR_STATS(read_touch);
		break;
	case FROM_RE_REPL:
		if (tr->from.re_repl_orig_cb) {
			tr->from.re_repl_orig_cb(tr);
			tr->from.re_repl_orig_cb = NULL; // pattern, not needed
		}
		UPDATE_ERROR_STATS(re_repl);
		break;
	default:
		cf_crash(AS_PROTO, "unexpected transaction origin %u", tr->origin);
		break;
	}
}

// TODO - temporary, until query can do its own synchronous failure responses.
// (Here we forfeit namespace info and add to global-scope error.)
void
as_multi_rec_transaction_error(as_transaction* tr, uint32_t error_code)
{
	if (error_code == 0) {
		cf_warning(AS_PROTO, "converting error code 0 to 1 (unknown)");
		error_code = AS_ERR_UNKNOWN;
	}

	switch (tr->origin) {
	case FROM_CLIENT:
		if (tr->from.proto_fd_h) {
			as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proto_fd_h = NULL; // pattern, not needed
		}
		as_incr_uint64(&g_stats.n_tsvc_client_error);
		break;
	default:
		cf_crash(AS_PROTO, "unexpected transaction origin %u", tr->origin);
		break;
	}
}

// Helper to release transaction file handles.
void
as_release_file_handle(as_file_handle *proto_fd_h)
{
	if (cf_rc_release(proto_fd_h) != 0) {
		return;
	}

	cf_socket_close(&proto_fd_h->sock);
	cf_socket_term(&proto_fd_h->sock);

	if (proto_fd_h->proto != NULL) {
		cf_free(proto_fd_h->proto);
	}

	if (proto_fd_h->security_filter != NULL) {
		as_security_filter_destroy(proto_fd_h->security_filter);
	}

	cf_rc_free(proto_fd_h);

	as_incr_uint64_rls(&g_stats.proto_connections_closed);
}

void
as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close)
{
	if (force_close) {
		cf_socket_shutdown(&proto_fd_h->sock);
	}

	// Rearm first.
	as_service_rearm(proto_fd_h);

	// Now allow service threads to exit and close the epoll instance.
	as_decr_uint32_rls(&proto_fd_h->in_transaction);
}

void
as_end_of_transaction_ok(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, false);
}

void
as_end_of_transaction_force_close(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, true);
}
