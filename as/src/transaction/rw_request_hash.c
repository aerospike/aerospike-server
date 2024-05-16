/*
 * rw_request_hash.c
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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

#include "transaction/rw_request_hash.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"
#include "msg.h"
#include "node.h"
#include "rchash.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "fabric/fabric.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/replica_ping.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

const msg_template rw_mt[] = {
		{ RW_FIELD_OP, M_FT_UINT32 },
		{ RW_FIELD_RESULT, M_FT_UINT32 },
		{ RW_FIELD_NAMESPACE, M_FT_BUF },
		{ RW_FIELD_NS_IX, M_FT_UINT32 },
		{ RW_FIELD_GENERATION, M_FT_UINT32 },
		{ RW_FIELD_DIGEST, M_FT_BUF },
		{ RW_FIELD_RECORD, M_FT_BUF },
		{ RW_FIELD_UNUSED_7, M_FT_BUF },
		{ RW_FIELD_UNUSED_8, M_FT_UINT64 },
		{ RW_FIELD_UNUSED_9, M_FT_BUF },
		{ RW_FIELD_TID, M_FT_UINT32 },
		{ RW_FIELD_UNUSED_11, M_FT_UINT32 },
		{ RW_FIELD_INFO, M_FT_UINT32 },
		{ RW_FIELD_UNUSED_13, M_FT_BUF },
		{ RW_FIELD_UNUSED_14, M_FT_BUF },
		{ RW_FIELD_UNUSED_15, M_FT_UINT64 },
		{ RW_FIELD_LAST_UPDATE_TIME, M_FT_UINT64 },
		{ RW_FIELD_UNUSED_17, M_FT_BUF },
		{ RW_FIELD_UNUSED_18, M_FT_BUF },
		{ RW_FIELD_REGIME, M_FT_UINT32 }
};

COMPILER_ASSERT(sizeof(rw_mt) / sizeof(msg_template) == NUM_RW_FIELDS);

#define RW_MSG_SCRATCH_SIZE 192

#define RETRANSMIT_PERIOD_US (5 * 1000) // 5 ms


//==========================================================
// Globals.
//

static cf_rchash* g_rw_request_hash = NULL;


//==========================================================
// Forward declarations.
//

static uint32_t rw_request_hash_fn(const void* key);
static transaction_status handle_hot_key(rw_request* rw0, as_transaction* tr);
static const char* action_tag(const as_transaction* tr);
static const char* from_tag(const as_transaction* tr);

static void* run_retransmit(void* arg);
static int retransmit_reduce_fn(const void* key, void* data, void* udata);
static void update_retransmit_stats(const rw_request* rw);
static void update_non_sub_retransmit_stats(const rw_request* rw);
static void update_batch_sub_retransmit_stats(const rw_request* rw);

static int rw_msg_cb(cf_node id, msg* m, void* udata);


//==========================================================
// Public API.
//

void
as_rw_init()
{
	g_rw_request_hash = cf_rchash_create(rw_request_hash_fn,
			rw_request_hdestroy, sizeof(rw_request_hkey), 32 * 1024);

	cf_thread_create_detached(run_retransmit, NULL);

	as_fabric_register_msg_fn(M_TYPE_RW, rw_mt, sizeof(rw_mt),
			RW_MSG_SCRATCH_SIZE, rw_msg_cb, NULL);
}

uint32_t
rw_request_hash_count()
{
	return cf_rchash_get_size(g_rw_request_hash);
}

transaction_status
rw_request_hash_insert(rw_request_hkey* hkey, rw_request* rw,
		as_transaction* tr)
{
	while (cf_rchash_put_unique(g_rw_request_hash, hkey, rw) != CF_RCHASH_OK) {
		// rw_request with this digest already in hash - get it.

		rw_request* rw0;

		if (cf_rchash_get(g_rw_request_hash, hkey, (void**)&rw0) !=
				CF_RCHASH_OK) {
			// But now it's gone - try insertion again immediately.
			continue;
		}
		// else - got it - handle "hot key" scenario.

		cf_mutex_lock(&rw0->lock);

		transaction_status status = handle_hot_key(rw0, tr);

		cf_mutex_unlock(&rw0->lock);
		rw_request_release(rw0);

		return status; // rw_request was not inserted in the hash
	}

	return TRANS_IN_PROGRESS; // rw_request was inserted in the hash
}

void
rw_request_hash_delete(rw_request_hkey* hkey, rw_request* rw)
{
	cf_rchash_delete_object(g_rw_request_hash, hkey, rw);
}

rw_request*
rw_request_hash_get(rw_request_hkey* hkey)
{
	rw_request* rw = NULL;

	cf_rchash_get(g_rw_request_hash, hkey, (void**)&rw);

	return rw;
}

// For debugging only.
void
rw_request_hash_dump()
{
	cf_info(AS_RW, "rw_request_hash dump not yet implemented");
	// TODO - implement something, or deprecate.
}


//==========================================================
// Local helpers - hash insertion.
//

static uint32_t
rw_request_hash_fn(const void* key)
{
	rw_request_hkey* hkey = (rw_request_hkey*)key;

	return *(uint32_t*)&hkey->keyd.digest[DIGEST_HASH_BASE_BYTE];
}

static transaction_status
handle_hot_key(rw_request* rw0, as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;

	if (tr->origin == FROM_READ_TOUCH) {
		// If the key is in the hash already, we don't need to touch it. The
		// result code set here is for internal use only.
		tr->result_code = AS_ERR_KEY_BUSY;

		return TRANS_DONE_SUCCESS;
	}
	else if (tr->origin == FROM_RE_REPL) {
		// Always put this transaction at the head of the original rw_request's
		// queue - it will be retried (first) when the original is complete.
		rw_request_wait_q_push_head(rw0, tr);

		return TRANS_WAITING;
	}
	else if (ns->transaction_pending_limit != 0 &&
			rw0->wait_queue_depth > ns->transaction_pending_limit) {
		// If we're over the hot key pending limit, fail this transaction.
		cf_ticker_detail(AS_KEY_BUSY, "{%s} %pD %s from %s", ns->name,
				&tr->keyd, action_tag(tr), from_tag(tr));

		as_incr_uint64(&ns->n_fail_key_busy);
		tr->result_code = AS_ERR_KEY_BUSY;

		return TRANS_DONE_ERROR;
	}
	else {
		// Queue this transaction on the original rw_request - it will be
		// retried when the original is complete.
		rw_request_wait_q_push(rw0, tr);

		return TRANS_WAITING;
	}
}

static const char*
action_tag(const as_transaction* tr)
{
	if (as_transaction_is_batch_sub(tr)) {
		return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) == 0 ?
				"batch-sub-read" : (as_transaction_is_delete(tr) ?
						"batch-sub-delete" : (as_transaction_is_udf(tr) ?
								"batch-sub-udf" : "batch-sub-write"));
	}

	return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) == 0 ?
			"read" : (as_transaction_is_delete(tr) ?
					"delete" : (as_transaction_is_udf(tr) ?
							"udf" : "write"));
}

static const char*
from_tag(const as_transaction* tr)
{
	switch (tr->origin) {
	case FROM_CLIENT:
		return tr->from.proto_fd_h->client;
	case FROM_PROXY:
		return "proxy";
	case FROM_BATCH:
		return as_batch_get_fd_h(tr->from.batch_shared)->client;
	case FROM_IUDF:
		return "bg-udf";
	case FROM_IOPS:
		return "bg-ops";
	case FROM_READ_TOUCH:
		return "read-touch";
	case FROM_RE_REPL:
		return "re-repl";
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		return NULL;
	}
}


//==========================================================
// Local helpers - retransmit.
//

static void*
run_retransmit(void* arg)
{
	while (true) {
		now_times now;

		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		if (cf_rchash_get_size(g_rw_request_hash) != 0) {
			cf_rchash_reduce(g_rw_request_hash, retransmit_reduce_fn, &now);
		}

		uint64_t lap_us = (cf_getns() - now.now_ns) / 1000;

		if (lap_us < RETRANSMIT_PERIOD_US) {
			usleep(RETRANSMIT_PERIOD_US - lap_us);
		}
	}

	return NULL;
}

static int
retransmit_reduce_fn(const void* key, void* data, void* udata)
{
	rw_request* rw = data;
	now_times* now = (now_times*)udata;

	if (! as_load_bool_acq(&rw->is_set_up)) {
		return 0;
	}

	if (now->now_ns > rw->end_time) {
		cf_mutex_lock(&rw->lock);

		rw->timeout_cb(rw);

		cf_mutex_unlock(&rw->lock);

		return CF_RCHASH_REDUCE_DELETE;
	}

	if (rw->xmit_ms < now->now_ms) {
		cf_mutex_lock(&rw->lock);

		if (rw->from.any) {
			rw->xmit_ms = now->now_ms + rw->retry_interval_ms;
			rw->retry_interval_ms *= 2;

			if (rw->repl_write_cb != NULL) {
				repl_write_reset_replicas(rw);
			}

			send_rw_messages(rw);
			update_retransmit_stats(rw);
		}
		// else - lost race against dup-res or repl-write callback.

		cf_mutex_unlock(&rw->lock);
	}

	return 0;
}

static void
update_retransmit_stats(const rw_request* rw)
{
	as_namespace* ns = rw->rsv.ns;
	bool is_repl_write = rw->repl_write_cb != NULL;

	// Note - only one retransmit thread, so no need for atomic increments.

	switch (rw->origin) {
	case FROM_PROXY:
		if (rw_request_is_batch_sub(rw)) {
			update_batch_sub_retransmit_stats(rw);
			break;
		}
		// No break.
	case FROM_CLIENT:
		update_non_sub_retransmit_stats(rw);
		break;
	case FROM_BATCH:
		update_batch_sub_retransmit_stats(rw);
		break;
	case FROM_IUDF:
		if (is_repl_write) {
			ns->n_retransmit_udf_sub_repl_write++;
		}
		else {
			ns->n_retransmit_udf_sub_dup_res++;
		}
		break;
	case FROM_IOPS:
		if (is_repl_write) {
			ns->n_retransmit_ops_sub_repl_write++;
		}
		else {
			ns->n_retransmit_ops_sub_dup_res++;
		}
		break;
	case FROM_RE_REPL:
		// For now we don't report re-replication retransmit stats.
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}
}

static void
update_non_sub_retransmit_stats(const rw_request* rw)
{
	as_namespace* ns = rw->rsv.ns;
	as_msg* m = &rw->msgp->msg;

	bool is_repl_write = rw->repl_write_cb != NULL;
	bool is_repl_ping = rw->repl_ping_cb != NULL;

	bool is_write = (m->info2 & AS_MSG_INFO2_WRITE) != 0;
	bool is_delete = (m->info2 & AS_MSG_INFO2_DELETE) != 0;
	bool is_udf = (rw->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;

	if (is_repl_write) {
		cf_assert(is_write, AS_RW, "read doing replica write");

		if (is_delete) {
			ns->n_retransmit_all_delete_repl_write++;
		}
		else if (is_udf) {
			ns->n_retransmit_all_udf_repl_write++;
		}
		else {
			ns->n_retransmit_all_write_repl_write++;
		}
	}
	else {
		if (is_write) {
			if (is_delete) {
				ns->n_retransmit_all_delete_dup_res++;
			}
			else if (is_udf) {
				ns->n_retransmit_all_udf_dup_res++;
			}
			else {
				ns->n_retransmit_all_write_dup_res++;
			}
		}
		else {
			if (is_repl_ping) {
				ns->n_retransmit_all_read_repl_ping++;
			}
			else {
				ns->n_retransmit_all_read_dup_res++;
			}
		}
	}
}

static void
update_batch_sub_retransmit_stats(const rw_request* rw)
{
	as_namespace* ns = rw->rsv.ns;
	as_msg* m = &rw->msgp->msg;

	bool is_repl_write = rw->repl_write_cb != NULL;
	bool is_repl_ping = rw->repl_ping_cb != NULL;

	bool is_write = (m->info2 & AS_MSG_INFO2_WRITE) != 0;
	bool is_delete = (m->info2 & AS_MSG_INFO2_DELETE) != 0;
	bool is_udf = (rw->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;

	if (is_repl_write) {
		cf_assert(is_write, AS_RW, "read doing replica write");

		if (is_delete) {
			ns->n_retransmit_all_batch_sub_delete_repl_write++;
		}
		else if (is_udf) {
			ns->n_retransmit_all_batch_sub_udf_repl_write++;
		}
		else {
			ns->n_retransmit_all_batch_sub_write_repl_write++;
		}
	}
	else {
		if (is_write) {
			if (is_delete) {
				ns->n_retransmit_all_batch_sub_delete_dup_res++;
			}
			else if (is_udf) {
				ns->n_retransmit_all_batch_sub_udf_dup_res++;
			}
			else {
				ns->n_retransmit_all_batch_sub_write_dup_res++;
			}
		}
		else {
			if (is_repl_ping) {
				ns->n_retransmit_all_batch_sub_read_repl_ping++;
			}
			else {
				ns->n_retransmit_all_batch_sub_read_dup_res++;
			}
		}
	}
}


//==========================================================
// Local helpers - handle RW fabric messages.
//

static int
rw_msg_cb(cf_node id, msg* m, void* udata)
{
	uint32_t op;

	if (msg_get_uint32(m, RW_FIELD_OP, &op) != 0) {
		cf_warning(AS_RW, "got rw msg without op field");
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	//--------------------------------------------
	// Duplicate resolution:
	//
	case RW_OP_DUP:
		dup_res_handle_request(id, m);
		break;
	case RW_OP_DUP_ACK:
		dup_res_handle_ack(id, m);
		break;

	//--------------------------------------------
	// Replica writes:
	//
	case RW_OP_REPL_WRITE:
		repl_write_handle_op(id, m);
		break;
	case RW_OP_WRITE_ACK:
		repl_write_handle_ack(id, m);
		break;
	case RW_OP_REPL_CONFIRM:
		repl_write_handle_confirmation(m);
		break;

	//--------------------------------------------
	// Replica pings:
	//
	case RW_OP_REPL_PING:
		repl_ping_handle_op(id, m);
		break;
	case RW_OP_REPL_PING_ACK:
		repl_ping_handle_ack(id, m);
		break;

	default:
		cf_warning(AS_RW, "got rw msg with unrecognized op %u", op);
		as_fabric_msg_put(m);
		break;
	}

	return 0;
}
