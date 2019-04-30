/*
 * rw_request_hash.c
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

#include "transaction/rw_request_hash.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "rchash.h"

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
		{ RW_FIELD_NS_ID, M_FT_UINT32 },
		{ RW_FIELD_GENERATION, M_FT_UINT32 },
		{ RW_FIELD_DIGEST, M_FT_BUF },
		{ RW_FIELD_RECORD, M_FT_BUF },
		{ RW_FIELD_UNUSED_7, M_FT_BUF },
		{ RW_FIELD_CLUSTER_KEY, M_FT_UINT64 },
		{ RW_FIELD_OLD_RECORD, M_FT_BUF },
		{ RW_FIELD_TID, M_FT_UINT32 },
		{ RW_FIELD_VOID_TIME, M_FT_UINT32 },
		{ RW_FIELD_INFO, M_FT_UINT32 },
		{ RW_FIELD_UNUSED_13, M_FT_BUF },
		{ RW_FIELD_UNUSED_14, M_FT_BUF },
		{ RW_FIELD_UNUSED_15, M_FT_UINT64 },
		{ RW_FIELD_LAST_UPDATE_TIME, M_FT_UINT64 },
		{ RW_FIELD_SET_NAME, M_FT_BUF },
		{ RW_FIELD_KEY, M_FT_BUF },
		{ RW_FIELD_REGIME, M_FT_UINT32 }
};

COMPILER_ASSERT(sizeof(rw_mt) / sizeof(msg_template) == NUM_RW_FIELDS);

#define RW_MSG_SCRATCH_SIZE 192


//==========================================================
// Globals.
//

static cf_rchash* g_rw_request_hash = NULL;


//==========================================================
// Forward declarations.
//

uint32_t rw_request_hash_fn(const void* value, uint32_t value_len);
transaction_status handle_hot_key(rw_request* rw0, as_transaction* tr);

void* run_retransmit(void* arg);
int retransmit_reduce_fn(const void* key, uint32_t keylen, void* data, void* udata);
void update_retransmit_stats(const rw_request* rw);

int rw_msg_cb(cf_node id, msg* m, void* udata);


//==========================================================
// Public API.
//

void
as_rw_init()
{
	g_rw_request_hash = cf_rchash_create(rw_request_hash_fn,
			rw_request_hdestroy, sizeof(rw_request_hkey), 32 * 1024,
			CF_RCHASH_MANY_LOCK);

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
	while (cf_rchash_put_unique(g_rw_request_hash, hkey, sizeof(*hkey), rw) !=
			CF_RCHASH_OK) {
		// rw_request with this digest already in hash - get it.

		rw_request* rw0;

		if (cf_rchash_get(g_rw_request_hash, hkey, sizeof(*hkey),
				(void**)&rw0) != CF_RCHASH_OK) {
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
	cf_rchash_delete_object(g_rw_request_hash, hkey, sizeof(*hkey), rw);
}


rw_request*
rw_request_hash_get(rw_request_hkey* hkey)
{
	rw_request* rw = NULL;

	cf_rchash_get(g_rw_request_hash, hkey, sizeof(*hkey), (void**)&rw);

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

uint32_t
rw_request_hash_fn(const void* key, uint32_t key_size)
{
	rw_request_hkey* hkey = (rw_request_hkey*)key;

	return *(uint32_t*)&hkey->keyd.digest[DIGEST_HASH_BASE_BYTE];
}


transaction_status
handle_hot_key(rw_request* rw0, as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;

	if (tr->origin == FROM_RE_REPL) {
		// Always put this transaction at the head of the original rw_request's
		// queue - it will be retried (first) when the original is complete.
		rw_request_wait_q_push_head(rw0, tr);

		return TRANS_WAITING;
	}
	else if (ns->transaction_pending_limit != 0 &&
			rw0->wait_queue_depth > ns->transaction_pending_limit) {
		// If we're over the hot key pending limit, fail this transaction.
		cf_detail_digest(AS_RW, &tr->keyd, "{%s} key busy ", ns->name);

		cf_atomic64_incr(&ns->n_fail_key_busy);
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


//==========================================================
// Local helpers - retransmit.
//

void*
run_retransmit(void* arg)
{
	while (true) {
		usleep(130 * 1000);

		now_times now;

		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		cf_rchash_reduce(g_rw_request_hash, retransmit_reduce_fn, &now);
	}

	return NULL;
}


int
retransmit_reduce_fn(const void* key, uint32_t keylen, void* data, void* udata)
{
	rw_request* rw = data;
	now_times* now = (now_times*)udata;

	if (! rw->is_set_up) {
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

			send_rw_messages(rw);
			update_retransmit_stats(rw);
		}
		// else - lost race against dup-res or repl-write callback.

		cf_mutex_unlock(&rw->lock);
	}

	return 0;
}


void
update_retransmit_stats(const rw_request* rw)
{
	as_namespace* ns = rw->rsv.ns;
	as_msg* m = &rw->msgp->msg;
	bool is_dup_res = rw->repl_write_cb == NULL;

	// Note - only one retransmit thread, so no need for atomic increments.

	switch (rw->origin) {
	case FROM_PROXY:
		if (rw_request_is_batch_sub(rw)) {
			ns->n_retransmit_all_batch_sub_dup_res++;
			break;
		}
		// No break.
	case FROM_CLIENT: {
			bool is_write = (m->info2 & AS_MSG_INFO2_WRITE) != 0;
			bool is_delete = (m->info2 & AS_MSG_INFO2_DELETE) != 0;
			bool is_udf = (rw->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;

			if (is_dup_res) {
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
					ns->n_retransmit_all_read_dup_res++;
				}
			}
			else {
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
		}
		break;
	case FROM_BATCH:
		// For now batch sub transactions are read-only.
		ns->n_retransmit_all_batch_sub_dup_res++;
		break;
	case FROM_IUDF:
		if (is_dup_res) {
			ns->n_retransmit_udf_sub_dup_res++;
		}
		else {
			ns->n_retransmit_udf_sub_repl_write++;
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


//==========================================================
// Local helpers - handle RW fabric messages.
//

int
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
	case RW_OP_WRITE:
		repl_write_handle_old_op(id, m);
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
