/*
 * proxy.c
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

#include "transaction/proxy.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "shash.h"
#include "socket.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/stats.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"


//==========================================================
// Typedefs & constants.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	PROXY_FIELD_OP,
	PROXY_FIELD_TID,
	PROXY_FIELD_DIGEST,
	PROXY_FIELD_REDIRECT,
	PROXY_FIELD_AS_PROTO, // request as_proto - currently contains only as_msg's
	PROXY_FIELD_UNUSED_5,
	PROXY_FIELD_UNUSED_6,
	PROXY_FIELD_UNUSED_7,

	NUM_PROXY_FIELDS
} proxy_msg_field;

#define PROXY_OP_REQUEST 1
#define PROXY_OP_RESPONSE 2
#define PROXY_OP_RETURN_TO_SENDER 3

const msg_template proxy_mt[] = {
	{ PROXY_FIELD_OP, M_FT_UINT32 },
	{ PROXY_FIELD_TID, M_FT_UINT32 },
	{ PROXY_FIELD_DIGEST, M_FT_BUF },
	{ PROXY_FIELD_REDIRECT, M_FT_UINT64 },
	{ PROXY_FIELD_AS_PROTO, M_FT_BUF },
	{ PROXY_FIELD_UNUSED_5, M_FT_UINT64 },
	{ PROXY_FIELD_UNUSED_6, M_FT_UINT32 },
	{ PROXY_FIELD_UNUSED_7, M_FT_UINT32 },
};

COMPILER_ASSERT(sizeof(proxy_mt) / sizeof(msg_template) == NUM_PROXY_FIELDS);

#define PROXY_MSG_SCRATCH_SIZE 128

typedef struct proxy_request_s {
	uint32_t		msg_fields;

	uint8_t			origin;
	uint8_t			from_flags;

	union {
		void*				any;
		as_file_handle*		proto_fd_h;
		as_batch_shared*	batch_shared;
		// No need yet for other members of this union.
	} from;

	// No need yet for a 'from_data" union.
	uint32_t		batch_index;

	uint64_t		start_time;
	uint64_t		end_time;

	// The original proxy message.
	msg*			fab_msg;

	as_namespace*	ns;
} proxy_request;


//==========================================================
// Globals.
//

static cf_shash* g_proxy_hash = NULL;
static cf_atomic32 g_proxy_tid = 0;


//==========================================================
// Forward declarations.
//

void* run_proxy_timeout(void* arg);
int proxy_timeout_reduce_fn(const void* key, void* data, void* udata);

int proxy_msg_cb(cf_node src, msg* m, void* udata);

void proxyer_handle_response(msg* m, uint32_t tid);
int proxyer_handle_client_response(msg* m, proxy_request* pr);
int proxyer_handle_batch_response(msg* m, proxy_request* pr);
void proxyer_handle_return_to_sender(msg* m, uint32_t tid);

void proxyee_handle_request(cf_node src, msg* m, uint32_t tid);


//==========================================================
// Inlines & macros.
//

static inline void
error_response(cf_node src, uint32_t tid, uint32_t error)
{
	as_proxy_send_response(src, tid, error, 0, 0, NULL, NULL, 0, NULL, 0);
}

static inline void
client_proxy_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_client_proxy_complete);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_proxy_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_proxy_error);
		break;
	}
}

static inline void
batch_sub_proxy_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_complete);
		break;
	case AS_ERR_TIMEOUT:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_error);
		break;
	}
}


//==========================================================
// Public API.
//

void
as_proxy_init()
{
	g_proxy_hash = cf_shash_create(cf_shash_fn_u32, sizeof(uint32_t),
			sizeof(proxy_request), 4 * 1024, CF_SHASH_MANY_LOCK);

	cf_thread_create_detached(run_proxy_timeout, NULL);

	as_fabric_register_msg_fn(M_TYPE_PROXY, proxy_mt, sizeof(proxy_mt),
			PROXY_MSG_SCRATCH_SIZE, proxy_msg_cb, NULL);
}


uint32_t
as_proxy_hash_count()
{
	return cf_shash_get_size(g_proxy_hash);
}


// Proxyer - divert a transaction request to another node.
void
as_proxy_divert(cf_node dst, as_transaction* tr, as_namespace* ns)
{
	// Special log detail.
	switch (tr->origin) {
	case FROM_CLIENT:
		cf_detail_digest(AS_PROXY_DIVERT, &tr->keyd,
				"{%s} diverting from client %s to node %lx ",
				ns->name, tr->from.proto_fd_h->client, dst);
		break;
	case FROM_BATCH:
		cf_detail_digest(AS_PROXY_DIVERT, &tr->keyd,
				"{%s} diverting batch-sub from client %s to node %lx ",
				ns->name, as_batch_get_fd_h(tr->from.batch_shared)->client,
				dst);
		break;
	default:
		cf_crash(AS_PROXY, "unexpected transaction origin %u", tr->origin);
		break;
	}

	// Get a fabric message and fill it out.

	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_type set_type = tr->origin == FROM_BATCH ?
			MSG_SET_COPY : MSG_SET_HANDOFF_MALLOC;

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void*)tr->msgp,
			sizeof(as_proto) + tr->msgp->proto.sz, set_type);

	// Set up a proxy_request and insert it in the hash.

	proxy_request pr;

	pr.msg_fields = tr->msg_fields;

	pr.origin = tr->origin;
	pr.from_flags = tr->from_flags;
	pr.from.any = tr->from.any;
	pr.batch_index = tr->from_data.batch_index;

	pr.start_time = tr->start_time;
	pr.end_time = tr->end_time;

	pr.fab_msg = m;

	pr.ns = ns;

	msg_incr_ref(m); // reference for the hash

	cf_shash_put(g_proxy_hash, &tid, &pr);

	tr->msgp = NULL; // pattern, not needed
	tr->from.any = NULL; // pattern, not needed

	// Send fabric message to remote node.

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	as_health_add_node_counter(dst, AS_HEALTH_NODE_PROXIES);
}


// Proxyee - transaction reservation failed here, tell proxyer to try again.
void
as_proxy_return_to_sender(const as_transaction* tr, as_namespace* ns)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);
	uint32_t pid = as_partition_getid(&tr->keyd);
	cf_node redirect_node = as_partition_proxyee_redirect(ns, pid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RETURN_TO_SENDER);
	msg_set_uint32(m, PROXY_FIELD_TID, tr->from_data.proxy_tid);
	msg_set_uint64(m, PROXY_FIELD_REDIRECT,
			redirect_node == (cf_node)0 ? tr->from.proxy_node : redirect_node);

	if (as_fabric_send(tr->from.proxy_node, m, AS_FABRIC_CHANNEL_RW) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


// Proxyee - transaction completed here, send response to proxyer.
void
as_proxy_send_response(cf_node dst, uint32_t proxy_tid, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op** ops, as_bin** bins,
		uint16_t bin_count, as_namespace* ns, uint64_t trid)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	size_t msg_sz = 0;
	uint8_t* msgp = (uint8_t*)as_msg_make_response_msg(result_code, generation,
			void_time, ops, bins, bin_count, ns, 0, &msg_sz, trid);

	msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_HANDOFF_MALLOC);

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


// Proxyee - transaction completed here, send response to proxyer.
void
as_proxy_send_ops_response(cf_node dst, uint32_t proxy_tid, cf_dyn_buf* db)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	uint8_t* msgp = db->buf;
	size_t msg_sz = db->used_sz;

	if (db->is_stack) {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_COPY);
	}
	else {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz,
				MSG_SET_HANDOFF_MALLOC);
		db->buf = NULL; // the fabric owns the buffer now
	}

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


//==========================================================
// Local helpers - proxyer.
//

void
proxyer_handle_response(msg* m, uint32_t tid)
{
	proxy_request pr;

	if (cf_shash_get_and_delete(g_proxy_hash, &tid, &pr) != CF_SHASH_OK) {
		// Some other response (or timeout) has already finished this pr.
		return;
	}

	cf_assert(pr.from.any, AS_PROXY, "origin %u has null 'from'", pr.origin);

	int result;

	switch (pr.origin) {
	case FROM_CLIENT:
		result = proxyer_handle_client_response(m, &pr);
		client_proxy_update_stats(pr.ns, result);
		break;
	case FROM_BATCH:
		result = proxyer_handle_batch_response(m, &pr);
		batch_sub_proxy_update_stats(pr.ns, result);
		// Note - no worries about msgp, proxy divert copied it.
		break;
	default:
		cf_crash(AS_PROXY, "unexpected transaction origin %u", pr.origin);
		break;
	}

	pr.from.any = NULL; // pattern, not needed

	as_fabric_msg_put(pr.fab_msg);

	// Note that this includes both origins.
	if (pr.ns->proxy_hist_enabled) {
		histogram_insert_data_point(pr.ns->proxy_hist, pr.start_time);
	}
}


int
proxyer_handle_client_response(msg* m, proxy_request* pr)
{
	uint8_t* proto;
	size_t proto_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, &proto, &proto_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		return AS_ERR_UNKNOWN;
	}

	as_file_handle* fd_h = pr->from.proto_fd_h;

	if (cf_socket_send_all(&fd_h->sock, proto, proto_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		as_end_of_transaction_force_close(fd_h);
		return AS_ERR_UNKNOWN;
	}

	as_end_of_transaction_ok(fd_h);
	return AS_OK;
}


int
proxyer_handle_batch_response(msg* m, proxy_request* pr)
{
	cl_msg* msgp;
	size_t msgp_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, &msgp_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		return AS_ERR_UNKNOWN;
	}

	cf_digest* keyd;

	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_crash(AS_PROXY, "original msg get for digest failed");
	}

	as_batch_add_proxy_result(pr->from.batch_shared, pr->batch_index, keyd,
			msgp, msgp_sz);

	return AS_OK;
}


void
proxyer_handle_return_to_sender(msg* m, uint32_t tid)
{
	proxy_request* pr;
	cf_mutex* lock;

	if (cf_shash_get_vlock(g_proxy_hash, &tid, (void**)&pr, &lock) !=
			CF_SHASH_OK) {
		// Some other response (or timeout) has already finished this pr.
		return;
	}

	cf_node redirect_node;

	if (msg_get_uint64(m, PROXY_FIELD_REDIRECT, &redirect_node) == 0
			&& redirect_node != g_config.self_node
			&& redirect_node != (cf_node)0) {
		// If this node was a "random" node, i.e. neither acting nor eventual
		// master, it diverts to the eventual master (the best it can do.) The
		// eventual master must inform this node about the acting master.

		msg_incr_ref(pr->fab_msg);

		if (as_fabric_send(redirect_node, pr->fab_msg, AS_FABRIC_CHANNEL_RW) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(pr->fab_msg);
		}

		cf_mutex_unlock(lock);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_crash(AS_PROXY, "original msg get for digest failed");
	}

	cl_msg* msgp;

	// TODO - inefficient! Should be a way to 'take' a buffer from msg.
	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, NULL,
			MSG_GET_COPY_MALLOC) != 0) {
		cf_crash(AS_PROXY, "original msg get for proto failed");
	}

	// Put the as_msg on the normal queue for processing.
	as_transaction tr;
	as_transaction_init_head(&tr, keyd, msgp);
	// msgp might not have digest - batch sub-transactions, old clients.
	// For old clients, will compute it again from msgp key and set.

	tr.msg_fields = pr->msg_fields;
	tr.origin = pr->origin;
	tr.from_flags = pr->from_flags;
	tr.from.any = pr->from.any;
	tr.from_data.batch_index = pr->batch_index;
	tr.start_time = pr->start_time;

	as_tsvc_enqueue(&tr);

	as_fabric_msg_put(pr->fab_msg);

	cf_shash_delete_lockfree(g_proxy_hash, &tid);
	cf_mutex_unlock(lock);
}


//==========================================================
// Local helpers - proxyee.
//

void
proxyee_handle_request(cf_node src, msg* m, uint32_t tid)
{
	cf_digest* keyd;

	if (msg_get_buf(m, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for digest failed");
		error_response(src, tid, AS_ERR_UNKNOWN);
		return;
	}

	cl_msg* msgp;
	size_t msgp_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, &msgp_sz,
			MSG_GET_COPY_MALLOC) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		error_response(src, tid, AS_ERR_UNKNOWN);
		return;
	}

	// Sanity check as_proto fields.
	as_proto* proto = &msgp->proto;

	if (! as_proto_wrapped_is_valid(proto, msgp_sz)) {
		cf_warning(AS_PROXY, "bad proto: version %u, type %u, sz %lu [%lu]",
				proto->version, proto->type, (uint64_t)proto->sz, msgp_sz);
		error_response(src, tid, AS_ERR_UNKNOWN);
		return;
	}

	// Put the as_msg on the normal queue for processing.
	as_transaction tr;
	as_transaction_init_head(&tr, keyd, msgp);
	// msgp might not have digest - batch sub-transactions, old clients.
	// For old clients, will compute it again from msgp key and set.

	tr.start_time = cf_getns();

	tr.origin = FROM_PROXY;
	tr.from.proxy_node = src;
	tr.from_data.proxy_tid = tid;

	// Proxyer has already done byte swapping in as_msg.
	if (! as_transaction_prepare(&tr, false)) {
		cf_warning(AS_PROXY, "bad proxy msg");
		error_response(src, tid, AS_ERR_UNKNOWN);
		return;
	}

	// For batch sub-transactions, make sure we flag them so they're not
	// mistaken for multi-record transactions (which never proxy).
	if (as_transaction_has_no_key_or_digest(&tr)) {
		tr.from_flags |= FROM_FLAG_BATCH_SUB;
	}

	as_tsvc_enqueue(&tr);
}


//==========================================================
// Local helpers - timeout.
//

void*
run_proxy_timeout(void* arg)
{
	while (true) {
		usleep(75 * 1000);

		now_times now;

		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		cf_shash_reduce(g_proxy_hash, proxy_timeout_reduce_fn, &now);
	}

	return NULL;
}


int
proxy_timeout_reduce_fn(const void* key, void* data, void* udata)
{
	proxy_request* pr = data;
	now_times* now = (now_times*)udata;

	if (now->now_ns < pr->end_time) {
		return CF_SHASH_OK;
	}

	// Handle timeouts.

	cf_assert(pr->from.any, AS_PROXY, "origin %u has null 'from'", pr->origin);

	switch (pr->origin) {
	case FROM_CLIENT:
		// TODO - when it becomes important enough, find a way to echo trid.
		as_msg_send_reply(pr->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, pr->ns, 0);
		client_proxy_update_stats(pr->ns, AS_ERR_TIMEOUT);
		break;
	case FROM_BATCH:
		as_batch_add_error(pr->from.batch_shared, pr->batch_index,
				AS_ERR_TIMEOUT);
		// Note - no worries about msgp, proxy divert copied it.
		batch_sub_proxy_update_stats(pr->ns, AS_ERR_TIMEOUT);
		break;
	default:
		cf_crash(AS_PROXY, "unexpected transaction origin %u", pr->origin);
		break;
	}

	pr->from.any = NULL; // pattern, not needed
	as_fabric_msg_put(pr->fab_msg);

	return CF_SHASH_REDUCE_DELETE;
}


//==========================================================
// Local helpers - handle PROXY fabric messages.
//

int
proxy_msg_cb(cf_node src, msg* m, void* udata)
{
	uint32_t op;

	if (msg_get_uint32(m, PROXY_FIELD_OP, &op) != 0) {
		cf_warning(AS_PROXY, "msg get for op failed");
		as_fabric_msg_put(m);
		return 0;
	}

	uint32_t tid;

	if (msg_get_uint32(m, PROXY_FIELD_TID, &tid) != 0) {
		cf_warning(AS_PROXY, "msg get for tid failed");
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	case PROXY_OP_REQUEST:
		proxyee_handle_request(src, m, tid);
		break;
	case PROXY_OP_RESPONSE:
		proxyer_handle_response(m, tid);
		break;
	case PROXY_OP_RETURN_TO_SENDER:
		proxyer_handle_return_to_sender(m, tid);
		break;
	default:
		cf_warning(AS_PROXY, "received unexpected message op %u", op);
		break;
	}

	as_fabric_msg_put(m);
	return 0;
}
