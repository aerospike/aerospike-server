/*
 * rw_request.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "msg.h"
#include "node.h"

#include "base/proto.h"
#include "base/transaction.h"
#include "fabric/hb.h"
#include "fabric/partition.h"


//==========================================================
// Forward declarations.
//

struct as_batch_shared_s;
struct as_file_handle_s;
struct as_transaction_s;
struct cl_msg_s;
struct iudf_origin_s;
struct rw_request_s;
struct rw_wait_ele_s;


//==========================================================
// Typedefs & constants.
//

typedef bool (*dup_res_done_cb) (struct rw_request_s* rw);
typedef void (*repl_write_done_cb) (struct rw_request_s* rw);
typedef void (*repl_ping_done_cb) (struct rw_request_s* rw);
typedef void (*timeout_done_cb) (struct rw_request_s* rw);

typedef struct rw_request_s {

	//------------------------------------------------------
	// Matches as_transaction.
	//

	struct cl_msg_s*	msgp;
	uint32_t			msg_fields;

	uint8_t				origin;
	uint8_t				from_flags;

	union {
		void*						any;
		struct as_file_handle_s*	proto_fd_h;
		cf_node						proxy_node;
		struct iudf_origin_s*		iudf_orig;
		struct as_batch_shared_s*	batch_shared;
	} from;

	union {
		uint32_t any;
		uint32_t batch_index;
		uint32_t proxy_tid;
	} from_data;

	cf_digest			keyd;

	uint64_t			start_time;
	uint64_t			benchmark_time;

	as_partition_reservation rsv;

	uint64_t			end_time;
	uint8_t				result_code;
	uint8_t				flags;
	uint16_t			generation;
	uint32_t			void_time;
	uint64_t			last_update_time;

	//
	// End of as_transaction look-alike.
	//------------------------------------------------------

	cf_mutex			lock;

	struct rw_wait_ele_s* wait_queue_head;
	struct rw_wait_ele_s* wait_queue_tail;
	uint32_t			wait_queue_depth;

	bool				is_set_up; // TODO - redundant with timeout_cb

	// Store pickled data, for use in replica write.
	bool				is_old_pickle; // TODO - old pickle - remove in "six months"
	uint8_t*			pickle;
	size_t				pickle_sz;
	const char*			set_name; // points directly into vmap - never free it
	uint32_t			set_name_len;
	uint8_t*			key;
	uint32_t			key_size;

	// Store ops' responses here.
	cf_dyn_buf			response_db;

	// Manage responses for duplicate resolution and replica write requests, or
	// alternatively, timeouts.
	uint32_t			tid;
	bool				dup_res_complete;
	bool				repl_write_complete;
	bool				repl_ping_complete;
	dup_res_done_cb		dup_res_cb;
	repl_write_done_cb	repl_write_cb;
	repl_ping_done_cb	repl_ping_cb;
	timeout_done_cb		timeout_cb;

	// Message being sent to dest_nodes. May be duplicate resolution or replica
	// write request. Message is kept in case it needs to be retransmitted.
	msg*				dest_msg;

	uint64_t			xmit_ms; // time of next retransmit
	uint32_t			retry_interval_ms; // interval to add for next retransmit

	// Destination info for duplicate resolution and replica write requests.
	uint32_t			n_dest_nodes;
	cf_node				dest_nodes[AS_CLUSTER_SZ];
	bool				dest_complete[AS_CLUSTER_SZ];

	// Duplicate resolution response messages from nodes with duplicates.
	msg*				best_dup_msg;
	// TODO - could store best dup node-id - worth it?
	uint8_t				best_dup_result_code;
	uint16_t			best_dup_gen;
	uint64_t			best_dup_lut;

	bool				tie_was_replicated; // enterprise only

	// Node health related stat, to track replication latency.
	uint64_t			repl_start_us;

} rw_request;


//==========================================================
// Public API.
//

rw_request* rw_request_create();
void rw_request_destroy(rw_request* rw);
void rw_request_wait_q_push(rw_request* rw, struct as_transaction_s* tr);
void rw_request_wait_q_push_head(rw_request* rw, struct as_transaction_s* tr);

static inline void
rw_request_hdestroy(void* pv)
{
	rw_request_destroy((rw_request*)pv);
}

static inline void
rw_request_release(rw_request* rw)
{
	if (cf_rc_release(rw) == 0) {
		rw_request_destroy(rw);
		cf_rc_free(rw);
	}
}

static inline bool
rw_request_is_batch_sub(const rw_request* rw)
{
	return (rw->from_flags & FROM_FLAG_BATCH_SUB) != 0;
}

// See as_transaction_trid().
static inline uint64_t
rw_request_trid(const rw_request* rw)
{
	if ((rw->msg_fields & AS_MSG_FIELD_BIT_TRID) == 0) {
		return 0;
	}

	as_msg_field *f = as_msg_field_get(&rw->msgp->msg, AS_MSG_FIELD_TYPE_TRID);

	return cf_swap_from_be64(*(uint64_t*)f->data);
}
