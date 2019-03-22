/*
 * transaction.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "msg.h"
#include "node.h"
#include "socket.h"

#include "base/proto.h"
#include "fabric/partition.h"

struct as_namespace_s;


//==========================================================
// Histogram macros.
//

#define G_HIST_INSERT_DATA_POINT(name, start_time) \
{ \
	if (g_config.name##_enabled) { \
		histogram_insert_data_point(g_stats.name, start_time); \
	} \
}

#define G_HIST_ACTIVATE_INSERT_DATA_POINT(name, start_time) \
{ \
	g_stats.name##_active = true; \
	histogram_insert_data_point(g_stats.name, start_time); \
}

#define HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(trw, name) \
{ \
	trw->rsv.ns->name##_active = true; \
	cf_hist_track_insert_data_point(trw->rsv.ns->name, trw->start_time); \
}

#define HIST_ACTIVATE_INSERT_DATA_POINT(trw, name) \
{ \
	trw->rsv.ns->name##_active = true; \
	histogram_insert_data_point(trw->rsv.ns->name, trw->start_time); \
}

#define BENCHMARK_START(tr, name, orig) \
{ \
	if (tr->rsv.ns->name##_benchmarks_enabled && tr->origin == orig) { \
		if (tr->benchmark_time == 0) { \
			tr->benchmark_time = histogram_insert_data_point(tr->rsv.ns->name##_start_hist, tr->start_time); \
		} \
		else { \
			tr->benchmark_time = histogram_insert_data_point(tr->rsv.ns->name##_restart_hist, tr->benchmark_time); \
		} \
	} \
}

#define BENCHMARK_NEXT_DATA_POINT(trw, name, tok) \
{ \
	if (trw->rsv.ns->name##_benchmarks_enabled && trw->benchmark_time != 0) { \
		trw->benchmark_time = histogram_insert_data_point(trw->rsv.ns->name##_##tok##_hist, trw->benchmark_time); \
	} \
}

#define BENCHMARK_NEXT_DATA_POINT_FROM(trw, name, orig, tok) \
{ \
	if (trw->rsv.ns->name##_benchmarks_enabled && trw->origin == orig && trw->benchmark_time != 0) { \
		trw->benchmark_time = histogram_insert_data_point(trw->rsv.ns->name##_##tok##_hist, trw->benchmark_time); \
	} \
}


//==========================================================
// Client socket information - as_file_handle.
//

typedef struct as_file_handle_s {
	char		client[64];		// client identifier (currently ip-addr:port)
	uint64_t	last_used;		// last nanoseconds we read or wrote
	cf_socket	sock;			// our client socket
	cf_poll		poll;			// our epoll instance
	bool		reap_me;		// force reaping (overrides do_not_reap)
	bool		do_not_reap;	// don't reap during mid-transaction idle
	bool		is_xdr;			// XDR client connection
	as_proto	proto_hdr;		// space for header when reading it from socket
	as_proto	*proto;			// complete request message
	uint64_t	proto_unread;	// bytes not yet read from socket
	void		*security_filter;
} as_file_handle;

// Helpers to release transaction file handles.
void as_release_file_handle(as_file_handle *proto_fd_h);
void as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close);
void as_end_of_transaction_ok(as_file_handle *proto_fd_h);
void as_end_of_transaction_force_close(as_file_handle *proto_fd_h);


//==========================================================
// Transaction.
//

typedef enum {
	TRANS_DONE_ERROR	= -1, // tsvc frees msgp & reservation, response was sent to origin
	TRANS_DONE_SUCCESS	=  0, // tsvc frees msgp & reservation, response was sent to origin
	TRANS_IN_PROGRESS	=  1, // tsvc leaves msgp & reservation alone, rw_request now owns them
	TRANS_WAITING		=  2  // tsvc leaves msgp alone but frees reservation
} transaction_status;

// How to interpret the 'from' union.
//
// NOT a generic transaction type flag, e.g. batch sub-transactions that proxy
// are FROM_PROXY on the proxyee node, hence we still need a separate
// FROM_FLAG_BATCH_SUB.
//
typedef enum {
	// External, comes through service or fabric:
	FROM_CLIENT	= 1,
	FROM_PROXY,

	// Internal, generated on local node:
	FROM_BATCH,
	FROM_IUDF,
	FROM_RE_REPL, // enterprise-only

	FROM_UNDEF	= 0
} transaction_origin;

struct as_batch_shared_s;
struct iudf_origin_s;

typedef struct as_transaction_s {

	//------------------------------------------------------
	// transaction 'head' - copied onto queue.
	//

	cl_msg*		msgp;
	uint32_t	msg_fields;

	uint8_t		origin;
	uint8_t		from_flags;

	// 2 spare bytes.

	union {
		void*						any;
		as_file_handle*				proto_fd_h;
		cf_node						proxy_node;
		struct as_batch_shared_s*	batch_shared;
		struct iudf_origin_s*		iudf_orig;
		void (*re_repl_orig_cb) (struct as_transaction_s* tr);
	} from;

	union {
		uint32_t any;
		uint32_t proxy_tid;
		uint32_t batch_index;
	} from_data;

	cf_digest	keyd; // only batch sub-transactions require this on queue

	uint64_t	start_time;
	uint64_t	benchmark_time;

	//<><><><><><><><><><><> 64 bytes <><><><><><><><><><><>

	//------------------------------------------------------
	// transaction 'body' - NOT copied onto queue.
	//

	as_partition_reservation rsv;

	uint64_t	end_time;
	uint8_t		result_code;
	uint8_t		flags;
	uint16_t	generation;
	uint32_t	void_time;
	uint64_t	last_update_time;

} as_transaction;

#define AS_TRANSACTION_HEAD_SIZE (offsetof(as_transaction, rsv))

// 'from_flags' bits - set before queuing transaction head:
#define FROM_FLAG_BATCH_SUB			0x0001
#define FROM_FLAG_RESTART			0x0002
#define FROM_FLAG_RESTART_STRICT	0x0004 // enterprise-only

// 'flags' bits - set in transaction body after queuing:
#define AS_TRANSACTION_FLAG_SINDEX_TOUCHED	0x01
#define AS_TRANSACTION_FLAG_IS_DELETE		0x02
#define AS_TRANSACTION_FLAG_MUST_PING		0x04 // enterprise-only
#define AS_TRANSACTION_FLAG_RSV_PROLE		0x08 // enterprise-only
#define AS_TRANSACTION_FLAG_RSV_UNAVAILABLE	0x10 // enterprise-only


void as_transaction_init_head(as_transaction *tr, cf_digest *, cl_msg *);
void as_transaction_init_body(as_transaction *tr);

void as_transaction_copy_head(as_transaction *to, const as_transaction *from);

struct rw_request_s;

void as_transaction_init_from_rw(as_transaction *tr, struct rw_request_s *rw);
void as_transaction_init_head_from_rw(as_transaction *tr, struct rw_request_s *rw);

bool as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type);
bool as_transaction_prepare(as_transaction *tr, bool swap);

static inline bool
as_transaction_is_restart(const as_transaction *tr)
{
	return (tr->from_flags & FROM_FLAG_RESTART) != 0;
}

static inline bool
as_transaction_is_batch_sub(const as_transaction *tr)
{
	return (tr->from_flags & FROM_FLAG_BATCH_SUB) != 0;
}

static inline bool
as_transaction_is_restart_strict(const as_transaction *tr)
{
	return (tr->from_flags & FROM_FLAG_RESTART_STRICT) != 0;
}

static inline bool
as_transaction_has_set(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SET) != 0;
}

static inline bool
as_transaction_has_key(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_KEY) != 0;
}

static inline bool
as_transaction_has_digest(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE) != 0;
}

static inline bool
as_transaction_has_no_key_or_digest(const as_transaction *tr)
{
	return (tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0;
}

static inline bool
as_transaction_is_multi_record(const as_transaction *tr)
{
	return	(tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0 &&
			(tr->from_flags & FROM_FLAG_BATCH_SUB) == 0;
}

static inline bool
as_transaction_is_batch_direct(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY) != 0;
}

static inline bool
as_transaction_is_query(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_INDEX_RANGE) != 0;
}

static inline bool
as_transaction_is_udf(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;
}

static inline bool
as_transaction_has_udf_op(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_OP) != 0;
}

static inline bool
as_transaction_has_scan_options(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SCAN_OPTIONS) != 0;
}

static inline bool
as_transaction_has_socket_timeout(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SOCKET_TIMEOUT) != 0;
}

static inline bool
as_transaction_has_predexp(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_PREDEXP) != 0;
}

// For now it's not worth storing the trid in the as_transaction struct since we
// only parse it from the msg once per transaction anyway.
static inline uint64_t
as_transaction_trid(const as_transaction *tr)
{
	if ((tr->msg_fields & AS_MSG_FIELD_BIT_TRID) == 0) {
		return 0;
	}

	as_msg_field *f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_TRID);

	return cf_swap_from_be64(*(uint64_t*)f->data);
}

static inline bool
as_transaction_is_delete(const as_transaction *tr)
{
	return (tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE) != 0;
}

static inline bool
as_transaction_is_durable_delete(const as_transaction *tr)
{
	return (tr->msgp->msg.info2 & AS_MSG_INFO2_DURABLE_DELETE) != 0;
}

// TODO - where should this go?
static inline bool
as_msg_is_xdr(const as_msg *m)
{
	return (m->info1 & AS_MSG_INFO1_XDR) != 0;
}

static inline bool
as_transaction_is_xdr(const as_transaction *tr)
{
	return (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR) != 0;
}

static inline bool
as_transaction_is_linearized_read(const as_transaction *tr)
{
	return (tr->msgp->msg.info3 & AS_MSG_INFO3_SC_READ_RELAX) == 0 &&
			(tr->msgp->msg.info3 & AS_MSG_INFO3_SC_READ_TYPE) != 0;
}

static inline bool
as_transaction_is_allow_unavailable_read(const as_transaction *tr)
{
	return (tr->msgp->msg.info3 & AS_MSG_INFO3_SC_READ_RELAX) != 0 &&
			(tr->msgp->msg.info3 & AS_MSG_INFO3_SC_READ_TYPE) != 0;
}

static inline bool
as_transaction_is_strict_read(const as_transaction *tr)
{
	return (tr->msgp->msg.info3 & AS_MSG_INFO3_SC_READ_RELAX) == 0;
}

void as_transaction_init_iudf(as_transaction *tr, struct as_namespace_s *ns, cf_digest *keyd, struct iudf_origin_s *iudf_orig, bool is_durable_delete);

void as_transaction_demarshal_error(as_transaction *tr, uint32_t error_code);
void as_transaction_error(as_transaction *tr, struct as_namespace_s *ns, uint32_t error_code);
void as_multi_rec_transaction_error(as_transaction *tr, uint32_t error_code);
