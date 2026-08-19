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
struct as_namespace_s;
struct as_transaction_s;
struct cl_msg_s;
struct iops_origin_s;
struct iudf_origin_s;
struct monitor_roll_origin_s;
struct proxy_origin_s;
struct rw_request_s;
struct rw_wait_ele_s;

//==========================================================
// Typedefs & constants.
//

typedef bool (*dup_res_done_cb)(struct rw_request_s* rw);
typedef void (*repl_write_done_cb)(struct rw_request_s* rw);
typedef void (*repl_ping_done_cb)(struct rw_request_s* rw);
typedef void (*timeout_done_cb)(struct rw_request_s* rw);

typedef struct rw_request_s {

	//------------------------------------------------------
	// Matches as_transaction.
	//

	struct cl_msg_s* msgp;
	uint32_t msg_fields;

	uint8_t origin;
	uint8_t from_flags;

	union {
		void* any;
		struct as_file_handle_s* proto_fd_h;
		struct proxy_origin_s* proxy_orig;
		struct as_batch_shared_s* batch_shared;
		struct iudf_origin_s* iudf_orig;
		struct iops_origin_s* iops_orig;
		struct monitor_roll_origin_s* monitor_roll_orig;
	} from;

	union {
		uint32_t any;
		uint32_t batch_index;
		uint32_t proxy_tid;
	} from_data;

	cf_digest keyd;

	uint64_t start_time;
	uint64_t benchmark_time;

	as_partition_reservation rsv;

	uint64_t end_time;
	uint8_t result_code;
	uint8_t flags;
	uint16_t generation;
	uint32_t void_time;
	uint64_t last_update_time;

	//
	// End of as_transaction look-alike.
	//------------------------------------------------------

	cf_mutex lock;

	struct rw_wait_ele_s* wait_queue_head;
	struct rw_wait_ele_s* wait_queue_tail;
	uint32_t wait_queue_depth;

	bool is_set_up; // TODO - redundant with timeout_cb

	// Store pickled data, for use in replica write.
	uint8_t* pickle;
	size_t pickle_sz;

	// Wire-compression state. Populated by compute_delta_for_replication and
	// compute_compression_for_replication when their respective namespace
	// modes are active. Only one of use_delta / use_compressed is true.
	bool use_delta;
	bool use_compressed;
	// True only when fill_repl_write_message() actually emitted a delta or
	// compressed op on the wire. use_delta/use_compressed record what was
	// computed; this records what was sent. They diverge when the rolling-
	// upgrade compatibility gate forces a plain RW_OP_REPL_WRITE even though a
	// compressed payload was computed. repl_write_handle_ack() credits
	// bytes_saved off this flag, so a compat-gated plain send never over-credits
	// savings for a record that actually went out uncompressed.
	bool wire_compressed_op_sent;
	uint8_t* delta;
	size_t delta_sz;
	uint8_t* compressed;
	size_t compressed_sz;
	// Snapshot of the OLD record's flat bytes, taken by
	// capture_delta_base_for_replication() BEFORE as_storage_record_write().
	// compute_delta_for_replication() patches against this instead of rd->flat,
	// which for in-memory namespaces aliases the storage arena the write frees
	// in place (use-after-free). NULL when delta mode is off or there is no old
	// version.
	uint8_t* delta_base;
	uint32_t delta_base_sz;
	// Set by capture_delta_base_for_replication() when the old record's
	// stored flat is storage-compressed. The bin-load path decompresses into
	// a separate buffer - rd->flat_end no longer points into rd->flat's
	// allocation - so no coherent base snapshot exists, and
	// compute_delta_for_replication() must skip the delta without touching
	// rd->flat (freed in place by the write for in-memory namespaces).
	bool delta_base_storage_compressed;
	uint32_t base_generation;
	uint64_t base_lut;
	// Per-destination bytes saved by the chosen wire-compression path
	// (pickle_sz - delta_sz or pickle_sz - compressed_sz). Credited to
	// ns->repl_wire_comp_stat.bytes_saved one destination at a time on
	// successful ack, so the counter reflects bytes actually saved on the
	// wire rather than bytes we hoped to save. Zero if compression was
	// not beneficial.
	//
	// On the fire-and-forget path (write-commit-level master) there is no ack,
	// so send_rw_messages_forget() credits it per accepted destination at send
	// time. Exactly one of the two sites ever runs for a given rw_request.
	uint64_t per_dest_bytes_saved;
	// Info bits computed from the originating transaction at first build.
	// Re-emitted verbatim on delta-fallback retransmit so the rebuilt
	// message preserves bits like RW_INFO_NO_REPL_ACK that would otherwise
	// be lost on a zero-initialized synthetic transaction.
	uint32_t repl_info_bits;
	bool repl_info_bits_set;
	const char* set_name; // points directly into vmap - never free it
	uint32_t set_name_len;
	uint8_t* key;
	uint32_t key_size;

	// Store ops' responses here.
	cf_dyn_buf response_db;

	// Manage responses for duplicate resolution and replica write requests, or
	// alternatively, timeouts.
	uint32_t tid;
	bool dup_res_complete;
	bool repl_write_w_orig; // enterprise only
	bool repl_write_complete;
	bool repl_ping_complete;
	dup_res_done_cb dup_res_cb;
	repl_write_done_cb repl_write_cb;
	repl_ping_done_cb repl_ping_cb;
	timeout_done_cb timeout_cb;

	// Message being sent to dest_nodes. May be duplicate resolution or replica
	// write request. Message is kept in case it needs to be retransmitted.
	msg* dest_msg;

	uint64_t xmit_ms; // time of next retransmit
	uint32_t retry_interval_ms; // interval to add for next retransmit

	// Destination info for duplicate resolution and replica write requests.
	uint32_t n_dest_nodes;
	cf_node dest_nodes[AS_CLUSTER_SZ];
	bool dest_complete[AS_CLUSTER_SZ];

	// Duplicate resolution response messages from nodes with duplicates.
	msg* best_dup_msg;
	// TODO - could store best dup node-id - worth it?
	uint8_t best_dup_result_code;
	uint16_t best_dup_gen;
	uint64_t best_dup_lut;

	bool tie_was_replicated; // enterprise only

	// Node health related stat, to track replication latency.
	uint64_t repl_start_us;

	// Dedicated replication-latency histogram sample timestamp. Always
	// set (cheap) so the histogram captures every replica write when
	// enable-benchmarks-repl is on. Distinct from repl_start_us, which is
	// the sampled-subset timing used by the as_health outlier detector.
	uint64_t repl_start_ns;

} rw_request;

//==========================================================
// Public API.
//

rw_request* rw_request_create(cf_digest* keyd);
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
