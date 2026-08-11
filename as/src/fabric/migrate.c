/*
 * migrate.c
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

//==========================================================
// Includes.
//

#include "fabric/migrate.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"
#include "msg.h"
#include "node.h"
#include "rchash.h"
#include "shash.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/zstd_wire.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/meta_batch.h"
#include "fabric/migrate_test_support.h" // reinsert internals (also used by unit tests)
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"

//==========================================================
// Typedefs & constants.
//

// clang-format off
const msg_template migrate_mt[] = {
		{ MIG_FIELD_OP, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_1, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_ID, M_FT_UINT32 },
		{ MIG_FIELD_NAMESPACE, M_FT_BUF },
		{ MIG_FIELD_PARTITION, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_5, M_FT_BUF },
		{ MIG_FIELD_COMPRESSION, M_FT_UINT32 },
		{ MIG_FIELD_RECORD, M_FT_BUF },
		{ MIG_FIELD_CLUSTER_KEY, M_FT_UINT64 },
		{ MIG_FIELD_ORIG_RECORD, M_FT_BUF },
		{ MIG_FIELD_UNUSED_10, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_11, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_12, M_FT_BUF },
		{ MIG_FIELD_INFO, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_14, M_FT_UINT64 },
		{ MIG_FIELD_UNUSED_15, M_FT_BUF },
		{ MIG_FIELD_UNUSED_16, M_FT_BUF },
		{ MIG_FIELD_UNUSED_17, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_18, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_19, M_FT_UINT64 },
		{ MIG_FIELD_FEATURES, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_21, M_FT_UINT32 },
		{ MIG_FIELD_META_RECORDS, M_FT_BUF },
		{ MIG_FIELD_META_SEQUENCE, M_FT_UINT32 },
		{ MIG_FIELD_META_SEQUENCE_FINAL, M_FT_UINT32 },
		{ MIG_FIELD_PARTITION_SIZE, M_FT_UINT64 },
		{ MIG_FIELD_UNUSED_26, M_FT_BUF },
		{ MIG_FIELD_UNUSED_27, M_FT_BUF },
		{ MIG_FIELD_UNUSED_28, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_INSERT_ID, M_FT_UINT64 }
};
// clang-format on

COMPILER_ASSERT(sizeof(migrate_mt) / sizeof(msg_template) == NUM_MIG_FIELDS);

#define MIG_MSG_SCRATCH_SIZE 192

#define EMIGRATION_SLOW_Q_WAIT_MS 1000 // 1 second
#define MIGRATE_RETRANSMIT_STARTDONE_MS 1000 // for now, not configurable
#define MIGRATE_RETRANSMIT_SIGNAL_MS 1000 // for now, not configurable
#define MAX_BYTES_EMIGRATING (32 * 1024 * 1024)

#define IMMIGRATION_DEBOUNCE_MS (60 * 1000) // 1 minute

typedef enum {
	EMIG_START_RESULT_OK,
	EMIG_START_RESULT_ERROR,
	EMIG_START_RESULT_EAGAIN
} emigration_start_result;

typedef enum {
	// Order matters - we use an atomic set-max that relies on it.
	EMIG_STATE_ACTIVE,
	EMIG_STATE_FINISHED,
	EMIG_STATE_ABORTED
} emigration_state;

typedef struct emigration_pop_info_s {
	uint32_t order;
	uint64_t dest_score;
	uint32_t type;
	uint64_t n_elements;

	uint64_t avoid_dest;
} emigration_pop_info;

//==========================================================
// Globals.
//

cf_rchash* g_emigration_hash = NULL;
cf_rchash* g_immigration_hash = NULL;
cf_queue g_emigration_q;

static uint64_t g_avoid_dest = 0;
static uint32_t g_emigration_id = 0;
static cf_queue g_emigration_slow_q;

//==========================================================
// Forward declarations.
//

// Various initializers and destructors.
void emigration_init(emigration* emig);
void emigration_destroy(void* parm);
int emigration_reinsert_destroy_reduce_fn(const void* key, void* data,
		void* udata);
void immigration_destroy(void* parm);

// Emigration.
void* run_emigration(void* arg);
void* run_emigration_slow(void* arg);
void emigration_pop(emigration** emigp);
int emigration_pop_reduce_fn(void* buf, void* udata);
void emigration_hash_insert(emigration* emig);
void emigration_hash_delete(emigration* emig);
bool emigrate_transfer(emigration* emig);
void emigrate_signal(emigration* emig);
emigration_start_result emigration_send_start(emigration* emig);
bool emigrate_tree(emigration* emig);
bool emigration_send_done(emigration* emig);
void* run_emigration_reinserter(void* arg);
bool emigrate_tree_reduce_fn(as_index_ref* r_ref, void* udata);
int emigration_reinsert_reduce_fn(const void* key, void* data, void* udata);
void emigrate_record(emigration* emig, msg* m, cf_digest* keyd, uint64_t lut,
		uint64_t bytes_saved);
static uint64_t migration_compression_bytes_saved(size_t raw_pickle_sz,
		size_t compressed_pickle_sz, size_t raw_orig_pickle_sz,
		size_t compressed_orig_pickle_sz);

// Migration compression is fully independent of replication compression —
// see ns_migrate_compression_mode in datamodel.h. Helpers stay for readability
// at the call sites and to keep the policy decision in one place.
static inline bool
migrate_zstd_enabled(const as_namespace* ns)
{
	return ns->migrate_compression_mode ==
			AS_NAMESPACE_MIGRATE_COMPRESSION_MODE_ZSTD;
}

static inline int32_t
migrate_zstd_level(const as_namespace* ns)
{
	return ns->migrate_compression_level;
}

// Immigration.
uint32_t immigration_hashfn(const void* key);
void* run_immigration_reaper(void* arg);
int immigration_reaper_reduce_fn(const void* key, void* object, void* udata);

// Migrate fabric message handling.
int migrate_receive_msg_cb(cf_node src, msg* m, void* udata);
void immigration_handle_start_request(cf_node src, msg* m);
void immigration_ack_start_request(cf_node src, msg* m, uint32_t op);
void immigration_ack_insert_request(cf_node src, msg* m);
void immigration_handle_insert_request(cf_node src, msg* m);
void immigration_handle_done_request(cf_node src, msg* m);
void immigration_handle_all_done_request(cf_node src, msg* m);
void emigration_handle_insert_ack(cf_node src, msg* m);
void emigration_handle_ctrl_ack(cf_node src, msg* m, uint32_t op);

// Info API helpers.
int emigration_dump_reduce_fn(const void* key, void* object, void* udata);
int immigration_dump_reduce_fn(const void* key, void* object, void* udata);

//==========================================================
// Public API.
//

void
as_migrate_init()
{
	g_avoid_dest = (uint64_t)g_config.self_node;

	cf_queue_init(&g_emigration_q, sizeof(emigration*), 4096, true);
	cf_queue_init(&g_emigration_slow_q, sizeof(emigration*), 4096, true);

	g_emigration_hash = cf_rchash_create(cf_rchash_fn_u32, emigration_destroy,
			sizeof(uint32_t), 64);

	g_immigration_hash = cf_rchash_create(immigration_hashfn,
			immigration_destroy, sizeof(immigration_hkey), 64);

	// Looks like an as_priority_thread_pool, but the reduce-pop is different.
	for (uint32_t i = 0; i < g_config.n_migrate_threads; i++) {
		cf_thread_create_transient(run_emigration, NULL);
	}

	cf_thread_create_detached(run_emigration_slow, NULL);
	cf_thread_create_detached(run_immigration_reaper, NULL);

	emigrate_fill_queue_init();

	as_fabric_register_msg_fn(M_TYPE_MIGRATE, migrate_mt, sizeof(migrate_mt),
			MIG_MSG_SCRATCH_SIZE, migrate_receive_msg_cb, NULL);
}

// Kicks off an emigration.
void
as_migrate_emigrate(const pb_task* task)
{
	emigration* emig = cf_rc_alloc(sizeof(emigration));

	emig->dest = task->dest;
	emig->cluster_key = task->cluster_key;
	emig->id = as_faa_uint32(&g_emigration_id, 1);
	emig->type = task->type;
	emig->tx_flags = task->tx_flags;
	emig->state = EMIG_STATE_ACTIVE;

	// Create these later only when we need them - we'll get lots at once.
	emig->bytes_emigrating = 0;
	emig->reinsert_hash = NULL;
	emig->insert_id = 0;
	emig->ctrl_q = NULL;
	emig->meta_q = NULL;

	as_partition_reserve(task->ns, task->pid, &emig->rsv);

	as_incr_uint64(&emig->rsv.ns->migrate_tx_instance_count);

	emigrate_queue_push(emig);
}

// Called via info command. Caller has sanity-checked n_threads.
void
as_migrate_set_num_xmit_threads(uint32_t n_threads)
{
	if (g_config.n_migrate_threads > n_threads) {
		// Decrease the number of migrate transmit threads to n_threads.
		while (g_config.n_migrate_threads > n_threads) {
			void* death_msg = NULL;

			// Send terminator (NULL message).
			cf_queue_push(&g_emigration_q, &death_msg);
			g_config.n_migrate_threads--;
		}
	}
	else {
		// Increase the number of migrate transmit threads to n_threads.
		while (g_config.n_migrate_threads < n_threads) {
			cf_thread_create_transient(run_emigration, NULL);
			g_config.n_migrate_threads++;
		}
	}
}

// Called via info command - print information about migration to the log.
void
as_migrate_dump(bool verbose)
{
	cf_info(AS_MIGRATE, "migration info:");
	cf_info(AS_MIGRATE, "---------------");
	cf_info(AS_MIGRATE, "number of emigrations in g_emigration_hash: %d",
			cf_rchash_get_size(g_emigration_hash));
	cf_info(AS_MIGRATE,
			"number of requested emigrations waiting in g_emigration_q : %u",
			cf_queue_sz(&g_emigration_q));
	cf_info(AS_MIGRATE,
			"number of requested emigrations waiting in g_emigration_slow_q : %u",
			cf_queue_sz(&g_emigration_slow_q));
	cf_info(AS_MIGRATE, "number of immigrations in g_immigration_hash: %d",
			cf_rchash_get_size(g_immigration_hash));
	cf_info(AS_MIGRATE, "current emigration id: %d", g_emigration_id);

	if (verbose) {
		int item_num = 0;

		if (cf_rchash_get_size(g_emigration_hash) > 0) {
			cf_info(AS_MIGRATE, "contents of g_emigration_hash:");
			cf_info(AS_MIGRATE, "------------------------------");

			cf_rchash_reduce(g_emigration_hash, emigration_dump_reduce_fn,
					&item_num);
		}

		if (cf_rchash_get_size(g_immigration_hash) > 0) {
			item_num = 0;

			cf_info(AS_MIGRATE, "contents of g_immigration_hash:");
			cf_info(AS_MIGRATE, "-------------------------------");

			cf_rchash_reduce(g_immigration_hash, immigration_dump_reduce_fn,
					&item_num);
		}
	}
}

//==========================================================
// Local helpers - various initializers and destructors.
//

void
emigration_init(emigration* emig)
{
	// emig->features is written by the fabric receive thread on START_ACK
	// (emigration_handle_ctrl_ack) and read by the emigration worker thread
	// (emigrate_tree_reduce_fn). Use atomic load/store on every access so the
	// worker can't observe a torn/stale value.
	as_store_uint32(&emig->features, 0);
	emig->reinsert_hash = cf_shash_create(cf_shash_fn_u32, sizeof(uint64_t),
			sizeof(emigration_reinsert_ctrl), 16 * 1024, true);
	emig->ctrl_q = cf_queue_create(sizeof(int), true);
	emig->meta_q = meta_in_q_create();
}

// Destructor handed to rchash.
void
emigration_destroy(void* parm)
{
	emigration* emig = (emigration*)parm;

	if (emig->reinsert_hash) {
		cf_shash_reduce(emig->reinsert_hash,
				emigration_reinsert_destroy_reduce_fn, NULL);
		cf_shash_destroy(emig->reinsert_hash);
	}

	if (emig->ctrl_q) {
		cf_queue_destroy(emig->ctrl_q);
	}

	if (emig->meta_q) {
		meta_in_q_destroy(emig->meta_q);
	}

	as_partition_release(&emig->rsv);

	as_decr_uint64(&emig->rsv.ns->migrate_tx_instance_count);
}

int
emigration_reinsert_destroy_reduce_fn(const void* key, void* data, void* udata)
{
	emigration_reinsert_ctrl* ri_ctrl = (emigration_reinsert_ctrl*)data;

	as_fabric_msg_put(ri_ctrl->m);

	return CF_SHASH_REDUCE_DELETE;
}

void
emigration_release(emigration* emig)
{
	if (cf_rc_release(emig) == 0) {
		emigration_destroy((void*)emig);
		cf_rc_free(emig);
	}
}

// Destructor handed to rchash.
void
immigration_destroy(void* parm)
{
	immigration* immig = (immigration*)parm;

	if (immig->rsv.p) {
		as_partition_release(&immig->rsv);
	}

	if (immig->meta_q) {
		meta_out_q_destroy(immig->meta_q);
	}

	as_decr_uint64(&immig->ns->migrate_rx_instance_count);
}

void
immigration_release(immigration* immig)
{
	if (cf_rc_release(immig) == 0) {
		immigration_destroy((void*)immig);
		cf_rc_free(immig);
	}
}

//==========================================================
// Local helpers - emigration.
//

void*
run_emigration(void* arg)
{
	while (true) {
		emigration* emig;

		emigration_pop(&emig);

		// This is the case for intentionally stopping the migrate thread.
		if (! emig) {
			break; // signal of death
		}

		as_partition_balance_emigration_yield();

		if (emig->cluster_key != as_exchange_cluster_key()) {
			emigration_hash_delete(emig);
			continue;
		}

		as_namespace* ns = emig->rsv.ns;
		bool requeued = false;

		// Add the emigration to the global hash so acks can find it.
		emigration_hash_insert(emig);

		switch (emig->type) {
		case PB_TASK_EMIG_TRANSFER:
			as_incr_uint64(&ns->migrate_tx_partitions_active);
			requeued = emigrate_transfer(emig);
			as_decr_uint64(&ns->migrate_tx_partitions_active);
			break;
		case PB_TASK_EMIG_SIGNAL_ALL_DONE:
			as_incr_uint64(&ns->migrate_signals_active);
			emigrate_signal(emig);
			as_decr_uint64(&ns->migrate_signals_active);
			break;
		default:
			cf_crash(AS_MIGRATE, "bad emig type %u", emig->type);
			break;
		}

		if (! requeued) {
			emigration_hash_delete(emig);
		}
	}

	return NULL;
}

void*
run_emigration_slow(void* arg)
{
	while (true) {
		emigration* emig;

		if (cf_queue_pop(&g_emigration_slow_q, (void*)&emig, CF_QUEUE_FOREVER) !=
				CF_QUEUE_OK) {
			cf_crash(AS_MIGRATE, "emigration slow queue pop failed");
		}

		uint64_t now_ms = cf_getms();

		if (emig->wait_until_ms > now_ms) {
			usleep(1000 * (emig->wait_until_ms - now_ms));
		}

		cf_queue_push(&g_emigration_q, &emig);
	}

	return NULL;
}

void
emigration_pop(emigration** emigp)
{
	emigration_pop_info best;

	best.order = 0xFFFFffff;
	best.dest_score = 0;
	best.type = 0;
	best.n_elements = 0xFFFFffffFFFFffff;

	best.avoid_dest = 0;

	if (cf_queue_reduce_pop(&g_emigration_q, (void*)emigp, CF_QUEUE_FOREVER,
				emigration_pop_reduce_fn, &best) != CF_QUEUE_OK) {
		cf_crash(AS_MIGRATE, "emigration queue reduce pop failed");
	}
}

int
emigration_pop_reduce_fn(void* buf, void* udata)
{
	emigration_pop_info* best = (emigration_pop_info*)udata;
	emigration* emig = *(emigration**)buf;

	if (! emig || // null emig terminates thread
			emig->cluster_key != as_exchange_cluster_key()) {
		return -1; // process immediately
	}

	if (emig->ctrl_q && cf_queue_sz(emig->ctrl_q) != 0) {
		// This emig was requeued after its start command got an ACK_EAGAIN,
		// likely because dest hit 'migrate-max-num-incoming'. A new ack has
		// arrived - if it's ACK_OK, don't leave remote node hanging.

		return -1; // process immediately
	}

	if (emig->type == PB_TASK_EMIG_SIGNAL_ALL_DONE) {
		return -1; // process immediately
	}

	if (best->avoid_dest == 0) {
		best->avoid_dest = g_avoid_dest;
	}

	uint32_t order = emig->rsv.ns->migrate_order;
	uint64_t dest_score = (uint64_t)emig->dest - best->avoid_dest;
	uint32_t type = (emig->tx_flags & TX_FLAGS_LEAD) != 0
			? 2
			: ((emig->tx_flags & TX_FLAGS_CONTINGENT) != 0 ? 1 : 0);
	uint64_t n_elements = as_index_tree_size(emig->rsv.tree);

	if (order < best->order ||
			(order == best->order &&
					(dest_score > best->dest_score ||
							(dest_score == best->dest_score &&
									(type > best->type ||
											(type == best->type &&
													n_elements <
															best->n_elements)))))) {
		best->order = order;
		best->dest_score = dest_score;
		best->type = type;
		best->n_elements = n_elements;

		g_avoid_dest = (uint64_t)emig->dest;

		return -2; // candidate
	}

	return 0; // not interested
}

void
emigration_hash_insert(emigration* emig)
{
	if (! emig->ctrl_q) {
		emigration_init(emig); // creates emig->ctrl_q etc.

		cf_rchash_put(g_emigration_hash, (void*)&emig->id, (void*)emig);
	}
}

void
emigration_hash_delete(emigration* emig)
{
	if (emig->ctrl_q) {
		cf_rchash_delete(g_emigration_hash, (void*)&emig->id);
	}
	else {
		emigration_release(emig);
	}
}

bool
emigrate_transfer(emigration* emig)
{
	//--------------------------------------------
	// Send START request.
	//

	emigration_start_result result = emigration_send_start(emig);

	if (result == EMIG_START_RESULT_EAGAIN) {
		// Remote node refused migration, requeue and fetch another.
		emig->wait_until_ms = cf_getms() + EMIGRATION_SLOW_Q_WAIT_MS;

		cf_queue_push(&g_emigration_slow_q, &emig);

		return true; // requeued
	}

	if (result != EMIG_START_RESULT_OK) {
		return false; // did not requeue
	}

	//--------------------------------------------
	// Send whole tree - may block a while.
	//

	if (! emigrate_tree(emig)) {
		return false; // did not requeue
	}

	//--------------------------------------------
	// Send DONE request.
	//

	if (emigration_send_done(emig)) {
		as_partition_emigrate_done(emig->rsv.ns, emig->rsv.p->id,
				emig->cluster_key, emig->dest, emig->tx_flags);
	}

	return false; // did not requeue
}

void
emigrate_signal(emigration* emig)
{
	as_namespace* ns = emig->rsv.ns;
	msg* m = as_fabric_msg_get(M_TYPE_MIGRATE);

	switch (emig->type) {
	case PB_TASK_EMIG_SIGNAL_ALL_DONE:
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_ALL_DONE);
		break;
	default:
		cf_crash(AS_MIGRATE, "signal: bad emig type %u", emig->type);
		break;
	}

	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_uint64(m, MIG_FIELD_CLUSTER_KEY, emig->cluster_key);
	msg_set_buf(m, MIG_FIELD_NAMESPACE, (const uint8_t*)ns->name,
			strlen(ns->name), MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_PARTITION, emig->rsv.p->id);

	uint64_t signal_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return;
		}

		uint64_t now = cf_getms();

		if (signal_xmit_ms + MIGRATE_RETRANSMIT_SIGNAL_MS < now) {
			as_fabric_retransmit(emig->dest, m, AS_FABRIC_CHANNEL_CTRL);
			signal_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_SIGNAL_MS) ==
				CF_QUEUE_OK) {
			switch (op) {
			case OPERATION_ALL_DONE_ACK:
				as_partition_signal_done(ns, emig->rsv.p->id, emig->cluster_key);
				as_fabric_msg_put(m);
				return;
			default:
				cf_warning(AS_MIGRATE, "signal: unexpected ctrl op %d", op);
				break;
			}
		}
	}
}

emigration_start_result
emigration_send_start(emigration* emig)
{
	as_namespace* ns = emig->rsv.ns;
	msg* m = as_fabric_msg_get(M_TYPE_MIGRATE);

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START);
	msg_set_uint32(m, MIG_FIELD_FEATURES, MY_MIG_FEATURES);
	msg_set_uint64(m, MIG_FIELD_PARTITION_SIZE,
			as_index_tree_size(emig->rsv.tree));
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_uint64(m, MIG_FIELD_CLUSTER_KEY, emig->cluster_key);
	msg_set_buf(m, MIG_FIELD_NAMESPACE, (const uint8_t*)ns->name,
			strlen(ns->name), MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_PARTITION, emig->rsv.p->id);

	uint64_t start_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return EMIG_START_RESULT_ERROR;
		}

		uint64_t now = cf_getms();

		if (cf_queue_sz(emig->ctrl_q) == 0 &&
				start_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			as_fabric_retransmit(emig->dest, m, AS_FABRIC_CHANNEL_CTRL);
			start_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_STARTDONE_MS) ==
				CF_QUEUE_OK) {
			switch (op) {
			case OPERATION_START_ACK_OK:
				as_fabric_msg_put(m);
				return EMIG_START_RESULT_OK;
			case OPERATION_START_ACK_EAGAIN:
				as_fabric_msg_put(m);
				return EMIG_START_RESULT_EAGAIN;
			case OPERATION_START_ACK_FAIL:
				cf_warning(AS_MIGRATE,
						"imbalance: %lx refused migrate with ACK_FAIL",
						emig->dest);
				as_incr_uint64(&ns->migrate_tx_partitions_imbalance);
				as_fabric_msg_put(m);
				return EMIG_START_RESULT_ERROR;
			default:
				cf_warning(AS_MIGRATE, "unexpected ctrl op %d", op);
				break;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return EMIG_START_RESULT_ERROR;
}

bool
emigrate_tree(emigration* emig)
{
	if (as_index_tree_size(emig->rsv.tree) == 0) {
		return true;
	}

	emig->state = EMIG_STATE_ACTIVE;

	cf_tid tid =
			cf_thread_create_joinable(run_emigration_reinserter, (void*)emig);

	if (as_index_reduce(emig->rsv.tree, emigrate_tree_reduce_fn, emig)) {
		// Sets EMIG_STATE_FINISHED only if not already EMIG_STATE_ABORTED.
		as_setmax_uint32(&emig->state, EMIG_STATE_FINISHED);
	}
	else {
		emig->state = EMIG_STATE_ABORTED;
	}

	cf_thread_join(tid);

	return emig->state != EMIG_STATE_ABORTED;
}

bool
emigration_send_done(emigration* emig)
{
	as_namespace* ns = emig->rsv.ns;

	if (! as_partition_pre_emigrate_done(ns, emig->rsv.p->id, emig->cluster_key,
				emig->tx_flags)) {
		return false;
	}

	msg* m = as_fabric_msg_get(M_TYPE_MIGRATE);

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE);
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);

	uint64_t done_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return false;
		}

		uint64_t now = cf_getms();

		if (done_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			as_fabric_retransmit(emig->dest, m, AS_FABRIC_CHANNEL_CTRL);
			done_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_STARTDONE_MS) ==
				CF_QUEUE_OK) {
			if (op == OPERATION_DONE_ACK) {
				as_fabric_msg_put(m);
				return true;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return false;
}

void*
run_emigration_reinserter(void* arg)
{
	emigration* emig = (emigration*)arg;
	emigration_state emig_state;

	// Reduce over the reinsert hash until finished.
	while ((emig_state = as_load_uint32(&emig->state)) != EMIG_STATE_ABORTED) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			emig->state = EMIG_STATE_ABORTED;
			return NULL;
		}

		usleep(1000);

		if (cf_shash_get_size(emig->reinsert_hash) == 0) {
			if (emig_state == EMIG_STATE_FINISHED) {
				return NULL;
			}

			continue;
		}

		cf_shash_reduce(emig->reinsert_hash, emigration_reinsert_reduce_fn,
				(void*)cf_getms());
	}

	return NULL;
}

// Free the per-record compressed buffers and reset them so the caller falls
// through to the plain-pickle send path. cf_free(NULL) is a no-op, so this is
// safe whether or not the buffers were allocated.
static inline void
emigrate_drop_compressed(uint8_t** pickle, size_t* pickle_sz,
		uint8_t** orig_pickle, size_t* orig_pickle_sz)
{
	cf_free(*pickle);
	cf_free(*orig_pickle);
	*pickle = NULL;
	*orig_pickle = NULL;
	*pickle_sz = 0;
	*orig_pickle_sz = 0;
}

// Post-send bookkeeping shared by the compressed and plain paths of
// emigrate_tree_reduce_fn: count the record, apply the migrate-sleep pacing,
// then block while we're over the in-flight byte ceiling. Returns whether this
// emigration's cluster key is still current - the reduce-fn's "keep going?"
// signal.
static bool
emigrate_throttle_after_send(emigration* emig, as_namespace* ns)
{
	as_incr_uint64(&ns->migrate_records_transmitted);

	if (ns->migrate_sleep != 0) {
		usleep(ns->migrate_sleep);
	}

	uint32_t waits = 0;

	while (emig->bytes_emigrating > MAX_BYTES_EMIGRATING &&
			emig->cluster_key == as_exchange_cluster_key()) {
		usleep(1000);

		// Temporary paranoia to inform us old nodes aren't acking properly.
		if (++waits % (ns->migrate_retransmit_ms * 4) == 0) {
			cf_warning(AS_MIGRATE, "missing acks from node %lx", emig->dest);
		}
	}

	return emig->cluster_key == as_exchange_cluster_key();
}

bool
emigrate_tree_reduce_fn(as_index_ref* r_ref, void* udata)
{
	emigration* emig = (emigration*)udata;
	as_namespace* ns = emig->rsv.ns;
	as_record* r = r_ref->r;

	if (! should_emigrate_record(emig, r_ref)) {
		as_record_done(r_ref, ns);
		return emig->cluster_key == as_exchange_cluster_key();
	}

	msg* m = as_fabric_msg_get(M_TYPE_MIGRATE);

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT);
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);

	uint32_t info = emigration_pack_info(ns, r);
	bool try_compress =
			(as_load_uint32(&emig->features) & MIG_FEATURE_ZSTD) != 0 &&
			migrate_zstd_enabled(ns);

	if (info != 0) {
		msg_set_uint32(m, MIG_FIELD_INFO, info);
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	if (as_storage_record_load_pickle(&rd)) {
		uint8_t* orig_pickle = NULL;
		uint32_t orig_pickle_sz = 0;

		if (mrt_load_orig_pickle(ns, r, &orig_pickle, &orig_pickle_sz)) {
			migrate_wire_compression compression = MIGRATE_COMPRESSION_NONE;
			uint8_t* compressed_pickle = NULL;
			size_t compressed_pickle_sz = 0;
			uint8_t* compressed_orig_pickle = NULL;
			size_t compressed_orig_pickle_sz = 0;
			uint64_t wire_bytes_saved = 0; // credited at insert-ack
			// Only consumed by the try_compress blocks below; short-circuit on
			// try_compress so we don't pay a full flat meta-unpack (twice when
			// an orig exists) per emigrated record when migration compression
			// is off or the peer never advertised MIG_FEATURE_ZSTD.
			bool has_storage_compressed_pickle = try_compress &&
					as_flat_pickle_is_storage_compressed(rd.pickle, rd.pickle_sz);
			bool has_storage_compressed_orig_pickle = try_compress &&
					orig_pickle != NULL &&
					as_flat_pickle_is_storage_compressed(orig_pickle,
							orig_pickle_sz);

			if (try_compress &&
					(has_storage_compressed_pickle ||
							has_storage_compressed_orig_pickle)) {
				cf_debug(AS_MIGRATE,
						"emigrate_tree_reduce_fn: skipping wire compression for storage-compressed pickle %pD",
						&r->keyd);
			}

			if (try_compress && ! has_storage_compressed_pickle &&
					! has_storage_compressed_orig_pickle &&
					// Skip records too small to beat zstd's framing overhead -
					// don't pay a per-record compress + alloc for no gain.
					rd.pickle_sz >= ZSTD_WIRE_COMPRESS_MIN_SZ) {
				int32_t level = migrate_zstd_level(ns);

				compressed_pickle_sz =
						zstd_wire_compress_buffer((void**)&compressed_pickle,
								rd.pickle, rd.pickle_sz, level);

				if (orig_pickle == NULL) {
					// Sentinel value indicating no orig_pickle. A real compressed
					// buffer cannot be 1 byte, so this is a safe sentinel.
					compressed_orig_pickle_sz = 1;
				}
				else {
					// No separate size threshold for orig_pickle: MIG_FIELD_COMPRESSION
					// is one per-message flag and the receiver decompresses both
					// MIG_FIELD_RECORD and MIG_FIELD_ORIG_RECORD together, so a raw
					// orig cannot ride inside a ZSTD message. Compressing a tiny orig
					// is cheap, and migration_compression_bytes_saved() weighs the
					// combined pickle+orig result and drops it if not beneficial.
					compressed_orig_pickle_sz =
							zstd_wire_compress_buffer((void**)&compressed_orig_pickle,
									orig_pickle, orig_pickle_sz, level);
				}

				if (compressed_pickle_sz != 0 && compressed_orig_pickle_sz != 0) {
					uint64_t saved =
							migration_compression_bytes_saved(rd.pickle_sz,
									compressed_pickle_sz, orig_pickle_sz,
									compressed_orig_pickle_sz);

					if (saved == 0) {
						// Compressed payload is no smaller than the
						// plain pickles — sending it would only cost the
						// receiver a decompress for no gain.
						as_incr_uint64(&ns->migrate_wire_comp_stat
										.compression_not_beneficial);
						emigrate_drop_compressed(&compressed_pickle,
								&compressed_pickle_sz, &compressed_orig_pickle,
								&compressed_orig_pickle_sz);
					}
					else {
						compression = MIGRATE_COMPRESSION_ZSTD;
						// bytes_saved credited at ack-time via ri_ctrl,
						// not here.
						wire_bytes_saved = saved;
					}
				}
				else {
					// The codec itself failed (e.g. allocation failure) -
					// nothing we can guard against here, so send the record
					// plain and count it.
					//
					// This is now the ONLY contributor to migration's
					// fallback_count: the other one was the NACK-driven
					// uncompressed rebuild, removed with the fallback itself. So
					// for migration the stat means "codec failed on send, record
					// went plain", NOT "fell back after a peer refused it" - the
					// name predates the change and is kept because
					// migrate_wire_compression_fallbacks is already on the info
					// surface. Read it against the codec, not against peer
					// behavior.
					as_incr_uint64(&ns->migrate_wire_comp_stat.fallback_count);
					emigrate_drop_compressed(&compressed_pickle,
							&compressed_pickle_sz, &compressed_orig_pickle,
							&compressed_orig_pickle_sz);
				}
			}

			if (compression != MIGRATE_COMPRESSION_NONE) {
				msg_set_uint32(m, MIG_FIELD_COMPRESSION, compression);
				msg_set_buf(m, MIG_FIELD_RECORD, compressed_pickle,
						compressed_pickle_sz, MSG_SET_HANDOFF_MALLOC);

				if (orig_pickle != NULL) {
					msg_set_buf(m, MIG_FIELD_ORIG_RECORD, compressed_orig_pickle,
							compressed_orig_pickle_sz, MSG_SET_HANDOFF_MALLOC);
				}

				// The compressed buffers are what's on the wire and the msg
				// owns them - the raw pickles are done. (There's no
				// uncompressed rebuild to keep them around for: a peer that
				// can't decode a well-formed frame has a problem we can't fix
				// by re-sending, so retransmit is the only recovery, same as
				// for an uncompressed record.)
				cf_free(rd.pickle);
				cf_free(orig_pickle);

				as_storage_record_close(&rd);

				cf_digest keyd = r->keyd;
				uint64_t lut = r->last_update_time;

				as_record_done(r_ref, ns);

				emigrate_record(emig, m, &keyd, lut, wire_bytes_saved);

				return emigrate_throttle_after_send(emig, ns);
			}

			msg_set_buf(m, MIG_FIELD_RECORD, rd.pickle, rd.pickle_sz,
					MSG_SET_HANDOFF_MALLOC);

			if (orig_pickle != NULL) {
				msg_set_buf(m, MIG_FIELD_ORIG_RECORD, orig_pickle,
						orig_pickle_sz, MSG_SET_HANDOFF_MALLOC);
			}
		}
		else {
			cf_warning(AS_MIGRATE, "unreadable orig %pD", &r->keyd);
			cf_free(rd.pickle);
		}
	}
	else {
		cf_warning(AS_MIGRATE, "unreadable digest %pD", &r->keyd);

		if (ns->migrate_skip_unreadable) {
			ns->migrate_records_unreadable++;
			as_storage_record_close(&rd);
			as_record_done(r_ref, ns);
			return emig->cluster_key == as_exchange_cluster_key();
		}
	}

	as_storage_record_close(&rd);

	cf_digest keyd = r->keyd;
	uint64_t lut = r->last_update_time;

	as_record_done(r_ref, ns);

	// This might block if the queues are backed up.
	emigrate_record(emig, m, &keyd, lut, 0);

	return emigrate_throttle_after_send(emig, ns);
}

int
emigration_reinsert_reduce_fn(const void* key, void* data, void* udata)
{
	emigration_reinsert_ctrl* ri_ctrl = (emigration_reinsert_ctrl*)data;
	as_namespace* ns = ri_ctrl->emig->rsv.ns;
	uint64_t now = (uint64_t)udata;

	if (ri_ctrl->xmit_ms + ns->migrate_retransmit_ms < now) {
		bool satisfied = false;
		as_index_ref r_ref;

		if (as_record_get(ri_ctrl->emig->rsv.tree, &ri_ctrl->keyd, &r_ref) == 0) {
			if (r_ref.r->last_update_time != ri_ctrl->lut &&
					emigration_is_replicated(ns, r_ref.r)) {
				satisfied = true; // replication satisfied by recent update
			}

			as_record_done(&r_ref, ns);
		}
		else {
			satisfied = true; // replication satisfied by recent drop
		}

		if (satisfied) {
			if ((int32_t)as_aaf_uint32(&ri_ctrl->emig->bytes_emigrating,
						-(int32_t)msg_get_wire_size(ri_ctrl->m)) < 0) {
				cf_warning(AS_MIGRATE, "bytes_emigrating less than zero");
			}

			as_fabric_msg_put(ri_ctrl->m);

			return CF_SHASH_REDUCE_DELETE;
		}

		if (msg_is_set(ri_ctrl->m, MIG_FIELD_RECORD)) {
			cf_detail(AS_MIGRATE, "retransmit digest %pD", &ri_ctrl->keyd);
		}
		else {
			cf_warning(AS_MIGRATE, "unreadable digest %pD", &ri_ctrl->keyd);
		}

		if (as_fabric_retransmit(ri_ctrl->emig->dest, ri_ctrl->m,
					AS_FABRIC_CHANNEL_BULK) != AS_FABRIC_SUCCESS) {
			return CF_SHASH_ERR; // this will stop the reduce
		}

		ri_ctrl->xmit_ms = now;
		as_incr_uint64(&ns->migrate_record_retransmits);
	}

	return CF_SHASH_OK;
}

static uint64_t
migration_compression_bytes_saved(size_t raw_pickle_sz,
		size_t compressed_pickle_sz, size_t raw_orig_pickle_sz,
		size_t compressed_orig_pickle_sz)
{
	size_t raw_sz = raw_pickle_sz + raw_orig_pickle_sz;
	size_t compressed_sz = compressed_pickle_sz + compressed_orig_pickle_sz;

	// compressed_orig_pickle_sz is a 1-byte sentinel when there is no orig.
	if (raw_orig_pickle_sz == 0 && compressed_orig_pickle_sz == 1) {
		compressed_sz--;
	}

	return compressed_sz < raw_sz ? raw_sz - compressed_sz : 0;
}

// bytes_saved is what compressing this record's pickle(s) took off the wire -
// 0 for a plain send. It's credited to the namespace at insert-ack time, so the
// counter reflects bytes actually saved rather than bytes we hoped to save.
void
emigrate_record(emigration* emig, msg* m, cf_digest* keyd, uint64_t lut,
		uint64_t bytes_saved)
{
	uint64_t insert_id = emig->insert_id++;

	msg_set_uint64(m, MIG_FIELD_EMIG_INSERT_ID, insert_id);

	emigration_reinsert_ctrl ri_ctrl = {
		.xmit_ms = cf_getms(),
		.start_ns = cf_getns(),
		.emig = emig,
		.m = m,
		.keyd = *keyd,
		.lut = lut,
		.bytes_saved = bytes_saved,
	};

	msg_incr_ref(m); // the reference in the hash
	cf_shash_put(emig->reinsert_hash, &insert_id, &ri_ctrl);

	as_add_uint32(&emig->bytes_emigrating, (int32_t)msg_get_wire_size(m));

	if (as_fabric_send(emig->dest, m, AS_FABRIC_CHANNEL_BULK) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

//==========================================================
// Local helpers - immigration.
//

uint32_t
immigration_hashfn(const void* key)
{
	return ((const immigration_hkey*)key)->emig_id;
}

void*
run_immigration_reaper(void* arg)
{
	while (true) {
		cf_rchash_reduce(g_immigration_hash, immigration_reaper_reduce_fn, NULL);
		sleep(1);
	}

	return NULL;
}

int
immigration_reaper_reduce_fn(const void* key, void* object, void* udata)
{
	immigration* immig = (immigration*)object;

	if (immig->start_recv_ms == 0) {
		// Still in immigration start.
		return CF_RCHASH_OK;
	}

	if (immig->cluster_key != as_exchange_cluster_key() ||
			(immig->done_recv_ms != 0 &&
					cf_getms() > immig->done_recv_ms + IMMIGRATION_DEBOUNCE_MS)) {
		if (immig->start_result == AS_MIGRATE_OK &&
				// If we started ok, must be a cluster key change - make sure
				// DONE handler doesn't also decrement active counter.
				as_faa_uint32(&immig->done_recv, 1) == 0) {
			as_namespace* ns = immig->rsv.ns;

			int64_t n_migrate_rx_partitions_active =
					(int64_t)as_aaf_uint64(&ns->migrate_rx_partitions_active, -1);

			cf_assert(n_migrate_rx_partitions_active >= 0, AS_MIGRATE,
					"migrate_rx_partitions_active < 0");
		}

		return CF_RCHASH_REDUCE_DELETE;
	}

	return CF_RCHASH_OK;
}

//==========================================================
// Local helpers - migrate fabric message handling.
//

int
migrate_receive_msg_cb(cf_node src, msg* m, void* udata)
{
	uint32_t op;

	if (msg_get_uint32(m, MIG_FIELD_OP, &op) != 0) {
		cf_warning(AS_MIGRATE, "received message with no op");
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	//--------------------------------------------
	// Emigration - handle requests:
	//
	case OPERATION_MERGE_META:
		emigration_handle_meta_batch_request(src, m);
		break;

	//--------------------------------------------
	// Immigration - handle requests:
	//
	case OPERATION_START:
		immigration_handle_start_request(src, m);
		break;
	case OPERATION_INSERT:
		immigration_handle_insert_request(src, m);
		break;
	case OPERATION_DONE:
		immigration_handle_done_request(src, m);
		break;
	case OPERATION_ALL_DONE:
		immigration_handle_all_done_request(src, m);
		break;

	//--------------------------------------------
	// Emigration - handle acknowledgments:
	//
	case OPERATION_INSERT_ACK:
		emigration_handle_insert_ack(src, m);
		break;
	case OPERATION_START_ACK_OK:
	case OPERATION_START_ACK_EAGAIN:
	case OPERATION_START_ACK_FAIL:
	case OPERATION_DONE_ACK:
	case OPERATION_ALL_DONE_ACK:
		emigration_handle_ctrl_ack(src, m, op);
		break;

	//--------------------------------------------
	// Immigration - handle acknowledgments:
	//
	case OPERATION_MERGE_META_ACK:
		immigration_handle_meta_batch_ack(src, m);
		break;

	default:
		cf_detail(AS_MIGRATE, "received unexpected message op %u", op);
		as_fabric_msg_put(m);
		break;
	}

	return 0;
}

//----------------------------------------------------------
// Immigration - request message handling.
//

void
immigration_handle_start_request(cf_node src, msg* m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint64_t cluster_key;

	if (msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &cluster_key) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for cluster key failed");
		as_fabric_msg_put(m);
		return;
	}

	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for namespace failed");
		as_fabric_msg_put(m);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_MIGRATE, "handle start: bad namespace");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t pid;

	if (msg_get_uint32(m, MIG_FIELD_PARTITION, &pid) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for pid failed");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t emig_features = 0;

	msg_get_uint32(m, MIG_FIELD_FEATURES, &emig_features);

	uint64_t emig_n_recs = 0;

	msg_get_uint64(m, MIG_FIELD_PARTITION_SIZE, &emig_n_recs);

	immigration* immig = cf_rc_alloc(sizeof(immigration));

	as_incr_uint64(&ns->migrate_rx_instance_count);

	immig->src = src;
	immig->cluster_key = cluster_key;
	immig->pid = pid;
	immig->start_recv_ms = 0;
	immig->done_recv = 0;
	immig->done_recv_ms = 0;
	immig->emig_id = emig_id;
	immig->meta_q = meta_out_q_create();
	immig->features = MY_MIG_FEATURES;
	immig->ns = ns;
	immig->rsv.p = NULL;

	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	while (true) {
		if (cf_rchash_put_unique(g_immigration_hash, (void*)&hkey,
					(void*)immig) == CF_RCHASH_OK) {
			cf_rc_reserve(immig); // so either put or get yields ref-count 2

			// First start request (not a retransmit) for this pid this round,
			// or we had ack'd previous start request with 'EAGAIN'.
			immig->start_result =
					as_partition_immigrate_start(ns, pid, cluster_key, src);
			break;
		}

		immigration* immig0;

		if (cf_rchash_get(g_immigration_hash, (void*)&hkey, (void*)&immig0) ==
				CF_RCHASH_OK) {
			immigration_release(immig); // free just-alloc'd immig ...

			if (immig0->start_recv_ms == 0) {
				immigration_release(immig0);
				return; // allow previous thread to respond
			}

			if (immig0->cluster_key != cluster_key) {
				immigration_release(immig0);
				return; // other node reused an emig_id, allow reaper to reap
			}

			immig = immig0; // ...  and use original
			break;
		}
	}

	switch (immig->start_result) {
	case AS_MIGRATE_OK:
		break;
	case AS_MIGRATE_FAIL:
		immig->start_recv_ms = cf_getms(); // permits reaping
		immig->done_recv_ms = immig->start_recv_ms; // permits reaping
		immigration_release(immig);
		immigration_ack_start_request(src, m, OPERATION_START_ACK_FAIL);
		return;
	case AS_MIGRATE_AGAIN:
		// Remove from hash so that the immig can be tried again.
		// Note - no real need to specify object, but paranoia costs nothing.
		cf_rchash_delete_object(g_immigration_hash, (void*)&hkey, (void*)immig);
		immigration_release(immig);
		immigration_ack_start_request(src, m, OPERATION_START_ACK_EAGAIN);
		return;
	default:
		cf_crash(AS_MIGRATE, "unexpected as_partition_immigrate_start result");
		break;
	}

	if (immig->start_recv_ms == 0) {
		as_partition_reserve(ns, pid, &immig->rsv);
		as_incr_uint64(&immig->rsv.ns->migrate_rx_partitions_active);

		if (! immigration_start_meta_sender(immig, emig_features, emig_n_recs)) {
			immig->features &= ~MIG_FEATURE_MERGE;
		}

		immig->start_recv_ms = cf_getms(); // permits reaping
	}

	msg_set_uint32(m, MIG_FIELD_FEATURES, immig->features);

	immigration_release(immig);
	immigration_ack_start_request(src, m, OPERATION_START_ACK_OK);
}

void
immigration_ack_start_request(cf_node src, msg* m, uint32_t op)
{
	// Trim the echoed START payload for all three ack variants (OK, FAIL,
	// EAGAIN) - the emigrator reads only EMIG_ID and FEATURES.
	msg_preserve_fields(m, 2, MIG_FIELD_EMIG_ID, MIG_FIELD_FEATURES);
	msg_set_uint32(m, MIG_FIELD_OP, op);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

// An insert is acked only when the record was applied (or was a no-op the
// emigrator can treat as applied). Every failure - including a compressed
// record we couldn't decode - drops the message unacked and lets the
// emigrator's reinserter retransmit.
void
immigration_ack_insert_request(cf_node src, msg* m)
{
	msg_preserve_fields(m, 2, MIG_FIELD_EMIG_INSERT_ID, MIG_FIELD_EMIG_ID);
	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_BULK) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

void
immigration_handle_insert_request(cf_node src, msg* m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle insert: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	immigration* immig;

	if (cf_rchash_get(g_immigration_hash, (void*)&hkey, (void**)&immig) !=
			CF_RCHASH_OK) {
		// The immig no longer exists, likely the cluster key advanced and this
		// record immigration is from prior round. Do not ack this request.
		as_fabric_msg_put(m);
		return;
	}

	if (immig->start_result != AS_MIGRATE_OK || immig->start_recv_ms == 0) {
		// If this immigration didn't start and reserve a partition, it's
		// likely in the hash on a retransmit and this insert is for the
		// original - ignore, and let this immigration proceed.
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	as_incr_uint64(&immig->rsv.ns->migrate_record_receives);

	if (immig->cluster_key != as_exchange_cluster_key()) {
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	if (as_storage_overloaded(immig->rsv.ns, 64, "immigrate")) {
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	as_remote_record rr = { .via = VIA_MIGRATION, .src = src, .rsv = &immig->rsv };
	uint32_t compression = MIGRATE_COMPRESSION_NONE;
	uint8_t* record_buf = NULL;
	size_t record_sz = 0;
	uint8_t* orig_buf = NULL;
	size_t orig_buf_sz = 0;

	msg_get_uint32(m, MIG_FIELD_COMPRESSION, &compression);

	// Trust no byte from a peer - reject anything but the two legal values
	// rather than falling through to the uncompressed path.
	if (compression != MIGRATE_COMPRESSION_NONE &&
			compression != MIGRATE_COMPRESSION_ZSTD) {
		cf_warning(AS_MIGRATE, "handle insert: bad compression %u", compression);
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	if (msg_get_buf(m, MIG_FIELD_RECORD, &record_buf, &record_sz,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle insert: got no record");
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	msg_get_buf(m, MIG_FIELD_ORIG_RECORD, &orig_buf, &orig_buf_sz,
			MSG_GET_DIRECT);

	if (compression == MIGRATE_COMPRESSION_ZSTD) {
		rr.pickle_sz = zstd_wire_decompress_buffer((void**)&rr.pickle,
				record_buf, record_sz);

		if (orig_buf != NULL) {
			rr.orig_pickle_sz =
					zstd_wire_decompress_buffer((void**)&rr.orig_pickle,
							orig_buf, orig_buf_sz);
		}
	}
	else {
		rr.pickle = record_buf;
		rr.pickle_sz = record_sz;
		rr.orig_pickle = orig_buf;
		rr.orig_pickle_sz = orig_buf_sz;
	}

	// A well-formed frame that we can't decode means memory pressure or bytes
	// corrupted between here and the emigrator's compress - nothing we can
	// negotiate our way out of. Drop it like any other unusable record and let
	// the reinserter retransmit.
	if (rr.pickle_sz == 0 || (orig_buf != NULL && rr.orig_pickle_sz == 0)) {
		cf_warning(AS_MIGRATE, "handle insert: failed to decompress record");
		// Free-keying must match alloc-keying (== ZSTD): only the codec path
		// allocates - the else-branch pointers are owned by the msg.
		if (compression == MIGRATE_COMPRESSION_ZSTD) {
			cf_free(rr.pickle);
			cf_free(rr.orig_pickle);
		}
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	if (! as_flat_unpack_remote_record_meta(rr.rsv->ns, &rr)) {
		cf_warning(AS_MIGRATE, "handle insert: got bad record");
		if (compression == MIGRATE_COMPRESSION_ZSTD) {
			cf_free(rr.pickle);
			cf_free(rr.orig_pickle);
		}
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	uint32_t info = 0;

	msg_get_uint32(m, MIG_FIELD_INFO, &info);

	immigration_init_repl_state(&rr, info);

	int rv = as_record_replace_if_better(&rr);

	// If replace failed, don't ack - it will be retransmitted.
	if (! (rv == AS_OK ||
				// Migrations just treat these errors as successful no-ops:
				rv == AS_ERR_RECORD_EXISTS || rv == AS_ERR_GENERATION)) {
		if (compression == MIGRATE_COMPRESSION_ZSTD) {
			cf_free(rr.pickle);
			cf_free(rr.orig_pickle);
		}
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	if (compression == MIGRATE_COMPRESSION_ZSTD) {
		cf_free(rr.pickle);
		cf_free(rr.orig_pickle);
	}

	immigration_release(immig);
	immigration_ack_insert_request(src, m);
}

void
immigration_handle_done_request(cf_node src, msg* m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle done: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 1, MIG_FIELD_EMIG_ID);

	// See if this migration already exists & has been notified.
	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	immigration* immig;

	if (cf_rchash_get(g_immigration_hash, (void*)&hkey, (void**)&immig) ==
			CF_RCHASH_OK) {
		if (immig->start_result != AS_MIGRATE_OK || immig->start_recv_ms == 0) {
			// If this immigration didn't start and reserve a partition, it's
			// likely in the hash on a retransmit and this DONE is for the
			// original - ignore, and let this immigration proceed.
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		if (as_faa_uint32(&immig->done_recv, 1) == 0) {
			// Record the time of the first DONE received.
			immig->done_recv_ms = cf_getms();

			as_namespace* ns = immig->rsv.ns;

			int64_t n_migrate_rx_partitions_active =
					(int64_t)as_aaf_uint64(&ns->migrate_rx_partitions_active, -1);

			cf_assert(n_migrate_rx_partitions_active >= 0, AS_MIGRATE,
					"migrate_rx_partitions_active < 0");

			as_partition_immigrate_done(ns, immig->rsv.p->id,
					immig->cluster_key, immig->src);
		}
		// else - was likely a retransmitted done message.

		immigration_release(immig);
	}
	// else - garbage, or super-stale retransmitted done message.

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

void
immigration_handle_all_done_request(cf_node src, msg* m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint64_t cluster_key;

	if (msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &cluster_key) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for cluster key failed");
		as_fabric_msg_put(m);
		return;
	}

	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for namespace failed");
		as_fabric_msg_put(m);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_MIGRATE, "handle all done: bad namespace");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t pid;

	if (msg_get_uint32(m, MIG_FIELD_PARTITION, &pid) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for pid failed");
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 1, MIG_FIELD_EMIG_ID);

	// TODO - optionally, for replicas we might use this to remove immig objects
	// from hash and deprecate timer...

	if (as_partition_migrations_all_done(ns, pid, cluster_key) != AS_MIGRATE_OK) {
		as_fabric_msg_put(m);
		return;
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_ALL_DONE_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

//----------------------------------------------------------
// Emigration - acknowledgment message handling.
//

void
emigration_handle_insert_ack(cf_node src, msg* m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "insert ack: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	emigration* emig;

	if (cf_rchash_get(g_emigration_hash, (void*)&emig_id, (void**)&emig) !=
			CF_RCHASH_OK) {
		// Probably came from a migration prior to the latest rebalance.
		as_fabric_msg_put(m);
		return;
	}

	uint64_t insert_id;

	if (msg_get_uint64(m, MIG_FIELD_EMIG_INSERT_ID, &insert_id) != 0) {
		cf_warning(AS_MIGRATE, "insert ack: msg get for emig insert id failed");
		emigration_release(emig);
		as_fabric_msg_put(m);
		return;
	}

	emigration_reinsert_ctrl* ri_ctrl = NULL;
	cf_mutex* vlock;

	if (cf_shash_get_vlock(emig->reinsert_hash, &insert_id, (void**)&ri_ctrl,
				&vlock) == CF_SHASH_OK) {
		if (src == emig->dest) {
			// An insert is acked only when the peer applied the record, so an
			// ack always retires it. Anything the peer couldn't apply - a
			// record it couldn't decompress included - is dropped there
			// unacked, leaving this entry for the reinserter to retransmit.

			if ((int32_t)as_aaf_uint32(&emig->bytes_emigrating,
						-(int32_t)msg_get_wire_size(ri_ctrl->m)) < 0) {
				cf_warning(AS_MIGRATE, "bytes_emigrating less than zero");
			}

			// Sample emigrate latency from first-send to ack. Gated on
			// migrate_benchmarks_enabled so production clusters pay
			// nothing; the histogram itself is always created so the
			// flag can be flipped dynamically without restart.
			as_namespace* ns = emig->rsv.ns;
			if (ns->migrate_benchmarks_enabled && ri_ctrl->start_ns != 0) {
				histogram_insert_data_point(ns->migrate_emigrate_hist,
						ri_ctrl->start_ns);
			}

			// Credit bytes saved on the wire only now that the compressed
			// send has landed - the counter tracks bytes actually saved, not
			// bytes we hoped to save. Zero for a plain send.
			if (ri_ctrl->bytes_saved != 0) {
				as_add_uint64(&ns->migrate_wire_comp_stat.bytes_saved,
						ri_ctrl->bytes_saved);
			}

			as_fabric_msg_put(ri_ctrl->m);
			// At this point, the rt is *GONE*.
			cf_shash_delete_lockfree(emig->reinsert_hash, &insert_id);
		}
		else {
			cf_warning(AS_MIGRATE, "insert ack: unexpected source %lx", src);
		}

		cf_mutex_unlock(vlock);
	}

	emigration_release(emig);
	as_fabric_msg_put(m);
}

void
emigration_handle_ctrl_ack(cf_node src, msg* m, uint32_t op)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "ctrl ack: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t immig_features = 0;

	msg_get_uint32(m, MIG_FIELD_FEATURES, &immig_features);

	as_fabric_msg_put(m);

	emigration* emig;

	if (cf_rchash_get(g_emigration_hash, (void*)&emig_id, (void**)&emig) ==
			CF_RCHASH_OK) {
		if (emig->dest == src) {
			if (op == OPERATION_START_ACK_OK) {
				// Paired atomic store; the emigration worker reads this via
				// as_load_uint32 in emigrate_tree_reduce_fn.
				as_store_uint32(&emig->features, immig_features);
			}

			if ((immig_features & MIG_FEATURE_MERGE) == 0) {
				// TODO - rethink where this should go after further refactor.
				if (op == OPERATION_START_ACK_OK && emig->meta_q) {
					meta_in_q_rejected(emig->meta_q);
				}
			}

			cf_queue_push(emig->ctrl_q, &op);
		}
		else {
			cf_warning(AS_MIGRATE, "ctrl ack (%d): unexpected source %lx", op,
					src);
		}

		emigration_release(emig);
	}
	else {
		cf_detail(AS_MIGRATE, "ctrl ack (%d): can't find emig id %u", op,
				emig_id);
	}
}

//==========================================================
// Local helpers - info API helpers.
//

int
emigration_dump_reduce_fn(const void* key, void* object, void* udata)
{
	uint32_t emig_id = *(const uint32_t*)key;
	emigration* emig = (emigration*)object;
	int* item_num = (int*)udata;

	cf_info(AS_MIGRATE, "[%d]: mig_id %u : id %u ; ck %lx", *item_num, emig_id,
			emig->id, emig->cluster_key);

	*item_num += 1;

	return 0;
}

int
immigration_dump_reduce_fn(const void* key, void* object, void* udata)
{
	const immigration_hkey* hkey = (const immigration_hkey*)key;
	immigration* immig = (immigration*)object;
	int* item_num = (int*)udata;

	cf_info(AS_MIGRATE,
			"[%d]: src %016lx ; id %u : src %016lx ; done recv %u ; start recv ms %lu ; done recv ms %lu ; ck %lx",
			*item_num, hkey->src, hkey->emig_id, immig->src, immig->done_recv,
			immig->start_recv_ms, immig->done_recv_ms, immig->cluster_key);

	*item_num += 1;

	return 0;
}
