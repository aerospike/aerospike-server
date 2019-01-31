/*
 * partition_balance.c
 *
 * Copyright (C) 2016-2018 Aerospike, Inc.
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

#include "fabric/partition_balance.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "fault.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/exchange.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

const as_partition_version ZERO_VERSION = { 0 };


//==========================================================
// Globals.
//

cf_atomic32 g_partition_generation = (uint32_t)-1;
uint64_t g_rebalance_sec;
uint64_t g_rebalance_generation = 0;

// Using int for 4-byte size, but maintaining bool semantics.
// TODO - ok as non-volatile, but should selectively load/store in the future.
static int g_init_balance_done = false;

static cf_atomic32 g_migrate_num_incoming = 0;

// Using int for 4-byte size, but maintaining bool semantics.
volatile int g_allow_migrations = false;

uint64_t g_hashed_pids[AS_PARTITIONS];

// Shortcuts to values set by as_exchange, for use in partition balance only.
uint32_t g_cluster_size = 0;
cf_node* g_succession = NULL;

cf_node g_full_node_seq_table[AS_CLUSTER_SZ * AS_PARTITIONS];
sl_ix_t g_full_sl_ix_table[AS_CLUSTER_SZ * AS_PARTITIONS];


//==========================================================
// Forward declarations.
//

// Only partition_balance hooks into exchange.
extern cf_node* as_exchange_succession_unsafe();

// Helpers - generic.
void create_trees(as_partition* p, as_namespace* ns);
void drop_trees(as_partition* p);

// Helpers - balance partitions.
void fill_global_tables();
void apply_single_replica_limit_ap(as_namespace* ns);
int find_working_master_ap(const as_partition* p, const sl_ix_t* ns_sl_ix, const as_namespace* ns);
uint32_t find_duplicates_ap(const as_partition* p, const cf_node* ns_node_seq, const sl_ix_t* ns_sl_ix, const struct as_namespace_s* ns, uint32_t working_master_n, cf_node dupls[]);
void advance_version_ap(as_partition* p, const sl_ix_t* ns_sl_ix, as_namespace* ns, uint32_t self_n,	uint32_t working_master_n, uint32_t n_dupl, const cf_node dupls[]);
uint32_t fill_family_versions(const as_partition* p, const sl_ix_t* ns_sl_ix, const as_namespace* ns, uint32_t working_master_n, uint32_t n_dupl, const cf_node dupls[], as_partition_version family_versions[]);
bool has_replica_parent(const as_partition* p, const sl_ix_t* ns_sl_ix, const as_namespace* ns, const as_partition_version* subset_version, uint32_t subset_n);
uint32_t find_family(const as_partition_version* self_version, uint32_t n_families, const as_partition_version family_versions[]);

// Helpers - migration-related.
bool partition_immigration_is_valid(const as_partition* p, cf_node source_node, const as_namespace* ns, const char* tag);


//==========================================================
// Inlines & macros.
//

static inline bool
is_self_final_master(const as_partition* p)
{
	return p->replicas[0] == g_config.self_node;
}

static inline bool
is_family_same(const as_partition_version* v1, const as_partition_version* v2)
{
	return v1->ckey == v2->ckey && v1->family == v2->family &&
			v1->family != VERSION_FAMILY_UNIQUE;
}


//==========================================================
// Public API - regulate migrations.
//

void
as_partition_balance_disallow_migrations()
{
	cf_detail(AS_PARTITION, "disallow migrations");

	g_allow_migrations = false;
}

bool
as_partition_balance_are_migrations_allowed()
{
	return g_allow_migrations;
}

void
as_partition_balance_synchronize_migrations()
{
	// Acquire and release each partition lock to ensure threads acquiring a
	// partition lock after this will be forced to check the latest cluster key.
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			cf_mutex_lock(&p->lock);
			cf_mutex_unlock(&p->lock);
		}
	}

	// Prior-round migrations won't decrement g_migrate_num_incoming due to
	// cluster key check.
	cf_atomic32_set(&g_migrate_num_incoming, 0);
}


//==========================================================
// Public API - balance partitions.
//

void
as_partition_balance_init()
{
	// Cache hashed pids for all future rebalances.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		g_hashed_pids[pid] = cf_hash_fnv64((const uint8_t*)&pid,
				sizeof(uint32_t));
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		uint32_t n_stored = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			as_storage_load_pmeta(ns, p);

			if (as_partition_version_has_data(&p->version)) {
				as_partition_isolate_version(ns, p);
				n_stored++;
			}
		}

		cf_info(AS_PARTITION, "{%s} %u partitions: found %u absent, %u stored",
				ns->name, AS_PARTITIONS, AS_PARTITIONS - n_stored, n_stored);
	}

	partition_balance_init();
}

// Has the node resolved as operating either in a multi-node cluster or as a
// single-node cluster?
bool
as_partition_balance_is_init_resolved()
{
	return g_init_balance_done;
}

void
as_partition_balance_revert_to_orphan()
{
	g_init_balance_done = false;
	g_allow_migrations = false;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		client_replica_maps_clear(ns);

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			cf_mutex_lock(&p->lock);

			as_partition_freeze(p);
			as_partition_isolate_version(ns, p);

			cf_mutex_unlock(&p->lock);
		}

		ns->n_unavailable_partitions = AS_PARTITIONS;
	}

	cf_atomic32_incr(&g_partition_generation);
}

void
as_partition_balance()
{
	// Temporary paranoia.
	static uint64_t last_cluster_key = 0;

	if (last_cluster_key == as_exchange_cluster_key()) {
		cf_warning(AS_PARTITION, "as_partition_balance: cluster key %lx same as last time",
				last_cluster_key);
		return;
	}

	last_cluster_key = as_exchange_cluster_key();
	// End - temporary paranoia.

	// These shortcuts must only be used within the scope of this function.
	g_cluster_size = as_exchange_cluster_size();
	g_succession = as_exchange_succession_unsafe();

	// Each partition separately shuffles the node succession list to generate
	// its own node sequence.
	fill_global_tables();

	cf_queue mq;

	cf_queue_init(&mq, sizeof(pb_task), g_config.n_namespaces * AS_PARTITIONS,
			false);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		balance_namespace(g_config.namespaces[ns_ix], &mq);
	}

	prepare_for_appeals();

	// All partitions now have replicas assigned, ok to allow transactions.
	g_init_balance_done = true;
	cf_atomic32_incr(&g_partition_generation);

	g_allow_migrations = true;
	cf_detail(AS_PARTITION, "allow migrations");

	g_rebalance_sec = cf_get_seconds(); // must precede process_pb_tasks()

	process_pb_tasks(&mq);
	cf_queue_destroy(&mq);

	g_rebalance_generation++;
}

uint64_t
as_partition_balance_remaining_migrations()
{
	uint64_t remaining_migrations = 0;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		remaining_migrations += ns->migrate_tx_partitions_remaining;
		remaining_migrations += ns->migrate_rx_partitions_remaining;
	}

	return remaining_migrations;
}


//==========================================================
// Public API - migration-related as_partition methods.
//

// Currently used only for enterprise build.
bool
as_partition_pending_migrations(as_partition* p)
{
	cf_mutex_lock(&p->lock);

	bool pending = p->pending_immigrations + p->pending_emigrations != 0;

	cf_mutex_unlock(&p->lock);

	return pending;
}

void
as_partition_emigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t tx_flags)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		cf_debug(AS_PARTITION, "{%s:%u} emigrate_done - cluster key mismatch",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return;
	}

	if (p->pending_emigrations == 0) {
		cf_warning(AS_PARTITION, "{%s:%u} emigrate_done - no pending emigrations",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return;
	}

	p->pending_emigrations--;

	int64_t migrates_tx_remaining =
			cf_atomic_int_decr(&ns->migrate_tx_partitions_remaining);

	if (migrates_tx_remaining < 0){
		cf_warning(AS_PARTITION, "{%s:%u} (%hu,%ld) emigrate_done - counter went negative",
				ns->name, pid, p->pending_emigrations, migrates_tx_remaining);
	}

	if ((tx_flags & TX_FLAGS_LEAD) != 0) {
		p->pending_lead_emigrations--;
		cf_atomic_int_decr(&ns->migrate_tx_partitions_lead_remaining);
	}

	if (! is_self_final_master(p)) {
		emigrate_done_advance_non_master_version(ns, p, tx_flags);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	cf_queue mq;
	pb_task task;
	int w_ix = -1;

	if (is_self_final_master(p) &&
			p->pending_emigrations == 0 && p->pending_immigrations == 0) {
		cf_queue_init(&mq, sizeof(pb_task), p->n_witnesses, false);

		for (w_ix = 0; w_ix < (int)p->n_witnesses; w_ix++) {
			pb_task_init(&task, p->witnesses[w_ix], ns, pid, orig_cluster_key,
					PB_TASK_EMIG_SIGNAL_ALL_DONE, TX_FLAGS_CONTINGENT);
			cf_queue_push(&mq, &task);
		}
	}

	cf_mutex_unlock(&p->lock);

	if (w_ix >= 0) {
		while (cf_queue_pop(&mq, &task, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
			as_migrate_emigrate(&task);
		}

		cf_queue_destroy(&mq);
	}
}

as_migrate_result
as_partition_immigrate_start(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, cf_node source_node)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key() ||
			immigrate_yield()) {
		cf_debug(AS_PARTITION, "{%s:%u} immigrate_start - cluster key mismatch",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	uint32_t num_incoming = (uint32_t)cf_atomic32_incr(&g_migrate_num_incoming);

	if (num_incoming > g_config.migrate_max_num_incoming) {
		cf_debug(AS_PARTITION, "{%s:%u} immigrate_start - exceeded max_num_incoming",
				ns->name, pid);
		cf_atomic32_decr(&g_migrate_num_incoming);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	if (! partition_immigration_is_valid(p, source_node, ns, "start")) {
		cf_atomic32_decr(&g_migrate_num_incoming);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (! is_self_final_master(p)) {
		immigrate_start_advance_non_master_version(ns, p);
		as_storage_save_pmeta(ns, p);
	}

	cf_mutex_unlock(&p->lock);

	return AS_MIGRATE_OK;
}

as_migrate_result
as_partition_immigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, cf_node source_node)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		cf_debug(AS_PARTITION, "{%s:%u} immigrate_done - cluster key mismatch",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	cf_atomic32_decr(&g_migrate_num_incoming);

	if (! partition_immigration_is_valid(p, source_node, ns, "done")) {
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	p->pending_immigrations--;

	int64_t migrates_rx_remaining =
			cf_atomic_int_decr(&ns->migrate_rx_partitions_remaining);

	// Sanity-check only.
	if (migrates_rx_remaining < 0) {
		cf_warning(AS_PARTITION, "{%s:%u} (%hu,%ld) immigrate_done - counter went negative",
				ns->name, pid, p->pending_immigrations, migrates_rx_remaining);
	}

	if (p->pending_immigrations == 0 &&
			! as_partition_version_same(&p->version, &p->final_version)) {
		p->version = p->final_version;
		as_storage_save_pmeta(ns, p);
	}

	if (! is_self_final_master(p)) {
		if (client_replica_maps_update(ns, pid)) {
			cf_atomic32_incr(&g_partition_generation);
		}

		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_OK;
	}

	// Final master finished an immigration, adjust duplicates.

	if (source_node == p->working_master) {
		p->working_master = g_config.self_node;

		immigrate_done_advance_final_master_version(ns, p);
	}
	else {
		p->n_dupl = remove_node(p->dupls, p->n_dupl, source_node);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	if (p->pending_immigrations != 0) {
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_OK;
	}

	// Final master finished all immigration.

	cf_queue mq;
	pb_task task;

	if (p->pending_emigrations != 0) {
		cf_queue_init(&mq, sizeof(pb_task), p->n_replicas - 1, false);

		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->immigrators[repl_ix]) {
				pb_task_init(&task, p->replicas[repl_ix], ns, pid,
						orig_cluster_key, PB_TASK_EMIG_TRANSFER,
						TX_FLAGS_CONTINGENT);
				cf_queue_push(&mq, &task);
			}
		}
	}
	else {
		cf_queue_init(&mq, sizeof(pb_task), p->n_witnesses, false);

		for (uint16_t w_ix = 0; w_ix < p->n_witnesses; w_ix++) {
			pb_task_init(&task, p->witnesses[w_ix], ns, pid, orig_cluster_key,
					PB_TASK_EMIG_SIGNAL_ALL_DONE, TX_FLAGS_CONTINGENT);
			cf_queue_push(&mq, &task);
		}
	}

	cf_mutex_unlock(&p->lock);

	while (cf_queue_pop(&mq, &task, 0) == CF_QUEUE_OK) {
		as_migrate_emigrate(&task);
	}

	cf_queue_destroy(&mq);

	return AS_MIGRATE_OK;
}

as_migrate_result
as_partition_migrations_all_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		cf_debug(AS_PARTITION, "{%s:%u} all_done - cluster key mismatch",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (p->pending_emigrations != 0) {
		cf_debug(AS_PARTITION, "{%s:%u} all_done - eagain",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	// Not a replica and non-null version ...
	if (! is_self_replica(p) && ! as_partition_version_is_null(&p->version)) {
		// ...  and not quiesced - drop partition.
		if (drop_superfluous_version(p, ns)) {
			drop_trees(p);
			as_storage_save_pmeta(ns, p);
		}
		// ... or quiesced more than one node - become subset of final version.
		else if (adjust_superfluous_version(p, ns)) {
			as_storage_save_pmeta(ns, p);
		}
	}

	cf_mutex_unlock(&p->lock);

	return AS_MIGRATE_OK;
}

void
as_partition_signal_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		cf_debug(AS_PARTITION, "{%s:%u} signal_done - cluster key mismatch",
				ns->name, pid);
		cf_mutex_unlock(&p->lock);
		return;
	}

	cf_atomic_int_decr(&ns->migrate_signals_remaining);

	cf_mutex_unlock(&p->lock);
}


//==========================================================
// Local helpers - generic.
//

void
pb_task_init(pb_task* task, cf_node dest, as_namespace* ns,
		uint32_t pid, uint64_t cluster_key, pb_task_type type,
		uint32_t tx_flags)
{
	task->dest = dest;
	task->ns = ns;
	task->pid = pid;
	task->type = type;
	task->tx_flags = tx_flags;
	task->cluster_key = cluster_key;
}

void
create_trees(as_partition* p, as_namespace* ns)
{
	cf_assert(! p->tree, AS_PARTITION, "unexpected - tree already exists");

	as_partition_advance_tree_id(p, ns->name);

	p->tree = as_index_tree_create(&ns->tree_shared, p->tree_id,
			as_partition_tree_done, (void*)p);
}

void
drop_trees(as_partition* p)
{
	if (! p->tree) {
		return; // CP signals can get here - 0e/0r versions are witnesses
	}

	as_index_tree_release(p->tree);
	p->tree = NULL;

	// TODO - consider p->n_tombstones?
	cf_atomic32_set(&p->max_void_time, 0);
}


//==========================================================
// Local helpers - balance partitions.
//

//  Succession list - all nodes in cluster
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
//  Succession list index (sl_ix) - used as version table and rack-id index
//  +---------------+
//  | 0 | 1 | 2 | 3 |
//  +---------------+
//
// Every partition shuffles the succession list independently, e.g. for pid 0:
// Hash the node names with the pid:
//  H(A,0) = Y, H(B,0) = X, H(C,0) = W, H(D,0) = Z
// Store sl_ix in last byte of hash results so it doesn't affect sort:
//  +-----------------------+
//  | Y_0 | X_1 | W_2 | Z_3 |
//  +-----------------------+
// This sorts to:
//  +-----------------------+
//  | W_2 | X_1 | Y_0 | Z_3 |
//  +-----------------------+
// Replace original node names, and keep sl_ix order, resulting in:
//  +---------------+    +---------------+
//  | C | B | A | D |    | 2 | 1 | 0 | 3 |
//  +---------------+    +---------------+
//
//  Node sequence table      Succession list index table
//   pid                      pid
//  +===+---------------+    +===+---------------+
//  | 0 | C | B | A | D |    | 0 | 2 | 1 | 0 | 3 |
//  +===+---------------+    +===+---------------+
//  | 1 | A | D | C | B |    | 1 | 0 | 3 | 2 | 1 |
//  +===+---------------+    +===+---------------+
//  | 2 | D | C | B | A |    | 2 | 3 | 2 | 1 | 0 |
//  +===+---------------+    +===+---------------+
//  | 3 | B | A | D | C |    | 3 | 1 | 0 | 3 | 2 |
//  +===+---------------+    +===+---------------+
//  | 4 | D | B | C | A |    | 4 | 3 | 1 | 2 | 0 |
//  +===+---------------+    +===+---------------+
//  ... to pid 4095.
//
// We keep the succession list index table so we can refer back to namespaces'
// partition version tables and rack-id lists, where nodes are in the original
// succession list order.
void
fill_global_tables()
{
	uint64_t hashed_nodes[g_cluster_size];

	for (uint32_t n = 0; n < g_cluster_size; n++) {
		hashed_nodes[n] = cf_hash_fnv64((const uint8_t*)&g_succession[n],
				sizeof(cf_node));
	}

	// Build the node sequence table.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		inter_hash h;

		h.hashed_pid = g_hashed_pids[pid];

		for (uint32_t n = 0; n < g_cluster_size; n++) {
			h.hashed_node = hashed_nodes[n];

			cf_node* node_p = &FULL_NODE_SEQ(pid, n);

			*node_p = cf_hash_jen64((const uint8_t*)&h, sizeof(h));

			// Overlay index onto last byte.
			*node_p &= AS_CLUSTER_SZ_MASKP;
			*node_p += n;
		}

		// Sort the hashed node values.
		qsort(&FULL_NODE_SEQ(pid, 0), g_cluster_size, sizeof(cf_node),
				cf_node_compare_desc);

		// Overwrite the sorted hash values with the original node IDs.
		for (uint32_t n = 0; n < g_cluster_size; n++) {
			cf_node* node_p = &FULL_NODE_SEQ(pid, n);
			sl_ix_t sl_ix = (sl_ix_t)(*node_p & AS_CLUSTER_SZ_MASKN);

			*node_p = g_succession[sl_ix];

			// Saved to refer back to partition version table and rack-id list.
			FULL_SL_IX(pid, n) = sl_ix;
		}
	}
}

void
balance_namespace_ap(as_namespace* ns, cf_queue* mq)
{
	bool ns_less_than_global = ns->cluster_size != g_cluster_size;

	if (ns_less_than_global) {
		cf_info(AS_PARTITION, "{%s} is on %u of %u nodes", ns->name,
				ns->cluster_size, g_cluster_size);
	}

	// Figure out effective replication factor in the face of node failures.
	apply_single_replica_limit_ap(ns);

	// Active size will be less than cluster size if nodes are quiesced.
	set_active_size(ns);

	uint32_t n_racks = rack_count(ns);

	// If a namespace is not on all nodes or is rack aware or uniform balance
	// is preferred or nodes are quiesced, it can't use the global node sequence
	// and index tables.
	bool ns_not_equal_global = ns_less_than_global || n_racks != 1 ||
			ns->prefer_uniform_balance || ns->active_size != ns->cluster_size;

	// The translation array is used to convert global table rows to namespace
	// rows, if  necessary.
	int translation[ns_less_than_global ? g_cluster_size : 0];

	if (ns_less_than_global) {
		fill_translation(translation, ns);
	}

	uint32_t claims_size = ns->prefer_uniform_balance ?
			ns->replication_factor * g_cluster_size : 0;
	uint32_t claims[claims_size];
	uint32_t target_claims[claims_size];

	if (ns->prefer_uniform_balance) {
		memset(claims, 0, sizeof(claims));
		init_target_claims_ap(ns, translation, target_claims);
	}

	uint32_t ns_pending_emigrations = 0;
	uint32_t ns_pending_lead_emigrations = 0;
	uint32_t ns_pending_immigrations = 0;
	uint32_t ns_pending_signals = 0;

	uint32_t ns_fresh_partitions = 0;

	for (uint32_t pid_group = 0; pid_group < NUM_PID_GROUPS; pid_group++) {
		uint32_t start_pid = pid_group * PIDS_PER_GROUP;
		uint32_t end_pid = start_pid + PIDS_PER_GROUP;

		for (uint32_t pid = start_pid; pid < end_pid; pid++) {
			as_partition* p = &ns->partitions[pid];

			cf_node* full_node_seq = &FULL_NODE_SEQ(pid, 0);
			sl_ix_t* full_sl_ix = &FULL_SL_IX(pid, 0);

			// Usually a namespace can simply use the global tables...
			cf_node* ns_node_seq = full_node_seq;
			sl_ix_t* ns_sl_ix = full_sl_ix;

			cf_node stack_node_seq[ns_not_equal_global ? ns->cluster_size : 0];
			sl_ix_t stack_sl_ix[ns_not_equal_global ? ns->cluster_size : 0];

			// ... but sometimes a namespace is different.
			if (ns_not_equal_global) {
				ns_node_seq = stack_node_seq;
				ns_sl_ix = stack_sl_ix;

				fill_namespace_rows(full_node_seq, full_sl_ix, ns_node_seq,
						ns_sl_ix, ns, translation);

				if (ns->active_size != ns->cluster_size) {
					quiesce_adjust_row(ns_node_seq, ns_sl_ix, ns);
				}

				if (ns->prefer_uniform_balance) {
					uniform_adjust_row(ns_node_seq, ns->active_size, ns_sl_ix,
							ns->replication_factor, claims, target_claims,
							ns->rack_ids, n_racks);
				}
				else if (n_racks != 1) {
					rack_aware_adjust_row(ns_node_seq, ns_sl_ix,
							ns->replication_factor, ns->rack_ids,
							ns->active_size, n_racks, 1);
				}
			}

			cf_mutex_lock(&p->lock);

			p->working_master = (cf_node)0;

			p->n_replicas = ns->replication_factor;
			memcpy(p->replicas, ns_node_seq, p->n_replicas * sizeof(cf_node));

			p->n_dupl = 0;

			p->pending_emigrations = 0;
			p->pending_lead_emigrations = 0;
			p->pending_immigrations = 0;

			p->n_witnesses = 0;

			uint32_t self_n = find_self(ns_node_seq, ns);

			as_partition_version final_version = {
					.ckey = as_exchange_cluster_key(),
					.master = self_n == 0 ? 1 : 0
			};

			p->final_version = final_version;

			int working_master_n = find_working_master_ap(p, ns_sl_ix, ns);

			uint32_t n_dupl = 0;
			cf_node dupls[ns->cluster_size];

			as_partition_version orig_version = p->version;

			// TEMPORARY debugging.
			uint32_t debug_n_immigrators = 0;

			if (working_master_n == -1) {
				// No existing versions - assign fresh version to replicas.
				working_master_n = 0;

				if (self_n < p->n_replicas) {
					p->version = p->final_version;
				}

				ns_fresh_partitions++;
			}
			else {
				n_dupl = find_duplicates_ap(p, ns_node_seq, ns_sl_ix, ns,
						(uint32_t)working_master_n, dupls);

				uint32_t n_immigrators = fill_immigrators(p, ns_sl_ix, ns,
						(uint32_t)working_master_n, n_dupl);

				// TEMPORARY debugging.
				debug_n_immigrators = n_immigrators;

				if (n_immigrators != 0) {
					// Migrations required - advance versions for next
					// rebalance, queue migrations for this rebalance.

					advance_version_ap(p, ns_sl_ix, ns, self_n,
							(uint32_t)working_master_n, n_dupl, dupls);

					uint32_t lead_flags[ns->replication_factor];

					emig_lead_flags_ap(p, ns_sl_ix, ns, lead_flags);

					queue_namespace_migrations(p, ns, self_n,
							ns_node_seq[working_master_n], n_dupl, dupls,
							lead_flags, mq);

					if (self_n == 0) {
						fill_witnesses(p, ns_node_seq, ns_sl_ix, ns);
						ns_pending_signals += p->n_witnesses;
					}
				}
				else if (self_n < p->n_replicas) {
					// No migrations required - refresh replicas' versions (only
					// truly necessary if replication factor decreased).
					p->version = p->final_version;
				}
				else {
					// No migrations required - drop superfluous non-replica
					// partitions immediately.
					if (! drop_superfluous_version(p, ns)) {
						// Quiesced nodes become subset of final version.
						adjust_superfluous_version(p, ns);
					}
				}
			}

			if (self_n == 0 || self_n == working_master_n) {
				p->working_master = ns_node_seq[working_master_n];
			}

			handle_version_change(p, ns, &orig_version);

			ns_pending_emigrations += p->pending_emigrations;
			ns_pending_lead_emigrations += p->pending_lead_emigrations;
			ns_pending_immigrations += p->pending_immigrations;

			// TEMPORARY debugging.
			if (pid < 20) {
				cf_debug(AS_PARTITION, "ck%012lX %02u (%hu %hu) %s -> %s - self_n %u wm_n %d repls %u dupls %u immigrators %u",
						as_exchange_cluster_key(), pid, p->pending_emigrations,
						p->pending_immigrations,
						VERSION_AS_STRING(&orig_version),
						VERSION_AS_STRING(&p->version), self_n,
						working_master_n, p->n_replicas, n_dupl,
						debug_n_immigrators);
			}

			client_replica_maps_update(ns, pid);
		}

		// Flush partition metadata for this group of partitions ...
		as_storage_flush_pmeta(ns, start_pid, PIDS_PER_GROUP);

		// ... and unlock the group.
		for (uint32_t pid = start_pid; pid < end_pid; pid++) {
			as_partition* p = &ns->partitions[pid];

			cf_mutex_unlock(&p->lock);
		}
	}

	cf_info(AS_PARTITION, "{%s} rebalanced: expected-migrations (%u,%u,%u) fresh-partitions %u",
			ns->name, ns_pending_emigrations, ns_pending_immigrations,
			ns_pending_signals, ns_fresh_partitions);

	ns->n_unavailable_partitions = 0;

	ns->migrate_tx_partitions_initial = ns_pending_emigrations;
	ns->migrate_tx_partitions_remaining = ns_pending_emigrations;
	ns->migrate_tx_partitions_lead_remaining = ns_pending_lead_emigrations;

	ns->migrate_rx_partitions_initial = ns_pending_immigrations;
	ns->migrate_rx_partitions_remaining = ns_pending_immigrations;

	ns->migrate_signals_remaining = ns_pending_signals;
}

void
apply_single_replica_limit_ap(as_namespace* ns)
{
	// Replication factor can't be bigger than observed cluster.
	uint32_t repl_factor = ns->cluster_size < ns->cfg_replication_factor ?
			ns->cluster_size : ns->cfg_replication_factor;

	// Reduce the replication factor to 1 if the cluster size is less than or
	// equal to the specified limit.
	ns->replication_factor =
			ns->cluster_size <= g_config.paxos_single_replica_limit ?
					1 : repl_factor;

	cf_info(AS_PARTITION, "{%s} replication factor is %u", ns->name,
			ns->replication_factor);
}

void
fill_translation(int translation[], const as_namespace* ns)
{
	int ns_n = 0;

	for (uint32_t full_n = 0; full_n < g_cluster_size; full_n++) {
		translation[full_n] = ns_n < ns->cluster_size &&
				g_succession[full_n] == ns->succession[ns_n] ? ns_n++ : -1;
	}
}

void
fill_namespace_rows(const cf_node* full_node_seq, const sl_ix_t* full_sl_ix,
		cf_node* ns_node_seq, sl_ix_t* ns_sl_ix, const as_namespace* ns,
		const int translation[])
{
	if (ns->cluster_size == g_cluster_size) {
		// Rack-aware but namespace is on all nodes - just copy. Rack-aware will
		// rearrange the copies - we can't rearrange the global originals.
		memcpy(ns_node_seq, full_node_seq, g_cluster_size * sizeof(cf_node));
		memcpy(ns_sl_ix, full_sl_ix, g_cluster_size * sizeof(sl_ix_t));

		return;
	}

	// Fill namespace sequences from global table rows using translation array.
	uint32_t n = 0;

	for (uint32_t full_n = 0; full_n < g_cluster_size; full_n++) {
		int ns_n = translation[full_sl_ix[full_n]];

		if (ns_n != -1) {
			ns_node_seq[n] = ns->succession[ns_n];
			ns_sl_ix[n] = (sl_ix_t)ns_n;
			n++;
		}
	}
}

uint32_t
find_self(const cf_node* ns_node_seq, const as_namespace* ns)
{
	int n = index_of_node(ns_node_seq, ns->cluster_size, g_config.self_node);

	cf_assert(n != -1, AS_PARTITION, "{%s} self node not in succession list",
			ns->name);

	return (uint32_t)n;
}

// Preference: Vm > V > Ve > Vs > Vse > absent.
int
find_working_master_ap(const as_partition* p, const sl_ix_t* ns_sl_ix,
		const as_namespace* ns)
{
	int best_n = -1;
	int best_score = -1;

	for (int n = 0; n < (int)ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		// Skip versions with no data.
		if (! as_partition_version_has_data(version)) {
			continue;
		}

		// If previous working master exists, use it. (There can be more than
		// one after split brains. Also, the flag is only to prevent superfluous
		// master swaps on rebalance when rack-aware.)
		if (version->master == 1) {
			return shift_working_master(p, ns_sl_ix, ns, n, version);
		}
		// else - keep going but remember the best so far.

		// V = 3 > Ve = 2 > Vs = 1 > Vse = 0.
		int score = (version->evade == 1 ? 0 : 1) +
				(version->subset == 1 ? 0 : 2);

		if (score > best_score) {
			best_score = score;
			best_n = n;
		}
	}

	return best_n;
}

int
shift_working_master(const as_partition* p, const sl_ix_t* ns_sl_ix,
		const as_namespace* ns, int working_master_n,
		const as_partition_version* working_master_version)
{
	if (working_master_n == 0 || working_master_version->subset == 1) {
		return working_master_n; // can only shift full masters
	}

	for (int n = 0; n < working_master_n; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		if (is_same_as_full_master(working_master_version, version)) {
			return n; // master flag will get shifted later
		}
	}

	return working_master_n;
}

uint32_t
find_duplicates_ap(const as_partition* p, const cf_node* ns_node_seq,
		const sl_ix_t* ns_sl_ix, const as_namespace* ns,
		uint32_t working_master_n, cf_node dupls[])
{
	uint32_t n_dupl = 0;
	as_partition_version parent_dupl_versions[ns->cluster_size];

	memset(parent_dupl_versions, 0, sizeof(parent_dupl_versions));

	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		// Skip versions without data, and postpone subsets to next pass.
		if (! as_partition_version_has_data(version) || version->subset == 1) {
			continue;
		}

		// Every unique version is a duplicate.
		if (version->family == VERSION_FAMILY_UNIQUE) {
			dupls[n_dupl++] = ns_node_seq[n];
			continue;
		}

		// Add parent versions as duplicates, unless they are already in.

		uint32_t d;

		for (d = 0; d < n_dupl; d++) {
			if (is_family_same(&parent_dupl_versions[d], version)) {
				break;
			}
		}

		if (d == n_dupl) {
			// Not in dupls.
			parent_dupl_versions[n_dupl] = *version;
			dupls[n_dupl++] = ns_node_seq[n];
		}
	}

	// Second pass to deal with subsets.
	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		if (version->subset == 0) {
			continue;
		}

		uint32_t d;

		for (d = 0; d < n_dupl; d++) {
			if (is_family_same(&parent_dupl_versions[d], version)) {
				break;
			}
		}

		if (d == n_dupl) {
			// Not in dupls.
			// Leave 0 in parent_dupl_versions array.
			dupls[n_dupl++] = ns_node_seq[n];
		}
	}

	// Remove working master from 'variants' to leave duplicates.
	return remove_node(dupls, n_dupl, ns_node_seq[working_master_n]);
}

uint32_t
fill_immigrators(as_partition* p, const sl_ix_t* ns_sl_ix, as_namespace* ns,
		uint32_t working_master_n, uint32_t n_dupl)
{
	memset(p->immigrators, 0, ((sizeof(bool) * ns->cluster_size) + 31) & -32);

	uint32_t n_immigrators = 0;

	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (n_dupl != 0 || (repl_ix != working_master_n &&
				(! as_partition_version_has_data(version) ||
						version->subset == 1))) {
			p->immigrators[repl_ix] = true;
			n_immigrators++;
		}
	}

	return n_immigrators;
}

void
advance_version_ap(as_partition* p, const sl_ix_t* ns_sl_ix, as_namespace* ns,
		uint32_t self_n, uint32_t working_master_n, uint32_t n_dupl,
		const cf_node dupls[])
{
	// Advance working master.
	if (self_n == working_master_n) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = (self_n == 0 || n_dupl == 0) ? 0 : 1;
		p->version.master = 1;
		p->version.subset = 0;
		p->version.evade = 0;

		return;
	}

	p->version.master = 0;

	bool self_is_versionless = ! as_partition_version_has_data(&p->version);

	// Advance eventual master.
	if (self_n == 0) {
		bool was_subset = p->version.subset == 1;

		p->version.ckey = p->final_version.ckey;
		p->version.family = 0;
		p->version.subset = n_dupl == 0 ? 1 : 0;

		if (self_is_versionless || (was_subset && p->version.subset == 0)) {
			p->version.evade = 1;
		}
		// else - don't change evade flag.

		return;
	}

	// Advance version-less proles and non-replicas (common case).
	if (self_is_versionless) {
		if (self_n < p->n_replicas) {
			p->version.ckey = p->final_version.ckey;
			p->version.family = 0;
			p->version.subset = 1;
			p->version.evade = 1;
		}
		// else - non-replicas remain version-less.

		return;
	}

	// Fill family versions.

	uint32_t max_n_families = p->n_replicas + 1;

	if (max_n_families > AS_PARTITION_N_FAMILIES) {
		max_n_families = AS_PARTITION_N_FAMILIES;
	}

	as_partition_version family_versions[max_n_families];
	uint32_t n_families = fill_family_versions(p, ns_sl_ix, ns,
			working_master_n, n_dupl, dupls, family_versions);

	uint32_t family = find_family(&p->version, n_families, family_versions);

	// Advance non-masters with prior versions ...

	// ... proles ...
	if (self_n < p->n_replicas) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = family;

		if (n_dupl != 0 && p->version.family == 0) {
			p->version.subset = 1;
		}
		// else - don't change either subset or evade flag.

		return;
	}

	// ... or non-replicas.
	if (family != VERSION_FAMILY_UNIQUE &&
			family_versions[family].subset == 0) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = family;
		p->version.subset = 1;
	}
	// else - leave version as-is.
}

uint32_t
fill_family_versions(const as_partition* p, const sl_ix_t* ns_sl_ix,
		const as_namespace* ns, uint32_t working_master_n, uint32_t n_dupl,
		const cf_node dupls[], as_partition_version family_versions[])
{
	uint32_t n_families = 1;
	const as_partition_version* final_master_version = INPUT_VERSION(0);

	family_versions[0] = *final_master_version;

	if (working_master_n != 0) {
		const as_partition_version* working_master_version =
				INPUT_VERSION(working_master_n);

		if (n_dupl == 0) {
			family_versions[0] = *working_master_version;
		}
		else {
			family_versions[0] = p->final_version; // not matchable
			family_versions[1] = *working_master_version;
			n_families = 2;
		}
	}

	for (uint32_t repl_ix = 1;
			repl_ix < p->n_replicas && n_families < AS_PARTITION_N_FAMILIES;
			repl_ix++) {
		if (repl_ix == working_master_n) {
			continue;
		}

		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (contains_node(dupls, n_dupl, p->replicas[repl_ix])) {
			family_versions[n_families++] = *version;
		}
		else if (version->subset == 1 &&
				! has_replica_parent(p, ns_sl_ix, ns, version, repl_ix)) {
			family_versions[n_families++] = *version;
		}
	}

	return n_families;
}

bool
has_replica_parent(const as_partition* p, const sl_ix_t* ns_sl_ix,
		const as_namespace* ns, const as_partition_version* subset_version,
		uint32_t subset_n)
{
	for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
		if (repl_ix == subset_n) {
			continue;
		}

		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (version->subset == 0 && is_family_same(version, subset_version)) {
			return true;
		}
	}

	return false;
}

uint32_t
find_family(const as_partition_version* self_version, uint32_t n_families,
		const as_partition_version family_versions[])
{
	for (uint32_t n = 0; n < n_families; n++) {
		if (is_family_same(self_version, &family_versions[n])) {
			return n;
		}
	}

	return VERSION_FAMILY_UNIQUE;
}

void
queue_namespace_migrations(as_partition* p, as_namespace* ns, uint32_t self_n,
		cf_node working_master, uint32_t n_dupl, cf_node dupls[],
		const uint32_t lead_flags[], cf_queue* mq)
{
	pb_task task;

	if (self_n == 0) {
		// <><><><><><>  Final Master  <><><><><><>

		if (g_config.self_node == working_master) {
			p->pending_immigrations = (uint16_t)n_dupl;
		}
		else {
			// Remove self from duplicates.
			n_dupl = remove_node(dupls, n_dupl, g_config.self_node);

			p->pending_immigrations = (uint16_t)n_dupl + 1;
		}

		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupls, n_dupl * sizeof(cf_node));
		}

		if (p->pending_immigrations != 0) {
			for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
				if (p->immigrators[repl_ix]) {
					p->pending_emigrations++;
				}
			}

			// Emigrate later, after all immigration is complete.
			return;
		}

		// Emigrate now, no immigrations to wait for.
		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->immigrators[repl_ix]) {
				p->pending_emigrations++;

				if (lead_flags[repl_ix] != TX_FLAGS_NONE) {
					p->pending_lead_emigrations++;
				}

				pb_task_init(&task, p->replicas[repl_ix], ns, p->id,
						as_exchange_cluster_key(), PB_TASK_EMIG_TRANSFER,
						lead_flags[repl_ix]);
				cf_queue_push(mq, &task);
			}
		}

		return;
	}
	// else - <><><><><><>  Not Final Master  <><><><><><>

	if (g_config.self_node == working_master) {
		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupls, n_dupl * sizeof(cf_node));
		}

		p->pending_emigrations = 1;

		if (lead_flags[0] != TX_FLAGS_NONE) {
			p->pending_lead_emigrations = 1;
		}

		pb_task_init(&task, p->replicas[0], ns, p->id,
				as_exchange_cluster_key(), PB_TASK_EMIG_TRANSFER,
				TX_FLAGS_ACTING_MASTER | lead_flags[0]);
		cf_queue_push(mq, &task);
	}
	else if (contains_self(dupls, n_dupl)) {
		p->pending_emigrations = 1;

		if (lead_flags[0] != TX_FLAGS_NONE) {
			p->pending_lead_emigrations = 1;
		}

		pb_task_init(&task, p->replicas[0], ns, p->id,
				as_exchange_cluster_key(), PB_TASK_EMIG_TRANSFER,
				lead_flags[0]);
		cf_queue_push(mq, &task);
	}

	if (self_n < p->n_replicas && p->immigrators[self_n]) {
		p->pending_immigrations = 1;
	}
}

void
fill_witnesses(as_partition* p, const cf_node* ns_node_seq,
		const sl_ix_t* ns_sl_ix, as_namespace* ns)
{
	for (uint32_t n = 1; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		// Note - 0e/0r versions (CP) are witnesses.
		if (n < p->n_replicas || ! as_partition_version_is_null(version)) {
			p->witnesses[p->n_witnesses++] = ns_node_seq[n];
		}
	}
}

// If version changed, create/drop trees as appropriate, and cache for storage.
void
handle_version_change(as_partition* p, struct as_namespace_s* ns,
		as_partition_version* orig_version)
{
	if (as_partition_version_same(&p->version, orig_version)) {
		return;
	}

	if (! as_partition_version_has_data(orig_version) &&
			as_partition_version_has_data(&p->version)) {
		create_trees(p, ns);
	}

	if (as_partition_version_has_data(orig_version) &&
			! as_partition_version_has_data(&p->version)) {
		// FIXME - temporary paranoia.
		cf_assert(p->tree, AS_PARTITION, "unexpected - null tree");
		drop_trees(p);
	}

	as_storage_cache_pmeta(ns, p);
}


//==========================================================
// Local helpers - migration-related as_partition methods.
//

// Sanity checks for immigrations commands.
bool
partition_immigration_is_valid(const as_partition* p, cf_node source_node,
		const as_namespace* ns, const char* tag)
{
	char* failure_reason = NULL;

	if (p->pending_immigrations == 0) {
		failure_reason = "no immigrations expected";
	}
	else if (is_self_final_master(p)) {
		if (source_node != p->working_master &&
				! contains_node(p->dupls, p->n_dupl, source_node)) {
			failure_reason = "final master's source not acting master or duplicate";
		}
	}
	else if (source_node != p->replicas[0]) {
		failure_reason = "prole's source not final working master";
	}

	if (failure_reason) {
		cf_warning(AS_PARTITION, "{%s:%u} immigrate_%s - source %lx working-master %lx pending-immigrations %hu - %s",
				ns->name, p->id, tag, source_node, p->working_master,
				p->pending_immigrations, failure_reason);

		return false;
	}

	return true;
}

void
emigrate_done_advance_non_master_version_ap(as_namespace* ns, as_partition* p,
		uint32_t tx_flags)
{
	if ((tx_flags & TX_FLAGS_ACTING_MASTER) != 0) {
		p->working_master = (cf_node)0;
		p->n_dupl = 0;
		p->version.master = 0;
	}

	p->version.ckey = p->final_version.ckey;
	p->version.family = 0;

	if (p->pending_immigrations != 0 || ! is_self_replica(p)) {
		p->version.subset = 1;
	}
	// else - must already be a parent.

	as_storage_save_pmeta(ns, p);
}

void
immigrate_start_advance_non_master_version_ap(as_partition* p)
{
	// Become subset of final version if not already such.
	if (! (p->version.ckey == p->final_version.ckey &&
			p->version.family == 0 && p->version.subset == 1)) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = 0;
		p->version.master = 0; // racing emigrate done if we were acting master
		p->version.subset = 1;
		// Leave evade flag as-is.
	}
}

void
immigrate_done_advance_final_master_version_ap(as_namespace* ns,
		as_partition* p)
{
	if (! as_partition_version_same(&p->version, &p->final_version)) {
		p->version = p->final_version;
		as_storage_save_pmeta(ns, p);
	}
}
