/*
 * partition_balance.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_queue.h"

#include "dynbuf.h"
#include "fault.h"
#include "node.h"

#include "fabric/hb.h"
#include "fabric/partition.h"


//==========================================================
// Forward declarations.
//

struct as_namespace_s;


//==========================================================
// Typedefs & constants.
//

typedef enum {
	PB_TASK_EMIG_TRANSFER,
	PB_TASK_EMIG_SIGNAL_ALL_DONE,
	PB_TASK_APPEAL
} pb_task_type;

typedef struct pb_task_s {
	cf_node dest;
	struct as_namespace_s* ns;
	uint32_t pid;
	uint64_t cluster_key;
	pb_task_type type;
	uint32_t tx_flags;
} pb_task;

#define MAX_RACK_ID 1000000
#define MAX_RACK_ID_LEN 7 // number of decimal characters


//==========================================================
// Public API - regulate migrations.
//

void as_partition_balance_disallow_migrations();
bool as_partition_balance_are_migrations_allowed();
void as_partition_balance_synchronize_migrations();
void as_partition_balance_emigration_yield();


//==========================================================
// Public API - balance partitions.
//

void as_partition_balance_init();
bool as_partition_balance_is_init_resolved();
void as_partition_balance_revert_to_orphan();
void as_partition_balance();

uint64_t as_partition_balance_remaining_migrations();
bool as_partition_balance_revive(struct as_namespace_s* ns);
void as_partition_balance_protect_roster_set(struct as_namespace_s* ns);
void as_partition_balance_effective_rack_ids(cf_dyn_buf* db);


//==========================================================
// Public API - migration-related as_partition methods.
//

bool as_partition_pending_migrations(as_partition* p);

bool as_partition_pre_emigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, uint32_t tx_flags);
void as_partition_emigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, uint32_t tx_flags);
as_migrate_result as_partition_immigrate_start(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, cf_node source_node);
as_migrate_result as_partition_immigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, cf_node source_node);
as_migrate_result as_partition_migrations_all_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key);
void as_partition_signal_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key);

// Counter that tells clients partition ownership has changed.
extern cf_atomic32 g_partition_generation;

// Time of last rebalance.
extern uint64_t g_rebalance_sec;

// Count rebalances.
extern uint64_t g_rebalance_generation;


//==========================================================
// Private API - for enterprise separation only.
//

//------------------------------------------------
// Typedefs & constants.
//

COMPILER_ASSERT((AS_CLUSTER_SZ & (AS_CLUSTER_SZ - 1)) == 0);

#define AS_CLUSTER_SZ_MASKP (-(uint64_t)AS_CLUSTER_SZ)
#define AS_CLUSTER_SZ_MASKN ((uint64_t)AS_CLUSTER_SZ - 1)

typedef uint8_t sl_ix_t;

COMPILER_ASSERT(AS_CLUSTER_SZ_MASKN >> (sizeof(sl_ix_t) * 8) == 0);

typedef struct inter_hash_s {
	uint64_t hashed_node;
	uint64_t hashed_pid;
} inter_hash;

extern const as_partition_version ZERO_VERSION;

#define REBALANCE_FLUSH_SIZE 4096
#define PMETA_SIZE 16 // sizeof(ssd_common_pmeta) without including drv_ssd.h

#define PIDS_PER_GROUP (REBALANCE_FLUSH_SIZE / PMETA_SIZE) // 256
#define NUM_PID_GROUPS (AS_PARTITIONS / PIDS_PER_GROUP) // 16


//------------------------------------------------
// Globals.
//

extern volatile int g_allow_migrations;

extern uint64_t g_hashed_pids[AS_PARTITIONS];

// Shortcuts to values set by as_exchange, for use in partition balance only.
extern uint32_t g_cluster_size;
extern cf_node* g_succession;

extern cf_node g_full_node_seq_table[AS_CLUSTER_SZ * AS_PARTITIONS];
extern sl_ix_t g_full_sl_ix_table[AS_CLUSTER_SZ * AS_PARTITIONS];


//------------------------------------------------
// Forward declarations.
//

void partition_balance_init();

void pb_task_init(pb_task* task, cf_node dest, struct as_namespace_s* ns, uint32_t pid, uint64_t cluster_key, pb_task_type type, uint32_t tx_flags);

void balance_namespace(struct as_namespace_s* ns, cf_queue* mq);
void prepare_for_appeals();
void process_pb_tasks(cf_queue* tq);
void balance_namespace_ap(struct as_namespace_s* ns, cf_queue* mq);
void set_active_size(struct as_namespace_s* ns);
uint32_t rack_count(const struct as_namespace_s* ns);
void fill_translation(int translation[], const struct as_namespace_s* ns);
void init_target_claims_ap(const struct as_namespace_s* ns, const int translation[], uint32_t* target_claims);
void fill_namespace_rows(const cf_node* full_node_seq, const sl_ix_t* full_sl_ix, cf_node* ns_node_seq, sl_ix_t* ns_sl_ix, const struct as_namespace_s* ns, const int translation[]);
void quiesce_adjust_row(cf_node* ns_node_seq, sl_ix_t* ns_sl_ix, struct as_namespace_s* ns);
void uniform_adjust_row(cf_node* node_seq, uint32_t n_nodes, sl_ix_t* ns_sl_ix, uint32_t n_replicas, uint32_t* claims, const uint32_t* target_claims, const uint32_t* rack_ids, uint32_t n_racks);
void rack_aware_adjust_row(cf_node* ns_node_seq, sl_ix_t* ns_sl_ix, uint32_t replication_factor, const uint32_t* rack_ids, uint32_t n_ids, uint32_t n_racks, uint32_t start_n);
uint32_t find_self(const cf_node* ns_node_seq, const struct as_namespace_s* ns);
int shift_working_master(const as_partition* p, const sl_ix_t* ns_sl_ix, const struct as_namespace_s* ns, int working_master_n, const as_partition_version* working_master_version);
uint32_t fill_immigrators(as_partition* p, const sl_ix_t* ns_sl_ix, struct as_namespace_s* ns, uint32_t working_master_n, uint32_t n_dupl);
void emig_lead_flags_ap(const as_partition* p, const sl_ix_t* ns_sl_ix, const struct as_namespace_s* ns, uint32_t lead_flags[]);
void queue_namespace_migrations(as_partition* p, struct as_namespace_s* ns, uint32_t self_n, cf_node working_master, uint32_t n_dupl, cf_node dupls[], const uint32_t lead_flags[], cf_queue* mq);
bool drop_superfluous_version(as_partition* p, struct as_namespace_s* ns);
bool adjust_superfluous_version(as_partition* p, struct as_namespace_s* ns);
void fill_witnesses(as_partition* p, const cf_node* ns_node_seq, const sl_ix_t* ns_sl_ix, struct as_namespace_s* ns);
void handle_version_change(as_partition* p, struct as_namespace_s* ns, as_partition_version* orig_version);

void emigrate_done_advance_non_master_version(struct as_namespace_s* ns, as_partition* p, uint32_t tx_flags);
void emigrate_done_advance_non_master_version_ap(struct as_namespace_s* ns, as_partition* p, uint32_t tx_flags);
void immigrate_start_advance_non_master_version(struct as_namespace_s* ns, as_partition* p);
void immigrate_start_advance_non_master_version_ap(as_partition* p);
void immigrate_done_advance_final_master_version(struct as_namespace_s* ns, as_partition* p);
void immigrate_done_advance_final_master_version_ap(struct as_namespace_s* ns, as_partition* p);
bool immigrate_yield();


//------------------------------------------------
// Inlines and macros.
//

static inline bool
is_same_as_full_master(const as_partition_version* mv, const as_partition_version* v)
{
	// Works for CP too, even with family check.
	return v->subset == 0 && mv->ckey == v->ckey && mv->family == v->family &&
			mv->family != VERSION_FAMILY_UNIQUE;
}

// Define macros for accessing the full node-seq and sl-ix arrays.
#define FULL_NODE_SEQ(x, y) g_full_node_seq_table[(x * g_cluster_size) + y]
#define FULL_SL_IX(x, y) g_full_sl_ix_table[(x * g_cluster_size) + y]

// Get the partition version that was input by exchange.
#define INPUT_VERSION(_n) (&ns->cluster_versions[ns_sl_ix[_n]][p->id])
