/*
 * partition.h
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
#include <stdio.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "node.h"

#include "base/cfg.h"
#include "fabric/hb.h"


//==========================================================
// Forward declarations.
//

struct as_index_tree_s;
struct as_namespace_s;
struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

#define AS_PARTITIONS 4096
#define AS_PARTITION_MASK (AS_PARTITIONS - 1)

#define VERSION_FAMILY_BITS 4
#define VERSION_FAMILY_UNIQUE ((1 << VERSION_FAMILY_BITS) - 1)
#define AS_PARTITION_N_FAMILIES VERSION_FAMILY_UNIQUE

typedef struct as_partition_version_s {
	uint64_t ckey:48;
	uint64_t family:VERSION_FAMILY_BITS;
	uint64_t unused:8;
	uint64_t revived:1; // enterprise only
	uint64_t master:1;
	uint64_t subset:1;
	uint64_t evade:1;
} as_partition_version;

COMPILER_ASSERT(sizeof(as_partition_version) == sizeof(uint64_t));

typedef struct as_partition_version_string_s {
	char s[19 + 1]; // format CCCCccccCCCC.F.mse - F may someday be 2 characters
} as_partition_version_string;

typedef struct as_partition_s {
	//--------------------------------------------
	// Used during every transaction.
	//

	cf_atomic64 n_tombstones; // relevant only for enterprise edition
	cf_atomic32 max_void_time; // TODO - do we really need this?

	cf_mutex lock;

	struct as_index_tree_s* tree;

	cf_node working_master;

	uint32_t regime; // relevant only for enterprise edition

	uint32_t n_nodes; // relevant only for enterprise edition
	uint32_t n_replicas;
	uint32_t n_dupl;

	// @ 48 bytes - room for 2 replicas within above 64-byte cache line.
	cf_node replicas[AS_CLUSTER_SZ];

	uint8_t align_1[16];
	// @ 64-byte-aligned boundary.

	//--------------------------------------------
	// Used only when cluster is in flux.
	//

	uint32_t id;

	bool must_appeal; // relevant only for enterprise edition

	uint8_t tree_id;
	uint64_t tree_ids_used; // bit map

	as_partition_version final_version;
	as_partition_version version;

	uint16_t pending_emigrations;
	uint16_t pending_lead_emigrations;
	uint16_t pending_immigrations;

	uint16_t n_witnesses;

	// @ 40 bytes - room for 3 duplicates within above 64-byte cache line.
	cf_node dupls[AS_CLUSTER_SZ];

	uint8_t align_2[24];
	// @ 64-byte-aligned boundary.

	bool immigrators[AS_CLUSTER_SZ];
	// Byte alignment depends on AS_CLUSTER_SZ - pad below to realign.

	uint8_t align_3[AS_CLUSTER_SZ == 8 ? 56 : 0];
	// @ 64-byte-aligned boundary.

	cf_node witnesses[AS_CLUSTER_SZ];
} as_partition;

COMPILER_ASSERT(sizeof(as_partition) % 64 == 0);

typedef struct as_partition_reservation_s {
	struct as_namespace_s* ns;
	as_partition* p;
	struct as_index_tree_s* tree;
	uint32_t regime;
	uint32_t n_dupl;
	cf_node dupl_nodes[AS_CLUSTER_SZ];
} as_partition_reservation;

typedef struct repl_stats_s {
	uint64_t n_master_objects;
	uint64_t n_prole_objects;
	uint64_t n_non_replica_objects;
	uint64_t n_master_tombstones;
	uint64_t n_prole_tombstones;
	uint64_t n_non_replica_tombstones;
} repl_stats;

#define CLIENT_BITMAP_BYTES ((AS_PARTITIONS + 7) / 8)
#define CLIENT_B64MAP_BYTES (((CLIENT_BITMAP_BYTES + 2) / 3) * 4)

typedef struct client_replica_map_s {
	cf_mutex write_lock;

	volatile uint8_t bitmap[CLIENT_BITMAP_BYTES];
	volatile char b64map[CLIENT_B64MAP_BYTES];
} client_replica_map;

typedef enum {
	AS_MIGRATE_OK,
	AS_MIGRATE_FAIL,
	AS_MIGRATE_AGAIN
} as_migrate_result;


//==========================================================
// Public API.
//

void as_partition_init(struct as_namespace_s* ns, uint32_t pid);
void as_partition_shutdown(struct as_namespace_s* ns, uint32_t pid);

void as_partition_isolate_version(const struct as_namespace_s* ns, as_partition* p);
int as_partition_check_source(const struct as_namespace_s* ns, as_partition* p, cf_node src, bool* from_replica);
void as_partition_freeze(as_partition* p);

uint32_t as_partition_get_other_replicas(as_partition* p, cf_node* nv);

cf_node as_partition_writable_node(struct as_namespace_s* ns, uint32_t pid);
cf_node as_partition_proxyee_redirect(struct as_namespace_s* ns, uint32_t pid);

void as_partition_get_replicas_master_str(cf_dyn_buf* db);
void as_partition_get_replicas_all_str(cf_dyn_buf* db, bool include_regime);

void as_partition_get_replica_stats(struct as_namespace_s* ns, repl_stats* p_stats);

void as_partition_reserve(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_replica(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_write(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node);
int as_partition_reserve_read_tr(struct as_namespace_s* ns, uint32_t pid, struct as_transaction_s* tr, cf_node* node);
int as_partition_prereserve_query(struct as_namespace_s* ns, bool can_partition_query[], as_partition_reservation rsv[]);
int as_partition_reserve_query(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_xdr_read(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
void as_partition_reservation_copy(as_partition_reservation* dst, as_partition_reservation* src);

void as_partition_release(as_partition_reservation* rsv);

void as_partition_advance_tree_id(as_partition* p, const char* ns_name);
void as_partition_tree_done(uint8_t id, void* udata);

void as_partition_getinfo_str(cf_dyn_buf* db);

// Use VERSION_AS_STRING() - see below.
static inline as_partition_version_string
as_partition_version_as_string(const as_partition_version* version)
{
	as_partition_version_string str;

	if (version->family == VERSION_FAMILY_UNIQUE) {
		sprintf(str.s, "%012lx.U.%c%c%c", (uint64_t)version->ckey,
				version->master == 0 ? '-' : 'm',
				version->subset == 0 ? 'p' : 's',
				version->evade == 0 ? '-' : 'e');
	}
	else {
		sprintf(str.s, "%012lx.%X.%c%c%c", (uint64_t)version->ckey,
				(uint32_t)version->family,
				version->master == 0 ? '-' : 'm',
				version->subset == 0 ? 'p' : 's',
				version->evade == 0 ?
						(version->revived == 0 ? '-' : 'r') : 'e');
	}

	return str;
}

static inline bool
as_partition_version_is_null(const as_partition_version* version)
{
	return *(uint64_t*)version == 0;
}

static inline bool
as_partition_version_has_data(const as_partition_version* version)
{
	return version->ckey != 0;
}

static inline bool
as_partition_version_same(const as_partition_version* v1, const as_partition_version* v2)
{
	return *(uint64_t*)v1 == *(uint64_t*)v2;
}

static inline uint32_t
as_partition_getid(const cf_digest* d)
{
	return *(uint32_t*)d & AS_PARTITION_MASK;
}

static inline int
find_self_in_replicas(const as_partition* p)
{
	return index_of_node(p->replicas, p->n_replicas, g_config.self_node);
}

static inline bool
is_self_replica(const as_partition* p)
{
	return contains_node(p->replicas, p->n_replicas, g_config.self_node);
}

static inline bool
contains_self(const cf_node* nodes, uint32_t n_nodes)
{
	return contains_node(nodes, n_nodes, g_config.self_node);
}

#define AS_PARTITION_ID_UNDEF ((uint16_t)0xFFFF)

#define AS_PARTITION_RESERVATION_INIT(__rsv) \
	__rsv.ns = NULL; \
	__rsv.p = NULL; \
	__rsv.tree = NULL; \
	__rsv.regime = 0; \
	__rsv.n_dupl = 0;

#define VERSION_AS_STRING(v_ptr) (as_partition_version_as_string(v_ptr).s)


//==========================================================
// Public API - client view replica maps.
//

void client_replica_maps_create(struct as_namespace_s* ns);
void client_replica_maps_clear(struct as_namespace_s* ns);
bool client_replica_maps_update(struct as_namespace_s* ns, uint32_t pid);
bool client_replica_maps_is_partition_queryable(const struct as_namespace_s* ns, uint32_t pid);


//==========================================================
// Private API - for enterprise separation only.
//

int partition_reserve_unavailable(const struct as_namespace_s* ns, const as_partition* p, struct as_transaction_s* tr, cf_node* node);
bool partition_reserve_promote(const struct as_namespace_s* ns, const as_partition* p, struct as_transaction_s* tr);
