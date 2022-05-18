/*
 * sindex_tree.h
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "arenax.h"

#include "sindex/sindex_arena.h"


//==========================================================
// Forward declarations.
//

struct as_index_ref_s;
struct as_partition_reservation_s;
struct as_query_range_s;
struct as_sindex_s;
struct as_sindex_arena_s;
struct si_btree_node_s;


//==========================================================
// Typedefs & constants.
//

typedef bool (*as_sindex_reduce_fn)(struct as_index_ref_s* value, int64_t bval, void* udata);

// In header for enterprise separation only - not public.

typedef struct si_btree_s {
	pthread_rwlock_t lock;
	cf_arenax* arena;
	struct as_sindex_arena_s* si_arena;
	bool unsigned_bvals;
	uint32_t inner_order;
	uint32_t leaf_order;
	uint32_t keys_off;
	uint32_t children_off;
	si_arena_handle root_h;
	uint64_t n_nodes;
	uint64_t n_keys;
} si_btree;

typedef struct si_btree_key_s {
	int64_t bval;
	uint8_t keyd_stub;
	uint64_t r_h: 40; // primary index arena handle
} __attribute__ ((__packed__)) si_btree_key;

typedef struct search_key_s {
	int64_t bval;
	bool has_digest;
	uint8_t keyd_stub;
	cf_digest keyd;
} search_key;

typedef bool (*si_btree_reduce_fn)(const si_btree_key* key, void* udata);


//==========================================================
// Public API.
//

void as_sindex_tree_create(struct as_sindex_s* si);
void as_sindex_tree_destroy(struct as_sindex_s* si);

uint64_t as_sindex_tree_n_keys(const struct as_sindex_s* si);
uint64_t as_sindex_tree_mem_size(const struct as_sindex_s* si);

void as_sindex_tree_gc(struct as_sindex_s* si);

bool as_sindex_tree_put(struct as_sindex_s* si, int64_t bval, cf_arenax_handle r_h);
bool as_sindex_tree_delete(struct as_sindex_s* si, int64_t bval, cf_arenax_handle r_h);
void as_sindex_tree_query(struct as_sindex_s* si, const struct as_query_range_s* range, struct as_partition_reservation_s* rsv, int64_t bval, cf_digest* keyd, as_sindex_reduce_fn cb, void* udata);


//==========================================================
// Private API - for enterprise separation only.
//

void query_reduce_no_rc(si_btree* bt, struct as_partition_reservation_s* rsv, int64_t start_bval, int64_t end_bval, int64_t resume_bval, cf_digest* keyd, bool de_dup, as_sindex_reduce_fn cb, void* udata);

void si_btree_reduce(si_btree* bt, const search_key* start_skey, const search_key* end_skey, si_btree_reduce_fn cb, void* udata);

static inline uint8_t
get_keyd_stub(const cf_digest* keyd)
{
	return (keyd->digest[1] & 0xF0) | (keyd->digest[2] >> 4);
}
