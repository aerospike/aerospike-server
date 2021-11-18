/*
 * btree.h
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

#include <stdbool.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

typedef int32_t (*as_btree_key_comp_fn)(const void* key_1, const void* key_2);
typedef bool (*as_btree_reduce_fn)(const void* key, void* value, void* udata);

#define N_KEYS_SZ 10

typedef struct as_btree_node_s {
	uint16_t n_keys : N_KEYS_SZ;
	uint16_t leaf : 1;
	uint16_t : 5;
} as_btree_node;

typedef struct as_btree_s {
	as_btree_key_comp_fn key_comp;
	uint32_t key_sz;
	uint32_t value_sz;
	uint32_t min_degree;
	uint32_t max_degree;
	uint32_t keys_off;
	uint32_t values_off;
	uint32_t children_off;
	uint32_t inner_sz;
	uint32_t leaf_sz;
	as_btree_node* root;
	uint64_t n_nodes;
	uint64_t n_keys;
	uint64_t size;
	uint64_t extra_size; // tracks size of second-level trees
} as_btree;


//==========================================================
// Public API.
//

as_btree* as_btree_create(as_btree_key_comp_fn key_comp, uint32_t key_sz, uint32_t value_sz, uint32_t order);
void as_btree_destroy(as_btree* bt);

bool as_btree_put(as_btree* bt, const void* key, const void* value);
bool as_btree_get(const as_btree* bt, const void* key, void* value);
bool as_btree_delete(as_btree* bt, const void* key);
void as_btree_reduce(as_btree* bt, const void* start_key, const void* end_key, as_btree_reduce_fn cb, void* udata);
