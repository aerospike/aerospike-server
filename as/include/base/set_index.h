/*
 * set_index.h
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

#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "cf_mutex.h"

#include "base/index.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_index_tree_s;
struct as_namespace_s;
struct as_set_s;


//==========================================================
// Typedefs & constants.
//

//--------------------------------------
// uarena header.
//

typedef uint32_t uarena_handle;

typedef struct uarena_s {
	cf_mutex lock;
	uarena_handle free_h;
	uint16_t at_stage_id;
	uint16_t at_ele_id;
	uint32_t n_stages;
	uint8_t** stages;
} uarena;

#define ELE_ID_N_BITS 8
#define ELE_ID_MASK ((1 << ELE_ID_N_BITS) - 1) // 0xFF
#define ELE_SIZE 16

//
// END uarena header.
//--------------------------------------

// Minimum set sprigs - we're sharing the primary index tree's sprig locks.
#define N_SET_SPRIGS NUM_LOCK_PAIRS

typedef struct as_set_index_tree_s {
	uarena ua;
	uarena_handle roots[N_SET_SPRIGS];
} as_set_index_tree;

typedef struct index_ele_s {
	// Key - primary index arena handle.
	uint64_t key_r_h: 40;

	uint64_t keyd_stub: 23;
	uint64_t color: 1;

	// Children - micro-arena handles.
	uarena_handle left_h;
	uarena_handle right_h;
} __attribute__ ((__packed__)) index_ele;

COMPILER_ASSERT(sizeof(index_ele) == ELE_SIZE);

typedef struct ssprig_info_s {
	cf_arenax* arena;
	uarena* ua;
	uarena_handle* root;
	uint32_t keyd_stub;
	const cf_digest* keyd;
} ssprig_info;

typedef struct ssprig_reduce_info_s {
	ssprig_info _ssi; // must be first
	as_index_value_destructor destructor;
	void* destructor_udata;
	cf_mutex* olock;
} ssprig_reduce_info;


//==========================================================
// Public API.
//

// Startup.
void as_set_index_init(void);

// Set-index tree lifecycle.
void as_set_index_create_all(struct as_namespace_s* ns, struct as_index_tree_s* tree);
void as_set_index_destroy_all(struct as_index_tree_s* tree);
void as_set_index_tree_create(struct as_index_tree_s* tree, uint16_t set_id);
void as_set_index_tree_destroy(struct as_index_tree_s* tree, uint16_t set_id);
void as_set_index_balance_lock(void);
void as_set_index_balance_unlock(void);

// Transactions.
void as_set_index_insert(struct as_namespace_s* ns, struct as_index_tree_s* tree, uint16_t set_id, uint64_t r_h);
void as_set_index_delete(struct as_namespace_s* ns, struct as_index_tree_s* tree, uint16_t set_id, uint64_t r_h);
void as_set_index_delete_live(struct as_namespace_s* ns, struct as_index_tree_s* tree, struct as_index_s* r, uint64_t r_h);
bool as_set_index_reduce(struct as_namespace_s* ns, struct as_index_tree_s* tree, uint16_t set_id, cf_digest* keyd, as_index_reduce_fn cb, void* udata);

// Info & stats.
void as_set_index_enable(struct as_namespace_s* ns, struct as_set_s* p_set, uint16_t set_id);
void as_set_index_disable(struct as_namespace_s* ns, struct as_set_s* p_set, uint16_t set_id);
uint64_t as_set_index_used_bytes(const struct as_namespace_s* ns);


//==========================================================
// Private API - enterprise separation only.
//

bool ssprig_insert(ssprig_info* ssi, uint64_t key_r_h);
bool ssprig_reduce_no_rc(struct as_index_tree_s* tree, ssprig_reduce_info* ssri, as_index_reduce_fn cb, void* udata);

static inline int
ssprig_ele_cmp(const ssprig_info* ssi, const index_ele* e)
{
	if (ssi->keyd_stub > e->keyd_stub) {
		return 1;
	}

	if (ssi->keyd_stub < e->keyd_stub) {
		return -1;
	}
	// else - equal - need whole digests.

	as_index* r = cf_arenax_resolve(ssi->arena, e->key_r_h);

	return cf_digest_compare(ssi->keyd, &r->keyd);
}

//--------------------------------------
// uarena API.
//

static inline void*
uarena_resolve(uarena* ua, uarena_handle h)
{
	return ua->stages[h >> ELE_ID_N_BITS] + ((h & ELE_ID_MASK) * ELE_SIZE);
}

#define UA_RESOLVE(_h) ((index_ele*)uarena_resolve(ssi->ua, _h))
