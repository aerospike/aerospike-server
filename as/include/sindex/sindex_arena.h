/*
 * sindex_arena.h
 *
 * Copyright (C) 2022 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "cf_mutex.h"


//==========================================================
// Typedefs & constants.
//

#define SI_ARENA_MAX_STAGES 2048

#define SI_ARENA_MIN_STAGE_SIZE (128L * 1024L * 1024L) // 128M
#define SI_ARENA_MAX_STAGE_SIZE (4L * 1024L * 1024L * 1024L) // 4G

// Element is indexed by 20 bits.
#define SI_ELE_ID_N_BITS 20
#define SI_ELE_ID_MASK ((1 << SI_ELE_ID_N_BITS) - 1) // 0xFffff

typedef uint32_t si_arena_handle;

typedef struct as_sindex_arena_s {
	key_t key_base; // enterprise only - to create stage (xmem) blocks

	// Configuration (passed in constructors).
	uint32_t ele_sz;
	uint32_t stage_capacity; // derived
	size_t stage_sz;

	// Free/used element tracking.
	si_arena_handle free_h;
	uint64_t n_used_eles;

	// Where to end-allocate.
	uint32_t at_stage_id;
	uint32_t at_ele_id;

	// Thread safety.
	cf_mutex lock;

	// Current stages.
	uint32_t n_stages;
	uint8_t* stages[SI_ARENA_MAX_STAGES];

} as_sindex_arena;

typedef struct si_arena_free_ele_s {
	uint64_t magic;
	si_arena_handle next_h;
} si_arena_free_ele;

#define SI_FREE_MAGIC 0xf7f7fefefefef7f7UL


//==========================================================
// Public API.
//

void as_sindex_arena_init(as_sindex_arena* arena, key_t key_base, uint32_t ele_sz, size_t stage_sz);

si_arena_handle as_sindex_arena_alloc(as_sindex_arena* arena);
void as_sindex_arena_free(as_sindex_arena* arena, si_arena_handle h);

static inline void*
as_sindex_arena_resolve(as_sindex_arena* arena, si_arena_handle h)
{
	return arena->stages[h >> SI_ELE_ID_N_BITS] +
			((h & SI_ELE_ID_MASK) * arena->ele_sz);
}


//==========================================================
// Private API - for enterprise separation only.
//

void si_arena_add_stage(as_sindex_arena* arena);
void si_arena_reset(as_sindex_arena* arena);

static inline void
si_arena_set_handle(si_arena_handle* h, uint32_t stage_id, uint32_t ele_id)
{
	*h = (stage_id << SI_ELE_ID_N_BITS) | ele_id;
}
