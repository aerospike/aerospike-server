/*
 * sindex_arena.c
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

//==========================================================
// Includes.
//

#include "sindex/sindex_arena.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "log.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

// TODO - will promote for EE.
typedef struct si_arena_free_ele_s {
	uint64_t magic;
	si_arena_handle next_h;
} si_arena_free_ele;

#define SI_FREE_MAGIC 0xf7f7fefefefef7f7UL


//==========================================================
// Forward declarations.
//

// TODO - will split for EE.
void si_arena_add_stage(as_sindex_arena* arena);


//==========================================================
// Inlines & macros.
//

// TODO - will promote for EE.
static inline void
si_arena_set_handle(si_arena_handle* h, uint32_t stage_id, uint32_t ele_id)
{
	*h = (stage_id << SI_ELE_ID_N_BITS) | ele_id;
}


//==========================================================
// Public API.
//

void
as_sindex_arena_init(as_sindex_arena* arena, uint32_t ele_sz, size_t stage_sz)
{
	arena->ele_sz = ele_sz;
	arena->stage_capacity = (uint32_t)(stage_sz / ele_sz);
	arena->stage_sz = stage_sz;

	arena->free_h = 0;
	arena->n_used_eles = 0;

	// Skip 0:0 so null handle is never used. TODO - bother?
	arena->at_stage_id = 0;
	arena->at_ele_id = 1;

	cf_mutex_init(&arena->lock);

	arena->n_stages = 0;
	memset(arena->stages, 0, sizeof(arena->stages));
}

si_arena_handle
as_sindex_arena_alloc(as_sindex_arena* arena)
{
	cf_mutex_lock(&arena->lock);

	si_arena_handle h;

	// Check free list first.
	if (arena->free_h != 0) {
		h = arena->free_h;

		si_arena_free_ele* free_ele = as_sindex_arena_resolve(arena, h);

		arena->free_h = free_ele->next_h;
	}
	// Otherwise keep end-allocating.
	else {
		// Add first stage lazily.
		if (arena->n_stages == 0) {
			si_arena_add_stage(arena);

			// Clear the null element. TODO - bother?
			memset(as_sindex_arena_resolve(arena, 0), 0, arena->ele_sz);
		}

		if (arena->at_ele_id >= arena->stage_capacity) {
			si_arena_add_stage(arena);
			arena->at_stage_id++;
			arena->at_ele_id = 0;
		}

		si_arena_set_handle(&h, arena->at_stage_id, arena->at_ele_id);

		arena->at_ele_id++;
	}

	arena->n_used_eles++;

	cf_mutex_unlock(&arena->lock);

	return h;
}

void
as_sindex_arena_free(as_sindex_arena* arena, si_arena_handle h)
{
	si_arena_free_ele* free_ele = as_sindex_arena_resolve(arena, h);

	cf_mutex_lock(&arena->lock);

	free_ele->magic = SI_FREE_MAGIC;
	free_ele->next_h = arena->free_h;
	arena->free_h = h;

	arena->n_used_eles--;

	cf_mutex_unlock(&arena->lock);
}


//==========================================================
// Local helpers.
//

// TODO - assumes we can't fail - ok?
void
si_arena_add_stage(as_sindex_arena* arena)
{
	if (arena->n_stages >= SI_ARENA_MAX_STAGES) {
		cf_crash(AS_SINDEX, "can't allocate more than %u arena stages",
				SI_ARENA_MAX_STAGES);
	}

	arena->stages[arena->n_stages++] = cf_malloc(arena->stage_sz);
}


