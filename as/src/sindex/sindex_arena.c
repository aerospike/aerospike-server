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
#include <sys/types.h>

#include "cf_mutex.h"
#include "log.h"
#include "xmem.h"

#include "warnings.h"


//==========================================================
// Public API.
//

void
as_sindex_arena_init(as_sindex_arena* arena, cf_xmem_type xmem_type,
		const void* xmem_type_cfg, key_t key_base, uint32_t ele_sz,
		size_t stage_sz)
{
	arena->xmem_type = xmem_type;
	arena->xmem_type_cfg = xmem_type_cfg;

	arena->key_base = key_base;

	arena->ele_sz = ele_sz;
	arena->stage_capacity = (uint32_t)(stage_sz / ele_sz);
	arena->stage_sz = stage_sz;

	arena->free_h = 0;
	arena->n_used_eles = 0;

	// Skip 0:0 so null handle is never used. Not necessary but seems nice.
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

			// Clear the null element. Not necessary but seems nice.
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

	if (--arena->n_used_eles == 0) {
		cf_info(AS_SINDEX, "removing %u empty arena stage(s)", arena->n_stages);

		si_arena_reset(arena);
	}

	cf_mutex_unlock(&arena->lock);
}
