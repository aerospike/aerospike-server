/*
 * arenax.c
 *
 * Copyright (C) 2012-2023 Aerospike, Inc.
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

#include "arenax.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "log.h"
#include "xmem.h"

//==========================================================
// Typedefs & constants.
//

// Must be in-sync with cf_arenax_err:
const char* ARENAX_ERR_STRINGS[] = { "ok", "bad parameter",
	"error creating stage", "error attaching stage", "error detaching stage",
	"unknown error" };

//==========================================================
// Public API.
//

// Convert cf_arenax_err to meaningful string.
const char*
cf_arenax_errstr(cf_arenax_err err)
{
	if (err < 0 || err > CF_ARENAX_ERR_UNKNOWN) {
		err = CF_ARENAX_ERR_UNKNOWN;
	}

	return ARENAX_ERR_STRINGS[err];
}

// Create a cf_arenax object in persistent memory. Also create and attach the
// first arena stage in persistent memory.
void
cf_arenax_init(cf_arenax* arena, cf_xmem_type xmem_type,
		const void* xmem_type_cfg, key_t key_base, uint32_t element_size,
		uint32_t chunk_count, size_t stage_size)
{
	arena->xmem_type = xmem_type;
	arena->xmem_type_cfg = xmem_type_cfg;
	arena->key_base = key_base;
	arena->element_size = element_size;
	arena->chunk_count = chunk_count;
	arena->stage_capacity = (uint32_t)(stage_size / element_size);
	arena->unused_1 = 0;
	arena->unused_2 = 0;
	arena->stage_size = stage_size;

	arena->stash = cf_malloc(CF_ARENAX_N_STASHES * sizeof(cf_arenax_stash));

	for (uint32_t i = 0; i < CF_ARENAX_N_STASHES; i++) {
		cf_arenax_stash* stash = &arena->stash[i];

		cf_mutex_init(&stash->lock);
		stash->free_h = 0;
	}

	if (chunk_count == 1) {
		arena->pool_len = 0;
		arena->pool_buf = NULL;
	}
	else {
		arena->pool_len = arena->stage_capacity;
		arena->pool_buf = cf_malloc(arena->pool_len * sizeof(cf_arenax_chunk));
	}

	arena->pool_i = 0;
	arena->alloc_sz = 0; // for flash index stats only

	// Skip 0:0 so null handle is never used.
	arena->at_stage_id = 0;
	arena->at_element_id = arena->chunk_count;

	cf_mutex_init(&arena->lock);

	arena->stage_count = 0;
	memset(arena->stages, 0, sizeof(arena->stages));

	// Add first stage.
	if (cf_arenax_add_stage(arena) != CF_ARENAX_OK) {
		cf_crash(CF_ARENAX, "failed to add first stage");
	}

	// Clear the null element - allocation bypasses it, but it may be read.
	memset(cf_arenax_resolve(arena, 0), 0, element_size * chunk_count);
}

// Allocate an element within the arena.
cf_arenax_handle
cf_arenax_alloc(cf_arenax* arena, cf_arenax_puddle* puddle)
{
	if (puddle != NULL) {
		return cf_arenax_alloc_chunked(arena, puddle);
	}

	static uint32_t rr = 0;
	cf_arenax_stash* stash = &arena->stash[rr++ % CF_ARENAX_N_STASHES];

	cf_mutex_lock(&stash->lock);

	cf_arenax_handle h;

	// Check free list first.
	if (stash->free_h != 0) {
		h = stash->free_h;

		free_element* p_free_element = cf_arenax_resolve(arena, h);

		stash->free_h = p_free_element->next_h;
	}
	// Otherwise keep end-allocating.
	else {
		cf_mutex_lock(&arena->lock);

		if (arena->at_element_id >= arena->stage_capacity) {
			if (cf_arenax_add_stage(arena) != CF_ARENAX_OK) {
				cf_mutex_unlock(&arena->lock);
				cf_mutex_unlock(&stash->lock);
				return 0;
			}

			arena->at_stage_id++;
			arena->at_element_id = 0;
		}

		uint32_t start = arena->at_element_id;
		uint32_t end = (start + CF_ARENAX_STASH_LEN) & -CF_ARENAX_STASH_LEN;
		uint32_t stage_id = arena->at_stage_id;

		arena->at_element_id = end;

		cf_mutex_unlock(&arena->lock);

		cf_assert(end <= arena->stage_capacity, CF_ARENAX, "bad stash length");

		for (uint32_t i = end - 1; i > start; i--) {
			cf_arenax_set_handle(&h, stage_id, i);

			free_element* p_free_element = cf_arenax_resolve(arena, h);

			p_free_element->magic = FREE_MAGIC;
			p_free_element->next_h = stash->free_h;
			stash->free_h = h;
		}

		cf_arenax_set_handle(&h, stage_id, start);
	}

	cf_mutex_unlock(&stash->lock);

	return h;
}

// Free an element.
void
cf_arenax_free(cf_arenax* arena, cf_arenax_handle h, cf_arenax_puddle* puddle)
{
	if (puddle != NULL) {
		cf_arenax_free_chunked(arena, h, puddle);
		return;
	}

	uint32_t stage_id;
	uint32_t element_id;

	cf_arenax_expand_handle(&stage_id, &element_id, h);

	cf_arenax_stash* stash = &arena->stash[element_id % CF_ARENAX_N_STASHES];
	free_element* p_free_element = cf_arenax_resolve(arena, h);

	cf_mutex_lock(&stash->lock);

	// OLD PARANOIA.
	cf_assert(p_free_element->magic != FREE_MAGIC, CF_ARENAX,
			"double freed arena element");

	p_free_element->magic = FREE_MAGIC;
	p_free_element->next_h = stash->free_h;
	stash->free_h = h;

	cf_mutex_unlock(&stash->lock);
}

bool
cf_arenax_is_stage_address(cf_arenax* arena, const void* address)
{
	bool found = false;

	cf_mutex_lock(&arena->lock);

	for (uint32_t i = 0; i < arena->stage_count; i++) {
		if (arena->stages[i] == address) {
			found = true;
			break;
		}
	}

	cf_mutex_unlock(&arena->lock);

	return found;
}
