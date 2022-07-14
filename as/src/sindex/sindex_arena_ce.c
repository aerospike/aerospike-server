/*
 * sindex_arena_ce.c
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

#include "citrusleaf/alloc.h"

#include "log.h"

#include "warnings.h"


//==========================================================
// Private API - for enterprise separation only.
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

void
si_arena_reset(as_sindex_arena* arena)
{
	for (uint32_t i = 0; i < arena->n_stages; i++) {
		cf_free(arena->stages[i]);
	}

	arena->free_h = 0;

	// Skip 0:0 so null handle is never used. TODO - bother?
	arena->at_stage_id = 0;
	arena->at_ele_id = 1;

	arena->n_stages = 0;
	memset(arena->stages, 0, sizeof(arena->stages));
}
