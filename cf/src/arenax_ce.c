/*
 * arenax_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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
#include <stdint.h>
#include "citrusleaf/alloc.h"
#include "fault.h"


//==========================================================
// Public API.
//

bool
cf_arenax_want_prefetch(cf_arenax* arena)
{
	return false;
}

void
cf_arenax_reclaim(cf_arenax* arena, cf_arenax_puddle* puddles,
		uint32_t n_puddles)
{
}


//==========================================================
// Private API - for enterprise separation only.
//

// Allocate an arena stage, and store its pointer in the stages array.
cf_arenax_err
cf_arenax_add_stage(cf_arenax* arena)
{
	if (arena->stage_count >= arena->max_stages) {
		cf_ticker_warning(CF_ARENAX, "can't allocate more than %u arena stages",
				arena->max_stages);
		return CF_ARENAX_ERR_STAGE_CREATE;
	}

	uint8_t* p_stage = (uint8_t*)cf_try_malloc(arena->stage_size);

	if (! p_stage) {
		cf_ticker_warning(CF_ARENAX,
				"could not allocate %zu-byte arena stage %u",
				arena->stage_size, arena->stage_count);
		return CF_ARENAX_ERR_STAGE_CREATE;
	}

	arena->stages[arena->stage_count++] = p_stage;

	return CF_ARENAX_OK;
}

cf_arenax_handle
cf_arenax_alloc_chunked(cf_arenax* arena, cf_arenax_puddle* puddle)
{
	cf_crash(AS_INDEX, "CE code called cf_arenax_alloc_chunked()");
	return 0;
}

void
cf_arenax_free_chunked(cf_arenax* arena, cf_arenax_handle h,
		cf_arenax_puddle* puddle)
{
	cf_crash(AS_INDEX, "CE code called cf_arenax_free_chunked()");
}
