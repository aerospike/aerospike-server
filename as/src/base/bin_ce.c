/*
 * bin_ce.c
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "log.h"

#include "base/datamodel.h"
#include "storage/storage.h"


//==========================================================
// Public API.
//

uint8_t
as_bin_get_particle_type(const as_bin* b)
{
	switch (b->state) {
	case AS_BIN_STATE_INUSE_INTEGER:
		return AS_PARTICLE_TYPE_INTEGER;
	case AS_BIN_STATE_INUSE_FLOAT:
		return AS_PARTICLE_TYPE_FLOAT;
	case AS_BIN_STATE_INUSE_BOOL:
		return AS_PARTICLE_TYPE_BOOL;
	case AS_BIN_STATE_INUSE_OTHER:
		return b->particle->type;
	case AS_BIN_STATE_UNUSED:
	default:
		return AS_PARTICLE_TYPE_NULL;
	}
}

bool
as_bin_particle_is_tombstone(as_particle_type type)
{
	return false;
}

bool
as_bin_is_tombstone(const as_bin* b)
{
	// TODO - for development only?
	cf_assert(b->state != AS_BIN_STATE_UNUSED, AS_BIN, "unexpected empty bin");

	return false;
}

bool
as_bin_is_live(const as_bin* b)
{
	return b->state != AS_BIN_STATE_UNUSED;
}

void
as_bin_set_tombstone(as_bin* b)
{
	cf_crash(AS_BIN, "unreachable function for CE");
}

bool
as_bin_empty_if_all_tombstones(as_storage_rd* rd, bool is_dd)
{
	return rd->n_bins == 0;
}

void
as_bin_clear_meta(as_bin* b)
{
	b->unused_flags = 0;
	b->lut = 0;
}


//==========================================================
// Special API for downgrades.
//

int
as_bin_downgrade_pickle(as_storage_rd* rd)
{
	return 0;
}
