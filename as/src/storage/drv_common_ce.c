/*
 * drv_common_ce.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
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

#include "storage/drv_common.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "arenax.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/set_index.h"
#include "storage/flat.h"
#include "transaction/rw_utils.h"


//==========================================================
// Public API.
//

void
drv_adjust_sc_version_flags(as_namespace* ns, drv_pmeta* pmeta,
		bool wiped_drives, bool dirty)
{
	// Nothing to do - relevant for enterprise version only.
}

void
drv_mrt_create_cold_start_hash(as_namespace* ns)
{
}

// Might be better in a storage_ce.c if we ever do that.
bool
drv_load_needs_2nd_pass(const as_namespace* ns)
{
	return false;
}

bool
drv_mrt_2nd_pass_load_ticker(const as_namespace* ns, const char* pcts)
{
	return false;
}

void
drv_cold_start_remove_from_set_index(as_namespace* ns, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_set_index_delete_live(ns, tree, r_ref->r, r_ref->r_h);
}

void
drv_cold_start_record_create(as_namespace* ns, const as_flat_record* flat,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_set_index_insert(ns, tree, as_index_get_set_id(r_ref->r), r_ref->r_h);
}

bool
drv_cold_start_sweeps_done(as_namespace* ns)
{
	return true;
}

as_record*
drv_current_record(as_namespace* ns, as_record* r, int file_id,
		uint64_t rblock_id)
{
	if (r->file_id == file_id && r->rblock_id == rblock_id) {
		return r;
	}

	return NULL;
}

uint32_t
drv_max_record_size(const as_namespace* ns, const as_record* r)
{
	return ns->max_record_size;
}
