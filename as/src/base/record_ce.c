/*
 * record_ce.c
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/set_index.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


//==========================================================
// Public API.
//

uint32_t
clock_skew_stop_writes_sec(void)
{
	return 0;
}

bool
as_record_handle_clock_skew(as_namespace* ns, uint64_t skew_ms)
{
	return false;
}

uint16_t
plain_generation(uint16_t regime_generation, const as_namespace* ns)
{
	return regime_generation;
}

void
as_record_set_lut(as_record *r, uint32_t regime, uint64_t now_ms,
		const as_namespace* ns)
{
	// Note - last-update-time is not allowed to go backwards!
	if (r->last_update_time < now_ms) {
		r->last_update_time = now_ms;
	}
}

void
as_record_increment_generation(as_record *r, const as_namespace* ns)
{
	// The generation might wrap - 0 is reserved as "uninitialized".
	if (++r->generation == 0) {
		r->generation = 1;
	}
}

bool
as_record_is_binless(const as_record* r)
{
	return false;
}

bool
as_record_is_live(const as_record* r)
{
	return true;
}

int
as_record_fix_setless_tombstone(as_record* r, as_namespace* ns,
		const char* set_name, uint32_t len, bool apply_count_limit)
{
	cf_crash(AS_RECORD, "CE code called as_record_fix_setless_tombstone()");

	return AS_OK;
}

void
as_record_drop_stats(as_record* r, as_namespace* ns)
{
	as_namespace_release_set_id(ns, as_index_get_set_id(r));

	as_decr_uint64(&ns->n_objects);
}

void
as_record_transition_stats(as_record* r, as_namespace* ns,
		const as_record* old_r)
{
}

void
as_record_transition_set_index(as_index_tree* tree, as_index_ref* r_ref,
		as_namespace* ns, uint16_t n_bins, const as_record* old_r)
{
	as_record* r = r_ref->r;

	bool is_delete = n_bins == 0;
	bool inserted = old_r == NULL || old_r->generation == 0;

	if (is_delete) {
		as_set_index_delete(ns, tree, as_index_get_set_id(r), r_ref->r_h);
	}
	else if (inserted) {
		as_set_index_insert(ns, tree, as_index_get_set_id(r), r_ref->r_h);
	}
}


//==========================================================
// Private API - for enterprise separation only.
//

int
record_resolve_conflict_cp(uint16_t left_gen, uint64_t left_lut,
		uint16_t right_gen, uint64_t right_lut)
{
	cf_crash(AS_RECORD, "CE code called record_resolve_conflict_cp()");

	return 0;
}

void
replace_index_metadata(const as_remote_record *rr, as_record *r)
{
	r->generation = (uint16_t)rr->generation;
	r->void_time = trim_void_time(rr->void_time);
	r->last_update_time = rr->last_update_time;
}
