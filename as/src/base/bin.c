/*
 * bin.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "log.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"


//==========================================================
// Inlines & macros.
//

static inline void
as_bin_init_nameless(as_bin* b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
	b->particle = NULL;
	// Don't touch b->unused - like b->id, it's past the end of its enclosing
	// as_index if single-bin, data-in-memory.
}

static inline as_bin_space*
safe_bin_space(const as_record* r)
{
	return r->dim ? as_index_get_bin_space(r) : NULL;
}

static inline uint16_t
safe_n_bins(const as_record* r)
{
	as_bin_space* bin_space = safe_bin_space(r);

	return bin_space ? bin_space->n_bins : 0;
}

static inline as_bin*
safe_bins(const as_record* r)
{
	as_bin_space* bin_space = safe_bin_space(r);

	return bin_space ? bin_space->bins : NULL;
}


//==========================================================
// Public API.
//

// Can't inline - compiler warns of as_bin members we don't want to initialize.
void
as_bin_copy(const as_namespace* ns, as_bin* to, const as_bin* from)
{
	if (ns->single_bin) {
		as_single_bin_copy(to, from);
	}
	else {
		*to = *from;
	}
}

int32_t
as_bin_get_id(const as_namespace* ns, const char* name)
{
	cf_assert(! ns->single_bin, AS_BIN, "unexpected single-bin call");

	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (int32_t)idx;
	}

	return -1;
}

bool
as_bin_get_or_assign_id_w_len(as_namespace* ns, const char* name, size_t len,
		uint16_t* id)
{
	// May later replace with assert if we never call with single-bin.
	if (ns->single_bin) {
		return true;
	}

	uint32_t idx;

	if (cf_vmapx_get_index_w_len(ns->p_bin_name_vmap, name, len, &idx) ==
			CF_VMAPX_OK) {
		*id = (uint16_t)idx;
		return true;
	}

	// TODO - add a check for legal bin name characters here.

	cf_vmapx_err result = cf_vmapx_put_unique_w_len(ns->p_bin_name_vmap, name,
			len, &idx);

	if (result == CF_VMAPX_ERR_FULL) {
		cf_warning(AS_BIN, "{%s} bin name quota full - can't add %.*s",
				ns->name, (uint32_t)len, name);
		return false;
	}

	if (! (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS)) {
		cf_warning(AS_BIN, "vmap err %d - can't add new bin name '%.*s'",
				result, (uint32_t)len, name);
		return false;
	}

	*id = (uint16_t)idx;

	return true;
}

const char*
as_bin_get_name_from_id(const as_namespace* ns, uint16_t id)
{
	cf_assert(! ns->single_bin, AS_BIN, "unexpected single-bin call");

	const char* name = NULL;

	if (cf_vmapx_get_by_index(ns->p_bin_name_vmap, id, (void**)&name) !=
			CF_VMAPX_OK) {
		// Should be impossible since id originates from vmap.
		cf_crash(AS_BIN, "no bin name for id %u", id);
	}

	return name;
}

// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins and rd->n_bins!
int
as_storage_rd_load_bins(as_storage_rd* rd, as_bin* stack_bins)
{
	as_namespace* ns = rd->ns;

	if (ns->storage_data_in_memory) {
		as_record* r = rd->r;

		if (ns->single_bin) {
			rd->bins = as_index_get_single_bin(r);
			rd->n_bins = as_bin_inuse(rd->bins) ? 1 : 0;
		}
		else {
			rd->bins = safe_bins(r);
			rd->n_bins = safe_n_bins(r);
		}

		return 0;
	}

	// Data NOT in-memory.

	rd->bins = stack_bins;
	rd->n_bins = 0;

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_bins(rd); // sets rd->n_bins
	}

	return 0;
}

as_bin*
as_bin_get_by_id(as_storage_rd* rd, uint32_t id)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}

as_bin*
as_bin_get(as_storage_rd* rd, const char* name)
{
	return as_bin_get_w_len(rd, (const uint8_t*)name, strlen(name));
}

as_bin*
as_bin_get_w_len(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	if (rd->ns->single_bin) {
		return rd->n_bins == 0 ? NULL : rd->bins;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(rd->ns->p_bin_name_vmap, (const char*)name,
			len, &id) != CF_VMAPX_OK) {
		return NULL;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}

as_bin*
as_bin_create_w_len(as_storage_rd* rd, const uint8_t* name, size_t len,
		int* result)
{
	as_namespace* ns = rd->ns;

	if (ns->single_bin) {
		cf_assert(rd->n_bins == 0, AS_BIN, "single-bin create found used bin");

		as_bin_init_nameless(rd->bins);
		rd->n_bins = 1;

		return rd->bins;
	}

	as_bin* b = &rd->bins[rd->n_bins];

	as_bin_init_nameless(b);

	if (! as_bin_get_or_assign_id_w_len(ns, (const char*)name, len, &b->id)) {
		if (result != NULL) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	rd->n_bins++;

	return b;
}

as_bin*
as_bin_get_or_create(as_storage_rd* rd, const char* name, int* result)
{
	return as_bin_get_or_create_w_len(rd, (const uint8_t*)name, strlen(name),
			result);
}

// Does not check bin name length.
// Checks bin name quota - use appropriately.
as_bin*
as_bin_get_or_create_w_len(as_storage_rd* rd, const uint8_t* name, size_t len,
		int* result)
{
	as_namespace* ns = rd->ns;

	if (ns->single_bin) {
		if (rd->n_bins == 0) {
			as_bin_init_nameless(rd->bins);
			rd->n_bins = 1;
		}

		return rd->bins;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(ns->p_bin_name_vmap, (const char*)name, len,
			&id) == CF_VMAPX_OK) {
		for (uint16_t i = 0; i < rd->n_bins; i++) {
			as_bin* b = &rd->bins[i];

			if ((uint32_t)b->id == id) {
				return b;
			}
		}

		as_bin* b = &rd->bins[rd->n_bins];

		as_bin_init_nameless(b);
		b->id = (uint16_t)id;

		rd->n_bins++;

		return b;
	}
	// else - bin name is new.

	as_bin* b = &rd->bins[rd->n_bins];

	as_bin_init_nameless(b);

	if (! as_bin_get_or_assign_id_w_len(ns, (const char*)name, len, &b->id)) {
		if (result != NULL) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	rd->n_bins++;

	return b;
}

bool
as_bin_pop(as_storage_rd* rd, const char* name, as_bin* bin)
{
	return as_bin_pop_w_len(rd, (const uint8_t*)name, strlen(name), bin);
}

bool
as_bin_pop_w_len(as_storage_rd* rd, const uint8_t* name, size_t len,
		as_bin* bin)
{
	if (rd->ns->single_bin) {
		if (rd->n_bins == 0) {
			return false;
		}

		as_single_bin_copy(bin, rd->bins);
		as_bin_set_empty_shift(rd, 0);

		// Note - for single-bin DIM as_storage_rd_load_bins() derives
		// rd->n_bins from bin (used) state - must clear deleted bin.
		if (rd->ns->storage_data_in_memory) {
			as_bin_set_empty(rd->bins);
		}

		return true;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(rd->ns->p_bin_name_vmap, (const char*)name,
			len, &id) != CF_VMAPX_OK) {
		return false;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if ((uint32_t)b->id == id) {
			*bin = *b;
			as_bin_set_empty_shift(rd, i);

			return true;
		}
	}

	return false;
}
