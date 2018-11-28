/*
 * bin.c
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"


//==========================================================
// Inlines & macros.
//

static inline void
as_bin_init_nameless(as_bin *b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
	b->particle = NULL;
	// Don't touch b->unused - like b->id, it's past the end of its enclosing
	// as_index if single-bin, data-in-memory.
}

static inline as_bin_space *
safe_bin_space(const as_record *r)
{
	return r->dim ? as_index_get_bin_space(r) : NULL;
}

static inline uint16_t
safe_n_bins(const as_record *r)
{
	as_bin_space* bin_space = safe_bin_space(r);

	return bin_space ? bin_space->n_bins : 0;
}

static inline as_bin *
safe_bins(const as_record *r)
{
	as_bin_space* bin_space = safe_bin_space(r);

	return bin_space ? bin_space->bins : NULL;
}


//==========================================================
// Public API.
//

// Caller-beware, name cannot be null, must be null-terminated.
int16_t
as_bin_get_id(as_namespace *ns, const char *name)
{
	cf_assert(! ns->single_bin, AS_BIN, "unexpected single-bin call");

	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (uint16_t)idx;
	}

	return -1;
}


bool
as_bin_get_or_assign_id_w_len(as_namespace *ns, const char *name, size_t len,
		uint16_t *id)
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

	if (! (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS)) {
		CF_ZSTR_DEFINE(zname, AS_BIN_NAME_MAX_SZ, name, len);
		cf_warning(AS_BIN, "adding bin name %s, vmap err %d", zname, result);
		return false;
	}

	*id = (uint16_t)idx;

	return true;
}


const char *
as_bin_get_name_from_id(as_namespace *ns, uint16_t id)
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


bool
as_bin_name_within_quota(as_namespace *ns, const char *name)
{
	// Won't exceed quota if single-bin or currently below quota.
	if (ns->single_bin ||
			cf_vmapx_count(ns->p_bin_name_vmap) < BIN_NAMES_QUOTA) {
		return true;
	}

	// Won't exceed quota if name is found (and so would NOT be added to vmap).
	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, NULL) == CF_VMAPX_OK) {
		return true;
	}

	cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s",
			ns->name, name);

	return false;
}


void
as_bin_copy(as_namespace *ns, as_bin *to, const as_bin *from)
{
	if (ns->single_bin) {
		as_single_bin_copy(to, from);
	}
	else {
		*to = *from;
	}
}


// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->n_bins!
int
as_storage_rd_load_n_bins(as_storage_rd *rd)
{
	if (rd->ns->single_bin) {
		rd->n_bins = 1;
		return 0;
	}

	if (rd->ns->storage_data_in_memory) {
		rd->n_bins = safe_n_bins(rd->r);
		return 0;
	}

	rd->n_bins = 0;

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_n_bins(rd); // sets rd->n_bins
	}

	return 0;
}


// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins!
int
as_storage_rd_load_bins(as_storage_rd *rd, as_bin *stack_bins)
{
	if (rd->ns->storage_data_in_memory) {
		rd->bins = rd->ns->single_bin ? as_index_get_single_bin(rd->r) :
				safe_bins(rd->r);
		return 0;
	}

	// Data NOT in-memory.

	rd->bins = stack_bins;
	as_bin_set_all_empty(rd);

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_bins(rd);
	}

	return 0;
}


void
as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		bin_ptrs[i] = &rd->bins[i];
	}
}


as_bin *
as_bin_get_by_id(as_storage_rd *rd, uint32_t id)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}


as_bin *
as_bin_get(as_storage_rd *rd, const char *name)
{
	return as_bin_get_from_buf(rd, (const uint8_t *)name, strlen(name));
}


as_bin *
as_bin_get_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? rd->bins : NULL;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(rd->ns->p_bin_name_vmap, (const char *)name,
			len, &id) != CF_VMAPX_OK) {
		return NULL;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}


as_bin *
as_bin_create_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len,
		int *result)
{
	as_namespace *ns = rd->ns;

	if (ns->single_bin) {
		if (as_bin_inuse(rd->bins)) {
			cf_crash(AS_BIN, "single bin create found bin in use");
		}

		as_bin_init_nameless(rd->bins);

		return rd->bins;
	}

	// TODO - already handled (with generic warning) by vmap - remove check?
	if (len >= AS_BIN_NAME_MAX_SZ) {
		cf_warning(AS_BIN, "bin name too long (%lu)", len);

		if (result) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	as_bin *b = NULL;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			b = &rd->bins[i];
			break;
		}
	}

	cf_assert(b, AS_BIN, "ran out of allocated bins in rd");

	as_bin_init_nameless(b);

	if (! as_bin_get_or_assign_id_w_len(ns, (const char *)name, len, &b->id)) {
		if (result) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	return b;
}


as_bin *
as_bin_get_or_create(as_storage_rd *rd, const char *name)
{
	return as_bin_get_or_create_from_buf(rd, (const uint8_t *)name,
			strlen(name), NULL);
}


// Does not check bin name length.
// Checks bin name quota - use appropriately.
as_bin *
as_bin_get_or_create_from_buf(as_storage_rd *rd, const uint8_t *name,
		size_t len, int *result)
{
	as_namespace *ns = rd->ns;

	if (ns->single_bin) {
		if (! as_bin_inuse_has(rd)) {
			as_bin_init_nameless(rd->bins);
		}

		return rd->bins;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(ns->p_bin_name_vmap, (const char *)name, len,
			&id) == CF_VMAPX_OK) {
		for (uint16_t i = 0; i < rd->n_bins; i++) {
			as_bin *b = &rd->bins[i];

			if (! as_bin_inuse(b)) {
				as_bin_init_nameless(b);
				b->id = (uint16_t)id;
				return b;
			}

			if ((uint32_t)b->id == id) {
				return b;
			}
		}

		cf_crash(AS_BIN, "ran out of allocated bins in rd");
	}
	// else - bin name is new.

	if (cf_vmapx_count(ns->p_bin_name_vmap) >= BIN_NAMES_QUOTA) {
		CF_ZSTR_DEFINE(zname, AS_BIN_NAME_MAX_SZ, name, len);

		cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s",
				ns->name, zname);

		if (result) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	uint16_t i = as_bin_inuse_count(rd);

	cf_assert(i < rd->n_bins, AS_BIN, "ran out of allocated bins in rd");

	as_bin *b = &rd->bins[i];

	as_bin_init_nameless(b);

	if (! as_bin_get_or_assign_id_w_len(ns, (const char *)name, len, &b->id)) {
		if (result) {
			*result = AS_ERR_BIN_NAME;
		}

		return NULL;
	}

	return b;
}


int32_t
as_bin_get_index(as_storage_rd *rd, const char *name)
{
	return as_bin_get_index_from_buf(rd, (const uint8_t *)name, strlen(name));
}


int32_t
as_bin_get_index_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? 0 : -1;
	}

	uint32_t id;

	if (cf_vmapx_get_index_w_len(rd->ns->p_bin_name_vmap, (const char *)name,
			len, &id) != CF_VMAPX_OK) {
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return (int32_t)i;
		}
	}

	return -1;
}


void
as_bin_destroy(as_storage_rd *rd, uint16_t i)
{
	as_bin_particle_destroy(&rd->bins[i], rd->ns->storage_data_in_memory);
	as_bin_set_empty_shift(rd, i);
}


void
as_bin_allocate_bin_space(as_storage_rd *rd, int32_t delta)
{
	as_record *r = rd->r;

	if (rd->n_bins == 0) {
		rd->n_bins = (uint16_t)delta;

		size_t size = sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin));
		as_bin_space* bin_space = (as_bin_space*)cf_malloc_ns(size);

		rd->bins = bin_space->bins;
		as_bin_set_all_empty(rd);

		bin_space->n_bins = rd->n_bins;
		as_index_set_bin_space(r, bin_space);

		return;
	}
	// else - there were bins before.

	uint16_t new_n_bins = (uint16_t)((int32_t)rd->n_bins + delta);

	if (delta < 0) {
		as_record_destroy_bins_from(rd, new_n_bins);
	}

	uint16_t old_n_bins = rd->n_bins;

	rd->n_bins = new_n_bins;

	if (new_n_bins != 0) {
		size_t size = sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin));
		as_bin_space* bin_space = (as_bin_space*)
				cf_realloc_ns((void*)as_index_get_bin_space(r), size);

		rd->bins = bin_space->bins;

		if (delta > 0) {
			as_bin_set_empty_from(rd, old_n_bins);
		}

		bin_space->n_bins = rd->n_bins;
		as_index_set_bin_space(r, bin_space);
	}
	else {
		cf_free((void*)as_index_get_bin_space(r));
		as_index_set_bin_space(r, NULL);
		rd->bins = NULL;
	}
}
