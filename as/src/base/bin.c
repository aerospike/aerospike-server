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

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/storage.h"

//==========================================================
// Public API.
//

// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins and rd->n_bins!
int
as_storage_rd_lazy_load_bins(as_storage_rd* rd, as_bin* stack_bins)
{
	rd->bins = stack_bins;
	rd->n_bins = 0;
	rd->check_flat = true;

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		// Calls ssd_read_record, which sets rd->flat_bins and rd->flat_n_bins.
		if (! as_storage_record_load_key(rd)) {
			return -1;
		}
	}

	return 0;
}

// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins and rd->n_bins!
int
as_storage_rd_load_bins(as_storage_rd* rd, as_bin* stack_bins)
{
	rd->bins = stack_bins;
	rd->n_bins = 0;
	rd->check_flat = false;

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_bins(rd); // sets rd->n_bins
	}

	return 0;
}

as_bin*
as_bin_get(as_storage_rd* rd, const char* name)
{
	return as_bin_get_w_len(rd, (const uint8_t*)name, strlen(name));
}

// Assumes bin name has been checked!
as_bin*
as_bin_get_w_len(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		__builtin_prefetch(rd->bins[i + 1].name, 0, 0);

		if (rd->bins[i].name[len] == '\0' &&
				memcmp(rd->bins[i].name, name, len) == 0) {
			return &rd->bins[i];
		}
	}

	if (! rd->check_flat) {
		return NULL;
	}

	const uint8_t* at = rd->flat_bins;
	uint16_t bi = 0;

	while (bi < rd->flat_n_bins) {
		uint8_t flat_name_len = *at++;

		if ((flat_name_len == len) && memcmp(at, name, len) == 0) {
			break;
		}

		if ((at = as_flat_skip_bin_data(at + flat_name_len, rd->flat_end)) ==
				NULL) {
			break;
		}

		bi++;
	}

	if (bi == rd->flat_n_bins || at == NULL) {
		return NULL;
	}

	as_bin* b = &rd->bins[rd->n_bins];

	as_bin_set_empty(b);
	b->particle = NULL;

	as_bin_clear_meta(b);

	if (! as_flat_unpack_bin_data(b, at + len, rd->flat_end)) {
		return NULL;
	}

	memcpy(b->name, name, len);
	b->name[len] = '\0';
	rd->n_bins++;

	return b;
}

as_bin*
as_bin_get_live(as_storage_rd* rd, const char* name)
{
	return as_bin_get_live_w_len(rd, (const uint8_t*)name, strlen(name));
}

// Assumes bin name has been checked!
as_bin*
as_bin_get_live_w_len(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	as_bin* b = as_bin_get_w_len(rd, name, len);
	return b != NULL && as_bin_is_live(b) ? b : NULL;
}

as_bin*
as_bin_get_or_create(as_storage_rd* rd, const char* name)
{
	return as_bin_get_or_create_w_len(rd, (const uint8_t*)name, strlen(name));
}

// Assumes bin name has been checked!
as_bin*
as_bin_get_or_create_w_len(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	as_bin* b = as_bin_get_w_len(rd, name, len);

	if (b != NULL) {
		as_bin_clear_meta(b);
		return b;
	}

	b = &rd->bins[rd->n_bins];

	as_bin_set_empty(b);
	b->particle = NULL;

	memcpy(b->name, name, len);
	b->name[len] = '\0';

	as_bin_clear_meta(b);

	rd->n_bins++;

	return b;
}

void
as_bin_delete(as_storage_rd* rd, const char* name)
{
	as_bin_delete_w_len(rd, (const uint8_t*)name, strlen(name));
}

// Assumes bin name has been checked!
void
as_bin_delete_w_len(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	as_bin* b = as_bin_get_w_len(rd, name, len);

	if (b) {
		as_bin_remove(rd, b - rd->bins);
	}
}
