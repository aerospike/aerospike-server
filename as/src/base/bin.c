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
as_storage_rd_load_bins(as_storage_rd* rd, as_bin* stack_bins)
{
	rd->bins = stack_bins;
	rd->n_bins = 0;

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
		as_bin* b = &rd->bins[i];

		if (memcmp(b->name, name, len) == 0 && b->name[len] == '\0') {
			return b;
		}
	}

	return NULL;
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
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if (memcmp(b->name, name, len) == 0 && b->name[len] == '\0') {
			return as_bin_is_live(b) ? b : NULL;
		}
	}

	return NULL;
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
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if (memcmp(b->name, name, len) == 0 && b->name[len] == '\0') {
			as_bin_clear_meta(b);
			return b;
		}
	}

	as_bin* b = &rd->bins[rd->n_bins];

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
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if (memcmp(b->name, name, len) == 0 && b->name[len] == '\0') {
			as_bin_remove(rd, i);
			return;
		}
	}
}
