/*
 * drv_memory.c
 *
 * Copyright (C) 2009-2014 Aerospike, Inc.
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

#include "storage/storage.h"

#include <stdint.h>

#include "base/datamodel.h"
#include "base/truncate.h"


//==========================================================
// Public API - storage API implementation.
//

void
as_storage_namespace_init_memory(as_namespace *ns)
{
	as_truncate_done_startup(ns);
}

int
as_storage_namespace_destroy_memory(as_namespace *ns)
{
	return 0;
}

int
as_storage_stats_memory(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	if (available_pct) {
		*available_pct = 100;
	}

	if (used_disk_bytes) {
		*used_disk_bytes = 0;
	}

	return 0;
}
