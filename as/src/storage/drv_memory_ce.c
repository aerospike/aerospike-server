/*
 * drv_memory_ce.c
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

#include <stdbool.h>
#include <stdint.h>

#include "base/datamodel.h"
#include "base/proto.h"
#include "fabric/partition.h"
#include "storage/flat.h"
#include "storage/storage.h"


void
as_storage_start_tomb_raider_memory(as_namespace* ns)
{
	// Tomb raider is for enterprise version only.
}

int
as_storage_record_write_memory(as_storage_rd* rd)
{
	bool is_master = rd->pickle == NULL;

	// Make a pickle if needed. (No pickle needed for drop.)
	if (rd->n_bins != 0 && rd->keep_pickle) {
		as_flat_pickle_record(rd);
	}

	as_namespace* ns = rd->ns;

	if (is_master && ns->max_record_size != 0) {
		uint32_t flat_sz = rd->pickle_sz == 0 ?
				as_flat_record_size(rd) : rd->pickle_sz;

		if (flat_sz > ns->max_record_size) {
			return -AS_ERR_RECORD_TOO_BIG;
		}
	}

	return 0;
}

void
as_storage_load_pmeta_memory(as_namespace *ns, as_partition *p)
{
}
