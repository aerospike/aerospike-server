/*
 * partition_ce.c
 *
 * Copyright (C) 2017-2018 Aerospike, Inc.
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

#include "fabric/partition.h"

#include <stdbool.h>

#include "node.h"

#include "base/datamodel.h"
#include "base/proto.h"


//==========================================================
// Public API.
//

void
as_partition_isolate_version(const as_namespace* ns, as_partition* p)
{
	if (as_partition_version_has_data(&p->version)) {
		p->version.master = 0;
		p->version.subset = 1;
	}
}

int
as_partition_check_source(const as_namespace* ns, as_partition* p, cf_node src,
		bool* from_replica)
{
	return AS_OK;
}


//==========================================================
// Private API - for enterprise separation only.
//

bool
partition_reserve_promote(const as_namespace* ns, const as_partition* p,
		bool would_dup_res)
{
	return p->n_dupl != 0 && would_dup_res;
}
