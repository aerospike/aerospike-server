/*
 * truncate_ce.c
 *
 * Copyright (C) 2017-2019 Aerospike, Inc.
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

#include "base/truncate.h"

#include <stdbool.h>
#include <stdint.h>

#include "base/datamodel.h"


//==========================================================
// Public API.
//

void
as_truncate_done_startup(as_namespace* ns)
{
}


void
as_truncate_list_cenotaphs(as_namespace* ns)
{
}


bool
as_truncate_lut_is_truncated(uint64_t rec_lut, as_namespace* ns,
		const char* set_name, uint32_t set_name_len)
{
	return false;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
truncate_startup_hash_init(as_namespace* ns)
{
}


void
truncate_action_startup(as_namespace* ns, const char* set_name, uint64_t lut)
{
}

