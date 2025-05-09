/*
 * roster_ce.c
 *
 * Copyright (C) 2017-2020 Aerospike, Inc.
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

#include "fabric/roster.h"

#include <stdbool.h>

#include "dynbuf.h"
#include "log.h"

#include "base/proto.h"
#include "base/thr_info.h"


//==========================================================
// Public API.
//

void
as_roster_init(void)
{
	// CE Code doesn't invoke roster SMD module.
}

void
as_roster_set_nodes_cmd(const char* ns_name, const char* nodes, cf_dyn_buf* db)
{
	cf_crash(AS_ROSTER, "CE code called as_roster_set_nodes_cmd()");
}
