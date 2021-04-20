/*
 * set_index_ce.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include "base/set_index.h"

#include <stdbool.h>

#include "log.h"

#include "base/index.h"


//==========================================================
// Private API - enterprise separation only.
//

bool
ssprig_reduce_no_rc(as_index_tree* tree, ssprig_reduce_info* ssri,
		as_index_reduce_fn cb, void* udata)
{
	cf_crash(AS_INDEX, "CE code called ssprig_reduce_no_rc()");
	return false;
}
