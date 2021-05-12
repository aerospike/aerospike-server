/*
 * index_ce.c
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

//==========================================================
// Includes.
//

#include "base/index.h"

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "log.h"

#include "base/datamodel.h"


//==========================================================
// Public API.
//

as_index_tree*
as_index_tree_resume(as_index_tree_shared* shared, as_treex* xmem_trees,
		uint32_t pid, as_index_tree_done_fn cb, void* udata)
{
	cf_crash(AS_INDEX, "CE code called as_index_tree_resume()");
	return NULL;
}

bool
as_index_reduce_live(as_index_tree* tree, as_index_reduce_fn cb, void* udata)
{
	return as_index_reduce(tree, cb, udata);
}

bool
as_index_reduce_from_live(as_index_tree* tree, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	return as_index_reduce_from(tree, keyd, cb, udata);
}


//==========================================================
// Private API - for enterprise separation only.
//

bool
as_index_sprig_reduce_no_rc(as_index_sprig* isprig, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	cf_crash(AS_INDEX, "CE code called as_index_sprig_reduce_no_rc()");
	return false;
}
