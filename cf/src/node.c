/*
 * node.c
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

#include "node.h"

#include <stdint.h>


//==========================================================
// Inlines & macros.
//

static inline uint32_t
node_id_hash_fn(cf_node id)
{
	return (uint32_t)(id >> 32) ^ (uint32_t)id;
}


//==========================================================
// Public API.
//

uint32_t
cf_nodeid_shash_fn(const void* key)
{
	return node_id_hash_fn(*(const cf_node*)key);
}

uint32_t
cf_nodeid_rchash_fn(const void* key)
{
	return node_id_hash_fn(*(const cf_node*)key);
}
