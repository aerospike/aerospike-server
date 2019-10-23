/*
 * proto_ce.c
 *
 * Copyright (C) 2019 Aerospike, Inc.
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

#include "base/proto.h"

#include <stddef.h>
#include <stdint.h>


//==========================================================
// Public API.
//

const uint8_t*
as_proto_compress(const uint8_t* original, size_t* sz,
		as_proto_comp_stat* comp_stat)
{
	return original;
}

uint8_t*
as_proto_compress_alloc(const uint8_t* original, size_t alloc_sz, size_t indent,
		size_t* sz, as_proto_comp_stat* comp_stat)
{
	return (uint8_t*)original;
}

uint32_t
as_proto_decompress(const as_comp_proto* cproto, as_proto** p_proto)
{
	return AS_ERR_ENTERPRISE_ONLY;
}
