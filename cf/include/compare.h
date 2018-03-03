/*
 * compare.h
 *
 * Copyright (C) 2018 Aerospike, Inc.
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

#pragma once

//==========================================================
// Includes.
//

#include <stdint.h>


//==========================================================
// Public API - qsort() comparators.
//

static inline int
cf_compare_uint64_desc(const void* pa, const void* pb)
{
	uint64_t a = *(const uint64_t*)pa;
	uint64_t b = *(const uint64_t*)pb;

	return a > b ? -1 : (a == b ? 0 : 1);
}

static inline int
cf_compare_uint32_desc(const void* pa, const void* pb)
{
	uint32_t a = *(const uint32_t*)pa;
	uint32_t b = *(const uint32_t*)pb;

	return a > b ? -1 : (a == b ? 0 : 1);
}
