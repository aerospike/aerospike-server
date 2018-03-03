/*
 * bits.h
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
// Public API.
//

// Position of most significant bit, 0 ... 63 from low to high. -1 for value 0.
static inline int
cf_msb(uint64_t value)
{
	int n = -1;

	while (value != 0) {
		value >>= 1;
		n++;
	}

	return n;
}

// Returns number of trailing zeros in a uint64_t, 64 for x == 0.
static inline uint32_t
cf_lsb64(uint64_t x)
{
	if (x == 0) {
		return 64;
	}

	return (uint32_t)__builtin_ctzll(x);
}

// Returns number of leading zeros in a uint64_t, 64 for x == 0.
static inline uint32_t
cf_msb64(uint64_t x)
{
	if (x == 0) {
		return 64;
	}

	return (uint32_t)__builtin_clzll(x);
}

static inline uint32_t
cf_bit_count64(uint64_t x)
{
	x -= (x >> 1) & 0x5555555555555555;
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333);
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f;

	return (uint32_t)((x * 0x0101010101010101) >> 56);
}
