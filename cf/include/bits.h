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

// Number of bytes occupied by val converted to a "uintvar".
static inline uint32_t
uintvar_size(uint32_t val)
{
	if ((val & 0xFFFFff80) == 0) {
		return 1;
	}

	if ((val & 0xFFFFc000) == 0) {
		return 2;
	}

	if ((val & 0xFFE00000) == 0) {
		return 3;
	}

	if ((val & 0xF0000000) == 0) {
		return 4;
	}

	return 5;
}

static inline uint8_t *
uintvar_pack(uint8_t *buf, uint32_t val)
{
	if ((val & 0xFFFFff80) == 0) { // one byte only - most common
		*buf++ = (uint8_t)val;
		return buf;
	}

	if ((val & 0xFFFFc000) == 0) { // two bytes - next most common
		*buf++ = (uint8_t)(val >> 7) | 0x80;
		*buf++ = (uint8_t)(val & 0x7F);
		return buf;
	}

	// More explicit cases have odd performance testing results, so just loop
	// for everything that would take more than two bytes...

	for (int i = 4; i > 0; i--) {
		uint32_t shift_val = val >> (7 * i);

		if (shift_val != 0) {
			*buf++ = (uint8_t)(shift_val | 0x80);
		}
	}

	*buf++ = (uint8_t)(val & 0x7F);

	return buf;
}

static inline uint32_t
uintvar_parse(const uint8_t** pp_read, const uint8_t* end)
{
	const uint8_t* p_read = *pp_read;

	if (p_read >= end) {
		*pp_read = NULL;
		return 0;
	}

	if ((*p_read & 0x80) == 0) { // one byte only - most common - done
		*pp_read = p_read + 1;
		return *p_read;
	}

	if (*p_read == 0x80) { // leading zeros are illegal
		*pp_read = NULL;
		return 0;
	}

	uint32_t val = 0;

	do {
		val |= *p_read++ & 0x7F;
		val <<= 7;

		if (p_read >= end) {
			break;
		}

		if ((*p_read & 0x80) == 0) { // no continuation bit - last byte - done
			val |= *p_read;
			*pp_read = p_read + 1;
			return val;
		}
	} while ((val & 0xFE000000) == 0); // don't shift significant bits off top

	*pp_read = NULL;

	return 0;
}
