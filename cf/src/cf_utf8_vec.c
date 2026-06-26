/*
 * cf_utf8_vec.c
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

// SIMD UTF-8 validation (lookup4 algorithm via SIMDe). Scalar UTF-8 validation
// and the fast/slow dispatch live in cf_str.c (cf_str_is_valid_utf8); this file
// only implements the SIMD bulk path (cf_utf8_validate_128).

#include "cf_utf8_vec.h"

#include <simde/x86/sse4.1.h>
#include <string.h>

//==========================================================
// lookup4v — SSSE3/SSE4.1 vectorised UTF-8 validator.
//
// Three 16-byte nibble lookups via pshufb (Lemire's algorithm).
//

// clang-format off
#define L4_TOO_SHORT      (1u << 0)
#define L4_TOO_LONG       (1u << 1)
#define L4_OVERLONG_3     (1u << 2)
#define L4_TOO_LARGE      (1u << 3)
#define L4_SURROGATE      (1u << 4)
#define L4_OVERLONG_2     (1u << 5)
#define L4_TOO_LARGE_1000 (1u << 6)
#define L4_TWO_CONTS      (1u << 7)
#define L4_OVERLONG_4     (1u << 6)

#define L4_CARRY (L4_TOO_SHORT | L4_TOO_LONG | L4_TWO_CONTS)

static const uint8_t lookup4v_byte_1_high[16] __attribute__((aligned(16))) = {
	L4_TOO_LONG, L4_TOO_LONG, L4_TOO_LONG, L4_TOO_LONG,
	L4_TOO_LONG, L4_TOO_LONG, L4_TOO_LONG, L4_TOO_LONG,
	L4_TWO_CONTS, L4_TWO_CONTS, L4_TWO_CONTS, L4_TWO_CONTS,
	(uint8_t)(L4_TOO_SHORT | L4_OVERLONG_2),
	L4_TOO_SHORT,
	(uint8_t)(L4_TOO_SHORT | L4_OVERLONG_3 | L4_SURROGATE),
	(uint8_t)(L4_TOO_SHORT | L4_TOO_LARGE | L4_TOO_LARGE_1000 | L4_OVERLONG_4),
};

static const uint8_t lookup4v_byte_1_low[16] __attribute__((aligned(16))) = {
	(uint8_t)(L4_CARRY | L4_OVERLONG_3 | L4_OVERLONG_2 | L4_OVERLONG_4),
	(uint8_t)(L4_CARRY | L4_OVERLONG_2),
	L4_CARRY,
	L4_CARRY,
	(uint8_t)(L4_CARRY | L4_TOO_LARGE),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000 | L4_SURROGATE),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
	(uint8_t)(L4_CARRY | L4_TOO_LARGE | L4_TOO_LARGE_1000),
};

static const uint8_t lookup4v_byte_2_high[16] __attribute__((aligned(16))) = {
	L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT,
	L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT,
	(uint8_t)(L4_TOO_LONG | L4_OVERLONG_2 | L4_TWO_CONTS | L4_OVERLONG_3 | L4_TOO_LARGE_1000 | L4_OVERLONG_4),
	(uint8_t)(L4_TOO_LONG | L4_OVERLONG_2 | L4_TWO_CONTS | L4_OVERLONG_3 | L4_TOO_LARGE),
	(uint8_t)(L4_TOO_LONG | L4_OVERLONG_2 | L4_TWO_CONTS | L4_SURROGATE | L4_TOO_LARGE),
	(uint8_t)(L4_TOO_LONG | L4_OVERLONG_2 | L4_TWO_CONTS | L4_SURROGATE | L4_TOO_LARGE),
	L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT, L4_TOO_SHORT,
};

static const uint8_t lookup4v_inc_max[16] __attribute__((aligned(16))) = {
	255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255,
	(uint8_t)(0xF0u - 1u), (uint8_t)(0xE0u - 1u), (uint8_t)(0xC0u - 1u),
};
// clang-format on

static inline simde__m128i
lookup4v_check_special_cases(simde__m128i prev1, simde__m128i cur,
		simde__m128i tbl_b1h, simde__m128i tbl_b1l, simde__m128i tbl_b2h)
{
	simde__m128i mask_0f = simde_mm_set1_epi8(0x0F);

	simde__m128i p1_hi = simde_mm_shuffle_epi8(tbl_b1h,
			simde_mm_and_si128(simde_mm_srli_epi16(prev1, 4), mask_0f));
	simde__m128i p1_lo =
			simde_mm_shuffle_epi8(tbl_b1l, simde_mm_and_si128(prev1, mask_0f));
	simde__m128i c_hi = simde_mm_shuffle_epi8(tbl_b2h,
			simde_mm_and_si128(simde_mm_srli_epi16(cur, 4), mask_0f));

	return simde_mm_and_si128(simde_mm_and_si128(p1_hi, p1_lo), c_hi);
}

static inline simde__m128i
lookup4v_must_be_23_cont(simde__m128i prev2, simde__m128i prev3)
{
	simde__m128i is_third =
			simde_mm_subs_epu8(prev2, simde_mm_set1_epi8((char)(0xE0u - 0x80u)));
	simde__m128i is_fourth =
			simde_mm_subs_epu8(prev3, simde_mm_set1_epi8((char)(0xF0u - 0x80u)));

	return simde_mm_or_si128(is_third, is_fourth);
}

static inline simde__m128i
lookup4v_check_utf8_bytes(simde__m128i cur, simde__m128i prev,
		simde__m128i tbl_b1h, simde__m128i tbl_b1l, simde__m128i tbl_b2h)
{
	simde__m128i prev1 = simde_mm_alignr_epi8(cur, prev, 16 - 1);
	simde__m128i prev2 = simde_mm_alignr_epi8(cur, prev, 16 - 2);
	simde__m128i prev3 = simde_mm_alignr_epi8(cur, prev, 16 - 3);

	simde__m128i sc =
			lookup4v_check_special_cases(prev1, cur, tbl_b1h, tbl_b1l, tbl_b2h);
	simde__m128i mb = lookup4v_must_be_23_cont(prev2, prev3);

	return simde_mm_xor_si128(simde_mm_and_si128(mb,
									  simde_mm_set1_epi8((char)0x80u)),
			sc);
}

bool
cf_utf8_validate_128(const uint8_t* buf, size_t buf_sz)
{
	simde__m128i tbl_b1h = simde_mm_load_si128((const simde__m128i*)(const void*)
					lookup4v_byte_1_high);
	simde__m128i tbl_b1l = simde_mm_load_si128((const simde__m128i*)(const void*)
					lookup4v_byte_1_low);
	simde__m128i tbl_b2h = simde_mm_load_si128((const simde__m128i*)(const void*)
					lookup4v_byte_2_high);
	simde__m128i inc_max =
			simde_mm_load_si128((const simde__m128i*)(const void*)lookup4v_inc_max);
	simde__m128i zero = simde_mm_setzero_si128();
	simde__m128i prev = zero;
	simde__m128i prev_inc = zero;
	size_t i = 0;

	while (i + 64 <= buf_sz) {
		simde__m128i c0 =
				simde_mm_loadu_si128((const simde__m128i*)(const void*)(buf + i));
		simde__m128i c1 =
				simde_mm_loadu_si128((const simde__m128i*)(const void*)(buf +
						i + 16));
		simde__m128i c2 =
				simde_mm_loadu_si128((const simde__m128i*)(const void*)(buf +
						i + 32));
		simde__m128i c3 =
				simde_mm_loadu_si128((const simde__m128i*)(const void*)(buf +
						i + 48));

		simde__m128i any_hi = simde_mm_or_si128(simde_mm_or_si128(c0, c1),
				simde_mm_or_si128(c2, c3));

		if (simde_mm_movemask_epi8(any_hi) == 0) {
			if (! simde_mm_testz_si128(prev_inc, prev_inc)) {
				return false;
			}

			prev = c3;
			prev_inc = zero;
			i += 64;
			continue;
		}

		simde__m128i err =
				lookup4v_check_utf8_bytes(c0, prev, tbl_b1h, tbl_b1l, tbl_b2h);
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c1, c0, tbl_b1h, tbl_b1l, tbl_b2h));
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c2, c1, tbl_b1h, tbl_b1l, tbl_b2h));
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c3, c2, tbl_b1h, tbl_b1l, tbl_b2h));

		if (! simde_mm_testz_si128(err, err)) {
			return false;
		}

		prev_inc = simde_mm_subs_epu8(c3, inc_max);
		prev = c3;
		i += 64;
	}

	if (buf_sz > i) {
		uint8_t tail[64] __attribute__((aligned(16))) = { 0 };

		memcpy(tail, buf + i, buf_sz - i);

		simde__m128i c0 =
				simde_mm_load_si128((const simde__m128i*)(const void*)tail);
		simde__m128i c1 =
				simde_mm_load_si128((const simde__m128i*)(const void*)(tail + 16));
		simde__m128i c2 =
				simde_mm_load_si128((const simde__m128i*)(const void*)(tail + 32));
		simde__m128i c3 =
				simde_mm_load_si128((const simde__m128i*)(const void*)(tail + 48));

		simde__m128i err =
				lookup4v_check_utf8_bytes(c0, prev, tbl_b1h, tbl_b1l, tbl_b2h);
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c1, c0, tbl_b1h, tbl_b1l, tbl_b2h));
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c2, c1, tbl_b1h, tbl_b1l, tbl_b2h));
		err = simde_mm_or_si128(err,
				lookup4v_check_utf8_bytes(c3, c2, tbl_b1h, tbl_b1l, tbl_b2h));

		if (! simde_mm_testz_si128(err, err)) {
			return false;
		}

		prev_inc = simde_mm_subs_epu8(c3, inc_max);
	}

	return simde_mm_testz_si128(prev_inc, prev_inc);
}
