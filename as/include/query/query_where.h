/*
 * query_where.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

//==========================================================
// Typedefs & constants.
//

typedef struct as_exp_s as_exp;

// WHERE field payload: [flag-byte 0][flag-byte 1]...[flag-byte N][AEL source].
//
// Flags use a varInt-style continuation encoding. In each flag byte:
//   bit 0    - continuation: 1 = another flag byte follows, 0 = last flag byte.
//   bits 1-7 - flag payload, decoded to flag positions [1 + 7*i .. 7 + 7*i] for
//              the i-th flag byte.
//
// A single flag byte with bit 0 clear (the common case) decodes to itself, so
// legacy single-byte clients (0x02, 0x04, 0x0e) are unaffected.
#define AS_QUERY_WHERE_FLAG_CONT (1 << 0)
#define AS_QUERY_WHERE_FLAG_EXPLAIN (1 << 1)
#define AS_QUERY_WHERE_FLAG_REQUIRE_INDEX (1 << 2)
#define AS_QUERY_WHERE_FLAG_HARD_HINT (1 << 3)
#define AS_QUERY_WHERE_FLAG_KNOWN                                              \
	(AS_QUERY_WHERE_FLAG_EXPLAIN | AS_QUERY_WHERE_FLAG_REQUIRE_INDEX |         \
			AS_QUERY_WHERE_FLAG_HARD_HINT)

// 9 bytes * 7 payload bits = 63 flag positions (bits 1-63), bit 0 is reserved,
// so this is the most that fits the uint64_t decoded value.
#define AS_QUERY_WHERE_FLAGS_MAX_BYTES 9

typedef struct as_query_where_s {
	uint64_t flags_decoded; // flag bytes decoded once at parse time
	const uint8_t* ael_str;
	uint32_t ael_sz;
} as_query_where;

//==========================================================
// Forward declarations.
//

struct as_exp_s;
struct as_transaction_s;

//==========================================================
// Public API.
//

bool as_query_where_parse(const struct as_transaction_s* tr,
		as_query_where* where);
as_exp* as_query_where_build_filter(const as_query_where* where);
bool as_query_where_validate_execution(const as_query_where* where);

static inline bool
as_query_where_is_explain(const as_query_where* where)
{
	return (where->flags_decoded & AS_QUERY_WHERE_FLAG_EXPLAIN) != 0;
}

static inline bool
as_query_where_require_index(const as_query_where* where)
{
	return (where->flags_decoded & AS_QUERY_WHERE_FLAG_REQUIRE_INDEX) != 0;
}

static inline bool
as_query_where_hard_hint(const as_query_where* where)
{
	return (where->flags_decoded & AS_QUERY_WHERE_FLAG_HARD_HINT) != 0;
}
