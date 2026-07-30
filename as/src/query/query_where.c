/*
 * query_where.c
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

//==========================================================
// Includes.
//

#include "query/query_where.h"

#include <inttypes.h>

#include "log.h"

#include "base/proto.h"
#include "base/transaction.h"
#include "exp/exp.h"

#include "warnings.h"

//==========================================================
// Forward declarations.
//

static const as_msg_field* where_msg_field(const as_transaction* tr);
static bool where_parse_flags(const as_msg_field* f, uint64_t* decoded_r,
		uint32_t* flags_sz_r);

//==========================================================
// Public API.
//

bool
as_query_where_parse(const as_transaction* tr, as_query_where* where)
{
	where->flags_decoded = 0;
	where->ael_str = NULL;
	where->ael_sz = 0;

	const as_msg_field* f = where_msg_field(tr);

	if (f == NULL) {
		cf_ticker_warning(AS_QUERY, "WHERE parse: missing WHERE field");
		return false;
	}

	if (as_transaction_has_predexp(tr)) {
		cf_ticker_warning(AS_QUERY,
				"WHERE parse: cannot specify both WHERE and PREDEXP");
		return false;
	}

	uint32_t flags_sz = 0;

	if (! where_parse_flags(f, &where->flags_decoded, &flags_sz)) {
		return false;
	}

	uint32_t len = as_msg_field_get_value_sz(f);

	if (len <= flags_sz) {
		cf_ticker_warning(AS_QUERY, "WHERE parse: missing AEL filter");
		return false;
	}

	where->ael_str = f->data + flags_sz;
	where->ael_sz = len - flags_sz;

	return true;
}

as_exp*
as_query_where_build_filter(const as_query_where* where)
{
	if (where == NULL || where->ael_str == NULL || where->ael_sz == 0) {
		cf_ticker_warning(AS_QUERY, "WHERE filter: missing AEL filter");
		return NULL;
	}

	return as_exp_filter_build_ael(where->ael_str, where->ael_sz);
}

bool
as_query_where_validate_execution(const as_query_where* where)
{
	if (as_query_where_require_index(where) || as_query_where_hard_hint(where)) {
		cf_ticker_warning(AS_QUERY,
				"WHERE execution: REQUIRE_INDEX or HARD_HINT requires EXPLAIN");
		return false;
	}

	return true;
}

//==========================================================
// Local helpers.
//

static const as_msg_field*
where_msg_field(const as_transaction* tr)
{
	if (! as_transaction_has_where_field(tr)) {
		return NULL;
	}

	return as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_WHERE);
}

static bool
where_parse_flags(const as_msg_field* f, uint64_t* decoded_r, uint32_t* flags_sz_r)
{
	uint32_t len = as_msg_field_get_value_sz(f);
	const uint8_t* p = f->data;
	uint64_t decoded = 0;
	uint32_t i = 0;

	while (true) {
		if (i >= len || i >= AS_QUERY_WHERE_FLAGS_MAX_BYTES) {
			cf_ticker_warning(AS_QUERY, "WHERE parse: malformed flags");
			return false;
		}

		uint8_t b = p[i];

		decoded |= (uint64_t)(b >> 1) << (1 + 7 * i);

		bool more = (b & AS_QUERY_WHERE_FLAG_CONT) != 0;

		i++;

		if (! more) {
			break;
		}
	}

	if ((decoded & ~(uint64_t)AS_QUERY_WHERE_FLAG_KNOWN) != 0) {
		cf_ticker_warning(AS_QUERY, "WHERE parse: unknown flags 0x%" PRIx64,
				decoded);
		return false;
	}

	*decoded_r = decoded;
	*flags_sz_r = i;

	return true;
}
