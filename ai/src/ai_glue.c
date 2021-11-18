/*
 * ai_glue.c
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

#include "ai_glue.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "ai_types.h"

#include "log.h"

#include <citrusleaf/cf_digest.h>

#include "warnings.h"

#pragma GCC diagnostic warning "-Wcast-qual"
#pragma GCC diagnostic warning "-Wcast-align"


//==========================================================
// Typedefs & constants.
//

#define ORDER_DIGEST 32
#define ORDER_INTEGER 18


//==========================================================
// Public API.
//

int32_t
comp_digest(const void* key_1, const void* key_2)
{
	return memcmp(key_1, key_2, sizeof(cf_digest));
}

int32_t
comp_integer(const void* key_1, const void* key_2)
{
	const uint64_t* int_1 = key_1;
	const uint64_t* int_2 = key_2;

	return *int_1 == *int_2 ? 0 : (*int_1 > *int_2) ? 1 : -1;
}

as_btree*
createIBT(col_type_t type)
{
	as_btree_key_comp_fn key_comp;
	uint32_t key_sz;
	uint32_t order;

	if (C_IS_DIGEST(type)) {
		key_comp = comp_digest;
		key_sz = sizeof(cf_digest);
		order = ORDER_DIGEST;
	}
	else {
		key_comp = comp_integer;
		key_sz = sizeof(uint64_t);
		order = ORDER_INTEGER;
	}

	return as_btree_create(key_comp, key_sz, sizeof(uint64_t), order);

}

as_btree*
createNBT(void)
{
	return as_btree_create(comp_integer, sizeof(uint64_t), 0, ORDER_INTEGER);
}

void
btIndAdd(as_btree* ibt, const ai_obj* ikey, const ai_nbtr* nbt)
{
	bool ok = as_btree_put(ibt, ikey, &nbt);

	cf_assert(ok, AS_INDEX, "first-level key exists");
}

ai_nbtr*
btIndFind(const as_btree* ibt, const ai_obj* ikey)
{
	ai_nbtr* value;

	if (! as_btree_get(ibt, ikey, &value)) {
		return NULL;
	}

	return value;
}

void
btIndDelete(as_btree* ibt, const ai_obj* ikey)
{
	bool ok = as_btree_delete(ibt, ikey);

	cf_assert(ok, AS_SINDEX, "first-level key not found");
}

void
btIndNodeAdd(as_btree* nbt, const ai_obj* nkey)
{
	cf_assert(nbt->value_sz == 0, AS_SINDEX, "bad second-level value size");

	bool ok = as_btree_put(nbt, nkey, NULL);

	cf_assert(ok, AS_INDEX, "second-level key exists");
}

bool
btIndNodeExist(const as_btree* nbt, const ai_obj* nkey)
{
	cf_assert(nbt->value_sz == 0, AS_SINDEX, "bad second-level value size");

	return as_btree_get(nbt, nkey, NULL);
}

void
btIndNodeDelete(as_btree* nbt, const ai_obj* nkey)
{
	bool ok = as_btree_delete(nbt, nkey);

	cf_assert(ok, AS_SINDEX, "second-level key not found");
}
