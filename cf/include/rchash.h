/*
 * rchash.h
 *
 * Copyright (C) 2018-2025 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "hash_table.h"

//==========================================================
// Typedefs & constants.
//

// Return codes.
#define CF_RCHASH_ERR_FOUND -4
#define CF_RCHASH_ERR_NOT_FOUND -3
#define CF_RCHASH_ERR -1
#define CF_RCHASH_OK HASH_TABLE_REDUCE_CONTINUE
#define CF_RCHASH_REDUCE_DELETE HASH_TABLE_REDUCE_DELETE

// User must provide the hash function at create time.
typedef hash_table_hash_fn cf_rchash_hash_fn;

// The "reduce" function called for every element. Returned value governs
// behavior during reduce as follows:
// - CF_RCHASH_OK - continue iterating
// - CF_RCHASH_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. CF_RCHASH_ERR) - stop iterating and return reduce_fn's
//   returned value
typedef hash_table_reduce_fn cf_rchash_reduce_fn;

// User may provide an object "destructor" at create time. The destructor is
// called - and the deleted element's object freed - from cf_rchash_delete(),
// cf_rchash_delete_object(), or cf_rchash_reduce(), if the ref-count hits 0.
// The destructor should not free the object itself - that is always done after
// releasing the object if its ref-count hits 0. The destructor should only
// clean up the object's "internals".
typedef hash_table_destructor_fn cf_rchash_destructor_fn;

typedef hash_table cf_rchash;

//==========================================================
// Public API - useful hash functions.
//

#define cf_rchash_fn_u32 hash_table_fn_u32
#define cf_rchash_fn_zstr hash_table_fn_zstr

//==========================================================
// Public API.
//

void cf_rchash_init(cf_rchash* h, cf_rchash_hash_fn h_fn,
		cf_rchash_destructor_fn d_fn, uint32_t key_size, uint32_t n_buckets);
cf_rchash* cf_rchash_create(cf_rchash_hash_fn h_fn,
		cf_rchash_destructor_fn d_fn, uint32_t key_size, uint32_t n_buckets);
void cf_rchash_destroy(cf_rchash* h);

// O(n_buckets) - rchash has no shared element counter (see RCHASH_FLAGS).
static inline uint32_t
cf_rchash_get_size(const cf_rchash* h)
{
	return hash_table_get_size(h);
}

static inline void
cf_rchash_put(cf_rchash* h, const void* key, void* object)
{
	hash_table_put(h, key, &object);
}

static inline int
cf_rchash_put_unique(cf_rchash* h, const void* key, void* object)
{
	return hash_table_put_unique(h, key, &object) ? CF_RCHASH_OK
												  : CF_RCHASH_ERR_FOUND;
}

static inline int
cf_rchash_get(cf_rchash* h, const void* key, void** object_r)
{
	cf_mutex* m;

	if (! hash_table_get(h, key, object_r, &m)) {
		return CF_RCHASH_ERR_NOT_FOUND;
	}

	if (object_r != NULL) {
		cf_rc_reserve(*object_r);
	}

	cf_mutex_unlock(m);

	return CF_RCHASH_OK;
}

static inline int
cf_rchash_delete_object(cf_rchash* h, const void* key, void* object)
{
	return hash_table_delete(h, key, (object == NULL) ? NULL : (void*)(&object),
				   NULL, false)
			? CF_RCHASH_OK
			: CF_RCHASH_ERR_NOT_FOUND;
}

static inline int
cf_rchash_delete(cf_rchash* h, const void* key)
{
	return cf_rchash_delete_object(h, key, NULL);
}

static inline int
cf_rchash_reduce(cf_rchash* h, cf_rchash_reduce_fn reduce_fn, void* udata)
{
	return hash_table_reduce(h, reduce_fn, udata) ? CF_RCHASH_OK : CF_RCHASH_ERR;
}
