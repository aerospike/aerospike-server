/*
 * shash.h
 *
 * Copyright (C) 2017-2025 Aerospike, Inc.
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

#include "cf_mutex.h"
#include "hash_table.h"

//==========================================================
// Typedefs & constants.
//

// Return codes.
#define CF_SHASH_ERR_FOUND -4
#define CF_SHASH_ERR_NOT_FOUND -3
#define CF_SHASH_ERR -1
#define CF_SHASH_OK HASH_TABLE_REDUCE_CONTINUE
#define CF_SHASH_REDUCE_DELETE HASH_TABLE_REDUCE_DELETE

// User must provide the hash function at create time.
typedef hash_table_hash_fn cf_shash_hash_fn;

// The "reduce" function called for every element. Returned value governs
// behavior during reduce as follows:
// - CF_SHASH_OK - continue iterating
// - CF_SHASH_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. CF_SHASH_ERR) - stop iterating and return reduce_fn's
//   returned value
typedef hash_table_reduce_fn cf_shash_reduce_fn;

typedef hash_table cf_shash;

//==========================================================
// Public API - useful hash functions.
//

#define cf_shash_fn_u32 hash_table_fn_u32
#define cf_shash_fn_ptr hash_table_fn_ptr
#define cf_shash_fn_zstr hash_table_fn_zstr

//==========================================================
// Public API.
//

void cf_shash_init(cf_shash* h, cf_shash_hash_fn h_fn, uint32_t key_size,
		uint32_t value_size, uint32_t n_buckets, bool thread_safe);
cf_shash* cf_shash_create(cf_shash_hash_fn h_fn, uint32_t key_size,
		uint32_t value_size, uint32_t n_buckets, bool thread_safe);
void cf_shash_destroy(cf_shash* h);

static inline uint32_t
cf_shash_get_size(const cf_shash* h)
{
	return hash_table_get_size(h);
}

static inline void
cf_shash_put(cf_shash* h, const void* key, const void* value)
{
	hash_table_put(h, key, value);
}

static inline int
cf_shash_put_unique(cf_shash* h, const void* key, const void* value)
{
	return hash_table_put_unique(h, key, (void*)value) ? CF_SHASH_OK
													   : CF_SHASH_ERR_FOUND;
}

static inline int
cf_shash_get(cf_shash* h, const void* key, void* value)
{
	return hash_table_get(h, key, value, NULL) ? CF_SHASH_OK
											   : CF_SHASH_ERR_NOT_FOUND;
}

// value_r aliases bucket storage and is invalidated by any delete in the same
// bucket, even under the held lock.
static inline int
cf_shash_get_vlock(cf_shash* h, const void* key, void** value_r, cf_mutex** m_r)
{
	return hash_table_get_direct_ptr(h, key, value_r, m_r, false)
			? CF_SHASH_OK
			: CF_SHASH_ERR_NOT_FOUND;
}

// returns value pointer instead of copy and without lock.
static inline int
cf_shash_get_p(cf_shash* h, const void* key, void** value_r)
{
	return hash_table_get_direct_ptr(h, key, value_r, NULL, true)
			? CF_SHASH_OK
			: CF_SHASH_ERR_NOT_FOUND;
}

static inline int
cf_shash_pop(cf_shash* h, const void* key, void* value)
{
	return hash_table_delete(h, key, NULL, value, false)
			? CF_SHASH_OK
			: CF_SHASH_ERR_NOT_FOUND;
}

static inline int
cf_shash_delete(cf_shash* h, const void* key)
{
	return hash_table_delete(h, key, NULL, NULL, false) ? CF_SHASH_OK
														: CF_SHASH_ERR_NOT_FOUND;
}

static inline int
cf_shash_delete_lockfree(cf_shash* h, const void* key)
{
	return hash_table_delete(h, key, NULL, NULL, true) ? CF_SHASH_OK
													   : CF_SHASH_ERR_NOT_FOUND;
}

static inline void
cf_shash_delete_all(cf_shash* h)
{
	return hash_table_clear(h);
}

static inline int
cf_shash_reduce(cf_shash* h, cf_shash_reduce_fn reduce_fn, void* udata)
{
	return hash_table_reduce(h, reduce_fn, udata) ? CF_SHASH_OK : CF_SHASH_ERR;
}
