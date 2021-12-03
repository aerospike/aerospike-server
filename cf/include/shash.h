/*
 * shash.h
 *
 * Copyright (C) 2017-2021 Aerospike, Inc.
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


//==========================================================
// Typedefs & constants.
//

// Return codes.
#define CF_SHASH_ERR_FOUND -4
#define CF_SHASH_ERR_NOT_FOUND -3
#define CF_SHASH_ERR -1
#define CF_SHASH_OK 0
#define CF_SHASH_REDUCE_DELETE 1

// User must provide the hash function at create time.
typedef uint32_t (*cf_shash_hash_fn)(const void* key);

// The "reduce" function called for every element. Returned value governs
// behavior during reduce as follows:
// - CF_SHASH_OK - continue iterating
// - CF_SHASH_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. CF_SHASH_ERR) - stop iterating and return reduce_fn's
//   returned value
typedef int (*cf_shash_reduce_fn)(const void* key, void* value, void* udata);

// Private data.
typedef struct cf_shash_s {
	cf_shash_hash_fn h_fn;
	uint32_t key_size;
	uint32_t value_size;
	uint32_t ele_size;
	uint32_t n_buckets;
	bool thread_safe;
	uint32_t n_elements;
	void* table;
	cf_mutex* bucket_locks;
} cf_shash;


//==========================================================
// Public API - useful hash functions.
//

// TODO - hash function signature may change.
uint32_t cf_shash_fn_u32(const void* key);
uint32_t cf_shash_fn_ptr(const void* key);
uint32_t cf_shash_fn_zstr(const void* key);


//==========================================================
// Public API.
//

cf_shash* cf_shash_create(cf_shash_hash_fn h_fn, uint32_t key_size, uint32_t value_size, uint32_t n_buckets, bool thread_safe);
void cf_shash_destroy(cf_shash* h);
uint32_t cf_shash_get_size(const cf_shash* h);

void cf_shash_put(cf_shash* h, const void* key, const void* value);
int cf_shash_put_unique(cf_shash* h, const void* key, const void* value);

int cf_shash_get(cf_shash* h, const void* key, void* value);
int cf_shash_get_vlock(cf_shash* h, const void* key, void** value_r, cf_mutex** vlock_r);

int cf_shash_pop(cf_shash* h, const void* key, void* value);

int cf_shash_delete(cf_shash* h, const void* key);
int cf_shash_delete_lockfree(cf_shash* h, const void* key);
void cf_shash_delete_all(cf_shash* h);

int cf_shash_reduce(cf_shash* h, cf_shash_reduce_fn reduce_fn, void* udata);
