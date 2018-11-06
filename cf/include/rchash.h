/*
 * rchash.h
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

#include "cf_mutex.h"


//==========================================================
// Typedefs & constants.
//

// Return codes.
#define CF_RCHASH_ERR_FOUND -4
#define CF_RCHASH_ERR_NOT_FOUND -3
#define CF_RCHASH_ERR -1
#define CF_RCHASH_OK 0
#define CF_RCHASH_REDUCE_DELETE 1

// Bit-values for 'flags' parameter.
#define CF_RCHASH_BIG_LOCK  0x01 // thread-safe with single big lock
#define CF_RCHASH_MANY_LOCK 0x02 // thread-safe with lock per bucket

// User must provide the hash function at create time.
typedef uint32_t (*cf_rchash_hash_fn)(const void *key, uint32_t key_size);

// The "reduce" function called for every element. Returned value governs
// behavior during reduce as follows:
// - CF_RCHASH_OK - continue iterating
// - CF_RCHASH_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. CF_RCHASH_ERR) - stop iterating and return reduce_fn's
//   returned value
typedef int (*cf_rchash_reduce_fn)(const void *key, uint32_t key_size, void *object, void *udata);

// User may provide an object "destructor" at create time. The destructor is
// called - and the deleted element's object freed - from cf_rchash_delete(),
// cf_rchash_delete_object(), or cf_rchash_reduce(), if the ref-count hits 0.
// The destructor should not free the object itself - that is always done after
// releasing the object if its ref-count hits 0. The destructor should only
// clean up the object's "internals".
typedef void (*cf_rchash_destructor_fn)(void *object);

// Private data.
typedef struct cf_rchash_s {
	cf_rchash_hash_fn h_fn;
	cf_rchash_destructor_fn d_fn;
	uint32_t key_size; // if key_size == 0, use variable size functions
	uint32_t n_buckets;
	uint32_t flags;
	uint32_t n_elements;
	void *table;
	cf_mutex *bucket_locks;
	cf_mutex big_lock;
} cf_rchash;


//==========================================================
// Public API - useful hash functions.
//

uint32_t cf_rchash_fn_u32(const void *key, uint32_t key_size);
uint32_t cf_rchash_fn_fnv32(const void *key, uint32_t key_size);
uint32_t cf_rchash_fn_zstr(const void *key, uint32_t key_size);


//==========================================================
// Public API.
//

cf_rchash *cf_rchash_create(cf_rchash_hash_fn h_fn, cf_rchash_destructor_fn d_fn, uint32_t key_size, uint32_t n_buckets, uint32_t flags);
void cf_rchash_destroy(cf_rchash *h);
uint32_t cf_rchash_get_size(const cf_rchash *h);

void cf_rchash_put(cf_rchash *h, const void *key, uint32_t key_size, void *object);
int cf_rchash_put_unique(cf_rchash *h, const void *key, uint32_t key_size, void *object);

int cf_rchash_get(cf_rchash *h, const void *key, uint32_t key_size, void **object_r);

int cf_rchash_delete(cf_rchash *h, const void *key, uint32_t key_size);
int cf_rchash_delete_object(cf_rchash *h, const void *key, uint32_t key_size, void *object);

int cf_rchash_reduce(cf_rchash *h, cf_rchash_reduce_fn reduce_fn, void *udata);
