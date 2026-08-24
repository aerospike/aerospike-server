/*
 * hash_table.h
 *
 * Copyright (C) 2025 Aerospike, Inc.
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
#include "hardware.h"
#include "log.h"

//==========================================================
// Typedefs & constants.
//

struct hash_table_s;
typedef uint32_t (*hash_table_hash_fn)(const void* key);
typedef void (*hash_table_destructor_fn)(void* val);
typedef void (*hash_table_rel_fn)(struct hash_table_s* h, void* val);

// The "reduce" function called for every element. Returned value governs
// behavior during reduce as follows:
// - HASH_TABLE_REDUCE_CONTINUE - continue iterating
// - HASH_TABLE_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. -1) - stop iterating and return that value
typedef int (*hash_table_reduce_fn)(const void* key, void* val, void* udata);

typedef struct hash_table_s {
	hash_table_hash_fn h_fn;
	hash_table_destructor_fn d_fn;
	hash_table_rel_fn rel_fn;
	void* table;

	uint16_t key_sz;
	uint16_t val_sz;
	uint16_t key_off;
	uint16_t val_off;

	uint32_t n_buckets;
	uint32_t linked_data_sz;
	uint32_t line_sz;
	uint32_t data_sz;
	uint32_t n_elements; // only if FLAG_SLOW_SHARE_FOR_FAST_SIZE is set

	uint16_t n_line_max_ele;
	uint16_t flags;
} hash_table;

// Not over-aligned as a type: it's embedded in cf_rc_alloc'd storage whose
// refcount header offsets the body off a cacheline. Alignment is done per
// allocation instead (cf_aligned_alloc). Assert keeps it to one line when so
// aligned.
COMPILER_ASSERT(sizeof(hash_table) <= CF_CACHELINE_SZ);

typedef enum hash_table_reduce_rc_e {
	HASH_TABLE_REDUCE_CONTINUE = 0,
	HASH_TABLE_REDUCE_DELETE = 1,
} hash_table_reduce_rc;

typedef enum hash_table_flags_e {
	HASH_TABLE_FLAG_NONE = 0x00,
	HASH_TABLE_FLAG_THREAD_SAFE = 0x01,
	HASH_TABLE_FLAG_ALIGN_KEY = 0x02,
	HASH_TABLE_FLAG_ALIGN_VALUE = 0x04,
	// SLOW_SHARE_FOR_FAST_SIZE trades faster get_size() at the expense of
	// cache line coherency for modify operations (they can be slower).
	HASH_TABLE_FLAG_SLOW_SHARE_FOR_FAST_SIZE = 0x08,
	HASH_TABLE_FLAG_REDUCE_PTR = 0x10, // for rchash

	HASH_TABLE_FLAG_WAS_MALLOCED = 0x80,
} hash_table_flags;

//==========================================================
// Public API - useful hash functions.
//

uint32_t hash_table_fn_u32(const void* key);
uint32_t hash_table_fn_ptr(const void* key);
uint32_t hash_table_fn_zstr(const void* key);

//==========================================================
// Public API.
//

void hash_table_init(hash_table* h, hash_table_hash_fn h_fn,
		hash_table_destructor_fn d_fn, hash_table_rel_fn rel_fn, uint32_t key_sz,
		uint32_t ele_sz, uint32_t n_buckets, hash_table_flags flags);
hash_table* hash_table_create(hash_table_hash_fn h_fn,
		hash_table_destructor_fn d_fn, hash_table_rel_fn rel_fn, uint32_t key_sz,
		uint32_t ele_sz, uint32_t n_buckets, hash_table_flags flags);
void hash_table_destroy(hash_table* h);
void hash_table_clear(hash_table* h);

uint32_t hash_table_get_size(const hash_table* h);

void hash_table_put(hash_table* h, const void* key, const void* val);
bool hash_table_put_unique(hash_table* h, const void* key, void* val);

bool hash_table_get(hash_table* h, const void* key, void* val_r, cf_mutex** m_r);
bool hash_table_get_ptr(hash_table* h, const void* key, void** ptr_r,
		cf_mutex** m_r);
// Returns a pointer directly into bucket storage, not a copy - invalidated by
// any delete in the same bucket (delete relocates survivors). no_lock is only
// valid on non-thread-safe tables.
bool hash_table_get_direct_ptr(hash_table* h, const void* key, void** ptr_r,
		cf_mutex** m_r, bool no_lock);

bool hash_table_delete(hash_table* h, const void* key, const void* val,
		void* val_r, bool no_lock);

// Returns HASH_TABLE_REDUCE_CONTINUE after a complete pass, else the value
// reduce_fn stopped with - which is therefore never CONTINUE or DELETE, see
// hash_table_reduce_fn above.
int hash_table_reduce(hash_table* h, hash_table_reduce_fn reduce_fn, void* udata);
