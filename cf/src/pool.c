/*
 * pool.c
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

#include "pool.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"

#include "bits.h"


//==========================================================
// Inlines & macros.
//

// TODO - move to bits.h?
static inline uint32_t
uint32_up_to_pow2(uint32_t x)
{
	if (x == 0) {
		return 1;
	}

	if ((x & (x - 1)) == 0) {
		return x;
	}

	// TODO - crash on x >> 31 == 1?

	return 1 << (64 - cf_msb64(x));
}

#define POOL_MOD(_ix) (_ix & pool->cap_minus_1)


//==========================================================
// Public API - cf_pool_int32.
//

void
cf_pool_int32_init(cf_pool_int32* pool, uint32_t capacity, int32_t empty_val)
{
	uint32_t round_capacity = uint32_up_to_pow2(capacity);

	*pool = (cf_pool_int32){
			.capacity = round_capacity,
			.cap_minus_1 = round_capacity - 1,
			.empty_val = empty_val,
			.data = cf_malloc(sizeof(int32_t) * round_capacity)
	};

	for (uint32_t ix = 0; ix < round_capacity; ix++) {
		pool->data[ix] = empty_val;
	}
}

void
cf_pool_int32_destroy(cf_pool_int32* pool)
{
	cf_free(pool->data);
}

int32_t
cf_pool_int32_pop(cf_pool_int32* pool)
{
	int32_t count = as_load_int32(&pool->count);

	if (count <= 0) {
		return pool->empty_val;
	}

	count = as_aaf_int32(&pool->count, -1);

	if (count < 0) {
		as_incr_int32(&pool->count);
		return pool->empty_val;
	}

	// We now own one of the items in the pool.

	int32_t val;

	while (true) {
		uint32_t read_ix = as_faa_uint32(&pool->read_ix, 1);
		uint32_t ix = POOL_MOD(read_ix);

		if (pool->data[ix] != pool->empty_val) {
			val = as_fas_int32(&pool->data[ix], pool->empty_val);

			if (val != pool->empty_val) {
				break;
			}
		}
	}

	return val;
}

void
cf_pool_int32_push(cf_pool_int32* pool, int32_t val)
{
	while (true) {
		uint32_t write_ix = as_faa_uint32(&pool->write_ix, 1);
		uint32_t ix = POOL_MOD(write_ix);

		if (pool->data[ix] == pool->empty_val &&
				as_cas_int32(&pool->data[ix], pool->empty_val, val)) {
			break;
		}
	}

	as_incr_int32(&pool->count);
}


//==========================================================
// Public API - cf_pool_ptr.
//

void
cf_pool_ptr_init(cf_pool_ptr* pool, uint32_t capacity)
{
	uint32_t round_capacity = uint32_up_to_pow2(capacity);

	*pool = (cf_pool_ptr){
			.capacity = round_capacity,
			.cap_minus_1 = round_capacity - 1,
			.data = cf_calloc(round_capacity, sizeof(void*))
	};
}

void
cf_pool_ptr_destroy(cf_pool_ptr* pool)
{
	cf_free(pool->data);
}

void*
cf_pool_ptr_pop(cf_pool_ptr* pool)
{
	int32_t count = as_load_int32(&pool->count);

	if (count <= 0) {
		return NULL;
	}

	count = as_aaf_int32(&pool->count, -1);

	if (count < 0) {
		as_incr_int32(&pool->count);
		return NULL;
	}

	// We now own one of the items in the pool.

	void* val;

	while (true) {
		uint32_t read_ix = as_faa_uint32(&pool->read_ix, 1);
		uint32_t ix = POOL_MOD(read_ix);

		if (pool->data[ix] != NULL) {
			val = as_fas_ptr(&pool->data[ix], NULL);

			if (val != NULL) {
				break;
			}
		}
	}

	return val;
}

void
cf_pool_ptr_push(cf_pool_ptr* pool, void* val)
{
	while (true) {
		uint32_t write_ix = as_faa_uint32(&pool->write_ix, 1);
		uint32_t ix = POOL_MOD(write_ix);

		if (pool->data[ix] == NULL && as_cas_ptr(&pool->data[ix], NULL, val)) {
			break;
		}
	}

	as_incr_int32(&pool->count);
}

bool
cf_pool_ptr_remove(cf_pool_ptr* pool, void* val)
{
	void* val2 = cf_pool_ptr_pop(pool);

	if (val2 == val) {
		return true;
	}

	if (val2 == NULL) {
		return false;
	}

	uint32_t ix;

	for (ix = 0; ix < pool->capacity; ix++) {
		if (pool->data[ix] == val) {
			break;
		}
	}

	if (ix == pool->capacity || ! as_cas_ptr(&pool->data[ix], val, val2)) {
		cf_pool_ptr_push(pool, val2);
		return false;
	}

	return true;
}

uint32_t
cf_pool_ptr_count(const cf_pool_ptr* pool)
{
	int32_t count = as_load_int32(&pool->count);

	return count < 0 ? 0 : (uint32_t)count;
}
