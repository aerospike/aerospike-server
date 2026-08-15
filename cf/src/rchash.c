/*
 * rchash.c
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

//==========================================================
// Includes.
//

#include "rchash.h"

#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

//==========================================================
// Typedefs & constants.
//

// No SLOW_SHARE_FOR_FAST_SIZE (unlike shash): a shared element counter would be
// written by every thread on every modify, reintroducing the hot cache line
// this design eliminates. cf_rchash_get_size() is therefore O(n_buckets).
#define RCHASH_FLAGS                                                           \
	(HASH_TABLE_FLAG_THREAD_SAFE | HASH_TABLE_FLAG_ALIGN_VALUE |               \
			HASH_TABLE_FLAG_REDUCE_PTR)

//==========================================================
// Forward declarations.
//

static void rel_object(cf_rchash* h, void* object);

//==========================================================
// Public API.
//

void
cf_rchash_init(cf_rchash* h, cf_rchash_hash_fn h_fn,
		cf_rchash_destructor_fn d_fn, uint32_t key_size, uint32_t n_buckets)
{
	hash_table_init(h, h_fn, d_fn, rel_object, key_size, sizeof(void*),
			n_buckets, RCHASH_FLAGS);
}

cf_rchash*
cf_rchash_create(cf_rchash_hash_fn h_fn, cf_rchash_destructor_fn d_fn,
		uint32_t key_size, uint32_t n_buckets)
{
	return hash_table_create(h_fn, d_fn, rel_object, key_size, sizeof(void*),
			n_buckets, RCHASH_FLAGS);
}

void
cf_rchash_destroy(cf_rchash* h)
{
	hash_table_destroy(h);
}

//==========================================================
// Local helpers.
//

static void
rel_object(cf_rchash* h, void* val)
{
	void* object = *((void**)val);

	if (cf_rc_release(object) == 0) {
		if (h->d_fn != NULL) {
			(h->d_fn)(object);
		}

		cf_rc_free(object);
	}
}
