/*
 * shash.c
 *
 * Copyright (C) 2017-2020 Aerospike, Inc.
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

#include "shash.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "log.h"

//==========================================================
// Public API.
//

void
cf_shash_init(cf_shash* h, cf_shash_hash_fn h_fn, uint32_t key_size,
		uint32_t value_size, uint32_t n_buckets, bool thread_safe)
{
	hash_table_init(h, h_fn, NULL, NULL, key_size, value_size, n_buckets,
			(thread_safe ? HASH_TABLE_FLAG_THREAD_SAFE : 0) |
					HASH_TABLE_FLAG_SLOW_SHARE_FOR_FAST_SIZE);
}

cf_shash*
cf_shash_create(cf_shash_hash_fn h_fn, uint32_t key_size, uint32_t value_size,
		uint32_t n_buckets, bool thread_safe)
{
	cf_assert(h_fn != NULL && key_size != 0 && n_buckets != 0, CF_MISC,
			"bad param");
	// Note - value_size 0 works, and is used.

	return hash_table_create(h_fn, NULL, NULL, key_size, value_size, n_buckets,
			(thread_safe ? HASH_TABLE_FLAG_THREAD_SAFE : 0) |
					HASH_TABLE_FLAG_SLOW_SHARE_FOR_FAST_SIZE);
}

void
cf_shash_destroy(cf_shash* h)
{
	hash_table_destroy(h);
}
