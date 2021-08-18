/*
 * pool.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

typedef struct cf_pool_int32_s {
	uint32_t capacity;
	uint32_t cap_minus_1;

	uint32_t read_ix;
	uint32_t write_ix;

	int32_t count;

	int32_t empty_val;
	int32_t* data;
} cf_pool_int32;

typedef struct cf_pool_ptr_s {
	uint32_t capacity;
	uint32_t cap_minus_1;

	uint32_t read_ix;
	uint32_t write_ix;

	int32_t count;

	void** data;
} cf_pool_ptr;


//==========================================================
// Public API - cf_pool_int32.
//

void cf_pool_int32_init(cf_pool_int32* pool, uint32_t capacity, int32_t empty_val);
void cf_pool_int32_destroy(cf_pool_int32* pool);

int32_t cf_pool_int32_pop(cf_pool_int32* pool);
void cf_pool_int32_push(cf_pool_int32* pool, int32_t val);


//==========================================================
// Public API - cf_pool_ptr.
//

void cf_pool_ptr_init(cf_pool_ptr* pool, uint32_t capacity);
void cf_pool_ptr_destroy(cf_pool_ptr* pool);

void* cf_pool_ptr_pop(cf_pool_ptr* pool);
void cf_pool_ptr_push(cf_pool_ptr* pool, void* val);

bool cf_pool_ptr_remove(cf_pool_ptr* pool, void* val);
uint32_t cf_pool_ptr_count(const cf_pool_ptr* pool);
