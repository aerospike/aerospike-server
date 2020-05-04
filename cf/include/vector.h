/*
 * vector.h
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include "cf_mutex.h"


//==========================================================
// Typedefs & constants.
//

// Public flags.
#define VECTOR_FLAG_BIGLOCK  0x01
#define VECTOR_FLAG_INITZERO 0x02 // vector elements start cleared to 0

// Return this to delete element during reduce.
#define VECTOR_REDUCE_DELETE 1

// Private data.
typedef struct cf_vector_s {
	uint8_t* eles;
	uint32_t ele_sz;
	uint32_t capacity; // number of elements currently allocated
	uint32_t count; // number of elements in table, largest element set
	uint32_t flags;
	cf_mutex lock; // mutable
} cf_vector;


//==========================================================
// Public API.
//

cf_vector* cf_vector_create(uint32_t ele_sz, uint32_t capacity, uint32_t flags);
int cf_vector_init(cf_vector* v, uint32_t ele_sz, uint32_t capacity, uint32_t flags);
void cf_vector_init_with_buf(cf_vector* v, uint32_t ele_sz, uint32_t capacity, uint8_t* buf, uint32_t flags);
void cf_vector_destroy(cf_vector* v);

// Deprecated - use cf_vector_init_with_buf().
void cf_vector_init_smalloc(cf_vector* v, uint32_t ele_sz, uint8_t* sbuf, uint32_t sbuf_sz, uint32_t flags);

int cf_vector_append(cf_vector* v, const void* ele);
int cf_vector_append_unique(cf_vector* v, const void* ele);
int cf_vector_set(cf_vector* v, uint32_t ix, const void* ele);

int cf_vector_get(const cf_vector* v, uint32_t ix, void* ele);
void* cf_vector_getp(cf_vector* v, uint32_t ele);

int cf_vector_pop(cf_vector* v, void* ele);

int cf_vector_delete(cf_vector* v, uint32_t ele);
int cf_vector_delete_range(cf_vector* v, uint32_t start, uint32_t end);
void cf_vector_clear(cf_vector* v);

static inline uint32_t
cf_vector_size(const cf_vector* v)
{
	return v->count;
}

static inline uint32_t
cf_vector_element_size(const cf_vector* v)
{
	return v->ele_sz;
}

#define cf_vector_inita(_v, _ele_sz, _ele_cnt, _flags) \
		cf_vector_init_with_buf(_v, _ele_sz, _ele_cnt, alloca((_ele_sz) * (_ele_cnt)), _flags);

#define cf_vector_define(_v, _ele_sz, _ele_cnt, _flags) \
		cf_vector _v; \
		uint8_t _v ## __mem[(_ele_sz) * (_ele_cnt)]; \
		cf_vector_init_with_buf(&_v, _ele_sz, _ele_cnt, _v ## __mem, _flags);

// Deprecated - use cf_vector_element_size().
#define VECTOR_ELEM_SZ(_v) ( _v->ele_sz )


//==========================================================
// Public API - wrapper for vector of pointers.
//

static inline int
cf_vector_set_ptr(cf_vector* v, uint32_t ix, const void* ptr)
{
	return cf_vector_set(v, ix, &ptr);
}

static inline void*
cf_vector_get_ptr(const cf_vector* v, uint32_t ix)
{
	void* p = NULL;

	cf_vector_get(v, ix, &p);

	return p;
}

static inline int
cf_vector_append_ptr(cf_vector* v, const void* ptr)
{
	return cf_vector_append(v, &ptr);
}
