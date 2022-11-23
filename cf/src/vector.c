/*
 * vector.c
 *
 * Copyright (C) 2008-2022 Aerospike, Inc.
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

#include "vector.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

// Private flags.
#define VECTOR_FLAG_FREE_SELF 0x10
#define VECTOR_FLAG_FREE_ELES 0x20


//==========================================================
// Forward declarations.
//

static void append_lockfree(cf_vector* v, const void* ele);
static void increase_capacity(cf_vector* v, uint32_t new_capacity);


//==========================================================
// Inlines & macros.
//

static inline void
vector_lock(const cf_vector* v)
{
	if ((v->flags & VECTOR_FLAG_BIGLOCK) != 0) {
		cf_mutex_lock(&((cf_vector*)v)->lock);
	}
}

static inline void
vector_unlock(const cf_vector* v)
{
	if ((v->flags & VECTOR_FLAG_BIGLOCK) != 0) {
		cf_mutex_unlock(&((cf_vector*)v)->lock);
	}
}


//==========================================================
// Public API.
//

cf_vector*
cf_vector_create(uint32_t ele_sz, uint32_t capacity, uint32_t flags)
{
	cf_vector* v = cf_malloc(sizeof(cf_vector));

	cf_vector_init(v, ele_sz, capacity, flags | VECTOR_FLAG_FREE_SELF);

	return v;
}

void
cf_vector_init(cf_vector* v, uint32_t ele_sz, uint32_t capacity, uint32_t flags)
{
	uint8_t* buf = capacity != 0 ? cf_malloc(capacity * ele_sz) : NULL;

	cf_vector_init_with_buf(v, ele_sz, capacity, buf,
			flags | VECTOR_FLAG_FREE_ELES);
}

void
cf_vector_init_with_buf(cf_vector* v, uint32_t ele_sz, uint32_t capacity,
		uint8_t* buf, uint32_t flags)
{
	v->eles = buf;
	v->ele_sz = ele_sz;
	v->capacity = capacity;
	v->count = 0;
	v->flags = flags;

	if ((flags & VECTOR_FLAG_INITZERO) != 0 && v->eles != NULL) {
		memset(v->eles, 0, capacity * ele_sz);
	}

	if ((flags & VECTOR_FLAG_BIGLOCK) != 0) {
		cf_mutex_init(&v->lock);
	}
}

void
cf_vector_destroy(cf_vector* v)
{
	if ((v->flags & VECTOR_FLAG_BIGLOCK) != 0) {
		cf_mutex_destroy(&v->lock);
	}

	if (v->eles != NULL && (v->flags & VECTOR_FLAG_FREE_ELES) != 0) {
		cf_free(v->eles);
	}

	if ((v->flags & VECTOR_FLAG_FREE_SELF) != 0) {
		cf_free(v);
	}
}

void
cf_vector_append(cf_vector* v, const void* ele)
{
	vector_lock(v);

	append_lockfree(v, ele);

	vector_unlock(v);
}

void
cf_vector_append_unique(cf_vector* v, const void* ele)
{
	vector_lock(v);

	uint8_t* p = v->eles;
	uint32_t ele_sz = v->ele_sz;

	for (uint32_t i = 0; i < v->count; i++) {
		if (memcmp(ele, p, ele_sz) == 0) {
			vector_unlock(v);
			return; // found - for now not distinguished from insert
		}

		p += ele_sz;
	}

	append_lockfree(v, ele);

	vector_unlock(v);
}

int
cf_vector_set(cf_vector* v, uint32_t ix, const void* ele)
{
	vector_lock(v);

	if (ix >= v->capacity) {
		vector_unlock(v);
		return -1;
	}

	memcpy(v->eles + (ix * v->ele_sz), ele, v->ele_sz);

	if (ix >= v->count) {
		v->count = ix + 1;
	}

	vector_unlock(v);

	return 0;
}

int
cf_vector_get(const cf_vector* v, uint32_t ix, void* ele)
{
	vector_lock(v);

	if (ix >= v->capacity) {
		vector_unlock(v);
		return -1;
	}

	memcpy(ele, v->eles + (ix * v->ele_sz), v->ele_sz);

	vector_unlock(v);

	return 0;
}

void*
cf_vector_getp(cf_vector* v, uint32_t ix)
{
	vector_lock(v);

	if (ix >= v->capacity) {
		vector_unlock(v);
		return NULL;
	}

	void* ele = v->eles + (ix * v->ele_sz);

	vector_unlock(v);

	return ele;
}

int
cf_vector_pop(cf_vector* v, void* ele)
{
	vector_lock(v);

	if (v->count == 0) {
		vector_unlock(v);
		return -1;
	}

	v->count--;
	memcpy(ele, v->eles + (v->count * v->ele_sz), v->ele_sz);

	vector_unlock(v);

	return 0;
}

int
cf_vector_delete(cf_vector* v, uint32_t ix)
{
	vector_lock(v);

	if (ix >= v->count) {
		vector_unlock(v);
		return -1;
	}

	if (ix != v->count - 1) {
		memmove(v->eles + (ix * v->ele_sz),
				v->eles + ((ix + 1) * v->ele_sz),
				(v->count - (ix + 1)) * v->ele_sz );
	}

	v->count--;

	vector_unlock(v);

	return 0;
}

// Inclusive-exclusive, e.g. start 0 & end 3 removes indexes 0,1,2.
int
cf_vector_delete_range(cf_vector* v, uint32_t start, uint32_t end)
{
	if (start >= end) {
		return -1;
	}

	vector_lock(v);

	if (start >= v->count || end > v->count) {
		vector_unlock(v);
		return -1;
	}

	// Copy down if not at end.
	if (end != v->count) {
		memmove(v->eles + (start * v->ele_sz),
				v->eles + ((end) * v->ele_sz),
				(v->count - end) * v->ele_sz );
	}

	v->count -= end - start;

	vector_unlock(v);

	return 0;
}

void
cf_vector_clear(cf_vector* v)
{
	vector_lock(v);

	v->count = 0;

	if ((v->flags & VECTOR_FLAG_INITZERO) != 0) {
		memset(v->eles, 0, v->capacity * v->ele_sz);
	}

	vector_unlock(v);
}


//==========================================================
// Local helpers.
//

static void
append_lockfree(cf_vector* v, const void* ele)
{
	if (v->count >= v->capacity) {
		increase_capacity(v, v->count * 2);
	}

	memcpy(v->eles + (v->count * v->ele_sz), ele, v->ele_sz);
	v->count++;
}

static void
increase_capacity(cf_vector* v, uint32_t new_capacity)
{
	if (new_capacity == 0) {
		new_capacity = 2;
	}

	uint8_t* p;

	if (v->eles == NULL || (v->flags & VECTOR_FLAG_FREE_ELES) == 0) {
		p = cf_malloc(new_capacity * v->ele_sz);

		if (v->eles != NULL) {
			memcpy(p, v->eles, v->capacity * v->ele_sz);
		}

		v->flags |= VECTOR_FLAG_FREE_ELES;
	}
	else {
		p = cf_realloc(v->eles, new_capacity * v->ele_sz);
	}

	v->eles = p;

	if ((v->flags & VECTOR_FLAG_INITZERO) != 0) {
		memset(v->eles + (v->capacity * v->ele_sz), 0,
				(new_capacity - v->capacity) * v->ele_sz);
	}

	v->capacity = new_capacity;
}
