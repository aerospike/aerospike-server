/*
 * hash_table.c
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

//==========================================================
// Includes.
//

#include "hash_table.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"

#include "cf_mutex.h"
#include "hardware.h"
#include "log.h"

//==========================================================
// Typedefs & constants.
//

typedef struct linked_ele_s {
	struct linked_ele_s* next;
	uint8_t data[];
} linked_ele;

typedef struct hash_line_s {
	linked_ele* next;
	cf_mutex lock;
	uint32_t n;
	uint8_t data[];
} hash_line;

#define ROUNDUP_MUL_POW2(x, mul) (((x) + (mul) - 1) & ~((mul) - 1))

//==========================================================
// Forward declarations.
//

static inline bool is_key_aligned(hash_table_flags flags);
static inline bool is_value_aligned(hash_table_flags flags);
static inline bool is_only_value_aligned(hash_table_flags flags);
static inline bool is_fast_size(hash_table_flags flags);

static inline const void* h_get_key(const hash_table* h, const uint8_t* data);
static inline void* h_get_value(const hash_table* h, uint8_t* data);
static inline void* h_get_value_ptr(const hash_table* h, uint8_t* data);
static inline void h_release(hash_table* h, uint8_t* data);
static inline void h_destroy_all(hash_table* h);

static inline uint32_t h_get_bucket_idx(const hash_table* h, const void* key);
static inline cf_mutex* h_lock(hash_table* h, hash_line* line);
static inline void h_unlock(cf_mutex* m);
static inline hash_line* h_get_bucket_line(const hash_table* h, uint32_t idx);
static inline void h_fill_element(hash_table* h, void* data, const void* key,
		const void* val);
static inline uint8_t* h_find_data_by_key(hash_table* h, hash_line* line,
		const void* key, linked_ele*** e_r, bool insert, bool err_on_found);

static inline bool h_line_delete_element(hash_table* h, hash_line* line,
		uint8_t* data, linked_ele** ppe);

//==========================================================
// Public API - useful hash functions.
//

uint32_t
hash_table_fn_u32(const void* key)
{
	return *(const uint32_t*)key;
}

uint32_t
hash_table_fn_ptr(const void* key)
{
	return cf_hash_ptr32(key);
}

uint32_t
hash_table_fn_zstr(const void* key)
{
	return cf_wyhash32((const uint8_t*)key, strlen(key));
}

//==========================================================
// Public API.
//

void
hash_table_init(hash_table* h, hash_table_hash_fn h_fn,
		hash_table_destructor_fn d_fn, hash_table_rel_fn rel_fn, uint32_t key_sz,
		uint32_t val_sz, uint32_t n_buckets, hash_table_flags flags)
{
	cf_assert(h_fn != NULL && key_sz != 0 && n_buckets != 0, CF_MISC,
			"bad param");
	cf_assert(key_sz <= UINT16_MAX && val_sz <= UINT16_MAX, CF_MISC, "bad param");

	h->h_fn = h_fn;
	h->d_fn = d_fn;
	h->rel_fn = rel_fn;
	h->key_sz = key_sz;
	h->val_sz = val_sz;
	h->n_elements = 0;

	if (is_only_value_aligned(flags)) {
		cf_assert(val_sz != 0, CF_MISC, "bad param");
		h->key_off = val_sz;
		h->val_off = 0;
		h->data_sz = ROUNDUP_MUL_POW2(val_sz + key_sz, sizeof(void*));
		h->linked_data_sz = h->data_sz;
	}
	else {
		h->key_off = 0;
		h->val_off = key_sz;

		if (is_value_aligned(flags)) {
			h->val_off = ROUNDUP_MUL_POW2(h->val_off, sizeof(void*));
		}

		h->data_sz = h->val_off + h->val_sz;
		h->linked_data_sz = h->data_sz;

		if (is_key_aligned(flags)) {
			h->data_sz = ROUNDUP_MUL_POW2(h->data_sz, sizeof(void*));
		}
	}

	h->line_sz = sizeof(hash_line) + h->data_sz;
	h->line_sz = ROUNDUP_MUL_POW2(h->line_sz, CF_CACHELINE_SZ);
	cf_assert(h->line_sz <= UINT16_MAX, CF_MISC, "line_sz too large");

	h->n_buckets = n_buckets;

	size_t table_sz = (size_t)n_buckets * h->line_sz;

	h->table = cf_aligned_alloc(CF_CACHELINE_SZ, table_sz);
	memset(h->table, 0, table_sz);
	h->n_line_max_ele = (h->line_sz - sizeof(hash_line)) / h->data_sz;
	h->flags = flags;

	// NOTE: memset should have taken care of all mutex inits.
}

hash_table*
hash_table_create(hash_table_hash_fn h_fn, hash_table_destructor_fn d_fn,
		hash_table_rel_fn rel_fn, uint32_t key_sz, uint32_t ele_sz,
		uint32_t n_buckets, hash_table_flags flags)
{
	hash_table* h = cf_aligned_alloc(CF_CACHELINE_SZ, sizeof(hash_table));

	hash_table_init(h, h_fn, d_fn, rel_fn, key_sz, ele_sz, n_buckets, flags);
	h->flags |= HASH_TABLE_FLAG_WAS_MALLOCED;

	return h;
}

void
hash_table_destroy(hash_table* h)
{
	cf_assert(h != NULL, CF_MISC, "bad param");

	h_destroy_all(h);

	// NOTE: cf_mutex currently does not need destruction cleanup.

	cf_free(h->table);

	if ((h->flags & HASH_TABLE_FLAG_WAS_MALLOCED) != 0) {
		cf_free(h);
	}
}

void
hash_table_clear(hash_table* h)
{
	cf_assert(h != NULL, CF_MISC, "bad param");
	h_destroy_all(h);
}

uint32_t
hash_table_get_size(const hash_table* h)
{
	cf_assert(h != NULL, CF_MISC, "bad param");

	if (is_fast_size(h->flags)) {
		return h->n_elements;
	}

	uint32_t count = 0;

	for (uint32_t i = 0; i < h->n_buckets; i++) {
		const hash_line* line = h_get_bucket_line(h, i);

		count += line->n;
	}

	return count;
}

void
hash_table_put(hash_table* h, const void* key, const void* val)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");
	cf_assert(val != NULL || h->val_sz == 0, CF_MISC, "invalid val param %p",
			val);

	uint32_t idx = h_get_bucket_idx(h, key);
	hash_line* line = h_get_bucket_line(h, idx);
	cf_mutex* m = h_lock(h, line);

	// Handle common case first.
	if (line->n == 0) {
		h_fill_element(h, line->data, key, val);
		line->n++;

		if (is_fast_size(h->flags)) {
			as_incr_uint32(&h->n_elements);
		}

		h_unlock(m);
		return;
	}

	uint32_t old_n = line->n;
	uint8_t* data = h_find_data_by_key(h, line, key, NULL, true, false);

	// Unchanged n means we found an existing element - release its old value
	// before overwriting.
	if (old_n == line->n) {
		h_release(h, data);
	}

	h_fill_element(h, data, key, val);
	h_unlock(m);
}

bool
hash_table_put_unique(hash_table* h, const void* key, void* val)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");
	cf_assert(val != NULL || h->val_sz == 0, CF_MISC, "invalid val param %p",
			val);

	uint32_t idx = h_get_bucket_idx(h, key);
	hash_line* line = h_get_bucket_line(h, idx);
	cf_mutex* m = h_lock(h, line);

	// Handle common case first.
	if (line->n == 0) {
		h_fill_element(h, line->data, key, val);
		line->n++;

		if (is_fast_size(h->flags)) {
			as_incr_uint32(&h->n_elements);
		}

		h_unlock(m);
		return true;
	}

	uint8_t* data = h_find_data_by_key(h, line, key, NULL, true, true);

	if (data == NULL) {
		h_unlock(m);
		return false;
	}

	h_fill_element(h, data, key, val);
	h_unlock(m);

	return true;
}

bool
hash_table_get(hash_table* h, const void* key, void* val_r, cf_mutex** m_r)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");

	uint32_t idx = h_get_bucket_idx(h, key);
	hash_line* line = h_get_bucket_line(h, idx);

	if (line->n == 0) {
		return false;
	}

	cf_mutex* m = h_lock(h, line);
	uint8_t* data = h_find_data_by_key(h, line, key, NULL, false, false);

	if (data == NULL) {
		h_unlock(m);
		return false;
	}

	if (val_r != NULL) {
		memcpy(val_r, h_get_value(h, data), h->val_sz);
	}

	if (m_r != NULL) {
		cf_assert(m != NULL, CF_MISC, "bad param");
		*m_r = m;
	}
	else {
		h_unlock(m);
	}

	return true;
}

bool
hash_table_get_ptr(hash_table* h, const void* key, void** ptr_r, cf_mutex** m_r)
{
	return hash_table_get(h, key, ptr_r, m_r);
}

bool
hash_table_get_direct_ptr(hash_table* h, const void* key, void** ptr_r,
		cf_mutex** m_r, bool no_lock)
{
	cf_assert(h != NULL && key != NULL && ptr_r != NULL, CF_MISC, "bad param");
	cf_assert(! no_lock || (h->flags & HASH_TABLE_FLAG_THREAD_SAFE) == 0,
			CF_MISC, "get_direct_ptr with no_lock incompatible with thread-safe");

	uint32_t idx = h_get_bucket_idx(h, key);
	hash_line* line = h_get_bucket_line(h, idx);

	if (line->n == 0) {
		return false;
	}

	cf_mutex* m = no_lock ? NULL : h_lock(h, line);
	uint8_t* data = h_find_data_by_key(h, line, key, NULL, false, false);

	if (data == NULL) {
		h_unlock(m);
		return false;
	}

	*ptr_r = h_get_value(h, data);

	if (m_r != NULL) {
		cf_assert(m != NULL, CF_MISC, "bad param");
		*m_r = m;
	}
	else {
		h_unlock(m);
	}

	return true;
}

bool
hash_table_delete(hash_table* h, const void* key, const void* val, void* val_r,
		bool no_lock)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");

	uint32_t idx = h_get_bucket_idx(h, key);
	hash_line* line = h_get_bucket_line(h, idx);

	if (line->n == 0) { // optimized case
		return false;
	}

	linked_ele** ppe;
	cf_mutex* m = no_lock ? NULL : h_lock(h, line);
	uint8_t* data = h_find_data_by_key(h, line, key, &ppe, false, false);

	if (data == NULL) {
		h_unlock(m);
		return false;
	}

	if (val != NULL) {
		cf_assert(h->val_sz != 0, CF_MISC, "bad param");
		if (memcmp(val, h_get_value(h, data), h->val_sz) != 0) {
			h_unlock(m);
			return false;
		}
	}

	if (val_r != NULL) {
		memcpy(val_r, h_get_value(h, data), h->val_sz);
	}

	h_release(h, data);
	h_line_delete_element(h, line, data, ppe);
	h_unlock(m);

	return true;
}

int
hash_table_reduce(hash_table* h, hash_table_reduce_fn reduce_fn, void* udata)
{
	cf_assert(h != NULL && reduce_fn != NULL, CF_MISC, "bad param");

	for (uint32_t i = 0; i < h->n_buckets; i++) {
		hash_line* line = h_get_bucket_line(h, i);

		if (line->n == 0) {
			continue;
		}

		cf_mutex* m = h_lock(h, line);
		uint8_t* data = line->data;
		uint32_t n = (line->n > h->n_line_max_ele) ? h->n_line_max_ele : line->n;

		for (uint32_t j = 0; j < n; j++) {
			void* val = ((h->flags & HASH_TABLE_FLAG_REDUCE_PTR) != 0)
					? h_get_value_ptr(h, data)
					: h_get_value(h, data);
			int rv = reduce_fn(h_get_key(h, data), val, udata);

			if (rv == HASH_TABLE_REDUCE_DELETE) {
				h_release(h, data);

				// Delete refills this slot with a survivor, so don't advance
				// data; if it came from the overflow list, n grows to cover it.
				if (h_line_delete_element(h, line, data, NULL)) {
					n++;
				}
			}
			else if (rv == HASH_TABLE_REDUCE_CONTINUE) {
				data += h->data_sz;
			}
			else { // caller says stop - return its value
				h_unlock(m);
				return rv;
			}
		}

		linked_ele** ppe = &line->next;

		while (*ppe != NULL) {
			void* val = ((h->flags & HASH_TABLE_FLAG_REDUCE_PTR) != 0)
					? h_get_value_ptr(h, (*ppe)->data)
					: h_get_value(h, (*ppe)->data);
			int rv = reduce_fn(h_get_key(h, (*ppe)->data), val, udata);

			if (rv == HASH_TABLE_REDUCE_DELETE) {
				h_release(h, (*ppe)->data);
				h_line_delete_element(h, line, (*ppe)->data, ppe);
			}
			else if (rv == HASH_TABLE_REDUCE_CONTINUE) {
				ppe = &(*ppe)->next;
			}
			else { // caller says stop - return its value
				h_unlock(m);
				return rv;
			}
		}

		h_unlock(m);
	}

	return HASH_TABLE_REDUCE_CONTINUE;
}

//==========================================================
// Local helpers.
//

static inline bool
is_key_aligned(hash_table_flags flags)
{
	return (flags & HASH_TABLE_FLAG_ALIGN_KEY) != 0;
}

static inline bool
is_value_aligned(hash_table_flags flags)
{
	return (flags & HASH_TABLE_FLAG_ALIGN_VALUE) != 0;
}

static inline bool
is_only_value_aligned(hash_table_flags flags)
{
	return ! is_key_aligned(flags) && is_value_aligned(flags);
}

static inline bool
is_fast_size(hash_table_flags flags)
{
	return (flags & HASH_TABLE_FLAG_SLOW_SHARE_FOR_FAST_SIZE) != 0;
}

static inline const void*
h_get_key(const hash_table* h, const uint8_t* data)
{
	return (const void*)(data + h->key_off);
}

static inline void*
h_get_value(const hash_table* h, uint8_t* data)
{
	return (void*)(data + h->val_off);
}

static inline void*
h_get_value_ptr(const hash_table* h, uint8_t* data)
{
	return *(void**)(data + h->val_off);
}

static inline void
h_release(hash_table* h, uint8_t* data)
{
	if (h->rel_fn != NULL) {
		h->rel_fn(h, h_get_value(h, data));
	}
}

// Holds line->lock across the destructor - destructors must not re-enter the
// table on the same bucket (the lock is non-recursive).
static inline void
h_destroy_all(hash_table* h)
{
	for (uint32_t i = 0; i < h->n_buckets; i++) {
		hash_line* line = h_get_bucket_line(h, i);
		cf_mutex* m = h_lock(h, line);

		if (line->n == 0) {
			h_unlock(m);
			continue;
		}

		if (h->rel_fn != NULL) {
			uint8_t* data = line->data;
			uint32_t n = (line->n > h->n_line_max_ele) ? h->n_line_max_ele
													   : line->n;

			for (uint32_t j = 0; j < n; j++) {
				h_release(h, data);
				data += h->data_sz;
			}
		}

		linked_ele* e = line->next;

		while (e != NULL) {
			linked_ele* next = e->next;

			h_release(h, e->data);
			cf_free(e);
			e = next;
		}

		if (is_fast_size(h->flags)) {
			as_faa_rlx(&h->n_elements, -line->n);
		}

		line->next = NULL;
		line->n = 0;
		h_unlock(m);
	}
}

static inline uint32_t
h_get_bucket_idx(const hash_table* h, const void* key)
{
	return h->h_fn(key) % h->n_buckets;
}

static inline cf_mutex*
h_lock(hash_table* h, hash_line* line)
{
	if ((h->flags & HASH_TABLE_FLAG_THREAD_SAFE) == 0) {
		return NULL;
	}

	cf_mutex_lock(&line->lock);

	return &line->lock;
}

static inline void
h_unlock(cf_mutex* m)
{
	if (m == NULL) {
		return;
	}

	cf_mutex_unlock(m);
}

static inline hash_line*
h_get_bucket_line(const hash_table* h, uint32_t idx)
{
	return (hash_line*)(((uint8_t*)h->table) + (h->line_sz * idx));
}

static inline void
h_fill_element(hash_table* h, void* data, const void* key, const void* val)
{
	memcpy(data + h->key_off, key, h->key_sz);
	memcpy(data + h->val_off, val, h->val_sz);
}

static inline uint8_t*
h_find_data_by_key(hash_table* h, hash_line* line, const void* key,
		linked_ele*** e_r, bool insert, bool err_on_found)
{
	if (e_r != NULL) {
		*e_r = NULL;
	}

	uint8_t* data = line->data;
	uint32_t n = (line->n > h->n_line_max_ele) ? h->n_line_max_ele : line->n;

	for (uint32_t i = 0; i < n; i++) {
		const void* h_key = h_get_key(h, data);

		if (memcmp(key, h_key, h->key_sz) == 0) {
			return err_on_found ? NULL : data;
		}

		data += h->data_sz;
	}

	linked_ele** ppe = &line->next;

	while (*ppe != NULL) {
		const void* h_key = h_get_key(h, (*ppe)->data);

		if (memcmp(key, h_key, h->key_sz) == 0) {
			if (e_r != NULL) {
				*e_r = ppe;
			}

			return err_on_found ? NULL : (*ppe)->data;
		}

		ppe = &(*ppe)->next;
	}

	// Did not find key.

	if (! insert) {
		return NULL;
	}

	line->n++;

	if (is_fast_size(h->flags)) {
		as_incr_uint32(&h->n_elements);
	}

	if (line->n <= h->n_line_max_ele) {
		return data;
	}

	linked_ele* e =
			(linked_ele*)cf_malloc(sizeof(linked_ele) + h->linked_data_sz);

	e->next = line->next;
	line->next = e;

	return e->data;
}

static inline bool
h_line_delete_element(hash_table* h, hash_line* line, uint8_t* data,
		linked_ele** ppe)
{
	bool promoted_from_overflow = false;

	if (ppe == NULL) { // element is in line data array
		if (line->next != NULL) {
			linked_ele* e = line->next;

			memcpy(data, e->data, h->linked_data_sz);
			line->next = e->next;
			promoted_from_overflow = true;
			cf_free(e);
		}
		else {
			uint32_t i = (data - line->data) / h->data_sz + 1;

			if (i < line->n) {
				memmove(data, data + h->data_sz, (line->n - i) * h->data_sz);
			}
		}
	}
	else {
		linked_ele* e = *ppe;

		*ppe = e->next;
		cf_free(e);
	}

	line->n--;

	if (is_fast_size(h->flags)) {
		as_decr_uint32(&h->n_elements);
	}

	return promoted_from_overflow;
}
