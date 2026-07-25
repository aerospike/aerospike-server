/*
 * dynbuf.c
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

#include "dynbuf.h"

#include <citrusleaf/alloc.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "log.h"

static bool dynmem_grow(dynmem* dm);

#define MAX_BACKOFF (1024 * 256)
#define MAX_FORMAT 100

size_t
get_new_size(int alloc, int used, int requested)
{
	if (alloc - used > requested) {
		return alloc;
	}

	size_t new_sz = alloc + requested + sizeof(cf_buf_builder);
	int backoff;

	if (new_sz < 1024 * 8) {
		backoff = 1024;
	}
	else if (new_sz < 1024 * 32) {
		backoff = 1024 * 4;
	}
	else if (new_sz < 1024 * 128) {
		backoff = 1024 * 32;
	}
	else {
		backoff = MAX_BACKOFF;
	}

	return new_sz + (backoff - (new_sz % backoff));
}

void
cf_dyn_buf_reserve_internal(cf_dyn_buf* db, size_t sz)
{
	size_t new_sz = get_new_size(db->alloc_sz, db->used_sz, sz);

	if (new_sz > db->alloc_sz) {
		uint8_t* _t;

		if (db->is_stack) {
			_t = cf_malloc(new_sz);
			memcpy(_t, db->buf, db->used_sz);
			db->is_stack = false;
		}
		else {
			_t = cf_realloc(db->buf, new_sz);
		}

		db->buf = _t;
		db->alloc_sz = new_sz;
	}
}

#define DB_RESERVE(_n)                                                         \
	if (db->alloc_sz - db->used_sz < _n) {                                     \
		cf_dyn_buf_reserve_internal(db, _n);                                   \
	}

void
cf_dyn_buf_init_heap(cf_dyn_buf* db, size_t sz)
{
	db->buf = cf_malloc(sz);
	db->is_stack = false;
	db->alloc_sz = sz;
	db->used_sz = 0;
}

void
cf_dyn_buf_reserve(cf_dyn_buf* db, size_t sz, uint8_t** from)
{
	DB_RESERVE(sz);

	if (from) {
		*from = &db->buf[db->used_sz];
	}

	db->used_sz += sz;
}

void
cf_dyn_buf_append_buf(cf_dyn_buf* db, const uint8_t* buf, size_t sz)
{
	DB_RESERVE(sz);
	memcpy(&db->buf[db->used_sz], buf, sz);
	db->used_sz += sz;
}

void
cf_dyn_buf_append_string(cf_dyn_buf* db, const char* s)
{
	size_t len = strlen(s);

	DB_RESERVE(len);
	memcpy(&db->buf[db->used_sz], s, len);
	db->used_sz += len;
}

void
cf_dyn_buf_append_char(cf_dyn_buf* db, char c)
{
	DB_RESERVE(1);
	db->buf[db->used_sz] = (uint8_t)c;
	db->used_sz++;
}

void
cf_dyn_buf_append_bool(cf_dyn_buf* db, bool b)
{
	if (b) {
		DB_RESERVE(4);
		memcpy(&db->buf[db->used_sz], "true", 4);
		db->used_sz += 4;
	}
	else {
		DB_RESERVE(5);
		memcpy(&db->buf[db->used_sz], "false", 5);
		db->used_sz += 5;
	}
}

void
cf_dyn_buf_append_int(cf_dyn_buf* db, int i)
{
	DB_RESERVE(12);
	db->used_sz += sprintf((char*)&db->buf[db->used_sz], "%d", i);
}

void
cf_dyn_buf_append_uint64_x(cf_dyn_buf* db, uint64_t i)
{
	DB_RESERVE(18);
	db->used_sz += sprintf((char*)&db->buf[db->used_sz], "%lX", i);
}

void
cf_dyn_buf_append_uint64(cf_dyn_buf* db, uint64_t i)
{
	DB_RESERVE(22);
	db->used_sz += sprintf((char*)&db->buf[db->used_sz], "%lu", i);
}

void
cf_dyn_buf_append_uint32(cf_dyn_buf* db, uint32_t i)
{
	DB_RESERVE(12);
	db->used_sz += sprintf((char*)&db->buf[db->used_sz], "%u", i);
}

void
cf_dyn_buf_append_format_va(cf_dyn_buf* db, const char* form, va_list va)
{
	DB_RESERVE(MAX_FORMAT + 1);
	int32_t len =
			vsnprintf((char*)&db->buf[db->used_sz], MAX_FORMAT + 1, form, va);

	if (len > MAX_FORMAT) {
		len = MAX_FORMAT;
	}

	db->used_sz += len;
}

void
cf_dyn_buf_append_format(cf_dyn_buf* db, const char* form, ...)
{
	va_list va;
	va_start(va, form);

	cf_dyn_buf_append_format_va(db, form, va);

	va_end(va);
}

void
cf_dyn_buf_chomp(cf_dyn_buf* db)
{
	if (db->used_sz > 0) {
		db->used_sz--;
	}
}

void
cf_dyn_buf_chomp_char(cf_dyn_buf* db, char c)
{
	if (db->used_sz > 0 && db->buf[db->used_sz - 1] == (uint8_t)c) {
		db->used_sz--;
	}
}

char*
cf_dyn_buf_strdup(cf_dyn_buf* db)
{
	if (db->used_sz == 0) {
		return NULL;
	}

	char* s = cf_malloc(db->used_sz + 1);

	memcpy(s, db->buf, db->used_sz);
	s[db->used_sz] = 0;

	return s;
}

void
cf_dyn_buf_free(cf_dyn_buf* db)
{
	if (! db->is_stack && db->buf) {
		cf_free(db->buf);
	}
}

// Helpers to append name value pairs to a cf_dyn_buf in pattern: name=value;

void
info_append_bool(cf_dyn_buf* db, const char* name, bool value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_bool(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_int(cf_dyn_buf* db, const char* name, int value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_int(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_string(cf_dyn_buf* db, const char* name, const char* value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_string(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_string_safe(cf_dyn_buf* db, const char* name, const char* value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_string(db, value ? value : "null");
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint32(cf_dyn_buf* db, const char* name, uint32_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint32(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint64(cf_dyn_buf* db, const char* name, uint64_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint64(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint64_x(cf_dyn_buf* db, const char* name, uint64_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint64_x(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_format(cf_dyn_buf* db, const char* name, const char* form, ...)
{
	va_list va;
	va_start(va, form);

	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_format_va(db, form, va);
	cf_dyn_buf_append_char(db, ';');

	va_end(va);
}

static inline void
append_indexed_name(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '[');
	cf_dyn_buf_append_uint32(db, ix);
	cf_dyn_buf_append_char(db, ']');

	if (attr) {
		cf_dyn_buf_append_char(db, '.');
		cf_dyn_buf_append_string(db, attr);
	}

	cf_dyn_buf_append_char(db, '=');
}

void
info_append_indexed_string(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, const char* value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_string(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_int(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, int value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_int(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_uint32(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, uint32_t value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_uint32(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_uint64(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, uint64_t value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_uint64(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
cf_buf_builder_reserve_internal(cf_buf_builder** bb_r, size_t sz)
{
	cf_buf_builder* bb = *bb_r;
	size_t new_sz = get_new_size(bb->alloc_sz, bb->used_sz, sz);

	if (new_sz > bb->alloc_sz) {
		if (bb->alloc_sz - bb->used_sz < MAX_BACKOFF) {
			bb = cf_realloc(bb, new_sz);
		}
		else {
			// Only possible if buffer was reset. Avoids potential expensive
			// copy within realloc.
			cf_buf_builder* _t = cf_malloc(new_sz);

			memcpy(_t->buf, bb->buf, bb->used_sz);
			_t->used_sz = bb->used_sz;
			cf_free(bb);
			bb = _t;
		}

		bb->alloc_sz = new_sz - sizeof(cf_buf_builder);
		*bb_r = bb;
	}
}

#define BB_RESERVE(_n)                                                         \
	if ((*bb_r)->alloc_sz - (*bb_r)->used_sz < _n) {                           \
		cf_buf_builder_reserve_internal(bb_r, _n);                             \
	}

void
cf_buf_builder_reserve(cf_buf_builder** bb_r, int sz, uint8_t** buf)
{
	BB_RESERVE(sz);
	cf_buf_builder* bb = *bb_r;

	if (buf) {
		*buf = &bb->buf[bb->used_sz];
	}

	bb->used_sz += sz;
}

cf_buf_builder*
cf_buf_builder_create(size_t sz)
{
	size_t malloc_sz = (sz < 1024) ? 1024 : sz;
	cf_buf_builder* bb = cf_malloc(malloc_sz);

	bb->alloc_sz = malloc_sz - sizeof(cf_buf_builder);
	bb->used_sz = 0;

	return bb;
}

void
cf_buf_builder_free(cf_buf_builder* bb)
{
	cf_free(bb);
}

void
cf_buf_builder_reset(cf_buf_builder* bb)
{
	bb->used_sz = 0;
}

// TODO - We've only implemented a few cf_ll_buf methods for now. We'll add more
// functionality if and when it's needed.

void
cf_ll_buf_init_heap(cf_ll_buf* llb, size_t buf_sz)
{
	cf_ll_buf_stage* stage = cf_malloc(sizeof(cf_ll_buf_stage) + buf_sz);

	*stage = (cf_ll_buf_stage){ .buf_sz = buf_sz };
	*llb = (cf_ll_buf){ .head = stage, .tail = stage };
}

void
cf_ll_buf_grow(cf_ll_buf* llb, size_t sz)
{
	size_t buf_sz = sz > llb->head->buf_sz ? sz : llb->head->buf_sz;
	cf_ll_buf_stage* new_tail = cf_malloc(sizeof(cf_ll_buf_stage) + buf_sz);

	new_tail->next = NULL;
	new_tail->buf_sz = buf_sz;
	new_tail->used_sz = 0;

	llb->tail->next = new_tail;
	llb->tail = new_tail;
}

#define LLB_RESERVE(_n)                                                        \
	if (_n > llb->tail->buf_sz - llb->tail->used_sz) {                         \
		cf_ll_buf_grow(llb, _n);                                               \
	}

void
cf_ll_buf_reserve(cf_ll_buf* llb, size_t sz, uint8_t** from)
{
	LLB_RESERVE(sz);

	if (from) {
		*from = llb->tail->buf + llb->tail->used_sz;
	}

	llb->tail->used_sz += sz;
}

void
cf_ll_buf_free(cf_ll_buf* llb)
{
	cf_ll_buf_stage* cur = llb->head_is_stack ? llb->head->next : llb->head;

	while (cur) {
		cf_ll_buf_stage* temp = cur;

		cur = cur->next;
		cf_free(temp);
	}
}

void
dynmem_init(dynmem* dm, uint32_t obj_sz, uint32_t n_obj, void* stack_mem)
{
	cf_assert(obj_sz > 0, CF_MISC, "obj_sz %u must be greater than 0", obj_sz);
	cf_assert(n_obj >= 8, CF_MISC, "n_obj %u < 8", n_obj);
	cf_assert((n_obj & (n_obj - 1)) == 0, CF_MISC,
			"n_obj %u must be a power of 2", n_obj);

	if (__builtin_mul_overflow(n_obj, obj_sz, &dm->size)) {
		cf_crash(CF_MISC, "overflow: n_obj %u * obj_sz %u", n_obj, obj_sz);
	}

	dm->obj_sz = obj_sz;
	dm->alloc_idx = 0;
	dm->n_mem = 1;
	dm->shift0 = (uint8_t)(31 - __builtin_clz(n_obj));
	dm->flags = (stack_mem == NULL) ? 0 : DYNMEM_FLAG_0_ON_STACK;
	dm->pad = 0;
	dm->mem[0] = stack_mem;

	if (stack_mem == NULL) {
		dm->mem[0] = cf_malloc(dm->size);
	}
}

void*
dynmem_at(dynmem* dm, dynmem_obj_idx index)
{
	const dynmem* cdm =
			(const dynmem*)dm; // ensure dm doesn't change for dynmem_get()
	const uint8_t mem_i = dynmem_get_buf_idx(cdm, index);
	const uint32_t sub = (mem_i == 0 ? 0 : (1U << (mem_i - 1)) << cdm->shift0);
	const uint32_t offset = index - sub;

	cf_assert(index >= sub, CF_MISC, "index %u < sub %u", index, sub);

	if (mem_i >= cdm->n_mem) {
		return NULL;
	}

	uint32_t byte_offset;

	if (__builtin_mul_overflow(offset, cdm->obj_sz, &byte_offset)) {
		cf_crash(CF_MISC, "overflow");
	}

	return (void*)(dm->mem[mem_i] + byte_offset);
}

const void*
dynmem_get(const dynmem* dm, dynmem_obj_idx index)
{
	return (const void*)dynmem_at((dynmem*)dm, index);
}

void*
dynmem_reserve(dynmem* dm, dynmem_obj_idx* index_r)
{
	return dynmem_reserve_n(dm, 1, index_r);
}

void*
dynmem_reserve_n(dynmem* dm, uint32_t n, dynmem_obj_idx* index_r)
{
	uint32_t index = dm->alloc_idx;

	while (index + n > dm->size / dm->obj_sz) {
		if (! dynmem_grow(dm)) {
			return NULL; // overflow
		}
	}

	dm->alloc_idx += n;

	if (index_r) {
		*index_r = index;
	}

	return dynmem_at(dm, index);
}

bool
dynmem_set(dynmem* dm, const dynmem_obj_idx index, const void* buf)
{
	void* dst = dynmem_at(dm, index);

	if (dst == NULL || buf == NULL) {
		return false;
	}

	memcpy(dst, buf, dm->obj_sz);

	return true;
}

void
dynmem_destroy(dynmem* dm)
{
	uint8_t start = (dm->flags & DYNMEM_FLAG_0_ON_STACK) ? 1 : 0;

	for (uint8_t i = start; i < dm->n_mem; i++) {
		if (dm->mem[i]) {
			cf_free(dm->mem[i]);
			dm->mem[i] = NULL;
		}
	}

	dm->n_mem = 0;
	dm->size = 0;
	dm->alloc_idx = 0;
}

static inline uint32_t
dynmem_n_objs(const dynmem* dm, dynmem_buf_idx buf_idx)
{
	cf_assert(buf_idx < dm->n_mem, CF_MISC, "buf_idx %u >= n_mem %u", buf_idx,
			dm->n_mem);

	uint32_t n_obj = 1U << dm->shift0;

	if (buf_idx == 0) {
		return n_obj;
	}

	return n_obj << (buf_idx - 1);
}

dynmem_obj_idx
dynmem_get_obj_idx(const dynmem* dm, dynmem_buf_idx buf_idx, void* obj)
{
	uint32_t n_obj_at_idx = dynmem_n_objs(dm, buf_idx);
	uint32_t sz = n_obj_at_idx * dm->obj_sz;

	cf_assert((uintptr_t)obj >= (uintptr_t)dm->mem[buf_idx], CF_MISC,
			"obj %p < dm->mem[%u] %p", obj, buf_idx, dm->mem[buf_idx]);
	cf_assert((uint8_t*)obj < dm->mem[buf_idx] + sz, CF_MISC,
			"obj %p is out of bounds of dm->mem[%u] %p + %u", obj, buf_idx,
			dm->mem[buf_idx], sz);
	return (dynmem_obj_idx)(((uint8_t*)obj - dm->mem[buf_idx]) / dm->obj_sz);
}

static bool
dynmem_grow(dynmem* dm)
{
	uint32_t new_sz;

	if (dm->n_mem >= 30 || __builtin_add_overflow(dm->size, dm->size, &new_sz)) {
		return false;
	}

	dm->mem[dm->n_mem++] = cf_malloc(dm->size);
	dm->size = new_sz;

	return true;
}
