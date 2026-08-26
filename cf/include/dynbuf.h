/*
 * dynbuf.h
 *
 * Copyright (C) 2009-2022 Aerospike, Inc.
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

/*
 * A simple dynamic buffer implementation
 * Allows the first, simpler part of the buffer to be on the stack
 * which is usually all that's needed
 *
 */

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct cf_dyn_buf_s {
	uint8_t* buf;
	bool is_stack;
	size_t alloc_sz;
	size_t used_sz;
} cf_dyn_buf;

#define cf_dyn_buf_define(__x)                                                 \
	uint8_t dyn_buf##__x[1024];                                                \
	cf_dyn_buf __x = { dyn_buf##__x, true, 1024, 0 }
#define cf_dyn_buf_define_size(__x, __sz)                                      \
	uint8_t dyn_buf##__x[__sz];                                                \
	cf_dyn_buf __x = { dyn_buf##__x, true, __sz, 0 }

extern void cf_dyn_buf_init_heap(cf_dyn_buf* db, size_t sz);
extern void cf_dyn_buf_reserve(cf_dyn_buf* db, size_t sz, uint8_t** from);
extern void cf_dyn_buf_append_string(cf_dyn_buf* db, const char* s);
extern void cf_dyn_buf_append_char(cf_dyn_buf* db, char c);
extern void cf_dyn_buf_append_bool(cf_dyn_buf* db, bool b);
extern void cf_dyn_buf_append_buf(cf_dyn_buf* db, const uint8_t* buf, size_t sz);
extern void cf_dyn_buf_append_int(cf_dyn_buf* db, int i);
extern void cf_dyn_buf_append_uint64_x(cf_dyn_buf* db, uint64_t i); // HEX FORMAT!
extern void cf_dyn_buf_append_uint64(cf_dyn_buf* db, uint64_t i);
extern void cf_dyn_buf_append_uint32(cf_dyn_buf* db, uint32_t i);
extern void cf_dyn_buf_append_format_va(cf_dyn_buf* db, const char* form,
		va_list va);
extern void cf_dyn_buf_append_format(cf_dyn_buf* db, const char* form, ...);
extern void cf_dyn_buf_chomp(cf_dyn_buf* db);
extern void cf_dyn_buf_chomp_char(cf_dyn_buf* db, char c);
extern char* cf_dyn_buf_strdup(cf_dyn_buf* db);
extern void cf_dyn_buf_free(cf_dyn_buf* db);

// Helpers to append name value pairs to a cf_dyn_buf in pattern: name=value;
void info_append_bool(cf_dyn_buf* db, const char* name, bool value);
void info_append_int(cf_dyn_buf* db, const char* name, int value);
void info_append_string(cf_dyn_buf* db, const char* name, const char* value);
void info_append_string_safe(cf_dyn_buf* db, const char* name, const char* value);
void info_append_uint32(cf_dyn_buf* db, const char* name, uint32_t value);
void info_append_uint64(cf_dyn_buf* db, const char* name, uint64_t value);
void info_append_uint64_x(cf_dyn_buf* db, const char* name, uint64_t value);
void info_append_format(cf_dyn_buf* db, const char* name, const char* form, ...);

// Append indexed name with optional attribute and value: name[ix].attr=value;
void info_append_indexed_string(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, const char* value);
void info_append_indexed_int(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, int value);
void info_append_indexed_uint32(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, uint32_t value);
void info_append_indexed_uint64(cf_dyn_buf* db, const char* name, uint32_t ix,
		const char* attr, uint64_t value);

typedef struct cf_buf_builder_s {
	size_t alloc_sz;
	size_t used_sz;
	uint8_t buf[];
} cf_buf_builder;

extern cf_buf_builder* cf_buf_builder_create(size_t sz);
extern void cf_buf_builder_free(cf_buf_builder* bb);
extern void cf_buf_builder_reset(cf_buf_builder* bb);
// Reserve the bytes and give me the handle to the spot reserved:
extern void cf_buf_builder_reserve(cf_buf_builder** bb_r, int sz, uint8_t** buf);

// TODO - We've only implemented a few cf_ll_buf methods for now. We'll add more
// functionality if and when it's needed.

typedef struct cf_ll_buf_stage_s {
	struct cf_ll_buf_stage_s* next;
	size_t buf_sz;
	size_t used_sz;
	uint8_t buf[];
} cf_ll_buf_stage;

typedef struct cf_ll_buf_s {
	bool head_is_stack;
	cf_ll_buf_stage* head;
	cf_ll_buf_stage* tail;
} cf_ll_buf;

#define cf_ll_buf_define(__x, __sz)                                            \
	uint8_t llb_stage##__x[sizeof(cf_ll_buf_stage) + __sz];                    \
	cf_ll_buf_stage* ll_buf_stage##__x = (cf_ll_buf_stage*)llb_stage##__x;     \
	ll_buf_stage##__x->next = NULL;                                            \
	ll_buf_stage##__x->buf_sz = __sz;                                          \
	ll_buf_stage##__x->used_sz = 0;                                            \
	cf_ll_buf __x = { true, ll_buf_stage##__x, ll_buf_stage##__x }

extern void cf_ll_buf_init_heap(cf_ll_buf* llb, size_t buf_sz);
extern void cf_ll_buf_reserve(cf_ll_buf* llb, size_t sz, uint8_t** from);
extern void cf_ll_buf_free(cf_ll_buf* llb);

typedef uint8_t dynmem_buf_idx;
typedef uint32_t dynmem_obj_idx;

typedef struct dynmem_s {
	uint32_t size;
	uint32_t obj_sz;
	dynmem_obj_idx alloc_idx;
	dynmem_buf_idx n_mem;
	uint8_t shift0;
	uint8_t flags;
	uint8_t pad;
	uint8_t* mem[30];
} dynmem;

#define DYNMEM_FLAG_0_ON_STACK 0x01

#define define_dynobj(_name, _obj_sz, _n_obj)                                  \
	uint8_t _##_name##_mem[(_n_obj) * (_obj_sz)] __attribute__((aligned(16))); \
	dynmem _name;                                                              \
	dynmem_init(&_name, (_obj_sz), (_n_obj), (_##_name##_mem))

#define define_dynmem(_name, _size) define_dynobj(_name, 1, _size)

void dynmem_init(dynmem* dm, uint32_t obj_sz, uint32_t n_obj, void* stack_mem);
// Out-of-bounds / arithmetic-anomaly tail of dynmem_at -- carries the
// cf_assert diagnostics (log.h can't be included here; it includes this
// header). Call dynmem_at, not this.
void* dynmem_at_slow(dynmem* dm, dynmem_obj_idx index);

// @return ptr to obj at index, will grow if necessary
void* dynmem_reserve(dynmem* dm, dynmem_obj_idx* index_r);
void* dynmem_reserve_n(dynmem* dm, uint32_t n, dynmem_obj_idx* index_r);
bool dynmem_set(dynmem* dm, const dynmem_obj_idx index, const void* buf);
void dynmem_destroy(dynmem* dm);

static inline dynmem_buf_idx
dynmem_get_buf_idx(const dynmem* dm, dynmem_obj_idx index)
{
	const uint32_t high = index >> dm->shift0;
	return (dynmem_buf_idx)(high == 0 ? 0 : (32 - __builtin_clz(high)));
}

// First object index held by buffer mem_i. Shared with dynmem_at_slow, which
// re-derives the same position to assert on it.
static inline uint32_t
dynmem_get_buf_base(const dynmem* dm, dynmem_buf_idx mem_i)
{
	return mem_i == 0 ? 0 : (1U << (mem_i - 1)) << dm->shift0;
}

// @return NULL if index is out of bounds. Inline fast path -- object derefs
// sit on the per-transaction expression-compile path; anything abnormal
// drops to dynmem_at_slow for the asserted diagnostics.
static inline void*
dynmem_at(dynmem* dm, dynmem_obj_idx index)
{
	const dynmem_buf_idx mem_i = dynmem_get_buf_idx(dm, index);
	const uint32_t sub = dynmem_get_buf_base(dm, mem_i);
	const uint32_t offset = index - sub;
	uint32_t byte_offset;

	if (index < sub || mem_i >= dm->n_mem ||
			__builtin_mul_overflow(offset, dm->obj_sz, &byte_offset)) {
		return dynmem_at_slow(dm, index);
	}

	return (void*)(dm->mem[mem_i] + byte_offset);
}

static inline const void*
dynmem_get(const dynmem* dm, dynmem_obj_idx index)
{
	return (const void*)dynmem_at((dynmem*)dm, index);
}

dynmem_obj_idx dynmem_get_obj_idx(const dynmem* dm, dynmem_buf_idx buf_idx,
		void* obj);
