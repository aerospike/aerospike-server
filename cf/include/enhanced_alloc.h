/*
 * enhanced_alloc.h
 *
 * Copyright (C) 2013-2026 Aerospike, Inc.
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

#include <malloc.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"

#include "cf_defer.h"
#include "log.h"

//==========================================================
// Typedefs & constants.
//

typedef struct cf_rc_header_s {
	uint32_t rc;
	uint32_t sz;
} cf_rc_header;

// Per-thread budget for define_deferred_memory's stack arm. When the
// next request would push cumulative usage past this, the macro
// promotes to cf_malloc + cleanup-on-scope-exit instead.
#define CF_DEFERRED_STACK_BUDGET (4U << 20) // 4 MiB

extern __thread size_t g_tl_deferred_stack_used;

//==========================================================
// Public API - arena management and stats.
//

void cf_alloc_init(void);
void cf_alloc_set_debug(bool debug_allocations, bool indent_allocations,
		bool poison_allocations, uint32_t quarantine_allocations);

void cf_alloc_heap_stats(size_t* allocated_kbytes, size_t* active_kbytes,
		size_t* mapped_kbytes, double* efficiency_pct, uint32_t* site_count);
void cf_alloc_log_stats(const char* file, const char* opts);
void cf_alloc_log_site_infos(const char* file);

//==========================================================
// Public API - ordinary allocation.
//

// Don't call these directly - use wrappers below.
void* cf_alloc_try_malloc(size_t sz);

#define cf_try_malloc(_sz) cf_alloc_try_malloc(_sz)
#define cf_malloc(_sz) malloc(_sz)
#define cf_calloc(_n, _sz) calloc(_n, _sz)
#define cf_realloc(_p, _sz) realloc(_p, _sz)
#define cf_valloc(_sz) valloc(_sz)
#define cf_aligned_alloc(_align, _sz) aligned_alloc(_align, _sz)

#define cf_strdup(_s) strdup(_s)
#define cf_strndup(_s, _n) strndup(_s, _n)

#define cf_asprintf(_s, _f, ...)                                               \
	({                                                                         \
		int32_t _n = asprintf(_s, _f, __VA_ARGS__);                            \
		_n;                                                                    \
	})

#define cf_free(_p) free(_p)

void cf_validate_pointer(const void* p_indent);
void cf_trim_region_to_mapped(void** p, size_t* sz);

extern bool g_alloc_started;

//==========================================================
// Public API - reference-counted allocation.
//

void* cf_rc_alloc(size_t sz);
void cf_rc_free(void* p);

uint32_t cf_rc_count(const void* p);
uint32_t cf_rc_reserve(void* p);
uint32_t cf_rc_release(void* p);
uint32_t cf_rc_releaseandfree(void* p);

//==========================================================
// Private API - defer
//

inline static void
cf_defer_free_internal(void* p)
{
	cf_free(*(void**)p);
}

inline static void
cf_defer_atomic_free_assert_internal(void* p)
{
	void** pp = *(void***)p;
	void* local_p = as_fas_ptr(pp, NULL);

	cf_assert(local_p != NULL, CF_MISC, "deferred free pointer is NULL");
	cf_free(local_p);
}

inline static void
cf_defer_atomic_free_optional_internal(void* p)
{
	void** pp = *(void***)p;
	void* local_p = as_fas_ptr(pp, NULL);

	cf_free(local_p);
}

//==========================================================
// Public API - defer
//

#define DEFER_ATTR_FREE __attribute__((cleanup(cf_defer_free_internal)))

#define CONCAT_IMPL(a, b) a##b
#define CONCAT(a, b) CONCAT_IMPL(a, b)

#define DEFER_FREE(_x)                                                               \
	__attribute__((cleanup(cf_defer_free_internal))) void* cf_defer_var_##__LINE__ = \
			(_x)

#define DEFER_ATOMIC_FREE(_x)                                                  \
	__attribute__((cleanup(cf_defer_atomic_free_assert_internal))) void*       \
			cf_defer_var_##__LINE__ = &(_x)

#define DEFER_ATOMIC_FREE_OPTIONAL(_x)                                         \
	__attribute__((cleanup(cf_defer_atomic_free_optional_internal))) void*     \
			cf_defer_var_##__LINE__ = &(_x)

// Define a buffer that lives on the stack when there's room within
// the thread's CF_DEFERRED_STACK_BUDGET (4 MiB), or on the heap
// otherwise. Heap is auto-freed and the stack accounting decremented
// when the enclosing scope exits via cf_defer. Requested size is
// rounded up to a multiple of 8 so the thread-local counter slightly over-
// estimates rather than under-counts the compiler's actual stack
// alignment — safer to trip to heap a tiny bit early than to under-
// account and overflow.
#define define_deferred_memory(_name, _alloc_sz)                                \
	const size_t _name##_req_sz_ = ((_alloc_sz) + 7U) & ~(size_t)7U;            \
	const bool _name##_use_heap_ = g_tl_deferred_stack_used + _name##_req_sz_ > \
			CF_DEFERRED_STACK_BUDGET;                                           \
	uint8_t __attribute__((aligned(8)))                                         \
	_name##_mem_[_name##_use_heap_ ? 1 : _name##_req_sz_];                      \
	uint8_t* _name = _name##_use_heap_ ? cf_malloc(_name##_req_sz_)             \
									   : _name##_mem_;                          \
	if (! _name##_use_heap_) {                                                  \
		g_tl_deferred_stack_used += _name##_req_sz_;                            \
	}                                                                           \
	cf_defer                                                                    \
	{                                                                           \
		if (_name##_use_heap_) {                                                \
			cf_free(_name);                                                     \
		}                                                                       \
		else {                                                                  \
			g_tl_deferred_stack_used -= _name##_req_sz_;                        \
		}                                                                       \
	}

// Typed-array sibling of define_deferred_memory. Stages an
// uint8_t-backed buffer of `_count * sizeof(_type)` bytes through
// define_deferred_memory and exposes `_name` as a `_type*`.
#define define_deferred_array(_name, _type, _count)                            \
	define_deferred_memory(_name##_da_, (size_t)(_count) * sizeof(_type));     \
	_type* _name = (_type*)_name##_da_
