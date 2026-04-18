/*
 * enhanced_alloc.h
 *
 * Copyright (C) 2013-2020 Aerospike, Inc.
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

#include "log.h"

//==========================================================
// Typedefs & constants.
//

typedef struct cf_rc_header_s {
	uint32_t rc;
	uint32_t sz;
} cf_rc_header;

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
__cf_defer_free_internal(void* p)
{
	cf_free(*(void**)p);
}

inline static void
__cf_defer_atomic_free_assert_internal(void* p)
{
	void** pp = *(void***)p;
	void* local_p = as_fas_ptr(pp, NULL);

	cf_assert(local_p != NULL, CF_MISC, "deferred free pointer is NULL");
	cf_free(local_p);
}

inline static void
__cf_defer_atomic_free_optional_internal(void* p)
{
	void** pp = *(void***)p;
	void* local_p = as_fas_ptr(pp, NULL);

	cf_free(local_p);
}

//==========================================================
// Public API - defer
//

#define DEFER_ATTR_FREE __attribute__((cleanup(__cf_defer_free_internal)))

#define DEFER_FREE(__x)                                                                \
	__attribute__((cleanup(__cf_defer_free_internal))) void* __defer_free_##__LINE__ = \
			(__x)

#define DEFER_ATOMIC_FREE(__x)                                                 \
	__attribute__((cleanup(__cf_defer_atomic_free_assert_internal))) void*     \
			__defer_free_##__LINE__ = &(__x)

#define DEFER_ATOMIC_FREE_OPTIONAL(__x)                                        \
	__attribute__((cleanup(__cf_defer_atomic_free_optional_internal))) void*   \
			__defer_free_##__LINE__ = &(__x)

// Define a buffer that is either on the stack(sz <= max_stack) or
// on the heap(sz > max_stack), depending on the size.
// Auto free the memory when the scope ends.
#define define_deferred_memory(__name, __alloc_sz, __max_stack)                \
	const uint32_t __name##__sz = ((__alloc_sz) > (__max_stack)) ? 1           \
																 : __alloc_sz; \
	uint8_t __name##__mem[__name##__sz];                                       \
	DEFER_ATTR_FREE uint8_t* __name##__alloc =                                 \
			((__alloc_sz) > (__max_stack)) ? cf_malloc(__alloc_sz) : NULL;     \
	uint8_t* __name = ((__alloc_sz) > (__max_stack)) ? __name##__alloc         \
													 : __name##__mem
