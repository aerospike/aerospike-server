/*
 * enhanced_alloc.h
 *
 * Copyright (C) 2013-2017 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"


//==========================================================
// Typedefs & constants.
//

typedef struct cf_rc_header_s {
	cf_atomic32 rc;
	uint32_t	sz;
} cf_rc_header;

typedef enum {
	CF_ALLOC_DEBUG_NONE,
	CF_ALLOC_DEBUG_TRANSIENT,
	CF_ALLOC_DEBUG_PERSISTENT,
	CF_ALLOC_DEBUG_ALL
} cf_alloc_debug;


//==========================================================
// Public API - arena management and stats.
//

extern __thread int32_t g_ns_arena;

void cf_alloc_init(void);
void cf_alloc_set_debug(cf_alloc_debug debug);
int32_t cf_alloc_create_arena(void);

#define CF_ALLOC_SET_NS_ARENA(_ns) \
	(g_ns_arena = _ns->storage_data_in_memory ? _ns->jem_arena : -1)

static inline int32_t
cf_alloc_clear_ns_arena(void)
{
	int32_t old_arena = g_ns_arena;
	g_ns_arena = -1;
	return old_arena;
}

static inline void
cf_alloc_restore_ns_arena(int32_t old_arena)
{
	g_ns_arena = old_arena;
}

void cf_alloc_heap_stats(size_t *allocated_kbytes, size_t *active_kbytes, size_t *mapped_kbytes, double *efficiency_pct, uint32_t *site_count);
void cf_alloc_log_stats(const char *file, const char *opts);
void cf_alloc_log_site_infos(const char *file);


//==========================================================
// Public API - ordinary allocation.
//

// Don't call these directly - use wrappers below.
void *cf_alloc_try_malloc(size_t sz);
void *cf_alloc_malloc_arena(size_t sz, int32_t arena);
void *cf_alloc_calloc_arena(size_t n, size_t sz, int32_t arena);
void *cf_alloc_realloc_arena(void *p, size_t sz, int32_t arena);

#define cf_try_malloc(_sz)       cf_alloc_try_malloc(_sz)

#define cf_malloc(_sz)           malloc(_sz)
#define cf_malloc_ns(_sz)        cf_alloc_malloc_arena(_sz, g_ns_arena)

#define cf_calloc(_n, _sz)       calloc(_n, _sz)
#define cf_calloc_ns(_n, _sz)    cf_alloc_calloc_arena(_n, _sz, g_ns_arena)

#define cf_realloc(_p, _sz)      realloc(_p, _sz)
#define cf_realloc_ns(_p, _sz)   cf_alloc_realloc_arena(_p, _sz, g_ns_arena)

#define cf_valloc(_sz)           valloc(_sz)

#define cf_strdup(_s)            strdup(_s)
#define cf_strndup(_s, _n)       strndup(_s, _n)

#define cf_asprintf(_s, _f, ...) ({ \
	int32_t _n = asprintf(_s, _f, __VA_ARGS__); \
	_n; \
})

#define cf_free(_p)              free(_p)


//==========================================================
// Public API - reference-counted allocation.
//

void *cf_rc_alloc(size_t sz);
void cf_rc_free(void *p);

int32_t cf_rc_count(const void *p);
int32_t cf_rc_reserve(void *p);
int32_t cf_rc_release(void *p);
int32_t cf_rc_releaseandfree(void *p);
