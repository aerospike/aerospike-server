/*
 * alloc.c
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
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

// Make sure that stdlib.h gives us aligned_alloc().
#define _ISOC11_SOURCE

#include "enhanced_alloc.h"

#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <jemalloc/jemalloc.h>

#include <sys/syscall.h>
#include <sys/types.h>

#include "fault.h"
#include "mem_count.h"

#include "aerospike/ck/ck_pr.h"
#include "citrusleaf/cf_atomic.h"

#include "warnings.h"

#undef strdup
#undef strndup

#define N_ARENAS 150
#define PAGE_SZ 4096

#define MAX_SITES 4096
#define MAX_THREADS 256

#define MULT 3486784401u
#define MULT_INV 3396732273u

#define STR_(x) #x
#define STR(x) STR_(x)

typedef struct site_info_s {
	uint32_t site_id;
	pid_t thread_id;
	size_t size_lo;
	size_t size_hi;
} site_info;

// Old glibc versions don't provide this; work around compiler warning.
void *aligned_alloc(size_t align, size_t sz);

const char *jem_malloc_conf = "narenas:" STR(N_ARENAS);

extern size_t je_chunksize_mask;
extern void *je_huge_aalloc(const void *p);

__thread int32_t g_ns_arena = -1;
static __thread int32_t g_ns_tcache = -1;

static const void *g_site_ras[MAX_SITES];
static uint32_t g_n_site_ras;

static site_info g_site_infos[MAX_SITES * MAX_THREADS];
// Start at 1, then we can use site ID 0 to mean "no site ID".
static uint32_t g_n_site_infos = 1;

static __thread uint32_t g_thread_site_infos[MAX_SITES];

static __thread pid_t g_tid;
// Start with *_ALL; see cf_alloc_set_debug() for details.
static cf_alloc_debug g_debug = CF_ALLOC_DEBUG_ALL;

// All the hook_*() functions are invoked from hook functions that hook into
// malloc() and friends for memory accounting purposes.
//
// This means that we have no idea who called us and, for example, which locks
// they hold. Let's be careful when calling back into asd code.

static int32_t
hook_get_arena(const void *p)
{
	int32_t **base = (int32_t **)((uint64_t)p & ~je_chunksize_mask);
	int32_t *arena;

	if (base != p) {
		// Small or large allocation.
		arena = base[0];
	}
	else {
		// Huge allocation.
		arena = je_huge_aalloc(p);
	}

	return arena[0];
}

static void
hook_check_arena(const void *p, int32_t arena)
{
	if (g_debug == CF_ALLOC_DEBUG_NONE) {
		return;
	}

	int32_t arena_p = hook_get_arena(p);

	if (arena < 0 && arena_p < N_ARENAS) {
		return;
	}

	// The "arena" parameter is never < N_ARENAS.

	if (arena >= N_ARENAS && arena_p >= N_ARENAS) {
		return;
	}

	size_t jem_sz = jem_sallocx(p, 0);
	cf_crash(CF_ALLOC, "arena change for %zu@%p: %d -> %d", jem_sz, p, arena_p, arena);
}

static pid_t
hook_gettid(void)
{
	if (g_tid == 0) {
		g_tid = (pid_t)syscall(SYS_gettid);
	}

	return g_tid;
}

// Map a 64-bit address to a 12-bit site ID.

static uint32_t
hook_get_site_id(const void *ra)
{
	uint32_t site_id = (uint32_t)(uint64_t)ra & (MAX_SITES - 1);

	for (uint32_t i = 0; i < MAX_SITES; ++i) {
		const void *site_ra = ck_pr_load_ptr(g_site_ras + site_id);

		// The allocation site is already registered and we found its
		// slot. Return the slot index.

		if (site_ra == ra) {
			return site_id;
		}

		// We reached an empty slot, i.e., the allocation site isn't yet
		// registered. Try to register it. If somebody else managed to grab
		// this slot in the meantime, keep looping. Otherwise return the
		// slot index.

		if (site_ra == NULL && ck_pr_cas_ptr(g_site_ras + site_id, NULL, (void *)ra)) {
			ck_pr_inc_32(&g_n_site_ras);
			return site_id;
		}

		site_id = (site_id + 1) & (MAX_SITES - 1);
	}

	// More than MAX_SITES call sites.
	cf_crash(CF_ALLOC, "too many call sites");
	// Not reached.
	return 0;
}

static uint32_t
hook_new_site_info_id(void)
{
	uint32_t info_id = ck_pr_faa_32(&g_n_site_infos, 1);

	if (info_id >= g_n_site_infos) {
		cf_crash(CF_ALLOC, "site info pool exhausted");
	}

	return info_id;
}

// Get the info ID of the site_info record for the given site ID and the current
// thread. In case the current thread doesn't yet have a site_info record for the
// given site ID, a new site_info record is allocated.

static uint32_t
hook_get_site_info_id(uint32_t site_id)
{
	uint32_t info_id = g_thread_site_infos[site_id];

	// This thread encountered this allocation site before. We already
	// have a site info record.

	if (info_id != 0) {
		return info_id;
	}

	// This is the first time that this thread encounters this allocation
	// site. We need to allocate a site_info record.

	info_id = hook_new_site_info_id();
	site_info *info = g_site_infos + info_id;

	info->site_id = site_id;
	info->thread_id = hook_gettid();
	info->size_lo = 0;
	info->size_hi = 0;

	g_thread_site_infos[site_id] = info_id;
	return info_id;
}

// Account for an allocation by the current thread for the allocation site
// with the given address.

static void
hook_handle_alloc(const void *ra, void *p, size_t sz)
{
	if (p == NULL) {
		return;
	}

	size_t jem_sz = jem_sallocx(p, 0);

	uint32_t site_id = hook_get_site_id(ra);
	uint32_t info_id = hook_get_site_info_id(site_id);
	site_info *info = g_site_infos + info_id;

	size_t size_lo = info->size_lo;
	info->size_lo += jem_sz;

	// Carry?

	if (info->size_lo < size_lo) {
		++info->size_hi;
	}

	uint8_t *data = (uint8_t *)p + jem_sz - sizeof(uint32_t);
	uint32_t *data32 = (uint32_t *)data;

	uint8_t *mark = (uint8_t *)p + sz;
	size_t delta = (size_t)(data - mark);

	// Keep 0xffff as a marker for double free detection.

	if (delta > 0xfffe) {
		delta = 0;
	}

	*data32 = ((site_id << 16) | (uint32_t)delta) * MULT + 1;

	for (uint32_t i = 0; i < 4 && i < delta; ++i) {
		mark[i] = data[i];
	}
}

// Account for a deallocation by the current thread for the allocation
// site with the given address.

static void
hook_handle_free(const void *ra, void *p, size_t jem_sz)
{
	uint8_t *data = (uint8_t *)p + jem_sz - sizeof(uint32_t);
	uint32_t *data32 = (uint32_t *)data;

	uint32_t val = (*data32 - 1) * MULT_INV;
	uint32_t site_id = val >> 16;
	uint32_t delta = val & 0xffff;

	if (site_id >= MAX_SITES) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA %p, invalid site ID", jem_sz, p, ra);
	}

	const void *data_ra = ck_pr_load_ptr(g_site_ras + site_id);

	if (delta == 0xffff) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA %p, potential double free, possibly freed before with RA %p",
				jem_sz, p, ra, data_ra);
	}

	if (delta > jem_sz - sizeof(uint32_t)) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA %p, invalid delta length, possibly allocated with RA %p",
				jem_sz, p, ra, data_ra);
	}

	uint8_t *mark = data - delta;

	for (uint32_t i = 0; i < 4 && i < delta; ++i) {
		if (mark[i] != data[i]) {
			cf_crash(CF_ALLOC, "corruption %zu@%p RA %p, invalid mark, possibly allocated with RA %p",
					jem_sz, p, ra, data_ra);
		}
	}

	uint32_t info_id = hook_get_site_info_id(site_id);
	site_info *info = g_site_infos + info_id;

	size_t size_lo = info->size_lo;
	info->size_lo -= jem_sz;

	// Borrow?

	if (info->size_lo > size_lo) {
		--info->size_hi;
	}

	// Replace the allocation site with the deallocation site to facilitate
	// double-free debugging.

	site_id = hook_get_site_id(ra);

	// Also invalidate the delta length, so that we are more likely to detect
	// double frees.

	*data32 = ((site_id << 16) | 0xffff) * MULT + 1;

	for (uint32_t i = 0; i < 4 && i < delta; ++i) {
		mark[i] = data[i];
	}
}

static void
valgrind_check(void)
{
	// Make sure that we actually call into JEMalloc when invoking malloc().
	//
	// By default, Valgrind redirects the standard allocation API functions,
	// i.e., malloc(), calloc(), etc., to glibc.
	//
	// The problem with this is that Valgrind only redirects the standard API
	// functions. It does not know about, and thus doesn't redirect, our
	// non-standard functions, e.g., cf_alloc_malloc_arena().
	//
	// As we use both, standard and non-standard functions, to allocate memory,
	// we would end up with an inconsistent mix of allocations, some allocated
	// by JEMalloc and some by glibc's allocator.
	//
	// Sooner or later, we will thus end up passing a memory block allocated by
	// JEMalloc to free(), which Valgrind has redirected to glibc's allocator.

	void *p1 = malloc(1);
	free(p1);

	void *p2 = jem_malloc(1);
	jem_free(p2);

	// If both of the above allocations are handled by JEMalloc, then they will
	// be located in the same memory page. If, however, the first allocation is
	// handled by glibc, then the memory blocks will come from two different
	// memory pages.

	uint64_t page1 = (uint64_t)p1 >> 12;
	uint64_t page2 = (uint64_t)p2 >> 12;

	if (page1 != page2) {
		cf_crash_nostack(CF_ALLOC, "Valgrind redirected malloc() to glibc; please run Valgrind with --soname-synonyms=somalloc=nouserintercepts");
	}
}

void
cf_alloc_init(void)
{
	valgrind_check();

	// Turn off libstdc++'s memory caching, as it just duplicates JEMalloc's.

	if (setenv("GLIBCXX_FORCE_NEW", "1", 1) < 0) {
		cf_crash(CF_ALLOC, "setenv() failed: %d (%s)", errno, cf_strerror(errno));
	}

	// Double-check that hook_get_arena() works, as it depends on JEMalloc's
	// internal data structures.

	int32_t err = jem_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "error while flushing thread cache: %d (%s)", err, cf_strerror(err));
	}

	for (size_t sz = 1; sz <= 16 * 1024 * 1024; sz *= 2) {
		void *p = cf_alloc_malloc_arena(sz, N_ARENAS / 2);
		int32_t arena = hook_get_arena(p);

		if (arena != N_ARENAS / 2) {
			cf_crash(CF_ALLOC, "arena mismatch: %d vs. %d", arena, N_ARENAS / 2);
		}

		free(p);
	}
}

// Restrict memory debugging.
//
// We always start out with memory debugging fully enabled (*_ALL). Then,
// once we have parsed the configuration file, we restrict it to what the
// configuration file says (e.g., *_TRANSIENT).
//
// The reason is that we can safely go from "on" to "off", but not vice
// versa.
//
// When "off", we don't add accounting info to an allocation. Now, if we
// deallocated such an allocation when "on", then we'd erroneously detect
// a corruption, because we'd try to validate accounting info that isn't
// there.

void
cf_alloc_set_debug(cf_alloc_debug debug)
{
	g_debug = debug;
}

int32_t
cf_alloc_create_arena(void)
{
	int32_t arena;
	size_t arena_len = sizeof(arena);

	int32_t err = jem_mallctl("arenas.extend", &arena, &arena_len, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "failed to create new arena: %d (%s)", err, cf_strerror(err));
	}

	cf_debug(CF_ALLOC, "created new arena %d", arena);
	return arena;
}

void
cf_alloc_heap_stats(size_t *allocated_kbytes, size_t *active_kbytes, size_t *mapped_kbytes,
		double *efficiency_pct, uint32_t *site_count)
{
	uint64_t epoch = 1;
	size_t len = sizeof(epoch);

	int32_t err = jem_mallctl("epoch", &epoch, &len, &epoch, len);

	if (err != 0) {
		cf_crash(CF_ALLOC, "failed to retrieve epoch: %d (%s)", err, cf_strerror(err));
	}

	size_t allocated;
	len = sizeof(allocated);

	err = jem_mallctl("stats.allocated", &allocated, &len, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "failed to retrieve stats.allocated: %d (%s)", err, cf_strerror(err));
	}

	size_t active;
	len = sizeof(active);

	err = jem_mallctl("stats.active", &active, &len, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "failed to retrieve stats.active: %d (%s)", err, cf_strerror(err));
	}

	size_t mapped;
	len = sizeof(mapped);

	err = jem_mallctl("stats.mapped", &mapped, &len, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "failed to retrieve stats.mapped: %d (%s)", err, cf_strerror(err));
	}

	if (allocated_kbytes) {
		*allocated_kbytes = allocated / 1024;
	}

	if (active_kbytes) {
		*active_kbytes = active / 1024;
	}

	if (mapped_kbytes) {
		*mapped_kbytes = mapped / 1024;
	}

	if (efficiency_pct) {
		*efficiency_pct = mapped != 0 ?
				(double)allocated * 100.0 / (double)mapped : 0.0;
	}

	if (site_count) {
		*site_count = ck_pr_load_32(&g_n_site_ras);
	}
}

static void
line_to_log(void *data, const char *line)
{
	(void)data;

	char buff[1000];
	size_t i;

	for (i = 0; i < sizeof(buff) - 1 && line[i] != 0 && line[i] != '\n'; ++i) {
		buff[i] = line[i];
	}

	buff[i] = 0;
	cf_info(CF_ALLOC, "%s", buff);
}

static void
line_to_file(void *data, const char *line)
{
	fprintf((FILE *)data, "%s", line);
}

static void
time_to_file(FILE *fh)
{
	time_t now = time(NULL);

	if (now == (time_t)-1) {
		cf_crash(CF_ALLOC, "time() failed: %d (%s)", errno, cf_strerror(errno));
	}

	struct tm gmt;

	if (gmtime_r(&now, &gmt) == NULL) {
		cf_crash(CF_ALLOC, "gmtime_r() failed");
	}

	char text[250];

	if (strftime(text, sizeof(text), "%b %d %Y %T %Z", &gmt) == 0) {
		cf_crash(CF_ALLOC, "strftime() failed");
	}

	fprintf(fh, "---------- %s ----------\n", text);
}

void
cf_alloc_log_stats(const char *file, const char *opts)
{
	if (file == NULL) {
		jem_malloc_stats_print(line_to_log, NULL, opts);
		return;
	}

	FILE *fh = fopen(file, "a");

	if (fh == NULL) {
		cf_warning(CF_ALLOC, "failed to open allocation stats file %s: %d (%s)",
				file, errno, cf_strerror(errno));
		return;
	}

	time_to_file(fh);
	jem_malloc_stats_print(line_to_file, fh, opts);
	fclose(fh);
}

void
cf_alloc_log_site_infos(const char *file)
{
	FILE *fh = fopen(file, "a");

	if (fh == NULL) {
		cf_warning(CF_ALLOC, "failed to open site info file %s: %d (%s)",
				file, errno, cf_strerror(errno));
		return;
	}

	time_to_file(fh);
	uint32_t n_site_infos = ck_pr_load_32(&g_n_site_infos);

	for (uint32_t i = 1; i < n_site_infos; ++i) {
		site_info *info = g_site_infos + i;
		const void *ra = ck_pr_load_ptr(g_site_ras + info->site_id);
		fprintf(fh, "0x%016" PRIx64 " %9d 0x%016zx 0x%016zx\n", (uint64_t)ra, info->thread_id,
				info->size_hi, info->size_lo);
	}

	fclose(fh);
}

static bool
is_transient(int32_t arena)
{
	// Note that this also considers -1 (i.e., the default thread arena)
	// to be transient, in addition to arenas 0 .. (N_ARENAS - 1).

	return arena < N_ARENAS;
}

static bool
want_debug(int32_t arena)
{
	switch (g_debug) {
	case CF_ALLOC_DEBUG_NONE:
		return false;

	case CF_ALLOC_DEBUG_TRANSIENT:
		return is_transient(arena);

	case CF_ALLOC_DEBUG_PERSISTENT:
		return !is_transient(arena);

	case CF_ALLOC_DEBUG_ALL:
		return true;
	}

	// Not reached.
	return false;
}

static int32_t
calc_free_flags(int32_t arena)
{
	// If it's a transient allocation, then simply use the default
	// thread-local cache. No flags needed. Same, if we don't debug
	// at all; then we can save ourselves the second cache.

	if (is_transient(arena) || g_debug == CF_ALLOC_DEBUG_NONE) {
		return 0;
	}

	// If it's a persistent allocation, then use the second per-thread
	// cache. Add it to the flags. See calc_alloc_flags() for more on
	// this second cache.

	return MALLOCX_TCACHE(g_ns_tcache);
}

static void
do_free(void *p, const void *ra)
{
	if (p == NULL) {
		return;
	}

	int32_t arena = hook_get_arena(p);
	int32_t flags = calc_free_flags(arena);

	if (!want_debug(arena)) {
		jem_dallocx(p, flags);
		return;
	}

	size_t jem_sz = jem_sallocx(p, 0);
	hook_handle_free(ra, p, jem_sz);
	jem_sdallocx(p, jem_sz, flags);
}

void
__attribute__ ((noinline))
free(void *p)
{
	do_free(p, __builtin_return_address(0));
}

static int32_t
calc_alloc_flags(int32_t flags, int32_t arena)
{
	// Default arena and default thread-local cache. No additional flags
	// needed.

	if (arena < 0) {
		return flags;
	}

	// We're allocating from a specific arena. Add it to the flags.

	flags |= MALLOCX_ARENA(arena);

	// If it's an arena for transient allocations, then we use the default
	// thread-local cache. No additional flags needed. Same, if we don't
	// debug at all; then we can save ourselves the second cache.

	if (is_transient(arena) || g_debug == CF_ALLOC_DEBUG_NONE) {
		return flags;
	}

	// We have a second per-thread cache for persistent allocations. In this
	// way we never mix persistent allocations and transient allocations in
	// the same cache. We need to keep them apart, because debugging may be
	// enabled for one, but not the other.

	// Create the second per-thread cache, if we haven't already done so.

	if (g_ns_tcache < 0) {
		size_t len = sizeof(g_ns_tcache);
		int32_t err = jem_mallctl("tcache.create", &g_ns_tcache, &len, NULL, 0);

		if (err != 0) {
			cf_crash(CF_ALLOC, "failed to create new cache: %d (%s)", err, cf_strerror(err));
		}
	}

	// Add the second (non-default) per-thread cache to the flags.

	flags |= MALLOCX_TCACHE(g_ns_tcache);
	return flags;
}

static void *
do_mallocx(size_t sz, int32_t arena, const void *ra)
{
	int32_t flags = calc_alloc_flags(0, arena);

	if (!want_debug(arena)) {
		return jem_mallocx(sz == 0 ? 1 : sz, flags);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = jem_mallocx(ext_sz, flags);
	hook_handle_alloc(ra, p, sz);

	return p;
}

void *
cf_alloc_try_malloc(size_t sz)
{
	// Allowed to return NULL.
	return do_mallocx(sz, -1, __builtin_return_address(0));
}

void *
cf_alloc_malloc_arena(size_t sz, int32_t arena)
{
	void *p = do_mallocx(sz, arena, __builtin_return_address(0));
	cf_assert(p, CF_ALLOC, "malloc_ns failed sz %zu arena %d", sz, arena);
	return p;
}

void *
__attribute__ ((noinline))
malloc(size_t sz)
{
	void *p = do_mallocx(sz, -1, __builtin_return_address(0));
	cf_assert(p, CF_ALLOC, "malloc failed sz %zu", sz);
	return p;
}

static void *
do_callocx(size_t n, size_t sz, int32_t arena, const void *ra)
{
	int32_t flags = calc_alloc_flags(MALLOCX_ZERO, arena);
	size_t tot_sz = n * sz;

	if (!want_debug(arena)) {
		return jem_mallocx(tot_sz == 0 ? 1 : tot_sz, flags);
	}

	size_t ext_sz = tot_sz + sizeof(uint32_t);

	void *p = jem_mallocx(ext_sz, flags);
	hook_handle_alloc(ra, p, tot_sz);

	return p;
}

void *
cf_alloc_calloc_arena(size_t n, size_t sz, int32_t arena)
{
	void *p = do_callocx(n, sz, arena, __builtin_return_address(0));
	cf_assert(p, CF_ALLOC, "calloc_ns failed n %zu sz %zu arena %d", n, sz, arena);
	return p;
}

void *
calloc(size_t n, size_t sz)
{
	void *p = do_callocx(n, sz, -1, __builtin_return_address(0));
	cf_assert(p, CF_ALLOC, "calloc failed n %zu sz %zu", n, sz);
	return p;
}

static void *
do_rallocx(void *p, size_t sz, int32_t arena, const void *ra)
{
	if (p == NULL) {
		return do_mallocx(sz, arena, ra);
	}

	hook_check_arena(p, arena);

	if (sz == 0) {
		do_free(p, ra);
		return NULL;
	}

	int32_t flags = calc_alloc_flags(0, arena);

	if (!want_debug(arena)) {
		return jem_rallocx(p, sz, flags);
	}

	size_t jem_sz = jem_sallocx(p, 0);
	hook_handle_free(ra, p, jem_sz);

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p2 = jem_rallocx(p, ext_sz, flags);
	hook_handle_alloc(ra, p2, sz);

	return p2;
}

void *
cf_alloc_realloc_arena(void *p, size_t sz, int32_t arena)
{
	void *p2 = do_rallocx(p, sz, arena, __builtin_return_address(0));
	cf_assert(p2 || sz == 0, CF_ALLOC, "realloc_ns failed sz %zu arena %d", sz, arena);
	return p2;
}

void *
realloc(void *p, size_t sz)
{
	void *p2 = do_rallocx(p, sz, -1, __builtin_return_address(0));
	cf_assert(p2 || sz == 0, CF_ALLOC, "realloc failed sz %zu", sz);
	return p2;
}

static char *
do_strdup(const char *s, size_t n, const void *ra)
{
	size_t sz = n + 1;
	size_t ext_sz = want_debug(-1) ? sz + sizeof(uint32_t) : sz;

	char *s2 = jem_mallocx(ext_sz, 0);
	cf_assert(s2, CF_ALLOC, "strdup failed len %zu", n);

	if (want_debug(-1)) {
		hook_handle_alloc(ra, s2, sz);
	}

	memcpy(s2, s, sz);
	return s2;
}

char *
strdup(const char *s)
{
	return do_strdup(s, strlen(s), __builtin_return_address(0));
}

char *
strndup(const char *s, size_t n)
{
	size_t n2 = 0;

	while (n2 < n && s[n2] != 0) {
		++n2;
	}

	size_t sz = n2 + 1;
	size_t ext_sz = want_debug(-1) ? sz + sizeof(uint32_t) : sz;

	char *s2 = jem_mallocx(ext_sz, 0);
	cf_assert(s2, CF_ALLOC, "strndup failed limit %zu", n);

	if (want_debug(-1)) {
		hook_handle_alloc(__builtin_return_address(0), s2, sz);
	}

	memcpy(s2, s, n2);
	s2[n2] = 0;

	return s2;
}

int32_t
asprintf(char **res, const char *form, ...)
{
	char buff[25000];

	va_list va;
	va_start(va, form);

	int32_t n = vsnprintf(buff, sizeof(buff), form, va);

	va_end(va);

	if ((size_t)n >= sizeof(buff)) {
		cf_crash(CF_ALLOC, "asprintf overflow len %d", n);
	}

	*res = do_strdup(buff, (size_t)n, __builtin_return_address(0));
	return n;
}

int32_t
posix_memalign(void **p, size_t align, size_t sz)
{
	if (!want_debug(-1)) {
		return jem_posix_memalign(p, align, sz == 0 ? 1 : sz);
	}

	size_t ext_sz = sz + sizeof(uint32_t);
	int32_t err = jem_posix_memalign(p, align, ext_sz);

	if (err != 0) {
		return err;
	}

	hook_handle_alloc(__builtin_return_address(0), *p, sz);
	return 0;
}

void *
aligned_alloc(size_t align, size_t sz)
{
	if (!want_debug(-1)) {
		return jem_aligned_alloc(align, sz == 0 ? 1 : sz);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = jem_aligned_alloc(align, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, sz);

	return p;
}

static void *
do_valloc(size_t sz)
{
	if (!want_debug(-1)) {
		return jem_aligned_alloc(PAGE_SZ, sz == 0 ? 1 : sz);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = jem_aligned_alloc(PAGE_SZ, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, sz);

	return p;
}

void *
valloc(size_t sz)
{
	void *p = do_valloc(sz);
	cf_assert(p, CF_ALLOC, "valloc failed sz %zu", sz);
	return p;
}

void *
memalign(size_t align, size_t sz)
{
	if (!want_debug(-1)) {
		return jem_aligned_alloc(align, sz == 0 ? 1 : sz);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = jem_aligned_alloc(align, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, sz);

	return p;
}

void *
pvalloc(size_t sz)
{
	(void)sz;
	cf_crash(CF_ALLOC, "obsolete pvalloc() called");
	// Not reached.
	return NULL;
}

void *
cf_rc_alloc(size_t sz)
{
	size_t tot_sz = sizeof(cf_rc_header) + sz;
	size_t ext_sz = want_debug(-1) ? tot_sz + sizeof(uint32_t) : tot_sz;

	cf_rc_header *head = jem_malloc(ext_sz);
	cf_assert(head, CF_ALLOC, "rc_alloc failed sz %zu", sz);

	if (want_debug(-1)) {
		hook_handle_alloc(__builtin_return_address(0), head, tot_sz);
	}

	head->rc = 1;
	head->sz = (uint32_t)sz;

	return head + 1;
}

void
cf_rc_free(void *p)
{
	if (p == NULL) {
		cf_crash(CF_ALLOC, "trying to cf_rc_free() null pointer");
	}

	cf_rc_header *head = (cf_rc_header *)p - 1;

	if (!want_debug(-1)) {
		jem_dallocx(head, 0);
		return;
	}

	size_t jem_sz = jem_sallocx(head, 0);
	hook_handle_free(__builtin_return_address(0), head, jem_sz);
	jem_sdallocx(head, jem_sz, 0);
}

int32_t
cf_rc_reserve(void *p)
{
	cf_rc_header *head = (cf_rc_header *)p - 1;
	return cf_atomic32_incr(&head->rc);
}

int32_t
cf_rc_release(void *p)
{
	cf_rc_header *head = (cf_rc_header *)p - 1;
	int32_t rc = cf_atomic32_decr(&head->rc);
	cf_assert(rc >= 0, CF_ALLOC, "reference count underflow");
	return rc;
}

int32_t
cf_rc_releaseandfree(void *p)
{
	cf_rc_header *head = (cf_rc_header *)p - 1;
	int32_t rc = cf_atomic32_decr(&head->rc);
	cf_assert(rc >= 0, CF_ALLOC, "reference count underflow");

	if (rc > 0) {
		return rc;
	}

	if (!want_debug(-1)) {
		jem_dallocx(head, 0);
		return 0;
	}

	size_t jem_sz = jem_sallocx(head, 0);
	hook_handle_free(__builtin_return_address(0), head, jem_sz);
	jem_sdallocx(head, jem_sz, 0);
	return 0;
}

int32_t
cf_rc_count(const void *p)
{
	const cf_rc_header *head = (const cf_rc_header *)p - 1;
	return (int32_t)head->rc;
}
