/*
 * alloc.c
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

// Make sure that stdlib.h gives us aligned_alloc().
#define _ISOC11_SOURCE

#include "enhanced_alloc.h"

#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <jemalloc/internal/jemalloc_internal_defs.h>
#include <jemalloc/jemalloc.h>

#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "bits.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"
#include "trace.h"

#include "aerospike/as_atomic.h"
#include "aerospike/as_random.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_hash_math.h"

#include "warnings.h"

#undef strdup
#undef strndup

#define N_ARENAS 149 // used to be 150; now arena 149 is startup arena
#define PAGE_SZ (1ul << LG_PAGE)

#define MAX_SITES 4096
#define MAX_THREADS 1024

#define MULT 3486784401u
#define MULT_INV 3396732273u

#define MULT_64 12157665459056928801ul
#define MULT_INV_64 12381265223964269537ul

#define STR_(x) #x
#define STR(x) STR_(x)

#define MAX_INDENT (32 * 8)

#define POISON_CHAR (0xae)
#define MAX_POISON_SZ (1024 * 1024 * 64ul)

typedef struct site_info_s {
	uint32_t site_id;
	pid_t thread_id;
	size_t size_lo;
	size_t size_hi;
} site_info;

typedef struct quarantined_s {
	uint32_t site_id;
	pid_t thread_id;
	void *p;
	uint32_t jem_sz;
	int32_t flags;
	uint32_t hash;
	cf_mutex mutex;
} quarantined;

// Old glibc versions don't provide this; work around compiler warning.
void *aligned_alloc(size_t align, size_t sz);

// When fortification is disabled, glibc's headers don't provide this.
int32_t __asprintf_chk(char **res, int32_t flags, const char *form, ...);

const char *jem_malloc_conf = "narenas:" STR(N_ARENAS);

extern size_t je_chunksize_mask;
extern void *je_huge_aalloc(const void *p);

static const void *g_site_ras[MAX_SITES];
static uint32_t g_n_site_ras;

static site_info g_site_infos[MAX_SITES * MAX_THREADS];
// Start at 1, then we can use site ID 0 to mean "no site ID".
static uint64_t g_n_site_infos = 1;

static __thread uint32_t g_thread_site_infos[MAX_SITES];

static quarantined *g_quarantined;
static uint64_t g_n_quarantined = 0;

bool g_alloc_started = false;
static int32_t g_startup_arena = -1;

static bool g_debug;
static bool g_indent;
static bool g_poison;
static uint32_t g_quarantine;

static __thread as_random g_rand = { .initialized = false };

// All the hook_*() functions are invoked from hook functions that hook into
// malloc() and friends for memory accounting purposes.
//
// This means that we have no idea who called us and, for example, which locks
// they hold. Let's be careful when calling back into asd code.

static int32_t
hook_get_arena(const void *p_indent)
{
	// Disregard indent by rounding down to page boundary. Works universally:
	//
	//   - Small / large: chunk's base aligned to 2 MiB && p >= base + PAGE_SZ.
	//   - Huge: p aligned to 2 MiB && MAX_INDENT < PAGE_SZ.
	//
	// A huge allocations is thus rounded to its actual p (aligned to 2 MiB),
	// but a small or large allocation is never rounded to the chunk's base.

	const void *p = (const void *)((uint64_t)p_indent & -PAGE_SZ);

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

// Map a 64-bit address to a 12-bit site ID.

static uint32_t
hook_get_site_id(const void *ra)
{
	uint32_t site_id = cf_hash_ptr32(&ra) & (MAX_SITES - 1);

	for (uint32_t i = 0; i < MAX_SITES; ++i) {
		const void *site_ra = as_load_rlx(g_site_ras + site_id);

		// The allocation site is already registered and we found its
		// slot. Return the slot index.

		if (site_ra == ra) {
			return site_id;
		}

		// We reached an empty slot, i.e., the allocation site isn't yet
		// registered. Try to register it. If somebody else managed to grab
		// this slot in the meantime, keep looping, unless they grabbed it
		// for the same address as ours. Otherwise return the slot index.

		if (site_ra == NULL) {
			if (as_cas_rlx(g_site_ras + site_id, &site_ra, ra)) {
				// We took the empty slot.
				as_incr_uint32(&g_n_site_ras);
				return site_id;
			}

			if (site_ra == ra) {
				// Somebody else took the empty slot, but for the same address.
				return site_id;
			}
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
	uint64_t info_id = as_faa_uint64(&g_n_site_infos, 1);

	if (info_id < MAX_SITES * MAX_THREADS) {
		return (uint32_t)info_id;
	}

	if (info_id == MAX_SITES * MAX_THREADS) {
		cf_warning(CF_ALLOC, "site info pool exhausted");
	}

	return 0;
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

	if ((info_id = hook_new_site_info_id()) == 0) {
		return 0;
	}

	site_info *info = g_site_infos + info_id;

	info->thread_id = cf_thread_sys_tid();
	info->size_lo = 0;
	info->size_hi = 0;

	// Set site_id field last - it acts as a "site_info is valid" flag.
	as_store_uint32_rls(&info->site_id, site_id);

	g_thread_site_infos[site_id] = info_id;
	return info_id;
}

// Account for an allocation by the current thread for the allocation site
// with the given address.

static void
hook_handle_alloc(const void *ra, void *p, void *p_indent, size_t sz)
{
	if (p == NULL) {
		return;
	}

	size_t jem_sz = jem_sallocx(p, 0);

	uint32_t site_id = hook_get_site_id(ra);
	uint32_t info_id = hook_get_site_info_id(site_id);

	if (info_id != 0) {
		site_info *info = g_site_infos + info_id;

		size_t size_lo = info->size_lo;
		info->size_lo += jem_sz;

		// Carry?

		if (info->size_lo < size_lo) {
			++info->size_hi;
		}
	}

	uint8_t *data = (uint8_t *)p + jem_sz - sizeof(uint32_t);
	uint32_t *data32 = (uint32_t *)data;

	uint8_t *mark = (uint8_t *)p_indent + sz;
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

static void
hook_handle_free_check(const void* ra, const void* p, size_t jem_sz)
{
	uint8_t *data = (uint8_t *)p + jem_sz - sizeof(uint32_t);
	uint32_t *data32 = (uint32_t *)data;

	uint32_t val = (*data32 - 1) * MULT_INV;
	uint32_t site_id = val >> 16;
	uint32_t delta = val & 0xffff;

	if (site_id >= MAX_SITES) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA 0x%lx, invalid site ID",
				jem_sz, p, cf_strip_aslr(ra));
	}

	if (delta == 0xffff) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA 0x%lx, potential double free, possibly freed before with RA 0x%lx",
				jem_sz, p, cf_strip_aslr(ra),
				cf_strip_aslr(as_load_rlx(g_site_ras + site_id)));
	}

	if (delta > jem_sz - sizeof(uint32_t)) {
		cf_crash(CF_ALLOC, "corruption %zu@%p RA 0x%lx, invalid delta length, possibly allocated with RA 0x%lx",
				jem_sz, p, cf_strip_aslr(ra),
				cf_strip_aslr(as_load_rlx(g_site_ras + site_id)));
	}

	uint8_t *mark = data - delta;

	for (uint32_t i = 0; i < 4 && i < delta; ++i) {
		if (mark[i] != data[i]) {
			cf_crash(CF_ALLOC, "corruption %zu@%p RA 0x%lx, invalid mark, possibly allocated with RA 0x%lx",
					jem_sz, p, cf_strip_aslr(ra),
					cf_strip_aslr(as_load_rlx(g_site_ras + site_id)));
		}
	}
}

// Account for a deallocation by the current thread for the allocation
// site with the given address.

static void
hook_handle_free(const void *ra, void *p, void *p_user, size_t jem_sz,
		bool poison)
{
	hook_handle_free_check(ra, p, jem_sz);

	uint8_t *data = (uint8_t *)p + jem_sz - sizeof(uint32_t);
	uint32_t *data32 = (uint32_t *)data;

	uint32_t val = (*data32 - 1) * MULT_INV;
	uint32_t site_id = val >> 16;
	uint32_t delta = val & 0xffff;

	uint32_t info_id = hook_get_site_info_id(site_id);

	if (info_id != 0) {
		site_info *info = g_site_infos + info_id;

		size_t size_lo = info->size_lo;
		info->size_lo -= jem_sz;

		// Borrow?

		if (info->size_lo > size_lo) {
			--info->size_hi;
		}
	}

	// Replace the allocation site with the deallocation site to facilitate
	// double-free debugging.

	site_id = hook_get_site_id(ra);

	// Also invalidate the delta length, so that we are more likely to detect
	// double frees.

	uint8_t *mark = data - delta;

	*data32 = ((site_id << 16) | 0xffff) * MULT + 1;

	for (uint32_t i = 0; i < 4 && i < delta; ++i) {
		mark[i] = data[i];
	}

	if (poison) {
		size_t sz = (size_t)(mark - (uint8_t *)p_user);

		dead_memset(p_user, POISON_CHAR, sz > MAX_POISON_SZ ?
				MAX_POISON_SZ : sz);
	}
}

static void
hook_quarantine_or_free(const void *ra, void *p, size_t jem_sz, int32_t flags)
{
	if (g_quarantine == 0 || jem_sz > MAX_POISON_SZ) {
		jem_sdallocx(p, jem_sz, flags);
		return;
	}

	uint32_t i;
	quarantined *q;

	for (i = 0; i < g_quarantine; i++) {
		uint64_t n = as_faa_uint64(&g_n_quarantined, 1);
		q = g_quarantined + n % g_quarantine;

		if (cf_mutex_trylock(&q->mutex)) {
			break;
		}
	}

	if (i == g_quarantine) {
		jem_sdallocx(p, jem_sz, flags);
		return;
	}

	if (q->p != NULL) {
		uint32_t hash = cf_wyhash32(q->p, q->jem_sz);

		if (hash != q->hash) {
			cf_thread_run_fn run = cf_thread_get_run_fn(q->thread_id);

			cf_crash_nostack(CF_ALLOC,
				"use after free in %u@%p, quarantined on thread 0x%lx with RA 0x%lx",
				q->jem_sz, q->p, cf_strip_aslr(run),
				cf_strip_aslr(as_load_rlx(g_site_ras + q->site_id)));
		}

		jem_sdallocx(q->p, q->jem_sz, q->flags);
	}

	volatile uint8_t *p2 = p;
	uint64_t off = (((uint64_t)p + (PAGE_SZ - 1)) & -PAGE_SZ) - (uint64_t)p;

	*p2 = *p2; // @suppress("Assignment to itself") for Eclipse

	for (uint64_t k = off; k < jem_sz; k += PAGE_SZ) {
		*(p2 + k) = *(p2 + k); // @suppress("Assignment to itself") for Eclipse
	}

	q->site_id = hook_get_site_id(ra);
	q->thread_id = cf_thread_sys_tid();
	q->p = p;
	q->jem_sz = (uint32_t)jem_sz;
	q->flags = flags;
	q->hash = cf_wyhash32(p, jem_sz);

	cf_mutex_unlock(&q->mutex);
}

static uint32_t
indent_hops(void *p)
{
	if (!g_rand.initialized) {
		g_rand.seed0 = (uint64_t)cf_thread_sys_tid();
		g_rand.seed1 = cf_getns();
		g_rand.initialized = true;
	}

	uint32_t n_hops;
	void **p_indent;

	// Indented pointer must not look like aligned allocation. See outdent().

	do {
		n_hops = 2 + (as_random_next_uint32(&g_rand) % (MAX_INDENT / 8));
		n_hops &= ~1U; // make it even - results in 16-byte aligned indents
		p_indent = (void **)p + n_hops;
	}
	while (((uint64_t)p_indent & (PAGE_SZ - 1)) == 0);

	return n_hops;
}

static void *
indent(void *p)
{
	if (p == NULL) {
		return NULL;
	}

	uint32_t n_hops = indent_hops(p);
	uint64_t *p_indent = (uint64_t *)p + n_hops;

	p_indent[-1] = (uint64_t)p * MULT_64;
	*(uint64_t *)p = (uint64_t)p_indent * MULT_64;

	return (void *)p_indent;
}

static void *
reindent(void *p2, size_t sz, void *p, void *p_indent)
{
	if (p2 == NULL) {
		return NULL;
	}

	uint32_t n_hops = (uint32_t)(((uint8_t *)p_indent - (uint8_t *)p)) / 8;
	void **from = (void **)p2 + n_hops;

	uint32_t n_hops2 = indent_hops(p2);
	uint64_t *p2_indent = (uint64_t *)p2 + n_hops2;

	memmove(p2_indent, from, sz);

	p2_indent[-1] = (uint64_t)p2 * MULT_64;
	*(uint64_t *)p2 = (uint64_t)p2_indent * MULT_64;

	return (void *)p2_indent;
}

static void *
outdent(void *p_indent)
{
	// Aligned allocations aren't indented.

	if (((uint64_t)p_indent & (PAGE_SZ - 1)) == 0) {
		return p_indent;
	}

	uint64_t p = ((uint64_t *)p_indent)[-1] * MULT_INV_64;
	int64_t diff = (int64_t)p_indent - (int64_t)p;

	if (diff < 16 || diff > MAX_INDENT || diff % 8 != 0) {
		cf_crash(CF_ALLOC, "bad free of %p via %p", (void *)p, p_indent);
	}

	uint64_t p_expect = *(uint64_t *)p * MULT_INV_64;

	if ((uint64_t)p_indent != p_expect) {
		cf_crash(CF_ALLOC, "bad free of %p via %p (vs. %p)", (void *)p,
				p_indent, (void *)p_expect);
	}

	return (void *)p;
}

static void
page_size_check(void)
{
	int64_t page_sz = sysconf(_SC_PAGESIZE);

	if (page_sz < 0) {
		cf_crash_nostack(CF_ALLOC, "failed to get kernel page size: %d (%s)",
			errno, cf_strerror(errno));
	}

	if ((uint64_t)page_sz != PAGE_SZ) {
		cf_crash_nostack(CF_ALLOC, "bad kernel page size %ld, expected %lu",
			page_sz, PAGE_SZ);
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
	// non-standard functions, e.g., cf_alloc_try_malloc().
	//
	// As we use both, standard and non-standard functions, to allocate memory,
	// we would end up with an inconsistent mix of allocations, some allocated
	// by JEMalloc and some by glibc's allocator.
	//
	// Sooner or later, we will thus end up passing a memory block allocated by
	// JEMalloc to free(), which Valgrind has redirected to glibc's allocator.

	uint32_t tries;

	void *p1[2];
	void *p2[2];

	for (tries = 0; tries < 2; ++tries) {
		p1[tries] = malloc(1); // known API function, possibly redirected
		p2[tries] = cf_alloc_try_malloc(1); // our own, never redirected

		// If both of the above allocations are handled by JEMalloc, then their
		// base addresses will be identical (cache enabled), contiguous (cache
		// disabled), or unrelated (cache disabled, different runs). Trying
		// twice prevents the latter.
		//
		// If the first allocation is handled by glibc, then the base addresses
		// will always be unrelated.

		ptrdiff_t diff = (uint8_t *)p2[tries] - (uint8_t *)p1[tries];

		if (diff > -1024 && diff < 1024) {
			break;
		}
	}

	if (tries == 2) {
		cf_crash_nostack(CF_ALLOC, "Valgrind redirected malloc() to glibc; please run Valgrind with --soname-synonyms=somalloc=nouserintercepts");
	}

	for (uint32_t i = 0; i < tries; ++i) {
		free(p1[tries]);
		cf_free(p2[tries]);
	}
}

void
cf_alloc_init(void)
{
	page_size_check();
	valgrind_check();

	// Turn off libstdc++'s memory caching, as it just duplicates JEMalloc's.

	if (setenv("GLIBCXX_FORCE_NEW", "1", 1) < 0) {
		cf_crash(CF_ALLOC, "setenv() failed: %d (%s)", errno, cf_strerror(errno));
	}

	// Double-check that hook_get_arena() works, as it depends on JEMalloc's
	// internal data structures.

	int err = jem_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);

	if (err != 0) {
		cf_crash(CF_ALLOC, "error while flushing thread cache: %d (%s)", err, cf_strerror(err));
	}

	for (size_t sz = 1; sz <= 16 * 1024 * 1024; sz *= 2) {
		void *p = malloc(sz);
		int32_t arena = hook_get_arena(p);

		if (arena != N_ARENAS) {
			cf_crash(CF_ALLOC, "arena mismatch: %d vs. %d", arena, N_ARENAS / 2);
		}

		free(p);
	}
}

static void
prepare_quarantine(void)
{
	size_t size = (g_quarantine * sizeof(quarantined) + PAGE_SZ - 1) &
		-(size_t)PAGE_SZ;

	g_quarantined = mmap(NULL, size, PROT_READ | PROT_WRITE,
		MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

	if (g_quarantined == MAP_FAILED) {
		cf_crash(CF_ALLOC, "failed to map %zu bytes for quarantine", size);
	}

	for (size_t i = 0; i < g_quarantine; i++) {
		quarantined *q = g_quarantined + i;

		cf_mutex_init(&q->mutex);
	}
}

void
cf_alloc_set_debug(bool debug_allocations, bool indent_allocations,
		bool poison_allocations, uint32_t quarantine_allocations)
{
	g_debug = debug_allocations;
	g_indent = indent_allocations;
	g_poison = poison_allocations;
	g_quarantine = quarantine_allocations;

	if (quarantine_allocations != 0) {
		prepare_quarantine();
	}

	g_alloc_started = true;
}

void
cf_alloc_heap_stats(size_t *allocated_kbytes, size_t *active_kbytes, size_t *mapped_kbytes,
		double *efficiency_pct, uint32_t *site_count)
{
	uint64_t epoch = 1;
	size_t len = sizeof(epoch);

	int err = jem_mallctl("epoch", &epoch, &len, &epoch, len);

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
		*efficiency_pct = active != 0 ?
				(double)allocated * 100.0 / (double)active : 0.0;
	}

	if (site_count) {
		*site_count = as_load_uint32(&g_n_site_ras);
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
	uint64_t n_site_infos = as_load_uint64(&g_n_site_infos);

	if (n_site_infos > MAX_SITES * MAX_THREADS) {
		n_site_infos = MAX_SITES * MAX_THREADS;
	}

	for (uint64_t i = 1; i < n_site_infos; ++i) {
		site_info *info = g_site_infos + i;
		// See corresponding as_store_rls() in hook_get_site_info_id().
		uint32_t site_id = as_load_uint32_acq(&info->site_id);

		if (site_id == 0) {
			continue;
		}

		const void *ra = as_load_rlx(g_site_ras + site_id);

		fprintf(fh, "0x%016" PRIx64 " %9d 0x%016zx 0x%016zx\n",
				cf_strip_aslr(ra), info->thread_id, info->size_hi,
				info->size_lo);
	}

	fclose(fh);
}

static bool
want_debug_alloc(void)
{
	// Exempt allocations during the startup phase.
	return g_debug && g_alloc_started;
}

static bool
want_debug_free(int32_t arena)
{
	// Expect the startup arena during the startup phase.
	cf_assert(g_alloc_started || arena == N_ARENAS, CF_ALLOC,
			"bad arena %d during startup", arena);

	// Exempt allocations in the startup arena.
	return g_debug && arena != N_ARENAS;
}

static int32_t
calc_free_flags(int32_t arena)
{
	// Expect the startup arena during the startup phase.
	cf_assert(g_alloc_started || arena == N_ARENAS, CF_ALLOC,
			"bad arena %d during startup", arena);

	// Bypass the thread-local cache for allocations in the startup arena.
	return arena == g_startup_arena ? MALLOCX_TCACHE_NONE : 0;
}

static void
do_free(void *p_indent, const void *ra)
{
	if (p_indent == NULL) {
		return;
	}

	int32_t arena = hook_get_arena(p_indent);
	int32_t flags = calc_free_flags(arena);

	if (!want_debug_free(arena)) {
		jem_dallocx(p_indent, flags); // not indented
		return;
	}

	void *p = g_indent ? outdent(p_indent) : p_indent;
	size_t jem_sz = jem_sallocx(p, 0);

	hook_handle_free(ra, p, p_indent, jem_sz, g_poison);
	hook_quarantine_or_free(ra, p, jem_sz, flags);
}

void
__attribute__ ((noinline))
free(void *p_indent)
{
	do_free(p_indent, __builtin_return_address(0));
}

static int32_t
calc_alloc_flags(int32_t flags)
{
	if (g_alloc_started) {
		// Default arena - no additional flags needed.
		return flags;
	}

	// During startup, allocate from the startup arena and bypass the
	// thread-local cache.

	if (g_startup_arena < 0) {
		size_t len = sizeof(g_startup_arena);

		int err = jem_mallctl("arenas.extend", &g_startup_arena, &len,
				NULL, 0);

		if (err != 0) {
			cf_crash(CF_ALLOC, "failed to create startup arena: %d (%s)",
					err, cf_strerror(err));
		}

		// Expect arena 149.
		cf_assert(g_startup_arena == N_ARENAS, CF_ALLOC,
				"bad startup arena %d", g_startup_arena);
	}

	// Set startup arena, bypass thread-local cache.
	flags |= MALLOCX_ARENA(g_startup_arena) | MALLOCX_TCACHE_NONE;

	return flags;
}

static void *
do_mallocx(size_t sz, const void *ra)
{
	int32_t flags = calc_alloc_flags(0);

	if (!want_debug_alloc()) {
		return jem_mallocx(sz == 0 ? 1 : sz, flags);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	if (g_indent) {
		ext_sz += MAX_INDENT;
	}

	void *p = jem_mallocx(ext_sz, flags);
	void *p_indent = g_indent ? indent(p) : p;

	hook_handle_alloc(ra, p, p_indent, sz);

	if (g_poison) {
		memset(p_indent, POISON_CHAR, sz > MAX_POISON_SZ ? MAX_POISON_SZ : sz);
	}

	return p_indent;
}

void *
cf_alloc_try_malloc(size_t sz)
{
	// Allowed to return NULL.
	return do_mallocx(sz, __builtin_return_address(0));
}

void *
__attribute__ ((noinline))
malloc(size_t sz)
{
	void *p_indent = do_mallocx(sz, __builtin_return_address(0));

	cf_assert(p_indent != NULL, CF_ALLOC, "malloc failed sz %zu", sz);

	return p_indent;
}

static void *
do_callocx(size_t n, size_t sz, const void *ra)
{
	int32_t flags = calc_alloc_flags(MALLOCX_ZERO);
	size_t tot_sz = n * sz;

	if (!want_debug_alloc()) {
		return jem_mallocx(tot_sz == 0 ? 1 : tot_sz, flags);
	}

	size_t ext_sz = tot_sz + sizeof(uint32_t);

	if (g_indent) {
		ext_sz += MAX_INDENT;
	}

	void *p = jem_mallocx(ext_sz, flags);
	void *p_indent = g_indent ? indent(p) : p;

	hook_handle_alloc(ra, p, p_indent, tot_sz);

	return p_indent;
}

void *
calloc(size_t n, size_t sz)
{
	void *p_indent = do_callocx(n, sz, __builtin_return_address(0));

	cf_assert(p_indent != NULL, CF_ALLOC, "calloc failed n %zu sz %zu", n, sz);

	return p_indent;
}

static void *
do_rallocx(void *p_indent, size_t sz, const void *ra)
{
	if (p_indent == NULL) {
		return do_mallocx(sz, ra);
	}

	int32_t arena_p = hook_get_arena(p_indent);

	bool debug_p = want_debug_free(arena_p);
	bool debug = want_debug_alloc();

	// Allow debug change for startup arena - handled below.

	if (debug != debug_p && arena_p != N_ARENAS) {
		cf_crash(CF_ALLOC, "debug change - p_indent %p arena_p %d", p_indent,
				arena_p);
	}

	if (sz == 0) {
		do_free(p_indent, ra);
		return NULL;
	}

	int32_t flags = calc_alloc_flags(0);

	// Going from startup or non-debug arena to non-debug arena.

	if (!debug) {
		return jem_rallocx(p_indent, sz, flags); // not indented
	}

	// Going from startup arena to debug arena.

	if (arena_p == N_ARENAS) {
		void *p = p_indent; // not indented
		void *p_move = do_mallocx(sz, ra);

		size_t sz_move = jem_sallocx(p, 0);

		if (sz < sz_move) {
			sz_move = sz;
		}

		memcpy(p_move, p, sz_move);
		cf_free(p);

		return p_move;
	}

	// Going from debug arena to debug arena.

	void *p = g_indent ? outdent(p_indent) : p_indent;
	size_t jem_sz = jem_sallocx(p, 0);

	hook_handle_free(ra, p, p_indent, jem_sz, false);

	size_t ext_sz = sz + sizeof(uint32_t);

	if (g_indent) {
		ext_sz += MAX_INDENT;
	}

	void *p2 = jem_rallocx(p, ext_sz, flags);
	void *p2_indent = g_indent ? reindent(p2, sz, p, p_indent) : p2;

	hook_handle_alloc(ra, p2, p2_indent, sz);

	return p2_indent;
}

void *
realloc(void *p_indent, size_t sz)
{
	void *p2_indent = do_rallocx(p_indent, sz, __builtin_return_address(0));

	cf_assert(p2_indent != NULL || sz == 0, CF_ALLOC, "realloc failed sz %zu",
			sz);

	return p2_indent;
}

static char *
do_strdup(const char *s, size_t n, const void *ra)
{
	int32_t flags = calc_alloc_flags(0);

	size_t sz = n + 1;
	size_t ext_sz = sz;

	if (want_debug_alloc()) {
		ext_sz += sizeof(uint32_t);

		if (g_indent) {
			ext_sz += MAX_INDENT;
		}
	}

	char *s2 = jem_mallocx(ext_sz, flags);
	char *s2_indent = s2;

	if (want_debug_alloc()) {
		if (g_indent) {
			s2_indent = indent(s2);
		}

		hook_handle_alloc(ra, s2, s2_indent, sz);
	}

	memcpy(s2_indent, s, n);
	s2_indent[n] = 0;

	return s2_indent;
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

	return do_strdup(s, n2, __builtin_return_address(0));
}

static int32_t
do_asprintf(char **res, const char *form, va_list va, const void *ra)
{
	char buff[25000];
	int32_t n = vsnprintf(buff, sizeof(buff), form, va);

	if ((size_t)n >= sizeof(buff)) {
		cf_crash(CF_ALLOC, "asprintf overflow len %d", n);
	}

	*res = do_strdup(buff, (size_t)n, ra);
	return n;
}

int32_t
asprintf(char **res, const char *form, ...)
{
	va_list va;
	va_start(va, form);

	int32_t n = do_asprintf(res, form, va, __builtin_return_address(0));

	va_end(va);
	return n;
}

int32_t
__asprintf_chk(char **res, int32_t flags, const char *form, ...)
{
	(void)flags;

	va_list va;
	va_start(va, form);

	int32_t n = do_asprintf(res, form, va, __builtin_return_address(0));

	va_end(va);
	return n;
}

// Helpers to make aligned allocations in the startup arena during startup.
// jem_aligned_alloc() and jem_posix_memalign() don't allow to specify an arena.

static void*
aligned_alloc_helper(size_t align, size_t sz)
{
	int32_t flags = calc_alloc_flags(MALLOCX_ALIGN(align));

	return jem_mallocx(sz, flags); // sz is never zero
}

static int32_t
posix_memalign_helper(void **p, size_t align, size_t sz)
{
	*p = aligned_alloc_helper(align, sz);

	if (*p == NULL) {
		return ENOMEM;
	}

	return 0;
}

int32_t
posix_memalign(void **p, size_t align, size_t sz)
{
	cf_assert((align & (align - 1)) == 0, CF_ALLOC, "bad alignment");

	if (!want_debug_alloc()) {
		return posix_memalign_helper(p, align, sz == 0 ? 1 : sz);
	}

	// When indentation is enabled, align to PAGE_SZ+ to mark as unindented.

	if (g_indent) {
		align = (align + (PAGE_SZ - 1)) & -PAGE_SZ;
	}

	size_t ext_sz = sz + sizeof(uint32_t);
	int32_t err = posix_memalign_helper(p, align, ext_sz);

	if (err != 0) {
		return err;
	}

	hook_handle_alloc(__builtin_return_address(0), *p, *p, sz);
	return 0;
}

void *
aligned_alloc(size_t align, size_t sz)
{
	cf_assert((align & (align - 1)) == 0, CF_ALLOC, "bad alignment");

	if (!want_debug_alloc()) {
		return aligned_alloc_helper(align, sz == 0 ? 1 : sz);
	}

	// When indentation is enabled, align to PAGE_SZ+ to mark as unindented.

	if (g_indent) {
		align = (align + (PAGE_SZ - 1)) & -PAGE_SZ;
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = aligned_alloc_helper(align, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, p, sz);

	return p;
}

static void *
do_valloc(size_t sz)
{
	if (!want_debug_alloc()) {
		return aligned_alloc_helper(PAGE_SZ, sz == 0 ? 1 : sz);
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = aligned_alloc_helper(PAGE_SZ, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, p, sz);

	if (g_poison) {
		memset(p, POISON_CHAR, sz > MAX_POISON_SZ ? MAX_POISON_SZ : sz);
	}

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
	cf_assert((align & (align - 1)) == 0, CF_ALLOC, "bad alignment");

	if (!want_debug_alloc()) {
		return aligned_alloc_helper(align, sz == 0 ? 1 : sz);
	}

	// When indentation is enabled, align to PAGE_SZ+ to mark as unindented.

	if (g_indent) {
		align = (align + (PAGE_SZ - 1)) & -PAGE_SZ;
	}

	size_t ext_sz = sz + sizeof(uint32_t);

	void *p = aligned_alloc_helper(align, ext_sz);
	hook_handle_alloc(__builtin_return_address(0), p, p, sz);

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

static void
do_validate_pointer(const void* p_indent, const void* ra)
{
	if (p_indent == NULL) {
		return;
	}

	int32_t arena = hook_get_arena(p_indent);

	if (! want_debug_free(arena)) {
		return;
	}

	const void *p = g_indent ? outdent((void*)p_indent) : p_indent;
	size_t jem_sz = jem_sallocx(p, 0);

	hook_handle_free_check(ra, p, jem_sz);
}

void
cf_validate_pointer(const void* p_indent)
{
	do_validate_pointer(p_indent, __builtin_return_address(0));
}

void
cf_trim_region_to_mapped(void **p, size_t *sz)
{
	cf_assert(*sz <= PAGE_SZ, CF_ALLOC, "bad size: %zu", *sz);

	void *lo = (void *)((uint64_t)*p & -PAGE_SZ);
	void *hi = (void *)(((uint64_t)*p + *sz - 1) & -PAGE_SZ);

	bool lo_ok = madvise(lo, PAGE_SZ, MADV_NORMAL) == 0;
	bool hi_ok = madvise(hi, PAGE_SZ, MADV_NORMAL) == 0;

	if (!lo_ok) {
		*sz -= (size_t)((uint8_t *)hi - (uint8_t *)*p);
		*p = hi;
	}

	if (!hi_ok) {
		*sz -= (size_t)(((uint8_t *)*p + *sz) - (uint8_t *)hi);
	}
}

void *
cf_rc_alloc(size_t sz)
{
	int32_t flags = calc_alloc_flags(0);

	size_t tot_sz = sizeof(cf_rc_header) + sz;
	size_t ext_sz = tot_sz;

	if (want_debug_alloc()) {
		ext_sz += sizeof(uint32_t);

		if (g_indent) {
			ext_sz += MAX_INDENT;
		}
	}

	void *p = jem_mallocx(ext_sz, flags);
	void *p_indent = p;

	if (want_debug_alloc()) {
		if (g_indent) {
			p_indent = indent(p);
		}

		hook_handle_alloc(__builtin_return_address(0), p, p_indent, tot_sz);

		if (g_poison) {
			memset(p_indent, POISON_CHAR, sz > MAX_POISON_SZ ?
					MAX_POISON_SZ : sz);
		}
	}

	cf_rc_header *head = p_indent;

	head->rc = 1;
	head->sz = (uint32_t)sz;

	return head + 1; // body
}

static void
do_rc_free(void *body, void *ra)
{
	if (body == NULL) {
		cf_crash(CF_ALLOC, "trying to cf_rc_free() null pointer");
	}

	cf_rc_header *head = (cf_rc_header *)body - 1;

	int32_t arena = hook_get_arena(head);
	int32_t flags = calc_free_flags(arena);

	if (!want_debug_free(arena)) {
		jem_dallocx(head, flags); // not indented
		return;
	}

	void *p = g_indent ? outdent(head) : head;
	size_t jem_sz = jem_sallocx(p, 0);

	hook_handle_free(ra, p, body, jem_sz, g_poison);
	hook_quarantine_or_free(ra, p, jem_sz, flags);
}

void
cf_rc_free(void *body)
{
	do_rc_free(body, __builtin_return_address(0));
}

uint32_t
cf_rc_reserve(void *body)
{
	cf_rc_header *head = (cf_rc_header *)body - 1;
	return as_aaf_uint32(&head->rc, 1);
}

uint32_t
cf_rc_release(void *body)
{
	cf_rc_header *head = (cf_rc_header *)body - 1;
	uint32_t rc = as_aaf_uint32_rls(&head->rc, -1);

	cf_assert(rc != (uint32_t)-1, CF_ALLOC, "reference count underflow");

	if (rc == 0) {
		// Subsequent destructor may require an 'acquire' barrier.
		as_fence_acq();
	}

	return rc;
}

uint32_t
cf_rc_releaseandfree(void *body)
{
	cf_rc_header *head = (cf_rc_header *)body - 1;
	uint32_t rc = as_aaf_uint32_rls(&head->rc, -1);

	cf_assert(rc != (uint32_t)-1, CF_ALLOC, "reference count underflow");

	if (rc != 0) {
		return rc;
	}

	do_rc_free(body, __builtin_return_address(0));
	return 0;
}

uint32_t
cf_rc_count(const void *body)
{
	const cf_rc_header *head = (const cf_rc_header *)body - 1;
	return head->rc;
}
