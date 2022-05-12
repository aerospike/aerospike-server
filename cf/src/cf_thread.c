/*
 * cf_thread.c
 *
 * Copyright (C) 2018-2022 Aerospike, Inc.
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

#include "cf_thread.h"

#include <errno.h>
#include <execinfo.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef struct thread_req_s {
	cf_thread_run_fn run;
	void* udata;
} thread_req;

#define MAX_N_ADDRS 50

typedef struct thread_info_s {
	cf_ll_element link; // base object must be first
	cf_thread_run_fn run;
	void* udata;
	pid_t sys_tid;
	uint32_t n_addrs;
	void* addrs[MAX_N_ADDRS];
} thread_info;

typedef struct thread_alloc_s {
	void** pp;
	size_t* psz;
} thread_alloc;

typedef struct thread_exit_s {
	cf_thread_exit_fn cb;
	void* udata;
} thread_exit;


//==========================================================
// Globals.
//

__thread pid_t g_sys_tid = 0;

static pthread_attr_t g_attr_detached;

static cf_queue g_thread_req_q;
static cf_mutex g_pool_lock = CF_MUTEX_INIT;

static uint32_t g_n_joinable = 0;
static uint32_t g_n_detached = 0;
static uint32_t g_n_pool_total = 0;
static uint32_t g_n_pool_active = 0;

static cf_ll g_thread_list;
static __thread thread_info* g_thread_info;

static volatile uint32_t g_traces_pending;
static volatile uint32_t g_traces_done;

static __thread thread_alloc* g_allocs = NULL;
static __thread uint32_t g_n_allocs = 0;

static __thread thread_exit* g_exits = NULL;
static __thread uint32_t g_n_exits = 0;


//==========================================================
// Forward declarations.
//

static void* run_pool(void* udata);
static thread_info* make_thread_info(cf_thread_run_fn run, void* udata);
static void register_thread_info(void* udata);
static void deregister_thread_info(void);
static void* pool_shim_fn(void* udata);
static void* detached_shim_fn(void* udata);
static void* joinable_shim_fn(void* udata);
static int32_t collect_traces_cb(cf_ll_element* ele, void* udata);
static int32_t print_traces_cb(cf_ll_element* ele, void* udata);
static void cleanup(void);


//==========================================================
// Public API.
//

void
cf_thread_init(void)
{
	pthread_attr_init(&g_attr_detached);
	pthread_attr_setdetachstate(&g_attr_detached, PTHREAD_CREATE_DETACHED);

	cf_queue_init(&g_thread_req_q, sizeof(thread_req), 8, true);

	cf_ll_init(&g_thread_list, NULL, true);
}

void
cf_thread_create_transient(cf_thread_run_fn run, void* udata)
{
	cf_assert(g_alloc_started, CF_MISC, "started thread too early");

	thread_req treq = { .run = run, .udata = udata };

	cf_mutex_lock(&g_pool_lock);

	if (g_n_pool_active == g_n_pool_total) {
		g_n_pool_total++;

		thread_info* info = make_thread_info(run_pool, udata);
		pthread_t tid;
		int result = pthread_create(&tid, &g_attr_detached, pool_shim_fn, info);

		if (result != 0) {
			// Non-zero return values are errno values.
			cf_crash(CF_MISC, "failed to create pool thread: %d (%s)", result,
					cf_strerror(result));
		}
	}

	as_incr_uint32(&g_n_pool_active);
	cf_queue_push(&g_thread_req_q, &treq);

	cf_mutex_unlock(&g_pool_lock);
}

cf_tid
cf_thread_create_detached(cf_thread_run_fn run, void* udata)
{
	cf_assert(g_alloc_started, CF_MISC, "started thread too early");

	as_incr_uint32(&g_n_detached);

	thread_info* info = make_thread_info(run, udata);
	pthread_t tid;
	int result = pthread_create(&tid, &g_attr_detached, detached_shim_fn, info);

	if (result != 0) {
		// Non-zero return values are errno values.
		cf_crash(CF_MISC, "failed to create detached thread: %d (%s)", result,
				cf_strerror(result));
	}

	return (cf_tid)tid;
}

cf_tid
cf_thread_create_joinable(cf_thread_run_fn run, void* udata)
{
	cf_assert(g_alloc_started, CF_MISC, "started thread too early");

	as_incr_uint32(&g_n_joinable);

	thread_info* info = make_thread_info(run, udata);
	pthread_t tid;
	int result = pthread_create(&tid, NULL, joinable_shim_fn, info);

	if (result != 0) {
		// Non-zero return values are errno values.
		cf_crash(CF_MISC, "failed to create joinable thread: %d (%s)", result,
				cf_strerror(result));
	}

	return (cf_tid)tid;
}

void
cf_thread_get_stats(cf_thread_stats* stats)
{
	stats->n_joinable = g_n_joinable;
	stats->n_detached = g_n_detached;
	stats->n_pool_total = g_n_pool_total;
	stats->n_pool_active = g_n_pool_active;
}

int32_t
cf_thread_traces(char* key, cf_dyn_buf* db)
{
	(void)key;

	g_traces_pending = 0;
	g_traces_done = 0;

	cf_ll_reduce(&g_thread_list, true, collect_traces_cb, NULL);

	// Quit after 15 seconds - may not get all done if a thread exits after
	// we signal it but before its action is handled.
	for (uint32_t i = 0; i < 1500; i++) {
		if (g_traces_done == g_traces_pending) {
			break;
		}

		usleep(10 * 1000);
	}

	cf_ll_reduce(&g_thread_list, true, print_traces_cb, db);
	cf_dyn_buf_chomp(db);

	return 0;
}

void
cf_thread_traces_action(int32_t sig_num, siginfo_t* info, void* ctx)
{
	(void)sig_num;
	(void)info;
	(void)ctx;

	g_thread_info->n_addrs = (uint32_t)backtrace(g_thread_info->addrs,
			MAX_N_ADDRS);
	g_traces_done++;
}

void
cf_thread_realloc(void** pp, size_t* psz)
{
	for (uint32_t i = 0; i < g_n_allocs; i++) {
		thread_alloc* talloc = &g_allocs[i];

		if (pp == talloc->pp) {
			cf_assert(psz == talloc->psz, CF_MISC, "unmatched realloc psz");

			*pp = cf_realloc(*pp, *psz);
			// Don't bother save size - only needs to be zeroed on cleanup.

			if (*pp == NULL) {
				uint32_t last_i = g_n_allocs - 1;

				*talloc = g_allocs[last_i]; // no-op if i == last_i
				g_n_allocs--;
			}

			return;
		}
	}

	cf_assert(*pp == NULL, CF_MISC, "non-null pointer for new alloc");

	*pp = cf_malloc(*psz);

	g_allocs = cf_realloc(g_allocs, (g_n_allocs + 1) * sizeof(thread_alloc));

	thread_alloc new_talloc = { .pp = pp, .psz = psz };

	g_allocs[g_n_allocs++] = new_talloc;
}

void
cf_thread_add_exit(cf_thread_exit_fn cb, void* udata)
{
	g_exits = cf_realloc(g_exits, (g_n_exits + 1) * sizeof(thread_exit));

	thread_exit texit = { .cb = cb, .udata = udata };

	g_exits[g_n_exits++] = texit;
}

void
cf_thread_remove_exit(cf_thread_exit_fn cb)
{
	for (uint32_t i = 0; i < g_n_exits; i++) {
		if (cb == g_exits[i].cb) {
			uint32_t last_i = g_n_exits - 1;

			if (i != last_i) {
				g_exits[i] = g_exits[last_i];
			}

			g_n_exits--;

			return;
		}
	}
}


//==========================================================
// Local helpers.
//

static void*
run_pool(void* udata)
{
	(void)udata;

	while (true) {
		thread_req treq;

		cf_queue_pop(&g_thread_req_q, &treq, CF_QUEUE_FOREVER);

		treq.run(treq.udata);

		cleanup();
		as_decr_uint32(&g_n_pool_active);
	}

	return NULL;
}

static thread_info*
make_thread_info(cf_thread_run_fn run, void* udata)
{
	thread_info* info = cf_calloc(1, sizeof(thread_info));

	info->run = run;
	info->udata = udata;

	return info;
}

static void
register_thread_info(void* udata)
{
	g_thread_info = (thread_info*)udata;
	g_thread_info->sys_tid = cf_thread_sys_tid();

	cf_ll_append(&g_thread_list, &g_thread_info->link);
}

static void
deregister_thread_info(void)
{
	cf_ll_delete(&g_thread_list, &g_thread_info->link);
	cf_free(g_thread_info);
}

static void*
pool_shim_fn(void* udata)
{
	register_thread_info(udata);

	g_thread_info->run(g_thread_info->udata);

	// Note - for now the pool never shrinks, so we never get here!
	cf_crash(CF_MISC, "unexpected - pool thread exited");

	return NULL;
}

static void*
detached_shim_fn(void* udata)
{
	register_thread_info(udata);

	void* rv = g_thread_info->run(g_thread_info->udata);

	// Prevent crashes in glibc 2.24 for short-lived detached threads.
	usleep(100 * 1000);

	cleanup();
	deregister_thread_info();
	as_decr_uint32(&g_n_detached);

	return rv;
}

static void*
joinable_shim_fn(void* udata)
{
	register_thread_info(udata);

	void* rv = g_thread_info->run(g_thread_info->udata);

	cleanup();
	deregister_thread_info();
	as_decr_uint32(&g_n_joinable);

	return rv;
}

static int32_t
collect_traces_cb(cf_ll_element* ele, void* udata)
{
	(void)udata;

	thread_info* info = (thread_info*)ele;

	if (syscall(SYS_tgkill, getpid(), info->sys_tid, SIGUSR2) < 0) {
		cf_warning(CF_MISC, "failed to signal thread %d: %d (%s)",
				info->sys_tid, errno, cf_strerror(errno));
		return 0;
	}

	g_traces_pending++;

	return 0;
}

static int32_t
print_traces_cb(cf_ll_element* ele, void* udata)
{
	thread_info* info = (thread_info*)ele;
	cf_dyn_buf* db = (cf_dyn_buf*)udata;

	cf_dyn_buf_append_format(db, "---------- %d (0x%lx) ----------;",
			info->sys_tid, cf_log_strip_aslr(info->run));

	if (info->n_addrs == 0) { // race: thread created after we sent SIGUSR2
		return 0;
	}

	char** syms = backtrace_symbols(info->addrs, (int32_t)info->n_addrs);

	if (syms == NULL) {
		cf_dyn_buf_append_format(db, "failed;");
	}
	else {
		for (uint32_t i = 0; i < info->n_addrs; i++) {
			cf_dyn_buf_append_format(db, "%s;", syms[i]);
		}

		free(syms);
	}

	info->n_addrs = 0;

	return 0;
}

static void
cleanup(void)
{
	if (g_allocs != NULL) {
		for (uint32_t i = 0; i < g_n_allocs; i++) {
			thread_alloc* talloc = &g_allocs[i];

			cf_free(*talloc->pp);

			*talloc->pp = NULL;
			*talloc->psz = 0;
		}

		cf_free(g_allocs);

		g_allocs = NULL;
		g_n_allocs = 0;
	}

	if (g_exits != NULL) {
		for (uint32_t i = 0; i < g_n_exits; i++) {
			thread_exit* texit = &g_exits[i];

			texit->cb(texit->udata);
		}

		cf_free(g_exits);

		g_exits = NULL;
		g_n_exits = 0;
	}
}
