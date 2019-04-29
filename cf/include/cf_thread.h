/*
 * cf_thread.h
 *
 * Copyright (C) 2018 Aerospike, Inc.
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

#include <pthread.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

typedef pthread_t cf_tid;
typedef void* (*cf_thread_run_fn) (void* udata);


//==========================================================
// Globals.
//

extern __thread pid_t g_sys_tid;


//==========================================================
// Public API.
//

void cf_thread_init(void);
cf_tid cf_thread_create_detached(cf_thread_run_fn run, void* udata);
cf_tid cf_thread_create_joinable(cf_thread_run_fn run, void* udata);
int32_t cf_thread_traces(char* key, cf_dyn_buf* db);
void cf_thread_traces_action(int32_t sig_num, siginfo_t* info, void* ctx);

static inline void
cf_thread_join(cf_tid tid)
{
	pthread_join(tid, NULL);
}

static inline void
cf_thread_cancel(cf_tid tid)
{
	pthread_cancel(tid);
}

static inline void
cf_thread_disable_cancel(void)
{
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
}

static inline void
cf_thread_test_cancel(void)
{
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	pthread_testcancel();
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
}

// Prefer this to cf_tid (i.e. pthread_t) for logging, etc.
static inline pid_t
cf_thread_sys_tid(void)
{
	if (g_sys_tid == 0) {
		g_sys_tid = (pid_t)syscall(SYS_gettid);
	}

	return g_sys_tid;
}
