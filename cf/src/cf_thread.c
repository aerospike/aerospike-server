/*
 * cf_thread.c
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

//==========================================================
// Includes.
//

#include "cf_thread.h"

#include <pthread.h>
#include <sys/types.h>

#include "fault.h"


//==========================================================
// Globals.
//

__thread pid_t g_sys_tid = 0;

static pthread_attr_t g_attr_detached;


//==========================================================
// Public API.
//

void
cf_thread_init(void)
{
	// TODO - check system thread limit and warn if too low?

	pthread_attr_init(&g_attr_detached);
	pthread_attr_setdetachstate(&g_attr_detached, PTHREAD_CREATE_DETACHED);
}

cf_tid
cf_thread_create_detached(cf_thread_run_fn run, void* udata)
{
	pthread_t tid;
	int result = pthread_create(&tid, &g_attr_detached, run, udata);

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
	pthread_t tid;
	int result = pthread_create(&tid, NULL, run, udata);

	if (result != 0) {
		// Non-zero return values are errno values.
		cf_crash(CF_MISC, "failed to create joinable thread: %d (%s)", result,
				cf_strerror(result));
	}

	return (cf_tid)tid;
}
