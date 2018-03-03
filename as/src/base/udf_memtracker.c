/*
 * udf_memtracker.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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


#include <pthread.h>

#include "fault.h"

#include "base/udf_memtracker.h"


/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static pthread_key_t   modules_tlskey = 0;
static as_memtracker   g_udf_memtracker;
static int
udf_memtracker_generic(mem_tracker *mt, const uint32_t num_bytes, memtracker_op op)
{
	if (!mt || !mt->udata || !mt->cb) {
		return false;
	}

	mt->cb(mt, num_bytes, op);
	if (op == MEM_RESERVE) {
		cf_detail(AS_UDF, "%ld: Memory Tracker %p reserved = %d (bytes)",
				  pthread_self(), mt, num_bytes);
	} else if (op == MEM_RELEASE) {
		cf_detail(AS_UDF, "%ld: Memory Tracker %p released = %d (bytes)",
				  pthread_self(), mt, num_bytes);
	} else {
		cf_detail(AS_UDF, "%ld: Memory Tracker %p reset",
				  pthread_self(), mt);
	}
	return 0;
}

void
udf_memtracker_setup(mem_tracker *mt)
{
	pthread_setspecific(modules_tlskey, mt);
	cf_detail(AS_UDF, "%ld: Memory Tracker %p set", pthread_self(), mt);
}

void
udf_memtracker_cleanup()
{
	pthread_setspecific(modules_tlskey, NULL);
	cf_detail(AS_UDF, "%ld: Memory Tracker reset", pthread_self());
}

static bool
udf_memtracker_reset(const as_memtracker *as_mt) {
	mem_tracker *mt = (mem_tracker *)pthread_getspecific(modules_tlskey);
	return udf_memtracker_generic(mt, 0, MEM_RESET);

}

static bool
udf_memtracker_reserve(const as_memtracker *as_mt, const uint32_t num_bytes)
{
	mem_tracker *mt = (mem_tracker *)pthread_getspecific(modules_tlskey);
	return udf_memtracker_generic(mt, num_bytes, MEM_RESERVE);
}

static bool
udf_memtracker_release(const as_memtracker *as_mt, const uint32_t num_bytes)
{
	mem_tracker *mt = (mem_tracker *)pthread_getspecific(modules_tlskey);
	return udf_memtracker_generic(mt, num_bytes, MEM_RELEASE);
}

static const as_memtracker_hooks udf_memtracker_hooks = {
	.destroy	= NULL,
	.reserve	= udf_memtracker_reserve,
	.release	= udf_memtracker_release,
	.reset		= udf_memtracker_reset
};

as_memtracker *
udf_memtracker_init()
{
	as_memtracker_init(&g_udf_memtracker, NULL, &udf_memtracker_hooks);
	return &g_udf_memtracker;
}
