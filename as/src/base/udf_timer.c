/*
 * udf_timer.c
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

#include "base/udf_timer.h"

#include <pthread.h>

#include "citrusleaf/cf_clock.h"

#include "fault.h"


/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static pthread_key_t   timer_tlskey = 0;
static pthread_once_t  key_once = PTHREAD_ONCE_INIT;

static void
udf_make_key()
{
	pthread_key_create(&timer_tlskey, NULL);
}

void
udf_timer_setup(time_tracker *tt)
{
	pthread_once(&key_once, udf_make_key);
	pthread_setspecific(timer_tlskey, tt);
	cf_detail(AS_UDF, "tid=%ld tt=%p", pthread_self(), tt);
}

void
udf_timer_cleanup()
{
	pthread_setspecific(timer_tlskey, NULL);
	cf_detail(AS_UDF, "tid=%ld", pthread_self());
}

bool
udf_timer_timedout(const as_timer * timer)
{
	time_tracker *tt = (time_tracker *)pthread_getspecific(timer_tlskey);
	cf_detail(AS_UDF, "tid=%ld tt=%p", pthread_self(), tt);

	if (!tt || !tt->end_time) {
		return true;
	}
	uint64_t now = cf_getns();
	bool timedout = (now > tt->end_time(tt));
	if (timedout) {
		cf_warning(AS_UDF, "UDF Timed Out [%lu:%lu]", now / 1000000, tt->end_time(tt) / 1000000);
		return true;
	}
	return false;
}

uint64_t
udf_timer_timeslice(const as_timer * timer)
{
	time_tracker *tt = (time_tracker *)pthread_getspecific(timer_tlskey);
	cf_detail(AS_UDF, "tid=%ld tt=%p", pthread_self(), tt);

	if (!tt || !tt->end_time) {
		return 0;
	}
	uint64_t timeslice = (tt->end_time(tt) - cf_getns()) / 1000000;
	return (timeslice > 0) ? timeslice : 1;
}


const as_timer_hooks udf_timer_hooks = {
	.destroy	= NULL,
	.timedout	= udf_timer_timedout,
	.timeslice	= udf_timer_timeslice
};
