/*
 * udf_timer.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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

/*
 * An as_timer for tests.
 */

#pragma once

#include <stdint.h>
#include "aerospike/as_timer.h"

typedef struct   time_tracker_s time_tracker;
typedef uint64_t (* as_timer_end_time_cb)(time_tracker *tt);
typedef uint64_t (* as_timer_timeslice_cb)(time_tracker *tt);

struct time_tracker_s {
	void                  * udata;
	as_timer_end_time_cb    end_time;
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/
void         udf_timer_setup(time_tracker *tt);
void         udf_timer_cleanup();
extern const as_timer_hooks udf_timer_hooks;

