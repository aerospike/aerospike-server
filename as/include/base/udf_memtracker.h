/*
 * udf_memtracker.h
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

/**
 * An as_memtracker for tests.
 */

#pragma once

#include <stdint.h>
#include "aerospike/as_memtracker.h"

typedef enum {
	MEM_RESERVE	= 0,
	MEM_RELEASE	= 1,
	MEM_RESET	= 2
} memtracker_op;

typedef struct mem_tracker_s mem_tracker;
typedef bool (*as_memtracker_op_cb)(mem_tracker *mt, uint32_t, memtracker_op);

struct mem_tracker_s {
	void					*udata;
	as_memtracker_op_cb		cb;
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/
as_memtracker * udf_memtracker_init();
void udf_memtracker_setup(mem_tracker *mt);
void udf_memtracker_cleanup();
