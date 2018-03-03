/*
 * json_init.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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

#include "jansson.h"
#include "citrusleaf/alloc.h"
#include "base/json_init.h"

/* SYNOPSIS
 *  This module handles initialization of the Jansson JSON API by
 *  setting the memory allocation functions to be used internally
 *  by Jansson to the CF allocation-related functions.
 */

/*
 *  Note that actual wrapper functions are needed instead of simply
 *  using the names of the CF malloc() and free() functions, since the
 *  memory allocation instrumentation infrastructure uses macroexpansion
 *  of the CF allocation-related function names to track all allocations.
 */

/*
 *  Wrapper function to call the CF malloc() function.
 */
static void *as_json_malloc(size_t size)
{
	return cf_malloc(size);
}

/*
 *  Wrapper function to call the CF free() function.
 */
static void as_json_free(void *ptr)
{
	cf_free(ptr);
}

/*
 *  Initialize the JSON module by setting the memory allocation functions.
 */
void as_json_init()
{
	json_set_alloc_funcs(as_json_malloc, as_json_free);
}
