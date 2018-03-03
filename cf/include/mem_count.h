/*
 * mem_count.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

#include "dynbuf.h"

/*
 * Type for selecting the field to be sorted on for memory count reporting.
 */
typedef enum sort_field_e {
    CF_ALLOC_SORT_NET_SZ,
    CF_ALLOC_SORT_DELTA_SZ,
    CF_ALLOC_SORT_NET_ALLOC_COUNT,
    CF_ALLOC_SORT_TOTAL_ALLOC_COUNT,
    CF_ALLOC_SORT_TIME_LAST_MODIFIED
} sort_field_t;

/*
 *  Type for mode of enabling / disabling memory accounting.
 */
typedef enum mem_count_mode_e {
	MEM_COUNT_DISABLE,            // Disable memory accounting.
	MEM_COUNT_ENABLE,             // Enable memory accounting at daemon start-up time.
	MEM_COUNT_ENABLE_DYNAMIC      // Enable memory accounting at run-time.
} mem_count_mode_t;

int mem_count_init(mem_count_mode_t mode);
void mem_count_stats(void);
int mem_count_alloc_info(char *file, int line, cf_dyn_buf *db);
int mem_count_report(sort_field_t sort_field, int top_n, cf_dyn_buf *db);
void mem_count_shutdown(void);
