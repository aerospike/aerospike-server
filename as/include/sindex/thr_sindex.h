/*
 * thr_sindex.h
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
 * secondary index function declarations
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "ai_obj.h"
#include "dynbuf.h"
#include "hist.h"

#include "base/datamodel.h"
#include "base/monitor.h"

#define SINDEX_GC_QUEUE_HIGHWATER  10
#define SINDEX_GC_NUM_OBJS_PER_ARR 20

typedef struct acol_digest_t {
	cf_digest dig;
	ai_obj    acol;
} acol_digest;

typedef struct objs_to_defrag_arr_t {
	acol_digest acol_digs[SINDEX_GC_NUM_OBJS_PER_ARR];
	uint32_t    num;
} objs_to_defrag_arr;

typedef struct ll_sindex_gc_element_s {
	cf_ll_element        ele;
	objs_to_defrag_arr * objs_to_defrag;
} ll_sindex_gc_element;

extern pthread_rwlock_t sindex_rwlock;
extern cf_queue *g_sindex_populate_q;
extern cf_queue *g_sindex_destroy_q;
extern cf_queue *g_sindex_populateall_done_q;
extern bool      g_sindex_boot_done;

void as_sindex_thr_init();
objs_to_defrag_arr * as_sindex_gc_get_defrag_arr(void);

#define MAX_SINDEX_BUILDER_THREADS 32

void as_sbld_init();
void as_sbld_build_all(as_namespace* ns);
void as_sbld_resize_thread_pool(uint32_t n_threads);
int as_sbld_list(char* name, cf_dyn_buf* db);
as_mon_jobstat* as_sbld_get_jobstat(uint64_t trid);
as_mon_jobstat* as_sbld_get_jobstat_all(int* size);
int as_sbld_abort(uint64_t trid);
