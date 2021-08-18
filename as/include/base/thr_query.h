/*
 * thr_query.h
 *
 * Copyright (C) 2016-2021 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"


//==========================================================
// Forward declarations.
//

struct as_config_s;
struct as_index_ref_s;
struct as_mon_jobstat_s;
struct as_namespace_s;
struct as_sindex_qctx_s;
struct as_transaction_s;
struct cf_dyn_buf_s;
struct cf_ll_element_s;


//==========================================================
// Typedefs & constants.
//

#define AS_QUERY_MAX_THREADS 32
#define AS_QUERY_MAX_WORKER_THREADS 15 * AS_QUERY_MAX_THREADS


//==========================================================
// Public API.
//

void as_index_keys_ll_destroy_fn(struct cf_ll_element_s* ele);
void as_query_release_ref(struct as_namespace_s* ns, struct as_sindex_qctx_s* qctx, struct as_index_ref_s* r_ref);

void as_query_init();
bool as_query(struct as_transaction_s* tr, struct as_namespace_s* ns);
void as_query_reinit(uint32_t new_thread_count);
void as_query_worker_reinit(uint32_t new_thread_count);
int as_query_list(char* name, struct cf_dyn_buf_s* db);
bool as_query_kill(uint64_t trid);
void as_query_gconfig_default(struct as_config_s* c);
struct as_mon_jobstat_s* as_query_get_jobstat(uint64_t trid);
struct as_mon_jobstat_s* as_query_get_jobstat_all(int* size);
bool as_query_set_priority(uint64_t trid, uint32_t priority);
void as_query_histogram_dumpall();

bool as_query_reserve_partition_tree(struct as_namespace_s* ns, struct as_sindex_qctx_s* qctx, uint32_t pid);

extern cf_atomic32 g_query_short_running;
extern cf_atomic32 g_query_long_running;
