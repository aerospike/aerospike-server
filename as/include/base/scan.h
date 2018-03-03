/*
 * scan.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdint.h>

#include "dynbuf.h"


//==========================================================
// Forward declarations.
//

struct as_mon_jobstat_s;
struct as_namespace_s;
struct as_transaction_s;


//==========================================================
// Public API.
//

void as_scan_init();
int as_scan(struct as_transaction_s *tr, struct as_namespace_s *ns);
void as_scan_limit_active_jobs(uint32_t max_active);
void as_scan_limit_finished_jobs(uint32_t max_done);
void as_scan_resize_thread_pool(uint32_t n_threads);
int as_scan_get_active_job_count();
int as_scan_list(char* name, cf_dyn_buf* db);
struct as_mon_jobstat_s* as_scan_get_jobstat(uint64_t trid);
struct as_mon_jobstat_s* as_scan_get_jobstat_all(int* size);
int as_scan_abort(uint64_t trid);
int as_scan_abort_all();
int as_scan_change_job_priority(uint64_t trid, uint32_t priority);
