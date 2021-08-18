/*
 * scan.h
 *
 * Copyright (C) 2015-2021 Aerospike, Inc.
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

void as_scan_init(void);
int as_scan(struct as_transaction_s *tr, struct as_namespace_s *ns);
void as_scan_limit_finished_jobs(void);
uint32_t as_scan_get_active_job_count(void);
struct as_mon_jobstat_s* as_scan_get_jobstat(uint64_t trid);
struct as_mon_jobstat_s* as_scan_get_jobstat_all(int* size);
bool as_scan_abort(uint64_t trid);
uint32_t as_scan_abort_all(void);
