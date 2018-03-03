/*
 * thr_tsvc.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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


//==========================================================
// Forward declarations.
//

struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

#define MAX_TRANSACTION_QUEUES 128
#define MAX_TRANSACTION_THREADS_PER_QUEUE 256


//==========================================================
// Public API.
//

void as_tsvc_init();
void as_tsvc_enqueue(struct as_transaction_s *tr);
void as_tsvc_set_threads_per_queue(uint32_t n_threads);
int as_tsvc_queue_get_size();
void as_tsvc_process_transaction(struct as_transaction_s *tr);
