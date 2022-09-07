/*
 * thr_info.h
 *
 * Copyright (C) 2008-2022 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "socket.h"


//==========================================================
// Forward declarations.
//

struct as_file_handle_s;
struct as_proto_s;


//==========================================================
// Typedefs & constants.
//

#define MAX_INFO_THREADS 256

typedef int (*as_info_get_tree_fn)(char* name, char* subtree, cf_dyn_buf* db);
typedef int (*as_info_get_value_fn)(char* name, cf_dyn_buf* db);
typedef int (*as_info_command_fn)(char* name, char* parameters, cf_dyn_buf* db);

typedef struct as_info_transaction_s {
	struct as_file_handle_s* fd_h;
	struct as_proto_s* proto;
	uint64_t start_time;
} as_info_transaction;


//==========================================================
// Globals.
//

extern uint64_t g_start_sec;
extern cf_dyn_buf g_bad_practices;


//==========================================================
// Public API.
//

void as_info_init();
void as_info(as_info_transaction* it);
int as_info_parameter_get(const char* param_str, const char* param, char* value, int* value_len);
int as_info_buffer(uint8_t* req_buf, size_t req_buf_len, cf_dyn_buf* rsp);
void as_info_set_num_info_threads(uint32_t n_threads);

// Needed by heartbeat:
char* as_info_bind_to_string(const cf_serv_cfg* cfg, cf_sock_owner owner);

// Needed by ticker:
uint32_t as_info_queue_get_size();
uint32_t process_cpu(void);
void sys_cpu_info(uint32_t* user_pct, uint32_t* kernel_pct);
void sys_mem_info(uint64_t* free_mem_kbytes, uint32_t* free_mem_pct, uint64_t* thp_mem_kbytes);
