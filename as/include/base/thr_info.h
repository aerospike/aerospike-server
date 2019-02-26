/*
 * thr_info.h
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"

#include "base/proto.h"
#include "base/security.h"
#include "base/transaction.h"

#define MAX_INFO_THREADS 256

typedef int (*as_info_get_tree_fn) (char *name, char *subtree, cf_dyn_buf *db);
typedef int (*as_info_get_value_fn) (char *name, cf_dyn_buf *db);
typedef int (*as_info_command_fn) (char *name, char *parameters, cf_dyn_buf *db);

// Sets a static value - set to 0 to remove a previous value.
extern int as_info_set_buf(const char *name, const uint8_t *value, size_t value_sz, bool def);
extern int as_info_set(const char *name, const char *value, bool def);

// For dynamic items - you will get called when the name is requested. The
// dynbuf will be fully set up for you - just add the information you want to
// return.
extern int as_info_set_dynamic(const char *name, as_info_get_value_fn gv_fn, bool def);

// For tree items - you will get called when the name is requested, and it will
// have the name you registered (name) and the subtree portion (value). The
// dynbuf will be fully set up for you - just add the information you want to
// return
extern int as_info_set_tree(char *name, as_info_get_tree_fn gv_fn);

// For commands - you will be called with the parameters.
extern int as_info_set_command(const char *name, as_info_command_fn command_fn, as_sec_perm required_perm);

int as_info_parameter_get(char *param_str, char *param, char *value, int *value_len);

typedef struct as_info_transaction_s {
	as_file_handle *fd_h;
	as_proto *proto;
	uint64_t start_time;
} as_info_transaction;

// Processes an info request that comes in from the network, sends the response.
extern void as_info(as_info_transaction *it);

// Processes a pure puffer request without any info header stuff.
extern int as_info_buffer(uint8_t *req_buf, size_t req_buf_len, cf_dyn_buf *rsp);

// The info unit uses the fabric to communicate with the other members of the
// cluster so it needs to register for different messages and create listener
// threads, etc.
extern int as_info_init();

// Needed by heartbeat:

char *as_info_bind_to_string(const cf_serv_cfg *cfg, cf_sock_owner owner);

// Needed by ticker:

int as_info_queue_get_size();
void info_log_with_datestamp(void (*log_fn)(void));
void sys_mem_info(uint64_t *free_mem, uint32_t *free_pct);

extern bool g_mstats_enabled;

// Needed by main():
extern uint64_t g_start_sec;
