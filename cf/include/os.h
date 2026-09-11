/*
 * os.h
 *
 * Copyright (C) 2021 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "dynbuf.h"
#include "log.h"

//==========================================================
// Typedefs & constants.
//

typedef enum {
	CF_OS_FILE_RES_OK,
	CF_OS_FILE_RES_NOT_FOUND,
	CF_OS_FILE_RES_ERROR
} cf_os_file_res;

typedef cf_os_file_res (*cf_os_test_read_file_fn)(const char* path, void* buf,
		size_t* limit);

// 'usable' covers used_bytes - it is set only when the kernel's raw used is
// within the limit, and gates the published cgroup_memory_* statistics.
//
// 'limit_governs' says this call derived its cgroup free-memory numbers from
// limit_bytes - the same denominator the stop-writes trigger will use. It is
// NOT "am I in a limited cgroup": a real limit can be resolved into limit_bytes
// and this still be false, because cgroup accounting failed (adjusted usage
// over the limit). Consumers wanting the budget to agree with what stop-writes
// enforces want this flag; it falls back to host MemTotal on exactly the paths
// where the trigger does too.
//
// The two flags differ: the inactive-file adjustment can bring usage back under
// the limit, so a cgroup can supply the numbers while 'usable' is false.
//
// 'limit_bytes' is set whenever a real, finite cgroup limit was resolved -
// which either flag being true implies, but neither flag reports on its own.
// With both flags false it holds either nothing or a real limit, and the
// caller cannot tell which: read it only behind one of them.
typedef struct cf_os_cgroup_mem_stats_s {
	bool usable;
	bool limit_governs;
	uint64_t used_bytes;
	uint64_t limit_bytes;
} cf_os_cgroup_mem_stats;

#define CF_OS_OPEN_MODE_USR (S_IRUSR | S_IWUSR)
#define CF_OS_OPEN_MODE_GRP (CF_OS_OPEN_MODE_USR | S_IRGRP | S_IWGRP)

//==========================================================
// Inlines & macros.
//

#define os_check_failed(_db, _name, _msg, ...)                                 \
	do {                                                                       \
		cf_warning(CF_OS, "failed %s check - " _msg, _name, ##__VA_ARGS__);    \
		cf_dyn_buf_append_string(_db, _name);                                  \
		cf_dyn_buf_append_char(_db, ',');                                      \
	} while (false)

//==========================================================
// Public API - file permissions.
//

void cf_os_use_group_perms(bool use);
bool cf_os_is_using_group_perms(void);

static inline mode_t
cf_os_base_perms(void)
{
	return cf_os_is_using_group_perms() ? CF_OS_OPEN_MODE_GRP
										: CF_OS_OPEN_MODE_USR;
}

static inline mode_t
cf_os_log_perms(void)
{
	return cf_os_base_perms() | S_IRGRP | S_IROTH;
}

//==========================================================
// Public API - get system memory info.

void get_mem_info(bool cgroup_mode, uint64_t* free_mem_kbytes,
		uint32_t* free_mem_pct, uint64_t* host_free_mem_kbytes,
		uint32_t* host_free_mem_pct, uint64_t* thp_mem_kbytes,
		uint64_t* mem_limit_kbytes);

void get_mem_info_with_cgroup_stats(bool cgroup_mode, uint64_t* free_mem_kbytes,
		uint32_t* free_mem_pct, uint64_t* host_free_mem_kbytes,
		uint32_t* host_free_mem_pct, uint64_t* host_total_mem_kbytes,
		uint64_t* thp_mem_kbytes, cf_os_cgroup_mem_stats* cg_stats);

uint64_t cf_os_process_rss_bytes(void);

void cf_os_set_mem_read_file_fn_for_test(cf_os_test_read_file_fn fn);

//==========================================================
// Public API - read system files.
//

cf_os_file_res cf_os_read_file(const char* path, void* buf, size_t* limit);
cf_os_file_res cf_os_read_int_from_file(const char* path, int64_t* val);

//==========================================================
// Public API - best practices.
//

void cf_os_best_practices_checks(cf_dyn_buf* db, uint64_t max_alloc_sz);
void cf_os_best_practices_check(const char* name, const char* path, int64_t min,
		int64_t max, cf_dyn_buf* db);

//==========================================================
// Private API - for enterprise separation only.
//

void cf_os_best_practices_checks_ee(cf_dyn_buf* db);
