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


//==========================================================
// Typedefs & constants.
//

typedef enum {
	CF_OS_FILE_RES_OK,
	CF_OS_FILE_RES_NOT_FOUND,
	CF_OS_FILE_RES_ERROR
} cf_os_file_res;

#define CF_OS_OPEN_MODE_USR (S_IRUSR | S_IWUSR)
#define CF_OS_OPEN_MODE_GRP (CF_OS_OPEN_MODE_USR | S_IRGRP | S_IWGRP)


//==========================================================
// Public API - file permissions.
//

void cf_os_use_group_perms(bool use);
bool cf_os_is_using_group_perms(void);

static inline mode_t
cf_os_base_perms(void)
{
	return cf_os_is_using_group_perms() ?
			CF_OS_OPEN_MODE_GRP : CF_OS_OPEN_MODE_USR;
}

static inline mode_t
cf_os_log_perms(void)
{
	return cf_os_base_perms() | S_IRGRP | S_IROTH;
}


//==========================================================
// Public API - read system files.
//

cf_os_file_res cf_os_read_file(const char* path, void* buf, size_t* limit);
cf_os_file_res cf_os_read_int_from_file(const char* path, int64_t* val);


//==========================================================
// Public API - best practices.
//

void cf_os_best_practices_check(cf_dyn_buf* db, uint64_t max_alloc_sz);
