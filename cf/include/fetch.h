/*
 * fetch.h
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include "citrusleaf/alloc.h"


//==========================================================
// Typedefs & constants.
//

typedef enum {
	PATH_TYPE_ENV,
	PATH_TYPE_ENV_B64,
	PATH_TYPE_FILE,
	PATH_TYPE_SECRETS,
	PATH_TYPE_VAULT
} path_type;


//==========================================================
// Public API.
//

path_type cf_fetch_path_type(const char* path);
uint8_t* cf_fetch_bytes(const char* path, size_t* buf_sz_r);
char* cf_fetch_string(const char* path);

static inline bool
cf_fetch_validate_string(const char* path)
{
	char* s = cf_fetch_string(path);

	if (s == NULL) {
		return false;
	}

	cf_free(s);

	return true;
}
