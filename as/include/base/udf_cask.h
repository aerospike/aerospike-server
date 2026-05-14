/*
 * udf_cask.h
 *
 * Copyright (C) 2013-2021 Aerospike, Inc.
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
#include <string.h>

#include "cfg_info.h"
#include "dynbuf.h"

//==========================================================
// Public API.
//

// Startup.
void udf_cask_init(void);

// Info commands.
void udf_cask_info_clear_cache(struct as_info_cmd_args_s* args);
void udf_cask_info_get(struct as_info_cmd_args_s* args);
void udf_cask_info_list(struct as_info_cmd_args_s* args);
void udf_cask_info_put(struct as_info_cmd_args_s* args);
void udf_cask_info_remove(struct as_info_cmd_args_s* args);

// Returns true iff filename is a single basename safe to combine with the
// UDF user_path. Accepts only the byte set [A-Za-z0-9._-$]; additionally
// rejects empty names, names beginning with '.', and the substring "..".
// Sufficient to prevent path traversal at every caller in udf_cask.c, and
// tight enough that the accepted name set contains no info-protocol
// delimiters or control characters that could poison logs or responses
// downstream. Defined here rather than in udf_cask.c so unit tests can
// link against it without exposing the symbol through a separate header.
static inline bool
udf_filename_is_valid(const char* filename)
{
	if (filename[0] == '\0' || filename[0] == '.') {
		return false;
	}

	for (const char* p = filename; *p != '\0'; p++) {
		char c = *p;

		// Enforces the [A-Za-z0-9._-$] byte set.
		if (! ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '.' || c == '_' ||
				c == '-' || c == '$')) {
			return false;
		}
	}

	return strstr(filename, "..") == NULL;
}
