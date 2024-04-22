/*
 * cf_str.h
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include <stdint.h>

// These functions convert a string to a number of different integer types, and
// returns 0 on success.
int cf_str_atoi(const char *s, int *value);
int cf_str_atoi_u32(const char *s, uint32_t *value);
int cf_str_atoi_u64(const char *s, uint64_t *value);
int cf_str_atoi_seconds(const char *s, uint32_t *value);

int cf_strtoul_x64(const char *s, uint64_t *value);
int cf_strtoul_u32(const char *s, uint32_t *value);
int cf_strtoul_u64(const char *s, uint64_t *value);
int cf_strtoul_u64_raw(const char *s, uint64_t *value);
int cf_strtol_i32(const char *s, int32_t *value);

static inline int
cf_str_strnchr(const uint8_t *s, int sz, int c)
{
	for (int i = 0; i < sz; i++) {
		if (s[i] == c) {
			return i;
		}
	}

	return -1;
}

static inline const char *
cf_str_safe_as_empty(const char *s)
{
	return s ? s : "";
}

static inline const char *
cf_str_safe_as_null(const char *s)
{
	return s ? s : "null";
}
