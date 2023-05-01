/*
 * cf_str.c
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

/*
 * String helper functions
 *
 */

#include "cf_str.h"

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>


// return 0 on success, -1 on fail
int cf_str_atoi(char *s, int *value)
{
	int i = 0;
	bool neg = false;

	if (*s == '-') { neg = true; s++; }

	while (*s >= '0' && *s <= '9') {
		i *= 10;
		i += *s - '0';
		s++;
	}
	switch (*s) {
		case 'k':
		case 'K':
			i *= 1024L;
			s++;
			break;
		case 'M':
		case 'm':
			i *= (1024L * 1024L);
			s++;
			break;
		case 'G':
		case 'g':
			i *= (1024L * 1024L * 1024L);
			s++;
			break;
		default:
			break;
	}
	if (*s != 0) {
		return(-1); // reached a non-num before EOL
	}
	*value = neg ? -i : i;
	return(0);
}

// return 0 on success, -1 on fail
int cf_str_atoi_u32(char *s, unsigned int *value)
{
	unsigned int i = 0;

	while (*s >= '0' && *s <= '9') {
		i *= 10;
		i += *s - '0';
		s++;
	}
	switch (*s) {
		case 'k':
		case 'K':
			i *= 1024L;
			s++;
			break;
		case 'M':
		case 'm':
			i *= (1024L * 1024L);
			s++;
			break;
		case 'G':
		case 'g':
			i *= (1024L * 1024L * 1024L);
			s++;
			break;
		default:
			break;
	}
	if (*s != 0) {
		return(-1); // reached a non-num before EOL
	}
	*value = i;
	return(0);
}

int cf_str_atoi_u64(char *s, uint64_t *value)
{
	uint64_t i = 0;

	while (*s >= '0' && *s <= '9') {
		i *= 10;
		i += *s - '0';
		s++;
	}
	switch (*s) {
		case 'k':
		case 'K':
			i *= 1024L;
			s++;
			break;
		case 'M':
		case 'm':
			i *= (1024L * 1024L);
			s++;
			break;
		case 'G':
		case 'g':
			i *= (1024L * 1024L * 1024L);
			s++;
			break;
		case 'T':
		case 't':
			i *= (1024L * 1024L * 1024L * 1024L);
			s++;
			break;
		case 'P':
		case 'p':
			i *= (1024L * 1024L * 1024L * 1024L * 1024L);
			s++;
			break;
		default:
			break;
	}
	if (*s != 0) {
		return(-1); // reached a non-num before EOL
	}
	*value = i;
	return(0);
}

int cf_str_atoi_seconds(char *s, uint32_t *value)
{
	// Special case: treat -1 the same as 0.
	if (*s == '-' && *(s + 1) == '1') {
		*value = 0;
		return 0;
	}

	uint64_t i = 0;

	while (*s >= '0' && *s <= '9') {
		i *= 10;
		i += *s - '0';
		s++;
	}
	switch (*s) {
		case 'S':
		case 's':
			s++;
			break;
		case 'M':
		case 'm':
			i *= 60;
			s++;
			break;
		case 'H':
		case 'h':
			i *= (60 * 60);
			s++;
			break;
		case 'D':
		case 'd':
			i *= (60 * 60 * 24);
			s++;
			break;
		default:
			break;
	}
	if (*s != 0) {
		return(-1); // reached a non-num before EOL
	}
	if (i > UINT32_MAX) {
		return(-1); // overflows a uint32_t
	}
	*value = (uint32_t)i;
	return(0);
}

int
cf_strtoul_x64(const char *s, uint64_t *value)
{
	if (! ((*s >= '0' && *s <= '9') ||
			(*s >= 'a' && *s <= 'f') ||
			(*s >= 'A' && *s <= 'F'))) {
		return -1;
	}

	errno = 0;

	char* tail = NULL;
	uint64_t i = strtoul(s, &tail, 16);

	// Check for overflow.
	if (errno == ERANGE) {
		return -1;
	}

	// Don't allow trailing non-hex characters.
	if (tail && *tail != 0) {
		return -1;
	}

	*value = i;

	return 0;
}

int
cf_strtoul_u32(const char *s, uint32_t *value)
{
	if (! (*s >= '0' && *s <= '9')) {
		return -1;
	}

	errno = 0;

	char* tail = NULL;
	uint64_t i = strtoul(s, &tail, 10);

	// Check for overflow.
	if (errno == ERANGE || i > UINT32_MAX) {
		return -1;
	}

	// Don't allow trailing non-digit characters.
	if (tail && *tail != 0) {
		return -1;
	}

	*value = (uint32_t)i;

	return 0;
}

int
cf_strtoul_u64(const char *s, uint64_t *value)
{
	if (! (*s >= '0' && *s <= '9')) {
		return -1;
	}

	errno = 0;

	char* tail = NULL;
	uint64_t i = strtoul(s, &tail, 10);

	// Check for overflow.
	if (errno == ERANGE) {
		return -1;
	}

	// Don't allow trailing non-digit characters.
	if (tail && *tail != 0) {
		return -1;
	}

	*value = i;

	return 0;
}

// Like cf_strtoul_u64() but doesn't force base 10, and allows sign character.
int
cf_strtoul_u64_raw(const char *s, uint64_t *value)
{
	if (isspace(*s)) {
		return -1;
	}

	errno = 0;

	char* tail = NULL;
	uint64_t i = strtoul(s, &tail, 0);

	// Check for overflow.
	if (errno == ERANGE) {
		return -1;
	}

	// Don't allow trailing non-digit characters.
	if (tail && *tail != 0) {
		return -1;
	}

	*value = i;

	return 0;
}

int
cf_strtol_i32(const char *s, int32_t *value)
{
	if (! ((*s >= '0' && *s <= '9') || *s == '-')) {
		return -1;
	}

	errno = 0;

	char* tail = NULL;
	int64_t i = strtol(s, &tail, 10);

	// Check for overflow.
	if (errno == ERANGE || i < INT32_MIN || i > INT32_MAX) {
		return -1;
	}

	// Don't allow trailing non-digit characters.
	if (tail && *tail != 0) {
		return -1;
	}

	*value = (int32_t)i;

	return 0;
}
