/*
 * cf_str.c
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

/*
 * String helper functions
 *
 */

#include "cf_str.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include <citrusleaf/cf_vector.h>


static char itoa_table[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N' };

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

int cf_str_atoi_64(char *s, int64_t *value)
{
	int64_t i = 0;
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
	*value = neg ? -i : i;
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

int cf_str_atoi_x64(const char *s, uint64_t *value)
{
	if (! ((*s >= '0' && *s <= '9') ||
			(*s >= 'a' && *s <= 'f') ||
			(*s >= 'A' && *s <= 'F'))) {
		return -1;
	}

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


unsigned int
cf_str_itoa(int _value, char *_s, int _radix)
{
	// special case is the easy way
	if (_value == 0) {
		_s[0] = itoa_table[0];
		_s[1] = 0;
		return(1);
	}

	// Account for negatives
	unsigned int sign_len = 0;
	if (_value < 0) {
		*_s++ = '-';
		_value = - _value;
		sign_len = 1;
	}
	int _v = _value;
	unsigned int _nd = 0;
	while (_v) {
		_nd++;
		_v /= _radix;
	}

	unsigned int rv = sign_len + _nd;
	_s[_nd] = 0;
	while (_nd) {
		_nd --;
		_s[_nd ] = itoa_table [ _value % _radix ];
		_value = _value / _radix;
	}
	return(rv);
}

unsigned int
cf_str_itoa_u64(uint64_t _value, char *_s, int _radix)
{
	// special case is the easy way
	if (_value == 0) {
		_s[0] = itoa_table[0];
		_s[1] = 0;
		return(1);
	}

	uint64_t _v = _value;
	unsigned int _nd = 0;
	while (_v) {
		_nd++;
		_v /= _radix;
	}

	unsigned int rv = _nd;
	_s[_nd] = 0;
	while (_nd) {
		_nd --;
		_s[_nd ] = itoa_table [ _value % _radix ];
		_value = _value / _radix;
	}
	return(rv);
}

unsigned int
cf_str_itoa_u32(uint32_t _value, char *_s, int _radix)
{
	// special case is the easy way
	if (_value == 0) {
		_s[0] = itoa_table[0];
		_s[1] = 0;
		return(1);
	}

	uint32_t _v = _value;
	unsigned int _nd = 0;
	while (_v) {
		_nd++;
		_v /= _radix;
	}

	unsigned int rv = _nd;
	_s[_nd] = 0;
	while (_nd) {
		_nd --;
		_s[_nd ] = itoa_table [ _value % _radix ];
		_value = _value / _radix;
	}
	return(rv);
}

#define ATOI_ILLEGAL -1


static int8_t atoi_table[] = {
/*			00   01   02   03   04   05   06   07   08   09   0A   0B   0C   0D   0E   0F */
/* 00 */	-1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
/* 10 */	-1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
/* 20 */	-1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
/* 30 */	 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  -1,  -1,  -1,  -1,  -1,  -1,
/* 40 */	-1,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
/* 50 */	25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  -1,  -1,  -1,  -1,
/* 60 */	-1,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
/* 70 */	25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  -1,  -1,  -1,  -1 };


int
cf_str_atoi_u64_x(char *s, uint64_t *value, int radix)
{
	uint64_t i = 0;
	while (*s) {
		if (*s < 0)	return(-1);
		int8_t cv = atoi_table[(uint8_t)*s];
		if (cv < 0 || cv >= radix) return(-1);
		i *= radix;
		i += cv;
		s++;
	}
	*value = i;
	return(0);
}



void
cf_str_split(char *fmt, char *str, cf_vector *v)
{
	char c;
	char *prev = str;
	while ((c = *str)) {
		for (uint32_t j = 0; fmt[j]; j++) {
			if (fmt[j] == c) {
				*str = 0;
				cf_vector_append(v, &prev);
				prev = str+1;
				break;
			}
		}
		str++;
	}
	if (prev != str)
		cf_vector_append(v, &prev);
}
