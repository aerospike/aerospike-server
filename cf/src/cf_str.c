/*
 * cf_str.c
 *
 * Copyright (C) 2008-2026 Aerospike, Inc.
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
#include <string.h>

#include "cf_utf8_vec.h"

// return 0 on success, -1 on fail
int
cf_str_atoi(const char* s, int* value)
{
	int i = 0;
	bool neg = false;

	if (*s == '-') {
		neg = true;
		s++;
	}

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
		return (-1); // reached a non-num before EOL
	}
	*value = neg ? -i : i;
	return (0);
}

// return 0 on success, -1 on fail
int
cf_str_atoi_u32(const char* s, unsigned int* value)
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
		return (-1); // reached a non-num before EOL
	}
	*value = i;
	return (0);
}

// cf_str_atoi_size() supports both SI and IEC suffixes.
// SI are powers of 10, IEC powers of 2.
int
cf_str_atoi_size(const char* s, uint64_t* value)
{
	size_t length = strlen(s);
	// If the string ends with 'i', use IEC suffixes.
	// IEC suffixes have the form "XKi", "XMi", "XGi", "XTi", "XPi"
	// so they should be at least 3 characters long.
	if (length > 2 && (s[length - 1] == 'i' || s[length - 1] == 'I')) {
		return cf_str_atoi_iec(s, value);
	}
	// Otherwise, use SI suffixes.
	return cf_str_atoi_si(s, value);
}

int
cf_str_atoi_u64(const char* s, uint64_t* value)
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
		return (-1); // reached a non-num before EOL
	}
	*value = i;
	return (0);
}

int
cf_str_atoi_iec(const char* s, uint64_t* value)
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
	// Tolerate a missing 'i' suffix so that parsing plain numbers is supported.
	if (*s == 'i' || *s == 'I') {
		s++; // skip the 'i'
	}

	if (*s != 0) {
		return (-1); // reached a non-num before EOL
	}

	*value = i;
	return (0);
}

int
cf_str_atoi_si(const char* s, uint64_t* value)
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
		i *= 1000L;
		s++;
		break;
	case 'M':
	case 'm':
		i *= (1000L * 1000L);
		s++;
		break;
	case 'G':
	case 'g':
		i *= (1000L * 1000L * 1000L);
		s++;
		break;
	case 'T':
	case 't':
		i *= (1000L * 1000L * 1000L * 1000L);
		s++;
		break;
	case 'P':
	case 'p':
		i *= (1000L * 1000L * 1000L * 1000L * 1000L);
		s++;
		break;
	default:
		break;
	}
	if (*s != 0) {
		return (-1); // reached a non-num before EOL
	}
	*value = i;
	return (0);
}

int
cf_str_atoi_seconds(const char* s, uint32_t* value)
{
	// Special case: accept -1.
	if (*s == '-' && *(s + 1) == '1' && *(s + 2) == 0) {
		*value = (uint32_t)-1;
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
		return (-1); // reached a non-num before EOL
	}
	if (i > UINT32_MAX) {
		return (-1); // overflows a uint32_t
	}
	*value = (uint32_t)i;
	return (0);
}

int
cf_strtoul_x64(const char* s, uint64_t* value)
{
	if (! ((*s >= '0' && *s <= '9') || (*s >= 'a' && *s <= 'f') ||
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
cf_strtoul_u32(const char* s, uint32_t* value)
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
cf_strtoul_u64(const char* s, uint64_t* value)
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
cf_strtoul_u64_raw(const char* s, uint64_t* value)
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
cf_strtol_i32(const char* s, int32_t* value)
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

// DSB-stable aligned ASCII scan. Returns index of first non-ASCII 8-byte word
// or n64 * 8 if all ASCII.
__attribute__((noinline, hot)) static size_t
utf8_fast_bulk(const uint64_t* p64, size_t n64)
{
	if (n64 == 0) {
		return 0;
	}

	const uint64_t mask = 0x8080808080808080ULL;

#if defined(__x86_64__) || defined(__i386__)
	__asm__ volatile(".p2align 5" ::: "memory");
#endif

	for (size_t i = 0; i < n64; i++) {
		const uint64_t t = p64[i] & mask;

		if (t != 0) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
			unsigned j = (unsigned)__builtin_clzll(t) / 8u;
#else
			unsigned j = ((unsigned)__builtin_ctzll(t) - 7u) / 8u;
#endif
			return i * 8 + (size_t)j;
		}
	}

	return n64 * 8;
}

// Scan buf for the first non-ASCII byte. Returns buf_sz if all ASCII.
static inline size_t
utf8_fast_scalar(const uint8_t* buf, size_t buf_sz)
{
	size_t off = 0;

	uintptr_t addr = (uintptr_t)buf;
	size_t head = (-addr) & 7;

	if (head > buf_sz) {
		head = buf_sz;
	}

	for (size_t i = 0; i < head; i++) {
		if ((buf[i] & 0x80) != 0) {
			return i;
		}
	}

	off += head;

	size_t rem = buf_sz - off;
	size_t n64 = rem / 8;
	const uint64_t* p64 = (const uint64_t*)(buf + off);
	size_t bulk = utf8_fast_bulk(p64, n64);

	if (bulk < n64 * 8) {
		return off + bulk;
	}

	off += n64 * 8;

	size_t tail = buf_sz - off;

	for (size_t i = 0; i < tail; i++) {
		if ((buf[off + i] & 0x80) != 0) {
			return off + i;
		}
	}

	return buf_sz;
}

// Strict scalar UTF-8 validator (multibyte + overlong + surrogates).
static inline bool
utf8_slow_scalar(const uint8_t* buf, size_t buf_sz)
{
	static const uint32_t cp_min[] = { 0, 0x80, 0x800, 0x10000 };

	for (size_t i = 0; i < buf_sz; i++) {
		uint8_t b = buf[i];

		if (b <= 0x7F) {
			continue;
		}

		uint32_t cp;
		uint32_t n;

		if ((b & 0xE0) == 0xC0) {
			cp = b & 0x1F;
			n = 1;
		}
		else if ((b & 0xF0) == 0xE0) {
			cp = b & 0x0F;
			n = 2;
		}
		else if ((b & 0xF8) == 0xF0) {
			cp = b & 0x07;
			n = 3;
		}
		else {
			return false;
		}

		if (i + n >= buf_sz) {
			return false;
		}

		for (uint32_t j = 0; j < n; j++) {
			uint8_t c = buf[++i];

			if ((c & 0xC0) != 0x80) {
				return false;
			}

			cp = (cp << 6) | (c & 0x3F);
		}

		if (cp < cp_min[n] || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
			return false;
		}
	}

	return true;
}

bool
cf_str_is_valid_utf8(const uint8_t* buf, size_t buf_sz)
{
	if (buf == NULL) {
		return buf_sz == 0;
	}

	if (buf_sz < 64) {
		const size_t k = utf8_fast_scalar(buf, buf_sz);

		return k == buf_sz || utf8_slow_scalar(buf + k, buf_sz - k);
	}

#if defined(__x86_64__) || defined(__i386__)
	if (! __builtin_cpu_supports("sse4.1")) {
		const size_t k = utf8_fast_scalar(buf, buf_sz);

		return k == buf_sz || utf8_slow_scalar(buf + k, buf_sz - k);
	}
#endif

	return cf_utf8_validate_128(buf, buf_sz);
}
