/*
 * ael_string.c
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "exp/ael_string.h"

#include <stdbool.h>
#include <stdint.h>

#include "cf_str.h"
#include "log.h"

//==========================================================
// Local helpers.
//

static inline bool
hex_nibble(char c, uint8_t* out)
{
	if (c >= '0' && c <= '9') {
		*out = (uint8_t)(c - '0');
		return true;
	}

	if (c >= 'a' && c <= 'f') {
		*out = (uint8_t)(c - 'a' + 10);
		return true;
	}

	if (c >= 'A' && c <= 'F') {
		*out = (uint8_t)(c - 'A' + 10);
		return true;
	}

	return false;
}

// Returns true iff c is a recognized single-letter escape (not \xHH).
static inline bool
is_simple_escape(char c)
{
	switch (c) {
	case '\\':
	case 'n':
	case 'r':
	case 't':
	case '0':
	case '"':
	case '\'':
		return true;
	default:
		return false;
	}
}

// Base64 char -> 6-bit sextet. Caller guarantees c is a base64 alphabet
// char (the lexer regex ensures it); '=' padding is handled by the
// caller and never passed here.
static inline uint8_t
b64_sextet(char c)
{
	if (c >= 'A' && c <= 'Z') {
		return (uint8_t)(c - 'A');
	}

	if (c >= 'a' && c <= 'z') {
		return (uint8_t)(c - 'a' + 26);
	}

	if (c >= '0' && c <= '9') {
		return (uint8_t)(c - '0' + 52);
	}

	if (c == '+') {
		return 62;
	}

	return 63; // '/'
}

//==========================================================
// Public API.
//

bool
ael_string_validate(const char* src, uint32_t sz, bool* has_escape_r,
		uint32_t* bad_offset_r, const char** err_r)
{
	bool has_escape = false;

	for (uint32_t i = 0; i < sz; i++) {
		// memchr the gap to the next backslash -- vectorized, and the
		// no-escape common case is a single scan.
		const char* bs = memchr(src + i, '\\', sz - i);

		if (bs == NULL) {
			break;
		}

		i = (uint32_t)(bs - src);
		has_escape = true;

		if (i + 1 >= sz) {
			*bad_offset_r = i;
			*err_r = "string ends with stray backslash";
			return false;
		}

		char esc = src[i + 1];

		if (is_simple_escape(esc)) {
			i++; // skip esc char; loop's i++ skips the backslash
			continue;
		}

		if (esc == 'x') {
			if (i + 3 >= sz) {
				*bad_offset_r = i;
				*err_r = "\\x escape needs two hex digits";
				return false;
			}

			uint8_t hi;
			uint8_t lo;

			if (! hex_nibble(src[i + 2], &hi) || ! hex_nibble(src[i + 3], &lo)) {
				*bad_offset_r = i;
				*err_r = "\\x escape needs two hex digits";
				return false;
			}

			i += 3; // skip x, HH; loop's i++ skips the backslash
			continue;
		}

		*bad_offset_r = i;
		*err_r = "invalid escape sequence";
		return false;
	}

	// Reject invalid UTF-8 in the literal source at parse time; the runtime
	// (as_exp_eval) also checks decoded string data, but this gives a pointed
	// diagnostic instead of an eval-time failure.
	if (! cf_str_is_valid_utf8((const uint8_t*)src, sz)) {
		*bad_offset_r = 0;
		*err_r = "invalid UTF-8 in string";
		return false;
	}

	*has_escape_r = has_escape;
	return true;
}

uint32_t
ael_string_decoded_sz(const char* src, uint32_t sz)
{
	uint32_t out = 0;

	for (uint32_t i = 0; i < sz;) {
		const char* bs = memchr(src + i, '\\', sz - i);

		if (bs == NULL) {
			out += sz - i;
			break;
		}

		uint32_t run = (uint32_t)(bs - src) - i;

		out += run + 1; // literal run + the escape's one decoded byte
		// Validated upstream — bs[1] exists and is a known escape.
		i += run + (bs[1] == 'x' ? 4 : 2);
	}

	return out;
}

uint32_t
ael_string_decode(const char* src, uint32_t sz, uint8_t* dst)
{
	uint32_t out = 0;

	for (uint32_t i = 0; i < sz; i++) {
		if (src[i] != '\\') {
			// memcpy the whole run to the next backslash (or the end).
			const char* bs = memchr(src + i, '\\', sz - i);
			uint32_t run = bs == NULL ? sz - i : (uint32_t)(bs - src) - i;

			memcpy(dst + out, src + i, run);
			out += run;
			i += run;

			if (bs == NULL) {
				break;
			}
		}

		// Validated upstream — src[i + 1] exists and is recognized.
		char esc = src[i + 1];

		switch (esc) {
		case '\\':
			dst[out++] = '\\';
			i++;
			break;
		case 'n':
			dst[out++] = '\n';
			i++;
			break;
		case 'r':
			dst[out++] = '\r';
			i++;
			break;
		case 't':
			dst[out++] = '\t';
			i++;
			break;
		case '0':
			dst[out++] = '\0';
			i++;
			break;
		case '"':
			dst[out++] = '"';
			i++;
			break;
		case '\'':
			dst[out++] = '\'';
			i++;
			break;
		case 'x': {
			uint8_t hi = 0;
			uint8_t lo = 0;

			(void)hex_nibble(src[i + 2], &hi);
			(void)hex_nibble(src[i + 3], &lo);
			dst[out++] = (uint8_t)((hi << 4) | lo);
			i += 3;
			break;
		}
		default:
			cf_crash(AS_EXP, "ael_string_decode - unvalidated escape '\\%c'",
					esc);
		}
	}

	return out;
}

// Decode a (lexer-validated) hex literal into dst, one byte per digit pair.
// Returns the number of bytes written.
uint32_t
ael_hex_decode(const char* src, uint32_t sz, uint8_t* dst)
{
	uint32_t out = sz / 2;

	for (uint32_t i = 0; i < out; i++) {
		dst[i] = (uint8_t)((ael_hex_nibble(src[i * 2]) << 4) |
				ael_hex_nibble(src[i * 2 + 1]));
	}

	return out;
}

// Decode a (lexer-validated) base64 literal into dst, writing exactly
// ael_b64_decoded_sz(src, sz) bytes -- unlike cf_b64_decode, which writes
// 3 bytes per group and would overrun a tight buffer on the padded final
// group. Returns the number of bytes written.
uint32_t
ael_b64_decode(const char* src, uint32_t sz, uint8_t* dst)
{
	uint32_t out = 0;

	for (uint32_t i = 0; i + 4 <= sz; i += 4) {
		bool pad2 = src[i + 2] == '=';
		bool pad3 = src[i + 3] == '=';
		uint8_t s0 = b64_sextet(src[i]);
		uint8_t s1 = b64_sextet(src[i + 1]);
		uint8_t s2 = pad2 ? 0 : b64_sextet(src[i + 2]);
		uint8_t s3 = pad3 ? 0 : b64_sextet(src[i + 3]);

		dst[out++] = (uint8_t)((s0 << 2) | (s1 >> 4));

		if (! pad2) {
			dst[out++] = (uint8_t)((s1 << 4) | (s2 >> 2));
		}

		if (! pad3) {
			dst[out++] = (uint8_t)((s2 << 6) | s3);
		}
	}

	return out;
}
