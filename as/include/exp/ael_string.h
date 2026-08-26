/*
 * ael_string.h
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

#pragma once

#include <stdbool.h>
#include <stdint.h>

// Escape sequences recognized inside AEL string literals:
//
//   \\   -> 0x5C
//   \n   -> 0x0A
//   \r   -> 0x0D
//   \t   -> 0x09
//   \0   -> 0x00
//   \"   -> 0x22
//   \'   -> 0x27
//   \xHH -> byte HH (two hex digits, upper or lower case)
//
// Spec-defined: \\ \n \t \" \'
// Impl extensions (not yet in literals-and-types.md §3): \r \0 \xHH

// Validate escapes in src[0..sz). On invalid escape, sets *bad_offset_r
// to the offset of the offending backslash within src and *err_r to a
// static diagnostic string. Returns true on success; on success also
// sets *has_escape_r to true iff src contains any backslash escape
// (callers cache this to skip a rescan at codegen time).
bool ael_string_validate(const char* src, uint32_t sz, bool* has_escape_r,
		uint32_t* bad_offset_r, const char** err_r);

// Decoded byte count for a (validated) string. Equals sz when there
// are no escapes — callers use this as the fast-path signal.
uint32_t ael_string_decoded_sz(const char* src, uint32_t sz);

// Decode src[0..sz) into dst. Caller must ensure src was validated
// and that dst has at least ael_string_decoded_sz(src, sz) bytes.
// Returns the number of bytes written.
uint32_t ael_string_decode(const char* src, uint32_t sz, uint8_t* dst);

// Decode a (lexer-validated) base64 literal into dst, writing exactly
// ael_b64_decoded_sz(src, sz) bytes. Returns the number written.
uint32_t ael_b64_decode(const char* src, uint32_t sz, uint8_t* dst);

// Decode a (lexer-validated) hex literal into dst, one byte per digit pair.
// Returns the number written.
uint32_t ael_hex_decode(const char* src, uint32_t sz, uint8_t* dst);

// Hex digit -> nibble [0..15]. Caller must have already validated
// that c is a hex character (the parser regex / blob-odd-hex check
// guarantees this); returns 0 on a stray invalid char.
static inline uint8_t
ael_hex_nibble(char c)
{
	if (c >= '0' && c <= '9') {
		return (uint8_t)(c - '0');
	}
	if (c >= 'a' && c <= 'f') {
		return (uint8_t)(c - 'a' + 10);
	}
	if (c >= 'A' && c <= 'F') {
		return (uint8_t)(c - 'A' + 10);
	}
	return 0;
}

// Decoded byte count for a (lexer-validated) base64 literal: 3 bytes per
// 4-char group, less one per trailing '=' pad. The lexer guarantees
// sz == 0 or sz % 4 == 0 with at most 2 trailing pads, so a non-empty
// literal has sz >= 4 and src[sz - 2] is in range.
static inline uint32_t
ael_b64_decoded_sz(const char* src, uint32_t sz)
{
	if (sz == 0) {
		return 0;
	}

	uint32_t units = (sz / 4) * 3;

	if (src[sz - 1] == '=') {
		units--;
	}
	if (src[sz - 2] == '=') {
		units--;
	}

	return units;
}
