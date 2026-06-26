/*
 * particle_string.c
 *
 * Copyright (C) 2015-2026 Aerospike, Inc.
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

//clang-format off
#include <errno.h> // for strtoX() validation
#include <math.h> // for isinf(), isnan()
#include <stdint.h> // for uint#_t
#include <stdlib.h> // for labs(), strtoll()
#include <string.h> // for memcpy()
#include <unicode/ucasemap.h> // ucasemap_utf8ToUpper/ToLower/FoldCase (direct UTF-8)
#include <unicode/uchar.h> // u_strToUpper, u_strToLower
#include <unicode/ucol.h> // UCollator for locale-aware comparison settings
#include <unicode/unorm2.h> // NFC normalization (unorm2_normalize)
#include <unicode/uregex.h> // ICU regex (uregex_open, uregex_find, uregex_replaceAll)
#include <unicode/usearch.h> // String search with canonical equivalence (NFC↔NFD)
#include <unicode/ustring.h> // UTF-8 ↔ UTF-16 conversion (u_strFromUTF8)
#include <unicode/utext.h> // UText for direct UTF-8 regex (utext_openUTF8)
#include <unicode/utf8.h> // UTF-8 iteration macros (U8_NEXT, U8_APPEND)
#include <unicode/utypes.h> // U_INVALID_CHAR_FOUND, UErrorCode
#define PCRE2_CODE_UNIT_WIDTH 8 // must be defined before including pcre2.h
#include <pcre2.h> // for ascii / nfc regex fast paths
//clang-format on

#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"

#include "cf_str.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"
#include "base/thr_info.h"

#define STRING_OP_STACK_BUF_SZ 65536
#define STRING_REPLACE_ALL_MAX (8 * 1024 * 1024)
// more than (2**20 * 8) matches not possible with 8MB max record size

// Regex resource limits (user-supplied patterns on request paths).
#define STRING_REGEX_ICU_STEP_LIMIT 5000000
#define STRING_REGEX_ICU_STACK_BYTES (8 * 1024 * 1024)
#define STRING_PCRE2_MATCH_LIMIT 5000000u
#define STRING_PCRE2_DEPTH_LIMIT 10000000u

//==========================================================
// STRING particle interface - function declarations.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle "wire" format.
int string_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp);
static int string_append_from_wire(as_particle_type wire_type,
		const uint8_t* wire_value, uint32_t value_size, as_particle** pp);
static int string_prepend_from_wire(as_particle_type wire_type,
		const uint8_t* wire_value, uint32_t value_size, as_particle** pp);

// Handle as_val translation.
uint32_t string_size_from_asval(const as_val* val);
void string_from_asval(const as_val* val, as_particle** pp);
as_val* string_to_asval(const as_particle* p);
uint32_t string_asval_wire_size(const as_val* val);
uint32_t string_asval_to_wire(const as_val* val, uint8_t* wire);

//==========================================================
// STRING particle interface - vtable.
//

// clang-format off
const as_particle_vtable string_vtable = {
		blob_destruct,
		blob_size,

		blob_concat_size_from_wire,
		string_append_from_wire,
		string_prepend_from_wire,
		blob_incr_from_wire,
		blob_size_from_wire,
		string_from_wire,
		blob_wire_size,
		blob_to_wire,

		string_size_from_asval,
		string_from_asval,
		string_to_asval,
		string_asval_wire_size,
		string_asval_to_wire,

		blob_size_from_msgpack,
		blob_from_msgpack,

		blob_skip_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};
// clang-format on

//==========================================================
// Typedefs & constants.
//

// Same as related BLOB struct. TODO: just expose BLOB structs?

typedef struct string_mem_s {
	uint8_t type;
	uint32_t sz;
	uint8_t data[];
} __attribute__((__packed__)) string_mem;

//==========================================================
// STRING particle interface - function definitions.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

int
string_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	if (! cf_str_is_valid_utf8(wire_value, value_size)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_from_wire - invalid UTF-8 detected in string data; "
				"string APIs will fail on this bin");
	}

	cf_assert(wire_type == AS_PARTICLE_TYPE_STRING, AS_PARTICLE,
			"string_from_wire: wire_type must be STRING");

	return blob_string_particle_from_wire(wire_value, value_size, pp);
}

static int
string_append_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	as_info_warn_deprecated(
			"AS_MSG_OP_APPEND on string bin is deprecated - use string.append or string.concat");

	return blob_append_from_wire(wire_type, wire_value, value_size, pp);
}

static int
string_prepend_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	as_info_warn_deprecated(
			"AS_MSG_OP_PREPEND on string bin is deprecated - use string.prepend or string.insert at index 0");

	return blob_prepend_from_wire(wire_type, wire_value, value_size, pp);
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
string_size_from_asval(const as_val* val)
{
	return (uint32_t)(sizeof(string_mem) + as_string_len(as_string_fromval(val)));
}

void
string_from_asval(const as_val* val, as_particle** pp)
{
	string_mem* p_string_mem = (string_mem*)*pp;

	as_string* string = as_string_fromval(val);

	p_string_mem->type = AS_PARTICLE_TYPE_STRING;
	p_string_mem->sz = (uint32_t)as_string_len(string);
	memcpy(p_string_mem->data, as_string_tostring(string), p_string_mem->sz);
}

as_val*
string_to_asval(const as_particle* p)
{
	string_mem* p_string_mem = (string_mem*)p;

	uint8_t* value = cf_malloc(p_string_mem->sz + 1);

	memcpy(value, p_string_mem->data, p_string_mem->sz);
	value[p_string_mem->sz] = 0;

	return (as_val*)as_string_new_wlen((char*)value, p_string_mem->sz, true);
}

uint32_t
string_asval_wire_size(const as_val* val)
{
	return as_string_len(as_string_fromval(val));
}

uint32_t
string_asval_to_wire(const as_val* val, uint8_t* wire)
{
	as_string* string = as_string_fromval(val);
	uint32_t size = (uint32_t)as_string_len(string);

	memcpy(wire, as_string_tostring(string), size);

	return size;
}

//==========================================================
// as_bin particle functions specific to STRING.
//

uint32_t
as_bin_particle_string_ptr(const as_bin* b, char** p_value)
{
	// Caller must ensure this is called only for STRING particles.
	string_mem* p_string_mem = (string_mem*)b->particle;

	*p_value = (char*)p_string_mem->data;

	return p_string_mem->sz;
}

// WIP for SERVER-97

typedef struct string_op_s {
	int64_t int_arg1;
	int64_t int_arg2;
	const uint8_t* buf;
	uint32_t buf_sz;
	uint64_t flags;
} string_op;

struct string_state_s;

typedef bool (*string_parse_fn)(struct string_state_s* state, string_op* op);
typedef int (*string_modify_fn)(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
typedef int (*string_read_fn)(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
typedef int (*string_prepare_fn)(struct string_state_s* state, string_op* op);

// Operation descriptor selected from the op table.
// Each entry defines how to:
// 1) parse msgpack args into string_op,
// 2) validate/prepare against current bin state,
// 3) execute either modify or read implementation.
typedef struct string_op_def_s {
	const string_prepare_fn prepare;
	union {
		const string_modify_fn modify;
		const string_read_fn read;
	} fn;
	uint64_t bad_flags;
	uint32_t min_args;
	uint32_t max_args;
	const string_parse_fn* args;
	const char* name;
} string_op_def;

// Per-request execution context for one string operation.
// Carries decoded request metadata and size bookkeeping from parse/prepare
// into execute, avoiding repeated scans/conversions.
//
// old_cp_len is lazily computed UTF-8 code-point length of old value;
// has_cp_len indicates whether old_cp_len is valid.
typedef struct string_state_s {
	const uint8_t* bin_name;
	msgpack_in_vec* mv;
	string_op_def* def;

	as_string_op_type op_type;
	uint32_t n_args;
	uint32_t old_size;
	uint32_t new_size;
	uint32_t old_cp_len;
	uint8_t bin_name_sz : 4; // max 15
	uint8_t has_cp_len : 1;
	uint8_t has_ctx : 1; // true if op uses ctx path (sentinel was 0xFF)
	uint8_t is_expr : 1; // true if invoked from expression context
	uint8_t : 1; // unused

	// Context path state (valid when has_ctx is true).
	// ctx_mv_idx and ctx_mv_offset point to the start of the ctx list in mv.
	uint32_t ctx_mv_idx;
	uint32_t ctx_mv_offset;
} string_state;

//==========================================================
// Forward declarations.
//

// Parse functions
static bool string_parse_int1(string_state* state, string_op* op);
static bool string_parse_int2(string_state* state, string_op* op);
static bool string_parse_buf(string_state* state, string_op* op);
static bool string_parse_list(string_state* state, string_op* op);
static bool string_parse_flags(string_state* state, string_op* op);

// Prepare functions
static int string_prepare_read_op(string_state* state, string_op* op);
static int string_prepare_modify_op(string_state* state, string_op* op);

static int64_t utf8_string_length(const uint8_t* from, uint32_t sz);

// Modify ops
static int string_modify_op_insert(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_upper(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_lower(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_snip(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_trim_both(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_trim_start(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_trim_end(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_pad_start(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_pad_end(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_normalize_nfc(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_case_fold(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_replace(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_replace_all(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_overwrite(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_concatenate(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_append(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_prepend(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);
static int string_modify_op_repeat(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);

// Read ops
static int string_read_op_strlen(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_substr(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_find(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_contains(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_char_at(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_starts_with(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_ends_with(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_to_integer(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_to_double(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_byte_length(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_is_numeric(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_is_upper(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_is_lower(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_to_blob(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_split(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_b64_decode(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb);
static int string_read_op_regex_compare(const string_op* op,
		const uint8_t* from, uint32_t sz, as_bin* rb);
static int string_modify_op_regex_replace(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz);

static bool
string_op_allows_bin_create(as_string_op_type op_type)
{
	switch (op_type) {
	case AS_STRING_OP_INSERT:
	case AS_STRING_OP_CONCAT:
	case AS_STRING_OP_APPEND:
	case AS_STRING_OP_PREPEND:
		return true;
	default:
		return false;
	}
}

//==========================================================
// Macros.
//

#define STRING_MODIFY_OP_ENTRY(_op, _name, _op_fn, _flags, _min_args,          \
		_max_args, ...)                                                        \
	[(_op) - AS_STRING_MODIFY_OP_START] = { .name = _name,                     \
		.prepare = string_prepare_modify_op,                                   \
		.fn.modify = _op_fn,                                                   \
		.bad_flags = ~((uint64_t)(_flags)),                                    \
		.min_args = _min_args,                                                 \
		.max_args = _max_args,                                                 \
		.args = (string_parse_fn[]){ __VA_ARGS__ } }

#define STRING_READ_OP_ENTRY(_op, _name, _op_fn, _min_args, _max_args, ...)             \
	[_op].name = _name, [_op].prepare = string_prepare_read_op, [_op].fn.read = _op_fn, \
	[_op].bad_flags = ~((uint64_t)(0)), [_op].min_args = _min_args,                     \
	[_op].max_args = _max_args, [_op].args = (string_parse_fn[])                        \
	{                                                                                   \
		__VA_ARGS__                                                                     \
	}

#define STRING_FLAGS_CREATE_CAPABLE                                            \
	(AS_STRING_FLAG_CREATE_ONLY | AS_STRING_FLAG_UPDATE_ONLY |                 \
			AS_STRING_FLAG_NO_FAIL)

#define STRING_FLAGS_UPDATE_ONLY                                               \
	(AS_STRING_FLAG_UPDATE_ONLY | AS_STRING_FLAG_NO_FAIL)

//==========================================================
// Op tables.
//

static const string_op_def string_modify_op_table[] = {
	// Content ops
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_INSERT, "string_insert",
			string_modify_op_insert, (STRING_FLAGS_CREATE_CAPABLE), 2, 3,
			string_parse_int1, string_parse_buf, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_OVERWRITE, "string_overwrite",
			string_modify_op_overwrite, (STRING_FLAGS_UPDATE_ONLY), 2, 3,
			string_parse_int1, string_parse_buf, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_CONCAT, "string_concatenate",
			string_modify_op_concatenate, (STRING_FLAGS_CREATE_CAPABLE), 1, 2,
			string_parse_list, string_parse_flags),
	// Client must send end before flags -- (start, flags) will parse flags as end.
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_SNIP, "string_snip",
			string_modify_op_snip, (STRING_FLAGS_UPDATE_ONLY), 1, 3,
			string_parse_int1, string_parse_int2, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_REPLACE, "string_replace",
			string_modify_op_replace, (STRING_FLAGS_UPDATE_ONLY), 1, 2,
			string_parse_list, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_REPLACE_ALL, "string_replace_all",
			string_modify_op_replace_all, (STRING_FLAGS_UPDATE_ONLY), 1, 2,
			string_parse_list, string_parse_flags),
	// Case & normalization ops
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_UPPER, "string_upper",
			string_modify_op_upper, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_LOWER, "string_lower",
			string_modify_op_lower, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_CASE_FOLD, "string_case_fold",
			string_modify_op_case_fold, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_NORMALIZE_NFC, "string_normalize_nfc",
			string_modify_op_normalize_nfc, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	// Trim & pad ops
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_TRIM_START, "string_trim_start",
			string_modify_op_trim_start, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_TRIM_END, "string_trim_end",
			string_modify_op_trim_end, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_TRIM, "string_trim",
			string_modify_op_trim_both, (STRING_FLAGS_UPDATE_ONLY), 0, 1,
			string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_PAD_START, "string_pad_start",
			string_modify_op_pad_start, (STRING_FLAGS_UPDATE_ONLY), 2, 3,
			string_parse_int1, string_parse_buf, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_PAD_END, "string_pad_end",
			string_modify_op_pad_end, (STRING_FLAGS_UPDATE_ONLY), 2, 3,
			string_parse_int1, string_parse_buf, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_REPEAT, "string_repeat",
			string_modify_op_repeat, (STRING_FLAGS_UPDATE_ONLY), 1, 2,
			string_parse_int1, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_REGEX_REPLACE, "string_regex_replace",
			string_modify_op_regex_replace, (STRING_FLAGS_UPDATE_ONLY), 1, 2,
			string_parse_list, string_parse_int1),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_APPEND, "string_append",
			string_modify_op_append, (STRING_FLAGS_CREATE_CAPABLE), 1, 2,
			string_parse_buf, string_parse_flags),
	STRING_MODIFY_OP_ENTRY(AS_STRING_OP_PREPEND, "string_prepend",
			string_modify_op_prepend, (STRING_FLAGS_CREATE_CAPABLE), 1, 2,
			string_parse_buf, string_parse_flags),
};

static const string_op_def string_read_op_table[] = {
	STRING_READ_OP_ENTRY(AS_STRING_OP_STRLEN, "string_strlen",
			string_read_op_strlen, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_SUBSTR, "string_substr",
			string_read_op_substr, 1, 2, string_parse_int1, string_parse_int2),
	STRING_READ_OP_ENTRY(AS_STRING_OP_FIND, "string_find", string_read_op_find,
			1, 2, string_parse_buf, string_parse_int1),
	STRING_READ_OP_ENTRY(AS_STRING_OP_CONTAINS, "string_contains",
			string_read_op_contains, 1, 1, string_parse_buf),
	STRING_READ_OP_ENTRY(AS_STRING_OP_CHAR_AT, "string_char_at",
			string_read_op_char_at, 1, 1, string_parse_int1),
	STRING_READ_OP_ENTRY(AS_STRING_OP_STARTS_WITH, "string_starts_with",
			string_read_op_starts_with, 1, 1, string_parse_buf),
	STRING_READ_OP_ENTRY(AS_STRING_OP_ENDS_WITH, "string_ends_with",
			string_read_op_ends_with, 1, 1, string_parse_buf),
	STRING_READ_OP_ENTRY(AS_STRING_OP_TO_INTEGER, "string_to_integer",
			string_read_op_to_integer, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_TO_DOUBLE, "string_to_double",
			string_read_op_to_double, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_BYTE_LENGTH, "string_byte_length",
			string_read_op_byte_length, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_IS_NUMERIC, "string_is_numeric",
			string_read_op_is_numeric, 0, 1, string_parse_int1),
	STRING_READ_OP_ENTRY(AS_STRING_OP_IS_UPPER, "string_is_upper",
			string_read_op_is_upper, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_IS_LOWER, "string_is_lower",
			string_read_op_is_lower, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_TO_BLOB, "string_to_blob",
			string_read_op_to_blob, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_SPLIT, "string_split",
			string_read_op_split, 0, 1, string_parse_buf),
	STRING_READ_OP_ENTRY(AS_STRING_OP_B64_DECODE, "string_b64_decode",
			string_read_op_b64_decode, 0, 0),
	STRING_READ_OP_ENTRY(AS_STRING_OP_REGEX_COMPARE, "string_regex_compare",
			string_read_op_regex_compare, 1, 2, string_parse_buf,
			string_parse_int1),
};

//==========================================================
// More forward declarations.
//

static bool string_state_init(string_state* state, const uint8_t* bin_name,
		uint8_t bin_name_sz, msgpack_in_vec* mv, bool is_read, bool is_expr);
static int string_modify(string_state* state, as_bin* b,
		cf_ll_buf* particles_llb);
static int string_read(string_state* state, const as_bin* b, as_bin* rb);
static bool string_parse_op(string_state* state, string_op* op);

//==========================================================
// Inlines & macros.
//

// Initializes a single-vector msgpack_in_vec from an as_msg_op value payload.
// Use this at transaction entry points before calling string_state_init.
#define INIT_MV_FROM_MSG(_msg_op)                                              \
	msgpack_vec vecs = { .buf = as_msg_op_get_value_p(_msg_op),                \
		.buf_sz = as_msg_op_get_value_sz(_msg_op) };                           \
	msgpack_in_vec mv = { .n_vecs = 1, .vecs = &vecs }

//==========================================================
// Public API - entry points called from transaction code.
// New entry points should go through string_modify/string_read which set
// has_cp_len and old_cp_len before calling def->prepare.
//

int
string_to_string(const as_bin* b, as_bin* rb)
{
	string_mem* src = (string_mem*)b->particle;

	return string_particle_bin_from_bytes(src->data, src->sz, rb);
}

// Transaction entry point for string modify ops.
// Decodes the op and args from the wire message, then executes the modify.
// Returns AS_OK on success, negative AS_ERR_* on failure.
int
as_bin_string_modify_tr(as_bin* b, const as_msg_op* msg_op,
		cf_ll_buf* particles_llb)
{
	// Unlike bits, string ops should work on STRING type
	if ((as_particle_type)msg_op->particle_type != AS_PARTICLE_TYPE_STRING) {
		as_error_details_set_fmt(AS_SUB_NONE,
				"string modify requires string particle type, got %s",
				as_particle_type_str(msg_op->particle_type));
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	INIT_MV_FROM_MSG(msg_op);
	string_state state = { 0 };

	if (! string_state_init(&state, msg_op->name, msg_op->name_sz, &mv, false,
				false)) {
		// Error details set by string_state_init.
		return -AS_ERR_PARAMETER;
	}

	return string_modify(&state, b, particles_llb);
}

// Transaction entry point for string read ops.
// Decodes the op and args from the wire message, then executes the read.
// Result is written into rb; returns AS_OK on success, negative AS_ERR_* on failure.
int
as_bin_string_read_tr(const as_bin* b, const as_msg_op* msg_op, as_bin* rb)
{
	INIT_MV_FROM_MSG(msg_op);
	string_state state = { 0 };

	if (! string_state_init(&state, msg_op->name, msg_op->name_sz, &mv, true,
				false)) {
		// Error details set by string_state_init.
		return -AS_ERR_PARAMETER;
	}

	return string_read(&state, b, rb);
}

// Expression entry point for string modify ops.
// Like as_bin_string_modify_tr but takes a pre-parsed msgpack vec (no wire message).
// particles_llb is NULL; caller is responsible for particle lifetime.
int
as_bin_string_modify_exp(as_bin* b, msgpack_in_vec* mv)
{
	string_state state = { 0 };

	if (! string_state_init(&state, NULL, 0, mv, false, true)) {
		// Error details set by string_state_init.
		return -AS_ERR_PARAMETER;
	}

	return string_modify(&state, b, NULL);
}

// Expression entry point for string read ops.
// Like as_bin_string_read_tr but takes a pre-parsed msgpack vec (no wire message).
// Result is written into rb; returns AS_OK on success, negative AS_ERR_* on failure.
int
as_bin_string_read_exp(const as_bin* b, msgpack_in_vec* mv, as_bin* rb)
{
	string_state state = { 0 };

	if (! string_state_init(&state, NULL, 0, mv, true, true)) {
		// Error details set by string_state_init.
		return -AS_ERR_PARAMETER;
	}

	return string_read(&state, b, rb);
}

// Returns the human-readable name for a string op code (e.g. "string_upper").
// Used in log/warning messages. Returns "INVALID_STRING_OP" for unknown codes.
const char*
as_string_op_name(uint32_t op_code, bool is_modify)
{
	const char* name = "INVALID_STRING_OP";

	if (is_modify) {
		if (op_code >= AS_STRING_MODIFY_OP_START &&
				op_code < AS_STRING_MODIFY_OP_END) {
			name = string_modify_op_table[op_code - AS_STRING_MODIFY_OP_START].name;
		}
	}
	else if (op_code < AS_STRING_READ_OP_END) {
		name = string_read_op_table[op_code].name;
	}

	return name;
}

//==========================================================
// Local helpers - string state init and main entry points.
//

/// @brief Initialize the string state for a string operation. Called by expr / tr entry points.
/// @param state The string state to initialize.
/// @param bin_name The name of the bin (NULL for expression context).
/// @param bin_name_sz The length of the bin name (0 for expression context).
/// @param mv The message pack input vector.
/// @param is_read Whether the operation is a read operation.
/// @param is_expr Whether invoked from expression context (no bin name available).
/// @return True if the string state was initialized successfully, false otherwise.
static bool
string_state_init(string_state* state, const uint8_t* bin_name,
		uint8_t bin_name_sz, msgpack_in_vec* mv, bool is_read, bool is_expr)
{
	*state = (string_state){ .mv = mv, .is_expr = is_expr ? 1 : 0 };

	uint32_t ele_count;

	if (! msgpack_get_list_ele_count_vec(state->mv, &ele_count) ||
			ele_count == 0) {
		if (is_expr) {
			cf_ticker_warning(AS_PARTICLE,
					"string_state_init - error %u (expression) "
					"insufficient args (%u) or unable to parse args",
					AS_ERR_PARAMETER, ele_count);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
					"string op has insufficient args or malformed request");
		}
		else {
			cf_ticker_warning(AS_PARTICLE,
					"string_state_init - error %u bin %.*s "
					"insufficient args (%u) or unable to parse args",
					AS_ERR_PARAMETER, (int)bin_name_sz, bin_name, ele_count);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
					"string op on bin %.*s has insufficient args or malformed request",
					(int)bin_name_sz, bin_name);
		}
		return false;
	}

	state->n_args = ele_count - 1; // removed op argument

	uint64_t op_code;

	if (! msgpack_get_uint64_vec(state->mv, &op_code)) {
		if (is_expr) {
			cf_ticker_warning(AS_PARTICLE,
					"string_state_init - error %u (expression) unable to parse op",
					AS_ERR_PARAMETER);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
					"string op has unreadable op code");
		}
		else {
			cf_ticker_warning(AS_PARTICLE,
					"string_state_init - error %u bin %.*s unable to parse op",
					AS_ERR_PARAMETER, (int)bin_name_sz, bin_name);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
					"string op on bin %.*s has unreadable op code",
					(int)bin_name_sz, bin_name);
		}
		return false;
	}

	// Check for context sentinel (0xFF), mirroring AS_CDT_OP_CONTEXT_EVAL.
	if (op_code == AS_STRING_OP_CONTEXT_EVAL) {
		state->has_ctx = true;

		// Remember where the ctx list starts so we can replay it later.
		state->ctx_mv_idx = state->mv->idx;
		state->ctx_mv_offset = state->mv->vecs[state->mv->idx].offset;

		// Skip over ctx list without parsing (cdt_leaf_apply_* will parse it).
		if (msgpack_sz_vec(state->mv) == 0) {
			if (is_expr) {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u (expression) unable to parse ctx",
						AS_ERR_PARAMETER);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_CTX_NOT_APPLICABLE,
						"string op has malformed context path");
			}
			else {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u bin %.*s unable to parse ctx",
						AS_ERR_PARAMETER, (int)bin_name_sz, bin_name);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_CTX_NOT_APPLICABLE,
						"string op on bin %.*s has malformed context path",
						(int)bin_name_sz, bin_name);
			}
			return false;
		}

		// Read the inner sub-op.
		if (! msgpack_get_uint64_vec(state->mv, &op_code)) {
			if (is_expr) {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u (expression) "
						"unable to parse inner op",
						AS_ERR_PARAMETER);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op has unreadable inner op code");
			}
			else {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u bin %.*s "
						"unable to parse inner op",
						AS_ERR_PARAMETER, (int)bin_name_sz, bin_name);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op on bin %.*s has unreadable inner op code",
						(int)bin_name_sz, bin_name);
			}
			return false;
		}

		// We consumed sentinel + ctx + inner_op; adjust n_args.
		state->n_args -= 2;
	}

	state->op_type = (as_string_op_type)op_code;

	if (is_read) {
		if (state->op_type >= AS_STRING_READ_OP_END) {
			if (is_expr) {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u (expression) op %u expected read op",
						AS_ERR_PARAMETER, state->op_type);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op %u is not a valid read op (max is %u)",
						state->op_type, AS_STRING_READ_OP_END - 1);
			}
			else {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u bin %.*s op %u expected read op",
						AS_ERR_PARAMETER, (int)bin_name_sz, bin_name,
						state->op_type);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op %u on bin %.*s is not a valid read op (max is %u)",
						state->op_type, (int)bin_name_sz, bin_name,
						AS_STRING_READ_OP_END - 1);
			}
			return false;
		}

		state->def = (string_op_def*)&string_read_op_table[state->op_type];
	}
	else {
		if (state->op_type < AS_STRING_MODIFY_OP_START ||
				state->op_type >= AS_STRING_MODIFY_OP_END) {
			if (is_expr) {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u (expression) op %u "
						"expected modify op",
						AS_ERR_PARAMETER, state->op_type);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op %u is not a valid modify op (range %u-%u)",
						state->op_type, AS_STRING_MODIFY_OP_START,
						AS_STRING_MODIFY_OP_END - 1);
			}
			else {
				cf_ticker_warning(AS_PARTICLE,
						"string_state_init - error %u bin %.*s op %u "
						"expected modify op",
						AS_ERR_PARAMETER, (int)bin_name_sz, bin_name,
						state->op_type);
				as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
						"string op %u on bin %.*s is not a valid modify op (range %u-%u)",
						state->op_type, (int)bin_name_sz, bin_name,
						AS_STRING_MODIFY_OP_START, AS_STRING_MODIFY_OP_END - 1);
			}
			return false;
		}

		state->def = (string_op_def*)&string_modify_op_table[state->op_type -
				AS_STRING_MODIFY_OP_START];
	}

	state->bin_name_sz = bin_name_sz;
	state->bin_name = bin_name;

	return true;
}

// Core modify dispatcher: parses op args, validates bin type, calls prepare,
// allocates the new particle, executes the modify fn, and commits the result.
//
// @param state   Initialized execution context (op type, def, bin name).
// @param b       Target bin; must be a live STRING bin, except INSERT, CONCAT,
//                APPEND, and PREPEND may create a missing bin from empty.
// @param particles_llb  Arena for particle allocation; NULL means cf_malloc is used instead.
// @return AS_OK on success; negative AS_ERR_* on failure.
//         If AS_STRING_FLAG_NO_FAIL is set, op failures return AS_OK with bin unchanged.
static int
string_modify(string_state* state, as_bin* b, cf_ll_buf* particles_llb)
{
	string_op op = { 0 };

	if (! string_parse_op(state, &op)) {
		// Error details set by string_parse_op.
		return -AS_ERR_PARAMETER;
	}

	as_particle* old_particle;

	if (as_bin_is_live(b)) {
		if ((op.flags & AS_STRING_FLAG_CREATE_ONLY) != 0) {
			if ((op.flags & AS_STRING_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			if (state->is_expr) {
				cf_detail(AS_PARTICLE,
						"string_modify - error %u operation (%s) - "
						"value exists but CREATE_ONLY flag is set",
						AS_ERR_BIN_EXISTS, state->def->name);
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s: value exists but CREATE_ONLY flag is set",
						state->def->name);
			}
			else {
				cf_detail(AS_PARTICLE,
						"string_modify - error %u operation (%s) on bin %.*s - "
						"bin exists but CREATE_ONLY flag is set",
						AS_ERR_BIN_EXISTS, state->def->name,
						(int)state->bin_name_sz, state->bin_name);
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s: bin %.*s exists but CREATE_ONLY flag is set",
						state->def->name, (int)state->bin_name_sz,
						state->bin_name);
			}
			return -AS_ERR_BIN_EXISTS;
		}

		if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_STRING) {
			if (state->is_expr) {
				cf_ticker_warning(AS_PARTICLE,
						"string_modify - error %u operation (%s) "
						"must be on a string - found %u",
						AS_ERR_INCOMPATIBLE_TYPE, state->def->name,
						as_bin_get_particle_type(b));
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s requires string value, got %s", state->def->name,
						as_particle_type_str(as_bin_get_particle_type(b)));
			}
			else {
				cf_ticker_warning(AS_PARTICLE,
						"string_modify - error %u operation (%s) on bin %.*s "
						"must be on a string - found %u",
						AS_ERR_INCOMPATIBLE_TYPE, state->def->name,
						(int)state->bin_name_sz, state->bin_name,
						as_bin_get_particle_type(b));
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s requires string bin, got %s", state->def->name,
						as_particle_type_str(as_bin_get_particle_type(b)));
			}
			return -AS_ERR_INCOMPATIBLE_TYPE;
		}

		old_particle = b->particle;
		state->old_size = ((string_mem*)old_particle)->sz;
		int64_t cp_len = utf8_string_length(((string_mem*)old_particle)->data,
				state->old_size);
		if (cp_len < 0) {
			cf_ticker_warning(AS_PARTICLE,
					"invalid UTF-8 detected in string data");
			if (state->is_expr) {
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s: value contains non-UTF-8 bytes", state->def->name);
			}
			else {
				as_error_details_set_fmt(AS_SUB_NONE,
						"%s: bin %.*s contains non-UTF-8 bytes", state->def->name,
						(int)state->bin_name_sz, state->bin_name);
			}
			return -AS_ERR_INVALID_ENCODING;
		}
		state->has_cp_len = true;
		state->old_cp_len = (uint32_t)cp_len;
	}
	else {
		if (string_op_allows_bin_create(state->op_type) &&
				(op.flags & AS_STRING_FLAG_UPDATE_ONLY) == 0) {
			old_particle = NULL;
			state->old_size = 0;
			state->old_cp_len = 0;
			state->has_cp_len = true;
		}
		else {
			if (state->is_expr) {
				cf_detail(AS_PARTICLE,
						"string_modify - operation (%s) on absent value, "
						"returning ok",
						state->def->name);
			}
			else {
				cf_detail(AS_PARTICLE,
						"string_modify - operation (%s) on absent bin %.*s, "
						"returning ok",
						state->def->name, (int)state->bin_name_sz,
						state->bin_name);
			}

			return AS_OK;
		}
	}

	// has_cp_len and old_cp_len must be set before calling prepare.
	int result = state->def->prepare(state, &op);

	if (result != AS_OK) {
		// Error details set by prepare.
		return result;
	}

	// Allocate new particle (state->new_size is upper bound for variable-size ops)
	size_t alloc_size = sizeof(string_mem) + (size_t)state->new_size;

	if (particles_llb == NULL) {
		b->particle = cf_malloc(alloc_size);
	}
	else {
		cf_ll_buf_reserve(particles_llb, alloc_size, (uint8_t**)&b->particle);
	}

	string_mem* new_str = (string_mem*)b->particle;
	static const uint8_t empty_string_data[] = { 0 };
	const uint8_t* from = old_particle == NULL
			? empty_string_data
			: ((const string_mem*)old_particle)->data;
	uint32_t actual_size;

	int modify_result = state->def->fn.modify(&op, new_str->data, from,
			state->old_size, &actual_size);

	if (modify_result != AS_OK) {
		if (particles_llb == NULL) {
			cf_free(b->particle);
		}

		b->particle = old_particle;

		if ((op.flags & AS_STRING_FLAG_NO_FAIL) == 0) {
			// Error details set by modify fn.
			return modify_result;
		}

		return AS_OK;
	}

	if (! cf_str_is_valid_utf8(new_str->data, actual_size)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify - invalid UTF-8 detected in operation result (%s)",
				state->def->name);
		as_error_details_set_fmt(AS_SUB_NONE,
				"%s: result contains non-UTF-8 bytes", state->def->name);
		if (particles_llb == NULL) {
			cf_free(b->particle);
		}
		b->particle = old_particle;
		return -AS_ERR_INVALID_ENCODING;
	}

	new_str->type = AS_PARTICLE_TYPE_STRING;
	new_str->sz = actual_size;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_STRING);
	// Old particle may be on-record flat storage, ll_buf-backed, or heap. Never
	// cf_free() here — same contract as bits_modify(). Transaction callers use a
	// non-NULL particles_llb arena; expression eval_call() frees replaced heap
	// particles via rt_value_need_destroy() + as_bin_particle_destroy() on a
	// pre-modify bin snapshot. Direct unit tests of as_bin_string_modify_exp()
	// must destroy the prior particle when the pointer changes (see
	// test_as_bin_string_modify_exp in particle_string_wire_common.h).
	return AS_OK;
}

// Core read dispatcher: parses op args, validates bin type, calls prepare,
// then executes the read fn and writes the result into rb.
//
// @param state  Initialized execution context (op type, def, bin name).
// @param b      Source bin; must be a live STRING bin.
// @param rb     Result bin; populated with the read result on success.
// @return AS_OK on success; negative AS_ERR_* on failure, with rb cleared.
static int
string_read(string_state* state, const as_bin* b, as_bin* rb)
{
	cf_assert(as_bin_is_live(b), AS_PARTICLE, "unused or dead bin");

	string_op op = { 0 };

	if (! string_parse_op(state, &op)) {
		// Error details set by string_parse_op.
		return -AS_ERR_PARAMETER;
	}

	if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_STRING) {
		if (state->is_expr) {
			cf_ticker_warning(AS_PARTICLE,
					"string_read - error %u operation (%s) "
					"must be on a string - found %u",
					AS_ERR_INCOMPATIBLE_TYPE, state->def->name,
					as_bin_get_particle_type(b));
			as_error_details_set_fmt(AS_SUB_NONE,
					"%s requires string value, got %s", state->def->name,
					as_particle_type_str(as_bin_get_particle_type(b)));
		}
		else {
			cf_ticker_warning(AS_PARTICLE,
					"string_read - error %u operation (%s) on bin %.*s "
					"must be on a string - found %u",
					AS_ERR_INCOMPATIBLE_TYPE, state->def->name,
					(int)state->bin_name_sz, state->bin_name,
					as_bin_get_particle_type(b));
			as_error_details_set_fmt(AS_SUB_NONE,
					"%s requires string bin, got %s", state->def->name,
					as_particle_type_str(as_bin_get_particle_type(b)));
		}
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	state->old_size = ((string_mem*)b->particle)->sz;
	int64_t cp_len = utf8_string_length(((string_mem*)b->particle)->data,
			state->old_size);
	state->has_cp_len = true;
	if (cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE, "invalid UTF-8 detected in string data");
		if (state->is_expr) {
			as_error_details_set_fmt(AS_SUB_NONE,
					"%s: value contains non-UTF-8 bytes", state->def->name);
		}
		else {
			as_error_details_set_fmt(AS_SUB_NONE,
					"%s: bin %.*s contains non-UTF-8 bytes", state->def->name,
					(int)state->bin_name_sz, state->bin_name);
		}
		return -AS_ERR_INVALID_ENCODING;
	}
	state->old_cp_len = (uint32_t)cp_len;
	// has_cp_len and old_cp_len must be set before calling prepare.
	int result = state->def->prepare(state, &op);

	if (result != AS_OK) {
		// Error details set by prepare.
		return result;
	}

	const uint8_t* data = ((string_mem*)b->particle)->data;

	int read_result = state->def->fn.read(&op, data, state->old_size, rb);

	if (read_result != AS_OK) {
		as_bin_set_empty(rb);
		// Error details set by read fn.
		return read_result;
	}

	return AS_OK;
}

//==========================================================
// Context-aware string ops.
// Execute string read/modify on a string leaf inside a list/map CDT.
//

// Helper to create a ctx_mv that replays the stored ctx position.
static void
string_state_ctx_mv(const string_state* state, msgpack_in_vec* ctx_mv)
{
	*ctx_mv = *state->mv;
	ctx_mv->idx = state->ctx_mv_idx;
	ctx_mv->vecs[ctx_mv->idx].offset = state->ctx_mv_offset;
}

// Core ctx read: descend to leaf, parse/prepare, execute read fn.
static int
string_read_ctx(string_state* state, const as_bin* b, as_bin* rb)
{
	string_op op = { 0 };

	if (! string_parse_op(state, &op)) {
		return -AS_ERR_PARAMETER;
	}

	// Create ctx_mv pointing to the stored ctx list position.
	define_msgpack_vec_copy(ctx_mv, state->mv);
	string_state_ctx_mv(state, &ctx_mv);

	const uint8_t* leaf_bytes;
	uint32_t leaf_sz;

	int rv = cdt_leaf_apply_read(b, &ctx_mv, AS_PARTICLE_TYPE_STRING,
			&leaf_bytes, &leaf_sz);

	if (rv != AS_OK) {
		return rv;
	}

	// leaf_bytes is type_byte + string_data. Aerospike strings have 0x03 type byte.
	if (leaf_sz < 1 || leaf_bytes[0] != AS_BYTES_STRING) {
		cf_detail(AS_PARTICLE, "string_read_ctx - leaf not an Aerospike string");
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	// Skip type byte to get raw string data.
	const uint8_t* data = leaf_bytes + 1;
	uint32_t data_sz = leaf_sz - 1;

	state->old_size = data_sz;
	int64_t cp_len = utf8_string_length(data, data_sz);

	if (cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE, "invalid UTF-8 detected in string data");
		return -AS_ERR_INVALID_ENCODING;
	}

	state->has_cp_len = true;
	state->old_cp_len = (uint32_t)cp_len;

	int result = state->def->prepare(state, &op);

	if (result != AS_OK) {
		return result;
	}

	int read_result = state->def->fn.read(&op, data, data_sz, rb);

	if (read_result != AS_OK) {
		as_bin_set_empty(rb);
		return read_result;
	}

	return AS_OK;
}

// Core ctx modify: descend to leaf, parse/prepare, execute modify fn, commit.
static int
string_modify_ctx(string_state* state, as_bin* b, cf_ll_buf* particles_llb)
{
	string_op op = { 0 };

	if (! string_parse_op(state, &op)) {
		return -AS_ERR_PARAMETER;
	}

	// Create ctx_mv pointing to the stored ctx list position.
	define_msgpack_vec_copy(ctx_mv, state->mv);
	string_state_ctx_mv(state, &ctx_mv);

	define_rollback_alloc(alloc_buf, particles_llb, 1);
	cdt_context ctx = { 0 };
	const uint8_t* leaf_bytes;
	uint32_t leaf_sz;

	int rv = cdt_leaf_apply_modify_begin(&ctx, b, alloc_buf, &ctx_mv,
			AS_PARTICLE_TYPE_STRING, &leaf_bytes, &leaf_sz);

	if (rv != AS_OK) {
		if ((op.flags & AS_STRING_FLAG_NO_FAIL) != 0 &&
				rv == -AS_ERR_OP_NOT_APPLICABLE) {
			return AS_OK;
		}
		return rv;
	}

	// leaf_bytes is type_byte + string_data. Aerospike strings have 0x03 type byte.
	if (leaf_sz < 1 || leaf_bytes[0] != AS_BYTES_STRING) {
		cf_detail(AS_PARTICLE,
				"string_modify_ctx - leaf not an Aerospike string");
		cf_free(ctx.pstack);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	// Skip type byte to get raw string data.
	const uint8_t* from = leaf_bytes + 1;
	uint32_t from_sz = leaf_sz - 1;

	state->old_size = from_sz;
	int64_t cp_len = utf8_string_length(from, from_sz);

	if (cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE, "invalid UTF-8 detected in string data");
		cf_free(ctx.pstack);
		return -AS_ERR_INVALID_ENCODING;
	}

	state->has_cp_len = true;
	state->old_cp_len = (uint32_t)cp_len;

	int result = state->def->prepare(state, &op);

	if (result != AS_OK) {
		cf_free(ctx.pstack);
		if ((op.flags & AS_STRING_FLAG_NO_FAIL) != 0) {
			return AS_OK;
		}
		return result;
	}

	// Allocate scratch buffer for the modify result.
	uint8_t stack_buf[STRING_OP_STACK_BUF_SZ];
	uint8_t* to = stack_buf;
	bool heap_alloc = false;

	if (state->new_size > STRING_OP_STACK_BUF_SZ) {
		to = cf_malloc(state->new_size);
		heap_alloc = true;
	}

	uint32_t actual_size;

	int modify_result =
			state->def->fn.modify(&op, to, from, from_sz, &actual_size);

	if (modify_result != AS_OK) {
		if (heap_alloc) {
			cf_free(to);
		}
		cf_free(ctx.pstack);

		if ((op.flags & AS_STRING_FLAG_NO_FAIL) == 0) {
			return modify_result;
		}

		return AS_OK;
	}

	if (! cf_str_is_valid_utf8(to, actual_size)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_ctx - invalid UTF-8 detected in operation result (%s)",
				state->def->name);
		if (heap_alloc) {
			cf_free(to);
		}
		cf_free(ctx.pstack);
		return -AS_ERR_INVALID_ENCODING;
	}

	// Commit the new string into the CDT.
	rv = cdt_leaf_apply_modify_commit(&ctx, AS_PARTICLE_TYPE_STRING, to,
			actual_size);

	if (heap_alloc) {
		cf_free(to);
	}

	return rv;
}

// Transaction entry point for context-aware string read ops.
int
as_bin_string_read_ctx_tr(const as_bin* b, const as_msg_op* msg_op, as_bin* rb)
{
	INIT_MV_FROM_MSG(msg_op);
	string_state state = { 0 };

	if (! string_state_init(&state, msg_op->name, msg_op->name_sz, &mv, true,
				false)) {
		return -AS_ERR_PARAMETER;
	}

	if (! state.has_ctx) {
		cf_warning(AS_PARTICLE,
				"as_bin_string_read_ctx_tr called but no ctx present");
		return -AS_ERR_PARAMETER;
	}

	return string_read_ctx(&state, b, rb);
}

// Transaction entry point for context-aware string modify ops.
int
as_bin_string_modify_ctx_tr(as_bin* b, const as_msg_op* msg_op,
		cf_ll_buf* particles_llb)
{
	INIT_MV_FROM_MSG(msg_op);
	string_state state = { 0 };

	if (! string_state_init(&state, msg_op->name, msg_op->name_sz, &mv, false,
				false)) {
		return -AS_ERR_PARAMETER;
	}

	if (! state.has_ctx) {
		cf_warning(AS_PARTICLE,
				"as_bin_string_modify_ctx_tr called but no ctx present");
		return -AS_ERR_PARAMETER;
	}

	return string_modify_ctx(&state, b, particles_llb);
}

// Validates arg count and dispatches to each per-arg parse function in order.
// Populates op fields (int_arg1, int_arg2, buf, buf_sz, flags) ready for prepare/execute.
static bool
string_parse_op(string_state* state, string_op* op)
{
	string_op_def* def = state->def;

	// Check for the correct number of args
	if (state->n_args < def->min_args || state->n_args > def->max_args) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_op - error %u op %s(%u) unexpected number of args %u",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->n_args);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s has %u args, expected %u to %u", state->def->name,
				state->n_args, def->min_args, def->max_args);
		return false;
	}

	// Parse the args with the appropriate parse function
	for (uint32_t i = 0; i < state->n_args; i++) {
		if (! def->args[i](state, op)) {
			// Error details set by parse fn.
			return false;
		}
	}

	return true;
}

//==========================================================
// Local helpers - parse functions.
//

// Parses the first integer argument into op->int_arg1.
// Meaning is op-dependent: code-point index (INSERT, OVERWRITE, CHAR_AT, SUBSTR, SNIP),
// occurrence number (FIND), target length (PAD_START, PAD_END), repeat count (REPEAT),
// numeric type selector (IS_NUMERIC), or regex flags (REGEX_COMPARE, REGEX_REPLACE).
static bool
string_parse_int1(string_state* state, string_op* op)
{
	int64_t val;

	if (! msgpack_get_int64_vec(state->mv, &val)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_int1 - error %u op %s (%u) unable to parse int1",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s: first integer arg is not a valid integer", state->def->name);
		return false;
	}

	if (val < -(int64_t)PROTO_SIZE_MAX || val > (int64_t)PROTO_SIZE_MAX) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_int1"
				" - error %u op %s (%u) int1 (%ld) larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, val,
				PROTO_SIZE_MAX);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s: first integer arg %ld exceeds max %d", state->def->name,
				val, PROTO_SIZE_MAX);
		return false;
	}

	op->int_arg1 = val;

	return true;
}

// Parses the second integer argument into op->int_arg2.
// Always an exclusive end index (code-point position); normalized in prepare.
static bool
string_parse_int2(string_state* state, string_op* op)
{
	int64_t val;

	if (! msgpack_get_int64_vec(state->mv, &val)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_int2 - error %u op %s (%u) unable to parse int2",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s: second integer arg is not a valid integer",
				state->def->name);
		return false;
	}

	if (val < -(int64_t)PROTO_SIZE_MAX || val > (int64_t)PROTO_SIZE_MAX) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_int2 - "
				"error %u op %s (%u) int2 (%ld) larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, val,
				PROTO_SIZE_MAX);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s: second integer arg %ld exceeds max %d", state->def->name,
				val, PROTO_SIZE_MAX);
		return false;
	}

	op->int_arg2 = val;

	return true;
}

// Validates a UTF-8 string arg; logs and returns false on invalid UTF-8.
static bool
string_validate_utf8_arg(const uint8_t* buf, uint32_t sz, const char* op_name,
		const char* arg_label)
{
	if (cf_str_is_valid_utf8(buf, sz)) {
		return true;
	}

	cf_ticker_warning(AS_PARTICLE,
			"string_validate_utf8_arg - error %u op %s: %s is not valid UTF-8",
			AS_ERR_PARAMETER, op_name, arg_label);
	as_error_details_set_fmt(AS_SUB_PARAM_STRING_UTF8_INVALID,
			"%s: %s arg contains non-UTF-8 bytes", op_name, arg_label);

	return false;
}

// Parses a binary/string argument into op->buf and op->buf_sz.
// Skips the leading AS msgpack type byte so buf points to raw UTF-8 content.
// Validates UTF-8 and returns false on failure.
static bool
string_parse_buf(string_state* state, string_op* op)
{
	uint32_t size;

	op->buf = msgpack_get_bin_vec(state->mv, &size);

	// AS msgpack has a one byte blob type field which we ignore here.
	if (op->buf == NULL || size == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_buf - "
				"error %u op %s (%u) parsed invalid buffer with size %u",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size);
		return false;
	}

	op->buf++;
	op->buf_sz = size - 1;

	if (! string_validate_utf8_arg(op->buf, op->buf_sz, state->def->name,
				"argument")) {
		return false;
	}

	return true;
}

// Parses buf as a msgpack array and sets op->buf and op->buf_sz.
// The array is polymorphic depending on the operation.
// Should always contain only strings.
static bool
string_parse_list(string_state* state, string_op* op)
{
	op->buf = msgpack_get_ele_vec(state->mv, &op->buf_sz);

	if (op->buf == NULL || op->buf_sz == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_list - error %u op %s (%u) invalid string list",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	return true;
}

// Parses the policy flags argument into op->flags and validates against
// the op's bad_flags mask (flags not permitted for this op).
static bool
string_parse_flags(string_state* state, string_op* op)
{
	if (! msgpack_get_uint64_vec(state->mv, &op->flags)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_flags - error %u op %s (%u) unable to parse flags",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if ((op->flags & state->def->bad_flags) != 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_flags - error %u op %s (%u) invalid flags (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags);
		return false;
	}

	if ((op->flags & AS_STRING_FLAG_CREATE_ONLY) != 0 &&
			(op->flags & AS_STRING_FLAG_UPDATE_ONLY) != 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_flags - error %u op %s (%u) invalid flags combination (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags);
		return false;
	}

	if (state->has_ctx && (op->flags & AS_STRING_FLAG_CREATE_ONLY) != 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_flags - error %u op %s (%u) CREATE_ONLY not supported with context",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	return true;
}

//==========================================================
// Local helpers - prepare functions.
//

/// @brief Normalize a single code-point index (for CHAR_AT, INSERT).
/// Input/Output: int_arg1 = index (code-point index, clamped to [0, len]).
static int
string_normalize_single_index(const string_state* state, string_op* op)
{
	int64_t len = (int64_t)state->old_cp_len;

	if (op->int_arg1 < 0)
		op->int_arg1 = len + op->int_arg1;
	if (op->int_arg1 < 0)
		op->int_arg1 = 0;
	if (op->int_arg1 > len)
		op->int_arg1 = len;

	return AS_OK;
}

/// @brief Normalize the arguments for the SUBSTR/SNIP operations.
/// Input/Output: int_arg1 = from_idx, int_arg2 = to_idx (code-point indices, clamped to [0, len]).
/// @return AS_OK if successful,
///     -AS_ERR_PARAMETER if the operation cannot be performed.
static int
string_normalize_index_range_args(const string_state* state, string_op* op)
{
	int64_t len = (int64_t)state->old_cp_len;

	// normalize from_idx: negative wraps from end
	if (op->int_arg1 < 0)
		op->int_arg1 = len + op->int_arg1;
	if (op->int_arg1 < 0)
		op->int_arg1 = 0;
	if (op->int_arg1 > len)
		op->int_arg1 = len;

	// normalize to_idx: negative wraps from end
	if (op->int_arg2 < 0)
		op->int_arg2 = len + op->int_arg2;
	if (op->int_arg2 < 0)
		op->int_arg2 = 0;
	if (op->int_arg2 > len)
		op->int_arg2 = len;

	// if from >= to, result is empty substring
	if (op->int_arg1 >= op->int_arg2) {
		op->int_arg1 = 0;
		op->int_arg2 = 0;
	}

	return AS_OK;
}

// Prepares a read op before execution: normalizes index/range args against
// the current string length and fills in defaults for optional arguments.
// No allocation or size bookkeeping needed for read ops.
// Requires state->has_cp_len to be set before calling.
static int
string_prepare_read_op(string_state* state, string_op* op)
{
	// Caller must set has_cp_len and old_cp_len - see string_read:679.
	cf_assert(state->has_cp_len, AS_PARTICLE,
			"string read op requires known code point length");

	switch (state->op_type) {
	case AS_STRING_OP_STRLEN:
	case AS_STRING_OP_STARTS_WITH:
	case AS_STRING_OP_ENDS_WITH:
	case AS_STRING_OP_TO_INTEGER:
	case AS_STRING_OP_TO_DOUBLE:
	case AS_STRING_OP_BYTE_LENGTH:
	case AS_STRING_OP_CONTAINS:
	case AS_STRING_OP_IS_NUMERIC:
	case AS_STRING_OP_IS_UPPER:
	case AS_STRING_OP_IS_LOWER:
	case AS_STRING_OP_TO_BLOB:
	case AS_STRING_OP_SPLIT:
	case AS_STRING_OP_B64_DECODE:
	case AS_STRING_OP_REGEX_COMPARE:
		return AS_OK;
	case AS_STRING_OP_SUBSTR:
		// fill in end index when only start index is provided
		if (state->n_args == 1) {
			op->int_arg2 = (int64_t)state->old_cp_len;
		}
		return string_normalize_index_range_args(state, op);
	case AS_STRING_OP_CHAR_AT:
		return string_normalize_single_index(state, op);
	case AS_STRING_OP_FIND:
		if (state->n_args == 1) {
			op->int_arg1 = 1; // default to first occurrence when not provided
		}
		if (op->int_arg1 == 0)
			// occurrence must be non-zero (1=first, -1=last)
			return -AS_ERR_PARAMETER;
		break;
	default:
		// should never happen?
		return -AS_ERR_OP_NOT_APPLICABLE;
	}
	return AS_OK;
}

// Parse the [needle, replacement] msgpack list from op->buf.
// Returns AS_OK and sets *needle_sz_r / *replacement_sz_r (both without the
// Aerospike type byte) on success, or a negative error code on failure.
static int
string_parse_needle_replacement(const string_op* op, const string_state* state,
		uint32_t* needle_sz_r, uint32_t* replacement_sz_r)
{
	msgpack_in mp = { .buf = op->buf, .buf_sz = op->buf_sz };

	uint32_t argc;
	if (! msgpack_get_list_ele_count(&mp, &argc)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_needle_replacement - "
				"error parsing args for op %s (%u)",
				state->def->name, state->op_type);
		return -AS_ERR_PARAMETER;
	}
	if (argc != 2) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_needle_replacement - "
				"%s expects exactly 2 string arguments, got %u",
				state->def->name, argc);
		return -AS_ERR_PARAMETER;
	}

	uint32_t needle_sz;
	const uint8_t* needle = msgpack_get_bin(&mp, &needle_sz);

	if (needle == NULL || needle_sz <= 1) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_needle_replacement - "
				"%s needle is missing or empty",
				state->def->name);
		return -AS_ERR_PARAMETER;
	}

	// Skip Aerospike type byte for UTF-8 validation and size output
	if (! string_validate_utf8_arg(needle + 1, needle_sz - 1, state->def->name,
				"needle")) {
		return -AS_ERR_PARAMETER;
	}
	*needle_sz_r = needle_sz - 1;

	uint32_t replacement_sz;
	const uint8_t* replacement = msgpack_get_bin(&mp, &replacement_sz);

	if (replacement == NULL || replacement_sz == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_parse_needle_replacement - "
				"%s failed to parse replacement size",
				state->def->name);
		return -AS_ERR_PARAMETER;
	}

	// Skip Aerospike type byte for UTF-8 validation and size output
	if (replacement_sz > 1 &&
			! string_validate_utf8_arg(replacement + 1, replacement_sz - 1,
					state->def->name, "replacement")) {
		return -AS_ERR_PARAMETER;
	}
	*replacement_sz_r = replacement_sz - 1;

	return AS_OK;
}

// Upper bound for allocated buffer during string modify (matches replace-all scan cap).
static int
string_modify_set_estimated_size(string_state* state, uint64_t v)
{
	if (v > STRING_REPLACE_ALL_MAX) {
		cf_ticker_warning(AS_PARTICLE,
				"string_prepare_modify_op - "
				"error %u op %s - estimated output size %llu exceeds maximum %u",
				AS_ERR_PARAMETER, state->def->name, (unsigned long long)v,
				STRING_REPLACE_ALL_MAX);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
				"%s: estimated result size exceeds server limit",
				state->def->name);
		return -AS_ERR_PARAMETER;
	}
	state->new_size = (uint32_t)v;
	return AS_OK;
}

static int
string_modify_estimated_size_overflow(string_state* state)
{
	cf_ticker_warning(AS_PARTICLE,
			"string_prepare_modify_op - "
			"error %u op %s - estimated output size arithmetic overflow",
			AS_ERR_PARAMETER, state->def->name);
	as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
			"%s: estimated result size overflow", state->def->name);
	return -AS_ERR_PARAMETER;
}

// Prepares a modify op before execution: normalizes index/range args, validates
// bounds, parses needle/replacement for replace ops, and sets state->new_size
// as an upper bound for the particle allocation in string_modify.
// Requires state->has_cp_len to be set before calling.
static int
string_prepare_modify_op(string_state* state, string_op* op)
{
	// Caller must set has_cp_len and old_cp_len - see string_modify:600.
	cf_assert(state->has_cp_len, AS_PARTICLE,
			"string modify op requires known code point length");

	switch (state->op_type) {
	case AS_STRING_OP_SNIP: {
		if (state->n_args == 1) {
			op->int_arg2 = (int64_t)state->old_cp_len;
		}

		int snip_rc = string_normalize_index_range_args(state, op);

		if (snip_rc != AS_OK) {
			return snip_rc;
		}

		return string_modify_set_estimated_size(state, state->old_size);
	}
	case AS_STRING_OP_TRIM_START:
	case AS_STRING_OP_TRIM_END:
	case AS_STRING_OP_TRIM:
		return string_modify_set_estimated_size(state, state->old_size);
	case AS_STRING_OP_UPPER:
	case AS_STRING_OP_LOWER:
	case AS_STRING_OP_CASE_FOLD:
	case AS_STRING_OP_NORMALIZE_NFC: {
		// Unicode can expand (letters) up to 3x
		uint64_t v;

		if (__builtin_mul_overflow((uint64_t)state->old_size, 3ULL, &v)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, v);
	}
	case AS_STRING_OP_REPLACE:
	case AS_STRING_OP_REPLACE_ALL: {
		uint32_t needle_sz, replacement_sz;
		int rc = string_parse_needle_replacement(op, state, &needle_sz,
				&replacement_sz);

		if (rc != AS_OK) {
			return rc;
		}

		// Multiply by 3 for worst-case UTF-16->UTF-8 re-encoding expansion.
		// REPLACE: k=1, at most one substitution.
		// REPLACE_ALL: at most (old_size / needle_sz) non-overlapping substitutions.
		uint64_t k = (state->op_type == AS_STRING_OP_REPLACE)
				? 1ULL
				: (uint64_t)(state->old_size / needle_sz);
		uint64_t removed;

		if (__builtin_mul_overflow(k, (uint64_t)needle_sz, &removed)) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t base = (uint64_t)state->old_size;

		if (removed > base) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t added;

		if (__builtin_mul_overflow(k, (uint64_t)replacement_sz, &added)) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t inner;

		if (__builtin_add_overflow(base - removed, added, &inner)) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t est;

		if (__builtin_mul_overflow(inner, 3ULL, &est)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, est);
	}
	case AS_STRING_OP_INSERT: {
		string_normalize_single_index(state, op);
		uint64_t v;

		if (__builtin_add_overflow((uint64_t)state->old_size,
					(uint64_t)op->buf_sz, &v)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, v);
	}
	case AS_STRING_OP_OVERWRITE:
		if (op->int_arg1 < 0 || op->int_arg1 >= (int64_t)state->old_cp_len) {
			cf_ticker_warning(AS_PARTICLE,
					"string_prepare_modify_op - "
					"error %u op %s - invalid overwrite index (%ld)",
					AS_ERR_PARAMETER, state->def->name, op->int_arg1);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_INDEX_OUT_OF_BOUNDS,
					"%s: index %ld out of bounds for string length %u",
					state->def->name, op->int_arg1, state->old_cp_len);
			return -AS_ERR_PARAMETER;
		}
		// fall through — same allocation bound as concat (payload appended by op).
	case AS_STRING_OP_CONCAT: {
		uint64_t v;

		if (__builtin_add_overflow((uint64_t)state->old_size,
					(uint64_t)op->buf_sz, &v)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, v);
	}
	case AS_STRING_OP_APPEND:
	case AS_STRING_OP_PREPEND: {
		uint64_t v;

		if (__builtin_add_overflow((uint64_t)state->old_size,
					(uint64_t)op->buf_sz, &v)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, v);
	}
	case AS_STRING_OP_PAD_START:
	case AS_STRING_OP_PAD_END:
		if (op->int_arg1 < 0 || op->buf_sz == 0) {
			cf_ticker_warning(AS_PARTICLE,
					"string_prepare_modify_op - error %u op %s - invalid padding",
					AS_ERR_PARAMETER, state->def->name);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
					"%s: target length %ld must be non-negative and pad string must not be empty",
					state->def->name, op->int_arg1);
			return -AS_ERR_PARAMETER;
		}
		{
			// worst-case UTF-8 expansion: target_length chars, up to 4 bytes each
			uint64_t max_pad_sz;

			if (__builtin_mul_overflow((uint64_t)op->int_arg1, 4ULL, &max_pad_sz)) {
				return string_modify_estimated_size_overflow(state);
			}

			uint64_t v = state->old_size > max_pad_sz ? state->old_size
													  : max_pad_sz;

			return string_modify_set_estimated_size(state, v);
		}
	case AS_STRING_OP_REPEAT:
		if (op->int_arg1 < 0) {
			cf_ticker_warning(AS_PARTICLE,
					"string_prepare_modify_op - "
					"error %u op %s - unexpected negative repeat count %ld",
					AS_ERR_PARAMETER, state->def->name, op->int_arg1);
			as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_PARAMS_INVALID,
					"%s: repeat count %ld must be non-negative",
					state->def->name, op->int_arg1);
			return -AS_ERR_PARAMETER;
		}
		{
			uint64_t v;

			if (__builtin_mul_overflow((uint64_t)state->old_size,
						(uint64_t)op->int_arg1, &v)) {
				return string_modify_estimated_size_overflow(state);
			}

			return string_modify_set_estimated_size(state, v);
		}
	case AS_STRING_OP_REGEX_REPLACE: {
		uint64_t old_sz = state->old_size;
		uint64_t mul;

		if (__builtin_mul_overflow(old_sz + 1, (uint64_t)op->buf_sz, &mul)) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t sum;

		if (__builtin_add_overflow(old_sz, mul, &sum)) {
			return string_modify_estimated_size_overflow(state);
		}

		uint64_t est;

		if (__builtin_mul_overflow(sum, 3ULL, &est)) {
			return string_modify_estimated_size_overflow(state);
		}

		return string_modify_set_estimated_size(state, est);
	}
	default:
		cf_ticker_warning(AS_PARTICLE,
				"string_prepare_modify_op - "
				"error %u op %s - unexpected read op type %u",
				AS_ERR_OP_NOT_APPLICABLE, state->def->name, state->op_type);
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_OP_INVALID,
				"%s: op type %u is not a modify op", state->def->name,
				state->op_type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

// Helper utilities for string ops

// Returns true if every byte in [data, data+sz) is a 7-bit ASCII value.
//
// ASCII bytes have bit 7 clear (value <= 0x7F). By OR-ing all bytes into an
// accumulator, any non-ASCII byte sets bit 7 of its position. Processing 8
// bytes at a time via a uint64_t load lets us check all 8 high bits in one
// mask test (0x8080808080808080 selects bit 7 of each byte lane). The tail
// loop handles the remaining < 8 bytes the same way into the same accumulator.
static bool
is_ascii(const uint8_t* data, uint32_t sz)
{
	uint64_t acc = 0;
	uint32_t i = 0;

	for (; i + 8 <= sz; i += 8) {
		uint64_t chunk;
		memcpy(&chunk, data + i, 8);
		acc |= chunk;
	}

	for (; i < sz; i++) {
		acc |= data[i];
	}

	return (acc & 0x8080808080808080ULL) == 0;
}

// find the first non-whitespace character's position
// Uses u_isUWhiteSpace (Unicode White_Space property) which includes NBSP, etc.
static size_t
find_first_non_ws(const uint8_t* from, uint32_t sz)
{
	if (is_ascii(from, sz)) {
		for (uint32_t i = 0; i < sz; i++) {
			uint8_t c = from[i];
			if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\f' &&
					c != '\v') {
				return i;
			}
		}
		return sz;
	}

	int32_t i = 0;
	while (i < (int32_t)sz) {
		int32_t pos = i;
		UChar32 c;
		U8_NEXT(from, i, (int32_t)sz, c);
		if (! u_isUWhiteSpace(c))
			return (size_t)pos;
	}
	return sz;
}

// find the last non-whitespace character's position
static size_t
find_last_non_ws_end(const uint8_t* from, uint32_t sz)
{
	if (is_ascii(from, sz)) {
		for (uint32_t i = sz; i > 0; i--) {
			uint8_t c = from[i - 1];
			if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\f' &&
					c != '\v') {
				return i;
			}
		}
		return 0;
	}

	int32_t i = (int32_t)sz;
	while (i > 0) {
		int32_t pos = i;
		UChar32 c;
		U8_PREV(from, 0, i, c);
		if (! u_isUWhiteSpace(c)) {
			return (size_t)pos;
		}
	}
	return 0;
}

// count the number of *code points* in a UTF-8 string
static int64_t
utf8_string_length(const uint8_t* from, uint32_t sz)
{
	if (is_ascii(from, sz)) {
		return (int64_t)sz;
	}

	int64_t count = 0;
	int32_t i = 0;

	while (i < (int32_t)sz) {
		UChar32 c;
		U8_NEXT(from, i, (int32_t)sz, c);
		if (c < 0) { // U_SENTINEL = -1 for malformed
			// Caller logs cf_ticker_warning; detail only here (offset for support).
			cf_ticker_detail(AS_PARTICLE,
					"utf8_string_length - malformed UTF-8 at byte offset %d", i);
			return -1;
		}
		count++;
	}

	return count;
}

// Map ICU status from u_strFromUTF8 / u_strToUTF8 / u_strToUpper / u_strToLower / …
static int
icu_uerror_to_as_err(UErrorCode status)
{
	switch (status) {
	case U_ZERO_ERROR:
		return AS_OK;
	case U_INVALID_CHAR_FOUND:
		return -AS_ERR_INVALID_ENCODING;
	case U_ILLEGAL_ARGUMENT_ERROR:
		return -AS_ERR_PARAMETER;
	default:
		// Includes U_BUFFER_OVERFLOW_ERROR (caller sizing bug — already
		// cf_crash'd at inline sites where overflow is impossible) and
		// U_MEMORY_ALLOCATION_ERROR (ICU internal OOM).
		return U_FAILURE(status) ? -AS_ERR_OP_NOT_APPLICABLE : AS_OK;
	}
}

// Convert a UTF-8 string to UTF-16. Tries the caller-provided stack buffer
// first; falls back to heap allocation when the result doesn't fit.
//
// All out-params are written unconditionally -- no pre-initialization needed.
//
// @param utf8       Source UTF-8 bytes.
// @param utf8_sz    Length of @p utf8 in bytes.
// @param stack_buf  Caller-provided stack buffer for the UTF-16 output.
// @param stack_cap  Capacity of @p stack_buf in UChar units.
// @param out_u16    [out] Points to the UTF-16 result (either @p stack_buf or
//                   a heap allocation).
// @param out_len    [out] Length of the UTF-16 result in UChar units.
// @param out_heap   [out] Heap pointer if allocated, NULL when @p stack_buf was
//                   used. Declare with DEFER_ATTR_FREE for automatic cleanup.
// @return AS_OK on success; -AS_ERR_INVALID_ENCODING for ill-formed UTF-8;
//         -AS_ERR_PARAMETER for illegal ICU arguments;
//         -AS_ERR_OP_NOT_APPLICABLE for other ICU failures (including OOM).
static int
utf8_to_u16(const uint8_t* utf8, uint32_t utf8_sz, UChar* stack_buf,
		int32_t stack_cap, UChar** out_u16, int32_t* out_len, void** out_heap)
{
	UErrorCode status = U_ZERO_ERROR;
	*out_heap = NULL;
	*out_u16 = stack_buf;

	u_strFromUTF8(stack_buf, stack_cap, out_len, (const char*)utf8,
			(int32_t)utf8_sz, &status);

	// Stack buffer too small - heap allocate with the exact required size
	if (status == U_BUFFER_OVERFLOW_ERROR) {
		status = U_ZERO_ERROR;
		// Guard *out_len so *out_len+1 fits int32_t (ICU destCapacity) and
		// (nchars * sizeof(UChar)) cannot wrap size_t.

		if (*out_len < 0 || *out_len > INT32_MAX - 1) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		size_t nchars;

		if (__builtin_add_overflow((size_t)*out_len, 1, &nchars)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		size_t nbytes;

		if (__builtin_mul_overflow(nchars, sizeof(UChar), &nbytes)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		status = U_ZERO_ERROR;
		*out_u16 = cf_malloc(nbytes);
		*out_heap = *out_u16;
		u_strFromUTF8(*out_u16, (int32_t)nchars, out_len, (const char*)utf8,
				(int32_t)utf8_sz, &status);
	}

	return icu_uerror_to_as_err(status);
}

// Convert a UTF-16 string back to UTF-8 into a pre-allocated buffer.
//
// @param utf8      Destination buffer, must be pre-allocated by the caller.
// @param utf8_cap  Capacity of @p utf8 in bytes.
// @param out_len   [out] Actual length of the UTF-8 result in bytes (only
//                  written on success).
// @param u16       Source UTF-16 string.
// @param u16_len   Length of @p u16 in UChar units.
// @return AS_OK on success. On failure, see icu_uerror_to_as_err() (INVALID_ENCODING,
//         PARAMETER, OP_NOT_APPLICABLE, …).
static int
u16_to_utf8(uint8_t* utf8, uint32_t utf8_cap, uint32_t* out_len,
		const UChar* u16, int32_t u16_len)
{
	UErrorCode status = U_ZERO_ERROR;
	int32_t result_len;

	u_strToUTF8((char*)utf8, utf8_cap, &result_len, u16, u16_len, &status);

	if (U_FAILURE(status)) {
		return icu_uerror_to_as_err(status);
	}

	*out_len = (uint32_t)result_len;
	return AS_OK;
}

// Thread-local cached UStringSearch. Created once per thread on first use.
// The UStringSearch owns a UCollator (root locale, NFC normalization,
// UCOL_TERTIARY strength) and lazily creates an internal UBreakIterator.
// Reusing the object across calls avoids the expensive ucol_open, ubrk_open,
// and associated locale/data loading that otherwise dominates each search.
static __thread UCollator* tl_coll;
static __thread UStringSearch* tl_search;

static void
icu_search_exit(void* udata)
{
	(void)udata;

	usearch_close(tl_search);
	tl_search = NULL;

	ucol_close(tl_coll);
	tl_coll = NULL;
}

// Assumes from is ASCII (all bytes < 0x80).
static bool
is_ascii_upper(const uint8_t* from, uint32_t sz)
{
	for (uint32_t i = 0; i < sz; i++) {
		if (from[i] < 'A' || from[i] > 'Z') {
			return false;
		}
	}
	return true;
}

// Assumes from is ASCII (all bytes < 0x80).
static bool
is_ascii_lower(const uint8_t* from, uint32_t sz)
{
	for (uint32_t i = 0; i < sz; i++) {
		if (from[i] < 'a' || from[i] > 'z') {
			return false;
		}
	}
	return true;
}

static bool
is_utf8_lower(const uint8_t* from, uint32_t sz)
{
	UChar32 c;
	int32_t i = 0;
	while (i < (int32_t)sz) {
		U8_NEXT(from, i, (int32_t)sz, c);
		if (c < 0 || ! u_islower(c))
			return false;
	}
	return true;
}

static bool
is_utf8_upper(const uint8_t* from, uint32_t sz)
{
	UChar32 c;
	int32_t i = 0;
	while (i < (int32_t)sz) {
		U8_NEXT(from, i, (int32_t)sz, c);
		if (c < 0 || ! u_isupper(c))
			return false;
	}
	return true;
}

// Returns true if the UTF-8 string is already in NFC (Canonical Decomposition
// followed by Canonical Composition) form. Used as a fast pre-check before
// normalize_nfc to avoid a full normalization pass on already-normalized input.
static bool
is_nfc_utf8(const uint8_t* data, uint32_t sz)
{
	UErrorCode status = U_ZERO_ERROR;
	const UNormalizer2* nfc = unorm2_getNFCInstance(&status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"is_nfc_utf8 - "
				"error %u - getNFCInstance failed with ICU status code %d",
				AS_ERR_OP_NOT_APPLICABLE, status);
		// Conservative: cannot determine NFC; callers skip the fast path. Same
		// singleton-init failures as string_modify_op_normalize_nfc (not UTF-8).
		return false;
	}

	// unorm2_isNormalized requires UTF-16 input; convert with a stack buffer
	// and fall back to heap only if the string is large.
	uint16_t stack_buf_sz = 512;
	UChar stack_buf[stack_buf_sz];
	UChar* u16;
	int32_t u16_len;
	DEFER_ATTR_FREE void* heap = NULL;

	if (utf8_to_u16(data, sz, stack_buf, stack_buf_sz, &u16, &u16_len, &heap) !=
			AS_OK) {
		return false;
	}

	// unorm2_isNormalized can set status to an error even when returning FALSE
	// (e.g. on malformed input), so AND with U_SUCCESS to avoid a false positive.
	UBool result = unorm2_isNormalized(nfc, u16, u16_len, &status);

	return result && U_SUCCESS(status);
}

// Byte-exact search using memmem. Returns the code-point index of the
// occurrence-th match (1-based forward, negative = backward), or -1 if not
// found. Only correct when both haystack and needle have identical byte
// representations for equivalent strings (i.e. both ASCII or both NFC).
static int64_t
memmem_find(const uint8_t* haystack, uint32_t h_sz, const uint8_t* needle,
		uint32_t n_sz, int64_t occurrence)
{
	cf_assert(n_sz != 0, AS_PARTICLE,
			"memmem_find: empty needle (caller invariant violated)");

	if (n_sz > h_sz || occurrence == 0) {
		return -1;
	}

	if (occurrence > 0) {
		const uint8_t* p = haystack;
		uint32_t remaining = h_sz;
		int64_t count = 0;

		while (remaining >= n_sz) {
			const uint8_t* found = memmem(p, remaining, needle, n_sz);

			if (found == NULL) {
				return -1;
			}

			if (++count == occurrence) {
				// Haystack is valid UTF-8 on this path (ASCII or is_nfc_utf8);
				// prefix [haystack, found) is well-formed, so count cannot be -1.
				return (int64_t)utf8_string_length(haystack,
						(uint32_t)(found - haystack));
			}

			uint32_t advance = (uint32_t)(found - p) + n_sz;
			p = found + n_sz;
			remaining -= advance;
		}

		return -1;
	}

	// Mirror ICU's backward iterator: after a match, resume before the
	// matched span so self-overlaps are skipped in the reverse direction.
	int64_t target = -occurrence;
	int64_t count = 0;
	uint32_t pos = h_sz - n_sz;

	while (true) {
		if (memcmp(haystack + pos, needle, n_sz) == 0) {
			if (++count == target) {
				// Same invariant as forward branch: valid UTF-8 haystack/needle.
				return (int64_t)utf8_string_length(haystack, pos);
			}

			if (pos < n_sz) {
				break;
			}

			pos -= n_sz;
			continue;
		}

		if (pos == 0) {
			break;
		}

		pos--;
	}

	return -1;
}

// Return a thread-local UStringSearch configured for canonical (NFC-aware,
// case- and accent-sensitive) substring matching.
//
// On the first call from a given thread the function allocates a root-locale
// UCollator with full normalization enabled at TERTIARY strength, then wraps
// it in a UStringSearch.  On subsequent calls the existing objects are reused
// by swapping in the new pattern and text, avoiding repeated allocation.
//
// @param haystack  UTF-16 text to search within.
// @param h_len     Length of @p haystack in UChars, or -1 if NUL-terminated.
// @param needle    UTF-16 pattern to search for.
// @param n_len     Length of @p needle in UChars, or -1 if NUL-terminated.
// @return          The thread-local UStringSearch ready to iterate, or NULL on
//                  any ICU error.
static UStringSearch*
get_canon_search(const UChar* haystack, int32_t h_len, const UChar* needle,
		int32_t n_len)
{
	UErrorCode status = U_ZERO_ERROR;

	if (tl_search != NULL) {
		// Reuse the existing search object: swap pattern first, then text.
		usearch_setPattern(tl_search, needle, n_len, &status);

		if (U_FAILURE(status)) {
			return NULL;
		}

		usearch_setText(tl_search, haystack, h_len, &status);

		if (U_FAILURE(status)) {
			return NULL;
		}

		return tl_search;
	}

	// First call on this thread: build the collator and search from scratch.

	// "root" locale gives language-neutral Unicode CLDR collation rules.
	tl_coll = ucol_open("root", &status);

	if (U_FAILURE(status)) {
		return NULL;
	}

	// Normalize input so that canonically equivalent sequences compare equal,
	// and use TERTIARY strength to distinguish base, case, and accents.
	ucol_setAttribute(tl_coll, UCOL_NORMALIZATION_MODE, UCOL_ON, &status);
	ucol_setStrength(tl_coll, UCOL_TERTIARY);

	if (U_FAILURE(status)) {
		ucol_close(tl_coll);
		tl_coll = NULL;
		return NULL;
	}

	tl_search = usearch_openFromCollator(needle, n_len, haystack, h_len,
			tl_coll, NULL, &status);

	if (U_FAILURE(status)) {
		ucol_close(tl_coll);
		tl_coll = NULL;
		tl_search = NULL;
		return NULL;
	}

	cf_thread_add_exit(icu_search_exit, NULL);

	return tl_search;
}

//==========================================================
// Local helpers - modify op implementations.
// called by string_modify as state->def->fn.modify(..)
static int
string_modify_op_concatenate(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	memcpy(to, from, old_sz); // start with the existing string unmodified
	*new_sz = old_sz;

	msgpack_in mp = {
		.buf = op->buf,
		.buf_sz = op->buf_sz,
	};

	// make sure we got a msgpack list
	uint32_t list_sz;
	if (! msgpack_get_list_ele_count(&mp, &list_sz)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_concatenate - error %u invalid msgpack list",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}
	if (list_sz == 0)
		return AS_OK;

	// iterate over buf (msgpack list of as_string particles)
	for (uint32_t i = 0; i < list_sz; ++i) {
		// make sure each element is a string
		msgpack_type type = msgpack_peek_type(&mp);
		if (type != MSGPACK_TYPE_STRING) {
			cf_ticker_warning(AS_PARTICLE,
					"string_modify_op_concatenate - error %u invalid msgpack "
					"string at element %u",
					AS_ERR_PARAMETER, i);
			*new_sz = old_sz;
			return -AS_ERR_PARAMETER;
		}
		uint32_t string_sz;
		const uint8_t* string_arg = msgpack_get_bin(&mp, &string_sz);

		if (string_arg == NULL || string_sz == 0) {
			cf_ticker_warning(AS_PARTICLE,
					"string_modify_op_concatenate - error %u empty or missing "
					"string at element %u",
					AS_ERR_PARAMETER, i);
			*new_sz = old_sz;
			return -AS_ERR_PARAMETER;
		}

		string_arg++; // advance past Aerospike string type byte (0x03)
		string_sz--;

		if (! cf_str_is_valid_utf8(string_arg, string_sz)) {
			cf_ticker_warning(AS_PARTICLE,
					"string_modify_op_concatenate - error %u invalid UTF-8 "
					"in element %u",
					AS_ERR_PARAMETER, i);
			*new_sz = old_sz;
			return -AS_ERR_PARAMETER;
		}

		memcpy(to + *new_sz, string_arg, string_sz);
		*new_sz += string_sz;
	}

	return AS_OK;
}

static int
string_modify_op_append(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	memcpy(to, from, old_sz);
	memcpy(to + old_sz, op->buf, op->buf_sz);
	*new_sz = old_sz + op->buf_sz;
	return AS_OK;
}

static int
string_modify_op_prepend(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	string_op insert_op = *op;

	insert_op.int_arg1 = 0;
	return string_modify_op_insert(&insert_op, to, from, old_sz, new_sz);
}

static const UCaseMap*
get_root_casemap(void)
{
	static UCaseMap* csm = NULL;

	if (csm != NULL) {
		return csm;
	}

	UErrorCode status = U_ZERO_ERROR;
	UCaseMap* new_csm = ucasemap_open("root", U_FOLD_CASE_DEFAULT, &status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"get_root_casemap - ucasemap_open failed: %d", status);
		return NULL;
	}

	csm = new_csm;
	return csm;
}

static int
string_modify_op_normalize_nfc(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	if (is_ascii(from, old_sz)) {
		memcpy(to, from, old_sz);
		*new_sz = old_sz;
		return AS_OK;
	}

	UErrorCode status = U_ZERO_ERROR;

	const UNormalizer2* nfc = unorm2_getNFCInstance(&status);
	if (U_FAILURE(status)) {
		// Singleton init (hardcoded NFC tables in this ICU build): OOM or rare
		// init failures — not UTF-8/UTF-16 conversion (no INVALID_CHAR / overflow).
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_normalize_nfc - "
				"getNFCInstance failed: %d",
				status);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	UChar src_stack[STRING_OP_STACK_BUF_SZ];
	UChar dst_stack[STRING_OP_STACK_BUF_SZ];
	UChar* src_u16;
	UChar* dst_u16 = dst_stack;
	int32_t src_len;
	int32_t dst_cap = STRING_OP_STACK_BUF_SZ;
	// DEFER_FREE(x) expands to a *new* variable initialized to x's current
	// value (NULL here). When utf8_to_u16 later writes through &src_heap, or
	// when we assign dst_heap below, only the original variables change — the
	// defer copies stay NULL and cf_free(NULL) is a no-op at scope exit.
	// DEFER_ATTR_FREE puts the cleanup attribute on the variable itself, so
	// cf_free sees whatever value it holds when the scope actually exits.
	DEFER_ATTR_FREE void* src_heap = NULL;
	DEFER_ATTR_FREE void* dst_heap = NULL;

	int convert_rc = utf8_to_u16(from, old_sz, src_stack, dst_cap, &src_u16,
			&src_len, &src_heap);

	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_normalize_nfc - "
				"UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	int32_t dst_len =
			unorm2_normalize(nfc, src_u16, src_len, dst_u16, dst_cap, &status);

	if (status == U_BUFFER_OVERFLOW_ERROR) {
		status = U_ZERO_ERROR;
		dst_u16 = cf_malloc((dst_len + 1) * sizeof(UChar));
		dst_heap = dst_u16;
		unorm2_normalize(nfc, src_u16, src_len, dst_u16, dst_len + 1, &status);
	}

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_normalize_nfc - "
				"error %u - unorm2_normalize failed with ICU status code %d",
				AS_ERR_OP_NOT_APPLICABLE, status);
		switch (status) {
		case U_INVALID_CHAR_FOUND:
			return -AS_ERR_INVALID_ENCODING;
		case U_ILLEGAL_ARGUMENT_ERROR:
			return -AS_ERR_PARAMETER;
		case U_BUFFER_OVERFLOW_ERROR:
			// Impossible: we just allocated dst_len + 1 UChars as reported
			// by ICU's own pre-flight call. Overflow here means ICU lied
			// about the required size or our sizing logic is wrong.
			cf_crash(AS_PARTICLE,
					"normalize_nfc: unorm2_normalize overflow after "
					"exact resize to %d UChars",
					dst_len + 1);
		default:
			return -AS_ERR_OP_NOT_APPLICABLE;
		}
	}

	convert_rc = u16_to_utf8(to, old_sz * 3, new_sz, dst_u16, dst_len);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_normalize_nfc - "
				"error %u - UTF-16 to UTF-8 failed",
				convert_rc);
		return convert_rc;
	}

	return AS_OK;
}

static int
string_modify_op_case_fold(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	if (is_ascii(from, old_sz)) {
		for (uint32_t i = 0; i < old_sz; i++) {
			uint8_t c = from[i];
			to[i] = (c >= 'A' && c <= 'Z') ? c + 32 : c;
		}
		*new_sz = old_sz;
		return AS_OK;
	}

	const UCaseMap* csm = get_root_casemap();

	if (csm == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_case_fold - "
				"error %u - get_root_casemap failed",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	UErrorCode status = U_ZERO_ERROR;
	int32_t dst_len = ucasemap_utf8FoldCase(csm, (char*)to, old_sz * 3,
			(const char*)from, old_sz, &status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_case_fold - "
				"error %u - ucasemap_utf8FoldCase failed with ICU status code %d",
				AS_ERR_OP_NOT_APPLICABLE, status);
		switch (status) {
		case U_INVALID_CHAR_FOUND:
			return -AS_ERR_INVALID_ENCODING;
		case U_ILLEGAL_ARGUMENT_ERROR:
			return -AS_ERR_PARAMETER;
		case U_BUFFER_OVERFLOW_ERROR:
			// Impossible: old_sz * 3 is the worst-case UTF-8 growth for
			// Unicode case folding. Overflow here means our sizing bound
			// is wrong.
			cf_crash(AS_PARTICLE,
					"case_fold: ucasemap_utf8FoldCase overflow with "
					"capacity %u for input of %u bytes",
					old_sz * 3, old_sz);
		default:
			return -AS_ERR_OP_NOT_APPLICABLE;
		}
	}
	*new_sz = (uint32_t)dst_len;
	return AS_OK;
}

static int
string_modify_op_repeat(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	uint8_t* write_ptr = to;
	for (int64_t n = 0, end = op->int_arg1; n < end; ++n) {
		memcpy(write_ptr, from, old_sz);
		write_ptr += old_sz;
	}
	*new_sz = (uint32_t)(write_ptr - to);

	return AS_OK;
}

static int
string_modify_op_snip(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	int32_t prefix_sz;
	int32_t snip_sz;

	if (is_ascii(from, old_sz)) {
		prefix_sz = (int32_t)(op->int_arg1 < (int64_t)old_sz ? op->int_arg1
															 : (int64_t)old_sz);
		int64_t snip_cp = op->int_arg2 - op->int_arg1;

		if (snip_cp <= 0) {
			snip_sz = 0;
		}
		else {
			snip_sz = (int32_t)(snip_cp < (int64_t)(old_sz - prefix_sz)
							? snip_cp
							: (int64_t)(old_sz - prefix_sz));
		}
	}
	else {
		prefix_sz = 0;
		for (int64_t i = 0; i < op->int_arg1; i++) {
			UChar32 c;
			U8_NEXT(from, prefix_sz, (int32_t)old_sz, c);
		}
		const uint8_t* cur = from + prefix_sz;
		int32_t remaining_sz = (int32_t)old_sz - prefix_sz;
		snip_sz = 0;
		for (int64_t i = 0; i < op->int_arg2 - op->int_arg1; i++) {
			UChar32 c;
			U8_NEXT(cur, snip_sz, remaining_sz, c);
		}
	}

	memcpy(to, from, (size_t)prefix_sz);
	uint32_t suffix_sz = old_sz - (uint32_t)prefix_sz - (uint32_t)snip_sz;
	memcpy(to + prefix_sz, from + prefix_sz + snip_sz, suffix_sz);
	*new_sz = prefix_sz + suffix_sz;
	return AS_OK;
}

static int
string_modify_op_pad_start(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	// Invariant: `from` is valid UTF-8 — string_modify rejects the op otherwise.
	// utf8_string_length(from, old_sz) is therefore non-negative.
	size_t target_len = (size_t)op->int_arg1;
	size_t current_len = (size_t)utf8_string_length(from, old_sz);

	if (current_len >= target_len) {
		*new_sz = old_sz;
		memcpy(to, from, old_sz);
		return AS_OK;
	}

	int64_t pad_cp_len = utf8_string_length(op->buf, op->buf_sz);

	if (pad_cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_pad_start - "
				"error %u - invalid UTF-8 in pad pattern",
				AS_ERR_INVALID_ENCODING);
		return -AS_ERR_INVALID_ENCODING;
	}

	size_t pad_len = (size_t)pad_cp_len;
	size_t extended_pad_len = target_len - current_len;
	size_t reps = extended_pad_len / pad_len;
	size_t remainder_len = extended_pad_len % pad_len;

	uint8_t* dest = to;

	for (size_t i = 0; i < reps; i++) {
		memcpy(dest, op->buf, op->buf_sz);
		dest += op->buf_sz;
	}

	uint32_t remainder_bytes;
	if (is_ascii(op->buf, op->buf_sz)) {
		remainder_bytes = (uint32_t)remainder_len;
	}
	else {
		int32_t rb = 0;
		for (size_t i = 0; i < remainder_len; i++) {
			UChar32 c;
			U8_NEXT(op->buf, rb, (int32_t)op->buf_sz, c);
		}
		remainder_bytes = (uint32_t)rb;
	}
	memcpy(dest, op->buf, remainder_bytes);
	dest += remainder_bytes;

	memcpy(dest, from, old_sz);
	dest += old_sz;

	*new_sz = (uint32_t)(dest - to);
	return AS_OK;
}

static int
string_modify_op_pad_end(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	memcpy(to, from, old_sz);

	// Invariant: `from` is valid UTF-8 — string_modify rejects the op otherwise.
	size_t target_len = (size_t)op->int_arg1;
	size_t current_len = (size_t)utf8_string_length(from, old_sz);

	if (current_len >= target_len) {
		*new_sz = old_sz;
		return AS_OK;
	}

	int64_t pad_cp_len = utf8_string_length(op->buf, op->buf_sz);

	if (pad_cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_pad_end - invalid UTF-8 in pad pattern");
		return -AS_ERR_INVALID_ENCODING;
	}

	size_t pad_len = (size_t)pad_cp_len;
	size_t extended_pad_len = target_len - current_len;
	size_t reps = extended_pad_len / pad_len;
	size_t remainder_len = extended_pad_len % pad_len;

	uint8_t* dest = to + old_sz;

	for (size_t i = 0; i < reps; i++) {
		memcpy(dest, op->buf, op->buf_sz);
		dest += op->buf_sz;
	}

	uint32_t remainder_bytes;
	if (is_ascii(op->buf, op->buf_sz)) {
		remainder_bytes = (uint32_t)remainder_len;
	}
	else {
		int32_t rb = 0;
		for (size_t i = 0; i < remainder_len; i++) {
			UChar32 c;
			U8_NEXT(op->buf, rb, (int32_t)op->buf_sz, c);
		}
		remainder_bytes = (uint32_t)rb;
	}
	memcpy(dest, op->buf, remainder_bytes);
	dest += remainder_bytes;

	*new_sz = (uint32_t)(dest - to);
	return AS_OK;
}

static int
string_modify_op_trim_start(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	size_t i = find_first_non_ws(from, old_sz);
	memcpy(to, from + i, old_sz - i);
	*new_sz = old_sz - i;
	return AS_OK;
}

static int
string_modify_op_trim_end(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	size_t i = find_last_non_ws_end(from, old_sz);
	memcpy(to, from, i);
	*new_sz = i;
	return AS_OK;
}

static int
string_modify_op_trim_both(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	size_t i = find_first_non_ws(from, old_sz);
	size_t j = find_last_non_ws_end(from, old_sz);

	if (i >= j) {
		*new_sz = 0;
		return AS_OK;
	}

	memcpy(to, from + i, j - i);
	*new_sz = j - i;
	return AS_OK;
}

// How many bytes are needed to encode the next `n` UTF-8 code points
// of the string `from`?
static uint32_t
utf8_byte_len_of_next_n_code_points(const uint8_t* from, uint32_t sz, uint32_t n)
{
	if (is_ascii(from, sz)) {
		return n < sz ? n : sz;
	}

	int32_t pos = 0;

	for (uint32_t cp = 0; cp < n && pos < (int32_t)sz; cp++) {
		UChar32 c;
		U8_NEXT(from, pos, (int32_t)sz, c);
	}

	return (uint32_t)pos;
}

static int
string_modify_op_overwrite(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	// op->int_arg1 is the offset (code point index)
	// op->buf is the replacement string
	// op->buf_sz is the replacement string length (bytes)

	int64_t replacement_cp_len = utf8_string_length(op->buf, op->buf_sz);

	if (replacement_cp_len < 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_overwrite - invalid UTF-8 in replacement");
		return -AS_ERR_INVALID_ENCODING;
	}

	// 1. Walk int_arg1 code points from `from` to find prefix_byte_len
	uint32_t prefix_byte_len =
			utf8_byte_len_of_next_n_code_points(from, old_sz, op->int_arg1);
	// 2. Copy prefix: memcpy(to, from, prefix_byte_len)
	memcpy(to, from, prefix_byte_len);
	// 3. Copy replacement: memcpy(to + prefix_byte_len, op->buf, op->buf_sz)
	memcpy(to + prefix_byte_len, op->buf, op->buf_sz);
	// 4. Find skip_byte_len: number of remaining bytes replacement overwrites
	uint32_t skip_byte_len =
			utf8_byte_len_of_next_n_code_points(from + prefix_byte_len,
					old_sz - prefix_byte_len, (uint32_t)replacement_cp_len);
	memcpy(to + prefix_byte_len + op->buf_sz,
			from + prefix_byte_len + skip_byte_len,
			old_sz - prefix_byte_len - skip_byte_len);
	// 5. Set new_sz
	*new_sz = prefix_byte_len + op->buf_sz +
			(old_sz - prefix_byte_len - skip_byte_len);
	return AS_OK;
}

__attribute__((noinline)) static int
string_modify_op_replace_K_icu(uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz, uint32_t k, const uint8_t* needle,
		uint32_t needle_sz, const uint8_t* replacement, uint32_t replacement_sz)
{
	UChar haystack_stack[STRING_OP_STACK_BUF_SZ];
	UChar needle_stack[STRING_OP_STACK_BUF_SZ];
	UChar replacement_stack[STRING_OP_STACK_BUF_SZ];
	UChar* haystack_u16;
	UChar* needle_u16;
	UChar* replacement_u16;
	int32_t h_len, n_len, r_len;
	// DEFER_FREE(x) captured x's value (NULL) into a hidden variable at
	// declaration time. When utf8_to_u16 later wrote through &haystack_heap
	// etc., the hidden variables stayed NULL — nothing was ever freed.
	// DEFER_ATTR_FREE on the variables themselves ensures cleanup sees the
	// live pointer value at scope exit.
	DEFER_ATTR_FREE void* haystack_heap = NULL;
	DEFER_ATTR_FREE void* needle_heap = NULL;
	DEFER_ATTR_FREE void* replacement_heap = NULL;

	int convert_rc = utf8_to_u16(from, old_sz, haystack_stack,
			STRING_OP_STACK_BUF_SZ, &haystack_u16, &h_len, &haystack_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %u - haystack UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	convert_rc = utf8_to_u16(needle, needle_sz, needle_stack,
			STRING_OP_STACK_BUF_SZ, &needle_u16, &n_len, &needle_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"needle UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	convert_rc = utf8_to_u16(replacement, replacement_sz, replacement_stack,
			STRING_OP_STACK_BUF_SZ, &replacement_u16, &r_len, &replacement_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"replacement UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	UStringSearch* search =
			get_canon_search(haystack_u16, h_len, needle_u16, n_len);

	if (search == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %u - get_canon_search failed",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	int32_t result_cap = old_sz * 2;
	// DEFER_FREE(result_u16) captured the initial cf_malloc pointer by value.
	// cf_realloc below may return a different address (freeing the original
	// internally), leaving the captured copy stale — double-free at scope
	// exit. DEFER_ATTR_FREE on the variable itself tracks the live pointer
	// through reallocs, so cleanup always frees the current buffer.
	DEFER_ATTR_FREE UChar* result_u16 = cf_malloc(result_cap * sizeof(UChar));
	int32_t result_len = 0;

	UErrorCode status = U_ZERO_ERROR;
	int32_t copy_from = 0;
	uint32_t matches = 0;
	int32_t match_pos = usearch_first(search, &status);

	while (match_pos != USEARCH_DONE && U_SUCCESS(status) && matches < k) {
		int32_t match_len = usearch_getMatchedLength(search);

		int32_t needed = result_len + (match_pos - copy_from) + r_len;
		if (needed > result_cap) {
			result_cap = needed * 2;
			result_u16 = cf_realloc(result_u16, result_cap * sizeof(UChar));
		}

		if (match_pos > copy_from) {
			memcpy(result_u16 + result_len, haystack_u16 + copy_from,
					(match_pos - copy_from) * sizeof(UChar));
			result_len += (match_pos - copy_from);
		}

		memcpy(result_u16 + result_len, replacement_u16, r_len * sizeof(UChar));
		result_len += r_len;

		copy_from = match_pos + match_len;
		matches++;
		match_pos = usearch_next(search, &status);
	}

	if (copy_from < h_len) {
		int32_t remaining = h_len - copy_from;
		int32_t needed = result_len + remaining;
		if (needed > result_cap) {
			result_cap = needed;
			result_u16 = cf_realloc(result_u16, result_cap * sizeof(UChar));
		}
		memcpy(result_u16 + result_len, haystack_u16 + copy_from,
				remaining * sizeof(UChar));
		result_len += remaining;
	}

	convert_rc = u16_to_utf8(to, (uint32_t)result_len * 3, new_sz, result_u16,
			result_len);
	if (convert_rc != AS_OK) {
		// u16_to_utf8: U_INVALID_CHAR_FOUND => ill-formed UTF-16 in result_u16
		// (mapped to AS_ERR_INVALID_ENCODING); other ICU codes => PARAMETER / …
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %d UTF-16 to UTF-8 failed",
				convert_rc);
		return convert_rc;
	}

	return AS_OK;
}

static int
string_modify_op_replace_K(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz, uint32_t k)
{
	msgpack_in mp = {
		.buf = op->buf,
		.buf_sz = op->buf_sz,
	};

	uint32_t argc;
	msgpack_get_list_ele_count(&mp, &argc);
	(void)argc; // called to advance msgpack offset, value unused

	msgpack_type needle_type = msgpack_peek_type(&mp);
	if (needle_type != MSGPACK_TYPE_STRING) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %u - needle is not a string",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	uint32_t needle_sz;
	const uint8_t* needle = msgpack_get_bin(&mp, &needle_sz);
	if (needle_sz == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %u - needle cannot be empty",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}
	needle++; // Advance past Aerospike string type byte (0x03)
	needle_sz--;

	msgpack_type replacement_type = msgpack_peek_type(&mp);
	if (replacement_type != MSGPACK_TYPE_STRING) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_replace_K - "
				"error %u - replacement is not a string",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	uint32_t replacement_sz;
	const uint8_t* replacement = msgpack_get_bin(&mp, &replacement_sz);
	replacement++; // Advance past Aerospike string type byte (0x03)
	replacement_sz--;

	if (needle_sz > 0 &&
			((is_ascii(from, old_sz) && is_ascii(needle, needle_sz)) ||
					(is_nfc_utf8(from, old_sz) &&
							is_nfc_utf8(needle, needle_sz)))) {
		uint8_t* out = to;
		const uint8_t* p = from;
		uint32_t remaining = old_sz;
		uint32_t matches = 0;

		while (remaining >= needle_sz && matches < k) {
			const uint8_t* found = memmem(p, remaining, needle, needle_sz);

			if (found == NULL) {
				break;
			}

			uint32_t prefix = (uint32_t)(found - p);
			memcpy(out, p, prefix);
			out += prefix;
			memcpy(out, replacement, replacement_sz);
			out += replacement_sz;
			p = found + needle_sz;
			remaining -= prefix + needle_sz;
			matches++;
		}

		memcpy(out, p, remaining);
		out += remaining;
		*new_sz = (uint32_t)(out - to);
		return AS_OK;
	}

	return string_modify_op_replace_K_icu(to, from, old_sz, new_sz, k, needle,
			needle_sz, replacement, replacement_sz);
}

static int
string_modify_op_replace(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	return string_modify_op_replace_K(op, to, from, old_sz, new_sz, 1);
}

static int
string_modify_op_replace_all(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	return string_modify_op_replace_K(op, to, from, old_sz, new_sz,
			STRING_REPLACE_ALL_MAX);
}

// Inserts op->buf at code-point position op->int_arg1 in the source string.
// Content before and after the insertion point is preserved unchanged.
static int
string_modify_op_insert(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	// Walk int_arg1 code points from `from` to find prefix_byte_len
	uint8_t* write_ptr = to;
	uint32_t prefix_byte_len =
			utf8_byte_len_of_next_n_code_points(from, old_sz, op->int_arg1);
	// Copy prefix
	memcpy(write_ptr, from, prefix_byte_len);
	write_ptr += prefix_byte_len;
	// Copy new_part
	memcpy(write_ptr, op->buf, op->buf_sz);
	write_ptr += op->buf_sz;
	// copy remaining part
	memcpy(write_ptr, from + prefix_byte_len, old_sz - prefix_byte_len);
	write_ptr += old_sz - prefix_byte_len;
	// set new_sz
	*new_sz = write_ptr - to;
	return AS_OK;
}

static int
string_modify_op_upper(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	if (is_ascii(from, old_sz)) {
		for (uint32_t i = 0; i < old_sz; i++) {
			uint8_t c = from[i];
			to[i] = (c >= 'a' && c <= 'z') ? c - ' ' : c;
		}
		*new_sz = old_sz;
		return AS_OK;
	}

	UErrorCode status = U_ZERO_ERROR;

	UChar src_stack[STRING_OP_STACK_BUF_SZ];
	UChar dst_stack[STRING_OP_STACK_BUF_SZ];
	UChar* src_u16;
	UChar* dst_u16 = dst_stack;
	int32_t src_len;
	int32_t dst_cap = STRING_OP_STACK_BUF_SZ;
	DEFER_ATTR_FREE void* src_heap = NULL;
	DEFER_ATTR_FREE void* dst_heap = NULL;

	int convert_rc = utf8_to_u16(from, old_sz, src_stack,
			STRING_OP_STACK_BUF_SZ, &src_u16, &src_len, &src_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_upper - "
				"error %u - UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	int32_t dst_len =
			u_strToUpper(dst_u16, dst_cap, src_u16, src_len, "root", &status);

	if (status == U_BUFFER_OVERFLOW_ERROR) {
		status = U_ZERO_ERROR;
		dst_u16 = cf_malloc((dst_len + 1) * sizeof(UChar));
		dst_heap = dst_u16;
		u_strToUpper(dst_u16, dst_len + 1, src_u16, src_len, "root", &status);
	}

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_upper - "
				"u_strToUpper failed with ICU status code %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	convert_rc = u16_to_utf8(to, old_sz * 3, new_sz, dst_u16, dst_len);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_upper - "
				"error %u - UTF-16 to UTF-8 failed",
				convert_rc);
		return convert_rc;
	}

	return AS_OK;
}

static int
string_modify_op_lower(const string_op* op, uint8_t* to, const uint8_t* from,
		uint32_t old_sz, uint32_t* new_sz)
{
	(void)op;

	if (is_ascii(from, old_sz)) {
		for (uint32_t i = 0; i < old_sz; i++) {
			uint8_t c = from[i];
			to[i] = (c >= 'A' && c <= 'Z') ? c + ' ' : c;
		}
		*new_sz = old_sz;
		return AS_OK;
	}

	UErrorCode status = U_ZERO_ERROR;

	UChar src_stack[STRING_OP_STACK_BUF_SZ];
	UChar dst_stack[STRING_OP_STACK_BUF_SZ];
	UChar* src_u16;
	UChar* dst_u16 = dst_stack;
	int32_t src_len;
	int32_t dst_cap = STRING_OP_STACK_BUF_SZ;
	DEFER_ATTR_FREE void* src_heap = NULL;
	DEFER_ATTR_FREE void* dst_heap = NULL;

	int convert_rc = utf8_to_u16(from, old_sz, src_stack,
			STRING_OP_STACK_BUF_SZ, &src_u16, &src_len, &src_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_lower - "
				"error %u - UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	int32_t dst_len =
			u_strToLower(dst_u16, dst_cap, src_u16, src_len, "root", &status);

	if (status == U_BUFFER_OVERFLOW_ERROR) {
		status = U_ZERO_ERROR;
		dst_u16 = cf_malloc((dst_len + 1) * sizeof(UChar));
		dst_heap = dst_u16;
		u_strToLower(dst_u16, dst_len + 1, src_u16, src_len, "root", &status);
	}

	if (U_FAILURE(status)) {
		int as_err = icu_uerror_to_as_err(status);
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_lower - "
				"error %u - u_strToLower failed with ICU status code %d",
				as_err, status);
		return as_err;
	}

	convert_rc = u16_to_utf8(to, old_sz * 3, new_sz, dst_u16, dst_len);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_lower - "
				"error %u - UTF-16 to UTF-8 failed",
				convert_rc);
		return convert_rc;
	}

	return AS_OK;
}

//==========================================================
// Local helpers - read op implementations.
// called by string_read as state->def->fn.read(..)

// Returns the number of Unicode code points in the string as an integer result.
// For ASCII strings this equals the byte length; for multi-byte UTF-8 it may be less.
static int
string_read_op_strlen(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	int64_t length = utf8_string_length(from, sz);

	if (length < 0) {
		cf_ticker_warning(AS_PARTICLE, "invalid UTF-8 detected in string data");
		return -AS_ERR_INVALID_ENCODING;
	}

	rb->particle = (as_particle*)(uint64_t)length;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return AS_OK;
}

/// @brief Read the substring from the string.
/// @param `op->int_arg1` is the starting index (included)
/// @param `op->int_arg2` is end index (excluded).
///
/// Note - `op->int_arg1` and `op->int_arg2` must be normalized prior to call.
static int
string_read_op_substr(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	uint32_t from_idx = (uint32_t)op->int_arg1, to_idx = (uint32_t)op->int_arg2;
	const uint8_t* start;
	const uint8_t* end;

	if (is_ascii(from, sz)) {
		uint32_t s = from_idx < sz ? from_idx : sz;
		uint32_t e = to_idx < sz ? to_idx : sz;
		start = from + s;
		end = e > s ? from + e : start;
	}
	else {
		int32_t cur = 0;
		for (uint32_t i = 0; i < from_idx; i++) {
			UChar32 c;
			U8_NEXT(from, cur, (int32_t)sz, c);
		}
		start = from + cur;

		for (uint32_t i = from_idx; i < to_idx; i++) {
			UChar32 c;
			U8_NEXT(from, cur, (int32_t)sz, c);
		}
		end = from + cur;
	}

	size_t byte_len = end - start;
	string_mem* answer = cf_malloc(sizeof(string_mem) + byte_len);

	memcpy(answer->data, start, byte_len);
	answer->sz = (uint32_t)byte_len;
	answer->type = AS_PARTICLE_TYPE_STRING;

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_STRING);

	return AS_OK;
}

static int
string_read_op_char_at(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	string_op substr_op = *op;
	substr_op.int_arg2 = op->int_arg1 + 1;
	return string_read_op_substr(&substr_op, from, sz, rb);
}

// Canonical-equivalence find using ICU usearch.
// Handles NFC/NFD equivalence, case-sensitive, returns code point index.
// occurrence > 0: search forward (1=first, 2=second, ...)
// occurrence < 0: search backward (-1=last, -2=second to last, ...)
// Returns AS_OK on success (with -1 in result if not found), error code on ICU failure.
static int
string_read_op_find(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	if (op->buf_sz == 0) {
		rb->particle = (as_particle*)0;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
		return AS_OK;
	}

	if (op->buf_sz > sz) {
		rb->particle = (as_particle*)(uint64_t)-1;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
		return AS_OK;
	}

	int64_t cp_index = -1;
	int64_t occurrence_arg = op->int_arg1;

	if ((is_ascii(from, sz) && is_ascii(op->buf, op->buf_sz)) ||
			(is_nfc_utf8(from, sz) && is_nfc_utf8(op->buf, op->buf_sz))) {
		cp_index = memmem_find(from, sz, op->buf, op->buf_sz, occurrence_arg);
		rb->particle = (as_particle*)(uint64_t)cp_index;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
		return AS_OK;
	}

	UChar haystack_stack[STRING_OP_STACK_BUF_SZ];
	UChar needle_stack[STRING_OP_STACK_BUF_SZ];
	UChar* haystack_u16;
	UChar* needle_u16;
	int32_t h_len, n_len;
	// DEFER_FREE(x) captured NULL by value — later writes through
	// &haystack_heap/&needle_heap were invisible to cleanup.
	// DEFER_ATTR_FREE tracks the live value.
	DEFER_ATTR_FREE void* haystack_heap = NULL;
	DEFER_ATTR_FREE void* needle_heap = NULL;

	int convert_rc = utf8_to_u16(from, sz, haystack_stack,
			STRING_OP_STACK_BUF_SZ, &haystack_u16, &h_len, &haystack_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_find - "
				"error %u - haystack UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	convert_rc = utf8_to_u16(op->buf, op->buf_sz, needle_stack,
			STRING_OP_STACK_BUF_SZ, &needle_u16, &n_len, &needle_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_find - "
				"error %u - needle UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	UStringSearch* search =
			get_canon_search(haystack_u16, h_len, needle_u16, n_len);

	if (search == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_find - "
				"error %u - get_canon_search failed",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	UErrorCode status = U_ZERO_ERROR;
	uint32_t count = 0;
	int32_t u16_pos;

	if (occurrence_arg > 0) {
		uint32_t target = (uint32_t)occurrence_arg;
		u16_pos = usearch_first(search, &status);

		while (u16_pos != USEARCH_DONE && U_SUCCESS(status)) {
			if (++count == target) {
				cp_index = (int64_t)u_countChar32(haystack_u16, u16_pos);
				break;
			}
			u16_pos = usearch_next(search, &status);
		}
	}
	else {
		uint32_t target = (uint32_t)(-occurrence_arg);
		u16_pos = usearch_last(search, &status);

		while (u16_pos != USEARCH_DONE && U_SUCCESS(status)) {
			if (++count == target) {
				cp_index = (int64_t)u_countChar32(haystack_u16, u16_pos);
				break;
			}
			u16_pos = usearch_previous(search, &status);
		}
	}

	rb->particle = (as_particle*)(uint64_t)cp_index;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return AS_OK;
}

static int
string_read_op_contains(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	string_op find_op = *op;
	find_op.int_arg1 = 1;

	int result = string_read_op_find(&find_op, from, sz, rb);

	if (result != AS_OK) {
		return result;
	}

	int64_t index = (int64_t)(uint64_t)rb->particle;
	rb->particle = (as_particle*)(uint64_t)(index >= 0 ? 1 : 0);
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_read_op_starts_with(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb)
{
	if ((is_ascii(from, sz) && is_ascii(op->buf, op->buf_sz)) ||
			(is_nfc_utf8(from, sz) && is_nfc_utf8(op->buf, op->buf_sz))) {
		rb->particle = (as_particle*)(uint64_t)(op->buf_sz <= sz &&
				memcmp(from, op->buf, op->buf_sz) == 0);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		return AS_OK;
	}

	// at least one of the strings is neither NFC nor UTF8..
	UErrorCode status = U_ZERO_ERROR;
	UChar haystack_stack[STRING_OP_STACK_BUF_SZ];
	UChar needle_stack[STRING_OP_STACK_BUF_SZ];
	UChar* haystack_u16;
	UChar* needle_u16;
	int32_t h_len, n_len;
	// DEFER_FREE(x) captured NULL by value — later writes through
	// &haystack_heap/&needle_heap were invisible to cleanup.
	// DEFER_ATTR_FREE tracks the live value.
	DEFER_ATTR_FREE void* haystack_heap = NULL;
	DEFER_ATTR_FREE void* needle_heap = NULL;

	int convert_rc = utf8_to_u16(from, sz, haystack_stack,
			STRING_OP_STACK_BUF_SZ, &haystack_u16, &h_len, &haystack_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_starts_with - "
				"haystack UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	convert_rc = utf8_to_u16(op->buf, op->buf_sz, needle_stack,
			STRING_OP_STACK_BUF_SZ, &needle_u16, &n_len, &needle_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_starts_with - "
				"error %u - needle UTF-8 to UTF-16 failed",
				convert_rc);
		return convert_rc;
	}

	UStringSearch* search =
			get_canon_search(haystack_u16, h_len, needle_u16, n_len);

	if (search == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_starts_with - "
				"error %u - get_canon_search failed",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	int32_t match_pos = usearch_first(search, &status);

	rb->particle = (as_particle*)(uint64_t)(match_pos == 0);
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_read_op_ends_with(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	if ((is_ascii(from, sz) && is_ascii(op->buf, op->buf_sz)) ||
			(is_nfc_utf8(from, sz) && is_nfc_utf8(op->buf, op->buf_sz))) {
		rb->particle = (as_particle*)(uint64_t)(op->buf_sz <= sz &&
				memcmp(from + sz - op->buf_sz, op->buf, op->buf_sz) == 0);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		return AS_OK;
	}

	// at least one of the strings is neither NFC nor UTF8..
	UErrorCode status = U_ZERO_ERROR;
	UChar haystack_stack[STRING_OP_STACK_BUF_SZ];
	UChar needle_stack[STRING_OP_STACK_BUF_SZ];
	UChar* haystack_u16;
	UChar* needle_u16;
	int32_t h_len, n_len;
	// DEFER_FREE(x) captured NULL by value — later writes through
	// &haystack_heap/&needle_heap were invisible to cleanup.
	// DEFER_ATTR_FREE tracks the live value.
	DEFER_ATTR_FREE void* haystack_heap = NULL;
	DEFER_ATTR_FREE void* needle_heap = NULL;

	int convert_rc = utf8_to_u16(from, sz, haystack_stack,
			STRING_OP_STACK_BUF_SZ, &haystack_u16, &h_len, &haystack_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_ends_with - "
				"haystack UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	convert_rc = utf8_to_u16(op->buf, op->buf_sz, needle_stack,
			STRING_OP_STACK_BUF_SZ, &needle_u16, &n_len, &needle_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_ends_with - "
				"needle UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	UStringSearch* search =
			get_canon_search(haystack_u16, h_len, needle_u16, n_len);

	if (search == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_ends_with - "
				"error %u - get_canon_search failed",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	int32_t match_pos = usearch_last(search, &status);
	int32_t match_len = usearch_getMatchedLength(search);

	rb->particle = (as_particle*)(uint64_t)(match_pos != USEARCH_DONE &&
			match_len + match_pos == (int32_t)h_len);
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

// INT64_MIN ("-9223372036854775808") is 20 chars -- the longest valid int64.
#define INT64_MAX_STRLEN 20

static int
string_read_op_to_integer(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	if (sz == 0 || sz > INT64_MAX_STRLEN) {
		as_error_details_set_fmt(AS_SUB_OPNOT_STRING_CONVERSION_FAILED,
				"string_to_integer: string length %u is invalid (must be 1-%d)",
				sz, INT64_MAX_STRLEN);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	char buf[INT64_MAX_STRLEN + 1] = { 0 };

	memcpy(buf, from, sz);

	errno = 0;
	char* endptr;
	int64_t val = strtoll(buf, &endptr, 10);

	// No digits parsed, trailing non-numeric chars, or overflow.
	if (endptr == buf || endptr != buf + sz || errno == ERANGE) {
		as_error_details_set_fmt(AS_SUB_OPNOT_STRING_CONVERSION_FAILED,
				"string_to_integer: string is not a valid integer");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	rb->particle = (as_particle*)(uint64_t)val;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
	return AS_OK;
}

// Smallest subnormal (5e-324) fully expanded is ~325 chars; +1 for sign, +1
// for null terminator.
#define DOUBLE_MAX_STRLEN 327

static int
string_read_op_to_double(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	if (sz == 0 || sz > DOUBLE_MAX_STRLEN) {
		as_error_details_set_fmt(AS_SUB_OPNOT_STRING_CONVERSION_FAILED,
				"string_to_double: string length %u is invalid (must be 1-%d)",
				sz, DOUBLE_MAX_STRLEN);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	char buf[DOUBLE_MAX_STRLEN + 1] = { 0 };

	memcpy(buf, from, sz);

	errno = 0;
	char* endptr;
	double val = strtod(buf, &endptr);

	// No digits parsed, trailing non-numeric chars, or overflow.
	if (endptr == buf || endptr != buf + sz || errno == ERANGE) {
		as_error_details_set_fmt(AS_SUB_OPNOT_STRING_CONVERSION_FAILED,
				"string_to_double: string is not a valid double");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	*((double*)(&rb->particle)) = val;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_FLOAT);
	return AS_OK;
}

static int
string_read_op_byte_length(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb)
{
	rb->particle = (as_particle*)(uint64_t)sz;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
	return AS_OK;
}

static bool
is_numeric_syntax(const uint8_t* from, uint32_t sz, bool allow_dot)
{
	if (sz == 0) {
		return false;
	}

	uint32_t i = 0;

	if (from[0] == '+' || from[0] == '-') {
		i++;
	}

	if (i == sz) {
		return false;
	}

	bool has_dot = false;
	bool has_digit = false;

	for (; i < sz; i++) {
		if (from[i] >= '0' && from[i] <= '9') {
			has_digit = true;
		}
		else if (allow_dot && from[i] == '.' && ! has_dot) {
			has_dot = true;
		}
		else {
			return false;
		}
	}

	return has_digit;
}

static bool
is_valid_int64(const uint8_t* from, uint32_t sz)
{
	if (! is_numeric_syntax(from, sz, false)) {
		return false;
	}

	if (sz <= INT64_MAX_STRLEN) {
		char buf[INT64_MAX_STRLEN + 1] = { 0 };
		memcpy(buf, from, sz);
		errno = 0;
		char* endptr;
		strtoll(buf, &endptr, 10);
		return endptr == buf + sz && errno != ERANGE;
	}

	// Strip leading zeros for long strings (sz > 20 implies leading zeros).
	uint32_t start = 0;
	bool negative = (from[0] == '-');

	if (from[0] == '+' || from[0] == '-') {
		start = 1;
	}

	while (start + 1 < sz && from[start] == '0') {
		start++;
	}

	uint32_t sig_digits = sz - start;

	if (sig_digits > 19) {
		return false;
	}

	char buf[INT64_MAX_STRLEN + 1] = { 0 };
	uint32_t pos = 0;

	if (negative) {
		buf[pos++] = '-';
	}

	memcpy(buf + pos, from + start, sig_digits);

	errno = 0;
	char* endptr;
	strtoll(buf, &endptr, 10);
	return endptr == buf + pos + sig_digits && errno != ERANGE;
}

static bool
is_valid_double(const uint8_t* from, uint32_t sz)
{
	if (! is_numeric_syntax(from, sz, true)) {
		return false;
	}

	if (sz <= DOUBLE_MAX_STRLEN) {
		char buf[DOUBLE_MAX_STRLEN + 1] = { 0 };
		memcpy(buf, from, sz);
		errno = 0;
		char* endptr;
		double val = strtod(buf, &endptr);
		return endptr == buf + sz && errno != ERANGE && ! isinf(val) &&
				! isnan(val);
	}

	// Strip leading zeros and trailing fractional zeros for long strings.
	uint32_t start = 0;
	bool negative = (from[0] == '-');

	if (from[0] == '+' || from[0] == '-') {
		start = 1;
	}

	while (start + 1 < sz && from[start] == '0' && from[start + 1] != '.') {
		start++;
	}

	uint32_t end = sz;
	const uint8_t* dot = memchr(from + start, '.', end - start);

	if (dot != NULL) {
		uint32_t dot_pos = (uint32_t)(dot - from);

		while (end > dot_pos + 1 && from[end - 1] == '0') {
			end--;
		}

		if (end == dot_pos + 1) {
			end--;
		}
	}

	uint32_t compact_data_len = end - start;

	if (compact_data_len == 0) {
		return true; // all zeros — 0.0 is valid
	}

	uint32_t compact_len = compact_data_len + (negative ? 1 : 0);

	if (compact_len > DOUBLE_MAX_STRLEN) {
		return false;
	}

	char buf[DOUBLE_MAX_STRLEN + 1] = { 0 };
	uint32_t pos = 0;

	if (negative) {
		buf[pos++] = '-';
	}

	memcpy(buf + pos, from + start, compact_data_len);
	pos += compact_data_len;

	errno = 0;
	char* endptr;
	double val = strtod(buf, &endptr);
	return endptr == buf + pos && errno != ERANGE && ! isinf(val) && ! isnan(val);
}

// True iff s contains '.' followed by at least one ASCII digit (fractional part).
// IS_NUMERIC FLOAT requires this; integer-only and overflow digit-only strings are not float-class.
static bool
has_decimal_fraction(const uint8_t* from, uint32_t sz)
{
	for (uint32_t i = 0; i < sz; i++) {
		if (from[i] != '.') {
			continue;
		}
		for (uint32_t j = i + 1; j < sz; j++) {
			if (from[j] >= '0' && from[j] <= '9') {
				return true;
			}
		}
		return false; // '.' with no following digit (e.g. "5.")
	}
	return false;
}

static bool
is_valid_float_string(const uint8_t* from, uint32_t sz)
{
	return is_valid_double(from, sz) && has_decimal_fraction(from, sz);
}

static int
string_read_op_is_numeric(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	bool result;

	switch ((as_string_numeric_type)op->int_arg1) {
	case AS_STRING_NUMERIC_ANY:
		result = is_valid_int64(from, sz) || is_valid_float_string(from, sz);
		break;
	case AS_STRING_NUMERIC_INT:
		result = is_valid_int64(from, sz);
		break;
	case AS_STRING_NUMERIC_FLOAT:
		result = is_valid_float_string(from, sz);
		break;
	default:
		return -AS_ERR_PARAMETER;
	}

	rb->particle = (as_particle*)(uint64_t)result;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_read_op_is_upper(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	if (is_ascii(from, sz)) {
		rb->particle = (as_particle*)(uint64_t)is_ascii_upper(from, sz);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		return AS_OK;
	}

	rb->particle = (as_particle*)(uint64_t)is_utf8_upper(from, sz);
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_read_op_is_lower(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	if (is_ascii(from, sz)) {
		rb->particle = (as_particle*)(uint64_t)is_ascii_lower(from, sz);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		return AS_OK;
	}

	rb->particle = (as_particle*)(uint64_t)is_utf8_lower(from, sz);
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_read_op_to_blob(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	string_mem* answer = cf_malloc(sizeof(string_mem) + sz);

	memcpy(answer->data, from, sz);
	answer->sz = sz;
	answer->type = AS_PARTICLE_TYPE_BLOB;

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BLOB);

	return AS_OK;
}

static int
string_read_op_b64_decode(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	(void)op;

	uint32_t max_decoded = cf_b64_decoded_buf_size(sz);
	string_mem* answer = cf_malloc(sizeof(string_mem) + max_decoded);
	uint32_t decoded_sz;

	if (! cf_b64_validate_and_decode((const char*)from, sz, answer->data,
				&decoded_sz)) {
		cf_free(answer);
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_b64_decode -"
				"error %u - invalid base64",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	answer->sz = decoded_sz;
	answer->type = AS_PARTICLE_TYPE_BLOB;

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BLOB);

	return AS_OK;
}

// typedef struct list_mem_s {
// 	uint8_t type;
// 	uint32_t sz;
// 	uint8_t data[];
// } __attribute__ ((__packed__)) list_mem;
COMPILER_ASSERT(sizeof(string_mem) == AS_PARTICLE_MEM_HDR_SZ);

// Write a msgpack array header for 'count' elements into 'buf'.
// Encodes as fixarray (0x90|n) for n<16, array16 (0xdc) for n<65536,
// or array32 (0xdd) otherwise. Returns bytes written (1, 3, or 5).
// Used by string_read_op_split to open the result list.
static uint32_t
msgpack_list_hdr(uint8_t* buf, uint32_t count)
{
	if (count < 16) {
		buf[0] = (uint8_t)(0x90 | count); // fixarray
		return 1;
	}

	if (count < (1 << 16)) {
		buf[0] = 0xdc; // array16
		buf[1] = (uint8_t)(count >> 8);
		buf[2] = (uint8_t)count;
		return 3;
	}

	buf[0] = 0xdd; // array32
	buf[1] = (uint8_t)(count >> 24);
	buf[2] = (uint8_t)(count >> 16);
	buf[3] = (uint8_t)(count >> 8);
	buf[4] = (uint8_t)count;
	return 5;
}

// Write one Aerospike typed-string element into 'buf' as a msgpack str:
//   msgpack-str-header(len+1) | AS_PARTICLE_TYPE_STRING | str-bytes
// The +1 in the encoded length accounts for the Aerospike type byte that
// is stored inline before the string data (Aerospike list element encoding).
// Returns total bytes written. Used by string_read_op_split for each token.
static uint32_t
msgpack_typed_str_elt(uint8_t* buf, const uint8_t* str, uint32_t len)
{
	uint32_t enc_len = len + 1; // +1 for the AS type byte
	uint32_t hdr_sz;

	if (enc_len < 32) {
		buf[0] = (uint8_t)(0xa0 | enc_len); // fixstr
		hdr_sz = 1;
	}
	else if (enc_len < (1 << 8)) {
		buf[0] = 0xd9; // str8
		buf[1] = (uint8_t)enc_len;
		hdr_sz = 2;
	}
	else if (enc_len < (1 << 16)) {
		buf[0] = 0xda; // str16
		buf[1] = (uint8_t)(enc_len >> 8);
		buf[2] = (uint8_t)enc_len;
		hdr_sz = 3;
	}
	else {
		buf[0] = 0xdb; // str32
		buf[1] = (uint8_t)(enc_len >> 24);
		buf[2] = (uint8_t)(enc_len >> 16);
		buf[3] = (uint8_t)(enc_len >> 8);
		buf[4] = (uint8_t)enc_len;
		hdr_sz = 5;
	}

	buf[hdr_sz] = AS_PARTICLE_TYPE_STRING;
	memcpy(buf + hdr_sz + 1, str, len);

	return hdr_sz + 1 + len;
}

// Maximum bytes a msgpack array header can occupy (array32 = 5 bytes).
#define MSGPACK_MAX_LIST_HDR_SZ 5

static int
string_read_op_split(const string_op* op, const uint8_t* from, uint32_t sz,
		as_bin* rb)
{
	uint32_t sep_len = op->buf_sz;
	uint32_t max_elts = sep_len > 0 ? (sz / sep_len) + 1 : sz;
	uint32_t alloc_sz = sz + 2 * max_elts + 10;

	string_mem* answer = cf_malloc(alloc_sz + AS_PARTICLE_MEM_HDR_SZ);
	answer->type = AS_PARTICLE_TYPE_LIST;

	// Reserve worst-case list header space; elements go after it.
	uint8_t* elem_start = answer->data + MSGPACK_MAX_LIST_HDR_SZ;
	uint8_t* pos = elem_start;
	uint32_t n_elts = 0;
	const uint8_t* end = from + sz;

	if (sep_len == 0) {
		if (is_ascii(from, sz)) {
			for (uint32_t i = 0; i < sz; i++) {
				pos += msgpack_typed_str_elt(pos, from + i, 1);
				n_elts++;
			}
		}
		else {
			int32_t i = 0;

			while (i < (int32_t)sz) {
				int32_t cp_start = i;
				UChar32 c;

				U8_NEXT(from, i, (int32_t)sz, c);
				pos += msgpack_typed_str_elt(pos, from + cp_start,
						(uint32_t)(i - cp_start));
				n_elts++;
			}
		}
	}
	else {
		// ASCII separator: memmem cannot match inside a UTF-8 multibyte code point
		// (continuation bytes are 0x80–0xBF; ASCII is 0x00–0x7F).
		//
		// Non-ASCII separator: memmem can match at a byte offset that is not a
		// character boundary (substring of one code point can equal the needle).
		// Scan only UTF-8 character starts so each list element stays well-formed
		// when the stored string is well-formed UTF-8.
		if (is_ascii(op->buf, sep_len)) {
			const uint8_t* seg = from;

			for (;;) {
				uint8_t* found = memmem(seg, end - seg, op->buf, sep_len);

				if (found == NULL) {
					pos += msgpack_typed_str_elt(pos, seg, end - seg);
					n_elts++;
					break;
				}

				pos += msgpack_typed_str_elt(pos, seg, found - seg);
				n_elts++;
				seg = found + sep_len;
			}
		}
		else {
			const uint8_t* seg_start = from;
			int32_t j = 0;

			while (j < (int32_t)sz) {
				if (sep_len <= sz - (uint32_t)j &&
						memcmp(from + j, op->buf, sep_len) == 0) {
					pos += msgpack_typed_str_elt(pos, seg_start,
							(uint32_t)(from + j - seg_start));
					n_elts++;
					seg_start = from + j + sep_len;
					j = (int32_t)(seg_start - from);
					continue;
				}

				UChar32 c;
				U8_NEXT(from, j, (int32_t)sz, c);
			}

			pos += msgpack_typed_str_elt(pos, seg_start,
					(uint32_t)(end - seg_start));
			n_elts++;
		}
	}

	// Write actual list header and shift elements into place.
	uint32_t hdr_sz = msgpack_list_hdr(answer->data, n_elts);
	uint32_t elem_sz = pos - elem_start;

	if (hdr_sz < MSGPACK_MAX_LIST_HDR_SZ) {
		memmove(answer->data + hdr_sz, elem_start, elem_sz);
	}

	answer->sz = hdr_sz + elem_sz;
	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_LIST);

	return AS_OK;
}

// Thread-local compiled regex cache. Keyed on (pattern bytes, flags).
// Reusing the compiled URegularExpression avoids the expensive uregex_open
// that dominates each regex call (e.g. 27us for \d+ due to Unicode class
// construction). Shared by regex_compare and regex_replace.
static __thread URegularExpression* tl_regex;
static __thread uint8_t* tl_regex_pattern;
static __thread uint32_t tl_regex_pattern_sz;
static __thread uint32_t tl_regex_flags;

// PCRE2 thread-local compiled regex cache. For ASCII text, compiled without
// UTF/UCP flags for maximum speed. For non-ASCII UTF-8 text, compiled with
// PCRE2_UTF | PCRE2_UCP for native UTF-8 processing without ICU's UTF-16
// conversion overhead.
static __thread pcre2_code* tl_pcre2;
static __thread uint8_t* tl_pcre2_pattern;
static __thread uint32_t tl_pcre2_pattern_sz;
static __thread uint32_t tl_pcre2_flags;
static __thread pcre2_match_data* tl_pcre2_md;
static __thread pcre2_match_context* tl_pcre2_mctx;
static __thread bool tls_regex_exit_registered;

static void
regex_tls_exit(void* udata)
{
	(void)udata;

	if (tl_pcre2_mctx != NULL) {
		pcre2_match_context_free(tl_pcre2_mctx);
		tl_pcre2_mctx = NULL;
	}

	if (tl_pcre2 != NULL) {
		pcre2_code_free(tl_pcre2);
		tl_pcre2 = NULL;
	}

	if (tl_pcre2_md != NULL) {
		pcre2_match_data_free(tl_pcre2_md);
		tl_pcre2_md = NULL;
	}

	cf_free(tl_pcre2_pattern);
	tl_pcre2_pattern = NULL;
	tl_pcre2_pattern_sz = 0;
	tl_pcre2_flags = 0;

	if (tl_regex != NULL) {
		uregex_close(tl_regex);
		tl_regex = NULL;
	}

	cf_free(tl_regex_pattern);
	tl_regex_pattern = NULL;
	tl_regex_pattern_sz = 0;
	tl_regex_flags = 0;
}

static void
ensure_regex_tls_exit_registered(void)
{
	if (! tls_regex_exit_registered) {
		cf_thread_add_exit(regex_tls_exit, NULL);
		tls_regex_exit_registered = true;
	}
}

static URegularExpression*
get_cached_regex(const uint8_t* pattern, uint32_t pattern_sz, uint32_t flags)
{
	if (tl_regex != NULL && tl_regex_flags == flags &&
			tl_regex_pattern_sz == pattern_sz &&
			memcmp(tl_regex_pattern, pattern, pattern_sz) == 0) {
		return tl_regex;
	}

	if (tl_regex != NULL) {
		uregex_close(tl_regex);
		tl_regex = NULL;
	}

	UErrorCode status = U_ZERO_ERROR;
	UText ut_pattern = UTEXT_INITIALIZER;

	utext_openUTF8(&ut_pattern, (const char*)pattern, pattern_sz, &status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"get_cached_regex - pattern UText open failed");
		return NULL;
	}

	UParseError pe = { 0 };
	tl_regex = uregex_openUText(&ut_pattern, flags, &pe, &status);
	utext_close(&ut_pattern);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"get_cached_regex - malformed regex pattern");
		tl_regex = NULL;
		return NULL;
	}

	UErrorCode lim_status = U_ZERO_ERROR;

	uregex_setTimeLimit(tl_regex, STRING_REGEX_ICU_STEP_LIMIT, &lim_status);

	if (U_FAILURE(lim_status)) {
		uregex_close(tl_regex);
		tl_regex = NULL;
		cf_ticker_warning(AS_PARTICLE,
				"get_cached_regex - uregex_setTimeLimit failed");
		return NULL;
	}

	lim_status = U_ZERO_ERROR;
	uregex_setStackLimit(tl_regex, STRING_REGEX_ICU_STACK_BYTES, &lim_status);

	if (U_FAILURE(lim_status)) {
		uregex_close(tl_regex);
		tl_regex = NULL;
		cf_ticker_warning(AS_PARTICLE,
				"get_cached_regex - uregex_setStackLimit failed");
		return NULL;
	}

	if (tl_regex_pattern == NULL || tl_regex_pattern_sz < pattern_sz) {
		cf_free(tl_regex_pattern);
		tl_regex_pattern = cf_malloc(pattern_sz);
	}

	memcpy(tl_regex_pattern, pattern, pattern_sz);
	tl_regex_pattern_sz = pattern_sz;
	tl_regex_flags = flags;

	ensure_regex_tls_exit_registered();

	return tl_regex;
}

static uint32_t
translate_regex_flags(uint8_t flags)
{
	uint32_t result = 0;
	static const uint32_t map[] = {
		[AS_STRING_REGEX_CASE_INSENSITIVE] = UREGEX_CASE_INSENSITIVE,
		[AS_STRING_REGEX_MULTILINE] = UREGEX_MULTILINE,
		[AS_STRING_REGEX_DOTALL] = UREGEX_DOTALL,
		[AS_STRING_REGEX_UNIX_LINES_ONLY] = UREGEX_UNIX_LINES,
	};

	while (flags != 0) {
		result |= map[flags & -flags];
		flags &= flags - 1;
	}

	return result;
}

static uint32_t
translate_pcre2_flags(uint8_t flags, bool utf8_mode)
{
	uint32_t result = utf8_mode ? (PCRE2_UTF | PCRE2_UCP) : 0;

	if (flags & AS_STRING_REGEX_CASE_INSENSITIVE) {
		result |= PCRE2_CASELESS;
	}

	if (flags & AS_STRING_REGEX_MULTILINE) {
		result |= PCRE2_MULTILINE;
	}

	if (flags & AS_STRING_REGEX_DOTALL) {
		result |= PCRE2_DOTALL;
	}

	return result;
}

static pcre2_code*
get_cached_pcre2(const uint8_t* pattern, uint32_t pattern_sz, uint32_t flags,
		bool unix_lines)
{
	if (tl_pcre2 != NULL && tl_pcre2_flags == flags &&
			tl_pcre2_pattern_sz == pattern_sz &&
			memcmp(tl_pcre2_pattern, pattern, pattern_sz) == 0) {
		return tl_pcre2;
	}

	if (tl_pcre2 != NULL) {
		pcre2_code_free(tl_pcre2);
		tl_pcre2 = NULL;
	}

	if (tl_pcre2_mctx != NULL) {
		pcre2_match_context_free(tl_pcre2_mctx);
		tl_pcre2_mctx = NULL;
	}

	if (tl_pcre2_md != NULL) {
		pcre2_match_data_free(tl_pcre2_md);
		tl_pcre2_md = NULL;
	}

	pcre2_compile_context* cctx = NULL;

	if (unix_lines) {
		cctx = pcre2_compile_context_create(NULL);
		pcre2_set_newline(cctx, PCRE2_NEWLINE_LF);
	}

	int errorcode;
	PCRE2_SIZE erroroffset;

	tl_pcre2 = pcre2_compile(pattern, pattern_sz, flags, &errorcode,
			&erroroffset, cctx);

	if (cctx != NULL) {
		pcre2_compile_context_free(cctx);
	}

	if (tl_pcre2 == NULL) {
		return NULL;
	}

	pcre2_jit_compile(tl_pcre2, PCRE2_JIT_COMPLETE);

	tl_pcre2_md = pcre2_match_data_create_from_pattern(tl_pcre2, NULL);

	tl_pcre2_mctx = pcre2_match_context_create(NULL);
	pcre2_set_match_limit(tl_pcre2_mctx, STRING_PCRE2_MATCH_LIMIT);
	pcre2_set_depth_limit(tl_pcre2_mctx, STRING_PCRE2_DEPTH_LIMIT);

	ensure_regex_tls_exit_registered();

	if (tl_pcre2_pattern == NULL || tl_pcre2_pattern_sz < pattern_sz) {
		cf_free(tl_pcre2_pattern);
		tl_pcre2_pattern = cf_malloc(pattern_sz);
	}

	memcpy(tl_pcre2_pattern, pattern, pattern_sz);
	tl_pcre2_pattern_sz = pattern_sz;
	tl_pcre2_flags = flags;

	return tl_pcre2;
}

static int
string_read_op_regex_compare(const string_op* op, const uint8_t* from,
		uint32_t sz, as_bin* rb)
{
	if (op->buf_sz == 0) {
		rb->particle = (as_particle*)((uint64_t)true);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		return AS_OK;
	}

	// PCRE2 byte-mode fast path requires both haystack AND pattern to be
	// ASCII. A non-ASCII pattern needs ICU's full (1-to-many) Unicode case
	// folding under CASE_INSENSITIVE (e.g. 'ß' -> "ss"); PCRE2_CASELESS
	// without PCRE2_UCP only folds within ASCII.
	if (is_ascii(from, sz) && is_ascii(op->buf, op->buf_sz)) {
		uint8_t raw_flags = op->int_arg1;
		uint32_t pcre2_flags = translate_pcre2_flags(raw_flags, false);
		bool unix_lines = (raw_flags & AS_STRING_REGEX_UNIX_LINES_ONLY) != 0;
		pcre2_code* re =
				get_cached_pcre2(op->buf, op->buf_sz, pcre2_flags, unix_lines);

		if (re != NULL) {
			int rc = pcre2_match(re, from, sz, 0, 0, tl_pcre2_md, tl_pcre2_mctx);

			rb->particle = (as_particle*)(uint64_t)(rc >= 0);
			as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
			return AS_OK;
		}
	}

	uint32_t flags = translate_regex_flags(op->int_arg1);
	URegularExpression* regex = get_cached_regex(op->buf, op->buf_sz, flags);

	if (regex == NULL) {
		// Same as string_modify_op_regex_replace: NULL from utext_openUTF8 or
		// uregex_openUText (bad pattern / UText failure) — not storage quota.
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_regex_compare - "
				"get_cached_regex failed");
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_REGEX_INVALID,
				"string_regex_compare: regex pattern is invalid or could not be compiled");
		return -AS_ERR_PARAMETER;
	}

	UErrorCode status = U_ZERO_ERROR;
	UText ut_text = UTEXT_INITIALIZER;
	utext_openUTF8(&ut_text, (const char*)from, sz, &status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_regex_compare - "
				"text utext_openUTF8 failed with status: %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	uregex_setUText(regex, &ut_text, &status);
	if (U_FAILURE(status)) {
		utext_close(&ut_text);
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_regex_compare - "
				"uregex_setUText failed with status: %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	UBool found = uregex_find(regex, 0, &status);
	utext_close(&ut_text);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_read_op_regex_compare - "
				"uregex_find failed with status: %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	rb->particle = (as_particle*)(uint64_t)found;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
	return AS_OK;
}

static int
string_modify_op_regex_replace(const string_op* op, uint8_t* to,
		const uint8_t* from, uint32_t old_sz, uint32_t* new_sz)
{
	msgpack_in mp = {
		.buf = op->buf,
		.buf_sz = op->buf_sz,
	};

	uint32_t argc;
	msgpack_get_list_ele_count(&mp, &argc);

	if (argc != 2) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - error %u expected 2 list "
				"elements, got %u",
				AS_ERR_PARAMETER, argc);
		return -AS_ERR_PARAMETER;
	}

	if (msgpack_peek_type(&mp) != MSGPACK_TYPE_STRING) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - pattern is not a string");
		return -AS_ERR_PARAMETER;
	}

	uint32_t pattern_raw_sz;
	const uint8_t* pattern_raw = msgpack_get_bin(&mp, &pattern_raw_sz);

	if (pattern_raw == NULL || pattern_raw_sz == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - error %u empty or missing "
				"pattern",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	pattern_raw++;
	pattern_raw_sz--;

	if (! cf_str_is_valid_utf8(pattern_raw, pattern_raw_sz)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - error %u: pattern is not valid UTF-8",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	if (msgpack_peek_type(&mp) != MSGPACK_TYPE_STRING) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - replacement is not a string");
		return -AS_ERR_PARAMETER;
	}

	uint32_t repl_raw_sz;
	const uint8_t* repl_raw = msgpack_get_bin(&mp, &repl_raw_sz);

	if (repl_raw == NULL || repl_raw_sz == 0) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - error %u empty or missing "
				"replacement",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	repl_raw++;
	repl_raw_sz--;

	if (! cf_str_is_valid_utf8(repl_raw, repl_raw_sz)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - error %u: replacement is not valid UTF-8",
				AS_ERR_PARAMETER);
		return -AS_ERR_PARAMETER;
	}

	bool global = op->int_arg1 & AS_STRING_REGEX_GLOBAL;
	uint8_t raw_flags = op->int_arg1 & ~AS_STRING_REGEX_GLOBAL;

	if (pattern_raw_sz == 0) {
		if (! global) {
			memcpy(to, repl_raw, repl_raw_sz);
			memcpy(to + repl_raw_sz, from, old_sz);
			*new_sz = repl_raw_sz + old_sz;
			return AS_OK;
		}

		uint8_t* out = to;
		const uint8_t* p = from;
		uint32_t remaining = old_sz;

		while (remaining > 0) {
			memcpy(out, repl_raw, repl_raw_sz);
			out += repl_raw_sz;

			uint32_t cp_sz = 1;
			if (*p >= 0xF0)
				cp_sz = 4;
			else if (*p >= 0xE0)
				cp_sz = 3;
			else if (*p >= 0xC0)
				cp_sz = 2;

			memcpy(out, p, cp_sz);
			out += cp_sz;
			p += cp_sz;
			remaining -= cp_sz;
		}

		memcpy(out, repl_raw, repl_raw_sz);
		out += repl_raw_sz;
		*new_sz = (uint32_t)(out - to);
		return AS_OK;
	}

	bool ascii = is_ascii(from, old_sz) && is_ascii(repl_raw, repl_raw_sz);
	uint32_t pcre2_flags = translate_pcre2_flags(raw_flags, ! ascii);
	bool unix_lines = (raw_flags & AS_STRING_REGEX_UNIX_LINES_ONLY) != 0;
	pcre2_code* re = get_cached_pcre2(pattern_raw, pattern_raw_sz, pcre2_flags,
			unix_lines);

	if (re != NULL) {
		uint32_t sub_opts = PCRE2_SUBSTITUTE_OVERFLOW_LENGTH | PCRE2_NO_UTF_CHECK;

		if (global) {
			sub_opts |= PCRE2_SUBSTITUTE_GLOBAL;
		}

		PCRE2_SIZE out_cap =
				(PCRE2_SIZE)(old_sz + (old_sz + 1) * repl_raw_sz) * 3;
		PCRE2_SIZE outlength = out_cap;
		int rc = pcre2_substitute(re, from, old_sz, 0, sub_opts, tl_pcre2_md,
				tl_pcre2_mctx, repl_raw, repl_raw_sz, to, &outlength);

		if (rc >= 0) {
			*new_sz = (uint32_t)outlength;
			return AS_OK;
		}
	}

	uint32_t icu_flags = translate_regex_flags(raw_flags);

	URegularExpression* regex =
			get_cached_regex(pattern_raw, pattern_raw_sz, icu_flags);

	if (regex == NULL) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"get_cached_regex failed");
		as_error_details_set_fmt(AS_SUB_PARAM_STRING_REGEX_INVALID,
				"string_regex_replace: regex pattern is invalid or could not be compiled");
		return -AS_ERR_PARAMETER;
	}

	UErrorCode status = U_ZERO_ERROR;

	// Text + replacement need UTF-16 since uregex_replaceAll/First iterates
	// every character (UText per-char indirection is slower than bulk convert).
	UChar repl_stack[256];
	DEFER_ATTR_FREE void* repl_heap = NULL;
	int32_t repl_len;
	UChar* repl_u16;

	int convert_rc = utf8_to_u16(repl_raw, repl_raw_sz, repl_stack, 256,
			&repl_u16, &repl_len, &repl_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"replacement UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	UChar text_stack[STRING_OP_STACK_BUF_SZ];
	DEFER_ATTR_FREE void* text_heap = NULL;
	int32_t text_len;
	UChar* text_u16;

	convert_rc = utf8_to_u16(from, old_sz, text_stack, STRING_OP_STACK_BUF_SZ,
			&text_u16, &text_len, &text_heap);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"input UTF-8 to UTF-16 failed: %d",
				convert_rc);
		return convert_rc;
	}

	uregex_setText(regex, text_u16, text_len, &status);

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"uregex_setText failed with status: %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	UChar result_stack[STRING_OP_STACK_BUF_SZ];
	DEFER_ATTR_FREE void* result_heap_ptr = NULL;
	UChar* result_u16 = result_stack;
	int32_t result_cap = STRING_OP_STACK_BUF_SZ;

	int32_t result_len = global ? uregex_replaceAll(regex, repl_u16, repl_len,
										  result_u16, result_cap, &status)
								: uregex_replaceFirst(regex, repl_u16, repl_len,
										  result_u16, result_cap, &status);

	if (status == U_BUFFER_OVERFLOW_ERROR) {
		status = U_ZERO_ERROR;
		result_cap = result_len + 1;
		result_heap_ptr = cf_malloc(result_cap * sizeof(UChar));
		result_u16 = (UChar*)result_heap_ptr;

		uregex_setText(regex, text_u16, text_len, &status);

		result_len = global ? uregex_replaceAll(regex, repl_u16, repl_len,
									  result_u16, result_cap, &status)
							: uregex_replaceFirst(regex, repl_u16, repl_len,
									  result_u16, result_cap, &status);
	}

	if (U_FAILURE(status)) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"uregex_replaceFirst/replaceAll failed with status: %d",
				status);
		return icu_uerror_to_as_err(status);
	}

	convert_rc = u16_to_utf8(to, (uint32_t)result_len * 3, new_sz, result_u16,
			result_len);
	if (convert_rc != AS_OK) {
		cf_ticker_warning(AS_PARTICLE,
				"string_modify_op_regex_replace - "
				"UTF-16 to UTF-8 failed: %d",
				convert_rc);
		return convert_rc;
	}

	return AS_OK;
}
