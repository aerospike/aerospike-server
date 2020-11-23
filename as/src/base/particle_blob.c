/*
 * particle_blob.c
 *
 * Copyright (C) 2015-2020 Aerospike, Inc.
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


#include "base/particle_blob.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_bytes.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "bits.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"

#include "warnings.h"


// BLOB particle interface function declarations are in particle_blob.h since
// BLOB functions are used by other particles derived from BLOB.


//==========================================================
// BLOB particle interface - vtable.
//

const as_particle_vtable blob_vtable = {
		blob_destruct,
		blob_size,

		blob_concat_size_from_wire,
		blob_append_from_wire,
		blob_prepend_from_wire,
		blob_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		blob_size_from_asval,
		blob_from_asval,
		blob_to_asval,
		blob_asval_wire_size,
		blob_asval_to_wire,

		blob_size_from_msgpack,
		blob_from_msgpack,

		blob_skip_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

typedef struct blob_mem_s {
	uint8_t type;
	uint32_t sz;
	uint8_t data[];
} __attribute__ ((__packed__)) blob_mem;

typedef struct blob_flat_s {
	uint8_t type;
	uint32_t size; // host order on device
	uint8_t data[];
} __attribute__ ((__packed__)) blob_flat;

typedef struct bits_op_s {
	int32_t offset;
	uint32_t size;
	uint64_t value;
	const uint8_t* buf;
	uint64_t subflags;
	uint64_t flags;
} bits_op;

struct bits_state_s;

typedef bool (*bits_parse_fn) (struct bits_state_s* state, bits_op* op);
typedef bool (*bits_modify_fn) (const bits_op* op, uint8_t* to,
		const uint8_t* from, uint32_t n_bytes);
typedef bool (*bits_read_fn) (const bits_op* op, const uint8_t* from,
		as_bin* rb, uint32_t n_bytes);
typedef int (*bits_prepare_op_fn)(struct bits_state_s* state, bits_op* op,
		uint32_t byte_offset, uint32_t op_size);

typedef struct bits_op_def_s {
	const bits_prepare_op_fn prepare;
	union {
		const bits_modify_fn modify;
		const bits_read_fn read;
	} fn;
	uint64_t bad_flags;
	uint32_t min_args;
	uint32_t max_args;
	const bits_parse_fn* args;
	const char* name;
} bits_op_def;

typedef enum {
	OP_ACTION_OVERWRITE,
	OP_ACTION_EXPAND,
	OP_ACTION_REMOVE
} op_action;

typedef struct bits_state_s {
	as_bits_op_type op_type;
	msgpack_in pk;
	uint32_t n_args;
	bits_op_def* def;

	uint32_t n_bytes_head;
	uint32_t n_bytes_expand;
	uint32_t n_bytes_op;
	uint32_t n_bytes_tail;
	op_action action;

	uint32_t old_size;
	uint32_t new_size;
} bits_state;


//==========================================================
// Forward declarations.
//

static bool bits_state_init(bits_state* state, const as_msg_op* msg_op, bool is_read);
static bool bits_parse_op(bits_state* state, bits_op* op);
static bool bits_parse_byte_offset(bits_state* state, bits_op* op);
static bool bits_parse_offset(bits_state* state, bits_op* op);
static bool bits_parse_byte_size(bits_state* state, bits_op* op);
static bool bits_parse_byte_size_allow_zero(bits_state* state, bits_op* op);
static bool bits_parse_integer_size(bits_state* state, bits_op* op);
static bool bits_parse_size(bits_state* state, bits_op* op);
static bool bits_parse_boolean_value(bits_state* state, bits_op* op);
static bool bits_parse_n_bits_value(bits_state* state, bits_op* op);
static bool bits_parse_integer_value(bits_state* state, bits_op* op);
static bool bits_parse_buf(bits_state* state, bits_op* op);
static bool bits_parse_flags(bits_state* state, bits_op* op);
static bool bits_parse_resize_subflags(bits_state* state, bits_op* op);
static bool bits_parse_arithmetic_subflags(bits_state* state, bits_op* op);
static bool bits_parse_get_integer_subflags(bits_state* state, bits_op* op);
static bool bits_parse_subflags(bits_state* state, bits_op* op);

static int bits_prepare_read(bits_state* state, bits_op* op, const as_bin* b);
static int bits_prepare_modify(bits_state* state, bits_op* op, const as_bin* b);
static int bits_prepare_op(bits_state* state, bits_op* op, const as_bin* b);
static int32_t bits_normalize_offset(const bits_state* state, bits_op* op);
static int bits_prepare_resize_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);
static int bits_prepare_insert_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);
static int bits_prepare_remove_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);
static int bits_prepare_modify_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);
static int bits_prepare_integer_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);
static int bits_prepare_read_op(bits_state* state, bits_op* op, uint32_t byte_offset, uint32_t op_size);

static bool bits_execute_modify_op(const bits_state* state, const bits_op* op, const as_particle* old_blob, as_particle* new_blob);
static bool bits_execute_read_op(const bits_state* state, const bits_op* op, const as_particle* blob, as_bin* rb);

static bool bits_modify_op_resize(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_insert(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_set(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_or(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_xor(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_and(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_not(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_lshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_rshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_add(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_subtract(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static bool bits_modify_op_set_int(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);

static bool bits_read_op_get(const bits_op* op, const uint8_t* from, as_bin* rb, uint32_t n_bytes);
static bool bits_read_op_count(const bits_op* op, const uint8_t* from, as_bin* rb, uint32_t n_bytes);
static bool bits_read_op_lscan(const bits_op* op, const uint8_t* from, as_bin* rb, uint32_t n_bytes);
static bool bits_read_op_rscan(const bits_op* op, const uint8_t* from, as_bin* rb, uint32_t n_bytes);
static bool bits_read_op_get_integer(const bits_op* op, const uint8_t* from, as_bin* rb, uint32_t n_bytes);

static void lshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static void rshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);
static void rshift_with_or(const bits_op* op, uint8_t* to, const uint8_t* from);
static void rshift_with_xor(const bits_op* op, uint8_t* to, const uint8_t* from);
static void rshift_with_and(const bits_op* op, uint8_t* to, const uint8_t* from);
static uint64_t load_int(const bits_op* op, const uint8_t* from, uint32_t n_bytes);
static bool handle_signed_overflow(uint32_t n_bits, bool is_saturate, uint64_t load, uint64_t value, uint64_t* result);
static void store_int(const bits_op* op, uint8_t* to, uint64_t value, uint32_t n_bytes);
static void restore_ends(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes);


//==========================================================
// Inlines & macros.
//

static inline as_particle_type
blob_bytes_type_to_particle_type(as_bytes_type type)
{
	switch (type) {
	case AS_BYTES_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case AS_BYTES_BLOB:
		return AS_PARTICLE_TYPE_BLOB;
	case AS_BYTES_JAVA:
		return AS_PARTICLE_TYPE_JAVA_BLOB;
	case AS_BYTES_CSHARP:
		return AS_PARTICLE_TYPE_CSHARP_BLOB;
	case AS_BYTES_PYTHON:
		return AS_PARTICLE_TYPE_PYTHON_BLOB;
	case AS_BYTES_RUBY:
		return AS_PARTICLE_TYPE_RUBY_BLOB;
	case AS_BYTES_PHP:
		return AS_PARTICLE_TYPE_PHP_BLOB;
	case AS_BYTES_ERLANG:
		return AS_PARTICLE_TYPE_ERLANG_BLOB;
	case AS_BYTES_HLL:
		return AS_PARTICLE_TYPE_HLL;
	case AS_BYTES_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case AS_BYTES_INTEGER:
	case AS_BYTES_DOUBLE:
	case AS_BYTES_MAP:
	case AS_BYTES_LIST:
	case AS_BYTES_UNDEF:
	default:
		break;
	}

	// Invalid blob types remain as blobs.
	return AS_PARTICLE_TYPE_BLOB;
}

#define RSHIFT_WITH_OP(_bop) \
{ \
	uint32_t n_shift = (uint32_t)op->value; \
	const uint8_t* buf = &op->buf[0]; \
	const uint8_t* end = &op->buf[(op->size + 7) / 8]; \
	uint32_t r8 = n_shift % 8; \
	uint32_t l8 = 8 - r8; \
	uint32_t l64 = 64 - r8; \
	\
	if (r8 == 0) { \
		while (buf < end) { \
			*to++ = *buf++ _bop *from++; \
		} \
		\
		return; \
	} \
	\
	*to++ = (uint8_t)((*buf++ >> r8) _bop *from++); \
	\
	while (buf < end && (buf - op->buf < 8 || (uint64_t)buf % 8 != 0)) { \
		*to++ = (uint8_t)((((*(buf - 1) << l8) | (*buf >> r8))) _bop *from++); \
		buf++; \
	} \
	\
	while (end - buf >= 64) { \
		for (uint32_t i = 0; i < 8; i++) { \
			uint64_t v = cf_swap_from_be64(*(uint64_t*)(buf - 8)); \
			uint64_t n = cf_swap_from_be64(*(uint64_t*)buf); \
			\
			*(uint64_t*)to = cf_swap_to_be64((v << l64) | (n >> r8)) _bop \
					*(uint64_t*)from; \
			to += 8; \
			buf += 8; \
			from += 8; \
		} \
	} \
	\
	while (end - buf >= 8) { \
		uint64_t v = cf_swap_from_be64(*(uint64_t*)(buf - 8)); \
		uint64_t n = cf_swap_from_be64(*(uint64_t*)buf); \
		\
		*(uint64_t*)to = cf_swap_to_be64((v << l64) | (n >> r8)) _bop \
				*(uint64_t*)from; \
		to += 8; \
		buf += 8; \
		from += 8; \
	} \
	\
	while (buf < end) { \
		*to++ = (uint8_t)((((*(buf - 1) << l8) | (*buf >> r8))) _bop *from++); \
		buf++; \
	} \
	\
	if (op->size + n_shift > 8) { \
		*to = (uint8_t)((*(buf - 1) << l8) _bop *from); \
	} \
}

#define BITS_MODIFY_OP_ENTRY(_op, _op_fn, _prep_fn, _flags, _min_args, \
		_max_args, ...) \
	[_op].name = # _op, [_op].prepare = _prep_fn, [_op].fn.modify = _op_fn, \
	[_op].bad_flags = ~((uint64_t)(_flags)), [_op].min_args = _min_args, \
	[_op].max_args = _max_args, [_op].args = (bits_parse_fn[]){__VA_ARGS__}
#define BITS_READ_OP_ENTRY(_op, _op_fn, _min_args, _max_args, ...) \
	[_op].name = # _op, [_op].prepare = bits_prepare_read_op, \
	[_op].fn.read = _op_fn, [_op].bad_flags = ~((uint64_t)(0)), \
	[_op].min_args = _min_args, [_op].max_args = _max_args, \
	[_op].args = (bits_parse_fn[]){__VA_ARGS__}


//==========================================================
// Op tables.
//

static const bits_op_def bits_modify_op_table[] = {
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_RESIZE, bits_modify_op_resize,
				bits_prepare_resize_op,
				(AS_BITS_FLAG_CREATE_ONLY | AS_BITS_FLAG_UPDATE_ONLY |
						AS_BITS_FLAG_NO_FAIL),
				1, 3, bits_parse_byte_size_allow_zero, bits_parse_flags,
				bits_parse_resize_subflags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_INSERT, bits_modify_op_insert,
				bits_prepare_insert_op,
				(AS_BITS_FLAG_CREATE_ONLY | AS_BITS_FLAG_UPDATE_ONLY |
						AS_BITS_FLAG_NO_FAIL),
				2, 3, bits_parse_byte_offset, bits_parse_buf, bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_REMOVE, NULL,
				bits_prepare_remove_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				2, 3, bits_parse_byte_offset, bits_parse_byte_size,
				bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_SET, bits_modify_op_set,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size, bits_parse_buf,
				bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_OR, bits_modify_op_or,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size, bits_parse_buf,
				bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_XOR, bits_modify_op_xor,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size, bits_parse_buf,
				bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_AND, bits_modify_op_and,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size, bits_parse_buf,
				bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_NOT, bits_modify_op_not,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				2, 3, bits_parse_offset, bits_parse_size, bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_LSHIFT, bits_modify_op_lshift,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size,
				bits_parse_n_bits_value, bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_RSHIFT, bits_modify_op_rshift,
				bits_prepare_modify_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL |
						AS_BITS_FLAG_PARTIAL),
				3, 4, bits_parse_offset, bits_parse_size,
				bits_parse_n_bits_value, bits_parse_flags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_ADD, bits_modify_op_add,
				bits_prepare_integer_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL),
				3, 5, bits_parse_offset, bits_parse_integer_size,
				bits_parse_integer_value, bits_parse_flags,
				bits_parse_arithmetic_subflags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_SUBTRACT, bits_modify_op_subtract,
				bits_prepare_integer_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL),
				3, 5, bits_parse_offset, bits_parse_integer_size,
				bits_parse_integer_value, bits_parse_flags,
				bits_parse_arithmetic_subflags),
		BITS_MODIFY_OP_ENTRY(AS_BITS_OP_SET_INT, bits_modify_op_set_int,
				bits_prepare_integer_op,
				(AS_BITS_FLAG_UPDATE_ONLY | AS_BITS_FLAG_NO_FAIL),
				3, 4, bits_parse_offset, bits_parse_integer_size,
				bits_parse_integer_value, bits_parse_flags),
};

static const bits_op_def bits_read_op_table[] = {
		BITS_READ_OP_ENTRY(AS_BITS_OP_GET, bits_read_op_get,
				2, 2, bits_parse_offset, bits_parse_size),
		BITS_READ_OP_ENTRY(AS_BITS_OP_COUNT, bits_read_op_count,
				2, 2, bits_parse_offset, bits_parse_size),
		BITS_READ_OP_ENTRY(AS_BITS_OP_LSCAN, bits_read_op_lscan,
				3, 3, bits_parse_offset, bits_parse_size,
				bits_parse_boolean_value),
		BITS_READ_OP_ENTRY(AS_BITS_OP_RSCAN, bits_read_op_rscan,
				3, 3, bits_parse_offset, bits_parse_size,
				bits_parse_boolean_value),
		BITS_READ_OP_ENTRY(AS_BITS_OP_GET_INT, bits_read_op_get_integer,
				2, 3, bits_parse_offset, bits_parse_integer_size,
				bits_parse_get_integer_subflags),
};


//==========================================================
// BLOB particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
blob_destruct(as_particle* p)
{
	cf_free(p);
}

uint32_t
blob_size(const as_particle* p)
{
	return (uint32_t)(sizeof(blob_mem) + ((blob_mem*)p)->sz);
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
blob_concat_size_from_wire(as_particle_type wire_type,
		const uint8_t* wire_value, uint32_t value_size, as_particle** pp)
{
	(void)wire_value;
	blob_mem* p_blob_mem = (blob_mem*)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "error %u type mismatch concat sizing blob/string, %d:%d",
				AS_ERR_INCOMPATIBLE_TYPE, p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	return (int32_t)(sizeof(blob_mem) + p_blob_mem->sz + value_size);
}

int
blob_append_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	blob_mem* p_blob_mem = (blob_mem*)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "error %u type mismatch appending to blob/string, %d:%d",
				AS_ERR_INCOMPATIBLE_TYPE, p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	memcpy(p_blob_mem->data + p_blob_mem->sz, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_prepend_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	blob_mem* p_blob_mem = (blob_mem*)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "error %u type mismatch prepending to blob/string, %d:%d",
				AS_ERR_INCOMPATIBLE_TYPE, p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	memmove(p_blob_mem->data + value_size, p_blob_mem->data, p_blob_mem->sz);
	memcpy(p_blob_mem->data, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_incr_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;
	cf_warning(AS_PARTICLE, "error %u unexpected increment of blob/string",
			AS_ERR_INCOMPATIBLE_TYPE);
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int32_t
blob_size_from_wire(const uint8_t* wire_value, uint32_t value_size)
{
	(void)wire_value;
	// Wire value is same as in-memory value.
	return (int32_t)(sizeof(blob_mem) + value_size);
}

int
blob_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	blob_mem* p_blob_mem = (blob_mem*)*pp;

	p_blob_mem->type = wire_type;
	p_blob_mem->sz = value_size;
	memcpy(p_blob_mem->data, wire_value, p_blob_mem->sz);

	return 0;
}

int
blob_compare_from_wire(const as_particle* p, as_particle_type wire_type,
		const uint8_t* wire_value, uint32_t value_size)
{
	blob_mem* p_blob_mem = (blob_mem*)p;

	return (wire_type == p_blob_mem->type &&
			value_size == p_blob_mem->sz &&
			memcmp(wire_value, p_blob_mem->data, value_size) == 0) ? 0 : 1;
}

uint32_t
blob_wire_size(const as_particle* p)
{
	blob_mem* p_blob_mem = (blob_mem*)p;

	return p_blob_mem->sz;
}

uint32_t
blob_to_wire(const as_particle* p, uint8_t* wire)
{
	blob_mem* p_blob_mem = (blob_mem*)p;

	memcpy(wire, p_blob_mem->data, p_blob_mem->sz);

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
blob_size_from_asval(const as_val* val)
{
	return (uint32_t)sizeof(blob_mem) + as_bytes_size(as_bytes_fromval(val));
}

void
blob_from_asval(const as_val* val, as_particle** pp)
{
	blob_mem* p_blob_mem = (blob_mem*)*pp;

	as_bytes* bytes = as_bytes_fromval(val);

	p_blob_mem->type = (uint8_t)blob_bytes_type_to_particle_type(bytes->type);
	p_blob_mem->sz = as_bytes_size(bytes);
	memcpy(p_blob_mem->data, as_bytes_get(bytes), p_blob_mem->sz);
}

as_val*
blob_to_asval(const as_particle* p)
{
	blob_mem* p_blob_mem = (blob_mem*)p;

	uint8_t* value = cf_malloc(p_blob_mem->sz);

	memcpy(value, p_blob_mem->data, p_blob_mem->sz);

	return (as_val*)as_bytes_new_wrap(value, p_blob_mem->sz, true);
}

uint32_t
blob_asval_wire_size(const as_val* val)
{
	return as_bytes_size(as_bytes_fromval(val));
}

uint32_t
blob_asval_to_wire(const as_val* val, uint8_t* wire)
{
	as_bytes* bytes = as_bytes_fromval(val);
	uint32_t size = as_bytes_size(bytes);

	memcpy(wire, as_bytes_get(bytes), size);

	return size;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
blob_size_from_msgpack(const uint8_t* packed, uint32_t packed_size)
{
	(void)packed;
	// Ok to oversize by a few bytes - only used for allocation sizing.
	// -1 for blob internal type and -1 for blob header.
	return (uint32_t)sizeof(blob_mem) + packed_size - 2;
}

void
blob_from_msgpack(const uint8_t* packed, uint32_t packed_size, as_particle** pp)
{
	msgpack_in mp = {
			.buf = packed,
			.buf_sz = packed_size
	};

	uint32_t size;
	const uint8_t* ptr = msgpack_get_bin(&mp, &size);//pk.buffer + pk.offset;

	cf_assert(ptr != NULL, AS_PARTICLE, "invalid msgpack");

	uint8_t type = *ptr;

	// Adjust for type (1 byte).
	ptr++;
	size--;

	blob_mem* p_blob_mem = (blob_mem*)*pp;

	p_blob_mem->type = (uint8_t)blob_bytes_type_to_particle_type(
			(as_bytes_type)type);
	p_blob_mem->sz = size;
	memcpy(p_blob_mem->data, ptr, p_blob_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t*
blob_skip_flat(const uint8_t* flat, const uint8_t* end)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	blob_flat* p_blob_flat = (blob_flat*)flat;

	return flat + sizeof(blob_flat) + p_blob_flat->size;
}

const uint8_t*
blob_cast_from_flat(const uint8_t* flat, const uint8_t* end, as_particle** pp)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	const blob_flat* p_blob_flat = (const blob_flat*)flat;

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle*)p_blob_flat;

	return flat + sizeof(blob_flat) + p_blob_flat->size;
}

const uint8_t*
blob_from_flat(const uint8_t* flat, const uint8_t* end, as_particle** pp)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	const blob_flat* p_blob_flat = (const blob_flat*)flat;

	// Flat value is same as in-memory value.
	size_t mem_size = sizeof(blob_mem) + p_blob_flat->size;

	flat += mem_size; // blob_mem same size as blob_flat

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	blob_mem* p_blob_mem = (blob_mem*)cf_malloc_ns(mem_size);

	p_blob_mem->type = p_blob_flat->type;
	p_blob_mem->sz = p_blob_flat->size;
	memcpy(p_blob_mem->data, p_blob_flat->data, p_blob_mem->sz);

	*pp = (as_particle*)p_blob_mem;

	return flat;
}

uint32_t
blob_flat_size(const as_particle* p)
{
	return (uint32_t)(sizeof(blob_flat) + ((blob_mem*)p)->sz);
}

uint32_t
blob_to_flat(const as_particle* p, uint8_t* flat)
{
	blob_mem* p_blob_mem = (blob_mem*)p;
	blob_flat* p_blob_flat = (blob_flat*)flat;

	// Already wrote the type.
	p_blob_flat->size = p_blob_mem->sz;
	memcpy(p_blob_flat->data, p_blob_mem->data, p_blob_flat->size);

	return blob_flat_size(p);
}


//==========================================================
// as_bin particle functions specific to BLOB.
//

int
as_bin_bits_packed_read(const as_bin* b, const as_msg_op* msg_op, as_bin* rb)
{
	cf_assert(as_bin_inuse(b), AS_PARTICLE, "unused bin");

	if ((as_particle_type)msg_op->particle_type != AS_PARTICLE_TYPE_BLOB) {
		cf_warning(AS_PARTICLE, "as_bin_bits_packed_read - error %u unexpected particle type %u for bin %.*s",
				AS_ERR_INCOMPATIBLE_TYPE, msg_op->particle_type,
				(int)msg_op->name_sz, msg_op->name);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	bits_state state = { 0 };

	if (! bits_state_init(&state, msg_op, true)) {
		return -AS_ERR_PARAMETER;
	}

	bits_op op = { 0 };

	if (! bits_parse_op(&state, &op)) {
		return -AS_ERR_PARAMETER;
	}

	if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_BLOB) {
		cf_warning(AS_PARTICLE, "as_bin_bits_packed_read - error %u operation (%s) on bin %.*s bin type must be blob found %u",
				AS_ERR_INCOMPATIBLE_TYPE, state.def->name, (int)msg_op->name_sz,
				msg_op->name, as_bin_get_particle_type(b));
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	int result = bits_prepare_read(&state, &op, b);

	if (result < 1) {
		return result;
	}

	if (! bits_execute_read_op(&state, &op, b->particle, rb)) {
		as_bin_set_empty(rb);
	}

	return AS_OK;
}

int
as_bin_bits_packed_modify(as_bin* b, const as_msg_op* msg_op,
		cf_ll_buf* particles_llb)
{
	if ((as_particle_type)msg_op->particle_type != AS_PARTICLE_TYPE_BLOB) {
		cf_warning(AS_PARTICLE, "as_bin_bits_packed_modify - error %u unexpected particle type %u for bin %.*s",
				AS_ERR_INCOMPATIBLE_TYPE, msg_op->particle_type,
				(int)msg_op->name_sz, msg_op->name);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	bits_state state = { 0 };

	if (! bits_state_init(&state, msg_op, false)) {
		return -AS_ERR_PARAMETER;
	}

	bits_op op = { 0 };

	if (! bits_parse_op(&state, &op)) {
		return -AS_ERR_PARAMETER;
	}

	as_particle* old_blob;

	if (as_bin_inuse(b)) {
		if ((op.flags & AS_BITS_FLAG_CREATE_ONLY) != 0) {
			if ((op.flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "as_bin_bits_packed_modify - error %u operation (%s) on bin %.*s would update - not allowed",
					AS_ERR_BIN_EXISTS, state.def->name, (int)msg_op->name_sz,
					msg_op->name);
			return -AS_ERR_BIN_EXISTS;
		}

		if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_BLOB) {
			cf_warning(AS_PARTICLE, "as_bin_bits_packed_modify - error %u operation (%s) on bin %.*s must be on a blob - found %u",
					AS_ERR_INCOMPATIBLE_TYPE, state.def->name,
					(int)msg_op->name_sz, msg_op->name,
					as_bin_get_particle_type(b));
			return -AS_ERR_INCOMPATIBLE_TYPE;
		}
		// else - there is an existing blob particle, which we will modify.

		old_blob = b->particle;
	}
	else {
		if ((state.op_type != AS_BITS_OP_INSERT &&
				state.op_type != AS_BITS_OP_RESIZE) ||
				(op.flags & AS_BITS_FLAG_UPDATE_ONLY) != 0) {
			if ((op.flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "as_bin_bits_packed_modify - error %u operation (%s) on bin %.*s would create - not allowed",
					AS_ERR_BIN_NOT_FOUND, state.def->name, (int)msg_op->name_sz,
					msg_op->name);
			return -AS_ERR_BIN_NOT_FOUND;
		}

		cf_assert(b->particle == NULL, AS_PARTICLE, "particle not null");

		old_blob = NULL;
	}

	int result = bits_prepare_modify(&state, &op, b);

	if (result < 1) {
		return result;
	}

	size_t alloc_size = sizeof(blob_mem) + (size_t)state.new_size;

	if (particles_llb == NULL) {
		b->particle = cf_malloc_ns(alloc_size);
	}
	else {
		cf_ll_buf_reserve(particles_llb, alloc_size, (uint8_t**)&b->particle);
	}

	if (! bits_execute_modify_op(&state, &op, old_blob, b->particle)) {
		if (particles_llb == NULL) {
			cf_free(b->particle);
		}

		b->particle = old_blob;

		if ((op.flags & AS_BITS_FLAG_NO_FAIL) == 0) {
			cf_warning(AS_PARTICLE, "as_bin_bits_packed_modify - error %u operation (%s) unable to apply operation",
					AS_ERR_OP_NOT_APPLICABLE, state.def->name);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		return AS_OK;
	}

	if (old_blob == NULL) {
		// Set the bin's state member.
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_BLOB);
	}

	return AS_OK;
}


//==========================================================
// Local helpers - bits parsing.
//

static bool
bits_state_init(bits_state* state, const as_msg_op* msg_op, bool is_read)
{
	const uint8_t* data = msg_op->name + msg_op->name_sz;
	uint32_t sz = msg_op->op_sz - (uint32_t)OP_FIXED_SZ - msg_op->name_sz;

	state->pk.buf = data;
	state->pk.buf_sz = sz;
	state->pk.offset = 0;

	uint32_t n_args;

	if (! msgpack_get_list_ele_count(&state->pk, &n_args) || n_args == 0) {
		cf_warning(AS_PARTICLE, "bits_state_init - error %u bin %.*s insufficient args (%u) or unable to parse args",
				AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name, n_args);
		return false;
	}

	state->n_args = n_args - 1; // ignore the 'op' arg.

	uint64_t type64;

	if (! msgpack_get_uint64(&state->pk, &type64)) {
		cf_warning(AS_PARTICLE, "bits_state_init - error %u bin %.*s unable to parse op",
				AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name);
		return false;
	}

	state->op_type = (as_bits_op_type)type64;

	if (is_read) {
		if (! (state->op_type >= AS_BITS_READ_OP_START &&
				state->op_type < AS_BITS_READ_OP_END)) {
			cf_warning(AS_PARTICLE, "bits_state_init - error %u bin %.*s op %u expected read op",
					AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name,
					state->op_type);
			return false;
		}

		state->def = (bits_op_def*)&bits_read_op_table[state->op_type];
	}
	else {
		if (! (state->op_type >= AS_BITS_MODIFY_OP_START &&
				state->op_type < AS_BITS_MODIFY_OP_END)) {
			cf_warning(AS_PARTICLE, "bits_state_init - error %u bin %.*s op %u expected modify op",
					AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name,
					state->op_type);
			return false;
		}

		state->def = (bits_op_def*)&bits_modify_op_table[state->op_type];
	}

	return true;
}

static bool
bits_parse_op(bits_state* state, bits_op* op)
{
	bits_op_def* def = state->def;

	if (state->n_args < def->min_args || state->n_args > def->max_args) {
		cf_warning(AS_PARTICLE, "bits_parse_op - error %u op %s (%u) unexpected number of args %u for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->n_args, state->def->name);
		return false;
	}

	for (uint32_t i = 0; i < state->n_args; i++) {
		if (! def->args[i](state, op)) {
			return false;
		}
	}

	return true;
}

static bool
bits_parse_byte_offset(bits_state* state, bits_op* op)
{
	int64_t offset;

	if (! msgpack_get_int64(&state->pk, &offset)) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_offset - error %u op %s (%u) unable to parse offset",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (labs(offset) > PROTO_SIZE_MAX) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_offset - error %u op %s (%u) offset (%ld) larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, offset,
				PROTO_SIZE_MAX);
		return false;
	}

	op->offset = (int32_t)offset * 8; // normalize to bits

	return true;
}

static bool
bits_parse_offset(bits_state* state, bits_op* op)
{
	int64_t offset;

	if (! msgpack_get_int64(&state->pk, &offset)) {
		cf_warning(AS_PARTICLE, "bits_parse_offset - error %u op %s (%u) unable to parse offset",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (labs(offset) > PROTO_SIZE_MAX * 8) {
		cf_warning(AS_PARTICLE, "bits_parse_offset - error %u op %s (%u) offset (%ld) is larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, offset,
				PROTO_SIZE_MAX * 8);
		return false;
	}

	op->offset = (int32_t)offset;

	return true;
}

static bool
bits_parse_integer_size(bits_state* state, bits_op* op)
{
	uint64_t size;

	if (! msgpack_get_uint64(&state->pk, &size)) {
		cf_warning(AS_PARTICLE, "bits_parse_integer_size - error %u op %s (%u) unable to parse byte_size",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size == 0) {
		cf_warning(AS_PARTICLE, "bits_parse_integer_size - error %u op %s (%u) size may not be 0",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size > 64) {
		cf_warning(AS_PARTICLE, "bits_parse_integer_size - error %u op %s (%u) size (%lu) larger than max (64)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size);
		return false;
	}

	op->size = (uint32_t)size;

	return true;
}

static bool
bits_parse_byte_size(bits_state* state, bits_op* op)
{
	uint64_t size;

	if (! msgpack_get_uint64(&state->pk, &size)) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_size - error %u op %s (%u) unable to parse byte_size",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size == 0) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_size - error %u op %s (%u) size may not be 0",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size > PROTO_SIZE_MAX) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_size - error %u op %s (%u) size (%lu) larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size,
				PROTO_SIZE_MAX);
		return false;
	}

	op->size = (uint32_t)size * 8; // normalize to bits

	return true;
}

static bool
bits_parse_byte_size_allow_zero(bits_state* state, bits_op* op)
{
	uint64_t size;

	if (! msgpack_get_uint64(&state->pk, &size)) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_size_allow_zero - error %u op %s (%u) unable to parse byte_size",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size > PROTO_SIZE_MAX) {
		cf_warning(AS_PARTICLE, "bits_parse_byte_size_allow_zero - error %u op %s (%u) size (%lu) larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size,
				PROTO_SIZE_MAX);
		return false;
	}

	op->size = (uint32_t)size * 8; // normalize to bits

	return true;
}

static bool
bits_parse_size(bits_state* state, bits_op* op)
{
	uint64_t size;

	if (! msgpack_get_uint64(&state->pk, &size)) {
		cf_warning(AS_PARTICLE, "bits_parse_size - error %u op %s (%u) unable to parse size",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size == 0) {
		cf_warning(AS_PARTICLE, "bits_parse_size - error %u op %s (%u) size may not be 0",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if (size > PROTO_SIZE_MAX * 8) {
		cf_warning(AS_PARTICLE, "bits_parse_size - error %u op %s (%u) size (%lu) is larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size,
				PROTO_SIZE_MAX * 8);
		return false;
	}

	op->size = (uint32_t)size;

	return true;
}

static bool
bits_parse_boolean_value(bits_state* state, bits_op* op)
{
	bool value;

	if (! msgpack_get_bool(&state->pk, &value)) {
		cf_warning(AS_PARTICLE, "bits_parse_boolean_value - error %u op %s (%u) unable to parse boolean",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	op->value = value ? 1 : 0;

	return true;
}

static bool
bits_parse_n_bits_value(bits_state* state, bits_op* op)
{
	if (! bits_parse_integer_value(state, op)) {
		return false;
	}

	if (op->value > PROTO_SIZE_MAX * 8) {
		cf_warning(AS_PARTICLE, "bits_parse_n_bits_value - error %u op %s (%u) n_bits value (%lu) is larger than max (%d)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->value,
				PROTO_SIZE_MAX * 8);
		return false;
	}

	return true;
}

static bool
bits_parse_integer_value(bits_state* state, bits_op* op)
{
	if (! msgpack_get_uint64(&state->pk, &op->value)) {
		cf_warning(AS_PARTICLE, "bits_parse_integer_value - error %u op %s (%u) unable to parse number",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	return true;
}

static bool
bits_parse_buf(bits_state* state, bits_op* op)
{
	uint32_t size;

	op->buf = msgpack_get_bin(&state->pk, &size);

	// AS msgpack has a one byte blob type field which we ignore here.
	if (op->buf == NULL || size == 1) {
		cf_warning(AS_PARTICLE, "bits_parse_buf - error %u op %s (%u) parsed invalid buffer with size %u",
				AS_ERR_PARAMETER, state->def->name, state->op_type, size);
		return false;
	}

	op->buf++;
	size = (size - 1) * 8;

	if (op->size != 0) {
		if (size < op->size) {
			cf_warning(AS_PARTICLE, "bits_parse_buf - error %u op %s (%u) parsed buffer size less than size buf_sz %u sz %u",
					AS_ERR_PARAMETER, state->def->name, state->op_type, size,
					op->size);
			return false;
		}
	}
	else {
		op->size = size;
	}

	return true;
}

static bool
bits_parse_flags(bits_state* state, bits_op* op)
{
	if (! msgpack_get_uint64(&state->pk, &op->flags)) {
		cf_warning(AS_PARTICLE, "bits_parse_flags - error %u op %s (%u) unable to parse subflags",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	if ((op->flags & state->def->bad_flags) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_flags - error %u op %s (%u) invalid flags (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags);
		return false;
	}

	if ((op->flags & AS_BITS_FLAG_CREATE_ONLY) != 0 &&
			(op->flags & AS_BITS_FLAG_UPDATE_ONLY) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_flags - error %u op %s (%u) invalid flags combination (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags);
		return false;
	}

	return true;
}

static bool
bits_parse_resize_subflags(bits_state* state, bits_op* op)
{
	if (! bits_parse_subflags(state, op)) {
		return false;
	}

	uint64_t bad_flags = (uint64_t)~(AS_BITS_SUBFLAG_RESIZE_FROM_FRONT |
			AS_BITS_SUBFLAG_RESIZE_GROW_ONLY |
			AS_BITS_SUBFLAG_RESIZE_SHRINK_ONLY);

	if ((op->subflags & bad_flags) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_resize_subflags - error %u op %s (%u) invalid subflags (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->subflags);
		return false;
	}

	if ((op->subflags & AS_BITS_SUBFLAG_RESIZE_GROW_ONLY) != 0 &&
			(op->subflags & AS_BITS_SUBFLAG_RESIZE_SHRINK_ONLY) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_resize_subflags - error %u op %s (%u) invalid subflags combination (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->subflags);
		return false;
	}

	return true;
}

static bool
bits_parse_arithmetic_subflags(bits_state* state, bits_op* op)
{
	if (! bits_parse_subflags(state, op)) {
		return false;
	}

	uint64_t bad_flags = (uint64_t)~(AS_BITS_INT_SUBFLAG_SIGNED |
			AS_BITS_INT_SUBFLAG_SATURATE | AS_BITS_INT_SUBFLAG_WRAP);

	if ((op->subflags & bad_flags) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_arithmetic_subflags - error %u op %s (%u) invalid subflags (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->subflags);
		return false;
	}

	if ((op->subflags & AS_BITS_INT_SUBFLAG_SATURATE) != 0 &&
			(op->subflags & AS_BITS_INT_SUBFLAG_WRAP) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_arithmetic_subflags - error %u op %s (%u) invalid subflags combination (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->subflags);
		return false;
	}

	return true;
}

static bool
bits_parse_get_integer_subflags(bits_state* state, bits_op* op)
{
	if (! bits_parse_subflags(state, op)) {
		return false;
	}

	uint64_t bad_flags = (uint64_t)~(AS_BITS_INT_SUBFLAG_SIGNED);

	if ((op->subflags & bad_flags) != 0) {
		cf_warning(AS_PARTICLE, "bits_parse_get_integer_subflags - error %u op %s (%u) invalid subflags (0x%lx)",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->subflags);
		return false;
	}

	return true;
}

static bool
bits_parse_subflags(bits_state* state, bits_op* op)
{
	if (! msgpack_get_uint64(&state->pk, &op->subflags)) {
		cf_warning(AS_PARTICLE, "bits_parse_subflags - error %u op %s (%u) unable to parse subflags",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	return true;
}


//==========================================================
// Local helpers - prepare ops.
//

static int
bits_prepare_read(bits_state* state, bits_op* op, const as_bin* b)
{
	return bits_prepare_op(state, op, b);
}

static int
bits_prepare_modify(bits_state* state, bits_op* op, const as_bin* b)
{
	int result = bits_prepare_op(state, op, b);

	if (result != 1) {
		return result;
	}

	state->new_size = state->n_bytes_head + state->n_bytes_expand +
			state->n_bytes_tail;

	if (state->action != OP_ACTION_REMOVE) {
		state->new_size += state->n_bytes_op;
	}

	as_bits_op_type op_type = state->op_type;

	cf_assert(op_type == AS_BITS_OP_RESIZE || op_type == AS_BITS_OP_INSERT ||
			op_type == AS_BITS_OP_REMOVE || state->old_size == state->new_size,
			AS_PARTICLE, "size changed op %u old %u new %u",
			op_type, state->old_size, state->new_size);

	if (state->new_size >= PROTO_SIZE_MAX) {
		cf_warning(AS_PARTICLE, "bits_prepare_op - error %u op %s (%u) result blob size %u is larger than max %u",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->new_size, PROTO_SIZE_MAX);
		return -AS_ERR_PARAMETER;
	}

	return 1; // success
}

static int
bits_prepare_op(bits_state* state, bits_op* op, const as_bin* b)
{
	bits_op_def* def = state->def;

	if (as_bin_inuse(b)) {
		state->old_size = ((blob_mem*)b->particle)->sz;
	}
	else if (op->offset < 0) {
		cf_warning(AS_PARTICLE, "bits_prepare_op - error %u op %s (%u) cannot use negative offset on non-existent bin",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return -AS_ERR_PARAMETER;
	}

	int32_t byte_offset = bits_normalize_offset(state, op);

	if (byte_offset < 0) {
		return byte_offset;
	}

	uint32_t op_size = ((uint32_t)op->offset + op->size + 7) / 8;

	return def->prepare(state, op, (uint32_t)byte_offset, op_size);
}

static int32_t
bits_normalize_offset(const bits_state* state, bits_op* op)
{
	if (op->offset < 0) {
		if ((uint32_t)abs(op->offset) > state->old_size * 8) {
			return -AS_ERR_PARAMETER;
		}

		op->offset = (int32_t)(state->old_size * 8) + op->offset;
	}

	uint32_t byte_offset = (uint32_t)op->offset / 8;

	op->offset %= 8;

	return (int32_t)byte_offset;
}

static int
bits_prepare_resize_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	(void)byte_offset;

	bool from_front = (op->subflags & AS_BITS_SUBFLAG_RESIZE_FROM_FRONT) != 0;
	bool grow_only = (op->subflags & AS_BITS_SUBFLAG_RESIZE_GROW_ONLY) != 0;
	bool shrink_only = (op->subflags & AS_BITS_SUBFLAG_RESIZE_SHRINK_ONLY) != 0;

	if (op_size < state->old_size) {
		if (grow_only) {
			if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "bits_prepare_resize_op - error %u op %s (%u) cannot shrink with grow_only set",
					AS_ERR_PARAMETER, state->def->name, state->op_type);
			return -AS_ERR_PARAMETER;
		}

		uint32_t n_bytes_remove = state->old_size - op_size;

		state->action = OP_ACTION_REMOVE;

		if (from_front) {
			state->n_bytes_op = n_bytes_remove;
			state->n_bytes_tail = state->old_size - n_bytes_remove;
		}
		else {
			state->n_bytes_head = state->old_size - n_bytes_remove;
			state->n_bytes_op = n_bytes_remove;
		}
	}
	else if (op_size > state->old_size) {
		if (shrink_only) {
			if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "bits_prepare_resize_op - error %u op %s (%u) cannot grow with shrink_only set",
					AS_ERR_PARAMETER, state->def->name, state->op_type);
			return -AS_ERR_PARAMETER;
		}

		uint32_t n_bytes_add = op_size - state->old_size;

		state->action = OP_ACTION_EXPAND;

		if (from_front) {
			state->n_bytes_op = n_bytes_add;
			state->n_bytes_tail = state->old_size;
		}
		else {
			state->n_bytes_head = state->old_size;
			state->n_bytes_op = n_bytes_add;
		}
	}
	else {
		return AS_OK; // already the specified size - no-op
	}

	return 1;
}

static int
bits_prepare_insert_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	(void)op;

	state->action = OP_ACTION_EXPAND;

	if (byte_offset >= state->old_size) {
		state->n_bytes_head = state->old_size;
		state->n_bytes_expand = byte_offset - state->old_size;
		state->n_bytes_op = op_size;
	}
	else {
		state->n_bytes_head = byte_offset;
		state->n_bytes_op = op_size;
		state->n_bytes_tail = state->old_size - byte_offset;
	}

	return 1;
}

static int
bits_prepare_remove_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	if (byte_offset >= state->old_size) {
		if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
			return AS_OK;
		}

		cf_warning(AS_PARTICLE, "bits_prepare_remove_op - error %u op %s (%u) tried to remove past the end of blob",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return -AS_ERR_PARAMETER;
	}

	state->action = OP_ACTION_REMOVE;

	if (byte_offset + op_size > state->old_size) {
		if ((op->flags & AS_BITS_FLAG_PARTIAL) == 0) {
			if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "bits_prepare_remove_op - error %u op %s (%u) remove size extended past end of blob without the partial policy",
					AS_ERR_PARAMETER, state->def->name, state->op_type);
			return -AS_ERR_PARAMETER;
		}

		state->n_bytes_head = byte_offset;
		state->n_bytes_op= state->old_size - byte_offset;
		op->size = (state->old_size * 8) - (uint32_t)op->offset;
	}
	else {
		state->n_bytes_head = byte_offset;
		state->n_bytes_op= op_size;
		state->n_bytes_tail = state->old_size - (byte_offset + op_size);
	}

	return 1;
}

static int
bits_prepare_modify_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	if (byte_offset >= state->old_size) {
		if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
			return AS_OK;
		}

		cf_warning(AS_PARTICLE, "bits_prepare_modify_op - error %u op %s (%u) operation may not expand blob",
				AS_ERR_OP_NOT_APPLICABLE, state->def->name, state->op_type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	state->action = OP_ACTION_OVERWRITE;

	if (byte_offset + op_size > state->old_size) {
		if ((op->flags & AS_BITS_FLAG_PARTIAL) == 0) {
			if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "bits_prepare_modify_op - error %u op %s (%u) operation too large - either use partial or increase size of blob",
					AS_ERR_OP_NOT_APPLICABLE, state->def->name, state->op_type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		state->n_bytes_head = byte_offset;
		state->n_bytes_op = state->old_size - byte_offset;
		op->size = (state->old_size * 8) - (uint32_t)op->offset;
	}
	else {
		state->n_bytes_head = byte_offset;
		state->n_bytes_op = op_size;
		state->n_bytes_tail = state->old_size - (byte_offset + op_size);
	}

	return 1;
}

static int
bits_prepare_integer_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	state->action = OP_ACTION_OVERWRITE;

	return bits_prepare_read_op(state, op, byte_offset, op_size);
}

static int
bits_prepare_read_op(bits_state* state, bits_op* op, uint32_t byte_offset,
		uint32_t op_size)
{
	if (byte_offset >= state->old_size) {
		if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
			return AS_OK;
		}

		cf_warning(AS_PARTICLE, "bits_prepare_read_op - error %u op %s (%u) operation would expand blob",
				AS_ERR_OP_NOT_APPLICABLE, state->def->name, state->op_type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (byte_offset + op_size > state->old_size) {
		if ((op->flags & AS_BITS_FLAG_NO_FAIL) != 0) {
			return AS_OK;
		}

		cf_warning(AS_PARTICLE, "bits_prepare_read_op - error %u op %s (%u) operation too large for blob",
				AS_ERR_OP_NOT_APPLICABLE, state->def->name, state->op_type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	state->n_bytes_head = byte_offset;
	state->n_bytes_op = op_size;
	state->n_bytes_tail = state->old_size - (byte_offset + op_size);

	return 1;
}


//==========================================================
// Local helpers - execute ops.
//

static bool
bits_execute_modify_op(const bits_state* state, const bits_op* op,
		const as_particle* old_blob, as_particle* new_blob)
{
	uint8_t* to = ((blob_mem*)new_blob)->data;
	const uint8_t* end_to = to + state->new_size; // paranoia

	if (old_blob != NULL) {
		const uint8_t* from = ((const blob_mem*)old_blob)->data;
		const uint8_t* end_from = from + state->old_size; // paranoia

		if (state->n_bytes_head != 0) {
			memcpy(to, from, state->n_bytes_head);
			to += state->n_bytes_head;
			from += state->n_bytes_head;
		}

		if (state->action == OP_ACTION_OVERWRITE) {
			if (! state->def->fn.modify(op, to, from, state->n_bytes_op)) {
				return false;
			}

			to += state->n_bytes_op;
			from += state->n_bytes_op;
		}
		else if (state->action == OP_ACTION_EXPAND) {
			if (state->n_bytes_expand != 0) {
				memset(to, 0, state->n_bytes_expand);
				to += state->n_bytes_expand;
			}

			if (! state->def->fn.modify(op, to, NULL, state->n_bytes_op)) {
				return false;
			}

			to += state->n_bytes_op;
		}
		else { // OP_ACTION_REMOVE
			from += state->n_bytes_op;
		}

		if (state->n_bytes_tail != 0) {
			memcpy(to, from, state->n_bytes_tail);
			to += state->n_bytes_tail;
			from += state->n_bytes_tail;
		}

		// Paranoia.
		cf_assert(to == end_to && from == end_from, AS_PARTICLE,
				"either to or from didn't stop at end - to (%p %p) from (%p %p)",
				to, end_to, from, end_from);
	}
	else {
		memset(to, 0, state->new_size);
		to += state->n_bytes_expand;

		cf_assert(state->action == OP_ACTION_EXPAND, AS_PARTICLE,
				"creating a blob expected expand action - found %u",
				state->action);

		if (! state->def->fn.modify(op, to, NULL, state->n_bytes_op)) {
			return false;
		}

		to += state->n_bytes_op; // paranoia

		// Paranoia.
		cf_assert(to == end_to, AS_PARTICLE, "to didn't stop at end - to (%p %p)",
				to, end_to);
	}

	((blob_mem*)new_blob)->sz = state->new_size;
	((blob_mem*)new_blob)->type = AS_PARTICLE_TYPE_BLOB;

	return true;
}

static bool
bits_execute_read_op(const bits_state* state, const bits_op* op,
		const as_particle* blob, as_bin* rb)
{
	const uint8_t* from = ((const blob_mem*)blob)->data + state->n_bytes_head;

	return state->def->fn.read(op, from, rb, state->n_bytes_op);
}


//==========================================================
// Local helpers - bits modify ops.
//

static bool
bits_modify_op_resize(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	(void)op;
	(void)from;

	// Note - this function isn't called when sizing down.

	memset(to, 0, n_bytes);

	return true;
}

static bool
bits_modify_op_insert(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	(void)from;

	memcpy(to, op->buf, n_bytes);

	return true;
}

static bool
bits_modify_op_set(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	const bits_op cmd = {
		.size = ((op->size + 7) / 8) * 8,
		.value = (uint64_t)op->offset
	};

	rshift(&cmd, to, op->buf, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_or(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	const bits_op cmd = {
		.size = op->size,
		.value = (uint64_t)op->offset,
		.buf = op->buf
	};

	rshift_with_or(&cmd, to, from);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_xor(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	const bits_op cmd = {
		.size = op->size,
		.value = (uint64_t)op->offset,
		.buf = op->buf
	};

	rshift_with_xor(&cmd, to, from);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_and(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	const bits_op cmd = {
		.size = op->size,
		.value = (uint64_t)op->offset,
		.buf = op->buf
	};

	rshift_with_and(&cmd, to, from);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_not(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	uint8_t* orig_to = &to[0];
	const uint8_t* orig_from = &from[0];
	const uint8_t* cur = &from[0];
	const uint8_t* end = &from[n_bytes];

	while (cur < end) {
		*to++ = (uint8_t)~*cur++;
	}

	restore_ends(op, orig_to, orig_from, n_bytes);

	return true;
}

static bool
bits_modify_op_lshift(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	lshift(op, to, from, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_rshift(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	rshift(op, to, from, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_add(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	uint64_t load = load_int(op, from, n_bytes);
	uint32_t n_bits = op->size;
	uint32_t leading = 64 - n_bits;
	uint64_t value_m =  0xFFFFffffFFFFffff >> leading;
	uint64_t value = op->value & value_m;

	uint64_t result = (load + value) & value_m;
	bool is_signed = (op->subflags & AS_BITS_INT_SUBFLAG_SIGNED) != 0;
	bool is_wrap = (op->subflags & AS_BITS_INT_SUBFLAG_WRAP) != 0;
	bool is_saturate = (op->subflags & AS_BITS_INT_SUBFLAG_SATURATE) != 0;

	if (! is_wrap) {
		if (is_signed) {
			if (! handle_signed_overflow(n_bits, is_saturate, load, value,
					&result)) {
				return false;
			}
		}
		else if (result < load) {
			if (is_saturate) {
				result = value_m;
			}
			else {
				return false;
			}
		}
	}

	store_int(op, to, result, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_subtract(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	uint64_t load = load_int(op, from, n_bytes);
	uint32_t n_bits = op->size;
	uint32_t leading = 64 - n_bits;
	uint64_t value_m =  0xFFFFffffFFFFffff >> leading;
	uint64_t value = op->value & value_m;

	uint64_t result = (load - value) & value_m;
	bool is_signed = (op->subflags & AS_BITS_INT_SUBFLAG_SIGNED) != 0;
	bool is_wrap = (op->subflags & AS_BITS_INT_SUBFLAG_WRAP) != 0;
	bool is_saturate = (op->subflags & AS_BITS_INT_SUBFLAG_SATURATE) != 0;

	if (! is_wrap) {
		if (is_signed) {
			if (! handle_signed_overflow(n_bits, is_saturate, load,
					(uint64_t)((int64_t)value * -1), &result)) {
				return false;
			}
		}
		else if (result > load) {
			if (is_saturate) {
				result = 0;
			}
			else {
				return false;
			}
		}
	}

	store_int(op, to, result, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}

static bool
bits_modify_op_set_int(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	uint64_t value_m =  0xFFFFffffFFFFffff >> (64 - op->size);
	uint64_t value = op->value & value_m;

	store_int(op, to, value, n_bytes);
	restore_ends(op, to, from, n_bytes);

	return true;
}


//==========================================================
// Local helpers - bits read ops.
//

static bool
bits_read_op_get(const bits_op* op, const uint8_t* from, as_bin* rb,
		uint32_t n_bytes)
{
	uint32_t answer_sz = (op->size + 7) / 8;

	const bits_op cmd = {
		.size = answer_sz * 8,
		.value = (uint64_t)op->offset
	};

	blob_mem* answer = cf_malloc(sizeof(blob_mem) + n_bytes);

	lshift(&cmd, answer->data, from, n_bytes);

	// Zero end.
	uint32_t size = op->size % 8;

	if (size != 0) {
		uint8_t from_tail_m = (uint8_t)(0xFF << (8 - size));

		answer->data[answer_sz - 1] &= from_tail_m;
	}

	answer->sz = answer_sz;
	answer->type = AS_PARTICLE_TYPE_BLOB;
	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BLOB);

	return true;
}

static bool
bits_read_op_count(const bits_op* op, const uint8_t* from, as_bin* rb,
		uint32_t n_bytes)
{
	const uint8_t* end = &from[n_bytes - 1];
	const uint8_t* cur = &from[0];
	uint8_t head_m = (uint8_t)(0xFF << (8 - op->offset));
	uint32_t tail_len = (op->size + (uint32_t)op->offset) % 8;
	uint8_t tail_m = (uint8_t)(tail_len != 0 ? 0xFF >> tail_len : 0x00);
	uint64_t answer = 0;

	if (n_bytes == 1) {
		uint8_t m = (uint8_t)~(head_m | tail_m);

		answer = cf_bit_count64((uint64_t)(*cur & m));
	}
	else {
		answer = cf_bit_count64((uint64_t)(*cur++ & ~head_m));

		while (cur < end && (cur - from < 8 || (uint64_t)cur % 8 != 0)) {
			answer += cf_bit_count64(*cur++);
		}

		while (end - cur >= 64) {
			for (uint32_t i = 0; i < 8; i++) {
				answer += cf_bit_count64(*(uint64_t*)cur);
				cur += 8;
			}
		}

		while (cur < end) {
			answer += cf_bit_count64((uint64_t)*cur++);
		}

		answer += cf_bit_count64((uint64_t)(*end & ~tail_m));
	}

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return true;
}

static bool
bits_read_op_lscan(const bits_op* op, const uint8_t* from, as_bin* rb,
		uint32_t n_bytes)
{
	const uint8_t* end = &from[n_bytes - 1];
	const uint8_t* cur = from;
	uint8_t head_m = (uint8_t)(0xFF >> op->offset);
	uint8_t last = (uint8_t)(op->value == 1 ? *cur & head_m : ~*cur & head_m);
	uint8_t skip = (uint8_t)(op->value == 1 ? 0x00 : 0xFF);

	if (last == 0 && n_bytes != 1) {
		cur++;

		while (*cur == skip && cur < end) {
			cur++;
		}

		last = (uint8_t)(op->value == 1 ? *cur : ~*cur);
	}

	int64_t answer;

	if (last != 0) {
		uint32_t bit_ix = 0;
		uint8_t m = (uint8_t)0x80;

		while ((last & (m >> bit_ix)) == 0) {
			bit_ix++;
		}

		answer = ((cur - from) * 8 + bit_ix) - op->offset;

		if (answer >= op->size) {
			answer = -1;
		}
	}
	else {
		answer = -1;
	}

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return true;
}

static bool
bits_read_op_rscan(const bits_op* op, const uint8_t* from, as_bin* rb,
		uint32_t n_bytes)
{
	const uint8_t* cur = &from[n_bytes - 1];
	uint8_t head_m = (uint8_t)(0xFF << ((8 - (op->size +
			(uint32_t)op->offset) % 8) % 8));
	uint8_t last = (uint8_t)(op->value == 1 ? *cur & head_m : ~*cur & head_m);
	uint8_t skip = (uint8_t)(op->value == 1 ? 0x00 : 0xFF);

	if (last == 0 && n_bytes != 1) {
		cur--;

		while (*cur == skip && cur > from) {
			cur--;
		}

		last = (uint8_t)(op->value == 1 ? *cur : ~*cur);
	}

	int64_t answer;

	if (last != 0) {
		uint32_t bit_ix = 7;
		uint8_t m = (uint8_t)0x80;

		while ((last & (m >> bit_ix)) == 0) {
			bit_ix--;
		}

		answer = ((cur - from) * 8 + bit_ix) - op->offset;

		if (answer >= op->size) {
			answer = -1;
		}
	}
	else {
		answer = -1;
	}

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return true;
}

static bool
bits_read_op_get_integer(const bits_op* op, const uint8_t* from, as_bin* rb,
		uint32_t n_bytes)
{
	uint32_t leading = 64 - op->size;
	int64_t answer = (int64_t)load_int(op, from, n_bytes);

	if ((op->subflags & AS_BITS_INT_SUBFLAG_SIGNED) != 0) {
		// Sign extend values.
		answer = ((answer << leading) >> leading);
	}

	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);

	return true;
}


//==========================================================
// Local helpers - bits op helpers.
//

static void
lshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes)
{
	uint32_t n_shift = (uint32_t)op->value;
	uint32_t n_shift_bytes = n_shift / 8;
	const uint8_t* cur = &from[n_shift_bytes];
	const uint8_t* end = &from[n_bytes - 1];
	uint8_t last_m = (uint8_t)(0xFF <<
			((8 - ((uint32_t)op->offset + op->size) % 8) % 8));
	uint8_t last_byte = (uint8_t)(from[n_bytes - 1] & last_m);
	uint32_t l8 = n_shift % 8;
	uint32_t r8 = 8 - l8;
	uint32_t r64 = 64 - l8;

	if (n_shift >= op->size) {
		// This condition needs to be in terms of bits.
		memset(to, 0, n_bytes);

		return;
	}

	if (l8 == 0) {
		size_t copy_bytes = (size_t)(end - cur);

		memcpy(to, cur, copy_bytes);
		to += copy_bytes;
		cur += copy_bytes;
		*to++ = last_byte;
		memset(to, 0, n_shift_bytes);

		return;
	}

	// Begin - optimize enveloped 8-byte groups.

	while (end - cur > 1 && (uint64_t)cur % 8 != 0) {
		*to++ = (uint8_t)((*cur << l8) | ((*(cur + 1)) >> r8));
		cur++;
	}

	if (end - cur >= 16) {
		while (end - cur >= 64 + 8) {
			for (uint32_t i = 0; i < 8; i++) {
				uint64_t v = cf_swap_from_be64(*(uint64_t*)cur);
				cur += 8;
				uint64_t n = cf_swap_from_be64(*(uint64_t*)cur);

				*(uint64_t*)to = cf_swap_to_be64((v << l8) | (n >> r64));
				to += 8;
			}
		}

		while (end - cur >= 16) {
			uint64_t v = cf_swap_from_be64(*(uint64_t*)cur);
			cur += 8;
			uint64_t n = cf_swap_from_be64(*(uint64_t*)cur);

			*(uint64_t*)to = cf_swap_to_be64((v << l8) | (n >> r64));
			to += 8;
		}
	}

	// End - optimize enveloped 8-byte groups.

	while (end - cur > 1) {
		*to++ = (uint8_t)((*cur << l8) | (*(cur + 1) >> r8));
		cur++;
	}

	if (cur < end) {
		*to++ = (uint8_t)((*cur++ << l8) | (last_byte >> r8));
	}

	*to++ = (uint8_t)(last_byte << l8);
	memset(to, 0, n_shift_bytes);
}

static void
rshift(const bits_op* op, uint8_t* to, const uint8_t* from, uint32_t n_bytes)
{
	uint32_t n_shift = (uint32_t)op->value;
	uint32_t n_shift_bytes = n_shift / 8;
	uint8_t first_byte = (uint8_t)(from[0] & (0xFF >> op->offset));
	const uint8_t* cur = &from[1];
	const uint8_t* end = &from[n_bytes - n_shift_bytes];
	uint32_t r8 = n_shift % 8;
	uint32_t l8 = 8 - r8;
	uint32_t l64 = 64 - r8;

	if (n_shift >= op->size) {
		memset(to, 0, n_bytes);

		return;
	}

	memset(to, 0, n_shift_bytes);
	to += n_shift_bytes;

	if (r8 == 0) {
		*to++ = first_byte;
		memcpy(to, cur, n_bytes - n_shift_bytes - 1);

		return;
	}

	*to++ = (uint8_t)(first_byte >> r8);

	if (cur < end) {
		*to++ = (uint8_t)((first_byte << l8) | (*cur++ >> r8));
	}

	// Begin - optimize enveloped 8-byte groups.

	while (cur < end && (cur - from < 8 || (uint64_t)cur % 8 != 0)) {
		*to++ = (uint8_t)((*(cur - 1) << l8) | (*cur >> r8));
		cur++;
	}

	while (end - cur >= 64) {
		for (uint32_t i = 0; i < 8; i++) {
			uint64_t v = cf_swap_from_be64(*(uint64_t*)(cur - 8));
			uint64_t n = cf_swap_from_be64(*(uint64_t*)cur);

			*(uint64_t*)to = cf_swap_to_be64((v << l64) | (n >> r8));
			to += sizeof(uint64_t);
			cur += sizeof(uint64_t);
		}
	}

	while (end - cur >= 8) {
		uint64_t v = cf_swap_from_be64(*(uint64_t*)(cur - 8));
		uint64_t n = cf_swap_from_be64(*(uint64_t*)cur);

		*(uint64_t*)to = cf_swap_to_be64((v << l64) | (n >> r8));
		to += sizeof(uint64_t);
		cur += sizeof(uint64_t);
	}

	// End - optimize enveloped 8-byte groups.

	while (cur < end) {
		*to++ = (uint8_t)((*(cur - 1) << l8) | (*cur >> r8));
		cur++;
	}
}

static void
rshift_with_or(const bits_op* op, uint8_t* to, const uint8_t* from)
{
	RSHIFT_WITH_OP(|)
}

static void
rshift_with_xor(const bits_op* op, uint8_t* to, const uint8_t* from)
{
	RSHIFT_WITH_OP(^)
}

static void
rshift_with_and(const bits_op* op, uint8_t* to, const uint8_t* from)
{
	RSHIFT_WITH_OP(&)
}

static uint64_t
load_int(const bits_op* op, const uint8_t* from, uint32_t n_bytes)
{
	const bits_op load_cmd = {
		.offset = 0,
		.size = 64,
		.value = (uint64_t)op->offset
	};

	uint8_t load[9]; // last byte is needed for overflow but never read

	lshift(&load_cmd, load, from, n_bytes);
	*(uint64_t*)load = cf_swap_from_be64(*(uint64_t*)load) >> (64 - op->size);

	return *(uint64_t*)load;
}

static bool
handle_signed_overflow(uint32_t n_bits, bool is_saturate, uint64_t load,
		uint64_t value, uint64_t* result)
{
	uint32_t leading = 64 - n_bits;
	uint64_t value_m =  0xFFFFffffFFFFffff >> leading;

	// Sign extend values.
	int64_t s_result = (((int64_t)*result) << leading) >> leading;
	int64_t s_load = (((int64_t)load) << leading) >> leading;
	int64_t s_value = (((int64_t)value) << leading) >> leading;

	if (s_load < 0 && s_value < 0 && s_result > 0) {
		if (is_saturate) {
			// Max negative.
			*result = (value_m >> (n_bits - 1)) << (n_bits - 1);
		}
		else {
			return false;
		}
	}
	else if (s_load > 0 && s_value > 0 && s_result < 0) {
		if (is_saturate) {
			// Max positive.
			*result = value_m >> 1;
		}
		else {
			return false;
		}
	}

	return true;
}

static void
store_int(const bits_op* op, uint8_t* to, uint64_t value,
		uint32_t n_bytes)
{
	uint64_t store = cf_swap_to_be64(value << (64 - op->size));

	const bits_op store_cmd = {
		.size = ((op->size + 7) / 8) * 8,
		.value = (uint64_t)op->offset
	};

	rshift(&store_cmd, to, (uint8_t*)&store, n_bytes);
}

static void
restore_ends(const bits_op* op, uint8_t* to, const uint8_t* from,
		uint32_t n_bytes)
{
	uint32_t offset = (uint32_t)op->offset;
	uint32_t size = op->size;
	uint8_t from_head = from[0];

	if (n_bytes == 1) {
		uint32_t from_head_len = offset;
		uint8_t from_head_m = (uint8_t)(0xFF << (8 - from_head_len));
		uint32_t from_tail_len = (8 - (offset + size) % 8) % 8;
		uint8_t from_tail_m = (uint8_t)(0xFF >> (8 - from_tail_len));
		uint8_t m = from_head_m | from_tail_m;

		*to = (uint8_t)((*to & ~m) | (from_head & m));

		return;
	}

	if (offset != 0) {
		uint32_t from_head_len = offset;
		uint8_t from_head_m = (uint8_t)(0xFF << (8 - from_head_len));

		*to = (uint8_t)((*to & ~from_head_m) | (from_head & from_head_m));
	}

	if ((offset + size) % 8 != 0) {
		uint8_t from_tail = from[n_bytes - 1];
		uint32_t from_tail_len = 8 - ((offset + size) % 8);
		uint8_t from_tail_m = (uint8_t)(0xFF >> (8 - from_tail_len));

		to += (n_bytes - 1);
		*to = (uint8_t)((*to & ~from_tail_m) | (from_tail & from_tail_m));
	}
}
