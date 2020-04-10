/*
 * particle_hll.c
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

//==========================================================
// Includes.
//

#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_hash_math.h"

#include "bits.h"
#include "dynbuf.h"
#include "fault.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"

#include "warnings.h"


//==========================================================
// HLL particle interface - function declarations.
//

// Most HLL particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle "wire" format.
static int32_t hll_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
static int hll_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
static int hll_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
static int hll_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
static int hll_from_wire(as_particle_type wire_type, const uint8_t* wire_value, uint32_t value_size, as_particle** pp);

//==========================================================
// HLL particle interface - vtable.
//

const as_particle_vtable hll_vtable = {
		blob_destruct,
		blob_size,

		hll_concat_size_from_wire,
		hll_append_from_wire,
		hll_prepend_from_wire,
		hll_incr_from_wire,
		blob_size_from_wire,
		hll_from_wire,
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

// Same as related BLOB struct. TODO - just expose BLOB structs?
typedef struct hll_mem_s {
	uint8_t type;
	uint32_t sz;
	uint8_t data[];
} __attribute__ ((__packed__)) hll_mem;

typedef struct hll_s {
	uint8_t flags;
	uint8_t n_index_bits; // n_registers = 1 << index_bits (range 4 to 16)
	uint8_t n_minhash_bits; // r in https://arxiv.org/pdf/1710.08436.pdf
	uint64_t cache;
	uint8_t registers[];
} __attribute__ ((__packed__)) hll_t;

#define MIN_INDEX_BITS 4
#define MAX_INDEX_BITS 16
#define HLL_BITS 6
#define HLL_MAX_VALUE (1 << HLL_BITS)
#define MIN_MINHASH_BITS 4
#define MAX_MINHASH_BITS (64 - HLL_BITS - 7)
#define MAX_INDEX_AND_MINHASH_BITS 64

typedef struct element_buf_s {
	uint32_t sz;
	const uint8_t* buf;
} element_buf;

typedef struct hll_op_s {
	uint32_t n_elements;
	element_buf* elements;
	uint8_t n_index_bits;
	uint8_t n_minhash_bits;
	const uint8_t* hll;
	uint64_t flags;
} hll_op;

struct hll_state_s;

typedef bool (*hll_parse_fn) (struct hll_state_s* state, hll_op* op);
typedef void (*hll_modify_fn) (const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);
typedef void (*hll_read_fn) (const hll_op* op, const hll_t* hll, as_bin* rb);
typedef int32_t (*hll_prepare_op_fn) (hll_op* op, const as_particle* old_p);

typedef struct hll_op_def_s {
	const hll_prepare_op_fn prepare;
	union {
		const hll_modify_fn modify;
		const hll_read_fn read;
	} fn;
	uint64_t bad_flags;
	uint32_t min_args;
	uint32_t max_args;
	const hll_parse_fn* args;
	const char* name;
} hll_op_def;

typedef struct hll_state_s {
	as_hll_op_type op_type;
	msgpack_in pk;
	uint32_t n_args;
	hll_op_def* def;
} hll_state;


//==========================================================
// Forward declarations.
//

void hll_op_destroy(hll_op* op);

static int32_t hll_verify_bin(const as_bin* b);
static bool hll_state_init(hll_state* state, const as_msg_op* msg_op, bool is_read);
static bool hll_parse_op(hll_state* state, hll_op* op);
static bool hll_parse_n_index_bits(hll_state* state, hll_op* op);
static bool hll_parse_n_minhash_bits(hll_state* state, hll_op* op);
static bool hll_parse_flags(hll_state* state, hll_op* op);
static bool hll_parse_elements(hll_state* state, hll_op* op);
static bool hll_parse_hlls(hll_state* state, hll_op* op);

static int32_t hll_modify_prepare_init_op(hll_op* op, const as_particle* old_p);
static int32_t hll_modify_prepare_add_op(hll_op* op, const as_particle* old_p);
static int32_t hll_modify_prepare_union_op(hll_op* op, const as_particle* old_p);
static int32_t hll_modify_prepare_count_op(hll_op* op, const as_particle* old_p);
static int32_t hll_modify_prepare_fold_op(hll_op* op, const as_particle* old_p);

static void hll_modify_execute_op(const hll_state* state, const hll_op* op, const as_particle* old_p, as_particle* new_p, uint32_t new_size, as_bin* rb);
static void hll_read_execute_op(const hll_state* state, const hll_op* op, const as_particle* p, as_bin* rb);

static void hll_modify_op_init(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);
static void hll_modify_op_add(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);
static void hll_modify_op_union(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);
static void hll_modify_op_count(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);
static void hll_modify_op_fold(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb);

static void hll_read_op_count(const hll_op* op, const hll_t* from, as_bin* rb);
static void hll_read_op_union(const hll_op* op, const hll_t* from, as_bin* rb);
static void hll_read_op_union_count(const hll_op* op, const hll_t* from, as_bin* rb);
static void hll_read_op_intersect_count(const hll_op* op, const hll_t* from, as_bin* rb);
static void hll_read_op_similarity(const hll_op* op, const hll_t* from, as_bin* rb);
static void hll_read_op_describe(const hll_op* op, const hll_t* from, as_bin* rb);

static bool validate_n_combined_bits(uint64_t n_index_bits, uint64_t n_minhash_bits);
static bool validate_n_index_bits(uint64_t n_index_bits);
static bool validate_n_minhash_bits(uint64_t n_minhash_bits);

static bool verify_hll_sz(const hll_t* hmh, uint32_t expected_sz);
static uint32_t hmh_required_sz(uint8_t n_index_bits, uint8_t n_minhash_bits);
static void hmh_init(hll_t* hmh, uint8_t n_index_bits, uint8_t n_minhash_bits);
static bool hmh_add(hll_t* hmh, size_t buf_sz, const uint8_t* buf);
static void hmh_estimate_cardinality_update_cache(hll_t* hmh);
static uint64_t hmh_estimate_cardinality(const hll_t* hmh);
static uint64_t hmh_estimate_union_cardinality(uint32_t n_hmhs, const hll_t** hmhs);
static void hmh_compatible_template(uint32_t n_hmhs, const hll_t** hmhs, hll_t* template);
static void hmh_union(hll_t* hmhunion, const hll_t* hmh);
static uint64_t hmh_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs);
static double hmh_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs);

static uint32_t registers_sz(uint8_t n_index_bits, uint8_t n_minhash_bits);
static void hmh_hash(const hll_t* hmh, const uint8_t* element, size_t value_sz, uint16_t* register_ix, uint64_t* value);
static uint64_t get_register(const hll_t* hmh, uint32_t r);
static void set_register(hll_t* hmh, uint32_t r, uint64_t value);
static uint8_t unpack_register_hll_val(const hll_t* hmh, uint64_t value);
static double hmh_tau(double val);
static double hmh_sigma(double val);
static double hmh_alpha(const hll_t* hmh);
static void hll_union(hll_t* hllunion, const hll_t* hmh);
static uint64_t hll_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs);
static bool hmh_has_data(const hll_t* hmh);
static void hmh_intersect_one(hll_t* intersect_hmh, const hll_t* hmh);
static uint32_t hmh_n_used_registers(const hll_t* hmh);
static double hmh_jaccard_estimate_collisions(const hll_t* template, uint64_t card0, uint64_t card1);
static double hll_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs);


//==========================================================
// Inlines & macros.
//

#define HLL_MODIFY_OP_ENTRY(_op, _op_fn, _prep_fn, _flags, _min_args, \
		_max_args, ...) \
	[_op].name = # _op, [_op].prepare = _prep_fn, [_op].fn.modify = _op_fn, \
	[_op].bad_flags = ~((uint64_t)(_flags)), [_op].min_args = _min_args, \
	[_op].max_args = _max_args, [_op].args = (hll_parse_fn[]){__VA_ARGS__}
#define HLL_READ_OP_ENTRY(_op, _op_fn, _n_args, ...) \
	[_op].name = # _op, [_op].prepare = NULL, \
	[_op].fn.read = _op_fn, [_op].bad_flags = ~((uint64_t)(0)), \
	[_op].min_args = _n_args, [_op].max_args = _n_args, \
	[_op].args = (hll_parse_fn[]){__VA_ARGS__}


//==========================================================
// Op tables.
//

static const hll_op_def hll_modify_op_table[] = {
		HLL_MODIFY_OP_ENTRY(AS_HLL_OP_INIT, hll_modify_op_init,
				hll_modify_prepare_init_op,
				(AS_HLL_FLAG_CREATE_ONLY | AS_HLL_FLAG_UPDATE_ONLY |
						AS_HLL_FLAG_NO_FAIL),
				0, 3, hll_parse_n_index_bits, hll_parse_n_minhash_bits,
				hll_parse_flags),
		HLL_MODIFY_OP_ENTRY(AS_HLL_OP_ADD, hll_modify_op_add,
				hll_modify_prepare_add_op,
				(AS_HLL_FLAG_CREATE_ONLY | AS_HLL_FLAG_NO_FAIL),
				1, 4, hll_parse_elements, hll_parse_n_index_bits,
				hll_parse_n_minhash_bits, hll_parse_flags),
		HLL_MODIFY_OP_ENTRY(AS_HLL_OP_UNION, hll_modify_op_union,
				hll_modify_prepare_union_op,
				(AS_HLL_FLAG_CREATE_ONLY | AS_HLL_FLAG_UPDATE_ONLY |
						AS_HLL_FLAG_ALLOW_FOLD | AS_HLL_FLAG_NO_FAIL),
				1, 2, hll_parse_hlls, hll_parse_flags),
		HLL_MODIFY_OP_ENTRY(AS_HLL_OP_UPDATE_COUNT, hll_modify_op_count,
				hll_modify_prepare_count_op,
				(0),
				0, 0),
		HLL_MODIFY_OP_ENTRY(AS_HLL_OP_FOLD, hll_modify_op_fold,
				hll_modify_prepare_fold_op,
				(0),
				1, 1, hll_parse_n_index_bits),
};

static const hll_op_def hll_read_op_table[] = {
		HLL_READ_OP_ENTRY(AS_HLL_OP_COUNT, hll_read_op_count, 0),
		HLL_READ_OP_ENTRY(AS_HLL_OP_GET_UNION, hll_read_op_union,
				1, hll_parse_hlls),
		HLL_READ_OP_ENTRY(AS_HLL_OP_UNION_COUNT, hll_read_op_union_count,
				1, hll_parse_hlls),
		HLL_READ_OP_ENTRY(AS_HLL_OP_INTERSECT_COUNT,
				hll_read_op_intersect_count,
				1, hll_parse_hlls),
		HLL_READ_OP_ENTRY(AS_HLL_OP_SIMILARITY, hll_read_op_similarity,
				1, hll_parse_hlls),
		HLL_READ_OP_ENTRY(AS_HLL_OP_DESCRIBE, hll_read_op_describe, 0),
};


//==========================================================
// HLL particle interface - function definitions.
//

// Most HLL particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

static int32_t
hll_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;

	cf_warning(AS_PARTICLE, "invalid operation on hll particle");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

static int
hll_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;

	cf_warning(AS_PARTICLE, "invalid operation on hll particle");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

static int
hll_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;

	cf_warning(AS_PARTICLE, "invalid operation on hll particle");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

static int
hll_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;

	cf_warning(AS_PARTICLE, "invalid operation on hll particle");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

static int
hll_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	const hll_t* hll = (const hll_t*)wire_value;

	if (! (hll->flags == 0 && validate_n_combined_bits(hll->n_index_bits,
			hll->n_minhash_bits) && verify_hll_sz(hll, value_size))) {
		cf_warning(AS_PARTICLE, "bad hll - flags %x n_index_bits %u n_minhash_bits %u sz %u",
				hll->flags, hll->n_index_bits, hll->n_minhash_bits, value_size);
		return -AS_ERR_PARAMETER;
	}

	hll_mem* p_hll_mem = (hll_mem*)*pp;

	p_hll_mem->type = wire_type;
	p_hll_mem->sz = value_size;
	memcpy(p_hll_mem->data, wire_value, p_hll_mem->sz);

	return AS_OK;
}


//==========================================================
// as_bin particle functions specific to HLL.
//

int
as_bin_hll_read(const as_bin* b, const as_msg_op* msg_op, as_bin* rb)
{
	cf_assert(as_bin_inuse(b), AS_PARTICLE, "unused bin");

	int32_t verify_result = hll_verify_bin(b);

	if (verify_result != AS_OK) {
		return verify_result;
	}

	hll_state state = { 0 };

	if (! hll_state_init(&state, msg_op, true)) {
		return -AS_ERR_PARAMETER;
	}

	hll_op op = { 0 };

	if (! hll_parse_op(&state, &op)) {
		hll_op_destroy(&op);
		return -AS_ERR_PARAMETER;
	}

	hll_read_execute_op(&state, &op, b->particle, rb);
	hll_op_destroy(&op);

	return AS_OK;
}

int
as_bin_hll_modify(as_bin* b, const as_msg_op* msg_op, cf_ll_buf* particles_llb,
		as_bin* rb)
{
	int32_t verify_result = hll_verify_bin(b);

	if (verify_result != AS_OK) {
		return verify_result;
	}

	hll_state state = { 0 };

	if (! hll_state_init(&state, msg_op, false)) {
		return -AS_ERR_PARAMETER;
	}

	hll_op op = { .n_index_bits = 0xFF, .n_minhash_bits = 0xFF };

	if (! hll_parse_op(&state, &op)) {
		hll_op_destroy(&op);
		return -AS_ERR_PARAMETER;
	}

	as_particle* old_p;

	if (as_bin_inuse(b)) {
		if ((op.flags & AS_HLL_FLAG_CREATE_ONLY) != 0) {
			if ((op.flags & AS_HLL_FLAG_NO_FAIL) != 0) {
				hll_op_destroy(&op);
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "as_bin_hll_modify - error %u operation %s (%u) on bin %.*s would update",
					AS_ERR_BIN_EXISTS, state.def->name, state.op_type,
					(int)msg_op->name_sz, msg_op->name);
			hll_op_destroy(&op);
			return -AS_ERR_BIN_EXISTS;
		}
		// else - there is an existing hll particle, which we will modify.

		old_p = b->particle;
	}
	else {
		if ((op.flags & AS_HLL_FLAG_UPDATE_ONLY) != 0) {
			if ((op.flags & AS_HLL_FLAG_NO_FAIL) != 0) {
				hll_op_destroy(&op);
				return AS_OK;
			}

			cf_warning(AS_PARTICLE, "as_bin_hll_modify - error %u operation %s (%u) on bin %.*s would create",
					AS_ERR_BIN_NOT_FOUND, state.def->name, state.op_type,
					(int)msg_op->name_sz, msg_op->name);
			hll_op_destroy(&op);
			return -AS_ERR_BIN_NOT_FOUND;
		}

		cf_assert(b->particle == NULL, AS_PARTICLE, "particle not null");

		old_p = NULL;
	}

	int32_t prepare_result = state.def->prepare(&op, old_p);

	if (prepare_result != AS_OK) {
		hll_op_destroy(&op);
		return prepare_result;
	}

	uint32_t new_size = hmh_required_sz(op.n_index_bits, op.n_minhash_bits);
	size_t alloc_size = sizeof(hll_mem) + new_size;

	if (particles_llb == NULL) {
		b->particle = cf_malloc_ns(alloc_size);
	}
	else {
		cf_ll_buf_reserve(particles_llb, alloc_size, (uint8_t**)&b->particle);
	}

	hll_modify_execute_op(&state, &op, old_p, b->particle, new_size, rb);
	hll_op_destroy(&op);

	if (old_p == NULL) {
		// Set the bin's iparticle metadata.
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_HLL);
	}

	return AS_OK;
}


//==========================================================
// Local helpers - cleanup.
//

void
hll_op_destroy(hll_op* op)
{
	if (op->elements != NULL) {
		cf_free(op->elements);
	}
}


//==========================================================
// Local helpers - hll parsing.
//

static int32_t
hll_verify_bin(const as_bin* b)
{
	if (! as_bin_inuse(b)) {
		return AS_OK;
	}

	const hll_mem* bmem = (const hll_mem*)b->particle;
	const hll_t* hll = (const hll_t*)bmem->data;
	uint8_t type = as_bin_get_particle_type(b);

	if (type != AS_PARTICLE_TYPE_HLL) {
		cf_warning(AS_PARTICLE, "hll_verify_bin - error %u bin is not hll (%u) found %u",
				AS_ERR_INCOMPATIBLE_TYPE, AS_PARTICLE_TYPE_HLL, type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	if (! (hll->flags == 0 && validate_n_combined_bits(hll->n_index_bits,
			hll->n_minhash_bits) && verify_hll_sz(hll, bmem->sz))) {
		cf_warning(AS_PARTICLE, "hll_verify_bin - error %u found invalid hll flags %x n_index_bits %u n_minhash_bits %u sz %u",
				AS_ERR_UNKNOWN, hll->flags, hll->n_index_bits,
				hll->n_minhash_bits, bmem->sz);
		return -AS_ERR_UNKNOWN;
	}

	return AS_OK;
}

static bool
hll_state_init(hll_state* state, const as_msg_op* msg_op, bool is_read)
{
	const uint8_t* data = msg_op->name + msg_op->name_sz;
	uint32_t sz = msg_op->op_sz - (uint32_t)OP_FIXED_SZ - msg_op->name_sz;

	state->pk.buf = data;
	state->pk.buf_sz = sz;
	state->pk.offset = 0;

	uint32_t ele_count = UINT32_MAX;

	if (! msgpack_get_list_ele_count(&state->pk, &ele_count) ||
			ele_count == 0) {
		cf_warning(AS_PARTICLE, "hll_state_init - error %u bin %.*s insufficient args (%u) or unable to parse args",
				AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name,
				ele_count);
		return false;
	}

	uint64_t type64;

	if (! msgpack_get_uint64(&state->pk, &type64)) {
		cf_warning(AS_PARTICLE, "hll_state_init - error %u bin %.*s unable to parse op",
				AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name);
		return false;
	}

	state->op_type = (as_hll_op_type)type64;
	state->n_args = ele_count - 1; // removed op argument

	if (is_read) {
		if (! (state->op_type >= AS_HLL_READ_OP_START &&
				state->op_type < AS_HLL_READ_OP_END)) {
			cf_warning(AS_PARTICLE, "hll_state_init - error %u bin %.*s op %u expected read op",
					AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name,
					state->op_type);
			return false;
		}

		state->def = (hll_op_def*)&hll_read_op_table[state->op_type];
	}
	else {
		if (! (state->op_type >= AS_HLL_MODIFY_OP_START &&
				state->op_type < AS_HLL_MODIFY_OP_END)) {
			cf_warning(AS_PARTICLE, "hll_state_init - error %u bin %.*s op %u expected modify op",
					AS_ERR_PARAMETER, (int)msg_op->name_sz, msg_op->name,
					state->op_type);
			return false;
		}

		state->def = (hll_op_def*)&hll_modify_op_table[state->op_type];
	}

	return true;
}

static bool
hll_parse_op(hll_state* state, hll_op* op)
{
	hll_op_def* def = state->def;

	if (state->n_args < def->min_args || state->n_args > def->max_args) {
		cf_warning(AS_PARTICLE, "hll_parse_op - error %u op %s (%u) unexpected number of args %u for op %s",
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
hll_parse_n_index_bits(hll_state* state, hll_op* op)
{
	int64_t n_index_bits;

	if (! msgpack_get_int64(&state->pk, &n_index_bits)) {
		cf_warning(AS_PARTICLE, "hll_parse_n_index_bits - error %u op %s (%u) unable to parse n_index_bits for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->def->name);
		return false;
	}

	if (n_index_bits != -1 && ! validate_n_index_bits((uint64_t)n_index_bits)) {
		cf_warning(AS_PARTICLE, "hll_parse_n_index_bits - error %u op %s (%u) n_index_bits (%ld) is out of range",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				n_index_bits);
		return false;
	}

	op->n_index_bits = (uint8_t)n_index_bits;

	return true;
}

static bool
hll_parse_n_minhash_bits(hll_state* state, hll_op* op)
{
	int64_t n_minhash_bits;

	if (! msgpack_get_int64(&state->pk, &n_minhash_bits)) {
		cf_warning(AS_PARTICLE, "hll_parse_n_minhash_bits - error %u op %s (%u) unable to parse n_minhash_bits for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->def->name);
		return false;
	}

	if (n_minhash_bits == -1) {
		op->n_minhash_bits = (uint8_t)n_minhash_bits;

		return true;
	}

	if (! validate_n_minhash_bits((uint64_t)n_minhash_bits)) {
		cf_warning(AS_PARTICLE, "hll_parse_n_minhash_bits - error %u op %s (%u) n_minhash_bits (%ld) is out of range",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				n_minhash_bits);
		return false;
	}

	if (op->n_index_bits != 0xFF && ! validate_n_combined_bits(
			op->n_index_bits, (uint64_t)n_minhash_bits)) {
		cf_warning(AS_PARTICLE, "hll_parse_n_minhash_bits - error %u op %s (%u) n_index_bits (%u) and n_minhash_bits (%ld) must be less than or equal to %u",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				op->n_index_bits, n_minhash_bits, MAX_INDEX_AND_MINHASH_BITS);
		return false;
	}

	op->n_minhash_bits = (uint8_t)n_minhash_bits;

	return true;
}

static bool
hll_parse_flags(hll_state* state, hll_op* op)
{
	if (! msgpack_get_uint64(&state->pk, &op->flags)) {
		cf_warning(AS_PARTICLE, "hll_parse_flags - error %u op %s (%u) unable to parse flags for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type,
				state->def->name);
		return false;
	}

	if ((op->flags & state->def->bad_flags) != 0) {
		cf_warning(AS_PARTICLE, "hll_parse_flags - error %u op %s (%u) invalid flags (0x%lx) for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags,
				state->def->name);
		return false;
	}

	if ((op->flags & AS_HLL_FLAG_CREATE_ONLY) != 0 &&
			(op->flags & AS_HLL_FLAG_UPDATE_ONLY) != 0) {
		cf_warning(AS_PARTICLE, "hll_parse_flags - error %u op %s (%u) invalid flags combination (0x%lx) for op %s",
				AS_ERR_PARAMETER, state->def->name, state->op_type, op->flags,
				state->def->name);
		return false;
	}

	return true;
}

static bool
hll_parse_elements(hll_state* state, hll_op* op) {
	if (! msgpack_get_list_ele_count(&state->pk, &op->n_elements) ||
			op->n_elements == 0) {
		cf_warning(AS_PARTICLE, "hll_parse_elements - error %u op %s (%u) not enough elements or unable to parse elements",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	op->elements = cf_malloc(sizeof(element_buf) * op->n_elements);

	for (uint32_t i = 0; i < op->n_elements; i++) {
		element_buf* e = &op->elements[i];
		msgpack_type t = msgpack_peek_type(&state->pk);

		switch (t) {
		case MSGPACK_TYPE_LIST:
		case MSGPACK_TYPE_MAP:
			cf_warning(AS_PARTICLE, "hll_parse_elements - error %u op %s (%u) element (%u) is either a list or map which are unsupported (%u)",
					AS_ERR_PARAMETER, state->def->name, state->op_type, i, t);
			return false;
		default:
			e->buf = msgpack_skip(&state->pk, &e->sz);

			if (e->buf == NULL) {
				cf_warning(AS_PARTICLE, "hll_parse_elements - error %u op %s (%u) unable to parse element (%u)",
						AS_ERR_PARAMETER, state->def->name, state->op_type, i);
			}
		}
	}

	return true;
}

static bool
hll_parse_hlls(hll_state* state, hll_op* op) {
	if (! msgpack_get_list_ele_count(&state->pk, &op->n_elements) ||
			op->n_elements == 0) {
		cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) not enough elements or unable to parse elements",
				AS_ERR_PARAMETER, state->def->name, state->op_type);
		return false;
	}

	op->elements = cf_malloc(sizeof(element_buf) * op->n_elements);

	for (uint32_t i = 0; i < op->n_elements; i++) {
		element_buf* e = &op->elements[i];

		e->buf = msgpack_get_bin(&state->pk, &e->sz);

		// AS msgpack has a one byte blob type field which we ignore here.
		if (e->buf == NULL || e->sz < sizeof(hll_t) + 1) {
			cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) hll (%u) has a bad pointer %p or size (%u) minimum size (%zu)",
					AS_ERR_PARAMETER, state->def->name, state->op_type, i,
					e->buf, e->sz, sizeof(hll_t));
			return false;
		}

		e->buf++; // skip blob type byte
		e->sz--;

		hll_t* hll = (hll_t*)e->buf;

		if (hll->flags != 0) {
			cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) hll (%u) contains unknown flags (%x)",
					AS_ERR_PARAMETER, state->def->name, state->op_type, i,
					hll->flags);
			return false;
		}

		if (! validate_n_index_bits((uint64_t)hll->n_index_bits)) {
			cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) n_index_bits (%u) out of range",
					AS_ERR_PARAMETER, state->def->name, state->op_type,
					hll->n_index_bits);
			return false;
		}

		if (! validate_n_minhash_bits((uint64_t)hll->n_minhash_bits)) {
			cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) n_minhash_bits (%u) out of range",
					AS_ERR_PARAMETER, state->def->name, state->op_type,
					hll->n_minhash_bits);
			return false;
		}

		uint32_t expected_sz = hmh_required_sz(hll->n_index_bits,
				hll->n_minhash_bits);

		if (e->sz != expected_sz) {
			cf_warning(AS_PARTICLE, "hll_parse_hlls - error %u op %s (%u) hll (%u) has a bad size (%u) expected (%u)",
					AS_ERR_PARAMETER, state->def->name, state->op_type, i,
					e->sz, expected_sz);
			return false;
		}
	}

	return true;
}


//==========================================================
// Local helpers - prepare modify ops.
//

static int32_t
hll_modify_prepare_init_op(hll_op* op, const as_particle* old_p)
{
	if (old_p == NULL) { // creating new HLL
		if (op->n_index_bits == 0xFF) {
			cf_warning(AS_PARTICLE, "hll_modify_prepare_init_op - error %u cannot create when n_index_bits is unset",
					AS_ERR_OP_NOT_APPLICABLE);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (op->n_minhash_bits == 0xFF) {
			op->n_minhash_bits = 0;
		}

		return AS_OK;
	}

	hll_t* old_hll = (hll_t*)((hll_mem*)old_p)->data;

	if (op->n_index_bits == 0xFF) {
		op->n_index_bits = old_hll->n_index_bits;
	}

	if (op->n_minhash_bits == 0xFF) {
		op->n_minhash_bits = old_hll->n_minhash_bits;
	}

	if (! validate_n_combined_bits(op->n_index_bits, op->n_minhash_bits)) {
		cf_warning(AS_PARTICLE, "hll_modify_prepare_init_op - error %u init on results in an invalid bit configuration (%u,%u)",
				AS_ERR_OP_NOT_APPLICABLE, op->n_index_bits, op->n_minhash_bits);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

static int32_t
hll_modify_prepare_add_op(hll_op* op, const as_particle* old_p)
{
	if (old_p == NULL) { // creating new HLL
		if (op->n_index_bits == 0xFF) {
			cf_warning(AS_PARTICLE, "hll_modify_prepare_add_op - error %u cannot create when n_index_bits is unset",
					AS_ERR_OP_NOT_APPLICABLE);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (op->n_minhash_bits == 0xFF) {
			op->n_minhash_bits = 0;
		}

		return AS_OK;
	}

	hll_t* old_hll = (hll_t*)((hll_mem*)old_p)->data;

	op->n_index_bits = old_hll->n_index_bits;
	op->n_minhash_bits = old_hll->n_minhash_bits;

	return AS_OK;
}

static int32_t
hll_modify_prepare_union_op(hll_op* op, const as_particle* old_p)
{
	uint8_t min_n_index_bits = 0xFF;
	uint8_t min_n_minhash_bits = 0xFF;

	for (uint32_t i = 0; i < op->n_elements; i++) {
		element_buf* e = &op->elements[i];
		hll_t* hll = (hll_t*)e->buf;

		if (hll->n_index_bits < min_n_index_bits) {
			min_n_index_bits = hll->n_index_bits;
		}

		if (hll->n_minhash_bits != min_n_minhash_bits) {
			if (min_n_minhash_bits == 0xFF) {
				min_n_minhash_bits = hll->n_minhash_bits;
			}
			else {
				min_n_minhash_bits = 0;
			}
		}
	}

	if (old_p == NULL) { // creating new HLL
		op->n_index_bits = min_n_index_bits;
		op->n_minhash_bits = min_n_minhash_bits;
		return AS_OK;
	}

	hll_t* old_hll = (hll_t*)((hll_mem*)old_p)->data;

	if ((op->flags & AS_HLL_FLAG_ALLOW_FOLD) == 0) {
		if (old_hll->n_index_bits > min_n_index_bits) {
			cf_warning(AS_PARTICLE, "hll_modify_prepare_union_op - error %u cannot reduce n_index_bits",
					AS_ERR_OP_NOT_APPLICABLE);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (old_hll->n_minhash_bits != 0 &&
				old_hll->n_minhash_bits != min_n_minhash_bits) {
			cf_warning(AS_PARTICLE, "hll_modify_prepare_union_op - error %u cannot reduce n_minhash_bits to zero",
					AS_ERR_OP_NOT_APPLICABLE);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		op->n_index_bits = old_hll->n_index_bits;
		op->n_minhash_bits = old_hll->n_minhash_bits;
	}
	else { // allow_fold
		op->n_index_bits = old_hll->n_index_bits < min_n_index_bits ?
				old_hll->n_index_bits : min_n_index_bits;
		op->n_minhash_bits = old_hll->n_minhash_bits != min_n_minhash_bits ?
				0 : min_n_minhash_bits;
	}

	return AS_OK;
}

static int32_t
hll_modify_prepare_count_op(hll_op* op, const as_particle* old_p)
{
	if (old_p == NULL) { // Creating new HLL.
		cf_warning(AS_PARTICLE, "hll_modify_prepare_count_op - error %u cannot create bin with count op",
				AS_ERR_BIN_NOT_FOUND);
		return -AS_ERR_BIN_NOT_FOUND;
	}

	hll_t* old_hll = (hll_t*)((hll_mem*)old_p)->data;

	op->n_index_bits = old_hll->n_index_bits;
	op->n_minhash_bits = old_hll->n_minhash_bits;

	return AS_OK;
}

static int32_t
hll_modify_prepare_fold_op(hll_op* op, const as_particle* old_p)
{
	if (old_p == NULL) { // Creating new HLL.
		cf_warning(AS_PARTICLE, "hll_modify_prepare_fold_op - error %u cannot create bin with fold op",
				AS_ERR_BIN_NOT_FOUND);
		return -AS_ERR_BIN_NOT_FOUND;
	}

	hll_t* old_hll = (hll_t*)((hll_mem*)old_p)->data;

	if (old_hll->n_minhash_bits > 0) {
		cf_warning(AS_PARTICLE, "hll_modify_prepare_fold_op - error %u cannot fold an HLL containing minhash bits",
				AS_ERR_OP_NOT_APPLICABLE);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (old_hll->n_index_bits < op->n_index_bits) {
		cf_warning(AS_PARTICLE, "hll_modify_prepare_fold_op - error %u existing HLL has less or equal n_index_bits (%u) than (%u)",
				AS_ERR_OP_NOT_APPLICABLE, old_hll->n_index_bits,
				op->n_index_bits);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	op->n_minhash_bits = 0;

	return AS_OK;
}


//==========================================================
// Local helpers - execute ops.
//

static void
hll_modify_execute_op(const hll_state* state, const hll_op* op,
		const as_particle* old_p, as_particle* new_p, uint32_t new_size,
		as_bin* rb)
{
	hll_t* new_hll = (hll_t*)((hll_mem*)new_p)->data;

	state->def->fn.modify(op, new_hll,
			old_p == NULL ? NULL : (hll_t*)((hll_mem*)old_p)->data, rb);

	((hll_mem*)new_p)->sz = new_size;
	((hll_mem*)new_p)->type = AS_PARTICLE_TYPE_HLL;

	cf_assert(verify_hll_sz(new_hll, new_size), AS_PARTICLE, "result corrupt - op-type %u desc (%u,%u) expected-desc (%u,%u) sz %u expected-sz %u",
			state->op_type,
			new_hll->n_index_bits, new_hll->n_minhash_bits,
			op->n_index_bits, op->n_minhash_bits,
			hmh_required_sz(new_hll->n_index_bits, new_hll->n_minhash_bits),
			new_size);
}

static void
hll_read_execute_op(const hll_state* state, const hll_op* op,
		const as_particle* p, as_bin* rb)
{
	state->def->fn.read(op, (const hll_t*)((const hll_mem*)p)->data, rb);
}


//==========================================================
// Local helpers - hll modify ops.
//


static void
hll_modify_op_init(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb)
{
	(void)from;
	(void)rb;

	uint8_t n_index_bits = op->n_index_bits;
	uint8_t n_minhash_bits = op->n_minhash_bits;

	hmh_init(to, n_index_bits, n_minhash_bits);
}

static void
hll_modify_op_add(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb)
{
	if (from == NULL) {
		hmh_init(to, op->n_index_bits, op->n_minhash_bits);
	}
	else {
		memcpy(to, from, hmh_required_sz(from->n_index_bits,
				from->n_minhash_bits));
	}

	uint64_t n_added = 0;

	for (uint32_t i = 0; i < op->n_elements; i++) {
		element_buf* e = &op->elements[i];

		if (hmh_add(to, e->sz, e->buf)) {
			n_added++;
		}
	}

	rb->particle = (as_particle*)n_added;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
}

static void
hll_modify_op_union(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb)
{
	(void)rb;

	uint32_t n_hmhs = op->n_elements;
	const hll_t* hmhs[n_hmhs];

	for (uint32_t i = 0; i < op->n_elements; i++) {
		hmhs[i] = (hll_t*)op->elements[i].buf;
	}

	if (from != NULL) {
		hmhs[n_hmhs++] = from;
	}

	hmh_compatible_template(n_hmhs, hmhs, to);
	hmh_init(to, to->n_index_bits, to->n_minhash_bits);

	for (uint32_t i = 0; i < n_hmhs; i++) {
		hmh_union(to, hmhs[i]);
	}
}

static void
hll_modify_op_count(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb)
{
	(void)op;

	memcpy(to, from, hmh_required_sz(from->n_index_bits,
			from->n_minhash_bits));

	hmh_estimate_cardinality_update_cache(to);

	rb->particle = (as_particle*)to->cache;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
}

static void
hll_modify_op_fold(const hll_op* op, hll_t* to, const hll_t* from, as_bin* rb)
{
	(void)rb;

	if (op->n_index_bits == from->n_index_bits) {
		// TODO - would be better to noop.
		memcpy(to, from, hmh_required_sz(op->n_index_bits, 0));
		return;
	}

	hmh_init(to, op->n_index_bits, 0);
	hmh_union(to, from);
}


//==========================================================
// Local helpers - hll read ops.
//

static void
hll_read_op_count(const hll_op* op, const hll_t* from, as_bin* rb)
{
	(void)op;

	uint64_t count = hmh_estimate_cardinality(from);

	rb->particle = (as_particle*)count;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
}

static void
hll_read_op_union(const hll_op* op, const hll_t* from, as_bin* rb)
{
	uint32_t n_hmhs = op->n_elements + 1;
	const hll_t* hmhs[n_hmhs];

	for (uint32_t i = 0; i < op->n_elements; i++) {
		hmhs[i] = (hll_t*)op->elements[i].buf;
	}

	hmhs[op->n_elements] = from;

	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);

	uint32_t answer_sz = hmh_required_sz(template.n_index_bits,
			template.n_minhash_bits);
	hll_mem* answer = cf_malloc(sizeof(hll_mem) + answer_sz);
	hll_t* union_hmh = (hll_t*)answer->data;

	hmh_init(union_hmh, template.n_index_bits, template.n_minhash_bits);

	for (uint32_t i = 0; i < n_hmhs; i++) {
		hmh_union(union_hmh, hmhs[i]);
	}

	answer->sz = answer_sz;
	answer->type = AS_PARTICLE_TYPE_HLL;
	rb->particle = (as_particle*)answer;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_HLL);
}

static void
hll_read_op_union_count(const hll_op* op, const hll_t* from, as_bin* rb)
{
	uint32_t n_hmhs = op->n_elements + 1;
	const hll_t* hmhs[n_hmhs];

	for (uint32_t i = 0; i < op->n_elements; i++) {
		hmhs[i] = (hll_t*)op->elements[i].buf;
	}

	hmhs[op->n_elements] = from;

	uint64_t count = hmh_estimate_union_cardinality(n_hmhs, hmhs);

	rb->particle = (as_particle*)count;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
}

static void
hll_read_op_intersect_count(const hll_op* op, const hll_t* from, as_bin* rb)
{
	uint32_t n_hmhs = op->n_elements + 1;
	const hll_t* hmhs[n_hmhs];

	for (uint32_t i = 0; i < op->n_elements; i++) {
		hmhs[i] = (hll_t*)op->elements[i].buf;
	}

	hmhs[op->n_elements] = from;

	uint64_t count = hmh_estimate_intersect_cardinality(n_hmhs, hmhs);

	rb->particle = (as_particle*)count;
	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
}

static void
hll_read_op_similarity(const hll_op* op, const hll_t* from, as_bin* rb)
{
	uint32_t n_hmhs = op->n_elements + 1;
	const hll_t* hmhs[n_hmhs];

	for (uint32_t i = 0; i < op->n_elements; i++) {
		hmhs[i] = (hll_t*)op->elements[i].buf;
	}

	hmhs[op->n_elements] = from;

	double similarity = hmh_estimate_similarity(n_hmhs, hmhs);

	as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_FLOAT);
	*((double *)(&rb->particle)) = similarity;
}

static void
hll_read_op_describe(const hll_op* op, const hll_t* from, as_bin* rb)
{
	(void)op;

	define_rollback_alloc(alloc, NULL, 1, false);
	cdt_result_data rd = { .result=rb, .alloc=alloc };

	result_data_set_list_int2x(&rd, from->n_index_bits, from->n_minhash_bits);
}


//==========================================================
// Local helpers - hll validation.
//

static bool
validate_n_combined_bits(uint64_t n_index_bits, uint64_t n_minhash_bits)
{
	return validate_n_index_bits(n_index_bits) &&
			validate_n_minhash_bits(n_minhash_bits) &&
			n_index_bits + n_minhash_bits <= MAX_INDEX_AND_MINHASH_BITS;
}

static bool
validate_n_index_bits(uint64_t n_index_bits)
{
	return n_index_bits >= MIN_INDEX_BITS && n_index_bits <= MAX_INDEX_BITS;
}

static bool
validate_n_minhash_bits(uint64_t n_minhash_bits)
{
	return n_minhash_bits == 0 || (n_minhash_bits >= MIN_MINHASH_BITS &&
			n_minhash_bits <= MAX_MINHASH_BITS);
}


//==========================================================
// Local helpers - hmh lib.
//

static bool
verify_hll_sz(const hll_t* hmh, uint32_t expected_sz)
{
	uint32_t sz = hmh_required_sz(hmh->n_index_bits, hmh->n_minhash_bits);

	if (sz != expected_sz) {
		cf_warning(AS_PARTICLE, "verify_hll_sz - bad hll particle - description (%u,%u) sz %u expected-sz %u",
				hmh->n_index_bits, hmh->n_minhash_bits, sz, expected_sz);
		return false;
	}

	return true;
}

static uint32_t
hmh_required_sz(uint8_t n_index_bits, uint8_t n_minhash_bits)
{
	uint32_t sz = registers_sz(n_index_bits, n_minhash_bits);

	return (uint32_t)sizeof(hll_t) + sz;
}

static void
hmh_init(hll_t* hmh, uint8_t n_index_bits, uint8_t n_minhash_bits)
{
	hmh->flags = 0;
	hmh->n_index_bits = n_index_bits;
	hmh->n_minhash_bits = n_minhash_bits;
	hmh->cache = 0;

	size_t sz = registers_sz(n_index_bits, n_minhash_bits);

	memset(hmh->registers, 0, sz);
}

static bool
hmh_add(hll_t* hmh, size_t buf_sz, const uint8_t* buf)
{
	uint16_t register_ix;
	uint64_t new_value;

	hmh_hash(hmh, buf, buf_sz, &register_ix, &new_value);

	uint64_t cur_value = get_register(hmh, register_ix);

	if (cur_value < new_value) {
		set_register(hmh, register_ix, new_value);
		hmh->cache = 0;
		return true;
	}

	return false;
}

static void
hmh_estimate_cardinality_update_cache(hll_t* hmh)
{
	hmh->cache = hmh_estimate_cardinality(hmh);
}

// @misc{ertl2017new,
//     title={New cardinality estimation algorithms for HyperLogLog sketches},
//     author={Otmar Ertl},
//     year={2017},
//     eprint={1702.01284},
//     archivePrefix={arXiv},
//     primaryClass={cs.DS}
// }
// Algorithm 6
static uint64_t
hmh_estimate_cardinality(const hll_t* hmh)
{
	if (hmh->cache != 0) {
		return hmh->cache;
	}

	uint32_t n_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t c[HLL_MAX_VALUE + 1] = {0}; // q_bits + 1

	for (uint32_t r = 0; r < n_registers; r++) {
		c[unpack_register_hll_val(hmh, get_register(hmh, r))]++;
	}

	// TODO - evaluate if allowing q_bits to be 64 was a good choice.
	double z = n_registers * hmh_tau(c[HLL_MAX_VALUE] / (double)n_registers);

	for (uint32_t k = HLL_MAX_VALUE; k > 0; k--) {
		z = 0.5 * (z + c[k]);
	}

	z += n_registers * hmh_sigma(c[0] / (double)n_registers);

	return (uint64_t)(llroundl(hmh_alpha(hmh) * n_registers * n_registers / z));
}

static uint64_t
hmh_estimate_union_cardinality(uint32_t n_hmhs, const hll_t** hmhs)
{
	hll_t template;
	hmh_compatible_template(n_hmhs, hmhs, &template);
	uint32_t sz = hmh_required_sz(template.n_index_bits,
			template.n_minhash_bits);
	uint8_t buf[sz];
	hll_t* hmhunion = (hll_t*)buf;

	hmh_init(hmhunion, template.n_index_bits, template.n_minhash_bits);

	for (uint32_t i = 0; i < n_hmhs; i++) {
		hmh_union(hmhunion, hmhs[i]);
	}

	return hmh_estimate_cardinality(hmhunion);
}

static void
hmh_compatible_template(uint32_t n_hmhs, const hll_t** hmhs, hll_t* template)
{
	uint8_t index_bits = hmhs[0]->n_index_bits;
	uint64_t minhash_bits = hmhs[0]->n_minhash_bits;

	for (uint32_t i = 1; i < n_hmhs; i++) {
		const hll_t* hmh = hmhs[i];

		if (index_bits > hmh->n_index_bits) {
			index_bits = hmh->n_index_bits;
		}

		if (minhash_bits != 0 && minhash_bits != hmh->n_minhash_bits) {
			minhash_bits = 0;
		}
	}

	*template = (hll_t){
		.n_index_bits = index_bits,
		.n_minhash_bits = (uint8_t)minhash_bits
	};
}

static void
hmh_union(hll_t* hmhunion, const hll_t* hmh)
{
	if (hmhunion->n_minhash_bits == 0) {
		return hll_union(hmhunion, hmh);
	}

	// Invariant: hmhunion.n_index_bits <= hmh.n_index_bits

	uint32_t max_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t n_registers = (uint32_t)1 << hmhunion->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t union_r = r & register_mask;
		uint64_t v0 = get_register(hmhunion, union_r);
		uint64_t v1 = get_register(hmh, r);

		if (v0 < v1) {
			set_register(hmhunion, union_r, v1);
		}
	}

	hmhunion->cache = 0;
}

static uint64_t
hmh_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs)
{
	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);

	if (template.n_minhash_bits == 0) {
		return hll_estimate_intersect_cardinality(n_hmhs, hmhs);
	}

	double j = hmh_estimate_similarity(n_hmhs, hmhs);

	if (isnan(j)) {
		return 0;
	}

	uint64_t cu = hmh_estimate_union_cardinality(n_hmhs, hmhs);

	return (uint64_t)llround((double)cu * j);
}

static double
hmh_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs)
{
	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);

	if (template.n_minhash_bits == 0) {
		return hll_estimate_similarity(n_hmhs, hmhs);
	}

	bool one_has_data = false;
	bool one_is_empty = false;

	for (uint32_t h = 0; h < n_hmhs; h++) {
		if (hmh_has_data(hmhs[h])) {
			one_has_data = true;
		}
		else {
			one_is_empty = true;
		}
	}

	if (one_is_empty) {
		return one_has_data ? 0.0 : NAN;
	}

	uint32_t sz = hmh_required_sz(template.n_index_bits,
			template.n_minhash_bits);
	uint8_t agg_buf[sz];
	hll_t* agg_hmh = (hll_t*)agg_buf;

	hmh_init(agg_hmh, template.n_index_bits, template.n_minhash_bits);

	for (uint32_t h = 0; h < n_hmhs; h++) {
		hmh_union(agg_hmh, hmhs[h]);
	}

	uint32_t n_union = hmh_n_used_registers(agg_hmh);

	hmh_init(agg_hmh, template.n_index_bits, template.n_minhash_bits);
	hmh_union(agg_hmh, hmhs[0]);

	for (uint32_t h = 1; h < n_hmhs; h++) {
		hmh_intersect_one(agg_hmh, hmhs[h]);
	}

	uint32_t n_intersect = hmh_n_used_registers(agg_hmh);

	double ec = 0.0;

	if (n_hmhs == 2) {
		uint64_t card0 = hmh_estimate_cardinality(hmhs[0]);
		uint64_t card1 = hmh_estimate_cardinality(hmhs[1]);

		// If n sets is == 2 then estimate collisions.
		ec = hmh_jaccard_estimate_collisions(&template, card0, card1);
	}

	double result = (n_intersect - ec) / (double)n_union;

	if (result < 0.0) {
		result = 0.0;
	}

	return result;
}


//==========================================================
// Local helpers - hmh lib helpers.
//

static uint32_t
registers_sz(uint8_t n_index_bits, uint8_t n_minhash_bits)
{
	return (((uint32_t)HLL_BITS + n_minhash_bits) *
			((uint32_t)1 << n_index_bits) + 7) / 8;
}

static void
hmh_hash(const hll_t* hmh, const uint8_t* element, size_t value_sz,
		uint16_t* register_ix, uint64_t* value)
{
	uint8_t hash[16];

	murmurHash3_x64_128(element, value_sz, hash);

	uint16_t index_mask = (uint16_t)(((uint16_t)1 << hmh->n_index_bits) - 1);

	uint64_t* hash_64 = (uint64_t*)hash;

	*register_ix = (uint16_t)hash_64[0] & index_mask;

	uint64_t minhash_mask = ((uint64_t)1 << hmh->n_minhash_bits) - 1;
	uint64_t minhash_val = (hash_64[0] >> hmh->n_index_bits) & minhash_mask;
	uint8_t hll_val = (uint8_t)(cf_lsb64(hash_64[1]) + 1);

	*value = ((uint64_t)hll_val << hmh->n_minhash_bits) | minhash_val;
}

static uint64_t
get_register(const hll_t* hmh, uint32_t r) {
	uint32_t n_bits = (uint32_t)HLL_BITS + hmh->n_minhash_bits;
	uint32_t bit_offset = n_bits * r;
	uint32_t byte_offset = bit_offset / 8;

	uint32_t n_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t bit_end = n_bits * n_registers;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;
	uint32_t l_bit = bit_offset % 8;

	if (byte_offset > max_offset) {
		l_bit += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	uint64_t* dwords = (uint64_t*)(hmh->registers + byte_offset);
	uint64_t value = cf_swap_from_be64(dwords[0]);
	uint32_t shift_bits = 64 - (l_bit + n_bits);
	uint64_t mask = ((uint64_t)1 << n_bits) - 1;

	return (value >> shift_bits) & mask;
}

static void
set_register(hll_t* hmh, uint32_t r, uint64_t value) {
	uint32_t n_bits = (uint32_t)HLL_BITS + hmh->n_minhash_bits;
	uint32_t bit_offset = n_bits * r;
	uint32_t byte_offset = bit_offset / 8;
	uint64_t mask = (((uint64_t)1 << n_bits) - 1) << (64 - n_bits);
	uint32_t n_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t bit_end = n_bits * n_registers;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;
	uint32_t shift_bits = bit_offset % 8;

	if (byte_offset > max_offset) {
		shift_bits += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	mask >>= shift_bits;
	value <<= (64 - n_bits);
	value >>= shift_bits;

	uint64_t* dwords = (uint64_t*)(hmh->registers + byte_offset);
	uint64_t dword = cf_swap_from_be64(dwords[0]);

	dwords[0] = cf_swap_to_be64((dword & ~mask) | value);
}

static uint8_t
unpack_register_hll_val(const hll_t* hmh, uint64_t value)
{
	return (uint8_t)(value >> hmh->n_minhash_bits);
}

static double
hmh_tau(double val)
{
	if (val == 0.0 || val == 1.0) {
		return 0.0;
	}

	double z_prime;
	double y = 1.0;
	double z = 1 - val;

	do {
		val = sqrt(val);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - val, 2) * y;
	} while (z_prime != z);

	return z / 3;
}

static double
hmh_sigma(double val)
{
	if (val == 1.0) {
		return INFINITY;
	}

	double z_prime;
	double y = 1;
	double z = val;

	do {
		val *= val;
		z_prime = z;
		z += val * y;
		y += y;
	} while (z_prime != z);

	return z;
}

static double
hmh_alpha(const hll_t* hmh)
{
	switch (hmh->n_index_bits) {
	case 4:
		return 0.673;
	case 5:
		return 0.697;
	case 6:
		return 0.709;
	}

	return 0.7213 / (1.0 + 1.079 / (1 << hmh->n_index_bits));
}

static void
hll_union(hll_t* hllunion, const hll_t* hmh)
{
	// Invariant: hmhunion.n_index_bits <= hmh.n_index_bits.

	uint32_t n_registers = (uint32_t)1 << hllunion->n_index_bits;
	uint32_t max_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t union_r = r & register_mask;
		uint8_t v0 = unpack_register_hll_val(hllunion, get_register(hllunion,
				union_r));
		uint8_t v1 = unpack_register_hll_val(hmh, get_register(hmh, r));

		if (v0 < v1) {
			set_register(hllunion, union_r, v1);
		}
	}
}

static uint64_t
hll_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs)
{
	uint64_t cu = hmh_estimate_union_cardinality(n_hmhs, hmhs);
	uint64_t sum_hmhs = 0;

	for (uint32_t i = 0; i < n_hmhs; i++) {
		uint64_t c = hmh_estimate_cardinality(hmhs[i]);

		if (sum_hmhs + c < sum_hmhs) {
			cf_warning(AS_PARTICLE, "intersection cardinality estimate wrapped (%lu) plus (%lu)",
					sum_hmhs, c);
			sum_hmhs = UINT64_MAX;
			break;
		}

		sum_hmhs += c;
	}

	return sum_hmhs > cu ? sum_hmhs - cu : 0;
}

static bool
hmh_has_data(const hll_t* hmh) {
	uint32_t n_dwords = ((uint32_t)1 << hmh->n_index_bits) / 8;
	uint64_t* dwords = (uint64_t*)hmh->registers;

	for (uint32_t i = 0; i < n_dwords; i++) {
		if (dwords[i] != 0) {
			return true;
		}
	}

	return false;
}

static void
hmh_intersect_one(hll_t* intersect_hmh, const hll_t* hmh)
{
	// Invariant: hmhunion.n_index_bits <= hmh.n_index_bits.
	uint32_t n_registers = (uint32_t)1 << intersect_hmh->n_index_bits;
	uint32_t max_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t small_r = r & register_mask;
		uint64_t v0 = get_register(intersect_hmh, small_r);
		uint64_t v1 = get_register(hmh, r);

		set_register(intersect_hmh, small_r, v0 == v1 ? v0 : 0);
	}
}

static uint32_t
hmh_n_used_registers(const hll_t* hmh)
{
	uint32_t n_registers = (uint32_t)1 << hmh->n_index_bits;
	uint32_t count = 0;

	for (uint32_t r = 0; r < n_registers; r++ ) {
		if (get_register(hmh, r)) {
			count++;
		}
	}

	return count;
}

static double
hmh_jaccard_estimate_collisions(const hll_t* template, uint64_t card0,
		uint64_t card1)
{
	double cp = 0.0;
	uint32_t n_registers = (uint32_t)1 << template->n_index_bits;
	uint32_t n_hll_buckets = (uint32_t)1 << HLL_BITS;
	double b1;
	double b2;

	// Note that i must be signed.
	for (int32_t i = 1; (uint32_t)i <= n_hll_buckets; i++) {
		if ((uint32_t)i != n_hll_buckets) {
			b1 = pow(2, -i);
			b2 = pow(2, -i + 1);
		}
		else {
			b1 = 0.0;
			b2 = pow(2, -i + 1);
		}

		b1 /= n_registers;
		b2 /= n_registers;

		double pr_x = pow(1 - b1, (double)card0) - pow(1 - b2, (double)card0);
		double pr_y = pow(1 - b1, (double)card1) - pow(1 - b2, (double)card1);

		cp += pr_x * pr_y;
	}

	return cp * n_registers / pow(2, template->n_minhash_bits);
}

static double
hll_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs)
{
	uint64_t intersect_est = hll_estimate_intersect_cardinality(n_hmhs, hmhs);
	uint64_t union_est = hmh_estimate_union_cardinality(n_hmhs, hmhs);

	return (double)intersect_est / (double)union_est;
}
