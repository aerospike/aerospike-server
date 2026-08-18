/*
 * query_plan_candidates.c
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

#include "query/query_plan_candidates.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_hash_math.h"

#include "log.h"
#include "msgpack_in.h"
#include "vector.h"

#include "base/cdt_wire.h"
#include "base/datamodel.h"
#include "exp/exp.h"
#include "exp/exp_rt.h"
#include "exp/exp_wire.h"
#include "sindex/sindex.h"
#include "sindex/sindex_manager.h"

//==========================================================
// Typedefs & constants.
//

// The candidate ctx capacity must cover the largest raw ctx the server
// accepts at sindex-create (CTX_B64_MAX_SZ of base64, 4:3) - the header
// can't reference the constant directly (sindex.h includes it first).
COMPILER_ASSERT(AS_EXP_SINDEX_CTX_MAX == CTX_B64_MAX_SZ / 4 * 3);

// geo_bound_val is a borrowed pointer, so its oversize ceiling can (and
// should) match the wire path's real limit rather than a smaller guess.
COMPILER_ASSERT(AS_EXP_SINDEX_GEO_BOUND_VAL_MAX == MAX_GEOJSON_KSIZE);

// bound_val is a copied buffer sized for the largest string/blob literal
// extract_literal_bound/parse_probe_literal will accept - if MAX_STRING_KSIZE
// or MAX_BLOB_KSIZE is ever raised without this, long literals silently stop
// being indexable with no build failure.
COMPILER_ASSERT(AS_EXP_SINDEX_BOUND_VAL_MAX == MAX_STRING_KSIZE);
COMPILER_ASSERT(AS_EXP_SINDEX_BOUND_VAL_MAX == MAX_BLOB_KSIZE);

// Short aliases for the op-node layouts, matching exp.c's local convention.
typedef struct exp_op_base_mem_s op_base_mem;
typedef struct exp_op_var_s op_var;
typedef struct exp_op_value_blob_s op_value_blob;
typedef struct exp_op_value_int_s op_value_int;
typedef struct exp_op_value_bool_s op_value_bool;
typedef struct exp_op_call_s op_call;
typedef exp_op_table_entry op_table_entry;

#define op_table exp_op_table

typedef enum {
	WALK_OK,
	WALK_FILTERED_OUT,
	WALK_PI_FALLBACK,
	WALK_PARAMETER_ERROR,
} walk_status;

typedef struct cmp_operand_spans_s {
	const uint8_t* left_ptr;
	uint32_t left_start_ix;
	uint32_t left_end_ix;
	const uint8_t* right_ptr;
	uint32_t right_start_ix;
	uint32_t cmp_end_ix;
} cmp_operand_spans;

// Which side of a comparison is the EXP_CALL (CDT path read) and which is
// the literal, with the comparison direction flipped as needed so the call
// is always logically on the left (found == false means neither side is a
// well-formed EXP_CALL subtree).
typedef struct call_lit_operands_s {
	const uint8_t* call_ptr;
	uint32_t call_ix;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;
	exp_op_code effective_cmp;
	bool found;
} call_lit_operands;

typedef struct parsed_literal_s {
	as_particle_type ktype;
	int64_t bval; // low bound when is_range, the single value otherwise
	int64_t bval_high; // high bound - only meaningful when is_range
	bool is_range; // true for a CDT interval-selector probe (e.g. [=10:30])
	const uint8_t* bound_val;
	uint32_t bound_val_sz;
	bool parsed;
	bool oversized;
} parsed_literal;

typedef struct path_step_s {
	uint64_t ctx_type;
	const uint8_t* val_ptr;
	uint32_t val_sz;
	bool is_range; // True if val_ptr/val_sz holds a [lo, hi] pair, not one value.
} path_step;

#define PATH_READ_MAX_STEPS 7

// A CDT path read recognized on a bare bin. Holds the step list, the read
// kind (EXISTS / COUNT / VALUE), and, if the last step's value is a scalar
// literal, that literal parsed out for containment matching.
typedef struct cdt_path_read_s {
	path_step steps[PATH_READ_MAX_STEPS];
	uint32_t n_steps;
	result_type_t result_type;
	parsed_literal last_value;
	const op_var* bin_op;
	bool valid;
} cdt_path_read;

typedef struct resolved_bin_literal_s {
	op_var* bin_op;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;
	exp_op_code effective_cmp;
	bool found;

	// True when the non-literal side is a sub-expression, not a bare bin
	// ref - then bin_op is NULL and exp_ptr/start_ix/end_ix locate it instead.
	bool is_exp;
	const uint8_t* exp_ptr;
	uint32_t exp_start_ix;
	uint32_t exp_end_ix;

	// Set when an operand span was probed as a CDT-path-read call while
	// resolving bin/literal sides, so extract_call_cmp_candidate can reuse
	// the parse instead of re-walking the same call subtree.
	bool cdt_probe_done;
	const uint8_t* cdt_probe_ptr;
	uint32_t cdt_probe_ix;
	cdt_path_read cdt_probe_result;
} resolved_bin_literal;

#define SINDEX_CONST_FOLD_MAX_DEPTH 32

//==========================================================
// Forward declarations.
//

static walk_status walk_subtree(const as_exp* exp, const uint8_t** instr_ptr,
		uint32_t* op_ix, uint32_t parent_end_ix, cf_vector* candidates);
static walk_status extract_candidate(const as_exp* exp, const uint8_t* cmp_ptr,
		uint32_t cmp_start_ix, cf_vector* candidates);
static void cmp_locate_operands(const uint8_t* cmp_ptr, uint32_t cmp_start_ix,
		cmp_operand_spans* spans_r);
static resolved_bin_literal resolve_bin_and_literal(const cmp_operand_spans* spans,
		exp_op_code cmp_code);
static parsed_literal extract_literal_bound(const uint8_t* lit_ptr,
		uint32_t lit_start_ix, uint32_t lit_end_ix);
static walk_status merge_candidate(cf_vector* candidates, const char* bin_name,
		uint32_t bin_name_sz, as_sindex_type itype, const uint8_t* ctx_buf,
		uint32_t ctx_buf_sz, exp_op_code cmp_code, const parsed_literal* literal);
static bool parse_path_step_value(msgpack_in* mp, path_step* step,
		uint64_t ctx_type);
static bool parse_path_step(msgpack_in* mp, path_step* step);
static bool parse_get_op_tail(msgpack_in* mp, uint64_t op_code,
		cdt_path_read* pr);
static cdt_path_read parse_cdt_path_read(const uint8_t* call_ptr,
		uint32_t call_ix);
static bool is_cdt_path_read_subtree(const uint8_t* ptr, uint32_t start_ix,
		uint32_t end_ix, cdt_path_read* pr_r);
static bool is_singular_nav_step(const path_step* step);
static as_sindex_type itype_from_selector(uint64_t ctx_type);
static bool build_ctx_buf(const path_step* steps, uint32_t n_steps,
		uint8_t* buf, uint32_t* sz_r);
static walk_status append_collection_candidate(const as_exp* exp,
		cf_vector* candidates, const op_var* bin_op, as_sindex_type itype,
		const parsed_literal* literal, const uint8_t* ctx_buf,
		uint32_t ctx_buf_sz);
static walk_status append_containment_candidate(const as_exp* exp,
		cf_vector* candidates, const cdt_path_read* pr);
static walk_status extract_in_list_candidate(const as_exp* exp,
		const uint8_t* node_ptr, uint32_t node_ix, cf_vector* candidates);
static walk_status extract_exists_call_candidate(const as_exp* exp,
		const uint8_t* call_ptr, uint32_t call_ix, cf_vector* candidates);
static bool try_bool_literal(const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix, bool* value_r);
static call_lit_operands locate_call_and_literal_operands(const cmp_operand_spans* spans,
		exp_op_code cmp_code);
static walk_status extract_exists_cmp_candidate(const as_exp* exp,
		cf_vector* candidates, const cdt_path_read* pr, const uint8_t* lit_ptr,
		uint32_t lit_start_ix, uint32_t lit_end_ix, exp_op_code effective_cmp);
static walk_status extract_count_cmp_candidate(const as_exp* exp,
		cf_vector* candidates, const cdt_path_read* pr,
		const parsed_literal* cmp_lit, exp_op_code effective_cmp);
static walk_status extract_value_cmp_candidate(const as_exp* exp,
		cf_vector* candidates, const cdt_path_read* pr,
		const parsed_literal* cmp_lit, exp_op_code effective_cmp);
static walk_status extract_call_cmp_candidate(const as_exp* exp,
		const cmp_operand_spans* spans, exp_op_code cmp_code,
		cf_vector* candidates, const resolved_bin_literal* sides);
static bool is_indexable_deterministic_subtree(const uint8_t* ptr,
		uint32_t start_ix, uint32_t end_ix);
static walk_status append_exp_candidate(const as_exp* exp,
		cf_vector* candidates, const uint8_t* subtree_ptr,
		uint32_t subtree_start_ix, uint32_t subtree_end_ix,
		exp_op_code cmp_code, const parsed_literal* literal);
static bool intersect_into(as_exp_sindex_candidate* c, int64_t lo, int64_t hi);
static as_exp_sindex_candidate* find_candidate(cf_vector* candidates,
		const char* bin_name, uint32_t bin_name_sz, as_particle_type ktype,
		as_sindex_type itype, const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
static bool cmp_to_interval(as_particle_type ktype, exp_op_code cmp_code,
		int64_t bval, int64_t* lo_r, int64_t* hi_r);
static exp_op_code reverse_cmp(exp_op_code code);
static bool is_hash_type(as_particle_type ktype);
static void skip_subtree(const uint8_t** instr_ptr, uint32_t* op_ix,
		uint32_t instr_end_ix);
static bool resolve_geo_literal(const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix, parsed_literal* out);
static walk_status append_geo_candidate(cf_vector* candidates,
		const char* bin_name, uint32_t bin_name_sz, const parsed_literal* literal,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
static walk_status extract_geo_ctx_candidate(const as_exp* exp,
		const cmp_operand_spans* spans, cf_vector* candidates);
static walk_status extract_geo_candidate(const as_exp* exp,
		const uint8_t* cmp_ptr, uint32_t cmp_start_ix, cf_vector* candidates);
static walk_status extract_geo_eq_candidate(const as_exp* exp,
		const cmp_operand_spans* spans, exp_op_code cmp_code,
		cf_vector* candidates);

//==========================================================
// Local helpers - sindex candidate extraction.
//

static int64_t
get_bval(op_base_mem* right, as_particle_type* ktype_r)
{
	switch (right->code) {
	case EXP_VOP_VALUE_INT:
		*ktype_r = AS_PARTICLE_TYPE_INTEGER;
		return ((op_value_int*)right)->value;
	case EXP_VOP_VALUE_STR: {
		op_value_blob* val = (op_value_blob*)right;

		*ktype_r = AS_PARTICLE_TYPE_STRING;
		return (int64_t)cf_wyhash64(val->value, val->value_sz);
	}
	case EXP_VOP_VALUE_BLOB: {
		op_value_blob* val = (op_value_blob*)right;

		*ktype_r = AS_PARTICLE_TYPE_BLOB;
		return (int64_t)cf_wyhash64(val->value, val->value_sz);
	}
	case EXP_VOP_VALUE_GEO:
		*ktype_r = AS_PARTICLE_TYPE_GEOJSON;
		return 0;
	default:
		*ktype_r = AS_PARTICLE_TYPE_NULL;
		return 0;
	}
}

static bool
try_fold_int_subtree(const uint8_t** pp, uint32_t* op_ix,
		uint32_t subtree_end_ix, int depth, int64_t* out)
{
	if (depth > SINDEX_CONST_FOLD_MAX_DEPTH || *op_ix >= subtree_end_ix) {
		return false;
	}

	op_base_mem* ob = (op_base_mem*)*pp;

	*pp += op_table[ob->code].size;
	(*op_ix)++;

	switch (ob->code) {
	case EXP_VOP_VALUE_INT:
		*out = ((op_value_int*)ob)->value;
		return true;
	case EXP_TO_INT: {
		uint32_t end = ob->instr_end_ix;

		if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, out)) {
			return false;
		}

		return *op_ix == end;
	}
	case EXP_ADD:
	case EXP_SUB:
	case EXP_MUL:
	case EXP_MOD: {
		exp_op_code op = ob->code;
		uint32_t end = ob->instr_end_ix;

		if (*op_ix >= end) {
			return false;
		}

		int64_t acc;

		if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, &acc)) {
			return false;
		}

		if (op == EXP_SUB && *op_ix == end) {
			if (acc == INT64_MIN) {
				return false;
			}

			*out = -acc;
			return true;
		}

		if (op == EXP_MUL) {
			while (*op_ix < end) {
				int64_t v;

				if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, &v)) {
					return false;
				}

				if (__builtin_mul_overflow(acc, v, &acc)) {
					return false;
				}
			}
		}
		else if (op == EXP_ADD) {
			while (*op_ix < end) {
				int64_t v;

				if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, &v)) {
					return false;
				}

				if (__builtin_add_overflow(acc, v, &acc)) {
					return false;
				}
			}
		}
		else if (op == EXP_SUB) {
			while (*op_ix < end) {
				int64_t v;

				if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, &v)) {
					return false;
				}

				if (__builtin_sub_overflow(acc, v, &acc)) {
					return false;
				}
			}
		}
		else { // EXP_MOD - two operands only (matches eval_mod).
			int64_t v;

			if (*op_ix >= end) {
				return false;
			}

			if (! try_fold_int_subtree(pp, op_ix, end, depth + 1, &v)) {
				return false;
			}

			if (v == 0 || *op_ix != end || (acc == INT64_MIN && v == -1)) {
				return false;
			}

			*out = acc % v;
			return true;
		}

		*out = acc;
		return *op_ix == end;
	}
	default:
		return false;
	}
}

static bool
try_literal_from_subtree(const uint8_t** pp, uint32_t* op_ix,
		uint32_t subtree_end_ix, int depth, as_particle_type* ktype_r,
		int64_t* bval_r)
{
	if (*op_ix >= subtree_end_ix) {
		return false;
	}

	op_base_mem* ob = (op_base_mem*)*pp;

	if (subtree_end_ix == *op_ix + 1) {
		*bval_r = get_bval(ob, ktype_r);

		if (*ktype_r != AS_PARTICLE_TYPE_NULL) {
			*pp += op_table[ob->code].size;
			(*op_ix)++;
			return true;
		}

		return false;
	}

	if (try_fold_int_subtree(pp, op_ix, subtree_end_ix, depth, bval_r)) {
		*ktype_r = AS_PARTICLE_TYPE_INTEGER;
		return true;
	}

	return false;
}

static bool
consume_bin_ref_chain(const uint8_t** pp, uint32_t* op_ix,
		uint32_t subtree_end_ix, op_var** bin_r)
{
	if (*op_ix >= subtree_end_ix) {
		return false;
	}

	op_base_mem* ob = (op_base_mem*)*pp;

	while (ob->code == EXP_TO_INT) {
		*pp += op_table[ob->code].size;
		(*op_ix)++;

		if (*op_ix >= subtree_end_ix) {
			return false;
		}

		ob = (op_base_mem*)*pp;
	}

	if (ob->code != EXP_BIN) {
		return false;
	}

	*bin_r = (op_var*)ob;
	*pp += op_table[ob->code].size;
	(*op_ix)++;

	return *op_ix == subtree_end_ix;
}

// LITERAL <cmp> BIN is equivalent to BIN <cmp'> LITERAL with cmp' derived here.
static exp_op_code
reverse_cmp(exp_op_code code)
{
	switch (code) {
	case EXP_CMP_EQ:
	case EXP_CMP_NE:
		return code;
	case EXP_CMP_GT:
		return EXP_CMP_LT;
	case EXP_CMP_GE:
		return EXP_CMP_LE;
	case EXP_CMP_LT:
		return EXP_CMP_GT;
	case EXP_CMP_LE:
		return EXP_CMP_GE;
	default:
		return code;
	}
}

static bool
cmp_to_interval(as_particle_type ktype, exp_op_code cmp_code, int64_t bval,
		int64_t* lo_r, int64_t* hi_r)
{
	if (is_hash_type(ktype) && cmp_code != EXP_CMP_EQ) {
		return false;
	}

	*lo_r = INT64_MIN;
	*hi_r = INT64_MAX;

	switch (cmp_code) {
	case EXP_CMP_EQ:
		*lo_r = bval;
		*hi_r = bval;
		break;
	case EXP_CMP_GT:
		if (ktype == AS_PARTICLE_TYPE_INTEGER) {
			if (bval == INT64_MAX) {
				return false;
			}

			*lo_r = bval + 1;
		}
		else {
			*lo_r = bval;
		}

		break;
	case EXP_CMP_GE:
		*lo_r = bval;
		break;
	case EXP_CMP_LT:
		if (ktype == AS_PARTICLE_TYPE_INTEGER) {
			if (bval == INT64_MIN) {
				return false;
			}

			*hi_r = bval - 1;
		}
		else {
			*hi_r = bval;
		}

		break;
	case EXP_CMP_LE:
		*hi_r = bval;
		break;
	default:
		return false;
	}

	return true;
}

static bool
intersect_into(as_exp_sindex_candidate* c, int64_t lo, int64_t hi)
{
	if (lo > c->bval_low) {
		c->bval_low = lo;
	}

	if (hi < c->bval_high) {
		c->bval_high = hi;
	}

	c->is_range = c->bval_low != c->bval_high;

	return c->bval_low <= c->bval_high;
}

static as_exp_sindex_candidate*
find_candidate(cf_vector* candidates, const char* bin_name,
		uint32_t bin_name_sz, as_particle_type ktype, as_sindex_type itype,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz)
{
	for (uint32_t i = 0; i < cf_vector_size(candidates); i++) {
		as_exp_sindex_candidate* c =
				(as_exp_sindex_candidate*)cf_vector_getp(candidates, i);

		if (c->ktype == ktype && c->itype == itype &&
				c->bin_name_sz == bin_name_sz &&
				memcmp(c->bin_name, bin_name, bin_name_sz) == 0 &&
				c->ctx_buf_sz == ctx_buf_sz &&
				(ctx_buf_sz == 0 || memcmp(c->ctx_buf, ctx_buf, ctx_buf_sz) == 0)) {
			return c;
		}
	}

	return NULL;
}

static walk_status
merge_candidate(cf_vector* candidates, const char* bin_name,
		uint32_t bin_name_sz, as_sindex_type itype, const uint8_t* ctx_buf,
		uint32_t ctx_buf_sz, exp_op_code cmp_code, const parsed_literal* literal)
{
	const as_particle_type ktype = literal->ktype;

	int64_t lo;
	int64_t hi;

	if (! cmp_to_interval(ktype, cmp_code, literal->bval, &lo, &hi)) {
		if (is_hash_type(ktype) || cmp_code == EXP_CMP_NE) {
			return WALK_PI_FALLBACK;
		}

		cf_ticker_warning(AS_EXP,
				"query-plan: unsatisfiable on bin '%.*s' (ktype=%d, itype=%d)",
				(int)bin_name_sz, bin_name, ktype, itype);
		return WALK_FILTERED_OUT;
	}

	as_exp_sindex_candidate* existing = find_candidate(candidates, bin_name,
			bin_name_sz, ktype, itype, ctx_buf, ctx_buf_sz);

	if (existing != NULL) {
		if (! intersect_into(existing, lo, hi)) {
			cf_ticker_warning(AS_EXP,
					"query-plan: contradiction on bin '%s' (ktype=%d, itype=%d)",
					existing->bin_name, existing->ktype, existing->itype);
			return WALK_FILTERED_OUT;
		}

		if (ktype == AS_PARTICLE_TYPE_INTEGER &&
				existing->bval_low > existing->bval_high) {
			return WALK_FILTERED_OUT;
		}

		return WALK_OK;
	}

	if (ktype == AS_PARTICLE_TYPE_INTEGER && lo > hi) {
		return WALK_FILTERED_OUT;
	}

	if (bin_name_sz >= AS_BIN_NAME_MAX_SZ) {
		cf_ticker_warning(AS_EXP, "query-plan: bin_name_sz exceeds max %u",
				AS_BIN_NAME_MAX_SZ);
		return WALK_PARAMETER_ERROR;
	}

	as_exp_sindex_candidate fresh = {
		.bin_name_sz = bin_name_sz,
		.ktype = ktype,
		.itype = itype,
		.bval_low = lo,
		.bval_high = hi,
		.is_range = lo != hi,
		.bound_val_sz = literal->bound_val_sz,
		.ctx_buf_sz = ctx_buf_sz,
	};

	memcpy(fresh.bin_name, bin_name, bin_name_sz);
	fresh.bin_name[bin_name_sz] = '\0';

	if (literal->bound_val_sz != 0) {
		cf_assert(literal->bound_val_sz <= AS_EXP_SINDEX_BOUND_VAL_MAX, AS_EXP,
				"query-plan: bound_val_sz %u > max %u", literal->bound_val_sz,
				AS_EXP_SINDEX_BOUND_VAL_MAX);
		memcpy(fresh.bound_val, literal->bound_val, literal->bound_val_sz);
	}

	if (ctx_buf_sz != 0) {
		memcpy(fresh.ctx_buf, ctx_buf, ctx_buf_sz);
	}

	cf_vector_append(candidates, &fresh);

	return WALK_OK;
}

static void
cmp_locate_operands(const uint8_t* cmp_ptr, uint32_t cmp_start_ix,
		cmp_operand_spans* spans_r)
{
	op_base_mem* cmp = (op_base_mem*)cmp_ptr;
	const op_table_entry* cmp_entry = &op_table[cmp->code];
	const uint8_t* arg_ptr = cmp_ptr + cmp_entry->size;
	const uint32_t left_start_ix = cmp_start_ix + 1;

	op_base_mem* left_root = (op_base_mem*)arg_ptr;
	const uint32_t left_end_ix = left_root->instr_end_ix;

	const uint8_t* right_ptr = arg_ptr;
	uint32_t right_ix = left_start_ix;

	skip_subtree(&right_ptr, &right_ix, left_end_ix);

	*spans_r = (cmp_operand_spans){
		.left_ptr = arg_ptr,
		.left_start_ix = left_start_ix,
		.left_end_ix = left_end_ix,
		.right_ptr = right_ptr,
		.right_start_ix = right_ix,
		.cmp_end_ix = cmp->instr_end_ix,
	};
}

// pr_r is always written when ptr is an EXP_CALL node (even if the parse
// turns out invalid), so callers can cache the parse and reuse it instead of
// re-walking the same call subtree later.
static bool
is_cdt_path_read_subtree(const uint8_t* ptr, uint32_t start_ix, uint32_t end_ix,
		cdt_path_read* pr_r)
{
	const op_base_mem* ob = (const op_base_mem*)ptr;

	if (ob->code != EXP_CALL || ob->instr_end_ix != end_ix) {
		return false;
	}

	*pr_r = parse_cdt_path_read(ptr, start_ix);

	return pr_r->valid;
}

static resolved_bin_literal
resolve_bin_and_literal(const cmp_operand_spans* spans, exp_op_code cmp_code)
{
	resolved_bin_literal sides = {
		.effective_cmp = cmp_code,
	};

	const uint8_t* lp = spans->left_ptr;
	uint32_t lix = spans->left_start_ix;
	op_var* lbin = NULL;

	if (consume_bin_ref_chain(&lp, &lix, spans->left_end_ix, &lbin)) {
		sides.bin_op = lbin;
		sides.lit_ptr = spans->right_ptr;
		sides.lit_start_ix = spans->right_start_ix;
		sides.lit_end_ix = spans->cmp_end_ix;
		sides.found = true;
		return sides;
	}

	const uint8_t* rp = spans->right_ptr;
	uint32_t rix = spans->right_start_ix;
	op_var* rbin = NULL;

	if (consume_bin_ref_chain(&rp, &rix, spans->cmp_end_ix, &rbin)) {
		sides.bin_op = rbin;
		sides.lit_ptr = spans->left_ptr;
		sides.lit_start_ix = spans->left_start_ix;
		sides.lit_end_ix = spans->left_end_ix;
		sides.effective_cmp = reverse_cmp(cmp_code);
		sides.found = true;
		return sides;
	}

	// Neither side is a bare bin ref, so treat this as EXP <cmp> LITERAL
	// (flipping if needed). Each exp candidate is kept separate rather than
	// merged like bin candidates are, since merging would need a structural
	// comparison we skip for now - correct, just less selective.
	cdt_path_read right_pr = { 0 };
	bool right_is_cdt = is_cdt_path_read_subtree(spans->right_ptr,
			spans->right_start_ix, spans->cmp_end_ix, &right_pr);

	if (((const op_base_mem*)spans->right_ptr)->code == EXP_CALL) {
		sides.cdt_probe_done = true;
		sides.cdt_probe_ptr = spans->right_ptr;
		sides.cdt_probe_ix = spans->right_start_ix;
		sides.cdt_probe_result = right_pr;
	}

	if (! right_is_cdt &&
			is_indexable_deterministic_subtree(spans->right_ptr,
					spans->right_start_ix, spans->cmp_end_ix)) {
		parsed_literal left_lit = extract_literal_bound(spans->left_ptr,
				spans->left_start_ix, spans->left_end_ix);

		if (left_lit.parsed) {
			sides.is_exp = true;
			sides.exp_ptr = spans->right_ptr;
			sides.exp_start_ix = spans->right_start_ix;
			sides.exp_end_ix = spans->cmp_end_ix;
			sides.lit_ptr = spans->left_ptr;
			sides.lit_start_ix = spans->left_start_ix;
			sides.lit_end_ix = spans->left_end_ix;
			sides.effective_cmp = reverse_cmp(cmp_code);
			sides.found = true;
			return sides;
		}
	}

	cdt_path_read left_pr = { 0 };
	bool left_is_cdt = is_cdt_path_read_subtree(spans->left_ptr,
			spans->left_start_ix, spans->left_end_ix, &left_pr);

	if (((const op_base_mem*)spans->left_ptr)->code == EXP_CALL) {
		sides.cdt_probe_done = true;
		sides.cdt_probe_ptr = spans->left_ptr;
		sides.cdt_probe_ix = spans->left_start_ix;
		sides.cdt_probe_result = left_pr;
	}

	if (! left_is_cdt &&
			is_indexable_deterministic_subtree(spans->left_ptr,
					spans->left_start_ix, spans->left_end_ix)) {
		parsed_literal right_lit = extract_literal_bound(spans->right_ptr,
				spans->right_start_ix, spans->cmp_end_ix);

		if (right_lit.parsed) {
			sides.is_exp = true;
			sides.exp_ptr = spans->left_ptr;
			sides.exp_start_ix = spans->left_start_ix;
			sides.exp_end_ix = spans->left_end_ix;
			sides.lit_ptr = spans->right_ptr;
			sides.lit_start_ix = spans->right_start_ix;
			sides.lit_end_ix = spans->cmp_end_ix;
			sides.effective_cmp = cmp_code;
			sides.found = true;
		}
	}

	return sides;
}

// True if no record key or non-digest-metadata read appears in
// [start_ix, end_ix) - a flat scan suffices since preorder linearization
// already covers the whole subtree. OR/NOT/EXCLUSIVE are NOT rejected here:
// this runs on a comparison operand (a value-producing subtree), where those
// are ordinary deterministic combinators (e.g. cond(or($.a > 1, $.b > 2), 1,
// 0) == 1) - the conjunct-level poisoning check for them lives separately in
// walk_subtree.
static bool
is_indexable_deterministic_subtree(const uint8_t* ptr, uint32_t start_ix,
		uint32_t end_ix)
{
	uint32_t ix = start_ix;

	while (ix < end_ix) {
		op_base_mem* ob = (op_base_mem*)ptr;

		switch (ob->code) {
		case EXP_REC_KEY:
		case EXP_META_DEVICE_SIZE:
		case EXP_META_LAST_UPDATE:
		case EXP_META_SINCE_UPDATE:
		case EXP_META_VOID_TIME:
		case EXP_META_TTL:
		case EXP_META_SET_NAME:
		case EXP_META_KEY_EXISTS:
		case EXP_META_IS_TOMBSTONE:
		case EXP_META_MEMORY_SIZE:
		case EXP_META_RECORD_SIZE:
			return false;
		default:
			break;
		}

		ptr += op_table[ob->code].size;
		ix++;
	}

	return true;
}

static walk_status
append_exp_candidate(const as_exp* exp, cf_vector* candidates,
		const uint8_t* subtree_ptr, uint32_t subtree_start_ix,
		uint32_t subtree_end_ix, exp_op_code cmp_code,
		const parsed_literal* literal)
{
	const as_particle_type ktype = literal->ktype;
	const uint8_t* bound_val = literal->bound_val;
	const uint32_t bound_val_sz = literal->bound_val_sz;

	int64_t lo;
	int64_t hi;

	if (! cmp_to_interval(ktype, cmp_code, literal->bval, &lo, &hi)) {
		if (is_hash_type(ktype) || cmp_code == EXP_CMP_NE) {
			return WALK_PI_FALLBACK;
		}

		cf_ticker_warning(AS_EXP,
				"query-plan: unsatisfiable on exp-based candidate (ktype=%d)",
				ktype);
		return WALK_FILTERED_OUT;
	}

	if (ktype == AS_PARTICLE_TYPE_INTEGER && lo > hi) {
		return WALK_FILTERED_OUT;
	}

	as_exp_sindex_candidate fresh = {
		.ktype = ktype,
		.itype = AS_SINDEX_ITYPE_DEFAULT,
		.bval_low = lo,
		.bval_high = hi,
		.is_range = lo != hi,
		.bound_val_sz = bound_val_sz,
		.is_exp = true,
		.exp_subtree_ptr = subtree_ptr,
		.exp_subtree_start_ix = subtree_start_ix,
		.exp_subtree_end_ix = subtree_end_ix,
		.owner_exp = exp,
	};

	if (bound_val_sz != 0) {
		cf_assert(bound_val_sz <= AS_EXP_SINDEX_BOUND_VAL_MAX, AS_EXP,
				"query-plan: bound_val_sz %u > max %u", bound_val_sz,
				AS_EXP_SINDEX_BOUND_VAL_MAX);
		memcpy(fresh.bound_val, bound_val, bound_val_sz);
	}

	cf_vector_append(candidates, &fresh);

	return WALK_OK;
}

static void
warn_literal_oversized(void)
{
	cf_ticker_warning(AS_EXP,
			"query-plan: literal size exceeds sindex bound max %u - "
			"term not indexed",
			AS_EXP_SINDEX_BOUND_VAL_MAX);
}

static parsed_literal
extract_literal_bound(const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix)
{
	parsed_literal literal = { 0 };

	const uint8_t* p = lit_ptr;
	uint32_t ix = lit_start_ix;

	if (! try_literal_from_subtree(&p, &ix, lit_end_ix, 0, &literal.ktype,
				&literal.bval)) {
		return literal;
	}

	if (ix != lit_end_ix) {
		return literal;
	}

	if (literal.ktype != AS_PARTICLE_TYPE_INTEGER &&
			literal.ktype != AS_PARTICLE_TYPE_STRING &&
			literal.ktype != AS_PARTICLE_TYPE_BLOB) {
		return literal;
	}

	if (is_hash_type(literal.ktype)) {
		op_base_mem* lit_ob = (op_base_mem*)lit_ptr;

		if (lit_ob->code != EXP_VOP_VALUE_STR &&
				lit_ob->code != EXP_VOP_VALUE_BLOB) {
			return literal;
		}

		op_value_blob* blob_lit = (op_value_blob*)lit_ob;

		if (blob_lit->value_sz >= AS_EXP_SINDEX_BOUND_VAL_MAX) {
			warn_literal_oversized();
			return literal;
		}

		literal.bound_val = blob_lit->value;
		literal.bound_val_sz = blob_lit->value_sz;
	}

	literal.parsed = true;

	return literal;
}

//==========================================================
// Local helpers - collection (itype) candidate extraction.
//

// Parse one msgpack scalar into a candidate-compatible literal - int, or
// string / blob (leading as_bytes particle-type byte stripped, raw bytes
// hashed the same way sindex bvals are). Only called for non-range terminal
// values (see parse_cdt_path_read) - a true CDT interval selector's [lo, hi]
// tail is a distinct wire shape (two adjacent scalar args, not one msgpack
// list value) and is decoded separately via parse_path_step_range_value /
// the pr.last_value.is_range branch. A msgpack list here is therefore always
// an actual by-value literal (e.g. LIST_GET_BY_VALUE([1,2])), never a range
// probe, and is left unrecognized (PI fallback) rather than misread as one.
static bool
parse_probe_literal(msgpack_in* mp, parsed_literal* value)
{
	switch (msgpack_peek_type(mp)) {
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT: {
		int64_t val;

		if (! msgpack_get_int64(mp, &val)) {
			return false;
		}

		value->ktype = AS_PARTICLE_TYPE_INTEGER;
		value->bval = val;
		break;
	}
	case MSGPACK_TYPE_STRING:
	case MSGPACK_TYPE_BYTES: {
		uint32_t sz;
		const uint8_t* buf = msgpack_get_bin(mp, &sz);

		if (buf == NULL || sz == 0) {
			return false;
		}

		// First byte is the as_bytes particle type.
		if (buf[0] == AS_PARTICLE_TYPE_STRING) {
			value->ktype = AS_PARTICLE_TYPE_STRING;
		}
		else if (buf[0] == AS_PARTICLE_TYPE_BLOB) {
			value->ktype = AS_PARTICLE_TYPE_BLOB;
		}
		else {
			return false;
		}

		buf++;
		sz--;

		if (sz >= AS_EXP_SINDEX_BOUND_VAL_MAX) {
			warn_literal_oversized();
			value->oversized = true;
			return false;
		}

		value->bval = (int64_t)cf_wyhash64(buf, sz);
		value->bound_val = buf;
		value->bound_val_sz = sz;
		break;
	}
	default:
		return false;
	}

	value->parsed = true;

	return true;
}

static bool
parse_path_step_value(msgpack_in* mp, path_step* step, uint64_t ctx_type)
{
	step->ctx_type = ctx_type;
	step->val_ptr = mp->buf + mp->offset;

	uint32_t sz = msgpack_sz(mp);

	if (sz == 0) {
		return false;
	}

	step->val_sz = sz;

	return true;
}

// One verbatim (type, value) pair from a CONTEXT_EVAL ctx pair list.
static bool
parse_path_step(msgpack_in* mp, path_step* step)
{
	uint64_t ctx_type;

	if (! msgpack_get_uint64(mp, &ctx_type)) {
		return false;
	}

	return parse_path_step_value(mp, step, ctx_type);
}

// Slice an interval selector's [lo, hi] tail: two adjacent scalar wire args
// (not a single msgpack value, and not wrapped in a nested list) - capture
// both as one contiguous byte span, marked is_range so the caller knows to
// decode two sequential values out of it rather than one.
static bool
parse_path_step_range_value(msgpack_in* mp, path_step* step, uint64_t ctx_type)
{
	step->ctx_type = ctx_type;
	step->is_range = true;
	step->val_ptr = mp->buf + mp->offset;

	uint32_t lo_sz = msgpack_sz(mp);

	if (lo_sz == 0) {
		return false;
	}

	uint32_t hi_sz = msgpack_sz(mp);

	if (hi_sz == 0) {
		return false;
	}

	step->val_sz = lo_sz + hi_sz;

	return true;
}

static bool
parse_get_op_tail(msgpack_in* mp, uint64_t op_code, cdt_path_read* pr)
{
	uint64_t ctx_type;
	bool is_interval;

	switch (op_code) {
	case AS_CDT_OP_LIST_GET_BY_VALUE:
		ctx_type = AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE;
		is_interval = false;
		break;
	case AS_CDT_OP_MAP_GET_BY_KEY:
		ctx_type = AS_CDT_CTX_MAP | AS_CDT_CTX_KEY;
		is_interval = false;
		break;
	case AS_CDT_OP_MAP_GET_BY_VALUE:
		ctx_type = AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE;
		is_interval = false;
		break;
	case AS_CDT_OP_LIST_GET_BY_INDEX:
		// Plain positional list read (e.g. $.scoreList.[2]) - already
		// recognized as a singular nav step by is_singular_nav_step, this
		// switch just never produced the ctx_type for it.
		ctx_type = AS_CDT_CTX_LIST | AS_CDT_CTX_INDEX;
		is_interval = false;
		break;
	case AS_CDT_OP_MAP_GET_BY_INDEX:
		ctx_type = AS_CDT_CTX_MAP | AS_CDT_CTX_INDEX;
		is_interval = false;
		break;
	case AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL:
		// Range selector (e.g. $.nums.[=10:30]) - same base dimension as the
		// singular LIST_GET_BY_VALUE case; the tail carries TWO adjacent
		// scalar args (lo, hi), not one - see parse_path_step_range_value.
		ctx_type = AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE;
		is_interval = true;
		break;
	case AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL:
		ctx_type = AS_CDT_CTX_MAP | AS_CDT_CTX_KEY;
		is_interval = true;
		break;
	case AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL:
		ctx_type = AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE;
		is_interval = true;
		break;
	default:
		// Includes AS_CDT_OP_SELECT - not plannable.
		return false;
	}

	uint64_t flags;

	if (! msgpack_get_uint64(mp, &flags)) {
		return false;
	}

	if ((flags & AS_CDT_OP_FLAG_INVERTED) != 0) {
		return false; // inverted selection is "not in" - not containment
	}

	pr->result_type = (result_type_t)(flags & AS_CDT_OP_FLAG_RESULT_MASK);

	if (pr->n_steps >= PATH_READ_MAX_STEPS) {
		return false;
	}

	return is_interval
			? parse_path_step_range_value(mp, &pr->steps[pr->n_steps++], ctx_type)
			: parse_path_step_value(mp, &pr->steps[pr->n_steps++], ctx_type);
}

// Parse a compiled EXP_CALL that is a recognized CDT path read on a bare
// bin: CDT family, no eval-token params, and one of:
// - flat get-op: [op, flags, value] (single-step path - AEL exists(),
//   scalar-at-path reads);
// - CONTEXT_EVAL: [0xFF, [type, val, ...], [inner]] where inner is either
//   a get-op (steps = pairs + synthesized op step) or SIZE (steps = pairs;
//   COUNT semantics - AEL count() folds the final selector into the ctx).
// Anything else is left unrecognized (valid == false) - the query still
// runs, the path read just can't drive an index.
static cdt_path_read
parse_cdt_path_read(const uint8_t* call_ptr, uint32_t call_ix)
{
	cdt_path_read pr = { 0 };
	const op_call* call = (const op_call*)call_ptr;

	if (call->system_type != EXP_CALL_CDT || call->eval_count != 0 ||
			call->n_vecs != 1) {
		return pr;
	}

	msgpack_in mp = { .buf = call->vecs[0].buf, .buf_sz = call->vecs[0].buf_sz };

	uint32_t ele_count;

	// 3 elements: [op_code, flags, value] (most get-ops). 4 elements:
	// [op_code, flags, lo, hi] (an _INTERVAL range-selector's tail carries
	// two adjacent bound args instead of one - see parse_get_op_tail).
	if (! msgpack_get_list_ele_count(&mp, &ele_count) ||
			(ele_count != 3 && ele_count != 4)) {
		return pr;
	}

	uint64_t op_code;

	if (! msgpack_get_uint64(&mp, &op_code)) {
		return pr;
	}

	if (op_code == AS_CDT_OP_CONTEXT_EVAL) {
		uint32_t n_pair_eles;

		if (! msgpack_get_list_ele_count(&mp, &n_pair_eles) || n_pair_eles == 0 ||
				(n_pair_eles & 1) != 0 || n_pair_eles / 2 > PATH_READ_MAX_STEPS) {
			return pr;
		}

		for (uint32_t i = 0; i < n_pair_eles / 2; i++) {
			if (! parse_path_step(&mp, &pr.steps[pr.n_steps])) {
				return pr;
			}

			pr.n_steps++;
		}

		uint32_t n_inner_eles;

		if (! msgpack_get_list_ele_count(&mp, &n_inner_eles)) {
			return pr;
		}

		if (n_inner_eles == 1) {
			uint64_t inner_op;

			if (! msgpack_get_uint64(&mp, &inner_op) ||
					inner_op != AS_CDT_OP_SIZE) {
				return pr;
			}

			pr.result_type = RESULT_TYPE_COUNT;
		}
		else if (n_inner_eles == 3 || n_inner_eles == 4) {
			uint64_t inner_op;

			if (! msgpack_get_uint64(&mp, &inner_op) ||
					! parse_get_op_tail(&mp, inner_op, &pr)) {
				return pr;
			}
		}
		else {
			return pr;
		}
	}
	else {
		// Flat [get_op, flags, value] - single-step path.
		if (! parse_get_op_tail(&mp, op_code, &pr)) {
			return pr;
		}
	}

	// Parse the final step's value as a scalar literal where possible -
	// containment forms use it as the index probe value.
	const path_step* last = &pr.steps[pr.n_steps - 1];
	msgpack_in lm = { .buf = last->val_ptr, .buf_sz = last->val_sz };

	if (last->is_range) {
		// Two adjacent scalars (lo, hi), not one msgpack value - decode
		// sequentially instead of via parse_probe_literal. Integer bounds
		// only for now (matches parsed_literal's bval/bval_high fields);
		// string/blob range bounds are left unparsed (last_value.parsed
		// stays false) and correctly fall back to PI.
		int64_t lo;
		int64_t hi;

		if (msgpack_get_int64(&lm, &lo) && msgpack_get_int64(&lm, &hi)) {
			pr.last_value.ktype = AS_PARTICLE_TYPE_INTEGER;
			pr.last_value.bval = lo;
			pr.last_value.bval_high = hi;
			pr.last_value.is_range = true;
			pr.last_value.parsed = true;
		}
	}
	else {
		parse_probe_literal(&lm, &pr.last_value);
	}

	if (pr.last_value.oversized) {
		return pr;
	}

	const uint8_t* p = call_ptr + op_table[EXP_CALL].size;
	uint32_t ix = call_ix + 1;
	const uint32_t call_end_ix = call->base.instr_end_ix;

	if (ix >= call_end_ix) {
		return pr;
	}

	const op_base_mem* ob = (const op_base_mem*)p;

	if (ob->code != EXP_BIN || ix + 1 != call_end_ix) {
		return pr;
	}

	pr.bin_op = (const op_var*)ob;
	pr.valid = true;

	return pr;
}

static bool
is_singular_nav_step(const path_step* step)
{
	if (step->is_range) {
		return false;
	}

	switch (step->ctx_type) {
	case AS_CDT_CTX_LIST | AS_CDT_CTX_INDEX:
	case AS_CDT_CTX_LIST | AS_CDT_CTX_RANK:
	case AS_CDT_CTX_MAP | AS_CDT_CTX_INDEX:
	case AS_CDT_CTX_MAP | AS_CDT_CTX_RANK:
	case AS_CDT_CTX_MAP | AS_CDT_CTX_KEY:
		return true;
	default:
		return false;
	}
}

static as_sindex_type
itype_from_selector(uint64_t ctx_type)
{
	switch (ctx_type) {
	case AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE:
		return AS_SINDEX_ITYPE_LIST;
	case AS_CDT_CTX_MAP | AS_CDT_CTX_KEY:
		return AS_SINDEX_ITYPE_MAPKEYS;
	case AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE:
		return AS_SINDEX_ITYPE_MAPVALUES;
	default:
		return AS_SINDEX_N_ITYPES;
	}
}

static bool
build_ctx_buf(const path_step* steps, uint32_t n_steps, uint8_t* buf,
		uint32_t* sz_r)
{
	if (n_steps == 0) {
		*sz_r = 0;
		return true;
	}

	uint32_t sz = 1; // fixarray header

	for (uint32_t i = 0; i < n_steps; i++) {
		if (steps[i].ctx_type > 0x7f) {
			return false; // fixint-encodable ctx types only
		}

		sz += 1 + steps[i].val_sz;
	}

	if (sz > AS_EXP_SINDEX_CTX_MAX) {
		return false;
	}

	uint8_t* p = buf;

	*p++ = (uint8_t)(0x90 | (n_steps * 2)); // fixarray - max 14 elements

	for (uint32_t i = 0; i < n_steps; i++) {
		*p++ = (uint8_t)steps[i].ctx_type;
		memcpy(p, steps[i].val_ptr, steps[i].val_sz);
		p += steps[i].val_sz;
	}

	*sz_r = sz;

	return true;
}

static walk_status
append_collection_candidate(const as_exp* exp, cf_vector* candidates,
		const op_var* bin_op, as_sindex_type itype, const parsed_literal* literal,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz)
{
	const exp_rt_bin_table* bt = (const exp_rt_bin_table*)exp->bin_table;
	const exp_bin_name_entry* bn = &bt->table[bin_op->idx];
	const char* bin_name = (const char*)(bt->base + bn->off);
	const uint32_t bin_name_sz = bn->sz;

	if (literal->is_range && literal->bval > literal->bval_high) {
		return WALK_FILTERED_OUT;
	}

	if (bin_name_sz >= AS_BIN_NAME_MAX_SZ) {
		cf_ticker_warning(AS_EXP, "query-plan: bin_name_sz exceeds max %u",
				AS_BIN_NAME_MAX_SZ);
		return WALK_PARAMETER_ERROR;
	}

	for (uint32_t i = 0; i < cf_vector_size(candidates); i++) {
		as_exp_sindex_candidate* c =
				(as_exp_sindex_candidate*)cf_vector_getp(candidates, i);

		if (c->itype == itype && c->ktype == literal->ktype &&
				c->bin_name_sz == bin_name_sz &&
				memcmp(c->bin_name, bin_name, bin_name_sz) == 0 &&
				c->bval_low == literal->bval &&
				c->bval_high ==
						(literal->is_range ? literal->bval_high : literal->bval) &&
				c->bound_val_sz == literal->bound_val_sz &&
				(literal->bound_val_sz == 0 ||
						memcmp(c->bound_val, literal->bound_val,
								literal->bound_val_sz) == 0) &&
				c->ctx_buf_sz == ctx_buf_sz &&
				(ctx_buf_sz == 0 || memcmp(c->ctx_buf, ctx_buf, ctx_buf_sz) == 0)) {
			return WALK_OK; // exact duplicate
		}
	}

	as_exp_sindex_candidate fresh = {
		.bin_name_sz = bin_name_sz,
		.ktype = literal->ktype,
		.itype = itype,
		.bval_low = literal->bval,
		.bval_high = literal->is_range ? literal->bval_high : literal->bval,
		.is_range = literal->is_range,
		.bound_val_sz = literal->bound_val_sz,
		.ctx_buf_sz = ctx_buf_sz,
	};

	memcpy(fresh.bin_name, bin_name, bin_name_sz);
	fresh.bin_name[bin_name_sz] = '\0';

	if (literal->bound_val_sz != 0) {
		cf_assert(literal->bound_val_sz <= AS_EXP_SINDEX_BOUND_VAL_MAX, AS_EXP,
				"query-plan: bound_val_sz %u > max %u", literal->bound_val_sz,
				AS_EXP_SINDEX_BOUND_VAL_MAX);
		memcpy(fresh.bound_val, literal->bound_val, literal->bound_val_sz);
	}

	if (ctx_buf_sz != 0) {
		memcpy(fresh.ctx_buf, ctx_buf, ctx_buf_sz);
	}

	cf_vector_append(candidates, &fresh);

	return WALK_OK;
}

static walk_status
append_containment_candidate(const as_exp* exp, cf_vector* candidates,
		const cdt_path_read* pr)
{
	const path_step* last = &pr->steps[pr->n_steps - 1];
	as_sindex_type itype = itype_from_selector(last->ctx_type);

	if (itype == AS_SINDEX_N_ITYPES || ! pr->last_value.parsed) {
		return WALK_OK;
	}

	for (uint32_t i = 0; i < pr->n_steps - 1; i++) {
		if (! is_singular_nav_step(&pr->steps[i])) {
			return WALK_OK;
		}
	}

	uint8_t ctx_buf[AS_EXP_SINDEX_CTX_MAX];
	uint32_t ctx_buf_sz;

	if (! build_ctx_buf(pr->steps, pr->n_steps - 1, ctx_buf, &ctx_buf_sz)) {
		return WALK_OK;
	}

	return append_collection_candidate(exp, candidates, pr->bin_op, itype,
			&pr->last_value, ctx_buf, ctx_buf_sz);
}

static walk_status
extract_in_list_candidate(const as_exp* exp, const uint8_t* node_ptr,
		uint32_t node_ix, cf_vector* candidates)
{
	const op_base_mem* node = (const op_base_mem*)node_ptr;
	const uint32_t node_end_ix = node->instr_end_ix;

	const uint8_t* val_ptr = node_ptr + op_table[EXP_IN_LIST].size;
	const uint32_t val_start_ix = node_ix + 1;

	if (val_start_ix >= node_end_ix) {
		return WALK_OK;
	}

	const uint32_t val_end_ix = ((const op_base_mem*)val_ptr)->instr_end_ix;

	parsed_literal literal =
			extract_literal_bound(val_ptr, val_start_ix, val_end_ix);

	if (! literal.parsed) {
		return WALK_OK;
	}

	const uint8_t* list_ptr = val_ptr;
	uint32_t list_ix = val_start_ix;

	skip_subtree(&list_ptr, &list_ix, val_end_ix);

	if (list_ix >= node_end_ix) {
		return WALK_OK;
	}

	const op_base_mem* list_ob = (const op_base_mem*)list_ptr;

	if (list_ob->code == EXP_BIN && list_ix + 1 == node_end_ix) {
		// Top-level list bin.
		return append_collection_candidate(exp, candidates,
				(const op_var*)list_ob, AS_SINDEX_ITYPE_LIST, &literal, NULL, 0);
	}

	if (list_ob->code == EXP_CALL && list_ob->instr_end_ix == node_end_ix) {
		// Nested list - "x" in $.attrs.specs. The call is a VALUE read of
		// the list at a singular path; that full path is the candidate ctx.
		cdt_path_read pr = parse_cdt_path_read(list_ptr, list_ix);

		if (! pr.valid || pr.result_type != RESULT_TYPE_VALUE) {
			return WALK_OK;
		}

		for (uint32_t i = 0; i < pr.n_steps; i++) {
			if (! is_singular_nav_step(&pr.steps[i])) {
				return WALK_OK;
			}
		}

		uint8_t ctx_buf[AS_EXP_SINDEX_CTX_MAX];
		uint32_t ctx_buf_sz;

		if (! build_ctx_buf(pr.steps, pr.n_steps, ctx_buf, &ctx_buf_sz)) {
			return WALK_OK;
		}

		return append_collection_candidate(exp, candidates, pr.bin_op,
				AS_SINDEX_ITYPE_LIST, &literal, ctx_buf, ctx_buf_sz);
	}

	return WALK_OK;
}

// A bare trilean CDT call at conjunct level - AEL exists(), e.g.
// $.tags.[="red"].exists() or $.attrs.specs.[="x"].exists() - is
// containment when the read's result type is EXISTS.
static walk_status
extract_exists_call_candidate(const as_exp* exp, const uint8_t* call_ptr,
		uint32_t call_ix, cf_vector* candidates)
{
	cdt_path_read pr = parse_cdt_path_read(call_ptr, call_ix);

	if (! pr.valid || pr.result_type != RESULT_TYPE_EXISTS) {
		return WALK_OK;
	}

	return append_containment_candidate(exp, candidates, &pr);
}

// True when [start_ix, end_ix) is exactly one EXP_VOP_VALUE_BOOL literal -
// e.g. the "true" in ".exists() == true" or "geoCompare(...) == true".
// extract_literal_bound never recognizes this shape (get_bval has no
// EXP_VOP_VALUE_BOOL case, by design - a trilean result is never itself an
// indexable value; see try_bool_literal's callers, which only use the bool
// to decide *which* already-valid-ktype candidate to build).
static bool
try_bool_literal(const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix, bool* value_r)
{
	if (lit_end_ix != lit_start_ix + 1) {
		return false;
	}

	const op_base_mem* ob = (const op_base_mem*)lit_ptr;

	if (ob->code != EXP_VOP_VALUE_BOOL) {
		return false;
	}

	*value_r = ((const op_value_bool*)lit_ptr)->value;

	return true;
}

// Determines which side of a comparison is a well-formed EXP_CALL subtree
// (the CDT path read) and which is the literal, flipping effective_cmp when
// the call is on the right so callers can always reason "call <cmp> lit".
static call_lit_operands
locate_call_and_literal_operands(const cmp_operand_spans* spans,
		exp_op_code cmp_code)
{
	call_lit_operands r = { 0 };

	const op_base_mem* left_ob = (const op_base_mem*)spans->left_ptr;
	const op_base_mem* right_ob = (const op_base_mem*)spans->right_ptr;

	uint32_t call_end_ix;

	if (left_ob->code == EXP_CALL) {
		r.call_ptr = spans->left_ptr;
		r.call_ix = spans->left_start_ix;
		call_end_ix = spans->left_end_ix;
		r.lit_ptr = spans->right_ptr;
		r.lit_start_ix = spans->right_start_ix;
		r.lit_end_ix = spans->cmp_end_ix;
		r.effective_cmp = cmp_code;
	}
	else if (right_ob->code == EXP_CALL) {
		r.call_ptr = spans->right_ptr;
		r.call_ix = spans->right_start_ix;
		call_end_ix = spans->cmp_end_ix;
		r.lit_ptr = spans->left_ptr;
		r.lit_start_ix = spans->left_start_ix;
		r.lit_end_ix = spans->left_end_ix;
		r.effective_cmp = reverse_cmp(cmp_code);
	}
	else {
		return r;
	}

	r.found = ((const op_base_mem*)r.call_ptr)->instr_end_ix == call_end_ix;

	return r;
}

// EXISTS semantics (AEL exists(), e.g. $.tags.[="red"].exists() == true): a
// containment candidate, same as the bare-conjunct form
// (extract_exists_call_candidate) - only "== true" (or, symmetrically, the
// literal on the left) implies containment; "== false" has no superset
// representation and stays residual-only.
static walk_status
extract_exists_cmp_candidate(const as_exp* exp, cf_vector* candidates,
		const cdt_path_read* pr, const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix, exp_op_code effective_cmp)
{
	bool bool_val;

	if (! try_bool_literal(lit_ptr, lit_start_ix, lit_end_ix, &bool_val)) {
		return WALK_OK;
	}

	if (effective_cmp != EXP_CMP_EQ || ! bool_val) {
		return WALK_OK; // "== false" has no superset form - residual only
	}

	return append_containment_candidate(exp, candidates, pr);
}

// COUNT semantics (AEL count(), e.g. $.tags.[="red"].count() > 0): a
// containment candidate when the comparison implies "count >= 1" - the index
// yields a superset and the filter expression re-checks exact counts per
// record. Negations / exact zero / upper bounds stay residual-only.
static walk_status
extract_count_cmp_candidate(const as_exp* exp, cf_vector* candidates,
		const cdt_path_read* pr, const parsed_literal* cmp_lit,
		exp_op_code effective_cmp)
{
	if (cmp_lit->ktype != AS_PARTICLE_TYPE_INTEGER) {
		return WALK_OK;
	}

	const int64_t n = cmp_lit->bval;
	bool implies_containment;

	switch (effective_cmp) {
	case EXP_CMP_GT:
		implies_containment = n >= 0;
		break;
	case EXP_CMP_GE:
	case EXP_CMP_EQ:
		implies_containment = n >= 1;
		break;
	default:
		implies_containment = false;
		break;
	}

	if (! implies_containment) {
		return WALK_OK;
	}

	return append_containment_candidate(exp, candidates, pr);
}

// VALUE semantics (scalar at a singular nested path, e.g.
// $.address.city == "SF"): a DEFAULT-itype candidate on the ctx sindex for
// the full path; scalar interval merge rules apply.
static walk_status
extract_value_cmp_candidate(const as_exp* exp, cf_vector* candidates,
		const cdt_path_read* pr, const parsed_literal* cmp_lit,
		exp_op_code effective_cmp)
{
	// Family B - every step must land on a single location for the read to
	// have scalar semantics.
	for (uint32_t i = 0; i < pr->n_steps; i++) {
		if (! is_singular_nav_step(&pr->steps[i])) {
			return WALK_OK;
		}
	}

	uint8_t ctx_buf[AS_EXP_SINDEX_CTX_MAX];
	uint32_t ctx_buf_sz;

	if (! build_ctx_buf(pr->steps, pr->n_steps, ctx_buf, &ctx_buf_sz)) {
		return WALK_OK;
	}

	const exp_rt_bin_table* bt = (const exp_rt_bin_table*)exp->bin_table;
	const exp_bin_name_entry* bn = &bt->table[pr->bin_op->idx];

	return merge_candidate(candidates, (const char*)(bt->base + bn->off), bn->sz,
			AS_SINDEX_ITYPE_DEFAULT, ctx_buf, ctx_buf_sz, effective_cmp, cmp_lit);
}

// A comparison whose bin side is a CDT path read - dispatches on the read's
// result kind (EXISTS / COUNT / VALUE) to the matching handler above.
static walk_status
extract_call_cmp_candidate(const as_exp* exp, const cmp_operand_spans* spans,
		exp_op_code cmp_code, cf_vector* candidates,
		const resolved_bin_literal* sides)
{
	call_lit_operands ops = locate_call_and_literal_operands(spans, cmp_code);

	if (! ops.found) {
		return WALK_OK;
	}

	// resolve_bin_and_literal already parsed this exact call span while
	// deciding it wasn't a bare-bin/is_exp match - reuse that instead of
	// walking the same call subtree again.
	cdt_path_read pr = sides->cdt_probe_done &&
					sides->cdt_probe_ptr == ops.call_ptr &&
					sides->cdt_probe_ix == ops.call_ix
			? sides->cdt_probe_result
			: parse_cdt_path_read(ops.call_ptr, ops.call_ix);

	if (! pr.valid) {
		return WALK_OK;
	}

	if (pr.result_type == RESULT_TYPE_EXISTS) {
		return extract_exists_cmp_candidate(exp, candidates, &pr, ops.lit_ptr,
				ops.lit_start_ix, ops.lit_end_ix, ops.effective_cmp);
	}

	parsed_literal cmp_lit =
			extract_literal_bound(ops.lit_ptr, ops.lit_start_ix, ops.lit_end_ix);

	if (! cmp_lit.parsed) {
		return WALK_OK;
	}

	if (pr.result_type == RESULT_TYPE_COUNT) {
		return extract_count_cmp_candidate(exp, candidates, &pr, &cmp_lit,
				ops.effective_cmp);
	}

	if (pr.result_type == RESULT_TYPE_VALUE) {
		return extract_value_cmp_candidate(exp, candidates, &pr, &cmp_lit,
				ops.effective_cmp);
	}

	return WALK_OK;
}

static walk_status
extract_candidate(const as_exp* exp, const uint8_t* cmp_ptr,
		uint32_t cmp_start_ix, cf_vector* candidates)
{
	cmp_operand_spans spans;

	cmp_locate_operands(cmp_ptr, cmp_start_ix, &spans);

	resolved_bin_literal sides =
			resolve_bin_and_literal(&spans, ((op_base_mem*)cmp_ptr)->code);

	if (! sides.found) {
		exp_op_code cmp_code = ((op_base_mem*)cmp_ptr)->code;
		const op_base_mem* left_ob = (const op_base_mem*)spans.left_ptr;
		const op_base_mem* right_ob = (const op_base_mem*)spans.right_ptr;

		if (left_ob->code == EXP_CMP_GEO || right_ob->code == EXP_CMP_GEO) {
			return extract_geo_eq_candidate(exp, &spans, cmp_code, candidates);
		}

		return extract_call_cmp_candidate(exp, &spans, cmp_code, candidates,
				&sides);
	}

	parsed_literal literal = extract_literal_bound(sides.lit_ptr,
			sides.lit_start_ix, sides.lit_end_ix);

	// An oversized literal is unindexable, not invalid, on every comparison
	// shape - PI-fallback (WALK_OK, no candidate appended for this term)
	// rather than erroring the whole query (warning already logged inside
	// extract_literal_bound at the point the size was determined).
	if (! literal.parsed) {
		return WALK_OK;
	}

	if (sides.is_exp) {
		return append_exp_candidate(exp, candidates, sides.exp_ptr,
				sides.exp_start_ix, sides.exp_end_ix, sides.effective_cmp,
				&literal);
	}

	const exp_rt_bin_table* bt = (const exp_rt_bin_table*)exp->bin_table;
	const exp_bin_name_entry* bn = &bt->table[sides.bin_op->idx];

	return merge_candidate(candidates, (const char*)(bt->base + bn->off), bn->sz,
			AS_SINDEX_ITYPE_DEFAULT, NULL, 0, sides.effective_cmp, &literal);
}

static bool
resolve_geo_literal(const uint8_t* lit_ptr, uint32_t lit_start_ix,
		uint32_t lit_end_ix, parsed_literal* out)
{
	if (lit_end_ix != lit_start_ix + 1) {
		return false; // must be exactly one op - the GEO literal itself
	}

	const op_base_mem* ob = (const op_base_mem*)lit_ptr;

	if (ob->code != EXP_VOP_VALUE_GEO) {
		return false;
	}

	const exp_op_value_geo* geo = (const exp_op_value_geo*)ob;

	msgpack_in mp = { .buf = geo->contents, .buf_sz = geo->content_sz };
	uint32_t sz;
	const uint8_t* buf = msgpack_get_bin(&mp, &sz);

	if (buf == NULL || sz == 0 || buf[0] != AS_BYTES_GEOJSON) {
		return false;
	}

	buf++;
	sz--;

	if (sz == 0) {
		return false;
	}

	if (sz >= AS_EXP_SINDEX_GEO_BOUND_VAL_MAX) {
		cf_ticker_warning(AS_EXP,
				"query-plan: geo literal size exceeds sindex bound max %u - "
				"term not indexed",
				AS_EXP_SINDEX_GEO_BOUND_VAL_MAX);
		return true;
	}

	out->ktype = AS_PARTICLE_TYPE_GEOJSON;
	out->bound_val = buf;
	out->bound_val_sz = sz;
	out->parsed = true;

	return true;
}

static walk_status
append_geo_candidate(cf_vector* candidates, const char* bin_name,
		uint32_t bin_name_sz, const parsed_literal* literal,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz)
{
	if (bin_name_sz >= AS_BIN_NAME_MAX_SZ) {
		cf_ticker_warning(AS_EXP, "query-plan: bin_name_sz exceeds max %u",
				AS_BIN_NAME_MAX_SZ);
		return WALK_PARAMETER_ERROR;
	}

	as_exp_sindex_candidate fresh = {
		.bin_name_sz = bin_name_sz,
		.ktype = AS_PARTICLE_TYPE_GEOJSON,
		.itype = AS_SINDEX_ITYPE_DEFAULT,
		.bound_val_sz = literal->bound_val_sz,
		.ctx_buf_sz = ctx_buf_sz,
	};

	memcpy(fresh.bin_name, bin_name, bin_name_sz);
	fresh.bin_name[bin_name_sz] = '\0';

	// Always borrowed - GEO bound bytes are never copied into bound_val,
	// unlike STRING/BLOB candidates (resolve_geo_literal guarantees
	// bound_val_sz != 0 whenever literal->parsed is true).
	fresh.geo_bound_val = literal->bound_val;

	if (ctx_buf_sz != 0) {
		memcpy(fresh.ctx_buf, ctx_buf, ctx_buf_sz);
	}

	cf_vector_append(candidates, &fresh);

	return WALK_OK;
}

static walk_status
finish_geo_candidate(const as_exp* exp, const uint8_t* lit_ptr,
		uint32_t lit_start_ix, uint32_t lit_end_ix, uint32_t bin_idx,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz, cf_vector* candidates)
{
	parsed_literal literal = { 0 };

	if (! resolve_geo_literal(lit_ptr, lit_start_ix, lit_end_ix, &literal)) {
		return WALK_OK;
	}

	// resolve_geo_literal leaves bound_val/bound_val_sz/ktype unset only when
	// oversized (a warning was already logged inside resolve_geo_literal) on
	// this path - without this check an oversized literal would append a
	// bounds-less, zero-length geo candidate instead of simply not indexing
	// the term.
	if (! literal.parsed) {
		return WALK_OK;
	}

	const exp_rt_bin_table* bt = (const exp_rt_bin_table*)exp->bin_table;
	const exp_bin_name_entry* bn = &bt->table[bin_idx];

	return append_geo_candidate(candidates, (const char*)(bt->base + bn->off),
			bn->sz, &literal, ctx_buf, ctx_buf_sz);
}

// geoCompare is symmetric (both operands GEOJSON-typed), so no reverse_cmp
// bookkeeping is needed here unlike extract_candidate. Only bare-bin and
// ctx-scalar receiver shapes are handled - AEL selectors take literal
// scalar probes, not sub-expressions, so collection-containment geo has no
// syntax to express and is unreachable here; other shapes fall to WALK_OK.
static walk_status
extract_geo_candidate(const as_exp* exp, const uint8_t* cmp_ptr,
		uint32_t cmp_start_ix, cf_vector* candidates)
{
	cmp_operand_spans spans;

	cmp_locate_operands(cmp_ptr, cmp_start_ix, &spans);

	const uint8_t* lp = spans.left_ptr;
	uint32_t lix = spans.left_start_ix;
	op_var* bin_op = NULL;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;
	bool found = false;

	if (consume_bin_ref_chain(&lp, &lix, spans.left_end_ix, &bin_op)) {
		lit_ptr = spans.right_ptr;
		lit_start_ix = spans.right_start_ix;
		lit_end_ix = spans.cmp_end_ix;
		found = true;
	}
	else {
		const uint8_t* rp = spans.right_ptr;
		uint32_t rix = spans.right_start_ix;

		if (consume_bin_ref_chain(&rp, &rix, spans.cmp_end_ix, &bin_op)) {
			lit_ptr = spans.left_ptr;
			lit_start_ix = spans.left_start_ix;
			lit_end_ix = spans.left_end_ix;
			found = true;
		}
	}

	if (! found) {
		return extract_geo_ctx_candidate(exp, &spans, candidates);
	}

	return finish_geo_candidate(exp, lit_ptr, lit_start_ix, lit_end_ix,
			bin_op->idx, NULL, 0, candidates);
}

// $.venue.location shape: neither operand is a bare bin, so one of them must
// be a CDT path read (mirrors extract_call_cmp_candidate's left_ob/right_ob
// dispatch) navigating to a scalar GEO value - every step singular, ctx_buf
// built from ALL steps (there is no trailing containment selector here,
// unlike append_containment_candidate's n_steps-1 - this is a plain read).
static walk_status
extract_geo_ctx_candidate(const as_exp* exp, const cmp_operand_spans* spans,
		cf_vector* candidates)
{
	const op_base_mem* left_ob = (const op_base_mem*)spans->left_ptr;
	const op_base_mem* right_ob = (const op_base_mem*)spans->right_ptr;

	const uint8_t* call_ptr;
	uint32_t call_ix;
	uint32_t call_end_ix;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;

	if (left_ob->code == EXP_CALL) {
		call_ptr = spans->left_ptr;
		call_ix = spans->left_start_ix;
		call_end_ix = spans->left_end_ix;
		lit_ptr = spans->right_ptr;
		lit_start_ix = spans->right_start_ix;
		lit_end_ix = spans->cmp_end_ix;
	}
	else if (right_ob->code == EXP_CALL) {
		call_ptr = spans->right_ptr;
		call_ix = spans->right_start_ix;
		call_end_ix = spans->cmp_end_ix;
		lit_ptr = spans->left_ptr;
		lit_start_ix = spans->left_start_ix;
		lit_end_ix = spans->left_end_ix;
	}
	else {
		return WALK_OK;
	}

	if (((const op_base_mem*)call_ptr)->instr_end_ix != call_end_ix) {
		return WALK_OK;
	}

	cdt_path_read pr = parse_cdt_path_read(call_ptr, call_ix);

	if (! pr.valid || pr.result_type != RESULT_TYPE_VALUE) {
		return WALK_OK;
	}

	for (uint32_t i = 0; i < pr.n_steps; i++) {
		if (! is_singular_nav_step(&pr.steps[i])) {
			return WALK_OK;
		}
	}

	uint8_t ctx_buf[AS_EXP_SINDEX_CTX_MAX];
	uint32_t ctx_buf_sz;

	if (! build_ctx_buf(pr.steps, pr.n_steps, ctx_buf, &ctx_buf_sz)) {
		return WALK_OK;
	}

	return finish_geo_candidate(exp, lit_ptr, lit_start_ix, lit_end_ix,
			pr.bin_op->idx, ctx_buf, ctx_buf_sz, candidates);
}

// geoCompare(a, b) == true reaches here via extract_candidate's fallback
// since a bool literal never parses through resolve_bin_and_literal, it
// unwraps the bool and hands off to extract_geo_candidate. Only "== true"
// implies the comparison - "== false" stays residual-only.
static walk_status
extract_geo_eq_candidate(const as_exp* exp, const cmp_operand_spans* spans,
		exp_op_code cmp_code, cf_vector* candidates)
{
	const op_base_mem* left_ob = (const op_base_mem*)spans->left_ptr;
	const op_base_mem* right_ob = (const op_base_mem*)spans->right_ptr;

	const uint8_t* geo_ptr;
	uint32_t geo_ix;
	uint32_t geo_end_ix;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;

	if (left_ob->code == EXP_CMP_GEO) {
		geo_ptr = spans->left_ptr;
		geo_ix = spans->left_start_ix;
		geo_end_ix = spans->left_end_ix;
		lit_ptr = spans->right_ptr;
		lit_start_ix = spans->right_start_ix;
		lit_end_ix = spans->cmp_end_ix;
	}
	else if (right_ob->code == EXP_CMP_GEO) {
		geo_ptr = spans->right_ptr;
		geo_ix = spans->right_start_ix;
		geo_end_ix = spans->cmp_end_ix;
		lit_ptr = spans->left_ptr;
		lit_start_ix = spans->left_start_ix;
		lit_end_ix = spans->left_end_ix;
	}
	else {
		return WALK_OK;
	}

	if (((const op_base_mem*)geo_ptr)->instr_end_ix != geo_end_ix) {
		return WALK_OK;
	}

	bool bool_val;

	if (! try_bool_literal(lit_ptr, lit_start_ix, lit_end_ix, &bool_val)) {
		return WALK_OK;
	}

	if (cmp_code != EXP_CMP_EQ || ! bool_val) {
		return WALK_OK;
	}

	return extract_geo_candidate(exp, geo_ptr, geo_ix, candidates);
}

static walk_status
walk_subtree(const as_exp* exp, const uint8_t** instr_ptr, uint32_t* op_ix,
		uint32_t parent_end_ix, cf_vector* candidates)
{
	if (*op_ix >= parent_end_ix) {
		return WALK_OK;
	}

	op_base_mem* node = (op_base_mem*)*instr_ptr;
	const uint32_t node_end_ix = node->instr_end_ix;
	walk_status st = WALK_OK;

	switch (node->code) {
	case EXP_AND:
		*instr_ptr += op_table[EXP_AND].size;
		(*op_ix)++;

		while (*op_ix < node_end_ix) {
			st = walk_subtree(exp, instr_ptr, op_ix, node_end_ix, candidates);

			if (st == WALK_PI_FALLBACK) {
				// This one AND-conjunct can't be turned into an index
				// candidate (it's rooted in OR/NOT/EXCLUSIVE). That's fine:
				// we just skip it and keep collecting candidates from the
				// other conjuncts, because the full filter always gets
				// re-checked per-record after the index scan anyway - so
				// missing this conjunct's index just means a slightly
				// bigger scan, not a wrong result.
				uint32_t conjunct_end_ix =
						((const op_base_mem*)*instr_ptr)->instr_end_ix;

				skip_subtree(instr_ptr, op_ix, conjunct_end_ix);
				continue;
			}

			if (st != WALK_OK) {
				return st;
			}
		}

		return WALK_OK;
	case EXP_CMP_EQ:
	case EXP_CMP_GT:
	case EXP_CMP_GE:
	case EXP_CMP_LT:
	case EXP_CMP_LE:
		st = extract_candidate(exp, *instr_ptr, *op_ix, candidates);

		if (st != WALK_OK) {
			return st;
		}

		break;
	case EXP_CMP_GEO:
		st = extract_geo_candidate(exp, *instr_ptr, *op_ix, candidates);

		if (st != WALK_OK) {
			return st;
		}

		break;
	case EXP_IN_LIST:
		st = extract_in_list_candidate(exp, *instr_ptr, *op_ix, candidates);

		if (st != WALK_OK) {
			return st;
		}

		break;
	case EXP_CALL:
		// Every call goes here. Only EXISTS-typed CDT calls (AEL exists())
		// become candidates in extract_exists_call_candidate, count()/value
		// comparisons go through extract_call_cmp_candidate instead.
		st = extract_exists_call_candidate(exp, *instr_ptr, *op_ix, candidates);

		if (st != WALK_OK) {
			return st;
		}

		break;
	case EXP_OR:
	case EXP_NOT:
	case EXP_EXCLUSIVE:
		cf_debug(AS_EXP,
				"query-plan: op=%s (opcode=%u) - PI fallback, candidates cleared",
				op_table[node->code].name, node->code);
		return WALK_PI_FALLBACK;
	default:
		break;
	}

	skip_subtree(instr_ptr, op_ix, node_end_ix);
	return WALK_OK;
}

static bool
is_hash_type(as_particle_type ktype)
{
	// GEOJSON is deliberately excluded here even though geo candidates use
	// the same hash-shaped bound_val/bound_val_sz representation: geo
	// literals are built exclusively through append_geo_candidate /
	// extract_geo_candidate, a dedicated path that never calls
	// cmp_to_interval, merge_candidate, or append_exp_candidate - the only
	// callers of this function. Do not add GEOJSON here without first
	// auditing those call sites, since they assume "hash type" means
	// "opaque bytes with no interval semantics" for STRING/BLOB, which
	// hasn't been verified for GEOJSON's compiled-cellid/region form.
	return ktype == AS_PARTICLE_TYPE_STRING || ktype == AS_PARTICLE_TYPE_BLOB;
}

static void
skip_subtree(const uint8_t** instr_ptr, uint32_t* op_ix, uint32_t instr_end_ix)
{
	while (*op_ix < instr_end_ix) {
		op_base_mem* ob = (op_base_mem*)*instr_ptr;

		*instr_ptr += op_table[ob->code].size;
		(*op_ix)++;
	}
}

//==========================================================
// Public API.
//

as_exp_sindex_extract_result
as_exp_get_sindex_candidates(const as_exp* exp, cf_vector* candidates)
{
	if (exp == NULL) {
		return AS_EXP_SINDEX_EXTRACT_PI;
	}

	const op_base_mem* root = (const op_base_mem*)exp->mem;
	const uint32_t root_end_ix = root->instr_end_ix;

	const uint8_t* instr_ptr = exp->mem;
	uint32_t op_ix = 0;

	walk_status status =
			walk_subtree(exp, &instr_ptr, &op_ix, root_end_ix, candidates);

	as_exp_sindex_extract_result result;

	switch (status) {
	case WALK_OK:
		result = cf_vector_size(candidates) == 0
				? AS_EXP_SINDEX_EXTRACT_PI
				: AS_EXP_SINDEX_EXTRACT_CANDIDATES;
		break;
	case WALK_PI_FALLBACK:
		result = AS_EXP_SINDEX_EXTRACT_PI;
		break;
	case WALK_FILTERED_OUT:
		result = AS_EXP_SINDEX_EXTRACT_FILTERED_OUT;
		break;
	case WALK_PARAMETER_ERROR:
		result = AS_EXP_SINDEX_EXTRACT_ERROR;
		break;
	default:
		cf_crash(AS_EXP, "unexpected walk status %d", status);
	}

	return result;
}
