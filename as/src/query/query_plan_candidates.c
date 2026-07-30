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
#include "vector.h"

#include "base/datamodel.h"
#include "exp/exp.h"
#include "exp/exp_rt.h"
#include "exp/exp_wire.h"
#include "sindex/sindex_manager.h"

//==========================================================
// Typedefs & constants.
//

// Short aliases for the op-node layouts, matching exp.c's local convention.
typedef struct exp_op_base_mem_s op_base_mem;
typedef struct exp_op_var_s op_var;
typedef struct exp_op_value_blob_s op_value_blob;
typedef struct exp_op_value_int_s op_value_int;
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

typedef struct parsed_literal_s {
	as_particle_type ktype;
	int64_t bval;
	const uint8_t* bound_val;
	uint32_t bound_val_sz;
	bool parsed;
	bool oversized;
} parsed_literal;

typedef struct resolved_bin_literal_s {
	op_var* bin_op;
	const uint8_t* lit_ptr;
	uint32_t lit_start_ix;
	uint32_t lit_end_ix;
	exp_op_code effective_cmp;
	bool found;
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
		uint32_t bin_name_sz, as_sindex_type itype, exp_op_code cmp_code,
		const parsed_literal* literal);
static bool intersect_into(as_exp_sindex_candidate* c, int64_t lo, int64_t hi);
static as_exp_sindex_candidate* find_candidate(cf_vector* candidates,
		const char* bin_name, uint32_t bin_name_sz, as_particle_type ktype,
		as_sindex_type itype);
static bool cmp_to_interval(as_particle_type ktype, exp_op_code cmp_code,
		int64_t bval, int64_t* lo_r, int64_t* hi_r);
static exp_op_code reverse_cmp(exp_op_code code);
static bool is_hash_type(as_particle_type ktype);
static void skip_subtree(const uint8_t** instr_ptr, uint32_t* op_ix,
		uint32_t instr_end_ix);

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
		uint32_t bin_name_sz, as_particle_type ktype, as_sindex_type itype)
{
	for (uint32_t i = 0; i < cf_vector_size(candidates); i++) {
		as_exp_sindex_candidate* c =
				(as_exp_sindex_candidate*)cf_vector_getp(candidates, i);

		if (c->ktype == ktype && c->itype == itype &&
				c->bin_name_sz == bin_name_sz &&
				memcmp(c->bin_name, bin_name, bin_name_sz) == 0) {
			return c;
		}
	}

	return NULL;
}

static walk_status
merge_candidate(cf_vector* candidates, const char* bin_name,
		uint32_t bin_name_sz, as_sindex_type itype, exp_op_code cmp_code,
		const parsed_literal* literal)
{
	const as_particle_type ktype = literal->ktype;
	const uint8_t* bound_val = literal->bound_val;
	const uint32_t bound_val_sz = literal->bound_val_sz;

	if (bound_val_sz != 0) {
		if (! is_hash_type(ktype) || bound_val == NULL ||
				bound_val_sz > AS_EXP_SINDEX_BOUND_VAL_MAX) {
			return WALK_PARAMETER_ERROR;
		}
	}

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

	as_exp_sindex_candidate* existing =
			find_candidate(candidates, bin_name, bin_name_sz, ktype, itype);

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
		cf_ticker_warning(AS_EXP, "query-plan: bin_name_sz %u >= max %u",
				bin_name_sz, AS_BIN_NAME_MAX_SZ);
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
	};

	memcpy(fresh.bin_name, bin_name, bin_name_sz);
	fresh.bin_name[bin_name_sz] = '\0';

	if (literal->bound_val_sz != 0) {
		memcpy(fresh.bound_val, literal->bound_val, literal->bound_val_sz);
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
	}

	return sides;
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

		if (blob_lit->value_sz > AS_EXP_SINDEX_BOUND_VAL_MAX) {
			literal.oversized = true;
			return literal;
		}

		literal.bound_val = blob_lit->value;
		literal.bound_val_sz = blob_lit->value_sz;
	}

	literal.parsed = true;

	return literal;
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
		return WALK_OK;
	}

	parsed_literal literal = extract_literal_bound(sides.lit_ptr,
			sides.lit_start_ix, sides.lit_end_ix);

	if (literal.oversized) {
		cf_ticker_warning(AS_EXP,
				"query-plan: literal size exceeds sindex bound max %u",
				AS_EXP_SINDEX_BOUND_VAL_MAX);
		return WALK_PARAMETER_ERROR;
	}

	if (! literal.parsed) {
		return WALK_OK;
	}

	const exp_rt_bin_table* bt = (const exp_rt_bin_table*)exp->bin_table;
	const exp_bin_name_entry* bn = &bt->table[sides.bin_op->idx];

	return merge_candidate(candidates, (const char*)(bt->base + bn->off),
			bn->sz, AS_SINDEX_ITYPE_DEFAULT, sides.effective_cmp, &literal);
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
