/*
 * exp.c
 *
 * Copyright (C) 2020-2026 Aerospike, Inc.
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

#include "exp/exp.h"

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <regex.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_hash_math.h"

#include "bits.h"
#include "cf_defer.h"
#include "cf_str.h"
#include "dynbuf.h"
#include "enhanced_alloc.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/masking.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"
#include "base/thr_info.h"
#include "exp/ael_codegen.h"
#include "exp/ael_parse.h"
#include "exp/ael_string.h"
#include "exp/ast.h"
#include "exp/exp_ael.h"
#include "exp/exp_rt.h"
#include "exp/exp_wire.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"

// #include "warnings.h"

//==========================================================
// Typedefs & constants.
//

// EXP_MAX_DEPTH (nesting cap for all traversals) is in exp/ast.h, shared with
// ael_codegen.c and the AEL build passes.

// parse_op_call dispatches on the high sentinels before it knows the family, so
// a family op grown into their range would decode as one.
#define EXP_OP_BELOW_SENTINELS(_end)                                           \
	COMPILER_ASSERT((uint32_t)(_end) < (uint32_t)AS_CDT_OP_SIZE)

EXP_OP_BELOW_SENTINELS(AS_BITS_MODIFY_OP_END);
EXP_OP_BELOW_SENTINELS(AS_BITS_READ_OP_END);
EXP_OP_BELOW_SENTINELS(AS_HLL_MODIFY_OP_END);
EXP_OP_BELOW_SENTINELS(AS_HLL_READ_OP_END);
EXP_OP_BELOW_SENTINELS(AS_STRING_READ_OP_END);
EXP_OP_BELOW_SENTINELS(AS_STRING_MODIFY_OP_END);
EXP_OP_BELOW_SENTINELS(AS_CDT_OP_END);

//==========================================================
// Type aliases.
//

typedef struct exp_op_base_mem_s op_base_mem;
typedef struct exp_op_cmp_regex_s op_cmp_regex;
typedef struct exp_op_meta_digest_modulo_s op_meta_digest_modulo;
typedef struct exp_op_cond_s op_cond;
typedef struct exp_op_var_s op_var;
typedef struct exp_op_let_s op_let;
typedef struct exp_op_vec_s op_vec;
typedef struct exp_op_call_s op_call;
typedef struct exp_op_value_geo_s op_value_geo;
typedef struct exp_op_value_bool_s op_value_bool;
typedef struct exp_op_value_blob_s op_value_blob;
typedef struct exp_op_value_int_s op_value_int;
typedef struct exp_op_value_float_s op_value_float;
typedef struct exp_runtime_s runtime;

typedef struct exp_var_entry_s var_entry;
typedef struct exp_var_scope_s var_scope;
typedef struct exp_build_bin_table_s build_bin_table;

static const char* exp_rtype_str[] = {
	[EXP_RTYPE_NIL] = "nil",
	[EXP_RTYPE_TRILEAN] = "bool",
	[EXP_RTYPE_INT] = "int",
	[EXP_RTYPE_STR] = "str",
	[EXP_RTYPE_LIST] = "list",
	[EXP_RTYPE_MAP] = "map",
	[EXP_RTYPE_BLOB] = "blob",
	[EXP_RTYPE_FLOAT] = "float",
	[EXP_RTYPE_GEOJSON] = "geojson",
	[EXP_RTYPE_HLL] = "hll",
	[EXP_RTYPE_RESULT_REMOVE] = "result_remove",
};

ARRAY_ASSERT(exp_rtype_str, EXP_RTYPE_END);

const char*
exp_rtype_to_str(exp_rtype type)
{
	return (uint32_t)type >= EXP_RTYPE_END
			? "invalid"
			: exp_rtype_str[type]; // (uint32_t) cast because enum can be signed
}

static const exp_op_code exp_rtype_to_op_code[] = {
	[EXP_RTYPE_NIL] = EXP_VOP_VALUE_NIL,
	[EXP_RTYPE_TRILEAN] = EXP_VOP_VALUE_TRILEAN,
	[EXP_RTYPE_INT] = EXP_VOP_VALUE_INT,
	[EXP_RTYPE_STR] = EXP_VOP_VALUE_STR,
	[EXP_RTYPE_LIST] = EXP_VOP_VALUE_LIST,
	[EXP_RTYPE_MAP] = EXP_VOP_VALUE_MAP,
	[EXP_RTYPE_BLOB] = EXP_VOP_VALUE_BLOB,
	[EXP_RTYPE_FLOAT] = EXP_VOP_VALUE_FLOAT,
	[EXP_RTYPE_GEOJSON] = EXP_VOP_VALUE_GEO,
	[EXP_RTYPE_HLL] = EXP_VOP_VALUE_HLL,
	[EXP_RTYPE_RESULT_REMOVE] = EXP_RESULT_REMOVE,
};

ARRAY_ASSERT(exp_rtype_to_op_code, EXP_RTYPE_END);

typedef struct build_counts_s {
	uint32_t total_sz;
	uint32_t cleanup_count;
	uint32_t literal_cleanup;
	// Gate, not a reservation - a marker-bearing literal that is already
	// canonical retains nothing, so it must not be counted above.
	bool has_marker_literal;
	uint32_t counter;
	const uint8_t* end;
	build_bin_table bin_table;
} build_counts;

typedef enum {
	CDT_LITERAL_OK,
	CDT_LITERAL_NEEDS_REWRITE,
	CDT_LITERAL_INVALID
} cdt_literal_status;

// Wire (msgpack) build-pass context -- the op-table build_cb argument.
// Forward-declared in exp.h for the exp_op_table_build_cb signature; the tag is
// completed here since it is used only by this file's build pass.
struct exp_build_args_s {
	as_exp* exp;
	uint8_t* mem;
	msgpack_in mp;

	const exp_op_table_entry* entry;
	uint32_t ele_count;
	uint32_t instr_ix;

	uint32_t var_idx;
	uint32_t max_var_idx;
	var_scope* current;

	// Live ancestor chain for the build-failure path/depth trace. Maintained
	// by build_next(); leave NULL when driving build_cbs directly.
	struct build_frame_s* current_frame;

	uint32_t depth; // build_next recursion depth, capped at EXP_MAX_DEPTH

	// Count of CDT literals the sizing pass found out of canonical form -- the
	// build pass consumes one per literal it retains, and releases one per
	// call parameter it hands to the op instead.
	uint32_t literal_cleanup;
	bool has_marker_literal;

	build_bin_table bin_table;
	cf_vector* bins_info_r; // only valid for secondary index expressions
};

// clang-format off
#define OP_TABLE_ENTRY(_code, _name, _size_name, _build_name, _eval_name, _display_name, _static_param_count, _eval_param_count, _r_type) \
		[_code].code = _code, \
		[_code].name = _name, \
		[_code].size = (uint32_t)sizeof(_size_name), \
		[_code].build_cb = _build_name, \
		[_code].eval_cb = _eval_name, \
		[_code].display_cb = _display_name, \
		[_code].static_param_count = _static_param_count, \
		[_code].eval_param_count = _eval_param_count, \
		[_code].r_type = _r_type,
// clang-format on

typedef struct exp_build_args_s build_args;
typedef exp_op_table_entry op_table_entry;

// A build-time ancestor frame. build_next pushes one for the op it's about to
// build and pops it on return, so at any failure the live chain
// deepest -> root IS the path to the fault.
typedef struct build_frame_s {
	struct build_frame_s* parent;
	exp_op_code op_code;
} build_frame;

// Thread-local build-failure accumulator. Reset at the top of build_internal
// and filled first-set-wins as the recursion unwinds, so the deepest failing
// op is the one kept. Raw codes and borrowed pointers only - rendering (op
// name, path, snippet) happens at as_exp_take_build_error(), strictly on the
// error path.
typedef struct exp_build_err_s {
	bool set;
	uint32_t offset;
	bool has_op;
	exp_op_code op_code;
	// Depth + ancestor chain (root -> fault), captured by walking the
	// live build_frame chain at record time.
	uint16_t depth;
	bool path_truncated;
	uint16_t n_frames;
	exp_op_code frames[AS_EXP_TRACE_MAX_FRAMES];
	// The msgpack payload (wire build) or the AEL source text (AEL build),
	// retained so the snippet can be rendered from the failing element at
	// as_exp_take_build_error() time. snippet_offset is the start of the
	// failing element (the whole op list), which differs from 'offset' (the
	// byte_offset, which points past the op header).
	const uint8_t* payload;
	uint32_t payload_sz;
	uint32_t snippet_offset;
	// AEL build failure: lang == AS_EXP_TRACE_LANG_AEL (0 for a wire build).
	// 'offset'/'span' then index the AEL source text ('payload'), never the
	// msgpack payload, and 'msg' carries the compile diagnostic verbatim.
	uint8_t lang;
	bool has_pos;
	uint32_t span;
	bool has_msg;
	char msg[AS_EXP_BUILD_ERROR_MSG_MAX];
} exp_build_err;

static __thread exp_build_err g_exp_build_err;

// Walk the live ancestor chain (deepest -> root) into the accumulator's frames
// (stored root -> fault). depth is the true nesting depth even when the chain
// is deeper than AS_EXP_TRACE_MAX_FRAMES; in that case the outermost frames
// plus the innermost (failing) op are kept and path_truncated is set.
static inline void
exp_build_err_capture_path(exp_build_err* e, const build_frame* leaf)
{
	uint16_t depth = 0;

	for (const build_frame* f = leaf; f != NULL; f = f->parent) {
		depth++;
	}

	e->depth = depth;

	if (depth == 0) {
		e->n_frames = 0;
		e->path_truncated = false;
		return;
	}

	if (depth <= AS_EXP_TRACE_MAX_FRAMES) {
		// Fits - fill frames[] right-to-left so it ends up root -> fault.
		e->n_frames = depth;
		e->path_truncated = false;

		uint16_t ix = depth;

		for (const build_frame* f = leaf; f != NULL; f = f->parent) {
			e->frames[--ix] = f->op_code;
		}

		return;
	}

	// Deeper than the cap: keep the outermost (cap - 1) frames plus the
	// innermost (failing) op, dropping the middle. pack_exp_trace renders an
	// explicit "..." frame in the gap and path_truncated drives that.
	uint16_t n_outer = AS_EXP_TRACE_MAX_FRAMES - 1;

	e->n_frames = AS_EXP_TRACE_MAX_FRAMES;
	e->path_truncated = true;
	e->frames[AS_EXP_TRACE_MAX_FRAMES - 1] = leaf->op_code; // innermost

	// For each outer slot p, the frame is at distance (depth - 1 - p) from the
	// leaf toward the root. O(cap^2) but only on the deep-expression error path.
	for (uint16_t p = 0; p < n_outer; p++) {
		uint16_t dist = (uint16_t)(depth - 1 - p);
		const build_frame* f = leaf;

		for (uint16_t s = 0; s < dist; s++) {
			f = f->parent;
		}

		e->frames[p] = f->op_code;
	}
}

// 'offset' is the byte_offset (points past the op header, at the first arg);
// 'snippet_offset' is the start of the failing element, where snippet
// rendering begins. Pass-1 structural sites pass has_op false and render no
// snippet - their 'offset' is the coarse position where parsing halted.
static inline void
exp_build_err_record(uint32_t offset, uint32_t snippet_offset, bool has_op,
		exp_op_code op_code, const build_frame* leaf)
{
	if (g_exp_build_err.set) {
		return; // deepest failure already captured (first-set-wins)
	}

	g_exp_build_err.set = true;
	g_exp_build_err.offset = offset;
	g_exp_build_err.snippet_offset = snippet_offset;
	g_exp_build_err.has_op = has_op;
	g_exp_build_err.op_code = op_code;
	g_exp_build_err.depth = 0;
	g_exp_build_err.path_truncated = false;
	g_exp_build_err.n_frames = 0;
	g_exp_build_err.lang = 0;
	g_exp_build_err.has_msg = false;

	if (leaf != NULL) {
		exp_build_err_capture_path(&g_exp_build_err, leaf);
	}
}

// Record an AEL build failure (first-set-wins, like exp_build_err_record).
// 'offset'/'span' locate the offending region in the AEL source text; the
// payload retained at build_internal entry is repointed at that source so
// as_exp_take_build_error() can render the true source-slice snippet. 'msg' is
// the compile diagnostic (or a fixed fallback), copied out because diag
// storage dies with the parse pool.
void
ael_build_err_record(const uint8_t* src, uint32_t src_sz, bool has_pos,
		uint32_t offset, uint32_t span, const char* msg)
{
	if (g_exp_build_err.set) {
		return; // deepest failure already captured (first-set-wins)
	}

	g_exp_build_err.set = true;
	g_exp_build_err.lang = AS_EXP_TRACE_LANG_AEL;
	g_exp_build_err.has_op = false;
	g_exp_build_err.depth = 0;
	g_exp_build_err.path_truncated = false;
	g_exp_build_err.n_frames = 0;
	g_exp_build_err.has_pos = has_pos;
	g_exp_build_err.offset = has_pos ? offset : 0;
	g_exp_build_err.span = has_pos ? span : 0;
	g_exp_build_err.payload = src;
	g_exp_build_err.payload_sz = src_sz;
	g_exp_build_err.snippet_offset = 0; // unused for AEL

	if (msg != NULL) {
		size_t len = strlen(msg);

		if (len > sizeof(g_exp_build_err.msg) - 1) {
			len = sizeof(g_exp_build_err.msg) - 1;
		}

		memcpy(g_exp_build_err.msg, msg, len);
		g_exp_build_err.msg[len] = '\0';
		g_exp_build_err.has_msg = true;
	}
	else {
		g_exp_build_err.has_msg = false;
	}
}

//==========================================================
// Forward declarations.
//

// Build.
static as_exp* build_internal(const uint8_t* buf, uint32_t buf_sz,
		bool cpy_wire, cf_vector* bins_info_r);
static bool build_next(build_args* args);
static bool build_count_sz(msgpack_in* mp, build_counts* bc, uint32_t depth);
static var_entry* build_find_var_entry(build_args* args, const uint8_t* name,
		uint32_t name_sz);
static cdt_literal_status build_check_cdt_literal(const uint8_t* buf,
		uint32_t buf_sz, bool* marker_r);
static bool build_count_literal(build_counts* bc, msgpack_in* mp,
		msgpack_type literal_type);
static bool build_canonicalize_cdt(build_args* args, op_value_blob* op);

static bool build_default(build_args* args);
static bool build_meta_default(build_args* args);
static bool build_compare(build_args* args);
static bool build_cmp_regex(build_args* args);
static bool build_cmp_geo(build_args* args);
static bool build_in_list(build_args* args);
static bool build_logical_vargs(build_args* args);
static bool build_logical_not(build_args* args);
static bool build_math_vargs(build_args* args);
static bool build_number_op(build_args* args);
static bool build_float_op(build_args* args);
static bool build_int_op(build_args* args);
static bool build_to_string_op(build_args* args);
static bool build_device_size(build_args* args);
static bool build_memory_size(build_args* args);
static bool build_math_pow(build_args* args);
static bool build_math_log(build_args* args);
static bool build_math_mod(build_args* args);
static bool build_int_vargs(build_args* args);
static bool build_int_one(build_args* args);
static bool build_int_shift(build_args* args);
static bool build_int_scan(build_args* args);
static bool build_meta_digest_mod(build_args* args);
static bool build_rec_key(build_args* args);
static bool build_bin(build_args* args);
static bool build_bin_meta(build_args* args);
static bool build_cond(build_args* args);
static bool build_var_builtin(build_args* args);
static bool build_var(build_args* args);
static bool build_let(build_args* args);
static bool build_quote(build_args* args);
static bool build_call(build_args* args);
static bool build_ael_compile(build_args* args);
static bool build_map_kv(build_args* args);
static bool build_value_nil(build_args* args);
static bool build_value_bool(build_args* args);
static bool build_value_int(build_args* args);
static bool build_value_float(build_args* args);
static bool build_value_blob(build_args* args);
static bool build_value_geo(build_args* args);
static bool build_value_msgpack(build_args* args);

// Build utilities.
static bool parse_op_call(op_call* op, build_args* args);
// exp_build_set_expected_type is declared in exp/exp.h (shared with exp_ael.c).
static as_exp* check_filter_exp(as_exp* exp);

// Debug utilities.
static void debug_exp_check(const as_exp* exp);

//==========================================================
// Inlines & macros.
//

static inline bool
build_args_setup(build_args* args, const char* name)
{
	const op_table_entry* entry = args->entry;

	if (args->ele_count != entry->eval_param_count + entry->static_param_count) {
		cf_warning(AS_EXP, "%s - error %u expected %u args found %u", name,
				AS_ERR_PARAMETER,
				entry->eval_param_count + entry->static_param_count,
				args->ele_count);
		return false;
	}

	op_base_mem* ob = (op_base_mem*)args->mem;

	ob->code = entry->code;
	args->mem += entry->size;

	return true;
}

static inline const uint8_t*
bin_table_get_name(const build_bin_table* t, uint32_t idx)
{
	return t->base + t->table[idx].off;
}

//==========================================================
// Op table.
//

// clang-format off
#define op_table exp_op_table
const exp_op_table_entry op_table[] = {
		OP_TABLE_ENTRY(EXP_UNK, "unknown", op_base_mem, build_default, exp_eval_unknown, exp_display_0_args, 0, 0, EXP_RTYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_CMP_EQ, "eq", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_NE, "ne", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GT, "gt", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GE, "ge", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_LT, "lt", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_LE, "le", op_base_mem, build_compare, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_CMP_REGEX, "cmp_regex", op_cmp_regex, build_cmp_regex, exp_eval_cmp_regex, exp_display_cmp_regex, 2, 1, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GEO, "cmp_geo", op_base_mem, build_cmp_geo, exp_eval_compare, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_IN_LIST, "in_list", op_base_mem, build_in_list, exp_eval_in_list, exp_display_2_args, 0, 2, EXP_RTYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_AND, "and", op_base_mem, build_logical_vargs, exp_eval_and, exp_display_logical_vargs, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_OR, "or", op_base_mem, build_logical_vargs, exp_eval_or, exp_display_logical_vargs, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_NOT, "not", op_base_mem, build_logical_not, exp_eval_not, exp_display_1_arg, 0, 1, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_EXCLUSIVE, "exclusive", op_base_mem, build_logical_vargs, exp_eval_exclusive, exp_display_logical_vargs, 0, 0, EXP_RTYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_ADD, "add", op_base_mem, build_math_vargs, exp_eval_add, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_SUB, "sub", op_base_mem, build_math_vargs, exp_eval_sub, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_MUL, "mul", op_base_mem, build_math_vargs, exp_eval_mul, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_DIV, "div", op_base_mem, build_math_vargs, exp_eval_div, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_POW, "pow", op_base_mem, build_math_pow, exp_eval_pow, exp_display_2_args, 0, 2, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_LOG, "log", op_base_mem, build_math_log, exp_eval_log, exp_display_2_args, 0, 2, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_MOD, "mod", op_base_mem, build_math_mod, exp_eval_mod, exp_display_2_args, 0, 2, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_ABS, "abs", op_base_mem, build_number_op, exp_eval_abs, exp_display_1_arg, 0, 1, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_FLOOR, "floor", op_base_mem, build_float_op, exp_eval_floor, exp_display_1_arg, 0, 1, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_CEIL, "ceil", op_base_mem, build_float_op, exp_eval_ceil, exp_display_1_arg, 0, 1, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_TO_INT, "to_int", op_base_mem, build_float_op, exp_eval_to_int, exp_display_1_arg, 0, 1, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_TO_FLOAT, "to_float", op_base_mem, build_int_op, exp_eval_to_float, exp_display_1_arg, 0, 1, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_TO_STRING, "to_string", op_base_mem, build_to_string_op, exp_eval_to_string, exp_display_1_arg, 0, 1, EXP_RTYPE_STR)

		OP_TABLE_ENTRY(EXP_INT_AND, "int_and", op_base_mem, build_int_vargs, exp_eval_int_and, exp_display_int_vargs, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_OR, "int_or", op_base_mem, build_int_vargs, exp_eval_int_or, exp_display_int_vargs, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_XOR, "int_xor", op_base_mem, build_int_vargs, exp_eval_int_xor, exp_display_int_vargs, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_NOT, "int_not", op_base_mem, build_int_one, exp_eval_int_not, exp_display_1_arg, 0, 1, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_LSHIFT, "int_lshift", op_base_mem, build_int_shift, exp_eval_int_lshift, exp_display_2_args, 0, 2, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_RSHIFT, "int_rshift", op_base_mem, build_int_shift, exp_eval_int_rshift, exp_display_2_args, 0, 2, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_ARSHIFT, "int_arshift", op_base_mem, build_int_shift, exp_eval_int_arshift, exp_display_2_args, 0, 2, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_COUNT, "int_count", op_base_mem, build_int_one, exp_eval_int_count, exp_display_1_arg, 0, 1, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_LSCAN, "int_lscan", op_base_mem, build_int_scan, exp_eval_int_lscan, exp_display_2_args, 0, 2, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_RSCAN, "int_rscan", op_base_mem, build_int_scan, exp_eval_int_rscan, exp_display_2_args, 0, 2, EXP_RTYPE_INT)

		OP_TABLE_ENTRY(EXP_MIN, "min", op_base_mem, build_math_vargs, exp_eval_min, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_MAX, "max", op_base_mem, build_math_vargs, exp_eval_max, exp_display_math_vargs, 0, 0, EXP_RTYPE_END)

		OP_TABLE_ENTRY(EXP_META_DIGEST_MOD, "digest_modulo", op_meta_digest_modulo, build_meta_digest_mod, exp_eval_meta_digest_mod, exp_display_meta_digest_mod, 1, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_DEVICE_SIZE, "device_size", op_base_mem, build_device_size, exp_eval_meta_device_size, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_LAST_UPDATE, "last_update", op_base_mem, build_meta_default, exp_eval_meta_last_update, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_SINCE_UPDATE, "since_update", op_base_mem, build_meta_default, exp_eval_meta_since_update, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_VOID_TIME, "void_time", op_base_mem, build_meta_default, exp_eval_meta_void_time, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_TTL, "ttl", op_base_mem, build_meta_default, exp_eval_meta_ttl, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_SET_NAME, "set_name", op_base_mem, build_meta_default, exp_eval_meta_set_name, exp_display_0_args, 0, 0, EXP_RTYPE_STR)
		OP_TABLE_ENTRY(EXP_META_KEY_EXISTS, "key_exists", op_base_mem, build_meta_default, exp_eval_meta_key_exists, exp_display_0_args, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_META_IS_TOMBSTONE, "is_tombstone", op_base_mem, build_meta_default, exp_eval_meta_is_tombstone, exp_display_0_args, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_META_MEMORY_SIZE, "memory_size", op_base_mem, build_memory_size, exp_eval_meta_memory_size, exp_display_0_args, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_META_RECORD_SIZE, "record_size", op_base_mem, build_meta_default, exp_eval_meta_record_size, exp_display_0_args, 0, 0, EXP_RTYPE_INT)

		OP_TABLE_ENTRY(EXP_REC_KEY, "key", op_base_mem, build_rec_key, exp_eval_rec_key, exp_display_0_args, 1, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_BIN, "bin", op_var, build_bin, exp_eval_bin, exp_display_bin, 2, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_BIN_TYPE, "bin_type", op_var, build_bin_meta, exp_eval_bin_type, exp_display_bin_type, 1, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_BIN_EXISTS, "bin_exists", op_var, build_bin_meta, exp_eval_bin_exists, exp_display_bin_exists, 1, 0, EXP_RTYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_RESULT_REMOVE, "result_remove", op_base_mem, build_default, exp_eval_result_remove, exp_display_0_args, 0, 0, EXP_RTYPE_RESULT_REMOVE)

		OP_TABLE_ENTRY(EXP_MAP_KEYS, "map_keys", op_base_mem, build_map_kv, exp_eval_map_keys, exp_display_1_arg, 0, 1, EXP_RTYPE_LIST)
		OP_TABLE_ENTRY(EXP_MAP_VALUES, "map_values", op_base_mem, build_map_kv, exp_eval_map_values, exp_display_1_arg, 0, 1, EXP_RTYPE_LIST)

		OP_TABLE_ENTRY(EXP_VAR_BUILTIN, "var_builtin", op_var, build_var_builtin, exp_eval_var_builtin, exp_display_var_builtin, 2, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_COND, "cond", op_cond, build_cond, exp_eval_cond, exp_display_cond, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_VAR, "var", op_var, build_var, exp_eval_var, exp_display_var, 1, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_LET, "let", op_let, build_let, exp_eval_let, exp_display_let, 0, 0, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_QUOTE, "quote", op_value_blob, build_quote, exp_eval_value, exp_display_value, 1, 0, EXP_RTYPE_LIST)
		OP_TABLE_ENTRY(EXP_CALL, "call", op_call, build_call, exp_eval_call, exp_display_call, 2, 2, EXP_RTYPE_END)
		OP_TABLE_ENTRY(EXP_AEL_COMPILE, "ael_compile", op_base_mem, build_ael_compile, NULL, exp_display_1_arg, 1, 0, EXP_RTYPE_END)

		OP_TABLE_ENTRY(EXP_VOP_VALUE_NIL, "nil", op_base_mem, build_value_nil, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_NIL)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_BOOL, "bool", op_value_bool, build_value_bool, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_TRILEAN, "trilean", op_base_mem, NULL, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_INT, "int", op_value_int, build_value_int, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_INT)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_FLOAT, "float", op_value_float, build_value_float, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_STR, "str", op_value_blob, build_value_blob, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_STR)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_BLOB, "blob", op_value_blob, build_value_blob, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_BLOB)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_GEO, "geo", op_value_geo, build_value_geo, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_GEOJSON)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_MSGPACK, "msgpack", op_value_blob, build_value_msgpack, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_END)

		OP_TABLE_ENTRY(EXP_VOP_VALUE_HLL, "hll", op_value_blob, NULL, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_HLL)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_MAP, "map", op_base_mem, NULL, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_MAP)
		OP_TABLE_ENTRY(EXP_VOP_VALUE_LIST, "list", op_value_blob, NULL, exp_eval_value, exp_display_value, 0, 0, EXP_RTYPE_LIST)

		OP_TABLE_ENTRY(EXP_VOP_COND_CASE, "case", op_base_mem, NULL, NULL, exp_display_case, 0, 0, EXP_RTYPE_END)
};
// clang-format on

//==========================================================
// Public API.
//

as_exp*
as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(buf64_sz);
	define_deferred_memory(buf, buf_sz);
	uint32_t buf_sz_out;

	if (! cf_b64_validate_and_decode(buf64, buf64_sz, buf, &buf_sz_out)) {
		return NULL;
	}

	cf_debug(AS_EXP, "as_exp_filter_build_base64 - buf_sz %u msg-dump:\n%*pH",
			buf_sz_out, buf_sz_out, buf);

	as_exp* exp = build_internal(buf, buf_sz_out, true, NULL);
	as_exp* checked = exp == NULL ? NULL : check_filter_exp(exp);

	// build_internal retained 'buf' as the snippet payload, which dies with
	// this scope (deferred free). Reset AFTER check_filter_exp, not between
	// it and the build: check_filter_exp is itself a recording site on the
	// AEL arm, so a reset placed before it leaves g_exp_build_err.set true on
	// return - armed thread-local state for a build this caller never takes.
	// (This entry's only callers today are XDR filter config, which never
	// stage a trace.)
	as_exp_build_err_reset();

	return checked;
}

as_exp*
as_exp_filter_build_ael(const uint8_t* ael_str, uint32_t ael_sz)
{
	as_exp* exp = exp_build_internal_ael(ael_str, ael_sz, NULL);

	return exp == NULL ? NULL : check_filter_exp(exp);
}

as_exp*
as_exp_filter_build(const as_msg_field* m, bool cpy_wire)
{
	cf_debug(AS_EXP, "as_exp_filter_build - msg_field_sz %u msg-dump\n%*pH",
			m->field_sz, as_msg_field_get_value_sz(m), m->data);

	as_exp* exp = build_internal(m->data, as_msg_field_get_value_sz(m),
			cpy_wire, NULL);

	return exp == NULL ? NULL : check_filter_exp(exp);
}

as_exp*
as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire,
		cf_vector* bins_info_r)
{
	cf_debug(AS_EXP, "as_exp_build_buf - buf_sz %u buf-dump:\n%*pH", buf_sz,
			buf_sz, buf);

	return build_internal(buf, buf_sz, cpy_wire, bins_info_r);
}

void
as_exp_destroy(as_exp* exp)
{
	if (exp == NULL) {
		return;
	}

	for (uint32_t i = 0; i < exp->cleanup_stack_ix; i++) {
		op_base_mem* ob = exp->cleanup_stack[i];

		switch (ob->code) {
		case EXP_VOP_VALUE_GEO:
			cf_assert(((op_value_geo*)ob)->compiled.type == GEO_REGION, AS_EXP,
					"unexpected");
			geo_region_destroy(((op_value_geo*)ob)->compiled.region);
			break;
		case EXP_CMP_REGEX:
			regfree(&((op_cmp_regex*)ob)->regex);
			break;
		case EXP_VOP_VALUE_LIST:
		case EXP_VOP_VALUE_MSGPACK:
			// A canonicalized CDT literal retained at build - literals that
			// arrived canonical alias the wire and are never on this stack.
			cf_free((void*)((op_value_blob*)ob)->value);
			break;
		default:
			break;
		}
	}

	cf_free(exp);
}

// Append a NUL-terminated string into buf[*used .. cap), tracking *used. Stops
// (and marks truncation by returning false) once it would overflow, so the
// renderer can stop early instead of writing a misleading partial token.
static bool
snippet_append(char* buf, uint32_t cap, uint32_t* used, const char* s)
{
	uint32_t len = (uint32_t)strlen(s);

	if (*used + len + 1 > cap) { // +1 for the NUL
		return false;
	}

	memcpy(buf + *used, s, len);
	*used += len;
	buf[*used] = '\0';

	return true;
}

// Render a bounded, single-line slice of the AEL source with the offending
// [offset, offset+span) region focus-marked (e.g. ... a + »5 / 0« ...).
// Human-only: control characters flatten to spaces, truncated edges are
// marked "...". Returns the rendered length (no NUL); 0 means nothing usable
// was rendered. Used by both build diagnostics and runtime eval traces.
#define AEL_SNIPPET_PRE_CTX 40 // source bytes kept ahead of the focus

// Recursion cap for snippet_render's msgpack walk. Deliberately not the wire
// path-frame cap: this bounds how deep the rendered expression nests, so tuning
// the path array for field-45 size reasons must not change it.
#define EXP_SNIPPET_MAX_DEPTH 16

uint16_t
exp_render_ael_src_snippet(const uint8_t* src, uint32_t src_sz, uint32_t offset,
		uint32_t span, char* out, uint32_t cap)
{
	// cap >= 16 covers the NUL, both 2-byte focus marks, and both "..."
	// pads, so the budget arithmetic below cannot underflow.
	if (src == NULL || src_sz == 0 || offset > src_sz || cap < 16) {
		return 0;
	}

	if (span > src_sz - offset) {
		span = src_sz - offset;
	}

	if (span == 0 && offset < src_sz) {
		span = 1; // a point diagnostic focuses the character it points at
	}

	// Window: up to PRE_CTX bytes of context before the focus, then as much of
	// the focus + tail as the budget allows.
	uint32_t start = offset > AEL_SNIPPET_PRE_CTX ? offset - AEL_SNIPPET_PRE_CTX
												  : 0;

	// Both window edges are raw byte offsets, so either can land inside a
	// multi-byte sequence and turn valid UTF-8 source into an invalid str.
	// Advance the head off any continuation byte. Bounded by offset, which is
	// PRE_CTX bytes away, so this both terminates and stays in bounds.
	if (start != 0) {
		while (start < offset && (src[start] & 0xc0) == 0x80) {
			start++;
		}
	}

	uint32_t used = 0;

	if (start > 0 && ! snippet_append(out, cap, &used, "...")) {
		return 0;
	}

	// Text budget: cap less the NUL, the two 2-byte UTF-8 focus marks, and a
	// possible trailing "...".
	uint32_t budget = cap - 1 - 4 - 3 - used;
	uint32_t end = src_sz;
	bool tail_truncated = false;

	if (end - start > budget) {
		end = start + budget;
		tail_truncated = true;

		// Same for the tail. end is exclusive, so a continuation byte AT end
		// means the sequence reaching it is split. In bounds: end < src_sz here,
		// since end - start > budget implied src_sz - start > budget.
		while (end > start && (src[end] & 0xc0) == 0x80) {
			end--;
		}
	}

	bool closed = false;

	for (uint32_t i = start; i < end; i++) {
		if (i == offset && ! snippet_append(out, cap, &used, "\xc2\xbb")) {
			return 0; // focus open mark
		}

		if (i == offset + span && ! closed) {
			if (! snippet_append(out, cap, &used, "\xc2\xab")) {
				return 0; // focus close mark
			}

			closed = true;
		}

		uint8_t c = src[i];

		out[used++] = (c < 0x20 || c == 0x7f) ? ' ' : (char)c;
	}

	out[used] = '\0';

	if (offset >= end && ! snippet_append(out, cap, &used, "\xc2\xbb")) {
		return 0; // focus starts at/past the window edge (e.g. EOF diagnostic)
	}

	if (! closed && ! snippet_append(out, cap, &used, "\xc2\xab")) {
		return 0; // focus ran to the window edge - close it
	}

	if (tail_truncated && ! snippet_append(out, cap, &used, "...")) {
		return 0;
	}

	// Trace key 6 ships as a msgpack str, which is defined to hold UTF-8, and
	// the source is unvalidated client bytes - the edge alignment above fixes a
	// split sequence but cannot make non-UTF-8 input valid. Drop the snippet
	// whole rather than push an invalid str at the client's decoder, where it
	// would fail while decoding the very error being reported. Callers already
	// treat 0 as has_snippet false.
	if (! cf_str_is_valid_utf8((const uint8_t*)out, used)) {
		return 0;
	}

	return (uint16_t)used;
}

// Render one msgpack element (advancing mp past it) into the snippet buffer as
// short human-readable text. A list whose first element is a known op code is
// rendered op_name(arg, ...) - the readable expression form; any other element
// is rendered as a scalar via msgpack_display (e.g. "5", "6.0", "<string#3>").
// Bounded by 'cap'; on overflow it appends "..." (best effort) and returns
// false so the caller marks the snippet truncated. depth_left guards against
// pathologically deep payloads. Human-only - clients must not parse it.
static bool
snippet_render(msgpack_in* mp, char* buf, uint32_t cap, uint32_t* used,
		uint32_t depth_left)
{
	if (depth_left == 0) {
		snippet_append(buf, cap, used, "...");
		return false;
	}

	// The (already malformed) payload can over-claim a list's element count,
	// leaving the cursor at the end. Bail here rather than round-trip through
	// msgpack_peek_type + msgpack_display, which fail closed on the resulting
	// MSGPACK_TYPE_ERROR anyway. Redundant, kept because it names the
	// exhausted-cursor case at the point it happens.
	if (mp->offset >= mp->buf_sz) {
		return false;
	}

	msgpack_type type = msgpack_peek_type(mp);

	if (type == MSGPACK_TYPE_LIST) {
		// Peek the op code without disturbing mp - use a copy.
		msgpack_in peek = *mp;
		uint32_t ele_count = 0;
		uint64_t op_code = 0;

		if (! msgpack_get_list_ele_count(&peek, &ele_count) || ele_count == 0 ||
				! msgpack_get_uint64(&peek, &op_code)) {
			// Not an op list (empty, or head is not an op code) - render the
			// whole element as one token. Committing just the header for a
			// non-int head would misalign the arg walk with the element
			// stream, leaking inner elements into the enclosing op's args.
			msgpack_display_str s;

			if (! msgpack_display(mp, &s)) {
				return false;
			}

			return snippet_append(buf, cap, used, s.str);
		}

		const char* name = NULL;

		if (op_code < EXP_OP_CODE_END && op_table[op_code].code != 0) {
			name = op_table[op_code].name;
		}

		// Commit: consume the list header + op code from mp.
		(void)msgpack_get_list_ele_count(mp, &ele_count);
		(void)msgpack_get_uint64(mp, &op_code);

		uint32_t n_args = ele_count - 1; // -1 for the op code

		if (! snippet_append(buf, cap, used, name != NULL ? name : "op")) {
			return false;
		}

		if (! snippet_append(buf, cap, used, "(")) {
			return false;
		}

		// No explicit arity cap needed: snippet_append() fails the moment
		// output would exceed 'cap', so depth_left caps depth and the buffer
		// caps breadth.
		for (uint32_t i = 0; i < n_args; i++) {
			if (i != 0 && ! snippet_append(buf, cap, used, ", ")) {
				return false;
			}

			if (! snippet_render(mp, buf, cap, used, depth_left - 1)) {
				return false;
			}
		}

		return snippet_append(buf, cap, used, ")");
	}

	// Scalar (or map) - render a single token and advance mp past it.
	msgpack_display_str s;

	if (! msgpack_display(mp, &s)) {
		return false;
	}

	return snippet_append(buf, cap, used, s.str);
}

// Pre-render the human-only snippet for the failing element into 'out'. Only
// pass-2 (per-op) failures carry a reliable element start (build_next captures
// it before consuming the op header), so the snippet is rendered only when an
// op was recorded. Pass-1 structural sites stop mid-stream at an unvalidated
// boundary, so they get no snippet (offset + message suffice). Best-effort:
// any render failure just leaves has_snippet false.
static void
render_build_snippet(as_exp_build_error* out)
{
	if (! g_exp_build_err.has_op || g_exp_build_err.payload == NULL ||
			g_exp_build_err.snippet_offset >= g_exp_build_err.payload_sz) {
		return;
	}

	msgpack_in mp = { .buf = g_exp_build_err.payload,
		.buf_sz = g_exp_build_err.payload_sz,
		.offset = g_exp_build_err.snippet_offset };

	uint32_t used = 0;

	out->snippet[0] = '\0';

	// Only expose the snippet on a COMPLETE render. snippet_render() returns
	// false on overflow / truncation / malformed payload, which can leave a
	// misleading partial fragment (e.g. "eq(5, " with no closing paren) - drop
	// the whole component rather than emit a partial (matches the field-45
	// budget policy and this function's "render failure -> has_snippet false").
	if (snippet_render(&mp, out->snippet, sizeof(out->snippet), &used,
				EXP_SNIPPET_MAX_DEPTH) &&
			used != 0) {
		out->snippet_len = (uint16_t)used;
		out->has_snippet = true;
	}
}

bool
as_exp_take_build_error(as_exp_build_error* out)
{
	if (! g_exp_build_err.set) {
		// A build can return NULL without recording - check_filter_exp()'s wire
		// arm rejects a non-bool root and has no message channel to record into -
		// yet build_internal() stamped its borrowed payload ref regardless. Drop
		// it here so every exit of this function, and of
		// as_exp_stage_build_error_details(), honors the contract.
		as_exp_build_err_reset();
		return false;
	}

	if (g_exp_build_err.lang == AS_EXP_TRACE_LANG_AEL) {
		// AEL build failure: no op/path (there is no compiled op stream), no
		// msgpack byte_offset - instead the source-text position + span, the
		// compile diagnostic, and the true source-slice snippet.
		*out = (as_exp_build_error){ .phase = AS_EXP_TRACE_PHASE_BUILD,
			.lang = AS_EXP_TRACE_LANG_AEL };

		if (g_exp_build_err.has_pos) {
			out->has_ael_offset = true;
			out->ael_offset = g_exp_build_err.offset;
			out->has_ael_span = g_exp_build_err.span != 0;
			out->ael_span = g_exp_build_err.span;

			out->snippet_len = exp_render_ael_src_snippet(g_exp_build_err.payload,
					g_exp_build_err.payload_sz, g_exp_build_err.offset,
					g_exp_build_err.span, out->snippet, sizeof(out->snippet));
			out->has_snippet = out->snippet_len != 0;
		}

		if (g_exp_build_err.has_msg) {
			strcpy(out->msg, g_exp_build_err.msg);
			out->has_msg = true;
		}

		g_exp_build_err.set = false;
		g_exp_build_err.payload = NULL;
		g_exp_build_err.payload_sz = 0;

		return true;
	}

	*out = (as_exp_build_error){ .phase = AS_EXP_TRACE_PHASE_BUILD,
		.has_offset = true,
		.byte_offset = g_exp_build_err.offset };

	if (g_exp_build_err.has_op) {
		const char* name = op_table[g_exp_build_err.op_code].name;

		if (name != NULL) {
			size_t len = strlen(name);

			if (len > sizeof(out->op) - 1) {
				len = sizeof(out->op) - 1;
			}

			memcpy(out->op, name, len);
			out->op[len] = '\0';
			out->op_len = (uint16_t)len;
			out->has_op = true;
		}
	}

	// Pre-render the ancestor path op names (root -> fault). The op-name
	// table lives here, so callers and proto.c stay free of it.
	if (g_exp_build_err.n_frames != 0) {
		out->depth = g_exp_build_err.depth;
		out->path_truncated = g_exp_build_err.path_truncated;
		out->n_frames = g_exp_build_err.n_frames;

		for (uint16_t i = 0; i < g_exp_build_err.n_frames; i++) {
			const char* name = op_table[g_exp_build_err.frames[i]].name;

			if (name == NULL) {
				name = "op";
			}

			size_t len = strlen(name);

			if (len > sizeof(out->path[i]) - 1) {
				len = sizeof(out->path[i]) - 1;
			}

			memcpy(out->path[i], name, len);
			out->path[i][len] = '\0';
		}

		out->has_path = true;
	}

	// Pre-render the human-only snippet from the failing element.
	render_build_snippet(out);

	g_exp_build_err.set = false;
	// Drop the retained payload reference now that the snippet is rendered, so a
	// later read can never dereference a buffer that has since been freed.
	g_exp_build_err.payload = NULL;
	g_exp_build_err.payload_sz = 0;

	return true;
}

// Reset the accumulator for a build whose result is NOT taken via
// as_exp_take_build_error(). build_internal() retains 'payload' pointing into
// the caller's buffer for a lazy snippet render - a non-taking caller must
// drop that ref so a stale thread-local can't point into a freed request
// buffer.
void
as_exp_build_err_reset(void)
{
	g_exp_build_err.set = false;
	g_exp_build_err.lang = 0;
	g_exp_build_err.payload = NULL;
	g_exp_build_err.payload_sz = 0;
}

void
as_exp_stage_build_error_details(const char* context_msg)
{
	// Nothing below this point can reach the wire with details off, and
	// as_exp_take_build_error() is not cheap - it zeroes a ~1 KB struct,
	// renders the op name and up to AS_EXP_TRACE_MAX_FRAMES path frames, and
	// walks the client's msgpack payload to render a snippet. A malformed
	// expression field is client-reachable at full request rate, so pay none
	// of it with details off. Still drop the accumulator's retained payload
	// ref - it points into a request buffer about to be freed.
	//
	// The gate is OFF-only, not tiered: tiers 1 and 2 still pay the whole
	// render, and only the message survives (the trace components are dropped
	// by as_error_exp_trace_set's own TRACE check). The take has to run anyway
	// for tier 2's be.msg fold, and set_fmt has to run at every tier to claim
	// first-set-wins - so the residue is the op/path/snippet render on an
	// already-failed build, not a second gate waiting to be added.
	if (g_error_verbosity == AS_ERROR_VERBOSITY_OFF) {
		as_exp_build_err_reset();
		return;
	}

	as_exp_build_error be;
	bool has_be = as_exp_take_build_error(&be);

	// A build-error with none of offset/op/snippet/ael_offset is message-only
	// by definition - its trace would carry only phase+lang, which is noise.
	// Wire (msgpack) builds always set has_offset and positioned AEL
	// parse/build errors always set has_ael_offset, so their real traces are
	// unaffected.
	if (has_be &&
			(be.has_offset || be.has_op || be.has_snippet || be.has_ael_offset)) {
		as_error_exp_trace_set_from_build(&be);
	}

	// An AEL build carries its compile diagnostic - fold it in.
	if (has_be && be.has_msg) {
		as_error_details_set_fmt(AS_SUB_NONE, "%s: %s", context_msg, be.msg);
	}
	else {
		as_error_details_set_fmt(AS_SUB_NONE, "%s", context_msg);
	}
}

//==========================================================
// Inter-file helpers.
//
// Non-static functions shared across the exp/ module but not part of the
// public as_exp_* API: exp_geo_mp_to_op, called from exp_rt.c (eval).
// (ael_pack_ctx lives in ael_codegen.c, alongside ael_pack_ctx_seg_pair.)
//

bool
exp_geo_mp_to_op(msgpack_in* mp, op_value_geo* op, const char* debug_str)
{
	op->contents = mp->buf + mp->offset;

	uint32_t json_sz;
	const uint8_t* json = msgpack_get_bin(mp, &json_sz);

	cf_assert(json_sz != 0, AS_EXP, "unexpected");

	op->content_sz = (uint32_t)(mp->buf + mp->offset - op->contents);

	if (json == NULL) {
		cf_warning(AS_EXP, "%s - error %u failed to parse string", debug_str,
				AS_ERR_PARAMETER);
		return false;
	}

	// Skip as_bytes type.
	json++;
	json_sz--;

	uint64_t cellid;
	geo_region_t region;

	op->compiled.region = NULL;

	if (! as_geojson_parse(NULL, (const char*)json, json_sz, &cellid, &region)) {
		cf_warning(AS_EXP, "%s - error %u invalid geojson", debug_str,
				AS_ERR_PARAMETER);
		return false;
	}

	if (region != NULL) {
		op->compiled.type = GEO_REGION;
		op->compiled.region = region;
	}
	else {
		op->compiled.type = GEO_CELL;
		op->compiled.cellid = cellid;
	}

	return true;
}

//==========================================================
// Local helpers - build.
//

static as_exp*
build_internal(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire,
		cf_vector* bins_info_r)
{
	msgpack_in mp = { .buf = buf, .buf_sz = buf_sz };
	uint32_t top_count;
	bool is_list = msgpack_buf_get_list_ele_count(mp.buf, mp.buf_sz, &top_count);

	// Reset the build-failure accumulator - build_internal is the single entry
	// for all public build functions, so one reset covers them all. Retain the
	// payload so the snippet can be rendered from the failing element later.
	// lang resets to wire; the AEL path re-stamps it when it records.
	//
	// Stamped here rather than at record time because this is the only scope
	// holding the original buffer: with cpy_wire the deep recording site has
	// only args.mp.buf, which points into the as_exp that as_exp_destroy() frees
	// on that same failure path.
	g_exp_build_err.set = false;
	g_exp_build_err.lang = 0;
	g_exp_build_err.payload = buf;
	g_exp_build_err.payload_sz = buf_sz;

	if (is_list && top_count == 0) {
		cf_warning(AS_EXP, "build_internal - empty list");
		exp_build_err_record(0, 0, false, EXP_UNK, NULL);
		return NULL;
	}

	// Detect AEL compilation: top-level [EXP_AEL_COMPILE, <ael_string>].
	// top_count is meaningful only when the top level is a list.
	if (is_list && top_count == 2) {
		uint32_t saved_offset = mp.offset;

		msgpack_get_list_ele_count(&mp, &top_count); // skip list header
		uint64_t first_op;

		if (msgpack_get_uint64(&mp, &first_op) && first_op == EXP_AEL_COMPILE) {
			uint32_t ael_sz;
			const uint8_t* ael_str = msgpack_get_bin(&mp, &ael_sz);

			if (ael_str != NULL && ael_sz > 0) {
				return exp_build_internal_ael(ael_str, ael_sz, bins_info_r);
			}

			cf_warning(AS_EXP, "build_internal - invalid AEL compilation");
			// Keep the reporting contract every other build failure meets:
			// no position/snippet (the defect is the envelope, not the
			// source), but a diagnostic still folds into the message.
			ael_build_err_record(NULL, 0, false, 0, 0,
					"invalid AEL compilation envelope");
			return NULL;
		}

		mp.offset = saved_offset;
	}

	// Proto layer caps this upstream; a larger buffer is a caller bug.
	cf_assert(buf_sz <= PROTO_SIZE_MAX, AS_EXP,
			"build_internal - expression field too large %u bytes", buf_sz);

	exp_bin_name_entry table[RECORD_MAX_BINS];

	build_counts bc = {
		.counter = 1,
		.end = mp.buf + mp.buf_sz,
		.bin_table = { .table = table, .base = mp.buf },
	};

	if (! build_count_sz(&mp, &bc, 0)) {
		// Pass-1 (structural) failure - mp.offset is the coarse position where
		// parsing halted (it may sit past the failing element's header). The op
		// is unknown and there is no live frame chain (pre-tree), so no op, no
		// path, and no snippet - only the offset and the caller's message.
		exp_build_err_record(mp.offset, mp.offset, false, EXP_UNK, NULL);
		return NULL;
	}

	if (bc.counter != 0) {
		cf_warning(AS_EXP,
				"build_internal - incomplete expression field expected %u more elements",
				bc.counter);
		exp_build_err_record(mp.offset, mp.offset, false, EXP_UNK, NULL);
		return NULL;
	}

	if (bc.total_sz >= EXP_MAX_SIZE) {
		cf_warning(AS_EXP,
				"build_internal - expression size exceeds limit of %u bytes",
				EXP_MAX_SIZE);
		exp_build_err_record(mp.offset, mp.offset, false, EXP_UNK, NULL);
		return NULL;
	}

	if (mp.offset != mp.buf_sz) {
		cf_warning(AS_EXP, "build_internal - malformed expression field");
		exp_build_err_record(mp.offset, mp.offset, false, EXP_UNK, NULL);
		return NULL;
	}

	// CDT literals found out of canonical form get sorted into retained
	// allocations at build - reserve a cleanup slot for each.
	bc.cleanup_count += bc.literal_cleanup;

	bc.total_sz = (bc.total_sz + 7) & ~7; // align to 8 bytes

	uint32_t cleanup_offset = bc.total_sz;

	bc.total_sz += bc.cleanup_count * (uint32_t)sizeof(void*);

	uint32_t bin_table_offset = bc.total_sz;

	bc.total_sz += sizeof(exp_rt_bin_table) +
			bc.bin_table.n_bins * sizeof(exp_bin_name_entry);

	if (cpy_wire) {
		bc.total_sz += mp.buf_sz;
	}

	build_args args = {
		.exp = cf_calloc(1, sizeof(as_exp) + bc.total_sz),
		.literal_cleanup = bc.literal_cleanup,
		.has_marker_literal = bc.has_marker_literal,
		.bins_info_r = bins_info_r,
	};

	args.mem = args.exp->mem;
	args.exp->cleanup_stack = (void**)(args.mem + cleanup_offset);

	exp_rt_bin_table* rt_bins = (exp_rt_bin_table*)(args.mem + bin_table_offset);

	args.exp->bin_table = rt_bins;
	args.bin_table = bc.bin_table;

	if (cpy_wire) {
		uint8_t* wire_mem = args.mem + bc.total_sz - mp.buf_sz;

		memcpy(wire_mem, buf, mp.buf_sz);
		args.mp.buf = wire_mem;
	}
	else {
		args.mp.buf = buf;
	}

	args.mp.buf_sz = mp.buf_sz;

	// Bins occupy the first n_bins var slots; let-vars follow.
	args.var_idx = bc.bin_table.n_bins;
	args.max_var_idx = bc.bin_table.n_bins;

	debug_exp_check(args.exp);

	if (! build_next(&args)) {
		as_exp_destroy(args.exp);
		return NULL;
	}

	cf_assert(args.mem <= (uint8_t*)args.exp->cleanup_stack, AS_EXP,
			"read past cleanup_stack %p > %p", args.mem, args.exp->cleanup_stack);
	cf_assert(args.exp->cleanup_stack_ix <= bc.cleanup_count, AS_EXP,
			"cleanup_stack_ix (%u) not equal to cleanup_count (%u)",
			args.exp->cleanup_stack_ix, bc.cleanup_count);
	// One-sided: a remainder proves the build pass skipped a literal it owed,
	// but releasing one it never owed balances the books just as well and is
	// invisible here - the literal it stole from aliases non-canonical.
	cf_assert(args.literal_cleanup == 0, AS_EXP,
			"literal_cleanup (%u) not consumed", args.literal_cleanup);

	memcpy(rt_bins->table, bc.bin_table.table,
			bc.bin_table.n_bins * sizeof(exp_bin_name_entry));
	rt_bins->n_bins = bc.bin_table.n_bins;
	// buf may have been copied so we cannot use bc.bin_table.base here.
	rt_bins->base = args.mp.buf;

	args.exp->max_var_count = args.max_var_idx;

	if (! exp_build_set_expected_type(args.exp, args.entry)) {
		exp_build_err_record(args.mp.offset, args.mp.offset, false, EXP_UNK,
				NULL);
		as_exp_destroy(args.exp);
		return NULL;
	}

	if (cf_log_check_level(AS_EXP, CF_DETAIL)) {
		cf_dyn_buf_define_size(db, 10240);
		runtime rt = {
			.instr_ptr = args.exp->mem,
			.bin_table = args.exp->bin_table,
		};

		exp_rt_display(&rt, &db);

		cf_detail(AS_EXP, "parsed exp: %.*s", (int)db.used_sz, db.buf);

		cf_dyn_buf_free(&db);
	}

	return args.exp;
}

static bool
build_next(build_args* args)
{
	if (args->depth >= EXP_MAX_DEPTH) {
		cf_warning(AS_EXP, "build_next - expression nesting exceeds %u",
				EXP_MAX_DEPTH);
		return false;
	}

	args->depth++;
	cf_defer { args->depth--; }

	// Offset of the element about to be built (the op's whole list element, or
	// a bare scalar) - before the header/op-code are consumed. The
	// byte_offset points past them (at the first arg); the snippet renderer
	// needs the element start so it can render op_name(arg, ...).
	uint32_t elem_offset = args->mp.offset;

	msgpack_type type = msgpack_peek_type(&args->mp);

	cf_assert(type != MSGPACK_TYPE_ERROR, AS_EXP, "unexpected");

	uint64_t op_code;
	uint32_t ele_count = 0;

	if (type != MSGPACK_TYPE_LIST) {
		switch (type) {
		case MSGPACK_TYPE_NIL:
			op_code = EXP_VOP_VALUE_NIL;
			break;
		case MSGPACK_TYPE_FALSE:
		case MSGPACK_TYPE_TRUE:
			op_code = EXP_VOP_VALUE_BOOL;
			break;
		case MSGPACK_TYPE_NEGINT:
		case MSGPACK_TYPE_INT:
			op_code = EXP_VOP_VALUE_INT;
			break;
		case MSGPACK_TYPE_DOUBLE:
			op_code = EXP_VOP_VALUE_FLOAT;
			break;
		case MSGPACK_TYPE_STRING:
			op_code = EXP_VOP_VALUE_STR;
			break;
		case MSGPACK_TYPE_BYTES:
			op_code = EXP_VOP_VALUE_BLOB;
			break;
		case MSGPACK_TYPE_GEOJSON:
			op_code = EXP_VOP_VALUE_GEO;
			break;
		default:
			op_code = EXP_VOP_VALUE_MSGPACK;
			break;
		}
	}
	else {
		msgpack_get_list_ele_count(&args->mp, &ele_count);
		msgpack_get_uint64(&args->mp, &op_code);
		ele_count--; // -1 for op_code

		cf_assert(op_code < EXP_OP_CODE_END && op_table[op_code].size != 0,
				AS_EXP, "invalid expression op %lu", op_code);
	}

	args->entry = &op_table[op_code];
	args->ele_count = ele_count;
	args->instr_ix++;

	// Push this op onto the ancestor chain before recursing into children, so
	// a child failure walks back through it (and pop on return). The frame is
	// a stack local - the chain is only ever read synchronously on the error
	// path while these frames are still live.
	build_frame frame = { .parent = args->current_frame,
		.op_code = (exp_op_code)op_code };

	args->current_frame = &frame;

	op_base_mem* op = (op_base_mem*)args->mem;
	uint32_t op_offset = args->mp.offset;
	bool rv = op_table[op_code].build_cb(args);

	if (! rv) {
		// One site covers every per-op pass-2 build failure. build_cb recurses
		// into build_next for children, so the deepest failing op records first
		// and first-set-wins keeps the innermost one. The live frame chain
		// (this op back to the root) is the path to the fault.
		exp_build_err_record(op_offset, elem_offset, true, (exp_op_code)op_code,
				args->current_frame);
	}

	args->current_frame = frame.parent;

	op->instr_end_ix = args->instr_ix;

	debug_exp_check(args->exp);

	return rv;
}

const exp_op_table_entry*
exp_build_get_entry(exp_rtype type)
{
	if ((uint32_t)type >=
			EXP_RTYPE_END) { // (uint32_t) cast because enum can be signed
		return NULL;
	}

	return &op_table[exp_rtype_to_op_code[type]];
}

static bool
build_count_sz(msgpack_in* mp, build_counts* bc, uint32_t depth)
{
	// Only nested 'let' values recurse here (the loop flattens everything else),
	// so the guard bounds let-nesting before it can overflow the size pass.
	if (depth >= EXP_MAX_DEPTH) {
		cf_warning(AS_EXP, "build_count_sz - expression nesting exceeds %u",
				EXP_MAX_DEPTH);
		return false;
	}

	while (mp->offset < mp->buf_sz) {
		msgpack_type type = msgpack_peek_type(mp);

		if (type == MSGPACK_TYPE_ERROR) {
			cf_warning(AS_EXP,
					"build_count_sz - invalid instruction at offset %u",
					mp->offset);
			return false;
		}

		bc->counter--;

		uint64_t op_code;
		uint32_t ele_count = 0;

		if (type != MSGPACK_TYPE_LIST) {
			switch (type) {
			case MSGPACK_TYPE_NIL:
				op_code = EXP_VOP_VALUE_NIL;
				break;
			case MSGPACK_TYPE_NEGINT:
			case MSGPACK_TYPE_INT:
				op_code = EXP_VOP_VALUE_INT;
				break;
			case MSGPACK_TYPE_DOUBLE:
				op_code = EXP_VOP_VALUE_FLOAT;
				break;
			case MSGPACK_TYPE_STRING:
				op_code = EXP_VOP_VALUE_STR;
				break;
			case MSGPACK_TYPE_BYTES:
				op_code = EXP_VOP_VALUE_BLOB;
				break;
			case MSGPACK_TYPE_GEOJSON:
				op_code = EXP_VOP_VALUE_GEO;
				break;
			default:
				op_code = EXP_VOP_VALUE_MSGPACK;
				break;
			}

			if (type == MSGPACK_TYPE_BYTES) {
				uint32_t temp_sz;
				const uint8_t* buf = msgpack_get_bin(mp, &temp_sz);

				if (buf == NULL || temp_sz == 0) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid blob at offset %u",
							mp->offset);
					return false;
				}

				if (*buf != AS_BYTES_BLOB && *buf != AS_BYTES_HLL) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid blob type %d at offset %u",
							*buf, mp->offset);
					return false;
				}
			}
			else {
				if (! build_count_literal(bc, mp, type)) {
					return false;
				}
			}
		}
		else {
			if (! msgpack_get_list_ele_count(mp, &ele_count) || ele_count == 0) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			bc->counter += ele_count - 1;

			if (! msgpack_get_uint64(mp, &op_code)) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			if (op_code >= EXP_OP_CODE_END) {
				cf_warning(AS_EXP, "build_count_sz - invalid op_code %lu",
						op_code);
				return false;
			}
		}

		const op_table_entry* entry = &op_table[op_code];

		if (entry->size == 0) {
			cf_warning(AS_EXP, "build_count_sz - invalid op_code %lu size %u",
					op_code, entry->size);
			return false;
		}

		// Static parameters -- special processing.
		switch (op_code) {
		case EXP_BIN:
			if (msgpack_sz(mp) == 0) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}
			// no break
		case EXP_BIN_TYPE:
		case EXP_BIN_EXISTS: {
			uint32_t name_sz;
			const uint8_t* name = msgpack_get_bin(mp, &name_sz);

			if (name == NULL || ! as_bin_name_sz_check(name_sz)) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid bin name at offset %u",
						mp->offset);
				return false;
			}

			// Offsets are relative to the one top-level base, not the
			// repositioned 'let' sub-buffer this recursion may walk.
			exp_bin_name_entry bn = {
				.off = (uint32_t)(name - bc->bin_table.base),
				.sz = name_sz,
				.need_memcpy = as_bin_name_need_memcpy(name, bc->end),
			};

			if (! exp_build_get_or_add_bin_entry(&bc->bin_table, &bn)) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid bin name at offset %u",
						mp->offset);
				return false;
			}

			break;
		}
		case EXP_QUOTE: {
			if (! build_count_literal(bc, mp, MSGPACK_TYPE_MAP)) {
				return false;
			}

			break;
		}
		default:
			if (entry->static_param_count != 0 &&
					msgpack_sz_rep(mp, entry->static_param_count) == 0) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			break;
		}

		bc->counter -= entry->static_param_count;
		bc->total_sz += entry->size;

		// Evaled parameters -- special processing.
		switch (op_code) {
		case EXP_CALL: {
			uint32_t param_count;
			uint64_t call_op_code;

			bc->counter--;

			if (! msgpack_get_list_ele_count(mp, &param_count) ||
					param_count == 0 || ! msgpack_get_uint64(mp, &call_op_code)) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			if (call_op_code == AS_CDT_OP_CONTEXT_EVAL &&
					(param_count != 3 || msgpack_sz(mp) == 0 || // skip context
							! msgpack_get_list_ele_count(mp, &param_count) ||
							! msgpack_get_uint64(mp, &call_op_code))) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}
			else if (call_op_code == AS_CDT_OP_SELECT) {
				uint64_t flags;

				if (param_count < 3 || param_count > 4 ||
						msgpack_peek_type(mp) != MSGPACK_TYPE_LIST ||
						msgpack_sz(mp) == 0 || // skip context
						! msgpack_get_uint64(mp, &flags)) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid instruction at offset %u",
							mp->offset);
					return false;
				}

				if (param_count == 4 && msgpack_sz(mp) == 0) { // mod_exp
					cf_warning(AS_EXP,
							"build_count_sz - invalid instruction at offset %u",
							mp->offset);
					return false;
				}

				param_count = 0; // already parsed
			}

			for (uint32_t i = 1; i < param_count; i++) {
				// TODO - Skip allocating space until we reach the first list.
				// Non lists after the first list will result in over-allocation
				// of op space. May want to improve accounting in the future.
				msgpack_type p_type = msgpack_peek_type(mp);

				if (p_type == MSGPACK_TYPE_LIST) {
					bc->counter += param_count - i;
					break;
				}

				// A bare map before the first list never reaches the
				// instruction loop, so nothing else counts it - and the call
				// parser releases for every non-canonical parameter, wherever
				// it sits.
				if (! build_count_literal(bc, mp, p_type)) {
					return false;
				}
			}

			break;
		}
		case EXP_COND:
			bc->total_sz += (ele_count / 2) * op_table[EXP_VOP_COND_CASE].size;
			break;
		case EXP_LET: {
			if (ele_count % 2 == 1) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid 'let' op at offset %u ele_count %u",
						mp->offset, ele_count);
				return false;
			}

			for (uint32_t i = 0; i < (ele_count - 1) / 2; i++) {
				uint32_t sz;

				bc->counter--;

				if (msgpack_get_bin(mp, &sz) == NULL) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid 'let' var at offset %u",
							mp->offset);
					return false;
				}

				const uint8_t* start = mp->buf + mp->offset;

				msgpack_in mp_var = { .buf = start, .buf_sz = msgpack_sz(mp) };

				if (mp_var.buf_sz == 0) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid msgpack at offset %u",
							mp->offset);
					return false;
				}

				if (! build_count_sz(&mp_var, bc, depth + 1)) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid 'let' value at offset %u",
							mp->offset);
					return false;
				}
			}

			break;
		}
		default:
			break;
		}

		switch (op_code) {
		case EXP_CMP_REGEX:
		case EXP_VOP_VALUE_GEO:
			bc->cleanup_count++;
			break;
		default:
			break;
		}
	}

	return true;
}

static var_entry*
build_find_var_entry(build_args* args, const uint8_t* name, uint32_t name_sz)
{
	for (var_scope* cur = args->current; cur != NULL; cur = cur->parent) {
		for (uint32_t i = 0; i < cur->n_entries; i++) {
			if (cur->entries[i].name_sz != name_sz) {
				continue;
			}

			if (memcmp(cur->entries[i].name, name, name_sz) == 0) {
				return &cur->entries[i];
			}
		}
	}

	return NULL;
}

uint32_t
exp_build_find_bin_idx(const build_bin_table* t, bin_name128 name128)
{
	for (uint32_t i = 0; i < t->n_bins; i++) {
		define_bin_name128(e, bin_table_get_name(t, i), t->table[i].sz,
				t->table[i].need_memcpy);

		if (name128.name128 == e.name128) {
			return i;
		}
	}

	return t->n_bins;
}

bool
exp_build_get_or_add_bin_entry(build_bin_table* t, const exp_bin_name_entry* n)
{
	define_bin_name128(name128, t->base + n->off, n->sz, n->need_memcpy);

	if (! as_bin_name128_check(name128, n->sz)) {
		return false;
	}

	if (exp_build_find_bin_idx(t, name128) == t->n_bins) {
		if (t->n_bins == RECORD_MAX_BINS) {
			return false; // never more distinct bins than a record can hold
		}

		t->table[t->n_bins++] = *n;
	}

	return true;
}

// literal_type says what counts as a literal here rather than describing the
// element - a quoted payload is one whatever it holds, so that caller names
// the map type outright.
//
// post: mp has stepped past the element, literal or not.
static bool
build_count_literal(build_counts* bc, msgpack_in* mp, msgpack_type literal_type)
{
	const uint8_t* buf = mp->buf + mp->offset;
	uint32_t buf_sz = msgpack_sz(mp);

	if (buf_sz == 0) {
		cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
				mp->offset);
		return false;
	}

	if (literal_type != MSGPACK_TYPE_MAP) {
		return true;
	}

	bool marker = false;

	switch (build_check_cdt_literal(buf, buf_sz, &marker)) {
	case CDT_LITERAL_INVALID:
		return false;
	case CDT_LITERAL_NEEDS_REWRITE:
		bc->literal_cleanup++;
		break;
	default:
		break;
	}

	bc->has_marker_literal |= marker;

	return true;
}

// Unsorted keys and wide headers are legal client input, not faults. A literal
// can be a compare parameter, so markers pass too - keeping those out of a
// particle is the storage-bound consumer's job.
//
// post: *marker_r on OK and NEEDS_REWRITE alike - a literal can be both.
static cdt_literal_status
build_check_cdt_literal(const uint8_t* buf, uint32_t buf_sz, bool* marker_r)
{
	// toplvl_index false strips a top-level persist-index flag - set it to
	// support PERSIST_INDEX in exp literals.
	define_untrusted_info(info, buf, buf_sz, .allow_nonstorage = true);

	if (! cdt_untrusted_check(&info)) {
		cf_warning(AS_EXP, "build_check_cdt_literal - error %u %s",
				AS_ERR_PARAMETER, cdt_untrusted_err_msg(info.err));
		return CDT_LITERAL_INVALID;
	}

	*marker_r = info.has_nonstorage;

	return info.is_canonical ? CDT_LITERAL_OK : CDT_LITERAL_NEEDS_REWRITE;
}

// Neither counter says anything about THIS op - together they mean only that
// some literal here needs the walk, so an expression with neither skips it.
// The two passes must agree, which relies on the source bytes being immutable
// between them - guaranteed because nothing writes parse-source bytes after
// demarshal. Do not "optimize" this into an in-place rewrite: batch repeat
// sub-transactions share one msgp, so writing the source races sibling
// sub-transactions' reads.
static bool
build_canonicalize_cdt(build_args* args, op_value_blob* op)
{
	if (args->literal_cleanup == 0 && ! args->has_marker_literal) {
		return true;
	}

	define_untrusted_info(info, op->value, op->value_sz,
			.allow_nonstorage = true);

	// The sizing pass cleared these exact bytes.
	bool ok = cdt_untrusted_check(&info);

	cf_assert(ok, AS_EXP, "cdt literal check disagrees with the sizing pass");

	// A marker survives the rewrite, so this is set either way.
	op->has_nonstorage = info.has_nonstorage;

	if (info.is_canonical) {
		return true; // keep aliasing the source
	}

	uint8_t* mem = cf_malloc(info.sz);

	// Not an invariant: an unsorted literal's duplicate keys are only adjacent
	// once sorted, so the check leaves them for this walk to find.
	if (! cdt_untrusted_rewrite(mem, &info)) {
		cf_warning(AS_EXP, "build_canonicalize_cdt - error %u %s",
				AS_ERR_PARAMETER, cdt_untrusted_err_msg(info.err));
		cf_free(mem);
		return false;
	}

	// Retain the canonicalized copy - the op owns it and as_exp_destroy()
	// frees it via the cleanup stack. The rewrite only strips and compacts, so
	// value_sz can only shrink.
	op->value = mem;
	op->value_sz = info.sz;
	args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = op;
	args->literal_cleanup--;

	return true;
}

static bool
build_default(build_args* args)
{
	return build_args_setup(args, "build_default");
}

static bool
build_meta_default(build_args* args)
{
	args->exp->flags |= AS_EXP_HAS_NON_DIGEST_META;

	return build_args_setup(args, "build_meta_default");
}

static bool
build_compare(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	if (! build_args_setup(args, "build_compare")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype ltype = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	if (ltype != rtype) {
		cf_warning(AS_EXP,
				"build_compare - error %u mismatched arg types ltype %u (%s) rtype %u (%s)",
				AS_ERR_PARAMETER, ltype, exp_rtype_to_str(ltype), rtype,
				exp_rtype_to_str(rtype));
		return false;
	}

	switch (ltype) {
	case EXP_RTYPE_GEOJSON:
	case EXP_RTYPE_HLL:
		cf_warning(AS_EXP,
				"build_compare - error %u cannot compare arg type %u (%s)",
				AS_ERR_PARAMETER, ltype, exp_rtype_to_str(ltype));
		return false;
	default:
		break;
	}

	op->rtype = ltype;
	args->entry = entry;

	return true;
}

static bool
build_cmp_regex(build_args* args)
{
	as_info_warn_deprecated(
			"'cmp_regex' expression op is deprecated - use the strings expression API (regex_compare) instead");

	op_cmp_regex* op = (op_cmp_regex*)args->mem;
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_cmp_regex")) {
		return false;
	}

	int64_t regex_options;

	if (! msgpack_get_int64(&args->mp, &regex_options)) {
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid regex options",
				AS_ERR_PARAMETER);
		return false;
	}

	uint32_t regex_str_sz;
	const uint8_t* regex_str = msgpack_get_bin(&args->mp, &regex_str_sz);

	if (regex_str == NULL) {
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid regex string",
				AS_ERR_PARAMETER);
		return false;
	}

	int rv;

	if (! build_next(args)) {
		return false;
	}

	if (args->entry->r_type != EXP_RTYPE_STR) {
		cf_warning(AS_EXP,
				"build_cmp_regex - error %u invalid arg type %u (%s) != %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type), EXP_RTYPE_STR,
				exp_rtype_str[EXP_RTYPE_STR]);
		return false;
	}

	if (regex_str_sz == 0 || regex_str[regex_str_sz - 1] != '\0') {
		define_deferred_memory(temp, regex_str_sz + 1);

		memcpy(temp, regex_str, regex_str_sz);
		temp[regex_str_sz] = '\0';
		rv = regcomp(&op->regex, (const char*)temp, (int)regex_options);
	}
	else {
		rv = regcomp(&op->regex, (const char*)regex_str, (int)regex_options);
	}

	if (rv != 0) {
		char errbuf[1024];

		regerror(rv, &op->regex, errbuf, sizeof(errbuf));
		cf_warning(AS_EXP, "build_cmp_regex - error %u regex compile %s",
				AS_ERR_PARAMETER, errbuf);

		return false;
	}

	op->regex_str_sz = regex_str_sz;
	op->flags = (int32_t)regex_options;

	args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = op;
	args->entry = entry;

	return true;
}

static bool
build_cmp_geo(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	if (! build_args_setup(args, "build_cmp_geo")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype ltype = args->entry->r_type;

	if (ltype != EXP_RTYPE_GEOJSON) {
		cf_warning(AS_EXP,
				"build_cmp_geo - error %u mismatched arg types ltype %u (%s) != %u (%s)",
				AS_ERR_PARAMETER, ltype, exp_rtype_to_str(ltype),
				EXP_RTYPE_GEOJSON, exp_rtype_str[EXP_RTYPE_GEOJSON]);
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	if (ltype != rtype) {
		cf_warning(AS_EXP,
				"build_cmp_geo - error %u mismatched arg types ltype %u (%s) rtype %u (%s)",
				AS_ERR_PARAMETER, ltype, exp_rtype_to_str(ltype), rtype,
				exp_rtype_to_str(rtype));
		return false;
	}

	op->rtype = ltype;
	args->entry = entry;

	return true;
}

static bool
build_in_list(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_in_list")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	if (rtype != EXP_RTYPE_LIST) {
		cf_warning(AS_EXP, "build_in_list - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, rtype, exp_rtype_to_str(rtype));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_logical_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 2) {
		cf_warning(AS_EXP, "build_logical - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != EXP_RTYPE_TRILEAN) {
			cf_warning(AS_EXP, "build_logical - error %u invalid type at arg %u",
					AS_ERR_PARAMETER, i + 1);
			return false;
		}
	}

	args->entry = entry;

	return true;
}

static bool
build_logical_not(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_logical_not")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	if (args->entry->r_type != EXP_RTYPE_TRILEAN) {
		cf_warning(AS_EXP,
				"build_logical_not - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 1) {
		cf_warning(AS_EXP, "build_math - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;
	exp_rtype expected_type = EXP_RTYPE_END;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		switch (args->entry->r_type) {
		case EXP_RTYPE_INT:
		case EXP_RTYPE_FLOAT:
			if (expected_type == EXP_RTYPE_END) {
				expected_type = args->entry->r_type;
				break;
			}
			else if (expected_type == args->entry->r_type) {
				break;
			}

			cf_warning(AS_EXP, "build_math - error %u mixed types at arg %u",
					AS_ERR_PARAMETER, i + 1);

			return false;
		default:
			cf_warning(AS_EXP,
					"build_math - error %u invalid type %u (%s) at arg %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type), i + 1);
			return false;
		}
	}

	args->entry =
			&op_table[expected_type == EXP_RTYPE_FLOAT ? EXP_VOP_VALUE_FLOAT
													   : EXP_VOP_VALUE_INT];

	return true;
}

static bool
build_device_size(build_args* args)
{
	as_info_warn_deprecated(
			"'device_size' expression is deprecated - use 'record_size' instead");
	return build_meta_default(args);
}

static bool
build_memory_size(build_args* args)
{
	as_info_warn_deprecated(
			"'memory_size' expression is deprecated - use 'record_size' instead");
	return build_meta_default(args);
}

static bool
build_math_pow(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_pow")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg1 = args->entry->r_type;

	if (! (arg0 == EXP_RTYPE_FLOAT && arg1 == EXP_RTYPE_FLOAT)) {
		cf_warning(AS_EXP,
				"build_math_pow - error %u args are not numeric or different types - base %u (%s) exponent %u (%s)",
				AS_ERR_PARAMETER, arg0, exp_rtype_to_str(arg0), arg1,
				exp_rtype_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_log(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_log")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg1 = args->entry->r_type;

	if (! (arg0 == EXP_RTYPE_FLOAT && arg1 == EXP_RTYPE_FLOAT)) {
		cf_warning(AS_EXP,
				"build_math_log - error %u args are not numeric or different types - value %u (%s) base %u (%s)",
				AS_ERR_PARAMETER, arg0, exp_rtype_to_str(arg0), arg1,
				exp_rtype_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_mod(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_mod")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg1 = args->entry->r_type;

	if (! (arg0 == EXP_RTYPE_INT && arg1 == EXP_RTYPE_INT)) {
		cf_warning(AS_EXP,
				"build_math_mod - error %u args are not integers - numerator %u (%s) denominator %u (%s)",
				AS_ERR_PARAMETER, arg0, exp_rtype_to_str(arg0), arg1,
				exp_rtype_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_number_op(build_args* args)
{
	if (! build_args_setup(args, "build_number_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	switch (rtype) {
	case EXP_RTYPE_FLOAT:
		args->entry = &op_table[EXP_VOP_VALUE_FLOAT];
		break;
	case EXP_RTYPE_INT:
		args->entry = &op_table[EXP_VOP_VALUE_INT];
		break;
	default:
		cf_warning(AS_EXP, "build_number_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type));
		return false;
	}

	return true;
}

static bool
build_float_op(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_float_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	if (rtype != EXP_RTYPE_FLOAT) {
		cf_warning(AS_EXP, "build_float_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_op(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	if (rtype != EXP_RTYPE_INT) {
		cf_warning(AS_EXP, "build_int_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_to_string_op(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_to_string_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype rtype = args->entry->r_type;

	// as_bin_to_string handles INT / FLOAT / STR / bool / BLOB.
	if (rtype != EXP_RTYPE_INT && rtype != EXP_RTYPE_FLOAT &&
			rtype != EXP_RTYPE_STR && rtype != EXP_RTYPE_TRILEAN &&
			rtype != EXP_RTYPE_BLOB) {
		cf_warning(AS_EXP,
				"build_to_string_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, rtype, exp_rtype_to_str(rtype));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 1) {
		cf_warning(AS_EXP, "build_int_vargs - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != EXP_RTYPE_INT) {
			cf_warning(AS_EXP,
					"build_int_vargs - error %u invalid type %u (%s) at arg %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type), i + 1);
			return false;
		}
	}

	args->entry = entry;

	return true;
}

static bool
build_int_one(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_one")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (arg0 != EXP_RTYPE_INT) {
		cf_warning(AS_EXP,
				"build_int_one - error %u arg type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg0, exp_rtype_to_str(arg0), EXP_RTYPE_INT,
				exp_rtype_str[EXP_RTYPE_INT]);
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_shift(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_shift")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg1 = args->entry->r_type;

	if (! (arg0 == EXP_RTYPE_INT && arg1 == EXP_RTYPE_INT)) {
		cf_warning(AS_EXP,
				"build_int_shift - error %u all args are type %u (%s) - arg0 %u (%s) arg1 %u (%s)",
				AS_ERR_PARAMETER, EXP_RTYPE_INT, exp_rtype_str[EXP_RTYPE_INT],
				arg0, exp_rtype_to_str(arg0), arg1, exp_rtype_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_scan(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_scan")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	exp_rtype arg1 = args->entry->r_type;

	if (arg0 != EXP_RTYPE_INT) {
		cf_warning(AS_EXP,
				"build_int_scan - error %u arg0 type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg0, exp_rtype_to_str(arg0), EXP_RTYPE_INT,
				exp_rtype_str[EXP_RTYPE_INT]);
		return false;
	}

	if (arg1 != EXP_RTYPE_TRILEAN) {
		cf_warning(AS_EXP,
				"build_int_scan - error %u arg1 type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg1, exp_rtype_to_str(arg1),
				EXP_RTYPE_TRILEAN, exp_rtype_str[EXP_RTYPE_TRILEAN]);
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_meta_digest_mod(build_args* args)
{
	op_meta_digest_modulo* op = (op_meta_digest_modulo*)args->mem;

	if (! build_args_setup(args, "build_rec_digest_modulo")) {
		return false;
	}

	int64_t mod64;

	if (! msgpack_get_int64(&args->mp, &mod64)) {
		cf_warning(AS_EXP,
				"build_rec_digest_modulo - error %u failed to parse an integer",
				AS_ERR_PARAMETER);
		return false;
	}

	op->mod = (int32_t)mod64;

	if (op->mod == 0) {
		cf_warning(AS_EXP,
				"build_rec_digest_modulo - error %u cannot modulo by zero",
				AS_ERR_PARAMETER);
		return false;
	}

	args->exp->flags |= AS_EXP_HAS_DIGEST_MOD;

	return true;
}

static bool
build_rec_key(build_args* args)
{
	op_base_mem* op = (op_base_mem*)args->mem;

	if (! build_args_setup(args, "build_rec_key")) {
		return false;
	}

	uint64_t type64;

	if (! msgpack_get_uint64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_rec_key - error %u failed to parse an integer",
				AS_ERR_PARAMETER);
		return false;
	}

	switch ((exp_rtype)type64) {
	case EXP_RTYPE_INT:
		op->type = RT_INT;
		break;
	case EXP_RTYPE_STR:
		op->type = RT_STR;
		break;
	case EXP_RTYPE_BLOB:
		op->type = RT_BLOB;
		break;
	default:
		cf_warning(AS_EXP, "build_rec_key - error %u invalid exp_rtype %lu (%s)",
				AS_ERR_PARAMETER, type64, exp_rtype_to_str(type64));
		return false;
	}

	args->entry = exp_build_get_entry((exp_rtype)type64);

	args->exp->flags |= AS_EXP_HAS_REC_KEY;

	return true;
}

static bool
build_bin(build_args* args)
{
	op_var* op = (op_var*)args->mem;

	if (! build_args_setup(args, "build_bin")) {
		return false;
	}

	uint64_t type64;

	if (! msgpack_get_uint64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_bin - error %u failed to parse an integer",
				AS_ERR_PARAMETER);
		return false;
	}

	if (type64 >= EXP_RTYPE_INPUT_END) {
		cf_warning(AS_EXP, "build_bin - error %u invalid type %lu",
				AS_ERR_PARAMETER, type64);
		return false;
	}

	op->base.rtype = (exp_rtype)type64;

	uint32_t name_sz;
	const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

	cf_assert(name != NULL, AS_EXP, "build_bin - name prechecked");

	define_bin_name128(name128, name, name_sz,
			as_bin_name_need_memcpy(name, args->mp.buf + args->mp.buf_sz));
	uint32_t idx = exp_build_find_bin_idx(&args->bin_table, name128);

	cf_assert(idx < args->bin_table.n_bins, AS_EXP,
			"build_bin - unexpected error bin name %.*s", name_sz, name);

	op->idx = idx;

	if (args->bins_info_r != NULL) {
		as_bin_info bin_info;

		memcpy(bin_info.name, &name128, sizeof(name128));
		bin_info.type = exp_rtype_to_particle_type(op->base.rtype);
		cf_vector_append(args->bins_info_r, &bin_info);
	}

	if ((args->entry = exp_build_get_entry(op->base.rtype)) == NULL) {
		cf_warning(AS_EXP, "build_bin - error %u invalid exp_rtype %d (%s)",
				AS_ERR_PARAMETER, op->base.rtype,
				exp_rtype_to_str(op->base.rtype));
		return false;
	}

	return true;
}

static bool
build_bin_meta(build_args* args)
{
	op_var* op = (op_var*)args->mem;
	// INT for bin_type, TRILEAN for bin_exists.
	exp_rtype r_type = args->entry->r_type;

	if (! build_args_setup(args, "build_bin_meta")) {
		return false;
	}

	uint32_t name_sz;
	const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

	cf_assert(name != NULL, AS_EXP, "build_bin_meta - name prechecked");

	define_bin_name128(name128, name, name_sz,
			as_bin_name_need_memcpy(name, args->mp.buf + args->mp.buf_sz));
	uint32_t idx = exp_build_find_bin_idx(&args->bin_table, name128);

	cf_assert(idx < args->bin_table.n_bins, AS_EXP,
			"build_bin_meta - unexpected error bin name %.*s", name_sz, name);

	op->idx = idx;
	op->base.rtype = r_type;

	if (args->bins_info_r != NULL) {
		as_bin_info bin_info;

		memcpy(bin_info.name, &name128, sizeof(name128));
		// Wildcard, we depend on bins with this name with *all* types.
		bin_info.type = AS_PARTICLE_TYPE_NULL;
		cf_vector_append(args->bins_info_r, &bin_info);
	}

	return true;
}

static bool
build_cond(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_cond* op = (op_cond*)args->mem;

	op->base.code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 3) {
		cf_warning(AS_EXP, "build_cond - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	if (args->ele_count % 2 == 0) {
		cf_warning(AS_EXP, "build_cond - error %u requires default case",
				AS_ERR_PARAMETER);
		return false;
	}

	uint32_t ele_count = args->ele_count;
	exp_rtype type = EXP_RTYPE_END;

	op->case_count = ele_count / 2;

	for (uint32_t i = 0; i < op->case_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != EXP_RTYPE_TRILEAN) {
			cf_warning(AS_EXP,
					"build_cond - error %u invalid type %u (%s) at condition %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type), i + 1);
			return false;
		}

		op_base_mem* op_case = (op_base_mem*)args->mem;

		op_case->code = EXP_VOP_COND_CASE;
		args->instr_ix++;
		args->mem += op_table[EXP_VOP_COND_CASE].size;

		if (! build_next(args)) {
			return false;
		}

		// Allow returning AS_EXP_UNK and any other return type.
		if (args->entry->code != EXP_UNK) {
			if (type == EXP_RTYPE_END) {
				type = args->entry->r_type;
			}

			if (args->entry->r_type != type) {
				cf_warning(AS_EXP,
						"build_cond - error %u mismatched arg type %d (%s) expected type %d (%s) at condition %u",
						AS_ERR_PARAMETER, args->entry->r_type,
						exp_rtype_to_str(args->entry->r_type), type,
						exp_rtype_to_str(type), i + 1);
				return false;
			}
		}

		op_case->instr_end_ix = args->instr_ix;
	}

	if (! build_next(args)) {
		return false;
	}

	if (type == EXP_RTYPE_END) {
		type = args->entry->r_type;
	}
	else if (args->entry->code != EXP_UNK && args->entry->r_type != type) {
		cf_warning(AS_EXP,
				"build_cond - error %u mismatched type %d (%s) expected type %d (%s) at default condition",
				AS_ERR_PARAMETER, args->entry->r_type,
				exp_rtype_to_str(args->entry->r_type), type,
				exp_rtype_to_str(type));
		return false;
	}

	args->entry = exp_build_get_entry(type);
	cf_assert(args->entry != NULL, AS_EXP, "unexpected type %d", type);

	return true;
}

static bool
build_var_builtin(build_args* args)
{
	op_var* op = (op_var*)args->mem;

	if (! build_args_setup(args, "build_var_builtin")) {
		return false;
	}

	uint64_t type64;
	uint64_t idx64;

	if (! msgpack_get_uint64(&args->mp, &type64)) {
		cf_warning(AS_EXP,
				"build_var_builtin - error %u failed to parse an integer",
				AS_ERR_PARAMETER);
		return false;
	}

	op->base.rtype = (exp_rtype)type64;

	if (! msgpack_get_uint64(&args->mp, &idx64)) {
		cf_warning(AS_EXP,
				"build_var_builtin - error %u failed to parse an integer at offset %u",
				AS_ERR_PARAMETER, args->mp.offset);
		return false;
	}

	if (idx64 >= AS_EXP_BUILTIN_COUNT) {
		cf_warning(AS_EXP,
				"build_var_builtin - error %u invalid builtin var %lu at offset %u",
				AS_ERR_PARAMETER, idx64, args->mp.offset);
		return false;
	}
	else if (idx64 == AS_EXP_BUILTIN_INDEX && op->base.rtype != EXP_RTYPE_INT) {
		cf_warning(AS_EXP,
				"build_var_builtin - error %u invalid builtin var type %ld for index is not integer at offset %u",
				AS_ERR_PARAMETER, idx64, args->mp.offset);
		return false;
	}

	op->idx = (uint32_t)idx64;

	if ((args->entry = exp_build_get_entry(op->base.rtype)) == NULL) {
		cf_warning(AS_EXP,
				"build_var_builtin - error %u invalid exp_rtype %d (%s)",
				AS_ERR_PARAMETER, op->base.rtype,
				exp_rtype_to_str(op->base.rtype));
		return false;
	}

	return true;
}

static bool
build_var(build_args* args)
{
	op_var* op = (op_var*)args->mem;

	if (! build_args_setup(args, "build_var")) {
		return false;
	}

	uint32_t name_sz;
	const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

	if (name == NULL) {
		cf_warning(AS_EXP,
				"build_var - error %u failed to parse a string at offset %u",
				AS_ERR_PARAMETER, args->mp.offset);
		return false;
	}

	var_entry* entry = build_find_var_entry(args, name, name_sz);

	if (entry == NULL) {
		cf_warning(AS_EXP,
				"build_var - error %u undefined var name %.*s at offset %u",
				AS_ERR_PARAMETER, name_sz, name, args->mp.offset);
		return false;
	}

	op->idx = entry->idx;
	op->base.rtype = entry->r_type;

	switch (op->base.rtype) {
	case EXP_RTYPE_END:
		cf_warning(AS_EXP,
				"build_var - error %u using var name %.*s while defining it at offset %u",
				AS_ERR_PARAMETER, name_sz, name, args->mp.offset);
		return false;
	case EXP_RTYPE_NIL:
		args->entry = &op_table[EXP_VOP_VALUE_NIL];
		break;
	case EXP_RTYPE_TRILEAN:
		args->entry = &op_table[EXP_VOP_VALUE_TRILEAN];
		break;
	default:
		if ((args->entry = exp_build_get_entry(op->base.rtype)) == NULL) {
			cf_warning(AS_EXP,
					"build_var - error %u var name %.*s unknown entry type %d (%s)",
					AS_ERR_PARAMETER, name_sz, name, op->base.rtype,
					exp_rtype_to_str(op->base.rtype));
			return false;
		}
	}

	return true;
}

static bool
build_let(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_let* op = (op_let*)args->mem;

	op->base.code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 3) {
		cf_warning(AS_EXP, "build_let - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	if (args->ele_count % 2 == 0) {
		cf_warning(AS_EXP, "build_let - error %u invalid arg count %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t n_vars = args->ele_count / 2;
	define_deferred_array(entries, var_entry, n_vars);
	var_scope scope = {
		.parent = args->current,
		.n_entries = 0,
		.entries = entries,
	};

	args->current = &scope;
	op->var_idx = args->var_idx;
	op->n_vars = n_vars;

	for (uint32_t i = 0; i < n_vars; i++) {
		uint32_t name_sz;
		const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

		if (name == NULL) {
			cf_warning(AS_EXP,
					"build_let - error %u failed to parse blob - var at %u",
					AS_ERR_PARAMETER, i);
			return false;
		}

		if (name_sz == 0) {
			cf_warning(AS_EXP, "build_let - error %u variable name length is 0.",
					AS_ERR_PARAMETER);
			return false;
		}

		if (name[0] != '_' && isalpha(name[0]) == 0) {
			cf_warning(AS_EXP,
					"build_let - error %u illegal variable name '%.*s' at %u - must begin with an alpha or underscore",
					AS_ERR_PARAMETER, name_sz, name, i);
			return false;
		}

		for (uint32_t j = 1; j < name_sz; j++) {
			if (name[j] != '_' && isalnum(name[j]) == 0) {
				cf_warning(AS_EXP,
						"build_let - error %u illegal variable name '%.*s' at %u - must contain only alpha, digits or underscore",
						AS_ERR_PARAMETER, name_sz, name, i);
				return false;
			}
		}

		if (build_find_var_entry(args, name, name_sz) != NULL) {
			cf_warning(AS_EXP,
					"build_let - error %u duplicate var name '%.*s' at %u",
					AS_ERR_PARAMETER, name_sz, name, i);
			return false;
		}

		scope.entries[i].name = name;
		scope.entries[i].name_sz = name_sz;
		scope.entries[i].idx = args->var_idx++;
		scope.n_entries++;
		scope.entries[i].r_type = EXP_RTYPE_END;

		if (! build_next(args)) {
			return false;
		}

		scope.entries[i].r_type = args->entry->r_type;
	}

	if (! build_next(args)) {
		return false;
	}

	op->base.rtype = args->entry->r_type;

	if (args->max_var_idx < args->var_idx) {
		args->max_var_idx = args->var_idx;
	}

	args->current = scope.parent;
	args->var_idx -= scope.n_entries;

	return true;
}

static bool
build_quote(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_quote")) {
		return false;
	}

	if (msgpack_peek_type(&args->mp) != MSGPACK_TYPE_LIST ||
			(op->value = msgpack_get_ele(&args->mp, &op->value_sz)) == NULL) {
		cf_warning(AS_EXP, "build_quote - error %u invalid list arg",
				AS_ERR_PARAMETER);
		return false;
	}

	op->base.code = EXP_VOP_VALUE_LIST;

	if (! build_canonicalize_cdt(args, op)) {
		return false;
	}

	return true;
}

// A CONTEXT_EVAL leader means the call navigates a path, so its receiver is the
// container being walked rather than the family's own leaf type.
//
// The wire decode already reads this leader, and this deliberately reads it
// again rather than having that carry the answer out: the decode is shared by
// every call family and runs per op, while only the string family has a pathed
// form and only build asks. Two header reads keep the build-time check stating
// its own precondition.
static bool
op_call_has_ctx(const op_call* op)
{
	msgpack_in mp = { .buf = op->vecs[0].buf, .buf_sz = op->vecs[0].buf_sz };
	uint32_t ele_count;
	uint64_t op_code;

	return msgpack_get_list_ele_count(&mp, &ele_count) &&
			msgpack_get_uint64(&mp, &op_code) &&
			op_code == AS_CDT_OP_CONTEXT_EVAL;
}

static bool
build_call(build_args* args)
{
	op_call* op = (op_call*)args->mem;

	if (! build_args_setup(args, "build_call")) {
		return false;
	}

	uint64_t type64;

	if (! msgpack_get_uint64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_call - error %u invalid exp_rtype arg",
				AS_ERR_PARAMETER);
		return false;
	}

	if (type64 >= EXP_RTYPE_INPUT_END) {
		cf_warning(AS_EXP, "build_call - error %u invalid type %lu",
				AS_ERR_PARAMETER, type64);
		return false;
	}

	uint64_t system_type64;

	if (! msgpack_get_uint64(&args->mp, &system_type64)) {
		cf_warning(AS_EXP, "build_call - error %u invalid system_type arg",
				AS_ERR_PARAMETER);
		return false;
	}

	// system_type is a base exp_call_stype ([0, EXP_CALL_END)) optionally OR'd
	// with EXP_CALL_FLAG_MODIFY_LOCAL. Reject client wire outside that set.
	if ((system_type64 & ~(uint64_t)EXP_CALL_FLAG_MODIFY_LOCAL) >= EXP_CALL_END) {
		cf_warning(AS_EXP, "build_call - error %u invalid system_type %lu",
				AS_ERR_PARAMETER, system_type64);
		return false;
	}

	op->type = (exp_rtype)type64;
	op->system_type = (exp_call_stype)system_type64;

	if (! parse_op_call(op, args)) {
		cf_warning(AS_EXP, "build_call - error %u invalid msgpack list",
				AS_ERR_PARAMETER);
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	if (args->entry->r_type == EXP_RTYPE_NIL) {
		return true;
	}

	switch (op->system_type & (uint32_t)~EXP_CALL_FLAG_MODIFY_LOCAL) {
	case EXP_CALL_CDT:
		if (args->entry->r_type != EXP_RTYPE_LIST &&
				args->entry->r_type != EXP_RTYPE_MAP) {
			cf_warning(AS_EXP,
					"build_call - error %u arg %u (%s) is not list or map",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type));
			return false;
		}
		break;
	case EXP_CALL_HLL:
		if (args->entry->r_type != EXP_RTYPE_HLL) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not hll",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type));
			return false;
		}
		break;
	case EXP_CALL_STRING:
		// A pathed string call receives the container the ctx walks; the leaf
		// it lands on is what must be a string, and only the walk can know
		// that.
		if (op_call_has_ctx(op)) {
			if (args->entry->r_type != EXP_RTYPE_LIST &&
					args->entry->r_type != EXP_RTYPE_MAP) {
				cf_warning(AS_EXP,
						"build_call - error %u arg %u (%s) is not list or map",
						AS_ERR_PARAMETER, args->entry->r_type,
						exp_rtype_to_str(args->entry->r_type));
				return false;
			}
		}
		else if (args->entry->r_type != EXP_RTYPE_STR) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not string",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type));
			return false;
		}
		break;
	case EXP_CALL_BITS:
		if (args->entry->r_type != EXP_RTYPE_BLOB) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not blob",
					AS_ERR_PARAMETER, args->entry->r_type,
					exp_rtype_to_str(args->entry->r_type));
			return false;
		}
		break;
	default:
		cf_warning(AS_EXP, "build_call - error %u invalid system %d",
				AS_ERR_PARAMETER, op->system_type);
		return false;
	}

	if ((args->entry = exp_build_get_entry(op->type)) == NULL) {
		cf_warning(AS_EXP, "build_call - error %u invalid exp_rtype %d (%s)",
				AS_ERR_PARAMETER, op->type, exp_rtype_to_str(op->type));
		return false;
	}

	return true;
}

static bool
build_ael_compile(build_args* args)
{
	cf_warning(AS_EXP, "build_ael_compile - error %u only allowed at top level",
			AS_ERR_PARAMETER);
	return false;
}

static bool
build_value_nil(build_args* args)
{
	if (! build_args_setup(args, "build_value_nil")) {
		return false;
	}

	if (msgpack_sz(&args->mp) == 0) {
		cf_warning(AS_EXP, "build_value_nil - error %u failed to parse nil",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_bool(build_args* args)
{
	op_value_bool* op = (op_value_bool*)args->mem;

	if (! build_args_setup(args, "build_value_bool")) {
		return false;
	}

	if (! msgpack_get_bool(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_bool - error %u failed to parse bool",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_int(build_args* args)
{
	op_value_int* op = (op_value_int*)args->mem;

	if (! build_args_setup(args, "build_value_int")) {
		return false;
	}

	if (! msgpack_get_int64(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_int - error %u failed to parse integer",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_float(build_args* args)
{
	op_value_float* op = (op_value_float*)args->mem;

	if (! build_args_setup(args, "build_value_float")) {
		return false;
	}

	if (! msgpack_get_double(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_float - error %u failed to parse float",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_blob(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_value_blob")) {
		return false;
	}

	op->value = msgpack_get_bin(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP, "build_value_blob - error %u failed to parse bin",
				AS_ERR_PARAMETER);
		return false;
	}

	cf_assert(op->value_sz != 0, AS_EXP, "unexpected");

	as_bytes_type type = *op->value++;
	op->value_sz--;

	switch (type) {
	case AS_BYTES_BLOB:
		return true; // already set
	case AS_BYTES_STRING:
		op->base.code = EXP_VOP_VALUE_STR;
		break;
	case AS_BYTES_HLL:
		op->base.code = EXP_VOP_VALUE_HLL;
		break;
	default:
		cf_warning(AS_EXP, "unexpected blob type %u", type);
		return false;
	}

	args->entry = &op_table[op->base.code];

	return true;
}

static bool
build_value_geo(build_args* args)
{
	op_value_geo* op = (op_value_geo*)args->mem;

	if (! build_args_setup(args, "build_value_geo")) {
		return false;
	}

	if (! exp_geo_mp_to_op(&args->mp, op, "build_value_geo")) {
		return false;
	}

	if (op->compiled.type == GEO_REGION) {
		args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = op;
	}

	return true;
}

static bool
build_value_msgpack(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_value_msgpack")) {
		return false;
	}

	msgpack_type type = msgpack_peek_type(&args->mp);

	op->value = msgpack_get_ele(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP,
				"build_value_msgpack - error %u failed to parse element from type %u",
				AS_ERR_PARAMETER, type);
		return false;
	}

	if (type == MSGPACK_TYPE_MAP) {
		// Result is MAP-typed (entry carries r_type = EXP_RTYPE_MAP);
		// the op's `code` field stays MSGPACK so exp_eval_value handles
		// it uniformly with list literals (no VALUE_MAP runtime arm).
		if (! build_canonicalize_cdt(args, op)) {
			return false;
		}

		args->entry = &op_table[EXP_VOP_VALUE_MAP];
	}
	else if (type == MSGPACK_TYPE_BYTES) {
		cf_warning(AS_EXP,
				"build_value_msgpack - error %u unexpected msgpack blob",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_map_kv(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_map_kv")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	args->entry = entry;

	return true;
}

//==========================================================
// Local helpers - build utilities.
//

static bool
parse_op_call(op_call* op, build_args* args)
{
	msgpack_in* mp = &args->mp;
	uint32_t ele_count;
	uint32_t offset_start = mp->offset;

	op->eval_count = 0;
	op->vecs[0].buf = mp->buf + mp->offset;

	if (! msgpack_get_list_ele_count(mp, &ele_count)) {
		return false;
	}

	uint64_t op_code;

	if (ele_count == 0 || ! msgpack_get_uint64(mp, &op_code)) {
		return false;
	}

	op->vecs[0].buf_sz = mp->offset - offset_start;

	uint32_t base_stype = op->system_type & (uint32_t)~EXP_CALL_FLAG_MODIFY_LOCAL;

	if (op_code == AS_CDT_OP_CONTEXT_EVAL) {
		// CONTEXT_EVAL (0xFF) is a sentinel shared only by the CDT and STRING
		// call families (STRING mirrors it via AS_STRING_OP_CONTEXT_EVAL); a
		// leader this high under BITS / HLL is malformed wire.
		if (base_stype != EXP_CALL_CDT && base_stype != EXP_CALL_STRING) {
			return false;
		}

		if (ele_count != 3 || msgpack_sz(mp) == 0) { // skip context
			return false;
		}

		if (! msgpack_get_list_ele_count(mp, &ele_count)) {
			return false;
		}

		if (! msgpack_get_uint64(mp, &op_code)) {
			return false;
		}

		op->vecs[0].buf_sz = mp->offset - offset_start;
	}
	else if (op_code == AS_CDT_OP_SELECT) {
		uint64_t flags;

		// SELECT (0xFE) is a CDT-only sentinel.
		if (base_stype != EXP_CALL_CDT) {
			return false;
		}

		if (ele_count < 3 || ele_count > 4 ||
				msgpack_peek_type(mp) != MSGPACK_TYPE_LIST || // ctx
				msgpack_sz(mp) == 0 || ! msgpack_get_uint64(mp, &flags)) {
			return false;
		}

		if (ele_count == 4 && msgpack_sz(mp) == 0) { // mod_exp
			return false;
		}

		ele_count = 1;
		op->vecs[0].buf_sz = mp->offset - offset_start;
	}

	uint32_t idx = 0;

	for (uint32_t i = 1; i < ele_count; i++) {
		msgpack_type type = msgpack_peek_type(mp);
		uint32_t sz;

		switch (type) {
		case MSGPACK_TYPE_LIST:
			if (op->vecs[idx].buf_sz != 0) {
				idx++;
				if (idx >= EXP_CALL_MAX_VEC_IDX) {
					cf_warning(AS_EXP,
							"parse_op_call - too many call parameters");
					return false;
				}
			}

			op->vecs[idx].buf = exp_call_eval_token;
			op->vecs[idx].buf_sz = 0;
			idx++;
			if (idx >= EXP_CALL_MAX_VEC_IDX) {
				cf_warning(AS_EXP, "parse_op_call - too many call parameters");
				return false;
			}
			op->eval_count++;

			if (! build_next(args)) {
				return false;
			}

			op->vecs[idx].buf = mp->buf + mp->offset; // next vector
			op->vecs[idx].buf_sz = 0;
			break;
		case MSGPACK_TYPE_MAP: {
			const uint8_t* start = mp->buf + mp->offset;

			sz = msgpack_sz(mp);

			if (sz == 0) {
				return false;
			}

			op->vecs[idx].buf_sz += sz;

			// The sizing pass cannot tell a call parameter from an
			// instruction, so a non-canonical one still holds a reservation
			// nothing here consumes - release it. The op that takes the
			// parameter rewrites it, so there is nothing to canonicalize.
			if (args->literal_cleanup != 0) {
				bool marker = false;

				if (build_check_cdt_literal(start, sz, &marker) ==
						CDT_LITERAL_NEEDS_REWRITE) {
					args->literal_cleanup--;
				}
			}

			break;
		}
		case MSGPACK_TYPE_ERROR:
			return false;
		default:
			sz = msgpack_sz(mp);
			op->vecs[idx].buf_sz += sz;

			if (sz == 0) {
				return false;
			}

			break;
		}
	}

	if (op->vecs[idx].buf_sz != 0) {
		idx++;
		if (idx > EXP_CALL_MAX_VEC_IDX) {
			cf_warning(AS_EXP, "parse_op_call - too many call parameters");
			return false;
		}
	}

	op->n_vecs = idx;

	return true;
}

bool
exp_build_set_expected_type(as_exp* exp, const exp_op_table_entry* entry)
{
	switch (entry->r_type) {
	case EXP_RTYPE_NIL:
		exp->expected_type = AS_PARTICLE_TYPE_NULL;
		break;
	case EXP_RTYPE_TRILEAN:
		exp->expected_type = AS_PARTICLE_TYPE_BOOL;
		break;
	case EXP_RTYPE_INT:
		exp->expected_type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case EXP_RTYPE_STR:
		exp->expected_type = AS_PARTICLE_TYPE_STRING;
		break;
	case EXP_RTYPE_LIST:
		exp->expected_type = AS_PARTICLE_TYPE_LIST;
		break;
	case EXP_RTYPE_MAP:
		exp->expected_type = AS_PARTICLE_TYPE_MAP;
		break;
	case EXP_RTYPE_BLOB:
		exp->expected_type = AS_PARTICLE_TYPE_BLOB;
		break;
	case EXP_RTYPE_FLOAT:
		exp->expected_type = AS_PARTICLE_TYPE_FLOAT;
		break;
	case EXP_RTYPE_GEOJSON:
		exp->expected_type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	case EXP_RTYPE_HLL:
		exp->expected_type = AS_PARTICLE_TYPE_HLL;
		break;
	case EXP_RTYPE_RESULT_REMOVE:
		// No real particle type — RESULT_REMOVE is a write-side
		// sentinel handled elsewhere. NULL here just guarantees the
		// downstream "is BOOL?" check fails as expected.
		exp->expected_type = AS_PARTICLE_TYPE_NULL;
		break;
	case EXP_RTYPE_END:
	default:
		cf_warning(AS_EXP,
				"exp_build_set_expected_type - unexpected exp_rtype %u",
				entry->r_type);
		return false;
	}

	return true;
}

static as_exp*
check_filter_exp(as_exp* exp)
{
	if ((as_particle_type)exp->expected_type != AS_PARTICLE_TYPE_BOOL) {
		cf_warning(AS_EXP,
				"check_filter_exp - filters must return type %u (bool) found %u",
				AS_PARTICLE_TYPE_BOOL, exp->expected_type);

		// The build SUCCEEDED (accumulator empty), so a staging caller would
		// otherwise fold a bare context message with no diagnostic at all.
		// Record the reason, message-only for the AEL flavor: no position and
		// no payload ref - the source lives in the exp destroyed just below,
		// so a retained pointer would dangle at as_exp_take_build_error()
		// time (the base64-path lesson). Wire builds keep the pre-existing
		// no-detail behavior - the wire arm of as_exp_take_build_error()
		// carries no message channel.
		if (exp->ael_map != NULL) {
			ael_build_err_record(NULL, 0, false, 0, 0,
					"filter expression must evaluate to boolean");
		}

		as_exp_destroy(exp);
		return NULL;
	}

	return exp;
}

//==========================================================
// Local helpers - debug utilites.
//

static void
debug_exp_check(const as_exp* exp)
{
#ifndef exp_check
	(void)exp;
#else
	cf_assert(exp != NULL, AS_EXP, "exp was null");

	if (exp->version != 2) {
		return;
	}

	cf_validate_pointer(exp->buf_cleanup);
	cf_validate_pointer(exp);
#endif
}
