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
#include "exp/ael_emit.h"
#include "exp/ael_parse.h"
#include "exp/ael_string.h"
#include "exp/ast.h"
#include "exp/exp_rt.h"
#include "exp/exp_wire.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"

// #include "warnings.h"

//==========================================================
// Typedefs & constants.
//

#define EXP_MAX_SIZE (1 * 1024 * 1024) // 1 MiB

// Pre-parse source cap. Output is capped at EXP_MAX_SIZE, so a larger source is
// rejected later anyway; reject early to bound parse-pool use.
#define EXP_MAX_AEL_SRC_SIZE EXP_MAX_SIZE

// EXP_MAX_DEPTH (nesting cap for all traversals) is in exp/ast.h, shared with
// ael_codegen.c and the AEL build passes.

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

typedef struct var_entry_s {
	const uint8_t* name;
	uint32_t name_sz;
	uint32_t idx;
	exp_rtype r_type;
} var_entry;

typedef struct var_scope_s {
	struct var_scope_s* parent;
	uint32_t n_entries;
	var_entry* entries;
} var_scope;

// AEL direct compilation context -- sizing pass.
typedef struct ael_size_ctx_s {
	uint32_t instr_sz;
	uint32_t instr_count; // upper bound on emitted ops - sizes the source map
	uint32_t extra_sz;
	uint32_t cleanup_count;
	uint32_t depth; // ael_count_sz recursion depth (capped at EXP_MAX_DEPTH)
	ast_pool* pool;
	const char* ael_src; // for ael_codegen_pack of inner exps
} ael_size_ctx;

// Build-time view of the bin table (table points at scratch during the build).
typedef struct build_bin_table_s {
	const uint8_t* base;
	exp_bin_name_entry* table;
	uint32_t n_bins;
} build_bin_table;

// AEL direct compilation context -- build pass.
typedef struct ael_build_args_s {
	as_exp* exp;
	uint8_t* mem;
	ast_pool* pool;

	const exp_op_table_entry* entry;
	uint32_t instr_ix;

	// ael_build_node recursion depth, capped at EXP_MAX_DEPTH. Independent of
	// ael_count_sz's cap so this emitter can't overflow the C stack if the two
	// traversals ever descend a node shape differently.
	uint32_t depth;

	uint32_t var_idx;
	uint32_t max_var_idx;
	var_scope* current;

	uint8_t* extra_ptr;
	const char* ael_src;
	uint8_t* ael_buf;
	uint32_t ael_src_sz;
	ael_src_map* src_map; // op ordinal -> source span, filled during emission

	build_bin_table bin_table;
	cf_vector* bins_info_r;
} ael_build_args;

// Record the just-claimed op's AST source span at its preorder ordinal
// (instr_ix was incremented as the op was claimed, so the ordinal is
// instr_ix - 1). Bounds-guarded so a count/emit mismatch degrades to a
// missing map entry, never a scribble.
static inline void
ael_src_map_record(const ael_build_args* args, const ast_node* np)
{
	ael_src_map* m = args->src_map;
	uint32_t ix = args->instr_ix - 1;

	if (m != NULL && ix < m->n_ops) {
		// The map is consumed only to render a source slice, so it stores the
		// DISPLAY start -- for a bin or variable that reaches back over its
		// '$.' / paren prefix, node.offset alone would focus the bare name.
		m->entries[ix] =
				(ael_src_entry){ .offset = ast_disp_offset(np), .sz = np->sz };
	}
}

typedef struct build_counts_s {
	uint32_t total_sz;
	uint32_t cleanup_count;
	uint32_t literal_cleanup;
	uint32_t counter;
	const uint8_t* end;
	build_bin_table bin_table;
} build_counts;

typedef enum {
	CDT_LITERAL_OK,
	CDT_LITERAL_NEEDS_SORT,
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

	// Count of CDT literals the sizing pass found out of canonical form --
	// build_canonicalize_cdt() consumes one per literal it retains.
	uint32_t literal_cleanup;

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
static void
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
static const exp_op_table_entry* build_get_entry(exp_rtype type);
static bool build_count_sz(msgpack_in* mp, build_counts* bc, uint32_t depth);
static var_entry* build_find_var_entry(build_args* args, const uint8_t* name,
		uint32_t name_sz);
static uint32_t build_find_bin_idx(const build_bin_table* t, bin_name128 name128);
static bool build_get_or_add_bin_entry(build_bin_table* t,
		const exp_bin_name_entry* n);
static cdt_literal_status build_check_cdt_literal(const uint8_t* buf,
		uint32_t buf_sz);
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
static bool build_set_expected_particle_type(build_args* args);
static as_exp* check_filter_exp(as_exp* exp);

// Build AEL direct.
static as_exp* build_internal_ael(const uint8_t* ael_str, uint32_t ael_sz,
		cf_vector* bins_info_r);
static bool ael_count_sz(ael_size_ctx* ctx, ast_ref ref);
static bool ael_build_left_fold(ael_build_args* args, ast_ref ref,
		ast_node_t type);
static bool ael_build_node(ael_build_args* args, ast_ref ref);
static bool ast_is_blob_literal(ast_node_t type);

// SIZE/BUILD context for ael_pack_call_blob (defined fully below).
// Exactly one of `size` / `build` is non-NULL per call. Caller pre-
// seeds vec[0] (BUILD only) and the packer (sizer for SIZE).
typedef struct call_blob_ctx_s {
	ael_size_ctx* size;
	ael_build_args* build;
	op_call* opc;
	as_packer* pk;
	bool allow_inline; // SIZE may set op_node->u.cdt_op.blob_inline
} call_blob_ctx;

static int ael_pack_call_blob(call_blob_ctx* cb, ast_pool* pool,
		const char* ael_src, ast_node* op_node);

static uint32_t compute_call_n_vecs(ast_pool* pool, ast_ref param_head,
		ast_prop_bits props_flags, int cdt_op);

// AEL msgpack literal packing.
static int ael_pack_literal(as_packer* pk, ast_pool* pool, ast_ref ref);
static uint32_t ael_literal_pack_sz(ast_pool* pool, ast_ref ref);

// SELECT-blob packing context. ael_pack_select_blob needs to dispatch
// between the size pass (size_ctx set) and the build pass (args set) so
// it can recurse correctly when packing the apply / filter sub-expressions.
typedef struct {
	as_packer* pk; // always set
	ast_pool* pool; // always set
	ael_size_ctx* size_ctx; // set during size pass; NULL during build
	ael_build_args* args; // set during build pass; NULL during size
} blob_pack_ctx;

// ael_emit_cdt_op_blob is shared with ael_codegen.c -- declaration in ael_emit.h.
static int ael_pack_select_blob(blob_pack_ctx* bc, ast_ref ctx_seg_head,
		ast_ref pf_ref);

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
	as_exp* exp = build_internal_ael(ael_str, ael_sz, NULL);

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
			// A canonicalized CDT literal retained by
			// build_canonicalize_cdt() - literals that arrived canonical
			// alias the wire and are never on this stack.
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

// Record the primary diagnostic from 'diags' (positioned), or the fixed
// fallback when the failure produced no diagnostic. Shared by the sizing- and
// build-pass failure epilogues of build_internal_ael.
static void
ael_build_err_record_diags(const uint8_t* src, uint32_t src_sz,
		const ael_diag_list* diags, const char* fallback_msg)
{
	if (ael_diag_has_error(diags)) {
		const ael_diag* d = &diags->entries[0];

		cf_warning(AS_EXP, "build_internal_ael - %s @ %u", d->msg, d->offset);
		ael_build_err_record(src, src_sz, true, d->offset, d->sz, d->msg);
	}
	else {
		cf_warning(AS_EXP, "build_internal_ael - %s", fallback_msg);
		ael_build_err_record(src, src_sz, false, 0, 0, fallback_msg);
	}
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
				return build_internal_ael(ael_str, ael_sz, bins_info_r);
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
	// Every literal the sizing pass counted as needing canonicalization must
	// have been consumed by build_canonicalize_cdt() - a remainder means the
	// passes disagreed and an unsorted literal may have aliased through.
	cf_assert(args.literal_cleanup == 0, AS_EXP,
			"literal_cleanup (%u) not consumed", args.literal_cleanup);

	memcpy(rt_bins->table, bc.bin_table.table,
			bc.bin_table.n_bins * sizeof(exp_bin_name_entry));
	rt_bins->n_bins = bc.bin_table.n_bins;
	// buf may have been copied so we cannot use bc.bin_table.base here.
	rt_bins->base = args.mp.buf;

	args.exp->max_var_count = args.max_var_idx;

	if (! build_set_expected_particle_type(&args)) {
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

static const exp_op_table_entry*
build_get_entry(exp_rtype type)
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
				const uint8_t* ele_start = mp->buf + mp->offset;
				uint32_t ele_sz = msgpack_sz(mp);

				if (ele_sz == 0) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid instruction at offset %u",
							mp->offset);
					return false;
				}

				// Map literals: validate now, and count any needing
				// canonicalization so a cleanup slot gets reserved for
				// build_canonicalize_cdt().
				if (type == MSGPACK_TYPE_MAP) {
					switch (build_check_cdt_literal(ele_start, ele_sz)) {
					case CDT_LITERAL_INVALID:
						return false;
					case CDT_LITERAL_NEEDS_SORT:
						bc->literal_cleanup++;
						break;
					default:
						break;
					}
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

			if (! build_get_or_add_bin_entry(&bc->bin_table, &bn)) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid bin name at offset %u",
						mp->offset);
				return false;
			}

			break;
		}
		case EXP_QUOTE: {
			// Quoted list literal: validate now, and count it if it needs
			// canonicalization so a cleanup slot gets reserved for
			// build_canonicalize_cdt().
			const uint8_t* q_start = mp->buf + mp->offset;
			uint32_t q_sz = msgpack_sz(mp);

			if (q_sz == 0) {
				cf_warning(AS_EXP,
						"build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			switch (build_check_cdt_literal(q_start, q_sz)) {
			case CDT_LITERAL_INVALID:
				return false;
			case CDT_LITERAL_NEEDS_SORT:
				bc->literal_cleanup++;
				break;
			default:
				break;
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
				if (msgpack_peek_type(mp) == MSGPACK_TYPE_LIST) {
					bc->counter += param_count - i;
					break;
				}

				if (msgpack_sz(mp) == 0) {
					cf_warning(AS_EXP,
							"build_count_sz - invalid instruction at offset %u",
							mp->offset);
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

static uint32_t
build_find_bin_idx(const build_bin_table* t, bin_name128 name128)
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

static bool
build_get_or_add_bin_entry(build_bin_table* t, const exp_bin_name_entry* n)
{
	define_bin_name128(name128, t->base + n->off, n->sz, n->need_memcpy);

	if (! as_bin_name128_check(name128, n->sz)) {
		return false;
	}

	if (build_find_bin_idx(t, name128) == t->n_bins) {
		if (t->n_bins == RECORD_MAX_BINS) {
			return false; // never more distinct bins than a record can hold
		}

		t->table[t->n_bins++] = *n;
	}

	return true;
}

// Register every bin in a bin_next-threaded AST_BIN list into the runtime bin
// table (deduped by name, capped at RECORD_MAX_BINS). Used for both the
// canonical bin_root list and the local ($.bin:LOCAL:T) list -- every emitted
// bin op must have a table slot, matching the wire path.
static bool
ael_register_bins(build_bin_table* t, ast_pool* pool, const uint8_t* ael_str,
		uint32_t ael_sz, ast_ref head)
{
	for (ast_ref br = head; br != AST_REF_NULL;) {
		const ast_node* bp = ast_pool_at(pool, br);
		const uint8_t* name = ael_str + bp->offset;
		exp_bin_name_entry bn = {
			.off = bp->offset,
			.sz = bp->u.bin.name_sz,
			.need_memcpy = as_bin_name_need_memcpy(name, ael_str + ael_sz),
		};

		if (! build_get_or_add_bin_entry(t, &bn)) {
			// Record the source-positioned error detail for the offending bin
			// (the caller only logs a generic warning).
			ael_build_err_record(ael_str, ael_sz, true, bp->offset,
					bp->u.bin.name_sz, "invalid bin name");
			return false;
		}

		br = bp->u.bin.bin_next;
	}

	return true;
}

// Sizing-pass validation of a CDT literal (quoted list or bare map). Corrupt
// or non-compact input is invalid. A compact literal whose map keys arrive
// unsorted is legal client input (e.g. a language-level unordered map) -
// report NEEDS_SORT so build_internal() reserves a cleanup slot and
// build_canonicalize_cdt() sorts it into an exp-owned copy at build.
static cdt_literal_status
build_check_cdt_literal(const uint8_t* buf, uint32_t buf_sz)
{
	// has_toplvl false strips any top-level persist-index flag, so the rewrite
	// only reorders/compactifies and can never exceed buf_sz.
	// Need more options for cdt_untrusted_rewrite() if we want to support
	// PERSIST_INDEX in exp literals in the future.
	define_deferred_memory(temp, buf_sz);
	uint32_t new_sz = cdt_untrusted_rewrite(temp, buf, buf_sz, false);

	if (new_sz == 0) {
		cf_warning(AS_EXP, "build_check_cdt_literal - error %u invalid cdt",
				AS_ERR_PARAMETER);
		return CDT_LITERAL_INVALID;
	}

	if (new_sz != buf_sz) {
		cf_warning(AS_EXP,
				"build_check_cdt_literal - error %u cdt not compactified",
				AS_ERR_PARAMETER);
		return CDT_LITERAL_INVALID;
	}

	return memcmp(buf, temp, buf_sz) == 0 ? CDT_LITERAL_OK
										  : CDT_LITERAL_NEEDS_SORT;
}

// Build-pass companion to build_check_cdt_literal(). The sizing pass counted
// the literals needing canonicalization into literal_cleanup and
// build_internal() reserved a cleanup slot for each, so a zero count means
// every literal already aliases canonical source bytes.
// The two passes must agree, which relies on the source bytes being immutable
// between them - guaranteed because nothing writes parse-source bytes after
// demarshal. Do not "optimize" this into an in-place rewrite: batch repeat
// sub-transactions share one msgp, so writing the source races sibling
// sub-transactions' reads.
static bool
build_canonicalize_cdt(build_args* args, op_value_blob* op)
{
	if (args->literal_cleanup == 0) {
		return true;
	}

	uint8_t* mem = cf_malloc(op->value_sz);

	cdt_untrusted_rewrite(mem, op->value, op->value_sz, false);

	if (memcmp(mem, op->value, op->value_sz) == 0) {
		cf_free(mem); // already canonical - keep aliasing the source
		return true;
	}

	// Retain the canonicalized copy - the op owns it and as_exp_destroy()
	// frees it via the cleanup stack.
	op->value = mem;
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

	args->entry = build_get_entry((exp_rtype)type64);

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
	uint32_t idx = build_find_bin_idx(&args->bin_table, name128);

	cf_assert(idx < args->bin_table.n_bins, AS_EXP,
			"build_bin - unexpected error bin name %.*s", name_sz, name);

	op->idx = idx;

	if (args->bins_info_r != NULL) {
		as_bin_info bin_info;

		memcpy(bin_info.name, &name128, sizeof(name128));
		bin_info.type = exp_rtype_to_particle_type(op->base.rtype);
		cf_vector_append(args->bins_info_r, &bin_info);
	}

	if ((args->entry = build_get_entry(op->base.rtype)) == NULL) {
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
	uint32_t idx = build_find_bin_idx(&args->bin_table, name128);

	cf_assert(idx < args->bin_table.n_bins, AS_EXP,
			"build_bin_meta - unexpected error bin name %.*s", name_sz, name);

	op->idx = idx;
	op->base.rtype = r_type;

	if (args->bins_info_r != NULL) {
		as_bin_info bin_info;

		memcpy(bin_info.name, &name128, sizeof(name128));
		// Wildcard: this bin is depended on at *all* types.
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

	args->entry = build_get_entry(type);
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

	if ((args->entry = build_get_entry(op->base.rtype)) == NULL) {
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
		if ((args->entry = build_get_entry(op->base.rtype)) == NULL) {
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
		if (args->entry->r_type != EXP_RTYPE_STR) {
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

	if ((args->entry = build_get_entry(op->type)) == NULL) {
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
build_set_expected_particle_type(build_args* args)
{
	switch (args->entry->r_type) {
	case EXP_RTYPE_NIL:
		args->exp->expected_type = AS_PARTICLE_TYPE_NULL;
		break;
	case EXP_RTYPE_TRILEAN:
		args->exp->expected_type = AS_PARTICLE_TYPE_BOOL;
		break;
	case EXP_RTYPE_INT:
		args->exp->expected_type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case EXP_RTYPE_STR:
		args->exp->expected_type = AS_PARTICLE_TYPE_STRING;
		break;
	case EXP_RTYPE_LIST:
		args->exp->expected_type = AS_PARTICLE_TYPE_LIST;
		break;
	case EXP_RTYPE_MAP:
		args->exp->expected_type = AS_PARTICLE_TYPE_MAP;
		break;
	case EXP_RTYPE_BLOB:
		args->exp->expected_type = AS_PARTICLE_TYPE_BLOB;
		break;
	case EXP_RTYPE_FLOAT:
		args->exp->expected_type = AS_PARTICLE_TYPE_FLOAT;
		break;
	case EXP_RTYPE_GEOJSON:
		args->exp->expected_type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	case EXP_RTYPE_HLL:
		args->exp->expected_type = AS_PARTICLE_TYPE_HLL;
		break;
	case EXP_RTYPE_RESULT_REMOVE:
		// No real particle type — RESULT_REMOVE is a write-side
		// sentinel handled elsewhere. NULL here just guarantees the
		// downstream "is BOOL?" check fails as expected.
		args->exp->expected_type = AS_PARTICLE_TYPE_NULL;
		break;
	case EXP_RTYPE_END:
	default:
		cf_warning(AS_EXP,
				"build_set_expected_particle_type - unexpected exp_rtype %u",
				args->entry->r_type);
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
// Local helpers - build AEL direct.
//

// 1-based line/column of a byte offset in the source, for client-facing
// parse diagnostics (the server log carries the raw offset).
static void
ael_offset_line_col(const char* src, uint32_t sz, uint32_t offset,
		uint32_t* line_r, uint32_t* col_r)
{
	uint32_t line = 1;
	uint32_t col = 1;
	uint32_t end = offset < sz ? offset : sz;

	for (uint32_t i = 0; i < end; i++) {
		if (src[i] == '\n') {
			line++;
			col = 1;
		}
		else {
			col++;
		}
	}

	*line_r = line;
	*col_r = col;
}

static as_exp*
build_internal_ael(const uint8_t* ael_str, uint32_t ael_sz, cf_vector* bins_info_r)
{
	if (ael_sz > EXP_MAX_AEL_SRC_SIZE) {
		cf_warning(AS_EXP,
				"build_internal_ael - AEL source size %u exceeds limit of %u bytes",
				ael_sz, EXP_MAX_AEL_SRC_SIZE);
		// First-set-wins: give the client the specific reason via the
		// build-error accumulator (which also stages the lang=AEL trace); the
		// transaction layer's generic fallback then no-ops.
		char msg[AS_EXP_BUILD_ERROR_MSG_MAX];

		snprintf(msg, sizeof(msg), "expression exceeds maximum size of %u bytes",
				EXP_MAX_AEL_SRC_SIZE);
		ael_build_err_record(ael_str, ael_sz, false, 0, 0, msg);
		return NULL;
	}

	ast_pool pool;

	ast_pool_init(&pool);

	// The AST pool and the parse / build diag lists are freed on every return
	// below via cf_defer (scope-exit), each registered once its resource exists
	// -- so a new early return can't leak them. Defers run LIFO, reproducing the
	// former build_diags -> pr.diags -> pool order.
	cf_defer { ast_pool_destroy(&pool); }

	ael_parse_result pr = ael_parse(&pool, (const char*)ael_str, ael_sz);

	cf_defer { ael_diag_list_destroy(&pr.diags); }

	if (ael_parse_has_error(&pr)) {
		const ael_diag* d = &pr.diags.entries[0];

		cf_warning(AS_EXP, "build_internal_ael - %s @ %u", d->msg, d->offset);
		// Record the primary parse diagnostic, composing in the line/col so
		// message-tier clients (no trace) get the location too.
		uint32_t line;
		uint32_t col;

		ael_offset_line_col((const char*)ael_str, ael_sz, d->offset, &line, &col);

		char msg[AS_EXP_BUILD_ERROR_MSG_MAX];

		snprintf(msg, sizeof(msg), "%s at line %u col %u", d->msg, line, col);
		ael_build_err_record(ael_str, ael_sz, true, d->offset, d->sz, msg);
		return NULL;
	}

	// Collect size/build-pass diagnostics (codegen's sub-program emitters
	// report AEL-positioned errors via pool->diags in both passes).
	ael_diag_list build_diags = { 0 };

	cf_defer { ael_diag_list_destroy(&build_diags); }

	pool.diags = &build_diags;

	ael_size_ctx ctx = { .pool = &pool, .ael_src = (const char*)ael_str };

	if (! ael_count_sz(&ctx, pr.root)) {
		cf_warning(AS_EXP, "build_internal_ael - sizing failed");
		ael_build_err_record_diags(ael_str, ael_sz, &build_diags,
				"expression sizing failed");
		return NULL;
	}

	// Collect the distinct bins the expression reads. Canonical bins are on
	// pr.bin_root; $.bin:LOCAL:T bins are on pr.local_bin_root (kept off
	// bin_root for type narrowing). Both need a runtime slot -- mirrors the
	// wire path, where build_count_sz registers every bin op. They occupy the
	// first n_bins var slots, loaded/masked once by exp_eval_bin.
	exp_bin_name_entry bin_entries[RECORD_MAX_BINS];
	build_bin_table bin_table = { .table = bin_entries, .base = ael_str };

	// Register both bin lists: canonical bins on pr.bin_root and the loose
	// $.bin:LOCAL:T bins on pr.local_bin_root (kept off bin_root for type
	// narrowing). Both need a runtime slot -- mirrors the wire path. The
	// invalid-bin-name error detail is staged inside ael_register_bins.
	if (! ael_register_bins(&bin_table, &pool, ael_str, ael_sz, pr.bin_root) ||
			! ael_register_bins(&bin_table, &pool, ael_str, ael_sz,
					pr.local_bin_root)) {
		cf_warning(AS_EXP, "build_internal_ael - invalid bin name");
		return NULL;
	}

	// Layout: [ops][extra][cleanup][exp_rt_bin_table + entries][ael_src_map +
	// entries][ael_buf]. The extra region is byte data, so 8-align the void**
	// cleanup_stack; the source map holds a pointer, so 8-align it too.
	// Offsets are computed in uint64 so the EXP_MAX_SIZE guard below stays
	// authoritative regardless of the source cap -- no intermediate 32-bit
	// wrap can slip a too-small buffer past the check.
	uint64_t cleanup_offset = ((uint64_t)ctx.instr_sz + ctx.extra_sz + 7) & ~7ull;
	uint64_t bin_table_offset =
			(cleanup_offset + (uint64_t)ctx.cleanup_count * sizeof(void*) + 7) &
			~7ull;
	uint64_t bin_table_sz = sizeof(exp_rt_bin_table) +
			(uint64_t)bin_table.n_bins * sizeof(exp_bin_name_entry);
	uint64_t src_map_offset = (bin_table_offset + bin_table_sz + 7) & ~7ull;
	uint64_t src_map_sz = sizeof(ael_src_map) +
			(uint64_t)ctx.instr_count * sizeof(ael_src_entry);
	uint64_t ael_buf_offset = src_map_offset + src_map_sz;
	uint64_t total_sz_u64 = ael_buf_offset + ael_sz;

	if (total_sz_u64 >= EXP_MAX_SIZE) {
		cf_warning(AS_EXP,
				"build_internal_ael - expression size %lu exceeds limit of %u bytes",
				total_sz_u64, EXP_MAX_SIZE);
		ael_build_err_record(ael_str, ael_sz, false, 0, 0,
				"compiled expression exceeds the size limit");
		return NULL;
	}

	uint32_t total_sz = (uint32_t)total_sz_u64;

	ael_build_args args = {
		.exp = cf_calloc(1, sizeof(as_exp) + total_sz),
		.pool = &pool,
		.bins_info_r = bins_info_r,
	};

	args.mem = args.exp->mem;
	args.exp->cleanup_stack = (void**)(args.exp->mem + cleanup_offset);
	args.extra_ptr = args.exp->mem + ctx.instr_sz;
	args.ael_buf = args.exp->mem + ael_buf_offset;
	args.ael_src = (const char*)ael_str;
	args.ael_src_sz = ael_sz;

	memcpy(args.ael_buf, ael_str, ael_sz);

	// Publish the runtime source map - entries fill in as ops are emitted.
	ael_src_map* src_map = (ael_src_map*)(args.exp->mem + src_map_offset);

	src_map->src = args.ael_buf;
	src_map->src_sz = ael_sz;
	src_map->n_ops = ctx.instr_count;
	args.src_map = src_map;
	args.exp->ael_map = src_map;

	// Publish the bin table -- entries locate name bytes in the copied ael_buf.
	exp_rt_bin_table* rt_bins =
			(exp_rt_bin_table*)(args.exp->mem + bin_table_offset);

	memcpy(rt_bins->table, bin_entries,
			bin_table.n_bins * sizeof(exp_bin_name_entry));
	rt_bins->n_bins = bin_table.n_bins;
	rt_bins->base = args.ael_buf;
	args.exp->bin_table = rt_bins;

	args.bin_table = (build_bin_table){
		.base = args.ael_buf,
		.table = rt_bins->table,
		.n_bins = bin_table.n_bins,
	};

	// Bins own var slots [0, n_bins); let-vars follow, shifted up by n_bins at
	// the AST_VAR / AST_LET emit sites.
	args.var_idx = bin_table.n_bins;
	args.max_var_idx = bin_table.n_bins;

	// Sub-program emission validates the just-emitted msgpack via
	// pool->diags (armed above, before sizing); any AEL-positioned errors
	// land in build_diags.
	bool build_ok = ael_build_node(&args, pr.root);

	pool.diags = NULL;

	if (! build_ok) {
		if (ael_diag_has_error(&build_diags)) {
			cf_warning(AS_EXP, "build_internal_ael - %s @ %u",
					build_diags.entries[0].msg, build_diags.entries[0].offset);
		}
		else {
			cf_warning(AS_EXP, "build_internal_ael - build failed");
		}

		// The caller stages the trace and folds the diagnostic into its
		// context-prefixed message.
		ael_build_err_record_diags(ael_str, ael_sz, &build_diags,
				"expression build failed");
		as_exp_destroy(args.exp);
		return NULL;
	}

	cf_assert(args.mem <= (uint8_t*)args.exp->cleanup_stack, AS_EXP,
			"ael build overran cleanup_stack %p > %p", args.mem,
			args.exp->cleanup_stack);
	cf_assert(args.extra_ptr <= (uint8_t*)args.exp->cleanup_stack, AS_EXP,
			"ael build extra_data overran cleanup_stack %p > %p",
			args.extra_ptr, args.exp->cleanup_stack);
	// Sized-op count is an UPPER bound on emitted ops, not an equality:
	// left-folded chains (a - b - c, a / b / c) size one op per AST chain
	// node but emit a single vararg op. Emitting MORE than sized means the
	// op region itself was under-reserved - crash-worthy corruption.
	cf_assert(args.instr_ix <= ctx.instr_count, AS_EXP,
			"ael op overrun - emitted %u sized %u", args.instr_ix,
			ctx.instr_count);

	args.exp->max_var_count = args.max_var_idx;

	// Set expected particle type.
	build_args type_args = { .exp = args.exp, .entry = args.entry };

	if (args.entry == NULL || ! build_set_expected_particle_type(&type_args)) {
		ael_build_err_record(ael_str, ael_sz, false, 0, 0,
				"invalid expression result type");
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

		cf_detail(AS_EXP, "parsed ael exp: %.*s", (int)db.used_sz, db.buf);

		cf_dyn_buf_free(&db);
	}

	return args.exp;
}

// Account n emitted ops of 'code' in the sizing pass. The byte size and the
// op count MUST move together: the count sizes the runtime source map and
// upper-bounds the emitted-op assert in build_internal_ael - a size without a
// count would fire that assert on any expression using the op.
static inline void
ael_size_ops(ael_size_ctx* ctx, exp_op_code code, uint32_t n)
{
	ctx->instr_sz += n * op_table[code].size;
	ctx->instr_count += n;
}

static inline void
ael_size_op(ael_size_ctx* ctx, exp_op_code code)
{
	ael_size_ops(ctx, code, 1);
}

// Depth guard for the recursive sizer: a left-deep chain is rejected at
// EXP_MAX_DEPTH before it overflows the C stack. Cleanup is the non-recursive
// ast_pool_destroy, so the deep tree is freed without recursing.
static bool
ael_count_sz(ael_size_ctx* ctx, ast_ref ref)
{
	if (ctx->depth >= EXP_MAX_DEPTH) {
		cf_warning(AS_EXP, "ael_count_sz - expression nesting exceeds %u",
				EXP_MAX_DEPTH);
		return false;
	}

	ctx->depth++;
	cf_defer { ctx->depth--; }

	if (ref == AST_REF_NULL) {
		return true;
	}

	ast_pool* pool = ctx->pool;
	ast_node* np = ast_pool_at(pool, ref);

	switch (np->type) {
	case AST_NIL:
		ael_size_op(ctx, EXP_VOP_VALUE_NIL);
		return true;

	case AST_INT:
		ael_size_op(ctx, EXP_VOP_VALUE_INT);
		return true;

	case AST_FLOAT:
		ael_size_op(ctx, EXP_VOP_VALUE_FLOAT);
		return true;

	case AST_ZERO:
		if (np->etype == AST_ETYPE_FLOAT) {
			ael_size_op(ctx, EXP_VOP_VALUE_FLOAT);
		}
		else {
			ael_size_op(ctx, EXP_VOP_VALUE_INT);
		}
		return true;

	case AST_BOOL:
		ael_size_op(ctx, EXP_VOP_VALUE_BOOL);
		return true;

	case AST_STRING: {
		ael_size_op(ctx, EXP_VOP_VALUE_STR);

		if (np->u.str.has_escape) {
			ctx->extra_sz += ael_string_decoded_sz(np->u.str.str, np->u.str.sz);
		}

		return true;
	}

	case AST_BLOB:
		ael_size_op(ctx, EXP_VOP_VALUE_BLOB);
		ctx->extra_sz += np->u.str.sz / 2; // decoded binary
		return true;

	case AST_B64_BLOB:
		ael_size_op(ctx, EXP_VOP_VALUE_BLOB);
		ctx->extra_sz += ael_b64_decoded_sz(np->u.str.str, np->u.str.sz);
		return true;

	case AST_GEO_LITERAL:
		ael_size_op(ctx, EXP_VOP_VALUE_GEO);
		// Raw JSON copied into extra_data + 1 byte AS_BYTES_GEOJSON
		// prefix; build_value_geo reads from a msgpack-bin source so
		// account for the bin header bytes too (max 5 for u32 len).
		ctx->extra_sz += np->u.str.sz + 6;
		ctx->cleanup_count++; // s2 region needs geo_region_destroy
		return true;

	case AST_BIN:
	case AST_BIN_REF:
		ael_size_op(ctx, EXP_BIN);
		return true;

	case AST_META: {
		exp_op_code mc = np->u.meta.op_code;

		ael_size_op(ctx, mc);

		if (mc == EXP_META_DIGEST_MOD) {
			ctx->extra_sz += 0; // digest mod param is in struct
		}

		return true;
	}

	case AST_VAR:
		ael_size_op(ctx, EXP_VAR);
		return true;

	case AST_LOOP_VAR:
		ael_size_op(ctx, EXP_VAR_BUILTIN);
		return true;

	case AST_UNKNOWN:
		ael_size_op(ctx, EXP_UNK);
		return true;

	// N-ary math (circular list).
	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN: {
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		ast_ref e = ast_nmath_head(pool, np);

		for (uint32_t i = 0; i < np->u.nmath.count; i++) {
			if (! ael_count_sz(ctx, e)) {
				return false;
			}

			e = ast_pool_at(pool, e)->next;
		}

		return true;
	}

	// N-ary ops (linear list).
	case AST_AND:
	case AST_OR:
	case AST_EXCLUSIVE:
	case AST_BIT_AND:
	case AST_BIT_OR:
	case AST_BIT_XOR: {
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		ast_ref e = np->u.list.head;

		for (uint32_t i = 0; i < np->u.list.count; i++) {
			if (! ael_count_sz(ctx, e)) {
				return false;
			}

			if (i + 1 < np->u.list.count) {
				e = ast_pool_at(pool, e)->next;
			}
		}

		return true;
	}

	// Left-fold binary ops (emitted as n-ary).
	case AST_SUB:
	case AST_DIV:
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		return ael_count_sz(ctx, np->u.binary.left) &&
				ael_count_sz(ctx, np->u.binary.right);

	// Binary ops.
	case AST_CMP_EQ:
	case AST_CMP_NE:
	case AST_CMP_GT:
	case AST_CMP_GE:
	case AST_CMP_LT:
	case AST_CMP_LE:
	case AST_CMP_IN:
	case AST_CMP_GEO:
	case AST_MOD:
	case AST_POW:
	case AST_LSHIFT:
	case AST_RSHIFT_ARITH:
	case AST_RSHIFT_LOGIC:
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		return ael_count_sz(ctx, np->u.binary.left) &&
				ael_count_sz(ctx, np->u.binary.right);

	// Unary ops.
	case AST_NOT:
	case AST_BIT_NOT:
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
	case AST_PATH_FUNC_CAST_STRING:
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		return ael_count_sz(ctx, np->u.unary.operand);

	// 1-arg functions.
	case AST_FUNC_ABS:
	case AST_FUNC_CEIL:
	case AST_FUNC_FLOOR:
	case AST_FUNC_COUNT_ONE_BITS:
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		return ael_count_sz(ctx, np->u.func1.arg);

	// 2-arg functions.
	case AST_FUNC_LOG:
	case AST_FUNC_POW:
	case AST_FUNC_FIND_BIT_LEFT:
	case AST_FUNC_FIND_BIT_RIGHT:
		ael_size_op(ctx, ast_node_table[np->type].exp_cmd);
		return ael_count_sz(ctx, np->u.func2.arg1) &&
				ael_count_sz(ctx, np->u.func2.arg2);

	// Collection literals -- msgpack-encoded into extra_data.
	case AST_LIST: {
		ael_size_op(ctx, EXP_QUOTE);
		uint32_t pack_sz = ael_literal_pack_sz(pool, ref);

		if (pack_sz == 0 && np->u.list.count > 0) {
			return false;
		}

		ctx->extra_sz += pack_sz;
		return true;
	}

	case AST_MAP: {
		ael_size_op(ctx, EXP_VOP_VALUE_MSGPACK);
		uint32_t pack_sz = ael_literal_pack_sz(pool, ref);

		if (pack_sz == 0 && np->u.list.count > 0) {
			return false;
		}

		ctx->extra_sz += pack_sz;
		return true;
	}

	// WITH → LET.
	case AST_LET: {
		uint32_t defs_count = np->u.list.count - 2; // exclude let_scope + body

		ael_size_op(ctx, EXP_LET);

		// Skip let_scope at head.
		ast_ref def = ast_pool_at(pool, np->u.list.head)->next;

		for (uint32_t i = 0; i < defs_count; i++) {
			ast_node* dp = ast_pool_at(pool, def);

			if (! ael_count_sz(ctx, dp->u.var_def.value)) {
				return false;
			}

			def = dp->next;
		}

		return ael_count_sz(ctx, np->u.list.tail);
	}

	// WHEN → COND.
	case AST_WHEN: {
		uint32_t mappings_count = np->u.list.count - 1;

		ael_size_op(ctx, EXP_COND);
		ael_size_ops(ctx, EXP_VOP_COND_CASE, mappings_count);

		ast_ref m = np->u.list.head;

		for (uint32_t i = 0; i < mappings_count; i++) {
			ast_node* mp = ast_pool_at(pool, m);

			if (! ael_count_sz(ctx, mp->u.when_case.cond) ||
					! ael_count_sz(ctx, mp->u.when_case.result)) {
				return false;
			}

			m = mp->next;
		}

		return ael_count_sz(ctx, np->u.list.tail);
	}

	// Path access → CALL.
	case AST_PATH_CALL: {
		ast_node* op_node = ast_pool_at(pool, np->u.call.call_op);

		ael_size_op(ctx, EXP_CALL);

		as_packer sizer = { .buffer = NULL };

		// Value-recv path-call: u.call.ctx holds the receiver value-
		// expression directly (no AST_PATH_CTX list). EXP_CALL_BITS
		// always uses this shape; EXP_CALL_CDT may (when the receiver
		// is a paren-expr / exp_base). Pack [op_code, args...] as the
		// inner blob, then recurse on the receiver sub-expression.
		// ael_pack_call_blob routes sub-expression params through
		// eval_token slots (BUILD) / ael_count_sz recursion (SIZE),
		// matching what parse_op_call does for the codegen wire.
		if (np->u.call.stype == EXP_CALL_BITS ||
				ast_pool_at(pool, np->u.call.ctx)->type != AST_PATH_CTX) {
			// Value-recv: no preceding CTX wrapper, so the entire static
			// blob can be considered for inline placement in op_call.vecs.
			call_blob_ctx cb = { .size = ctx, .pk = &sizer, .allow_inline = true };

			if (ael_pack_call_blob(&cb, pool, ctx->ael_src, op_node) != 0) {
				return false;
			}

			return ael_count_sz(ctx, np->u.call.ctx);
		}

		ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);
		ast_ref bin_ref = cxn->u.ctx_list.head;
		ast_ref ctx_seg_head = ast_pool_at(pool, bin_ref)->next;

		blob_pack_ctx size_bc = { .pk = &sizer, .pool = pool, .size_ctx = ctx };

		// .select() / .modify() / wildcard-.remove() — distinct blob
		// shape (SELECT op, no ctx wrapper).
		if (op_node->type == AST_PATH_FUNC_SELECT ||
				op_node->type == AST_PATH_FUNC_MODIFY ||
				op_node->type == AST_PATH_FUNC_PSELECT_REMOVE) {
			if (ael_pack_select_blob(&size_bc, ctx_seg_head,
						np->u.call.call_op) != 0) {
				return false;
			}

			ctx->extra_sz += sizer.offset;

			return ael_count_sz(ctx, bin_ref);
		}

		uint32_t ctx_seg_count = 0;

		for (ast_ref er = ctx_seg_head; er != AST_REF_NULL;
				er = ast_pool_at(pool, er)->next) {
			ctx_seg_count++;
		}

		if (ctx_seg_count > 0) {
			if (ael_pack_ctx(&sizer, pool, ctx->ael_src, ctx_seg_head,
						ctx_seg_count) != 0) {
				return false;
			}
		}

		// CDT path-ctx: the CTX wrapper bytes precede the inner blob in
		// extra_data. The inner blob can't be relocated to op_call.vecs
		// tail without splitting from its CTX wrapper, so disable inline.
		call_blob_ctx cb = { .size = ctx, .pk = &sizer };

		if (ael_pack_call_blob(&cb, pool, ctx->ael_src, op_node) != 0) {
			return false;
		}

		return ael_count_sz(ctx, bin_ref);
	}

	// Type-of: bin_type op.
	case AST_BIN_TYPE:
		ael_size_op(ctx, EXP_BIN_TYPE);
		return true;

	case AST_BIN_EXISTS:
		ael_size_op(ctx, EXP_BIN_EXISTS);
		return true;

	default:
		cf_warning(AS_EXP, "ael_count_sz - unsupported node type %d", np->type);
		return false;
	}
}

//----------------------------------------------------------
// AEL build pass.
//

static bool
ael_build_left_fold(ael_build_args* args, ast_ref ref, ast_node_t type)
{
	ast_node* np = ast_pool_at(args->pool, ref);

	if (np->type == type) {
		if (! ael_build_left_fold(args, np->u.binary.left, type)) {
			return false;
		}

		return ael_build_node(args, np->u.binary.right);
	}

	return ael_build_node(args, ref);
}

// Pure literal AST nodes — ael_codegen_emit packs them as scalar msgpack
// bytes (no leading list header). Anything else emits as an
// `[opcode, ...]` instruction list and must go through an eval_token
// + ael_build_node sub-instruction at the fast-path blob level.
static bool
ast_is_blob_literal(ast_node_t type)
{
	switch (type) {
	case AST_INT:
	case AST_FLOAT:
	case AST_ZERO:
	case AST_BOOL:
	case AST_STRING:
	case AST_BLOB:
	case AST_B64_BLOB:
	case AST_NIL:
	case AST_INF:
	case AST_WILDCARD:
	case AST_GEO_LITERAL:
	case AST_MAP:
		return true;
	default:
		return false;
	}
}

// AST-side counterpart to parse_op_call (which scans the same shape
// from the codegen wire). Both produce the op_call.vecs layout:
//   - One vec range of contiguous static msgpack bytes
//   - call_eval_token marker for each sub-expression slot
//   - Repeat
// The msgpack inside the static vecs is the partial form: list header
// + op_code + literal params + flags. Sub-expression slots are absent
// from the static bytes; the runtime substitutes their evaluated
// msgpack at eval time. Sub-instructions land on the instruction
// stream via ael_build_node (BUILD) / ael_count_sz (SIZE).
//
// Mode is implicit in which member of `cb` is non-NULL:
//   - SIZE: cb->size != NULL — pk is a sizer (NULL buffer); accumulates
//     extra_sz; recurses ael_count_sz on sub-expression params.
//   - BUILD: cb->build != NULL — pk writes into extra_data; opc->vecs
//     gets populated; recurses ael_build_node on sub-expressions.
//
// Caller pre-initializes cb->pk and (BUILD) opc->vecs[0].buf /
// .buf_sz = 0 / opc->n_vecs = 1. Helper may extend buf_sz of the
// current vec and append new vecs as it goes. On return,
// (BUILD) args->extra_ptr is advanced past all blob bytes;
// (SIZE) ctx->extra_sz is incremented by the blob's size.
//
// call_blob_ctx struct typedef'd near the forward declarations above.
//
// When `cb->allow_inline` is true (BUILD won't have a CTX wrapper
// preceding the inner blob), SIZE may set
// `op_node->u.cdt_op.blob_inline` if the entire static byte payload
// fits in the unused `vecs[n_vecs..]` tail of the parent op_call. In
// that case SIZE skips the extra_sz add, and BUILD redirects pk to
// write directly into the op_call tail (no extra_data use).
static uint32_t
compute_call_n_vecs(ast_pool* pool, ast_ref param_head,
		ast_prop_bits props_flags, int cdt_op)
{
	uint64_t mf = ael_prop_to_cdt_flag(props_flags);
	uint64_t cf = ael_prop_to_cdt_create_flag(props_flags);
	bool has_cslot = ael_cdt_op_has_create_flags_slot(cdt_op);
	bool flags_arg = mf != 0 && ! ael_cdt_op_no_modify_flags_slot(cdt_op);
	bool create_arg = has_cslot && (cf != 0 || flags_arg);
	bool sort_arg = ael_sort_flags(props_flags) != 0;

	// Vec[0] starts with the list header + op_code → always non-empty.
	uint32_t cur_idx = 0;
	bool cur_nonempty = true;

	for (ast_ref e = param_head; e != AST_REF_NULL;
			e = ast_pool_at(pool, e)->next) {
		ast_node_t t = ast_pool_at(pool, e)->type;

		if (ast_is_blob_literal(t)) {
			cur_nonempty = true;
			continue;
		}

		if (cur_nonempty) {
			cur_idx++;
		}

		cur_idx++; // eval_token vec
		cur_nonempty = false;
	}

	if (create_arg || flags_arg || sort_arg) {
		cur_nonempty = true;
	}

	if (cur_nonempty) {
		cur_idx++;
	}

	return cur_idx;
}

static int
ael_pack_call_blob(call_blob_ctx* cb, ast_pool* pool, const char* ael_src,
		ast_node* op_node)
{
	bool is_build = (cb->build != NULL);
	as_packer* pk = cb->pk;
	int cdt_op = ast_cdt_op_op_code(op_node);
	ast_ref param_head = ast_cdt_op_head(op_node);
	uint8_t param_count = ast_cdt_op_count(op_node);
	ast_prop_bits props_flags = ast_cdt_op_props(op_node);

	uint64_t mf = ael_prop_to_cdt_flag(props_flags);
	uint64_t cf = ael_prop_to_cdt_create_flag(props_flags);
	bool has_cslot = ael_cdt_op_has_create_flags_slot(cdt_op);
	bool flags_arg = mf != 0 && ! ael_cdt_op_no_modify_flags_slot(cdt_op);
	bool create_arg = has_cslot && (cf != 0 || flags_arg);
	// sort() :DROP_DUPS rides the op's own FLAGS param (not a modify slot).
	uint64_t sf = ael_sort_flags(props_flags);
	bool sort_arg = sf != 0;
	uint32_t extra =
			(sort_arg ? 1 : 0) + (create_arg ? 1 : 0) + (flags_arg ? 1 : 0);

	bool inline_mode = is_build && ast_cdt_op_blob_inline(op_node);

	// BUILD inline mode: redirect pk to write into the op_call tail.
	if (inline_mode) {
		uint32_t n_vecs =
				compute_call_n_vecs(pool, param_head, props_flags, cdt_op);
		uint8_t* inline_buf = (uint8_t*)&cb->opc->vecs[n_vecs];

		cb->opc->vecs[0].buf = inline_buf;
		cb->opc->vecs[0].buf_sz = 0;
		pk->buffer = inline_buf;
		pk->offset = 0;
	}

	uint32_t cur_idx = is_build ? cb->opc->n_vecs - 1 : 0;

	// vec[0].buf is pinned by the caller to pk->buffer + 0 (either at
	// args->extra_ptr's entry position or, for inline mode, the start
	// of opc->vecs tail). Even when the caller pre-packed bytes into
	// pk (e.g. CDT path-ctx packs the CTX wrapper before us), vec[0]
	// covers those bytes too — so the start is offset 0.
	uint32_t vec_start = 0;

	int rc = as_pack_list_header(pk, 1 + param_count + extra);
	if (rc != 0) {
		return -1;
	}

	rc = as_pack_int64(pk, cdt_op);
	if (rc != 0) {
		return -1;
	}

	for (ast_ref e = param_head; e != AST_REF_NULL;
			e = ast_pool_at(pool, e)->next) {
		ast_node* ep = ast_pool_at(pool, e);
		bool is_literal = ast_is_blob_literal(ep->type);

		if (is_literal) {
			rc = ael_codegen_emit(pk, pool, ael_src, e);
			if (rc != 0) {
				return -1;
			}
			continue;
		}

		// Sub-expression.
		if (is_build) {
			op_call* opc = cb->opc;
			uint32_t bytes_in_cur = pk->offset - vec_start;

			// Close current literal vec.
			opc->vecs[cur_idx].buf_sz = bytes_in_cur;
			if (bytes_in_cur != 0) {
				cur_idx++;
				if (cur_idx >= EXP_CALL_MAX_VEC_IDX) {
					return -1;
				}
			}

			// Eval-token vec.
			opc->vecs[cur_idx].buf = exp_call_eval_token;
			opc->vecs[cur_idx].buf_sz = 0;
			cur_idx++;
			opc->eval_count++;
			if (cur_idx >= EXP_CALL_MAX_VEC_IDX) {
				return -1;
			}

			if (inline_mode) {
				// Inline: pk keeps writing into op_call tail.
				// Sub-instruction's extra_data is separate from our
				// blob — it lives in args->extra_ptr.
				if (! ael_build_node(cb->build, e)) {
					return -1;
				}

				vec_start = pk->offset;
				opc->vecs[cur_idx].buf = pk->buffer + pk->offset;
				opc->vecs[cur_idx].buf_sz = 0;
			}
			else {
				// Extra_data: advance extra_ptr past closed-vec bytes,
				// build sub-instruction (may further advance
				// extra_ptr), reset pk to the new region.
				cb->build->extra_ptr += pk->offset;
				if (! ael_build_node(cb->build, e)) {
					return -1;
				}

				pk->buffer = cb->build->extra_ptr;
				pk->offset = 0;
				vec_start = 0;
				opc->vecs[cur_idx].buf = cb->build->extra_ptr;
				opc->vecs[cur_idx].buf_sz = 0;
			}
			continue;
		}

		// SIZE + sub-expression.
		if (! ael_count_sz(cb->size, e)) {
			return -1;
		}
	}

	if (sort_arg) {
		rc = as_pack_int64(pk, (int64_t)sf);
		if (rc != 0) {
			return -1;
		}
	}
	if (create_arg) {
		rc = as_pack_int64(pk, (int64_t)cf); // 0 when pure pad before mf
		if (rc != 0) {
			return -1;
		}
	}
	if (flags_arg) {
		rc = as_pack_int64(pk, (int64_t)mf);
		if (rc != 0) {
			return -1;
		}
	}

	if (is_build) {
		op_call* opc = cb->opc;
		uint32_t last_bytes = pk->offset - vec_start;

		opc->vecs[cur_idx].buf_sz = last_bytes;
		if (last_bytes != 0) {
			cur_idx++;
		}
		opc->n_vecs = cur_idx;

		if (! inline_mode) {
			cb->build->extra_ptr += pk->offset;
		}
		// inline: pk wrote into opc->vecs[] tail; no extra_ptr advance.
	}
	else {
		// SIZE: decide whether blob fits inline. Restricted to the
		// no-sub-expression case (n_vecs == 1). Multi-vec inlining
		// would corrupt the data bytes: BUILD's vec[cur_idx].buf
		// writes for cur_idx >= n_vecs overlap with the inline
		// payload region (vecs[n_vecs..19] memory).
		uint32_t n_vecs =
				compute_call_n_vecs(pool, param_head, props_flags, cdt_op);
		uint32_t inline_cap =
				(n_vecs != 1) ? 0 : (EXP_CALL_MAX_VEC_IDX + 1) * sizeof(op_vec);

		if (cb->allow_inline && pk->offset <= inline_cap) {
			op_node->u.cdt_op.blob_inline = true;
			// Don't add to extra_sz — bytes go inline at BUILD.
		}
		else {
			cb->size->extra_sz += pk->offset;
		}
	}

	return 0;
}

static bool
ael_build_node(ael_build_args* args, ast_ref ref)
{
	if (ref == AST_REF_NULL) {
		return true;
	}

	if (args->depth >= EXP_MAX_DEPTH) {
		cf_warning(AS_EXP, "ael_build_node - expression nesting exceeds %u",
				EXP_MAX_DEPTH);
		return false;
	}

	args->depth++;
	cf_defer { args->depth--; }

	ast_pool* pool = args->pool;
	ast_node* np = ast_pool_at(pool, ref);

	op_base_mem* op = (op_base_mem*)args->mem;
	args->instr_ix++;
	ael_src_map_record(args, np);

	switch (np->type) {
	case AST_NIL:
		op->code = EXP_VOP_VALUE_NIL;
		args->mem += op_table[EXP_VOP_VALUE_NIL].size;
		args->entry = &op_table[EXP_VOP_VALUE_NIL];
		op->instr_end_ix = args->instr_ix;
		return true;

	case AST_UNKNOWN:
		op->code = EXP_UNK;
		args->mem += op_table[EXP_UNK].size;
		args->entry = &op_table[EXP_UNK];
		op->instr_end_ix = args->instr_ix;
		return true;

	case AST_INT: {
		op_value_int* opi = (op_value_int*)op;

		opi->base.code = EXP_VOP_VALUE_INT;
		opi->value = np->u.ival;
		args->mem += op_table[EXP_VOP_VALUE_INT].size;
		args->entry = &op_table[EXP_VOP_VALUE_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FLOAT: {
		op_value_float* opf = (op_value_float*)op;

		opf->base.code = EXP_VOP_VALUE_FLOAT;
		opf->value = np->u.fval;
		args->mem += op_table[EXP_VOP_VALUE_FLOAT].size;
		args->entry = &op_table[EXP_VOP_VALUE_FLOAT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_ZERO: {
		if (np->etype == AST_ETYPE_FLOAT) {
			op_value_float* opf = (op_value_float*)op;

			opf->base.code = EXP_VOP_VALUE_FLOAT;
			opf->value = 0.0;
			args->mem += op_table[EXP_VOP_VALUE_FLOAT].size;
			args->entry = &op_table[EXP_VOP_VALUE_FLOAT];
		}
		else {
			op_value_int* opi = (op_value_int*)op;

			opi->base.code = EXP_VOP_VALUE_INT;
			opi->value = 0;
			args->mem += op_table[EXP_VOP_VALUE_INT].size;
			args->entry = &op_table[EXP_VOP_VALUE_INT];
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_BOOL: {
		op_value_bool* opb = (op_value_bool*)op;

		opb->base.code = EXP_VOP_VALUE_BOOL;
		opb->value = np->u.bval;
		args->mem += op_table[EXP_VOP_VALUE_BOOL].size;
		args->entry = &op_table[EXP_VOP_VALUE_BOOL];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_STRING: {
		op_value_blob* ops = (op_value_blob*)op;

		ops->base.code = EXP_VOP_VALUE_STR;

		if (np->u.str.has_escape) {
			uint32_t decoded_sz = ael_string_decode(np->u.str.str, np->u.str.sz,
					args->extra_ptr);

			ops->value = args->extra_ptr;
			ops->value_sz = decoded_sz;
			args->extra_ptr += decoded_sz;
		}
		else {
			ops->value = args->ael_buf + (np->u.str.str - args->ael_src);
			ops->value_sz = np->u.str.sz;
		}

		args->mem += op_table[EXP_VOP_VALUE_STR].size;
		args->entry = &op_table[EXP_VOP_VALUE_STR];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_BLOB: {
		op_value_blob* opbl = (op_value_blob*)op;
		uint32_t bin_sz = np->u.str.sz / 2;

		opbl->base.code = EXP_VOP_VALUE_BLOB;
		opbl->value = args->extra_ptr;
		opbl->value_sz = bin_sz;

		for (uint32_t i = 0; i < bin_sz; i++) {
			args->extra_ptr[i] =
					(uint8_t)((ael_hex_nibble(np->u.str.str[i * 2]) << 4) |
							ael_hex_nibble(np->u.str.str[i * 2 + 1]));
		}

		args->extra_ptr += bin_sz;
		args->mem += op_table[EXP_VOP_VALUE_BLOB].size;
		args->entry = &op_table[EXP_VOP_VALUE_BLOB];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_B64_BLOB: {
		op_value_blob* opbl = (op_value_blob*)op;
		uint32_t bin_sz =
				ael_b64_decode(np->u.str.str, np->u.str.sz, args->extra_ptr);

		opbl->base.code = EXP_VOP_VALUE_BLOB;
		opbl->value = args->extra_ptr;
		opbl->value_sz = bin_sz;

		args->extra_ptr += bin_sz;
		args->mem += op_table[EXP_VOP_VALUE_BLOB].size;
		args->entry = &op_table[EXP_VOP_VALUE_BLOB];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_GEO_LITERAL: {
		op_value_geo* opg = (op_value_geo*)op;
		const uint8_t* json = (const uint8_t*)np->u.str.str;
		uint32_t json_sz = np->u.str.sz;

		opg->base.code = EXP_VOP_VALUE_GEO;

		// Pack the GeoJSON as a msgpack-bin with AS_BYTES_GEOJSON
		// prefix into extra_data. op->contents must reference msgpack
		// bytes whose first content byte is the AS_BYTES_* type tag.
		as_packer pk = { .buffer = args->extra_ptr, .capacity = UINT32_MAX };

		opg->contents = args->extra_ptr;

		if (as_pack_str_with_type(&pk, AS_BYTES_GEOJSON, json, json_sz) != 0) {
			cf_warning(AS_EXP, "ael_build_node geojson - pack failed");
			return false;
		}

		opg->content_sz = (uint32_t)pk.offset;
		args->extra_ptr += pk.offset;

		// Parse the GeoJSON into cellid/region. Mirrors exp_geo_mp_to_op.
		uint64_t cellid;
		geo_region_t region;

		if (! as_geojson_parse(NULL, (const char*)json, json_sz, &cellid,
					&region)) {
			cf_warning(AS_EXP, "ael_build_node - invalid geojson");
			return false;
		}

		opg->compiled.region = NULL;

		if (region != NULL) {
			opg->compiled.type = GEO_REGION;
			opg->compiled.region = region;
			args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = opg;
		}
		else {
			opg->compiled.type = GEO_CELL;
			opg->compiled.cellid = cellid;
		}

		args->mem += op_table[EXP_VOP_VALUE_GEO].size;
		args->entry = &op_table[EXP_VOP_VALUE_GEO];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_BIN:
	case AST_BIN_REF: {
		const ast_node* bin_canon = (np->type == AST_BIN_REF)
				? ast_pool_at(pool, np->u.bin_ref.bin)
				: np;
		op_var* opv = (op_var*)op;
		uint32_t name_sz = bin_canon->u.bin.name_sz;
		const uint8_t* name = args->ael_buf + bin_canon->offset;

		opv->base.code = EXP_BIN;
		opv->base.rtype = ast_type_resolved(np->etype)
				? ast_etype_to_rtype(np->etype)
				: EXP_RTYPE_END;

		args->mem += op_table[EXP_BIN].size;

		if (opv->base.rtype == EXP_RTYPE_END) {
			cf_warning(AS_EXP,
					"ael_build_node - bin '%.*s' has unresolved type, use $.bin:T",
					name_sz, name);
			return false;
		}

		if ((args->entry = build_get_entry(opv->base.rtype)) == NULL) {
			cf_warning(AS_EXP, "ael_build_node bin - invalid type %d",
					opv->base.rtype);
			return false;
		}

		define_bin_name128(name128, name, name_sz,
				as_bin_name_need_memcpy(name, args->ael_buf + args->ael_src_sz));
		opv->idx = build_find_bin_idx(&args->bin_table, name128);

		cf_assert(opv->idx < args->bin_table.n_bins, AS_EXP,
				"ael_build_node - bin %.*s absent from bin table", name_sz, name);

		if (args->bins_info_r != NULL) {
			as_bin_info bin_info;

			memcpy(bin_info.name, &name128, sizeof(name128));
			bin_info.type = exp_rtype_to_particle_type(opv->base.rtype);
			cf_vector_append(args->bins_info_r, &bin_info);
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_META: {
		exp_op_code mc = np->u.meta.op_code;

		op->code = mc;
		args->mem += op_table[mc].size;

		if (mc == EXP_META_DIGEST_MOD) {
			op_meta_digest_modulo* opd = (op_meta_digest_modulo*)op;
			opd->mod = (int32_t)np->u.meta.param;
			args->exp->flags |= AS_EXP_HAS_DIGEST_MOD;
		}
		else if (mc == EXP_REC_KEY) {
			op_base_mem* opk = (op_base_mem*)op;

			// Derive the runtime key type from the narrowed etype (the
			// wire path's build_rec_key does the same from its rtype arg).
			// The post-parse pass has already rejected an unresolved key,
			// so etype is a single concrete key type here.
			exp_rtype rt = ast_rec_key_rtype(np);

			switch (rt) {
			case EXP_RTYPE_INT:
				opk->type = RT_INT;
				break;
			case EXP_RTYPE_STR:
				opk->type = RT_STR;
				break;
			case EXP_RTYPE_BLOB:
				opk->type = RT_BLOB;
				break;
			default:
				cf_warning(AS_EXP,
						"ael_build_node - rec key unresolved type %d", rt);
				return false;
			}

			args->exp->flags |= AS_EXP_HAS_REC_KEY;
			// Typed entry so the parent op sees INT/STR/BLOB, not END.
			args->entry = build_get_entry(rt);
			op->instr_end_ix = args->instr_ix;
			return true;
		}
		else {
			args->exp->flags |= AS_EXP_HAS_NON_DIGEST_META;
		}

		args->entry = &op_table[mc];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_LOOP_VAR: {
		op_var* opv = (op_var*)op;

		opv->base.code = EXP_VAR_BUILTIN;
		args->mem += op_table[EXP_VAR_BUILTIN].size;

		opv->idx = np->u.loop_var.builtin;
		opv->base.rtype = ast_loop_var_rtype(np);

		args->entry = build_get_entry(opv->base.rtype);

		if (args->entry == NULL) {
			cf_warning(AS_EXP, "ael_build_node - loop_var unknown type %d",
					opv->base.rtype);
			return false;
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_VAR: {
		op_var* opv = (op_var*)op;

		opv->base.code = EXP_VAR;
		args->mem += op_table[EXP_VAR].size;

		// Bins own var slots [0, n_bins); let-vars are numbered above them.
		opv->idx = np->u.var.var_idx + args->bin_table.n_bins;
		opv->base.rtype = ast_type_resolved(np->etype)
				? ast_etype_to_rtype(np->etype)
				: EXP_RTYPE_END;

		switch (opv->base.rtype) {
		case EXP_RTYPE_NIL:
			args->entry = &op_table[EXP_VOP_VALUE_NIL];
			break;
		case EXP_RTYPE_TRILEAN:
			args->entry = &op_table[EXP_VOP_VALUE_TRILEAN];
			break;
		default:
			args->entry = build_get_entry(opv->base.rtype);

			if (args->entry == NULL) {
				cf_warning(AS_EXP, "ael_build_node - var unknown type %d",
						opv->base.rtype);
				return false;
			}
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// N-ary logical.
	case AST_AND:
	case AST_OR:
	case AST_EXCLUSIVE: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;
		const op_table_entry* entry = &op_table[code];

		op->code = code;
		args->mem += entry->size;

		ast_ref e = np->u.list.head;

		for (uint32_t i = 0; i < np->u.list.count; i++) {
			if (! ael_build_node(args, e)) {
				return false;
			}

			if (i + 1 < np->u.list.count) {
				e = ast_pool_at(pool, e)->next;
			}
		}

		args->entry = entry;
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// N-ary math (circular list).
	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN: {
		if (! ast_type_resolved(np->etype)) {
			cf_warning(AS_EXP, "ael_build_node - unresolved type for %s",
					ast_node_table[np->type].name);
			return false;
		}

		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		ast_ref e = ast_nmath_head(pool, np);

		for (uint32_t i = 0; i < np->u.nmath.count; i++) {
			if (! ael_build_node(args, e)) {
				return false;
			}

			e = ast_pool_at(pool, e)->next;
		}

		args->entry =
				&op_table[np->etype == AST_ETYPE_FLOAT ? EXP_VOP_VALUE_FLOAT
													   : EXP_VOP_VALUE_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// N-ary int (linear list).
	case AST_BIT_AND:
	case AST_BIT_OR:
	case AST_BIT_XOR: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		ast_ref e = np->u.list.head;

		for (uint32_t i = 0; i < np->u.list.count; i++) {
			if (! ael_build_node(args, e)) {
				return false;
			}

			if (i + 1 < np->u.list.count) {
				e = ast_pool_at(pool, e)->next;
			}
		}

		args->entry = &op_table[EXP_VOP_VALUE_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Left-fold (SUB, DIV) → emitted as n-ary.
	case AST_SUB:
	case AST_DIV: {
		if (! ast_type_resolved(np->etype)) {
			cf_warning(AS_EXP, "ael_build_node - unresolved type for %s",
					ast_node_table[np->type].name);
			return false;
		}

		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_left_fold(args, ref, np->type)) {
			return false;
		}

		args->entry =
				&op_table[np->etype == AST_ETYPE_FLOAT ? EXP_VOP_VALUE_FLOAT
													   : EXP_VOP_VALUE_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Binary comparison.
	case AST_CMP_EQ:
	case AST_CMP_NE:
	case AST_CMP_GT:
	case AST_CMP_GE:
	case AST_CMP_LT:
	case AST_CMP_LE:
	case AST_CMP_IN:
	case AST_CMP_GEO: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;
		const op_table_entry* entry = &op_table[code];

		op->code = code;
		args->mem += entry->size;

		if (! ael_build_node(args, np->u.binary.left)) {
			return false;
		}

		exp_rtype ltype = args->entry->r_type;

		if (! ael_build_node(args, np->u.binary.right)) {
			return false;
		}

		exp_rtype rtype = args->entry->r_type;

		// Mirror wire-format type checks. END marks "type not pinned at
		// compile" (bins, var refs) — runtime catches those.
		if (code == EXP_IN_LIST) {
			// in(): right side must be a LIST; left can be any type.
			if (rtype != EXP_RTYPE_END && rtype != EXP_RTYPE_LIST) {
				cf_warning(AS_EXP,
						"ael_build_node - in() right side must be list, got %u (%s)",
						rtype, exp_rtype_to_str(rtype));
				return false;
			}
		}
		else {
			// Ordering / equality: operand types must match.
			if (ltype != EXP_RTYPE_END && rtype != EXP_RTYPE_END &&
					ltype != rtype) {
				if (pool->diags != NULL) {
					ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
							np->sz, "comparison operand types do not match");
				}

				return false;
			}

			// exp_eval_compare dispatches on this captured operand type, mirroring
			// the wire-path build_compare. EXP_IN_LIST shares this case but is
			// not a comparison op, so it is excluded above.
			((op_base_mem*)op)->rtype = ltype != EXP_RTYPE_END ? ltype : rtype;
		}

		args->entry = entry;
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Binary math (mod, pow, shifts).
	case AST_MOD: {
		op->code = EXP_MOD;
		args->mem += op_table[EXP_MOD].size;

		if (! ael_build_node(args, np->u.binary.left) ||
				! ael_build_node(args, np->u.binary.right)) {
			return false;
		}

		args->entry = &op_table[EXP_MOD];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_POW: {
		op->code = EXP_POW;
		args->mem += op_table[EXP_POW].size;

		if (! ael_build_node(args, np->u.binary.left) ||
				! ael_build_node(args, np->u.binary.right)) {
			return false;
		}

		args->entry = &op_table[EXP_POW];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_LSHIFT:
	case AST_RSHIFT_ARITH:
	case AST_RSHIFT_LOGIC: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_node(args, np->u.binary.left) ||
				! ael_build_node(args, np->u.binary.right)) {
			return false;
		}

		args->entry = &op_table[code];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Unary ops.
	case AST_NOT: {
		op->code = EXP_NOT;
		args->mem += op_table[EXP_NOT].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[EXP_NOT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_BIT_NOT: {
		op->code = EXP_INT_NOT;
		args->mem += op_table[EXP_INT_NOT].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[EXP_INT_NOT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_PATH_FUNC_CAST_INT: {
		op->code = EXP_TO_INT;
		args->mem += op_table[EXP_TO_INT].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[EXP_TO_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_PATH_FUNC_CAST_FLOAT: {
		op->code = EXP_TO_FLOAT;
		args->mem += op_table[EXP_TO_FLOAT].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[EXP_TO_FLOAT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_PATH_FUNC_CAST_STRING: {
		op->code = EXP_TO_STRING;
		args->mem += op_table[EXP_TO_STRING].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[EXP_TO_STRING];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// 1-arg functions.
	case AST_FUNC_ABS: {
		op->code = EXP_ABS;
		args->mem += op_table[EXP_ABS].size;

		if (! ael_build_node(args, np->u.func1.arg)) {
			return false;
		}

		// ABS result type matches arg type.
		args->entry = &op_table[args->entry->r_type == EXP_RTYPE_FLOAT
						? EXP_VOP_VALUE_FLOAT
						: EXP_VOP_VALUE_INT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_CEIL: {
		op->code = EXP_CEIL;
		args->mem += op_table[EXP_CEIL].size;

		if (! ael_build_node(args, np->u.func1.arg)) {
			return false;
		}

		args->entry = &op_table[EXP_CEIL];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_FLOOR: {
		op->code = EXP_FLOOR;
		args->mem += op_table[EXP_FLOOR].size;

		if (! ael_build_node(args, np->u.func1.arg)) {
			return false;
		}

		args->entry = &op_table[EXP_FLOOR];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_COUNT_ONE_BITS: {
		op->code = EXP_INT_COUNT;
		args->mem += op_table[EXP_INT_COUNT].size;

		if (! ael_build_node(args, np->u.func1.arg)) {
			return false;
		}

		args->entry = &op_table[EXP_INT_COUNT];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// 2-arg functions.
	case AST_FUNC_LOG: {
		op->code = EXP_LOG;
		args->mem += op_table[EXP_LOG].size;

		if (! ael_build_node(args, np->u.func2.arg1) ||
				! ael_build_node(args, np->u.func2.arg2)) {
			return false;
		}

		args->entry = &op_table[EXP_LOG];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_POW: {
		op->code = EXP_POW;
		args->mem += op_table[EXP_POW].size;

		if (! ael_build_node(args, np->u.func2.arg1) ||
				! ael_build_node(args, np->u.func2.arg2)) {
			return false;
		}

		args->entry = &op_table[EXP_POW];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_FIND_BIT_LEFT: {
		op->code = EXP_INT_LSCAN;
		args->mem += op_table[EXP_INT_LSCAN].size;

		if (! ael_build_node(args, np->u.func2.arg1) ||
				! ael_build_node(args, np->u.func2.arg2)) {
			return false;
		}

		args->entry = &op_table[EXP_INT_LSCAN];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_FUNC_FIND_BIT_RIGHT: {
		op->code = EXP_INT_RSCAN;
		args->mem += op_table[EXP_INT_RSCAN].size;

		if (! ael_build_node(args, np->u.func2.arg1) ||
				! ael_build_node(args, np->u.func2.arg2)) {
			return false;
		}

		args->entry = &op_table[EXP_INT_RSCAN];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Collection literals.
	case AST_LIST: {
		op_value_blob* opv = (op_value_blob*)op;

		opv->base.code = EXP_VOP_VALUE_LIST;
		args->mem += op_table[EXP_QUOTE].size;

		uint8_t* start = args->extra_ptr;
		as_packer pk = {
			.buffer = start,
			.capacity = UINT32_MAX // extra_data was pre-sized
		};

		if (ael_pack_literal(&pk, pool, ref) != 0) {
			cf_warning(AS_EXP, "ael_build_node list - pack failed");
			return false;
		}

		opv->value = start;
		opv->value_sz = pk.offset;
		args->extra_ptr += pk.offset;
		args->entry = &op_table[EXP_QUOTE];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_MAP: {
		op_value_blob* opv = (op_value_blob*)op;

		// Map literals are opaque msgpack to the Exp VM — emit as MSGPACK
		// so exp_eval_value handles them via the same code path as lists.
		opv->base.code = EXP_VOP_VALUE_MSGPACK;
		args->mem += op_table[EXP_VOP_VALUE_MSGPACK].size;

		uint8_t* start = args->extra_ptr;
		as_packer pk = { .buffer = start, .capacity = UINT32_MAX };

		if (ael_pack_literal(&pk, pool, ref) != 0) {
			cf_warning(AS_EXP, "ael_build_node map - pack failed");
			return false;
		}

		opv->value = start;
		opv->value_sz = pk.offset;
		args->extra_ptr += pk.offset;
		// Entry carries r_type = EXP_RTYPE_MAP so the caller (e.g.,
		// build_set_expected_particle_type) sets the as_exp's expected
		// particle type to MAP. The op's `code` field above stays
		// MSGPACK for the runtime exp_eval_value dispatch.
		args->entry = &op_table[EXP_VOP_VALUE_MAP];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// WITH → LET.
	case AST_LET: {
		op_let* opl = (op_let*)op;
		uint32_t defs_count = np->u.list.count - 2; // exclude let_scope + body

		opl->base.code = EXP_LET;
		args->mem += op_table[EXP_LET].size;

		define_deferred_array(entries, var_entry, defs_count);
		var_scope scope = { .parent = args->current, .entries = entries };

		args->current = &scope;
		opl->var_idx = args->var_idx;
		opl->n_vars = defs_count;

		// Skip let_scope at head.
		ast_ref def = ast_pool_at(pool, np->u.list.head)->next;

		for (uint32_t i = 0; i < defs_count; i++) {
			ast_node* dp = ast_pool_at(pool, def);

			scope.entries[i].name = (const uint8_t*)args->ael_src + dp->offset;
			scope.entries[i].name_sz = dp->u.var_def.name_sz;
			scope.entries[i].idx = args->var_idx++;
			scope.entries[i].r_type = EXP_RTYPE_END;
			scope.n_entries++;

			if (! ael_build_node(args, dp->u.var_def.value)) {
				return false;
			}

			scope.entries[i].r_type = args->entry->r_type;
			def = dp->next;
		}

		// Build body.
		if (! ael_build_node(args, np->u.list.tail)) {
			return false;
		}

		opl->base.rtype = args->entry->r_type;

		if (args->max_var_idx < args->var_idx) {
			args->max_var_idx = args->var_idx;
		}

		args->current = scope.parent;
		args->var_idx -= scope.n_entries;

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// WHEN → COND.
	case AST_WHEN: {
		op_cond* opc = (op_cond*)op;
		uint32_t mappings_count = np->u.list.count - 1;

		opc->base.code = EXP_COND;
		opc->case_count = mappings_count;
		args->mem += op_table[EXP_COND].size;

		exp_rtype result_type = EXP_RTYPE_END;

		ast_ref m = np->u.list.head;

		for (uint32_t i = 0; i < mappings_count; i++) {
			ast_node* mp = ast_pool_at(pool, m);

			// Emit condition.
			if (! ael_build_node(args, mp->u.when_case.cond)) {
				return false;
			}

			// Mirror wire-format build_cond's TRILEAN check on each
			// case condition. END marks "type not pinned" — runtime
			// catches those.
			if (args->entry->r_type != EXP_RTYPE_END &&
					args->entry->r_type != EXP_RTYPE_TRILEAN) {
				cf_warning(AS_EXP,
						"ael_build_node - when case %u condition type %u (%s) is not boolean",
						i + 1, args->entry->r_type,
						exp_rtype_to_str(args->entry->r_type));
				return false;
			}

			// Emit COND_CASE marker - attributed to the when-case's source.
			op_base_mem* op_case = (op_base_mem*)args->mem;
			op_case->code = EXP_VOP_COND_CASE;
			args->instr_ix++;
			ael_src_map_record(args, mp);
			args->mem += op_table[EXP_VOP_COND_CASE].size;

			// Emit result.
			if (! ael_build_node(args, mp->u.when_case.result)) {
				return false;
			}

			if (args->entry->code != EXP_UNK) {
				if (result_type == EXP_RTYPE_END) {
					result_type = args->entry->r_type;
				}
				else if (args->entry->r_type != result_type) {
					cf_warning(AS_EXP,
							"ael_build_node - when branch %u type %d (%s) mismatches expected %d (%s)",
							i + 1, args->entry->r_type,
							exp_rtype_to_str(args->entry->r_type), result_type,
							exp_rtype_to_str(result_type));
					return false;
				}
			}

			op_case->instr_end_ix = args->instr_ix;
			m = mp->next;
		}

		// Emit default.
		if (! ael_build_node(args, np->u.list.tail)) {
			return false;
		}

		if (result_type == EXP_RTYPE_END) {
			result_type = args->entry->r_type;
		}
		else if (args->entry->code != EXP_UNK &&
				args->entry->r_type != result_type) {
			cf_warning(AS_EXP,
					"ael_build_node - when default type %d (%s) mismatches expected %d (%s)",
					args->entry->r_type, exp_rtype_to_str(args->entry->r_type),
					result_type, exp_rtype_to_str(result_type));
			return false;
		}

		if (result_type != EXP_RTYPE_END) {
			const op_table_entry* re = build_get_entry(result_type);

			if (re != NULL) {
				args->entry = re;
			}
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	// Path access → CALL.
	case AST_PATH_CALL: {
		ast_node* op_node = ast_pool_at(pool, np->u.call.call_op);
		op_call* opc = (op_call*)op;

		// SELECT/MODIFY/PSELECT_REMOVE compute their rtype from the
		// bin's etype downstream — skip the resolved-etype check for
		// them. All other path-calls (CDT GET, BIT, HLL, value-recv
		// CDT) derive the wire rtype from `np->etype`; reject if that
		// isn't a single resolved bit.
		if (op_node->type != AST_PATH_FUNC_SELECT &&
				op_node->type != AST_PATH_FUNC_MODIFY &&
				op_node->type != AST_PATH_FUNC_PSELECT_REMOVE &&
				! ast_type_resolved(np->etype)) {
			if (pool->diags != NULL) {
				ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
						np->sz, "cannot infer type — pin with :T");
			}

			return false;
		}

		// Pack blob into extra_data.
		uint8_t* blob_start = args->extra_ptr;

		// Value-recv path-call: u.call.ctx is the receiver value-
		// expression (not an AST_PATH_CTX list). Wire shape:
		//   [CALL, rtype, stype, [op_code, args...], <receiver>]
		// Used by EXP_CALL_BITS unconditionally, and by EXP_CALL_CDT
		// when the parser built a value-recv form (`(expr).cdtFn(...)`).
		if (np->u.call.stype == EXP_CALL_BITS ||
				ast_pool_at(pool, np->u.call.ctx)->type != AST_PATH_CTX) {
			opc->base.code = EXP_CALL;
			args->mem += op_table[EXP_CALL].size;
			opc->eval_count = 0;
			opc->system_type = np->u.call.stype;
			opc->type = ast_etype_to_rtype(np->etype);

			// ael_pack_call_blob inserts call_eval_token slots for any
			// sub-expression params (AST_BIN, AST_VAR, etc.) and builds
			// them as sub-instructions — matches what parse_op_call
			// does for the codegen wire.
			as_packer pk = {
				.buffer = blob_start, .offset = 0, .capacity = UINT32_MAX
			};

			opc->vecs[0].buf = blob_start;
			opc->vecs[0].buf_sz = 0;
			opc->n_vecs = 1;

			call_blob_ctx cb = {
				.build = args, .opc = opc, .pk = &pk, .allow_inline = true
			};

			if (ael_pack_call_blob(&cb, pool, args->ael_src, op_node) != 0) {
				return false;
			}

			if (! ael_build_node(args, np->u.call.ctx)) {
				return false;
			}

			args->entry = build_get_entry(opc->type);

			if (args->entry == NULL) {
				cf_warning(AS_EXP, "build_path_call - unresolved rtype %u",
						opc->type);
				return false;
			}

			op->instr_end_ix = args->instr_ix;
			return true;
		}

		ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);
		ast_ref bin_ref = cxn->u.ctx_list.head;
		ast_ref ctx_seg_head = ast_pool_at(pool, bin_ref)->next;
		as_packer pk = {
			.buffer = blob_start, .offset = 0, .capacity = UINT32_MAX
		};

		opc->base.code = EXP_CALL;
		args->mem += op_table[EXP_CALL].size;
		opc->eval_count = 0;
		opc->system_type = np->u.call.stype;

		blob_pack_ctx build_bc = { .pk = &pk, .pool = pool, .args = args };

		if (op_node->type == AST_PATH_FUNC_SELECT ||
				op_node->type == AST_PATH_FUNC_MODIFY ||
				op_node->type == AST_PATH_FUNC_PSELECT_REMOVE) {
			// Bin etype is resolved by the post-parse check; modify and
			// wildcard-remove return the (mutated) container, so the call
			// rtype mirrors the bin. SELECT TREE matches bin shape too;
			// LEAF_* always returns LIST.
			ast_node* binp = ast_pool_at(pool, bin_ref);
			const ast_node* bin_canon = (binp->type == AST_BIN_REF)
					? ast_pool_at(pool, binp->u.bin_ref.bin)
					: binp;
			exp_rtype bin_rtype = ast_type_resolved(bin_canon->etype)
					? ast_etype_to_rtype(bin_canon->etype)
					: EXP_RTYPE_LIST;

			if (op_node->type == AST_PATH_FUNC_SELECT) {
				switch (op_node->u.modify.sel_type) {
				case AS_CDT_SELECT_TREE:
					opc->type = bin_rtype;
					break;
				case AS_CDT_SELECT_COUNT:
					opc->type = EXP_RTYPE_INT;
					break;
				case AS_CDT_SELECT_EXISTS:
					opc->type = EXP_RTYPE_TRILEAN;
					break;
				default:
					opc->type = EXP_RTYPE_LIST;
					break;
				}
			}
			else {
				opc->type = bin_rtype;
			}

			if (ael_pack_select_blob(&build_bc, ctx_seg_head,
						np->u.call.call_op) != 0) {
				return false;
			}

			// SELECT path: single static vec (no sub-expressions).
			opc->vecs[0].buf = blob_start;
			opc->vecs[0].buf_sz = pk.offset;
			opc->n_vecs = 1;
			args->extra_ptr += pk.offset;
		}
		else {
			uint32_t ctx_seg_count = 0;

			for (ast_ref er = ctx_seg_head; er != AST_REF_NULL;
					er = ast_pool_at(pool, er)->next) {
				ctx_seg_count++;
			}

			opc->type = ast_etype_to_rtype(np->etype);

			// Seed vec[0] before any packing; ael_pack_call_blob extends
			// it (and may split into more vecs if a future CDT op grows
			// non-literal params — today CDT operands are parser-enforced
			// literals, so n_vecs stays 1).
			opc->vecs[0].buf = blob_start;
			opc->vecs[0].buf_sz = 0;
			opc->n_vecs = 1;

			if (ctx_seg_count > 0) {
				if (ael_pack_ctx(&pk, pool, args->ael_src, ctx_seg_head,
							ctx_seg_count) != 0) {
					return false;
				}
			}

			// CDT path-ctx: inline disabled (CTX wrapper precedes the
			// inner blob in extra_data; splitting would require multi-
			// vec from the start). allow_inline left false.
			call_blob_ctx cb = { .build = args, .opc = opc, .pk = &pk };

			if (ael_pack_call_blob(&cb, pool, args->ael_src, op_node) != 0) {
				return false;
			}
		}

		if (! ael_build_node(args, bin_ref)) {
			return false;
		}

		args->entry = build_get_entry(opc->type);

		if (args->entry == NULL) {
			cf_warning(AS_EXP, "build_path_call - unresolved rtype %u",
					opc->type);
			return false;
		}

		op->instr_end_ix = args->instr_ix;
		return true;
	}

	case AST_BIN_TYPE:
	case AST_BIN_EXISTS: {
		exp_op_code code = (np->type == AST_BIN_TYPE) ? EXP_BIN_TYPE
													  : EXP_BIN_EXISTS;
		op_var* opv = (op_var*)op;
		uint32_t name_sz = np->u.bin_type.name_sz;
		const uint8_t* name = args->ael_buf + np->offset;

		opv->base.code = code;
		opv->base.rtype =
				op_table[code].r_type; // INT for bin_type, TRILEAN for bin_exists

		define_bin_name128(name128, name, name_sz,
				as_bin_name_need_memcpy(name, args->ael_buf + args->ael_src_sz));
		opv->idx = build_find_bin_idx(&args->bin_table, name128);

		cf_assert(opv->idx < args->bin_table.n_bins, AS_EXP,
				"ael_build_node - bin %.*s absent from bin table", name_sz, name);

		args->mem += op_table[code].size;
		args->entry = &op_table[code];
		op->instr_end_ix = args->instr_ix;
		return true;
	}

	default:
		cf_warning(AS_EXP, "ael_build_node - unsupported node type %d", np->type);
		return false;
	}
}

//==========================================================
// Local helpers - AEL msgpack literal packing.
//

// Used by collection literals (AST_LIST, AST_MAP) which must
// be msgpack-encoded into extra_data. Works in two modes:
// - pk->buffer == NULL: sizing only (counts bytes)
// - pk->buffer != NULL: actual packing

static int
ael_pack_literal(as_packer* pk, ast_pool* pool, ast_ref ref)
{
	if (ref == AST_REF_NULL) {
		return as_pack_nil(pk);
	}

	ast_node* np = ast_pool_at(pool, ref);
	int rc;

	switch (np->type) {
	case AST_NIL:
		return as_pack_nil(pk);
	case AST_INF:
		return as_pack_cmp_inf(pk);
	case AST_WILDCARD:
		return as_pack_cmp_wildcard(pk);
	case AST_BOOL:
		return as_pack_bool(pk, np->u.bval);
	case AST_INT:
		return as_pack_int64(pk, np->u.ival);
	case AST_FLOAT:
		return as_pack_double(pk, np->u.fval);
	case AST_ZERO:
		if (np->etype == AST_ETYPE_FLOAT) {
			return as_pack_double(pk, 0.0);
		}
		return as_pack_int64(pk, 0);
	case AST_STRING: {
		if (! np->u.str.has_escape) {
			return as_pack_str_with_type(pk, AS_BYTES_STRING,
					(const uint8_t*)np->u.str.str, np->u.str.sz);
		}

		uint32_t decoded_sz = ael_string_decoded_sz(np->u.str.str, np->u.str.sz);
		uint8_t* dst;

		if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_STRING, decoded_sz,
					&dst)) {
			return -1;
		}

		if (dst != NULL) {
			ael_string_decode(np->u.str.str, np->u.str.sz, dst);
		}

		return 0;
	}
	case AST_BLOB: {
		uint32_t bin_sz = np->u.str.sz / 2;
		uint8_t* dst;

		if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
			return -1;
		}

		if (dst != NULL) {
			for (uint32_t i = 0; i < bin_sz; i++) {
				dst[i] = (uint8_t)((ael_hex_nibble(np->u.str.str[i * 2]) << 4) |
						ael_hex_nibble(np->u.str.str[i * 2 + 1]));
			}
		}

		return 0;
	}
	case AST_B64_BLOB: {
		uint32_t bin_sz = ael_b64_decoded_sz(np->u.str.str, np->u.str.sz);
		uint8_t* dst;

		if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
			return -1;
		}

		if (dst != NULL) {
			ael_b64_decode(np->u.str.str, np->u.str.sz, dst);
		}

		return 0;
	}
	case AST_LIST: {
		uint32_t ele_count = np->u.list.count;
		// Lists default to plain/unordered; :ORDERED opts in to an
		// ORDERED-flagged list (validated, not sorted, below).
		bool ordered = np->u.list.order_override == AEL_ORDER_ORDERED;
		uint32_t start = pk->offset;

		rc = as_pack_list_header(pk, ele_count + (ordered ? 1 : 0));

		if (rc != 0) {
			return rc;
		}

		if (ordered) {
			rc = as_pack_ext_header(pk, 0, AS_PACKED_LIST_FLAG_ORDERED);

			if (rc != 0) {
				return rc;
			}
		}

		ast_ref e = np->u.list.head;

		for (uint32_t i = 0; i < ele_count; i++) {
			rc = ael_pack_literal(pk, pool, e);

			if (rc != 0) {
				return rc;
			}

			if (i + 1 < ele_count) {
				e = ast_pool_at(pool, e)->next;
			}
		}

		if (ordered && pk->buffer != NULL && ele_count >= 2 &&
				! list_buf_check_ordered(pk->buffer + start, pk->offset - start)) {
			if (pool->diags != NULL) {
				ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
						np->sz,
						":ORDERED list literal is not in ascending order");
			}

			return -1;
		}

		return 0;
	}
	case AST_MAP: {
		uint32_t ele_count = np->u.list.count;
		// Maps default to ORDERED (K_ORDERED + key-sort, below); :UNORDERED
		// opts out to a plain map (smaller, but uncomparable).
		bool ordered = np->u.list.order_override != AEL_ORDER_UNORDERED;
		uint32_t start = pk->offset;

		rc = as_pack_map_header(pk, ele_count + (ordered ? 1 : 0));

		if (rc != 0) {
			return rc;
		}

		if (ordered) {
			rc = as_pack_ext_header(pk, 0, AS_PACKED_MAP_FLAG_K_ORDERED);

			if (rc != 0) {
				return rc;
			}

			rc = as_pack_nil(pk); // ext pair value

			if (rc != 0) {
				return rc;
			}
		}

		ast_ref key = np->u.list.head;

		for (uint32_t i = 0; i < ele_count; i++) {
			ast_node* kp = ast_pool_at(pool, key);
			ast_ref val = kp->next;

			rc = ael_pack_literal(pk, pool, key);

			if (rc != 0) {
				return rc;
			}

			rc = ael_pack_literal(pk, pool, val);

			if (rc != 0) {
				return rc;
			}

			key = (i + 1 < ele_count) ? ast_map_literal_next_key(pool, key)
									  : AST_REF_NULL;
		}

		if (ordered && pk->buffer != NULL && ele_count >= 2 &&
				! map_buf_sort_in_place(pk->buffer + start, pk->offset - start)) {
			if (pool->diags != NULL) {
				ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
						np->sz, "map literal has duplicate keys");
			}

			return -1;
		}

		return 0;
	}
	default:
		cf_warning(AS_EXP, "ael_pack_literal - unsupported type %d", np->type);
		return -1;
	}
}

// Compute msgpack size of a literal AST subtree.
static uint32_t
ael_literal_pack_sz(ast_pool* pool, ast_ref ref)
{
	as_packer sizer = { 0 };

	if (ael_pack_literal(&sizer, pool, ref) != 0) {
		return 0;
	}

	return sizer.offset;
}

//----------------------------------------------------------
// AEL path call helpers -- compute CDT blob msgpack size.
//

// Pack the SELECT op blob (no ctx wrapper). Wire shape:
//   [SELECT, [ (ctx_type, value), ... ], flags [, apply_expr]]
// Wildcard segs emit (AS_CDT_CTX_EXP, true) for bare `*` or
// (AS_CDT_CTX_EXP, filter_expr_msgpack) for `*[?(filter)]`.
// AST_PATH_FUNC_MODIFY adds an apply blob; AST_PATH_FUNC_PSELECT_REMOVE
// synthesizes a one-op `[EXP_RESULT_REMOVE]` apply blob.
static int
ael_pack_select_blob(blob_pack_ctx* bc, ast_ref ctx_seg_head, ast_ref pf_ref)
{
	as_packer* pk = bc->pk;
	ast_pool* pool = bc->pool;
	ast_node* pfp = ast_pool_at(pool, pf_ref);
	const char* ael_buf = bc->args != NULL
			? bc->args->ael_src
			: (bc->size_ctx != NULL ? bc->size_ctx->ael_src : NULL);

	uint32_t seg_count = 0;

	for (ast_ref er = ctx_seg_head; er != AST_REF_NULL;
			er = ast_pool_at(pool, er)->next) {
		seg_count++;
	}

	as_cdt_select_flags sel_type;
	bool is_apply;
	ast_ref apply_expr = AST_REF_NULL;

	switch (pfp->type) {
	case AST_PATH_FUNC_SELECT:
		sel_type = pfp->u.modify.sel_type;
		is_apply = false;
		break;
	case AST_PATH_FUNC_MODIFY:
		sel_type = AS_CDT_SELECT_APPLY;
		is_apply = true;
		apply_expr = pfp->u.modify.value;
		break;
	default: // AST_PATH_FUNC_PSELECT_REMOVE
		sel_type = AS_CDT_SELECT_APPLY;
		is_apply = true;
		break;
	}

	uint32_t blob_items = is_apply ? 4 : 3;
	int rc = as_pack_list_header(pk, blob_items);

	if (rc != 0) {
		return rc;
	}

	rc = as_pack_uint64(pk, AS_CDT_OP_SELECT);

	if (rc != 0) {
		return rc;
	}

	rc = as_pack_list_header(pk, seg_count * 2);

	if (rc != 0) {
		return rc;
	}

	for (ast_ref er = ctx_seg_head; er != AST_REF_NULL;
			er = ast_pool_at(pool, er)->next) {
		rc = ael_pack_ctx_seg_pair(pk, pool, ael_buf, er);

		if (rc != 0) {
			return rc;
		}
	}

	int64_t flags_int = (int64_t)sel_type |
			(int64_t)ael_prop_to_select_flag(pfp->u.modify.props);

	rc = as_pack_int64(pk, flags_int);

	if (rc != 0) {
		return rc;
	}

	if (is_apply) {
		if (apply_expr != AST_REF_NULL) {
			rc = ael_codegen_emit(pk, pool, ael_buf, apply_expr);
		}
		else {
			// PSELECT_REMOVE — synthetic [EXP_RESULT_REMOVE].
			rc = as_pack_list_header(pk, 1);

			if (rc != 0) {
				return rc;
			}

			rc = as_pack_uint64(pk, EXP_RESULT_REMOVE);
		}
	}

	return rc;
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
