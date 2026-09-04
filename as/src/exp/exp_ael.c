/*
 * exp_ael.c
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

#include "exp/exp_ael.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "cf_defer.h"
#include "dynbuf.h"
#include "enhanced_alloc.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "exp/ael_actions.h"
#include "exp/ael_codegen.h"
#include "exp/ael_diag.h"
#include "exp/ael_emit.h"
#include "exp/ael_parse.h"
#include "exp/ael_string.h"
#include "exp/ast.h"
#include "exp/exp_rt.h"
#include "exp/exp_wire.h"
#include "geospatial/geospatial.h"

//==========================================================
// Type aliases.
//
// Ahead of the typedefs below, unlike exp.c: call_blob_ctx names op_call.
//

typedef struct exp_op_base_mem_s op_base_mem;
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

typedef exp_op_table_entry op_table_entry;

#define op_table exp_op_table

//==========================================================
// Typedefs & constants.
//

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
} ael_build_args;

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

//==========================================================
// Forward declarations.
//

static void ael_offset_line_col(const char* src, uint32_t sz, uint32_t offset,
		uint32_t* line_r, uint32_t* col_r);
static bool ael_register_bins(build_bin_table* t, ast_pool* pool,
		const uint8_t* ael_str, uint32_t ael_sz, ast_ref head);
static void ael_fill_bins_info(cf_vector* bins_info_r, ast_pool* pool,
		const uint8_t* ael_str, uint32_t ael_sz, ast_ref head);
static void ael_build_err_record_diags(const uint8_t* src, uint32_t src_sz,
		const ael_diag_list* diags, const char* fallback_msg);
static bool ael_count_sz(ael_size_ctx* ctx, ast_ref ref);
static bool ael_build_left_fold(ael_build_args* args, ast_ref ref,
		ast_node_t type);
static bool ael_build_node(ael_build_args* args, ast_ref ref);
static bool ast_is_blob_literal(ast_node_t type);

static int ael_pack_call_blob(call_blob_ctx* cb, ast_pool* pool,
		const char* ael_src, ast_node* op_node);

static bool ael_call_params_all_literal(ast_pool* pool, ast_ref param_head);

// AEL msgpack literal packing.
static int ael_pack_literal(as_packer* pk, ast_pool* pool, ast_ref ref);
static uint32_t ael_literal_pack_sz(ast_pool* pool, ast_ref ref);

//==========================================================
// Inlines & macros.
//

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

//==========================================================
// Public API.
//

as_exp*
exp_build_internal_ael(const uint8_t* ael_str, uint32_t ael_sz,
		cf_vector* bins_info_r)
{
	if (ael_sz > EXP_MAX_AEL_SRC_SIZE) {
		cf_warning(AS_EXP,
				"exp_build_internal_ael - AEL source size %u exceeds limit of %u bytes",
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

	// The invalid-bin-name error detail is staged by the registration itself.
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
	if (args.entry == NULL ||
			! exp_build_set_expected_type(args.exp, args.entry)) {
		ael_build_err_record(ael_str, ael_sz, false, 0, 0,
				"invalid expression result type");
		as_exp_destroy(args.exp);
		return NULL;
	}

	// At the one success point, so a failed build leaves nothing behind.
	if (bins_info_r != NULL) {
		ael_fill_bins_info(bins_info_r, &pool, ael_str, ael_sz, pr.bin_root);
		ael_fill_bins_info(bins_info_r, &pool, ael_str, ael_sz,
				pr.local_bin_root);
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

//==========================================================
// Local helpers - build AEL direct.
//

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

		// EXP_META_DIGEST_MOD's param lives in the op struct -- no extra_sz.
		ael_size_op(ctx, mc);

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

	// Binary ops. SUB and DIV emit as n-ary left folds and the builder splits
	// them out for it, but a fold of two is two operands, so sizing is the same.
	case AST_SUB:
	case AST_DIV:
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
		if (ast_call_is_value_recv(pool, np)) {
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

		// .select() / .modify() / wildcard-.remove() — distinct blob
		// shape (SELECT op, no ctx wrapper).
		if (ast_is_select_family(op_node->type)) {
			ael_select_shape_t shape =
					ael_select_shape(pool, np->u.call.call_op, bin_ref);

			if (ael_pack_select_op_blob(&sizer, pool, ctx->ael_src, ctx_seg_head,
						ast_ctx_seg_count(cxn), np->u.call.call_op, &shape) != 0) {
				return false;
			}

			ctx->extra_sz += sizer.offset;

			return ael_count_sz(ctx, bin_ref);
		}

		uint32_t ctx_seg_count = ast_ctx_seg_count(cxn);

		if (ctx_seg_count > 0) {
			if (ael_pack_ctx(&sizer, pool, ctx->ael_src, ctx_seg_head,
						ctx_seg_count, ael_ctx_path_flags(op_node)) != 0) {
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
		// AST_LIST is deliberately absent: the wire decoder
		// (parse_op_call) gives a list arg its own eval-token +
		// EXP_VOP_VALUE_LIST sub-instruction, so the AST build path takes
		// the sub-expression route for lists to produce the identical
		// runtime shape (see the AelListParam ASSERT_DSL_EQ tests).
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
// True when every param in the chain is a static blob literal — i.e. the
// packed call blob is one contiguous static vec (n_vecs == 1, no eval-token
// vecs; the trailing flag args are always static).
static bool
ael_call_params_all_literal(ast_pool* pool, ast_ref param_head)
{
	for (ast_ref e = param_head; e != AST_REF_NULL;
			e = ast_pool_at(pool, e)->next) {
		if (! ast_is_blob_literal(ast_pool_at(pool, e)->type)) {
			return false;
		}
	}

	return true;
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

	ael_cdt_flag_shape_t fs =
			ael_cdt_flag_shape(cdt_op, props_flags, op_node->type);

	bool inline_mode = is_build && ast_cdt_op_blob_inline(op_node);

	// BUILD inline mode: redirect pk to write into the op_call tail, which
	// begins one past the last vec.
	if (inline_mode) {
		uint8_t* inline_buf = (uint8_t*)&cb->opc->vecs[cb->opc->n_vecs];

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

	int rc = as_pack_list_header(pk, 1 + param_count + fs.extra);
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

	if (ael_emit_cdt_trailing_flags(pk, &fs) != 0) {
		return -1;
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
		// no-sub-expression case (all params literal => a single static
		// vec). Multi-vec inlining would corrupt the data bytes: BUILD's
		// vec[cur_idx].buf writes for cur_idx >= n_vecs overlap with the
		// inline payload region (vecs[n_vecs..19] memory).
		uint32_t inline_cap = ael_call_params_all_literal(pool, param_head)
				? (EXP_CALL_MAX_VEC_IDX + 1) * sizeof(op_vec)
				: 0;

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

// AEL packs its own literals, so the wire path's build_check_cdt_literal()
// never sees them - but the eval sinks read this flag whatever the source.
static bool
ael_literal_has_nonstorage(const uint8_t* buf, uint32_t buf_sz)
{
	msgpack_in mp = { .buf = buf, .buf_sz = buf_sz };

	return msgpack_sz(&mp) != 0 && mp.has_nonstorage;
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
		break;

	case AST_UNKNOWN:
		op->code = EXP_UNK;
		args->mem += op_table[EXP_UNK].size;
		args->entry = &op_table[EXP_UNK];
		break;

	case AST_INT: {
		op_value_int* opi = (op_value_int*)op;

		opi->base.code = EXP_VOP_VALUE_INT;
		opi->value = np->u.ival;
		args->mem += op_table[EXP_VOP_VALUE_INT].size;
		args->entry = &op_table[EXP_VOP_VALUE_INT];
		break;
	}

	case AST_FLOAT: {
		op_value_float* opf = (op_value_float*)op;

		opf->base.code = EXP_VOP_VALUE_FLOAT;
		opf->value = np->u.fval;
		args->mem += op_table[EXP_VOP_VALUE_FLOAT].size;
		args->entry = &op_table[EXP_VOP_VALUE_FLOAT];
		break;
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

		break;
	}

	case AST_BOOL: {
		op_value_bool* opb = (op_value_bool*)op;

		opb->base.code = EXP_VOP_VALUE_BOOL;
		opb->value = np->u.bval;
		args->mem += op_table[EXP_VOP_VALUE_BOOL].size;
		args->entry = &op_table[EXP_VOP_VALUE_BOOL];
		break;
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
		break;
	}

	case AST_BLOB: {
		op_value_blob* opbl = (op_value_blob*)op;
		uint32_t bin_sz = np->u.str.sz / 2;

		opbl->base.code = EXP_VOP_VALUE_BLOB;
		opbl->value = args->extra_ptr;
		opbl->value_sz = bin_sz;

		ael_hex_decode(np->u.str.str, np->u.str.sz, args->extra_ptr);

		args->extra_ptr += bin_sz;
		args->mem += op_table[EXP_VOP_VALUE_BLOB].size;
		args->entry = &op_table[EXP_VOP_VALUE_BLOB];
		break;
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
		break;
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
		break;
	}

	case AST_BIN:
	case AST_BIN_REF: {
		const ast_node* bin_canon = ast_bin_canonical(pool, np);
		op_var* opv = (op_var*)op;
		uint32_t name_sz = bin_canon->u.bin.name_sz;
		const uint8_t* name = args->ael_buf + bin_canon->offset;

		cf_assert(ast_type_resolved(np->etype), AS_EXP,
				"ael_build_node - bin %.*s unresolved at build", name_sz, name);

		opv->base.code = EXP_BIN;
		opv->base.rtype = ast_etype_to_rtype(np->etype);

		args->mem += op_table[EXP_BIN].size;

		args->entry = exp_build_get_entry(opv->base.rtype);

		cf_assert(args->entry != NULL, AS_EXP,
				"ael_build_node - bin %.*s rtype %d has no op entry", name_sz,
				name, opv->base.rtype);

		define_bin_name128(name128, name, name_sz,
				as_bin_name_need_memcpy(name, args->ael_buf + args->ael_src_sz));
		opv->idx = exp_build_find_bin_idx(&args->bin_table, name128);

		cf_assert(opv->idx < args->bin_table.n_bins, AS_EXP,
				"ael_build_node - bin %.*s absent from bin table", name_sz, name);

		cf_assert(np->etype == bin_canon->etype, AS_EXP,
				"ael_build_node - bin %.*s ref/canonical etype divergence %u vs %u",
				name_sz, name, np->etype, bin_canon->etype);

		break;
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
			args->entry = exp_build_get_entry(rt);
			break;
		}
		else {
			args->exp->flags |= AS_EXP_HAS_NON_DIGEST_META;
		}

		args->entry = &op_table[mc];
		break;
	}

	case AST_LOOP_VAR: {
		op_var* opv = (op_var*)op;

		opv->base.code = EXP_VAR_BUILTIN;
		args->mem += op_table[EXP_VAR_BUILTIN].size;

		opv->idx = np->u.loop_var.builtin;
		opv->base.rtype = ast_loop_var_rtype(np);

		args->entry = exp_build_get_entry(opv->base.rtype);

		if (args->entry == NULL) {
			cf_warning(AS_EXP, "ael_build_node - loop_var unknown type %d",
					opv->base.rtype);
			return false;
		}

		break;
	}

	case AST_VAR: {
		op_var* opv = (op_var*)op;

		opv->base.code = EXP_VAR;
		args->mem += op_table[EXP_VAR].size;

		cf_assert(ast_type_resolved(np->etype), AS_EXP,
				"ael_build_node - var slot %u unresolved at build",
				np->u.var.var_idx);

		// Bins own var slots [0, n_bins); let-vars are numbered above them.
		opv->idx = np->u.var.var_idx + args->bin_table.n_bins;
		opv->base.rtype = ast_etype_to_rtype(np->etype);

		switch (opv->base.rtype) {
		case EXP_RTYPE_NIL:
			args->entry = &op_table[EXP_VOP_VALUE_NIL];
			break;
		case EXP_RTYPE_TRILEAN:
			args->entry = &op_table[EXP_VOP_VALUE_TRILEAN];
			break;
		default:
			args->entry = exp_build_get_entry(opv->base.rtype);

			if (args->entry == NULL) {
				cf_warning(AS_EXP, "ael_build_node - var unknown type %d",
						opv->base.rtype);
				return false;
			}
		}

		break;
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
		break;
	}

	// N-ary math (circular list).
	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN: {
		cf_assert(ast_type_resolved(np->etype), AS_EXP,
				"ael_build_node - %s unresolved at build",
				ast_node_table[np->type].name);

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
		break;
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
		break;
	}

	// Left-fold (SUB, DIV) → emitted as n-ary.
	case AST_SUB:
	case AST_DIV: {
		cf_assert(ast_type_resolved(np->etype), AS_EXP,
				"ael_build_node - %s unresolved at build",
				ast_node_table[np->type].name);

		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_left_fold(args, ref, np->type)) {
			return false;
		}

		args->entry =
				&op_table[np->etype == AST_ETYPE_FLOAT ? EXP_VOP_VALUE_FLOAT
													   : EXP_VOP_VALUE_INT];
		break;
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
		break;
	}

	// Binary math (mod, pow, shifts) — op code from the node table, same
	// grouping as ael_count_sz so the two passes stay isomorphic.
	case AST_MOD:
	case AST_POW:
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
		break;
	}

	// Unary ops — op code from the node table.
	case AST_NOT:
	case AST_BIT_NOT:
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
	case AST_PATH_FUNC_CAST_STRING: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_node(args, np->u.unary.operand)) {
			return false;
		}

		args->entry = &op_table[code];
		break;
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
		break;
	}

	case AST_FUNC_CEIL:
	case AST_FUNC_FLOOR:
	case AST_FUNC_COUNT_ONE_BITS: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_node(args, np->u.func1.arg)) {
			return false;
		}

		args->entry = &op_table[code];
		break;
	}

	// 2-arg functions — op code from the node table.
	case AST_FUNC_LOG:
	case AST_FUNC_POW:
	case AST_FUNC_FIND_BIT_LEFT:
	case AST_FUNC_FIND_BIT_RIGHT: {
		exp_op_code code = (exp_op_code)ast_node_table[np->type].exp_cmd;

		op->code = code;
		args->mem += op_table[code].size;

		if (! ael_build_node(args, np->u.func2.arg1) ||
				! ael_build_node(args, np->u.func2.arg2)) {
			return false;
		}

		args->entry = &op_table[code];
		break;
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
		opv->has_nonstorage = ael_literal_has_nonstorage(start, pk.offset);
		args->extra_ptr += pk.offset;
		args->entry = &op_table[EXP_QUOTE];
		break;
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
		opv->has_nonstorage = ael_literal_has_nonstorage(start, pk.offset);
		args->extra_ptr += pk.offset;
		// Entry carries r_type = EXP_RTYPE_MAP so the caller (e.g.,
		// build_set_expected_particle_type) sets the as_exp's expected
		// particle type to MAP. The op's `code` field above stays
		// MSGPACK for the runtime exp_eval_value dispatch.
		args->entry = &op_table[EXP_VOP_VALUE_MAP];
		break;
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

		break;
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
			const op_table_entry* re = exp_build_get_entry(result_type);

			if (re != NULL) {
				args->entry = re;
			}
		}

		break;
	}

	// Path access → CALL.
	case AST_PATH_CALL: {
		ast_node* op_node = ast_pool_at(pool, np->u.call.call_op);
		op_call* opc = (op_call*)op;

		// The select family computes its rtype from the bin's etype
		// downstream. Every other path-call derives the wire rtype from
		// this node's, so an unresolved one has nothing to emit.
		cf_assert(ast_is_select_family(op_node->type) ||
						ast_type_resolved(np->etype),
				AS_EXP, "ael_build_node - path call unresolved at build");

		// Pack blob into extra_data.
		uint8_t* blob_start = args->extra_ptr;

		// Value-recv path-call: u.call.ctx is the receiver value-
		// expression (not an AST_PATH_CTX list). Wire shape:
		//   [CALL, rtype, stype, [op_code, args...], <receiver>]
		// Used by EXP_CALL_BITS unconditionally, and by EXP_CALL_CDT
		// when the parser built a value-recv form (`(expr).cdtFn(...)`).
		if (ast_call_is_value_recv(pool, np)) {
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

			args->entry = exp_build_get_entry(opc->type);

			cf_assert(args->entry != NULL, AS_EXP,
					"ael_build_node - value-recv call rtype %u has no op entry",
					opc->type);

			break;
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

		if (ast_is_select_family(op_node->type)) {
			// The call's rtype comes from the shared shape resolver — the
			// same one the static emitter uses, so the customer-visible
			// rtype can't drift between the two paths.
			ael_select_shape_t shape =
					ael_select_shape(pool, np->u.call.call_op, bin_ref);

			opc->type = shape.rtype;

			if (ael_pack_select_op_blob(&pk, pool, args->ael_src, ctx_seg_head,
						ast_ctx_seg_count(cxn), np->u.call.call_op, &shape) != 0) {
				return false;
			}

			// SELECT path: single static vec (no sub-expressions).
			opc->vecs[0].buf = blob_start;
			opc->vecs[0].buf_sz = pk.offset;
			opc->n_vecs = 1;
			args->extra_ptr += pk.offset;
		}
		else {
			uint32_t ctx_seg_count = ast_ctx_seg_count(cxn);

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
							ctx_seg_count, ael_ctx_path_flags(op_node)) != 0) {
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

		args->entry = exp_build_get_entry(opc->type);

		if (args->entry == NULL) {
			cf_warning(AS_EXP, "build_path_call - unresolved rtype %u",
					opc->type);
			return false;
		}

		break;
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
		opv->idx = exp_build_find_bin_idx(&args->bin_table, name128);

		cf_assert(opv->idx < args->bin_table.n_bins, AS_EXP,
				"ael_build_node - bin %.*s absent from bin table", name_sz, name);

		args->mem += op_table[code].size;
		args->entry = &op_table[code];
		break;
	}

	default:
		cf_warning(AS_EXP, "ael_build_node - unsupported node type %d", np->type);
		return false;
	}

	op->instr_end_ix = args->instr_ix;

	return true;
}

//==========================================================
// Local helpers - AEL msgpack literal packing.
//

// Used by collection literals (AST_LIST, AST_MAP) which must
// be msgpack-encoded into extra_data. Works in two modes:
// - pk->buffer == NULL: sizing only (counts bytes)
// - pk->buffer != NULL: actual packing

// ael_lit_child_emit adapter: raw-literal recursion (nested collections
// stay raw — no EXP_QUOTE, unlike the wire form's ael_codegen_emit).
static int
ael_pack_literal_child(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref child)
{
	(void)input;
	return ael_pack_literal(pk, pool, child);
}

static int
ael_pack_literal(as_packer* pk, ast_pool* pool, ast_ref ref)
{
	if (ref == AST_REF_NULL) {
		return as_pack_nil(pk);
	}

	ast_node* np = ast_pool_at(pool, ref);

	// The byte-producing bodies live in ael_codegen.c and are shared with
	// the static emitter — one source of literal bytes for both paths.
	// (Collection elements are parser-enforced literals, so the recursion
	// never leaves literal territory here.)
	switch (np->type) {
	case AST_LIST:
		return ael_emit_list_literal(pk, pool, NULL, ref, ael_pack_literal_child);
	case AST_MAP:
		return ael_emit_map_literal(pk, pool, NULL, ref, ael_pack_literal_child);
	default: {
		int rc = ael_emit_scalar_literal(pk, np);

		if (rc != 0) {
			cf_warning(AS_EXP, "ael_pack_literal - unsupported type %d",
					np->type);
		}

		return rc;
	}
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

//==========================================================
// Local helpers - bin registration.
//

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

		if (! exp_build_get_or_add_bin_entry(t, &bn)) {
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

// Record the bins a sindex-on-expression depends on.
// pre:  bins_info_r is non-NULL; head threads AST_BIN nodes via u.bin.bin_next,
//       from a parse that published its roots - one with no error diag.
// post: bins_info_r gains one entry per distinct (name, type); a bin nothing
//       pinned, or one left on a partially-narrowed AUTO_* mask, gets
//       AS_PARTICLE_TYPE_NULL.
//
// A NULL type is a wildcard, standing for bins of this name with *all* types.
//
// Watch local_bin_root: it holds one node per occurrence, not per name, so the
// dedup has to happen at the vector.
static void
ael_fill_bins_info(cf_vector* bins_info_r, ast_pool* pool,
		const uint8_t* ael_str, uint32_t ael_sz, ast_ref head)
{
	for (ast_ref br = head; br != AST_REF_NULL;) {
		const ast_node* bp = ast_pool_at(pool, br);
		uint32_t name_sz = bp->u.bin.name_sz;
		const uint8_t* name = ael_str + bp->offset;
		as_bin_info bin_info;

		define_bin_name128(name128, name, name_sz,
				as_bin_name_need_memcpy(name, ael_str + ael_sz));

		memcpy(bin_info.name, &name128, sizeof(name128));
		bin_info.type = (as_particle_type)ael_etype_to_particle_type(bp->etype);
		cf_vector_append_unique(bins_info_r, &bin_info);

		br = bp->u.bin.bin_next;
	}
}
