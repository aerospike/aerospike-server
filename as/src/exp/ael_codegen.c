/*
 * ael_codegen.c
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

#include "exp/ael_codegen.h"

#include <stdlib.h>

#include "aerospike/as_msgpack.h"

#include "cf_defer.h"
#include "enhanced_alloc.h"

#include "base/cdt.h"
#include "base/proto.h"
#include "exp/ael_emit.h"
#include "exp/ael_parse.h"
#include "exp/ael_string.h"
#include "exp/ast.h"
#include "exp/exp.h"

//==========================================================
// Forward declarations.
//

#define RC_RET_ON_ERR()                                                        \
	if (rc != 0) {                                                             \
		return rc;                                                             \
	}

static uint32_t count_left_fold(ast_pool* pool, ast_ref node, ast_node_t type);

static int emit_particle_string(as_packer* pk, const char* s, uint32_t sz);
static int emit_raw_string(as_packer* pk, const char* s, uint32_t sz);
static int emit_cmd(as_packer* pk, int cmd, uint32_t nargs);
static int emit_left_fold(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ast_node_t type);
static int emit_nary(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_nmath(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_binary(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_func1(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_func2(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_meta(as_packer* pk, ast_pool* pool, ast_ref node);
static int emit_list(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_map(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_with(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_when(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
// ael_emit_cdt_op_blob is shared with exp.c -- declaration in ael_emit.h.
static int emit_path_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_select_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_bit_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_hll_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);

// Property → wire-flag projections. Defined later; forward-declared so
// they're callable from ael_emit_cdt_op_blob (which lives above them).
static uint64_t prop_to_bits_flag(ast_prop_bits props);
static uint64_t prop_to_hll_flag(ast_prop_bits props);
// ael_prop_to_cdt_flag / ael_prop_to_select_flag /
// ael_cdt_op_has_create_flags_slot / ael_cdt_op_no_modify_flags_slot are
// non-static; declared in ael_emit.h for shared use with exp.c
// (ael_pack_call_blob, ael_pack_select_blob).
static uint64_t prop_to_ctx_create(ast_prop_bits props);

//==========================================================
// Local helpers.
//

// Iterative (not recursive) so it adds no C-stack depth on top of the
// ael_codegen_emit frame already descending this left-deep chain.
static uint32_t
count_left_fold(ast_pool* pool, ast_ref node, ast_node_t type)
{
	uint32_t count = 0;

	while (true) {
		ast_node* np = ast_pool_at(pool, node);

		if (np->type != type) {
			return count + 1;
		}

		count++;
		node = np->u.binary.left;
	}
}

//==========================================================
// Packing helpers.
//

bool
ael_pack_str_with_type_reserve(as_packer* pk, uint8_t type, uint32_t sz,
		uint8_t** buf_r)
{
	// Header for (type byte + sz body bytes) via the public packer's
	// NULL-buf path. as_pack_str's header emit goes through
	// pack_byte / pack_type_uint*, which gate the write on
	// `if (pk->buffer)` but always run advance_offset (with its
	// INT32_MAX check). Body skip is intentional — we fill it
	// ourselves below.
	if (as_pack_str(pk, NULL, sz + 1) != 0) {
		return false;
	}

	// Reserve type byte + sz body bytes. Mirrors pack_byte /
	// pack_append: capacity check is packing-mode only, sizing mode
	// just advances offset. __builtin_add_overflow guards the
	// uint32_t addition; INT32_MAX is the upper bound advance_offset
	// uses.
	uint32_t need = 1 + sz;
	uint32_t new_offset;

	if (__builtin_add_overflow(pk->offset, need, &new_offset) ||
			new_offset > INT32_MAX) {
		return false;
	}

	if (pk->buffer != NULL) {
		if (new_offset > pk->capacity) {
			return false;
		}

		pk->buffer[pk->offset] = type;
		*buf_r = pk->buffer + pk->offset + 1;
	}
	else {
		*buf_r = NULL;
	}

	pk->offset = new_offset;
	return true;
}

static int
pack_hex_blob(as_packer* pk, const char* hex, uint32_t hex_sz)
{
	uint32_t bin_sz = hex_sz / 2;
	uint8_t* dst;

	if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
		return -1;
	}

	if (dst != NULL) {
		for (uint32_t i = 0; i < bin_sz; i++) {
			dst[i] = (uint8_t)((ael_hex_nibble(hex[i * 2]) << 4) |
					ael_hex_nibble(hex[i * 2 + 1]));
		}
	}

	return 0;
}

static int
pack_b64_blob(as_packer* pk, const char* b64, uint32_t b64_sz)
{
	uint32_t bin_sz = ael_b64_decoded_sz(b64, b64_sz);
	uint8_t* dst;

	if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
		return -1;
	}

	if (dst != NULL) {
		ael_b64_decode(b64, b64_sz, dst);
	}

	return 0;
}

//==========================================================
// Emit functions.
//

static int
emit_particle_string(as_packer* pk, const char* s, uint32_t sz)
{
	return as_pack_str_with_type(pk, AS_BYTES_STRING, (const uint8_t*)s, sz);
}

// Bin / var / let names are plain msgpack strings on the wire — no
// AS_BYTES_STRING type prefix. The runtime's build_bin / build_var /
// build_let read them with msgpack_get_bin and compare bytes directly
// against rd->bins[].name (likewise the var scope name), so any
// in-band type byte would corrupt the lookup.
static int
emit_raw_string(as_packer* pk, const char* s, uint32_t sz)
{
	return as_pack_str(pk, (const uint8_t*)s, sz);
}

static int
emit_cmd(as_packer* pk, int cmd, uint32_t nargs)
{
	int rc = as_pack_list_header(pk, nargs + 1);
	RC_RET_ON_ERR();
	return as_pack_int64(pk, cmd);
}

static int
emit_left_fold(as_packer* pk, ast_pool* pool, const char* input, ast_ref node,
		ast_node_t type)
{
	int rc;
	ast_node* np = ast_pool_at(pool, node);

	if (np->type == type) {
		rc = emit_left_fold(pk, pool, input, np->u.binary.left, type);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, np->u.binary.right);
	}

	return ael_codegen_emit(pk, pool, input, node);
}

static int
emit_nary(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, np->u.list.count);
	RC_RET_ON_ERR();

	ast_ref e = np->u.list.head;

	for (uint32_t i = 0; i < np->u.list.count; i++) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		e = ast_pool_at(pool, e)->next;
	}

	return rc;
}

static int
emit_nmath(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, np->u.nmath.count);
	RC_RET_ON_ERR();

	ast_ref head = ast_nmath_head(pool, np);
	ast_ref e = head;

	for (uint32_t i = 0; i < np->u.nmath.count; i++) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		e = ast_pool_at(pool, e)->next;
	}

	return rc;
}

static int
emit_binary(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, 2);
	RC_RET_ON_ERR();
	rc = ael_codegen_emit(pk, pool, input, np->u.binary.left);
	RC_RET_ON_ERR();
	return ael_codegen_emit(pk, pool, input, np->u.binary.right);
}

static int
emit_func1(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, 1);
	RC_RET_ON_ERR();
	return ael_codegen_emit(pk, pool, input, np->u.func1.arg);
}

static int
emit_func2(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, 2);
	RC_RET_ON_ERR();
	rc = ael_codegen_emit(pk, pool, input, np->u.func2.arg1);
	RC_RET_ON_ERR();
	return ael_codegen_emit(pk, pool, input, np->u.func2.arg2);
}

static int
emit_meta(as_packer* pk, ast_pool* pool, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = (int)np->u.meta.op_code;

	if (cmd == 0) {
		return -1;
	}

	if (cmd == EXP_META_DIGEST_MOD) {
		int rc = emit_cmd(pk, cmd, 1);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, np->u.meta.param);
	}

	if (cmd == EXP_REC_KEY) {
		// rtype is derived from the narrowed etype (shared with the
		// production emitter via ast_rec_key_rtype). The post-parse pass
		// rejects an unresolved key — no silent default type — so etype
		// is resolved here; guard defensively against a stray call.
		if (! ast_type_resolved(np->etype)) {
			return -1;
		}

		exp_rtype rt = ast_rec_key_rtype(np);
		int rc = emit_cmd(pk, cmd, 1);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, (int64_t)rt);
	}

	return emit_cmd(pk, cmd, 0);
}

static int
emit_list(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t ele_count = np->u.list.count;
	// Lists default to plain/unordered; :ORDERED opts in to an ORDERED-flagged
	// list (validated, not sorted, below).
	bool ordered = np->u.list.order_override == AEL_ORDER_ORDERED;
	uint32_t start = pk->offset;

	int rc = as_pack_list_header(pk, ele_count + (ordered ? 1 : 0));
	RC_RET_ON_ERR();

	if (ordered) {
		rc = as_pack_ext_header(pk, 0, AS_PACKED_LIST_FLAG_ORDERED);
		RC_RET_ON_ERR();
	}

	ast_ref e = np->u.list.head;

	for (uint32_t i = 0; i < ele_count; i++) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		if (i + 1 < ele_count) {
			e = ast_pool_at(pool, e)->next;
		}
	}

	if (ordered && pk->buffer != NULL && ele_count >= 2 &&
			! list_buf_check_ordered(pk->buffer + start, pk->offset - start)) {
		if (pool->diags != NULL) {
			ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
					np->sz, ":ORDERED list literal is not in ascending order");
		}

		return -1;
	}

	return rc;
}

static int
emit_map(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t ele_count = np->u.list.count;
	// Maps default to ORDERED (K_ORDERED + key-sort, below); :UNORDERED opts
	// out to a plain map (smaller, but uncomparable).
	bool ordered = np->u.list.order_override != AEL_ORDER_UNORDERED;
	uint32_t start = pk->offset;

	int rc = as_pack_map_header(pk, ele_count + (ordered ? 1 : 0));
	RC_RET_ON_ERR();

	if (ordered) {
		rc = as_pack_ext_header(pk, 0, AS_PACKED_MAP_FLAG_K_ORDERED);
		RC_RET_ON_ERR();
		rc = as_pack_nil(pk); // ext pair value
		RC_RET_ON_ERR();
	}

	ast_ref key = np->u.list.head;

	for (uint32_t i = 0; i < ele_count; i++) {
		ast_node* kp = ast_pool_at(pool, key);
		ast_ref val = kp->next;

		rc = ael_codegen_emit(pk, pool, input, key);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, val);
		RC_RET_ON_ERR();
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

	return rc;
}

static int
emit_with(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t defs_count = np->u.list.count - 2; // exclude let_scope + body
	int rc = emit_cmd(pk, EXP_LET, (defs_count * 2) + 1);
	RC_RET_ON_ERR();

	// Skip let_scope at head.
	ast_ref def = ast_pool_at(pool, np->u.list.head)->next;

	for (uint32_t i = 0; i < defs_count; i++) {
		ast_node* dp = ast_pool_at(pool, def);
		rc = emit_raw_string(pk, input + dp->offset, dp->u.var_def.name_sz);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, dp->u.var_def.value);
		RC_RET_ON_ERR();
		def = dp->next;
	}

	return ael_codegen_emit(pk, pool, input, np->u.list.tail);
}

static int
emit_when(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t mappings_count = np->u.list.count - 1;
	int rc = emit_cmd(pk, EXP_COND, (mappings_count * 2) + 1);
	RC_RET_ON_ERR();

	ast_ref m = np->u.list.head;

	for (uint32_t i = 0; i < mappings_count; i++) {
		ast_node* mp = ast_pool_at(pool, m);
		rc = ael_codegen_emit(pk, pool, input, mp->u.when_case.cond);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, mp->u.when_case.result);
		RC_RET_ON_ERR();
		m = mp->next;
	}

	return ael_codegen_emit(pk, pool, input, np->u.list.tail);
}

// Pack the inner CDT op blob (no ctx wrapper). The param chain is in
// wire-format order after op_code: each entry maps 1:1 to a wire element.
// List leaves are transmuted to AST_LIST at parse time and packed via
// emit_list (no EXP_QUOTE prefix — the bytes go straight into the blob).
// True for CDT modify ops whose wire shape is `[op, args..., create_flags?,
// modify_flags?]` — i.e. ops that have BOTH a create_flags and a
// modify_flags trailing slot. When emitting modify_flags via properties,
// we need to pad create_flags=0 to fill the create slot first.
bool
ael_cdt_op_has_create_flags_slot(int cdt_op)
{
	switch (cdt_op) {
	case AS_CDT_OP_LIST_APPEND:
	case AS_CDT_OP_LIST_APPEND_ITEMS:
	case AS_CDT_OP_LIST_INSERT_ITEMS:
	case AS_CDT_OP_LIST_INCREMENT:
	case AS_CDT_OP_MAP_ADD:
	case AS_CDT_OP_MAP_ADD_ITEMS:
	case AS_CDT_OP_MAP_PUT:
	case AS_CDT_OP_MAP_PUT_ITEMS:
		return true;
	default:
		return false;
	}
}

// True for CDT modify ops that have NO modify_flags slot at all
// (MAP_REPLACE, MAP_INCREMENT/DECREMENT, CLEAR, SORT, REMOVE). Properties
// here are silently no-op'd at codegen — the parse-time apply_prop layer
// could tighten this further per-op, but the wide CDT_MAP valid mask is
// intentionally permissive for now.
bool
ael_cdt_op_no_modify_flags_slot(int cdt_op)
{
	switch (cdt_op) {
	case AS_CDT_OP_MAP_REPLACE:
	case AS_CDT_OP_MAP_REPLACE_ITEMS:
	case AS_CDT_OP_MAP_INCREMENT:
	case AS_CDT_OP_MAP_DECREMENT:
	case AS_CDT_OP_LIST_CLEAR:
	case AS_CDT_OP_MAP_CLEAR:
	case AS_CDT_OP_LIST_SORT:
		return true;
	default:
		return false;
	}
}

int
ael_emit_cdt_op_blob(as_packer* pk, ast_pool* pool, const char* input, int cdt_op,
		ast_ref param_head, uint8_t param_count, ast_prop_bits props_flags)
{
	uint64_t mf = ael_prop_to_cdt_flag(props_flags);
	uint64_t cf = ael_prop_to_cdt_create_flag(props_flags);
	bool has_cslot = ael_cdt_op_has_create_flags_slot(cdt_op);
	bool emit_flags = mf != 0 && ! ael_cdt_op_no_modify_flags_slot(cdt_op);
	// The create slot appears when a create-order flag is set, or as a
	// pad-to-position before a modify flag on a two-slot op.
	bool emit_create = has_cslot && (cf != 0 || emit_flags);

	// sort() :DROP_DUPS rides the op's own FLAGS param (not a modify slot).
	uint64_t sf = ael_sort_flags(props_flags);
	bool emit_sort_flag = sf != 0;

	uint32_t extra = (emit_sort_flag ? 1 : 0) + (emit_create ? 1 : 0) +
			(emit_flags ? 1 : 0);

	int rc = as_pack_list_header(pk, 1 + param_count + extra);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, cdt_op);
	RC_RET_ON_ERR();

	for (ast_ref e = param_head; e != AST_REF_NULL;
			e = ast_pool_at(pool, e)->next) {
		if (ast_pool_at(pool, e)->type == AST_LIST) {
			rc = emit_list(pk, pool, input, e);
		}
		else {
			rc = ael_codegen_emit(pk, pool, input, e);
		}

		RC_RET_ON_ERR();
	}

	if (emit_sort_flag) {
		rc = as_pack_int64(pk, (int64_t)sf);
		RC_RET_ON_ERR();
	}
	if (emit_create) {
		rc = as_pack_int64(pk, (int64_t)cf); // 0 when pure pad before mf
		RC_RET_ON_ERR();
	}
	if (emit_flags) {
		rc = as_pack_int64(pk, (int64_t)mf);
		RC_RET_ON_ERR();
	}

	return 0;
}

static int
emit_path_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);

	if (op->type == AST_PATH_FUNC_SELECT || op->type == AST_PATH_FUNC_MODIFY ||
			op->type == AST_PATH_FUNC_PSELECT_REMOVE) {
		return emit_select_call(pk, pool, input, node);
	}

	// Everything below derives the wire rtype byte from np->etype.
	// Reject any path-call whose etype isn't a single resolved bit:
	// GET without `:T` pin or cross-narrow leaves etype=AUTO; math
	// chains that narrowed only to AUTO_NUMERIC leave it multi-bit;
	// modify ops with `return: KEY/VALUE` and no pin land at AUTO via
	// pcl_exp_etype. Modify ops with default-NONE or INDEX/RANK/COUNT
	// return modifiers have resolved etypes (NIL / INT) and pass.
	if (! ast_type_resolved(np->etype)) {
		if (pool->diags != NULL) {
			ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
					np->sz, "cannot infer type — pin with :T");
		}
		return -1;
	}

	// HLL calls are always value-recv with their own wire-flag set.
	if (np->u.call.stype == EXP_CALL_HLL ||
			(np->u.call.stype & ~EXP_CALL_FLAG_MODIFY_LOCAL) == EXP_CALL_HLL) {
		return emit_hll_call(pk, pool, input, node);
	}

	// Value-recv path-call: u.call.ctx is a value-expression, not an
	// AST_PATH_CTX list. BITS calls are always value-recv; CDT calls
	// can be either, depending on the receiver grammar that built them
	// (`(expr).cdtFn(...)` builds value-recv form). The wire shape is
	// identical — emit_bit_call serves both.
	if (np->u.call.stype == EXP_CALL_BITS ||
			ast_pool_at(pool, np->u.call.ctx)->type != AST_PATH_CTX) {
		return emit_bit_call(pk, pool, input, node);
	}

	ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);

	// Bin is the head of the ctx list; segs (if any) chain after it.
	ast_ref bin_ref = cxn->u.ctx_list.head;
	ast_ref ctx_seg_head = ast_pool_at(pool, bin_ref)->next;

	uint32_t ctx_seg_count = 0;

	for (ast_ref er = ctx_seg_head; er != AST_REF_NULL;
			er = ast_pool_at(pool, er)->next) {
		ctx_seg_count++;
	}

	// AUTO rtype may leak past the comparison/math validator when the
	// path-call sits in a context the validator doesn't cover (unary
	// cast wrapper, let-value, function arg). The wire emit passes
	// AUTO through; the runtime's rtype mismatch check catches it.
	// Defer a stricter codegen-time assert until the validator covers
	// every result-consuming context.
	// [CALL, rtype, stype, cdt_blob, bin]
	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_etype_to_rtype(np->etype));
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, np->u.call.stype);
	RC_RET_ON_ERR();

	if (ctx_seg_count > 0) {
		rc = ael_pack_ctx(pk, pool, input, ctx_seg_head, ctx_seg_count);
		RC_RET_ON_ERR();
	}

	rc = ael_emit_cdt_op_blob(pk, pool, input, ast_cdt_op_op_code(op),
			ast_cdt_op_head(op), ast_cdt_op_count(op), ast_cdt_op_props(op));
	RC_RET_ON_ERR();

	return ael_codegen_emit(pk, pool, input, bin_ref);
}

// AST_PROP_* → wire AS_BITS_FLAG_*. Returns 0 for read-op props (their
// flag bits are masked out at ael_apply_prop time, so this is a no-op).
static uint64_t
prop_to_bits_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_BITS_FLAG_NO_FAIL;
	}
	if ((props & AST_PROP_CREATE_ONLY) != 0) {
		w |= AS_BITS_FLAG_CREATE_ONLY;
	}
	if ((props & AST_PROP_UPDATE_ONLY) != 0) {
		w |= AS_BITS_FLAG_UPDATE_ONLY;
	}
	if ((props & AST_PROP_PARTIAL) != 0) {
		w |= AS_BITS_FLAG_PARTIAL;
	}

	return w;
}

// AST_PROP_* → wire AS_CDT_LIST_* / AS_CDT_MAP_*. The list and map flag
// constants share values where overlapping (NO_FAIL=4, DO_PARTIAL=8), so
// a single projection covers both. NO_OVERWRITE / NO_CREATE are map-only
// at the wire level — list valid masks reject them at parse.
uint64_t
ael_prop_to_cdt_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_CDT_MAP_NO_FAIL; // == AS_CDT_LIST_NO_FAIL (0x04)
	}
	if ((props & AST_PROP_NO_OVERWRITE) != 0) {
		w |= AS_CDT_MAP_NO_OVERWRITE;
	}
	if ((props & AST_PROP_PARTIAL) != 0) {
		w |= AS_CDT_MAP_DO_PARTIAL; // == AS_CDT_LIST_DO_PARTIAL (0x08)
	}
	if ((props & AST_PROP_NO_CREATE) != 0) {
		w |= AS_CDT_MAP_NO_CREATE;
	}

	return w;
}

uint64_t
ael_prop_to_cdt_create_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_CR_KEY_ORDERED) != 0) {
		w |= AS_PACKED_MAP_FLAG_K_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_VALUE_ORDERED) != 0) {
		w |= AS_PACKED_MAP_FLAG_KV_ORDERED;
	}
	if ((props & AST_PROP_CR_ORDERED) != 0) {
		w |= AS_PACKED_LIST_FLAG_ORDERED; // == 0x01
	}
	if ((props & AST_PROP_PERSIST_INDEX) != 0) {
		w |= AS_PACKED_PERSIST_INDEX;
	}
	// UNORDERED has no packed bit -- unordered is the default (0).

	return w;
}

uint64_t
ael_sort_flags(ast_prop_bits props)
{
	return (props & AST_PROP_DROP_DUPS) != 0 ? AS_CDT_SORT_DROP_DUPLICATES : 0;
}

// AST_PROP_* → wire AS_CDT_SELECT_* flag byte (OR'd onto the SELECT
// return-type byte). Today only NO_FAIL maps to a wire bit — SELECT
// doesn't have a DO_PARTIAL constant in proto.h yet.
uint64_t
ael_prop_to_select_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_CDT_SELECT_NO_FAIL;
	}

	return w;
}

// AST_PROP_CR_* / PERSIST_INDEX → wire AS_CDT_CTX_CREATE_* bits
// (OR'd onto a sub-context type byte).
static uint64_t
prop_to_ctx_create(ast_prop_bits props)
{
	uint64_t w = 0;

	// UNORDERED is container-agnostic: LIST_UNORDERED == MAP_UNORDERED == 0x40.
	// The ordered flavors are container-specific but the apply-time validator
	// guarantees the seg's container matches (ORDERED on a list, KEY[_VALUE]_
	// ORDERED on a map), so emitting the matching wire bit is always correct.
	if ((props & AST_PROP_UNORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_LIST_UNORDERED;
	}
	if ((props & AST_PROP_CR_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_LIST_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_MAP_K_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_VALUE_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_MAP_KV_ORDERED;
	}
	if ((props & AST_PROP_PERSIST_INDEX) != 0) {
		w |= AS_CDT_CTX_CREATE_PERSIST_INDEX;
	}

	return w;
}

// emit_bit_call — pack an EXP_CALL_BITS invocation. Unlike CDT path
// calls, BIT_OPs have no CONTEXT_EVAL / path navigation: the receiver
// is a single value-producing sub-expression (stored in u.call.ctx by
// ael_finalize_bit_call, not an AST_PATH_CTX list).
//   Wire: [CALL, rtype, EXP_CALL_BITS, [op_code, args..., flags?], receiver]
static int
emit_bit_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);

	// For a BIT op the property flags are already materialized into the
	// wire chain at parse time (ael_bit_materialize_flags cleared props),
	// so prop_to_bits_flag returns 0 and the chain is emitted verbatim —
	// matching the production packer. The projection here only fires for
	// the CDT value-recv calls this function also serves.
	uint64_t wire_flags = prop_to_bits_flag(ast_cdt_op_props(op));

	// bit-add / bit-subtract with `signed: true` already pushed a
	// (flags=0, subflags=SIGNED) pair onto the chain at parse time —
	// chain.count == 5 in that case. For properties we OR the wire
	// flags into the existing flags-placeholder. Otherwise (any other
	// modify op with non-zero properties) we append a flags arg after
	// the chain.
	bool chain_has_flags_slot =
			(ast_cdt_op_op_code(op) == AS_BITS_OP_ADD ||
					ast_cdt_op_op_code(op) == AS_BITS_OP_SUBTRACT) &&
			ast_cdt_op_count(op) == 5;
	bool append_flags = wire_flags != 0 && ! chain_has_flags_slot;

	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_etype_to_rtype(np->etype));
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, np->u.call.stype);
	RC_RET_ON_ERR();

	uint32_t inner_count = 1 + ast_cdt_op_count(op) + (append_flags ? 1 : 0);

	rc = as_pack_list_header(pk, inner_count);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_cdt_op_op_code(op));
	RC_RET_ON_ERR();

	uint32_t idx = 0;

	for (ast_ref a = ast_cdt_op_head(op); a != AST_REF_NULL;
			a = ast_pool_at(pool, a)->next) {
		if (chain_has_flags_slot && idx == 3 && wire_flags != 0) {
			// 4th chain element is the flags placeholder (AST_INT 0).
			// OR the property-derived wire flags into its value.
			ast_node* fnode = ast_pool_at(pool, a);
			rc = as_pack_int64(pk, fnode->u.ival | (int64_t)wire_flags);
		}
		else {
			rc = ael_codegen_emit(pk, pool, input, a);
		}
		RC_RET_ON_ERR();
		idx++;
	}

	if (append_flags) {
		rc = as_pack_int64(pk, (int64_t)wire_flags);
		RC_RET_ON_ERR();
	}

	return ael_codegen_emit(pk, pool, input, np->u.call.ctx);
}

// AST_PROP_* → wire AS_HLL_FLAG_*.
static uint64_t
prop_to_hll_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_CREATE_ONLY) != 0) {
		w |= AS_HLL_FLAG_CREATE_ONLY;
	}
	if ((props & AST_PROP_UPDATE_ONLY) != 0) {
		w |= AS_HLL_FLAG_UPDATE_ONLY;
	}
	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_HLL_FLAG_NO_FAIL;
	}

	return w;
}

// emit_hll_call — pack an EXP_CALL_HLL invocation. Same shape as
// emit_bit_call: receiver is a single value-producing sub-expression
// (stored in u.call.ctx, not an AST_PATH_CTX list).
//   Wire: [CALL, rtype, EXP_CALL_HLL, [op_code, args..., flags?], receiver]
static int
emit_hll_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);

	// HLL property flags are materialized into the wire chain at parse time
	// (ael_hll_materialize_flags cleared props), so prop_to_hll_flag is 0
	// here and the chain is emitted verbatim — matching the production
	// packer. Kept as a defensive no-op should a future path leave props set.
	uint64_t wire_flags = prop_to_hll_flag(ast_cdt_op_props(op));
	bool append_flags = wire_flags != 0;

	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_etype_to_rtype(np->etype));
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, np->u.call.stype);
	RC_RET_ON_ERR();

	uint32_t inner_count = 1 + ast_cdt_op_count(op) + (append_flags ? 1 : 0);

	rc = as_pack_list_header(pk, inner_count);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_cdt_op_op_code(op));
	RC_RET_ON_ERR();

	for (ast_ref a = ast_cdt_op_head(op); a != AST_REF_NULL;
			a = ast_pool_at(pool, a)->next) {
		rc = ael_codegen_emit(pk, pool, input, a);
		RC_RET_ON_ERR();
	}

	if (append_flags) {
		rc = as_pack_int64(pk, (int64_t)wire_flags);
		RC_RET_ON_ERR();
	}

	return ael_codegen_emit(pk, pool, input, np->u.call.ctx);
}

// Pack a single ctx-pair: (ctx_type, value). Reached from two contexts: the
// CONTEXT_EVAL chain, where parse construction guarantees only single-element
// segs so the BY_EXP / PLURAL / LIST / REL branches stay dormant; and the
// SELECT ctx-pair list, where all branches fire.
// Wire format per branch:
//   BY_EXP (wildcard):    (EXP, true | filter_expr_msgpack)
//   REL_RANGE:            [pivot, start, count?]   count = end-start+1
//   LIST_SEG:             [op0, op1, ...]
//   PLURAL (closed):      [start, end_or_count?]
//                         INDEX/RANK: count = end-start (static fold)
//                         KEY/VALUE INTERVAL: emit end as-is
//   PLURAL (open-end):    [start]
//   single:               operand
// Slow-path diagnostic: re-parse the sub-AEL to surface a AEL-position
// error when fast-path msgpack validation rejects the emitted bytes. If
// the re-parse produces no diagnostics (codegen-side bug — sub-AEL is
// well-formed but our msgpack is wrong), append a generic fallback at
// body_offset.
static void
ael_codegen_slow_path_diag(ast_pool* pool, const char* input, const ast_node* seg)
{
	if (pool->diags == NULL) {
		return;
	}

	uint32_t body_offset = seg->u.by_exp_seg.body_offset;
	uint32_t body_sz = seg->u.by_exp_seg.body_sz;

	ast_pool sub_pool;
	ast_pool_init(&sub_pool);

	ael_parse_result pr =
			ael_parse_filter_body(&sub_pool, input + body_offset, body_sz);

	if (ael_parse_has_error(&pr)) {
		for (uint32_t i = 0; i < pr.diags.count; i++) {
			const ael_diag* d = &pr.diags.entries[i];

			// Deep-copy: pr.diags is destroyed below, so an owned (addf) msg
			// must be duplicated rather than aliased into pool->diags.
			ael_diag_add_copy(pool->diags, d, d->offset + body_offset, d->sz);
		}
	}
	else {
		ael_diag_add(pool->diags, AEL_SEV_ERROR, body_offset, body_sz,
				"internal codegen error: invalid sub-program msgpack");
	}

	ael_diag_list_destroy(&pr.diags);

	// Reclaim the sub-parse non-recursively: ast_pool_destroy frees the whole
	// pool at once. A recursive ast_free would overflow the C stack on a deeply
	// nested body (bounded only by the 1 MiB source cap), like the main path.
	ast_pool_destroy(&sub_pool);
}

// Fast-path validation: run the runtime's msgpack-to-as_exp compile on
// the just-emitted sub-program bytes. Catches structural defects
// (op-code range, param count, EXP_CALL substructure) at parse time.
// Returns -1 if validation fails, after invoking the slow path.
//
// Skipped during the sizer pass (no buffer to validate against) and
// when there's no diag list to write to (e.g., ael_count_sz path that
// doesn't carry diagnostics through to user-facing errors).
static int
validate_sub_program(as_packer* pk, ast_pool* pool, const char* input,
		const ast_node* seg, uint32_t start)
{
	if (pk->buffer == NULL || pool->diags == NULL) {
		return 0;
	}

	uint32_t sub_sz = pk->offset - start;
	as_exp* sub = as_exp_build_buf(pk->buffer + start, sub_sz, false, NULL);

	// The probe re-enters the public builder, which resets and records into
	// the thread-local build-error accumulator against transient sub-program
	// bytes. Clear it: a leftover record would suppress the caller's
	// AEL-positioned record and leave the accumulator's payload dangling.
	as_exp_build_err_reset();

	if (sub != NULL) {
		as_exp_destroy(sub);
		return 0;
	}

	ael_codegen_slow_path_diag(pool, input, seg);
	return -1;
}

int
ael_pack_ctx(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref ctx_ele_head, uint32_t ctx_seg_count)
{
	int rc = as_pack_list_header(pk, 3);

	if (rc != 0) {
		return rc;
	}

	rc = as_pack_int64(pk, AS_CDT_OP_CONTEXT_EVAL);

	if (rc != 0) {
		return rc;
	}

	rc = as_pack_list_header(pk, ctx_seg_count * 2);

	if (rc != 0) {
		return rc;
	}

	ast_ref er = ctx_ele_head;
	uint32_t emitted = 0;

	while (er != AST_REF_NULL && emitted < ctx_seg_count) {
		ast_node* seg = ast_pool_at(pool, er);

		rc = ael_pack_ctx_seg_pair(pk, pool, input, er);

		if (rc != 0) {
			return rc;
		}

		emitted++;
		er = seg->next;
	}

	return 0;
}

int
ael_pack_ctx_seg_pair(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref seg_ref)
{
	ast_node* seg = ast_pool_at(pool, seg_ref);
	uint8_t flags = ast_node_table[seg->type].flags;
	int rc;

	if ((flags & AST_NF_BY_EXP) != 0) {
		rc = as_pack_uint64(pk, AS_CDT_CTX_EXP);
		RC_RET_ON_ERR();

		if (seg->u.by_exp_seg.filter == AST_REF_NULL) {
			return as_pack_bool(pk, true);
		}

		uint32_t start = pk->offset;
		rc = ael_codegen_emit(pk, pool, input, seg->u.by_exp_seg.filter);
		RC_RET_ON_ERR();
		return validate_sub_program(pk, pool, input, seg, start);
	}

	if ((flags & AST_NF_AND_EXP) != 0) {
		rc = as_pack_uint64(pk, AS_CDT_CTX_AND | AS_CDT_CTX_EXP);
		RC_RET_ON_ERR();

		uint32_t start = pk->offset;
		rc = ael_codegen_emit(pk, pool, input, seg->u.by_exp_seg.filter);
		RC_RET_ON_ERR();
		return validate_sub_program(pk, pool, input, seg, start);
	}

	int32_t ctx_type = ast_node_table[seg->type].ctx_type;

	if (ctx_type == 0) {
		return -1;
	}

	if ((flags & AST_NF_PLURAL) != 0 && ast_seg_inverted(seg)) {
		ctx_type |= AS_CDT_CTX_INVERTED;
	}

	// Single-select segs can carry CTX_CREATE / PERSIST_INDEX bits via
	// the `:PROPERTY` postfix grammar. OR them into the ctx_type byte —
	// the runtime CONTEXT_EVAL reads these to auto-create intermediate
	// containers when navigating.
	if (ast_node_table[seg->type].kind == NK_SEG_S) {
		ctx_type |= (int32_t)prop_to_ctx_create(ast_seg_props(seg));
	}

	rc = as_pack_uint64(pk, (uint64_t)ctx_type);
	RC_RET_ON_ERR();

	if ((flags & AST_NF_REL_RANGE) != 0) {
		ast_ref start = seg->u.rel_range_seg.start;
		ast_ref end = seg->u.rel_range_seg.end;
		ast_ref pivot = seg->u.rel_range_seg.relative_to;

		if (end != AST_REF_NULL) {
			int64_t s_v = ast_pool_at(pool, start)->u.ival;
			int64_t e_v = ast_pool_at(pool, end)->u.ival;

			rc = as_pack_list_header(pk, 3);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, pivot);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, start);
			RC_RET_ON_ERR();
			return as_pack_int64(pk, e_v - s_v + 1);
		}

		rc = as_pack_list_header(pk, 2);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, pivot);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, start);
	}

	if ((flags & AST_NF_LIST_SEG) != 0) {
		uint32_t count = seg->u.list_seg.count;

		rc = as_pack_list_header(pk, count);
		RC_RET_ON_ERR();

		for (ast_ref er = seg->u.list_seg.head; er != AST_REF_NULL;
				er = ast_pool_at(pool, er)->next) {
			rc = ael_codegen_emit(pk, pool, input, er);
			RC_RET_ON_ERR();
		}

		return 0;
	}

	if ((flags & AST_NF_PLURAL) != 0) {
		ast_ref start = seg->u.range_seg.start;
		ast_ref end = seg->u.range_seg.end;

		if (end != AST_REF_NULL) {
			rc = as_pack_list_header(pk, 2);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, start);
			RC_RET_ON_ERR();

			int16_t ct = ctx_type & 0x0F;

			if ((ct == AS_CDT_CTX_INDEX_RANGE || ct == AS_CDT_CTX_RANK_RANGE) &&
					ast_pool_at(pool, start)->type == AST_INT &&
					ast_pool_at(pool, end)->type == AST_INT) {
				int64_t s_v = ast_pool_at(pool, start)->u.ival;
				int64_t e_v = ast_pool_at(pool, end)->u.ival;

				return as_pack_int64(pk, e_v - s_v);
			}

			return ael_codegen_emit(pk, pool, input, end);
		}

		rc = as_pack_list_header(pk, 1);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, start);
	}

	return ael_codegen_emit(pk, pool, input, ast_seg_operand(seg));
}

static int
emit_select_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);
	ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);

	ast_ref bin_ref = cxn->u.ctx_list.head;
	ast_ref seg_head = ast_pool_at(pool, bin_ref)->next;

	uint32_t seg_count = 0;

	for (ast_ref er = seg_head; er != AST_REF_NULL;
			er = ast_pool_at(pool, er)->next) {
		seg_count++;
	}

	// op->u.modify.props carries AST_PROP_* internal bits set via the
	// `:PROPERTY` postfix. Project to AS_CDT_SELECT_* before merging
	// with the sel_type byte.
	uint8_t flags_byte = (uint8_t)ael_prop_to_select_flag(op->u.modify.props);
	as_cdt_select_flags sel_type;
	bool is_apply;
	ast_ref apply_expr = AST_REF_NULL;
	exp_rtype rtype;

	// Resolve the bin's CDT shape — modify / wildcard-remove return the
	// (mutated) container, so the call's rtype must match the bin. SELECT
	// TREE likewise mirrors the bin's shape; LEAF_* always returns LIST.
	ast_node* binp = ast_pool_at(pool, bin_ref);
	const ast_node* bin_canon = (binp->type == AST_BIN_REF)
			? ast_pool_at(pool, binp->u.bin_ref.bin)
			: binp;
	exp_rtype bin_rtype = ast_type_resolved(bin_canon->etype)
			? ast_etype_to_rtype(bin_canon->etype)
			: EXP_RTYPE_LIST;

	switch (op->type) {
	case AST_PATH_FUNC_SELECT:
		sel_type = op->u.modify.sel_type;
		is_apply = false;
		switch (sel_type) {
		case AS_CDT_SELECT_TREE:
			rtype = bin_rtype;
			break;
		case AS_CDT_SELECT_COUNT:
			rtype = EXP_RTYPE_INT;
			break;
		case AS_CDT_SELECT_EXISTS:
			rtype = EXP_RTYPE_TRILEAN;
			break;
		default:
			rtype = EXP_RTYPE_LIST;
			break;
		}
		break;
	case AST_PATH_FUNC_MODIFY:
		sel_type = AS_CDT_SELECT_APPLY;
		is_apply = true;
		apply_expr = op->u.modify.value;
		rtype = bin_rtype;
		break;
	default: // AST_PATH_FUNC_PSELECT_REMOVE
		sel_type = AS_CDT_SELECT_APPLY;
		is_apply = true;
		rtype = bin_rtype;
		break;
	}

	int64_t flags_int = (int64_t)sel_type | (int64_t)flags_byte;

	// [CALL, rtype, stype, select_blob, bin]
	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, (int64_t)rtype);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, (int64_t)np->u.call.stype);
	RC_RET_ON_ERR();

	// select_blob = [SELECT, ctx_pairs_list, flags_int [, apply_expr]]
	uint32_t blob_items = is_apply ? 4 : 3;
	rc = as_pack_list_header(pk, blob_items);
	RC_RET_ON_ERR();
	rc = as_pack_uint64(pk, AS_CDT_OP_SELECT);
	RC_RET_ON_ERR();
	rc = as_pack_list_header(pk, seg_count * 2);
	RC_RET_ON_ERR();

	for (ast_ref er = seg_head; er != AST_REF_NULL;
			er = ast_pool_at(pool, er)->next) {
		rc = ael_pack_ctx_seg_pair(pk, pool, input, er);
		RC_RET_ON_ERR();
	}

	rc = as_pack_int64(pk, flags_int);
	RC_RET_ON_ERR();

	if (is_apply) {
		if (apply_expr != AST_REF_NULL) {
			rc = ael_codegen_emit(pk, pool, input, apply_expr);
		}
		else {
			// PSELECT_REMOVE: synthesize [EXP_RESULT_REMOVE] — the
			// runtime sentinel that drops the matched element.
			rc = emit_cmd(pk, EXP_RESULT_REMOVE, 0);
		}
		RC_RET_ON_ERR();
	}

	return ael_codegen_emit(pk, pool, input, bin_ref);
}

//==========================================================
// ael_codegen_emit -- recursive AST-to-msgpack dispatcher.
//

// Depth guard (mirrors ael_count_sz): a left-deep chain is rejected at
// EXP_MAX_DEPTH before C-stack overflow. The sizer pass fails first, so emit
// never sees a too-deep tree. __thread: per-thread, balanced inc/dec.
int
ael_codegen_emit(as_packer* pk, ast_pool* pool, const char* input, ast_ref ref)
{
	static __thread uint32_t depth;

	if (depth >= EXP_MAX_DEPTH) {
		return -1; // surfaced by ael_codegen_pack as a "codegen failed" diag
	}

	depth++;
	cf_defer { depth--; }

	if (ref == AST_REF_NULL) {
		return as_pack_nil(pk);
	}

	ast_node* node = ast_pool_at(pool, ref);
	int rc;

	switch (node->type) {
	case AST_NIL:
		return as_pack_nil(pk);

	case AST_INF:
		return as_pack_cmp_inf(pk);

	case AST_WILDCARD:
		return as_pack_cmp_wildcard(pk);

	case AST_UNKNOWN:
		return emit_cmd(pk, EXP_UNK, 0);

	case AST_BOOL:
		return as_pack_bool(pk, node->u.bval);

	case AST_INT:
		return as_pack_int64(pk, node->u.ival);

	case AST_FLOAT:
		return as_pack_double(pk, node->u.fval);

	case AST_ZERO:
		if (node->etype == AST_ETYPE_FLOAT) {
			return as_pack_double(pk, 0.0);
		}
		return as_pack_int64(pk, 0);

	case AST_STRING: {
		if (! node->u.str.has_escape) {
			return emit_particle_string(pk, node->u.str.str, node->u.str.sz);
		}

		uint32_t decoded_sz =
				ael_string_decoded_sz(node->u.str.str, node->u.str.sz);
		uint8_t* dst;

		if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_STRING, decoded_sz,
					&dst)) {
			return -1;
		}

		if (dst != NULL) {
			ael_string_decode(node->u.str.str, node->u.str.sz, dst);
		}

		return 0;
	}

	case AST_BLOB:
		return pack_hex_blob(pk, node->u.str.str, node->u.str.sz);

	case AST_B64_BLOB:
		return pack_b64_blob(pk, node->u.str.str, node->u.str.sz);

	case AST_GEO_LITERAL:
		return as_pack_str_with_type(pk, AS_BYTES_GEOJSON,
				(const uint8_t*)node->u.str.str, node->u.str.sz);

	case AST_BIN:
		rc = emit_cmd(pk, EXP_BIN, 2);
		RC_RET_ON_ERR();
		rc = as_pack_int64(pk,
				ast_type_resolved(node->etype) ? ast_etype_to_rtype(node->etype)
											   : 0);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset, node->u.bin.name_sz);

	case AST_BIN_REF: {
		ast_node* canonical = ast_pool_at(pool, node->u.bin_ref.bin);
		rc = emit_cmd(pk, EXP_BIN, 2);
		RC_RET_ON_ERR();
		rc = as_pack_int64(pk,
				ast_type_resolved(node->etype) ? ast_etype_to_rtype(node->etype)
											   : 0);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + canonical->offset,
				canonical->u.bin.name_sz);
	}

	case AST_BIN_TYPE:
		rc = emit_cmd(pk, EXP_BIN_TYPE, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset,
				node->u.bin_type.name_sz);

	case AST_BIN_EXISTS:
		rc = emit_cmd(pk, EXP_BIN_EXISTS, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset,
				node->u.bin_type.name_sz);

	case AST_VAR:
		rc = emit_cmd(pk, EXP_VAR, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset, node->u.var.name_sz);

	case AST_LOOP_VAR: {
		rc = emit_cmd(pk, EXP_VAR_BUILTIN, 2);
		RC_RET_ON_ERR();

		exp_rtype rt = ast_loop_var_rtype(node);

		rc = as_pack_int64(pk, (int64_t)rt);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, (int64_t)node->u.loop_var.builtin);
	}

	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN:
		return emit_nmath(pk, pool, input, ref);

	case AST_AND:
	case AST_OR:
	case AST_BIT_AND:
	case AST_BIT_OR:
	case AST_BIT_XOR:
	case AST_EXCLUSIVE:
		return emit_nary(pk, pool, input, ref);

	case AST_SUB:
	case AST_DIV: {
		int cmd = ast_node_table[node->type].exp_cmd;
		uint32_t nargs = count_left_fold(pool, ref, node->type);
		rc = emit_cmd(pk, cmd, nargs);
		RC_RET_ON_ERR();
		return emit_left_fold(pk, pool, input, ref, node->type);
	}

	case AST_CMP_IN:
	case AST_CMP_EQ:
	case AST_CMP_NE:
	case AST_CMP_GT:
	case AST_CMP_GE:
	case AST_CMP_LT:
	case AST_CMP_LE:
	case AST_CMP_GEO:
	case AST_MOD:
	case AST_POW:
	case AST_LSHIFT:
	case AST_RSHIFT_ARITH:
	case AST_RSHIFT_LOGIC:
		return emit_binary(pk, pool, input, ref);

	case AST_NOT:
	case AST_BIT_NOT:
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
	case AST_PATH_FUNC_CAST_STRING:
		rc = emit_cmd(pk, ast_node_table[node->type].exp_cmd, 1);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, node->u.unary.operand);

	case AST_FUNC_ABS:
	case AST_FUNC_CEIL:
	case AST_FUNC_FLOOR:
	case AST_FUNC_COUNT_ONE_BITS:
		return emit_func1(pk, pool, input, ref);

	case AST_FUNC_LOG:
	case AST_FUNC_POW:
	case AST_FUNC_FIND_BIT_LEFT:
	case AST_FUNC_FIND_BIT_RIGHT:
		return emit_func2(pk, pool, input, ref);

	case AST_META:
		return emit_meta(pk, pool, ref);

	case AST_LIST:
		rc = emit_cmd(pk, EXP_QUOTE, 1);
		RC_RET_ON_ERR();
		return emit_list(pk, pool, input, ref);

	case AST_MAP:
		return emit_map(pk, pool, input, ref);

	case AST_LET:
		return emit_with(pk, pool, input, ref);

	case AST_WHEN:
		return emit_when(pk, pool, input, ref);

	case AST_PATH_CALL:
		return emit_path_call(pk, pool, input, ref);

	default:
		break;
	}

	return -1;
}

//==========================================================
// Public API.
//

ael_codegen_result
ael_codegen_pack(ast_pool* pool, const char* input, ast_ref root)
{
	ael_codegen_result result = { .buf = NULL, .buf_sz = 0 };

	// Sizer pass — buffer-dependent validation (ordered lists, dup keys) is
	// dormant with no buffer, so run it with diags off. A failure here is a
	// structural check that fires in both passes (e.g. an unresolved path-call
	// type), so re-run the sizer with diags on to capture that located
	// diagnostic -- buffer == NULL keeps the buffer-dependent checks dormant --
	// instead of falling back to the generic message below.
	as_packer sizer = { 0 };
	pool->diags = NULL;

	if (ael_codegen_emit(&sizer, pool, input, root) != 0) {
		as_packer diag_sizer = { 0 };

		pool->diags = &result.diags;
		ael_codegen_emit(&diag_sizer, pool, input, root);
		pool->diags = NULL;

		if (! ael_diag_has_error(&result.diags)) {
			ael_diag_add(&result.diags, AEL_SEV_ERROR, 0, 0,
					"internal error: expression could not be compiled");
		}

		return result;
	}

	uint32_t sz = sizer.offset;
	uint8_t* buf = cf_malloc(sz);

	as_packer pk = { .buffer = buf, .capacity = sz };

	// Emit pass — sub-program validation appends to result.diags via
	// pool->diags. Cleared after to avoid stale pointer.
	pool->diags = &result.diags;

	int rc = ael_codegen_emit(&pk, pool, input, root);

	pool->diags = NULL;

	if (rc != 0) {
		cf_free(buf);

		if (! ael_diag_has_error(&result.diags)) {
			ael_diag_add(&result.diags, AEL_SEV_ERROR, 0, 0,
					"internal error: expression could not be compiled");
		}

		return result;
	}

	result.buf = buf;
	result.buf_sz = pk.offset;
	return result;
}

// Release a ael_codegen_result's heap buffer (cf_malloc'd by ael_codegen_pack). Safe on
// a failed result (buf == NULL — cf_free(NULL) is a no-op). Callers use this
// rather than free()-ing buf directly.
void
ael_codegen_result_destroy(ael_codegen_result* r)
{
	cf_free(r->buf);
	r->buf = NULL;
	r->buf_sz = 0;
	ael_diag_list_destroy(&r->diags);
}
