/*
 * ael_emit.h
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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "exp/ast.h"

//==========================================================
// AEL emit helpers.
//
// The msgpack-emission primitives (AST -> wire) shared by the two AEL
// emitters: exp_ael.c's runtime build pass (ael_build_node) and ael_codegen.c's
// static emitter. The complementary parse phase -- grammar actions that build
// and validate the AST -- lives in ael_actions.h.

// Pack one ctx-pair: (ctx_type, value). Handles every seg category --
// BY_EXP (wildcard / wildcard-with-filter), AND_EXP (post-filter),
// REL_RANGE, LIST_SEG, PLURAL (range / interval), and single-element.
// Used by both the CONTEXT_EVAL chain and the SELECT op's ctx-pair
// list. For BY_EXP / AND_EXP segs, the emitted sub-program msgpack
// is validated via `as_exp_build_buf`; if the runtime would reject
// it, a slow-path re-parse of the sub-AEL appends a AEL-position
// diagnostic to `pool->diags` (when set).
int ael_pack_ctx_seg_pair(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref seg_ref);

// Literal emitters — the single source of literal msgpack bytes for BOTH
// emitters (ael_codegen_emit and exp_ael.c's ael_pack_literal). Scalar covers
// every non-collection literal node (returns -1 on a non-literal type).
// The list/map emitters own the collection shape — element/pair headers,
// the :ORDERED / :UNORDERED ext headers, and the ordered/dup-key buffer
// validations — while `child_emit` keeps element dispatch per-context:
// the wire form (ael_codegen_emit) QUOTE-wraps a nested list, the raw
// runtime-literal form (ael_pack_literal) stays raw all the way down.
typedef int (*ael_lit_child_emit)(as_packer* pk, ast_pool* pool,
		const char* input, ast_ref child);

int ael_emit_scalar_literal(as_packer* pk, const ast_node* node);
int ael_emit_list_literal(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ael_lit_child_emit child_emit);
int ael_emit_map_literal(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ael_lit_child_emit child_emit);

// Select-family (.select() / .modify() / wildcard-.remove()) wire shape,
// resolved from the pf node + the path's bin. modify / wildcard-remove
// return the (mutated) container, so rtype mirrors the bin; SELECT TREE
// matches the bin's shape; COUNT -> INT, EXISTS -> TRILEAN, LEAF_* ->
// LIST. Single-sourced so the customer-visible rtype can't drift between
// the two emitters.
typedef struct {
	as_cdt_select_flags sel_type;
	bool is_apply;
	ast_ref apply_expr; // AST_REF_NULL unless .modify()
	exp_rtype rtype;
} ael_select_shape_t;

ael_select_shape_t ael_select_shape(ast_pool* pool, ast_ref pf_ref,
		ast_ref bin_ref);

// Pack the SELECT op blob (no CALL wrapper, no ctx wrapper):
//   [SELECT, [ (ctx_type, value), ... ], flags_int [, apply_blob]]
// Wildcard segs emit (AS_CDT_CTX_EXP, true) for bare `*` or
// (AS_CDT_CTX_EXP, filter_expr_msgpack) for `*[?(filter)]`. .modify()
// appends its apply expression; wildcard-.remove() synthesizes the
// one-op [EXP_RESULT_REMOVE] apply. Shared by both emitters.
int ael_pack_select_op_blob(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref ctx_seg_head, uint32_t seg_count, ast_ref pf_ref,
		const ael_select_shape_t* shape);

// A CDT op's trailing-flag wire shape, derived once from (op, props) and
// shared by both emitters so the trailing-arg order/pad contract
// ([args..., sort_flags?, create_flags?, modify_flags?]) is
// single-sourced. sort() :DROP_DUPS rides the op's own FLAGS param; the
// create slot appears when a create-order flag is set, or as a
// pad-to-position before a modify flag on a two-slot op.
typedef struct {
	uint64_t sort_flags;
	uint64_t create_flags;
	uint64_t modify_flags;
	bool emit_sort;
	bool emit_create;
	bool emit_modify;
	uint32_t extra; // trailing wire-arg count
} ael_cdt_flag_shape_t;

ael_cdt_flag_shape_t ael_cdt_flag_shape(int cdt_op, ast_prop_bits props,
		ast_node_t op_type);

// Pack the trailing flag args per the shape (no-op when extra == 0).
int ael_emit_cdt_trailing_flags(as_packer* pk, const ael_cdt_flag_shape_t* fs);

// Whether an op has a create-flags or modify-flags slot at all -- the
// per-op table the trailing-flag shape is derived from.
bool ael_cdt_op_has_create_flags_slot(int cdt_op);
bool ael_cdt_op_has_modify_flags_slot(int cdt_op);

// Reserve space in pk for a typed-string body (str header + type byte
// + sz body bytes), handing the caller a pointer to write the body
// in-place. Returns true on success; on success *buf_r is either the
// write position (packing mode) or NULL (sizing mode, pk->buffer ==
// NULL). Returns false on capacity-exceeded / offset-overflow.
//
// Interim placement -- the proper home is as_msgpack.c next to
// as_pack_str_with_type; migrate on the next common-module bump.
bool ael_pack_str_with_type_reserve(as_packer* pk, uint8_t type, uint32_t sz,
		uint8_t** buf_r);
