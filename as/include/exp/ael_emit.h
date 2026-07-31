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
// emitters: exp.c's runtime build pass (ael_build_node) and ael_codegen.c's
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

// Pack the inner CDT op blob (no ctx wrapper):
// `[op_code, param0, param1, ..., create_flags?, modify_flags?]`.
// Params come from `cdtprm_push_leaf_elems` in ael_actions.c -- always
// literals post-dyn-removal. AST_LIST entries (transmuted from LIST_SEG)
// are recursed into. `props_flags` is the AST_PROP_* bitmask from the
// path-func node — when non-zero and the op supports trailing flag args
// at the wire level, the projection appends them (padding create_flags=0
// where the op shape requires it).
int ael_emit_cdt_op_blob(as_packer* pk, ast_pool* pool, const char* input,
		int cdt_op, ast_ref param_head, uint8_t param_count,
		ast_prop_bits props_flags);

// Wire-shape helpers — used by ael_emit_cdt_op_blob's internals and by the
// AEL fast path's ael_pack_call_blob (exp.c). Same per-op rules either
// way.
uint64_t ael_prop_to_cdt_flag(ast_prop_bits props);
// AST_PROP_CR_* / PERSIST_INDEX -> a CDT op's create_flags slot (packed-flag
// space AS_PACKED_MAP_FLAG_* / AS_PACKED_LIST_FLAG_*). Distinct from the
// CTX-nav create space (prop_to_ctx_create, 0x40/0x80/0xc0) -- don't conflate
// them. UNORDERED is the packed default, so it projects to 0.
uint64_t ael_prop_to_cdt_create_flag(ast_prop_bits props);
uint64_t ael_prop_to_select_flag(ast_prop_bits props);
// list sort() :DROP_DUPS -> the AS_CDT_SORT_* flags int. Distinct from the
// modify-flags projection above: sort's flag is the op's own (optional) FLAGS
// param, not a trailing modify slot. Returns 0 when the prop is absent (bare
// sort() emits no flags arg).
uint64_t ael_sort_flags(ast_prop_bits props);
bool ael_cdt_op_has_create_flags_slot(int cdt_op);
bool ael_cdt_op_no_modify_flags_slot(int cdt_op);

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
