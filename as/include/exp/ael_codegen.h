/*
 * ael_codegen.h
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

#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "exp/ael_diag.h"
#include "exp/ast.h"

typedef struct ael_codegen_result_s {
	uint8_t* buf;
	uint32_t buf_sz;
	ael_diag_list diags;
} ael_codegen_result;

ael_codegen_result ael_codegen_pack(ast_pool* pool, const char* input,
		ast_ref root);

// Release a ael_codegen_result's heap buffer (cf_malloc'd by ael_codegen_pack). Safe on
// a failed result (buf == NULL). Callers must use this rather than free()-ing
// buf directly.
void ael_codegen_result_destroy(ael_codegen_result* r);

// AST → msgpack emission for a single sub-tree, without round-tripping
// through ael_codegen_pack's two-pass alloc + emit.
int ael_codegen_emit(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);

// Pack a CDT context-eval wrapper -- [CONTEXT_EVAL, [ctx_type, key, ...]] --
// ahead of the caller's inner expression. Pure msgpack, so shared by the
// static codegen path and the ael-build runtime path.
// pre:  ctx_ele_head heads ctx_seg_count context segs in pool; their operands
//       are always literals (parse construction bars dyn segs from a ctx list).
// post: the wrapper header + ctx_seg_count key/value pairs are packed into pk;
//       the inner expression is left to the caller. Returns a non-zero as_pack
//       rc on packer error.
int ael_pack_ctx(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref ctx_ele_head, uint32_t ctx_seg_count);
