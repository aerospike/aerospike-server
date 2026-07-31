/*
 * ael_parse.h
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

#include "exp/ael_diag.h"
#include "exp/ast.h"

typedef struct ael_parse_result_s {
	ast_ref root;
	ast_ref bin_root; // head of the deduped canonical AST_BIN list (bin_next chain)
	ast_ref local_bin_root; // head of $.bin:LOCAL:T AST_BIN list (off bin_root)
	ael_diag_list diags;
	const char* input;
} ael_parse_result;

static inline bool
ael_parse_has_error(const ael_parse_result* r)
{
	return ael_diag_has_error(&r->diags);
}

// Shared parse entry; ael_parse / ael_parse_filter_body wrap it with a preset
// depth. initial_filter_depth pre-seeds ael_context.filter_depth: 0 for a
// top-level expression, 1 for a filter / modify body (loop vars @ / @key /
// @index parse at the top level).
// pre:  input is input_sz bytes; pool is a fresh pool owned by the caller.
// post: an ael_parse_result. On success root / bin_root / local_bin_root are
//       set; on error result.root stays AST_REF_NULL and the caller destroys
//       the pool.
ael_parse_result ael_parse_with_depth(ast_pool* pool, const char* input,
		uint32_t input_sz, uint8_t initial_filter_depth);

// The parse primitive under ael_parse_with_depth: drive the lexer and lemon
// parser over ctx->input, feeding tokens and collecting results into *ctx.
// Defined in the generated parser (ael_parser.y) -- owns the lemon-internal,
// stack-allocated yyParser (no malloc on the parse path), sized only there.
// This declaration and that %code definition are a hand-kept contract with no
// cross-TU compiler check: change the signature in both, and regenerate the
// checked-in ael_parser.c.copy so the fallback build stays in sync.
// pre:  ctx is initialized (input, pool, filter_depth) with empty diags.
// post: ctx->root / ctx->diags / bin chains reflect the parse; the caller
//       runs the post-parse passes and assembles the result.
void ael_run_parser(ael_context* ctx, uint32_t input_sz);

static inline ael_parse_result
ael_parse(ast_pool* pool, const char* input, uint32_t input_sz)
{
	return ael_parse_with_depth(pool, input, input_sz, 0);
}

// Parse a filter / modify body substring (the bytes between `?(` and `)`).
// Pre-sets filter_depth = 1 so loop vars are accepted at the top level. Used
// by codegen's slow-path diagnostic.
static inline ael_parse_result
ael_parse_filter_body(ast_pool* pool, const char* input, uint32_t input_sz)
{
	return ael_parse_with_depth(pool, input, input_sz, 1);
}
