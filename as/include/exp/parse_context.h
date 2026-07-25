/*
 * parse_context.h
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

#include <string.h>

#include "exp/ael_diag.h"
#include "exp/ast.h"

typedef struct parse_context_s {
	ast_pool* pool;
	const char* input;
	ast_ref root;
	ael_diag_list diags;
	uint32_t last_token_offset;
	uint32_t last_token_sz;
	// Snapshot of the most-recently-reduced postfix value (type_name /
	// prop_flag) token. Captured in those reductions so the OUTER rule
	// (e.g. `path_seg ::= path_seg TOK_COLON type_name`) can point its
	// diagnostic at the offending postfix value rather than at the
	// parser-driver lookahead (which has already advanced past it by
	// the time the outer reduction runs).
	uint32_t postfix_token_offset;
	uint32_t postfix_token_sz;
	ast_ref cur_scope; // current AST_LET_LIST ref, or AST_REF_NULL
	ast_ref bin_root; // head of canonical AST_BIN linked list
	ast_ref local_bin_root; // head of $.bin:local:T AST_BIN list, off bin_root
	// Head of the $.key() (EXP_REC_KEY) node chain, linked through the
	// otherwise-unused u.meta.param field. Walked post-parse to reject a
	// key whose type never resolved (spec: no silent default types).
	ast_ref key_meta_root;
	uint32_t scope_idx; // global var slot counter

	// Depth of the ast_set_implicit_type propagation recursion, capped at
	// AEL_MAX_DEPTH. Left-associative operator chains build left-deep ASTs the
	// lemon stack (YYSTACKDEPTH) doesn't bound, so the walk caps itself here to
	// stop a huge client expression from overflowing the C stack.
	uint32_t prop_depth;

	// Filter / modify body depth — 0 (outside) or 1 (inside).
	// Nested sub-programs are rejected at parse time, so this never
	// exceeds 1. Used to validate `@` placement and to reject
	// `$.bin` / `with(...)` / `?N` inside a sub-program body.
	uint8_t filter_depth;

	// The single sub-program scope's loop-var occurrence chains. Each
	// AST_LOOP_VAR(@) / AST_LOOP_VAR(@key) is prepended on construction.
	// At scope pop the chain is unified (etype intersection across all
	// occurrences); AST_ETYPE_ERROR triggers a diagnostic. Reset to
	// AST_REF_NULL at pop so a subsequent top-level filter starts clean.
	ast_ref at_root;
	ast_ref key_root;
} parse_context;

typedef struct {
	uint32_t var_idx;
	ast_etype etype;
	bool found;
} ael_var_lookup;

// Look up a variable by name, walking the let_scope chain.
// cur_scope is an AST_LET_LIST. The let_scope node is at list.head;
// var_defs follow it as siblings. let_scope.parent points to the
// enclosing AST_LET_LIST (or AST_REF_NULL).
static inline ael_var_lookup
ael_find_var(parse_context* ctx, const char* name, uint32_t name_sz)
{
	ael_var_lookup result = { .etype = AST_ETYPE_AUTO };

	for (ast_ref lr = ctx->cur_scope; lr != AST_REF_NULL;) {
		ast_node* list = ast_pool_at(ctx->pool, lr);
		ast_node* scope = ast_pool_at(ctx->pool, list->u.list.head);
		uint32_t idx = scope->u.let_scope.start_idx;

		for (ast_ref def = scope->next; def != AST_REF_NULL;
				def = ast_pool_at(ctx->pool, def)->next) {
			ast_node* dp = ast_pool_at(ctx->pool, def);

			if (dp->type != AST_VAR_DEF) {
				break;
			}

			if (dp->u.var_def.name_sz == name_sz &&
					memcmp(ctx->input + dp->u.var_def.name_offset, name,
							name_sz) == 0) {
				ast_node* vp = ast_pool_at(ctx->pool, dp->u.var_def.value);
				result.var_idx = idx;
				result.etype = vp->etype;
				result.found = true;
				return result;
			}

			idx++;
		}

		lr = scope->u.let_scope.parent;
	}

	return result;
}
