/*
 * ael_actions.h
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

#include <stdbool.h>
#include <stdint.h>

#include "exp/ael_context.h"
#include "exp/ast.h"

//==========================================================
// Public API.
//

// ptype — particle type (expr → particle)
int64_t ael_etype_to_particle_type(ast_etype etype);

// Map a lexer error token (TOK_ERROR_*) to its diagnostic message; called from
// the parser driver's token loop (ael_run_parser).
// pre:  tok < 0 (a lexer error code from lexer_next).
// post: the fixed message for tok; "unexpected character" for an unknown code.
const char* ael_lex_error_msg(int tok);

// ctx — path-context list
ast_ref ael_ctx_list_append(ael_context* ctx, ast_ref ctx_ref, ast_ref seg);
ast_ref ael_ctx_list_pop(ael_context* ctx, ast_ref ctx_ref);

// A path root has to be a container, and a value loop var arrives polymorphic,
// so the first seg's map / list nature is what pins it. An explicit type is
// narrowed against, never overwritten -- a disagreement is a conflict.
void ael_pin_path_root_etype(ael_context* ctx, ast_ref head, ast_ref seg);

// post: true, or false having emitted the diagnostic.
bool ael_check_path_root_is_container(ael_context* ctx, ast_ref head);

// `:ORDERED` / `:UNORDERED` postfix on an AST_LIST / AST_MAP literal. Matches
// the property name by text (not a keyword) and sets the node's
// order_override; emits a diagnostic for any other name.
void ael_apply_literal_order(ael_context* ctx, ast_ref ref, uint32_t off,
		uint32_t sz);

// Resolve a `:PROPERTY` postfix flag name (TOK_NAME, not a keyword) to its
// AST_PROP_* bit. Returns 0 and emits a diagnostic for an unknown name.
ast_prop_bits ael_resolve_prop_flag(ael_context* ctx, uint32_t off, uint32_t sz);

// Resolve a `$.NAME(...)` record-level call (TOK_NAME, not a keyword) to the
// record key accessor or a metadata function. has_param marks the `$.NAME(INT)`
// form. Emits a diagnostic + returns a nil node on mismatch.
ast_ref ael_resolve_meta_call(ael_context* ctx, uint32_t off, uint32_t sz,
		int64_t param, bool has_param);

// Unified postfix decorator apply: a single `:VALUE` postfix step on
// the grammar reduces to this. The kind discriminator picks the
// validation mask (info->valid_props vs info->valid_types) and the
// accumulator slot (u.{cdt_op,seg,modify}.props vs node->etype).
// `bit` is an AST_PROP_* bit (for KIND_PROP) or an AST_ETYPE_* bit
// (for KIND_TYPE). For AST_PATH_CALL refs the helper follows
// u.call.call_op to the inner morphed BIT_OP / HLL_OP.
typedef enum {
	AEL_POSTFIX_PROP,
	AEL_POSTFIX_TYPE,
} ael_postfix_kind;

ast_ref ael_node_apply_postfix(ael_context* ctx, ast_ref ref,
		ael_postfix_kind kind, uint32_t bit);

// {S:E~K} relative-range endpoints — the wire count is end - start + 1,
// computed in int64 by cdtprm_push_leaf_elems / codegen. Reject a span whose
// count overflows (e.g. {-1:INT64_MAX~K}; the +1 means even {0:INT64_MAX~K}
// overflows) with a diagnostic. Both endpoints are AST_INT when called.
bool ael_rel_range_count_ok(ael_context* ctx, ast_ref start, ast_ref end);

// path — path-call finalize / operands
ast_ref ael_consume_leaf_then_finalize(ael_context* ctx, ast_ref ctx_ref,
		ast_ref pf, ast_ref leaf);

// Table-driven function calls. The grammar collects a generic argument list
// (positional `expr` and named `name: expr` entries via ael_new_*_arg /
// ael_*_arg_list) and the resolvers bind it against ael_func_table:
//   ael_resolve_func_call  — top-level `name(args)`: SCALAR / GEO / concat
//                            (BIT / HLL / PATH need a receiver).
//   ael_resolve_method_fn  — `name(args)` after a receiver dot (BIT / HLL),
//                            producing a transient op node.
//   ael_finalize_method_call[_path] — attach the receiver and pin its type,
//                            routing to the bit or hll finalizer by node type.
ast_ref ael_new_named_arg(ael_context* ctx, uint32_t name_offset,
		uint32_t name_sz, ast_ref value);
ast_ref ael_new_positional_arg(ael_context* ctx, ast_ref value);
ast_ref ael_new_arg_list(ael_context* ctx, ast_ref arg);
ast_ref ael_new_empty_arg_list(ael_context* ctx);
ast_ref ael_append_arg(ael_context* ctx, ast_ref list, ast_ref arg);
ast_ref ael_resolve_func_call(ael_context* ctx, uint32_t name_off,
		uint32_t name_sz, ast_ref arg_list);
ast_ref ael_resolve_method_fn(ael_context* ctx, uint32_t name_off,
		uint32_t name_sz, ast_ref arg_list);
ast_ref ael_finalize_method_call(ael_context* ctx, ast_ref recv,
		ast_ref method_fn);
ast_ref ael_finalize_method_call_bin(ael_context* ctx, ast_ref recv,
		ast_ref method_fn);
ast_ref ael_finalize_method_call_path(ael_context* ctx, ast_ref ctx_ref,
		ast_ref method_fn);

// `expr =~ /pattern/flags`. Lowers to a REGEX_COMPARE string call on lhs
// (pinned STRING) -> TRILEAN. pat_* / flag_* are source spans of the regex
// literal (pattern between the slashes; flag run after). Unsupported flags
// (x / w -- no wire bit) produce a diagnostic and AST_REF_NULL.
ast_ref ael_new_regex_match(ael_context* ctx, ast_ref lhs, uint32_t pat_off,
		uint32_t pat_sz, uint32_t flag_off, uint32_t flag_sz);

// Regex literal used as a `pattern:` named argument (regexReplace). Builds a
// transient AST_REGEX_LIT (pattern string + flags int); ael_build_regex_replace
// consumes it. Same flag rules as ael_new_regex_match (x / w -> diagnostic).
ast_ref ael_new_regex_operand(ael_context* ctx, uint32_t pat_off,
		uint32_t pat_sz, uint32_t flag_off, uint32_t flag_sz);

// path expressions — loop variable @, @key, @index. Diagnoses if used
// outside an enclosing wildcard filter / modify body. tok_offset and
// tok_sz are the offsets of the token in the source for diagnostics.
ast_ref ael_new_loop_var(ael_context* ctx, as_exp_builtin builtin,
		uint32_t tok_offset, uint32_t tok_sz);

// path expressions — filter / modify scope. push() bumps filter_depth
// to 1 on entry to a filter or modify body; pop() unifies the loop-var
// chains accumulated in the scope and resets. Nested sub-programs are
// rejected at push (push returns false on a depth > 0 entry).
bool ael_create_park_push(ael_context* ctx);
void ael_create_park_pop(ael_context* ctx);

bool ael_filter_scope_push(ael_context* ctx);
void ael_filter_scope_pop(ael_context* ctx);

// path expressions — wildcard seg with attached filter expression. Used
// by `*[?(filter)]` after the filter scope has been popped. body_offset
// and body_sz are the source byte range between `?(` and `)`, used by
// codegen's slow-path diagnostic.
ast_ref ael_new_wild_filter_seg(ael_context* ctx, ast_ref filter_expr,
		uint32_t body_offset, uint32_t body_sz);

// path expressions — extend a ctx_list with an AND_EXP post-filter seg
// `&[?(filter)]`. Validates pairing constraints (cannot follow wildcard
// or another AND_EXP) and emits a diagnostic on failure. body_offset
// and body_sz mark the source byte range of the body content.
ast_ref ael_extend_with_and_seg(ael_context* ctx, ast_ref ctx_ref,
		ast_ref filter_expr, uint32_t body_offset, uint32_t body_sz);

// when() — narrow case condition toward TRILEAN; if its etype is fully
// resolved and shares no bit with TRILEAN, emit a diagnostic. Deferable
// conditions are left for downstream narrowing.
void ael_check_when_cond(ael_context* ctx, ast_ref cond);
