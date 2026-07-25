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

#include "exp/ast.h"
#include "exp/parse_context.h"

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
ast_ref ael_ctx_list_append(parse_context* ctx, ast_ref ctx_ref, ast_ref seg);
ast_ref ael_ctx_list_pop(parse_context* ctx, ast_ref ctx_ref);

// `:PROPERTY` postfix — generic apply helper. Validates the incoming bit
// against the per-attachment valid mask, detects duplicates and group-
// exclusion (CR_LIST_* / CR_MAP_* mutually exclusive), and ORs
// the bit into *dst on success. Returns false (and emits a diagnostic
// via ael_diag_add) on any failure.
bool ael_apply_prop(parse_context* ctx, ast_prop_bits valid, ast_prop_bits* dst,
		ast_prop_bits bit, uint32_t prop_offset, uint32_t prop_sz);

// `:ORDERED` / `:UNORDERED` postfix on an AST_LIST / AST_MAP literal. Matches
// the property name by text (not a keyword) and sets the node's
// order_override; emits a diagnostic for any other name.
void ael_apply_literal_order(parse_context* ctx, ast_ref ref, uint32_t off,
		uint32_t sz);

// Resolve a `:PROPERTY` postfix flag name (TOK_NAME, not a keyword) to its
// AST_PROP_* bit. Returns 0 and emits a diagnostic for an unknown name.
ast_prop_bits ael_resolve_prop_flag(parse_context* ctx, uint32_t off,
		uint32_t sz);

// Resolve a `$.NAME(...)` record-level call (TOK_NAME, not a keyword) to the
// record key accessor or a metadata function. has_param marks the `$.NAME(INT)`
// form. Emits a diagnostic + returns a nil node on mismatch.
ast_ref ael_resolve_meta_call(parse_context* ctx, uint32_t off, uint32_t sz,
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

ast_ref ael_node_apply_postfix(parse_context* ctx, ast_ref ref,
		ael_postfix_kind kind, uint32_t bit);

// {S:E~K} relative-range endpoints — the wire count is end - start + 1,
// computed in int64 by cdtprm_push_leaf_elems / codegen. Reject a span whose
// count overflows (e.g. {-1:INT64_MAX~K}; the +1 means even {0:INT64_MAX~K}
// overflows) with a diagnostic. Both endpoints are AST_INT when called.
bool ael_rel_range_count_ok(parse_context* ctx, ast_ref start, ast_ref end);

// morph — path func → cdt op
bool ael_cdt_op_from_fn(parse_context* ctx, ast_ref ctx_ref, ast_ref seg_ref,
		ast_ref pf);

// path — path-call finalize / operands
ast_ref ael_finalize_path_call_wrap(parse_context* ctx, ast_ref ctx_ref,
		ast_ref pf);
ast_ref ael_consume_leaf_then_finalize(parse_context* ctx, ast_ref ctx_ref,
		ast_ref pf, ast_ref leaf);
ast_ref ael_finalize_cast(parse_context* ctx, ast_ref ctx_ref, ast_ref cast);
ast_ref ael_build_bin_func(parse_context* ctx, ast_ref bin, ast_ref pf);

// BLOB-bit method-style calls. ael_new_bit_fn builds a transient
// AST_PATH_FUNC_BIT_* node carrying the parsed args; the wrapping
// `operand ::= bit_recv . bit_fn` rule attaches the receiver via
// ael_finalize_bit_call. ael_bit_recv_from_ctx folds a path into a
// self-contained value-producing sub-expression suitable as a
// receiver (the runtime BIT_OP has no CDT-style path navigation).
ast_ref ael_new_bit_fn(parse_context* ctx, ast_node_t pf_type, ast_ref offset,
		ast_ref size, ast_ref opt_arg);
// bitAdd / bitSubtract — when signed flag is present, emits 5 wire
// args [offset, size, value, flags=0, subflags=SIGNED]; otherwise 3.
ast_ref ael_new_bit_arith(parse_context* ctx, ast_node_t pf_type,
		ast_ref offset, ast_ref size, ast_ref value, int signed_flag);
// Single-arg modify (bitResize), 2-arg byte-pair modifies (bitInsert,
// bitRemove). Distinct from offset/size shape — separate helpers keep
// the parser actions simple.
ast_ref ael_new_bit_resize(parse_context* ctx, ast_ref byte_size);
ast_ref ael_new_bit_insert(parse_context* ctx, ast_ref byte_offset,
		ast_ref value);
ast_ref ael_new_bit_remove(parse_context* ctx, ast_ref byte_offset,
		ast_ref byte_size);
ast_ref ael_bit_recv_from_ctx(parse_context* ctx, ast_ref ctx_ref);
ast_ref ael_finalize_bit_call(parse_context* ctx, ast_ref recv, ast_ref bit_fn);

// Single-select-path bit-modify simulation. Wraps the modify call in a
// LIST_SET (for LIST_INDEX leaf) or MAP_REPLACE (for MAP_KEY leaf) at
// the parent ctx — encodes `$.bin.p0.p1.bitModFn(args)` as the future
// BIT_EVAL wire op would semantically. Rejects unsupported leaf shapes.
ast_ref ael_finalize_bit_mod_path(parse_context* ctx, ast_ref ctx_ref,
		ast_ref bit_fn);

// HLL method-style calls — same shape as the bit-call helpers. The
// receiver gets pinned to HLL; ael_finalize_hll_call morphs the
// transient AST_PATH_FUNC_HLL_* node to AST_HLL_OP and wraps in an
// AST_PATH_CALL with stype=EXP_CALL_HLL.
ast_ref ael_new_hll_fn(parse_context* ctx, ast_node_t pf_type, ast_ref a1,
		ast_ref a2, ast_ref a3);
ast_ref ael_new_hll_set_fn(parse_context* ctx, ast_node_t pf_type, ast_ref arg);
ast_ref ael_new_hll_init(parse_context* ctx, ast_ref index_bits,
		ast_ref min_hash_bits);
ast_ref ael_new_hll_add(parse_context* ctx, ast_ref list, ast_ref index_bits,
		ast_ref min_hash_bits);
ast_ref ael_finalize_hll_call(parse_context* ctx, ast_ref recv, ast_ref hll_fn);

// Table-driven function calls. The grammar collects a generic argument list
// (positional `expr` and named `name: expr` entries via ael_new_*_arg /
// ael_*_arg_list) and the resolvers bind it against ael_func_table:
//   ael_resolve_func_call  — top-level `name(args)`: SCALAR / GEO / concat
//                            (BIT / HLL / PATH need a receiver).
//   ael_resolve_method_fn  — `name(args)` after a receiver dot (BIT / HLL),
//                            producing a transient op node.
//   ael_finalize_method_call[_path] — attach the receiver and pin its type,
//                            routing to the bit or hll finalizer by node type.
ast_ref ael_new_named_arg(parse_context* ctx, uint32_t name_offset,
		uint32_t name_sz, ast_ref value);
ast_ref ael_new_positional_arg(parse_context* ctx, ast_ref value);
ast_ref ael_new_arg_list(parse_context* ctx, ast_ref arg);
ast_ref ael_new_empty_arg_list(parse_context* ctx);
ast_ref ael_append_arg(parse_context* ctx, ast_ref list, ast_ref arg);
ast_ref ael_resolve_func_call(parse_context* ctx, uint32_t name_off,
		uint32_t name_sz, ast_ref arg_list);
ast_ref ael_resolve_method_fn(parse_context* ctx, uint32_t name_off,
		uint32_t name_sz, ast_ref arg_list);
ast_ref ael_finalize_method_call(parse_context* ctx, ast_ref recv,
		ast_ref method_fn);
ast_ref ael_finalize_method_call_bin(parse_context* ctx, ast_ref recv,
		ast_ref method_fn);
ast_ref ael_finalize_method_call_path(parse_context* ctx, ast_ref ctx_ref,
		ast_ref method_fn);

// `expr =~ /pattern/flags`. Lowers to a REGEX_COMPARE string call on lhs
// (pinned STRING) -> TRILEAN. pat_* / flag_* are source spans of the regex
// literal (pattern between the slashes; flag run after). Unsupported flags
// (x / w -- no wire bit) produce a diagnostic and AST_REF_NULL.
ast_ref ael_new_regex_match(parse_context* ctx, ast_ref lhs, uint32_t pat_off,
		uint32_t pat_sz, uint32_t flag_off, uint32_t flag_sz);

// Regex literal used as a `pattern:` named argument (regexReplace). Builds a
// transient AST_REGEX_LIT (pattern string + flags int); ael_build_regex_replace
// consumes it. Same flag rules as ael_new_regex_match (x / w -> diagnostic).
ast_ref ael_new_regex_operand(parse_context* ctx, uint32_t pat_off,
		uint32_t pat_sz, uint32_t flag_off, uint32_t flag_sz);

// Geo builtins. `geoJson('...')` produces an AST_GEO_LITERAL whose
// u.str holds the source JSON bytes (offset+sz into input). The runtime
// validates the JSON at decode time; the parser stays cheap.
// `geoCompare(a, b)` pins both args to GEOJSON and builds an
// AST_CMP_GEO binary that emits as EXP_CMP_GEO (returns BOOLEAN).
ast_ref ael_new_geo_literal(parse_context* ctx, uint32_t offset, uint32_t sz);
ast_ref ael_new_geo_compare(parse_context* ctx, ast_ref a, ast_ref b);

// (expr).func() — value-recv path-func dispatch. Mirrors
// ael_build_bin_func but with an expression instead of a bin. Rejects
// bin-specific ops (exists, type) and multi-select-requiring reads
// (getKeys/getKeyValues/getTree) with helpful diagnostics.
ast_ref ael_build_value_func(parse_context* ctx, ast_ref recv, ast_ref pf);

// path expressions — .select() / .modify() / wildcard-.remove() construction
// and finalize.
ast_ref ael_new_select(parse_context* ctx, as_cdt_select_flags sel_type,
		bool nofail);
ast_ref ael_new_modify(parse_context* ctx, ast_ref apply_expr, bool nofail);
ast_ref ael_new_pselect_remove(parse_context* ctx, bool nofail);
ast_ref ael_finalize_select_call(parse_context* ctx, ast_ref ctx_ref, ast_ref pf);

// path expressions — loop variable @, @key, @index. Diagnoses if used
// outside an enclosing wildcard filter / modify body. tok_offset and
// tok_sz are the offsets of the token in the source for diagnostics.
ast_ref ael_new_loop_var(parse_context* ctx, as_exp_builtin builtin,
		uint32_t tok_offset, uint32_t tok_sz);

// path expressions — filter / modify scope. push() bumps filter_depth
// to 1 on entry to a filter or modify body; pop() unifies the loop-var
// chains accumulated in the scope and resets. Nested sub-programs are
// rejected at push (push returns false on a depth > 0 entry).
bool ael_filter_scope_push(parse_context* ctx);
void ael_filter_scope_pop(parse_context* ctx);

// path expressions — wildcard seg with attached filter expression. Used
// by `*[?(filter)]` after the filter scope has been popped. body_offset
// and body_sz are the source byte range between `?(` and `)`, used by
// codegen's slow-path diagnostic.
ast_ref ael_new_wild_filter_seg(parse_context* ctx, ast_ref filter_expr,
		uint32_t body_offset, uint32_t body_sz);

// path expressions — extend a ctx_list with an AND_EXP post-filter seg
// `&[?(filter)]`. Validates pairing constraints (cannot follow wildcard
// or another AND_EXP) and emits a diagnostic on failure. body_offset
// and body_sz mark the source byte range of the body content.
ast_ref ael_extend_with_and_seg(parse_context* ctx, ast_ref ctx_ref,
		ast_ref filter_expr, uint32_t body_offset, uint32_t body_sz);

// when() — narrow case condition toward TRILEAN; if its etype is fully
// resolved and shares no bit with TRILEAN, emit a diagnostic. Deferable
// conditions are left for downstream narrowing.
void ael_check_when_cond(parse_context* ctx, ast_ref cond);
