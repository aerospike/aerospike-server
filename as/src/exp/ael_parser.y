/*
 * parser.y
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

/*
 * Text expression AEL (filter expressions): canonical sources are this file,
 * lexer.re, ast.c, ael_codegen.c, and headers in as/include/exp/.
 */

%include {
// Lock the parser stack to a fixed-size array -- no malloc on the parse path.
// Must appear before lemon's own #ifndef YYSTACKDEPTH guard.
#define YYSTACKDEPTH 512

#include <stdlib.h>
#include <string.h>

#include "exp/ael_actions.h"
#include "exp/ael_diag.h"
#include "exp/ael_parse.h"
#include "exp/exp_wire.h"
#include "exp/ael_lexer.h"

// Included unconditionally so lemon's ParseAlloc / Parse / ParseFree (always
// defined below) are cross-checked against ael_oracle.h's declarations in
// every build. ael_can_accept / ael_num_tokens exist only under AEL_ORACLE;
// an unmatched prototype there is harmless since nothing else calls them.
#include "exp/ael_oracle.h"

// Recover pointer from token_value str offset.
#define TOK_STR(ctx, v) ((ctx)->input + (v).str.offset)

// Stamp AST node R with token V's precise source span. A leaf reduction fires
// on lookahead, so ast_new's pool->cur_offset default is the NEXT token's span;
// this overrides it with the leaf token's own span (token_value.offset/sz, set
// for every token by lexer_next). Composite nodes get their span from children
// in the ast_new_* constructors instead.
#define STAMP(r, v) ast_set_span(ctx->pool, (r), (v).offset, (v).sz)

// Anchor a map-segment diagnostic on its operand node(s) instead of
// ctx->last_token_offset. By the time a `{...}` rule reduces, the lookahead has
// advanced past the closing brace -- and to EOF (zero-width) when the segment
// ends the source. Pass a second ref for a range (`a:b`), AST_REF_NULL for a
// single operand. Falls back to the last token if the operand span is empty.
static void
seg_operand_span(ael_context* ctx, ast_ref a, ast_ref b, uint32_t* offset,
		uint32_t* byte_sz)
{
	ast_node* ap = ast_pool_at(ctx->pool, a);
	uint32_t start = ast_disp_offset(ap);
	uint32_t end = start + ap->sz;

	if (b != AST_REF_NULL) {
		ast_node* bp = ast_pool_at(ctx->pool, b);
		uint32_t b_start = ast_disp_offset(bp);

		start = b_start < start ? b_start : start;
		end = b_start + bp->sz > end ? b_start + bp->sz : end;
	}

	if (end > start) {
		*offset = start;
		*byte_sz = end - start;
	}
	else {
		*offset = ctx->last_token_offset;
		*byte_sz = ctx->last_token_sz;
	}
}

// `!`-inverted singular segment — record the diagnostic (the reduction
// still builds its node so the parse continues).
static void
seg_inv_err(ael_context* ctx, ast_ref v, int inv, const char* msg)
{
	if (inv) {
		uint32_t seg_off;
		uint32_t seg_sz;

		seg_operand_span(ctx, v, AST_REF_NULL, &seg_off, &seg_sz);
		ael_err(ctx, seg_off, seg_sz, msg);
	}
}

// INT-required range segment; both endpoints must share a sign when both
// are given (count = end - start can't be computed at parse time when
// signs differ). b == AST_REF_NULL is open-end (no sign check). The
// family-specific diagnostics come in as strings so they stay verbatim.
static ast_ref
seg_int_range(ael_context* ctx, ast_node_t type, ast_ref a, ast_ref b,
		int inv, const char* type_msg, const char* sign_msg)
{
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, a, b, &seg_off, &seg_sz);

	// type_msg == NULL skips the type check (rank operands are
	// grammar-guaranteed INT).
	if (type_msg != NULL &&
			(ast_pool_at(ctx->pool, a)->type != AST_INT ||
					(b != AST_REF_NULL &&
							ast_pool_at(ctx->pool, b)->type != AST_INT))) {
		ael_err(ctx, seg_off, seg_sz, type_msg);
		return AST_REF_NULL;
	}

	if (b != AST_REF_NULL &&
			(ast_pool_at(ctx->pool, a)->u.ival < 0) !=
					(ast_pool_at(ctx->pool, b)->u.ival < 0)) {
		ael_err(ctx, seg_off, seg_sz, sign_msg);
		return AST_REF_NULL;
	}

	return ast_new_range_seg(ctx->pool, type, a, b, inv != 0);
}

// Open-start range (`{:B}` / `[:B]` shapes) — synthesized start=0
// (wire-identical to `0:B`); B must be a non-negative int (negative B
// would be mixed-sign against the synthesized 0).
static ast_ref
seg_open_start_int(ael_context* ctx, ast_node_t type, ast_ref b, int inv,
		const char* type_msg, const char* neg_msg)
{
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, b, AST_REF_NULL, &seg_off, &seg_sz);

	// type_msg == NULL skips the type check (rank operands are
	// grammar-guaranteed INT).
	if (type_msg != NULL && ast_pool_at(ctx->pool, b)->type != AST_INT) {
		ael_err(ctx, seg_off, seg_sz, type_msg);
		return AST_REF_NULL;
	}

	if (ast_pool_at(ctx->pool, b)->u.ival < 0) {
		ael_err(ctx, seg_off, seg_sz, neg_msg);
		return AST_REF_NULL;
	}

	return ast_new_range_seg(ctx->pool, type, ast_new_int(ctx->pool, 0), b,
			inv != 0);
}

// Comma-list shape in a family with no list wire op (bare index braces,
// list index, map/list rank) — consume the full shape and reject with
// guidance instead of a bare syntax error.
static ast_ref
seg_list_unsupported(ael_context* ctx, ast_ref a, const char* msg)
{
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, a, AST_REF_NULL, &seg_off, &seg_sz);
	ael_err(ctx, seg_off, seg_sz, msg);
	return AST_REF_NULL;
}

}

%token_type { token_value }
%extra_argument { ael_context* ctx }

%stack_overflow {
	ael_err(ctx, 0, 0, "expression too complex (parser stack overflow)");
}

%default_type { ast_ref }

%type expr { ast_ref }
%type operand { ast_ref }
%type literal { ast_ref }
%type list_elem { ast_ref }
%type bin_base { ast_ref }
%type exp_base { ast_ref }
%type map_key { ast_ref }
%type func_call { ast_ref }
%type path_seg { ast_ref }
%type method_fn { ast_ref }
%type method_call { ast_ref }
%type arg { ast_ref }
%type arg_list { ast_ref }
%type ctx_list { ast_ref }
%type type_name { int }
%type brace_open { int }
%type brace_eq_open { int }
%type brace_hash_open { int }
%type brace_at_open { int }
%type bracket_open { int }
%type bracket_eq_open { int }
%type bracket_hash_open { int }
%type key_val { ast_ref }
%type key_list { ast_ref }
%type int_val { ast_ref }
%type empty_list { ast_ref }
%type empty_map { ast_ref }
%type let_head { ast_ref }
%type filter_open { uint32_t }
%type and_filter_open { uint32_t }

%syntax_error {
	(void)TOKEN;

	const char* msg;

	if (yymajor == 0) {
		msg = "unexpected end of expression";
	}
	else if (yymajor == TOK_ASSIGN) {
		msg = "'=' is assignment (let only) -- use '==' to compare";
	}
	else if (yymajor == TOK_FLOAT) {
		// A dotted numeric path (`$.a.1.1`) lexes `1.1` as one float, not two
		// keys. For integer keys use the key selector {@N}; for string keys
		// quote them. `%syntax_error` fires before the float is shifted, so
		// last_token_offset still points at it.
		msg = "unexpected number with a decimal point -- for integer map keys use {@N} (e.g. $.a.{@1}.{@1}); for string keys quote them (\"1\".\"1\"); a bare 1.1 is read as a float";
	}
	else {
		msg = "syntax error";
	}

	ael_err(ctx, ctx->last_token_offset, ctx->last_token_sz, msg);
}

%parse_failure {
	ael_err(ctx, ctx->last_token_offset, ctx->last_token_sz, "parse failure");
}

// Precedence: lowest to highest.
//
// Two rows intentionally diverge from the prose spec's precedence table
// (operators-and-precedence.md §8). Comparisons are %nonassoc, not its
// "left": the spec shows no chained example, and a < b < c is a footgun
// and a type error under strict typing (chain with and/or instead).
// Bitwise | ^ & stay three C-style tiers, not its one co-equal level:
// no mixed-operator example is given, and a | b & c parses as
// a | (b & c), matching C-family expectation.
%left TOK_OR.
%left TOK_AND.
%nonassoc TOK_EQ TOK_NE TOK_GT TOK_GE TOK_LT TOK_LE TOK_IN TOK_MATCH.
%left TOK_PIPE.
%left TOK_CARET.
%left TOK_AMP.
%left TOK_LSHIFT TOK_RSHIFT TOK_RSHIFT_LOGIC.
%left TOK_PLUS TOK_MINUS.
%left TOK_STAR TOK_SLASH TOK_PERCENT.
%right TOK_POWER.
%right TOK_TILDE TOK_NOT TOK_UMINUS.

// `:` postfix on bin_base — the recursive rule (added below) conflicts
// at TOK_COLON with the existing 4-token TYPE rules. Lemon's default-
// shift resolution is correct (the 4-token rule handles the initial
// :T; the recursive rule only fires for subsequent :T). To formalize
// the resolution and silence the warning, give the bare bin_base
// rule a pseudo-token precedence lower than TOK_COLON — then the
// lookahead's higher precedence picks shift cleanly.
%nonassoc TOK_BIN_BARE_PREC.
%left TOK_COLON.

//==========================================================
// Top-level.
//

program ::= expr(E). { ctx->root = E; }

//==========================================================
// Expressions.
//

expr(R) ::= expr(A) TOK_OR expr(B).   { R = ast_nary_merge(ctx, AST_OR, A, B, AST_ETYPE_TRILEAN); }
expr(R) ::= expr(A) TOK_AND expr(B).  { R = ast_nary_merge(ctx, AST_AND, A, B, AST_ETYPE_TRILEAN); }

// Comparison.
expr(R) ::= expr(A) TOK_EQ expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_EQ, A, B); }
expr(R) ::= expr(A) TOK_NE expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_NE, A, B); }
expr(R) ::= expr(A) TOK_GT expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_GT, A, B); }
expr(R) ::= expr(A) TOK_GE expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_GE, A, B); }
expr(R) ::= expr(A) TOK_LT expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_LT, A, B); }
expr(R) ::= expr(A) TOK_LE expr(B).  { R = ast_new_bcmp(ctx, AST_CMP_LE, A, B); }
expr(R) ::= expr(A) TOK_IN expr(B).  { R = ast_new_in(ctx, A, B); }

// Regex match: `expr =~ /pattern/flags`. The RHS is a regex literal (TOK_REGEX,
// scanned by the lexer's expect_regex latch), not an expr. Lowers to a
// REGEX_COMPARE string call on the LHS (pinned STRING) -> TRILEAN.
expr(R) ::= expr(A) TOK_MATCH TOK_REGEX(P). { R = ael_new_regex_match(ctx, A, P.offset + 1, P.regex.pat_sz, P.offset + P.regex.pat_sz + 2, P.regex.flag_sz); }

// Arithmetic. SUB / DIV are polymorphic (INT or FLOAT) at the runtime
// dispatch; MOD requires INT operands and POW requires FLOAT operands,
// matching build_int_op / build_math_pow in exp.c.
expr(R) ::= expr(A) TOK_PLUS expr(B).    { R = ast_nary_merge(ctx, AST_ADD, A, B, AST_ETYPE_AUTO_ADD); }
expr(R) ::= expr(A) TOK_MINUS expr(B).   { R = ast_new_bmath(ctx, AST_SUB, A, B, AST_ETYPE_AUTO_NUMERIC); }
expr(R) ::= expr(A) TOK_STAR expr(B).    { R = ast_nary_merge(ctx, AST_MUL, A, B, AST_ETYPE_AUTO_NUMERIC); }
expr(R) ::= expr(A) TOK_SLASH expr(B).   { R = ast_new_bmath(ctx, AST_DIV, A, B, AST_ETYPE_AUTO_NUMERIC); }
expr(R) ::= expr(A) TOK_PERCENT expr(B). { R = ast_new_bmath(ctx, AST_MOD, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_POWER expr(B).   { R = ast_new_bmath(ctx, AST_POW, A, B, AST_ETYPE_FLOAT); }

// Unary minus / plus (prefix). `-x` lowers to SUB(0, x) with the zero
// typed to the operand -- there is no wire negate op. A `-` glued to a
// number (`-10`) is a literal token when the sign position allows it
// (start of an operand); after an operand-ending token the lexer splits
// it to TOK_MINUS so `$.a-1` is infix subtraction (see prev_ends_operand
// in ael_lexer.h). This rule fires for `-$.x`, `- 10`, and similar.
// `+x` is a numeric-checked identity.
expr(R) ::= TOK_MINUS(M) expr(A). [TOK_UMINUS] {
	R = ast_new_neg(ctx, A, M.offset + M.sz);
}
expr(R) ::= TOK_PLUS expr(A). [TOK_UMINUS] {
	ast_set_implicit_type(ctx, A, AST_ETYPE_AUTO_NUMERIC);
	R = A;
}

// Bitwise -- infer INT on bare-bin operands (bitwise is always integer).
expr(R) ::= expr(A) TOK_AMP expr(B).            { R = ast_nary_merge(ctx, AST_BIT_AND, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_PIPE expr(B).           { R = ast_nary_merge(ctx, AST_BIT_OR, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_CARET expr(B).          { R = ast_nary_merge(ctx, AST_BIT_XOR, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_LSHIFT expr(B).         { R = ast_new_bmath(ctx, AST_LSHIFT, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_RSHIFT expr(B).         { R = ast_new_bmath(ctx, AST_RSHIFT_ARITH, A, B, AST_ETYPE_INT); }
expr(R) ::= expr(A) TOK_RSHIFT_LOGIC expr(B).   { R = ast_new_bmath(ctx, AST_RSHIFT_LOGIC, A, B, AST_ETYPE_INT); }
expr(R) ::= TOK_TILDE expr(A). [TOK_TILDE] {
	ast_set_implicit_type(ctx, A, AST_ETYPE_INT);
	R = ast_new_unary(ctx->pool, AST_BIT_NOT, A);
	if (R != AST_REF_NULL) {
		ast_pool_at(ctx->pool, R)->etype = AST_ETYPE_INT;
	}
}

// not(...)
expr(R) ::= TOK_NOT TOK_LPAREN expr(A) TOK_RPAREN. [TOK_NOT] {
	ast_set_implicit_type(ctx, A, AST_ETYPE_TRILEAN);
	R = ast_new_unary(ctx->pool, AST_NOT, A);
	if (R != AST_REF_NULL) {
		ast_pool_at(ctx->pool, R)->etype = AST_ETYPE_TRILEAN;
	}
}

// let(x = e1, ...) then (body) — split into let_head + expr so that the
// var scope is active during parsing of the body expression.
let_head(R) ::= TOK_LET TOK_LPAREN var_def_list(D) TOK_RPAREN TOK_THEN. {
	R = D;
	// Scope was already pushed in var_def_list.
}

expr(R) ::= let_head(H) TOK_LPAREN expr(B) TOK_RPAREN. {
	if (H == AST_REF_NULL || B == AST_REF_NULL) {
		R = AST_REF_NULL;
	}
	else {
		R = H;
		ast_node* n = ast_pool_at(ctx->pool, R);
		n->type = AST_LET;

		// Append body.
		AST_PLIST_PUSH_TAIL(ctx->pool, n, B);

		// The shell was spanned over the first binding when it was created
		// (mid-construct); extend through the body so the LET's span covers
		// bindings -> body.
		ast_node* bp = ast_pool_at(ctx->pool, B);
		uint32_t let_end = ast_disp_offset(bp) + bp->sz;
		uint32_t let_start = ast_disp_offset(n);

		if (let_end > let_start) {
			uint32_t span = let_end - let_start;
			n->sz = span > 255 ? 255 : (uint8_t)span;
		}

		// Pop scope — let_scope is at list head.
		ast_node* sp = ast_pool_at(ctx->pool, n->u.list.head);
		ctx->cur_scope = sp->u.let_scope.parent;
	}
}

// when(c1 => r1, ..., default => d)
expr(R) ::= TOK_WHEN(W) TOK_LPAREN case_list(M) TOK_COMMA TOK_DEFAULT TOK_ARROW expr(D) TOK_RPAREN(RP). {
	// M is a freshly-built list (never null); only D can fail.
	if (D == AST_REF_NULL) {
		R = AST_REF_NULL;
	}
	else {
		R = M;
		ast_node* n = ast_pool_at(ctx->pool, R);
		n->type = AST_WHEN;

		// Span the whole when(...) so diagnostics (e.g. a non-boolean when used
		// as a logical operand) point at the operator, not the reused case_list.
		ast_set_span(ctx->pool, R, W.offset, RP.offset + RP.sz - W.offset);

		AST_PLIST_PUSH_TAIL(ctx->pool, n, D);

		// Validate that all case results and default have compatible
		// types. Accumulate a bitmask of all resolved result types.
		ast_etype result_types = AST_ETYPE_ERROR;

		for (ast_ref m = n->u.list.head; m != D; ) {
			ast_node* mp = ast_pool_at(ctx->pool, m);
			ast_ref res = mp->u.when_case.result;
			ast_etype rt = AST_ETYPE_ERROR;

			if (res != AST_REF_NULL) {
				rt = ast_pool_at(ctx->pool, res)->etype;
			}

			if (ast_type_resolved(rt)) {
				result_types |= rt;
			}

			m = mp->next;
		}

		ast_etype dt = ast_pool_at(ctx->pool, D)->etype;

		if (ast_type_resolved(dt)) {
			result_types |= dt;
		}

		// All resolved types must match, or all be numeric (INT|FLOAT).
		bool incompatible = result_types != AST_ETYPE_ERROR &&
				! ast_type_resolved(result_types) &&
				result_types != AST_ETYPE_AUTO_NUMERIC;

		if (incompatible) {
			ael_err(ctx, ast_disp_offset(n), n->sz,
				"when branches have incompatible types");
		}

		// Propagate the unified result type so downstream type pins /
		// inference (`when(...) :T`, comparisons) see it. When all
		// branches are unresolved, leave etype as-is — matches the
		// msgpack EXP_COND runtime which lets the default case's
		// trilean-UNK propagate as a legitimate eval-time outcome.
		if (result_types != AST_ETYPE_ERROR) {
			n->etype = result_types;
		}

		// When the branches share a single resolved type, pin every arm to it
		// so an unresolved bare bin in one branch (e.g. `$.a` beside a STRING
		// default) is inferred rather than flagged "unresolved bin type" --
		// mirrors how `$.a == 'x'` infers $.a:STRING.
		if (! incompatible && ast_type_resolved(result_types)) {
			for (ast_ref m = n->u.list.head; m != D;
					m = ast_pool_at(ctx->pool, m)->next) {
				ast_ref res = ast_pool_at(ctx->pool, m)->u.when_case.result;

				if (res != AST_REF_NULL) {
					ast_set_implicit_type(ctx, res, result_types);
				}
			}

			ast_set_implicit_type(ctx, D, result_types);
		}
	}
}

// Parenthesized expression — reduced through exp_base so the (expr)
// form can also serve as a particle-producing receiver for path-funcs,
// path-segs, and bit-calls (parallel of bin_base). For a bare paren-
// expression that's not followed by `.something`, the chain is just
// exp_base → operand → expr — a no-op compared to the prior single-
// step reduction.
exp_base(R) ::= TOK_LPAREN(LP) expr(A) TOK_RPAREN(RP). {
	R = A;

	// Widen the grouped node's displayed span over the parens so a highlight
	// of the group includes both. Applies to every kind: ast_set_display_span
	// widens without moving the node's anchor, so a grouped bin or variable
	// keeps its name locator on the name.
	ast_set_display_span(ctx->pool, R, LP.offset, RP.offset + RP.sz);
}

// Operands.
expr(R) ::= operand(A). { R = A; }

// Function calls.
expr(R) ::= func_call(A). { R = A; }

//==========================================================
// Literals (constant values only -- no bin, var, or expressions).
//

literal(R) ::= TOK_INT(V).   { R = ast_new_int(ctx->pool, V.ival); STAMP(R, V); }
literal(R) ::= TOK_FLOAT(V). { R = ast_new_float(ctx->pool, V.fval); STAMP(R, V); }
literal(R) ::= TOK_TRUE(V).  { R = ast_new_bool(ctx->pool, true); STAMP(R, V); }
literal(R) ::= TOK_FALSE(V). { R = ast_new_bool(ctx->pool, false); STAMP(R, V); }
literal(R) ::= TOK_NIL(V).   { R = ast_new_nil(ctx->pool); STAMP(R, V); }
literal(R) ::= TOK_STRING(V).       { R = ast_new_string_token(ctx, V.str.offset, V.str.sz); STAMP(R, V); }
literal(R) ::= TOK_BLOB_LITERAL(V). { R = ast_new_blob_from_hex(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }
literal(R) ::= TOK_B64_BLOB_LITERAL(V). { R = ast_new_blob_from_b64(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }

// Map keys: restricted to int, string, blob per spec.
map_key(R) ::= TOK_INT(V).          { R = ast_new_int(ctx->pool, V.ival); STAMP(R, V); }
map_key(R) ::= TOK_STRING(V).       { R = ast_new_string_token(ctx, V.str.offset, V.str.sz); STAMP(R, V); }
map_key(R) ::= TOK_NAME(V).         { R = ast_new_string(ctx->pool, TOK_STR(ctx, V), V.str.sz, false); STAMP(R, V); }
map_key(R) ::= TOK_BLOB_LITERAL(V). { R = ast_new_blob_from_hex(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }
map_key(R) ::= TOK_B64_BLOB_LITERAL(V). { R = ast_new_blob_from_b64(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }

// List literal. The list_body node accumulates elements without extending
// its span (elements append via AST_LIST_PUSH_TAIL, not a span-merging
// constructor), so the bracket rules stamp the literal over '[' .. ']' —
// otherwise the span stays frozen at the first element and any parent's
// child-derived span truncates mid-literal. The ':ORDER' postfix stays
// outside the span, like a bin's ':T' pin -- the empty_list / empty_map
// nonterminals carry the stamp so both the bare and ':ORDER' forms inherit it.
empty_list(R) ::= TOK_LBRACKET(LB) TOK_RBRACKET(RB). {
	R = ast_new(ctx->pool, AST_LIST);
	ast_pool_at(ctx->pool, R)->etype = AST_ETYPE_LIST;

	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

literal(R) ::= empty_list(L). { R = L; }

literal(R) ::= TOK_LBRACKET(LB) list_body(L) TOK_RBRACKET(RB). {
	R = L;
	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

// Map literal.
empty_map(R) ::= TOK_LBRACE(LB) TOK_RBRACE(RB). {
	R = ast_new(ctx->pool, AST_MAP);
	ast_pool_at(ctx->pool, R)->etype = AST_ETYPE_MAP;

	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

literal(R) ::= empty_map(M). { R = M; }

literal(R) ::= TOK_LBRACE(LB) map_body(M) TOK_RBRACE(RB). {
	R = M;
	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

// `:ORDERED` / `:UNORDERED` ordering override on a collection literal. The
// property is a plain TOK_NAME (not a keyword) — ael_apply_literal_order
// matches it by text. Scoping the postfix to the brace/bracket forms keeps a
// scalar postfix (e.g. 5:ORDERED) a syntax error.
literal(R) ::= empty_list(L) TOK_COLON TOK_NAME(N). {
	ael_apply_literal_order(ctx, L, N.str.offset, N.str.sz);
	R = L;
}

literal(R) ::= TOK_LBRACKET(LB) list_body(L) TOK_RBRACKET(RB) TOK_COLON TOK_NAME(N). {
	ael_apply_literal_order(ctx, L, N.str.offset, N.str.sz);
	R = L;
	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

literal(R) ::= empty_map(M) TOK_COLON TOK_NAME(N). {
	ael_apply_literal_order(ctx, M, N.str.offset, N.str.sz);
	R = M;
}

literal(R) ::= TOK_LBRACE(LB) map_body(M) TOK_RBRACE(RB) TOK_COLON TOK_NAME(N). {
	ael_apply_literal_order(ctx, M, N.str.offset, N.str.sz);
	R = M;
	ast_set_span(ctx->pool, R, LB.offset, RB.offset + RB.sz - LB.offset);
}

//==========================================================
// Operands (leaf nodes).
//

operand(R) ::= literal(A). { R = A; }

operand(R) ::= TOK_UNKNOWN. { R = ast_new(ctx->pool, AST_UNKNOWN); }

// A bare identifier is never a valid operand: bins are `$.name`, functions
// are `name(...)`, and variables use their own token. Catch it here so the
// error points at the name rather than the following token -- otherwise the
// parser shifts the name as a would-be func-call prefix and only fails at
// the next token (e.g. `flag and (...)` used to caret the `and`). A real
// `name(...)` still wins by shift via func_call; this reduces only when no
// `(` follows.
operand(R) ::= TOK_NAME(N). {
	ael_errf(ctx, N.str.offset, N.str.sz,
			"unexpected identifier '%.*s' -- use $.%.*s for a bin",
			(int)N.str.sz, TOK_STR(ctx, N), (int)N.str.sz, TOK_STR(ctx, N));
	R = ast_new(ctx->pool, AST_UNKNOWN);
}

// Loop variables: @, @key, @index. Only legal inside a wildcard filter
// (`*[?(...)]`) or modify body — ael_new_loop_var enforces that via the
// filter_depth check. The loop_var non-terminal owns both bare
// construction and recursive `:T` postfix so chained pins
// (`@:INT:FLOAT`) reach the unified helper instead of the
// %syntax_error path.
%type loop_var { ast_ref }

loop_var(R) ::= TOK_AT(T).       { R = ael_new_loop_var(ctx, AS_EXP_BUILTIN_VALUE, T.str.offset, T.str.sz); }
loop_var(R) ::= TOK_AT_KEY(T).   { R = ael_new_loop_var(ctx, AS_EXP_BUILTIN_KEY,   T.str.offset, T.str.sz); }
loop_var(R) ::= TOK_AT_INDEX(T). { R = ael_new_loop_var(ctx, AS_EXP_BUILTIN_INDEX, T.str.offset, T.str.sz); }

// `:T` postfix on loop_var. ael_node_apply_postfix validates against
// AST_LOOP_VAR.valid_types (AUTO) and applies the @key→AUTO_KEY
// narrowing as an instance-level check. No @index:T form is allowed —
// @index is always INT, so any pin lands as a duplicate-type-pin
// against the constructor's pre-set etype.
loop_var(R) ::= loop_var(L) TOK_COLON type_name(T). {
	R = ael_node_apply_postfix(ctx, L, AEL_POSTFIX_TYPE, (uint32_t)T);
}

operand(R) ::= loop_var(L). { R = L; }

operand(R) ::= TOK_VARIABLE(V). { R = ast_new_var_ref(ctx, V.str.offset, V.str.sz); }

operand(R) ::= TOK_PLACEHOLDER(V). {
	ael_err(ctx, V.offset, V.sz,
			"placeholder expressions (?N) are not yet supported");
	// Stand-in node so the rest of the parse can proceed without
	// NULL-deref. The diag above prevents successful compile.
	R = ast_new(ctx->pool, AST_UNKNOWN);
}

// bin_base: $.binName, $.binName:T (strict — pins the canonical bin's
// etype; the narrowing propagates to other references), or
// $.binName:LOCAL:T (loose — creates a separate bin reference whose
// type is private to this call site, so $.bin:LOCAL:INT in one when
// branch can coexist with $.bin:LOCAL:STRING in another). Both suffix
// forms replace the old $.local(name, type: T) function-style grammar.
//
// Quoted forms ($."bin name" / $.'bin name') accept any byte except
// the delimiter and reach bin names that fall outside the unquoted
// [A-Za-z_][A-Za-z0-9_]* identifier rule (e.g. hyphens, spaces).
// ast_new_bin enforces the server's bin-name constraints (non-empty,
// ≤15 bytes, no NULs) for both forms.
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_NAME(V). [TOK_BIN_BARE_PREC] {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);
}
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_NAME(V) TOK_COLON type_name(T). {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);

	if (R != AST_REF_NULL) {
		ast_bin_set_implicit_type(ctx, R, (ast_etype)T);
	}
}
// Mirrors the rule above for a create-order suffix. Needed as its own 4-token
// form because lemon prefers shift on the first `:` after a bin, so the
// recursive `bin_base : prop_flag` rule is only reachable for a second suffix.
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_NAME(V) TOK_COLON prop_flag(P). {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);
	R = ael_node_apply_postfix(ctx, R, AEL_POSTFIX_PROP, P);
}
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_NAME(V) TOK_COLON TOK_LOCAL TOK_COLON type_name(T). {
	R = ast_new_local_bin(ctx, D.offset, V.offset + V.sz, V.str.offset,
			V.str.sz, (ast_etype)T);
}
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_STRING(V). [TOK_BIN_BARE_PREC] {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);
}
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_STRING(V) TOK_COLON type_name(T). {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);

	if (R != AST_REF_NULL) {
		ast_bin_set_implicit_type(ctx, R, (ast_etype)T);
	}
}
// Mirrors the rule above for a create-order suffix. Needed as its own 4-token
// form because lemon prefers shift on the first `:` after a bin, so the
// recursive `bin_base : prop_flag` rule is only reachable for a second suffix.
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_STRING(V) TOK_COLON prop_flag(P). {
	R = ast_new_bin(ctx, D.offset, V.offset + V.sz, V.str.offset, V.str.sz);
	R = ael_node_apply_postfix(ctx, R, AEL_POSTFIX_PROP, P);
}
bin_base(R) ::= TOK_DOLLAR_DOT(D) TOK_STRING(V) TOK_COLON TOK_LOCAL TOK_COLON type_name(T). {
	R = ast_new_local_bin(ctx, D.offset, V.offset + V.sz, V.str.offset,
			V.str.sz, (ast_etype)T);
}

// `:T` postfix on an already-reduced bin_base. Lemon prefers shift
// on the first :T (the 4-token TYPE rules above consume the initial
// pin via shift); this rule fires only for subsequent :T, so chained
// `$.bin:INT:FLOAT` reaches ast_bin_set_implicit_type and surfaces a
// clean "bin type conflict" diagnostic instead of "syntax error".
bin_base(R) ::= bin_base(B) TOK_COLON type_name(T). {
	if (B != AST_REF_NULL) {
		ast_bin_set_implicit_type(ctx, B, (ast_etype)T);
	}
	R = B;
}

// `:PROPERTY` postfix on a bin — a create-order names the container it creates,
// and the bin names the top-level one. The flag is parked in the parse context
// rather than stored on the bin node (which has no props slot) and drains onto
// the first path segment; see ael_drain_create_props.
bin_base(R) ::= bin_base(B) TOK_COLON prop_flag(P). {
	R = ael_node_apply_postfix(ctx, B, AEL_POSTFIX_PROP, P);
}

// Simple bin access (no CDT path).
operand(R) ::= bin_base(B). { R = B; }

// $.bin.type() and all other bare-bin path functions (getKeys / count / set /
// toInt / ...) are TOK_NAME and flow through `bin_base . method_fn` below.

// (expr) — parallel of bin_base. A parenthesized expression serves as
// a particle-producing receiver anywhere a bin would; the runtime
// CONTEXT_EVAL / CDT-op machinery is agnostic to the source of the
// particle. ael_build_value_func enforces value-recv restrictions
// (rejects .exists() / .type() etc).
operand(R) ::= exp_base(B). { R = B; }

// $.bin.seg.seg... — path with one or more CDT segments. ctx_list builds
// the AST_PATH_CTX from bin + segs as the parser walks left-to-right.
operand(R) ::= ctx_list(C). {
	// Implicit get on the last seg of ctx_list. The grammar guarantees at
	// least one path_seg here, so ael_ctx_list_pop always finds a leaf.
	ast_ref leaf = ael_ctx_list_pop(ctx, C);
	ast_ref impl_get = ast_new_cdt_op(ctx->pool, AST_PATH_FUNC_GET);

	if (leaf != AST_REF_NULL) {
		// Propagate `:T` postfix on the last seg (set by
		// ael_path_seg_apply_type) onto the implicit-GET so the wire
		// carries the leaf's resolved type.
		ast_etype leaf_etype = ast_pool_at(ctx->pool, leaf)->etype;

		if (ast_type_resolved(leaf_etype)) {
			ast_pool_at(ctx->pool, impl_get)->etype = leaf_etype;
		}
	}

	R = leaf == AST_REF_NULL ?
			AST_REF_NULL :
			ael_consume_leaf_then_finalize(ctx, C, impl_get, leaf);
}

// The pathed path functions (getKeys / count / set / toInt / append / ...) are
// TOK_NAME and flow through `ctx_list . method_fn` below;
// ael_finalize_method_call_path → ael_finalize_path_on_ctx reproduces the
// former per-kind leaf-consume logic.

// $.bin.path.*.path.modify(expr) is now a TOK_NAME method call (AEL_FAM_MODIFY)
// flowing through `ctx_list . method_fn` below; ael_finalize_method_call_path
// routes the modify node to ael_finalize_select_call. The filter scope is
// pushed by method_open and popped in ael_resolve_modify_fn.

// receiver.fn(...) — method-style bit / HLL calls. The function name is a
// TOK_NAME resolved via ael_func_table (ael_resolve_method_fn, in the
// "Function calls" section below) into a transient bit / HLL / CDT-path op
// node. Each receiver shape is enumerated so each is unambiguous in LALR(1);
// method_call left-recurses for chaining (`a.bitX().bitY()`).
// ael_finalize_method_call routes by node type — CDT path funcs to
// ael_build_value_func, else bit/HLL by receiver type. The bin_base receiver
// uses the _bin variant so bare-bin path funcs (.exists()/.type()/folds) go
// to ael_build_bin_func — parens are transparent, so the bin vs value split
// must be made at the grammar level, not by inspecting the receiver node.
method_call(R) ::= bin_base(B) TOK_DOT method_fn(F). {
	R = ael_finalize_method_call_bin(ctx, B, F);
}
// Nested-path receiver: bit-modify simulates on the path, hll-modify is
// rejected, reads fold the path into a value receiver — all inside
// ael_finalize_method_call_path.
method_call(R) ::= ctx_list(C) TOK_DOT method_fn(F). {
	R = ael_finalize_method_call_path(ctx, C, F);
}
method_call(R) ::= TOK_BLOB_LITERAL(V) TOK_DOT method_fn(F). {
	ast_ref recv = ast_new_blob_from_hex(ctx->pool, TOK_STR(ctx, V),
			V.str.sz);
	R = ael_finalize_method_call(ctx, recv, F);
}
method_call(R) ::= TOK_B64_BLOB_LITERAL(V) TOK_DOT method_fn(F). {
	ast_ref recv = ast_new_blob_from_b64(ctx->pool, TOK_STR(ctx, V),
			V.str.sz);
	R = ael_finalize_method_call(ctx, recv, F);
}
method_call(R) ::= func_call(C) TOK_DOT method_fn(F). {
	R = ael_finalize_method_call(ctx, C, F);
}
method_call(R) ::= method_call(C) TOK_DOT method_fn(F). {
	R = ael_finalize_method_call(ctx, C, F);
}
// A loop variable as receiver. `@` also roots a path (`ctx_list ::= TOK_AT
// TOK_DOT path_seg` below), so `@.name` is ambiguous until the method_fn's
// TOK_LPAREN is in view — the same one-token separation that lets a bin
// receiver carry both. Spelled with the bare token rather than loop_var so
// both rules keep the TOK_AT TOK_DOT prefix; reducing loop_var first would
// need the decision a token earlier than the lookahead allows, and costs a
// parser conflict.
//
// @key and @index get a method but no path rule of their own: AUTO_KEY is
// int / str / blob and @index is pinned INT, so neither is worth a path root.
// The grammar omits that spelling rather than forbidding it.
method_call(R) ::= TOK_AT(T) TOK_DOT method_fn(F). {
	ast_ref lv = ael_new_loop_var(ctx, AS_EXP_BUILTIN_VALUE, T.str.offset,
			T.str.sz);

	R = ael_finalize_method_call(ctx, lv, F);
}
method_call(R) ::= TOK_AT_KEY(T) TOK_DOT method_fn(F). {
	ast_ref lv = ael_new_loop_var(ctx, AS_EXP_BUILTIN_KEY, T.str.offset,
			T.str.sz);

	R = ael_finalize_method_call(ctx, lv, F);
}
method_call(R) ::= TOK_AT_INDEX(T) TOK_DOT method_fn(F). {
	ast_ref lv = ael_new_loop_var(ctx, AS_EXP_BUILTIN_INDEX, T.str.offset,
			T.str.sz);

	R = ael_finalize_method_call(ctx, lv, F);
}
// Parenthesized arbitrary expression as receiver — exp_base is the generic
// particle-producing receiver.
method_call(R) ::= exp_base(B) TOK_DOT method_fn(F). {
	R = ael_finalize_method_call(ctx, B, F);
}
// `:PROPERTY` postfix on the method op — pre-morph application so the
// validation row matches the actual op (bit reads reject all flags, bit
// modifies accept VALID_BIT_MODIFY; HLL_INIT/HLL_ADD accept their
// NO_FAIL/CREATE_ONLY/UPDATE_ONLY subsets — all via ast_node_table
// valid_props in ael_node_apply_postfix).
method_fn(R) ::= method_fn(F) TOK_COLON prop_flag(P). {
	R = ael_node_apply_postfix(ctx, F, AEL_POSTFIX_PROP, P);
}
operand(R) ::= method_call(B). { R = B; }

// ctx_list always starts with bin_base + at least one path_seg, then
// optionally extends with more path_segs. (Bare-bin without segs is the
// `operand ::= bin_base` rule above.)
ctx_list(R) ::= bin_base(B) TOK_DOT path_seg(S). {
	if (B == AST_REF_NULL) {
		R = AST_REF_NULL;
	}
	else {
		ast_ref c = ast_new_path_ctx_bin(ctx->pool, B);
		R = ael_ctx_list_append(ctx, c, S);
	}
}
// (expr).path_seg — start a path navigation rooted at a value-producing
// expression. The runtime CONTEXT_EVAL accepts any LIST/MAP particle as
// the navigation root, so the rest of the ctx_list / path-func machinery
// works unchanged. ast_new_path_ctx_bin is generic — it just wraps the
// head ast_ref in an AST_PATH_CTX regardless of node type.
ctx_list(R) ::= exp_base(E) TOK_DOT path_seg(S). {
	// Check before pinning: no-kind-of-container is refused here, wrong-kind by
	// the pin's own conflict. One diagnostic each.
	if (E == AST_REF_NULL || ! ael_check_path_root_is_container(ctx, E)) {
		R = AST_REF_NULL;
	}
	else {
		ael_pin_path_root_etype(ctx, E, S);

		ast_ref c = ast_new_path_ctx_bin(ctx->pool, E);
		R = ael_ctx_list_append(ctx, c, S);
	}
}
ctx_list(R) ::= ctx_list(L) TOK_DOT path_seg(S). {
	R = ael_ctx_list_append(ctx, L, S);
}

// AND_EXP post-filter: <ctx_list>&[?(filter)]. Extends an existing
// ctx_list with an AS_CDT_CTX_AND|AS_CDT_CTX_EXP seg that filters the
// elements selected by the preceding seg. No leading dot — the fused
// `&[?(` token disambiguates from bitwise-AND. The ctx_list LHS
// guarantees a preceding seg; pairing rules (no AND on `*[?(...)]`,
// no two AND in a row) are checked in ael_extend_with_and_seg.
ctx_list(R) ::= ctx_list(L) and_filter_open(BS) expr(F) TOK_RPAREN(P) TOK_RBRACKET. {
	ael_create_park_pop(ctx);
	ael_filter_scope_pop(ctx);
	R = ael_extend_with_and_seg(ctx, L, F, BS, P.str.offset - BS);
}

and_filter_open(BS) ::= TOK_AMP_LBRACK_QPAREN(K). {
	(void)ael_create_park_push(ctx);
	(void)ael_filter_scope_push(ctx);
	BS = K.str.offset + K.str.sz;
}

// `@.path` — loop-variable-rooted path navigation. The loop var's etype
// is pinned by the first seg's MAP / LIST nature so the implicit-get
// codegen picks the right CDT op family.
ctx_list(R) ::= TOK_AT(T) TOK_DOT path_seg(S). {
	ast_ref lv = ael_new_loop_var(ctx, AS_EXP_BUILTIN_VALUE, T.str.offset, T.str.sz);

	ael_pin_path_root_etype(ctx, lv, S);

	if (lv == AST_REF_NULL) {
		R = AST_REF_NULL;
	}
	else {
		ast_ref c = ast_new_path_ctx_bin(ctx->pool, lv);
		R = ael_ctx_list_append(ctx, c, S);
	}
}

// Geo builtins geoJson('...') and geoCompare(a, b) are ordinary TOK_NAME
// functions now (AEL_FAM_GEO) — they flow through func_call and resolve in
// ael_resolve_geo_fn (geoJson builds a GEOJSON literal from its string-literal
// arg; geoCompare a bidirectional spatial compare with both args pinned GEO).

// $.NAME(...) — record-level call. Metadata function names and `key` are
// ordinary identifiers (TOK_NAME), not keywords — ael_resolve_meta_call
// dispatches by name. The meta_key_call non-terminal owns the no-arg form,
// the `$.digestModulo(INT)` form, and the recursive `:T` type-pin chain. The
// pin chain is spec-canonical for `$.key():INT:FLOAT` (DECISIONS.md row 17);
// for other metadata nodes the pin is rejected via valid_types.
%type meta_key_call { ast_ref }

meta_key_call(R) ::= TOK_DOLLAR_DOT(D) TOK_NAME(V) TOK_LPAREN TOK_RPAREN(RP). {
	R = ael_resolve_meta_call(ctx, V.str.offset, V.str.sz, 0, false);
	// Span the whole '$.name()' so a runtime trace (e.g. an ABSENT $.key())
	// focuses the accessor, not ast_new's lookahead default.
	ast_set_span(ctx->pool, R, D.offset, RP.offset + RP.sz - D.offset);
}

meta_key_call(R) ::=
		TOK_DOLLAR_DOT(D) TOK_NAME(V) TOK_LPAREN TOK_INT(P) TOK_RPAREN(RP). {
	R = ael_resolve_meta_call(ctx, V.str.offset, V.str.sz, P.ival, true);
	ast_set_span(ctx->pool, R, D.offset, RP.offset + RP.sz - D.offset);
}

meta_key_call(R) ::= meta_key_call(K) TOK_COLON type_name(T). {
	R = ael_node_apply_postfix(ctx, K, AEL_POSTFIX_TYPE, (uint32_t)T);
}

operand(R) ::= meta_key_call(K). { R = K; }

//==========================================================
// Function calls.
//

// Function and parameter names are ordinary identifiers (TOK_NAME), not
// keywords — dispatch is table-driven (ael_func_table + ael_resolve_*). One
// generic call shape feeds every function; per-function arity, naming, and
// type rules live in the table + resolvers, not the grammar.

// An argument is positional (`expr`) or named (`name: expr`). A bare TOK_NAME
// is never a complete expr, so `TOK_NAME :` unambiguously begins a named arg
// while `TOK_NAME (` begins a positional func_call.
arg(R) ::= expr(E). { R = ael_new_positional_arg(ctx, E); }
arg(R) ::= TOK_NAME(N) TOK_COLON expr(E). {
	R = ael_new_named_arg(ctx, N.str.offset, N.str.sz, E);
}
// `pattern: /re/flags` — regexReplace's regex operand. The lexer arms the
// regex latch after a `pattern:` so `/` opens a literal here; the value is
// a TOK_REGEX, not an expr.
arg(R) ::= TOK_NAME(N) TOK_COLON TOK_REGEX(P). {
	R = ael_new_named_arg(ctx, N.str.offset, N.str.sz,
			ael_new_regex_operand(ctx, P.offset + 1, P.regex.pat_sz,
					P.offset + P.regex.pat_sz + 2, P.regex.flag_sz));
}
arg_list(R) ::= arg(A). { R = ael_new_arg_list(ctx, A); }
arg_list(R) ::= arg_list(L) TOK_COMMA arg(A). { R = ael_append_arg(ctx, L, A); }

// Top-level scalar call: name(args). Bit / HLL names error here (they need a
// receiver). The empty-paren form is split out to keep arg_list non-nullable.
func_call(R) ::= TOK_NAME(F) TOK_LPAREN arg_list(L) TOK_RPAREN. {
	R = ael_resolve_func_call(ctx, F.str.offset, F.str.sz, L);
}
func_call(R) ::= TOK_NAME(F) TOK_LPAREN TOK_RPAREN. {
	R = ael_resolve_func_call(ctx, F.str.offset, F.str.sz,
			ael_new_empty_arg_list(ctx));
}

// Method-style call body: name(args) after a receiver dot (see method_call in
// the "Expressions" section). Resolves bit / HLL / CDT-path functions to a
// transient op node; SCALAR names error here.
//
// method_open is a mid-rule reduction firing the moment `name (` is consumed —
// before the args. It pushes the filter scope for modify's body so its loop
// vars (@ / @key / @index) compile in isolation; for every other name it does
// nothing. This generalizes the former `modify_open` keyword rule: a bin named
// `modify` parses while `.modify(expr)` still scopes its body. method_open
// carries the name token forward for the resolver, which does the real
// func-table lookup.
//
// modify is the only scope-bodied call, so compare directly rather than scan
// the whole func table here (its sole purpose is the push decision). NOTE:
// ael_resolve_modify_fn pops on AEL_FAM_MODIFY — if another modify-family call
// is ever added, make this a family lookup again so push/pop stay balanced.
//
// The create-order park is pushed here and popped in the rules below. The pop
// has to stay in those rules: the enclosing `... TOK_DOT method_fn` reduction
// finalizes the call, and by then the flag must be back. Where in the action it
// sits does not matter -- resolving the name only builds the op node.
%type method_open { token_value }
method_open(O) ::= TOK_NAME(F) TOK_LPAREN. {
	O = F;
	(void)ael_create_park_push(ctx);

	if (F.str.sz == 6 && memcmp(ctx->input + F.str.offset, "modify", 6) == 0) {
		(void)ael_filter_scope_push(ctx);
	}
}
method_fn(R) ::= method_open(O) arg_list(L) TOK_RPAREN. {
	ael_create_park_pop(ctx);
	R = ael_resolve_method_fn(ctx, O.str.offset, O.str.sz, L);
}
method_fn(R) ::= method_open(O) TOK_RPAREN. {
	ael_create_park_pop(ctx);
	R = ael_resolve_method_fn(ctx, O.str.offset, O.str.sz,
			ael_new_empty_arg_list(ctx));
}

//==========================================================
// CDT path segments.
//

// Wildcard segment: iterate all children at this level.
path_seg(R) ::= TOK_STAR. {
	R = ast_new(ctx->pool, AST_WILDCARD_SEG);
	ast_pool_at(ctx->pool, R)->u.by_exp_seg.filter = AST_REF_NULL;
}

// Filter-attached wildcard: *[?(filter)]. The filter sub-expression
// compiles to a separate as_exp blob with no access to outer scope.
// The trailing `)]` is two tokens (TOK_RPAREN TOK_RBRACKET) — fusing
// them into one would collide with `[(expr)]` dyn segments.
//
// filter_open returns the body start offset (just past `[?(`); combined
// with the `)`'s offset we capture the body's source byte range on the
// wildcard seg for use by codegen's slow-path diagnostic.
path_seg(R) ::= filter_open(BS) expr(F) TOK_RPAREN(P) TOK_RBRACKET. {
	ael_create_park_pop(ctx);
	ael_filter_scope_pop(ctx);
	R = ael_new_wild_filter_seg(ctx, F, BS, P.str.offset - BS);
}

// filter_open is a single-token reduction; its action runs the moment
// `*[?(` has been consumed, before the filter expression is parsed.
filter_open(BS) ::= TOK_STAR TOK_LBRACK_QPAREN(K). {
	(void)ael_create_park_push(ctx);
	(void)ael_filter_scope_push(ctx);
	BS = K.str.offset + K.str.sz;
}

// Bare key access (no brackets).
path_seg(R) ::= TOK_NAME(V). {
	ast_ref key = ast_new_string(ctx->pool, TOK_STR(ctx, V), V.str.sz, false);
	STAMP(key, V);
	R = ast_new(ctx->pool, AST_MAP_KEY);
	STAMP(R, V);
	ast_pool_at(ctx->pool, R)->u.seg.operand = key;
}
path_seg(R) ::= TOK_STRING(V). {
	ast_ref key = ast_new_string_token(ctx, V.str.offset, V.str.sz);
	STAMP(key, V);
	R = ast_new(ctx->pool, AST_MAP_KEY);
	STAMP(R, V);
	ast_pool_at(ctx->pool, R)->u.seg.operand = key;
}
path_seg(R) ::= TOK_INT(V). {
	ast_ref key = ast_new_int(ctx->pool, V.ival);
	STAMP(key, V);
	R = ast_new(ctx->pool, AST_MAP_KEY);
	STAMP(R, V);
	ast_pool_at(ctx->pool, R)->u.seg.operand = key;
}

// Map segments via { } or {! }: singular `{x}` is INDEX (must be
// int); for a key use `{@x}` or just `x` (bare). Range forms
// `{a:b}` / `{a:}` / `{:b}` are INDEX_RANGE only -- non-int
// operands are an error (use `{@a:b}` for KEY_RANGE).
path_seg(R) ::= brace_open(INV) key_val(V) TOK_RBRACE. {
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, V, AST_REF_NULL, &seg_off, &seg_sz);

	if (INV) {
		ael_err(ctx, seg_off, seg_sz,
				"singular map segment cannot be inverted");
	}
	if (ast_pool_at(ctx->pool, V)->type != AST_INT) {
		ael_err(ctx, seg_off, seg_sz,
				"bare {x} is index — for a key use {@x} or just x");
		R = AST_REF_NULL;
	}
	else {
		R = ast_new(ctx->pool, AST_MAP_INDEX);
		ast_pool_at(ctx->pool, R)->u.seg.operand = V;
	}
}
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COLON key_val(B) TOK_RBRACE. {
	R = seg_int_range(ctx, AST_MAP_INDEX_RANGE, A, B, INV,
			"bare {a:b} is index range — for a key range, use {@a:b}",
			"index range endpoints must have the same sign — count = end - start can't be computed at parse time when signs differ");
}
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COLON TOK_RBRACE. {
	R = seg_int_range(ctx, AST_MAP_INDEX_RANGE, A, AST_REF_NULL, INV,
			"bare {a:} is index range — for a key range, use {@a:}", NULL);
}
// Open-start `{:B}`: B must be a non-negative int -- INDEX_RANGE
// with synthesized start=0 (wire-identical to `{0:B}`). For a key
// open-start, use `{@:B}`. Negative B would be mixed-sign (start=0
// is non-negative) -- count = end - start can't be computed at parse
// time when signs differ.
path_seg(R) ::= brace_open(INV) TOK_COLON key_val(B) TOK_RBRACE. {
	R = seg_open_start_int(ctx, AST_MAP_INDEX_RANGE, B, INV,
			"bare {:b} is index range — for a key range, use {@:b}",
			"open-start index range needs a non-negative end (synthesized start=0; mixed-sign with end<0)");
}
// Key-relative index range `{S:E~K}` and open-end `{S:~K}`. S/E are
// rank offsets relative to the position of key K in the map; emits
// MAP_*_BY_KEY_REL_INDEX_RANGE. S/E must be int literals.
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COLON key_val(B) TOK_TILDE key_val(V) TOK_RBRACE. {
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, A, B, &seg_off, &seg_sz);

	if (ast_pool_at(ctx->pool, A)->type != AST_INT ||
			ast_pool_at(ctx->pool, B)->type != AST_INT) {
		ael_err(ctx, seg_off, seg_sz,
				"relative range endpoints must be int literals");
		R = AST_REF_NULL;
	}
	else if (! ael_rel_range_count_ok(ctx, A, B)) {
		R = AST_REF_NULL;
	}
	else {
		R = ast_new_rel_range_seg(ctx->pool, AST_MAP_INDEX_REL_RANGE, A, B,
				V, INV != 0);
	}
}
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COLON TOK_TILDE key_val(V) TOK_RBRACE. {
	uint32_t seg_off;
	uint32_t seg_sz;

	seg_operand_span(ctx, A, AST_REF_NULL, &seg_off, &seg_sz);

	if (ast_pool_at(ctx->pool, A)->type != AST_INT) {
		ael_err(ctx, seg_off, seg_sz,
				"relative range start must be an int literal");
		R = AST_REF_NULL;
	}
	else {
		R = ast_new_rel_range_seg(ctx->pool, AST_MAP_INDEX_REL_RANGE, A,
				AST_REF_NULL, V, INV != 0);
	}
}
// Bare-brace comma lists are a ratified spec rejection (the key dimension
// inside {...} is explicit: {@a,b,c}), and there is no index-list wire op.
// Consume the full shape so the user gets guidance instead of a bare
// syntax error.
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COMMA key_list(L) TOK_RBRACE. {
	(void)INV;
	(void)L;
	R = seg_list_unsupported(ctx, A,
			"bare {a,b} is not a selector — for a key list use {@a,b}");
}
path_seg(R) ::= brace_open(INV) key_val(A) TOK_COMMA TOK_RBRACE. {
	(void)INV;
	R = seg_list_unsupported(ctx, A,
			"bare {a,b} is not a selector — for a key list use {@a,b}");
}

// Map value segments via {= } or {!= }: singular, value range, value list.
path_seg(R) ::= brace_eq_open(INV) key_val(V) TOK_RBRACE. {
	seg_inv_err(ctx, V, INV, "singular map segment cannot be inverted");
	R = ast_new(ctx->pool, AST_MAP_VALUE);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= brace_eq_open(INV) key_val(A) TOK_COLON key_val(B) TOK_RBRACE. {
	R = ast_new_range_seg(ctx->pool, AST_MAP_VALUE_RANGE, A, B, INV != 0);
}
path_seg(R) ::= brace_eq_open(INV) key_val(A) TOK_COLON TOK_RBRACE. {
	R = ast_new_range_seg(ctx->pool, AST_MAP_VALUE_RANGE, A, AST_REF_NULL, INV != 0);
}
// Open-start `{=:V}`: NIL value_start — runtime treats as "from
// smallest possible value".
path_seg(R) ::= brace_eq_open(INV) TOK_COLON key_val(B) TOK_RBRACE. {
	ast_ref nil_start = ast_new_nil(ctx->pool);
	R = ast_new_range_seg(ctx->pool, AST_MAP_VALUE_RANGE, nil_start, B, INV != 0);
}
path_seg(R) ::= brace_eq_open(INV) key_val(A) TOK_COMMA key_list(L) TOK_RBRACE. {
	ast_node* n = ast_pool_at(ctx->pool, L);
	// The two union variants alias -- snapshot before re-laying.
	ast_ref tail = n->u.list.tail;
	ast_pool_at(ctx->pool, A)->next = n->u.list.head;
	uint32_t count = n->u.list.count + 1;
	n->type = AST_MAP_VALUE_LIST;
	n->u.list_seg.head = A;
	n->u.list_seg.tail = tail;
	n->u.list_seg.count = count;
	n->u.list_seg.inverted = INV;
	R = L;
}
// 1-element value list: {=V,}
path_seg(R) ::= brace_eq_open(INV) key_val(V) TOK_COMMA TOK_RBRACE. {
	R = ast_new(ctx->pool, AST_MAP_VALUE_LIST);
	ast_node* n = ast_pool_at(ctx->pool, R);
	ast_pool_at(ctx->pool, V)->next = AST_REF_NULL;
	n->u.list_seg.head = V;
	n->u.list_seg.tail = V;
	n->u.list_seg.count = 1;
	n->u.list_seg.inverted = INV;
}

// Map rank segments via {# } or {!# }: singular, rank range.
path_seg(R) ::= brace_hash_open(INV) int_val(V) TOK_RBRACE. {
	seg_inv_err(ctx, V, INV, "singular map segment cannot be inverted");
	R = ast_new(ctx->pool, AST_MAP_RANK);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COLON int_val(B) TOK_RBRACE. {
	R = seg_int_range(ctx, AST_MAP_RANK_RANGE, A, B, INV, NULL,
			"rank range endpoints must have the same sign — count = end - start can't be computed at parse time when signs differ");
}
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COLON TOK_RBRACE. {
	R = ast_new_range_seg(ctx->pool, AST_MAP_RANK_RANGE, A, AST_REF_NULL, INV != 0);
}
path_seg(R) ::= brace_hash_open(INV) TOK_COLON int_val(B) TOK_RBRACE. {
	R = seg_open_start_int(ctx, AST_MAP_RANK_RANGE, B, INV, NULL,
			"open-start rank range needs a non-negative end (synthesized start=0; mixed-sign with end<0)");
}
// Value-relative rank range `{#S:E~V}` and open-end `{#S:~V}`. S/E
// are rank offsets relative to the rank of value V; emits
// MAP_*_BY_VALUE_REL_RANK_RANGE.
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COLON int_val(B) TOK_TILDE key_val(V) TOK_RBRACE. {
	if (! ael_rel_range_count_ok(ctx, A, B)) {
		R = AST_REF_NULL;
	}
	else {
		R = ast_new_rel_range_seg(ctx->pool, AST_MAP_RANK_REL_RANGE, A, B, V, INV != 0);
	}
}
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COLON TOK_TILDE key_val(V) TOK_RBRACE. {
	R = ast_new_rel_range_seg(ctx->pool, AST_MAP_RANK_REL_RANGE, A,
			AST_REF_NULL, V, INV != 0);
}

// No rank-list wire op exists — reject the comma shapes with guidance.
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COMMA key_list(L) TOK_RBRACE. {
	(void)INV;
	(void)L;
	R = seg_list_unsupported(ctx, A,
			"rank lists are not supported — use {#a:b} or a value list {=a,b}");
}
path_seg(R) ::= brace_hash_open(INV) int_val(A) TOK_COMMA TOK_RBRACE. {
	(void)INV;
	R = seg_list_unsupported(ctx, A,
			"rank lists are not supported — use {#a:b} or a value list {=a,b}");
}

// Map explicit-key segments via {@ } or {!@ }: singular, range, key list.
// Same AST as the bare {} forms but forces KEY semantics regardless of
// operand type — handy for disambiguating int-keyed maps from index ops.
path_seg(R) ::= brace_at_open(INV) key_val(V) TOK_RBRACE. {
	seg_inv_err(ctx, V, INV, "singular map segment cannot be inverted");
	R = ast_new(ctx->pool, AST_MAP_KEY);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= brace_at_open(INV) key_val(A) TOK_COLON key_val(B) TOK_RBRACE. {
	R = ast_new_range_seg(ctx->pool, AST_MAP_KEY_RANGE, A, B, INV != 0);
}
path_seg(R) ::= brace_at_open(INV) key_val(A) TOK_COLON TOK_RBRACE. {
	R = ast_new_range_seg(ctx->pool, AST_MAP_KEY_RANGE, A, AST_REF_NULL, INV != 0);
}
// Open-start `{@:K}`: NIL key_start — runtime treats as "from
// smallest possible key".
path_seg(R) ::= brace_at_open(INV) TOK_COLON key_val(B) TOK_RBRACE. {
	ast_ref nil_start = ast_new_nil(ctx->pool);
	R = ast_new_range_seg(ctx->pool, AST_MAP_KEY_RANGE, nil_start, B, INV != 0);
}
path_seg(R) ::= brace_at_open(INV) key_val(A) TOK_COMMA key_list(L) TOK_RBRACE. {
	ast_node* n = ast_pool_at(ctx->pool, L);
	// The two union variants alias -- snapshot before re-laying.
	ast_ref tail = n->u.list.tail;
	ast_pool_at(ctx->pool, A)->next = n->u.list.head;
	uint32_t count = n->u.list.count + 1;
	n->type = AST_MAP_KEY_LIST;
	n->u.list_seg.head = A;
	n->u.list_seg.tail = tail;
	n->u.list_seg.count = count;
	n->u.list_seg.inverted = INV;
	R = L;
}
// 1-element key list: {@V,}
path_seg(R) ::= brace_at_open(INV) key_val(V) TOK_COMMA TOK_RBRACE. {
	R = ast_new(ctx->pool, AST_MAP_KEY_LIST);
	ast_node* n = ast_pool_at(ctx->pool, R);
	ast_pool_at(ctx->pool, V)->next = AST_REF_NULL;
	n->u.list_seg.head = V;
	n->u.list_seg.tail = V;
	n->u.list_seg.count = 1;
	n->u.list_seg.inverted = INV;
}

// List index segments via [ ] or [! ]: singular, index range.
path_seg(R) ::= bracket_open(INV) int_val(V) TOK_RBRACKET. {
	seg_inv_err(ctx, V, INV, "singular list segment cannot be inverted");
	R = ast_new(ctx->pool, AST_LIST_INDEX);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= bracket_open(INV) int_val(A) TOK_COLON int_val(B) TOK_RBRACKET. {
	R = seg_int_range(ctx, AST_LIST_INDEX_RANGE, A, B, INV, NULL,
			"index range endpoints must have the same sign — count = end - start can't be computed at parse time when signs differ");
}
path_seg(R) ::= bracket_open(INV) int_val(A) TOK_COLON TOK_RBRACKET. {
	R = ast_new_range_seg(ctx->pool, AST_LIST_INDEX_RANGE, A, AST_REF_NULL, INV != 0);
}
path_seg(R) ::= bracket_open(INV) TOK_COLON int_val(B) TOK_RBRACKET. {
	R = seg_open_start_int(ctx, AST_LIST_INDEX_RANGE, B, INV, NULL,
			"open-start index range needs a non-negative end (synthesized start=0; mixed-sign with end<0)");
}

// No index-list wire op exists — reject the comma shapes with guidance.
path_seg(R) ::= bracket_open(INV) int_val(A) TOK_COMMA key_list(L) TOK_RBRACKET. {
	(void)INV;
	(void)L;
	R = seg_list_unsupported(ctx, A,
			"index lists are not supported — use [a:b] or a value list [=a,b]");
}
path_seg(R) ::= bracket_open(INV) int_val(A) TOK_COMMA TOK_RBRACKET. {
	(void)INV;
	R = seg_list_unsupported(ctx, A,
			"index lists are not supported — use [a:b] or a value list [=a,b]");
}

// List value segments via [= ] or [!= ]: singular, value range, value list.
path_seg(R) ::= bracket_eq_open(INV) key_val(V) TOK_RBRACKET. {
	seg_inv_err(ctx, V, INV, "singular list segment cannot be inverted");
	R = ast_new(ctx->pool, AST_LIST_VALUE);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= bracket_eq_open(INV) key_val(A) TOK_COLON key_val(B) TOK_RBRACKET. {
	R = ast_new_range_seg(ctx->pool, AST_LIST_VALUE_RANGE, A, B, INV != 0);
}
path_seg(R) ::= bracket_eq_open(INV) key_val(A) TOK_COLON TOK_RBRACKET. {
	R = ast_new_range_seg(ctx->pool, AST_LIST_VALUE_RANGE, A, AST_REF_NULL, INV != 0);
}
// Open-start `[=:V]`: NIL value_start — runtime treats as "from
// smallest possible value".
path_seg(R) ::= bracket_eq_open(INV) TOK_COLON key_val(B) TOK_RBRACKET. {
	ast_ref nil_start = ast_new_nil(ctx->pool);
	R = ast_new_range_seg(ctx->pool, AST_LIST_VALUE_RANGE, nil_start, B, INV != 0);
}
path_seg(R) ::= bracket_eq_open(INV) key_val(A) TOK_COMMA key_list(L) TOK_RBRACKET. {
	ast_node* n = ast_pool_at(ctx->pool, L);
	// The two union variants alias -- snapshot before re-laying.
	ast_ref tail = n->u.list.tail;
	ast_pool_at(ctx->pool, A)->next = n->u.list.head;
	uint32_t count = n->u.list.count + 1;
	n->type = AST_LIST_VALUE_LIST;
	n->u.list_seg.head = A;
	n->u.list_seg.tail = tail;
	n->u.list_seg.count = count;
	n->u.list_seg.inverted = INV;
	R = L;
}
// 1-element list-value list: [=V,]
path_seg(R) ::= bracket_eq_open(INV) key_val(V) TOK_COMMA TOK_RBRACKET. {
	R = ast_new(ctx->pool, AST_LIST_VALUE_LIST);
	ast_node* n = ast_pool_at(ctx->pool, R);
	ast_pool_at(ctx->pool, V)->next = AST_REF_NULL;
	n->u.list_seg.head = V;
	n->u.list_seg.tail = V;
	n->u.list_seg.count = 1;
	n->u.list_seg.inverted = INV;
}

// List rank segments via [# ] or [!# ]: singular, rank range.
path_seg(R) ::= bracket_hash_open(INV) int_val(V) TOK_RBRACKET. {
	seg_inv_err(ctx, V, INV, "singular list segment cannot be inverted");
	R = ast_new(ctx->pool, AST_LIST_RANK);
	ast_pool_at(ctx->pool, R)->u.seg.operand = V;
}
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COLON int_val(B) TOK_RBRACKET. {
	R = seg_int_range(ctx, AST_LIST_RANK_RANGE, A, B, INV, NULL,
			"rank range endpoints must have the same sign — count = end - start can't be computed at parse time when signs differ");
}
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COLON TOK_RBRACKET. {
	R = ast_new_range_seg(ctx->pool, AST_LIST_RANK_RANGE, A, AST_REF_NULL, INV != 0);
}
path_seg(R) ::= bracket_hash_open(INV) TOK_COLON int_val(B) TOK_RBRACKET. {
	R = seg_open_start_int(ctx, AST_LIST_RANK_RANGE, B, INV, NULL,
			"open-start rank range needs a non-negative end (synthesized start=0; mixed-sign with end<0)");
}
// Value-relative list rank range `[#S:E~V]` and open-end `[#S:~V]`.
// S/E are rank offsets relative to the rank of value V; emits
// LIST_*_BY_VALUE_REL_RANK_RANGE.
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COLON int_val(B) TOK_TILDE key_val(V) TOK_RBRACKET. {
	if (! ael_rel_range_count_ok(ctx, A, B)) {
		R = AST_REF_NULL;
	}
	else {
		R = ast_new_rel_range_seg(ctx->pool, AST_LIST_RANK_REL_RANGE, A, B, V, INV != 0);
	}
}
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COLON TOK_TILDE key_val(V) TOK_RBRACKET. {
	R = ast_new_rel_range_seg(ctx->pool, AST_LIST_RANK_REL_RANGE, A,
			AST_REF_NULL, V, INV != 0);
}

// `:PROPERTY` postfix on path-seg — attaches a ctx-create flag bit
// (CR_LIST_* / CR_MAP_* / PERSIST_INDEX). The table's per-seg-row
// valid_props mask makes this an error on multi-select segs (their
// valid_props is 0).
path_seg(R) ::= path_seg(S) TOK_COLON prop_flag(P). {
	R = ael_node_apply_postfix(ctx, S, AEL_POSTFIX_PROP, P);
}

// `:TYPE` postfix on path-seg — pins the result type of the implicit
// GET (or method-call) that consumes this seg. The type lands on the
// seg's etype; the implicit-get handler at the operand reduction copies
// it onto the synthesized cdt_op. Lookahead distinguishes from the
// prop_flag rule above (disjoint TOK_TNAME_* vs TOK_NAME classes).
path_seg(R) ::= path_seg(S) TOK_COLON type_name(T). {
	R = ael_node_apply_postfix(ctx, S, AEL_POSTFIX_TYPE, (uint32_t)T);
}

//==========================================================
// CDT path segment helpers.
//

// Opening bracket non-terminals (capture inverted flag).
// No rank-list wire op exists — reject the comma shapes with guidance.
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COMMA key_list(L) TOK_RBRACKET. {
	(void)INV;
	(void)L;
	R = seg_list_unsupported(ctx, A,
			"rank lists are not supported — use [#a:b] or a value list [=a,b]");
}
path_seg(R) ::= bracket_hash_open(INV) int_val(A) TOK_COMMA TOK_RBRACKET. {
	(void)INV;
	R = seg_list_unsupported(ctx, A,
			"rank lists are not supported — use [#a:b] or a value list [=a,b]");
}

brace_open(R) ::= TOK_LBRACE.      { R = 0; }
brace_open(R) ::= TOK_LBRACE_BANG. { R = 1; }

brace_eq_open(R) ::= TOK_LBRACE_EQ.      { R = 0; }
brace_eq_open(R) ::= TOK_LBRACE_BANG_EQ. { R = 1; }

brace_hash_open(R) ::= TOK_LBRACE_HASH.      { R = 0; }
brace_hash_open(R) ::= TOK_LBRACE_BANG_HASH. { R = 1; }

brace_at_open(R) ::= TOK_LBRACE_AT.      { R = 0; }
brace_at_open(R) ::= TOK_LBRACE_BANG_AT. { R = 1; }

bracket_open(R) ::= TOK_LBRACKET.      { R = 0; }
bracket_open(R) ::= TOK_LBRACKET_BANG. { R = 1; }

bracket_eq_open(R) ::= TOK_LBRACKET_EQ.      { R = 0; }
bracket_eq_open(R) ::= TOK_LBRACKET_BANG_EQ. { R = 1; }

bracket_hash_open(R) ::= TOK_LBRACKET_HASH.      { R = 0; }
bracket_hash_open(R) ::= TOK_LBRACKET_BANG_HASH. { R = 1; }

// Key/value literal for use inside path segment brackets. Path segs are
// static literals; dynamic navigation goes through future function-
// parameter syntax (`.get(by_key: $.k)` etc.), not via path segs.
key_val(R) ::= TOK_NAME(V).   { R = ast_new_string(ctx->pool, TOK_STR(ctx, V), V.str.sz, false); STAMP(R, V); }
key_val(R) ::= TOK_STRING(V). { R = ast_new_string_token(ctx, V.str.offset, V.str.sz); STAMP(R, V); }
key_val(R) ::= TOK_INT(V).    { R = ast_new_int(ctx->pool, V.ival); STAMP(R, V); }
key_val(R) ::= TOK_FLOAT(V).  { R = ast_new_float(ctx->pool, V.fval); STAMP(R, V); }
key_val(R) ::= TOK_TRUE(V).   { R = ast_new_bool(ctx->pool, true); STAMP(R, V); }
key_val(R) ::= TOK_FALSE(V).  { R = ast_new_bool(ctx->pool, false); STAMP(R, V); }
key_val(R) ::= TOK_NIL(V).    { R = ast_new_nil(ctx->pool); STAMP(R, V); }
key_val(R) ::= TOK_INF(V).    { R = ast_new_inf(ctx->pool); STAMP(R, V); }
// Blob keys/values are first-class (AUTO_KEY = INT|STR|BLOB) — accept the
// same literal forms as map_key.
key_val(R) ::= TOK_BLOB_LITERAL(V). { R = ast_new_blob_from_hex(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }
key_val(R) ::= TOK_B64_BLOB_LITERAL(V). { R = ast_new_blob_from_b64(ctx->pool, TOK_STR(ctx, V), V.str.sz); STAMP(R, V); }

// Comma-separated list of key values (at least 1 item). Trailing
// comma is absorbed via the third rule so callers like `{@a, b,}`
// parse the same as `{@a, b}`.
key_list(R) ::= key_val(V). {
	R = ast_new(ctx->pool, AST_LIST);

	AST_LIST_PUSH_TAIL(ctx->pool, R, V);
}
key_list(R) ::= key_list(L) TOK_COMMA key_val(V). {
	AST_LIST_PUSH_TAIL(ctx->pool, L, V);
	R = L;
}
key_list(R) ::= key_list(L) TOK_COMMA. {
	R = L;
}

// Integer literal for index / rank operands.

int_val(R) ::= TOK_INT(V). { R = ast_new_int(ctx->pool, V.ival); STAMP(R, V); }

//==========================================================
// Path functions.
//
// Most path functions (get/exists/count/casts/getKeys/.../mutations) are
// ordinary identifiers (TOK_NAME) dispatched via ael_func_table (AEL_FAM_PATH);
// the resolver builds the NK_CDT_OP / cast node and the path finalizers resolve
// bin/leaf context (ael_resolve_path_fn / ael_finalize_path_on_ctx /
// ael_build_bin_func / ael_build_value_func). `modify` is also a TOK_NAME now
// (AEL_FAM_MODIFY) — its filter-scope push happens in the generic `method_open`
// rule and ael_resolve_modify_fn pops it; .modify(expr) routes through
// `ctx_list . method_fn` → ael_finalize_method_call_path → ael_finalize_select_call.
// `type` is a TOK_NAME too (AEL_FAM_PATH): $.bin.type() flows through method_fn
// like the rest; bare type names are particle-type constants (below). No keyword
// path functions remain.
//

// exists / count / getKeys / getKeyValues / getTree / getIndexes / getRanks /
// remove / set / insert / increment / modify / type are no longer keywords —
// they resolve via ael_func_table (AEL_FAM_PATH / AEL_FAM_MODIFY) through
// `method_fn` and the path finalizers. The `:PROPERTY` postfix (including
// modify's :noFail) is carried by the `method_fn : prop_flag` rule.

// A bare type name is a particle-type constant (INT == 1, FLOAT == 2, ...), for
// `$.bin.type() == INT` comparisons. Replaces the former type(T) wrapper. Type
// names only ever appear after `:` (postfix pins) or — formerly — inside
// type(...), never at operand position, so this is purely additive. type() the
// path function is now an ordinary TOK_NAME (AEL_FAM_PATH) like the rest.
//
// EXTENSION beyond the canonical spec: Condition.g4 / the product .md specs
// define type() as returning a raw INTEGER with no named-type constant (the
// type names are otherwise only `:T` suffixes). Bare type-name constants are a
// readability extension (vs undocumented magic ints), pending product
// ratification — like getIndexes().
operand(R) ::= type_name(T). {
	R = ast_new_int(ctx->pool, ael_etype_to_particle_type(T));
	// type_name reductions snapshot the TOK_TNAME_* span into postfix_token_*.
	ast_set_span(ctx->pool, R, ctx->postfix_token_offset, ctx->postfix_token_sz);
}

// Type name values (Exp::Type codes).
// Each type_name reduction snapshots the underlying TOK_TNAME_*
// token's source location so an outer postfix-apply rule can point
// its diagnostic at the type name itself (e.g. FLOAT in `:INT:FLOAT`)
// — by the time the outer rule reduces, ctx->last_token_offset has
// already advanced to the parser-driver lookahead.
type_name(R) ::= TOK_TNAME_INT(T).     { R = AST_ETYPE_INT;     ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_STRING(T).  { R = AST_ETYPE_STR;     ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_LIST(T).    { R = AST_ETYPE_LIST;    ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_MAP(T).     { R = AST_ETYPE_MAP;     ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_BLOB(T).    { R = AST_ETYPE_BLOB;    ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_FLOAT(T).   { R = AST_ETYPE_FLOAT;   ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_GEO(T).     { R = AST_ETYPE_GEOJSON; ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_HLL(T).     { R = AST_ETYPE_HLL;     ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }
type_name(R) ::= TOK_TNAME_BOOL(T).    { R = AST_ETYPE_TRILEAN; ctx->postfix_token_offset = T.str.offset; ctx->postfix_token_sz = T.str.sz; }

// VECTOR is reserved for a planned data type. Lexing it as a type-name keyword
// (not a bare TOK_NAME) keeps it out of the identifier space so the real type
// can be added later without breaking compatibility. Until it ships, any use
// is rejected here; AST_ETYPE_ERROR is the inert sentinel that downstream
// consumers (ael_etype_to_particle_type, ast_bin_set_implicit_type) already
// absorb without a second diagnostic.
type_name(R) ::= TOK_TNAME_VECTOR(T). {
	ael_err(ctx, T.str.offset, T.str.sz,
			"VECTOR is a reserved type name; not yet supported");
	R = AST_ETYPE_ERROR;
	ctx->postfix_token_offset = T.str.offset;
	ctx->postfix_token_sz = T.str.sz;
}

// Generic flag-property non-terminal for the `:PROPERTY` postfix syntax.
// Returns a single AST_PROP_* bit; ael_apply_prop (called from per-node
// wrappers) validates the bit against the attachment's valid mask,
// detects duplicates, and enforces group-mutual-exclusion.
// Flag names are ordinary identifiers (TOK_NAME), not keywords —
// ael_resolve_prop_flag maps the name to its AST_PROP_* bit. The
// postfix_token_* snapshot feeds diagnostics in ael_apply_prop (same
// rationale as type_name above).
%type prop_flag { ast_prop_bits }
prop_flag(R) ::= TOK_NAME(N). {
	R = ael_resolve_prop_flag(ctx, N.str.offset, N.str.sz);
	ctx->postfix_token_offset = N.str.offset;
	ctx->postfix_token_sz = N.str.sz;
}

//==========================================================
// Helper lists.
//

// List body: comma-separated list-elements. A list_elem is a literal or
// one of the non-storage CDT-compare specials (INF, WILDCARD via `*`).
list_elem(R) ::= literal(A). { R = A; }
list_elem(R) ::= TOK_INF.    { R = ast_new_inf(ctx->pool); }
list_elem(R) ::= TOK_STAR.   { R = ast_new_wildcard(ctx->pool); }

list_body(R) ::= list_elem(A). {
	R = ast_new(ctx->pool, AST_LIST);
	ast_pool_at(ctx->pool, R)->etype = AST_ETYPE_LIST;

	AST_LIST_PUSH_TAIL(ctx->pool, R, A);
}
list_body(R) ::= list_body(L) TOK_COMMA list_elem(A). {
	AST_LIST_PUSH_TAIL(ctx->pool, L, A);
	R = L;
}

// Map body: interleaved key->val->key->val chain via next.
// Keys restricted to int/string/blob; values can be any literal.
map_body(R) ::= map_key(K) TOK_COLON literal(V). {
	ast_pool_at(ctx->pool, K)->next = V;
	ast_pool_at(ctx->pool, V)->next = AST_REF_NULL;
	R = ast_new(ctx->pool, AST_MAP);
	ast_node* n = ast_pool_at(ctx->pool, R);
	n->etype = AST_ETYPE_MAP;
	n->u.list.head = K;
	n->u.list.tail = V;
	n->u.list.count = 1;
}
map_body(R) ::= map_body(M) TOK_COMMA map_key(K) TOK_COLON literal(V). {
	ast_pool_at(ctx->pool, K)->next = V;
	ast_pool_at(ctx->pool, V)->next = AST_REF_NULL;
	ast_node* n = ast_pool_at(ctx->pool, M);
	ast_pool_at(ctx->pool, n->u.list.tail)->next = K;
	n->u.list.tail = V;
	n->u.list.count++;
	R = M;
}

// Variable definition list for 'with'.
// The let_scope is at the head of the list so that var types are searchable
// during parsing of subsequent var definitions (e.g., y = ${x} + 1).
var_def_list(R) ::= TOK_NAME(N) TOK_ASSIGN expr(V). {
	R = ast_new_let_scope(ctx, N.str.offset, N.str.sz, V);
}
var_def_list(R) ::= var_def_list(L) TOK_COMMA TOK_NAME(N) TOK_ASSIGN expr(V). {
	ast_ref def = ast_new_var_def(ctx, N.str.offset, N.str.sz, V);

	if (def != AST_REF_NULL) {
		AST_LIST_PUSH_TAIL(ctx->pool, L, def);
	}

	R = L;
}

// Expression mapping list for 'when'.
case_list(R) ::= expr(C) TOK_ARROW expr(V). {
	ael_check_when_cond(ctx, C);
	ast_ref m = ast_new(ctx->pool, AST_CASE);
	ast_node* mp = ast_pool_at(ctx->pool, m);
	mp->u.when_case.cond = C;
	mp->u.when_case.result = V;
	mp->next = AST_REF_NULL;
	R = ast_new(ctx->pool, AST_LIST);

	AST_LIST_PUSH_TAIL(ctx->pool, R, m);
}
case_list(R) ::= case_list(L) TOK_COMMA expr(C) TOK_ARROW expr(V). {
	ael_check_when_cond(ctx, C);
	ast_ref m = ast_new(ctx->pool, AST_CASE);
	ast_node* mp = ast_pool_at(ctx->pool, m);
	mp->u.when_case.cond = C;
	mp->u.when_case.result = V;
	mp->next = AST_REF_NULL;

	AST_LIST_PUSH_TAIL(ctx->pool, L, m);
	R = L;
}

// No per-node destructors needed: on error the entire ast_pool is destroyed.
// Suppress unused-variable warning for 'ctx' in Lemon's yy_destructor.
%default_destructor { (void)ctx; (void)yypminor; }

%code {
void
ael_run_parser(ael_context* ctx, uint32_t input_sz)
{
	lexer_t lex;
	lexer_init(&lex, ctx->input, input_sz);

	yyParser parser;
	ParseInit(&parser);

	int prev_tok = 0;

	for (;;) {
		token_value val;
		int tok = lexer_next(&lex, &val);

		ctx->last_token_offset = lex.token_offset;
		ctx->last_token_sz = lex.token_sz;
		ctx->pool->cur_offset = lex.token_offset;
		ctx->pool->cur_sz = (lex.token_sz > 255) ? 255 : (uint8_t)lex.token_sz;

		// `&` followed by `[?(` with whitespace between is never valid:
		// AND_EXP requires the fused `&[?(` token, and bit-AND can't be
		// followed by `[?(` (not an expression start). Emit a tailored
		// diagnostic instead of letting the parser produce a generic
		// "syntax error" at TOK_LBRACK_QPAREN.
		if (prev_tok == TOK_AMP && tok == TOK_LBRACK_QPAREN) {
			ael_err(ctx, lex.token_offset, lex.token_sz,
					"`&[?(` must be contiguous (no whitespace allowed)");
			break;
		}

		// `&&` / `||` are two bit-op tokens in a row -- never valid, and
		// almost always a logical operator carried over from another language.
		if (prev_tok == TOK_AMP && tok == TOK_AMP) {
			ael_err(ctx, lex.token_offset,
					lex.token_sz, "use 'and' for logical AND (not '&&')");
			break;
		}

		if (prev_tok == TOK_PIPE && tok == TOK_PIPE) {
			ael_err(ctx, lex.token_offset,
					lex.token_sz, "use 'or' for logical OR (not '||')");
			break;
		}

		if (tok < 0) {
			ael_err(ctx, lex.token_offset,
					lex.token_sz, ael_lex_error_msg(tok));
			break;
		}

		if (tok == TOK_EOF) {
			Parse(&parser, 0, val, ctx);
			break;
		}

		Parse(&parser, tok, val, ctx);

		if (ael_diag_has_error(&ctx->diags)) {
			break;
		}

		prev_tok = tok;
	}

	ParseFinalize(&parser);
}

#ifdef AEL_ORACLE

//==========================================================
// Expected-token oracle (completion tooling; #ifdef'd out of the server build).
//
// ael_simulate_accepts is ported from citrusleaf/aerospike-ael's
// gen/expected.go: given a snapshot of the live parser's state-number stack,
// terminal `tok` is a legal next token iff simulating lemon's dispatch -- a
// shift, or a chain of reduces ending in a shift/accept -- never reaches
// YY_ERROR_ACTION. Reduces are followed through yyRuleInfo{Lhs,NRhs} +
// yy_find_reduce_action WITHOUT running any semantic action, so it is
// side-effect-free and cheap. It couples to lemon's generated action encoding
// (YY_MIN_REDUCE / YY_MAX_SHIFTREDUCE / ...); the drift test in tests/ael_lsp
// re-checks that contract if lemon is ever re-vendored.

// Bounds the reduce chain per probed terminal (real chains are a few dozen;
// this only stops a table-reading bug from looping forever).
#define AEL_ORACLE_MAX_STEPS 512

static bool
ael_simulate_accepts(YYACTIONTYPE* states, int top, int cap, YYCODETYPE tok)
{
	for (int steps = 0; steps < AEL_ORACLE_MAX_STEPS; steps++) {
		YYACTIONTYPE act = yy_find_shift_action(tok, states[top]);

		if (act >= YY_MIN_REDUCE) {
			int rule = (int)(act - YY_MIN_REDUCE);
			int base = top + yyRuleInfoNRhs[rule]; // NRhs stored non-positive

			if (base < 0) {
				return false; // defensive; unreachable on valid tables
			}

			YYACTIONTYPE next = yy_find_reduce_action(states[base],
					(YYCODETYPE)yyRuleInfoLhs[rule]);

			top = base + 1;

			if (top >= cap) {
				return false; // out of scratch -- treat as not accepting
			}

			states[top] = next;
		}
		else if (act <= YY_MAX_SHIFTREDUCE) {
			return true; // plain shift or shift-reduce
		}
		else if (act == YY_ACCEPT_ACTION) {
			return true;
		}
		else {
			return false; // YY_ERROR_ACTION / unused slot
		}
	}

	return false;
}

// Report whether terminal `tok` would be accepted as the next token right now
// (no syntax error). `parser` is the opaque yyParser* the caller is mid-drive;
// snapshots its state-number stack and probes without mutating it.
int
ael_can_accept(void* parser, int tok)
{
	if (tok <= 0 || tok >= YYNTOKEN) {
		return 0;
	}

	yyParser* p = (yyParser*)parser;
	YYACTIONTYPE states[YYSTACKDEPTH + 64];
	int cap = (int)(sizeof(states) / sizeof(states[0]));
	int n = 0;

	for (yyStackEntry* e = &p->yystack[0]; e <= p->yytos && n < cap; e++) {
		states[n++] = e->stateno;
	}

	if (n == 0) {
		return 0; // empty stack -- nothing to accept
	}

	return ael_simulate_accepts(states, n - 1, cap, (YYCODETYPE)tok) ? 1 : 0;
}

// Terminal codes for completion run 1 .. ael_num_tokens() - 1.
int
ael_num_tokens(void)
{
	return YYNTOKEN;
}

#endif // AEL_ORACLE
}
