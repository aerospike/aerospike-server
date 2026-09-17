/*
 * lexer.re
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

#include "exp/ael_lexer.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "ael_parser.h"

#include "log.h"

// token_value rides the parser's inline value stack, so keep it at 16
// bytes: the int64_t union floor plus the 8-byte span. The regex
// sub-struct stores only lengths so it never widens the union (its
// offsets derive from the span). See ael_lexer.h.
COMPILER_ASSERT(sizeof(token_value) == 16);

// Bounded number parsing: copies the token [start, start+len) to a small
// stack buffer with null terminator, avoiding reads past the input limit.
// Token length is bounded by the regex (integer/float literals are short).
#define LEX_NUM_BUF_SZ 64

// Returns true on success, false on overflow (ERANGE) or if the token
// would exceed the stack buffer (which silently truncates digits).
static bool
lex_strtoll(const char* start, uint32_t len, int base, int64_t* out)
{
	char buf[LEX_NUM_BUF_SZ];

	if (len >= LEX_NUM_BUF_SZ) {
		return false;
	}

	memcpy(buf, start, len);
	buf[len] = '\0';

	errno = 0;
	*out = strtoll(buf, NULL, base);

	return errno != ERANGE;
}

// Returns true on success, false on overflow (ERANGE) or if the token
// would exceed the stack buffer.
static bool
lex_strtod(const char* start, uint32_t len, double* out)
{
	char buf[LEX_NUM_BUF_SZ];

	if (len >= LEX_NUM_BUF_SZ) {
		return false;
	}

	memcpy(buf, start, len);
	buf[len] = '\0';

	errno = 0;
	*out = strtod(buf, NULL);

	return errno != ERANGE;
}

void
lexer_init(lexer_t* lex, const char* input, uint32_t input_sz)
{
	lex->start = input;
	lex->cursor = input;
	lex->limit = input + input_sz;
	lex->marker = input;
	lex->token = input;
	lex->expect_regex = false;
	lex->prev_name_pattern = false;
	lex->prev_ends_operand = false;
}

// True when a numeric rule matched a leading '-' that is really infix
// subtraction (the previous token can end an operand, e.g. `$.a-1`, `1-2`).
// Gives back everything after the '-' for re-scanning; the caller returns
// TOK_MINUS. Keeps `-9223372036854775808` (INT64_MIN, unrepresentable as
// MINUS + INT) lexing as one literal wherever a sign is actually possible.
static bool
lex_infix_minus(lexer_t* lex)
{
	if (*lex->token == '-' && lex->prev_ends_operand) {
		lex->cursor = lex->token + 1;
		return true;
	}

	return false;
}

static int
lex_scan(lexer_t* lex, token_value* val)
{
loop:
	lex->token = lex->cursor;

	/*!re2c
	re2c:api = custom;
	re2c:api:style = free-form;
	re2c:define:YYCTYPE    = char;
	re2c:define:YYPEEK     = "((lex->cursor < lex->limit) ? *lex->cursor : '\\0')";
	re2c:define:YYSKIP     = "++lex->cursor;";
	re2c:define:YYBACKUP   = "lex->marker = lex->cursor;";
	re2c:define:YYRESTORE  = "lex->cursor = lex->marker;";
	re2c:define:YYLESSTHAN = "lex->limit - lex->cursor < @@{len}";
	re2c:yyfill:enable     = 0;
	re2c:eof               = 0;

	// Whitespace.
	[ \t\r\n]+   { goto loop; }

	// Comments: C-style block only (per AEL spec; // is not supported).
	// Body: [^*] or runs of * followed by a char that is neither * nor /;
	// close is one-or-more * then / (matches /**/, /* **/, etc.).
	"/*" ([^*] | "*"+ [^*/])* "*"+ "/" { goto loop; }

	// Unclosed block comment: same body, but no closing "*/" -- consumes to
	// EOF. re2c longest-match keeps the complete-comment rule above winning
	// whenever a close exists, so this fires only at EOF. Token span (set by
	// the caller from token..cursor) then covers the whole "/*..." run.
	"/*" ([^*] | "*"+ [^*/])* "*"* { return TOK_ERROR_UNCLOSED_COMMENT; }

	// Keywords.
	"and"        { return TOK_AND; }
	"or"         { return TOK_OR; }
	"not"        { return TOK_NOT; }
	"let"        { return TOK_LET; }
	"then"       { return TOK_THEN; }
	"when"       { return TOK_WHEN; }
	"default"    { return TOK_DEFAULT; }
	"in"         { return TOK_IN; }
	"true"       { return TOK_TRUE; }
	"false"      { return TOK_FALSE; }
	"unknown"    { return TOK_UNKNOWN; }
	"error"      { return TOK_UNKNOWN; }
	"NIL"        { return TOK_NIL; }
	"INF"        { return TOK_INF; }

	// Scalar math (abs / ceil / floor / log / pow / max / min /
	// countOneBits / findBitLeft / findBitRight), bit (bitGet / …), HLL
	// (hllCount / …), and most CDT path functions (get/exists/count/casts/
	// getKeys/getIndexes/.../set/remove/insert/increment/append/clear/sort)
	// are NOT keywords — they lex as TOK_NAME and dispatch through
	// ael_func_table. Likewise their parameter names (offset / size / value /
	// signed / shift / byteOffset / byteSize / indexBits / minHashBits). This
	// is why a bin named `value`, `bitGet`, or `getKeys` parses, and adding a
	// function is a table edit, not a lexer/grammar change. No path functions
	// are keywords: `type` is a TOK_NAME (AEL_FAM_PATH) and `modify` is a
	// TOK_NAME whose mid-rule filter-scope push is driven from `method_open`.
	// Bare type-name keywords (INT / FLOAT / ...) are particle-type constants
	// in operand position; type() reads a bin's type via the func table.

	// `LOCAL` — bin scope marker (uppercase modifier keyword). Promoted from
	// TOK_NAME string-compare so the parser doesn't need to validate
	// the spelling at every bin_base reduction.
	"LOCAL"        { return TOK_LOCAL; }

	// `:PROPERTY` postfix flag constants (NO_FAIL / REVERSE / LIST_ORDERED /
	// MAP_KEY_ORDERED / PERSIST_INDEX / …) are NOT keywords — they lex as
	// TOK_NAME and resolve by name in ael_resolve_prop_flag. This keeps them
	// usable as bin names ($.NO_FAIL) while the `:PROPERTY` grammar still
	// accepts them.

	// Bit-on-BLOB functions (bitGet / bitSet / …), HLL functions (hllCount /
	// hllAdd / …), and their parameter names (offset / size / value / signed
	// / shift / byteOffset / byteSize / indexBits / minHashBits) are NOT
	// keywords — they lex as TOK_NAME and dispatch through ael_func_table.

	// Geo builtins `geoJson('json...')` (GEO-typed literal) and `geoCompare(a, b)`
	// (bidirectional spatial containment) are NOT keywords — they lex as
	// TOK_NAME and dispatch through ael_func_table (AEL_FAM_GEO), so $.geoJson
	// is usable as a bin name.

	// Type names (for path function params).
	"INT"            { return TOK_TNAME_INT; }
	"STRING"         { return TOK_TNAME_STRING; }
	"HLL"            { return TOK_TNAME_HLL; }
	"BLOB"           { return TOK_TNAME_BLOB; }
	"FLOAT"          { return TOK_TNAME_FLOAT; }
	"BOOL"           { return TOK_TNAME_BOOL; }
	"LIST"           { return TOK_TNAME_LIST; }
	"MAP"            { return TOK_TNAME_MAP; }
	"GEO"            { return TOK_TNAME_GEO; }

	// VECTOR -- reserved now for a planned data type so it cannot be used as
	// an identifier; reserving keywords post-release breaks compatibility.
	// The parser rejects any use until the type ships.
	"VECTOR"         { return TOK_TNAME_VECTOR; }

	// Metadata functions (deviceSize / ttl / setName / digestModulo / …) are
	// NOT keywords — they lex as TOK_NAME and resolve by name in
	// ael_resolve_meta_call (the `$.NAME(...)` form), like `$.key()`. This
	// keeps them usable as bin names ($.ttl) while `$.ttl()` still works.

	// Numeric literals take an optional '-' sign so INT64_MIN
	// (-9223372036854775808) lexes as one token, but a '-' after an
	// operand-ending token is infix subtraction — lex_infix_minus splits it
	// off (see prev_ends_operand in ael_lexer.h).

	// Float literal.
	"-"? [0-9]+ "." [0-9]+ {
		if (lex_infix_minus(lex)) {
			return TOK_MINUS;
		}
		if (! lex_strtod(lex->token,
				(uint32_t)(lex->cursor - lex->token), &val->fval)) {
			return TOK_ERROR_FLOAT_RANGE;
		}
		return TOK_FLOAT;
	}

	// Exponent notation (1e5, 1.5e10) is unsupported -- catch the full form so
	// it is a clear error rather than an INT/FLOAT split from a NAME.
	"-"? [0-9]+ ("." [0-9]+)? [eE] [-+]? [0-9]+ {
		if (lex_infix_minus(lex)) {
			return TOK_MINUS;
		}
		return TOK_ERROR_EXP_FLOAT;
	}

	// Hex integer.
	"-"? "0x" [0-9a-fA-F]+ {
		if (lex_infix_minus(lex)) {
			return TOK_MINUS;
		}
		if (! lex_strtoll(lex->token,
				(uint32_t)(lex->cursor - lex->token), 16, &val->ival)) {
			return TOK_ERROR_INT_RANGE;
		}
		return TOK_INT;
	}

	// Binary integer.
	"-"? "0b" [01]+ {
		if (lex_infix_minus(lex)) {
			return TOK_MINUS;
		}
		const char* p = lex->token;
		bool neg = false;
		if (*p == '-') { neg = true; p++; }
		p += 2; // skip "0b"
		if (! lex_strtoll(p, (uint32_t)(lex->cursor - p), 2, &val->ival)) {
			return TOK_ERROR_INT_RANGE;
		}
		// strtoll(base=2) caps at LLONG_MAX so val->ival is non-negative;
		// defined-wraparound negation guards against future widenings that
		// could let INT64_MIN reach this point.
		if (neg) {
			val->ival = (int64_t)(0 - (uint64_t)val->ival);
		}
		return TOK_INT;
	}

	// Decimal integer.
	"-"? [0-9]+ {
		if (lex_infix_minus(lex)) {
			return TOK_MINUS;
		}
		if (! lex_strtoll(lex->token,
				(uint32_t)(lex->cursor - lex->token), 10, &val->ival)) {
			return TOK_ERROR_INT_RANGE;
		}
		return TOK_INT;
	}

	// Blob literal: X'HH...' or x'HH...' (spec form). Hex chars between
	// single quotes; even count required.
	[xX] "'" [0-9a-fA-F]* "'" {
		const char* p = lex->token + 2;       // skip x' / X'
		const char* end = lex->cursor - 1;    // strip trailing '
		uint32_t hex_sz = (uint32_t)(end - p);

		if (hex_sz % 2 != 0) {
			return TOK_ERROR_BLOB_ODD_HEX;
		}

		val->str.offset = (uint32_t)(p - lex->start);
		val->str.sz = hex_sz;
		return TOK_BLOB_LITERAL;
	}

	// Base64 blob literal: b64'...' or B64'...' (spec form). The regex
	// pins the charset and at most 2 trailing '=' pads, so only the
	// length (a multiple of 4) needs checking here.
	[bB] "64" "'" [A-Za-z0-9+/]* [=]{0,2} "'" {
		const char* p = lex->token + 4;       // skip b64' / B64'
		const char* end = lex->cursor - 1;    // strip trailing '
		uint32_t b64_sz = (uint32_t)(end - p);

		if (b64_sz != 0 && b64_sz % 4 != 0) {
			return TOK_ERROR_B64_BAD_LEN;
		}

		val->str.offset = (uint32_t)(p - lex->start);
		val->str.sz = b64_sz;
		return TOK_B64_BLOB_LITERAL;
	}

	// Quoted strings — `\X` is accepted here as an in-string escape
	// pair so `\"` / `\'` don't terminate the string early. The
	// individual escapes are validated and decoded later by
	// ael_string_validate / ael_string_decode. Raw newline (0x0A)
	// and CR (0x0D) bytes inside the quotes break the body match
	// and fall through to TOK_ERROR → TOK_ERROR_UNCLOSED_STRING via
	// the post-processor below — use `\n` / `\r` escapes instead.
	"'" ([^'\\\n\r] | "\\" [^\n\r])* "'" {
		val->str.offset = (uint32_t)(lex->token + 1 - lex->start);
		val->str.sz = (uint32_t)(lex->cursor - lex->token - 2);
		return TOK_STRING;
	}

	"\"" ([^"\\\n\r] | "\\" [^\n\r])* "\"" {
		val->str.offset = (uint32_t)(lex->token + 1 - lex->start);
		val->str.sz = (uint32_t)(lex->cursor - lex->token - 2);
		return TOK_STRING;
	}

	// Variable reference ${name}.
	"${" [a-zA-Z_][a-zA-Z0-9_]* "}" {
		val->str.offset = (uint32_t)(lex->token + 2 - lex->start);
		val->str.sz = (uint32_t)(lex->cursor - lex->token - 3);
		return TOK_VARIABLE;
	}

	// Placeholder ?N.
	"?" [0-9]+ {
		if (! lex_strtoll(lex->token + 1,
				(uint32_t)(lex->cursor - lex->token - 1), 10, &val->ival)) {
			return TOK_ERROR_INT_RANGE;
		}
		return TOK_PLACEHOLDER;
	}

	// Dollar-dot path prefix.
	"$."   { return TOK_DOLLAR_DOT; }

	// Multi-char operators (order matters for longest match).
	">>>"  { return TOK_RSHIFT_LOGIC; }
	">>"   { return TOK_RSHIFT; }
	"<<"   { return TOK_LSHIFT; }
	"**"   { return TOK_POWER; }
	"=="   { return TOK_EQ; }
	"!="   { return TOK_NE; }
	">="   { return TOK_GE; }
	"<="   { return TOK_LE; }
	"=>"   { return TOK_ARROW; }
	// `..` is reserved for future recursive-descent path syntax (similar
	// to JSONPath). Inside CDT selectors, ranges now use `:` instead of
	// `..`. Tokenize `..` to a dedicated error so it produces a pointed
	// diagnostic rather than two adjacent TOK_DOT tokens.
	".."   { return TOK_ERROR_RANGE_DOTS; }
	"[?("  { return TOK_LBRACK_QPAREN; }
	"&[?(" { return TOK_AMP_LBRACK_QPAREN; }
	"@key"   { return TOK_AT_KEY; }
	"@index" { return TOK_AT_INDEX; }
	"@"      { return TOK_AT; }

	// Compound bracket tokens.
	"{="   { return TOK_LBRACE_EQ; }
	"{!="  { return TOK_LBRACE_BANG_EQ; }
	"{!"   { return TOK_LBRACE_BANG; }
	"{#"   { return TOK_LBRACE_HASH; }
	"{!#"  { return TOK_LBRACE_BANG_HASH; }
	"{@"   { return TOK_LBRACE_AT; }
	"{!@"  { return TOK_LBRACE_BANG_AT; }
	"[="   { return TOK_LBRACKET_EQ; }
	"[!="  { return TOK_LBRACKET_BANG_EQ; }
	"[!"   { return TOK_LBRACKET_BANG; }
	"[#"   { return TOK_LBRACKET_HASH; }
	"[!#"  { return TOK_LBRACKET_BANG_HASH; }

	// Single-char operators and punctuation.
	">"    { return TOK_GT; }
	"<"    { return TOK_LT; }
	"+"    { return TOK_PLUS; }
	"-"    { return TOK_MINUS; }
	"*"    { return TOK_STAR; }
	"/"    { return TOK_SLASH; }
	"%"    { return TOK_PERCENT; }
	"&"    { return TOK_AMP; }
	"|"    { return TOK_PIPE; }
	"^"    { return TOK_CARET; }
	"~"    { return TOK_TILDE; }
	"("    { return TOK_LPAREN; }
	")"    { return TOK_RPAREN; }
	"["    { return TOK_LBRACKET; }
	"]"    { return TOK_RBRACKET; }
	"{"    { return TOK_LBRACE; }
	"}"    { return TOK_RBRACE; }
	","    { return TOK_COMMA; }
	"."    { return TOK_DOT; }
	":"    { return TOK_COLON; }
	"=~"   { return TOK_MATCH; }
	"="    { return TOK_ASSIGN; }
	"!"    { return TOK_ERROR_LOGICAL_NOT; }

	// Name identifier (must come after all keywords).
	[a-zA-Z_][a-zA-Z0-9_]* {
		val->str.offset = (uint32_t)(lex->token - lex->start);
		val->str.sz = (uint32_t)(lex->cursor - lex->token);
		return TOK_NAME;
	}

	// End of input (re2c:eof bounds-checked).
	$      { return TOK_EOF; }

	// Any other character.
	*      { return TOK_ERROR; }
	*/
}

// Scan a regex literal (lex->cursor at the opening '/'): pattern up to the next
// unescaped '/', then [imsgxw] flags. Newline/EOF before the close ->
// TOK_ERROR_UNCLOSED_REGEX. Fills val->regex with the pattern + flag lengths;
// the offsets derive from the token span (see token_value).
static int
lex_scan_regex(lexer_t* lex, token_value* val)
{
	const char* pat = lex->cursor + 1; // past the opening '/'
	const char* p = pat;

	while (p < lex->limit && *p != '/' && *p != '\n') {
		p += (*p == '\\' && p + 1 < lex->limit) ? 2 : 1;
	}

	if (p >= lex->limit || *p != '/') {
		lex->cursor = lex->limit;
		return TOK_ERROR_UNCLOSED_REGEX;
	}

	val->regex.pat_sz = (uint32_t)(p - pat);

	const char* flag = p + 1; // past the closing '/'

	p = flag;

	while (p < lex->limit && (*p == 'i' || *p == 'm' || *p == 's' ||
			*p == 'g' || *p == 'x' || *p == 'w')) {
		p++;
	}

	val->regex.flag_sz = (uint32_t)(p - flag);

	lex->cursor = p;
	return TOK_REGEX;
}

// Tokens that can end an operand — a '-' right after one of these is infix
// subtraction, never a numeric literal's sign. Closing brackets, literals,
// names, variables/placeholders, loop vars, and the bare type-name constants
// (operand position, e.g. `$.a.type() == INT`). Everything else (operators,
// commas, colons, opening brackets, keywords like `in`/`then`) leaves a
// following signed literal intact.
static bool
tok_ends_operand(int tok)
{
	switch (tok) {
	case TOK_NAME:
	case TOK_INT:
	case TOK_FLOAT:
	case TOK_STRING:
	case TOK_BLOB_LITERAL:
	case TOK_B64_BLOB_LITERAL:
	case TOK_VARIABLE:
	case TOK_PLACEHOLDER:
	case TOK_TRUE:
	case TOK_FALSE:
	case TOK_UNKNOWN:
	case TOK_NIL:
	case TOK_INF:
	case TOK_RPAREN:
	case TOK_RBRACKET:
	case TOK_RBRACE:
	case TOK_AT:
	case TOK_AT_KEY:
	case TOK_AT_INDEX:
	case TOK_REGEX:
	case TOK_TNAME_INT:
	case TOK_TNAME_STRING:
	case TOK_TNAME_HLL:
	case TOK_TNAME_BLOB:
	case TOK_TNAME_FLOAT:
	case TOK_TNAME_BOOL:
	case TOK_TNAME_LIST:
	case TOK_TNAME_MAP:
	case TOK_TNAME_GEO:
	case TOK_TNAME_VECTOR:
		return true;
	default:
		return false;
	}
}

int
lexer_next(lexer_t* lex, token_value* val)
{
	memset(val, 0, sizeof(*val));

	// Regex-literal latch (set right after `=~`). A regex literal is legal only
	// here, so scan `/pattern/flags` directly; anything else clears the latch
	// and falls through to normal scanning (a malformed RHS then gives an
	// ordinary error, and `/` keeps lexing as divide everywhere else).
	if (lex->expect_regex) {
		lex->expect_regex = false;

		const char* p = lex->cursor;

		while (p < lex->limit &&
				(*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) {
			p++;
		}

		if (p < lex->limit && *p == '/') {
			lex->cursor = p;
			lex->token = p;

			int rtok = lex_scan_regex(lex, val);

			lex->token_offset = (uint32_t)(lex->token - lex->start);
			lex->token_sz = (uint32_t)(lex->cursor - lex->token);

			// offsets derive from this span; see token_value.
			val->offset = lex->token_offset;
			val->sz = lex->token_sz;
			lex->prev_ends_operand = tok_ends_operand(rtok);
			return rtok;
		}
	}

	if (lex->cursor >= lex->limit) {
		lex->token_offset = (uint32_t)(lex->limit - lex->start);
		lex->token_sz = 0;
		return TOK_EOF;
	}

	int tok = lex_scan(lex, val);

	if (tok == TOK_MATCH) {
		lex->expect_regex = true;
	}
	else if (tok == TOK_COLON && lex->prev_name_pattern) {
		// regexReplace(pattern: /re/flags, ...) — the operand after
		// `pattern:` is a regex literal. A false latch (e.g.
		// `$.pattern:INT`) is harmless: lexer_next only scans a regex
		// when the next non-space char is `/`.
		lex->expect_regex = true;
	}

	lex->token_offset = (uint32_t)(lex->token - lex->start);
	lex->token_sz = (uint32_t)(lex->cursor - lex->token);

	// Carry the token's own span so grammar reductions can stamp precise AST
	// node offsets (see token_value). Every token gets it, including numeric
	// literals whose val holds ival/fval (no str span otherwise).
	val->offset = lex->token_offset;
	val->sz = lex->token_sz;

	lex->prev_name_pattern = (tok == TOK_NAME && lex->token_sz == 7 &&
			memcmp(lex->start + lex->token_offset, "pattern", 7) == 0);
	lex->prev_ends_operand = tok_ends_operand(tok);

	switch (tok) {
	case TOK_INT:
	case TOK_FLOAT:
	case TOK_PLACEHOLDER:
	case TOK_STRING:           // legitimate sz == 0 for "" / ''
	case TOK_BLOB_LITERAL:     // legitimate sz == 0 for x'' / X''
	case TOK_B64_BLOB_LITERAL: // legitimate sz == 0 for b64'' / B64''
		break;
	default:
		// Backfill str position for tokens that don't set val themselves:
		// keywords, operators, punctuation
		if (val->str.sz == 0) {
			val->str.offset = lex->token_offset;
			val->str.sz = lex->token_sz;
		}
		break;
	}

	// Refine generic TOK_ERROR into specific error tokens based on what
	// the lexer was looking at when it failed.
	if (tok == TOK_ERROR) {
		char ch = *lex->token;

		if (ch == '\'' || ch == '"') {
			// Advance cursor to EOF so the parser stops cleanly.
			lex->token_sz = (uint32_t)(lex->limit - lex->token);
			lex->cursor = lex->limit;
			tok = TOK_ERROR_UNCLOSED_STRING;
		}
		// Unclosed comments are caught by their own re2c rule (see the
		// "/*"... rule above), never reaching this generic-error refinement.
	}

	return tok;
}
