/*
 * ael_func_table.h
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

#include "exp/ast.h"

//==========================================================
// Public API.
//

// Table-driven function-call dispatch. Function names and parameter names
// are ordinary identifiers (TOK_NAME) — NOT lexer keywords — so a bin named
// `value` or `bitGet` parses cleanly and adding a function is a table edit,
// not a grammar/lexer change. The parser collects a generic arg list and the
// resolver (ael_resolve_*) binds it against the spec row found here.

// Parameter-name vocabulary shared across all table-driven functions.
// AEL_PNAME_NONE marks a positional-only slot (the slot has no name and can
// only be supplied by position). Named slots are matched by these tags.
typedef enum {
	AEL_PNAME_NONE = 0,
	AEL_PNAME_OFFSET,
	AEL_PNAME_SIZE,
	AEL_PNAME_VALUE,
	AEL_PNAME_SIGNED,
	AEL_PNAME_SHIFT,
	AEL_PNAME_BYTE_OFFSET,
	AEL_PNAME_BYTE_SIZE,
	AEL_PNAME_INDEX_BITS,
	AEL_PNAME_MIN_HASH_BITS,
	AEL_PNAME_FROM,
	AEL_PNAME_TO,
	AEL_PNAME_NEEDLE,
	AEL_PNAME_OCCURRENCE,
	AEL_PNAME_LENGTH,
	AEL_PNAME_PAD,
	AEL_PNAME_FIND,
	AEL_PNAME_REPLACE,
	AEL_PNAME_PATTERN,
	AEL_PNAME_BASE,
	AEL_PNAME_EXPONENT,
	AEL_PNAME_X,
} ael_pname_t;

// Function family — selects grammar context and node-construction path.
//   SCALAR: top-level `name(...)`; builds via ast_new_func1/func2/nary.
//   BIT:    method-style `recv.name(...)`; BLOB receiver; bit builders.
//   HLL:    method-style `recv.name(...)`; HLL receiver; hll builders.
//   PATH:   method-style `recv.name(...)` CDT path functions (getKeys /
//           set / count / toInt / ...); builds an AST_PATH_FUNC_* node and
//           routes through the existing path finalizers (bin / value / ctx).
//   MODIFY: method-style `recv.name(expr)` whose body is parsed in a pushed
//           filter scope (loop vars @ / @key / @index). The `method_open`
//           mid-rule pushes the scope for this family before the args; the
//           resolver pops it and builds the SELECT-apply node (modify).
//   GEO:    top-level `name(...)` (no receiver) with a bespoke node builder —
//           geoJson('json') -> GEO literal, geoCompare(a, b) -> spatial compare.
//   STR:    method-style `recv.name(...)`; STRING receiver; string builders.
//           Rides the CDT path-func node/emit machinery (u.cdt_op chain) but
//           emits stype EXP_CALL_STRING. Bare/value receiver only (no ctx path);
//           the receiver is pinned STR.
typedef enum {
	AEL_FAM_SCALAR,
	AEL_FAM_BIT,
	AEL_FAM_HLL,
	AEL_FAM_PATH,
	AEL_FAM_MODIFY,
	AEL_FAM_GEO,
	AEL_FAM_STR,
} ael_fam_t;

// Widest fixed-arity function is bitAdd / bitSubtract (offset, size, value,
// signed) = 4 slots.
#define AEL_MAX_PARAMS 4

// One parameter slot. A slot is positional when name == AEL_PNAME_NONE,
// otherwise it is supplied by `name:`. Positional slots always precede named
// slots in a row (e.g. hllAdd's positional list before its named indexBits /
// minHashBits), so the count of leading NONE slots gives the positional arity.
typedef struct {
	ael_pname_t name;
	ast_etype etype; // inference target; AST_ETYPE_AUTO => builder owns it
} ael_param_spec_t;

// One function row.
typedef struct {
	const char* name; // "pow", "bitGet", "hllAdd", ...
	ast_node_t ast_type; // AST_FUNC_POW, AST_PATH_FUNC_BIT_GET, ...
	ael_fam_t family;
	bool variadic; // max / min: >= required_count same-type
	uint8_t param_count; // declared slots (0 when variadic)
	uint8_t required_count; // first N slots are mandatory
	ast_etype result_etype; // scalar result / operand type
	ael_param_spec_t params[AEL_MAX_PARAMS];
} ael_func_spec_t;

// Look up a function spec by name bytes (not NUL-terminated). NULL if no row
// matches.
const ael_func_spec_t* ael_func_lookup(const char* name, uint32_t sz);

// Nearest known function name to (name, sz) for a "did you mean" hint, or NULL
// if nothing is close enough. Returns a static table string.
const char* ael_func_suggest(const char* name, uint32_t sz);

// Enumerate the builtin function table (for completion tooling). Returns the
// table base; *count receives the row count. The rows are stable static data.
const ael_func_spec_t* ael_func_table_rows(uint32_t* count);

// Match parameter-name bytes to a tag. AEL_PNAME_NONE if unrecognized.
ael_pname_t ael_pname_match(const char* name, uint32_t sz);

// Human-readable parameter name for diagnostics (static string).
const char* ael_pname_str(ael_pname_t name);

// Transient pf_type ranges (contiguous in ast_node_t) — used by the unified
// method-call finalizer to route a resolved node to the bit vs HLL path.
#define AEL_IS_BIT_TYPE(t)                                                     \
	((t) >= AST_PATH_FUNC_BIT_GET && (t) <= AST_PATH_FUNC_BIT_REMOVE)
#define AEL_IS_HLL_TYPE(t)                                                     \
	((t) >= AST_PATH_FUNC_HLL_COUNT && (t) <= AST_PATH_FUNC_HLL_ADD)
// STRING functions resolved via the func table (length / substr / uppercase /
// ...). Contiguous block routed to the string finalizer by ael_resolve_str_fn.
#define AEL_IS_STR_TYPE(t)                                                     \
	((t) >= AST_PATH_FUNC_STR_LENGTH && (t) <= AST_PATH_FUNC_STR_REGEX_REPLACE)
// The casts (toInt / toFloat / toString) — NK_UNARY wrappers around their
// operand, not CDT ops, so the path finalizers route them to finish_cast /
// ael_finalize_cast rather than the leaf-consuming builders.
#define AEL_IS_CAST_TYPE(t)                                                    \
	((t) >= AST_PATH_FUNC_CAST_INT && (t) <= AST_PATH_FUNC_CAST_STRING)
// CDT path functions resolved via the func table (getKeys / set / count /
// toInt / type / ...). Two ranges: the read/cast/get block and the mutation
// block. type() reaches the finalizers via method_fn — bare bin builds
// AST_BIN_TYPE, value/ctx_list receivers reject it. AST_PATH_FUNC_GET is
// synthesized (implicit get) and never arrives via method_fn — harmless.
#define AEL_IS_PATH_FN_TYPE(t)                                                 \
	(((t) >= AST_PATH_FUNC_EXISTS && (t) <= AST_PATH_FUNC_JOIN) ||             \
			((t) >= AST_PATH_FUNC_REMOVE && (t) <= AST_PATH_FUNC_SORT))
