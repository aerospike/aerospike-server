/*
 * ast.c
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

//==========================================================
// Includes.
//

#include "exp/ast.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dynbuf.h"
#include "log.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "exp/ael_context.h"
#include "exp/ael_diag.h"
#include "exp/ael_string.h"
#include "exp/exp.h"
#include "exp/exp_wire.h"

//==========================================================
// Typedefs & constants.
//

#define AEL_ETYPE_STR_SZ 64

//==========================================================
// Forward declarations.
//

static const char* ast_esubtype_name(ast_esubtype sub);
static const char* ast_etype_name(ast_etype et);
static const char* ast_etype_str(ast_etype et, char* buf, size_t sz);
static ast_etype ast_set_implicit_type_lr(ael_context* ctx, ast_node_t op,
		ast_ref left, ast_ref right, ast_etype etype);

//==========================================================
// Node info table.
//

#define N(t, n, k, ...) [t] = { .name = (n), .kind = (k), __VA_ARGS__ }
#define N_CMD(t, n, c, k, ...)                                                 \
	[t] = { .name = (n), .exp_cmd = (c), .kind = (k), __VA_ARGS__ }

// Single-select path segs accept CTX_CREATE props (LIST_*, MAP_*,
// PERSIST_INDEX) and any leaf etype via :T postfix.
#define N_SEG_S(t, n, f, ctx, get, rm)                                         \
	[t] = { .name = (n),                                                       \
		.flags = (f),                                                          \
		.ctx_type = (ctx),                                                     \
		.cdt_get_op = (get),                                                   \
		.cdt_remove_op = (rm),                                                 \
		.kind = NK_SEG_S,                                                      \
		.valid_props = AST_PROP_VALID_PATH_SEG,                                \
		.valid_types = AST_ETYPE_AUTO }

// Multi-select segs accept :T (propagates to the wrapping GET) but no
// CTX_CREATE props (those need a single concrete seg to land in). Range
// and list segs share the row shape; the layout split (u.range_seg vs
// u.list_seg) is carried by AST_NF_LIST_SEG in the flags.
#define N_SEG_M(t, n, f, ctx_t, get, rm)                                       \
	[t] = { .name = (n),                                                       \
		.flags = (f),                                                          \
		.ctx_type = (ctx_t),                                                   \
		.cdt_get_op = (get),                                                   \
		.cdt_remove_op = (rm),                                                 \
		.kind = NK_SEG_M,                                                      \
		.valid_types = AST_ETYPE_AUTO }

#define N_CDT_OP(t, n, f, ...)                                                 \
	[t] = { .name = (n), .flags = (f), .kind = NK_CDT_OP, __VA_ARGS__ }

const ast_node_info ast_node_table[AST_NODE_TYPE_COUNT] = {

	//==========================================================
	// Logical.

	N(AST_NIL, "nil", NK_LEAF),
	N(AST_UNKNOWN, "unknown", NK_LEAF),
	N_CMD(AST_AND, "and", EXP_AND, NK_LIST),
	N_CMD(AST_OR, "or", EXP_OR, NK_LIST),
	N_CMD(AST_NOT, "not", EXP_NOT, NK_UNARY),
	N_CMD(AST_EXCLUSIVE, "exclusive", EXP_EXCLUSIVE, NK_LIST),

	//==========================================================
	// Comparison.

	N_CMD(AST_CMP_EQ, "==", EXP_CMP_EQ, NK_BINARY),
	N_CMD(AST_CMP_NE, "!=", EXP_CMP_NE, NK_BINARY),
	N_CMD(AST_CMP_GT, ">", EXP_CMP_GT, NK_BINARY),
	N_CMD(AST_CMP_GE, ">=", EXP_CMP_GE, NK_BINARY),
	N_CMD(AST_CMP_LT, "<", EXP_CMP_LT, NK_BINARY),
	N_CMD(AST_CMP_LE, "<=", EXP_CMP_LE, NK_BINARY),
	N_CMD(AST_CMP_IN, "in", EXP_IN_LIST, NK_BINARY),

	// Bidirectional spatial containment — both args must be GEOJSON.
	N_CMD(AST_CMP_GEO, "geoCompare", EXP_CMP_GEO, NK_BINARY),

	//==========================================================
	// Arithmetic.

	N_CMD(AST_ADD, "+", EXP_ADD, NK_NMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_SUB, "-", EXP_SUB, NK_BMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_MUL, "*", EXP_MUL, NK_NMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_DIV, "/", EXP_DIV, NK_BMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_MOD, "%", EXP_MOD, NK_BMATH),
	N_CMD(AST_POW, "**", EXP_POW, NK_BMATH),

	//==========================================================
	// Bitwise.

	N_CMD(AST_BIT_AND, "&", EXP_INT_AND, NK_LIST),
	N_CMD(AST_BIT_OR, "|", EXP_INT_OR, NK_LIST),
	N_CMD(AST_BIT_XOR, "^", EXP_INT_XOR, NK_LIST),
	N_CMD(AST_BIT_NOT, "~", EXP_INT_NOT, NK_UNARY),
	N_CMD(AST_LSHIFT, "<<", EXP_INT_LSHIFT, NK_BINARY),
	N_CMD(AST_RSHIFT_ARITH, ">>", EXP_INT_ARSHIFT, NK_BINARY),
	N_CMD(AST_RSHIFT_LOGIC, ">>>", EXP_INT_RSHIFT, NK_BINARY),

	//==========================================================
	// Literals.

	N(AST_INT, "int", NK_LEAF),
	N(AST_FLOAT, "float", NK_LEAF),
	N(AST_ZERO, "zero", NK_LEAF),
	N(AST_STRING, "string", NK_LEAF),
	N(AST_BOOL, "bool", NK_LEAF),
	N(AST_BLOB, "blob", NK_LEAF),
	N(AST_B64_BLOB, "b64_blob", NK_LEAF),
	N(AST_GEO_LITERAL, "geoJson", NK_LEAF),
	N(AST_INF, "inf", NK_LEAF),
	N(AST_WILDCARD, "wildcard", NK_LEAF),

	//==========================================================
	// Collections.

	N(AST_LIST, "list", NK_LIST),
	N(AST_MAP, "map", NK_LIST),

	//==========================================================
	// Bin / path / meta / var.

	N(AST_BIN, "bin", NK_LEAF, .valid_types = AST_ETYPE_AUTO),
	N(AST_BIN_REF, "bin_ref", NK_LEAF, .valid_types = AST_ETYPE_AUTO),
	N(AST_BIN_TYPE, "bin_type", NK_LEAF),
	N(AST_BIN_EXISTS, "bin_exists", NK_LEAF),
	N(AST_PATH_CTX, "path_ctx", NK_NONE),
	N(AST_PATH_CALL, "path_call", NK_NONE, .flags = AST_NF_NEEDS_ETYPE),
	// AST_META covers $.key() / $.ttl() / digest_modulo / etc.; the
	// only meta op that takes a :T postfix is EXP_REC_KEY (via the
	// $.key():T grammar rule), so the AUTO_KEY mask is what the
	// postfix-apply helper needs to validate against. Other meta ops
	// never reach the helper.
	N(AST_META, "meta", NK_LEAF, .valid_types = AST_ETYPE_AUTO_KEY),
	N(AST_VAR, "var", NK_LEAF),
	N(AST_LET, "let", NK_LIST),
	N(AST_WHEN, "when", NK_LIST),

	//==========================================================
	// Functions.

	N_CMD(AST_FUNC_ABS, "abs", EXP_ABS, NK_UNARY),
	N_CMD(AST_FUNC_CEIL, "ceil", EXP_CEIL, NK_UNARY),
	N_CMD(AST_FUNC_FLOOR, "floor", EXP_FLOOR, NK_UNARY),
	N_CMD(AST_FUNC_LOG, "log", EXP_LOG, NK_BINARY),
	N_CMD(AST_FUNC_POW, "pow", EXP_POW, NK_BINARY),
	N_CMD(AST_FUNC_MAX, "max", EXP_MAX, NK_NMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_FUNC_MIN, "min", EXP_MIN, NK_NMATH, .flags = AST_NF_NEEDS_ETYPE),
	N_CMD(AST_FUNC_COUNT_ONE_BITS, "countOneBits", EXP_INT_COUNT, NK_UNARY),
	N_CMD(AST_FUNC_FIND_BIT_LEFT, "findBitLeft", EXP_INT_LSCAN, NK_BINARY),
	N_CMD(AST_FUNC_FIND_BIT_RIGHT, "findBitRight", EXP_INT_RSCAN, NK_BINARY),

	//==========================================================
	// Path functions.

	// CDT reads — props rejected; :T pin accepted (lands on the call's
	// etype as the value-type hint for ast_etype_to_rtype).
	N_CDT_OP(AST_PATH_FUNC_EXISTS, "exists()", 0, .valid_types = AST_ETYPE_AUTO),
	N_CDT_OP(AST_PATH_FUNC_COUNT, "count()", 0, .valid_types = AST_ETYPE_AUTO),
	N_CMD(AST_PATH_FUNC_CAST_INT, "toInt()", EXP_TO_INT, NK_UNARY),
	N_CMD(AST_PATH_FUNC_CAST_FLOAT, "toFloat()", EXP_TO_FLOAT, NK_UNARY),
	N_CMD(AST_PATH_FUNC_CAST_STRING, "toString()", EXP_TO_STRING, NK_UNARY),
	N_CDT_OP(AST_PATH_FUNC_GET, "get()", 0, .valid_types = AST_ETYPE_AUTO),
	N_CDT_OP(AST_PATH_FUNC_GET_KEYS, "getKeys()", 0,
			.valid_types = AST_ETYPE_AUTO),
	N_CDT_OP(AST_PATH_FUNC_GET_KEY_VALUES, "getKeyValues()", 0,
			.valid_types = AST_ETYPE_AUTO),
	N_CDT_OP(AST_PATH_FUNC_GET_TREE, "getTree()", 0,
			.valid_types = AST_ETYPE_AUTO),
	N_CDT_OP(AST_PATH_FUNC_GET_INDEXES, "getIndexes()", 0,
			.valid_types = AST_ETYPE_AUTO,
			.valid_props = AST_PROP_VALID_GET_POSITIONAL),
	N_CDT_OP(AST_PATH_FUNC_GET_RANKS, "getRanks()", 0,
			.valid_types = AST_ETYPE_AUTO,
			.valid_props = AST_PROP_VALID_GET_POSITIONAL),
	N_CDT_OP(AST_PATH_FUNC_GET_MAPS, "getMaps()", 0, .valid_types = AST_ETYPE_AUTO,
			.valid_props = AST_PROP_VALID_GET_MAPS),
	N(AST_PATH_FUNC_TYPE, "type()", NK_UNARY),

	// join() — LIST → STR. Op + STR result forced in the resolver; the
	// separator rides the u.cdt_op chain. :T pin is meaningless (result is
	// always STR), so valid_types is STR.
	N_CDT_OP(AST_PATH_FUNC_JOIN, "join()", AST_NF_WHOLE_COLL,
			.valid_types = AST_ETYPE_STR),

	// BLOB bit-op path functions (transient pf_types — morphed to
	// AST_BIT_OP by ael_finalize_bit_call). The exp_cmd slot carries
	// the runtime AS_BITS_OP_* opcode so codegen can read it directly.
	// Modify ops carry AST_NF_MODIFY; ael_finalize_bit_call sets
	// EXP_CALL_FLAG_MODIFY_LOCAL on the wrapping path-call's stype.
	N(AST_PATH_FUNC_BIT_GET, "bitGet()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_GET),
	N(AST_PATH_FUNC_BIT_B64_ENCODE, "b64Encode()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_B64_ENCODE),
	N(AST_PATH_FUNC_BIT_COUNT, "bitCount()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_COUNT),
	N(AST_PATH_FUNC_BIT_LSCAN, "bitLscan()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_LSCAN),
	N(AST_PATH_FUNC_BIT_RSCAN, "bitRscan()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_RSCAN),
	N(AST_PATH_FUNC_BIT_GET_INT, "bitGetInt()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_GET_INT),
	N(AST_PATH_FUNC_BIT_SET, "bitSet()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_SET,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_OR, "bitOr()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_OR,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_XOR, "bitXor()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_XOR,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_AND, "bitAnd()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_AND,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_NOT, "bitNot()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_NOT,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_LSHIFT, "bitLshift()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_LSHIFT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_RSHIFT, "bitRshift()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_RSHIFT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_INPLACE),
	N(AST_PATH_FUNC_BIT_ADD, "bitAdd()", NK_CDT_OP, .exp_cmd = AS_BITS_OP_ADD,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_BIT_ARITH),
	N(AST_PATH_FUNC_BIT_SUBTRACT, "bitSubtract()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_SUBTRACT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_ARITH),
	N(AST_PATH_FUNC_BIT_SET_INT, "bitSetInt()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_SET_INT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_ARITH),
	N(AST_PATH_FUNC_BIT_RESIZE, "bitResize()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_RESIZE, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_SIZING),
	N(AST_PATH_FUNC_BIT_INSERT, "bitInsert()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_INSERT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_SIZING),
	N(AST_PATH_FUNC_BIT_REMOVE, "bitRemove()", NK_CDT_OP,
			.exp_cmd = AS_BITS_OP_REMOVE, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_BIT_INPLACE),

	// HLL transient pf_types (morphed to AST_HLL_OP by
	// ael_finalize_hll_call). exp_cmd carries the AS_HLL_OP_* opcode.
	N(AST_PATH_FUNC_HLL_COUNT, "hllCount()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_COUNT),
	N(AST_PATH_FUNC_HLL_DESCRIBE, "hllDescribe()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_DESCRIBE),
	N(AST_PATH_FUNC_HLL_MAY_CONTAIN, "hllMayContain()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_MAY_CONTAIN),
	N(AST_PATH_FUNC_HLL_UNION, "hllUnion()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_GET_UNION),
	N(AST_PATH_FUNC_HLL_UNION_COUNT, "hllUnionCount()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_UNION_COUNT),
	N(AST_PATH_FUNC_HLL_INTERSECT_COUNT, "hllIntersectCount()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_INTERSECT_COUNT),
	N(AST_PATH_FUNC_HLL_SIMILARITY, "hllSimilarity()", NK_CDT_OP,
			.exp_cmd = AS_HLL_OP_SIMILARITY),
	// HLL_INIT supports the full INIT-only update-or-create flag set; ADD
	// rejects UPDATE_ONLY since it has no create-vs-update mode.
	N(AST_PATH_FUNC_HLL_INIT, "hllInit()", NK_CDT_OP, .exp_cmd = AS_HLL_OP_INIT,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_HLL_MODIFY),
	N(AST_PATH_FUNC_HLL_ADD, "hllAdd()", NK_CDT_OP, .exp_cmd = AS_HLL_OP_ADD,
			.flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_NO_FAIL | AST_PROP_CREATE_ONLY),

	// STRING transient pf_types (morphed to AST_STR_OP). exp_cmd carries the
	// AS_STRING_OP_* opcode; AST_NF_MODIFY marks the value-producing ops (they
	// get MODIFY_LOCAL). No :PROPERTY flags on string ops -- valid_props is 0.
	N(AST_PATH_FUNC_STR_LENGTH, "strlen()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_STRLEN),
	N(AST_PATH_FUNC_STR_SUBSTR, "substr()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_SUBSTR),
	N(AST_PATH_FUNC_STR_INDEX_OF, "find()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_FIND),
	N(AST_PATH_FUNC_STR_CHAR_AT, "charAt()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_CHAR_AT),
	N(AST_PATH_FUNC_STR_CONTAINS, "contains()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_CONTAINS),
	N(AST_PATH_FUNC_STR_STARTS_WITH, "startsWith()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_STARTS_WITH),
	N(AST_PATH_FUNC_STR_ENDS_WITH, "endsWith()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_ENDS_WITH),
	N(AST_PATH_FUNC_STR_TO_INT, "toInt()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_TO_INTEGER),
	N(AST_PATH_FUNC_STR_TO_FLOAT, "toFloat()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_TO_DOUBLE),
	N(AST_PATH_FUNC_STR_BYTES_LENGTH, "bytesLength()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_BYTE_LENGTH),
	N(AST_PATH_FUNC_STR_IS_NUMERIC, "isNumeric()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_IS_NUMERIC),
	N(AST_PATH_FUNC_STR_IS_UPPER, "isUpper()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_IS_UPPER),
	N(AST_PATH_FUNC_STR_IS_LOWER, "isLower()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_IS_LOWER),
	N(AST_PATH_FUNC_STR_TO_BLOB, "toBlob()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_TO_BLOB),
	N(AST_PATH_FUNC_STR_SPLIT, "split()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_SPLIT),
	N(AST_PATH_FUNC_STR_FROM_BASE64, "b64Decode()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_B64_DECODE),
	N(AST_PATH_FUNC_STR_REGEX_COMPARE, "=~", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_REGEX_COMPARE),
	N(AST_PATH_FUNC_STR_INSERT, "splice()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_INSERT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_OVERWRITE, "overwrite()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_OVERWRITE, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_SNIP, "snip()", NK_CDT_OP, .exp_cmd = AS_STRING_OP_SNIP,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_REPLACE, "replace()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_REPLACE, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_REPLACE_ALL, "replaceAll()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_REPLACE_ALL, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_UPPERCASE, "upper()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_UPPER, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_LOWERCASE, "lower()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_LOWER, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_CASEFOLD, "caseFold()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_CASE_FOLD, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_NORMALIZE, "normalizeNFC()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_NORMALIZE_NFC, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_TRIM_START, "trimStart()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_TRIM_START, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_TRIM_END, "trimEnd()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_TRIM_END, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_TRIM, "trim()", NK_CDT_OP, .exp_cmd = AS_STRING_OP_TRIM,
			.flags = AST_NF_MODIFY, .valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_PAD_START, "padStart()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_PAD_START, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_PAD_END, "padEnd()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_PAD_END, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_REPEAT, "repeat()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_REPEAT, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),
	N(AST_PATH_FUNC_STR_REGEX_REPLACE, "regexReplace()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_REGEX_REPLACE, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),

	// concat's fold block — AS_STRING_OP_APPEND (scalar-arg modify).
	N(AST_PATH_FUNC_STR_APPEND, "concat()", NK_CDT_OP,
			.exp_cmd = AS_STRING_OP_APPEND, .flags = AST_NF_MODIFY,
			.valid_props = AST_PROP_VALID_STR_MODIFY),

	//==========================================================
	// Single-select map segments.

	N_SEG_S(AST_MAP_KEY, "map_key", AST_NF_MAP_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_KEY), AS_CDT_OP_MAP_GET_BY_KEY,
			AS_CDT_OP_MAP_REMOVE_BY_KEY),
	N_SEG_S(AST_MAP_VALUE, "map_value", AST_NF_MAP_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE), AS_CDT_OP_MAP_GET_BY_VALUE,
			AS_CDT_OP_MAP_REMOVE_BY_VALUE),
	N_SEG_S(AST_MAP_INDEX, "map_index", AST_NF_MAP_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_INDEX), AS_CDT_OP_MAP_GET_BY_INDEX,
			AS_CDT_OP_MAP_REMOVE_BY_INDEX),
	N_SEG_S(AST_MAP_RANK, "map_rank", AST_NF_MAP_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_RANK), AS_CDT_OP_MAP_GET_BY_RANK,
			AS_CDT_OP_MAP_REMOVE_BY_RANK),

	//==========================================================
	// Single-select list segments.

	N_SEG_S(AST_LIST_INDEX, "list_index", 0, (AS_CDT_CTX_LIST | AS_CDT_CTX_INDEX),
			AS_CDT_OP_LIST_GET_BY_INDEX, AS_CDT_OP_LIST_REMOVE_BY_INDEX),
	N_SEG_S(AST_LIST_VALUE, "list_value", 0, (AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE),
			AS_CDT_OP_LIST_GET_BY_VALUE, AS_CDT_OP_LIST_REMOVE_BY_VALUE),
	N_SEG_S(AST_LIST_RANK, "list_rank", 0, (AS_CDT_CTX_LIST | AS_CDT_CTX_RANK),
			AS_CDT_OP_LIST_GET_BY_RANK, AS_CDT_OP_LIST_REMOVE_BY_RANK),

	//==========================================================
	// Multi-select map range segments.

	N_SEG_M(AST_MAP_KEY_RANGE, "map_key_range", AST_NF_MAP_SEG | AST_NF_PLURAL,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_KEY_INTERVAL),
			AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL,
			AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL),
	N_SEG_M(AST_MAP_INDEX_RANGE, "map_index_range", AST_NF_MAP_SEG | AST_NF_PLURAL,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_INDEX_RANGE),
			AS_CDT_OP_MAP_GET_BY_INDEX_RANGE, AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE),
	N_SEG_M(AST_MAP_VALUE_RANGE, "map_value_range", AST_NF_MAP_SEG | AST_NF_PLURAL,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE_INTERVAL),
			AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL,
			AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL),
	N_SEG_M(AST_MAP_RANK_RANGE, "map_rank_range", AST_NF_MAP_SEG | AST_NF_PLURAL,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_RANK_RANGE),
			AS_CDT_OP_MAP_GET_BY_RANK_RANGE, AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE),

	//==========================================================
	// Multi-select list range segments.

	N_SEG_M(AST_LIST_INDEX_RANGE, "list_index_range", AST_NF_PLURAL,
			(AS_CDT_CTX_LIST | AS_CDT_CTX_INDEX_RANGE),
			AS_CDT_OP_LIST_GET_BY_INDEX_RANGE,
			AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE),
	N_SEG_M(AST_LIST_VALUE_RANGE, "list_value_range", AST_NF_PLURAL,
			(AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE_INTERVAL),
			AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL,
			AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL),
	N_SEG_M(AST_LIST_RANK_RANGE, "list_rank_range", AST_NF_PLURAL,
			(AS_CDT_CTX_LIST | AS_CDT_CTX_RANK_RANGE),
			AS_CDT_OP_LIST_GET_BY_RANK_RANGE, AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE),

	//==========================================================
	// Relative range segments.

	N_SEG_M(AST_MAP_INDEX_REL_RANGE, "map_index_rel_range",
			AST_NF_MAP_SEG | AST_NF_PLURAL | AST_NF_REL_RANGE,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_KEY_REL_INDEX_RANGE),
			AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE,
			AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE),
	N_SEG_M(AST_MAP_RANK_REL_RANGE, "map_rank_rel_range",
			AST_NF_MAP_SEG | AST_NF_PLURAL | AST_NF_REL_RANGE,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE_REL_RANK_RANGE),
			AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE,
			AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE),
	N_SEG_M(AST_LIST_RANK_REL_RANGE, "list_rank_rel_range",
			AST_NF_PLURAL | AST_NF_REL_RANGE,
			(AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE_REL_RANK_RANGE),
			AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE,
			AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE),

	//==========================================================
	// Multi-select map list segments.

	N_SEG_M(AST_MAP_KEY_LIST, "map_key_list",
			AST_NF_MAP_SEG | AST_NF_PLURAL | AST_NF_LIST_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_KEY_LIST),
			AS_CDT_OP_MAP_GET_BY_KEY_LIST, AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST),
	N_SEG_M(AST_MAP_VALUE_LIST, "map_value_list",
			AST_NF_MAP_SEG | AST_NF_PLURAL | AST_NF_LIST_SEG,
			(AS_CDT_CTX_MAP | AS_CDT_CTX_VALUE_LIST),
			AS_CDT_OP_MAP_GET_BY_VALUE_LIST, AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST),

	//==========================================================
	// Multi-select list list segments.

	N_SEG_M(AST_LIST_VALUE_LIST, "list_value_list",
			AST_NF_PLURAL | AST_NF_LIST_SEG,
			(AS_CDT_CTX_LIST | AS_CDT_CTX_VALUE_LIST),
			AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST,
			AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST),

	//==========================================================
	// Modify path functions.

	// Rows serving both containers carry the union; ael_check_cdt_op_props
	// re-checks against the receiver once the leaf selector fixes it.
	N_CDT_OP(AST_PATH_FUNC_REMOVE, "remove()", AST_NF_MODIFY | AST_NF_MOD_TARGET,
			.valid_props = AST_PROP_VALID_CDT_MODIFY),
	// setTo (identifier SET) — DEFAULT upsert.
	N_CDT_OP(AST_PATH_FUNC_SET, "setTo()",
			AST_NF_MODIFY | AST_NF_MOD_TARGET | AST_NF_NEEDS_LEAF,
			.valid_props = AST_PROP_VALID_CDT_MODIFY),
	// insert — map CREATE_ONLY (NO_OVERWRITE preset) / list positional.
	N_CDT_OP(AST_PATH_FUNC_INSERT, "insert()",
			AST_NF_MODIFY | AST_NF_MOD_TARGET | AST_NF_NEEDS_LEAF,
			.valid_props = AST_PROP_VALID_CDT_MODIFY),
	// update — map UPDATE_ONLY (NO_CREATE preset); rejected on lists.
	N_CDT_OP(AST_PATH_FUNC_UPDATE, "update()",
			AST_NF_MODIFY | AST_NF_MOD_TARGET | AST_NF_NEEDS_LEAF,
			.valid_props = AST_PROP_VALID_CDT_MAP_MODIFY),
	N_CDT_OP(AST_PATH_FUNC_APPEND, "append()", AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_LIST_MODIFY),
	// Bulk *Items — one raw collection arg. appendItems/putItems/updateItems
	// are whole-collection (no leaf); insertItems takes a list index on a list
	// receiver and is whole-collection on a map.
	N_CDT_OP(AST_PATH_FUNC_APPEND_ITEMS, "appendItems()",
			AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_LIST_ITEMS),
	N_CDT_OP(AST_PATH_FUNC_INSERT_ITEMS, "insertItems()",
			AST_NF_MODIFY | AST_NF_MOD_TARGET,
			.valid_props = AST_PROP_VALID_CDT_ITEMS),
	N_CDT_OP(AST_PATH_FUNC_PUT_ITEMS, "putItems()",
			AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_MAP_ITEMS),
	N_CDT_OP(AST_PATH_FUNC_UPDATE_ITEMS, "updateItems()",
			AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_MAP_ITEMS),
	// add (identifier INCREMENT).
	N_CDT_OP(AST_PATH_FUNC_INCREMENT, "add()",
			AST_NF_MODIFY | AST_NF_MOD_TARGET | AST_NF_NEEDS_LEAF,
			.valid_props = AST_PROP_VALID_CDT_MODIFY),
	// No modify-flags slot on either container's wire op, so :NO_FAIL cannot
	// ride the op -- it rides the context list instead (ael_ctx_path_flags),
	// which is what makes an absent path a no-op rather than an error.
	N_CDT_OP(AST_PATH_FUNC_CLEAR, "clear()", AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_CLEAR),
	N_CDT_OP(AST_PATH_FUNC_SORT, "sort()", AST_NF_MODIFY | AST_NF_WHOLE_COLL,
			.valid_props = AST_PROP_VALID_CDT_SORT),

	//==========================================================
	// Path expression nodes (path-expressions.md).

	// Wildcard segment in a path. Multi-select with no leaf-consumption
	// shape — the SELECT op consumes the whole chain, so we don't
	// register cdt_get_op / cdt_remove_op here.
	N(AST_WILDCARD_SEG, "wildcard_seg", NK_SEG_M, .flags = AST_NF_BY_EXP),

	// AND_EXP post-filter seg `&[?(filter)]`. Same NK_SEG_M / by_exp_seg
	// layout as AST_WILDCARD_SEG; the AND_EXP flag distinguishes the wire
	// emission (AS_CDT_CTX_AND|AS_CDT_CTX_EXP).
	N(AST_AND_EXP_SEG, "and_exp_seg", NK_SEG_M, .flags = AST_NF_AND_EXP),

	// Loop variable: @, @key, @index. Leaf node — codegen emits
	// EXP_VAR_BUILTIN with the builtin idx and current rtype. The
	// valid_types mask is AUTO (any concrete type); @key's
	// AUTO_KEY restriction is enforced as an instance-level check
	// in ael_node_apply_postfix (the only node-type whose effective
	// mask depends on an instance field).
	N(AST_LOOP_VAR, "loop_var", NK_LEAF, .valid_types = AST_ETYPE_AUTO),

	// .select() / .modify() / wildcard-.remove() path functions —
	// distinct codegen path (emit_select_call) rather than the regular
	// path-call wrapper.
	// A select is a read, so it takes no tolerance flag.
	N(AST_PATH_FUNC_SELECT, "select()", NK_NONE),
	N(AST_PATH_FUNC_MODIFY, "modify()", NK_NONE,
			.valid_props = AST_PROP_VALID_SELECT_MODIFY),
	N(AST_PATH_FUNC_PSELECT_REMOVE, "remove()*", NK_NONE,
			.valid_props = AST_PROP_VALID_SELECT_MODIFY),

	// Resolved cdt_op (post-ael_cdt_op_from_fn morph). Carries no flags
	// since op_code/rtype/etype on the node now drive all dispatch.
	N_CDT_OP(AST_CDT_OP, "cdt_op", 0),

	// Resolved bit_op (post-ael_finalize_bit_call morph). Shares the
	// cdt_op u-shape; the wrapping AST_PATH_CALL holds the receiver in
	// u.call.ctx (single AST_ref, not an AST_PATH_CTX list).
	N_CDT_OP(AST_BIT_OP, "bit_op", 0),

	// Resolved hll_op (post-ael_finalize_hll_call morph). Same u.cdt_op
	// layout as AST_BIT_OP; the wrapping AST_PATH_CALL holds the
	// receiver in u.call.ctx.
	N_CDT_OP(AST_HLL_OP, "hll_op", 0),

	// Resolved str_op (post-ael_finalize_str_call morph). Same u.cdt_op
	// layout as AST_BIT_OP; op_code is an AS_STRING_OP_*, wire stype
	// EXP_CALL_STRING. Receiver held in u.call.ctx.
	N_CDT_OP(AST_STR_OP, "str_op", 0),

	//==========================================================
	// Internal linked-list nodes.

	N(AST_LET_LIST, "let_list", NK_LIST),
	N(AST_VAR_DEF, "var_def", NK_NONE),
	N(AST_LET_SCOPE, "let_scope", NK_NONE),
	N(AST_CASE, "case", NK_BINARY),

	// Transient function-call args — NK_NONE: never generically traversed;
	// the function-call resolver walks the chain and discards the wrappers.
	N(AST_ARG, "arg", NK_NONE),
	N(AST_ARG_LIST, "arg_list", NK_NONE),
	N(AST_REGEX_LIT, "regex_lit", NK_NONE),
};

#undef N
#undef N_CMD
#undef N_SEG_S
#undef N_SEG_M
#undef N_CDT_OP

//==========================================================
// Pool allocator.
//

void
ast_pool_init(ast_pool* pool)
{
	pool->free_head = AST_REF_NULL;
	pool->cur_offset = 0;
	pool->cur_sz = 0;
	// Not every pool user arms diagnostics - make "unarmed" a constructor
	// invariant instead of stack garbage every "diags != NULL" guard reads.
	pool->diags = NULL;
	dynmem_init(&pool->dm, (uint32_t)sizeof(ast_node), AST_POOL_DEFAULT_INIT,
			pool->stack_mem);
}

ast_ref
ast_pool_alloc(ast_pool* pool)
{
	ast_ref id;
	ast_node* n = NULL;

	if (pool->free_head != AST_REF_NULL) {
		id = pool->free_head;
		n = ast_pool_at(pool, id);
		pool->free_head = n->next;
	}
	else {
		if ((n = dynmem_reserve(&pool->dm, &id)) == NULL) {
			cf_crash(CF_MISC, "ast_pool: dynmem_reserve failed");
		}
	}

	// The union is fully zeroed — no variant's "empty" bit pattern leaks
	// into another variant's fields. Chain-bearing kinds get their
	// AST_REF_NULL head/tail in ast_new, keyed on the node kind.
	*n = (ast_node){ .next = AST_REF_NULL };

	return id;
}

void
ast_pool_release(ast_pool* pool, ast_ref r)
{
	if (r == AST_REF_NULL) {
		return;
	}

	ast_node* n = ast_pool_at(pool, r);

	cf_assert(! n->is_free, CF_MISC,
			"ast_pool_release: double release (ref %u)", r);

	n->is_free = true;
	n->next = pool->free_head;
	pool->free_head = r;
}

void
ast_pool_destroy(ast_pool* pool)
{
	dynmem_destroy(&pool->dm);
}

static ast_ref
ast_pool_next_ref(const ast_pool* pool, ast_ref ref)
{
	const ast_node* n = (const ast_node*)dynmem_get(&pool->dm, ref);

	cf_assert(n != NULL, CF_MISC, "ast_pool_next_ref: bad ref %u", ref);

	return n->next;
}

ast_ref
ast_nmath_head(ast_pool* pool, const ast_node* np)
{
	return ast_pool_at(pool, np->u.nmath.tail)->next;
}

// Set the parent ref on a child node, if it tracks parents.
static void
ast_set_parent(ast_pool* pool, ast_ref child, ast_ref parent)
{
	if (child == AST_REF_NULL) {
		return;
	}

	ast_node* cp = ast_pool_at(pool, child);

	switch (cp->type) {
	case AST_BIN:
		cp->u.bin.parent = parent;
		break;
	case AST_BIN_REF:
		cp->u.bin_ref.parent = parent;
		break;
	case AST_VAR:
		cp->u.var.parent = parent;
		break;
	default:
		switch (ast_node_table[cp->type].kind) {
		case NK_NMATH:
			cp->u.nmath.parent = parent;
			break;
		case NK_BMATH:
			cp->u.binary.parent = parent;
			break;
		default:
			break;
		}
		break;
	}
}

//==========================================================
// Node construction.
//

ast_ref
ast_new(ast_pool* pool, ast_node_t type)
{
	ast_ref r = ast_pool_alloc(pool);
	ast_node* n = ast_pool_at(pool, r);
	n->type = type;
	n->etype = AST_ETYPE_AUTO;
	n->offset = pool->cur_offset;
	n->sz = pool->cur_sz;

	// Chain-bearing kinds start with an empty chain. Only refs that mean
	// "no node" need AST_REF_NULL (0 is a valid pool index); every other
	// variant field starts zeroed from ast_pool_alloc, and constructors
	// set what they use.
	switch (ast_node_table[type].kind) {
	case NK_LIST:
		n->u.list.head = AST_REF_NULL;
		n->u.list.tail = AST_REF_NULL;
		break;
	case NK_CDT_OP:
		n->u.cdt_op.head = AST_REF_NULL;
		n->u.cdt_op.tail = AST_REF_NULL;
		break;
	default:
		if (type == AST_PATH_CTX) {
			n->u.ctx_list.head = AST_REF_NULL;
			n->u.ctx_list.tail = AST_REF_NULL;
		}
		break;
	}

	return r;
}

ast_ref
ast_new_nil(ast_pool* pool)
{
	ast_ref r = ast_new(pool, AST_NIL);
	ast_pool_at(pool, r)->etype = AST_ETYPE_NIL;
	return r;
}

ast_ref
ast_new_inf(ast_pool* pool)
{
	ast_ref r = ast_new(pool, AST_INF);
	// INF intersects with any storage type in CDT compare context.
	ast_pool_at(pool, r)->etype = AST_ETYPE_AUTO;
	return r;
}

ast_ref
ast_new_wildcard(ast_pool* pool)
{
	ast_ref r = ast_new(pool, AST_WILDCARD);
	// WILDCARD is non-storage and only valid as a list element; etype
	// stays AUTO so it intersects in any list-element context.
	ast_pool_at(pool, r)->etype = AST_ETYPE_AUTO;
	return r;
}

ast_ref
ast_new_int(ast_pool* pool, int64_t val)
{
	ast_ref r = ast_new(pool, AST_INT);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_INT;
	n->u.ival = val;
	return r;
}

ast_ref
ast_new_float(ast_pool* pool, double val)
{
	ast_ref r = ast_new(pool, AST_FLOAT);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_FLOAT;
	n->u.fval = val;
	return r;
}

// Synthesized numeric zero for unary minus. Left AUTO_NUMERIC so the
// wrapping SUB resolves it to the operand's INT/FLOAT; emitted as 0 / 0.0.
static ast_ref
ast_new_zero(ast_pool* pool)
{
	ast_ref r = ast_new(pool, AST_ZERO);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_AUTO_NUMERIC;
	return r;
}

ast_ref
ast_new_neg(ael_context* ctx, ast_ref operand, uint32_t minus_end)
{
	ast_ref zero = ast_new_zero(ctx->pool);

	// Stamp the synthesized zero one past the '-' (sz 0) - ast_new's default
	// stamp is the lookahead past the whole -x, which would drag the SUB's
	// child-derived span (ast_span_children) past the construct. One-past-'-'
	// keeps the union anchored where the operand begins.
	ast_set_span(ctx->pool, zero, minus_end, 0);

	// -x == 0 - x; the zero adopts the operand's numeric type via the SUB,
	// so INT and FLOAT both negate (there is no wire negate op).
	return ast_new_bmath(ctx, AST_SUB, zero, operand, AST_ETYPE_AUTO_NUMERIC);
}

ast_ref
ast_new_bool(ast_pool* pool, bool val)
{
	ast_ref r = ast_new(pool, AST_BOOL);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_TRILEAN;
	n->u.bval = val;
	return r;
}

ast_ref
ast_new_string(ast_pool* pool, const char* s, uint32_t sz, bool has_escape)
{
	ast_ref r = ast_new(pool, AST_STRING);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_STR;
	n->u.str.str = s;
	n->u.str.sz = sz;
	n->u.str.has_escape = has_escape;
	return r;
}

// Parser-side wrapper: validates AEL escape sequences in the token
// body before constructing the AST_STRING. On invalid escape, emits a
// parse-time diag pinned at the offending backslash and returns an
// AST_UNKNOWN stand-in so the rest of the parse can proceed without
// NULL-deref. The body bytes still alias the source buffer — decoding is
// deferred to codegen. The has_escape flag is captured here so codegen can skip
// a rescan for the no-escape common case.
ast_ref
ast_new_string_token(ael_context* ctx, uint32_t name_offset, uint32_t name_sz)
{
	const char* s = ctx->input + name_offset;
	bool has_escape = false;
	uint32_t bad_offset;
	const char* err;

	if (! ael_string_validate(s, name_sz, &has_escape, &bad_offset, &err)) {
		ael_err(ctx, name_offset + bad_offset, 2, err);
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	return ast_new_string(ctx->pool, s, name_sz, has_escape);
}

ast_ref
ast_new_blob_from_hex(ast_pool* pool, const char* hex, uint32_t hex_sz)
{
	ast_ref r = ast_new(pool, AST_BLOB);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_BLOB;
	n->u.str.str = hex;
	n->u.str.sz = hex_sz;
	return r;
}

ast_ref
ast_new_blob_from_b64(ast_pool* pool, const char* b64, uint32_t b64_sz)
{
	ast_ref r = ast_new(pool, AST_B64_BLOB);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_BLOB;
	n->u.str.str = b64;
	n->u.str.sz = b64_sz;
	return r;
}

// Shallow clone of trivial literal / bin-ref nodes. Strings and blobs
// share the input buffer (which outlives the pool), so aliasing the
// .str pointer is safe. Returns AST_REF_NULL for anything that would
// require deep cloning (operators, function calls, bins with state,
// ctx_lists, etc.).
ast_ref
ast_clone_simple(ast_pool* pool, ast_ref src)
{
	if (src == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* sp = ast_pool_at(pool, src);

	switch (sp->type) {
	case AST_INT:
		return ast_new_int(pool, sp->u.ival);
	case AST_FLOAT:
		return ast_new_float(pool, sp->u.fval);
	case AST_BOOL:
		return ast_new_bool(pool, sp->u.bval);
	case AST_NIL:
		return ast_new_nil(pool);
	case AST_STRING:
		return ast_new_string(pool, sp->u.str.str, sp->u.str.sz,
				sp->u.str.has_escape);
	case AST_BLOB:
		return ast_new_blob_from_hex(pool, sp->u.str.str, sp->u.str.sz);
	case AST_B64_BLOB:
		return ast_new_blob_from_b64(pool, sp->u.str.str, sp->u.str.sz);
	default:
		return AST_REF_NULL;
	}
}

// Validate a bin-name token's body. Thin wrapper around the server's
// canonical validator (also used at runtime by build_bin /
// build_bin_meta in exp.c) — keeping a single source of truth for the
// bin-name rules: ≤15 bytes, no NULs. Empty names are allowed.
// Emits a granular diag identifying which constraint failed. Filter-
// depth gating is the caller's responsibility — it has a different
// fallback (AST_UNKNOWN, not AST_REF_NULL).
static bool
bin_name_check(ael_context* ctx, uint32_t name_offset, uint32_t name_sz)
{
	const uint8_t* name = (const uint8_t*)(ctx->input + name_offset);

	if (as_bin_name_check(name, name_sz)) {
		return true;
	}

	if (name_sz >= AS_BIN_NAME_MAX_SZ) {
		ael_err(ctx, name_offset, name_sz,
				"bin name too long (max 15 characters)");
	}
	else {
		ael_err(ctx, name_offset, name_sz, "bin name cannot contain NUL bytes");
	}

	return false;
}

ast_ref
ast_new_bin(ael_context* ctx, uint32_t ref_offset, uint32_t ref_end,
		uint32_t name_offset, uint32_t name_sz)
{
	ast_pool* pool = ctx->pool;
	const char* name = ctx->input + name_offset;
	uint32_t ref_sz = ref_end > ref_offset ? ref_end - ref_offset : name_sz;

	if (ctx->filter_depth > 0) {
		ael_err(ctx, name_offset, name_sz,
				"$.bin not allowed inside filter / modify body");
		return ast_new(pool, AST_UNKNOWN);
	}

	if (! bin_name_check(ctx, name_offset, name_sz)) {
		return AST_REF_NULL;
	}

	// Look for an existing canonical bin with this name.
	for (ast_ref br = ctx->bin_root; br != AST_REF_NULL;) {
		ast_node* bp = ast_pool_at(pool, br);

		if (bp->u.bin.name_sz == name_sz &&
				memcmp(ctx->input + bp->offset, name, name_sz) == 0) {
			// Found — create a bin_ref.
			ast_ref r = ast_new(pool, AST_BIN_REF);
			ast_node* n = ast_pool_at(pool, r);
			// Anchor on the name; the display span reaches back over '$.'.
			ast_set_span(pool, r, name_offset, name_sz);
			ast_set_display_span(pool, r, ref_offset, ref_offset + ref_sz);
			n->has_deferable = true;
			n->u.bin_ref.bin = br;
			n->u.bin_ref.parent = AST_REF_NULL;
			n->etype = bp->etype;

			// Only chain if type is still unresolved.
			if (ast_type_resolved(bp->etype)) {
				n->u.bin_ref.ref_next = AST_REF_NULL;
			}
			else {
				n->u.bin_ref.ref_next = bp->u.bin.ref_root;
				bp->u.bin.ref_root = r;
			}

			return r;
		}

		br = bp->u.bin.bin_next;
	}

	// First occurrence — create canonical bin node.
	ast_ref r = ast_new(pool, AST_BIN);
	ast_node* n = ast_pool_at(pool, r);
	// Anchor on the name; the display span reaches back over '$.'.
	ast_set_span(pool, r, name_offset, name_sz);
	ast_set_display_span(pool, r, ref_offset, ref_offset + ref_sz);
	n->has_deferable = true;
	n->u.bin.name_sz = name_sz;
	n->u.bin.bin_next = ctx->bin_root;
	n->u.bin.parent = AST_REF_NULL;
	n->u.bin.ref_root = AST_REF_NULL;
	ctx->bin_root = r;
	return r;
}

ast_ref
ast_new_local_bin(ael_context* ctx, uint32_t ref_offset, uint32_t ref_end,
		uint32_t name_offset, uint32_t name_sz, ast_etype etype)
{
	uint32_t ref_sz = ref_end > ref_offset ? ref_end - ref_offset : name_sz;

	if (ctx->filter_depth > 0) {
		ael_err(ctx, name_offset, name_sz,
				"$.bin not allowed inside filter / modify body");
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	if (! bin_name_check(ctx, name_offset, name_sz)) {
		return AST_REF_NULL;
	}

	// "Local" is encoded structurally — the bin is kept off ctx->bin_root so it
	// doesn't participate in cross-occurrence type narrowing, but it is threaded
	// onto ctx->local_bin_root so build_internal_ael still gives it a runtime
	// bin-table slot (the wire path registers every bin op likewise). No header
	// flag needed.
	ast_ref r = ast_new(ctx->pool, AST_BIN);
	ast_node* n = ast_pool_at(ctx->pool, r);
	// Anchor on the name; the display span reaches back over '$.'.
	ast_set_span(ctx->pool, r, name_offset, name_sz);
	ast_set_display_span(ctx->pool, r, ref_offset, ref_offset + ref_sz);
	n->etype = etype;
	n->u.bin.name_sz = name_sz;
	n->u.bin.bin_next = ctx->local_bin_root;
	n->u.bin.parent = AST_REF_NULL;
	n->u.bin.ref_root = AST_REF_NULL;
	ctx->local_bin_root = r;
	return r;
}

// Record a "<what>: <A> vs <B>" diagnostic naming both etypes. Centralizes the
// two-buffer ast_etype_str formatting the type-conflict sites share.
void
ast_diag_type_conflict(ael_context* ctx, uint32_t offset, uint32_t byte_sz,
		const char* what, ast_etype a, ast_etype b)
{
	char a_buf[AEL_ETYPE_STR_SZ];
	char b_buf[AEL_ETYPE_STR_SZ];

	ael_errf(ctx, offset, byte_sz, "%s: %s vs %s", what,
			ast_etype_str(a, a_buf, sizeof a_buf),
			ast_etype_str(b, b_buf, sizeof b_buf));
}

// Source span covering two operands, [min-start, max-end). For diagnostics
// that fault a binary relation as a whole -- neither operand alone is "the"
// culprit (e.g. two incomparable types). Never zero-width when both operands
// carry a real span, so it beats anchoring on ctx->last_token_offset, which is
// the lookahead and collapses to EOF when the relation ends the source.
static void
ast_operand_pair_span(const ast_node* lp, const ast_node* rp, uint32_t* offset,
		uint32_t* byte_sz)
{
	uint32_t l_start = ast_disp_offset(lp);
	uint32_t r_start = ast_disp_offset(rp);
	uint32_t l_end = l_start + lp->sz;
	uint32_t r_end = r_start + rp->sz;
	uint32_t start = l_start < r_start ? l_start : r_start;
	uint32_t end = l_end > r_end ? l_end : r_end;

	*offset = start;
	*byte_sz = end - start;
}

// Report an operator operand type conflict. Anchors at the OFFENDING operand's
// own span and names the operator and the type it requires -- not
// ctx->last_token_offset, which is the parser's lookahead token and, for a
// reduce forced by a lower-precedence operator, points one token PAST the
// culprit (e.g. `<string> and <bool>` caret-ed under a trailing `or`). Falls
// back to "type mismatch: A vs B" (at the right operand) when both operands
// satisfy the operator individually but not each other.
static void
ast_diag_operand_conflict(ael_context* ctx, ast_node_t op, ast_etype required,
		const ast_node* n_left, const ast_node* n_right)
{
	const ast_node* bad = NULL;

	if ((required & n_left->etype) == AST_ETYPE_ERROR) {
		bad = n_left;
	}
	else if ((required & n_right->etype) == AST_ETYPE_ERROR) {
		bad = n_right;
	}

	if (bad == NULL) {
		ast_diag_type_conflict(ctx, ast_disp_offset(n_right), n_right->sz,
				"type mismatch", n_left->etype, n_right->etype);
		return;
	}

	char req_buf[AEL_ETYPE_STR_SZ];
	char got_buf[AEL_ETYPE_STR_SZ];

	ael_errf(ctx, ast_disp_offset(bad), bad->sz,
			"operand of '%s' must be %s, got %s", ast_node_table[op].name,
			ast_etype_str(required, req_buf, sizeof req_buf),
			ast_etype_str(bad->etype, got_buf, sizeof got_buf));
}

ast_ref
ast_new_bcmp(ael_context* ctx, ast_node_t ntype, ast_ref left, ast_ref right)
{
	// A failed operand sub-parse reduces here as AST_REF_NULL (the
	// diagnostic is already recorded); don't dereference it.
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_pool* pool = ctx->pool;
	ast_node* lp = ast_pool_at(pool, left);
	ast_node* rp = ast_pool_at(pool, right);

	// Both checks apply only when neither side can still be narrowed by
	// downstream context (no deferable bins / vars).
	if (! lp->has_deferable && ! rp->has_deferable) {
		if (! ast_type_resolved(lp->etype) && ! ast_type_resolved(rp->etype)) {
			uint32_t offset;
			uint32_t byte_sz;

			ast_operand_pair_span(lp, rp, &offset, &byte_sz);
			ael_err(ctx, offset, byte_sz, "cannot infer types for comparison");
			return AST_REF_NULL;
		}

		// Operand etypes must have at least one type in common — otherwise
		// the comparison can never succeed. Fully unresolved (AUTO)
		// intersects to AUTO ≠ ERROR, so this only fires for genuinely
		// incompatible types (e.g. INT vs NIL literals).
		if ((lp->etype & rp->etype) == AST_ETYPE_ERROR) {
			uint32_t offset;
			uint32_t byte_sz;

			ast_operand_pair_span(lp, rp, &offset, &byte_sz);
			ast_diag_type_conflict(ctx, offset, byte_sz, "cannot compare",
					lp->etype, rp->etype);
			return AST_REF_NULL;
		}
	}

	ast_ref r = ast_new_binary(pool, ntype, left, right);
	ast_node* n = ast_pool_at(pool, r);
	n->etype = AST_ETYPE_TRILEAN;

	// Cross-infer: if one side is resolved, narrow the other.
	// If both unresolved with deferables, back-propagation may resolve.
	ast_set_implicit_type(ctx, left, rp->etype);
	ast_set_implicit_type(ctx, right, lp->etype);
	return r;
}

ast_ref
ast_new_in(ael_context* ctx, ast_ref left, ast_ref right)
{
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_pool* pool = ctx->pool;

	// RHS must be a list (literal, bin, or runtime CDT result).
	ast_set_implicit_type(ctx, right, AST_ETYPE_LIST);

	// `in` matches by value, so the operand needn't share the list's
	// element types. When untyped, and the RHS is a list literal, take the
	// first element's type (mixed lists ok). A typed operand stays as-is;
	// an unresolved one is caught by the post-parse strict-typing pass.
	ast_node* lp = ast_pool_at(pool, left);

	if (! ast_type_resolved(lp->etype)) {
		ast_node* rp = ast_pool_at(pool, right);

		if (rp->type == AST_LIST && rp->u.list.head != AST_REF_NULL) {
			ast_etype head = ast_pool_at(pool, rp->u.list.head)->etype;

			if (ast_type_resolved(head)) {
				ast_set_implicit_type(ctx, left, head);
			}
		}
	}

	// Both operands were null-checked above, so ast_new_binary can't fail.
	ast_ref r = ast_new_binary(pool, AST_CMP_IN, left, right);

	ast_pool_at(pool, r)->etype = AST_ETYPE_TRILEAN;
	return r;
}

// Set R's source span to cover its (non-null) children -- [min offset .. max
// end) -- overriding ast_new's lookahead-based default so a composite node
// points at the operands it was built from. Children carry precise spans (leaf
// tokens stamped via STAMP; inner composites spanned recursively). Pass
// AST_REF_NULL for absent children (e.g. an open range's missing bound); if all
// are null the default span is left in place.
static void
ast_span_children(ast_pool* pool, ast_ref ref, ast_ref a, ast_ref b, ast_ref c)
{
	ast_ref kids[3] = { a, b, c };
	uint32_t start = UINT32_MAX;
	uint32_t end = 0;

	for (uint32_t i = 0; i < 3; i++) {
		if (kids[i] == AST_REF_NULL) {
			continue;
		}

		const ast_node* k = ast_pool_at(pool, kids[i]);
		uint32_t k_start = ast_disp_offset(k);

		if (k_start < start) {
			start = k_start;
		}

		if (k_start + k->sz > end) {
			end = k_start + k->sz;
		}
	}

	if (start == UINT32_MAX) {
		return;
	}

	ast_set_span(pool, ref, start, end - start);
}

ast_ref
ast_new_bmath(ael_context* ctx, ast_node_t type, ast_ref left, ast_ref right,
		ast_etype operand_etype)
{
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref r = ast_new_binary(ctx->pool, type, left, right);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->etype = ast_set_implicit_type_lr(ctx, type, left, right, operand_etype);
	return r;
}

ast_ref
ast_new_binary(ast_pool* pool, ast_node_t type, ast_ref left, ast_ref right)
{
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref r = ast_new(pool, type);
	ast_node* n = ast_pool_at(pool, r);
	n->u.binary.left = left;
	n->u.binary.right = right;
	n->u.binary.parent = AST_REF_NULL;
	n->has_deferable = ast_pool_at(pool, left)->has_deferable ||
			ast_pool_at(pool, right)->has_deferable;
	ast_set_parent(pool, left, r);
	ast_set_parent(pool, right, r);
	ast_span_children(pool, r, left, right, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_range_seg(ast_pool* pool, ast_node_t type, ast_ref start, ast_ref end,
		bool inverted)
{
	ast_ref r = ast_new(pool, type);
	ast_node* n = ast_pool_at(pool, r);
	n->u.range_seg.start = start;
	n->u.range_seg.end = end;
	n->u.range_seg.inverted = inverted;
	ast_span_children(pool, r, start, end, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_rel_range_seg(ast_pool* pool, ast_node_t type, ast_ref start,
		ast_ref end, ast_ref relative_to, bool inverted)
{
	ast_ref r = ast_new(pool, type);
	ast_node* n = ast_pool_at(pool, r);
	n->u.rel_range_seg.start = start;
	n->u.rel_range_seg.end = end;
	n->u.rel_range_seg.relative_to = relative_to;
	n->u.rel_range_seg.inverted = inverted;
	ast_span_children(pool, r, start, end, relative_to);
	return r;
}

static ast_ref
ast_new_nary(ael_context* ctx, ast_node_t type, ast_ref left, ast_ref right,
		ast_etype etype)
{
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref r = ast_new(ctx->pool, type);
	ast_node* n_r = ast_pool_at(ctx->pool, r);
	ast_node* n_left = ast_pool_at(ctx->pool, left);
	ast_node* n_right = ast_pool_at(ctx->pool, right);

	// Circular: left->next = right, right->next = left (tail->next == head).
	n_left->next = right;
	n_right->next = left;
	n_r->u.nmath.tail = right;
	n_r->u.nmath.count = 2;
	n_r->u.nmath.parent = AST_REF_NULL;
	n_r->has_deferable = n_left->has_deferable || n_right->has_deferable;
	ast_set_parent(ctx->pool, left, r);
	ast_set_parent(ctx->pool, right, r);
	n_r->etype = ast_set_implicit_type_lr(ctx, type, left, right, etype);
	ast_span_children(ctx->pool, r, left, right, AST_REF_NULL);

	return r;
}

ast_ref
ast_new_unary(ast_pool* pool, ast_node_t type, ast_ref operand)
{
	if (operand == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref r = ast_new(pool, type);
	ast_node* n = ast_pool_at(pool, r);
	n->u.unary.operand = operand;
	n->has_deferable = ast_pool_at(pool, operand)->has_deferable;
	ast_span_children(pool, r, operand, AST_REF_NULL, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_func1(ael_context* ctx, ast_node_t type, ast_ref arg, ast_etype etype)
{
	ast_set_implicit_type(ctx, arg, etype);
	ast_ref r = ast_new(ctx->pool, type);
	ast_node* n = ast_pool_at(ctx->pool, r);
	n->u.func1.arg = arg;
	n->etype = etype;
	ast_span_children(ctx->pool, r, arg, AST_REF_NULL, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_func2(ael_context* ctx, ast_node_t type, ast_ref a, ast_ref b,
		ast_etype a_etype, ast_etype b_etype, ast_etype result_etype)
{
	ast_set_implicit_type(ctx, a, a_etype);
	ast_set_implicit_type(ctx, b, b_etype);
	ast_ref r = ast_new(ctx->pool, type);
	ast_node* n = ast_pool_at(ctx->pool, r);
	n->u.func2.arg1 = a;
	n->u.func2.arg2 = b;
	n->etype = result_etype;
	ast_span_children(ctx->pool, r, a, b, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_meta(ast_pool* pool, exp_op_code op_code, int64_t param)
{
	ast_ref r = ast_new(pool, AST_META);
	ast_node* n = ast_pool_at(pool, r);

	if (op_code == EXP_REC_KEY) {
		// param is an ast_etype bitmask — may be unresolved
		// (AST_ETYPE_AUTO_KEY) when the spelling is bare `$.key()`, or
		// a single bit when `$.key():T`. Inference narrows etype
		// between parse and codegen; both emitters derive the wire rtype
		// from `etype` (ast_rec_key_rtype). The wire type does NOT live
		// in u.meta.param — the caller repurposes param as the
		// ctx->key_meta_root chain link. Seed it AST_REF_NULL here.
		n->etype = (ast_etype)param;
		n->u.meta.param = (int64_t)AST_REF_NULL;
	}
	else {
		n->etype = ast_rtype_to_etype(exp_op_table[op_code].r_type);
		n->u.meta.param = param;
	}

	n->u.meta.op_code = op_code;
	return r;
}

ast_ref
ast_new_var_ref(ael_context* ctx, uint32_t name_offset, uint32_t name_sz)
{
	const char* name = ctx->input + name_offset;

	if (ctx->filter_depth > 0) {
		// `with(...)` is rejected inside filter / modify, so any ${x}
		// here would reference an outer-scope let-var. Sub-programs are
		// separate as_exp blobs with their own var-slot space, so outer
		// slots can't be addressed at runtime.
		ael_err(ctx, name_offset, name_sz,
				"${var} not allowed inside filter / modify body");
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	ael_var_lookup v = ael_find_var(ctx, name, name_sz);

	if (! v.found) {
		ael_err(ctx, name_offset, name_sz, "undefined variable");
		// Sentinel UNKNOWN so the rest of the parse-time reduction
		// chain has a non-NULL operand to walk; the diagnostic stops
		// the build before codegen.
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	ast_ref r = ast_new(ctx->pool, AST_VAR);
	ast_node* n = ast_pool_at(ctx->pool, r);
	// The node anchors on the name -- codegen emits the variable name from
	// node.offset, so a '${' prefix or an enclosing paren may only ever widen
	// this node's span via disp_pre, never move it.
	ast_set_span(ctx->pool, r, name_offset, name_sz);
	n->u.var.var_idx = v.var_idx;
	n->u.var.name_sz = name_sz;
	n->u.var.parent = AST_REF_NULL;
	n->etype = v.etype;
	n->has_deferable = ! ast_type_resolved(v.etype);

	// Push onto the def's reference chain so a later narrowing of the
	// def's value reaches this reference (and climbs its parents).
	ast_node* dp = ast_pool_at(ctx->pool, v.def);
	n->u.var.ref_next = dp->u.var_def.ref_root;
	dp->u.var_def.ref_root = r;

	return r;
}

ast_ref
ast_new_var_def(ael_context* ctx, uint32_t name_offset, uint32_t name_sz,
		ast_ref value)
{
	if (value == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	const char* name = ctx->input + name_offset;

	if (ctx->filter_depth > 0) {
		ael_err(ctx, name_offset, name_sz,
				"with(...) not allowed inside filter / modify body");
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	if (name_sz > AEL_VAR_NAME_MAX) {
		ael_errf(ctx, name_offset, name_sz,
				"variable name too long (max %u bytes)", AEL_VAR_NAME_MAX);
		return AST_REF_NULL;
	}

	if (ctx->scope_idx >= AEL_VAR_MAX) {
		ael_errf(ctx, name_offset, name_sz, "too many variables (max %u)",
				AEL_VAR_MAX);
		return AST_REF_NULL;
	}

	// Check for duplicate name in current scope.
	if (ael_find_var(ctx, name, name_sz).found) {
		ael_err(ctx, name_offset, name_sz, "duplicate variable name");
		return AST_REF_NULL;
	}

	// Value type must be resolved unless it has deferable nodes (bins/vars)
	// that may be resolved later via back-propagation.
	ast_node* vp = ast_pool_at(ctx->pool, value);

	if (! ast_type_resolved(vp->etype) && ! vp->has_deferable) {
		ael_err(ctx, name_offset, name_sz,
				"cannot infer type for variable definition");
		return AST_REF_NULL;
	}

	ast_ref r = ast_new(ctx->pool, AST_VAR_DEF);
	ast_node* dp = ast_pool_at(ctx->pool, r);
	dp->u.var_def.name_sz = name_sz;
	dp->u.var_def.value = value;
	dp->u.var_def.ref_root = AST_REF_NULL;
	dp->next = AST_REF_NULL;
	ctx->var_by_idx[ctx->scope_idx] = r;

	// Parent the value to this def so a narrowing that starts inside the
	// value (e.g. its bin pinned by another occurrence) climbs here and
	// fans out to the ${name} references — see ast_climb_and_propagate.
	ast_set_parent(ctx->pool, value, r);

	// Span over `name = value` so a parent that folds this node (via
	// ast_span_children) gets a precise caret, not the parser lookahead. The
	// node anchors on the name, so name_offset is both the semantic locator
	// and the display start -- no disp_pre reach-back. That offset also serves
	// as the def's name position (read by ael_find_var and the emitters —
	// there is no u.var_def.name_offset).
	const ast_node* val_node = ast_pool_at(ctx->pool, value);
	uint32_t val_end = ast_disp_offset(val_node) + val_node->sz;
	ast_set_span(ctx->pool, r, name_offset,
			val_end > name_offset ? val_end - name_offset : name_sz);

	ctx->scope_idx++;
	return r;
}

ast_ref
ast_new_let_scope(ael_context* ctx, uint32_t name_offset, uint32_t name_sz,
		ast_ref value)
{
	if (ctx->filter_depth > 0) {
		ael_err(ctx, name_offset, name_sz,
				"with(...) not allowed inside filter / modify body");
		return ast_new(ctx->pool, AST_UNKNOWN);
	}

	ast_ref def = ast_new_var_def(ctx, name_offset, name_sz, value);

	if (def == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	// Create let_scope at head.
	ast_ref scope_ref = ast_new(ctx->pool, AST_LET_SCOPE);
	ast_node* sp = ast_pool_at(ctx->pool, scope_ref);
	sp->u.let_scope.parent = ctx->cur_scope;
	sp->u.let_scope.start_idx = ctx->scope_idx - 1; // var_def already incremented

	// Build list: [let_scope, var_def].
	sp->next = def;
	ast_ref r = ast_new(ctx->pool, AST_LET_LIST);
	ast_node* n = ast_pool_at(ctx->pool, r);
	n->u.list.head = scope_ref;
	n->u.list.tail = def;
	n->u.list.count = 2;

	// Span the let-list over the binding (def); scope_ref is a synthetic
	// marker with no source span.
	ast_span_children(ctx->pool, r, def, AST_REF_NULL, AST_REF_NULL);

	// Push scope.
	ctx->cur_scope = r;
	return r;
}

ast_ref
ast_new_path_ctx_bin(ast_pool* pool, ast_ref bin)
{
	// Callers guard the base; a null here is a programmer error.
	cf_assert(bin != AST_REF_NULL, CF_MISC, "ast_new_path_ctx_bin: null bin");

	ast_ref r = ast_new(pool, AST_PATH_CTX);
	ast_node* n = ast_pool_at(pool, r);

	ast_pool_at(pool, bin)->next = AST_REF_NULL;
	n->u.ctx_list.head = bin;
	n->u.ctx_list.tail = bin;
	n->u.ctx_list.count = 1;
	n->u.ctx_list.is_multi = false;
	n->u.ctx_list.last_is_multi = false;

	// Span over the bin (head == tail == bin), not the parser lookahead.
	ast_span_children(pool, r, bin, AST_REF_NULL, AST_REF_NULL);
	return r;
}

ast_ref
ast_new_loop_var(ast_pool* pool, as_exp_builtin builtin)
{
	ast_ref r = ast_new(pool, AST_LOOP_VAR);
	ast_node* n = ast_pool_at(pool, r);

	n->u.loop_var.builtin = builtin;
	// @index never chains (always pinned INT), so make its "no chain"
	// explicit rather than relying on the alloc fill.
	n->u.loop_var.next_same_kind = AST_REF_NULL;

	switch (builtin) {
	case AS_EXP_BUILTIN_INDEX:
		n->etype = AST_ETYPE_INT;
		break;
	case AS_EXP_BUILTIN_KEY:
		// Map keys can be int, str, or blob — let comparison / cast
		// narrow. Codegen falls back to STR if unresolved at emit.
		n->etype = AST_ETYPE_AUTO_KEY;
		break;
	default: // AS_EXP_BUILTIN_VALUE
		// Element values can be any type; let comparison context narrow.
		n->etype = AST_ETYPE_AUTO;
		break;
	}

	return r;
}

// Build an NK_CDT_OP node. The node's type IS the path-function kind
// (AST_PATH_FUNC_GET / EXISTS / COUNT / REMOVE / SET / INSERT / INCREMENT /
// APPEND / CLEAR / SORT). op_code stays 0 (resolved at the call combine
// step from ctx_list + leaf seg context); the wire payload chain starts
// empty — finalize_cdt_op pushes ret_type, leaf_seg, and/or value onto it
// per the path-func's wire shape.
ast_ref
ast_new_cdt_op(ast_pool* pool, ast_node_t pf_type)
{
	// op_code / is_modify / leaf_consumed / props start zeroed from the
	// pool; the chain head/tail are nulled by ast_new's NK_CDT_OP arm.
	return ast_new(pool, pf_type);
}

ast_ref
ast_map_literal_next_key(ast_pool* pool, ast_ref key)
{
	ast_ref val = ast_pool_next_ref(pool, key);

	cf_assert(val != AST_REF_NULL, CF_MISC,
			"ast_map_literal_next_key: map key has no value");

	return ast_pool_next_ref(pool, val);
}

//==========================================================
// Child iteration.
//

// Walk a counted sibling chain. The NULL check is belt-and-braces against a
// count that disagrees with the chain: AST_REF_NULL is out of the pool's index
// range, so running past the end would fault rather than stop.
static void
ast_foreach_chain(ast_pool* pool, ast_ref head, uint32_t count, ast_child_cb cb,
		void* arg)
{
	ast_ref ele = head;

	for (uint32_t i = 0; i < count && ele != AST_REF_NULL; i++) {
		ast_ref next = ast_pool_at(pool, ele)->next;

		cb(pool, ele, arg);
		ele = next;
	}
}

void
ast_children_foreach(ast_pool* pool, ast_ref ref, ast_child_cb cb, void* arg)
{
	if (ref == AST_REF_NULL) {
		return;
	}

	ast_node* np = ast_pool_at(pool, ref);

	// Types whose child layout 'kind' cannot imply - these are the only ones
	// that need naming. Everything else falls through to the kind dispatch.
	switch (np->type) {
	case AST_MAP: {
		// Interleaved key->val->key->val chain.
		ast_ref key = np->u.list.head;

		for (uint32_t i = 0; i < np->u.list.count && key != AST_REF_NULL; i++) {
			ast_ref val = ast_pool_at(pool, key)->next;

			cf_assert(val != AST_REF_NULL, CF_MISC,
					"ast_children_foreach: map literal missing value");

			ast_ref next_key = ast_pool_at(pool, val)->next;

			cb(pool, key, arg);
			cb(pool, val, arg);
			key = next_key;
		}

		return;
	}

	case AST_VAR_DEF:
		cb(pool, np->u.var_def.value, arg);
		return;

	case AST_PATH_CALL:
		cb(pool, np->u.call.ctx, arg);
		cb(pool, np->u.call.call_op, arg);
		return;

	case AST_PATH_FUNC_SELECT:
	case AST_PATH_FUNC_MODIFY:
	case AST_PATH_FUNC_PSELECT_REMOVE:
		// SELECT: value always NULL. MODIFY: value is the apply expr.
		// PSELECT_REMOVE: value always NULL (codegen synthesizes blob).
		cb(pool, np->u.modify.value, arg);
		return;

	case AST_PATH_CTX:
		ast_foreach_chain(pool, np->u.ctx_list.head, np->u.ctx_list.count, cb,
				arg);
		return;

	default:
		break;
	}

	switch (ast_node_table[np->type].kind) {
	case NK_UNARY:
		// u.func1.arg aliases u.unary.operand.
		cb(pool, np->u.unary.operand, arg);
		break;
	case NK_SEG_S:
		cb(pool, ast_seg_operand(np), arg);
		break;
	case NK_BINARY:
	case NK_BMATH:
		// u.func2.arg1/arg2 and u.when_case.cond/result alias left/right.
		cb(pool, np->u.binary.left, arg);
		cb(pool, np->u.binary.right, arg);
		break;
	case NK_SEG_M: {
		uint32_t flags = ast_node_table[np->type].flags;

		// Flag order matches ast_seg_inverted: by-exp, then rel-range, then
		// list, then plain range.
		if ((flags & (AST_NF_BY_EXP | AST_NF_AND_EXP)) != 0) {
			cb(pool, np->u.by_exp_seg.filter, arg);
			break;
		}

		if ((flags & AST_NF_REL_RANGE) != 0) {
			cb(pool, np->u.rel_range_seg.start, arg);
			cb(pool, np->u.rel_range_seg.end, arg);
			// The anchor is a child too. The hand-written walks all missed it,
			// benignly - the grammar admits only literals here today.
			cb(pool, np->u.rel_range_seg.relative_to, arg);
			break;
		}

		if ((flags & AST_NF_LIST_SEG) != 0) {
			ast_foreach_chain(pool, np->u.list_seg.head, np->u.list_seg.count,
					cb, arg);
			break;
		}

		cb(pool, np->u.range_seg.start, arg);
		cb(pool, np->u.range_seg.end, arg);
		break;
	}
	case NK_NMATH:
		// Circular chain - the head is tail->next.
		ast_foreach_chain(pool, ast_nmath_head(pool, np), np->u.nmath.count, cb,
				arg);
		break;
	case NK_LIST:
		ast_foreach_chain(pool, np->u.list.head, np->u.list.count, cb, arg);
		break;
	case NK_CDT_OP: {
		// NUL-terminated rather than counted.
		ast_ref ele = ast_cdt_op_head(np);

		while (ele != AST_REF_NULL) {
			ast_ref next = ast_pool_at(pool, ele)->next;

			cb(pool, ele, arg);
			ele = next;
		}

		break;
	}
	default:
		break; // NK_LEAF / NK_NONE - no children
	}
}

//==========================================================
// Destruction.
//

static void
ast_free_child_cb(ast_pool* pool, ast_ref child, void* arg)
{
	(void)arg;

	ast_free(pool, child);
}

void
ast_free(ast_pool* pool, ast_ref node)
{
	if (node == AST_REF_NULL) {
		return;
	}

	cf_assert(ast_pool_at(pool, node)->is_free == 0, CF_MISC,
			"ast_free: double free of ref %u", node);

	// No need to unlink children before freeing them: ast_pool_release
	// overwrites 'next' with the free-list link.
	ast_children_foreach(pool, node, ast_free_child_cb, NULL);
	ast_pool_release(pool, node);
}

// Merge for NK_NMATH (circular list via u.nmath).
static ast_ref
ast_nmath_merge(ael_context* ctx, ast_node_t type, ast_ref left, ast_ref right,
		ast_etype etype)
{
	ast_pool* pool = ctx->pool;
	ast_node* n_left = ast_pool_at(pool, left);
	ast_node* n_right = ast_pool_at(pool, right);

	if (n_left->type == type && n_right->type == type) {
		// Both n-ary: splice B's circle into A's, release B shell.
		// Reparent B's children to A.
		ast_ref a_head = ast_nmath_head(pool, n_left);
		ast_ref b_head = ast_nmath_head(pool, n_right);
		ast_ref e = b_head;

		for (uint32_t i = 0; i < n_right->u.nmath.count; i++) {
			ast_set_parent(pool, e, left);
			e = ast_pool_at(pool, e)->next;
		}

		ast_pool_at(pool, n_left->u.nmath.tail)->next = b_head;
		ast_pool_at(pool, n_right->u.nmath.tail)->next = a_head;
		n_left->u.nmath.tail = n_right->u.nmath.tail;
		n_left->u.nmath.count += n_right->u.nmath.count;
		n_left->has_deferable |= n_right->has_deferable;
		ast_span_children(pool, left, left, right, AST_REF_NULL);
		ast_pool_release(pool, right);
		return left;
	}

	if (n_left->type == type) {
		// A is n-ary: append B as last child.
		ast_ref a_head = ast_nmath_head(pool, n_left);
		ast_pool_at(pool, n_left->u.nmath.tail)->next = right;
		n_right->next = a_head;
		n_left->u.nmath.tail = right;
		n_left->u.nmath.count++;
		n_left->has_deferable |= n_right->has_deferable;
		ast_set_parent(pool, right, left);
		ast_span_children(pool, left, left, right, AST_REF_NULL);
		return left;
	}

	if (n_right->type == type) {
		// B is n-ary: prepend A as first child.
		ast_ref b_head = ast_nmath_head(pool, n_right);
		n_left->next = b_head;
		ast_pool_at(pool, n_right->u.nmath.tail)->next = left;
		n_right->u.nmath.count++;
		n_right->has_deferable |= n_left->has_deferable;
		ast_set_parent(pool, left, right);
		ast_span_children(pool, right, right, left, AST_REF_NULL);
		return right;
	}

	// Neither: create new n-ary with A, B as children.
	return ast_new_nary(ctx, type, left, right, etype);
}

// Merge for NK_LIST (linear list via u.list).
static ast_ref
ast_list_merge(ael_context* ctx, ast_node_t type, ast_ref left, ast_ref right)
{
	ast_pool* pool = ctx->pool;
	ast_node* n_left = ast_pool_at(pool, left);
	ast_node* n_right = ast_pool_at(pool, right);

	if (n_left->type == type && n_right->type == type) {
		// Both same: splice B's children into A, release B shell.
		ast_pool_at(pool, n_left->u.list.tail)->next = n_right->u.list.head;
		n_left->u.list.tail = n_right->u.list.tail;
		n_left->u.list.count += n_right->u.list.count;
		n_left->has_deferable |= n_right->has_deferable;
		ast_span_children(pool, left, left, right, AST_REF_NULL);
		ast_pool_release(pool, right);
		return left;
	}

	if (n_left->type == type) {
		// A is n-ary: append B as last child.
		AST_LIST_PUSH_TAIL(pool, left, right);
		n_left->has_deferable |= n_right->has_deferable;
		ast_span_children(pool, left, left, right, AST_REF_NULL);
		return left;
	}

	if (n_right->type == type) {
		// B is n-ary: prepend A as first child.
		AST_LIST_PUSH_HEAD(pool, right, left);
		n_right->has_deferable |= n_left->has_deferable;
		ast_span_children(pool, right, right, left, AST_REF_NULL);
		return right;
	}

	// Neither: create new list with A, B as children.
	ast_ref r = ast_new(pool, type);
	ast_node* n = ast_pool_at(pool, r);
	ast_pool_at(pool, left)->next = right;
	ast_pool_at(pool, right)->next = AST_REF_NULL;
	n->u.list.head = left;
	n->u.list.tail = right;
	n->u.list.count = 2;
	n->has_deferable = n_left->has_deferable || n_right->has_deferable;
	ast_span_children(pool, r, left, right, AST_REF_NULL);
	return r;
}

ast_ref
ast_nary_merge(ael_context* ctx, ast_node_t type, ast_ref left, ast_ref right,
		ast_etype etype)
{
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* n_left = ast_pool_at(ctx->pool, left);
	ast_node* n_right = ast_pool_at(ctx->pool, right);

	ast_etype merged = etype & n_left->etype & n_right->etype;

	if (merged == AST_ETYPE_ERROR) {
		ast_diag_operand_conflict(ctx, type, etype, n_left, n_right);
		return AST_REF_NULL;
	}

	etype = merged;
	ast_set_implicit_type(ctx, left, etype);
	ast_set_implicit_type(ctx, right, etype);

	ast_ref r;

	if (ast_node_table[type].kind == NK_NMATH) {
		// The splice / append / prepend arms don't re-set the surviving
		// node's etype: the ast_set_implicit_type calls above already
		// narrowed it (their NK_NMATH arm) when it was an operand here --
		// hence the kind check on the etype write below.
		r = ast_nmath_merge(ctx, type, left, right, etype);
	}
	else {
		r = ast_list_merge(ctx, type, left, right);
	}

	// Unreachable with the non-null children guaranteed above, but a null
	// merge result must not reach the writes below - ast_pool_at(pool,
	// AST_REF_NULL) would silently corrupt pool slot 0.
	if (r == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* n_r = ast_pool_at(ctx->pool, r);

	if (ast_node_table[type].kind != NK_NMATH) {
		n_r->etype = etype;
	}

	// No re-stamp needed: the merge branches extend the surviving node's
	// span over the absorbed operand (ast_span_children), so a flattened
	// chain's span tracks the whole chain.
	return r;
}

static ast_etype
ast_set_implicit_type_lr(ael_context* ctx, ast_node_t op, ast_ref left,
		ast_ref right, ast_etype etype)
{
	// A failed operand sub-parse may pass AST_REF_NULL here (e.g. shift
	// rules call this directly before building the node).
	if (left == AST_REF_NULL || right == AST_REF_NULL) {
		return AST_ETYPE_ERROR;
	}

	ast_node* n_left = ast_pool_at(ctx->pool, left);
	ast_node* n_right = ast_pool_at(ctx->pool, right);
	ast_etype merged = etype & n_left->etype & n_right->etype;

	if (merged == AST_ETYPE_ERROR) {
		ast_diag_operand_conflict(ctx, op, etype, n_left, n_right);
		return AST_ETYPE_ERROR;
	}

	ast_set_implicit_type(ctx, left, merged);
	ast_set_implicit_type(ctx, right, merged);

	return merged;
}

// Find the var_def node for a given var_idx -- direct index into the
// context's dense slot map (works even after the def's scope popped).
static ast_ref
ast_find_var_def(ael_context* ctx, uint32_t var_idx)
{
	return var_idx < ctx->scope_idx ? ctx->var_by_idx[var_idx] : AST_REF_NULL;
}

static void ast_var_def_propagate_refs(ael_context* ctx, ast_ref def_ref,
		ast_etype etype, ast_ref skip_ref);

static void
ast_set_implicit_type_body(ael_context* ctx, ast_ref node, ast_etype etype)
{
	if (etype == AST_ETYPE_AUTO || etype == AST_ETYPE_ERROR ||
			node == AST_REF_NULL) {
		return;
	}

	ast_pool* pool = ctx->pool;
	ast_node* np = ast_pool_at(pool, node);

	if (np->type == AST_BIN || np->type == AST_BIN_REF) {
		ast_bin_set_implicit_type(ctx, node, etype);
		return;
	}

	if (np->type == AST_VAR) {
		// Narrow the var ref and propagate back to the var_def's value.
		ast_etype narrowed = np->etype & etype;

		if (narrowed == AST_ETYPE_ERROR) {
			ast_diag_type_conflict(ctx, np->offset, np->u.var.name_sz,
					"variable type conflict", np->etype, etype);
			return;
		}

		if (narrowed == np->etype) {
			return;
		}

		np->etype = narrowed;

		// Find the var_def; propagate to its value expression and to the
		// sibling ${name} references (each climbs its own parents).
		ast_ref var_def = ast_find_var_def(ctx, np->u.var.var_idx);

		if (var_def != AST_REF_NULL) {
			ast_node* dp = ast_pool_at(pool, var_def);
			ast_set_implicit_type(ctx, dp->u.var_def.value, narrowed);
			ast_var_def_propagate_refs(ctx, var_def, narrowed, node);
		}

		return;
	}

	if (np->type == AST_PATH_CALL) {
		ast_ref op_ref = np->u.call.call_op;

		// Opaque calls -- modify calls return the modified collection, and
		// .select() / .modify() / wildcard-.remove() return types are set
		// by the call's own semantics (u.modify layout). Neither is
		// narrowable through the CDT op chain, but a requested type they
		// can't be (e.g. BOOL on a logical operand) is still a conflict,
		// not a silent pass.
		bool opaque = (np->u.call.stype & EXP_CALL_FLAG_MODIFY_LOCAL) != 0 ||
				(op_ref != AST_REF_NULL &&
						ast_is_select_family(ast_pool_at(pool, op_ref)->type));

		if (opaque) {
			if ((np->etype & etype) == AST_ETYPE_ERROR) {
				ast_diag_type_conflict(ctx, ast_disp_offset(np), np->sz,
						"type mismatch", np->etype, etype);
			}

			return;
		}

		if (op_ref == AST_REF_NULL) {
			return;
		}

		ast_node* op = ast_pool_at(pool, op_ref);

		// .select() / .modify() / wildcard-.remove() return type is set
		// by the call's own semantics (u.modify layout) and is not
		// narrowable through the regular CDT op chain -- but a requested
		// type it can't satisfy (e.g. BOOL on a list-returning select) is
		// a conflict, not a silent pass.
		if (ast_is_select_family(op->type)) {
			if ((np->etype & etype) == AST_ETYPE_ERROR) {
				ast_diag_type_conflict(ctx, ast_disp_offset(np), np->sz,
						"type mismatch", np->etype, etype);
			}

			return;
		}

		ast_etype op_prev = op->etype;

		op->etype &= etype;

		if (op->etype == AST_ETYPE_ERROR) {
			ast_diag_type_conflict(ctx, ast_disp_offset(np), np->sz,
					"type mismatch on bare-path get", op_prev, etype);
			return;
		}

		np->etype = op->etype;

		return;
	}

	ast_etype req = etype;

	etype &= np->etype;

	if (etype == AST_ETYPE_ERROR) {
		// Any kind reaches here, including the name-anchored bin queries and
		// var defs - take the display start, not the bare name.
		ast_diag_type_conflict(ctx, ast_disp_offset(np), np->sz,
				"type mismatch", req, np->etype);
		return;
	}

	if (np->etype == etype) {
		return;
	}

	np->etype = etype;

	const ast_node_info* info = &ast_node_table[np->type];

	switch (info->kind) {
	case NK_NMATH: {
		ast_ref head = ast_nmath_head(pool, np);
		ast_ref e = head;

		for (uint32_t i = 0; i < np->u.nmath.count; i++) {
			ast_set_implicit_type(ctx, e, etype);
			e = ast_pool_at(pool, e)->next;
		}

		break;
	}
	case NK_LIST:
		for (ast_ref e = np->u.list.head; e != AST_REF_NULL;
				e = ast_pool_at(pool, e)->next) {
			ast_set_implicit_type(ctx, e, etype);
		}
		break;
	case NK_BMATH:
		np->etype = ast_set_implicit_type_lr(ctx, np->type, np->u.binary.left,
				np->u.binary.right, etype);
		break;
	case NK_UNARY:
		ast_set_implicit_type(ctx, np->u.unary.operand, etype);
		break;
	default:
		// NK_BINARY math funcs (log/pow/findBit*) are intentionally absent:
		// their args have heterogeneous, fixed types (e.g. findBit = INT +
		// TRILEAN) set at construction, so a result-driven back-prop would
		// wrongly retype them. Don't add them here.
		break;
	}
}

// Public entry: bounds the mutually recursive propagation walk (_lr, bin_set,
// and climb all re-enter here) so it can't overflow the C stack.
void
ast_set_implicit_type(ael_context* ctx, ast_ref node, ast_etype etype)
{
	if (ctx->prop_depth >= EXP_MAX_DEPTH) {
		// Left-deep chains escape YYSTACKDEPTH; cap the walk so a huge
		// expression can't overflow the C stack (like ael_count_sz). Anchor on
		// the node being narrowed when available, else the current token.
		uint32_t off = ctx->last_token_offset;
		uint32_t sz = ctx->last_token_sz;

		if (node != AST_REF_NULL) {
			const ast_node* np = ast_pool_at(ctx->pool, node);

			off = ast_disp_offset(np);
			sz = np->sz;
		}

		ael_err(ctx, off, sz, "expression too deeply nested");
		return;
	}

	ctx->prop_depth++;
	ast_set_implicit_type_body(ctx, node, etype);
	ctx->prop_depth--;
}

// Relational comparisons (== != < > <= >=): their operands share a type
// (unlike `in` / geoCompare).
static inline bool
ast_is_rel_cmp(ast_node_t type)
{
	return type >= AST_CMP_EQ && type <= AST_CMP_LE;
}

// Climb from node through consecutive math parents to the top,
// then propagate the type downward from there.
static void
ast_climb_and_propagate(ael_context* ctx, ast_ref node, ast_ref parent,
		ast_etype etype)
{
	ast_pool* pool = ctx->pool;
	ast_ref top = node;
	ast_ref pr = parent;
	ast_node* pp = NULL; // node at pr whenever pr is non-NULL below

	while (pr != AST_REF_NULL) {
		pp = ast_pool_at(pool, pr);
		ast_node_kind k = ast_node_table[pp->type].kind;

		if (k != NK_NMATH && k != NK_BMATH) {
			break;
		}

		top = pr;
		pr = (k == NK_NMATH) ? pp->u.nmath.parent : pp->u.binary.parent;
	}

	ast_set_implicit_type(ctx, top, etype);

	if (pr != AST_REF_NULL) {
		// Relational operands share a type: narrow the sibling too, so a bin
		// pinned later flows across (ast_new_bcmp only cross-infers at
		// reduction) -- inference is order-independent and transitive.
		if (ast_is_rel_cmp(pp->type)) {
			ast_ref left = pp->u.binary.left;
			ast_ref other = (top == left) ? pp->u.binary.right : left;

			ast_set_implicit_type(ctx, other, etype);
		}
		else if (pp->type == AST_VAR_DEF) {
			// The narrowing started inside a with-binding's value (e.g. its
			// bin was pinned by another occurrence) -- flow it to every
			// ${name} reference chained off the def.
			ast_var_def_propagate_refs(ctx, pr, etype, AST_REF_NULL);
		}
	}
}

// Narrow every ${name} reference chained off a var_def and climb each
// reference's parents -- the var twin of ast_bin_set_implicit_type's
// ref-stack walk. skip_ref is the reference that initiated the narrowing
// (already narrowed by its caller), or AST_REF_NULL when the narrowing came
// from inside the def's value. Convergent: a reference whose etype doesn't
// change is not re-climbed, so mutually-triggered walks terminate.
static void
ast_var_def_propagate_refs(ael_context* ctx, ast_ref def_ref, ast_etype etype,
		ast_ref skip_ref)
{
	ast_pool* pool = ctx->pool;
	const ast_node* dp = ast_pool_at(pool, def_ref);

	for (ast_ref rr = dp->u.var_def.ref_root; rr != AST_REF_NULL;) {
		ast_node* ref = ast_pool_at(pool, rr);
		ast_ref next = ref->u.var.ref_next;

		if (rr != skip_ref) {
			ast_etype narrowed = ref->etype & etype;

			if (narrowed == AST_ETYPE_ERROR) {
				ast_diag_type_conflict(ctx, ref->offset, ref->u.var.name_sz,
						"variable type conflict", ref->etype, etype);
			}
			else if (narrowed != ref->etype) {
				ref->etype = narrowed;
				ast_climb_and_propagate(ctx, rr, ref->u.var.parent, narrowed);
			}
		}

		rr = next;
	}
}

void
ast_bin_set_implicit_type(ael_context* ctx, ast_ref bin_or_ref, ast_etype etype)
{
	if (etype == AST_ETYPE_AUTO || etype == AST_ETYPE_ERROR) {
		return;
	}

	ast_pool* pool = ctx->pool;
	ast_node* np = ast_pool_at(pool, bin_or_ref);

	// Find the canonical bin.
	ast_ref canon_ref;
	ast_node* canon;

	if (np->type == AST_BIN) {
		canon_ref = bin_or_ref;
		canon = np;
	}
	else if (np->type == AST_BIN_REF) {
		canon_ref = np->u.bin_ref.bin;
		canon = ast_pool_at(pool, canon_ref);
	}
	else {
		return;
	}

	// Narrow the canonical bin's etype.
	ast_etype narrowed = canon->etype & etype;

	if (narrowed == AST_ETYPE_ERROR) {
		// Point at the offending postfix token snapshotted by the
		// type_name reduction, so chained `:T:T` flags the second
		// type name rather than the bin's start.
		ast_diag_type_conflict(ctx, ctx->postfix_token_offset,
				ctx->postfix_token_sz, "bin type conflict", canon->etype, etype);
		return;
	}

	if (narrowed == canon->etype) {
		return; // no change
	}

	canon->etype = narrowed;

	// Climb to the top math ancestor from node, then propagate down.
	ast_climb_and_propagate(ctx, canon_ref, canon->u.bin.parent, narrowed);

	// Walk the ref stack — set etype, climb each ref's parent chain.
	for (ast_ref rr = canon->u.bin.ref_root; rr != AST_REF_NULL;) {
		ast_node* ref = ast_pool_at(pool, rr);
		ref->etype = narrowed;
		ast_climb_and_propagate(ctx, rr, ref->u.bin_ref.parent, narrowed);
		rr = ref->u.bin_ref.ref_next;
	}
}

//==========================================================
// Debug print.
//

static const char*
ast_type_name(ast_node_t type)
{
	const char* name = ast_node_table[type].name;
	return name != NULL ? name : "?";
}

static const char*
ast_esubtype_name(ast_esubtype sub)
{
	switch (sub) {
	case AST_ESUBTYPE_UNORDERED:
		return "UNORDERED";
	case AST_ESUBTYPE_ORDERED:
		return "ORDERED";
	case AST_ESUBTYPE_KEY_VALUE_ORDERED:
		return "KEY_VALUE_ORDERED";
	default:
		return "?";
	}
}

static const char*
ast_etype_name(ast_etype et)
{
	switch (et) {
	case AST_ETYPE_ERROR:
		return "ERROR";
	case AST_ETYPE_NIL:
		return "NIL";
	case AST_ETYPE_TRILEAN:
		return "BOOL";
	case AST_ETYPE_INT:
		return "INT";
	case AST_ETYPE_STR:
		return "STRING";
	case AST_ETYPE_LIST:
		return "LIST";
	case AST_ETYPE_MAP:
		return "MAP";
	case AST_ETYPE_BLOB:
		return "BLOB";
	case AST_ETYPE_FLOAT:
		return "FLOAT";
	case AST_ETYPE_AUTO_NUMERIC:
		return "AUTO_NUMERIC";
	case AST_ETYPE_GEOJSON:
		return "GEOJSON";
	case AST_ETYPE_HLL:
		return "HLL";
	case AST_ETYPE_AUTO_CDT:
		return "AUTO_CDT";
	case AST_ETYPE_AUTO:
		return "AUTO";
	default:
		return "?";
	}
}

// Render a possibly multi-bit etype for a diagnostic: the canonical name for a
// single type, "any" for a fully-open slot, else the candidate types joined by
// '|' (e.g. "INT|FLOAT"). Returns buf, so two names in one message need two
// separate buffers.
static const char*
ast_etype_str(ast_etype et, char* buf, size_t sz)
{
	if (et == AST_ETYPE_AUTO) {
		snprintf(buf, sz, "any");
		return buf;
	}

	if (ast_type_resolved(et)) {
		snprintf(buf, sz, "%s", ast_etype_name(et));
		return buf;
	}

	size_t off = 0;

	buf[0] = '\0';

	for (uint32_t bit = AST_ETYPE_NIL; bit <= (uint32_t)AST_ETYPE_HLL; bit <<= 1) {
		if ((et & (ast_etype)bit) == 0) {
			continue;
		}

		int n = snprintf(buf + off, sz - off, "%s%s", off == 0 ? "" : "|",
				ast_etype_name((ast_etype)bit));

		if (n < 0 || (size_t)n >= sz - off) {
			break; // truncated -- stop cleanly
		}

		off += (size_t)n;
	}

	if (off == 0) {
		snprintf(buf, sz, "?");
	}

	return buf;
}

//==========================================================
// Loop-var unification — intersect etypes across all occurrences of
// the same builtin in a sub-program's chain (built up at construction
// in ael_context.{at_root, key_root}). Called at filter scope-pop
// and at end-of-parse for the standalone-filter entry.
//

void
ast_unify_loop_var_chain(ast_pool* pool, ast_ref head, ael_diag_list* diags)
{
	if (head == AST_REF_NULL || diags == NULL) {
		return;
	}

	ast_etype acc = AST_ETYPE_AUTO;
	uint32_t conflict_offset = 0;
	uint32_t conflict_sz = 0;
	bool conflict = false;

	for (ast_ref r = head; r != AST_REF_NULL;) {
		ast_node* np = ast_pool_at(pool, r);
		ast_etype next = (ast_etype)(acc & np->etype);

		if (next == AST_ETYPE_ERROR) {
			conflict = true;
			conflict_offset = ast_disp_offset(np);
			conflict_sz = np->sz;
			break;
		}

		acc = next;
		r = np->u.loop_var.next_same_kind;
	}

	if (conflict) {
		ael_diag_add(diags, AEL_SEV_ERROR, conflict_offset, conflict_sz,
				"loop variable used with incompatible types in the same filter / modify body");
		return;
	}

	for (ast_ref r = head; r != AST_REF_NULL;) {
		ast_node* np = ast_pool_at(pool, r);

		np->etype = acc;
		r = np->u.loop_var.next_same_kind;
	}
}

typedef struct {
	const char* input;
	int indent;
} print_walk;

// ast_children_foreach hands over empty slots too; skip them so an absent
// optional child (open-ended range end, bare '*', select()'s null value)
// prints nothing rather than a "(null)" line.
static void
ast_print_child(ast_pool* pool, ast_ref child, void* arg)
{
	if (child == AST_REF_NULL) {
		return;
	}

	print_walk* w = arg;

	ast_print(pool, w->input, child, w->indent + 2);
}

void
ast_print(ast_pool* pool, const char* input, ast_ref node, int indent)
{
	if (node == AST_REF_NULL) {
		printf("%*s(null)\n", indent, "");
		return;
	}

	ast_node* np = ast_pool_at(pool, node);

	printf("%*s(%s", indent, "", ast_type_name(np->type));

	if (np->esubtype != AST_ESUBTYPE_UNSET) {
		printf(":%s", ast_esubtype_name(np->esubtype));
	}

	switch (np->type) {
	case AST_INT:
		printf(" %ld)\n", (long)np->u.ival);
		break;
	case AST_FLOAT:
		printf(" %g)\n", np->u.fval);
		break;
	case AST_ZERO:
		printf(" 0)\n");
		break;
	case AST_BOOL:
		printf(" %s)\n", np->u.bval ? "true" : "false");
		break;
	case AST_STRING:
		printf(" \"%.*s\")\n", (int)np->u.str.sz, np->u.str.str);
		break;
	case AST_BIN:
		printf(" \"%.*s\" type: %s)\n", (int)np->u.bin.name_sz,
				input + np->offset, ast_etype_name(np->etype));
		break;
	case AST_BIN_REF: {
		ast_node* canon = ast_pool_at(pool, np->u.bin_ref.bin);
		printf(" \"%.*s\" type: %s)\n", (int)canon->u.bin.name_sz,
				input + canon->offset, ast_etype_name(np->etype));
		break;
	}
	case AST_BIN_TYPE:
	case AST_BIN_EXISTS:
		printf(" \"%.*s\")\n", (int)np->u.bin_type.name_sz, input + np->offset);
		break;
	case AST_VAR:
		printf(" \"%.*s\" idx=%u)\n", (int)np->u.var.name_sz,
				input + np->offset, np->u.var.var_idx);
		break;
	case AST_BLOB:
		printf(" 0x%.*s)\n", (int)np->u.str.sz, np->u.str.str);
		break;
	case AST_B64_BLOB:
		printf(" b64'%.*s')\n", (int)np->u.str.sz, np->u.str.str);
		break;
	case AST_GEO_LITERAL:
		printf(" geo'%.*s')\n", (int)np->u.str.sz, np->u.str.str);
		break;
	case AST_META:
		printf(" op_code=%d param=%ld)\n", (int)np->u.meta.op_code,
				(long)np->u.meta.param);
		break;

	// Plain child descent — the walker owns the per-layout knowledge; the
	// bespoke cases below (map pairs, filters, call headers) keep their
	// structural labels.
	case AST_CMP_EQ:
	case AST_CMP_NE:
	case AST_CMP_GT:
	case AST_CMP_GE:
	case AST_CMP_LT:
	case AST_CMP_LE:
	case AST_CMP_IN:
	case AST_SUB:
	case AST_DIV:
	case AST_MOD:
	case AST_POW:
	case AST_LSHIFT:
	case AST_RSHIFT_ARITH:
	case AST_RSHIFT_LOGIC:
	case AST_NOT:
	case AST_BIT_NOT:
	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN:
	case AST_AND:
	case AST_OR:
	case AST_BIT_AND:
	case AST_BIT_OR:
	case AST_BIT_XOR:
	case AST_EXCLUSIVE:
	case AST_LIST:
	case AST_LET:
	case AST_WHEN:
	case AST_MAP_KEY:
	case AST_MAP_VALUE:
	case AST_MAP_INDEX:
	case AST_MAP_RANK:
	case AST_LIST_INDEX:
	case AST_LIST_VALUE:
	case AST_LIST_RANK:
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
	case AST_PATH_FUNC_CAST_STRING:
	case AST_PATH_FUNC_TYPE:
	case AST_MAP_KEY_RANGE:
	case AST_MAP_INDEX_RANGE:
	case AST_MAP_VALUE_RANGE:
	case AST_MAP_RANK_RANGE:
	case AST_LIST_INDEX_RANGE:
	case AST_LIST_VALUE_RANGE:
	case AST_LIST_RANK_RANGE:
	case AST_MAP_KEY_LIST:
	case AST_MAP_VALUE_LIST:
	case AST_LIST_VALUE_LIST:
	case AST_MAP_INDEX_REL_RANGE:
	case AST_MAP_RANK_REL_RANGE:
	case AST_LIST_RANK_REL_RANGE: {
		if (ast_seg_inverted(np)) {
			printf(" INVERTED");
		}

		printf("\n");

		print_walk w = { .input = input, .indent = indent };

		ast_children_foreach(pool, node, ast_print_child, &w);
		printf("%*s)\n", indent, "");
		break;
	}

	case AST_PATH_CTX:
		printf(" count=%u%s%s\n", np->u.ctx_list.count,
				np->u.ctx_list.is_multi ? " multi" : "",
				np->u.ctx_list.last_is_multi ? " last_multi" : "");
		for (ast_ref e = np->u.ctx_list.head; e != AST_REF_NULL;
				e = ast_pool_at(pool, e)->next) {
			ast_print(pool, input, e, indent + 2);
		}
		printf("%*s)\n", indent, "");
		break;

	case AST_PATH_CALL:
		printf(" stype=%d etype=%s\n", np->u.call.stype,
				ast_etype_name(np->etype));
		if (np->u.call.ctx != AST_REF_NULL) {
			ast_print(pool, input, np->u.call.ctx, indent + 2);
		}
		ast_print(pool, input, np->u.call.call_op, indent + 2);
		printf("%*s)\n", indent, "");
		break;

	case AST_PATH_FUNC_GET:
	case AST_PATH_FUNC_GET_KEYS:
	case AST_PATH_FUNC_GET_KEY_VALUES:
	case AST_PATH_FUNC_GET_TREE:
	case AST_PATH_FUNC_GET_INDEXES:
	case AST_PATH_FUNC_GET_RANKS:
	case AST_PATH_FUNC_GET_MAPS:
	case AST_PATH_FUNC_EXISTS:
	case AST_PATH_FUNC_COUNT:
	case AST_PATH_FUNC_JOIN:
	case AST_PATH_FUNC_REMOVE:
	case AST_PATH_FUNC_SET:
	case AST_PATH_FUNC_INSERT:
	case AST_PATH_FUNC_UPDATE:
	case AST_PATH_FUNC_INCREMENT:
	case AST_PATH_FUNC_APPEND:
	case AST_PATH_FUNC_APPEND_ITEMS:
	case AST_PATH_FUNC_INSERT_ITEMS:
	case AST_PATH_FUNC_PUT_ITEMS:
	case AST_PATH_FUNC_CLEAR:
	case AST_PATH_FUNC_SORT:
	case AST_PATH_FUNC_BIT_GET:
	case AST_PATH_FUNC_BIT_COUNT:
	case AST_PATH_FUNC_BIT_LSCAN:
	case AST_PATH_FUNC_BIT_RSCAN:
	case AST_PATH_FUNC_BIT_GET_INT:
	case AST_PATH_FUNC_BIT_SET:
	case AST_PATH_FUNC_BIT_OR:
	case AST_PATH_FUNC_BIT_XOR:
	case AST_PATH_FUNC_BIT_AND:
	case AST_PATH_FUNC_BIT_NOT:
	case AST_PATH_FUNC_BIT_LSHIFT:
	case AST_PATH_FUNC_BIT_RSHIFT:
	case AST_PATH_FUNC_BIT_ADD:
	case AST_PATH_FUNC_BIT_SUBTRACT:
	case AST_PATH_FUNC_BIT_SET_INT:
	case AST_PATH_FUNC_BIT_RESIZE:
	case AST_PATH_FUNC_BIT_INSERT:
	case AST_PATH_FUNC_BIT_REMOVE:
	case AST_PATH_FUNC_HLL_COUNT:
	case AST_PATH_FUNC_HLL_DESCRIBE:
	case AST_PATH_FUNC_HLL_MAY_CONTAIN:
	case AST_PATH_FUNC_HLL_UNION:
	case AST_PATH_FUNC_HLL_UNION_COUNT:
	case AST_PATH_FUNC_HLL_INTERSECT_COUNT:
	case AST_PATH_FUNC_HLL_SIMILARITY:
	case AST_PATH_FUNC_HLL_INIT:
	case AST_PATH_FUNC_HLL_ADD:
	case AST_BIT_OP:
	case AST_HLL_OP:
	case AST_STR_OP:
	case AST_CDT_OP: {
		const char* op_name;

		if (ast_cdt_op_op_code(np) == 0) {
			op_name = "unresolved";
		}
		else if (np->type == AST_HLL_OP) {
			op_name = as_hll_op_name(ast_cdt_op_op_code(np),
					ast_cdt_op_is_modify(np));
		}
		else if (np->type == AST_BIT_OP) {
			op_name = as_bits_op_name(ast_cdt_op_op_code(np),
					ast_cdt_op_is_modify(np));
		}
		else if (np->type == AST_STR_OP) {
			op_name = as_string_op_name(ast_cdt_op_op_code(np),
					ast_cdt_op_is_modify(np));
		}
		else {
			op_name = cdt_exp_display_name((as_cdt_optype)ast_cdt_op_op_code(np));
		}

		printf(" op=%s(%u) params=%u\n", op_name, ast_cdt_op_op_code(np),
				ast_cdt_op_count(np));

		for (ast_ref e = ast_cdt_op_head(np); e != AST_REF_NULL;
				e = ast_pool_at(pool, e)->next) {
			ast_print(pool, input, e, indent + 2);
		}
		printf("%*s)\n", indent, "");
		break;
	}

	case AST_CASE:
		printf(":\n");
		ast_print(pool, input, np->u.when_case.cond, indent + 4);
		ast_print(pool, input, np->u.when_case.result, indent + 2);
		printf("%*s)\n", indent, "");
		break;

	case AST_VAR_DEF: {
		printf(" \"%.*s\"\n", (int)np->u.var_def.name_sz, input + np->offset);

		print_walk w = { .input = input, .indent = indent };

		ast_children_foreach(pool, node, ast_print_child, &w);
		printf("%*s)\n", indent, "");
		break;
	}

	case AST_FUNC_ABS:
	case AST_FUNC_CEIL:
	case AST_FUNC_FLOOR:
	case AST_FUNC_COUNT_ONE_BITS:
		printf("\n");
		ast_print(pool, input, np->u.func1.arg, indent + 2);
		printf("%*s)\n", indent, "");
		break;

	case AST_FUNC_LOG:
	case AST_FUNC_POW:
	case AST_FUNC_FIND_BIT_LEFT:
	case AST_FUNC_FIND_BIT_RIGHT:
		printf("\n");
		ast_print(pool, input, np->u.func2.arg1, indent + 2);
		ast_print(pool, input, np->u.func2.arg2, indent + 2);
		printf("%*s)\n", indent, "");
		break;

	case AST_MAP: {
		ast_ref key = np->u.list.head;

		printf("\n");
		for (uint32_t i = 0; i < np->u.list.count; i++) {
			ast_node* kp = ast_pool_at(pool, key);
			ast_ref val = kp->next;

			cf_assert(val != AST_REF_NULL, CF_MISC,
					"ast_print: map literal missing value");

			printf("%*s(pair\n", indent + 2, "");
			ast_print(pool, input, key, indent + 4);
			ast_print(pool, input, val, indent + 4);
			printf("%*s)\n", indent + 2, "");
			key = ast_pool_at(pool, val)->next;
		}
		printf("%*s)\n", indent, "");
		break;
	}

	case AST_LOOP_VAR: {
		const char* names[AS_EXP_BUILTIN_COUNT] = { "@key", "@", "@index" };
		uint8_t idx = np->u.loop_var.builtin;

		printf(" %s)\n", idx < AS_EXP_BUILTIN_COUNT ? names[idx] : "@?");
		break;
	}

	case AST_WILDCARD_SEG:
		printf("\n");
		if (np->u.by_exp_seg.filter != AST_REF_NULL) {
			printf("%*sfilter:\n", indent + 2, "");
			ast_print(pool, input, np->u.by_exp_seg.filter, indent + 4);
		}
		printf("%*s)\n", indent, "");
		break;

	case AST_AND_EXP_SEG:
		printf("\n");
		printf("%*sand_filter:\n", indent + 2, "");
		ast_print(pool, input, np->u.by_exp_seg.filter, indent + 4);
		printf("%*s)\n", indent, "");
		break;

	case AST_PATH_FUNC_SELECT:
	case AST_PATH_FUNC_MODIFY:
	case AST_PATH_FUNC_PSELECT_REMOVE:
		printf(" sel_type=%d props=0x%x", np->u.modify.sel_type,
				np->u.modify.props);
		if (np->u.modify.value != AST_REF_NULL) {
			printf("\n");
			ast_print(pool, input, np->u.modify.value, indent + 2);
			printf("%*s)\n", indent, "");
		}
		else {
			printf(")\n");
		}
		break;

	default:
		printf(")\n");
		break;
	}
}
