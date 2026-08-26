/*
 * ael_func_table.c
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

#include "exp/ael_func_table.h"

#include <stddef.h>
#include <string.h>

#include "log.h"

// The AEL_IS_BIT_TYPE / AEL_IS_HLL_TYPE range macros (and the bit-vs-HLL
// finalizer routing they drive) assume the bit and HLL pf-type enum blocks
// stay contiguous and immediately adjacent. Lock that here so inserting an
// unrelated AST_PATH_FUNC_* between them is a compile error, not a silent
// misroute.
COMPILER_ASSERT(AST_PATH_FUNC_HLL_COUNT == AST_PATH_FUNC_BIT_REMOVE + 1);
// The STRING block follows the HLL block; AEL_IS_STR_TYPE assumes it stays
// contiguous and immediately after AST_PATH_FUNC_HLL_ADD.
COMPILER_ASSERT(AST_PATH_FUNC_STR_LENGTH == AST_PATH_FUNC_HLL_ADD + 1);

//==========================================================
// Function table.
//
// One row per callable function. The arg etype on a slot is the inference
// target the resolver applies before construction; AUTO means "the family
// builder owns inference" (bit / HLL ops infer per-op inside ael_new_*).
// result_etype is meaningful only for SCALAR rows (passed to
// ast_new_func1/func2/nary); BIT / HLL result types are derived in their
// finalizers.
//
// Slots with name AEL_PNAME_NONE are positional; named slots follow. A slot is
// positional-only or named-only, enforced purely by the slot names (no extra
// flag). Fixed-arity multi-arg scalars (log / pow / findBitLeft / findBitRight)
// and every BIT row are fully named — mandatory and order-independent;
// single-arg scalars and variadic min / max stay positional. hllAdd is the one
// hybrid (positional list, then named indexBits / minHashBits).
//

static const ael_func_spec_t FUNC_TABLE[] = {
	//------------------------------------------------
	// Scalar math (positional, except named fixed-arity log / pow / findBit*).

	{ "abs", AST_FUNC_ABS, AEL_FAM_SCALAR, false, 1, 1, AST_ETYPE_AUTO_NUMERIC,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO_NUMERIC } } },
	{ "ceil", AST_FUNC_CEIL, AEL_FAM_SCALAR, false, 1, 1, AST_ETYPE_FLOAT,
			{ { AEL_PNAME_NONE, AST_ETYPE_FLOAT } } },
	{ "floor", AST_FUNC_FLOOR, AEL_FAM_SCALAR, false, 1, 1, AST_ETYPE_FLOAT,
			{ { AEL_PNAME_NONE, AST_ETYPE_FLOAT } } },
	{ "countOneBits", AST_FUNC_COUNT_ONE_BITS, AEL_FAM_SCALAR, false, 1, 1,
			AST_ETYPE_INT, { { AEL_PNAME_NONE, AST_ETYPE_INT } } },

	{ "log", AST_FUNC_LOG, AEL_FAM_SCALAR, false, 2, 2, AST_ETYPE_FLOAT,
			{ { AEL_PNAME_VALUE, AST_ETYPE_FLOAT },
					{ AEL_PNAME_BASE, AST_ETYPE_FLOAT } } },
	{ "pow", AST_FUNC_POW, AEL_FAM_SCALAR, false, 2, 2, AST_ETYPE_FLOAT,
			{ { AEL_PNAME_BASE, AST_ETYPE_FLOAT },
					{ AEL_PNAME_EXPONENT, AST_ETYPE_FLOAT } } },

	// findBitLeft / findBitRight(x:, value:): x = INT scanned, value = TRILEAN
	// (true => find a set bit, false => a clear bit); result INT (offset).
	// See build_int_scan in exp.c.
	{ "findBitLeft", AST_FUNC_FIND_BIT_LEFT, AEL_FAM_SCALAR, false, 2, 2,
			AST_ETYPE_INT,
			{ { AEL_PNAME_X, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_TRILEAN } } },
	{ "findBitRight", AST_FUNC_FIND_BIT_RIGHT, AEL_FAM_SCALAR, false, 2, 2,
			AST_ETYPE_INT,
			{ { AEL_PNAME_X, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_TRILEAN } } },

	// Variadic: >= required_count same-typed positional args. params[0]
	// is the element template; param_count is 0 (no fixed slots).
	{ "max", AST_FUNC_MAX, AEL_FAM_SCALAR, true, 0, 2, AST_ETYPE_AUTO_NUMERIC,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO_NUMERIC } } },
	{ "min", AST_FUNC_MIN, AEL_FAM_SCALAR, true, 0, 2, AST_ETYPE_AUTO_NUMERIC,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO_NUMERIC } } },
	{ "exclusive", AST_EXCLUSIVE, AEL_FAM_SCALAR, true, 0, 2, AST_ETYPE_TRILEAN,
			{ { AEL_PNAME_NONE, AST_ETYPE_TRILEAN } } },

	//------------------------------------------------
	// Geo builtins — top-level calls (no receiver) with bespoke builders in
	// ael_resolve_geo_fn. geoJson's arg must be a string literal; geoCompare
	// pins both operands to GEOJSON in ael_new_geo_compare, so slot etypes are
	// AUTO.

	{ "geoJson", AST_GEO_LITERAL, AEL_FAM_GEO, false, 1, 1, AST_ETYPE_GEOJSON,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "geoCompare", AST_CMP_GEO, AEL_FAM_GEO, false, 2, 2, AST_ETYPE_TRILEAN,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO },
					{ AEL_PNAME_NONE, AST_ETYPE_AUTO } } },

	//------------------------------------------------
	// BLOB-bit method-style ops — named args. Slot etypes are declarative:
	// the generic builder (ael_build_from_spec) pins them. `signed` stays
	// AUTO — it's a bool literal converted to a wire int 0 / 1 by
	// ael_build_bit's prelude. Result etypes stay derived in the finalizer
	// (they depend on is_modify).

	{ "bitGet", AST_PATH_FUNC_BIT_GET, AEL_FAM_BIT, false, 2, 2, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT } } },
	{ "b64Encode", AST_PATH_FUNC_BIT_B64_ENCODE, AEL_FAM_BIT, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "bitCount", AST_PATH_FUNC_BIT_COUNT, AEL_FAM_BIT, false, 2, 2,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT } } },
	{ "bitLscan", AST_PATH_FUNC_BIT_LSCAN, AEL_FAM_BIT, false, 3, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_TRILEAN } } },
	{ "bitRscan", AST_PATH_FUNC_BIT_RSCAN, AEL_FAM_BIT, false, 3, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_TRILEAN } } },
	{ "bitGetInt", AST_PATH_FUNC_BIT_GET_INT, AEL_FAM_BIT, false, 3, 2,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_SIGNED, AST_ETYPE_AUTO } } },
	{ "bitSet", AST_PATH_FUNC_BIT_SET, AEL_FAM_BIT, false, 3, 3, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_BLOB } } },
	{ "bitOr", AST_PATH_FUNC_BIT_OR, AEL_FAM_BIT, false, 3, 3, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_BLOB } } },
	{ "bitXor", AST_PATH_FUNC_BIT_XOR, AEL_FAM_BIT, false, 3, 3, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_BLOB } } },
	{ "bitAnd", AST_PATH_FUNC_BIT_AND, AEL_FAM_BIT, false, 3, 3, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_BLOB } } },
	{ "bitNot", AST_PATH_FUNC_BIT_NOT, AEL_FAM_BIT, false, 2, 2, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT } } },
	{ "bitLshift", AST_PATH_FUNC_BIT_LSHIFT, AEL_FAM_BIT, false, 3, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_SHIFT, AST_ETYPE_INT } } },
	{ "bitRshift", AST_PATH_FUNC_BIT_RSHIFT, AEL_FAM_BIT, false, 3, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_SHIFT, AST_ETYPE_INT } } },
	{ "bitAdd", AST_PATH_FUNC_BIT_ADD, AEL_FAM_BIT, false, 4, 3, AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_INT },
					{ AEL_PNAME_SIGNED, AST_ETYPE_AUTO } } },
	{ "bitSubtract", AST_PATH_FUNC_BIT_SUBTRACT, AEL_FAM_BIT, false, 4, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_INT },
					{ AEL_PNAME_SIGNED, AST_ETYPE_AUTO } } },
	{ "bitSetInt", AST_PATH_FUNC_BIT_SET_INT, AEL_FAM_BIT, false, 3, 3,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_SIZE, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_INT } } },
	{ "bitResize", AST_PATH_FUNC_BIT_RESIZE, AEL_FAM_BIT, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_BYTE_SIZE, AST_ETYPE_INT } } },
	{ "bitInsert", AST_PATH_FUNC_BIT_INSERT, AEL_FAM_BIT, false, 2, 2,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_BYTE_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_BLOB } } },
	{ "bitRemove", AST_PATH_FUNC_BIT_REMOVE, AEL_FAM_BIT, false, 2, 2,
			AST_ETYPE_AUTO,
			{ { AEL_PNAME_BYTE_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_BYTE_SIZE, AST_ETYPE_INT } } },

	//------------------------------------------------
	// HLL method-style ops. Read set-ops take one positional list; hllInit
	// is named; hllAdd mixes a positional list with named indexBits /
	// minHashBits. hllInit / hllAdd slots are declarative (generic
	// builder); the set-ops stay AUTO — their list elements pin per
	// element (HLL) in ael_new_hll_set_fn, which a wrapper-LIST pin can't
	// express.

	{ "hllCount", AST_PATH_FUNC_HLL_COUNT, AEL_FAM_HLL, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "hllDescribe", AST_PATH_FUNC_HLL_DESCRIBE, AEL_FAM_HLL, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "hllMayContain", AST_PATH_FUNC_HLL_MAY_CONTAIN, AEL_FAM_HLL, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "hllUnion", AST_PATH_FUNC_HLL_UNION, AEL_FAM_HLL, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "hllUnionCount", AST_PATH_FUNC_HLL_UNION_COUNT, AEL_FAM_HLL, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "hllIntersectCount", AST_PATH_FUNC_HLL_INTERSECT_COUNT, AEL_FAM_HLL, false,
			1, 1, AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "hllSimilarity", AST_PATH_FUNC_HLL_SIMILARITY, AEL_FAM_HLL, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "hllInit", AST_PATH_FUNC_HLL_INIT, AEL_FAM_HLL, false, 2, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_INDEX_BITS, AST_ETYPE_INT },
					{ AEL_PNAME_MIN_HASH_BITS, AST_ETYPE_INT } } },
	{ "hllAdd", AST_PATH_FUNC_HLL_ADD, AEL_FAM_HLL, false, 3, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_LIST },
					{ AEL_PNAME_INDEX_BITS, AST_ETYPE_INT },
					{ AEL_PNAME_MIN_HASH_BITS, AST_ETYPE_INT } } },

	//------------------------------------------------
	// CDT path functions — method-style `recv.name(...)`. Build an
	// AST_PATH_FUNC_* node (ael_resolve_path_fn) routed through the bin /
	// value / ctx_list path finalizers. No-arg reads/casts/whole-CDT, plus the
	// arity-1 leaf mutations (one positional value). etype is owned by the
	// finalizers, so slot/result etypes are AUTO.

	{ "exists", AST_PATH_FUNC_EXISTS, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	{ "count", AST_PATH_FUNC_COUNT, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	// type() — bin particle-type read; valid only on a bare bin
	// (ael_build_bin_func -> AST_BIN_TYPE). The path / value finalizers reject
	// it. Bare type names (INT / ...) are separate particle-type constants.
	{ "type", AST_PATH_FUNC_TYPE, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	{ "getKeys", AST_PATH_FUNC_GET_KEYS, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "getKeyValues", AST_PATH_FUNC_GET_KEY_VALUES, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "getTree", AST_PATH_FUNC_GET_TREE, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "getIndexes", AST_PATH_FUNC_GET_INDEXES, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "getRanks", AST_PATH_FUNC_GET_RANKS, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "getMaps", AST_PATH_FUNC_GET_MAPS, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	// toInt / toFloat are polymorphic casts: the operand may be numeric (a
	// FLOAT->INT / INT->FLOAT cast) or a STRING (parsed). The concrete op is
	// chosen from the resolved operand type post-inference (ael_dispatch_to_cast).
	{ "toInt", AST_PATH_FUNC_CAST_INT, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "toFloat", AST_PATH_FUNC_CAST_FLOAT, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },
	{ "clear", AST_PATH_FUNC_CLEAR, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	{ "sort", AST_PATH_FUNC_SORT, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	{ "remove", AST_PATH_FUNC_REMOVE, AEL_FAM_PATH, false, 0, 0, AST_ETYPE_AUTO,
			{ { 0 } } },
	// Write-intent verbs. setTo = DEFAULT upsert; insert = CREATE_ONLY
	// (map) / positional (list); update = UPDATE_ONLY (map only); add =
	// increment. The verb presets the create/update flag in the resolver.
	{ "setTo", AST_PATH_FUNC_SET, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "insert", AST_PATH_FUNC_INSERT, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "update", AST_PATH_FUNC_UPDATE, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "add", AST_PATH_FUNC_INCREMENT, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "append", AST_PATH_FUNC_APPEND, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	// Bulk collection mutations — one collection arg (list for
	// append/insert, map for put), read raw by the runtime
	// (AS_CDT_PARAM_STORAGE). The arg's LIST / MAP pin lives in
	// cdt_pf_resolve_op_and_types (it needs the leaf / container
	// context), so the slot stays AUTO here like the other PATH rows.
	{ "appendItems", AST_PATH_FUNC_APPEND_ITEMS, AEL_FAM_PATH, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "insertItems", AST_PATH_FUNC_INSERT_ITEMS, AEL_FAM_PATH, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "putItems", AST_PATH_FUNC_PUT_ITEMS, AEL_FAM_PATH, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	{ "updateItems", AST_PATH_FUNC_UPDATE_ITEMS, AEL_FAM_PATH, false, 1, 1,
			AST_ETYPE_AUTO, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
	// join(separator) — LIST -> STR. One positional separator; the runtime
	// validates it is a string (etype owned by the finalizer, so AUTO).
	{ "join", AST_PATH_FUNC_JOIN, AEL_FAM_PATH, false, 1, 1, AST_ETYPE_AUTO,
			{ { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },

	//------------------------------------------------
	// STRING method-style ops -- STRING receiver, bare/value only.
	// Concrete slot etypes (INT/STR) so the resolver type-checks args; params are
	// in wire-arg order. replace/replaceAll and regexReplace/`=~` are wired
	// separately (list-arg / regex-literal).

	// Reads.
	{ "strlen", AST_PATH_FUNC_STR_LENGTH, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_INT, { { 0 } } },
	{ "substr", AST_PATH_FUNC_STR_SUBSTR, AEL_FAM_STR, false, 2, 1, AST_ETYPE_STR,
			{ { AEL_PNAME_FROM, AST_ETYPE_INT },
					{ AEL_PNAME_TO, AST_ETYPE_INT } } },
	{ "find", AST_PATH_FUNC_STR_INDEX_OF, AEL_FAM_STR, false, 2, 1, AST_ETYPE_INT,
			{ { AEL_PNAME_NEEDLE, AST_ETYPE_STR },
					{ AEL_PNAME_OCCURRENCE, AST_ETYPE_INT } } },
	{ "charAt", AST_PATH_FUNC_STR_CHAR_AT, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_STR, { { AEL_PNAME_INDEX, AST_ETYPE_INT } } },
	// The spec labels this one and leaves startsWith / endsWith positional,
	// so a single argument does not by itself imply positional here.
	{ "contains", AST_PATH_FUNC_STR_CONTAINS, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_TRILEAN, { { AEL_PNAME_NEEDLE, AST_ETYPE_STR } } },
	{ "startsWith", AST_PATH_FUNC_STR_STARTS_WITH, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_TRILEAN, { { AEL_PNAME_NONE, AST_ETYPE_STR } } },
	{ "endsWith", AST_PATH_FUNC_STR_ENDS_WITH, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_TRILEAN, { { AEL_PNAME_NONE, AST_ETYPE_STR } } },
	// toInt / toFloat are the polymorphic path-casts above; their string-parse
	// form is reached through ael_dispatch_to_cast, not by name from here.
	{ "bytesLength", AST_PATH_FUNC_STR_BYTES_LENGTH, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_INT, { { 0 } } },
	{ "isNumeric", AST_PATH_FUNC_STR_IS_NUMERIC, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_TRILEAN, { { 0 } } },
	{ "isUpper", AST_PATH_FUNC_STR_IS_UPPER, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_TRILEAN, { { 0 } } },
	{ "isLower", AST_PATH_FUNC_STR_IS_LOWER, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_TRILEAN, { { 0 } } },
	{ "toBlob", AST_PATH_FUNC_STR_TO_BLOB, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_BLOB, { { 0 } } },
	{ "split", AST_PATH_FUNC_STR_SPLIT, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_LIST, { { AEL_PNAME_NONE, AST_ETYPE_STR } } },
	{ "b64Decode", AST_PATH_FUNC_STR_FROM_BASE64, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_BLOB, { { 0 } } },

	// Modifies — MODIFY_LOCAL: produce a new value, do not persist.
	// splice: insert a substring at an offset. Named "splice" to avoid the CDT
	// insert verb -- a string op named "insert" collides with it and is
	// shadowed (unreachable). Provisional pending a product naming decision.
	{ "splice", AST_PATH_FUNC_STR_INSERT, AEL_FAM_STR, false, 2, 2, AST_ETYPE_STR,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_STR } } },
	{ "overwrite", AST_PATH_FUNC_STR_OVERWRITE, AEL_FAM_STR, false, 2, 2,
			AST_ETYPE_STR,
			{ { AEL_PNAME_OFFSET, AST_ETYPE_INT },
					{ AEL_PNAME_VALUE, AST_ETYPE_STR } } },
	{ "snip", AST_PATH_FUNC_STR_SNIP, AEL_FAM_STR, false, 2, 1, AST_ETYPE_STR,
			{ { AEL_PNAME_FROM, AST_ETYPE_INT },
					{ AEL_PNAME_TO, AST_ETYPE_INT } } },
	// replace / replaceAll — args wrapped into one [find, replace] list
	// (ael_build_str_list), read by string_parse_list.
	{ "replace", AST_PATH_FUNC_STR_REPLACE, AEL_FAM_STR, false, 2, 2,
			AST_ETYPE_STR,
			{ { AEL_PNAME_FIND, AST_ETYPE_STR },
					{ AEL_PNAME_REPLACE, AST_ETYPE_STR } } },
	{ "replaceAll", AST_PATH_FUNC_STR_REPLACE_ALL, AEL_FAM_STR, false, 2, 2,
			AST_ETYPE_STR,
			{ { AEL_PNAME_FIND, AST_ETYPE_STR },
					{ AEL_PNAME_REPLACE, AST_ETYPE_STR } } },
	// regexReplace(pattern: /re/flags, replace: str). pattern is a regex
	// literal (AUTO slot; ael_build_regex_replace validates AST_REGEX_LIT).
	{ "regexReplace", AST_PATH_FUNC_STR_REGEX_REPLACE, AEL_FAM_STR, false, 2, 2,
			AST_ETYPE_STR,
			{ { AEL_PNAME_PATTERN, AST_ETYPE_AUTO },
					{ AEL_PNAME_REPLACE, AST_ETYPE_STR } } },
	{ "upper", AST_PATH_FUNC_STR_UPPERCASE, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "lower", AST_PATH_FUNC_STR_LOWERCASE, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "caseFold", AST_PATH_FUNC_STR_CASEFOLD, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "normalizeNFC", AST_PATH_FUNC_STR_NORMALIZE, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "trim", AST_PATH_FUNC_STR_TRIM, AEL_FAM_STR, false, 0, 0, AST_ETYPE_STR,
			{ { 0 } } },
	{ "trimStart", AST_PATH_FUNC_STR_TRIM_START, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "trimEnd", AST_PATH_FUNC_STR_TRIM_END, AEL_FAM_STR, false, 0, 0,
			AST_ETYPE_STR, { { 0 } } },
	{ "padStart", AST_PATH_FUNC_STR_PAD_START, AEL_FAM_STR, false, 2, 2,
			AST_ETYPE_STR,
			{ { AEL_PNAME_LENGTH, AST_ETYPE_INT },
					{ AEL_PNAME_PAD, AST_ETYPE_STR } } },
	{ "padEnd", AST_PATH_FUNC_STR_PAD_END, AEL_FAM_STR, false, 2, 2, AST_ETYPE_STR,
			{ { AEL_PNAME_LENGTH, AST_ETYPE_INT },
					{ AEL_PNAME_PAD, AST_ETYPE_STR } } },
	{ "repeat", AST_PATH_FUNC_STR_REPEAT, AEL_FAM_STR, false, 1, 1,
			AST_ETYPE_STR, { { AEL_PNAME_NONE, AST_ETYPE_INT } } },

	//------------------------------------------------
	// Cross-type conversion — toString() on an INT / FLOAT / BLOB receiver;
	// zero args, result STR. A unary op like toInt / toFloat (EXP_TO_STRING).
	{ "toString", AST_PATH_FUNC_CAST_STRING, AEL_FAM_PATH, false, 0, 0,
			AST_ETYPE_AUTO, { { 0 } } },

	// modify(expr) — apply the body to each matched element. The body is one
	// positional expr, parsed in the filter scope pushed by method_open
	// (AEL_FAM_MODIFY) so its loop vars (@ / @key / @index) resolve in
	// isolation; :noFail rides the generic :PROPERTY postfix. The resolver
	// pops the scope and builds the SELECT-apply node via ael_new_modify.
	{ "modify", AST_PATH_FUNC_MODIFY, AEL_FAM_MODIFY, false, 1, 1,
			AST_ETYPE_AUTO_CDT, { { AEL_PNAME_NONE, AST_ETYPE_AUTO } } },
};

#define FUNC_TABLE_COUNT (sizeof(FUNC_TABLE) / sizeof(FUNC_TABLE[0]))

//==========================================================
// Parameter-name vocabulary.
//

static const struct {
	ael_pname_t name;
	const char* str;
} PNAME_TABLE[] = {
	{ AEL_PNAME_OFFSET, "offset" },
	{ AEL_PNAME_SIZE, "size" },
	{ AEL_PNAME_VALUE, "value" },
	{ AEL_PNAME_SIGNED, "signed" },
	{ AEL_PNAME_SHIFT, "shift" },
	{ AEL_PNAME_BYTE_OFFSET, "byteOffset" },
	{ AEL_PNAME_BYTE_SIZE, "byteSize" },
	{ AEL_PNAME_INDEX_BITS, "indexBits" },
	{ AEL_PNAME_MIN_HASH_BITS, "minHashBits" },
	{ AEL_PNAME_FROM, "from" },
	{ AEL_PNAME_TO, "to" },
	{ AEL_PNAME_NEEDLE, "needle" },
	{ AEL_PNAME_INDEX, "index" },
	{ AEL_PNAME_OCCURRENCE, "occurrence" },
	{ AEL_PNAME_LENGTH, "length" },
	{ AEL_PNAME_PAD, "pad" },
	{ AEL_PNAME_FIND, "find" },
	{ AEL_PNAME_REPLACE, "replace" },
	{ AEL_PNAME_PATTERN, "pattern" },
	{ AEL_PNAME_BASE, "base" },
	{ AEL_PNAME_EXPONENT, "exponent" },
	{ AEL_PNAME_X, "x" },
};

#define PNAME_TABLE_COUNT (sizeof(PNAME_TABLE) / sizeof(PNAME_TABLE[0]))

// PNAME_TABLE rows sit at [tag - 1] in enum order -- ael_pname_str indexes
// directly (asserting per row) instead of scanning.
COMPILER_ASSERT(PNAME_TABLE_COUNT == AEL_PNAME_X);

//==========================================================
// Public API.
//

const ael_func_spec_t*
ael_func_lookup(const char* name, uint32_t sz)
{
	for (size_t i = 0; i < FUNC_TABLE_COUNT; i++) {
		if (ael_name_eq(FUNC_TABLE[i].name, name, sz)) {
			return &FUNC_TABLE[i];
		}
	}

	return NULL;
}

const ael_func_spec_t*
ael_func_table_rows(uint32_t* count)
{
	*count = (uint32_t)FUNC_TABLE_COUNT;

	return FUNC_TABLE;
}

// Cap for the edit-distance DP; every function name is well under this, and a
// longer typed identifier just yields no suggestion.
#define AEL_SUGGEST_MAX_LEN 24

// Levenshtein edit distance, returning AEL_SUGGEST_MAX_LEN + 1 ("too far") if
// either string exceeds the DP bound. Used only to rank suggestions.
static uint32_t
ael_edit_distance(const char* a, uint32_t a_sz, const char* b, uint32_t b_sz)
{
	if (a_sz > AEL_SUGGEST_MAX_LEN || b_sz > AEL_SUGGEST_MAX_LEN) {
		return AEL_SUGGEST_MAX_LEN + 1;
	}

	uint32_t prev[AEL_SUGGEST_MAX_LEN + 1];
	uint32_t curr[AEL_SUGGEST_MAX_LEN + 1];

	for (uint32_t j = 0; j <= b_sz; j++) {
		prev[j] = j;
	}

	for (uint32_t i = 1; i <= a_sz; i++) {
		curr[0] = i;

		for (uint32_t j = 1; j <= b_sz; j++) {
			uint32_t cost = a[i - 1] == b[j - 1] ? 0 : 1;
			uint32_t del = prev[j] + 1;
			uint32_t ins = curr[j - 1] + 1;
			uint32_t sub = prev[j - 1] + cost;
			uint32_t best = del < ins ? del : ins;

			curr[j] = best < sub ? best : sub;
		}

		for (uint32_t j = 0; j <= b_sz; j++) {
			prev[j] = curr[j];
		}
	}

	return prev[b_sz];
}

const char*
ael_func_suggest(const char* name, uint32_t sz)
{
	const char* best = NULL;
	uint32_t best_dist = AEL_SUGGEST_MAX_LEN + 1;

	for (size_t i = 0; i < FUNC_TABLE_COUNT; i++) {
		const char* cand = FUNC_TABLE[i].name;
		uint32_t dist = ael_edit_distance(name, sz, cand, (uint32_t)strlen(cand));

		if (dist < best_dist) {
			best_dist = dist;
			best = cand;
		}
	}

	// Suggest only when genuinely close -- within a third of the typed length
	// (min 1) -- so a random unknown name gets no nonsense "did you mean".
	uint32_t threshold = sz / 3 < 1 ? 1 : sz / 3;

	return best_dist <= threshold ? best : NULL;
}

ael_pname_t
ael_pname_match(const char* name, uint32_t sz)
{
	for (size_t i = 0; i < PNAME_TABLE_COUNT; i++) {
		if (ael_name_eq(PNAME_TABLE[i].str, name, sz)) {
			return PNAME_TABLE[i].name;
		}
	}

	return AEL_PNAME_NONE;
}

const char*
ael_pname_str(ael_pname_t name)
{
	if (name == AEL_PNAME_NONE || (size_t)name > PNAME_TABLE_COUNT) {
		return "?";
	}

	const size_t i = (size_t)name - 1;

	cf_assert(PNAME_TABLE[i].name == name, AS_EXP,
			"PNAME_TABLE order mismatch at %zu", i);
	return PNAME_TABLE[i].str;
}
