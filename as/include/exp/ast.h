/*
 * ast.h
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
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "log.h"

#include "base/cdt_wire.h"
#include "base/proto.h"
#include "exp/ael_diag.h"
#include "exp/exp_wire.h"

// Naming: use `sz` (optionally qualified, e.g. name_sz) for byte counts;
// use `count` (e.g. list.count, defs_count) for numbers of elements.

//==========================================================
// Typedefs & constants.
//

typedef enum ast_node_e {
	AST_NIL = 0,

	// N-ary logical (children chained via each child's `next`; parent `next` is first).
	AST_AND,
	AST_OR,

	// Unary logical.
	AST_NOT,

	// Exclusive (n-ary).
	AST_EXCLUSIVE,

	// Comparison.
	AST_CMP_EQ,
	AST_CMP_NE,
	AST_CMP_GT,
	AST_CMP_GE,
	AST_CMP_LT,
	AST_CMP_LE,
	AST_CMP_IN,

	// Geospatial bidirectional containment test — both sides must be
	// GEOJSON. Emits as EXP_CMP_GEO; runtime build_cmp_geo enforces
	// the type at decode time.
	AST_CMP_GEO,

	// Arithmetic.
	AST_ADD,
	AST_SUB,
	AST_MUL,
	AST_DIV,
	AST_MOD,
	AST_POW,

	// Bitwise.
	AST_BIT_AND,
	AST_BIT_OR,
	AST_BIT_XOR,
	AST_BIT_NOT,
	AST_LSHIFT,
	AST_RSHIFT_ARITH,
	AST_RSHIFT_LOGIC,

	// Literals.
	AST_INT,
	AST_FLOAT,
	AST_STRING,
	AST_BOOL,
	AST_BLOB,
	// Base64 sibling of AST_BLOB (b64'...' / B64'...'): stores base64
	// source text (u.str offset+sz), decoded to the same AS_BYTES_BLOB
	// wire form at emit.
	AST_B64_BLOB,
	// GeoJSON literal — `geoJson('{"type":"Point",...}')`. Stores the
	// source JSON bytes (u.str: offset+sz into input). Codegen emits as
	// an AS_BYTES_GEOJSON-typed msgpack value, which the runtime
	// auto-translates to EXP_VOP_VALUE_GEO at parse time.
	AST_GEO_LITERAL,
	AST_INF,
	AST_WILDCARD,
	// Typed numeric zero synthesized for unary minus: `-x` lowers to
	// AST_SUB(AST_ZERO, x). Resolves to INT or FLOAT via the SUB and
	// emits 0 / 0.0 accordingly.
	AST_ZERO,

	// Collections.
	AST_LIST,
	AST_MAP,

	// Bin / path call.
	AST_BIN,
	AST_BIN_REF,
	AST_BIN_TYPE,
	AST_BIN_EXISTS,
	AST_PATH_CTX,
	AST_PATH_CALL,

	// Metadata.
	AST_META,

	// Variable.
	AST_VAR,

	// With/do and when/default.
	AST_LET,
	AST_WHEN,

	// Functions.
	AST_FUNC_ABS,
	AST_FUNC_CEIL,
	AST_FUNC_FLOOR,
	AST_FUNC_LOG,
	AST_FUNC_POW,
	AST_FUNC_MAX,
	AST_FUNC_MIN,
	AST_FUNC_COUNT_ONE_BITS,
	AST_FUNC_FIND_BIT_LEFT,
	AST_FUNC_FIND_BIT_RIGHT,

	// Path functions.
	AST_PATH_FUNC_EXISTS,
	AST_PATH_FUNC_COUNT,
	AST_PATH_FUNC_CAST_INT,
	AST_PATH_FUNC_CAST_FLOAT,
	AST_PATH_FUNC_CAST_STRING,
	AST_PATH_FUNC_GET,
	AST_PATH_FUNC_GET_KEYS,
	AST_PATH_FUNC_GET_KEY_VALUES,
	AST_PATH_FUNC_GET_TREE,
	AST_PATH_FUNC_GET_INDEXES,
	AST_PATH_FUNC_GET_RANKS,
	AST_PATH_FUNC_GET_MAPS,
	AST_PATH_FUNC_TYPE,

	// LIST join — whole-list read taking a scalar separator, returning STR.
	// Rides the CDT machinery (op AS_CDT_OP_STRING_LIST_JOIN) but is the
	// lone read-with-arg-returning-STR shape; op + STR result are forced
	// in cdt_pf_resolve_op_and_types (no leaf consumed).
	AST_PATH_FUNC_JOIN,

	// BLOB bit functions — transient pf_types built by `bit_fn` grammar
	// reductions. The wrapping `operand ::= bit_recv . bit_fn` rule
	// morphs these to AST_BIT_OP via ael_finalize_bit_call. Modify-flagged
	// entries (in ast_node_table) get EXP_CALL_FLAG_MODIFY_LOCAL on the
	// wrapping path-call's stype.
	AST_PATH_FUNC_BIT_GET,
	AST_PATH_FUNC_BIT_B64_ENCODE,
	AST_PATH_FUNC_BIT_COUNT,
	AST_PATH_FUNC_BIT_LSCAN,
	AST_PATH_FUNC_BIT_RSCAN,
	AST_PATH_FUNC_BIT_GET_INT,
	AST_PATH_FUNC_BIT_SET,
	AST_PATH_FUNC_BIT_OR,
	AST_PATH_FUNC_BIT_XOR,
	AST_PATH_FUNC_BIT_AND,
	AST_PATH_FUNC_BIT_NOT,
	AST_PATH_FUNC_BIT_LSHIFT,
	AST_PATH_FUNC_BIT_RSHIFT,
	AST_PATH_FUNC_BIT_ADD,
	AST_PATH_FUNC_BIT_SUBTRACT,
	AST_PATH_FUNC_BIT_SET_INT,
	AST_PATH_FUNC_BIT_RESIZE,
	AST_PATH_FUNC_BIT_INSERT,
	AST_PATH_FUNC_BIT_REMOVE,

	// HLL functions — transient pf_types built by `hll_fn` grammar
	// reductions. Morph to AST_HLL_OP via ael_finalize_hll_call. Modify
	// entries get EXP_CALL_FLAG_MODIFY_LOCAL on the wrapping path-call.
	AST_PATH_FUNC_HLL_COUNT,
	AST_PATH_FUNC_HLL_DESCRIBE,
	AST_PATH_FUNC_HLL_MAY_CONTAIN,
	AST_PATH_FUNC_HLL_UNION,
	AST_PATH_FUNC_HLL_UNION_COUNT,
	AST_PATH_FUNC_HLL_INTERSECT_COUNT,
	AST_PATH_FUNC_HLL_SIMILARITY,
	AST_PATH_FUNC_HLL_INIT,
	AST_PATH_FUNC_HLL_ADD,

	// STRING functions -- transient pf_types morphed to AST_STR_OP by
	// ael_finalize_str_call, which emits stype EXP_CALL_STRING. Modify ops
	// (AST_NF_MODIFY) get MODIFY_LOCAL: they produce a new value as the result,
	// they don't persist. Bare/value receiver only; the receiver is pinned STR.
	AST_PATH_FUNC_STR_LENGTH,
	AST_PATH_FUNC_STR_SUBSTR,
	AST_PATH_FUNC_STR_INDEX_OF,
	AST_PATH_FUNC_STR_CHAR_AT,
	AST_PATH_FUNC_STR_CONTAINS,
	AST_PATH_FUNC_STR_STARTS_WITH,
	AST_PATH_FUNC_STR_ENDS_WITH,
	AST_PATH_FUNC_STR_TO_INT,
	AST_PATH_FUNC_STR_TO_FLOAT,
	AST_PATH_FUNC_STR_BYTES_LENGTH,
	AST_PATH_FUNC_STR_IS_NUMERIC,
	AST_PATH_FUNC_STR_IS_UPPER,
	AST_PATH_FUNC_STR_IS_LOWER,
	AST_PATH_FUNC_STR_TO_BLOB,
	AST_PATH_FUNC_STR_SPLIT,
	AST_PATH_FUNC_STR_FROM_BASE64,
	AST_PATH_FUNC_STR_REGEX_COMPARE, // read REGEX_COMPARE — lowered from `=~`
	AST_PATH_FUNC_STR_INSERT,
	AST_PATH_FUNC_STR_OVERWRITE,
	AST_PATH_FUNC_STR_SNIP,
	AST_PATH_FUNC_STR_REPLACE,
	AST_PATH_FUNC_STR_REPLACE_ALL,
	AST_PATH_FUNC_STR_UPPERCASE,
	AST_PATH_FUNC_STR_LOWERCASE,
	AST_PATH_FUNC_STR_CASEFOLD,
	AST_PATH_FUNC_STR_NORMALIZE,
	AST_PATH_FUNC_STR_TRIM_START,
	AST_PATH_FUNC_STR_TRIM_END,
	AST_PATH_FUNC_STR_TRIM,
	AST_PATH_FUNC_STR_PAD_START,
	AST_PATH_FUNC_STR_PAD_END,
	AST_PATH_FUNC_STR_REPEAT,

	AST_PATH_FUNC_STR_REGEX_REPLACE,

	// The string `+` operator's concat lowering op. A resolved-STR `+` chain
	// lowers (ael_lower_str_add) to a fold of AS_STRING_OP_APPEND calls, each a
	// scalar-arg MODIFY_LOCAL string op, so dynamic operands ride as
	// sub-expressions. Built by the lowering, not a func-table method name --
	// deliberately outside the AEL_IS_STR_TYPE range.
	AST_PATH_FUNC_STR_APPEND,

	// Map/List singular access modifiers.
	AST_MAP_KEY,
	AST_MAP_VALUE,
	AST_MAP_INDEX,
	AST_MAP_RANK,
	AST_LIST_INDEX,
	AST_LIST_VALUE,
	AST_LIST_RANK,

	// Map/List range segments (u.binary: left=start, right=end/count or NULL).
	AST_MAP_KEY_RANGE,
	AST_MAP_INDEX_RANGE,
	AST_MAP_VALUE_RANGE,
	AST_MAP_RANK_RANGE,
	AST_LIST_INDEX_RANGE,
	AST_LIST_VALUE_RANGE,
	AST_LIST_RANK_RANGE,

	// Relative range segments — like ranges but with an extra
	// "relative-to" operand. u.rel_range_seg. Wire format:
	// (rtype, relative_to, start, count?). count is end-start+1; omitted
	// for open-end. start/end must be static AST_INT.
	AST_MAP_INDEX_REL_RANGE, // {start..end~key} → BY_KEY_REL_INDEX_RANGE
	AST_MAP_RANK_REL_RANGE, // {#start..end~val} → BY_VALUE_REL_RANK_RANGE
	AST_LIST_RANK_REL_RANGE, // [#start..end~val] → LIST BY_VALUE_REL_RANK_RANGE

	// Map/List list segments (u.list.head/tail/count; values chained by next).
	AST_MAP_KEY_LIST,
	AST_MAP_VALUE_LIST,
	AST_LIST_VALUE_LIST,

	// Modify path functions. A few user-facing verb names differ from these
	// identifiers (kept stable to avoid churn): SET = setTo, INCREMENT =
	// add. UPDATE and the *_ITEMS bulk ops are the spec's newer mutation
	// verbs. All stay in the REMOVE..SORT range so AEL_IS_PATH_FN_TYPE
	// covers them.
	AST_PATH_FUNC_REMOVE,
	AST_PATH_FUNC_SET,
	AST_PATH_FUNC_INSERT,
	AST_PATH_FUNC_UPDATE,
	AST_PATH_FUNC_APPEND,
	AST_PATH_FUNC_APPEND_ITEMS,
	AST_PATH_FUNC_INSERT_ITEMS,
	AST_PATH_FUNC_PUT_ITEMS,
	AST_PATH_FUNC_UPDATE_ITEMS,
	AST_PATH_FUNC_INCREMENT,
	AST_PATH_FUNC_CLEAR,
	AST_PATH_FUNC_SORT,

	// Path-expression wildcard segment. With no filter, iterates all
	// children at this level (`*`). With a filter `*[?(filter)]`,
	// u.by_exp_seg.filter holds the filter sub-expression — codegen
	// embeds it as a separate as_exp blob in the SELECT wire form.
	AST_WILDCARD_SEG,

	// Path-expression AND_EXP post-filter segment: `&[?(filter)]`.
	// Attaches an AS_CDT_CTX_AND|AS_CDT_CTX_EXP ctx-pair to the
	// preceding seg's selection. u.by_exp_seg.filter holds the filter
	// sub-expression (mandatory; no bare-`&` form).
	AST_AND_EXP_SEG,

	// Loop variable: @, @key, @index — bound by SELECT during iteration.
	// u.loop_var.builtin selects which builtin (AS_EXP_BUILTIN_*).
	AST_LOOP_VAR,

	// Path-expression .select() function — multi-element extract over a
	// path containing wildcards. u.modify carries sel_type (one of
	// the SELECT_* constants from cdt.c) and a noFail flag. Codegen
	// routes to emit_select_call (separate from the regular CDT op
	// path so the wire emits a single AS_CDT_OP_SELECT instead of a
	// CONTEXT_EVAL chain).
	AST_PATH_FUNC_SELECT,

	// .modify(expr [, noFail: B]) — applies the embedded expression to
	// each element matched by the wildcard path. u.modify holds value
	// (the apply expr), sel_type = AS_CDT_SELECT_APPLY, and props
	// (NO_FAIL bit). Codegen emits the SELECT_APPLY wire form with the
	// apply expression as a third element after the flags-int.
	AST_PATH_FUNC_MODIFY,

	// Wildcard-path .remove() — synthesizes a SELECT_APPLY with a
	// canned removeResult() body (single op = EXP_RESULT_REMOVE).
	// Distinct from AST_PATH_FUNC_REMOVE (single-element CDT remove).
	AST_PATH_FUNC_PSELECT_REMOVE,

	// Resolved cdt_op — ael_cdt_op_from_fn morphs an AST_PATH_FUNC_* node
	// to this once op_code/rtype/etype/chain are set.
	AST_CDT_OP,

	// Resolved bit_op — ael_finalize_bit_call morphs an
	// AST_PATH_FUNC_BIT_* node to this. Shares the u.cdt_op layout
	// (op_code + arg chain via head/count). The wrapping AST_PATH_CALL
	// holds the receiver in u.call.ctx (BITS reuses that slot for the
	// single receiver expression; there's no AST_PATH_CTX list for BITS).
	AST_BIT_OP,

	// Resolved hll_op — ael_finalize_hll_call morphs an
	// AST_PATH_FUNC_HLL_* node to this. Shares the u.cdt_op layout with
	// AST_BIT_OP; the wrapping AST_PATH_CALL holds the receiver in
	// u.call.ctx (single value-producing receiver, no AST_PATH_CTX list).
	AST_HLL_OP,

	// Resolved str_op — ael_finalize_str_call morphs an
	// AST_PATH_FUNC_STR_* node to this. Shares the u.cdt_op layout with
	// AST_BIT_OP / AST_HLL_OP; op_code is an AS_STRING_OP_*. The wrapping
	// AST_PATH_CALL carries stype EXP_CALL_STRING and holds the STR-pinned
	// receiver in u.call.ctx (no AST_PATH_CTX list — bare/value receiver).
	AST_STR_OP,

	// Var definition (for 'with').
	AST_LET_LIST,
	AST_VAR_DEF,
	AST_LET_SCOPE,

	// Expression mapping (for 'when').
	AST_CASE,

	// Transient function-call arguments — produced by the arg_list grammar
	// and consumed immediately by the function-call resolver
	// (ael_resolve_*). Never linked into the final tree or seen by codegen.
	// AST_ARG uses u.arg (name + value); AST_ARG_LIST uses u.list.
	AST_ARG,
	AST_ARG_LIST,

	// Transient regex literal for a `pattern:` named arg (regexReplace).
	// Carries the raw pattern string + flags int in u.binary.left / .right;
	// consumed by ael_build_regex_replace. Never linked into the final tree
	// or seen by codegen.
	AST_REGEX_LIT,

	// Unknown / error.
	AST_UNKNOWN,

	AST_NODE_TYPE_COUNT
} __attribute__((packed)) ast_node_t;

typedef enum ast_etype_e {
	AST_ETYPE_ERROR = 0,
	AST_ETYPE_NIL = 1 << EXP_RTYPE_NIL,
	AST_ETYPE_TRILEAN = 1 << EXP_RTYPE_TRILEAN,
	AST_ETYPE_INT = 1 << EXP_RTYPE_INT,
	AST_ETYPE_STR = 1 << EXP_RTYPE_STR,
	AST_ETYPE_LIST = 1 << EXP_RTYPE_LIST,
	AST_ETYPE_MAP = 1 << EXP_RTYPE_MAP,
	AST_ETYPE_AUTO_CDT = AST_ETYPE_MAP | AST_ETYPE_LIST,
	AST_ETYPE_BLOB = 1 << EXP_RTYPE_BLOB,
	AST_ETYPE_AUTO_KEY = AST_ETYPE_INT | AST_ETYPE_STR | AST_ETYPE_BLOB,
	AST_ETYPE_FLOAT = 1 << EXP_RTYPE_FLOAT,
	AST_ETYPE_AUTO_NUMERIC = AST_ETYPE_INT | AST_ETYPE_FLOAT,
	// The scalar STRING-or-numeric set, with two clients. (1) `+`: numeric add
	// OR string concat — a resolved-STR AST_ADD is lowered to the concat fold.
	// (2) toInt() / toFloat(): the polymorphic cast operand — a resolved-STR
	// operand parses, a numeric one casts (ael_dispatch_to_cast). Either way,
	// inference resolves it; if it stays multi-bit (all-bin chain, no pin) it
	// errors at finalize like any unresolved type.
	AST_ETYPE_AUTO_ADD = AST_ETYPE_STR | AST_ETYPE_INT | AST_ETYPE_FLOAT,
	// toString() receiver set (per spec): INT / FLOAT / BLOB. REPR eval
	// dispatches on the runtime particle type, so pinning to this mask only
	// rejects non-convertible receivers (LIST / MAP / ...) at parse time.
	AST_ETYPE_AUTO_REPR = AST_ETYPE_INT | AST_ETYPE_FLOAT | AST_ETYPE_BLOB,
	AST_ETYPE_GEOJSON = 1 << EXP_RTYPE_GEOJSON,
	AST_ETYPE_HLL = 1 << EXP_RTYPE_HLL,
	AST_ETYPE_AUTO = (1 << EXP_RTYPE_INPUT_END) - 1,
} __attribute__((packed)) ast_etype;

// The container order named by a create-order suffix, held on the element the
// suffix was written on -- unlike the AST_PROP_CR_* bit, which travels one
// element toward the leaf to reach the position the wire orders from.
//
// Not a bitmask: one order per element, and which spelling a value stands for
// depends on the element's etype, the way the wire's shared unordered value
// does. UNSORTED_PAD is UNORDERED here -- padding is not an order, and the bit
// asking for it stays in props, which is what codegen reads.
typedef enum ast_esubtype_e {
	AST_ESUBTYPE_UNSET = 0,
	AST_ESUBTYPE_UNORDERED = 1, // list UNSORTED / UNSORTED_PAD, map UNORDERED
	AST_ESUBTYPE_ORDERED = 2, // list SORTED, map KEY_ORDERED
	AST_ESUBTYPE_KEY_VALUE_ORDERED = 3, // map only
} __attribute__((packed)) ast_esubtype;

// A type is resolved when exactly one bit is set (concrete type).
static inline bool
ast_type_resolved(ast_etype etype)
{
	return etype != 0 && (etype & (etype - 1)) == 0;
}

// Convert ast_etype bitmask to exp_rtype index (for wire format).
// Assumes etype is resolved (single bit set).
static inline exp_rtype
ast_etype_to_rtype(ast_etype etype)
{
	return (exp_rtype)__builtin_ctz(etype);
}

// Convert exp_rtype index to ast_etype bitmask.
static inline ast_etype
ast_rtype_to_etype(exp_rtype r)
{
	return (ast_etype)(1 << r);
}

// Node info table flags.
enum {
	AST_NF_MAP_SEG = 0x01,
	AST_NF_PLURAL = 0x02,
	AST_NF_LIST_SEG = 0x04,
	AST_NF_MODIFY = 0x08,
	AST_NF_MOD_TARGET = 0x10,
	AST_NF_REL_RANGE = 0x20, // u.rel_range_seg layout (extra operand)
	AST_NF_BY_EXP = 0x40, // wildcard `*` / `*[?(filter)]`; AS_CDT_CTX_EXP wire
	AST_NF_AND_EXP = 0x80, // `&[?(filter)]`; AS_CDT_CTX_AND|AS_CDT_CTX_EXP wire
	// Modify op that must be anchored on a leaf seg (setTo / insert / update /
	// add).
	AST_NF_NEEDS_LEAF = 0x100,
	// Whole-collection op — takes no leaf seg (append / appendItems /
	// putItems / clear / sort / join).
	AST_NF_WHOLE_COLL = 0x200,
	// Post-parse requires a single concrete etype here. Both emitters read
	// this node's wire rtype from its etype and neither can invent one, so a
	// multi-bit etype is the author's to disambiguate with `:T`.
	AST_NF_NEEDS_ETYPE = 0x400,
};

// Property bitmask — flags that attach to a node via the `:PROPERTY`
// postfix grammar. Each single-bit value corresponds to one property
// constant; ael_apply_prop validates against a per-attachment "valid"
// mask, detects duplicates, and enforces mutual-exclusion within group
// masks (e.g. only one CR_LIST_* per segment). Mirrors the
// packed-enum pattern used by ast_etype — 2 bytes wide, all single-
// bit and multi-bit values defined together so debuggers and grep
// land in one place.
typedef enum ast_prop_bits_e {
	// Bit 0 = "unordered", two uses on different node types: getMaps():UNORDERED
	// return shape (-> UNORDERED_MAP; bare getMaps() -> ORDERED_MAP), and a
	// create-seg `:UNORDERED` order (create-if-missing, unordered). (Bit freed
	// when AST_PROP_LOCAL was removed — local-ness is now structural.)
	AST_PROP_UNORDERED = 1 << 0,

	// Modify-op flags.
	AST_PROP_NO_FAIL = 1 << 1,
	AST_PROP_CREATE_ONLY = 1 << 2,
	AST_PROP_UPDATE_ONLY = 1 << 3,
	AST_PROP_PARTIAL = 1 << 4,
	// Map write modes, preset by the insert / update / insertItems / updateItems
	// verbs -- these two carry no `:FLAG` spelling of their own.
	AST_PROP_NO_OVERWRITE = 1 << 5,
	AST_PROP_NO_CREATE = 1 << 6,
	// list sort() drop-duplicates option (`:DROP_DUPS`). Claims the last
	// free low bit; a future HLL ALLOW_FOLD (also earmarked here) will need
	// the enum widened or a family-shared bit.
	AST_PROP_DROP_DUPS = 1 << 7,

	// List modify-op flag: ADD_UNIQUE fails (or skips, under NO_FAIL) an element
	// already present. Map writes have no equivalent -- their modes ride on the
	// verb-preset NO_OVERWRITE / NO_CREATE. Bit 1 << 11 is free.
	//
	// There is deliberately no INSERT_BOUNDED property. AEL emits the wire's
	// bounded flag by default on every list write that could nil-pad, so the
	// only knob is the opt-out, :UNSORTED_PAD -- see ael_cdt_flag_shape.
	AST_PROP_ADD_UNIQUE = 1 << 8,

	// Path-seg create-order flags: an explicit "create-if-missing with this
	// order" (default = don't create). The container is fixed by the selector
	// (`.k`/`{...}` map, `.[i]` list), so the flag carries no MAP_/LIST_ prefix
	// and is container-validated at apply: SORTED / UNSORTED / UNSORTED_PAD
	// are list-only, KEY_ORDERED / KEY_VALUE_ORDERED / UNORDERED map-only.
	// UNSORTED and UNORDERED share bit 0 -- one wire value serves both
	// containers (AS_CDT_CTX_CREATE_LIST_UNORDERED == ..._MAP_UNORDERED) -- so
	// the spelling, not the bit, is what the container check tests.
	//
	// UNSORTED_PAD opts in to nil-padding a list out to a navigated index;
	// the ctx-create path is bounded by default and errors without it.
	AST_PROP_CR_UNSORTED_PAD = 1 << 9,
	AST_PROP_CR_ORDERED = 1 << 10,
	AST_PROP_CR_KEY_ORDERED = 1 << 12,
	AST_PROP_CR_KEY_VALUE_ORDERED = 1 << 13,
	AST_PROP_PERSIST_INDEX = 1 << 14,

	// Read-getter flag — selects the reverse positional return shape
	// (getIndexes():REVERSE -> REVINDEX, getRanks():REVERSE -> REVRANK).
	AST_PROP_REVERSE = 1 << 15,

	// Create-order group masks. GROUP_CR_ORDER = the mutually-exclusive orders
	// (at most one per seg); the LIST / MAP subsets are the orders valid for
	// each container (the apply-time validator). PERSIST_INDEX is orthogonal
	// (combines with any order) so it sits outside GROUP_CR_ORDER.
	AST_PROP_GROUP_CR_ORDER = AST_PROP_UNORDERED | AST_PROP_CR_ORDERED |
			AST_PROP_CR_UNSORTED_PAD | AST_PROP_CR_KEY_ORDERED |
			AST_PROP_CR_KEY_VALUE_ORDERED,
	AST_PROP_CR_ORDER_LIST =
			AST_PROP_UNORDERED | AST_PROP_CR_ORDERED | AST_PROP_CR_UNSORTED_PAD,
	AST_PROP_CR_ORDER_MAP = AST_PROP_UNORDERED | AST_PROP_CR_KEY_ORDERED |
			AST_PROP_CR_KEY_VALUE_ORDERED,
	AST_PROP_GROUP_CTX_CR = AST_PROP_GROUP_CR_ORDER | AST_PROP_PERSIST_INDEX,

	// Per-attachment valid masks. ael_apply_prop checks `bit & ~valid`.
	//
	// Bit ops accept flags per op, matching bits_op_def.bad_flags in
	// particle_blob.c -- the runtime rejects a disallowed bit with
	// AS_ERR_PARAMETER, so a wider mask here would compile expressions that
	// cannot execute. SIZING = the two ops that change the blob's length,
	// INPLACE = the ones that can be clipped to it, ARITH = the integer ops.
	AST_PROP_VALID_BIT_SIZING =
			AST_PROP_NO_FAIL | AST_PROP_CREATE_ONLY | AST_PROP_UPDATE_ONLY,
	AST_PROP_VALID_BIT_INPLACE =
			AST_PROP_NO_FAIL | AST_PROP_UPDATE_ONLY | AST_PROP_PARTIAL,
	AST_PROP_VALID_BIT_ARITH = AST_PROP_NO_FAIL | AST_PROP_UPDATE_ONLY,
	// HLL modify superset — INIT row uses this directly; ADD row uses
	// NO_FAIL | CREATE_ONLY (no UPDATE_ONLY, since ADD has no
	// create-vs-update mode).
	AST_PROP_VALID_HLL_MODIFY =
			AST_PROP_NO_FAIL | AST_PROP_CREATE_ONLY | AST_PROP_UPDATE_ONLY,
	// CDT write masks. PARTIAL is multi-item only: the single-key map put never
	// reads do_partial, and per-item skipping is meaningless for one item.
	// Rows serving both containers carry the union; the container-exclusive
	// flags are re-checked once the leaf fixes the receiver.
	AST_PROP_VALID_CDT_LIST_MODIFY = AST_PROP_NO_FAIL | AST_PROP_ADD_UNIQUE,
	AST_PROP_VALID_CDT_LIST_ITEMS =
			AST_PROP_VALID_CDT_LIST_MODIFY | AST_PROP_PARTIAL,
	AST_PROP_VALID_CDT_MAP_MODIFY = AST_PROP_NO_FAIL,
	AST_PROP_VALID_CDT_MAP_ITEMS =
			AST_PROP_VALID_CDT_MAP_MODIFY | AST_PROP_PARTIAL,
	AST_PROP_VALID_CDT_MODIFY =
			AST_PROP_VALID_CDT_LIST_MODIFY | AST_PROP_VALID_CDT_MAP_MODIFY,
	AST_PROP_VALID_CDT_ITEMS =
			AST_PROP_VALID_CDT_LIST_ITEMS | AST_PROP_VALID_CDT_MAP_ITEMS,
	// list sort() accepts :DROP_DUPS, plus path tolerance.
	AST_PROP_VALID_CDT_SORT = AST_PROP_DROP_DUPS | AST_PROP_NO_FAIL,
	// clear() takes no op-level flag, only path tolerance.
	AST_PROP_VALID_CDT_CLEAR = AST_PROP_NO_FAIL,
	// A string modify takes path tolerance only, and only when pathed --
	// ael_finalize_str_call refuses it on a bare bin, which has no path.
	AST_PROP_VALID_STR_MODIFY = AST_PROP_NO_FAIL,
	AST_PROP_VALID_SELECT_MODIFY = AST_PROP_NO_FAIL,
	AST_PROP_VALID_PATH_SEG = AST_PROP_GROUP_CTX_CR,
	// Positional getters (getIndexes / getRanks) accept only :REVERSE.
	AST_PROP_VALID_GET_POSITIONAL = AST_PROP_REVERSE,
	// getMaps() accepts only :UNORDERED (unordered-map return shape;
	// the default is ordered).
	AST_PROP_VALID_GET_MAPS = AST_PROP_UNORDERED,
} __attribute__((packed)) ast_prop_bits;

// Node structural kind -- drives generic ast_free / traversal.
typedef enum {
	NK_NONE = 0,
	NK_LEAF,
	NK_UNARY,
	NK_BINARY,
	NK_BMATH, // binary math — propagates type to children

	NK_SEG_S, // single-select path segment (u.seg)
	NK_SEG_M, // multi-select path segment; layout via AST_NF_LIST_SEG flag
	//   set:  u.list_seg
	//   unset: u.range_seg
	NK_LIST,
	NK_NMATH, // n-ary math — circular list via u.nmath
	NK_CDT_OP, // path function nodes (GET/EXISTS/COUNT/REMOVE/SET/INSERT/
	// INCREMENT/APPEND/CLEAR/SORT) using u.cdt_op. The node's
	// own type IS the path-function kind.

} ast_node_kind;

typedef struct {
	const char* name;
	int16_t exp_cmd;
	int16_t ctx_type;
	int16_t cdt_get_op;
	int16_t cdt_remove_op;
	uint16_t flags;
	ast_node_kind kind;
	ast_prop_bits valid_props; // accepted AST_PROP_* bits via :PROPERTY
	ast_etype valid_types; // accepted AST_ETYPE_* bits via :TYPE
} ast_node_info;

extern const ast_node_info ast_node_table[AST_NODE_TYPE_COUNT];

// Index into ast_pool dynmem; not a C pointer.
typedef dynmem_obj_idx ast_ref;

#define AST_REF_NULL ((1u << 24) - 1)

// Explicit ordering override on an AST_LIST / AST_MAP literal, set by the
// `:ORDERED` / `:UNORDERED` postfix. DEFAULT resolves per kind: maps default
// ordered (K_ORDERED + key-sort), lists default unordered (plain).
typedef enum {
	AEL_ORDER_DEFAULT = 0,
	AEL_ORDER_ORDERED,
	AEL_ORDER_UNORDERED,
} ael_order_override;

// Maximum AST_PATH_CTX depth (bin + ctx segs). SELECT (multi-select) paths
// allow up to 64 levels of plain ctx + up to 128 multi-element steps + 1
// terminator; plain CDT eval ops are far below that. 256 is the safe cap.
#define AST_PATH_CTX_MAX 256

// Maximum let-variable definitions per expression and maximum variable-name
// length, enforced at ast_new_var_def. Bounds the O(defs) scope walks on the
// per-transaction parse path and sizes u.var's var_idx:16 / name_sz:16
// fields. AEL-only; the legacy wire runtime keeps its own (unlimited)
// behavior.
#define AEL_VAR_MAX 1024
#define AEL_VAR_NAME_MAX 255

// Canonical nesting cap for every recursive expression traversal — the wire
// build (build_next / build_count_sz), the AEL sizer/builder, and, transitively,
// rt_eval (it can't descend a tree deeper than was built). +/* flatten to
// n-ary; left-associative -, /, %, shifts, comparisons build left-deep trees
// that YYSTACKDEPTH doesn't bound (the parser reduces eagerly), so `1-1-1-...`
// would overflow the C stack. Well above any real expression.
#define EXP_MAX_DEPTH 512

// Abstract Syntax Tree node.
typedef struct ast_node_s {
	union __attribute__((packed)) {
		// AST_INT.
		int64_t ival;

		// AST_FLOAT.
		double fval;

		// AST_BOOL.
		bool bval;

		// AST_STRING, AST_BLOB, AST_B64_BLOB, AST_GEO_LITERAL, AST_VAR:
		// borrowed pointer into input buffer. AST_BLOB holds hex digits,
		// AST_B64_BLOB holds base64 text; binary is emitted at codegen.
		// `sz` is the raw between-quotes
		// byte count (unchanged by escape decoding). `has_escape` is
		// set by ast_new_string_token when the body contains any
		// backslash escape — codegen branches on it to skip a rescan
		// for the no-escape common case. Non-AST_STRING sharers leave
		// has_escape == false via ast_pool_alloc zero-init.
		struct __attribute__((packed)) {
			const char* str;
			uint32_t sz : 31;
			bool has_escape : 1;
		} str;

		// AST_BIN: canonical bin node. The name is at node.offset (see
		// disp_pre in the node header - the display span reaches back over
		// the '$.' prefix), and name_sz is stored because node.sz covers
		// the whole reference.
		// bin_next chains all distinct bins from ctx->bin_root.
		// ref_root is the head of the AST_BIN_REF stack for this bin.
		// allow_unresolved=true marks bins used only by name (.exists()
		// / .type()) — the post-parse strict-typing check skips them.
		// Conflicts (etype = ERROR) still surface regardless.
		struct {
			ast_ref bin_next;
			ast_ref ref_root;
			ast_ref parent : 24;
			uint8_t name_sz : 7; // 1..15 (AS_BIN_NAME_MAX_SZ - 1)
			bool allow_unresolved : 1;
		} bin;

		// AST_BIN_REF: subsequent reference to a canonical AST_BIN.
		// ref_next chains to the next ref in the canonical bin's stack.
		// (allow_unresolved lives only on the canonical AST_BIN — the
		// writers deref to it and the strict-typing pass walks only
		// canonical bins.)
		struct {
			ast_ref bin; // ref to canonical AST_BIN node
			ast_ref ref_next; // next bin_ref in stack, or AST_REF_NULL
			ast_ref parent : 24;
		} bin_ref;

		// AST_BIN_TYPE / AST_BIN_EXISTS: bin queries. The name is at
		// node.offset / name_sz, and the display span (offset/disp_pre/sz)
		// is copied whole from the bin reference. No chain linkage.
		struct {
			uint8_t name_sz; // 1..15 (AS_BIN_NAME_MAX_SZ - 1)
		} bin_type;

		// AST_META (op_code is exp_op_code: EXP_META_* or EXP_REC_KEY).
		// param: wire int64 — digest modulus for EXP_META_DIGEST_MOD, or
		// exp_rtype (key type) for EXP_REC_KEY; otherwise unused (0).
		struct __attribute__((packed)) {
			int64_t param;
			exp_op_code op_code;
		} meta;

		// Binary ops: AST_CMP_*, AST_SUB, AST_DIV, AST_MOD, AST_POW,
		// AST_LSHIFT, AST_RSHIFT_ARITH, AST_RSHIFT_LOGIC.
		struct {
			ast_ref left;
			ast_ref right;
			ast_ref parent;
		} binary;

		// N-ary math: AST_ADD, AST_MUL, AST_FUNC_MAX, AST_FUNC_MIN.
		// Circular list: tail->next == head. Get head via pool_at(tail)->next.
		struct {
			ast_ref tail;
			uint32_t count;
			ast_ref parent;
		} nmath;

		// Unary ops: AST_NOT, AST_BIT_NOT, AST_PATH_FUNC_CAST_INT,
		// AST_PATH_FUNC_CAST_FLOAT (cast wrappers around AST_PATH_CALL or
		// directly around a bin). AST_PATH_FUNC_TYPE never escapes the
		// parser (folded into AST_BIN_TYPE).
		struct {
			ast_ref operand;
		} unary;

		// Singular path segments (AST_MAP_KEY, AST_MAP_VALUE, etc.)
		// `props` carries CR_* create-order / PERSIST_INDEX bits
		// applied via the `:PROPERTY` postfix grammar (e.g.
		// $.l.[0]:UNORDERED).
		// Readers of u.seg must gate on
		// `ast_node_table[type].kind == NK_SEG_S`.
		struct {
			ast_prop_bits props;
			ast_ref operand;
		} seg;

		// Range path segments (AST_MAP_KEY_RANGE, AST_LIST_INDEX_RANGE, etc.)
		struct {
			ast_ref start;
			ast_ref end;
			bool inverted;
		} range_seg;

		// AST_WILDCARD_SEG: filter expression for `*[?(filter)]`, or
		// AST_REF_NULL for the bare wildcard `*`.
		// AST_AND_EXP_SEG: filter expression for `&[?(filter)]` (always
		// non-NULL; grammar has no bare `&` form).
		// body_offset / body_sz: source byte range of the body content
		// between `?(` and `)]`, used by codegen's slow-path diagnostic
		// when sub-program msgpack validation fails. 0/0 for bare `*`.
		struct {
			ast_ref filter;
			uint32_t body_offset;
			uint32_t body_sz;
		} by_exp_seg;

		// Relative range segments (AST_MAP_INDEX_REL_RANGE,
		// AST_MAP_RANK_REL_RANGE, AST_LIST_RANK_REL_RANGE).
		// start/end are static AST_INT (or end == AST_REF_NULL for
		// open-end). relative_to is the anchor key/value (static or dyn).
		// inverted packs into the spare byte of relative_to (24-bit
		// ref) — total 12 bytes.
		struct {
			ast_ref start;
			ast_ref end;
			ast_ref relative_to : 24;
			bool inverted : 1;
		} rel_range_seg;

		// List path segments (AST_MAP_KEY_LIST, AST_MAP_VALUE_LIST, etc.)
		// Built by re-typing an AST_LIST in place; tail is carried across
		// the morph so the reverse morph (cdtprm_push_leaf_elems) is a
		// field copy, not a chain rescan. count matches u.list.count's
		// 24-bit budget.
		struct {
			ast_ref head : 24;
			ast_ref tail : 24;
			uint32_t count : 24;
			bool inverted : 1;
		} list_seg;

		// All NK_LIST nodes: children chained via `next` as siblings.
		//
		// N-ary non-math (AST_AND, AST_OR, AST_BIT_AND, AST_BIT_OR,
		//   AST_BIT_XOR, AST_EXCLUSIVE): head = first, count = count.
		//
		// AST_LIST: head = first element, count = element count.
		//
		// AST_MAP: interleaved key->val->key->val chain.
		//   head = first key, tail = last value, count = pair count.
		//
		// AST_MAP_KEY_LIST, AST_MAP_VALUE_LIST, AST_LIST_VALUE_LIST:
		//   head = first key/value, count = element count.
		//   node->next is the path segment link (next segment or path func).
		//
		// AST_LET: head = first AST_VAR_DEF, tail = body expression,
		//   count = defs + 1 (body is last child).
		//
		// AST_WHEN: head = first AST_CASE, tail = default expression.
		//   count = mappings + 1 (default is last child).
		struct {
			ast_ref head;
			ast_ref tail;
			uint32_t count : 24;
			uint32_t order_override : 2; // ael_order_override (AST_LIST/AST_MAP)
		} list;

		// AST_VAR_DEF: value ref + reference-chain head; sibling chain is
		// node->next. The name starts at node.offset (ast_new_var_def spans
		// the node from the name); name_sz is stored because node.sz covers
		// the whole `name = value` binding. ref_root heads the AST_VAR
		// reference chain (linked via u.var.ref_next) so a narrowing of the
		// value's type flows to every ${name} reference — the var twin of
		// the canonical bin's ref_root stack.
		struct {
			uint32_t name_sz; // 1..AEL_VAR_NAME_MAX (255)
			ast_ref value : 24;
			ast_ref ref_root : 24;
		} var_def;

		// AST_ARG: transient function-call argument. name_sz == 0 means
		// positional; otherwise name_offset/name_sz locate the param name
		// in the input. `value` is the argument expression. Siblings chain
		// via node->next; the owning AST_ARG_LIST counts them.
		struct {
			uint32_t name_offset;
			uint32_t name_sz;
			ast_ref value;
		} arg;

		// AST_VAR: resolved variable reference. The name is at node.offset
		// (into input, for codegen wire format) / name_sz.
		// ref_next chains all references to the same var_def (head at the
		// def's u.var_def.ref_root). var_idx:16 is bounded by AEL_VAR_MAX;
		// name_sz:16 is bounded by AEL_VAR_NAME_MAX (both checked at
		// ast_new_var_def, and a reference only matches a stored def).
		struct {
			ast_ref ref_next;
			uint32_t var_idx : 16; // 0..AEL_VAR_MAX - 1 (1023)
			uint32_t name_sz : 16; // 1..AEL_VAR_NAME_MAX (255)
			ast_ref parent;
		} var;

		// AST_LET_SCOPE: scope node for with/let variable lookup.
		// The var_defs are the preceding siblings in the enclosing AST_LET_LIST.
		struct {
			ast_ref parent; // enclosing let_scope, or AST_REF_NULL
			uint32_t start_idx; // global var slot index
		} let_scope;

		// AST_CASE: cond/result refs; sibling chain is node->next.
		struct {
			ast_ref cond;
			ast_ref result;
		} when_case;

		// AST_FUNC_ABS, AST_FUNC_CEIL, AST_FUNC_FLOOR,
		// AST_FUNC_COUNT_ONE_BITS: single arg.
		struct {
			ast_ref arg;
		} func1;

		// AST_FUNC_LOG, AST_FUNC_POW,
		// AST_FUNC_FIND_BIT_LEFT, AST_FUNC_FIND_BIT_RIGHT: two args.
		struct {
			ast_ref arg1;
			ast_ref arg2;
		} func2;

		// AST_PATH_CALL: thin wrapper composing ctx + op into a single node
		// the rest of the AST machinery can treat uniformly. System-type-
		// agnostic; op-specific data lives on the linked AST_CDT_OP (or
		// AST_BIT_OP / AST_HLL_OP / AST_STR_OP). The bin operand is the
		// head of the AST_PATH_CTX list — see u.list comment below. The
		// wire rtype byte is derived from `etype` at codegen via
		// ast_etype_to_rtype; no separate rtype slot is needed.
		struct {
			exp_call_stype stype; // EXP_CALL_CDT (| EXP_CALL_FLAG_MODIFY_LOCAL)
			ast_ref ctx; // AST_PATH_CTX (always present)
			ast_ref call_op; // NK_CDT_OP node (or future HLL/BIT op)
		} call;

		// NK_CDT_OP nodes (AST_PATH_FUNC_GET / EXISTS / COUNT / REMOVE / SET /
		// INSERT / INCREMENT / APPEND / CLEAR / SORT). The node's own type IS
		// the path-function kind. The wire payload chain (head/tail/count, linked via
		// node->next) maps one-to-one to wire-format positions after op_code.
		// Singular and range leaves are flattened into the chain; LIST_SEG
		// passes through as one entry that expands to a sub-list at pack time.
		//   GET / EXISTS / COUNT / REMOVE
		//     singular leaf:    [ret_type, key]
		//     range leaf:       [ret_type, start] or [ret_type, start, end]
		//     list leaf:        [ret_type, list_seg]
		//   SET / INSERT / INCREMENT          [key, value]
		//   APPEND / APPENDITEMS              [value]
		//   CLEAR / SORT / SIZE               []
		// The bin operand lives at the head of the enclosing AST_PATH_CTX.
		// op_code is 0 until the call combine step resolves it.
		struct {
			uint16_t op_code : 13; // CDT op (AS_CDT_OP_*); 0 = unresolved
			bool is_modify : 1; // set on the resolved-op morph for stype
			bool leaf_consumed : 1; // op took a leaf seg as a chain operand
			bool blob_inline : 1; // SIZE→BUILD: static blob fits in opc->vecs tail
			ast_prop_bits props; // AST_PROP_* bits applied via `:PROPERTY`
			ast_ref head : 24; // head + count packed into one 32-bit word
			uint8_t count; // sibling chain length (same as u.list)
			ast_ref tail; // tail of the chain
		} cdt_op;

		// AST_PATH_CTX: linked context-list under u.call.ctx — the bin
		// head + path segments. head/tail/count form the same chain
		// shape as u.list / u.cdt_op (siblings linked via node->next).
		// count includes the bin, so a bare $.bin has count=1; capped
		// at AST_PATH_CTX_MAX (256). is_multi is true if any non-tail
		// segment is multi-select (forces SELECT emit); last_is_multi
		// tracks the tail seg separately so finalize can decide between
		// GET (only the tail is multi → splittable leaf) and SELECT
		// (any inner multi → must select).
		struct {
			uint16_t count; // segs + 1 (for the bin)
			bool is_multi; // has multi-select levels
			bool last_is_multi; // last seg is multi-select
			ast_ref head; // head of the chain (always the bin)
			ast_ref tail; // tail of the chain (last seg or bin)
		} ctx_list;

		// Modify path functions (AST_PATH_FUNC_REMOVE, _SET, _INSERT,
		// _APPEND, _INCREMENT): optional value + CDT select-op type.
		// AST_PATH_FUNC_SELECT, AST_PATH_FUNC_MODIFY, AST_PATH_FUNC_PSELECT_REMOVE
		// also reuse this layout:
		//   SELECT: value unused, sel_type = SELECT_* type code,
		//     props carries AST_PROP_NO_FAIL bit.
		//   MODIFY: value = apply expression ref, sel_type =
		//     AS_CDT_SELECT_APPLY, props carries AST_PROP_NO_FAIL bit.
		//   PSELECT_REMOVE: value = AST_REF_NULL (codegen synthesizes
		//     the apply blob), sel_type = AS_CDT_SELECT_APPLY,
		//     props carries AST_PROP_NO_FAIL bit.
		// Codegen's prop_to_select_flag projects AST_PROP_NO_FAIL to
		// the wire AS_CDT_SELECT_NO_FAIL bit at emit time.
		struct {
			ast_ref value;
			as_cdt_select_flags sel_type; // 8-bit AS_CDT_SELECT_* code
			ast_prop_bits props; // 16-bit AST_PROP_*
		} modify;

		// AST_LOOP_VAR: @, @key, @index — bound by SELECT iteration.
		// Emits as EXP_VAR_BUILTIN(rtype, idx).
		// next_same_kind chains all occurrences with the same builtin
		// in a single sub-program body; populated by ast_unify_loop_vars
		// at body finalize to share etype across occurrences.
		struct {
			ast_ref next_same_kind;
			as_exp_builtin builtin;
		} loop_var;
	} u;

	// Sibling link only. Points to next peer in a parent's child chain.
	// When on freelist, used as freelist link.
	// Never points to a child -- children are always in u.list.head.
	ast_ref next : 24;
	ast_node_t type; // 8-bit node kind
	uint32_t offset : 24; // offset into input buffer (16 MB cap — plenty)
	uint8_t sz; // span size in source bytes, clamped to 255
	ast_etype etype : 13; // type bitmask, 1 bit per type (10 bits used)
	ast_esubtype esubtype : 2; // container order, on the element that named it
	bool is_free : 1;
	bool has_deferable : 1;
	uint32_t padding : 7;
	// How many bytes BEFORE offset the display span starts, so the display
	// span is [offset - disp_pre, offset - disp_pre + sz). Set when the
	// displayed construct reaches back past the node's own anchor -- over a
	// bin's '$.' / '$."' prefix, or out over an enclosing paren, which can
	// wrap a node of any kind. It exists so that widening never has to move
	// `offset`, which for a bin or variable is the name codegen puts on the
	// wire (see u.bin / u.var / u.var_def). Written only by
	// ast_set_display_span, read only via ast_disp_offset. Last in the header
	// so that it starts on a byte boundary.
	uint32_t disp_pre : 8;
} __attribute__((packed)) ast_node;

// The union is budgeted at 12 bytes (see the per-variant packing notes above);
// with the 12 header bytes that's a 24-byte node. Keep it that way -- the pool
// sizing and the per-variant bit budgets depend on it. A 32-bit offset added
// to a union variant (rather than derived, as disp_pre is) silently blows the
// budget: it compiles, and every AST node grows.
COMPILER_ASSERT(sizeof(ast_node) == 24);

// Kind-asserted accessors for type-punned union members. Reading
// u.seg.* on a non-NK_SEG_S node (or u.cdt_op.* on a non-NK_CDT_OP
// node) reads bytes from a different variant's fields — a silent
// data-corruption bug. The asserts here turn that into an immediate
// crash. Cost is negligible outside tight loops, and Aerospike does
// not disable asserts in production.

// Wire result type of a $.key() (EXP_REC_KEY) node, derived from its
// narrowed etype. The key type must be resolved (single bit) before build —
// either by inference or an explicit `:T` suffix; the post-parse pass errors
// on an unresolved key (spec: no silent default types), so both emitters can
// assume ast_type_resolved(etype) here.
static inline exp_rtype
ast_rec_key_rtype(const ast_node* np)
{
	return ast_etype_to_rtype(np->etype);
}

// Wire result type of a loop variable (@, @key, @index). @index is pinned
// INT at construction; @key (AUTO_KEY) and @ (AUTO) fall back to STR when no
// comparison / cast narrowed them (STR is the common map-key type and the
// historical default for an unresolved value). Shared by both emitters so the
// default policy can't drift between them.
static inline exp_rtype
ast_loop_var_rtype(const ast_node* np)
{
	if (ast_type_resolved(np->etype)) {
		return ast_etype_to_rtype(np->etype);
	}

	return np->u.loop_var.builtin == AS_EXP_BUILTIN_INDEX ? EXP_RTYPE_INT
														  : EXP_RTYPE_STR;
}

static inline ast_ref
ast_seg_operand(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_SEG_S, AS_EXP,
			"ast_seg_operand on non-NK_SEG_S kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.seg.operand;
}

static inline ast_prop_bits
ast_seg_props(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_SEG_S, AS_EXP,
			"ast_seg_props on non-NK_SEG_S kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.seg.props;
}

static inline ast_ref
ast_cdt_op_head(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_head on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.head;
}

static inline ast_ref
ast_cdt_op_tail(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_tail on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.tail;
}

static inline uint8_t
ast_cdt_op_count(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_count on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.count;
}

static inline uint16_t
ast_cdt_op_op_code(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_op_code on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.op_code;
}

static inline uint16_t
ast_cdt_op_props(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_props on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.props;
}

static inline bool
ast_cdt_op_is_modify(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_is_modify on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.is_modify;
}

static inline bool
ast_cdt_op_leaf_consumed(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_leaf_consumed on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.leaf_consumed;
}

static inline bool
ast_cdt_op_blob_inline(const ast_node* np)
{
	cf_assert(ast_node_table[np->type].kind == NK_CDT_OP, AS_EXP,
			"ast_cdt_op_blob_inline on non-NK_CDT_OP kind=%d type=%d",
			ast_node_table[np->type].kind, np->type);
	return np->u.cdt_op.blob_inline;
}

// .select() / .modify() / wildcard-.remove() — the ops that emit the
// SELECT wire form (distinct blob shape, rtype from ael_select_shape).
// Both emitters gate on this; keep it single-sourced.
static inline bool
ast_is_select_family(ast_node_t t)
{
	return t == AST_PATH_FUNC_SELECT || t == AST_PATH_FUNC_MODIFY ||
			t == AST_PATH_FUNC_PSELECT_REMOVE;
}

// Number of path segments after the bin in an AST_PATH_CTX — its stored
// count includes the bin (maintained by the CTXCHAIN pushes and
// ctx_pop_tail), so no chain walk is needed.
static inline uint32_t
ast_ctx_seg_count(const ast_node* cxn)
{
	cf_assert(cxn->type == AST_PATH_CTX, AS_EXP, "ast_ctx_seg_count on type=%d",
			cxn->type);
	return (uint32_t)cxn->u.ctx_list.count - 1;
}

static inline bool
ast_seg_is_multi(const ast_node* np)
{
	return (ast_node_table[np->type].flags & AST_NF_PLURAL) != 0;
}

static inline bool
ast_seg_inverted(const ast_node* np)
{
	if (ast_node_table[np->type].kind != NK_SEG_M) {
		return false;
	}

	uint32_t flags = ast_node_table[np->type].flags;

	if ((flags & AST_NF_REL_RANGE) != 0) {
		return np->u.rel_range_seg.inverted;
	}

	return (flags & AST_NF_LIST_SEG) != 0 ? np->u.list_seg.inverted
										  : np->u.range_seg.inverted;
}

//==========================================================
// Pool allocator.
//
// All storage is cf dynmem (slabs from cf_malloc); grows geometrically.
//

// Default initial node count for ast_pool_init(); must be power-of-2 and >= 8 (dynmem_init).
#define AST_POOL_DEFAULT_INIT 128
COMPILER_ASSERT((AST_POOL_DEFAULT_INIT & (AST_POOL_DEFAULT_INIT - 1)) ==
		0); // power of 2

typedef struct ast_pool_s {
	dynmem dm;
	ast_ref free_head;
	uint32_t cur_offset; // current token offset, set by parser
	uint8_t cur_sz; // current token size (clamped 255), set by parser
	ael_diag_list* diags; // transient: codegen sets this so sub-program
			// emission can append slow-path diagnostics
			// without threading through every helper.
	uint8_t stack_mem[AST_POOL_DEFAULT_INIT * sizeof(ast_node)]
			__attribute__((aligned(16)));
} ast_pool;

typedef struct ael_context_s ael_context;

// Get head of circular nmath list: tail->next == head.
ast_ref ast_nmath_head(ast_pool* pool, const ast_node* np);

void ast_pool_init(ast_pool* pool);
ast_ref ast_pool_alloc(ast_pool* pool);
// An ast_node* stays valid for the pool's lifetime: dynmem grows by appending a
// new block (dynmem_grow) and never reallocates the ones already handed out. So
// a node pointer held across an ast_new* / ast_pool_alloc call is safe, and code
// may keep one rather than re-deriving it from the ref. Inline -- node derefs
// dominate the per-transaction compile path, and the visible body lets the
// compiler share repeated derefs of the same ref.
static inline ast_node*
ast_pool_at(ast_pool* pool, ast_ref r)
{
	if (r == AST_REF_NULL) {
		return NULL;
	}

	ast_node* n = (ast_node*)dynmem_at(&pool->dm, r);

	cf_assert(n != NULL, CF_MISC, "ast_pool_at: dynmem_at null at %u", r);
	return n;
}

// Canonical AST_BIN node for a bin occurrence (identity for AST_BIN,
// deref for AST_BIN_REF). The canonical node carries name_sz / offset /
// the ref stack; etype is mirrored on refs.
static inline ast_node*
ast_bin_canonical(ast_pool* pool, ast_node* np)
{
	return np->type == AST_BIN_REF ? ast_pool_at(pool, np->u.bin_ref.bin) : np;
}

// A path call whose receiver is a single value expression rather than an
// AST_PATH_CTX list — BITS always (it reuses u.call.ctx for its one
// receiver), and any HLL / STR / value-recv CDT call whose ctx slot holds
// the receiver expression. Both emitters pick their emit shape on this.
static inline bool
ast_call_is_value_recv(ast_pool* pool, const ast_node* np)
{
	return np->u.call.stype == EXP_CALL_BITS ||
			ast_pool_at(pool, np->u.call.ctx)->type != AST_PATH_CTX;
}

// Stamp a node's source span, overriding ast_new's lookahead-based default.
// offset is a 24-bit field, sz an 8-bit field (clamped) -- both always fit the
// EXP_MAX_AEL_SRC_SIZE-capped input. No-op on AST_REF_NULL (a failed subparse).
//
// This MOVES the node's anchor, so on a node whose offset is a name locator
// (bin / variable) it must be given the NAME's position -- ast_set_display_span
// is how such a node's displayed span is then widened over its prefix. Resets
// disp_pre: a re-stamp replaces the whole span, it doesn't extend one.
static inline void
ast_set_span(ast_pool* pool, ast_ref ref, uint32_t offset, uint32_t sz)
{
	if (ref == AST_REF_NULL) {
		return;
	}

	ast_node* n = ast_pool_at(pool, ref);

	n->offset = offset;
	n->sz = sz > 255 ? 255 : (uint8_t)sz;
	n->disp_pre = 0;
}

// Where a node's displayed source span starts -- what a diagnostic caret, a
// snippet focus, a composite parent span, or the runtime source map wants.
// Differs from node.offset only where offset is pinned to a name (disp_pre).
//
// EVERY read of a node's displayed span goes through this -- any kind can carry
// a reach-back, since a parenthesized group widens whatever node it wraps. The
// one exception is a read deliberately paired with a NAME size (u.*.name_sz):
// that is caret-ing the name, so it wants node.offset.
//
// Note `offset + sz` is NOT the span end on a node with a reach-back; it
// overshoots by disp_pre. Ends are ast_disp_offset(n) + n->sz.
static inline uint32_t
ast_disp_offset(const ast_node* n)
{
	return n->offset - n->disp_pre;
}

// Widen a node's DISPLAYED span to [start, end) without moving its anchor. Use
// for the syntax that surrounds a node's own text: the '$.' / '$."' prefix, an
// enclosing paren.
//
// This never writes node.offset, for ANY kind -- that is the whole point. A
// node's offset can be a semantic locator (a bin's or variable's name, which
// codegen puts on the wire), and widening a display span must not be able to
// move one. There is deliberately no list of kinds to keep in step here.
//
// pre: start <= node's current offset <= end.
// A prefix is syntax-determined and tiny ('$.' 2, '$."' 3, '${' 2, paren 1 +
// whitespace), but whitespace is unbounded -- '(' + 300 spaces + '$.x' + ')'
// would need a 301-byte reach-back. Past disp_pre's 8-bit range the widening is
// REFUSED rather than clamped: clamping the delta would drag the name locator
// along with it, so an absurdly padded source loses a paren from its highlight
// instead of corrupting the name on the wire.
static inline void
ast_set_display_span(ast_pool* pool, ast_ref ref, uint32_t start, uint32_t end)
{
	if (ref == AST_REF_NULL) {
		return;
	}

	ast_node* n = ast_pool_at(pool, ref);

	if (start > n->offset || n->offset - start > 255) {
		return;
	}

	uint32_t sz = end > start ? end - start : 0;

	n->disp_pre = n->offset - start;
	n->sz = sz > 255 ? 255 : (uint8_t)sz;
}

// A node's whole span as a value. For the post-parse lowerings that must
// capture a span BEFORE a splice invalidates the node it came from, then put it
// back on the replacement -- carrying offset/sz alone would drop the reach-back
// and shift the focus onto a bare name or inside a paren.
typedef struct ast_span_s {
	uint32_t offset;
	uint8_t sz;
	uint8_t disp_pre;
} ast_span;

static inline ast_span
ast_get_span(ast_pool* pool, ast_ref ref)
{
	const ast_node* n = ast_pool_at(pool, ref);

	return (ast_span){
		.offset = n->offset, .sz = n->sz, .disp_pre = (uint8_t)n->disp_pre
	};
}

static inline void
ast_put_span(ast_pool* pool, ast_ref ref, ast_span s)
{
	if (ref == AST_REF_NULL) {
		return;
	}

	ast_node* n = ast_pool_at(pool, ref);

	n->offset = s.offset;
	n->sz = s.sz;
	n->disp_pre = s.disp_pre;
}

// Copy src's whole span -- display start, extent, and anchor pinning -- onto
// dst. For a lowering that splices a new node in over an existing construct
// and must present the same source text.
static inline void
ast_copy_span(ast_pool* pool, ast_ref dst, ast_ref src)
{
	if (dst == AST_REF_NULL || src == AST_REF_NULL) {
		return;
	}

	ast_put_span(pool, dst, ast_get_span(pool, src));
}
void ast_pool_release(ast_pool* pool, ast_ref r);
void ast_pool_destroy(ast_pool* pool);

// Intrusive lists: sibling chain via ast_node.next; head/tail/count live on u.list,
// u.ctx_list, or u.cdt_op. Low-level FIELD macros expose head/tail/count lvalues;
// LIST_* / CHAIN_* take ast_ref owning nodes; PLIST_* take ast_node* for u.list.
// PUSH_* skip a AST_REF_NULL noderef (a failed sub-parse), leaving the list
// unchanged.

#define AST_LIST_FIELDS_CLEAR(head, tail, count)                               \
	do {                                                                       \
		(head) = AST_REF_NULL;                                                 \
		(tail) = AST_REF_NULL;                                                 \
		(count) = 0;                                                           \
	} while (0)

#define AST_LIST_FIELDS_PUSH_TAIL(pool, head, tail, count, noderef)            \
	do {                                                                       \
		ast_pool* __ast_ap = (pool);                                           \
		ast_ref __nr = (noderef);                                              \
		if (__nr == AST_REF_NULL) {                                            \
			break;                                                             \
		}                                                                      \
		ast_pool_at(__ast_ap, __nr)->next = AST_REF_NULL;                      \
		if ((head) == AST_REF_NULL) {                                          \
			(head) = (tail) = __nr;                                            \
		}                                                                      \
		else {                                                                 \
			ast_pool_at(__ast_ap, (tail))->next = __nr;                        \
			(tail) = __nr;                                                     \
		}                                                                      \
		(count)++;                                                             \
	} while (0)

#define AST_LIST_FIELDS_PUSH_HEAD(pool, head, tail, count, noderef)            \
	do {                                                                       \
		ast_pool* __ast_ap = (pool);                                           \
		ast_ref __nr = (noderef);                                              \
		if (__nr == AST_REF_NULL) {                                            \
			break;                                                             \
		}                                                                      \
		ast_pool_at(__ast_ap, __nr)->next = (head);                            \
		(head) = __nr;                                                         \
		if ((tail) == AST_REF_NULL) {                                          \
			(tail) = __nr;                                                     \
		}                                                                      \
		(count)++;                                                             \
	} while (0)

// The ref-taking forms bind the owner node once -- the FIELDS bodies
// reference each field lvalue several times, and an ast_pool_at per
// mention adds up on the compile path.
#define AST_LIST_PUSH_TAIL(pool, list_owner_ref, node_ref)                     \
	do {                                                                       \
		ast_pool* __ast_lp = (pool);                                           \
		ast_node* __ast_lo = ast_pool_at(__ast_lp, (list_owner_ref));          \
		AST_LIST_FIELDS_PUSH_TAIL(__ast_lp, __ast_lo->u.list.head,             \
				__ast_lo->u.list.tail, __ast_lo->u.list.count, (node_ref));    \
	} while (0)

#define AST_LIST_PUSH_HEAD(pool, list_owner_ref, node_ref)                     \
	do {                                                                       \
		ast_pool* __ast_lp = (pool);                                           \
		ast_node* __ast_lo = ast_pool_at(__ast_lp, (list_owner_ref));          \
		AST_LIST_FIELDS_PUSH_HEAD(__ast_lp, __ast_lo->u.list.head,             \
				__ast_lo->u.list.tail, __ast_lo->u.list.count, (node_ref));    \
	} while (0)

// Same for ast_node * (NK_LIST-bearing node).
#define AST_PLIST_PUSH_TAIL(pool, np, node_ref)                                \
	AST_LIST_FIELDS_PUSH_TAIL((pool), (np)->u.list.head, (np)->u.list.tail,    \
			(np)->u.list.count, (node_ref));

// NK path-func CDT wire chain (u.cdt_op).
#define AST_CHAIN_CLR(pool, op_ref)                                            \
	do {                                                                       \
		ast_node* __ast_co = ast_pool_at((pool), (op_ref));                    \
		AST_LIST_FIELDS_CLEAR(__ast_co->u.cdt_op.head,                         \
				__ast_co->u.cdt_op.tail, __ast_co->u.cdt_op.count);            \
	} while (0)

// u.cdt_op.count is uint8_t (packed-union budget); cap pushes at
// UINT8_MAX so wraparound can't silently emit a 0-length msgpack
// list header in codegen.
#define AST_CHAIN_PUSH_TAIL(pool, op_ref, node_ref)                             \
	do {                                                                        \
		ast_pool* __ast_cp = (pool);                                            \
		ast_node* __ast_co = ast_pool_at(__ast_cp, (op_ref));                   \
		if (__ast_co->u.cdt_op.count == UINT8_MAX) {                            \
			cf_warning(AS_EXP,                                                  \
					"AST_CHAIN_PUSH_TAIL - chain length exceeds 255");          \
			break;                                                              \
		}                                                                       \
		AST_LIST_FIELDS_PUSH_TAIL(__ast_cp, __ast_co->u.cdt_op.head,            \
				__ast_co->u.cdt_op.tail, __ast_co->u.cdt_op.count, (node_ref)); \
	} while (0)

#define AST_CHAIN_PUSH_HEAD(pool, op_ref, node_ref)                             \
	do {                                                                        \
		ast_pool* __ast_cp = (pool);                                            \
		ast_node* __ast_co = ast_pool_at(__ast_cp, (op_ref));                   \
		if (__ast_co->u.cdt_op.count == UINT8_MAX) {                            \
			cf_warning(AS_EXP,                                                  \
					"AST_CHAIN_PUSH_HEAD - chain length exceeds 255");          \
			break;                                                              \
		}                                                                       \
		AST_LIST_FIELDS_PUSH_HEAD(__ast_cp, __ast_co->u.cdt_op.head,            \
				__ast_co->u.cdt_op.tail, __ast_co->u.cdt_op.count, (node_ref)); \
	} while (0)

// PATH_CTX segment chain (u.ctx_list).
#define AST_CTXCHAIN_PUSH_TAIL(pool, ctx_ref, node_ref)                        \
	do {                                                                       \
		ast_pool* __ast_xp = (pool);                                           \
		ast_node* __ast_xo = ast_pool_at(__ast_xp, (ctx_ref));                 \
		AST_LIST_FIELDS_PUSH_TAIL(__ast_xp, __ast_xo->u.ctx_list.head,         \
				__ast_xo->u.ctx_list.tail, __ast_xo->u.ctx_list.count,         \
				(node_ref));                                                   \
	} while (0)

#define AST_PLCTX_PUSH_TAIL(pool, np, node_ref)                                \
	AST_LIST_FIELDS_PUSH_TAIL((pool), (np)->u.ctx_list.head,                   \
			(np)->u.ctx_list.tail, (np)->u.ctx_list.count, (node_ref));

#define AST_REF_CHECK_RET(_ref, _ret)                                          \
	do {                                                                       \
		if ((_ref) == AST_REF_NULL) {                                          \
			return (_ret);                                                     \
		}                                                                      \
	} while (0)

// Construction -- all allocate from the pool.
ast_ref ast_new(ast_pool* pool, ast_node_t type);
ast_ref ast_new_nil(ast_pool* pool);
ast_ref ast_new_inf(ast_pool* pool);
ast_ref ast_new_wildcard(ast_pool* pool);
ast_ref ast_new_int(ast_pool* pool, int64_t val);
ast_ref ast_new_float(ast_pool* pool, double val);
// Unary minus: builds AST_SUB(AST_ZERO, operand) so the negation reuses
// SUB and the zero adopts the operand's numeric type. minus_end (one past
// the '-' token) stamps the synthesized zero so the SUB's child-derived
// span stays anchored at the operand, not the parser lookahead.
ast_ref ast_new_neg(ael_context* ctx, ast_ref operand, uint32_t minus_end);
ast_ref ast_new_bool(ast_pool* pool, bool val);
ast_ref ast_new_string(ast_pool* pool, const char* s, uint32_t sz,
		bool has_escape);
ast_ref ast_new_string_token(ael_context* ctx, uint32_t name_offset,
		uint32_t name_sz);
ast_ref ast_new_blob_from_hex(ast_pool* pool, const char* hex, uint32_t hex_sz);
ast_ref ast_new_blob_from_b64(ast_pool* pool, const char* b64, uint32_t b64_sz);

// Shallow clone for primitive literal / bin-ref nodes — used by the
// bit-modify path simulation where a single-element leaf's index/key
// expression has to appear in both the outer LIST_SET / MAP_REPLACE
// (as position) and the inner read (to navigate). Returns AST_REF_NULL
// if the node type isn't trivially cloneable; the caller errors out.
ast_ref ast_clone_simple(ast_pool* pool, ast_ref src);
// ref_offset/ref_end span the whole reference ('$.' through the end of the
// name or its closing quote) - the node's display span. name_offset/name_sz
// locate the NAME's bytes in the input (string-form: the content between the
// quotes) - the node's anchor, with the '$.' prefix carried as disp_pre.
ast_ref ast_new_bin(ael_context* ctx, uint32_t ref_offset, uint32_t ref_end,
		uint32_t name_offset, uint32_t name_sz);
ast_ref ast_new_local_bin(ael_context* ctx, uint32_t ref_offset, uint32_t ref_end,
		uint32_t name_offset, uint32_t name_sz, ast_etype etype);
ast_ref ast_new_bcmp(ael_context* ctx, ast_node_t type, ast_ref left,
		ast_ref right);
ast_ref ast_new_in(ael_context* ctx, ast_ref left, ast_ref right);
ast_ref ast_new_bmath(ael_context* ctx, ast_node_t type, ast_ref left,
		ast_ref right, ast_etype operand_etype);
ast_ref ast_new_binary(ast_pool* pool, ast_node_t type, ast_ref left,
		ast_ref right);
ast_ref ast_new_range_seg(ast_pool* pool, ast_node_t type, ast_ref start,
		ast_ref end, bool inverted);
ast_ref ast_new_rel_range_seg(ast_pool* pool, ast_node_t type, ast_ref start,
		ast_ref end, ast_ref relative_to, bool inverted);
ast_ref ast_new_unary(ast_pool* pool, ast_node_t type, ast_ref operand);
ast_ref ast_new_func1(ael_context* ctx, ast_node_t type, ast_ref arg,
		ast_etype etype);
ast_ref ast_new_func2(ael_context* ctx, ast_node_t type, ast_ref a, ast_ref b,
		ast_etype a_etype, ast_etype b_etype, ast_etype result_etype);
ast_ref ast_new_meta(ast_pool* pool, exp_op_code op_code, int64_t param);

// Resolve a ${name} variable reference — looks up idx and etype from scope.
// Returns AST_REF_NULL with diagnostic if variable is undefined.
ast_ref ast_new_var_ref(ael_context* ctx, uint32_t name_offset, uint32_t name_sz);

// Variable definition — checks for duplicate name and unresolved value type.
// Returns AST_REF_NULL on error (diagnostic added to ctx).
ast_ref ast_new_var_def(ael_context* ctx, uint32_t name_offset,
		uint32_t name_sz, ast_ref value);

// Create a new let scope and push it. Returns the AST_LET_LIST ref.
// The let_scope is at the head, followed by the first var_def.
ast_ref ast_new_let_scope(ael_context* ctx, uint32_t name_offset,
		uint32_t name_sz, ast_ref value);

// Path context list with bin as sole segment (empty context chain).
ast_ref ast_new_path_ctx_bin(ast_pool* pool, ast_ref bin);

// Loop variable constructor — @, @key, @index.
ast_ref ast_new_loop_var(ast_pool* pool, as_exp_builtin builtin);

// Unify the etypes of all AST_LOOP_VAR occurrences linked through
// `next_same_kind` (one chain per builtin within a sub-program scope).
// Intersects etypes across the chain and writes the result back. If
// any intersection is AST_ETYPE_ERROR (e.g., `@key` used as both INT
// and STRING), emits a diagnostic at the conflicting node's offset.
// Called from ael_filter_scope_pop (per-scope) and from the parser
// driver (for the standalone-filter entry point).
void ast_unify_loop_var_chain(ast_pool* pool, ast_ref head, ael_diag_list* diags);

// NK_CDT_OP node constructor. `pf_type` is one of AST_PATH_FUNC_GET /
// EXISTS / COUNT / REMOVE / SET / INSERT / INCREMENT / APPEND / CLEAR /
// SORT — it becomes the node's type. op_code/rtype default to 0 (resolved
// at the call combine step from ctx_list + leaf seg context).
ast_ref ast_new_cdt_op(ast_pool* pool, ast_node_t pf_type);

// Next key in a map literal's interleaved chain: key->val->next_key.
// Since val->next is always the sibling link, this is just key->next->next.
ast_ref ast_map_literal_next_key(ast_pool* pool, ast_ref key);

// Child iteration -- the single definition of "what are a node's children".
//
// Every walker over the AST descends through this, so a node type added to
// ast_node_table inherits correct descent from the 'kind' its entry must
// already declare (the table macros take kind positionally, so it cannot be
// omitted). A hand-enumerated descent is how a missed AST_VAR_DEF child once
// left a polymorphic toInt()/toFloat() unresolved, which then mis-emitted as a
// numeric op over a string operand.
//
// cb is invoked once per child slot, including slots holding AST_REF_NULL --
// callbacks must tolerate it (ast_free and the rewrite walk both no-op on it).
// A child's sibling link is read before its callback runs, so cb may free or
// splice the child it is handed.
typedef void (*ast_child_cb)(ast_pool* pool, ast_ref child, void* arg);

void ast_children_foreach(ast_pool* pool, ast_ref ref, ast_child_cb cb,
		void* arg);

// Destruction -- releases nodes back to pool, recursively frees children.
void ast_free(ast_pool* pool, ast_ref node);

ast_ref ast_nary_merge(ael_context* ctx, ast_node_t type, ast_ref A, ast_ref B,
		ast_etype etype);

// Record a "<what>: <A> vs <B>" diagnostic naming both etypes.
void ast_diag_type_conflict(ael_context* ctx, uint32_t offset, uint32_t byte_sz,
		const char* what, ast_etype a, ast_etype b);

// Set etype on the implicit GET at the leaf of an AST_PATH_CALL, or on a
// bare bin reference.
void ast_set_implicit_type(ael_context* ctx, ast_ref node, ast_etype etype);

// Narrow a bin's type and propagate to all references and their parents.
void ast_bin_set_implicit_type(ael_context* ctx, ast_ref bin_or_ref,
		ast_etype etype);

// Debug print.
void ast_print(ast_pool* pool, const char* input, ast_ref node, int indent);
