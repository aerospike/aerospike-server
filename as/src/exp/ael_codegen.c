/*
 * ael_codegen.c
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

#include "exp/ael_codegen.h"

#include <stdlib.h>

#include "aerospike/as_msgpack.h"

#include "cf_defer.h"
#include "enhanced_alloc.h"

#include "base/cdt.h"
#include "base/proto.h"
#include "exp/ael_emit.h"
#include "exp/ael_parse.h"
#include "exp/ael_string.h"
#include "exp/ast.h"
#include "exp/exp.h"

//==========================================================
// Forward declarations.
//

#define RC_RET_ON_ERR()                                                        \
	if (rc != 0) {                                                             \
		return rc;                                                             \
	}

static uint32_t count_left_fold(ast_pool* pool, ast_ref node, ast_node_t type);

static int emit_particle_string(as_packer* pk, const char* s, uint32_t sz);
static int emit_raw_string(as_packer* pk, const char* s, uint32_t sz);
static int emit_cmd(as_packer* pk, int cmd, uint32_t nargs);
static int emit_left_fold(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ast_node_t type);
static int emit_nary(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_nmath(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_binary(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_meta(as_packer* pk, ast_pool* pool, ast_ref node);
static int emit_with(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_when(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int ael_emit_cdt_op_blob(as_packer* pk, ast_pool* pool,
		const char* input, int cdt_op, ast_ref param_head, uint8_t param_count,
		ast_prop_bits props_flags, ast_node_t op_type);
static int emit_path_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_select_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);
static int emit_bit_call(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node);

// Property → wire-flag projections. Defined later; forward-declared so
// they're callable from ael_emit_cdt_op_blob (which lives above them).
static uint64_t prop_to_ctx_create(ast_prop_bits props);
static uint64_t ael_prop_to_cdt_flag(ast_prop_bits props);
static uint64_t ael_prop_to_cdt_create_flag(ast_prop_bits props);
static uint64_t ael_prop_to_select_flag(ast_prop_bits props);
static uint64_t ael_sort_flags(ast_prop_bits props);

//==========================================================
// Local helpers.
//

// Iterative (not recursive) so it adds no C-stack depth on top of the
// ael_codegen_emit frame already descending this left-deep chain.
static uint32_t
count_left_fold(ast_pool* pool, ast_ref node, ast_node_t type)
{
	uint32_t count = 0;

	while (true) {
		ast_node* np = ast_pool_at(pool, node);

		if (np->type != type) {
			return count + 1;
		}

		count++;
		node = np->u.binary.left;
	}
}

//==========================================================
// Packing helpers.
//

bool
ael_pack_str_with_type_reserve(as_packer* pk, uint8_t type, uint32_t sz,
		uint8_t** buf_r)
{
	// Header for (type byte + sz body bytes) via the public packer's
	// NULL-buf path. as_pack_str's header emit goes through
	// pack_byte / pack_type_uint*, which gate the write on
	// `if (pk->buffer)` but always run advance_offset (with its
	// INT32_MAX check). Body skip is intentional — we fill it
	// ourselves below.
	if (as_pack_str(pk, NULL, sz + 1) != 0) {
		return false;
	}

	// Reserve type byte + sz body bytes. Mirrors pack_byte /
	// pack_append: capacity check is packing-mode only, sizing mode
	// just advances offset. __builtin_add_overflow guards the
	// uint32_t addition; INT32_MAX is the upper bound advance_offset
	// uses.
	uint32_t need = 1 + sz;
	uint32_t new_offset;

	if (__builtin_add_overflow(pk->offset, need, &new_offset) ||
			new_offset > INT32_MAX) {
		return false;
	}

	if (pk->buffer != NULL) {
		if (new_offset > pk->capacity) {
			return false;
		}

		pk->buffer[pk->offset] = type;
		*buf_r = pk->buffer + pk->offset + 1;
	}
	else {
		*buf_r = NULL;
	}

	pk->offset = new_offset;
	return true;
}

static int
pack_hex_blob(as_packer* pk, const char* hex, uint32_t hex_sz)
{
	uint32_t bin_sz = hex_sz / 2;
	uint8_t* dst;

	if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
		return -1;
	}

	if (dst != NULL) {
		ael_hex_decode(hex, hex_sz, dst);
	}

	return 0;
}

static int
pack_b64_blob(as_packer* pk, const char* b64, uint32_t b64_sz)
{
	uint32_t bin_sz = ael_b64_decoded_sz(b64, b64_sz);
	uint8_t* dst;

	if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_BLOB, bin_sz, &dst)) {
		return -1;
	}

	if (dst != NULL) {
		ael_b64_decode(b64, b64_sz, dst);
	}

	return 0;
}

//==========================================================
// Emit functions.
//

static int
emit_particle_string(as_packer* pk, const char* s, uint32_t sz)
{
	return as_pack_str_with_type(pk, AS_BYTES_STRING, (const uint8_t*)s, sz);
}

// Scalar-literal msgpack — every non-collection literal node type, one
// place. Shared by ael_codegen_emit and exp_ael.c's ael_pack_literal so the two
// emitters produce the same bytes by construction. Returns -1 for a
// non-literal node type.
int
ael_emit_scalar_literal(as_packer* pk, const ast_node* node)
{
	switch (node->type) {
	case AST_NIL:
		return as_pack_nil(pk);

	case AST_INF:
		return as_pack_cmp_inf(pk);

	case AST_WILDCARD:
		return as_pack_cmp_wildcard(pk);

	case AST_BOOL:
		return as_pack_bool(pk, node->u.bval);

	case AST_INT:
		return as_pack_int64(pk, node->u.ival);

	case AST_FLOAT:
		return as_pack_double(pk, node->u.fval);

	case AST_ZERO:
		if (node->etype == AST_ETYPE_FLOAT) {
			return as_pack_double(pk, 0.0);
		}
		return as_pack_int64(pk, 0);

	case AST_STRING: {
		if (! node->u.str.has_escape) {
			return emit_particle_string(pk, node->u.str.str, node->u.str.sz);
		}

		uint32_t decoded_sz =
				ael_string_decoded_sz(node->u.str.str, node->u.str.sz);
		uint8_t* dst;

		if (! ael_pack_str_with_type_reserve(pk, AS_BYTES_STRING, decoded_sz,
					&dst)) {
			return -1;
		}

		if (dst != NULL) {
			ael_string_decode(node->u.str.str, node->u.str.sz, dst);
		}

		return 0;
	}

	case AST_BLOB:
		return pack_hex_blob(pk, node->u.str.str, node->u.str.sz);

	case AST_B64_BLOB:
		return pack_b64_blob(pk, node->u.str.str, node->u.str.sz);

	case AST_GEO_LITERAL:
		return as_pack_str_with_type(pk, AS_BYTES_GEOJSON,
				(const uint8_t*)node->u.str.str, node->u.str.sz);

	default:
		return -1;
	}
}

// Bin / var / let names are plain msgpack strings on the wire — no
// AS_BYTES_STRING type prefix. The runtime's build_bin / build_var /
// build_let read them with msgpack_get_bin and compare bytes directly
// against rd->bins[].name (likewise the var scope name), so any
// in-band type byte would corrupt the lookup.
static int
emit_raw_string(as_packer* pk, const char* s, uint32_t sz)
{
	return as_pack_str(pk, (const uint8_t*)s, sz);
}

static int
emit_cmd(as_packer* pk, int cmd, uint32_t nargs)
{
	int rc = as_pack_list_header(pk, nargs + 1);
	RC_RET_ON_ERR();
	return as_pack_int64(pk, cmd);
}

static int
emit_left_fold(as_packer* pk, ast_pool* pool, const char* input, ast_ref node,
		ast_node_t type)
{
	int rc;
	ast_node* np = ast_pool_at(pool, node);

	if (np->type == type) {
		rc = emit_left_fold(pk, pool, input, np->u.binary.left, type);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, np->u.binary.right);
	}

	return ael_codegen_emit(pk, pool, input, node);
}

static int
emit_nary(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, np->u.list.count);
	RC_RET_ON_ERR();

	ast_ref e = np->u.list.head;

	for (uint32_t i = 0; i < np->u.list.count; i++) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		e = ast_pool_at(pool, e)->next;
	}

	return rc;
}

static int
emit_nmath(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, np->u.nmath.count);
	RC_RET_ON_ERR();

	ast_ref head = ast_nmath_head(pool, np);
	ast_ref e = head;

	for (uint32_t i = 0; i < np->u.nmath.count; i++) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		e = ast_pool_at(pool, e)->next;
	}

	return rc;
}

static int
emit_binary(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = ast_node_table[np->type].exp_cmd;

	if (cmd == 0) {
		return -1;
	}

	int rc = emit_cmd(pk, cmd, 2);
	RC_RET_ON_ERR();
	rc = ael_codegen_emit(pk, pool, input, np->u.binary.left);
	RC_RET_ON_ERR();
	return ael_codegen_emit(pk, pool, input, np->u.binary.right);
}

static int
emit_meta(as_packer* pk, ast_pool* pool, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	int cmd = (int)np->u.meta.op_code;

	if (cmd == 0) {
		return -1;
	}

	if (cmd == EXP_META_DIGEST_MOD) {
		int rc = emit_cmd(pk, cmd, 1);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, np->u.meta.param);
	}

	if (cmd == EXP_REC_KEY) {
		// The rtype comes from the narrowed etype, which the post-parse pass
		// has already held to a single key type.
		cf_assert(ast_type_resolved(np->etype), AS_EXP,
				"ael_codegen_emit - rec key unresolved at emit");

		exp_rtype rt = ast_rec_key_rtype(np);
		int rc = emit_cmd(pk, cmd, 1);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, (int64_t)rt);
	}

	return emit_cmd(pk, cmd, 0);
}

// AST_LIST literal -> msgpack list (no EXP_QUOTE wrapper here; the wire
// form's caller adds it). Shared with exp_ael.c's ael_pack_literal so the
// collection shape + validations cannot drift; child_emit keeps element
// dispatch per-context (see ael_emit.h).
int
ael_emit_list_literal(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ael_lit_child_emit child_emit)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t ele_count = np->u.list.count;
	// Lists default to plain/unordered; :ORDERED opts in to an ORDERED-flagged
	// list (validated, not sorted, below).
	bool ordered = np->u.list.order_override == AEL_ORDER_ORDERED;
	uint32_t start = pk->offset;

	int rc = as_pack_list_header(pk, ele_count + (ordered ? 1 : 0));
	RC_RET_ON_ERR();

	if (ordered) {
		rc = as_pack_ext_header(pk, 0, AS_PACKED_LIST_FLAG_ORDERED);
		RC_RET_ON_ERR();
	}

	ast_ref e = np->u.list.head;

	for (uint32_t i = 0; i < ele_count; i++) {
		rc = child_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
		if (i + 1 < ele_count) {
			e = ast_pool_at(pool, e)->next;
		}
	}

	if (ordered && pk->buffer != NULL && ele_count >= 2 &&
			! list_buf_check_ordered(pk->buffer + start, pk->offset - start)) {
		if (pool->diags != NULL) {
			ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
					np->sz, ":SORTED list literal is not in ascending order");
		}

		return -1;
	}

	return rc;
}

// AST_MAP literal -> msgpack map. Shared with exp_ael.c's ael_pack_literal.
int
ael_emit_map_literal(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref node, ael_lit_child_emit child_emit)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t ele_count = np->u.list.count;
	// Maps default to ORDERED (K_ORDERED + key-sort, below); :UNORDERED opts
	// out to a plain map (smaller, but uncomparable).
	bool ordered = np->u.list.order_override != AEL_ORDER_UNORDERED;
	uint32_t start = pk->offset;

	int rc = as_pack_map_header(pk, ele_count + (ordered ? 1 : 0));
	RC_RET_ON_ERR();

	if (ordered) {
		rc = as_pack_ext_header(pk, 0, AS_PACKED_MAP_FLAG_K_ORDERED);
		RC_RET_ON_ERR();
		rc = as_pack_nil(pk); // ext pair value
		RC_RET_ON_ERR();
	}

	ast_ref key = np->u.list.head;

	for (uint32_t i = 0; i < ele_count; i++) {
		ast_node* kp = ast_pool_at(pool, key);
		ast_ref val = kp->next;

		rc = child_emit(pk, pool, input, key);
		RC_RET_ON_ERR();
		rc = child_emit(pk, pool, input, val);
		RC_RET_ON_ERR();
		key = (i + 1 < ele_count) ? ast_map_literal_next_key(pool, key)
								  : AST_REF_NULL;
	}

	// Uniqueness is checked at both orderings: :UNORDERED opts out of the key
	// sort, not out of distinct keys. Nothing downstream re-checks -- the
	// untrusted rewriter only sees client msgpack, never a literal packed here,
	// and map_verify compiles out (and passes K_ORDERED duplicates anyway).
	// Sizing runs with a NULL buffer and no bytes to read; it re-runs here with
	// one, so skipping it then loses nothing.
	if (pk->buffer != NULL && ele_count >= 2) {
		if (! map_buf_check_unique_and_sort(pk->buffer + start,
					pk->offset - start, ordered)) {
			if (pool->diags != NULL) {
				ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(np),
						np->sz, "map literal has duplicate keys");
			}

			return -1;
		}
	}

	return rc;
}

static int
emit_with(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t defs_count = np->u.list.count - 2; // exclude let_scope + body
	int rc = emit_cmd(pk, EXP_LET, (defs_count * 2) + 1);
	RC_RET_ON_ERR();

	// Skip let_scope at head.
	ast_ref def = ast_pool_at(pool, np->u.list.head)->next;

	for (uint32_t i = 0; i < defs_count; i++) {
		ast_node* dp = ast_pool_at(pool, def);
		rc = emit_raw_string(pk, input + dp->offset, dp->u.var_def.name_sz);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, dp->u.var_def.value);
		RC_RET_ON_ERR();
		def = dp->next;
	}

	return ael_codegen_emit(pk, pool, input, np->u.list.tail);
}

static int
emit_when(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	uint32_t mappings_count = np->u.list.count - 1;
	int rc = emit_cmd(pk, EXP_COND, (mappings_count * 2) + 1);
	RC_RET_ON_ERR();

	ast_ref m = np->u.list.head;

	for (uint32_t i = 0; i < mappings_count; i++) {
		ast_node* mp = ast_pool_at(pool, m);
		rc = ael_codegen_emit(pk, pool, input, mp->u.when_case.cond);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, mp->u.when_case.result);
		RC_RET_ON_ERR();
		m = mp->next;
	}

	return ael_codegen_emit(pk, pool, input, np->u.list.tail);
}

// Pack the inner CDT op blob (no ctx wrapper). The param chain is in
// wire-format order after op_code: each entry maps 1:1 to a wire element.
// List leaves are transmuted to AST_LIST at parse time and packed via
// ael_emit_list_literal (no EXP_QUOTE prefix — the bytes go straight into
// the blob).
// True for CDT modify ops whose wire shape is `[op, args..., create_flags?,
// modify_flags?]` — i.e. ops that have BOTH a create_flags and a
// modify_flags trailing slot. When emitting modify_flags via properties,
// we need to pad create_flags=0 to fill the create slot first.
bool
ael_cdt_op_has_create_flags_slot(int cdt_op)
{
	switch (cdt_op) {
	// LIST_INSERT_ITEMS is NOT here: its op-table entry takes one optional
	// FLAGS arg (the modify word), so claiming a create slot made a modify flag
	// emit a pad plus the word -- one arg more than the op accepts. MAP_ADD and
	// MAP_ADD_ITEMS are single-slot the same way, but AEL emits neither (insert
	// and insertItems go through MAP_PUT / MAP_PUT_ITEMS).
	case AS_CDT_OP_LIST_APPEND:
	case AS_CDT_OP_LIST_APPEND_ITEMS:
	case AS_CDT_OP_LIST_INCREMENT:
	case AS_CDT_OP_MAP_PUT:
	case AS_CDT_OP_MAP_PUT_ITEMS:
		return true;
	default:
		return false;
	}
}

// True for the CDT modify ops whose wire shape ends in a modify_flags slot.
//
// An allowlist, so an op nobody has classified emits no flags rather than a word
// the op cannot take -- which the runtime rejects as a malformed op list, not as
// a bad flag. AelOpFlagSlots cross-checks every entry against cdt_op_table's own
// count of trailing optional FLAGS args.
//
// Absent, and why: LIST_SORT's one slot is sort flags; MAP_ADD / MAP_ADD_ITEMS
// and MAP_INCREMENT / MAP_DECREMENT take a create word in theirs; MAP_REPLACE*,
// the CLEARs and every REMOVE_BY* have no trailing slot at all.
bool
ael_cdt_op_has_modify_flags_slot(int cdt_op)
{
	switch (cdt_op) {
	case AS_CDT_OP_LIST_APPEND:
	case AS_CDT_OP_LIST_APPEND_ITEMS:
	case AS_CDT_OP_LIST_INSERT:
	case AS_CDT_OP_LIST_INSERT_ITEMS:
	case AS_CDT_OP_LIST_SET:
	case AS_CDT_OP_LIST_INCREMENT:
	case AS_CDT_OP_MAP_PUT:
	case AS_CDT_OP_MAP_PUT_ITEMS:
		return true;
	default:
		return false;
	}
}

// True for the list writes that nil-pad when the target index is past the end --
// the ops routed through packed_list_insert. append / appendItems can't (their
// index is always ele_count), and the map ops grow by key.
//
// Gated on the node type as well as the op code. The CDT, string and bit
// families number their ops in separate spaces that overlap -- LIST_INSERT and
// STRING_FIND are both 3; LIST_INCREMENT, STRING_IS_LOWER and BITS_SET_INT are
// all 12 -- and this shape function serves all of them, so the code alone is
// ambiguous. The morphed node type (AST_CDT_OP vs AST_BIT_OP / AST_STR_OP /
// AST_HLL_OP) is what says which space the code belongs to. The other helpers
// here match the code alone, so the shape function gates on the node type before
// consulting any of them -- AS_STRING_OP_APPEND and AS_CDT_OP_MAP_PUT are both
// 67, and a string op can carry :NO_FAIL.
static bool
op_pads_list(ast_node_t op_type, int cdt_op)
{
	if (op_type != AST_CDT_OP) {
		return false;
	}

	switch (cdt_op) {
	case AS_CDT_OP_LIST_SET:
	case AS_CDT_OP_LIST_INSERT:
	case AS_CDT_OP_LIST_INSERT_ITEMS:
	case AS_CDT_OP_LIST_INCREMENT:
		return true;
	default:
		return false;
	}
}

ael_cdt_flag_shape_t
ael_cdt_flag_shape(int cdt_op, ast_prop_bits props, ast_node_t op_type)
{
	// The slot classifiers match the code alone, and the families overlap.
	if (op_type != AST_CDT_OP) {
		return (ael_cdt_flag_shape_t){ 0 };
	}

	ael_cdt_flag_shape_t fs = {
		.sort_flags = ael_sort_flags(props),
		.create_flags = ael_prop_to_cdt_create_flag(props),
		.modify_flags = ael_prop_to_cdt_flag(props),
	};

	// Writing past the end of a list nil-pads out to the index, which one bad
	// index turns into a huge record. The wire defaults to that for backwards
	// compatibility; AEL doesn't, so bound every padding-capable op unless the
	// segment asked for padding with :UNSORTED_PAD. That mirrors the
	// context-navigation path, which the wire already defaults to bounded.
	if (op_pads_list(op_type, cdt_op) && (props & AST_PROP_CR_UNSORTED_PAD) == 0) {
		fs.modify_flags |= AS_CDT_LIST_INSERT_BOUNDED;
	}

	// LIST_SORT is the only op with a sort-flags slot, and DROP_DUPS is the only
	// prop that reaches sort_flags. The valid_props mask already keeps that prop
	// off every other verb, but stating the op here means the shape can never
	// exceed what the op accepts on its own -- see AelOpFlagSlots.
	fs.emit_sort = fs.sort_flags != 0 && cdt_op == AS_CDT_OP_LIST_SORT;
	fs.emit_modify = fs.modify_flags != 0 &&
			ael_cdt_op_has_modify_flags_slot(cdt_op);
	// The create slot appears when a create-order flag is set, or as a
	// pad-to-position before a modify flag on a two-slot op.
	fs.emit_create = ael_cdt_op_has_create_flags_slot(cdt_op) &&
			(fs.create_flags != 0 || fs.emit_modify);
	fs.extra = (fs.emit_sort ? 1 : 0) + (fs.emit_create ? 1 : 0) +
			(fs.emit_modify ? 1 : 0);

	return fs;
}

int
ael_emit_cdt_trailing_flags(as_packer* pk, const ael_cdt_flag_shape_t* fs)
{
	int rc;

	if (fs->emit_sort) {
		rc = as_pack_int64(pk, (int64_t)fs->sort_flags);
		RC_RET_ON_ERR();
	}
	if (fs->emit_create) {
		// 0 when a pure pad before modify_flags.
		rc = as_pack_int64(pk, (int64_t)fs->create_flags);
		RC_RET_ON_ERR();
	}
	if (fs->emit_modify) {
		rc = as_pack_int64(pk, (int64_t)fs->modify_flags);
		RC_RET_ON_ERR();
	}

	return 0;
}

// Pack the inner CDT op blob (no ctx wrapper):
// `[op_code, param0, param1, ..., create_flags?, modify_flags?]`.
// Params come from `cdtprm_push_leaf_elems` in ael_actions.c -- always
// literals post-dyn-removal. AST_LIST entries (transmuted from LIST_SEG,
// or the replace / regexReplace arg wrappers) go through ael_codegen_emit
// like everything else, i.e. QUOTE-wrapped: a raw msgpack list in a param
// slot is an expression to the wire decoder (a numeric list like [1,2,3]
// would decode as an op), so literal lists must ride EXP_QUOTE -- same as
// the client's as_exp list values. `props_flags` is the AST_PROP_* bitmask
// from the path-func node -- when non-zero and the op supports trailing
// flag args at the wire level, the projection appends them (padding
// create_flags=0 where the op shape requires it).
static int
ael_emit_cdt_op_blob(as_packer* pk, ast_pool* pool, const char* input,
		int cdt_op, ast_ref param_head, uint8_t param_count,
		ast_prop_bits props_flags, ast_node_t op_type)
{
	ael_cdt_flag_shape_t fs = ael_cdt_flag_shape(cdt_op, props_flags, op_type);

	int rc = as_pack_list_header(pk, 1 + param_count + fs.extra);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, cdt_op);
	RC_RET_ON_ERR();

	for (ast_ref e = param_head; e != AST_REF_NULL;
			e = ast_pool_at(pool, e)->next) {
		rc = ael_codegen_emit(pk, pool, input, e);
		RC_RET_ON_ERR();
	}

	return ael_emit_cdt_trailing_flags(pk, &fs);
}

static int
emit_path_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);

	if (ast_is_select_family(op->type)) {
		return emit_select_call(pk, pool, input, node);
	}

	// Everything below derives the wire rtype byte from np->etype.
	cf_assert(ast_type_resolved(np->etype), AS_EXP,
			"ael_codegen_emit - path call unresolved at emit");

	// Value-recv path-call: u.call.ctx is a value-expression, not an
	// AST_PATH_CTX list. BITS and HLL calls are always value-recv; CDT calls
	// can be either, depending on the receiver grammar that built them
	// (`(expr).cdtFn(...)` builds value-recv form). One emit serves all three --
	// which flag slots an op has comes from the op, and an HLL op has none.
	if (ast_call_is_value_recv(pool, np)) {
		return emit_bit_call(pk, pool, input, node);
	}

	ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);

	// Bin is the head of the ctx list; segs (if any) chain after it.
	ast_ref bin_ref = cxn->u.ctx_list.head;
	ast_ref ctx_seg_head = ast_pool_at(pool, bin_ref)->next;
	uint32_t ctx_seg_count = ast_ctx_seg_count(cxn);

	// [CALL, rtype, stype, cdt_blob, bin]
	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_etype_to_rtype(np->etype));
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, np->u.call.stype);
	RC_RET_ON_ERR();

	if (ctx_seg_count > 0) {
		rc = ael_pack_ctx(pk, pool, input, ctx_seg_head, ctx_seg_count,
				ael_ctx_path_flags(op));
		RC_RET_ON_ERR();
	}

	rc = ael_emit_cdt_op_blob(pk, pool, input, ast_cdt_op_op_code(op),
			ast_cdt_op_head(op), ast_cdt_op_count(op), ast_cdt_op_props(op),
			op->type);
	RC_RET_ON_ERR();

	return ael_codegen_emit(pk, pool, input, bin_ref);
}

// AST_PROP_* → wire AS_CDT_LIST_* / AS_CDT_MAP_*. The list and map flag
// constants share values where overlapping (NO_FAIL=4, DO_PARTIAL=8), so
// a single projection covers both. NO_OVERWRITE / NO_CREATE are map-only
// at the wire level — list valid masks reject them at parse.
static uint64_t
ael_prop_to_cdt_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_CDT_MAP_NO_FAIL; // == AS_CDT_LIST_NO_FAIL (0x04)
	}
	if ((props & AST_PROP_NO_OVERWRITE) != 0) {
		w |= AS_CDT_MAP_NO_OVERWRITE;
	}
	if ((props & AST_PROP_PARTIAL) != 0) {
		w |= AS_CDT_MAP_DO_PARTIAL; // == AS_CDT_LIST_DO_PARTIAL (0x08)
	}
	if ((props & AST_PROP_NO_CREATE) != 0) {
		w |= AS_CDT_MAP_NO_CREATE;
	}
	// List-only; the apply-time receiver check keeps them off map ops.
	if ((props & AST_PROP_ADD_UNIQUE) != 0) {
		w |= AS_CDT_LIST_ADD_UNIQUE;
	}

	return w;
}

// A CDT op's create_flags slot, in packed-flag space
// (AS_PACKED_MAP_FLAG_* / AS_PACKED_LIST_FLAG_*). Distinct from the CTX-nav
// create space prop_to_ctx_create projects into -- don't conflate them.
// UNORDERED is the packed default, so it projects to 0.
static uint64_t
ael_prop_to_cdt_create_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_CR_KEY_ORDERED) != 0) {
		w |= AS_PACKED_MAP_FLAG_K_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_VALUE_ORDERED) != 0) {
		w |= AS_PACKED_MAP_FLAG_KV_ORDERED;
	}
	if ((props & AST_PROP_CR_ORDERED) != 0) {
		w |= AS_PACKED_LIST_FLAG_ORDERED; // == 0x01
	}
	if ((props & AST_PROP_PERSIST_INDEX) != 0) {
		w |= AS_PACKED_PERSIST_INDEX;
	}
	// UNORDERED has no packed bit -- unordered is the default (0).

	return w;
}

// sort's flag is the op's own optional FLAGS param, not a trailing modify
// slot. 0 when the prop is absent, since a bare sort() emits no flags arg.
static uint64_t
ael_sort_flags(ast_prop_bits props)
{
	return (props & AST_PROP_DROP_DUPS) != 0 ? AS_CDT_SORT_DROP_DUPLICATES : 0;
}

// AST_PROP_* → wire AS_CDT_SELECT_* flag byte (OR'd onto the SELECT
// return-type byte). Only NO_FAIL maps to a wire bit -- SELECT's DO_PARTIAL is
// a declared placeholder that no runtime code acts on.
static uint64_t
ael_prop_to_select_flag(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_CDT_SELECT_NO_FAIL;
	}

	return w;
}

// AST_PROP_CR_* / PERSIST_INDEX → wire AS_CDT_CTX_CREATE_* bits
// (OR'd onto a sub-context type byte). This space is for segments the path
// navigates through; a consumed leaf's ordering goes to the op's own
// create_flags slot instead -- one property, two value spaces.
static uint64_t
prop_to_ctx_create(ast_prop_bits props)
{
	uint64_t w = 0;

	// UNORDERED is container-agnostic: LIST_UNORDERED == MAP_UNORDERED == 0x40.
	// The ordered flavors are container-specific but the apply-time validator
	// guarantees the seg's container matches (ORDERED on a list, KEY[_VALUE]_
	// ORDERED on a map), so emitting the matching wire bit is always correct.
	if ((props & AST_PROP_UNORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_LIST_UNORDERED;
	}
	// The surface says PAD and the wire says UNBOUND: same flag, named for the
	// mechanism on one side and for the removed limit on the other.
	if ((props & AST_PROP_CR_UNSORTED_PAD) != 0) {
		w |= AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND;
	}
	if ((props & AST_PROP_CR_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_LIST_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_MAP_K_ORDERED;
	}
	if ((props & AST_PROP_CR_KEY_VALUE_ORDERED) != 0) {
		w |= AS_CDT_CTX_CREATE_MAP_KV_ORDERED;
	}
	if ((props & AST_PROP_PERSIST_INDEX) != 0) {
		w |= AS_CDT_CTX_CREATE_PERSIST_INDEX;
	}

	return w;
}

// emit_bit_call — pack an EXP_CALL_BITS invocation. Unlike CDT path
// calls, BIT_OPs have no CONTEXT_EVAL / path navigation: the receiver
// is a single value-producing sub-expression (stored in u.call.ctx by
// ael_finalize_bit_call, not an AST_PATH_CTX list).
//   Wire: [CALL, rtype, EXP_CALL_BITS, [op_code, args..., flags?], receiver]
static int
emit_bit_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* op = ast_pool_at(pool, np->u.call.call_op);

	// Of the three families that arrive, only value-recv CDT ops still carry
	// props: a bit op's are in its chain by now, and a string op's mask admits
	// one flag that a receiver with no path is refused. The shape places them,
	// which is what the production packer derives too.
	const ael_cdt_flag_shape_t fs = ael_cdt_flag_shape(ast_cdt_op_op_code(op),
			ast_cdt_op_props(op), op->type);

	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_etype_to_rtype(np->etype));
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, np->u.call.stype);
	RC_RET_ON_ERR();

	uint32_t inner_count = 1 + ast_cdt_op_count(op) + fs.extra;

	rc = as_pack_list_header(pk, inner_count);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, ast_cdt_op_op_code(op));
	RC_RET_ON_ERR();

	for (ast_ref a = ast_cdt_op_head(op); a != AST_REF_NULL;
			a = ast_pool_at(pool, a)->next) {
		rc = ael_codegen_emit(pk, pool, input, a);
		RC_RET_ON_ERR();
	}

	rc = ael_emit_cdt_trailing_flags(pk, &fs);
	RC_RET_ON_ERR();

	return ael_codegen_emit(pk, pool, input, np->u.call.ctx);
}

// post: NULL when the bytes build, else the failing op's name, or "" when the
//       builder named none. The accumulator is drained either way -- a leftover
//       record would misattribute itself to whatever fails next.
//
// NULL bins_info: a sub-program can hold no record bin to depend on, since
// $.bin is rejected while filter_depth > 0 -- only loop vars reach here.
static const char*
sub_program_reject_cause(as_packer* pk, uint32_t start, as_exp_build_error* be)
{
	uint32_t sub_sz = pk->offset - start;
	as_exp* sub = as_exp_build_buf(pk->buffer + start, sub_sz, false, NULL);
	bool took = as_exp_take_build_error(be);

	if (sub != NULL) {
		as_exp_destroy(sub);
		return NULL;
	}

	return took && be->has_op ? be->op : "";
}

// Report what the runtime's compile refuses, against the body's own span.
//
// Reports rather than asserts, though the bytes are ours: the enclosing build
// descends into the bin reference alone, so a body's leaves are compiled here
// first, and this compile checks their content. A geo literal reaches it
// unparsed by design -- well-formed JSON that is not a geometry has been
// checked nowhere before this.
static int
validate_sub_program(as_packer* pk, ast_pool* pool, const ast_node* seg,
		uint32_t start)
{
	if (pk->buffer == NULL || pool->diags == NULL) {
		return 0;
	}

	as_exp_build_error be;
	const char* cause = sub_program_reject_cause(pk, start, &be);

	if (cause == NULL) {
		return 0;
	}

	uint32_t body_offset = seg->u.by_exp_seg.body_offset;
	uint32_t body_sz = seg->u.by_exp_seg.body_sz;

	if (*cause != '\0') {
		ael_diag_addf(pool->diags, AEL_SEV_ERROR, body_offset, body_sz,
				"sub-program rejected at %s", cause);
	}
	else {
		ael_diag_add(pool->diags, AEL_SEV_ERROR, body_offset, body_sz,
				"internal codegen error: invalid sub-program msgpack");
	}

	return -1;
}

// Nothing else round-trips a modify body; its leaves also arrive unchecked.
static int
validate_apply_body(as_packer* pk, ast_pool* pool, uint32_t start, ast_ref body)
{
	if (pk->buffer == NULL || pool->diags == NULL || body == AST_REF_NULL) {
		return 0;
	}

	as_exp_build_error be;
	const char* cause = sub_program_reject_cause(pk, start, &be);

	if (cause == NULL) {
		return 0;
	}

	const ast_node* bn = ast_pool_at(pool, body);

	if (*cause != '\0') {
		ael_diag_addf(pool->diags, AEL_SEV_ERROR, ast_disp_offset(bn), bn->sz,
				"modify body rejected at %s", cause);
	}
	else {
		ael_diag_add(pool->diags, AEL_SEV_ERROR, ast_disp_offset(bn), bn->sz,
				"internal codegen error: invalid modify body msgpack");
	}

	return -1;
}

// A `:NO_FAIL` on a CDT path modify also asks the context walk itself to
// tolerate an absent path: the walk runs before the inner op is unpacked, so the
// op's own NO_FAIL cannot cover a missing intermediate.
//
// Only the postfix on this op counts. A family can be pathed at all only where
// the runtime has a native context for it, so this fires for CDT and string
// modifies; bit and HLL are bin-direct and never reach here.
uint64_t
ael_ctx_path_flags(const ast_node* op_node)
{
	if ((op_node->type != AST_CDT_OP && op_node->type != AST_STR_OP) ||
			! ast_cdt_op_is_modify(op_node)) {
		return 0;
	}

	if ((ast_cdt_op_props(op_node) & AST_PROP_NO_FAIL) == 0) {
		return 0;
	}

	// The aggregate, not one mode: `:NO_FAIL` asks for every mode the wire
	// defines, and gains new ones for free.
	return AS_CDT_CTX_FLAGS_NO_FAIL_ALL;
}

// Walk the segment chain, packing each element's (ctx_type, value) pair. The
// count bounds the walk: a SELECT emits pairs for the segments ahead of its
// leaf, not the whole chain.
static int
pack_ctx_seg_pairs(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref head, uint32_t count)
{
	ast_ref er = head;

	for (uint32_t emitted = 0; er != AST_REF_NULL && emitted < count; emitted++) {
		ast_node* seg = ast_pool_at(pool, er);
		int rc = ael_pack_ctx_seg_pair(pk, pool, input, er);

		RC_RET_ON_ERR();
		er = seg->next;
	}

	return 0;
}

int
ael_pack_ctx(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref ctx_ele_head, uint32_t ctx_seg_count, uint64_t path_flags)
{
	int rc = as_pack_list_header(pk, 3);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, AS_CDT_OP_CONTEXT_EVAL);
	RC_RET_ON_ERR();
	// The [type, val] pairs keep the list even, so the odd count is what tells
	// cdt_context_dig() the leading element is a flags word.
	rc = as_pack_list_header(pk, ctx_seg_count * 2 + (path_flags != 0 ? 1 : 0));
	RC_RET_ON_ERR();

	if (path_flags != 0) {
		rc = as_pack_int64(pk, (int64_t)path_flags);
		RC_RET_ON_ERR();
	}

	return pack_ctx_seg_pairs(pk, pool, input, ctx_ele_head, ctx_seg_count);
}

// Pack a single ctx-pair: (ctx_type, value). Reached from two contexts: the
// CONTEXT_EVAL chain, where parse construction guarantees only single-element
// segs so the BY_EXP / PLURAL / LIST / REL branches stay dormant; and the
// SELECT ctx-pair list, where all branches fire.
// Wire format per branch:
//   BY_EXP (wildcard):    (EXP, true | filter_expr_msgpack)
//   REL_RANGE:            [pivot, start, count?]   count = end-start+1
//   LIST_SEG:             [op0, op1, ...]
//   PLURAL (closed):      [start, end_or_count?]
//                         INDEX/RANK: count = end-start (static fold)
//                         KEY/VALUE INTERVAL: emit end as-is
//   PLURAL (open-end):    [start]
//   single:               operand
int
ael_pack_ctx_seg_pair(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref seg_ref)
{
	ast_node* seg = ast_pool_at(pool, seg_ref);
	// Wide enough for the whole field: flags is uint16_t and carries values up
	// to AST_NF_WHOLE_COLL (0x200), so a byte-wide copy drops the top two bits.
	// Only bits below 0x100 are tested here today, which is why nothing is
	// broken -- but a flag test added later would silently read false.
	uint32_t flags = ast_node_table[seg->type].flags;
	int rc;

	if ((flags & AST_NF_BY_EXP) != 0) {
		rc = as_pack_uint64(pk, AS_CDT_CTX_EXP);
		RC_RET_ON_ERR();

		if (seg->u.by_exp_seg.filter == AST_REF_NULL) {
			return as_pack_bool(pk, true);
		}

		uint32_t start = pk->offset;
		rc = ael_codegen_emit(pk, pool, input, seg->u.by_exp_seg.filter);
		RC_RET_ON_ERR();
		return validate_sub_program(pk, pool, seg, start);
	}

	if ((flags & AST_NF_AND_EXP) != 0) {
		rc = as_pack_uint64(pk, AS_CDT_CTX_AND | AS_CDT_CTX_EXP);
		RC_RET_ON_ERR();

		uint32_t start = pk->offset;
		rc = ael_codegen_emit(pk, pool, input, seg->u.by_exp_seg.filter);
		RC_RET_ON_ERR();
		return validate_sub_program(pk, pool, seg, start);
	}

	int32_t ctx_type = ast_node_table[seg->type].ctx_type;

	if (ctx_type == 0) {
		return -1;
	}

	if ((flags & AST_NF_PLURAL) != 0 && ast_seg_inverted(seg)) {
		ctx_type |= AS_CDT_CTX_INVERTED;
	}

	// Single-select segs can carry CTX_CREATE / PERSIST_INDEX bits via
	// the `:PROPERTY` postfix grammar. OR them into the ctx_type byte —
	// the runtime CONTEXT_EVAL reads these to auto-create intermediate
	// containers when navigating. Only CONTEXT_EVAL reaches here with them
	// set: ael_finalize_select_call rejects create-order on a select path,
	// where the bits would be ignored.
	if (ast_node_table[seg->type].kind == NK_SEG_S) {
		ctx_type |= (int32_t)prop_to_ctx_create(ast_seg_props(seg));
	}

	rc = as_pack_uint64(pk, (uint64_t)ctx_type);
	RC_RET_ON_ERR();

	if ((flags & AST_NF_REL_RANGE) != 0) {
		ast_ref start = seg->u.rel_range_seg.start;
		ast_ref end = seg->u.rel_range_seg.end;
		ast_ref pivot = seg->u.rel_range_seg.relative_to;

		if (end != AST_REF_NULL) {
			int64_t s_v = ast_pool_at(pool, start)->u.ival;
			int64_t e_v = ast_pool_at(pool, end)->u.ival;

			rc = as_pack_list_header(pk, 3);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, pivot);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, start);
			RC_RET_ON_ERR();
			return as_pack_int64(pk, e_v - s_v + 1);
		}

		rc = as_pack_list_header(pk, 2);
		RC_RET_ON_ERR();
		rc = ael_codegen_emit(pk, pool, input, pivot);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, start);
	}

	if ((flags & AST_NF_LIST_SEG) != 0) {
		uint32_t count = seg->u.list_seg.count;

		rc = as_pack_list_header(pk, count);
		RC_RET_ON_ERR();

		for (ast_ref er = seg->u.list_seg.head; er != AST_REF_NULL;
				er = ast_pool_at(pool, er)->next) {
			rc = ael_codegen_emit(pk, pool, input, er);
			RC_RET_ON_ERR();
		}

		return 0;
	}

	if ((flags & AST_NF_PLURAL) != 0) {
		ast_ref start = seg->u.range_seg.start;
		ast_ref end = seg->u.range_seg.end;

		if (end != AST_REF_NULL) {
			rc = as_pack_list_header(pk, 2);
			RC_RET_ON_ERR();
			rc = ael_codegen_emit(pk, pool, input, start);
			RC_RET_ON_ERR();

			int16_t ct = ctx_type & 0x0F;

			if ((ct == AS_CDT_CTX_INDEX_RANGE || ct == AS_CDT_CTX_RANK_RANGE) &&
					ast_pool_at(pool, start)->type == AST_INT &&
					ast_pool_at(pool, end)->type == AST_INT) {
				int64_t s_v = ast_pool_at(pool, start)->u.ival;
				int64_t e_v = ast_pool_at(pool, end)->u.ival;

				return as_pack_int64(pk, e_v - s_v);
			}

			return ael_codegen_emit(pk, pool, input, end);
		}

		rc = as_pack_list_header(pk, 1);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, start);
	}

	return ael_codegen_emit(pk, pool, input, ast_seg_operand(seg));
}

static int
emit_select_call(as_packer* pk, ast_pool* pool, const char* input, ast_ref node)
{
	ast_node* np = ast_pool_at(pool, node);
	ast_node* cxn = ast_pool_at(pool, np->u.call.ctx);

	ast_ref bin_ref = cxn->u.ctx_list.head;
	ast_ref seg_head = ast_pool_at(pool, bin_ref)->next;
	uint32_t seg_count = ast_ctx_seg_count(cxn);

	ael_select_shape_t shape =
			ael_select_shape(pool, np->u.call.call_op, bin_ref);

	// [CALL, rtype, stype, select_blob, bin]
	int rc = as_pack_list_header(pk, 5);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, EXP_CALL);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, (int64_t)shape.rtype);
	RC_RET_ON_ERR();
	rc = as_pack_int64(pk, (int64_t)np->u.call.stype);
	RC_RET_ON_ERR();

	rc = ael_pack_select_op_blob(pk, pool, input, seg_head, seg_count,
			np->u.call.call_op, &shape);
	RC_RET_ON_ERR();

	return ael_codegen_emit(pk, pool, input, bin_ref);
}

ael_select_shape_t
ael_select_shape(ast_pool* pool, ast_ref pf_ref, ast_ref bin_ref)
{
	ael_select_shape_t shape = { .apply_expr = AST_REF_NULL };
	const ast_node* pfp = ast_pool_at(pool, pf_ref);

	const ast_node* bin_canon =
			ast_bin_canonical(pool, ast_pool_at(pool, bin_ref));

	cf_assert(ast_type_resolved(bin_canon->etype), AS_EXP,
			"ael_select_shape - receiver bin unresolved at emit");

	exp_rtype bin_rtype = ast_etype_to_rtype(bin_canon->etype);

	switch (pfp->type) {
	case AST_PATH_FUNC_SELECT:
		shape.sel_type = pfp->u.modify.sel_type;
		shape.is_apply = false;
		switch (shape.sel_type) {
		case AS_CDT_SELECT_TREE:
			shape.rtype = bin_rtype;
			break;
		case AS_CDT_SELECT_COUNT:
			shape.rtype = EXP_RTYPE_INT;
			break;
		case AS_CDT_SELECT_EXISTS:
			shape.rtype = EXP_RTYPE_TRILEAN;
			break;
		default:
			shape.rtype = EXP_RTYPE_LIST;
			break;
		}
		break;
	case AST_PATH_FUNC_MODIFY:
		shape.sel_type = AS_CDT_SELECT_APPLY;
		shape.is_apply = true;
		shape.apply_expr = pfp->u.modify.value;
		shape.rtype = bin_rtype;
		break;
	default: // AST_PATH_FUNC_PSELECT_REMOVE
		shape.sel_type = AS_CDT_SELECT_APPLY;
		shape.is_apply = true;
		shape.rtype = bin_rtype;
		break;
	}

	return shape;
}

int
ael_pack_select_op_blob(as_packer* pk, ast_pool* pool, const char* input,
		ast_ref ctx_seg_head, uint32_t seg_count, ast_ref pf_ref,
		const ael_select_shape_t* shape)
{
	const ast_node* pfp = ast_pool_at(pool, pf_ref);

	// pfp->u.modify.props carries AST_PROP_* internal bits set via the
	// `:PROPERTY` postfix. Project to AS_CDT_SELECT_* before merging with
	// the sel_type byte.
	int64_t flags_int = (int64_t)shape->sel_type |
			(int64_t)ael_prop_to_select_flag(pfp->u.modify.props);

	// select_blob = [SELECT, ctx_pairs_list, flags_int [, apply_expr]]
	uint32_t blob_items = shape->is_apply ? 4 : 3;
	int rc = as_pack_list_header(pk, blob_items);
	RC_RET_ON_ERR();
	rc = as_pack_uint64(pk, AS_CDT_OP_SELECT);
	RC_RET_ON_ERR();
	rc = as_pack_list_header(pk, seg_count * 2);
	RC_RET_ON_ERR();

	rc = pack_ctx_seg_pairs(pk, pool, input, ctx_seg_head, seg_count);
	RC_RET_ON_ERR();

	rc = as_pack_int64(pk, flags_int);
	RC_RET_ON_ERR();

	if (shape->is_apply) {
		uint32_t body_start = pk->offset;

		if (shape->apply_expr != AST_REF_NULL) {
			rc = ael_codegen_emit(pk, pool, input, shape->apply_expr);
		}
		else {
			// PSELECT_REMOVE: synthesize [EXP_RESULT_REMOVE] — the
			// runtime sentinel that drops the matched element.
			rc = emit_cmd(pk, EXP_RESULT_REMOVE, 0);
		}

		RC_RET_ON_ERR();
		rc = validate_apply_body(pk, pool, body_start, shape->apply_expr);
		RC_RET_ON_ERR();
	}

	return rc;
}

//==========================================================
// ael_codegen_emit -- recursive AST-to-msgpack dispatcher.
//

// Depth guard (mirrors ael_count_sz): a left-deep chain is rejected at
// EXP_MAX_DEPTH before C-stack overflow. The sizer pass fails first, so emit
// never sees a too-deep tree. __thread: per-thread, balanced inc/dec.
int
ael_codegen_emit(as_packer* pk, ast_pool* pool, const char* input, ast_ref ref)
{
	static __thread uint32_t depth;

	if (depth >= EXP_MAX_DEPTH) {
		return -1; // surfaced by ael_codegen_pack as a "codegen failed" diag
	}

	depth++;
	cf_defer { depth--; }

	if (ref == AST_REF_NULL) {
		return as_pack_nil(pk);
	}

	ast_node* node = ast_pool_at(pool, ref);
	int rc;

	switch (node->type) {
	case AST_NIL:
	case AST_INF:
	case AST_WILDCARD:
	case AST_BOOL:
	case AST_INT:
	case AST_FLOAT:
	case AST_ZERO:
	case AST_STRING:
	case AST_BLOB:
	case AST_B64_BLOB:
	case AST_GEO_LITERAL:
		return ael_emit_scalar_literal(pk, node);

	case AST_UNKNOWN:
		return emit_cmd(pk, EXP_UNK, 0);

	case AST_BIN:
		cf_assert(ast_type_resolved(node->etype), AS_EXP,
				"ael_codegen_emit - bin unresolved at emit");

		rc = emit_cmd(pk, EXP_BIN, 2);
		RC_RET_ON_ERR();
		rc = as_pack_int64(pk, ast_etype_to_rtype(node->etype));
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset, node->u.bin.name_sz);

	case AST_BIN_REF: {
		ast_node* canonical = ast_pool_at(pool, node->u.bin_ref.bin);

		cf_assert(ast_type_resolved(node->etype), AS_EXP,
				"ael_codegen_emit - bin unresolved at emit");

		rc = emit_cmd(pk, EXP_BIN, 2);
		RC_RET_ON_ERR();
		rc = as_pack_int64(pk, ast_etype_to_rtype(node->etype));
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + canonical->offset,
				canonical->u.bin.name_sz);
	}

	case AST_BIN_TYPE:
		rc = emit_cmd(pk, EXP_BIN_TYPE, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset,
				node->u.bin_type.name_sz);

	case AST_BIN_EXISTS:
		rc = emit_cmd(pk, EXP_BIN_EXISTS, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset,
				node->u.bin_type.name_sz);

	case AST_VAR:
		rc = emit_cmd(pk, EXP_VAR, 1);
		RC_RET_ON_ERR();
		return emit_raw_string(pk, input + node->offset, node->u.var.name_sz);

	case AST_LOOP_VAR: {
		rc = emit_cmd(pk, EXP_VAR_BUILTIN, 2);
		RC_RET_ON_ERR();

		exp_rtype rt = ast_loop_var_rtype(node);

		rc = as_pack_int64(pk, (int64_t)rt);
		RC_RET_ON_ERR();
		return as_pack_int64(pk, (int64_t)node->u.loop_var.builtin);
	}

	case AST_ADD:
	case AST_MUL:
	case AST_FUNC_MAX:
	case AST_FUNC_MIN:
		return emit_nmath(pk, pool, input, ref);

	case AST_AND:
	case AST_OR:
	case AST_BIT_AND:
	case AST_BIT_OR:
	case AST_BIT_XOR:
	case AST_EXCLUSIVE:
		return emit_nary(pk, pool, input, ref);

	case AST_SUB:
	case AST_DIV: {
		int cmd = ast_node_table[node->type].exp_cmd;
		uint32_t nargs = count_left_fold(pool, ref, node->type);
		rc = emit_cmd(pk, cmd, nargs);
		RC_RET_ON_ERR();
		return emit_left_fold(pk, pool, input, ref, node->type);
	}

	case AST_CMP_IN:
	case AST_CMP_EQ:
	case AST_CMP_NE:
	case AST_CMP_GT:
	case AST_CMP_GE:
	case AST_CMP_LT:
	case AST_CMP_LE:
	case AST_CMP_GEO:
	case AST_MOD:
	case AST_POW:
	case AST_LSHIFT:
	case AST_RSHIFT_ARITH:
	case AST_RSHIFT_LOGIC:
	// The 2-arg functions ride the binary path: u.func2.arg1 / arg2 alias
	// u.binary.left / right (same leading ast_ref pair).
	case AST_FUNC_LOG:
	case AST_FUNC_POW:
	case AST_FUNC_FIND_BIT_LEFT:
	case AST_FUNC_FIND_BIT_RIGHT:
		return emit_binary(pk, pool, input, ref);

	// The 1-arg functions ride the unary path: u.func1.arg aliases
	// u.unary.operand (both are the union's single leading ast_ref).
	case AST_NOT:
	case AST_BIT_NOT:
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
	case AST_PATH_FUNC_CAST_STRING:
	case AST_FUNC_ABS:
	case AST_FUNC_CEIL:
	case AST_FUNC_FLOOR:
	case AST_FUNC_COUNT_ONE_BITS:
		rc = emit_cmd(pk, ast_node_table[node->type].exp_cmd, 1);
		RC_RET_ON_ERR();
		return ael_codegen_emit(pk, pool, input, node->u.unary.operand);

	case AST_META:
		return emit_meta(pk, pool, ref);

	case AST_LIST:
		rc = emit_cmd(pk, EXP_QUOTE, 1);
		RC_RET_ON_ERR();
		return ael_emit_list_literal(pk, pool, input, ref, ael_codegen_emit);

	case AST_MAP:
		return ael_emit_map_literal(pk, pool, input, ref, ael_codegen_emit);

	case AST_LET:
		return emit_with(pk, pool, input, ref);

	case AST_WHEN:
		return emit_when(pk, pool, input, ref);

	case AST_PATH_CALL:
		return emit_path_call(pk, pool, input, ref);

	default:
		break;
	}

	return -1;
}

//==========================================================
// Public API.
//

ael_codegen_result
ael_codegen_pack(ast_pool* pool, const char* input, ast_ref root)
{
	ael_codegen_result result = { .buf = NULL, .buf_sz = 0 };

	// Sizer pass — buffer-dependent validation (ordered lists, dup keys) is
	// dormant with no buffer, so run it with diags off. A failure here is a
	// structural check that fires in both passes (e.g. a sub-program whose
	// msgpack does not round-trip), so re-run the sizer with diags on to
	// capture that located diagnostic -- buffer == NULL keeps the
	// buffer-dependent checks dormant -- instead of falling back to the
	// generic message below.
	as_packer sizer = { 0 };
	pool->diags = NULL;

	if (ael_codegen_emit(&sizer, pool, input, root) != 0) {
		as_packer diag_sizer = { 0 };

		pool->diags = &result.diags;
		ael_codegen_emit(&diag_sizer, pool, input, root);
		pool->diags = NULL;

		if (! ael_diag_has_error(&result.diags)) {
			ael_diag_add(&result.diags, AEL_SEV_ERROR, 0, 0,
					"internal error: expression could not be compiled");
		}

		return result;
	}

	uint32_t sz = sizer.offset;
	uint8_t* buf = cf_malloc(sz);

	as_packer pk = { .buffer = buf, .capacity = sz };

	// Emit pass — sub-program validation appends to result.diags via
	// pool->diags. Cleared after to avoid stale pointer.
	pool->diags = &result.diags;

	int rc = ael_codegen_emit(&pk, pool, input, root);

	pool->diags = NULL;

	if (rc != 0) {
		cf_free(buf);

		if (! ael_diag_has_error(&result.diags)) {
			ael_diag_add(&result.diags, AEL_SEV_ERROR, 0, 0,
					"internal error: expression could not be compiled");
		}

		return result;
	}

	result.buf = buf;
	result.buf_sz = pk.offset;
	return result;
}

// Release a ael_codegen_result's heap buffer (cf_malloc'd by ael_codegen_pack). Safe on
// a failed result (buf == NULL — cf_free(NULL) is a no-op). Callers use this
// rather than free()-ing buf directly.
void
ael_codegen_result_destroy(ael_codegen_result* r)
{
	cf_free(r->buf);
	r->buf = NULL;
	r->buf_sz = 0;
	ael_diag_list_destroy(&r->diags);
}
