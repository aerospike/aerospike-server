/*
 * ael_actions.c
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

#include "exp/ael_actions.h"

#include <stdint.h>

#include "log.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "exp/ael_emit.h"
#include "exp/ael_func_table.h"
#include "exp/ael_lexer.h"
#include "exp/ael_parse.h"
#include "exp/exp.h"

//==========================================================
// Typedefs & constants.
//

typedef struct {
	bool consume_leaf;
	ast_node_t leaf_type;
	bool leaf_is_multi;
	bool inverted;
	bool map_ctx;
	// A create-order drained onto the leaf becomes the op's create flags -- but
	// only if the op has a create-flags slot. Carried so the resolver, which is
	// where the op code is known, can reject the combination instead of letting
	// the emitter drop the flag. :UNSORTED_PAD is excluded: it works through
	// the modify word, not the create slot.
	bool leaf_has_order_needing_create_slot;
	ast_etype bare_bin_etype; // container type of a bare-bin receiver, else AUTO
} cdt_pf_leaf_ctx;

typedef struct {
	int cdt_op_code;
	int cdt_ret_type;
	ast_etype call_etype;
} cdt_pf_resolved;

// Walk state for the live-tree search that answers whether a bin is read.
typedef struct {
	ast_pool* pool;
	const ast_node* canon;
	uint32_t depth;
	bool found;
} bin_live_ctx;

//==========================================================
// Forward declarations.
//

// pcl — path-call layer
static int pcl_cdt_op(ast_node_t pf_type, ast_node_t leaf_seg_type, bool map_ctx);
static const char* whole_collection_wrong_container(ast_node_t pf_type,
		ast_etype bare_etype);
static bool pf_modify_needs_leaf(ast_node_t pf_type);

// cdtprm — cdt-op param chain
static void cdtprm_push_leaf_elems(ast_pool* pool, ast_ref cdt_op_ref,
		ast_ref leaf);

// bin_live — is a bin read, rather than only named
static void bin_live_walk(bin_live_ctx* lc, ast_ref ref);

// ctx — path-context list
static ast_ref ctx_pop_tail(ael_context* ctx, ast_ref ctx_ref);
static bool ctx_map_noleaf(ast_pool* pool, ast_ref ctx_ref);
static ast_etype ctx_bare_bin_etype(ast_pool* pool, ast_ref ctx_ref);

// cdt_pf — path-func morph (phases)
static void cdt_pf_derive_leaf_ctx(ael_context* ctx, ast_ref ctx_ref,
		ast_ref seg_ref, bool is_modify, cdt_pf_leaf_ctx* lc);
static bool cdt_pf_validate_multi_select(ael_context* ctx, ast_node_t pf_type,
		const ast_node* fp, const cdt_pf_leaf_ctx* lc);
static bool cdt_pf_resolve_op_and_types(ael_context* ctx, ast_node_t pf_type,
		bool is_modify, ast_node* fp, const cdt_pf_leaf_ctx* lc,
		cdt_pf_resolved* rs);
static void cdt_pf_finalize_wire_chain(ael_context* ctx, ast_node* fp,
		ast_ref pf, ast_ref seg_ref, bool is_mod_target, bool wire_is_read,
		bool consume_leaf, bool inverted, int cdt_ret_type);
static bool ctx_list_has_seg_flags(ael_context* ctx, ast_ref ctx_ref,
		uint32_t mask);

// method-call construction — bit / hll / str / regex
static uint64_t ael_bits_wire_flags(ast_prop_bits props);
static uint64_t ael_hll_wire_flags(ast_prop_bits props);
static void ael_bit_materialize_flags(ael_context* ctx, ast_ref bit_op);
static void ael_hll_materialize_flags(ael_context* ctx, ast_ref hll_op);
static ast_ref ael_finalize_str_call(ael_context* ctx, ast_ref recv,
		ast_ref str_fn);
static ast_ref ael_finalize_str_path(ael_context* ctx, ast_ref ctx_ref,
		ast_ref str_fn);
static bool ael_regex_parse_flags(ael_context* ctx, uint32_t flag_off,
		uint32_t flag_sz, uint64_t* out);
static ast_etype hll_result_etype(ast_node_t pf_type);

// func-call resolution — arg binding + builders
static bool ael_arg_bool(ael_context* ctx, ast_ref ref, int* out);
static bool ael_bind_args(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t fname_off, uint32_t fname_sz, ast_ref arg_list, ast_ref* slots);
static ast_ref ael_build_scalar(ael_context* ctx, const ael_func_spec_t* spec,
		ast_ref* slots);
static ast_ref ael_build_variadic(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t fname_off, uint32_t fname_sz, ast_ref arg_list);
static ast_ref ael_build_bit(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots);
static ast_ref ael_build_hll(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots);
static ast_ref ael_resolve_geo_fn(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref arg_list);
static ast_ref ael_concat_append(ael_context* ctx, ast_ref acc, ast_ref arg);
static void ael_diag_unknown_func(ael_context* ctx, uint32_t name_off,
		uint32_t name_sz);
static ast_ref ael_build_from_spec(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots);
static ast_ref ael_build_str_list(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots);
static ast_ref ael_build_regex_replace(ael_context* ctx,
		const ael_func_spec_t* spec, uint32_t name_off, uint32_t name_sz,
		ast_ref* slots);
static ast_ref ael_resolve_path_fn(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref arg_list);
static ast_ref ael_resolve_modify_fn(ael_context* ctx,
		const ael_func_spec_t* spec, uint32_t name_off, uint32_t name_sz,
		ast_ref arg_list);

// path / value — finalize, cdt-call, cast
static ast_ref ael_finalize_path_on_ctx(ael_context* ctx, ast_ref ctx_ref,
		ast_ref pf);
static ast_ref build_value_cdt_call(ael_context* ctx, ast_ref recv, ast_ref pf,
		int cdt_op_code, bool is_modify, ast_etype recv_etype,
		ast_etype result_etype);
static ast_ref finish_cast(ael_context* ctx, ast_ref recv, ast_ref pf,
		bool is_bin);

// loop var
static void loop_var_chain_push(ael_context* ctx, as_exp_builtin builtin,
		ast_ref r);

// parse driver
static void ael_finalize_parse(ael_context* ctx);

// path-call wrap + method-call family finalize
static ast_ref ael_wrap_path_call(ael_context* ctx, exp_call_stype stype,
		ast_ref recv, ast_ref op, ast_etype etype, ast_ref deferable_from);

// grammar-internal actions — reached only through other actions in this file
// (never called from ael_parser.y directly)
static bool ael_apply_prop(ael_context* ctx, ast_prop_bits valid,
		ast_prop_bits* dst, ast_prop_bits bit, uint32_t prop_offset,
		uint32_t prop_sz);
static bool ael_cdt_op_from_fn(ael_context* ctx, ast_ref ctx_ref,
		ast_ref seg_ref, ast_ref pf);
static ast_ref ael_finalize_path_call_wrap(ael_context* ctx, ast_ref ctx_ref,
		ast_ref pf, exp_call_stype base_stype);
static ast_ref ael_finalize_cast(ael_context* ctx, ast_ref ctx_ref, ast_ref cast);
static ast_ref ael_build_bin_func(ael_context* ctx, ast_ref bin, ast_ref pf);
static ast_ref ael_build_value_func(ael_context* ctx, ast_ref recv, ast_ref pf);
static ast_ref ael_bit_recv_from_ctx(ael_context* ctx, ast_ref ctx_ref);
static ast_ref ael_finalize_bit_call(ael_context* ctx, ast_ref recv,
		ast_ref bit_fn);
static ast_ref ael_new_hll_set_fn(ael_context* ctx, ast_node_t pf_type,
		ast_ref arg);
static ast_ref ael_finalize_hll_call(ael_context* ctx, ast_ref recv,
		ast_ref hll_fn);
static ast_ref ael_new_geo_literal(ael_context* ctx, uint32_t offset,
		uint32_t sz);
static ast_ref ael_new_geo_compare(ael_context* ctx, ast_ref a, ast_ref b);
static ast_ref ael_new_select(ael_context* ctx, as_cdt_select_flags sel_type);
static ast_ref ael_new_modify(ael_context* ctx, ast_ref apply_expr, bool nofail);
static ast_ref ael_new_pselect_remove(ael_context* ctx, bool nofail);
static ast_ref ael_finalize_select_call(ael_context* ctx, ast_ref ctx_ref,
		ast_ref pf);

// post-infer — checks and rewrites keyed on final types
static void ael_check_etype(ael_context* ctx, ast_ref ref, uint32_t depth);
static void ael_post_infer_rewrite(ael_context* ctx, ast_ref ref, uint32_t depth);

//==========================================================
// Static helpers.
//
// Naming: pcl_* path-call layer. cdtprm_* NK_CDT_OP param-chain; cdt_pf_* morph.
// ctx_* / ctx_map_noleaf static path-ctx (vs public ael_ctx_*).
//

// pcl — path-call layer

// Verb dispatch: (path-func verb, leaf dimension) -> CDT op code.
// pre:  leaf_seg_type is the leaf selector's node type (AST_MAP_KEY /
//       AST_LIST_INDEX / ...), or AST_NIL when leafless; map_ctx marks a map
//       container (used only by the leafless clear).
// post: the CDT op for (verb, dimension), or -1 when the verb has no op for
//       that container / dimension -- the caller turns -1 into a diagnostic.
static int
pcl_cdt_op(ast_node_t pf_type, ast_node_t leaf_seg_type, bool map_ctx)
{
	switch (pf_type) {
	case AST_PATH_FUNC_REMOVE: {
		int op = ast_node_table[leaf_seg_type].cdt_remove_op;
		return op != 0 ? op : -1;
	}
	// Element-addressing verbs dispatch on the exact leaf dimension: a map KEY
	// (AST_MAP_KEY) -> map op, a list INDEX (AST_LIST_INDEX) -> list op. The
	// resolver's leaf-dimension gate guarantees the leaf is one of those two
	// before we get here, so the else arm is the list-index case. map_ctx
	// (container only) would wrongly accept a map value/index/rank selector.
	case AST_PATH_FUNC_SET:
		return (leaf_seg_type == AST_MAP_KEY) ? AS_CDT_OP_MAP_PUT
											  : AS_CDT_OP_LIST_SET;
	case AST_PATH_FUNC_INSERT:
		// map: create-only put (NO_OVERWRITE preset by the resolver);
		// list: positional insert (shift).
		return (leaf_seg_type == AST_MAP_KEY) ? AS_CDT_OP_MAP_PUT
											  : AS_CDT_OP_LIST_INSERT;
	case AST_PATH_FUNC_UPDATE:
		// Map-only: update-only put (NO_CREATE preset). No list form.
		return (leaf_seg_type == AST_MAP_KEY) ? AS_CDT_OP_MAP_PUT : -1;
	case AST_PATH_FUNC_APPEND:
		return AS_CDT_OP_LIST_APPEND;
	case AST_PATH_FUNC_APPEND_ITEMS:
		return AS_CDT_OP_LIST_APPEND_ITEMS;
	case AST_PATH_FUNC_INSERT_ITEMS:
		// list: positional bulk insert, needs the index leaf; map: create-only
		// bulk put (NO_OVERWRITE preset by the resolver), leafless. A list
		// receiver without an index has no form -- faulted by the caller.
		if (leaf_seg_type == AST_LIST_INDEX) {
			return AS_CDT_OP_LIST_INSERT_ITEMS;
		}

		return map_ctx ? AS_CDT_OP_MAP_PUT_ITEMS : -1;
	case AST_PATH_FUNC_PUT_ITEMS:
		return AS_CDT_OP_MAP_PUT_ITEMS;
	case AST_PATH_FUNC_UPDATE_ITEMS:
		// Map-only: update-only bulk put (NO_CREATE preset). MAP_REPLACE_ITEMS
		// would fit the verb but carries no flag slot, so NO_FAIL / PARTIAL
		// could not ride along with it.
		return AS_CDT_OP_MAP_PUT_ITEMS;
	case AST_PATH_FUNC_INCREMENT:
		return (leaf_seg_type == AST_MAP_KEY) ? AS_CDT_OP_MAP_INCREMENT
											  : AS_CDT_OP_LIST_INCREMENT;
	case AST_PATH_FUNC_CLEAR:
		return map_ctx ? AS_CDT_OP_MAP_CLEAR : AS_CDT_OP_LIST_CLEAR;
	case AST_PATH_FUNC_SORT:
		return AS_CDT_OP_LIST_SORT;
	default:
		return -1;
	}
}

// Container gate for the leafless whole-collection verbs (append / appendItems
// / sort are list-only; putItems / updateItems are map-only).
// pre:  bare_etype is a bare-bin container etype (from ctx_bare_bin_etype).
// post: the tailored diagnostic when bare_etype is the wrong container for
//       pf_type; NULL when it is fine, not a whole-collection verb, or an AUTO
//       / pathed receiver (no definitive mismatch to fault).
static const char*
whole_collection_wrong_container(ast_node_t pf_type, ast_etype bare_etype)
{
	if (bare_etype == AST_ETYPE_MAP) {
		switch (pf_type) {
		case AST_PATH_FUNC_APPEND:
			return "append requires a list receiver";
		case AST_PATH_FUNC_APPEND_ITEMS:
			return "appendItems requires a list receiver";
		case AST_PATH_FUNC_SORT:
			return "sort requires a list receiver";
		default:
			return NULL;
		}
	}

	if (bare_etype == AST_ETYPE_LIST && pf_type == AST_PATH_FUNC_UPDATE_ITEMS) {
		return "updateItems requires a map receiver";
	}

	if (bare_etype == AST_ETYPE_LIST && pf_type == AST_PATH_FUNC_PUT_ITEMS) {
		return "putItems requires a map receiver (use appendItems on a list)";
	}

	return NULL;
}

// Modify verbs that address a single element: they need an index (list) or key
// (map) leaf, so a bare-collection receiver is an error. Whole-collection verbs
// (append / appendItems / putItems / clear / sort / join -- routed leafless by
// ael_finalize_path_on_ctx) and remove (already rejected leafless via a null op
// code) are excluded. Without this guard, setTo('x') on a bare list emits a
// LIST_SET with the value where the index belongs.
// pre:  pf_type is a resolved path-func verb.
// post: true for the element-addressing modify verbs (set / insert / update /
//       increment) that need a leaf selector; false for the whole-collection
//       verbs and reads.
static bool
pf_modify_needs_leaf(ast_node_t pf_type)
{
	return (ast_node_table[pf_type].flags & AST_NF_NEEDS_LEAF) != 0;
}

// cdtprm — cdt-op param chain

// Append a leaf selector's wire operand(s) to a CDT op's param chain, folding
// range endpoints to counts per the inline cases. Consumes the leaf.
// pre:  cdt_op_ref is the CDT op being built; leaf is its leaf selector seg,
//       or AST_REF_NULL for a leafless whole-collection op (no-op).
// post: the operand(s) are pushed onto cdt_op_ref's chain; leaf is consumed
//       (repurposed as the AST_LIST arg for a list-seg, else released).
static void
cdtprm_push_leaf_elems(ast_pool* pool, ast_ref cdt_op_ref, ast_ref leaf)
{
	if (leaf == AST_REF_NULL) {
		return;
	}

	ast_node* lp = ast_pool_at(pool, leaf);
	uint32_t flags = ast_node_table[lp->type].flags;

	if ((flags & AST_NF_LIST_SEG) != 0) {
		// The two variants alias -- snapshot before re-laying the union.
		ast_ref head = lp->u.list_seg.head;
		ast_ref tail = lp->u.list_seg.tail;
		uint32_t count = lp->u.list_seg.count;

		lp->type = AST_LIST;
		lp->etype = AST_ETYPE_LIST;
		lp->u.list.head = head;
		lp->u.list.tail = tail;
		lp->u.list.count = count;
		lp->u.list.order_override = AEL_ORDER_DEFAULT;

		AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, leaf);
		return;
	}

	if ((flags & AST_NF_REL_RANGE) != 0) {
		// Wire format: relative_to, start, count? (count omitted for
		// open-end). count is end-start+1; both endpoints are static
		// AST_INT (enforced at parse time).
		ast_ref start = lp->u.rel_range_seg.start;
		ast_ref end = lp->u.rel_range_seg.end;
		ast_ref relative_to = lp->u.rel_range_seg.relative_to;

		AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, relative_to);
		AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, start);

		if (end != AST_REF_NULL) {
			int64_t start_v = ast_pool_at(pool, start)->u.ival;
			int64_t end_v = ast_pool_at(pool, end)->u.ival;
			ast_ref count = ast_new_int(pool, end_v - start_v + 1);

			AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, count);
			ast_pool_release(pool, end);
		}
	}
	else if ((flags & AST_NF_PLURAL) != 0) {
		ast_ref start = lp->u.range_seg.start;
		ast_ref end = lp->u.range_seg.end;

		AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, start);

		if (end != AST_REF_NULL) {
			// INDEX_RANGE / RANK_RANGE expect (index, count); KEY_INTERVAL
			// / VALUE_INTERVAL expect (start, end). Translate end → count
			// only for the *_RANGE forms when both endpoints are static.
			// Same-sign is enforced at parse time so end-start is meaningful.
			int16_t ctx_type = ast_node_table[lp->type].ctx_type & 0x0F;

			if ((ctx_type == AS_CDT_CTX_INDEX_RANGE ||
						ctx_type == AS_CDT_CTX_RANK_RANGE) &&
					ast_pool_at(pool, start)->type == AST_INT &&
					ast_pool_at(pool, end)->type == AST_INT) {
				int64_t s_v = ast_pool_at(pool, start)->u.ival;
				int64_t e_v = ast_pool_at(pool, end)->u.ival;
				ast_ref count = ast_new_int(pool, e_v - s_v);

				AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, count);
				ast_pool_release(pool, end);
			}
			else {
				AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, end);
			}
		}
	}
	else {
		AST_CHAIN_PUSH_TAIL(pool, cdt_op_ref, ast_seg_operand(lp));
	}

	ast_pool_release(pool, leaf);
}

// ctx — path-context list (static)

// Detach and return a bin-path's tail segment (the last navigation seg).
// pre:  ctx_ref is an AST_PATH_CTX.
// post: the tail seg, unlinked, with the list's count-- and last_is_multi
//       cleared; AST_REF_NULL when only the bin head remains (nothing to pop).
static ast_ref
ctx_pop_tail(ael_context* ctx, ast_ref ctx_ref)
{
	cf_assert(ctx_ref != AST_REF_NULL, AS_EXP, "ctx_pop_tail: null operand");

	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);

	if (cxn->u.ctx_list.count <= 1) {
		return AST_REF_NULL;
	}

	ast_ref tail = cxn->u.ctx_list.tail;
	ast_ref prev = cxn->u.ctx_list.head;

	while (ast_pool_at(ctx->pool, prev)->next != tail) {
		prev = ast_pool_at(ctx->pool, prev)->next;
	}

	ast_pool_at(ctx->pool, prev)->next = AST_REF_NULL;
	ast_pool_at(ctx->pool, tail)->next = AST_REF_NULL;
	cxn->u.ctx_list.tail = prev;
	cxn->u.ctx_list.count--;
	cxn->u.ctx_list.last_is_multi = false;

	return tail;
}

// True when the receiver is a map with no element leaf consumed: a pathed tail
// that is a map segment, or a bare :MAP bin (tail == head, etype MAP).
// pre:  ctx_ref is an AST_PATH_CTX.
// post: a container-only signal -- true for BOTH a bare map bin and a pathed
//       map-key tail, so it must not be used to reject whole-collection verbs
//       (ctx_bare_bin_etype is the bare-bin-only signal for that).
static bool
ctx_map_noleaf(ast_pool* pool, ast_ref ctx_ref)
{
	ast_node* cxn = ast_pool_at(pool, ctx_ref);
	ast_ref bin = cxn->u.ctx_list.head;

	if (cxn->u.ctx_list.tail != bin) {
		ast_node_t tail_type = ast_pool_at(pool, cxn->u.ctx_list.tail)->type;

		return (ast_node_table[tail_type].flags & AST_NF_MAP_SEG) != 0;
	}

	const ast_node* canon = ast_bin_canonical(pool, ast_pool_at(pool, bin));

	return canon->etype == AST_ETYPE_MAP;
}

// Static container etype of a bare-bin receiver -- the signal for gating the
// leafless whole-collection verbs.
// pre:  ctx_ref is an AST_PATH_CTX.
// post: the bin's canonical etype when the receiver is a bare bin (head ==
//       tail, no navigation segs); AST_ETYPE_AUTO otherwise, since a pathed
//       receiver applies the verb to the navigated element, whose type is not
//       knowable here.
static ast_etype
ctx_bare_bin_etype(ast_pool* pool, ast_ref ctx_ref)
{
	ast_node* cxn = ast_pool_at(pool, ctx_ref);
	ast_ref bin = cxn->u.ctx_list.head;

	if (cxn->u.ctx_list.tail != bin) {
		return AST_ETYPE_AUTO;
	}

	const ast_node* canon = ast_bin_canonical(pool, ast_pool_at(pool, bin));

	return canon->etype;
}

// cdt_pf — path-func morph (phases)

// Phase 1: derive the leaf context from the receiver and optional leaf seg.
// pre:  seg_ref is the leaf selector seg, or AST_REF_NULL when leafless;
//       is_modify marks a mutating verb.
// post: *lc is populated -- consume_leaf plus leaf_type / leaf_is_multi /
//       inverted from the seg (defaults when leafless), and map_ctx +
//       bare_bin_etype from the receiver. Pure derivation, no diagnostics.
static void
cdt_pf_derive_leaf_ctx(ael_context* ctx, ast_ref ctx_ref, ast_ref seg_ref,
		bool is_modify, cdt_pf_leaf_ctx* lc)
{
	lc->consume_leaf = seg_ref != AST_REF_NULL;
	lc->leaf_type = AST_NIL;
	lc->leaf_is_multi = false;
	lc->inverted = false;
	lc->leaf_has_order_needing_create_slot = false;
	lc->bare_bin_etype = ctx_bare_bin_etype(ctx->pool, ctx_ref);

	if (lc->consume_leaf) {
		ast_node* lp = ast_pool_at(ctx->pool, seg_ref);
		uint32_t flags = ast_node_table[lp->type].flags;

		lc->leaf_type = lp->type;
		lc->leaf_has_order_needing_create_slot =
				ast_node_table[lp->type].kind == NK_SEG_S &&
				(ast_seg_props(lp) &
						(AST_PROP_GROUP_CTX_CR & ~AST_PROP_CR_UNSORTED_PAD)) != 0;
		lc->leaf_is_multi = ast_seg_is_multi(lp);
		lc->inverted = ! is_modify && ast_seg_inverted(lp);
		lc->map_ctx = (flags & AST_NF_MAP_SEG) != 0;
	}
	else {
		lc->map_ctx = ctx_map_noleaf(ctx->pool, ctx_ref);
	}
}

// Phase 2: reject a plural (multi-select) leaf for element-addressing verbs.
// pre:  lc is from cdt_pf_derive_leaf_ctx.
// post: false + a diagnostic when a plural leaf feeds set / insert / update /
//       insertItems / increment; true otherwise.
static bool
cdt_pf_validate_multi_select(ael_context* ctx, ast_node_t pf_type,
		const ast_node* fp, const cdt_pf_leaf_ctx* lc)
{
	if (lc->consume_leaf && lc->leaf_is_multi &&
			(pf_type == AST_PATH_FUNC_SET || pf_type == AST_PATH_FUNC_INSERT ||
					pf_type == AST_PATH_FUNC_UPDATE ||
					pf_type == AST_PATH_FUNC_INSERT_ITEMS ||
					pf_type == AST_PATH_FUNC_INCREMENT)) {
		ael_err(ctx, ast_disp_offset(fp), fp->sz,
				"multi-select segment not allowed for this path function");
		return false;
	}
	return true;
}

// Phase 3: resolve the CDT op, wire ret-type, and result etype into *rs.
// pre:  lc is from cdt_pf_derive_leaf_ctx; fp is the transient path-func node
//       (its props may carry :postfix flags).
// post: on success fills rs->{cdt_op_code, cdt_ret_type, call_etype} and
//       returns true (may also set write-intent props and pin bulk-item
//       args); on an unsupported verb / dimension / container, returns false
//       after a tailored diagnostic.
static bool
cdt_pf_resolve_op_and_types(ael_context* ctx, ast_node_t pf_type, bool is_modify,
		ast_node* fp, const cdt_pf_leaf_ctx* lc, cdt_pf_resolved* rs)
{
	if (is_modify) {
		if (! lc->consume_leaf && pf_modify_needs_leaf(pf_type)) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					lc->map_ctx
							? "this function needs a map key leaf, e.g. $.bin.key.setTo(v)"
							: "this function needs an index leaf, e.g. $.bin.[0].setTo(v)");
			return false;
		}

		// Element-addressing verbs address ONE element and need a single map key
		// (AST_MAP_KEY) or list index (AST_LIST_INDEX) leaf. A wrong-dimension
		// single selector (map value/index/rank, list value/rank) has no
		// put/set/increment op and would otherwise mis-emit (e.g. $.m.{1}.setTo
		// -> LIST_SET on a map). Plural leaves are already rejected upstream by
		// cdt_pf_validate_multi_select.
		if (pf_modify_needs_leaf(pf_type) && lc->leaf_type != AST_MAP_KEY &&
				lc->leaf_type != AST_LIST_INDEX) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					"this function needs a single map key or list index leaf");
			return false;
		}

		// Whole-collection verbs on a bare bin typed to the wrong container get
		// a tailored message (vs the generic "bin type conflict" from the later
		// op-code -> bin-type pin). Only the bare-bin case is definitive; a
		// pathed receiver applies the verb to the navigated element.
		const char* wc_msg =
				whole_collection_wrong_container(pf_type, lc->bare_bin_etype);

		if (wc_msg != NULL) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz, wc_msg);
			return false;
		}

		rs->cdt_op_code = pcl_cdt_op(pf_type, lc->leaf_type, lc->map_ctx);

		// A create-order that reaches an op with no create-flags slot would be
		// dropped at emit -- the same silent no-op the create-order path-shape
		// rules already reject. List set / insert are the cases: their wire ops
		// take no create word, while append / add and the map put family do.
		//
		// :UNSORTED_PAD is exempt because it does not ride the create slot:
		// it suppresses the INSERT_BOUNDED modify bit, which every padding-
		// capable list write carries, so it still has an effect here.
		// The order may have landed on the leaf (which becomes the op's position
		// arg) or straight on the op itself, so check both sources.
		bool op_has_order_needing_slot =
				(fp->u.cdt_op.props &
						(AST_PROP_GROUP_CTX_CR & ~AST_PROP_CR_UNSORTED_PAD)) != 0;

		if ((lc->leaf_has_order_needing_create_slot || op_has_order_needing_slot) &&
				rs->cdt_op_code >= 0 &&
				! ael_cdt_op_has_create_flags_slot(rs->cdt_op_code)) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					"this write cannot create its container — use append or add, or create it on an outer segment");
			return false;
		}

		if (rs->cdt_op_code < 0) {
			const char* msg;

			switch (pf_type) {
			case AST_PATH_FUNC_UPDATE:
				msg = "update requires a map key (use setTo on a list)";
				break;
			case AST_PATH_FUNC_INSERT_ITEMS:
				msg = "insertItems on a list requires an index leaf";
				break;
			default:
				msg = "path function not supported on this segment";
				break;
			}

			ael_err(ctx, ast_disp_offset(fp), fp->sz, msg);
			return false;
		}

		// A verb serving both containers carries the union of the two flag
		// masks in the node table, because the receiver isn't known until the
		// leaf is resolved -- here. Only the container-exclusive flags need
		// re-checking; NO_FAIL and PARTIAL are valid on either.
		if (lc->map_ctx && (fp->u.cdt_op.props & AST_PROP_ADD_UNIQUE) != 0) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					"ADD_UNIQUE is a list-only flag");
			return false;
		}

		// Verb-carried write intent: preset the create/update flag, which
		// lands in the MAP_PUT slot at emit.
		if (pf_type == AST_PATH_FUNC_UPDATE ||
				pf_type == AST_PATH_FUNC_UPDATE_ITEMS) {
			fp->u.cdt_op.props |= AST_PROP_NO_CREATE;
		}
		else if ((pf_type == AST_PATH_FUNC_INSERT ||
						 pf_type == AST_PATH_FUNC_INSERT_ITEMS) &&
				lc->leaf_type != AST_LIST_INDEX) {
			fp->u.cdt_op.props |= AST_PROP_NO_OVERWRITE;
		}

		// :PARTIAL alone is a dead encoding on a CDT op -- the runtime reads
		// do_partial only inside its no_fail branch, so without NO_FAIL the op
		// hard-fails and the flag the author wrote does nothing. Arm it.
		//
		// Here rather than at the postfix itself, because this runs after every
		// postfix on the node: an author who writes both gets their own NO_FAIL
		// applied first, so the implicit one never reads as a duplicate. §17
		// calls that pair redundant, not invalid. Bit ops never reach this
		// resolution step, which is the BLOB carve-out -- there PARTIAL clips to
		// the end of the blob and stands on its own.
		if ((fp->u.cdt_op.props & AST_PROP_PARTIAL) != 0) {
			fp->u.cdt_op.props |= AST_PROP_NO_FAIL;
		}

		// Bulk *Items: pin the collection arg (chain head now; index
		// leaf is HEAD-pushed later) so a dynamic bin resolves. A
		// literal already carries the etype.
		if (pf_type == AST_PATH_FUNC_APPEND_ITEMS ||
				pf_type == AST_PATH_FUNC_INSERT_ITEMS ||
				pf_type == AST_PATH_FUNC_PUT_ITEMS ||
				pf_type == AST_PATH_FUNC_UPDATE_ITEMS) {
			ast_ref items = ast_cdt_op_head(fp);

			if (items != AST_REF_NULL) {
				bool is_list = pf_type == AST_PATH_FUNC_APPEND_ITEMS ||
						(pf_type == AST_PATH_FUNC_INSERT_ITEMS &&
								lc->leaf_type == AST_LIST_INDEX);

				ast_set_implicit_type(ctx, items,
						is_list ? AST_ETYPE_LIST : AST_ETYPE_MAP);
			}
		}

		rs->cdt_ret_type = RESULT_TYPE_NONE;
		// Real rtype (the mutated container's type) is set later by
		// finalize_path_call_wrap, once the root bin type is known.
		rs->call_etype = AST_ETYPE_NIL;
		return true;
	}

	if (pf_type == AST_PATH_FUNC_EXISTS) {
		rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;
		rs->cdt_ret_type = RESULT_TYPE_EXISTS;
		rs->call_etype = AST_ETYPE_TRILEAN;
		return true;
	}

	if (pf_type == AST_PATH_FUNC_COUNT) {
		if (lc->consume_leaf) {
			// single → multi-tail: get_by_X(mp, ret=COUNT).
			rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;
			rs->cdt_ret_type = RESULT_TYPE_COUNT;
		}
		else {
			// All-single (or bare bin): emit the polymorphic SIZE op.
			// Runtime dispatches to LIST_SIZE / MAP_SIZE based on the
			// navigated particle's actual type, so the bin doesn't need
			// a `:LIST` / `:MAP` pin. Non-CDT leaf → clean runtime error.
			rs->cdt_op_code = AS_CDT_OP_SIZE;
			rs->cdt_ret_type = 0;
		}
		rs->call_etype = AST_ETYPE_INT;
		return true;
	}

	if (pf_type == AST_PATH_FUNC_JOIN) {
		// Whole-list read: no leaf/ret_type. Wire = [op, sep]. The
		// separator rides the chain (ael_resolve_path_fn); result is
		// always STR regardless of the list element type.
		rs->cdt_op_code = AS_CDT_OP_STRING_LIST_JOIN;
		rs->cdt_ret_type = 0;
		rs->call_etype = AST_ETYPE_STR;
		return true;
	}

	// getKeys() / getKeyValues() use the get_by_X fast path only when the
	// tail seg is multi-select (range / REL range / list). Bare-bin and
	// all-single paths can't produce a key collection from a single
	// element — reject with a hint pointing at the bare-path form.
	if (pf_type == AST_PATH_FUNC_GET_KEYS) {
		if (! lc->consume_leaf || ! lc->leaf_is_multi) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					".getKeys() requires a multi-select segment — use the bare path for a single navigated value");
			return false;
		}

		rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;
		rs->cdt_ret_type = RESULT_TYPE_KEY;
		rs->call_etype = AST_ETYPE_LIST;
		return true;
	}

	// getMaps() — key->value map return shape. Like getKeys() it needs a
	// multi-select leaf to have a key collection to wrap; the default is the
	// ordered-map shape, and the :UNORDERED postfix opts out to unordered.
	// The leaf must be a map selector ({...}): only map get-ops produce a
	// RESULT_TYPE_*_MAP, list get-ops reject it (-AS_ERR_OP_NOT_APPLICABLE),
	// so a list selector is rejected here at parse time rather than failing at
	// run time. Wildcard / inner-multi-select route to SELECT and are rejected
	// there (the SELECT_LEAF_MAP_* shapes are deferred — ael-TODO).
	if (pf_type == AST_PATH_FUNC_GET_MAPS) {
		if (! lc->consume_leaf || ! lc->leaf_is_multi) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					".getMaps() requires a multi-select segment — use the bare path for a single navigated value");
			return false;
		}

		if (! lc->map_ctx) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					".getMaps() requires a map selector ({...}) — a list selector has no keys to return as a map");
			return false;
		}

		bool unordered = (ast_cdt_op_props(fp) & AST_PROP_UNORDERED) != 0;

		rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;
		rs->cdt_ret_type = unordered ? RESULT_TYPE_UNORDERED_MAP
									 : RESULT_TYPE_ORDERED_MAP;
		rs->call_etype = AST_ETYPE_MAP;
		return true;
	}

	// getIndexes() / getRanks() — positional return shapes. Unlike getKeys()
	// a single-select leaf is allowed (the position of one navigated element);
	// a leaf range / list returns a list of positions. The :REVERSE postfix
	// flips to the reverse shape. Wildcard / inner-multi-select leaves are
	// routed to SELECT upstream and rejected there (deferred this release).
	if (pf_type == AST_PATH_FUNC_GET_INDEXES ||
			pf_type == AST_PATH_FUNC_GET_RANKS) {
		if (! lc->consume_leaf) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					"getIndexes() / getRanks() require a path segment selecting element(s)");
			return false;
		}

		bool rev = (ast_cdt_op_props(fp) & AST_PROP_REVERSE) != 0;

		rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;

		if (pf_type == AST_PATH_FUNC_GET_INDEXES) {
			rs->cdt_ret_type = rev ? RESULT_TYPE_REVINDEX : RESULT_TYPE_INDEX;
		}
		else {
			rs->cdt_ret_type = rev ? RESULT_TYPE_REVRANK : RESULT_TYPE_RANK;
		}

		rs->call_etype = lc->leaf_is_multi ? AST_ETYPE_LIST : AST_ETYPE_INT;
		return true;
	}

	// getKeyValues() is always routed through SELECT (LEAF_MAP_KEY_VALUE) by
	// ael_consume_leaf_then_finalize, so it never reaches this fast-path
	// resolver — same as getTree(). Its flat-[k,v]-list result can't be
	// produced by a single get_by_X op (a map result can't hold duplicate
	// keys from a multi-source select), so there is no fast path here.

	// Implicit GET synthesized by the bare-path operand rule (no other
	// codepath produces AST_PATH_FUNC_GET). Always RESULT_TYPE_VALUE: the
	// alternative return shapes have no DSL surface this release (see
	// docs/ael-TODO.md).
	rs->cdt_op_code = ast_node_table[lc->leaf_type].cdt_get_op;
	rs->cdt_ret_type = RESULT_TYPE_VALUE;

	if (lc->leaf_is_multi) {
		rs->call_etype = AST_ETYPE_LIST;
	}
	else {
		// etype either resolved (from `:T` pin or earlier
		// cross-narrow), or AUTO. AUTO is rejected at codegen by
		// ast_type_resolved(np->etype); the earlier implementation
		// silently defaulted the wire rtype byte to STR (map ctx) or
		// INT (list ctx), masking the unresolved state and causing
		// runtime-only errors.
		rs->call_etype = fp->etype;
	}
	return true;
}

// Phase 4: build pf's wire param chain from the resolved op.
// pre:  phases 1-3 resolved without error; wire_is_read / is_mod_target
//       classify the op; cdt_ret_type is rs->cdt_ret_type.
// post: a read HEAD-pushes [ret_type (| INVERTED), leaf elems...]; a
//       modify-target HEAD-pushes the leaf's single operand; unused seg nodes
//       are released.
static void
cdt_pf_finalize_wire_chain(ael_context* ctx, ast_node* fp, ast_ref pf,
		ast_ref seg_ref, bool is_mod_target, bool wire_is_read,
		bool consume_leaf, bool inverted, int cdt_ret_type)
{
	if (wire_is_read) {
		if (consume_leaf) {
			int rt_value = cdt_ret_type;

			if (inverted) {
				rt_value |= AS_CDT_OP_FLAG_INVERTED;
			}

			AST_CHAIN_PUSH_HEAD(ctx->pool, pf, ast_new_int(ctx->pool, rt_value));
			cdtprm_push_leaf_elems(ctx->pool, pf, seg_ref);
		}
		else if (seg_ref != AST_REF_NULL) {
			// Empty-designator leaf demoted to no-consume by
			// cdt_pf_derive_leaf_ctx; release the unused seg node.
			ast_pool_release(ctx->pool, seg_ref);
		}
	}
	else if (is_mod_target) {
		if (! consume_leaf) {
			// Empty-designator leaf reached a modify-target op
			// (e.g. $.m.{}.set(x)). cdt_pf_derive_leaf_ctx already
			// emitted a diag; just avoid dereferencing operand=NULL.
			if (seg_ref != AST_REF_NULL) {
				ast_pool_release(ctx->pool, seg_ref);
			}
			return;
		}

		if (seg_ref == AST_REF_NULL) {
			return;
		}

		ast_node* lp = ast_pool_at(ctx->pool, seg_ref);

		// A consumed leaf like .k:KEY_VALUE_ORDERED describes the
		// container this modify op creates -- its ordering belongs in the
		// op's create_flags wire slot, not a CTX-nav byte. (Intermediate
		// segs stay in the ctx and keep their prop_to_ctx_create handling,
		// so nothing is projected twice.)
		if (ast_node_table[lp->type].kind == NK_SEG_S) {
			fp->u.cdt_op.props |= ast_seg_props(lp) & AST_PROP_GROUP_CTX_CR;
		}

		AST_CHAIN_PUSH_HEAD(ctx->pool, pf, ast_seg_operand(lp));
		ast_pool_release(ctx->pool, seg_ref);
	}
}

//==========================================================
// ael_actions — public API.
//

// Relative-range {S:E~K} wire count is end - start + 1 (int64). Reject a span
// whose count overflows — e.g. {-1 : INT64_MAX ~ K}, and even
// {0 : INT64_MAX ~ K} since the +1 tips it — so neither cdtprm_push_leaf_elems
// nor codegen ever computes the overflow. Callers guarantee both endpoints
// are AST_INT.
bool
ael_rel_range_count_ok(ael_context* ctx, ast_ref start, ast_ref end)
{
	int64_t start_v = ast_pool_at(ctx->pool, start)->u.ival;
	int64_t end_v = ast_pool_at(ctx->pool, end)->u.ival;
	int64_t count;

	if (__builtin_sub_overflow(end_v, start_v, &count) ||
			__builtin_add_overflow(count, 1, &count)) {
		// Anchor on the start..end operand span, not the parser lookahead.
		const ast_node* sp = ast_pool_at(ctx->pool, start);
		const ast_node* ep = ast_pool_at(ctx->pool, end);
		uint32_t s_off = ast_disp_offset(sp);
		uint32_t e_off = ast_disp_offset(ep);
		uint32_t off = s_off < e_off ? s_off : e_off;
		uint32_t s_end = s_off + sp->sz;
		uint32_t e_end = e_off + ep->sz;

		ael_err(ctx, off, (e_end > s_end ? e_end : s_end) - off,
				"relative range endpoints too far apart — count = end - start + 1 overflows");
		return false;
	}

	return true;
}

// ptype — particle type (expr → particle)

int64_t
ael_etype_to_particle_type(ast_etype etype)
{
	switch (etype) {
	case AST_ETYPE_INT:
		return AS_PARTICLE_TYPE_INTEGER;
	case AST_ETYPE_FLOAT:
		return AS_PARTICLE_TYPE_FLOAT;
	case AST_ETYPE_STR:
		return AS_PARTICLE_TYPE_STRING;
	case AST_ETYPE_BLOB:
		return AS_PARTICLE_TYPE_BLOB;
	case AST_ETYPE_TRILEAN:
		return AS_PARTICLE_TYPE_BOOL;
	case AST_ETYPE_HLL:
		return AS_PARTICLE_TYPE_HLL;
	case AST_ETYPE_MAP:
		return AS_PARTICLE_TYPE_MAP;
	case AST_ETYPE_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case AST_ETYPE_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	default:
		return AS_PARTICLE_TYPE_NULL;
	}
}

// `:PROPERTY` postfix — generic apply helper. Validates the incoming
// bit against the per-attachment valid mask, detects duplicates and
// mutual-exclusion within group masks (one CR_LIST_* per seg,
// one CR_MAP_* per seg). On success, ORs the bit into *dst.
static bool
ael_apply_prop(ael_context* ctx, ast_prop_bits valid, ast_prop_bits* dst,
		ast_prop_bits bit, uint32_t prop_offset, uint32_t prop_sz)
{
	if ((bit & valid) == 0) {
		ael_err(ctx, prop_offset, prop_sz, "property not valid for this node");
		return false;
	}

	if ((*dst & bit) != 0) {
		ael_err(ctx, prop_offset, prop_sz, "duplicate property");
		return false;
	}

	if ((bit & AST_PROP_GROUP_CR_ORDER) != 0 &&
			(*dst & AST_PROP_GROUP_CR_ORDER) != 0) {
		ael_err(ctx, prop_offset, prop_sz, "only one create-order per segment");
		return false;
	}

	*dst |= bit;
	return true;
}

// Ordering postfix on a collection literal. The property is a plain TOK_NAME
// (not a keyword), so it's matched here by text. Sets the AST_LIST / AST_MAP
// node's order_override; the packer resolves the default per kind (map ->
// ordered, list -> unordered). Names follow the container, as they do on a path
// segment.
void
ael_apply_literal_order(ael_context* ctx, ast_ref ref, uint32_t off, uint32_t sz)
{
	const char* s = ctx->input + off;
	bool is_map = ast_pool_at(ctx->pool, ref)->type == AST_MAP;
	ael_order_override order = AEL_ORDER_DEFAULT;

	if (is_map) {
		if (ael_name_eq("KEY_ORDERED", s, sz)) {
			ael_err(ctx, off, sz,
					"key-ordered is a map literal's default - drop the suffix");
			return;
		}

		// A surface restriction, not an encoding limit --
		// AS_PACKED_MAP_FLAG_KV_ORDERED exists and order_override has a spare
		// value, so this would be a third case in ael_emit_map_literal.
		if (ael_name_eq("KEY_VALUE_ORDERED", s, sz)) {
			ael_err(ctx, off, sz,
					"a map literal cannot be key-value ordered - create it on a path segment instead");
			return;
		}

		if (ael_name_eq("UNORDERED", s, sz)) {
			order = AEL_ORDER_UNORDERED;
		}
	}
	else if (ael_name_eq("SORTED", s, sz)) {
		order = AEL_ORDER_ORDERED;
	}
	else if (ael_name_eq("UNSORTED", s, sz)) {
		order = AEL_ORDER_UNORDERED;
	}

	if (order == AEL_ORDER_DEFAULT) {
		ael_err(ctx, off, sz,
				is_map ? "a map literal's only order suffix is UNORDERED"
					   : "list literal order must be SORTED or UNSORTED");
		return;
	}

	ast_pool_at(ctx->pool, ref)->u.list.order_override = order;
}

// `:PROPERTY` postfix flags are ordinary identifiers (TOK_NAME), not keywords.
// Map the name to its AST_PROP_* bit; the per-attachment valid_props mask is
// checked later in ael_apply_prop. Returns 0 (and emits a diagnostic) for an
// unknown name.
ast_prop_bits
ael_resolve_prop_flag(ael_context* ctx, uint32_t off, uint32_t sz)
{
	static const struct {
		const char* name;
		ast_prop_bits bit;
	} table[] = {
		{ "NO_FAIL", AST_PROP_NO_FAIL },
		{ "CREATE_ONLY", AST_PROP_CREATE_ONLY },
		{ "UPDATE_ONLY", AST_PROP_UPDATE_ONLY },
		{ "PARTIAL", AST_PROP_PARTIAL },
		{ "ADD_UNIQUE", AST_PROP_ADD_UNIQUE },
		{ "REVERSE", AST_PROP_REVERSE },
		// Map orders. UNORDERED also selects getMaps()' unordered return shape.
		{ "UNORDERED", AST_PROP_UNORDERED },
		{ "KEY_ORDERED", AST_PROP_CR_KEY_ORDERED },
		{ "KEY_VALUE_ORDERED", AST_PROP_CR_KEY_VALUE_ORDERED },
		// List orders. UNSORTED shares UNORDERED's bit, so the spelling is what
		// separates them -- by container in ael_drain_create_props, by name in
		// ael_node_apply_postfix.
		{ "SORTED", AST_PROP_CR_ORDERED },
		{ "UNSORTED", AST_PROP_UNORDERED },
		{ "UNSORTED_PAD", AST_PROP_CR_UNSORTED_PAD },
		{ "PERSIST_INDEX", AST_PROP_PERSIST_INDEX },
		{ "DROP_DUPS", AST_PROP_DROP_DUPS },
	};

	const char* s = ctx->input + off;

	for (uint32_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
		if (ael_name_eq(table[i].name, s, sz)) {
			return table[i].bit;
		}
	}

	ael_err(ctx, off, sz, "unknown property flag");
	return 0;
}

// `$.NAME(...)` record-level call. The name is an ordinary identifier
// (TOK_NAME), not a keyword — dispatched here to the record key accessor or a
// metadata function. has_param is true for the `$.NAME(INT)` form (only
// digestModulo). Emits a diagnostic + returns a nil node on any mismatch.
ast_ref
ael_resolve_meta_call(ael_context* ctx, uint32_t off, uint32_t sz,
		int64_t param, bool has_param)
{
	const char* s = ctx->input + off;

	// $.key() — record key accessor; etype defaults to AUTO_KEY and narrows
	// by comparison / cast context. Takes no call argument.
	if (sz == 3 && memcmp(s, "key", 3) == 0) {
		if (has_param) {
			ael_err(ctx, off, sz, "key() takes no arguments");
			return ast_new_nil(ctx->pool);
		}

		ast_ref r = ast_new_meta(ctx->pool, EXP_REC_KEY, AST_ETYPE_AUTO_KEY);

		// Chain onto ctx->key_meta_root (via the unused u.meta.param) so
		// the post-parse pass can reject a key whose type never resolved.
		ast_pool_at(ctx->pool, r)->u.meta.param = (int64_t)ctx->key_meta_root;
		ctx->key_meta_root = r;

		return r;
	}

	static const struct {
		const char* name;
		exp_op_code op_code;
	} table[] = {
		{ "deviceSize", EXP_META_DEVICE_SIZE },
		{ "memorySize", EXP_META_MEMORY_SIZE },
		{ "recordSize", EXP_META_RECORD_SIZE },
		{ "isTombstone", EXP_META_IS_TOMBSTONE },
		{ "keyExists", EXP_META_KEY_EXISTS },
		{ "lastUpdateTime", EXP_META_LAST_UPDATE },
		{ "timeSinceLastUpdate", EXP_META_SINCE_UPDATE },
		{ "setName", EXP_META_SET_NAME },
		{ "ttl", EXP_META_TTL },
		{ "voidTime", EXP_META_VOID_TIME },
		{ "digestModulo", EXP_META_DIGEST_MOD },
	};

	for (uint32_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
		if (! ael_name_eq(table[i].name, s, sz)) {
			continue;
		}

		// digestModulo is the only metadata builtin with an argument; the
		// op-table's static_param_count is the single source of truth.
		bool takes_param = exp_op_table[table[i].op_code].static_param_count != 0;

		if (takes_param && ! has_param) {
			ael_err(ctx, off, sz, "digestModulo requires an integer argument");
			return ast_new_nil(ctx->pool);
		}

		if (! takes_param && has_param) {
			ael_err(ctx, off, sz, "this metadata function takes no arguments");
			return ast_new_nil(ctx->pool);
		}

		// Match build_meta_digest_mod: op->mod is int32_t, so a literal
		// that truncates to 0 would divide by zero at eval.
		if (takes_param && (int32_t)param == 0) {
			ael_err(ctx, off, sz, "digestModulo cannot modulo by zero");
			return ast_new_nil(ctx->pool);
		}

		return ast_new_meta(ctx->pool, table[i].op_code, takes_param ? param : 0);
	}

	ael_err(ctx, off, sz, "unknown record/metadata function");
	return ast_new_nil(ctx->pool);
}

// UNSORTED_PAD lands on UNORDERED: the padding is a separate wire bit, and what
// it asks for is an unsorted list either way. PERSIST_INDEX names no order.
static ast_esubtype
ael_create_order_subtype(ast_prop_bits bit)
{
	switch (bit) {
	case AST_PROP_UNORDERED:
	case AST_PROP_CR_UNSORTED_PAD:
		return AST_ESUBTYPE_UNORDERED;
	case AST_PROP_CR_ORDERED:
	case AST_PROP_CR_KEY_ORDERED:
		return AST_ESUBTYPE_ORDERED;
	case AST_PROP_CR_KEY_VALUE_ORDERED:
		return AST_ESUBTYPE_KEY_VALUE_ORDERED;
	default:
		return AST_ESUBTYPE_UNSET;
	}
}

// A parked flag nothing claimed.
static void
report_unplaced_create(ael_context* ctx, ast_prop_bits props, uint32_t off,
		uint32_t sz)
{
	if (props == 0) {
		return;
	}

	ael_err(ctx, off, sz,
			"create-order has no container to create here — it names the container a following segment navigates into");
}

// Whatever is still in the slots, for the callers that have not taken it out.
static void
report_unplaced_create_slots(ael_context* ctx)
{
	bool from_here = ctx->here_create_props != 0;

	report_unplaced_create(ctx,
			ctx->carried_create_props | ctx->here_create_props,
			from_here ? ctx->here_create_offset : ctx->carried_create_offset,
			from_here ? ctx->here_create_sz : ctx->carried_create_sz);
}

// Called at the open of an argument list or a filter body.
bool
ael_create_park_push(ael_context* ctx)
{
	ctx->park_level++;

	if ((ctx->here_create_props | ctx->carried_create_props) == 0) {
		return true;
	}

	if (ctx->park_n == AEL_CREATE_PARK_MAX) {
		ael_err(ctx, ctx->here_create_offset, ctx->here_create_sz,
				"expression too deeply nested");
		return false;
	}

	ctx->park_stack[ctx->park_n++] = (ael_create_park){
		.level = ctx->park_level,
		.here_props = ctx->here_create_props,
		.here_offset = ctx->here_create_offset,
		.here_sz = ctx->here_create_sz,
		.carried_props = ctx->carried_create_props,
		.carried_offset = ctx->carried_create_offset,
		.carried_sz = ctx->carried_create_sz,
		.carried_owner = ctx->carried_create_owner,
	};

	ctx->here_create_props = 0;
	ctx->carried_create_props = 0;
	ctx->carried_create_owner = AST_REF_NULL;

	return true;
}

void
ael_create_park_pop(ael_context* ctx)
{
	// A flag the window parked for itself has nowhere left to go, and the restore
	// below would overwrite it unreported.
	report_unplaced_create_slots(ctx);

	if (ctx->park_n != 0 &&
			ctx->park_stack[ctx->park_n - 1].level == ctx->park_level) {
		const ael_create_park* p = &ctx->park_stack[--ctx->park_n];

		ctx->here_create_props = p->here_props;
		ctx->here_create_offset = p->here_offset;
		ctx->here_create_sz = p->here_sz;
		ctx->carried_create_props = p->carried_props;
		ctx->carried_create_offset = p->carried_offset;
		ctx->carried_create_sz = p->carried_sz;
		ctx->carried_create_owner = p->carried_owner;
	}
	else {
		ctx->here_create_props = 0;
		ctx->carried_create_props = 0;
		ctx->carried_create_owner = AST_REF_NULL;
	}

	if (ctx->park_level != 0) {
		ctx->park_level--;
	}
}

// Every create-order spelling names a container kind, including the one bit that
// UNSORTED and UNORDERED share -- there the spelling is all there is to go on.
static ast_etype
ael_create_order_etype(ael_context* ctx, ast_prop_bits bit, uint32_t off,
		uint32_t sz)
{
	switch (bit) {
	case AST_PROP_UNORDERED:
		return ael_name_eq("UNSORTED", ctx->input + off, sz) ? AST_ETYPE_LIST
															 : AST_ETYPE_MAP;
	case AST_PROP_CR_ORDERED:
	case AST_PROP_CR_UNSORTED_PAD:
		return AST_ETYPE_LIST;
	case AST_PROP_CR_KEY_ORDERED:
	case AST_PROP_CR_KEY_VALUE_ORDERED:
		return AST_ETYPE_MAP;
	default:
		return AST_ETYPE_AUTO; // an index hint narrows nothing
	}
}

// Unified `:VALUE` postfix apply. Dispatches via the node-info table:
// AEL_POSTFIX_PROP routes by node kind to u.{cdt_op,seg,modify}.props
// and validates against info->valid_props; AEL_POSTFIX_TYPE writes to
// the node header's etype, validated against info->valid_types with
// a "duplicate pin" check that preserves the existing single-pin
// semantic. Always applied to the pre-morph node — bit_fn / hll_fn /
// x_cdt_fn / modify_fn / path_seg — so the table row matches the
// actual op or seg kind.
ast_ref
ael_node_apply_postfix(ael_context* ctx, ast_ref ref, ael_postfix_kind kind,
		uint32_t bit)
{
	if (ref == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* np = ast_pool_at(ctx->pool, ref);
	const ast_node_info* info = &ast_node_table[np->type];

	// Diagnostics point at the postfix-value token snapshotted by the
	// type_name / prop_flag reductions, so chained pins
	// (`:INT:FLOAT`, `:NO_FAIL:NO_FAIL`) flag the offending postfix
	// value rather than the start of the node being decorated.
	uint32_t diag_off = ctx->postfix_token_offset;
	uint32_t diag_sz = ctx->postfix_token_sz;

	if (kind == AEL_POSTFIX_TYPE) {
		ast_etype t = (ast_etype)bit;

		// @key narrowing: AST_LOOP_VAR.valid_types is AUTO (any
		// concrete type) at the table level, but @key specifically
		// only accepts INT / STR / BLOB (the keys a map can carry).
		// This is the only node type whose effective valid_types
		// depends on an instance field — if more cases arise, factor
		// into a per-instance valid_types slot or callback.
		if (np->type == AST_LOOP_VAR &&
				np->u.loop_var.builtin == AS_EXP_BUILTIN_KEY &&
				(t & AST_ETYPE_AUTO_KEY) == 0) {
			ael_err(ctx, diag_off, diag_sz,
					"@key cast must be int, str, or blob");
			return AST_REF_NULL;
		}

		// A create-order on this element has already fixed its container kind,
		// so a pin that disagrees is a conflict, not a second pin.
		if (np->esubtype != AST_ESUBTYPE_UNSET && (t & np->etype) == 0) {
			ast_diag_type_conflict(ctx, diag_off, diag_sz,
					"create-order type conflict", np->etype, t);
			return AST_REF_NULL;
		}

		// Duplicate check first: a chained `:T1:T2` where T1 already
		// pinned and T2 differs reads as "duplicate type pin" even if
		// T2 happens to be outside the table's valid_types mask
		// (which would also reject it via the next check). Keeps the
		// chained-pin diagnostic specific.
		if (ast_type_resolved(np->etype) && np->etype != t) {
			ael_err(ctx, diag_off, diag_sz, "duplicate type pin");
			return AST_REF_NULL;
		}

		if ((t & ~info->valid_types) != 0) {
			ael_err(ctx, diag_off, diag_sz, "type pin not valid at this node");
			return AST_REF_NULL;
		}

		np->etype = t;
		return ref;
	}

	// A ctx-create property is parked rather than applied here: its slot belongs
	// to the next path element, whose selector also fixes the container kind, and
	// a bin root has no props slot at all. See ael_context for the shift.
	//
	// AST_PROP_UNORDERED is overloaded, so the position guard matters -- a
	// create-order on a bin or segment, but the getMaps() return shape on a
	// method call, where it applies here as usual.
	bool on_bin = np->type == AST_BIN || np->type == AST_BIN_REF;
	bool on_seg = info->kind == NK_SEG_S || info->kind == NK_SEG_M;

	if (((ast_prop_bits)bit & AST_PROP_GROUP_CTX_CR) != 0 && (on_bin || on_seg)) {
		ast_prop_bits* slot = on_bin ? &ctx->carried_create_props
									 : &ctx->here_create_props;

		// Dropped rather than merged with this one, which would read as two
		// orders written on one element.
		if (on_bin && ctx->carried_create_props != 0 &&
				ctx->carried_create_owner != ref) {
			report_unplaced_create(ctx, ctx->carried_create_props,
					ctx->carried_create_offset, ctx->carried_create_sz);
			ctx->carried_create_props = 0;
		}

		if ((*slot & (ast_prop_bits)bit) != 0) {
			ael_err(ctx, diag_off, diag_sz, "duplicate property");
			return AST_REF_NULL;
		}

		if (((ast_prop_bits)bit & AST_PROP_GROUP_CR_ORDER) != 0 &&
				(*slot & AST_PROP_GROUP_CR_ORDER) != 0) {
			ael_err(ctx, diag_off, diag_sz, "only one create-order per segment");
			return AST_REF_NULL;
		}

		// The order names this element's container, so it narrows the element's
		// type the way a pin would. A bin needs no help: the segment that
		// follows it implies its kind regardless.
		if (on_seg) {
			ast_etype kind = ael_create_order_etype(ctx, (ast_prop_bits)bit,
					diag_off, diag_sz);

			if ((np->etype & kind) == 0) {
				ast_diag_type_conflict(ctx, diag_off, diag_sz,
						"create-order type conflict", np->etype, kind);
				return AST_REF_NULL;
			}

			np->etype &= kind;
		}

		*slot |= (ast_prop_bits)bit;

		ast_esubtype sub = ael_create_order_subtype((ast_prop_bits)bit);

		if (sub != AST_ESUBTYPE_UNSET) {
			// A bin's mentions all name the one container, so its order unifies
			// the way its type does. The spec is silent on branches that name
			// different orders; this is the strict reading, which can be relaxed
			// without invalidating anything already written. A segment has no
			// identity to unify on -- a computed key has no value until eval.
			if (on_bin) {
				ast_node* canon = ast_bin_canonical(ctx->pool, np);

				if (canon->esubtype != AST_ESUBTYPE_UNSET &&
						canon->esubtype != sub) {
					ael_err(ctx, diag_off, diag_sz,
							"another mention of this bin names a different create-order");
					return AST_REF_NULL;
				}

				canon->esubtype = sub;
			}

			np->esubtype = sub;
		}

		if (on_bin) {
			ctx->carried_create_offset = diag_off;
			ctx->carried_create_sz = diag_sz;
			ctx->carried_create_owner = ref;
		}
		else {
			ctx->here_create_offset = diag_off;
			ctx->here_create_sz = diag_sz;
		}

		return ref;
	}

	// getMaps() takes only :UNORDERED, but UNSORTED shares its bit so the mask
	// admits both. A method call has no container to judge by, and this is the
	// last point where the spelling still exists.
	if ((ast_prop_bits)bit == AST_PROP_UNORDERED &&
			info->valid_props == AST_PROP_VALID_GET_MAPS &&
			ael_name_eq("UNSORTED", ctx->input + diag_off, diag_sz)) {
		ael_err(ctx, diag_off, diag_sz,
				"UNSORTED is a list flag — getMaps() returns a map, use UNORDERED");
		return AST_REF_NULL;
	}

	// Pick the accumulator slot by kind.
	ast_prop_bits* dst;

	if (ast_is_select_family(np->type)) {
		dst = &np->u.modify.props;
	}
	else if (info->kind == NK_CDT_OP) {
		dst = &np->u.cdt_op.props;
	}
	else if (info->kind == NK_SEG_S || info->kind == NK_SEG_M) {
		dst = &np->u.seg.props;
	}
	else {
		ael_err(ctx, diag_off, diag_sz, "property not valid at this node");
		return AST_REF_NULL;
	}

	if (! ael_apply_prop(ctx, info->valid_props, dst, (ast_prop_bits)bit,
				diag_off, diag_sz)) {
		return AST_REF_NULL;
	}

	return ref;
}

// Move a parked ctx-create property onto the segment that navigates INTO the
// container it names. Internally a segment's create bits order the container one
// level up (cdt_context_fill_create), so the flag's home is the segment after
// the one it was written on -- this is that shift, and it is also the first
// point where the container's kind is known.
//
// pre:  cxn is the AST_PATH_CTX being built; sp is the segment about to be
//       pushed. ctx->carried_create_props holds at most one create-order,
//       optionally stacked with PERSIST_INDEX.
// post: pending is cleared, and on success its bits are OR'd into sp's props.
//       Returns false with a diagnostic if the flag names the wrong container
//       kind, or if PERSIST_INDEX names anything but the bin's own container.
// The two travel in one prop group and are refused by the same rules, so naming
// the group sends an author who wrote only the index hint after a flag they
// never used.
static const char*
ael_create_prop_name(ast_prop_bits props)
{
	return (props & ~AST_PROP_PERSIST_INDEX) == 0 ? ":PERSIST_INDEX"
												  : "a create-order flag";
}

// pre:  pending is non-zero; off/sz span the suffix as the author wrote it.
// post: false with a diagnostic when the order names a kind the container being
//       created is not, or when PERSIST_INDEX is claimed for a container other
//       than the bin's own.
static bool
ael_create_props_fit(ael_context* ctx, ast_prop_bits pending, uint32_t off,
		uint32_t sz, bool is_map, bool is_bin_container)
{
	ast_prop_bits ok = is_map ? AST_PROP_CR_ORDER_MAP : AST_PROP_CR_ORDER_LIST;
	// UNSORTED and UNORDERED share a bit, so the wrong *spelling* has to be
	// caught by text rather than by mask.
	bool wrong_spelling = (pending & AST_PROP_UNORDERED) != 0 &&
			ael_name_eq(is_map ? "UNSORTED" : "UNORDERED", ctx->input + off, sz);

	if ((pending & AST_PROP_GROUP_CR_ORDER & ~ok) != 0 || wrong_spelling) {
		const char* msg = is_map
				? "map segment order must be KEY_ORDERED, KEY_VALUE_ORDERED, or UNORDERED"
				: "list segment order must be SORTED, UNSORTED, or UNSORTED_PAD";

		ael_err(ctx, off, sz, msg);
		return false;
	}

	// The persisted index is the bin's own, and the CDT layer rejects it on any
	// later context element.
	if ((pending & AST_PROP_PERSIST_INDEX) != 0 && ! is_bin_container) {
		ael_err(ctx, off, sz, "PERSIST_INDEX only allowed on the bin");
		return false;
	}

	return true;
}

static bool
ael_drain_create_props(ael_context* ctx, const ast_node* cxn, ast_node* sp)
{
	ast_prop_bits pending = ctx->carried_create_props;
	uint32_t off = ctx->carried_create_offset;
	uint32_t sz = ctx->carried_create_sz;
	ast_ref owner = ctx->carried_create_owner;

	// This segment's own suffix becomes what the next one is owed, and it is owed
	// to this chain -- the segment that wrote it is the one being appended.
	ctx->carried_create_props = ctx->here_create_props;
	ctx->carried_create_offset = ctx->here_create_offset;
	ctx->carried_create_sz = ctx->here_create_sz;
	ctx->carried_create_owner = cxn->u.ctx_list.head;
	ctx->here_create_props = 0;

	if (pending == 0) {
		return true;
	}

	// Another chain's flag, and that chain is done with it -- so it has nowhere
	// left to go, and this path is not the place.
	if (owner != cxn->u.ctx_list.head) {
		report_unplaced_create(ctx, pending, off, sz);
		return false;
	}

	// A multi-select segment cannot create anything, and has no props slot to
	// park this in either -- u.seg overlaps by_exp_seg.filter on a wildcard, so
	// writing it there corrupts the filter ref. Stating the rule rather than
	// advising a single-select path, which some verbs cannot have.
	if (ast_node_table[sp->type].kind != NK_SEG_S) {
		ael_errf(ctx, off, sz, "a multi-select path cannot carry %s",
				ael_create_prop_name(pending));
		return false;
	}

	// The container being created is the one this segment reaches into, so this
	// segment's dimension is what fixes list vs map -- not the position the
	// suffix was written on.
	if (! ael_create_props_fit(ctx, pending, off, sz,
				(ast_node_table[sp->type].flags & AST_NF_MAP_SEG) != 0,
				cxn->u.ctx_list.count == 1)) {
		return false;
	}

	sp->u.seg.props |= pending;

	return true;
}

// Which path functions actually consume create bits. Verified against the
// runtime: a context-create on a remove() is accepted by the server and does
// nothing -- the container is not materialised -- and the same holds for the
// other non-creating terminals. Only these verbs make a create-order reach
// anything.
static bool
pf_creates_containers(ast_node_t pf_type)
{
	switch (pf_type) {
	case AST_PATH_FUNC_SET:
	case AST_PATH_FUNC_INSERT:
	case AST_PATH_FUNC_UPDATE:
	case AST_PATH_FUNC_INCREMENT:
	case AST_PATH_FUNC_APPEND:
	case AST_PATH_FUNC_APPEND_ITEMS:
	case AST_PATH_FUNC_INSERT_ITEMS:
	case AST_PATH_FUNC_PUT_ITEMS:
	case AST_PATH_FUNC_UPDATE_ITEMS:
		return true;
	default:
		return false;
	}
}

// Which of the leafless creating verbs builds a map. The container a leafless
// verb writes into is the one the path already named, so the verb is what fixes
// its dimension -- the bulk map puts take a map order, the list appends a list
// one. The element-addressing verbs are absent because they need a leaf, which
// the drain refuses before asking.
static bool
pf_creates_map(ast_node_t pf_type)
{
	switch (pf_type) {
	case AST_PATH_FUNC_PUT_ITEMS:
	case AST_PATH_FUNC_UPDATE_ITEMS:
	// The leafless spelling is the create-only bulk put; with an index leaf it
	// is the list form, which never reaches here.
	case AST_PATH_FUNC_INSERT_ITEMS:
		return true;
	default:
		return false;
	}
}

// Drain a parked ctx-create property at the end of a path, where there is no
// following segment to take it.
//
// pre:  pf is the path-function node; leaf is the popped leaf segment, or
//       AST_REF_NULL when the verb consumes the whole collection; ctx_ref is
//       the path the verb ends. *to_op_r is initialised by the caller.
// post: pending is cleared. For a leafless verb the bits land on the op, which
//       creates the container the last segment named, and *to_op_r is set --
//       only ever set, never cleared, since nothing can read the bits back off
//       the op. With a leaf, the suffix sat on the op's own key or index -- a
//       value, not a container -- so it fails with a diagnostic.
static bool
ael_drain_create_props_to_op(ael_context* ctx, ast_ref ctx_ref, ast_ref pf,
		ast_ref leaf, bool* to_op_r)
{
	ast_prop_bits pending = ctx->carried_create_props;

	if (pending == 0) {
		return true;
	}

	const ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);

	// Another chain wrote this and is done with it, so it is not this verb's to
	// spend -- a sibling arm of the same when(), most often, whose flag would
	// otherwise reach the wire as this arm's create order.
	if (ctx->carried_create_owner != cxn->u.ctx_list.head) {
		ctx->carried_create_props = 0;
		report_unplaced_create(ctx, pending, ctx->carried_create_offset,
				ctx->carried_create_sz);
		return false;
	}

	if (leaf != AST_REF_NULL) {
		ctx->carried_create_props = 0;
		ctx->here_create_props = 0;
		ael_err(ctx, ctx->carried_create_offset, ctx->carried_create_sz,
				"create-order must name a container — this segment is the value being written");
		return false;
	}

	ast_node* pfp = ast_pool_at(ctx->pool, pf);

	// Anything that is not a CDT op takes no create flags. Leave the flag
	// parked rather than reporting here -- ael_finalize_parse catches every
	// unplaced flag in one place, including the terminals that never reach this
	// funnel.
	if (ast_node_table[pfp->type].kind != NK_CDT_OP) {
		return true;
	}

	// The terminal gate reports this one, and reads the flag out of the slots to
	// do it.
	if (! pf_creates_containers(pfp->type)) {
		return true;
	}

	// Nothing reads it again once the resolver refuses the missing leaf, so
	// leaving it parked only earns a second diagnostic from the closing sweep.
	if ((ast_node_table[pfp->type].flags & AST_NF_NEEDS_LEAF) != 0) {
		ctx->carried_create_props = 0;
		ctx->here_create_props = 0;
		return true;
	}

	if (! ael_create_props_fit(ctx, pending, ctx->carried_create_offset,
				ctx->carried_create_sz, pf_creates_map(pfp->type),
				cxn->u.ctx_list.count == 1)) {
		ctx->carried_create_props = 0;
		ctx->here_create_props = 0;
		return false;
	}

	ctx->carried_create_props = 0;
	ctx->here_create_props = 0;
	pfp->u.cdt_op.props |= pending;
	*to_op_r = true;

	return true;
}

ast_ref
ael_ctx_list_append(ael_context* ctx, ast_ref ctx_ref, ast_ref seg)
{
	if (ctx_ref == AST_REF_NULL || seg == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* sp = ast_pool_at(ctx->pool, seg);
	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);

	if ((uint32_t)cxn->u.ctx_list.count + 1 > AST_PATH_CTX_MAX) {
		ael_err(ctx, ast_disp_offset(sp), sp->sz,
				"path context exceeds maximum depth");
		return AST_REF_NULL;
	}

	if (! ael_drain_create_props(ctx, cxn, sp)) {
		return AST_REF_NULL;
	}

	AST_CTXCHAIN_PUSH_TAIL(ctx->pool, ctx_ref, seg);

	if (! cxn->u.ctx_list.is_multi) {
		if (cxn->u.ctx_list.last_is_multi) {
			cxn->u.ctx_list.is_multi = true;
		}
		cxn->u.ctx_list.last_is_multi = ast_seg_is_multi(sp);
	}

	return ctx_ref;
}

void
ael_pin_path_root_etype(ael_context* ctx, ast_ref head, ast_ref seg)
{
	if (head == AST_REF_NULL || seg == AST_REF_NULL) {
		return;
	}

	ast_node* hn = ast_pool_at(ctx->pool, head);

	// Only the value loop var. @key is int / str / blob and @index is pinned
	// INT, so neither is a container -- stamping one here would assert a type
	// the node cannot have.
	if (hn->type != AST_LOOP_VAR ||
			hn->u.loop_var.builtin != AS_EXP_BUILTIN_VALUE) {
		return;
	}

	ast_node* sp = ast_pool_at(ctx->pool, seg);
	bool is_map = (ast_node_table[sp->type].flags & AST_NF_MAP_SEG) != 0;

	// Narrow, not assign -- an explicit (@:T) has to survive to be refused.
	ast_set_implicit_type(ctx, head, is_map ? AST_ETYPE_MAP : AST_ETYPE_LIST);
}

bool
ael_check_path_root_is_container(ael_context* ctx, ast_ref head)
{
	if (head == AST_REF_NULL) {
		return true;
	}

	ast_node* hn = ast_pool_at(ctx->pool, head);

	if ((hn->etype & AST_ETYPE_AUTO_CDT) != 0) {
		return true;
	}

	ael_err(ctx, ast_disp_offset(hn), hn->sz,
			"a path receiver must be a list or map");
	return false;
}

ast_ref
ael_ctx_list_pop(ael_context* ctx, ast_ref ctx_ref)
{
	if (ctx_ref == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref tail = ctx_pop_tail(ctx, ctx_ref);

	if (tail == AST_REF_NULL) {
		ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
		ast_ref bin = cxn->u.ctx_list.head;
		const ast_node* bp = ast_pool_at(ctx->pool, bin);

		ael_err(ctx, ast_disp_offset(bp), bp->sz,
				"path function requires a leaf segment");
	}

	return tail;
}

// morph — path func → cdt op

// Morph a transient AST_PATH_FUNC_* node into a resolved AST_CDT_OP,
// binding it to the path context and (possibly consumed) leaf seg.
static bool
ael_cdt_op_from_fn(ael_context* ctx, ast_ref ctx_ref, ast_ref seg_ref, ast_ref pf)
{
	ast_node* fp = ast_pool_at(ctx->pool, pf);
	ast_node_t pf_type = fp->type;
	bool is_modify = (ast_node_table[pf_type].flags & AST_NF_MODIFY) != 0;
	bool is_mod_target = (ast_node_table[pf_type].flags & AST_NF_MOD_TARGET) != 0;
	bool wire_is_read = ! is_modify || pf_type == AST_PATH_FUNC_REMOVE;

	cdt_pf_leaf_ctx lc;
	cdt_pf_derive_leaf_ctx(ctx, ctx_ref, seg_ref, is_modify, &lc);

	if (! cdt_pf_validate_multi_select(ctx, pf_type, fp, &lc)) {
		return false;
	}

	cdt_pf_resolved rs;

	if (! cdt_pf_resolve_op_and_types(ctx, pf_type, is_modify, fp, &lc, &rs)) {
		return false;
	}

	cdt_pf_finalize_wire_chain(ctx, fp, pf, seg_ref, is_mod_target,
			wire_is_read, lc.consume_leaf, lc.inverted, rs.cdt_ret_type);

	fp->type = AST_CDT_OP;
	fp->u.cdt_op.op_code = (uint16_t)rs.cdt_op_code;
	fp->u.cdt_op.is_modify = is_modify;
	fp->u.cdt_op.leaf_consumed = lc.consume_leaf;
	fp->etype = rs.call_etype;
	return true;
}

// path — path-call finalize / operands

// Span a just-created AST_PATH_CALL over its whole construct: from the
// receiver's start to the parser's current lookahead, which at the wrapping
// reduce is the first token AFTER the call (whitespace-trimmed). The wrapper
// is created mid-reduce, so ast_new's default stamp is that same lookahead: a
// zero-width anchor past the end of the construct. Call-op runtime traces
// (string / CDT / bit / HLL faults and explains) focus on this span, so it has
// to cover the call rather than sit after it.
static void
ael_span_call(ael_context* ctx, ast_ref pc_ref, ast_ref recv)
{
	const ast_node* rp = ast_pool_at(ctx->pool, recv);
	const char* in = ctx->input;
	// The receiver is usually a bin, whose offset is its name -- span the call
	// from where the reference is DISPLAYED ('$.'), not from the name.
	uint32_t start = ast_disp_offset(rp);
	uint32_t end = ctx->pool->cur_offset;

	while (end > start &&
			(in[end - 1] == ' ' || in[end - 1] == '\t' || in[end - 1] == '\n' ||
					in[end - 1] == '\r')) {
		end--;
	}

	if (end > start) {
		ast_set_span(ctx->pool, pc_ref, start, end - start);
	}
	else {
		ast_set_span(ctx->pool, pc_ref, start, rp->sz);
	}
}

static void
ael_err_inert_no_fail(ael_context* ctx, const ast_node* np)
{
	ael_err(ctx, ast_disp_offset(np), np->sz,
			"NO_FAIL has nowhere to ride here — this op has no flags word of its own, and no path segment for it to tolerate");
}

// The receiver is deliberately not part of the test: a leaf-consuming verb pops
// its segment into the op before this runs, so a written path is no longer
// visible to look at, and a segless op that does have a flags word -- append(),
// insert() -- is legitimate.
static bool
ael_reject_inert_no_fail(ael_context* ctx, const ast_node* pfp)
{
	if (pfp->type != AST_CDT_OP ||
			(ast_cdt_op_props(pfp) & AST_PROP_NO_FAIL) == 0 ||
			ael_cdt_op_has_modify_flags_slot(ast_cdt_op_op_code(pfp))) {
		return false;
	}

	ael_err_inert_no_fail(ctx, pfp);
	return true;
}

// Wrap a resolved op + path context into the AST_PATH_CALL node the rest
// of the AST machinery treats uniformly.
static ast_ref
ael_finalize_path_call_wrap(ael_context* ctx, ast_ref ctx_ref, ast_ref pf,
		exp_call_stype base_stype)
{
	cf_assert(ctx_ref != AST_REF_NULL && pf != AST_REF_NULL, AS_EXP,
			"ael_finalize_path_call_wrap: null operand");

	ast_node* pfp = ast_pool_at(ctx->pool, pf);
	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
	ast_ref bin = cxn->u.ctx_list.head;

	if (cxn->u.ctx_list.is_multi || cxn->u.ctx_list.last_is_multi) {
		const ast_node* bp = ast_pool_at(ctx->pool, bin);

		ael_err(ctx, ast_disp_offset(bp), bp->sz,
				"multi-select segment not allowed in path context");
		return AST_REF_NULL;
	}

	if (cxn->u.ctx_list.tail == bin && ael_reject_inert_no_fail(ctx, pfp)) {
		return AST_REF_NULL;
	}

	ast_etype seg_type;

	{
		if (cxn->u.ctx_list.tail != bin) {
			ast_node* bp = ast_pool_at(ctx->pool, bin);
			ast_node_t first_seg_type = bp->next != AST_REF_NULL
					? ast_pool_at(ctx->pool, bp->next)->type
					: AST_NIL;

			seg_type = (ast_node_table[first_seg_type].flags & AST_NF_MAP_SEG)
					? AST_ETYPE_MAP
					: AST_ETYPE_LIST;
		}
		else if (pfp->type == AST_CDT_OP) {
			// Bare-bin or leaf-consuming call: the resolved op_code
			// (LIST_* vs MAP_*) pins the bin's CDT shape — used for
			// e.g. .append() / .clear() / .sort() with no path seg.
			// AS_CDT_OP_SIZE is polymorphic — leave the bin AUTO_CDT
			// so the runtime resolves list-vs-map from the particle.
			if (ast_cdt_op_op_code(pfp) == AS_CDT_OP_SIZE) {
				seg_type = AST_ETYPE_AUTO_CDT;
			}
			else {
				seg_type = IS_CDT_LIST_OP(ast_cdt_op_op_code(pfp))
						? AST_ETYPE_LIST
						: AST_ETYPE_MAP;
			}
		}
		else {
			seg_type = AST_ETYPE_AUTO_CDT;
		}

		ast_bin_set_implicit_type(ctx, bin, seg_type);
	}

	exp_call_stype stype = base_stype;

	if (ast_cdt_op_is_modify(pfp)) {
		stype |= EXP_CALL_FLAG_MODIFY_LOCAL;
		// Modify returns the mutated container; the call rtype is the
		// bin's type, never NIL (as the builder does for .modify()). Holds for
		// a nested string modify too -- string_modify_ctx writes the leaf back
		// and unwinds the parent headers, so the bin is what changed.
		pfp->etype = seg_type;
	}

	return ael_wrap_path_call(ctx, stype, ctx_ref, pf, pfp->etype, bin);
}

// True if any segment in the ctx_list (excluding the bin head) requires
// SELECT routing -- wildcards (BY_EXP), AND_EXP post-filters, or anything
// flagged AST_NF_PLURAL (ranges, intervals, REL ranges, list-form segs).
// Mid-path multi-select forces SELECT routing because CONTEXT_EVAL (the
// CDT_OP chain navigator) cannot carry a multi-select operand or an
// AND_EXP modifier. Leaf-only multi-select stays on CDT_OP for the
// binary-search optimization on K_ORDERED bins.
// pre:  ctx_ref is an AST_PATH_CTX (the leaf seg is not yet appended).
// post: true if any seg after the bin head is a wildcard / AND_EXP / plural
//       selector -- a mid-path multi-select that forces SELECT routing.
static bool
ctx_list_has_seg_flags(ael_context* ctx, ast_ref ctx_ref, uint32_t mask)
{
	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);

	for (ast_ref er = ast_pool_at(ctx->pool, cxn->u.ctx_list.head)->next;
			er != AST_REF_NULL; er = ast_pool_at(ctx->pool, er)->next) {
		ast_node* sp = ast_pool_at(ctx->pool, er);

		if ((ast_node_table[sp->type].flags & mask) != 0) {
			return true;
		}
	}

	return false;
}

// First seg after the bin head carrying a create-order property, or
// AST_REF_NULL.
static ast_ref
ctx_list_seg_with_create_props(ael_context* ctx, ast_ref ctx_ref)
{
	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);

	for (ast_ref er = ast_pool_at(ctx->pool, cxn->u.ctx_list.head)->next;
			er != AST_REF_NULL; er = ast_pool_at(ctx->pool, er)->next) {
		ast_node* sp = ast_pool_at(ctx->pool, er);

		if (ast_node_table[sp->type].kind == NK_SEG_S &&
				(ast_seg_props(sp) & AST_PROP_GROUP_CTX_CR) != 0) {
			return er;
		}
	}

	return AST_REF_NULL;
}

// A flag parked elsewhere in the program belongs to the path that wrote it,
// placed or not.
static bool
create_owed_to(ael_context* ctx, ast_ref ctx_ref)
{
	return ctx->carried_create_props != 0 &&
			ctx->carried_create_owner ==
			ast_pool_at(ctx->pool, ctx_ref)->u.ctx_list.head;
}

// Asked of the path, not of the parse -- though an argument reaches this funnel
// with the enclosing write's flag hidden, so what is parked here is this path's.
//
// The drain reports its own case rather than the op being inspected, because
// create-order bits are shared with flags meaning something else on some verbs
// -- getMaps() and :UNORDERED among them -- so the intent cannot be read back.
static bool
path_has_create_order(ael_context* ctx, ast_ref ctx_ref, ast_ref leaf,
		bool drained_to_op)
{
	if (drained_to_op || create_owed_to(ctx, ctx_ref) ||
			ctx_list_seg_with_create_props(ctx, ctx_ref) != AST_REF_NULL) {
		return true;
	}

	if (leaf == AST_REF_NULL) {
		return false;
	}

	const ast_node* lp = ast_pool_at(ctx->pool, leaf);

	return ast_node_table[lp->type].kind == NK_SEG_S &&
			(ast_seg_props(lp) & AST_PROP_GROUP_CTX_CR) != 0;
}

// Shared because two funnels reach a terminal: the string, bit and HLL families
// compose their own path call and never pass through the leaf-consuming one.
static bool
reject_create_order(ael_context* ctx, ast_ref ctx_ref, ast_ref recv,
		ast_ref leaf, bool drained_to_op)
{
	if (ctx_ref == AST_REF_NULL) {
		// No path, so only a suffix written on this bin is still owed -- an
		// enclosing write's flag is hidden for the length of the argument list,
		// which is what keeps a bare-receiver method usable inside one.
		if (ctx->carried_create_props == 0 || ctx->carried_create_owner != recv) {
			return false;
		}

		ctx->carried_create_props = 0;
		ael_err(ctx, ctx->carried_create_offset, ctx->carried_create_sz,
				"this terminal cannot create a container — drop the create-order");

		return true;
	}

	if (! path_has_create_order(ctx, ctx_ref, leaf, drained_to_op)) {
		return false;
	}

	ast_ref cr_seg = ctx_list_seg_with_create_props(ctx, ctx_ref);
	uint32_t off = ctx->carried_create_offset;
	uint32_t sz = ctx->carried_create_sz;

	if (cr_seg != AST_REF_NULL) {
		const ast_node* crp = ast_pool_at(ctx->pool, cr_seg);

		off = crp->offset;
		sz = crp->sz;
	}

	ctx->carried_create_props = 0;
	ctx->here_create_props = 0;
	ael_err(ctx, off, sz,
			"this terminal cannot create a container — drop the create-order");

	return true;
}

ast_ref
ael_consume_leaf_then_finalize(ael_context* ctx, ast_ref ctx_ref, ast_ref pf,
		ast_ref leaf)
{
	if (ctx_ref == AST_REF_NULL || pf == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	// The path is ending, so a still-parked create-order has no following
	// segment to name a container for. Where the op writes into the container
	// the last segment names (the leafless bulk verbs) it belongs on the op;
	// where the last segment is the op's own key or index, it named a value
	// rather than a container and is a mistake.
	bool drained_to_op = false;

	if (! ael_drain_create_props_to_op(ctx, ctx_ref, pf, leaf, &drained_to_op)) {
		return AST_REF_NULL;
	}

	ast_node* pfp = ast_pool_at(ctx->pool, pf);

	// One gate for every terminal that cannot act on a create-order: reads, and
	// the writes that do not materialise containers.
	if (! pf_creates_containers(pfp->type) &&
			reject_create_order(ctx, ctx_ref, AST_REF_NULL, leaf, drained_to_op)) {
		return AST_REF_NULL;
	}

	uint32_t leaf_flags = leaf != AST_REF_NULL
			? ast_node_table[ast_pool_at(ctx->pool, leaf)->type].flags
			: 0;
	bool leaf_requires_select =
			(leaf_flags & (AST_NF_BY_EXP | AST_NF_AND_EXP)) != 0;
	bool leaf_is_multi = (leaf_flags & AST_NF_PLURAL) != 0;
	bool is_plural = ctx_list_has_seg_flags(ctx, ctx_ref,
			AST_NF_BY_EXP | AST_NF_AND_EXP | AST_NF_PLURAL);
	// getTree() and getKeyValues() always emit SELECT — get_by_X can't
	// produce a tree shape, and getKeyValues() must return the flat [k,v]
	// list (a single map result can't represent duplicate keys from a
	// multi-source select), so even the single-parent multi-tail case goes
	// through SELECT.
	bool always_select = pfp->type == AST_PATH_FUNC_GET_TREE ||
			pfp->type == AST_PATH_FUNC_GET_KEY_VALUES;

	// Wildcard-bearing or AND_EXP-bearing paths route through SELECT (for
	// read) or SELECT_APPLY (for modify). The implicit GET on a bare
	// path and .remove() are wired here. .count() emits SELECT(COUNT) —
	// the runtime counts the
	// matched leaves and returns an int directly, no list-materialise +
	// size-wrap round-trip. The leaf-only multi-select path keeps using
	// RESULT_TYPE_COUNT directly on the BY_*_RANGE op for the
	// O(log N) K_ORDERED case. .set() / .insert() / .increment() are
	// not supported on wildcard paths -- semantically meaningless on
	// multi-select, no plans to add. Use .modify(expr) for per-element
	// updates.
	if (leaf_requires_select || is_plural || always_select) {
		ast_ref new_pf;

		if (pfp->type == AST_PATH_FUNC_REMOVE) {
			bool nofail = (ast_cdt_op_props(pfp) & AST_PROP_NO_FAIL) != 0;
			new_pf = ael_new_pselect_remove(ctx, nofail);
		}
		else if (pfp->type == AST_PATH_FUNC_GET) {
			new_pf = ael_new_select(ctx, AS_CDT_SELECT_LEAF_LIST);
		}
		else if (pfp->type == AST_PATH_FUNC_GET_KEYS) {
			new_pf = ael_new_select(ctx, AS_CDT_SELECT_LEAF_MAP_KEY);
		}
		else if (pfp->type == AST_PATH_FUNC_GET_KEY_VALUES) {
			// Needs a multi-element source: a multi-select leaf — range /
			// list (leaf_is_multi) or wildcard / filter (leaf_requires_select)
			// — or a plural mid-path. A single navigated value has no key
			// collection.
			if (! (leaf_requires_select || leaf_is_multi || is_plural)) {
				ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
						".getKeyValues() requires a multi-select segment — use the bare path for a single navigated value");
				return AST_REF_NULL;
			}

			new_pf = ael_new_select(ctx, AS_CDT_SELECT_LEAF_MAP_KEY_VALUE);
		}
		else if (pfp->type == AST_PATH_FUNC_GET_TREE) {
			new_pf = ael_new_select(ctx, AS_CDT_SELECT_TREE);
		}
		else if (pfp->type == AST_PATH_FUNC_COUNT) {
			new_pf = ael_new_select(ctx, AS_CDT_SELECT_COUNT);
		}
		else if (pfp->type == AST_PATH_FUNC_EXISTS) {
			new_pf = ael_new_select(ctx, AS_CDT_SELECT_EXISTS);
		}
		else if (pfp->type == AST_PATH_FUNC_GET_INDEXES ||
				pfp->type == AST_PATH_FUNC_GET_RANKS) {
			// Positional getters work on single-select and leaf range/list
			// selectors (the BY_* op carries the return-type byte). Wildcard /
			// filter leaves and inner-multi-select route here and need the
			// SELECT_LEAF_INDEX/_RANK runtime shapes — deferred (docs/ael-TODO).
			ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
					"getIndexes() / getRanks() support single-select and leaf range/list selectors only this release — not wildcard/filter or inner-multi-select paths");
			return AST_REF_NULL;
		}
		else if (pfp->type == AST_PATH_FUNC_GET_MAPS) {
			// Like the positional getters: the leaf map range/list fast path
			// carries the RESULT_TYPE_*_MAP byte directly, but wildcard /
			// filter / inner-multi-select need the SELECT_LEAF_MAP_*
			// runtime shapes — deferred (docs/ael-TODO).
			ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
					"getMaps() supports leaf map range/list selectors only this release — not wildcard/filter or inner-multi-select paths");
			return AST_REF_NULL;
		}
		else {
			ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
					"path function does not accept wildcard segments — use modify()");
			return AST_REF_NULL;
		}

		// Re-append the popped leaf — select-style finalize works on
		// the full ctx chain, with no separate leaf seg parameter.
		if (leaf != AST_REF_NULL) {
			ael_ctx_list_append(ctx, ctx_ref, leaf);
		}

		ast_pool_release(ctx->pool, pf);

		return ael_finalize_select_call(ctx, ctx_ref, new_pf);
	}

	if (! ael_cdt_op_from_fn(ctx, ctx_ref, leaf, pf)) {
		return AST_REF_NULL;
	}

	return ael_finalize_path_call_wrap(ctx, ctx_ref, pf, EXP_CALL_CDT);
}

// Source (operand) and result etypes for the casts. toInt / toFloat are
// polymorphic: the operand is INT | FLOAT | STRING (AUTO_ADD) and the concrete
// op is chosen from its resolved type post-inference (ael_dispatch_to_cast).
// toString takes INT | FLOAT | BLOB -> STR.
static void
ael_cast_types(ast_node_t cast_type, ast_etype* src, ast_etype* result)
{
	switch (cast_type) {
	case AST_PATH_FUNC_CAST_INT: // toInt
		*src = AST_ETYPE_AUTO_ADD;
		*result = AST_ETYPE_INT;
		break;
	case AST_PATH_FUNC_CAST_FLOAT: // toFloat
		*src = AST_ETYPE_AUTO_ADD;
		*result = AST_ETYPE_FLOAT;
		break;
	default: // AST_PATH_FUNC_CAST_STRING
		*src = AST_ETYPE_AUTO_REPR;
		*result = AST_ETYPE_STR;
		break;
	}
}

// toInt() / toFloat() / toString() on a path receiver -- wraps the
// implicit GET in the cast unary.
static ast_ref
ael_finalize_cast(ael_context* ctx, ast_ref ctx_ref, ast_ref cast)
{
	cf_assert(ctx_ref != AST_REF_NULL && cast != AST_REF_NULL, AS_EXP,
			"ael_finalize_cast: null operand");

	ast_ref synth_get = ast_new_cdt_op(ctx->pool, AST_PATH_FUNC_GET);
	ast_ref leaf = ael_ctx_list_pop(ctx, ctx_ref);

	if (leaf == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	// Pin the leaf to the cast's required source type so untyped leaves narrow
	// and conflicting `:T` pins surface as AST_ETYPE_ERROR at the post-parse
	// check (toInt / toFloat: INT|FLOAT|STRING operand; toString: INT|FLOAT|BLOB).
	ast_node* wp = ast_pool_at(ctx->pool, cast);
	ast_etype src_type;
	ast_etype result_type;

	ael_cast_types(wp->type, &src_type, &result_type);
	ast_set_implicit_type(ctx, leaf, src_type);

	// Propagate the (now-narrowed) leaf etype onto the synthetic GET,
	// parallel to the implicit-GET operand reduction at ael_parser.y
	// operand ::= ctx_list.
	ast_etype leaf_etype = ast_pool_at(ctx->pool, leaf)->etype;

	if (ast_type_resolved(leaf_etype)) {
		ast_pool_at(ctx->pool, synth_get)->etype = leaf_etype;
	}

	ast_ref pc = ael_consume_leaf_then_finalize(ctx, ctx_ref, synth_get, leaf);

	if (pc == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	wp->etype = result_type;
	wp->u.unary.operand = pc;
	return cast;
}

// BLOB-bit method-style call construction.
//
// The runtime AS_BITS_OP doesn't navigate paths the way CDT ops do, so
// the receiver is always a self-contained value-producing sub-expression.
// `ael_new_bit_fn` builds a transient AST_PATH_FUNC_BIT_* node carrying
// the parsed args (chained via the cdt_op head/tail/count fields, same
// as CDT path-funcs); `ael_finalize_bit_call` pins the receiver to BLOB
// and morphs the node to AST_BIT_OP wrapped in AST_PATH_CALL with
// stype = EXP_CALL_BITS.

// Fold a path into a self-contained value-producing sub-expression
// suitable as a bit-call receiver (the runtime BIT_OP has no CDT-style
// path navigation).
static ast_ref
ael_bit_recv_from_ctx(ael_context* ctx, ast_ref ctx_ref)
{
	cf_assert(ctx_ref != AST_REF_NULL, AS_EXP,
			"ael_bit_recv_from_ctx: null operand");

	// Fold the path to a value-producing AST_PATH_CALL — same shape as
	// the bare `operand ::= ctx_list` rule (implicit get on the tail).
	ast_ref leaf = ael_ctx_list_pop(ctx, ctx_ref);

	if (leaf == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref impl_get = ast_new_cdt_op(ctx->pool, AST_PATH_FUNC_GET);

	return ael_consume_leaf_then_finalize(ctx, ctx_ref, impl_get, leaf);
}

// --- property-flag materialization (parse-time) ---
//
// A `:PROPERTY` postfix on a bit/HLL modify (e.g. bitAdd(...):NO_FAIL) sets
// AST_PROP_* bits on the op node. Rather than have each emitter project those
// bits at emit time — the generic CDT projection is wrong for BIT/HLL ops
// (different flag values, and the AS_BITS_OP_SUBTRACT == AS_CDT_OP_LIST_CLEAR
// opcode collision drops the flags slot) — we translate them here, once, into
// the op's wire-arg chain and clear the props. Both emitters then emit the
// chain verbatim, so they can't diverge on flags.

// pre:  props is a bit op's AST_PROP_* mask.
// post: the AS_BITS_FLAG_* wire word (NO_FAIL / CREATE_ONLY / UPDATE_ONLY /
//       PARTIAL). Pure translation.
static uint64_t
ael_bits_wire_flags(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_BITS_FLAG_NO_FAIL;
	}
	if ((props & AST_PROP_CREATE_ONLY) != 0) {
		w |= AS_BITS_FLAG_CREATE_ONLY;
	}
	if ((props & AST_PROP_UPDATE_ONLY) != 0) {
		w |= AS_BITS_FLAG_UPDATE_ONLY;
	}
	if ((props & AST_PROP_PARTIAL) != 0) {
		w |= AS_BITS_FLAG_PARTIAL;
	}

	return w;
}

// pre:  props is an HLL op's AST_PROP_* mask.
// post: the AS_HLL_FLAG_* wire word (CREATE_ONLY / UPDATE_ONLY / NO_FAIL).
//       Pure translation.
static uint64_t
ael_hll_wire_flags(ast_prop_bits props)
{
	uint64_t w = 0;

	if ((props & AST_PROP_CREATE_ONLY) != 0) {
		w |= AS_HLL_FLAG_CREATE_ONLY;
	}
	if ((props & AST_PROP_UPDATE_ONLY) != 0) {
		w |= AS_HLL_FLAG_UPDATE_ONLY;
	}
	if ((props & AST_PROP_NO_FAIL) != 0) {
		w |= AS_HLL_FLAG_NO_FAIL;
	}

	return w;
}

// Fold a bit op's property flags into its wire chain, then clear props.
// bitAdd/bitSubtract with `signed: true` already carry a (flags=0,
// subflags=SIGNED) pair (chain count 5) — OR the property flags into that
// existing flags slot (the 4th chain element). Any other case appends a
// flags arg after the operands.
// pre:  bit_op is an AST_BIT_OP whose props hold its :PROPERTY postfix flags.
// post: the flags are folded into the wire chain (OR'd into the signed-flags
//       slot when present, else appended) and props is cleared.
static void
ael_bit_materialize_flags(ael_context* ctx, ast_ref bit_op)
{
	ast_node* op = ast_pool_at(ctx->pool, bit_op);
	uint64_t wf = ael_bits_wire_flags(op->u.cdt_op.props);

	op->u.cdt_op.props = 0;

	if (wf == 0) {
		return;
	}

	bool signed_slot = (op->u.cdt_op.op_code == AS_BITS_OP_ADD ||
							   op->u.cdt_op.op_code == AS_BITS_OP_SUBTRACT) &&
			op->u.cdt_op.count == 5;

	if (signed_slot) {
		ast_ref e = op->u.cdt_op.head;

		for (uint32_t i = 0; i < 3; i++) {
			e = ast_pool_at(ctx->pool, e)->next;
		}

		ast_pool_at(ctx->pool, e)->u.ival |= (int64_t)wf;
	}
	else {
		AST_CHAIN_PUSH_TAIL(ctx->pool, bit_op,
				ast_new_int(ctx->pool, (int64_t)wf));
	}
}

// Fold an HLL op's property flags into its wire chain, then clear props.
// HLL has no signed slot; a non-zero flag byte appends one arg.
// pre:  hll_op is an AST_HLL_OP whose props hold its :PROPERTY postfix flags.
// post: a non-zero flag word is appended as one arg and props is cleared.
static void
ael_hll_materialize_flags(ael_context* ctx, ast_ref hll_op)
{
	ast_node* op = ast_pool_at(ctx->pool, hll_op);
	uint64_t wf = ael_hll_wire_flags(op->u.cdt_op.props);

	op->u.cdt_op.props = 0;

	if (wf != 0) {
		AST_CHAIN_PUSH_TAIL(ctx->pool, hll_op,
				ast_new_int(ctx->pool, (int64_t)wf));
	}
}

// Wrap a resolved op node + its receiver into the uniform AST_PATH_CALL.
// `recv` fills u.call.ctx — an AST_PATH_CTX list for path calls, or the
// bare receiver value-expression for method-style calls. stype already
// carries any MODIFY_LOCAL flag. `deferable_from` is the node whose
// has_deferable the call inherits (the bin for path calls, the receiver
// for value calls).
static ast_ref
ael_wrap_path_call(ael_context* ctx, exp_call_stype stype, ast_ref recv,
		ast_ref op, ast_etype etype, ast_ref deferable_from)
{
	ast_ref pc_ref = ast_new(ctx->pool, AST_PATH_CALL);
	ast_node* pcp = ast_pool_at(ctx->pool, pc_ref);

	pcp->u.call.stype = stype;
	pcp->u.call.ctx = recv;
	pcp->u.call.call_op = op;
	pcp->etype = etype;
	pcp->has_deferable = ast_pool_at(ctx->pool, deferable_from)->has_deferable;

	ael_span_call(ctx, pc_ref, recv);

	return pc_ref;
}

// One method-call family (BIT / HLL / STR): the finalizers differ only in
// these values, so a shared finalizer keeps the morph + MODIFY_LOCAL +
// has_deferable handling from drifting between them.
typedef struct {
	ast_etype recv_etype; // receiver pin
	ast_node_t op_type; // resolved node type (AST_BIT_OP / ...)
	exp_call_stype stype; // EXP_CALL_BITS / _HLL / _STRING
	// Fold :PROPERTY flags into the wire chain post-morph; NULL when the
	// family has none (STR).
	void (*materialize)(ael_context* ctx, ast_ref fn);
	// Family result-type rule; fp is the pre-morph pf node.
	ast_etype (*result_etype)(const ast_node* fp, ast_node_t pf_type,
			bool is_modify);
} ael_method_family;

// Attach the receiver to a transient AST_PATH_FUNC_* method node: pin the
// receiver, morph to the family's resolved op type (op_code from the node
// table's exp_cmd), fold flags, and wrap. Modify ops always carry
// MODIFY_LOCAL — the runtime decides writeback vs value-flow from the
// enclosing expression context. The u.call.ctx slot holds the receiver
// value-expression directly (no AST_PATH_CTX list — these families have
// no path-navigation mechanism).
static ast_ref
ael_finalize_method_family(ael_context* ctx, ast_ref recv, ast_ref fn,
		const ael_method_family* fam)
{
	cf_assert(recv != AST_REF_NULL && fn != AST_REF_NULL, AS_EXP,
			"ael_finalize_method_family: null operand");

	// The bare-receiver route for all three families.
	if (reject_create_order(ctx, AST_REF_NULL, recv, AST_REF_NULL, false)) {
		return AST_REF_NULL;
	}

	// Pin the receiver; a conflicting explicit pin fails through the
	// intersection-conflict diagnostic in ast_set_implicit_type.
	ast_set_implicit_type(ctx, recv, fam->recv_etype);

	ast_node* fp = ast_pool_at(ctx->pool, fn);
	ast_node_t pf_type = fp->type;
	bool is_modify = (ast_node_table[pf_type].flags & AST_NF_MODIFY) != 0;
	ast_etype result_etype = fam->result_etype(fp, pf_type, is_modify);

	fp->type = fam->op_type;
	fp->u.cdt_op.op_code = (uint16_t)ast_node_table[pf_type].exp_cmd;
	fp->u.cdt_op.is_modify = is_modify;
	fp->etype = result_etype;

	if (fam->materialize != NULL) {
		fam->materialize(ctx, fn);
	}

	exp_call_stype stype = fam->stype;

	if (is_modify) {
		stype |= EXP_CALL_FLAG_MODIFY_LOCAL;
	}

	return ael_wrap_path_call(ctx, stype, recv, fn, result_etype, recv);
}

// Result etype: GET returns BLOB and b64Encode STRING; all other reads return
// INT; all modifies return BLOB (the modified value).
static ast_etype
bit_result_etype(const ast_node* fp, ast_node_t pf_type, bool is_modify)
{
	(void)fp;

	if (pf_type == AST_PATH_FUNC_BIT_B64_ENCODE) {
		return AST_ETYPE_STR;
	}

	bool blob = pf_type == AST_PATH_FUNC_BIT_GET || is_modify;

	return blob ? AST_ETYPE_BLOB : AST_ETYPE_INT;
}

// STR builders set the result type on the pf node up front.
static ast_etype
str_result_etype(const ast_node* fp, ast_node_t pf_type, bool is_modify)
{
	(void)pf_type;
	(void)is_modify;
	return fp->etype;
}

static ast_etype
hll_result_etype_cb(const ast_node* fp, ast_node_t pf_type, bool is_modify)
{
	(void)fp;
	(void)is_modify;
	return hll_result_etype(pf_type);
}

static ast_ref
ael_finalize_bit_call(ael_context* ctx, ast_ref recv, ast_ref bit_fn)
{
	static const ael_method_family fam = {
		.recv_etype = AST_ETYPE_BLOB,
		.op_type = AST_BIT_OP,
		.stype = EXP_CALL_BITS,
		.materialize = ael_bit_materialize_flags,
		.result_etype = bit_result_etype,
	};

	return ael_finalize_method_family(ctx, recv, bit_fn, &fam);
}

// String method call on a bare-bin or value receiver. Mirrors
// ael_finalize_bit_call: morph the transient AST_PATH_FUNC_STR_* node to
// AST_STR_OP (u.cdt_op.op_code = AS_STRING_OP_*) and wrap in AST_PATH_CALL with
// stype EXP_CALL_STRING (| MODIFY_LOCAL for modify ops).
// pre:  str_fn->etype is the result type (set by the builder).
// post: recv is pinned STRING and sits in u.call.ctx directly (not an
//       AST_PATH_CTX list), so the emitters take the no-ctx value shape -- ctx
//       navigation isn't wired.
static ast_ref
ael_finalize_str_call(ael_context* ctx, ast_ref recv, ast_ref str_fn)
{
	ast_node* sfp = ast_pool_at(ctx->pool, str_fn);

	// The mask is checked at postfix time, before the receiver shape is known,
	// so a bare receiver gets this far. No string op has a flags word, so
	// unlike the CDT families the prop alone decides.
	if ((ast_node_table[sfp->type].valid_props & AST_PROP_NO_FAIL) != 0 &&
			(sfp->u.cdt_op.props & AST_PROP_NO_FAIL) != 0) {
		ael_err_inert_no_fail(ctx, sfp);
		return AST_REF_NULL;
	}

	static const ael_method_family fam = {
		.recv_etype = AST_ETYPE_STR,
		.op_type = AST_STR_OP,
		.stype = EXP_CALL_STRING,
		.result_etype = str_result_etype,
	};

	return ael_finalize_method_family(ctx, recv, str_fn, &fam);
}

// String method call on a pathed receiver. The string family has its own
// CONTEXT_EVAL sentinel (AS_STRING_OP_CONTEXT_EVAL, the same 0xFF as the CDT
// one), so the op carries the context natively and the emitters reuse the CDT
// composition: ael_pack_ctx supplies [0xFF, ctx, _] and ael_emit_cdt_op_blob
// fills the third slot with the inner [op, args...] list.
// pre:  ctx_ref is an AST_PATH_CTX list; str_fn->etype is the result type.
// post: the bin is pinned to its container type by the path wrapper, which
//       also retypes a modify to that container -- the leaf write-back mutates
//       the bin, not the string.
static ast_ref
ael_finalize_str_path(ael_context* ctx, ast_ref ctx_ref, ast_ref str_fn)
{
	cf_assert(ctx_ref != AST_REF_NULL && str_fn != AST_REF_NULL, AS_EXP,
			"ael_finalize_str_path: null operand");

	// A bare bin routes to ael_finalize_str_call, so a segless ctx never carries
	// a string op -- there would be nothing for the leaf apply to descend into.
	cf_assert(ast_pool_at(ctx->pool, ctx_ref)->u.ctx_list.count > 1, AS_EXP,
			"ael_finalize_str_path: ctx has no segment");

	if (reject_create_order(ctx, ctx_ref, AST_REF_NULL, AST_REF_NULL, false)) {
		return AST_REF_NULL;
	}

	ast_node* fp = ast_pool_at(ctx->pool, str_fn);
	ast_node_t pf_type = fp->type;

	fp->type = AST_STR_OP;
	fp->u.cdt_op.op_code = (uint16_t)ast_node_table[pf_type].exp_cmd;
	fp->u.cdt_op.is_modify = (ast_node_table[pf_type].flags & AST_NF_MODIFY) != 0;

	return ael_finalize_path_call_wrap(ctx, ctx_ref, str_fn, EXP_CALL_STRING);
}

// Parse regex flag chars to AS_STRING_REGEX_* wire bits.
// pre:  flag_* is a source span into ctx->input (the run after the closing /).
// post: i/m/s -> *out (true); x/w are lexer-admitted but have no wire bit, so
//       they diagnose and return false.
static bool
ael_regex_parse_flags(ael_context* ctx, uint32_t flag_off, uint32_t flag_sz,
		uint64_t* out)
{
	uint64_t flags = 0;

	for (uint32_t i = 0; i < flag_sz; i++) {
		switch (ctx->input[flag_off + i]) {
		case 'i':
			flags |= AS_STRING_REGEX_CASE_INSENSITIVE;
			break;
		case 'm':
			flags |= AS_STRING_REGEX_MULTILINE;
			break;
		case 's':
			flags |= AS_STRING_REGEX_DOTALL;
			break;
		default: // 'x' / 'w': admitted by the lexer, but no wire bit exists
			ael_err(ctx, flag_off + i, 1,
					"regex flag not supported (only i, m, s)");
			return false;
		}
	}

	*out = flags;
	return true;
}

// `expr =~ /pattern/flags` -> a REGEX_COMPARE string call on lhs -> TRILEAN.
// Pattern bytes are stored raw -- ICU/PCRE2 interpret the escapes, not the AEL
// string decoder. Wire args: [pattern (buf), flags].
// pre:  pat_* / flag_* are source spans into ctx->input.
// post: flags i/m/s -> wire; unsupported x/w -> diagnostic + AST_REF_NULL.
ast_ref
ael_new_regex_match(ael_context* ctx, ast_ref lhs, uint32_t pat_off,
		uint32_t pat_sz, uint32_t flag_off, uint32_t flag_sz)
{
	if (lhs == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	uint64_t flags;

	if (! ael_regex_parse_flags(ctx, flag_off, flag_sz, &flags)) {
		return AST_REF_NULL;
	}

	ast_ref pat = ast_new_string(ctx->pool, ctx->input + pat_off, pat_sz, false);
	ast_ref fl = ast_new_int(ctx->pool, (int64_t)flags);
	ast_ref op = ast_new_cdt_op(ctx->pool, AST_PATH_FUNC_STR_REGEX_COMPARE);

	AST_CHAIN_PUSH_TAIL(ctx->pool, op, pat);
	AST_CHAIN_PUSH_TAIL(ctx->pool, op, fl);
	ast_pool_at(ctx->pool, op)->etype = AST_ETYPE_TRILEAN;

	return ael_finalize_str_call(ctx, lhs, op);
}

// Regex literal used as a `pattern:` named argument (regexReplace). Builds a
// transient AST_REGEX_LIT carrying the raw pattern string and the flags int in
// u.binary.left / .right; ael_build_regex_replace consumes it. Kept separate
// from ael_new_regex_match because a `pattern:` operand feeds a modify op, not
// the `=~` compare.
// pre:  pat_* / flag_* are source spans into ctx->input.
// post: flags i/m/s -> wire; unsupported x/w -> diagnostic + AST_REF_NULL.
ast_ref
ael_new_regex_operand(ael_context* ctx, uint32_t pat_off, uint32_t pat_sz,
		uint32_t flag_off, uint32_t flag_sz)
{
	uint64_t flags;

	if (! ael_regex_parse_flags(ctx, flag_off, flag_sz, &flags)) {
		return AST_REF_NULL;
	}

	// regexReplace replaces every match (the spec's `/\d+/ -> ''` removes
	// all digits); there is no first-only regex form. GLOBAL is unused by
	// `=~` (REGEX_COMPARE), so set here, not in the shared flag parser.
	flags |= AS_STRING_REGEX_GLOBAL;

	ast_ref pat = ast_new_string(ctx->pool, ctx->input + pat_off, pat_sz, false);
	ast_ref fl = ast_new_int(ctx->pool, (int64_t)flags);
	ast_ref r = ast_new(ctx->pool, AST_REGEX_LIT);
	ast_node* np = ast_pool_at(ctx->pool, r);

	np->u.binary.left = pat;
	np->u.binary.right = fl;
	np->etype = AST_ETYPE_STR;

	return r;
}

//==========================================================
// HLL method-style call construction. Mirrors the bit-call helpers
// above: the receiver is a self-contained value-producing sub-expression
// (HLL bin or value-producing path), and the call's AST_PATH_CALL is
// emitted as `[CALL, rtype, EXP_CALL_HLL, [op_code, args..., flags?],
// receiver]` — exactly the wire shape particle_hll.c expects.

// Set-op builder for hllUnion / UnionCount / IntersectCount / Similarity
// / MayContain. For the four set-of-HLLs ops (UNION family), the operand
// is one HLL or a list of HLLs — pin to HLL; the runtime's
// hll_parse_hlls accepts either a list or a single HLL value. For
// MAY_CONTAIN, the operand is a list of arbitrary values — pin to LIST.
static ast_ref
ael_new_hll_set_fn(ael_context* ctx, ast_node_t pf_type, ast_ref arg)
{
	cf_assert(arg != AST_REF_NULL, AS_EXP, "ael_new_hll_set_fn: null operand");

	ast_node* ap = ast_pool_at(ctx->pool, arg);
	ast_etype elem_etype = (pf_type == AST_PATH_FUNC_HLL_MAY_CONTAIN)
			? AST_ETYPE_AUTO
			: AST_ETYPE_HLL;

	if (ap->type == AST_LIST) {
		// Explicit list literal — pin wrapper LIST, then each element
		// (manually, since ast_set_implicit_type would re-propagate
		// LIST to children via the NK_LIST branch).
		ast_set_implicit_type(ctx, arg, AST_ETYPE_LIST);

		if (elem_etype != AST_ETYPE_AUTO) {
			for (ast_ref e = ap->u.list.head; e != AST_REF_NULL;
					e = ast_pool_at(ctx->pool, e)->next) {
				ast_set_implicit_type(ctx, e, elem_etype);
			}
		}
	}
	else if (elem_etype != AST_ETYPE_AUTO) {
		// Single HLL operand — pin to HLL directly, no auto-wrap.
		ast_set_implicit_type(ctx, arg, elem_etype);
	}
	else {
		// MAY_CONTAIN single arg — keep LIST pin.
		ast_set_implicit_type(ctx, arg, AST_ETYPE_LIST);
	}

	ast_ref r = ast_new_cdt_op(ctx->pool, pf_type);

	AST_CHAIN_PUSH_TAIL(ctx->pool, r, arg);
	return r;
}

// Final etype for an HLL op result. Drives the wrapping AST_PATH_CALL's
// etype and the wire rtype byte (via ast_etype_to_rtype).
// pre:  pf_type is an HLL path-func verb.
// post: the result etype -- INT for the count / mayContain reads, FLOAT for
//       similarity, LIST for describe, HLL for the set-ops / init / add.
static ast_etype
hll_result_etype(ast_node_t pf_type)
{
	switch (pf_type) {
	case AST_PATH_FUNC_HLL_COUNT:
	case AST_PATH_FUNC_HLL_UNION_COUNT:
	case AST_PATH_FUNC_HLL_INTERSECT_COUNT:
	case AST_PATH_FUNC_HLL_MAY_CONTAIN:
		return AST_ETYPE_INT;
	case AST_PATH_FUNC_HLL_SIMILARITY:
		return AST_ETYPE_FLOAT;
	case AST_PATH_FUNC_HLL_DESCRIBE:
		return AST_ETYPE_LIST;
	case AST_PATH_FUNC_HLL_UNION:
	case AST_PATH_FUNC_HLL_INIT:
	case AST_PATH_FUNC_HLL_ADD:
	default:
		return AST_ETYPE_HLL;
	}
}

// Attach the receiver to a transient AST_PATH_FUNC_HLL_* node: pin the
// receiver HLL, morph to AST_HLL_OP and wrap in an AST_PATH_CALL with
// stype = EXP_CALL_HLL.
static ast_ref
ael_finalize_hll_call(ael_context* ctx, ast_ref recv, ast_ref hll_fn)
{
	static const ael_method_family fam = {
		.recv_etype = AST_ETYPE_HLL,
		.op_type = AST_HLL_OP,
		.stype = EXP_CALL_HLL,
		.materialize = ael_hll_materialize_flags,
		.result_etype = hll_result_etype_cb,
	};

	return ael_finalize_method_family(ctx, recv, hll_fn, &fam);
}

//==========================================================
// Table-driven function-call resolution.
//
// The grammar collects a generic argument list (positional `expr` and named
// `name: expr` entries) and hands it, with the function-name token, to a
// resolver. The resolver looks the name up in ael_func_table, binds the args
// to the spec's ordered slots, applies per-slot type inference, and dispatches
// to the existing family builders (ast_new_func*, ael_new_bit_*, ael_new_hll_*).
// Function and parameter names are ordinary identifiers — not lexer keywords —
// so a bin named `value` / `bitGet` parses fine and new functions are table
// edits.
//

// --- argument-list construction (grammar actions) ---

ast_ref
ael_new_named_arg(ael_context* ctx, uint32_t name_offset, uint32_t name_sz,
		ast_ref value)
{
	ast_ref r = ast_new(ctx->pool, AST_ARG);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.arg.name_offset = name_offset;
	n->u.arg.name_sz = name_sz;
	n->u.arg.value = value;

	return r;
}

ast_ref
ael_new_positional_arg(ael_context* ctx, ast_ref value)
{
	// name_sz == 0 marks a positional argument.
	return ael_new_named_arg(ctx, 0, 0, value);
}

ast_ref
ael_new_arg_list(ael_context* ctx, ast_ref arg)
{
	ast_ref r = ast_new(ctx->pool, AST_ARG_LIST);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.list.head = arg;
	n->u.list.tail = arg;
	n->u.list.count = (arg == AST_REF_NULL) ? 0 : 1;

	return r;
}

ast_ref
ael_new_empty_arg_list(ael_context* ctx)
{
	return ael_new_arg_list(ctx, AST_REF_NULL);
}

ast_ref
ael_append_arg(ael_context* ctx, ast_ref list, ast_ref arg)
{
	if (list == AST_REF_NULL || arg == AST_REF_NULL) {
		return list;
	}

	ast_node* ln = ast_pool_at(ctx->pool, list);

	if (ln->u.list.tail == AST_REF_NULL) {
		ln->u.list.head = arg;
		ln->u.list.tail = arg;
		ln->u.list.count = 1;
	}
	else {
		ast_pool_at(ctx->pool, ln->u.list.tail)->next = arg;
		ln->u.list.tail = arg;
		ln->u.list.count++;
	}

	return list;
}

// --- argument binding + family builders ---

// Extract a 0/1 from a boolean-literal arg (for bit `signed:`). The original
// grammar accepted only literal true/false (bool_lit_int) here.
// pre:  ref is a bound arg operand (the bit `signed:` slot).
// post: *out = 0/1 and true for a bool literal; else false + a diagnostic.
static bool
ael_arg_bool(ael_context* ctx, ast_ref ref, int* out)
{
	ast_node* n = ast_pool_at(ctx->pool, ref);

	if (n->type != AST_BOOL) {
		ael_err(ctx, ast_disp_offset(n), n->sz,
				"signed must be a boolean literal (true or false)");
		return false;
	}

	*out = n->u.bval ? 1 : 0;

	return true;
}

// Bind a fixed-arity spec's arguments to ordered slots. Positional args fill
// the leading NONE-named slots; named args fill the matching named slot.
// Positionals must precede named (Python style). On success slots[i] holds
// the bound expr (AST_REF_NULL for an unfilled optional slot). Adds a
// diagnostic and returns false on any arity / naming / ordering error.
// pre:  spec is a fixed-arity table row; arg_list is the parsed arg list;
//       slots has room for AEL_MAX_PARAMS.
// post: slots[0..param_count) bound (AST_REF_NULL for an unfilled optional),
//       returns true; else false + a diagnostic on an arity / naming /
//       ordering error.
static bool
ael_bind_args(ael_context* ctx, const ael_func_spec_t* spec, uint32_t fname_off,
		uint32_t fname_sz, ast_ref arg_list, ast_ref* slots)
{
	// slots[] is AEL_MAX_PARAMS deep and every loop below indexes by
	// param_count — a table row wider than that would overrun it. The table
	// is static so this is a programmer error, never wire input.
	cf_assert(spec->param_count <= AEL_MAX_PARAMS, AS_EXP,
			"func spec '%s' param_count %u exceeds AEL_MAX_PARAMS", spec->name,
			spec->param_count);

	for (uint32_t i = 0; i < AEL_MAX_PARAMS; i++) {
		slots[i] = AST_REF_NULL;
	}

	// Leading positional (unnamed) slots.
	uint32_t pos_count = 0;

	while (pos_count < spec->param_count &&
			spec->params[pos_count].name == AEL_PNAME_NONE) {
		pos_count++;
	}

	ast_node* ln = ast_pool_at(ctx->pool, arg_list);
	uint32_t pos_filled = 0;
	bool named_started = false;

	for (ast_ref a = ln->u.list.head; a != AST_REF_NULL;) {
		ast_node* an = ast_pool_at(ctx->pool, a);
		ast_ref next = an->next;

		if (an->u.arg.name_sz != 0) {
			named_started = true;

			ael_pname_t pn = ael_pname_match(ctx->input + an->u.arg.name_offset,
					an->u.arg.name_sz);
			int32_t idx = -1;

			if (pn != AEL_PNAME_NONE) {
				for (uint32_t i = pos_count; i < spec->param_count; i++) {
					if (spec->params[i].name == pn) {
						idx = (int32_t)i;
						break;
					}
				}
			}

			if (idx < 0) {
				ael_err(ctx, an->u.arg.name_offset, an->u.arg.name_sz,
						"unknown parameter name for this function");
				return false;
			}

			if (slots[idx] != AST_REF_NULL) {
				ael_err(ctx, an->u.arg.name_offset, an->u.arg.name_sz,
						"duplicate parameter");
				return false;
			}

			slots[idx] = an->u.arg.value;
		}
		else {
			if (named_started) {
				// value may be null (a failed sub-expr); use the wrapper.
				const ast_node* vp = ast_pool_at(ctx->pool, an->u.arg.value);
				const ast_node* anchor = vp != NULL ? vp : an;

				ael_err(ctx, ast_disp_offset(anchor), anchor->sz,
						"positional argument after named argument");
				return false;
			}

			if (pos_filled >= pos_count) {
				const char* msg;

				if (pos_count != 0) {
					msg = "too many positional arguments";
				}
				else if (spec->param_count == 0 && ! spec->variadic) {
					msg = "this function takes no arguments";
				}
				else {
					msg = "this function takes named arguments";
				}

				ael_err(ctx, fname_off, fname_sz, msg);
				return false;
			}

			slots[pos_filled++] = an->u.arg.value;
		}

		a = next;
	}

	// First required_count slots must be filled.
	for (uint32_t i = 0; i < spec->required_count; i++) {
		if (slots[i] == AST_REF_NULL) {
			ael_pname_t pn = spec->params[i].name;

			if (pn != AEL_PNAME_NONE) {
				ael_errf(ctx, fname_off, fname_sz,
						"missing required argument '%s'", ael_pname_str(pn));
			}
			else {
				ael_errf(ctx, fname_off, fname_sz,
						"missing required argument %u of %u", i + 1,
						spec->required_count);
			}

			return false;
		}
	}

	return true;
}

// Dispatch bound slots to the scalar-math node builder (func1 / func2).
// pre:  slots is the ael_bind_args output; spec is a SCALAR family row.
// post: an AST func node (func1 for arity 1, func2 for arity 2) with the
//       spec's param / result etypes.
static ast_ref
ael_build_scalar(ael_context* ctx, const ael_func_spec_t* spec, ast_ref* slots)
{
	if (spec->param_count == 1) {
		return ast_new_func1(ctx, spec->ast_type, slots[0],
				spec->params[0].etype);
	}

	// param_count == 2 (log / pow / findBitLeft / findBitRight).
	return ast_new_func2(ctx, spec->ast_type, slots[0], slots[1],
			spec->params[0].etype, spec->params[1].etype, spec->result_etype);
}

// max / min / exclusive: >= required_count positional args of
// params[0].etype. max / min fold left into nested n-ary merges; exclusive
// ("exactly one true") is not foldable, so it keeps the wire's flat n-ary
// shape.
// pre:  arg_list is the parsed arg list; spec requires >= 2 positional args.
// post: the built node (all operands pinned params[0].etype); AST_REF_NULL +
//       a diagnostic on a named arg or too few args.
static ast_ref
ael_build_variadic(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t fname_off, uint32_t fname_sz, ast_ref arg_list)
{
	// The seed below merges the first two args, so a variadic spec must
	// require at least two — enforced by the table (programmer error).
	cf_assert(spec->required_count >= 2, AS_EXP,
			"variadic func spec '%s' must require >= 2 args", spec->name);

	ast_node* ln = ast_pool_at(ctx->pool, arg_list);
	ast_etype et = spec->params[0].etype;
	uint32_t n = 0;

	for (ast_ref a = ln->u.list.head; a != AST_REF_NULL;
			a = ast_pool_at(ctx->pool, a)->next) {
		ast_node* an = ast_pool_at(ctx->pool, a);

		if (an->u.arg.name_sz != 0) {
			ael_err(ctx, an->u.arg.name_offset, an->u.arg.name_sz,
					"this function takes positional arguments");
			return AST_REF_NULL;
		}

		n++;
	}

	if (n < spec->required_count) {
		ael_errf(ctx, fname_off, fname_sz,
				"too few arguments: expected at least %u, got %u",
				spec->required_count, n);
		return AST_REF_NULL;
	}

	if (spec->ast_type == AST_EXCLUSIVE) {
		ast_ref head = ln->u.list.head;
		ast_ref r = ast_new(ctx->pool, AST_EXCLUSIVE);

		for (ast_ref a = head; a != AST_REF_NULL;
				a = ast_pool_at(ctx->pool, a)->next) {
			ast_ref v = ast_pool_at(ctx->pool, a)->u.arg.value;

			if (v == AST_REF_NULL) {
				continue; // failed operand sub-parse — already diagnosed
			}

			ast_set_implicit_type(ctx, v, et);
			AST_LIST_PUSH_TAIL(ctx->pool, r, v);
		}

		ast_pool_at(ctx->pool, r)->etype = AST_ETYPE_TRILEAN;
		ast_set_span(ctx->pool, r, fname_off, fname_sz);

		return r;
	}

	ast_ref a0 = ln->u.list.head;
	ast_ref a1 = ast_pool_at(ctx->pool, a0)->next;
	ast_ref acc = ast_nary_merge(ctx, spec->ast_type,
			ast_pool_at(ctx->pool, a0)->u.arg.value,
			ast_pool_at(ctx->pool, a1)->u.arg.value, et);

	for (ast_ref a = ast_pool_at(ctx->pool, a1)->next; a != AST_REF_NULL;
			a = ast_pool_at(ctx->pool, a)->next) {
		acc = ast_nary_merge(ctx, spec->ast_type, acc,
				ast_pool_at(ctx->pool, a)->u.arg.value, et);
	}

	return acc;
}

// Dispatch bound slots to the bit builders. Per-op arg inference lives inside
// ael_new_bit_fn / ael_new_bit_arith.
// pre:  slots is the ael_bind_args output for a BIT family row.
// post: the AST_PATH_FUNC_BIT_* transient (via ael_new_bit_*); AST_REF_NULL +
//       a diagnostic on a bad `signed:` literal.
// BIT calls build generically from the table's slot etypes; only the two
// bool-flag shapes need code. The result etype stays AUTO here — the
// finalizer derives it (it depends on is_modify).
static ast_ref
ael_build_bit(ael_context* ctx, const ael_func_spec_t* spec, uint32_t name_off,
		uint32_t name_sz, ast_ref* slots)
{
	bool arith_signed = false;

	switch (spec->ast_type) {
	case AST_PATH_FUNC_BIT_GET_INT:
		// `signed:` rides the wire as an int 0 / 1 — convert the slot in
		// place; the generic build then pushes it like any other slot.
		if (slots[2] != AST_REF_NULL) {
			int sv;

			if (! ael_arg_bool(ctx, slots[2], &sv)) {
				return AST_REF_NULL;
			}

			slots[2] = ast_new_int(ctx->pool, sv);
		}
		break;

	case AST_PATH_FUNC_BIT_ADD:
	case AST_PATH_FUNC_BIT_SUBTRACT:
		// signed: true emits the 5-arg wire form [offset, size, value,
		// flags=0, subflags=SIGNED]; false / absent emits 3 args.
		if (slots[3] != AST_REF_NULL) {
			int sv;

			if (! ael_arg_bool(ctx, slots[3], &sv)) {
				return AST_REF_NULL;
			}

			arith_signed = sv != 0;
			slots[3] = AST_REF_NULL; // not a wire slot itself
		}
		break;

	default:
		break;
	}

	ast_ref r = ael_build_from_spec(ctx, spec, name_off, name_sz, slots);

	if (arith_signed) {
		AST_CHAIN_PUSH_TAIL(ctx->pool, r, ast_new_int(ctx->pool, 0));
		AST_CHAIN_PUSH_TAIL(ctx->pool, r,
				ast_new_int(ctx->pool, AS_BITS_INT_SUBFLAG_SIGNED));
	}

	return r;
}

// Dispatch bound slots to the HLL builders.
// pre:  slots is the ael_bind_args output for an HLL family row.
// post: the AST_PATH_FUNC_HLL_* transient (via ael_new_hll_*); AST_REF_NULL +
//       a diagnostic on minHashBits without indexBits.
// HLL reads / init / add build generically from the table; the set-ops
// (union / mayContain / ...) keep ael_new_hll_set_fn for the per-element
// list pin a wrapper-LIST pin can't express.
static ast_ref
ael_build_hll(ael_context* ctx, const ael_func_spec_t* spec, uint32_t name_off,
		uint32_t name_sz, ast_ref* slots)
{
	switch (spec->ast_type) {
	case AST_PATH_FUNC_HLL_MAY_CONTAIN:
	case AST_PATH_FUNC_HLL_UNION:
	case AST_PATH_FUNC_HLL_UNION_COUNT:
	case AST_PATH_FUNC_HLL_INTERSECT_COUNT:
	case AST_PATH_FUNC_HLL_SIMILARITY: {
		ast_ref r = ael_new_hll_set_fn(ctx, spec->ast_type, slots[0]);

		ast_set_span(ctx->pool, r, name_off, name_sz);
		return r;
	}

	case AST_PATH_FUNC_HLL_ADD:
		// minHashBits has no standalone wire slot — the op chains its
		// optional bit-counts positionally, so minHashBits is meaningful
		// only alongside indexBits (matches the original grammar, which had
		// no `hllAdd(list, minHashBits:)` rule). Without this guard a lone
		// minHashBits would silently encode into the indexBits slot.
		if (slots[2] != AST_REF_NULL && slots[1] == AST_REF_NULL) {
			ast_node* mh = ast_pool_at(ctx->pool, slots[2]);

			ael_err(ctx, ast_disp_offset(mh), mh->sz,
					"minHashBits requires indexBits");
			return AST_REF_NULL;
		}
		break;

	default:
		break;
	}

	return ael_build_from_spec(ctx, spec, name_off, name_sz, slots);
}

// --- resolvers (grammar entry points) ---

// Geo builtins (geoJson / geoCompare) — top-level calls with bespoke node
// builders. method_open never runs here (these are func_call, not method_fn), so
// the lookup is done once in ael_resolve_func_call and passed in.
// pre:  spec is the geoJson / geoCompare row; arg_list is the parsed args.
// post: the geo node -- geoCompare(a,b) -> TRILEAN, geoJson('..') -> a
//       compile-time GEOJSON const; AST_REF_NULL + a diagnostic on a bind
//       error or a non-literal geoJson arg.
static ast_ref
ael_resolve_geo_fn(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref arg_list)
{
	ast_ref slots[AEL_MAX_PARAMS];

	if (! ael_bind_args(ctx, spec, name_off, name_sz, arg_list, slots)) {
		return AST_REF_NULL;
	}

	// geoCompare(a, b) — pins both operands to GEOJSON, result TRILEAN.
	if (spec->ast_type == AST_CMP_GEO) {
		return ael_new_geo_compare(ctx, slots[0], slots[1]);
	}

	// geoJson('json') — the arg must be a string literal (the GEO literal is a
	// compile-time constant). ast_new_string stores u.str.str = input + offset,
	// so recover the source offset for a byte-identical ael_new_geo_literal.
	ast_node* a = ast_pool_at(ctx->pool, slots[0]);

	if (a->type != AST_STRING) {
		ael_err(ctx, name_off, name_sz, "geoJson requires a string literal");
		return AST_REF_NULL;
	}

	uint32_t off = (uint32_t)(a->u.str.str - ctx->input);

	return ael_new_geo_literal(ctx, off, a->u.str.sz);
}

// Fold one operand onto a string accumulator: acc := append(acc, arg), an
// AS_STRING_OP_APPEND call (arg pinned STR). The building block of the string
// `+` lowering (ael_lower_str_add): `a + b + c` becomes append(append(a, b), c).
// pre:  acc / arg are AST refs; a failed sub-expr is null -> bail.
// post: the append AST_PATH_CALL, or AST_REF_NULL to propagate a failure.
static ast_ref
ael_concat_append(ael_context* ctx, ast_ref acc, ast_ref arg)
{
	if (acc == AST_REF_NULL || arg == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_ref op = ast_new_cdt_op(ctx->pool, AST_PATH_FUNC_STR_APPEND);

	ast_set_implicit_type(ctx, arg, AST_ETYPE_STR);
	AST_CHAIN_PUSH_TAIL(ctx->pool, op, arg);
	ast_pool_at(ctx->pool, op)->etype = AST_ETYPE_STR;

	return ael_finalize_str_call(ctx, acc, op);
}

// Emit the "unknown function" diagnostic for an unresolved name.
// pre:  name_off / name_sz span an unresolved function name in ctx->input.
// post: emits "unknown function '<name>'", plus a "did you mean '<near>'?"
//       hint when ael_func_suggest finds a close match.
static void
ael_diag_unknown_func(ael_context* ctx, uint32_t name_off, uint32_t name_sz)
{
	const char* suggest = ael_func_suggest(ctx->input + name_off, name_sz);

	if (suggest != NULL) {
		ael_errf(ctx, name_off, name_sz,
				"unknown function '%.*s' -- did you mean '%s'?", (int)name_sz,
				ctx->input + name_off, suggest);
	}
	else {
		ael_errf(ctx, name_off, name_sz, "unknown function '%.*s'",
				(int)name_sz, ctx->input + name_off);
	}
}

ast_ref
ael_resolve_func_call(ael_context* ctx, uint32_t name_off, uint32_t name_sz,
		ast_ref arg_list)
{
	const ael_func_spec_t* spec = ael_func_lookup(ctx->input + name_off, name_sz);

	if (spec == NULL) {
		ael_diag_unknown_func(ctx, name_off, name_sz);
		return AST_REF_NULL;
	}

	if (spec->family == AEL_FAM_GEO) {
		return ael_resolve_geo_fn(ctx, spec, name_off, name_sz, arg_list);
	}

	if (spec->family != AEL_FAM_SCALAR) {
		ael_err(ctx, name_off, name_sz,
				"this function requires a receiver (e.g. $.bin.fn(...))");
		return AST_REF_NULL;
	}

	if (spec->variadic) {
		return ael_build_variadic(ctx, spec, name_off, name_sz, arg_list);
	}

	ast_ref slots[AEL_MAX_PARAMS];

	if (! ael_bind_args(ctx, spec, name_off, name_sz, arg_list, slots)) {
		return AST_REF_NULL;
	}

	return ael_build_scalar(ctx, spec, slots);
}

// Build a string op node from bound slots: apply each present slot's concrete
// etype (strict -- a mismatched arg conflicts via ast_set_implicit_type) and
// push the args in wire order onto the u.cdt_op chain.
// pre:  slots is the ael_bind_args output; a NULL slot is an absent trailing
//       optional, so skipping NULLs preserves wire order.
// post: node etype = spec->result_etype, for ael_finalize_str_call.
static ast_ref
ael_build_from_spec(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots)
{
	ast_ref r = ast_new_cdt_op(ctx->pool, spec->ast_type);

	for (uint8_t i = 0; i < spec->param_count; i++) {
		if (slots[i] == AST_REF_NULL) {
			continue;
		}

		if (spec->params[i].etype != AST_ETYPE_AUTO) {
			ast_set_implicit_type(ctx, slots[i], spec->params[i].etype);
		}

		AST_CHAIN_PUSH_TAIL(ctx->pool, r, slots[i]);
	}

	ast_node* np = ast_pool_at(ctx->pool, r);

	np->etype = spec->result_etype;
	ast_set_span(ctx->pool, r, name_off, name_sz);

	return r;
}

// Build a string op whose args are a single msgpack list (replace / replaceAll
// -> [find, replace]). The runtime's string_parse_list reads one raw list
// element, so the bound slots are wrapped in an AST_LIST pushed as the sole
// chain entry -- distinct from ael_build_str, which pushes one entry per slot.
// pre:  slots[0..param_count) are all present (these ops have no optionals).
// post: node etype = spec->result_etype, for ael_finalize_str_call.
static ast_ref
ael_build_str_list(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots)
{
	ast_ref r = ast_new_cdt_op(ctx->pool, spec->ast_type);

	ast_ref list = ast_new(ctx->pool, AST_LIST);
	ast_node* lp = ast_pool_at(ctx->pool, list);

	lp->etype = AST_ETYPE_LIST;

	for (uint8_t i = 0; i < spec->param_count; i++) {
		if (spec->params[i].etype != AST_ETYPE_AUTO) {
			ast_set_implicit_type(ctx, slots[i], spec->params[i].etype);
		}

		AST_LIST_PUSH_TAIL(ctx->pool, list, slots[i]);
	}

	AST_CHAIN_PUSH_TAIL(ctx->pool, r, list);

	ast_node* np = ast_pool_at(ctx->pool, r);

	np->etype = spec->result_etype;
	ast_set_span(ctx->pool, r, name_off, name_sz);

	return r;
}

// regexReplace(pattern: /re/flags, replace: str). The pattern slot must be a
// regex literal (AST_REGEX_LIT from ael_new_regex_operand); its pattern string
// pairs with the replace arg into the [pattern, replace] list
// (string_parse_list) and its flags int becomes the trailing flags arg
// (string_parse_flags).
// pre:  slots[0] = pattern operand, slots[1] = replace expr.
// post: node etype = STR (modify); AST_REF_NULL + diag if pattern isn't a
//       literal.
static ast_ref
ael_build_regex_replace(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref* slots)
{
	ast_node* pat_arg = ast_pool_at(ctx->pool, slots[0]);

	if (pat_arg->type != AST_REGEX_LIT) {
		ael_err(ctx, ast_disp_offset(pat_arg), pat_arg->sz,
				"regexReplace pattern must be a regex literal (/pattern/flags)");
		return AST_REF_NULL;
	}

	ast_ref pat = pat_arg->u.binary.left;
	ast_ref flags = pat_arg->u.binary.right;
	ast_ref replace = slots[1];

	ast_set_implicit_type(ctx, replace, AST_ETYPE_STR);

	// [pattern, replace] list — string_parse_list input.
	ast_ref list = ast_new(ctx->pool, AST_LIST);
	ast_node* lp = ast_pool_at(ctx->pool, list);

	lp->etype = AST_ETYPE_LIST;
	AST_LIST_PUSH_TAIL(ctx->pool, list, pat);
	AST_LIST_PUSH_TAIL(ctx->pool, list, replace);

	// Op chain: [ [pattern, replace] list, flags int ].
	ast_ref r = ast_new_cdt_op(ctx->pool, spec->ast_type);

	AST_CHAIN_PUSH_TAIL(ctx->pool, r, list);
	AST_CHAIN_PUSH_TAIL(ctx->pool, r, flags);

	ast_node* np = ast_pool_at(ctx->pool, r);

	np->etype = spec->result_etype;
	ast_set_span(ctx->pool, r, name_off, name_sz);

	ast_pool_release(ctx->pool, slots[0]); // AST_REGEX_LIT consumed

	return r;
}

// CDT path functions (getKeys / set / count / toInt / ...) resolved by name
// from the func table. Builds the same AST_PATH_FUNC_* node the former keyword
// rules built; the receiver attaches in the path finalizers. ael_bind_args
// enforces arity and rejects named args (path rows are positional-only).
// pre:  spec is a PATH family row; arg_list is the parsed args.
// post: the transient AST_PATH_FUNC_* node (cast wrapper or CDT op) stamped
//       with the name span; AST_REF_NULL + a diagnostic on a bind error.
static ast_ref
ael_resolve_path_fn(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref arg_list)
{
	ast_ref slots[AEL_MAX_PARAMS];

	if (! ael_bind_args(ctx, spec, name_off, name_sz, arg_list, slots)) {
		return AST_REF_NULL;
	}

	// Casts are NK_UNARY wrappers (ast_new); the rest are CDT ops.
	bool is_cast = AEL_IS_CAST_TYPE(spec->ast_type);
	ast_ref r = is_cast ? ast_new(ctx->pool, spec->ast_type)
						: ast_new_cdt_op(ctx->pool, spec->ast_type);

	// The chain lives in u.cdt_op, which a cast's union does not have.
	if (! is_cast && spec->param_count == 1) {
		AST_CHAIN_PUSH_TAIL(ctx->pool, r, slots[0]);
	}

	ast_set_span(ctx->pool, r, name_off, name_sz);

	return r;
}

// modify(expr) resolved by name. method_open pushed the filter scope before the
// body parsed (so its loop vars @ / @key / @index resolved in isolation); pop
// it here on every exit path, then bind the single positional body expr and
// build the SELECT-apply node. The receiver attaches in
// ael_finalize_method_call_path (→ ael_finalize_select_call); a non-path
// receiver is rejected by the value / bin finalizers.
// pre:  spec is the MODIFY row; the filter scope pushed by method_open is
//       still open; arg_list is the parsed body.
// post: pops the filter scope, then returns the AST modify (SELECT-apply)
//       node stamped with the name span; AST_REF_NULL + a diagnostic on a
//       bind error (scope still popped).
static ast_ref
ael_resolve_modify_fn(ael_context* ctx, const ael_func_spec_t* spec,
		uint32_t name_off, uint32_t name_sz, ast_ref arg_list)
{
	ael_filter_scope_pop(ctx);

	ast_ref slots[AEL_MAX_PARAMS];

	if (! ael_bind_args(ctx, spec, name_off, name_sz, arg_list, slots)) {
		return AST_REF_NULL;
	}

	ast_ref r = ael_new_modify(ctx, slots[0], false);
	ast_set_span(ctx->pool, r, name_off, name_sz);

	return r;
}

// Method-style `name(args)` after a receiver dot. Resolves PATH / BIT / HLL /
// MODIFY functions to a transient op node; the receiver attaches in
// ael_finalize_method_call*. SCALAR names error here.
ast_ref
ael_resolve_method_fn(ael_context* ctx, uint32_t name_off, uint32_t name_sz,
		ast_ref arg_list)
{
	const ael_func_spec_t* spec = ael_func_lookup(ctx->input + name_off, name_sz);

	if (spec == NULL) {
		ael_diag_unknown_func(ctx, name_off, name_sz);
		return AST_REF_NULL;
	}

	switch (spec->family) {
	case AEL_FAM_MODIFY:
		return ael_resolve_modify_fn(ctx, spec, name_off, name_sz, arg_list);

	case AEL_FAM_PATH:
		return ael_resolve_path_fn(ctx, spec, name_off, name_sz, arg_list);

	// GEO shares SCALAR's arm: neither has a method form, and both need an arm
	// of their own here or the switch falls through to the HLL builder, which
	// returns AST_REF_NULL with no diagnostic.
	case AEL_FAM_SCALAR:
	case AEL_FAM_GEO:
		ael_err(ctx, name_off, name_sz,
				"this function is not a method — call it as fn(...)");
		return AST_REF_NULL;

	case AEL_FAM_STR:
	case AEL_FAM_BIT:
	case AEL_FAM_HLL: {
		ast_ref slots[AEL_MAX_PARAMS];

		if (! ael_bind_args(ctx, spec, name_off, name_sz, arg_list, slots)) {
			return AST_REF_NULL;
		}

		// STR builds a transient u.cdt_op node from the bound slots; the
		// string finalizer stamps stype EXP_CALL_STRING.
		if (spec->family == AEL_FAM_STR) {
			// replace / replaceAll take their [find, replace] args as one
			// msgpack list (string_parse_list), not a wire element per arg.
			if (spec->ast_type == AST_PATH_FUNC_STR_REPLACE ||
					spec->ast_type == AST_PATH_FUNC_STR_REPLACE_ALL) {
				return ael_build_str_list(ctx, spec, name_off, name_sz, slots);
			}

			// regexReplace: pattern is a regex literal; args become a
			// [pattern, replace] list + flags int.
			if (spec->ast_type == AST_PATH_FUNC_STR_REGEX_REPLACE) {
				return ael_build_regex_replace(ctx, spec, name_off, name_sz,
						slots);
			}

			return ael_build_from_spec(ctx, spec, name_off, name_sz, slots);
		}

		return spec->family == AEL_FAM_BIT
				? ael_build_bit(ctx, spec, name_off, name_sz, slots)
				: ael_build_hll(ctx, spec, name_off, name_sz, slots);
	}
	}

	return AST_REF_NULL; // unreachable — -Wswitch covers new families
}

// --- receiver-attach finalizers (grammar entry points) ---

// Route a resolved method op (path / bit / hll) to its finalizer, attaching
// `recv` and pinning its type. The method-op node's type carries the family.
// This entry is for VALUE receivers (parenthesized expr, blob literal,
// func/method-chain result); ael_build_value_func enforces the value-recv
// restrictions (rejects .exists() / .type() / multi-select getters).
ast_ref
ael_finalize_method_call(ael_context* ctx, ast_ref recv, ast_ref method_fn)
{
	if (recv == AST_REF_NULL || method_fn == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* fp = ast_pool_at(ctx->pool, method_fn);
	ast_node_t t = fp->type;

	// modify only attaches to a path receiver (handled in
	// ael_finalize_method_call_path → ael_finalize_select_call). On a value or
	// bare-bin receiver (the bin variant falls through to here) there is no
	// path to select over, so reject. Was a syntax error under the keyword
	// grammar; now a clearer semantic diagnostic.
	if (t == AST_PATH_FUNC_MODIFY) {
		ael_err(ctx, ast_disp_offset(fp), fp->sz,
				"modify requires a path with at least one segment");
		return AST_REF_NULL;
	}

	if (AEL_IS_PATH_FN_TYPE(t)) {
		return ael_build_value_func(ctx, recv, method_fn);
	}

	if (AEL_IS_STR_TYPE(t)) {
		return ael_finalize_str_call(ctx, recv, method_fn);
	}

	if (AEL_IS_HLL_TYPE(t)) {
		return ael_finalize_hll_call(ctx, recv, method_fn);
	}

	return ael_finalize_bit_call(ctx, recv, method_fn);
}

// Bare-bin receiver variant ($.bin.fn()). Path functions on a bare bin go to
// ael_build_bin_func (which allows .exists() / .type() and the bare-bin
// folds); bit / HLL fall through to the value-receiver routing above. The
// grammar keeps bin_base distinct from value receivers because parens are
// transparent ($.x vs ($.x) reduce to the same node) yet must differ here.
ast_ref
ael_finalize_method_call_bin(ael_context* ctx, ast_ref recv, ast_ref method_fn)
{
	if (recv == AST_REF_NULL || method_fn == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	if (AEL_IS_PATH_FN_TYPE(ast_pool_at(ctx->pool, method_fn)->type)) {
		return ael_build_bin_func(ctx, recv, method_fn);
	}

	return ael_finalize_method_call(ctx, recv, method_fn);
}

// CDT path function on a ctx_list (pathed) receiver — reproduces the former
// `ctx_list . {x_cdt_fn,count_fn,cdt_fn,cast_fn}` operand rules, dispatching on
// the resolved pf node type. All downstream machinery (leaf consumption,
// getIndexes/getRanks etype, deferred-SELECT errors, :PROPERTY) is keyed off
// the pf node type, identical to what the old keyword rules produced.
// pre:  ctx_ref is the pathed receiver (AST_PATH_CTX); pf is the resolved
//       path-func node.
// post: the finalized AST_PATH_CALL for the receiver + verb (routed by pf
//       type to cast / whole-collection / leaf-consuming / SELECT paths);
//       AST_REF_NULL + a diagnostic on an inapplicable verb (e.g. type()).
static ast_ref
ael_finalize_path_on_ctx(ael_context* ctx, ast_ref ctx_ref, ast_ref pf)
{
	ast_node_t t = ast_pool_at(ctx->pool, pf)->type;

	// type() reads a whole bin's particle-type byte — meaningless on a CDT
	// sub-path. Reject here (a ctx_list receiver reaches this finalizer now that
	// type is a TOK_NAME); was a syntax error under the keyword grammar.
	if (t == AST_PATH_FUNC_TYPE) {
		const ast_node* pfp = ast_pool_at(ctx->pool, pf);

		ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
				"type() requires a bare bin reference");
		return AST_REF_NULL;
	}

	// toInt() / toFloat() / toString() — wrap the navigated value, no leaf seg.
	if (AEL_IS_CAST_TYPE(t)) {
		return ael_finalize_cast(ctx, ctx_ref, pf);
	}

	// append() / appendItems() / putItems() / updateItems() / clear() / sort()
	// / join() — whole-CDT, no leaf seg.
	if ((ast_node_table[t].flags & AST_NF_WHOLE_COLL) != 0) {
		return ael_consume_leaf_then_finalize(ctx, ctx_ref, pf, AST_REF_NULL);
	}

	// insertItems() is shaped by its receiver: a list inserts at an index and
	// consumes the leaf, a map merges entries and is whole-collection like
	// putItems. Every other verb's leaf appetite is static in the node table.
	if (t == AST_PATH_FUNC_INSERT_ITEMS) {
		if (ctx_ref == AST_REF_NULL) {
			return AST_REF_NULL;
		}

		ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
		ast_ref tail = cxn->u.ctx_list.tail;
		ast_ref leaf = AST_REF_NULL;

		if (tail != cxn->u.ctx_list.head &&
				ast_pool_at(ctx->pool, tail)->type == AST_LIST_INDEX) {
			leaf = ael_ctx_list_pop(ctx, ctx_ref);
		}

		return ael_consume_leaf_then_finalize(ctx, ctx_ref, pf, leaf);
	}

	// count() — consumes the leaf only when the last seg is multi-select;
	// single-select / bare folds to AS_CDT_OP_SIZE on the navigated value.
	if (t == AST_PATH_FUNC_COUNT) {
		if (ctx_ref == AST_REF_NULL) {
			return AST_REF_NULL;
		}

		ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
		ast_ref leaf = cxn->u.ctx_list.last_is_multi
				? ael_ctx_list_pop(ctx, ctx_ref)
				: AST_REF_NULL;

		return ael_consume_leaf_then_finalize(ctx, ctx_ref, pf, leaf);
	}

	// exists / getKeys / getKeyValues / getTree / getIndexes / getRanks /
	// remove / set / insert / increment — consume the leaf seg.
	ast_ref leaf = ael_ctx_list_pop(ctx, ctx_ref);

	return ael_consume_leaf_then_finalize(ctx, ctx_ref, pf, leaf);
}

// ctx_list (nested-path) receiver variant. Path functions route to
// ael_finalize_path_on_ctx; string ops carry the path natively; bit and hll
// modifies on a path are unsupported; their reads fold the path into a
// value-producing receiver.
ast_ref
ael_finalize_method_call_path(ael_context* ctx, ast_ref ctx_ref, ast_ref method_fn)
{
	if (ctx_ref == AST_REF_NULL || method_fn == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* fp = ast_pool_at(ctx->pool, method_fn);

	if (AEL_IS_PATH_FN_TYPE(fp->type)) {
		return ael_finalize_path_on_ctx(ctx, ctx_ref, method_fn);
	}

	// SELECT-apply modify (.modify(expr)) on a path. Checked before the
	// AST_NF_MODIFY bit/HLL routing below — the SELECT modify node also carries
	// AST_NF_MODIFY and would otherwise be refused as a pathed bit modify.
	if (fp->type == AST_PATH_FUNC_MODIFY) {
		return ael_finalize_select_call(ctx, ctx_ref, method_fn);
	}

	if (AEL_IS_STR_TYPE(fp->type)) {
		return ael_finalize_str_path(ctx, ctx_ref, method_fn);
	}

	bool is_modify = (ast_node_table[fp->type].flags & AST_NF_MODIFY) != 0;

	if (AEL_IS_HLL_TYPE(fp->type)) {
		if (is_modify) {
			ael_err(ctx, ast_disp_offset(fp), fp->sz,
					"hll modify on a nested path is not supported — use a bin-direct receiver");
			return AST_REF_NULL;
		}

		ast_ref recv = ael_bit_recv_from_ctx(ctx, ctx_ref);

		AST_REF_CHECK_RET(recv, AST_REF_NULL);

		return ael_finalize_hll_call(ctx, recv, method_fn);
	}

	// A bit op is a value operator with no context of its own, so a pathed
	// modify can only be simulated as read-modify-write over the same path --
	// two walks, the first of them a read, which never tolerates an absent path.
	// :NO_FAIL therefore could not mean what it says here. Offered again when
	// bit ops gain a context, at which point they path like CDT and string do.
	if (is_modify) {
		ael_err(ctx, ast_disp_offset(fp), fp->sz,
				"bit modify on a nested path is not supported — use a bin-direct receiver");
		return AST_REF_NULL;
	}

	ast_ref recv = ael_bit_recv_from_ctx(ctx, ctx_ref);

	AST_REF_CHECK_RET(recv, AST_REF_NULL);

	return ael_finalize_bit_call(ctx, recv, method_fn);
}

//==========================================================
// Geo literals + geoCompare. Both args of geoCompare are pinned to
// GEOJSON; codegen emits the binary as EXP_CMP_GEO via emit_binary
// (NK_BINARY table entry).

// geoJson('...') -- AST_GEO_LITERAL whose u.str holds the source JSON
// bytes (offset+sz into input). The runtime validates the JSON at decode
// time; the parser stays cheap.
static ast_ref
ael_new_geo_literal(ael_context* ctx, uint32_t offset, uint32_t sz)
{
	ast_ref r = ast_new(ctx->pool, AST_GEO_LITERAL);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->etype = AST_ETYPE_GEOJSON;
	ast_set_span(ctx->pool, r, offset, sz);
	n->u.str.str = ctx->input + offset;
	n->u.str.sz = sz;

	return r;
}

// geoCompare(a, b) -- pins both args to GEOJSON and builds an AST_CMP_GEO
// binary that emits as EXP_CMP_GEO (returns BOOLEAN).
static ast_ref
ael_new_geo_compare(ael_context* ctx, ast_ref a, ast_ref b)
{
	cf_assert(a != AST_REF_NULL && b != AST_REF_NULL, AS_EXP,
			"ael_new_geo_compare: null operand");

	ast_set_implicit_type(ctx, a, AST_ETYPE_GEOJSON);
	ast_set_implicit_type(ctx, b, AST_ETYPE_GEOJSON);

	ast_ref r = ast_new_binary(ctx->pool, AST_CMP_GEO, a, b);
	ast_pool_at(ctx->pool, r)->etype = AST_ETYPE_TRILEAN;

	return r;
}

// path expressions — .select() helpers

// .select() -- multi-element extract over a wildcard path.
static ast_ref
ael_new_select(ael_context* ctx, as_cdt_select_flags sel_type)
{
	ast_ref r = ast_new(ctx->pool, AST_PATH_FUNC_SELECT);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.modify.value = AST_REF_NULL;
	n->u.modify.sel_type = sel_type;

	// SELECT defaults to returning a LIST of values; the runtime may
	// override based on the SELECT_TREE select-op type. Codegen sets the
	// final etype from sel_type.
	switch (sel_type) {
	case AS_CDT_SELECT_TREE:
		n->etype = AST_ETYPE_MAP;
		break;
	case AS_CDT_SELECT_COUNT:
		n->etype = AST_ETYPE_INT;
		break;
	case AS_CDT_SELECT_EXISTS:
		n->etype = AST_ETYPE_TRILEAN;
		break;
	default:
		n->etype = AST_ETYPE_LIST;
		break;
	}

	return r;
}

// Attach the path context to a select-family node (.select() / .modify()
// / wildcard-.remove()) and validate the path is multi-select.
static ast_ref
ael_finalize_select_call(ael_context* ctx, ast_ref ctx_ref, ast_ref pf)
{
	cf_assert(ctx_ref != AST_REF_NULL && pf != AST_REF_NULL, AS_EXP,
			"ael_finalize_select_call: null operand");

	ast_node* pfp = ast_pool_at(ctx->pool, pf);
	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
	ast_ref bin = cxn->u.ctx_list.head;
	bool is_modify = pfp->type == AST_PATH_FUNC_MODIFY ||
			pfp->type == AST_PATH_FUNC_PSELECT_REMOVE;

	// Validate: read-style SELECT calls need at least one multi-select
	// segment (wildcard, range, list, or REL range) — otherwise the path
	// is a chain of single-element navigations and belongs on the CDT_OP
	// fast path. .modify(expr) is uniform: it always uses SELECT_APPLY
	// regardless of path shape, so an all-single path is fine for
	// modify. Wildcards don't carry AST_NF_PLURAL on their flag word, so
	// check type explicitly.
	if (pfp->type != AST_PATH_FUNC_MODIFY) {
		if (! ctx_list_has_seg_flags(ctx, ctx_ref, AST_NF_BY_EXP | AST_NF_PLURAL)) {
			ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
					"path expression requires at least one wildcard / range segment");
			return AST_REF_NULL;
		}
	}

	// The SELECT op ignores the ctx create bits — it can't create anything, it
	// requires an existing list or map bin. Rejecting keeps the flag from
	// silently doing nothing. .modify() lands here whatever the path shape,
	// so it needs its own wording — telling those users to make the path
	// single-select sends them after a fix that cannot work.
	ast_ref cr_seg = ctx_list_seg_with_create_props(ctx, ctx_ref);

	if (cr_seg != AST_REF_NULL) {
		ast_node* crp = ast_pool_at(ctx->pool, cr_seg);
		const char* what = ael_create_prop_name(ast_seg_props(crp));

		if (pfp->type == AST_PATH_FUNC_MODIFY) {
			ael_errf(ctx, crp->offset, crp->sz,
					"modify() does not support %s — use a single-target write such as setTo() or add()",
					what);
		}
		else {
			ael_errf(ctx, crp->offset, crp->sz,
					"a multi-select path cannot carry %s", what);
		}

		return AST_REF_NULL;
	}

	// The bin's type is determined by the FIRST segment only: a map
	// seg pins it to MAP, a list seg pins it to LIST. Subsequent segs
	// operate on sub-containers and tell us nothing about the bin
	// itself. If the first seg is a wildcard, leave the bin as
	// AUTO_CDT — the runtime SELECT op accepts either particle type.
	ast_ref first = ast_pool_at(ctx->pool, bin)->next;
	ast_etype bin_etype = AST_ETYPE_AUTO_CDT;

	if (first != AST_REF_NULL) {
		ast_node_t st = ast_pool_at(ctx->pool, first)->type;

		if (st != AST_WILDCARD_SEG) {
			bin_etype = (ast_node_table[st].flags & AST_NF_MAP_SEG) != 0
					? AST_ETYPE_MAP
					: AST_ETYPE_LIST;
		}
	}

	ast_bin_set_implicit_type(ctx, bin, bin_etype);

	exp_call_stype stype = EXP_CALL_CDT;

	if (is_modify) {
		stype |= EXP_CALL_FLAG_MODIFY_LOCAL;
	}

	return ael_wrap_path_call(ctx, stype, ctx_ref, pf, pfp->etype, bin);
}

// ael_build_value_func — parallel of ael_build_bin_func for value-recv
// (`(expr).func()` form). Encodes the call with the expression as the
// receiver — no CONTEXT_EVAL, no path navigation. Rejects bin-specific
// ops (exists, type) and multi-select-requiring reads (getKeys etc.).
// pre:  recv is the value receiver; pf is the resolved path-func; cdt_op_code
//       is its resolved CDT op.
// post: pf is morphed to AST_CDT_OP and wrapped in a value-recv AST_PATH_CALL
//       (u.call.ctx = recv directly, no AST_PATH_CTX); recv pinned to
//       recv_etype unless AUTO.
static ast_ref
build_value_cdt_call(ael_context* ctx, ast_ref recv, ast_ref pf, int cdt_op_code,
		bool is_modify, ast_etype recv_etype, ast_etype result_etype)
{
	ast_node* pfp = ast_pool_at(ctx->pool, pf);

	if (recv_etype != AST_ETYPE_AUTO) {
		ast_set_implicit_type(ctx, recv, recv_etype);
	}

	// Morph the pf node to AST_CDT_OP with op_code resolved. Existing
	// arg chain (e.g. the value-expr for .append(v)) flows through
	// u.cdt_op.head/count unchanged.
	pfp->type = AST_CDT_OP;
	pfp->u.cdt_op.op_code = (uint16_t)cdt_op_code;
	pfp->u.cdt_op.is_modify = is_modify;
	pfp->etype = result_etype;

	if (ael_reject_inert_no_fail(ctx, pfp)) {
		return AST_REF_NULL;
	}

	// stype = EXP_CALL_CDT (| MODIFY_LOCAL). u.call.ctx holds the receiver
	// value-expression directly (no AST_PATH_CTX list — value-recv
	// path-calls reuse the BITS pattern where ctx is a sub-expression).
	exp_call_stype stype = EXP_CALL_CDT;

	if (is_modify) {
		stype |= EXP_CALL_FLAG_MODIFY_LOCAL;
	}

	return ael_wrap_path_call(ctx, stype, recv, pf, result_etype, recv);
}

// toInt() / toFloat() take an INT | FLOAT | STRING operand (the concrete op is
// picked from the resolved type in ael_dispatch_to_cast); toString() produces a
// STRING. Pin the receiver to the cast's source type so untyped operands narrow
// and conflicting `:T` pins surface as AST_ETYPE_ERROR at the post-parse check.
// is_bin selects the bin-aware setter.
// pre:  recv is the cast receiver; pf is a toInt / toFloat / toString node;
//       is_bin marks a bin receiver (vs a value expr).
// post: recv pinned to the cast's source type (via ael_cast_types) and pf's
//       unary operand set to recv; pf etype = the target.
static ast_ref
finish_cast(ael_context* ctx, ast_ref recv, ast_ref pf, bool is_bin)
{
	ast_node* pfp = ast_pool_at(ctx->pool, pf);
	ast_etype src_type;
	ast_etype result_type;

	ael_cast_types(pfp->type, &src_type, &result_type);

	if (is_bin) {
		ast_bin_set_implicit_type(ctx, recv, src_type);
	}
	else {
		ast_set_implicit_type(ctx, recv, src_type);
	}

	pfp->etype = result_type;
	pfp->u.unary.operand = recv;

	return pf;
}

// (expr).func() -- value-recv path-func dispatch. Mirrors
// ael_build_bin_func but with an expression instead of a bin. Rejects
// bin-specific ops (exists, type) and multi-select-requiring reads
// (getKeys/getKeyValues/getTree) with helpful diagnostics.
static ast_ref
ael_build_value_func(ael_context* ctx, ast_ref recv, ast_ref pf)
{
	cf_assert(recv != AST_REF_NULL && pf != AST_REF_NULL, AS_EXP,
			"ael_build_value_func: null operand");

	ast_node* pfp = ast_pool_at(ctx->pool, pf);
	ast_node_t pf_type = pfp->type;

	// Bin-specific ops — reject for value-recv with a clear hint.
	if (pf_type == AST_PATH_FUNC_TYPE) {
		ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
				".type() requires a bare bin reference");
		return AST_REF_NULL;
	}

	if (pf_type == AST_PATH_FUNC_EXISTS) {
		ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
				".exists() requires a bare bin reference");
		return AST_REF_NULL;
	}

	// Reads that need a multi-select segment — reject with same
	// diagnostic the bare-bin case produces.
	if (pf_type == AST_PATH_FUNC_GET_KEYS ||
			pf_type == AST_PATH_FUNC_GET_KEY_VALUES ||
			pf_type == AST_PATH_FUNC_GET_TREE ||
			pf_type == AST_PATH_FUNC_GET_MAPS) {
		ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
				"this path function requires a multi-select segment");
		return AST_REF_NULL;
	}

	// Positional getters need a path segment to position against — a bare
	// value receiver has none.
	if (pf_type == AST_PATH_FUNC_GET_INDEXES ||
			pf_type == AST_PATH_FUNC_GET_RANKS) {
		ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
				"getIndexes() / getRanks() require a path segment selecting element(s)");
		return AST_REF_NULL;
	}

	// .toInt() / .toFloat() / .toString() — wrap recv in the cast unary.
	if (AEL_IS_CAST_TYPE(pf_type)) {
		return finish_cast(ctx, recv, pf, false);
	}

	// .count() — polymorphic AS_CDT_OP_SIZE on the value. Recv may be
	// LIST or MAP; pin to AUTO_CDT so runtime resolves from particle.
	if (pf_type == AST_PATH_FUNC_COUNT) {
		// Clear any existing chain on the pf (none expected for count).
		AST_CHAIN_CLR(ctx->pool, pf);
		return build_value_cdt_call(ctx, recv, pf, AS_CDT_OP_SIZE, false,
				AST_ETYPE_AUTO_CDT, AST_ETYPE_INT);
	}

	// Whole-container CDT modifies — clear / sort / append. Recv etype
	// determines LIST vs MAP variant for CLEAR; sort and append are LIST.
	if (pf_type == AST_PATH_FUNC_APPEND) {
		return build_value_cdt_call(ctx, recv, pf, AS_CDT_OP_LIST_APPEND, true,
				AST_ETYPE_LIST, AST_ETYPE_LIST);
	}

	if (pf_type == AST_PATH_FUNC_SORT) {
		return build_value_cdt_call(ctx, recv, pf, AS_CDT_OP_LIST_SORT, true,
				AST_ETYPE_LIST, AST_ETYPE_LIST);
	}

	// join(separator) — whole-list read (not a modify) returning STR. The
	// separator rides the pf chain and flows through unchanged.
	if (pf_type == AST_PATH_FUNC_JOIN) {
		return build_value_cdt_call(ctx, recv, pf, AS_CDT_OP_STRING_LIST_JOIN,
				false, AST_ETYPE_LIST, AST_ETYPE_STR);
	}

	if (pf_type == AST_PATH_FUNC_CLEAR) {
		// Need to know LIST vs MAP. Inspect recv's resolved etype.
		ast_etype recv_etype = ast_pool_at(ctx->pool, recv)->etype;
		bool is_map = recv_etype == AST_ETYPE_MAP;
		bool is_list = recv_etype == AST_ETYPE_LIST;

		if (! is_map && ! is_list) {
			ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
					".clear() requires a LIST or MAP-typed receiver");
			return AST_REF_NULL;
		}

		return build_value_cdt_call(ctx, recv, pf,
				is_map ? AS_CDT_OP_MAP_CLEAR : AS_CDT_OP_LIST_CLEAR, true,
				recv_etype, recv_etype);
	}

	// Other path-funcs not supported on value-recv — emit a generic diag.
	ael_err(ctx, ast_disp_offset(pfp), pfp->sz,
			"path function not supported on parenthesized-expression receiver");
	return AST_REF_NULL;
}

// .type() / .exists() on a bare bin fold to a name-only node — they read
// the bin's particle-type byte / presence, not its value, so the canonical
// bin is exempted from the strict resolve check (other refs may still pin
// it, e.g. `$.x.type() == 1 and $.x > 5`).
static ast_ref
bin_meta_node(ael_context* ctx, ast_ref bin, ast_ref pf, ast_node_t type,
		ast_etype etype)
{
	ast_node* canon = ast_bin_canonical(ctx->pool, ast_pool_at(ctx->pool, bin));

	canon->u.bin.allow_unresolved = true;

	ast_ref r = ast_new(ctx->pool, type);
	ast_node* np = ast_pool_at(ctx->pool, r);

	// Adopt the reference's whole span, prefix reach-back included, so the
	// query node's own offset lands on the name like the bin's does.
	ast_copy_span(ctx->pool, r, bin);
	np->u.bin_type.name_sz = canon->u.bin.name_sz;
	np->etype = etype;

	ast_pool_release(ctx->pool, pf);
	return r;
}

// $.bin.func() -- bare-bin path-func dispatch: .type() / .exists() fold
// to AST_BIN_TYPE / AST_BIN_EXISTS, casts wrap the bin, everything else
// routes through the leaf-consuming CDT finalizers.
static ast_ref
ael_build_bin_func(ael_context* ctx, ast_ref bin, ast_ref pf)
{
	cf_assert(bin != AST_REF_NULL && pf != AST_REF_NULL, AS_EXP,
			"ael_build_bin_func: null operand");

	ast_node* pfp = ast_pool_at(ctx->pool, pf);

	if (pfp->type == AST_CDT_OP) {
		return ael_finalize_path_call_wrap(ctx,
				ast_new_path_ctx_bin(ctx->pool, bin), pf, EXP_CALL_CDT);
	}

	if (pfp->type == AST_PATH_FUNC_TYPE) {
		return bin_meta_node(ctx, bin, pf, AST_BIN_TYPE, AST_ETYPE_INT);
	}

	if (AEL_IS_CAST_TYPE(pfp->type)) {
		// .toInt() / .toFloat() / .toString() — wrap the bin in the cast unary.
		return finish_cast(ctx, bin, pf, true);
	}

	switch (pfp->type) {
	case AST_PATH_FUNC_EXISTS:
		return bin_meta_node(ctx, bin, pf, AST_BIN_EXISTS, AST_ETYPE_TRILEAN);

	default:
		return ael_consume_leaf_then_finalize(ctx,
				ast_new_path_ctx_bin(ctx->pool, bin), pf, AST_REF_NULL);
	}
}

// path expressions — modify(), wildcard-remove(), loop vars,
// filter scope, *::field shorthand.

// .modify(expr [, noFail]) -- applies the embedded expression to each
// element matched by the wildcard path (SELECT_APPLY wire form).
static ast_ref
ael_new_modify(ael_context* ctx, ast_ref apply_expr, bool nofail)
{
	ast_ref r = ast_new(ctx->pool, AST_PATH_FUNC_MODIFY);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.modify.value = apply_expr;
	n->u.modify.sel_type = AS_CDT_SELECT_APPLY;
	n->u.modify.props = nofail ? AST_PROP_NO_FAIL : 0;
	// modify returns the modified collection — a CDT shape.
	n->etype = AST_ETYPE_AUTO_CDT;

	return r;
}

// Wildcard-path .remove() -- synthesizes a SELECT_APPLY with a canned
// removeResult() body.
static ast_ref
ael_new_pselect_remove(ael_context* ctx, bool nofail)
{
	ast_ref r = ast_new(ctx->pool, AST_PATH_FUNC_PSELECT_REMOVE);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.modify.value = AST_REF_NULL;
	n->u.modify.sel_type = AS_CDT_SELECT_APPLY;
	n->u.modify.props = nofail ? AST_PROP_NO_FAIL : 0;
	n->etype = AST_ETYPE_AUTO_CDT;

	return r;
}

// Prepend r onto the current scope's chain for builtin (@ or @key).
// @index doesn't chain — always pinned INT.
// pre:  r is a loop-var node of kind builtin (@ / @key / @index).
// post: r is prepended to the matching scope root (at_root for @, key_root
//       for @key); @index is a no-op (never chains, always pinned INT).
static void
loop_var_chain_push(ael_context* ctx, as_exp_builtin builtin, ast_ref r)
{
	ast_node* n = ast_pool_at(ctx->pool, r);

	if (builtin == AS_EXP_BUILTIN_KEY) {
		n->u.loop_var.next_same_kind = ctx->key_root;
		ctx->key_root = r;
	}
	else if (builtin == AS_EXP_BUILTIN_VALUE) {
		n->u.loop_var.next_same_kind = ctx->at_root;
		ctx->at_root = r;
	}
}

ast_ref
ael_new_loop_var(ael_context* ctx, as_exp_builtin builtin, uint32_t tok_offset,
		uint32_t tok_sz)
{
	if (ctx->filter_depth == 0) {
		ael_err(ctx, tok_offset, tok_sz,
				"loop variable requires an enclosing wildcard filter or modify");
		// Return a sentinel UNKNOWN node rather than AST_REF_NULL so the
		// rest of the parser's reduction chain (n-ary merges, comparisons,
		// etc.) doesn't dereference NULL before the diagnostic propagates
		// out.
		ast_ref u = ast_new(ctx->pool, AST_UNKNOWN);

		ast_set_span(ctx->pool, u, tok_offset, tok_sz);
		return u;
	}

	ast_ref r = ast_new_loop_var(ctx->pool, builtin);

	loop_var_chain_push(ctx, builtin, r);
	// ast_new spans from the parser lookahead, which for a path root is the
	// dot after the loop variable.
	ast_set_span(ctx->pool, r, tok_offset, tok_sz);
	return r;
}

ast_ref
ael_new_wild_filter_seg(ael_context* ctx, ast_ref filter_expr,
		uint32_t body_offset, uint32_t body_sz)
{
	ast_ref r = ast_new(ctx->pool, AST_WILDCARD_SEG);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.by_exp_seg.filter = filter_expr;
	n->u.by_exp_seg.body_offset = body_offset;
	n->u.by_exp_seg.body_sz = body_sz;

	return r;
}

ast_ref
ael_extend_with_and_seg(ael_context* ctx, ast_ref ctx_ref, ast_ref filter_expr,
		uint32_t body_offset, uint32_t body_sz)
{
	if (ctx_ref == AST_REF_NULL || filter_expr == AST_REF_NULL) {
		return AST_REF_NULL;
	}

	ast_node* cxn = ast_pool_at(ctx->pool, ctx_ref);
	ast_node_t tail_type = ast_pool_at(ctx->pool, cxn->u.ctx_list.tail)->type;

	// The grammar's ctx_list LHS already stops AND as the first seg. These
	// two orderings it cannot see, and both mirror a rejection the select
	// stack makes at run time: a wildcard seg emits an EXP base, and an AND
	// onto either an EXP base or an existing AND is refused there.
	if (tail_type == AST_WILDCARD_SEG) {
		ael_err(ctx, body_offset, body_sz,
				"&[?(...)] cannot follow wildcard *[?(...)]");
		return AST_REF_NULL;
	}

	if (tail_type == AST_AND_EXP_SEG) {
		ael_err(ctx, body_offset, body_sz,
				"&[?(...)] cannot follow another &[?(...)]");
		return AST_REF_NULL;
	}

	ast_ref r = ast_new(ctx->pool, AST_AND_EXP_SEG);
	ast_node* n = ast_pool_at(ctx->pool, r);

	n->u.by_exp_seg.filter = filter_expr;
	n->u.by_exp_seg.body_offset = body_offset;
	n->u.by_exp_seg.body_sz = body_sz;

	return ael_ctx_list_append(ctx, ctx_ref, r);
}

bool
ael_filter_scope_push(ael_context* ctx)
{
	// Always increment so push/pop stay balanced even when a nested
	// scope is rejected — the grammar's reduction action calls pop
	// unconditionally and we need the outer scope's state to survive
	// the inner pop.
	if (++ctx->filter_depth > 1) {
		ael_err(ctx, ctx->last_token_offset, ctx->last_token_sz,
				"nested filter / modify not allowed");
		return false;
	}

	return true;
}

void
ael_filter_scope_pop(ael_context* ctx)
{
	if (ctx->filter_depth == 0) {
		return; // shouldn't happen with balanced push/pop
	}

	if (--ctx->filter_depth != 0) {
		return; // inner pop of a rejected-nested scope; preserve outer
	}

	// Outermost pop — unify the just-closed scope's loop-var chains,
	// then reset so a subsequent top-level filter starts with empty
	// chains.
	ast_unify_loop_var_chain(ctx->pool, ctx->at_root, &ctx->diags);
	ast_unify_loop_var_chain(ctx->pool, ctx->key_root, &ctx->diags);

	ctx->at_root = AST_REF_NULL;
	ctx->key_root = AST_REF_NULL;
}

void
ael_check_when_cond(ael_context* ctx, ast_ref cond)
{
	if (cond == AST_REF_NULL) {
		return;
	}

	ast_set_implicit_type(ctx, cond, AST_ETYPE_TRILEAN);

	ast_node* cp = ast_pool_at(ctx->pool, cond);

	if (ast_type_resolved(cp->etype) && (cp->etype & AST_ETYPE_TRILEAN) == 0 &&
			! cp->has_deferable) {
		ael_err(ctx, ast_disp_offset(cp), cp->sz,
				"when case condition must be a boolean expression");
	}
}

//==========================================================
// Parse driver.
//
// The lexer / lemon-parser loop lives in the generated parser (ael_run_parser,
// ael_parser.y) because it owns the stack-allocated yyParser. The context
// setup, lexer-error message map, post-parse type-resolution, and result
// assembly live here so the generated file stays thin.
//

const char*
ael_lex_error_msg(int tok)
{
	switch (tok) {
	case TOK_ERROR_INT_RANGE:
		return "integer out of range";
	case TOK_ERROR_UNCLOSED_STRING:
		return "unclosed string literal";
	case TOK_ERROR_UNCLOSED_REGEX:
		return "unclosed regex literal";
	case TOK_ERROR_UNCLOSED_COMMENT:
		return "unclosed comment";
	case TOK_ERROR_BLOB_ODD_HEX:
		return "blob literal must have an even number of hex characters";
	case TOK_ERROR_B64_BAD_LEN:
		return "base64 blob literal length must be a multiple of 4";
	case TOK_ERROR_FLOAT_RANGE:
		return "float out of range";
	case TOK_ERROR_RANGE_DOTS:
		return "'..' is reserved -- use ':' for ranges (e.g. [1:3])";
	case TOK_ERROR_EXP_FLOAT:
		return "exponent notation is not supported";
	case TOK_ERROR_LOGICAL_NOT:
		return "use not(...) for logical negation";
	default:
		return "unexpected character";
	}
}

// Move src's node body into dst's slot, keeping dst's sibling link. The
// post-inference rewrites replace a node in place because the walk carries no
// parent edge to repoint -- so the parent's existing downward ref stays valid.
// src is left orphaned; the pool is bulk-destroyed, and both emitters resolve a
// bin operand by name (not node identity), so a duplicated body is inert.
static void
ael_splice_node(ael_context* ctx, ast_ref dst, ast_ref src)
{
	ast_ref saved_next = ast_pool_at(ctx->pool, dst)->next;

	*ast_pool_at(ctx->pool, dst) = *ast_pool_at(ctx->pool, src);
	ast_pool_at(ctx->pool, dst)->next = saved_next;
}

// Lower a resolved-STR n-ary AST_ADD (a string `+`) in place to the concat
// append-fold -- append(append(o1, o2), ...) -- the shape concat() produces, so
// both emitters need zero string-add-specific emit code.
// pre:  ref is a resolved-STR AST_ADD; operands are the circular nmath list.
// post: ref's slot holds the fold head, with its `next` preserved -- so the
//       parent's downward ref still resolves and needs no relink.
static void
ael_lower_str_add(ael_context* ctx, ast_ref ref)
{
	ast_node* np = ast_pool_at(ctx->pool, ref);
	ast_ref head = ast_nmath_head(ctx->pool, np);

	// ael_concat_append ends in ael_finalize_str_call, whose ael_span_call
	// anchors a call's end at the parser lookahead. That only holds mid-parse;
	// here parsing is over and the lookahead sits at end-of-source, so each
	// fold would be spanned operand..EOF and a fault inside a nested string '+'
	// would highlight the rest of the expression. Span each fold over the
	// operand it absorbs, and put the AST_ADD's own span back after the splice.
	// Capture it now - ael_splice_node overwrites the slot np points into.
	//
	// The operand alone, not receiver..operand: ast_nmath_merge flattens
	// through the paren rule, so by here the source's bracketing is gone and a
	// run computed from operand offsets can straddle one side of a paren pair.
	// ('a' + 'b') + '\x80' would span the fold "'a' + 'b') + '\x80'". An
	// operand's own span is a token or a paren-widened balanced group, so it is
	// well formed by construction.
	ast_span add_span = ast_get_span(ctx->pool, ref);

	// Linearize the circular operand list (tail->next == head).
	ast_pool_at(ctx->pool, np->u.nmath.tail)->next = AST_REF_NULL;

	ast_ref acc = head;
	ast_ref arg = ast_pool_at(ctx->pool, head)->next;

	ast_pool_at(ctx->pool, head)->next = AST_REF_NULL; // detach the seed

	while (arg != AST_REF_NULL) {
		ast_node* ap = ast_pool_at(ctx->pool, arg);
		ast_ref next = ap->next;

		// Read before the append - ael_concat_append allocates, which may move
		// the pool out from under ap.
		ast_span arg_span = ast_get_span(ctx->pool, arg);

		ap->next = AST_REF_NULL;
		acc = ael_concat_append(ctx, acc, arg);
		cf_assert(acc != AST_REF_NULL, AS_EXP,
				"string + lowering produced a null fold");

		if (arg_span.sz != 0) {
			ast_put_span(ctx->pool, acc, arg_span);
		}

		arg = next;
	}

	ael_splice_node(ctx, ref, acc);
	ast_put_span(ctx->pool, ref, add_span);
}

// Resolve a polymorphic toInt()/toFloat() (CAST_INT / CAST_FLOAT) from its
// operand's now-final type: STRING parses (the AST_PATH_CALL / AST_STR_OP shape
// ael_finalize_str_call builds); the cross numeric type casts, keeping CAST_INT
// / CAST_FLOAT so the EXP_TO_* emit is unchanged; the target type itself is a
// no-op, spliced away so neither emitter ever sees a pass-through node. A
// still-multi-bit (unresolved) operand is an error -- an illegal concrete type
// was already rejected by the AUTO_ADD pin in ael_finalize_cast / finish_cast.
// pre:  ref is a CAST_INT / CAST_FLOAT node; its operand's etype is final.
// post: ref's slot holds the resolved form, with its `next` preserved.
static void
ael_dispatch_to_cast(ael_context* ctx, ast_ref ref)
{
	ast_node* np = ast_pool_at(ctx->pool, ref);
	ast_ref operand = np->u.unary.operand;
	ast_node* op = ast_pool_at(ctx->pool, operand);

	switch (op->etype) {
	case AST_ETYPE_STR: {
		ast_ref str_fn = ast_new_cdt_op(ctx->pool,
				np->type == AST_PATH_FUNC_CAST_INT ? AST_PATH_FUNC_STR_TO_INT
												   : AST_PATH_FUNC_STR_TO_FLOAT);

		// The finalizer reads the result type off the transient node and the op
		// code out of the node table, as it does for every other string method.
		ast_pool_at(ctx->pool, str_fn)->etype = np->etype;
		ast_set_span(ctx->pool, str_fn, ast_disp_offset(np), np->sz);

		// ael_span_call, which the finalizer ends with, spans a call from its
		// receiver to the parser's lookahead. That only holds mid-parse; here
		// parsing is over and the lookahead sits at end-of-source, which would
		// focus a fault in this call on the whole expression. Span it over
		// receiver..cast instead.
		uint32_t start = ast_disp_offset(ast_pool_at(ctx->pool, operand));
		uint32_t end = ast_disp_offset(np) + np->sz;
		ast_ref call = ael_finalize_str_call(ctx, operand, str_fn);

		if (call != AST_REF_NULL && end > start) {
			ast_set_span(ctx->pool, call, start, end - start);
		}

		ael_splice_node(ctx, ref, call);
		break;
	}
	case AST_ETYPE_INT:
	case AST_ETYPE_FLOAT:
		if (op->etype == np->etype) {
			ael_splice_node(ctx, ref, operand);
		}
		break;
	default:
		ael_errf(ctx, ast_disp_offset(np), np->sz,
				"unresolved %s operand type, pin it with :INT, :FLOAT, or :STRING",
				ast_node_table[np->type].name);
		break;
	}
}

static void
bin_live_cb(ast_pool* pool, ast_ref child, void* arg)
{
	(void)pool;

	bin_live_walk((bin_live_ctx*)arg, child);
}

static void
bin_live_walk(bin_live_ctx* lc, ast_ref ref)
{
	if (ref == AST_REF_NULL || lc->found || lc->depth >= EXP_MAX_DEPTH) {
		return;
	}

	ast_node* np = ast_pool_at(lc->pool, ref);

	if ((np->type == AST_BIN || np->type == AST_BIN_REF) &&
			ast_bin_canonical(lc->pool, np) == lc->canon) {
		lc->found = true;
		return;
	}

	lc->depth++;
	ast_children_foreach(lc->pool, ref, bin_live_cb, lc);
	lc->depth--;
}

// A meta terminal folds its mention into a name-only node, so that bin node
// leaves the live tree -- one still reachable as a bin is being read.
static bool
bin_value_is_read(ael_context* ctx, const ast_node* canon)
{
	bin_live_ctx lc = { .pool = ctx->pool, .canon = canon };

	bin_live_walk(&lc, ctx->root);

	return lc.found;
}

typedef struct ael_walk_ctx_s {
	ael_context* ctx;
	uint32_t depth;
} ael_walk_ctx;

static void
ael_check_etype_cb(ast_pool* pool, ast_ref child, void* arg)
{
	(void)pool;

	const ael_walk_ctx* wc = (const ael_walk_ctx*)arg;

	ael_check_etype(wc->ctx, child, wc->depth + 1);
}

// The emitters read a flagged node's wire type from its etype, and neither can
// invent one -- so the author has to say. Deepest first, naming the operand
// that needs the pin rather than the operator that inherited its ambiguity.
static void
ael_check_etype(ael_context* ctx, ast_ref ref, uint32_t depth)
{
	if (ref == AST_REF_NULL || depth >= EXP_MAX_DEPTH) {
		return;
	}

	ael_walk_ctx wc = { .ctx = ctx, .depth = depth };

	ast_children_foreach(ctx->pool, ref, ael_check_etype_cb, &wc);

	const ast_node* np = ast_pool_at(ctx->pool, ref);

	if ((ast_node_table[np->type].flags & AST_NF_NEEDS_ETYPE) == 0 ||
			ast_type_resolved(np->etype)) {
		return;
	}

	// The select family takes its type from the bin downstream, so its own
	// etype is free to stay open.
	if (np->type == AST_PATH_CALL &&
			ast_is_select_family(ast_pool_at(ctx->pool, np->u.call.call_op)->type)) {
		return;
	}

	ael_err(ctx, ast_disp_offset(np), np->sz, "cannot infer type — pin with :T");
}

static void
ael_post_infer_rewrite_cb(ast_pool* pool, ast_ref child, void* arg)
{
	(void)pool;

	const ael_walk_ctx* rc = (const ael_walk_ctx*)arg;

	ael_post_infer_rewrite(rc->ctx, child, rc->depth + 1);
}

// Both rewrites key on a type that is only final once inference has run, which
// is what puts them in a pass of their own. Descent is table-driven, so a node
// type that can carry a subexpression is covered as soon as its table entry
// declares a kind. Only the live tree is walked -- the AST_ADDs an n-ary merge
// orphans are unreachable anyway.
static void
ael_post_infer_rewrite(ael_context* ctx, ast_ref ref, uint32_t depth)
{
	if (ref == AST_REF_NULL || depth >= EXP_MAX_DEPTH) {
		return;
	}

	// Children first - both rewrites key on an operand's final type.
	ael_walk_ctx rc = { .ctx = ctx, .depth = depth };

	ast_children_foreach(ctx->pool, ref, ael_post_infer_rewrite_cb, &rc);

	// Re-read: read after the descent rather than holding a node pointer across
	// it, since a rewrite splices over the node slot it replaces.
	ast_node* np = ast_pool_at(ctx->pool, ref);

	switch (np->type) {
	case AST_PATH_FUNC_CAST_INT:
	case AST_PATH_FUNC_CAST_FLOAT:
		ael_dispatch_to_cast(ctx, ref);
		break;
	case AST_ADD:
		if (np->etype == AST_ETYPE_STR) {
			ael_lower_str_add(ctx, ref);
		}
		break;

	default:
		break;
	}
}

// Post-parse resolution: unify the loop-var chains, then require every bin and
// $.key() to resolve to a single concrete type (or fault with a tailored
// diagnostic). Silent default types are disallowed (type-inference.md).
// pre:  the parse loop has run; ctx's bin_root / key_meta_root / at_root /
//       key_root chains are populated.
// post: an unresolved bin / key adds a diagnostic to ctx->diags; on success
//       the AST is ready for codegen.
static void
ael_finalize_parse(ael_context* ctx)
{
	ast_pool* pool = ctx->pool;

	// A create-order still parked here never found a container to name: the
	// terminal that ended the path took no create flags, or took none from this
	// position. Not every terminal routes through the leaf-consume funnel
	// (modify() does not), so this is the catch-all that keeps an unplaced flag
	// from being dropped silently.
	report_unplaced_create_slots(ctx);

	// For ael_parse_filter_body (initial_filter_depth > 0), the body
	// is the top-level program and never goes through filter_scope_pop.
	// Unify whatever loop-var chains accumulated at top level so the
	// standalone-filter case gets the same shared-inference checks as
	// nested sub-programs. For normal ael_parse, filter_depth starts
	// at 0 so loop vars can't appear at top level — chains stay empty
	// and the calls are no-ops.
	ast_unify_loop_var_chain(pool, ctx->at_root, &ctx->diags);
	ast_unify_loop_var_chain(pool, ctx->key_root, &ctx->diags);

	// Post-parse: bins must resolve to a single concrete type before
	// codegen. AUTO_NUMERIC = used in math/function context but never
	// narrowed; AUTO_CDT = used in select/path-expression context where
	// the first segment is a wildcard (so neither MAP nor LIST is
	// pinned). The runtime VM is strict — no NIL/ANY escape — so the
	// user must disambiguate via the postfix `:T` form: `$.bin:INT`,
	// `$.bin:FLOAT`, `$.bin:LIST`, `$.bin:MAP` (DECISIONS row 13).
	if (! ael_diag_has_error(&ctx->diags)) {
		for (ast_ref br = ctx->bin_root; br != AST_REF_NULL;) {
			ast_node* bp = ast_pool_at(pool, br);

			if (! ast_type_resolved(bp->etype)) {
				if (bp->etype == AST_ETYPE_ERROR) {
					// Conflict: surface regardless of allow_unresolved
					// — a name-only-use exemption must not mask
					// incompatible cross-narrowings.
					ael_err(ctx, ast_disp_offset(bp), bp->sz,
							"bin type conflict — incompatible constraints from earlier narrowings");
				}
				// The exemption is for a bin nothing reads. One whose value is
				// read needs a type, and the build refuses it without one --
				// said here, where the bin can be named and pointed at.
				else if (! bp->u.bin.allow_unresolved ||
						bin_value_is_read(ctx, bp)) {
					if (bp->etype == AST_ETYPE_AUTO_NUMERIC) {
						ael_err(ctx, ast_disp_offset(bp), bp->sz,
								"unresolved bin type, use $.bin:INT or $.bin:FLOAT");
					}
					else if (bp->etype == AST_ETYPE_AUTO_CDT) {
						ael_err(ctx, ast_disp_offset(bp), bp->sz,
								"unresolved bin type, use $.bin:LIST or $.bin:MAP");
					}
					else if (bp->etype == AST_ETYPE_AUTO_ADD) {
						ael_err(ctx, ast_disp_offset(bp), bp->sz,
								"unresolved bin type, use $.bin:STRING, $.bin:INT, or $.bin:FLOAT");
					}
					else {
						ael_err(ctx, ast_disp_offset(bp), bp->sz,
								"unresolved bin type, pin with $.bin:T");
					}
				}
			}

			br = bp->u.bin.bin_next;
		}
	}

	// Post-parse: a $.key() must resolve to a single concrete key type
	// (INT/STRING/BLOB) via inference or an explicit `:T` suffix. Silent
	// default types are disallowed (spec: type-inference.md), so a key
	// still carrying the multi-bit AUTO_KEY is an error, not a STR default.
	if (! ael_diag_has_error(&ctx->diags)) {
		for (ast_ref kr = ctx->key_meta_root; kr != AST_REF_NULL;) {
			ast_node* kp = ast_pool_at(pool, kr);

			if (! ast_type_resolved(kp->etype)) {
				ael_err(ctx, ast_disp_offset(kp), kp->sz,
						"unresolved key type, use $.key():INT, :STRING, or :BLOB");
			}

			kr = (ast_ref)kp->u.meta.param;
		}
	}

	// Types are final: run the rewrites that key on a resolved type — resolve
	// the polymorphic toInt() / toFloat() casts and lower every resolved-STR
	// `+` to the concat fold. Guarded on no-error so an operand or `+` chain
	// that failed to resolve (and errored above) is not rewritten against a
	// broken tree.
	if (! ael_diag_has_error(&ctx->diags)) {
		ael_post_infer_rewrite(ctx, ctx->root, 0);
	}

	// Last, so that a node with a diagnostic of its own -- a cast that names
	// the function it could not resolve -- gets to say the more useful thing
	// first. What reaches here owes the wire a type nothing else objected to,
	// and this is the last point that can still name the expression: an
	// emitter refusing the same tree has no source position to give.
	if (! ael_diag_has_error(&ctx->diags)) {
		ael_check_etype(ctx, ctx->root, 0);
	}
}

ael_parse_result
ael_parse_with_depth(ast_pool* pool, const char* input, uint32_t input_sz,
		uint8_t initial_filter_depth)
{
	ael_parse_result result = { .root = AST_REF_NULL };
	ael_context ctx = {
		.pool = pool,
		.input = input,
		.root = AST_REF_NULL,
		.cur_scope = AST_REF_NULL,
		.bin_root = AST_REF_NULL,
		.local_bin_root = AST_REF_NULL,
		.key_meta_root = AST_REF_NULL,
		.carried_create_owner = AST_REF_NULL,
		.filter_depth = initial_filter_depth,
		.at_root = AST_REF_NULL,
		.key_root = AST_REF_NULL,
	};

	ael_run_parser(&ctx, input_sz);
	ael_finalize_parse(&ctx);

	result.diags = ctx.diags;
	result.input = input;

	if (! ael_diag_has_error(&result.diags)) {
		result.root = ctx.root;
		result.bin_root = ctx.bin_root;
		result.local_bin_root = ctx.local_bin_root;
	}
	// On error, leave ctx.root for the caller's ast_pool_destroy (non-recursive).
	// A recursive ast_free here would overflow the C stack on a deep left-
	// associative chain, which YYSTACKDEPTH doesn't bound.

	return result;
}
