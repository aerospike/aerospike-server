/*
 * sindex.c
 *
 * Copyright (C) 2022-2023 Aerospike, Inc.
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

#include "sindex/sindex.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_ll.h"

#include "arenax.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"
#include "shash.h"
#include "vector.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/smd.h"
#include "base/thr_info.h"
#include "exp/exp.h"
#include "exp/exp_rt.h"
#include "geospatial/geospatial.h"
#include "sindex/gc.h"
#include "sindex/populate.h"
#include "sindex/sindex_tree.h"
#include "storage/storage.h"

#include "warnings.h"

//==========================================================
// Typedefs & constants.
//

#define TOK_CHAR_DELIMITER '|'

typedef struct exp_def_s {
	as_exp* exp; // built exp points to buf
	uint8_t* buf;
	int32_t buf_sz;
	cf_vector* bins_info;
} exp_def;

typedef bool (*add_ktype_from_msgpack_fn)(msgpack_in* val, as_sindex_bin* sbin);

#define CARDINALITY_PERIOD 3600

//==========================================================
// Globals.
//

//==========================================================
// Forward declarations.
//

static bool parse_exp(const char* exp_b64, exp_def* e_def_r);
static void free_exp_def(exp_def* e_def);

static uint32_t populate_sbins(as_namespace* ns, uint16_t set_id,
		const as_bin* b, as_sindex_bin* sbins, as_sindex_op op);
static uint32_t populate_sbin_si(as_sindex* si, const as_bin* b,
		as_sindex_bin* sbin, as_sindex_op op);

static bool sbin_from_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin);
static bool sbin_from_simple_bin(as_sindex* si, const as_bin* b,
		as_sindex_bin* sbin);
static bool sbin_from_cdt_bin(as_sindex* si, const as_bin* b,
		as_sindex_bin* sbin);

static void add_value_to_sbin(as_sindex_bin* sbin, int64_t val);

static bool add_listvalues_foreach(msgpack_in* element, void* udata);
static bool add_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata);
static bool add_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata);

static void add_long_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_string_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_blob_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_geojson_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);

static char const* ktype_str(as_particle_type ktype, bool use_integer);

static as_sindex* as_sindex_find_best(const as_namespace* ns, uint16_t set_id,
		const cf_vector* candidates, const char* hint_iname,
		as_exp_sindex_candidate* matched_r);
static as_sindex* find_matching_exp_sindex(const as_namespace* ns,
		uint16_t set_id, const as_exp_sindex_candidate* c,
		const char* hint_iname);
static bool subtree_equals_exp(const as_exp* cand_exp, const uint8_t* subtree_ptr,
		uint32_t start_ix, uint32_t end_ix, const as_exp* si_exp);
static bool compare_bufs(const uint8_t* buf1, uint32_t buf1_sz,
		const uint8_t* buf2, uint32_t buf2_sz);
const char* sindex_particle_type_str(as_particle_type type);
static as_particle_type itype_to_exp_particle_type(as_sindex_type itype);

static void* run_cardinality(void* udata);

//==========================================================
// Inlines & macros.
//

static inline void
add_keytype_from_msgpack(as_particle_type ktype, msgpack_in* element,
		as_sindex_bin* sbin)
{
	switch (ktype) {
	case AS_PARTICLE_TYPE_INTEGER:
		add_long_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_STRING:
		add_string_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_BLOB:
		add_blob_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		add_geojson_from_msgpack(element, sbin);
		break;
	default:
		break;
	}
}

static inline void
init_sbin(as_sindex_bin* sbin, as_sindex_op op, as_sindex* si)
{
	*sbin = (as_sindex_bin){ .si = si, .op = op };
}

static inline void
sbin_free(as_sindex_bin* sbin)
{
	if (sbin->values != NULL) {
		cf_free(sbin->values);
	}
}

//==========================================================
// Public API - startup.
//

void
as_sindex_init(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];
		as_sindex_gc_ns_init(ns);
	}
}

void
as_sindex_load(void)
{
	as_sindex_populate_startup();
}

void
as_sindex_start(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_thread_create_detached(as_sindex_run_gc, ns);
		cf_thread_create_detached(run_cardinality, ns);
	}
}

//==========================================================
// Public API - populate sindexes.
//

void
as_sindex_put_all_rd(as_namespace* ns, as_storage_rd* rd, as_index_ref* r_ref)
{
	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si != NULL && ! si->readable &&
				(si->set_id == INVALID_SET_ID ||
						si->set_id == as_index_get_set_id(rd->r))) {
			as_sindex_put_rd(si, rd, r_ref);
		}
	}
}

void
as_sindex_put_rd(as_sindex* si, as_storage_rd* rd, as_index_ref* r_ref)
{
	as_bin* b = NULL;
	as_bin rb;
	as_record* r = r_ref->r;

	if (si->exp == NULL) {
		b = as_bin_get_live(rd, si->bin_name);

		if (b == NULL) {
			return;
		}
	}
	else {
		uint16_t set_id = si->set_id;

		if (set_id != INVALID_SET_ID && set_id != as_index_get_set_id(r)) {
			return;
		}

		as_bin_set_empty(&rb);

		as_exp_ctx ctx_rd = { .ns = si->ns, .r = r, .rd = rd };

		// No eval-fault suppression needed here - only sindex populate calls
		// this, on builder threads that are never armed. (Contrast the write
		// path's eval_and_populate_sbin() in rw_utils.c, which runs armed.)
		as_exp_trilean rv = as_exp_eval(si->exp, &ctx_rd, &rb, NULL);

		if (rv != AS_EXP_TRUE) {
			return;
		}

		b = &rb;
	}

	as_sindex_bin sbin;

	init_sbin(&sbin, AS_SINDEX_OP_INSERT, si);

	if (sbin_from_bin(si, b, &sbin)) {
		// Mark record for sindex before insertion.
		as_index_set_in_sindex(r);

		as_sindex_update_by_sbin(&sbin, 1, r_ref->r_h);
		sbin_free(&sbin);
	}

	if (si->exp != NULL) {
		as_bin_particle_destroy(&rb);
	}
}

//==========================================================
// Public API - modify sindexes from writes/deletes.
//

uint32_t
as_sindex_populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbins,
		as_sindex_op op)
{
	return populate_sbin_si(si, b, sbins, op);
}

uint32_t
as_sindex_populate_sbins(as_namespace* ns, uint16_t set_id, const as_bin* b,
		as_sindex_bin* sbins, as_sindex_op op)
{
	if (as_bin_is_tombstone(b)) {
		return 0;
	}

	uint32_t n_populated = populate_sbins(ns, set_id, b, sbins, op);

	if (set_id != INVALID_SET_ID) {
		n_populated +=
				populate_sbins(ns, INVALID_SET_ID, b, sbins + n_populated, op);
	}

	return n_populated;
}

void
as_sindex_update_by_sbin(as_sindex_bin* sbins, uint32_t n_sbins,
		cf_arenax_handle r_h)
{
	// Deletes before inserts - a sindex key can recur with different op.

	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		if (sbin->op == AS_SINDEX_OP_DELETE) {
			for (uint32_t j = 0; j < sbin->n_values; j++) {
				int64_t bval = j == 0 ? sbin->val : sbin->values[j];

				as_sindex_tree_delete(sbin->si, bval, r_h);
			}
		}
	}

	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		if (sbin->op == AS_SINDEX_OP_INSERT) {
			for (uint32_t j = 0; j < sbin->n_values; j++) {
				int64_t bval = j == 0 ? sbin->val : sbin->values[j];

				as_sindex_tree_put(sbin->si, bval, r_h);
			}
		}
	}
}

void
as_sindex_sbin_free_all(as_sindex_bin* sbins, uint32_t n_sbins)
{
	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		as_sindex_release(sbin->si);
		sbin_free(sbin);
	}
}

//==========================================================
// Public API - lookup.
//

as_sindex*
as_sindex_lookup_by_defn(const as_namespace* ns, uint16_t set_id,
		const char* bin_name, as_particle_type ktype, as_sindex_type itype,
		const uint8_t* exp_buf, uint32_t exp_buf_sz, const uint8_t* ctx_buf,
		uint32_t ctx_buf_sz)
{
	SINDEX_GRLOCK();

	as_sindex* si = as_si_by_defn(ns, set_id, bin_name, ktype, itype, exp_buf,
			exp_buf_sz, ctx_buf, ctx_buf_sz);

	if (si == NULL && set_id != INVALID_SET_ID) {
		si = as_si_by_defn(ns, INVALID_SET_ID, bin_name, ktype, itype, exp_buf,
				exp_buf_sz, ctx_buf, ctx_buf_sz);
	}

	if (si == NULL || si->dropped) {
		SINDEX_GRUNLOCK();
		return NULL;
	}

	as_sindex_job_reserve(si);

	SINDEX_GRUNLOCK();

	return si;
}

as_sindex*
as_sindex_lookup_by_iname(const as_namespace* ns, const char* iname)
{
	SINDEX_GRLOCK();

	as_sindex* si = as_si_by_iname(ns, iname);

	if (si == NULL || si->dropped) {
		SINDEX_GRUNLOCK();
		return NULL;
	}

	as_sindex_job_reserve(si);

	SINDEX_GRUNLOCK();

	return si;
}

//==========================================================
// Local helpers - structural op-array equality (exp-based sindex matching).
//

// Short aliases for the op-node layouts, matching exp.c's local convention.
typedef struct exp_op_base_mem_s op_base_mem;
typedef struct exp_op_var_s op_var;
typedef struct exp_op_value_blob_s op_value_blob;
typedef struct exp_op_value_int_s op_value_int;
typedef struct exp_op_value_bool_s op_value_bool;
typedef struct exp_op_value_float_s op_value_float;
typedef struct exp_op_call_s op_call;

static bool
compare_bufs(const uint8_t* buf1, uint32_t buf1_sz, const uint8_t* buf2,
		uint32_t buf2_sz)
{
	if (buf1 == NULL && buf2 == NULL) {
		return true;
	}

	return buf1 != NULL && buf2 != NULL && buf1_sz == buf2_sz &&
			(buf1_sz == 0 || memcmp(buf1, buf2, buf1_sz) == 0);
}

static bool
call_vec_equal(const uint8_t* a_buf, uint32_t a_sz, const uint8_t* b_buf,
		uint32_t b_sz, bool is_leader, bool allow_trailing_default)
{
	msgpack_in ma = { .buf = a_buf, .buf_sz = a_sz };
	msgpack_in mb = { .buf = b_buf, .buf_sz = b_sz };

	if (is_leader) {
		uint32_t a_n = 0;
		uint32_t b_n = 0;

		// An absent segment is an empty list, not a decode failure - just
		// consumed here so the shared compare below can walk both sides as
		// plain element streams. This does NOT itself grant zero-default
		// forgiveness: that's gated by the caller's allow_trailing_default,
		// which is always false at the leader (i == 0) - a single-vec call's
		// own optional trailing arg (e.g. BIT_COUNT's size) can be
		// semantically different when omitted vs. explicit 0, unlike the
		// trailing write-flags scalar the n_vecs-level tolerance exists for.
		// See SingleVecLeaderTrailingZeroNeverForgiven in
		// test_query_plan_cdt.cc for the regression this guards against.
		// The list header's declared count is read but NOT trusted as a
		// bound: a mixed scalar/sub-expression call carves the
		// sub-expression out into a sibling op, leaving the header's
		// original full count stale against this buffer's truncated bytes -
		// so the true count is just "however many elements physically fit"
		// in buf_sz.
		if (a_sz != 0 && ! msgpack_get_list_ele_count(&ma, &a_n)) {
			// Not a list (shouldn't happen for a well-formed leader
			// segment) - fall back to the original exact byte compare.
			// Unverified by test: every leader this function has been
			// exercised against (parse_op_call's own output, on both the
			// query-plan and unit-test sides) is already a valid list, and
			// no construction via the public API has been found that
			// produces a malformed one here - likely genuinely
			// unreachable/defensive, same class as EXP_VOP_VALUE_MAP below.
			return a_sz == b_sz && (a_sz == 0 || memcmp(a_buf, b_buf, a_sz) == 0);
		}

		if (b_sz != 0 && ! msgpack_get_list_ele_count(&mb, &b_n)) {
			return a_sz == b_sz && (a_sz == 0 || memcmp(a_buf, b_buf, a_sz) == 0);
		}

		(void)a_n;
		(void)b_n;
	}

	// Compare by decoded value (msgpack_cmp), not raw bytes - two packers
	// can legally choose different integer widths for the same value
	// (fixint vs padded uint8), which a byte-exact compare would wrongly
	// reject, same false-negative class as the flags-omission case above.
	while (ma.offset < ma.buf_sz && mb.offset < mb.buf_sz) {
		if (msgpack_cmp(&ma, &mb) != MSGPACK_CMP_EQUAL) {
			return false;
		}
	}

	// Extra trailing elements are only forgivable at the call's true final
	// slot - elsewhere, leftover content means the sides disagree on this
	// argument, not that one omitted a default.
	if (ma.offset == ma.buf_sz && mb.offset == mb.buf_sz) {
		return true;
	}

	if (! allow_trailing_default) {
		return false;
	}

	msgpack_in* longer = ma.offset < ma.buf_sz ? &ma : &mb;

	while (longer->offset < longer->buf_sz) {
		uint64_t v;

		if (! msgpack_get_uint64(longer, &v) || v != 0) {
			return false;
		}
	}

	return true;
}

// Debug-only: hex-dumps the mismatching vecs[] slot for node_self_equal's
// EXP_CALL case. Split out so the comparator itself stays focused on the
// comparison logic - this is purely diagnostic formatting on the failure path.
static void
log_call_vec_mismatch(uint32_t i, const uint8_t* a_buf, uint32_t a_sz,
		const uint8_t* b_buf, uint32_t b_sz, const op_call* ca, const op_call* cb)
{
	if (! cf_log_check_level(AS_SINDEX, CF_DEBUG)) {
		return;
	}

	char ahex[128] = { 0 };
	char bhex[128] = { 0 };

	for (uint32_t k = 0; k < a_sz && k < 20; k++) {
		snprintf(ahex + strlen(ahex), sizeof(ahex) - strlen(ahex), "%02x ",
				a_buf[k]);
	}

	for (uint32_t k = 0; k < b_sz && k < 20; k++) {
		snprintf(bhex + strlen(bhex), sizeof(bhex) - strlen(bhex), "%02x ",
				b_buf[k]);
	}

	cf_debug(AS_SINDEX,
			"node_self_equal: EXP_CALL vecs[%u] hex a=[%s] b=[%s] "
			"n_vecs a=%u b=%u eval_count a=%u b=%u",
			i, ahex, bhex, ca->n_vecs, cb->n_vecs, ca->eval_count,
			cb->eval_count);
	cf_debug(AS_SINDEX,
			"node_self_equal: EXP_CALL vecs[%u] mismatch (a_sz=%u b_sz=%u)", i,
			a_sz, b_sz);
}

// Compares one op node's own payload (not children) - opcode is assumed
// already equal by the caller. Bin references resolve by NAME via each
// exp's own bin-name table, never by index - the two op arrays being
// compared were independently built (filter vs. sindex-create), so table
// slot order can differ even for the identical bin.
static bool
node_self_equal(const as_exp* a_exp, const op_base_mem* na, const as_exp* b_exp,
		const op_base_mem* nb)
{
	if (na->type != nb->type || na->rtype != nb->rtype) {
		cf_debug(AS_SINDEX,
				"node_self_equal: base tag mismatch code=%u type a=%d b=%d rtype a=%d b=%d",
				na->code, na->type, nb->type, na->rtype, nb->rtype);
		return false;
	}

	switch (na->code) {
	case EXP_BIN: {
		const op_var* va = (const op_var*)na;
		const op_var* vb = (const op_var*)nb;
		const exp_rt_bin_table* ta = (const exp_rt_bin_table*)a_exp->bin_table;
		const exp_rt_bin_table* tb = (const exp_rt_bin_table*)b_exp->bin_table;

		if (va->idx >= ta->n_bins || vb->idx >= tb->n_bins) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_BIN idx out of range a=%u/%u b=%u/%u",
					va->idx, ta->n_bins, vb->idx, tb->n_bins);
			return false;
		}

		const exp_bin_name_entry* ea = &ta->table[va->idx];
		const exp_bin_name_entry* eb = &tb->table[vb->idx];

		bool eq = ea->sz == eb->sz &&
				memcmp(ta->base + ea->off, tb->base + eb->off, ea->sz) == 0;

		if (! eq) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_BIN name mismatch a='%.*s' b='%.*s'",
					(int)ea->sz, ta->base + ea->off, (int)eb->sz,
					tb->base + eb->off);
		}

		return eq;
	}
	case EXP_VAR:
		// Only reachable inside a let-binding - not a construct parse_exp's
		// validation allows into a sindex expression, but compare by idx
		// for completeness/safety rather than silently matching.
		if (((const op_var*)na)->idx != ((const op_var*)nb)->idx) {
			cf_debug(AS_SINDEX, "node_self_equal: EXP_VAR idx mismatch a=%u b=%u",
					((const op_var*)na)->idx, ((const op_var*)nb)->idx);
			return false;
		}
		return true;
	case EXP_VOP_VALUE_INT:
		if (((const op_value_int*)na)->value != ((const op_value_int*)nb)->value) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_VOP_VALUE_INT mismatch a=%ld b=%ld",
					(long)((const op_value_int*)na)->value,
					(long)((const op_value_int*)nb)->value);
			return false;
		}
		return true;
	case EXP_QUOTE:
	case EXP_VOP_VALUE_STR:
	case EXP_VOP_VALUE_BLOB:
	case EXP_VOP_VALUE_LIST:
	case EXP_VOP_VALUE_MSGPACK: {
		// Quoted list/map literals fold at build time into EXP_VOP_VALUE_LIST/
		// _MSGPACK, not EXP_QUOTE - same op_value_blob pointer+size layout as
		// STR/BLOB. Without this case they hit the default memcmp, comparing
		// heap pointers that never match across independently-built trees.
		//
		// This case IS reachable, but only for LIST-shaped (or STR/BLOB)
		// nodes structurally embedded inside a matched is_exp subtree - never
		// for a comparison whose own literal is MAP-typed:
		// extract_literal_bound (query_plan_candidates.c) only ever produces
		// INTEGER/STRING/BLOB
		// literals, so append_exp_candidate (the sole is_exp producer) can't
		// emit a candidate for a MAP-valued comparison in the first place.
		//
		// EXP_VOP_VALUE_MAP is deliberately NOT cased here even though it's
		// the map-typed sibling of the two above: build_value_msgpack
		// (exp.c) keeps op->code == EXP_VOP_VALUE_MSGPACK even when the
		// peeked wire type is MSGPACK_TYPE_MAP ("no VALUE_MAP runtime arm"),
		// so EXP_VOP_VALUE_MAP is never actually produced as a runtime
		// op->code (no construction was found that reaches it either way).
		// And even if that ever changed: unlike LIST/MSGPACK,
		// EXP_VOP_VALUE_MAP's own op_table entry (exp.c) types it as plain
		// op_base_mem, not op_value_blob - it carries no pointer/heap
		// member, so falling to the default memcmp below would compare only
		// the shared op_base_mem header and produce a harmless, always-true
		// (empty) comparison rather than resurrecting the LIST/MSGPACK
		// pointer-comparison bug this case exists to fix.
		const op_value_blob* ba = (const op_value_blob*)na;
		const op_value_blob* bb = (const op_value_blob*)nb;

		bool eq = ba->value_sz == bb->value_sz &&
				(ba->value_sz == 0 ||
						memcmp(ba->value, bb->value, ba->value_sz) == 0);

		if (! eq) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_VOP_VALUE_STR/BLOB mismatch code=%u a_sz=%u b_sz=%u",
					na->code, ba->value_sz, bb->value_sz);
		}

		return eq;
	}
	case EXP_VOP_VALUE_BOOL:
		if (((const op_value_bool*)na)->value !=
				((const op_value_bool*)nb)->value) {
			cf_debug(AS_SINDEX, "node_self_equal: EXP_VOP_VALUE_BOOL mismatch");
			return false;
		}
		return true;
	case EXP_VOP_VALUE_FLOAT:
		if (((const op_value_float*)na)->value !=
				((const op_value_float*)nb)->value) {
			cf_debug(AS_SINDEX, "node_self_equal: EXP_VOP_VALUE_FLOAT mismatch");
			return false;
		}
		return true;
	case EXP_VOP_VALUE_GEO: {
		// Compare the original GeoJSON source bytes (contents/content_sz),
		// not exp_geo_compiled - that union's cellid/region is derived at
		// compile time and, for region, holds a heap pointer, so a raw
		// memcmp of the node (the default: case below) would compare
		// pointer/derived bytes rather than the literal's actual value.
		//
		// Defensive, not currently reachable from the sindex-candidate path:
		// geo literals are built exclusively through append_geo_candidate /
		// extract_geo_candidate (query_plan_candidates.c), a dedicated path
		// that never produces an is_exp candidate - only is_exp candidates
		// (built by append_exp_candidate, whose literal always came through
		// extract_literal_bound, itself restricted to INTEGER/STRING/BLOB)
		// ever reach subtree_equals_exp -> node_self_equal. If geo-in-
		// expression indexing is ever added, that producer is the missing
		// piece, not this comparison.
		const exp_op_value_geo* ga = (const exp_op_value_geo*)na;
		const exp_op_value_geo* gb = (const exp_op_value_geo*)nb;

		bool eq = ga->content_sz == gb->content_sz &&
				(ga->content_sz == 0 ||
						memcmp(ga->contents, gb->contents, ga->content_sz) == 0);

		if (! eq) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_VOP_VALUE_GEO mismatch a_sz=%u b_sz=%u",
					ga->content_sz, gb->content_sz);
		}

		return eq;
	}
	case EXP_CALL: {
		const op_call* ca = (const op_call*)na;
		const op_call* cb = (const op_call*)nb;

		if (ca->system_type != cb->system_type || ca->type != cb->type) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_CALL header mismatch "
					"system_type a=%d b=%d, type a=%d b=%d",
					ca->system_type, cb->system_type, ca->type, cb->type);
			return false;
		}

		// n_vecs need not match exactly - wire-built calls pack a trailing
		// write-flags scalar even at default, AEL's compiled form omits it;
		// a byte-exact compare wrongly rejected this, silently breaking
		// STRING/BITS/HLL sindex matching. eval_count must still match
		// exactly - a differing sub-expression argument count is a
		// genuinely different call shape.
		if (ca->eval_count != cb->eval_count) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_CALL eval_count mismatch a=%u b=%u",
					ca->eval_count, cb->eval_count);
			return false;
		}

		uint32_t n_vecs_max = ca->n_vecs > cb->n_vecs ? ca->n_vecs : cb->n_vecs;

		for (uint32_t i = 0; i < n_vecs_max; i++) {
			bool a_is_eval = i < ca->n_vecs &&
					ca->vecs[i].buf == exp_call_eval_token;
			bool b_is_eval = i < cb->n_vecs &&
					cb->vecs[i].buf == exp_call_eval_token;

			// Which vecs[] slot holds the eval-token distinguishes same-call
			// encodings from calls with scalar/sub-expression args swapped
			// (bitCount(0, $.n) vs bitCount($.n, 0)) - a mismatch here means
			// a genuinely different argument shape, so no tolerance applies.
			if (a_is_eval != b_is_eval) {
				cf_debug(AS_SINDEX,
						"node_self_equal: EXP_CALL eval-token position mismatch "
						"at vecs[%u] a=%d b=%d",
						i, a_is_eval, b_is_eval);
				return false;
			}

			const uint8_t* a_buf = i < ca->n_vecs ? ca->vecs[i].buf : NULL;
			uint32_t a_sz = i < ca->n_vecs ? ca->vecs[i].buf_sz : 0;
			const uint8_t* b_buf = i < cb->n_vecs ? cb->vecs[i].buf : NULL;
			uint32_t b_sz = i < cb->n_vecs ? cb->vecs[i].buf_sz : 0;

			// Trailing-default (0) tolerance applies ONLY at the call's true
			// final slot, and never at the leader (i == 0): a single-vec
			// call has no separate trailing slot to forgive, so its
			// trailing elements are the call's own operands, not omittable
			// defaults - a real argument can legitimately be 0 too, so
			// forgiving it elsewhere would let a genuinely different value
			// slip through unchecked, same failure class as above.
			if (! call_vec_equal(a_buf, a_sz, b_buf, b_sz, i == 0,
						i > 0 && i == n_vecs_max - 1)) {
				log_call_vec_mismatch(i, a_buf, a_sz, b_buf, b_sz, ca, cb);
				return false;
			}
		}

		return true;
	}
	case EXP_CMP_REGEX: {
		// op_cmp_regex carries a compiled regex_t (opaque, heap-backed) -
		// byte-comparing it would compare internal pointers/bytecode, not
		// the pattern, so only flags/pattern length are checked here (the
		// pattern text itself is a child literal, matched by the caller).
		const exp_op_cmp_regex* ra = (const exp_op_cmp_regex*)na;
		const exp_op_cmp_regex* rb = (const exp_op_cmp_regex*)nb;

		if (ra->flags != rb->flags || ra->regex_str_sz != rb->regex_str_sz) {
			cf_debug(AS_SINDEX,
					"node_self_equal: EXP_CMP_REGEX mismatch flags a=%d b=%d sz a=%u b=%u",
					ra->flags, rb->flags, ra->regex_str_sz, rb->regex_str_sz);
			return false;
		}

		return true;
	}
	default: {
		// Ops not cased above carry inline immediate data (e.g. digestModulo's
		// `mod`) that used to go unchecked here, letting digestModulo(4) and
		// digestModulo(8) compare as equal - memcmp everything past the common
		// header. INVARIANT: only correct because no op reaching this default
		// holds a pointer/heap member; any new one that does needs its own
		// case above, or it'll spuriously mismatch by pointer value and
		// silently disable exp-sindex matching for that op.
		uint32_t sz = exp_op_table[na->code].size;

		if (sz > sizeof(op_base_mem) &&
				memcmp((const uint8_t*)na + sizeof(op_base_mem),
						(const uint8_t*)nb + sizeof(op_base_mem),
						sz - sizeof(op_base_mem)) != 0) {
			cf_debug(AS_SINDEX,
					"node_self_equal: immediate payload mismatch code=%u sz=%u",
					na->code, sz);
			return false;
		}

		return true;
	}
	}
}

static bool
subtree_equals_exp_range(const as_exp* a_exp, const uint8_t* a_ptr,
		uint32_t a_ix, uint32_t a_end_ix, const as_exp* b_exp,
		const uint8_t* b_ptr, uint32_t b_ix, uint32_t b_end_ix)
{
	while (a_ix < a_end_ix && b_ix < b_end_ix) {
		const op_base_mem* na = (const op_base_mem*)a_ptr;
		const op_base_mem* nb = (const op_base_mem*)b_ptr;

		if (na->code != nb->code) {
			cf_debug(AS_SINDEX,
					"subtree_equals_exp_range: opcode mismatch at a_ix=%u (code=%u) b_ix=%u (code=%u)",
					a_ix, na->code, b_ix, nb->code);
			return false;
		}

		if (! node_self_equal(a_exp, na, b_exp, nb)) {
			cf_debug(AS_SINDEX,
					"subtree_equals_exp_range: node_self_equal false at a_ix=%u b_ix=%u code=%u",
					a_ix, b_ix, na->code);
			return false;
		}

		a_ptr += exp_op_table[na->code].size;
		a_ix++;
		b_ptr += exp_op_table[nb->code].size;
		b_ix++;
	}

	if (a_ix != a_end_ix || b_ix != b_end_ix) {
		cf_debug(AS_SINDEX,
				"subtree_equals_exp_range: length mismatch a_ix=%u a_end_ix=%u b_ix=%u b_end_ix=%u",
				a_ix, a_end_ix, b_ix, b_end_ix);
		return false;
	}

	return true;
}

// Structural equality between a candidate's LHS subtree (in the filter's own
// op array, [start_ix, end_ix) ) and an exp-based sindex's root expression
// (si_exp, its own independently-built op array). The subtree's byte pointer
// was already resolved once, at candidate-creation time (op sizes are
// opcode-dependent, so there is no O(1) index -> pointer mapping) - callers
// pass it in rather than re-walking cand_exp from its root on every sindex
// tried, since this runs once per (candidate, exp-sindex) pair under
// SINDEX_GRLOCK.
static bool
subtree_equals_exp(const as_exp* cand_exp, const uint8_t* subtree_ptr,
		uint32_t start_ix, uint32_t end_ix, const as_exp* si_exp)
{
	if (cand_exp == NULL || si_exp == NULL || subtree_ptr == NULL) {
		cf_debug(AS_SINDEX,
				"subtree_equals_exp: null exp (cand_exp=%p si_exp=%p subtree_ptr=%p)",
				(void*)cand_exp, (void*)si_exp, (const void*)subtree_ptr);
		return false;
	}

	const op_base_mem* si_root = (const op_base_mem*)si_exp->mem;

	return subtree_equals_exp_range(cand_exp, subtree_ptr, start_ix, end_ix,
			si_exp, si_exp->mem, 0, si_root->instr_end_ix);
}

//==========================================================
// Local helpers - sindex selection from expression candidates.
//

// Scans only the given scope (an exact set_id, never a set_id-or-namespace-
// wide mix) so callers can run the set-scoped pass before the namespace-wide
// fallback pass, mirroring as_si_by_defn's two-pass preference below. Reports
// via hint_matched_r whether the returned match is the requested hint, so the
// caller knows whether it's still worth searching the other scope.
static as_sindex*
find_matching_exp_sindex_in_scope(const as_namespace* ns, uint16_t scope_set_id,
		const as_exp_sindex_candidate* c, const char* hint_iname,
		bool* hint_matched_r)
{
	as_sindex* match = NULL;

	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si == NULL || si->exp == NULL || si->dropped || ! si->readable) {
			continue;
		}

		if (si->set_id != scope_set_id) {
			continue;
		}

		if (si->ktype != c->ktype || si->itype != c->itype) {
			continue;
		}

		const uint8_t* ctx_buf = c->ctx_buf_sz != 0 ? c->ctx_buf : NULL;

		if (! compare_bufs(si->ctx_buf, si->ctx_buf_sz, ctx_buf, c->ctx_buf_sz)) {
			continue;
		}

		if (! subtree_equals_exp(c->owner_exp, c->exp_subtree_ptr,
					c->exp_subtree_start_ix, c->exp_subtree_end_ix, si->exp)) {
			continue;
		}

		bool is_hint = hint_iname != NULL && strcmp(si->iname, hint_iname) == 0;

		if (match == NULL || is_hint) {
			match = si;
		}

		if (is_hint) {
			*hint_matched_r = true;
			break;
		}

		if (hint_iname == NULL) {
			break;
		}
	}

	return match;
}

// Prefers a set-scoped structural match over a namespace-wide one, same as
// the non-exp as_si_by_defn(set_id, ...) / as_si_by_defn(INVALID_SET_ID, ...)
// fallback below - previously this resolved by ns->sindexes[] slot order
// alone, so a namespace-wide index could win over an equally-valid
// set-scoped one purely by chance of creation order.
static as_sindex*
find_matching_exp_sindex(const as_namespace* ns, uint16_t set_id,
		const as_exp_sindex_candidate* c, const char* hint_iname)
{
	bool hint_matched = false;
	as_sindex* si = find_matching_exp_sindex_in_scope(ns, set_id, c, hint_iname,
			&hint_matched);

	// Skip the namespace-wide pass when set_id already was INVALID_SET_ID
	// (repeating the same scope), or when pass 1 found a match and there's
	// no hint left that could still resolve differently in the other scope.
	if (set_id != INVALID_SET_ID &&
			(si == NULL || (hint_iname != NULL && ! hint_matched))) {
		bool fallback_hint_matched = false;
		as_sindex* fallback_si = find_matching_exp_sindex_in_scope(ns,
				INVALID_SET_ID, c, hint_iname, &fallback_hint_matched);

		if (fallback_hint_matched || si == NULL) {
			si = fallback_si;
		}
	}

	return si;
}

static as_sindex*
as_sindex_find_best(const as_namespace* ns, uint16_t set_id,
		const cf_vector* candidates, const char* hint_iname,
		as_exp_sindex_candidate* matched_r)
{
	as_sindex* best_si = NULL;
	uint64_t best_keys_per_bval = UINT64_MAX;
	uint64_t best_n_keys = UINT64_MAX;
	as_exp_sindex_candidate best_match = { 0 };

	uint32_t n_candidates = cf_vector_size(candidates);

	SINDEX_GRLOCK();

	for (uint32_t i = 0; i < n_candidates; i++) {
		as_exp_sindex_candidate c;

		cf_vector_get(candidates, i, &c);

		as_sindex* si;

		if (c.is_exp) {
			// Expression-based candidates can't hash into the bin/exp defn
			// hash (that needs a real exp_buf, which planning never has -
			// only the built op-array subtree) - enumerate the set's
			// expression-based sindexes directly and match structurally.
			si = find_matching_exp_sindex(ns, set_id, &c, hint_iname);

			if (si == NULL) {
				cf_debug(AS_SINDEX,
						"{%s} query-plan: skip exp-based candidate ktype=%d (no structural match)",
						ns->name, c.ktype);
				continue; // hint naming a real but non-matching exp-based si
						// falls through here too - never silently accepted
			}
		}
		else {
			// itype and ctx come from the candidate so collection sindexes
			// (LIST / MAPKEYS / MAPVALUES) and CDT-context sindexes can be
			// matched. Top-level scalar candidates emit
			// AS_SINDEX_ITYPE_DEFAULT with no ctx, which keeps prior
			// behaviour identical.
			const uint8_t* ctx_buf = c.ctx_buf_sz != 0 ? c.ctx_buf : NULL;

			si = as_si_by_defn(ns, set_id, c.bin_name, c.ktype, c.itype, NULL,
					0, ctx_buf, c.ctx_buf_sz);

			if (si == NULL && set_id != INVALID_SET_ID) {
				si = as_si_by_defn(ns, INVALID_SET_ID, c.bin_name, c.ktype,
						c.itype, NULL, 0, ctx_buf, c.ctx_buf_sz);
			}

			if (si == NULL || si->dropped || ! si->readable) {
				cf_debug(AS_SINDEX,
						"{%s} query-plan: skip candidate bin=%s ktype=%d (no/dropped/unreadable si)",
						ns->name, c.bin_name, c.ktype);
				continue;
			}
		}

		// If the hint names this sindex exactly, prefer it over a higher-scoring one.
		if (hint_iname != NULL && strcmp(si->iname, hint_iname) == 0) {
			best_si = si;
			best_match = c;
			break;
		}

		uint64_t n_keys = as_sindex_tree_n_keys(si);

		// Lower keys_per_bval = more selective, so it's our cost metric.
		// n_keys == 0 means genuinely empty (live count), making this
		// AND-conjunction provably empty - the cheapest correct pick.
		// keys_per_bval == 0 otherwise just means no stats yet, so demote
		// it to worst score instead.
		uint64_t keys_per_bval = n_keys == 0 ? 0
				: si->keys_per_bval != 0	 ? si->keys_per_bval
											 : UINT64_MAX;

		if (keys_per_bval < best_keys_per_bval ||
				(keys_per_bval == best_keys_per_bval && n_keys < best_n_keys)) {
			best_si = si;
			best_keys_per_bval = keys_per_bval;
			best_n_keys = n_keys;
			best_match = c;
		}
	}

	if (best_si != NULL) {
		as_sindex_job_reserve(best_si);
		*matched_r = best_match;

		if (hint_iname == NULL || strcmp(best_si->iname, hint_iname) != 0) {
			cf_debug(AS_SINDEX,
					"{%s} query-plan: cost pick iname=%s bin=%s kpb=%" PRIu64
					" n_keys=%" PRIu64 " from %u candidates",
					ns->name, best_si->iname, best_match.bin_name,
					best_keys_per_bval, best_n_keys, n_candidates);
		}
		else {
			cf_debug(AS_SINDEX,
					"{%s} query-plan: hint pick iname=%s bin=%s from %u candidates",
					ns->name, best_si->iname, best_match.bin_name, n_candidates);
		}
	}

	SINDEX_GRUNLOCK();

	return best_si;
}

static as_query_plan_result
plan_result_from_extract(as_exp_sindex_extract_result extract_result)
{
	switch (extract_result) {
	case AS_EXP_SINDEX_EXTRACT_PI:
		return AS_QUERY_PLAN_PI;
	case AS_EXP_SINDEX_EXTRACT_FILTERED_OUT:
		return AS_QUERY_PLAN_FILTERED_OUT;
	case AS_EXP_SINDEX_EXTRACT_ERROR:
		return AS_QUERY_PLAN_ERROR;
	default:
		cf_crash(AS_SINDEX, "unexpected extract result %d", extract_result);
	}
}

//==========================================================
// Public API - sindex selection from expression candidates.
//

as_sindex_selection
as_sindex_select_from_exp(const as_namespace* ns, uint16_t set_id,
		const as_exp* exp, const char* hint_iname)
{
	as_sindex_selection sel = {
		.si = NULL,
	};

	cf_vector_define(candidates, sizeof(as_exp_sindex_candidate), 4, 0);

	as_exp_sindex_extract_result extract_result =
			as_exp_get_sindex_candidates(exp, &candidates);

	if (extract_result != AS_EXP_SINDEX_EXTRACT_CANDIDATES) {
		sel.result = plan_result_from_extract(extract_result);
		cf_vector_destroy(&candidates);
		return sel;
	}

	sel.si = as_sindex_find_best(ns, set_id, &candidates, hint_iname, &sel.match);

	if (sel.si == NULL) {
		sel.result = AS_QUERY_PLAN_PI;
		cf_debug(AS_SINDEX,
				"{%s} query-plan: fallback to PI - no good sindex found",
				ns->name);
	}
	else {
		sel.result = AS_QUERY_PLAN_SINDEX;
	}

	cf_vector_destroy(&candidates);

	return sel;
}

//==========================================================
// Public API - info & stats.
//

as_particle_type
as_sindex_ktype_from_string(const char* ktype_str)
{
	if (strcasecmp(ktype_str, "integer") == 0) {
		return AS_PARTICLE_TYPE_INTEGER;
	}

	if (strcasecmp(ktype_str, "numeric") == 0) {
		as_info_warn_deprecated(
				"sindex ktype 'numeric' is deprecated - use 'integer' instead");
		return AS_PARTICLE_TYPE_INTEGER;
	}

	if (strcasecmp(ktype_str, "string") == 0) {
		return AS_PARTICLE_TYPE_STRING;
	}

	if (strcasecmp(ktype_str, "blob") == 0) {
		return AS_PARTICLE_TYPE_BLOB;
	}

	if (strcasecmp(ktype_str, "geo2dsphere") == 0) {
		return AS_PARTICLE_TYPE_GEOJSON;
	}

	cf_warning(AS_SINDEX, "invalid key type %s", ktype_str);

	return AS_PARTICLE_TYPE_BAD;
}

as_sindex_type
as_sindex_itype_from_string(const char* itype_str)
{
	if (strcasecmp(itype_str, as_sindex_type_names[AS_SINDEX_ITYPE_DEFAULT]) ==
			0) {
		return AS_SINDEX_ITYPE_DEFAULT;
	}

	if (strcasecmp(itype_str, as_sindex_type_names[AS_SINDEX_ITYPE_LIST]) == 0) {
		return AS_SINDEX_ITYPE_LIST;
	}

	if (strcasecmp(itype_str, as_sindex_type_names[AS_SINDEX_ITYPE_MAPKEYS]) ==
			0) {
		return AS_SINDEX_ITYPE_MAPKEYS;
	}

	if (strcasecmp(itype_str, as_sindex_type_names[AS_SINDEX_ITYPE_MAPVALUES]) ==
			0) {
		return AS_SINDEX_ITYPE_MAPVALUES;
	}

	if (strcasecmp(itype_str, as_sindex_type_names[AS_SINDEX_ITYPE_SET]) == 0) {
		return AS_SINDEX_ITYPE_SET;
	}

	return AS_SINDEX_N_ITYPES;
}

void
as_sindex_list_str(const as_namespace* ns, bool b64, bool use_integer,
		cf_dyn_buf* db)
{
	SINDEX_GRLOCK();

	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si == NULL) {
			continue;
		}

		cf_dyn_buf_append_string(db, "ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_string(db, ":indexname=");
		cf_dyn_buf_append_string(db, si->iname);
		cf_dyn_buf_append_string(db, ":set=");
		cf_dyn_buf_append_string(db,
				si->set_name[0] != '\0' ? si->set_name : "null");
		cf_dyn_buf_append_string(db, ":bin=");
		cf_dyn_buf_append_string(db,
				si->bin_name[0] != '\0' ? si->bin_name : "null");
		cf_dyn_buf_append_string(db, ":type=");
		cf_dyn_buf_append_string(db, ktype_str(si->ktype, use_integer));
		cf_dyn_buf_append_string(db, ":indextype=");
		cf_dyn_buf_append_string(db, as_sindex_type_names[si->itype]);
		cf_dyn_buf_append_string(db, ":context=");

		if (si->ctx_buf == NULL) {
			cf_dyn_buf_append_string(db, "null");
		}
		else {
			if (b64) {
				cf_dyn_buf_append_string(db, si->ctx_b64);
			}
			else {
				cdt_ctx_to_dynbuf(si->ctx_buf, si->ctx_buf_sz, db);
			}
		}

		cf_dyn_buf_append_string(db, ":exp=");

		if (si->exp == NULL) {
			cf_dyn_buf_append_string(db, "null");
		}
		else {
			if (b64) {
				cf_dyn_buf_append_string(db, si->exp_b64);
			}
			else {
				as_exp_display(si->exp, db);
			}
		}

		if (si->error) {
			cf_dyn_buf_append_string(db, ":state=ERROR");
		}
		else if (si->readable) {
			cf_dyn_buf_append_string(db, ":state=RW");
		}
		else {
			cf_dyn_buf_append_string(db, ":state=WO");
		}

		cf_dyn_buf_append_char(db, ';');
	}

	SINDEX_GRUNLOCK();
}

bool
as_sindex_stats_str(const as_sindex* si, cf_dyn_buf* db)
{
	info_append_uint64(db, "entries", as_sindex_tree_n_keys(si));
	info_append_uint64(db, "used_bytes", as_sindex_tree_mem_size(si));
	info_append_uint64(db, "entries_per_bval", si->keys_per_bval);
	info_append_uint64(db, "entries_per_rec", si->keys_per_rec);
	info_append_uint32(db, "load_pct", si->populate_pct);
	info_append_uint64(db, "load_time", si->load_time);
	info_append_uint64(db, "stat_gc_recs", si->n_gc_cleaned);

	cf_dyn_buf_chomp(db);

	return true;
}

int32_t
as_sindex_cdt_ctx_b64_decode(const char* ctx_b64, uint32_t ctx_b64_len,
		uint8_t** buf_r)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(ctx_b64_len);
	uint32_t buf_sz_out;
	uint8_t* buf;

	buf = cf_malloc(buf_sz);

	if (! cf_b64_validate_and_decode(ctx_b64, ctx_b64_len, buf, &buf_sz_out)) {
		cf_free(buf);
		return -1;
	}

	msgpack_vec vecs[1];
	msgpack_in_vec mv = { .n_vecs = 1, .vecs = vecs };

	vecs[0].buf = buf;
	vecs[0].buf_sz = buf_sz_out;
	vecs[0].offset = 0;

	if (! cdt_context_read_check_peek(&mv)) {
		cf_free(buf);
		return -2;
	}

	bool was_modified;

	buf_sz = msgpack_compactify(buf, (uint32_t)buf_sz, &was_modified);

	if (buf_sz == 0) {
		cf_free(buf);
		return -2;
	}

	if (was_modified) {
		cf_free(buf);
		return -3;
	}

	*buf_r = buf;

	return (int32_t)buf_sz_out;
}

int32_t
as_sindex_exp_b64_decode(const char* exp_b64, uint32_t exp_b64_len,
		uint8_t** buf_r)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(exp_b64_len);
	uint8_t* buf;
	uint32_t buf_sz_out;

	buf = cf_malloc(buf_sz);

	if (! cf_b64_validate_and_decode(exp_b64, exp_b64_len, buf, &buf_sz_out)) {
		cf_free(buf);
		return -1;
	}

	*buf_r = buf;

	return (int32_t)buf_sz_out;
}

bool
as_sindex_validate_exp(const char* exp_b64, uint8_t* exp_type_r, cf_dyn_buf* db)
{
	exp_def e_def = { 0 };

	if (! parse_exp(exp_b64, &e_def)) {
		as_info_respond_error(db, AS_ERR_PARAMETER, "bad 'exp'");
		return false;
	}

	*exp_type_r = e_def.exp->expected_type;

	free_exp_def(&e_def);

	return true;
}

bool
as_sindex_validate_exp_type(const char* iname, as_sindex_type itype,
		as_particle_type ktype, uint8_t exp_type, cf_dyn_buf* db)
{
	uint8_t expected_type = ktype;

	if (itype != AS_SINDEX_ITYPE_DEFAULT) {
		expected_type = itype_to_exp_particle_type(itype);
	}

	if (exp_type != expected_type) {
		if (db != NULL) {
			as_info_respond_error(db, AS_ERR_PARAMETER,
					"bad 'exp' - expression type '%s' does not match expected type '%s'",
					sindex_particle_type_str(exp_type),
					sindex_particle_type_str(expected_type));
		}

		cf_warning(AS_SINDEX,
				"sindex-create %s: bad 'exp' - expression type '%s' does not match expected type '%s'",
				iname, sindex_particle_type_str(exp_type),
				sindex_particle_type_str(expected_type));
		return false;
	}

	return true;
}

//==========================================================
// Local helpers - create, delete, rename sindexes.
//

void
smd_sindex_create(as_sindex_def* def, bool startup)
{
	SINDEX_GWLOCK();

	as_namespace* ns = def->ns;
	as_sindex* cur_si = as_si_by_iname(ns, def->iname);

	if (cur_si != NULL) {
		cf_warning(AS_SINDEX,
				"SINDEX CREATE: iname already in use - ignoring %s", def->iname);
		SINDEX_GWUNLOCK();
		return;
	}

	as_set* p_set = NULL;
	uint16_t set_id = INVALID_SET_ID;

	if (def->set_name[0] != '\0' &&
			as_namespace_get_create_set_w_len(ns, def->set_name,
					strlen(def->set_name), &p_set, &set_id) != 0) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: failed get-create set %s",
				def->set_name);
		SINDEX_GWUNLOCK();
		return;
	}

	uint8_t* ctx_buf = NULL;
	int32_t ctx_buf_sz = 0;
	exp_def e_def = { 0 };

	if (def->ctx_b64 != NULL) {
		ctx_buf_sz = as_sindex_cdt_ctx_b64_decode(def->ctx_b64,
				(uint32_t)strlen(def->ctx_b64), &ctx_buf);

		if (ctx_buf_sz < 0) {
			cf_warning(AS_SINDEX,
					"SINDEX CREATE: invalid cdt context decode result %d",
					ctx_buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}
	else if (def->exp_b64 != NULL) {
		if (! parse_exp(def->exp_b64, &e_def)) {
			SINDEX_GWUNLOCK();
			return;
		}

		if (! as_sindex_validate_exp_type(def->iname, def->itype, def->ktype,
					e_def.exp->expected_type, NULL)) {
			free_exp_def(&e_def);
			SINDEX_GWUNLOCK();
			return;
		}
	}

	if ((cur_si = as_si_by_defn(ns, set_id, def->bin_name, def->ktype,
				 def->itype, e_def.buf, (uint32_t)e_def.buf_sz, ctx_buf,
				 (uint32_t)ctx_buf_sz)) != NULL) {
		cf_info(AS_SINDEX, "SINDEX CREATE: renaming %s to %s", cur_si->iname,
				def->iname);
		as_rename_sindex(cur_si, def->iname);
		SINDEX_GWUNLOCK();

		if (ctx_buf != NULL) {
			cf_free(ctx_buf);
		}

		free_exp_def(&e_def);
		return;
	}

	if (as_sindex_n_sindexes(ns) == MAX_N_SINDEXES) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: at sindex limit - ignoring %s",
				def->iname);
		SINDEX_GWUNLOCK();

		if (ctx_buf != NULL) {
			cf_free(ctx_buf);
		}

		free_exp_def(&e_def);
		return;
	}

	cf_info(AS_SINDEX, "SINDEX CREATE: request received for %s:%s via smd",
			ns->name, def->iname);

	as_sindex* si = cf_rc_alloc(sizeof(as_sindex));

	*si = (as_sindex){ .ns = ns,
		.set_id = set_id,
		.ktype = def->ktype,
		.itype = def->itype,
		.ctx_b64 = def->ctx_b64,
		.ctx_buf = ctx_buf,
		.ctx_buf_sz = (uint32_t)ctx_buf_sz,
		.exp = e_def.exp,
		.exp_b64 = def->exp_b64,
		.exp_buf = e_def.buf,
		.exp_buf_sz = (uint32_t)e_def.buf_sz,
		.exp_bins_info = e_def.bins_info,
		.n_btrees = AS_PARTITIONS };

	strcpy(si->iname, def->iname);
	strcpy(si->set_name, def->set_name);
	strcpy(si->bin_name, def->bin_name);

	// These are now owned by si - don't free outside.
	def->ctx_b64 = NULL;
	def->exp_b64 = NULL;

	if (ns->flat_sindexes == NULL) {
		add_to_sindexes(si);
		as_sindex_tree_create(si);
	}
	else {
		// Also inserts si in sindexes array, and marks si readable if so.
		as_sindex_tree_resume(si);
	}

	as_add_sindex(si);

	if (def->set_name[0] == '\0') {
		ns->n_setless_sindexes++;
	}
	else {
		p_set->n_sindexes++;
	}

	// Startup has its own mechanism to populate.
	if (startup) {
		SINDEX_GWUNLOCK();
		return;
	}

	as_fence_seq();

	if (p_set == NULL ? ns->n_objects == 0 : p_set->n_objects == 0) {
		// Shortcut if the set is empty.

		si->readable = true;
		si->populate_pct = 100;

		cf_info(AS_SINDEX, "{%s} empty sindex %s ready", ns->name, si->iname);
	}
	else {
		as_sindex_populate_add(si);
	}

	SINDEX_GWUNLOCK();
}

void
smd_sindex_drop(as_sindex_def* def)
{
	SINDEX_GWLOCK();

	as_namespace* ns = def->ns;
	uint16_t set_id = INVALID_SET_ID;

	if (def->set_name[0] != '\0' &&
			(set_id = as_namespace_get_set_id(ns, def->set_name)) ==
					INVALID_SET_ID) {
		cf_warning(AS_SINDEX, "SINDEX DROP: set '%s' not found", def->set_name);
		SINDEX_GWUNLOCK();
		return;
	}

	uint8_t* ctx_buf = NULL;
	int32_t ctx_buf_sz = 0;
	char* ctx_b64 = def->ctx_b64;
	uint8_t* exp_buf = NULL;
	int32_t exp_buf_sz = 0;
	char* exp_b64 = def->exp_b64;

	if (ctx_b64 != NULL) {
		ctx_buf_sz = as_sindex_cdt_ctx_b64_decode(ctx_b64,
				(uint32_t)strlen(ctx_b64), &ctx_buf);

		if (ctx_buf_sz < 0) {
			cf_warning(AS_SINDEX,
					"SINDEX DROP: invalid cdt context decode result %d",
					ctx_buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}
	else if (exp_b64 != NULL) {
		exp_buf_sz = as_sindex_exp_b64_decode(exp_b64,
				(uint32_t)strlen(exp_b64), &exp_buf);

		int32_t buf_sz = exp_buf_sz;

		if (buf_sz < 0) {
			cf_warning(AS_SINDEX,
					"SINDEX DROP: invalid expression decode result %d", buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}

	as_sindex* si = as_si_by_defn(ns, set_id, def->bin_name, def->ktype,
			def->itype, exp_buf, (uint32_t)exp_buf_sz, ctx_buf,
			(uint32_t)ctx_buf_sz);

	if (ctx_buf != NULL) {
		cf_free(ctx_buf);
	}

	if (exp_buf != NULL) {
		cf_free(exp_buf);
	}

	if (si == NULL) {
		cf_warning(AS_SINDEX, "SINDEX DROP: defn not found");
		SINDEX_GWUNLOCK();
		return;
	}

	si->dropped = true; // allow queries, populate, GC, collect-stats to abort

	SINDEX_GWUNLOCK();

	cf_info(AS_SINDEX, "SINDEX DROP: request received for %s:%s via smd",
			ns->name, si->iname);

	// Wait for queries etc. to be done with this sindex.
	while (si->n_jobs != 0) {
		usleep(100);
	}

	as_fence_acq();

	// At this point, no queries etc. can operate on this sindex. It's safe to
	// remove it and allow transactions to vacate/recycle references in the
	// sindex without harming the queries etc. (See AER-6611.)

	SINDEX_GWLOCK();

	drop_from_sindexes(si); // must precede bin_bitmap_clear()

	as_delete_sindex(si);

	if (def->set_name[0] == '\0') {
		ns->n_setless_sindexes--;
	}
	else {
		as_set* p_set = as_namespace_get_set_by_name(ns, def->set_name);

		p_set->n_sindexes--;
	}

	SINDEX_GWUNLOCK();

	// Release original rc-alloc ref-count.
	as_sindex_release(si);
}

static bool
parse_exp(const char* exp_b64, exp_def* e_def_r)
{
	uint8_t* buf = NULL;
	int32_t buf_sz =
			as_sindex_exp_b64_decode(exp_b64, (uint32_t)strlen(exp_b64), &buf);

	if (buf_sz < 0) {
		cf_warning(AS_SINDEX,
				"SINDEX CREATE: invalid expression decode result %d", buf_sz);
		return false;
	}

	cf_vector* bins_info = cf_vector_create(sizeof(as_bin_info), 10, 0);
	as_exp* exp = as_exp_build_buf(buf, (uint32_t)buf_sz, false, bins_info);
	// Non-taking build - drop the accumulator's payload ref.
	as_exp_build_err_reset();

	if (exp == NULL) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression %s", exp_b64);
		cf_free(buf);
		cf_vector_destroy(bins_info);
		return false;
	}

	bool unsupported_exp = false;

	if ((exp->flags & AS_EXP_HAS_NON_DIGEST_META) != 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX,
				"SINDEX CREATE: invalid expression %s - has non-digest metadata",
				exp_b64);
	}

	if ((exp->flags & AS_EXP_HAS_REC_KEY) != 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX,
				"SINDEX CREATE: invalid expression %s - has record key", exp_b64);
	}

	if ((exp->flags & AS_EXP_HAS_DIGEST_MOD) == 0 &&
			cf_vector_size(bins_info) == 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX,
				"SINDEX CREATE: invalid expression %s - needs digest modifier or bins",
				exp_b64);
	}

	if (unsupported_exp) {
		as_exp_destroy(exp);
		cf_free(buf);
		cf_vector_destroy(bins_info);
		return false;
	}

	// Transfer responsibility of freeing to caller.
	*e_def_r = (exp_def){
		.exp = exp, .buf = buf, .buf_sz = buf_sz, .bins_info = bins_info
	};

	return true;
}

static void
free_exp_def(exp_def* e_def)
{
	if (e_def->exp != NULL) {
		as_exp_destroy(e_def->exp);
	}

	if (e_def->buf != NULL) {
		cf_free(e_def->buf);
	}

	if (e_def->bins_info != NULL) {
		cf_vector_destroy(e_def->bins_info);
	}
}

//==========================================================
// Local helpers - set+(bin-name or exp) hash.
//

//==========================================================
// Local helpers - populate sbin.
//

static uint32_t
populate_sbins(as_namespace* ns, uint16_t set_id, const as_bin* b,
		as_sindex_bin* sbins, as_sindex_op op)
{
	cf_ll* si_ll = as_si_list_by_defn(ns, set_id, b->name, NULL, 0);

	if (si_ll == NULL) {
		return 0;
	}

	uint32_t n_populated = 0;
	cf_ll_element* ele = cf_ll_get_head(si_ll);

	while (ele != NULL) {
		defn_hash_ele* si_ele = (defn_hash_ele*)ele;
		as_sindex* si = si_ele->si;

		n_populated += populate_sbin_si(si, b, &sbins[n_populated], op);

		ele = ele->next;
	}

	return n_populated;
}

static uint32_t
populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbin,
		as_sindex_op op)
{
	init_sbin(sbin, op, si);

	if (sbin_from_bin(si, b, sbin)) {
		as_sindex_reserve(si);
		// Release & free will happen once sbin is updated in sindex tree.
		return 1;
	}

	sbin_free(sbin);

	return 0;
}

//==========================================================
// Local helpers - sindex lookup.
//

//==========================================================
// Local helpers - sbins from bins.
//

static bool
sbin_from_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	as_particle_type type = as_bin_get_particle_type(b);
	as_bin ctx_bin = { 0 };

	if (si->ctx_buf != NULL) {
		if (type != AS_PARTICLE_TYPE_LIST && type != AS_PARTICLE_TYPE_MAP) {
			return false;
		}

		if (! as_bin_cdt_get_by_context(b, si->ctx_buf, si->ctx_buf_sz, &ctx_bin)) {
			return false;
		}

		type = as_bin_get_particle_type(&ctx_bin);

		if (type == AS_PARTICLE_TYPE_GEOJSON &&
				! as_bin_cdt_context_geojson_parse(&ctx_bin)) {
			return false;
		}

		b = &ctx_bin;
	}

	bool rv;

	switch (si->itype) {
	case AS_SINDEX_ITYPE_DEFAULT:
		rv = (type == si->ktype && sbin_from_simple_bin(si, b, sbin));
		break;
	case AS_SINDEX_ITYPE_LIST:
		rv = (type == AS_PARTICLE_TYPE_LIST && sbin_from_cdt_bin(si, b, sbin));
		break;
	case AS_SINDEX_ITYPE_MAPKEYS:
	case AS_SINDEX_ITYPE_MAPVALUES:
		rv = (type == AS_PARTICLE_TYPE_MAP && sbin_from_cdt_bin(si, b, sbin));
		break;
	default:
		cf_crash(AS_SINDEX, "invalid index type %d", si->itype);
	}

	if (si->ctx_buf != NULL) {
		as_bin_particle_destroy(&ctx_bin);
	}

	return rv;
}

static bool
sbin_from_simple_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	as_particle_type type = as_bin_get_particle_type(b);

	if (type == AS_PARTICLE_TYPE_INTEGER) {
		add_value_to_sbin(sbin, as_bin_particle_integer_value(b));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_STRING) {
		char* str;
		uint32_t len = as_bin_particle_string_ptr(b, &str);

		if (len > MAX_STRING_KSIZE) {
			cf_ticker_warning(AS_SINDEX,
					"failed sindex on bin %s - string longer than %u",
					si->bin_name, MAX_STRING_KSIZE);
			return false;
		}

		add_value_to_sbin(sbin, as_sindex_string_to_bval(str, len));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_BLOB) {
		uint8_t* blob;
		uint32_t sz = as_bin_particle_blob_ptr(b, &blob);

		if (sz > MAX_BLOB_KSIZE) {
			cf_ticker_warning(AS_SINDEX,
					"failed sindex on bin %s - blob longer than %u",
					si->bin_name, MAX_BLOB_KSIZE);
			return false;
		}

		add_value_to_sbin(sbin, as_sindex_blob_to_bval(blob, sz));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_GEOJSON) {
		// GeoJSON is like AS_PARTICLE_TYPE_STRING when reading the value and
		// AS_PARTICLE_TYPE_INTEGER for adding the result to the index.
		uint64_t* cells;
		size_t ncells = as_bin_particle_geojson_cellids(b, &cells);

		if (ncells == 0) {
			// Empty coordinate arrays are "null objects".
			return false;
		}

		for (size_t ndx = 0; ndx < ncells; ++ndx) {
			add_value_to_sbin(sbin, (int64_t)cells[ndx]);
		}

		return true;
	}

	cf_crash(AS_SINDEX, "invalid bin type %d", type);
	return false;
}

static bool
sbin_from_cdt_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	switch (si->itype) {
	case AS_SINDEX_ITYPE_LIST:
		as_bin_list_foreach(b, add_listvalues_foreach, sbin);
		break;
	case AS_SINDEX_ITYPE_MAPKEYS:
		as_bin_map_foreach(b, add_mapkeys_foreach, sbin);
		break;
	case AS_SINDEX_ITYPE_MAPVALUES:
		as_bin_map_foreach(b, add_mapvalues_foreach, sbin);
		break;
	default:
		cf_crash(AS_SINDEX, "unexpected");
	}

	return sbin->n_values != 0;
}

//==========================================================
// Local helpers - value to sbin.
//

static void
add_value_to_sbin(as_sindex_bin* sbin, int64_t val)
{
	// If this is the first value, assign the value to the embedded field.
	if (sbin->n_values == 0) {
		sbin->val = val;
		sbin->n_values++;
		return;
	}

	if (sbin->values == NULL) {
		sbin->capacity = 32;
		sbin->values = cf_malloc(sbin->capacity * sizeof(int64_t));

		// Note - as used now, copied val is superfluous, we never look at it.
		sbin->values[0] = sbin->val;
	}
	else if (sbin->capacity == sbin->n_values) {
		sbin->capacity = 2 * sbin->capacity;
		sbin->values = cf_realloc(sbin->values, sbin->capacity * sizeof(int64_t));
	}

	sbin->values[sbin->n_values++] = val;
}

//==========================================================
// Local helpers - msgpack to sbin - iterator callbacks.
//

static bool
add_listvalues_foreach(msgpack_in* element, void* udata)
{
	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, element, sbin);

	return true;
}

static bool
add_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)val;

	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, key, sbin);

	return true;
}

static bool
add_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)key;

	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, val, sbin);

	return true;
}

//==========================================================
// Local helpers - msgpack to sbin - convert to ktypes.
//

static void
add_long_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	int64_t v;

	if (! msgpack_get_int64(element, &v)) {
		return;
	}

	add_value_to_sbin(sbin, v);
}

static void
add_string_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t str_sz;
	const uint8_t* str = msgpack_get_bin(element, &str_sz);

	if (str_sz == 0 || str == NULL || *str != AS_PARTICLE_TYPE_STRING) {
		return;
	}

	// Skip as_bytes type.
	str++;
	str_sz--;

	add_value_to_sbin(sbin, as_sindex_string_to_bval((const char*)str, str_sz));
}

static void
add_blob_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t blob_sz;
	const uint8_t* blob = msgpack_get_bin(element, &blob_sz);

	if (blob_sz == 0 || blob == NULL || *blob != AS_PARTICLE_TYPE_BLOB) {
		return;
	}

	// Skip as_bytes type.
	blob++;
	blob_sz--;

	add_value_to_sbin(sbin, as_sindex_blob_to_bval(blob, blob_sz));
}

static void
add_geojson_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t json_sz;
	const uint8_t* json = msgpack_get_bin(element, &json_sz);

	if (json_sz == 0 || json == NULL || *json != AS_PARTICLE_TYPE_GEOJSON) {
		return;
	}

	// Skip as_bytes type.
	json++;
	json_sz--;

	uint64_t cellid;
	geo_region_t region;

	if (! as_geojson_parse(NULL, (const char*)json, json_sz, &cellid, &region)) {
		return;
	}

	if (cellid != 0) { // POINT
		add_value_to_sbin(sbin, (int64_t)cellid);
	}
	else { // REGION
		uint32_t ncells;
		uint64_t outcells[MAX_REGION_CELLS];

		if (! geo_region_cover(NULL, region, MAX_REGION_CELLS, outcells, NULL,
					NULL, &ncells)) {
			cf_warning(AS_SINDEX, "geo_region_cover failed");
			geo_region_destroy(region);
			return;
		}

		geo_region_destroy(region);

		for (uint32_t i = 0; i < ncells; i++) {
			add_value_to_sbin(sbin, (int64_t)outcells[i]);
		}
	}
}

//==========================================================
// Local helpers - type utilities.
//

static char const*
ktype_str(as_particle_type ktype, bool use_integer)
{
	switch (ktype) {
	case AS_PARTICLE_TYPE_INTEGER:
		return use_integer ? "integer" : "numeric";
	case AS_PARTICLE_TYPE_STRING:
		return "string";
	case AS_PARTICLE_TYPE_BLOB:
		return "blob";
	case AS_PARTICLE_TYPE_GEOJSON:
		return "geo2dsphere";
	default:
		cf_crash(AS_SINDEX, "invalid ktype %d", ktype);
	}

	return NULL;
}

const char*
sindex_particle_type_str(as_particle_type type)
{
	return as_particle_type_str(type);
}

static as_particle_type
itype_to_exp_particle_type(as_sindex_type itype)
{
	switch (itype) {
	case AS_SINDEX_ITYPE_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case AS_SINDEX_ITYPE_MAPKEYS:
		return AS_PARTICLE_TYPE_MAP;
	case AS_SINDEX_ITYPE_MAPVALUES:
		return AS_PARTICLE_TYPE_MAP;
	default:
		return AS_PARTICLE_TYPE_BAD;
	}
}

//==========================================================
// Local helpers - stats.
//

static void*
run_cardinality(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;

	while (true) {
		for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
			SINDEX_GRLOCK();

			as_sindex* si = ns->sindexes[i];

			if (si == NULL || si->dropped || ! si->readable) {
				SINDEX_GRUNLOCK();
				continue;
			}

			as_sindex_job_reserve(si);

			SINDEX_GRUNLOCK();

			as_sindex_tree_collect_cardinality(si);

			as_sindex_job_release(si);
		}

		sleep(CARDINALITY_PERIOD);
	}

	return NULL;
}
