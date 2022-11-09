/*
 * sindex_tree.c
 *
 * Copyright (C) 2021-2022 Aerospike, Inc.
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

#include "sindex/sindex_tree.h"

#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_hash_math.h"

#include "arenax.h"
#include "bits.h"
#include "log.h"
#include "xmem.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "query/query_job.h"
#include "sindex/sindex.h"
#include "sindex/sindex_arena.h"

//#include "warnings.h"

#pragma GCC diagnostic warning "-Wcast-qual"
#pragma GCC diagnostic warning "-Wcast-align"

#define BINARY_SEARCH 1


//==========================================================
// Typedefs & constants.
//

#define MAX_GC_BURST 1000
#define MAX_GC_BURST_AF 30 // limit primary index IO under sindex tree lock

#define MAX_QUERY_BURST 100

#define MAX_CARDINALITY_BURST 1000

#define CACHE_LINE_SZ 64

#define HLL_N_INDEX_BITS 16
#define HLL_N_REGISTERS (1 << HLL_N_INDEX_BITS) // 64K

#define HLL_BITS 6
#define HLL_MAX_VALUE (1 << HLL_BITS)

#define HLL_REGISTERS_SZ (HLL_BITS * HLL_N_REGISTERS / 8) // 48K

#define HLL_ALPHA (0.7213 / (1.0 + 1.079 / HLL_N_REGISTERS))

typedef struct key_bound_s {
	int32_t index;
	bool equal;
} key_bound;

typedef enum key_mode_s {
	KEY_MODE_MATCH,
	KEY_MODE_MIN,
	KEY_MODE_MAX
} key_mode;

typedef struct gc_collect_cb_info_s {
	as_namespace* ns;

	uint32_t max_burst;

	uint32_t n_keys_reduced;
	uint32_t n_keys;
	si_btree_key* keys;

	search_key last;
} gc_collect_cb_info;

typedef struct query_collect_cb_info_s {
	cf_arenax* arena;
	as_index_tree* tree;

	uint32_t n_keys_reduced;
	uint32_t n_keys;
	si_btree_key* keys;

	bool de_dup;

	search_key last;
} query_collect_cb_info;

typedef struct hyperloglog_s {
	uint8_t registers[HLL_REGISTERS_SZ];
} hyperloglog;

typedef struct cardinality_collect_cb_info_s {
	as_namespace* ns;

	uint64_t* n_keys;
	hyperloglog* bval_hll;
	hyperloglog* rec_hll;

	uint32_t n_keys_reduced;

	search_key last;
} cardinality_collect_cb_info;


//==========================================================
// Forward declarations.
//

static void gc_reduce_and_delete(as_sindex* si, si_btree* bt);
static bool gc_collect_cb(const si_btree_key* key, void* udata);
static void query_reduce(si_btree* bt, as_partition_reservation* rsv, int64_t start_bval, int64_t end_bval, int64_t resume_bval, cf_digest* keyd, bool de_dup, as_sindex_reduce_fn cb, void* udata);
static bool query_collect_cb(const si_btree_key* key, void* udata);
static void cardinality_reduce(as_sindex* si, si_btree* bt, uint64_t* n_keys, hyperloglog* bval_hll, hyperloglog* rec_hll);
static bool cardinality_collect_cb(const si_btree_key* key, void* udata);

static si_btree* si_btree_create(cf_arenax* arena, as_sindex_arena* si_arena, bool unsigned_bvals, uint32_t si_id, uint16_t tree_ix);
static void si_btree_destroy(si_btree* bt);
static bool si_btree_put(si_btree* bt, const si_btree_key* key);

static void btree_destroy(si_btree* bt, si_arena_handle node_h);
static bool btree_put(si_btree* bt, si_btree_node* node, const si_btree_key* key);
static bool btree_delete(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out);
static bool btree_reduce(si_btree* bt, si_btree_node* node, const search_key* start_skey, const search_key* end_skey, si_btree_reduce_fn cb, void* udata);
static bool delete_case_1(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out);
static bool delete_case_2(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, key_bound bound);
static bool delete_case_2a(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static bool delete_case_2b(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static bool delete_case_2c(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* left, si_arena_handle right_h);
static bool delete_case_3(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, key_bound bound);
static bool delete_case_3a_left(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static bool delete_case_3a_right(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static bool delete_case_3b_left(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_arena_handle child_h, si_btree_node* sibling);
static bool delete_case_3b_right(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_arena_handle sibling_h);
static si_arena_handle create_node(const si_btree* bt, bool leaf);
static void move_keys(const si_btree* bt, si_btree_node* dst, uint32_t dst_i, si_btree_node* src, uint32_t src_i, uint32_t n_keys);
static void move_children(const si_btree* bt, si_btree_node* dst, uint32_t dst_i, si_btree_node* src, uint32_t src_i, uint32_t n_children);
static key_bound greatest_lower_bound(const si_btree* bt, const si_btree_node* node, const si_btree_key* key);
static key_bound left_bound(const si_btree* bt, const si_btree_node* node, const search_key* skey);
static void split_child(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static void merge_children(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* left, si_arena_handle right_h);

static void hll_add(hyperloglog* hll, const uint8_t* buf, size_t buf_sz);
static uint64_t hll_estimate_cardinality(const hyperloglog* hll);

static void hll_hash(const uint8_t* ele, size_t ele_sz, uint16_t* register_ix, uint64_t* value);
static uint64_t hll_get_register(const hyperloglog* hll, uint32_t r);
static void hll_set_register(hyperloglog* hll, uint32_t r, uint64_t value);
static double hll_tau(double val);
static double hll_sigma(double val);


//==========================================================
// Inlines & macros.
//

static inline bool
find_r_h(cf_arenax_handle r_h, const si_btree_key* keys, uint32_t n_keys)
{
	for (uint32_t i = 0; i < n_keys; i++) {
		if (keys[i].r_h == r_h) {
			return true;
		}
	}

	return false;
}

static inline int32_t
bval_cmp(int64_t bval_1, int64_t bval_2)
{
	return bval_1 > bval_2 ? 1 : (bval_1 < bval_2 ? -1 : 0);
}

static inline int32_t
bval_cmp_unsigned(int64_t bval_1, int64_t bval_2)
{
	return (uint64_t)bval_1 > (uint64_t)bval_2 ?
			1 : ((uint64_t)bval_1 < (uint64_t)bval_2 ? -1 : 0);
}

static inline int32_t
key_cmp(const si_btree* bt, const si_btree_key* key_1,
		const si_btree_key* key_2)
{
	int32_t cmp = bt->unsigned_bvals ?
			bval_cmp_unsigned(key_1->bval, key_2->bval) :
			bval_cmp(key_1->bval, key_2->bval);

	if (cmp != 0) {
		return cmp;
	}
	// else - same bval

	if (key_1->keyd_stub > key_2->keyd_stub) {
		return 1;
	}

	if (key_1->keyd_stub < key_2->keyd_stub) {
		return -1;
	}

	as_index* r_1 = cf_arenax_resolve(bt->arena, key_1->r_h);
	as_index* r_2 = cf_arenax_resolve(bt->arena, key_2->r_h);

	return cf_digest_compare(&r_1->keyd, &r_2->keyd);
}

static inline int32_t
skey_cmp(const si_btree* bt, const search_key* skey, const si_btree_key* key)
{
	int32_t cmp = bt->unsigned_bvals ?
			bval_cmp_unsigned(skey->bval, key->bval) :
			bval_cmp(skey->bval, key->bval);

	if (cmp != 0) {
		return cmp;
	}
	// else - same bval

	if (! skey->has_digest) {
		return -1; // keep looking left
	}

	if (skey->keyd_stub > key->keyd_stub) {
		return 1;
	}

	if (skey->keyd_stub < key->keyd_stub) {
		return -1;
	}

	as_index* r = cf_arenax_resolve(bt->arena, key->r_h);

	return cf_digest_compare(&skey->keyd, &r->keyd);
}

static inline int32_t
end_skey_cmp(const si_btree* bt, const search_key* skey,
		const si_btree_key* key)
{
	return bt->unsigned_bvals ?
			bval_cmp_unsigned(skey->bval, key->bval) :
			bval_cmp(skey->bval, key->bval);
}

static inline si_btree_key*
mut_key(const si_btree* bt, si_btree_node* node, uint32_t i)
{
	return (si_btree_key*)((uint8_t*)node + bt->keys_off) + i;
}

static inline const si_btree_key*
const_key(const si_btree* bt, const si_btree_node* node, uint32_t i)
{
	return (const si_btree_key*)((const uint8_t*)node + bt->keys_off) + i;
}

static inline void
get_key(const si_btree* bt, const si_btree_node* node, uint32_t i,
		si_btree_key* key)
{
	const si_btree_key* node_key = const_key(bt, node, i);

	memcpy(key, node_key, sizeof(si_btree_key));
}

static inline void
set_key(const si_btree* bt, si_btree_node* node, uint32_t i,
		const si_btree_key* key)
{
	si_btree_key* node_key = mut_key(bt, node, i);

	memcpy(node_key, key, sizeof(si_btree_key));
}

static inline bool
refs_match(const si_btree* bt, const si_btree_node* node, int32_t i,
		const si_btree_key* key)
{
	return key->r_h == const_key(bt, node, (uint32_t)i)->r_h;
}

static inline si_arena_handle*
mut_children(const si_btree* bt, si_btree_node* node)
{
	return (si_arena_handle*)((uint8_t*)node + bt->children_off);
}

static inline const si_arena_handle*
const_children(const si_btree* bt, si_btree_node* node)
{
	return (const si_arena_handle*)((const uint8_t*)node + bt->children_off);
}

static inline void
set_child(const si_btree* bt, si_btree_node* node, uint32_t i,
		si_arena_handle child_h)
{
	si_arena_handle* node_children = mut_children(bt, node);

	node_children[i] = child_h;
}

#define SI_RESOLVE(_h) \
	((si_btree_node*)as_sindex_arena_resolve(bt->si_arena, _h))

#define ROUND_FRACTION(n, d) ((uint64_t)(((double)n / d) + 0.5))


//==========================================================
// Public API.
//

void
as_sindex_tree_create(as_sindex* si)
{
	as_namespace* ns = si->ns;

	si->btrees = cf_malloc(si->n_btrees * sizeof(si_btree*));

	bool unsigned_bvals = si->ktype == AS_PARTICLE_TYPE_GEOJSON;

	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		si->btrees[ix] = si_btree_create(ns->arena, ns->si_arena,
				unsigned_bvals, si->id, (uint16_t)ix);
	}
}

void
as_sindex_tree_destroy(as_sindex* si)
{
	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		si_btree_destroy(si->btrees[ix]);
	}

	cf_free(si->btrees);
	si->btrees = NULL;
}

uint64_t
as_sindex_tree_n_keys(const as_sindex* si)
{
	uint64_t n_keys = 0;

	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		n_keys += si->btrees[ix]->n_keys;
	}

	return n_keys;
}

uint64_t
as_sindex_tree_mem_size(const as_sindex* si)
{
	uint64_t n_nodes = 0;

	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		n_nodes += si->btrees[ix]->n_nodes;
	}

	return n_nodes * si->ns->si_arena->ele_sz; // ignore si_btree overhead
}

void
as_sindex_tree_gc(as_sindex* si)
{
	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		gc_reduce_and_delete(si, si->btrees[ix]);
	}
}

bool
as_sindex_tree_put(as_sindex* si, int64_t bval, cf_arenax_handle r_h)
{
	as_index* r = cf_arenax_resolve(si->ns->arena, r_h);
	si_btree* bt = si->btrees[as_partition_getid(&r->keyd)];

	si_btree_key key = {
			.bval = bval,
			.keyd_stub = get_keyd_stub(&r->keyd),
			.r_h = r_h
	};

	return si_btree_put(bt, &key);
}

bool
as_sindex_tree_delete(as_sindex* si, int64_t bval, cf_arenax_handle r_h)
{
	as_index* r = cf_arenax_resolve(si->ns->arena, r_h);
	si_btree* bt = si->btrees[as_partition_getid(&r->keyd)];

	si_btree_key key = {
			.bval = bval,
			.keyd_stub = get_keyd_stub(&r->keyd),
			.r_h = r_h
	};

	return si_btree_delete(bt, &key);
}

void
as_sindex_tree_query(as_sindex* si, const as_query_range* range,
		as_partition_reservation* rsv, int64_t bval, cf_digest* keyd,
		as_sindex_reduce_fn cb, void* udata)
{
	si_btree* bt = si->btrees[rsv->p->id];

	if (range->bin_type == AS_PARTICLE_TYPE_GEOJSON && range->isrange) {
		for (uint32_t r_ix = 0; r_ix < range->u.geo.num_r; r_ix++) {
			as_query_range_start_end* r = &range->u.geo.r[r_ix];

			if (keyd != NULL && (uint64_t)bval > (uint64_t)r->end) {
				continue;
			}

			query_reduce(bt, rsv, r->start, r->end, bval, keyd, range->de_dup,
					cb, udata);
		}

		return;
	}

	query_reduce(bt, rsv, range->u.r.start, range->u.r.end, bval, keyd,
			range->de_dup, cb, udata);
}

void
as_sindex_tree_collect_cardinality(as_sindex* si)
{
	uint64_t n_keys = 0;
	hyperloglog bval_hll = { { 0 } };
	hyperloglog* rec_hll = si->itype == AS_SINDEX_ITYPE_DEFAULT ?
			NULL : cf_calloc(1, sizeof(hyperloglog));

	for (uint32_t ix = 0; ix < si->n_btrees; ix++) {
		si_btree* bt = si->btrees[ix];

		if (bt->n_keys != 0) {
			cardinality_reduce(si, bt, &n_keys, &bval_hll, rec_hll);

			usleep(100);
		}
	}

	uint64_t n_bvals = hll_estimate_cardinality(&bval_hll);

	si->keys_per_bval = n_bvals == 0 ? 0 : ROUND_FRACTION(n_keys, n_bvals);

	if (rec_hll != NULL) {
		uint64_t n_recs = hll_estimate_cardinality(rec_hll);

		si->keys_per_rec = n_recs == 0 ? 0 : ROUND_FRACTION(n_keys, n_recs);

		cf_free(rec_hll);
	}
	else {
		si->keys_per_rec = n_keys == 0 ? 0 : 1;
	}
}


//==========================================================
// Local helpers - reduce utilities.
//

static void
gc_reduce_and_delete(as_sindex* si, si_btree* bt)
{
	as_namespace* ns = si->ns;

	uint32_t max_burst = ns->xmem_type == CF_XMEM_TYPE_FLASH ?
			MAX_GC_BURST_AF : MAX_GC_BURST;

	bool first = true;
	si_btree_key keys[max_burst];

	gc_collect_cb_info ci = {
			.ns = ns,
			.max_burst = max_burst,
			.keys = keys
	};

	while (! si->dropped) {
		search_key* last = first ? NULL : &ci.last;

		si_btree_reduce(bt, last, NULL, gc_collect_cb, &ci);

		first = false;

		for (uint32_t i = 0; i < ci.n_keys; i++) {
			si_btree_delete(bt, &keys[i]);
		}

		si->n_gc_cleaned += ci.n_keys;
		ns->n_sindex_gc_cleaned += ci.n_keys;

		if (ci.n_keys_reduced != max_burst) {
			return; // done with this physical tree
		}

		ci.n_keys_reduced = 0;
		ci.n_keys = 0;
	}
}

static bool
gc_collect_cb(const si_btree_key* key, void* udata)
{
	gc_collect_cb_info* ci = (gc_collect_cb_info*)udata;
	as_namespace* ns = ci->ns;

	as_index* r = cf_arenax_resolve(ns->arena, key->r_h);

	if (r->generation == 0 ||
			ns->si_gc_tlist_map[as_partition_getid(&r->keyd)][r->tree_id]) {
		ci->keys[ci->n_keys++] = *key;
	}

	if (++ci->n_keys_reduced == ci->max_burst) {
		ci->last = (search_key){
				.bval = key->bval,
				.has_digest = true,
				.keyd_stub = get_keyd_stub(&r->keyd),
				.keyd = r->keyd
		};

		return false; // stops si_btree_reduce()
	}

	return true;
}

static void
query_reduce(si_btree* bt, as_partition_reservation* rsv, int64_t start_bval,
		int64_t end_bval, int64_t resume_bval, cf_digest* keyd, bool de_dup,
		as_sindex_reduce_fn cb, void* udata)
{
	as_namespace* ns = rsv->ns;

	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		query_reduce_no_rc(bt, rsv, start_bval, end_bval, resume_bval, keyd,
				de_dup, cb, udata);
		return;
	}

	si_btree_key keys[MAX_QUERY_BURST];

	query_collect_cb_info ci = {
			.arena = bt->arena,
			.tree = rsv->tree,
			.keys = keys,
			.de_dup = de_dup,
			.last = { .bval = start_bval }
	};

	if (keyd != NULL && (bt->unsigned_bvals ?
			(uint64_t)resume_bval >= (uint64_t)start_bval :
			resume_bval >= start_bval)) {
		ci.last = (search_key){
				.bval = resume_bval,
				.has_digest = true,
				.keyd_stub = get_keyd_stub(keyd),
				.keyd = *keyd
		};
	}

	if (bt->unsigned_bvals ?
			(uint64_t)ci.last.bval > (uint64_t)end_bval :
			ci.last.bval > end_bval) {
		return;
	}

	search_key end_skey = { .bval = end_bval };

	while (true) {
		si_btree_reduce(bt, &ci.last, &end_skey, query_collect_cb, &ci);

		bool do_more = true;

		for (uint32_t i = 0; i < ci.n_keys; i++) {
			si_btree_key* key = &keys[i];

			as_record* r = cf_arenax_resolve(bt->arena, key->r_h);
			as_index_ref r_ref = {
					.r = r,
					.r_h = key->r_h,
					.olock = as_index_olock_from_keyd(rsv->tree, &r->keyd)
			};

			cf_mutex_lock(r_ref.olock);

			as_index_release(r);

			if (! as_index_is_valid_record(r)) {
				as_record_done(&r_ref, ns);
				continue;
			}

			if (do_more) {
				// Callback MUST call as_record_done() to unlock record.
				do_more = cb(&r_ref, key->bval, udata);
			}
			else {
				cf_mutex_unlock(r_ref.olock);
			}
		}

		if (! do_more) {
			return; // user callback stopped query
		}

		if (ci.n_keys_reduced != MAX_QUERY_BURST) {
			return; // done with this physical tree
		}

		ci.n_keys_reduced = 0;
		ci.n_keys = 0;
	}
}

static bool
query_collect_cb(const si_btree_key* key, void* udata)
{
	query_collect_cb_info* ci = (query_collect_cb_info*)udata;
	as_index_tree* tree = ci->tree;

	as_index* r = cf_arenax_resolve(ci->arena, key->r_h);

	if (r->tree_id == tree->id && r->generation != 0 && (r->rc == 1 ||
			! (ci->de_dup && find_r_h(key->r_h, ci->keys, ci->n_keys)))) {
		cf_mutex* rlock = as_index_rlock_from_keyd(tree, &r->keyd);

		cf_mutex_lock(rlock);

		if (r->generation != 0) {
			as_index_reserve(r);
			ci->keys[ci->n_keys++] = *key;
		}

		cf_mutex_unlock(rlock);
	}

	if (++ci->n_keys_reduced == MAX_QUERY_BURST) {
		ci->last = (search_key){
				.bval = key->bval,
				.has_digest = true,
				.keyd_stub = get_keyd_stub(&r->keyd),
				.keyd = r->keyd
		};

		return false; // stops si_btree_reduce()
	}

	return true;
}

static void
cardinality_reduce(as_sindex* si, si_btree* bt, uint64_t* n_keys,
		hyperloglog* bval_hll, hyperloglog* rec_hll)
{
	as_namespace* ns = si->ns;

	bool first = true;

	cardinality_collect_cb_info ci = {
			.ns = ns,
			.n_keys = n_keys,
			.bval_hll = bval_hll,
			.rec_hll = rec_hll
	};

	while (! si->dropped) {
		search_key* last = first ? NULL : &ci.last;

		si_btree_reduce(bt, last, NULL, cardinality_collect_cb, &ci);

		first = false;

		if (ci.n_keys_reduced != MAX_CARDINALITY_BURST) {
			return; // done with this physical tree
		}

		ci.n_keys_reduced = 0;
	}
}

static bool
cardinality_collect_cb(const si_btree_key* key, void* udata)
{
	cardinality_collect_cb_info* ci = (cardinality_collect_cb_info*)udata;
	as_namespace* ns = ci->ns;

	(*ci->n_keys)++;

	hll_add(ci->bval_hll, (const uint8_t*)&key->bval, sizeof(key->bval));

	if (ci->rec_hll != NULL) {
		uint64_t h = key->r_h;

		// TODO - worth it to "swap" to LE and use 5 bytes?
		hll_add(ci->rec_hll, (const uint8_t*)&h, sizeof(h));
	}

	if (++ci->n_keys_reduced == MAX_CARDINALITY_BURST) {
		as_index* r = cf_arenax_resolve(ns->arena, key->r_h);

		ci->last.bval = key->bval;
		ci->last.has_digest = true;
		ci->last.keyd_stub = get_keyd_stub(&r->keyd);
		ci->last.keyd = r->keyd;

		return false; // stops si_btree_reduce()
	}

	return true;
}


//==========================================================
// Local helpers - si_btree layer.
//

static si_btree*
si_btree_create(cf_arenax* arena, as_sindex_arena* si_arena,
		bool unsigned_bvals, uint32_t si_id, uint16_t tree_ix)
{
	si_btree* bt = cf_calloc(1, sizeof(si_btree));

	pthread_rwlockattr_t rwattr;

	pthread_rwlockattr_init(&rwattr);
	pthread_rwlockattr_setkind_np(&rwattr,
			PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

	pthread_rwlock_init(&bt->lock, &rwattr);

	bt->arena = arena;
	bt->si_arena = si_arena;
	bt->unsigned_bvals = unsigned_bvals;
	bt->si_id = si_id;
	bt->tree_ix = tree_ix;

	uint32_t node_sz = si_arena->ele_sz;

	bt->inner_order = (uint32_t)
			(((((node_sz - sizeof(si_btree_node) - sizeof(si_arena_handle)) /
			(sizeof(si_btree_key) + sizeof(si_arena_handle))) + 1) / 2) * 2);
			// 226
	bt->leaf_order = (uint32_t)
			(((((node_sz - sizeof(si_btree_node)) /
			sizeof(si_btree_key)) + 1) / 2) * 2);
			// 292

	bt->keys_off = (uint32_t)sizeof(si_btree_node);
	bt->children_off = (uint32_t)
			(node_sz - (bt->inner_order * sizeof(si_arena_handle)));

	bt->root_h = create_node(bt, true);
	bt->n_nodes = 1;
	bt->n_keys = 0;

	return bt;
}

static void
si_btree_destroy(si_btree* bt)
{
	btree_destroy(bt, bt->root_h);
	pthread_rwlock_destroy(&bt->lock);
	cf_free(bt);
}

static bool
si_btree_put(si_btree* bt, const si_btree_key* key)
{
	pthread_rwlock_wrlock(&bt->lock);

	si_btree_node* root = SI_RESOLVE(bt->root_h);

	if (root->n_keys == root->max_degree - 1) {
		si_arena_handle new_root_h = create_node(bt, false);
		si_btree_node* new_root = SI_RESOLVE(new_root_h);

		bt->n_nodes++;

		set_child(bt, new_root, 0, bt->root_h);
		split_child(bt, new_root, 0, root);

		bt->root_h = new_root_h;
		root = new_root;
	}

	if (! btree_put(bt, root, key)) {
		pthread_rwlock_unlock(&bt->lock);
		return false;
	}

	bt->n_keys++;

	pthread_rwlock_unlock(&bt->lock);
	return true;
}

// Accessed from enterprise split.
bool
si_btree_delete(si_btree* bt, const si_btree_key* key)
{
	pthread_rwlock_wrlock(&bt->lock);

	si_btree_node* root = SI_RESOLVE(bt->root_h);

	if (! btree_delete(bt, root, KEY_MODE_MATCH, key, NULL)) {
		pthread_rwlock_unlock(&bt->lock);
		return false;
	}

	bt->n_keys--;

	if (root->n_keys == 0 && root->leaf == 0) {
		si_arena_handle root_h = bt->root_h;

		bt->root_h = const_children(bt, root)[0];

		as_sindex_arena_free(bt->si_arena, root_h);

		bt->n_nodes--;
	}

	pthread_rwlock_unlock(&bt->lock);
	return true;
}

// Accessed from enterprise split.
void
si_btree_reduce(si_btree* bt, const search_key* start_skey,
		const search_key* end_skey, si_btree_reduce_fn cb, void* udata)
{
	pthread_rwlock_rdlock(&bt->lock);

	btree_reduce(bt, SI_RESOLVE(bt->root_h), start_skey, end_skey, cb, udata);

	pthread_rwlock_unlock(&bt->lock);
}


//==========================================================
// Local helpers - lowest btree layer.
//

static void
btree_destroy(si_btree* bt, si_arena_handle node_h)
{
	si_btree_node* node = SI_RESOLVE(node_h);

	if (node->leaf == 0) {
		si_arena_handle* children = mut_children(bt, node);

		for (uint32_t i = 0; i <= node->n_keys; i++) {
			btree_destroy(bt, children[i]);
		}
	}

	as_sindex_arena_free(bt->si_arena, node_h);
}

static bool
btree_put(si_btree* bt, si_btree_node* node, const si_btree_key* key)
{
	cf_assert(node->n_keys < node->max_degree - 1, AS_SINDEX,
			"bad key count: %d", node->n_keys);

	key_bound bound = greatest_lower_bound(bt, node, key);

	if (bound.equal) {
		si_btree_key* node_key = mut_key(bt, node, (uint32_t)bound.index);

		if (key->r_h != node_key->r_h) {
			*node_key = *key; // replace old key pending garbage collection
		}

		return false; // didn't add a new key - don't bump key count
	}

	uint32_t i = (uint32_t)(bound.index + 1);

	if (node->leaf != 0) {
		if (i < node->n_keys) {
			move_keys(bt, node, i + 1, node, i, node->n_keys - i);
		}

		set_key(bt, node, i, key);
		node->n_keys++;
		return true;
	}

	si_arena_handle* children = mut_children(bt, node);
	si_btree_node* child = SI_RESOLVE(children[i]);

	if (child->n_keys == child->max_degree - 1) {
		split_child(bt, node, i, child);

		// split_child() pulled up a key to i - look at it.

		si_btree_key* node_key = mut_key(bt, node, i);
		int32_t comp = key_cmp(bt, key, node_key);

		if (comp == 0) {
			if (key->r_h != node_key->r_h) {
				*node_key = *key; // replace old key pending garbage collection
			}

			return false; // didn't add a new key - don't bump key count
		}

		if (comp > 0) {
			i++;
			child = SI_RESOLVE(children[i]);
		}
	}

	return btree_put(bt, child, key);
}

static bool
btree_delete(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out)
{
	cf_assert(mode != KEY_MODE_MATCH || (key_in != NULL && key_out == NULL),
			AS_SINDEX, "bad arguments");
	cf_assert(mode == KEY_MODE_MATCH || (key_in == NULL && key_out != NULL),
			AS_SINDEX, "bad arguments");

	si_btree_node* root = SI_RESOLVE(bt->root_h);

	cf_assert(node == root || node->n_keys >= node->min_degree, AS_SINDEX,
			"bad key count: %d", node->n_keys);

	if (node == root && node->n_keys == 0) {
		return false;
	}

	if (node->leaf != 0) {
		return delete_case_1(bt, node, mode, key_in, key_out);
	}

	key_bound bound;

	switch (mode) {
	case KEY_MODE_MATCH:
		bound = greatest_lower_bound(bt, node, key_in);
		if (bound.equal && ! refs_match(bt, node, bound.index, key_in)) {
			return false;
		}
		break;

	case KEY_MODE_MIN:
		bound = (key_bound){ .index = -1, .equal = false };
		break;

	case KEY_MODE_MAX:
		bound = (key_bound){ .index = node->n_keys - 1, .equal = false };
		break;

	default:
		cf_crash(AS_SINDEX, "bad mode");
	}

	if (bound.equal) {
		return delete_case_2(bt, node, mode, key_in, key_out, bound);
	}

	return delete_case_3(bt, node, mode, key_in, key_out, bound);
}

static bool
btree_reduce(si_btree* bt, si_btree_node* node, const search_key* start_skey,
		const search_key* end_skey, si_btree_reduce_fn cb, void* udata)
{
	key_bound bound;

	if (start_skey != NULL) {
		bound = left_bound(bt, node, start_skey);

		if (bound.equal) {
			// No callback since explicit start key is exclusive.
			start_skey = NULL;
		}
	}
	else {
		bound = (key_bound){ .index = -1, .equal = false };
	}

	uint32_t i = (uint32_t)(bound.index + 1);
	const si_arena_handle* children = node->leaf == 0 ?
			const_children(bt, node) : NULL;

	if (children != NULL &&
			! btree_reduce(bt, SI_RESOLVE(children[i]), start_skey, end_skey,
					cb, udata)) {
		return false;
	}

	const si_btree_key* key_cb = const_key(bt, node, i);

	while (++i <= node->n_keys) {
		if (end_skey != NULL && end_skey_cmp(bt, end_skey, key_cb) < 0) {
			return false;
		}

		if (! cb(key_cb, udata)) {
			return false;
		}

		if (children != NULL &&
				! btree_reduce(bt, SI_RESOLVE(children[i]), NULL, end_skey,
						cb, udata)) {
			return false;
		}

		key_cb++;
	}

	return true;
}

static bool
delete_case_1(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out)
{
	cf_assert(node->leaf != 0, AS_SINDEX, "bad leaf flag");

	key_bound bound;

	switch (mode) {
	case KEY_MODE_MATCH:
		bound = greatest_lower_bound(bt, node, key_in);
		if (bound.equal && ! refs_match(bt, node, bound.index, key_in)) {
			return false;
		}
		break;

	case KEY_MODE_MIN:
		bound = (key_bound){ .index = 0, .equal = true };
		break;

	case KEY_MODE_MAX:
		bound = (key_bound){ .index = node->n_keys - 1, .equal = true };
		break;

	default:
		cf_crash(AS_SINDEX, "bad mode");
	}

	if (! bound.equal) {
		return false;
	}

	if (mode != KEY_MODE_MATCH) {
		get_key(bt, node, (uint32_t)bound.index, key_out);
	}

	if (bound.index < node->n_keys - 1) {
		uint32_t i = (uint32_t)bound.index;

		move_keys(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	}

	node->n_keys--;
	return true;
}

static bool
delete_case_2(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, key_bound bound)
{
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(mode == KEY_MODE_MATCH, AS_SINDEX, "bad mode");
	cf_assert(bound.equal, AS_SINDEX, "bad equality flag");

	uint32_t i = (uint32_t)bound.index;
	si_arena_handle* children = mut_children(bt, node);
	si_btree_node* left = SI_RESOLVE(children[i]);

	if (left->n_keys >= left->min_degree) {
		return delete_case_2a(bt, node, i, left);
	}

	si_arena_handle right_h = children[i + 1];
	si_btree_node* right = SI_RESOLVE(right_h);

	if (right->n_keys >= right->min_degree) {
		return delete_case_2b(bt, node, i, right);
	}

	return delete_case_2c(bt, node, mode, key_in, key_out, i, left, right_h);
}

static bool
delete_case_2a(si_btree* bt, si_btree_node* node, uint32_t i,
		si_btree_node* child)
{
	si_btree_key* node_key = mut_key(bt, node, i);

	// NOTE: May modify this key not just from the child, but from anywhere
	// further down in the tree. Keep in mind for lock coupling.

	return btree_delete(bt, child, KEY_MODE_MAX, NULL, node_key);
}

static bool
delete_case_2b(si_btree* bt, si_btree_node* node, uint32_t i,
		si_btree_node* child)
{
	si_btree_key* node_key = mut_key(bt, node, i);

	// NOTE: May modify this key not just from the child, but from anywhere
	// further down in the tree. Keep in mind for lock coupling.

	return btree_delete(bt, child, KEY_MODE_MIN, NULL, node_key);
}

static bool
delete_case_2c(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_btree_node* left, si_arena_handle right_h)
{
	merge_children(bt, node, i, left, right_h);

	return btree_delete(bt, left, mode, key_in, key_out);
}

static bool
delete_case_3(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, key_bound bound)
{
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(! bound.equal, AS_SINDEX, "bad equality flag");

	uint32_t i = (uint32_t)(bound.index + 1);
	si_arena_handle* children = mut_children(bt, node);
	si_arena_handle child_h = children[i];
	si_btree_node* child = SI_RESOLVE(child_h);

	if (child->n_keys >= child->min_degree) {
		return btree_delete(bt, child, mode, key_in, key_out);
	}

	cf_assert(child->n_keys == child->min_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	if (i > 0) {
		si_btree_node* sibling = SI_RESOLVE(children[i - 1]);

		if (sibling->n_keys >= sibling->min_degree) {
			return delete_case_3a_left(bt, node, mode, key_in, key_out, i,
					child, sibling);
		}

		cf_assert(sibling->n_keys == sibling->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i < node->n_keys) {
		si_btree_node* sibling = SI_RESOLVE(children[i + 1]);

		if (sibling->n_keys >= sibling->min_degree) {
			return delete_case_3a_right(bt, node, mode, key_in, key_out, i,
					child, sibling);
		}

		cf_assert(sibling->n_keys == sibling->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i > 0) {
		si_btree_node* sibling = SI_RESOLVE(children[i - 1]);

		return delete_case_3b_left(bt, node, mode, key_in, key_out, i, child_h,
				sibling);
	}

	si_arena_handle sibling_h = children[i + 1];

	return delete_case_3b_right(bt, node, mode, key_in, key_out, i, child,
			sibling_h);
}

static bool
delete_case_3a_left(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_btree_node* child, si_btree_node* sibling)
{
	cf_assert(child->leaf == sibling->leaf, AS_SINDEX, "bad leaf flag");

	move_keys(bt, child, 1, child, 0, child->n_keys);

	if (child->leaf == 0) {
		move_children(bt, child, 1, child, 0, (uint32_t)child->n_keys + 1);
	}

	move_keys(bt, child, 0, node, i - 1, 1);

	if (child->leaf == 0) {
		move_children(bt, child, 0, sibling, sibling->n_keys, 1);
	}

	child->n_keys++;

	move_keys(bt, node, i - 1, sibling, (uint32_t)sibling->n_keys - 1, 1);

	sibling->n_keys--;

	return btree_delete(bt, child, mode, key_in, key_out);
}

static bool
delete_case_3a_right(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_btree_node* child, si_btree_node* sibling)
{
	cf_assert(child->leaf == sibling->leaf, AS_SINDEX, "bad leaf flag");

	move_keys(bt, child, child->n_keys, node, i, 1);

	if (child->leaf == 0) {
		move_children(bt, child, (uint32_t)child->n_keys + 1, sibling, 0, 1);
	}

	child->n_keys++;

	move_keys(bt, node, i, sibling, 0, 1);

	move_keys(bt, sibling, 0, sibling, 1, (uint32_t)sibling->n_keys - 1);

	if (child->leaf == 0) {
		move_children(bt, sibling, 0, sibling, 1, sibling->n_keys);
	}

	sibling->n_keys--;

	return btree_delete(bt, child, mode, key_in, key_out);
}

static bool
delete_case_3b_left(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_arena_handle child_h, si_btree_node* sibling)
{
	merge_children(bt, node, i - 1, sibling, child_h);

	return btree_delete(bt, sibling, mode, key_in, key_out);
}

static bool
delete_case_3b_right(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_btree_node* child, si_arena_handle sibling_h)
{
	merge_children(bt, node, i, child, sibling_h);

	return btree_delete(bt, child, mode, key_in, key_out);
}

static si_arena_handle
create_node(const si_btree* bt, bool leaf)
{
	si_arena_handle h = as_sindex_arena_alloc(bt->si_arena);
	si_btree_node* node = SI_RESOLVE(h);

	if (leaf) {
		*node = (si_btree_node){
				.leaf = 1,
				.si_id = bt->si_id,
				.tree_ix = bt->tree_ix,
				.min_degree = (uint16_t)(bt->leaf_order / 2),
				.max_degree = (uint16_t)bt->leaf_order
		};
	}
	else {
		*node = (si_btree_node){
				.si_id = bt->si_id,
				.tree_ix = bt->tree_ix,
				.min_degree = (uint16_t)(bt->inner_order / 2),
				.max_degree = (uint16_t)bt->inner_order
		};
	}

	return h;
}

static void
move_keys(const si_btree* bt, si_btree_node* dst, uint32_t dst_i,
		si_btree_node* src, uint32_t src_i, uint32_t n_keys)
{
	si_btree_key* dst_keys = mut_key(bt, dst, dst_i);
	const si_btree_key* src_keys = const_key(bt, src, src_i);

	memmove(dst_keys, src_keys, n_keys * sizeof(si_btree_key));
}

static void
move_children(const si_btree* bt, si_btree_node* dst, uint32_t dst_i,
		si_btree_node* src, uint32_t src_i, uint32_t n_children)
{
	si_arena_handle* dst_children = mut_children(bt, dst);
	const si_arena_handle* src_children = const_children(bt, src);

	memmove(&dst_children[dst_i], &src_children[src_i],
			n_children * sizeof(si_arena_handle));
}

#if BINARY_SEARCH == 0
static key_bound
greatest_lower_bound(const si_btree* bt, const si_btree_node* node,
		const si_btree_key* key)
{
	int32_t index = -1;
	bool equal = false;
	const si_btree_key* key_i = const_key(bt, node, 0);
	const uint8_t* pref = (const uint8_t*)key_i;

	as_arch_prefetch_nt(pref);

	for (uint32_t i = 0; i < node->n_keys; i++) {
		if ((const uint8_t*)key_i >= pref) {
			pref += CACHE_LINE_SZ;
			as_arch_prefetch_nt(pref);
		}

		int32_t rel = key_cmp(bt, key, key_i);

		if (rel < 0) {
			break;
		}

		index = (int32_t)i;

		if (rel == 0) {
			equal = true;
			break;
		}

		key_i++;
	}

	return (key_bound){ .index = index, .equal = equal };
}
#else
static key_bound
greatest_lower_bound(const si_btree* bt, const si_btree_node* node,
		const si_btree_key* key)
{
	int32_t lower = 0;
	int32_t upper = node->n_keys - 1;
	key_bound bound = { .index = -1, .equal = false };

	while (lower <= upper) {
		int32_t i = (lower + upper) / 2;
		const si_btree_key* key_i = const_key(bt, node, (uint32_t)i);

		as_arch_prefetch_nt(key_i);

		int32_t rel = key_cmp(bt, key_i, key);

		if (rel == 0) {
			bound.index = i;
			bound.equal = true;
			break;
		}

		if (rel < 0) {
			bound.index = (int32_t)i;
			lower = i + 1;
		}
		else {
			upper = i - 1;
		}
	}

	return bound;
}
#endif

#if BINARY_SEARCH == 0
static key_bound
left_bound(const si_btree* bt, const si_btree_node* node,
		const search_key* skey)
{
	int32_t index = -1;
	bool equal = false;
	const si_btree_key* key_i = const_key(bt, node, 0);
	const uint8_t* pref = (const uint8_t*)key_i;

	as_arch_prefetch_nt(pref);

	for (uint32_t i = 0; i < node->n_keys; i++) {
		if ((const uint8_t*)key_i >= pref) {
			pref += CACHE_LINE_SZ;
			as_arch_prefetch_nt(pref);
		}

		int32_t rel = skey_cmp(bt, skey, key_i);

		if (rel < 0) {
			break;
		}

		index = (int32_t)i;

		if (rel == 0) {
			equal = true;
			break;
		}

		key_i++;
	}

	return (key_bound){ .index = index, .equal = equal };
}
#else
static key_bound
left_bound(const si_btree* bt, const si_btree_node* node,
		const search_key* skey)
{
	int32_t lower = 0;
	int32_t upper = node->n_keys - 1;
	key_bound bound = { .index = -1, .equal = false };

	while (lower <= upper) {
		int32_t i = (lower + upper) / 2;
		const si_btree_key* key_i = const_key(bt, node, (uint32_t)i);

		as_arch_prefetch_nt(key_i);

		int32_t rel = skey_cmp(bt, skey, key_i);

		if (rel == 0) {
			bound.index = i;
			bound.equal = true;
			break;
		}

		if (rel > 0) {
			bound.index = (int32_t)i;
			lower = i + 1;
		}
		else {
			upper = i - 1;
		}
	}

	return bound;
}
#endif

static void
split_child(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child)
{
	cf_assert(node->n_keys < node->max_degree - 1, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(child->n_keys == child->max_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	// Child's last (min_degree - 1) keys go to new sibling.

	si_arena_handle sibling_h = create_node(bt, child->leaf != 0);
	si_btree_node* sibling = SI_RESOLVE(sibling_h);

	bt->n_nodes++;

	move_keys(bt, sibling, 0, child, child->min_degree,
			(uint32_t)child->min_degree - 1);

	if (child->leaf == 0) {
		move_children(bt, sibling, 0, child, child->min_degree,
				child->min_degree);
	}

	child->n_keys = (uint16_t)(child->n_keys - (child->min_degree - 1));
	sibling->n_keys = (uint16_t)(sibling->n_keys + (sibling->min_degree - 1));

	// Make room in parent for new sibling at (i + 1).

	move_keys(bt, node, i + 1, node, i, node->n_keys - i);
	move_children(bt, node, i + 2, node, i + 1,
			(uint32_t)node->n_keys + 1 - (i + 1));

	node->n_keys++;

	// Attach new sibling to parent using child's last key.

	move_keys(bt, node, i, child, (uint32_t)child->n_keys - 1, 1);

	set_child(bt, node, i + 1, sibling_h);

	child->n_keys--;
}

static void
merge_children(si_btree* bt, si_btree_node* node, uint32_t i,
		si_btree_node* left, si_arena_handle right_h)
{
	si_btree_node* root = SI_RESOLVE(bt->root_h);
	si_btree_node* right = SI_RESOLVE(right_h);

	cf_assert(node != root || node->n_keys > 0, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node == root || node->n_keys >= node->min_degree, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(left->leaf == right->leaf, AS_SINDEX, "bad leaf flag");
	cf_assert(left->n_keys == left->min_degree - 1, AS_SINDEX,
			"bad key count: %d", left->n_keys);
	cf_assert(right->n_keys == right->min_degree - 1, AS_SINDEX,
			"bad key count: %d", right->n_keys);

	// Append separating key to sibling.

	move_keys(bt, left, left->n_keys, node, i, 1);

	left->n_keys++;

	// Append child to sibling.

	move_keys(bt, left, left->n_keys, right, 0, right->n_keys);

	if (left->leaf == 0) {
		move_children(bt, left, left->n_keys, right, 0,
				(uint32_t)right->n_keys + 1);
	}

	left->n_keys = (uint16_t)(left->n_keys + right->n_keys);

	// Drop separating key.

	move_keys(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	move_children(bt, node, i + 1, node, i + 2,
			(uint32_t)node->n_keys + 1 - (i + 2));

	node->n_keys--;

	as_sindex_arena_free(bt->si_arena, right_h);

	bt->n_nodes--;
}


//==========================================================
// Local helpers - HLL.
// TODO - use refactored hmh lib code from particle_hll.c?
//

static void
hll_add(hyperloglog* hll, const uint8_t* buf, size_t buf_sz)
{
	uint16_t register_ix;
	uint64_t new_value;

	hll_hash(buf, buf_sz, &register_ix, &new_value);

	if (new_value > hll_get_register(hll, register_ix)) {
		hll_set_register(hll, register_ix, new_value);
	}
}

static uint64_t
hll_estimate_cardinality(const hyperloglog* hll)
{
	uint32_t c[HLL_MAX_VALUE + 1] = { 0 }; // q_bits + 1

	for (uint32_t r = 0; r < HLL_N_REGISTERS; r++) {
		c[hll_get_register(hll, r)]++;
	}

	double z = HLL_N_REGISTERS *
			hll_tau(c[HLL_MAX_VALUE] / (double)HLL_N_REGISTERS);

	for (uint32_t k = HLL_MAX_VALUE; k > 0; k--) {
		z = 0.5 * (z + c[k]);
	}

	z += HLL_N_REGISTERS * hll_sigma(c[0] / (double)HLL_N_REGISTERS);

	return (uint64_t)
			(llroundl(HLL_ALPHA * HLL_N_REGISTERS * HLL_N_REGISTERS / z));
}

static void
hll_hash(const uint8_t* ele, size_t ele_sz, uint16_t* register_ix,
		uint64_t* value)
{
	uint8_t hash[16];

	murmurHash3_x64_128(ele, ele_sz, hash);

	uint64_t* hash_64 = (uint64_t*)hash;
	uint16_t index_mask = (uint16_t)(HLL_N_REGISTERS - 1);

	*register_ix = (uint16_t)hash_64[0] & index_mask;
	*value = (uint64_t)(cf_lsb64(hash_64[1]) + 1);
}

static uint64_t
hll_get_register(const hyperloglog* hll, uint32_t r) {
	uint32_t bit_offset = HLL_BITS * r;
	uint32_t byte_offset = bit_offset / 8;
	uint32_t bit_end = HLL_BITS * HLL_N_REGISTERS;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;

	uint32_t l_bit = bit_offset % 8;

	if (byte_offset > max_offset) {
		l_bit += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	const uint64_t* dwords = (const uint64_t*)(hll->registers + byte_offset);
	uint64_t value = cf_swap_from_be64(dwords[0]);
	uint32_t shift_bits = 64 - (l_bit + HLL_BITS);

	uint64_t mask = HLL_MAX_VALUE - 1;

	return (value >> shift_bits) & mask;
}

static void
hll_set_register(hyperloglog* hll, uint32_t r, uint64_t value) {
	uint32_t bit_offset = HLL_BITS * r;
	uint32_t byte_offset = bit_offset / 8;
	uint32_t bit_end = HLL_BITS * HLL_N_REGISTERS;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;

	uint32_t shift_bits = bit_offset % 8;

	if (byte_offset > max_offset) {
		shift_bits += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	uint64_t mask = (uint64_t)(HLL_MAX_VALUE - 1) << (64 - HLL_BITS);

	mask >>= shift_bits;
	value <<= (64 - HLL_BITS);
	value >>= shift_bits;

	uint64_t* dwords = (uint64_t*)(hll->registers + byte_offset);
	uint64_t dword = cf_swap_from_be64(dwords[0]);

	dwords[0] = cf_swap_to_be64((dword & ~mask) | value);
}

static double
hll_tau(double val)
{
	if (val == 0.0 || val == 1.0) {
		return 0.0;
	}

	double z_prime;
	double y = 1.0;
	double z = 1 - val;

	do {
		val = sqrt(val);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - val, 2) * y;
	} while (z_prime != z);

	return z / 3;
}

static double
hll_sigma(double val)
{
	if (val == 1.0) {
		return INFINITY;
	}

	double z_prime;
	double y = 1;
	double z = val;

	do {
		val *= val;
		z_prime = z;
		z += val * y;
		y += y;
	} while (z_prime != z);

	return z;
}
