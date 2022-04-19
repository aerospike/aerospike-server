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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <xmmintrin.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "log.h"
#include "xmem.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "query/query_job.h"
#include "sindex/secondary_index.h"

//#include "warnings.h"

#pragma GCC diagnostic warning "-Wcast-qual"
#pragma GCC diagnostic warning "-Wcast-align"

#define BINARY_SEARCH 0


//==========================================================
// Typedefs & constants.
//

#define BTREE_ORDER 32 // TODO - constant for now - but what?

#define N_KEYS_SZ 10

typedef struct si_btree_node_s {
	uint16_t n_keys : N_KEYS_SZ;
	uint16_t leaf : 1;
	uint16_t : 5;
} si_btree_node;

#define CACHE_LINE_SZ 64

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

	search_key last;
} query_collect_cb_info;

#define MAX_GC_BURST 1000


//==========================================================
// Forward declarations.
//

static void gc_reduce_and_delete(as_sindex* si, si_btree* bt);
static bool gc_collect_cb(const si_btree_key* key, void* udata);
static void query_reduce(si_btree* bt, as_partition_reservation* rsv, int64_t start_bval, int64_t end_bval, int64_t resume_bval, cf_digest* keyd, as_sindex_reduce_fn cb, void* udata);
static bool query_collect_cb(const si_btree_key* key, void* udata);

static si_btree* si_btree_create(cf_arenax* arena, bool unsigned_bvals, uint32_t order);
static void si_btree_destroy(si_btree* bt);
static bool si_btree_put(si_btree* bt, const si_btree_key* key);
static bool si_btree_delete(si_btree* bt, const si_btree_key* key);

static void btree_destroy(si_btree* bt, si_btree_node* node);
static bool btree_put(si_btree* bt, si_btree_node* node, const si_btree_key* key);
static bool btree_delete(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out);
static bool btree_reduce(si_btree* bt, si_btree_node* node, const search_key* start_skey, const search_key* end_skey, si_btree_reduce_fn cb, void* udata);
static bool delete_case_1(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out);
static bool delete_case_2(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, key_bound bound);
static bool delete_case_2a(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static bool delete_case_2b(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static bool delete_case_2c(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* left, si_btree_node* right);
static bool delete_case_3(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, key_bound bound);
static bool delete_case_3a_left(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static bool delete_case_3a_right(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static bool delete_case_3b_left(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static bool delete_case_3b_right(si_btree* bt, si_btree_node* node, key_mode mode, const si_btree_key* key_in, si_btree_key* key_out, uint32_t i, si_btree_node* child, si_btree_node* sibling);
static si_btree_node* create_node(const si_btree* bt, bool leaf);
static void move_keys(const si_btree* bt, si_btree_node* dst, uint32_t dst_i, si_btree_node* src, uint32_t src_i, uint32_t n_keys);
static void move_children(const si_btree* bt, si_btree_node* dst, uint32_t dst_i, si_btree_node* src, uint32_t src_i, uint32_t n_children);
static key_bound greatest_lower_bound(const si_btree* bt, const si_btree_node* node, const si_btree_key* key);
static key_bound left_bound(const si_btree* bt, const si_btree_node* node, const search_key* skey);
static void split_child(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* child);
static void merge_children(si_btree* bt, si_btree_node* node, uint32_t i, si_btree_node* left, si_btree_node* right);


//==========================================================
// Inlines & macros.
//

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

#define N_KEYS(n) (uint32_t)((n) & ((1 << N_KEYS_SZ) - 1))

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

static inline si_btree_node**
mut_children(const si_btree* bt, si_btree_node* node)
{
	return (si_btree_node**)((uint8_t*)node + bt->children_off);
}

static inline si_btree_node* const*
const_children(const si_btree* bt, const si_btree_node* node)
{
	return (si_btree_node* const*)((const uint8_t*)node + bt->children_off);
}

static inline void
set_child(const si_btree* bt, si_btree_node* node, uint32_t i,
		si_btree_node* child)
{
	si_btree_node** node_children = mut_children(bt, node);

	node_children[i] = child;
}


//==========================================================
// Public API.
//

void
as_sindex_tree_create(as_sindex* si)
{
	si->imd->btrees = cf_malloc(si->imd->n_pimds * sizeof(si_btree*));

	bool unsigned_bvals = si->imd->sktype == COL_TYPE_GEOJSON;

	for (uint32_t ix = 0; ix < si->imd->n_pimds; ix++) {
		si->imd->btrees[ix] = si_btree_create(si->ns->arena, unsigned_bvals,
				BTREE_ORDER);

		as_add_uint64(&si->ns->n_bytes_sindex_memory,
				(int64_t)si->imd->btrees[ix]->size);
	}
}

void
as_sindex_tree_destroy(as_sindex* si)
{
	for (uint32_t ix = 0; ix < si->imd->n_pimds; ix++) {
		as_add_uint64(&si->ns->n_bytes_sindex_memory,
				-(int64_t)si->imd->btrees[ix]->size);

		si_btree_destroy(si->imd->btrees[ix]);
	}

	cf_free(si->imd->btrees);
	si->imd->btrees = NULL;
}

uint64_t
as_sindex_tree_n_keys(const as_sindex* si)
{
	uint64_t n_keys = 0;

	for (uint32_t ix = 0; ix < si->imd->n_pimds; ix++) {
		n_keys += si->imd->btrees[ix]->n_keys;
	}

	return n_keys;
}

uint64_t
as_sindex_tree_mem_size(const as_sindex* si)
{
	uint64_t sz = 0;

	for (uint32_t ix = 0; ix < si->imd->n_pimds; ix++) {
		sz += si->imd->btrees[ix]->size;
	}

	return sz;
}

void
as_sindex_tree_gc(as_sindex* si)
{
	for (uint32_t ix = 0; ix < si->imd->n_pimds; ix++) {
		gc_reduce_and_delete(si, si->imd->btrees[ix]);
	}
}

bool
as_sindex_tree_put(as_sindex* si, int64_t bval, cf_arenax_handle r_h)
{
	as_index* r = cf_arenax_resolve(si->ns->arena, r_h);
	si_btree* bt = si->imd->btrees[as_partition_getid(&r->keyd)];

	si_btree_key key = {
			.bval = bval,
			.keyd_stub = get_keyd_stub(&r->keyd),
			.r_h = r_h
	};

	int64_t sz = (int64_t)bt->size;

	bool rv = si_btree_put(bt, &key);

	if ((int64_t)bt->size != sz) {
		as_add_uint64(&si->ns->n_bytes_sindex_memory, (int64_t)bt->size - sz);
	}

	return rv;
}

bool
as_sindex_tree_delete(as_sindex* si, int64_t bval, cf_arenax_handle r_h)
{
	as_index* r = cf_arenax_resolve(si->ns->arena, r_h);
	si_btree* bt = si->imd->btrees[as_partition_getid(&r->keyd)];

	si_btree_key key = {
			.bval = bval,
			.keyd_stub = get_keyd_stub(&r->keyd),
			.r_h = r_h
	};

	int64_t sz = (int64_t)bt->size;

	bool rv = si_btree_delete(bt, &key);

	if ((int64_t)bt->size != sz) {
		as_add_uint64(&si->ns->n_bytes_sindex_memory, (int64_t)bt->size - sz);
	}

	return rv;
}

void
as_sindex_tree_query(as_sindex* si, const as_query_range* range,
		as_partition_reservation* rsv, int64_t bval, cf_digest* keyd,
		as_sindex_reduce_fn cb, void* udata)
{
	si_btree* bt = si->imd->btrees[rsv->p->id];

	// TODO - better way to tell geo query/sindex?
	if (si->imd->sktype == COL_TYPE_GEOJSON && range->isrange) {
		for (uint32_t r_ix = 0; r_ix < range->u.geo.num_r; r_ix++) {
			as_query_range_start_end* r = &range->u.geo.r[r_ix];

			if (keyd != NULL && (uint64_t)bval > (uint64_t)r->end) {
				continue;
			}

			query_reduce(bt, rsv, r->start, r->end, bval, keyd, cb, udata);
		}

		return;
	}

	query_reduce(bt, rsv, range->u.r.start, range->u.r.end, bval, keyd, cb,
			udata);
}


//==========================================================
// Local helpers - reduce utilities.
//

void
gc_reduce_and_delete(as_sindex* si, si_btree* bt)
{
	as_namespace* ns = si->ns;

	bool first = true;
	si_btree_key keys[MAX_GC_BURST];

	gc_collect_cb_info ci = {
			.ns = ns,
			.keys = keys
	};

	while (true) {
		search_key* last = first ? NULL : &ci.last;

		si_btree_reduce(bt, last, NULL, gc_collect_cb, &ci);

		first = false;

		for (uint32_t i = 0; i < ci.n_keys; i++) {
			si_btree_delete(bt, &keys[i]);
		}

		si->n_defrag_records += ci.n_keys;
		ns->n_sindex_gc_cleaned += ci.n_keys;

		if (ci.n_keys_reduced != MAX_GC_BURST) {
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

	if (++ci->n_keys_reduced == MAX_GC_BURST) {
		ci->last.bval = key->bval;
		ci->last.has_digest = true;
		ci->last.keyd_stub = get_keyd_stub(&r->keyd);
		ci->last.keyd = r->keyd;

		return false; // stops si_btree_reduce()
	}

	return true;
}

static void
query_reduce(si_btree* bt, as_partition_reservation* rsv, int64_t start_bval,
		int64_t end_bval, int64_t resume_bval, cf_digest* keyd,
		as_sindex_reduce_fn cb, void* udata)
{
	as_namespace* ns = rsv->ns;

	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		query_reduce_no_rc(bt, rsv, start_bval, end_bval, resume_bval, keyd, cb,
				udata);
		return;
	}

	si_btree_key keys[MAX_QUERY_BURST];

	query_collect_cb_info ci = {
			.arena = bt->arena,
			.tree = rsv->tree,
			.keys = keys,
			.last = { .bval = start_bval }
	};

	if (keyd != NULL && (bt->unsigned_bvals ?
			(uint64_t)resume_bval >= (uint64_t)start_bval :
			resume_bval >= start_bval)) {
		ci.last.bval = resume_bval;
		ci.last.has_digest = true;
		ci.last.keyd_stub = get_keyd_stub(keyd);
		ci.last.keyd = *keyd;
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

	if (r->tree_id == tree->id && r->generation != 0) {
		if (r->rc > MAX_QUERY_BURST * 1024) {
			cf_crash(AS_SINDEX, "unexpected - query rc %hu", r->rc);
		}

		cf_mutex* rlock = as_index_rlock_from_keyd(tree, &r->keyd);

		cf_mutex_lock(rlock);

		if (r->generation != 0) {
			as_index_reserve(r);
			ci->keys[ci->n_keys++] = *key;
		}

		cf_mutex_unlock(rlock);
	}

	if (++ci->n_keys_reduced == MAX_QUERY_BURST) {
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
si_btree_create(cf_arenax* arena, bool unsigned_bvals, uint32_t order)
{
	cf_assert(order % 2 == 0, AS_SINDEX, "odd B-tree order: %u", order);

	si_btree* bt = cf_calloc(1, sizeof(si_btree));

	pthread_rwlockattr_t rwattr;

	pthread_rwlockattr_init(&rwattr);
	pthread_rwlockattr_setkind_np(&rwattr,
			PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

	pthread_rwlock_init(&bt->lock, &rwattr);

	bt->arena = arena;
	bt->unsigned_bvals = unsigned_bvals;

	bt->min_degree = order / 2;
	bt->max_degree = order;

	bt->keys_off = sizeof(si_btree_node);
	bt->children_off = bt->keys_off +
			(order - 1) * (uint32_t)sizeof(si_btree_key);

	bt->inner_sz = bt->children_off + order * (uint32_t)sizeof(si_btree_node*);
	bt->leaf_sz = bt->children_off;

	bt->root = create_node(bt, true);
	bt->n_nodes = 1;
	bt->n_keys = 0;
	bt->size = sizeof(si_btree) + bt->leaf_sz;

	return bt;
}

static void
si_btree_destroy(si_btree* bt)
{
	btree_destroy(bt, bt->root);
	pthread_rwlock_destroy(&bt->lock);
	cf_free(bt);
}

static bool
si_btree_put(si_btree* bt, const si_btree_key* key)
{
	pthread_rwlock_wrlock(&bt->lock);

	if (bt->root->n_keys == bt->max_degree - 1) {
		si_btree_node* root = create_node(bt, false);

		bt->n_nodes++;
		bt->size += bt->inner_sz;

		set_child(bt, root, 0, bt->root);
		split_child(bt, root, 0, bt->root);

		bt->root = root;
	}

	if (! btree_put(bt, bt->root, key)) {
		pthread_rwlock_unlock(&bt->lock);
		return false;
	}

	bt->n_keys++;

	pthread_rwlock_unlock(&bt->lock);
	return true;
}

static bool
si_btree_delete(si_btree* bt, const si_btree_key* key)
{
	pthread_rwlock_wrlock(&bt->lock);

	if (! btree_delete(bt, bt->root, KEY_MODE_MATCH, key, NULL)) {
		pthread_rwlock_unlock(&bt->lock);
		return false;
	}

	bt->n_keys--;

	si_btree_node* root = bt->root;

	if (root->n_keys == 0 && root->leaf == 0) {
		bt->root = const_children(bt, root)[0];

		cf_free(root);

		bt->n_nodes--;
		bt->size -= bt->inner_sz;
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

	btree_reduce(bt, bt->root, start_skey, end_skey, cb, udata);

	pthread_rwlock_unlock(&bt->lock);
}


//==========================================================
// Local helpers - lowest btree layer.
//

static void
btree_destroy(si_btree* bt, si_btree_node* node)
{
	if (node->leaf == 0) {
		si_btree_node** children = mut_children(bt, node);

		for (uint32_t i = 0; i <= node->n_keys; i++) {
			btree_destroy(bt, children[i]);
		}
	}

	cf_free(node);
}

static bool
btree_put(si_btree* bt, si_btree_node* node, const si_btree_key* key)
{
	cf_assert(node->n_keys < bt->max_degree - 1, AS_SINDEX, "bad key count: %d",
			node->n_keys);

	key_bound bound = greatest_lower_bound(bt, node, key);

	if (bound.equal) {
		si_btree_key* node_key = mut_key(bt, node, bound.index);

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

	si_btree_node** children = mut_children(bt, node);

	if (children[i]->n_keys == bt->max_degree - 1) {
		split_child(bt, node, i, children[i]);

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
		}
	}

	return btree_put(bt, children[i], key);
}

static bool
btree_delete(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out)
{
	cf_assert(mode != KEY_MODE_MATCH || (key_in != NULL && key_out == NULL),
			AS_SINDEX, "bad arguments");
	cf_assert(mode == KEY_MODE_MATCH || (key_in == NULL && key_out != NULL),
			AS_SINDEX, "bad arguments");

	cf_assert(node == bt->root || node->n_keys >= bt->min_degree, AS_SINDEX,
			"bad key count: %d", node->n_keys);

	if (node == bt->root && node->n_keys == 0) {
		return false;
	}

	if (node->leaf != 0) {
		return delete_case_1(bt, node, mode, key_in, key_out);
	}

	key_bound bound;

	switch (mode) {
	case KEY_MODE_MATCH:
		bound = greatest_lower_bound(bt, node, key_in);
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
	si_btree_node* const* children = node->leaf == 0 ?
			const_children(bt, node) : NULL;

	if (children != NULL &&
			! btree_reduce(bt, children[i], start_skey, end_skey, cb, udata)) {
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
				! btree_reduce(bt, children[i], NULL, end_skey, cb, udata)) {
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
	si_btree_node** children = mut_children(bt, node);
	si_btree_node* left = children[i];

	if (left->n_keys >= bt->min_degree) {
		return delete_case_2a(bt, node, i, left);
	}

	si_btree_node* right = children[i + 1];

	if (right->n_keys >= bt->min_degree) {
		return delete_case_2b(bt, node, i, right);
	}

	return delete_case_2c(bt, node, mode, key_in, key_out, i, left, right);
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
		si_btree_node* left, si_btree_node* right)
{
	merge_children(bt, node, i, left, right);

	return btree_delete(bt, left, mode, key_in, key_out);
}

static bool
delete_case_3(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, key_bound bound)
{
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(! bound.equal, AS_SINDEX, "bad equality flag");

	uint32_t i = (uint32_t)(bound.index + 1);
	si_btree_node** children = mut_children(bt, node);
	si_btree_node* child = children[i];

	if (child->n_keys >= bt->min_degree) {
		return btree_delete(bt, child, mode, key_in, key_out);
	}

	cf_assert(child->n_keys == bt->min_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	if (i > 0) {
		si_btree_node* sibling = children[i - 1];

		if (sibling->n_keys >= bt->min_degree) {
			return delete_case_3a_left(bt, node, mode, key_in, key_out, i,
					child, sibling);
		}

		cf_assert(sibling->n_keys == bt->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i < node->n_keys) {
		si_btree_node* sibling = children[i + 1];

		if (sibling->n_keys >= bt->min_degree) {
			return delete_case_3a_right(bt, node, mode, key_in, key_out, i,
					child, sibling);
		}

		cf_assert(sibling->n_keys == bt->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i > 0) {
		si_btree_node* sibling = children[i - 1];

		return delete_case_3b_left(bt, node, mode, key_in, key_out, i, child,
				sibling);
	}

	si_btree_node* sibling = children[i + 1];

	return delete_case_3b_right(bt, node, mode, key_in, key_out, i, child,
			sibling);
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
		si_btree_node* child, si_btree_node* sibling)
{
	merge_children(bt, node, i - 1, sibling, child);

	return btree_delete(bt, sibling, mode, key_in, key_out);
}

static bool
delete_case_3b_right(si_btree* bt, si_btree_node* node, key_mode mode,
		const si_btree_key* key_in, si_btree_key* key_out, uint32_t i,
		si_btree_node* child, si_btree_node* sibling)
{
	merge_children(bt, node, i, child, sibling);

	return btree_delete(bt, child, mode, key_in, key_out);
}

static si_btree_node*
create_node(const si_btree* bt, bool leaf)
{
	si_btree_node* node = cf_calloc(1, leaf ? bt->leaf_sz : bt->inner_sz);

	node->n_keys = 0;
	node->leaf = leaf ? 1 : 0;

	return node;
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
	si_btree_node** dst_children = mut_children(bt, dst);
	si_btree_node* const* src_children = const_children(bt, src);

	memmove(&dst_children[dst_i], &src_children[src_i],
			n_children * sizeof(si_btree_node*));
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

	_mm_prefetch(pref, _MM_HINT_NTA);

	for (uint32_t i = 0; i < node->n_keys; i++) {
		if ((const uint8_t*)key_i >= pref) {
			pref += CACHE_LINE_SZ;
			_mm_prefetch(pref, _MM_HINT_NTA);
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

		_mm_prefetch(key_i, _MM_HINT_NTA);

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

	_mm_prefetch(pref, _MM_HINT_NTA);

	for (uint32_t i = 0; i < node->n_keys; i++) {
		if ((const uint8_t*)key_i >= pref) {
			pref += CACHE_LINE_SZ;
			_mm_prefetch(pref, _MM_HINT_NTA);
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

		_mm_prefetch(key_i, _MM_HINT_NTA);

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
	cf_assert(node->n_keys < bt->max_degree - 1, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(child->n_keys == bt->max_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	// Child's last (min_degree - 1) keys go to new sibling.

	si_btree_node* sibling = create_node(bt, child->leaf != 0);

	bt->n_nodes++;
	bt->size += child->leaf == 0 ? bt->inner_sz : bt->leaf_sz;

	move_keys(bt, sibling, 0, child, bt->min_degree, bt->min_degree - 1);

	if (child->leaf == 0) {
		move_children(bt, sibling, 0, child, bt->min_degree, bt->min_degree);
	}

	child->n_keys = N_KEYS(child->n_keys - (bt->min_degree - 1));
	sibling->n_keys = N_KEYS(sibling->n_keys + (bt->min_degree - 1));

	// Make room in parent for new sibling at (i + 1).

	move_keys(bt, node, i + 1, node, i, node->n_keys - i);
	move_children(bt, node, i + 2, node, i + 1,
			(uint32_t)node->n_keys + 1 - (i + 1));

	node->n_keys++;

	// Attach new sibling to parent using child's last key.

	move_keys(bt, node, i, child, (uint32_t)child->n_keys - 1, 1);

	set_child(bt, node, i + 1, sibling);

	child->n_keys--;
}

static void
merge_children(si_btree* bt, si_btree_node* node, uint32_t i,
		si_btree_node* left, si_btree_node* right)
{
	cf_assert(node != bt->root || node->n_keys > 0, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node == bt->root || node->n_keys >= bt->min_degree, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(left->leaf == right->leaf, AS_SINDEX, "bad leaf flag");
	cf_assert(left->n_keys == bt->min_degree - 1, AS_SINDEX,
			"bad key count: %d", left->n_keys);
	cf_assert(right->n_keys == bt->min_degree - 1, AS_SINDEX,
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

	left->n_keys = N_KEYS(left->n_keys + right->n_keys);

	// Drop separating key.

	move_keys(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	move_children(bt, node, i + 1, node, i + 2,
			(uint32_t)node->n_keys + 1 - (i + 2));

	node->n_keys--;

	cf_free(right);

	bt->n_nodes--;
	bt->size -= left->leaf == 0 ? bt->inner_sz : bt->leaf_sz;
}
