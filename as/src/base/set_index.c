/*
 * set_index.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include "base/set_index.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "sindex/gc.h"

//#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef struct stack_ele_s {
	struct stack_ele_s* parent;
	uarena_handle me_h;
	index_ele* me;
} stack_ele;

typedef struct populate_info_s {
	as_namespace* ns;
	as_set* p_set;
	uint16_t set_id;
	uint32_t pid;
} populate_info;

typedef struct populate_cb_info_s {
	as_namespace* ns;
	as_set* p_set;
	uint16_t set_id;
	as_index_tree* tree;
	as_set_index_tree* stree;
} populate_cb_info;

#define N_POPULATE_THREADS 4

//--------------------------------------
// uarena constants.
//

#define STAGES_STEP 8
#define STAGE_CAPACITY (1 << ELE_ID_N_BITS) // 256
#define STAGE_SIZE (STAGE_CAPACITY * ELE_SIZE) // 4K


//==========================================================
// Globals.
//

cf_mutex g_balance_lock = CF_MUTEX_INIT;

static cf_queue g_populate_q;


//==========================================================
// Forward declarations.
//

static inline bool is_set_indexed(const as_namespace* ns, uint16_t set_id);
static inline bool is_set_populated(const as_namespace* ns, uint16_t set_id);

static int populate_q_reduce_cb(void* buf, void* udata);
static void* run_populate_q(void* udata);
static void* run_populate(void* udata);
static bool populate_reduce_cb(as_index_ref* r_ref, void* udata);

static inline as_set_index_tree* stree_create(void);
static inline void stree_destroy(as_set_index_tree* stree);
static inline as_set_index_tree* stree_reserve(as_index_tree* tree, uint16_t set_id);
static inline void stree_release(as_set_index_tree* stree);

static inline void ssi_from_keyd(as_index_tree* tree, as_set_index_tree* stree, const cf_digest* keyd, ssprig_info* ssi);
static inline uint32_t ssprig_i_from_keyd(const cf_digest* keyd);
static inline uint32_t stub_from_keyd(const cf_digest* keyd);
static inline void ssri_from_ssprig_i(as_index_tree* tree, as_set_index_tree* stree, const cf_digest* keyd, uint32_t keyd_stub, uint32_t ssprig_i, ssprig_reduce_info* ssri);

static void ssprig_delete(ssprig_info* ssi);
static bool ssprig_reduce(ssprig_reduce_info* ssri, as_index_reduce_fn cb, void* udata);
static void ssprig_traverse(ssprig_reduce_info* ssri, uarena_handle r_h, as_index_ph_array* ph_a);

static void insert_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele);
static void delete_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele);
static void rotate_left(stack_ele* a, stack_ele* b);
static void rotate_right(stack_ele* a, stack_ele* b);

//--------------------------------------
// uarena API.
//

static void uarena_init(uarena* ua);
static void uarena_destroy(uarena* ua);
static void uarena_add_stage(uarena* ua);
static uarena_handle uarena_alloc(uarena* ua);
static void uarena_free(uarena* ua, uarena_handle h);


//==========================================================
// Inlines & macros.
//

//--------------------------------------
// uarena API.
//

#define NEXT_FREE_H(_ua, _h) (*(uarena_handle*)uarena_resolve(_ua, _h))

static inline void
uarena_set_handle(uarena_handle* h, uint32_t stage_id, uint32_t ele_id)
{
	*h = (stage_id << ELE_ID_N_BITS) | ele_id;
}


//==========================================================
// Public API - startup.
//

void
as_set_index_init(void)
{
	cf_queue_init(&g_populate_q, sizeof(populate_info), 4, true);

	cf_thread_create_detached(run_populate_q, NULL);
}


//==========================================================
// Public API - set-index tree lifecycle.
//

// May be under partition lock.
void
as_set_index_create_all(as_namespace* ns, as_index_tree* tree)
{
	uint32_t n_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint16_t set_id = 1; set_id <= n_sets; set_id++) {
		if (is_set_indexed(ns, set_id)) {
			tree->set_trees[set_id] = stree_create();
		}
	}
}

void
as_set_index_destroy_all(as_index_tree* tree)
{
	for (uint32_t set_id = 1; set_id <= AS_SET_MAX_COUNT; set_id++) {
		as_set_index_tree* stree = tree->set_trees[set_id];

		if (stree != NULL) {
			// TODO - paranoia - remove or simplify eventually.
			int32_t rc = cf_rc_count(stree);
			cf_assert(rc == 1, AS_INDEX, "bad stree rc %d id %u", rc, set_id);

			stree_destroy(stree); // ok to directly destroy stree
		}
	}

	cf_mutex_destroy(&tree->set_trees_lock);
}

void
as_set_index_tree_create(as_index_tree* tree, uint16_t set_id)
{
	tree->set_trees[set_id] = stree_create();
}

void
as_set_index_tree_destroy(as_index_tree* tree, uint16_t set_id)
{
	cf_mutex_lock(&tree->set_trees_lock);

	as_set_index_tree* stree = tree->set_trees[set_id];

	tree->set_trees[set_id] = NULL;
	stree_release(stree);

	cf_mutex_unlock(&tree->set_trees_lock);
}

void
as_set_index_balance_lock(void)
{
	cf_mutex_lock(&g_balance_lock);
}

void
as_set_index_balance_unlock(void)
{
	cf_mutex_unlock(&g_balance_lock);
}


//==========================================================
// Public API - transactions.
//

void
as_set_index_insert(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		uint64_t r_h)
{
	if (! is_set_indexed(ns, set_id)) {
		return;
	}

	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return;
	}

	as_index* r = cf_arenax_resolve(tree->shared->arena, r_h);
	ssprig_info ssi;

	ssi_from_keyd(tree, stree, &r->keyd, &ssi);

	if (! ssprig_insert(&ssi, r_h)) {
		cf_warning(AS_INDEX, "insert found existing element - unexpected");
	}

	stree_release(stree);
}

void
as_set_index_delete(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		uint64_t r_h)
{
	if (! is_set_indexed(ns, set_id)) {
		return;
	}

	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return;
	}

	as_index* r = cf_arenax_resolve(tree->shared->arena, r_h);
	ssprig_info ssi;

	ssi_from_keyd(tree, stree, &r->keyd, &ssi);
	ssprig_delete(&ssi);

	stree_release(stree);
}

void
as_set_index_delete_live(as_namespace* ns, as_index_tree* tree, as_record* r,
		uint64_t r_h)
{
	if (as_record_is_live(r)) {
		as_set_index_delete(ns, tree, as_index_get_set_id(r), r_h);
	}
}

bool
as_set_index_reduce(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		cf_digest* keyd, as_index_reduce_fn cb, void* udata)
{
	if (! is_set_populated(ns, set_id)) {
		return false;
	}

	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return false;
	}

	uint32_t start_sprig_i;
	uint32_t keyd_stub;

	if (keyd == NULL) {
		start_sprig_i = N_SET_SPRIGS - 1;
		keyd_stub = 0;
	}
	else {
		start_sprig_i = ssprig_i_from_keyd(keyd);
		keyd_stub = stub_from_keyd(keyd);
	}

	for (int i = (int)start_sprig_i; i >= 0; i--, keyd = NULL) {
		// Very common to encounter empty sprigs - check optimistically.
		if (stree->roots[i] == SENTINEL_H) {
			continue;
		}

		ssprig_reduce_info ssri;
		ssri_from_ssprig_i(tree, stree, keyd, keyd_stub, (uint32_t)i, &ssri);

		if (tree->shared->puddles_offset == 0) {
			if (! ssprig_reduce(&ssri, cb, udata)) {
				break; // don't care why it finished reducing
			}
		}
		else {
			if (! ssprig_reduce_no_rc(tree, &ssri, cb, udata)) {
				break; // don't care why it finished reducing
			}
		}
	}

	stree_release(stree);

	return true;
}


//==========================================================
// Public API - info & stats.
//

void
as_set_index_enable(as_namespace* ns, as_set* p_set, uint16_t set_id)
{
	if (p_set->index_enabled) {
		return;
	}

	// Ensure either rebalance or this call creates the strees.
	cf_mutex_lock(&g_balance_lock);

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_create_set_index(ns, pid, set_id);
	}

	p_set->index_populating = true;
	// TODO - non-x86 would need memory barrier here.
	p_set->index_enabled = true;

	cf_mutex_unlock(&g_balance_lock);

	if (p_set->n_objects == 0) {
		p_set->index_populating = false;

		cf_info(AS_INDEX, "{%s|%s} done populating set-index (0 ms)", ns->name,
				p_set->name);

		return;
	}

	populate_info popi = {
			.ns = ns,
			.p_set = p_set,
			.set_id = set_id
	};

	cf_queue_push(&g_populate_q, &popi);
}

void
as_set_index_disable(as_namespace* ns, as_set* p_set, uint16_t set_id)
{
	if (! p_set->index_enabled) {
		return;
	}

	// Ensure rebalance won't set NULL strees - destroy assumes they're not.
	cf_mutex_lock(&g_balance_lock);

	p_set->index_enabled = false;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_destroy_set_index(ns, pid, set_id);
	}

	cf_mutex_unlock(&g_balance_lock);

	// If it's still queued to be enabled, remove it from the queue.
	cf_queue_reduce(&g_populate_q, populate_q_reduce_cb, p_set);

	// If we cancelled, ensure cancellation is done (in case we repopulate).
	while (p_set->index_populating) {
		usleep(100);
	}
}

uint64_t
as_set_index_used_bytes(const as_namespace* ns)
{
	uint64_t n_objects = 0;
	uint32_t n_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t set_ix = 0; set_ix < n_sets; set_ix++) {
		as_set* p_set;

		if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
				CF_VMAPX_OK) {
			cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
		}

		if (p_set->index_enabled) {
			n_objects += p_set->n_objects;
		}
	}

	return n_objects * sizeof(index_ele);
}


//==========================================================
// Local helpers - configuration.
//

static inline bool
is_set_indexed(const as_namespace* ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return false;
	}

	uint32_t set_ix = (uint32_t)(set_id - 1);
	as_set* p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
			CF_VMAPX_OK) {
		cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
	}

	return p_set->index_enabled;
}

static inline bool
is_set_populated(const as_namespace* ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return false;
	}

	uint32_t set_ix = (uint32_t)(set_id - 1);
	as_set* p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
			CF_VMAPX_OK) {
		cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
	}

	return p_set->index_enabled && ! p_set->index_populating;
}


//==========================================================
// Local helpers - population.
//

static int
populate_q_reduce_cb(void* buf, void* udata)
{
	as_set* p_set = (as_set*)udata;

	if (((populate_info*)buf)->p_set == p_set) {
		p_set->index_populating = false;
		return -2;
	}

	return 0;
}

static void*
run_populate_q(void* udata)
{
	while (true) {
		populate_info popi;

		cf_queue_pop(&g_populate_q, &popi, CF_QUEUE_FOREVER);

		cf_info(AS_INDEX, "{%s|%s} start populating set-index ...",
				popi.ns->name, popi.p_set->name);

		uint64_t start_ms = cf_getms();

		cf_tid tids[N_POPULATE_THREADS];

		for (uint32_t n = 0; n < N_POPULATE_THREADS; n++) {
			tids[n] = cf_thread_create_joinable(run_populate, &popi);
		}

		for (uint32_t n = 0; n < N_POPULATE_THREADS; n++) {
			cf_thread_join(tids[n]);
		}

		popi.p_set->index_populating = false;

		cf_info(AS_INDEX, "{%s|%s} done populating set-index (%lu ms)",
				popi.ns->name, popi.p_set->name, cf_getms() - start_ms);
	}

	return NULL;
}

static void*
run_populate(void* udata)
{
	populate_info* popi = (populate_info*)udata;
	uint32_t pid;

	while ((pid = as_faa_uint32(&popi->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(popi->ns, pid, &rsv);

		if (rsv.tree == NULL) {
			as_partition_release(&rsv);
			continue;
		}

		// Populate can be cancelled abruptly - check each step for that.

		as_set_index_tree* stree = stree_reserve(rsv.tree, popi->set_id);

		if (stree == NULL) {
			as_partition_release(&rsv);
			break;
		}

		populate_cb_info cbi = {
				.ns = popi->ns,
				.p_set = popi->p_set,
				.set_id = popi->set_id,
				.tree = rsv.tree,
				.stree = stree
		};

		if (! as_index_reduce_live(rsv.tree, populate_reduce_cb, &cbi)) {
			stree_release(stree);
			as_partition_release(&rsv);
			break;
		}

		stree_release(stree);
		as_partition_release(&rsv);
	}

	return NULL;
}

static bool
populate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	populate_cb_info* cbi = (populate_cb_info*)udata;

	if (! cbi->p_set->index_enabled) {
		as_record_done(r_ref, cbi->ns);
		return false;
	}

	if (as_index_get_set_id(r_ref->r) != cbi->set_id) {
		as_record_done(r_ref, cbi->ns);
		return true;
	}

	ssprig_info ssi;

	ssi_from_keyd(cbi->tree, cbi->stree, &r_ref->r->keyd, &ssi);
	ssprig_insert(&ssi, r_ref->r_h);

	as_record_done(r_ref, cbi->ns);

	return true;
}


//==========================================================
// Local helpers - as_set_index_tree lifecycle.
//

static inline as_set_index_tree*
stree_create(void)
{
	as_set_index_tree* stree = cf_rc_alloc(sizeof(as_set_index_tree));

	memset(stree, 0, sizeof(as_set_index_tree));
	uarena_init(&stree->ua);

	return stree;
}

static inline void
stree_destroy(as_set_index_tree* stree)
{
	uarena_destroy(&stree->ua);
	cf_rc_free(stree);
}

static inline as_set_index_tree*
stree_reserve(as_index_tree* tree, uint16_t set_id)
{
	cf_mutex_lock(&tree->set_trees_lock);

	as_set_index_tree* stree = tree->set_trees[set_id];

	if (stree != NULL) {
		cf_rc_reserve(stree);
	}

	cf_mutex_unlock(&tree->set_trees_lock);

	return stree;
}

static inline void
stree_release(as_set_index_tree* stree)
{
	if (cf_rc_release(stree) == 0) {
		stree_destroy(stree);
	}
}


//==========================================================
// Local helpers - sprig parameter utilities.
//

static inline void
ssi_from_keyd(as_index_tree* tree, as_set_index_tree* stree,
		const cf_digest* keyd, ssprig_info* ssi)
{
	// Get the 8 + 23 most significant non-pid bits in the digest. Note - this
	// is hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	uint32_t bits = (((uint32_t)keyd->digest[1] & 0xF0) << 23) |
			((uint32_t)keyd->digest[2] << 19) |
			((uint32_t)keyd->digest[3] << 11) |
			((uint32_t)keyd->digest[4] << 3) |
			((uint32_t)keyd->digest[5] >> 5);

	uint32_t ssprig_i = bits >> 23;

	ssi->arena = tree->shared->arena;
	ssi->ua = &stree->ua;
	ssi->root = stree->roots + ssprig_i;
	ssi->keyd_stub = bits & ((1 << 23) - 1);
	ssi->keyd = keyd;
}

static inline uint32_t
ssprig_i_from_keyd(const cf_digest* keyd)
{
	// Get the 8 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	return ((uint32_t)keyd->digest[1] & 0xF0) |
			((uint32_t)keyd->digest[2] >> 4);
}

static inline uint32_t
stub_from_keyd(const cf_digest* keyd)
{
	// Get the 23 most significant non-sprig bits in the digest. Note - this
	// is hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	return (((uint32_t)keyd->digest[2] & 0x0F) << 19) |
			((uint32_t)keyd->digest[3] << 11) |
			((uint32_t)keyd->digest[4] << 3) |
			((uint32_t)keyd->digest[5] >> 5);
}

static inline void
ssri_from_ssprig_i(as_index_tree* tree, as_set_index_tree* stree,
		const cf_digest* keyd, uint32_t keyd_stub, uint32_t ssprig_i,
		ssprig_reduce_info* ssri)
{
	ssprig_info* ssi = (ssprig_info*)ssri;

	ssi->arena = tree->shared->arena;
	ssi->ua = &stree->ua;
	ssi->root = stree->roots + ssprig_i;
	ssi->keyd_stub = keyd_stub;
	ssi->keyd = keyd;

	ssri->destructor = tree->shared->destructor;
	ssri->destructor_udata = tree->shared->destructor_udata;
	ssri->olock = &(tree_locks(tree) + ssprig_i)->lock;
}


//==========================================================
// Local helpers - red-black sprigs.
//

// Accessed by enterprise split.
bool
ssprig_insert(ssprig_info* ssi, uint64_t key_r_h)
{
	// Shortcut inserting into empty sprig. May be common for set-indexes.
	if (*ssi->root == SENTINEL_H) {
		uarena_handle n_h = uarena_alloc(ssi->ua);
		index_ele* n = UA_RESOLVE(n_h);

		*n = (index_ele){
				.key_r_h = key_r_h,
				.keyd_stub = ssi->keyd_stub,
				.color = BLACK,
				.left_h = SENTINEL_H,
				.right_h = SENTINEL_H
		};

		*ssi->root = n_h;

		return true;
	}

	int cmp = 0;

	// Use a stack index_ele object for the root's parent, for convenience.
	index_ele root_parent;

	// Save parents as we search for the specified element's insertion point.
	stack_ele eles[64]; // enough for 16M elements per sprig
	stack_ele* ele = eles;

	// Search for the specified element, or a parent to insert it under.

	root_parent.left_h = *ssi->root;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	uarena_handle t_h = *ssi->root;

	while (t_h != SENTINEL_H) {
		index_ele* t = UA_RESOLVE(t_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = t_h;
		ele->me = t;

		// FIXME - is this a bad idea given our index-ele size & arenas?
//		_mm_prefetch(t, _MM_HINT_NTA);

		if ((cmp = ssprig_ele_cmp(ssi, t)) == 0) {
			return false; // element already exists
		}

		t_h = cmp > 0 ? t->left_h : t->right_h;
	}

	// We didn't find the tree element - create a new element and insert it.

	// Save the root so we can detect whether it changes.
	uarena_handle old_root = *ssi->root;

	// Make the new element.
	uarena_handle n_h = uarena_alloc(ssi->ua);
	index_ele* n = UA_RESOLVE(n_h);

	*n = (index_ele){
			.key_r_h = key_r_h,
			.keyd_stub = ssi->keyd_stub,
			.color = RED,
			.left_h = SENTINEL_H,
			.right_h = SENTINEL_H
	};

	// Insert the new element n under parent ele.
	if (ele->me == &root_parent || 0 < cmp) {
		ele->me->left_h = n_h;
	}
	else {
		ele->me->right_h = n_h;
	}

	ele++;
	ele->parent = ele - 1;
	ele->me_h = n_h;
	ele->me = n;

	// Rebalance the sprig as needed.
	insert_rebalance(ssi, &root_parent, ele);

	// If insertion caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		*ssi->root = root_parent.left_h;
	}

	return true;
}

static void
ssprig_delete(ssprig_info* ssi)
{
	index_ele* r;
	uarena_handle r_h;

	// Use a stack index_ele object for the root's parent, for convenience.
	index_ele root_parent;

	// Save parents as we search for the specified element (or its successor).
	stack_ele eles[128]; // enough for 16M elements per sprig
	stack_ele* ele = eles;

	root_parent.left_h = *ssi->root;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	r_h = *ssi->root;

	while (r_h != SENTINEL_H) {
		r = UA_RESOLVE(r_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r_h;
		ele->me = r;

//		_mm_prefetch(r, _MM_HINT_NTA);

		int cmp = ssprig_ele_cmp(ssi, r);

		if (cmp == 0) {
			break; // found, we'll be deleting it
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	if (r_h == SENTINEL_H) {
		return; // element not found - can happen while populating
	}

	// We found the tree element - delete the element.

	// Save the root so we can detect whether it changes.
	uarena_handle old_root = *ssi->root;

	// Snapshot the element to delete, r. (Already have r_h and r shortcuts.)
	stack_ele* r_e = ele;

	if (r->left_h != SENTINEL_H && r->right_h != SENTINEL_H) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = UA_RESOLVE(ele->me_h);

		while (ele->me->left_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = UA_RESOLVE(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	stack_ele* s_e = ele;
	uarena_handle s_h = s_e->me_h;
	index_ele* s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	ele->me_h = s->left_h == SENTINEL_H ? s->right_h : s->left_h;
	ele->me = UA_RESOLVE(ele->me_h);

	// Cut s (remember, it could be r) out of the tree.
	ele->parent = s_e->parent;

	if (s_h == s_e->parent->me->left_h) {
		s_e->parent->me->left_h = ele->me_h;
	}
	else {
		s_e->parent->me->right_h = ele->me_h;
	}

	// Rebalance at ele if necessary. (Note - if r != s, r is in the tree, and
	// its parent may change during rebalancing.)
	if (s->color == BLACK) {
		delete_rebalance(ssi, &root_parent, ele);
	}

	if (s != r) {
		// s was a successor distinct from r, put it in r's place in the tree.
		s->left_h = r->left_h;
		s->right_h = r->right_h;
		s->color = r->color;

		if (r_h == r_e->parent->me->left_h) {
			r_e->parent->me->left_h = s_h;
		}
		else {
			r_e->parent->me->right_h = s_h;
		}
	}

	// If delete caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		*ssi->root = root_parent.left_h;
	}

	uarena_free(ssi->ua, r_h);
}

static bool
ssprig_reduce(ssprig_reduce_info* ssri, as_index_reduce_fn cb, void* udata)
{
	ssprig_info* ssi = (ssprig_info*)ssri;

	cf_mutex_lock(ssri->olock);

	// Very common to encounter empty sprigs - check again under lock.
	if (*ssi->root == SENTINEL_H) {
		cf_mutex_unlock(ssri->olock);
		return true;
	}

	as_index_ph stack_phs[MAX_STACK_PHS];
	as_index_ph_array ph_a = {
			.is_stack = true,
			.capacity = MAX_STACK_PHS,
			.phs = stack_phs
	};

	// Traverse just fills array, then we make callbacks afterwards.
	ssprig_traverse(ssri, *ssi->root, &ph_a);

	cf_mutex_unlock(ssri->olock);

	bool do_more = true;

	for (uint32_t i = 0; i < ph_a.n_used; i++) {
		as_index_ph* ph = &ph_a.phs[i];
		as_index_ref r_ref = {
				.r = ph->r,
				.r_h = ph->r_h,
				.olock = ssri->olock
		};

		cf_mutex_lock(r_ref.olock);

		uint16_t rc = as_index_release(r_ref.r);

		// Ignore this record if it's been deleted.
		if (! as_index_is_valid_record(r_ref.r)) {
			as_namespace* ns = ssri->destructor_udata;

			if (rc == 0) {
				if (ssri->destructor != NULL) {
					ssri->destructor(r_ref.r, ns);
				}

				cf_arenax_free(ssi->arena, r_ref.r_h, NULL);
			}
			else if (r_ref.r->in_sindex == 1 && rc == 1) {
				as_sindex_gc_record(ns, &r_ref);
			}

			cf_mutex_unlock(r_ref.olock);
			continue;
		}

		if (do_more) {
			// Callback MUST call as_record_done() to unlock record.
			do_more = cb(&r_ref, udata);
		}
		else {
			cf_mutex_unlock(r_ref.olock);
		}
	}

	if (! ph_a.is_stack) {
		cf_free(ph_a.phs);
	}

	return do_more;
}

static void
ssprig_traverse(ssprig_reduce_info* ssri, uarena_handle r_h,
		as_index_ph_array* ph_a)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	ssprig_info* ssi = (ssprig_info*)ssri;

	index_ele* r = uarena_resolve(ssi->ua, r_h);
	int cmp = 0; // initialized to satisfy compiler

	if (ssi->keyd == NULL || (cmp = ssprig_ele_cmp(ssi, r)) > 0) {
		ssprig_traverse(ssri, r->left_h, ph_a);
	}

	if (ph_a->n_used == ph_a->capacity) {
		as_index_grow_ph_array(ph_a);
	}

	// We do not collect the element with the boundary digest.

	if (ssi->keyd == NULL || cmp > 0) {
		as_index* key_r = cf_arenax_resolve(ssi->arena, r->key_r_h);

		as_index_reserve(key_r);

		as_index_ph* ph = &ph_a->phs[ph_a->n_used++];

		ph->r = key_r;
		ph->r_h = r->key_r_h;

		ssi->keyd = NULL;
	}

	ssprig_traverse(ssri, r->right_h, ph_a);
}


//==========================================================
// Local helpers - red-black sprig utilities.
//

static void
insert_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	stack_ele* r_e = ele;
	stack_ele* parent_e = r_e->parent;

	while (parent_e->me->color == RED) {
		stack_ele* grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			uarena_handle u_h = grandparent_e->me->right_h;
			index_ele* u = UA_RESOLVE(u_h);

			if (u->color == RED) {
				u->color = BLACK;
				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->right_h) {
					// Save original r, which will become new r's parent.
					stack_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					rotate_left(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				rotate_right(grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			uarena_handle u_h = grandparent_e->me->left_h;
			index_ele* u = UA_RESOLVE(u_h);

			if (u->color == RED) {
				u->color = BLACK;
				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->left_h) {
					// Save original r, which will become new r's parent.
					stack_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					rotate_right(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				rotate_left(grandparent_e, parent_e);
			}
		}
	}

	UA_RESOLVE(root_parent->left_h)->color = BLACK;
}

static void
delete_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	stack_ele* r_e = ele;

	while (r_e->me->color == BLACK && r_e->me_h != root_parent->left_h) {
		index_ele* r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			uarena_handle s_h = r_parent->right_h;
			index_ele* s = UA_RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_left(r_e->parent, ele);

				s_h = r_parent->right_h;
				s = UA_RESOLVE(s_h);
			}

			index_ele* s_left = UA_RESOLVE(s->left_h);
			index_ele* s_right = UA_RESOLVE(s->right_h);

			if (s_left->color == BLACK && s_right->color == BLACK) {
				s->color = RED;

				r_e = r_e->parent;
			}
			else {
				if (s_right->color == BLACK) {
					s_left->color = BLACK;
					s->color = RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					stack_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					rotate_right(s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				UA_RESOLVE(s->right_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_left(r_e->parent, ele);

				UA_RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
		else {
			uarena_handle s_h = r_parent->left_h;
			index_ele* s = UA_RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_right(r_e->parent, ele);

				s_h = r_parent->left_h;
				s = UA_RESOLVE(s_h);
			}

			index_ele* s_left = UA_RESOLVE(s->left_h);
			index_ele* s_right = UA_RESOLVE(s->right_h);

			if (s_left->color == BLACK && s_right->color == BLACK) {
				s->color = RED;

				r_e = r_e->parent;
			}
			else {
				if (s_left->color == BLACK) {
					s_right->color = BLACK;
					s->color = RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					stack_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					rotate_left(s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				UA_RESOLVE(s->left_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_right(r_e->parent, ele);

				UA_RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
	}

	r_e->me->color = BLACK;
}

static void
rotate_left(stack_ele* a, stack_ele* b)
{
	// Element b is element a's right child - a will become b's left child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *     [x]  b           a  [y]
	 *         / \         / \
	 *        c  [y]     [x]  c
	 */

	// Set a's right child to c, b's former left child.
	a->me->right_h = b->me->left_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's left child to a, and a's parent to b.
	b->me->left_h = a->me_h;
	a->parent = b;
}

static void
rotate_right(stack_ele* a, stack_ele* b)
{
	// Element b is element a's left child - a will become b's right child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *      b  [x]         [y]  a
	 *     / \                 / \
	 *   [y]  c               c  [x]
	 */

	// Set a's left child to c, b's former right child.
	a->me->left_h = b->me->right_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's right child to a, and a's parent to b.
	b->me->right_h = a->me_h;
	a->parent = b;
}


//==========================================================
// uarena API.
//

static void
uarena_init(uarena* ua)
{
	cf_mutex_init(&ua->lock);

	uarena_add_stage(ua);

	ua->at_ele_id = 1;
	memset(uarena_resolve(ua, 0), 0, ELE_SIZE);
}

static void
uarena_destroy(uarena* ua)
{
	for (uint32_t i = 0; i < ua->n_stages; i++) {
		cf_free(ua->stages[i]);
	}

	cf_free(ua->stages);
	cf_mutex_destroy(&ua->lock);
}

static void
uarena_add_stage(uarena* ua)
{
	uint8_t* stage = cf_malloc(STAGE_SIZE);

	if (ua->n_stages % STAGES_STEP == 0) {
		ua->stages = realloc(ua->stages,
				(ua->n_stages + STAGES_STEP) * sizeof(uint8_t*));
	}

	ua->stages[ua->n_stages++] = stage;
}

static uarena_handle
uarena_alloc(uarena* ua)
{
	cf_mutex_lock(&ua->lock);

	uarena_handle h;

	// Check free list first.
	if (ua->free_h != 0) {
		h = ua->free_h;
		ua->free_h = NEXT_FREE_H(ua, h);
	}
	// Otherwise keep end-allocating.
	else {
		if (ua->at_ele_id >= STAGE_CAPACITY) {
			uarena_add_stage(ua);
			ua->at_stage_id++;
			ua->at_ele_id = 0;
		}

		uarena_set_handle(&h, ua->at_stage_id, ua->at_ele_id);
		ua->at_ele_id++;
	}

	cf_mutex_unlock(&ua->lock);

	return h;
}

static void
uarena_free(uarena* ua, uarena_handle h)
{
	cf_mutex_lock(&ua->lock);

	NEXT_FREE_H(ua, h) = ua->free_h;
	ua->free_h = h;

	cf_mutex_unlock(&ua->lock);
}
