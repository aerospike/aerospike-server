/*
 * index.c
 *
 * Copyright (C) 2012-2026 Aerospike, Inc.
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

#include "base/index.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/set_index.h"
#include "base/stats.h"
#include "sindex/gc.h"
#include "sindex/sindex.h"
#include "transaction/mrt_utils.h"


//==========================================================
// Typedefs & constants.
//

typedef struct as_index_ele_s {
	struct as_index_ele_s* parent;
	cf_arenax_handle me_h;
	as_index* me;
} as_index_ele;


//==========================================================
// Globals.
//

static cf_queue g_gc_queue;


//==========================================================
// Forward declarations.
//

static void* run_index_tree_gc(void* unused);
static void as_index_tree_destroy(as_index_tree* tree);

static bool as_index_sprig_reduce(as_index_sprig* isprig, const cf_digest* keyd, as_index_reduce_fn cb, void* udata);
static void as_index_sprig_traverse(as_index_sprig* isprig, const cf_digest* keyd, cf_arenax_handle r_h, as_index_ph_array* ph_a);
static void as_index_sprig_traverse_purge(as_index_sprig* isprig, cf_arenax_handle r_h);

static int as_index_sprig_get_insert_vlock(as_index_sprig* isprig, uint8_t tree_id, const cf_digest* keyd, as_index_ref* index_ref);

static int as_index_sprig_search_lockless(as_index_sprig* isprig, const cf_digest* keyd, as_index** ret, cf_arenax_handle* ret_h);
static void as_index_sprig_insert_rebalance(as_index_sprig* isprig, as_index* root_parent, as_index_ele* ele);
static void as_index_sprig_delete_rebalance(as_index_sprig* isprig, as_index* root_parent, as_index_ele* ele);
static void as_index_rotate_left(as_index_ele* a, as_index_ele* b);
static void as_index_rotate_right(as_index_ele* a, as_index_ele* b);

static inline void
as_index_sprig_from_i(as_index_tree* tree, as_index_sprig* isprig,
		uint32_t sprig_i)
{
	uint32_t lock_i = sprig_i >>
			(tree->shared->locks_shift - tree->shared->sprigs_shift);

	isprig->destructor = tree->shared->destructor;
	isprig->destructor_udata = tree->shared->destructor_udata;
	isprig->arena = tree->shared->arena;
	isprig->pair = tree_locks(tree) + lock_i;
	isprig->sprig = tree_sprigs(tree) + sprig_i;
	isprig->puddle = tree_puddle_for_sprig(tree, sprig_i);
}


//==========================================================
// Public API - garbage collection system.
//

void
as_index_tree_gc_init()
{
	cf_queue_init(&g_gc_queue, sizeof(as_index_tree*), 4096, true);
	cf_thread_create_detached(run_index_tree_gc, NULL);
}

uint32_t
as_index_tree_gc_queue_size()
{
	return cf_queue_sz(&g_gc_queue);
}

void
as_index_tree_gc(as_index_tree* tree)
{
	cf_queue_push(&g_gc_queue, &tree);
}


//==========================================================
// Public API - create/destroy/size a tree.
//

// Create a new red-black tree.
as_index_tree*
as_index_tree_create(as_index_tree_shared* shared, uint8_t id,
		as_index_tree_done_fn cb, void* udata)
{
	size_t locks_size = sizeof(cf_mutex) * NUM_LOCK_PAIRS * 2;
	size_t sprigs_size = sizeof(as_sprig) * shared->n_sprigs;
	size_t puddles_size = tree_puddles_size(shared);

	size_t tree_size = sizeof(as_index_tree) +
			locks_size + sprigs_size + puddles_size;

	as_index_tree* tree = cf_rc_alloc(tree_size);

	tree->id = id;
	tree->done_cb = cb;
	tree->udata = udata;

	tree->shared = shared;
	tree->n_elements = 0;

	cf_mutex_init(&tree->set_trees_lock);
	memset(tree->set_trees, 0, sizeof(tree->set_trees));

	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_init(&pair->lock);
		cf_mutex_init(&pair->reduce_lock);
		pair++;
	}

	// The tree starts empty.
	memset(tree_sprigs(tree), 0, sprigs_size);
	memset(tree_puddles(tree), 0, puddles_size);

	return tree;
}

// On shutdown, lock all record locks.
void
as_index_tree_block(as_index_tree* tree)
{
	if (tree == NULL) {
		return;
	}

	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_lock(&pair->lock);
		pair++;
	}
}

void
as_index_tree_reserve(as_index_tree* tree)
{
	if (tree != NULL) {
		cf_rc_reserve(tree);
	}
}

// Destroy a red-black tree; return 0 if the tree was destroyed or 1 otherwise.
void
as_index_tree_release(as_namespace* ns, as_index_tree* tree)
{
	if (tree == NULL) {
		return;
	}

	if (cf_rc_release(tree) != 0) {
		return;
	}

	as_set_index_destroy_all(tree);

	// TODO - call as_index_tree_destroy() directly if tree is empty?

	if (as_sindex_n_sindexes(ns) != 0) {
		as_sindex_gc_tree(ns, tree);
	}
	else {
		as_index_tree_gc(tree);
	}
}

// Get the number of elements in the tree.
uint64_t
as_index_tree_size(as_index_tree* tree)
{
	return tree == NULL ? 0 : tree->n_elements;
}


//==========================================================
// Public API - reduce a tree.
//

// Make a callback for every element in the tree, from outside the tree lock.
bool
as_index_reduce(as_index_tree* tree, as_index_reduce_fn cb, void* udata)
{
	return as_index_reduce_from(tree, NULL, cb, udata);
}

// Like as_index_reduce(), but from specfied "boundary" digest.
bool
as_index_reduce_from(as_index_tree* tree, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	if (tree == NULL) {
		return true;
	}

	// Reduce sprigs from largest to smallest digests to preserve this order for
	// the whole tree. (Rapid rebalance requires exact order.)

	uint32_t start_sprig_i = keyd == NULL ?
			tree->shared->n_sprigs - 1 : as_index_sprig_i_from_keyd(tree, keyd);

	for (int i = (int)start_sprig_i; i >= 0; i--) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, (uint32_t)i);

		if (tree->shared->puddles_offset == 0) {
			if (! as_index_sprig_reduce(&isprig, keyd, cb, udata)) {
				return false;
			}
		}
		else {
			if (! as_index_sprig_reduce_no_rc(&isprig, keyd, cb, udata)) {
				return false;
			}
		}

		keyd = NULL; // only need boundary digest for first sprig
	}

	return true;
}


//==========================================================
// Public API - get/insert/delete an element in a tree.
//

// If there's an element with specified digest in the tree, return a locked
// reference to it in index_ref.
//
// Returns:
//		 0 - found (reference returned in index_ref)
//		-1 - not found (index_ref untouched)
int
as_index_get_vlock(as_index_tree* tree, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	if (tree == NULL) {
		return -1;
	}

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	return as_index_sprig_get_vlock(&isprig, keyd, index_ref);
}

// If there's an element with specified digest in the tree, return a locked
// reference to it in index_ref. If not, create an element with this digest,
// insert it into the tree, and return a locked reference to it in index_ref.
//
// Returns:
//		 1 - created and inserted (reference returned in index_ref)
//		 0 - found already existing (reference returned in index_ref)
//		-1 - error - could not allocate arena stage
int
as_index_get_insert_vlock(as_index_tree* tree, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	cf_assert(tree != NULL, AS_INDEX, "inserting in null tree");

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	int result = as_index_sprig_get_insert_vlock(&isprig, tree->id, keyd,
			index_ref);

	if (result == 1) {
		as_incr_uint64(&tree->n_elements);
	}

	return result;
}

// If there's an element with specified digest in the tree, delete it.
//
// This MUST be called under the record (sprig) lock!
//
void
as_index_delete(as_index_tree* tree, const cf_digest* keyd)
{
	if (tree == NULL) {
		return;
	}

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	if (as_index_sprig_delete(&isprig, keyd) == 0) {
		as_decr_uint64(&tree->n_elements);
	}
}


//==========================================================
// Local helpers - garbage collection, generic.
//

static void*
run_index_tree_gc(void* unused)
{
	as_index_tree* tree;

	while (cf_queue_pop(&g_gc_queue, &tree, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_index_tree_destroy(tree);
	}

	return NULL;
}

static void
as_index_tree_destroy(as_index_tree* tree)
{
	for (uint32_t i = 0; i < tree->shared->n_sprigs; i++) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, i);

		as_index_sprig_traverse_purge(&isprig, isprig.sprig->root_h);
	}

	cf_arenax_reclaim(tree->shared->arena, tree_puddles(tree),
			tree_puddles_count(tree->shared));

	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_destroy(&pair->lock);
		cf_mutex_destroy(&pair->reduce_lock);
		pair++;
	}

	tree->done_cb(tree->id, tree->udata);

	cf_rc_free(tree);
}


//==========================================================
// Local helpers - reduce a sprig.
//

// Make a callback for a specified number of elements in the tree, from outside
// the tree lock.
static bool
as_index_sprig_reduce(as_index_sprig* isprig, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	cf_mutex_lock(&isprig->pair->reduce_lock);

	// Common to encounter empty sprigs.
	if (isprig->sprig->root_h == SENTINEL_H) {
		cf_mutex_unlock(&isprig->pair->reduce_lock);
		return true;
	}

	as_index_ph stack_phs[MAX_STACK_PHS];
	as_index_ph_array ph_a = {
			.is_stack = true,
			.capacity = MAX_STACK_PHS,
			.phs = stack_phs
	};

	// Traverse just fills array, then we make callbacks outside reduce lock.
	as_index_sprig_traverse(isprig, keyd, isprig->sprig->root_h, &ph_a);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	bool do_more = true;

	for (uint32_t i = 0; i < ph_a.n_used; i++) {
		as_index_ph* ph = &ph_a.phs[i];
		as_index_ref r_ref = {
				.r = ph->r,
				.r_h = ph->r_h,
				.olock = &isprig->pair->lock
		};

		cf_mutex_lock(r_ref.olock);

		uint16_t rc = as_index_release(r_ref.r);

		// Ignore this record if it's been deleted.
		if (! as_index_is_valid_record(r_ref.r)) {
			as_namespace* ns = isprig->destructor_udata;

			if (rc == 0) {
				if (isprig->destructor != NULL) {
					isprig->destructor(r_ref.r, ns);
				}

				// MRT PARANOIA - can we encounter a provisional here?
				cf_assert(r_ref.r->orig_h == 0, AS_INDEX,
						"unexpected - dropped provisional");

				cf_arenax_free(isprig->arena, r_ref.r_h, NULL);
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
as_index_sprig_traverse(as_index_sprig* isprig, const cf_digest* keyd,
		cf_arenax_handle r_h, as_index_ph_array* ph_a)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index* r = RESOLVE(r_h);
	int cmp = 0; // initialized to satisfy compiler

	if (keyd == NULL || (cmp = cf_digest_compare(&r->keyd, keyd)) < 0) {
		as_index_sprig_traverse(isprig, keyd, r->left_h, ph_a);
	}

	if (ph_a->n_used == ph_a->capacity) {
		as_index_grow_ph_array(ph_a);
	}

	// We do not collect the element with the boundary digest.

	if (keyd == NULL || cmp < 0) {
		as_index_reserve(r);

		as_index_ph* ph = &ph_a->phs[ph_a->n_used++];

		ph->r = r;
		ph->r_h = r_h;

		keyd = NULL;
	}

	as_index_sprig_traverse(isprig, keyd, r->right_h, ph_a);
}

// Used also by set indexes, not a local helper.
void
as_index_grow_ph_array(as_index_ph_array* ph_a)
{
	uint32_t new_capacity = ph_a->capacity * 2;
	size_t new_sz = sizeof(as_index_ph) * new_capacity;

	if (ph_a->is_stack) {
		as_index_ph* phs = cf_malloc(new_sz);

		memcpy(phs, ph_a->phs, sizeof(as_index_ph) * ph_a->capacity);
		ph_a->phs = phs;
		ph_a->is_stack = false;
	}
	else {
		ph_a->phs = cf_realloc(ph_a->phs, new_sz);
	}

	ph_a->capacity = new_capacity;
}

static void
as_index_sprig_traverse_purge(as_index_sprig* isprig, cf_arenax_handle r_h)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index* r = RESOLVE(r_h);

	as_index_sprig_traverse_purge(isprig, r->left_h);
	as_index_sprig_traverse_purge(isprig, r->right_h);

	// There should be no references during a tree purge (reduce should have
	// reserved the tree).
	cf_assert(r->rc == 0 || (r->rc == 1 && r->in_sindex == 1), AS_INDEX,
			"purge found non-0 record rc 0x%hx", r->rc);

	if (isprig->destructor != NULL) {
		isprig->destructor(r, isprig->destructor_udata);
	}

	mrt_free_orig(isprig->arena, r, isprig->puddle);
	cf_arenax_free(isprig->arena, r_h, isprig->puddle);
}


//==========================================================
// Local helpers - get/insert/delete an element in a sprig.
//

// Used by EE index and set index functions, not a local helper.
int
as_index_sprig_get_vlock(as_index_sprig* isprig, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	cf_mutex_lock(&isprig->pair->lock);

	int rv = as_index_sprig_search_lockless(isprig, keyd, &index_ref->r,
			&index_ref->r_h);

	if (rv != 0) {
		cf_mutex_unlock(&isprig->pair->lock);
		return rv;
	}

	index_ref->puddle = isprig->puddle;
	index_ref->olock = &isprig->pair->lock;

	return 0;
}

static int
as_index_sprig_get_insert_vlock(as_index_sprig* isprig, uint8_t tree_id,
		const cf_digest* keyd, as_index_ref* index_ref)
{
	int cmp = 0;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element's insertion point.
	as_index_ele eles[64]; // must be >= (24 * 2)
	as_index_ele* ele;

	while (true) {
		ele = eles;

		cf_mutex_lock(&isprig->pair->lock);

		// Search for the specified element, or a parent to insert it under.

		root_parent.left_h = isprig->sprig->root_h;
		root_parent.color = BLACK;

		ele->parent = NULL; // we'll never look this far up
		ele->me_h = 0; // root parent has no handle, never used
		ele->me = &root_parent;

		cf_arenax_handle t_h = isprig->sprig->root_h;

		while (t_h != SENTINEL_H) {
			as_index* t = RESOLVE(t_h);

			ele++;
			ele->parent = ele - 1;
			ele->me_h = t_h;
			ele->me = t;

			as_arch_prefetch_nt(t);

			if ((cmp = cf_digest_compare(keyd, &t->keyd)) == 0) {
				// The element already exists, simply return it.

				index_ref->r = t;
				index_ref->r_h = t_h;

				index_ref->puddle = isprig->puddle;
				index_ref->olock = &isprig->pair->lock;

				return 0;
			}

			t_h = cmp > 0 ? t->left_h : t->right_h;
		}

		// We didn't find the tree element, so we'll be inserting it.

		if (cf_mutex_trylock(&isprig->pair->reduce_lock)) {
			break; // no reduce in progress - go ahead and insert new element
		}

		// The tree is being reduced - could take long, unlock so reads and
		// overwrites aren't blocked.
		cf_mutex_unlock(&isprig->pair->lock);

		// Wait until the tree reduce is done...
		cf_mutex_lock(&isprig->pair->reduce_lock);
		cf_mutex_unlock(&isprig->pair->reduce_lock);

		// ... and start over - we unlocked, so the tree may have changed.
	}

	// Create a new element and insert it.

	// Save the root so we can detect whether it changes.
	cf_arenax_handle old_root = isprig->sprig->root_h;

	// Make the new element.
	cf_arenax_handle n_h = cf_arenax_alloc(isprig->arena, isprig->puddle);

	if (n_h == 0) {
		cf_ticker_warning(AS_INDEX, "arenax alloc failed");
		cf_mutex_unlock(&isprig->pair->reduce_lock);
		cf_mutex_unlock(&isprig->pair->lock);
		return -1;
	}

	as_index* n = RESOLVE(n_h);

	*n = (as_index){
		.tree_id = tree_id,
		.keyd = *keyd,
		.left_h = SENTINEL_H,
		.right_h = SENTINEL_H,
		.color = RED
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
	as_index_sprig_insert_rebalance(isprig, &root_parent, ele);

	// If insertion caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		isprig->sprig->root_h = root_parent.left_h;
	}

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	index_ref->r = n;
	index_ref->r_h = n_h;

	index_ref->puddle = isprig->puddle;
	index_ref->olock = &isprig->pair->lock;

	return 1;
}

// Used by EE index function, not a local helper.
// This MUST be called under the record (sprig) lock!
int
as_index_sprig_delete(as_index_sprig* isprig, const cf_digest* keyd)
{
	as_index* r;
	cf_arenax_handle r_h;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element (or its successor).
	as_index_ele eles[128]; // must be >= ((24 * 2) * 2) + 3
	as_index_ele* ele = eles;

	root_parent.left_h = isprig->sprig->root_h;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	r_h = isprig->sprig->root_h;

	while (r_h != SENTINEL_H) {
		r = RESOLVE(r_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r_h;
		ele->me = r;

		as_arch_prefetch_nt(r);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			break; // found, we'll be deleting it
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	if (r_h == SENTINEL_H) {
		return -1; // not found, nothing to delete
	}

	// We found the tree element, so we'll be deleting it.

	// If the tree is being reduced, wait until it's done...
	cf_mutex_lock(&isprig->pair->reduce_lock);

	// Delete the element.

	// Save the root so we can detect whether it changes.
	cf_arenax_handle old_root = isprig->sprig->root_h;

	// Snapshot the element to delete, r. (Already have r_h and r shortcuts.)
	as_index_ele* r_e = ele;

	if (r->left_h != SENTINEL_H && r->right_h != SENTINEL_H) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = RESOLVE(ele->me_h);

		while (ele->me->left_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = RESOLVE(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	as_index_ele* s_e = ele;
	cf_arenax_handle s_h = s_e->me_h;
	as_index* s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	ele->me_h = s->left_h == SENTINEL_H ? s->right_h : s->left_h;
	ele->me = RESOLVE(ele->me_h);

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
		as_index_sprig_delete_rebalance(isprig, &root_parent, ele);
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
		isprig->sprig->root_h = root_parent.left_h;
	}

	// Flag record as deleted.
	as_index_invalidate_record(r);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	return 0;
}


//==========================================================
// Local helpers - search/rebalance a sprig.
//

static int
as_index_sprig_search_lockless(as_index_sprig* isprig, const cf_digest* keyd,
		as_index** ret, cf_arenax_handle* ret_h)
{
	cf_arenax_handle r_h = isprig->sprig->root_h;

	while (r_h != SENTINEL_H) {
		as_index* r = RESOLVE(r_h);

		as_arch_prefetch_nt(r);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			if (ret_h != NULL) {
				*ret_h = r_h;
			}

			if (ret != NULL) {
				*ret = r;
			}

			return 0; // found
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	return -1; // not found
}

static void
as_index_sprig_insert_rebalance(as_index_sprig* isprig, as_index* root_parent,
		as_index_ele* ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	as_index_ele* r_e = ele;
	as_index_ele* parent_e = r_e->parent;

	while (parent_e->me->color == RED) {
		as_index_ele* grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->right_h;
			as_index* u = RESOLVE(u_h);

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
					as_index_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_left(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_right(grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->left_h;
			as_index* u = RESOLVE(u_h);

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
					as_index_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_right(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_left(grandparent_e, parent_e);
			}
		}
	}

	RESOLVE(root_parent->left_h)->color = BLACK;
}

static void
as_index_sprig_delete_rebalance(as_index_sprig* isprig, as_index* root_parent,
		as_index_ele* ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	as_index_ele* r_e = ele;

	while (r_e->me->color == BLACK && r_e->me_h != root_parent->left_h) {
		as_index* r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			cf_arenax_handle s_h = r_parent->right_h;
			as_index* s = RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				s_h = r_parent->right_h;
				s = RESOLVE(s_h);
			}

			as_index* s_left = RESOLVE(s->left_h);
			as_index* s_right = RESOLVE(s->right_h);

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

					as_index_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					as_index_rotate_right(s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				RESOLVE(s->right_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
		else {
			cf_arenax_handle s_h = r_parent->left_h;
			as_index* s = RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				s_h = r_parent->left_h;
				s = RESOLVE(s_h);
			}

			as_index* s_left = RESOLVE(s->left_h);
			as_index* s_right = RESOLVE(s->right_h);

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

					as_index_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					as_index_rotate_left(s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				RESOLVE(s->left_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
	}

	r_e->me->color = BLACK;
}

static void
as_index_rotate_left(as_index_ele* a, as_index_ele* b)
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
as_index_rotate_right(as_index_ele* a, as_index_ele* b)
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
