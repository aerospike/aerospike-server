/*
 * index.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
#include <xmmintrin.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/stats.h"


//==========================================================
// Typedefs & constants.
//

typedef enum {
	AS_BLACK	= 0,
	AS_RED		= 1
} as_index_color;

typedef struct as_index_ph_s {
	as_index			*r;
	cf_arenax_handle	r_h;
} as_index_ph;

typedef struct as_index_ph_array_s {
	uint32_t	alloc_sz;
	uint32_t	pos;
	as_index_ph	indexes[];
} as_index_ph_array;

typedef struct as_index_ele_s {
	struct as_index_ele_s	*parent;
	cf_arenax_handle		me_h;
	as_index				*me;
} as_index_ele;

static const size_t MAX_STACK_ARRAY_BYTES = 128 * 1024;


//==========================================================
// Globals.
//

static cf_queue g_gc_queue;


//==========================================================
// Forward declarations.
//

void *run_index_tree_gc(void *unused);
void as_index_tree_destroy(as_index_tree *tree);

uint64_t as_index_sprig_reduce_partial(as_index_sprig *isprig, uint64_t sample_count, as_index_reduce_fn cb, void *udata);
void as_index_sprig_traverse(as_index_sprig *isprig, cf_arenax_handle r_h, as_index_ph_array *v_a);
void as_index_sprig_traverse_purge(as_index_sprig *isprig, cf_arenax_handle r_h);

int as_index_sprig_try_exists(as_index_sprig *isprig, const cf_digest *keyd);
int as_index_sprig_try_get_vlock(as_index_sprig *isprig, const cf_digest *keyd, as_index_ref *index_ref);
int as_index_sprig_get_insert_vlock(as_index_sprig *isprig, uint8_t tree_id, const cf_digest *keyd, as_index_ref *index_ref);

int as_index_sprig_search_lockless(as_index_sprig *isprig, const cf_digest *keyd, as_index **ret, cf_arenax_handle *ret_h);
void as_index_sprig_insert_rebalance(as_index_sprig *isprig, as_index *root_parent, as_index_ele *ele);
void as_index_sprig_delete_rebalance(as_index_sprig *isprig, as_index *root_parent, as_index_ele *ele);
void as_index_rotate_left(as_index_ele *a, as_index_ele *b);
void as_index_rotate_right(as_index_ele *a, as_index_ele *b);

static inline void
as_index_sprig_from_i(as_index_tree *tree, as_index_sprig *isprig,
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
// Public API - initialize garbage collection system.
//

void
as_index_tree_gc_init()
{
	cf_queue_init(&g_gc_queue, sizeof(as_index_tree*), 4096, true);
	cf_thread_create_detached(run_index_tree_gc, NULL);
}


int
as_index_tree_gc_queue_size()
{
	return cf_queue_sz(&g_gc_queue);
}


//==========================================================
// Public API - create/destroy/size a tree.
//

// Create a new red-black tree.
as_index_tree *
as_index_tree_create(as_index_tree_shared *shared, uint8_t id,
		as_index_tree_done_fn cb, void *udata)
{
	size_t locks_size = sizeof(cf_mutex) * NUM_LOCK_PAIRS * 2;
	size_t sprigs_size = sizeof(as_sprig) * shared->n_sprigs;
	size_t puddles_size = tree_puddles_size(shared);

	size_t tree_size = sizeof(as_index_tree) +
			locks_size + sprigs_size + puddles_size;

	as_index_tree *tree = cf_rc_alloc(tree_size);

	tree->id = id;
	tree->done_cb = cb;
	tree->udata = udata;

	tree->shared = shared;
	tree->n_elements = 0;

	as_lock_pair *pair = tree_locks(tree);
	as_lock_pair *pair_end = pair + NUM_LOCK_PAIRS;

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
as_index_tree_block(as_index_tree *tree)
{
	if (! tree) {
		return;
	}

	as_lock_pair *pair = tree_locks(tree);
	as_lock_pair *pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_lock(&pair->lock);
		pair++;
	}
}


void
as_index_tree_reserve(as_index_tree *tree)
{
	if (tree) {
		cf_rc_reserve(tree);
	}
}


// Destroy a red-black tree; return 0 if the tree was destroyed or 1 otherwise.
// TODO - nobody cares about the return value, make it void?
int
as_index_tree_release(as_index_tree *tree)
{
	if (! tree) {
		return 0;
	}

	int rc = cf_rc_release(tree);

	if (rc > 0) {
		return 1;
	}

	cf_assert(rc == 0, AS_INDEX, "tree ref-count %d", rc);

	// TODO - call as_index_tree_destroy() directly if tree is empty?

	cf_queue_push(&g_gc_queue, &tree);

	return 0;
}


// Get the number of elements in the tree.
uint64_t
as_index_tree_size(as_index_tree *tree)
{
	return tree ? cf_atomic64_get(tree->n_elements) : 0;
}


//==========================================================
// Public API - reduce a tree.
//

// Make a callback for every element in the tree, from outside the tree lock.
void
as_index_reduce(as_index_tree *tree, as_index_reduce_fn cb, void *udata)
{
	as_index_reduce_partial(tree, AS_REDUCE_ALL, cb, udata);
}


// Make a callback for a specified number of elements in the tree, from outside
// the tree lock.
void
as_index_reduce_partial(as_index_tree *tree, uint64_t sample_count,
		as_index_reduce_fn cb, void *udata)
{
	if (! tree) {
		return;
	}

	// Reduce sprigs from largest to smallest digests to preserve this order for
	// the whole tree. (Rapid rebalance requires exact order.)

	for (int i = (int)tree->shared->n_sprigs - 1; i >= 0; i--) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, (uint32_t)i);

		if (tree_puddles(tree) != NULL) {
			sample_count -= as_index_sprig_keyd_reduce_partial(&isprig,
					sample_count, cb, udata);
		}
		else {
			sample_count -= as_index_sprig_reduce_partial(&isprig,
					sample_count, cb, udata);
		}

		if (sample_count == 0) {
			break;
		}
	}
}


//==========================================================
// Public API - get/insert/delete an element in a tree.
//

// Is there an element with specified digest in the tree?
//
// Returns:
//		 0 - found (yes)
//		-1 - not found (no)
//		-2 - can't lock (don't know)
int
as_index_try_exists(as_index_tree *tree, const cf_digest *keyd)
{
	if (! tree) {
		return -1;
	}

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	return as_index_sprig_try_exists(&isprig, keyd);
}


// If there's an element with specified digest in the tree, return a locked
// reference to it in index_ref.
//
// Returns:
//		 0 - found (reference returned in index_ref)
//		-1 - not found (index_ref untouched)
//		-2 - can't lock (don't know, index_ref untouched)
int
as_index_try_get_vlock(as_index_tree *tree, const cf_digest *keyd,
		as_index_ref *index_ref)
{
	if (! tree) {
		return -1;
	}

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	return as_index_sprig_try_get_vlock(&isprig, keyd, index_ref);
}


// If there's an element with specified digest in the tree, return a locked
// reference to it in index_ref.
//
// Returns:
//		 0 - found (reference returned in index_ref)
//		-1 - not found (index_ref untouched)
int
as_index_get_vlock(as_index_tree *tree, const cf_digest *keyd,
		as_index_ref *index_ref)
{
	if (! tree) {
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
as_index_get_insert_vlock(as_index_tree *tree, const cf_digest *keyd,
		as_index_ref *index_ref)
{
	cf_assert(tree, AS_INDEX, "inserting in null tree");

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	int result = as_index_sprig_get_insert_vlock(&isprig, tree->id, keyd,
			index_ref);

	if (result == 1) {
		cf_atomic64_incr(&tree->n_elements);
	}

	return result;
}


// If there's an element with specified digest in the tree, delete it.
//
// This MUST be called under the record (sprig) lock!
//
// Returns:
//		 0 - found and deleted
//		-1 - not found
// TODO - nobody cares about the return value, make it void?
int
as_index_delete(as_index_tree *tree, const cf_digest *keyd)
{
	if (! tree) {
		return -1;
	}

	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	int result = as_index_sprig_delete(&isprig, keyd);

	if (result == 0) {
		cf_atomic64_decr(&tree->n_elements);
	}

	return result;
}


//==========================================================
// Local helpers - garbage collection, generic.
//

void *
run_index_tree_gc(void *unused)
{
	as_index_tree *tree;

	while (cf_queue_pop(&g_gc_queue, &tree, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_index_tree_destroy(tree);
	}

	return NULL;
}


void
as_index_tree_destroy(as_index_tree *tree)
{
	for (uint32_t i = 0; i < tree->shared->n_sprigs; i++) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, i);

		as_index_sprig_traverse_purge(&isprig, isprig.sprig->root_h);
	}

	cf_arenax_reclaim(tree->shared->arena, tree_puddles(tree),
			tree_puddles_count(tree->shared));

	as_lock_pair *pair = tree_locks(tree);
	as_lock_pair *pair_end = pair + NUM_LOCK_PAIRS;

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
uint64_t
as_index_sprig_reduce_partial(as_index_sprig *isprig, uint64_t sample_count,
		as_index_reduce_fn cb, void *udata)
{
	bool reduce_all = sample_count == AS_REDUCE_ALL;

	cf_mutex_lock(&isprig->pair->reduce_lock);

	if (reduce_all || sample_count > isprig->sprig->n_elements) {
		sample_count = isprig->sprig->n_elements;
	}

	// Common to encounter empty sprigs.
	if (sample_count == 0) {
		cf_mutex_unlock(&isprig->pair->reduce_lock);
		return 0;
	}

	size_t sz = sizeof(as_index_ph_array) +
			(sizeof(as_index_ph) * sample_count);
	as_index_ph_array *v_a;
	uint8_t buf[MAX_STACK_ARRAY_BYTES];

	v_a = sz > MAX_STACK_ARRAY_BYTES ? cf_malloc(sz) : (as_index_ph_array*)buf;

	v_a->alloc_sz = (uint32_t)sample_count;
	v_a->pos = 0;

	uint64_t start_ms = cf_getms();

	// Recursively, fetch all the value pointers into this array, so we can make
	// all the callbacks outside the big lock.
	as_index_sprig_traverse(isprig, isprig->sprig->root_h, v_a);

	cf_detail(AS_INDEX, "sprig reduce took %lu ms", cf_getms() - start_ms);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	uint64_t i;

	for (i = 0; i < v_a->pos; i++) {
		as_index_ref r_ref;

		r_ref.r = v_a->indexes[i].r;
		r_ref.r_h = v_a->indexes[i].r_h;
		r_ref.puddle = isprig->puddle;

		r_ref.olock = &isprig->pair->lock;
		cf_mutex_lock(r_ref.olock);

		uint16_t rc = as_index_release(r_ref.r);

		// Ignore this record if it's been deleted.
		if (! as_index_is_valid_record(r_ref.r)) {
			if (rc == 0) {
				if (isprig->destructor) {
					isprig->destructor(r_ref.r, isprig->destructor_udata);
				}

				cf_arenax_free(isprig->arena, r_ref.r_h, r_ref.puddle);
			}

			cf_mutex_unlock(r_ref.olock);
			continue;
		}

		// Callback MUST call as_record_done() to unlock and release record.
		cb(&r_ref, udata);
	}

	if (v_a != (as_index_ph_array*)buf) {
		cf_free(v_a);
	}

	// In reduce-all mode, return 0 so outside loop continues to pass
	// sample_count = AS_REDUCE_ALL.
	return reduce_all ? 0 : i;
}


void
as_index_sprig_traverse(as_index_sprig *isprig, cf_arenax_handle r_h,
		as_index_ph_array *v_a)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index *r = RESOLVE_H(r_h);

	as_index_sprig_traverse(isprig, r->left_h, v_a);

	if (v_a->pos >= v_a->alloc_sz) {
		return;
	}

	as_index_reserve(r);

	v_a->indexes[v_a->pos].r = r;
	v_a->indexes[v_a->pos].r_h = r_h;
	v_a->pos++;

	as_index_sprig_traverse(isprig, r->right_h, v_a);
}


void
as_index_sprig_traverse_purge(as_index_sprig *isprig, cf_arenax_handle r_h)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index *r = RESOLVE_H(r_h);

	as_index_sprig_traverse_purge(isprig, r->left_h);
	as_index_sprig_traverse_purge(isprig, r->right_h);

	// There should be no references during a tree purge (reduce should have
	// reserved the tree).
	cf_assert(r->rc == 0, AS_INDEX, "purge found referenced record");

	if (isprig->destructor) {
		isprig->destructor(r, isprig->destructor_udata);
	}

	cf_arenax_free(isprig->arena, r_h, isprig->puddle);
}


//==========================================================
// Local helpers - get/insert/delete an element in a sprig.
//

int
as_index_sprig_try_exists(as_index_sprig *isprig, const cf_digest *keyd)
{
	if (! cf_mutex_trylock(&isprig->pair->lock)) {
		return -2;
	}

	int rv = as_index_sprig_search_lockless(isprig, keyd, NULL, NULL);

	cf_mutex_unlock(&isprig->pair->lock);

	return rv;
}


int
as_index_sprig_try_get_vlock(as_index_sprig *isprig, const cf_digest *keyd,
		as_index_ref *index_ref)
{
	if (! cf_mutex_trylock(&isprig->pair->lock)) {
		return -2;
	}

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


int
as_index_sprig_get_vlock(as_index_sprig *isprig, const cf_digest *keyd,
		as_index_ref *index_ref)
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


int
as_index_sprig_get_insert_vlock(as_index_sprig *isprig, uint8_t tree_id,
		const cf_digest *keyd, as_index_ref *index_ref)
{
	int cmp = 0;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element's insertion point.
	as_index_ele eles[64]; // must be >= (24 * 2)
	as_index_ele *ele;

	while (true) {
		ele = eles;

		cf_mutex_lock(&isprig->pair->lock);

		// Search for the specified element, or a parent to insert it under.

		root_parent.left_h = isprig->sprig->root_h;
		root_parent.color = AS_BLACK;

		ele->parent = NULL; // we'll never look this far up
		ele->me_h = 0; // root parent has no handle, never used
		ele->me = &root_parent;

		cf_arenax_handle t_h = isprig->sprig->root_h;
		as_index *t = RESOLVE_H(t_h);

		while (t_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = t_h;
			ele->me = t;

			_mm_prefetch(t, _MM_HINT_NTA);

			if ((cmp = cf_digest_compare(keyd, &t->keyd)) == 0) {
				// The element already exists, simply return it.

				index_ref->r = t;
				index_ref->r_h = t_h;

				index_ref->puddle = isprig->puddle;
				index_ref->olock = &isprig->pair->lock;

				return 0;
			}

			t_h = cmp > 0 ? t->left_h : t->right_h;
			t = RESOLVE_H(t_h);
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

	as_index *n = RESOLVE_H(n_h);

	memset(n, 0, sizeof(as_index));

	n->tree_id = tree_id;
	n->keyd = *keyd;

	n->left_h = n->right_h = SENTINEL_H; // n starts as a leaf element
	n->color = AS_RED; // n's color starts as red

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

	isprig->sprig->n_elements++;

	// Surely we won't hit 16M elements, but...
	cf_assert(isprig->sprig->n_elements != 0, AS_INDEX, "sprig overflow");

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	index_ref->r = n;
	index_ref->r_h = n_h;

	index_ref->puddle = isprig->puddle;
	index_ref->olock = &isprig->pair->lock;

	return 1;
}


// This MUST be called under the record (sprig) lock!
int
as_index_sprig_delete(as_index_sprig *isprig, const cf_digest *keyd)
{
	as_index *r;
	cf_arenax_handle r_h;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element (or its successor).
	as_index_ele eles[128]; // must be >= ((24 * 2) * 2) + 3
	as_index_ele *ele = eles;

	root_parent.left_h = isprig->sprig->root_h;
	root_parent.color = AS_BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	r_h = isprig->sprig->root_h;
	r = RESOLVE_H(r_h);

	while (r_h != SENTINEL_H) {
		ele++;
		ele->parent = ele - 1;
		ele->me_h = r_h;
		ele->me = r;

		_mm_prefetch(r, _MM_HINT_NTA);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			break; // found, we'll be deleting it
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
		r = RESOLVE_H(r_h);
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
	as_index_ele *r_e = ele;

	if (r->left_h != SENTINEL_H && r->right_h != SENTINEL_H) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = RESOLVE_H(ele->me_h);

		while (ele->me->left_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = RESOLVE_H(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	as_index_ele *s_e = ele;
	cf_arenax_handle s_h = s_e->me_h;
	as_index *s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	if (s->left_h == SENTINEL_H) {
		ele->me_h = s->right_h;
	}
	else {
		ele->me_h = s->left_h;
	}

	ele->me = RESOLVE_H(ele->me_h);

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
	if (s->color == AS_BLACK) {
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

	// Rely on n_elements being little endian at the beginning of as_sprig. Only
	// needs to be atomic during warm restart, but not worth special case.
	cf_atomic32_decr((cf_atomic32*)isprig->sprig);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	return 0;
}


//==========================================================
// Local helpers - search/rebalance a sprig.
//

int
as_index_sprig_search_lockless(as_index_sprig *isprig, const cf_digest *keyd,
		as_index **ret, cf_arenax_handle *ret_h)
{
	cf_arenax_handle r_h = isprig->sprig->root_h;
	as_index *r = RESOLVE_H(r_h);

	while (r_h != SENTINEL_H) {
		_mm_prefetch(r, _MM_HINT_NTA);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			if (ret_h) {
				*ret_h = r_h;
			}

			if (ret) {
				*ret = r;
			}

			return 0; // found
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
		r = RESOLVE_H(r_h);
	}

	return -1; // not found
}


void
as_index_sprig_insert_rebalance(as_index_sprig *isprig, as_index *root_parent,
		as_index_ele *ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	as_index_ele *r_e = ele;
	as_index_ele *parent_e = r_e->parent;

	while (parent_e->me->color == AS_RED) {
		as_index_ele *grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->right_h;
			as_index *u = RESOLVE_H(u_h);

			if (u->color == AS_RED) {
				u->color = AS_BLACK;
				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->right_h) {
					// Save original r, which will become new r's parent.
					as_index_ele *r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_left(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_right(grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->left_h;
			as_index *u = RESOLVE_H(u_h);

			if (u->color == AS_RED) {
				u->color = AS_BLACK;
				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->left_h) {
					// Save original r, which will become new r's parent.
					as_index_ele *r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_right(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_left(grandparent_e, parent_e);
			}
		}
	}

	RESOLVE_H(root_parent->left_h)->color = AS_BLACK;
}


void
as_index_sprig_delete_rebalance(as_index_sprig *isprig, as_index *root_parent,
		as_index_ele *ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	as_index_ele *r_e = ele;

	while (r_e->me->color == AS_BLACK && r_e->me_h != root_parent->left_h) {
		as_index *r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			cf_arenax_handle s_h = r_parent->right_h;
			as_index *s = RESOLVE_H(s_h);

			if (s->color == AS_RED) {
				s->color = AS_BLACK;
				r_parent->color = AS_RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				s_h = r_parent->right_h;
				s = RESOLVE_H(s_h);
			}

			as_index *s_left = RESOLVE_H(s->left_h);
			as_index *s_right = RESOLVE_H(s->right_h);

			if (s_left->color == AS_BLACK && s_right->color == AS_BLACK) {
				s->color = AS_RED;

				r_e = r_e->parent;
			}
			else {
				if (s_right->color == AS_BLACK) {
					s_left->color = AS_BLACK;
					s->color = AS_RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele *s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					as_index_rotate_right(s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = AS_BLACK;
				RESOLVE_H(s->right_h)->color = AS_BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				RESOLVE_H(root_parent->left_h)->color = AS_BLACK;

				return;
			}
		}
		else {
			cf_arenax_handle s_h = r_parent->left_h;
			as_index *s = RESOLVE_H(s_h);

			if (s->color == AS_RED) {
				s->color = AS_BLACK;
				r_parent->color = AS_RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				s_h = r_parent->left_h;
				s = RESOLVE_H(s_h);
			}

			as_index *s_left = RESOLVE_H(s->left_h);
			as_index *s_right = RESOLVE_H(s->right_h);

			if (s_left->color == AS_BLACK && s_right->color == AS_BLACK) {
				s->color = AS_RED;

				r_e = r_e->parent;
			}
			else {
				if (s_left->color == AS_BLACK) {
					s_right->color = AS_BLACK;
					s->color = AS_RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele *s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					as_index_rotate_left(s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = AS_BLACK;
				RESOLVE_H(s->left_h)->color = AS_BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				RESOLVE_H(root_parent->left_h)->color = AS_BLACK;

				return;
			}
		}
	}

	r_e->me->color = AS_BLACK;
}


void
as_index_rotate_left(as_index_ele *a, as_index_ele *b)
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


void
as_index_rotate_right(as_index_ele *a, as_index_ele *b)
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
