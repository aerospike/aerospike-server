/*
 * btree.c
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

#include "sindex/btree.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <xmmintrin.h>

#include "citrusleaf/alloc.h"

#include "log.h"

#include "warnings.h"

#pragma GCC diagnostic warning "-Wcast-qual"
#pragma GCC diagnostic warning "-Wcast-align"

#define BINARY_SEARCH 0


//==========================================================
// Typedefs & constants.
//

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


//==========================================================
// Forward declarations.
//

static void btree_destroy(as_btree* bt, as_btree_node* node);
static bool btree_put(as_btree* bt, as_btree_node* node, const void* key, const void* value);
static bool btree_get(const as_btree* bt, const as_btree_node* node, const void* key, void* value);
static bool btree_delete(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out);
static bool btree_reduce(as_btree* bt, as_btree_node* node, const void* start_key, const void* end_key, as_btree_reduce_fn cb, void* udata);
static bool delete_case_1(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out);
static bool delete_case_2(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, key_bound bound);
static bool delete_case_2a(as_btree* bt, as_btree_node* node, uint32_t i, as_btree_node* child);
static bool delete_case_2b(as_btree* bt, as_btree_node* node, uint32_t i, as_btree_node* child);
static bool delete_case_2c(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, uint32_t i, as_btree_node* left, as_btree_node* right);
static bool delete_case_3(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, key_bound bound);
static bool delete_case_3a_left(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, uint32_t i, as_btree_node* child, as_btree_node* sibling);
static bool delete_case_3a_right(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, uint32_t i, as_btree_node* child, as_btree_node* sibling);
static bool delete_case_3b_left(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, uint32_t i, as_btree_node* child, as_btree_node* sibling);
static bool delete_case_3b_right(as_btree* bt, as_btree_node* node, key_mode mode, const void* key_in, void* key_out, void* value_out, uint32_t i, as_btree_node* child, as_btree_node* sibling);
static as_btree_node* create_node(const as_btree* bt, bool leaf);
static void move_keys(const as_btree* bt, as_btree_node* dst, uint32_t dst_i, as_btree_node* src, uint32_t src_i, uint32_t n_keys);
static void move_values(const as_btree* bt, as_btree_node* dst, uint32_t dst_i, as_btree_node* src, uint32_t src_i, uint32_t n_values);
static void move_children(const as_btree* bt, as_btree_node* dst, uint32_t dst_i, as_btree_node* src, uint32_t src_i, uint32_t n_children);
static key_bound greatest_lower_bound(const as_btree* bt, const as_btree_node* node, const void* key);
static void split_child(as_btree* bt, as_btree_node* node, uint32_t i, as_btree_node* child);
static void merge_children(as_btree* bt, as_btree_node* node, uint32_t i, as_btree_node* left, as_btree_node* right);


//==========================================================
// Inlines & macros.
//

#define N_KEYS(n) (uint32_t)((n) & ((1 << N_KEYS_SZ) - 1))

static inline void*
mut_key(const as_btree* bt, as_btree_node* node, uint32_t i)
{
	return (uint8_t*)node + bt->keys_off + i * bt->key_sz;
}

static inline const void*
const_key(const as_btree* bt, const as_btree_node* node, uint32_t i)
{
	return (const uint8_t*)node + bt->keys_off + i * bt->key_sz;
}

static inline const void*
next_const_key(const as_btree* bt, const void* key)
{
	return (const uint8_t*)key + bt->key_sz;
}

static inline void
get_key(const as_btree* bt, const as_btree_node* node, uint32_t i,
		void* key)
{
	const void* node_key = const_key(bt, node, i);

	memcpy(key, node_key, bt->key_sz);
}

static inline void
set_key(const as_btree* bt, as_btree_node* node, uint32_t i, const void* key)
{
	void* node_key = mut_key(bt, node, i);

	memcpy(node_key, key, bt->key_sz);
}

static inline void*
mut_value(const as_btree* bt, as_btree_node* node, uint32_t i)
{
	return (uint8_t*)node + bt->values_off + i * bt->value_sz;
}

static inline const void*
const_value(const as_btree* bt, const as_btree_node* node, uint32_t i)
{
	return (const uint8_t*)node + bt->values_off + i * bt->value_sz;
}

static inline void*
next_mut_value(const as_btree* bt, void* value)
{
	return (uint8_t*)value + bt->value_sz;
}

static inline void
get_value(const as_btree* bt, const as_btree_node* node, uint32_t i,
		void* value)
{
	const void* node_value = const_value(bt, node, i);

	memcpy(value, node_value, bt->value_sz);
}

static inline void
set_value(const as_btree* bt, as_btree_node* node, uint32_t i,
		const void* value)
{
	void* node_value = mut_value(bt, node, i);

	memcpy(node_value, value, bt->value_sz);
}

static inline as_btree_node**
mut_children(const as_btree* bt, as_btree_node* node)
{
	return (as_btree_node**)((uint8_t*)node + bt->children_off);
}

static inline as_btree_node* const*
const_children(const as_btree* bt, const as_btree_node* node)
{
	return (as_btree_node* const*)((const uint8_t*)node + bt->children_off);
}

static inline void
set_child(const as_btree* bt, as_btree_node* node, uint32_t i,
		as_btree_node* child)
{
	as_btree_node** node_children = mut_children(bt, node);

	node_children[i] = child;
}


//==========================================================
// Public API.
//

as_btree*
as_btree_create(as_btree_key_comp_fn key_comp, uint32_t key_sz,
		uint32_t value_sz, uint32_t order)
{
	cf_assert(order % 2 == 0, AS_SINDEX, "odd B-tree order: %u", order);

	as_btree* bt = cf_calloc(1, sizeof(as_btree));

	bt->key_comp = key_comp;
	bt->key_sz = key_sz;
	bt->value_sz = value_sz;

	bt->min_degree = order / 2;
	bt->max_degree = order;

	bt->keys_off = sizeof(as_btree_node);
	bt->values_off = bt->keys_off + (order - 1) * key_sz;
	bt->children_off = bt->values_off + (order - 1) * value_sz;

	bt->inner_sz = bt->children_off + order * (uint32_t)sizeof(as_btree_node*);
	bt->leaf_sz = bt->children_off;

	bt->root = create_node(bt, true);
	bt->n_nodes = 1;
	bt->n_keys = 0;
	bt->size = sizeof(as_btree) + bt->leaf_sz;
	bt->extra_size = 0;

	return bt;
}

void
as_btree_destroy(as_btree* bt)
{
	btree_destroy(bt, bt->root);
	cf_free(bt);
}

bool
as_btree_put(as_btree* bt, const void* key, const void* value)
{
	if (bt->root->n_keys == bt->max_degree - 1) {
		as_btree_node* root = create_node(bt, false);

		bt->n_nodes++;
		bt->size += bt->inner_sz;

		set_child(bt, root, 0, bt->root);
		split_child(bt, root, 0, bt->root);

		bt->root = root;
	}

	if (! btree_put(bt, bt->root, key, value)) {
		return false;
	}

	bt->n_keys++;
	return true;
}

bool
as_btree_get(const as_btree* bt, const void* key, void* value)
{
	return btree_get(bt, bt->root, key, value);
}

bool
as_btree_delete(as_btree* bt, const void* key)
{
	if (! btree_delete(bt, bt->root, KEY_MODE_MATCH, key, NULL, NULL)) {
		return false;
	}

	bt->n_keys--;

	as_btree_node* root = bt->root;

	if (root->n_keys == 0 && root->leaf == 0) {
		bt->root = const_children(bt, root)[0];

		cf_free(root);

		bt->n_nodes--;
		bt->size -= bt->inner_sz;
	}

	return true;
}

void
as_btree_reduce(as_btree* bt, const void* start_key, const void* end_key,
		as_btree_reduce_fn cb, void* udata)
{
	if (start_key == NULL || end_key == NULL ||
			bt->key_comp(start_key, end_key) <= 0) {
		btree_reduce(bt, bt->root, start_key, end_key, cb, udata);
	}
}


//==========================================================
// Local helpers.
//

static void
btree_destroy(as_btree* bt, as_btree_node* node)
{
	if (node->leaf == 0) {
		as_btree_node** children = mut_children(bt, node);

		for (uint32_t i = 0; i <= node->n_keys; i++) {
			btree_destroy(bt, children[i]);
		}
	}

	cf_free(node);
}

static bool
btree_put(as_btree* bt, as_btree_node* node, const void* key, const void* value)
{
	cf_assert(node->n_keys < bt->max_degree - 1, AS_SINDEX, "bad key count: %d",
			node->n_keys);

	key_bound bound = greatest_lower_bound(bt, node, key);

	if (bound.equal) {
		set_value(bt, node, (uint32_t)bound.index, value);
		return false;
	}

	uint32_t i = (uint32_t)(bound.index + 1);

	if (node->leaf != 0) {
		if (i < node->n_keys) {
			move_keys(bt, node, i + 1, node, i, node->n_keys - i);
			move_values(bt, node, i + 1, node, i, node->n_keys - i);
		}

		set_key(bt, node, i, key);
		set_value(bt, node, i, value);
		node->n_keys++;
		return true;
	}

	as_btree_node** children = mut_children(bt, node);

	if (children[i]->n_keys == bt->max_degree - 1) {
		split_child(bt, node, i, children[i]);

		// split_child() pulled up a key to i - look at it.

		const void* node_key = const_key(bt, node, i);
		int32_t comp = bt->key_comp(key, node_key);

		if (comp == 0) {
			set_value(bt, node, i, value);
			return false;
		}

		if (comp > 0) {
			i++;
		}
	}

	return btree_put(bt, children[i], key, value);
}

static bool
btree_get(const as_btree* bt, const as_btree_node* node, const void* key,
		void* value)
{
	key_bound bound = greatest_lower_bound(bt, node, key);

	if (bound.equal) {
		get_value(bt, node, (uint32_t)bound.index, value);
		return true;
	}

	if (node->leaf != 0) {
		return false;
	}

	as_btree_node* const* children = const_children(bt, node);

	return btree_get(bt, children[bound.index + 1], key, value);
}

static bool
btree_delete(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out)
{
	cf_assert(mode != KEY_MODE_MATCH || (key_in != NULL && key_out == NULL &&
			value_out == NULL), AS_SINDEX, "bad arguments");
	cf_assert(mode == KEY_MODE_MATCH || (key_in == NULL && key_out != NULL &&
			value_out != NULL), AS_SINDEX, "bad arguments");

	cf_assert(node != bt->root || node->n_keys > 0, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node == bt->root || node->n_keys >= bt->min_degree, AS_SINDEX,
			"bad key count: %d", node->n_keys);

	if (node->leaf != 0) {
		return delete_case_1(bt, node, mode, key_in, key_out, value_out);
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
		return delete_case_2(bt, node, mode, key_in, key_out, value_out, bound);
	}

	return delete_case_3(bt, node, mode, key_in, key_out, value_out, bound);
}

static bool
btree_reduce(as_btree* bt, as_btree_node* node, const void* start_key,
		const void* end_key, as_btree_reduce_fn cb, void* udata)
{
	key_bound bound;

	if (start_key != NULL) {
		bound = greatest_lower_bound(bt, node, start_key);
	}
	else {
		bound = (key_bound){ .index = -1, .equal = false };
	}

	const void* key_cb;
	void* value_cb;

	if (bound.equal) {
		key_cb = const_key(bt, node, (uint32_t)bound.index);
		value_cb = mut_value(bt, node, (uint32_t)bound.index);

		if (! cb(key_cb, value_cb, udata)) {
			return false;
		}
	}

	uint32_t i = (uint32_t)(bound.index + 1);
	as_btree_node* const* children = node->leaf == 0 ?
			const_children(bt, node) : NULL;

	if (children != NULL && ! btree_reduce(bt, children[i], start_key, end_key,
			cb, udata)) {
		return false;
	}

	key_cb = const_key(bt, node, i);
	value_cb = mut_value(bt, node, i);

	while (++i <= node->n_keys) {
		if (end_key != NULL && bt->key_comp(key_cb, end_key) > 0) {
			return false;
		}

		if (! cb(key_cb, value_cb, udata)) {
			return false;
		}

		if (children != NULL && ! btree_reduce(bt, children[i], NULL, end_key,
				cb, udata)) {
			return false;
		}

		key_cb = next_const_key(bt, key_cb);
		value_cb = next_mut_value(bt, value_cb);
	}

	return true;
}

static bool
delete_case_1(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out)
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
		get_value(bt, node, (uint32_t)bound.index, value_out);
	}

	if (bound.index < node->n_keys - 1) {
		uint32_t i = (uint32_t)bound.index;

		move_keys(bt, node, i, node, i + 1, node->n_keys - (i + 1));
		move_values(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	}

	node->n_keys--;
	return true;
}

static bool
delete_case_2(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, key_bound bound)
{
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(mode == KEY_MODE_MATCH, AS_SINDEX, "bad mode");
	cf_assert(bound.equal, AS_SINDEX, "bad equality flag");

	uint32_t i = (uint32_t)bound.index;
	as_btree_node** children = mut_children(bt, node);
	as_btree_node* left = children[i];

	if (left->n_keys >= bt->min_degree) {
		return delete_case_2a(bt, node, i, left);
	}

	as_btree_node* right = children[i + 1];

	if (right->n_keys >= bt->min_degree) {
		return delete_case_2b(bt, node, i, right);
	}

	return delete_case_2c(bt, node, mode, key_in, key_out, value_out, i, left,
			right);
}

static bool
delete_case_2a(as_btree* bt, as_btree_node* node, uint32_t i,
		as_btree_node* child)
{
	void* node_key = mut_key(bt, node, i);
	void* node_value = mut_value(bt, node, i);

	// NOTE: May modify this key and value not just from the child, but from
	// anywhere further down in the tree. Keep in mind for lock coupling.

	return btree_delete(bt, child, KEY_MODE_MAX, NULL, node_key, node_value);
}

static bool
delete_case_2b(as_btree* bt, as_btree_node* node, uint32_t i,
		as_btree_node* child)
{
	void* node_key = mut_key(bt, node, i);
	void* node_value = mut_value(bt, node, i);

	// NOTE: May modify this key and value not just from the child, but from
	// anywhere further down in the tree. Keep in mind for lock coupling.

	return btree_delete(bt, child, KEY_MODE_MIN, NULL, node_key, node_value);
}

static bool
delete_case_2c(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, uint32_t i,
		as_btree_node* left, as_btree_node* right)
{
	merge_children(bt, node, i, left, right);

	return btree_delete(bt, left, mode, key_in, key_out, value_out);
}

static bool
delete_case_3(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, key_bound bound)
{
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(! bound.equal, AS_SINDEX, "bad equality flag");

	uint32_t i = (uint32_t)(bound.index + 1);
	as_btree_node** children = mut_children(bt, node);
	as_btree_node* child = children[i];

	if (child->n_keys >= bt->min_degree) {
		return btree_delete(bt, child, mode, key_in, key_out, value_out);
	}

	cf_assert(child->n_keys == bt->min_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	if (i > 0) {
		as_btree_node* sibling = children[i - 1];

		if (sibling->n_keys >= bt->min_degree) {
			return delete_case_3a_left(bt, node, mode, key_in, key_out,
					value_out, i, child, sibling);
		}

		cf_assert(sibling->n_keys == bt->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i < node->n_keys) {
		as_btree_node* sibling = children[i + 1];

		if (sibling->n_keys >= bt->min_degree) {
			return delete_case_3a_right(bt, node, mode, key_in, key_out,
					value_out, i, child, sibling);
		}

		cf_assert(sibling->n_keys == bt->min_degree - 1, AS_SINDEX,
				"bad key count: %d", sibling->n_keys);
	}

	if (i > 0) {
		as_btree_node* sibling = children[i - 1];

		return delete_case_3b_left(bt, node, mode, key_in, key_out, value_out,
				i, child, sibling);
	}

	as_btree_node* sibling = children[i + 1];

	return delete_case_3b_right(bt, node, mode, key_in, key_out, value_out, i,
			child, sibling);
}

static bool
delete_case_3a_left(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, uint32_t i,
		as_btree_node* child, as_btree_node* sibling)
{
	cf_assert(child->leaf == sibling->leaf, AS_SINDEX, "bad leaf flag");

	move_keys(bt, child, 1, child, 0, child->n_keys);
	move_values(bt, child, 1, child, 0, child->n_keys);

	if (child->leaf == 0) {
		move_children(bt, child, 1, child, 0, (uint32_t)child->n_keys + 1);
	}

	move_keys(bt, child, 0, node, i - 1, 1);
	move_values(bt, child, 0, node, i - 1, 1);

	if (child->leaf == 0) {
		move_children(bt, child, 0, sibling, sibling->n_keys, 1);
	}

	child->n_keys++;

	move_keys(bt, node, i - 1, sibling, (uint32_t)sibling->n_keys - 1, 1);
	move_values(bt, node, i - 1, sibling, (uint32_t)sibling->n_keys - 1, 1);

	sibling->n_keys--;

	return btree_delete(bt, child, mode, key_in, key_out, value_out);
}

static bool
delete_case_3a_right(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, uint32_t i,
		as_btree_node* child, as_btree_node* sibling)
{
	cf_assert(child->leaf == sibling->leaf, AS_SINDEX, "bad leaf flag");

	move_keys(bt, child, child->n_keys, node, i, 1);
	move_values(bt, child, child->n_keys, node, i, 1);

	if (child->leaf == 0) {
		move_children(bt, child, (uint32_t)child->n_keys + 1, sibling, 0, 1);
	}

	child->n_keys++;

	move_keys(bt, node, i, sibling, 0, 1);
	move_values(bt, node, i, sibling, 0, 1);

	move_keys(bt, sibling, 0, sibling, 1, (uint32_t)sibling->n_keys - 1);
	move_values(bt, sibling, 0, sibling, 1, (uint32_t)sibling->n_keys - 1);

	if (child->leaf == 0) {
		move_children(bt, sibling, 0, sibling, 1, sibling->n_keys);
	}

	sibling->n_keys--;

	return btree_delete(bt, child, mode, key_in, key_out, value_out);
}

static bool
delete_case_3b_left(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, uint32_t i,
		as_btree_node* child, as_btree_node* sibling)
{
	merge_children(bt, node, i - 1, sibling, child);

	return btree_delete(bt, sibling, mode, key_in, key_out, value_out);
}

static bool
delete_case_3b_right(as_btree* bt, as_btree_node* node, key_mode mode,
		const void* key_in, void* key_out, void* value_out, uint32_t i,
		as_btree_node* child, as_btree_node* sibling)
{
	merge_children(bt, node, i, child, sibling);

	return btree_delete(bt, child, mode, key_in, key_out, value_out);
}

static as_btree_node*
create_node(const as_btree* bt, bool leaf)
{
	as_btree_node* node = cf_calloc(1, leaf ? bt->leaf_sz : bt->inner_sz);

	node->n_keys = 0;
	node->leaf = leaf ? 1 : 0;

	return node;
}

static void
move_keys(const as_btree* bt, as_btree_node* dst, uint32_t dst_i,
		as_btree_node* src, uint32_t src_i, uint32_t n_keys)
{
	void* dst_keys = mut_key(bt, dst, dst_i);
	const void* src_keys = const_key(bt, src, src_i);

	memmove(dst_keys, src_keys, n_keys * bt->key_sz);
}

static void
move_values(const as_btree* bt, as_btree_node* dst, uint32_t dst_i,
		as_btree_node* src, uint32_t src_i, uint32_t n_values)
{
	void* dst_values = mut_value(bt, dst, dst_i);
	const void* src_values = const_value(bt, src, src_i);

	memmove(dst_values, src_values, n_values * bt->value_sz);
}

static void
move_children(const as_btree* bt, as_btree_node* dst, uint32_t dst_i,
		as_btree_node* src, uint32_t src_i, uint32_t n_children)
{
	as_btree_node** dst_children = mut_children(bt, dst);
	as_btree_node* const* src_children = const_children(bt, src);

	memmove(&dst_children[dst_i], &src_children[src_i],
			n_children * sizeof(as_btree_node*));
}

#if BINARY_SEARCH == 0
static key_bound
greatest_lower_bound(const as_btree* bt, const as_btree_node* node,
		const void* key)
{
	int32_t index = -1;
	bool equal = false;
	const void* key_i = const_key(bt, node, 0);
	const uint8_t* pref = key_i;

	_mm_prefetch(pref, _MM_HINT_NTA);
	pref += CACHE_LINE_SZ;
	_mm_prefetch(pref, _MM_HINT_NTA);

	for (uint32_t i = 0; i < node->n_keys; i++) {
		int32_t rel = bt->key_comp(key, key_i);

		if (rel < 0) {
			break;
		}

		index = (int32_t)i;
		equal = rel == 0;

		key_i = next_const_key(bt, key_i);

		if (key_i >= (const void*)pref) {
			pref += CACHE_LINE_SZ;
			_mm_prefetch(pref, _MM_HINT_NTA);
		}
	}

	return (key_bound){ .index = index, .equal = equal };
}
#else
static key_bound
greatest_lower_bound(const as_btree* bt, const as_btree_node* node,
		const void* key)
{
	int32_t lower = 0;
	int32_t upper = node->n_keys - 1;
	key_bound bound = { .index = -1, .equal = false };

	while (lower <= upper) {
		int32_t i = (lower + upper) / 2;
		const void* key_i = const_key(bt, node, (uint32_t)i);

		_mm_prefetch(key_i, _MM_HINT_NTA);

		int32_t rel = bt->key_comp(key_i, key);

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

static void
split_child(as_btree* bt, as_btree_node* node, uint32_t i, as_btree_node* child)
{
	cf_assert(node->n_keys < bt->max_degree - 1, AS_SINDEX,
			"bad key count: %d", node->n_keys);
	cf_assert(node->leaf == 0, AS_SINDEX, "bad leaf flag");
	cf_assert(child->n_keys == bt->max_degree - 1, AS_SINDEX,
			"bad key count: %d", child->n_keys);

	// Child's last (min_degree - 1) keys go to new sibling.

	as_btree_node* sibling = create_node(bt, child->leaf != 0);

	bt->n_nodes++;
	bt->size += child->leaf == 0 ? bt->inner_sz : bt->leaf_sz;

	move_keys(bt, sibling, 0, child, bt->min_degree, bt->min_degree - 1);
	move_values(bt, sibling, 0, child, bt->min_degree, bt->min_degree - 1);

	if (child->leaf == 0) {
		move_children(bt, sibling, 0, child, bt->min_degree, bt->min_degree);
	}

	child->n_keys = N_KEYS(child->n_keys - (bt->min_degree - 1));
	sibling->n_keys = N_KEYS(sibling->n_keys + (bt->min_degree - 1));

	// Make room in parent for new sibling at (i + 1).

	move_keys(bt, node, i + 1, node, i, node->n_keys - i);
	move_values(bt, node, i + 1, node, i, node->n_keys - i);
	move_children(bt, node, i + 2, node, i + 1,
			(uint32_t)node->n_keys + 1 - (i + 1));

	node->n_keys++;

	// Attach new sibling to parent using child's last key.

	move_keys(bt, node, i, child, (uint32_t)child->n_keys - 1, 1);
	move_values(bt, node, i, child, (uint32_t)child->n_keys - 1, 1);

	set_child(bt, node, i + 1, sibling);

	child->n_keys--;
}

static void
merge_children(as_btree* bt, as_btree_node* node, uint32_t i,
		as_btree_node* left, as_btree_node* right)
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
	move_values(bt, left, left->n_keys, node, i, 1);

	left->n_keys++;

	// Append child to sibling.

	move_keys(bt, left, left->n_keys, right, 0, right->n_keys);
	move_values(bt, left, left->n_keys, right, 0, right->n_keys);

	if (left->leaf == 0) {
		move_children(bt, left, left->n_keys, right, 0,
				(uint32_t)right->n_keys + 1);
	}

	left->n_keys = N_KEYS(left->n_keys + right->n_keys);

	// Drop separating key.

	move_keys(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	move_values(bt, node, i, node, i + 1, node->n_keys - (i + 1));
	move_children(bt, node, i + 1, node, i + 2,
			(uint32_t)node->n_keys + 1 - (i + 2));

	node->n_keys--;

	cf_free(right);

	bt->n_nodes--;
	bt->size -= left->leaf == 0 ? bt->inner_sz : bt->leaf_sz;
}
