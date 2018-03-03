/*
 * bt_iteretor.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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

/*
 * This file implements Aerospike Index B-tree iterators.
 */

#pragma once

#include "ai_types.h"
#include "bt.h"

typedef struct btEntry {
	void   *key;
	void   *val;
	void   *stream; // some iterators need the raw stream (INDEX CURSORS)
	bt_n   *x;      // some iterators need the position in the bt_n
	int     i;      // some iterators need the position in the bt_n
	bool    missed;
	uint32  dr;     // RANGE DELETEs simulate Keys using DR
} btEntry;

typedef struct bTreeLinkedListNode { // 3ptr(24) 2int(8) -> 32 bytes
	struct bTreeLinkedListNode *parent;
	struct btreenode           *self;
	struct bTreeLinkedListNode *child;
	int                         ik;
	int                         in; //TODO in not needed, ik & logic is enough
} bt_ll_n;

typedef void iter_single(struct btIterator *iter);

/* using 16 as 8^16 can hold 2.8e14 elements (8 is min members in a btn)*/
#define MAX_BTREE_DEPTH 16
typedef struct btIterator { // 60B + 16*bt_ll_n(512) -> dont malloc
	bt          *btr;
	bt_ll_n     *bln;
	int          depth;
	iter_single *iNode;     // function to iterate on node's
	iter_single *iLeaf;     // function to iterate on leaf's
	bool         finished;
	long         high;      // HIGH for INT & LONG
	uint160      highy;     // HIGH for U160
	uchar        num_nodes; // \/-slot in nodes[]
	bt_ll_n      nodes[MAX_BTREE_DEPTH];
} btIterator;

typedef struct btSIter { // btIterator 500+ bytes -> STACK (globals) ALLOCATE
	btIterator x;
	bool       missed; // CURRENT iteration is miss
	bool       nim;    // NEXT    iteration is miss
	bool       empty;
	bool       scan;
	col_type_t ktype;
	btEntry    be;
	ai_obj     key;    // static AI_OBJ for be.key
	char       dofree;
} btSIter;

#define II_FAIL       -1
#define II_OK          0
#define II_LEAF_EXIT   1
#define II_ONLY_RIGHT  2
#define II_MISS        3
#define II_L_MISS      4

bt_ll_n *get_new_iter_child(btIterator *iter);
void     to_child(btIterator *iter, bt_n* self);
int      init_iterator(bt *btr, bt_data_t simkey, struct btIterator *iter);

btSIter *btGetRangeIter    (bt *btr, ai_obj *alow, ai_obj *ahigh,         bool asc);
btSIter *btGetFullRangeIter(bt *btr,             bool asc, cswc_t *w);
btSIter *btGetFullXthIter  (bt *btr,     ulong x, bool asc, cswc_t *w, long lim);
btSIter *btSetFullRangeIter(btSIter *iter, bt *btr, bool asc, cswc_t *w);
btSIter *btSetRangeIter    (btSIter *iter, bt *btr, ai_obj *alow, ai_obj *ahigh, bool asc);
btEntry *btRangeNext           (btSIter *iter,                        bool asc);
void     btReleaseRangeIterator(btSIter *iter);
bool assignMinKey(bt *btr, ai_obj *key);
bool assignMaxKey(bt *btr, ai_obj *key);
