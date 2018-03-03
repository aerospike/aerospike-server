/*
 * bt_iterator.c
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

#include <assert.h>
#include <float.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/param.h>  // For MAX() & MIN().

#include "ai_obj.h"
#include "bt_iterator.h"
#include "stream.h"
#include <citrusleaf/alloc.h>

// HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES
#define GET_NEW_CHILD(iter) \
 if (!iter->bln->child) { iter->bln->child = get_new_iter_child(iter); }

#define SETITER8R(iter, btr, asc, l, lrev, n, nrev) \
  btSIter *siter = setIterator(iter, btr, asc ? l : lrev, asc ? n : nrev);

#define CR8ITER8R(btr, asc, l, lrev, n, nrev) \
  btSIter *siter = createIterator(btr, asc ? l : lrev, asc ? n : nrev);

bt_ll_n *get_new_iter_child(btIterator *iter) { //printf("get_newiterchild\n");
	assert(iter->num_nodes < MAX_BTREE_DEPTH);
	bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
	bzero(nn, sizeof(bt_ll_n));
	iter->num_nodes++;
	return nn;
}

void to_child(btIterator *iter, bt_n* self) {  //printf("to_child\n");
	iter->depth++;
	iter->bln->child->parent = iter->bln;
	iter->bln->child->ik     = 0;
	iter->bln->child->in     = 0;
	iter->bln->child->self   = self;
	iter->bln                = iter->bln->child;
}
static void toparentrecurse(btIterator *iter) {  //printf("to_parent\n");
	if (!iter->bln->parent) {
		iter->finished = 1;    /* finished */
		return;
	}
	iter->depth--;
	bt   *btr    = iter->btr;
	void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
	iter->bln    = iter->bln->parent;                      /* -> parent */
	void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
	int   x      = btr->cmp(child, parent);
	if (x > 0) {
		if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
		if ((iter->bln->in + 1) < iter->bln->self->n) iter->bln->in++;
		else                                          toparentrecurse(iter);
	}
}
static void iter_leaf(btIterator *iter) { //printf("iter_leaf\n");
	if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
	else                                          toparentrecurse(iter);
}
static void tochildrecurse(btIterator *iter, bt_n* self) {
	to_child(iter, self);
	if (!iter->bln->self->leaf) { // depth-first
		GET_NEW_CHILD(iter)
		tochildrecurse(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
	}
}
static void iter_node(btIterator *iter) {
	if ((iter->bln->ik + 1) <  iter->bln->self->n) iter->bln->ik++;
	if ((iter->bln->in + 1) <= iter->bln->self->n) iter->bln->in++;
	GET_NEW_CHILD(iter)
	tochildrecurse(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
}

static void *btNext(btSIter *siter, bt_n **rx, int *ri, bool asc) {
	btIterator *iter = &(siter->x);
	if (iter->finished) {
		if (siter->scan) siter->missed = siter->nim;
		return NULL;
	}
	if (asc) siter->missed = siter->nim; //Curr MISSED = LastLoop's NextIsMissed
	bt_n       *x    = iter->bln->self;
	if (rx) *rx = x;
	int         i    = iter->bln->ik;
	if (ri) *ri = i;
	void       *curr = KEYS(iter->btr, x, i);
	siter->nim       = getDR(iter->btr, x, i) ? 1 : 0;
	if (iter->bln->self->leaf) (*iter->iLeaf)(iter);
	else                       (*iter->iNode)(iter);
	return curr;
}

void to_child_rev(btIterator *iter, bt_n* self) {
	iter->depth++;
	iter->bln->child->parent = iter->bln;
	iter->bln->child->ik     = self->n - 1;
	iter->bln->child->in     = self->n;
	iter->bln->child->self   = self;
	iter->bln                = iter->bln->child;
}
static void tochildrecurserev(btIterator *iter, bt_n* self) {
	to_child_rev(iter, self);
	if (!iter->bln->self->leaf) { // depth-first
		GET_NEW_CHILD(iter)
		tochildrecurserev(iter,
						  NODES(iter->btr, iter->bln->self)[iter->bln->in]);
	}
}
static void toparentrecurserev(btIterator *iter) {
	if (!iter->bln->parent) {
		iter->finished = 1;    /* finished */
		return;
	}
	iter->depth--;
	bt   *btr    = iter->btr;
	void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
	iter->bln    = iter->bln->parent;                      /* -> parent */
	void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
	int   x      = btr->cmp(child, parent);
	if (x < 0) {
		if (iter->bln->ik) iter->bln->ik--;
		if (iter->bln->in) iter->bln->in--;
		else               toparentrecurserev(iter);
	}
	if (iter->bln->in == iter->bln->self->n) iter->bln->in--;
}
static void iter_leaf_rev(btIterator *iter) { //printf("iter_leaf_rev\n");
	if (iter->bln->ik) iter->bln->ik--;
	else               toparentrecurserev(iter);
}
static void iter_node_rev(btIterator *iter) {
	GET_NEW_CHILD(iter)
	tochildrecurserev(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
}

// INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR
static void *setIter(bt    *btr, bt_data_t  bkey, btSIter *siter, ai_obj *alow,
					 bt_n **rx,  int       *ri,   bool     asc) {
	btIterator *iter = &(siter->x);
	int         ret  = bt_init_iterator(btr, bkey, iter, alow);
	//printf("setIter: ret: %d\n", ret);
	if (ret == II_FAIL) return NULL;
	siter->empty = 0;
	if      (ret == II_L_MISS) {
		siter->nim = siter->missed = 1;
		return NULL;
	}
	else if (ret == II_MISS)     siter->nim = siter->missed = 1;
	else if (ret != II_OK) { /* range queries, find nearest match */
		int x = btr->cmp(bkey, KEYS(btr, iter->bln->self, iter->bln->ik));
		if (x > 0) {
			if (ret == II_ONLY_RIGHT) { // off end of B-tree
				siter->empty = 1;
				return NULL;
			} else { // II_LEAF_EXIT
				//printf("setIter: [II_LEAF_EXIT\n"); //TODO needed?
				return btNext(siter, rx, ri, asc); // find next
			}
		}
	}
	if (rx) *rx = iter->bln->self;
	if (ri) *ri = iter->bln->ik;
	return KEYS(iter->btr, iter->bln->self, iter->bln->ik);
}
static void init_iter(btIterator  *iter, bt          *btr,
					  iter_single *itl, iter_single *itn) {
	iter->btr         = btr;
	iter->high      = LONG_MIN;
	iter->iLeaf       = itl;
	iter->iNode     = itn;
	iter->finished    = 0;
	iter->num_nodes = 0;
	iter->bln         = &(iter->nodes[0]);
	iter->bln->ik     = iter->bln->in         = 0;
	iter->num_nodes++;
	iter->bln->self   = btr->root;
	iter->bln->parent = iter->bln->child  = NULL;
	iter->depth       = 0;
}

// AEROSPIKE MULTI_THREAD
static btSIter *newIter() {
	btSIter *siter = cf_malloc(sizeof(btSIter));
	bzero(siter, sizeof(btSIter));
	return siter;
}

static btSIter *getIterator() {
	return newIter();
}

static void releaseIterator(btSIter *siter) {
	if (siter) {
		cf_free(siter);
	}
	return;
}

static btSIter *createIterator(bt *btr, iter_single *itl, iter_single *itn) {
	btSIter *siter = getIterator();
	siter->dofree  = 1;
	siter->missed  = 0;
	siter->nim     = 0;
	siter->empty   = 1;
	siter->scan    = 0;
	siter->ktype   = btr->s.ktype;
	init_ai_obj(&siter->key);
	siter->be.key  = &(siter->key);
	siter->be.val = NULL;
	init_iter(&siter->x, btr, itl, itn);
	return siter;
}
//extra insertion

static btSIter *setIterator(btSIter *iter, bt *btr, iter_single *itl, iter_single *itn) {
	btSIter *siter = iter;
	siter->dofree  = 0;
	siter->missed  = 0;
	siter->nim     = 0;
	siter->empty   = 1;
	siter->scan    = 0;
	siter->ktype   = btr->s.ktype;
	init_ai_obj(&siter->key);
	siter->be.key  = &(siter->key);
	siter->be.val = NULL;
	init_iter(&siter->x, btr, itl, itn);
	return siter;
}
void btReleaseRangeIterator(btSIter *siter) {
	if (!siter) return;
	if (siter->dofree) {
		releaseIterator(siter);
	}
}
static void setHigh(btSIter *siter, ai_obj *high, col_type_t ktype) {
	if (C_IS_L(ktype) || C_IS_G(ktype)) {
		siter->x.high  = high->l;
	}
	else if (C_IS_DG(ktype)) {
		siter->x.highy = high->y;
	}
}

static bool streamToBTEntry(uchar *stream, btSIter *siter, bt_n *x, int i) {
	if (!stream) return 0;
	if (i < 0) i = 0;
	convertStream2Key(stream, siter->be.key, siter->x.btr);
	siter->be.val    = parseStream(stream, siter->x.btr);
	bool  gost       = IS_GHOST(siter->x.btr, siter->be.val);
	if (gost) {
		siter->missed = 1;    // GHOST key
		siter->nim = 0;
	}
	siter->be.dr = x ? getDR(siter->x.btr, x, i) : 0;
	siter->be.stream = stream;
	siter->be.x      = x;
	siter->be.i = i; //NOTE: used by bt_validate_dirty
	//DUMP_STREAM_TO_BT_ENTRY
	return 1;
}
btSIter *btGetRangeIter(bt *btr, ai_obj *alow, ai_obj *ahigh, bool asc) {
	if (!btr->root || !btr->numkeys)           return NULL;
	btk_t btk;
	bool med;
	uint32 ksize;           //bt_dumptree(btr, btr->ktype);
	CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
	setHigh(siter, asc ? ahigh : alow, btr->s.ktype);
	char    *bkey  = createBTKey(asc ? alow : ahigh,
								 &med, &ksize, btr, &btk); //D032
	if (!bkey)                                 goto rangeiter_err;
	bt_n *x  = NULL;
	int i = -1;
	uchar *stream = setIter(btr, bkey, siter, asc ? alow : ahigh, &x, &i, asc);
	destroyBTKey(bkey, med);                                /* DESTROYED 032 */
	if (!streamToBTEntry(stream, siter, x, i)) goto rangeiter_err;
	return siter;

rangeiter_err:
	btReleaseRangeIterator(siter);
	return NULL;
}


btSIter *btSetRangeIter(btSIter * iter, bt *btr, ai_obj *alow, ai_obj *ahigh, bool asc) {
	if (!btr->root || !btr->numkeys)           return NULL;
	btk_t btk;
	bool med;
	uint32 ksize;           //bt_dumptree(btr, btr->ktype);
	SETITER8R(iter, btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
	setHigh(siter, asc ? ahigh : alow, btr->s.ktype);
	char    *bkey  = createBTKey(asc ? alow : ahigh,
								 &med, &ksize, btr, &btk); //D032
	if (!bkey)                                 goto rangeiter_err;
	bt_n *x  = NULL;
	int i = -1;
	uchar *stream = setIter(btr, bkey, siter, asc ? alow : ahigh, &x, &i, asc);
	destroyBTKey(bkey, med);                                /* DESTROYED 032 */
	if (!streamToBTEntry(stream, siter, x, i)) goto rangeiter_err;
	return siter;

rangeiter_err:
	btReleaseRangeIterator(siter);
	return NULL;
}
btEntry *btRangeNext(btSIter *siter, bool asc) { //printf("btRangeNext\n");
	//printf("btRangeNext: siter: %p\n", (void *)siter);
	//if (siter) printf("btRangeNext: empty: %d\n", siter->empty);
	if (!siter || siter->empty) return NULL;
	bt_n *x  = NULL;
	int i = -1;
	uchar *stream = btNext(siter, &x, &i, asc);
	if (!streamToBTEntry(stream, siter, x, i)) return NULL;
	if (C_IS_L(siter->ktype) || C_IS_G(siter->ktype)) {
		long l = siter->key.l;
		if (l == siter->x.high)  siter->x.finished = 1;       /* exact match */
		if (!asc) {
			//printf("btRangeNext: DESC: l: %lu dr: %u\n",
			//       l, getDR(siter->x.btr, x, i));
			l += getDR(siter->x.btr, x, i);
		}
		bool over = asc ? (l > siter->x.high) : (l < siter->x.high);
		if (over && siter->nim) {
			siter->missed = 1;
		}
		//printf("btRangeNext: over: %d l: %lu high: %lu\n",
		//       over, l, siter->x.high);
		return over ? NULL : &(siter->be);
	} else if (C_IS_DG(siter->ktype)) {
		uint160 yy = siter->key.y;
		int ret = u160Cmp(&yy, &siter->x.highy);
		if (!ret) siter->x.finished = 1;                      /* exact match */
		if (!asc) { //TODO is ENDIANness of memcpy() correct
			uint32 low;
			char *spot = ((char *)&yy) + 12;
			memcpy(&low, spot, 4);
			low += getDR(siter->x.btr, x, i);
			memcpy(spot, &low, 4);
		}
		bool over = asc ? (ret > 0) : (ret < 0);
		return over ? NULL : &(siter->be);
	} else {
		return NULL;
	}
}

// FULL_BTREE_ITERATOR FULL_BTREE_ITERATOR FULL_BTREE_ITERATOR
bool assignMinKey(bt *btr, ai_obj *akey) {       //TODO combine w/ setIter()
	void *e = bt_min(btr);
	if (!e)   return 0; //      iter can be initialised
	convertStream2Key(e, akey, btr);
	return 1; //      w/ this lookup
}
bool assignMaxKey(bt *btr, ai_obj *akey) {
	void *e = bt_max(btr);
	if (!e)   return 0;
	convertStream2Key(e, akey, btr);
	return 1;
}
btSIter *btGetFullRangeIter(bt *btr, bool asc, cswc_t *w) {
	cswc_t W; // used in setHigh()
	if (!btr->root || !btr->numkeys)                      return NULL;
	if (!w) w = &W;
	ai_obj *aL = &w->wf.alow, *aH = &w->wf.ahigh;
	if (!assignMinKey(btr, aL) || !assignMaxKey(btr, aH)) return NULL;
	btk_t btk;
	bool med;
	uint32 ksize;
	CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
	siter->scan = 1;
	setHigh(siter, asc ? aH : aL, btr->s.ktype);
	char *bkey  = createBTKey(asc ? aL : aH,
							  &med, &ksize, btr, &btk); //DEST 030
	if (!bkey)                                            goto frangeiter_err;
	bt_n *x  = NULL;
	int i = -1;
	uchar *stream = setIter(btr, bkey, siter, asc ? aL : aH, &x, &i, asc);
	destroyBTKey(bkey, med);                             /* DESTROYED 030 */
	if (!stream && siter->missed)                         return siter;//IILMISS
	if (!streamToBTEntry(stream, siter, x, i))            goto frangeiter_err;
	if (btr->dirty_left) siter->missed = 1; // FULL means 100% FULL
	return siter;

frangeiter_err:
	btReleaseRangeIterator(siter);
	return NULL;
}

btSIter *btSetFullRangeIter(btSIter *iter, bt *btr, bool asc, cswc_t *w) {
	cswc_t W; // used in setHigh()
	if (!btr->root || !btr->numkeys)                      return NULL;
	if (!w) w = &W;
	ai_obj *aL = &w->wf.alow, *aH = &w->wf.ahigh;
	if (!assignMinKey(btr, aL) || !assignMaxKey(btr, aH)) return NULL;
	btk_t btk;
	bool med;
	uint32 ksize;
	SETITER8R(iter, btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
	siter->scan = 1;
	setHigh(siter, asc ? aH : aL, btr->s.ktype);
	char *bkey  = createBTKey(asc ? aL : aH,
							  &med, &ksize, btr, &btk); //DEST 030
	if (!bkey)                                            goto frangeiter_err;
	bt_n *x  = NULL;
	int i = -1;
	uchar *stream = setIter(btr, bkey, siter, asc ? aL : aH, &x, &i, asc);
	destroyBTKey(bkey, med);                             /* DESTROYED 030 */
	if (!stream && siter->missed)                         return siter;//IILMISS
	if (!streamToBTEntry(stream, siter, x, i))            goto frangeiter_err;
	if (btr->dirty_left) siter->missed = 1; // FULL means 100% FULL
	return siter;

frangeiter_err:
	btReleaseRangeIterator(siter);
	return NULL;
}

typedef struct four_longs {
	long cnt;
	long ofst;
	long diff;
	long over;
} fol_t;

#define INIT_ITER_BEENTRY(siter, btr, x, i)  \
  { uchar *iistream = KEYS(btr, x, i); streamToBTEntry(iistream, siter, x, i); }
static bool btScionFind(btSIter *siter, bt_n *x, ulong ofst, bt *btr, bool asc,
						cswc_t  *w,     long  lim) {
	int    i   = asc ? 0        : x->n;
	int    fin = asc ? x->n + 1 : -1;
	while (i != fin) {
		if (x->leaf) break;
		uint32_t scion = NODES(btr, x)[i]->scion;
		if (scion >= ofst) {
			bool i_end_n     = (i == siter->x.bln->self->n);
			siter->x.bln->in = i;
			siter->x.bln->ik = (i_end_n) ? i - 1 : i;
			if (scion == ofst) {
				if (!asc) {
					siter->x.bln->in = siter->x.bln->ik = i - 1;
				}
				return 1;
			}
			siter->x.bln->child = get_new_iter_child(&siter->x);
			to_child(&siter->x, NODES(btr, x)[i]);
			bt_n *kid = NODES(btr, x)[i];
			if (!kid->leaf) {
				btScionFind(siter, kid, ofst, btr, asc, w, lim);
				return 1;
			} else x = kid;
			break;
		} else ofst -= (scion + 1); // +1 for NODE itself
		i = asc ? i + 1 : i - 1;    // loop increment
	}
	// Now Find the rest of the OFFSET (respecting DRs)
	uint32  n    = siter->x.bln->self->n;
	i            = asc ? 0            : n - 1;
	fin          = asc ? MIN(ofst, n) : MAX(-1, (n - ofst));
	int last     = asc ? n - 1        : 0;
	ulong   cnt  = 0;
	//TODO findminnode() is too inefficient -> needs to be a part of btr
	bt_n   *minx = findminnode(btr, btr->root);
	int     btdl = btr->dirty_left;
	int     dr   = 0;
	while (i != fin) {
		dr   = getDR(btr, x, i);
		cnt += dr;
		if (!i && x == minx) cnt += btdl;
		if (cnt >= ofst) break;
		cnt++;
		i = asc ? i + 1 : i - 1; // loop increment
	}
	if      (i == fin && i == last) {
		if (cnt >= x->scion) return 0;
	}
	else if (cnt < ofst)                                   return 0; //OFST 2big
	siter->x.bln->ik = i;
	INIT_ITER_BEENTRY(siter, btr, x, siter->x.bln->ik);
	if (asc)  {
		if ((ofst + dr) != cnt) siter->missed = 1;
	}
	else      {
		if (!i && x == minx) {
			if (ofst != (cnt - btdl)) siter->missed = 1;
		}
		else                 {
			if (ofst != cnt)          siter->missed = 1;
		}
	}
	return 1;
}
btSIter *btGetFullXthIter(bt *btr, ulong oofst, bool asc, cswc_t *w, long lim) {
	ulong ofst = oofst;
	cswc_t W; // used in setHigh()
	if (!btr->root || !btr->numkeys)                      return NULL;
	if (!w) w = &W;
	ai_obj *aL = &w->wf.alow, *aH = &w->wf.ahigh;
	if (!assignMinKey(btr, aL) || !assignMaxKey(btr, aH)) return NULL;
	CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
	setHigh(siter, asc ? aH : aL, btr->s.ktype);
	if (btScionFind(siter, btr->root, ofst, btr, asc, w, lim)) siter->empty = 0;
	return siter;
}
