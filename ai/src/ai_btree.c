/*
 * ai_btree.c
 *
 * Copyright (C) 2013-2021 Aerospike, Inc.
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

#include <string.h>

#include "ai_obj.h"
#include "ai_btree.h"
#include "bt_iterator.h"
#include "stream.h"

#include "base/cfg.h"
#include "base/index.h"
#include "base/thr_query.h"
#include "fabric/partition.h"
#include "sindex/gc.h"
#include "sindex/secondary_index.h"

#include "arenax.h"
#include "log.h"
#include "shash.h"

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_ll.h>

// Rounds up to a 512-byte array, including overhead.
#define AI_ARR_MAX_USED 63

#define SINDEX_GC_NUM_OBJS_PER_ARR 100

#define R_H_HASH_RC_THRESHOLD 10000

typedef struct acol_handle_s {
	cf_arenax_handle r_h;
	ai_obj acol;
} acol_handle;

typedef struct objs_to_defrag_arr_s {
	uint32_t num;
	acol_handle acol_handles[SINDEX_GC_NUM_OBJS_PER_ARR];
} objs_to_defrag_arr;

typedef struct ll_sindex_gc_element_s {
	cf_ll_element ele;
	objs_to_defrag_arr objs_to_defrag;
} ll_sindex_gc_element;

static const uint8_t INIT_CAPACITY = 1;

static ai_arr *
ai_arr_new()
{
	ai_arr *arr = cf_malloc_ns(sizeof(ai_arr) + (INIT_CAPACITY * L_SIZE));
	arr->capacity = INIT_CAPACITY;
	arr->used = 0;
	return arr;
}

static void
ai_arr_move_to_tree(ai_arr *arr, bt *nbtr)
{
	for (int i = 0; i < arr->used; i++) {
		ai_obj apk;
		init_ai_objLong(&apk, arr->data[i]);
		if (!btIndNodeAdd(nbtr, &apk)) {
			// what to do ??
			continue;
		}
	}
}

/*
 * Side effect if success full *arr will be freed
 */
static void
ai_arr_destroy(ai_arr *arr)
{
	if (!arr) return;
	cf_free(arr);
}

static int
ai_arr_size(ai_arr *arr)
{
	if (!arr) return 0;
	return(sizeof(ai_arr) + (arr->capacity * L_SIZE));
}

/*
 * Finds the digest in the AI array.
 * Returns
 *      idx if found
 *      -1  if not found
 */
static int
ai_arr_find(ai_arr *arr, ulong l)
{
	for (int i = 0; i < arr->used; i++) {
		if (l == arr->data[i]) {
			return i;
		}
	}
	return -1;
}

static ai_arr *
ai_arr_shrink(ai_arr *arr)
{
	int size = arr->capacity / 2;

	// Do not shrink if the capacity not greater than 4
	// or if the halving capacity is not a extra level
	// over currently used
	if ((arr->capacity <= 4) || (size < arr->used * 2)) {
		return arr;
	}

	ai_arr * temp_arr = cf_realloc_ns(arr, sizeof(ai_arr) + (size * L_SIZE));
	temp_arr->capacity = size;
	return temp_arr;
}

static ai_arr *
ai_arr_delete(ai_arr *arr, ulong l, bool *notfound)
{
	int idx = ai_arr_find(arr, l);
	// Nothing to delete
	if (idx < 0) {
		*notfound = true;
		return arr;
	}
	if (idx != arr->used - 1) {
		// move last element
		ulong *data = arr->data;
		data[idx] = data[arr->used - 1];
	}
	arr->used--;
	return ai_arr_shrink(arr);
}

/*
 * Returns
 *      arr pointer in case of successful operation
 *      NULL in case of failure
 */
static ai_arr *
ai_arr_expand(ai_arr *arr)
{
	int size = arr->capacity * 2;

	if (size > AI_ARR_MAX_SIZE) {
		cf_crash(AS_SINDEX, "Refusing to expand ai_arr to %d (beyond limit of %d)",
				size, AI_ARR_MAX_SIZE);
	}

	arr = cf_realloc_ns(arr, sizeof(ai_arr) + (size * L_SIZE));
	//cf_info(AS_SINDEX, "EXPAND REALLOC to %d", size);
	arr->capacity = size;
	return arr;
}

/*
 * Returns
 *      arr in case of success
 *      NULL in case of failure
 */
static ai_arr *
ai_arr_insert(ai_arr *arr, ulong l, bool *found)
{
	int idx = ai_arr_find(arr, l);
	// already found
	if (idx >= 0) {
		*found = true;
		return arr;
	}
	if (arr->used == arr->capacity) {
		arr = ai_arr_expand(arr);
	}

	arr->data[arr->used++] = l;
	return arr;
}

/*
 * Returns the size diff
 */
static int
anbtr_check_convert(ai_nbtr *anbtr)
{
	// Nothing to do
	if (anbtr->is_btree)
		return 0;

	ai_arr *arr = anbtr->u.arr;
	if (arr && (arr->used >= AI_ARR_MAX_USED)) {
		//cf_info(AS_SINDEX,"Flipped @ %d", arr->used);
		ulong ba = ai_arr_size(arr);
		// Allocate btree move digest from arr to btree
		bt *nbtr = createNBT();
		if (!nbtr) {
			cf_warning(AS_SINDEX, "btree allocation failure");
			return 0;
		}

		ai_arr_move_to_tree(arr, nbtr);
		ai_arr_destroy(anbtr->u.arr);

		// Update anbtr
		anbtr->u.nbtr = nbtr;
		anbtr->is_btree = true;

		ulong aa = nbtr->msize;
		return (aa - ba);
	}
	return 0;
}

/*
 * Insert operation for the nbtr does the following
 * 1. Sets up anbtr if it is set up
 * 2. Inserts in the arr or nbtr depending number of elements.
 * 3. Cuts over from arr to btr at AI_ARR_MAX_USED
 *
 * Parameter:   ibtr  : Btree of key
 *              acol  : Secondary index key
 *              apk   : value (primary key to be inserted)
 *
 * Returns:
 *      AS_SINDEX_OK        : In case of success
 *      AS_SINDEX_ERR       : In case of failure
 *      AS_SINDEX_KEY_FOUND : If key already exists
 */
static int
reduced_iAdd(bt *ibtr, ai_obj *acol, ai_obj *apk)
{
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(ibtr, acol);
	ulong ba = 0, aa = 0;

	if (!anbtr) {
		anbtr = cf_calloc_ns(1, sizeof(ai_nbtr));
		aa += sizeof(ai_nbtr);

		anbtr->u.arr = ai_arr_new();
		ibtr->nsize += ai_arr_size(anbtr->u.arr);

		btIndAdd(ibtr, acol, (bt *)anbtr);
	}

	// Convert from arr to nbtr if limit is hit
	ibtr->nsize += anbtr_check_convert(anbtr);

	// If already a btree use it
	if (anbtr->is_btree) {
		bt *nbtr = anbtr->u.nbtr;
		if (!nbtr) {
			return AS_SINDEX_ERR;
		}

		if (btIndNodeExist(nbtr, apk)) {
			return AS_SINDEX_KEY_FOUND;
		}

		ba += nbtr->msize;
		if (!btIndNodeAdd(nbtr, apk)) {
			return AS_SINDEX_ERR;
		}
		aa += nbtr->msize;

	} else {
		ai_arr *arr = anbtr->u.arr;
		if (!arr) {
			return AS_SINDEX_ERR;
		}

		ba += ai_arr_size(anbtr->u.arr);
		bool found = false;
		ai_arr *t_arr = ai_arr_insert(arr, apk->l, &found);
		if (found) {
			return AS_SINDEX_KEY_FOUND;
		}
		anbtr->u.arr = t_arr;
		aa += ai_arr_size(anbtr->u.arr);
	}
	ibtr->nsize += (aa - ba);  // ibtr inherits nbtr

	return AS_SINDEX_OK;
}

/*
 * Delete operation for the nbtr does the following. Delete in the arr or nbtr
 * based on state of anbtr
 *
 * Parameter:   ibtr  : Btree of key
 *              acol  : Secondary index key
 *              apk   : value (primary key to be inserted)
 *
 * Returns:
 *      AS_SINDEX_OK           : In case of success
 *      AS_SINDEX_ERR          : In case of failure
 *      AS_SINDEX_KEY_NOTFOUND : If key does not exist
 */
static int
reduced_iRem(bt *ibtr, ai_obj *acol, ai_obj *apk)
{
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(ibtr, acol);
	ulong ba = 0, aa = 0;
	if (!anbtr) {
		return AS_SINDEX_KEY_NOTFOUND;
	}
	if (anbtr->is_btree) {
		if (!anbtr->u.nbtr) return AS_SINDEX_ERR;

		// Remove from nbtr if found
		bt *nbtr = anbtr->u.nbtr;
		if (!btIndNodeExist(nbtr, apk)) {
			return AS_SINDEX_KEY_NOTFOUND;
		}
		ba = nbtr->msize;

		// TODO - Needs to be cleaner, type convert from signed
		// to unsigned. Should be 64 bit !!
		int nkeys_before = nbtr->numkeys; 
		int nkeys_after = btIndNodeDelete(nbtr, apk, NULL);
		aa = nbtr->msize;

		if (nkeys_after == nkeys_before) {
			return AS_SINDEX_KEY_NOTFOUND;
		}

		// remove from ibtr
		if (nkeys_after == 0) {
			btIndDelete(ibtr, acol);
			aa = 0;
			bt_destroy(nbtr);
			ba += sizeof(ai_nbtr);
			cf_free(anbtr);
		}
	} else {
		if (!anbtr->u.arr) return AS_SINDEX_ERR;

		// Remove from arr if found
		bool notfound = false;
		ba = ai_arr_size(anbtr->u.arr);
		anbtr->u.arr = ai_arr_delete(anbtr->u.arr, apk->l, &notfound);
		if (notfound) return AS_SINDEX_KEY_NOTFOUND;
		aa = ai_arr_size(anbtr->u.arr);

		// Remove from ibtr
		if (anbtr->u.arr->used == 0) {
			btIndDelete(ibtr, acol);
			aa = 0;
			ai_arr_destroy(anbtr->u.arr);
			ba += sizeof(ai_nbtr);
			cf_free(anbtr);
		}
	}
	ibtr->nsize -= (ba - aa);

	return AS_SINDEX_OK;
}

/*
 * r_h will be valid (not necessarily in the tree) as we are under a pimd lock
 * 1. updates and ns->storage_data_in_memory deletes need pimd lock
 * 2. other deletes will queue to sindex gc which needs pimd lock
 */
static int
btree_addsinglerec(as_sindex_metadata *imd, cf_arenax_handle r_h,
		as_sindex_qctx *qctx)
{
	as_namespace *ns = imd->si->ns;
	as_index *r = (as_index *)cf_arenax_resolve(ns->arena, r_h);
	uint32_t pid = as_partition_getid(&r->keyd);
	uint16_t rc = r->rc;

	// Once sindex is marked for destruction, write can reset in_sindex.
	cf_assert(imd->si->state == AS_SINDEX_DESTROY || rc > 0, AS_SINDEX,
			"invalid rc %u in_sindex %u gen %lu si-state %u", rc,
			(uint16_t)r->in_sindex, (uint64_t)r->generation, imd->si->state);

	if (! as_query_reserve_partition_tree(ns, qctx, pid) ||
			r->tree_id != qctx->reserved_trees[pid]->id ||
			r->generation == 0) {
		return 0;
	}

	static const uint8_t dummy_value = 0;

	// Only list/map bins may go to high refcount. For other types, refcount
	// should not be higher than the number of parallel queries and scans.
	// Delay hashtable creation till absolutely necessary as it is expensive.
	if (rc >= R_H_HASH_RC_THRESHOLD) {
		if (qctx->r_h_hash == NULL) {
			// No locking because only query generator uses this.
			qctx->r_h_hash = cf_shash_create(cf_shash_fn_u32,
					sizeof(cf_arenax_chunk), 0, 64 * 1024, 0);
		}

		if (cf_shash_put_unique(qctx->r_h_hash, &r_h, &dummy_value) ==
				CF_SHASH_ERR_FOUND) {
			return 0;
		}
	}

	// TODO - proper EE split.
	if (ns->xmem_type != CF_XMEM_TYPE_FLASH) {
		// Reserve to protect against future deletes.
		cf_mutex* rlock = as_index_rlock_from_keyd(qctx->reserved_trees[pid],
				&r->keyd);

		cf_mutex_lock(rlock);

		if (r->generation == 0) {
			cf_mutex_unlock(rlock);
			return 0;
		}

		as_index_reserve(r);
		cf_mutex_unlock(rlock);
	}

	cf_ll *recl = qctx->recl;
	bool create = cf_ll_size(recl) == 0;
	as_index_keys_arr *keys_arr;

	if (!create) {
		cf_ll_element *ele = cf_ll_get_tail(recl);
		keys_arr = ((as_index_keys_ll_element*)ele)->keys_arr;
		if (keys_arr->num == AS_INDEX_KEYS_PER_ARR) {
			create = true;
		}
	}

	if (create) {
		keys_arr = cf_malloc(sizeof(as_index_keys_arr));
		keys_arr->num = 0;

		as_index_keys_ll_element *node = cf_malloc(sizeof(as_index_keys_ll_element));
		node->keys_arr = keys_arr;

		cf_ll_append(recl, (cf_ll_element *)node);
	}

	// TODO - proper EE split.
	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		keys_arr->u.digests[keys_arr->num] = r->keyd;
	}
	else {
		keys_arr->u.handles[keys_arr->num] = (uint64_t)r_h;
	}

	keys_arr->num++;
	qctx->n_recs++;

	return 0;
}

/*
 * Return 0 in case of success
 *       -1 in case of failure
 */
static int
add_recs_from_nbtr(as_sindex_metadata *imd, ai_obj *ikey, bt *nbtr,
		as_sindex_qctx *qctx, bool fullrng)
{
	ai_obj sfk, efk;
	init_ai_obj(&sfk);
	init_ai_obj(&efk);
	btSIter *nbi;
	btEntry *nbe;
	btSIter stack_nbi;

	if (fullrng) {
		nbi = btSetFullRangeIter(&stack_nbi, nbtr, 1, NULL);
	} else { // search from LAST batches end-point
		init_ai_objLong(&sfk, qctx->nbtr_last_key);
		assignMaxKey(nbtr, &efk);
		nbi = btSetRangeIter(&stack_nbi, nbtr, &sfk, &efk, 1);
	}

	// nbi will be NULL if elements >= sfk got deleted since last round.
	if (nbi == NULL) {
		return 0;
	}

	int ret = 0;

	while ((nbe = btRangeNext(nbi, 1))) {
		ai_obj *akey = nbe->key;
		// FIRST can be REPEAT (last batch)
		if (!fullrng && ai_objEQ(&sfk, akey)) {
			continue;
		}
		if (btree_addsinglerec(imd, (cf_arenax_handle)akey->l, qctx)) {
			ret = -1;
			break;
		}
		if (qctx->n_recs == qctx->bsize) {
			if (ikey) {
				ai_objClone(qctx->ibtr_last_key, ikey);
			}
			qctx->nbtr_last_key = akey->l;
			break;
		}
	}

	btReleaseRangeIterator(nbi);

	return ret;
}

static int
add_recs_from_arr(as_sindex_metadata *imd, ai_obj *ikey, ai_arr *arr,
		as_sindex_qctx *qctx)
{
	bool ret = 0;

	for (int i = 0; i < arr->used; i++) {
		if (btree_addsinglerec(imd, (cf_arenax_handle)arr->data[i], qctx)) {
			ret = -1;
			break;
		}
		// do not break on hitting batch limit, if the tree converts to
		// bt from arr, there is no way to know which digest were already
		// returned when attempting subsequent batch. Return the entire
		// thing.
	}
	// mark nbtr as finished and copy the offset
	qctx->nbtr_done = true;
	if (ikey) {
		ai_objClone(qctx->ibtr_last_key, ikey);
	}

	return ret;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
get_recl(as_sindex_metadata *imd, ai_obj *afk, as_sindex_qctx *qctx)
{
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_ix];
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(pimd->ibtr, afk);

	if (!anbtr) {
		return 0;
	}

	if (anbtr->is_btree) {
		if (add_recs_from_nbtr(imd, afk, anbtr->u.nbtr, qctx, qctx->new_ibtr)) {
			return -1;
		}
	} else {
		// If already entire batch is returned
		if (qctx->nbtr_done) {
			return 0;
		}
		if (add_recs_from_arr(imd, afk, anbtr->u.arr, qctx)) {
			return -1;
		}
	}
	return 0;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
get_numeric_range_recl(as_sindex_metadata *imd, uint64_t begk, uint64_t endk, as_sindex_qctx *qctx)
{
	ai_obj sfk;
	init_ai_objLong(&sfk, qctx->new_ibtr ? begk : qctx->ibtr_last_key->l);
	ai_obj efk;
	init_ai_objLong(&efk, endk);
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_ix];
	bool fullrng              = qctx->new_ibtr;
	int ret                   = 0;
	btSIter *bi               = btGetRangeIter(pimd->ibtr, &sfk, &efk, 1);
	btEntry *be;

	if (bi) {
		while ((be = btRangeNext(bi, 1))) {
			ai_obj  *ikey  = be->key;
			ai_nbtr *anbtr = be->val;

			if (!anbtr) {
				ret = -1;
				break;
			}

			// figure out nbtr to deal with. If the key which was
			// used last time vanishes work with next key. If the
			// key exist but 'last' entry made to list in the last
			// iteration; Move to next nbtr
			if (!fullrng) {
				if (!ai_objEQ(&sfk, ikey)) {
					fullrng = 1; // bkey disappeared
				} else if (qctx->nbtr_done) {
					qctx->nbtr_done = false;
					// If we are moving to the next key, we need 
					// to search the full range.
					fullrng = 1;
					continue;
				}
			}

			if (anbtr->is_btree) {
				if (add_recs_from_nbtr(imd, ikey, anbtr->u.nbtr, qctx, fullrng)) {
					ret = -1;
					break;
				}
			} else {
				if (add_recs_from_arr(imd, ikey, anbtr->u.arr, qctx)) {
					ret = -1;
					break;
				}
			}

			// Since add_recs_from_arr() returns entire thing and do not support the batch limit,
			// >= operator is needed here.
			if (qctx->n_recs >= qctx->bsize) {
				break;
			}

			// If it reaches here, this means last key could not fill the batch.
			// So if we are to start a new key, search should be done on full range 
			// and the new nbtr is obviously not done.
			fullrng         = 1;
			qctx->nbtr_done = false;
		}
		btReleaseRangeIterator(bi);
	}
	return ret;
}

int
ai_btree_query(as_sindex_metadata *imd, const as_query_range *srange,
		as_sindex_qctx *qctx)
{
	bool err = 1;
	if (!srange->isrange) { // EQUALITY LOOKUP
		ai_obj afk;
		init_ai_obj(&afk);
		if (C_IS_DG(imd->sktype)) {
			init_ai_objU160(&afk, srange->u.digest);
		}
		else {
			// geo queries can never be point queries. Must be NUMERIC.
			init_ai_objLong(&afk, srange->u.r.start);
		}
		err = get_recl(imd, &afk, qctx);
	} else {                // RANGE LOOKUP
		if (C_IS_L(imd->sktype)) {
			err = get_numeric_range_recl(imd, srange->u.r.start, srange->u.r.end,
					qctx);
		}
		else {
			const as_query_range_start_end *r =
					&srange->u.geo.r[qctx->range_index];
			err = get_numeric_range_recl(imd, r->start, r->end, qctx);
		}
	}
	return (err ? AS_SINDEX_ERR_NO_MEMORY : AS_SINDEX_OK);
}

as_sindex_status
ai_btree_put(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *skey,
		cf_arenax_handle r_h)
{
	ai_obj ncol;
	if (C_IS_DG(imd->sktype)) {
		init_ai_objU160(&ncol, *(uint160 *)skey);
	}
	else {
		// TODO - ai_obj type is LONG for both Geo and Long
		init_ai_objLong(&ncol, *(ulong *)skey);
	}

	ai_obj apk;
	init_ai_objLong(&apk, r_h);

	uint64_t before = pimd->ibtr->msize + pimd->ibtr->nsize;
	int ret = reduced_iAdd(pimd->ibtr, &ncol, &apk);
	uint64_t after = pimd->ibtr->msize + pimd->ibtr->nsize;
	cf_atomic64_add(&imd->si->ns->n_bytes_sindex_memory, (after - before));

	if (ret && ret != AS_SINDEX_KEY_FOUND) {
		cf_warning(AS_SINDEX, "Insert into the btree failed");
		return AS_SINDEX_ERR_NO_MEMORY;
	}
	return ret;
}

as_sindex_status
ai_btree_delete(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *skey,
		cf_arenax_handle r_h)
{
	int ret = AS_SINDEX_OK;

	if (!pimd->ibtr) {
		return AS_SINDEX_KEY_NOTFOUND;
	}

	ai_obj ncol;
	if (C_IS_DG(imd->sktype)) {
		init_ai_objU160(&ncol, *(uint160 *)skey);
	}
	else {
		// TODO - ai_obj type is LONG for both Geo and Long
		init_ai_objLong(&ncol, *(ulong *)skey);
	}

	ai_obj apk;
	init_ai_objLong(&apk, r_h);

	uint64_t before = pimd->ibtr->msize + pimd->ibtr->nsize;
	ret = reduced_iRem(pimd->ibtr, &ncol, &apk);
	uint64_t after = pimd->ibtr->msize + pimd->ibtr->nsize;
	cf_atomic64_sub(&imd->si->ns->n_bytes_sindex_memory, (before - after));

	return ret;
}

/*
 * Internal function which adds digests to the defrag_list
 * Mallocs the nodes of defrag_list
 */
static void
build_defrag_list_from_nbtr(as_namespace *ns, ai_obj *ikey, bt *nbtr,
		cf_arenax_handle *nbtr_last_key, ulong *limit, cf_ll *gc_list)
{
	btEntry *nbe;
	ai_obj sfk;
	ai_obj efk;

	init_ai_objLong(&sfk, (ulong)*nbtr_last_key);
	assignMaxKey(nbtr, &efk);

	btSIter stack_nbi;

	// STEP 1: go thru a portion of the nbtr and find to-be-deleted-PKs
	btSIter *nbi = btSetRangeIter(&stack_nbi, nbtr, &sfk, &efk, true);

	// nbi will be NULL if elements >= sfk got deleted since last round.
	if (nbi == NULL) {
		return;
	}

	while ((nbe = btRangeNext(nbi, 1))) {
		ai_obj *akey = nbe->key;
		cf_arenax_handle r_h = (cf_arenax_handle)akey->l;

		if (as_sindex_can_defrag_record(ns, r_h)) {

			bool create   = (cf_ll_size(gc_list) == 0) ? true : false;
			objs_to_defrag_arr *dt;

			if (!create) {
				cf_ll_element * ele = cf_ll_get_tail(gc_list);
				dt = &((ll_sindex_gc_element*)ele)->objs_to_defrag;
				if (dt->num == SINDEX_GC_NUM_OBJS_PER_ARR) {
					create = true;
				}
			}
			if (create) {
				ll_sindex_gc_element *node =
						cf_malloc(sizeof(ll_sindex_gc_element));
				dt = &node->objs_to_defrag;
				dt->num = 0;
				cf_ll_append(gc_list, (cf_ll_element *)node);
			}
			dt->acol_handles[dt->num].r_h = r_h;
			ai_objClone(&(dt->acol_handles[dt->num].acol), ikey);

			dt->num += 1;		
		}
		(*limit)--;
		if (*limit == 0) {
			*nbtr_last_key = r_h;
			break;
		}
	}
	btReleaseRangeIterator(nbi);
}

static void
build_defrag_list_from_arr(as_namespace *ns, ai_obj *acol, ai_arr *arr,
		ulong *limit, cf_ll *gc_list)
{
	for (ulong i = 0; i < arr->used; i++) {
		cf_arenax_handle r_h = (cf_arenax_handle)arr->data[i];

		if (as_sindex_can_defrag_record(ns, r_h)) {
			bool create   = (cf_ll_size(gc_list) == 0) ? true : false;
			objs_to_defrag_arr *dt;

			if (!create) {
				cf_ll_element * ele = cf_ll_get_tail(gc_list);
				dt = &((ll_sindex_gc_element*)ele)->objs_to_defrag;
				if (dt->num == SINDEX_GC_NUM_OBJS_PER_ARR) {
					create = true;
				}
			}
			if (create) {
				ll_sindex_gc_element *node =
						cf_malloc(sizeof(ll_sindex_gc_element));
				dt = &node->objs_to_defrag;
				dt->num = 0;
				cf_ll_append(gc_list, (cf_ll_element *)node);
			}
			dt->acol_handles[dt->num].r_h = r_h;
			ai_objClone(&(dt->acol_handles[dt->num].acol), acol);

			dt->num += 1;		
		}

		if (*limit > 0) {
			(*limit)--;
		}
	}
}

/*
 * Aerospike Index interface to build a defrag_list.
 *
 * Returns :
 *  AS_SINDEX_DONE     ---> The current pimd has been scanned completely for defragging
 *  AS_SINDEX_CONTINUE ---> Current pimd sill may have some candidate digest to be defragged
 *  AS_SINDEX_ERR      ---> Error. Abort this pimd.
 *
 *  Notes :  Caller has the responsibility to free the iterators.
 *           Requires a proper offset value from the caller.
 */
as_sindex_status
ai_btree_build_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd,
		ai_obj *ibtr_last_key, cf_arenax_handle *nbtr_last_key, ulong limit,
		cf_ll *gc_list)
{
	int ret = AS_SINDEX_ERR;

	if (!pimd || !imd) {
		return ret;
	}

	as_namespace *ns = imd->si->ns;
	if (!ns) {
		ns = as_namespace_get_byname(imd->ns_name);
	}

	if (!pimd || !pimd->ibtr || !pimd->ibtr->numkeys) {
		goto END;
	}

	// Entry is range query, FROM previous icol TO maxKey(ibtr).
	ai_obj iH;
	assignMaxKey(pimd->ibtr, &iH);
	btSIter *bi = btGetRangeIter(pimd->ibtr, ibtr_last_key, &iH, 1);
	if (!bi) {
		goto END;
	}

	while ( true ) {
		btEntry *be = btRangeNext(bi, 1);
		if (!be) {
			ret = AS_SINDEX_DONE;
			break;
		}
		ai_obj *ikey = be->key;
		ai_nbtr *anbtr = be->val;
		if (!anbtr) {
			break;
		}
		if (anbtr->is_btree) {
			build_defrag_list_from_nbtr(ns, ikey, anbtr->u.nbtr,
					nbtr_last_key, &limit, gc_list);
		} else {
			// We always process arrays fully and may cross the limit.
			// limit == 0 indicates we met or crossed the limit.
			build_defrag_list_from_arr(ns, ikey, anbtr->u.arr, &limit, gc_list);
		}

		if (limit == 0) {
			ai_objClone(ibtr_last_key, ikey);
			cf_detail(AS_SINDEX, "Current pimd may need more iteration of defragging.");
			ret = AS_SINDEX_CONTINUE;
			break;
		}

		// Reinitialize the starting point when moving to the next nbtr.
		*nbtr_last_key = 0;
	}
	btReleaseRangeIterator(bi);
END:

	return ret;
}

/*
 * Deletes the digest as in the passed in as gc_list, bound by n2del number of
 * elements per iteration, with *deleted successful deletes.
 */
bool
ai_btree_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, cf_ll *gc_list, ulong n2del, ulong *deleted)
{
	// If n2del is zero here, that means caller do not want to defrag
	if (n2del == 0) {
		return false;
	}
	ulong success = 0;
	// STEP 3: go thru the PKtoDeleteList and delete the keys

	uint64_t before = 0;
	uint64_t after = 0;

	while (cf_ll_size(gc_list)) {
		cf_ll_element        * ele  = cf_ll_get_head(gc_list);
		ll_sindex_gc_element * node = (ll_sindex_gc_element * )ele;
		objs_to_defrag_arr   * dt   = &node->objs_to_defrag;

		// check before deleting. The digest may re-appear after the list
		// creation and before deletion from the secondary index

		int i = 0;
		while (dt->num != 0) {
			i = dt->num - 1;

			ai_obj apk;

			init_ai_objLong(&apk, dt->acol_handles[i].r_h);

			ai_obj *acol = &(dt->acol_handles[i].acol);

			cf_detail(AS_SINDEX, "Defragged %lu %lu", acol->l, apk.l);

			before += pimd->ibtr->msize + pimd->ibtr->nsize;
			if (reduced_iRem(pimd->ibtr, acol, &apk) == AS_SINDEX_OK) {
				success++;
			}
			after += pimd->ibtr->msize + pimd->ibtr->nsize;
			dt->num -= 1;
			n2del--;
			if (n2del == 0) {
				goto END;
			}
		}
		cf_ll_delete(gc_list, (cf_ll_element*)node);
	}

END:
	cf_atomic64_sub(&imd->si->ns->n_bytes_sindex_memory, (before - after));
	*deleted += success;
	return cf_ll_size(gc_list) ? true : false;
}

void ai_btree_gc_list_destroy_fn(cf_ll_element *ele)
{
	ll_sindex_gc_element* node = (ll_sindex_gc_element*)ele;

	if (node != NULL) {
		cf_free(node);
	}
}

void
ai_btree_create(as_sindex_metadata *imd)
{
	for (uint32_t i = 0; i < imd->n_pimds; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		pimd->ibtr = createIBT(imd->sktype, -1);
		if (! pimd->ibtr) {
			cf_crash(AS_SINDEX, "Failed to allocate secondary index tree for ns:%s, indexname:%s",
					imd->ns_name, imd->iname);
		}
	}
}

static void
destroy_index(bt *ibtr, bt_n *n)                        
{                                                                               
	if (! n->leaf) {                                                             
		for (int i = 0; i <= n->n; i++) {                                       
			destroy_index(ibtr, NODES(ibtr, n)[i]);                     
		}                                                                       
	}                                                                           

	for (int i = 0; i < n->n; i++) {                                            
		void *be = KEYS(ibtr, n, i);                                            
		ai_nbtr *anbtr = (ai_nbtr *) parseStream(be, ibtr);                     
		if (anbtr) {                                                            
			if (anbtr->is_btree) {                                              
				bt_destroy(anbtr->u.nbtr);                                      
			} else {                                                            
				ai_arr_destroy(anbtr->u.arr);                                   
			}                                                                   
			cf_free(anbtr);                                                     
		}                                                                       
	}                                                                           
}                 

uint64_t
ai_btree_get_numkeys(as_sindex_metadata *imd)
{
	uint64_t val = 0;

	for (uint32_t i = 0; i < imd->n_pimds; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		val += pimd->ibtr->numkeys;
		PIMD_RUNLOCK(&pimd->slock);
	}

	return val;
}

static uint64_t
ai_btree_get_pimd_isize(as_sindex_pmetadata *pimd)
{
	// TODO - Why check of > 0
	return pimd->ibtr->msize > 0 ? pimd->ibtr->msize : 0;
}

uint64_t
ai_btree_get_isize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	for (uint32_t i = 0; i < imd->n_pimds; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		size += ai_btree_get_pimd_isize(pimd);
		PIMD_RUNLOCK(&pimd->slock);
	}
	return size;
}

static uint64_t
ai_btree_get_pimd_nsize(as_sindex_pmetadata *pimd)
{
	// TODO - Why check of > 0
	return pimd->ibtr->nsize > 0 ? pimd->ibtr->nsize : 0;
}

uint64_t
ai_btree_get_nsize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	for (uint32_t i = 0; i < imd->n_pimds; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		size += ai_btree_get_pimd_nsize(pimd);
		PIMD_RUNLOCK(&pimd->slock);
	}

	return size;
}

void
ai_btree_reset_pimd(as_sindex_pmetadata *pimd)
{
	if (! pimd->ibtr) {
		cf_crash(AS_SINDEX, "IBTR is null");
	}
	pimd->ibtr = NULL;
}

void
ai_btree_delete_ibtr(bt * ibtr)
{
	if (! ibtr) {
		cf_crash(AS_SINDEX, "IBTR is null");
	}
	destroy_index(ibtr, ibtr->root); 
}
