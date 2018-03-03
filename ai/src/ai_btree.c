/*
 * ai_btree.c
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

#include <sys/time.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "ai_obj.h"
#include "ai_btree.h"
#include "bt_iterator.h"
#include "bt_output.h"
#include "stream.h"
#include "base/thr_sindex.h"
#include "base/cfg.h"
#include "fabric/partition.h"

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_ll.h>

#include "fault.h"

#define AI_ARR_MAX_USED 32

/*
 *  Global determining whether to use array rather than B-Tree.
 */
bool g_use_arr = true;

static void
cloneDigestFromai_obj(cf_digest *d, ai_obj *akey)
{
	memcpy(d, &akey->y, CF_DIGEST_KEY_SZ);
}

static void
init_ai_objFromDigest(ai_obj *akey, cf_digest *d)
{
	init_ai_objU160(akey, *(uint160 *)d);
}

const uint8_t INIT_CAPACITY = 1;

static ai_arr *
ai_arr_new()
{
	ai_arr *arr = cf_malloc(sizeof(ai_arr) + (INIT_CAPACITY * CF_DIGEST_KEY_SZ));
	arr->capacity = INIT_CAPACITY;
	arr->used = 0;
	return arr;
}

static void
ai_arr_move_to_tree(ai_arr *arr, bt *nbtr)
{
	for (int i = 0; i < arr->used; i++) {
		ai_obj apk;
		init_ai_objFromDigest(&apk, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ]);
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
	return(sizeof(ai_arr) + (arr->capacity * CF_DIGEST_KEY_SZ));
}

/*
 * Finds the digest in the AI array.
 * Returns
 *      idx if found
 *      -1  if not found
 */
static int
ai_arr_find(ai_arr *arr, cf_digest *dig)
{
	for (int i = 0; i < arr->used; i++) {
		if (0 == cf_digest_compare(dig, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ])) {
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
	if ((arr->capacity <= 4) ||
			(size < arr->used * 2)) {
		return arr;
	}

	ai_arr * temp_arr = cf_realloc(arr, sizeof(ai_arr) + (size * CF_DIGEST_KEY_SZ));
	temp_arr->capacity = size;
	return temp_arr;
}

static ai_arr *
ai_arr_delete(ai_arr *arr, cf_digest *dig, bool *notfound)
{
	int idx = ai_arr_find(arr, dig);
	// Nothing to delete
	if (idx < 0) {
		*notfound = true;
		return arr;
	}
	if (idx != arr->used - 1) {
		int dest_offset = idx * CF_DIGEST_KEY_SZ;
		int src_offset = (arr->used - 1) * CF_DIGEST_KEY_SZ;
		// move last element
		memcpy(&arr->data[dest_offset], &arr->data[src_offset], CF_DIGEST_KEY_SZ);
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
		cf_crash(AS_SINDEX, "Refusing to expand ai_arr to %d (beyond limit of %d)", size, AI_ARR_MAX_SIZE);
	}

	arr = cf_realloc(arr, sizeof(ai_arr) + (size * CF_DIGEST_KEY_SZ));
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
ai_arr_insert(ai_arr *arr, cf_digest *dig, bool *found)
{
	int idx = ai_arr_find(arr, dig);
	// already found
	if (idx >= 0) {
		*found = true;
		return arr;
	}
	if (arr->used == arr->capacity) {
		arr = ai_arr_expand(arr);
	}
	memcpy(&arr->data[arr->used * CF_DIGEST_KEY_SZ], dig, CF_DIGEST_KEY_SZ);
	arr->used++;
	return arr;
}

/*
 * Returns the size diff
 */
static int
anbtr_check_convert(ai_nbtr *anbtr, col_type_t sktype)
{
	// Nothing to do
	if (anbtr->is_btree)
		return 0;

	ai_arr *arr = anbtr->u.arr;
	if (arr && (arr->used >= AI_ARR_MAX_USED)) {
		//cf_info(AS_SINDEX,"Flipped @ %d", arr->used);
		ulong ba = ai_arr_size(arr);
		// Allocate btree move digest from arr to btree
		bt *nbtr = createNBT(sktype);
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
 *  return -1    in case of failure
 *          size of allocation in case of success
 */
static int
anbtr_check_init(ai_nbtr *anbtr, col_type_t sktype)
{
	bool create_arr = false;
	bool create_nbtr = false;

	if (anbtr->is_btree) {
		if (anbtr->u.nbtr) {
			create_nbtr = false;
		} else {
			create_nbtr = true;
		}
	} else {
		if (anbtr->u.arr) {
			create_arr = false;
		} else {
			if (g_use_arr) {
				create_arr = true;
			} else {
				create_nbtr = true;
			}
		}
	}

	// create array or btree
	if (create_arr) {
		anbtr->u.arr = ai_arr_new();
		return ai_arr_size(anbtr->u.arr);
	} else if (create_nbtr) {
		anbtr->u.nbtr = createNBT(sktype);
		if (!anbtr->u.nbtr) {
			return -1;
		}
		anbtr->is_btree = true;
		return anbtr->u.nbtr->msize;
	} else {
		if (!anbtr->u.arr && !anbtr->u.nbtr) {
			cf_warning(AS_SINDEX, "Something wrong!!!");
			return -1;
		}
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
 *              sktype : value type (U160 currently)
 *
 * Returns:
 *      AS_SINDEX_OK        : In case of success
 *      AS_SINDEX_ERR       : In case of failure
 *      AS_SINDEX_KEY_FOUND : If key already exists
 */
static int
reduced_iAdd(bt *ibtr, ai_obj *acol, ai_obj *apk, col_type_t sktype)
{
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(ibtr, acol);
	ulong ba = 0, aa = 0;
	bool allocated_anbtr = false;
	if (!anbtr) {
		anbtr = cf_malloc(sizeof(ai_nbtr));
		aa += sizeof(ai_nbtr);
		memset(anbtr, 0, sizeof(ai_nbtr));
		allocated_anbtr = true;
	}

	// Init the array
	int ret = anbtr_check_init(anbtr, sktype);
	if (ret < 0) {
		if (allocated_anbtr) {
			cf_free(anbtr);
		}
		return AS_SINDEX_ERR;
	} else if (ret) {
		ibtr->nsize += ret;
		btIndAdd(ibtr, acol, (bt *)anbtr);
	}

	// Convert from arr to nbtr if limit is hit
	ibtr->nsize += anbtr_check_convert(anbtr, sktype);

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
		ai_arr *t_arr = ai_arr_insert(arr, (cf_digest *)&apk->y, &found);
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
		anbtr->u.arr = ai_arr_delete(anbtr->u.arr, (cf_digest *)&apk->y, &notfound);
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

int
ai_btree_key_hash_from_sbin(as_sindex_metadata *imd, as_sindex_bin_data *b)
{
	uint64_t u;

	if (C_IS_DG(imd->sktype)) {
		char *x = (char *) &b->digest; // x += 4;
		u = ((* (uint128 *) x) % imd->nprts);
	} else {
		u = (((uint64_t) b->u.i64) % imd->nprts);
	}

	return (int) u;
}

int
ai_btree_key_hash(as_sindex_metadata *imd, void *skey)
{
	uint64_t u;

	if (C_IS_DG(imd->sktype)) {
		char *x = (char *) ((cf_digest *)skey); // x += 4;
		u = ((* (uint128 *) x) % imd->nprts);
	} else {
		u = ((*(uint64_t*)skey) % imd->nprts);
	}

	return (int) u;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
btree_addsinglerec(as_sindex_metadata *imd, ai_obj * key, cf_digest *dig, cf_ll *recl, uint64_t *n_bdigs, 
								bool * can_partition_query, bool partitions_pre_reserved)
{
	// The digests which belongs to one of the query-able partitions are elligible to go into recl
	uint32_t pid =  as_partition_getid(dig);
	as_namespace * ns = imd->si->ns;
	if (partitions_pre_reserved) {
		if (!can_partition_query[pid]) {
			return 0;
		}
	}
	else {
		if (! client_replica_maps_is_partition_queryable(ns, pid)) {
			return 0;
		}
	}

	bool create                     = (cf_ll_size(recl) == 0) ? true : false;
	as_index_keys_arr * keys_arr    = NULL;
	if (!create) {
		cf_ll_element * ele         = cf_ll_get_tail(recl);
		keys_arr                    = ((as_index_keys_ll_element*)ele)->keys_arr;
		if (keys_arr->num == AS_INDEX_KEYS_PER_ARR) {
			create = true;
		}
	}
	if (create) {
		keys_arr                    = as_index_get_keys_arr();
		if (!keys_arr) {
			cf_warning(AS_SINDEX, "Fail to allocate sindex key value array");
			return -1;
		}
		as_index_keys_ll_element * node =  cf_malloc(sizeof(as_index_keys_ll_element));
		node->keys_arr                  = keys_arr;
		cf_ll_append(recl, (cf_ll_element *)node);
	}
	// Copy the digest (value)
	memcpy(&keys_arr->pindex_digs[keys_arr->num], dig, CF_DIGEST_KEY_SZ);

	// Copy the key
	if (C_IS_DG(imd->sktype)) {
		memcpy(&keys_arr->sindex_keys[keys_arr->num].key.str_key, &key->y, CF_DIGEST_KEY_SZ);
	}
	else {
		keys_arr->sindex_keys[keys_arr->num].key.int_key = key->l;
	}

	keys_arr->num++;
	*n_bdigs = *n_bdigs + 1;
	return 0;
}

/*
 * Return 0 in case of success
 *       -1 in case of failure
 */
static int
add_recs_from_nbtr(as_sindex_metadata *imd, ai_obj *ikey, bt *nbtr, as_sindex_qctx *qctx, bool fullrng)
{
	int ret = 0;
	ai_obj sfk, efk;
	init_ai_obj(&sfk);
	init_ai_obj(&efk);
	btSIter *nbi;
	btEntry *nbe;
	btSIter stack_nbi;

	if (fullrng) {
		nbi = btSetFullRangeIter(&stack_nbi, nbtr, 1, NULL);
	} else { // search from LAST batches end-point
		init_ai_objFromDigest(&sfk, &qctx->bdig);
		assignMaxKey(nbtr, &efk);
		nbi = btSetRangeIter(&stack_nbi, nbtr, &sfk, &efk, 1);
	}
 	if (nbi) {
		while ((nbe = btRangeNext(nbi, 1))) {
			ai_obj *akey = nbe->key;
			// FIRST can be REPEAT (last batch)
			if (!fullrng && ai_objEQ(&sfk, akey)) {
				continue;
			}
			if (btree_addsinglerec(imd, ikey, (cf_digest *)&akey->y, qctx->recl, &qctx->n_bdigs,
									qctx->can_partition_query, qctx->partitions_pre_reserved)) {
				ret = -1;
				break;
			}
			if (qctx->n_bdigs == qctx->bsize) {
				if (ikey) {
					ai_objClone(qctx->bkey, ikey);
				}
				cloneDigestFromai_obj(&qctx->bdig, akey);
				break;
			}
		}
		btReleaseRangeIterator(nbi);
	} else {
		cf_warning(AS_QUERY, "Could not find nbtr iterator.. skipping !!");
	}
	return ret;
}

static int
add_recs_from_arr(as_sindex_metadata *imd, ai_obj *ikey, ai_arr *arr, as_sindex_qctx *qctx)
{
	bool ret = 0;

	for (int i = 0; i < arr->used; i++) {
		if (btree_addsinglerec(imd, ikey, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ], qctx->recl, 
					&qctx->n_bdigs, qctx->can_partition_query, qctx->partitions_pre_reserved)) {
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
		ai_objClone(qctx->bkey, ikey);
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
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_idx];
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
	init_ai_objLong(&sfk, qctx->new_ibtr ? begk : qctx->bkey->l);
	ai_obj efk;
	init_ai_objLong(&efk, endk);
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_idx];
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
			if (qctx->n_bdigs >= qctx->bsize) {
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
ai_btree_query(as_sindex_metadata *imd, as_sindex_range *srange, as_sindex_qctx *qctx)
{
	bool err = 1;
	if (!srange->isrange) { // EQUALITY LOOKUP
		ai_obj afk;
		init_ai_obj(&afk);
		if (C_IS_DG(imd->sktype)) {
			init_ai_objFromDigest(&afk, &srange->start.digest);
		}
		else {
			init_ai_objLong(&afk, srange->start.u.i64);
		}
		err = get_recl(imd, &afk, qctx);
	} else {                // RANGE LOOKUP
		err = get_numeric_range_recl(imd, srange->start.u.i64, srange->end.u.i64, qctx);
	}
	return (err ? AS_SINDEX_ERR_NO_MEMORY :
			(qctx->n_bdigs >= qctx->bsize) ? AS_SINDEX_CONTINUE : AS_SINDEX_OK);
}

int
ai_btree_put(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *skey, cf_digest *value)
{
	ai_obj ncol;
	if (C_IS_DG(imd->sktype)) {
		init_ai_objFromDigest(&ncol, (cf_digest*)skey);
	}
	else {
		// TODO - ai_obj type is LONG for both Geo and Long
		init_ai_objLong(&ncol, *(ulong *)skey);
	}

	ai_obj apk;
	init_ai_objFromDigest(&apk, value);


	uint64_t before = pimd->ibtr->msize + pimd->ibtr->nsize;
	int ret = reduced_iAdd(pimd->ibtr, &ncol, &apk, COL_TYPE_DIGEST);
	uint64_t after = pimd->ibtr->msize + pimd->ibtr->nsize;
	cf_atomic64_add(&imd->si->ns->n_bytes_sindex_memory, (after - before));

	if (ret && ret != AS_SINDEX_KEY_FOUND) {
		cf_warning(AS_SINDEX, "Insert into the btree failed");
		return AS_SINDEX_ERR_NO_MEMORY;
	}
	return ret;
}

int
ai_btree_delete(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void * skey, cf_digest * value)
{
	int ret = AS_SINDEX_OK;

	if (!pimd->ibtr) {
		return AS_SINDEX_KEY_NOTFOUND;
	}

	ai_obj ncol;
	if (C_IS_DG(imd->sktype)) {
		init_ai_objFromDigest(&ncol, (cf_digest *)skey);
	}
	else {
		// TODO - ai_obj type is LONG for both Geo and Long
		init_ai_objLong(&ncol, *(ulong *)skey);
	}

	ai_obj apk;
	init_ai_objFromDigest(&apk, value);

	uint64_t before = pimd->ibtr->msize + pimd->ibtr->nsize;
	ret = reduced_iRem(pimd->ibtr, &ncol, &apk);
	uint64_t after = pimd->ibtr->msize + pimd->ibtr->nsize;
	cf_atomic64_sub(&imd->si->ns->n_bytes_sindex_memory, (before - after));

	return ret;
}

/*
 * Internal function which adds digests to the defrag_list
 * Mallocs the nodes of defrag_list
 * Returns :
 *      -1 : Error
 *      number of digests found : success
 *
 */
static long
build_defrag_list_from_nbtr(as_namespace *ns, ai_obj *acol, bt *nbtr, ulong nofst, ulong *limit, uint64_t * tot_found, cf_ll *gc_list)
{
	int error = -1;
	btEntry *nbe;
	// STEP 1: go thru a portion of the nbtr and find to-be-deleted-PKs
	// TODO: a range query may be smarter then using the Xth Iterator
	btSIter *nbi = (nofst ? btGetFullXthIter(nbtr, nofst, 1, NULL, 0) :
					btGetFullRangeIter(nbtr, 1, NULL));
	if (!nbi) {
		return error;
	}

	long      found             = 0;
	long  processed             = 0;
	while ((nbe = btRangeNext(nbi, 1))) {
		ai_obj *akey = nbe->key;
		int ret = as_sindex_can_defrag_record(ns, (cf_digest *) (&akey->y));

		if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
			*limit = 0;
			break;
		} else if (ret == AS_SINDEX_GC_OK) {

			bool create   = (cf_ll_size(gc_list) == 0) ? true : false;
			objs_to_defrag_arr *dt;

			if (!create) {
				cf_ll_element * ele = cf_ll_get_tail(gc_list);
				dt = ((ll_sindex_gc_element*)ele)->objs_to_defrag;
				if (dt->num == SINDEX_GC_NUM_OBJS_PER_ARR) {
					create = true;
				}
			}
			if (create) {
				dt = as_sindex_gc_get_defrag_arr();
				if (!dt) {
					*tot_found += found;
					return -1;
				}
				ll_sindex_gc_element  * node;
				node = cf_malloc(sizeof(ll_sindex_gc_element));
				node->objs_to_defrag = dt;
				cf_ll_append(gc_list, (cf_ll_element *)node);
			}
			cloneDigestFromai_obj(&(dt->acol_digs[dt->num].dig), akey);
			ai_objClone(&(dt->acol_digs[dt->num].acol), acol);

			dt->num += 1;		
			found++;
		}
		processed++;
		(*limit)--;
		if (*limit == 0) break;
	}
	btReleaseRangeIterator(nbi);
	*tot_found += found; 
	return processed;
}

static long
build_defrag_list_from_arr(as_namespace *ns, ai_obj *acol, ai_arr *arr, ulong nofst, ulong *limit, uint64_t * tot_found, cf_ll *gc_list)
{
	long     found              = 0;
	long     processed          = 0;

	for (ulong i = nofst; i < arr->used; i++) {
		int ret = as_sindex_can_defrag_record(ns, (cf_digest *) &arr->data[i * CF_DIGEST_KEY_SZ]);
		if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
			*limit = 0;
			break;
		} else if (ret == AS_SINDEX_GC_OK) {
			bool create   = (cf_ll_size(gc_list) == 0) ? true : false;
			objs_to_defrag_arr *dt;

			if (!create) {
				cf_ll_element * ele = cf_ll_get_tail(gc_list);
				dt = ((ll_sindex_gc_element*)ele)->objs_to_defrag;
				if (dt->num == SINDEX_GC_NUM_OBJS_PER_ARR) {
					create = true;
				}
			}
			if (create) {
				dt = as_sindex_gc_get_defrag_arr();
				if (!dt) {
					*tot_found += found;
					return -1;
				}
				ll_sindex_gc_element  * node;
				node = cf_malloc(sizeof(ll_sindex_gc_element));
				node->objs_to_defrag = dt;
				cf_ll_append(gc_list, (cf_ll_element *)node);
			}
			memcpy(&(dt->acol_digs[dt->num].dig), (cf_digest *) &arr->data[i * CF_DIGEST_KEY_SZ], CF_DIGEST_KEY_SZ);	
			ai_objClone(&(dt->acol_digs[dt->num].acol), acol);

			dt->num += 1;		
			found++;
		}
		processed++;
		(*limit)--;
		if (*limit == 0) {
			break;
		}
	}
	*tot_found += found; 
	return processed;
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
int
ai_btree_build_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, ai_obj *icol,
						   ulong *nofst, ulong limit, uint64_t * tot_processed, uint64_t * tot_found, cf_ll *gc_list)
{
	int ret = AS_SINDEX_ERR;

	if (!pimd || !imd) {
		return ret;
	}

	as_namespace *ns = imd->si->ns;
	if (!ns) {
		ns = as_namespace_get_byname((char *)imd->ns_name);
	}

	if (!pimd || !pimd->ibtr || !pimd->ibtr->numkeys) {
		goto END;
	}
	//Entry is range query, FROM previous icol TO maxKey(ibtr)
	if (icol->type == COL_TYPE_INVALID) {
		assignMinKey(pimd->ibtr, icol); // init first call
	}
	ai_obj iH;
	assignMaxKey(pimd->ibtr, &iH);
	btEntry *be = NULL;
	btSIter *bi = btGetRangeIter(pimd->ibtr, icol, &iH, 1);
	if (!bi) {
		goto END;
	}

	while ( true ) {
		be = btRangeNext(bi, 1);
		if (!be) {
			ret = AS_SINDEX_DONE;
			break;
		}
		ai_obj *acol = be->key;
		ai_nbtr *anbtr = be->val;
		long processed = 0;
		if (!anbtr) {
			break;
		}
		if (anbtr->is_btree) {
			processed = build_defrag_list_from_nbtr(ns, acol, anbtr->u.nbtr, *nofst, &limit, tot_found, gc_list);
		} else {
			processed = build_defrag_list_from_arr(ns, acol, anbtr->u.arr, *nofst, &limit, tot_found, gc_list);
		}

		if (processed < 0) {    // error .. abort everything.
			cf_detail(AS_SINDEX, "build_defrag_list returns an error. Aborting defrag on current pimd");
			ret = AS_SINDEX_ERR;
			break;
		}
		*tot_processed += processed;
		// This tree may have some more digest to defrag
		if (limit == 0) {
			*nofst = *nofst + processed;
			ai_objClone(icol, acol);
			cf_detail(AS_SINDEX, "Current pimd may need more iteration of defragging.");
			ret = AS_SINDEX_CONTINUE;
			break;
		}

		// We have finished this tree. Yet we have not reached our limit to defrag.
		// Goes to next iteration
		*nofst = 0;
		ai_objClone(icol, acol);
	};
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
	as_namespace *ns = imd->si->ns;
	// STEP 3: go thru the PKtoDeleteList and delete the keys

	uint64_t before = 0;
	uint64_t after = 0;

	while (cf_ll_size(gc_list)) {
		cf_ll_element        * ele  = cf_ll_get_head(gc_list);
		ll_sindex_gc_element * node = (ll_sindex_gc_element * )ele;
		objs_to_defrag_arr   * dt   = node->objs_to_defrag;

		// check before deleting. The digest may re-appear after the list
		// creation and before deletion from the secondary index

		int i = 0;
		while (dt->num != 0) {
			i = dt->num - 1;
			int ret = as_sindex_can_defrag_record(ns, &(dt->acol_digs[i].dig));
			if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
				goto END;
			} else if (ret == AS_SINDEX_GC_OK) {
				ai_obj apk;
				init_ai_objFromDigest(&apk, &(dt->acol_digs[i].dig));
				ai_obj *acol = &(dt->acol_digs[i].acol);
				cf_detail(AS_SINDEX, "Defragged %lu %ld", acol->l, *((uint64_t *)&apk.y));
				
				before += pimd->ibtr->msize + pimd->ibtr->nsize;
				if (reduced_iRem(pimd->ibtr, acol, &apk) == AS_SINDEX_OK) {
					success++;
				}
				after += pimd->ibtr->msize + pimd->ibtr->nsize;
			}
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

void
ai_btree_create(as_sindex_metadata *imd)
{
	for (int i = 0; i < imd->nprts; i++) {
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

void
ai_btree_dump(as_sindex_metadata *imd, char *fname, bool verbose)
{
	FILE *fp = NULL;                                                            
	if (!(fp = fopen(fname, "w"))) {                                         
		return;                                                              
	}           

	fprintf(fp, "Namespace: %s set: %s\n", imd->ns_name, imd->set ? imd->set : "None");

	for (int i = 0; i < imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		fprintf(fp, "INDEX: name: %s:%d (%p)\n", imd->iname, i, (void *) pimd->ibtr);
		if (pimd->ibtr) {
			bt_dumptree(fp, pimd->ibtr, 1, verbose);
		}
	}

	fclose(fp);
}

uint64_t
ai_btree_get_numkeys(as_sindex_metadata *imd)
{
	uint64_t val = 0;

	for (int i = 0; i < imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		val += pimd->ibtr->numkeys;
		PIMD_RUNLOCK(&pimd->slock);
	}

	return val;
}

uint64_t
ai_btree_get_pimd_isize(as_sindex_pmetadata *pimd)
{
	// TODO - Why check of > 0
	return pimd->ibtr->msize > 0 ? pimd->ibtr->msize : 0;
}

uint64_t
ai_btree_get_isize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	for (int i = 0; i < imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		size += ai_btree_get_pimd_isize(pimd);
		PIMD_RUNLOCK(&pimd->slock);
	}
	return size;
}

uint64_t
ai_btree_get_pimd_nsize(as_sindex_pmetadata *pimd)
{
	// TODO - Why check of > 0
	return pimd->ibtr->nsize > 0 ? pimd->ibtr->nsize : 0;
}

uint64_t
ai_btree_get_nsize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	for (int i = 0; i < imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &imd->pimd[i];
		PIMD_RLOCK(&pimd->slock);
		size += ai_btree_get_pimd_nsize(pimd);
		PIMD_RUNLOCK(&pimd->slock)
	}

	return size;
}

void
ai_btree_reinit_pimd(as_sindex_pmetadata * pimd, col_type_t sktype)
{
	if (! pimd->ibtr) {
		cf_crash(AS_SINDEX, "IBTR is null");
	}
	pimd->ibtr = createIBT(sktype, -1);
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
