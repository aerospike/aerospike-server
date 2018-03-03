/*
 * bt.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * Creation of different btree types and
 * Public B-tree operations w/ stream abstractions under the covers.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "bt.h"
#include "bt_iterator.h"
#include "stream.h"

#include <citrusleaf/alloc.h>

bt *createIBT(col_type_t ktype, int imatch) {
	bt_cmp_t cmp;
	bts_t bts;
	bts.ktype = ktype;
	bts.btype = INDEX_BTREE;
	bts.num = imatch;
	if (C_IS_L(ktype)) { /* NOTE: under the covers: LL */
		bts.ksize = LL_SIZE;
		cmp = llCmp;
		bts.bflag = BTFLAG_ULONG_ULONG;
	} else if (C_IS_G(ktype)) { /* NOTE: under the covers: LL */
		bts.ksize = LL_SIZE;
		cmp = llCmp;
		bts.bflag = BTFLAG_ULONG_ULONG;
	} else if (C_IS_DG(ktype)) { /* NOTE: under the covers: YL */
		bts.ksize = YL_SIZE;
		cmp = ylCmp;
		bts.bflag = BTFLAG_U160_ULONG;
	} else {                  /* STRING or FLOAT */
		assert(!"Unsupport Key Type");
	}

	return bt_create(cmp, &bts, 0);
}

bt *createNBT(col_type_t ktype) {
	bt_cmp_t cmp;
	bts_t bts;
	bts.ktype = ktype;
	bts.btype = NODE_BTREE;
	bts.num   = -1;
	if (C_IS_DG(ktype)) {
		cmp = u160Cmp;
		bts.ksize = U160SIZE;
		bts.bflag = BTFLAG_U160;
	} else {
		assert(!"Unsupport Key Type");
	}

	return bt_create(cmp, &bts, 0);
}

static void *abt_find(bt *btr, ai_obj *akey) {
	DECLARE_BT_KEY(akey, 0)
	uchar *stream = bt_find(btr, btkey, akey);
	destroyBTKey(btkey, med);                            /* FREED 026 */
	return parseStream(stream, btr);
}
static bool abt_exist(bt *btr, ai_obj *akey) { //NOTE: Evicted Indexes are NULL
	DECLARE_BT_KEY(akey, 0)
	bool ret = bt_exist(btr, btkey, akey);
	destroyBTKey(btkey, med);                            /* FREED 026 */
	return ret;
}
static bool abt_del(bt *btr, ai_obj *akey, bool leafd) { // DELETE the row
	DECLARE_BT_KEY(akey, 0)
	dwd_t  dwd    = bt_delete(btr, btkey, leafd);        /* FREED 028 */
	if (!dwd.k) return 0;
	uchar *stream = dwd.k;
	destroyBTKey(btkey, med);                            /* FREED 026 */
	return destroyStream(btr, stream);                   /* DESTROYED 027 */
}
static uint32 abt_insert(bt *btr, ai_obj *akey, void *val) {
	crs_t crs;
	uint32 ssize;
	DECLARE_BT_KEY(akey, 0)
	char *stream = createStream(btr, val, btkey, ksize, &ssize, &crs); // D 027
	if (!stream) return 0;
	destroyBTKey(btkey, med);                            /* FREED 026 */
	if (!bt_insert(btr, stream, 0)) return 0;            /* FREE ME 028 */
	return 1;
}

/* INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX */
void  btIndAdd   (bt *ibtr, ai_obj *ikey, bt *nbtr) {
	abt_insert (ibtr, ikey, nbtr);
}
bt   *btIndFind  (bt *ibtr, ai_obj *ikey) {
	return abt_find   (ibtr, ikey);
}
int   btIndDelete(bt *ibtr, ai_obj *ikey) {
	abt_del    (ibtr, ikey, 0);
	return ibtr->numkeys;
}

bool  btIndNodeExist(bt *nbtr, ai_obj *apk) {
	return abt_exist(nbtr, apk);
}
bool  btIndNodeAdd(bt *nbtr, ai_obj *apk) { //DEBUG_NBT_ADD
	return abt_insert(nbtr, apk, NULL);
}
int  btIndNodeDelete(bt *nbtr, ai_obj *apk, ai_obj *ocol) {
	abt_del  (nbtr, ocol ? ocol : apk, 0);
	return nbtr->numkeys;
}
