/*
 * stream.c
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
 * This file implements stream parsing for rows.
 */

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "ai_obj.h"
#include "bt.h"
#include "stream.h"

#include "log.h"

#include <citrusleaf/alloc.h>

/* COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE */
int u160Cmp(void *s1, void *s2) {
	char *p1 = (char *)s1;
	char *p2 = (char *)s2;
	uint128 x1, x2;
	memcpy(&x1, p1 + 4, 16);
	memcpy(&x2, p2 + 4, 16);
	if (x1 == x2) {
		uint32 u1;
		memcpy(&u1, p1, 4);
		uint32 u2;
		memcpy(&u2, p2, 4);
		return u1 == u2 ? 0 : (u1 > u2) ? 1 : -1;
	} else return             (x1 > x2) ? 1 : -1;
}

int llCmp(void *s1, void *s2) {
	llk   *ll1 = (llk *)s1;
	llk   *ll2 = (llk *)s2;
	long   l1  = ll1->key;
	long   l2  = ll2->key;
	return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}

int ylCmp(void *s1, void *s2) {
	ylk     *yl1 = (ylk *)s1;
	ylk     *yl2 = (ylk *)s2;
	uint160  y1  = yl1->key;
	uint160  y2  = yl2->key;
	return u160Cmp(&y1, &y2);
}

int lCmp(void *s1, void *s2) {
	lk *l1  = (lk *)s1;
	lk *l2  = (lk *)s2;
	long k1 = l1->key;
	long k2 = l2->key;
	return k1 == k2 ? 0 : (k1 > k2) ? 1 : -1;
}

void destroyBTKey(char *btkey, bool med) {
	if (med) cf_free(btkey);
}

char *createBTKey(ai_obj *akey, bool *med, bt *btr, btk_t *btk) {
	*med   = 0;

	if (NBT(btr)) {
		return (char *)&akey->l;
	} else if (LL(btr)) {
		btk->LL.key = akey->l;
		return (char *)&btk->LL;
	} else if (YL(btr)) {
		btk->YL.key = akey->y;
		return (char *)&btk->YL;
	}
	
	cf_crash(AS_SINDEX, "Unsupported Btree type");
	return NULL;
}

uchar *parseStream(uchar *stream, bt *btr) {
	if (!stream || NBT(btr)) {
		return NULL;
	} else if (LL(btr)) {
		return (uchar *)(*(llk *)(stream)).val;
	} else if (YL(btr)) {
		return (uchar *)(long)(*(ylk *)(stream)).val;
	}

	cf_crash(AS_SINDEX, "Unsupported Btree type");
	return NULL;
}

void convertStream2Key(uchar *stream, ai_obj *key, bt *btr) {
	init_ai_obj(key);
	if (NBT(btr)) {
		key->type = COL_TYPE_LONG;
		key->l = ((lk *)stream)->key;
	} else if (LL(btr)) {
		key->type = COL_TYPE_LONG;
		key->l = ((llk *)stream)->key;
	} else if (YL(btr)) {
		key->type = COL_TYPE_DIGEST;
		key->y = ((ylk *)stream)->key;
	} else {
		cf_crash(AS_SINDEX, "Unsupported Btree type");
	}
}

void *createStream(bt *btr, void *val, char *btkey, uint32 *size, crs_t *crs) {
	*size = 0;
	if (NBT(btr)) {
		return btkey;
	} else if (LL(btr)) {
		llk *ll               = (llk *)btkey;
		crs->LL_StreamPtr.key = ll->key;
		crs->LL_StreamPtr.val = (ulong) val;
		return &crs->LL_StreamPtr;
	} else if (YL(btr)) {
		ylk *yl               = (ylk *)btkey;
		crs->YL_StreamPtr.key = yl->key;
		crs->YL_StreamPtr.val = (ulong) val;
		return &crs->YL_StreamPtr;
	}

	cf_crash(AS_SINDEX, "Unsupported Btree type");
	return NULL;
}
