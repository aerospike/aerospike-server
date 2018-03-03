/*
 * bt.h
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
 * Public Btree Operations w/ stream abstractions under the covers
 */

#pragma once

#include "ai_obj.h"
#include "btreepriv.h"

bt *createIBT      (col_type_t ktype, int imatch);
bt *createNBT      (col_type_t ktype);

/* different Btree types */
#define INDEX_BTREE   0
#define NODE_BTREE    1

// SPAN OUTS
// This values are choosen to fit the node size into multiples
// of cacheline (64 byte)
#define BTREE_LONG_TYPE_DEGREE    31 // node size becomes 504
#define BTREE_STRING_TYPE_DEGREE  18 // node size becomes 512

#define NBT_DG(btr) \
  (btr->s.btype == NODE_BTREE && C_IS_DG(btr->s.ktype))

#define NBT(btr) (NBT_DG(btr))

typedef struct ulong_ulong_key {
	ulong key;
	ulong val;
}  __attribute__ ((packed)) llk;
#define LL(btr) (btr->s.bflag & BTFLAG_ULONG_ULONG)
#define LL_SIZE 16
typedef struct u160_ulong_key {
	uint160 key;
	ulong   val;
}  __attribute__ ((packed)) ylk;
#define YL(btr) (btr->s.bflag & BTFLAG_U160_ULONG)
#define YL_SIZE 28

typedef struct btk_t {
	llk LL;
	ylk YL;
} btk_t;

#define DECLARE_BT_KEY(akey, ret)                                            \
    bool  med; uint32 ksize; btk_t btk;                                      \
    char *btkey = createBTKey(akey, &med, &ksize, btr, &btk);/*FREE ME 026*/ \
    if (!btkey) return ret;

typedef struct crs_t {
	llk LL_StreamPtr;
	ylk YL_StreamPtr;
} crs_t;

#define OTHER_BT(btr) (btr->s.bflag >= BTFLAG_ULONG_ULONG)
#define NONE_BT(btr)  (btr->s.bflag == BTFLAG_U160)
#define BIG_BT(btr)   (btr->s.ksize > 8)

#define IS_GHOST(btr, rrow) (NONE_BT(btr) && rrow && !(*(uchar *)rrow))

void  btIndAdd   (bt *ibtr, ai_obj *ikey, bt  *nbtr);
bt   *btIndFind  (bt *ibtr, ai_obj *ikey);
int   btIndDelete(bt *ibtr, ai_obj *ikey);

bool  btIndNodeAdd    (bt *nbtr, ai_obj *apk);
bool  btIndNodeExist  (bt *nbtr, ai_obj *apk);
int   btIndNodeDelete (bt *nbtr, ai_obj *apk, ai_obj *ocol);
