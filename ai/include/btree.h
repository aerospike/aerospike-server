/*-
 * Copyright 1997, 1998, 2001 John-Mark Gurney.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#pragma once

#include "ai_types.h"

struct btree;
struct btreenode;

#define VOIDSIZE   8 /* force to 8, otherwise UU would not work on 32bit */
#define U160SIZE  AS_DIGEST_KEY_SZ

typedef struct btree_specification { /* size 9B */
	unsigned char   ktype;    /* [STRING,INT,FLOAT,LONG]--------------------| */
	unsigned char   btype;    /* [data,index,node]                          | */
	unsigned char   ksize;    /* UU&INDEX(8), UL&LU(12), LL(16) | */
	unsigned int    bflag;    /* [OTHER_BT + BTFLAG_*_INDEX]                | */
	unsigned short  num;      /*--------------------------------------------| */
} __attribute__ ((packed)) bts_t;

typedef void * bt_data_t;
typedef int (*bt_cmp_t)(bt_data_t k1, bt_data_t k2);

// CONSTRUCTOR CONSTRUCTOR CONSTRUCTOR CONSTRUCTOR CONSTRUCTOR CONSTRUCTOR
struct btree *bt_create(bt_cmp_t cmp, bts_t *s, char dirty);

// CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD CRUD
typedef struct data_with_dirt_t {
	bt_data_t k;     // the data
	uint32    dr;    // dirty-right
} dwd_t;
bool      bt_insert  (struct btree *btr, bt_data_t k, uint32     dr);
dwd_t     bt_delete  (struct btree *btr, bt_data_t k, bool leafd);

// OPERATORS OPERATORS OPERATORS OPERATORS OPERATORS OPERATORS OPERATORS
bt_data_t  bt_max     (struct btree *btr);
bt_data_t  bt_min     (struct btree *btr);
bt_data_t  bt_find    (struct btree *btr, bt_data_t k, ai_obj *akey);

// DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY
struct btreenode *addDStoBTN(struct btree *btr, struct btreenode *x,
							 struct btreenode *p, int pi, char dirty);

uint32    getDR          (struct btree *btr, struct btreenode *x, int i);
bool      bt_exist       (struct btree *btr, bt_data_t k, ai_obj *akey);

typedef struct data_with_miss_t {
	bt_data_t         k;    // the data
	bool              miss;
	struct btreenode *x;    // NOTE: used for DELETE an EVICTed row
	int               i;    // NOTE: used for DELETE an EVICTed row
	struct btreenode *p;    // NOTE: used for DELETE an EVICTed row
	int               pi;   // NOTE: used for DELETE an EVICTed row
} dwm_t;

struct ai_obj;
dwm_t findnodekey(struct btree *btr, struct btreenode *x, bt_data_t k, ai_obj *akey);

// ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR
struct btIterator;
int  bt_init_iterator(struct btree *br, bt_data_t k, struct btIterator *iter, ai_obj *alow);
void bt_destroy   (struct btree *btr);
