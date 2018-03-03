/*-
 * Copyright 1997-1999, 2001 John-Mark Gurney.
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
 */

#pragma once

#include "btree.h"

// BTREE TYPE FLAGS
#define BTFLAG_U160          0x00
#define BTFLAG_ULONG_ULONG   0x01
#define BTFLAG_U160_ULONG    0x02

struct btree { // 62 Bytes -> 64B
	struct btreenode  *root;
	bt_cmp_t           cmp;

	unsigned long      msize;
	unsigned long      nsize;   // sizeof underlying nbtr
	unsigned long      dsize;

	unsigned int       numkeys;  /* --- 8 bytes | */
	unsigned int       numnodes; /* ------------| */

	unsigned short     keyofst;  /* --- 8 bytes | */ //TODO can be computed
	unsigned short     nodeofst; /*             | */ //TODO can be computed
	unsigned short     nbyte;    /*             | */
	unsigned short     kbyte;    /* ------------| */

	unsigned char      t;
	unsigned char      nbits;
	bts_t              s;          // 9 bytes

	unsigned int       dirty_left; // 4 bytes (num evicted before 1st key)
	unsigned char      dirty;      // NOTE: bool: if ANY btn in btr is dirty
} __attribute__ ((packed));

// Aerospike Index local list ... this is to optimize for space for the high selectivity index.
typedef struct {
	uint8_t    capacity;
	uint8_t    used;
	uint8_t    data[];
} __attribute__ ((__packed__)) ai_arr;

/*
 *  Note:  The "ai_arr" structure is limited to 8 bits for capacity / used.
 */
#define AI_ARR_MAX_SIZE 255

// Do not change order it is same as struct B-tree inside Aerospike Index ~~
//  pretty hacky stuff.  Inside Aerospike Index code is_btree is checked
typedef struct {
	union {
		ai_arr *arr;
		bt      *nbtr;
	} u;
	bool     is_btree;
} __attribute__ ((__packed__)) ai_nbtr;

//NOTE: For Aerospike, not currently using EVICT, save one byte in bt_n
//      This changes a 2049 allocation to 2048 -> which is IMPORTANT
typedef struct btreenode { // 9 bytes -> 16 bytes
	unsigned int   scion;       /* 4 billion max scion */
	unsigned short n;           /* 65 thousand max entries (per bt_n)*/
	unsigned char  leaf;
	// DIRTY: -1->CLEAN,
	//         0->TreeDirty but BTN_clean, 1->ucharDR, 2->ushortDR, 3->uintDR
	char           dirty;
} __attribute__ ((packed)) bt_n;

// BTREE access of KEYs & NODEs via position in bt_n
void *KEYS(bt *btr, bt_n *x, int i);
#define NODES(btr, x) ((bt_n **)((char *)x + btr->nodeofst))

#define GET_BTN_SIZE(leaf)   \
  size_t nsize = leaf          ? btr->kbyte : btr->nbyte;
#define GET_BTN_MSIZE(dirty) \
  size_t msize = (dirty == -1) ? nsize      : nsize + sizeof(void *);
#define GET_BTN_SIZES(leaf, dirty) \
    GET_BTN_SIZE(leaf) GET_BTN_MSIZE(dirty)
#define GET_DS(x, nsize) (*((void **)((char *)x + nsize)))

bt_n *findminnode(bt *btr, bt_n *x);
