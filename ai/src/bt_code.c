/*
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
 */

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

#include "bt.h"
#include "bt_iterator.h"
#include "stream.h"

#include <citrusleaf/alloc.h>

/* CACHE TODO LIST
   8.) U128PK/FK CACHE:[EVICT,MISS] support

  11.) DS as stream         -\/
   7.) DS in rdbSave/Load  (dependency on 11)

  12.) slab allocator for ALL btn's

  14.) btFind() in setUniqIndexVal() -> btFindD() + TESTING

  18.) CREATE TABLE () DIRTY

  19.) btreesplitchild dirty math (only set dirty if new split child has dirty)
*/

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG

//#define DEBUG_DEL_CASE_STATS
//#define BT_MEM_PROFILE
#ifdef BT_MEM_PROFILE
static ulong tot_bt_data     = 0; static ulong tot_bt_data_mem = 0;
static ulong tot_num_bt_ns   = 0; static ulong tnbtnmem        = 0;
static ulong tot_num_bts     = 0; static ulong tot_num_bt_mem  = 0;
  #define BT_MEM_PROFILE_BT   {tot_num_bts++; tot_num_bt_mem += size;}
  #define BT_MEM_PROFILE_NODE {tot_num_bt_ns++; tnbtnmem += size;}
#else
  #define BT_MEM_PROFILE_BT
  #define BT_MEM_PROFILE_NODE
#endif

/* PROTOYPES */
static void      release_dirty_stream(bt *btr, bt_n *x);
static int       real_log2           (unsigned int a, int nbits);
static bt_data_t findminkey          (bt *btr, bt_n *x);
static bt_data_t findmaxkey          (bt *btr, bt_n *x);

// HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER
static ulong getNumKey(bt *btr, bt_n *x, int i) { //TODO U128 support
    if (i < 0 || i >= x->n) return 0;
    else {
        ai_obj  akey; void *be = KEYS(btr, x, i);
        convertStream2Key(be, &akey, btr);
        return akey.l;
    }
}

// MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT
/* NOTE used-memory bookkeeping maintained at the Btree level */
static void bt_increment_used_memory(bt *btr, size_t size) {  //DEBUG_INCR_MEM
    btr->msize += (ull)size;
}
static void bt_decrement_used_memory(bt *btr, size_t size) {  //DEBUG_DECR_MEM
    btr->msize -= (ull)size;
}
// DIRTY_STREAM DIRTY_STREAM DIRTY_STREAM DIRTY_STREAM DIRTY_STREAM
static uint32 get_dssize(bt *btr, char dirty) {
    assert(dirty > 0);
    uint32 drsize = (dirty == 3) ? sizeof(uint32)   :
                    (dirty == 2) ? sizeof(ushort16) : sizeof(uchar); // 1 
    //DEBUG_GETDSSIZE
    return (btr->t * 2) * drsize;
}
static void alloc_ds(bt *btr, bt_n *x, size_t size, char dirty) {
    assert(dirty != -1);
    void   **dsp    = (void *)((char *)x + size);
    if (!dirty) { *dsp = NULL; return; }
    size_t   dssize = get_dssize(btr, dirty);
    void    *ds     = cf_malloc(dssize); bzero(ds, dssize); // FREEME 108
    bt_increment_used_memory(btr, dssize);
    *dsp            = ds;                                      //DEBUG_ALLOC_DS
}
void incr_ds(bt *btr, bt_n *x) {//USE: when a DR is too big for its DS (incr_ds)
    assert(x->dirty > 0);
    GET_BTN_SIZE(x->leaf)
    void   *ods    = GET_DS(x, nsize);
    uint32  osize  = get_dssize(btr, x->dirty);
    uint32  num    = (x->leaf ? (btr->t * 2) : btr->t);     //DEBUG_RESIZE_DS_1
    alloc_ds(btr, x, nsize, x->dirty + 1);
    void   *nds    = GET_DS(x, nsize);
    if        (x->dirty == 1) {
        uchar    *s_ds = (uchar    *)ods; ushort16 *d_ds = (ushort16 *)nds;
        for (uint32 i = 0; i < num; i++) d_ds[i] = (ushort16)s_ds[i];
    } else if (x->dirty == 2) {
        ushort16 *s_ds = (ushort16 *)ods; uint32   *d_ds = (uint32   *)nds;
        for (uint32 i = 0; i < num; i++) d_ds[i] = (uint32  )s_ds[i];
    } else assert(!"incr_ds ERROR");
    x->dirty++;                                             //DEBUG_RESIZE_DS_2
    cf_free(ods); bt_decrement_used_memory(btr, osize);
}

// BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE
// BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE BT_ALLOC_BTREE
static bt_n *allocbtreenode(bt *btr, bool leaf, char dirty) {
    btr->numnodes++;
    GET_BTN_SIZES(leaf, dirty)   BT_MEM_PROFILE_NODE          //DEBUG_ALLOC_BTN
    bt_n   *x     = cf_malloc(msize); bzero(x, msize);
    bt_increment_used_memory(btr, msize);
    x->leaf       = -1;
    x->dirty      = dirty;
    if (dirty != -1) alloc_ds(btr, x, nsize, dirty);
    return x;
}
static bt *allocbtree() {
    int  size = sizeof(struct btree);
    BT_MEM_PROFILE_BT
    bt  *btr  = (bt *) cf_malloc(size); bzero(btr, size);    // FREE ME 035
    bt_increment_used_memory(btr, size);                    //DEBUG_ALLOC_BTREE
    return btr;
}

static void release_dirty_stream(bt *btr, bt_n *x) {      //DEBUG_BTF_BTN_DIRTY
    assert(x->dirty > 0);
    GET_BTN_SIZE(x->leaf)
    bt_decrement_used_memory(btr, get_dssize(btr, x->dirty));
    void **dsp = GET_DS(x, nsize); cf_free(dsp);          // FREED 108
    x->dirty   = 0;
}
static void bt_free_btreenode(bt *btr, bt_n *x) {
    GET_BTN_SIZES(x->leaf, x->dirty) bt_decrement_used_memory(btr, msize);
    if (x->dirty > 0) release_dirty_stream(btr, x);
    cf_free(x);                                           // FREED 035
}
static void bt_free_btree(bt *btr) { cf_free(btr); }

// BT_CREATE BT_CREATE BT_CREATE BT_CREATE BT_CREATE BT_CREATE BT_CREATE
bt *bt_create(bt_cmp_t cmp, bts_t *s, char dirty) {
	int n = BTREE_LONG_TYPE_DEGREE;

	if (C_IS_L(s->ktype) || C_IS_G(s->ktype)) {
		n = BTREE_LONG_TYPE_DEGREE;
	}
	else if (C_IS_DG(s->ktype)) {
		n = BTREE_STRING_TYPE_DEGREE;
	}

    uchar  t        = (uchar)((int)(n + 1) / 2);
    int    kbyte    = sizeof(bt_n) + n * s->ksize;
    int    nbyte    = kbyte + (n + 1) * VOIDSIZE;
    bt    *btr      = allocbtree();
    if (!btr) return NULL;
    memcpy(&btr->s, s, sizeof(bts_t)); /* ktype, btype, ksize, bflag, num */
    btr->cmp        = cmp;
    btr->keyofst    = sizeof(bt_n);
    uint32 nodeofst = btr->keyofst + n * s->ksize;
    btr->nodeofst   = (ushort16)nodeofst;
    btr->t          = t;
    int nbits       = real_log2(n, sizeof(int) * 8) + 1;
    nbits           = 1 << (real_log2(nbits, sizeof(int) * 8) + 1);
    btr->nbits      = (uchar)nbits;
    btr->nbyte      = nbyte;
    btr->kbyte      = kbyte;
    btr->dirty      = dirty;
    btr->root       = allocbtreenode(btr, 1, dirty ? 0: -1);
    if (!btr->root) return NULL;
    btr->numnodes   = 1; //printf("bt_create\n"); bt_dump_info(printf, btr);
    return btr;
}

// BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH
/* This is the real log2 function.  It is only called when we don't have
 * a value in the table. -> which is basically never */
static inline int real_log2(unsigned int a, int nbits) {
    uint32 i = 0;
    uint32 b = (nbits + 1) / 2; /* divide in half rounding up */
    while (b) {
        i = (i << 1);
        if (a >= (unsigned int)(1 << b)) { // select top half and mark this bit
            a /= (1 << b);
            i  = i | 1;
        } else {                           // select bottom half & dont set bit 
            a &= (1 << b) - 1;
        }
        b /= 2;
    }
    return i;
}

#if 0

// TODO: global table is pain disabled for avoiding issue
// open it up later
/* Implement a lookup table for the log values.  This will only allocate
 * memory that we need.  This is much faster than calling the log2 routine
 * every time.  Doing 1 million insert, searches, and deletes will generate
 * ~58 million calls to log2.  Using a lookup table IS NECESSARY!
 -> memory usage of this is trivial, like less than 1KB */
static inline int _log2(unsigned int a, int nbits) {
    static char   *table   = NULL;
    static uint32  alloced = 0;
    uint32 i;
    if (a >= alloced) {
        table = cf_realloc(table, (a + 1) * sizeof *table);
        for (i = alloced; i < a + 1; i++) table[i] = -1;
        alloced = a + 1;
    }
    if (table[a] == -1) table[a] = real_log2(a, nbits);
    return table[a];
}
#endif

static inline int _log2(unsigned int a, int nbits) {
	return real_log2(a, nbits);
}

static int findkindex(bt *btr, bt_n *x, bt_data_t k, int *r, btIterator *iter) {
    if (x->n == 0) return -1;
    int b, tr;
    int *rr = r ? r : &tr ; /* rr: key is greater than current entry */
    int  i  = 0;
    int  a  = x->n - 1;
    while (a > 0) {
        b            = _log2(a, (int)btr->nbits);
        int slot     = (1 << b) + i;
        bt_data_t k2 = KEYS(btr, x, slot);
        if ((*rr = btr->cmp(k, k2)) < 0) {
            a        = (1 << b) - 1;
        } else {
            a       -= (1 << b);
            i       |= (1 << b);
        }
    }
    if ((*rr = btr->cmp(k, KEYS(btr, x, i))) < 0)  i--;
    if (iter) { iter->bln->in = iter->bln->ik = (i > 0) ? i : 0; }
    return i;
}

// KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING
// NOTE: KEYS are variable sizes: [4,8,12,16,20,24,32 bytes]
#define ISVOID(btr)  (btr->s.ksize == VOIDSIZE)

static inline void **AKEYS(bt *btr, bt_n *x, int i) {
    int   ofst = (i * btr->s.ksize);
    char *v    = (char *)x + btr->keyofst + ofst;                 //DEBUG_AKEYS
    return (void **)v;
}
#define OKEYS(btr, x) ((void **)((char *)x + btr->keyofst))
inline void *KEYS(bt *btr, bt_n *x, int i) {                       //DEBUG_KEYS
    if      ISVOID(btr) return                  OKEYS(btr, x)[i];
    else /* OTHER_BT */ return (void *)         AKEYS(btr, x, i);
}

// SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION
static inline void incr_scion(bt_n *x, int n) { x->scion += n; }
static inline void decr_scion(bt_n *x, int n) { x->scion -= n; }
static inline void move_scion(bt *btr, bt_n *y, bt_n *z, int n) {
    for (int i = 0; i < n; i++) { incr_scion(y, NODES(btr, z)[i]->scion); }
}
static inline int get_scion_range(bt *btr, bt_n *x, int beg, int end) {
    if (x->dirty <= 0) return end - beg;
    int scion = 0;
    for (int i = beg; i < end; i++) scion += 1 + getDR(btr, x, i);
    return scion;
}

// DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY
typedef struct btn_pos {
    bt_n *x; int i;
} bp_t;
typedef struct two_bp_gens {
    bp_t p; /* parent */ bp_t c; /* child */
} tbg_t;
static inline void free_bp(void *v) { cf_free(v); }

typedef struct ll_ai_bp_element_s {
    cf_ll_element   ele;
    bp_t          * value;
} ll_ai_bp_element;

void
ll_ai_bp_destroy_fn(cf_ll_element * ele)
{
    cf_free(((ll_ai_bp_element *)ele)->value);
    cf_free((ll_ai_bp_element *)ele);
}
int
ll_ai_bp_reduce_fn(cf_ll_element *ele, void *udata)
{
    return CF_LL_REDUCE_DELETE;
}

//TODO inline
bt_n *addDStoBTN(bt *btr, bt_n *x, bt_n *p, int pi, char dirty) {
    bt_n *y = allocbtreenode(btr, x->leaf, dirty);
    GET_BTN_SIZE(x->leaf) memcpy(y, x, nsize); 
    y->dirty = dirty; btr->dirty = 1;
    if (x == btr->root) btr->root         = y;
    else                NODES(btr, p)[pi] = y; // update parent NODE bookkeeping
    bt_free_btreenode(btr, x);                            //DEBUG_ADD_DS_TO_BTN
    return y;
}
uint32 getDR(bt *btr, bt_n *x, int i) {
    if (x->dirty <= 0) return 0;
    GET_BTN_SIZE(x->leaf)
    void  *dsp = GET_DS(x, nsize);;
    if        (x->dirty == 1) {
        uchar    *ds = (uchar    *)dsp; return (uint32)ds[i];
    } else if (x->dirty == 2) {
        ushort16 *ds = (ushort16 *)dsp; return (uint32)ds[i];
    } else if (x->dirty == 3) {
        uint32   *ds = (uint32   *)dsp; return         ds[i];
    } else assert(!"getDR ERROR");
}
#define INCR_DS_SET_DR                                 \
  { incr_ds(btr, x); __setDR(btr, x, i, dr); return; }

static void __setDR(bt *btr, bt_n *x, int i, uint32 dr) {
    uint32 odr; GET_BTN_SIZE(x->leaf)
    void  *dsp = GET_DS(x, nsize);
    if        (x->dirty == 1) {
        uchar    *ds = (uchar    *)dsp; if (dr > UCHAR_MAX) INCR_DS_SET_DR
        odr = ds[i]; ds[i] = dr;
    } else if (x->dirty == 2) {
        ushort16 *ds = (ushort16 *)dsp; if (dr > USHRT_MAX) INCR_DS_SET_DR
        odr = ds[i]; ds[i] = dr;
    } else if (x->dirty == 3) { 
        uint32   *ds = (uint32   *)dsp;
        odr = ds[i]; ds[i] = dr;
    } else assert(!"setDR ERROR");
    (void) odr;        // silence compiler warnings
}
static bt_n *setDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (!dr)                return x;
    if (x->dirty <= 0) x = addDStoBTN(btr, x, p, pi, 1);
    __setDR(btr, x, i, dr); return x;
}
static bt_n *zeroDR(bt *btr, bt_n *x, int i, bt_n *p, int pi) {
    (void)p; (void) pi; // compiler warnings - these will be used later
    if (x->dirty <= 0)     return x;
    __setDR(btr, x, i, 0); return x;
}
static bt_n *incrDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (!dr) return x;
    if (x->dirty <= 0) x = addDStoBTN(btr, x, p, pi, 1);
    uint32 odr  = getDR(btr, x, i);
    odr        += dr;
    return setDR(btr, x, i, odr, p, pi);
}
static bt_n *overwriteDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (dr) return setDR (btr, x, i, dr, p, pi);
    else    return zeroDR(btr, x, i,     p, pi);
}

// DEL_CASE_DR DEL_CASE_DR DEL_CASE_DR DEL_CASE_DR DEL_CASE_DR DEL_CASE_DR
static bt_n *incrPrevDR(bt   *btr, bt_n *x,  int   i, uint32 dr,
                        bt_n *p,   int   pi, cf_ll *plist) {
    if (!dr)   return x;                                   //DEBUG_INCR_PREV_DR
    if (i > 0) return incrDR(btr, x, i - 1, dr, p,  pi); // prev sibling
    else   {
        //TODO findminnode() is too inefficient -> needs to be a part of btr
        if (x == findminnode(btr, btr->root)) { // MIN KEY
            btr->dirty_left += dr; btr->dirty = 1; return x;
        }
        cf_ll_element   * ele;
        cf_ll_iterator  * iter = cf_ll_getIterator(plist, true);
        bt_n *rx = btr->root; int ri = 0;

		while ((ele = cf_ll_getNext(iter))) {
            bp_t *bp = ((ll_ai_bp_element *)ele)->value;
            if (bp->i) { rx = bp->x; ri = bp->i - 1; break; }
        }
        bt_n *prx = btr->root; int pri = 0;
        if (rx != btr->root) { // get parent
            ele = cf_ll_getNext(iter);
            bp_t *bp = ((ll_ai_bp_element *)ele)->value;
            prx = bp->x; pri = bp->i;
        }
        cf_ll_releaseIterator(iter);
        //printf("rx: %p ri: %d prx: %p pri: %d\n", rx, ri, prx, pri);
        incrDR(btr, rx, ri, dr, prx, pri);
        return x; // x not modified (only rx)
    }
}
static tbg_t get_prev_child_recurse(bt *btr, bt_n *x, int i) {
    bt_n *xp = NODES(btr, x)[i];                          //DEBUG_GET_C_REC_1
    if (!xp->leaf) return get_prev_child_recurse(btr, xp, xp->n);
    tbg_t tbg;
    tbg.p.x = x;  tbg.p.i = i;
    tbg.c.x = xp; tbg.c.i = xp->n - 1;                    //DEBUG_GET_C_REC_2
    return tbg;
}
static bt_n *incrCase2B(bt *btr, bt_n *x, int i, int dr) {  //DEBUG_INCR_CASE2B
    tbg_t  tbg = get_prev_child_recurse(btr, x, i);       //DEBUG_INCR_PREV
    bt_n  *nc  = incrDR(btr, tbg.c.x, tbg.c.i, dr, tbg.p.x, tbg.p.i);
    incr_scion(nc, dr);
    return x; // x not modified (only tbg.c.x)
}

// SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY
static void setBTKeyRaw(bt *btr, bt_n *x, int i, void *src) { //PRIVATE
    void **dest = AKEYS(btr, x, i);
    if      ISVOID(btr) *dest                  = src;   
    else                memcpy(dest, src, btr->s.ksize);
    //DEBUG_SET_KEY
}
static bt_n *setBTKey(bt *btr,  bt_n *dx, int di,  bt_n *sx, int si,
                      bool drt, bt_n *pd, int pdi, bt_n *ps, int psi) {
    if (drt) {
        uint32 dr = getDR      (btr, sx, si);             //DEBUG_SET_BTKEY
        dx        = overwriteDR(btr, dx, di, dr, pd, pdi);
        sx        = zeroDR     (btr, sx, si,     ps, psi);
    } else sx = zeroDR         (btr, sx, si,     ps, psi);
    setBTKeyRaw(btr, dx, di, KEYS(btr, sx, si)); return dx;
}

static void mvXKeys(bt *btr, bt_n   **dx, int di,
                             bt_n   **sx, int si,  uint32 num, uint32 ks,
                             bt_n    *pd, int pdi,
                             bt_n    *ps, int psi) {
    if (!num) return;
    bool x2x = (*dx == *sx); bool forward = (di >= si);
    int i    = forward ? (int)num - 1:      0;
    int end  = forward ?      -1     : (int)num;
    while (i != end) { // DS remove destDR from dx @i, add srcDR to sx @i
        int    sii = si + i; int dii = di + i;
        uint32 drs = getDR(btr, *sx, sii), drd = getDR(btr, *dx, dii);
        if (drs) {                                    //DEBUG_MV_X_KEYS_1
            *dx = setDR (btr, *dx, dii, drs, pd, pdi);
            if (x2x && *dx != *sx) *sx = *dx;
            *sx = zeroDR(btr, *sx, sii,      ps, psi);
            if (x2x && *dx != *sx) *dx = *sx;
        } else if (drd) {                             //DEBUG_MV_X_KEYS_2
            *dx = zeroDR(btr, *dx, dii, pd, pdi);
            if (x2x && *dx != *sx) *sx = *dx;
        }
        bt_data_t *dest = AKEYS(btr, *dx, di);
        bt_data_t *src  = AKEYS(btr, *sx, si);
        void      *dk   = (char *)dest + (i * ks);
        void      *sk   = (char *)src  + (i * ks);
        memcpy(dk, sk, ks);
        if (forward) i--; else i++;
    }
}
static inline void mvXNodes(bt *btr, bt_n *x, int xofst,
                                     bt_n *z, int zofst, int num) {
  memmove(NODES(btr, x) + xofst, NODES(btr, z) + zofst, (num) * VOIDSIZE);
}

//NOTE: trimBTN*() do not ever dirty btn's -- TODO they could UN-dirty
static bt_n *trimBTN(bt *btr, bt_n *x, bool drt, bt_n *p, int pi) {
  //DEBUG_TRIM_BTN
    if (drt) x = zeroDR(btr, x, x->n, p, pi);
    x->n--; return x;
}
static bt_n *trimBTN_n(bt *btr, bt_n *x, int n, bool drt, bt_n *p, int pi) {
    if (drt) {
        for (int i = x->n; i >= (x->n - n); i--) x = zeroDR(btr, x, i, p, pi);
    }
    x->n -= n; return x;
}

// INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
static bool btreesplitchild(bt *btr, bt_n *x, int i, bt_n *y, bt_n *p, int pi) {
    ushort16  t = btr->t; //TODO dirtymath
    bt_n     *z = allocbtreenode(btr, y->leaf, y->dirty); if (!z) return 0;
    z->leaf     = y->leaf; /* duplicate leaf setting */
    for (int j = 0; j < t - 1; j++) {
        z = setBTKey(btr, z, j, y, j + t, 1, p, pi, p, pi);
    }
    z->scion = get_scion_range(btr, z, 0, t - 1); decr_scion(y, z->scion);
    z->n     = t - 1; y = trimBTN_n(btr, y, t - 1, 0, p, pi);
    if (!y->leaf) { // if it's an internal node, copy the ptr's too 
        for (int j = 0; j < t; j++) {
            uint32_t scion   = NODES(btr, y)[j + t]->scion;
            decr_scion(y, scion); incr_scion(z, scion);
            NODES(btr, z)[j] = NODES(btr, y)[j + t];
        }
    }
    for (int j = x->n; j > i; j--) {      // move nodes in parent down one
        NODES(btr, x)[j + 1] = NODES(btr, x)[j];
    }
    NODES(btr, x)[i + 1] = z;             // store new node 
    for (int j = x->n - 1; j >= i; j--) { // adjust the keys from previous move
        x = setBTKey(btr, x, j + 1, x, j, 1, p, pi, p, pi);
    }
    decr_scion(y, 1 + getDR(btr, y, y->n - 1)); //NEXT LINE: store new key
    x = setBTKey(btr, x, i, y, y->n - 1, 1, p, pi, p, pi); x->n++;
    trimBTN(btr, y, 0, p, pi);
    return 1;
}

#define GETN(btr) ((2 * btr->t) - 1)
static bool bt_insertnonfull(bt  *btr, bt_n *x, bt_data_t k, bt_n *p, int pi,
                             int  dr) {
    if (x->leaf) { /* we are a leaf, just add it in */
        int i = findkindex(btr, x, k, NULL, NULL);
        if (i != x->n - 1) {
            mvXKeys(btr, &x, i + 2, &x, i + 1, (x->n - i - 1), btr->s.ksize,
                    p, pi, p, pi);
        }
        x = overwriteDR(btr, x, i + 1, dr, p, pi);
        setBTKeyRaw(btr, x, i + 1, k); x->n++; incr_scion(x, 1);
    } else { /* not leaf */
        int i = findkindex(btr, x, k, NULL, NULL) + 1;
        if (NODES(btr, x)[i]->n == GETN(btr)) { // if next node is full
            if (!btreesplitchild(btr, x, i, NODES(btr, x)[i], x, i)) return 0;
            if (btr->cmp(k, KEYS(btr, x, i)) > 0) i++;
        }
        bt_insertnonfull(btr, NODES(btr, x)[i], k, x, i, dr); incr_scion(x, 1);
    }
    return 1;
}
bool bt_insert(bt *btr, bt_data_t k, uint32 dr) {
    bt_n *r  = btr->root;
    bt_n *p  = r;
    int   pi = 0;
    if (r->n == GETN(btr)) { /* NOTE: tree increase height */
        bt_n *s          = allocbtreenode(btr, 0, r->dirty); if (!s) return 0;
        btr->root        = s;
        s->leaf          = 0;
        s->n             = 0;
        incr_scion(s, r->scion);
        NODES(btr, s)[0] = r;
        if (!btreesplitchild(btr, s, 0, r, p, pi)) return 0;
        p                = r = s;
        btr->numnodes++;
    }
    if (!bt_insertnonfull(btr, r, k, p, pi, dr)) return 0;
    btr->numkeys++;
    return 1;
}

// DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
static bt_n *replaceKeyWithGhost(bt *btr, bt_n *x, int i, bt_data_t k,
                                 uint32 dr, bt_n *p,   int   pi) {
    //printf("replaceKeyWithGhost\n");
    ai_obj akey; convertStream2Key(k, &akey, btr);
    crs_t crs; uint32 ssize; DECLARE_BT_KEY(&akey, x)
    char *stream = createStream(btr, NULL, btkey, ksize, &ssize, &crs);//DEST027
    x            = overwriteDR(btr, x, i, dr, p, pi);
    setBTKeyRaw(btr, x, i, stream);
    return x;
}

#define ADD_BP(plist, p, pi) /* used to trace path to deleted key */       \
  if (plist) {                                                             \
    bp_t *bp = (bp_t *) cf_malloc(sizeof(bp_t)); /* FREE ME 109 */         \
    bp->x = p; bp->i = pi;                                                 \
    ll_ai_bp_element * node = cf_malloc(sizeof(ll_ai_bp_element));       \
     node->value = bp;                                                      \
     cf_ll_append(plist, (cf_ll_element *)node);                            \
  }

#define CREATE_RETURN_DELETED_KEY(btr, kp, dr)        \
  dwd_t dwd; bzero(&dwd, sizeof(dwd_t)); dwd.dr = dr; \
  if (BIG_BT(btr)) { memcpy(delbuf, kp, btr->s.ksize); } \
  dwd.k = BIG_BT(btr) ? delbuf : kp;

/* NOTE: ksize > 8 bytes needs buffer for CASE 1 */
#define MAX_KEY_SIZE (AS_DIGEST_KEY_SZ *2)

#define DK_NONE 0
#define DK_2A   1
#define DK_2B   2

/* remove an existing key from the tree. KEY MUST EXIST
   the s parameter:
     1.) for normal operation pass it as DK_NONE,
     2.) delete the max node, pass it as DK_2A,
     3.) delete the min node, pass it as DK_2B.
 */
typedef struct btds_t {
    ulong leaf_del_hits; ulong leaf_del_noop;
    ulong ndel;          ulong del_calls;
    ulong case1_del;
    ulong case2A_del; ulong case2B_del; ulong case2C_del;
    ulong case3_del;
    ulong case3A1_del; ulong case3A2_del; ulong case3B1_del; ulong case3B2_del;
} btds_t;

btds_t *btds = NULL;

static dwd_t deletekey(bt   *btr,  bt_n *x,  bt_data_t k,    int    s, bool drt,
                       bt_n *p,    int   pi, cf_ll     *plist, void **c2Cp,
                       bool leafd, char delbuf[]) { btds->del_calls++;
    bt_n *xp, *y, *z; bt_data_t kp;
    int   yn, zn, i = 0, r = -1, ks = btr->s.ksize;
    if (s != DK_NONE) { /* min or max node deletion */
        if (x->leaf)             r =  0;
        else {
            if      (s == DK_2A) r =  1;   // max node
            else if (s == DK_2B) r = -1;   // min node
        }
        if      (s == DK_2A) i = x->n - 1; // max node/leaf
        else if (s == DK_2B) i = -1;       // min node/leaf
    } else i = findkindex(btr, x, k, &r, NULL);              //DEBUG_DEL_POST_S

    if (!drt) decr_scion(x, 1); // scion reduced by 1 every DELETE

    /* Case 1:
     * If the key k is in node x and x is a leaf, delete the key k from x. */
    if (x->leaf) { btds->case1_del++;
        bool rgst = 0;
        if (s == DK_2B) i++;                                 //DEBUG_DEL_CASE_1
        kp        = KEYS (btr, x, i);
        int  dr   = getDR(btr, x, i);
        CREATE_RETURN_DELETED_KEY(btr, kp, dr)
        if (drt) {                                // CASE: EVICT
            if (s == DK_NONE) {           //NOTE: only place DR grows
                x = incrPrevDR(btr, x, i, (dr + 1), p, pi, plist);
            } else decr_scion(x, 1 + dr); //NOTE: key FOR Case2A/B
        } else if (s == DK_NONE) {                // CASE: DELETE NOT CASE2A/B
            if (dr) {
                if (NBT(btr)) { x = incrPrevDR(btr, x, i, dr, p, pi, plist); }
                else { rgst = 1; // DELETE DataBT KEY w/ DR -> REPLACE w/ GHOST
                    x = replaceKeyWithGhost(btr, x, i, kp, dr, p, pi);
                }
            }
        } else if (dr) decr_scion(x, dr); // CASE: DELETE CASE2A/B
        if (!rgst) { // IF NO REPLACE_W_GHOST -> Remove from BTREE
            mvXKeys(btr, &x, i, &x, i + 1, (x->n - i - 1), ks, p, pi, p, pi);
            x      = trimBTN(btr, x, drt, p, pi);
        }
        return dwd;
    }
    dwd_t dwde; bzero(&dwde, sizeof(dwd_t));
    if (r == 0) { /* (r==0) means key found, but in node */ //DEBUG_DEL_CASE_2
        kp = KEYS(btr, x, i);
        if (!drt) { // ON DELETE
            int dr = getDR(btr, x, i);
            if (dr) { // IF DR -> REPLACE_W_GHOST, no recursive delete 
                x = replaceKeyWithGhost(btr, x, i, kp, dr, p, pi);
                CREATE_RETURN_DELETED_KEY(btr, kp, dr)
                return dwd;
            }
        }
        /* Case 2:
         * if the key k is in the node x, and x is an internal node */
        if ((yn = NODES(btr, x)[i]->n) >= btr->t) {         //DEBUG_DEL_CASE_2a
            btds->case2A_del++;
            if (leafd) return dwde;
            /* Case 2a:
             * if the node y that precedes k in node x has at least t keys,
             * then find the previous sequential key (kp) of k.
             * Recursively delete kp, and replace k with kp in x. */
            xp         = NODES(btr, x)[i];
            ADD_BP(plist, x, i)
            //printf("CASE2A recurse: key: "); printKey(btr, x, i);
            dwd_t dwd  = deletekey(btr, xp, NULL, DK_2A, drt,
                                   x, i, plist, c2Cp, leafd, delbuf);
            //DEBUG_SET_BTKEY_2A
            if (drt) x = incrDR(btr, x, i, ++dwd.dr, p, pi);
            else     x = setDR (btr, x, i, dwd.dr,   p, pi);
            setBTKeyRaw(btr, x, i, dwd.k);
            dwd.k      = kp; // swap back in KPs original value
            return dwd;
        }
        if ((zn = NODES(btr, x)[i + 1]->n) >= btr->t) {     //DEBUG_DEL_CASE_2b
            btds->case2B_del++;
            if (leafd) return dwde;
            /* Case 2b:
             * if the node z that follows k in node x has at least t keys,
             * then find the next sequential key (kp) of k. Recursively delete
             * kp, and replace k with kp in x. */
            xp         = NODES(btr, x)[i + 1];
            ADD_BP(plist, x, i + 1)
            //printf("CASE2B recurse: key: "); printKey(btr, x, i);
            dwd_t dwd  = deletekey(btr, xp, NULL, DK_2B, drt,
                                   x, i + 1, plist, c2Cp, leafd, delbuf);
            //DEBUG_SET_BTKEY_2B
            if (drt) { // prev key inherits DR+1
                x      = incrCase2B (btr, x, i, (getDR(btr, x, i) + 1));
            } 
            x          = overwriteDR(btr, x, i, dwd.dr, p, pi);
            setBTKeyRaw(btr, x, i, dwd.k);
            dwd.k      = kp; // swap back in KPs original value
            return dwd;
        }
        if (yn == btr->t - 1 && zn == btr->t - 1) {         //DEBUG_DEL_CASE_2c
            btds->case2C_del++;
            if (leafd) return dwde;
            /* Case 2c:
             * if both y and z have only t - 1 keys, merge k
             * then all of z into y, so that x loses both k and
             * the pointer to z, and y now contains 2t - 1 keys. */
            if (!*c2Cp) *c2Cp = KEYS(btr, x, i); //used in remove_key()
            y = NODES(btr, x)[i];
            z = NODES(btr, x)[i + 1];
            dwd_t dwd; dwd.k = k; dwd.dr = getDR(btr, x, i);
            incr_scion(y, 1 + dwd.dr);                     //DEBUG_SET_BTKEY_2C
            y = setDR  (btr, y, y->n, dwd.dr, x, i);
            setBTKeyRaw(btr, y, y->n, dwd.k); y->n++;
            incr_scion(y, get_scion_range(btr, z, 0, z->n));
            mvXKeys(btr, &y, y->n, &z, 0, z->n, ks, x, i, x, i + 1);
            if (!y->leaf) {
                move_scion(btr, y,       z,     z->n + 1);
                mvXNodes  (btr, y, y->n, z, 0, (z->n + 1));
            }
            y->n += z->n;
            mvXKeys (btr, &x, i, &x, i + 1,   (x->n - i - 1), ks, p, pi, p, pi);
            mvXNodes(btr, x, i + 1, x, i + 2, (x->n - i - 1));
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, z);
            ADD_BP(plist, x, i)
            //printf("CASE2C key: "); printKey(btr, x, i);
            return deletekey(btr, y, k, s, drt, x, i, plist, c2Cp, leafd, delbuf);
        }
    }
    /* Case 3:
     * if k is not present in internal node x, determine the root xp of
     * the appropriate subtree that must contain k, if k is in the tree
     * at all.  If xp has only t - 1 keys, execute step 3a or 3b as
     * necessary to guarantee that we descend to a node containing at
     * least t keys.  Finish by recursing on the appropriate node of x. */
    i++;
    if ((xp = NODES(btr, x)[i])->n == btr->t - 1) { /* case 3a-c are !x->leaf */
        /* Case 3a:
         * If xp has only (t-1) keys but has a sibling(y) with at least t keys,
           give xp an extra key by moving a key from x down into xp,
           moving a key from xp's immediate left or right sibling(y) up into x,
           & moving the appropriate node from the sibling(y) into xp. */
        if (i > 0 && (y = NODES(btr, x)[i - 1])->n >= btr->t) {
            btds->case3A1_del++;
            //printf("CASE3A1 key: "); printKey(btr, x, i);
            if (leafd) return dwde;
            /* left sibling has t keys */                  //DEBUG_DEL_CASE_3a1
            mvXKeys(btr, &xp, 1, &xp, 0, xp->n, ks, x, i, x, i);
            if (!xp->leaf) mvXNodes(btr, xp, 1, xp, 0, (xp->n + 1));
            incr_scion(xp, 1 + getDR(btr, x, i - 1));
            xp = setBTKey(btr, xp, 0, x, i - 1, drt, x,  i,  p, pi); xp->n++;
            decr_scion(y, 1 + getDR(btr, y, y->n - 1));
            x  = setBTKey(btr, x,  i - 1, y, y->n - 1, drt, p,  pi, x, i - 1);
            if (!xp->leaf) {
                int dscion = NODES(btr, y)[y->n]->scion;
                incr_scion(xp, dscion); decr_scion(y, dscion);
                NODES(btr, xp)[0] = NODES(btr, y)[y->n];
            }
            y  = trimBTN(btr, y, drt, x, i - 1);
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n >= btr->t) {
            btds->case3A2_del++;
            //printf("CASE3A2 key: "); printKey(btr, x, i);
            if (leafd) return dwde;
            /* right sibling has t keys */                 //DEBUG_DEL_CASE_3a2
            incr_scion(xp, 1 + getDR(btr, x, i));
            xp = setBTKey(btr, xp, xp->n++, x, i, drt, x, i, p, pi);
            decr_scion(y, 1 + getDR(btr, y, 0));
            x  = setBTKey(btr, x,  i,       y, 0, drt, p, pi, x, i + 1);
            if (!xp->leaf) {
                int dscion = NODES(btr, y)[0]->scion;
                incr_scion(xp, dscion); decr_scion(y, dscion);
                NODES(btr, xp)[xp->n] = NODES(btr, y)[0];
            }
            mvXKeys(btr, &y, 0, &y, 1, y->n - 1, ks, x, i + 1, x, i + 1);
            if (!y->leaf) mvXNodes(btr, y, 0, y, 1, y->n);
            y  = trimBTN(btr, y, drt, x, i + 1);
        }
        /* Case 3b:
         * If xp and all of xp's siblings have t - 1 keys, merge xp with
           one sibling, which involves moving a key from x down into the
           new merged node to become the median key for that node.  */
        else if (i > 0 && (y = NODES(btr, x)[i - 1])->n == btr->t - 1) {
            btds->case3B1_del++;
            //printf("CASE3B1 key: "); printKey(btr, x, i);
            if (leafd) return dwde;
            /* merge i with left sibling */                //DEBUG_DEL_CASE_3b1
            incr_scion(y, 1 + getDR(btr, x, i - 1));
            y = setBTKey(btr, y, y->n++, x, i - 1, drt, x, i - 1, p, pi);
            incr_scion(y, get_scion_range(btr, xp, 0, xp->n));
            mvXKeys(btr, &y, y->n, &xp, 0, xp->n, ks, x, i - 1, x, i);
            if (!xp->leaf) {
                move_scion(btr, y,       xp,     xp->n + 1);
                mvXNodes  (btr, y, y->n, xp, 0, (xp->n + 1));
            }
            y->n += xp->n;
            mvXKeys (btr, &x, i - 1, &x, i, (x->n - i), ks, p, pi, p, pi);
            mvXNodes(btr, x, i, x, i + 1, (x->n - i));
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, xp);
            xp = y; i--; // i-- for parent-arg in recursion (below)
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n == btr->t - 1) {
            btds->case3B2_del++;
            //printf("CASE3B2 key: "); printKey(btr, x, i);
            if (leafd) return dwde;
            /* merge i with right sibling */               //DEBUG_DEL_CASE_3b2
            incr_scion(xp, 1 + getDR(btr, x, i));
            xp = setBTKey(btr, xp, xp->n++, x, i, drt, x, i, p, pi);
            incr_scion(xp, get_scion_range(btr, y, 0, y->n));
            mvXKeys(btr, &xp, xp->n, &y, 0, y->n, ks, x, i, x, i + 1);
            if (!xp->leaf) {
                move_scion(btr, xp,        y,     y->n + 1);
                mvXNodes  (btr, xp, xp->n, y, 0, (y->n + 1));
            }
            xp->n += y->n;
            mvXKeys (btr, &x, i, &x, i + 1, (x->n - i - 1), ks, p, pi, p, pi);
            mvXNodes(btr, x, i + 1, x, i + 2, (x->n - i - 1));
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, y);
        }
    } //printf("RECURSE CASE 3\n");
    btds->case3_del++;
    ADD_BP(plist, x, i)                                 //DEBUG_DEL_POST_CASE_3
    dwd_t dwd = deletekey(btr, xp, k, s, drt, x, i, plist, c2Cp, leafd, delbuf);
    // CASE2A/B pull keys up from depths, scion must be decremented
    if (s != DK_NONE) {
        if (drt) decr_scion(x, 1 + dwd.dr);
        else     decr_scion(x, dwd.dr);     // DELETE already decr_scion()ed 1
    }
    return dwd;
}

#ifdef DEBUG_DEL_CASE_STATS
static void print_del_case_stats(bool leafd, dwd_t dwd, bt *btr) {
    if (leafd) {
        if (!dwd.k) btds->leaf_del_noop++;
        else        btds->leaf_del_hits++;
        printf("deletes: %lu noop: %lu ratio: %f numkeys: %d\n",
               btds->leaf_del_hits, btds->leaf_del_noop,
               (btds->leaf_del_noop && btds->leaf_del_hits) ?
                (double)((double)btds->leaf_del_hits /
                         (double)btds->leaf_del_noop) : 0,
               btr->numkeys);
    } else
    printf("ndel: %lu ncalls: %lu C1: %lu(%.2f) C2A: %lu(%.2f) "
           "C2B: %lu(%.2f) C2C: %lu(%.2f) C3: %lu(%.2f) "
           "C3A1: %lu(%.2f) C3A2: %lu(%.2f) C3B1: %lu(%.2f) "
           "C3B2: %lu(%.2f)\n",
           btds->ndel, btds->del_calls,
           btds->case1_del,
           (double)((double)btds->case1_del / (double)btds->del_calls),
           btds->case2A_del,
           (double)((double)btds->case2A_del / (double)btds->del_calls),
           btds->case2B_del,
           (double)((double)btds->case2B_del / (double)btds->del_calls),
           btds->case2C_del,
           (double)((double)btds->case2C_del / (double)btds->del_calls),
           btds->case3_del,
           (double)((double)btds->case3_del / (double)btds->del_calls),
           btds->case3A1_del,
           (double)((double)btds->case3A1_del / (double)btds->del_calls),
           btds->case3A2_del,
           (double)((double)btds->case3A2_del / (double)btds->del_calls),
           btds->case3B1_del,
           (double)((double)btds->case3B1_del / (double)btds->del_calls),
           btds->case3B2_del,
           (double)((double)btds->case3B2_del / (double)btds->del_calls));
    fflush(NULL);
}
#endif

static dwd_t remove_key(bt *btr, bt_data_t k, bool drt, bool leafd) {
    if (!btds) { btds = cf_malloc(sizeof(btds_t)); bzero(btds, sizeof(btds_t)); }
    btds->ndel++;
    if (!btr->root) { dwd_t dwde; bzero(&dwde, sizeof(dwd_t)); return dwde; }
    void *c2Cp = NULL; /* NOTE: c2Cp gets lost in recursion */ //DEBUG_DEL_START
    bt_n  *p    = btr->root; int pi = 0;
    cf_ll plist_tmp;
	cf_ll * plist = &plist_tmp;  // NOTE: plist stores ancestor line during recursive delete
    if (drt) {
        cf_ll_init(plist, ll_ai_bp_destroy_fn, false);
        ADD_BP(plist, p, pi);//FR110
    } else plist = NULL;
    char delbuf[MAX_KEY_SIZE]; // NOTE: ksize > 8B needs buffer for CASE 1
	dwd_t dwd   = deletekey(btr, btr->root, k, DK_NONE, drt,
                            p, pi, plist, &c2Cp, leafd, delbuf);
#ifdef DEBUG_DEL_CASE_STATS
	print_del_case_stats(leafd, dwd, btr);
#endif
    if (!dwd.k) return dwd; // leafd NO-OP
    btr->numkeys--;                                             //DEBUG_DEL_END
    /* remove empty non-leaf node from root, */
    if (!btr->root->n && !btr->root->leaf) { /* NOTE: tree decrease height */
        btr->numnodes--;
        bt_n *x   = btr->root;
        btr->root = NODES(btr, x)[0];
        bt_free_btreenode(btr, x);
    }
    if (c2Cp) dwd.k = c2Cp;
    if (plist) {
		cf_ll_reduce(plist, true, ll_ai_bp_reduce_fn, NULL);
		plist = NULL;
	};                       // FREED 110
    return dwd;
}
dwd_t bt_delete(bt *btr, bt_data_t k, bool leafd) {
    return      remove_key(btr, k, 0, leafd);
}

// ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS
static inline bool key_covers_miss(bt *btr, bt_n *x, int i, ai_obj *akey) {
    if (!(C_IS_NUM(btr->s.ktype))) return 0;
    if (i < 0) i = 0;
    ulong mkey = getNumKey(btr, x, i);
    ulong dr   = (ulong)getDR(btr, x, i);
    if (mkey && dr) {
        ulong qkey = akey->l;
        ulong span = mkey + dr;
        //DEBUG_CURRKEY_MISS
        if (qkey >= mkey && qkey <= span) return 1;
    }
    return 0;
}
#define SET_DWM_XIP { dwm.x = x; dwm.i = i; dwm.p = p; dwm.pi = pi; }
dwm_t findnodekey(bt *btr, bt_n *x, bt_data_t k, ai_obj *akey) {
    int    r = -1,             i = 0;
    bt_n  *p = btr->root; int pi = 0;
    dwm_t  dwm; bzero(&dwm, sizeof(dwm_t)); SET_DWM_XIP
    while (x) {
        i = findkindex(btr, x, k, &r, NULL);              //DEBUG_FIND_NODE_KEY
        if (i >= 0 && !r) { SET_DWM_XIP dwm.k = KEYS(btr, x, i); return dwm; }
        if (key_covers_miss(btr, x, i, akey)) { SET_DWM_XIP dwm.miss = 1; }
        if (x->leaf)       {            dwm.k = NULL;            return dwm; }
        p = x; pi = i + 1; x = NODES(btr, x)[i + 1];
    }
    return dwm;
}
bt_data_t bt_find(bt *btr, bt_data_t k, ai_obj *akey) { //Indexes still use this
    dwm_t dwm = findnodekey(btr, btr->root, k, akey);
    return dwm.k;
}

static bool check_min_miss(bt *btr, ai_obj *alow) {
    if (!btr->dirty_left) return 0;
    ai_obj amin; convertStream2Key(bt_min(btr), &amin, btr);
    return ai_objEQ(alow, &amin);
}
int bt_init_iterator(bt *btr, bt_data_t k, btIterator *iter, ai_obj *alow) {
    if (!btr->root) return II_FAIL;
    int    r          = -1;
    bool   lmiss      = check_min_miss(btr, alow);
    bool   miss       =  0;
    uchar  only_right =  1;
    bt_n  *x          = btr->root;
    while (x) {
        int i = findkindex(btr, x, k, &r, iter);
        if (i >= 0 && r == 0) return lmiss ? II_L_MISS : II_OK;
        if (key_covers_miss(btr, x, i, alow)) miss = 1; //DEBUG_BT_II
        if (miss)             return II_MISS;
        if (r < 0 || i != (x->n - 1)) only_right = 0;
        if (x->leaf) {
            if      (i != (x->n - 1)) only_right = 0;
            return only_right ? II_ONLY_RIGHT : II_LEAF_EXIT;
        }
        iter->bln->child = get_new_iter_child(iter);
        x                = NODES(btr, x)[i + 1];
        to_child(iter, x);
    }
    return II_FAIL;
}

bool bt_exist(bt *btr, bt_data_t k, ai_obj *akey) {
    int   r  = -1;
    bt_n *x  = btr->root;
    while (x) {
        int i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0)                 return 1;
        if (key_covers_miss(btr, x, i, akey)) return 1;
        if (x->leaf)                          return 0;
        x = NODES(btr, x)[i + 1];
    }
    return 0;
}

static bt_data_t findminkey(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, 0);
    else         return findminkey(btr, NODES(btr, x)[0]);
}
bt_n *findminnode(bt *btr, bt_n *x) {
    if (x->leaf) return x;
    else         return findminnode(btr, NODES(btr, x)[0]);
}
static bt_data_t findmaxkey(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, x->n - 1);
    else         return findmaxkey(btr, NODES(btr, x)[x->n]);
}
bt_data_t bt_min(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    else                             return findminkey(btr, btr->root);
}
bt_data_t bt_max(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    else                             return findmaxkey(btr, btr->root);
}

// DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR
static void destroy_bt_node(bt *btr, bt_n *x) {
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            destroy_bt_node(btr, NODES(btr, x)[i]);
        }
	}
    bt_free_btreenode(btr, x); /* memory management in btr */
}
void bt_destroy(bt *btr) {
    if (btr->root) {
        if (btr->numkeys) destroy_bt_node  (btr, btr->root);
        else              bt_free_btreenode(btr, btr->root); 
        btr->root  = NULL;
    }
    bt_free_btree(btr);
}
