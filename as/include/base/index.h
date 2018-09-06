/*
 * index.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "cf_mutex.h"

#include "base/datamodel.h"


//==========================================================
// Index tree node - as_index, also known as as_record.
//
// There's one for every record. Contains metadata, and
// points to record data in memory and/or on storage device.
//

typedef struct as_index_s {

	// offset: 0
	uint16_t rc; // for now, incremented & decremented only when reducing sprig

	// offset: 2
	uint8_t : 8; // reserved for bigger rc, if needed

	// offset: 3
	uint8_t tree_id: 6;
	uint8_t color: 1;
	uint8_t : 1;

	// offset: 4
	cf_digest keyd;

	// offset: 24
	uint64_t left_h: 40;
	uint64_t right_h: 40;

	// offset: 34
	uint16_t set_id_bits: 10;
	uint16_t : 6;

	// offset: 36
	uint32_t : 2;
	uint32_t void_time: 30;

	// offset: 40
	uint64_t last_update_time: 40;
	uint64_t generation: 16;

	// offset: 47
	// Used by the storage engines.
	uint64_t rblock_id: 37;		// can address 2^37 * 16b = 2Tb drive
	uint64_t n_rblocks: 19;		// is enough for 8Mb/16b = 512K rblocks
	uint64_t file_id: 7;		// can spec 2^7 = 128 drives
	uint64_t key_stored: 1;

	// offset: 55
	// In single-bin mode for data-in-memory namespaces, this offset is cast to
	// an as_bin, but only 4 bits get used (for the iparticle state). The other
	// 4 bits are used for replication state and index flags.
	uint8_t repl_state: 2;
	uint8_t tombstone: 1;
	uint8_t cenotaph: 1;
	uint8_t single_bin_state: 4; // used indirectly, only in single-bin mode

	// offset: 56
	// For data-not-in-memory namespaces, these 8 bytes are currently unused.
	// For data-in-memory namespaces: in single-bin mode the as_bin is embedded
	// here (these 8 bytes plus 4 bits in flex_bits above), but in multi-bin
	// mode this is a pointer to either of:
	// - an as_bin_space containing n_bins and an array of as_bin structs
	// - an as_rec_space containing an as_bin_space pointer and other metadata
	void* dim;

	// final size: 64

} __attribute__ ((__packed__)) as_index;

COMPILER_ASSERT(sizeof(as_index) == 64);

#define AS_INDEX_SINGLE_BIN_OFFSET 55 // can't use offsetof() with bit fields


//==========================================================
// Accessor functions for bits in as_index.
//

#define as_index_reserve(_r) ({ \
	uint16_t rc = as_aaf_uint16(&(_r->rc), 1); \
	cf_assert(rc != 0, AS_INDEX, "ref-count overflow"); \
	rc; \
})

#define as_index_release(_r) ({ \
	uint16_t rc = as_aaf_uint16(&(_r->rc), -1); \
	cf_assert(rc != (uint16_t)-1, AS_INDEX, "ref-count underflow"); \
	rc; \
})

#define TREE_ID_NUM_BITS	6
#define MAX_NUM_TREE_IDS	(1 << TREE_ID_NUM_BITS) // 64
#define TREE_ID_MASK		(MAX_NUM_TREE_IDS - 1) // 0x3F

COMPILER_ASSERT(MAX_NUM_TREE_IDS <= 64); // must fit in 64-bit map

// Fast way to clear the record portion of as_index.
// Note - relies on current layout and size of as_index!
// FIXME - won't be able to "rescue" with future sindex method - will go away.
static inline
void as_index_clear_record_info(as_index *index)
{
	index->set_id_bits = 0;

	*(uint32_t*)((uint8_t*)index + 36) = 0;

	uint64_t *p_clear = (uint64_t*)((uint8_t*)index + 40);

	*p_clear++	= 0;
	*p_clear++	= 0;
	*p_clear	= 0;
}

// Generation 0 is never written, and generation plays no role in record
// destruction, so it works to flag deleted records.
static inline
void as_index_invalidate_record(as_index *index) {
	index->generation = 0;
}

static inline
bool as_index_is_valid_record(as_index *index) {
	return index->generation != 0;
}


//------------------------------------------------
// Single bin, as_bin_space & as_rec_space.
//

static inline
as_bin *as_index_get_single_bin(const as_index *index) {
	// We only use 4 bits of the first byte for the bin state.
	return (as_bin*)((uint8_t *)index + AS_INDEX_SINGLE_BIN_OFFSET);
}

static inline
as_bin_space* as_index_get_bin_space(const as_index *index) {
	return index->key_stored == 1 ?
		   ((as_rec_space*)index->dim)->bin_space : (as_bin_space*)index->dim;
}

static inline
void as_index_set_bin_space(as_index* index, as_bin_space* bin_space) {
	if (index->key_stored == 1) {
		((as_rec_space*)index->dim)->bin_space = bin_space;
	}
	else {
		index->dim = (void*)bin_space;
	}
}


//------------------------------------------------
// Set-ID bits.
//

static inline
uint16_t as_index_get_set_id(const as_index *index) {
	return index->set_id_bits;
}

static inline
void as_index_set_set_id(as_index *index, uint16_t set_id) {
	// TODO - check that it fits in the 10 bits ???
	index->set_id_bits = set_id;
}


//------------------------------------------------
// Set-ID helpers.
//

static inline
int as_index_set_set_w_len(as_index *index, as_namespace *ns,
		const char *set_name, size_t len, bool apply_restrictions) {
	uint16_t set_id;
	int rv = as_namespace_set_set_w_len(ns, set_name, len, &set_id,
			apply_restrictions);

	if (rv != 0) {
		return rv;
	}

	as_index_set_set_id(index, set_id);
	return 0;
}

static inline
const char *as_index_get_set_name(as_index *index, as_namespace *ns) {
	return as_namespace_get_set_name(ns, as_index_get_set_id(index));
}


//==========================================================
// Handling as_index objects.
//

// Container for as_index pointer with lock and location.
struct as_index_ref_s {
	as_index			*r;
	cf_arenax_handle	r_h;
	cf_arenax_puddle	*puddle;
	cf_mutex			*olock;
};


//==========================================================
// Index tree.
//

typedef void (*as_index_tree_done_fn) (uint8_t id, void *udata);

typedef struct as_index_tree_s {
	uint8_t					id;
	as_index_tree_done_fn	done_cb;
	void					*udata;

	// Data common to all trees in a namespace.
	as_index_tree_shared	*shared;

	cf_atomic64				n_elements;

	// Variable length data, dependent on configuration.
	uint8_t					data[];
} as_index_tree;


//==========================================================
// as_index_tree variable length data components.
//

#define NUM_LOCK_PAIRS 256 // per partition

typedef struct as_lock_pair_s {
	// Note: reduce_lock's scope is always inside of lock's scope.
	cf_mutex lock;        // insert, delete vs. insert, delete, get
	cf_mutex reduce_lock; // insert, delete vs. reduce
} as_lock_pair;

#define NUM_SPRIG_BITS 28 // 3.5 bytes - yes, that's a lot of sprigs

typedef struct as_sprig_s {
	uint64_t n_elements: 24; // max 16M records per sprig
	uint64_t root_h: 40;
} as_sprig;

static inline as_lock_pair *
tree_locks(as_index_tree *tree)
{
	return (as_lock_pair*)tree->data;
}

static inline as_sprig *
tree_sprigs(as_index_tree *tree)
{
	return (as_sprig*)(tree->data + tree->shared->sprigs_offset);
}

static inline cf_arenax_puddle *
tree_puddle_for_sprig(as_index_tree *tree, int sprig_i)
{
	uint32_t puddles_offset = tree->shared->puddles_offset;
	return puddles_offset == 0 ?
			NULL : (cf_arenax_puddle*)(tree->data + puddles_offset) + sprig_i;
}

static inline cf_arenax_puddle *
tree_puddles(as_index_tree *tree)
{
	return tree_puddle_for_sprig(tree, 0);
}

static inline size_t
tree_puddles_size(as_index_tree_shared *shared)
{
	return shared->puddles_offset == 0 ?
			0 : sizeof(cf_arenax_puddle) * shared->n_sprigs;
}

static inline uint32_t
tree_puddles_count(as_index_tree_shared *shared)
{
	return shared->puddles_offset == 0 ? 0 : shared->n_sprigs;
}


//------------------------------------------------
// as_index_tree public API.
//

void as_index_tree_gc_init();
int as_index_tree_gc_queue_size();

as_index_tree *as_index_tree_create(as_index_tree_shared *shared, uint8_t id, as_index_tree_done_fn cb, void *udata);
as_index_tree *as_index_tree_resume(as_index_tree_shared *shared, as_treex* xmem_trees, uint32_t pid, as_index_tree_done_fn cb, void *udata);
void as_index_tree_block(as_index_tree *tree);
void as_index_tree_reserve(as_index_tree *tree);
int as_index_tree_release(as_index_tree *tree);
uint64_t as_index_tree_size(as_index_tree *tree);

typedef void (*as_index_reduce_fn) (as_index_ref *value, void *udata);

void as_index_reduce(as_index_tree *tree, as_index_reduce_fn cb, void *udata);
void as_index_reduce_partial(as_index_tree *tree, uint64_t sample_count, as_index_reduce_fn cb, void *udata);

void as_index_reduce_live(as_index_tree *tree, as_index_reduce_fn cb, void *udata);
void as_index_reduce_partial_live(as_index_tree *tree, uint64_t sample_count, as_index_reduce_fn cb, void *udata);

int as_index_try_exists(as_index_tree *tree, const cf_digest *keyd);
int as_index_try_get_vlock(as_index_tree *tree, const cf_digest *keyd, as_index_ref *index_ref);
int as_index_get_vlock(as_index_tree *tree, const cf_digest *keyd, as_index_ref *index_ref);
int as_index_get_insert_vlock(as_index_tree *tree, const cf_digest *keyd, as_index_ref *index_ref);
int as_index_delete(as_index_tree *tree, const cf_digest *keyd);


//------------------------------------------------
// Private API - for enterprise separation only.
//

// Container for sprig-level function parameters.
typedef struct as_index_sprig_s {
	as_index_value_destructor destructor;
	void *destructor_udata;

	cf_arenax *arena;

	as_lock_pair *pair;
	as_sprig *sprig;
	cf_arenax_puddle *puddle;
} as_index_sprig;

uint64_t as_index_sprig_keyd_reduce_partial(as_index_sprig *isprig, uint64_t sample_count, as_index_reduce_fn cb, void *udata);

int as_index_sprig_get_vlock(as_index_sprig *isprig, const cf_digest *keyd, as_index_ref *index_ref);
int as_index_sprig_delete(as_index_sprig *isprig, const cf_digest *keyd);

static inline void
as_index_sprig_from_keyd(as_index_tree *tree, as_index_sprig *isprig,
		const cf_digest *keyd)
{
	// Get the 28 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the (12 bit) partition-ID
	// from the digest.
	uint32_t bits = (((uint32_t)keyd->digest[1] & 0xF0) << 20) |
			((uint32_t)keyd->digest[2] << 16) |
			((uint32_t)keyd->digest[3] << 8) |
			(uint32_t)keyd->digest[4];

	uint32_t lock_i = bits >> tree->shared->locks_shift;
	uint32_t sprig_i = bits >> tree->shared->sprigs_shift;

	isprig->destructor = tree->shared->destructor;
	isprig->destructor_udata = tree->shared->destructor_udata;
	isprig->arena = tree->shared->arena;
	isprig->pair = tree_locks(tree) + lock_i;
	isprig->sprig = tree_sprigs(tree) + sprig_i;
	isprig->puddle = tree_puddle_for_sprig(tree, sprig_i);
}

#define SENTINEL_H 0

#define RESOLVE_H(__h) ((as_index*)cf_arenax_resolve(isprig->arena, __h))

// Flag to indicate full index reduce.
#define AS_REDUCE_ALL (-1L)
