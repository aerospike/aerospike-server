/*
 * index.h
 *
 * Copyright (C) 2008-2021 Aerospike, Inc.
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
	uint8_t is_orig: 1;

	// offset: 4
	cf_digest keyd;

	// offset: 24
	uint64_t left_h: 40;
	uint64_t right_h: 40;

	// offset: 34
	uint16_t set_id_bits: 12;
	uint16_t in_sindex: 1;
	uint16_t xdr_bin_cemetery: 1;
	uint16_t has_bin_meta: 1; // named for warm restart erase (was for old DIM)
	uint16_t xdr_write: 1;

	// offset: 36
	uint32_t xdr_tombstone: 1;
	uint32_t xdr_nsup_tombstone: 1;
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
	uint8_t repl_state: 2;
	uint8_t tombstone: 1;
	uint8_t cenotaph: 1;
	uint8_t : 4;

	// offset: 56
	// MRT-related members.
	union {
		struct {
			uint64_t orig_h: 40;
			uint64_t : 24;
		} __attribute__ ((__packed__));

		uint64_t mrt_id;
	};

	// final size: 64

} __attribute__ ((__packed__)) as_index;

COMPILER_ASSERT(sizeof(as_index) == 64);


//==========================================================
// Accessor functions for bits in as_index.
//

static inline uint16_t
as_index_reserve(as_index* index)
{
	uint16_t rc = as_aaf_uint16(&(index->rc), 1);

	cf_assert(rc != 0, AS_INDEX, "ref-count overflow");

	return rc;
}

// No '_rls' needed on atomic decrement here since this is always under olock.
static inline uint16_t
as_index_release(as_index* index)
{
	uint16_t rc = as_aaf_uint16(&(index->rc), -1);

	cf_assert(rc != (uint16_t)-1, AS_INDEX, "ref-count underflow");

	return rc;
}

// MRT PARANOIA - clear unused bits in an orig_r.
static inline void
clear_unused_orig_bits(as_index* orig_r)
{
	orig_r->rc = 0;
	orig_r->color = 0;
	orig_r->left_h = 0;
	orig_r->right_h = 0;
}

// Fast way to clear the record portion of as_index.
// Note - relies on current layout and size of as_index!
static inline void
as_index_clear_record_info(as_index* index)
{
	*(uint16_t*)((uint8_t*)index + 34) = 0;
	*(uint32_t*)((uint8_t*)index + 36) = 0;

	uint64_t *p_clear = (uint64_t*)((uint8_t*)index + 40);

	*p_clear++	= 0;
	*p_clear++	= 0;
	*p_clear	= 0;
}

// Generation 0 is never written, and generation plays no role in record
// destruction, so it works to flag deleted records.
static inline void
as_index_invalidate_record(as_index* index)
{
	index->generation = 0;
}

static inline bool
as_index_is_valid_record(as_index* index)
{
	return index->generation != 0;
}

static inline void
as_index_set_in_sindex(as_index* index)
{
	if (index->in_sindex == 0) {
		as_index_reserve(index);
		index->in_sindex = 1;
	}
}

static inline void
as_index_clear_in_sindex(as_index* index)
{
	if (index->in_sindex == 1) {
		as_index_release(index);
		index->in_sindex = 0;
	}
}


//------------------------------------------------
// Set-ID bits.
//

static inline uint16_t
as_index_get_set_id(const as_index* index)
{
	return index->set_id_bits;
}

static inline void
as_index_set_set_id(as_index* index, uint16_t set_id)
{
	index->set_id_bits = set_id;
}


//------------------------------------------------
// Set-ID helpers.
//

static inline int
as_index_set_set_w_len(as_index* index, as_namespace* ns, const char* set_name,
		uint32_t len, bool apply_count_limit)
{
	uint16_t set_id;
	int rv = as_namespace_set_set_w_len(ns, set_name, len, &set_id,
			apply_count_limit);

	if (rv != AS_OK) {
		return rv;
	}

	as_index_set_set_id(index, set_id);
	return AS_OK;
}

static inline const char*
as_index_get_set_name(const as_index* index, as_namespace* ns)
{
	return as_namespace_get_set_name(ns, as_index_get_set_id(index));
}


//==========================================================
// Handling as_index objects.
//

// Container for as_index pointer with lock and location.
typedef struct as_index_ref_s {
	as_index* r;
	cf_arenax_handle r_h;
	cf_arenax_puddle* puddle;
	cf_mutex* olock;
} as_index_ref;


//==========================================================
// Index tree.
//

struct as_set_index_tree_s;

typedef void (*as_index_tree_done_fn) (uint8_t id, void* udata);

typedef struct as_index_tree_s {
	uint8_t id;
	as_index_tree_done_fn done_cb;
	void* udata;

	// Data common to all trees in a namespace.
	as_index_tree_shared* shared;

	uint64_t n_elements;

	cf_mutex set_trees_lock;
	struct as_set_index_tree_s* set_trees[1 + AS_SET_MAX_COUNT]; // 32M/cluster

	// Variable length data, dependent on configuration.
	uint8_t data[];
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
	uint64_t root_h: 40;
} __attribute__((packed)) as_sprig;

static inline as_lock_pair*
tree_locks(as_index_tree* tree)
{
	return (as_lock_pair*)tree->data;
}

static inline as_sprig*
tree_sprigs(as_index_tree* tree)
{
	return (as_sprig*)(tree->data + tree->shared->sprigs_offset);
}

static inline cf_arenax_puddle*
tree_puddle_for_sprig(as_index_tree* tree, int sprig_i)
{
	uint32_t puddles_offset = tree->shared->puddles_offset;

	return puddles_offset == 0 ?
			NULL : (cf_arenax_puddle*)(tree->data + puddles_offset) + sprig_i;
}

static inline cf_arenax_puddle*
tree_puddles(as_index_tree* tree)
{
	return tree_puddle_for_sprig(tree, 0);
}

static inline size_t
tree_puddles_size(as_index_tree_shared* shared)
{
	return shared->puddles_offset == 0 ?
			0 : sizeof(cf_arenax_puddle) * shared->n_sprigs;
}

static inline uint32_t
tree_puddles_count(as_index_tree_shared* shared)
{
	return shared->puddles_offset == 0 ? 0 : shared->n_sprigs;
}


//------------------------------------------------
// as_index_tree public API.
//

void as_index_tree_gc_init();
uint32_t as_index_tree_gc_queue_size();
void as_index_tree_gc(as_index_tree* tree);

as_index_tree* as_index_tree_create(as_index_tree_shared* shared, uint8_t id, as_index_tree_done_fn cb, void* udata);
as_index_tree* as_index_tree_resume(as_index_tree_shared* shared, as_treex* xmem_trees, uint32_t pid, as_index_tree_done_fn cb, void* udata);
void as_index_tree_block(as_index_tree* tree);
void as_index_tree_reserve(as_index_tree* tree);
void as_index_tree_release(as_namespace* ns, as_index_tree* tree);
uint64_t as_index_tree_size(as_index_tree* tree);

typedef bool (*as_index_reduce_fn) (as_index_ref* value, void* udata);

bool as_index_reduce(as_index_tree* tree, as_index_reduce_fn cb, void* udata);
bool as_index_reduce_from(as_index_tree* tree, const cf_digest* keyd, as_index_reduce_fn cb, void* udata);

int as_index_get_vlock(as_index_tree* tree, const cf_digest* keyd, as_index_ref* index_ref);
int as_index_get_insert_vlock(as_index_tree* tree, const cf_digest* keyd, as_index_ref* index_ref);
void as_index_delete(as_index_tree* tree, const cf_digest* keyd);

// Used by queries when reserving arena refs.

static inline cf_mutex*
as_index_olock_from_keyd(as_index_tree* tree, const cf_digest* keyd)
{
	// Note - assumes 256 locks per partition!

	// Get the 8 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	uint32_t lock_i = ((uint32_t)keyd->digest[1] & 0xF0) |
			((uint32_t)keyd->digest[2] >> 4);

	return &((tree_locks(tree) + lock_i))->lock;
}

static inline cf_mutex*
as_index_rlock_from_keyd(as_index_tree* tree, const cf_digest* keyd)
{
	// Note - assumes 256 locks per partition!

	// Get the 8 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	uint32_t lock_i = ((uint32_t)keyd->digest[1] & 0xF0) |
			((uint32_t)keyd->digest[2] >> 4);

	return &((tree_locks(tree) + lock_i))->reduce_lock;
}

// Used by as_index and set_index.

typedef enum {
	BLACK = 0,
	RED = 1
} rb_tree_color;

#define SENTINEL_H 0

typedef struct as_index_ph_s {
	as_index* r;
	cf_arenax_handle r_h;
} as_index_ph;

typedef struct as_index_ph_array_s {
	bool is_stack;
	uint32_t capacity;
	uint32_t n_used;
	as_index_ph* phs;
} as_index_ph_array;

#define MAX_STACK_PHS (16 * 1024) // TODO - go bigger? Warn if we grow array?

void as_index_grow_ph_array(as_index_ph_array* ph_a);


//------------------------------------------------
// Private API - for enterprise separation only.
//

// Container for sprig-level function parameters.
typedef struct as_index_sprig_s {
	as_index_value_destructor destructor;
	void* destructor_udata;

	cf_arenax* arena;

	as_lock_pair* pair;
	as_sprig* sprig;
	cf_arenax_puddle* puddle;
} as_index_sprig;

bool as_index_sprig_reduce_no_rc(as_index_sprig* isprig, const cf_digest* keyd, as_index_reduce_fn cb, void* udata);

int as_index_sprig_get_vlock(as_index_sprig* isprig, const cf_digest* keyd, as_index_ref* index_ref);
int as_index_sprig_delete(as_index_sprig* isprig, const cf_digest* keyd);

static inline uint32_t
as_index_sprig_i_from_keyd(as_index_tree* tree, const cf_digest* keyd)
{
	// Get the 28 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the (12 bit) partition-ID
	// from the digest.
	uint32_t bits = (((uint32_t)keyd->digest[1] & 0xF0) << 20) |
			((uint32_t)keyd->digest[2] << 16) |
			((uint32_t)keyd->digest[3] << 8) |
			(uint32_t)keyd->digest[4];

	return bits >> tree->shared->sprigs_shift;
}

static inline void
as_index_sprig_from_keyd(as_index_tree* tree, as_index_sprig* isprig,
		const cf_digest* keyd)
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

#define RESOLVE(__h) ((as_index*)cf_arenax_resolve(isprig->arena, __h))
