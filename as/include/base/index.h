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
	cf_atomic32 rc;

	// offset: 4
	cf_digest keyd;

	// offset: 24
	uint64_t right_h: 40;
	uint64_t left_h: 40;

	// offset: 34
	// Don't use the free bits here for record info - this is accessed outside
	// the record lock.
	uint16_t color: 1;
	uint16_t unused_but_unsafe: 15;

	// Everything below here is used under the record lock.

	// offset: 36
	uint32_t tombstone: 1;
	uint32_t cenotaph: 1;
	uint32_t void_time: 30;

	// offset: 40
	uint64_t last_update_time: 40;
	uint64_t generation: 16;

	// offset: 47
	// Used by the storage engines.
	uint64_t rblock_id: 34;		// can address 2^34 * 128b = 2Tb drive
	uint64_t n_rblocks: 14;		// is enough for 1Mb/128b = 8K rblocks
	uint64_t file_id: 6;		// can spec 2^6 = 64 drives

	uint64_t set_id_bits: 10;	// do not use directly, used for set-ID

	// offset: 55
	// In single-bin mode for data-in-memory namespaces, this offset is cast to
	// an as_bin, but only 4 bits get used (for the iparticle state). The other
	// 4 bits are used for replication state and index flags.
	uint8_t repl_state: 2;
	uint8_t unused_flag: 1;
	uint8_t key_stored: 1;
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

#define AS_INDEX_SINGLE_BIN_OFFSET 55 // can't use offsetof() with bit fields


//==========================================================
// Accessor functions for bits in as_index.
//

// Size in bytes of as_index, currently the same for all namespaces.
static inline
uint32_t as_index_size_get(as_namespace *ns)
{
	return (uint32_t)sizeof(as_index);
}

// Fast way to clear the record portion of as_index.
// Note - relies on current layout and size of as_index!
static inline
void as_index_clear_record_info(as_index *index) {
	*(uint32_t*)((uint8_t*)index + 36) = 0;

	uint64_t *p_clear = (uint64_t*)((uint8_t*)index + 40);

	*p_clear++	= 0;
	*p_clear++	= 0;
	*p_clear	= 0;
}

// Generation 0 is never written, and generation plays no role in record
// destruction, so it works to flag both "half created" and deleted records.
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

static inline
bool as_index_has_set(const as_index *index) {
	return index->set_id_bits != 0;
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
int as_index_set_set(as_index *index, as_namespace *ns, const char *set_name,
		bool apply_restrictions) {
	return as_index_set_set_w_len(index, ns, set_name, strlen(set_name),
		apply_restrictions);
}

static inline
const char *as_index_get_set_name(as_index *index, as_namespace *ns) {
	// TODO - don't really need this check - remove?
	if (! as_index_has_set(index)) {
		return NULL;
	}

	return as_namespace_get_set_name(ns, as_index_get_set_id(index));
}


//==========================================================
// Handling as_index objects.
//

// Container for as_index pointer with lock and location.
struct as_index_ref_s {
	bool				skip_lock;
	as_index			*r;
	cf_arenax_handle	r_h;
	cf_mutex			*olock;
};


//==========================================================
// Index tree.
//

typedef struct as_index_tree_s {
	// Data common to all trees in a namespace.
	as_index_tree_shared	*shared;

	// Where we allocate from and free to. Left out of 'shared' since we may
	// later use multiple arenas per namespace.
	cf_arenax				*arena;

	// Variable length data, dependent on configuration.
	uint8_t					data[];
} as_index_tree;


//==========================================================
// as_index_tree variable length data components.
//

typedef struct as_lock_pair_s {
	// Note: reduce_lock's scope is always inside of lock's scope.
	cf_mutex lock;        // insert, delete vs. insert, delete, get
	cf_mutex reduce_lock; // insert, delete vs. reduce
} as_lock_pair;

typedef struct as_sprig_s {
	cf_arenax_handle	root_h;
	uint64_t			n_elements;
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


//------------------------------------------------
// as_index_tree public API.
//

void as_index_tree_gc_init();
int as_index_tree_gc_queue_size();

as_index_tree *as_index_tree_create(as_index_tree_shared *shared, cf_arenax *arena);
as_index_tree *as_index_tree_resume(as_index_tree_shared *shared, cf_arenax *arena, as_treex *treex);
void as_index_tree_shutdown(as_index_tree *tree, as_treex *treex);
int as_index_tree_release(as_index_tree *tree);
uint64_t as_index_tree_size(as_index_tree *tree);

typedef void (*as_index_reduce_fn) (as_index_ref *value, void *udata);

void as_index_reduce(as_index_tree *tree, as_index_reduce_fn cb, void *udata);
void as_index_reduce_partial(as_index_tree *tree, uint64_t sample_count, as_index_reduce_fn cb, void *udata);

void as_index_reduce_live(as_index_tree *tree, as_index_reduce_fn cb, void *udata);
void as_index_reduce_partial_live(as_index_tree *tree, uint64_t sample_count, as_index_reduce_fn cb, void *udata);

int as_index_exists(as_index_tree *tree, cf_digest *keyd);
int as_index_get_vlock(as_index_tree *tree, cf_digest *keyd, as_index_ref *index_ref);
int as_index_get_insert_vlock(as_index_tree *tree, cf_digest *keyd, as_index_ref *index_ref);
int as_index_delete(as_index_tree *tree, cf_digest *keyd);

#define as_index_reserve(_r) cf_atomic32_incr(&(_r->rc))
#define as_index_release(_r) cf_atomic32_decr(&(_r->rc))


//------------------------------------------------
// Private API - for enterprise separation only.
//

// Container for sprig-level function parameters.
typedef struct as_index_sprig_s {
	as_index_value_destructor destructor;
	void			*destructor_udata;

	cf_arenax		*arena;

	as_lock_pair	*pair;
	as_sprig		*sprig;
} as_index_sprig;

#define SENTINEL_H 0

#define RESOLVE_H(__h) ((as_index*)cf_arenax_resolve(isprig->arena, __h))

// Flag to indicate full index reduce.
#define AS_REDUCE_ALL (-1L)
