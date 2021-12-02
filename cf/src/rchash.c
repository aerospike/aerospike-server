/*
 * rchash.c
 *
 * Copyright (C) 2018-2020 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "rchash.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"

#include "cf_mutex.h"
#include "log.h"


//==========================================================
// Typedefs & constants.
//

typedef struct cf_rchash_ele_s {
	struct cf_rchash_ele_s* next;
	void* object; // this is a reference counted object
	uint8_t key[];
} cf_rchash_ele;


//==========================================================
// Forward declarations.
//

// Generic utilities.
static inline void cf_rchash_destroy_elements(cf_rchash* h);
static inline uint32_t cf_rchash_calculate_hash(cf_rchash* h, const void* key);
static inline cf_mutex* cf_rchash_lock(cf_rchash* h, uint32_t i);
static inline void cf_rchash_unlock(cf_mutex* l);
static inline cf_rchash_ele* cf_rchash_get_bucket(cf_rchash* h, uint32_t i);
static inline void cf_rchash_fill_element(cf_rchash_ele* e, cf_rchash* h, const void* key, void* object);
static inline void cf_rchash_size_incr(cf_rchash* h);
static inline void cf_rchash_size_decr(cf_rchash* h);
static inline void cf_rchash_release_object(cf_rchash* h, void* object);


//==========================================================
// Public API - useful hash functions.
//

// Interpret first 4 bytes of key as (host-ordered) uint32_t. (Note - caller is
// responsible for ensuring key size is at least 4 bytes.)
uint32_t
cf_rchash_fn_u32(const void* key)
{
	return *(const uint32_t*)key;
}

// Useful if key is a null-terminated string. (Note - since we use fixed-size
// keys, key must still be padded to correctly compare keys in a bucket.)
uint32_t
cf_rchash_fn_zstr(const void* key)
{
	return cf_hash_fnv32((const uint8_t*)key, strlen(key));
}


//==========================================================
// Public API.
//

cf_rchash*
cf_rchash_create(cf_rchash_hash_fn h_fn, cf_rchash_destructor_fn d_fn,
		uint32_t key_size, uint32_t n_buckets)
{
	cf_assert(h_fn != NULL && key_size != 0 && n_buckets != 0, CF_MISC,
			"bad param");

	cf_rchash* h = cf_malloc(sizeof(cf_rchash));

	h->h_fn = h_fn;
	h->d_fn = d_fn;
	h->key_size = key_size;
	h->n_buckets = n_buckets;
	h->n_elements = 0;
	h->table = cf_calloc(n_buckets, sizeof(cf_rchash_ele) + key_size);
	h->bucket_locks = cf_malloc(sizeof(cf_mutex) * n_buckets);

	for (uint32_t i = 0; i < n_buckets; i++) {
		cf_mutex_init(&h->bucket_locks[i]);
	}

	return h;
}

void
cf_rchash_destroy(cf_rchash* h)
{
	cf_assert(h != NULL, CF_MISC, "bad param");

	cf_rchash_destroy_elements(h);

	for (uint32_t i = 0; i < h->n_buckets; i++) {
		cf_mutex_destroy(&h->bucket_locks[i]);
	}

	cf_free(h->bucket_locks);
	cf_free(h->table);
	cf_free(h);
}

uint32_t
cf_rchash_get_size(const cf_rchash* h)
{
	cf_assert(h != NULL, CF_MISC, "bad param");

	// For now, not bothering with different methods per lock mode.
	return as_load_uint32(&h->n_elements);
}

// If key is not already in hash, insert it with specified rc_malloc'd object.
// If key is already in hash, replace (and release) existing object.
void
cf_rchash_put(cf_rchash* h, const void* key, void* object)
{
	cf_assert(h != NULL && key != NULL && object != NULL, CF_MISC, "bad param");

	uint32_t hash = cf_rchash_calculate_hash(h, key);
	cf_rchash_ele* e = cf_rchash_get_bucket(h, hash);

	cf_mutex* l = cf_rchash_lock(h, hash);

	// Most common case should be insert into empty bucket.
	if (e->object == NULL) {
		cf_rchash_fill_element(e, h, key, object);
		cf_rchash_unlock(l);
		return;
	}

	uint32_t key_size = h->key_size;
	cf_rchash_ele* e_head = e;

	while (e != NULL) {
		if (memcmp(e->key, key, key_size) != 0) {
			e = e->next;
			continue;
		}

		// In this case we're replacing the previous object with the new object.
		void* free_object = e->object;

		e->object = object;

		cf_rchash_unlock(l);
		cf_rchash_release_object(h, free_object);

		return;
	}

	e = (cf_rchash_ele*)cf_malloc(sizeof(cf_rchash_ele) + key_size);

	cf_rchash_fill_element(e, h, key, object);

	// Insert just after head.
	e->next = e_head->next;
	e_head->next = e;

	cf_rchash_unlock(l);
}

// Like cf_rchash_put(), but if key is already in hash, fail.
int
cf_rchash_put_unique(cf_rchash* h, const void* key, void* object)
{
	cf_assert(h != NULL && key != NULL && object != NULL, CF_MISC, "bad param");

	uint32_t hash = cf_rchash_calculate_hash(h, key);
	cf_rchash_ele* e = cf_rchash_get_bucket(h, hash);

	cf_mutex* l = cf_rchash_lock(h, hash);

	// Most common case should be insert into empty bucket.
	if (e->object == NULL) {
		cf_rchash_fill_element(e, h, key, object);
		cf_rchash_unlock(l);
		return CF_RCHASH_OK;
	}

	uint32_t key_size = h->key_size;
	cf_rchash_ele* e_head = e;

	// Check for uniqueness of key - if not unique, fail!
	while (e != NULL) {
		if (memcmp(e->key, key, key_size) == 0) {
			cf_rchash_unlock(l);
			return CF_RCHASH_ERR_FOUND;
		}

		e = e->next;
	}

	e = (cf_rchash_ele*)cf_malloc(sizeof(cf_rchash_ele) + key_size);

	cf_rchash_fill_element(e, h, key, object);

	// Insert just after head.
	e->next = e_head->next;
	e_head->next = e;

	cf_rchash_unlock(l);

	return CF_RCHASH_OK;
}

// If key is found, object is returned with extra ref-count. When finished with
// it, caller must always release the returned object, and must destroy and free
// the object if the ref-count hits 0 - i.e. caller should do the equivalent of
// cf_rchash_release_object().
//
// Or, caller may pass NULL object_r to use this method as an existence check.
int
cf_rchash_get(cf_rchash* h, const void* key, void** object_r)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");

	uint32_t hash = cf_rchash_calculate_hash(h, key);
	cf_rchash_ele* e = cf_rchash_get_bucket(h, hash);

	if (e->object == NULL) {
		return CF_RCHASH_ERR_NOT_FOUND;
	}

	cf_mutex* l = cf_rchash_lock(h, hash);

	uint32_t key_size = h->key_size;

	while (e != NULL && e->object != NULL) {
		if (memcmp(key, e->key, key_size) != 0) {
			e = e->next;
			continue;
		}

		if (object_r != NULL) {
			cf_rc_reserve(e->object);
			*object_r = e->object;
		}

		cf_rchash_unlock(l);

		return CF_RCHASH_OK;
	}

	cf_rchash_unlock(l);

	return CF_RCHASH_ERR_NOT_FOUND;
}

// Removes the key and object from the hash, releasing the "original" ref-count.
// If this causes the ref-count to hit 0, the object destructor is called and
// the object is freed.
int
cf_rchash_delete(cf_rchash* h, const void* key)
{
	// No check to verify the object.
	return cf_rchash_delete_object(h, key, NULL);
}

// Like cf_rchash_delete() but checks that object found matches that specified.
// Threads may race to delete and release the same object - they may be doing a
// typical get ... delete, release sequence, or a reduce that deletes. While
// ref-counts ensure only the *last* release destroys the object, the *first*
// delete removes the object from the hash. If a new object is then immediately
// inserted with the same key, other threads' deletes would mistakenly remove
// this new element from the hash if they do not verify the object.
int
cf_rchash_delete_object(cf_rchash* h, const void* key, void* object)
{
	cf_assert(h != NULL && key != NULL, CF_MISC, "bad param");

	uint32_t hash = cf_rchash_calculate_hash(h, key);
	cf_rchash_ele* e = cf_rchash_get_bucket(h, hash);

	if (e->object == NULL) {
		return CF_RCHASH_ERR_NOT_FOUND;
	}

	cf_mutex* l = cf_rchash_lock(h, hash);

	uint32_t key_size = h->key_size;
	cf_rchash_ele* e_prev = NULL;

	// Look for the element, remove and release if found.
	while (e != NULL && e->object != NULL) {
		if (memcmp(e->key, key, key_size) != 0) {
			e_prev = e;
			e = e->next;
			continue;
		}
		// else - found it, remove from hash and release outside lock...

		// ... unless it's the wrong object.
		if (object != NULL && object != e->object) {
			cf_rchash_unlock(l);
			return CF_RCHASH_ERR_NOT_FOUND;
		}

		// Save pointers to release & free.
		void* free_object = e->object;
		cf_rchash_ele* free_e = NULL;

		// If not at head, patch pointers and free element.
		if (e_prev != NULL) {
			e_prev->next = e->next;
			free_e = e;
		}
		// If at head with no next, empty head.
		else if (e->next == NULL) {
			e->object = NULL;
		}
		// If at head with a next, copy next into head and free next.
		else {
			free_e = e->next;
			memcpy(e, e->next, sizeof(cf_rchash_ele) + key_size);
		}

		cf_rchash_size_decr(h);
		cf_rchash_unlock(l);

		cf_rchash_release_object(h, free_object);

		if (free_e != NULL) {
			cf_free(free_e);
		}

		return CF_RCHASH_OK;
	}

	cf_rchash_unlock(l);

	return CF_RCHASH_ERR_NOT_FOUND;
}

// Call the given function (reduce_fn) for every element in the tree.
//
// The value returned by reduce_fn governs behavior as follows:
// - CF_RCHASH_OK - continue iterating
// - CF_RCHASH_REDUCE_DELETE - delete the current element, continue iterating
// - anything else (e.g. CF_RCHASH_ERR) - stop iterating and return reduce_fn's
//   returned value
//
// If deleting an element causes the object ref-count to hit 0, the object
// destructor is called and the object is freed.
int
cf_rchash_reduce(cf_rchash* h, cf_rchash_reduce_fn reduce_fn, void* udata)
{
	cf_assert(h != NULL && reduce_fn != NULL, CF_MISC, "bad param");

	uint32_t key_size = h->key_size;

	for (uint32_t i = 0; i < h->n_buckets; i++) {
		cf_rchash_ele* e = cf_rchash_get_bucket(h, i);

		if (e->object == NULL) {
			continue;
		}

		cf_mutex* l = cf_rchash_lock(h, i);

		cf_rchash_ele* e_prev = NULL;

		while (e != NULL && e->object != NULL) {
			int rv = reduce_fn(e->key, e->object, udata);

			if (rv == CF_RCHASH_OK) {
				// Caller says keep going - most common case.

				e_prev = e;
				e = e->next;
			}
			else if (rv == CF_RCHASH_REDUCE_DELETE) {
				// Caller says delete this element and keep going.

				cf_rchash_release_object(h, e->object);
				cf_rchash_size_decr(h);

				// If not at head, patch pointers and free element.
				if (e_prev != NULL) {
					e_prev->next = e->next;
					cf_free(e);
					e = e_prev->next;
				}
				// If at head with no next, empty head.
				else if (e->next == NULL) {
					e->object = NULL;
				}
				// If at head with a next, copy next into head and free next.
				else {
					cf_rchash_ele* free_e = e->next;

					memcpy(e, e->next, sizeof(cf_rchash_ele) + key_size);
					cf_free(free_e);
				}
			}
			else {
				// Caller says stop iterating.
				cf_rchash_unlock(l);
				return rv;
			}
		}

		cf_rchash_unlock(l);
	}

	return CF_RCHASH_OK;
}


//==========================================================
// Local helpers - generic utilities.
//

static inline void
cf_rchash_destroy_elements(cf_rchash* h)
{
	for (uint32_t i = 0; i < h->n_buckets; i++) {
		cf_rchash_ele* e = cf_rchash_get_bucket(h, i);

		if (e->object == NULL) {
			continue;
		}

		cf_rchash_release_object(h, e->object);
		e = e->next; // skip the first, it's in place

		while (e != NULL) {
			cf_rchash_ele* temp = e->next;

			cf_rchash_release_object(h, e->object);
			cf_free(e);
			e = temp;
		}
	}
}

static inline uint32_t
cf_rchash_calculate_hash(cf_rchash* h, const void* key)
{
	return h->h_fn(key) % h->n_buckets;
}

static inline cf_mutex*
cf_rchash_lock(cf_rchash* h, uint32_t i)
{
	cf_mutex* l = &h->bucket_locks[i];

	cf_mutex_lock(l);

	return l;
}

static inline void
cf_rchash_unlock(cf_mutex* l)
{
	cf_mutex_unlock(l);
}

static inline cf_rchash_ele*
cf_rchash_get_bucket(cf_rchash* h, uint32_t i)
{
	return (cf_rchash_ele*)((uint8_t*)h->table +
			((sizeof(cf_rchash_ele) + h->key_size) * i));
}

static inline void
cf_rchash_fill_element(cf_rchash_ele* e, cf_rchash* h, const void* key,
		void* object)
{
	memcpy(e->key, key, h->key_size);
	e->object = object;
	cf_rchash_size_incr(h);
}

static inline void
cf_rchash_size_incr(cf_rchash* h)
{
	// For now, not bothering with different methods per lock mode.
	as_incr_int32(&h->n_elements);
}

static inline void
cf_rchash_size_decr(cf_rchash* h)
{
	// For now, not bothering with different methods per lock mode.
	as_decr_int32(&h->n_elements);
}

static inline void
cf_rchash_release_object(cf_rchash* h, void* object)
{
	if (cf_rc_release(object) == 0) {
		if (h->d_fn != NULL) {
			(h->d_fn)(object);
		}

		cf_rc_free(object);
	}
}
