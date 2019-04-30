/*
 * record.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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

#include <alloca.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/truncate.h"
#include "base/xdr_serverside.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

#define STACK_PARTICLES_SIZE (1024 * 1024)


//==========================================================
// Forward declarations.
//

void record_replace_failed(as_remote_record *rr, as_index_ref* r_ref, as_storage_rd* rd, bool is_create);

int record_apply_dim_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int record_apply_dim(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);
int record_apply_ssd_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);

int old_record_apply_dim_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int old_record_apply_dim(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);
int old_record_apply_ssd_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int old_record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);

void update_index_metadata(as_remote_record *rr, index_metadata *old, as_record *r);
void unwind_index_metadata(const index_metadata *old, as_record *r);
void unwind_dim_single_bin(as_bin* old_bin, as_bin* new_bin);

int unpickle_bins(as_remote_record *rr, as_storage_rd *rd, cf_ll_buf *particles_llb);

void xdr_write_replica(as_remote_record *rr, bool is_delete, uint32_t set_id);


//==========================================================
// Inlines & macros.
//

static inline int
resolve_generation_direct(uint16_t left, uint16_t right)
{
	return left == right ? 0 : (right > left  ? 1 : -1);
}

static inline int
resolve_generation(uint16_t left, uint16_t right)
{
	return left == right ? 0 : (as_gen_less_than(left, right) ? 1 : -1);
}

// Assumes remote generation is not 0. (Local may be 0 if creating record.)
static inline bool
next_generation(uint16_t local, uint16_t remote, as_namespace* ns)
{
	local = plain_generation(local, ns);
	remote = plain_generation(remote, ns);

	return local == 0xFFFF ? remote == 1 : remote - local == 1;
}

// Quietly trim void-time. (Clock on remote node different?) TODO - best way?
static inline uint32_t
trim_void_time(uint32_t void_time)
{
	uint32_t max_void_time = as_record_void_time_get() + MAX_ALLOWED_TTL;

	return void_time > max_void_time ? max_void_time : void_time;
}


//==========================================================
// Public API - record lock lifecycle.
//

// Returns:
//  1 - created new record
//  0 - found existing record
// -1 - failure - could not allocate arena stage
int
as_record_get_create(as_index_tree *tree, const cf_digest *keyd,
		as_index_ref *r_ref, as_namespace *ns)
{
	int rv = as_index_get_insert_vlock(tree, keyd, r_ref);

	if (rv == 1) {
		cf_atomic64_incr(&ns->n_objects);
	}

	return rv;
}


// Returns:
//  0 - found
// -1 - not found
int
as_record_get(as_index_tree *tree, const cf_digest *keyd, as_index_ref *r_ref)
{
	return as_index_get_vlock(tree, keyd, r_ref);
}


// Done with record - unlock. If record was removed from tree and is not
// reserved (by reduce), destroy record and free arena element.
void
as_record_done(as_index_ref *r_ref, as_namespace *ns)
{
	as_record *r = r_ref->r;

	if (! as_index_is_valid_record(r) && r->rc == 0) {
		as_record_destroy(r, ns);
		cf_arenax_free(ns->arena, r_ref->r_h, r_ref->puddle);
	}

	cf_mutex_unlock(r_ref->olock);
}


//==========================================================
// Public API - record lifecycle utilities.
//

// Returns:
//  0 - found
// -1 - not found
// -2 - can't lock
int
as_record_exists(as_index_tree *tree, const cf_digest *keyd)
{
	return as_index_try_exists(tree, keyd);
}


// TODO - inline this, if/when we unravel header files.
bool
as_record_is_expired(const as_record *r)
{
	return r->void_time != 0 && r->void_time < as_record_void_time_get();
}


// Called when writes encounter a "doomed" record, to delete the doomed record
// and create a new one in place without giving up the record lock.
// FIXME - won't be able to "rescue" with future sindex method - will go away.
void
as_record_rescue(as_index_ref *r_ref, as_namespace *ns)
{
	record_delete_adjust_sindex(r_ref->r, ns);
	as_record_destroy(r_ref->r, ns);
	as_index_clear_record_info(r_ref->r);
	cf_atomic64_incr(&ns->n_objects);
}


// Called only after last reference is released. Called by as_record_done(),
// also given to index trees to be called when tree releases record reference.
void
as_record_destroy(as_record *r, as_namespace *ns)
{
	if (ns->storage_data_in_memory) {
		// Note - rd is a limited container here - not calling
		// as_storage_record_create(), _open(), _close().
		as_storage_rd rd;

		rd.r = r;
		rd.ns = ns;
		as_storage_rd_load_n_bins(&rd);
		as_storage_rd_load_bins(&rd, NULL);

		as_storage_record_drop_from_mem_stats(&rd);

		as_record_destroy_bins(&rd);

		if (! ns->single_bin) {
			as_record_free_bin_space(r);

			if (r->dim) {
				cf_free(r->dim); // frees the key
			}
		}
	}

	as_record_drop_stats(r, ns);

	// Dereference record's storage used-size.
	as_storage_record_destroy(ns, r);

	return;
}


// Called only if data-in-memory, and not single-bin.
void
as_record_free_bin_space(as_record *r)
{
	as_bin_space *bin_space = as_index_get_bin_space(r);

	if (bin_space) {
		cf_free((void*)bin_space);
		as_index_set_bin_space(r, NULL);
	}
}


// Destroy all particles in all bins.
void
as_record_destroy_bins(as_storage_rd *rd)
{
	as_record_destroy_bins_from(rd, 0);
}


// Destroy particles in specified bins.
void
as_record_destroy_bins_from(as_storage_rd *rd, uint16_t from)
{
	for (uint16_t i = from; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			return; // no more used bins - there are never unused bin gaps
		}

		as_bin_particle_destroy(b, rd->ns->storage_data_in_memory);
		as_bin_set_empty(b);
	}
}


// Note - this is not called on the master write (or durable delete) path, where
// keys are stored but never dropped. Only a UDF will drop a key on master.
void
as_record_finalize_key(as_record *r, as_namespace *ns, const uint8_t *key,
		uint32_t key_size)
{
	// If a key wasn't stored, and we got one, accommodate it.
	if (r->key_stored == 0) {
		if (key != NULL) {
			if (ns->storage_data_in_memory) {
				as_record_allocate_key(r, key, key_size);
			}

			r->key_stored = 1;
		}
	}
	// If a key was stored, but we didn't get one, remove the key.
	else if (key == NULL) {
		if (ns->storage_data_in_memory) {
			as_bin_space *bin_space = ((as_rec_space *)r->dim)->bin_space;

			cf_free(r->dim);
			r->dim = (void *)bin_space;
		}

		r->key_stored = 0;
	}
}


// Called only for data-in-memory multi-bin, with no key currently stored.
// Note - have to modify if/when other metadata joins key in as_rec_space.
void
as_record_allocate_key(as_record *r, const uint8_t *key, uint32_t key_size)
{
	as_rec_space *rec_space = (as_rec_space *)
			cf_malloc_ns(sizeof(as_rec_space) + key_size);

	rec_space->bin_space = (as_bin_space *)r->dim;
	rec_space->key_size = key_size;
	memcpy((void*)rec_space->key, (const void*)key, key_size);

	r->dim = (void*)rec_space;
}


//==========================================================
// Public API - pickled record utilities.
//

// TODO - old pickle - remove in "six months".
// Flatten record's bins into "pickle" format for fabric.
uint8_t *
as_record_pickle(as_storage_rd *rd, size_t *len_r)
{
	as_namespace *ns = rd->ns;

	uint32_t sz = 2; // always 2 bytes for number of bins
	uint16_t n_bins_in_use;

	for (n_bins_in_use = 0; n_bins_in_use < rd->n_bins; n_bins_in_use++) {
		as_bin *b = &rd->bins[n_bins_in_use];

		if (! as_bin_inuse(b)) {
			break;
		}

		sz += 1; // for bin name length
		sz += ns->single_bin ?
				0 : strlen(as_bin_get_name_from_id(ns, b->id)); // for bin name
		sz += 1; // was for version - currently not used

		sz += as_bin_particle_pickled_size(b);
	}

	uint8_t *pickle = cf_malloc(sz);
	uint8_t *buf = pickle;

	(*(uint16_t *)buf) = cf_swap_to_be16(n_bins_in_use); // number of bins
	buf += 2;

	for (uint16_t i = 0; i < n_bins_in_use; i++) {
		as_bin *b = &rd->bins[i];

		// Copy bin name, skipping a byte for name length.
		uint8_t name_len = (uint8_t)as_bin_memcpy_name(ns, buf + 1, b);

		*buf++ = name_len; // fill in bin name length
		buf += name_len; // skip past bin name
		*buf++ = 0; // was version - currently not used

		buf += as_bin_particle_to_pickled(b, buf);
	}

	*len_r = sz;

	return pickle;
}


// If remote record is better than local record, replace local with remote.
int
as_record_replace_if_better(as_remote_record *rr, bool is_repl_write,
		bool skip_sindex, bool do_xdr_write)
{
	as_namespace *ns = rr->rsv->ns;

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_RECORD, "{%s} record replace: drives full", ns->name);
		return AS_ERR_OUT_OF_SPACE;
	}

	CF_ALLOC_SET_NS_ARENA(ns);

	as_index_tree *tree = rr->rsv->tree;

	as_index_ref r_ref;
	int rv = as_record_get_create(tree, rr->keyd, &r_ref, ns);

	if (rv < 0) {
		return AS_ERR_OUT_OF_SPACE;
	}

	bool is_create = rv == 1;
	as_index *r = r_ref.r;

	int result;

	conflict_resolution_pol policy = ns->conflict_resolution_policy;

	if (is_repl_write) {
		bool from_replica;

		if ((result = as_partition_check_source(ns, rr->rsv->p, rr->src,
				&from_replica)) != AS_OK) {
			record_replace_failed(rr, &r_ref, NULL, is_create);
			return result;
		}

		repl_write_init_repl_state(rr, from_replica);
		policy = repl_write_conflict_resolution_policy(ns);
	}

	if (! is_create && record_replace_check(r, ns) < 0) {
		record_replace_failed(rr, &r_ref, NULL, is_create);
		return AS_ERR_FORBIDDEN;
	}

	// If local record is better, no-op or fail.
	if (! is_create && (result = as_record_resolve_conflict(policy,
			r->generation, r->last_update_time, (uint16_t)rr->generation,
			rr->last_update_time)) <= 0) {
		record_replace_failed(rr, &r_ref, NULL, is_create);
		return result == 0 ? AS_ERR_RECORD_EXISTS : AS_ERR_GENERATION;
	}
	// else - remote winner - apply it.

	// If creating record, write set-ID into index.
	if (is_create) {
		if (rr->set_name && (result = as_index_set_set_w_len(r, ns,
				rr->set_name, rr->set_name_len, false)) < 0) {
			record_replace_failed(rr, &r_ref, NULL, is_create);
			return -result;
		}

		r->last_update_time = rr->last_update_time;

		// Don't write record if it would be truncated.
		if (as_truncate_record_is_truncated(r, ns)) {
			record_replace_failed(rr, &r_ref, NULL, is_create);
			return AS_OK;
		}
	}
	// else - not bothering to check that sets match.

	as_storage_rd rd;

	if (is_create) {
		as_storage_record_create(ns, r, &rd);
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	// TODO - old pickle - remove condition in "six months".
	if (rr->is_old_pickle) {
		// Prepare to store set name, if there is one.
		rd.set_name = rr->set_name;
		rd.set_name_len = rr->set_name_len;
	}
	else {
		rd.pickle = rr->pickle;
		rd.pickle_sz = rr->pickle_sz;
		rd.orig_pickle_sz = as_flat_orig_pickle_size(rr, rd.pickle_sz);
	}

	// Note - deal with key after reading existing record (if such), in case
	// we're dropping the key.

	// Split according to configuration to replace local record.
	bool is_delete = false;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			result = record_apply_dim_single_bin(rr, &rd, &is_delete);
		}
		else {
			result = record_apply_dim(rr, &rd, skip_sindex, &is_delete);
		}
	}
	else {
		if (ns->single_bin) {
			result = record_apply_ssd_single_bin(rr, &rd, &is_delete);
		}
		else {
			result = record_apply_ssd(rr, &rd, skip_sindex, &is_delete);
		}
	}

	if (result != 0) {
		record_replace_failed(rr, &r_ref, &rd, is_create);
		return result;
	}

	uint16_t set_id = as_index_get_set_id(r); // save for XDR write

	record_replaced(r, rr);

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	if (do_xdr_write) {
		xdr_write_replica(rr, is_delete, set_id);
	}

	return AS_OK;
}


//==========================================================
// Public API - conflict resolution.
//

// Returns -1 if left wins, 1 if right wins, and 0 for tie.
int
as_record_resolve_conflict(conflict_resolution_pol policy, uint16_t left_gen,
		uint64_t left_lut, uint16_t right_gen, uint64_t right_lut)
{
	int result = 0;

	switch (policy) {
	case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION:
		// Doesn't use resolve_generation() - direct comparison gives much
		// better odds of picking the record with more history after a split
		// brain where one side starts the record from scratch.
		result = resolve_generation_direct(left_gen, right_gen);
		if (result == 0) {
			result = resolve_last_update_time(left_lut, right_lut);
		}
		break;
	case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME:
		result = resolve_last_update_time(left_lut, right_lut);
		if (result == 0) {
			result = resolve_generation(left_gen, right_gen);
		}
		break;
	case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_CP:
		result = record_resolve_conflict_cp(left_gen, left_lut, right_gen,
				right_lut);
		break;
	default:
		cf_crash(AS_RECORD, "invalid conflict resolution policy");
		break;
	}

	return result;
}


//==========================================================
// Local helpers.
//

void
record_replace_failed(as_remote_record *rr, as_index_ref* r_ref,
		as_storage_rd* rd, bool is_create)
{
	if (rd) {
		as_storage_record_close(rd);
	}

	if (is_create) {
		as_index_delete(rr->rsv->tree, rr->keyd);
	}

	as_record_done(r_ref, rr->rsv->ns);
}


int
record_apply_dim_single_bin(as_remote_record *rr, as_storage_rd *rd,
		bool *is_delete)
{
	// TODO - old pickle - remove in "six months".
	if (rr->is_old_pickle) {
		return old_record_apply_dim_single_bin(rr, rd, is_delete);
	}

	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	rd->n_bins = 1;

	// Set rd->bins!
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = 0;

	// TODO - as_storage_record_get_n_bytes_memory() could check bins in use.
	if (as_bin_inuse(rd->bins)) {
		memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}

	uint16_t n_new_bins = rr->n_bins;

	if (n_new_bins > 1) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: single-bin got %u bins ", ns->name, n_new_bins);
		return AS_ERR_UNKNOWN;
	}

	// Keep old bin for unwinding.
	as_bin old_bin;

	as_single_bin_copy(&old_bin, rd->bins);

	// No stack new bin - simpler to operate directly on bin embedded in index.
	as_bin_set_empty(rd->bins);

	int result;

	// Fill the new bins and particles.
	if (n_new_bins == 1 &&
			(result = as_flat_unpack_remote_bins(rr, rd->bins)) != 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bin ", ns->name);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return -result;
	}

	// Won't use to flatten, but needed to know if bins are in use. Amazingly,
	// rd->n_bins 0 ok adjusting memory stats. Also, rd->bins already filled.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return -result;
	}

	// Cleanup - destroy old bin, can't unwind after.
	as_bin_particle_destroy(&old_bin, true);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = n_new_bins == 0;

	return AS_OK;
}


int
record_apply_dim(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex,
		bool *is_delete)
{
	// TODO - old pickle - remove in "six months".
	if (rr->is_old_pickle) {
		return old_record_apply_dim(rr, rd, skip_sindex, is_delete);
	}

	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	// Set rd->n_bins!
	as_storage_rd_load_n_bins(rd);

	// Set rd->bins!
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	int result;

	// Keep old bins intact for sindex adjustment and unwinding.
	uint16_t n_old_bins = rd->n_bins;
	as_bin* old_bins = rd->bins;

	uint16_t n_new_bins = rr->n_bins;
	as_bin new_bins[n_new_bins];

	if (n_new_bins != 0) {
		memset(new_bins, 0, sizeof(new_bins));

		// Fill the new bins and particles.
		if ((result = as_flat_unpack_remote_bins(rr, new_bins)) != 0) {
			cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bins ", ns->name);
			destroy_stack_bins(new_bins, n_new_bins);
			return -result;
		}
	}

	// Won't use to flatten, but needed for memory stats, bins in use, etc.
	rd->n_bins = n_new_bins;
	rd->bins = new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		destroy_stack_bins(new_bins, n_new_bins);
		return -result;
	}

	// Success - adjust sindex, looking at old and new bins.
	if (! (skip_sindex &&
			next_generation(r->generation, (uint16_t)rr->generation, ns)) &&
					record_has_sindex(r, ns)) {
		write_sindex_update(ns, as_index_get_set_name(r, ns), rr->keyd,
				old_bins, n_old_bins, new_bins, n_new_bins);
	}

	// Cleanup - destroy relevant bins, can't unwind after.
	destroy_stack_bins(old_bins, n_old_bins);

	// Fill out new_bin_space.
	as_bin_space* new_bin_space = NULL;

	if (n_new_bins != 0) {
		new_bin_space = (as_bin_space*)
				cf_malloc_ns(sizeof(as_bin_space) + sizeof(new_bins));

		new_bin_space->n_bins = n_new_bins;
		memcpy((void*)new_bin_space->bins, new_bins, sizeof(new_bins));
	}

	// Swizzle the index element's as_bin_space pointer.
	as_bin_space* old_bin_space = as_index_get_bin_space(r);

	if (old_bin_space) {
		cf_free(old_bin_space);
	}

	as_index_set_bin_space(r, new_bin_space);

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rr->key, rr->key_size);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = n_new_bins == 0;

	return AS_OK;
}


int
record_apply_ssd_single_bin(as_remote_record *rr, as_storage_rd *rd,
		bool *is_delete)
{
	// TODO - old pickle - remove in "six months".
	if (rr->is_old_pickle) {
		return old_record_apply_ssd_single_bin(rr, rd, is_delete);
	}

	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	uint16_t n_new_bins = rr->n_bins;

	if (n_new_bins > 1) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: single-bin got %u bins ", ns->name, n_new_bins);
		return AS_ERR_UNKNOWN;
	}

	// Won't use to flatten, but needed to know if bins are in use.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Write the record to storage.
	int result = as_record_write_from_pickle(rd);

	if (result < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		return -result;
	}

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rr->key, rr->key_size);

	*is_delete = n_new_bins == 0;

	return AS_OK;
}


int
record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex,
		bool *is_delete)
{
	// TODO - old pickle - remove in "six months".
	if (rr->is_old_pickle) {
		return old_record_apply_ssd(rr, rd, skip_sindex, is_delete);
	}

	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	bool has_sindex = ! (skip_sindex &&
			next_generation(r->generation, (uint16_t)rr->generation, ns)) &&
					record_has_sindex(r, ns);

	int result;

	uint16_t n_old_bins = 0;
	as_bin *old_bins = NULL;

	uint16_t n_new_bins = rr->n_bins;
	as_bin *new_bins = NULL;

	if (has_sindex) {
		// TODO - separate function?
		if ((result = as_storage_rd_load_n_bins(rd)) < 0) {
			cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed load n-bins ", ns->name);
			return -result;
		}

		n_old_bins = rd->n_bins;
		old_bins = alloca(n_old_bins * sizeof(as_bin));

		if ((result = as_storage_rd_load_bins(rd, old_bins)) < 0) {
			cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed load bins ", ns->name);
			return -result;
		}

		// Won't use to flatten.
		rd->bins = NULL;

		if (n_new_bins != 0) {
			new_bins = alloca(n_new_bins * sizeof(as_bin));
			memset(new_bins, 0, n_new_bins * sizeof(as_bin));

			if ((result = as_flat_unpack_remote_bins(rr, new_bins)) != 0) {
				cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bins ", ns->name);
				return -result;
			}
		}
	}

	// Won't use to flatten, but needed to know if bins are in use.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		return -result;
	}

	// Success - adjust sindex, looking at old and new bins.
	if (has_sindex) {
		write_sindex_update(ns, as_index_get_set_name(r, ns), rr->keyd,
				old_bins, n_old_bins, new_bins, n_new_bins);
	}

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rr->key, rr->key_size);

	*is_delete = n_new_bins == 0;

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
int
old_record_apply_dim_single_bin(as_remote_record *rr, as_storage_rd *rd,
		bool *is_delete)
{
	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	rd->n_bins = 1;

	// Set rd->bins!
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = 0;

	// TODO - as_storage_record_get_n_bytes_memory() could check bins in use.
	if (as_bin_inuse(rd->bins)) {
		memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}

	uint16_t n_new_bins = cf_swap_from_be16(*(uint16_t *)rr->pickle);

	if (n_new_bins > 1) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: single-bin got %u bins ", ns->name, n_new_bins);
		return AS_ERR_UNKNOWN;
	}

	// Keep old bin intact for unwinding, clear record bin for incoming.
	as_bin old_bin;

	as_single_bin_copy(&old_bin, rd->bins);
	as_bin_set_empty(rd->bins);

	int result;

	// Fill the new bins and particles.
	if (n_new_bins == 1 &&
			(result = unpickle_bins(rr, rd, NULL)) != 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bin ", ns->name);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return result;
	}

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return -result;
	}

	// Cleanup - destroy old bin, can't unwind after.
	as_bin_particle_destroy(&old_bin, true);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = n_new_bins == 0;

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
int
old_record_apply_dim(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex,
		bool *is_delete)
{
	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	// Set rd->n_bins!
	as_storage_rd_load_n_bins(rd);

	// Set rd->bins!
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	// Keep old bins intact for sindex adjustment and unwinding.
	uint16_t n_old_bins = rd->n_bins;
	as_bin* old_bins = rd->bins;

	uint16_t n_new_bins = cf_swap_from_be16(*(uint16_t *)rr->pickle);
	as_bin new_bins[n_new_bins];

	memset(new_bins, 0, sizeof(new_bins));
	rd->n_bins = n_new_bins;
	rd->bins = new_bins;

	// Fill the new bins and particles.
	int result = unpickle_bins(rr, rd, NULL);

	if (result != 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bins ", ns->name);
		destroy_stack_bins(new_bins, n_new_bins);
		return result;
	}

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Prepare to store or drop key, as determined by message.
	rd->key = rr->key;
	rd->key_size = rr->key_size;

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		destroy_stack_bins(new_bins, n_new_bins);
		return -result;
	}

	// Success - adjust sindex, looking at old and new bins.
	if (! (skip_sindex &&
			next_generation(r->generation, (uint16_t)rr->generation, ns)) &&
					record_has_sindex(r, ns)) {
		write_sindex_update(ns, as_index_get_set_name(r, ns), rr->keyd,
				old_bins, n_old_bins, new_bins, n_new_bins);
	}

	// Cleanup - destroy relevant bins, can't unwind after.
	destroy_stack_bins(old_bins, n_old_bins);

	// Fill out new_bin_space.
	as_bin_space* new_bin_space = NULL;

	if (n_new_bins != 0) {
		new_bin_space = (as_bin_space*)
				cf_malloc_ns(sizeof(as_bin_space) + sizeof(new_bins));

		new_bin_space->n_bins = rd->n_bins;
		memcpy((void*)new_bin_space->bins, new_bins, sizeof(new_bins));
	}

	// Swizzle the index element's as_bin_space pointer.
	as_bin_space* old_bin_space = as_index_get_bin_space(r);

	if (old_bin_space) {
		cf_free(old_bin_space);
	}

	as_index_set_bin_space(r, new_bin_space);

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rd->key, rd->key_size);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = n_new_bins == 0;

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
int
old_record_apply_ssd_single_bin(as_remote_record *rr, as_storage_rd *rd,
		bool *is_delete)
{
	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	uint16_t n_new_bins = cf_swap_from_be16(*(uint16_t *)rr->pickle);

	if (n_new_bins > 1) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: single-bin got %u bins ", ns->name, n_new_bins);
		return AS_ERR_UNKNOWN;
	}

	as_bin stack_bin = { { 0 } };

	rd->n_bins = 1;
	rd->bins = &stack_bin;

	// Fill the new bin and particle.
	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	int result;

	if (n_new_bins == 1 &&
			(result = unpickle_bins(rr, rd, &particles_llb)) != 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bin ", ns->name);
		cf_ll_buf_free(&particles_llb);
		return result;
	}

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Prepare to store or drop key, as determined by message.
	rd->key = rr->key;
	rd->key_size = rr->key_size;

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		cf_ll_buf_free(&particles_llb);
		return -result;
	}

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rd->key, rd->key_size);

	*is_delete = n_new_bins == 0;

	cf_ll_buf_free(&particles_llb);

	return AS_OK;
}


// TODO - old pickle - remove in "six months".
int
old_record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex,
		bool *is_delete)
{
	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;
	bool has_sindex = ! (skip_sindex &&
			next_generation(r->generation, (uint16_t)rr->generation, ns)) &&
					record_has_sindex(r, ns);

	uint16_t n_old_bins = 0;
	int result;

	if (has_sindex) {
		// Set rd->n_bins!
		if ((result = as_storage_rd_load_n_bins(rd)) < 0) {
			cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed load n-bins ", ns->name);
			return -result;
		}

		n_old_bins = rd->n_bins;
	}

	as_bin old_bins[n_old_bins];

	if (has_sindex) {
		// Set rd->bins!
		if ((result = as_storage_rd_load_bins(rd, old_bins)) < 0) {
			cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed load bins ", ns->name);
			return -result;
		}
	}

	// Stack space for resulting record's bins.
	uint16_t n_new_bins = cf_swap_from_be16(*(uint16_t *)rr->pickle);
	as_bin new_bins[n_new_bins];

	memset(new_bins, 0, sizeof(new_bins));
	rd->n_bins = n_new_bins;
	rd->bins = new_bins;

	// Fill the new bins and particles.
	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	if ((result = unpickle_bins(rr, rd, &particles_llb)) != 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed unpickle bins ", ns->name);
		cf_ll_buf_free(&particles_llb);
		return result;
	}

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	update_index_metadata(rr, &old_metadata, r);

	// Prepare to store or drop key, as determined by message.
	rd->key = rr->key;
	rd->key_size = rr->key_size;

	// Write the record to storage.
	if ((result = as_record_write_from_pickle(rd)) < 0) {
		cf_warning_digest(AS_RECORD, rr->keyd, "{%s} record replace: failed write ", ns->name);
		unwind_index_metadata(&old_metadata, r);
		cf_ll_buf_free(&particles_llb);
		return -result;
	}

	// Success - adjust sindex, looking at old and new bins.
	if (has_sindex) {
		write_sindex_update(ns, as_index_get_set_name(r, ns), rr->keyd,
				old_bins, n_old_bins, new_bins, n_new_bins);
	}

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rd->key, rd->key_size);

	*is_delete = n_new_bins == 0;

	cf_ll_buf_free(&particles_llb);

	return AS_OK;
}


void
update_index_metadata(as_remote_record *rr, index_metadata *old, as_record *r)
{
	old->void_time = r->void_time;
	old->last_update_time = r->last_update_time;
	old->generation = r->generation;

	r->generation = (uint16_t)rr->generation;
	r->void_time = trim_void_time(rr->void_time);
	r->last_update_time = rr->last_update_time;
}


void
unwind_index_metadata(const index_metadata *old, as_record *r)
{
	r->void_time = old->void_time;
	r->last_update_time = old->last_update_time;
	r->generation = old->generation;
}


void
unwind_dim_single_bin(as_bin* old_bin, as_bin* new_bin)
{
	if (as_bin_inuse(new_bin)) {
		as_bin_particle_destroy(new_bin, true);
	}

	as_single_bin_copy(new_bin, old_bin);
}


// TODO - old pickle - remove in "six months".
int
unpickle_bins(as_remote_record *rr, as_storage_rd *rd, cf_ll_buf *particles_llb)
{
	as_namespace *ns = rd->ns;

	const uint8_t *end = rr->pickle + rr->pickle_sz;
	const uint8_t *buf = rr->pickle + 2;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (buf >= end) {
			cf_warning(AS_RECORD, "incomplete pickled record");
			return AS_ERR_UNKNOWN;
		}

		uint8_t name_sz = *buf++;
		const uint8_t *name = buf;

		buf += name_sz;
		buf++; // skipped byte was version

		if (buf > end) {
			cf_warning(AS_RECORD, "incomplete pickled record");
			return AS_ERR_UNKNOWN;
		}

		int result;
		as_bin *b = as_bin_create_from_buf(rd, name, name_sz, &result);

		if (! b) {
			return result;
		}

		if (ns->storage_data_in_memory) {
			if ((result = as_bin_particle_alloc_from_pickled(b,
					&buf, end)) < 0) {
				return -result;
			}
		}
		else {
			if ((result = as_bin_particle_stack_from_pickled(b, particles_llb,
					&buf, end)) < 0) {
				return -result;
			}
		}
	}

	if (buf != end) {
		cf_warning(AS_RECORD, "extra bytes on pickled record");
		return AS_ERR_UNKNOWN;
	}

	return AS_OK;
}


void
xdr_write_replica(as_remote_record *rr, bool is_delete, uint32_t set_id)
{
	uint16_t generation = (uint16_t)rr->generation;
	xdr_op_type op_type = XDR_OP_TYPE_WRITE;

	// Note - in this code path, only durable deletes get here.
	if (is_delete) {
		generation = 0;
		op_type = XDR_OP_TYPE_DURABLE_DELETE;
	}

	// Don't send an XDR delete if it's disallowed.
	if (is_delete && ! is_xdr_delete_shipping_enabled()) {
		// TODO - should we also not ship if there was no record here before?
		return;
	}

	xdr_write(rr->rsv->ns, rr->keyd, generation, rr->src, op_type, set_id,
			NULL);
}
