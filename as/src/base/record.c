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
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

void record_replace_failed(as_remote_record *rr, as_index_ref* r_ref, as_storage_rd* rd, bool is_create);

int record_apply_dim_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int record_apply_dim(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);
int record_apply_ssd_single_bin(as_remote_record *rr, as_storage_rd *rd, bool *is_delete);
int record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex, bool *is_delete);

void unwind_dim_single_bin(as_bin* old_bin, as_bin* new_bin);


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
	as_storage_destroy_record(ns, r);

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
as_record_finalize_key(as_record *r, const as_namespace *ns, const uint8_t *key,
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

// If remote record is better than local record, replace local with remote.
int
as_record_replace_if_better(as_remote_record *rr, bool skip_sindex)
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

	if (rr->via == VIA_REPLICATION) {
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

	rd.pickle = rr->pickle;
	rd.pickle_sz = rr->pickle_sz;
	rd.orig_pickle_sz = as_flat_orig_pickle_size(rr, rd.pickle_sz);

	// Note - deal with key after reading existing record (if such), in case
	// we're dropping the key.

	// Save for XDR submit.
	uint64_t prev_lut = r->last_update_time;

	// Split according to configuration to replace local record.
	bool is_delete = false;

	if (rr->via == VIA_REPLICATION) {
		rd.which_current_swb = SWB_PROLE;
	}
	else if (rr->via == VIA_MIGRATION) {
		rd.which_current_swb = SWB_UNCACHED;
	}
	// else - dup-res goes in SWB_MASTER.

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

	record_replaced(r, rr);

	// Save for XDR submit outside record lock.
	as_xdr_submit_info submit_info;

	as_xdr_get_submit_info(r, prev_lut, &submit_info);

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	if (rr->via == VIA_REPLICATION) {
		as_xdr_submit(ns, &submit_info);
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
		cf_warning(AS_RECORD, "{%s} record replace: single-bin got %u bins %pD", ns->name, n_new_bins, rr->keyd);
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
		cf_warning(AS_RECORD, "{%s} record replace: failed unpickle bin %pD", ns->name, rr->keyd);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return -result;
	}

	// Won't use to flatten, but needed to know if bins are in use. Amazingly,
	// rd->n_bins 0 ok adjusting memory stats. Also, rd->bins already filled.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	replace_index_metadata(rr, r);

	// Write the record to storage.
	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning(AS_RECORD, "{%s} record replace: failed write %pD", ns->name, rr->keyd);
		unwind_index_metadata(&old_metadata, r);
		unwind_dim_single_bin(&old_bin, rd->bins);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_metadata);

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
			cf_warning(AS_RECORD, "{%s} record replace: failed unpickle bins %pD", ns->name, rr->keyd);
			destroy_stack_bins(new_bins, n_new_bins);
			return -result;
		}
	}

	// Won't use to flatten, but needed for memory stats, bins in use, etc.
	rd->n_bins = n_new_bins;
	rd->bins = new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	replace_index_metadata(rr, r);

	// Write the record to storage.
	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning(AS_RECORD, "{%s} record replace: failed write %pD", ns->name, rr->keyd);
		unwind_index_metadata(&old_metadata, r);
		destroy_stack_bins(new_bins, n_new_bins);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_metadata);

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
	as_namespace* ns = rr->rsv->ns;
	as_record* r = rd->r;

	uint16_t n_new_bins = rr->n_bins;

	if (n_new_bins > 1) {
		cf_warning(AS_RECORD, "{%s} record replace: single-bin got %u bins %pD", ns->name, n_new_bins, rr->keyd);
		return AS_ERR_UNKNOWN;
	}

	// Won't use to flatten, but needed to know if bins are in use.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	replace_index_metadata(rr, r);

	// Write the record to storage.
	int result = as_storage_record_write(rd);

	if (result < 0) {
		cf_warning(AS_RECORD, "{%s} record replace: failed write %pD", ns->name, rr->keyd);
		unwind_index_metadata(&old_metadata, r);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_metadata);

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, ns, rr->key, rr->key_size);

	*is_delete = n_new_bins == 0;

	return AS_OK;
}

int
record_apply_ssd(as_remote_record *rr, as_storage_rd *rd, bool skip_sindex,
		bool *is_delete)
{
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
			cf_warning(AS_RECORD, "{%s} record replace: failed load n-bins %pD", ns->name, rr->keyd);
			return -result;
		}

		n_old_bins = rd->n_bins;
		old_bins = alloca(n_old_bins * sizeof(as_bin));

		if ((result = as_storage_rd_load_bins(rd, old_bins)) < 0) {
			cf_warning(AS_RECORD, "{%s} record replace: failed load bins %pD", ns->name, rr->keyd);
			return -result;
		}

		// Won't use to flatten.
		rd->bins = NULL;

		if (n_new_bins != 0) {
			new_bins = alloca(n_new_bins * sizeof(as_bin));
			memset(new_bins, 0, n_new_bins * sizeof(as_bin));

			if ((result = as_flat_unpack_remote_bins(rr, new_bins)) != 0) {
				cf_warning(AS_RECORD, "{%s} record replace: failed unpickle bins %pD", ns->name, rr->keyd);
				return -result;
			}
		}
	}

	// Won't use to flatten, but needed to know if bins are in use.
	rd->n_bins = n_new_bins;

	// Apply changes to metadata in as_index needed for and writing.
	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	replace_index_metadata(rr, r);

	// Write the record to storage.
	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning(AS_RECORD, "{%s} record replace: failed write %pD", ns->name, rr->keyd);
		unwind_index_metadata(&old_metadata, r);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_metadata);

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

void
unwind_dim_single_bin(as_bin* old_bin, as_bin* new_bin)
{
	if (as_bin_inuse(new_bin)) {
		as_bin_particle_destroy(new_bin, true);
	}

	as_single_bin_copy(new_bin, old_bin);
}
