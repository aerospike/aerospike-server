/*
 * record.c
 *
 * Copyright (C) 2012-2021 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/exchange.h" // for compatibility-id only
#include "sindex/gc.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

static void record_replace_failed(as_remote_record* rr, as_index_ref* r_ref, as_storage_rd* rd);


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


//==========================================================
// Public API - record lock lifecycle.
//

// Returns:
//  1 - created new record
//  0 - found existing record
// -1 - failure - could not allocate arena stage
int
as_record_get_create(as_index_tree* tree, const cf_digest* keyd,
		as_index_ref* r_ref, as_namespace* ns)
{
	int rv = as_index_get_insert_vlock(tree, keyd, r_ref);

	if (rv == 1) {
		as_incr_uint64(&ns->n_objects);
	}

	return rv;
}

// Returns:
//  0 - found
// -1 - not found
int
as_record_get(as_index_tree* tree, const cf_digest* keyd, as_index_ref* r_ref)
{
	return as_index_get_vlock(tree, keyd, r_ref);
}

// Done with record - unlock. If record was removed from tree and is not
// reserved (by reduce), destroy record and free arena element.
void
as_record_done(as_index_ref* r_ref, as_namespace* ns)
{
	as_record* r = r_ref->r;

	if (! as_index_is_valid_record(r)) {
		if (r->rc == 0) {
			cf_assert(r->in_sindex == 0, AS_RECORD, "bad in_sindex bit");

			as_record_destroy(r, ns);

			mrt_free_orig(ns->arena, r, r_ref->puddle);
			cf_arenax_free(ns->arena, r_ref->r_h, r_ref->puddle);
		}
		else if (r->in_sindex == 1 && r->rc == 1) {
			as_sindex_gc_record(ns, r_ref);
		}
	}

	cf_mutex_unlock(r_ref->olock);
}


//==========================================================
// Public API - record lifecycle utilities.
//

// TODO - inline this, if/when we unravel header files.
uint32_t
as_record_stored_size(const as_record* r)
{
	return N_RBLOCKS_TO_SIZE(r->n_rblocks);
}

// TODO - inline this, if/when we unravel header files.
bool
as_record_is_expired(const as_record* r)
{
	return r->void_time != 0 && r->void_time < as_record_void_time_get();
}

// Called when writes encounter a "doomed" record, to delete the doomed record
// and create a new one in place without giving up the record lock.
void
as_record_rescue(as_index_ref* r_ref, as_namespace* ns)
{
	remove_from_sindex(ns, r_ref);
	as_record_destroy(r_ref->r, ns);
	as_index_clear_record_info(r_ref->r);
	as_incr_uint64(&ns->n_objects);
}

// Called only after last reference is released. Called by as_record_done(),
// also given to index trees to be called when tree releases record reference.
void
as_record_destroy(as_record* r, as_namespace* ns)
{
	as_record_drop_stats(r, ns);

	// Dereference record's storage used-size.
	as_storage_destroy_record(ns, r);
}

// Note - this is not called on the master write (or durable delete) path, where
// keys are stored but never dropped. Only a UDF will drop a key on master.
void
as_record_finalize_key(as_record* r, const uint8_t* key, uint32_t key_size)
{
	// If a key wasn't stored, and we got one, accommodate it.
	if (r->key_stored == 0) {
		if (key != NULL) {
			r->key_stored = 1;
		}
	}
	// If a key was stored, but we didn't get one, remove the key.
	else if (key == NULL) {
		r->key_stored = 0;
	}
}


//==========================================================
// Public API - pickled record utilities.
//

// If remote record is better than local record, replace local with remote.
int
as_record_replace_if_better(as_remote_record* rr)
{
	as_namespace* ns = rr->rsv->ns;

	as_index_tree* tree = rr->rsv->tree;
	as_index_ref r_ref;
	int rv = as_record_get_create(tree, rr->keyd, &r_ref, ns);

	if (rv < 0) {
		return AS_ERR_OUT_OF_SPACE;
	}

	bool is_create = rv == 1; // also equivalent to r->generation == 0
	as_index* r = r_ref.r;

	int result;

	conflict_resolution_pol policy = ns->conflict_resolution_policy;

	if (rr->via == VIA_REPLICATION) {
		bool from_replica;

		if ((result = as_partition_check_source(ns, rr->rsv->p, rr->regime,
				rr->src, &from_replica)) != AS_OK) {
			record_replace_failed(rr, &r_ref, NULL);
			return result;
		}

		repl_write_init_repl_state(rr, from_replica);
		policy = repl_write_conflict_resolution_policy(ns);
	}

	if (! is_create && record_replace_check(r, ns) < 0) {
		record_replace_failed(rr, &r_ref, NULL);
		return AS_ERR_FORBIDDEN;
	}

	// If local record is better, no-op or fail.
	if (! is_create && (result = as_record_resolve_conflict(policy,
			r->generation, r->last_update_time, (uint16_t)rr->generation,
			rr->last_update_time)) <= 0) {
		if (rr->via != VIA_REPLICATION || result < 0) {
			record_replace_failed(rr, &r_ref, NULL);
			return result == 0 ? AS_ERR_RECORD_EXISTS : AS_ERR_GENERATION;
		}
		// else - replica write, result == 0 - submit to XDR in case migration
		// (which does not submit to XDR) had passed the replica write.

		// Save for XDR submit outside record lock.
		as_xdr_submit_info submit_info;

		as_xdr_get_submit_info(r, r->last_update_time, &submit_info);
		record_replace_failed(rr, &r_ref, NULL);
		as_xdr_submit(ns, &submit_info);

		return AS_ERR_RECORD_EXISTS;
	}
	// else - remote winner - apply it.

	// If creating record, write set-ID into index.
	if (is_create) {
		if (rr->set_name != NULL && (result = as_index_set_set_w_len(r, ns,
				rr->set_name, rr->set_name_len, false)) != 0) {
			record_replace_failed(rr, &r_ref, NULL);
			return result;
		}

		r->last_update_time = rr->last_update_time; // just for truncate check

		// Don't write record if it would be truncated.
		if (as_truncate_record_is_truncated(r, ns)) {
			record_replace_failed(rr, &r_ref, NULL);
			return AS_OK;
		}

		r->last_update_time = 0; // restore after check
	}
	// else - not bothering to check that sets match.

	if (rr->set_name_len != 0 && is_mrt_setless_tombstone(ns, r)) {
		if ((result = as_record_fix_setless_tombstone(r, ns, rr->set_name, 
				rr->set_name_len, false)) != 0) {
			record_replace_failed(rr, &r_ref, NULL);
			return result;
		}
	}

	// TODO - remove in "six months".
	// Note - including AS_STORAGE_ENGINE_MEMORY now: it needs exact sizes since
	// it uses an end mark, and even though old memory pickles would not have
	// been padded, let's cover switching from SSD to MEMORY during upgrade.
	if (rr->via != VIA_REPLICATION &&
			ns->storage_type != AS_STORAGE_ENGINE_PMEM &&
			as_exchange_min_compatibility_id() < 11) {
		if (! as_flat_fix_padded_rr(rr)) {
			record_replace_failed(rr, &r_ref, NULL);
			return AS_OK;
		}
	}

	if ((result = mrt_apply_original(rr, &r_ref)) != 0) {
		record_replace_failed(rr, &r_ref, NULL);
		return result;
	}

	is_create = r->generation == 0; // may be changed by mrt_apply_original()

	// Note - if we fail the final apply below, we may unwind a dirty record to
	// nothing, if above sets generation 0. (MRT create finds local record it
	// beats and replaces in two steps.) This is ok.

	as_storage_rd rd;

	if (is_create) {
		as_storage_record_create(ns, r, &rd);
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	// Add the MRT id, as appropriate.
	set_mrt_id(&rd, rr->mrt_id);

	rd.pickle = rr->pickle;
	rd.pickle_sz = (uint32_t)rr->pickle_sz;
	rd.orig_pickle_sz = as_flat_orig_pickle_size(rr, rd.pickle_sz);

	// Note - deal with key after reading existing record (if such), in case
	// we're dropping the key.

	// Save for XDR submit.
	uint64_t prev_lut = r->last_update_time;

	if (is_rr_mrt(rr) || is_rr_mrt_monitor_write(rr, as_index_get_set_id(r))) {
		rd.which_current_swb = SWB_MRT_SHORT_LIVED;
	}
	else if (rr->via == VIA_REPLICATION) {
		rd.which_current_swb = SWB_PROLE;
	}
	else if (rr->via == VIA_MIGRATION) {
		rd.which_current_swb = SWB_UNCACHED;
	}
	// else - dup-res goes in SWB_MASTER.

	result = ! is_mrt_provisional(r) || is_rr_mrt(rr) ?
			as_record_apply(rr, &r_ref, &rd) :
			mrt_apply_roll(rr, &r_ref, &rd);

	if (result != 0) {
		record_replace_failed(rr, &r_ref, &rd);
		return result;
	}

	record_replaced(r, rr);

	// Save for XDR submit outside record lock.
	as_xdr_submit_info submit_info;

	if (rr->via == VIA_REPLICATION) {
		as_xdr_get_submit_info(r, prev_lut, &submit_info);
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	if (rr->via == VIA_REPLICATION) {
		as_xdr_submit(ns, &submit_info);
	}

	return AS_OK;
}

int
as_record_apply(as_remote_record* rr, as_index_ref* r_ref, as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	bool do_indexes = rr->mrt_id == 0;
	bool set_has_si = false;
	bool si_needs_bins = false;

	if (do_indexes) {
		set_has_si = set_has_sindex(r, ns);
		si_needs_bins = set_has_si && r->in_sindex == 1;
	}

	int result;
	as_bin old_bins[si_needs_bins ? RECORD_MAX_BINS : 0];

	if (si_needs_bins) {
		// TODO - don't need to load a bin cemetery for sindex - optimize?
		if ((result = as_storage_rd_load_bins(rd, old_bins)) < 0) {
			cf_warning(AS_RECORD, "{%s} record replace: failed load bins %pD", ns->name, rr->keyd);
			return -result;
		}
	}

	uint16_t n_new_bins = rr->n_bins;
	as_bin new_bins[set_has_si ? n_new_bins : 0];

	if (set_has_si && n_new_bins != 0 &&
			(result = as_flat_unpack_remote_bins(rr, new_bins)) != 0) {
		cf_warning(AS_RECORD, "{%s} record replace: failed unpickle bins %pD", ns->name, rr->keyd);
		return -result;
	}

	// Apply changes to metadata in as_index needed for and writing.
	as_record old_r = *r;

	replace_index_metadata(rr, r);

	// Write the record to storage. Note - here the pickle is directly stored -
	// we will not use rd->bins and rd->n_bins at all to write.
	if ((result = as_storage_record_write(rd)) < 0) {
		cf_detail(AS_RECORD, "{%s} record replace: failed write %pD", ns->name, rr->keyd);
		unwind_index_metadata(&old_r, r);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_r);

	// Success - adjust set index and sindex.
	if (do_indexes) {
		as_record_transition_set_index(rr->rsv->tree, r_ref, ns, n_new_bins,
				&old_r);

		if (set_has_si) {
			update_sindex(ns, r_ref, rd->bins, rd->n_bins, new_bins,
					n_new_bins);
		}
		else {
			// Sindex drop will leave in_sindex bit. Good opportunity to clear.
			as_index_clear_in_sindex(r);
		}
	}

	finish_replace_mrt(rd, &old_r, r_ref->puddle);

	// Now ok to store or drop key, as determined by message.
	as_record_finalize_key(r, rr->key, rr->key_size);

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

static void
record_replace_failed(as_remote_record* rr, as_index_ref* r_ref,
		as_storage_rd* rd)
{
	if (rd != NULL) {
		as_storage_record_close(rd);
	}

	if (r_ref->r->generation == 0) { // was created
		as_index_delete(rr->rsv->tree, rr->keyd);
	}

	as_record_done(r_ref, rr->rsv->ns);
}
