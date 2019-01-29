/*
 * udf_aerospike.c
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

#include "base/udf_aerospike.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <asm/byteorder.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_boolean.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_bytes.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "base/udf_record.h"
#include "base/xdr_serverside.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"


static int udf_aerospike_rec_remove(const as_aerospike *, const as_rec *);
/*
 * Internal Function: udf_aerospike_delbin
 *
 * Parameters:
 * 		r 		- udf_record to be manipulated
 * 		bname 	- name of the bin to be deleted
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function deletes the bin with the name
 * 		passed in as parameter. The as_bin_destroy function
 * 		which is called here, only frees the data and
 * 		the bin is marked as not in use. The bin can then be reused later.
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin destroy.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If delete from sindex fails, we do not handle it.
 */
static int
udf_aerospike_delbin(udf_record * urecord, const char * bname)
{
	as_storage_rd *rd = urecord->rd;
	as_namespace *ns = rd->ns;

	// Check that bname is not completely invalid
	if (bname == NULL || (ns->single_bin && bname[0] != 0) || (! ns->single_bin && bname[0] == 0)) {
		cf_warning(AS_UDF, "udf_aerospike_delbin: Invalid Parameters: [Invalid bin name supplied]... Fail");
		return -1;
	}

	// Check quality of bname -- check that it is proper length, then make sure
	// that the bin exists.
	if (strlen(bname) >= AS_BIN_NAME_MAX_SZ) {
		// Can't read bin if name too large.
		cf_warning(AS_UDF, "udf_aerospike_delbin: Invalid Parameters [bin name(%s) too big]... Fail", bname);
		return -1;
	}

	as_bin * b = as_bin_get(rd, bname);
	if ( !b ) {
		cf_debug(AS_UDF, "udf_aerospike_delbin: Invalid Operation [Bin name(%s) not found of delete]... Fail", bname);
		return -1;
	}

	const char * set_name = as_index_get_set_name(rd->r, ns);
	
	bool has_sindex = record_has_sindex(rd->r, ns);
	SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);
	as_sindex * si_arr[ns->sindex_cnt];
	int si_arr_index = 0;
	int sbins_populated  = 0;
	if (has_sindex) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, b->id, &si_arr[si_arr_index]);
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, sbins, AS_SINDEX_OP_DELETE);
	}

	int32_t i = as_bin_get_index(rd, bname);
	if (i != -1) {
		if (has_sindex) {
			if (sbins_populated > 0) {	
				urecord->tr->flags |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
				as_sindex_update_by_sbin(ns, as_index_get_set_name(rd->r, ns), sbins, sbins_populated, &rd->r->keyd);
			}
		}
		as_bin_destroy(rd, i);
	} else {
		cf_warning(AS_UDF, "udf_aerospike_delbin: Internal Error [Deleting non-existing bin %s]... Fail", bname);
	}

	if (has_sindex) {
		as_sindex_sbin_freeall(sbins, sbins_populated);
		as_sindex_release_arr(si_arr, si_arr_index);
	}

	return 0;
}
/*
 * Internal function: udf__aerospike_get_particle_buf
 *
 * Parameters:
 * 		r 		-- udf_record_bin for which particle buf is requested
 * 		type    -- bin type
 * 		pbytes  -- current space required
 *
 * Return value:
 * 		NULL on failure
 * 		valid buf pointer success
 *
 * Description:
 * 		The function find space on preallocated particle_data for requested size.
 * 		In case it is found it tries to allocate space for bin independently. 
 * 		Return back the pointer to the offset on preallocated particle_data or newly
 * 		allocated space.
 *
 * 		Return NULL if both fails
 *
 *      Note: ubin->particle_buf will be set if new per bin memory is allocated.
 *
 * 		Callers:
 * 		udf_aerospike_setbin
 */
uint8_t *
udf__aerospike_get_particle_buf(udf_record *urecord, udf_record_bin *ubin, uint32_t pbytes)
{
	if (pbytes > urecord->rd->ns->storage_write_block_size) {
		cf_warning(AS_UDF, "udf__aerospike_get_particle_buf: Invalid Operation [Bin %s data too big size=%u]... Fail", ubin->name, pbytes);
		return NULL;
	}

	uint32_t alloc_size = pbytes == 0 ? 0 : urecord->rd->ns->storage_write_block_size;
	uint8_t *buf = NULL;

	if (ubin->particle_buf) {
		buf = ubin->particle_buf;
	} else {
		// Disable dynamic shifting from the flat allocater to dynamic
		// allocation.
		if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
			buf = urecord->cur_particle_data;
			urecord->cur_particle_data += pbytes;
		} else if (alloc_size) {
			// If there is no space in preallocated buffer then go
			// ahead and allocate space per bin. This may happen
			// if user keeps doing lot of execute update exhausting
			// the buffer. After this point the record size check will
			// trip instead of at the code when bin value is set.
			ubin->particle_buf = cf_malloc(alloc_size);
			buf = ubin->particle_buf;
		}
	}
	return buf;
}
/*
 * Internal function: udf_aerospike_setbin
 *
 * Parameters:
 *      offset  -- offset of udf bin in updates array 
 * 		r 		-- udf_record to be manipulated
 * 		bname 	-- name of the bin to be deleted
 *		val		-- value to be updated with
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function sets the bin with the name
 * 		passed in as parameter to the value, passed as the third parameter.
 * 		Before updating the bin, it is checked if the value can fit in the storage
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin update.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If put in sindex fails, we do not handle it.
 *
 * 		TODO make sure anything goes into setbin only if the bin value is
 * 		          changed
 */
static int
udf_aerospike_setbin(udf_record * urecord, int offset, const char * bname, const as_val * val)
{
	as_storage_rd *rd = urecord->rd;
	as_namespace *ns = rd->ns;

	if (bname == NULL || (ns->single_bin && bname[0] != 0) || (! ns->single_bin && bname[0] == 0)) {
		cf_warning(AS_UDF, "udf_aerospike_setbin: Invalid Parameters: [Invalid bin name supplied]... Fail");
		return -1;
	}

	if (as_particle_type_from_asval(val) == AS_PARTICLE_TYPE_NULL) {
		cf_warning(AS_UDF, "udf_aerospike_setbin: [%s] called with unusable as_val", bname);
		return -3;
	}

	uint8_t type = as_val_type(val);

	as_bin * b = as_bin_get_or_create(rd, bname);

	if ( !b ) {
		cf_warning(AS_UDF, "udf_aerospike_setbin: Internal Error [Bin %s not found.. Possibly ran out of bins]... Fail", bname);
		return -1;
	}

	bool has_sindex = record_has_sindex(rd->r, ns);
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);
	as_sindex * si_arr[2 * ns->sindex_cnt];
	int sbins_populated = 0;
	int si_arr_index = 0;
	const char * set_name = as_index_get_set_name(rd->r, ns);

	if (has_sindex ) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, b->id, &si_arr[si_arr_index]);
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
	}

	// we know we are doing an update now, make sure there is particle data,
	// set to be 1 wblock size now @TODO!
	int ret = 0;

	cf_detail(AS_UDF, "udf_setbin: bin %s type %d ", bname, type );

	if (ns->storage_data_in_memory) {
		if (as_bin_particle_replace_from_asval(b, val) != 0) {
			cf_warning(AS_UDF, "udf_aerospike_setbin: [%s] failed to replace particle", bname);
			ret = -4;
		}
	}
	else {
		uint32_t size = as_particle_size_from_asval(val);
		uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], size);

		if (particle_buf) {
			as_bin_particle_stack_from_asval(b, particle_buf, val);
		}
		else {
			cf_warning(AS_UDF, "udf_aerospike_setbin: [%s] failed to get space for particle size %u", bname, size);
			ret = -4;
		}
	}

	// Update sindex if required
	if (has_sindex) {
		if (ret) {
			if (sbins_populated > 0) {
				as_sindex_sbin_freeall(sbins, sbins_populated);
			}
			as_sindex_release_arr(si_arr, si_arr_index);
			return ret;
		}

		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, b->id, &si_arr[si_arr_index]);
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
		if (sbins_populated > 0) {
			urecord->tr->flags |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
			as_sindex_update_by_sbin(ns, as_index_get_set_name(rd->r, ns), sbins, sbins_populated, &rd->r->keyd);
			as_sindex_sbin_freeall(sbins, sbins_populated);
		}
		as_sindex_release_arr(si_arr, si_arr_index);
	}

	return ret;
} // end udf_aerospike_setbin()

/*
 * Check and validate parameter before performing operation
 *
 * return:
 *      UDF_ERR * in case of failure
 *      0 in case of success
 */
static int
udf_aerospike_param_check(const as_aerospike *as, const as_rec *rec, char *fname, int lineno)
{
	if (!as) {
		cf_debug(AS_UDF, "Invalid Parameters: aerospike=%p", as);
		return UDF_ERR_INTERNAL_PARAMETER;
	}

	int ret = udf_record_param_check(rec, fname, lineno);
	if (ret) {
		return ret;
	}
	return 0;
}

/*
 * Internal function: udf_aerospike__apply_update_atomic
 *
 * Parameters:
 * 		rec --	udf_record to be updated
 *
 * Return Values:
 * 		 0 success
 * 		-1 failure
 *
 * Description:
 * 		This function applies all the updates atomically. That is,
 * 		if one of the bin update/delete/create fails, the entire function
 * 		will fail. If the nth update fails, all the n-1 updates are rolled
 * 		back to their initial values
 *
 * 		Special Notes:
 * 		i. The basic checks of bin name being too long or if there is enough space
 * 		on the disk for the bin values is done before allocating space for any
 * 		of the bins.
 *
 * 		ii. If one of the updates to be rolled back is a bin creation,
 * 		udf_aerospike_delbin is called. This will not free up the bin metadata.
 * 		So there will be a small memory mismatch b/w replica (which did not get the
 * 		record at all and hence no memory is accounted) and the master will be seen.
 * 		To avoid such cases, we are doing checks upfront.
 *
 * 		Callers:
 * 		udf_aerospike__execute_updates
 * 		In this function, if udf_aerospike__apply_update_atomic fails, the record
 * 		is not committed to the storage. On success, record is closed which commits to
 * 		the storage and reopened for the next set of udf updates.
 * 		The return value from udf_aerospike__apply_update_atomic is passed on to the
 * 		callers of this function.
 */
int
udf_aerospike__apply_update_atomic(udf_record *urecord)
{
	int rc						= 0;
	int failmax					= 0;
	int new_bins				= 0;	// How many new bins have to be created in this update
	as_storage_rd * rd			= urecord->rd;
	as_namespace * ns			= rd->ns;
	bool has_sindex				= record_has_sindex(rd->r, ns);
	bool is_record_dirty		= false;

	// This will iterate over all the updates and apply them to storage.
	// The items will remain, and be used as cache values. If an error
	// occurred during setbin(), we rollback all the operation which
	// is and return failure
	cf_detail(AS_UDF, "execute updates: %d updates", urecord->nupdates);

	// loop twice to make sure the updates are performed first so in case
	// something wrong it can be rolled back. The deletes will go through
	// successfully generally.

	as_val* old_values[urecord->nupdates];

	// In first iteration, just calculate how many new bins need to be created
	for(uint32_t i = 0; i < urecord->nupdates; i++ ) {
		old_values[i] = NULL;

		if ( urecord->updates[i].dirty ) {
			char *      k = urecord->updates[i].name;
			if ( k != NULL ) {
				if ( !as_bin_get(rd, k) ) {
					new_bins++;
				}
			}
		}
	}
	// Free bins - total bins not in use in the record
	// Delta bins - new bins that need to be created
	int inuse_bins = as_bin_inuse_count(rd);
	int free_bins  = rd->n_bins - inuse_bins;
	int delta_bins = new_bins - free_bins;
	cf_detail(AS_UDF, "Total bins %d, In use bins %d, Free bins %d , New bins %d, Delta bins %d",
			  rd->n_bins, as_bin_inuse_count(urecord->rd), free_bins, new_bins, delta_bins);

	// Check bin usage limit.
	if ((inuse_bins + new_bins > UDF_RECORD_BIN_ULIMIT) ||
			(urecord->flag & UDF_RECORD_FLAG_TOO_MANY_BINS)) {
		cf_warning(AS_UDF, "bin limit of %d for UDF exceeded: %d bins in use, %d bins free, %s%d new bins needed",
				(int)UDF_RECORD_BIN_ULIMIT, inuse_bins, free_bins,
				(urecord->flag & UDF_RECORD_FLAG_TOO_MANY_BINS) ? ">" : "", new_bins);
		goto Rollback;
	}

	// Allocate space for all the new bins that need to be created beforehand
	if (delta_bins > 0 && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		as_bin_allocate_bin_space(rd, delta_bins);
	}

	if (!rd->ns->storage_data_in_memory && !urecord->particle_data) {
		urecord->particle_data = cf_malloc(rd->ns->storage_write_block_size);
		urecord->cur_particle_data = urecord->particle_data;
		urecord->end_particle_data = urecord->particle_data + rd->ns->storage_write_block_size;
	}

	if (has_sindex) {
		SINDEX_GRLOCK();
	}

	// In second iteration apply updates.
	for(uint32_t i = 0; i < urecord->nupdates; i++ ) {
		if ( urecord->updates[i].dirty && rc == 0) {

			char *      k = urecord->updates[i].name;
			as_val *    v = urecord->updates[i].value;

			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute update: position %d deletes bin %s", i, k);
					old_values[i] = udf_record_storage_get(urecord, k);
					// Only case delete fails if bin is not found that is 
					// as good as delete. Ignore return code !!
					udf_aerospike_delbin(urecord, k);

					if (urecord->dirty != NULL) {
						xdr_fill_dirty_bins(urecord->dirty);
					}
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute update: position %d sets bin %s", i, k);
					old_values[i] = udf_record_storage_get(urecord, k);
					rc = udf_aerospike_setbin(urecord, i, k, v);
					if (rc) {
						if (old_values[i]) {
							as_val_destroy(old_values[i]);
							old_values[i] = NULL;
						} 
						failmax = i;
						goto Rollback;
					}

					if (urecord->dirty != NULL) {
						xdr_add_dirty_bin(ns, urecord->dirty, k, strlen(k));
					}
				}
			}

			is_record_dirty = true;
		}
	}

	{
		if (! as_storage_record_size_and_check(rd)) {
			cf_warning(AS_UDF, "record failed storage size check, will not be updated");
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}

		if (rd->ns->clock_skew_stop_writes) {
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}

		if (rd->ns->stop_writes) {
			cf_warning(AS_UDF, "UDF failed by stop-writes, record will not be updated");
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}

		if (! as_storage_has_space(rd->ns)) {
			cf_warning(AS_UDF, "drives full, record will not be updated");
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}

		if (! is_valid_ttl(urecord->tr->msgp->msg.record_ttl)) {
			cf_warning(AS_UDF, "invalid ttl %u", urecord->tr->msgp->msg.record_ttl);
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}
	}

	if (has_sindex) {
		SINDEX_GRUNLOCK();
	}

	// If there were updates do miscellaneous successful commit
	// tasks
	if (is_record_dirty 
			|| (urecord->flag & UDF_RECORD_FLAG_METADATA_UPDATED)) {
		urecord->flag |= UDF_RECORD_FLAG_HAS_UPDATES; // will write to storage
	}

	// Clean up oldvalue cache and reset dirty. All the changes made 
	// here has made to the particle buffer. Nothing will now be backed out.
	for (uint32_t i = 0; i < urecord->nupdates; i++) {
		if (old_values[i]) {
			as_val_destroy(old_values[i]);
		}
		urecord->updates[i].dirty = false;
	}
	return rc;

Rollback:
	cf_debug(AS_UDF, "Rollback Called: failmax %d", failmax);
	for (int i = 0; i < failmax; i++) {
		if (urecord->updates[i].dirty) {
			char *      k = urecord->updates[i].name;
			// Pick the oldvalue for rollback
			as_val *    v = old_values[i];
			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute rollback: position %d deletes bin %s", i, k);
					rc = udf_aerospike_delbin(urecord, k);
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute rollback: position %d sets bin %s", i, k);
					rc = udf_aerospike_setbin(urecord, i, k, v);
					if (rc) {
						cf_warning(AS_UDF, "Rollback failed .. not good ... !!");
					}
				}
			}
			if (v) {
				as_val_destroy(v);
				cf_debug(AS_UDF, "ROLLBACK as_val_destroy()");
			}
		}
	}

	if (is_record_dirty && urecord->dirty != NULL) {
		xdr_clear_dirty_bins(urecord->dirty);
	}

	if (has_sindex) {
		SINDEX_GRUNLOCK();
	}

	// Reset the flat size in case the stuff is backedout !!! it should not
	// fail in the backout code ...
	if (! as_storage_record_size_and_check(rd)) {
		cf_warning(AS_UDF, "Does not fit even after rollback... it is trouble");
	}

	// Do not clean up the cache in case of failure
	return -1;
}

/*
 * Internal function: udf_aerospike_execute_updates
 *
 * Parameters:
 * 		rec - udf record to be updated
 *
 * Return values
 * 		 0 on success
 *		-1 on failure
 *
 * Description:
 * 		Execute set of udf_record updates. If these updates are successfully
 * 		applied atomically, the storage record is closed (committed to the disk)
 * 		and reopened. The cache is freed up at the end.
 *
 * 		Callers:
 * 		udf_aerospike_rec_create, interface func - aerospike:create(r)
 * 		udf_aerospike_rec_update, interface func - aerospike:update(r)
 * 		udf_aerospike__execute_updates is the key function which is executed in these
 * 		functions. The return value is directly passed on to the lua.
 */
int
udf_aerospike__execute_updates(udf_record * urecord)
{
	int rc = 0;
	as_storage_rd *rd    = urecord->rd;

	if ( urecord->nupdates == 0  &&
			(urecord->flag & UDF_RECORD_FLAG_METADATA_UPDATED) == 0 ) {
		cf_detail(AS_UDF, "No Update when execute update is called");
		return 0;
	}

	// fail updates in case update is not allowed. Queries and scans do not
	// not allow updates. Updates will never be true .. just being paranoid
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		cf_warning(AS_UDF, "Udf: execute updates: allow updates false; FAIL");
		return -1;
	}

	// Commit semantics is either all the update make it or none of it
	rc = udf_aerospike__apply_update_atomic(urecord);

	// allocate down if bins are deleted / not in use
	if (rd->ns && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		int32_t delta_bins = (int32_t)as_bin_inuse_count(rd) - (int32_t)rd->n_bins;
		if (delta_bins) {
			as_bin_allocate_bin_space(rd, delta_bins);
		}
	}
	return rc;
}

static void
udf_aerospike_destroy(as_aerospike * as)
{
	as_aerospike_destroy(as);
}

static cf_clock
udf_aerospike_get_current_time(const as_aerospike * as)
{
	(void)as;
	return cf_clock_getabsolute();
}

/**
 * aerospike::create(record)
 * Function: udf_aerospike_rec_create
 *
 * Parameters:
 * 		as - as_aerospike
 *		rec - as_rec
 *
 * Return Values:
 * 		1 if record is being read or on a create, it already exists
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Create a new record in local storage.
 * 		The record will only be created if it does not exist.
 * 		This assumes the record has a digest that is valid for local storage.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_create
 * 		The return value of udf_aerospike_rec_create is pushed on to the lua stack
 *
 * 		Notes:
 * 		The 'read' and 'exists' flag of udf_record are set to true.
*/
static int
udf_aerospike_rec_create(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord  = (udf_record *) as_rec_source(rec);

	// make sure record isn't already successfully read
	if ((urecord->flag & UDF_RECORD_FLAG_OPEN) != 0) {
		if (as_bin_inuse_has(urecord->rd)) {
			cf_detail(AS_UDF, "udf_aerospike_rec_create: Record Already Exists");
			return 1;
		}
		// else - binless record ok...

		if ((ret = udf_aerospike__execute_updates(urecord)) != 0) {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: failure executing record updates");
			udf_aerospike_rec_remove(as, rec);
		}

		return ret;
	}

	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_storage_rd  *rd    = urecord->rd;
	as_index_tree  *tree  = tr->rsv.tree;

	// make sure we got the record as a create
	int rv = as_record_get_create(tree, &tr->keyd, r_ref, tr->rsv.ns);
	cf_detail_digest(AS_UDF, &tr->keyd, "Creating Record ");

	// rv 0 means record exists, 1 means create, < 0 means fail
	// TODO: Verify correct result codes.
	if (rv == 1) {
		// Record created.
	} else if (rv == 0) {
		// If it's an expired or truncated record, pretend it's a fresh create.
		if (as_record_is_doomed(r_ref->r, tr->rsv.ns)) {
			as_record_rescue(r_ref, tr->rsv.ns);
		} else {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: Record Already Exists 2");
			as_record_done(r_ref, tr->rsv.ns);
			// DO NOT change it has special meaning for caller
			return 1;
		}
	} else if (rv < 0) {
		cf_detail_digest(AS_UDF, &tr->keyd, "udf_aerospike_rec_create: Record Open Failed with rv=%d ", rv);
		return rv;
	}

	// Associates the set name with the storage rec and index
	if (tr->msgp) {
		// Set the set name to index and close record if the setting the set name
		// is not successful
		int rv_set = as_transaction_has_set(tr) ?
				set_set_from_msg(r_ref->r, tr->rsv.ns, &tr->msgp->msg) : 0;
		if (rv_set != 0) {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: Failed to set setname");
			as_index_delete(tree, &tr->keyd);
			as_record_done(r_ref, tr->rsv.ns);
			return 4;
		}

		// Don't write record if it would be truncated.
		if (as_truncate_now_is_truncated(tr->rsv.ns, as_index_get_set_id(r_ref->r))) {
			as_index_delete(tree, &tr->keyd);
			as_record_done(r_ref, tr->rsv.ns);
			return 4;
		}
	}

	// open up storage
	as_storage_record_create(tr->rsv.ns, r_ref->r, rd);

	// Shortcut for set name storage.
	as_storage_record_get_set_name(rd);

	// If the message has a key, apply it to the record.
	if (! get_msg_key(tr, rd)) {
		cf_warning(AS_UDF, "udf_aerospike_rec_create: Can't store key");
		as_storage_record_close(rd);
		as_index_delete(tree, &tr->keyd);
		as_record_done(r_ref, tr->rsv.ns);
		return 4;
	}

	// if multibin storage, we will use urecord->stack_bins, so set the size appropriately
	if (rd->ns->single_bin) {
		rd->n_bins = 1;
	}
	else if (! rd->ns->storage_data_in_memory) {
		rd->n_bins = sizeof(urecord->stack_bins) / sizeof(as_bin);
	}

	// side effect: will set the unused bins to properly unused
	as_storage_rd_load_bins(rd, urecord->stack_bins); // TODO - handle error returned

	int rc = udf_aerospike__execute_updates(urecord);

	if (rc != 0) {
		//  Creating the udf record failed, destroy the as_record
		cf_warning(AS_UDF, "udf_aerospike_rec_create: failure executing record updates (%d)", rc);
		udf_record_close(urecord); // handles particle data and cache only
		as_storage_record_close(rd);
		as_index_delete(tree, &tr->keyd);
		as_record_done(r_ref, tr->rsv.ns);
		return rc;
	}

	// Success...

	urecord->flag |= UDF_RECORD_FLAG_OPEN | UDF_RECORD_FLAG_STORAGE_OPEN;

	return 0;
}

/**
 * aerospike::update(record)
 * Function: udf_aerospike_rec_update
 *
 * Parameters:
 *
 * Return Values:
 * 		-2 if record does not exist
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Updates an existing record in local storage.
 * 		The record will only be updated if it exists.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_update
 * 		The return value of udf_aerospike_rec_update is pushed on to the lua stack
 *
 * 		Notes:
 * 		If the record does not exist or is not read by anyone yet, we cannot
 * 		carry on with the update. 'exists' and 'set' are set to false on record
 * 		init or record remove.
*/
static int
udf_aerospike_rec_update(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record exists and is already opened up
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)
			|| !(urecord->flag & UDF_RECORD_FLAG_OPEN) ) {
		cf_warning(AS_UDF, "Record not found to be open while updating urecord flag=%d", urecord ? urecord->flag : -1);
		return -2;
	}
	cf_detail_digest(AS_UDF, &urecord->rd->r->keyd, "Executing Updates");
	ret = udf_aerospike__execute_updates(urecord);

	if (ret < 0) {
		cf_warning(AS_UDF, "udf_aerospike_rec_update: failure executing record updates (%d)", ret);
	}

	return ret;
}

/**
 * Function udf_aerospike_rec_exists
 *
 * Parameters:
 *
 * Return Values:
 * 		1 if record exists
 * 		0 o/w
 *
 * Description:
 * Check to see if the record exists
 */
static int
udf_aerospike_rec_exists(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	return (urecord && (urecord->flag & UDF_RECORD_FLAG_OPEN)) ? true : false;
}

/*
 * Function: udf_aerospike_rec_remove
 *
 * Parameters:
 *
 * Return Values:
 *		1 if record does not exist
 *		0 on success
 *
 * Description:
 * Removes an existing record from local storage.
 * The record will only be removed if it exists.
 */
static int
udf_aerospike_rec_remove(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}
	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record is already exists before removing it
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		return 1;
	}

	as_storage_rd* rd = urecord->rd;

	if (rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		delete_adjust_sindex(rd);
	}

	as_record_destroy_bins(rd);

	if (rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		as_record_free_bin_space(rd->r);
		rd->bins = NULL;
		rd->n_bins = 0;
	}

	if (urecord->particle_data) {
		cf_free(urecord->particle_data);
		urecord->particle_data = NULL;
	}

	udf_record_cache_free(urecord);
	urecord->flag |= UDF_RECORD_FLAG_HAS_UPDATES;

	return 0;
}

/**
 * Writes a log message
 */
static int
udf_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	(void)a;
	cf_fault_event(AS_UDF, lvl, file, line, "%s", (char *) msg);
	return 0;
}

// Would someone please explain the structure of these hooks?  Why are some null?
const as_aerospike_hooks udf_aerospike_hooks = {
	.rec_create       = udf_aerospike_rec_create,
	.rec_update       = udf_aerospike_rec_update,
	.rec_remove       = udf_aerospike_rec_remove,
	.rec_exists       = udf_aerospike_rec_exists,
	.log              = udf_aerospike_log,
	.get_current_time = udf_aerospike_get_current_time,
	.destroy          = udf_aerospike_destroy
};
