/*
 * udf_aerospike.c
 *
 * Copyright (C) 2012-2020 Aerospike, Inc.
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

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "base/udf_record.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"

#define CAPACITY_STEP (128 * 1024)

static int udf_aerospike_rec_remove(const as_aerospike *, const as_rec *);

static void
execute_delete_bin(udf_record* urecord, const char* bname)
{
	as_storage_rd *rd = urecord->rd;
	as_namespace *ns = rd->ns;

	as_bin cleanup_bin;

	if (as_bin_pop(rd, bname, &cleanup_bin) && ns->storage_data_in_memory) {
		append_bin_to_destroy(&cleanup_bin, urecord->cleanup_bins,
				&urecord->n_cleanup_bins);
	}
}

static uint8_t*
get_particle_buf(udf_record* urecord, udf_record_bin* ubin, uint32_t size)
{
	if (size > urecord->rd->ns->storage_write_block_size) {
		cf_warning(AS_UDF, "bin %s particle too big %u", ubin->name, size);
		return NULL;
	}

	as_namespace* ns = urecord->rd->ns;

	if (! ns->storage_data_in_memory && ! urecord->particle_buf) {
		urecord->buf_size = ns->storage_write_block_size;
		urecord->buf_offset = 0;

		urecord->particle_buf = cf_malloc(urecord->buf_size);
	}

	size_t new_size = urecord->buf_offset + size;

	if (new_size > urecord->buf_size) {
		urecord->buf_size = (new_size + CAPACITY_STEP - 1) & -CAPACITY_STEP;

		urecord->particle_buf = cf_realloc(urecord->particle_buf,
				urecord->buf_size);
	}

	uint8_t* buf = urecord->particle_buf + urecord->buf_offset;

	urecord->buf_offset += size;

	return buf;
}

static bool
execute_set_bin(udf_record* urecord, int ix, const char* bname,
		const as_val* val)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if (as_particle_type_from_asval(val) == AS_PARTICLE_TYPE_NULL) {
		cf_warning(AS_UDF, "setting bin %s with unusable as_val", bname);
		return false;
	}

	if (rd->n_bins == UDF_RECORD_BIN_ULIMIT && as_bin_get(rd, bname) == NULL) {
		cf_warning(AS_UDF, "exceeded UDF max bins %d", UDF_RECORD_BIN_ULIMIT);
		return false;
	}

	as_bin* b = as_bin_get_or_create(rd, bname);

	if (b == NULL) {
		cf_warning(AS_UDF, "can't create bin %s", bname);
		return false;
	}

	if (ns->storage_data_in_memory) {
		as_bin cleanup_bin;
		as_bin_copy(ns, &cleanup_bin, b);

		if (as_bin_particle_alloc_from_asval(b, val) != 0) {
			cf_warning(AS_UDF, "can't convert as_val to particle in %s", bname);
			return false;
		}

		append_bin_to_destroy(&cleanup_bin, urecord->cleanup_bins,
				&urecord->n_cleanup_bins);
	}
	else {
		uint32_t size = as_particle_size_from_asval(val);
		uint8_t* buf = get_particle_buf(urecord, &urecord->updates[ix], size);

		if (buf == NULL) {
			return false;
		}

		as_bin_particle_stack_from_asval(b, buf, val);
	}

	return true;
}

static void
execute_failed(udf_record* urecord)
{
	urecord->flag &= ~UDF_RECORD_FLAG_HAS_UPDATES;

	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if (ns->storage_data_in_memory) {
		write_dim_unwind(urecord->old_dim_bins, urecord->n_old_bins, rd->bins,
				rd->n_bins, urecord->cleanup_bins, urecord->n_cleanup_bins);
	}
}

static void
prepare_for_write(udf_record* urecord)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	urecord->starting_memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	urecord->n_old_bins = rd->n_bins;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			as_single_bin_copy(urecord->new_bins, as_index_get_single_bin(r));
		}
		else if (rd->n_bins != 0) {
			memcpy(urecord->new_bins, rd->bins, rd->n_bins * sizeof(as_bin));
		}

		urecord->old_dim_bins = rd->bins;
		rd->bins = urecord->new_bins;
	}
	else if (rd->n_bins != 0 && ! ns->single_bin && record_has_sindex(r, ns)) {
		memcpy(urecord->old_ssd_bins, rd->bins, rd->n_bins * sizeof(as_bin));
	}
}

static int
execute_updates(udf_record* urecord)
{
	// Check that stream UDFs (which are read-only) don't try to write.
	// TODO - would be better if we checked on making updates.
	if ((urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES) == 0) {
		cf_warning(AS_UDF, "read-only UDF trying to write");
		return -1;
	}

	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if ((urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) == 0) {
		if (ns->clock_skew_stop_writes) {
			return -1;
		}

		if (ns->stop_writes) {
			cf_warning(AS_UDF, "UDF failed by stop-writes");
			return -1;
		}

		prepare_for_write(urecord);
	}

	// TODO - bad bin name is just ignored - no equivalent to this.
	if ((urecord->flag & UDF_RECORD_FLAG_TOO_MANY_BINS) != 0) {
		execute_failed(urecord);
		return -1;
	}

	bool dirty = false;

	for (uint32_t i = 0; i < urecord->nupdates; i++) {
		if (urecord->updates[i].dirty) {
			char* k = urecord->updates[i].name;
			as_val* v = urecord->updates[i].value;

			if (v == NULL || v->type == AS_NIL) {
				execute_delete_bin(urecord, k);
			}
			else {
				if (! execute_set_bin(urecord, i, k, v)) {
					execute_failed(urecord);
					return -1;
				}
			}

			dirty = true;
		}
	}

	if (! as_storage_record_size_and_check(rd)) {
		cf_warning(AS_UDF, "record failed storage size check");
		execute_failed(urecord);
		return -1;
	}

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_UDF, "drives full");
		execute_failed(urecord);
		return -1;
	}

	if (! is_valid_ttl(urecord->tr->msgp->msg.record_ttl)) {
		cf_warning(AS_UDF, "invalid ttl %u", urecord->tr->msgp->msg.record_ttl);
		execute_failed(urecord);
		return -1;
	}

	if (is_ttl_disallowed(urecord->tr->msgp->msg.record_ttl, ns)) {
		cf_ticker_warning(AS_UDF, "disallowed ttl with nsup-period 0");
		execute_failed(urecord);
		return -1;
	}

	urecord->flag |= UDF_RECORD_FLAG_HAS_UPDATES;

	if (dirty) {
		for (uint32_t i = 0; i < urecord->nupdates; i++) {
			urecord->updates[i].dirty = false;
		}
	}

	return 0;
}

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


//==========================================================
// Hooks - aerospike: scoped functions.
//

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
	as_storage_rd  *rd    = urecord->rd;

	// make sure record isn't already successfully read
	if ((urecord->flag & UDF_RECORD_FLAG_OPEN) != 0) {
		if (rd->n_bins != 0) {
			cf_detail(AS_UDF, "udf_aerospike_rec_create: Record Already Exists");
			return 1;
		}
		// else - binless record ok...

		if ((ret = execute_updates(urecord)) != 0) {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: failure executing record updates");
			udf_aerospike_rec_remove(as, rec);
		}

		return ret;
	}

	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_index_tree  *tree  = tr->rsv.tree;

	// make sure we got the record as a create
	int rv = as_record_get_create(tree, &tr->keyd, r_ref, tr->rsv.ns);
	cf_detail(AS_UDF, "Creating Record %pD", &tr->keyd);

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
		cf_detail(AS_UDF, "udf_aerospike_rec_create: Record Open Failed with rv=%d %pD", rv, &tr->keyd);
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

	as_storage_rd_load_bins(rd, urecord->new_bins); // can't fail

	int rc = execute_updates(urecord);

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

	return execute_updates(urecord);
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

	udf_record_cache_free(urecord);

	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		const char* bname = ns->single_bin ?
				"" : as_bin_get_name_from_id(ns, b->id);

		udf_record_cache_set(urecord, bname, NULL, true);
	}

	return execute_updates(urecord);
}

/**
 * Writes a log message
 */
static int
udf_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	(void)a;
	cf_log_write(AS_UDF, lvl, file, line, "%s", (char *) msg);
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
