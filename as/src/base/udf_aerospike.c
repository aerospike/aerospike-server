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

//==========================================================
// Includes.
//

#include "base/udf_aerospike.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_rec.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "base/udf_record.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define CAPACITY_STEP (128UL * 1024)


//==========================================================
// Forward declarations.
//

static int udf_aerospike_rec_create(const as_aerospike* as, const as_rec* rec);
static int udf_aerospike_rec_update(const as_aerospike* as, const as_rec* rec);
static int udf_aerospike_rec_exists(const as_aerospike* as, const as_rec* rec);
static int udf_aerospike_rec_remove(const as_aerospike* as, const as_rec* rec);
static int udf_aerospike_log(const as_aerospike* as, const char* file, const int line, const int level, const char* message);
static cf_clock udf_aerospike_get_current_time(const as_aerospike* as);

static int execute_updates(udf_record* urecord);
static void prepare_for_write(udf_record* urecord);
static void execute_failed(udf_record* urecord, int result_code);
static void execute_delete_bin(udf_record* urecord, const char* name);
static int execute_set_bin(udf_record* urecord, const char* name, const as_val* val);
static uint8_t* get_particle_buf(udf_record* urecord, uint32_t size);


//==========================================================
// Inlines & macros.
//

static inline void
param_check(const as_aerospike* as, const as_rec* rec)
{
	cf_assert(as != NULL, AS_UDF, "null as_aerospike object");
	cf_assert(rec != NULL, AS_UDF, "null as_rec object");
	cf_assert(as_rec_source(rec) != NULL, AS_UDF, "null udf_record object");
}


//==========================================================
// Public API - aerospike: hooks.
//

const as_aerospike_hooks udf_aerospike_hooks = {
		.rec_create         = udf_aerospike_rec_create,
		.rec_update         = udf_aerospike_rec_update,
		.rec_exists         = udf_aerospike_rec_exists,
		.rec_remove         = udf_aerospike_rec_remove,
		.log                = udf_aerospike_log,
		.get_current_time   = udf_aerospike_get_current_time,
};


//==========================================================
// Public API - implementation of aerospike: hooks.
//

//------------------------------------------------
// aerospike:create(rec)
//
static int
udf_aerospike_rec_create(const as_aerospike* as, const as_rec* rec)
{
	// FIXME - do the exact return values really matter?

	param_check(as, rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);
	as_storage_rd* rd = urecord->rd;

	if (urecord->is_open) {
		if (rd->n_bins != 0) {
			cf_warning(AS_UDF, "record already exists");
			return 1;
		}
		// else - binless record ok...

		return execute_updates(urecord);
	}

	as_transaction* tr = urecord->tr;
	as_index_ref* r_ref = urecord->r_ref;
	cf_digest* keyd = &tr->keyd;
	as_index_tree* tree = tr->rsv.tree;
	as_namespace* ns = tr->rsv.ns;

	int create_rv = as_record_get_create(tree, keyd, r_ref, ns);

	if (create_rv < 0) {
		return create_rv; // -1 - couldn't allocate index arena
	}

	as_record* r = r_ref->r;

	if (create_rv == 0) {
		// If it's an expired or truncated record, pretend it's a fresh create.
		if (as_record_is_doomed(r, ns)) {
			if (record_has_sindex(r, ns)) {
				// Pessimistic, but not (yet) worth the full check.
				tr->flags |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
			}

			as_record_rescue(r_ref, ns);
		}
		else {
			cf_warning(AS_UDF, "record already exists");
			as_record_done(r_ref, ns);
			return 1;
		}
	}
	// else - record created or rescued.

	if (tr->msgp != NULL) {
		int rv_set = as_transaction_has_set(tr) ?
				set_set_from_msg(r, ns, &tr->msgp->msg) : 0;

		if (rv_set != 0) {
			as_index_delete(tree, keyd);
			as_record_done(r_ref, ns);
			return 4;
		}

		// Don't write record if it would be truncated.
		if (as_truncate_now_is_truncated(ns, as_index_get_set_id(r))) {
			as_index_delete(tree, keyd);
			as_record_done(r_ref, ns);
			return 4;
		}
	}

	as_storage_record_create(ns, r, rd);

	// Shortcut for set name storage.
	as_storage_record_get_set_name(rd);

	// If the message has a key, apply it to the record.
	if (! get_msg_key(tr, rd)) {
		as_storage_record_close(rd);
		as_index_delete(tree, keyd);
		as_record_done(r_ref, ns);
		return 4;
	}

	as_storage_rd_load_bins(rd, urecord->stack_bins); // can't fail

	int exec_rv = execute_updates(urecord);

	if (exec_rv != 0) {
		as_storage_record_close(rd);
		as_index_delete(tree, keyd);
		as_record_done(r_ref, ns);
		return exec_rv; // -1
	}

	urecord->is_open = true;

	return 0;
}

//------------------------------------------------
// aerospike:update(rec)
//
static int
udf_aerospike_rec_update(const as_aerospike* as, const as_rec* rec)
{
	param_check(as, rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "update found urecord not open");
		return -2;
	}

	return execute_updates(urecord);
}

//------------------------------------------------
// aerospike:exists(rec)
//
static int
udf_aerospike_rec_exists(const as_aerospike* as, const as_rec* rec)
{
	param_check(as, rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	return urecord->is_open ? 1 : 0;
}

//------------------------------------------------
// aerospike:remove(rec)
//
static int
udf_aerospike_rec_remove(const as_aerospike* as, const as_rec* rec)
{
	param_check(as, rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "remove found urecord not open");
		return 1;
	}

	udf_record_cache_free(urecord);

	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		const char* name = ns->single_bin ?
				"" : as_bin_get_name_from_id(ns, b->id);

		udf_record_cache_set(urecord, name, NULL, true);
	}

	return execute_updates(urecord);
}

//------------------------------------------------
// aerospike:log(<level>, <format>, ...)
//
static int
udf_aerospike_log(const as_aerospike* as, const char* file, const int line,
		const int level, const char* message)
{
	(void)as;

	cf_log_write(AS_UDF, level, file, line, "%s", (char*)message);

	return 0;
}

//------------------------------------------------
// aerospike:get_current_time()
//
static cf_clock
udf_aerospike_get_current_time(const as_aerospike* as)
{
	(void)as;

	return cf_clock_getabsolute();
}


//==========================================================
// Local helpers.
//

static int
execute_updates(udf_record* urecord)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if (! urecord->has_updates) {
		// Special fast failure case - for this, it's worth it.
		if (ns->clock_skew_stop_writes) {
			urecord->result_code = AS_ERR_FORBIDDEN;
			return -1;
		}

		// Special fast failure case - for this, it's worth it.
		if (ns->stop_writes) {
			cf_warning(AS_UDF, "UDF failed by stop-writes");
			urecord->result_code = AS_ERR_OUT_OF_SPACE;
			return -1;
		}

		prepare_for_write(urecord);
	}

	// TODO - bad bin name is just ignored - no equivalent to this.
	if (urecord->too_many_bins) {
		execute_failed(urecord, AS_ERR_BIN_NAME);
		return -1;
	}

	bool dirty = false;

	for (uint32_t i = 0; i < urecord->n_updates; i++) {
		if (urecord->updates[i].dirty) {
			char* name = urecord->updates[i].name;
			as_val* val = urecord->updates[i].value;

			if (val == NULL || val->type == AS_NIL) {
				execute_delete_bin(urecord, name);
				udf_record_cache_reclaim(urecord, i--); // decrements n_updates
			}
			else {
				int rv = execute_set_bin(urecord, name, val);

				if (rv != AS_OK) {
					execute_failed(urecord, rv);
					return -1;
				}
			}

			dirty = true;
		}
	}

	urecord->result_code = AS_OK;
	urecord->has_updates = true;

	if (dirty) {
		for (uint32_t i = 0; i < urecord->n_updates; i++) {
			urecord->updates[i].dirty = false;
		}
	}

	return 0;
}

static void
prepare_for_write(udf_record* urecord)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	urecord->old_memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	urecord->n_old_bins = rd->n_bins;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			as_single_bin_copy(urecord->stack_bins, as_index_get_single_bin(r));
		}
		else if (rd->n_bins != 0) {
			memcpy(urecord->stack_bins, rd->bins, rd->n_bins * sizeof(as_bin));
		}

		urecord->old_dim_bins = rd->bins;
		rd->bins = urecord->stack_bins;
	}
	else if (rd->n_bins != 0) {
		if (ns->single_bin) {
			as_single_bin_copy(urecord->old_ssd_bins, rd->bins);
		}
		else {
			memcpy(urecord->old_ssd_bins, rd->bins,
					rd->n_bins * sizeof(as_bin));
		}
	}
}

static void
execute_failed(udf_record* urecord, int result_code)
{
	urecord->result_code = (uint8_t)result_code;
	urecord->has_updates = false;

	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			write_dim_single_bin_unwind(urecord->old_dim_bins,
					urecord->n_old_bins, rd->bins, rd->n_bins,
					urecord->cleanup_bins, urecord->n_cleanup_bins);
		}
		else {
			write_dim_unwind(urecord->old_dim_bins, urecord->n_old_bins,
					rd->bins, rd->n_bins, urecord->cleanup_bins,
					urecord->n_cleanup_bins);
		}

		rd->bins = urecord->old_dim_bins;
	}
	else if (urecord->n_old_bins != 0) {
		if (ns->single_bin) {
			as_single_bin_copy(rd->bins, urecord->old_ssd_bins);
		}
		else {
			memcpy(rd->bins, urecord->old_ssd_bins,
					urecord->n_old_bins * sizeof(as_bin));
		}
	}

	rd->n_bins = (uint16_t)urecord->n_old_bins;

	if (urecord->particle_buf != NULL) {
		cf_free(urecord->particle_buf);
		urecord->particle_buf = NULL;
	}

	udf_record_cache_free(urecord);
}

static void
execute_delete_bin(udf_record* urecord, const char* name)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	as_bin cleanup_bin;

	if (as_bin_pop(rd, name, &cleanup_bin) && ns->storage_data_in_memory) {
		append_bin_to_destroy(&cleanup_bin, urecord->cleanup_bins,
				&urecord->n_cleanup_bins);
	}
}

static int
execute_set_bin(udf_record* urecord, const char* name, const as_val* val)
{
	as_storage_rd* rd = urecord->rd;
	as_namespace* ns = rd->ns;

	if (as_particle_type_from_asval(val) == AS_PARTICLE_TYPE_NULL) {
		cf_warning(AS_UDF, "setting bin %s with unusable as_val", name);
		return AS_ERR_INCOMPATIBLE_TYPE;
	}

	if (rd->n_bins == UDF_BIN_LIMIT && as_bin_get(rd, name) == NULL) {
		cf_warning(AS_UDF, "exceeded UDF max bins %d", UDF_BIN_LIMIT);
		return AS_ERR_BIN_NAME;
	}

	int rv;
	as_bin* b = as_bin_get_or_create(rd, name, &rv);

	if (b == NULL) {
		cf_warning(AS_UDF, "can't create bin %s", name);
		return rv;
	}

	if (ns->storage_data_in_memory) {
		as_bin cleanup_bin;
		as_bin_copy(ns, &cleanup_bin, b);

		if ((rv = as_bin_particle_alloc_from_asval(b, val)) != 0) {
			cf_warning(AS_UDF, "can't convert as_val to particle in %s", name);
			return -rv;
		}

		append_bin_to_destroy(&cleanup_bin, urecord->cleanup_bins,
				&urecord->n_cleanup_bins);
	}
	else {
		uint32_t size = as_particle_size_from_asval(val);
		uint8_t* buf = get_particle_buf(urecord, size);

		as_bin_particle_stack_from_asval(b, buf, val);
	}

	return AS_OK;
}

static uint8_t*
get_particle_buf(udf_record* urecord, uint32_t size)
{
	as_namespace* ns = urecord->rd->ns;

	if (urecord->particle_buf == NULL) {
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
