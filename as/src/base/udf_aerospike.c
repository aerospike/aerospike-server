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

#include "dynbuf.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "base/udf_record.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define LLB_ROUND_SZ (128U * 1024)


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
static int execute_set_bin(udf_record* urecord, const char* name, const as_val* val);
static uint8_t* get_particle_buf(udf_record* urecord, uint32_t size);


//==========================================================
// Inlines & macros.
//

static inline void
param_check(const as_rec* rec)
{
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

	(void)as;

	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);
	as_storage_rd* rd = urecord->rd;

	if (urecord->is_open) {
		if (udf_record_load(urecord) != 0) {
			cf_warning(AS_UDF, "record failed load");
			return 1;
		}

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
			as_set_index_delete_live(ns, tree, r, r_ref->r_h);
			as_record_rescue(r_ref, ns);
		}
		else {
			cf_warning(AS_UDF, "record already exists");
			as_record_done(r_ref, ns);
			return 1;
		}
	}
	// else - record created or rescued.

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
	urecord->is_loaded = true;

	return 0;
}

//------------------------------------------------
// aerospike:update(rec)
//
static int
udf_aerospike_rec_update(const as_aerospike* as, const as_rec* rec)
{
	(void)as;

	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "update found urecord not open");
		return -2;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
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
	(void)as;

	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	return urecord->is_open && as_record_is_live(urecord->r_ref->r) ? 1 : 0;
}

//------------------------------------------------
// aerospike:remove(rec)
//
static int
udf_aerospike_rec_remove(const as_aerospike* as, const as_rec* rec)
{
	(void)as;

	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "remove found urecord not open");
		return 1;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return 1;
	}

	udf_record_cache_free(urecord);

	as_storage_rd* rd = urecord->rd;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		udf_record_cache_set(urecord, b->name, NULL, true);
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

	cf_log_write(AS_UDF, (cf_log_level)level, file, line, "%s", (char*)message);

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
		prepare_for_write(urecord);
	}

	if (ns->clock_skew_stop_writes) {
		execute_failed(urecord, AS_ERR_FORBIDDEN);
		return -1;
	}

	if (ns->stop_writes) {
		execute_failed(urecord, AS_ERR_OUT_OF_SPACE);
		return -1;
	}

	as_set* p_set = as_namespace_get_record_set(ns, rd->r);

	if (as_set_size_stop_writes(p_set)) {
		cf_ticker_warning(AS_UDF, "{%s|%s} at stop-writes-size - can't execute",
				ns->name, p_set->name);
		execute_failed(urecord, AS_ERR_FORBIDDEN);
		return -1;
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

			if (! udf_resolve_bin(rd, name)) {
				execute_failed(urecord, AS_ERR_LOST_CONFLICT);
				return -1;
			}

			if (val == NULL || val->type == AS_NIL) {
				udf_delete_bin(urecord->rd, name);
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
	else { // treat this as a touch
		touch_bin_metadata(rd);
	}

	return 0;
}

static void
prepare_for_write(udf_record* urecord)
{
	as_storage_rd* rd = urecord->rd;

	urecord->n_old_bins = rd->n_bins;

	if (rd->n_bins == 0) {
		return;
	}

	memcpy(urecord->old_bins, rd->bins, rd->n_bins * sizeof(as_bin));
	prepare_bin_metadata(urecord->tr, rd);
}

static void
execute_failed(udf_record* urecord, int result_code)
{
	urecord->result_code = (uint8_t)result_code;
	urecord->has_updates = false;

	as_storage_rd* rd = urecord->rd;

	if (urecord->n_old_bins != 0) {
		memcpy(rd->bins, urecord->old_bins,
				urecord->n_old_bins * sizeof(as_bin));
	}

	rd->n_bins = (uint16_t)urecord->n_old_bins;

	if (urecord->particle_llb.head != NULL) {
		cf_ll_buf_free(&urecord->particle_llb);
		urecord->particle_llb.head = NULL;
	}

	udf_record_cache_free(urecord);
}

static int
execute_set_bin(udf_record* urecord, const char* name, const as_val* val)
{
	as_storage_rd* rd = urecord->rd;

	if (as_particle_type_from_asval(val) == AS_PARTICLE_TYPE_NULL) {
		cf_warning(AS_UDF, "setting bin %s with unusable as_val", name);
		return AS_ERR_INCOMPATIBLE_TYPE;
	}

	if (rd->n_bins == UDF_BIN_LIMIT && as_bin_get(rd, name) == NULL) {
		cf_warning(AS_UDF, "exceeded UDF max bins %d", UDF_BIN_LIMIT);
		return AS_ERR_BIN_NAME;
	}

	as_bin* b = as_bin_get_or_create(rd, name);
	uint32_t size = as_particle_size_from_asval(val);
	uint8_t* buf = get_particle_buf(urecord, size);

	as_bin_particle_from_asval(b, buf, val);

	return AS_OK;
}

static uint8_t*
get_particle_buf(udf_record* urecord, uint32_t size)
{
	if (urecord->particle_llb.head == NULL) {
		cf_ll_buf_init_heap(&urecord->particle_llb,
				(size + (LLB_ROUND_SZ - 1)) & -LLB_ROUND_SZ);
	}

	uint8_t* buf;

	cf_ll_buf_reserve(&urecord->particle_llb, size, &buf);

	return buf;
}
