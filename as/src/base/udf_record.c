/*
 * udf_record.c
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

#include "base/udf_record.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"

#include "warnings.h"


//==========================================================
// Forward declarations.
//

static int udf_record_set(const as_rec* rec, const char* name, const as_val* value);
static int udf_record_set_ttl(const as_rec* rec, uint32_t ttl);
static int udf_record_drop_key(const as_rec* rec);
static as_val* udf_record_get(const as_rec* rec, const char* name);
static const char* udf_record_setname(const as_rec* rec);
static as_val* udf_record_key(const as_rec* rec);
static as_bytes* udf_record_digest(const as_rec* rec);
static uint64_t udf_record_last_update_time(const as_rec* rec);
static uint16_t udf_record_gen(const as_rec* rec);
static uint32_t udf_record_ttl(const as_rec* rec);
static uint32_t udf_record_device_size(const as_rec* rec);
static uint16_t udf_record_numbins(const as_rec* rec);
static int udf_record_bin_names(const as_rec* rec, as_rec_bin_names_callback cb, void* udata);

static bool param_check_w_bin(const as_rec* rec, const char* name);
static as_val* udf_record_cache_get(udf_record* urecord, const char* name);
static as_val* as_val_from_flat_key(const uint8_t* flat_key, uint32_t size);


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
// Public API - rec hooks.
//

const as_rec_hooks udf_record_hooks = {
		.set                = udf_record_set,
		.set_ttl            = udf_record_set_ttl,
		.drop_key           = udf_record_drop_key,
		.get                = udf_record_get,
		.setname            = udf_record_setname,
		.key                = udf_record_key,
		.digest             = udf_record_digest,
		.last_update_time   = udf_record_last_update_time,
		.gen                = udf_record_gen,
		.ttl                = udf_record_ttl,
		.device_size        = udf_record_device_size,
		.numbins            = udf_record_numbins,
		.bin_names          = udf_record_bin_names
};

const as_rec_hooks as_aggr_record_hooks = {
		.get                = udf_record_get,
		.setname            = udf_record_setname,
		.key                = udf_record_key,
		.digest             = udf_record_digest,
		.last_update_time   = udf_record_last_update_time,
		.gen                = udf_record_gen,
		.ttl                = udf_record_ttl,
		.device_size        = udf_record_device_size,
		.numbins            = udf_record_numbins,
		.bin_names          = udf_record_bin_names
};


//==========================================================
// Public API.
//

void
udf_record_init(udf_record* urecord)
{
	urecord->tr = NULL;
	urecord->r_ref = NULL;
	urecord->rd = NULL;

	urecord->is_open = false;
	urecord->is_loaded = false;
	urecord->too_many_bins = false;
	urecord->has_updates = false;

	urecord->result_code = AS_OK;
	urecord->old_memory_bytes = 0;

	urecord->particle_buf = NULL;
	urecord->buf_size = 0;
	urecord->buf_offset = 0;

	urecord->n_old_bins = 0;
	urecord->n_cleanup_bins = 0;
	urecord->n_inserts = 0;
	urecord->n_updates = 0;
}

void
udf_record_cache_free(udf_record* urecord)
{
	for (uint32_t i = 0; i < urecord->n_updates; i++) {
		udf_record_bin* bin = &urecord->updates[i];

		if (bin->value != NULL) {
			as_val_destroy(bin->value);
			bin->value = NULL;
		}
	}

	urecord->n_inserts = 0;
	urecord->n_updates = 0;
	urecord->too_many_bins = false;
}

void
udf_record_cache_set(udf_record* urecord, const char* name, as_val* value,
		bool dirty)
{
	if (urecord->too_many_bins) {
		return;
	}

	bool is_insert = value != NULL && value->type != AS_NIL;

	for (uint32_t i = 0; i < urecord->n_updates; i++) {
		udf_record_bin* bin = &urecord->updates[i];

		if (strcmp(name, bin->name) == 0) {
			if (bin->value != NULL && bin->value->type != AS_NIL) {
				urecord->n_inserts--;
			}

			as_val_destroy(bin->value); // handles NULL and nil
			bin->value = (as_val*)value;
			bin->dirty = dirty;

			if (is_insert) {
				urecord->n_inserts++;
			}

			return;
		}
	}

	if (urecord->n_updates >= UDF_UPDATE_LIMIT) {
		cf_warning(AS_UDF, "too many bin updates for UDF");
		urecord->too_many_bins = true;
		return;
	}

	if (is_insert && urecord->n_inserts >= UDF_BIN_LIMIT) {
		cf_warning(AS_UDF, "too many bins for UDF");
		urecord->too_many_bins = true;
		return;
	}

	udf_record_bin* bin = &urecord->updates[urecord->n_updates];

	strcpy(bin->name, name);
	bin->value = (as_val*)value;
	bin->dirty = dirty;

	urecord->n_updates++;

	if (is_insert) {
		urecord->n_inserts++;
	}
}

void
udf_record_cache_reclaim(udf_record* urecord, uint32_t i)
{
	// Note - value removed is NULL/nil so does not need to be destroyed.

	urecord->n_updates--;

	if (i < urecord->n_updates) {
		urecord->updates[i] = urecord->updates[urecord->n_updates];
	}
}

int
udf_record_open(udf_record* urecord)
{
	if (urecord->is_open) {
		return 0;
	}

	as_transaction* tr = urecord->tr;
	as_index_ref* r_ref = urecord->r_ref;
	as_namespace* ns = tr->rsv.ns;

	if (as_record_get_live(tr->rsv.tree, &tr->keyd, r_ref, ns) != 0) {
		return -1;
	}

	as_index* r = r_ref->r;

	if (as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return -1;
	}

	urecord->is_open = true;

	return 0;
}

void
udf_record_close(udf_record* urecord)
{
	if (urecord->is_open) {
		// FIXME - development paranoia - remove eventually.
		cf_assert(! urecord->has_updates, AS_UDF,
				"unexpected - record has updates");
		cf_assert(urecord->particle_buf == NULL, AS_UDF,
				"unexpected - has particle buf");

		if (urecord->is_loaded) {
			as_storage_record_close(urecord->rd);
			urecord->is_loaded = false;
		}

		as_record_done(urecord->r_ref, urecord->tr->rsv.ns);
		urecord->is_open = false;
	}

	udf_record_cache_free(urecord);
}

int
udf_record_load(udf_record* urecord)
{
	cf_assert(urecord->is_open, AS_UDF, "loading unopened record");

	if (urecord->is_loaded) {
		return 0;
	}

	as_index_ref* r_ref = urecord->r_ref;
	as_namespace* ns = urecord->tr->rsv.ns;

	as_storage_rd* rd = urecord->rd;

	as_storage_record_open(ns, r_ref->r, rd);

	if (as_storage_rd_load_bins(rd, urecord->stack_bins) < 0) {
		as_storage_record_close(rd);
		return AS_ERR_UNKNOWN;
	}

	if (rd->n_bins > UDF_BIN_LIMIT) {
		cf_warning(AS_UDF, "too many bins (%d) for UDF", rd->n_bins);
		as_storage_record_close(rd);
		return AS_ERR_BIN_NAME;
	}

	as_storage_record_get_set_name(rd);
	as_storage_rd_load_key(rd);

	urecord->is_loaded = true;

	return 0;
}


//==========================================================
// Public API - implementation of rec hooks.
//

//------------------------------------------------
// rec["bin-x"] = 10
// rec.bin-x = 10
//
// -- delete a bin:
// rec["bin-x"] = nil
// rec.bin-x = nil
//
static int
udf_record_set(const as_rec* rec, const char* name, const as_val* value)
{
	if (! param_check_w_bin(rec, name)) {
		return -1;
	}

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	udf_record_cache_set(urecord, name, (as_val*)value, true);

	return 0;
}

//------------------------------------------------
// err = record.set_ttl(rec, 3600)
//
static int
udf_record_set_ttl(const as_rec* rec, uint32_t ttl)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	urecord->tr->msgp->msg.record_ttl = ttl;

	return 0;
}

//------------------------------------------------
// err = record.drop_key(rec)
//
static int
udf_record_drop_key(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return -1;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return -1;
	}

	// Flag the key to be dropped.
	urecord->rd->key = NULL;
	urecord->rd->key_size = 0;

	return 0;
}

//------------------------------------------------
// v = rec["bin-x"]
// v = rec.bin-x
//
static as_val*
udf_record_get(const as_rec* rec, const char* name)
{
	if (! param_check_w_bin(rec, name)) {
		return NULL;
	}

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	as_val* value = udf_record_cache_get(urecord, name);

	if (value != NULL) {
		return value;
	}

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return NULL;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return NULL;
	}

	as_bin* b = as_bin_get(urecord->rd, name);

	if (b == NULL) {
		return NULL;
	}

	value = as_bin_particle_to_asval(b);

	if (value == NULL) {
		return NULL;
	}

	udf_record_cache_set(urecord, name, value, false);

	return value;
}

//------------------------------------------------
// s = record.setname(rec)
//
static const char*
udf_record_setname(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return NULL;
	}

	return as_index_get_set_name(urecord->r_ref->r, urecord->tr->rsv.ns);
}

//------------------------------------------------
// v = record.key(rec)
//
static as_val*
udf_record_key(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return NULL;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return NULL;
	}

	if (urecord->rd->key != NULL) {
		return as_val_from_flat_key(urecord->rd->key, urecord->rd->key_size);
	}

	// TODO - perhaps look for the key in the message.
	return NULL;
}

//------------------------------------------------
// d = record.digest(rec)
//
static as_bytes*
udf_record_digest(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	cf_digest* keyd = cf_malloc(sizeof(cf_digest));

	*keyd = urecord->tr->keyd;

	return as_bytes_new_wrap(keyd->digest, CF_DIGEST_KEY_SZ, true);
}

//------------------------------------------------
// t = record.last_update_time(rec)
//
static uint64_t
udf_record_last_update_time(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return 0;
	}

	return urecord->r_ref->r->last_update_time;
}

//------------------------------------------------
// g = record.gen(rec)
//
static uint16_t
udf_record_gen(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return 0;
	}

	return plain_generation(urecord->r_ref->r->generation, urecord->tr->rsv.ns);
}

//------------------------------------------------
// t = record.ttl(rec)
//
static uint32_t
udf_record_ttl(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return 0;
	}

	uint32_t now = as_record_void_time_get();
	uint32_t void_time = urecord->r_ref->r->void_time;

	return void_time > now ? void_time - now : 0;
}

//------------------------------------------------
// sz = record.device_size(rec)
//
static uint32_t
udf_record_device_size(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return 0;
	}

	return as_storage_record_size(urecord->tr->rsv.ns, urecord->r_ref->r);
}

//------------------------------------------------
// n = record.numbins(rec)
//
static uint16_t
udf_record_numbins(const as_rec* rec)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return 0;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return 0;
	}

	return urecord->rd->n_bins;
}

//------------------------------------------------
// names = record.bin_names(rec)
//
// for i, name in ipairs(names) do
//     -- use i and name
// end
//
static int
udf_record_bin_names(const as_rec* rec, as_rec_bin_names_callback cb,
		void* udata)
{
	param_check(rec);

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (! urecord->is_open) {
		cf_warning(AS_UDF, "record not open");
		return -1;
	}

	if (udf_record_load(urecord) != 0) {
		cf_warning(AS_UDF, "record failed load");
		return -1;
	}

	as_namespace* ns = urecord->rd->ns;
	uint16_t n_bins = urecord->rd->n_bins;

	if (ns->single_bin) {
		char empty[] = "";

		cb(empty, n_bins, AS_BIN_NAME_MAX_SZ, udata);
		return 0;
	}

	char bin_names[n_bins * AS_BIN_NAME_MAX_SZ];

	for (uint16_t i = 0; i < n_bins; i++) {
		as_bin *b = &urecord->rd->bins[i];

		const char* name = as_bin_get_name_from_id(ns, b->id);
		strcpy(bin_names + (i * AS_BIN_NAME_MAX_SZ), name);
	}

	cb(bin_names, n_bins, AS_BIN_NAME_MAX_SZ, udata);

	return 0;
}


//==========================================================
// Local helpers.
//

static bool
param_check_w_bin(const as_rec* rec, const char* name)
{
	param_check(rec);

	cf_assert(name != NULL, AS_UDF, "null bin name");

	udf_record* urecord = (udf_record*)as_rec_source(rec);

	if (urecord->tr->rsv.ns->single_bin) {
		if (*name != 0) {
			cf_warning(AS_UDF, "non-empty bin name in single-bin namespace");
			return false;
		}

		return 0;
	}

	if (*name == 0) {
		cf_warning(AS_UDF, "empty bin name");
		return false;
	}

	if (strlen(name) >= AS_BIN_NAME_MAX_SZ) {
		cf_warning(AS_UDF, "bin name %s too big", name);
		return false;
	}

	return true;
}

static as_val*
udf_record_cache_get(udf_record* urecord, const char* name)
{
	if (urecord->n_updates == 0) {
		return NULL;
	}

	for (uint32_t i = 0; i < urecord->n_updates; i++) {
		udf_record_bin* bin = &urecord->updates[i];

		if (strcmp(name, bin->name) == 0) {
			return bin->value; // NULL or as_nil are ok
		}
	}

	return NULL;
}

static as_val*
as_val_from_flat_key(const uint8_t* flat_key, uint32_t size)
{
	uint8_t type = *flat_key;
	const uint8_t* key = flat_key + 1;

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
		if (size != 1 + sizeof(uint64_t)) {
			return NULL;
		}
		// Flat integer keys are in big-endian order.
		return (as_val*)
				as_integer_new((int64_t)cf_swap_from_be64(*(uint64_t*)key));
	case AS_PARTICLE_TYPE_STRING:
	{
		// Key length is size - 1, then +1 for null-termination.
		char* buf = cf_malloc(size);
		uint32_t len = size - 1;

		memcpy(buf, key, len);
		buf[len] = '\0';

		return (as_val*)as_string_new(buf, true);
	}
	case AS_PARTICLE_TYPE_BLOB:
	{
		uint32_t blob_size = size - 1;
		uint8_t* buf = cf_malloc(blob_size);

		memcpy(buf, key, blob_size);

		return (as_val*)as_bytes_new_wrap(buf, blob_size, true);
	}
	default:
		return NULL;
	}
}
