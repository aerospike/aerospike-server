/*
 * flat.c
 *
 * Copyright (C) 2019 Aerospike, Inc.
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

#include "storage/flat.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "bits.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"

#include "warnings.h"


//==========================================================
// Forward declarations.
//

static uint32_t flat_record_overhead_size(const as_storage_rd* rd);


//==========================================================
// Inlines & macros.
//

// storage-engine memory may truncate n_rblocks at 19 bits - only check those.
static inline uint32_t
check_n_rblocks(uint32_t size)
{
	return SIZE_TO_N_RBLOCKS(size) & ((1 << 19) - 1);
}


//==========================================================
// Public API.
//

void
as_flat_pickle_record(as_storage_rd* rd)
{
	rd->pickle_sz = as_flat_record_size(rd);

	// Note - will no-op for storage-engine memory, which doesn't compress.
	as_flat_record* flat = as_flat_compress_bins_and_pack_record(rd,
			rd->ns->storage_write_block_size, &rd->pickle_sz);

	rd->pickle = cf_malloc(rd->pickle_sz);

	if (flat == NULL) {
		// Note - storage-engine memory may truncate n_rblocks at 19 bits.
		as_flat_pack_record(rd, SIZE_TO_N_RBLOCKS(rd->pickle_sz),
				(as_flat_record*)rd->pickle);
	}
	else {
		memcpy(rd->pickle, flat, rd->pickle_sz);
	}
}

uint32_t
as_flat_record_size(const as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;

	// Start with the record storage overhead.
	uint32_t write_sz = flat_record_overhead_size(rd);

	// TODO - temporary, until we sort out the whole rd->n_bins mess.
	uint16_t n_used_bins;

	// Add the bins' sizes, including bin overhead.
	for (n_used_bins = 0; n_used_bins < rd->n_bins; n_used_bins++) {
		as_bin* bin = &rd->bins[n_used_bins];

		if (! as_bin_inuse(bin)) {
			break;
		}

		uint32_t name_sz = ns->single_bin ?
				0 : 1 + (uint32_t)strlen(as_bin_get_name_from_id(ns, bin->id));

		write_sz += name_sz + as_bin_particle_flat_size(bin);
	}

	// TODO - temporary, until we sort out the whole rd->n_bins mess.
	if (! ns->single_bin && n_used_bins != 0) {
		write_sz += uintvar_size(n_used_bins);
	}

	return write_sz;
}

void
as_flat_pack_record(const as_storage_rd* rd, uint32_t n_rblocks,
		as_flat_record* flat)
{
	uint8_t* buf = flatten_record_meta(rd, n_rblocks, NULL, flat);

	flatten_bins(rd, buf, NULL);
}

bool
as_flat_unpack_remote_record_meta(as_namespace* ns, as_remote_record* rr)
{
	if (rr->pickle_sz < sizeof(as_flat_record)) {
		cf_warning(AS_FLAT, "record too small %zu", rr->pickle_sz);
		return false;
	}

	as_flat_record* flat = (as_flat_record*)rr->pickle;

	if (flat->magic != AS_FLAT_MAGIC) {
		cf_warning(AS_FLAT, "bad magic %u", flat->magic);
		return false;
	}

	if (flat->n_rblocks != check_n_rblocks((uint32_t)rr->pickle_sz)) {
		cf_warning(AS_FLAT, "n_rblocks mismatch (%u,%zu)", flat->n_rblocks,
				rr->pickle_sz);
		return false;
	}

	rr->keyd = &flat->keyd;
	rr->generation = flat->generation;
	rr->last_update_time = flat->last_update_time;

	as_flat_opt_meta opt_meta = { 0 };

	const uint8_t* flat_bins = as_flat_unpack_record_meta(flat,
				rr->pickle + rr->pickle_sz, &opt_meta, ns->single_bin);

	if (flat_bins == NULL) {
		return false;
	}

	rr->void_time = opt_meta.void_time;
	rr->set_name = opt_meta.set_name;
	rr->set_name_len = opt_meta.set_name_len;
	rr->key = opt_meta.key;
	rr->key_size = opt_meta.key_size;
	rr->n_bins = (uint16_t)opt_meta.n_bins;
	rr->cm = opt_meta.cm;
	rr->meta_sz = (uint32_t)(flat_bins - rr->pickle);

	return true;
}

// Caller has already checked that end is within read buffer.
const uint8_t*
as_flat_unpack_record_meta(const as_flat_record* flat, const uint8_t* end,
		as_flat_opt_meta* opt_meta, bool single_bin)
{
	if (flat->unused != 0) {
		cf_warning(AS_FLAT, "unsupported storage fields");
		return NULL;
	}

	if (flat->generation == 0) {
		cf_warning(AS_FLAT, "generation 0");
		return NULL;
	}

	const uint8_t* at = flat->data;

	if (flat->has_void_time == 1) {
		if (at + sizeof(opt_meta->void_time) > end) {
			cf_warning(AS_FLAT, "incomplete void-time");
			return NULL;
		}

		opt_meta->void_time = *(uint32_t*)at;
		at += sizeof(opt_meta->void_time);
	}

	if (flat->has_set == 1) {
		if (at >= end) {
			cf_warning(AS_FLAT, "incomplete set name len");
			return NULL;
		}

		opt_meta->set_name_len = *at++;

		if (opt_meta->set_name_len == 0 ||
				opt_meta->set_name_len >= AS_SET_NAME_MAX_SIZE) {
			cf_warning(AS_FLAT, "bad set name len %u", opt_meta->set_name_len);
			return NULL;
		}

		opt_meta->set_name = (const char*)at;
		at += opt_meta->set_name_len;
	}

	if (flat->has_key == 1) {
		opt_meta->key_size = uintvar_parse(&at, end);

		if (opt_meta->key_size == 0) {
			cf_warning(AS_FLAT, "bad key size");
			return NULL;
		}

		opt_meta->key = (const uint8_t*)at;
		at += opt_meta->key_size;
	}

	if (flat->has_bins == 1) {
		if (single_bin) {
			opt_meta->n_bins = 1;
		}
		else {
			opt_meta->n_bins = uintvar_parse(&at, end);

			if (opt_meta->n_bins == 0 || opt_meta->n_bins > BIN_NAMES_QUOTA) {
				cf_warning(AS_FLAT, "bad n-bins %u", opt_meta->n_bins);
				return NULL;
			}
		}
	}

	at = unflatten_compression_meta(flat, at, end, &opt_meta->cm);

	if (at > end) {
		cf_warning(AS_FLAT, "incomplete record metadata");
		return NULL;
	}

	return at; // could be NULL, but would already have logged warning
}

int
as_flat_unpack_remote_bins(as_remote_record* rr, as_bin* bins)
{
	as_namespace* ns = rr->rsv->ns;
	const uint8_t* flat_bins = rr->pickle + rr->meta_sz;
	const uint8_t* end = rr->pickle + rr->pickle_sz;

	if (! as_flat_decompress_buffer(&rr->cm, ns->storage_write_block_size,
			&flat_bins, &end)) {
		cf_warning(AS_FLAT, "failed record decompression");
		return -AS_ERR_UNKNOWN;
	}

	return as_flat_unpack_bins(ns, flat_bins, end, rr->n_bins, bins);
}

int
as_flat_unpack_bins(as_namespace* ns, const uint8_t* at, const uint8_t* end,
		uint16_t n_bins, as_bin* bins)
{
	for (uint16_t i = 0; i < n_bins; i++) {
		if (at >= end) {
			cf_warning(AS_FLAT, "incomplete flat bin");
			return -AS_ERR_UNKNOWN;
		}

		if (! ns->single_bin) {
			size_t name_len = *at++;

			if (name_len >= AS_BIN_NAME_MAX_SZ) {
				cf_warning(AS_FLAT, "bad flat bin name");
				return -AS_ERR_UNKNOWN;
			}

			if (at + name_len > end) {
				cf_warning(AS_FLAT, "incomplete flat bin");
				return -AS_ERR_UNKNOWN;
			}

			if (! as_bin_set_id_from_name_w_len(ns, &bins[i], at, name_len)) {
				cf_warning(AS_FLAT, "flat bin name failed to assign id");
				return -AS_ERR_UNKNOWN;
			}

			at += name_len;
		}

		at = ns->storage_data_in_memory ?
				// FIXME - use an alloc instead of replace.
				as_bin_particle_replace_from_flat(&bins[i], at, end) :
				as_bin_particle_cast_from_flat(&bins[i], at, end);

		if (at == NULL) {
			return -AS_ERR_UNKNOWN;
		}
	}

	if (at > end) {
		cf_warning(AS_FLAT, "incomplete flat bin");
		return -AS_ERR_UNKNOWN;
	}

	// Some (but not all) callers pass end as an rblock-rounded value.
	if (at + RBLOCK_SIZE <= end) {
		cf_warning(AS_FLAT, "extra rblocks follow flat bin");
		return -AS_ERR_UNKNOWN;
	}

	return 0;
}

bool
as_flat_check_packed_bins(const uint8_t* at, const uint8_t* end,
		uint32_t n_bins, bool single_bin)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		if (at >= end) {
			cf_warning(AS_FLAT, "incomplete flat bin");
			return false;
		}

		if (! single_bin) {
			uint8_t name_len = *at++;

			if (name_len >= AS_BIN_NAME_MAX_SZ) {
				cf_warning(AS_FLAT, "bad flat bin name");
				return false;
			}

			at += name_len;
		}

		if (! (at = as_particle_skip_flat(at, end))) {
			return false;
		}
	}

	if (at > end) {
		cf_warning(AS_FLAT, "incomplete flat record");
		return false;
	}

	if (at + RBLOCK_SIZE <= end) {
		cf_warning(AS_FLAT, "extra rblocks follow flat record");
		return false;
	}

	return true;
}


//==========================================================
// Private API - for enterprise separation only.
//

uint8_t*
flatten_record_meta(const as_storage_rd* rd, uint32_t n_rblocks,
		const as_flat_comp_meta* cm, as_flat_record* flat)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	flat->magic = AS_FLAT_MAGIC;
	flat->n_rblocks = n_rblocks;
	// Flags are filled in below.
	flat->unused = 0;
	flat->tree_id = r->tree_id;
	flat->keyd = r->keyd;
	flat->last_update_time = r->last_update_time;
	flat->generation = r->generation;

	uint8_t* at = flat->data;

	if (r->void_time != 0) {
		*(uint32_t*)at = r->void_time;
		at += sizeof(uint32_t);

		flat->has_void_time = 1;
	}
	else {
		flat->has_void_time = 0;
	}

	if (rd->set_name) {
		*at++ = (uint8_t)rd->set_name_len;
		memcpy(at, rd->set_name, rd->set_name_len);
		at += rd->set_name_len;

		flat->has_set = 1;
	}
	else {
		flat->has_set = 0;
	}

	if (rd->key) {
		at = uintvar_pack(at, rd->key_size);
		memcpy(at, rd->key, rd->key_size);
		at += rd->key_size;

		flat->has_key = 1;
	}
	else {
		flat->has_key = 0;
	}

	// TODO - temporary, until we sort out the whole rd->n_bins mess.
	uint16_t n_used_bins = as_bin_inuse_count(rd);

	if (n_used_bins != 0) {
		if (! ns->single_bin) {
			at = uintvar_pack(at, n_used_bins);
		}

		flat->has_bins = 1;
	}
	else {
		flat->has_bins = 0;
	}

	return flatten_compression_meta(cm, flat, at);
}

uint16_t
flatten_bins(const as_storage_rd* rd, uint8_t* buf, uint32_t* sz)
{
	as_namespace* ns = rd->ns;

	uint8_t* start = buf;
	uint16_t n_bins;

	for (n_bins = 0; n_bins < rd->n_bins; n_bins++) {
		as_bin* bin = &rd->bins[n_bins];

		if (! as_bin_inuse(bin)) {
			break;
		}

		if (! ns->single_bin) {
			const char* bin_name = as_bin_get_name_from_id(ns, bin->id);
			size_t name_len = strlen(bin_name);

			*buf++ = (uint8_t)name_len;
			memcpy(buf, bin_name, name_len);
			buf += name_len;
		}

		buf += as_bin_particle_to_flat(bin, buf);
	}

	if (sz != NULL) {
		*sz = (uint32_t)(buf - start);
	}

	return n_bins;
}


//==========================================================
// Local helpers.
//

static uint32_t
flat_record_overhead_size(const as_storage_rd* rd)
{
	as_record* r = rd->r;

	// Start with size of record header struct.
	size_t size = sizeof(as_flat_record);

	if (r->void_time != 0) {
		size += sizeof(uint32_t);
	}

	if (rd->set_name) {
		size += 1 + rd->set_name_len;
	}

	if (rd->key) {
		size += uintvar_size(rd->key_size) + rd->key_size;
	}

	// TODO - size n_bins here when we sort out the whole rd->n_bins mess.
//	if (rd->n_bins != 0) {
//		size += uintvar_size(rd->n_bins);
//	}

	return (uint32_t)size;
}
