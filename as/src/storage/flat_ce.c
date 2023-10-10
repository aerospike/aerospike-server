/*
 * flat_ce.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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

#include "log.h"

#include "base/datamodel.h"
#include "storage/storage.h"


//==========================================================
// Public API.
//

as_flat_record*
as_flat_compress_bins_and_pack_record(const as_storage_rd* rd,
		uint32_t max_orig_sz, bool dirty, bool will_mark_end, uint32_t* flat_sz)
{
	return NULL;
}

uint32_t
as_flat_orig_pickle_size(const as_remote_record* rr, uint32_t pickle_sz)
{
	return pickle_sz;
}

bool
as_flat_skip_bins(const as_flat_comp_meta* cm, as_storage_rd* rd)
{
	rd->flat_end = as_flat_check_packed_bins(rd->flat_bins, rd->flat_end,
			rd->flat_n_bins);

	return rd->flat_end != NULL;
}

bool
as_flat_decompress_bins(const as_flat_comp_meta *cm, as_storage_rd *rd)
{
	return true;
}

bool
as_flat_decompress_buffer(const as_flat_comp_meta* cm, uint32_t max_orig_sz,
		const uint8_t** at, const uint8_t** end, const uint8_t** cb_end)
{
	return true;
}


//==========================================================
// Private API - for enterprise separation only.
//

uint8_t*
flatten_compression_meta(const as_flat_comp_meta* cm, as_flat_record* flat,
		uint8_t* buf)
{
	flat->is_compressed = 0;

	return buf;
}

const uint8_t*
unflatten_compression_meta(const as_flat_record* flat, const uint8_t* at,
		const uint8_t* end, as_flat_comp_meta* cm)
{
	if (flat->is_compressed == 0) {
		return at;
	}

	cf_warning(AS_FLAT, "community edition skipped compressed record %pD",
			&flat->keyd);

	return NULL;
}

void
set_remote_record_xdr_flags(const as_flat_record* flat,
		const as_flat_extra_flags* extra_flags, as_remote_record* rr)
{
}

void
set_flat_xdr_state(const as_record* r, as_flat_record* flat)
{
	// Don't store junk bit - may upgrade to EE, or downgrade -> 4.9 -> 4.8-.
	flat->xdr_write = 0;
}

as_flat_extra_flags
get_flat_extra_flags(const as_record* r)
{
	// So far all extra flags are enterprise-only.
	return (as_flat_extra_flags){ 0 };
}

void
unpack_bin_xdr_write(uint8_t flags, as_bin* b)
{
}

const uint8_t*
unpack_bin_src_id(uint8_t flags, const uint8_t* at, const uint8_t* end,
		as_bin* b)
{
	return at;
}

const uint8_t*
skip_bin_src_id(uint8_t flags, const uint8_t* at, const uint8_t* end)
{
	return at;
}

void
flatten_bin_xdr_write(const as_bin* b, uint8_t* flags)
{
}

uint32_t
bin_src_id_flat_size(const as_bin* b)
{
	return 0;
}

uint32_t
flatten_bin_src_id(const as_bin* b, uint8_t* flags, uint8_t* at)
{
	return 0;
}
