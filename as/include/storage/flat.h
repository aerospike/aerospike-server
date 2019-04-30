/*
 * flat.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_digest.h"


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_namespace_s;
struct as_remote_record_s;
struct as_storage_rd_s;


//==========================================================
// Typedefs & constants.
//

#define AS_FLAT_MAGIC 0x037AF201 // changed for storage version 3

// Per-record mandatory metadata on device.
typedef struct as_flat_record_s {
	uint32_t magic;

	// offset: 4
	uint32_t n_rblocks: 19; // unused if storage-engine memory
	uint32_t has_void_time: 1;
	uint32_t has_set: 1;
	uint32_t has_key: 1;
	uint32_t has_bins: 1; // i.e. is live
	uint32_t is_compressed: 1;
	uint32_t unused: 2;
	uint32_t tree_id: 6; // for local storage only

	// offset: 8
	cf_digest keyd;

	// offset: 28
	uint64_t last_update_time: 40;
	uint64_t generation: 16;

	// final size: 35
	uint8_t data[];
} __attribute__ ((__packed__)) as_flat_record;

typedef enum {
	AS_COMPRESSION_NONE,
	AS_COMPRESSION_LZ4,
	AS_COMPRESSION_SNAPPY,
	AS_COMPRESSION_ZSTD,

	AS_COMPRESSION_LAST_PLUS_1
} as_compression_method;

#define NS_COMPRESSION() ({ \
		as_compression_method meth = as_load_int32(&ns->storage_compression); \
		(meth == AS_COMPRESSION_NONE ? "none" : \
			(meth == AS_COMPRESSION_LZ4 ? "lz4" : \
				(meth == AS_COMPRESSION_SNAPPY ? "snappy" : \
					(meth == AS_COMPRESSION_ZSTD ? "zstd" : \
						"illegal")))); \
	})

// Compression metadata - relevant for enterprise only.
typedef struct ssd_comp_meta_s {
	as_compression_method method;
	uint32_t orig_sz;
	uint32_t comp_sz;
} as_flat_comp_meta;

// Per-record optional metadata container.
typedef struct as_flat_opt_meta_s {
	uint32_t void_time;
	uint32_t set_name_len;
	const char* set_name;
	uint32_t key_size;
	const uint8_t* key;
	uint32_t n_bins;
	as_flat_comp_meta cm;
} as_flat_opt_meta;

#define RBLOCK_SIZE			16	// 2^4
#define LOG_2_RBLOCK_SIZE	4	// must be in sync with RBLOCK_SIZE


//==========================================================
// Public API.
//

void as_flat_pickle_record(struct as_storage_rd_s* rd);
uint32_t as_flat_record_size(const struct as_storage_rd_s* rd);
void as_flat_pack_record(const struct as_storage_rd_s* rd, uint32_t n_rblocks, as_flat_record* flat);

as_flat_record* as_flat_compress_bins_and_pack_record(const struct as_storage_rd_s* rd, uint32_t max_orig_sz, uint32_t* flat_sz);

bool as_flat_unpack_remote_record_meta(struct as_namespace_s* ns, struct as_remote_record_s* rr);
const uint8_t* as_flat_unpack_record_meta(const as_flat_record* flat, const uint8_t* end, struct as_flat_opt_meta_s* opt_meta, bool single_bin);
int as_flat_unpack_remote_bins(struct as_remote_record_s* rr, struct as_bin_s* bins);
int as_flat_unpack_bins(struct as_namespace_s* ns, const uint8_t* at, const uint8_t* end, uint16_t n_bins, struct as_bin_s* bins);
bool as_flat_check_packed_bins(const uint8_t* at, const uint8_t* end, uint32_t n_bins, bool single_bin);

uint32_t as_flat_orig_pickle_size(const struct as_remote_record_s* rr, uint32_t pickle_sz);
bool as_flat_decompress_bins(const as_flat_comp_meta* cm, struct as_storage_rd_s* rd);
bool as_flat_decompress_buffer(const as_flat_comp_meta* cm, uint32_t max_orig_sz, const uint8_t** at, const uint8_t** end);

// Round size in bytes up to a multiple of rblock size.
static inline uint32_t
SIZE_UP_TO_RBLOCK_SIZE(uint32_t size) {
	return (size + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE;
}

// Convert size in bytes to n_rblocks.
static inline uint32_t
SIZE_TO_N_RBLOCKS(uint32_t size) {
	return ((size + (RBLOCK_SIZE - 1)) >> LOG_2_RBLOCK_SIZE) - 1;
}

// Convert size in bytes to n_rblocks - size must be a multiple of rblock size.
static inline uint32_t
ROUNDED_SIZE_TO_N_RBLOCKS(uint32_t size) {
	return (size >> LOG_2_RBLOCK_SIZE) - 1;
}

// Convert n_rblocks to size in bytes.
static inline uint32_t
N_RBLOCKS_TO_SIZE(uint32_t n_rblocks) {
	return (n_rblocks + 1) << LOG_2_RBLOCK_SIZE;
}


//==========================================================
// Private API - for enterprise separation only.
//

uint8_t* flatten_record_meta(const struct as_storage_rd_s* rd, uint32_t n_rblocks, const as_flat_comp_meta* cm, as_flat_record* flat);
uint16_t flatten_bins(const struct as_storage_rd_s* rd, uint8_t* buf, uint32_t* sz);

uint8_t* flatten_compression_meta(const as_flat_comp_meta* cm, as_flat_record* flat, uint8_t* buf);
const uint8_t* unflatten_compression_meta(const as_flat_record* flat, const uint8_t* at, const uint8_t* end, as_flat_comp_meta* cm);
