/*
 * drv_common.h
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "log.h"

#include "fabric/partition.h"
#include "storage/flat.h"


//==========================================================
// Forward declarations.
//

struct as_flat_opt_meta_s;
struct as_index_s;
struct as_namespace_s;


//==========================================================
// Typedefs & constants.
//

#define DRV_HEADER_OLD_MAGIC	(0x4349747275730707L)
#define DRV_HEADER_MAGIC		(0x4349747275730322L)
#define DRV_VERSION				3
// DRV_VERSION history:
// 1 - original
// 2 - minimum storage increment (RBLOCK_SIZE) from 512 to 128 bytes
// 3 - total overhaul including changed magic and moved version

// Device header flags.
#define DRV_HEADER_FLAG_TRUSTED				0x01
#define DRV_HEADER_FLAG_SINGLE_BIN			0x02
#define DRV_HEADER_FLAG_ENCRYPTED			0x04
#define DRV_HEADER_FLAG_CP					0x08
#define DRV_HEADER_FLAG_COMMIT_TO_DEVICE	0x10

// DRV_HEADER_SIZE must be a power of 2 and >= MAX_WRITE_BLOCK_SIZE.
// Do NOT change DRV_HEADER_SIZE!
#define DRV_HEADER_SIZE (8 * 1024 * 1024)

// Size rounding needed for sanity checking.
#define DRV_RECORD_MIN_SIZE \
	(((uint32_t)sizeof(as_flat_record) + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE)

#define DRV_DEFRAG_RESERVE 8

// Used when determining a device's io_min_size.
#define LO_IO_MIN_SIZE 512
#define HI_IO_MIN_SIZE 4096

typedef struct drv_prefix_s {
	uint64_t	magic;
	uint32_t	version;
	char		namespace[32];
	uint32_t	n_devices;
	uint64_t	random; // identify matching set of devices
	uint32_t	flags;
	uint32_t	write_block_size;
	uint32_t	eventual_regime;
	uint32_t	last_evict_void_time;
	uint32_t	roster_generation;
} drv_prefix;

// Because we pad explicitly:
COMPILER_ASSERT(sizeof(drv_prefix) <= HI_IO_MIN_SIZE);

// TODO - deal with the name and the name of as_storage_info_set/get!
typedef struct drv_pmeta_s {
	as_partition_version version;
	uint8_t		tree_id;
	uint8_t		unused[7];
} drv_pmeta;

// Make sure a drv_pmeta never unnecessarily crosses an IO size boundary.
COMPILER_ASSERT((sizeof(drv_pmeta) & (sizeof(drv_pmeta) - 1)) == 0);

typedef struct drv_generic_s {
	drv_prefix	prefix;
	uint8_t		pad_prefix[HI_IO_MIN_SIZE - sizeof(drv_prefix)];
	drv_pmeta	pmeta[AS_PARTITIONS];
} drv_generic;

typedef struct drv_unique_s {
	uint32_t	device_id;
	uint32_t	unused;
	uint8_t		encrypted_key[64];
	uint8_t		canary[16];
	uint64_t	pristine_offset;
} drv_unique;

typedef struct drv_atomic_s {
	size_t		size;
	off_t		offset;
	uint8_t		data[(128 * 1024) - (sizeof(size_t) + sizeof(off_t))];
} drv_atomic;

COMPILER_ASSERT(sizeof(drv_atomic) == 128 * 1024);

#define ROUND_UP_GENERIC \
	((sizeof(drv_generic) + (HI_IO_MIN_SIZE - 1)) & -HI_IO_MIN_SIZE)

#define ROUND_UP_UNIQUE \
	((sizeof(drv_atomic) + (HI_IO_MIN_SIZE - 1)) & -HI_IO_MIN_SIZE)

typedef struct drv_header_s {
	drv_generic	generic;
	uint8_t		pad_generic[ROUND_UP_GENERIC - sizeof(drv_generic)];
	drv_unique	unique;
	uint8_t		pad_unique[ROUND_UP_UNIQUE - sizeof(drv_unique)];
	drv_atomic	atomic;
} drv_header;

COMPILER_ASSERT(sizeof(drv_header) <= DRV_HEADER_SIZE);

COMPILER_ASSERT(offsetof(drv_header, generic) == 0);
COMPILER_ASSERT(offsetof(drv_header, generic.prefix) == 0);

#define DRV_OFFSET_UNIQUE (offsetof(drv_header, unique))

#define STORAGE_INVALID_WBLOCK 0xFFFFffff

// Ultimately this may become a full-blown state, but for now it's effectively
// just a defrag flag.
#define WBLOCK_STATE_NONE		0
#define WBLOCK_STATE_DEFRAG		1


//==========================================================
// Inlines and macros.
//

//
// Conversions between offsets and rblocks.
//

// TODO - make checks stricter (exclude drive header, consider drive size) ???
#define STORAGE_RBLOCK_IS_VALID(__x) ((__x) != 0)
#define STORAGE_RBLOCK_IS_INVALID(__x) ((__x) == 0)

// Convert byte offset to rblock_id, as long as offset is already a multiple of
// rblock size.
static inline uint64_t
OFFSET_TO_RBLOCK_ID(uint64_t offset)
{
	return offset >> LOG_2_RBLOCK_SIZE;
}

// Convert rblock_id to byte offset.
static inline uint64_t
RBLOCK_ID_TO_OFFSET(uint64_t rblocks)
{
	return rblocks << LOG_2_RBLOCK_SIZE;
}


//==========================================================
// Public API - shared code between storage engines.
//

bool drv_is_set_evictable(const struct as_namespace_s* ns, const struct as_flat_opt_meta_s* opt_meta);
void drv_apply_opt_meta(struct as_index_s* r, struct as_namespace_s* ns, const struct as_flat_opt_meta_s* opt_meta);
bool pread_all(int fd, void* buf, size_t size, off_t offset);
bool pwrite_all(int fd, const void* buf, size_t size, off_t offset);
