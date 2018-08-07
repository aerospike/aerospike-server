/*
 * drv_ssd.h
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "fault.h"
#include "hist.h"

#include "base/datamodel.h"
#include "fabric/partition.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_namespace_s;
struct as_rec_props_s;
struct as_storage_rd_s;
struct drv_ssd_s;


//==========================================================
// Typedefs & constants.
//

// Linux has removed O_DIRECT, but not its functionality.
#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

#define SSD_HEADER_OLD_MAGIC	(0x4349747275730707L)
#define SSD_HEADER_MAGIC		(0x4349747275730322L)
#define SSD_VERSION				3
// SSD_VERSION history:
// 1 - original
// 2 - minimum storage increment (RBLOCK_SIZE) from 512 to 128 bytes
// 3 - total overhaul including changed magic and moved version

// Device header flags.
#define SSD_HEADER_FLAG_TRUSTED				0x01
#define SSD_HEADER_FLAG_SINGLE_BIN			0x02
#define SSD_HEADER_FLAG_ENCRYPTED			0x04
#define SSD_HEADER_FLAG_CP					0x08
#define SSD_HEADER_FLAG_COMMIT_TO_DEVICE	0x10

// Used when determining a device's io_min_size.
#define LO_IO_MIN_SIZE 512
#define HI_IO_MIN_SIZE 4096

// SSD_HEADER_SIZE must be a power of 2 and >= MAX_WRITE_BLOCK_SIZE.
// Do NOT change SSD_HEADER_SIZE!
#define SSD_HEADER_SIZE (8 * 1024 * 1024)

// FIXME - remove when fmt-chg is done and no COMPILER_ASSERT collisions.


//------------------------------------------------
// Device header.
//

typedef struct ssd_common_prefix_s {
	uint64_t	magic;
	uint32_t	version;
	char		namespace[32];
	uint32_t	n_devices;
	uint64_t	random; // identify matching set of devices
	uint32_t	flags;
	uint32_t	write_block_size;
	uint32_t	eventual_regime;
	uint32_t	last_evict_void_time;
} ssd_common_prefix;

// Because we pad explicitly:
COMPILER_ASSERT(sizeof(ssd_common_prefix) <= HI_IO_MIN_SIZE);

// FIXME - deal with the name and the name of as_storage_info_set/get!
typedef struct ssd_common_pmeta_s {
	as_partition_version version;
	uint8_t		tree_id;
	uint8_t		unused[7];
} ssd_common_pmeta;

// Make sure a ssd_common_pmeta never unnecessarily crosses an IO size boundary.
COMPILER_ASSERT((sizeof(ssd_common_pmeta) & (sizeof(ssd_common_pmeta) - 1)) == 0);

typedef struct ssd_device_common_s {
	ssd_common_prefix	prefix;
	uint8_t				pad_prefix[HI_IO_MIN_SIZE - sizeof(ssd_common_prefix)];
	ssd_common_pmeta	pmeta[AS_PARTITIONS];
} ssd_device_common;

typedef struct ssd_device_unique_s {
	uint32_t	device_id;
	uint32_t	unused;
	uint8_t		encrypted_key[32];
	uint8_t		canary[16];
} ssd_device_unique;

#define ROUND_UP_COMMON \
	((sizeof(ssd_device_common) + (HI_IO_MIN_SIZE - 1)) & -HI_IO_MIN_SIZE)

typedef struct ssd_device_header_s {
	ssd_device_common	common;
	uint8_t				pad_common[ROUND_UP_COMMON - sizeof(ssd_device_common)];
	ssd_device_unique	unique;
} ssd_device_header;

COMPILER_ASSERT(sizeof(ssd_device_header) <= SSD_HEADER_SIZE);

COMPILER_ASSERT(offsetof(ssd_device_header, common) == 0);
COMPILER_ASSERT(offsetof(ssd_device_header, common.prefix) == 0);

#define SSD_OFFSET_UNIQUE (offsetof(ssd_device_header, unique))


//------------------------------------------------
// A defragged wblock waiting to be freed.
//
typedef struct vacated_wblock_s {
	uint32_t file_id;
	uint32_t wblock_id;
} vacated_wblock;


//------------------------------------------------
// Write buffer - where records accumulate until
// (the full buffer is) flushed to a device.
//
typedef struct {
	cf_atomic32			rc;
	cf_atomic32			n_writers;	// number of concurrent writers
	bool				skip_post_write_q;
	uint32_t			n_vacated;
	uint32_t			vacated_capacity;
	vacated_wblock		*vacated_wblocks;
	struct drv_ssd_s	*ssd;
	uint32_t			wblock_id;
	uint32_t			pos;
	uint8_t				*buf;
} ssd_write_buf;


//------------------------------------------------
// Per-wblock information.
//
typedef struct ssd_wblock_state_s {
	cf_atomic32			inuse_sz;	// number of bytes currently used in the wblock
	cf_mutex			LOCK;		// transactions, write_worker, and defrag all are interested in wblock_state
	ssd_write_buf		*swb;		// pending writes for the wblock, also treated as a cache for reads
	uint32_t			state;		// for now just a defrag flag
	cf_atomic32			n_vac_dests; // number of wblocks into which this wblock defragged
} ssd_wblock_state;

// wblock state
//
// Ultimately this may become a full-blown state, but for now it's effectively
// just a defrag flag.
#define WBLOCK_STATE_NONE		0
#define WBLOCK_STATE_DEFRAG		1


//------------------------------------------------
// Per-device information about its wblocks.
//
typedef struct ssd_alloc_table_s {
	uint32_t			n_wblocks;		// number allocated below
	ssd_wblock_state	wblock_state[];
} ssd_alloc_table;


//------------------------------------------------
// Where on free_wblock_q freed wblocks go.
//
typedef enum {
	FREE_TO_HEAD,
	FREE_TO_TAIL
} e_free_to;


//------------------------------------------------
// Per-device information.
//
typedef struct drv_ssd_s {
	struct as_namespace_s *ns;

	const char		*name;				// this device's name
	const char		*shadow_name;		// this device's shadow's name, if any

	uint32_t		running;

	pthread_mutex_t	write_lock;			// lock protects writes to current swb
	ssd_write_buf	*current_swb;		// swb currently being filled by writes

	int				commit_fd;			// relevant for enterprise edition only
	int				shadow_commit_fd;	// relevant for enterprise edition only

	pthread_mutex_t	defrag_lock;		// lock protects writes to defrag swb
	ssd_write_buf	*defrag_swb;		// swb currently being filled by defrag

	cf_queue		*fd_q;				// queue of open fds
	cf_queue		*shadow_fd_q;		// queue of open fds on shadow, if any

	cf_queue		*free_wblock_q;		// IDs of free wblocks
	cf_queue		*defrag_wblock_q;	// IDs of wblocks to defrag

	cf_queue		*swb_write_q;		// pointers to swbs ready to write
	cf_queue		*swb_shadow_q;		// pointers to swbs ready to write to shadow, if any
	cf_queue		*swb_free_q;		// pointers to swbs free and waiting
	cf_queue		*post_write_q;		// pointers to swbs that have been written but are cached

	uint8_t			encryption_key[32];		// relevant for enterprise edition only

	cf_atomic64		n_defrag_wblock_reads;	// total number of wblocks added to the defrag_wblock_q
	cf_atomic64		n_defrag_wblock_writes;	// total number of swbs added to the swb_write_q by defrag
	cf_atomic64		n_wblock_writes;		// total number of swbs added to the swb_write_q by writes

	cf_atomic64		n_wblock_defrag_io_skips;	// total number of wblocks empty on defrag_wblock_q pop
	cf_atomic64		n_wblock_direct_frees;		// total number of wblocks freed by other than defrag

	volatile uint64_t n_tomb_raider_reads;	// relevant for enterprise edition only

	cf_atomic32		defrag_sweep;		// defrag sweep flag

	uint64_t		file_size;
	int				file_id;

	uint32_t		open_flag;
	bool			data_in_memory;

	uint64_t		io_min_size;		// device IO operations are aligned and sized in multiples of this
	uint64_t		shadow_io_min_size;	// shadow device IO operations are aligned and sized in multiples of this

	uint64_t		commit_min_size;		// commit (write) operations are aligned and sized in multiples of this
	uint64_t		shadow_commit_min_size;	// shadow commit (write) operations are aligned and sized in multiples of this

	cf_atomic64		inuse_size;			// number of bytes in actual use on this device

	uint32_t		write_block_size;	// number of bytes to write at a time

	uint32_t		sweep_wblock_id;				// wblocks read at startup
	uint64_t		record_add_older_counter;		// records not inserted due to better existing one
	uint64_t		record_add_expired_counter;		// records not inserted due to expiration
	uint64_t		record_add_max_ttl_counter;		// records not inserted due to max-ttl
	uint64_t		record_add_replace_counter;		// records reinserted
	uint64_t		record_add_unique_counter;		// records inserted

	ssd_alloc_table	*alloc_table;

	pthread_t		write_worker_thread;
	pthread_t		shadow_worker_thread;

	histogram		*hist_read;
	histogram		*hist_large_block_read;
	histogram		*hist_write;
	histogram		*hist_shadow_write;
	histogram		*hist_fsync;
} drv_ssd;


//------------------------------------------------
// Per-namespace storage information.
//
typedef struct drv_ssds_s {
	struct as_namespace_s	*ns;
	ssd_device_common		*common;

	// Not a great place for this - used only at startup to determine whether to
	// load a record.
	bool get_state_from_storage[AS_PARTITIONS];

	// Indexed by previous device-id to get new device-id. -1 means device is
	// "fresh" or absent. Used only at startup to fix index elements' file-id.
	int8_t device_translation[AS_STORAGE_MAX_DEVICES];

	// Used only at startup, set true if all devices are fresh.
	bool all_fresh;

	cf_mutex			flush_lock;

	int					n_ssds;
	drv_ssd				ssds[];
} drv_ssds;


//==========================================================
// Private API - for enterprise separation only.
//

// Artificial limit on write-block-size, in case we ever move to an
// SSD_HEADER_SIZE that's too big to be a write-block size limit.
// MAX_WRITE_BLOCK_SIZE must be power of 2 and <= SSD_HEADER_SIZE.
#define MAX_WRITE_BLOCK_SIZE	(8 * 1024 * 1024)

// Artificial limit on write-block-size, must be power of 2 and >= RBLOCK_SIZE.
#define MIN_WRITE_BLOCK_SIZE	(1024 * 1)

#define SSD_BLOCK_MAGIC 0x037AF201 // changed for storage version 3

typedef struct ssd_load_records_info_s {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_rc;
} ssd_load_records_info;

// Per-record mandatory metadata on device.
typedef struct ssd_record_s {
	uint32_t magic;

	// offset: 4
	uint32_t n_rblocks: 19;
	uint32_t has_void_time: 1;
	uint32_t has_set: 1;
	uint32_t has_key: 1;
	uint32_t has_bins: 1; // i.e. is live
	uint32_t unused: 3;
	uint32_t tree_id: 6;

	// offset: 8
	cf_digest keyd;

	// offset: 28
	uint64_t last_update_time: 40;
	uint64_t generation: 16;

	// final size: 35
	uint8_t data[];
} __attribute__ ((__packed__)) ssd_record;

// Per-record optional metadata container.
typedef struct ssd_rec_props_s {
	uint32_t void_time;
	uint32_t set_name_len;
	const char *set_name;
	uint32_t key_size;
	const uint8_t *key;
	uint32_t n_bins;
} ssd_rec_props;

// Warm and cool restart.
void ssd_resume_devices(drv_ssds *ssds);
void *run_ssd_cool_start(void *udata);
void ssd_load_wblock_queues(drv_ssds *ssds);
void ssd_start_maintenance_threads(drv_ssds *ssds);
void ssd_start_write_worker_threads(drv_ssds *ssds);
void ssd_start_defrag_threads(drv_ssds *ssds);
const uint8_t* ssd_read_record_meta(const ssd_record* block, const uint8_t* end, ssd_rec_props* props, bool single_bin);
bool ssd_check_bins(const uint8_t* p_read, const uint8_t* end, uint32_t n_bins, bool single_bin); // TODO - demote when cool start unwinds on failure
void apply_rec_props(struct as_index_s *r, struct as_namespace_s *ns, const ssd_rec_props *props);

// Tomb raider.
void ssd_cold_start_adjust_cenotaph(struct as_namespace_s *ns, bool block_has_bins, uint32_t block_void_time, struct as_index_s *r);
void ssd_cold_start_transition_record(struct as_namespace_s *ns, const ssd_record *block, struct as_index_s *r, bool is_create);
void ssd_cold_start_drop_cenotaphs(struct as_namespace_s *ns);

// Record encryption.
void ssd_init_encryption_key(struct as_namespace_s *ns);
void ssd_do_encrypt(const uint8_t *key, uint64_t off, ssd_record *block);
void ssd_do_decrypt(const uint8_t *key, uint64_t off, ssd_record *block);
void ssd_do_decrypt_whole(const uint8_t* key, uint64_t off, uint32_t n_rblocks, ssd_record* block);

// CP.
void ssd_adjust_versions(struct as_namespace_s *ns, ssd_common_pmeta* pmeta);
conflict_resolution_pol ssd_cold_start_policy(struct as_namespace_s *ns);
void ssd_cold_start_init_repl_state(struct as_namespace_s *ns, struct as_index_s* r);

// Miscellaneous.
void ssd_header_init_cfg(const struct as_namespace_s *ns, drv_ssd* ssd, ssd_device_header *header);
void ssd_header_validate_cfg(const struct as_namespace_s *ns, drv_ssd* ssd, const ssd_device_header *header);
void ssd_flush_final_cfg(struct as_namespace_s *ns);
bool ssd_cold_start_is_valid_n_bins(uint32_t n_bins);
void ssd_write_header(drv_ssd *ssd, uint8_t *header, uint8_t *from, size_t size);
void ssd_prefetch_wblock(drv_ssd *ssd, uint64_t file_offset, uint8_t *read_buf);

// Durability.
void ssd_init_commit(drv_ssd *ssd);
uint64_t ssd_flush_max_us(const struct as_namespace_s *ns);
void ssd_post_write(drv_ssd *ssd, ssd_write_buf *swb);
int ssd_write_bins(struct as_storage_rd_s *rd);
int ssd_buffer_bins(struct as_storage_rd_s *rd);
uint32_t ssd_record_size(struct as_storage_rd_s *rd);
void ssd_flatten_record(const struct as_storage_rd_s *rd, uint32_t n_rblocks, ssd_record *block);
ssd_write_buf *swb_get(drv_ssd *ssd);

// Called in (enterprise-split) storage table function.
int ssd_write(struct as_storage_rd_s *rd);


//
// Conversions between bytes and rblocks.
//

// TODO - make checks stricter (exclude drive header, consider drive size) ???
#define STORAGE_RBLOCK_IS_VALID(__x)	((__x) != 0)
#define STORAGE_RBLOCK_IS_INVALID(__x)	((__x) == 0)

#define RBLOCK_SIZE			16	// 2^4
#define LOG_2_RBLOCK_SIZE	4	// must be in sync with RBLOCK_SIZE

// Round size in bytes up to a multiple of rblock size.
static inline uint32_t SIZE_UP_TO_RBLOCK_SIZE(uint32_t size) {
	return (size + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE;
}

// Convert byte offset to rblock_id, as long as offset is already a multiple of
// rblock size.
static inline uint64_t OFFSET_TO_RBLOCK_ID(uint64_t offset) {
	return offset >> LOG_2_RBLOCK_SIZE;
}

// Convert rblock_id to byte offset.
static inline uint64_t RBLOCK_ID_TO_OFFSET(uint64_t rblocks) {
	return rblocks << LOG_2_RBLOCK_SIZE;
}

// Convert size in bytes to n_rblocks as long as size is already a multiple of
// rblock size.
static inline uint32_t SIZE_TO_N_RBLOCKS(uint32_t size) {
	return (size >> LOG_2_RBLOCK_SIZE) - 1;
}

// Convert n_rblocks to size in bytes.
static inline uint32_t N_RBLOCKS_TO_SIZE(uint32_t n_rblocks) {
	return (n_rblocks + 1) << LOG_2_RBLOCK_SIZE;
}


//
// Conversions between bytes/rblocks and wblocks.
//

#define STORAGE_INVALID_WBLOCK 0xFFFFffff

// Convert byte offset to wblock_id.
static inline uint32_t OFFSET_TO_WBLOCK_ID(drv_ssd *ssd, uint64_t offset) {
	return (uint32_t)(offset / ssd->write_block_size);
}

// Convert wblock_id to byte offset.
static inline uint64_t WBLOCK_ID_TO_OFFSET(drv_ssd *ssd, uint32_t wblock_id) {
	return (uint64_t)wblock_id * (uint64_t)ssd->write_block_size;
}

// Convert rblock_id to wblock_id.
static inline uint32_t RBLOCK_ID_TO_WBLOCK_ID(drv_ssd *ssd, uint64_t rblock_id) {
	return (uint32_t)((rblock_id << LOG_2_RBLOCK_SIZE) / ssd->write_block_size);
}


//
// Size rounding needed for sanity checking.
//

#define SSD_RECORD_MIN_SIZE \
	(((uint32_t)sizeof(ssd_record) + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE)


//
// Size rounding needed for direct IO.
//

// Round bytes down to a multiple of device's minimum IO operation size.
static inline uint64_t BYTES_DOWN_TO_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return bytes & -ssd->io_min_size;
}

// Round bytes up to a multiple of device's minimum IO operation size.
static inline uint64_t BYTES_UP_TO_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return (bytes + (ssd->io_min_size - 1)) & -ssd->io_min_size;
}

// Round bytes down to a multiple of shadow device's minimum IO operation size.
static inline uint64_t
BYTES_DOWN_TO_SHADOW_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return bytes & -ssd->shadow_io_min_size;
}

// Round bytes up to a multiple of shadow device's minimum IO operation size.
static inline uint64_t
BYTES_UP_TO_SHADOW_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return (bytes + (ssd->shadow_io_min_size - 1)) & -ssd->shadow_io_min_size;
}


//
// Record encryption.
//

static inline void
ssd_encrypt(drv_ssd *ssd, uint64_t off, ssd_record *block)
{
	if (ssd->ns->storage_encryption_key_file != NULL) {
		ssd_do_encrypt(ssd->encryption_key, off, block);
	}
}

static inline void
ssd_decrypt(drv_ssd *ssd, uint64_t off, ssd_record *block)
{
	if (ssd->ns->storage_encryption_key_file != NULL) {
		ssd_do_decrypt(ssd->encryption_key, off, block);
	}
}

static inline void
ssd_decrypt_whole(drv_ssd *ssd, uint64_t off, uint32_t n_rblocks,
		ssd_record *block)
{
	if (ssd->ns->storage_encryption_key_file != NULL) {
		ssd_do_decrypt_whole(ssd->encryption_key, off, n_rblocks, block);
	}
}
