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

/*
 * Common header for drv_ssd.c, drv_ssd_cold.c, drv_ssd_warm.c.
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
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

#define SSD_HEADER_MAGIC	(0x4349747275730707L)
#define SSD_VERSION			2
// Must update conversion code when bumping version.
//
// SSD_VERSION history:
// 1 - original
// 2 - minimum storage increment (RBLOCK_SIZE) from 512 to 128 bytes

// Device header flags.
#define SSD_HEADER_FLAG_ENCRYPTED	0x01
#define SSD_HEADER_FLAG_CP			0x02
#define SSD_HEADER_FLAG_TRUSTED		0x04


//------------------------------------------------
// Device header.
//
typedef struct {
	uint64_t	magic;			// shows we've got the right stuff
	uint64_t	random;			// a random value - good for telling all disks are of the same state
	uint32_t	write_block_size;
	uint32_t	last_evict_void_time;
	uint8_t		version;
	uint8_t		flags;
	uint16_t	devices_n;		// number of devices
	uint32_t	header_length;
	char		namespace[32];	// ascii representation of the namespace name, null-terminated
	uint32_t	info_n;			// number of info slices (should be > a reasonable partition count)
	uint32_t	info_stride;	// currently 128 bytes
	uint8_t		info_data[];
} __attribute__((__packed__)) ssd_device_header;


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
typedef struct drv_ssd_s
{
	struct as_namespace_s *ns;

	char			*name;				// this device's name
	char			*shadow_name;		// this device's shadow's name, if any

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
	bool			started_fresh;		// relevant only for warm or cool restart

	uint64_t		io_min_size;		// device IO operations are aligned and sized in multiples of this
	uint64_t		commit_min_size;	// commit (write) operations are aligned and sized in multiples of this

	cf_atomic64		inuse_size;			// number of bytes in actual use on this device

	uint32_t		write_block_size;	// number of bytes to write at a time

	uint32_t		sweep_wblock_id;				// wblocks read at startup
	uint64_t		record_add_older_counter;		// records not inserted due to better existing one
	uint64_t		record_add_expired_counter;		// records not inserted due to expiration
	uint64_t		record_add_max_ttl_counter;		// records not inserted due to max-ttl
	uint64_t		record_add_replace_counter;		// records reinserted
	uint64_t		record_add_unique_counter;		// records inserted

	ssd_alloc_table	*alloc_table;

	pthread_t		maintenance_thread;
	pthread_t		write_worker_thread;
	pthread_t		shadow_worker_thread;
	pthread_t		defrag_thread;

	histogram		*hist_read;
	histogram		*hist_large_block_read;
	histogram		*hist_write;
	histogram		*hist_shadow_write;
	histogram		*hist_fsync;
} drv_ssd;


//------------------------------------------------
// Per-namespace storage information.
//
typedef struct drv_ssds_s
{
	ssd_device_header		*header;
	struct as_namespace_s	*ns;

	// Not a great place for this - used only at startup to determine whether to
	// load a record.
	bool get_state_from_storage[AS_PARTITIONS];

	int					n_ssds;
	drv_ssd				ssds[];
} drv_ssds;


//==========================================================
// Private API - for enterprise separation only
//

// SSD_HEADER_SIZE must be a power of 2 and >= MAX_WRITE_BLOCK_SIZE.
// Do NOT change SSD_HEADER_SIZE!
#define SSD_HEADER_SIZE			(1024 * 1024)

// Artificial limit on write-block-size, in case we ever move to an
// SSD_HEADER_SIZE that's too big to be a write-block size limit.
// MAX_WRITE_BLOCK_SIZE must be power of 2 and <= SSD_HEADER_SIZE.
#define MAX_WRITE_BLOCK_SIZE	(1024 * 1024)

// Artificial limit on write-block-size, must be power of 2 and >= RBLOCK_SIZE.
#define MIN_WRITE_BLOCK_SIZE	(1024 * 1)

#define SSD_BLOCK_MAGIC		0x037AF200
#define LENGTH_BASE			offsetof(struct drv_ssd_block_s, keyd)

#define SSD_HEADER_INFO_STRIDE 128

typedef struct ssd_load_records_info_s {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_udata;
	void *complete_rc;
} ssd_load_records_info;

// Per-record metadata on device.
typedef struct drv_ssd_block_s {
	uint64_t		sig;			// deprecated
	uint32_t		magic;
	uint32_t		length;			// total after this field - this struct's pointer + 16
	cf_digest		keyd;
	uint32_t		generation;
	cf_clock		void_time;
	uint32_t		bins_offset;	// offset to bins from data
	uint32_t		n_bins;
	uint64_t		last_update_time;
	uint8_t			data[];
} __attribute__ ((__packed__)) drv_ssd_block;

// Per-bin metadata on device.
typedef struct drv_ssd_bin_s {
	char		name[AS_ID_BIN_SZ];	// 15 aligns well
	uint8_t		version;			// now unused
	uint32_t	offset;				// offset of bin data within block
	uint32_t	len;				// size of bin data
	uint32_t	next;				// location of next bin: block offset
} __attribute__ ((__packed__)) drv_ssd_bin;

// Info slice in device header block.
typedef struct info_buf_s {
	uint32_t regime; // used to be len, but was never read
	as_partition_version version;
} __attribute__ ((__packed__)) info_buf;

// Warm and cool restart.
void ssd_resume_devices(drv_ssds *ssds);
void *run_ssd_cool_start(void *udata);
void ssd_load_wblock_queues(drv_ssds *ssds);
void ssd_start_maintenance_threads(drv_ssds *ssds);
void ssd_start_write_worker_threads(drv_ssds *ssds);
void ssd_start_defrag_threads(drv_ssds *ssds);
bool is_valid_record(const drv_ssd_block *block, const char *ns_name);
void apply_rec_props(struct as_index_s *r, struct as_namespace_s *ns, const struct as_rec_props_s *p_props);

// Tomb raider.
void ssd_cold_start_adjust_cenotaph(struct as_namespace_s *ns, const drv_ssd_block *block, struct as_index_s *r);
void ssd_cold_start_transition_record(struct as_namespace_s *ns, const drv_ssd_block *block, struct as_index_s *r, bool is_create);
void ssd_cold_start_drop_cenotaphs(struct as_namespace_s *ns);

// Record encryption.
void ssd_init_encryption_key(struct as_namespace_s *ns);
void ssd_do_encrypt(const uint8_t *key, uint64_t off, drv_ssd_block *block);
void ssd_do_decrypt(const uint8_t *key, uint64_t off, drv_ssd_block *block);

// CP.
void ssd_adjust_versions(struct as_namespace_s *ns, ssd_device_header *header);
conflict_resolution_pol ssd_cold_start_policy(struct as_namespace_s *ns);
void ssd_cold_start_init_repl_state(struct as_namespace_s *ns, struct as_index_s* r);

// Miscellaneous.
void ssd_header_init_cfg(const struct as_namespace_s *ns, ssd_device_header *header);
bool ssd_header_is_valid_cfg(const struct as_namespace_s *ns, const ssd_device_header *header);
bool ssd_cold_start_is_valid_n_bins(uint32_t n_bins);
bool ssd_cold_start_is_record_truncated(struct as_namespace_s *ns, const drv_ssd_block *block, const struct as_rec_props_s *p_props);
void ssd_write_header(drv_ssd *ssd, ssd_device_header *header, off_t offset, size_t size); // TODO - change name!

// Durability.
void ssd_init_commit(drv_ssd *ssd);
uint64_t ssd_flush_max_us(const struct as_namespace_s *ns);
void ssd_post_write(drv_ssd *ssd, ssd_write_buf *swb);
int ssd_write_bins(struct as_storage_rd_s *rd);
int ssd_buffer_bins(struct as_storage_rd_s *rd);
uint32_t ssd_record_size(struct as_storage_rd_s *rd);
ssd_write_buf *swb_get(drv_ssd *ssd);
void ssd_init_trusted(struct as_namespace_s *ns);
bool ssd_is_untrusted(struct as_namespace_s *ns, uint8_t header_flags);
void ssd_set_trusted(struct as_namespace_s *ns);

// Called in (enterprise-split) storage table function.
int ssd_write(struct as_storage_rd_s *rd);


//
// Conversions between bytes and rblocks.
//

// TODO - make checks stricter (exclude drive header, consider drive size) ???
#define STORAGE_RBLOCK_IS_VALID(__x)	((__x) != 0)
#define STORAGE_RBLOCK_IS_INVALID(__x)	((__x) == 0)

#define RBLOCK_SIZE			128	// 2^7
#define LOG_2_RBLOCK_SIZE	7	// must be in sync with RBLOCK_SIZE

// Round bytes up to a multiple of rblock size.
static inline uint32_t BYTES_TO_RBLOCK_BYTES(uint32_t bytes) {
	return (bytes + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE;
}

// Convert byte offset to rblock_id, or bytes to rblocks as long as 'bytes' is
// already a multiple of rblock size.
static inline uint64_t BYTES_TO_RBLOCKS(uint64_t bytes) {
	return bytes >> LOG_2_RBLOCK_SIZE;
}

// Convert rblock_id to byte offset, or rblocks to bytes.
static inline uint64_t RBLOCKS_TO_BYTES(uint64_t rblocks) {
	return rblocks << LOG_2_RBLOCK_SIZE;
}


//
// Conversions between bytes/rblocks and wblocks.
//

#define STORAGE_INVALID_WBLOCK 0xFFFFffff

// Convert byte offset to wblock_id.
static inline uint32_t BYTES_TO_WBLOCK_ID(drv_ssd *ssd, uint64_t bytes) {
	return (uint32_t)(bytes / ssd->write_block_size);
}

// Convert wblock_id to byte offset.
static inline uint64_t WBLOCK_ID_TO_BYTES(drv_ssd *ssd, uint32_t wblock_id) {
	return (uint64_t)wblock_id * (uint64_t)ssd->write_block_size;
}

// Convert rblock_id to wblock_id.
static inline uint32_t RBLOCK_ID_TO_WBLOCK_ID(drv_ssd *ssd, uint64_t rblock_id) {
	return (uint32_t)((rblock_id << LOG_2_RBLOCK_SIZE) / ssd->write_block_size);
}


//
// Size rounding needed for direct IO.
//

// Used when determining a device's io_min_size.
#define LO_IO_MIN_SIZE 512
#define HI_IO_MIN_SIZE 4096

// Round bytes down to a multiple of device's minimum IO operation size.
static inline uint64_t BYTES_DOWN_TO_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return bytes & -ssd->io_min_size;
}

// Round bytes up to a multiple of device's minimum IO operation size.
static inline uint64_t BYTES_UP_TO_IO_MIN(drv_ssd *ssd, uint64_t bytes) {
	return (bytes + (ssd->io_min_size - 1)) & -ssd->io_min_size;
}


//
// Device header parsing utilities.
//

static inline bool
can_convert_storage_version(uint8_t version)
{
	return version == 1
			// In case I bump version 2 and forget to tweak conversion code:
			&& SSD_VERSION == 2;
}


//
// Record encryption.
//

static inline void
ssd_encrypt(drv_ssd *ssd, uint64_t off, drv_ssd_block *block)
{
	if (ssd->ns->storage_encryption_key_file != NULL) {
		ssd_do_encrypt(ssd->ns->storage_encryption_key, off, block);
	}
}

static inline void
ssd_decrypt(drv_ssd *ssd, uint64_t off, drv_ssd_block *block)
{
	if (ssd->ns->storage_encryption_key_file != NULL) {
		ssd_do_decrypt(ssd->ns->storage_encryption_key, off, block);
	}
}
