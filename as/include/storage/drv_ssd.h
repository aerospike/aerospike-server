/*
 * drv_ssd.h
 *
 * Copyright (C) 2014-2020 Aerospike, Inc.
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

#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "hist.h"
#include "log.h"
#include "pool.h"

#include "base/datamodel.h"
#include "fabric/partition.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"


//==========================================================
// Forward declarations.
//

struct as_flat_opt_meta_s;
struct as_flat_record_s;
struct as_index_ref_s;
struct as_index_s;
struct as_index_tree_s;
struct as_namespace_s;
struct as_storage_rd_s;
struct drv_ssd_s;


//==========================================================
// Typedefs & constants.
//

//------------------------------------------------
// Write buffer - where records accumulate until
// (the full buffer is) flushed to a device.
//
typedef struct {
	uint32_t			rc;
	uint32_t			n_writers;	// number of concurrent writers
	bool				use_post_write_q;
	uint32_t			flush_pos;	// pos on last flush
	uint32_t			n_vacated;
	uint32_t			vacated_capacity;
	vacated_wblock		*vacated_wblocks;
	struct drv_ssd_s	*ssd;
	uint32_t			wblock_id;
	uint32_t			pos;
	uint8_t				*buf;
	uint8_t				*encrypted_buf;	// relevant for enterprise edition only
} ssd_write_buf;


//------------------------------------------------
// Per-wblock information.
//
typedef struct ssd_wblock_state_s {
	uint32_t			inuse_sz;	// number of bytes currently used in the wblock
	cf_mutex			LOCK;		// transactions, write_worker, and defrag all are interested in wblock_state
	ssd_write_buf		*swb;		// pending writes for the wblock, also treated as a cache for reads
	uint8_t				state;
	bool				short_lived; // relevant for enterprise edition only
	uint32_t			n_vac_dests; // number of wblocks into which this wblock defragged
} ssd_wblock_state;


//------------------------------------------------
// Per current write buffer information.
//
typedef struct current_swb_s {
	cf_mutex		lock;				// lock protects writes to swb
	ssd_write_buf	*swb;				// swb currently being filled by writes
	uint8_t			*encrypted_commits;	// relevant for enterprise edition only
	uint64_t		n_wblock_writes;	// total number of swbs added to the swb_write_q by writes
	uint64_t		n_wblock_partial_writes;	// total number of swbs "partial flushed" by writes
} current_swb;


//------------------------------------------------
// Per-device information.
//
typedef struct drv_ssd_s {
	struct as_namespace_s *ns;

	const char		*name;				// this device's name
	const char		*shadow_name;		// this device's shadow's name, if any

	bool			running;
	bool			running_shadow;

	current_swb		current_swbs[N_CURRENT_SWBS];

	int				commit_fd;			// relevant for enterprise edition only
	int				shadow_commit_fd;	// relevant for enterprise edition only

	cf_mutex		defrag_lock;		// lock protects writes to defrag swb
	ssd_write_buf	*defrag_swb;		// swb currently being filled by defrag

	cf_pool_int32	fd_pool;			// pool of open fds
	uint32_t		n_fds;

	cf_pool_int32	fd_cache_pool;		// pool of open fds that use page cache
	uint32_t		n_cache_fds;

	cf_queue		*shadow_fd_q;		// queue of open fds on shadow, if any

	cf_queue		*free_wblock_q;		// IDs of free wblocks
	cf_queue		*defrag_wblock_q;	// IDs of wblocks to defrag

	cf_queue		*swb_write_q;		// pointers to swbs ready to write
	cf_queue		*swb_shadow_q;		// pointers to swbs ready to write to shadow, if any
	cf_queue		*swb_free_q;		// pointers to swbs free and waiting
	cf_queue		*post_write_q;		// pointers to swbs that have been written but are cached

	uint8_t			encryption_key[64];		// relevant for enterprise edition only

	uint64_t		n_read_errors;		// total number of read errors

	uint64_t		n_defrag_wblock_reads;	// total number of wblocks added to the defrag_wblock_q
	uint64_t		n_defrag_wblock_writes;	// total number of swbs added to the swb_write_q by defrag
	uint64_t		n_defrag_wblock_partial_writes;	// total number of swbs "partial flushed" by defrag

	uint64_t		n_wblock_defrag_io_skips;	// total number of wblocks empty on defrag_wblock_q pop
	uint64_t		n_wblock_direct_frees;		// total number of wblocks freed by other than defrag

	volatile uint64_t n_tomb_raider_reads;	// relevant for enterprise edition only

	uint32_t		defrag_sweep;		// defrag sweep flag

	uint64_t		file_size;
	int				file_id;

	uint32_t		open_flag;

	uint64_t		io_min_size;		// device IO operations are aligned and sized in multiples of this
	uint64_t		shadow_io_min_size;	// shadow device IO operations are aligned and sized in multiples of this

	uint64_t		inuse_size;			// number of bytes in actual use on this device

	uint32_t		first_wblock_id;	// wblock-id of first non-header wblock

	uint32_t		pristine_wblock_id;	// minimum wblock-id of "pristine" region

	uint32_t			n_wblocks;		// number of wblocks on this device
	ssd_wblock_state	*wblock_state;	// array of info per wblock on this device

	bool			cold_start_local;	// i.e. instead of from shadow

	uint32_t		sweep_wblock_id;				// wblocks read at startup
	uint64_t		record_add_older_counter;		// records not inserted due to better existing one
	uint64_t		record_add_expired_counter;		// records not inserted due to expiration
	uint64_t		record_add_evicted_counter;		// records not inserted due to eviction
	uint64_t		record_add_replace_counter;		// records reinserted
	uint64_t		record_add_unique_counter;		// records inserted
	uint64_t		record_add_unowned_counter;		// records not inserted due to unowned partition
	uint64_t		record_add_dropped_counter;		// records not inserted due to dropped tree
	uint64_t		record_add_unparsable_counter;	// unparsable records not inserted

	cf_tid			write_tid;
	cf_tid			shadow_tid;

	histogram		*hist_read;
	histogram		*hist_large_block_read;
	histogram		*hist_write;
	histogram		*hist_shadow_write;
} drv_ssd;


//------------------------------------------------
// Per-namespace storage information.
//
typedef struct drv_ssds_s {
	struct as_namespace_s	*ns;
	drv_generic				*generic;

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

typedef struct ssd_load_records_info_s {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_rc;
} ssd_load_records_info;

// Warm restart.
void ssd_resume_devices(drv_ssds *ssds);

// Cold start.
void ssd_cold_start_sweep_device(drv_ssds* ssds, drv_ssd* ssd);
void ssd_cold_start_sweep(drv_ssds* ssds, drv_ssd* ssd);
void ssd_cold_start_fill_orig(drv_ssd* ssd, const struct as_flat_record_s* flat, uint64_t rblock_id, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s *r_ref);
void ssd_cold_start_record_create(struct as_namespace_s* ns, const struct as_flat_record_s* flat, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s* r_ref);
void ssd_cold_start_record_update(drv_ssds* ssds, const struct as_flat_record_s* flat, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s* r_ref);
void ssd_cold_start_adjust_cenotaph(struct as_namespace_s *ns, const struct as_flat_record_s *flat, uint32_t block_void_time, struct as_index_s *r);
void ssd_cold_start_drop_cenotaphs(struct as_namespace_s *ns);

// Record encryption.
uint8_t* ssd_encrypt_wblock(ssd_write_buf *swb, uint64_t off);
void ssd_decrypt(drv_ssd *ssd, uint64_t off, struct as_flat_record_s *flat);
void ssd_decrypt_whole(drv_ssd *ssd, uint64_t off, uint32_t n_rblocks, struct as_flat_record_s *flat);

// CP.
void ssd_adjust_versions(struct as_namespace_s *ns, drv_pmeta* pmeta);
conflict_resolution_pol ssd_cold_start_policy(const struct as_namespace_s *ns);
void ssd_cold_start_init_repl_state(struct as_namespace_s *ns, struct as_index_s* r);
void ssd_cold_start_set_unrepl_stat(struct as_namespace_s *ns);

// XDR.
void ssd_cold_start_init_xdr_state(const struct as_flat_record_s* flat, struct as_index_s* r);

// Miscellaneous.
int ssd_fd_get(drv_ssd *ssd);
int ssd_shadow_fd_get(drv_ssd *ssd);
void ssd_fd_put(drv_ssd *ssd, int fd);
void ssd_header_init_cfg(const struct as_namespace_s *ns, drv_ssd* ssd, drv_header *header);
void ssd_header_validate_cfg(const struct as_namespace_s *ns, drv_ssd* ssd, drv_header *header);
void ssd_clear_encryption_keys(struct as_namespace_s *ns);
void ssd_flush_final_cfg(struct as_namespace_s *ns);
void ssd_write_header(drv_ssd *ssd, uint8_t *header, uint8_t *from, size_t size);
void ssd_prefetch_wblock(drv_ssd *ssd, uint64_t file_offset, uint8_t *read_buf);
void ssd_block_free(drv_ssd *ssd, uint64_t rblock_id, uint32_t n_rblocks, char *msg);

// Durability.
void ssd_init_commit(drv_ssd *ssd);
uint64_t ssd_flush_max_us(const struct as_namespace_s *ns);
void ssd_post_write(drv_ssd *ssd, ssd_write_buf *swb);
int ssd_write_bins(struct as_storage_rd_s *rd);
int ssd_buffer_bins(struct as_storage_rd_s *rd);
void ssd_mrt_block_free_orig(drv_ssds* ssds, struct as_index_s* r);
void ssd_mrt_rd_block_free_orig(drv_ssds* ssds, struct as_storage_rd_s* rd);
ssd_write_buf *swb_get(drv_ssd *ssd, bool use_reserve);
void ssd_set_wblock_flags(struct as_storage_rd_s *rd, ssd_write_buf *swb);

// Called in (enterprise-split) storage table function.
int ssd_write(struct as_storage_rd_s *rd);

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
