/*
 * drv_mem.h
 *
 * Copyright (C) 2023 Aerospike, Inc.
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
#include <stdint.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "hist.h"

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
struct as_index_s;
struct as_index_ref_s;
struct as_index_tree_s;
struct as_namespace_s;
struct as_storage_rd_s;
struct drv_generic_s;
struct drv_header_s;
struct drv_mem_s;
struct drv_pmeta_s;


//==========================================================
// Typedefs & constants.
//

// Where records accumulate until flushed to device.
typedef struct mem_write_block_s {
	uint32_t			n_writers;	// number of concurrent writers
	uint32_t			flush_pos;	// pos on last flush
	uint32_t			n_vacated;
	uint32_t			vacated_capacity;
	vacated_wblock*		vacated_wblocks;
	struct drv_mem_s*	mem;
	uint32_t			wblock_id;
	uint8_t*			base_addr;
	uint32_t			first_dirty_pos;
	uint32_t			pos;
	uint8_t*			encrypted_buf;	// relevant for enterprise edition only
} mem_write_block;

// Per-wblock information.
typedef struct mem_wblock_state_s {
	uint32_t			inuse_sz;	// number of bytes currently used in the wblock
	cf_mutex			LOCK;		// transactions, write_worker, and defrag all are interested in wblock_state
	mem_write_block*	mwb;		// pending writes for the wblock, also treated as a cache for reads
	uint8_t				state;
	bool				short_lived; // relevant for enterprise edition only
	uint32_t			n_vac_dests; // number of wblocks into which this wblock defragged
} mem_wblock_state;

// Per current write buffer information.
typedef struct current_mwb_s {
	cf_mutex		lock;				// lock protects writes to mwb
	mem_write_block* mwb;				// mwb currently being filled by writes
	uint8_t*		encrypted_commits;	// relevant for enterprise edition only
	uint64_t		n_wblock_writes;	// total number of mwbs added to the mwb_write_q by writes
	uint64_t		n_wblock_partial_writes;	// total number of mwbs "partial flushed" by writes
} current_mwb;

// Per-device information.
typedef struct drv_mem_s {
	struct as_namespace_s* ns;

	const char*		name;				// this device's name
	const char*		shadow_name;		// this device's shadow's name, if any

	bool			running_shadow;

	current_mwb		current_mwbs[N_CURRENT_SWBS];

	int				shadow_commit_fd;	// relevant for enterprise edition only

	cf_mutex		defrag_lock;		// lock protects writes to defrag mwb
	mem_write_block* defrag_mwb;		// mwb currently being filled by defrag

	cf_queue*		shadow_fd_q;		// queue of open fds on shadow, if any

	uint8_t*		mem_base_addr;	// base address of mem-mapped file

	cf_queue*		free_wblock_q;		// IDs of free wblocks
	cf_queue*		defrag_wblock_q;	// IDs of wblocks to defrag

	cf_queue*		mwb_shadow_q;		// pointers to mwbs ready to write to shadow, if any
	cf_queue*		mwb_free_q;			// pointers to mwbs free and waiting

	uint8_t			encryption_key[64];		// relevant for enterprise edition only

	uint64_t		n_defrag_wblock_reads;	// total number of wblocks added to the defrag_wblock_q
	uint64_t		n_defrag_wblock_writes;	// total number of mwbs added to the mwb_write_q by defrag
	uint64_t		n_defrag_wblock_partial_writes;	// total number of mwbs "partial flushed" by defrag

	uint64_t		n_wblock_defrag_io_skips;	// total number of wblocks empty on defrag_wblock_q pop
	uint64_t		n_wblock_direct_frees;		// total number of wblocks freed by other than defrag

	volatile uint64_t n_tomb_raider_reads;	// relevant for enterprise edition only

	uint32_t		defrag_sweep;		// defrag sweep flag

	uint64_t		file_size;
	int				file_id;

	uint32_t		open_flag;

	uint64_t		io_min_size;		// device IO operations are aligned and sized in multiples of this

	uint64_t		inuse_size;			// number of bytes in actual use on this device

	uint32_t		first_wblock_id;	// wblock-id of first non-header wblock

	uint32_t		pristine_wblock_id;	// minimum wblock-id of "pristine" region

	uint32_t		n_wblocks;			// number of wblocks on this device
	mem_wblock_state* wblock_state;	// array of info per wblock on this device

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

	cf_tid			shadow_tid;

	histogram*		hist_shadow_write;
} drv_mem;

// Per-namespace storage information.
typedef struct drv_mems_s {
	struct as_namespace_s* ns;
	struct drv_generic_s* generic;

	// Not a great place for this - used only at startup to determine whether to
	// load a record.
	bool get_state_from_storage[AS_PARTITIONS];

	// Indexed by previous device-id to get new device-id. -1 means device is
	// "fresh" or absent. Used only at startup to fix index elements' file-id.
	int8_t device_translation[AS_STORAGE_MAX_DEVICES];

	// Used only at startup, set true if all devices are fresh.
	bool all_fresh;

	cf_mutex flush_lock;

	int n_mems;
	drv_mem mems[];
} drv_mems;


//==========================================================
// Private API - for enterprise separation only.
//

void init_commit(drv_mem* mem);
void resume_or_create_stripe(drv_mem* mem, const struct drv_header_s* shadow_header);
void header_validate_cfg(const struct as_namespace_s* ns, drv_mem* mem, struct drv_header_s* header);
void header_init_cfg(const struct as_namespace_s* ns, drv_mem* mem, struct drv_header_s* header);
void cleanup_unmatched_stripes(struct as_namespace_s* ns);
void clear_encryption_keys(struct as_namespace_s* ns);
void adjust_versions(const struct as_namespace_s* ns, struct drv_pmeta_s* pmeta);
void flush_final_cfg(struct as_namespace_s* ns);

void cold_start_sweep_device(drv_mems* mems, drv_mem* mem);
void cold_start_sweep(drv_mems* mems, drv_mem* mem);
void cold_start_fill_orig(drv_mem* mem, const struct as_flat_record_s* flat, uint64_t rblock_id, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s *r_ref);
void cold_start_record_create(struct as_namespace_s* ns, const struct as_flat_record_s* flat, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s* r_ref);
void cold_start_record_update(drv_mems* mems, const struct as_flat_record_s* flat, const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s *tree, struct as_index_ref_s* r_ref);
conflict_resolution_pol cold_start_policy(const struct as_namespace_s* ns);
void cold_start_adjust_cenotaph(const struct as_namespace_s* ns, const struct as_flat_record_s* flat, uint32_t block_void_time, struct as_index_s* r);
void cold_start_init_repl_state(const struct as_namespace_s* ns, struct as_index_s* r);
void cold_start_set_unrepl_stat(struct as_namespace_s* ns);
void cold_start_init_xdr_state(const struct as_flat_record_s* flat, struct as_index_s* r);
void cold_start_drop_cenotaphs(struct as_namespace_s* ns);

void resume_devices(drv_mems* mems);

int write_record(struct as_storage_rd_s* rd);
int write_bins(struct as_storage_rd_s* rd);
int buffer_bins(struct as_storage_rd_s* rd);
void set_wblock_flags(struct as_storage_rd_s* rd, mem_write_block* mwb);
void mrt_block_free_orig(drv_mems* mems, struct as_index_s* r);
void mrt_rd_block_free_orig(drv_mems* mems, struct as_storage_rd_s* rd);

void block_free(drv_mem* mem, uint64_t rblock_id, uint32_t n_rblocks, char* msg);

void write_header(drv_mem* mem, const uint8_t* header, const uint8_t* from, size_t size);

void mwb_release(drv_mem* mem, mem_write_block* mwb);
mem_write_block* mwb_get(drv_mem* mem, bool use_reserve);

void mem_mprotect(void *addr, size_t len, int prot);

int shadow_fd_get(drv_mem* mem);

void decrypt_record(drv_mem* mem, uint64_t off, struct as_flat_record_s* flat);
uint8_t* encrypt_wblock(mem_write_block* mwb, uint64_t off);

// Round bytes down to a multiple of shadow's minimum IO operation size.
static inline uint64_t
BYTES_DOWN_TO_IO_MIN(const drv_mem* mem, uint64_t bytes)
{
	return bytes & -mem->io_min_size;
}

// Round bytes up to a multiple of shadow's minimum IO operation size.
static inline uint64_t
BYTES_UP_TO_IO_MIN(const drv_mem* mem, uint64_t bytes)
{
	return (bytes + (mem->io_min_size - 1)) & -mem->io_min_size;
}

static inline void
mem_wait_writers_done(mem_write_block* mwb)
{
	while (mwb->n_writers != 0) {
		as_arch_pause();
	}

	as_fence_acq();
}
