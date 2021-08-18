/*
 * storage.h
 *
 * Copyright (C) 2009-2018 Aerospike, Inc.
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

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_flat_record_s;
struct as_index_s;
struct as_namespace_s;
struct as_partition_s;
struct drv_pmem_s;
struct drv_ssd_s;


//==========================================================
// Typedefs & constants.
//

#ifndef AS_NODE_STORAGE_SZ
#define AS_NODE_STORAGE_SZ (5L * 1024 * 1024 * 1024 * 1024 / 8)
#endif

typedef enum {
	AS_STORAGE_ENGINE_MEMORY	= 0,
	AS_STORAGE_ENGINE_PMEM		= 1,
	AS_STORAGE_ENGINE_SSD		= 2,

	AS_NUM_STORAGE_ENGINES
} as_storage_type;

#define AS_STORAGE_MAX_DEVICES 128 // maximum devices or files per namespace
#define AS_STORAGE_MAX_DEVICE_SIZE (2L * 1024 * 1024 * 1024 * 1024) // 2Tb

// Artificial limit on write-block-size, in case we ever move to an
// SSD_HEADER_SIZE that's too big to be a write-block size limit.
// MAX_WRITE_BLOCK_SIZE must be power of 2 and <= SSD_HEADER_SIZE.
#define MAX_WRITE_BLOCK_SIZE (8 * 1024 * 1024)

// Artificial limit on write-block-size, must be power of 2 and >= RBLOCK_SIZE.
#define MIN_WRITE_BLOCK_SIZE (1024 * 1)

#define DEFAULT_MAX_WRITE_CACHE (64UL * 1024 * 1024)

typedef enum {
	AS_ENCRYPTION_AES_128,
	AS_ENCRYPTION_AES_256,

	AS_ENCRYPTION_LAST_PLUS_1
} as_encryption_method;

// Which current write buffer to use.
#define SWB_MASTER		0
#define SWB_PROLE		1
#define SWB_UNCACHED	2

#define N_CURRENT_SWBS	3

#define DEFAULT_POST_WRITE_QUEUE 256
#define MAX_POST_WRITE_QUEUE (8 * 1024)

typedef struct as_storage_rd_s {
	struct as_index_s		*r;
	struct as_namespace_s	*ns;

	struct as_bin_s			*bins;
	uint16_t				n_bins;

	bool					record_on_device;
	bool					ignore_record_on_device;

	// Shortcuts for handling set name storage:
	uint32_t				set_name_len; // could make it a uint8_t
	const char				*set_name;

	// Parameters used when handling key storage:
	uint32_t				key_size;
	const uint8_t			*key;

	uint8_t					which_current_swb;
	bool					read_page_cache;
	bool					resolve_writes; // relevant only for enterprise edition
	bool					xdr_bin_writes; // relevant only for enterprise edition
	bool					bin_luts;

	// Used by storage types AS_STORAGE_ENGINE_PMEM and AS_STORAGE_ENGINE_SSD:
	const struct as_flat_record_s *flat;
	const uint8_t			*flat_end;
	const uint8_t			*flat_bins;
	uint16_t				flat_n_bins;

	union {
		struct drv_ssd_s	*ssd;
		struct drv_pmem_s	*pmem;
	};

	// Only used by storage type AS_STORAGE_ENGINE_SSD:
	uint8_t					*read_buf;

	// Flat storage format also used for pickled records sent via fabric:
	bool					keep_pickle;
	uint32_t				pickle_sz;
	uint32_t				orig_pickle_sz;
	uint8_t					*pickle;
} as_storage_rd;

typedef struct storage_device_stats_s {
	uint64_t used_sz;
	uint32_t n_free_wblocks;

	uint32_t write_q_sz;
	uint64_t n_writes;

	uint32_t defrag_q_sz;
	uint64_t n_defrag_reads;
	uint64_t n_defrag_writes;

	uint32_t shadow_write_q_sz;
} storage_device_stats;


//==========================================================
// Public API.
//

extern uint64_t g_unique_data_size;

//------------------------------------------------
// Generic "base class" functions that call
// through storage-engine "v-tables".
//

void as_storage_cfg_init(struct as_namespace_s *ns);
void as_storage_init();
void as_storage_load();
void as_storage_activate();
void as_storage_start_tomb_raider();
bool as_storage_shutdown(uint32_t instance);

void as_storage_destroy_record(struct as_namespace_s *ns, struct as_index_s *r); // not the counterpart of as_storage_record_create()

// Start and finish an as_storage_rd usage cycle.
void as_storage_record_create(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
void as_storage_record_open(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
void as_storage_record_close(as_storage_rd *rd);

// Called within as_storage_rd usage cycle.
int as_storage_record_load_bins(as_storage_rd *rd);
bool as_storage_record_load_key(as_storage_rd *rd);
bool as_storage_record_load_pickle(as_storage_rd *rd);
int as_storage_record_write(as_storage_rd *rd);

// Storage capacity monitoring.
bool as_storage_overloaded(const struct as_namespace_s *ns, uint32_t margin, const char* tag); // returns true if write queue is too backed up
void as_storage_defrag_sweep(struct as_namespace_s *ns);

// Storage of generic data into device headers.
void as_storage_load_regime(struct as_namespace_s *ns);
void as_storage_save_regime(struct as_namespace_s *ns);
void as_storage_load_roster_generation(struct as_namespace_s *ns);
void as_storage_save_roster_generation(struct as_namespace_s *ns);
void as_storage_load_pmeta(struct as_namespace_s *ns, struct as_partition_s *p);
void as_storage_save_pmeta(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_cache_pmeta(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_flush_pmeta(struct as_namespace_s *ns, uint32_t start_pid, uint32_t n_partitions);

// Statistics.
void as_storage_stats(struct as_namespace_s *ns, int *available_pct, uint64_t *used_bytes); // available percent is that of worst device
void as_storage_device_stats(const struct as_namespace_s *ns, uint32_t device_ix, storage_device_stats *stats);
void as_storage_ticker_stats(struct as_namespace_s *ns); // prints SSD histograms to the info ticker
void as_storage_dump_wb_summary(const struct as_namespace_s *ns);
void as_storage_histogram_clear_all(struct as_namespace_s *ns); // clears all SSD histograms

// Get record storage metadata.
uint32_t as_storage_record_device_size(const struct as_namespace_s *ns, const struct as_index_s *r);

//------------------------------------------------
// Generic functions that don't use "v-tables".
//

// Called within as_storage_rd usage cycle.
uint32_t as_storage_record_mem_size(const struct as_namespace_s *ns, const struct as_index_s *r);
void as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint32_t start_bytes);
void as_storage_record_drop_from_mem_stats(as_storage_rd *rd);
void as_storage_record_get_set_name(as_storage_rd *rd);
bool as_storage_rd_load_key(as_storage_rd *rd);
bool as_storage_rd_load_pickle(as_storage_rd *rd);

//------------------------------------------------
// AS_STORAGE_ENGINE_MEMORY functions.
//

void as_storage_init_memory(struct as_namespace_s *ns);
void as_storage_start_tomb_raider_memory(struct as_namespace_s *ns);

int as_storage_record_write_memory(as_storage_rd *rd);

void as_storage_load_pmeta_memory(struct as_namespace_s *ns, struct as_partition_s *p);

void as_storage_stats_memory(struct as_namespace_s *ns, int *available_pct, uint64_t *used_bytes);

//------------------------------------------------
// AS_STORAGE_ENGINE_SSD functions.
//

void as_storage_cfg_init_ssd(struct as_namespace_s *ns);
void as_storage_init_ssd(struct as_namespace_s *ns);
void as_storage_load_ssd(struct as_namespace_s *ns, cf_queue *complete_q); // table used directly in as_storage_init()
void as_storage_load_ticker_ssd(const struct as_namespace_s *ns); // table used directly in as_storage_init()
void as_storage_sindex_build_all_ssd(struct as_namespace_s *ns); // called directly without any table - TODO - add table?
void as_storage_activate_ssd(struct as_namespace_s *ns);
bool as_storage_wait_for_defrag_ssd(struct as_namespace_s *ns);
void as_storage_start_tomb_raider_ssd(struct as_namespace_s *ns);
void as_storage_shutdown_ssd(struct as_namespace_s *ns);

void as_storage_destroy_record_ssd(struct as_namespace_s *ns, struct as_index_s *r);

void as_storage_record_create_ssd(as_storage_rd *rd);
void as_storage_record_open_ssd(as_storage_rd *rd);
void as_storage_record_close_ssd(as_storage_rd *rd);

int as_storage_record_load_bins_ssd(as_storage_rd *rd);
bool as_storage_record_load_key_ssd(as_storage_rd *rd);
bool as_storage_record_load_pickle_ssd(as_storage_rd *rd);
int as_storage_record_write_ssd(as_storage_rd *rd);

bool as_storage_overloaded_ssd(const struct as_namespace_s *ns, uint32_t margin, const char* tag);
void as_storage_defrag_sweep_ssd(struct as_namespace_s *ns);

void as_storage_load_regime_ssd(struct as_namespace_s *ns);
void as_storage_save_regime_ssd(struct as_namespace_s *ns);
void as_storage_load_roster_generation_ssd(struct as_namespace_s *ns);
void as_storage_save_roster_generation_ssd(struct as_namespace_s *ns);
void as_storage_load_pmeta_ssd(struct as_namespace_s *ns, struct as_partition_s *p);
void as_storage_save_pmeta_ssd(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_cache_pmeta_ssd(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_flush_pmeta_ssd(struct as_namespace_s *ns, uint32_t start_pid, uint32_t n_partitions);

void as_storage_stats_ssd(struct as_namespace_s *ns, int *available_pct, uint64_t *used_bytes);
void as_storage_device_stats_ssd(const struct as_namespace_s *ns, uint32_t device_ix, storage_device_stats *stats);
void as_storage_ticker_stats_ssd(struct as_namespace_s *ns);
void as_storage_dump_wb_summary_ssd(const struct as_namespace_s *ns);
void as_storage_histogram_clear_ssd(struct as_namespace_s *ns);

uint32_t as_storage_record_device_size_ssd(const struct as_index_s *r);

//------------------------------------------------
// AS_STORAGE_ENGINE_PMEM functions.
//

void as_storage_cfg_init_pmem(struct as_namespace_s *ns);
void as_storage_init_pmem(struct as_namespace_s *ns);
void as_storage_load_pmem(struct as_namespace_s *ns, cf_queue *complete_q); // table used directly in as_storage_init()
void as_storage_load_ticker_pmem(const struct as_namespace_s *ns); // table used directly in as_storage_init()
void as_storage_activate_pmem(struct as_namespace_s* ns);
bool as_storage_wait_for_defrag_pmem(struct as_namespace_s *ns);
void as_storage_start_tomb_raider_pmem(struct as_namespace_s* ns);
void as_storage_shutdown_pmem(struct as_namespace_s* ns);

void as_storage_destroy_record_pmem(struct as_namespace_s *ns, struct as_index_s *r);

void as_storage_record_create_pmem(as_storage_rd *rd);
void as_storage_record_open_pmem(as_storage_rd *rd);
void as_storage_record_close_pmem(as_storage_rd *rd);

int as_storage_record_load_bins_pmem(as_storage_rd *rd);
bool as_storage_record_load_key_pmem(as_storage_rd *rd);
bool as_storage_record_load_pickle_pmem(as_storage_rd* rd);
int as_storage_record_write_pmem(as_storage_rd *rd);

bool as_storage_overloaded_pmem(const struct as_namespace_s *ns, uint32_t margin, const char* tag);
void as_storage_defrag_sweep_pmem(struct as_namespace_s *ns);

void as_storage_load_regime_pmem(struct as_namespace_s *ns);
void as_storage_save_regime_pmem(struct as_namespace_s *ns);
void as_storage_load_roster_generation_pmem(struct as_namespace_s *ns);
void as_storage_save_roster_generation_pmem(struct as_namespace_s *ns);
void as_storage_load_pmeta_pmem(struct as_namespace_s *ns, struct as_partition_s *p);
void as_storage_save_pmeta_pmem(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_cache_pmeta_pmem(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_flush_pmeta_pmem(struct as_namespace_s *ns, uint32_t start_pid, uint32_t n_partitions);

void as_storage_stats_pmem(struct as_namespace_s *ns, int *available_pct, uint64_t *used_bytes);
void as_storage_device_stats_pmem(const struct as_namespace_s *ns, uint32_t device_ix, storage_device_stats *stats);
void as_storage_ticker_stats_pmem(struct as_namespace_s *ns);
void as_storage_dump_wb_summary_pmem(const struct as_namespace_s *ns);
void as_storage_histogram_clear_pmem(struct as_namespace_s *ns);

uint32_t as_storage_record_device_size_pmem(const struct as_index_s *r);
