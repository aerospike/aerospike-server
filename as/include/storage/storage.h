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
struct drv_ssd_s;


//==========================================================
// Typedefs & constants.
//

typedef enum {
	AS_STORAGE_ENGINE_MEMORY	= 0,
	AS_STORAGE_ENGINE_SSD		= 1,

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

typedef enum {
	AS_ENCRYPTION_AES_128,
	AS_ENCRYPTION_AES_256,

	AS_ENCRYPTION_LAST_PLUS_1
} as_encryption_method;

typedef struct as_storage_rd_s {
	struct as_index_s		*r;
	struct as_namespace_s	*ns;

	struct as_bin_s			*bins;
	uint16_t				n_bins;

	bool					record_on_device;
	bool					ignore_record_on_device;

	// Shortcuts for handling set name storage:
	uint32_t				set_name_len;
	const char				*set_name;

	// Parameters used when handling key storage:
	uint32_t				key_size;
	const uint8_t			*key;

	bool					read_page_cache;
	bool					is_durable_delete; // enterprise only

	// Only used by storage type AS_STORAGE_ENGINE_SSD:
	struct as_flat_record_s	*flat;
	const uint8_t			*flat_end;
	const uint8_t			*flat_bins;
	uint16_t				flat_n_bins;
	uint8_t					*read_buf;
	struct drv_ssd_s		*ssd;

	// Flat storage format also used for pickled records sent via fabric:
	bool					keep_pickle;
	uint32_t				pickle_sz;
	uint32_t				orig_pickle_sz;
	uint8_t					*pickle;
} as_storage_rd;

typedef struct storage_device_stats_s {
	uint64_t used_sz;
	uint32_t free_wblock_q_sz;

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

//------------------------------------------------
// Generic "base class" functions that call
// through storage-engine "v-tables".
//

void as_storage_cfg_init(struct as_namespace_s *ns);
void as_storage_init();
void as_storage_load();
void as_storage_start_tomb_raider();
int as_storage_namespace_destroy(struct as_namespace_s *ns);

int as_storage_record_destroy(struct as_namespace_s *ns, struct as_index_s *r); // not the counterpart of as_storage_record_create()

// Start and finish an as_storage_rd usage cycle.
int as_storage_record_create(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
int as_storage_record_open(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
int as_storage_record_close(as_storage_rd *rd);

// Called within as_storage_rd usage cycle.
int as_storage_record_load_n_bins(as_storage_rd *rd);
int as_storage_record_load_bins(as_storage_rd *rd);
bool as_storage_record_size_and_check(as_storage_rd *rd);
int as_storage_record_write(as_storage_rd *rd);

// Storage capacity monitoring.
void as_storage_wait_for_defrag();
bool as_storage_overloaded(struct as_namespace_s *ns); // returns true if write queue is too backed up
bool as_storage_has_space(struct as_namespace_s *ns);
void as_storage_defrag_sweep(struct as_namespace_s *ns);

// Storage of generic data into device headers.
void as_storage_load_regime(struct as_namespace_s *ns);
void as_storage_load_roster_generation(struct as_namespace_s *ns);
void as_storage_save_regime(struct as_namespace_s *ns);
void as_storage_save_roster_generation(struct as_namespace_s *ns);
void as_storage_load_pmeta(struct as_namespace_s *ns, struct as_partition_s *p);
void as_storage_save_pmeta(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_cache_pmeta(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_flush_pmeta(struct as_namespace_s *ns, uint32_t start_pid, uint32_t n_partitions);

// Statistics.
int as_storage_stats(struct as_namespace_s *ns, int *available_pct, uint64_t *inuse_disk_bytes); // available percent is that of worst device
void as_storage_device_stats(struct as_namespace_s *ns, uint32_t device_ix, storage_device_stats *stats);
int as_storage_ticker_stats(struct as_namespace_s *ns); // prints SSD histograms to the info ticker
int as_storage_histogram_clear_all(struct as_namespace_s *ns); // clears all SSD histograms

// Get record storage metadata.
uint32_t as_storage_record_size(const struct as_namespace_s *ns, const struct as_index_s *r);

//------------------------------------------------
// Generic functions that don't use "v-tables".
//

// Called within as_storage_rd usage cycle.
uint64_t as_storage_record_get_n_bytes_memory(as_storage_rd *rd);
void as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint64_t start_bytes);
void as_storage_record_drop_from_mem_stats(as_storage_rd *rd);
void as_storage_record_get_set_name(as_storage_rd *rd);
bool as_storage_record_get_key(as_storage_rd *rd);
bool as_storage_record_get_pickle(as_storage_rd *rd);

// Called only at shutdown to flush all device write-queues.
void as_storage_shutdown(uint32_t instance);

//------------------------------------------------
// AS_STORAGE_ENGINE_MEMORY functions.
//

void as_storage_namespace_init_memory(struct as_namespace_s *ns);
void as_storage_start_tomb_raider_memory(struct as_namespace_s *ns);
int as_storage_namespace_destroy_memory(struct as_namespace_s *ns);

int as_storage_record_write_memory(as_storage_rd *rd);

void as_storage_load_pmeta_memory(struct as_namespace_s *ns, struct as_partition_s *p);

int as_storage_stats_memory(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);

//------------------------------------------------
// AS_STORAGE_ENGINE_SSD functions.
//

void as_storage_cfg_init_ssd(struct as_namespace_s *ns);
void as_storage_namespace_init_ssd(struct as_namespace_s *ns);
void as_storage_namespace_load_ssd(struct as_namespace_s *ns, cf_queue *complete_q);
void as_storage_start_tomb_raider_ssd(struct as_namespace_s *ns);
void as_storage_loading_records_ticker_ssd(); // called directly by as_storage_init()
int as_storage_namespace_destroy_ssd(struct as_namespace_s *ns);

int as_storage_record_destroy_ssd(struct as_namespace_s *ns, struct as_index_s *r);

int as_storage_record_create_ssd(as_storage_rd *rd);
int as_storage_record_open_ssd(as_storage_rd *rd);
int as_storage_record_close_ssd(as_storage_rd *rd);

int as_storage_record_load_n_bins_ssd(as_storage_rd *rd);
int as_storage_record_load_bins_ssd(as_storage_rd *rd);
bool as_storage_record_size_and_check_ssd(as_storage_rd *rd);
int as_storage_record_write_ssd(as_storage_rd *rd);

void as_storage_wait_for_defrag_ssd(struct as_namespace_s *ns);
bool as_storage_overloaded_ssd(struct as_namespace_s *ns);
bool as_storage_has_space_ssd(struct as_namespace_s *ns);
void as_storage_defrag_sweep_ssd(struct as_namespace_s *ns);

void as_storage_load_regime_ssd(struct as_namespace_s *ns);
void as_storage_load_roster_generation_ssd(struct as_namespace_s *ns);
void as_storage_save_regime_ssd(struct as_namespace_s *ns);
void as_storage_save_roster_generation_ssd(struct as_namespace_s *ns);
void as_storage_load_pmeta_ssd(struct as_namespace_s *ns, struct as_partition_s *p);
void as_storage_save_pmeta_ssd(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_cache_pmeta_ssd(struct as_namespace_s *ns, const struct as_partition_s *p);
void as_storage_flush_pmeta_ssd(struct as_namespace_s *ns, uint32_t start_pid, uint32_t n_partitions);

int as_storage_stats_ssd(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);
void as_storage_device_stats_ssd(struct as_namespace_s *ns, uint32_t device_ix, storage_device_stats *stats);
int as_storage_ticker_stats_ssd(struct as_namespace_s *ns);
int as_storage_histogram_clear_ssd(struct as_namespace_s *ns);

uint32_t as_storage_record_size_ssd(const struct as_index_s *r);

// Called by "base class" functions but not via table.
bool as_storage_record_get_key_ssd(as_storage_rd *rd);
bool as_storage_record_get_pickle_ssd(as_storage_rd *rd);
void as_storage_shutdown_ssd(struct as_namespace_s *ns);
