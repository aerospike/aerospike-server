/*
 * storage.h
 *
 * Copyright (C) 2009-2015 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "base/rec_props.h"


// Forward declarations.
struct as_bin_s;
struct as_index_s;
struct as_partition_s;
struct as_namespace_s;
struct drv_ssd_s;
struct drv_ssd_block_s;


typedef enum {
	AS_STORAGE_ENGINE_MEMORY	= 0,
	AS_STORAGE_ENGINE_SSD		= 1,

	AS_NUM_STORAGE_ENGINES
} as_storage_type;

typedef struct as_storage_rd_s {
	struct as_index_s		*r;
	struct as_namespace_s	*ns;

	as_rec_props			rec_props;

	struct as_bin_s			*bins;
	uint16_t				n_bins;

	bool					record_on_device;
	bool					ignore_record_on_device;

	// Parameters used when handling key storage:
	uint32_t				key_size;
	uint8_t					*key;

	bool					is_durable_delete; // enterprise only

	// Specific to storage type AS_STORAGE_ENGINE_SSD:
	struct drv_ssd_block_s	*block;
	uint8_t					*must_free_block;
	struct drv_ssd_s		*ssd;
} as_storage_rd;


//------------------------------------------------
// Generic "base class" functions that call
// through storage-engine "v-tables".
//

extern void as_storage_init();
extern void as_storage_start_tomb_raider();
extern int as_storage_namespace_destroy(struct as_namespace_s *ns);

extern int as_storage_record_destroy(struct as_namespace_s *ns, struct as_index_s *r); // not the counterpart of as_storage_record_create()

// Start and finish an as_storage_rd usage cycle.
extern int as_storage_record_create(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
extern int as_storage_record_open(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd);
extern int as_storage_record_close(as_storage_rd *rd);

// Called within as_storage_rd usage cycle.
extern int as_storage_record_load_n_bins(as_storage_rd *rd);
extern int as_storage_record_load_bins(as_storage_rd *rd);
extern bool as_storage_record_size_and_check(as_storage_rd *rd);
extern int as_storage_record_write(as_storage_rd *rd);

// Storage capacity monitoring.
extern void as_storage_wait_for_defrag();
extern bool as_storage_overloaded(struct as_namespace_s *ns); // returns true if write queue is too backed up
extern bool as_storage_has_space(struct as_namespace_s *ns);
extern void as_storage_defrag_sweep(struct as_namespace_s *ns);

// Storage of generic data into device headers.
extern void as_storage_info_set(struct as_namespace_s *ns, const struct as_partition_s *p, bool flush);
extern void as_storage_info_get(struct as_namespace_s *ns, struct as_partition_s *p);
extern int as_storage_info_flush(struct as_namespace_s *ns);
extern void as_storage_save_evict_void_time(struct as_namespace_s *ns, uint32_t evict_void_time);

// Statistics.
extern int as_storage_stats(struct as_namespace_s *ns, int *available_pct, uint64_t *inuse_disk_bytes); // available percent is that of worst device
extern int as_storage_ticker_stats(struct as_namespace_s *ns); // prints SSD histograms to the info ticker
extern int as_storage_histogram_clear_all(struct as_namespace_s *ns); // clears all SSD histograms


//------------------------------------------------
// Generic functions that don't use "v-tables".
//

// Called within as_storage_rd usage cycle.
extern uint64_t as_storage_record_get_n_bytes_memory(as_storage_rd *rd);
extern void as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint64_t start_bytes);
extern void as_storage_record_drop_from_mem_stats(as_storage_rd *rd);
extern bool as_storage_record_get_key(as_storage_rd *rd);
extern size_t as_storage_record_rec_props_size(as_storage_rd *rd);
extern void as_storage_record_set_rec_props(as_storage_rd *rd, uint8_t* rec_props_data);

// Called only at shutdown to flush all device write-queues.
extern void as_storage_shutdown();


//------------------------------------------------
// AS_STORAGE_ENGINE_MEMORY functions.
//

extern int as_storage_namespace_init_memory(struct as_namespace_s *ns, cf_queue *complete_q, void *udata);
extern void as_storage_start_tomb_raider_memory(struct as_namespace_s *ns);
extern int as_storage_namespace_destroy_memory(struct as_namespace_s *ns);

extern int as_storage_record_write_memory(as_storage_rd *rd);

extern void as_storage_info_get_memory(struct as_namespace_s *ns, struct as_partition_s *p);

extern int as_storage_stats_memory(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);


//------------------------------------------------
// AS_STORAGE_ENGINE_SSD functions.
//

extern int as_storage_namespace_init_ssd(struct as_namespace_s *ns, cf_queue *complete_q, void *udata);
extern void as_storage_start_tomb_raider_ssd(struct as_namespace_s *ns);
extern void as_storage_loading_records_ticker_ssd(); // called directly by as_storage_init()
extern int as_storage_namespace_destroy_ssd(struct as_namespace_s *ns);

extern int as_storage_record_destroy_ssd(struct as_namespace_s *ns, struct as_index_s *r);

extern int as_storage_record_create_ssd(as_storage_rd *rd);
extern int as_storage_record_open_ssd(as_storage_rd *rd);
extern int as_storage_record_close_ssd(as_storage_rd *rd);

extern int as_storage_record_load_n_bins_ssd(as_storage_rd *rd);
extern int as_storage_record_load_bins_ssd(as_storage_rd *rd);
extern bool as_storage_record_size_and_check_ssd(as_storage_rd *rd);
extern int as_storage_record_write_ssd(as_storage_rd *rd);

extern void as_storage_wait_for_defrag_ssd(struct as_namespace_s *ns);
extern bool as_storage_overloaded_ssd(struct as_namespace_s *ns);
extern bool as_storage_has_space_ssd(struct as_namespace_s *ns);
extern void as_storage_defrag_sweep_ssd(struct as_namespace_s *ns);

extern void as_storage_info_set_ssd(struct as_namespace_s *ns, const struct as_partition_s *p, bool flush);
extern void as_storage_info_get_ssd(struct as_namespace_s *ns, struct as_partition_s *p);
extern int as_storage_info_flush_ssd(struct as_namespace_s *ns);
extern void as_storage_save_evict_void_time_ssd(struct as_namespace_s *ns, uint32_t evict_void_time);

extern int as_storage_stats_ssd(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);
extern int as_storage_ticker_stats_ssd(struct as_namespace_s *ns);
extern int as_storage_histogram_clear_ssd(struct as_namespace_s *ns);

// Called by "base class" functions but not via table.
extern bool as_storage_record_get_key_ssd(as_storage_rd *rd);
extern void as_storage_shutdown_ssd(struct as_namespace_s *ns);
