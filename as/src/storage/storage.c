/*
 * storage.c
 *
 * Copyright (C) 2009-2016 Aerospike, Inc.
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

#include "storage/storage.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/thr_info.h"
#include "fabric/partition.h"


//==========================================================
// Generic "base class" functions that call through
// storage-engine "v-tables".
//

//--------------------------------------
// as_storage_cfg_init
//

typedef void (*as_storage_cfg_init_fn)(as_namespace *ns);
static const as_storage_cfg_init_fn as_storage_cfg_init_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't need this
	as_storage_cfg_init_ssd
};

void
as_storage_cfg_init(as_namespace *ns)
{
	if (as_storage_cfg_init_table[ns->storage_type]) {
		as_storage_cfg_init_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_init
//

typedef void (*as_storage_namespace_init_fn)(as_namespace *ns);
static const as_storage_namespace_init_fn as_storage_namespace_init_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_namespace_init_memory,
	as_storage_namespace_init_ssd
};

void
as_storage_init()
{
	// Includes resuming indexes for warm and cool restarts.

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (as_storage_namespace_init_table[ns->storage_type]) {
			as_storage_namespace_init_table[ns->storage_type](ns);
		}
	}
}

//--------------------------------------
// as_storage_load
//

typedef void (*as_storage_namespace_load_fn)(as_namespace *ns, cf_queue *complete_q);
static const as_storage_namespace_load_fn as_storage_namespace_load_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no load phase
	as_storage_namespace_load_ssd
};

#define TICKER_INTERVAL (5 * 1000) // 5 seconds

void
as_storage_load()
{
	// Includes device scans for cold starts and cool restarts.

	cf_queue complete_q;

	cf_queue_init(&complete_q, sizeof(void*), g_config.n_namespaces, true);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (as_storage_namespace_load_table[ns->storage_type]) {
			as_storage_namespace_load_table[ns->storage_type](ns, &complete_q);
		}
		else {
			void *_t = NULL;

			cf_queue_push(&complete_q, &_t);
		}
	}

	// Wait for completion - cold starts or cool restarts may take a while.

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		void *_t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			as_storage_loading_records_ticker_ssd();
		}
	}

	cf_queue_destroy(&complete_q);
}

//--------------------------------------
// as_storage_start_tomb_raider
//

typedef void (*as_storage_start_tomb_raider_fn)(as_namespace *ns);
static const as_storage_start_tomb_raider_fn as_storage_start_tomb_raider_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_start_tomb_raider_memory,
	as_storage_start_tomb_raider_ssd
};

void
as_storage_start_tomb_raider()
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (as_storage_start_tomb_raider_table[ns->storage_type]) {
			as_storage_start_tomb_raider_table[ns->storage_type](ns);
		}
	}
}

//--------------------------------------
// as_storage_namespace_destroy
//

typedef int (*as_storage_namespace_destroy_fn)(as_namespace *ns);
static const as_storage_namespace_destroy_fn as_storage_namespace_destroy_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no destroy
	as_storage_namespace_destroy_ssd
};

int
as_storage_namespace_destroy(as_namespace *ns)
{
	if (as_storage_namespace_destroy_table[ns->storage_type]) {
		return as_storage_namespace_destroy_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_destroy
//

typedef int (*as_storage_record_destroy_fn)(as_namespace *ns, as_record *r);
static const as_storage_record_destroy_fn as_storage_record_destroy_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record destroy
	as_storage_record_destroy_ssd
};

int
as_storage_record_destroy(as_namespace *ns, as_record *r)
{
	if (as_storage_record_destroy_table[ns->storage_type]) {
		return as_storage_record_destroy_table[ns->storage_type](ns, r);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_create
//

typedef int (*as_storage_record_create_fn)(as_storage_rd *rd);
static const as_storage_record_create_fn as_storage_record_create_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record create
	as_storage_record_create_ssd
};

int
as_storage_record_create(as_namespace *ns, as_record *r, as_storage_rd *rd)
{
	rd->r = r;
	rd->ns = ns;
	rd->bins = 0;
	rd->n_bins = 0;
	rd->record_on_device = false;
	rd->ignore_record_on_device = false;
	rd->set_name_len = 0;
	rd->set_name = NULL;
	rd->key_size = 0;
	rd->key = NULL;
	rd->read_page_cache = false;
	rd->is_durable_delete = false;
	rd->keep_pickle = false;
	rd->pickle_sz = 0;
	rd->orig_pickle_sz = 0;
	rd->pickle = NULL;

	if (as_storage_record_create_table[ns->storage_type]) {
		return as_storage_record_create_table[ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_open
//

typedef int (*as_storage_record_open_fn)(as_storage_rd *rd);
static const as_storage_record_open_fn as_storage_record_open_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record open
	as_storage_record_open_ssd
};

int
as_storage_record_open(as_namespace *ns, as_record *r, as_storage_rd *rd)
{
	rd->r = r;
	rd->ns = ns;
	rd->bins = 0;
	rd->n_bins = 0;
	rd->record_on_device = true;
	rd->ignore_record_on_device = false;
	rd->set_name_len = 0;
	rd->set_name = NULL;
	rd->key_size = 0;
	rd->key = NULL;
	rd->read_page_cache = false;
	rd->is_durable_delete = false;
	rd->keep_pickle = false;
	rd->pickle_sz = 0;
	rd->orig_pickle_sz = 0;
	rd->pickle = NULL;

	if (as_storage_record_open_table[ns->storage_type]) {
		return as_storage_record_open_table[ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_close
//

typedef int (*as_storage_record_close_fn)(as_storage_rd *rd);
static const as_storage_record_close_fn as_storage_record_close_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record close
	as_storage_record_close_ssd
};

int
as_storage_record_close(as_storage_rd *rd)
{
	if (as_storage_record_close_table[rd->ns->storage_type]) {
		return as_storage_record_close_table[rd->ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_load_n_bins
//

typedef int (*as_storage_record_load_n_bins_fn)(as_storage_rd *rd);
static const as_storage_record_load_n_bins_fn as_storage_record_load_n_bins_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record load n bins
	as_storage_record_load_n_bins_ssd
};

int
as_storage_record_load_n_bins(as_storage_rd *rd)
{
	if (as_storage_record_load_n_bins_table[rd->ns->storage_type]) {
		return as_storage_record_load_n_bins_table[rd->ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_load_bins
//

typedef int (*as_storage_record_load_bins_fn)(as_storage_rd *rd);
static const as_storage_record_load_bins_fn as_storage_record_load_bins_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record load bins
	as_storage_record_load_bins_ssd
};

int
as_storage_record_load_bins(as_storage_rd *rd)
{
	if (as_storage_record_load_bins_table[rd->ns->storage_type]) {
		return as_storage_record_load_bins_table[rd->ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_size_and_check
//

typedef bool (*as_storage_record_size_and_check_fn)(as_storage_rd *rd);
static const as_storage_record_size_and_check_fn as_storage_record_size_and_check_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // no limit if no persistent storage - flat size is irrelevant
	as_storage_record_size_and_check_ssd
};

bool
as_storage_record_size_and_check(as_storage_rd *rd)
{
	if (as_storage_record_size_and_check_table[rd->ns->storage_type]) {
		return as_storage_record_size_and_check_table[rd->ns->storage_type](rd);
	}

	return true;
}

//--------------------------------------
// as_storage_record_write
//

typedef int (*as_storage_record_write_fn)(as_storage_rd *rd);
static const as_storage_record_write_fn as_storage_record_write_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_record_write_memory,
	as_storage_record_write_ssd
};

int
as_storage_record_write(as_storage_rd *rd)
{
	if (as_storage_record_write_table[rd->ns->storage_type]) {
		return as_storage_record_write_table[rd->ns->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_wait_for_defrag
//

typedef void (*as_storage_wait_for_defrag_fn)(as_namespace *ns);
static const as_storage_wait_for_defrag_fn as_storage_wait_for_defrag_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't do defrag
	as_storage_wait_for_defrag_ssd
};

void
as_storage_wait_for_defrag()
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (as_storage_wait_for_defrag_table[ns->storage_type]) {
			as_storage_wait_for_defrag_table[ns->storage_type](ns);
		}
	}
}

//--------------------------------------
// as_storage_overloaded
//

typedef bool (*as_storage_overloaded_fn)(as_namespace *ns);
static const as_storage_overloaded_fn as_storage_overloaded_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no overload check
	as_storage_overloaded_ssd
};

bool
as_storage_overloaded(as_namespace *ns)
{
	if (as_storage_overloaded_table[ns->storage_type]) {
		return as_storage_overloaded_table[ns->storage_type](ns);
	}

	return false;
}

//--------------------------------------
// as_storage_has_space
//

typedef bool (*as_storage_has_space_fn)(as_namespace *ns);
static const as_storage_has_space_fn as_storage_has_space_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no space check
	as_storage_has_space_ssd
};

bool
as_storage_has_space(as_namespace *ns)
{
	if (as_storage_has_space_table[ns->storage_type]) {
		return as_storage_has_space_table[ns->storage_type](ns);
	}

	return true;
}

//--------------------------------------
// as_storage_defrag_sweep
//

typedef void (*as_storage_defrag_sweep_fn)(as_namespace *ns);
static const as_storage_defrag_sweep_fn as_storage_defrag_sweep_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't do defrag
	as_storage_defrag_sweep_ssd
};

void
as_storage_defrag_sweep(as_namespace *ns)
{
	if (as_storage_defrag_sweep_table[ns->storage_type]) {
		as_storage_defrag_sweep_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_load_regime
//

typedef void (*as_storage_load_regime_fn)(as_namespace *ns);
static const as_storage_load_regime_fn as_storage_load_regime_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't store info
	as_storage_load_regime_ssd
};

void
as_storage_load_regime(as_namespace *ns)
{
	if (as_storage_load_regime_table[ns->storage_type]) {
		as_storage_load_regime_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_save_regime
//

typedef void (*as_storage_save_regime_fn)(as_namespace *ns);
static const as_storage_save_regime_fn as_storage_save_regime_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't store info
	as_storage_save_regime_ssd
};

void
as_storage_save_regime(as_namespace *ns)
{
	if (as_storage_save_regime_table[ns->storage_type]) {
		as_storage_save_regime_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_load_roster_generation
//

typedef void (*as_storage_load_roster_generation_fn)(as_namespace *ns);
static const as_storage_load_roster_generation_fn as_storage_load_roster_generation_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't store info
	as_storage_load_roster_generation_ssd
};

void
as_storage_load_roster_generation(as_namespace *ns)
{
	if (as_storage_load_roster_generation_table[ns->storage_type]) {
		as_storage_load_roster_generation_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_save_roster_generation
//

typedef void (*as_storage_save_roster_generation_fn)(as_namespace *ns);
static const as_storage_save_roster_generation_fn as_storage_save_roster_generation_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't store info
	as_storage_save_roster_generation_ssd
};

void
as_storage_save_roster_generation(as_namespace *ns)
{
	if (as_storage_save_roster_generation_table[ns->storage_type]) {
		as_storage_save_roster_generation_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_load_pmeta
//

typedef void (*as_storage_load_pmeta_fn)(as_namespace *ns, as_partition *p);
static const as_storage_load_pmeta_fn as_storage_load_pmeta_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_load_pmeta_memory,
	as_storage_load_pmeta_ssd
};

void
as_storage_load_pmeta(as_namespace *ns, as_partition *p)
{
	if (as_storage_load_pmeta_table[ns->storage_type]) {
		as_storage_load_pmeta_table[ns->storage_type](ns, p);
	}
}

//--------------------------------------
// as_storage_save_pmeta
//

typedef void (*as_storage_save_pmeta_fn)(as_namespace *ns, const as_partition *p);
static const as_storage_save_pmeta_fn as_storage_save_pmeta_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support info
	as_storage_save_pmeta_ssd
};

void
as_storage_save_pmeta(as_namespace *ns, const as_partition *p)
{
	if (as_storage_save_pmeta_table[ns->storage_type]) {
		as_storage_save_pmeta_table[ns->storage_type](ns, p);
	}
}

//--------------------------------------
// as_storage_cache_pmeta
//

typedef void (*as_storage_cache_pmeta_fn)(as_namespace *ns, const as_partition *p);
static const as_storage_cache_pmeta_fn as_storage_cache_pmeta_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support info
	as_storage_cache_pmeta_ssd
};

void
as_storage_cache_pmeta(as_namespace *ns, const as_partition *p)
{
	if (as_storage_cache_pmeta_table[ns->storage_type]) {
		as_storage_cache_pmeta_table[ns->storage_type](ns, p);
	}
}

//--------------------------------------
// as_storage_flush_pmeta
//

typedef void (*as_storage_flush_pmeta_fn)(as_namespace *ns, uint32_t start_pid, uint32_t n_partitions);
static const as_storage_flush_pmeta_fn as_storage_flush_pmeta_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support info
	as_storage_flush_pmeta_ssd
};

void
as_storage_flush_pmeta(as_namespace *ns, uint32_t start_pid, uint32_t n_partitions)
{
	if (as_storage_flush_pmeta_table[ns->storage_type]) {
		as_storage_flush_pmeta_table[ns->storage_type](ns, start_pid, n_partitions);
	}
}

//--------------------------------------
// as_storage_stats
//

typedef int (*as_storage_stats_fn)(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes);
static const as_storage_stats_fn as_storage_stats_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_stats_memory,
	as_storage_stats_ssd
};

int
as_storage_stats(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	if (as_storage_stats_table[ns->storage_type]) {
		return as_storage_stats_table[ns->storage_type](ns, available_pct, used_disk_bytes);
	}

	return 0;
}

//--------------------------------------
// as_storage_device_stats
//

typedef void (*as_storage_device_stats_fn)(as_namespace *ns, uint32_t device_ix, storage_device_stats *stats);
static const as_storage_device_stats_fn as_storage_device_stats_table[AS_NUM_STORAGE_ENGINES] = {
	NULL,
	as_storage_device_stats_ssd
};

void
as_storage_device_stats(as_namespace *ns, uint32_t device_ix, storage_device_stats *stats)
{
	if (as_storage_device_stats_table[ns->storage_type]) {
		as_storage_device_stats_table[ns->storage_type](ns, device_ix, stats);
		return;
	}

	memset(stats, 0, sizeof(storage_device_stats));
}

//--------------------------------------
// as_storage_ticker_stats
//

typedef int (*as_storage_ticker_stats_fn)(as_namespace *ns);
static const as_storage_ticker_stats_fn as_storage_ticker_stats_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support per-disk histograms... for now.
	as_storage_ticker_stats_ssd
};

int
as_storage_ticker_stats(as_namespace *ns)
{
	if (as_storage_ticker_stats_table[ns->storage_type]) {
		return as_storage_ticker_stats_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_histogram_clear_all
//

typedef int (*as_storage_histogram_clear_fn)(as_namespace *ns);
static const as_storage_histogram_clear_fn as_storage_histogram_clear_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support per-disk histograms... for now.
	as_storage_histogram_clear_ssd
};

int
as_storage_histogram_clear_all(as_namespace *ns)
{
	if (as_storage_histogram_clear_table[ns->storage_type]) {
		return as_storage_histogram_clear_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_size
//

typedef uint32_t (*as_storage_record_size_fn)(const as_record *r);
static const as_storage_record_size_fn as_storage_record_size_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support record stored size.
	as_storage_record_size_ssd
};

uint32_t
as_storage_record_size(const as_namespace *ns, const as_record *r)
{
	if (as_storage_record_size_table[ns->storage_type]) {
		return as_storage_record_size_table[ns->storage_type](r);
	}

	return 0;
}


//==========================================================
// Generic functions that don't use "v-tables".
//

// Get size of record's in-memory data - everything except index bytes.
uint64_t
as_storage_record_get_n_bytes_memory(as_storage_rd *rd)
{
	if (! rd->ns->storage_data_in_memory) {
		return 0;
	}

	uint64_t n_bytes_memory = 0;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		n_bytes_memory += as_bin_particle_size(&rd->bins[i]);
	}

	if (! rd->ns->single_bin) {
		if (rd->r->key_stored == 1) {
			n_bytes_memory += sizeof(as_rec_space) +
					((as_rec_space*)rd->r->dim)->key_size;
		}

		if (as_index_get_bin_space(rd->r)) {
			n_bytes_memory += sizeof(as_bin_space) +
					(sizeof(as_bin) * rd->n_bins);
		}
	}

	return n_bytes_memory;
}

void
as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint64_t start_bytes)
{
	if (! rd->ns->storage_data_in_memory) {
		return;
	}

	uint64_t end_bytes = as_storage_record_get_n_bytes_memory(rd);
	int64_t delta_bytes = (int64_t)end_bytes - (int64_t)start_bytes;

	if (delta_bytes != 0) {
		cf_atomic_int_add(&rd->ns->n_bytes_memory, delta_bytes);
		as_namespace_adjust_set_memory(rd->ns, as_index_get_set_id(rd->r),
				delta_bytes);
	}
}

void
as_storage_record_drop_from_mem_stats(as_storage_rd *rd)
{
	if (! rd->ns->storage_data_in_memory) {
		return;
	}

	uint64_t drop_bytes = as_storage_record_get_n_bytes_memory(rd);

	cf_atomic_int_sub(&rd->ns->n_bytes_memory, drop_bytes);
	as_namespace_adjust_set_memory(rd->ns, as_index_get_set_id(rd->r),
			-(int64_t)drop_bytes);
}

void
as_storage_record_get_set_name(as_storage_rd *rd)
{
	rd->set_name = as_index_get_set_name(rd->r, rd->ns);

	if (rd->set_name) {
		rd->set_name_len = strlen(rd->set_name);
	}
}

bool
as_storage_record_get_key(as_storage_rd *rd)
{
	if (rd->r->key_stored == 0) {
		return false;
	}

	if (rd->ns->storage_data_in_memory) {
		rd->key_size = ((as_rec_space*)rd->r->dim)->key_size;
		rd->key = ((as_rec_space*)rd->r->dim)->key;
		return true;
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_get_key_ssd(rd);
	}

	return false;
}

bool
as_storage_record_get_pickle(as_storage_rd *rd)
{
	if (rd->ns->storage_data_in_memory) {
		as_storage_record_get_set_name(rd);
		as_storage_record_get_key(rd);
		as_storage_rd_load_n_bins(rd);
		as_storage_rd_load_bins(rd, NULL);
		as_flat_pickle_record(rd);
		return true;
	}

	return as_storage_record_get_pickle_ssd(rd);
}

void
as_storage_shutdown(uint32_t instance)
{
	cf_info(AS_STORAGE, "initiating storage shutdown ...");
 	cf_info(AS_STORAGE, "flushing data to storage ...");

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			// Lock all record locks - stops everything writing to current swbs
			// such that each write's record lock scope is either completed or
			// never entered.
			for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
				as_partition_shutdown(ns, pid);
			}

			// Now flush everything outstanding to storage devices.
			as_storage_shutdown_ssd(ns);
			as_namespace_xmem_shutdown(ns, instance);
		}
	}

  	cf_info(AS_STORAGE, "completed flushing to storage");
}
