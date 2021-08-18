/*
 * storage.c
 *
 * Copyright (C) 2009-2020 Aerospike, Inc.
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
#include <unistd.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/thr_info.h"
#include "fabric/partition.h"


//==========================================================
// Globals.
//

uint64_t g_unique_data_size = 0;


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
	as_storage_cfg_init_pmem,
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

typedef void (*as_storage_init_fn)(as_namespace *ns);
static const as_storage_init_fn as_storage_init_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_init_memory,
	as_storage_init_pmem,
	as_storage_init_ssd
};

void
as_storage_init()
{
	// Includes resuming indexes for warm and cool restarts.

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (as_storage_init_table[ns->storage_type]) {
			as_storage_init_table[ns->storage_type](ns);
		}
	}

	if (AS_NODE_STORAGE_SZ != 0 && g_unique_data_size > AS_NODE_STORAGE_SZ) {
		cf_crash_nostack(AS_STORAGE, "Community Edition limit exceeded");
	}
}

//--------------------------------------
// as_storage_load
//

typedef void (*as_storage_load_fn)(as_namespace *ns, cf_queue *complete_q);
static const as_storage_load_fn as_storage_load_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no load phase
	as_storage_load_pmem,
	as_storage_load_ssd
};

typedef void (*as_storage_load_ticker_fn)(const as_namespace *ns);
static const as_storage_load_ticker_fn as_storage_load_ticker_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no load phase - unreachable (ns->loading_records)
	as_storage_load_ticker_pmem,
	as_storage_load_ticker_ssd
};

#define TICKER_INTERVAL (5 * 1000) // 5 seconds

void
as_storage_load()
{
	// Includes device scans for cold starts and cool restarts.

	cf_queue complete_q;

	cf_queue_init(&complete_q, sizeof(void*), g_config.n_namespaces, true);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (as_storage_load_table[ns->storage_type]) {
			as_storage_load_table[ns->storage_type](ns, &complete_q);
		}
		else {
			void *_t = NULL;

			cf_queue_push(&complete_q, &_t);
		}
	}

	// Wait for completion - cold starts or cool restarts may take a while.

	for (uint32_t n_done = 0; n_done < g_config.n_namespaces; n_done++) {
		void *_t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace *ns = g_config.namespaces[ns_ix];

				if (ns->loading_records) {
					as_storage_load_ticker_table[ns->storage_type](ns);
				}
			}
		}
	}

	cf_queue_destroy(&complete_q);
}

//--------------------------------------
// as_storage_activate
//

typedef void (*as_storage_activate_fn)(as_namespace *ns);
static const as_storage_activate_fn as_storage_activate_table[AS_NUM_STORAGE_ENGINES] = {
	NULL,
	as_storage_activate_pmem,
	as_storage_activate_ssd
};

typedef bool (*as_storage_wait_for_defrag_fn)(as_namespace *ns);
static const as_storage_wait_for_defrag_fn as_storage_wait_for_defrag_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't do defrag
	as_storage_wait_for_defrag_pmem,
	as_storage_wait_for_defrag_ssd
};

void
as_storage_activate()
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (as_storage_activate_table[ns->storage_type]) {
			as_storage_activate_table[ns->storage_type](ns);
		}
	}

	while (true) {
		bool any_defragging = false;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_namespace *ns = g_config.namespaces[ns_ix];

			if (as_storage_wait_for_defrag_table[ns->storage_type] &&
					as_storage_wait_for_defrag_table[ns->storage_type](ns)) {
				any_defragging = true;
			}
		}

		if (! any_defragging) {
			break;
		}

		sleep(3);
	}
}

//--------------------------------------
// as_storage_start_tomb_raider
//

typedef void (*as_storage_start_tomb_raider_fn)(as_namespace *ns);
static const as_storage_start_tomb_raider_fn as_storage_start_tomb_raider_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_start_tomb_raider_memory,
	as_storage_start_tomb_raider_pmem,
	as_storage_start_tomb_raider_ssd
};

void
as_storage_start_tomb_raider()
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (as_storage_start_tomb_raider_table[ns->storage_type]) {
			as_storage_start_tomb_raider_table[ns->storage_type](ns);
		}
	}
}

//--------------------------------------
// as_storage_shutdown
//

typedef void (*as_storage_shutdown_fn)(as_namespace *ns);
static const as_storage_shutdown_fn as_storage_shutdown_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no shutdown phase - unreachable
	as_storage_shutdown_pmem,
	as_storage_shutdown_ssd
};

bool
as_storage_shutdown(uint32_t instance)
{
	bool all_ok = true;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (ns->storage_type == AS_STORAGE_ENGINE_MEMORY) {
			cf_info(AS_STORAGE, "{%s} storage-engine memory - nothing to do",
					ns->name);
			continue;
		}

		// Lock all record locks - stops everything writing to current wblocks
		// such that each write's record lock scope is either completed or
		// never entered.
		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_shutdown(ns, pid);
		}

		cf_info(AS_STORAGE, "{%s} partitions shut down", ns->name);

		// Now flush everything outstanding to storage devices.
		as_storage_shutdown_table[ns->storage_type](ns);

		cf_info(AS_STORAGE, "{%s} storage flushed", ns->name);

		if (! as_namespace_xmem_shutdown(ns, instance)) {
			all_ok = false; // but continue - next namespace may be ok
		}
	}

	return all_ok;
}

//--------------------------------------
// as_storage_destroy_record
//

typedef void (*as_storage_destroy_record_fn)(as_namespace *ns, as_record *r);
static const as_storage_destroy_record_fn as_storage_destroy_record_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no destroy record
	as_storage_destroy_record_pmem,
	as_storage_destroy_record_ssd
};

void
as_storage_destroy_record(as_namespace *ns, as_record *r)
{
	if (as_storage_destroy_record_table[ns->storage_type]) {
		as_storage_destroy_record_table[ns->storage_type](ns, r);
	}
}

//--------------------------------------
// as_storage_record_create
//

typedef void (*as_storage_record_create_fn)(as_storage_rd *rd);
static const as_storage_record_create_fn as_storage_record_create_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record create
	as_storage_record_create_pmem,
	as_storage_record_create_ssd
};

void
as_storage_record_create(as_namespace *ns, as_record *r, as_storage_rd *rd)
{
	rd->r = r;
	rd->ns = ns;
	rd->bins = NULL;
	rd->n_bins = 0;
	rd->record_on_device = false;
	rd->ignore_record_on_device = false;
	rd->set_name_len = 0;
	rd->set_name = NULL;
	rd->key_size = 0;
	rd->key = NULL;
	rd->which_current_swb = SWB_MASTER;
	rd->read_page_cache = false;
	rd->resolve_writes = false;
	rd->xdr_bin_writes = false;
	rd->bin_luts = false;
	rd->keep_pickle = false;
	rd->pickle_sz = 0;
	rd->orig_pickle_sz = 0;
	rd->pickle = NULL;

	if (as_storage_record_create_table[ns->storage_type]) {
		as_storage_record_create_table[ns->storage_type](rd);
	}
}

//--------------------------------------
// as_storage_record_open
//

typedef void (*as_storage_record_open_fn)(as_storage_rd *rd);
static const as_storage_record_open_fn as_storage_record_open_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record open
	as_storage_record_open_pmem,
	as_storage_record_open_ssd
};

void
as_storage_record_open(as_namespace *ns, as_record *r, as_storage_rd *rd)
{
	rd->r = r;
	rd->ns = ns;
	rd->bins = NULL;
	rd->n_bins = 0;
	rd->record_on_device = true;
	rd->ignore_record_on_device = false;
	rd->set_name_len = 0;
	rd->set_name = NULL;
	rd->key_size = 0;
	rd->key = NULL;
	rd->which_current_swb = SWB_MASTER;
	rd->read_page_cache = false;
	rd->resolve_writes = false;
	rd->xdr_bin_writes = false;
	rd->bin_luts = false;
	rd->keep_pickle = false;
	rd->pickle_sz = 0;
	rd->orig_pickle_sz = 0;
	rd->pickle = NULL;

	if (as_storage_record_open_table[ns->storage_type]) {
		as_storage_record_open_table[ns->storage_type](rd);
	}
}

//--------------------------------------
// as_storage_record_close
//

typedef void (*as_storage_record_close_fn)(as_storage_rd *rd);
static const as_storage_record_close_fn as_storage_record_close_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record close
	as_storage_record_close_pmem,
	as_storage_record_close_ssd
};

void
as_storage_record_close(as_storage_rd *rd)
{
	if (as_storage_record_close_table[rd->ns->storage_type]) {
		as_storage_record_close_table[rd->ns->storage_type](rd);
	}
}

//--------------------------------------
// as_storage_record_load_bins
//

typedef int (*as_storage_record_load_bins_fn)(as_storage_rd *rd);
static const as_storage_record_load_bins_fn as_storage_record_load_bins_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record load bins
	as_storage_record_load_bins_pmem,
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
// as_storage_record_load_key
//

typedef bool (*as_storage_record_load_key_fn)(as_storage_rd *rd);
static const as_storage_record_load_key_fn as_storage_record_load_key_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record load key
	as_storage_record_load_key_pmem,
	as_storage_record_load_key_ssd
};

bool
as_storage_record_load_key(as_storage_rd *rd)
{
	if (as_storage_record_load_key_table[rd->ns->storage_type]) {
		return as_storage_record_load_key_table[rd->ns->storage_type](rd);
	}

	return false;
}

//--------------------------------------
// as_storage_record_load_pickle
//

typedef bool (*as_storage_record_load_pickle_fn)(as_storage_rd *rd);
static const as_storage_record_load_pickle_fn as_storage_record_load_pickle_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no record load pickle
	as_storage_record_load_pickle_pmem,
	as_storage_record_load_pickle_ssd
};

bool
as_storage_record_load_pickle(as_storage_rd *rd)
{
	if (as_storage_record_load_pickle_table[rd->ns->storage_type]) {
		return as_storage_record_load_pickle_table[rd->ns->storage_type](rd);
	}

	return false;
}

//--------------------------------------
// as_storage_record_write
//

typedef int (*as_storage_record_write_fn)(as_storage_rd *rd);
static const as_storage_record_write_fn as_storage_record_write_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_record_write_memory,
	as_storage_record_write_pmem,
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
// as_storage_overloaded
//

typedef bool (*as_storage_overloaded_fn)(const as_namespace *ns, uint32_t margin, const char* tag);
static const as_storage_overloaded_fn as_storage_overloaded_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory has no overload check
	as_storage_overloaded_pmem,
	as_storage_overloaded_ssd
};

bool
as_storage_overloaded(const as_namespace *ns, uint32_t margin, const char* tag)
{
	if (as_storage_overloaded_table[ns->storage_type]) {
		return as_storage_overloaded_table[ns->storage_type](ns, margin, tag);
	}

	return false;
}

//--------------------------------------
// as_storage_defrag_sweep
//

typedef void (*as_storage_defrag_sweep_fn)(as_namespace *ns);
static const as_storage_defrag_sweep_fn as_storage_defrag_sweep_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't do defrag
	as_storage_defrag_sweep_pmem,
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
	as_storage_load_regime_pmem,
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
	as_storage_save_regime_pmem,
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
	as_storage_load_roster_generation_pmem,
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
	as_storage_save_roster_generation_pmem,
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
	as_storage_load_pmeta_pmem,
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
	as_storage_save_pmeta_pmem,
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
	as_storage_cache_pmeta_pmem,
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
	as_storage_flush_pmeta_pmem,
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

typedef void (*as_storage_stats_fn)(as_namespace *ns, int *available_pct, uint64_t *used_bytes);
static const as_storage_stats_fn as_storage_stats_table[AS_NUM_STORAGE_ENGINES] = {
	as_storage_stats_memory,
	as_storage_stats_pmem,
	as_storage_stats_ssd
};

void
as_storage_stats(as_namespace *ns, int *available_pct, uint64_t *used_bytes)
{
	if (as_storage_stats_table[ns->storage_type]) {
		as_storage_stats_table[ns->storage_type](ns, available_pct, used_bytes);
	}
}

//--------------------------------------
// as_storage_device_stats
//

typedef void (*as_storage_device_stats_fn)(const as_namespace *ns, uint32_t device_ix, storage_device_stats *stats);
static const as_storage_device_stats_fn as_storage_device_stats_table[AS_NUM_STORAGE_ENGINES] = {
	NULL,
	as_storage_device_stats_pmem,
	as_storage_device_stats_ssd
};

void
as_storage_device_stats(const as_namespace *ns, uint32_t device_ix, storage_device_stats *stats)
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

typedef void (*as_storage_ticker_stats_fn)(as_namespace *ns);
static const as_storage_ticker_stats_fn as_storage_ticker_stats_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support per-disk histograms... for now.
	as_storage_ticker_stats_pmem,
	as_storage_ticker_stats_ssd
};

void
as_storage_ticker_stats(as_namespace *ns)
{
	if (as_storage_ticker_stats_table[ns->storage_type]) {
		as_storage_ticker_stats_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_dump_wb_summary
//

typedef void (*as_storage_dump_wb_summary_fn)(const as_namespace *ns);
static const as_storage_dump_wb_summary_fn as_storage_dump_wb_summary_table[AS_NUM_STORAGE_ENGINES] = {
	NULL,
	as_storage_dump_wb_summary_pmem,
	as_storage_dump_wb_summary_ssd
};

void
as_storage_dump_wb_summary(const as_namespace *ns)
{
	if (as_storage_dump_wb_summary_table[ns->storage_type]) {
		as_storage_dump_wb_summary_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_histogram_clear_all
//

typedef void (*as_storage_histogram_clear_fn)(as_namespace *ns);
static const as_storage_histogram_clear_fn as_storage_histogram_clear_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support per-disk histograms... for now.
	as_storage_histogram_clear_pmem,
	as_storage_histogram_clear_ssd
};

void
as_storage_histogram_clear_all(as_namespace *ns)
{
	if (as_storage_histogram_clear_table[ns->storage_type]) {
		as_storage_histogram_clear_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_record_size
//

typedef uint32_t (*as_storage_record_device_size_fn)(const as_record *r);
static const as_storage_record_device_size_fn as_storage_record_device_size_table[AS_NUM_STORAGE_ENGINES] = {
	NULL, // memory doesn't support record stored size.
	as_storage_record_device_size_pmem,
	as_storage_record_device_size_ssd
};

uint32_t
as_storage_record_device_size(const as_namespace *ns, const as_record *r)
{
	if (as_storage_record_device_size_table[ns->storage_type]) {
		return as_storage_record_device_size_table[ns->storage_type](r);
	}

	return 0;
}


//==========================================================
// Generic functions that don't use "v-tables".
//

// Get size of record's in-memory data - everything except index bytes.
uint32_t
as_storage_record_mem_size(const as_namespace *ns, const as_record *r)
{
	if (! ns->storage_data_in_memory) {
		return 0;
	}

	if (ns->single_bin) {
		as_bin *b = as_index_get_single_bin(r);

		return as_bin_is_used(b) ? as_bin_particle_size(b) : 0;
	}

	uint64_t sz = 0;

	if (r->key_stored == 1) {
		sz += sizeof(as_rec_space) + ((as_rec_space*)r->dim)->key_size;
	}

	as_bin_space *bin_space = as_index_get_bin_space(r);

	if (bin_space == NULL) {
		return (uint32_t)sz;
	}

	uint16_t n_bins = bin_space->n_bins;

	if (r->has_bin_meta == 0) {
		sz += sizeof(as_bin_space) + (sizeof(as_bin_no_meta) * n_bins);

		as_bin_no_meta *bins = (as_bin_no_meta *)bin_space->bins;

		for (uint16_t i = 0; i < n_bins; i++) {
			sz += as_bin_particle_size((as_bin *)&bins[i]);
		}
	}
	else {
		sz += sizeof(as_bin_space) + (sizeof(as_bin) * n_bins);

		for (uint16_t i = 0; i < n_bins; i++) {
			sz += as_bin_particle_size(&bin_space->bins[i]);
		}
	}

	return (uint32_t)sz;
}

void
as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint32_t start_bytes)
{
	as_namespace *ns = rd->ns;

	if (! ns->storage_data_in_memory) {
		return;
	}

	as_record *r = rd->r;

	uint64_t end_bytes = as_storage_record_mem_size(ns, r);
	int64_t delta_bytes = (int64_t)end_bytes - (int64_t)start_bytes;

	if (delta_bytes != 0) {
		cf_atomic_int_add(&ns->n_bytes_memory, delta_bytes);
		as_namespace_adjust_set_memory(ns, as_index_get_set_id(r), delta_bytes);
	}
}

void
as_storage_record_drop_from_mem_stats(as_storage_rd *rd)
{
	as_namespace *ns = rd->ns;

	if (! ns->storage_data_in_memory) {
		return;
	}

	as_record *r = rd->r;

	uint64_t drop_bytes = as_storage_record_mem_size(ns, r);

	cf_atomic_int_sub(&ns->n_bytes_memory, drop_bytes);
	as_namespace_adjust_set_memory(ns, as_index_get_set_id(r),
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
as_storage_rd_load_key(as_storage_rd *rd)
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
		return as_storage_record_load_key(rd);
	}

	return false;
}

bool
as_storage_rd_load_pickle(as_storage_rd *rd)
{
	if (g_config.downgrading) {
		int rv = as_bin_downgrade_pickle(rd);

		if (rv != 0) {
			return rv == 1;
		}
	}

	if (rd->ns->storage_data_in_memory) {
		as_storage_record_get_set_name(rd);
		as_storage_rd_load_key(rd);

		as_bin stack_bins[rd->ns->single_bin ? 0 : RECORD_MAX_BINS];

		as_storage_rd_load_bins(rd, stack_bins);
		as_flat_pickle_record(rd);
		return true;
	}

	return as_storage_record_load_pickle(rd);
}
