/*
 * storage.c
 *
 * Copyright (C) 2009-2023 Aerospike, Inc.
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

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_queue.h"

#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "storage/drv_common.h"


//==========================================================
// Globals.
//

uint64_t g_unique_data_size = 0;


//==========================================================
// Generic "base class" functions that call through
// storage-engine "v-tables".
//

//--------------------------------------
// as_storage_init
//

typedef void (*as_storage_init_fn)(as_namespace* ns);
static const as_storage_init_fn as_storage_init_table[] = {
	as_storage_init_mem,
	as_storage_init_pmem,
	as_storage_init_ssd
};

void
as_storage_init(void)
{
	// Includes resuming indexes for warm restarts.

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		as_storage_init_table[ns->storage_type](ns);
	}

	if (AS_NODE_STORAGE_SZ != 0 && g_unique_data_size > AS_NODE_STORAGE_SZ) {
		cf_crash_nostack(AS_STORAGE, "Community Edition limit exceeded");
	}
}

//--------------------------------------
// as_storage_load
//

typedef void (*as_storage_load_fn)(as_namespace* ns, cf_queue* complete_q);
static const as_storage_load_fn as_storage_load_table[] = {
	as_storage_load_mem,
	as_storage_load_pmem,
	as_storage_load_ssd
};

typedef void (*as_storage_load_ticker_fn)(const as_namespace* ns);
static const as_storage_load_ticker_fn as_storage_load_ticker_table[] = {
	as_storage_load_ticker_mem,
	as_storage_load_ticker_pmem,
	as_storage_load_ticker_ssd
};

#define TICKER_INTERVAL (5 * 1000) // 5 seconds

void
as_storage_load(void)
{
	// Includes device scans for cold starts.

	cf_queue complete_q;

	cf_queue_init(&complete_q, sizeof(void*), g_config.n_namespaces, true);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		as_storage_load_table[ns->storage_type](ns, &complete_q);
	}

	// Wait for completion - cold starts may take a while.

	for (uint32_t n_done = 0; n_done < g_config.n_namespaces; n_done++) {
		void* _t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace* ns = g_config.namespaces[ns_ix];

				if (ns->loading_records) {
					as_storage_load_ticker_table[ns->storage_type](ns);
				}
			}
		}
	}

	uint32_t n_2nd_pass = 0;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		if (drv_load_needs_2nd_pass(ns)) {
			as_storage_load_table[ns->storage_type](ns, &complete_q);
			n_2nd_pass++;
		}
	}

	for (uint32_t n_done = 0; n_done < n_2nd_pass; n_done++) {
		void* _t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace* ns = g_config.namespaces[ns_ix];

				if (drv_load_needs_2nd_pass(ns) && ns->loading_records) {
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

typedef void (*as_storage_activate_fn)(as_namespace* ns);
static const as_storage_activate_fn as_storage_activate_table[] = {
	as_storage_activate_mem,
	as_storage_activate_pmem,
	as_storage_activate_ssd
};

typedef bool (*as_storage_wait_for_defrag_fn)(as_namespace* ns);
static const as_storage_wait_for_defrag_fn as_storage_wait_for_defrag_table[] = {
	as_storage_wait_for_defrag_mem,
	as_storage_wait_for_defrag_pmem,
	as_storage_wait_for_defrag_ssd
};

void
as_storage_activate(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		as_storage_activate_table[ns->storage_type](ns);
	}

	while (true) {
		bool any_defragging = false;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_namespace* ns = g_config.namespaces[ns_ix];

			if (as_storage_wait_for_defrag_table[ns->storage_type](ns)) {
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

typedef void (*as_storage_start_tomb_raider_fn)(as_namespace* ns);
static const as_storage_start_tomb_raider_fn as_storage_start_tomb_raider_table[] = {
	as_storage_start_tomb_raider_mem,
	as_storage_start_tomb_raider_pmem,
	as_storage_start_tomb_raider_ssd
};

void
as_storage_start_tomb_raider(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		as_storage_start_tomb_raider_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_shutdown
//

typedef void (*as_storage_shutdown_fn)(as_namespace* ns);
static const as_storage_shutdown_fn as_storage_shutdown_table[] = {
	as_storage_shutdown_mem,
	as_storage_shutdown_pmem,
	as_storage_shutdown_ssd
};

bool
as_storage_shutdown(uint32_t instance)
{
	bool all_ok = true;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// Lock all record locks - ensure each operation's record lock scope is
		// either completed or never entered.
		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_tree_shutdown(ns, pid);
		}

		// Lock all partition locks - ensure partition info in device header is
		// not changing (migrations may still drop trees). Separate loop so
		// (future) async-IO partition locks don't need to be coroutine aware.
		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_shutdown(ns, pid);
		}

		cf_info(AS_STORAGE, "{%s} partitions shut down", ns->name);

		as_sindex_shutdown(ns);

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

typedef void (*as_storage_destroy_record_fn)(as_namespace* ns, as_record* r);
static const as_storage_destroy_record_fn as_storage_destroy_record_table[] = {
	as_storage_destroy_record_mem,
	as_storage_destroy_record_pmem,
	as_storage_destroy_record_ssd
};

void
as_storage_destroy_record(as_namespace* ns, as_record* r)
{
	as_storage_destroy_record_table[ns->storage_type](ns, r);
}

//--------------------------------------
// as_storage_record_create
//

// Used to have table functions, but no foreseeable need - so we removed them.
void
as_storage_record_create(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	*rd = (as_storage_rd){
			.r = r,
			.ns = ns,
			.which_current_swb = SWB_MASTER
	};

	// Ancient paranoia...
	cf_assert(r->rblock_id == 0, AS_STORAGE, "uninitialized rblock-id");
}

//--------------------------------------
// as_storage_record_open
//

typedef void (*as_storage_record_open_fn)(as_storage_rd* rd);
static const as_storage_record_open_fn as_storage_record_open_table[] = {
	as_storage_record_open_mem,
	as_storage_record_open_pmem,
	as_storage_record_open_ssd
};

void
as_storage_record_open(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	*rd = (as_storage_rd){
			.r = r,
			.ns = ns,
			.record_on_device = true,
			.which_current_swb = SWB_MASTER
	};

	// Sets the device (union) pointer.
	as_storage_record_open_table[ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_close
//

// Used to have table functions, but no foreseeable need - so we removed them.
void
as_storage_record_close(as_storage_rd* rd)
{
	// Relevant only for AS_STORAGE_ENGINE_SSD.
	if (rd->read_buf != NULL) {
		cf_free(rd->read_buf);
		rd->read_buf = NULL; // TODO - needed? (Can we ever call this twice?)
	}
}

//--------------------------------------
// as_storage_record_load_bins
//

typedef int (*as_storage_record_load_bins_fn)(as_storage_rd* rd);
static const as_storage_record_load_bins_fn as_storage_record_load_bins_table[] = {
	as_storage_record_load_bins_mem,
	as_storage_record_load_bins_pmem,
	as_storage_record_load_bins_ssd
};

int
as_storage_record_load_bins(as_storage_rd* rd)
{
	return as_storage_record_load_bins_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_key
//

typedef bool (*as_storage_record_load_key_fn)(as_storage_rd* rd);
static const as_storage_record_load_key_fn as_storage_record_load_key_table[] = {
	as_storage_record_load_key_mem,
	as_storage_record_load_key_pmem,
	as_storage_record_load_key_ssd
};

bool
as_storage_record_load_key(as_storage_rd* rd)
{
	return as_storage_record_load_key_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_pickle
//

typedef bool (*as_storage_record_load_pickle_fn)(as_storage_rd* rd);
static const as_storage_record_load_pickle_fn as_storage_record_load_pickle_table[] = {
	as_storage_record_load_pickle_mem,
	as_storage_record_load_pickle_pmem,
	as_storage_record_load_pickle_ssd
};

bool
as_storage_record_load_pickle(as_storage_rd* rd)
{
	return as_storage_record_load_pickle_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_raw
//

typedef bool (*as_storage_record_load_raw_fn)(as_storage_rd* rd, bool leave_encrypted);
static const as_storage_record_load_raw_fn as_storage_record_load_raw_table[] = {
	as_storage_record_load_raw_mem,
	as_storage_record_load_raw_pmem,
	as_storage_record_load_raw_ssd
};

bool
as_storage_record_load_raw(as_storage_rd* rd, bool leave_encrypted)
{
	return as_storage_record_load_raw_table[rd->ns->storage_type](rd, leave_encrypted);
}

//--------------------------------------
// as_storage_record_write
//

typedef int (*as_storage_record_write_fn)(as_storage_rd* rd);
static const as_storage_record_write_fn as_storage_record_write_table[] = {
	as_storage_record_write_mem,
	as_storage_record_write_pmem,
	as_storage_record_write_ssd
};

int
as_storage_record_write(as_storage_rd* rd)
{
	return as_storage_record_write_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_overloaded
//

// Used to have table functions, but no foreseeable need - so we removed them.
bool
as_storage_overloaded(const as_namespace* ns, uint32_t margin, const char* tag)
{
	uint32_t limit = ns->storage_max_write_q + margin;

	if (ns->n_wblocks_to_flush > limit) {
		cf_ticker_warning(AS_STORAGE, "{%s} %s fail: queue too deep: exceeds max %u",
				ns->name, tag, limit);
		return true;
	}

	return false;
}

//--------------------------------------
// as_storage_defrag_sweep
//

typedef void (*as_storage_defrag_sweep_fn)(as_namespace* ns);
static const as_storage_defrag_sweep_fn as_storage_defrag_sweep_table[] = {
	as_storage_defrag_sweep_mem,
	as_storage_defrag_sweep_pmem,
	as_storage_defrag_sweep_ssd
};

void
as_storage_defrag_sweep(as_namespace* ns)
{
	as_storage_defrag_sweep_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_regime
//

typedef void (*as_storage_load_regime_fn)(as_namespace* ns);
static const as_storage_load_regime_fn as_storage_load_regime_table[] = {
	as_storage_load_regime_mem,
	as_storage_load_regime_pmem,
	as_storage_load_regime_ssd
};

void
as_storage_load_regime(as_namespace* ns)
{
	as_storage_load_regime_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_save_regime
//

typedef void (*as_storage_save_regime_fn)(as_namespace* ns);
static const as_storage_save_regime_fn as_storage_save_regime_table[] = {
	as_storage_save_regime_mem,
	as_storage_save_regime_pmem,
	as_storage_save_regime_ssd
};

void
as_storage_save_regime(as_namespace* ns)
{
	as_storage_save_regime_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_roster_generation
//

typedef void (*as_storage_load_roster_generation_fn)(as_namespace* ns);
static const as_storage_load_roster_generation_fn as_storage_load_roster_generation_table[] = {
	as_storage_load_roster_generation_mem,
	as_storage_load_roster_generation_pmem,
	as_storage_load_roster_generation_ssd
};

void
as_storage_load_roster_generation(as_namespace* ns)
{
	as_storage_load_roster_generation_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_save_roster_generation
//

typedef void (*as_storage_save_roster_generation_fn)(as_namespace* ns);
static const as_storage_save_roster_generation_fn as_storage_save_roster_generation_table[] = {
	as_storage_save_roster_generation_mem,
	as_storage_save_roster_generation_pmem,
	as_storage_save_roster_generation_ssd
};

void
as_storage_save_roster_generation(as_namespace* ns)
{
	as_storage_save_roster_generation_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_pmeta
//

typedef void (*as_storage_load_pmeta_fn)(as_namespace* ns, as_partition* p);
static const as_storage_load_pmeta_fn as_storage_load_pmeta_table[] = {
	as_storage_load_pmeta_mem,
	as_storage_load_pmeta_pmem,
	as_storage_load_pmeta_ssd
};

void
as_storage_load_pmeta(as_namespace* ns, as_partition* p)
{
	as_storage_load_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_save_pmeta
//

typedef void (*as_storage_save_pmeta_fn)(as_namespace* ns, const as_partition* p);
static const as_storage_save_pmeta_fn as_storage_save_pmeta_table[] = {
	as_storage_save_pmeta_mem,
	as_storage_save_pmeta_pmem,
	as_storage_save_pmeta_ssd
};

void
as_storage_save_pmeta(as_namespace* ns, const as_partition* p)
{
	as_storage_save_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_cache_pmeta
//

typedef void (*as_storage_cache_pmeta_fn)(as_namespace* ns, const as_partition* p);
static const as_storage_cache_pmeta_fn as_storage_cache_pmeta_table[] = {
	as_storage_cache_pmeta_mem,
	as_storage_cache_pmeta_pmem,
	as_storage_cache_pmeta_ssd
};

void
as_storage_cache_pmeta(as_namespace* ns, const as_partition* p)
{
	as_storage_cache_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_flush_pmeta
//

typedef void (*as_storage_flush_pmeta_fn)(as_namespace* ns, uint32_t start_pid, uint32_t n_partitions);
static const as_storage_flush_pmeta_fn as_storage_flush_pmeta_table[] = {
	as_storage_flush_pmeta_mem,
	as_storage_flush_pmeta_pmem,
	as_storage_flush_pmeta_ssd
};

void
as_storage_flush_pmeta(as_namespace* ns, uint32_t start_pid, uint32_t n_partitions)
{
	as_storage_flush_pmeta_table[ns->storage_type](ns, start_pid, n_partitions);
}

//--------------------------------------
// as_storage_stats
//

typedef void (*as_storage_stats_fn)(as_namespace* ns, uint32_t* avail_pct, uint64_t* used_bytes);
static const as_storage_stats_fn as_storage_stats_table[] = {
	as_storage_stats_mem,
	as_storage_stats_pmem,
	as_storage_stats_ssd
};

void
as_storage_stats(as_namespace* ns, uint32_t* avail_pct, uint64_t* used_bytes)
{
	as_storage_stats_table[ns->storage_type](ns, avail_pct, used_bytes);
}

//--------------------------------------
// as_storage_device_stats
//

typedef void (*as_storage_device_stats_fn)(const as_namespace* ns, uint32_t device_ix, storage_device_stats* stats);
static const as_storage_device_stats_fn as_storage_device_stats_table[] = {
	as_storage_device_stats_mem,
	as_storage_device_stats_pmem,
	as_storage_device_stats_ssd
};

void
as_storage_device_stats(const as_namespace* ns, uint32_t device_ix, storage_device_stats* stats)
{
	as_storage_device_stats_table[ns->storage_type](ns, device_ix, stats);
}

//--------------------------------------
// as_storage_ticker_stats
//

typedef void (*as_storage_ticker_stats_fn)(as_namespace* ns);
static const as_storage_ticker_stats_fn as_storage_ticker_stats_table[] = {
	as_storage_ticker_stats_mem,
	as_storage_ticker_stats_pmem,
	as_storage_ticker_stats_ssd
};

void
as_storage_ticker_stats(as_namespace* ns)
{
	as_storage_ticker_stats_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_dump_wb_summary
//

typedef void (*as_storage_dump_wb_summary_fn)(const as_namespace* ns, bool verbose);
static const as_storage_dump_wb_summary_fn as_storage_dump_wb_summary_table[] = {
	as_storage_dump_wb_summary_mem,
	as_storage_dump_wb_summary_pmem,
	as_storage_dump_wb_summary_ssd
};

void
as_storage_dump_wb_summary(const as_namespace* ns, bool verbose)
{
	as_storage_dump_wb_summary_table[ns->storage_type](ns, verbose);
}
//--------------------------------------
// as_storage_histogram_clear_all
//

typedef void (*as_storage_histogram_clear_fn)(as_namespace* ns);
static const as_storage_histogram_clear_fn as_storage_histogram_clear_table[] = {
	as_storage_histogram_clear_mem,
	as_storage_histogram_clear_pmem,
	as_storage_histogram_clear_ssd
};

void
as_storage_histogram_clear_all(as_namespace* ns)
{
	as_storage_histogram_clear_table[ns->storage_type](ns);
}


//==========================================================
// Generic functions that don't use "v-tables".
//

void
as_storage_record_get_set_name(as_storage_rd* rd)
{
	rd->set_name = as_index_get_set_name(rd->r, rd->ns);

	if (rd->set_name != NULL) {
		rd->set_name_len = strlen(rd->set_name);
	}
}

bool
as_storage_rd_load_key(as_storage_rd* rd)
{
	if (rd->r->key_stored == 0) {
		return false;
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_key(rd);
	}

	return false;
}
