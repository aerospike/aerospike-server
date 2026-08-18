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
// Bind storage ops to namespace after storage_type is known.
//

// Any writer of ns->storage_type must call this immediately after setting it -
// today that is the two config parsers, and test fixtures that build a
// namespace by hand. Every namespace reaching this function must have a valid
// storage engine: leaving storage_ops NULL defers the failure to a null call
// through one of the dispatch functions below, arbitrarily far from the real
// mistake, and only the first dispatch (as_storage_init) asserts on it.
void
as_storage_bind_ops(as_namespace* ns)
{
	switch (ns->storage_type) {
	case AS_STORAGE_ENGINE_MEMORY:
		ns->storage_ops = &as_storage_ops_mem;
		break;
	case AS_STORAGE_ENGINE_PMEM:
		ns->storage_ops = &as_storage_ops_pmem;
		break;
	case AS_STORAGE_ENGINE_SSD:
		ns->storage_ops = &as_storage_ops_ssd;
		break;
	default:
		cf_crash(AS_STORAGE, "{%s} invalid storage engine %d", ns->name,
				ns->storage_type);
	}
}

//==========================================================
// Generic "base class" functions that call through
// storage-engine ops.
//

//--------------------------------------
// as_storage_init
//

void
as_storage_init(void)
{
	// Includes resuming indexes for warm restarts.

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// First dispatch for this namespace - a namespace that never reached a
		// storage-engine stanza would arrive here unbound.
		cf_assert(ns->storage_ops != NULL, AS_STORAGE,
				"{%s} storage ops not bound", ns->name);

		ns->storage_ops->init(ns);
	}

	if (AS_NODE_STORAGE_SZ != 0 && g_unique_data_size > AS_NODE_STORAGE_SZ) {
		cf_crash_nostack(AS_STORAGE, "Community Edition limit exceeded");
	}
}

//--------------------------------------
// as_storage_load
//

#define TICKER_INTERVAL (5 * 1000) // 5 seconds

void
as_storage_load(void)
{
	// Includes device scans for cold starts.

	cf_queue complete_q;

	cf_queue_init(&complete_q, sizeof(void*), g_config.n_namespaces, true);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->storage_ops->load(ns, &complete_q);
	}

	// Wait for completion - cold starts may take a while.

	for (uint32_t n_done = 0; n_done < g_config.n_namespaces; n_done++) {
		void* _t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace* ns = g_config.namespaces[ns_ix];

				if (ns->loading_records) {
					ns->storage_ops->load_ticker(ns);
				}
			}
		}
	}

	uint32_t n_2nd_pass = 0;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		if (drv_load_needs_2nd_pass(ns)) {
			ns->storage_ops->load(ns, &complete_q);
			n_2nd_pass++;
		}
	}

	for (uint32_t n_done = 0; n_done < n_2nd_pass; n_done++) {
		void* _t;

		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace* ns = g_config.namespaces[ns_ix];

				if (drv_load_needs_2nd_pass(ns) && ns->loading_records) {
					ns->storage_ops->load_ticker(ns);
				}
			}
		}
	}

	cf_queue_destroy(&complete_q);
}

//--------------------------------------
// as_storage_activate
//

void
as_storage_activate(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->storage_ops->activate(ns);
	}

	while (true) {
		bool any_defragging = false;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_namespace* ns = g_config.namespaces[ns_ix];

			if (ns->storage_ops->wait_for_defrag(ns)) {
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

void
as_storage_start_tomb_raider(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->storage_ops->start_tomb_raider(ns);
	}
}

//--------------------------------------
// as_storage_shutdown
//

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
		ns->storage_ops->shutdown(ns);

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

void
as_storage_destroy_record(as_namespace* ns, as_record* r)
{
	ns->storage_ops->destroy_record(ns, r);
}

//--------------------------------------
// as_storage_record_create
//

// Used to have table functions, but no foreseeable need - so we removed them.
void
as_storage_record_create(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	*rd = (as_storage_rd){ .r = r, .ns = ns, .which_current_swb = SWB_MASTER };

	// Ancient paranoia...
	cf_assert(r->rblock_id == 0, AS_STORAGE, "uninitialized rblock-id");
}

//--------------------------------------
// as_storage_record_open
//

void
as_storage_record_open(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	*rd = (as_storage_rd){
		.r = r, .ns = ns, .record_on_device = true, .which_current_swb = SWB_MASTER
	};

	// Sets the device (union) pointer.
	ns->storage_ops->record_open(rd);
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

int
as_storage_record_load_bins(as_storage_rd* rd)
{
	return rd->ns->storage_ops->record_load_bins(rd);
}

//--------------------------------------
// as_storage_record_load_key
//

bool
as_storage_record_load_key(as_storage_rd* rd)
{
	return rd->ns->storage_ops->record_load_key(rd);
}

//--------------------------------------
// as_storage_record_load_pickle
//

bool
as_storage_record_load_pickle(as_storage_rd* rd)
{
	return rd->ns->storage_ops->record_load_pickle(rd);
}

//--------------------------------------
// as_storage_record_load_raw
//

bool
as_storage_record_load_raw(as_storage_rd* rd, bool leave_encrypted)
{
	return rd->ns->storage_ops->record_load_raw(rd, leave_encrypted);
}

//--------------------------------------
// as_storage_record_write
//

int
as_storage_record_write(as_storage_rd* rd)
{
	return rd->ns->storage_ops->record_write(rd);
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
		cf_ticker_warning(AS_STORAGE,
				"{%s} %s fail: queue too deep: exceeds max %u", ns->name, tag,
				limit);
		return true;
	}

	return false;
}

//--------------------------------------
// as_storage_defrag_sweep
//

void
as_storage_defrag_sweep(as_namespace* ns)
{
	ns->storage_ops->defrag_sweep(ns);
}

//--------------------------------------
// as_storage_load_regime
//

void
as_storage_load_regime(as_namespace* ns)
{
	ns->storage_ops->load_regime(ns);
}

//--------------------------------------
// as_storage_save_regime
//

void
as_storage_save_regime(as_namespace* ns)
{
	ns->storage_ops->save_regime(ns);
}

//--------------------------------------
// as_storage_load_roster_generation
//

void
as_storage_load_roster_generation(as_namespace* ns)
{
	ns->storage_ops->load_roster_generation(ns);
}

//--------------------------------------
// as_storage_save_roster_generation
//

void
as_storage_save_roster_generation(as_namespace* ns)
{
	ns->storage_ops->save_roster_generation(ns);
}

//--------------------------------------
// as_storage_load_pmeta
//

void
as_storage_load_pmeta(as_namespace* ns, as_partition* p)
{
	ns->storage_ops->load_pmeta(ns, p);
}

//--------------------------------------
// as_storage_save_pmeta
//

void
as_storage_save_pmeta(as_namespace* ns, const as_partition* p)
{
	ns->storage_ops->save_pmeta(ns, p);
}

//--------------------------------------
// as_storage_cache_pmeta
//

void
as_storage_cache_pmeta(as_namespace* ns, const as_partition* p)
{
	ns->storage_ops->cache_pmeta(ns, p);
}

//--------------------------------------
// as_storage_flush_pmeta
//

void
as_storage_flush_pmeta(as_namespace* ns, uint32_t start_pid, uint32_t n_partitions)
{
	ns->storage_ops->flush_pmeta(ns, start_pid, n_partitions);
}

//--------------------------------------
// as_storage_stats
//

void
as_storage_stats(as_namespace* ns, uint32_t* avail_pct, uint64_t* used_bytes)
{
	ns->storage_ops->stats(ns, avail_pct, used_bytes);
}

//--------------------------------------
// as_storage_device_stats
//

void
as_storage_device_stats(const as_namespace* ns, uint32_t device_ix,
		storage_device_stats* stats)
{
	ns->storage_ops->device_stats(ns, device_ix, stats);
}

//--------------------------------------
// as_storage_ticker_stats
//

void
as_storage_ticker_stats(as_namespace* ns)
{
	ns->storage_ops->ticker_stats(ns);
}

//--------------------------------------
// as_storage_dump_wb_summary
//

void
as_storage_dump_wb_summary(const as_namespace* ns, bool verbose)
{
	ns->storage_ops->dump_wb_summary(ns, verbose);
}
//--------------------------------------
// as_storage_histogram_clear_all
//

void
as_storage_histogram_clear_all(as_namespace* ns)
{
	ns->storage_ops->histogram_clear(ns);
}

//==========================================================
// Generic functions that don't use storage ops.
//

void
as_storage_record_get_set_name(as_storage_rd* rd)
{
	uint16_t set_id = as_index_get_set_id(rd->r);

	if (set_id == INVALID_SET_ID) {
		rd->set_name = NULL;
		rd->p_set = NULL;
		return;
	}

	as_set* p_set;

	if (cf_vmapx_get_by_index(rd->ns->p_sets_vmap, (uint32_t)set_id - 1,
				(void**)&p_set) == CF_VMAPX_OK) {
		rd->p_set = p_set;
		rd->set_name = p_set->name;
		rd->set_name_len = strlen(p_set->name);
	}
	else {
		rd->p_set = NULL;
		rd->set_name = NULL;
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

	if (rd->key_size != 0) {
		cf_assert(rd->key != NULL, AS_STORAGE, "key_size set without key");
		return true;
	}

	return false;
}
