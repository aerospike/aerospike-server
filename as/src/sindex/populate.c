/*
 * populate.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include "../../include/sindex/populate.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "log.h"

#include "ai_btree.h"
#include "ai_obj.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/set_index.h"
#include "fabric/partition.h"
#include "sindex/secondary_index.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define N_STARTUP_THREADS 32 // FIXME - what?

#define TICKER_INTERVAL (5 * 1000) // 5 seconds
#define PROGRESS_RESOLUTION 1000

typedef struct startup_info_s {
	as_namespace* ns;
	uint32_t pid;
} startup_info;

typedef struct startup_cb_info_s {
	as_namespace* ns;
	as_index_tree* tree;
	uint32_t n_reduced;
} startup_cb_info;

typedef struct populate_info_s {
	as_namespace* ns;
	as_sindex* si;
	uint16_t set_id;
	uint32_t pid;
	uint64_t n_total_reduced;
	bool aborted;
} populate_info;

typedef struct populate_cb_info_s {
	as_namespace* ns;
	as_sindex* si;
	uint16_t set_id;
	as_index_tree* tree;
	uint64_t* p_n_total_reduced;
	bool* p_aborted;
	uint32_t n_reduced;
} populate_cb_info;


//==========================================================
// Globals.
//

static cf_queue* g_ticker_done_q; // re-used by serialized namespaces

static cf_queue g_add_sindex_q;
static cf_queue g_destroy_sindex_q;


//==========================================================
// Forward declarations.
//

static void populate_startup(as_namespace* ns);
static void* run_ticker(void* udata);
static void* run_startup(void* udata);
static bool startup_reduce_cb(as_index_ref* r_ref, void* udata);

static void populate(as_sindex* si);
static void* run_populate(void* udata);
static bool populate_reduce_cb(as_index_ref* r_ref, void* udata);

static void* run_add_sindex(void* udata);
static void* run_destroy_sindex(void* udata);


//==========================================================
// Inlines & macros.
//


//==========================================================
// Public API.
//

void
as_sindex_populate_startup(void)
{
	g_ticker_done_q = cf_queue_create(sizeof(uint32_t), true);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		if (ns->sindex_cnt == 0) {
			continue;
		}

		if (! ns->storage_data_in_memory) {
			populate_startup(ns);
		}
		// else - data-in-memory (cold or cool restart) - already built sindex.

		cf_info(AS_SINDEX, "{%s} sindex population done", ns->name);
	}

	cf_queue_destroy(g_ticker_done_q);

	cf_queue_init(&g_add_sindex_q, sizeof(as_sindex*), 4, true);
	cf_queue_init(&g_destroy_sindex_q, sizeof(as_sindex*), 4, true);

	cf_thread_create_detached(run_add_sindex, NULL);
	cf_thread_create_detached(run_destroy_sindex, NULL);
}

void
as_sindex_populate_add(as_sindex* si)
{
	as_sindex_reserve(si); // for ref in queue
	cf_queue_push(&g_add_sindex_q, &si);
}

void
as_sindex_populate_destroy(as_sindex* si)
{
	cf_queue_push(&g_destroy_sindex_q, &si);
}


//==========================================================
// Local helpers - startup.
//

static void
populate_startup(as_namespace* ns)
{
	cf_tid ticker_tid = cf_thread_create_joinable(run_ticker, ns);

	if (ns->storage_sindex_startup_device_scan) {
		cf_info(AS_SINDEX, "{%s} populating sindex by device scan", ns->name);

		// For now, no function table - only get here for "SSD" namespaces.
		as_storage_sindex_build_all_ssd(ns);
	}
	else {
		cf_info(AS_SINDEX, "{%s} populating sindex by index scan", ns->name);

		startup_info starti = { .ns = ns };

		cf_tid tids[N_STARTUP_THREADS];

		for (uint32_t i = 0; i < N_STARTUP_THREADS; i++) {
			tids[i] = cf_thread_create_joinable(run_startup, &starti);
		}

		for (uint32_t i = 0; i < N_STARTUP_THREADS; i++) {
			cf_thread_join(tids[i]);
		}
	}

	uint32_t x = 0;

	cf_queue_push(g_ticker_done_q, &x);
	cf_thread_join(ticker_tid);
}

static void*
run_ticker(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;
	uint32_t x;

	while (cf_queue_pop(g_ticker_done_q, &x, TICKER_INTERVAL) != CF_QUEUE_OK) {
		uint64_t n_recs_checked = as_load_uint64(&ns->si_n_recs_checked);
		uint64_t n_objects = ns->n_objects;
		double pct = n_objects == 0 ?
				100.0 : (double)(n_recs_checked * 100) / (double)n_objects;

		cf_info(AS_SINDEX, "{%s} sindex-ticker: mem-used %lu objects-scanned %lu progress-pct %.3f",
				ns->name, ns->n_bytes_sindex_memory, n_recs_checked, pct);
	}

	return NULL;
}

static void*
run_startup(void* udata)
{
	startup_info* starti = (startup_info*)udata;
	as_namespace* ns = starti->ns;

	CF_ALLOC_SET_NS_ARENA(ns);

	startup_cb_info cbi = { .ns = ns };

	uint32_t pid;

	while ((pid = as_faa_uint32(&starti->pid, 1)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_index_tree* tree = ns->partitions[pid].tree;

		if (tree == NULL) {
			continue;
		}

		cbi.tree = tree;

		as_index_reduce_live(tree, startup_reduce_cb, &cbi);
	}

	return NULL;
}

static bool
startup_reduce_cb(as_index_ref* r_ref, void* udata)
{
	startup_cb_info* cbi = (startup_cb_info*)udata;
	as_namespace* ns = cbi->ns;

	if (++cbi->n_reduced == PROGRESS_RESOLUTION) {
		cbi->n_reduced = 0;
		as_add_uint64(&ns->si_n_recs_checked, PROGRESS_RESOLUTION);
	}

	as_index* r = r_ref->r;

	if (! set_has_sindex(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	// Note - we put expired records in the sindex. Replication does not check
	// if an existing record being replaced is doomed, and will not put the
	// record in the sindex if the bin(s) didn't change.

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS]; // no sindexes for single-bin

	if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
		// FIXME - better to just cf_crash?
		cf_warning(AS_SINDEX, "{%s} populating sindexes - failed to read %pD",
				ns->name, &r->keyd);
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return true;
	}

	as_sindex_putall_rd(ns, &rd, r_ref);

	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - runtime job.
//

static void
populate(as_sindex* si)
{
	as_namespace* ns = si->ns;
	as_sindex_metadata* imd = si->imd;

	cf_info(AS_SINDEX, "{%s} populating sindex %s ...", ns->name, imd->iname);

	uint64_t start_ms = cf_getms();

	// TODO - might be nice to put set_id on si and/or imd?
	char* set_name = imd->set;
	uint16_t set_id = set_name != NULL ?
			as_namespace_get_set_id(ns, set_name) : INVALID_SET_ID;

	populate_info popi = {
			.ns = ns,
			.si = si,
			.set_id = set_id
	};

	uint32_t n_threads = as_load_uint32(&g_config.sindex_builder_threads);
	cf_tid tids[n_threads];

	for (uint32_t i = 0; i < n_threads; i++) {
		tids[i] = cf_thread_create_joinable(run_populate, &popi);
	}

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_join(tids[i]);
	}

	if (popi.aborted) {
		cf_info(AS_SINDEX, "{%s} ... aborted populating sindex %s", ns->name,
				si->imd->iname);
		return;
	}

	si->readable = true;
	si->stats.populate_pct = 100;

	// TODO - really nead loadtime stat? Doesn't last through restart.
	si->stats.loadtime = cf_getms() - start_ms;

	cf_info(AS_SINDEX, "{%s} ... done populating sindex %s (%lu ms)", ns->name,
			si->imd->iname, si->stats.loadtime);
}

static void*
run_populate(void* udata)
{
	populate_info* popi = (populate_info*)udata;
	as_namespace* ns = popi->ns;
	uint16_t set_id = popi->set_id;

	CF_ALLOC_SET_NS_ARENA(ns);

	populate_cb_info cbi = {
			.ns = ns,
			.si = popi->si,
			.set_id = set_id,
			.p_n_total_reduced = &popi->n_total_reduced,
			.p_aborted = &popi->aborted
	};

	uint32_t pid;

	while ((pid = as_faa_uint32(&popi->pid, 1)) < AS_PARTITIONS) {
		if (popi->aborted) {
			break;
		}

		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		as_index_tree* tree = rsv.tree;

		if (tree == NULL) {
			as_partition_release(&rsv);
			continue;
		}

		cbi.tree = tree;

		if (! as_set_index_reduce(ns, tree, set_id, NULL, populate_reduce_cb,
				&cbi)) {
			as_index_reduce_live(tree, populate_reduce_cb, &cbi);
		}

		as_partition_release(&rsv);
	}

	return NULL;
}

static bool
populate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	populate_cb_info* cbi = (populate_cb_info*)udata;
	as_namespace* ns = cbi->ns;
	as_sindex* si = cbi->si;

	if (*cbi->p_aborted) {
		as_record_done(r_ref, ns);
		return false;
	}

	if (++cbi->n_reduced == PROGRESS_RESOLUTION) {
		cbi->n_reduced = 0;

		uint64_t n = as_aaf_uint64(cbi->p_n_total_reduced, PROGRESS_RESOLUTION);
		uint64_t n_objects = ns->n_objects;

		si->stats.populate_pct = (uint32_t)
				(n > n_objects ? 100 : (n * 100) / n_objects);
	}

	as_index* r = r_ref->r;

	// This comparison also works building index on records not in a set.
	if (cbi->set_id != as_index_get_set_id(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	// Note - we put doomed records in the sindex. Replication does not check
	// if an existing record being replaced is doomed, and will not put the
	// record in the sindex if the bin(s) didn't change.

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS]; // no sindexes for single-bin

	if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
		cf_warning(AS_SINDEX, "{%s} populating sindex %s - failed to read %pD",
				ns->name, si->imd->iname, &r->keyd);
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_sindex_put_rd(si, &rd, r_ref)) {
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		*cbi->p_aborted = true;
		return false; // cancelled because sindex dropped
	}

	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - runtime lifecycle.
//

static void*
run_add_sindex(void* udata)
{
	(void)udata;

	while (true) {
		as_sindex* si;

		cf_queue_pop(&g_add_sindex_q, &si, CF_QUEUE_FOREVER);

		populate(si);
		as_sindex_release(si);
	}

	return NULL;
}

static void*
run_destroy_sindex(void* udata)
{
	(void)udata;

	while (true) {
		as_sindex* si;

		cf_queue_pop(&g_destroy_sindex_q, &si, CF_QUEUE_FOREVER);

		SINDEX_GWLOCK();

		cf_assert(si->state == AS_SINDEX_DESTROY, AS_SINDEX,
				"bad state %d at cleanup - expected %d for %p and %s",
				si->state, AS_SINDEX_DESTROY, si,
				si != NULL ? (si->imd != NULL ? si->imd->iname : NULL) : NULL);

		as_sindex_delete_defn(si->ns, si->imd);

		// Free entire usage counter before tree destroy.
		cf_atomic64_sub(&si->ns->n_bytes_sindex_memory,
				(int64_t)(ai_btree_get_isize(si->imd) +
						ai_btree_get_nsize(si->imd)));

		// Cache the ibtr pointers.
		uint32_t n_pimds = si->imd->n_pimds;
		struct btree* ibtr[n_pimds];

		for (uint32_t i = 0; i < n_pimds; i++) {
			as_sindex_pmetadata* pimd = &si->imd->pimd[i];

			ibtr[i] = pimd->ibtr;
			ai_btree_reset_pimd(pimd);
		}

		as_sindex_destroy_pmetadata(si);
		si->state = AS_SINDEX_INACTIVE;

		si->ns->sindex_cnt--;

		if (si->imd->set != NULL) {
			as_set* p_set = as_namespace_get_set_by_name(si->ns, si->imd->set);

			p_set->n_sindexes--;
		}
		else {
			si->ns->n_setless_sindexes--;
		}

		as_sindex_metadata* imd = si->imd;

		si->imd = NULL;

		as_namespace* ns = si->ns;

		si->ns = NULL;

		if (si->recreate_imd != NULL) {
			as_sindex_metadata* recreate_imd = si->recreate_imd;
			si->recreate_imd = NULL;

			as_sindex_create_lockless(ns, recreate_imd);
			as_sindex_imd_free(recreate_imd);
			cf_rc_free(recreate_imd);
		}

		// Remember this is going to release the write lock of meta-data first.
		// This is the only special case where both GLOCK and LOCK is called
		// together.
		SINDEX_GWUNLOCK();

		// Destroy cached ibtr pointer.
		for (uint32_t i = 0; i < imd->n_pimds; i++) {
			ai_btree_delete_ibtr(ibtr[i]);
		}

		as_sindex_imd_free(imd);
		cf_rc_free(imd);
	}

	return NULL;
}
