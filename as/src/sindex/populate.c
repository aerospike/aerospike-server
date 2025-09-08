/*
 * populate.c
 *
 * Copyright (C) 2021-2022 Aerospike, Inc.
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

#include "sindex/populate.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/set_index.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "sindex/sindex_tree.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define N_STARTUP_THREADS 32 // TODO - use 5x CPUs?

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
	uint32_t pid;
	uint64_t n_total_reduced;
	bool aborted;
} populate_info;

typedef struct populate_cb_info_s {
	as_namespace* ns;
	as_sindex* si;
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
static void mark_all_readable(as_namespace* ns);
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

		if (as_sindex_n_sindexes(ns) == 0) {
			continue;
		}

		if (ns->sindexes_resumed_readable) {
			continue;
		}

		populate_startup(ns);

		mark_all_readable(ns);

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
	as_sindex_job_reserve(si);
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

static void
mark_all_readable(as_namespace* ns)
{
	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si != NULL) {
			si->readable = true;
			si->populate_pct = 100;
		}
	}
}

static void*
run_ticker(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;
	uint32_t x;

	while (cf_queue_pop(g_ticker_done_q, &x, TICKER_INTERVAL) != CF_QUEUE_OK) {
		uint64_t n_recs_checked = as_load_uint64(&ns->si_n_recs_checked);
		uint64_t n_objects = as_load_uint64(&ns->n_objects);
		double pct = n_objects == 0 ?
				100.0 : (double)(n_recs_checked * 100) / (double)n_objects;

		cf_info(AS_SINDEX, "{%s} sindex-populate: mem-used %lu objects-scanned %lu progress-pct %.3f",
				ns->name, as_sindex_used_bytes(ns), n_recs_checked, pct);
	}

	return NULL;
}

static void*
run_startup(void* udata)
{
	startup_info* starti = (startup_info*)udata;
	as_namespace* ns = starti->ns;

	startup_cb_info cbi = { .ns = ns };

	uint32_t pid;

	while ((pid = as_faa_uint32(&starti->pid, 1)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_index_tree* tree = ns->partitions[pid].tree;

		if (tree == NULL) {
			continue;
		}

		cbi.tree = tree;

		as_index_reduce(tree, startup_reduce_cb, &cbi);
	}

	return NULL;
}

static bool
startup_reduce_cb(as_index_ref* r_ref, void* udata)
{
	startup_cb_info* cbi = (startup_cb_info*)udata;
	as_namespace* ns = cbi->ns;

	as_record* r = read_r(ns, r_ref->r, false); // is not an MRT

	if (r == NULL) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (++cbi->n_reduced == PROGRESS_RESOLUTION) {
		cbi->n_reduced = 0;
		as_add_uint64(&ns->si_n_recs_checked, PROGRESS_RESOLUTION);
	}

	if (! set_has_sindex(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	// Note - we put expired records in the sindex. Replication does not check
	// if an existing record being replaced is doomed, and will not put the
	// record in the sindex if the bin(s) didn't change.

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS];

	if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
		// TODO - better to just cf_crash?
		cf_warning(AS_SINDEX, "{%s} populating sindexes - failed to read %pD",
				ns->name, &r->keyd);
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return true;
	}

	as_sindex_put_all_rd(ns, &rd, r_ref);

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

	cf_info(AS_SINDEX, "{%s} populating sindex %s ...", ns->name, si->iname);

	uint64_t start_ms = cf_getms();

	populate_info popi = { .ns = ns, .si = si };

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
				si->iname);
		return;
	}

	si->readable = true;
	si->populate_pct = 100;

	// TODO - really nead loadtime stat? Doesn't last through restart.
	si->load_time = cf_getms() - start_ms;

	cf_info(AS_SINDEX, "{%s} ... done populating sindex %s (%lu ms)", ns->name,
			si->iname, si->load_time);
}

static void*
run_populate(void* udata)
{
	populate_info* popi = (populate_info*)udata;
	as_namespace* ns = popi->ns;

	populate_cb_info cbi = {
			.ns = ns,
			.si = popi->si,
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

		if (! as_set_index_reduce(ns, tree, popi->si->set_id, NULL,
				populate_reduce_cb, &cbi)) {
			as_index_reduce(tree, populate_reduce_cb, &cbi);
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

	if (si->dropped) {
		*cbi->p_aborted = true;
		as_record_done(r_ref, ns);
		return false;
	}

	if (! as_namespace_sindex_persisted(ns) && ns->memory_breached) {
		cf_warning(AS_SINDEX, "{%s} populating sindex %s - aborted due to memory limit breach",
				ns->name, si->iname);
		si->error = true; // an error, once set, can't be cleared until restart
		*cbi->p_aborted = true;
		as_record_done(r_ref, ns);
		return false;
	}

	as_record* r = read_r(ns, r_ref->r, false); // is not an MRT

	if (r == NULL) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (++cbi->n_reduced == PROGRESS_RESOLUTION) {
		cbi->n_reduced = 0;

		uint64_t n = as_aaf_uint64(cbi->p_n_total_reduced, PROGRESS_RESOLUTION);
		uint64_t n_objects = as_load_uint64(&ns->n_objects);

		si->populate_pct = (uint32_t)
				(n > n_objects ? 100 : (n * 100) / n_objects);
	}

	// This also populates whole-namespace sindexes.
	if (si->set_id != INVALID_SET_ID && si->set_id != as_index_get_set_id(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	// Note - we put doomed records in the sindex. Replication does not check
	// if an existing record being replaced is doomed, and will not put the
	// record in the sindex if the bin(s) didn't change.

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS];

	if (as_storage_rd_lazy_load_bins(&rd, stack_bins) < 0) {
		cf_warning(AS_SINDEX, "{%s} populating sindex %s - failed to read %pD",
				ns->name, si->iname, &r->keyd);
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return true;
	}

	as_sindex_put_rd(si, &rd, r_ref);

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
		as_sindex_tree_collect_cardinality(si);
		as_sindex_job_release(si);
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

		as_sindex_tree_destroy(si);

		if (si->ctx_b64 != NULL) {
			cf_free(si->ctx_b64);
		}

		if (si->ctx_buf != NULL) {
			cf_free(si->ctx_buf);
		}

		if (si->exp != NULL) {
			as_exp_destroy(si->exp);
		}

		if (si->exp_buf != NULL) {
			cf_free(si->exp_buf);
		}

		if (si->exp_b64 != NULL) {
			cf_free(si->exp_b64);
		}

		if (si->exp_bin_names != NULL) {
			cf_vector_destroy(si->exp_bin_names);
		}

		cf_rc_free(si);
	}

	return NULL;
}
