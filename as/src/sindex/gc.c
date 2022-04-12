/*
 * gc.c
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

#include "sindex/gc.h"

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/stats.h"
#include "sindex/secondary_index.h"
#include "sindex/sindex_tree.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define THROTTLE_THRESHOLD (64 * 1024 * 1024)


//==========================================================
// Forward declarations.
//

static void gc_ns_cycle(as_namespace* ns);
static void gc_ns(as_namespace* ns);


//==========================================================
// Public API.
//

void
as_sindex_gc_ns_init(as_namespace* ns)
{
	cf_mutex_init(&ns->si_gc_list_mutex);

	create_rlist(ns);
	ns->si_gc_tlist = cf_queue_create(sizeof(as_index_tree*), false);
}

void*
as_sindex_run_gc(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;
	uint64_t last_time = cf_get_seconds();

	while (true) {
		sleep(1);

		uint64_t period = as_load_uint32(&g_config.sindex_gc_period);
		uint64_t curr_time = cf_get_seconds();

		if (period == 0 || curr_time - last_time < period) {
			continue;
		}

		last_time = curr_time;

		gc_ns_cycle(ns);
	}
}

void
as_sindex_gc_record(as_namespace* ns, as_index_ref* r_ref)
{
	if (r_ref->r->in_sindex == 0) {
		return;
	}

	cf_assert(! ns->storage_data_in_memory, AS_SINDEX,
			"data-in-memory ns queuing to rlist");

	cf_mutex_lock(&ns->si_gc_list_mutex);

	push_to_rlist(ns, r_ref);
	ns->si_gc_rlist_full = cf_queue_sz(ns->si_gc_rlist) >= THROTTLE_THRESHOLD;

	cf_mutex_unlock(&ns->si_gc_list_mutex);
}

void
as_sindex_gc_record_throttle(as_namespace* ns)
{
	if (! ns->si_gc_rlist_full) {
		return;
	}

	while (true) {
		cf_ticker_info(AS_SINDEX, "{%s} gc rlist full - throttling", ns->name);

		sleep(1);

		cf_mutex_lock(&ns->si_gc_list_mutex);

		if (cf_queue_sz(ns->si_gc_rlist) < THROTTLE_THRESHOLD) {
			ns->si_gc_rlist_full = false; // optional - but can't hurt
			cf_mutex_unlock(&ns->si_gc_list_mutex);
			return;
		}

		cf_mutex_unlock(&ns->si_gc_list_mutex);
	}
}

void
as_sindex_gc_tree(as_namespace* ns, as_index_tree* tree)
{
	as_partition* p = (as_partition*)tree->udata;

	cf_mutex_lock(&ns->si_gc_list_mutex);

	cf_queue_push(ns->si_gc_tlist, &tree);
	ns->si_gc_tlist_map[p->id][tree->id] = true;

	cf_mutex_unlock(&ns->si_gc_list_mutex);
}


//==========================================================
// Local helpers.
//

static void
gc_ns_cycle(as_namespace* ns)
{
	cf_mutex_lock(&ns->si_gc_list_mutex);

	cf_queue* rlist = ns->si_gc_rlist;
	cf_queue* tlist = ns->si_gc_tlist;

	// rlist is always empty for in-memory ns.
	if (cf_queue_sz(rlist) == 0 && cf_queue_sz(tlist) == 0) {
		cf_mutex_unlock(&ns->si_gc_list_mutex);
		return;
	}

	create_rlist(ns);
	ns->si_gc_tlist = cf_queue_create(sizeof(as_index_tree*), false);

	cf_mutex_unlock(&ns->si_gc_list_mutex);

	gc_ns(ns);

	purge_rlist(ns, rlist);

	as_index_tree* tree;

	while (cf_queue_pop(tlist, &tree, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
		as_partition* p = (as_partition*)tree->udata;

		cf_mutex_lock(&ns->si_gc_list_mutex);

		ns->si_gc_tlist_map[p->id][tree->id] = false;

		cf_mutex_unlock(&ns->si_gc_list_mutex);

		as_index_tree_gc(tree);
	}

	cf_queue_destroy(tlist);
}

static void
gc_ns(as_namespace* ns)
{
	cf_info(AS_SINDEX, "{%s} sindex-gc-start", ns->name);

	uint64_t start_ms = cf_getms();
	uint64_t n_cleaned = ns->n_sindex_gc_cleaned;

	// Avoid using ns->sindex_cnt as it needs a lock (for entire GC cycle).
	for (uint32_t i = 0; i < AS_SINDEX_MAX; i++) {
		SINDEX_GRLOCK();

		as_sindex* si = &ns->sindex[i];

		if (! as_sindex_isactive(si)) {
			SINDEX_GRUNLOCK();
			continue;
		}

		as_sindex_reserve(si);

		SINDEX_GRUNLOCK();

		as_sindex_tree_gc(si);

		as_sindex_release(si);
	}

	cf_info(AS_SINDEX, "{%s} sindex-gc-done: cleaned (%lu,%lu) total-ms %lu",
			ns->name, ns->n_sindex_gc_cleaned,
			ns->n_sindex_gc_cleaned - n_cleaned, cf_getms() - start_ms);
}
