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
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "log.h"

#include "ai_btree.h"
#include "ai_obj.h"
#include "bt_iterator.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/stats.h"
#include "sindex/secondary_index.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef struct gc_offset_s {
	ai_obj ibtr_last_key;
	cf_arenax_handle nbtr_last_key;
	bool done;
} gc_offset;

// TODO - Find the correct values.
#define CREATE_LIST_PER_ITERATION_LIMIT 10000
#define PROCESS_LIST_PER_ITERATION_LIMIT  10


//==========================================================
// Forward declarations.
//

static void gc_ns_cycle(as_namespace* ns);
static void gc_ns(as_namespace* ns);
static void gc_si(as_sindex* si, uint64_t* n_cleaned);
static void gc_si_pimd(as_sindex* si, as_sindex_pmetadata* pimd, uint64_t* n_cleaned);
static bool gc_create_list(as_sindex* si, as_sindex_pmetadata* pimd, cf_ll* gc_list, gc_offset* offsetp);
static void gc_process_list(as_sindex* si, as_sindex_pmetadata* pimd, cf_ll* gc_list, uint64_t* n_cleaned);
static int ll_sindex_gc_reduce_fn(cf_ll_element* ele, void* udata);


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

	cf_mutex_unlock(&ns->si_gc_list_mutex);
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
	uint64_t n_cleaned = 0;

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

		gc_si(si, &n_cleaned);

		as_sindex_release(si);
	}

	ns->n_sindex_gc_cleaned += n_cleaned;

	cf_info(AS_SINDEX, "{%s} sindex-gc-done: cleaned (%lu,%lu) total-ms %lu",
			ns->name, ns->n_sindex_gc_cleaned, n_cleaned,
			cf_getms() - start_ms);
}

static void
gc_si(as_sindex* si, uint64_t* n_cleaned) {
	for (uint32_t i = 0; i < si->imd->n_pimds; i++) {
		as_sindex_pmetadata* pimd = &si->imd->pimd[i];

		gc_si_pimd(si, pimd, n_cleaned);
	}
}

static void
gc_si_pimd(as_sindex* si, as_sindex_pmetadata* pimd, uint64_t* n_cleaned)
{
	gc_offset offset; // skey + r_h offset

	PIMD_RLOCK(&pimd->slock);

	bool has_keys = assignMinKey(pimd->ibtr, &offset.ibtr_last_key);

	PIMD_RUNLOCK(&pimd->slock);

	if(! has_keys) {
		return; // no keys in ibtr - nothing to GC
	}

	offset.nbtr_last_key = 0;
	offset.done = false;

	cf_ll gc_list;

	cf_ll_init(&gc_list, &ai_btree_gc_list_destroy_fn, false);

	while (true) {
		// Checking state without lock - if we read a stale value we will delete
		// a few handles. It is ok as we have a tree reference.
		if (si->state == AS_SINDEX_DESTROY) {
			break;
		}

		if (! gc_create_list(si, pimd, &gc_list, &offset)) {
			break;
		}

		if (cf_ll_size(&gc_list) > 0) {
			gc_process_list(si, pimd, &gc_list, n_cleaned);
			cf_ll_reduce(&gc_list, true, ll_sindex_gc_reduce_fn, NULL);
		}

		if (offset.done) {
			break;
		}
	}

	cf_ll_reduce(&gc_list, true, ll_sindex_gc_reduce_fn, NULL);
}

// true if tree is done
// false if more in tree
static bool
gc_create_list(as_sindex* si, as_sindex_pmetadata* pimd, cf_ll* gc_list,
		gc_offset* offsetp)
{
	uint64_t limit_per_iteration = CREATE_LIST_PER_ITERATION_LIMIT;

	PIMD_RLOCK(&pimd->slock);

	as_sindex_status status = ai_btree_build_defrag_list(si->imd, pimd,
			&offsetp->ibtr_last_key, &offsetp->nbtr_last_key,
			limit_per_iteration, gc_list);

	PIMD_RUNLOCK(&pimd->slock);

	if (status == AS_SINDEX_DONE) {
		offsetp->done = true;
	}

	return status != AS_SINDEX_ERR;
}

static void
gc_process_list(as_sindex* si, as_sindex_pmetadata* pimd, cf_ll* gc_list,
		uint64_t* n_cleaned)
{
	uint64_t n_deleted = 0;
	uint64_t limit_per_iteration = PROCESS_LIST_PER_ITERATION_LIMIT;

	bool more = true;

	while (more) {
		PIMD_WLOCK(&pimd->slock);

		more = ai_btree_defrag_list(si->imd, pimd, gc_list,
				limit_per_iteration, &n_deleted);

		PIMD_WUNLOCK(&pimd->slock);
	}

	// Update secondary index object count statistics aggressively.
	cf_atomic64_add(&si->stats.n_objects, (int64_t)-n_deleted);
	cf_atomic64_add(&si->stats.n_defrag_records, (int64_t)n_deleted);

	*n_cleaned += n_deleted;
}

static int
ll_sindex_gc_reduce_fn(cf_ll_element* ele, void* udata)
{
	(void)ele;
	(void)udata;

	return CF_LL_REDUCE_DELETE;
}
