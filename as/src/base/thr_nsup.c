/*
 * thr_nsup.c
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

/*
 * namespace supervisor
 */

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h> // for MIN and MAX

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "hardware.h"
#include "linear_hist.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/thr_sindex.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/xdr_serverside.h"
#include "fabric/partition.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

#define EVAL_STOP_WRITES_PERIOD 10 // seconds


//==========================================================
// Forward declarations.
//

static bool eval_stop_writes(as_namespace *ns);
static bool eval_hwm_breached(as_namespace *ns);
static void collect_size_histograms(as_namespace *ns);
static void size_histograms_reduce_cb(as_index_ref* r_ref, void* udata);


//==========================================================
// Eviction during cold start.
//
// No real need for this to be in thr_nsup.c, except maybe
// for convenient comparison to run-time eviction.
//

#define EVAL_WRITE_STATE_FREQUENCY 1024
#define COLD_START_HIST_MIN_BUCKETS 100000 // histogram memory is transient


//------------------------------------------------
// Reduce callback prepares for cold start eviction.
// - builds cold start eviction histogram
//
typedef struct cold_start_evict_prep_info_s {
	as_namespace*		ns;
	linear_hist*		hist;
	bool*				sets_not_evicting;
} cold_start_evict_prep_info;

static void
cold_start_evict_prep_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	cold_start_evict_prep_info* p_info = (cold_start_evict_prep_info*)udata;
	uint32_t set_id = as_index_get_set_id(r);
	uint32_t void_time = r->void_time;

	if (void_time != 0 &&
			! p_info->sets_not_evicting[set_id]) {
		linear_hist_insert_data_point(p_info->hist, void_time);
	}

	as_record_done(r_ref, p_info->ns);
}

//------------------------------------------------
// Threads prepare for cold start eviction.
//
typedef struct evict_prep_thread_info_s {
	as_namespace*		ns;
	cf_atomic32*		p_pid;
	uint32_t			i_cpu;
	linear_hist*		hist;
	bool*				sets_not_evicting;
} evict_prep_thread_info;

void*
run_cold_start_evict_prep(void* udata)
{
	evict_prep_thread_info* p_info = (evict_prep_thread_info*)udata;

	cf_topo_pin_to_cpu((cf_topo_cpu_index)p_info->i_cpu);

	as_namespace *ns = p_info->ns;

	cold_start_evict_prep_info cb_info;

	cb_info.ns = ns;
	cb_info.hist = p_info->hist;
	cb_info.sets_not_evicting = p_info->sets_not_evicting;

	int pid;

	while ((pid = (int)cf_atomic32_incr(p_info->p_pid)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_index_reduce_live(ns->partitions[pid].tree, cold_start_evict_prep_reduce_cb, &cb_info);
	}

	return NULL;
}

//------------------------------------------------
// Reduce callback evicts records on cold start.
// - evicts based on calculated threshold
//
typedef struct cold_start_evict_info_s {
	as_namespace*	ns;
	as_partition*	p_partition;
	bool*			sets_not_evicting;
	uint32_t		num_evicted;
	uint32_t		num_0_void_time;
} cold_start_evict_info;

static void
cold_start_evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	cold_start_evict_info* p_info = (cold_start_evict_info*)udata;
	as_namespace* ns = p_info->ns;
	as_partition* p_partition = p_info->p_partition;
	uint32_t set_id = as_index_get_set_id(r);
	uint32_t void_time = r->void_time;

	if (void_time != 0) {
		if (! p_info->sets_not_evicting[set_id] &&
				void_time < ns->cold_start_threshold_void_time) {
			as_index_delete(p_partition->tree, &r->keyd);
			p_info->num_evicted++;
		}
	}
	else {
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Threads do cold start eviction.
//
typedef struct evict_thread_info_s {
	as_namespace*	ns;
	cf_atomic32		pid;
	cf_atomic32		i_cpu;
	bool*			sets_not_evicting;
	cf_atomic32		total_evicted;
	cf_atomic32		total_0_void_time;
} evict_thread_info;

void*
run_cold_start_evict(void* udata)
{
	evict_thread_info* p_info = (evict_thread_info*)udata;

	cf_topo_pin_to_cpu((cf_topo_cpu_index)cf_atomic32_incr(&p_info->i_cpu));

	as_namespace* ns = p_info->ns;

	cold_start_evict_info cb_info;

	cb_info.ns = ns;
	cb_info.sets_not_evicting = p_info->sets_not_evicting;
	cb_info.num_evicted = 0;
	cb_info.num_0_void_time = 0;

	int pid;

	while ((pid = (int)cf_atomic32_incr(&p_info->pid)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_partition* p_partition = &ns->partitions[pid];

		cb_info.p_partition = p_partition;
		as_index_reduce_live(p_partition->tree, cold_start_evict_reduce_cb, &cb_info);
	}

	cf_atomic32_add(&p_info->total_evicted, cb_info.num_evicted);
	cf_atomic32_add(&p_info->total_0_void_time, cb_info.num_0_void_time);

	return NULL;
}

//------------------------------------------------
// Get the cold start histogram's TTL range.
//
// TODO - ttl_range to 32 bits?
static uint64_t
get_cold_start_ttl_range(as_namespace* ns, uint32_t now)
{
	uint64_t max_void_time = 0;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		uint64_t partition_max_void_time = cf_atomic32_get(ns->partitions[n].max_void_time);

		if (partition_max_void_time > max_void_time) {
			max_void_time = partition_max_void_time;
		}
	}

	// Use max-ttl to cap the namespace maximum void-time.
	uint64_t cap = now + ns->max_ttl;

	if (max_void_time > cap) {
		max_void_time = cap;
	}

	// Convert to TTL - used for cold start histogram range.
	return max_void_time > now ? max_void_time - now : 0;
}

//------------------------------------------------
// Set cold start eviction threshold.
//
static uint64_t
set_cold_start_threshold(as_namespace* ns, linear_hist* hist)
{
	linear_hist_threshold threshold;
	uint64_t subtotal = linear_hist_get_threshold_for_fraction(hist, ns->evict_tenths_pct, &threshold);
	bool all_buckets = threshold.value == 0xFFFFffff;

	if (subtotal == 0) {
		if (all_buckets) {
			cf_warning(AS_NSUP, "{%s} cold start found no records eligible for eviction", ns->name);
		}
		else {
			cf_warning(AS_NSUP, "{%s} cold start found no records below eviction void-time %u - threshold bucket %u, width %u sec, count %lu > target %lu (%.1f pct)",
					ns->name, threshold.value, threshold.bucket_index,
					threshold.bucket_width, threshold.bucket_count,
					threshold.target_count, (float)ns->evict_tenths_pct / 10.0);
		}

		return 0;
	}

	if (all_buckets) {
		cf_warning(AS_NSUP, "{%s} cold start would evict all %lu records eligible - not evicting!", ns->name, subtotal);
		return 0;
	}

	cf_atomic32_set(&ns->cold_start_threshold_void_time, threshold.value);

	return subtotal;
}

//------------------------------------------------
// Cold start eviction, called by drv_ssd.c.
// Returns false if a serious problem occurred and
// we can't proceed.
//
bool
as_cold_start_evict_if_needed(as_namespace* ns)
{
	pthread_mutex_lock(&ns->cold_start_evict_lock);

	// Only go further than here every thousand record add attempts.
	if (ns->cold_start_record_add_count++ % EVAL_WRITE_STATE_FREQUENCY != 0) {
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	uint32_t now = as_record_void_time_get();

	// Update threshold void-time if we're past it.
	if (now > cf_atomic32_get(ns->cold_start_threshold_void_time)) {
		cf_atomic32_set(&ns->cold_start_threshold_void_time, now);
	}

	// Are we out of control?
	if (eval_stop_writes(ns)) {
		cf_warning(AS_NSUP, "{%s} hit stop-writes limit", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return false;
	}

	// If we don't need to evict, we're done.
	if (! eval_hwm_breached(ns)) {
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	// We want to evict, but are we allowed to do so?
	if (ns->cold_start_eviction_disabled) {
		cf_warning(AS_NSUP, "{%s} hwm breached but not allowed to evict", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	// We may evict - set up the cold start eviction histogram.
	cf_info(AS_NSUP, "{%s} cold start building eviction histogram ...", ns->name);

	uint32_t ttl_range = (uint32_t)get_cold_start_ttl_range(ns, now);
	uint32_t n_buckets = MAX(ns->evict_hist_buckets, COLD_START_HIST_MIN_BUCKETS);

	uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);
	bool sets_not_evicting[AS_SET_MAX_COUNT + 1];

	memset(sets_not_evicting, 0, sizeof(sets_not_evicting));

	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;
		as_set* p_set;

		if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) != CF_VMAPX_OK) {
			cf_crash(AS_NSUP, "failed to get set index %u from vmap", j);
		}

		if (IS_SET_EVICTION_DISABLED(p_set)) {
			sets_not_evicting[set_id] = true;
		}
	}

	// Split these tasks across multiple threads.
	uint32_t n_cpus = cf_topo_count_cpus();
	pthread_t evict_threads[n_cpus];

	// Reduce all partitions to build the eviction histogram.
	evict_prep_thread_info prep_thread_infos[n_cpus];
	cf_atomic32 pid = -1;

	for (uint32_t n = 0; n < n_cpus; n++) {
		prep_thread_infos[n].ns = ns;
		prep_thread_infos[n].p_pid = &pid;
		prep_thread_infos[n].i_cpu = n;
		prep_thread_infos[n].hist = linear_hist_create("thread-hist", LINEAR_HIST_SECONDS, now, ttl_range, n_buckets);
		prep_thread_infos[n].sets_not_evicting = sets_not_evicting;

		if (pthread_create(&evict_threads[n], NULL, run_cold_start_evict_prep, (void*)&prep_thread_infos[n]) != 0) {
			cf_crash(AS_NSUP, "{%s} failed to create evict-prep thread %u", ns->name, n);
		}
	}

	for (uint32_t n = 0; n < n_cpus; n++) {
		pthread_join(evict_threads[n], NULL);

		if (n == 0) {
			continue;
		}

		linear_hist_merge(prep_thread_infos[0].hist, prep_thread_infos[n].hist);
		linear_hist_destroy(prep_thread_infos[n].hist);
	}
	// Now we're single-threaded again.

	// Calculate the eviction threshold.
	uint64_t n_evictable = set_cold_start_threshold(ns, prep_thread_infos[0].hist);

	linear_hist_destroy(prep_thread_infos[0].hist);

	if (n_evictable == 0) {
		cf_warning(AS_NSUP, "{%s} hwm breached but no records to evict", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	cf_info(AS_NSUP, "{%s} cold start found %lu records eligible for eviction, evict ttl %u", ns->name, n_evictable, cf_atomic32_get(ns->cold_start_threshold_void_time) - now);

	// Reduce all partitions to evict based on the thresholds.
	evict_thread_info thread_info = {
			.ns = ns,
			.pid = -1,
			.i_cpu = -1,
			.sets_not_evicting = sets_not_evicting,
			.total_evicted = 0,
			.total_0_void_time = 0
	};

	for (uint32_t n = 0; n < n_cpus; n++) {
		if (pthread_create(&evict_threads[n], NULL, run_cold_start_evict, (void*)&thread_info) != 0) {
			cf_crash(AS_NSUP, "{%s} failed to create evict thread %u", ns->name, n);
		}
	}

	for (uint32_t n = 0; n < n_cpus; n++) {
		pthread_join(evict_threads[n], NULL);
	}
	// Now we're single-threaded again.

	cf_info(AS_NSUP, "{%s} cold start evicted %u records, found %u 0-void-time records", ns->name, thread_info.total_evicted, thread_info.total_0_void_time);

	pthread_mutex_unlock(&ns->cold_start_evict_lock);
	return true;
}

//
// END - Eviction during cold start.
//==========================================================

//==========================================================
// Temporary dangling prole garbage collection.
//

typedef struct garbage_collect_info_s {
	as_namespace*	ns;
	as_index_tree*	p_tree;
	uint32_t		now;
	uint32_t		num_deleted;
} garbage_collect_info;

static void
garbage_collect_reduce_cb(as_index_ref* r_ref, void* udata)
{
	garbage_collect_info* p_info = (garbage_collect_info*)udata;
	uint32_t void_time = r_ref->r->void_time;

	// If we're past void-time plus safety margin, delete the record.
	if (void_time != 0 && p_info->now > void_time + g_config.prole_extra_ttl) {
		as_index_delete(p_info->p_tree, &r_ref->r->keyd);
		p_info->num_deleted++;
	}

	as_record_done(r_ref, p_info->ns);
}

static int
garbage_collect_next_prole_partition(as_namespace* ns, int pid)
{
	as_partition_reservation rsv;

	// Look for the next non-master partition past pid, but loop only once over
	// all partitions.
	for (int n = 0; n < AS_PARTITIONS; n++) {
		// Increment pid and wrap if necessary.
		if (++pid == AS_PARTITIONS) {
			pid = 0;
		}

		// Note - may want a new method to get these under a single partition
		// lock, but for now just do the two separate reserve calls.
		if (as_partition_reserve_write(ns, pid, &rsv, NULL) == 0) {
			// This is a master partition - continue.
			as_partition_release(&rsv);
		}
		else {
			as_partition_reserve(ns, pid, &rsv);

			// This is a non-master partition - garbage collect and break.
			garbage_collect_info cb_info;

			cb_info.ns = ns;
			cb_info.p_tree = rsv.tree;
			cb_info.now = as_record_void_time_get();
			cb_info.num_deleted = 0;

			// Reduce the partition, deleting long-expired records.
			as_index_reduce_live(rsv.tree, garbage_collect_reduce_cb, &cb_info);

			if (cb_info.num_deleted != 0) {
				cf_info(AS_NSUP, "namespace %s pid %d: %u expired non-masters",
						ns->name, pid, cb_info.num_deleted);
			}

			as_partition_release(&rsv);

			// Do only one partition per nsup loop.
			break;
		}
	}

	return pid;
}

//
// END - Temporary dangling prole garbage collection.
//==========================================================


static cf_queue* g_p_nsup_delete_q = NULL;

int
as_nsup_queue_get_size()
{
	return g_p_nsup_delete_q ? cf_queue_sz(g_p_nsup_delete_q) : 0;
}

// Make sure a huge nsup deletion wave won't blow delete queue up.
#define DELETE_Q_SAFETY_THRESHOLD	10000
#define DELETE_Q_SAFETY_SLEEP_us	1000 // 1 millisecond

// Wait for delete queue to clear.
#define DELETE_Q_CLEAR_SLEEP_us		1000 // 1 millisecond

typedef struct record_delete_info_s {
	as_namespace*	ns;
	cf_digest		digest;
} record_delete_info;


//------------------------------------------------
// Run thread to handle delete queue.
//
void*
run_nsup_delete(void* pv_data)
{
	while (true) {
		record_delete_info q_item;

		if (CF_QUEUE_OK != cf_queue_pop(g_p_nsup_delete_q, (void*)&q_item, CF_QUEUE_FOREVER)) {
			cf_crash(AS_NSUP, "nsup delete queue pop failed");
		}

		// Generate a delete transaction for this digest, and hand it to tsvc.

		uint8_t info2 = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE;

		cl_msg *msgp = as_msg_create_internal(q_item.ns->name, &q_item.digest,
				0, info2, 0);

		as_transaction tr;
		as_transaction_init_head(&tr, NULL, msgp);

		as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_NAMESPACE);
		as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
		tr.origin = FROM_NSUP;
		tr.start_time = cf_getns();

		as_tsvc_enqueue(&tr);

		// Throttle - don't overwhelm tsvc queue.
		if (g_config.nsup_delete_sleep != 0) {
			usleep(g_config.nsup_delete_sleep);
		}
	}

	return NULL;
}

//------------------------------------------------
// Queue a record for deletion.
//
static void
queue_for_delete(as_namespace* ns, cf_digest* p_digest)
{
	record_delete_info q_item;

	q_item.ns = ns; // not bothering with namespace reservation
	q_item.digest = *p_digest;

	cf_queue_push(g_p_nsup_delete_q, (void*)&q_item);
}

//------------------------------------------------
// Insert data into TTL histograms.
//
static void
add_to_ttl_histograms(as_namespace* ns, as_index* r)
{
	uint32_t set_id = as_index_get_set_id(r);
	linear_hist* set_ttl_hist = ns->set_ttl_hists[set_id];
	uint32_t void_time = r->void_time;

	linear_hist_insert_data_point(ns->ttl_hist, void_time);

	if (set_ttl_hist) {
		linear_hist_insert_data_point(set_ttl_hist, void_time);
	}
}

//------------------------------------------------
// Reduce callback prepares for eviction.
// - builds object size, eviction & TTL histograms
// - counts 0-void-time records
//
typedef struct evict_prep_info_s {
	as_namespace*	ns;
	bool*			sets_not_evicting;
	uint64_t		num_0_void_time;
} evict_prep_info;

static void
evict_prep_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	evict_prep_info* p_info = (evict_prep_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t set_id = as_index_get_set_id(r);
	uint32_t void_time = r->void_time;

	if (void_time != 0) {
		if (! p_info->sets_not_evicting[set_id]) {
			linear_hist_insert_data_point(ns->evict_hist, void_time);
		}

		add_to_ttl_histograms(ns, r);
	}
	else {
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback evicts records.
// - evicts based on general threshold
// - does expiration on eviction-disabled sets
//
typedef struct evict_info_s {
	as_namespace*	ns;
	uint32_t		now;
	bool*			sets_not_evicting;
	uint32_t		evict_void_time;
	uint64_t		num_evicted;
} evict_info;

static void
evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	evict_info* p_info = (evict_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t set_id = as_index_get_set_id(r);
	uint32_t void_time = r->void_time;

	if (void_time != 0) {
		if (p_info->sets_not_evicting[set_id]) {
			if (p_info->now > void_time) {
				queue_for_delete(ns, &r->keyd);
				p_info->num_evicted++;
			}
		}
		else if (void_time < p_info->evict_void_time) {
			queue_for_delete(ns, &r->keyd);
			p_info->num_evicted++;
		}
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback expires records.
// - does expiration
// - builds object size & TTL histograms
// - counts 0-void-time records
//
typedef struct expire_info_s {
	as_namespace*	ns;
	uint32_t		now;
	uint64_t		num_expired;
	uint64_t		num_0_void_time;
} expire_info;

static void
expire_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	expire_info* p_info = (expire_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t void_time = r->void_time;

	if (void_time != 0) {
		if (p_info->now > void_time) {
			queue_for_delete(ns, &r->keyd);
			p_info->num_expired++;
		}
		else {
			add_to_ttl_histograms(ns, r);
		}
	}
	else {
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce all master partitions, using specified
// functionality. Throttle to make sure deletions
// generated by reducing each partition don't blow
// up the delete queue.
//
static void
reduce_master_partitions(as_namespace* ns, as_index_reduce_fn cb, void* udata, uint32_t* p_n_waits, const char* tag)
{
	as_partition_reservation rsv;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		if (as_partition_reserve_write(ns, n, &rsv, NULL) != 0) {
			continue;
		}

		as_index_reduce_live(rsv.tree, cb, udata);

		as_partition_release(&rsv);

		while (cf_queue_sz(g_p_nsup_delete_q) > DELETE_Q_SAFETY_THRESHOLD) {
			usleep(DELETE_Q_SAFETY_SLEEP_us);
			(*p_n_waits)++;
		}

		cf_debug(AS_NSUP, "{%s} %s done partition index %d, waits %u", ns->name, tag, n, *p_n_waits);
	}
}

//------------------------------------------------
// Lazily create and clear a set's TTL histogram.
//
static void
clear_set_ttl_hist(as_namespace* ns, uint32_t set_id, uint32_t now, uint64_t ttl_range)
{
	if (! ns->set_ttl_hists[set_id]) {
		char hist_name[HISTOGRAM_NAME_SIZE];

		sprintf(hist_name, "%s set %u ttl histogram", ns->name, set_id);
		ns->set_ttl_hists[set_id] = linear_hist_create(hist_name, LINEAR_HIST_SECONDS, 0, 0, TTL_HIST_NUM_BUCKETS);
	}

	linear_hist_clear(ns->set_ttl_hists[set_id], now, ttl_range);
}

//------------------------------------------------
// Get the TTL range for histograms.
//
// TODO - ttl_range to 32 bits?
static uint64_t
get_ttl_range(as_namespace* ns, uint32_t now)
{
	uint64_t max_master_void_time = 0;
	as_partition_reservation rsv;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		if (as_partition_reserve_write(ns, n, &rsv, NULL) != 0) {
			continue;
		}

		as_partition_release(&rsv);

		uint64_t partition_max_void_time = cf_atomic32_get(ns->partitions[n].max_void_time);

		if (partition_max_void_time > max_master_void_time) {
			max_master_void_time = partition_max_void_time;
		}
	}

	// Use max-ttl to cap the namespace maximum void-time.
	uint64_t cap = now + ns->max_ttl;

	if (max_master_void_time > cap) {
		max_master_void_time = cap;
	}

	// Convert to TTL - used for histogram ranges.
	return max_master_void_time > now ? max_master_void_time - now : 0;
}

//------------------------------------------------
// Get general eviction threshold.
//
static bool
get_threshold(as_namespace* ns, uint32_t* p_evict_void_time)
{
	linear_hist_threshold threshold;
	uint64_t subtotal = linear_hist_get_threshold_for_fraction(ns->evict_hist, ns->evict_tenths_pct, &threshold);
	bool all_buckets = threshold.value == 0xFFFFffff;

	*p_evict_void_time = threshold.value;

	if (subtotal == 0) {
		if (all_buckets) {
			cf_warning(AS_NSUP, "{%s} no records eligible for eviction", ns->name);
		}
		else {
			cf_warning(AS_NSUP, "{%s} no records below eviction void-time %u - threshold bucket %u, width %u sec, count %lu > target %lu (%.1f pct)",
					ns->name, threshold.value, threshold.bucket_index,
					threshold.bucket_width, threshold.bucket_count,
					threshold.target_count, (float)ns->evict_tenths_pct / 10.0);
		}

		return false;
	}

	if (all_buckets) {
		cf_warning(AS_NSUP, "{%s} would evict all %lu records eligible - not evicting!", ns->name, subtotal);
		return false;
	}

	cf_info(AS_NSUP, "{%s} found %lu records eligible for eviction", ns->name, subtotal);

	return true;
}

//------------------------------------------------
// Stats per namespace at the end of an nsup lap.
//
static void
update_stats(as_namespace* ns, uint64_t n_master, uint64_t n_0_void_time,
		uint64_t n_expired_objects, uint64_t n_evicted_objects,
		uint32_t evict_ttl, uint32_t n_general_waits, uint32_t n_clear_waits,
		uint64_t start_ms)
{
	ns->non_expirable_objects = n_0_void_time;

	cf_atomic64_add(&ns->n_expired_objects, n_expired_objects);
	cf_atomic64_add(&ns->n_evicted_objects, n_evicted_objects);

	cf_atomic64_set(&ns->evict_ttl, evict_ttl);

	uint64_t total_duration_ms = cf_getms() - start_ms;

	ns->nsup_cycle_duration = (uint32_t)(total_duration_ms / 1000);
	ns->nsup_cycle_sleep_pct = total_duration_ms == 0 ? 0 : (uint32_t)((n_general_waits * 100) / total_duration_ms);

	cf_info(AS_NSUP, "{%s} nsup-done: master-objects (%lu,%lu) expired (%lu,%lu) evicted (%lu,%lu) evict-ttl %d waits (%u,%u) total-ms %lu",
			ns->name,
			n_master, n_0_void_time,
			ns->n_expired_objects, n_expired_objects,
			ns->n_evicted_objects, n_evicted_objects,
			evict_ttl,
			n_general_waits, n_clear_waits,
			total_duration_ms);
}

//------------------------------------------------
// Namespace supervisor thread "run" function.
//
void *
run_nsup(void *arg)
{
	// Garbage-collect long-expired proles, one partition per loop.
	int prole_pids[g_config.n_namespaces];

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		prole_pids[ns_ix] = -1;
	}

	uint64_t last_time = cf_get_seconds();

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t period = (uint64_t)g_config.nsup_period;
		uint64_t curr_time = cf_get_seconds();

		if (period == 0 || curr_time - last_time < period) {
			continue;
		}

		last_time = curr_time;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_namespace *ns = g_config.namespaces[ns_ix];

			if (ns->nsup_disabled) {
				cf_info(AS_NSUP, "{%s} nsup-skipped", ns->name);
				continue;
			}

			uint64_t start_ms = cf_getms();

			cf_info(AS_NSUP, "{%s} nsup-start", ns->name);

			// The "now" used for all expiration and eviction.
			uint32_t now = as_record_void_time_get();

			// Get the histogram range - used by all histograms.
			uint32_t ttl_range = (uint32_t)get_ttl_range(ns, now);

			linear_hist_clear(ns->ttl_hist, now, ttl_range);

			uint64_t n_expired_records = 0;
			uint64_t n_0_void_time_records = 0;

			uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

			bool sets_protected = false;

			// Giving this max possible size to spare us checking each record's
			// set-id during index reduce.
			bool sets_not_evicting[AS_SET_MAX_COUNT + 1];

			memset(sets_not_evicting, 0, sizeof(sets_not_evicting));

			for (uint32_t j = 0; j < num_sets; j++) {
				uint32_t set_id = j + 1;

				clear_set_ttl_hist(ns, set_id, now, ttl_range);

				as_set* p_set;

				if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) != CF_VMAPX_OK) {
					cf_crash(AS_NSUP, "failed to get set index %u from vmap", j);
				}

				if (IS_SET_EVICTION_DISABLED(p_set)) {
					sets_not_evicting[set_id] = true;
					sets_protected = true;
				}
			}

			uint64_t n_evicted_records = 0;
			uint32_t evict_ttl = 0;
			uint32_t n_general_waits = 0;

			// Check whether or not we need to do general eviction.

			if (eval_hwm_breached(ns)) {
				// Eviction is necessary.

				linear_hist_reset(ns->evict_hist, now, ttl_range, ns->evict_hist_buckets);

				evict_prep_info cb_info1;

				memset(&cb_info1, 0, sizeof(cb_info1));
				cb_info1.ns = ns;
				cb_info1.sets_not_evicting = sets_not_evicting;

				// Reduce master partitions, building histograms to calculate
				// general eviction threshold.
				reduce_master_partitions(ns, evict_prep_reduce_cb, &cb_info1, &n_general_waits, "evict-prep");

				n_0_void_time_records = cb_info1.num_0_void_time;

				evict_info cb_info2;

				memset(&cb_info2, 0, sizeof(cb_info2));
				cb_info2.ns = ns;
				cb_info2.now = now;
				cb_info2.sets_not_evicting = sets_not_evicting;

				// Determine general eviction threshold.
				if (get_threshold(ns, &cb_info2.evict_void_time)) {
					// Save the eviction depth in the device header(s) so it can
					// be used to speed up cold start, etc.
					as_storage_save_evict_void_time(ns, cb_info2.evict_void_time);

					// Reduce master partitions, deleting records up to
					// threshold. (This automatically deletes expired records.)
					reduce_master_partitions(ns, evict_reduce_cb, &cb_info2, &n_general_waits, "evict");

					evict_ttl = cb_info2.evict_void_time - now;
					n_evicted_records = cb_info2.num_evicted;
				}
				else if (sets_protected || cb_info2.evict_void_time == now) {
					// Convert eviction into expiration.
					cb_info2.evict_void_time = now;

					// Reduce master partitions, deleting expired records,
					// including those in eviction-protected sets.
					reduce_master_partitions(ns, evict_reduce_cb, &cb_info2, &n_general_waits, "expire-protected-sets");

					// Count these as expired rather than evicted, since we can.
					n_expired_records = cb_info2.num_evicted;
				}

				// For now there's no get_info() call for evict_hist.
				//linear_hist_save_info(ns->evict_hist);
			}
			else {
				// Eviction is not necessary, only expiration.

				expire_info cb_info;

				memset(&cb_info, 0, sizeof(cb_info));
				cb_info.ns = ns;
				cb_info.now = now;

				// Reduce master partitions, deleting expired records.
				reduce_master_partitions(ns, expire_reduce_cb, &cb_info, &n_general_waits, "expire");

				n_expired_records = cb_info.num_expired;
				n_0_void_time_records = cb_info.num_0_void_time;
			}

			linear_hist_dump(ns->ttl_hist);
			linear_hist_save_info(ns->ttl_hist);

			for (uint32_t j = 0; j < num_sets; j++) {
				uint32_t set_id = j + 1;

				linear_hist_dump(ns->set_ttl_hists[set_id]);
				linear_hist_save_info(ns->set_ttl_hists[set_id]);
			}

			// Wait for delete queue to clear.
			uint32_t n_clear_waits = 0;

			while (cf_queue_sz(g_p_nsup_delete_q) > 0) {
				usleep(DELETE_Q_CLEAR_SLEEP_us);
				n_clear_waits++;
			}

			update_stats(ns, linear_hist_get_total(ns->ttl_hist) + n_0_void_time_records, n_0_void_time_records,
					n_expired_records, n_evicted_records, evict_ttl,
					n_general_waits, n_clear_waits, start_ms);

			// Garbage-collect long-expired proles, one partition per loop.
			if (g_config.prole_extra_ttl != 0) {
				prole_pids[ns_ix] = garbage_collect_next_prole_partition(ns, prole_pids[ns_ix]);
			}
		}
	}

	return NULL;
}

//------------------------------------------------
// Namespace stop-writes thread "run" function.
//
void *
run_stop_writes(void *arg)
{
	while (true) {
		sleep(EVAL_STOP_WRITES_PERIOD);

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			eval_stop_writes(g_config.namespaces[ns_ix]);
		}
	}

	return NULL;
}

//------------------------------------------------
// Size-histogram thread "run" function.
//
void *
run_size_histograms(void *arg)
{
	bool wait = false; // make sure we run once right away on startup
	uint64_t last_time = 0;

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t period = (uint64_t)g_config.object_size_hist_period;
		uint64_t curr_time = cf_get_seconds();

		if (period == 0 || (wait && curr_time - last_time < period)) {
			continue;
		}

		wait = true;
		last_time = curr_time;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			collect_size_histograms(g_config.namespaces[ns_ix]);
		}
	}

	return NULL;
}

//------------------------------------------------
// Start supervisor threads.
//
void
as_nsup_start()
{
	// Seed the random number generator.
	srand(time(NULL));

	// Create queue for nsup-generated deletions.
	g_p_nsup_delete_q = cf_queue_create(sizeof(record_delete_info), true);

	cf_info(AS_NSUP, "starting namespace supervisor threads");

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	// Start thread to handle all nsup-generated deletions.
	if (0 != pthread_create(&thread, &attrs, run_nsup_delete, NULL)) {
		cf_crash(AS_NSUP, "nsup delete thread create failed");
	}

	// Start namespace supervisor thread to do expiration & eviction.
	if (0 != pthread_create(&thread, &attrs, run_nsup, NULL)) {
		cf_crash(AS_NSUP, "nsup thread create failed");
	}

	// Start thread to do stop-writes evaluation.
	if (0 != pthread_create(&thread, &attrs, run_stop_writes, NULL)) {
		cf_crash(AS_NSUP, "nsup stop-writes thread create failed");
	}

	// Start thread to do size histograms. TODO - eventually consolidate with
	// nsup when we expire/evict via SMD and include non-masters.
	if (0 != pthread_create(&thread, &attrs, run_size_histograms, NULL)) {
		cf_crash(AS_NSUP, "nsup size-histograms thread create failed");
	}
}


//==========================================================
// Local helpers.
//

static bool
eval_stop_writes(as_namespace *ns)
{
	// Compute the high-watermark.
	uint64_t mem_stop_writes = (ns->memory_size * ns->stop_writes_pct) / 100;

	// Compute device available percent for namespace.
	int device_avail_pct = 0;

	as_storage_stats(ns, &device_avail_pct, NULL);

	// Compute memory usage for namespace. Note that persisted index is not
	// counted against stop-writes.
	uint64_t index_mem_sz = as_namespace_index_persisted(ns) ?
			0 : (ns->n_tombstones + ns->n_objects) * sizeof(as_index);
	uint64_t sindex_sz = ns->n_bytes_sindex_memory;
	uint64_t data_in_memory_sz = ns->n_bytes_memory;
	uint64_t memory_sz = index_mem_sz + sindex_sz + data_in_memory_sz;

	// Possible reasons for stopping writes.
	static const char* reasons[] = {
			NULL,									// 0x0
			"(memory)",								// 0x1
			"(device-avail-pct)",					// 0x2
			"(memory & device-avail-pct)",			// 0x3 (0x1 | 0x2)
			"(xdr-log)",							// 0x4
			"(memory & xdr-log)",					// 0x5 (0x1 | 0x4)
			"(device-avail-pct & xdr-log)",			// 0x6 (0x2 | 0x4)
			"(memory & device-avail-pct & xdr-log)"	// 0x7 (0x1 | 0x2 | 0x4)
	};

	// Check if the writes should be stopped.
	uint32_t why_stopped = 0x0;

	if (memory_sz > mem_stop_writes) {
		why_stopped |= 0x1;
	}

	if (device_avail_pct < (int)ns->storage_min_avail_pct) {
		why_stopped |= 0x2;
	}

	if (is_xdr_digestlog_low(ns)) {
		why_stopped |= 0x4;
	}

	if (why_stopped != 0) {
		cf_warning(AS_NSUP, "{%s} breached stop-writes limit %s, memory sz:%lu (%lu + %lu + %lu) limit:%lu, disk avail-pct:%d",
				ns->name, reasons[why_stopped],
				memory_sz, index_mem_sz, sindex_sz, data_in_memory_sz, mem_stop_writes,
				device_avail_pct);

		cf_atomic32_set(&ns->stop_writes, 1);
		return true;
	}

	cf_debug(AS_NSUP, "{%s} stop-writes limit not breached, memory sz:%lu (%lu + %lu + %lu) limit:%lu, disk avail-pct:%d",
			ns->name,
			memory_sz, index_mem_sz, sindex_sz, data_in_memory_sz, mem_stop_writes,
			device_avail_pct);

	cf_atomic32_set(&ns->stop_writes, 0);

	return false;
}

static bool
eval_hwm_breached(as_namespace *ns)
{
	uint64_t index_sz = (ns->n_tombstones + ns->n_objects) * sizeof(as_index);

	uint64_t index_mem_sz = 0;
	uint64_t index_dev_sz = 0;
	uint64_t pix_hwm = 0;

	if (as_namespace_index_persisted(ns)) {
		index_dev_sz = index_sz;
		pix_hwm = (ns->mounts_size_limit * ns->mounts_hwm_pct) / 100;
	}
	else {
		index_mem_sz = index_sz;
	}

	uint64_t sindex_sz = ns->n_bytes_sindex_memory;
	uint64_t data_in_memory_sz = ns->n_bytes_memory;
	uint64_t memory_sz = index_mem_sz + sindex_sz + data_in_memory_sz;
	uint64_t mem_hwm = (ns->memory_size * ns->hwm_memory_pct) / 100;

	uint64_t used_disk_sz = 0;

	as_storage_stats(ns, NULL, &used_disk_sz);

	uint64_t ssd_hwm = (ns->ssd_size * ns->hwm_disk_pct) / 100;

	// Possible reasons for eviction.
	static const char* reasons[] = {
			NULL,								// 0x0
			"(memory)",							// 0x1
			"(index-device)",					// 0x2
			"(memory & index-device)",			// 0x3 (0x1 | 0x2)
			"(disk)",							// 0x4
			"(memory & disk)",					// 0x5 (0x1 | 0x4)
			"(index-device & disk)",			// 0x6 (0x2 | 0x4)
			"(memory & index-device & disk)"	// 0x7 (0x1 | 0x2 | 0x4)
	};

	uint32_t how_breached = 0x0;

	if (memory_sz > mem_hwm) {
		how_breached |= 0x1;
	}

	if (index_dev_sz > pix_hwm) {
		how_breached |= 0x2;
	}

	if (used_disk_sz > ssd_hwm) {
		how_breached |= 0x4;
	}

	if (how_breached != 0) {
		cf_warning(AS_NSUP, "{%s} breached eviction hwm %s, memory sz:%lu (%lu + %lu + %lu) hwm:%lu, index-device sz:%lu hwm:%lu, disk sz:%lu hwm:%lu",
				ns->name, reasons[how_breached],
				memory_sz, index_mem_sz, sindex_sz, data_in_memory_sz, mem_hwm,
				index_dev_sz, pix_hwm,
				used_disk_sz, ssd_hwm);

		cf_atomic32_set(&ns->hwm_breached, 1);
		return true;
	}

	cf_debug(AS_NSUP, "{%s} no eviction hwm breached, memory sz:%lu (%lu + %lu + %lu) hwm:%lu, index-device sz:%lu hwm:%lu, disk sz:%lu hwm:%lu",
			ns->name,
			memory_sz, index_mem_sz, sindex_sz, data_in_memory_sz, mem_hwm,
			index_dev_sz, pix_hwm,
			used_disk_sz, ssd_hwm);

	cf_atomic32_set(&ns->hwm_breached, 0);

	return false;
}

static void
collect_size_histograms(as_namespace *ns)
{
	if (ns->storage_type != AS_STORAGE_ENGINE_SSD || ns->n_objects == 0) {
		return;
	}

	cf_info(AS_NSUP, "{%s} collecting object size info ...", ns->name);

	histogram_clear(ns->obj_size_log_hist);
	linear_hist_clear(ns->obj_size_lin_hist, 0, ns->storage_write_block_size);

	uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;

		if (ns->set_obj_size_log_hists[set_id]) {
			histogram_clear(ns->set_obj_size_log_hists[set_id]);
			linear_hist_clear(ns->set_obj_size_lin_hists[set_id], 0, ns->storage_write_block_size);
		}
		else {
			char hist_name[HISTOGRAM_NAME_SIZE];
			const char* set_name = as_namespace_get_set_name(ns, set_id);

			sprintf(hist_name, "{%s}-%s-object-size-log2", ns->name, set_name);
			ns->set_obj_size_log_hists[set_id] = histogram_create(hist_name, HIST_SIZE);

			sprintf(hist_name, "{%s}-%s-object-size-linear", ns->name, set_name);
			ns->set_obj_size_lin_hists[set_id] = linear_hist_create(hist_name, LINEAR_HIST_SIZE, 0, ns->storage_write_block_size, OBJ_SIZE_HIST_NUM_BUCKETS);
		}
	}

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		as_index_reduce_live(rsv.tree, size_histograms_reduce_cb, (void*)ns);

		as_partition_release(&rsv);
	}

	histogram_save_info(ns->obj_size_log_hist);
	linear_hist_save_info(ns->obj_size_lin_hist);

	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;

		histogram_save_info(ns->set_obj_size_log_hists[set_id]);
		linear_hist_save_info(ns->set_obj_size_lin_hists[set_id]);
	}

	cf_info(AS_NSUP, "{%s} ... done collecting object size info", ns->name);
}

static void
size_histograms_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	as_namespace* ns = (as_namespace*)udata;

	uint32_t size = as_storage_record_size(ns, r);

	histogram_insert_raw_unsafe(ns->obj_size_log_hist, size);
	linear_hist_insert_data_point(ns->obj_size_lin_hist, size);

	uint32_t set_id = as_index_get_set_id(r);
	histogram* set_obj_size_log_hist = ns->set_obj_size_log_hists[set_id];
	linear_hist* set_obj_size_lin_hist = ns->set_obj_size_lin_hists[set_id];

	if (set_obj_size_log_hist) {
		histogram_insert_raw_unsafe(set_obj_size_log_hist, size);
		linear_hist_insert_data_point(set_obj_size_lin_hist, size);
	}

	as_record_done(r_ref, ns);
}
