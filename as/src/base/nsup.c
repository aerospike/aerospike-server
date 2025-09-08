/*
 * nsup.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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

#include "base/nsup.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "hardware.h"
#include "linear_hist.h"
#include "log.h"
#include "node.h"
#include "vector.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/smd.h"
#include "base/thr_info.h"
#include "fabric/partition.h"
#include "sindex/gc.h"
#include "sindex/sindex.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef struct expire_overall_info_s {
	as_namespace* ns;
	uint32_t pid;
	uint32_t now;
	uint64_t n_0_void_time;
	uint64_t n_expired;
} expire_overall_info;

typedef struct expire_per_thread_info_s {
	as_namespace* ns;
	as_partition_reservation* rsv;
	uint32_t now;
	uint64_t n_0_void_time;
	uint64_t n_expired;
} expire_per_thread_info;

typedef struct evict_overall_info_s {
	as_namespace* ns;
	uint32_t pid;
	uint32_t i_cpu; // for cold start eviction only
	uint32_t now;
	uint32_t evict_void_time;
	const bool* sets_not_evicting;
	uint64_t n_0_void_time;
	uint64_t n_expired;
	uint64_t n_evicted;
} evict_overall_info;

typedef struct evict_per_thread_info_s {
	as_namespace* ns;
	as_partition_reservation* rsv;
	uint32_t now;
	uint32_t evict_void_time;
	const bool* sets_not_evicting;
	uint64_t n_0_void_time;
	uint64_t n_expired;
	uint64_t n_evicted;
} evict_per_thread_info;

typedef struct prep_evict_per_thread_info_s {
	as_namespace* ns;
	uint32_t* p_pid;
	uint32_t i_cpu; // for cold start eviction only
	const bool* sets_not_evicting;
	linear_hist* evict_hist;
} prep_evict_per_thread_info;

#define SKEW_STOP_SEC 40
#define SKEW_WARN_SEC 30

#define EVICT_SMD_TIMEOUT (5 * 1000) // 5 seconds

#define TOO_LONG_MS (2 * 60 * 60 * 1000) // 2 hours
#define TOO_MUCH_DELETED_PCT 1.0

#define EVAL_STOP_WRITES_PERIOD 10 // seconds

#define EVAL_WRITE_STATE_FREQUENCY 1024
#define COLD_START_HIST_MIN_BUCKETS 100000 // histogram memory is transient


//==========================================================
// Forward declarations.
//

static bool nsup_smd_conflict_cb(const as_smd_item* existing_item, const as_smd_item* new_item);
static void nsup_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);

static void* run_expire_or_evict(void* udata);

static void expire(as_namespace* ns);
static void* run_expire(void* udata);
static bool expire_reduce_cb(as_index_ref* r_ref, void* udata);

static bool evict(as_namespace* ns);
static void* run_evict(void* udata);
static bool evict_reduce_cb(as_index_ref* r_ref, void* udata);

static bool eval_hwm_breached(as_namespace* ns);
static uint32_t find_evict_void_time(as_namespace* ns, uint32_t now);
static void* run_prep_evict(void* udata);
static bool prep_evict_reduce_cb(as_index_ref* r_ref, void* udata);

static void update_stats(as_namespace* ns, uint64_t n_0_void_time, uint64_t n_expired_objects, uint64_t n_evicted_objects, uint64_t start_ms);

static void* run_stop_writes(void* udata);
static bool eval_stop_writes(as_namespace* ns, bool cold_starting);
static uint32_t sys_mem_pct(void);

static void* run_nsup_histograms(void* udata);
static void collect_nsup_histograms(as_namespace* ns);
static bool nsup_histograms_reduce_cb(as_index_ref* r_ref, void* udata);

static bool cold_start_evict(as_namespace* ns);
static void* run_prep_cold_start_evict(void* udata);
static uint64_t set_cold_start_threshold(as_namespace* ns, linear_hist* hist);
static void* run_cold_start_evict(void* udata);
static bool cold_start_evict_reduce_cb(as_index_ref* r_ref, void* udata);

static bool sets_protected(as_namespace* ns);
static void init_sets_not_evicting(as_namespace* ns, bool sets_not_evicting[]);
static uint32_t get_ttl_range(as_namespace* ns, uint32_t now);


//==========================================================
// Inlines & macros.
//

static inline uint32_t
evict_void_time_from_smd(const as_smd_item* item)
{
	return (uint32_t)strtoul(item->value, NULL, 10); // TODO - sanity check?
}


//==========================================================
// Public API.
//

void
as_nsup_init(void)
{
	as_smd_module_load(AS_SMD_MODULE_EVICT, nsup_smd_accept_cb,
			nsup_smd_conflict_cb, NULL);
}

void
as_nsup_start(void)
{
	cf_info(AS_NSUP, "starting namespace supervisor threads");

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_thread_create_detached(run_expire_or_evict, ns);
		cf_thread_create_detached(run_nsup_histograms, ns);
	}

	cf_thread_create_detached(run_stop_writes, NULL);
}

bool
as_nsup_handle_clock_skew(as_namespace* ns, uint64_t skew_ms)
{
	if (ns->nsup_period == 0 || skew_ms < SKEW_WARN_SEC * 1000UL) {
		return false;
	}

	if (skew_ms > SKEW_STOP_SEC * 1000UL) {
		cf_ticker_warning(AS_NSUP, "{%s} clock-skew > %u sec stopped writes",
				ns->name, SKEW_STOP_SEC);
		return true;
	}

	cf_ticker_warning(AS_NSUP, "{%s} clock-skew > %u sec", ns->name,
			SKEW_WARN_SEC);

	return false;
}

void
as_nsup_eviction_reset_cmd(const char* ns_name, const char* ttl_str,
		cf_dyn_buf *db)
{
	if (ttl_str == NULL) {
		cf_info(AS_NSUP, "{%s} got command to delete evict-void-time", ns_name);

		if (! as_smd_delete_blocking(AS_SMD_MODULE_EVICT, ns_name, 0)) {
			cf_warning(AS_NSUP, "{%s} timeout deleting evict-void-time",
					ns_name);
			as_info_respond_error(db, AS_ERR_TIMEOUT, "timeout");
			return;
		}

		as_info_respond_ok(db);
		return;
	}

	uint64_t ttl = strtoul(ttl_str, NULL, 0);

	if (ttl > MAX_ALLOWED_TTL) {
		cf_warning(AS_NSUP, "{%s} command ttl %lu is too big", ns_name, ttl);
		as_info_respond_error(db, AS_ERR_PARAMETER, "ttl too big");
		return;
	}

	uint32_t now = as_record_void_time_get();
	uint32_t evict_void_time = now + (uint32_t)ttl;

	cf_info(AS_NSUP, "{%s} got command to set evict-ttl %lu evict-void-time %u",
			ns_name, ttl, evict_void_time);

	char value[10 + 1];

	sprintf(value, "%u", evict_void_time);

	if (! as_smd_set_blocking(AS_SMD_MODULE_EVICT, ns_name, value, 0)) {
		cf_warning(AS_NSUP, "{%s} timeout setting evict-ttl %lu evict-void-time %u",
				ns_name, ttl, evict_void_time);
		as_info_respond_error(db, AS_ERR_TIMEOUT, "timeout");
		return;
	}

	as_info_respond_ok(db);
}

bool
as_cold_start_evict_if_needed(as_namespace* ns)
{
	cf_mutex_lock(&ns->cold_start_evict_lock);

	bool result = cold_start_evict(ns);

	cf_mutex_unlock(&ns->cold_start_evict_lock);

	return result;
}


//==========================================================
// Local helpers - SMD callbacks.
//

static bool
nsup_smd_conflict_cb(const as_smd_item* existing_item,
		const as_smd_item* new_item)
{
	return evict_void_time_from_smd(new_item) >
			evict_void_time_from_smd(existing_item);
}

static void
nsup_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		as_smd_item* item = cf_vector_get_ptr(items, i);
		as_namespace* ns = as_namespace_get_byname(item->key);

		if (ns == NULL) {
			cf_detail(AS_NSUP, "skipping invalid ns");
			continue;
		}

		if (item->value != NULL) {
			ns->smd_evict_void_time = evict_void_time_from_smd(item);

			cf_info(AS_NSUP, "{%s} got smd evict-void-time %u", ns->name,
					ns->smd_evict_void_time);

			if (accept_type == AS_SMD_ACCEPT_OPT_START) {
				ns->evict_void_time = ns->smd_evict_void_time;
			}
		}
		else {
			cf_info(AS_NSUP, "{%s} deleted evict-void-time (%u,%u)", ns->name,
					ns->evict_void_time, ns->smd_evict_void_time);

			ns->evict_void_time = 0;
			ns->smd_evict_void_time = 0;
		}
	}
}


//==========================================================
// Local helpers - expiration/eviction control loop.
//

static void*
run_expire_or_evict(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;

	uint64_t last_time = cf_get_seconds();

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t period = as_load_uint32(&ns->nsup_period);

		if (period == 0 || ns->clock_skew_stop_writes) {
			continue;
		}

		if (evict(ns)) {
			continue;
		}

		uint64_t curr_time = cf_get_seconds();

		if (curr_time - last_time < period) {
			continue;
		}

		last_time = curr_time;

		if (eval_hwm_breached(ns)) {
			uint32_t now = as_record_void_time_get();
			uint32_t evict_void_time = find_evict_void_time(ns, now);

			if (evict_void_time > now) {
				if (evict_void_time < ns->evict_void_time) {
					// Unusual, maybe lots of new records with short TTLs ...
					cf_info(AS_NSUP, "{%s} evict-void-time %u < previous",
							ns->name, evict_void_time);

					if (! as_smd_delete_blocking(AS_SMD_MODULE_EVICT, ns->name,
							EVICT_SMD_TIMEOUT)) {
						cf_warning(AS_NSUP, "{%s} timeout deleting evict-void-time",
								ns->name);
					}
				}

				char value[10 + 1];

				sprintf(value, "%u", evict_void_time);

				if (! as_smd_set_blocking(AS_SMD_MODULE_EVICT, ns->name, value,
						EVICT_SMD_TIMEOUT)) {
					cf_warning(AS_NSUP, "{%s} timeout setting evict-void-time %u",
							ns->name, evict_void_time);
				}

				continue;
			}
			// else - evict_void_time is now or 0.

			if (! sets_protected(ns) && evict_void_time == 0) {
				continue; // no need to expire
			}
			// else - expire protected sets, or if evict-void-time is now.
		}

		expire(ns);
	}

	return NULL;
}


//==========================================================
// Local helpers - expire.
//

static void
expire(as_namespace* ns)
{
	uint64_t start_ms = cf_getms();
	uint32_t n_threads = as_load_uint32(&ns->n_nsup_threads);

	cf_info(AS_NSUP, "{%s} nsup-start: expire-threads %u", ns->name, n_threads);

	cf_tid tids[n_threads];

	expire_overall_info overall = {
			.ns = ns,
			.now = as_record_void_time_get()
	};

	for (uint32_t i = 0; i < n_threads; i++) {
		tids[i] = cf_thread_create_joinable(run_expire, (void*)&overall);
	}

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_join(tids[i]);
	}

	update_stats(ns, overall.n_0_void_time, overall.n_expired, 0, start_ms);
}

static void*
run_expire(void* udata)
{
	expire_overall_info* overall = (expire_overall_info*)udata;
	as_namespace* ns = overall->ns;

	expire_per_thread_info per_thread = {
			.ns = ns,
			.now = overall->now
	};

	uint32_t pid;

	while ((pid = as_faa_uint32(&overall->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		per_thread.rsv = &rsv;

		as_index_reduce(rsv.tree, expire_reduce_cb, (void*)&per_thread);
		as_partition_release(&rsv);
	}

	as_add_uint64(&overall->n_0_void_time, (int64_t)per_thread.n_0_void_time);
	as_add_uint64(&overall->n_expired, (int64_t)per_thread.n_expired);

	return NULL;
}

static bool
expire_reduce_cb(as_index_ref* r_ref, void* udata)
{
	expire_per_thread_info* per_thread = (expire_per_thread_info*)udata;
	as_namespace* ns = per_thread->ns;
	uint32_t void_time = r_ref->r->void_time;

	if (! as_record_is_live(r_ref->r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (void_time == 0) {
		per_thread->n_0_void_time++;
	}
	else if (per_thread->now > void_time) {
		if (mrt_skip_cleanup(ns, per_thread->rsv->tree, r_ref)) {
			as_record_done(r_ref, ns);
			return true;
		}

		if (drop_local(ns, per_thread->rsv, r_ref)) {
			as_sindex_gc_record_throttle(ns);
			per_thread->n_expired++;
		}

		return true; // drop_local() calls as_record_done()
	}

	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - evict.
//

static bool
evict(as_namespace* ns)
{
	uint32_t evict_void_time = as_load_uint32(&ns->evict_void_time);
	uint32_t smd_evict_void_time = as_load_uint32(&ns->smd_evict_void_time);

	if (evict_void_time >= smd_evict_void_time) {
		return false;
	}

	uint64_t start_ms = cf_getms();
	uint32_t now = as_record_void_time_get();
	uint32_t n_threads = as_load_uint32(&ns->n_nsup_threads);

	// For stats, show eviction depth WRT local time. Note - unlikely to be
	// negative, but theoretically possible - cutoff could have come from
	// another node, and/or taken very long to calculate/transmit.
	ns->evict_ttl = (int32_t)(smd_evict_void_time - now);

	cf_info(AS_NSUP, "{%s} nsup-start: evict-threads %u evict-ttl %d evict-void-time (%u,%u)",
			ns->name, n_threads, ns->evict_ttl, evict_void_time,
			smd_evict_void_time);

	evict_void_time = smd_evict_void_time;

	if (now > evict_void_time) {
		cf_info(AS_NSUP, "{%s} ignoring evict-void-time in the past - may remove using 'eviction-reset'",
				ns->name);

		ns->evict_void_time = evict_void_time;

		return false;
	}

	bool sets_not_evicting[AS_SET_MAX_COUNT + 1] = { false };
	init_sets_not_evicting(ns, sets_not_evicting);

	cf_tid tids[n_threads];

	evict_overall_info overall = {
			.ns = ns,
			.now = now,
			.evict_void_time = evict_void_time,
			.sets_not_evicting = (const bool*)sets_not_evicting
	};

	for (uint32_t i = 0; i < n_threads; i++) {
		tids[i] = cf_thread_create_joinable(run_evict, (void*)&overall);
	}

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_join(tids[i]);
	}

	update_stats(ns, overall.n_0_void_time, overall.n_expired,
			overall.n_evicted, start_ms);

	ns->evict_void_time = evict_void_time;

	return true;
}

static void*
run_evict(void* udata)
{
	evict_overall_info* overall = (evict_overall_info*)udata;
	as_namespace* ns = overall->ns;

	evict_per_thread_info per_thread = {
			.ns = ns,
			.now = overall->now,
			.evict_void_time = overall->evict_void_time,
			.sets_not_evicting = overall->sets_not_evicting
	};

	uint32_t pid;

	while ((pid = as_faa_uint32(&overall->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		per_thread.rsv = &rsv;

		as_index_reduce(rsv.tree, evict_reduce_cb, (void*)&per_thread);
		as_partition_release(&rsv);
	}

	as_add_uint64(&overall->n_0_void_time, (int64_t)per_thread.n_0_void_time);
	as_add_uint64(&overall->n_expired, (int64_t)per_thread.n_expired);
	as_add_uint64(&overall->n_evicted, (int64_t)per_thread.n_evicted);

	return NULL;
}

static bool
evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	evict_per_thread_info* per_thread = (evict_per_thread_info*)udata;
	as_namespace* ns = per_thread->ns;
	uint32_t void_time = r->void_time;

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (void_time == 0) {
		per_thread->n_0_void_time++;
	}
	else if (per_thread->sets_not_evicting[as_index_get_set_id(r)]) {
		if (per_thread->now > void_time) {
			if (mrt_skip_cleanup(ns, per_thread->rsv->tree, r_ref)) {
				as_record_done(r_ref, ns);
				return true;
			}

			if (drop_local(ns, per_thread->rsv, r_ref)) {
				as_sindex_gc_record_throttle(ns);
				per_thread->n_expired++;
			}

			return true; // drop_local() calls as_record_done()
		}
	}
	else if (per_thread->evict_void_time > void_time) {
		if (mrt_skip_cleanup(ns, per_thread->rsv->tree, r_ref)) {
			as_record_done(r_ref, ns);
			return true;
		}

		if (drop_local(ns, per_thread->rsv, r_ref)) {
			as_sindex_gc_record_throttle(ns);
			per_thread->n_evicted++;
		}

		return true; // drop_local() calls as_record_done()
	}

	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - initiate eviction.
//

static bool
eval_hwm_breached(as_namespace* ns)
{
	uint64_t index_used_sz =
			(ns->n_tombstones + ns->n_objects) * sizeof(as_index);
	uint64_t index_mem_sz = 0;
	uint32_t index_dev_used_pct = 0;
	char index_dev_tag[128];

	if (as_namespace_index_persisted(ns)) {
		index_dev_used_pct =
				(uint32_t)((index_used_sz * 100) / ns->pi_mounts_budget);
		sprintf(index_dev_tag, ", index-device sz:%lu used-pct:%u",
				index_used_sz, index_dev_used_pct);
	}
	else {
		index_mem_sz = index_used_sz;
		index_dev_tag[0] = '\0';
	}

	uint64_t sindex_used_sz = as_sindex_used_bytes(ns);
	uint64_t sindex_mem_sz = 0;
	uint32_t sindex_dev_used_pct = 0;
	char sindex_dev_tag[128];

	if (as_namespace_sindex_persisted(ns)) {
		sindex_dev_used_pct =
				(uint32_t)((sindex_used_sz * 100) / ns->si_mounts_budget);
		sprintf(sindex_dev_tag, ", sindex-device sz:%lu used-pct:%u",
				sindex_used_sz, sindex_dev_used_pct);
	}
	else {
		sindex_mem_sz = sindex_used_sz;
		sindex_dev_tag[0] = '\0';
	}

	uint64_t set_index_sz = as_set_index_used_bytes(ns);
	uint64_t ixs_sz = index_mem_sz + set_index_sz + sindex_mem_sz;

	uint64_t ixs_memory_budget = as_load_uint64(&ns->indexes_memory_budget);
	uint32_t ixs_used_pct = 0;
	char ixs_mem_tag[32];

	if (ixs_memory_budget != 0) {
		ixs_used_pct = (uint32_t)((ixs_sz * 100) / ixs_memory_budget);
		sprintf(ixs_mem_tag, " used-pct:%u", ixs_used_pct);
	}
	else {
		ixs_mem_tag[0] = '\0';
	}

	uint64_t data_used_sz = 0;

	as_storage_stats(ns, NULL, &data_used_sz);

	uint32_t data_used_pct = (uint32_t)((data_used_sz * 100) / ns->drives_size);

	char reasons[128] = { 0 };
	char* at = reasons;

	uint32_t cfg_pct = as_load_uint32(&ns->evict_indexes_memory_pct);

	if (cfg_pct != 0 && ixs_used_pct >= cfg_pct) {
		at = stpcpy(at, "indexes-memory & ");
	}

	cfg_pct = as_load_uint32(&ns->pi_evict_mounts_pct);

	if (cfg_pct != 0 && index_dev_used_pct >= cfg_pct) {
		at = stpcpy(at, "index-device & ");
	}

	cfg_pct = as_load_uint32(&ns->si_evict_mounts_pct);

	if (cfg_pct != 0 && sindex_dev_used_pct >= cfg_pct) {
		at = stpcpy(at, "sindex-device & ");
	}

	cfg_pct = as_load_uint32(&ns->storage_evict_used_pct);

	if (cfg_pct != 0 && data_used_pct >= cfg_pct) {
		at = stpcpy(at, "data-used-pct & ");
	}

	if (at != reasons) {
		at[-3] = '\0'; // strip " & " off end

		cf_warning(AS_NSUP, "{%s} breached eviction limit (%s), indexes-memory sz:%lu (%lu + %lu + %lu)%s%s%s, data used-pct:%u",
				ns->name, reasons,
				ixs_sz, index_mem_sz, set_index_sz, sindex_mem_sz, ixs_mem_tag,
				index_dev_tag,
				sindex_dev_tag,
				data_used_pct);

		ns->hwm_breached = true;
		return true;
	}

	cf_debug(AS_NSUP, "{%s} no eviction limit breached, indexes-memory sz:%lu (%lu + %lu + %lu)%s%s%s, data used-pct:%u",
			ns->name,
			ixs_sz, index_mem_sz, set_index_sz, sindex_mem_sz, ixs_mem_tag,
			index_dev_tag,
			sindex_dev_tag,
			data_used_pct);

	ns->hwm_breached = false;

	return false;
}

static uint32_t
find_evict_void_time(as_namespace* ns, uint32_t now)
{
	bool sets_not_evicting[AS_SET_MAX_COUNT + 1] = { false };
	init_sets_not_evicting(ns, sets_not_evicting);

	uint32_t ttl_range = get_ttl_range(ns, now);
	uint32_t n_buckets = ns->evict_hist_buckets;
	linear_hist_reset(ns->evict_hist, now, ttl_range, n_buckets);

	uint32_t n_threads = as_load_uint32(&ns->n_nsup_threads);
	cf_tid tids[n_threads];

	prep_evict_per_thread_info per_threads[n_threads];
	uint32_t pid = 0;

	for (uint32_t i = 0; i < n_threads; i++) {
		prep_evict_per_thread_info* per_thread = &per_threads[i];

		per_thread->ns = ns;
		per_thread->p_pid = &pid;
		per_thread->sets_not_evicting = (const bool*)sets_not_evicting;
		per_thread->evict_hist = linear_hist_create("per-thread-hist",
				LINEAR_HIST_SECONDS, now, ttl_range, n_buckets);

		tids[i] = cf_thread_create_joinable(run_prep_evict, (void*)per_thread);
	}

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_join(tids[i]);

		linear_hist_merge(ns->evict_hist, per_threads[i].evict_hist);
		linear_hist_destroy(per_threads[i].evict_hist);
	}

	linear_hist_threshold threshold;
	uint64_t subtotal = linear_hist_get_threshold_for_fraction(ns->evict_hist,
			ns->evict_tenths_pct, &threshold);
	uint32_t evict_void_time = threshold.value;

	if (evict_void_time == 0xFFFFffff) { // looped past all buckets
		if (subtotal == 0) {
			cf_warning(AS_NSUP, "{%s} no records eligible for eviction",
					ns->name);
		}
		else {
			cf_warning(AS_NSUP, "{%s} would evict all %lu records eligible - not evicting!",
					ns->name, subtotal);
		}

		return 0;
	}

	if (subtotal == 0) {
		cf_warning(AS_NSUP, "{%s} no records below eviction void-time %u - threshold bucket %u, width %u sec, count %lu > target %lu (%.1f pct)",
				ns->name, evict_void_time, threshold.bucket_index,
				threshold.bucket_width, threshold.bucket_count,
				threshold.target_count, (float)ns->evict_tenths_pct / 10.0);

		// If threshold > now and there are no records below it, there's nothing
		// to expire. But the first bucket is special - if threshold == now,
		// it's possible entries in the first bucket have expired.
		return evict_void_time == now ? now : 0;
	}

	cf_info(AS_NSUP, "{%s} found %lu records eligible for eviction at evict-ttl %u - submitting evict-void-time %u",
			ns->name, subtotal, evict_void_time - now, evict_void_time);

	return evict_void_time;
}

static void*
run_prep_evict(void* udata)
{
	prep_evict_per_thread_info* per_thread = (prep_evict_per_thread_info*)udata;
	uint32_t pid;

	while ((pid = as_faa_uint32(per_thread->p_pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(per_thread->ns, pid, &rsv);

		as_index_reduce(rsv.tree, prep_evict_reduce_cb, (void*)per_thread);
		as_partition_release(&rsv);
	}

	return NULL;
}

static bool
prep_evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	prep_evict_per_thread_info* per_thread = (prep_evict_per_thread_info*)udata;
	as_namespace* ns = per_thread->ns;
	uint32_t void_time = r->void_time;

	// Note - also used by cold start evict!
	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	// Note - also used by cold start evict!
	if (is_mrt_provisional(r_ref->r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (void_time != 0 &&
			! per_thread->sets_not_evicting[as_index_get_set_id(r)]) {
		linear_hist_insert_data_point(per_thread->evict_hist, void_time);
	}

	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - expiration/eviction ticker.
//

static void
update_stats(as_namespace* ns, uint64_t n_0_void_time,
		uint64_t n_expired_objects, uint64_t n_evicted_objects,
		uint64_t start_ms)
{
	ns->non_expirable_objects = n_0_void_time;

	ns->n_expired_objects += n_expired_objects;
	ns->n_evicted_objects += n_evicted_objects;

	uint64_t total_duration_ms = cf_getms() - start_ms;

	ns->nsup_cycle_duration = (uint32_t)(total_duration_ms / 1000);

	cf_info(AS_NSUP, "{%s} nsup-done: non-expirable %lu expired (%lu,%lu) evicted (%lu,%lu) evict-ttl %d total-ms %lu",
			ns->name,
			n_0_void_time,
			ns->n_expired_objects, n_expired_objects,
			ns->n_evicted_objects, n_evicted_objects,
			ns->evict_ttl,
			total_duration_ms);

	uint64_t n_deleted_objects = n_expired_objects + n_evicted_objects;

	// A good enough estimate for our purposes, and safer than the real thing.
	uint64_t old_n_objects = as_load_uint64(&ns->n_objects) + n_deleted_objects;

	ns->nsup_cycle_deleted_pct = n_deleted_objects == 0 ? 0 :
			(double)n_deleted_objects * 100.0 / (double)old_n_objects;

	if (total_duration_ms > TOO_LONG_MS &&
			ns->nsup_cycle_deleted_pct > TOO_MUCH_DELETED_PCT) {
		cf_warning(AS_NSUP, "{%s} nsup deleted %.2f%% of namespace - configure more nsup threads?",
				ns->name, ns->nsup_cycle_deleted_pct);
	}
}


//==========================================================
// Local helpers - stop writes.
//

static void*
run_stop_writes(void* udata)
{
	(void)udata;

	while (true) {
		sleep(EVAL_STOP_WRITES_PERIOD);

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			eval_stop_writes(g_config.namespaces[ns_ix], false);
		}
	}

	return NULL;
}

static bool
eval_stop_writes(as_namespace* ns, bool cold_starting)
{
	uint32_t sys_memory_pct = sys_mem_pct();

	// Note that persisted index is not counted against stop-writes.
	uint64_t index_mem_sz = as_namespace_index_persisted(ns) ?
			0 : (ns->n_tombstones + ns->n_objects) * sizeof(as_index);
	uint64_t set_index_sz = as_set_index_used_bytes(ns);
	// Note that persisted sindex is not counted against stop-writes.
	uint64_t sindex_mem_sz = as_namespace_sindex_persisted(ns) ?
			0 : as_sindex_used_bytes(ns);
	uint64_t ixs_sz = index_mem_sz + set_index_sz + sindex_mem_sz;

	// Note that device storage limits are ignored during cold start
	// (data_avail_pct will be 0, data_used_sz is ignored via flag).
	uint32_t data_avail_pct = 0;
	uint64_t data_used_sz = 0;

	as_storage_stats(ns, &data_avail_pct, &data_used_sz);

	uint32_t data_used_pct = (uint32_t)((data_used_sz * 100) / ns->drives_size);

	char reasons[128] = { 0 };
	char* at = reasons;

	uint32_t cfg_pct = as_load_uint32(&ns->stop_writes_sys_memory_pct);
	bool memory_breached = false;

	// Note - not >= since sys_memory_pct is rounded up, not down.
	if (cfg_pct != 0 && sys_memory_pct > cfg_pct) {
		memory_breached = true;
		at = stpcpy(at, "sys-memory & ");
	}

	// This budget serves double as a stop-writes trigger, as if there's a
	// 'stop-writes-indexes-memory-pct' always set to 100.
	uint64_t ixs_memory_budget = as_load_uint64(&ns->indexes_memory_budget);

	if (ixs_memory_budget != 0 && ixs_sz > ixs_memory_budget) {
		memory_breached = true;
		at = stpcpy(at, "indexes-memory & ");
	}

	ns->memory_breached = memory_breached;

	// Note - configured value 0 automatically switches this off. Also, not <=
	// because avail vs. used, though data_avail_pct is rounded down.
	if (data_avail_pct < ns->storage_stop_writes_avail_pct) {
		at = stpcpy(at, "data-avail-pct & ");
	}

	cfg_pct = as_load_uint32(&ns->storage_stop_writes_used_pct);

	if (! cold_starting && cfg_pct != 0 && data_used_pct >= cfg_pct) {
		at = stpcpy(at, "data-used-pct & ");
	}

	if (at != reasons) {
		at[-3] = '\0'; // strip " & " off end

		cf_warning(AS_NSUP, "{%s} breached stop-writes limit (%s), sys-memory pct:%u, indexes-memory sz:%lu (%lu + %lu + %lu), data avail-pct:%u used-pct:%u",
				ns->name, reasons, sys_memory_pct,
				ixs_sz, index_mem_sz, set_index_sz, sindex_mem_sz,
				data_avail_pct, data_used_pct);

		ns->stop_writes = true;
		return true;
	}

	cf_debug(AS_NSUP, "{%s} stop-writes limit not breached, sys-memory pct:%u, indexes-memory sz:%lu (%lu + %lu + %lu), data avail-pct:%u used-pct:%u",
			ns->name, sys_memory_pct,
			ixs_sz, index_mem_sz, set_index_sz, sindex_mem_sz,
			data_avail_pct, data_used_pct);

	ns->stop_writes = false;

	return false;
}

static uint32_t
sys_mem_pct(void)
{
	uint64_t free_mem_kbytes;
	uint32_t free_mem_pct;
	uint64_t thp_mem_kbytes;

	sys_mem_info(&free_mem_kbytes, &free_mem_pct, &thp_mem_kbytes);

	return 100 - free_mem_pct;
}


//==========================================================
// Local helpers - background histograms.
//

static void*
run_nsup_histograms(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;

	bool wait = false; // make sure we run once right away on startup
	uint64_t last_time = 0;

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t period = ns->nsup_hist_period;
		uint64_t curr_time = cf_get_seconds();

		if (period == 0 || (wait && curr_time - last_time < period)) {
			continue;
		}

		wait = true;
		last_time = curr_time;

		collect_nsup_histograms(ns);
	}

	return NULL;
}

static void
collect_nsup_histograms(as_namespace* ns)
{
	if (ns->n_objects == 0) {
		return;
	}

	cf_info(AS_NSUP, "{%s} collecting ttl & object size info ...", ns->name);

	uint32_t now = as_record_void_time_get();
	uint32_t ttl_range = get_ttl_range(ns, now);

	linear_hist_clear(ns->ttl_hist, now, ttl_range);

	uint32_t ns_max_sz = as_load_uint32(&ns->max_record_size);

	histogram_clear(ns->obj_size_log_hist);
	linear_hist_clear(ns->obj_size_lin_hist, 0, ns_max_sz);

	uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;
		uint32_t max_sz = as_mrt_monitor_is_monitor_set_id(ns, set_id) ?
				MAX_MONITOR_RECORD_SZ : ns_max_sz;

		if (ns->set_ttl_hists[set_id] != NULL) {
			linear_hist_clear(ns->set_ttl_hists[set_id], now, ttl_range);
		}
		else {
			char hist_name[HISTOGRAM_NAME_SIZE];
			const char* set_name =
					as_namespace_get_set_name(ns, (uint16_t)set_id);

			sprintf(hist_name, "{%s}-%s-ttl", ns->name, set_name);
			ns->set_ttl_hists[set_id] =
					linear_hist_create(hist_name, LINEAR_HIST_SECONDS, now,
							ttl_range, TTL_HIST_NUM_BUCKETS);
		}

		if (ns->set_obj_size_log_hists[set_id] != NULL) {
			histogram_clear(ns->set_obj_size_log_hists[set_id]);
			linear_hist_clear(ns->set_obj_size_lin_hists[set_id], 0, max_sz);
		}
		else {
			char hist_name[HISTOGRAM_NAME_SIZE];
			const char* set_name =
					as_namespace_get_set_name(ns, (uint16_t)set_id);

			sprintf(hist_name, "{%s}-%s-obj-size-log2", ns->name, set_name);
			ns->set_obj_size_log_hists[set_id] =
					histogram_create(hist_name, HIST_SIZE);

			sprintf(hist_name, "{%s}-%s-obj-size-linear", ns->name, set_name);
			ns->set_obj_size_lin_hists[set_id] =
					linear_hist_create(hist_name, LINEAR_HIST_SIZE, 0, max_sz,
							OBJ_SIZE_HIST_NUM_BUCKETS);
		}
	}

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		as_index_reduce(rsv.tree, nsup_histograms_reduce_cb, (void*)ns);
		as_partition_release(&rsv);
	}

	linear_hist_dump(ns->ttl_hist);
	linear_hist_save_info(ns->ttl_hist);

	histogram_save_info(ns->obj_size_log_hist);
	linear_hist_save_info(ns->obj_size_lin_hist);

	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;

		linear_hist_dump(ns->set_ttl_hists[set_id]);
		linear_hist_save_info(ns->set_ttl_hists[set_id]);

		histogram_save_info(ns->set_obj_size_log_hists[set_id]);
		linear_hist_save_info(ns->set_obj_size_lin_hists[set_id]);
	}

	cf_info(AS_NSUP, "{%s} ... done collecting ttl & object size info",
			ns->name);
}

static bool
nsup_histograms_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	as_namespace* ns = (as_namespace*)udata;
	uint32_t set_id = as_index_get_set_id(r);
	linear_hist* set_ttl_hist = ns->set_ttl_hists[set_id];
	uint32_t void_time = r->void_time;

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_mrt_monitor_is_monitor_record(ns, r)) {
		linear_hist_insert_data_point(ns->ttl_hist, void_time);
	}

	if (set_ttl_hist != NULL) {
		linear_hist_insert_data_point(set_ttl_hist, void_time);
	}

	uint32_t size = as_record_stored_size(r);

	histogram_insert_raw_unsafe(ns->obj_size_log_hist, size);
	linear_hist_insert_data_point(ns->obj_size_lin_hist, size);

	histogram* set_obj_size_log_hist = ns->set_obj_size_log_hists[set_id];
	linear_hist* set_obj_size_lin_hist = ns->set_obj_size_lin_hists[set_id];

	if (set_obj_size_log_hist != NULL) {
		histogram_insert_raw_unsafe(set_obj_size_log_hist, size);
		linear_hist_insert_data_point(set_obj_size_lin_hist, size);
	}

	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - cold start eviction.
//

static bool
cold_start_evict(as_namespace* ns)
{
	if (ns->cold_start_record_add_count++ % EVAL_WRITE_STATE_FREQUENCY != 0) {
		return true;
	}

	uint32_t now = as_record_void_time_get();

	if (now > ns->cold_start_now) {
		ns->cold_start_now = now;
	}

	if (eval_stop_writes(ns, true)) {
		cf_warning(AS_NSUP, "{%s} hit stop-writes limit", ns->name);
		return false;
	}

	if (! eval_hwm_breached(ns)) {
		return true;
	}

	if (ns->cold_start_eviction_disabled) {
		cf_warning(AS_NSUP, "{%s} breached but eviction disabled", ns->name);
		return true;
	}

	cf_info(AS_NSUP, "{%s} cold start building evict histogram ...", ns->name);

	uint32_t ttl_range = get_ttl_range(ns, now);
	uint32_t n_buckets = ns->evict_hist_buckets > COLD_START_HIST_MIN_BUCKETS ?
			ns->evict_hist_buckets : COLD_START_HIST_MIN_BUCKETS;

	bool sets_not_evicting[AS_SET_MAX_COUNT + 1] = { false };
	init_sets_not_evicting(ns, sets_not_evicting);

	uint32_t n_cpus = cf_topo_count_cpus();
	cf_tid tids[n_cpus];

	prep_evict_per_thread_info per_threads[n_cpus];
	uint32_t pid = 0;

	for (uint32_t n = 0; n < n_cpus; n++) {
		prep_evict_per_thread_info* per_thread = &per_threads[n];

		per_thread->ns = ns;
		per_thread->p_pid = &pid;
		per_thread->i_cpu = n;
		per_thread->sets_not_evicting = sets_not_evicting;
		per_thread->evict_hist = linear_hist_create("per-thread-hist",
				LINEAR_HIST_SECONDS, now, ttl_range, n_buckets);

		tids[n] = cf_thread_create_joinable(run_prep_cold_start_evict,
				(void*)per_thread);
	}

	for (uint32_t n = 0; n < n_cpus; n++) {
		cf_thread_join(tids[n]);

		if (n == 0) {
			continue;
		}

		linear_hist_merge(per_threads[0].evict_hist, per_threads[n].evict_hist);
		linear_hist_destroy(per_threads[n].evict_hist);
	}

	uint64_t n_evictable =
			set_cold_start_threshold(ns, per_threads[0].evict_hist);

	linear_hist_destroy(per_threads[0].evict_hist);

	if (n_evictable == 0) {
		cf_warning(AS_NSUP, "{%s} hwm breached but nothing to evict", ns->name);
		return true;
	}

	cf_info(AS_NSUP, "{%s} cold start found %lu records eligible for eviction at evict-ttl %u",
			ns->name, n_evictable, ns->evict_void_time - now);

	evict_overall_info overall = {
			.ns = ns,
			.sets_not_evicting = sets_not_evicting
			// Note - .now and .expired not needed at startup.
	};

	for (uint32_t n = 0; n < n_cpus; n++) {
		tids[n] = cf_thread_create_joinable(run_cold_start_evict,
				(void*)&overall);
	}

	for (uint32_t n = 0; n < n_cpus; n++) {
		cf_thread_join(tids[n]);
	}

	cf_info(AS_NSUP, "{%s} cold start evicted %lu records, found %lu 0-void-time records",
			ns->name, overall.n_evicted, overall.n_0_void_time);

	return true;
}

static void*
run_prep_cold_start_evict(void* udata)
{
	prep_evict_per_thread_info* per_thread = (prep_evict_per_thread_info*)udata;

	cf_topo_pin_to_cpu((cf_topo_cpu_index)per_thread->i_cpu);

	uint32_t pid;

	while ((pid = as_faa_uint32(per_thread->p_pid, 1)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup. Otherwise,
		// use the same reduce callback as at runtime.
		as_index_reduce(per_thread->ns->partitions[pid].tree,
				prep_evict_reduce_cb, (void*)per_thread);
	}

	return NULL;
}

static uint64_t
set_cold_start_threshold(as_namespace* ns, linear_hist* hist)
{
	linear_hist_threshold threshold;
	uint64_t subtotal = linear_hist_get_threshold_for_fraction(hist, ns->evict_tenths_pct, &threshold);
	uint32_t evict_void_time = threshold.value;

	if (evict_void_time == 0xFFFFffff) { // looped past all buckets
		if (subtotal == 0) {
			cf_warning(AS_NSUP, "{%s} cold start found no records eligible for eviction",
					ns->name);
		}
		else {
			cf_warning(AS_NSUP, "{%s} cold start would evict all %lu records eligible - not evicting!",
					ns->name, subtotal);
		}

		return 0;
	}

	if (subtotal == 0) {
		cf_warning(AS_NSUP, "{%s} cold start found no records below eviction void-time %u - threshold bucket %u, width %u sec, count %lu > target %lu (%.1f pct)",
				ns->name, evict_void_time, threshold.bucket_index,
				threshold.bucket_width, threshold.bucket_count,
				threshold.target_count, (float)ns->evict_tenths_pct / 10.0);

		// Unlike at runtime, bottom bucket is not special, no need to expire.
		return 0;
	}

	ns->evict_void_time = evict_void_time;

	return subtotal;
}

static void*
run_cold_start_evict(void* udata)
{
	evict_overall_info* overall = (evict_overall_info*)udata;

	cf_topo_pin_to_cpu((cf_topo_cpu_index)as_faa_uint32(&overall->i_cpu, 1));

	as_namespace* ns = overall->ns;

	evict_per_thread_info per_thread = {
			.ns = ns,
			.sets_not_evicting = overall->sets_not_evicting
	};

	uint32_t pid;

	while ((pid = as_faa_uint32(&overall->pid, 1)) < AS_PARTITIONS) {
		// Don't bother with real partition reservations - it's startup.
		as_partition_reservation rsv = { .tree = ns->partitions[pid].tree };
		per_thread.rsv = &rsv;

		as_index_reduce(rsv.tree, cold_start_evict_reduce_cb, &per_thread);
	}

	as_add_uint64(&overall->n_0_void_time, (int64_t)per_thread.n_0_void_time);
	as_add_uint64(&overall->n_evicted, (int64_t)per_thread.n_evicted);

	return NULL;
}

static bool
cold_start_evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_index* r = r_ref->r;
	evict_per_thread_info* per_thread = (evict_per_thread_info*)udata;
	as_namespace* ns = per_thread->ns;
	uint32_t void_time = r->void_time;

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (void_time == 0) {
		per_thread->n_0_void_time++;
	}
	else if (! per_thread->sets_not_evicting[as_index_get_set_id(r)] &&
			ns->evict_void_time > void_time) {
		remove_from_sindex(ns, r_ref); // no-op unless data-in-memory

		as_index_tree* tree = per_thread->rsv->tree;

		// Note - can't be a tombstone.
		as_set_index_delete(ns, tree, as_index_get_set_id(r), r_ref->r_h);
		as_index_delete(tree, &r->keyd);
		per_thread->n_evicted++;
	}

	as_record_done(r_ref, ns);

	return true;
}


//==========================================================
// Local helpers - generic.
//

static bool
sets_protected(as_namespace* ns)
{
	uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t j = 0; j < num_sets; j++) {
		as_set* p_set;

		if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) !=
				CF_VMAPX_OK) {
			cf_crash(AS_NSUP, "failed to get set index %u from vmap", j);
		}

		if (p_set->eviction_disabled) {
			return true;
		}
	}

	return false;
}

static void
init_sets_not_evicting(as_namespace* ns, bool sets_not_evicting[])
{
	uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t j = 0; j < num_sets; j++) {
		as_set* p_set;

		if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) !=
				CF_VMAPX_OK) {
			cf_crash(AS_NSUP, "failed to get set index %u from vmap", j);
		}

		if (p_set->eviction_disabled) {
			sets_not_evicting[j + 1] = true;
		}
	}
}

static uint32_t
get_ttl_range(as_namespace* ns, uint32_t now)
{
	uint32_t max_void_time = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		// Note - non-masters may or may not have a max-void-time.
		uint32_t partition_max_void_time = ns->partitions[pid].max_void_time;

		if (partition_max_void_time > max_void_time) {
			max_void_time = partition_max_void_time;
		}
	}

	return max_void_time > now ? max_void_time - now : 0;
}
