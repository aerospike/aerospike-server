/*
 * scan_manager.c
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

#include "base/scan_manager.h"

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"

#include "base/cfg.h"
#include "base/monitor.h"
#include "base/scan_job.h"
#include "fabric/partition.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

typedef struct find_item_s {
	uint64_t trid;
	as_scan_job* _job;
	bool remove_job;
} find_item;

typedef struct info_item_s {
	as_scan_job** p_job;
} info_item;


//==========================================================
// Globals.
//

uint32_t g_n_threads = 0;

static as_scan_manager g_mgr;


//==========================================================
// Forward declarations.
//

static void add_scan_job_thread(as_scan_job* _job);
static void evict_finished_jobs(void);
static int abort_cb(void* buf, void* udata);
static int info_cb(void* buf, void* udata);
static as_scan_job* find_any(uint64_t trid);
static as_scan_job* find_active(uint64_t trid);
static as_scan_job* remove_active(uint64_t trid);
static as_scan_job* find_job(cf_queue* jobs, uint64_t trid, bool remove_job);
static int find_cb(void* buf, void* udata);


//==========================================================
// Public API.
//

void
as_scan_manager_init(void)
{
	cf_mutex_init(&g_mgr.lock);

	g_mgr.active_jobs = cf_queue_create(sizeof(as_scan_job*), false);
	g_mgr.finished_jobs = cf_queue_create(sizeof(as_scan_job*), false);
}

int
as_scan_manager_start_job(as_scan_job* _job)
{
	cf_mutex_lock(&g_mgr.lock);

	if (g_n_threads >= g_config.n_scan_threads_limit) {
		cf_warning(AS_SCAN, "at scan threads limit - can't start new scan");
		cf_mutex_unlock(&g_mgr.lock);
		return AS_SCAN_ERR_FORBIDDEN;
	}

	// Make sure trid is unique.
	if (find_any(_job->trid)) {
		cf_warning(AS_SCAN, "job with trid %lu already active", _job->trid);
		cf_mutex_unlock(&g_mgr.lock);
		return AS_SCAN_ERR_PARAMETER;
	}

	_job->start_us = cf_getus();
	_job->base_us = _job->start_us;

	cf_queue_push(g_mgr.active_jobs, &_job);

	add_scan_job_thread(_job);

	cf_mutex_unlock(&g_mgr.lock);

	return 0;
}

void
as_scan_manager_add_job_thread(as_scan_job* _job)
{
	if ((_job->n_pids_requested != 0 &&
			_job->n_threads >= (uint32_t)_job->n_pids_requested) ||
			_job->n_threads >= _job->ns->n_single_scan_threads) {
		return;
	}

	cf_mutex_lock(&g_mgr.lock);

	if (g_n_threads < g_config.n_scan_threads_limit) {
		add_scan_job_thread(_job);
	}

	cf_mutex_unlock(&g_mgr.lock);
}

void
as_scan_manager_add_max_job_threads(as_scan_job* _job)
{
	uint32_t n_pids = _job->n_pids_requested == 0 ?
			AS_PARTITIONS : (uint32_t)_job->n_pids_requested;

	if (_job->n_threads >= n_pids) {
		return;
	}

	uint32_t single_max = as_load_uint32(&_job->ns->n_single_scan_threads);

	if (_job->n_threads >= single_max) {
		return;
	}

	// Don't need more threads than there are partitions to scan.
	uint32_t n_extra = n_pids - _job->n_threads;

	uint32_t single_extra = single_max - _job->n_threads;

	if (single_extra < n_extra) {
		n_extra = single_extra;
	}

	uint32_t all_max = as_load_uint32(&g_config.n_scan_threads_limit);

	cf_mutex_lock(&g_mgr.lock);

	if (g_n_threads >= all_max) {
		cf_mutex_unlock(&g_mgr.lock);
		return;
	}

	uint32_t all_extra = all_max - g_n_threads;

	if (all_extra < n_extra) {
		n_extra = all_extra;
	}

	for (uint32_t n = 0; n < n_extra; n++) {
		add_scan_job_thread(_job);
	}

	cf_mutex_unlock(&g_mgr.lock);
}

void
as_scan_manager_finish_job(as_scan_job* _job)
{
	cf_mutex_lock(&g_mgr.lock);

	remove_active(_job->trid);

	_job->finish_us = cf_getus();
	cf_queue_push(g_mgr.finished_jobs, &_job);
	evict_finished_jobs();

	cf_mutex_unlock(&g_mgr.lock);
}

void
as_scan_manager_abandon_job(as_scan_job* _job, int reason)
{
	_job->abandoned = reason;
}

bool
as_scan_manager_abort_job(uint64_t trid)
{
	cf_mutex_lock(&g_mgr.lock);

	as_scan_job* _job = find_active(trid);

	cf_mutex_unlock(&g_mgr.lock);

	if (_job == NULL) {
		return false;
	}

	_job->abandoned = AS_SCAN_ERR_USER_ABORT;

	return true;
}

uint32_t
as_scan_manager_abort_all_jobs(void)
{
	cf_mutex_lock(&g_mgr.lock);

	uint32_t n_jobs = cf_queue_sz(g_mgr.active_jobs);

	if (n_jobs != 0) {
		cf_queue_reduce(g_mgr.active_jobs, abort_cb, NULL);
	}

	cf_mutex_unlock(&g_mgr.lock);

	return n_jobs;
}

void
as_scan_manager_limit_finished_jobs(void)
{
	cf_mutex_lock(&g_mgr.lock);

	evict_finished_jobs();

	cf_mutex_unlock(&g_mgr.lock);
}

as_mon_jobstat*
as_scan_manager_get_job_info(uint64_t trid)
{
	cf_mutex_lock(&g_mgr.lock);

	as_scan_job* _job = find_any(trid);

	if (_job == NULL) {
		cf_mutex_unlock(&g_mgr.lock);
		return NULL;
	}

	as_mon_jobstat* stat = cf_malloc(sizeof(as_mon_jobstat));

	memset(stat, 0, sizeof(as_mon_jobstat));
	as_scan_job_info(_job, stat);

	cf_mutex_unlock(&g_mgr.lock);

	return stat; // caller must free this
}

as_mon_jobstat*
as_scan_manager_get_info(int* size)
{
	*size = 0;

	cf_mutex_lock(&g_mgr.lock);

	uint32_t n_jobs = cf_queue_sz(g_mgr.active_jobs) +
			cf_queue_sz(g_mgr.finished_jobs);

	if (n_jobs == 0) {
		cf_mutex_unlock(&g_mgr.lock);
		return NULL;
	}

	as_scan_job* _jobs[n_jobs];
	info_item item = { _jobs };

	cf_queue_reduce_reverse(g_mgr.active_jobs, info_cb, &item);
	cf_queue_reduce_reverse(g_mgr.finished_jobs, info_cb, &item);

	size_t stats_size = sizeof(as_mon_jobstat) * n_jobs;
	as_mon_jobstat* stats = cf_malloc(stats_size);

	memset(stats, 0, stats_size);

	for (uint32_t i = 0; i < n_jobs; i++) {
		as_scan_job_info(_jobs[i], &stats[i]);
	}

	cf_mutex_unlock(&g_mgr.lock);

	*size = (int)n_jobs; // no uint32_t upstream - may remove monitor

	return stats; // caller must free this
}

uint32_t
as_scan_manager_get_active_job_count(void)
{
	cf_mutex_lock(&g_mgr.lock);

	uint32_t n_jobs = cf_queue_sz(g_mgr.active_jobs);

	cf_mutex_unlock(&g_mgr.lock);

	return n_jobs;
}


//==========================================================
// Local helpers.
//

static void
add_scan_job_thread(as_scan_job* _job)
{
	as_incr_uint32(&g_n_threads);
	as_incr_uint32(&_job->n_threads);

	cf_thread_create_transient(as_scan_job_run, _job);
}

static void
evict_finished_jobs(void)
{
	uint32_t max_allowed = as_load_uint32(&g_config.scan_max_done);

	while (cf_queue_sz(g_mgr.finished_jobs) > max_allowed) {
		as_scan_job* _job;

		cf_queue_pop(g_mgr.finished_jobs, &_job, 0);
		as_scan_job_destroy(_job);
	}
}

static int
abort_cb(void* buf, void* udata)
{
	(void)udata;

	as_scan_job* _job = *(as_scan_job**)buf;

	_job->abandoned = AS_SCAN_ERR_USER_ABORT;

	return 0;
}

static int
info_cb(void* buf, void* udata)
{
	as_scan_job* _job = *(as_scan_job**)buf;
	info_item* item = (info_item*)udata;

	*item->p_job++ = _job;

	return 0;
}

static as_scan_job*
find_any(uint64_t trid)
{
	as_scan_job* _job = find_job(g_mgr.active_jobs, trid, false);

	if (_job == NULL) {
		_job = find_job(g_mgr.finished_jobs, trid, false);
	}

	return _job;
}

static as_scan_job*
find_active(uint64_t trid)
{
	return find_job(g_mgr.active_jobs, trid, false);
}

static as_scan_job*
remove_active(uint64_t trid)
{
	return find_job(g_mgr.active_jobs, trid, true);
}

static as_scan_job*
find_job(cf_queue* jobs, uint64_t trid, bool remove_job)
{
	find_item item = { trid, NULL, remove_job };

	cf_queue_reduce(jobs, find_cb, &item);

	return item._job;
}

static int
find_cb(void* buf, void* udata)
{
	as_scan_job* _job = *(as_scan_job**)buf;
	find_item* match = (find_item*)udata;

	if (match->trid == _job->trid) {
		match->_job = _job;
		return match->remove_job ? -2 : -1;
	}

	return 0;
}
