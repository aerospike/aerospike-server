/*
 * job_manager.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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
 * along with this program. If not, see http://www.gnu.org/licenses/
 */

//==============================================================================
// Includes.
//

#include "base/job_manager.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "aerospike/as_string.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_queue_priority.h"

#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/monitor.h"
#include "fabric/partition.h"


//==============================================================================
// Globals.
//

static cf_atomic32 g_job_trid = 0;



//==============================================================================
// Non-class-specific utilities.
//

static inline uint64_t
job_trid(uint64_t trid)
{
	return trid != 0 ? trid : (uint64_t)cf_atomic32_incr(&g_job_trid);
}

static inline const char*
job_result_str(int result_code)
{
	switch (result_code) {
	case 0:
		return "ok";
	case AS_JOB_FAIL_UNKNOWN:
		return "abandoned-unknown";
	case AS_JOB_FAIL_CLUSTER_KEY:
		return "abandoned-cluster-key";
	case AS_JOB_FAIL_USER_ABORT:
		return "user-aborted";
	case AS_JOB_FAIL_RESPONSE_ERROR:
		return "abandoned-response-error";
	case AS_JOB_FAIL_RESPONSE_TIMEOUT:
		return "abandoned-response-timeout";
	default:
		return "abandoned-?";
	}
}

static inline int
safe_priority(int priority) {
	// Handles priority 0, the 'auto' priority.
	return priority < AS_JOB_PRIORITY_LOW || priority > AS_JOB_PRIORITY_HIGH ?
			AS_JOB_PRIORITY_MEDIUM : priority;
}



//==============================================================================
// as_priority_thread_pool class implementation.
// TODO - move to common.
//

//----------------------------------------------------------
// as_priority_thread_pool typedefs and forward declarations.
//

typedef struct queue_task_s {
	as_priority_thread_pool_task_fn	task_fn;
	void*							task;
} queue_task;

uint32_t create_threads(as_priority_thread_pool* pool, uint32_t count);
void shutdown_threads(as_priority_thread_pool* pool, uint32_t count);
void* run_pool_thread(void* udata);
int compare_cb(void* buf, void* task);

//----------------------------------------------------------
// as_priority_thread_pool public API.
//

bool
as_priority_thread_pool_init(as_priority_thread_pool* pool, uint32_t n_threads)
{
	pthread_mutex_init(&pool->lock, NULL);

	// Initialize queues.
	pool->dispatch_queue = cf_queue_priority_create(sizeof(queue_task), true);
	pool->complete_queue = cf_queue_create(sizeof(uint32_t), true);

	// Start detached threads.
	pool->n_threads = create_threads(pool, n_threads);

	return pool->n_threads == n_threads;
}

void
as_priority_thread_pool_shutdown(as_priority_thread_pool* pool)
{
	shutdown_threads(pool, pool->n_threads);
	cf_queue_priority_destroy(pool->dispatch_queue);
	cf_queue_destroy(pool->complete_queue);
	pthread_mutex_destroy(&pool->lock);
}

bool
as_priority_thread_pool_resize(as_priority_thread_pool* pool,
		uint32_t n_threads)
{
	pthread_mutex_lock(&pool->lock);

	bool result = true;

	if (n_threads != pool->n_threads) {
		if (n_threads < pool->n_threads) {
			// Shutdown excess threads.
			shutdown_threads(pool, pool->n_threads - n_threads);
			pool->n_threads = n_threads;
		}
		else {
			// Start new detached threads.
			pool->n_threads += create_threads(pool,
					n_threads - pool->n_threads);
			result = pool->n_threads == n_threads;
		}
	}

	pthread_mutex_unlock(&pool->lock);

	return result;
}

bool
as_priority_thread_pool_queue_task(as_priority_thread_pool* pool,
		as_priority_thread_pool_task_fn task_fn, void* task, int priority)
{
	queue_task qtask = { task_fn, task };

	return cf_queue_priority_push(pool->dispatch_queue, &qtask, priority) ==
			CF_QUEUE_OK;
}

bool
as_priority_thread_pool_remove_task(as_priority_thread_pool* pool, void* task)
{
	queue_task qtask = { NULL, NULL };

	cf_queue_priority_reduce_pop(pool->dispatch_queue, &qtask, compare_cb,
			task);

	return qtask.task != NULL;
}

void
as_priority_thread_pool_change_task_priority(as_priority_thread_pool* pool,
		void* task, int new_priority)
{
	cf_queue_priority_reduce_change(pool->dispatch_queue, new_priority,
			compare_cb, task);
}

//----------------------------------------------------------
// as_priority_thread_pool utilities.
//

uint32_t
create_threads(as_priority_thread_pool* pool, uint32_t count)
{
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	uint32_t n_threads_created = 0;
	pthread_t thread;

	for (uint32_t i = 0; i < count; i++) {
		if (pthread_create(&thread, &attrs, run_pool_thread, pool) == 0) {
			n_threads_created++;
		}
	}

	return n_threads_created;
}

void
shutdown_threads(as_priority_thread_pool* pool, uint32_t count)
{
	// Send terminator tasks to kill 'count' threads.
	queue_task task = { NULL, NULL };

	for (uint32_t i = 0; i < count; i++) {
		cf_queue_priority_push(pool->dispatch_queue, &task,
				CF_QUEUE_PRIORITY_HIGH);
	}

	// Wait till threads finish.
	uint32_t complete;

	for (uint32_t i = 0; i < count; i++) {
		cf_queue_pop(pool->complete_queue, &complete, CF_QUEUE_FOREVER);
	}
}

void*
run_pool_thread(void* udata)
{
	as_priority_thread_pool* pool = (as_priority_thread_pool*)udata;
	queue_task qtask;

	// Retrieve tasks from queue and execute.
	while (cf_queue_priority_pop(pool->dispatch_queue, &qtask,
			CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// A null task indicates thread should be shut down.
		if (! qtask.task_fn) {
			break;
		}

		// Run task.
		qtask.task_fn(qtask.task);
	}

	// Send thread completion event back to caller.
	uint32_t complete = 1;

	cf_queue_push(pool->complete_queue, &complete);

	return NULL;
}

int
compare_cb(void* buf, void* task)
{
	return ((queue_task*)buf)->task == task ? -1 : 0;
}



//==============================================================================
// as_job base class implementation.
//

//----------------------------------------------------------
// as_job typedefs and forward declarations.
//

static inline const char* as_job_safe_set_name(as_job* _job);
static inline float as_job_progress(as_job* _job);
int as_job_partition_reserve(as_job* _job, int pid, as_partition_reservation* rsv);

//----------------------------------------------------------
// as_job public API.
//

void
as_job_init(as_job* _job, const as_job_vtable* vtable,
		as_job_manager* mgr, as_job_rsv_type rsv_type, uint64_t trid,
		as_namespace* ns, uint16_t set_id, int priority)
{
	memset(_job, 0, sizeof(as_job));

	_job->vtable	= *vtable;
	_job->mgr		= mgr;
	_job->rsv_type	= rsv_type;
	_job->trid		= job_trid(trid);
	_job->ns		= ns;
	_job->set_id	= set_id;
	_job->priority	= safe_priority(priority);

	pthread_mutex_init(&_job->requeue_lock, NULL);
}

void
as_job_slice(void* task)
{
	as_job* _job = (as_job*)task;

	int pid = _job->next_pid;
	as_partition_reservation rsv;

	if ((pid = as_job_partition_reserve(_job, pid, &rsv)) == AS_PARTITIONS) {
		_job->next_pid = AS_PARTITIONS;
		as_job_active_release(_job);
		return;
	}

	pthread_mutex_lock(&_job->requeue_lock);

	if (_job->abandoned != 0) {
		pthread_mutex_unlock(&_job->requeue_lock);
		as_partition_release(&rsv);
		as_job_active_release(_job);
		return;
	}

	if ((_job->next_pid = pid + 1) < AS_PARTITIONS) {
		as_job_active_reserve(_job);
		as_job_manager_requeue_job(_job->mgr, _job);
	}

	pthread_mutex_unlock(&_job->requeue_lock);

	_job->vtable.slice_fn(_job, &rsv);

	as_partition_release(&rsv);
	as_job_active_release(_job);
}

void
as_job_finish(as_job* _job)
{
	_job->vtable.finish_fn(_job);
	as_job_manager_finish_job(_job->mgr, _job);
}

void
as_job_destroy(as_job* _job)
{
	_job->vtable.destroy_fn(_job);

	pthread_mutex_destroy(&_job->requeue_lock);
	cf_free(_job);
}

void
as_job_info(as_job* _job, as_mon_jobstat* stat)
{
	uint64_t now = cf_getms();
	bool done = _job->finish_ms != 0;
	uint64_t since_start_ms = now - _job->start_ms;
	uint64_t since_finish_ms = done ? now - _job->finish_ms : 0;
	uint64_t active_ms = done ?
			_job->finish_ms - _job->start_ms : since_start_ms;

	stat->trid				= _job->trid;
	stat->priority			= (uint32_t)_job->priority;
	stat->progress_pct		= as_job_progress(_job);
	stat->run_time			= active_ms;
	stat->time_since_done	= since_finish_ms;
	stat->recs_read			= cf_atomic64_get(_job->n_records_read);

	strcpy(stat->ns, _job->ns->name);
	strcpy(stat->set, as_job_safe_set_name(_job));

	char status[64];
	sprintf(status, "%s(%s)", done ? "done" : "active",
			job_result_str(_job->abandoned));
	as_strncpy(stat->status, status, sizeof(stat->status));

	_job->vtable.info_mon_fn(_job, stat);
}

void
as_job_active_reserve(as_job* _job)
{
	cf_atomic32_incr(&_job->active_rc);
}

void
as_job_active_release(as_job* _job)
{
	if (cf_atomic32_decr(&_job->active_rc) == 0) {
		as_job_finish(_job);
	}
}

//----------------------------------------------------------
// as_job utilities.
//

static inline const char*
as_job_safe_set_name(as_job* _job)
{
	const char* set_name = as_namespace_get_set_name(_job->ns, _job->set_id);

	return set_name ? set_name : ""; // empty string means no set name displayed
}

static inline float
as_job_progress(as_job* _job)
{
	return ((float)(_job->next_pid * 100)) / (float)AS_PARTITIONS;
}

int
as_job_partition_reserve(as_job* _job, int pid, as_partition_reservation* rsv)
{
	if (_job->rsv_type == RSV_WRITE) {
		while (pid < AS_PARTITIONS && as_partition_reserve_write(_job->ns, pid,
				rsv, NULL) != 0) {
			pid++;
		}
	}
	else if (_job->rsv_type == RSV_MIGRATE) {
		as_partition_reserve(_job->ns, pid, rsv);
	}
	else {
		cf_crash(AS_JOB, "bad job rsv type %d", _job->rsv_type);
	}

	return pid;
}



//==============================================================================
// as_job_manager class implementation.
//

//----------------------------------------------------------
// as_job_manager typedefs and forward declarations.
//

typedef struct find_item_s {
	uint64_t	trid;
	as_job*		_job;
	bool		remove;
} find_item;

typedef struct info_item_s {
	as_job**	p_job;
} info_item;

void as_job_manager_evict_finished_jobs(as_job_manager* mgr);
int as_job_manager_find_cb(void* buf, void* udata);
as_job* as_job_manager_find_job(cf_queue* jobs, uint64_t trid, bool remove);
static inline as_job* as_job_manager_find_any(as_job_manager* mgr, uint64_t trid);
static inline as_job* as_job_manager_find_active(as_job_manager* mgr, uint64_t trid);
static inline as_job* as_job_manager_remove_active(as_job_manager* mgr, uint64_t trid);
int as_job_manager_info_cb(void* buf, void* udata);

//----------------------------------------------------------
// as_job_manager public API.
//

void
as_job_manager_init(as_job_manager* mgr, uint32_t max_active, uint32_t max_done,
		uint32_t n_threads)
{
	mgr->max_active	= max_active;
	mgr->max_done	= max_done;

	if (pthread_mutex_init(&mgr->lock, NULL) != 0) {
		cf_crash(AS_JOB, "job manager failed mutex init");
	}

	mgr->active_jobs = cf_queue_create(sizeof(as_job*), false);
	mgr->finished_jobs = cf_queue_create(sizeof(as_job*), false);

	if (! as_priority_thread_pool_init(&mgr->thread_pool, n_threads)) {
		cf_crash(AS_JOB, "job manager failed thread pool init");
	}
}

int
as_job_manager_start_job(as_job_manager* mgr, as_job* _job)
{
	pthread_mutex_lock(&mgr->lock);

	if (cf_queue_sz(mgr->active_jobs) >= mgr->max_active) {
		cf_warning(AS_JOB, "max of %u jobs currently active", mgr->max_active);
		pthread_mutex_unlock(&mgr->lock);
		return AS_JOB_FAIL_FORBIDDEN;
	}

	// Make sure trid is unique.
	if (as_job_manager_find_any(mgr, _job->trid)) {
		cf_warning(AS_JOB, "job with trid %lu already active", _job->trid);
		pthread_mutex_unlock(&mgr->lock);
		return AS_JOB_FAIL_PARAMETER;
	}

	_job->start_ms = cf_getms();
	as_job_active_reserve(_job);
	cf_queue_push(mgr->active_jobs, &_job);
	as_priority_thread_pool_queue_task(&mgr->thread_pool, as_job_slice, _job,
			_job->priority);

	pthread_mutex_unlock(&mgr->lock);
	return 0;
}

void
as_job_manager_requeue_job(as_job_manager* mgr, as_job* _job)
{
	as_priority_thread_pool_queue_task(&mgr->thread_pool, as_job_slice, _job,
			_job->priority);
}

void
as_job_manager_finish_job(as_job_manager* mgr, as_job* _job)
{
	pthread_mutex_lock(&mgr->lock);

	as_job_manager_remove_active(mgr, _job->trid);
	_job->finish_ms = cf_getms();
	cf_queue_push(mgr->finished_jobs, &_job);
	as_job_manager_evict_finished_jobs(mgr);

	pthread_mutex_unlock(&mgr->lock);
}

void
as_job_manager_abandon_job(as_job_manager* mgr, as_job* _job, int reason)
{
	pthread_mutex_lock(&_job->requeue_lock);
	_job->abandoned = reason;
	bool found = as_priority_thread_pool_remove_task(&mgr->thread_pool, _job);
	pthread_mutex_unlock(&_job->requeue_lock);

	if (found) {
		as_job_active_release(_job);
	}
}

bool
as_job_manager_abort_job(as_job_manager* mgr, uint64_t trid)
{
	pthread_mutex_lock(&mgr->lock);

	as_job* _job = as_job_manager_find_active(mgr, trid);

	if (! _job) {
		pthread_mutex_unlock(&mgr->lock);
		return false;
	}

	pthread_mutex_lock(&_job->requeue_lock);
	_job->abandoned = AS_JOB_FAIL_USER_ABORT;
	bool found = as_priority_thread_pool_remove_task(&mgr->thread_pool, _job);
	pthread_mutex_unlock(&_job->requeue_lock);

	pthread_mutex_unlock(&mgr->lock);

	if (found) {
		as_job_active_release(_job);
	}

	return true;
}

int
as_job_manager_abort_all_jobs(as_job_manager* mgr)
{
	pthread_mutex_lock(&mgr->lock);

	int n_jobs = cf_queue_sz(mgr->active_jobs);

	if (n_jobs == 0) {
		pthread_mutex_unlock(&mgr->lock);
		return 0;
	}

	as_job* _jobs[n_jobs];
	info_item item = { _jobs };

	cf_queue_reduce(mgr->active_jobs, as_job_manager_info_cb, &item);

	bool found[n_jobs];

	for (int i = 0; i < n_jobs; i++) {
		as_job* _job = _jobs[i];

		pthread_mutex_lock(&_job->requeue_lock);
		_job->abandoned = AS_JOB_FAIL_USER_ABORT;
		found[i] = as_priority_thread_pool_remove_task(&mgr->thread_pool, _job);
		pthread_mutex_unlock(&_job->requeue_lock);
	}

	pthread_mutex_unlock(&mgr->lock);

	for (int i = 0; i < n_jobs; i++) {
		if (found[i]) {
			as_job_active_release(_jobs[i]);
		}
	}

	return n_jobs;
}

bool
as_job_manager_change_job_priority(as_job_manager* mgr, uint64_t trid,
		int priority)
{
	pthread_mutex_lock(&mgr->lock);

	as_job* _job = as_job_manager_find_active(mgr, trid);

	if (! _job) {
		pthread_mutex_unlock(&mgr->lock);
		return false;
	}

	pthread_mutex_lock(&_job->requeue_lock);
	_job->priority = safe_priority(priority);
	as_priority_thread_pool_change_task_priority(&mgr->thread_pool, _job,
			_job->priority);
	pthread_mutex_unlock(&_job->requeue_lock);

	pthread_mutex_unlock(&mgr->lock);
	return true;
}

void
as_job_manager_limit_active_jobs(as_job_manager* mgr, uint32_t max_active)
{
	mgr->max_active = max_active;
}

void
as_job_manager_limit_finished_jobs(as_job_manager* mgr, uint32_t max_done)
{
	pthread_mutex_lock(&mgr->lock);
	mgr->max_done = max_done;
	as_job_manager_evict_finished_jobs(mgr);
	pthread_mutex_unlock(&mgr->lock);
}

void
as_job_manager_resize_thread_pool(as_job_manager* mgr, uint32_t n_threads)
{
	as_priority_thread_pool_resize(&mgr->thread_pool, n_threads);
}

as_mon_jobstat*
as_job_manager_get_job_info(as_job_manager* mgr, uint64_t trid)
{
	pthread_mutex_lock(&mgr->lock);

	as_job* _job = as_job_manager_find_any(mgr, trid);

	if (! _job) {
		pthread_mutex_unlock(&mgr->lock);
		return NULL;
	}

	as_mon_jobstat* stat = cf_malloc(sizeof(as_mon_jobstat));

	memset(stat, 0, sizeof(as_mon_jobstat));
	as_job_info(_job, stat);

	pthread_mutex_unlock(&mgr->lock);
	return stat; // caller must free this
}

as_mon_jobstat*
as_job_manager_get_info(as_job_manager* mgr, int* size)
{
	*size = 0;

	pthread_mutex_lock(&mgr->lock);

	int n_jobs = cf_queue_sz(mgr->active_jobs) +
				 cf_queue_sz(mgr->finished_jobs);

	if (n_jobs == 0) {
		pthread_mutex_unlock(&mgr->lock);
		return NULL;
	}

	as_job* _jobs[n_jobs];
	info_item item = { _jobs };

	cf_queue_reduce_reverse(mgr->active_jobs, as_job_manager_info_cb, &item);
	cf_queue_reduce_reverse(mgr->finished_jobs, as_job_manager_info_cb, &item);

	size_t stats_size = sizeof(as_mon_jobstat) * n_jobs;
	as_mon_jobstat* stats = cf_malloc(stats_size);

	memset(stats, 0, stats_size);

	for (int i = 0; i < n_jobs; i++) {
		as_job_info(_jobs[i], &stats[i]);
	}

	pthread_mutex_unlock(&mgr->lock);

	*size = n_jobs;
	return stats; // caller must free this
}

int
as_job_manager_get_active_job_count(as_job_manager* mgr)
{
	pthread_mutex_lock(&mgr->lock);
	int n_jobs = cf_queue_sz(mgr->active_jobs);
	pthread_mutex_unlock(&mgr->lock);

	return n_jobs;
}

//----------------------------------------------------------
// as_job_manager utilities.
//

void
as_job_manager_evict_finished_jobs(as_job_manager* mgr)
{
	int max_allowed = (int)mgr->max_done;

	while (cf_queue_sz(mgr->finished_jobs) > max_allowed) {
		as_job* _job;

		cf_queue_pop(mgr->finished_jobs, &_job, 0);
		as_job_destroy(_job);
	}
}

int
as_job_manager_find_cb(void* buf, void* udata)
{
	as_job* _job = *(as_job**)buf;
	find_item* match = (find_item*)udata;

	if (match->trid == _job->trid) {
		match->_job = _job;
		return match->remove ? -2 : -1;
	}

	return 0;
}

as_job*
as_job_manager_find_job(cf_queue* jobs, uint64_t trid, bool remove)
{
	find_item item = { trid, NULL, remove };

	cf_queue_reduce(jobs, as_job_manager_find_cb, &item);

	return item._job;
}

static inline as_job*
as_job_manager_find_any(as_job_manager* mgr, uint64_t trid)
{
	as_job* _job = as_job_manager_find_job(mgr->active_jobs, trid, false);

	if (! _job) {
		_job = as_job_manager_find_job(mgr->finished_jobs, trid, false);
	}

	return _job;
}

static inline as_job*
as_job_manager_find_active(as_job_manager* mgr, uint64_t trid)
{
	return as_job_manager_find_job(mgr->active_jobs, trid, false);
}

static inline as_job*
as_job_manager_remove_active(as_job_manager* mgr, uint64_t trid)
{
	return as_job_manager_find_job(mgr->active_jobs, trid, true);
}

int
as_job_manager_info_cb(void* buf, void* udata)
{
	as_job* _job = *(as_job**)buf;
	info_item* item = (info_item*)udata;

	*item->p_job++ = _job;

	return 0;
}
