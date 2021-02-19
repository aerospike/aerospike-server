/*
 * scan_job.c
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

#include "base/scan_job.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/monitor.h"
#include "base/scan_manager.h"
#include "fabric/partition.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define SLEEP_MIN 1000L // sleep at least 1 ms
#define SLEEP_CAP (1000L * 10) // don't sleep more than 10 ms
#define STREAK_MAX (1000L * 200) // spawn up to one thread per 200 ms


//==========================================================
// Globals.
//

static uint32_t g_scan_job_trid = 0;


//==========================================================
// Forward declarations.
//

static uint32_t throttle_sleep(as_scan_job* _job, uint64_t count, uint64_t now);


//==========================================================
// Inlines & macros.
//

static inline void
count_pids_requested(as_scan_job* _job)
{
	if (_job->pids != NULL) {
		for (uint32_t i = 0; i < AS_PARTITIONS; i++) {
			if (_job->pids[i].requested) {
				_job->n_pids_requested++;
			}
		}
	}
}

static inline uint64_t
scan_job_trid(uint64_t trid)
{
	return trid != 0 ? trid : (uint64_t)as_aaf_uint32(&g_scan_job_trid, 1);
}

static inline float
progress_pct(as_scan_job* _job)
{
	uint32_t pid = _job->pid + 1;

	if (pid > AS_PARTITIONS) {
		pid = AS_PARTITIONS;
	}

	return ((float)(pid * 100)) / (float)AS_PARTITIONS;
}

static inline const char*
result_str(int result)
{
	switch (result) {
	case 0:
		return "ok";
	case AS_SCAN_ERR_UNKNOWN:
		return "abandoned-unknown";
	case AS_SCAN_ERR_CLUSTER_KEY:
		return "abandoned-cluster-key";
	case AS_SCAN_ERR_USER_ABORT:
		return "user-aborted";
	case AS_SCAN_ERR_RESPONSE_ERROR:
		return "abandoned-response-error";
	case AS_SCAN_ERR_RESPONSE_TIMEOUT:
		return "abandoned-response-timeout";
	default:
		return "abandoned-?";
	}
}


//==========================================================
// Public API.
//

void
as_scan_job_init(as_scan_job* _job, const as_scan_vtable* vtable, uint64_t trid,
		as_namespace* ns, const char* set_name, uint16_t set_id,
		as_scan_pid* pids, uint32_t rps, const char* client)
{
	*_job = (as_scan_job){
			.vtable = *vtable,
			.trid = scan_job_trid(trid),
			.ns = ns,
			.set_id = set_id,
			.pids = pids,
			.rps = rps
	};

	strcpy(_job->set_name, set_name);
	strcpy(_job->client, client);
	count_pids_requested(_job);
}

void
as_scan_job_run(as_scan_job* _job)
{
	cf_detail(AS_SCAN, "running thread for trid %lu", _job->trid);

	if (! _job->started) {
		_job->base_sys_tid = cf_thread_sys_tid();
		_job->started = true;

		if (_job->rps == 0) {
			as_scan_manager_add_max_job_threads(_job);
		}
	}

	uint32_t pid;

	while ((pid = as_faa_uint32(&_job->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;

		if (_job->pids == NULL) {
			if (as_partition_reserve_write(_job->ns, pid, &rsv, NULL) != 0) {
				continue;
			}
		}
		else {
			if (! _job->pids[pid].requested) {
				continue;
			}

			if (as_partition_reserve_full(_job->ns, pid, &rsv) != 0) {
				// Null tree causes slice_fn to send partition-done error.
				rsv = (as_partition_reservation){
						.ns = _job->ns,
						.p = &_job->ns->partitions[pid]
				};

				_job->vtable.slice_fn(_job, &rsv);
				continue;
			}
		}

		_job->vtable.slice_fn(_job, &rsv);
		as_partition_release(&rsv);

		if (cf_thread_sys_tid() != _job->base_sys_tid &&
				(_job->n_threads > _job->ns->n_single_scan_threads ||
						g_n_threads > g_config.n_scan_threads_limit)) {
			break;
		}
	}

	cf_detail(AS_SCAN, "finished thread for trid %lu", _job->trid);

	as_decr_uint32(&g_n_threads);

	int32_t n = (int32_t)as_aaf_uint32(&_job->n_threads, -1);

	cf_assert(n >= 0, AS_SCAN, "scan job thread underflow %d", n);

	if (n == 0) {
		_job->vtable.finish_fn(_job);
		as_scan_manager_finish_job(_job);
	}
}

uint32_t
as_scan_job_throttle(as_scan_job* _job)
{
	uint64_t count = as_aaf_uint64(&_job->n_throttled, 1);
	uint64_t now = cf_getus();

	if (cf_thread_sys_tid() != _job->base_sys_tid) {
		return _job->rps == 0 ? 0 : throttle_sleep(_job, count, now);
	}
	// else - only base thread adds extra threads.

	if (_job->rps == 0) {
		if (_job->pid < AS_PARTITIONS - 1) {
			// Don't re-add threads that drop near the end.
			as_scan_manager_add_max_job_threads(_job);
		}

		return 0;
	}

	uint32_t sleep_us =  throttle_sleep(_job, count, now);

	if (sleep_us != 0) {
		return sleep_us;
	}
	// else - we're lagging, add threads.

	if (_job->pid >= AS_PARTITIONS - 1) {
		// Threads (and RPS) dropping near the end may fake us into adding more
		// threads that wouldn't get a partition and would exit immediately.
		return 0;
	}

	if (_job->streak_us == 0) {
		_job->streak_us = now;
	}
	else if (now - _job->streak_us > STREAK_MAX) {
		_job->streak_us = 0;

		_job->base_us = now;
		_job->base_count = count;

		as_scan_manager_add_job_thread(_job);
	}

	return 0;
}

void
as_scan_job_destroy(as_scan_job* _job)
{
	_job->vtable.destroy_fn(_job);

	if (_job->pids) {
		cf_free(_job->pids);
	}

	cf_free(_job);
}

void
as_scan_job_info(as_scan_job* _job, as_mon_jobstat* stat)
{
	uint64_t now = cf_getus();
	bool done = _job->finish_us != 0;
	uint64_t since_start_us = now - _job->start_us;
	uint64_t since_finish_us = done ? now - _job->finish_us : 0;
	uint64_t active_us = done ?
			_job->finish_us - _job->start_us : since_start_us;

	stat->trid = _job->trid;
	stat->n_pids_requested = _job->n_pids_requested;
	stat->rps = _job->rps;
	stat->active_threads = _job->n_threads;
	stat->progress_pct = progress_pct(_job);
	stat->run_time = active_us / 1000;
	stat->time_since_done = since_finish_us / 1000;

	stat->recs_throttled = _job->n_throttled;
	stat->recs_filtered_meta = _job->n_filtered_meta;
	stat->recs_filtered_bins = _job->n_filtered_bins;
	stat->recs_succeeded = _job->n_succeeded;
	stat->recs_failed = _job->n_failed;

	strcpy(stat->ns, _job->ns->name);
	strcpy(stat->set, _job->set_name);

	strcpy(stat->client, _job->client);

	// TODO - if we fix the monitor to not use colons as separators, remove:
	char* escape = stat->client;

	while (*escape != 0) {
		if (*escape == ':') {
			*escape = '+';
		}

		escape++;
	}

	sprintf(stat->status, "%s(%s)", done ? "done" : "active",
			result_str(_job->abandoned));

	_job->vtable.info_mon_fn(_job, stat);
}


//==========================================================
// Local helpers.
//

static uint32_t
throttle_sleep(as_scan_job* _job, uint64_t count, uint64_t now)
{
	uint64_t target_us = ((count - _job->base_count) * 1000000) / _job->rps;
	int64_t sleep_us = (int64_t)(target_us - (now - _job->base_us));

	if (sleep_us > SLEEP_MIN) {
		_job->streak_us = 0;
		return (uint32_t)(sleep_us > SLEEP_CAP ? SLEEP_CAP : sleep_us);
	}

	return 0;
}
