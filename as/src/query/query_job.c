/*
 * query_job.c
 *
 * Copyright (C) 2022 Aerospike, Inc.
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

#include "query/query_job.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "hist.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "query/query_manager.h"

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

static uint32_t g_query_job_trid = 0;


//==========================================================
// Forward declarations.
//

static void finish(as_query_job* _job);
static void range_free(as_query_range* range);
static uint32_t throttle_sleep(as_query_job* _job, uint64_t count, uint64_t now);


//==========================================================
// Inlines & macros.
//

static inline uint64_t
query_job_trid(uint64_t trid)
{
	return trid != 0 ? trid : (uint64_t)as_aaf_uint32(&g_query_job_trid, 1);
}

static inline float
progress_pct(as_query_job* _job)
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
	case AS_ERR_UNKNOWN:
		return "abandoned-unknown";
	case AS_ERR_QUERY_ABORT:
		return "user-aborted";
	case AS_QUERY_RESPONSE_ERROR:
		return "abandoned-response-error";
	case AS_QUERY_RESPONSE_TIMEOUT:
		return "abandoned-response-timeout";
	default:
		return "abandoned-?";
	}
}


//==========================================================
// Public API.
//

void
as_query_job_init(as_query_job* _job, const as_query_vtable* vtable,
		const as_transaction* tr, as_namespace* ns)
{
	_job->vtable = *vtable;
	_job->trid = query_job_trid(as_transaction_trid(tr));
	_job->ns = ns;
	_job->start_ns = tr->start_time;

	strcpy(_job->client, tr->from.proto_fd_h->client);
}

void*
as_query_job_run(void* pv_job)
{
	as_query_job* _job = (as_query_job*)pv_job;

	if (! _job->is_short && ! _job->started) {
		_job->base_sys_tid = cf_thread_sys_tid();
		_job->started = true;

		if (_job->rps == 0) {
			as_query_manager_add_max_job_threads(_job);
		}
	}

	cf_buf_builder* bb = NULL;
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

				_job->vtable.slice_fn(_job, &rsv, &bb);
				continue;
			}
		}

		_job->vtable.slice_fn(_job, &rsv, &bb);
		as_partition_release(&rsv);

		if (! _job->is_short && cf_thread_sys_tid() != _job->base_sys_tid &&
				(_job->n_threads > _job->ns->n_single_query_threads ||
						g_n_query_threads > g_config.n_query_threads_limit)) {
			break;
		}
	}

	if (bb != NULL) {
		_job->vtable.slice_fn(_job, NULL, &bb);
		cf_buf_builder_free(bb);
	}

	as_decr_uint32(&g_n_query_threads);

	int32_t n = (int32_t)as_aaf_uint32_rls(&_job->n_threads, -1);

	cf_assert(n >= 0, AS_QUERY, "query job thread underflow %d", n);

	if (n == 0) {
		// Subsequent finish/destroy may require an 'acquire' barrier.
		as_fence_acq();

		finish(_job);

		if (_job->is_short) {
			as_query_job_destroy(_job);
		}
		else {
			as_query_manager_finish_job(_job);
		}
	}

	return NULL;
}

uint32_t
as_query_job_throttle(as_query_job* _job)
{
	uint64_t count;
	uint64_t now;

	if (cf_thread_sys_tid() != _job->base_sys_tid) {
		if (_job->rps == 0) {
			return 0;
		}

		count = as_aaf_uint64(&_job->n_throttled, 1);
		now = cf_getus();

		return throttle_sleep(_job, count, now);
	}
	// else - only base thread adds extra threads.

	if (_job->rps == 0) {
		if (_job->pid < AS_PARTITIONS - 1) {
			// Don't re-add threads that drop near the end.
			as_query_manager_add_max_job_threads(_job);
		}

		return 0;
	}

	count = as_aaf_uint64(&_job->n_throttled, 1);
	now = cf_getus();

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

		as_query_manager_add_job_thread(_job);
	}

	return 0;
}

void
as_query_job_destroy(as_query_job* _job)
{
	_job->vtable.destroy_fn(_job);

	if (_job->rps_udata != NULL) {
		as_security_done_rps(_job->rps_udata, _job->rps, true);
	}

	if (_job->pids != NULL) {
		cf_free(_job->pids);
	}

	if (_job->si != NULL) {
		as_sindex_release(_job->si);
	}

	if (_job->range != NULL) {
		range_free(_job->range);
	}

	cf_free(_job);
}

void
as_query_job_info(as_query_job* _job, cf_dyn_buf* db)
{
	uint64_t now = cf_getns();
	bool done = _job->finish_ns != 0;
	uint64_t since_start_ns = now - _job->start_ns;
	uint64_t since_finish_ns = done ? now - _job->finish_ns : 0;
	uint64_t active_ns = done ?
			_job->finish_ns - _job->start_ns : since_start_ns;

	cf_dyn_buf_append_string(db, "trid=");
	cf_dyn_buf_append_uint64(db, _job->trid);

	cf_dyn_buf_append_string(db, ":ns=");
	cf_dyn_buf_append_string(db, _job->ns->name);

	if (_job->set_name[0]) {
		cf_dyn_buf_append_string(db, ":set=");
		cf_dyn_buf_append_string(db, _job->set_name);
	}

	if (_job->si_name[0]) {
		cf_dyn_buf_append_string(db, ":sindex-name=");
		cf_dyn_buf_append_string(db, _job->si_name);
	}

	if (_job->n_pids_requested != 0) {
		cf_dyn_buf_append_string(db, ":n-pids-requested=");
		cf_dyn_buf_append_uint32(db, _job->n_pids_requested);
	}

	cf_dyn_buf_append_string(db, ":rps=");
	cf_dyn_buf_append_uint32(db, _job->rps);

	cf_dyn_buf_append_string(db, ":active-threads=");
	cf_dyn_buf_append_uint32(db, _job->n_threads);

	cf_dyn_buf_append_string(db, ":status=");
	cf_dyn_buf_append_format(db, "%s(%s)", done ? "done" : "active",
			result_str(_job->abandoned));

	cf_dyn_buf_append_string(db, ":job-progress=");
	cf_dyn_buf_append_format(db, "%.2f", progress_pct(_job));

	cf_dyn_buf_append_string(db, ":run-time=");
	cf_dyn_buf_append_uint64(db, active_ns / 1000000);

	cf_dyn_buf_append_string(db, ":time-since-done=");
	cf_dyn_buf_append_uint64(db, since_finish_ns / 1000000);

	cf_dyn_buf_append_string(db, ":recs-throttled=");
	cf_dyn_buf_append_uint64(db, _job->n_throttled);

	cf_dyn_buf_append_string(db, ":recs-filtered-meta=");
	cf_dyn_buf_append_uint64(db, _job->n_filtered_meta);

	cf_dyn_buf_append_string(db, ":recs-filtered-bins=");
	cf_dyn_buf_append_uint64(db, _job->n_filtered_bins);

	cf_dyn_buf_append_string(db, ":recs-succeeded=");
	cf_dyn_buf_append_uint64(db, _job->n_succeeded);

	cf_dyn_buf_append_string(db, ":recs-failed=");
	cf_dyn_buf_append_uint64(db, _job->n_failed);

	char client[64];

	strcpy(client, _job->client);

	// TODO - if we fix the monitor to not use colons as separators, remove:
	char* escape = client;

	while (*escape != 0) {
		if (*escape == ':') {
			*escape = '+';
		}

		escape++;
	}

	cf_dyn_buf_append_string(db, ":from=");
	cf_dyn_buf_append_string(db, client);

	_job->vtable.info_mon_fn(_job, db);
}


//==========================================================
// Local helpers.
//

static void
finish(as_query_job* _job)
{
	_job->vtable.finish_fn(_job);

	if (_job->rps_udata != NULL) {
		as_security_done_rps(_job->rps_udata, _job->rps, false);
		_job->rps_udata = NULL;
	}

	if (_job->si != NULL) {
		as_sindex_release(_job->si);
		_job->si = NULL;
	}
}

// So that calloc'ed but not fully initialized range is freed correctly.
COMPILER_ASSERT(AS_PARTICLE_TYPE_GEOJSON != 0);

static void
range_free(as_query_range* range)
{
	if (range->bin_type == AS_PARTICLE_TYPE_GEOJSON) {
		cf_free(range->u.geo.r);

		if (range->u.geo.region) {
			geo_region_destroy(range->u.geo.region);
		}
	}

	if (range->ctx_buf != NULL) {
		cf_free(range->ctx_buf);
	}

	cf_free(range);
}

static uint32_t
throttle_sleep(as_query_job* _job, uint64_t count, uint64_t now)
{
	uint64_t target_us = ((count - _job->base_count) * 1000000) / _job->rps;
	int64_t sleep_us = (int64_t)(target_us - (now - _job->base_us));

	if (sleep_us > SLEEP_MIN) {
		_job->streak_us = 0;
		return (uint32_t)(sleep_us > SLEEP_CAP ? SLEEP_CAP : sleep_us);
	}

	return 0;
}
