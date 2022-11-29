/*
 * query_job.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

#include "dynbuf.h"

#include "base/datamodel.h"
#include "sindex/sindex.h"


//==========================================================
// Forward declarations.
//

struct as_namespace_s;
struct as_partition_reservation_s;
struct as_query_job_s;
struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

// These result codes can't make it back to the client, but show in monitor:
#define AS_QUERY_RESPONSE_ERROR   (-1)
#define AS_QUERY_RESPONSE_TIMEOUT (-2)

typedef struct as_query_range_start_end_s {
	int64_t start;
	int64_t end;  // -1 means infinity
} as_query_range_start_end;

typedef struct as_query_geo_range_s {
	uint64_t cellid;  // target of regions-containing-point query
	geo_region_t region;  // target of points-in-region query
	as_query_range_start_end* r;
	uint8_t num_r;
} as_query_geo_range;

typedef struct as_query_range_s {
	union {
		as_query_range_start_end r;
		as_query_geo_range geo;
	} u;

	uint32_t str_len;
	char str_stub[16];

	uint16_t bin_id;
	as_particle_type bin_type;
	as_sindex_type itype;
	bool isrange;
	bool de_dup;
	char bin_name[AS_BIN_NAME_MAX_SZ];
	uint8_t* ctx_buf;
	uint32_t ctx_buf_sz;
} as_query_range;

typedef void (*as_query_slice_fn)(struct as_query_job_s* _job, struct as_partition_reservation_s* rsv, cf_buf_builder** bb_r);
typedef void (*as_query_finish_fn)(struct as_query_job_s* _job);
typedef void (*as_query_destroy_fn)(struct as_query_job_s* _job);
typedef void (*as_query_info_fn)(struct as_query_job_s* _job, cf_dyn_buf* db);

typedef struct as_query_vtable_s {
	as_query_slice_fn slice_fn;
	as_query_finish_fn finish_fn;
	as_query_destroy_fn destroy_fn;
	as_query_info_fn info_mon_fn;
} as_query_vtable;

typedef struct as_query_pid_s {
	bool requested;
	bool has_resume;
	cf_digest keyd;
	int64_t bval;
} as_query_pid;

typedef struct as_query_job_s {
	// Mandatory interface for derived classes:
	as_query_vtable vtable;

	// Unique identifier:
	uint64_t trid;

	// Job scope:
	struct as_namespace_s* ns;
	struct as_sindex_s* si;
	struct as_query_range_s* range;
	char si_name[INAME_MAX_SZ];
	char set_name[AS_SET_NAME_MAX_SIZE];
	uint16_t set_id;

	// Partition scope:
	as_query_pid* pids;

	// Query threading model:
	bool is_short;

	// Handle active phase:
	uint32_t n_threads;
	uint32_t pid;
	uint64_t start_ms_clepoch;
	volatile int abandoned;

	// For throttling:
	uint32_t rps;
	bool started;
	pid_t base_sys_tid;
	uint64_t base_us;
	uint64_t base_count;
	uint64_t streak_us;

	// For rate limits:
	void* rps_udata;

	// For tracking:
	char client[64];
	uint16_t n_pids_requested;
	uint64_t start_ns;
	uint64_t finish_ns;
	uint64_t n_throttled;
	uint64_t n_filtered_meta;
	uint64_t n_filtered_bins;
	uint64_t n_succeeded;
	uint64_t n_failed;
} as_query_job;


//==========================================================
// Public API.
//

void as_query_job_init(as_query_job* _job, const as_query_vtable* vtable, const struct as_transaction_s* tr, struct as_namespace_s* ns);
void* as_query_job_run(void* pv_job);
uint32_t as_query_job_throttle(as_query_job* _job);
void as_query_job_destroy(as_query_job* _job);
void as_query_job_info(as_query_job* _job, cf_dyn_buf* db);
