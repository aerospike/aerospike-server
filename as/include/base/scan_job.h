/*
 * scan_job.h
 *
 * Copyright (C) 2019 Aerospike, Inc.
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

#include "base/proto.h"


//==========================================================
// Forward declarations.
//

struct as_mon_jobstat_s;
struct as_namespace_s;
struct as_partition_reservation_s;
struct as_scan_job_s;


//==========================================================
// Typedefs & constants.
//

// Same as proto result codes so connected scans don't have to convert:
#define AS_SCAN_ERR_UNKNOWN     AS_ERR_UNKNOWN
#define AS_SCAN_ERR_PARAMETER   AS_ERR_PARAMETER
#define AS_SCAN_ERR_CLUSTER_KEY AS_ERR_CLUSTER_KEY_MISMATCH
#define AS_SCAN_ERR_USER_ABORT  AS_ERR_SCAN_ABORT
#define AS_SCAN_ERR_FORBIDDEN   AS_ERR_FORBIDDEN

// These result codes can't make it back to the client, but show in monitor:
#define AS_SCAN_ERR_RESPONSE_ERROR      (-1)
#define AS_SCAN_ERR_RESPONSE_TIMEOUT    (-2)

typedef void (*as_scan_slice_fn)(struct as_scan_job_s* _job, struct as_partition_reservation_s* rsv);
typedef void (*as_scan_finish_fn)(struct as_scan_job_s* _job);
typedef void (*as_scan_destroy_fn)(struct as_scan_job_s* _job);
typedef void (*as_scan_info_fn)(struct as_scan_job_s* _job, struct as_mon_jobstat_s* stat);

typedef struct as_scan_vtable_s {
	as_scan_slice_fn slice_fn;
	as_scan_finish_fn finish_fn;
	as_scan_destroy_fn destroy_fn;
	as_scan_info_fn info_mon_fn;
} as_scan_vtable;

typedef struct as_scan_job_s {
	// Mandatory interface for derived classes:
	as_scan_vtable vtable;

	// Unique identifier:
	uint64_t trid;

	// Job scope:
	struct as_namespace_s* ns;
	uint16_t set_id;

	// Handle active phase:
	uint32_t n_threads;
	uint32_t pid;
	volatile int abandoned;

	// For throttling:
	uint32_t rps;
	bool started;
	pid_t base_sys_tid;
	uint64_t base_us;
	uint64_t base_count;
	uint64_t streak_us;

	// For tracking:
	char client[64];
	uint64_t start_us;
	uint64_t finish_us;
	uint64_t n_throttled;
	uint64_t n_filtered_meta;
	uint64_t n_filtered_bins;
	uint64_t n_succeeded;
	uint64_t n_failed;
} as_scan_job;


//==========================================================
// Public API.
//

void as_scan_job_init(as_scan_job* _job, const as_scan_vtable* vtable, uint64_t trid, struct as_namespace_s* ns, uint16_t set_id, uint32_t rps, const char* client);
void as_scan_job_add_thread(as_scan_job* _job);
uint32_t as_scan_job_throttle(as_scan_job* _job);
void as_scan_job_destroy(as_scan_job* _job);
void as_scan_job_info(as_scan_job* _job, struct as_mon_jobstat_s* stat);
