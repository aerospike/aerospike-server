/*
 * monitor.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 *  Long Running Job Monitoring interface
 *
 *  This file implements the generic interface for the long running jobs
 *  in Aerospike like query / scan / batch etc. The idea is to able to see
 *  what is going on in the system.
 *
 *  Each module which needs to show up in the monitoring needs to register
 *  and implement the interfaces.
 */

#pragma once

#include <stdint.h>

#include "dynbuf.h"

#include "base/datamodel.h"


#define AS_MON_OK 0
#define AS_MON_ERR -1
#define AS_MON_EXIST -2
#define TRID_LIST_SIZE 1000

typedef enum {
	QUERY_MOD	= 0,
	SCAN_MOD	= 1,
	SBLD_MOD	= 2
} as_mon_module_slot;

extern const char * AS_MON_MODULES[];

// Stat for currently running job
typedef struct as_mon_jobstat_s {
	uint64_t	trid;
	char		job_type[32];
	char		ns[AS_ID_NAMESPACE_SZ];
	char		set[AS_SET_NAME_MAX_SIZE];
	uint32_t	priority;
	char		status[64];
	float		progress_pct;
	uint64_t	run_time;
	uint64_t	time_since_done;
	uint64_t	recs_read;
	uint64_t	net_io_bytes;
	float		cpu;
	char		jdata[512];
} as_mon_jobstat;

typedef struct as_mon_cb_s {
	as_mon_jobstat *(*get_jobstat)		(uint64_t trid);
	as_mon_jobstat *(*get_jobstat_all)	(int * size);

	// Per transaction
	int (*set_priority)	(uint64_t trid, uint32_t priority);
	int (*kill)			(uint64_t trid);
	int (*suspend)		(uint64_t trid);

	// Per Module
	// Numer of pending transaction of this job type in queue allowed
	// incoming more than this will be rejected.
	int	(*set_pendingmax)	(int);

	// Set the number of transaction that can be inflight at
	// any point of time.
	int (*set_maxinflight)	(int);

	// Any individual transaction priority has upper bound of max
	// priority of jobtype
	int (*set_maxpriority)	(int);
} as_mon_cb;

// Structure to register module with as mon interface.
typedef struct as_mon_s {
	char 		*type;
	as_mon_cb	cb;
} as_mon;

void as_mon_info_cmd(const char *module, char *cmd, uint64_t trid, uint32_t priority, cf_dyn_buf *db);
int  as_mon_init();
