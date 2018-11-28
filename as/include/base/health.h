/*
 * health.h
 *
 * Copyright (C) 2018 Aerospike, Inc.
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

#include "dynbuf.h"
#include "node.h"


//==========================================================
// Typedefs & constants.
//

#define AS_HEALTH_SAMPLING_MASK 0xF // 1 out of 16 ops (6.25%)

typedef enum {
	AS_HEALTH_LOCAL_DEVICE_READ_LAT,
	AS_HEALTH_LOCAL_TYPE_MAX
} as_health_local_stat_type;

typedef enum {
	AS_HEALTH_NODE_FABRIC_FDS,
	AS_HEALTH_NODE_PROXIES,
	AS_HEALTH_NODE_ARRIVALS,
	AS_HEALTH_NODE_TYPE_MAX
} as_health_node_stat_type;

typedef enum {
	AS_HEALTH_NS_REPL_LAT,
	AS_HEALTH_NS_TYPE_MAX
} as_health_ns_stat_type;


//==========================================================
// Globals.
//

extern bool g_health_enabled;

extern __thread uint64_t g_device_read_counter;
extern __thread uint64_t g_replica_write_counter;


//==========================================================
// Public API.
//

void as_health_get_outliers(cf_dyn_buf* db);
void as_health_get_stats(cf_dyn_buf* db);
void as_health_start();

// Not called directly - called by inline wrappers below.
void health_add_device_latency(uint32_t ns_id, uint32_t d_id, uint64_t start_us);
void health_add_node_counter(cf_node node, as_health_node_stat_type stat_type);
void health_add_ns_latency(cf_node node, uint32_t ns_id, as_health_ns_stat_type stat_type, uint64_t start_us);

static inline bool
as_health_sample_device_read()
{
	return g_health_enabled &&
			(g_device_read_counter++ & AS_HEALTH_SAMPLING_MASK) == 0;
}

static inline bool
as_health_sample_replica_write()
{
	return g_health_enabled &&
			(g_replica_write_counter++ & AS_HEALTH_SAMPLING_MASK) == 0;
}

static inline void
as_health_add_device_latency(uint32_t ns_id, uint32_t d_id, uint64_t start_us)
{
	if (g_health_enabled && start_us != 0) {
		health_add_device_latency(ns_id, d_id, start_us);
	}
}

static inline void
as_health_add_node_counter(cf_node node, as_health_node_stat_type type)
{
	if (g_health_enabled) {
		health_add_node_counter(node, type);
	}
}

static inline void
as_health_add_ns_latency(cf_node node, uint32_t ns_id,
		as_health_ns_stat_type type, uint64_t start_us)
{
	if (g_health_enabled && start_us != 0) {
		health_add_ns_latency(node, ns_id, type, start_us);
	}
}
