/*
 * skew_monitor.c
 *
 * Copyright (C) 2012-2017 Aerospike, Inc.
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

#include "fabric/skew_monitor.h"

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>

#include "citrusleaf/alloc.h"

#include "msg.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "fabric/clustering.h"
#include "fabric/exchange.h"
#include "fabric/hb.h"

/*
 * Overview
 * ========
 * Monitors skew across nodes in a cluster to allow other modules to handle skew
 * beyond tolerances. For example CP namespaces block transctions on skew beyond
 * tolerable limits.
 *
 * Principle of skew monitoring
 * ============================
 * The hlc clock forms a pretty close upper bound on the physical clocks for
 * adjacent nodes within the bounds of network trip time.
 *
 * Lets call the difference between a node's physical component of hlc time and
 * physical time at the same instant as its hlc_delta.
 * The premise is that the difference between the min hlc_delta and max
 * hlc_delta observed for adjacent nodes closely follows the  maximum clock skew
 * in the cluster.
 *
 * The clock skew monitor adds a physical timestamp field to each heartbeat
 * pulse message.
 * For a peer node on receipt of a heartbeat pulse, hlc_delta is computed as
 * 	hlc_delta = physical-component(pulse-hlc) - pulse-timestamp
 *
 * We maintain a exponential moving average of the hlc_delta to buffer against
 * small fluctuations
 * 	avg_hlc_delta = (ALPHA)(hlc_delta) + (1-ALPHA)(avg_hlc_delta)
 *
 * where ALPHA is set to weigh current values more over older values.
 *
 * Cluster wide clock ckew is updated at periodic intervals. A low water mark
 * breach of the skew generates warnings and a high water mark breach causes
 * (TODO: ????).
 *
 * Design
 * =======
 * The monitor is ticks on heartbeat message sends without requiring an
 * additional thread. This is alright as heartbeat pulse messages are the
 * vehicle used for skew detection. The amount of computation amortized across
 * sent heartbeat pulse messages is minimal and should be maintained so.
 */

/*
 * ----------------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------------
 */

/**
 * Weightage of current clock delta over current moving average. For now weigh
 * recent values heavily over older values.
 */
#define ALPHA (0.65)

/*
 * ----------------------------------------------------------------------------
 * Logging
 * ----------------------------------------------------------------------------
 */
#define CRASH(format, ...) cf_crash(AS_SKEW, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_SKEW, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_SKEW, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_SKEW, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_SKEW, format, ##__VA_ARGS__)

/*
 * ----------------------------------------------------------------------------
 * Skew monitor data structures
 * ----------------------------------------------------------------------------
 */

/**
 * A struct to hold and its skew related information.
 */
typedef struct as_skew_monitor_node_skew_data_s
{
	cf_node nodeid;
	int64_t delta;
} as_skew_monitor_node_skew_data;

/**
 * HB plugin data iterate to get node hlc deltas.
 */
typedef struct as_skew_monitor_hlc_delta_udata_s
{
	int num_nodes;
	as_skew_monitor_node_skew_data skew_data[AS_CLUSTER_SZ];
} as_skew_monitor_hlc_delta_udata;

/*
 * ----------------------------------------------------------------------------
 * External protected API for skew monitor
 * ----------------------------------------------------------------------------
 */
extern int
as_hb_msg_send_hlc_ts_get(msg* msg, as_hlc_timestamp* send_ts);

/*
 * ----------------------------------------------------------------------------
 * Globals
 * ----------------------------------------------------------------------------
 */

/**
 * Last time skew was checked.
 */
cf_atomic64 g_last_skew_check_time = 0;

/**
 * Current value of clock skew.
 */
cf_atomic64 g_skew = 0;

/**
 * Moving average of the clock skew for self node.
 */
volatile int64_t g_self_skew_avg = 0;

/*
 * ----------------------------------------------------------------------------
 * Skew intervals and limits
 * ----------------------------------------------------------------------------
 */

/**
 * Interval at which skew checks should be made.
 */
static uint64_t
skew_check_interval()
{
	return MIN(2000, as_clustering_quantum_interval() / 2);
}

/**
 * Threshold for outlier detection. Skew values less than this threshold will
 * not invoke outlier detection.
 */
static uint64_t
skew_monitor_outlier_detection_threshold()
{
	return as_clustering_quantum_interval();
}

/*
 * ----------------------------------------------------------------------------
 * HLC delta related
 * ----------------------------------------------------------------------------
 */

/**
 * Find min and max skew using difference between physical clock and hlc.
 */
static void
skew_monitor_delta_collect_iterate(cf_node nodeid, void* plugin_data,
		size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	int64_t delta = 0;
	as_skew_monitor_hlc_delta_udata* deltas =
			(as_skew_monitor_hlc_delta_udata*)udata;

	if (!plugin_data || plugin_data_size < sizeof(uint64_t)) {
		// Assume missing nodes share the same delta as self.
		// Note: self node will not be in adjacency list and hence will also
		// follow same code path.
		delta = g_self_skew_avg;
	}
	else {
		delta = *(int64_t*)plugin_data;
	}

	int index = deltas->num_nodes;
	deltas->skew_data[index].delta = delta;
	deltas->skew_data[index].nodeid = nodeid;
	deltas->num_nodes++;
}

/**
 * Compute the skew across the cluster.
 */
static uint64_t
skew_monitor_compute_skew()
{
	uint64_t skew = 0;
	uint8_t buffer[AS_CLUSTER_SZ * sizeof(cf_node)];
	cf_vector succession = { 0 };

	cf_vector_init_smalloc(&succession, sizeof(cf_node), buffer, sizeof(buffer),
			VECTOR_FLAG_INITZERO);
	as_exchange_succession(&succession);

	if (cf_vector_size(&succession) <= 1) {
		// Self node is an orphan or single node cluster. No cluster wide skew.
		skew = 0;
		goto Cleanup;
	}

	as_skew_monitor_hlc_delta_udata udata = { 0 };
	as_hb_plugin_data_iterate(&succession, AS_HB_PLUGIN_SKEW_MONITOR,
			skew_monitor_delta_collect_iterate, &udata);

	int64_t min = INT64_MAX;
	int64_t max = INT64_MIN;

	for (int i = 0; i < udata.num_nodes; i++) {
		int64_t delta = udata.skew_data[i].delta;
		if (delta < min) {
			min = delta;
		}

		if (delta > max) {
			max = delta;
		}
	}
	skew = max - min;

Cleanup:
	cf_vector_destroy(&succession);
	return skew;
}

/**
 * Update clock skew and fire skew events.
 */
static void
skew_monitor_update()
{
	cf_clock now = cf_getms();
	cf_atomic64_set(&g_last_skew_check_time, now);

	uint64_t skew = skew_monitor_compute_skew();
	uint64_t avg_skew = cf_atomic64_get(g_skew);
	avg_skew = ALPHA * skew + (1 - ALPHA) * avg_skew;
	cf_atomic64_set(&g_skew, avg_skew);

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];
		handle_clock_skew(ns, avg_skew);
	}
}

/*
 * ----------------------------------------------------------------------------
 * Outlier detection
 * ----------------------------------------------------------------------------
 */

/**
 * Comparator for deltas.
 */
static int
skew_monitor_hlc_delta_compare(const void* o1, const void* o2)
{
	int64_t delta1 = ((as_skew_monitor_node_skew_data*)o1)->delta;
	int64_t delta2 = ((as_skew_monitor_node_skew_data*)o2)->delta;

	return delta1 > delta2 ? 1 : (delta1 == delta2 ? 0 : -1);
}

/**
 * Compute the median of the data.
 * @param values the values sorted.
 * @param from the start index (inclusive)
 * @param to the end index (inclusive)
 * @return the index of the median element
 */
static int
skew_monitor_median_index(int from, int to)
{
	int numElements = to - from + 1;
	if (numElements < 0) {
		return from;
	}
	return (to + from) / 2;
}

/**
 * Return the currently estimated outliers from our cluster.
 * Outliers should have space to hold at least AS_CLUSTER_SZ nodes.
 */
static uint32_t
skew_monitor_outliers_from_skew_data(cf_vector* outliers,
		as_skew_monitor_hlc_delta_udata* udata)
{
	// Use inter-quartile distance to detect outliers.
	// Sort the deltas in ascending order.
	qsort(udata->skew_data, udata->num_nodes,
			sizeof(as_skew_monitor_node_skew_data),
			skew_monitor_hlc_delta_compare);
	int q2_index = skew_monitor_median_index(0, udata->num_nodes - 1);
	int q3_index = skew_monitor_median_index(q2_index, udata->num_nodes - 1);
	int q1_index = skew_monitor_median_index(0, q2_index);
	int64_t q3 = udata->skew_data[q3_index].delta;
	int64_t q1 = udata->skew_data[q1_index].delta;

	// Compute the inter quartile range. Lower bound iqr to network latency to
	// allow that allow some fuzziness with tigth clock grouping.
	int64_t iqr = MAX(q3 - q1, g_config.fabric_latency_max_ms);
	double lower_bound = q1 - 1.5 * iqr;
	double upper_bound = q3 + 1.5 * iqr;

	uint32_t num_outliers = 0;

	// Isolate outliers
	for (int i = 0; i < udata->num_nodes; i++) {
		if (udata->skew_data[i].delta < lower_bound
				|| udata->skew_data[i].delta > upper_bound) {
			if (outliers) {
				cf_vector_append(outliers, &udata->skew_data[i].nodeid);
			}

			num_outliers++;
		}
	}

	return num_outliers;
}

/**
 * Return the currently estimated outliers from our cluster.
 * Outliers should have space to hold at least AS_CLUSTER_SZ nodes.
 */
static uint32_t
skew_monitor_outliers(cf_vector* outliers)
{
	if (as_skew_monitor_skew() < skew_monitor_outlier_detection_threshold()) {
		// Skew is not significant. Skip printing outliers.
		return 0;
	}

	uint8_t buffer[AS_CLUSTER_SZ * sizeof(cf_node)];
	cf_vector succession;
	cf_vector_init_smalloc(&succession, sizeof(cf_node), buffer, sizeof(buffer),
			VECTOR_FLAG_INITZERO);
	as_exchange_succession(&succession);

	uint32_t num_outliers = 0;

	uint32_t cluster_size = cf_vector_size(&succession);
	if (cluster_size <= 1) {
		// Self node is an orphan or single node cluster. No cluster wide skew.
		goto Cleanup;
	}

	as_skew_monitor_hlc_delta_udata udata = { 0 };
	as_hb_plugin_data_iterate(&succession, AS_HB_PLUGIN_SKEW_MONITOR,
			skew_monitor_delta_collect_iterate, &udata);

	num_outliers = skew_monitor_outliers_from_skew_data(outliers, &udata);

Cleanup:
	cf_vector_destroy(&succession);

	return num_outliers;
}

/*
 * ----------------------------------------------------------------------------
 * HB plugin functions
 * ----------------------------------------------------------------------------
 */

/**
 * Push current timestamp for self node into the heartbeat pulse message.
 */
static void
skew_monitor_hb_plugin_set_fn(msg* msg)
{
	cf_clock send_ts = cf_clock_getabsolute();
	msg_set_uint64(msg, AS_HB_MSG_SKEW_MONITOR_DATA, send_ts);

	// Update self skew.
	as_hlc_timestamp send_hlc_ts = as_hlc_timestamp_now();
	int64_t clock_delta = as_hlc_physical_ts_get(send_hlc_ts) - send_ts;

	// Update the average delta for self.
	g_self_skew_avg = clock_delta * ALPHA + (1 - ALPHA) * (g_self_skew_avg);

	cf_clock now = cf_getms();
	if (cf_atomic64_get(g_last_skew_check_time) + skew_check_interval() < now) {
		skew_monitor_update();
	}
}

/**
 * Compare the HLC timestamp and the physical clock and store the difference as
 * plugin data for the source node to enable skew detection.
 */
static void
skew_monitor_hb_plugin_parse_data_fn(msg* msg, cf_node source,
		as_hb_plugin_node_data* plugin_data)
{
	cf_clock send_ts = 0;
	as_hlc_timestamp send_hlc_ts = 0;
	if (msg_get_uint64(msg, AS_HB_MSG_SKEW_MONITOR_DATA, &send_ts) != 0
			|| as_hb_msg_send_hlc_ts_get(msg, &send_hlc_ts) != 0) {
		// Pre CP mode node. For now assumes it shares the same delta with hlc
		// as us.
		send_hlc_ts = as_hlc_timestamp_now();
		send_ts = cf_clock_getabsolute();
	}

	size_t required_capacity = sizeof(int64_t);
	if (required_capacity > plugin_data->data_capacity) {
		plugin_data->data = cf_realloc(plugin_data->data, required_capacity);

		if (plugin_data->data == NULL) {
			CRASH(
					"error allocating space for storing succession list for node %"PRIx64,
					source);
		}
		plugin_data->data_capacity = required_capacity;
		memset(plugin_data->data, 0, required_capacity);
	}

	int64_t clock_delta = as_hlc_physical_ts_get(send_hlc_ts) - send_ts;
	int64_t* average_clock_delta = (int64_t*)plugin_data->data;

	if (plugin_data->data_size == 0) {
		// This is the first data point.
		*average_clock_delta = clock_delta;
	}

	plugin_data->data_size = required_capacity;

	// update the average
	*average_clock_delta = clock_delta * ALPHA
			+ (1 - ALPHA) * (*average_clock_delta);

	DETAIL("node %"PRIx64" hlc:%lu clock:%lu delta:%ld moving-average:%ld", source, send_hlc_ts, send_ts, clock_delta, *average_clock_delta);
}

/*
 * ----------------------------------------------------------------------------
 * Protceted API only mean for clustering.
 * ----------------------------------------------------------------------------
 */

/**
 * Update clock skew and fire skew events.
 */
void
as_skew_monitor_update()
{
	skew_monitor_update();
}

/*
 * ----------------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------------
 */

/**
 * Initialize skew monitor.
 */
void
as_skew_monitor_init()
{
	as_hb_plugin skew_monitor_plugin = { 0 };

	skew_monitor_plugin.id = AS_HB_PLUGIN_SKEW_MONITOR;
	skew_monitor_plugin.wire_size_fixed = sizeof(int64_t);
	// Size of the node in succession list.
	skew_monitor_plugin.wire_size_per_node = 0;
	skew_monitor_plugin.set_fn = skew_monitor_hb_plugin_set_fn;
	skew_monitor_plugin.parse_fn = skew_monitor_hb_plugin_parse_data_fn;
	as_hb_plugin_register(&skew_monitor_plugin);

	DETAIL("skew monitor initialized");
}

/**
 * Return the current estimate of the clock skew in the cluster.
 */
uint64_t
as_skew_monitor_skew()
{
	return cf_atomic64_get(g_skew);
}

/**
 * Return the currently estimated outliers from our cluster.
 * Outliers should have space to hold at least AS_CLUSTER_SZ nodes.
 */
uint32_t
as_skew_monitor_outliers(cf_vector* outliers)
{
	return skew_monitor_outliers(outliers);
}

/**
 * Print skew outliers to a dynamic buffer.
 */
uint32_t
as_skew_monitor_outliers_append(cf_dyn_buf* db)
{
	uint8_t buffer[AS_CLUSTER_SZ * sizeof(cf_node)];
	cf_vector outliers;
	cf_vector_init_smalloc(&outliers, sizeof(cf_node), buffer, sizeof(buffer),
			VECTOR_FLAG_INITZERO);
	uint32_t num_outliers = skew_monitor_outliers(&outliers);

	for (uint32_t i = 0; i < num_outliers; i++) {
		cf_node outlier_id;
		cf_vector_get(&outliers, i, &outlier_id);
		cf_dyn_buf_append_uint64_x(db, outlier_id);
		cf_dyn_buf_append_char(db, ',');
	}

	if (num_outliers) {
		cf_dyn_buf_chomp(db);
	}

	cf_vector_destroy(&outliers);

	return num_outliers;
}

/**
 * Print skew monitor info to a dynamic buffer.
 */
void
as_skew_monitor_info(cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, "cluster_clock_skew_outliers=");
	uint32_t num_outliers = as_skew_monitor_outliers_append(db);
	if (num_outliers == 0) {
		cf_dyn_buf_append_string(db, "null");
	}
	cf_dyn_buf_append_char(db, ';');
}

/**
 * Dump some debugging information to the logs.
 */
void
as_skew_monitor_dump()
{
	uint8_t buffer[AS_CLUSTER_SZ * sizeof(cf_node)];
	cf_vector node_vector;
	cf_vector_init_smalloc(&node_vector, sizeof(cf_node), buffer,
			sizeof(buffer), VECTOR_FLAG_INITZERO);
	as_exchange_succession(&node_vector);

	INFO("CSM: cluster-clock-skew:%ld", as_skew_monitor_skew());
	if (cf_vector_size(&node_vector) <= 1) {
		// Self node is an orphan or single node cluster. No cluster wide skew.
		goto Cleanup;
	}

	as_skew_monitor_hlc_delta_udata udata = { 0 };
	as_hb_plugin_data_iterate(&node_vector, AS_HB_PLUGIN_SKEW_MONITOR,
			skew_monitor_delta_collect_iterate, &udata);

	for (int i = 0; i < udata.num_nodes; i++) {
		INFO("CSM:    node:%"PRIx64" hlc-delta:%ld", udata.skew_data[i].nodeid, udata.skew_data[i].delta);
	}

	// Log the outliers.
	cf_vector_clear(&node_vector);
	skew_monitor_outliers(&node_vector);
	if (cf_vector_size(&node_vector)) {
		as_clustering_log_cf_node_vector(AS_INFO, AS_SKEW,
				"CSM: Estimated clock outliers", &node_vector);
	}

Cleanup:
	cf_vector_destroy(&node_vector);
}
