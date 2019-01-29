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
#include "base/nsup.h"
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
 * Maximum allowed deviation in HLC clocks. A peer node's HLC clock will be
 * considered bad if the difference between self HLC and peer's HLC exceeds this
 * value.
 *
 * This value allows a node that barely synchronizes HLC once per node timeout.
 */
#define HLC_DEVIATION_MAX_MS (as_hb_node_timeout_get())

/**
 * Max allowed streak of bad HLC clock readings for a peer node. During the
 * allowed streak,
 * the peer node will be assumed to have hlc delta same as self node. Limited
 * between 3 and 5.
 */
#define BAD_HLC_STREAK_MAX (MIN(5, MAX(3, as_hb_max_intervals_missed_get() / 2)))

/**
 * Ring buffer maximum capacity. The actual capacity is a function of the
 * heartbeat node timeout.
 */
#define RING_BUFFER_CAPACITY_MAX (100)

/**
 * Threshold for  (absolute deviation/median absolute deviation) beyond which
 * nodes are labelled outliers.
 */
#define MAD_RATIO_OUTLIER_THRESHOLD 2

/*
 * ----------------------------------------------------------------------------
 * Logging
 * ----------------------------------------------------------------------------
 */
#define CRASH(format, ...) cf_crash(AS_SKEW, format, ##__VA_ARGS__)
#define TICKER_WARNING(format, ...)					\
cf_ticker_warning(AS_SKEW, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_SKEW, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_SKEW, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_SKEW, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_SKEW, format, ##__VA_ARGS__)
#define ring_buffer_log(buffer, message, severity) ring_buffer_log_event(buffer, message, severity, AS_SKEW,	\
		__FILENAME__, __LINE__);

/*
 * ----------------------------------------------------------------------------
 * Skew monitor data structures
 * ----------------------------------------------------------------------------
 */

/**
 * Ring buffer holding a window of skew delta for a node.
 */
typedef struct as_skew_ring_buffer_s
{
	int64_t data[RING_BUFFER_CAPACITY_MAX];
	int start;
	int size;
	int capacity;
} as_skew_ring_buffer;

/**
 * Skew plugin data stored for all adjacent nodes.
 */
typedef struct as_skew_plugin_data_s
{
	as_skew_ring_buffer ring_buffer;
	uint8_t bad_hlc_streak;
} as_skew_plugin_data;

/**
 * Skew summary for a node for the current skew update interval.
 */
typedef struct as_skew_node_summary_s
{
	cf_node nodeid;
	int64_t delta;
} as_skew_node_summary;

/**
 * HB plugin data iterate struct to get node hlc deltas.
 */
typedef struct as_skew_monitor_hlc_delta_udata_s
{
	int num_nodes;
	as_skew_node_summary skew_summary[AS_CLUSTER_SZ];
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
static cf_atomic64 g_last_skew_check_time = 0;

/**
 * Current value of clock skew.
 */
static cf_atomic64 g_skew = 0;

/**
 * Self HLC delta over the last skew window. Access should under the self skew
 * data lock.
 */
static as_skew_ring_buffer g_self_skew_ring_buffer = { { 0 } };

/**
 * Lock for self skew ring buffer.
 */
static pthread_mutex_t g_self_skew_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

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
 * Ring buffer related
 * ----------------------------------------------------------------------------
 */

/**
 * Log contents of the ring buffer.
 */
static void
ring_buffer_log_event(as_skew_ring_buffer* ring_buffer, char* prefix,
		cf_fault_severity severity, cf_fault_context context, char* file_name,
		int line)
{
	int max_per_line = 25;
	int max_bytes_per_value = 21;	// Include the space as well
	char log_buffer[(max_per_line * max_bytes_per_value) + 1];	// For the NULL terminator.
	char* value_buffer_start = log_buffer;

	if (prefix) {
		value_buffer_start += sprintf(log_buffer, "%s", prefix);
	}

	for (int i = 0; i < ring_buffer->size; i++) {
		char* buffer = value_buffer_start;
		for (int j = 0; j < max_per_line && i < ring_buffer->size; j++) {
			buffer += sprintf(buffer, "%ld ",
					ring_buffer->data[(ring_buffer->start + i)
							% ring_buffer->capacity]);
			i++;
		}

		// Overwrite the space from the last node on the log line only if there
		// is atleast one node output
		if (buffer != value_buffer_start) {
			*(buffer - 1) = 0;
			cf_fault_event(context, severity, file_name, line, "%s",
					log_buffer);
		}
	}
}

/**
 * Get the representative hlc delta value for current ring buffer contents.
 */
static int64_t
ring_buffer_hlc_delta(as_skew_ring_buffer* buffer)
{
	int64_t max_delta = 0;

	for (int i = 0; i < buffer->size; i++) {
		int64_t delta = buffer->data[(buffer->start + i) % buffer->capacity];
		if (delta > max_delta) {
			max_delta = delta;
		}
	}

	return max_delta;
}

/**
 * The current capacity of the ring buffer based on heartbeat node timeout,
 * which determines how much skew history is maintained.
 */
static int
ring_buffer_current_capacity()
{
	// Maintain a history for one node timeout interval
	return MIN(RING_BUFFER_CAPACITY_MAX, as_hb_max_intervals_missed_get());
}

/**
 * Adjust the contents of the ring buffer to new capacity.
 */
static void
ring_buffer_adjust_to_capacity(as_skew_ring_buffer* buffer,
		const int new_capacity)
{
	if (buffer->capacity == new_capacity) {
		// No adjustments to data needed.
		return;
	}

	// Will only happen if the heartbeat node timeout is changed of if this is
	// the first insert.
	int new_size = buffer->size;
	if (buffer->size > new_capacity) {
		int shrink_by = buffer->size - new_capacity;
		// Drop the oldest values and copy over the rest.
		buffer->start = (buffer->start + shrink_by) % buffer->capacity;
		new_size = new_capacity;
	}

	// Shift values to be retained to start of the data array. Since this is not
	// a frequent operations use the simple technique of making a copy.
	int64_t adjusted_data[RING_BUFFER_CAPACITY_MAX];
	for (int i = 0; i < new_size; i++) {
		int buffer_index = (buffer->start + i) % buffer->capacity;
		adjusted_data[i] = buffer->data[buffer_index];
	}

	// Reset the buffer to start at index 0 and have new capacity.
	memcpy(buffer->data, adjusted_data, new_size);
	buffer->capacity = new_capacity;
	buffer->start = 0;
	buffer->size = new_size;
}

/**
 * Insert a new delta into the ring_buffer.
 */
static void
ring_buffer_insert(as_skew_ring_buffer* buffer, const int64_t delta)
{
	ring_buffer_adjust_to_capacity(buffer, ring_buffer_current_capacity());

	int insert_index = 0;
	if (buffer->size == buffer->capacity) {
		insert_index = buffer->start;
		buffer->start = (buffer->start + 1) % buffer->capacity;
	}
	else {
		insert_index = buffer->size;
		buffer->size++;
	}

	buffer->data[insert_index] = delta;
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

	if (!plugin_data || plugin_data_size < sizeof(as_skew_plugin_data)) {
		// Assume missing nodes share the same delta as self.
		// Note: self node will not be in adjacency list and hence will also
		// follow same code path.
		pthread_mutex_lock(&g_self_skew_lock);
		delta = ring_buffer_hlc_delta(&g_self_skew_ring_buffer);
		pthread_mutex_unlock(&g_self_skew_lock);
	}
	else {
		as_skew_plugin_data* skew_plugin_data =
				(as_skew_plugin_data*)plugin_data;
		delta = ring_buffer_hlc_delta(&skew_plugin_data->ring_buffer);
	}

	int index = deltas->num_nodes;
	deltas->skew_summary[index].delta = delta;
	deltas->skew_summary[index].nodeid = nodeid;
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
		int64_t delta = udata.skew_summary[i].delta;
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
	cf_atomic64_set(&g_skew, skew);

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		// Store return values so all handlers warn independently.
		bool record_stop_writes = as_record_handle_clock_skew(ns, skew);
		bool nsup_stop_writes = as_nsup_handle_clock_skew(ns, skew);

		ns->clock_skew_stop_writes = record_stop_writes || nsup_stop_writes;
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
skew_monitor_hlc_float_compare(const void* o1, const void* o2)
{
	float v1 = *(float*)o1;
	float v2 = *(float*)o2;
	return v1 > v2 ? 1 : (v1 == v2 ? 0 : -1);
}

/**
 * Compute the median of the data.
 *
 * @param values the values sorted.
 * @param from the start index (inclusive)
 * @param to the end index (inclusive)
 * @return the index of the median element
 */
static float
skew_monitor_median(float* values, int num_elements)
{
	if (num_elements % 2 == 0) {
		int median_left = (num_elements - 1) / 2;
		int median_right = median_left + 1;
		return (values[median_left] + values[median_right]) / 2.0f;
	}

	int median_index = (num_elements / 2);
	return (float)values[median_index];
}

/**
 * Return the currently estimated outliers from our cluster.
 * Outliers should have space to hold at least AS_CLUSTER_SZ nodes.
 */
static uint32_t
skew_monitor_outliers_from_skew_summary(cf_vector* outliers,
		as_skew_monitor_hlc_delta_udata* udata)
{
	// Use Median Absolute Deviation(MAD) to detect outliers, in general the
	// delta distribution would be symmetric and very close to the median.
	int num_nodes = udata->num_nodes;
	float deltas[num_nodes];
	for (int i = 0; i < num_nodes; i++) {
		deltas[i] = udata->skew_summary[i].delta;
	}

	// Compute median.
	qsort(deltas, num_nodes, sizeof(float), skew_monitor_hlc_float_compare);
	float median = skew_monitor_median(deltas, num_nodes);

	// Compute absolute deviation from median.
	float abs_dev[num_nodes];
	for (int i = 0; i < num_nodes; i++) {
		abs_dev[i] = fabsf(deltas[i] - median);
	}

	// Compute MAD.
	qsort(abs_dev, num_nodes, sizeof(float), skew_monitor_hlc_float_compare);
	float mad = skew_monitor_median(abs_dev, num_nodes);

	uint32_t num_outliers = 0;

	if (mad < 0.001f) {
		// Most deltas are very close to the median. Call values significantly
		// away as outliers.
		for (int i = 0; i < udata->num_nodes; i++) {
			if (fabsf(udata->skew_summary[i].delta - median)
					> skew_monitor_outlier_detection_threshold()) {
				if (outliers) {
					cf_vector_append(outliers, &udata->skew_summary[i].nodeid);
				}

				num_outliers++;
			}
		}
	}
	else {
		// Any node with delta deviating significantly compared to MAD is an
		// outlier.
		for (int i = 0; i < udata->num_nodes; i++) {
			if ((fabsf(udata->skew_summary[i].delta - median) / mad)
					> MAD_RATIO_OUTLIER_THRESHOLD) {
				if (outliers) {
					cf_vector_append(outliers, &udata->skew_summary[i].nodeid);
				}

				num_outliers++;
			}
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

	num_outliers = skew_monitor_outliers_from_skew_summary(outliers, &udata);

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

	// Update the clock delta for self.
	pthread_mutex_lock(&g_self_skew_lock);
	ring_buffer_insert(&g_self_skew_ring_buffer, clock_delta);
	pthread_mutex_unlock(&g_self_skew_lock);

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
		as_hb_plugin_node_data* prev_plugin_data,
		as_hb_plugin_node_data* plugin_data)
{
	cf_clock send_ts = 0;
	as_hlc_timestamp send_hlc_ts = 0;
	as_hlc_timestamp hlc_now = as_hlc_timestamp_now();

	if (msg_get_uint64(msg, AS_HB_MSG_SKEW_MONITOR_DATA, &send_ts) != 0
			|| as_hb_msg_send_hlc_ts_get(msg, &send_hlc_ts) != 0) {
		// Pre SC mode node. For now assumes it shares the same delta with hlc
		// as us.
		send_hlc_ts = hlc_now;
		send_ts = cf_clock_getabsolute();
	}

	size_t required_capacity = sizeof(as_skew_plugin_data);
	if (required_capacity > plugin_data->data_capacity) {
		plugin_data->data = cf_realloc(plugin_data->data, required_capacity);

		if (plugin_data->data == NULL) {
			CRASH("error allocating skew data for node %lx", source);
		}
		plugin_data->data_capacity = required_capacity;
	}

	if (plugin_data->data_size == 0) {
		// First data point.
		memset(plugin_data->data, 0, required_capacity);
	}

	if (prev_plugin_data->data_size != 0) {
		// Copy over older values to carry forward.
		memcpy(plugin_data->data, prev_plugin_data->data, required_capacity);
	}

	as_skew_plugin_data* skew_plugin_data =
			(as_skew_plugin_data*)plugin_data->data;

	int64_t hlc_diff_ms = abs(as_hlc_timestamp_diff_ms(send_hlc_ts, hlc_now));

	if (hlc_diff_ms > HLC_DEVIATION_MAX_MS) {
		if (skew_plugin_data->bad_hlc_streak < BAD_HLC_STREAK_MAX) {
			skew_plugin_data->bad_hlc_streak++;
			INFO("node %lx HLC not in sync - hlc %lu self-hlc %lu diff %ld",
					source, send_hlc_ts, hlc_now, hlc_diff_ms);
		}
		else {
			// Long running streak.
			TICKER_WARNING("node %lx HLC not in sync", source);
		}
	}
	else {
		// End the bad streak if the source is in one.
		skew_plugin_data->bad_hlc_streak = 0;
	}

	int64_t delta = 0;
	if ((skew_plugin_data->bad_hlc_streak > 0)
			&& skew_plugin_data->bad_hlc_streak <= BAD_HLC_STREAK_MAX) {
		// The peer is in a tolerable bad hlc streak. Assume it has nominal hlc
		// delta. This is most likely a restarted or a new node that hasn't
		// caught up with the cluster HLC yet.
		pthread_mutex_lock(&g_self_skew_lock);
		delta = ring_buffer_hlc_delta(&g_self_skew_ring_buffer);
		pthread_mutex_unlock(&g_self_skew_lock);
	}
	else {
		// This measurement is safe to use.
		delta = as_hlc_physical_ts_get(send_hlc_ts) - send_ts;
	}

	// Update the ring buffer with the new delta.
	ring_buffer_insert(&skew_plugin_data->ring_buffer, delta);

	if (cf_context_at_severity(AS_SKEW, CF_DETAIL)) {
		// Temporary debugging.
		char message[100];
		sprintf(message, "Insert for node: %lx - ", source);
		ring_buffer_log(&skew_plugin_data->ring_buffer, message, CF_DETAIL);
	}

	// Ensure the data size is set correctly.
	plugin_data->data_size = required_capacity;

	DETAIL("node %lx - hlc:%lu clock:%lu delta:%ld", source, send_hlc_ts,
			send_ts, delta);
}

/*
 * ----------------------------------------------------------------------------
 * Protceted API only meant for clustering.
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
		INFO("CSM:    node:%lx hlc-delta:%ld", udata.skew_summary[i].nodeid,
				udata.skew_summary[i].delta);
	}

	// Log the outliers.
	cf_vector_clear(&node_vector);
	skew_monitor_outliers(&node_vector);
	if (cf_vector_size(&node_vector)) {
		as_clustering_log_cf_node_vector(CF_INFO, AS_SKEW,
				"CSM: Estimated clock outliers", &node_vector);
	}

Cleanup:
	cf_vector_destroy(&node_vector);
}
