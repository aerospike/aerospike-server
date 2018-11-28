/*
 * health.c
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

//==========================================================
// Includes.
//

#include "base/health.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_vector.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "node.h"
#include "shash.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "fabric/exchange.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

#define DETECTION_INTERVAL 60 // 1 min
#define MAX_IDLE_SEC (60 * 30) // 30 min
#define MAX_ID_SZ 200
#define MAX_NODES_TRACKED (2 * AS_CLUSTER_SZ)
#define MIN_CONFIDENCE_PCT 50
#define MOV_AVG_COEFF 0.5
#define SAMPLE_PERIOD_US (50 * 1000) // sampling 50ms for every sec
#define SEC_US (1000 * 1000)

typedef struct bucket_s {
	cf_atomic64 sample_sum;
	cf_atomic32 n_samples; // relevant only for specific stats
} bucket;

typedef struct health_stat_s {
	bucket* buckets;
	volatile uint32_t cur_bucket;
	char id[MAX_ID_SZ];
} health_stat;

typedef struct peer_stats_s {
	health_stat node_stats[AS_HEALTH_NODE_TYPE_MAX];
	health_stat ns_stats[AS_NAMESPACE_SZ][AS_HEALTH_NS_TYPE_MAX];
	bool is_in_cluster;
	uint64_t last_sample;
} peer_stats;

typedef struct local_stats_s {
	health_stat device_read_lat[AS_NAMESPACE_SZ][AS_STORAGE_MAX_DEVICES];
} local_stats;

typedef struct mov_avg_s {
	char* id;
	double value;
} mov_avg;

typedef struct cluster_mov_avg_s {
	uint32_t n_nodes;
	mov_avg nma_array[MAX_NODES_TRACKED];
} cluster_mov_avg;

typedef struct cluster_all_mov_avg_s {
	cluster_mov_avg cl_node_stats[AS_HEALTH_NODE_TYPE_MAX];
	cluster_mov_avg cl_ns_stats[AS_NAMESPACE_SZ][AS_HEALTH_NS_TYPE_MAX];
} cluster_all_mov_avg;

typedef struct local_all_mov_avg_s {
	mov_avg device_mov_avg[AS_NAMESPACE_SZ][AS_STORAGE_MAX_DEVICES];
} local_all_mov_avg;

typedef struct outlier_s {
	uint32_t confidence_pct;
	char* id;
	uint32_t ns_id;
	const char* reason;
} outlier;

typedef struct stat_spec_s {
	bool is_counter;
	bool depends_on_cluster;
	uint32_t n_buckets; // one bucket per detection interval
	uint32_t threshold; // min sum/avg below which stat is not considered outlier
	const char* stat_str;
} stat_spec;

// Maintain order as per as_health_local_stat_type enum.
static const stat_spec local_stat_spec[] = {
	{ false, false, 30, 0, "device_read_latency" } // AS_HEALTH_LOCAL_DEVICE_READ_LAT
};

// Maintain order as per as_health_node_stat_type enum.
static const stat_spec node_stat_spec[] = {
	{ true, false, 30, 5, "fabric_connections_opened" }, // AS_HEALTH_NODE_FABRIC_FDS
	{ true, true, 30, 2, "proxies" }, // AS_HEALTH_NODE_PROXIES
	{ true, false, 30, 1, "node_arrivals" } // AS_HEALTH_NODE_ARRIVALS
};

// Maintain order as per as_health_ns_stat_type enum.
static const stat_spec ns_stat_spec[] = {
	{ false, true, 30, 0, "replication_latency" } // AS_HEALTH_NS_REPL_LAT
};


//==========================================================
// Forward declarations.
//

static int32_t clear_data_reduce_fn(const void* key, void* data, void* udata);
static void cluster_state_changed_fn(const as_exchange_cluster_changed_event* event, void* udata);
static int compare_stats(const void* o1, const void* o2);
static void compute_local_stats_mov_avg (local_all_mov_avg* lma);
static void compute_node_mov_avg(peer_stats* ps, cluster_all_mov_avg* cs, cf_node node);
static void compute_ns_mov_avg(peer_stats* ps, cluster_all_mov_avg* cs, cf_node node);
static void create_local_stats();
static peer_stats* create_node(cf_node node);
static void find_outliers_from_local_stats(local_all_mov_avg* lma);
static void find_outliers_from_stats(cluster_all_mov_avg* cs);
static void find_outlier_per_stat(mov_avg* ma, uint32_t n_entries, uint32_t threshold, const char* reason, uint32_t ns_id);
static int32_t mark_cl_membership_reduce_fn(const void* key, void* data, void* udata);
static void print_local_stats(cf_dyn_buf* db);
static void print_node_stats(peer_stats* ps, cf_dyn_buf* db, cf_node node);
static void print_ns_stats(peer_stats* ps, cf_dyn_buf* db, cf_node node);
static int32_t print_stats_reduce_fn(const void* key, void* data, void* udata);
static void reset_local_stats();
static void* run_health();
static void shift_window_local_stat();
static void shift_window_node_stat(peer_stats* ps);
static void shift_window_ns_stat(peer_stats* ps);
static int32_t update_mov_avg_reduce_fn(const void* key, void* data, void* udata);


//==========================================================
// Globals.
//

bool g_health_enabled = false;

__thread uint64_t g_device_read_counter = 0;
__thread uint64_t g_replica_write_counter = 0;

static local_stats g_local_stats;
static cf_mutex g_outlier_lock = CF_MUTEX_INIT;
static cf_vector* g_outliers;
static cf_shash* g_stats;


//==========================================================
// Inlines and macros.
//

static inline void
add_counter_sample(health_stat* hs)
{
	bucket* b = &hs->buckets[hs->cur_bucket];

	// For counters, sample_sum is just the total count, n_samples is unused.
	cf_atomic64_incr(&b->sample_sum);
}

static inline void
add_latency_sample(health_stat* hs, uint64_t delta_us)
{
	bucket* b = &hs->buckets[hs->cur_bucket];

	cf_atomic64_add(&b->sample_sum, delta_us);
	cf_atomic32_incr(&b->n_samples);
}

static inline double
compute_mov_avg_count(bucket* buckets, uint32_t n_buckets)
{
	uint64_t sample_sum = 0;

	for (uint32_t i = 0; i < n_buckets; i++) {
		sample_sum += buckets[i].sample_sum;
	}

	return (double)sample_sum;
}

static inline double
compute_mov_avg_latency(bucket* buckets, uint32_t n_buckets)
{
	uint64_t sample_sum = 0;
	uint64_t n_samples = 0;

	for (uint32_t i = 0; i < n_buckets; i++) {
		sample_sum += buckets[i].sample_sum;
		n_samples += buckets[i].n_samples;
	}

	return n_samples == 0 ? 0.0 : (double)sample_sum / (double)n_samples;
}

static inline double
compute_mov_avg(bucket* buckets, uint32_t n_buckets, bool is_counter)
{
	return is_counter ?
			compute_mov_avg_count(buckets, n_buckets) :
			compute_mov_avg_latency(buckets, n_buckets);
}

static inline uint32_t
find_median_index(uint32_t from, uint32_t to)
{
	return (to + from) / 2;
}

static inline bool
is_node_active(peer_stats* ps)
{
	return ps->is_in_cluster ||
			cf_get_seconds() - ps->last_sample < MAX_IDLE_SEC;
}


//==========================================================
// Public API.
//

void
as_health_get_outliers(cf_dyn_buf* db)
{
	if (! g_health_enabled) {
		return;
	}

	cf_detail(AS_HEALTH, "getting outlier info");

	cf_mutex_lock(&g_outlier_lock);

	for (uint32_t i = 0; i < cf_vector_size(g_outliers); i++) {
		outlier cur;
		cf_vector_get(g_outliers, i, &cur);

		cf_dyn_buf_append_string(db, "id=");
		cf_dyn_buf_append_string(db, cur.id);
		cf_dyn_buf_append_char(db, ':');

		if (cur.ns_id != 0) {
			cf_dyn_buf_append_string(db, "namespace=");
			cf_dyn_buf_append_string(db,
					g_config.namespaces[cur.ns_id - 1]->name);
			cf_dyn_buf_append_char(db, ':');
		}

		cf_dyn_buf_append_string(db, "confidence_pct=");
		cf_dyn_buf_append_uint32(db, cur.confidence_pct);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, "reason=");
		cf_dyn_buf_append_string(db, cur.reason);

		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_chomp(db);

	cf_mutex_unlock(&g_outlier_lock);
}

void
as_health_get_stats(cf_dyn_buf* db)
{
	if (! g_health_enabled) {
		return;
	}

	cf_shash_reduce(g_stats, print_stats_reduce_fn, db);
	print_local_stats(db);
	cf_dyn_buf_chomp(db);
}

void
as_health_start()
{
	g_stats = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node),
			sizeof(peer_stats*), AS_CLUSTER_SZ, CF_SHASH_MANY_LOCK);
	g_outliers = cf_vector_create(sizeof(outlier), 10, 0);

	create_local_stats();

	as_exchange_register_listener(cluster_state_changed_fn, NULL);

	cf_info(AS_HEALTH, "starting health monitor thread");

	cf_thread_create_detached(run_health, NULL);
}

void
health_add_device_latency(uint32_t ns_id, uint32_t d_id, uint64_t start_us)
{
	uint64_t delta_us = cf_getus() - start_us;
	health_stat* hs = &g_local_stats.device_read_lat[ns_id - 1][d_id];

	add_latency_sample(hs, delta_us);
}

void
health_add_node_counter(cf_node node, as_health_node_stat_type type)
{
	peer_stats* ps = NULL;

	if (cf_shash_get(g_stats, &node, &ps) != CF_SHASH_OK) {
		ps = create_node(node);
	}

	add_counter_sample(&ps->node_stats[type]);
}

void
health_add_ns_latency(cf_node node, uint32_t ns_id,
		as_health_ns_stat_type type, uint64_t start_us)
{
	uint64_t delta_us = cf_getus() - start_us;
	peer_stats* ps = NULL;

	if (cf_shash_get(g_stats, &node, &ps) != CF_SHASH_OK) {
		ps = create_node(node);
	}

	add_latency_sample(&ps->ns_stats[ns_id - 1][type], delta_us);
}


//==========================================================
// Local helpers.
//

static int32_t
clear_data_reduce_fn(const void* key, void* data, void* udata)
{
	peer_stats* ps = *(peer_stats**)data;

	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		size_t buckets_sz = sizeof(bucket) * node_stat_spec[type].n_buckets;
		memset(ps->node_stats[type].buckets, 0, buckets_sz);
		ps->node_stats[type].cur_bucket = 0;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			size_t buckets_sz = sizeof(bucket) * ns_stat_spec[type].n_buckets;
			memset(ps->ns_stats[ns_ix][type].buckets, 0, buckets_sz);
			ps->ns_stats[ns_ix][type].cur_bucket = 0;
		}
	}

	return CF_SHASH_OK;
}

static void
cluster_state_changed_fn(const as_exchange_cluster_changed_event* event,
		void* udata)
{
	cf_detail(AS_HEALTH, "received cluster state changed event");
	cf_shash_reduce(g_stats, mark_cl_membership_reduce_fn, (void*)event);
}

static int
compare_stats(const void* o1, const void* o2)
{
	double stat1 = ((mov_avg*)o1)->value;
	double stat2 = ((mov_avg*)o2)->value;

	return stat1 > stat2 ? 1 : (stat1 == stat2 ? 0 : -1);
}

static void
compute_local_stats_mov_avg(local_all_mov_avg* lma)
{
	const stat_spec* spec = &local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT];

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		uint32_t n_devices =
				as_namespace_device_count(g_config.namespaces[ns_ix]);

		for (uint32_t d_id = 0; d_id < n_devices; d_id++) {
			health_stat* hs = &g_local_stats.device_read_lat[ns_ix][d_id];
			mov_avg* dma = &lma->device_mov_avg[ns_ix][d_id];
			const char* device_name =
					g_config.namespaces[ns_ix]->storage_devices[d_id];

			dma->id = hs->id;
			dma->value = compute_mov_avg(hs->buckets, spec->n_buckets,
					spec->is_counter);

			cf_detail(AS_HEALTH, "moving average: device %s value %lf current bucket %lu",
					device_name, dma->value,
					hs->buckets[hs->cur_bucket].sample_sum);
		}
	}
}

// Fills per node information while computing moving avg/sum.
static void
compute_node_mov_avg(peer_stats* ps, cluster_all_mov_avg* cs, cf_node node)
{
	if (! is_node_active(ps)) {
		return;
	}

	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		const stat_spec* spec = &node_stat_spec[type];

		if (spec->depends_on_cluster && ! ps->is_in_cluster) {
			continue;
		}

		health_stat* hs = &ps->node_stats[type];
		uint32_t node_index = cs->cl_node_stats[type].n_nodes;

		// In extreme cases (e.g., adjacency list size >> succession list size)
		// we may have more nodes than MAX_NODES_TRACKED.
		if (node_index >= MAX_NODES_TRACKED) {
			continue;
		}

		mov_avg* nma = &cs->cl_node_stats[type].nma_array[node_index];

		nma->id = hs->id;
		nma->value = compute_mov_avg(hs->buckets, spec->n_buckets,
				spec->is_counter);
		cs->cl_node_stats[type].n_nodes++;

		cf_detail(AS_HEALTH, "moving average/sum: node %lx type %u value %lf current-bucket %lu",
				node, type, nma->value, hs->buckets[hs->cur_bucket].sample_sum);
	}
}

// Fills per namespace and per node information while computing moving avg/sum.
static void
compute_ns_mov_avg(peer_stats* ps, cluster_all_mov_avg* cs, cf_node node)
{
	if (! is_node_active(ps)) {
		return;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			const stat_spec* spec = &ns_stat_spec[type];

			if (spec->depends_on_cluster && ! ps->is_in_cluster) {
				continue;
			}

			health_stat* hs = &ps->ns_stats[ns_ix][type];
			uint32_t node_index = cs->cl_ns_stats[ns_ix][type].n_nodes;

			// In extreme cases (e.g., too many alumni) we may have more nodes
			// than MAX_NODES_TRACKED.
			if (node_index >= MAX_NODES_TRACKED) {
				continue;
			}

			mov_avg* nma = &cs->cl_ns_stats[ns_ix][type].nma_array[node_index];

			nma->id = hs->id;
			nma->value = compute_mov_avg(hs->buckets, spec->n_buckets,
					spec->is_counter);
			cs->cl_ns_stats[ns_ix][type].n_nodes++;

			cf_detail(AS_HEALTH, "moving average/sum: node %lx ns-id %u type %u value %lf current-bucket %lu",
					node, ns_ix + 1, type, nma->value,
					hs->buckets[hs->cur_bucket].sample_sum);
		}
	}
}

static void
create_local_stats()
{
	size_t buckets_sz = sizeof(bucket) *
			local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT].n_buckets;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];
		health_stat* device_stats = g_local_stats.device_read_lat[ns_ix];
		uint32_t n_devices = as_namespace_device_count(ns);

		for (uint32_t d_id = 0; d_id < n_devices; d_id++) {
			health_stat* hs = &device_stats[d_id];
			hs->buckets = cf_malloc(buckets_sz);
			memset(hs->buckets, 0, buckets_sz);
			hs->cur_bucket = 0;
			strncpy(hs->id, ns->storage_devices[d_id], MAX_ID_SZ);
		}
	}
}

static peer_stats*
create_node(cf_node node)
{
	peer_stats* ps = cf_malloc(sizeof(peer_stats));
	memset(ps, 0, sizeof(peer_stats));
	ps->is_in_cluster = true;

	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		size_t buckets_sz = sizeof(bucket) * node_stat_spec[type].n_buckets;
		health_stat* hs = &ps->node_stats[type];
		hs->buckets = cf_malloc(buckets_sz);
		memset(hs->buckets, 0, buckets_sz);
		sprintf(hs->id, "%lx", node);
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			size_t buckets_sz = sizeof(bucket) * ns_stat_spec[type].n_buckets;
			health_stat* hs = &ps->ns_stats[ns_ix][type];
			hs->buckets = cf_malloc(buckets_sz);
			memset(hs->buckets, 0, buckets_sz);
			sprintf(hs->id, "%lx", node);
		}
	}

	// Multiple callers may race to create a node.
	if (cf_shash_put_unique(g_stats, &node, &ps) == CF_SHASH_ERR_FOUND) {
		for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
			cf_free(ps->node_stats[type].buckets);
		}

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
				cf_free(ps->ns_stats[ns_ix][type].buckets);
			}
		}

		cf_free(ps);
		cf_shash_get(g_stats, &node, &ps);
	}

	return ps;
}

// Use inter-quartile distance to detect outliers.
static void
find_outliers_from_local_stats(local_all_mov_avg* lma)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		uint32_t n_devices =
				as_namespace_device_count(g_config.namespaces[ns_ix]);
		mov_avg* dma = lma->device_mov_avg[ns_ix];
		const stat_spec* spec =
				&local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT];

		find_outlier_per_stat(dma, n_devices, spec->threshold, spec->stat_str,
				ns_ix + 1);
	}
}

// Use inter-quartile distance to detect outliers.
static void
find_outliers_from_stats(cluster_all_mov_avg* cs)
{
	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		cluster_mov_avg* cma = &cs->cl_node_stats[type];
		const stat_spec* spec = &node_stat_spec[type];

		find_outlier_per_stat(cma->nma_array, cma->n_nodes, spec->threshold,
				spec->stat_str, 0);
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			cluster_mov_avg* cma = &cs->cl_ns_stats[ns_ix][type];
			const stat_spec* spec = &ns_stat_spec[type];

			find_outlier_per_stat(cma->nma_array, cma->n_nodes, spec->threshold,
					spec->stat_str, ns_ix + 1);
		}
	}
}

static void
find_outlier_per_stat(mov_avg* ma, uint32_t n_entries, uint32_t threshold,
		const char* reason, uint32_t ns_id)
{
	// Nobody can be declared as outliers with 1 or 2 entries.
	if (n_entries <= 2) {
		return;
	}

	qsort(ma, n_entries, sizeof(mov_avg), compare_stats);
	uint32_t q2_index = find_median_index(0, n_entries - 1);
	uint32_t q3_index = find_median_index(q2_index, n_entries - 1);
	uint32_t q1_index = find_median_index(0, q2_index - 1);

	double q3 = ma[q3_index].value;
	double q2 = ma[q2_index].value;
	double q1 = ma[q1_index].value;
	// Picking k-factor as 3 to detect far off outliers.
	double iqr = q3 - q1;
	double upper_bound = q3 + 3 * iqr;

	for (uint32_t i = 0; i < n_entries; i++) {
		double mov_avg = ma[i].value;
		uint32_t confidence_pct = (uint32_t)
				(((mov_avg - q2) * 100 / mov_avg) + 0.5);

		if (mov_avg > upper_bound && mov_avg > threshold &&
				confidence_pct >= MIN_CONFIDENCE_PCT) {
			outlier outlier = {
					.confidence_pct = confidence_pct,
					.id = ma[i].id,
					.ns_id = ns_id,
					.reason = reason
			};

			cf_vector_append(g_outliers, &outlier);
		}
	}
}

static int32_t
mark_cl_membership_reduce_fn(const void* key, void* data, void* udata)
{
	peer_stats* ps = *(peer_stats**)data;
	cf_node* node = (cf_node*)key;
	as_exchange_cluster_changed_event* event =
			(as_exchange_cluster_changed_event*)udata;

	ps->is_in_cluster = contains_node(event->succession, event->cluster_size,
			*node);

	return CF_SHASH_OK;
}

static void
print_local_stats(cf_dyn_buf* db)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];
		health_stat* device_stats = g_local_stats.device_read_lat[ns_ix];
		uint32_t n_devices = as_namespace_device_count(ns);
		const stat_spec* spec =
				&local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT];

		for (uint32_t d_id = 0; d_id < n_devices; d_id++) {
			health_stat* hs = &device_stats[d_id];
			double mov_avg = compute_mov_avg(hs->buckets, spec->n_buckets,
					spec->is_counter);

			cf_dyn_buf_append_string(db, "stat=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, "_");
			cf_dyn_buf_append_string(db, spec->stat_str);
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "value=");
			cf_dyn_buf_append_int(db, (int)(mov_avg + 0.5));
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "device=");
			cf_dyn_buf_append_string(db, hs->id);
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "namespace=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ";");
		}
	}
}

static void
print_node_stats(peer_stats* ps, cf_dyn_buf* db, cf_node node)
{
	if (! is_node_active(ps)) {
		return;
	}

	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		const stat_spec* spec = &node_stat_spec[type];

		if (spec->depends_on_cluster && ! ps->is_in_cluster) {
			continue;
		}

		health_stat* hs = &ps->node_stats[type];
		double mov_avg = compute_mov_avg(hs->buckets, spec->n_buckets,
				spec->is_counter);

		cf_dyn_buf_append_string(db, "stat=");
		cf_dyn_buf_append_string(db, spec->stat_str);
		cf_dyn_buf_append_string(db, ":");
		cf_dyn_buf_append_string(db, "value=");
		cf_dyn_buf_append_int(db, (int)(mov_avg + 0.5));
		cf_dyn_buf_append_string(db, ":");
		cf_dyn_buf_append_string(db, "node=");
		cf_dyn_buf_append_uint64_x(db, node);
		cf_dyn_buf_append_string(db, ";");
	}
}

static void
print_ns_stats(peer_stats* ps, cf_dyn_buf* db, cf_node node)
{
	if (! is_node_active(ps)) {
		return;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			const stat_spec* spec = &ns_stat_spec[type];

			if (spec->depends_on_cluster && ! ps->is_in_cluster) {
				continue;
			}

			health_stat* hs = &ps->ns_stats[ns_ix][type];
			double mov_avg = compute_mov_avg(hs->buckets, spec->n_buckets,
					spec->is_counter);

			as_namespace* ns = g_config.namespaces[ns_ix];

			cf_dyn_buf_append_string(db, "stat=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, "_");
			cf_dyn_buf_append_string(db, spec->stat_str);
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "value=");
			cf_dyn_buf_append_int(db, (int)(mov_avg + 0.5));
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "node=");
			cf_dyn_buf_append_uint64_x(db, node);
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_string(db, "namespace=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ";");
		}
	}
}

static int32_t
print_stats_reduce_fn(const void* key, void* data, void* udata)
{
	peer_stats* ps = *(peer_stats**)data;
	cf_dyn_buf* db = (cf_dyn_buf*)udata;
	cf_node* node = (cf_node*)key;

	print_node_stats(ps, db, *node);
	print_ns_stats(ps, db, *node);

	return CF_SHASH_OK;
}

static void
reset_local_stats()
{
	size_t buckets_sz = sizeof(bucket) *
			local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT].n_buckets;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		health_stat* device_stats = g_local_stats.device_read_lat[ns_ix];
		uint32_t n_devices =
				as_namespace_device_count(g_config.namespaces[ns_ix]);

		for (uint32_t d_id = 0; d_id < n_devices; d_id++) {
			health_stat* hs = &device_stats[d_id];

			memset(hs->buckets, 0, buckets_sz);
			hs->cur_bucket = 0;
		}
	}
}

static void*
run_health()
{
	uint64_t last_time = 0; // try to ensure we run immediately at startup

	while (true) {
		sleep(1); // wake up every second to check

		if (! g_config.health_check_enabled) {
			if (g_health_enabled) {
				g_health_enabled = false;
				last_time = 0; // allow re-enabling to take immediate effect
			}

			continue;
		}

		uint64_t curr_time = cf_get_seconds(); // may be near 0 at startup

		if (curr_time - last_time < DETECTION_INTERVAL) {
			continue;
		}

		last_time = curr_time;

		cf_mutex_lock(&g_outlier_lock);

		cf_vector_clear(g_outliers);

		if (! g_health_enabled) {
			cf_mutex_unlock(&g_outlier_lock);

			// Clear everything.
			cf_shash_reduce(g_stats, clear_data_reduce_fn, NULL);
			reset_local_stats();

			g_health_enabled = true;
			continue; // no point analyzing yet - wait one interval
		}

		cluster_all_mov_avg cs;
		local_all_mov_avg lma;
		memset(&cs, 0, sizeof(cs));
		memset(&lma, 0, sizeof(lma));

		cf_shash_reduce(g_stats, update_mov_avg_reduce_fn, &cs);
		compute_local_stats_mov_avg(&lma);
		shift_window_local_stat();

		find_outliers_from_stats(&cs);
		find_outliers_from_local_stats(&lma);

		for (uint32_t i = 0; i < cf_vector_size(g_outliers); i++) {
			outlier cur;
			cf_vector_get(g_outliers, i, &cur);

			if (cur.ns_id == 0) {
				cf_warning(AS_HEALTH, "outlier %s: confidence-pct %u reason %s",
						cur.id, cur.confidence_pct, cur.reason);
			}
			else {
				cf_warning(AS_HEALTH, "outlier %s: namespace %s confidence-pct %u reason %s",
						cur.id, g_config.namespaces[cur.ns_id - 1]->name,
						cur.confidence_pct, cur.reason);
			}
		}

		cf_mutex_unlock(&g_outlier_lock);
	}

	return NULL;
}

static void
shift_window_local_stat()
{
	const stat_spec* spec = &local_stat_spec[AS_HEALTH_LOCAL_DEVICE_READ_LAT];

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		uint32_t n_devices =
				as_namespace_device_count(g_config.namespaces[ns_ix]);

		for (uint32_t d_id = 0; d_id < n_devices; d_id++) {
			health_stat* stat = &g_local_stats.device_read_lat[ns_ix][d_id];
			uint32_t index = (stat->cur_bucket + 1) % spec->n_buckets;
			bucket* new_bucket = &stat->buckets[index];

			cf_atomic64_set(&new_bucket->sample_sum, 0);
			cf_atomic32_set(&new_bucket->n_samples, 0);
			stat->cur_bucket = index;
		}
	}
}

static void
shift_window_node_stat(peer_stats* ps)
{
	for (uint32_t type = 0; type < AS_HEALTH_NODE_TYPE_MAX; type++) {
		health_stat* stat = &ps->node_stats[type];
		uint32_t index = stat->cur_bucket;
		bucket* cur_bucket = &stat->buckets[index];

		if (cur_bucket->n_samples != 0) {
			ps->last_sample = cf_get_seconds();
		}

		index = (index + 1) % node_stat_spec[type].n_buckets;
		bucket* new_bucket = &stat->buckets[index];

		cf_atomic64_set(&new_bucket->sample_sum, 0);
		cf_atomic64_set(&new_bucket->n_samples, 0);
		stat->cur_bucket = index;
	}
}

static void
shift_window_ns_stat(peer_stats* ps)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++ ) {
		for (uint32_t type = 0; type < AS_HEALTH_NS_TYPE_MAX; type++) {
			health_stat* stat = &ps->ns_stats[ns_ix][type];
			uint32_t index = stat->cur_bucket;
			bucket* cur_bucket = &stat->buckets[index];

			if (cur_bucket->n_samples != 0) {
				ps->last_sample = cf_get_seconds();
			}

			index = (index + 1) % ns_stat_spec[type].n_buckets;
			bucket* new_bucket = &stat->buckets[index];

			cf_atomic64_set(&new_bucket->sample_sum, 0);
			cf_atomic32_set(&new_bucket->n_samples, 0);
			stat->cur_bucket = index;
		}
	}
}

static int32_t
update_mov_avg_reduce_fn(const void* key, void* data, void* udata)
{
	peer_stats* ps = *(peer_stats**)data;
	cluster_all_mov_avg* cs = (cluster_all_mov_avg*)udata;
	cf_node* node = (cf_node*)key;

	compute_node_mov_avg(ps, cs, *node);
	compute_ns_mov_avg(ps, cs, *node);
	shift_window_node_stat(ps);
	shift_window_ns_stat(ps);

	return CF_SHASH_OK;
}
