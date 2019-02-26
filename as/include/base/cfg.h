/*
 * cfg.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include <grp.h>
#include <pwd.h>
#include <stdbool.h>
#include <stdint.h>

#include "xdr_config.h"

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/cf_atomic.h"

#include "enhanced_alloc.h"
#include "hardware.h"
#include "node.h"
#include "socket.h"
#include "tls.h"

#include "base/security_config.h"
#include "fabric/clustering.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/hlc.h"


//==========================================================
// Forward declarations.
//

struct as_namespace_s;


//==========================================================
// Typedefs and constants.
//

#ifndef AS_NAMESPACE_SZ
#define AS_NAMESPACE_SZ 2
#endif

#define AS_CLUSTER_NAME_SZ 65

#define MAX_BATCH_THREADS 256
#define MAX_TLS_SPECS 10

typedef struct as_config_s {

	// The order here matches that in the configuration parser's enum,
	// cfg_case_id. This is for organizational sanity.

	//--------------------------------------------
	// service context.
	//

	// Normally visible, in canonical configuration file order:

	uid_t			uid;
	gid_t			gid;
	uint32_t		paxos_single_replica_limit; // cluster size at which, and below, the cluster will run with replication factor 1
	char*			pidfile;
	uint32_t		n_proto_fd_max;

	// Normally hidden:

	// Note - advertise-ipv6 affects a cf_socket_ee.c global, so can't be here.
	cf_topo_auto_pin auto_pin;
	uint32_t		n_batch_index_threads;
	uint32_t		batch_max_buffers_per_queue; // maximum number of buffers allowed in a buffer queue at any one time, fail batch if full
	uint32_t		batch_max_requests; // maximum count of database requests in a single batch
	uint32_t		batch_max_unused_buffers; // maximum number of buffers allowed in buffer pool at any one time
	char			cluster_name[AS_CLUSTER_NAME_SZ];
	as_clustering_config clustering_config;
	bool			fabric_benchmarks_enabled;
	bool			svc_benchmarks_enabled;
	bool			health_check_enabled;
	bool			info_hist_enabled;
	const char*		feature_key_file;
	uint32_t		hist_track_back; // total time span in seconds over which to cache data
	uint32_t		hist_track_slice; // period in seconds at which to cache histogram data
	char*			hist_track_thresholds; // comma-separated bucket (ms) values to track
	uint32_t		n_info_threads;
	bool			keep_caps_ssd_health;
	// Note - log-local-time affects a cf_fault.c global, so can't be here.
	uint32_t		migrate_fill_delay;
	uint32_t		migrate_max_num_incoming;
	uint32_t		n_migrate_threads;
	char*			node_id_interface;
	int				proto_fd_idle_ms; // after this many milliseconds, connections are aborted unless transaction is in progress
	int				proto_slow_netio_sleep_ms; // dynamic only
	uint32_t		query_bsize;
	uint64_t		query_buf_size; // dynamic only
	uint32_t		query_bufpool_size;
	bool			query_in_transaction_thr;
	uint32_t		query_long_q_max_size;
	bool			query_enable_histogram;
	bool			partitions_pre_reserved; // query will reserve all partitions up front
	uint32_t		query_priority;
	uint64_t		query_sleep_us;
	uint64_t		query_rec_count_bound;
	bool			query_req_in_query_thread;
	uint32_t		query_req_max_inflight;
	uint32_t		query_short_q_max_size;
	uint32_t		query_threads;
	uint32_t		query_threshold;
	uint64_t		query_untracked_time_ms;
	uint32_t		query_worker_threads;
	bool			run_as_daemon;
	uint32_t		scan_max_active; // maximum number of active scans allowed
	uint32_t		scan_max_done; // maximum number of finished scans kept for monitoring
	uint32_t		scan_max_udf_transactions; // maximum number of active transactions per UDF background scan
	uint32_t		scan_threads; // size of scan thread pool
	uint32_t		n_service_threads;
	uint32_t		sindex_builder_threads; // secondary index builder thread pool size
	uint32_t		sindex_gc_max_rate; // Max sindex entries processed per second for gc
	uint32_t		sindex_gc_period; // same as nsup_period for sindex gc
	uint32_t		ticker_interval;
	uint64_t		transaction_max_ns;
	uint32_t		n_transaction_queues;
	uint32_t		transaction_retry_ms;
	uint32_t		n_transaction_threads_per_queue;
	char*			work_directory;

	// For special debugging or bug-related repair:

	cf_alloc_debug	debug_allocations; // how to instrument the memory allocation API
	bool		fabric_dump_msgs; // whether to log information about existing "msg" objects and queues

	//--------------------------------------------
	// network::service context.
	//

	// Normally visible, in canonical configuration file order:

	cf_serv_spec	service; // client service

	// Normally hidden:

	cf_serv_spec	tls_service; // TLS client service

	//--------------------------------------------
	// network::heartbeat context.
	//

	cf_serv_spec	hb_serv_spec; // literal binding address spec parsed from config
	cf_serv_spec	hb_tls_serv_spec; // literal binding address spec for TLS parsed from config
	cf_addr_list	hb_multicast_groups; // literal multicast groups parsed from config
	as_hb_config	hb_config;

	//--------------------------------------------
	// network::fabric context.
	//

	// Normally visible, in canonical configuration file order:

	cf_serv_spec	fabric; // fabric service
	cf_serv_spec	tls_fabric; // TLS fabric service

	// Normally hidden:

	uint32_t		n_fabric_channel_fds[AS_FABRIC_N_CHANNELS];
	uint32_t		n_fabric_channel_recv_threads[AS_FABRIC_N_CHANNELS];
	bool			fabric_keepalive_enabled;
	int				fabric_keepalive_intvl;
	int				fabric_keepalive_probes;
	int				fabric_keepalive_time;
	uint32_t		fabric_latency_max_ms; // time window for ordering
	uint32_t		fabric_recv_rearm_threshold;
	uint32_t		n_fabric_send_threads;

	//--------------------------------------------
	// network::info context.
	//

	// Normally visible, in canonical configuration file order:

	cf_serv_spec	info; // info service

	//--------------------------------------------
	// Remaining configuration top-level contexts.
	//

	mod_lua_config	mod_lua;
	as_sec_config	sec_cfg;

	uint32_t		n_tls_specs;
	cf_tls_spec		tls_specs[MAX_TLS_SPECS];


	//======================================================
	// Not (directly) configuration. Many should probably be
	// relocated...
	//

	// Global variable that just shouldn't be here.
	cf_node			self_node;

	// Global variables that just shouldn't be here.
	cf_node			xdr_clmap[AS_CLUSTER_SZ]; // cluster map as known to XDR
	xdr_node_lst	xdr_peers_lst[AS_CLUSTER_SZ]; // last XDR shipping info of other nodes
	uint64_t		xdr_self_lastshiptime[DC_MAX_NUM]; // last XDR shipping by this node

	// Namespaces.
	struct as_namespace_s* namespaces[AS_NAMESPACE_SZ];
	uint32_t		n_namespaces;

	// To speed up transaction enqueue's determination of whether to "inline":
	uint32_t		n_namespaces_inlined;
	uint32_t		n_namespaces_not_inlined;

} as_config;


//==========================================================
// Public API.
//

as_config* as_config_init(const char* config_file);
void as_config_post_process(as_config* c, const char* config_file);

void as_config_cluster_name_get(char* cluster_name);
bool as_config_cluster_name_set(const char* cluster_name);
bool as_config_cluster_name_matches(const char* cluster_name);

bool as_config_error_enterprise_only();
bool as_config_error_enterprise_feature_only(const char* name);
bool as_info_error_enterprise_only(); // TODO - until we have an info split

extern as_config g_config;


//==========================================================
// Private API - for enterprise separation only.
//

// Parsed configuration file line.
typedef struct cfg_line_s {
	int		num;
	char*	name_tok;
	char*	val_tok_1;
	char*	val_tok_2;
	char*	val_tok_3;
} cfg_line;

void cfg_enterprise_only(const cfg_line* p_line);
void cfg_post_process();
cf_tls_spec* cfg_link_tls(const char* which, char** our_name);
