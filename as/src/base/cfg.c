/*
 * cfg.c
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

#include "base/cfg.h"

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <linux/capability.h>
#include <sys/resource.h>
#include <sys/types.h>

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_vector.h"

#include "arenax.h"
#include "bits.h"
#include "cf_mutex.h"
#include "cf_str.h"
#include "daemon.h"
#include "dynbuf.h"
#include "fault.h"
#include "hardware.h"
#include "hist.h"
#include "hist_track.h"
#include "msg.h"
#include "node.h"
#include "socket.h"
#include "tls.h"
#include "xmem.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/security_config.h"
#include "base/service.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/thr_info_port.h"
#include "base/thr_query.h"
#include "base/thr_sindex.h"
#include "base/thr_tsvc.h"
#include "base/transaction_policy.h"
#include "base/xdr_config.h"
#include "base/xdr_serverside.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"


//==========================================================
// Globals.
//

// The runtime configuration instance.
as_config g_config;


//==========================================================
// Forward declarations.
//

void init_addr_list(cf_addr_list* addrs);
void add_addr(const char* name, cf_addr_list* addrs);
void add_tls_peer_name(const char* name, cf_serv_spec* spec);
void copy_addrs(const cf_addr_list* from, cf_addr_list* to);
void default_addrs(cf_addr_list* one, cf_addr_list* two);
void bind_to_access(const cf_serv_spec* from, cf_addr_list* to);
void cfg_add_addr_bind(const char* name, cf_serv_spec* spec);
void cfg_add_addr_std(const char* name, cf_serv_spec* spec);
void cfg_add_addr_alt(const char* name, cf_serv_spec* spec);
void cfg_mserv_config_from_addrs(cf_addr_list* addrs, cf_addr_list* bind_addrs, cf_mserv_cfg* serv_cfg, cf_ip_port port, cf_sock_owner owner, uint8_t ttl);
void cfg_serv_spec_to_bind(const cf_serv_spec* spec, const cf_serv_spec* def_spec, cf_serv_cfg* bind, cf_sock_owner owner);
void cfg_serv_spec_std_to_access(const cf_serv_spec* spec, cf_addr_list* access);
void cfg_serv_spec_alt_to_access(const cf_serv_spec* spec, cf_addr_list* access);
void cfg_add_mesh_seed_addr_port(char* addr, cf_ip_port port, bool tls);
as_set* cfg_add_set(as_namespace* ns);
void cfg_add_xmem_mount(as_namespace* ns, const char* mount);
void cfg_add_storage_file(as_namespace* ns, const char* file_name, const char* shadow_name);
void cfg_add_storage_device(as_namespace* ns, const char* device_name, const char* shadow_name);
void cfg_set_cluster_name(char* cluster_name);
void cfg_add_ldap_role_query_pattern(char* pattern);
void create_and_check_hist_track(cf_hist_track** h, const char* name, histogram_scale scale);
void cfg_create_all_histograms();
void cfg_init_serv_spec(cf_serv_spec* spec_p);
cf_tls_spec* cfg_create_tls_spec(as_config* cfg, char* name);
char* cfg_resolve_tls_name(char* tls_name, const char* cluster_name, const char* which);
void cfg_keep_cap(bool keep, bool* what, int32_t cap);

xdr_dest_config* xdr_cfg_add_datacenter(char* name);
void xdr_cfg_associate_datacenter(char* dc, uint32_t nsid);
void xdr_cfg_add_http_url(xdr_dest_config* dest_cfg, char* url);
void xdr_cfg_add_node_addr_port(xdr_dest_config* dest_cfg, char* addr, int port);
void xdr_cfg_add_tls_node(xdr_dest_config* dest_cfg, char* addr, char* tls_name, int port);


//==========================================================
// Helper - set as_config defaults.
//

void
cfg_set_defaults()
{
	as_config* c = &g_config;

	memset(c, 0, sizeof(as_config));

	cfg_init_serv_spec(&c->service);
	cfg_init_serv_spec(&c->tls_service);
	cfg_init_serv_spec(&c->hb_serv_spec);
	cfg_init_serv_spec(&c->hb_tls_serv_spec);
	cfg_init_serv_spec(&c->fabric);
	cfg_init_serv_spec(&c->tls_fabric);
	cfg_init_serv_spec(&c->info);

	c->uid = (uid_t)-1;
	c->gid = (gid_t)-1;
	c->paxos_single_replica_limit = 1; // by default all clusters obey replication counts
	c->n_proto_fd_max = 15000;
	c->batch_max_buffers_per_queue = 255; // maximum number of buffers allowed in a single queue
	c->batch_max_requests = 5000; // maximum requests/digests in a single batch
	c->batch_max_unused_buffers = 256; // maximum number of buffers allowed in batch buffer pool
	c->feature_key_file = "/etc/aerospike/features.conf";
	c->hist_track_back = 300;
	c->hist_track_slice = 10;
	c->n_info_threads = 16;
	c->migrate_max_num_incoming = AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING; // for receiver-side migration flow-control
	c->n_migrate_threads = 1;
	c->proto_fd_idle_ms = 60000; // 1 minute reaping of proto file descriptors
	c->proto_slow_netio_sleep_ms = 1; // 1 ms sleep between retry for slow queries
	c->run_as_daemon = true; // set false only to run in debugger & see console output
	c->scan_max_active = 100;
	c->scan_max_done = 100;
	c->scan_max_udf_transactions = 32;
	c->scan_threads = 4;
	c->ticker_interval = 10;
	c->transaction_max_ns = 1000 * 1000 * 1000; // 1 second
	c->transaction_retry_ms = 1000 + 2; // 1 second + epsilon, so default timeout happens first
	c->n_transaction_threads_per_queue = 4;
	as_sindex_gconfig_default(c);
	as_query_gconfig_default(c);
	c->work_directory = "/opt/aerospike";
	c->debug_allocations = CF_ALLOC_DEBUG_NONE;
	c->fabric_dump_msgs = false;

	// Network heartbeat defaults.
	c->hb_config.mode = AS_HB_MODE_UNDEF;
	c->hb_config.tx_interval = 150;
	c->hb_config.max_intervals_missed = 10;
	c->hb_config.protocol = AS_HB_PROTOCOL_V3;
	c->hb_config.override_mtu = 0;

	// Fabric defaults.
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK] = 2;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL] = 1;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_META] = 1;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW] = 8;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW] = 16;
	c->fabric_keepalive_enabled = true;
	c->fabric_keepalive_intvl = 1; // seconds
	c->fabric_keepalive_probes = 10; // tries
	c->fabric_keepalive_time = 1; // seconds
	c->fabric_latency_max_ms = 5; // assume a one way latency of 5 milliseconds by default
	c->fabric_recv_rearm_threshold = 1024;
	c->n_fabric_send_threads = 8;

	// Clustering defaults.
	c->clustering_config.cluster_size_min = 1;
	c->clustering_config.clique_based_eviction_enabled = true;

	// XDR defaults.
	for (int i = 0; i < AS_CLUSTER_SZ ; i++) {
		c->xdr_peers_lst[i].node = 0;

		for (int j = 0; j < DC_MAX_NUM; j++) {
			c->xdr_peers_lst[i].time[j] = 0;
		}

		c->xdr_clmap[i] = 0;
	}

	for (int j = 0; j < DC_MAX_NUM; j++) {
		c->xdr_self_lastshiptime[j] = 0;
	}

	// Mod-lua defaults.
	c->mod_lua.server_mode      = true;
	c->mod_lua.cache_enabled    = true;
	strcpy(c->mod_lua.user_path, "/opt/aerospike/usr/udf/lua");

	// TODO - security set default config API?
	// Security defaults.
	c->sec_cfg.n_ldap_login_threads = 8;
	c->sec_cfg.privilege_refresh_period = 60 * 5; // refresh socket privileges every 5 minutes
	// Security LDAP defaults.
	c->sec_cfg.ldap_polling_period = 60 * 5;
	c->sec_cfg.ldap_session_ttl = 60 * 60 * 24;
	c->sec_cfg.ldap_token_hash_method = AS_LDAP_EVP_SHA_256;
	// Security syslog defaults.
	c->sec_cfg.syslog_local = AS_SYSLOG_NONE;
}

//==========================================================
// All configuration items must have a switch case
// identifier somewhere in this enum. The order is not
// important, other than for organizational sanity.
//

typedef enum {
	// Generic:
	// Token not found:
	CASE_NOT_FOUND,
	// Start of parsing context:
	CASE_CONTEXT_BEGIN,
	// End of parsing context:
	CASE_CONTEXT_END,

	// Top-level options:
	// In canonical configuration file order:
	CASE_SERVICE_BEGIN,
	CASE_LOGGING_BEGIN,
	CASE_NETWORK_BEGIN,
	CASE_NAMESPACE_BEGIN,
	CASE_MOD_LUA_BEGIN,
	CASE_CLUSTER_BEGIN,
	// Enterprise-only:
	CASE_SECURITY_BEGIN,
	CASE_XDR_BEGIN,

	// Service options:
	// Normally visible, in canonical configuration file order:
	CASE_SERVICE_USER,
	CASE_SERVICE_GROUP,
	CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT,
	CASE_SERVICE_PIDFILE,
	CASE_SERVICE_CLIENT_FD_MAX, // renamed
	CASE_SERVICE_PROTO_FD_MAX,
	// Normally hidden:
	CASE_SERVICE_ADVERTISE_IPV6,
	CASE_SERVICE_AUTO_PIN,
	CASE_SERVICE_BATCH_INDEX_THREADS,
	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE,
	CASE_SERVICE_BATCH_MAX_REQUESTS,
	CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS,
	CASE_SERVICE_CLUSTER_NAME,
	CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC,
	CASE_SERVICE_ENABLE_BENCHMARKS_SVC,
	CASE_SERVICE_ENABLE_HEALTH_CHECK,
	CASE_SERVICE_ENABLE_HIST_INFO,
	CASE_SERVICE_FEATURE_KEY_FILE,
	CASE_SERVICE_HIST_TRACK_BACK,
	CASE_SERVICE_HIST_TRACK_SLICE,
	CASE_SERVICE_HIST_TRACK_THRESHOLDS,
	CASE_SERVICE_INFO_THREADS,
	CASE_SERVICE_KEEP_CAPS_SSD_HEALTH,
	CASE_SERVICE_LOG_LOCAL_TIME,
	CASE_SERVICE_LOG_MILLIS,
	CASE_SERVICE_MIGRATE_FILL_DELAY,
	CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING,
	CASE_SERVICE_MIGRATE_THREADS,
	CASE_SERVICE_MIN_CLUSTER_SIZE,
	CASE_SERVICE_NODE_ID,
	CASE_SERVICE_NODE_ID_INTERFACE,
	CASE_SERVICE_PROTO_FD_IDLE_MS,
	CASE_SERVICE_QUERY_BATCH_SIZE,
	CASE_SERVICE_QUERY_BUFPOOL_SIZE,
	CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD,
	CASE_SERVICE_QUERY_LONG_Q_MAX_SIZE,
	CASE_SERVICE_QUERY_PRE_RESERVE_PARTITIONS,
	CASE_SERVICE_QUERY_PRIORITY,
	CASE_SERVICE_QUERY_PRIORITY_SLEEP_US,
	CASE_SERVICE_QUERY_REC_COUNT_BOUND,
	CASE_SERVICE_QUERY_REQ_IN_QUERY_THREAD,
	CASE_SERVICE_QUERY_REQ_MAX_INFLIGHT,
	CASE_SERVICE_QUERY_SHORT_Q_MAX_SIZE,
	CASE_SERVICE_QUERY_THREADS,
	CASE_SERVICE_QUERY_THRESHOLD,
	CASE_SERVICE_QUERY_UNTRACKED_TIME_MS,
	CASE_SERVICE_QUERY_WORKER_THREADS,
	CASE_SERVICE_RUN_AS_DAEMON,
	CASE_SERVICE_SCAN_MAX_ACTIVE,
	CASE_SERVICE_SCAN_MAX_DONE,
	CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS,
	CASE_SERVICE_SCAN_THREADS,
	CASE_SERVICE_SERVICE_THREADS,
	CASE_SERVICE_SINDEX_BUILDER_THREADS,
	CASE_SERVICE_SINDEX_GC_MAX_RATE,
	CASE_SERVICE_SINDEX_GC_PERIOD,
	CASE_SERVICE_TICKER_INTERVAL,
	CASE_SERVICE_TRANSACTION_MAX_MS,
	CASE_SERVICE_TRANSACTION_QUEUES,
	CASE_SERVICE_TRANSACTION_RETRY_MS,
	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE,
	CASE_SERVICE_WORK_DIRECTORY,
	// For special debugging or bug-related repair:
	CASE_SERVICE_DEBUG_ALLOCATIONS,
	CASE_SERVICE_FABRIC_DUMP_MSGS,
	// Obsoleted:
	CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS,
	CASE_SERVICE_NSUP_PERIOD,
	CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD,
	CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION,
	CASE_SERVICE_TRANSACTION_PENDING_LIMIT,
	CASE_SERVICE_TRANSACTION_REPEATABLE_READ,
	// Deprecated:
	CASE_SERVICE_AUTO_DUN,
	CASE_SERVICE_AUTO_UNDUN,
	CASE_SERVICE_BATCH_PRIORITY,
	CASE_SERVICE_BATCH_RETRANSMIT,
	CASE_SERVICE_BATCH_THREADS,
	CASE_SERVICE_CLIB_LIBRARY,
	CASE_SERVICE_DEFRAG_QUEUE_ESCAPE,
	CASE_SERVICE_DEFRAG_QUEUE_HWM,
	CASE_SERVICE_DEFRAG_QUEUE_LWM,
	CASE_SERVICE_DEFRAG_QUEUE_PRIORITY,
	CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE,
	CASE_SERVICE_FABRIC_WORKERS,
	CASE_SERVICE_FB_HEALTH_BAD_PCT,
	CASE_SERVICE_FB_HEALTH_GOOD_PCT,
	CASE_SERVICE_FB_HEALTH_MSG_PER_BURST,
	CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT,
	CASE_SERVICE_GENERATION_DISABLE,
	CASE_SERVICE_MAX_MSGS_PER_TYPE,
	CASE_SERVICE_MIGRATE_READ_PRIORITY,
	CASE_SERVICE_MIGRATE_READ_SLEEP,
	CASE_SERVICE_MIGRATE_RX_LIFETIME_MS,
	CASE_SERVICE_MIGRATE_XMIT_HWM,
	CASE_SERVICE_MIGRATE_XMIT_LWM,
	CASE_SERVICE_MIGRATE_PRIORITY, // renamed
	CASE_SERVICE_MIGRATE_XMIT_PRIORITY,
	CASE_SERVICE_MIGRATE_XMIT_SLEEP,
	CASE_SERVICE_NSUP_AUTO_HWM,
	CASE_SERVICE_NSUP_AUTO_HWM_PCT,
	CASE_SERVICE_NSUP_DELETE_SLEEP,
	CASE_SERVICE_NSUP_MAX_DELETES,
	CASE_SERVICE_NSUP_QUEUE_HWM,
	CASE_SERVICE_NSUP_QUEUE_LWM,
	CASE_SERVICE_NSUP_QUEUE_ESCAPE,
	CASE_SERVICE_NSUP_REDUCE_PRIORITY,
	CASE_SERVICE_NSUP_REDUCE_SLEEP,
	CASE_SERVICE_NSUP_STARTUP_EVICT,
	CASE_SERVICE_NSUP_THREADS,
	CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE,
	CASE_SERVICE_PAXOS_PROTOCOL,
	CASE_SERVICE_PAXOS_RECOVERY_POLICY,
	CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD,
	CASE_SERVICE_PROLE_EXTRA_TTL,
	CASE_SERVICE_REPLICATION_FIRE_AND_FORGET,
	CASE_SERVICE_SCAN_MEMORY,
	CASE_SERVICE_SCAN_PRIORITY,
	CASE_SERVICE_SCAN_RETRANSMIT,
	CASE_SERVICE_SCHEDULER_PRIORITY,
	CASE_SERVICE_SCHEDULER_TYPE,
	CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS,
	CASE_SERVICE_TRIAL_ACCOUNT_KEY,
	CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY,
	CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY,
	CASE_SERVICE_USE_QUEUE_PER_DEVICE,
	CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE,

	// Service auto-pin options (value tokens):
	CASE_SERVICE_AUTO_PIN_NONE,
	CASE_SERVICE_AUTO_PIN_CPU,
	CASE_SERVICE_AUTO_PIN_NUMA,

	// Service debug-allocations options (value tokens):
	CASE_SERVICE_DEBUG_ALLOCATIONS_NONE,
	CASE_SERVICE_DEBUG_ALLOCATIONS_TRANSIENT,
	CASE_SERVICE_DEBUG_ALLOCATIONS_PERSISTENT,
	CASE_SERVICE_DEBUG_ALLOCATIONS_ALL,

	// Logging options:
	// Normally visible:
	CASE_LOG_FILE_BEGIN,
	// Normally hidden:
	CASE_LOG_CONSOLE_BEGIN,

	// Logging file options:
	// Normally visible:
	CASE_LOG_FILE_CONTEXT,

	// Logging console options:
	// Normally visible:
	CASE_LOG_CONSOLE_CONTEXT,

	// Network options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_SERVICE_BEGIN,
	CASE_NETWORK_HEARTBEAT_BEGIN,
	CASE_NETWORK_FABRIC_BEGIN,
	CASE_NETWORK_INFO_BEGIN,
	// Normally hidden:
	CASE_NETWORK_TLS_BEGIN,

	// Network service options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_SERVICE_ADDRESS,
	CASE_NETWORK_SERVICE_PORT,
	// Normally hidden:
	CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS, // renamed
	CASE_NETWORK_SERVICE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_ACCESS_PORT,
	CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT,
	CASE_NETWORK_SERVICE_TLS_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_TLS_ACCESS_PORT,
	CASE_NETWORK_SERVICE_TLS_ADDRESS,
	CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_PORT,
	CASE_NETWORK_SERVICE_TLS_AUTHENTICATE_CLIENT,
	CASE_NETWORK_SERVICE_TLS_NAME,
	CASE_NETWORK_SERVICE_TLS_PORT,
	// Obsoleted:
	CASE_NETWORK_SERVICE_ALTERNATE_ADDRESS,
	CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME,
	// Deprecated:
	CASE_NETWORK_SERVICE_REUSE_ADDRESS,

	// Network heartbeat options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_HEARTBEAT_MODE,
	CASE_NETWORK_HEARTBEAT_ADDRESS,
	CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP,
	CASE_NETWORK_HEARTBEAT_PORT,
	CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT,
	CASE_NETWORK_HEARTBEAT_INTERVAL,
	CASE_NETWORK_HEARTBEAT_TIMEOUT,
	// Normally hidden:
	CASE_NETWORK_HEARTBEAT_MTU,
	CASE_NETWORK_HEARTBEAT_MCAST_TTL, // renamed
	CASE_NETWORK_HEARTBEAT_MULTICAST_TTL,
	CASE_NETWORK_HEARTBEAT_PROTOCOL,
	CASE_NETWORK_HEARTBEAT_TLS_ADDRESS,
	CASE_NETWORK_HEARTBEAT_TLS_MESH_SEED_ADDRESS_PORT,
	CASE_NETWORK_HEARTBEAT_TLS_NAME,
	CASE_NETWORK_HEARTBEAT_TLS_PORT,
	// Obsoleted:
	CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS,

	// Network heartbeat mode options (value tokens):
	CASE_NETWORK_HEARTBEAT_MODE_MESH,
	CASE_NETWORK_HEARTBEAT_MODE_MULTICAST,

	// Network heartbeat protocol options (value tokens):
	CASE_NETWORK_HEARTBEAT_PROTOCOL_NONE,
	CASE_NETWORK_HEARTBEAT_PROTOCOL_V3,

	// Network fabric options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_FABRIC_ADDRESS,
	CASE_NETWORK_FABRIC_PORT,
	// Normally hidden:
	CASE_NETWORK_FABRIC_CHANNEL_BULK_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_BULK_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_CTRL_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_CTRL_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_META_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_META_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_RW_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS,
	CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED,
	CASE_NETWORK_FABRIC_KEEPALIVE_INTVL,
	CASE_NETWORK_FABRIC_KEEPALIVE_PROBES,
	CASE_NETWORK_FABRIC_KEEPALIVE_TIME,
	CASE_NETWORK_FABRIC_LATENCY_MAX_MS,
	CASE_NETWORK_FABRIC_RECV_REARM_THRESHOLD,
	CASE_NETWORK_FABRIC_SEND_THREADS,
	CASE_NETWORK_FABRIC_TLS_ADDRESS,
	CASE_NETWORK_FABRIC_TLS_NAME,
	CASE_NETWORK_FABRIC_TLS_PORT,

	// Network info options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_INFO_ADDRESS,
	CASE_NETWORK_INFO_PORT,
	// Deprecated:
	CASE_NETWORK_INFO_ENABLE_FASTPATH,

	// Network TLS options:
	CASE_NETWORK_TLS_CA_FILE,
	CASE_NETWORK_TLS_CA_PATH,
	CASE_NETWORK_TLS_CERT_BLACKLIST,
	CASE_NETWORK_TLS_CERT_FILE,
	CASE_NETWORK_TLS_CIPHER_SUITE,
	CASE_NETWORK_TLS_KEY_FILE,
	CASE_NETWORK_TLS_KEY_FILE_PASSWORD,
	CASE_NETWORK_TLS_PROTOCOLS,

	// Namespace options:
	// Normally visible, in canonical configuration file order:
	CASE_NAMESPACE_REPLICATION_FACTOR,
	CASE_NAMESPACE_LIMIT_SIZE, // renamed
	CASE_NAMESPACE_MEMORY_SIZE,
	CASE_NAMESPACE_DEFAULT_TTL,
	CASE_NAMESPACE_STORAGE_ENGINE_BEGIN,
	// For XDR only:
	CASE_NAMESPACE_ENABLE_XDR,
	CASE_NAMESPACE_SETS_ENABLE_XDR,
	CASE_NAMESPACE_XDR_REMOTE_DATACENTER,
	CASE_NAMESPACE_FORWARD_XDR_WRITES,
	CASE_NAMESPACE_ALLOW_NONXDR_WRITES,
	CASE_NAMESPACE_ALLOW_XDR_WRITES,
	// Normally hidden:
	CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY,
	CASE_NAMESPACE_DATA_IN_INDEX,
	CASE_NAMESPACE_DISABLE_COLD_START_EVICTION,
	CASE_NAMESPACE_DISABLE_WRITE_DUP_RES,
	CASE_NAMESPACE_DISALLOW_NULL_SETNAME,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_READ,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE,
	CASE_NAMESPACE_ENABLE_HIST_PROXY,
	CASE_NAMESPACE_EVICT_HIST_BUCKETS,
	CASE_NAMESPACE_EVICT_TENTHS_PCT,
	CASE_NAMESPACE_HIGH_WATER_DISK_PCT,
	CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT,
	CASE_NAMESPACE_INDEX_STAGE_SIZE,
	CASE_NAMESPACE_INDEX_TYPE_BEGIN,
	CASE_NAMESPACE_MIGRATE_ORDER,
	CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS,
	CASE_NAMESPACE_MIGRATE_SLEEP,
	CASE_NAMESPACE_NSUP_HIST_PERIOD,
	CASE_NAMESPACE_NSUP_PERIOD,
	CASE_NAMESPACE_NSUP_THREADS,
	CASE_NAMESPACE_PARTITION_TREE_SPRIGS,
	CASE_NAMESPACE_PREFER_UNIFORM_BALANCE,
	CASE_NAMESPACE_RACK_ID,
	CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE,
	CASE_NAMESPACE_SET_BEGIN,
	CASE_NAMESPACE_SINDEX_BEGIN,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN,
	CASE_NAMESPACE_SINGLE_BIN,
	CASE_NAMESPACE_STOP_WRITES_PCT,
	CASE_NAMESPACE_STRONG_CONSISTENCY,
	CASE_NAMESPACE_STRONG_CONSISTENCY_ALLOW_EXPUNGE,
	CASE_NAMESPACE_TOMB_RAIDER_ELIGIBLE_AGE,
	CASE_NAMESPACE_TOMB_RAIDER_PERIOD,
	CASE_NAMESPACE_TRANSACTION_PENDING_LIMIT,
	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE,
	// Obsoleted:
	CASE_NAMESPACE_DISABLE_NSUP,
	// Deprecated:
	CASE_NAMESPACE_ALLOW_VERSIONS,
	CASE_NAMESPACE_COLD_START_EVICT_TTL,
	CASE_NAMESPACE_DEMO_READ_MULTIPLIER,
	CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER,
	CASE_NAMESPACE_HIGH_WATER_PCT,
	CASE_NAMESPACE_LOW_WATER_PCT,
	CASE_NAMESPACE_MAX_TTL,
	CASE_NAMESPACE_OBJ_SIZE_HIST_MAX,
	CASE_NAMESPACE_PARTITION_TREE_LOCKS,
	CASE_NAMESPACE_SI_BEGIN,

	// Namespace conflict-resolution-policy options (value tokens):
	CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION,
	CASE_NAMESPACE_CONFLICT_RESOLUTION_LAST_UPDATE_TIME,

	// Namespace read consistency level options:
	CASE_NAMESPACE_READ_CONSISTENCY_ALL,
	CASE_NAMESPACE_READ_CONSISTENCY_OFF,
	CASE_NAMESPACE_READ_CONSISTENCY_ONE,

	// Namespace write commit level options:
	CASE_NAMESPACE_WRITE_COMMIT_ALL,
	CASE_NAMESPACE_WRITE_COMMIT_MASTER,
	CASE_NAMESPACE_WRITE_COMMIT_OFF,

	// Namespace index-type options (value tokens):
	CASE_NAMESPACE_INDEX_TYPE_SHMEM,
	CASE_NAMESPACE_INDEX_TYPE_PMEM,
	CASE_NAMESPACE_INDEX_TYPE_FLASH,

	// Namespace storage-engine options (value tokens):
	CASE_NAMESPACE_STORAGE_MEMORY,
	CASE_NAMESPACE_STORAGE_SSD,
	CASE_NAMESPACE_STORAGE_DEVICE,

	// Namespace index-type pmem options:
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT,
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT,

	// Namespace index-type flash options:
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT,
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT,

	// Namespace storage-engine device options:
	// Normally visible, in canonical configuration file order:
	CASE_NAMESPACE_STORAGE_DEVICE_DEVICE,
	CASE_NAMESPACE_STORAGE_DEVICE_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL, // renamed
	CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY,
	// Normally hidden:
	CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY,
	CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE,
	CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_MIN_SIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LEVEL,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM,
	CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES,
	CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_KEY_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS,
	CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE,
	CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER,
	CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP,
	// Obsoleted:
	CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT,
	CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC,
	// Deprecated:
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD,
	CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC,
	CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP,
	CASE_NAMESPACE_STORAGE_DEVICE_PERSIST,
	CASE_NAMESPACE_STORAGE_DEVICE_READONLY,
	CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS,

	// Namespace storage device compression options (value tokens):
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_NONE,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LZ4,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_SNAPPY,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ZSTD,

	// Namespace storage device encryption options (value tokens):
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_128,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_256,

	// Namespace set options:
	CASE_NAMESPACE_SET_DISABLE_EVICTION,
	CASE_NAMESPACE_SET_ENABLE_XDR,
	CASE_NAMESPACE_SET_STOP_WRITES_COUNT,
	// Deprecated:
	CASE_NAMESPACE_SET_EVICT_HWM_COUNT,
	CASE_NAMESPACE_SET_EVICT_HWM_PCT,
	CASE_NAMESPACE_SET_STOP_WRITE_COUNT,
	CASE_NAMESPACE_SET_STOP_WRITE_PCT,

	// Namespace set set-enable-xdr options (value tokens):
	CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT,
	CASE_NAMESPACE_SET_ENABLE_XDR_FALSE,
	CASE_NAMESPACE_SET_ENABLE_XDR_TRUE,

	// Namespace secondary-index options:
	// Deprecated:
	CASE_NAMESPACE_SI_GC_PERIOD,
	CASE_NAMESPACE_SI_GC_MAX_UNITS,
	CASE_NAMESPACE_SI_HISTOGRAM,
	CASE_NAMESPACE_SI_IGNORE_NOT_SYNC,

	// Namespace sindex options:
	CASE_NAMESPACE_SINDEX_NUM_PARTITIONS,

	// Namespace geo2dsphere within options:
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_STRICT,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MIN_LEVEL,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_LEVEL,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_CELLS,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_LEVEL_MOD,
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_EARTH_RADIUS_METERS,

	// Mod-lua options:
	CASE_MOD_LUA_CACHE_ENABLED,
	CASE_MOD_LUA_USER_PATH,
	// Deprecated:
	CASE_MOD_LUA_SYSTEM_PATH,

	// Security options:
	CASE_SECURITY_ENABLE_LDAP,
	CASE_SECURITY_ENABLE_SECURITY,
	CASE_SECURITY_LDAP_LOGIN_THREADS,
	CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD,
	CASE_SECURITY_LDAP_BEGIN,
	CASE_SECURITY_LOG_BEGIN,
	CASE_SECURITY_SYSLOG_BEGIN,

	// Security LDAP options:
	CASE_SECURITY_LDAP_DISABLE_TLS,
	CASE_SECURITY_LDAP_POLLING_PERIOD,
	CASE_SECURITY_LDAP_QUERY_BASE_DN,
	CASE_SECURITY_LDAP_QUERY_USER_DN,
	CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE,
	CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN,
	CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN,
	CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU,
	CASE_SECURITY_LDAP_SERVER,
	CASE_SECURITY_LDAP_SESSION_TTL,
	CASE_SECURITY_LDAP_TLS_CA_FILE,
	CASE_SECURITY_LDAP_TOKEN_HASH_METHOD,
	CASE_SECURITY_LDAP_USER_DN_PATTERN,
	CASE_SECURITY_LDAP_USER_QUERY_PATTERN,

	// Security LDAP token encryption options (value tokens):
	CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_256,
	CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_512,

	// Security (Aerospike) log options:
	CASE_SECURITY_LOG_REPORT_AUTHENTICATION,
	CASE_SECURITY_LOG_REPORT_DATA_OP,
	CASE_SECURITY_LOG_REPORT_SYS_ADMIN,
	CASE_SECURITY_LOG_REPORT_USER_ADMIN,
	CASE_SECURITY_LOG_REPORT_VIOLATION,

	// Security syslog options:
	CASE_SECURITY_SYSLOG_LOCAL,
	CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION,
	CASE_SECURITY_SYSLOG_REPORT_DATA_OP,
	CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN,
	CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN,
	CASE_SECURITY_SYSLOG_REPORT_VIOLATION,

	// XDR options:
	// Normally visible, in canonical configuration file order:
	CASE_XDR_ENABLE_XDR,
	CASE_XDR_ENABLE_CHANGE_NOTIFICATION,
	CASE_XDR_DIGESTLOG_PATH,
	CASE_XDR_DATACENTER_BEGIN,
	// Normally hidden:
	CASE_XDR_CLIENT_THREADS,
	CASE_XDR_COMPRESSION_THRESHOLD,
	CASE_XDR_DELETE_SHIPPING_ENABLED,
	CASE_XDR_DIGESTLOG_IOWAIT_MS,
	CASE_XDR_FORWARD_XDR_WRITES,
	CASE_XDR_HOTKEY_TIME_MS,
	CASE_XDR_INFO_PORT,
	CASE_XDR_INFO_TIMEOUT,
	CASE_XDR_MAX_SHIP_BANDWIDTH,
	CASE_XDR_MAX_SHIP_THROUGHPUT,
	CASE_XDR_MIN_DIGESTLOG_FREE_PCT,
	CASE_XDR_NSUP_DELETES_ENABLED,
	CASE_XDR_READ_THREADS,
	CASE_XDR_SHIP_BINS,
	CASE_XDR_SHIP_DELAY,
	CASE_XDR_SHIPPING_ENABLED,
	CASE_XDR_WRITE_TIMEOUT,

	// XDR (remote) datacenter options:
	// Normally visible, in canonical configuration file order:
	CASE_XDR_DATACENTER_DC_NODE_ADDRESS_PORT,
	// Normally hidden:
	CASE_XDR_DATACENTER_DC_CONNECTIONS,
	CASE_XDR_DATACENTER_DC_CONNECTIONS_IDLE_MS,
	CASE_XDR_DATACENTER_DC_INT_EXT_IPMAP,
	CASE_XDR_DATACENTER_DC_SECURITY_CONFIG_FILE,
	CASE_XDR_DATACENTER_DC_SHIP_BINS,
	CASE_XDR_DATACENTER_DC_TYPE,
	CASE_XDR_DATACENTER_DC_USE_ALTERNATE_SERVICES,
	CASE_XDR_DATACENTER_HTTP_URL,
	CASE_XDR_DATACENTER_HTTP_VERSION,
	CASE_XDR_DATACENTER_TLS_NAME,
	CASE_XDR_DATACENTER_TLS_NODE,

	// Used parsing separate file, but share this enum:

	// XDR security top-level options:
	XDR_SEC_CASE_CREDENTIALS_BEGIN,

	// XDR security credentials options:
	// Normally visible, in canonical configuration file order:
	XDR_SEC_CASE_CREDENTIALS_USERNAME,
	XDR_SEC_CASE_CREDENTIALS_PASSWORD

} cfg_case_id;


//==========================================================
// All configuration items must appear below as a cfg_opt
// struct in the appropriate array. Order within an array is
// not important, other than for organizational sanity.
//

typedef struct cfg_opt_s {
	const char*	tok;
	cfg_case_id	case_id;
} cfg_opt;

const cfg_opt GLOBAL_OPTS[] = {
		{ "service",						CASE_SERVICE_BEGIN },
		{ "logging",						CASE_LOGGING_BEGIN },
		{ "network",						CASE_NETWORK_BEGIN },
		{ "namespace",						CASE_NAMESPACE_BEGIN },
		{ "mod-lua",						CASE_MOD_LUA_BEGIN },
		{ "cluster",						CASE_CLUSTER_BEGIN },
		{ "security",						CASE_SECURITY_BEGIN },
		{ "xdr",							CASE_XDR_BEGIN }
};

const cfg_opt SERVICE_OPTS[] = {
		{ "user",							CASE_SERVICE_USER },
		{ "group",							CASE_SERVICE_GROUP },
		{ "paxos-single-replica-limit",		CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT },
		{ "pidfile",						CASE_SERVICE_PIDFILE },
		{ "client-fd-max",					CASE_SERVICE_CLIENT_FD_MAX },
		{ "proto-fd-max",					CASE_SERVICE_PROTO_FD_MAX },
		{ "advertise-ipv6",					CASE_SERVICE_ADVERTISE_IPV6 },
		{ "auto-pin",						CASE_SERVICE_AUTO_PIN },
		{ "batch-index-threads",			CASE_SERVICE_BATCH_INDEX_THREADS },
		{ "batch-max-buffers-per-queue",	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE },
		{ "batch-max-requests",				CASE_SERVICE_BATCH_MAX_REQUESTS },
		{ "batch-max-unused-buffers",		CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS },
		{ "cluster-name",					CASE_SERVICE_CLUSTER_NAME },
		{ "enable-benchmarks-fabric",		CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC },
		{ "enable-benchmarks-svc",			CASE_SERVICE_ENABLE_BENCHMARKS_SVC },
		{ "enable-health-check",			CASE_SERVICE_ENABLE_HEALTH_CHECK },
		{ "enable-hist-info",				CASE_SERVICE_ENABLE_HIST_INFO },
		{ "feature-key-file",				CASE_SERVICE_FEATURE_KEY_FILE },
		{ "hist-track-back",				CASE_SERVICE_HIST_TRACK_BACK },
		{ "hist-track-slice",				CASE_SERVICE_HIST_TRACK_SLICE },
		{ "hist-track-thresholds",			CASE_SERVICE_HIST_TRACK_THRESHOLDS },
		{ "info-threads",					CASE_SERVICE_INFO_THREADS },
		{ "keep-caps-ssd-health",			CASE_SERVICE_KEEP_CAPS_SSD_HEALTH },
		{ "log-local-time",					CASE_SERVICE_LOG_LOCAL_TIME },
		{ "log-millis",						CASE_SERVICE_LOG_MILLIS},
		{ "migrate-fill-delay",				CASE_SERVICE_MIGRATE_FILL_DELAY },
		{ "migrate-max-num-incoming",		CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING },
		{ "migrate-threads",				CASE_SERVICE_MIGRATE_THREADS },
		{ "min-cluster-size",				CASE_SERVICE_MIN_CLUSTER_SIZE },
		{ "node-id",						CASE_SERVICE_NODE_ID },
		{ "node-id-interface",				CASE_SERVICE_NODE_ID_INTERFACE },
		{ "proto-fd-idle-ms",				CASE_SERVICE_PROTO_FD_IDLE_MS },
		{ "query-batch-size",				CASE_SERVICE_QUERY_BATCH_SIZE },
		{ "query-bufpool-size",				CASE_SERVICE_QUERY_BUFPOOL_SIZE },
		{ "query-in-transaction-thread",	CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD },
		{ "query-long-q-max-size",			CASE_SERVICE_QUERY_LONG_Q_MAX_SIZE },
		{ "query-pre-reserve-partitions",   CASE_SERVICE_QUERY_PRE_RESERVE_PARTITIONS },
		{ "query-priority", 				CASE_SERVICE_QUERY_PRIORITY },
		{ "query-priority-sleep-us", 		CASE_SERVICE_QUERY_PRIORITY_SLEEP_US },
		{ "query-rec-count-bound",			CASE_SERVICE_QUERY_REC_COUNT_BOUND },
		{ "query-req-in-query-thread",		CASE_SERVICE_QUERY_REQ_IN_QUERY_THREAD },
		{ "query-req-max-inflight",			CASE_SERVICE_QUERY_REQ_MAX_INFLIGHT },
		{ "query-short-q-max-size",			CASE_SERVICE_QUERY_SHORT_Q_MAX_SIZE },
		{ "query-threads",					CASE_SERVICE_QUERY_THREADS },
		{ "query-threshold", 				CASE_SERVICE_QUERY_THRESHOLD },
		{ "query-untracked-time-ms",		CASE_SERVICE_QUERY_UNTRACKED_TIME_MS },
		{ "query-worker-threads",			CASE_SERVICE_QUERY_WORKER_THREADS },
		{ "run-as-daemon",					CASE_SERVICE_RUN_AS_DAEMON },
		{ "scan-max-active",				CASE_SERVICE_SCAN_MAX_ACTIVE },
		{ "scan-max-done",					CASE_SERVICE_SCAN_MAX_DONE },
		{ "scan-max-udf-transactions",		CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS },
		{ "scan-threads",					CASE_SERVICE_SCAN_THREADS },
		{ "service-threads",				CASE_SERVICE_SERVICE_THREADS },
		{ "sindex-builder-threads",			CASE_SERVICE_SINDEX_BUILDER_THREADS },
		{ "sindex-gc-max-rate",				CASE_SERVICE_SINDEX_GC_MAX_RATE },
		{ "sindex-gc-period",				CASE_SERVICE_SINDEX_GC_PERIOD },
		{ "ticker-interval",				CASE_SERVICE_TICKER_INTERVAL },
		{ "transaction-max-ms",				CASE_SERVICE_TRANSACTION_MAX_MS },
		{ "transaction-queues",				CASE_SERVICE_TRANSACTION_QUEUES },
		{ "transaction-retry-ms",			CASE_SERVICE_TRANSACTION_RETRY_MS },
		{ "transaction-threads-per-queue",	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE },
		{ "work-directory",					CASE_SERVICE_WORK_DIRECTORY },
		{ "debug-allocations",				CASE_SERVICE_DEBUG_ALLOCATIONS },
		{ "fabric-dump-msgs",				CASE_SERVICE_FABRIC_DUMP_MSGS },
		{ "allow-inline-transactions",		CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS },
		{ "nsup-period",					CASE_SERVICE_NSUP_PERIOD },
		{ "object-size-hist-period",		CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD },
		{ "respond-client-on-master-completion", CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION },
		{ "transaction-pending-limit",		CASE_SERVICE_TRANSACTION_PENDING_LIMIT },
		{ "transaction-repeatable-read",	CASE_SERVICE_TRANSACTION_REPEATABLE_READ },
		{ "auto-dun",						CASE_SERVICE_AUTO_DUN },
		{ "auto-undun",						CASE_SERVICE_AUTO_UNDUN },
		{ "batch-priority",					CASE_SERVICE_BATCH_PRIORITY },
		{ "batch-retransmit",				CASE_SERVICE_BATCH_RETRANSMIT },
		{ "batch-threads",					CASE_SERVICE_BATCH_THREADS },
		{ "clib-library",					CASE_SERVICE_CLIB_LIBRARY },
		{ "defrag-queue-escape",			CASE_SERVICE_DEFRAG_QUEUE_ESCAPE },
		{ "defrag-queue-hwm",				CASE_SERVICE_DEFRAG_QUEUE_HWM },
		{ "defrag-queue-lwm",				CASE_SERVICE_DEFRAG_QUEUE_LWM },
		{ "defrag-queue-priority",			CASE_SERVICE_DEFRAG_QUEUE_PRIORITY },
		{ "dump-message-above-size",		CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE },
		{ "fabric-workers",					CASE_SERVICE_FABRIC_WORKERS },
		{ "fb-health-bad-pct",				CASE_SERVICE_FB_HEALTH_BAD_PCT },
		{ "fb-health-good-pct",				CASE_SERVICE_FB_HEALTH_GOOD_PCT },
		{ "fb-health-msg-per-burst",		CASE_SERVICE_FB_HEALTH_MSG_PER_BURST },
		{ "fb-health-msg-timeout",			CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT },
		{ "generation-disable",				CASE_SERVICE_GENERATION_DISABLE },
		{ "max-msgs-per-type",				CASE_SERVICE_MAX_MSGS_PER_TYPE },
		{ "migrate-read-priority",			CASE_SERVICE_MIGRATE_READ_PRIORITY },
		{ "migrate-read-sleep",				CASE_SERVICE_MIGRATE_READ_SLEEP },
		{ "migrate-rx-lifetime-ms",			CASE_SERVICE_MIGRATE_RX_LIFETIME_MS },
		{ "migrate-xmit-hwm",				CASE_SERVICE_MIGRATE_XMIT_HWM },
		{ "migrate-xmit-lwm",				CASE_SERVICE_MIGRATE_XMIT_LWM },
		{ "migrate-priority",				CASE_SERVICE_MIGRATE_PRIORITY },
		{ "migrate-xmit-priority",			CASE_SERVICE_MIGRATE_XMIT_PRIORITY },
		{ "migrate-xmit-sleep",				CASE_SERVICE_MIGRATE_XMIT_SLEEP },
		{ "nsup-auto-hwm",					CASE_SERVICE_NSUP_AUTO_HWM },
		{ "nsup-auto-hwm-pct",				CASE_SERVICE_NSUP_AUTO_HWM_PCT },
		{ "nsup-delete-sleep",				CASE_SERVICE_NSUP_DELETE_SLEEP },
		{ "nsup-max-deletes",				CASE_SERVICE_NSUP_MAX_DELETES },
		{ "nsup-queue-escape",				CASE_SERVICE_NSUP_QUEUE_ESCAPE },
		{ "nsup-queue-hwm",					CASE_SERVICE_NSUP_QUEUE_HWM },
		{ "nsup-queue-lwm",					CASE_SERVICE_NSUP_QUEUE_LWM },
		{ "nsup-reduce-priority",			CASE_SERVICE_NSUP_REDUCE_PRIORITY },
		{ "nsup-reduce-sleep",				CASE_SERVICE_NSUP_REDUCE_SLEEP },
		{ "nsup-startup-evict",				CASE_SERVICE_NSUP_STARTUP_EVICT },
		{ "nsup-threads",					CASE_SERVICE_NSUP_THREADS },
		{ "paxos-max-cluster-size",			CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE },
		{ "paxos-protocol",					CASE_SERVICE_PAXOS_PROTOCOL },
		{ "paxos-recovery-policy",			CASE_SERVICE_PAXOS_RECOVERY_POLICY },
		{ "paxos-retransmit-period",		CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD },
		{ "prole-extra-ttl",				CASE_SERVICE_PROLE_EXTRA_TTL },
		{ "replication-fire-and-forget",	CASE_SERVICE_REPLICATION_FIRE_AND_FORGET },
		{ "scan-memory",					CASE_SERVICE_SCAN_MEMORY },
		{ "scan-priority",					CASE_SERVICE_SCAN_PRIORITY },
		{ "scan-retransmit",				CASE_SERVICE_SCAN_RETRANSMIT },
		{ "scheduler-priority",				CASE_SERVICE_SCHEDULER_PRIORITY },
		{ "scheduler-type",					CASE_SERVICE_SCHEDULER_TYPE },
		{ "transaction-duplicate-threads",	CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS },
		{ "trial-account-key",				CASE_SERVICE_TRIAL_ACCOUNT_KEY },
		{ "udf-runtime-max-gmemory",		CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY },
		{ "udf-runtime-max-memory",			CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY },
		{ "use-queue-per-device",			CASE_SERVICE_USE_QUEUE_PER_DEVICE },
		{ "write-duplicate-resolution-disable", CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SERVICE_AUTO_PIN_OPTS[] = {
		{ "none",							CASE_SERVICE_AUTO_PIN_NONE },
		{ "cpu",							CASE_SERVICE_AUTO_PIN_CPU },
		{ "numa",							CASE_SERVICE_AUTO_PIN_NUMA }
};

const cfg_opt SERVICE_DEBUG_ALLOCATIONS_OPTS[] = {
		{ "none",							CASE_SERVICE_DEBUG_ALLOCATIONS_NONE },
		{ "transient",						CASE_SERVICE_DEBUG_ALLOCATIONS_TRANSIENT },
		{ "persistent",						CASE_SERVICE_DEBUG_ALLOCATIONS_PERSISTENT },
		{ "all",							CASE_SERVICE_DEBUG_ALLOCATIONS_ALL }
};

const cfg_opt LOGGING_OPTS[] = {
		{ "file",							CASE_LOG_FILE_BEGIN },
		{ "console",						CASE_LOG_CONSOLE_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_FILE_OPTS[] = {
		{ "context",						CASE_LOG_FILE_CONTEXT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_CONSOLE_OPTS[] = {
		{ "context",						CASE_LOG_CONSOLE_CONTEXT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_OPTS[] = {
		{ "service",						CASE_NETWORK_SERVICE_BEGIN },
		{ "heartbeat",						CASE_NETWORK_HEARTBEAT_BEGIN },
		{ "fabric",							CASE_NETWORK_FABRIC_BEGIN },
		{ "info",							CASE_NETWORK_INFO_BEGIN },
		{ "tls",							CASE_NETWORK_TLS_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_SERVICE_OPTS[] = {
		{ "address",						CASE_NETWORK_SERVICE_ADDRESS },
		{ "port",							CASE_NETWORK_SERVICE_PORT },
		{ "external-address",				CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS },
		{ "access-address",					CASE_NETWORK_SERVICE_ACCESS_ADDRESS },
		{ "access-port",					CASE_NETWORK_SERVICE_ACCESS_PORT },
		{ "alternate-access-address",		CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS },
		{ "alternate-access-port",			CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT },
		{ "tls-access-address",				CASE_NETWORK_SERVICE_TLS_ACCESS_ADDRESS },
		{ "tls-access-port",				CASE_NETWORK_SERVICE_TLS_ACCESS_PORT },
		{ "tls-address",					CASE_NETWORK_SERVICE_TLS_ADDRESS },
		{ "tls-alternate-access-address",	CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_ADDRESS },
		{ "tls-alternate-access-port",		CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_PORT },
		{ "tls-authenticate-client",		CASE_NETWORK_SERVICE_TLS_AUTHENTICATE_CLIENT },
		{ "tls-name",						CASE_NETWORK_SERVICE_TLS_NAME },
		{ "tls-port",						CASE_NETWORK_SERVICE_TLS_PORT },
		{ "alternate-address",				CASE_NETWORK_SERVICE_ALTERNATE_ADDRESS },
		{ "network-interface-name",			CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME },
		{ "reuse-address",					CASE_NETWORK_SERVICE_REUSE_ADDRESS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_HEARTBEAT_OPTS[] = {
		{ "mode",							CASE_NETWORK_HEARTBEAT_MODE },
		{ "address",						CASE_NETWORK_HEARTBEAT_ADDRESS },
		{ "multicast-group",				CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP },
		{ "port",							CASE_NETWORK_HEARTBEAT_PORT },
		{ "mesh-seed-address-port",			CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT },
		{ "interval",						CASE_NETWORK_HEARTBEAT_INTERVAL },
		{ "timeout",						CASE_NETWORK_HEARTBEAT_TIMEOUT },
		{ "mtu",							CASE_NETWORK_HEARTBEAT_MTU },
		{ "mcast-ttl",						CASE_NETWORK_HEARTBEAT_MCAST_TTL },
		{ "multicast-ttl",					CASE_NETWORK_HEARTBEAT_MULTICAST_TTL },
		{ "protocol",						CASE_NETWORK_HEARTBEAT_PROTOCOL },
		{ "tls-address",					CASE_NETWORK_HEARTBEAT_TLS_ADDRESS },
		{ "tls-mesh-seed-address-port",		CASE_NETWORK_HEARTBEAT_TLS_MESH_SEED_ADDRESS_PORT },
		{ "tls-name",						CASE_NETWORK_HEARTBEAT_TLS_NAME },
		{ "tls-port",						CASE_NETWORK_HEARTBEAT_TLS_PORT },
		{ "interface-address",				CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_HEARTBEAT_MODE_OPTS[] = {
		{ "mesh",							CASE_NETWORK_HEARTBEAT_MODE_MESH },
		{ "multicast",						CASE_NETWORK_HEARTBEAT_MODE_MULTICAST }
};

const cfg_opt NETWORK_HEARTBEAT_PROTOCOL_OPTS[] = {
		{ "none",							CASE_NETWORK_HEARTBEAT_PROTOCOL_NONE },
		{ "v3",								CASE_NETWORK_HEARTBEAT_PROTOCOL_V3}
};

const cfg_opt NETWORK_FABRIC_OPTS[] = {
		{ "address",						CASE_NETWORK_FABRIC_ADDRESS },
		{ "port",							CASE_NETWORK_FABRIC_PORT },
		{ "channel-bulk-fds",				CASE_NETWORK_FABRIC_CHANNEL_BULK_FDS },
		{ "channel-bulk-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_BULK_RECV_THREADS },
		{ "channel-ctrl-fds",				CASE_NETWORK_FABRIC_CHANNEL_CTRL_FDS },
		{ "channel-ctrl-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_CTRL_RECV_THREADS },
		{ "channel-meta-fds",				CASE_NETWORK_FABRIC_CHANNEL_META_FDS },
		{ "channel-meta-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_META_RECV_THREADS },
		{ "channel-rw-fds",					CASE_NETWORK_FABRIC_CHANNEL_RW_FDS },
		{ "channel-rw-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS },
		{ "keepalive-enabled",				CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED },
		{ "keepalive-intvl",				CASE_NETWORK_FABRIC_KEEPALIVE_INTVL },
		{ "keepalive-probes",				CASE_NETWORK_FABRIC_KEEPALIVE_PROBES },
		{ "keepalive-time",					CASE_NETWORK_FABRIC_KEEPALIVE_TIME },
		{ "latency-max-ms",					CASE_NETWORK_FABRIC_LATENCY_MAX_MS },
		{ "recv-rearm-threshold",			CASE_NETWORK_FABRIC_RECV_REARM_THRESHOLD },
		{ "send-threads",					CASE_NETWORK_FABRIC_SEND_THREADS },
		{ "tls-address",					CASE_NETWORK_FABRIC_TLS_ADDRESS },
		{ "tls-name",						CASE_NETWORK_FABRIC_TLS_NAME },
		{ "tls-port",						CASE_NETWORK_FABRIC_TLS_PORT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_INFO_OPTS[] = {
		{ "address",						CASE_NETWORK_INFO_ADDRESS },
		{ "port",							CASE_NETWORK_INFO_PORT },
		{ "enable-fastpath",				CASE_NETWORK_INFO_ENABLE_FASTPATH },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_TLS_OPTS[] = {
		{ "ca-file",						CASE_NETWORK_TLS_CA_FILE },
		{ "ca-path",						CASE_NETWORK_TLS_CA_PATH },
		{ "cert-blacklist",					CASE_NETWORK_TLS_CERT_BLACKLIST },
		{ "cert-file",						CASE_NETWORK_TLS_CERT_FILE },
		{ "cipher-suite",					CASE_NETWORK_TLS_CIPHER_SUITE },
		{ "key-file",						CASE_NETWORK_TLS_KEY_FILE },
		{ "key-file-password",				CASE_NETWORK_TLS_KEY_FILE_PASSWORD },
		{ "protocols",						CASE_NETWORK_TLS_PROTOCOLS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_OPTS[] = {
		{ "replication-factor",				CASE_NAMESPACE_REPLICATION_FACTOR },
		{ "limit-size",						CASE_NAMESPACE_LIMIT_SIZE },
		{ "memory-size",					CASE_NAMESPACE_MEMORY_SIZE },
		{ "default-ttl",					CASE_NAMESPACE_DEFAULT_TTL },
		{ "storage-engine",					CASE_NAMESPACE_STORAGE_ENGINE_BEGIN },
		{ "enable-xdr",						CASE_NAMESPACE_ENABLE_XDR },
		{ "sets-enable-xdr",				CASE_NAMESPACE_SETS_ENABLE_XDR },
		{ "xdr-remote-datacenter",			CASE_NAMESPACE_XDR_REMOTE_DATACENTER },
		{ "ns-forward-xdr-writes",			CASE_NAMESPACE_FORWARD_XDR_WRITES },
		{ "allow-nonxdr-writes",			CASE_NAMESPACE_ALLOW_NONXDR_WRITES },
		{ "allow-xdr-writes",				CASE_NAMESPACE_ALLOW_XDR_WRITES },
		{ "conflict-resolution-policy",		CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY },
		{ "data-in-index",					CASE_NAMESPACE_DATA_IN_INDEX },
		{ "disable-cold-start-eviction",	CASE_NAMESPACE_DISABLE_COLD_START_EVICTION },
		{ "disable-write-dup-res",			CASE_NAMESPACE_DISABLE_WRITE_DUP_RES },
		{ "disallow-null-setname",			CASE_NAMESPACE_DISALLOW_NULL_SETNAME },
		{ "enable-benchmarks-batch-sub",	CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB },
		{ "enable-benchmarks-read",			CASE_NAMESPACE_ENABLE_BENCHMARKS_READ },
		{ "enable-benchmarks-udf",			CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF },
		{ "enable-benchmarks-udf-sub",		CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB },
		{ "enable-benchmarks-write",		CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE },
		{ "enable-hist-proxy",				CASE_NAMESPACE_ENABLE_HIST_PROXY },
		{ "evict-hist-buckets",				CASE_NAMESPACE_EVICT_HIST_BUCKETS },
		{ "evict-tenths-pct",				CASE_NAMESPACE_EVICT_TENTHS_PCT },
		{ "high-water-disk-pct",			CASE_NAMESPACE_HIGH_WATER_DISK_PCT },
		{ "high-water-memory-pct",			CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT },
		{ "index-stage-size",				CASE_NAMESPACE_INDEX_STAGE_SIZE },
		{ "index-type",						CASE_NAMESPACE_INDEX_TYPE_BEGIN },
		{ "migrate-order",					CASE_NAMESPACE_MIGRATE_ORDER },
		{ "migrate-retransmit-ms",			CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS },
		{ "migrate-sleep",					CASE_NAMESPACE_MIGRATE_SLEEP },
		{ "nsup-hist-period",				CASE_NAMESPACE_NSUP_HIST_PERIOD },
		{ "nsup-period",					CASE_NAMESPACE_NSUP_PERIOD },
		{ "nsup-threads",					CASE_NAMESPACE_NSUP_THREADS },
		{ "partition-tree-sprigs",			CASE_NAMESPACE_PARTITION_TREE_SPRIGS },
		{ "prefer-uniform-balance",			CASE_NAMESPACE_PREFER_UNIFORM_BALANCE },
		{ "rack-id",						CASE_NAMESPACE_RACK_ID },
		{ "read-consistency-level-override", CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE },
		{ "set",							CASE_NAMESPACE_SET_BEGIN },
		{ "sindex",							CASE_NAMESPACE_SINDEX_BEGIN },
		{ "geo2dsphere-within",				CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN },
		{ "single-bin",						CASE_NAMESPACE_SINGLE_BIN },
		{ "stop-writes-pct",				CASE_NAMESPACE_STOP_WRITES_PCT },
		{ "strong-consistency",				CASE_NAMESPACE_STRONG_CONSISTENCY },
		{ "strong-consistency-allow-expunge", CASE_NAMESPACE_STRONG_CONSISTENCY_ALLOW_EXPUNGE },
		{ "tomb-raider-eligible-age",		CASE_NAMESPACE_TOMB_RAIDER_ELIGIBLE_AGE },
		{ "tomb-raider-period",				CASE_NAMESPACE_TOMB_RAIDER_PERIOD },
		{ "transaction-pending-limit",		CASE_NAMESPACE_TRANSACTION_PENDING_LIMIT },
		{ "write-commit-level-override",	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE },
		{ "disable-nsup",					CASE_NAMESPACE_DISABLE_NSUP },
		{ "allow-versions",					CASE_NAMESPACE_ALLOW_VERSIONS },
		{ "cold-start-evict-ttl",			CASE_NAMESPACE_COLD_START_EVICT_TTL },
		{ "demo-read-multiplier",			CASE_NAMESPACE_DEMO_READ_MULTIPLIER },
		{ "demo-write-multiplier",			CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER },
		{ "high-water-pct",					CASE_NAMESPACE_HIGH_WATER_PCT },
		{ "low-water-pct",					CASE_NAMESPACE_LOW_WATER_PCT },
		{ "max-ttl",						CASE_NAMESPACE_MAX_TTL },
		{ "obj-size-hist-max",				CASE_NAMESPACE_OBJ_SIZE_HIST_MAX },
		{ "partition-tree-locks",			CASE_NAMESPACE_PARTITION_TREE_LOCKS },
		{ "si",								CASE_NAMESPACE_SI_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_CONFLICT_RESOLUTION_OPTS[] = {
		{ "generation",						CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION },
		{ "last-update-time",				CASE_NAMESPACE_CONFLICT_RESOLUTION_LAST_UPDATE_TIME }
};

const cfg_opt NAMESPACE_READ_CONSISTENCY_OPTS[] = {
		{ "all",							CASE_NAMESPACE_READ_CONSISTENCY_ALL },
		{ "off",							CASE_NAMESPACE_READ_CONSISTENCY_OFF },
		{ "one",							CASE_NAMESPACE_READ_CONSISTENCY_ONE }
};

const cfg_opt NAMESPACE_WRITE_COMMIT_OPTS[] = {
		{ "all",							CASE_NAMESPACE_WRITE_COMMIT_ALL },
		{ "master",							CASE_NAMESPACE_WRITE_COMMIT_MASTER },
		{ "off",							CASE_NAMESPACE_WRITE_COMMIT_OFF }
};

const cfg_opt NAMESPACE_INDEX_TYPE_OPTS[] = {
		{ "shmem",							CASE_NAMESPACE_INDEX_TYPE_SHMEM },
		{ "pmem",							CASE_NAMESPACE_INDEX_TYPE_PMEM },
		{ "flash",							CASE_NAMESPACE_INDEX_TYPE_FLASH }
};

const cfg_opt NAMESPACE_STORAGE_OPTS[] = {
		{ "memory",							CASE_NAMESPACE_STORAGE_MEMORY },
		{ "ssd",							CASE_NAMESPACE_STORAGE_SSD },
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE }
};

const cfg_opt NAMESPACE_INDEX_TYPE_PMEM_OPTS[] = {
		{ "mount",							CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT },
		{ "mounts-high-water-pct",			CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_INDEX_TYPE_FLASH_OPTS[] = {
		{ "mount",							CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT },
		{ "mounts-high-water-pct",			CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_DEVICE_OPTS[] = {
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE_DEVICE },
		{ "file",							CASE_NAMESPACE_STORAGE_DEVICE_FILE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE },
		{ "scheduler-mode",					CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE },
		{ "write-block-size",				CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE },
		{ "memory-all",						CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL },
		{ "data-in-memory",					CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY },
		{ "cold-start-empty",				CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY },
		{ "commit-to-device",				CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE },
		{ "commit-min-size",				CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_MIN_SIZE },
		{ "compression",					CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION },
		{ "compression-level",				CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LEVEL },
		{ "defrag-lwm-pct",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT },
		{ "defrag-queue-min",				CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN },
		{ "defrag-sleep",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP },
		{ "defrag-startup-minimum",			CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM },
		{ "direct-files",					CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES },
		{ "enable-benchmarks-storage",		CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE },
		{ "encryption",						CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION },
		{ "encryption-key-file",			CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_KEY_FILE },
		{ "flush-max-ms",					CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS },
		{ "max-write-cache",				CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE },
		{ "min-avail-pct",					CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT },
		{ "post-write-queue",				CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE },
		{ "read-page-cache",				CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE },
		{ "serialize-tomb-raider",			CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER },
		{ "tomb-raider-sleep",				CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP },
		{ "disable-odirect",				CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT },
		{ "fsync-max-sec",					CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC },
		{ "defrag-max-blocks",				CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS },
		{ "defrag-period",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD },
		{ "enable-osync",					CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC },
		{ "load-at-startup",				CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP },
		{ "persist",						CASE_NAMESPACE_STORAGE_DEVICE_PERSIST },
		{ "readonly",						CASE_NAMESPACE_STORAGE_DEVICE_READONLY },
		{ "signature",						CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE },
		{ "write-smoothing-period",			CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD },
		{ "write-threads",					CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_DEVICE_COMPRESSION_OPTS[] = {
		{ "none",							CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_NONE },
		{ "lz4",							CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LZ4 },
		{ "snappy",							CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_SNAPPY },
		{ "zstd",							CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ZSTD }
};

const cfg_opt NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OPTS[] = {
		{ "aes-128",						CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_128 },
		{ "aes-256",						CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_256 }
};

const cfg_opt NAMESPACE_SET_OPTS[] = {
		{ "set-disable-eviction",			CASE_NAMESPACE_SET_DISABLE_EVICTION },
		{ "set-enable-xdr",					CASE_NAMESPACE_SET_ENABLE_XDR },
		{ "set-stop-writes-count",			CASE_NAMESPACE_SET_STOP_WRITES_COUNT },
		{ "set-evict-hwm-count",			CASE_NAMESPACE_SET_EVICT_HWM_COUNT },
		{ "set-evict-hwm-pct",				CASE_NAMESPACE_SET_EVICT_HWM_PCT },
		{ "set-stop-write-count",			CASE_NAMESPACE_SET_STOP_WRITE_COUNT },
		{ "set-stop-write-pct",				CASE_NAMESPACE_SET_STOP_WRITE_PCT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SET_ENABLE_XDR_OPTS[] = {
		{ "use-default",					CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT },
		{ "false",							CASE_NAMESPACE_SET_ENABLE_XDR_FALSE },
		{ "true",							CASE_NAMESPACE_SET_ENABLE_XDR_TRUE }
};

const cfg_opt NAMESPACE_SI_OPTS[] = {
		{ "si-gc-period",					CASE_NAMESPACE_SI_GC_PERIOD },
		{ "si-gc-max-units",				CASE_NAMESPACE_SI_GC_MAX_UNITS },
		{ "si-histogram",					CASE_NAMESPACE_SI_HISTOGRAM },
		{ "si-ignore-not-sync",				CASE_NAMESPACE_SI_IGNORE_NOT_SYNC },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SINDEX_OPTS[] = {
		{ "num-partitions",					CASE_NAMESPACE_SINDEX_NUM_PARTITIONS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_GEO2DSPHERE_WITHIN_OPTS[] = {
		{ "strict",							CASE_NAMESPACE_GEO2DSPHERE_WITHIN_STRICT },
		{ "min-level",						CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MIN_LEVEL },
		{ "max-level",						CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_LEVEL },
		{ "max-cells",						CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_CELLS },
		{ "level-mod",						CASE_NAMESPACE_GEO2DSPHERE_WITHIN_LEVEL_MOD },
		{ "earth-radius-meters",			CASE_NAMESPACE_GEO2DSPHERE_WITHIN_EARTH_RADIUS_METERS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt MOD_LUA_OPTS[] = {
		{ "cache-enabled",					CASE_MOD_LUA_CACHE_ENABLED },
		{ "user-path",						CASE_MOD_LUA_USER_PATH },
		{ "system-path",					CASE_MOD_LUA_SYSTEM_PATH },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_OPTS[] = {
		{ "enable-ldap",					CASE_SECURITY_ENABLE_LDAP },
		{ "enable-security",				CASE_SECURITY_ENABLE_SECURITY },
		{ "ldap-login-threads",				CASE_SECURITY_LDAP_LOGIN_THREADS },
		{ "privilege-refresh-period",		CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD },
		{ "ldap",							CASE_SECURITY_LDAP_BEGIN },
		{ "log",							CASE_SECURITY_LOG_BEGIN },
		{ "syslog",							CASE_SECURITY_SYSLOG_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_LDAP_OPTS[] = {
		{ "disable-tls",					CASE_SECURITY_LDAP_DISABLE_TLS },
		{ "polling-period",					CASE_SECURITY_LDAP_POLLING_PERIOD },
		{ "query-base-dn",					CASE_SECURITY_LDAP_QUERY_BASE_DN },
		{ "query-user-dn",					CASE_SECURITY_LDAP_QUERY_USER_DN },
		{ "query-user-password-file",		CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE },
		{ "role-query-base-dn",				CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN },
		{ "role-query-pattern",				CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN },
		{ "role-query-search-ou",			CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU },
		{ "server",							CASE_SECURITY_LDAP_SERVER },
		{ "session-ttl",					CASE_SECURITY_LDAP_SESSION_TTL },
		{ "tls-ca-file",					CASE_SECURITY_LDAP_TLS_CA_FILE },
		{ "token-hash-method",				CASE_SECURITY_LDAP_TOKEN_HASH_METHOD },
		{ "user-dn-pattern",				CASE_SECURITY_LDAP_USER_DN_PATTERN },
		{ "user-query-pattern",				CASE_SECURITY_LDAP_USER_QUERY_PATTERN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS[] = {
		{ "sha-256",						CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_256 },
		{ "sha-512",						CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_512 }
};

const cfg_opt SECURITY_LOG_OPTS[] = {
		{ "report-authentication",			CASE_SECURITY_LOG_REPORT_AUTHENTICATION },
		{ "report-data-op",					CASE_SECURITY_LOG_REPORT_DATA_OP },
		{ "report-sys-admin",				CASE_SECURITY_LOG_REPORT_SYS_ADMIN },
		{ "report-user-admin",				CASE_SECURITY_LOG_REPORT_USER_ADMIN },
		{ "report-violation",				CASE_SECURITY_LOG_REPORT_VIOLATION },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_SYSLOG_OPTS[] = {
		{ "local",							CASE_SECURITY_SYSLOG_LOCAL },
		{ "report-authentication",			CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION },
		{ "report-data-op",					CASE_SECURITY_SYSLOG_REPORT_DATA_OP },
		{ "report-sys-admin",				CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN },
		{ "report-user-admin",				CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN },
		{ "report-violation",				CASE_SECURITY_SYSLOG_REPORT_VIOLATION },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_OPTS[] = {
		{ "{",								CASE_CONTEXT_BEGIN },
		{ "enable-xdr",						CASE_XDR_ENABLE_XDR },
		{ "enable-change-notification",		CASE_XDR_ENABLE_CHANGE_NOTIFICATION },
		{ "xdr-digestlog-path",				CASE_XDR_DIGESTLOG_PATH },
		{ "datacenter",						CASE_XDR_DATACENTER_BEGIN },
		{ "xdr-client-threads",				CASE_XDR_CLIENT_THREADS },
		{ "xdr-compression-threshold",		CASE_XDR_COMPRESSION_THRESHOLD },
		{ "xdr-delete-shipping-enabled",	CASE_XDR_DELETE_SHIPPING_ENABLED },
		{ "xdr-digestlog-iowait-ms",		CASE_XDR_DIGESTLOG_IOWAIT_MS },
		{ "forward-xdr-writes",				CASE_XDR_FORWARD_XDR_WRITES },
		{ "xdr-hotkey-time-ms",				CASE_XDR_HOTKEY_TIME_MS },
		{ "xdr-info-port",					CASE_XDR_INFO_PORT },
		{ "xdr-info-timeout",				CASE_XDR_INFO_TIMEOUT },
		{ "xdr-max-ship-bandwidth",			CASE_XDR_MAX_SHIP_BANDWIDTH },
		{ "xdr-max-ship-throughput",		CASE_XDR_MAX_SHIP_THROUGHPUT },
		{ "xdr-min-digestlog-free-pct",		CASE_XDR_MIN_DIGESTLOG_FREE_PCT },
		{ "xdr-nsup-deletes-enabled",		CASE_XDR_NSUP_DELETES_ENABLED },
		{ "xdr-read-threads",				CASE_XDR_READ_THREADS},
		{ "xdr-ship-bins",					CASE_XDR_SHIP_BINS },
		{ "xdr-ship-delay",					CASE_XDR_SHIP_DELAY }, // hidden
		{ "xdr-shipping-enabled",			CASE_XDR_SHIPPING_ENABLED },
		{ "xdr-write-timeout",				CASE_XDR_WRITE_TIMEOUT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_DATACENTER_OPTS[] = {
		{ "{",								CASE_CONTEXT_BEGIN },
		{ "dc-connections",					CASE_XDR_DATACENTER_DC_CONNECTIONS },
		{ "dc-connections-idle-ms",			CASE_XDR_DATACENTER_DC_CONNECTIONS_IDLE_MS },
		{ "dc-int-ext-ipmap",				CASE_XDR_DATACENTER_DC_INT_EXT_IPMAP },
		{ "dc-node-address-port",			CASE_XDR_DATACENTER_DC_NODE_ADDRESS_PORT },
		{ "dc-security-config-file",		CASE_XDR_DATACENTER_DC_SECURITY_CONFIG_FILE },
		{ "dc-ship-bins",					CASE_XDR_DATACENTER_DC_SHIP_BINS },
		{ "dc-type",						CASE_XDR_DATACENTER_DC_TYPE },
		{ "dc-use-alternate-services",		CASE_XDR_DATACENTER_DC_USE_ALTERNATE_SERVICES },
		{ "http-url",						CASE_XDR_DATACENTER_HTTP_URL },
		{ "http-version",					CASE_XDR_DATACENTER_HTTP_VERSION },
		{ "tls-name",						CASE_XDR_DATACENTER_TLS_NAME },
		{ "tls-node",						CASE_XDR_DATACENTER_TLS_NODE },
		{ "}",								CASE_CONTEXT_END }
};

// Used parsing separate file, but share cfg_case_id enum.

const cfg_opt XDR_SEC_GLOBAL_OPTS[] = {
		{ "credentials",					XDR_SEC_CASE_CREDENTIALS_BEGIN }
};

const cfg_opt XDR_SEC_CREDENTIALS_OPTS[] = {
		{ "{",								CASE_CONTEXT_BEGIN },
		{ "username",						XDR_SEC_CASE_CREDENTIALS_USERNAME },
		{ "password",						XDR_SEC_CASE_CREDENTIALS_PASSWORD },
		{ "}",								CASE_CONTEXT_END }
};

const int NUM_GLOBAL_OPTS							= sizeof(GLOBAL_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_OPTS							= sizeof(SERVICE_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_AUTO_PIN_OPTS					= sizeof(SERVICE_AUTO_PIN_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_DEBUG_ALLOCATIONS_OPTS		= sizeof(SERVICE_DEBUG_ALLOCATIONS_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_OPTS							= sizeof(LOGGING_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_FILE_OPTS						= sizeof(LOGGING_FILE_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_CONSOLE_OPTS					= sizeof(LOGGING_CONSOLE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_OPTS							= sizeof(NETWORK_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_SERVICE_OPTS					= sizeof(NETWORK_SERVICE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_OPTS				= sizeof(NETWORK_HEARTBEAT_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_MODE_OPTS			= sizeof(NETWORK_HEARTBEAT_MODE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_PROTOCOL_OPTS		= sizeof(NETWORK_HEARTBEAT_PROTOCOL_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_FABRIC_OPTS					= sizeof(NETWORK_FABRIC_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_INFO_OPTS						= sizeof(NETWORK_INFO_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_TLS_OPTS						= sizeof(NETWORK_TLS_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_OPTS						= sizeof(NAMESPACE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_CONFLICT_RESOLUTION_OPTS	= sizeof(NAMESPACE_CONFLICT_RESOLUTION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_READ_CONSISTENCY_OPTS		= sizeof(NAMESPACE_READ_CONSISTENCY_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_WRITE_COMMIT_OPTS			= sizeof(NAMESPACE_WRITE_COMMIT_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_INDEX_TYPE_OPTS				= sizeof(NAMESPACE_INDEX_TYPE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_OPTS				= sizeof(NAMESPACE_STORAGE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_INDEX_TYPE_PMEM_OPTS		= sizeof(NAMESPACE_INDEX_TYPE_PMEM_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_INDEX_TYPE_FLASH_OPTS		= sizeof(NAMESPACE_INDEX_TYPE_FLASH_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_DEVICE_OPTS			= sizeof(NAMESPACE_STORAGE_DEVICE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_DEVICE_COMPRESSION_OPTS = sizeof(NAMESPACE_STORAGE_DEVICE_COMPRESSION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OPTS = sizeof(NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SET_OPTS					= sizeof(NAMESPACE_SET_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SET_ENABLE_XDR_OPTS			= sizeof(NAMESPACE_SET_ENABLE_XDR_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SI_OPTS						= sizeof(NAMESPACE_SI_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SINDEX_OPTS					= sizeof(NAMESPACE_SINDEX_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_GEO2DSPHERE_WITHIN_OPTS		= sizeof(NAMESPACE_GEO2DSPHERE_WITHIN_OPTS) / sizeof(cfg_opt);
const int NUM_MOD_LUA_OPTS							= sizeof(MOD_LUA_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_OPTS							= sizeof(SECURITY_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LDAP_OPTS					= sizeof(SECURITY_LDAP_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS	= sizeof(SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LOG_OPTS						= sizeof(SECURITY_LOG_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_SYSLOG_OPTS					= sizeof(SECURITY_SYSLOG_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_OPTS								= sizeof(XDR_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DATACENTER_OPTS					= sizeof(XDR_DATACENTER_OPTS) / sizeof(cfg_opt);

// Used parsing separate file, but share cfg_case_id enum.

const int NUM_XDR_SEC_GLOBAL_OPTS					= sizeof(XDR_SEC_GLOBAL_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_SEC_CREDENTIALS_OPTS				= sizeof(XDR_SEC_CREDENTIALS_OPTS) / sizeof(cfg_opt);


//==========================================================
// Configuration value constants not for switch cases.
//

const char* DEVICE_SCHEDULER_MODES[] = {
		"anticipatory",
		"cfq",				// best for rotational drives
		"deadline",
		"noop"				// best for SSDs
};

const int NUM_DEVICE_SCHEDULER_MODES = sizeof(DEVICE_SCHEDULER_MODES) / sizeof(const char*);

const char* XDR_DESTINATION_TYPES[] = {
		XDR_CFG_DEST_AEROSPIKE,		// remote Aerospike cluster
		XDR_CFG_DEST_HTTP			// HTTP server
};

const int NUM_XDR_DESTINATION_TYPES = sizeof(XDR_DESTINATION_TYPES) / sizeof(const char*);

const char* XDR_HTTP_VERSION_TYPES[] = {
		XDR_CFG_HTTP_VERSION_1,
		XDR_CFG_HTTP_VERSION_2,
		XDR_CFG_HTTP_VERSION_2_PRIOR_KNOWLEDGE
};

const int NUM_XDR_HTTP_VERSION_TYPES = sizeof(XDR_HTTP_VERSION_TYPES) / sizeof(const char*);


//==========================================================
// Generic parsing utilities.
//

// Don't use these functions. Use the cf_str functions, which have better error
// handling, and support K, M, B/G, etc.
#undef atoi
#define atoi() DO_NOT_USE
#undef atol
#define atol() DO_NOT_USE
#undef atoll
#define atol() DO_NOT_USE

//------------------------------------------------
// Parsing state (context) tracking & switching.
//

typedef enum {
	GLOBAL,
	SERVICE,
	LOGGING, LOGGING_FILE, LOGGING_CONSOLE,
	NETWORK, NETWORK_SERVICE, NETWORK_HEARTBEAT, NETWORK_FABRIC, NETWORK_INFO, NETWORK_TLS,
	NAMESPACE, NAMESPACE_INDEX_TYPE_PMEM, NAMESPACE_INDEX_TYPE_FLASH, NAMESPACE_STORAGE_DEVICE, NAMESPACE_SET, NAMESPACE_SI, NAMESPACE_SINDEX, NAMESPACE_GEO2DSPHERE_WITHIN,
	MOD_LUA,
	SECURITY, SECURITY_LDAP, SECURITY_LOG, SECURITY_SYSLOG,
	XDR, XDR_DATACENTER,
	// Used parsing separate file, but shares this enum:
	XDR_SEC_CREDENTIALS,
	// Must be last, use for sanity-checking:
	PARSER_STATE_MAX_PLUS_1
} as_config_parser_state;

// For detail logging only - keep in sync with as_config_parser_state.
const char* CFG_PARSER_STATES[] = {
		"GLOBAL",
		"SERVICE",
		"LOGGING", "LOGGING_FILE", "LOGGING_CONSOLE",
		"NETWORK", "NETWORK_SERVICE", "NETWORK_HEARTBEAT", "NETWORK_FABRIC", "NETWORK_INFO", "NETWORK_TLS",
		"NAMESPACE", "NAMESPACE_INDEX_TYPE_PMEM", "NAMESPACE_INDEX_TYPE_SSD", "NAMESPACE_STORAGE_DEVICE", "NAMESPACE_SET", "NAMESPACE_SI", "NAMESPACE_SINDEX", "NAMESPACE_GEO2DSPHERE_WITHIN",
		"MOD_LUA",
		"SECURITY", "SECURITY_LDAP", "SECURITY_LOG", "SECURITY_SYSLOG",
		"XDR", "XDR_DATACENTER",
		// Used parsing separate file, but shares corresponding enum:
		"XDR_SEC_CREDENTIALS"
};

#define MAX_STACK_DEPTH 8

typedef struct cfg_parser_state_s {
	as_config_parser_state	current;
	as_config_parser_state	stack[MAX_STACK_DEPTH];
	int						depth;
} cfg_parser_state;

void
cfg_parser_state_init(cfg_parser_state* p_state)
{
	p_state->current = p_state->stack[0] = GLOBAL;
	p_state->depth = 0;
}

void
cfg_begin_context(cfg_parser_state* p_state, as_config_parser_state context)
{
	if (context < 0 || context >= PARSER_STATE_MAX_PLUS_1) {
		cf_crash(AS_CFG, "parsing - unknown context");
	}

	as_config_parser_state prev_context = p_state->stack[p_state->depth];

	if (++p_state->depth >= MAX_STACK_DEPTH) {
		cf_crash(AS_CFG, "parsing - context too deep");
	}

	p_state->current = p_state->stack[p_state->depth] = context;

	// To see this log, change NO_SINKS_LIMIT in fault.c:
	cf_detail(AS_CFG, "begin context: %s -> %s", CFG_PARSER_STATES[prev_context], CFG_PARSER_STATES[context]);
}

void
cfg_end_context(cfg_parser_state* p_state)
{
	as_config_parser_state prev_context = p_state->stack[p_state->depth];

	if (--p_state->depth < 0) {
		cf_crash(AS_CFG, "parsing - can't end context depth 0");
	}

	p_state->current = p_state->stack[p_state->depth];

	// To see this log, change NO_SINKS_LIMIT in fault.c:
	cf_detail(AS_CFG, "end context: %s -> %s", CFG_PARSER_STATES[prev_context], CFG_PARSER_STATES[p_state->current]);
}

void
cfg_parser_done(cfg_parser_state* p_state)
{
	if (p_state->depth != 0) {
		cf_crash_nostack(AS_CFG, "parsing - final context missing '}'?");
	}
}

//------------------------------------------------
// Given a token, return switch case identifier.
//

cfg_case_id
cfg_find_tok(const char* tok, const cfg_opt opts[], int num_opts)
{
	for (int i = 0; i < num_opts; i++) {
		if (strcmp(tok, opts[i].tok) == 0) {
			return opts[i].case_id;
		}
	}

	return CASE_NOT_FOUND;
}

//------------------------------------------------
// Value parsing and sanity-checking utilities.
//

void
cfg_renamed_name_tok(const cfg_line* p_line, const char* new_tok)
{
	cf_warning(AS_CFG, "line %d :: %s was renamed - please use '%s'",
			p_line->num, p_line->name_tok, new_tok);
}

void
cfg_renamed_val_tok_1(const cfg_line* p_line, const char* new_tok)
{
	cf_warning(AS_CFG, "line %d :: %s value '%s' was renamed - please use '%s'",
			p_line->num, p_line->name_tok, p_line->val_tok_1, new_tok);
}

void
cfg_deprecated_name_tok(const cfg_line* p_line)
{
	cf_warning(AS_CFG, "line %d :: %s is deprecated - please remove",
			p_line->num, p_line->name_tok);
}

void
cfg_deprecated_val_tok_1(const cfg_line* p_line)
{
	cf_warning(AS_CFG, "line %d :: %s value '%s' is deprecated - please remove",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

void
cfg_unknown_name_tok(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: unknown config parameter name '%s'",
			p_line->num, p_line->name_tok);
}

void
cfg_unknown_val_tok_1(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: %s has unknown value '%s'",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

void
cfg_obsolete(const cfg_line* p_line, const char* message)
{
	cf_crash_nostack(AS_CFG, "line %d :: '%s' is obsolete%s%s",
			p_line->num, p_line->name_tok, message ? " - " : "",
					message ? message : "");
}

char*
cfg_strdup_no_checks(const cfg_line* p_line)
{
	return cf_strdup(p_line->val_tok_1);
}

char*
cfg_strdup_val2_no_checks(const cfg_line* p_line)
{
	return cf_strdup(p_line->val_tok_2);
}

char*
cfg_strdup_anyval(const cfg_line* p_line, const char* val_tok, bool is_required)
{
	if (val_tok[0] == 0) {
		if (is_required) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must have a value specified",
					p_line->num, p_line->name_tok);
		}

		// Do not duplicate empty strings.
		return NULL;
	}

	return cf_strdup(val_tok);
}

char*
cfg_strdup(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_1, is_required);
}

char*
cfg_strdup_val2(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_2, is_required);
}

char*
cfg_strdup_one_of(const cfg_line* p_line, const char* toks[], int num_toks)
{
	for (int i = 0; i < num_toks; i++) {
		if (strcmp(p_line->val_tok_1, toks[i]) == 0) {
			return cfg_strdup_no_checks(p_line);
		}
	}

	uint32_t valid_toks_size = (num_toks * 2) + 1;

	for (int i = 0; i < num_toks; i++) {
		valid_toks_size += strlen(toks[i]);
	}

	char valid_toks[valid_toks_size];

	valid_toks[0] = 0;

	for (int i = 0; i < num_toks; i++) {
		strcat(valid_toks, toks[i]);
		strcat(valid_toks, ", ");
	}

	cf_crash_nostack(AS_CFG, "line %d :: %s must be one of: %snot %s",
			p_line->num, p_line->name_tok, valid_toks, p_line->val_tok_1);

	// Won't get here, but quiet warnings...
	return NULL;
}

void
cfg_strcpy(const cfg_line* p_line, char* p_str, size_t max_size)
{
	size_t tok1_len = strlen(p_line->val_tok_1);

	if (tok1_len == 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must have a value specified",
				p_line->num, p_line->name_tok);
	}

	if (tok1_len >= max_size) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be < %lu characters long, not %s",
				p_line->num, p_line->name_tok, max_size, p_line->val_tok_1);
	}

	strcpy(p_str, p_line->val_tok_1);
}

bool
cfg_bool(const cfg_line* p_line)
{
	if (strcasecmp(p_line->val_tok_1, "true") == 0 || strcasecmp(p_line->val_tok_1, "yes") == 0) {
		return true;
	}

	if (strcasecmp(p_line->val_tok_1, "false") == 0 || strcasecmp(p_line->val_tok_1, "no") == 0) {
		return false;
	}

	if (*p_line->val_tok_1 == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be true or false or yes or no",
				p_line->num, p_line->name_tok);
	}

	cf_crash_nostack(AS_CFG, "line %d :: %s must be true or false or yes or no, not %s",
			p_line->num, p_line->name_tok, p_line->val_tok_1);

	// Won't get here, but quiet warnings...
	return false;
}

bool
cfg_bool_no_value_is_true(const cfg_line* p_line)
{
	return (*p_line->val_tok_1 == '\0') ? true : cfg_bool(p_line);
}

int64_t
cfg_i64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an integer value",
				p_line->num, p_line->name_tok);
	}

	int64_t value;

	if (0 != cf_str_atoi_64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

int64_t
cfg_i64_no_checks(const cfg_line* p_line)
{
	return cfg_i64_anyval_no_checks(p_line, p_line->val_tok_1);
}

int64_t
cfg_i64_val2_no_checks(const cfg_line* p_line)
{
	return cfg_i64_anyval_no_checks(p_line, p_line->val_tok_2);
}

int64_t
cfg_i64_val3_no_checks(const cfg_line* p_line)
{
	return cfg_i64_anyval_no_checks(p_line, p_line->val_tok_3);
}

int64_t
cfg_i64(const cfg_line* p_line, int64_t min, int64_t max)
{
	int64_t value = cfg_i64_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %ld and <= %ld, not %ld",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

int
cfg_int_no_checks(const cfg_line* p_line)
{
	int64_t value = cfg_i64_no_checks(p_line);

	if (value < INT32_MIN || value > INT32_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %ld overflows int",
				p_line->num, p_line->name_tok, value);
	}

	return (int)value;
}

int
cfg_int(const cfg_line* p_line, int min, int max)
{
	int value = cfg_int_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %d and <= %d, not %d",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

int
cfg_int_val2_no_checks(const cfg_line* p_line)
{
	int64_t value = cfg_i64_val2_no_checks(p_line);

	if (value < INT32_MIN || value > INT32_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %ld overflows int",
				p_line->num, p_line->name_tok, value);
	}

	return (int)value;
}

int
cfg_int_val3_no_checks(const cfg_line* p_line)
{
	int64_t value = cfg_i64_val3_no_checks(p_line);

	if (value < INT32_MIN || value > INT32_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %ld overflows int",
				p_line->num, p_line->name_tok, value);
	}

	return (int)value;
}
int
cfg_int_val2(const cfg_line* p_line, int min, int max)
{
	int value = cfg_int_val2_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %d and <= %d, not %d",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

int
cfg_int_val3(const cfg_line* p_line, int min, int max)
{
	int value = cfg_int_val3_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %d and <= %d, not %d",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint64_t
cfg_x64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify a hex value",
				p_line->num, p_line->name_tok);
	}

	uint64_t value;

	if (0 != cf_str_atoi_x64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a 64-bit hex number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

uint64_t
cfg_x64_no_checks(const cfg_line* p_line)
{
	return cfg_x64_anyval_no_checks(p_line, p_line->val_tok_1);
}

uint64_t
cfg_x64(const cfg_line* p_line, uint64_t min, uint64_t max)
{
	uint64_t value = cfg_x64_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %lx, not %lx",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %lx and <= %lx, not %lx",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint64_t
cfg_u64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an unsigned integer value",
				p_line->num, p_line->name_tok);
	}

	uint64_t value;

	if (0 != cf_str_atoi_u64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an unsigned number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

uint64_t
cfg_u64_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_1);
}

uint64_t
cfg_u64_val2_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_2);
}

uint64_t
cfg_u64(const cfg_line* p_line, uint64_t min, uint64_t max)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %lu, not %lu",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %lu and <= %lu, not %lu",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

// Note - accepts 0 if min is 0.
uint64_t
cfg_u64_power_of_2(const cfg_line* p_line, uint64_t min, uint64_t max)
{
	uint64_t value = cfg_u64(p_line, min, max);

	if ((value & (value - 1)) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an exact power of 2, not %lu",
				p_line->num, p_line->name_tok, value);
	}

	return value;
}

uint32_t
cfg_u32_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT32_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned int",
				p_line->num, p_line->name_tok, value);
	}

	return (uint32_t)value;
}

uint32_t
cfg_u32(const cfg_line* p_line, uint32_t min, uint32_t max)
{
	uint32_t value = cfg_u32_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

// Note - accepts 0 if min is 0.
uint32_t
cfg_u32_power_of_2(const cfg_line* p_line, uint32_t min, uint32_t max)
{
	uint32_t value = cfg_u32(p_line, min, max);

	if ((value & (value - 1)) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an exact power of 2, not %u",
				p_line->num, p_line->name_tok, value);
	}

	return value;
}

uint16_t
cfg_u16_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT16_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned short",
				p_line->num, p_line->name_tok, value);
	}

	return (uint16_t)value;
}

uint16_t
cfg_u16(const cfg_line* p_line, uint16_t min, uint16_t max)
{
	uint16_t value = cfg_u16_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint8_t
cfg_u8_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT8_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned char",
				p_line->num, p_line->name_tok, value);
	}

	return (uint8_t)value;
}

uint8_t
cfg_u8(const cfg_line* p_line, uint8_t min, uint8_t max)
{
	uint8_t value = cfg_u8_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint32_t
cfg_seconds_no_checks(const cfg_line* p_line)
{
	if (*p_line->val_tok_1 == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an unsigned integer value with time unit (s, m, h, or d)",
				p_line->num, p_line->name_tok);
	}

	uint32_t value;

	if (cf_str_atoi_seconds(p_line->val_tok_1, &value) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a small enough unsigned number with time unit (s, m, h, or d), not %s",
				p_line->num, p_line->name_tok, p_line->val_tok_1);
	}

	return value;
}

uint32_t
cfg_seconds(const cfg_line* p_line, uint32_t min, uint32_t max)
{
	uint32_t value = cfg_seconds_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u seconds, not %u seconds",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u seconds and <= %u seconds, not %u seconds",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

// Minimum & maximum port numbers:
const int CFG_MIN_PORT = 1024;
const int CFG_MAX_PORT = UINT16_MAX;

cf_ip_port
cfg_port(const cfg_line* p_line)
{
	return (cf_ip_port)cfg_int(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

cf_ip_port
cfg_port_val2(const cfg_line* p_line)
{
	return (cf_ip_port)cfg_int_val2(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

cf_ip_port
cfg_port_val3(const cfg_line* p_line)
{
	return (cf_ip_port)cfg_int_val3(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

//------------------------------------------------
// Constants used in parsing.
//

// Token delimiter characters:
const char CFG_WHITESPACE[] = " \t\n\r\f\v";


//==========================================================
// Public API - parse the configuration file.
//

as_config*
as_config_init(const char* config_file)
{
	as_config* c = &g_config; // shortcut pointer

	// Set the service context defaults. Values parsed from the config file will
	// override the defaults.
	cfg_set_defaults();
	xdr_config_defaults();

	FILE* FD;
	char iobuf[256];
	int line_num = 0;
	cfg_parser_state state;

	cfg_parser_state_init(&state);

	as_namespace* ns = NULL;
	xdr_dest_config *cur_dest_cfg = NULL;
	cf_tls_spec* tls_spec = NULL;
	cf_fault_sink* sink = NULL;
	as_set* p_set = NULL; // local variable used for set initialization

	// Open the configuration file for reading.
	if (NULL == (FD = fopen(config_file, "r"))) {
		cf_crash_nostack(AS_CFG, "couldn't open configuration file %s: %s", config_file, cf_strerror(errno));
	}

	// Parse the configuration file, line by line.
	while (fgets(iobuf, sizeof(iobuf), FD)) {
		line_num++;

		// First chop the comment off, if there is one.

		char* p_comment = strchr(iobuf, '#');

		if (p_comment) {
			*p_comment = '\0';
		}

		// Find (and null-terminate) up to three whitespace-delimited tokens in
		// the line, a 'name' token and up to two 'value' tokens.

		cfg_line line = { line_num, NULL, NULL, NULL, NULL };

		line.name_tok = strtok(iobuf, CFG_WHITESPACE);

		// If there are no tokens, ignore this line, get the next line.
		if (! line.name_tok) {
			continue;
		}

		line.val_tok_1 = strtok(NULL, CFG_WHITESPACE);

		if (! line.val_tok_1) {
			line.val_tok_1 = ""; // in case it's used where NULL can't be used
		}
		else {
			line.val_tok_2 = strtok(NULL, CFG_WHITESPACE);
		}

		if (! line.val_tok_2) {
			line.val_tok_2 = ""; // in case it's used where NULL can't be used
		}
		else {
			line.val_tok_3 = strtok(NULL, CFG_WHITESPACE);
		}

		if (! line.val_tok_3) {
			line.val_tok_3 = ""; // in case it's used where NULL can't be used
		}

		// Note that we can't see this output until a logging sink is specified.
		cf_detail(AS_CFG, "line %d :: %s %s %s %s", line_num, line.name_tok, line.val_tok_1, line.val_tok_2, line.val_tok_3);

		// Parse the directive.
		switch (state.current) {

		//==================================================
		// Parse top-level items.
		//
		case GLOBAL:
			switch (cfg_find_tok(line.name_tok, GLOBAL_OPTS, NUM_GLOBAL_OPTS)) {
			case CASE_SERVICE_BEGIN:
				cfg_begin_context(&state, SERVICE);
				break;
			case CASE_LOGGING_BEGIN:
				cfg_begin_context(&state, LOGGING);
				break;
			case CASE_NETWORK_BEGIN:
				cfg_begin_context(&state, NETWORK);
				break;
			case CASE_NAMESPACE_BEGIN:
				// Create the namespace objects.
				ns = as_namespace_create(line.val_tok_1);
				cfg_begin_context(&state, NAMESPACE);
				break;
			case CASE_MOD_LUA_BEGIN:
				cfg_begin_context(&state, MOD_LUA);
				break;
			case CASE_SECURITY_BEGIN:
				cfg_enterprise_only(&line);
				cfg_begin_context(&state, SECURITY);
				break;
			case CASE_XDR_BEGIN:
				g_xcfg.xdr_section_configured = true;
				cfg_enterprise_only(&line);
				cfg_begin_context(&state, XDR);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse service context items.
		//
		case SERVICE:
			switch (cfg_find_tok(line.name_tok, SERVICE_OPTS, NUM_SERVICE_OPTS)) {
			case CASE_SERVICE_USER:
				{
					struct passwd* pwd;
					if (NULL == (pwd = getpwnam(line.val_tok_1))) {
						cf_crash_nostack(AS_CFG, "line %d :: user not found: %s", line_num, line.val_tok_1);
					}
					c->uid = pwd->pw_uid;
					endpwent();
				}
				break;
			case CASE_SERVICE_GROUP:
				{
					struct group* grp;
					if (NULL == (grp = getgrnam(line.val_tok_1))) {
						cf_crash_nostack(AS_CFG, "line %d :: group not found: %s", line_num, line.val_tok_1);
					}
					c->gid = grp->gr_gid;
					endgrent();
				}
				break;
			case CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT:
				c->paxos_single_replica_limit = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_PIDFILE:
				c->pidfile = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_CLIENT_FD_MAX:
				cfg_renamed_name_tok(&line, "proto-fd-max");
				// No break.
			case CASE_SERVICE_PROTO_FD_MAX:
				c->n_proto_fd_max = cfg_u32(&line, MIN_PROTO_FD_MAX, UINT32_MAX);
				break;
			case CASE_SERVICE_ADVERTISE_IPV6:
				cf_socket_set_advertise_ipv6(cfg_bool(&line));
				break;
			case CASE_SERVICE_AUTO_PIN:
				switch (cfg_find_tok(line.val_tok_1, SERVICE_AUTO_PIN_OPTS, NUM_SERVICE_AUTO_PIN_OPTS)) {
				case CASE_SERVICE_AUTO_PIN_NONE:
					c->auto_pin = CF_TOPO_AUTO_PIN_NONE;
					break;
				case CASE_SERVICE_AUTO_PIN_CPU:
					c->auto_pin = CF_TOPO_AUTO_PIN_CPU;
					break;
				case CASE_SERVICE_AUTO_PIN_NUMA:
					c->auto_pin = CF_TOPO_AUTO_PIN_NUMA;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_SERVICE_BATCH_INDEX_THREADS:
				c->n_batch_index_threads = cfg_u32(&line, 1, MAX_BATCH_THREADS);
				break;
			case CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE:
				c->batch_max_buffers_per_queue = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_MAX_REQUESTS:
				c->batch_max_requests = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS:
				c->batch_max_unused_buffers = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_CLUSTER_NAME:
				cfg_set_cluster_name(line.val_tok_1);
				break;
			case CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC:
				c->fabric_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_BENCHMARKS_SVC:
				c->svc_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_HEALTH_CHECK:
				c->health_check_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_HIST_INFO:
				c->info_hist_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_FEATURE_KEY_FILE:
				c->feature_key_file = cfg_strdup(&line, true);
				break;
			case CASE_SERVICE_HIST_TRACK_BACK:
				c->hist_track_back = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_HIST_TRACK_SLICE:
				c->hist_track_slice = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_HIST_TRACK_THRESHOLDS:
				c->hist_track_thresholds = cfg_strdup_no_checks(&line);
				// TODO - if config key present but no value (not even space) failure mode is bad...
				break;
			case CASE_SERVICE_INFO_THREADS:
				c->n_info_threads = cfg_u32(&line, 1, MAX_INFO_THREADS);
				break;
			case CASE_SERVICE_KEEP_CAPS_SSD_HEALTH:
				cfg_keep_cap(cfg_bool(&line), &c->keep_caps_ssd_health, CAP_SYS_ADMIN);
				break;
			case CASE_SERVICE_LOG_LOCAL_TIME:
				cf_fault_use_local_time(cfg_bool(&line));
				break;
			case CASE_SERVICE_LOG_MILLIS:
				cf_fault_log_millis(cfg_bool(&line));
				break;
			case CASE_SERVICE_MIGRATE_FILL_DELAY:
				cfg_enterprise_only(&line);
				c->migrate_fill_delay = cfg_seconds_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING:
				c->migrate_max_num_incoming = cfg_u32(&line, 0, AS_MIGRATE_LIMIT_MAX_NUM_INCOMING);
				break;
			case CASE_SERVICE_MIGRATE_THREADS:
				c->n_migrate_threads = cfg_u32(&line, 0, MAX_NUM_MIGRATE_XMIT_THREADS);
				break;
			case CASE_SERVICE_MIN_CLUSTER_SIZE:
				c->clustering_config.cluster_size_min = cfg_u32(&line, 0, AS_CLUSTER_SZ);
				break;
			case CASE_SERVICE_NODE_ID:
				c->self_node = cfg_x64(&line, 1, UINT64_MAX);
				break;
			case CASE_SERVICE_NODE_ID_INTERFACE:
				c->node_id_interface = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_PROTO_FD_IDLE_MS:
				c->proto_fd_idle_ms = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_BATCH_SIZE:
				c->query_bsize = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_BUFPOOL_SIZE:
				c->query_bufpool_size = cfg_u32(&line, 1, UINT32_MAX);
				break;
			case CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD:
				c->query_in_transaction_thr = cfg_bool(&line);
				break;
			case CASE_SERVICE_QUERY_LONG_Q_MAX_SIZE:
				c->query_long_q_max_size = cfg_u32(&line, 1, UINT32_MAX);
				break;
			case CASE_SERVICE_QUERY_PRE_RESERVE_PARTITIONS:
				c->partitions_pre_reserved = cfg_bool(&line);
				break;
			case CASE_SERVICE_QUERY_PRIORITY:
				c->query_priority = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_PRIORITY_SLEEP_US:
				c->query_sleep_us = cfg_u64_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_REC_COUNT_BOUND:
				c->query_rec_count_bound = cfg_u64(&line, 1, UINT64_MAX);
				break;
			case CASE_SERVICE_QUERY_REQ_IN_QUERY_THREAD:
				c->query_req_in_query_thread = cfg_bool(&line);
				break;
			case CASE_SERVICE_QUERY_REQ_MAX_INFLIGHT:
				c->query_req_max_inflight = cfg_u32(&line, 1, UINT32_MAX);
				break;
			case CASE_SERVICE_QUERY_SHORT_Q_MAX_SIZE:
				c->query_short_q_max_size = cfg_u32(&line, 1, UINT32_MAX);
				break;
			case CASE_SERVICE_QUERY_THREADS:
				c->query_threads = cfg_u32(&line, 1, AS_QUERY_MAX_THREADS);
				break;
			case CASE_SERVICE_QUERY_THRESHOLD:
				c->query_threshold = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_UNTRACKED_TIME_MS:
				c->query_untracked_time_ms = cfg_u64_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_WORKER_THREADS:
				c->query_worker_threads = cfg_u32(&line, 1, AS_QUERY_MAX_WORKER_THREADS);
				break;
			case CASE_SERVICE_RUN_AS_DAEMON:
				c->run_as_daemon = cfg_bool_no_value_is_true(&line);
				break;
			case CASE_SERVICE_SCAN_MAX_ACTIVE:
				c->scan_max_active = cfg_u32(&line, 0, 200);
				break;
			case CASE_SERVICE_SCAN_MAX_DONE:
				c->scan_max_done = cfg_u32(&line, 0, 1000);
				break;
			case CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS:
				c->scan_max_udf_transactions = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_SCAN_THREADS:
				c->scan_threads = cfg_u32(&line, 0, 128);
				break;
			case CASE_SERVICE_SERVICE_THREADS:
				c->n_service_threads = cfg_u32(&line, 1, MAX_SERVICE_THREADS);
				break;
			case CASE_SERVICE_SINDEX_BUILDER_THREADS:
				c->sindex_builder_threads = cfg_u32(&line, 1, MAX_SINDEX_BUILDER_THREADS);
				break;
			case CASE_SERVICE_SINDEX_GC_MAX_RATE:
				c->sindex_gc_max_rate = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_SINDEX_GC_PERIOD:
				c->sindex_gc_period = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TICKER_INTERVAL:
				c->ticker_interval = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TRANSACTION_MAX_MS:
				c->transaction_max_ns = cfg_u64_no_checks(&line) * 1000000;
				break;
			case CASE_SERVICE_TRANSACTION_QUEUES:
				c->n_transaction_queues = cfg_u32(&line, 1, MAX_TRANSACTION_QUEUES);
				break;
			case CASE_SERVICE_TRANSACTION_RETRY_MS:
				c->transaction_retry_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE:
				c->n_transaction_threads_per_queue = cfg_u32(&line, 1, MAX_TRANSACTION_THREADS_PER_QUEUE);
				break;
			case CASE_SERVICE_WORK_DIRECTORY:
				c->work_directory = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_DEBUG_ALLOCATIONS:
				switch (cfg_find_tok(line.val_tok_1, SERVICE_DEBUG_ALLOCATIONS_OPTS, NUM_SERVICE_DEBUG_ALLOCATIONS_OPTS)) {
				case CASE_SERVICE_DEBUG_ALLOCATIONS_NONE:
					c->debug_allocations = CF_ALLOC_DEBUG_NONE;
					break;
				case CASE_SERVICE_DEBUG_ALLOCATIONS_TRANSIENT:
					c->debug_allocations = CF_ALLOC_DEBUG_TRANSIENT;
					break;
				case CASE_SERVICE_DEBUG_ALLOCATIONS_PERSISTENT:
					c->debug_allocations = CF_ALLOC_DEBUG_PERSISTENT;
					break;
				case CASE_SERVICE_DEBUG_ALLOCATIONS_ALL:
					c->debug_allocations = CF_ALLOC_DEBUG_ALL;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_SERVICE_FABRIC_DUMP_MSGS:
				c->fabric_dump_msgs = cfg_bool(&line);
				break;
			case CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS:
				cfg_obsolete(&line, "please configure 'service-threads' carefully");
				break;
			case CASE_SERVICE_NSUP_PERIOD:
				cfg_obsolete(&line, "please use namespace-context 'nsup-period'");
				break;
			case CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD:
				cfg_obsolete(&line, "please use namespace-context 'nsup-hist-period'");
				break;
			case CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION:
				cfg_obsolete(&line, "please use namespace-context 'write-commit-level-override' and/or write transaction policy");
				break;
			case CASE_SERVICE_TRANSACTION_PENDING_LIMIT:
				cfg_obsolete(&line, "please use namespace-context 'transaction-pending-limit'");
				break;
			case CASE_SERVICE_TRANSACTION_REPEATABLE_READ:
				cfg_obsolete(&line, "please use namespace-context 'read-consistency-level-override' and/or read transaction policy");
				break;
			case CASE_SERVICE_AUTO_DUN:
			case CASE_SERVICE_AUTO_UNDUN:
			case CASE_SERVICE_BATCH_PRIORITY:
			case CASE_SERVICE_BATCH_RETRANSMIT:
			case CASE_SERVICE_BATCH_THREADS:
			case CASE_SERVICE_CLIB_LIBRARY:
			case CASE_SERVICE_DEFRAG_QUEUE_ESCAPE:
			case CASE_SERVICE_DEFRAG_QUEUE_HWM:
			case CASE_SERVICE_DEFRAG_QUEUE_LWM:
			case CASE_SERVICE_DEFRAG_QUEUE_PRIORITY:
			case CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE:
			case CASE_SERVICE_FABRIC_WORKERS:
			case CASE_SERVICE_FB_HEALTH_BAD_PCT:
			case CASE_SERVICE_FB_HEALTH_GOOD_PCT:
			case CASE_SERVICE_FB_HEALTH_MSG_PER_BURST:
			case CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT:
			case CASE_SERVICE_GENERATION_DISABLE:
			case CASE_SERVICE_MAX_MSGS_PER_TYPE:
			case CASE_SERVICE_MIGRATE_READ_PRIORITY:
			case CASE_SERVICE_MIGRATE_READ_SLEEP:
			case CASE_SERVICE_MIGRATE_RX_LIFETIME_MS:
			case CASE_SERVICE_MIGRATE_XMIT_HWM:
			case CASE_SERVICE_MIGRATE_XMIT_LWM:
			case CASE_SERVICE_MIGRATE_PRIORITY:
			case CASE_SERVICE_MIGRATE_XMIT_PRIORITY:
			case CASE_SERVICE_MIGRATE_XMIT_SLEEP:
			case CASE_SERVICE_NSUP_AUTO_HWM:
			case CASE_SERVICE_NSUP_AUTO_HWM_PCT:
			case CASE_SERVICE_NSUP_DELETE_SLEEP:
			case CASE_SERVICE_NSUP_MAX_DELETES:
			case CASE_SERVICE_NSUP_QUEUE_ESCAPE:
			case CASE_SERVICE_NSUP_QUEUE_HWM:
			case CASE_SERVICE_NSUP_QUEUE_LWM:
			case CASE_SERVICE_NSUP_REDUCE_PRIORITY:
			case CASE_SERVICE_NSUP_REDUCE_SLEEP:
			case CASE_SERVICE_NSUP_STARTUP_EVICT:
			case CASE_SERVICE_NSUP_THREADS:
			case CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE:
			case CASE_SERVICE_PAXOS_PROTOCOL:
			case CASE_SERVICE_PAXOS_RECOVERY_POLICY:
			case CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD:
			case CASE_SERVICE_PROLE_EXTRA_TTL:
			case CASE_SERVICE_REPLICATION_FIRE_AND_FORGET:
			case CASE_SERVICE_SCAN_MEMORY:
			case CASE_SERVICE_SCAN_PRIORITY:
			case CASE_SERVICE_SCAN_RETRANSMIT:
			case CASE_SERVICE_SCHEDULER_PRIORITY:
			case CASE_SERVICE_SCHEDULER_TYPE:
			case CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS:
			case CASE_SERVICE_TRIAL_ACCOUNT_KEY:
			case CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY:
			case CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY:
			case CASE_SERVICE_USE_QUEUE_PER_DEVICE:
			case CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse logging context items.
		//
		case LOGGING:
			switch (cfg_find_tok(line.name_tok, LOGGING_OPTS, NUM_LOGGING_OPTS)) {
			case CASE_LOG_FILE_BEGIN:
				if ((sink = cf_fault_sink_hold(line.val_tok_1)) == NULL) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add file %s as log sink", line_num, line.val_tok_1);
				}
				cfg_begin_context(&state, LOGGING_FILE);
				break;
			case CASE_LOG_CONSOLE_BEGIN:
				if ((sink = cf_fault_sink_hold("stderr")) == NULL) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add stderr as log sink", line_num);
				}
				cfg_begin_context(&state, LOGGING_CONSOLE);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse logging::file context items.
		//
		case LOGGING_FILE:
			switch (cfg_find_tok(line.name_tok, LOGGING_FILE_OPTS, NUM_LOGGING_FILE_OPTS)) {
			case CASE_LOG_FILE_CONTEXT:
				if (0 != cf_fault_sink_addcontext(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging file context %s %s", line_num, line.val_tok_1, line.val_tok_2);
				}
				break;
			case CASE_CONTEXT_END:
				sink = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse logging::console context items.
		//
		case LOGGING_CONSOLE:
			switch (cfg_find_tok(line.name_tok, LOGGING_CONSOLE_OPTS, NUM_LOGGING_CONSOLE_OPTS)) {
			case CASE_LOG_CONSOLE_CONTEXT:
				if (0 != cf_fault_sink_addcontext(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging console context %s %s", line_num, line.val_tok_1, line.val_tok_2);
				}
				break;
			case CASE_CONTEXT_END:
				sink = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse network context items.
		//
		case NETWORK:
			switch (cfg_find_tok(line.name_tok, NETWORK_OPTS, NUM_NETWORK_OPTS)) {
			case CASE_NETWORK_SERVICE_BEGIN:
				cfg_begin_context(&state, NETWORK_SERVICE);
				break;
			case CASE_NETWORK_HEARTBEAT_BEGIN:
				cfg_begin_context(&state, NETWORK_HEARTBEAT);
				break;
			case CASE_NETWORK_FABRIC_BEGIN:
				cfg_begin_context(&state, NETWORK_FABRIC);
				break;
			case CASE_NETWORK_INFO_BEGIN:
				cfg_begin_context(&state, NETWORK_INFO);
				break;
			case CASE_NETWORK_TLS_BEGIN:
				cfg_enterprise_only(&line);
				tls_spec = cfg_create_tls_spec(c, line.val_tok_1);
				cfg_begin_context(&state, NETWORK_TLS);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::service context items.
		//
		case NETWORK_SERVICE:
			switch (cfg_find_tok(line.name_tok, NETWORK_SERVICE_OPTS, NUM_NETWORK_SERVICE_OPTS)) {
			case CASE_NETWORK_SERVICE_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_PORT:
				c->service.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS:
				cfg_renamed_name_tok(&line, "access-address");
				// No break.
			case CASE_NETWORK_SERVICE_ACCESS_ADDRESS:
				cfg_add_addr_std(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_ACCESS_PORT:
				c->service.std_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS:
				cfg_add_addr_alt(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT:
				c->service.alt_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_TLS_ACCESS_ADDRESS:
				cfg_enterprise_only(&line);
				cfg_add_addr_std(line.val_tok_1, &c->tls_service);
				break;
			case CASE_NETWORK_SERVICE_TLS_ACCESS_PORT:
				cfg_enterprise_only(&line);
				c->tls_service.std_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_TLS_ADDRESS:
				cfg_enterprise_only(&line);
				cfg_add_addr_bind(line.val_tok_1, &c->tls_service);
				break;
			case CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_ADDRESS:
				cfg_enterprise_only(&line);
				cfg_add_addr_alt(line.val_tok_1, &c->tls_service);
				break;
			case CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_PORT:
				cfg_enterprise_only(&line);
				c->tls_service.alt_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_TLS_AUTHENTICATE_CLIENT:
				cfg_enterprise_only(&line);
				add_tls_peer_name(line.val_tok_1, &c->tls_service);
				break;
			case CASE_NETWORK_SERVICE_TLS_NAME:
				cfg_enterprise_only(&line);
				c->tls_service.tls_our_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_SERVICE_TLS_PORT:
				cfg_enterprise_only(&line);
				c->tls_service.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_ALTERNATE_ADDRESS:
				cfg_obsolete(&line, "see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/network_to_3_10");
				break;
			case CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME:
				cfg_obsolete(&line, "see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/network_to_3_10");
				break;
			case CASE_NETWORK_SERVICE_REUSE_ADDRESS:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::heartbeat context items.
		//
		case NETWORK_HEARTBEAT:
			switch (cfg_find_tok(line.name_tok, NETWORK_HEARTBEAT_OPTS, NUM_NETWORK_HEARTBEAT_OPTS)) {
			case CASE_NETWORK_HEARTBEAT_MODE:
				switch (cfg_find_tok(line.val_tok_1, NETWORK_HEARTBEAT_MODE_OPTS, NUM_NETWORK_HEARTBEAT_MODE_OPTS)) {
				case CASE_NETWORK_HEARTBEAT_MODE_MULTICAST:
					c->hb_config.mode = AS_HB_MODE_MULTICAST;
					break;
				case CASE_NETWORK_HEARTBEAT_MODE_MESH:
					c->hb_config.mode = AS_HB_MODE_MESH;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NETWORK_HEARTBEAT_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->hb_serv_spec);
				break;
			case CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP:
				add_addr(line.val_tok_1, &c->hb_multicast_groups);
				break;
			case CASE_NETWORK_HEARTBEAT_PORT:
				c->hb_serv_spec.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT:
				cfg_add_mesh_seed_addr_port(cfg_strdup_no_checks(&line), cfg_port_val2(&line), false);
				break;
			case CASE_NETWORK_HEARTBEAT_INTERVAL:
				c->hb_config.tx_interval = cfg_u32(&line, AS_HB_TX_INTERVAL_MS_MIN, AS_HB_TX_INTERVAL_MS_MAX);
				break;
			case CASE_NETWORK_HEARTBEAT_TIMEOUT:
				c->hb_config.max_intervals_missed = cfg_u32(&line, AS_HB_MAX_INTERVALS_MISSED_MIN, UINT32_MAX);
				break;
			case CASE_NETWORK_HEARTBEAT_MTU:
				c->hb_config.override_mtu = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MCAST_TTL:
				cfg_renamed_name_tok(&line, "multicast-ttl");
				// No break.
			case CASE_NETWORK_HEARTBEAT_MULTICAST_TTL:
				c->hb_config.multicast_ttl = cfg_u8_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_PROTOCOL:
				switch (cfg_find_tok(line.val_tok_1, NETWORK_HEARTBEAT_PROTOCOL_OPTS, NUM_NETWORK_HEARTBEAT_PROTOCOL_OPTS)) {
				case CASE_NETWORK_HEARTBEAT_PROTOCOL_NONE:
					c->hb_config.protocol = AS_HB_PROTOCOL_NONE;
					break;
				case CASE_NETWORK_HEARTBEAT_PROTOCOL_V3:
					c->hb_config.protocol = AS_HB_PROTOCOL_V3;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NETWORK_HEARTBEAT_TLS_ADDRESS:
				cfg_enterprise_only(&line);
				cfg_add_addr_bind(line.val_tok_1, &c->hb_tls_serv_spec);
				break;
			case CASE_NETWORK_HEARTBEAT_TLS_MESH_SEED_ADDRESS_PORT:
				cfg_add_mesh_seed_addr_port(cfg_strdup_no_checks(&line), cfg_port_val2(&line), true);
				break;
			case CASE_NETWORK_HEARTBEAT_TLS_NAME:
				cfg_enterprise_only(&line);
				c->hb_tls_serv_spec.tls_our_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_TLS_PORT:
				cfg_enterprise_only(&line);
				c->hb_tls_serv_spec.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS:
				cfg_obsolete(&line, "see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/network_to_3_10");
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::fabric context items.
		//
		case NETWORK_FABRIC:
			switch (cfg_find_tok(line.name_tok, NETWORK_FABRIC_OPTS, NUM_NETWORK_FABRIC_OPTS)) {
			case CASE_NETWORK_FABRIC_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->fabric);
				break;
			case CASE_NETWORK_FABRIC_PORT:
				c->fabric.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_BULK_FDS:
				c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_SOCKETS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_BULK_RECV_THREADS:
				c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_CTRL_FDS:
				c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_SOCKETS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_CTRL_RECV_THREADS:
				c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_META_FDS:
				c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_META] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_SOCKETS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_META_RECV_THREADS:
				c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_RW_FDS:
				c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_SOCKETS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS:
				c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED:
				c->fabric_keepalive_enabled = cfg_bool(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_INTVL:
				c->fabric_keepalive_intvl = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_PROBES:
				c->fabric_keepalive_probes = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_TIME:
				c->fabric_keepalive_time = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_LATENCY_MAX_MS:
				c->fabric_latency_max_ms = cfg_int(&line, 0, 1000);
				break;
			case CASE_NETWORK_FABRIC_RECV_REARM_THRESHOLD:
				c->fabric_recv_rearm_threshold = cfg_u32(&line, 0, 1024 * 1024);
				break;
			case CASE_NETWORK_FABRIC_SEND_THREADS:
				c->n_fabric_send_threads = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_TLS_ADDRESS:
				cfg_enterprise_only(&line);
				cfg_add_addr_bind(line.val_tok_1, &c->tls_fabric);
				break;
			case CASE_NETWORK_FABRIC_TLS_NAME:
				cfg_enterprise_only(&line);
				c->tls_fabric.tls_our_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_TLS_PORT:
				cfg_enterprise_only(&line);
				c->tls_fabric.bind_port = cfg_port(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::info context items.
		//
		case NETWORK_INFO:
			switch (cfg_find_tok(line.name_tok, NETWORK_INFO_OPTS, NUM_NETWORK_INFO_OPTS)) {
			case CASE_NETWORK_INFO_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->info);
				break;
			case CASE_NETWORK_INFO_PORT:
				c->info.bind_port = cfg_port(&line);
				break;
			case CASE_NETWORK_INFO_ENABLE_FASTPATH:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::tls context items.
		//
		case NETWORK_TLS:
			switch (cfg_find_tok(line.name_tok, NETWORK_TLS_OPTS, NUM_NETWORK_TLS_OPTS)) {
			case CASE_NETWORK_TLS_CA_FILE:
				tls_spec->ca_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_CA_PATH:
				tls_spec->ca_path = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_CERT_BLACKLIST:
				tls_spec->cert_blacklist = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_CERT_FILE:
				tls_spec->cert_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_CIPHER_SUITE:
				tls_spec->cipher_suite = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_KEY_FILE:
				tls_spec->key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_KEY_FILE_PASSWORD:
				tls_spec->key_file_password = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_TLS_PROTOCOLS:
				tls_spec->protocols = cfg_strdup_no_checks(&line);
				break;
			case CASE_CONTEXT_END:
				tls_spec = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse namespace items.
		//
		case NAMESPACE:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_OPTS, NUM_NAMESPACE_OPTS)) {
			case CASE_NAMESPACE_REPLICATION_FACTOR:
				ns->cfg_replication_factor = cfg_u32(&line, 1, AS_CLUSTER_SZ);
				break;
			case CASE_NAMESPACE_LIMIT_SIZE:
				cfg_renamed_name_tok(&line, "memory-size");
				// No break.
			case CASE_NAMESPACE_MEMORY_SIZE:
				ns->memory_size = cfg_u64(&line, 1024 * 1024, UINT64_MAX);
				break;
			case CASE_NAMESPACE_DEFAULT_TTL:
				ns->default_ttl = cfg_seconds(&line, 0, MAX_ALLOWED_TTL);
				break;
			case CASE_NAMESPACE_STORAGE_ENGINE_BEGIN:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_OPTS, NUM_NAMESPACE_STORAGE_OPTS)) {
				case CASE_NAMESPACE_STORAGE_MEMORY:
					ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
					ns->storage_data_in_memory = true;
					break;
				case CASE_NAMESPACE_STORAGE_SSD:
					cfg_renamed_val_tok_1(&line, "device");
					// No break.
				case CASE_NAMESPACE_STORAGE_DEVICE:
					ns->storage_type = AS_STORAGE_ENGINE_SSD;
					ns->storage_data_in_memory = false;
					cfg_begin_context(&state, NAMESPACE_STORAGE_DEVICE);
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_ENABLE_XDR:
				cfg_enterprise_only(&line);
				ns->enable_xdr = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_SETS_ENABLE_XDR:
				ns->sets_enable_xdr = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_FORWARD_XDR_WRITES:
				ns->ns_forward_xdr_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_XDR_REMOTE_DATACENTER:
				xdr_cfg_associate_datacenter(cfg_strdup(&line, true), ns->id);
				break;
			case CASE_NAMESPACE_ALLOW_NONXDR_WRITES:
				ns->ns_allow_nonxdr_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ALLOW_XDR_WRITES:
				ns->ns_allow_xdr_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_CONFLICT_RESOLUTION_OPTS, NUM_NAMESPACE_CONFLICT_RESOLUTION_OPTS)) {
				case CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION:
					ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
					break;
				case CASE_NAMESPACE_CONFLICT_RESOLUTION_LAST_UPDATE_TIME:
					ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_DATA_IN_INDEX:
				ns->data_in_index = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISABLE_COLD_START_EVICTION:
				ns->cold_start_eviction_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISABLE_WRITE_DUP_RES:
				ns->write_dup_res_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISALLOW_NULL_SETNAME:
				ns->disallow_null_setname = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB:
				ns->batch_sub_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_READ:
				ns->read_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF:
				ns->udf_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB:
				ns->udf_sub_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE:
				ns->write_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_ENABLE_HIST_PROXY:
				ns->proxy_hist_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_EVICT_HIST_BUCKETS:
				ns->evict_hist_buckets = cfg_u32(&line, 100, 10000000);
				break;
			case CASE_NAMESPACE_EVICT_TENTHS_PCT:
				ns->evict_tenths_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_HIGH_WATER_DISK_PCT:
				ns->hwm_disk_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT:
				ns->hwm_memory_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_INDEX_STAGE_SIZE:
				ns->index_stage_size = cfg_u64_power_of_2(&line, CF_ARENAX_MIN_STAGE_SIZE, CF_ARENAX_MAX_STAGE_SIZE);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_BEGIN:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_INDEX_TYPE_OPTS, NUM_NAMESPACE_INDEX_TYPE_OPTS)) {
				case CASE_NAMESPACE_INDEX_TYPE_SHMEM:
					ns->xmem_type = CF_XMEM_TYPE_SHMEM;
					break;
				case CASE_NAMESPACE_INDEX_TYPE_PMEM:
					ns->xmem_type = CF_XMEM_TYPE_PMEM;
					cfg_begin_context(&state, NAMESPACE_INDEX_TYPE_PMEM);
					break;
				case CASE_NAMESPACE_INDEX_TYPE_FLASH:
					ns->xmem_type = CF_XMEM_TYPE_FLASH;
					cfg_begin_context(&state, NAMESPACE_INDEX_TYPE_FLASH);
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_MIGRATE_ORDER:
				ns->migrate_order = cfg_u32(&line, 1, 10);
				break;
			case CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS:
				ns->migrate_retransmit_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_MIGRATE_SLEEP:
				ns->migrate_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_NSUP_HIST_PERIOD:
				ns->nsup_hist_period = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_NSUP_PERIOD:
				ns->nsup_period = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_NSUP_THREADS:
				ns->n_nsup_threads = cfg_u32(&line, 1, 128);
				break;
			case CASE_NAMESPACE_PARTITION_TREE_SPRIGS:
				ns->tree_shared.n_sprigs = cfg_u32_power_of_2(&line, NUM_LOCK_PAIRS, 1 << NUM_SPRIG_BITS);
				break;
			case CASE_NAMESPACE_PREFER_UNIFORM_BALANCE:
				cfg_enterprise_only(&line);
				ns->cfg_prefer_uniform_balance = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_RACK_ID:
				cfg_enterprise_only(&line);
				ns->rack_id = cfg_u32(&line, 0, MAX_RACK_ID);
				break;
			case CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_READ_CONSISTENCY_OPTS, NUM_NAMESPACE_READ_CONSISTENCY_OPTS)) {
				case CASE_NAMESPACE_READ_CONSISTENCY_ALL:
					ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ALL;
					break;
				case CASE_NAMESPACE_READ_CONSISTENCY_OFF:
					ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_PROTO;
					break;
				case CASE_NAMESPACE_READ_CONSISTENCY_ONE:
					ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ONE;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_SET_BEGIN:
				p_set = cfg_add_set(ns);
				cfg_strcpy(&line, p_set->name, AS_SET_NAME_MAX_SIZE);
				cfg_begin_context(&state, NAMESPACE_SET);
				break;
			case CASE_NAMESPACE_SINDEX_BEGIN:
				cfg_begin_context(&state, NAMESPACE_SINDEX);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN:
				cfg_begin_context(&state, NAMESPACE_GEO2DSPHERE_WITHIN);
				break;
			case CASE_NAMESPACE_SINGLE_BIN:
				ns->single_bin = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STOP_WRITES_PCT:
				ns->stop_writes_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STRONG_CONSISTENCY:
				cfg_enterprise_only(&line);
				ns->cp = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STRONG_CONSISTENCY_ALLOW_EXPUNGE:
				cfg_enterprise_only(&line);
				ns->cp_allow_drops = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_TOMB_RAIDER_ELIGIBLE_AGE:
				cfg_enterprise_only(&line);
				ns->tomb_raider_eligible_age = cfg_seconds_no_checks(&line);
				break;
			case CASE_NAMESPACE_TOMB_RAIDER_PERIOD:
				cfg_enterprise_only(&line);
				ns->tomb_raider_period = cfg_seconds_no_checks(&line);
				break;
			case CASE_NAMESPACE_TRANSACTION_PENDING_LIMIT:
				ns->transaction_pending_limit = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_WRITE_COMMIT_OPTS, NUM_NAMESPACE_WRITE_COMMIT_OPTS)) {
				case CASE_NAMESPACE_WRITE_COMMIT_ALL:
					ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_ALL;
					break;
				case CASE_NAMESPACE_WRITE_COMMIT_MASTER:
					ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_MASTER;
					break;
				case CASE_NAMESPACE_WRITE_COMMIT_OFF:
					ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_PROTO;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_DISABLE_NSUP:
				cfg_obsolete(&line, "please set namespace-context 'nsup-period' to 0 to disable nsup");
				break;
			case CASE_NAMESPACE_ALLOW_VERSIONS:
			case CASE_NAMESPACE_COLD_START_EVICT_TTL:
			case CASE_NAMESPACE_DEMO_READ_MULTIPLIER:
			case CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER:
			case CASE_NAMESPACE_HIGH_WATER_PCT:
			case CASE_NAMESPACE_LOW_WATER_PCT:
			case CASE_NAMESPACE_MAX_TTL:
			case CASE_NAMESPACE_OBJ_SIZE_HIST_MAX:
			case CASE_NAMESPACE_PARTITION_TREE_LOCKS:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_NAMESPACE_SI_BEGIN:
				cfg_deprecated_name_tok(&line);
				// Entire section is deprecated but needs to begin and end the
				// context to avoid crash.
				cfg_begin_context(&state, NAMESPACE_SI);
				break;
			case CASE_CONTEXT_END:
				if (ns->memory_size == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure non-zero 'memory-size'", ns->name);
				}
				if (ns->data_in_index && ! (ns->single_bin && ns->storage_data_in_memory && ns->storage_type == AS_STORAGE_ENGINE_SSD)) {
					cf_crash_nostack(AS_CFG, "ns %s data-in-index can't be true unless storage-engine is device and both single-bin and data-in-memory are true", ns->name);
				}
				if (ns->storage_data_in_memory) {
					ns->storage_post_write_queue = 0; // override default (or configuration mistake)
				}
				if (ns->storage_data_in_memory &&
						! ns->storage_commit_to_device) {
					c->n_namespaces_inlined++;
				}
				else {
					c->n_namespaces_not_inlined++;
				}
				ns = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::index-type pmem context items.
		//
		case NAMESPACE_INDEX_TYPE_PMEM:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_INDEX_TYPE_PMEM_OPTS, NUM_NAMESPACE_INDEX_TYPE_PMEM_OPTS)) {
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT:
				cfg_add_xmem_mount(ns, cfg_strdup(&line, true));
				break;
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT:
				ns->mounts_hwm_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT:
				ns->mounts_size_limit = cfg_u64(&line, 1024UL * 1024UL * 1024UL, UINT64_MAX);
				break;
			case CASE_CONTEXT_END:
				if (ns->mounts_size_limit == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'mounts-size-limit'", ns->name);
				}
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::index-type flash context items.
		//
		case NAMESPACE_INDEX_TYPE_FLASH:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_INDEX_TYPE_FLASH_OPTS, NUM_NAMESPACE_INDEX_TYPE_FLASH_OPTS)) {
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT:
				cfg_add_xmem_mount(ns, cfg_strdup(&line, true));
				break;
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT:
				ns->mounts_hwm_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT:
				ns->mounts_size_limit = cfg_u64(&line, 1024UL * 1024UL * 1024UL * 4UL, UINT64_MAX);
				break;
			case CASE_CONTEXT_END:
				if (ns->mounts_size_limit == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'mounts-size-limit'", ns->name);
				}
				cfg_end_context(&state);
				// TODO - main() doesn't yet support initialization as root.
				cf_page_cache_dirty_limits();
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::storage-engine device context items.
		//
		case NAMESPACE_STORAGE_DEVICE:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_DEVICE_OPTS, NUM_NAMESPACE_STORAGE_DEVICE_OPTS)) {
			case CASE_NAMESPACE_STORAGE_DEVICE_DEVICE:
				cfg_add_storage_device(ns, cfg_strdup(&line, true), cfg_strdup_val2(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILE:
				cfg_add_storage_file(ns, cfg_strdup(&line, true), cfg_strdup_val2(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE:
				ns->storage_filesize = cfg_u64(&line, 1024 * 1024, AS_STORAGE_MAX_DEVICE_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE:
				ns->storage_scheduler_mode = cfg_strdup_one_of(&line, DEVICE_SCHEDULER_MODES, NUM_DEVICE_SCHEDULER_MODES);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE:
				ns->storage_write_block_size = cfg_u32_power_of_2(&line, MIN_WRITE_BLOCK_SIZE, MAX_WRITE_BLOCK_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL:
				cfg_renamed_name_tok(&line, "data-in-memory");
				// No break.
			case CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY:
				ns->storage_data_in_memory = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY:
				ns->storage_cold_start_empty = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE:
				cfg_enterprise_only(&line);
				ns->storage_commit_to_device = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_MIN_SIZE:
				cfg_enterprise_only(&line);
				ns->storage_commit_min_size = cfg_u32_power_of_2(&line, 0, MAX_WRITE_BLOCK_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_DEVICE_COMPRESSION_OPTS, NUM_NAMESPACE_STORAGE_DEVICE_COMPRESSION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_NONE:
					ns->storage_compression = AS_COMPRESSION_NONE;
					break;
				case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LZ4:
					ns->storage_compression = AS_COMPRESSION_LZ4;
					break;
				case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_SNAPPY:
					ns->storage_compression = AS_COMPRESSION_SNAPPY;
					break;
				case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ZSTD:
					ns->storage_compression = AS_COMPRESSION_ZSTD;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LEVEL:
				cfg_enterprise_only(&line);
				ns->storage_compression_level = cfg_u32(&line, 1, 9);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT:
				ns->storage_defrag_lwm_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN:
				ns->storage_defrag_queue_min = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP:
				ns->storage_defrag_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM:
				ns->storage_defrag_startup_minimum = cfg_int(&line, 1, 99);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES:
				ns->storage_direct_files = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE:
				ns->storage_benchmarks_enabled = true;
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OPTS, NUM_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_128:
					ns->storage_encryption = AS_ENCRYPTION_AES_128;
					break;
				case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_AES_256:
					ns->storage_encryption = AS_ENCRYPTION_AES_256;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_KEY_FILE:
				cfg_enterprise_only(&line);
				ns->storage_encryption_key_file = cfg_strdup(&line, true);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS:
				ns->storage_flush_max_us = cfg_u64_no_checks(&line) * 1000;
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE:
				ns->storage_max_write_cache = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT:
				ns->storage_min_avail_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE:
				ns->storage_post_write_queue = cfg_u32(&line, 0, 4 * 1024);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE:
				ns->storage_read_page_cache = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER:
				cfg_enterprise_only(&line);
				ns->storage_serialize_tomb_raider = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP:
				cfg_enterprise_only(&line);
				ns->storage_tomb_raider_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT:
				cfg_obsolete(&line, "please use 'read-page-cache' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC:
				cfg_obsolete(&line, "please use 'flush-files' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS:
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD:
			case CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC:
			case CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP:
			case CASE_NAMESPACE_STORAGE_DEVICE_PERSIST:
			case CASE_NAMESPACE_STORAGE_DEVICE_READONLY:
			case CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE:
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD:
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				if (ns->n_storage_devices == 0 && ns->n_storage_files == 0) {
					cf_crash_nostack(AS_CFG, "{%s} has no devices or files", ns->name);
				}
				if (ns->n_storage_files != 0 && ns->storage_filesize == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'filesize' if using storage files", ns->name);
				}
				if (ns->storage_compression_level != 0 && ns->storage_compression != AS_COMPRESSION_ZSTD) {
					cf_crash_nostack(AS_CFG, "{%s} 'compression-level' is only relevant for 'compression zstd'", ns->name);
				}
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::set context items.
		//
		case NAMESPACE_SET:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_SET_OPTS, NUM_NAMESPACE_SET_OPTS)) {
			case CASE_NAMESPACE_SET_DISABLE_EVICTION:
				DISABLE_SET_EVICTION(p_set, cfg_bool(&line));
				break;
			case CASE_NAMESPACE_SET_ENABLE_XDR:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_SET_ENABLE_XDR_OPTS, NUM_NAMESPACE_SET_ENABLE_XDR_OPTS)) {
				case CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_DEFAULT;
					break;
				case CASE_NAMESPACE_SET_ENABLE_XDR_FALSE:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_FALSE;
					break;
				case CASE_NAMESPACE_SET_ENABLE_XDR_TRUE:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_TRUE;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_SET_STOP_WRITES_COUNT:
				p_set->stop_writes_count = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_SET_EVICT_HWM_COUNT:
			case CASE_NAMESPACE_SET_EVICT_HWM_PCT:
			case CASE_NAMESPACE_SET_STOP_WRITE_COUNT:
			case CASE_NAMESPACE_SET_STOP_WRITE_PCT:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::si context items.
		//
		case NAMESPACE_SI:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_SI_OPTS, NUM_NAMESPACE_SI_OPTS)) {
			case CASE_NAMESPACE_SI_GC_PERIOD:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_NAMESPACE_SI_GC_MAX_UNITS:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_NAMESPACE_SI_HISTOGRAM:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_NAMESPACE_SI_IGNORE_NOT_SYNC:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_val_tok_1(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::sindex context items.
		//
		case NAMESPACE_SINDEX:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_SINDEX_OPTS, NUM_NAMESPACE_SINDEX_OPTS)) {
			case CASE_NAMESPACE_SINDEX_NUM_PARTITIONS:
				ns->sindex_num_partitions = cfg_u32(&line, MIN_PARTITIONS_PER_INDEX, MAX_PARTITIONS_PER_INDEX);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::2dsphere-within context items.
		//
		case NAMESPACE_GEO2DSPHERE_WITHIN:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_GEO2DSPHERE_WITHIN_OPTS, NUM_NAMESPACE_GEO2DSPHERE_WITHIN_OPTS)) {
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_STRICT:
				ns->geo2dsphere_within_strict = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MIN_LEVEL:
				ns->geo2dsphere_within_min_level = cfg_u16(&line, 0, MAX_REGION_LEVELS);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_LEVEL:
				ns->geo2dsphere_within_max_level = cfg_u16(&line, 0, MAX_REGION_LEVELS);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_MAX_CELLS:
				ns->geo2dsphere_within_max_cells = cfg_u16(&line, 1, MAX_REGION_CELLS);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_LEVEL_MOD:
				ns->geo2dsphere_within_level_mod = cfg_u16(&line, 1, 3);
				break;
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_EARTH_RADIUS_METERS:
				ns->geo2dsphere_within_earth_radius_meters = cfg_u32_no_checks(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse mod-lua context items.
		//
		case MOD_LUA:
			switch (cfg_find_tok(line.name_tok, MOD_LUA_OPTS, NUM_MOD_LUA_OPTS)) {
			case CASE_MOD_LUA_CACHE_ENABLED:
				c->mod_lua.cache_enabled = cfg_bool(&line);
				break;
			case CASE_MOD_LUA_USER_PATH:
				cfg_strcpy(&line, c->mod_lua.user_path, sizeof(c->mod_lua.user_path));
				break;
			case CASE_MOD_LUA_SYSTEM_PATH:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse security context items.
		//
		case SECURITY:
			switch (cfg_find_tok(line.name_tok, SECURITY_OPTS, NUM_SECURITY_OPTS)) {
			case CASE_SECURITY_ENABLE_LDAP:
				c->sec_cfg.ldap_enabled = cfg_bool(&line);
				break;
			case CASE_SECURITY_ENABLE_SECURITY:
				c->sec_cfg.security_enabled = cfg_bool(&line);
				break;
			case CASE_SECURITY_LDAP_LOGIN_THREADS:
				c->sec_cfg.n_ldap_login_threads = cfg_u32(&line, 1, 64);
				break;
			case CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD:
				c->sec_cfg.privilege_refresh_period = cfg_u32(&line, PRIVILEGE_REFRESH_PERIOD_MIN, PRIVILEGE_REFRESH_PERIOD_MAX);
				break;
			case CASE_SECURITY_LDAP_BEGIN:
				cfg_begin_context(&state, SECURITY_LDAP);
				break;
			case CASE_SECURITY_LOG_BEGIN:
				cfg_begin_context(&state, SECURITY_LOG);
				break;
			case CASE_SECURITY_SYSLOG_BEGIN:
				cfg_begin_context(&state, SECURITY_SYSLOG);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse security::ldap context items.
		//
		case SECURITY_LDAP:
			switch (cfg_find_tok(line.name_tok, SECURITY_LDAP_OPTS, NUM_SECURITY_LDAP_OPTS)) {
			case CASE_SECURITY_LDAP_DISABLE_TLS:
				c->sec_cfg.ldap_tls_disabled = cfg_bool(&line);
				break;
			case CASE_SECURITY_LDAP_POLLING_PERIOD:
				c->sec_cfg.ldap_polling_period = cfg_u32(&line, LDAP_POLLING_PERIOD_MIN, LDAP_POLLING_PERIOD_MAX);
				break;
			case CASE_SECURITY_LDAP_QUERY_BASE_DN:
				c->sec_cfg.ldap_query_base_dn = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_QUERY_USER_DN:
				c->sec_cfg.ldap_query_user_dn = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE:
				c->sec_cfg.ldap_query_user_password_file = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN:
				c->sec_cfg.ldap_role_query_base_dn = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN:
				cfg_add_ldap_role_query_pattern(cfg_strdup(&line, true));
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU:
				c->sec_cfg.ldap_role_query_search_ou = cfg_bool(&line);
				break;
			case CASE_SECURITY_LDAP_SERVER:
				c->sec_cfg.ldap_server = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_SESSION_TTL:
				c->sec_cfg.ldap_session_ttl = cfg_u32(&line, LDAP_SESSION_TTL_MIN, LDAP_SESSION_TTL_MAX);
				break;
			case CASE_SECURITY_LDAP_TLS_CA_FILE:
				c->sec_cfg.ldap_tls_ca_file = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_TOKEN_HASH_METHOD:
				switch (cfg_find_tok(line.val_tok_1, SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS, NUM_SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS)) {
				case CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_256:
					c->sec_cfg.ldap_token_hash_method = AS_LDAP_EVP_SHA_256;
					break;
				case CASE_SECURITY_LDAP_TOKEN_HASH_METHOD_SHA_512:
					c->sec_cfg.ldap_token_hash_method = AS_LDAP_EVP_SHA_512;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_SECURITY_LDAP_USER_DN_PATTERN:
				c->sec_cfg.ldap_user_dn_pattern = cfg_strdup(&line, true);
				break;
			case CASE_SECURITY_LDAP_USER_QUERY_PATTERN:
				c->sec_cfg.ldap_user_query_pattern = cfg_strdup(&line, true);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse security::log context items.
		//
		case SECURITY_LOG:
			switch (cfg_find_tok(line.name_tok, SECURITY_LOG_OPTS, NUM_SECURITY_LOG_OPTS)) {
			case CASE_SECURITY_LOG_REPORT_AUTHENTICATION:
				c->sec_cfg.report.authentication |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_DATA_OP:
				as_security_config_log_scope(AS_SEC_SINK_LOG, line.val_tok_1, line.val_tok_2);
				break;
			case CASE_SECURITY_LOG_REPORT_SYS_ADMIN:
				c->sec_cfg.report.sys_admin |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_USER_ADMIN:
				c->sec_cfg.report.user_admin |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_VIOLATION:
				c->sec_cfg.report.violation |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse security::syslog context items.
		//
		case SECURITY_SYSLOG:
			switch (cfg_find_tok(line.name_tok, SECURITY_SYSLOG_OPTS, NUM_SECURITY_SYSLOG_OPTS)) {
			case CASE_SECURITY_SYSLOG_LOCAL:
				c->sec_cfg.syslog_local = (as_sec_syslog_local)cfg_int(&line, AS_SYSLOG_MIN, AS_SYSLOG_MAX);
				break;
			case CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION:
				c->sec_cfg.report.authentication |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_DATA_OP:
				as_security_config_log_scope(AS_SEC_SINK_SYSLOG, line.val_tok_1, line.val_tok_2);
				break;
			case CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN:
				c->sec_cfg.report.sys_admin |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN:
				c->sec_cfg.report.user_admin |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_VIOLATION:
				c->sec_cfg.report.violation |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse xdr context items.
		//
		case XDR:
			switch (cfg_find_tok(line.name_tok, XDR_OPTS, NUM_XDR_OPTS)) {
			case CASE_CONTEXT_BEGIN:
				// Allow open brace on its own line to begin this context.
				break;
			case CASE_XDR_ENABLE_XDR:
				g_xcfg.xdr_global_enabled = cfg_bool(&line);
				break;
			case CASE_XDR_ENABLE_CHANGE_NOTIFICATION:
				g_xcfg.xdr_enable_change_notification = cfg_bool(&line);
				break;
			case CASE_XDR_DIGESTLOG_PATH:
				g_xcfg.xdr_digestlog_path = cfg_strdup(&line, true);
				g_xcfg.xdr_digestlog_file_size = cfg_u64_val2_no_checks(&line);
				break;
			case CASE_XDR_DATACENTER_BEGIN:
				cur_dest_cfg = xdr_cfg_add_datacenter(cfg_strdup(&line, true));
				cfg_begin_context(&state, XDR_DATACENTER);
				break;
			case CASE_XDR_CLIENT_THREADS:
				g_xcfg.xdr_client_threads = cfg_u32(&line, 1, XDR_MAX_CLIENT_THREADS);
				break;
			case CASE_XDR_COMPRESSION_THRESHOLD:
				g_xcfg.xdr_compression_threshold = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_DELETE_SHIPPING_ENABLED:
				g_xcfg.xdr_delete_shipping_enabled = cfg_bool(&line);
				break;
			case CASE_XDR_DIGESTLOG_IOWAIT_MS:
				g_xcfg.xdr_digestlog_iowait_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_FORWARD_XDR_WRITES:
				g_xcfg.xdr_forward_xdrwrites = cfg_bool(&line);
				break;
			case CASE_XDR_HOTKEY_TIME_MS:
				g_xcfg.xdr_hotkey_time_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_INFO_PORT:
				g_xcfg.xdr_info_port = cfg_port(&line);
				break;
			case CASE_XDR_INFO_TIMEOUT:
				g_xcfg.xdr_info_request_timeout_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_MAX_SHIP_BANDWIDTH:
				g_xcfg.xdr_max_ship_bandwidth = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_MAX_SHIP_THROUGHPUT:
				g_xcfg.xdr_max_ship_throughput = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_MIN_DIGESTLOG_FREE_PCT:
				g_xcfg.xdr_min_dlog_free_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_XDR_NSUP_DELETES_ENABLED:
				g_xcfg.xdr_nsup_deletes_enabled = cfg_bool(&line);
				break;
			case CASE_XDR_READ_THREADS:
				g_xcfg.xdr_read_threads = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_SHIP_BINS:
				g_xcfg.xdr_ship_bins = cfg_bool(&line);
				break;
			case CASE_XDR_SHIP_DELAY:
				g_xcfg.xdr_internal_shipping_delay = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_SHIPPING_ENABLED:
				g_xcfg.xdr_shipping_enabled = cfg_bool(&line);
				break;
			case CASE_XDR_WRITE_TIMEOUT:
				g_xcfg.xdr_write_timeout = cfg_u32_no_checks(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse xdr::datacenter context items.
		//
		case XDR_DATACENTER:
			switch (cfg_find_tok(line.name_tok, XDR_DATACENTER_OPTS, NUM_XDR_DATACENTER_OPTS)) {
			case CASE_CONTEXT_BEGIN:
				// Allow open brace on its own line to begin this context.
				break;
			case CASE_XDR_DATACENTER_DC_TYPE:
				cur_dest_cfg->dc_type = cfg_strdup_one_of(&line, XDR_DESTINATION_TYPES, NUM_XDR_DESTINATION_TYPES);
				break;
			case CASE_XDR_DATACENTER_DC_NODE_ADDRESS_PORT:
				xdr_cfg_add_node_addr_port(cur_dest_cfg, cfg_strdup(&line, true), cfg_port_val2(&line));
				break;
			case CASE_XDR_DATACENTER_DC_CONNECTIONS:
				cur_dest_cfg->aero.dc_connections = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_DATACENTER_DC_CONNECTIONS_IDLE_MS:
				cur_dest_cfg->aero.dc_connections_idle_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_DATACENTER_DC_INT_EXT_IPMAP:
				xdr_cfg_add_int_ext_mapping(&cur_dest_cfg->aero, cfg_strdup(&line, true), cfg_strdup_val2(&line, true));
				break;
			case CASE_XDR_DATACENTER_DC_SECURITY_CONFIG_FILE:
				cur_dest_cfg->dc_security_cfg.sec_config_file = cfg_strdup(&line, true);
				break;
			case CASE_XDR_DATACENTER_DC_SHIP_BINS:
				cur_dest_cfg->dc_ship_bins = cfg_bool(&line);
				break;
			case CASE_XDR_DATACENTER_DC_USE_ALTERNATE_SERVICES:
				cur_dest_cfg->aero.dc_use_alternate_services = cfg_bool(&line);
				break;
			case CASE_XDR_DATACENTER_HTTP_URL:
				xdr_cfg_add_http_url(cur_dest_cfg, cfg_strdup(&line, true));
				break;
			case CASE_XDR_DATACENTER_HTTP_VERSION:
				cur_dest_cfg->http.version_str = cfg_strdup_one_of(&line, XDR_HTTP_VERSION_TYPES, NUM_XDR_HTTP_VERSION_TYPES);
				break;
			case CASE_XDR_DATACENTER_TLS_NAME:
				cur_dest_cfg->dc_tls_spec_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_XDR_DATACENTER_TLS_NODE:
				xdr_cfg_add_tls_node(cur_dest_cfg, cfg_strdup(&line, true), cfg_strdup_val2(&line, true), cfg_port_val3(&line));
				break;
			case CASE_CONTEXT_END:
				g_dc_count++;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parser state is corrupt.
		//
		default:
			cf_crash_nostack(AS_CFG, "line %d :: invalid parser top-level state %d", line_num, state.current);
			break;
		}
	}

	cfg_parser_done(&state);

	fclose(FD);

	//--------------------------------------------
	// Checks that must wait until everything is parsed. Alternatively, such
	// checks can be done in as_config_post_process() - doing them here means
	// failure logs show in the console, doing them in as_config_post_process()
	// means failure logs show in the log file.
	//

	as_security_config_check();

	return &g_config;
}


//==========================================================
// Public API - configuration-related tasks after parsing.
//

void
as_config_post_process(as_config* c, const char* config_file)
{
	//--------------------------------------------
	// Re-read the configuration file and print it to the logs, line by line.
	// This will be the first thing to appear in the log file(s).
	//

	FILE* FD;

	if (NULL == (FD = fopen(config_file, "r"))) {
		cf_crash_nostack(AS_CFG, "couldn't re-open configuration file %s: %s", config_file, cf_strerror(errno));
	}

	char iobuf[256];

	while (fgets(iobuf, sizeof(iobuf), FD)) {
		char* p = iobuf;
		char* p_last = p + (strlen(p) - 1);

		if ('\n' == *p_last) {
			*p_last-- = '\0';
		}

		if (p_last >= p && '\r' == *p_last) {
			*p_last = '\0';
		}

		cf_info(AS_CFG, "%s", p);
	}

	fclose(FD);

	//
	// Done echoing configuration file to log.
	//--------------------------------------------

	// Configuration checks and special defaults that differ between CE and EE.
	cfg_post_process();

	cf_alloc_set_debug(c->debug_allocations);

	// Check the configured file descriptor limit against the system limit.
	struct rlimit fd_limit;

	getrlimit(RLIMIT_NOFILE, &fd_limit);

	if ((rlim_t)c->n_proto_fd_max > fd_limit.rlim_cur) {
		cf_crash_nostack(AS_CFG, "%lu system file descriptors not enough, config specified %u", fd_limit.rlim_cur, c->n_proto_fd_max);
	}

	cf_info(AS_CFG, "system file descriptor limit: %lu, proto-fd-max: %u", fd_limit.rlim_cur, c->n_proto_fd_max);

	// Output NUMA topology information.
	cf_topo_info();

	if (c->auto_pin != CF_TOPO_AUTO_PIN_NONE) {
		if (c->n_service_threads != 0) {
			cf_crash_nostack(AS_CFG, "can't configure 'service-threads' and 'auto-pin' at the same time");
		}

		if (c->n_transaction_queues != 0) {
			cf_crash_nostack(AS_CFG, "can't configure 'transaction-queues' and 'auto-pin' at the same time");
		}
	}

	uint16_t n_cpus = cf_topo_count_cpus();

	if (c->n_service_threads == 0) {
		c->n_service_threads = n_cpus;
	}

	if (c->n_transaction_queues == 0) {
		// If there's at least one SSD namespace, use CPU count. Otherwise, be
		// modest - only proxies, internal retries, and background scans & queries
		// will use these queues & threads.
		c->n_transaction_queues = g_config.n_namespaces_not_inlined != 0 ? n_cpus : 4;
	}

	// Setup performance metrics histograms.
	cfg_create_all_histograms();

	// If node-id was not configured, generate one.
	if (c->self_node == 0) {
		cf_ip_port id_port = c->fabric.bind_port != 0 ? c->fabric.bind_port : c->tls_fabric.bind_port;

		if (cf_node_id_get(id_port, c->node_id_interface, &c->self_node) < 0) {
			cf_crash_nostack(AS_CFG, "could not get node id");
		}
	}
	else if (c->node_id_interface) {
		cf_crash_nostack(AS_CFG, "may not configure both 'node-id' and ''node-id-interface");
	}

	cf_info(AS_CFG, "node-id %lx", c->self_node);

	// Resolve TLS names and read key file passwords for all TLS
	// configurations.

	for (uint32_t i = 0; i < g_config.n_tls_specs; ++i) {
		cf_tls_spec *tspec = &g_config.tls_specs[i];

		if (tspec->name == NULL) {
			cf_crash_nostack(AS_CFG, "nameless TLS configuration section");
		}

		tspec->name = cfg_resolve_tls_name(tspec->name, g_config.cluster_name, NULL);

		if (tspec->key_file_password == NULL) {
			continue;
		}

		tspec->pw_string = tls_read_password(tspec->key_file_password);
	}

	// Populate access ports from configuration.

	g_access.service.port = g_config.service.std_port != 0 ?
			g_config.service.std_port : g_config.service.bind_port;

	g_access.alt_service.port = g_config.service.alt_port != 0 ?
			g_config.service.alt_port : g_access.service.port;

	g_access.tls_service.port = g_config.tls_service.std_port != 0 ?
			g_config.tls_service.std_port : g_config.tls_service.bind_port;

	g_access.alt_tls_service.port = g_config.tls_service.alt_port != 0 ?
			g_config.tls_service.alt_port : g_access.tls_service.port;

	// Populate access addresses from configuration.

	cfg_serv_spec_std_to_access(&g_config.service, &g_access.service.addrs);
	cfg_serv_spec_alt_to_access(&g_config.service, &g_access.alt_service.addrs);
	cfg_serv_spec_std_to_access(&g_config.tls_service, &g_access.tls_service.addrs);
	cfg_serv_spec_alt_to_access(&g_config.tls_service, &g_access.alt_tls_service.addrs);

	// By default, use bind addresses also as access addresses.

	if (g_access.service.addrs.n_addrs == 0) {
		bind_to_access(&g_config.service, &g_access.service.addrs);
	}

	if (g_access.tls_service.addrs.n_addrs == 0) {
		bind_to_access(&g_config.tls_service, &g_access.tls_service.addrs);
	}

	// By default, use non-TLS access addresses also for TLS - and vice versa.

	default_addrs(&g_access.service.addrs, &g_access.tls_service.addrs);
	default_addrs(&g_access.alt_service.addrs, &g_access.alt_tls_service.addrs);

	cf_serv_cfg_init(&g_service_bind);

	// Client service bind addresses.

	if (g_config.service.bind_port != 0) {
		cfg_serv_spec_to_bind(&g_config.service, &g_config.tls_service, &g_service_bind,
				CF_SOCK_OWNER_SERVICE);
	}

	// Client TLS service bind addresses.

	if (g_config.tls_service.bind_port != 0) {
		cfg_serv_spec_to_bind(&g_config.tls_service, &g_config.service, &g_service_bind,
				CF_SOCK_OWNER_SERVICE_TLS);

		cf_tls_spec* tls_spec = cfg_link_tls("service", &g_config.tls_service.tls_our_name);

		uint32_t n_peer_names = g_config.tls_service.n_tls_peer_names;
		char **peer_names = g_config.tls_service.tls_peer_names;

		bool has_any = false;
		bool has_false = false;

		for (uint32_t i = 0; i < n_peer_names; ++i) {
			has_any = has_any || strcmp(peer_names[i], "any") == 0;
			has_false = has_false || strcmp(peer_names[i], "false") == 0;
		}

		if ((has_any || has_false) && n_peer_names > 1) {
			cf_crash_nostack(AS_CFG, "\"any\" and \"false\" are incompatible with other tls-authenticate-client arguments");
		}

		bool auth_client;

		if (has_any || n_peer_names == 0) {
			auth_client = true;
			n_peer_names = 0;
			peer_names = NULL;
		}
		else if (has_false) {
			auth_client = false;
			n_peer_names = 0;
			peer_names = NULL;
		}
		else {
			auth_client = true;
		}

		g_service_tls = tls_config_server_context(tls_spec, auth_client, n_peer_names, peer_names);
	}

	if (g_service_bind.n_cfgs == 0) {
		cf_crash_nostack(AS_CFG, "no service ports configured");
	}

	// Heartbeat service bind addresses.

	cf_serv_cfg_init(&g_config.hb_config.bind_cfg);

	if (c->hb_serv_spec.bind_port != 0) {
		cfg_serv_spec_to_bind(&c->hb_serv_spec, &c->hb_tls_serv_spec, &c->hb_config.bind_cfg,
				CF_SOCK_OWNER_HEARTBEAT);
	}

	// Heartbeat TLS service bind addresses.

	if (c->hb_tls_serv_spec.bind_port != 0) {
		if (c->hb_config.mode != AS_HB_MODE_MESH) {
			cf_crash_nostack(AS_CFG, "multicast heartbeats do not support TLS");
		}

		cfg_serv_spec_to_bind(&c->hb_tls_serv_spec, &c->hb_serv_spec, &c->hb_config.bind_cfg,
				CF_SOCK_OWNER_HEARTBEAT_TLS);

		cf_tls_spec* tls_spec = cfg_link_tls("heartbeat", &c->hb_tls_serv_spec.tls_our_name);
		c->hb_config.tls = tls_config_intra_context(tls_spec, "heartbeat");
	}

	if (g_config.hb_config.bind_cfg.n_cfgs == 0) {
		cf_crash_nostack(AS_CFG, "no heartbeat ports configured");
	}

	// Heartbeat multicast groups.

	if (c->hb_multicast_groups.n_addrs > 0) {
		cfg_mserv_config_from_addrs(&c->hb_multicast_groups, &c->hb_serv_spec.bind,
				&g_config.hb_config.multicast_group_cfg, c->hb_serv_spec.bind_port,
				CF_SOCK_OWNER_HEARTBEAT, g_config.hb_config.multicast_ttl);
	}

	// Fabric service bind addresses.

	cf_serv_cfg_init(&g_fabric_bind);

	if (g_config.fabric.bind_port != 0) {
		cfg_serv_spec_to_bind(&g_config.fabric, &g_config.tls_fabric, &g_fabric_bind,
				CF_SOCK_OWNER_FABRIC);
	}

	// Fabric TLS service bind addresses.

	if (g_config.tls_fabric.bind_port != 0) {
		cfg_serv_spec_to_bind(&g_config.tls_fabric, &g_config.fabric, &g_fabric_bind,
				CF_SOCK_OWNER_FABRIC_TLS);

		cf_tls_spec* tls_spec = cfg_link_tls("fabric", &g_config.tls_fabric.tls_our_name);
		g_fabric_tls = tls_config_intra_context(tls_spec, "fabric");
	}

	if (g_fabric_bind.n_cfgs == 0) {
		cf_crash_nostack(AS_CFG, "no fabric ports configured");
	}

	// Info service port.

	g_info_port = g_config.info.bind_port;

	// Info service bind addresses.

	cf_serv_cfg_init(&g_info_bind);
	cfg_serv_spec_to_bind(&g_config.info, NULL, &g_info_bind, CF_SOCK_OWNER_INFO);

	// Validate heartbeat configuration.
	as_hb_config_validate();

	//--------------------------------------------
	// Per-namespace config post-processing.
	//

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		client_replica_maps_create(ns);

		uint32_t sprigs_offset = sizeof(as_lock_pair) * NUM_LOCK_PAIRS;
		uint32_t puddles_offset = 0;

		if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
			puddles_offset = sprigs_offset + sizeof(as_sprig) * ns->tree_shared.n_sprigs;
		}

		// Note - ns->tree_shared.arena is set later when it's allocated.
		ns->tree_shared.destructor			= (as_index_value_destructor)as_record_destroy;
		ns->tree_shared.destructor_udata	= (void*)ns;
		ns->tree_shared.locks_shift			= NUM_SPRIG_BITS - cf_msb(NUM_LOCK_PAIRS);
		ns->tree_shared.sprigs_shift		= NUM_SPRIG_BITS - cf_msb(ns->tree_shared.n_sprigs);
		ns->tree_shared.sprigs_offset		= sprigs_offset;
		ns->tree_shared.puddles_offset		= puddles_offset;

		as_storage_cfg_init(ns);

		char hist_name[HISTOGRAM_NAME_SIZE];

		// One-way activated histograms (may be tracked histograms).

		sprintf(hist_name, "{%s}-read", ns->name);
		create_and_check_hist_track(&ns->read_hist, hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-write", ns->name);
		create_and_check_hist_track(&ns->write_hist, hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-udf", ns->name);
		create_and_check_hist_track(&ns->udf_hist, hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-query", ns->name);
		create_and_check_hist_track(&ns->query_hist, hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-query-rec-count", ns->name);
		ns->query_rec_count_hist = histogram_create(hist_name, HIST_COUNT);

		sprintf(hist_name, "{%s}-re-repl", ns->name);
		ns->re_repl_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		// Activate-by-config histograms (can't be tracked histograms).

		sprintf(hist_name, "{%s}-proxy", ns->name);
		ns->proxy_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-read-start", ns->name);
		ns->read_start_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-read-restart", ns->name);
		ns->read_restart_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-read-dup-res", ns->name);
		ns->read_dup_res_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-read-repl-ping", ns->name);
		ns->read_repl_ping_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-read-local", ns->name);
		ns->read_local_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-read-response", ns->name);
		ns->read_response_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-write-start", ns->name);
		ns->write_start_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-write-restart", ns->name);
		ns->write_restart_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-write-dup-res", ns->name);
		ns->write_dup_res_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-write-master", ns->name);
		ns->write_master_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-write-repl-write", ns->name);
		ns->write_repl_write_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-write-response", ns->name);
		ns->write_response_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-udf-start", ns->name);
		ns->udf_start_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-restart", ns->name);
		ns->udf_restart_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-dup-res", ns->name);
		ns->udf_dup_res_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-master", ns->name);
		ns->udf_master_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-repl-write", ns->name);
		ns->udf_repl_write_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-response", ns->name);
		ns->udf_response_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-batch-sub-start", ns->name);
		ns->batch_sub_start_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-batch-sub-restart", ns->name);
		ns->batch_sub_restart_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-batch-sub-dup-res", ns->name);
		ns->batch_sub_dup_res_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-batch-sub-repl-ping", ns->name);
		ns->batch_sub_repl_ping_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-batch-sub-read-local", ns->name);
		ns->batch_sub_read_local_hist = histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-batch-sub-response", ns->name);
		ns->batch_sub_response_hist = histogram_create(hist_name, HIST_MILLISECONDS);

		sprintf(hist_name, "{%s}-udf-sub-start", ns->name);
		ns->udf_sub_start_hist =  histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-sub-restart", ns->name);
		ns->udf_sub_restart_hist =  histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-sub-dup-res", ns->name);
		ns->udf_sub_dup_res_hist =  histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-sub-master", ns->name);
		ns->udf_sub_master_hist =  histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-sub-repl-write", ns->name);
		ns->udf_sub_repl_write_hist =  histogram_create(hist_name, HIST_MILLISECONDS);
		sprintf(hist_name, "{%s}-udf-sub-response", ns->name);
		ns->udf_sub_response_hist =  histogram_create(hist_name, HIST_MILLISECONDS);

		// 'nsup' histograms.

		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			sprintf(hist_name, "{%s}-object-size-log2", ns->name);
			ns->obj_size_log_hist = histogram_create(hist_name, HIST_SIZE);
			sprintf(hist_name, "{%s}-object-size-linear", ns->name);
			ns->obj_size_lin_hist = linear_hist_create(hist_name, LINEAR_HIST_SIZE, 0, ns->storage_write_block_size, OBJ_SIZE_HIST_NUM_BUCKETS);
		}

		sprintf(hist_name, "{%s}-evict", ns->name);
		ns->evict_hist = linear_hist_create(hist_name, LINEAR_HIST_SECONDS, 0, 0, ns->evict_hist_buckets);

		sprintf(hist_name, "{%s}-ttl", ns->name);
		ns->ttl_hist = linear_hist_create(hist_name, LINEAR_HIST_SECONDS, 0, 0, TTL_HIST_NUM_BUCKETS);
	}
}


//==========================================================
// Public API - Cluster name.
//

static cf_mutex g_config_lock = CF_MUTEX_INIT;

void
as_config_cluster_name_get(char* cluster_name)
{
	cf_mutex_lock(&g_config_lock);
	strcpy(cluster_name, g_config.cluster_name);
	cf_mutex_unlock(&g_config_lock);
}

bool
as_config_cluster_name_set(const char* cluster_name)
{
	if (cluster_name[0] == '\0') {
		cf_warning(AS_CFG, "cluster name '%s' is not allowed. Ignoring.", cluster_name);
		return false;
	}

	if (strlen(cluster_name) >= AS_CLUSTER_NAME_SZ) {
		cf_warning(AS_CFG, "size of cluster name should not be greater than %d characters. Ignoring cluster name '%s'.",
			AS_CLUSTER_NAME_SZ - 1, cluster_name);
		return false;
	}

	cf_mutex_lock(&g_config_lock);

	if (strcmp(cluster_name,"null") == 0){
		// 'null' is a special value representing an unset cluster-name.
		strcpy(g_config.cluster_name, "");
	}
	else {
		strcpy(g_config.cluster_name, cluster_name);
	}

	cf_mutex_unlock(&g_config_lock);

	return true;
}

bool
as_config_cluster_name_matches(const char* cluster_name)
{
	cf_mutex_lock(&g_config_lock);
	bool matches = strcmp(cluster_name, g_config.cluster_name) == 0;
	cf_mutex_unlock(&g_config_lock);
	return matches;
}


//==========================================================
// Public API - XDR.
//

bool
xdr_read_security_configfile(xdr_security_config* sc)
{
	FILE* FD;
	char iobuf[256];
	int line_num = 0;
	cfg_parser_state state;

	cfg_parser_state_init(&state);

	// Initialize the XDR config values to the defaults.
	sc->username = NULL;
	sc->password = NULL;
	iobuf[0] = 0;

	// Open the configuration file for reading. Dont crash if it fails as this
	// function can be called during runtime (when credentials file change)
	if (NULL == (FD = fopen(sc->sec_config_file, "r"))) {
		cf_warning(AS_XDR, "Couldn't open configuration file %s: %s",
				sc->sec_config_file, cf_strerror(errno));
		return false;
	}

	// Parse the configuration file, line by line.
	while (fgets(iobuf, sizeof(iobuf), FD)) {
		line_num++;

		// First chop the comment off, if there is one.

		char* p_comment = strchr(iobuf, '#');

		if (p_comment) {
			*p_comment = '\0';
		}

		// Find (and null-terminate) up to three whitespace-delimited tokens in
		// the line, a 'name' token and up to two 'value' tokens.

		cfg_line line = { line_num, NULL, NULL, NULL, NULL };

		line.name_tok = strtok(iobuf, CFG_WHITESPACE);

		// If there are no tokens, ignore this line, get the next line.
		if (! line.name_tok) {
			continue;
		}

		line.val_tok_1 = strtok(NULL, CFG_WHITESPACE);

		if (! line.val_tok_1) {
			line.val_tok_1 = ""; // in case it's used where NULL can't be used
		}
		else {
			line.val_tok_2 = strtok(NULL, CFG_WHITESPACE);
		}

		if (! line.val_tok_2) {
			line.val_tok_2 = ""; // in case it's used where NULL can't be used
		}
		else {
			line.val_tok_3 = strtok(NULL, CFG_WHITESPACE);
		}

		if (! line.val_tok_3) {
			line.val_tok_3 = ""; // in case it's used where NULL can't be used
		}

		// Note that we can't see this output until a logging sink is specified.
		cf_detail(AS_CFG, "line %d :: %s %s %s %s", line_num, line.name_tok,
				line.val_tok_1, line.val_tok_2, line.val_tok_3);

		// Parse the directive.
		switch (state.current) {

		// Parse top-level items.
		case GLOBAL:
			switch (cfg_find_tok(line.name_tok, XDR_SEC_GLOBAL_OPTS, NUM_XDR_SEC_GLOBAL_OPTS)) {
			case XDR_SEC_CASE_CREDENTIALS_BEGIN:
				cfg_begin_context(&state, XDR_SEC_CREDENTIALS);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		// Parse xdr context items.
		case XDR_SEC_CREDENTIALS:
			switch (cfg_find_tok(line.name_tok, XDR_SEC_CREDENTIALS_OPTS, NUM_XDR_SEC_CREDENTIALS_OPTS)) {
			case CASE_CONTEXT_BEGIN:
				// Allow open brace on its own line to begin this context.
				break;
			case XDR_SEC_CASE_CREDENTIALS_USERNAME:
				sc->username = cfg_strdup(&line, true);
				break;
			case XDR_SEC_CASE_CREDENTIALS_PASSWORD:
				sc->password = cfg_strdup(&line, true);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		// Parser state is corrupt.
		default:
			cf_warning(AS_XDR, "line %d :: invalid parser top-level state %d",
					line_num, state.current);
			break;
		}
	}

	// Close the file.
	fclose(FD);
	return true;
}


//==========================================================
// Item-specific parsing utilities.
//

void
init_addr_list(cf_addr_list* addrs)
{
	addrs->n_addrs = 0;
	memset(&addrs->addrs, '\0', sizeof(addrs->addrs));
}

void
add_addr(const char* name, cf_addr_list* addrs)
{
	uint32_t n = addrs->n_addrs;

	if (n >= CF_SOCK_CFG_MAX) {
		cf_crash_nostack(CF_SOCKET, "Too many addresses: %s", name);
	}

	addrs->addrs[n] = cf_strdup(name);
	++addrs->n_addrs;
}

void
add_tls_peer_name(const char* name, cf_serv_spec* spec)
{
	uint32_t n = spec->n_tls_peer_names;

	if (n >= CF_SOCK_CFG_MAX) {
		cf_crash_nostack(CF_SOCKET, "Too many TLS peer names: %s", name);
	}

	spec->tls_peer_names[n] = cf_strdup(name);
	++spec->n_tls_peer_names;
}

void
copy_addrs(const cf_addr_list* from, cf_addr_list* to)
{
	for (uint32_t i = 0; i < from->n_addrs; ++i) {
		to->addrs[i] = from->addrs[i];
	}

	to->n_addrs = from->n_addrs;
}

void
default_addrs(cf_addr_list* one, cf_addr_list* two)
{
	if (one->n_addrs == 0) {
		copy_addrs(two, one);
	}

	if (two->n_addrs == 0) {
		copy_addrs(one, two);
	}
}

void
bind_to_access(const cf_serv_spec* from, cf_addr_list* to)
{
	cf_serv_spec spec;
	spec.bind_port = 0;
	init_addr_list(&spec.bind);
	spec.std_port = 0;
	init_addr_list(&spec.std);
	spec.alt_port = 0;
	init_addr_list(&spec.alt);

	for (uint32_t i = 0; i < from->bind.n_addrs; ++i) {
		cf_ip_addr resol[CF_SOCK_CFG_MAX];
		uint32_t n_resol = CF_SOCK_CFG_MAX;

		if (cf_ip_addr_from_string_multi(from->bind.addrs[i], resol, &n_resol) < 0) {
			cf_crash_nostack(AS_CFG, "Invalid default access address: %s", from->bind.addrs[i]);
		}

		bool valid = true;

		for (uint32_t k = 0; k < n_resol; ++k) {
			if (cf_ip_addr_is_any(&resol[k]) || cf_ip_addr_is_local(&resol[k])) {
				cf_debug(AS_CFG, "Skipping invalid default access address: %s",
						from->bind.addrs[i]);
				valid = false;
				break;
			}
		}

		if (valid) {
			uint32_t n = spec.std.n_addrs;
			spec.std.addrs[n] = from->bind.addrs[i];
			++spec.std.n_addrs;
		}
	}

	cfg_serv_spec_std_to_access(&spec, to);
}

void
cfg_add_addr_bind(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->bind);
}

void
cfg_add_addr_std(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->std);
}

void
cfg_add_addr_alt(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->alt);
}

void
cfg_mserv_config_from_addrs(cf_addr_list* addrs, cf_addr_list* bind_addrs,
		cf_mserv_cfg* serv_cfg, cf_ip_port port, cf_sock_owner owner,
		uint8_t ttl)
{
	static cf_addr_list def_addrs = {
		.n_addrs = 1, .addrs = { "any" }
	};

	if (bind_addrs->n_addrs == 0) {
		bind_addrs = &def_addrs;
	}

	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {

		cf_ip_addr resol[CF_SOCK_CFG_MAX];
		uint32_t n_resol = CF_SOCK_CFG_MAX;

		if (cf_ip_addr_from_string_multi(addrs->addrs[i], resol,
						 &n_resol) < 0) {
			cf_crash_nostack(AS_CFG, "Invalid multicast group: %s",
					 addrs->addrs[i]);
		}

		for (uint32_t j = 0; j < bind_addrs->n_addrs; j++) {

			cf_ip_addr bind_resol[CF_SOCK_CFG_MAX];
			uint32_t n_bind_resol = CF_SOCK_CFG_MAX;

			if (cf_ip_addr_from_string_multi(bind_addrs->addrs[j],
							 bind_resol,
							 &n_bind_resol) < 0) {
				cf_crash_nostack(AS_CFG, "Invalid address: %s",
						 bind_addrs->addrs[j]);
			}

			for (int32_t k = 0; k < n_resol; ++k) {
				for (int32_t l = 0; l < n_bind_resol; ++l) {
					if (cf_mserv_cfg_add_combo(serv_cfg, owner, port,
							&resol[k], &bind_resol[l], ttl) < 0) {
						cf_crash_nostack(AS_CFG, "Too many IP addresses");
					}
				}
			}
		}
	}
}

void
cfg_serv_spec_to_bind(const cf_serv_spec* spec, const cf_serv_spec* def_spec, cf_serv_cfg* bind,
		cf_sock_owner owner)
{
	static cf_addr_list def_addrs = {
		.n_addrs = 1, .addrs = { "any" }
	};

	cf_sock_cfg cfg;
	cf_sock_cfg_init(&cfg, owner);
	cfg.port = spec->bind_port;

	const cf_addr_list* addrs;

	if (spec->bind.n_addrs != 0) {
		addrs = &spec->bind;
	}
	else if (def_spec != NULL && def_spec->bind.n_addrs != 0) {
		addrs = &def_spec->bind;
	}
	else {
		addrs = &def_addrs;
	}

	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {
		cf_ip_addr resol[CF_SOCK_CFG_MAX];
		uint32_t n_resol = CF_SOCK_CFG_MAX;

		if (cf_ip_addr_from_string_multi(addrs->addrs[i], resol, &n_resol) < 0) {
			cf_crash_nostack(AS_CFG, "Invalid address: %s", addrs->addrs[i]);
		}

		for (uint32_t k = 0; k < n_resol; ++k) {
			cf_ip_addr_copy(&resol[k], &cfg.addr);

			if (cf_serv_cfg_add_sock_cfg(bind, &cfg) < 0) {
				cf_crash_nostack(AS_CFG, "Too many IP addresses: %s", addrs->addrs[i]);
			}
		}
	}
}

static void
addrs_to_access(const cf_addr_list* addrs, cf_addr_list* access)
{
	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {
		cf_ip_addr resol[CF_SOCK_CFG_MAX];
		uint32_t n_resol = CF_SOCK_CFG_MAX;

		if (cf_ip_addr_from_string_multi(addrs->addrs[i], resol, &n_resol) < 0) {
			cf_crash_nostack(AS_CFG, "Invalid access address: %s", addrs->addrs[i]);
		}

		for (uint32_t k = 0; k < n_resol; ++k) {
			if (cf_ip_addr_is_any(&resol[k])) {
				cf_crash_nostack(AS_CFG, "Invalid access address: %s", addrs->addrs[i]);
			}
		}

		if (cf_ip_addr_is_dns_name(addrs->addrs[i])) {
			add_addr(addrs->addrs[i], access);
		}
		else {
			for (uint32_t k = 0; k < n_resol; ++k) {
				char tmp[250];
				cf_ip_addr_to_string_safe(&resol[k], tmp, sizeof(tmp));
				add_addr(tmp, access);
			}
		}
	}
}

void
cfg_serv_spec_std_to_access(const cf_serv_spec* spec, cf_addr_list* access)
{
	addrs_to_access(&spec->std, access);
}

void
cfg_serv_spec_alt_to_access(const cf_serv_spec* spec, cf_addr_list* access)
{
	addrs_to_access(&spec->alt, access);
}

void
cfg_add_mesh_seed_addr_port(char* addr, cf_ip_port port, bool tls)
{
	int32_t i;

	for (i = 0; i < AS_CLUSTER_SZ; i++) {
		if (g_config.hb_config.mesh_seed_addrs[i] == NULL) {
			g_config.hb_config.mesh_seed_addrs[i] = addr;
			g_config.hb_config.mesh_seed_ports[i] = port;
			g_config.hb_config.mesh_seed_tls[i] = tls;
			break;
		}
	}

	if (i == AS_CLUSTER_SZ) {
		cf_crash_nostack(AS_CFG, "can't configure more than %d mesh-seed-address-port entries", AS_CLUSTER_SZ);
	}
}

as_set*
cfg_add_set(as_namespace* ns)
{
	if (ns->sets_cfg_count >= AS_SET_MAX_COUNT) {
		cf_crash_nostack(AS_CFG, "{%s} too many sets", ns->name);
	}

	// Lazily allocate temporary sets config array.
	if (! ns->sets_cfg_array) {
		size_t array_size = AS_SET_MAX_COUNT * sizeof(as_set);

		ns->sets_cfg_array = (as_set*)cf_malloc(array_size);
		memset(ns->sets_cfg_array, 0, array_size);
	}

	return &ns->sets_cfg_array[ns->sets_cfg_count++];
}

void
cfg_add_xmem_mount(as_namespace* ns, const char* mount)
{
	if (ns->n_xmem_mounts == CF_XMEM_MAX_MOUNTS) {
		cf_crash_nostack(AS_CFG, "{%s} too many mounts", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_xmem_mounts; i++) {
		if (strcmp(mount, ns->xmem_mounts[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate mount %s", ns->name, mount);
		}
	}

	ns->xmem_mounts[ns->n_xmem_mounts++] = mount;
}

void
cfg_add_storage_file(as_namespace* ns, const char* file_name,
		const char* shadow_name)
{
	if (ns->n_storage_devices != 0) {
		cf_crash_nostack(AS_CFG, "{%s} mixture of storage files and devices", ns->name);
	}

	if (ns->n_storage_files == AS_STORAGE_MAX_DEVICES) {
		cf_crash_nostack(AS_CFG, "{%s} too many storage files", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_storage_files; i++) {
		if (strcmp(file_name, ns->storage_devices[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage file %s", ns->name, file_name);
		}

		if (shadow_name && ns->storage_shadows[i] &&
				strcmp(shadow_name, ns->storage_shadows[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage shadow file %s", ns->name, shadow_name);
		}
	}

	if (shadow_name) {
		ns->storage_shadows[ns->n_storage_shadows++] = shadow_name;
	}

	ns->storage_devices[ns->n_storage_files++] = file_name;

	if (ns->n_storage_shadows != 0 &&
			ns->n_storage_shadows != ns->n_storage_files) {
		cf_crash_nostack(AS_CFG, "{%s} no shadow for file %s", ns->name, file_name);
	}
}

void
cfg_add_storage_device(as_namespace* ns, const char* device_name,
		const char* shadow_name)
{
	if (ns->n_storage_files != 0) {
		cf_crash_nostack(AS_CFG, "{%s} mixture of storage files and devices", ns->name);
	}

	if (ns->n_storage_devices == AS_STORAGE_MAX_DEVICES) {
		cf_crash_nostack(AS_CFG, "{%s} too many storage devices", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_storage_devices; i++) {
		if (strcmp(device_name, ns->storage_devices[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage device %s", ns->name, device_name);
		}

		if (shadow_name && ns->storage_shadows[i] &&
				strcmp(shadow_name, ns->storage_shadows[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage shadow device %s", ns->name, shadow_name);
		}
	}

	if (shadow_name) {
		ns->storage_shadows[ns->n_storage_shadows++] = shadow_name;
	}

	ns->storage_devices[ns->n_storage_devices++] = device_name;

	if (ns->n_storage_shadows != 0 &&
			ns->n_storage_shadows != ns->n_storage_devices) {
		cf_crash_nostack(AS_CFG, "{%s} no shadow for device %s", ns->name, device_name);
	}
}

void
cfg_set_cluster_name(char* cluster_name){
	if(!as_config_cluster_name_set(cluster_name)){
		cf_crash_nostack(AS_CFG, "cluster name '%s' is not allowed", cluster_name);
	}
}

void
cfg_add_ldap_role_query_pattern(char* pattern)
{
	int i;

	for (i = 0; i < MAX_ROLE_QUERY_PATTERNS; i++) {
		if (g_config.sec_cfg.ldap_role_query_patterns[i] == NULL) {
			g_config.sec_cfg.ldap_role_query_patterns[i] = pattern;
			break;
		}
	}

	if (i == MAX_ROLE_QUERY_PATTERNS) {
		cf_crash_nostack(AS_CFG, "too many ldap role query patterns");
	}
}


//==========================================================
// Other (non-item-specific) utilities.
//

void
create_and_check_hist_track(cf_hist_track** h, const char* name,
		histogram_scale scale)
{
	*h = cf_hist_track_create(name, scale);

	as_config* c = &g_config;

	if (c->hist_track_back != 0 &&
			! cf_hist_track_start(*h, c->hist_track_back, c->hist_track_slice, c->hist_track_thresholds)) {
		cf_crash_nostack(AS_AS, "couldn't enable histogram tracking: %s", name);
	}
}

// TODO - not really a config method any more, reorg needed.
void
cfg_create_all_histograms()
{
	g_stats.batch_index_hist = histogram_create("batch-index", HIST_MILLISECONDS);
	g_stats.info_hist = histogram_create("info", HIST_MILLISECONDS);
	g_stats.svc_demarshal_hist = histogram_create("svc-demarshal", HIST_MILLISECONDS);
	g_stats.svc_queue_hist = histogram_create("svc-queue", HIST_MILLISECONDS);

	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-send-init", HIST_MILLISECONDS);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-send-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-recv-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-recv-cb", HIST_MILLISECONDS);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-send-init", HIST_MILLISECONDS);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-send-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-recv-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-recv-cb", HIST_MILLISECONDS);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-send-init", HIST_MILLISECONDS);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-send-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-recv-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-recv-cb", HIST_MILLISECONDS);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-send-init", HIST_MILLISECONDS);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-send-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-recv-fragment", HIST_MILLISECONDS);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-recv-cb", HIST_MILLISECONDS);
}

void
cfg_init_serv_spec(cf_serv_spec* spec_p)
{
	spec_p->bind_port = 0;
	init_addr_list(&spec_p->bind);
	spec_p->std_port = 0;
	init_addr_list(&spec_p->std);
	spec_p->alt_port = 0;
	init_addr_list(&spec_p->alt);
	spec_p->tls_our_name = NULL;
	spec_p->n_tls_peer_names = 0;
	memset(spec_p->tls_peer_names, 0, sizeof(spec_p->tls_peer_names));
}

cf_tls_spec*
cfg_create_tls_spec(as_config* cfg, char* name)
{
	uint32_t ind = cfg->n_tls_specs++;

	if (ind >= MAX_TLS_SPECS) {
		cf_crash_nostack(AS_CFG, "too many TLS configuration sections");
	}

	cf_tls_spec* tls_spec = cfg->tls_specs + ind;
	tls_spec->name = cf_strdup(name);
	return tls_spec;
}

char*
cfg_resolve_tls_name(char* tls_name, const char* cluster_name, const char* which)
{
	bool expanded = false;

	if (strcmp(tls_name, "<hostname>") == 0) {
		char hostname[1024];
		int rv = gethostname(hostname, sizeof(hostname));
		if (rv != 0) {
			cf_crash_nostack(AS_CFG,
				"trouble resolving hostname for tls-name: %s", cf_strerror(errno));
		}
		hostname[sizeof(hostname)-1] = '\0'; // POSIX.1-2001
		cf_free(tls_name);
		tls_name = cf_strdup(hostname);
		expanded = true;
	}
	else if (strcmp(tls_name, "<cluster-name>") == 0) {
		if (strlen(cluster_name) == 0) {
			cf_crash_nostack
				(AS_CFG, "can't resolve tls-name to non-existent cluster-name");
		}
		cf_free(tls_name);
		tls_name = cf_strdup(cluster_name);
		expanded = true;
	}

	if (expanded && which != NULL) {
		cf_info(AS_CFG, "%s tls-name %s", which, tls_name);
	}

	return tls_name;
}

cf_tls_spec*
cfg_link_tls(const char* which, char** our_name)
{
	if (*our_name == NULL) {
		cf_crash_nostack(AS_CFG, "%s TLS configuration requires tls-name", which);
	}

	*our_name = cfg_resolve_tls_name(*our_name, g_config.cluster_name, which);
	cf_tls_spec* tls_spec = NULL;

	for (uint32_t i = 0; i < g_config.n_tls_specs; ++i) {
		if (strcmp(*our_name, g_config.tls_specs[i].name) == 0) {
			tls_spec = g_config.tls_specs + i;
			break;
		}
	}

	if (tls_spec == NULL) {
		cf_crash_nostack(AS_CFG, "invalid tls-name in TLS configuration: %s",
				*our_name);
	}

	return tls_spec;
}

void
cfg_keep_cap(bool keep, bool* what, int32_t cap)
{
	*what = keep;

	if (keep) {
		cf_process_add_runtime_cap(cap);
	}
}


//==========================================================
// XDR utilities.
//

xdr_dest_config*
xdr_cfg_add_datacenter(char* name)
{
	if (g_dc_count == DC_MAX_NUM) {
		cf_crash_nostack(AS_CFG, "Cannot have more than %d datacenters", DC_MAX_NUM);
	}

	xdr_dest_config* dest_cfg = &g_dest_xcfg_opt[g_dc_count];

	dest_cfg->name = name;
	dest_cfg->id = g_dc_count;
	xdr_config_dest_defaults(dest_cfg);

	return dest_cfg;
}

void
xdr_cfg_associate_datacenter(char* dc, uint32_t nsid)
{
	cf_vector* v = &g_config.namespaces[nsid-1]->xdr_dclist_v;

	// Crash if datacenter with same name already exists.
	for (uint32_t index = 0; index < cf_vector_size(v); index++) {
		if (strcmp((char *)cf_vector_pointer_get(v, index), dc) == 0) {
			cf_crash_nostack(AS_XDR, "datacenter %s already exists for namespace %s - please remove duplicate entries from config file",
					dc, g_config.namespaces[nsid-1]->name);
		}
	}

	// Add the string pointer (of the datacenter name) to the vector.
	cf_vector_pointer_append(v, dc);
}

void
xdr_cfg_add_http_url(xdr_dest_config* dest_cfg, char* url)
{
	// Remember that we are only putting pointer in the vector
	cf_vector_append(&dest_cfg->http.urls, &url);
}

void
xdr_cfg_add_node_addr_port(xdr_dest_config* dest_cfg, char* addr, int port)
{
	xdr_cfg_add_tls_node(dest_cfg, addr, NULL, port);
}

void
xdr_cfg_add_tls_node(xdr_dest_config* dest_cfg, char* addr, char* tls_name, int port)
{
	// Add the element to the vector.
	node_addr_port* nap = (node_addr_port*)cf_malloc(sizeof(node_addr_port));

	nap->addr = addr;
	nap->tls_name = tls_name;
	nap->port = port;

	cf_vector_pointer_append(&dest_cfg->aero.dc_nodes, nap);
}
