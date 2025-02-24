/*
 * cfg.c
 *
 * Copyright (C) 2008-2025 Aerospike, Inc.
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
#include <linux/fs.h> // for BLKGETSIZE64
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <syslog.h>

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "arenax.h"
#include "bits.h"
#include "cf_mutex.h"
#include "cf_str.h"
#include "daemon.h"
#include "dynbuf.h"
#include "hardware.h"
#include "hist.h"
#include "log.h"
#include "msg.h"
#include "node.h"
#include "os.h"
#include "secrets.h"
#include "socket.h"
#include "tls.h"
#include "vault.h"
#include "vector.h"
#include "xmem.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/security_config.h"
#include "base/service.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/thr_info_port.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "fabric/partition_balance.h"
#include "sindex/sindex.h"
#include "sindex/sindex_arena.h"
#include "storage/storage.h"


//==========================================================
// Globals.
//

// The runtime configuration instance.
as_config g_config;


//==========================================================
// Forward declarations.
//

static void cfg_add_feature_key_file(const char* path);
static void init_addr_list(cf_addr_list* addrs);
static void add_addr(const char* name, cf_addr_list* addrs);
static void add_tls_peer_name(const char* name, cf_serv_spec* spec);
static void copy_addrs(const cf_addr_list* from, cf_addr_list* to);
static void default_addrs(cf_addr_list* one, cf_addr_list* two);
static void bind_to_access(const cf_serv_spec* from, cf_addr_list* to);
static void cfg_add_addr_bind(const char* name, cf_serv_spec* spec);
static void cfg_add_addr_std(const char* name, cf_serv_spec* spec);
static void cfg_add_addr_alt(const char* name, cf_serv_spec* spec);
static void cfg_mserv_config_from_addrs(cf_addr_list* addrs, cf_addr_list* bind_addrs, cf_mserv_cfg* serv_cfg, cf_ip_port port, cf_sock_owner owner, uint8_t ttl);
static void cfg_serv_spec_to_bind(const cf_serv_spec* spec, const cf_serv_spec* def_spec, cf_serv_cfg* bind, cf_sock_owner owner);
static void cfg_serv_spec_std_to_access(const cf_serv_spec* spec, cf_addr_list* access);
static void cfg_serv_spec_alt_to_access(const cf_serv_spec* spec, cf_addr_list* access);
static void cfg_add_mesh_seed_addr_port(char* addr, cf_ip_port port, bool tls);
static void cfg_add_secrets_addr_port(char* addr, char* port, char* tls_name);
static as_set* cfg_add_set(as_namespace* ns);
static uint32_t cfg_check_set_default_ttl(const char* ns_name, const char* set_name, uint32_t value);
static void cfg_add_pi_xmem_mount(as_namespace* ns, const char* mount);
static void cfg_add_si_xmem_mount(as_namespace* ns, const char* mount);
static void cfg_add_mem_shadow_file(as_namespace* ns, const char* file_name);
static void cfg_add_storage_file(as_namespace* ns, const char* file_name, const char* shadow_name);
static void cfg_add_mem_shadow_device(as_namespace* ns, const char* device_name);
static void cfg_add_storage_device(as_namespace* ns, const char* device_name, const char* shadow_name);
static void cfg_set_cluster_name(char* cluster_name);
static void cfg_add_ldap_role_query_pattern(char* pattern);
static void cfg_create_all_histograms();
static void cfg_init_serv_spec(cf_serv_spec* spec_p);
static cf_tls_spec* cfg_create_tls_spec(as_config* cfg, char* name);
static void cfg_keep_cap(bool keep, bool* what, int32_t cap);
static void cfg_best_practices_check(void);


//==========================================================
// Inlines & macros.
//

#define check_failed(_db, _name, _msg, ...) \
	do { \
		cf_warning(AS_CFG, "failed " _name " check - " _msg, ##__VA_ARGS__); \
		cf_dyn_buf_append_string(_db, _name); \
		cf_dyn_buf_append_char(_db, ','); \
	} \
	while (false)


//==========================================================
// Helper - set as_config defaults.
//

static void
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
	c->n_proto_fd_max = 15000;
	c->batch_max_buffers_per_queue = 255; // maximum number of buffers allowed in a single queue
	c->batch_max_unused_buffers = 256; // maximum number of buffers allowed in batch buffer pool
	c->feature_key_files[0] = "/etc/aerospike/features.conf";
	c->info_max_ns = MAX_INFO_MAX_MS * 1000000UL;
	c->n_info_threads = 16;
	c->migrate_max_num_incoming = AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING; // for receiver-side migration flow-control
	c->n_migrate_threads = 1;
	cf_os_use_group_perms(false);
	c->query_max_done = 100;
	c->n_query_threads_limit = 128;
	c->run_as_daemon = true; // set false only to run in debugger & see console output
	c->sindex_builder_threads = 4;
	c->sindex_gc_period = 10; // every 10 seconds
	c->ticker_interval = 10;
	c->transaction_max_ns = 1000 * 1000 * 1000; // 1 second
	c->transaction_retry_ms = 1000 + 2; // 1 second + epsilon, so default timeout happens first
	c->work_directory = "/opt/aerospike";

	// Network heartbeat defaults.
	c->hb_config.mode = AS_HB_MODE_UNDEF;
	c->hb_config.tx_interval = 150;
	c->hb_config.max_intervals_missed = 10;
	c->hb_config.connect_timeout_ms = 500;
	c->hb_config.protocol = AS_HB_PROTOCOL_V3;
	c->hb_config.override_mtu = 0;

	// Fabric defaults.
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK] = 2;
	c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_BULK] = 1;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL] = 1;
	c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_CTRL] = 1;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_META] = 1;
	c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_META] = 1;
	c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META] = 4;
	c->n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW] = 8;
	c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW] = 1;
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

	// Mod-lua defaults.
	c->mod_lua.server_mode = true;
	c->mod_lua.cache_enabled = true;
	strcpy(c->mod_lua.user_path, "/opt/aerospike/usr/udf/lua");

	// TODO - security set default config API?
	// Security defaults.
	c->sec_cfg.privilege_refresh_period = 60 * 5; // refresh socket privileges every 5 minutes
	c->sec_cfg.session_ttl = 60 * 60 * 24;
	c->sec_cfg.tps_weight = TPS_WEIGHT_MIN;
	// Security LDAP defaults.
	c->sec_cfg.n_ldap_login_threads = 8;
	c->sec_cfg.ldap_polling_period = 60 * 5;
	c->sec_cfg.ldap_token_hash_method = AS_LDAP_EVP_SHA_256;
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
	// Enterprise-only:
	CASE_SECURITY_BEGIN,
	CASE_XDR_BEGIN,

	// Service options:
	CASE_SERVICE_ADVERTISE_IPV6,
	CASE_SERVICE_AUTO_PIN,
	CASE_SERVICE_BATCH_INDEX_THREADS,
	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE,
	CASE_SERVICE_BATCH_MAX_REQUESTS,
	CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS,
	CASE_SERVICE_CLUSTER_NAME,
	CASE_SERVICE_DEBUG_ALLOCATIONS,
	CASE_SERVICE_DISABLE_UDF_EXECUTION,
	CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC,
	CASE_SERVICE_ENABLE_HEALTH_CHECK,
	CASE_SERVICE_ENABLE_HIST_INFO,
	CASE_SERVICE_ENFORCE_BEST_PRACTICES,
	CASE_SERVICE_FEATURE_KEY_FILE,
	CASE_SERVICE_GROUP,
	CASE_SERVICE_INDENT_ALLOCATIONS,
	CASE_SERVICE_INFO_MAX_MS,
	CASE_SERVICE_INFO_THREADS,
	CASE_SERVICE_KEEP_CAPS_SSD_HEALTH,
	CASE_SERVICE_LOG_LOCAL_TIME,
	CASE_SERVICE_LOG_MILLIS,
	CASE_SERVICE_MICROSECOND_HISTOGRAMS,
	CASE_SERVICE_MIGRATE_FILL_DELAY,
	CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING,
	CASE_SERVICE_MIGRATE_THREADS,
	CASE_SERVICE_MIN_CLUSTER_SIZE,
	CASE_SERVICE_NODE_ID,
	CASE_SERVICE_NODE_ID_INTERFACE,
	CASE_SERVICE_OS_GROUP_PERMS,
	CASE_SERVICE_PIDFILE,
	CASE_SERVICE_POISON_ALLOCATIONS,
	CASE_SERVICE_PROTO_FD_IDLE_MS,
	CASE_SERVICE_PROTO_FD_MAX,
	CASE_SERVICE_QUARANTINE_ALLOCATIONS,
	CASE_SERVICE_QUERY_MAX_DONE,
	CASE_SERVICE_QUERY_THREADS_LIMIT,
	CASE_SERVICE_RUN_AS_DAEMON,
	CASE_SERVICE_SECRETS_ADDRESS_PORT,
	CASE_SERVICE_SECRETS_TLS_CONTEXT,
	CASE_SERVICE_SECRETS_UDS_PATH,
	CASE_SERVICE_SERVICE_THREADS,
	CASE_SERVICE_SINDEX_BUILDER_THREADS,
	CASE_SERVICE_SINDEX_GC_PERIOD,
	CASE_SERVICE_STAY_QUIESCED,
	CASE_SERVICE_TICKER_INTERVAL,
	CASE_SERVICE_TLS_REFRESH_PERIOD,
	CASE_SERVICE_TRANSACTION_MAX_MS,
	CASE_SERVICE_TRANSACTION_RETRY_MS,
	CASE_SERVICE_USER,
	CASE_SERVICE_VAULT_CA,
	CASE_SERVICE_VAULT_NAMESPACE,
	CASE_SERVICE_VAULT_PATH,
	CASE_SERVICE_VAULT_TOKEN_FILE,
	CASE_SERVICE_VAULT_URL,
	CASE_SERVICE_WORK_DIRECTORY,
	// Obsoleted:
	CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS,
	CASE_SERVICE_NSUP_PERIOD,
	CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD,
	CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION,
	CASE_SERVICE_SCAN_MAX_DONE,
	CASE_SERVICE_SCAN_THREADS_LIMIT,
	CASE_SERVICE_TRANSACTION_PENDING_LIMIT,
	CASE_SERVICE_TRANSACTION_QUEUES,
	CASE_SERVICE_TRANSACTION_REPEATABLE_READ,
	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE,

	// Service auto-pin options (value tokens):
	CASE_SERVICE_AUTO_PIN_NONE,
	CASE_SERVICE_AUTO_PIN_CPU,
	CASE_SERVICE_AUTO_PIN_NUMA,
	CASE_SERVICE_AUTO_PIN_ADQ,

	// Logging options:
	// Sub-contexts:
	CASE_LOG_CONSOLE_BEGIN,
	CASE_LOG_FILE_BEGIN,
	CASE_LOG_SYSLOG_BEGIN,

	// Logging console and file options:
	CASE_LOG_CONTEXT_CONTEXT,

	// Logging syslog options:
	CASE_LOG_SYSLOG_CONTEXT,
	CASE_LOG_SYSLOG_FACILITY,
	CASE_LOG_SYSLOG_PATH,
	CASE_LOG_SYSLOG_TAG,

	// Network options:
	// Sub-contexts, in canonical configuration file order:
	CASE_NETWORK_SERVICE_BEGIN,
	CASE_NETWORK_HEARTBEAT_BEGIN,
	CASE_NETWORK_FABRIC_BEGIN,
	CASE_NETWORK_INFO_BEGIN,
	CASE_NETWORK_TLS_BEGIN,

	// Network service options:
	CASE_NETWORK_SERVICE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_ACCESS_PORT,
	CASE_NETWORK_SERVICE_ADDRESS,
	CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT,
	CASE_NETWORK_SERVICE_DISABLE_LOCALHOST,
	CASE_NETWORK_SERVICE_PORT,
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
	CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS,
	CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME,

	// Network heartbeat options:
	CASE_NETWORK_HEARTBEAT_ADDRESS,
	CASE_NETWORK_HEARTBEAT_CONNECT_TIMEOUT_MS,
	CASE_NETWORK_HEARTBEAT_INTERVAL,
	CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT,
	CASE_NETWORK_HEARTBEAT_MODE,
	CASE_NETWORK_HEARTBEAT_MTU,
	CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP,
	CASE_NETWORK_HEARTBEAT_MULTICAST_TTL,
	CASE_NETWORK_HEARTBEAT_PORT,
	CASE_NETWORK_HEARTBEAT_PROTOCOL,
	CASE_NETWORK_HEARTBEAT_TIMEOUT,
	CASE_NETWORK_HEARTBEAT_TLS_ADDRESS,
	CASE_NETWORK_HEARTBEAT_TLS_MESH_SEED_ADDRESS_PORT,
	CASE_NETWORK_HEARTBEAT_TLS_NAME,
	CASE_NETWORK_HEARTBEAT_TLS_PORT,
	// Obsoleted:
	CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS,
	CASE_NETWORK_HEARTBEAT_MCAST_TTL,

	// Network heartbeat mode options (value tokens):
	CASE_NETWORK_HEARTBEAT_MODE_MESH,
	CASE_NETWORK_HEARTBEAT_MODE_MULTICAST,

	// Network heartbeat protocol options (value tokens):
	CASE_NETWORK_HEARTBEAT_PROTOCOL_NONE,
	CASE_NETWORK_HEARTBEAT_PROTOCOL_V3,

	// Network fabric options:
	CASE_NETWORK_FABRIC_ADDRESS,
	CASE_NETWORK_FABRIC_CHANNEL_BULK_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_BULK_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_CTRL_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_CTRL_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_META_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_META_RECV_THREADS,
	CASE_NETWORK_FABRIC_CHANNEL_RW_FDS,
	CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_POOLS,
	CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS,
	CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED,
	CASE_NETWORK_FABRIC_KEEPALIVE_INTVL,
	CASE_NETWORK_FABRIC_KEEPALIVE_PROBES,
	CASE_NETWORK_FABRIC_KEEPALIVE_TIME,
	CASE_NETWORK_FABRIC_LATENCY_MAX_MS,
	CASE_NETWORK_FABRIC_PORT,
	CASE_NETWORK_FABRIC_RECV_REARM_THRESHOLD,
	CASE_NETWORK_FABRIC_SEND_THREADS,
	CASE_NETWORK_FABRIC_TLS_ADDRESS,
	CASE_NETWORK_FABRIC_TLS_NAME,
	CASE_NETWORK_FABRIC_TLS_PORT,

	// Network info options:
	CASE_NETWORK_INFO_ADDRESS,
	CASE_NETWORK_INFO_PORT,

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
	CASE_NAMESPACE_ACTIVE_RACK,
	CASE_NAMESPACE_ALLOW_TTL_WITHOUT_NSUP,
	CASE_NAMESPACE_AUTO_REVIVE,
	CASE_NAMESPACE_BACKGROUND_QUERY_MAX_RPS,
	CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY,
	CASE_NAMESPACE_CONFLICT_RESOLVE_WRITES,
	CASE_NAMESPACE_DEFAULT_READ_TOUCH_TTL_PCT,
	CASE_NAMESPACE_DEFAULT_TTL,
	CASE_NAMESPACE_DISABLE_COLD_START_EVICTION,
	CASE_NAMESPACE_DISABLE_MRT_WRITES,
	CASE_NAMESPACE_DISABLE_WRITE_DUP_RES,
	CASE_NAMESPACE_DISALLOW_EXPUNGE,
	CASE_NAMESPACE_DISALLOW_NULL_SETNAME,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_OPS_SUB,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_READ,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB,
	CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE,
	CASE_NAMESPACE_ENABLE_HIST_PROXY,
	CASE_NAMESPACE_EVICT_HIST_BUCKETS,
	CASE_NAMESPACE_EVICT_INDEXES_MEMORY_PCT,
	CASE_NAMESPACE_EVICT_TENTHS_PCT,
	CASE_NAMESPACE_IGNORE_MIGRATE_FILL_DELAY,
	CASE_NAMESPACE_INDEX_STAGE_SIZE,
	CASE_NAMESPACE_INDEXES_MEMORY_BUDGET,
	CASE_NAMESPACE_INLINE_SHORT_QUERIES,
	CASE_NAMESPACE_MAX_RECORD_SIZE,
	CASE_NAMESPACE_MIGRATE_ORDER,
	CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS,
	CASE_NAMESPACE_MIGRATE_SKIP_UNREADABLE,
	CASE_NAMESPACE_MIGRATE_SLEEP,
	CASE_NAMESPACE_MRT_DURATION,
	CASE_NAMESPACE_NSUP_HIST_PERIOD,
	CASE_NAMESPACE_NSUP_PERIOD,
	CASE_NAMESPACE_NSUP_THREADS,
	CASE_NAMESPACE_PARTITION_TREE_SPRIGS,
	CASE_NAMESPACE_PREFER_UNIFORM_BALANCE,
	CASE_NAMESPACE_RACK_ID,
	CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE,
	CASE_NAMESPACE_REJECT_NON_XDR_WRITES,
	CASE_NAMESPACE_REJECT_XDR_WRITES,
	CASE_NAMESPACE_REPLICATION_FACTOR,
	CASE_NAMESPACE_SINDEX_STAGE_SIZE,
	CASE_NAMESPACE_SINGLE_QUERY_THREADS,
	CASE_NAMESPACE_STOP_WRITES_SYS_MEMORY_PCT,
	CASE_NAMESPACE_STRONG_CONSISTENCY,
	CASE_NAMESPACE_STRONG_CONSISTENCY_ALLOW_EXPUNGE,
	CASE_NAMESPACE_TOMB_RAIDER_ELIGIBLE_AGE,
	CASE_NAMESPACE_TOMB_RAIDER_PERIOD,
	CASE_NAMESPACE_TRANSACTION_PENDING_LIMIT,
	CASE_NAMESPACE_TRUNCATE_THREADS,
	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE,
	CASE_NAMESPACE_XDR_BIN_TOMBSTONE_TTL,
	CASE_NAMESPACE_XDR_TOMB_RAIDER_PERIOD,
	CASE_NAMESPACE_XDR_TOMB_RAIDER_THREADS,
	// Sub-contexts:
	CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN,
	CASE_NAMESPACE_INDEX_TYPE_BEGIN,
	CASE_NAMESPACE_SET_BEGIN,
	CASE_NAMESPACE_SINDEX_TYPE_BEGIN,
	CASE_NAMESPACE_STORAGE_ENGINE_BEGIN,
	// Obsoleted:
	CASE_NAMESPACE_BACKGROUND_SCAN_MAX_RPS,
	CASE_NAMESPACE_DATA_IN_INDEX,
	CASE_NAMESPACE_DISABLE_NSUP,
	CASE_NAMESPACE_EVICT_SYS_MEMORY_PCT,
	CASE_NAMESPACE_HIGH_WATER_DISK_PCT,
	CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT,
	CASE_NAMESPACE_MEMORY_SIZE,
	CASE_NAMESPACE_SINGLE_BIN,
	CASE_NAMESPACE_SINGLE_SCAN_THREADS,
	CASE_NAMESPACE_STOP_WRITES_PCT,

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

	// Namespace sindex-type options (value tokens):
	CASE_NAMESPACE_SINDEX_TYPE_SHMEM,
	CASE_NAMESPACE_SINDEX_TYPE_PMEM,
	CASE_NAMESPACE_SINDEX_TYPE_FLASH,

	// Namespace storage-engine options (value tokens):
	CASE_NAMESPACE_STORAGE_MEMORY,
	CASE_NAMESPACE_STORAGE_PMEM,
	CASE_NAMESPACE_STORAGE_DEVICE,

	// Namespace index-type pmem options:
	CASE_NAMESPACE_INDEX_TYPE_PMEM_EVICT_MOUNTS_PCT,
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT,
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_BUDGET,
	// Obsoleted:
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT,

	// Namespace index-type flash options:
	CASE_NAMESPACE_INDEX_TYPE_FLASH_EVICT_MOUNTS_PCT,
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT,
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_BUDGET,
	// Obsoleted:
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT,

	// Namespace sindex-type pmem options:
	CASE_NAMESPACE_SINDEX_TYPE_PMEM_EVICT_MOUNTS_PCT,
	CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNT,
	CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_BUDGET,
	// Obsoleted:
	CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT,

	// Namespace sindex-type flash options:
	CASE_NAMESPACE_SINDEX_TYPE_FLASH_EVICT_MOUNTS_PCT,
	CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNT,
	CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_BUDGET,
	// Obsoleted:
	CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT,
	CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT,

	// Namespace storage-engine memory options:
	CASE_NAMESPACE_STORAGE_MEMORY_COMMIT_TO_DEVICE,
	CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION,
	CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_ACCELERATION,
	CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_LEVEL,
	CASE_NAMESPACE_STORAGE_MEMORY_DATA_SIZE,
	CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_LWM_PCT,
	CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_QUEUE_MIN,
	CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_SLEEP,
	CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_STARTUP_MINIMUM,
	CASE_NAMESPACE_STORAGE_MEMORY_DEVICE,
	CASE_NAMESPACE_STORAGE_MEMORY_DIRECT_FILES,
	CASE_NAMESPACE_STORAGE_MEMORY_DISABLE_ODSYNC,
	CASE_NAMESPACE_STORAGE_MEMORY_ENABLE_BENCHMARKS_STORAGE,
	CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION,
	CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_KEY_FILE,
	CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_OLD_KEY_FILE,
	CASE_NAMESPACE_STORAGE_MEMORY_EVICT_USED_PCT,
	CASE_NAMESPACE_STORAGE_MEMORY_FILE,
	CASE_NAMESPACE_STORAGE_MEMORY_FILESIZE,
	CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_MAX_MS,
	CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_SIZE,
	CASE_NAMESPACE_STORAGE_MEMORY_MAX_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_USED_PCT,
	CASE_NAMESPACE_STORAGE_MEMORY_TOMB_RAIDER_SLEEP,
	// Obsoleted:
	CASE_NAMESPACE_STORAGE_MEMORY_MAX_USED_PCT,
	CASE_NAMESPACE_STORAGE_MEMORY_MIN_AVAIL_PCT,

	// Namespace storage-engine pmem options:
	CASE_NAMESPACE_STORAGE_PMEM_COMMIT_TO_DEVICE,
	CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION,
	CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_ACCELERATION,
	CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_LEVEL,
	CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_LWM_PCT,
	CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_QUEUE_MIN,
	CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_SLEEP,
	CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_STARTUP_MINIMUM,
	CASE_NAMESPACE_STORAGE_PMEM_DIRECT_FILES,
	CASE_NAMESPACE_STORAGE_PMEM_DISABLE_ODSYNC,
	CASE_NAMESPACE_STORAGE_PMEM_ENABLE_BENCHMARKS_STORAGE,
	CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION,
	CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_KEY_FILE,
	CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_OLD_KEY_FILE,
	CASE_NAMESPACE_STORAGE_PMEM_EVICT_USED_PCT,
	CASE_NAMESPACE_STORAGE_PMEM_FILE,
	CASE_NAMESPACE_STORAGE_PMEM_FILESIZE,
	CASE_NAMESPACE_STORAGE_PMEM_FLUSH_MAX_MS,
	CASE_NAMESPACE_STORAGE_PMEM_MAX_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_USED_PCT,
	CASE_NAMESPACE_STORAGE_PMEM_TOMB_RAIDER_SLEEP,
	// Obsoleted:
	CASE_NAMESPACE_STORAGE_PMEM_MAX_USED_PCT,
	CASE_NAMESPACE_STORAGE_PMEM_MIN_AVAIL_PCT,

	// Namespace storage-engine device options:
	CASE_NAMESPACE_STORAGE_DEVICE_CACHE_REPLICA_WRITES,
	CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY,
	CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ACCELERATION,
	CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LEVEL,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM,
	CASE_NAMESPACE_STORAGE_DEVICE_DEVICE,
	CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES,
	CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODSYNC,
	CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_KEY_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OLD_KEY_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_EVICT_USED_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS,
	CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_SIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER,
	CASE_NAMESPACE_STORAGE_DEVICE_SINDEX_STARTUP_DEVICE_SCAN,
	CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_USED_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP,
	// Obsoleted:
	CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY,
	CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT,
	CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC,
	CASE_NAMESPACE_STORAGE_DEVICE_MAX_USED_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE,

	// Namespace storage compression options (value tokens):
	CASE_NAMESPACE_STORAGE_COMPRESSION_NONE,
	CASE_NAMESPACE_STORAGE_COMPRESSION_LZ4,
	CASE_NAMESPACE_STORAGE_COMPRESSION_SNAPPY,
	CASE_NAMESPACE_STORAGE_COMPRESSION_ZSTD,

	// Namespace storage encryption options (value tokens):
	CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_128,
	CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_256,

	// Namespace set options:
	CASE_NAMESPACE_SET_DEFAULT_READ_TOUCH_TTL_PCT,
	CASE_NAMESPACE_SET_DEFAULT_TTL,
	CASE_NAMESPACE_SET_DISABLE_EVICTION,
	CASE_NAMESPACE_SET_ENABLE_INDEX,
	CASE_NAMESPACE_SET_STOP_WRITES_COUNT,
	CASE_NAMESPACE_SET_STOP_WRITES_SIZE,

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

	// Security options:
	CASE_SECURITY_DEFAULT_PASSWORD_FILE,
	CASE_SECURITY_ENABLE_QUOTAS,
	CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD,
	CASE_SECURITY_SESSION_TTL,
	CASE_SECURITY_TPS_WEIGHT,
	// Sub-contexts:
	CASE_SECURITY_LDAP_BEGIN,
	CASE_SECURITY_LOG_BEGIN,
	// Obsoleted:
	CASE_SECURITY_ENABLE_LDAP,
	CASE_SECURITY_ENABLE_SECURITY,
	CASE_SECURITY_SYSLOG_BEGIN,

	// Security LDAP options:
	CASE_SECURITY_LDAP_DISABLE_TLS,
	CASE_SECURITY_LDAP_LOGIN_THREADS,
	CASE_SECURITY_LDAP_POLLING_PERIOD,
	CASE_SECURITY_LDAP_QUERY_BASE_DN,
	CASE_SECURITY_LDAP_QUERY_USER_DN,
	CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE,
	CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN,
	CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN,
	CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU,
	CASE_SECURITY_LDAP_SERVER,
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
	CASE_SECURITY_LOG_REPORT_DATA_OP_ROLE,
	CASE_SECURITY_LOG_REPORT_DATA_OP_USER,
	CASE_SECURITY_LOG_REPORT_SYS_ADMIN,
	CASE_SECURITY_LOG_REPORT_USER_ADMIN,
	CASE_SECURITY_LOG_REPORT_VIOLATION,

	// XDR options:
	CASE_XDR_SRC_ID,
	// Sub-contexts:
	CASE_XDR_DC_BEGIN,

	// XDR (remote) DC options:
	CASE_XDR_DC_AUTH_MODE,
	CASE_XDR_DC_AUTH_PASSWORD_FILE,
	CASE_XDR_DC_AUTH_USER,
	CASE_XDR_DC_CONNECTOR,
	CASE_XDR_DC_MAX_RECOVERIES_INTERLEAVED,
	CASE_XDR_DC_NODE_ADDRESS_PORT,
	CASE_XDR_DC_PERIOD_MS,
	CASE_XDR_DC_TLS_NAME,
	CASE_XDR_DC_USE_ALTERNATE_ACCESS_ADDRESS,
	// Sub-contexts:
	CASE_XDR_DC_NAMESPACE_BEGIN,

	// XDR DC authentication mode (value tokens):
	CASE_XDR_DC_AUTH_MODE_NONE,
	CASE_XDR_DC_AUTH_MODE_INTERNAL,
	CASE_XDR_DC_AUTH_MODE_EXTERNAL,
	CASE_XDR_DC_AUTH_MODE_EXTERNAL_INSECURE,
	CASE_XDR_DC_AUTH_MODE_PKI,

	// XDR DC namespace options:
	CASE_XDR_DC_NAMESPACE_BIN_POLICY,
	CASE_XDR_DC_NAMESPACE_COMPRESSION_LEVEL,
	CASE_XDR_DC_NAMESPACE_COMPRESSION_THRESHOLD,
	CASE_XDR_DC_NAMESPACE_DELAY_MS,
	CASE_XDR_DC_NAMESPACE_ENABLE_COMPRESSION,
	CASE_XDR_DC_NAMESPACE_FORWARD,
	CASE_XDR_DC_NAMESPACE_HOT_KEY_MS,
	CASE_XDR_DC_NAMESPACE_IGNORE_BIN,
	CASE_XDR_DC_NAMESPACE_IGNORE_EXPUNGES,
	CASE_XDR_DC_NAMESPACE_IGNORE_SET,
	CASE_XDR_DC_NAMESPACE_MAX_THROUGHPUT,
	CASE_XDR_DC_NAMESPACE_REMOTE_NAMESPACE,
	CASE_XDR_DC_NAMESPACE_SC_REPLICATION_WAIT_MS,
	CASE_XDR_DC_NAMESPACE_SHIP_BIN,
	CASE_XDR_DC_NAMESPACE_SHIP_BIN_LUTS,
	CASE_XDR_DC_NAMESPACE_SHIP_NSUP_DELETES,
	CASE_XDR_DC_NAMESPACE_SHIP_ONLY_SPECIFIED_SETS,
	CASE_XDR_DC_NAMESPACE_SHIP_SET,
	CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_INTERVAL,
	CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY,
	CASE_XDR_DC_NAMESPACE_TRANSACTION_QUEUE_LIMIT,
	CASE_XDR_DC_NAMESPACE_WRITE_POLICY,

	// XDR DC namespace bin policy (value tokens):
	CASE_XDR_DC_NAMESPACE_BIN_POLICY_ALL,
	CASE_XDR_DC_NAMESPACE_BIN_POLICY_NO_BINS,
	CASE_XDR_DC_NAMESPACE_BIN_POLICY_ONLY_CHANGED,
	CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_AND_SPECIFIED,
	CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_OR_SPECIFIED,

	// XDR DC namespace ship-versions policy (value tokens):
	CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_LATEST,
	CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_ALL,
	CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_INTERVAL,

	// XDR DC namespace write policy (value tokens):
	CASE_XDR_DC_NAMESPACE_WRITE_POLICY_AUTO,
	CASE_XDR_DC_NAMESPACE_WRITE_POLICY_UPDATE,
	CASE_XDR_DC_NAMESPACE_WRITE_POLICY_REPLACE

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
		{ "security",						CASE_SECURITY_BEGIN },
		{ "xdr",							CASE_XDR_BEGIN }
};

const cfg_opt SERVICE_OPTS[] = {
		{ "advertise-ipv6",					CASE_SERVICE_ADVERTISE_IPV6 },
		{ "auto-pin",						CASE_SERVICE_AUTO_PIN },
		{ "batch-index-threads",			CASE_SERVICE_BATCH_INDEX_THREADS },
		{ "batch-max-buffers-per-queue",	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE },
		{ "batch-max-requests",				CASE_SERVICE_BATCH_MAX_REQUESTS },
		{ "batch-max-unused-buffers",		CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS },
		{ "cluster-name",					CASE_SERVICE_CLUSTER_NAME },
		{ "debug-allocations",				CASE_SERVICE_DEBUG_ALLOCATIONS },
		{ "disable-udf-execution",			CASE_SERVICE_DISABLE_UDF_EXECUTION },
		{ "enable-benchmarks-fabric",		CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC },
		{ "enable-health-check",			CASE_SERVICE_ENABLE_HEALTH_CHECK },
		{ "enable-hist-info",				CASE_SERVICE_ENABLE_HIST_INFO },
		{ "enforce-best-practices",			CASE_SERVICE_ENFORCE_BEST_PRACTICES },
		{ "feature-key-file",				CASE_SERVICE_FEATURE_KEY_FILE },
		{ "group",							CASE_SERVICE_GROUP },
		{ "indent-allocations",				CASE_SERVICE_INDENT_ALLOCATIONS },
		{ "info-max-ms",					CASE_SERVICE_INFO_MAX_MS },
		{ "info-threads",					CASE_SERVICE_INFO_THREADS },
		{ "keep-caps-ssd-health",			CASE_SERVICE_KEEP_CAPS_SSD_HEALTH },
		{ "log-local-time",					CASE_SERVICE_LOG_LOCAL_TIME },
		{ "log-millis",						CASE_SERVICE_LOG_MILLIS},
		{ "microsecond-histograms",			CASE_SERVICE_MICROSECOND_HISTOGRAMS },
		{ "migrate-fill-delay",				CASE_SERVICE_MIGRATE_FILL_DELAY },
		{ "migrate-max-num-incoming",		CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING },
		{ "migrate-threads",				CASE_SERVICE_MIGRATE_THREADS },
		{ "min-cluster-size",				CASE_SERVICE_MIN_CLUSTER_SIZE },
		{ "node-id",						CASE_SERVICE_NODE_ID },
		{ "node-id-interface",				CASE_SERVICE_NODE_ID_INTERFACE },
		{ "os-group-perms",					CASE_SERVICE_OS_GROUP_PERMS },
		{ "pidfile",						CASE_SERVICE_PIDFILE },
		{ "poison-allocations",				CASE_SERVICE_POISON_ALLOCATIONS },
		{ "proto-fd-idle-ms",				CASE_SERVICE_PROTO_FD_IDLE_MS },
		{ "proto-fd-max",					CASE_SERVICE_PROTO_FD_MAX },
		{ "quarantine-allocations",			CASE_SERVICE_QUARANTINE_ALLOCATIONS },
		{ "query-max-done",					CASE_SERVICE_QUERY_MAX_DONE },
		{ "query-threads-limit",			CASE_SERVICE_QUERY_THREADS_LIMIT },
		{ "run-as-daemon",					CASE_SERVICE_RUN_AS_DAEMON },
		{ "secrets-address-port",			CASE_SERVICE_SECRETS_ADDRESS_PORT },
		{ "secrets-tls-context",			CASE_SERVICE_SECRETS_TLS_CONTEXT },
		{ "secrets-uds-path",				CASE_SERVICE_SECRETS_UDS_PATH },
		{ "service-threads",				CASE_SERVICE_SERVICE_THREADS },
		{ "sindex-builder-threads",			CASE_SERVICE_SINDEX_BUILDER_THREADS },
		{ "sindex-gc-period",				CASE_SERVICE_SINDEX_GC_PERIOD },
		{ "stay-quiesced",					CASE_SERVICE_STAY_QUIESCED },
		{ "ticker-interval",				CASE_SERVICE_TICKER_INTERVAL },
		{ "tls-refresh-period",				CASE_SERVICE_TLS_REFRESH_PERIOD },
		{ "transaction-max-ms",				CASE_SERVICE_TRANSACTION_MAX_MS },
		{ "transaction-retry-ms",			CASE_SERVICE_TRANSACTION_RETRY_MS },
		{ "user",							CASE_SERVICE_USER },
		{ "vault-ca",						CASE_SERVICE_VAULT_CA },
		{ "vault-namespace",				CASE_SERVICE_VAULT_NAMESPACE },
		{ "vault-path",						CASE_SERVICE_VAULT_PATH },
		{ "vault-token-file",				CASE_SERVICE_VAULT_TOKEN_FILE },
		{ "vault-url",						CASE_SERVICE_VAULT_URL },
		{ "work-directory",					CASE_SERVICE_WORK_DIRECTORY },
		// Obsoleted:
		{ "allow-inline-transactions",		CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS },
		{ "nsup-period",					CASE_SERVICE_NSUP_PERIOD },
		{ "object-size-hist-period",		CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD },
		{ "respond-client-on-master-completion", CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION },
		{ "scan-max-done",					CASE_SERVICE_SCAN_MAX_DONE },
		{ "scan-threads-limit",				CASE_SERVICE_SCAN_THREADS_LIMIT },
		{ "transaction-pending-limit",		CASE_SERVICE_TRANSACTION_PENDING_LIMIT },
		{ "transaction-queues",				CASE_SERVICE_TRANSACTION_QUEUES },
		{ "transaction-repeatable-read",	CASE_SERVICE_TRANSACTION_REPEATABLE_READ },
		{ "transaction-threads-per-queue",	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SERVICE_AUTO_PIN_OPTS[] = {
		{ "none",							CASE_SERVICE_AUTO_PIN_NONE },
		{ "cpu",							CASE_SERVICE_AUTO_PIN_CPU },
		{ "numa",							CASE_SERVICE_AUTO_PIN_NUMA },
		{ "adq",							CASE_SERVICE_AUTO_PIN_ADQ }
};

const cfg_opt LOGGING_OPTS[] = {
		// Sub-contexts:
		{ "console",						CASE_LOG_CONSOLE_BEGIN },
		{ "file",							CASE_LOG_FILE_BEGIN },
		{ "syslog",							CASE_LOG_SYSLOG_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_CONTEXT_OPTS[] = {
		{ "context",						CASE_LOG_CONTEXT_CONTEXT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_SYSLOG_OPTS[] = {
		{ "context",						CASE_LOG_SYSLOG_CONTEXT },
		{ "facility",						CASE_LOG_SYSLOG_FACILITY },
		{ "path",							CASE_LOG_SYSLOG_PATH },
		{ "tag",							CASE_LOG_SYSLOG_TAG },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_OPTS[] = {
		// Sub-contexts, in canonical configuration file order:
		{ "service",						CASE_NETWORK_SERVICE_BEGIN },
		{ "heartbeat",						CASE_NETWORK_HEARTBEAT_BEGIN },
		{ "fabric",							CASE_NETWORK_FABRIC_BEGIN },
		{ "info",							CASE_NETWORK_INFO_BEGIN },
		{ "tls",							CASE_NETWORK_TLS_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_SERVICE_OPTS[] = {
		{ "access-address",					CASE_NETWORK_SERVICE_ACCESS_ADDRESS },
		{ "access-port",					CASE_NETWORK_SERVICE_ACCESS_PORT },
		{ "address",						CASE_NETWORK_SERVICE_ADDRESS },
		{ "alternate-access-address",		CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS },
		{ "alternate-access-port",			CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT },
		{ "disable-localhost",				CASE_NETWORK_SERVICE_DISABLE_LOCALHOST },
		{ "port",							CASE_NETWORK_SERVICE_PORT },
		{ "tls-access-address",				CASE_NETWORK_SERVICE_TLS_ACCESS_ADDRESS },
		{ "tls-access-port",				CASE_NETWORK_SERVICE_TLS_ACCESS_PORT },
		{ "tls-address",					CASE_NETWORK_SERVICE_TLS_ADDRESS },
		{ "tls-alternate-access-address",	CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_ADDRESS },
		{ "tls-alternate-access-port",		CASE_NETWORK_SERVICE_TLS_ALTERNATE_ACCESS_PORT },
		{ "tls-authenticate-client",		CASE_NETWORK_SERVICE_TLS_AUTHENTICATE_CLIENT },
		{ "tls-name",						CASE_NETWORK_SERVICE_TLS_NAME },
		{ "tls-port",						CASE_NETWORK_SERVICE_TLS_PORT },
		// Obsoleted:
		{ "alternate-address",				CASE_NETWORK_SERVICE_ALTERNATE_ADDRESS },
		{ "external-address",				CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS },
		{ "network-interface-name",			CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_HEARTBEAT_OPTS[] = {
		{ "address",						CASE_NETWORK_HEARTBEAT_ADDRESS },
		{ "connect-timeout-ms",				CASE_NETWORK_HEARTBEAT_CONNECT_TIMEOUT_MS },
		{ "interval",						CASE_NETWORK_HEARTBEAT_INTERVAL },
		{ "mesh-seed-address-port",			CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT },
		{ "mode",							CASE_NETWORK_HEARTBEAT_MODE },
		{ "mtu",							CASE_NETWORK_HEARTBEAT_MTU },
		{ "multicast-group",				CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP },
		{ "multicast-ttl",					CASE_NETWORK_HEARTBEAT_MULTICAST_TTL },
		{ "port",							CASE_NETWORK_HEARTBEAT_PORT },
		{ "protocol",						CASE_NETWORK_HEARTBEAT_PROTOCOL },
		{ "timeout",						CASE_NETWORK_HEARTBEAT_TIMEOUT },
		{ "tls-address",					CASE_NETWORK_HEARTBEAT_TLS_ADDRESS },
		{ "tls-mesh-seed-address-port",		CASE_NETWORK_HEARTBEAT_TLS_MESH_SEED_ADDRESS_PORT },
		{ "tls-name",						CASE_NETWORK_HEARTBEAT_TLS_NAME },
		{ "tls-port",						CASE_NETWORK_HEARTBEAT_TLS_PORT },
		// Obsoleted:
		{ "interface-address",				CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS },
		{ "mcast-ttl",						CASE_NETWORK_HEARTBEAT_MCAST_TTL },
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
		{ "channel-bulk-fds",				CASE_NETWORK_FABRIC_CHANNEL_BULK_FDS },
		{ "channel-bulk-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_BULK_RECV_THREADS },
		{ "channel-ctrl-fds",				CASE_NETWORK_FABRIC_CHANNEL_CTRL_FDS },
		{ "channel-ctrl-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_CTRL_RECV_THREADS },
		{ "channel-meta-fds",				CASE_NETWORK_FABRIC_CHANNEL_META_FDS },
		{ "channel-meta-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_META_RECV_THREADS },
		{ "channel-rw-fds",					CASE_NETWORK_FABRIC_CHANNEL_RW_FDS },
		{ "channel-rw-recv-pools",			CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_POOLS },
		{ "channel-rw-recv-threads",		CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS },
		{ "keepalive-enabled",				CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED },
		{ "keepalive-intvl",				CASE_NETWORK_FABRIC_KEEPALIVE_INTVL },
		{ "keepalive-probes",				CASE_NETWORK_FABRIC_KEEPALIVE_PROBES },
		{ "keepalive-time",					CASE_NETWORK_FABRIC_KEEPALIVE_TIME },
		{ "latency-max-ms",					CASE_NETWORK_FABRIC_LATENCY_MAX_MS },
		{ "port",							CASE_NETWORK_FABRIC_PORT },
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
		{ "active-rack",					CASE_NAMESPACE_ACTIVE_RACK },
		{ "allow-ttl-without-nsup",			CASE_NAMESPACE_ALLOW_TTL_WITHOUT_NSUP },
		{ "auto-revive",					CASE_NAMESPACE_AUTO_REVIVE },
		{ "background-query-max-rps",		CASE_NAMESPACE_BACKGROUND_QUERY_MAX_RPS },
		{ "conflict-resolution-policy",		CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY },
		{ "conflict-resolve-writes",		CASE_NAMESPACE_CONFLICT_RESOLVE_WRITES },
		{ "default-read-touch-ttl-pct",		CASE_NAMESPACE_DEFAULT_READ_TOUCH_TTL_PCT },
		{ "default-ttl",					CASE_NAMESPACE_DEFAULT_TTL },
		{ "disable-cold-start-eviction",	CASE_NAMESPACE_DISABLE_COLD_START_EVICTION },
		{ "disable-mrt-writes",				CASE_NAMESPACE_DISABLE_MRT_WRITES },
		{ "disable-write-dup-res",			CASE_NAMESPACE_DISABLE_WRITE_DUP_RES },
		{ "disallow-expunge",				CASE_NAMESPACE_DISALLOW_EXPUNGE },
		{ "disallow-null-setname",			CASE_NAMESPACE_DISALLOW_NULL_SETNAME },
		{ "enable-benchmarks-batch-sub",	CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB },
		{ "enable-benchmarks-ops-sub",		CASE_NAMESPACE_ENABLE_BENCHMARKS_OPS_SUB },
		{ "enable-benchmarks-read",			CASE_NAMESPACE_ENABLE_BENCHMARKS_READ },
		{ "enable-benchmarks-udf",			CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF },
		{ "enable-benchmarks-udf-sub",		CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB },
		{ "enable-benchmarks-write",		CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE },
		{ "enable-hist-proxy",				CASE_NAMESPACE_ENABLE_HIST_PROXY },
		{ "evict-hist-buckets",				CASE_NAMESPACE_EVICT_HIST_BUCKETS },
		{ "evict-indexes-memory-pct",		CASE_NAMESPACE_EVICT_INDEXES_MEMORY_PCT },
		{ "evict-tenths-pct",				CASE_NAMESPACE_EVICT_TENTHS_PCT },
		{ "ignore-migrate-fill-delay",		CASE_NAMESPACE_IGNORE_MIGRATE_FILL_DELAY },
		{ "index-stage-size",				CASE_NAMESPACE_INDEX_STAGE_SIZE },
		{ "indexes-memory-budget",			CASE_NAMESPACE_INDEXES_MEMORY_BUDGET },
		{ "inline-short-queries",			CASE_NAMESPACE_INLINE_SHORT_QUERIES },
		{ "max-record-size",				CASE_NAMESPACE_MAX_RECORD_SIZE },
		{ "migrate-order",					CASE_NAMESPACE_MIGRATE_ORDER },
		{ "migrate-retransmit-ms",			CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS },
		{ "migrate-skip-unreadable",		CASE_NAMESPACE_MIGRATE_SKIP_UNREADABLE },
		{ "migrate-sleep",					CASE_NAMESPACE_MIGRATE_SLEEP },
		{ "mrt-duration",					CASE_NAMESPACE_MRT_DURATION },
		{ "nsup-hist-period",				CASE_NAMESPACE_NSUP_HIST_PERIOD },
		{ "nsup-period",					CASE_NAMESPACE_NSUP_PERIOD },
		{ "nsup-threads",					CASE_NAMESPACE_NSUP_THREADS },
		{ "partition-tree-sprigs",			CASE_NAMESPACE_PARTITION_TREE_SPRIGS },
		{ "prefer-uniform-balance",			CASE_NAMESPACE_PREFER_UNIFORM_BALANCE },
		{ "rack-id",						CASE_NAMESPACE_RACK_ID },
		{ "read-consistency-level-override", CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE },
		{ "reject-non-xdr-writes",			CASE_NAMESPACE_REJECT_NON_XDR_WRITES },
		{ "reject-xdr-writes",				CASE_NAMESPACE_REJECT_XDR_WRITES },
		{ "replication-factor",				CASE_NAMESPACE_REPLICATION_FACTOR },
		{ "sindex-stage-size",				CASE_NAMESPACE_SINDEX_STAGE_SIZE },
		{ "single-query-threads",			CASE_NAMESPACE_SINGLE_QUERY_THREADS },
		{ "stop-writes-sys-memory-pct",		CASE_NAMESPACE_STOP_WRITES_SYS_MEMORY_PCT },
		{ "strong-consistency",				CASE_NAMESPACE_STRONG_CONSISTENCY },
		{ "strong-consistency-allow-expunge", CASE_NAMESPACE_STRONG_CONSISTENCY_ALLOW_EXPUNGE },
		{ "tomb-raider-eligible-age",		CASE_NAMESPACE_TOMB_RAIDER_ELIGIBLE_AGE },
		{ "tomb-raider-period",				CASE_NAMESPACE_TOMB_RAIDER_PERIOD },
		{ "transaction-pending-limit",		CASE_NAMESPACE_TRANSACTION_PENDING_LIMIT },
		{ "truncate-threads",				CASE_NAMESPACE_TRUNCATE_THREADS },
		{ "write-commit-level-override",	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE },
		{ "xdr-bin-tombstone-ttl",			CASE_NAMESPACE_XDR_BIN_TOMBSTONE_TTL },
		{ "xdr-tomb-raider-period",			CASE_NAMESPACE_XDR_TOMB_RAIDER_PERIOD },
		{ "xdr-tomb-raider-threads",		CASE_NAMESPACE_XDR_TOMB_RAIDER_THREADS },
		// Sub-contexts:
		{ "geo2dsphere-within",				CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN },
		{ "index-type",						CASE_NAMESPACE_INDEX_TYPE_BEGIN },
		{ "set",							CASE_NAMESPACE_SET_BEGIN },
		{ "sindex-type",					CASE_NAMESPACE_SINDEX_TYPE_BEGIN },
		{ "storage-engine",					CASE_NAMESPACE_STORAGE_ENGINE_BEGIN },
		// Obsoleted:
		{ "background-scan-max-rps",		CASE_NAMESPACE_BACKGROUND_SCAN_MAX_RPS },
		{ "data-in-index",					CASE_NAMESPACE_DATA_IN_INDEX },
		{ "disable-nsup",					CASE_NAMESPACE_DISABLE_NSUP },
		{ "evict-sys-memory-pct",			CASE_NAMESPACE_EVICT_SYS_MEMORY_PCT },
		{ "high-water-disk-pct",			CASE_NAMESPACE_HIGH_WATER_DISK_PCT },
		{ "high-water-memory-pct",			CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT },
		{ "memory-size",					CASE_NAMESPACE_MEMORY_SIZE },
		{ "single-bin",						CASE_NAMESPACE_SINGLE_BIN },
		{ "single-scan-threads",			CASE_NAMESPACE_SINGLE_SCAN_THREADS },
		{ "stop-writes-pct",				CASE_NAMESPACE_STOP_WRITES_PCT },
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

const cfg_opt NAMESPACE_SINDEX_TYPE_OPTS[] = {
		{ "shmem",							CASE_NAMESPACE_SINDEX_TYPE_SHMEM },
		{ "pmem",							CASE_NAMESPACE_SINDEX_TYPE_PMEM },
		{ "flash",							CASE_NAMESPACE_SINDEX_TYPE_FLASH }
};

const cfg_opt NAMESPACE_STORAGE_OPTS[] = {
		{ "memory",							CASE_NAMESPACE_STORAGE_MEMORY },
		{ "pmem",							CASE_NAMESPACE_STORAGE_PMEM },
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE }
};

const cfg_opt NAMESPACE_INDEX_TYPE_PMEM_OPTS[] = {
		{ "evict-mounts-pct",				CASE_NAMESPACE_INDEX_TYPE_PMEM_EVICT_MOUNTS_PCT },
		{ "mount",							CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT },
		{ "mounts-budget",					CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_BUDGET },
		// Obsoleted:
		{ "mounts-high-water-pct",			CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_INDEX_TYPE_FLASH_OPTS[] = {
		{ "evict-mounts-pct",				CASE_NAMESPACE_INDEX_TYPE_FLASH_EVICT_MOUNTS_PCT },
		{ "mount",							CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT },
		{ "mounts-budget",					CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_BUDGET },
		// Obsoleted:
		{ "mounts-high-water-pct",			CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SINDEX_TYPE_PMEM_OPTS[] = {
		{ "evict-mounts-pct",				CASE_NAMESPACE_SINDEX_TYPE_PMEM_EVICT_MOUNTS_PCT },
		{ "mount",							CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNT },
		{ "mounts-budget",					CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_BUDGET },
		// Obsoleted:
		{ "mounts-high-water-pct",			CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SINDEX_TYPE_FLASH_OPTS[] = {
		{ "evict-mounts-pct",				CASE_NAMESPACE_SINDEX_TYPE_FLASH_EVICT_MOUNTS_PCT },
		{ "mount",							CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNT },
		{ "mounts-budget",					CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_BUDGET },
		// Obsoleted:
		{ "mounts-high-water-pct",			CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT },
		{ "mounts-size-limit",				CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_MEMORY_OPTS[] = {
		{ "commit-to-device",				CASE_NAMESPACE_STORAGE_MEMORY_COMMIT_TO_DEVICE },
		{ "compression",					CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION },
		{ "compression-acceleration",		CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_ACCELERATION },
		{ "compression-level",				CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_LEVEL },
		{ "data-size",						CASE_NAMESPACE_STORAGE_MEMORY_DATA_SIZE },
		{ "defrag-lwm-pct",					CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_LWM_PCT },
		{ "defrag-queue-min",				CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_QUEUE_MIN },
		{ "defrag-sleep",					CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_SLEEP },
		{ "defrag-startup-minimum",			CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_STARTUP_MINIMUM },
		{ "device",							CASE_NAMESPACE_STORAGE_MEMORY_DEVICE },
		{ "direct-files",					CASE_NAMESPACE_STORAGE_MEMORY_DIRECT_FILES },
		{ "disable-odsync",					CASE_NAMESPACE_STORAGE_MEMORY_DISABLE_ODSYNC },
		{ "enable-benchmarks-storage",		CASE_NAMESPACE_STORAGE_MEMORY_ENABLE_BENCHMARKS_STORAGE },
		{ "encryption",						CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION },
		{ "encryption-key-file",			CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_KEY_FILE },
		{ "encryption-old-key-file",		CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_OLD_KEY_FILE },
		{ "evict-used-pct",					CASE_NAMESPACE_STORAGE_MEMORY_EVICT_USED_PCT },
		{ "file",							CASE_NAMESPACE_STORAGE_MEMORY_FILE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_MEMORY_FILESIZE },
		{ "flush-max-ms",					CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_MAX_MS },
		{ "flush-size",						CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_SIZE },
		{ "max-write-cache",				CASE_NAMESPACE_STORAGE_MEMORY_MAX_WRITE_CACHE },
		{ "stop-writes-avail-pct",			CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_AVAIL_PCT },
		{ "stop-writes-used-pct",			CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_USED_PCT },
		{ "tomb-raider-sleep",				CASE_NAMESPACE_STORAGE_MEMORY_TOMB_RAIDER_SLEEP },
		// Obsoleted:
		{ "max-used-pct",					CASE_NAMESPACE_STORAGE_MEMORY_MAX_USED_PCT },
		{ "min-avail-pct",					CASE_NAMESPACE_STORAGE_MEMORY_MIN_AVAIL_PCT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_PMEM_OPTS[] = {
		{ "commit-to-device",				CASE_NAMESPACE_STORAGE_PMEM_COMMIT_TO_DEVICE },
		{ "compression",					CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION },
		{ "compression-acceleration",		CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_ACCELERATION },
		{ "compression-level",				CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_LEVEL },
		{ "defrag-lwm-pct",					CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_LWM_PCT },
		{ "defrag-queue-min",				CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_QUEUE_MIN },
		{ "defrag-sleep",					CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_SLEEP },
		{ "defrag-startup-minimum",			CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_STARTUP_MINIMUM },
		{ "direct-files",					CASE_NAMESPACE_STORAGE_PMEM_DIRECT_FILES },
		{ "disable-odsync",					CASE_NAMESPACE_STORAGE_PMEM_DISABLE_ODSYNC },
		{ "enable-benchmarks-storage",		CASE_NAMESPACE_STORAGE_PMEM_ENABLE_BENCHMARKS_STORAGE },
		{ "encryption",						CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION },
		{ "encryption-key-file",			CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_KEY_FILE },
		{ "encryption-old-key-file",		CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_OLD_KEY_FILE },
		{ "evict-used-pct",					CASE_NAMESPACE_STORAGE_PMEM_EVICT_USED_PCT },
		{ "file",							CASE_NAMESPACE_STORAGE_PMEM_FILE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_PMEM_FILESIZE },
		{ "flush-max-ms",					CASE_NAMESPACE_STORAGE_PMEM_FLUSH_MAX_MS },
		{ "max-write-cache",				CASE_NAMESPACE_STORAGE_PMEM_MAX_WRITE_CACHE },
		{ "stop-writes-avail-pct",			CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_AVAIL_PCT },
		{ "stop-writes-used-pct",			CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_USED_PCT },
		{ "tomb-raider-sleep",				CASE_NAMESPACE_STORAGE_PMEM_TOMB_RAIDER_SLEEP },
		// Obsoleted:
		{ "max-used-pct",					CASE_NAMESPACE_STORAGE_PMEM_MAX_USED_PCT },
		{ "min-avail-pct",					CASE_NAMESPACE_STORAGE_PMEM_MIN_AVAIL_PCT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_DEVICE_OPTS[] = {
		{ "cache-replica-writes",			CASE_NAMESPACE_STORAGE_DEVICE_CACHE_REPLICA_WRITES },
		{ "cold-start-empty",				CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY },
		{ "commit-to-device",				CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE },
		{ "compression",					CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION },
		{ "compression-acceleration",		CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ACCELERATION },
		{ "compression-level",				CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_LEVEL },
		{ "defrag-lwm-pct",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT },
		{ "defrag-queue-min",				CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN },
		{ "defrag-sleep",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP },
		{ "defrag-startup-minimum",			CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM },
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE_DEVICE },
		{ "direct-files",					CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES },
		{ "disable-odsync",					CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODSYNC },
		{ "enable-benchmarks-storage",		CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE },
		{ "encryption",						CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION },
		{ "encryption-key-file",			CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_KEY_FILE },
		{ "encryption-old-key-file",		CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OLD_KEY_FILE },
		{ "evict-used-pct",					CASE_NAMESPACE_STORAGE_DEVICE_EVICT_USED_PCT },
		{ "file",							CASE_NAMESPACE_STORAGE_DEVICE_FILE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE },
		{ "flush-max-ms",					CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS },
		{ "flush-size",						CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_SIZE },
		{ "max-write-cache",				CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE },
		{ "post-write-cache",				CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_CACHE },
		{ "read-page-cache",				CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE },
		{ "serialize-tomb-raider",			CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER },
		{ "sindex-startup-device-scan",		CASE_NAMESPACE_STORAGE_DEVICE_SINDEX_STARTUP_DEVICE_SCAN },
		{ "stop-writes-avail-pct",			CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_AVAIL_PCT },
		{ "stop-writes-used-pct",			CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_USED_PCT },
		{ "tomb-raider-sleep",				CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP },
		// Obsoleted:
		{ "data-in-memory",					CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY },
		{ "disable-odirect",				CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT },
		{ "fsync-max-sec",					CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC },
		{ "max-used-pct",					CASE_NAMESPACE_STORAGE_DEVICE_MAX_USED_PCT },
		{ "min-avail-pct",					CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT },
		{ "post-write-queue",				CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE },
		{ "write-block-size",				CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_COMPRESSION_OPTS[] = {
		{ "none",							CASE_NAMESPACE_STORAGE_COMPRESSION_NONE },
		{ "lz4",							CASE_NAMESPACE_STORAGE_COMPRESSION_LZ4 },
		{ "snappy",							CASE_NAMESPACE_STORAGE_COMPRESSION_SNAPPY },
		{ "zstd",							CASE_NAMESPACE_STORAGE_COMPRESSION_ZSTD }
};

const cfg_opt NAMESPACE_STORAGE_ENCRYPTION_OPTS[] = {
		{ "aes-128",						CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_128 },
		{ "aes-256",						CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_256 }
};

const cfg_opt NAMESPACE_SET_OPTS[] = {
		{ "default-read-touch-ttl-pct",		CASE_NAMESPACE_SET_DEFAULT_READ_TOUCH_TTL_PCT },
		{ "default-ttl",					CASE_NAMESPACE_SET_DEFAULT_TTL },
		{ "disable-eviction",				CASE_NAMESPACE_SET_DISABLE_EVICTION },
		{ "enable-index",					CASE_NAMESPACE_SET_ENABLE_INDEX },
		{ "stop-writes-count",				CASE_NAMESPACE_SET_STOP_WRITES_COUNT },
		{ "stop-writes-size",				CASE_NAMESPACE_SET_STOP_WRITES_SIZE },
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
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_OPTS[] = {
		{ "default-password-file",			CASE_SECURITY_DEFAULT_PASSWORD_FILE },
		{ "enable-quotas",					CASE_SECURITY_ENABLE_QUOTAS },
		{ "privilege-refresh-period",		CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD },
		{ "session-ttl",					CASE_SECURITY_SESSION_TTL },
		{ "tps-weight",						CASE_SECURITY_TPS_WEIGHT },
		// Sub-contexts:
		{ "ldap",							CASE_SECURITY_LDAP_BEGIN },
		{ "log",							CASE_SECURITY_LOG_BEGIN },
		// Obsoleted:
		{ "enable-ldap",					CASE_SECURITY_ENABLE_LDAP },
		{ "enable-security",				CASE_SECURITY_ENABLE_SECURITY },
		{ "syslog",							CASE_SECURITY_SYSLOG_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_LDAP_OPTS[] = {
		{ "disable-tls",					CASE_SECURITY_LDAP_DISABLE_TLS },
		{ "login-threads",					CASE_SECURITY_LDAP_LOGIN_THREADS },
		{ "polling-period",					CASE_SECURITY_LDAP_POLLING_PERIOD },
		{ "query-base-dn",					CASE_SECURITY_LDAP_QUERY_BASE_DN },
		{ "query-user-dn",					CASE_SECURITY_LDAP_QUERY_USER_DN },
		{ "query-user-password-file",		CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE },
		{ "role-query-base-dn",				CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN },
		{ "role-query-pattern",				CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN },
		{ "role-query-search-ou",			CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU },
		{ "server",							CASE_SECURITY_LDAP_SERVER },
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
		{ "report-data-op-role",			CASE_SECURITY_LOG_REPORT_DATA_OP_ROLE },
		{ "report-data-op-user",			CASE_SECURITY_LOG_REPORT_DATA_OP_USER },
		{ "report-sys-admin",				CASE_SECURITY_LOG_REPORT_SYS_ADMIN },
		{ "report-user-admin",				CASE_SECURITY_LOG_REPORT_USER_ADMIN },
		{ "report-violation",				CASE_SECURITY_LOG_REPORT_VIOLATION },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_OPTS[] = {
		{ "src-id",							CASE_XDR_SRC_ID },
		// Sub-contexts:
		{ "dc",								CASE_XDR_DC_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_DC_OPTS[] = {
		{ "auth-mode",						CASE_XDR_DC_AUTH_MODE },
		{ "auth-password-file",				CASE_XDR_DC_AUTH_PASSWORD_FILE },
		{ "auth-user",						CASE_XDR_DC_AUTH_USER },
		{ "connector",						CASE_XDR_DC_CONNECTOR },
		{ "max-recoveries-interleaved",		CASE_XDR_DC_MAX_RECOVERIES_INTERLEAVED },
		{ "node-address-port",				CASE_XDR_DC_NODE_ADDRESS_PORT },
		{ "period-ms",						CASE_XDR_DC_PERIOD_MS },
		{ "tls-name",						CASE_XDR_DC_TLS_NAME },
		{ "use-alternate-access-address",	CASE_XDR_DC_USE_ALTERNATE_ACCESS_ADDRESS },
		// Sub-contexts:
		{ "namespace",						CASE_XDR_DC_NAMESPACE_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_DC_AUTH_MODE_OPTS[] = {
		{ "none",							CASE_XDR_DC_AUTH_MODE_NONE },
		{ "internal",						CASE_XDR_DC_AUTH_MODE_INTERNAL },
		{ "external",						CASE_XDR_DC_AUTH_MODE_EXTERNAL },
		{ "external-insecure",				CASE_XDR_DC_AUTH_MODE_EXTERNAL_INSECURE },
		{ "pki",							CASE_XDR_DC_AUTH_MODE_PKI }
};

const cfg_opt XDR_DC_NAMESPACE_OPTS[] = {
		{ "bin-policy",						CASE_XDR_DC_NAMESPACE_BIN_POLICY },
		{ "compression-level",				CASE_XDR_DC_NAMESPACE_COMPRESSION_LEVEL },
		{ "compression-threshold",			CASE_XDR_DC_NAMESPACE_COMPRESSION_THRESHOLD },
		{ "delay-ms",						CASE_XDR_DC_NAMESPACE_DELAY_MS },
		{ "enable-compression",				CASE_XDR_DC_NAMESPACE_ENABLE_COMPRESSION },
		{ "forward",						CASE_XDR_DC_NAMESPACE_FORWARD },
		{ "hot-key-ms",						CASE_XDR_DC_NAMESPACE_HOT_KEY_MS },
		{ "ignore-bin",						CASE_XDR_DC_NAMESPACE_IGNORE_BIN },
		{ "ignore-expunges", 				CASE_XDR_DC_NAMESPACE_IGNORE_EXPUNGES },
		{ "ignore-set",						CASE_XDR_DC_NAMESPACE_IGNORE_SET },
		{ "max-throughput", 				CASE_XDR_DC_NAMESPACE_MAX_THROUGHPUT },
		{ "remote-namespace", 				CASE_XDR_DC_NAMESPACE_REMOTE_NAMESPACE },
		{ "sc-replication-wait-ms",			CASE_XDR_DC_NAMESPACE_SC_REPLICATION_WAIT_MS },
		{ "ship-bin",						CASE_XDR_DC_NAMESPACE_SHIP_BIN },
		{ "ship-bin-luts",					CASE_XDR_DC_NAMESPACE_SHIP_BIN_LUTS },
		{ "ship-nsup-deletes",				CASE_XDR_DC_NAMESPACE_SHIP_NSUP_DELETES },
		{ "ship-only-specified-sets",		CASE_XDR_DC_NAMESPACE_SHIP_ONLY_SPECIFIED_SETS },
		{ "ship-set",						CASE_XDR_DC_NAMESPACE_SHIP_SET },
		{ "ship-versions-interval",			CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_INTERVAL },
		{ "ship-versions-policy",			CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY },
		{ "transaction-queue-limit",		CASE_XDR_DC_NAMESPACE_TRANSACTION_QUEUE_LIMIT },
		{ "write-policy",					CASE_XDR_DC_NAMESPACE_WRITE_POLICY },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt XDR_DC_NAMESPACE_BIN_POLICY_OPTS[] = {
		{ "all",							CASE_XDR_DC_NAMESPACE_BIN_POLICY_ALL },
		{ "no-bins",						CASE_XDR_DC_NAMESPACE_BIN_POLICY_NO_BINS },
		{ "only-changed",					CASE_XDR_DC_NAMESPACE_BIN_POLICY_ONLY_CHANGED },
		{ "changed-and-specified",			CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_AND_SPECIFIED },
		{ "changed-or-specified",			CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_OR_SPECIFIED }
};

const cfg_opt XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_OPTS[] = {
		{ "latest",							CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_LATEST },
		{ "all",							CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_ALL },
		{ "interval",						CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_INTERVAL }
};

const cfg_opt XDR_DC_NAMESPACE_WRITE_POLICY_OPTS[] = {
		{ "auto",							CASE_XDR_DC_NAMESPACE_WRITE_POLICY_AUTO },
		{ "update",							CASE_XDR_DC_NAMESPACE_WRITE_POLICY_UPDATE },
		{ "replace",						CASE_XDR_DC_NAMESPACE_WRITE_POLICY_REPLACE }
};

const int NUM_GLOBAL_OPTS							= sizeof(GLOBAL_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_OPTS							= sizeof(SERVICE_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_AUTO_PIN_OPTS					= sizeof(SERVICE_AUTO_PIN_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_OPTS							= sizeof(LOGGING_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_CONTEXT_OPTS					= sizeof(LOGGING_CONTEXT_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_SYSLOG_OPTS					= sizeof(LOGGING_SYSLOG_OPTS) / sizeof(cfg_opt);
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
const int NUM_NAMESPACE_SINDEX_TYPE_OPTS			= sizeof(NAMESPACE_SINDEX_TYPE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_OPTS				= sizeof(NAMESPACE_STORAGE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_INDEX_TYPE_PMEM_OPTS		= sizeof(NAMESPACE_INDEX_TYPE_PMEM_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_INDEX_TYPE_FLASH_OPTS		= sizeof(NAMESPACE_INDEX_TYPE_FLASH_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SINDEX_TYPE_PMEM_OPTS		= sizeof(NAMESPACE_SINDEX_TYPE_PMEM_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SINDEX_TYPE_FLASH_OPTS		= sizeof(NAMESPACE_SINDEX_TYPE_FLASH_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_MEMORY_OPTS			= sizeof(NAMESPACE_STORAGE_MEMORY_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_PMEM_OPTS			= sizeof(NAMESPACE_STORAGE_PMEM_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_DEVICE_OPTS			= sizeof(NAMESPACE_STORAGE_DEVICE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_COMPRESSION_OPTS	= sizeof(NAMESPACE_STORAGE_COMPRESSION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_ENCRYPTION_OPTS		= sizeof(NAMESPACE_STORAGE_ENCRYPTION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SET_OPTS					= sizeof(NAMESPACE_SET_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_GEO2DSPHERE_WITHIN_OPTS		= sizeof(NAMESPACE_GEO2DSPHERE_WITHIN_OPTS) / sizeof(cfg_opt);
const int NUM_MOD_LUA_OPTS							= sizeof(MOD_LUA_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_OPTS							= sizeof(SECURITY_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LDAP_OPTS					= sizeof(SECURITY_LDAP_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS	= sizeof(SECURITY_LDAP_TOKEN_HASH_METHOD_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LOG_OPTS						= sizeof(SECURITY_LOG_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_OPTS								= sizeof(XDR_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_OPTS							= sizeof(XDR_DC_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_NAMESPACE_OPTS					= sizeof(XDR_DC_NAMESPACE_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_AUTH_MODE_OPTS					= sizeof(XDR_DC_AUTH_MODE_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_NAMESPACE_BIN_POLICY_OPTS		= sizeof(XDR_DC_NAMESPACE_BIN_POLICY_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_OPTS = sizeof(XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_OPTS) / sizeof(cfg_opt);
const int NUM_XDR_DC_NAMESPACE_WRITE_POLICY_OPTS	= sizeof(XDR_DC_NAMESPACE_WRITE_POLICY_OPTS) / sizeof(cfg_opt);


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
	LOGGING, LOGGING_CONTEXT, LOGGING_SYSLOG,
	NETWORK, NETWORK_SERVICE, NETWORK_HEARTBEAT, NETWORK_FABRIC, NETWORK_INFO, NETWORK_TLS,
	NAMESPACE, NAMESPACE_INDEX_TYPE_PMEM, NAMESPACE_INDEX_TYPE_FLASH, NAMESPACE_SINDEX_TYPE_PMEM, NAMESPACE_SINDEX_TYPE_FLASH, NAMESPACE_STORAGE_MEMORY, NAMESPACE_STORAGE_PMEM, NAMESPACE_STORAGE_DEVICE, NAMESPACE_SET, NAMESPACE_GEO2DSPHERE_WITHIN,
	MOD_LUA,
	SECURITY, SECURITY_LDAP, SECURITY_LOG,
	XDR, XDR_DC, XDR_DC_NAMESPACE,
	// Must be last, use for sanity-checking:
	PARSER_STATE_MAX_PLUS_1
} as_config_parser_state;

// For detail logging only - keep in sync with as_config_parser_state.
const char* CFG_PARSER_STATES[] = {
		"GLOBAL",
		"SERVICE",
		"LOGGING", "LOGGING_CONTEXT", "LOGGING_SYSLOG",
		"NETWORK", "NETWORK_SERVICE", "NETWORK_HEARTBEAT", "NETWORK_FABRIC", "NETWORK_INFO", "NETWORK_TLS",
		"NAMESPACE", "NAMESPACE_INDEX_TYPE_PMEM", "NAMESPACE_INDEX_TYPE_SSD", "NAMESPACE_SINDEX_TYPE_PMEM", "NAMESPACE_SINDEX_TYPE_FLASH", "NAMESPACE_STORAGE_MEMORY", "NAMESPACE_STORAGE_PMEM", "NAMESPACE_STORAGE_DEVICE", "NAMESPACE_SET", "NAMESPACE_GEO2DSPHERE_WITHIN",
		"MOD_LUA",
		"SECURITY", "SECURITY_LDAP", "SECURITY_LOG",
		"XDR", "XDR_DC", "XDR_DC_NAMESPACE"
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

// We won't parse lines longer than this.
#define MAX_LINE_SIZE 1024

static void
cfg_unknown_name_tok(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: unknown config parameter name '%s'",
			p_line->num, p_line->name_tok);
}

static void
cfg_unknown_val_tok_1(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: %s has unknown value '%s'",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

static void
cfg_obsolete(const cfg_line* p_line, const char* message)
{
	cf_crash_nostack(AS_CFG, "line %d :: '%s' is obsolete%s%s",
			p_line->num, p_line->name_tok, message ? " - " : "",
					message ? message : "");
}

static char*
cfg_strdup_anyval(const cfg_line* p_line, const char* val_tok, size_t max_size,
		bool is_required)
{
	if (val_tok[0] == 0) {
		if (is_required) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must have a value specified",
					p_line->num, p_line->name_tok);
		}

		// Do not duplicate empty strings.
		return NULL;
	}

	if (max_size < MAX_LINE_SIZE && strlen(val_tok) >= max_size) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be < %lu characters long, not %s",
				p_line->num, p_line->name_tok, max_size, val_tok);
	}

	return cf_strdup(val_tok);
}

static char*
cfg_strdup(const cfg_line* p_line, size_t max_size)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_1, max_size, true);
}

static char*
cfg_strdup_no_checks(const cfg_line* p_line)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_1, MAX_LINE_SIZE, true);
}

static char*
cfg_strdup_val2_no_checks(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_2, MAX_LINE_SIZE,
			is_required);
}

static char*
cfg_strdup_val3_no_checks(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_3, MAX_LINE_SIZE,
			is_required);
}

static void
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

static bool
cfg_bool(const cfg_line* p_line)
{
	if (strcasecmp(p_line->val_tok_1, "true") == 0) {
		return true;
	}

	if (strcasecmp(p_line->val_tok_1, "false") == 0) {
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

static bool
cfg_bool_no_value_is_true(const cfg_line* p_line)
{
	return (*p_line->val_tok_1 == '\0') ? true : cfg_bool(p_line);
}

static uint64_t
cfg_x64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify a hex value",
				p_line->num, p_line->name_tok);
	}

	uint64_t value;

	if (0 != cf_strtoul_x64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a 64-bit hex number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

static uint64_t
cfg_x64_no_checks(const cfg_line* p_line)
{
	return cfg_x64_anyval_no_checks(p_line, p_line->val_tok_1);
}

static uint64_t
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

static uint64_t
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

static uint64_t
cfg_u64_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_1);
}

static uint64_t
cfg_u64_val2_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_2);
}

static uint64_t
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
static uint64_t
cfg_u64_power_of_2(const cfg_line* p_line, uint64_t min, uint64_t max)
{
	uint64_t value = cfg_u64(p_line, min, max);

	if ((value & (value - 1)) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an exact power of 2, not %lu",
				p_line->num, p_line->name_tok, value);
	}

	return value;
}

static uint32_t
cfg_u32_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT32_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned int",
				p_line->num, p_line->name_tok, value);
	}

	return (uint32_t)value;
}

static uint32_t
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
static uint32_t
cfg_u32_power_of_2(const cfg_line* p_line, uint32_t min, uint32_t max)
{
	uint32_t value = cfg_u32(p_line, min, max);

	if ((value & (value - 1)) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an exact power of 2, not %u",
				p_line->num, p_line->name_tok, value);
	}

	return value;
}

static uint32_t
cfg_u32_multiple_of(const cfg_line* p_line, uint32_t factor)
{
	cf_assert(factor != 0, AS_CFG, "can't ask for multiple of 0");

	uint32_t value = cfg_u32_no_checks(p_line);

	if (value % factor != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an exact multiple of %u, not %u",
				p_line->num, p_line->name_tok, factor, value);
	}

	return value;
}

static uint16_t
cfg_u16_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT16_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned short",
				p_line->num, p_line->name_tok, value);
	}

	return (uint16_t)value;
}

static uint16_t
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

static uint16_t
cfg_u16_val2_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_val2_no_checks(p_line);

	if (value > UINT16_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned short",
				p_line->num, p_line->name_tok, value);
	}

	return (uint16_t)value;
}

static uint16_t
cfg_u16_val2(const cfg_line* p_line, uint16_t min, uint16_t max)
{
	uint16_t value = cfg_u16_val2_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

static uint8_t
cfg_u8_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT8_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned char",
				p_line->num, p_line->name_tok, value);
	}

	return (uint8_t)value;
}

static uint8_t
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

static uint32_t
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

static uint32_t
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

static uint32_t
cfg_pct_w_minus_1(const cfg_line* p_line)
{
	int32_t value;

	if (cf_strtol_i32(p_line->val_tok_1, &value) != 0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a number, not %s",
				p_line->num, p_line->name_tok, p_line->val_tok_1);
	}

	if (value > 100 || value < -1) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be between 0 and 100 or -1, not %s",
				p_line->num, p_line->name_tok, p_line->val_tok_1);
	}

	return (uint32_t)value;
}

// Minimum & maximum port numbers:
const uint16_t CFG_MIN_PORT = 1024;
const uint16_t CFG_MAX_PORT = UINT16_MAX;

static cf_ip_port
cfg_port(const cfg_line* p_line)
{
	return (cf_ip_port)cfg_u16(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

static cf_ip_port
cfg_port_val2(const cfg_line* p_line)
{
	return (cf_ip_port)cfg_u16_val2(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
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

	FILE* FD;
	char iobuf[MAX_LINE_SIZE];
	int line_num = 0;
	cfg_parser_state state;

	cfg_parser_state_init(&state);

	as_namespace* ns = NULL;
	as_xdr_dc_cfg* dc_cfg = NULL;
	as_xdr_dc_ns_cfg* dc_ns_cfg = NULL;
	cf_tls_spec* tls_spec = NULL;
	cf_log_sink* sink = NULL;
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
				c->sec_cfg.security_configured = true;
				cfg_begin_context(&state, SECURITY);
				break;
			case CASE_XDR_BEGIN:
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
				case CASE_SERVICE_AUTO_PIN_ADQ:
					c->auto_pin = CF_TOPO_AUTO_PIN_ADQ;
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
			case CASE_SERVICE_DEBUG_ALLOCATIONS:
				c->debug_allocations = cfg_bool(&line);
				break;
			case CASE_SERVICE_DISABLE_UDF_EXECUTION:
				c->udf_execution_disabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_BENCHMARKS_FABRIC:
				c->fabric_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_HEALTH_CHECK:
				c->health_check_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENABLE_HIST_INFO:
				c->info_hist_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_ENFORCE_BEST_PRACTICES:
				c->enforce_best_practices = cfg_bool(&line);
				break;
			case CASE_SERVICE_FEATURE_KEY_FILE:
				cfg_enterprise_only(&line);
				cfg_add_feature_key_file(cfg_strdup_no_checks(&line));
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
			case CASE_SERVICE_INDENT_ALLOCATIONS:
				c->indent_allocations = cfg_bool(&line);
				break;
			case CASE_SERVICE_INFO_MAX_MS:
				c->info_max_ns = cfg_u64(&line, MIN_INFO_MAX_MS, MAX_INFO_MAX_MS) * 1000000;
				break;
			case CASE_SERVICE_INFO_THREADS:
				c->n_info_threads = cfg_u32(&line, 1, MAX_INFO_THREADS);
				break;
			case CASE_SERVICE_KEEP_CAPS_SSD_HEALTH:
				cfg_keep_cap(cfg_bool(&line), &c->keep_caps_ssd_health, CAP_SYS_ADMIN);
				break;
			case CASE_SERVICE_LOG_LOCAL_TIME:
				cf_log_use_local_time(cfg_bool(&line));
				break;
			case CASE_SERVICE_LOG_MILLIS:
				cf_log_use_millis(cfg_bool(&line));
				break;
			case CASE_SERVICE_MICROSECOND_HISTOGRAMS:
				c->microsecond_histograms = cfg_bool(&line);
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
			case CASE_SERVICE_OS_GROUP_PERMS:
				cf_os_use_group_perms(cfg_bool(&line));
				break;
			case CASE_SERVICE_PIDFILE:
				c->pidfile = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_POISON_ALLOCATIONS:
				c->poison_allocations = cfg_bool(&line);
				break;
			case CASE_SERVICE_PROTO_FD_IDLE_MS:
				c->proto_fd_idle_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_PROTO_FD_MAX:
				c->n_proto_fd_max = cfg_u32(&line, MIN_PROTO_FD_MAX, MAX_PROTO_FD_MAX);
				break;
			case CASE_SERVICE_QUARANTINE_ALLOCATIONS:
				c->quarantine_allocations = cfg_u32(&line, 0, 100000000);
				break;
			case CASE_SERVICE_QUERY_MAX_DONE:
				c->query_max_done = cfg_u32(&line, 0, 10000);
				break;
			case CASE_SERVICE_QUERY_THREADS_LIMIT:
				c->n_query_threads_limit = cfg_u32(&line, 1, 1024);
				break;
			case CASE_SERVICE_RUN_AS_DAEMON:
				c->run_as_daemon = cfg_bool_no_value_is_true(&line);
				break;
			case CASE_SERVICE_SECRETS_ADDRESS_PORT:
				cfg_enterprise_only(&line);
				cfg_add_secrets_addr_port(cfg_strdup_no_checks(&line), cfg_strdup_val2_no_checks(&line, true), cfg_strdup_val3_no_checks(&line, false));
				break;
			case CASE_SERVICE_SECRETS_TLS_CONTEXT:
				cfg_enterprise_only(&line);
				g_secrets_cfg.tls_context = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_SECRETS_UDS_PATH:
				cfg_enterprise_only(&line);
				g_secrets_cfg.uds_path = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_SERVICE_THREADS:
				c->n_service_threads = cfg_u32(&line, 1, MAX_SERVICE_THREADS);
				break;
			case CASE_SERVICE_SINDEX_BUILDER_THREADS:
				c->sindex_builder_threads = cfg_u32(&line, 1, 32);
				break;
			case CASE_SERVICE_SINDEX_GC_PERIOD:
				c->sindex_gc_period = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_STAY_QUIESCED:
				cfg_enterprise_only(&line);
				c->stay_quiesced = cfg_bool(&line);
				break;
			case CASE_SERVICE_TICKER_INTERVAL:
				c->ticker_interval = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TLS_REFRESH_PERIOD:
				cfg_enterprise_only(&line);
				tls_set_refresh_period(cfg_seconds_no_checks(&line));
				break;
			case CASE_SERVICE_TRANSACTION_MAX_MS:
				c->transaction_max_ns = cfg_u64_no_checks(&line) * 1000000;
				break;
			case CASE_SERVICE_TRANSACTION_RETRY_MS:
				c->transaction_retry_ms = cfg_u32_no_checks(&line);
				break;
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
			case CASE_SERVICE_VAULT_CA:
				cfg_enterprise_only(&line);
				g_vault_cfg.ca = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_VAULT_NAMESPACE:
				cfg_enterprise_only(&line);
				g_vault_cfg.namespace = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_VAULT_PATH:
				cfg_enterprise_only(&line);
				g_vault_cfg.path = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_VAULT_TOKEN_FILE:
				cfg_enterprise_only(&line);
				g_vault_cfg.token_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_VAULT_URL:
				cfg_enterprise_only(&line);
				g_vault_cfg.url = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_WORK_DIRECTORY:
				c->work_directory = cfg_strdup_no_checks(&line);
				break;
			// Obsoleted:
			case CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS:
				cfg_obsolete(&line, "please configure 'service-threads' carefully");
				break;
			case CASE_SERVICE_NSUP_PERIOD:
				cfg_obsolete(&line, "please use namespace context 'nsup-period'");
				break;
			case CASE_SERVICE_OBJECT_SIZE_HIST_PERIOD:
				cfg_obsolete(&line, "please use namespace context 'nsup-hist-period'");
				break;
			case CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION:
				cfg_obsolete(&line, "please use namespace context 'write-commit-level-override' and/or write transaction policy");
				break;
			case CASE_SERVICE_SCAN_MAX_DONE:
				cfg_obsolete(&line, "please use 'query-max-done'");
				break;
			case CASE_SERVICE_SCAN_THREADS_LIMIT:
				cfg_obsolete(&line, "please use 'query-threads-limit'");
				break;
			case CASE_SERVICE_TRANSACTION_PENDING_LIMIT:
				cfg_obsolete(&line, "please use namespace context 'transaction-pending-limit'");
				break;
			case CASE_SERVICE_TRANSACTION_QUEUES:
				cfg_obsolete(&line, "please configure 'service-threads' carefully");
				break;
			case CASE_SERVICE_TRANSACTION_REPEATABLE_READ:
				cfg_obsolete(&line, "please use namespace context 'read-consistency-level-override' and/or read transaction policy");
				break;
			case CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE:
				cfg_obsolete(&line, "please configure 'service-threads' carefully");
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
			// Sub-contexts:
			case CASE_LOG_CONSOLE_BEGIN:
				sink = cf_log_init_sink(NULL, -1, NULL);
				cfg_begin_context(&state, LOGGING_CONTEXT);
				break;
			case CASE_LOG_FILE_BEGIN:
				sink = cf_log_init_sink(line.val_tok_1, -1, NULL);
				cfg_begin_context(&state, LOGGING_CONTEXT);
				break;
			case CASE_LOG_SYSLOG_BEGIN:
				sink = cf_log_init_sink(DEFAULT_SYSLOG_PATH, LOG_LOCAL0, DEFAULT_SYSLOG_TAG);
				cfg_begin_context(&state, LOGGING_SYSLOG);
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
		// Parse logging::file and logging::console context items.
		//
		case LOGGING_CONTEXT:
			switch (cfg_find_tok(line.name_tok, LOGGING_CONTEXT_OPTS, NUM_LOGGING_CONTEXT_OPTS)) {
			case CASE_LOG_CONTEXT_CONTEXT:
				if (! cf_log_init_level(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging context %s %s", line_num, line.val_tok_1, line.val_tok_2);
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
		// Parse logging::syslog context items.
		//
		case LOGGING_SYSLOG:
			switch (cfg_find_tok(line.name_tok, LOGGING_SYSLOG_OPTS, NUM_LOGGING_SYSLOG_OPTS)) {
			case CASE_LOG_SYSLOG_CONTEXT:
				if (! cf_log_init_level(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging context %s %s", line_num, line.val_tok_1, line.val_tok_2);
				}
				break;
			case CASE_LOG_SYSLOG_FACILITY:
				if (! cf_log_init_facility(sink, line.val_tok_1)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging facility %s", line_num, line.val_tok_1);
				}
				break;
			case CASE_LOG_SYSLOG_PATH:
				cf_log_init_path(sink, line.val_tok_1);
				break;
			case CASE_LOG_SYSLOG_TAG:
				cf_log_init_tag(sink, line.val_tok_1);
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
			// Sub-contexts, in canonical configuration file order:
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
			case CASE_NETWORK_SERVICE_ACCESS_ADDRESS:
				cfg_add_addr_std(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_ACCESS_PORT:
				c->service.std_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_ADDRESS:
				cfg_add_addr_alt(line.val_tok_1, &c->service);
				break;
			case CASE_NETWORK_SERVICE_ALTERNATE_ACCESS_PORT:
				c->service.alt_port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_DISABLE_LOCALHOST:
				c->service_localhost_disabled = cfg_bool(&line);
				break;
			case CASE_NETWORK_SERVICE_PORT:
				c->service.bind_port = cfg_port(&line);
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
			// Obsoleted:
			case CASE_NETWORK_SERVICE_ALTERNATE_ADDRESS:
				cfg_obsolete(&line, "see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/network_to_3_10");
				break;
			case CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS:
				cfg_obsolete(&line, "pleas use 'access-address'");
				break;
			case CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME:
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
		// Parse network::heartbeat context items.
		//
		case NETWORK_HEARTBEAT:
			switch (cfg_find_tok(line.name_tok, NETWORK_HEARTBEAT_OPTS, NUM_NETWORK_HEARTBEAT_OPTS)) {
			case CASE_NETWORK_HEARTBEAT_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->hb_serv_spec);
				break;
			case CASE_NETWORK_HEARTBEAT_CONNECT_TIMEOUT_MS:
				c->hb_config.connect_timeout_ms = cfg_u32(&line, AS_HB_TX_INTERVAL_MS_MIN, UINT32_MAX);
				break;
			case CASE_NETWORK_HEARTBEAT_INTERVAL:
				c->hb_config.tx_interval = cfg_u32(&line, AS_HB_TX_INTERVAL_MS_MIN, AS_HB_TX_INTERVAL_MS_MAX);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT:
				cfg_add_mesh_seed_addr_port(cfg_strdup_no_checks(&line), cfg_port_val2(&line), false);
				break;
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
			case CASE_NETWORK_HEARTBEAT_MTU:
				c->hb_config.override_mtu = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MULTICAST_GROUP:
				add_addr(line.val_tok_1, &c->hb_multicast_groups);
				break;
			case CASE_NETWORK_HEARTBEAT_MULTICAST_TTL:
				c->hb_config.multicast_ttl = cfg_u8_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_PORT:
				c->hb_serv_spec.bind_port = cfg_port(&line);
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
			case CASE_NETWORK_HEARTBEAT_TIMEOUT:
				c->hb_config.max_intervals_missed = cfg_u32(&line, AS_HB_MAX_INTERVALS_MISSED_MIN, UINT32_MAX);
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
			// Obsoleted:
			case CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS:
				cfg_obsolete(&line, "see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/network_to_3_10");
				break;
			case CASE_NETWORK_HEARTBEAT_MCAST_TTL:
				cfg_obsolete(&line, "please use 'multicast-ttl'");
				break;
			case CASE_CONTEXT_END:
				if (c->hb_config.connect_timeout_ms > c->hb_config.tx_interval * c->hb_config.max_intervals_missed / 3) {
					cf_crash_nostack(AS_CFG, "'connect-timeout-ms' must be <= 'interval' * 'timeout' / 3");
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
		// Parse network::fabric context items.
		//
		case NETWORK_FABRIC:
			switch (cfg_find_tok(line.name_tok, NETWORK_FABRIC_OPTS, NUM_NETWORK_FABRIC_OPTS)) {
			case CASE_NETWORK_FABRIC_ADDRESS:
				cfg_add_addr_bind(line.val_tok_1, &c->fabric);
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
			case CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_POOLS:
				c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW] = cfg_u32(&line, 1, MAX_CHANNEL_POOLS);
				break;
			case CASE_NETWORK_FABRIC_CHANNEL_RW_RECV_THREADS:
				c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW] = cfg_u32(&line, 1, MAX_FABRIC_CHANNEL_THREADS);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED:
				c->fabric_keepalive_enabled = cfg_bool(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_INTVL:
				c->fabric_keepalive_intvl = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_PROBES:
				c->fabric_keepalive_probes = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_TIME:
				c->fabric_keepalive_time = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_LATENCY_MAX_MS:
				c->fabric_latency_max_ms = cfg_u32(&line, 0, 1000);
				break;
			case CASE_NETWORK_FABRIC_PORT:
				c->fabric.bind_port = cfg_port(&line);
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
				if (c->n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW] % c->n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW] != 0) {
					cf_crash_nostack(AS_CFG, "'channel-rw-recv-threads' must be a multiple of 'channel-rw-recv-pools'");
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
			case CASE_NAMESPACE_ACTIVE_RACK:
				cfg_enterprise_only(&line);
				ns->cfg_active_rack = cfg_u32(&line, 0, MAX_RACK_ID);
				break;
			case CASE_NAMESPACE_ALLOW_TTL_WITHOUT_NSUP:
				ns->allow_ttl_without_nsup = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_AUTO_REVIVE:
				cfg_enterprise_only(&line);
				ns->auto_revive = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_BACKGROUND_QUERY_MAX_RPS:
				ns->background_query_max_rps = cfg_u32(&line, 1, 1000000);
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
			case CASE_NAMESPACE_CONFLICT_RESOLVE_WRITES:
				cfg_enterprise_only(&line);
				ns->conflict_resolve_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DEFAULT_READ_TOUCH_TTL_PCT:
				ns->default_read_touch_ttl_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_DEFAULT_TTL:
				ns->default_ttl = cfg_seconds(&line, 0, MAX_ALLOWED_TTL);
				break;
			case CASE_NAMESPACE_DISABLE_COLD_START_EVICTION:
				ns->cold_start_eviction_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISABLE_MRT_WRITES:
				cfg_enterprise_only(&line);
				ns->mrt_writes_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISABLE_WRITE_DUP_RES:
				ns->write_dup_res_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISALLOW_EXPUNGE:
				cfg_enterprise_only(&line);
				ns->ap_disallow_drops = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISALLOW_NULL_SETNAME:
				ns->disallow_null_setname = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_BATCH_SUB:
				ns->batch_sub_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_OPS_SUB:
				ns->ops_sub_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_READ:
				ns->read_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF:
				ns->udf_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_UDF_SUB:
				ns->udf_sub_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_BENCHMARKS_WRITE:
				ns->write_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_ENABLE_HIST_PROXY:
				ns->proxy_hist_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_EVICT_HIST_BUCKETS:
				ns->evict_hist_buckets = cfg_u32(&line, 100, 10000000);
				break;
			case CASE_NAMESPACE_EVICT_INDEXES_MEMORY_PCT:
				ns->evict_indexes_memory_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_EVICT_TENTHS_PCT:
				ns->evict_tenths_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_IGNORE_MIGRATE_FILL_DELAY:
				cfg_enterprise_only(&line);
				ns->ignore_migrate_fill_delay = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_INDEX_STAGE_SIZE:
				ns->index_stage_size = cfg_u64_power_of_2(&line, CF_ARENAX_MIN_STAGE_SIZE, CF_ARENAX_MAX_STAGE_SIZE);
				break;
			case CASE_NAMESPACE_INDEXES_MEMORY_BUDGET:
				ns->indexes_memory_budget = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_INLINE_SHORT_QUERIES:
				ns->inline_short_queries = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_MAX_RECORD_SIZE:
				ns->max_record_size = cfg_u32(&line, MIN_MAX_RECORD_SIZE, WBLOCK_SZ);
				break;
			case CASE_NAMESPACE_MIGRATE_ORDER:
				ns->migrate_order = cfg_u32(&line, 1, 10);
				break;
			case CASE_NAMESPACE_MIGRATE_RETRANSMIT_MS:
				ns->migrate_retransmit_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_MIGRATE_SKIP_UNREADABLE:
				ns->migrate_skip_unreadable = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_MIGRATE_SLEEP:
				ns->migrate_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_MRT_DURATION:
				cfg_enterprise_only(&line);
				ns->mrt_duration = cfg_seconds(&line, MIN_MRT_DURATION, MAX_MRT_DURATION);
				break;
			case CASE_NAMESPACE_NSUP_HIST_PERIOD:
				ns->nsup_hist_period = cfg_seconds_no_checks(&line);
				break;
			case CASE_NAMESPACE_NSUP_PERIOD:
				ns->nsup_period = cfg_seconds_no_checks(&line);
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
			case CASE_NAMESPACE_REJECT_NON_XDR_WRITES:
				ns->reject_non_xdr_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_REJECT_XDR_WRITES:
				ns->reject_xdr_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_REPLICATION_FACTOR:
				ns->cfg_replication_factor = cfg_u32(&line, 1, AS_CLUSTER_SZ);
				break;
			case CASE_NAMESPACE_SINDEX_STAGE_SIZE:
				ns->sindex_stage_size = cfg_u64_power_of_2(&line, SI_ARENA_MIN_STAGE_SIZE, SI_ARENA_MAX_STAGE_SIZE);
				break;
			case CASE_NAMESPACE_SINGLE_QUERY_THREADS:
				ns->n_single_query_threads = cfg_u32(&line, 1, 128);
				break;
			case CASE_NAMESPACE_STOP_WRITES_SYS_MEMORY_PCT:
				ns->stop_writes_sys_memory_pct = cfg_u32(&line, 0, 100);
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
			case CASE_NAMESPACE_TRUNCATE_THREADS:
				ns->n_truncate_threads = cfg_u32(&line, 1, MAX_TRUNCATE_THREADS);
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
			case CASE_NAMESPACE_XDR_BIN_TOMBSTONE_TTL:
				cfg_enterprise_only(&line);
				ns->xdr_bin_tombstone_ttl_ms = cfg_seconds(&line, 0, MAX_ALLOWED_TTL) * 1000UL;
				break;
			case CASE_NAMESPACE_XDR_TOMB_RAIDER_PERIOD:
				cfg_enterprise_only(&line);
				ns->xdr_tomb_raider_period = cfg_seconds_no_checks(&line);
				break;
			case CASE_NAMESPACE_XDR_TOMB_RAIDER_THREADS:
				cfg_enterprise_only(&line);
				ns->n_xdr_tomb_raider_threads = cfg_u32(&line, 1, 128);
				break;
			// Sub-contexts:
			case CASE_NAMESPACE_GEO2DSPHERE_WITHIN_BEGIN:
				cfg_begin_context(&state, NAMESPACE_GEO2DSPHERE_WITHIN);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_BEGIN:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_INDEX_TYPE_OPTS, NUM_NAMESPACE_INDEX_TYPE_OPTS)) {
				case CASE_NAMESPACE_INDEX_TYPE_SHMEM:
					ns->pi_xmem_type = CF_XMEM_TYPE_SHMEM;
					break;
				case CASE_NAMESPACE_INDEX_TYPE_PMEM:
					ns->pi_xmem_type = CF_XMEM_TYPE_PMEM;
					cfg_begin_context(&state, NAMESPACE_INDEX_TYPE_PMEM);
					break;
				case CASE_NAMESPACE_INDEX_TYPE_FLASH:
					ns->pi_xmem_type = CF_XMEM_TYPE_FLASH;
					cfg_begin_context(&state, NAMESPACE_INDEX_TYPE_FLASH);
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
			case CASE_NAMESPACE_SINDEX_TYPE_BEGIN:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_SINDEX_TYPE_OPTS, NUM_NAMESPACE_SINDEX_TYPE_OPTS)) {
				case CASE_NAMESPACE_SINDEX_TYPE_SHMEM:
					ns->si_xmem_type = CF_XMEM_TYPE_SHMEM;
					break;
				case CASE_NAMESPACE_SINDEX_TYPE_PMEM:
					ns->si_xmem_type = CF_XMEM_TYPE_PMEM;
					cfg_begin_context(&state, NAMESPACE_SINDEX_TYPE_PMEM);
					break;
				case CASE_NAMESPACE_SINDEX_TYPE_FLASH:
					ns->si_xmem_type = CF_XMEM_TYPE_FLASH;
					cfg_begin_context(&state, NAMESPACE_SINDEX_TYPE_FLASH);
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_ENGINE_BEGIN:
				// Special check - duplicate storage-engines are nasty.
				if (ns->storage_type != AS_STORAGE_ENGINE_UNDEFINED) {
					cf_crash_nostack(AS_CFG, "{%s} can only configure one 'storage-engine'", ns->name);
				}
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_OPTS, NUM_NAMESPACE_STORAGE_OPTS)) {
				case CASE_NAMESPACE_STORAGE_MEMORY:
					ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
					ns->storage_post_write_cache = 0; // override non-0 default for info purposes
					cfg_begin_context(&state, NAMESPACE_STORAGE_MEMORY);
					break;
				case CASE_NAMESPACE_STORAGE_PMEM:
					cfg_enterprise_only(&line);
					ns->storage_type = AS_STORAGE_ENGINE_PMEM;
					ns->storage_post_write_cache = 0; // override non-0 default for info purposes
					cfg_begin_context(&state, NAMESPACE_STORAGE_PMEM);
					break;
				case CASE_NAMESPACE_STORAGE_DEVICE:
					ns->storage_type = AS_STORAGE_ENGINE_SSD;
					ns->storage_flush_size = DEFAULT_FLUSH_SIZE;
					cfg_begin_context(&state, NAMESPACE_STORAGE_DEVICE);
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			// Obsoleted:
			case CASE_NAMESPACE_BACKGROUND_SCAN_MAX_RPS:
				cfg_obsolete(&line, "please use 'background-query-max-rps'");
				break;
			case CASE_NAMESPACE_DATA_IN_INDEX:
				cfg_obsolete(&line, "see documentation for converting to multi-bin");
				break;
			case CASE_NAMESPACE_DISABLE_NSUP:
				cfg_obsolete(&line, "please set 'nsup-period' to 0 to disable nsup");
				break;
			case CASE_NAMESPACE_EVICT_SYS_MEMORY_PCT:
				cfg_obsolete(&line, "please use 'evict-indexes-memory-pct' and 'evict-used-pct'");
				break;
			case CASE_NAMESPACE_HIGH_WATER_DISK_PCT:
				cfg_obsolete(&line, "please use storage context 'evict-used-pct'");
				break;
			case CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT:
				cfg_obsolete(&line, "please use 'evict-indexes-memory-pct' and storage context 'evict-used-pct'");
				break;
			case CASE_NAMESPACE_MEMORY_SIZE:
				cfg_obsolete(&line, "please use 'stop-writes-sys-memory-pct', 'evict-indexes-memory-pct', and storage context 'evict-used-pct' for stop-writes & eviction");
				break;
			case CASE_NAMESPACE_SINGLE_BIN:
				cfg_obsolete(&line, "see documentation for converting to multi-bin");
				break;
			case CASE_NAMESPACE_SINGLE_SCAN_THREADS:
				cfg_obsolete(&line, "please use 'single-query-threads'");
				break;
			case CASE_NAMESPACE_STOP_WRITES_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-sys-memory-pct'");
				break;
			case CASE_CONTEXT_END:
				if (ns->storage_type == AS_STORAGE_ENGINE_UNDEFINED) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'storage-engine'", ns->name);
				}
				if (ns->pi_xmem_type == CF_XMEM_TYPE_FLASH && ns->storage_type != AS_STORAGE_ENGINE_SSD) {
					cf_crash_nostack(AS_CFG, "{%s} 'index-type flash' can only be used with 'storage-engine device'", ns->name);
				}
				if (ns->default_ttl != 0 && ns->nsup_period == 0 && ! ns->allow_ttl_without_nsup) {
					cf_crash_nostack(AS_CFG, "{%s} must configure non-zero 'nsup-period' or 'allow-ttl-without-nsup' true if 'default-ttl' is non-zero", ns->name);
				}
				if (ns->storage_type == AS_STORAGE_ENGINE_SSD || (ns->storage_commit_to_device && ns->n_storage_shadows != 0)) {
					c->n_namespaces_not_inlined++;
				}
				else {
					c->n_namespaces_inlined++;
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
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_EVICT_MOUNTS_PCT:
				ns->pi_evict_mounts_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNT:
				cfg_add_pi_xmem_mount(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_BUDGET:
				ns->pi_mounts_budget = cfg_u64(&line, 1024UL * 1024UL * 1024UL, UINT64_MAX);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT:
				cfg_obsolete(&line, "please use 'evict-mounts-pct'");
				break;
			case CASE_NAMESPACE_INDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT:
				cfg_obsolete(&line, "please use 'mounts-budget'");
				break;
			case CASE_CONTEXT_END:
				if (ns->pi_mounts_budget == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'mounts-budget'", ns->name);
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
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_EVICT_MOUNTS_PCT:
				ns->pi_evict_mounts_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNT:
				cfg_add_pi_xmem_mount(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_BUDGET:
				ns->pi_mounts_budget = cfg_u64(&line, 1024UL * 1024UL * 1024UL * 4UL, UINT64_MAX);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT:
				cfg_obsolete(&line, "please use 'evict-mounts-pct'");
				break;
			case CASE_NAMESPACE_INDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT:
				cfg_obsolete(&line, "please use 'mounts-budget'");
				break;
			case CASE_CONTEXT_END:
				if (ns->pi_mounts_budget == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'mounts-budget'", ns->name);
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
		// Parse namespace::sindex-type pmem context items.
		//
		case NAMESPACE_SINDEX_TYPE_PMEM:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_SINDEX_TYPE_PMEM_OPTS, NUM_NAMESPACE_SINDEX_TYPE_PMEM_OPTS)) {
			case CASE_NAMESPACE_SINDEX_TYPE_PMEM_EVICT_MOUNTS_PCT:
				ns->si_evict_mounts_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNT:
				cfg_add_si_xmem_mount(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_BUDGET:
				ns->si_mounts_budget = cfg_u64(&line, 1024UL * 1024UL * 1024UL, UINT64_MAX);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_HIGH_WATER_PCT:
				cfg_obsolete(&line, "please use 'evict-mounts-pct'");
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_PMEM_MOUNTS_SIZE_LIMIT:
				cfg_obsolete(&line, "please use 'mounts-budget'");
				break;
			case CASE_CONTEXT_END:
				if (ns->si_mounts_budget == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure sindex 'mounts-budget'", ns->name);
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
		// Parse namespace::sindex-type flash context items.
		//
		case NAMESPACE_SINDEX_TYPE_FLASH:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_SINDEX_TYPE_FLASH_OPTS, NUM_NAMESPACE_SINDEX_TYPE_FLASH_OPTS)) {
			case CASE_NAMESPACE_SINDEX_TYPE_FLASH_EVICT_MOUNTS_PCT:
				ns->si_evict_mounts_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNT:
				cfg_add_si_xmem_mount(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_BUDGET:
				ns->si_mounts_budget = cfg_u64(&line, 1024UL * 1024UL * 1024UL, UINT64_MAX);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_HIGH_WATER_PCT:
				cfg_obsolete(&line, "please use 'evict-mounts-pct'");
				break;
			case CASE_NAMESPACE_SINDEX_TYPE_FLASH_MOUNTS_SIZE_LIMIT:
				cfg_obsolete(&line, "please use 'mounts-budget'");
				break;
			case CASE_CONTEXT_END:
				if (ns->si_mounts_budget == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure sindex 'mounts-budget'", ns->name);
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
		// Parse namespace::storage-engine memory context items.
		//
		case NAMESPACE_STORAGE_MEMORY:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_MEMORY_OPTS, NUM_NAMESPACE_STORAGE_MEMORY_OPTS)) {
			case CASE_NAMESPACE_STORAGE_MEMORY_COMMIT_TO_DEVICE:
				cfg_enterprise_only(&line);
				ns->storage_commit_to_device = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_COMPRESSION_OPTS, NUM_NAMESPACE_STORAGE_COMPRESSION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_COMPRESSION_NONE:
					ns->storage_compression = AS_COMPRESSION_NONE;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_LZ4:
					ns->storage_compression = AS_COMPRESSION_LZ4;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_SNAPPY:
					ns->storage_compression = AS_COMPRESSION_SNAPPY;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_ZSTD:
					ns->storage_compression = AS_COMPRESSION_ZSTD;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_ACCELERATION:
				cfg_enterprise_only(&line);
				ns->storage_compression_acceleration = cfg_u32(&line, 1, 65537);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_COMPRESSION_LEVEL:
				cfg_enterprise_only(&line);
				ns->storage_compression_level = cfg_u32(&line, 1, 9);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DATA_SIZE:
				ns->storage_data_size = cfg_u64(&line, 256 * 1024 * 1024, 256UL * 1024 * 1024 * 1024 * 1024);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_LWM_PCT:
				ns->storage_defrag_lwm_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_QUEUE_MIN:
				ns->storage_defrag_queue_min = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_SLEEP:
				ns->storage_defrag_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DEFRAG_STARTUP_MINIMUM:
				ns->storage_defrag_startup_minimum = cfg_u32(&line, 0, 99);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DEVICE:
				cfg_add_mem_shadow_device(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DIRECT_FILES:
				ns->storage_direct_files = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_DISABLE_ODSYNC:
				ns->storage_disable_odsync = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_ENABLE_BENCHMARKS_STORAGE:
				ns->storage_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_ENCRYPTION_OPTS, NUM_NAMESPACE_STORAGE_ENCRYPTION_OPTS))
				{
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_128:
					ns->storage_encryption = AS_ENCRYPTION_AES_128;
					break;
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_256:
					ns->storage_encryption = AS_ENCRYPTION_AES_256;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_KEY_FILE:
				cfg_enterprise_only(&line);
				ns->storage_encryption_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_ENCRYPTION_OLD_KEY_FILE:
				cfg_enterprise_only(&line);
				ns->storage_encryption_old_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_EVICT_USED_PCT:
				ns->storage_evict_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_FILE:
				cfg_add_mem_shadow_file(ns, cfg_strdup_no_checks(&line));
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_FILESIZE:
				ns->storage_filesize = cfg_u64(&line, AS_STORAGE_MIN_DEVICE_SIZE, AS_STORAGE_MAX_DEVICE_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_MAX_MS:
				ns->storage_flush_max_us = cfg_u64_no_checks(&line) * 1000;
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_FLUSH_SIZE:
				ns->storage_flush_size = cfg_u32_power_of_2(&line, MIN_FLUSH_SIZE, MAX_FLUSH_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_MAX_WRITE_CACHE:
				ns->storage_max_write_cache = cfg_u64(&line, DEFAULT_MAX_WRITE_CACHE, UINT64_MAX);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_AVAIL_PCT:
				ns->storage_stop_writes_avail_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_STOP_WRITES_USED_PCT:
				ns->storage_stop_writes_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_TOMB_RAIDER_SLEEP:
				cfg_enterprise_only(&line);
				ns->storage_tomb_raider_sleep = cfg_u32_no_checks(&line);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_STORAGE_MEMORY_MAX_USED_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-used-pct' instead");
				break;
			case CASE_NAMESPACE_STORAGE_MEMORY_MIN_AVAIL_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-avail-pct' instead");
				break;
			case CASE_CONTEXT_END:
				if (ns->n_storage_shadows == 0 && ns->storage_data_size == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'data-size' if not using storage backing", ns->name);
				}
				if (ns->n_storage_shadows != 0 && ns->storage_data_size != 0) {
					cf_crash_nostack(AS_CFG, "{%s} can't configure 'data-size' if using storage backing", ns->name);
				}
				if (ns->n_storage_shadows == 0 && ns->storage_flush_size != 0) {
					cf_crash_nostack(AS_CFG, "{%s} can't configure 'flush-size' if not using storage backing", ns->name);
				}
				if (ns->n_storage_shadows != 0 && ns->storage_flush_size == 0) {
					ns->storage_flush_size = DEFAULT_FLUSH_SIZE; // set default if using storage backing
				}
				if (ns->n_storage_files != 0 && ns->storage_filesize == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'filesize' if using storage files", ns->name);
				}
				if (ns->n_storage_shadows == 0 && ns->storage_encryption_key_file != NULL) {
					cf_crash_nostack(AS_CFG, "{%s} 'encryption-key-file' is only relevant if using storage backing", ns->name);
				}
				if (ns->storage_commit_to_device && ns->n_storage_shadows == 0) {
					cf_crash_nostack(AS_CFG, "{%s} 'commit-to-device' guarantee is automatic if not using storage backing", ns->name);
				}
				if (ns->storage_commit_to_device && ns->storage_disable_odsync) {
					cf_crash_nostack(AS_CFG, "{%s} can't configure both 'commit-to-device' and 'disable-odsync'", ns->name);
				}
				if (ns->storage_compression_acceleration != 0 && ns->storage_compression != AS_COMPRESSION_LZ4) {
					cf_crash_nostack(AS_CFG, "{%s} 'compression-acceleration' is only relevant for 'compression lz4'", ns->name);
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
		// Parse namespace::storage-engine pmem context items.
		//
		case NAMESPACE_STORAGE_PMEM:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_PMEM_OPTS, NUM_NAMESPACE_STORAGE_PMEM_OPTS)) {
			case CASE_NAMESPACE_STORAGE_PMEM_COMMIT_TO_DEVICE:
				ns->storage_commit_to_device = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_COMPRESSION_OPTS, NUM_NAMESPACE_STORAGE_COMPRESSION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_COMPRESSION_NONE:
					ns->storage_compression = AS_COMPRESSION_NONE;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_LZ4:
					ns->storage_compression = AS_COMPRESSION_LZ4;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_SNAPPY:
					ns->storage_compression = AS_COMPRESSION_SNAPPY;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_ZSTD:
					ns->storage_compression = AS_COMPRESSION_ZSTD;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_ACCELERATION:
				ns->storage_compression_acceleration = cfg_u32(&line, 1, 65537);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_COMPRESSION_LEVEL:
				ns->storage_compression_level = cfg_u32(&line, 1, 9);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_LWM_PCT:
				ns->storage_defrag_lwm_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_QUEUE_MIN:
				ns->storage_defrag_queue_min = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_SLEEP:
				ns->storage_defrag_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DEFRAG_STARTUP_MINIMUM:
				ns->storage_defrag_startup_minimum = cfg_u32(&line, 0, 99);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DIRECT_FILES:
				ns->storage_direct_files = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_DISABLE_ODSYNC:
				ns->storage_disable_odsync = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_ENABLE_BENCHMARKS_STORAGE:
				ns->storage_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION:
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_ENCRYPTION_OPTS, NUM_NAMESPACE_STORAGE_ENCRYPTION_OPTS))
				{
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_128:
					ns->storage_encryption = AS_ENCRYPTION_AES_128;
					break;
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_256:
					ns->storage_encryption = AS_ENCRYPTION_AES_256;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_KEY_FILE:
				ns->storage_encryption_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_ENCRYPTION_OLD_KEY_FILE:
				ns->storage_encryption_old_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_EVICT_USED_PCT:
				ns->storage_evict_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_FILE:
				cfg_add_storage_file(ns, cfg_strdup_no_checks(&line), cfg_strdup_val2_no_checks(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_FILESIZE:
				ns->storage_filesize = cfg_u64(&line, AS_STORAGE_MIN_DEVICE_SIZE, AS_STORAGE_MAX_DEVICE_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_FLUSH_MAX_MS:
				ns->storage_flush_max_us = cfg_u64_no_checks(&line) * 1000;
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_MAX_WRITE_CACHE:
				ns->storage_max_write_cache = cfg_u64(&line, DEFAULT_MAX_WRITE_CACHE, UINT64_MAX);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_AVAIL_PCT:
				ns->storage_stop_writes_avail_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_STOP_WRITES_USED_PCT:
				ns->storage_stop_writes_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_TOMB_RAIDER_SLEEP:
				ns->storage_tomb_raider_sleep = cfg_u32_no_checks(&line);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_STORAGE_PMEM_MAX_USED_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-used-pct' instead");
				break;
			case CASE_NAMESPACE_STORAGE_PMEM_MIN_AVAIL_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-avail-pct' instead");
				break;
			case CASE_CONTEXT_END:
				if (ns->n_storage_files == 0) {
					cf_crash_nostack(AS_CFG, "{%s} has no files", ns->name);
				}
				if (ns->storage_filesize == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'filesize' if using storage files", ns->name);
				}
				if (ns->storage_commit_to_device && ns->storage_disable_odsync) {
					cf_crash_nostack(AS_CFG, "{%s} can't configure both 'commit-to-device' and 'disable-odsync'", ns->name);
				}
				if (ns->storage_compression_acceleration != 0 && ns->storage_compression != AS_COMPRESSION_LZ4) {
					cf_crash_nostack(AS_CFG, "{%s} 'compression-acceleration' is only relevant for 'compression lz4'", ns->name);
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
		// Parse namespace::storage-engine device context items.
		//
		case NAMESPACE_STORAGE_DEVICE:
			switch (cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_DEVICE_OPTS, NUM_NAMESPACE_STORAGE_DEVICE_OPTS)) {
			case CASE_NAMESPACE_STORAGE_DEVICE_CACHE_REPLICA_WRITES:
				ns->storage_cache_replica_writes = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY:
				ns->storage_cold_start_empty = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMMIT_TO_DEVICE:
				cfg_enterprise_only(&line);
				ns->storage_commit_to_device = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_COMPRESSION_OPTS, NUM_NAMESPACE_STORAGE_COMPRESSION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_COMPRESSION_NONE:
					ns->storage_compression = AS_COMPRESSION_NONE;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_LZ4:
					ns->storage_compression = AS_COMPRESSION_LZ4;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_SNAPPY:
					ns->storage_compression = AS_COMPRESSION_SNAPPY;
					break;
				case CASE_NAMESPACE_STORAGE_COMPRESSION_ZSTD:
					ns->storage_compression = AS_COMPRESSION_ZSTD;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COMPRESSION_ACCELERATION:
				cfg_enterprise_only(&line);
				ns->storage_compression_acceleration = cfg_u32(&line, 1, 65537);
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
				ns->storage_defrag_startup_minimum = cfg_u32(&line, 0, 99);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEVICE:
				cfg_add_storage_device(ns, cfg_strdup_no_checks(&line), cfg_strdup_val2_no_checks(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DIRECT_FILES:
				ns->storage_direct_files = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODSYNC:
				ns->storage_disable_odsync = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_BENCHMARKS_STORAGE:
				ns->storage_benchmarks_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION:
				cfg_enterprise_only(&line);
				switch (cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_ENCRYPTION_OPTS, NUM_NAMESPACE_STORAGE_ENCRYPTION_OPTS)) {
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_128:
					ns->storage_encryption = AS_ENCRYPTION_AES_128;
					break;
				case CASE_NAMESPACE_STORAGE_ENCRYPTION_AES_256:
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
				ns->storage_encryption_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENCRYPTION_OLD_KEY_FILE:
				cfg_enterprise_only(&line);
				ns->storage_encryption_old_key_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_EVICT_USED_PCT:
				ns->storage_evict_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILE:
				cfg_add_storage_file(ns, cfg_strdup_no_checks(&line), cfg_strdup_val2_no_checks(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE:
				ns->storage_filesize = cfg_u64(&line, AS_STORAGE_MIN_DEVICE_SIZE, AS_STORAGE_MAX_DEVICE_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS:
				ns->storage_flush_max_us = cfg_u64_no_checks(&line) * 1000;
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_SIZE:
				ns->storage_flush_size = cfg_u32_power_of_2(&line, MIN_FLUSH_SIZE, MAX_FLUSH_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE:
				ns->storage_max_write_cache = cfg_u64(&line, DEFAULT_MAX_WRITE_CACHE, UINT64_MAX);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_CACHE:
				ns->storage_post_write_cache = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_READ_PAGE_CACHE:
				ns->storage_read_page_cache = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SERIALIZE_TOMB_RAIDER:
				cfg_enterprise_only(&line);
				ns->storage_serialize_tomb_raider = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SINDEX_STARTUP_DEVICE_SCAN:
				ns->storage_sindex_startup_device_scan = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_AVAIL_PCT:
				ns->storage_stop_writes_avail_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_STOP_WRITES_USED_PCT:
				ns->storage_stop_writes_used_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_TOMB_RAIDER_SLEEP:
				cfg_enterprise_only(&line);
				ns->storage_tomb_raider_sleep = cfg_u32_no_checks(&line);
				break;
			// Obsoleted:
			case CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY:
				cfg_obsolete(&line, "please use 'storage-engine memory' to store data in memory");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT:
				cfg_obsolete(&line, "please use 'read-page-cache' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC:
				cfg_obsolete(&line, "please use 'flush-files' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MAX_USED_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-used-pct' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT:
				cfg_obsolete(&line, "please use 'stop-writes-avail-pct' instead");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE:
				cfg_obsolete(&line, "please use 'post-write-cache' and perhaps 'max-record-size'");
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE:
				cfg_obsolete(&line, "please use 'flush-size' and 'post-write-cache' and perhaps 'max-record-size'");
				break;
			case CASE_CONTEXT_END:
				if (ns->n_storage_devices == 0 && ns->n_storage_files == 0) {
					cf_crash_nostack(AS_CFG, "{%s} has no devices or files", ns->name);
				}
				if (ns->n_storage_files != 0 && ns->storage_filesize == 0) {
					cf_crash_nostack(AS_CFG, "{%s} must configure 'filesize' if using storage files", ns->name);
				}
				if (ns->storage_commit_to_device && ns->storage_disable_odsync) {
					cf_crash_nostack(AS_CFG, "{%s} can't configure both 'commit-to-device' and 'disable-odsync'", ns->name);
				}
				if (ns->storage_compression_acceleration != 0 && ns->storage_compression != AS_COMPRESSION_LZ4) {
					cf_crash_nostack(AS_CFG, "{%s} 'compression-acceleration' is only relevant for 'compression lz4'", ns->name);
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
			case CASE_NAMESPACE_SET_DEFAULT_READ_TOUCH_TTL_PCT:
				p_set->default_read_touch_ttl_pct = cfg_pct_w_minus_1(&line);
				break;
			case CASE_NAMESPACE_SET_DEFAULT_TTL:
				p_set->default_ttl = cfg_check_set_default_ttl(ns->name, p_set->name, cfg_seconds_no_checks(&line));
				break;
			case CASE_NAMESPACE_SET_DISABLE_EVICTION:
				p_set->eviction_disabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_SET_ENABLE_INDEX:
				p_set->index_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_SET_STOP_WRITES_COUNT:
				p_set->stop_writes_count = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_SET_STOP_WRITES_SIZE:
				p_set->stop_writes_size = cfg_u64_no_checks(&line);
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
		// Parse namespace::geo2dsphere-within context items.
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
			case CASE_SECURITY_DEFAULT_PASSWORD_FILE:
				c->sec_cfg.default_password_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_ENABLE_QUOTAS:
				c->sec_cfg.quotas_enabled = cfg_bool(&line);
				break;
			case CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD:
				c->sec_cfg.privilege_refresh_period = cfg_seconds(&line, PRIVILEGE_REFRESH_PERIOD_MIN, PRIVILEGE_REFRESH_PERIOD_MAX);
				break;
			case CASE_SECURITY_SESSION_TTL:
				c->sec_cfg.session_ttl = cfg_seconds(&line, SECURITY_SESSION_TTL_MIN, SECURITY_SESSION_TTL_MAX);
				break;
			case CASE_SECURITY_TPS_WEIGHT:
				c->sec_cfg.tps_weight = cfg_u32(&line, TPS_WEIGHT_MIN, TPS_WEIGHT_MAX);
				break;
			// Sub-contexts:
			case CASE_SECURITY_LDAP_BEGIN:
				c->sec_cfg.ldap_configured = true;
				cfg_begin_context(&state, SECURITY_LDAP);
				break;
			case CASE_SECURITY_LOG_BEGIN:
				cfg_begin_context(&state, SECURITY_LOG);
				break;
			// Obsoleted:
			case CASE_SECURITY_ENABLE_LDAP:
				cfg_obsolete(&line, "the 'ldap' context automatically enables LDAP");
				break;
			case CASE_SECURITY_ENABLE_SECURITY:
				cfg_obsolete(&line, "the 'security' context automatically enables security");
				break;
			case CASE_SECURITY_SYSLOG_BEGIN:
				cfg_obsolete(&line, "please use a logging context 'syslog' log sink");
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
			case CASE_SECURITY_LDAP_LOGIN_THREADS:
				c->sec_cfg.n_ldap_login_threads = cfg_u32(&line, 1, 64);
				break;
			case CASE_SECURITY_LDAP_POLLING_PERIOD:
				c->sec_cfg.ldap_polling_period = cfg_seconds(&line, 0, LDAP_POLLING_PERIOD_MAX);
				break;
			case CASE_SECURITY_LDAP_QUERY_BASE_DN:
				c->sec_cfg.ldap_query_base_dn = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_QUERY_USER_DN:
				c->sec_cfg.ldap_query_user_dn = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_QUERY_USER_PASSWORD_FILE:
				c->sec_cfg.ldap_query_user_password_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_BASE_DN:
				c->sec_cfg.ldap_role_query_base_dn = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_PATTERN:
				cfg_add_ldap_role_query_pattern(cfg_strdup_no_checks(&line));
				break;
			case CASE_SECURITY_LDAP_ROLE_QUERY_SEARCH_OU:
				c->sec_cfg.ldap_role_query_search_ou = cfg_bool(&line);
				break;
			case CASE_SECURITY_LDAP_SERVER:
				c->sec_cfg.ldap_server = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_TLS_CA_FILE:
				c->sec_cfg.ldap_tls_ca_file = cfg_strdup_no_checks(&line);
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
				c->sec_cfg.ldap_user_dn_pattern = cfg_strdup_no_checks(&line);
				break;
			case CASE_SECURITY_LDAP_USER_QUERY_PATTERN:
				c->sec_cfg.ldap_user_query_pattern = cfg_strdup_no_checks(&line);
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
				c->sec_cfg.report.authentication = cfg_bool(&line);
				break;
			case CASE_SECURITY_LOG_REPORT_DATA_OP:
				as_security_config_log_scope(line.val_tok_1, line.val_tok_2);
				break;
			case CASE_SECURITY_LOG_REPORT_DATA_OP_ROLE:
				as_security_config_log_role(line.val_tok_1);
				break;
			case CASE_SECURITY_LOG_REPORT_DATA_OP_USER:
				as_security_config_log_user(line.val_tok_1);
				break;
			case CASE_SECURITY_LOG_REPORT_SYS_ADMIN:
				c->sec_cfg.report.sys_admin = cfg_bool(&line);
				break;
			case CASE_SECURITY_LOG_REPORT_USER_ADMIN:
				c->sec_cfg.report.user_admin = cfg_bool(&line);
				break;
			case CASE_SECURITY_LOG_REPORT_VIOLATION:
				c->sec_cfg.report.violation = cfg_bool(&line);
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
			case CASE_XDR_SRC_ID:
				c->xdr_cfg.src_id = cfg_u8(&line, 1, UINT8_MAX);
				break;
			// Sub-contexts:
			case CASE_XDR_DC_BEGIN:
				dc_cfg = as_xdr_startup_create_dc(line.val_tok_1);
				cfg_begin_context(&state, XDR_DC);
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
		// Parse xdr::dc context items.
		//
		case XDR_DC:
			switch (cfg_find_tok(line.name_tok, XDR_DC_OPTS, NUM_XDR_DC_OPTS)) {
			case CASE_XDR_DC_AUTH_MODE:
				switch (cfg_find_tok(line.val_tok_1, XDR_DC_AUTH_MODE_OPTS, NUM_XDR_DC_AUTH_MODE_OPTS)) {
				case CASE_XDR_DC_AUTH_MODE_NONE:
					dc_cfg->auth_mode = XDR_AUTH_NONE;
					break;
				case CASE_XDR_DC_AUTH_MODE_INTERNAL:
					dc_cfg->auth_mode = XDR_AUTH_INTERNAL;
					break;
				case CASE_XDR_DC_AUTH_MODE_EXTERNAL:
					dc_cfg->auth_mode = XDR_AUTH_EXTERNAL;
					break;
				case CASE_XDR_DC_AUTH_MODE_EXTERNAL_INSECURE:
					dc_cfg->auth_mode = XDR_AUTH_EXTERNAL_INSECURE;
					break;
				case CASE_XDR_DC_AUTH_MODE_PKI:
					dc_cfg->auth_mode = XDR_AUTH_PKI;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_XDR_DC_AUTH_PASSWORD_FILE:
				dc_cfg->auth_password_file = cfg_strdup_no_checks(&line);
				break;
			case CASE_XDR_DC_AUTH_USER:
				dc_cfg->auth_user = cfg_strdup(&line, 64);
				break;
			case CASE_XDR_DC_CONNECTOR:
				dc_cfg->connector = cfg_bool(&line);
				break;
			case CASE_XDR_DC_MAX_RECOVERIES_INTERLEAVED:
				dc_cfg->max_recoveries_interleaved = cfg_u32_no_checks(&line);
				break;
			case CASE_XDR_DC_NODE_ADDRESS_PORT:
				as_xdr_startup_add_seed(dc_cfg, cfg_strdup_no_checks(&line), cfg_strdup_val2_no_checks(&line, true), cfg_strdup_val3_no_checks(&line, false));
				break;
			case CASE_XDR_DC_PERIOD_MS:
				dc_cfg->period_us = 1000 * cfg_u32(&line, AS_XDR_MIN_PERIOD_MS, AS_XDR_MAX_PERIOD_MS);
				break;
			case CASE_XDR_DC_TLS_NAME:
				dc_cfg->tls_our_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_XDR_DC_USE_ALTERNATE_ACCESS_ADDRESS:
				dc_cfg->use_alternate_access_address = cfg_bool(&line);
				break;
			// Sub-contexts:
			case CASE_XDR_DC_NAMESPACE_BEGIN:
				dc_ns_cfg = as_xdr_startup_create_dc_ns_cfg(line.val_tok_1);
				cf_vector_append_ptr(dc_cfg->ns_cfg_v, dc_ns_cfg);
				cfg_begin_context(&state, XDR_DC_NAMESPACE);
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
		// Parse xdr::dc::namespace context items.
		//
		case XDR_DC_NAMESPACE:
			switch (cfg_find_tok(line.name_tok, XDR_DC_NAMESPACE_OPTS, NUM_XDR_DC_NAMESPACE_OPTS)) {
			case CASE_XDR_DC_NAMESPACE_BIN_POLICY:
				switch (cfg_find_tok(line.val_tok_1, XDR_DC_NAMESPACE_BIN_POLICY_OPTS, NUM_XDR_DC_NAMESPACE_BIN_POLICY_OPTS)) {
				case CASE_XDR_DC_NAMESPACE_BIN_POLICY_ALL:
					dc_ns_cfg->bin_policy = XDR_BIN_POLICY_ALL;
					break;
				case CASE_XDR_DC_NAMESPACE_BIN_POLICY_NO_BINS:
					dc_ns_cfg->bin_policy = XDR_BIN_POLICY_NO_BINS;
					break;
				case CASE_XDR_DC_NAMESPACE_BIN_POLICY_ONLY_CHANGED:
					dc_ns_cfg->bin_policy = XDR_BIN_POLICY_ONLY_CHANGED;
					break;
				case CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_AND_SPECIFIED:
					dc_ns_cfg->bin_policy = XDR_BIN_POLICY_CHANGED_AND_SPECIFIED;
					break;
				case CASE_XDR_DC_NAMESPACE_BIN_POLICY_CHANGED_OR_SPECIFIED:
					dc_ns_cfg->bin_policy = XDR_BIN_POLICY_CHANGED_OR_SPECIFIED;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_XDR_DC_NAMESPACE_COMPRESSION_LEVEL:
				dc_ns_cfg->compression_level = cfg_u32(&line, 1, 9);
				break;
			case CASE_XDR_DC_NAMESPACE_COMPRESSION_THRESHOLD:
				dc_ns_cfg->compression_threshold = cfg_u32(&line, AS_XDR_MIN_COMPRESSION_THRESHOLD, UINT32_MAX);
				break;
			case CASE_XDR_DC_NAMESPACE_DELAY_MS:
				dc_ns_cfg->delay_ms = cfg_u32(&line, 0, AS_XDR_MAX_HOT_KEY_MS);
				break;
			case CASE_XDR_DC_NAMESPACE_ENABLE_COMPRESSION:
				dc_ns_cfg->compression_enabled = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_FORWARD:
				dc_ns_cfg->forward = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_HOT_KEY_MS:
				dc_ns_cfg->hot_key_ms = cfg_u32(&line, 0, AS_XDR_MAX_HOT_KEY_MS);
				break;
			case CASE_XDR_DC_NAMESPACE_IGNORE_BIN:
				cf_vector_append_ptr(dc_ns_cfg->ignored_bins, cfg_strdup(&line, AS_BIN_NAME_MAX_SZ));
				break;
			case CASE_XDR_DC_NAMESPACE_IGNORE_EXPUNGES:
				dc_ns_cfg->ignore_expunges = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_IGNORE_SET:
				cf_vector_append_ptr(dc_ns_cfg->ignored_sets, cfg_strdup(&line, AS_SET_NAME_MAX_SIZE));
				break;
			case CASE_XDR_DC_NAMESPACE_MAX_THROUGHPUT:
				dc_ns_cfg->max_throughput = cfg_u32_multiple_of(&line, 100);
				break;
			case CASE_XDR_DC_NAMESPACE_REMOTE_NAMESPACE:
				dc_ns_cfg->remote_namespace = cfg_strdup(&line, AS_ID_NAMESPACE_SZ);
				break;
			case CASE_XDR_DC_NAMESPACE_SC_REPLICATION_WAIT_MS:
				dc_ns_cfg->sc_replication_wait_ms = cfg_u32(&line, AS_XDR_MIN_SC_REPLICATION_WAIT_MS, AS_XDR_MAX_SC_REPLICATION_WAIT_MS);
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_BIN:
				cf_vector_append_ptr(dc_ns_cfg->shipped_bins, cfg_strdup(&line, AS_BIN_NAME_MAX_SZ));
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_BIN_LUTS:
				dc_ns_cfg->ship_bin_luts = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_NSUP_DELETES:
				dc_ns_cfg->ship_nsup_deletes = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_ONLY_SPECIFIED_SETS:
				dc_ns_cfg->ship_only_specified_sets = cfg_bool(&line);
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_SET:
				cf_vector_append_ptr(dc_ns_cfg->shipped_sets, cfg_strdup(&line, AS_SET_NAME_MAX_SIZE));
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_INTERVAL:
				dc_ns_cfg->ship_versions_interval_ms = cfg_seconds(&line, AS_XDR_MIN_SHIP_VERSIONS_INTERVAL, AS_XDR_MAX_SHIP_VERSIONS_INTERVAL) * 1000;
				break;
			case CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY:
				switch (cfg_find_tok(line.val_tok_1, XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_OPTS, NUM_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_OPTS)) {
				case CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_LATEST:
					dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_LATEST;
					break;
				case CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_ALL:
					dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_ALL;
					break;
				case CASE_XDR_DC_NAMESPACE_SHIP_VERSIONS_POLICY_INTERVAL:
					dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_INTERVAL;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_XDR_DC_NAMESPACE_TRANSACTION_QUEUE_LIMIT:
				dc_ns_cfg->transaction_queue_limit = cfg_u32_power_of_2(&line, AS_XDR_MIN_TRANSACTION_QUEUE_LIMIT, AS_XDR_MAX_TRANSACTION_QUEUE_LIMIT);
				break;
			case CASE_XDR_DC_NAMESPACE_WRITE_POLICY:
				switch (cfg_find_tok(line.val_tok_1, XDR_DC_NAMESPACE_WRITE_POLICY_OPTS, NUM_XDR_DC_NAMESPACE_WRITE_POLICY_OPTS)) {
				case CASE_XDR_DC_NAMESPACE_WRITE_POLICY_AUTO:
					dc_ns_cfg->write_policy = XDR_WRITE_POLICY_AUTO;
					break;
				case CASE_XDR_DC_NAMESPACE_WRITE_POLICY_UPDATE:
					dc_ns_cfg->write_policy = XDR_WRITE_POLICY_UPDATE;
					break;
				case CASE_XDR_DC_NAMESPACE_WRITE_POLICY_REPLACE:
					dc_ns_cfg->write_policy = XDR_WRITE_POLICY_REPLACE;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
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
		// Parser state is corrupt.
		//
		default:
			cf_crash_nostack(AS_CFG, "line %d :: invalid parser top-level state %d", line_num, state.current);
			break;
		}
	}

	cfg_parser_done(&state);

	fclose(FD);

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

	if (g_config.cluster_name[0] == '\0') {
		cf_crash_nostack(AS_CFG, "must configure 'cluster-name'");
	}

	if (g_config.n_namespaces == 0) {
		cf_crash_nostack(AS_CFG, "must configure at least one namespace");
	}

	cf_alloc_set_debug(c->debug_allocations, c->indent_allocations,
			c->poison_allocations, c->quarantine_allocations);

	// Configuration checks and special defaults that differ between CE and EE.
	cfg_post_process();

	// Check the configured file descriptor limit against the system limit.
	struct rlimit fd_limit;

	getrlimit(RLIMIT_NOFILE, &fd_limit);

	if ((rlim_t)c->n_proto_fd_max > fd_limit.rlim_cur) {
		cf_crash_nostack(AS_CFG, "%lu system file descriptors not enough, config specified %u", fd_limit.rlim_cur, c->n_proto_fd_max);
	}

	cf_info(AS_CFG, "system file descriptor limit: %lu, proto-fd-max: %u", fd_limit.rlim_cur, c->n_proto_fd_max);

	// Output NUMA topology information.
	cf_topo_info();

	uint16_t n_cpus = cf_topo_count_cpus();

	if (c->auto_pin != CF_TOPO_AUTO_PIN_NONE &&
			c->n_service_threads % n_cpus != 0) {
		cf_crash_nostack(AS_CFG, "with 'auto-pin', 'service-threads' must be a multiple of the number of CPUs (%hu)", n_cpus);
	}

	if (c->n_service_threads == 0) {
		c->n_service_threads = c->n_namespaces_not_inlined != 0 ?
				n_cpus * 5 : n_cpus;

		if (c->n_service_threads > MAX_SERVICE_THREADS) {
			c->n_service_threads = MAX_SERVICE_THREADS;
		}
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

		if (tls_spec == NULL) {
			cf_crash_nostack(AS_CFG, "failed to resolve service tls-name");
		}

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

		if (g_service_tls == NULL) {
			cf_crash_nostack(AS_CFG, "failed to set up service tls");
		}
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

		if (tls_spec == NULL) {
			cf_crash_nostack(AS_CFG, "failed to resolve heartbeat tls-name");
		}

		c->hb_config.tls = tls_config_intra_context(tls_spec, "heartbeat");

		if (c->hb_config.tls == NULL) {
			cf_crash_nostack(AS_CFG, "failed to set up heartbeat tls");
		}
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

		if (tls_spec == NULL) {
			cf_crash_nostack(AS_CFG, "failed to resolve fabric tls-name");
		}

		g_fabric_tls = tls_config_intra_context(tls_spec, "fabric");

		if (g_fabric_tls == NULL) {
			cf_crash_nostack(AS_CFG, "failed to set up fabric tls");
		}
	}

	if (g_fabric_bind.n_cfgs == 0) {
		cf_crash_nostack(AS_CFG, "no fabric ports configured");
	}

	// Info service port.

	g_info_port = g_config.info.bind_port;

	// Info service bind addresses.

	cf_serv_cfg_init(&g_info_bind);
	cfg_serv_spec_to_bind(&g_config.info, NULL, &g_info_bind, CF_SOCK_OWNER_INFO);

	// XDR TLS setup.

	as_xdr_link_tls();

	// Validate heartbeat configuration.
	as_hb_config_validate();

	//--------------------------------------------
	// Per-namespace config post-processing.
	//

	uint64_t max_alloc_sz = 0;

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		if ((ns->pi_xmem_type == CF_XMEM_TYPE_MEM ||
				ns->pi_xmem_type == CF_XMEM_TYPE_SHMEM) &&
				ns->index_stage_size > max_alloc_sz) {
			max_alloc_sz = ns->index_stage_size;
		}

		if (ns->sindex_stage_size > max_alloc_sz) {
			max_alloc_sz = ns->sindex_stage_size;
		}

		client_replica_maps_create(ns);

		uint32_t sprigs_offset = sizeof(as_lock_pair) * NUM_LOCK_PAIRS;
		uint32_t puddles_offset = 0;

		if (ns->pi_xmem_type == CF_XMEM_TYPE_FLASH) {
			puddles_offset = sprigs_offset + sizeof(as_sprig) * ns->tree_shared.n_sprigs;
		}

		// Note - ns->tree_shared.arena is set later when it's allocated.
		ns->tree_shared.destructor			= (as_index_value_destructor)as_record_destroy;
		ns->tree_shared.destructor_udata	= (void*)ns;
		ns->tree_shared.locks_shift			= NUM_SPRIG_BITS - cf_msb(NUM_LOCK_PAIRS);
		ns->tree_shared.sprigs_shift		= NUM_SPRIG_BITS - cf_msb(ns->tree_shared.n_sprigs);
		ns->tree_shared.sprigs_offset		= sprigs_offset;
		ns->tree_shared.puddles_offset		= puddles_offset;

		histogram_scale scale = as_config_histogram_scale();
		char hist_name[HISTOGRAM_NAME_SIZE];

		// One-way activated histograms.

		sprintf(hist_name, "{%s}-read", ns->name);
		ns->read_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-write", ns->name);
		ns->write_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-udf", ns->name);
		ns->udf_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-batch-sub-read", ns->name);
		ns->batch_sub_read_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-batch-sub-write", ns->name);
		ns->batch_sub_write_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-batch-sub-udf", ns->name);
		ns->batch_sub_udf_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-pi-query", ns->name);
		ns->pi_query_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-pi-query-rec-count", ns->name);
		ns->pi_query_rec_count_hist = histogram_create(hist_name, HIST_COUNT);

		sprintf(hist_name, "{%s}-si-query", ns->name);
		ns->si_query_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-si-query-rec-count", ns->name);
		ns->si_query_rec_count_hist = histogram_create(hist_name, HIST_COUNT);

		sprintf(hist_name, "{%s}-read-touch", ns->name);
		ns->read_touch_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-re-repl", ns->name);
		ns->re_repl_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-writes-per-mrt", ns->name);
		ns->writes_per_mrt_hist = histogram_create(hist_name, HIST_COUNT);

		// Activate-by-config histograms.

		sprintf(hist_name, "{%s}-proxy", ns->name);
		ns->proxy_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-read-start", ns->name);
		ns->read_start_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-read-restart", ns->name);
		ns->read_restart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-read-dup-res", ns->name);
		ns->read_dup_res_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-read-repl-ping", ns->name);
		ns->read_repl_ping_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-read-local", ns->name);
		ns->read_local_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-read-response", ns->name);
		ns->read_response_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-write-start", ns->name);
		ns->write_start_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-write-restart", ns->name);
		ns->write_restart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-write-dup-res", ns->name);
		ns->write_dup_res_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-write-master", ns->name);
		ns->write_master_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-write-repl-write", ns->name);
		ns->write_repl_write_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-write-response", ns->name);
		ns->write_response_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-udf-start", ns->name);
		ns->udf_start_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-restart", ns->name);
		ns->udf_restart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-dup-res", ns->name);
		ns->udf_dup_res_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-master", ns->name);
		ns->udf_master_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-repl-write", ns->name);
		ns->udf_repl_write_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-response", ns->name);
		ns->udf_response_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-batch-sub-prestart", ns->name);
		ns->batch_sub_prestart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-start", ns->name);
		ns->batch_sub_start_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-restart", ns->name);
		ns->batch_sub_restart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-dup-res", ns->name);
		ns->batch_sub_dup_res_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-repl-ping", ns->name);
		ns->batch_sub_repl_ping_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-read-local", ns->name);
		ns->batch_sub_read_local_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-write-master", ns->name);
		ns->batch_sub_write_master_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-udf-master", ns->name);
		ns->batch_sub_udf_master_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-repl-write", ns->name);
		ns->batch_sub_repl_write_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-batch-sub-response", ns->name);
		ns->batch_sub_response_hist = histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-udf-sub-start", ns->name);
		ns->udf_sub_start_hist =  histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-sub-restart", ns->name);
		ns->udf_sub_restart_hist =  histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-sub-dup-res", ns->name);
		ns->udf_sub_dup_res_hist =  histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-sub-master", ns->name);
		ns->udf_sub_master_hist =  histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-sub-repl-write", ns->name);
		ns->udf_sub_repl_write_hist =  histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-udf-sub-response", ns->name);
		ns->udf_sub_response_hist =  histogram_create(hist_name, scale);

		sprintf(hist_name, "{%s}-ops-sub-start", ns->name);
		ns->ops_sub_start_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-ops-sub-restart", ns->name);
		ns->ops_sub_restart_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-ops-sub-dup-res", ns->name);
		ns->ops_sub_dup_res_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-ops-sub-master", ns->name);
		ns->ops_sub_master_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-ops-sub-repl-write", ns->name);
		ns->ops_sub_repl_write_hist = histogram_create(hist_name, scale);
		sprintf(hist_name, "{%s}-ops-sub-response", ns->name);
		ns->ops_sub_response_hist = histogram_create(hist_name, scale);

		// 'nsup' histograms.

		sprintf(hist_name, "{%s}-object-size-log2", ns->name);
		ns->obj_size_log_hist = histogram_create(hist_name, HIST_SIZE);
		sprintf(hist_name, "{%s}-object-size-linear", ns->name);
		ns->obj_size_lin_hist = linear_hist_create(hist_name, LINEAR_HIST_SIZE, 0, ns->max_record_size, OBJ_SIZE_HIST_NUM_BUCKETS);

		sprintf(hist_name, "{%s}-evict", ns->name);
		ns->evict_hist = linear_hist_create(hist_name, LINEAR_HIST_SECONDS, 0, 0, ns->evict_hist_buckets);

		sprintf(hist_name, "{%s}-ttl", ns->name);
		ns->ttl_hist = linear_hist_create(hist_name, LINEAR_HIST_SECONDS, 0, 0, TTL_HIST_NUM_BUCKETS);
	}

	cf_os_best_practices_checks(&g_bad_practices, max_alloc_sz);
	cfg_best_practices_check();
	cf_dyn_buf_chomp_char(&g_bad_practices, ',');

	if (g_bad_practices.used_sz != 0) {
		if (c->enforce_best_practices) {
			cf_crash_nostack(AS_CFG, "failed best-practices checks - see 'https://docs.aerospike.com/docs/operations/install/linux/bestpractices/index.html'");
		}
		else {
			cf_warning(AS_CFG, "failed best-practices checks - see 'https://docs.aerospike.com/docs/operations/install/linux/bestpractices/index.html'");
		}
	}
}


//==========================================================
// Public API - Cluster name.
//

static cf_mutex g_cluster_name_lock = CF_MUTEX_INIT;

void
as_config_cluster_name_get(char* cluster_name)
{
	cf_mutex_lock(&g_cluster_name_lock);
	strcpy(cluster_name, g_config.cluster_name);
	cf_mutex_unlock(&g_cluster_name_lock);
}

bool
as_config_cluster_name_set(const char* cluster_name)
{
	size_t len = strlen(cluster_name);

	if (len == 0 || len >= AS_CLUSTER_NAME_SZ) {
		return false;
	}

	cf_mutex_lock(&g_cluster_name_lock);
	strcpy(g_config.cluster_name, cluster_name);
	cf_mutex_unlock(&g_cluster_name_lock);

	return true;
}

bool
as_config_cluster_name_matches(const char* cluster_name)
{
	cf_mutex_lock(&g_cluster_name_lock);
	bool matches = strcmp(cluster_name, g_config.cluster_name) == 0;
	cf_mutex_unlock(&g_cluster_name_lock);

	return matches;
}


//==========================================================
// Item-specific parsing utilities.
//

// TODO - should be split function or move to EE?
static void
cfg_add_feature_key_file(const char* path)
{
	if (g_config.n_feature_key_files == MAX_FEATURE_KEY_FILES) {
		cf_crash_nostack(AS_CFG, "too many feature keys");
	}

	for (uint32_t i = 0; i < g_config.n_feature_key_files; i++) {
		if (strcmp(path, g_config.feature_key_files[i]) == 0) {
			cf_crash_nostack(AS_CFG, "duplicate feature key %s", path);
		}
	}

	g_config.feature_key_files[g_config.n_feature_key_files++] = path;
}

static void
init_addr_list(cf_addr_list* addrs)
{
	addrs->n_addrs = 0;
	memset(&addrs->addrs, '\0', sizeof(addrs->addrs));
}

static void
add_addr(const char* name, cf_addr_list* addrs)
{
	uint32_t n = addrs->n_addrs;

	if (n >= CF_SOCK_CFG_MAX) {
		cf_crash_nostack(CF_SOCKET, "Too many addresses: %s", name);
	}

	addrs->addrs[n] = cf_strdup(name);
	++addrs->n_addrs;
}

// TODO - should be split function or move to EE?
static void
add_tls_peer_name(const char* name, cf_serv_spec* spec)
{
	uint32_t n = spec->n_tls_peer_names;

	if (n >= CF_SOCK_CFG_MAX) {
		cf_crash_nostack(CF_SOCKET, "Too many TLS peer names: %s", name);
	}

	spec->tls_peer_names[n] = cf_strdup(name);
	++spec->n_tls_peer_names;
}

static void
copy_addrs(const cf_addr_list* from, cf_addr_list* to)
{
	for (uint32_t i = 0; i < from->n_addrs; ++i) {
		to->addrs[i] = from->addrs[i];
	}

	to->n_addrs = from->n_addrs;
}

static void
default_addrs(cf_addr_list* one, cf_addr_list* two)
{
	if (one->n_addrs == 0) {
		copy_addrs(two, one);
	}

	if (two->n_addrs == 0) {
		copy_addrs(one, two);
	}
}

static void
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

static void
cfg_add_addr_bind(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->bind);
}

static void
cfg_add_addr_std(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->std);
}

static void
cfg_add_addr_alt(const char* name, cf_serv_spec* spec)
{
	add_addr(name, &spec->alt);
}

static void
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

static void
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

		cfg.i_addr = i;

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

static void
cfg_serv_spec_std_to_access(const cf_serv_spec* spec, cf_addr_list* access)
{
	addrs_to_access(&spec->std, access);
}

static void
cfg_serv_spec_alt_to_access(const cf_serv_spec* spec, cf_addr_list* access)
{
	addrs_to_access(&spec->alt, access);
}

static void
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

static void
cfg_add_secrets_addr_port(char* addr, char* port, char* tls_name)
{
	g_secrets_cfg.addr = addr;
	g_secrets_cfg.port = port;
	g_secrets_cfg.tls_name = tls_name;
}

static as_set*
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

static uint32_t
cfg_check_set_default_ttl(const char* ns_name, const char* set_name,
		uint32_t value)
{
	if (value > MAX_ALLOWED_TTL && value != TTL_NEVER_EXPIRE) {
		cf_crash_nostack(AS_CFG, "{%s|%s} default-ttl must be <= %u seconds",
				ns_name, set_name, MAX_ALLOWED_TTL);
	}

	return value;
}

// TODO - should be split function or move to EE?
static void
cfg_add_pi_xmem_mount(as_namespace* ns, const char* mount)
{
	if (ns->n_pi_xmem_mounts == CF_XMEM_MAX_MOUNTS) {
		cf_crash_nostack(AS_CFG, "{%s} too many index mounts", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_pi_xmem_mounts; i++) {
		if (strcmp(mount, ns->pi_xmem_mounts[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate index mount %s", ns->name, mount);
		}
	}

	ns->pi_xmem_mounts[ns->n_pi_xmem_mounts++] = mount;
}

// TODO - should be split function or move to EE?
static void
cfg_add_si_xmem_mount(as_namespace* ns, const char* mount)
{
	if (ns->n_si_xmem_mounts == CF_XMEM_MAX_MOUNTS) {
		cf_crash_nostack(AS_CFG, "{%s} too many sindex mounts", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_si_xmem_mounts; i++) {
		if (strcmp(mount, ns->si_xmem_mounts[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate sindex mount %s", ns->name, mount);
		}
	}

	ns->si_xmem_mounts[ns->n_si_xmem_mounts++] = mount;
}

static void
cfg_add_mem_shadow_file(as_namespace* ns, const char* file_name)
{
	if (ns->n_storage_devices != 0) {
		cf_crash_nostack(AS_CFG, "{%s} mixture of storage files and devices", ns->name);
	}

	if (ns->n_storage_files == AS_STORAGE_MAX_DEVICES) {
		cf_crash_nostack(AS_CFG, "{%s} too many storage files", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_storage_files; i++) {
		if (strcmp(file_name, ns->storage_shadows[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage file %s", ns->name, file_name);
		}
	}

	ns->storage_shadows[ns->n_storage_shadows++] = file_name;
	ns->n_storage_files++;
	// Note - n_storage_files used only for error checking here, and to signify
	// later that shadows are files.
}

static void
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

static void
cfg_add_mem_shadow_device(as_namespace* ns, const char* device_name)
{
	if (ns->n_storage_files != 0) {
		cf_crash_nostack(AS_CFG, "{%s} mixture of storage files and devices", ns->name);
	}

	if (ns->n_storage_devices == AS_STORAGE_MAX_DEVICES) {
		cf_crash_nostack(AS_CFG, "{%s} too many storage devices", ns->name);
	}

	for (uint32_t i = 0; i < ns->n_storage_devices; i++) {
		if (strcmp(device_name, ns->storage_shadows[i]) == 0) {
			cf_crash_nostack(AS_CFG, "{%s} duplicate storage device %s", ns->name, device_name);
		}
	}

	ns->storage_shadows[ns->n_storage_shadows++] = device_name;
	ns->storage_devices[ns->n_storage_devices++] = device_name;
	// Note - load devices array here for best practices device size check. The
	// array will later be used to keep memory stripes. Also, n_storage_devices
	// will signify later that shadows are devices.
}

static void
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

static void
cfg_set_cluster_name(char* cluster_name)
{
	if (! as_config_cluster_name_set(cluster_name)) {
		cf_crash_nostack(AS_CFG, "bad cluster name '%s'", cluster_name);
	}
}

// TODO - should be split function or move to EE?
static void
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

// TODO - not really a config method any more, reorg needed.
static void
cfg_create_all_histograms()
{
	histogram_scale scale = as_config_histogram_scale();

	g_stats.batch_index_hist = histogram_create("batch-index", scale);
	g_stats.batch_rec_count_hist = histogram_create("batch-rec-count", HIST_COUNT);

	g_stats.info_hist = histogram_create("info", scale);

	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-send-init", scale);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-send-fragment", scale);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-recv-fragment", scale);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_BULK] = histogram_create("fabric-bulk-recv-cb", scale);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-send-init", scale);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-send-fragment", scale);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-recv-fragment", scale);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_CTRL] = histogram_create("fabric-ctrl-recv-cb", scale);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-send-init", scale);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-send-fragment", scale);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-recv-fragment", scale);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_META] = histogram_create("fabric-meta-recv-cb", scale);
	g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-send-init", scale);
	g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-send-fragment", scale);
	g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-recv-fragment", scale);
	g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_RW] = histogram_create("fabric-rw-recv-cb", scale);
}

static void
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

// TODO - should be split function or move to EE?
static cf_tls_spec*
cfg_create_tls_spec(as_config* cfg, char* name)
{
	uint32_t ind = cfg->n_tls_specs++;

	if (ind >= MAX_TLS_SPECS) {
		cf_crash_nostack(AS_CFG, "too many TLS configuration sections");
	}

	cf_tls_spec* tls_spec = cfg->tls_specs + ind;
	tls_spec->name = cf_strdup(name);
	tls_spec->protocols = "TLSv1.2";

	tls_init_change_check(tls_spec);

	return tls_spec;
}

// TODO - already declared in header - should be split function?
cf_tls_spec*
cfg_link_tls(const char* which, char** our_name)
{
	if (*our_name == NULL) {
		cf_warning(AS_CFG, "%s TLS configuration requires tls-name", which);
		return NULL;
	}

	*our_name = cf_resolve_tls_name(*our_name, g_config.cluster_name, which);

	if (*our_name == NULL) {
		return NULL;
	}

	cf_tls_spec* tls_spec = NULL;

	for (uint32_t i = 0; i < g_config.n_tls_specs; ++i) {
		if (strcmp(*our_name, g_config.tls_specs[i].name) == 0) {
			tls_spec = g_config.tls_specs + i;
			break;
		}
	}

	if (tls_spec == NULL) {
		cf_warning(AS_CFG, "invalid tls-name in TLS configuration: %s", *our_name);
		return NULL;
	}

	return tls_spec;
}

static void
cfg_keep_cap(bool keep, bool* what, int32_t cap)
{
	*what = keep;

	if (keep) {
		cf_process_add_runtime_cap(cap);
	}
}

static void
cfg_best_practices_check(void)
{
	as_config* c = &g_config;

	uint32_t n_cpus = (uint32_t)cf_topo_count_cpus();
	uint32_t min_service_threads = c->n_namespaces_not_inlined != 0 ?
			n_cpus * 3 : n_cpus;

	if (c->n_service_threads < min_service_threads) {
		check_failed(&g_bad_practices, "service-threads",
				"'service-threads' should be at least %u", min_service_threads);
	}

	uint64_t ns_mem = 0;
	uint32_t margin = WBLOCK_SZ;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		uint64_t max_sz = 0;
		uint64_t min_sz = UINT64_MAX;

		for (uint32_t i = 0; i < ns->n_storage_devices; i++) {
			int fd = open(ns->storage_devices[i], O_RDWR | O_DIRECT);

			if (fd == -1) {
				cf_crash(AS_CFG, "{%s} unable to open device %s: %s", ns->name,
						ns->storage_devices[i], cf_strerror(errno));
			}

			uint64_t sz = 0;

			ioctl(fd, BLKGETSIZE64, &sz); // gets the number of bytes
			close(fd);

			if (sz > max_sz) {
				max_sz = sz;
			}

			if (sz < min_sz) {
				min_sz = sz;
			}
		}

		if (max_sz != 0 && max_sz - min_sz > margin) {
			check_failed(&g_bad_practices, "device",
					"{%s} 'device' sizes must match to within %u bytes",
					ns->name, margin);
		}

		ns_mem += ns->indexes_memory_budget;

		if (ns->storage_type == AS_STORAGE_ENGINE_MEMORY) {
			if (ns->storage_data_size != 0) {
				ns_mem += ns->storage_data_size;
			}
			else if (ns->n_storage_files != 0) {
				ns_mem += ns->storage_filesize * ns->n_storage_files;
			}
			else {
				ns_mem += min_sz * ns->n_storage_devices;
			}
		}
	}

	uint64_t sys_mem = (uint64_t)sysconf(_SC_PHYS_PAGES) *
			(uint64_t)sysconf(_SC_PAGESIZE);

	if (ns_mem > sys_mem) {
		check_failed(&g_bad_practices, "memory",
				"data & indexes memory spec for all namespaces (%lu) exceeds system memory (%lu)",
				ns_mem, sys_mem);
	}
}
