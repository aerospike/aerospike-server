/*
 * cfg_info.c
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

//==========================================================
// Includes.
//

#include "base/cfg_info.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "cf_str.h"
#include "hardware.h"
#include "hist.h"
#include "dynbuf.h"
#include "log.h"
#include "os.h"
#include "socket.h"
#include "vault.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/security.h"
#include "base/service.h"
#include "base/set_index.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "query/query.h"
#include "storage/storage.h"


//==========================================================
// Globals.
//

// Protect all set-config commands from concurrency issues.
static cf_mutex g_set_cfg_lock = CF_MUTEX_INIT;


//==========================================================
// Forward declarations.
//

// Command config-get helpers.
static void cfg_get_service(cf_dyn_buf* db);
static void cfg_get_network(cf_dyn_buf* db);
static void cfg_get_namespace(char* context, cf_dyn_buf* db);

// cfg_get_* helpers.
static const char* auto_pin_string(void);
static const char* debug_allocations_string(void);
static void append_addrs(cf_dyn_buf* db, const char* name, const cf_addr_list* list);

// Command config-set helpers.
static int cfg_set(char* name, char* cmd, cf_dyn_buf* db);
static bool cfg_set_service(const char* cmd);
static bool cfg_set_network(const char* cmd);
static bool cfg_set_namespace(const char* cmd);
static bool cfg_set_set(const char* cmd, as_namespace* ns, const char* set_name, size_t set_name_len);

// Histogram helpers.
static void fabric_histogram_clear_all(void);
static void read_benchmarks_histogram_clear_all(as_namespace* ns);
static void write_benchmarks_histogram_clear_all(as_namespace* ns);
static void udf_benchmarks_histogram_clear_all(as_namespace* ns);
static void batch_sub_benchmarks_histogram_clear_all(as_namespace* ns);
static void udf_sub_benchmarks_histogram_clear_all(as_namespace* ns);
static void ops_sub_benchmarks_histogram_clear_all(as_namespace* ns);
static bool any_benchmarks_enabled(void);


//==========================================================
// Public API.
//

void
as_cfg_info_cmd_config_get_with_params(char* name, char* params, cf_dyn_buf* db)
{
	char context[1024];
	int context_len = sizeof(context);

	if (as_info_parameter_get(params, "context", context, &context_len) != 0) {
		cf_dyn_buf_append_string(db, "Error::invalid get-config parameter");
		return;
	}

	if (strcmp(context, "service") == 0) {
		cfg_get_service(db);
	}
	else if (strcmp(context, "network") == 0) {
		cfg_get_network(db);
	}
	else if (strcmp(context, "namespace") == 0) {
		context_len = sizeof(context);

		if (as_info_parameter_get(params, "id", context, &context_len) != 0) {
			cf_dyn_buf_append_string(db, "Error::invalid id");
			return;
		}

		cfg_get_namespace(context, db);
	}
	else if (strcmp(context, "security") == 0) {
		as_security_get_config(db);
	}
	else if (strcmp(context, "xdr") == 0) {
		as_xdr_get_config(params, db);
	}
	else {
		cf_dyn_buf_append_string(db, "Error::invalid context");
	}
}

int
as_cfg_info_cmd_config_get(char* name, char* params, cf_dyn_buf* db)
{
	if (params && *params != 0) {
		cf_debug(AS_INFO, "config-get command received: params %s", params);

		as_cfg_info_cmd_config_get_with_params(name, params, db);
		// Response may be an error string (without a semicolon).
		cf_dyn_buf_chomp_char(db, ';');
		return 0;
	}

	cf_debug(AS_INFO, "config-get command received");

	// No context - collect everything.
	cfg_get_service(db);
	cfg_get_network(db);
	as_security_get_config(db);

	cf_dyn_buf_chomp(db);

	return 0;
}

void
as_cfg_info_namespace_config_get(char* context, cf_dyn_buf* db)
{
	cfg_get_namespace(context, db);
}

// config-set:context=service;variable=value;
// config-set:context=network;variable=heartbeat.value;
// config-set:context=namespace;id=test;variable=value;
int
as_cfg_info_cmd_config_set(char* name, char* params, cf_dyn_buf* db)
{
	cf_mutex_lock(&g_set_cfg_lock);

	int result = cfg_set(name, params, db);

	cf_mutex_unlock(&g_set_cfg_lock);

	return result;
}

void
as_cfg_info_get_printable_cluster_name(char* cluster_name)
{
	as_config_cluster_name_get(cluster_name);

	if (cluster_name[0] == '\0') {
		strcpy(cluster_name, "null");
	}
}


//==========================================================
// Local helpers - command config-get helpers.
//

static void
cfg_get_service(cf_dyn_buf* db)
{
	// Note - no user, group.

	info_append_bool(db, "advertise-ipv6", cf_socket_advertises_ipv6());
	info_append_string(db, "auto-pin", auto_pin_string());
	info_append_uint32(db, "batch-index-threads", g_config.n_batch_index_threads);
	info_append_uint32(db, "batch-max-buffers-per-queue", g_config.batch_max_buffers_per_queue);
	info_append_uint32(db, "batch-max-requests", g_config.batch_max_requests);
	info_append_uint32(db, "batch-max-unused-buffers", g_config.batch_max_unused_buffers);

	char cluster_name[AS_CLUSTER_NAME_SZ];
	as_cfg_info_get_printable_cluster_name(cluster_name);
	info_append_string(db, "cluster-name", cluster_name);

	info_append_string(db, "debug-allocations", debug_allocations_string());
	info_append_bool(db, "disable-udf-execution", g_config.udf_execution_disabled);
	info_append_bool(db, "downgrading", g_config.downgrading);
	info_append_bool(db, "enable-benchmarks-fabric", g_config.fabric_benchmarks_enabled);
	info_append_bool(db, "enable-health-check", g_config.health_check_enabled);
	info_append_bool(db, "enable-hist-info", g_config.info_hist_enabled);
	info_append_bool(db, "enforce-best-practices", g_config.enforce_best_practices);

	for (uint32_t i = 0; i < g_config.n_feature_key_files; i++) {
		info_append_indexed_string(db, "feature-key-file", i, NULL, g_config.feature_key_files[i]);
	}

	info_append_bool(db, "indent-allocations", g_config.indent_allocations);
	info_append_uint32(db, "info-threads", g_config.n_info_threads);
	info_append_bool(db, "keep-caps-ssd-health", g_config.keep_caps_ssd_health);
	info_append_bool(db, "log-local-time", cf_log_is_using_local_time());
	info_append_bool(db, "log-millis", cf_log_is_using_millis());
	info_append_bool(db, "microsecond-histograms", g_config.microsecond_histograms);
	info_append_uint32(db, "migrate-fill-delay", g_config.migrate_fill_delay);
	info_append_uint32(db, "migrate-max-num-incoming", g_config.migrate_max_num_incoming);
	info_append_uint32(db, "migrate-threads", g_config.n_migrate_threads);
	info_append_uint32(db, "min-cluster-size", g_config.clustering_config.cluster_size_min);
	info_append_uint64_x(db, "node-id", g_config.self_node); // may be configured or auto-generated
	info_append_string_safe(db, "node-id-interface", g_config.node_id_interface);
	info_append_bool(db, "os-group-perms", cf_os_is_using_group_perms());
	info_append_string_safe(db, "pidfile", g_config.pidfile);
	info_append_int(db, "proto-fd-idle-ms", g_config.proto_fd_idle_ms);
	info_append_uint32(db, "proto-fd-max", g_config.n_proto_fd_max);
	info_append_uint32(db, "query-max-done", g_config.query_max_done);
	info_append_uint32(db, "query-threads-limit", g_config.n_query_threads_limit);
	info_append_bool(db, "run-as-daemon", g_config.run_as_daemon);
	info_append_bool(db, "salt-allocations", g_config.salt_allocations);
	info_append_uint32(db, "service-threads", g_config.n_service_threads);
	info_append_uint32(db, "sindex-builder-threads", g_config.sindex_builder_threads);
	info_append_uint32(db, "sindex-gc-period", g_config.sindex_gc_period);
	info_append_bool(db, "stay-quiesced", g_config.stay_quiesced);
	info_append_uint32(db, "ticker-interval", g_config.ticker_interval);
	info_append_int(db, "transaction-max-ms", (int)(g_config.transaction_max_ns / 1000000));
	info_append_uint32(db, "transaction-retry-ms", g_config.transaction_retry_ms);
	info_append_string_safe(db, "vault-ca", g_vault_cfg.ca);
	info_append_string_safe(db, "vault-path", g_vault_cfg.path);
	info_append_string_safe(db, "vault-token-file", g_vault_cfg.token_file);
	info_append_string_safe(db, "vault-url", g_vault_cfg.url);
	info_append_string_safe(db, "work-directory", g_config.work_directory);
}

static void
cfg_get_network(cf_dyn_buf* db)
{
	// Service:
	info_append_int(db, "service.access-port", g_config.service.std_port);
	append_addrs(db, "service.access-address", &g_config.service.std);
	append_addrs(db, "service.address", &g_config.service.bind);
	info_append_int(db, "service.alternate-access-port", g_config.service.alt_port);
	append_addrs(db, "service.alternate-access-address", &g_config.service.alt);
	info_append_int(db, "service.port", g_config.service.bind_port);

	info_append_int(db, "service.tls-port", g_config.tls_service.bind_port);
	append_addrs(db, "service.tls-address", &g_config.tls_service.bind);
	info_append_int(db, "service.tls-access-port", g_config.tls_service.std_port);
	append_addrs(db, "service.tls-access-address", &g_config.tls_service.std);
	info_append_int(db, "service.tls-alternate-access-port", g_config.tls_service.alt_port);
	append_addrs(db, "service.tls-alternate-access-address", &g_config.tls_service.alt);
	info_append_string_safe(db, "service.tls-name", g_config.tls_service.tls_our_name);

	for (uint32_t i = 0; i < g_config.tls_service.n_tls_peer_names; ++i) {
		info_append_string(db, "service.tls-authenticate-client",
				g_config.tls_service.tls_peer_names[i]);
	}

	info_append_bool(db, "service.disable-localhost", g_config.service_localhost_disabled);

	// Heartbeat:
	as_hb_info_config_get(db);

	// Fabric:
	append_addrs(db, "fabric.address", &g_config.fabric.bind);
	append_addrs(db, "fabric.tls-address", &g_config.tls_fabric.bind);
	info_append_int(db, "fabric.tls-port", g_config.tls_fabric.bind_port);
	info_append_string_safe(db, "fabric.tls-name", g_config.tls_fabric.tls_our_name);
	info_append_uint32(db, "fabric.channel-bulk-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK]);
	info_append_uint32(db, "fabric.channel-bulk-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK]);
	info_append_uint32(db, "fabric.channel-ctrl-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL]);
	info_append_uint32(db, "fabric.channel-ctrl-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL]);
	info_append_uint32(db, "fabric.channel-meta-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_META]);
	info_append_uint32(db, "fabric.channel-meta-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META]);
	info_append_uint32(db, "fabric.channel-rw-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW]);
	info_append_uint32(db, "fabric.channel-rw-recv-pools", g_config.n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW]);
	info_append_uint32(db, "fabric.channel-rw-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW]);
	info_append_bool(db, "fabric.keepalive-enabled", g_config.fabric_keepalive_enabled);
	info_append_int(db, "fabric.keepalive-intvl", g_config.fabric_keepalive_intvl);
	info_append_int(db, "fabric.keepalive-probes", g_config.fabric_keepalive_probes);
	info_append_int(db, "fabric.keepalive-time", g_config.fabric_keepalive_time);
	info_append_int(db, "fabric.latency-max-ms", g_config.fabric_latency_max_ms);
	info_append_int(db, "fabric.port", g_config.fabric.bind_port);
	info_append_int(db, "fabric.recv-rearm-threshold", g_config.fabric_recv_rearm_threshold);
	info_append_int(db, "fabric.send-threads", g_config.n_fabric_send_threads);

	// Info:
	append_addrs(db, "info.address", &g_config.info.bind);
	info_append_int(db, "info.port", g_config.info.bind_port);

	// TLS:
	for (uint32_t i = 0; i < g_config.n_tls_specs; ++i) {
		cf_tls_spec* spec = g_config.tls_specs + i;
		char key[100];

		snprintf(key, sizeof(key), "tls[%u].name", i);
		info_append_string_safe(db, key, spec->name);

		snprintf(key, sizeof(key), "tls[%u].ca-file", i);
		info_append_string_safe(db, key, spec->ca_file);

		snprintf(key, sizeof(key), "tls[%u].ca-path", i);
		info_append_string_safe(db, key, spec->ca_path);

		snprintf(key, sizeof(key), "tls[%u].cert-blacklist", i);
		info_append_string_safe(db, key, spec->cert_blacklist);

		snprintf(key, sizeof(key), "tls[%u].cert-file", i);
		info_append_string_safe(db, key, spec->cert_file);

		snprintf(key, sizeof(key), "tls[%u].cipher-suite", i);
		info_append_string_safe(db, key, spec->cipher_suite);

		snprintf(key, sizeof(key), "tls[%u].key-file", i);
		info_append_string_safe(db, key, spec->key_file);

		snprintf(key, sizeof(key), "tls[%u].key-file-password", i);
		info_append_string_safe(db, key, spec->key_file_password);

		snprintf(key, sizeof(key), "tls[%u].protocols", i);
		info_append_string_safe(db, key, spec->protocols);
	}
}

static void
cfg_get_namespace(char* context, cf_dyn_buf* db)
{
	as_namespace* ns = as_namespace_get_byname(context);

	if (ns == NULL) {
		cf_dyn_buf_append_string(db, "ERROR::namespace not found");
		return;
	}

	info_append_bool(db, "allow-ttl-without-nsup", ns->allow_ttl_without_nsup);
	info_append_uint32(db, "background-query-max-rps", ns->background_query_max_rps);

	if (ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION) {
		info_append_string(db, "conflict-resolution-policy", "generation");
	}
	else if (ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME) {
		info_append_string(db, "conflict-resolution-policy", "last-update-time");
	}
	else {
		info_append_string(db, "conflict-resolution-policy", "undefined");
	}

	info_append_bool(db, "conflict-resolve-writes", ns->conflict_resolve_writes);
	info_append_bool(db, "data-in-index", ns->data_in_index);
	info_append_uint32(db, "default-ttl", ns->default_ttl);
	info_append_bool(db, "disable-cold-start-eviction", ns->cold_start_eviction_disabled);
	info_append_bool(db, "disable-write-dup-res", ns->write_dup_res_disabled);
	info_append_bool(db, "disallow-null-setname", ns->disallow_null_setname);
	info_append_bool(db, "enable-benchmarks-batch-sub", ns->batch_sub_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-ops-sub", ns->ops_sub_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-read", ns->read_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-udf", ns->udf_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-udf-sub", ns->udf_sub_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-write", ns->write_benchmarks_enabled);
	info_append_bool(db, "enable-hist-proxy", ns->proxy_hist_enabled);
	info_append_uint32(db, "evict-hist-buckets", ns->evict_hist_buckets);
	info_append_uint32(db, "evict-tenths-pct", ns->evict_tenths_pct);
	info_append_uint32(db, "high-water-disk-pct", ns->hwm_disk_pct);
	info_append_uint32(db, "high-water-memory-pct", ns->hwm_memory_pct);
	info_append_bool(db, "ignore-migrate-fill-delay", ns->ignore_migrate_fill_delay);
	info_append_uint64(db, "index-stage-size", ns->index_stage_size);
	info_append_uint32(db, "max-record-size", ns->max_record_size);
	info_append_uint64(db, "memory-size", ns->memory_size);
	info_append_uint32(db, "migrate-order", ns->migrate_order);
	info_append_uint32(db, "migrate-retransmit-ms", ns->migrate_retransmit_ms);
	info_append_uint32(db, "migrate-sleep", ns->migrate_sleep);
	info_append_uint32(db, "nsup-hist-period", ns->nsup_hist_period);
	info_append_uint32(db, "nsup-period", ns->nsup_period);
	info_append_uint32(db, "nsup-threads", ns->n_nsup_threads);
	info_append_uint32(db, "partition-tree-sprigs", ns->tree_shared.n_sprigs);
	info_append_bool(db, "prefer-uniform-balance", ns->cfg_prefer_uniform_balance);
	info_append_uint32(db, "rack-id", ns->rack_id);
	info_append_string(db, "read-consistency-level-override", NS_READ_CONSISTENCY_LEVEL_NAME());
	info_append_bool(db, "reject-non-xdr-writes", ns->reject_non_xdr_writes);
	info_append_bool(db, "reject-xdr-writes", ns->reject_xdr_writes);
	info_append_uint32(db, "replication-factor", ns->cfg_replication_factor);
	info_append_uint64(db, "sindex-stage-size", ns->sindex_stage_size);
	info_append_bool(db, "single-bin", ns->single_bin);
	info_append_uint32(db, "single-query-threads", ns->n_single_query_threads);
	info_append_uint32(db, "stop-writes-pct", ns->stop_writes_pct);
	info_append_bool(db, "strong-consistency", ns->cp);
	info_append_bool(db, "strong-consistency-allow-expunge", ns->cp_allow_drops);
	info_append_uint32(db, "tomb-raider-eligible-age", ns->tomb_raider_eligible_age);
	info_append_uint32(db, "tomb-raider-period", ns->tomb_raider_period);
	info_append_uint32(db, "transaction-pending-limit", ns->transaction_pending_limit);
	info_append_uint32(db, "truncate-threads", ns->n_truncate_threads);
	info_append_string(db, "write-commit-level-override", NS_WRITE_COMMIT_LEVEL_NAME());
	info_append_uint64(db, "xdr-bin-tombstone-ttl", ns->xdr_bin_tombstone_ttl_ms / 1000);
	info_append_uint32(db, "xdr-tomb-raider-period", ns->xdr_tomb_raider_period);
	info_append_uint32(db, "xdr-tomb-raider-threads", ns->n_xdr_tomb_raider_threads);

	info_append_bool(db, "geo2dsphere-within.strict", ns->geo2dsphere_within_strict);
	info_append_uint32(db, "geo2dsphere-within.min-level", (uint32_t)ns->geo2dsphere_within_min_level);
	info_append_uint32(db, "geo2dsphere-within.max-level", (uint32_t)ns->geo2dsphere_within_max_level);
	info_append_uint32(db, "geo2dsphere-within.max-cells", (uint32_t)ns->geo2dsphere_within_max_cells);
	info_append_uint32(db, "geo2dsphere-within.level-mod", (uint32_t)ns->geo2dsphere_within_level_mod);
	info_append_uint32(db, "geo2dsphere-within.earth-radius-meters", ns->geo2dsphere_within_earth_radius_meters);

	info_append_string(db, "index-type",
			ns->pi_xmem_type == CF_XMEM_TYPE_MEM ? "mem" :
					(ns->pi_xmem_type == CF_XMEM_TYPE_SHMEM ? "shmem" :
							(ns->pi_xmem_type == CF_XMEM_TYPE_PMEM ? "pmem" :
									(ns->pi_xmem_type == CF_XMEM_TYPE_FLASH ? "flash" :
											"illegal"))));

	for (uint32_t i = 0; i < ns->n_pi_xmem_mounts; i++) {
		info_append_indexed_string(db, "index-type.mount", i, NULL, ns->pi_xmem_mounts[i]);
	}

	if (as_namespace_index_persisted(ns)) {
		info_append_uint32(db, "index-type.mounts-high-water-pct", ns->pi_mounts_hwm_pct);
		info_append_uint64(db, "index-type.mounts-size-limit", ns->pi_mounts_size_limit);
	}

	info_append_string(db, "sindex-type",
			ns->si_xmem_type == CF_XMEM_TYPE_MEM ? "mem" :
					(ns->si_xmem_type == CF_XMEM_TYPE_SHMEM ? "shmem" :
							(ns->si_xmem_type == CF_XMEM_TYPE_PMEM ? "pmem" :
									"illegal")));

	for (uint32_t i = 0; i < ns->n_si_xmem_mounts; i++) {
		info_append_indexed_string(db, "sindex-type.mount", i, NULL, ns->si_xmem_mounts[i]);
	}

	if (as_namespace_sindex_persisted(ns)) {
		info_append_uint32(db, "sindex-type.mounts-high-water-pct", ns->si_mounts_hwm_pct);
		info_append_uint64(db, "sindex-type.mounts-size-limit", ns->si_mounts_size_limit);
	}

	info_append_string(db, "storage-engine",
			(ns->storage_type == AS_STORAGE_ENGINE_MEMORY ? "memory" :
				(ns->storage_type == AS_STORAGE_ENGINE_PMEM ? "pmem" :
					(ns->storage_type == AS_STORAGE_ENGINE_SSD ? "device" : "illegal"))));

	if (ns->storage_type == AS_STORAGE_ENGINE_PMEM) {
		uint32_t n = as_namespace_device_count(ns);

		for (uint32_t i = 0; i < n; i++) {
			info_append_indexed_string(db, "storage-engine.file", i, NULL, ns->storage_devices[i]);

			if (ns->n_storage_shadows != 0) {
				info_append_indexed_string(db, "storage-engine.file", i, "shadow", ns->storage_shadows[i]);
			}
		}

		info_append_bool(db, "storage-engine.commit-to-device", ns->storage_commit_to_device);
		info_append_string(db, "storage-engine.compression", NS_COMPRESSION());
		info_append_uint32(db, "storage-engine.compression-level", NS_COMPRESSION_LEVEL());
		info_append_uint32(db, "storage-engine.defrag-lwm-pct", ns->storage_defrag_lwm_pct);
		info_append_uint32(db, "storage-engine.defrag-queue-min", ns->storage_defrag_queue_min);
		info_append_uint32(db, "storage-engine.defrag-sleep", ns->storage_defrag_sleep);
		info_append_uint32(db, "storage-engine.defrag-startup-minimum", ns->storage_defrag_startup_minimum);
		info_append_bool(db, "storage-engine.direct-files", ns->storage_direct_files);
		info_append_bool(db, "storage-engine.disable-odsync", ns->storage_disable_odsync);
		info_append_bool(db, "storage-engine.enable-benchmarks-storage", ns->storage_benchmarks_enabled);

		if (ns->storage_encryption_key_file != NULL) {
			info_append_string(db, "storage-engine.encryption",
				ns->storage_encryption == AS_ENCRYPTION_AES_128 ? "aes-128" :
					(ns->storage_encryption == AS_ENCRYPTION_AES_256 ? "aes-256" :
						"illegal"));
		}

		info_append_string_safe(db, "storage-engine.encryption-key-file", ns->storage_encryption_key_file);
		info_append_string_safe(db, "storage-engine.encryption-old-key-file", ns->storage_encryption_old_key_file);
		info_append_uint64(db, "storage-engine.filesize", ns->storage_filesize);
		info_append_uint64(db, "storage-engine.flush-max-ms", ns->storage_flush_max_us / 1000);
		info_append_uint64(db, "storage-engine.max-write-cache", ns->storage_max_write_cache);
		info_append_uint32(db, "storage-engine.min-avail-pct", ns->storage_min_avail_pct);
		info_append_bool(db, "storage-engine.serialize-tomb-raider", ns->storage_serialize_tomb_raider);
		info_append_uint32(db, "storage-engine.tomb-raider-sleep", ns->storage_tomb_raider_sleep);
	}
	else if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
		uint32_t n = as_namespace_device_count(ns);
		const char* tag = ns->n_storage_devices != 0 ?
				"storage-engine.device" : "storage-engine.file";

		for (uint32_t i = 0; i < n; i++) {
			info_append_indexed_string(db, tag, i, NULL, ns->storage_devices[i]);

			if (ns->n_storage_shadows != 0) {
				info_append_indexed_string(db, tag, i, "shadow", ns->storage_shadows[i]);
			}
		}

		info_append_bool(db, "storage-engine.cache-replica-writes", ns->storage_cache_replica_writes);
		info_append_bool(db, "storage-engine.cold-start-empty", ns->storage_cold_start_empty);
		info_append_bool(db, "storage-engine.commit-to-device", ns->storage_commit_to_device);
		info_append_uint32(db, "storage-engine.commit-min-size", ns->storage_commit_min_size);
		info_append_string(db, "storage-engine.compression", NS_COMPRESSION());
		info_append_uint32(db, "storage-engine.compression-level", NS_COMPRESSION_LEVEL());
		info_append_bool(db, "storage-engine.data-in-memory", ns->storage_data_in_memory);
		info_append_uint32(db, "storage-engine.defrag-lwm-pct", ns->storage_defrag_lwm_pct);
		info_append_uint32(db, "storage-engine.defrag-queue-min", ns->storage_defrag_queue_min);
		info_append_uint32(db, "storage-engine.defrag-sleep", ns->storage_defrag_sleep);
		info_append_uint32(db, "storage-engine.defrag-startup-minimum", ns->storage_defrag_startup_minimum);
		info_append_bool(db, "storage-engine.direct-files", ns->storage_direct_files);
		info_append_bool(db, "storage-engine.disable-odsync", ns->storage_disable_odsync);
		info_append_bool(db, "storage-engine.enable-benchmarks-storage", ns->storage_benchmarks_enabled);

		if (ns->storage_encryption_key_file != NULL) {
			info_append_string(db, "storage-engine.encryption",
				ns->storage_encryption == AS_ENCRYPTION_AES_128 ? "aes-128" :
					(ns->storage_encryption == AS_ENCRYPTION_AES_256 ? "aes-256" :
						"illegal"));
		}

		info_append_string_safe(db, "storage-engine.encryption-key-file", ns->storage_encryption_key_file);
		info_append_string_safe(db, "storage-engine.encryption-old-key-file", ns->storage_encryption_old_key_file);
		info_append_uint64(db, "storage-engine.filesize", ns->storage_filesize);
		info_append_uint64(db, "storage-engine.flush-max-ms", ns->storage_flush_max_us / 1000);
		info_append_uint64(db, "storage-engine.max-write-cache", ns->storage_max_write_cache);
		info_append_uint32(db, "storage-engine.min-avail-pct", ns->storage_min_avail_pct);
		info_append_uint32(db, "storage-engine.post-write-queue", ns->storage_post_write_queue);
		info_append_bool(db, "storage-engine.read-page-cache", ns->storage_read_page_cache);
		info_append_string_safe(db, "storage-engine.scheduler-mode", ns->storage_scheduler_mode);
		info_append_bool(db, "storage-engine.serialize-tomb-raider", ns->storage_serialize_tomb_raider);
		info_append_bool(db, "storage-engine.sindex-startup-device-scan", ns->storage_sindex_startup_device_scan);
		info_append_uint32(db, "storage-engine.tomb-raider-sleep", ns->storage_tomb_raider_sleep);
		info_append_uint32(db, "storage-engine.write-block-size", ns->storage_write_block_size);
	}
}


//==========================================================
// Local helpers - cfg_get_* helpers.
//

static const char*
auto_pin_string(void)
{
	switch (g_config.auto_pin) {
	case CF_TOPO_AUTO_PIN_NONE:
		return "none";
	case CF_TOPO_AUTO_PIN_CPU:
		return "cpu";
	case CF_TOPO_AUTO_PIN_NUMA:
		return "numa";
	case CF_TOPO_AUTO_PIN_ADQ:
		return "adq";
	default:
		cf_crash(CF_ALLOC, "invalid CF_TOPO_AUTO_* value");
		return NULL;
	}
}

static const char*
debug_allocations_string(void)
{
	switch (g_config.debug_allocations) {
	case CF_ALLOC_DEBUG_NONE:
		return "none";
	case CF_ALLOC_DEBUG_TRANSIENT:
		return "transient";
	case CF_ALLOC_DEBUG_PERSISTENT:
		return "persistent";
	case CF_ALLOC_DEBUG_ALL:
		return "all";
	default:
		cf_crash(CF_ALLOC, "invalid CF_ALLOC_DEBUG_* value");
		return NULL;
	}
}

static void
append_addrs(cf_dyn_buf* db, const char* name, const cf_addr_list* list)
{
	for (uint32_t i = 0; i < list->n_addrs; ++i) {
		info_append_string(db, name, list->addrs[i]);
	}
}


//==========================================================
// Local helpers - command config-set helpers.
//

static int
cfg_set(char* name, char* cmd, cf_dyn_buf* db)
{
	cf_debug(AS_INFO, "config-set command received: params %s", cmd);

	char context[1024];
	int context_len = sizeof(context);

	if (as_info_parameter_get(cmd, "context", context, &context_len) != 0) {
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	if (strcmp(context, "service") == 0) {
		if (! cfg_set_service(cmd)) {
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	else if (strcmp(context, "network") == 0) {
		if (! cfg_set_network(cmd)) {
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	else if (strcmp(context, "namespace") == 0) {
		if (! cfg_set_namespace(cmd)) {
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	else if (strcmp(context, "security") == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "security is enterprise-only");
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}

		if (! as_security_set_config(cmd)) {
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	else if (strcmp(context, "xdr") == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "XDR is enterprise-only");
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}

		if (! as_xdr_set_config(cmd)) {
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	else {
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	cf_info(AS_INFO, "config-set command completed: params %s", cmd);
	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

static bool
cfg_set_service(const char* cmd)
{
	char v[1024];
	int v_len = sizeof(v);
	int val;

	if (as_info_parameter_get(cmd, "advertise-ipv6", v, &v_len) == 0) {
		if (strcmp(v, "true") == 0 || strcmp(v, "yes") == 0) {
			cf_socket_set_advertise_ipv6(true);
		}
		else if (strcmp(v, "false") == 0 || strcmp(v, "no") == 0) {
			cf_socket_set_advertise_ipv6(false);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "batch-index-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (as_batch_threads_resize(val) != 0) {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "batch-max-buffers-per-queue", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of batch-max-buffers-per-queue from %d to %d ",
				g_config.batch_max_buffers_per_queue, val);
		g_config.batch_max_buffers_per_queue = val;
	}
	else if (as_info_parameter_get(cmd, "batch-max-requests", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of batch-max-requests from %d to %d ",
				g_config.batch_max_requests, val);
		g_config.batch_max_requests = val;
	}
	else if (as_info_parameter_get(cmd, "batch-max-unused-buffers", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of batch-max-unused-buffers from %d to %d ",
				g_config.batch_max_unused_buffers, val);
		g_config.batch_max_unused_buffers = val;
	}
	else if (as_info_parameter_get(cmd, "cluster-name", v, &v_len) == 0) {
		if (! as_config_cluster_name_set(v)) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of cluster-name to '%s'", v);
	}
	else if (as_info_parameter_get(cmd, "downgrading", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of downgrading to %s", v);
			g_config.downgrading = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of downgrading to %s", v);
			g_config.downgrading = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-fabric", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-fabric to %s",
					v);
			if (! g_config.fabric_benchmarks_enabled) {
				fabric_histogram_clear_all();
			}
			g_config.fabric_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-fabric to %s",
					v);
			g_config.fabric_benchmarks_enabled = false;
			fabric_histogram_clear_all();
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-health-check", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-health-check to %s", v);
			g_config.health_check_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-health-check to %s", v);
			g_config.health_check_enabled = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-hist-info", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-hist-info to %s", v);
			if (! g_config.info_hist_enabled) {
				histogram_clear(g_stats.info_hist);
			}
			g_config.info_hist_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-hist-info to %s", v);
			g_config.info_hist_enabled = false;
			histogram_clear(g_stats.info_hist);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "info-threads", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_INFO_THREADS) {
			cf_warning(AS_INFO, "info-threads %d must be between 1 and %u", val,
					MAX_INFO_THREADS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of info-threads from %u to %d ",
				g_config.n_info_threads, val);
		as_info_set_num_info_threads((uint32_t)val);
	}
	else if (as_info_parameter_get(cmd, "microsecond-histograms", v,
			&v_len) == 0) {
		if (any_benchmarks_enabled()) {
			cf_warning(AS_INFO, "microsecond-histograms can only be changed if all microbenchmark histograms are disabled");
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of microsecond-histograms to %s",
					v);
			g_config.microsecond_histograms = true;
		} else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of microsecond-histograms to %s",
					v);
			g_config.microsecond_histograms = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "migrate-fill-delay", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "migrate-fill-delay is enterprise-only");
			return false;
		}
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-fill-delay from %u to %u ",
				g_config.migrate_fill_delay, val);
		g_config.migrate_fill_delay = val;
	}
	else if (as_info_parameter_get(cmd, "migrate-max-num-incoming", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if ((uint32_t)val > AS_MIGRATE_LIMIT_MAX_NUM_INCOMING) {
			cf_warning(AS_INFO, "migrate-max-num-incoming %d must be >= 0 and <= %u",
					val, AS_MIGRATE_LIMIT_MAX_NUM_INCOMING);
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-max-num-incoming from %u to %d ",
				g_config.migrate_max_num_incoming, val);
		g_config.migrate_max_num_incoming = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "migrate-threads", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if ((uint32_t)val > MAX_NUM_MIGRATE_XMIT_THREADS) {
			cf_warning(AS_INFO, "migrate-threads %d must be >= 0 and <= %u",
					val, MAX_NUM_MIGRATE_XMIT_THREADS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-threads from %u to %d ",
				g_config.n_migrate_threads, val);
		as_migrate_set_num_xmit_threads(val);
	}
	else if (as_info_parameter_get(cmd, "min-cluster-size", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || (0 > val) ||
				(as_clustering_cluster_size_min_set(val) < 0)) {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "proto-fd-idle-ms", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of proto-fd-idle-ms from %d to %d ",
				g_config.proto_fd_idle_ms, val);
		g_config.proto_fd_idle_ms = val;
	}
	else if (as_info_parameter_get(cmd, "proto-fd-max", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < MIN_PROTO_FD_MAX ||
				val > MAX_PROTO_FD_MAX) {
			cf_warning(AS_INFO, "invalid proto-fd-max %d", val);
			return false;
		}
		uint32_t prev_val = g_config.n_proto_fd_max;
		if (! as_service_set_proto_fd_max((uint32_t)val)) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of proto-fd-max from %u to %d ",
				prev_val, val);
	}
	else if (as_info_parameter_get(cmd, "query-max-done", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 0 || val > 10000) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of query-max-done from %d to %d ",
				g_config.query_max_done, val);
		g_config.query_max_done = val;
		as_query_limit_finished_jobs();
	}
	else if (as_info_parameter_get(cmd, "query-threads-limit", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 1024) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of query-threads-limit from %u to %d ",
				g_config.n_query_threads_limit, val);
		g_config.n_query_threads_limit = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "service-threads", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_SERVICE_THREADS) {
			cf_warning(AS_INFO, "service-threads must be between 1 and %u",
					MAX_SERVICE_THREADS);
			return false;
		}
		uint16_t n_cpus = cf_topo_count_cpus();
		if (g_config.auto_pin != CF_TOPO_AUTO_PIN_NONE && val % n_cpus != 0) {
			cf_warning(AS_INFO, "with auto-pin, service-threads must be a multiple of the number of CPUs (%hu)",
					n_cpus);
			return false;
		}
		cf_info(AS_INFO, "Changing value of service-threads from %u to %d ",
				g_config.n_service_threads, val);
		as_service_set_threads((uint32_t)val);
	}
	else if (as_info_parameter_get(cmd, "sindex-builder-threads", v,
			&v_len) == 0) {
		int val = 0;
		if (cf_str_atoi(v, &val) != 0 || (val > 32)) {
			cf_warning(AS_INFO, "sindex-builder-threads: value must be <= 32, not %s",
					v);
			return false;
		}
		cf_info(AS_INFO, "Changing value of sindex-builder-threads from %u to %d",
				g_config.sindex_builder_threads, val);
		g_config.sindex_builder_threads = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "sindex-gc-period", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of sindex-gc-period from %d to %d ",
				g_config.sindex_gc_period, val);
		g_config.sindex_gc_period = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "ticker-interval", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of ticker-interval from %d to %d ",
				g_config.ticker_interval, val);
		g_config.ticker_interval = val;
	}
	else if (as_info_parameter_get(cmd, "transaction-max-ms", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of transaction-max-ms from %lu to %d ",
				(g_config.transaction_max_ns / 1000000), val);
		g_config.transaction_max_ns = (uint64_t)val * 1000000;
	}
	else if (as_info_parameter_get(cmd, "transaction-retry-ms", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val == 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of transaction-retry-ms from %d to %d ",
				g_config.transaction_retry_ms, val);
		g_config.transaction_retry_ms = val;
	}
	else {
		return false;
	}

	return true;
}

static bool
cfg_set_network(const char* cmd)
{
	char v[1024];
	int v_len = sizeof(v);
	int val;

	if (as_info_parameter_get(cmd, "fabric.channel-bulk-recv-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
			cf_warning(AS_INFO, "fabric.channel-bulk-recv-threads must be between 1 and %u",
					MAX_FABRIC_CHANNEL_THREADS);
			return false;
		}
		cf_info(AS_FABRIC, "changing fabric.channel-bulk-recv-threads from %u to %d",
				g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK],
				val);
		as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_BULK, val);
	}
	else if (as_info_parameter_get(cmd, "fabric.channel-ctrl-recv-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
			cf_warning(AS_INFO, "fabric.channel-ctrl-recv-threads must be between 1 and %u",
					MAX_FABRIC_CHANNEL_THREADS);
			return false;
		}
		cf_info(AS_FABRIC, "changing fabric.channel-ctrl-recv-threads from %u to %d",
				g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL],
				val);
		as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_CTRL, val);
	}
	else if (as_info_parameter_get(cmd, "fabric.channel-meta-recv-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
			cf_warning(AS_INFO, "fabric.channel-meta-recv-threads must be between 1 and %u",
					MAX_FABRIC_CHANNEL_THREADS);
			return false;
		}
		cf_info(AS_FABRIC, "changing fabric.channel-meta-recv-threads from %u to %d",
				g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META],
				val);
		as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_META, val);
	}
	else if (as_info_parameter_get(cmd, "fabric.channel-rw-recv-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
			cf_warning(AS_INFO, "fabric.channel-rw-recv-threads must be between 1 and %u",
					MAX_FABRIC_CHANNEL_THREADS);
			return false;
		}
		if (val % g_config.n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW] != 0) {
			cf_warning(AS_INFO, "'fabric.channel-rw-recv-threads' must be a multiple of 'fabric.channel-rw-recv-pools'");
			return false;
		}
		cf_info(AS_FABRIC, "changing fabric.channel-rw-recv-threads from %u to %d",
				g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW],
				val);
		as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_RW, val);
	}
	else if (as_info_parameter_get(cmd, "fabric.recv-rearm-threshold", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val < 0 || val > 1024 * 1024) {
			return false;
		}
		g_config.fabric_recv_rearm_threshold = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "heartbeat.connect-timeout-ms", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (as_hb_connect_timeout_set(val) != 0) {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "heartbeat.interval", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (as_hb_tx_interval_set(val) != 0) {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "heartbeat.mtu", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		as_hb_override_mtu_set(val);
	}
	else if (as_info_parameter_get(cmd, "heartbeat.protocol", v, &v_len) == 0) {
		as_hb_protocol protocol =
				(! strcmp(v, "v3") ? AS_HB_PROTOCOL_V3 :
						(! strcmp(v, "reset") ? AS_HB_PROTOCOL_RESET :
								(! strcmp(v, "none") ? AS_HB_PROTOCOL_NONE :
										AS_HB_PROTOCOL_UNDEF)));
		if (AS_HB_PROTOCOL_UNDEF == protocol) {
			cf_warning(AS_INFO, "heartbeat protocol version %s not supported",
					v);
			return false;
		}
		cf_info(AS_INFO, "Changing value of heartbeat protocol version to %s",
				v);
		if (0 > as_hb_protocol_set(protocol)) {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "heartbeat.timeout", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (as_hb_max_intervals_missed_set(val) != 0) {
			return false;
		}
	}
	else {
		return false;
	}

	return true;
}

static bool
cfg_set_namespace(const char* cmd)
{
	char v[1024];
	int v_len = sizeof(v);
	int val;
	char bool_val[2][6] = {"false", "true"};

	if (as_info_parameter_get(cmd, "id", v, &v_len) != 0) {
		return false;
	}

	as_namespace* ns = as_namespace_get_byname(v);

	if (ns == NULL) {
		return false;
	}

	v_len = sizeof(v);

	if (as_info_parameter_get(cmd, "set", v, &v_len) == 0) {
		return cfg_set_set(cmd, ns, v, (size_t)v_len);
	}
	else if (as_info_parameter_get(cmd, "allow-ttl-without-nsup", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of allow-ttl-without-nsup of ns %s from %s to %s",
					ns->name, bool_val[ns->allow_ttl_without_nsup], v);
			ns->allow_ttl_without_nsup = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of allow-ttl-without-nsup of ns %s from %s to %s",
					ns->name, bool_val[ns->allow_ttl_without_nsup], v);
			ns->allow_ttl_without_nsup = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "background-query-max-rps", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 1000000) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of background-query-max-rps of ns %s from %u to %d ",
				ns->name, ns->background_query_max_rps, val);
		ns->background_query_max_rps = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "conflict-resolution-policy", v,
			&v_len) == 0) {
		if (ns->cp) {
			cf_warning(AS_INFO, "{%s} 'conflict-resolution-policy' is not applicable with 'strong-consistency'",
					ns->name);
			return false;
		}
		if (strncmp(v, "generation", 10) == 0) {
			cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s",
					ns->name, ns->conflict_resolution_policy, v);
			ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
		}
		else if (strncmp(v, "last-update-time", 16) == 0) {
			cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s",
					ns->name, ns->conflict_resolution_policy, v);
			ns->conflict_resolution_policy =
					AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "conflict-resolve-writes", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "conflict-resolve-writes is enterprise-only");
			return false;
		}
		if (ns->single_bin) {
			cf_warning(AS_INFO, "conflict-resolve-writes can't be set for single-bin namespace");
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of conflict-resolve-writes of ns %s to %s",
					ns->name, v);
			ns->conflict_resolve_writes = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of conflict-resolve-writes of ns %s to %s",
					ns->name, v);
			ns->conflict_resolve_writes = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "default-ttl", v, &v_len) == 0) {
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			cf_warning(AS_INFO, "default-ttl must be an unsigned number with time unit (s, m, h, or d)");
			return false;
		}
		if (val > MAX_ALLOWED_TTL) {
			cf_warning(AS_INFO, "default-ttl must be <= %u seconds",
					MAX_ALLOWED_TTL);
			return false;
		}
		if (val != 0 && ns->nsup_period == 0 && ! ns->allow_ttl_without_nsup) {
			cf_warning(AS_INFO, "must configure non-zero nsup-period or allow-ttl-without-nsup true to set non-zero default-ttl");
			return false;
		}
		cf_info(AS_INFO, "Changing value of default-ttl of ns %s from %u to %u",
				ns->name, ns->default_ttl, val);
		ns->default_ttl = val;
	}
	else if (as_info_parameter_get(cmd, "disable-write-dup-res", v,
			&v_len) == 0) {
		if (ns->cp) {
			cf_warning(AS_INFO, "{%s} 'disable-write-dup-res' is not applicable with 'strong-consistency'",
					ns->name);
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of disable-write-dup-res of ns %s from %s to %s",
					ns->name, bool_val[ns->write_dup_res_disabled], v);
			ns->write_dup_res_disabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of disable-write-dup-res of ns %s from %s to %s",
					ns->name, bool_val[ns->write_dup_res_disabled], v);
			ns->write_dup_res_disabled = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "disallow-null-setname", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s",
					ns->name, bool_val[ns->disallow_null_setname], v);
			ns->disallow_null_setname = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s",
					ns->name, bool_val[ns->disallow_null_setname], v);
			ns->disallow_null_setname = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-batch-sub", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-batch-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->batch_sub_benchmarks_enabled], v);
			if (! ns->batch_sub_benchmarks_enabled) {
				batch_sub_benchmarks_histogram_clear_all(ns);
			}
			ns->batch_sub_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-batch-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->batch_sub_benchmarks_enabled], v);
			ns->batch_sub_benchmarks_enabled = false;
			batch_sub_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-ops-sub", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-ops-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->ops_sub_benchmarks_enabled], v);
			if (! ns->ops_sub_benchmarks_enabled) {
				ops_sub_benchmarks_histogram_clear_all(ns);
			}
			ns->ops_sub_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-ops-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->ops_sub_benchmarks_enabled], v);
			ns->ops_sub_benchmarks_enabled = false;
			ops_sub_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-read", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-read of ns %s from %s to %s",
					ns->name, bool_val[ns->read_benchmarks_enabled], v);
			if (! ns->read_benchmarks_enabled) {
				read_benchmarks_histogram_clear_all(ns);
			}
			ns->read_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-read of ns %s from %s to %s",
					ns->name, bool_val[ns->read_benchmarks_enabled], v);
			ns->read_benchmarks_enabled = false;
			read_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-udf", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-udf of ns %s from %s to %s",
					ns->name, bool_val[ns->udf_benchmarks_enabled], v);
			if (! ns->udf_benchmarks_enabled) {
				udf_benchmarks_histogram_clear_all(ns);
			}
			ns->udf_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-udf of ns %s from %s to %s",
					ns->name, bool_val[ns->udf_benchmarks_enabled], v);
			ns->udf_benchmarks_enabled = false;
			udf_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-udf-sub", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-udf-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->udf_sub_benchmarks_enabled], v);
			if (! ns->udf_sub_benchmarks_enabled) {
				udf_sub_benchmarks_histogram_clear_all(ns);
			}
			ns->udf_sub_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-udf-sub of ns %s from %s to %s",
					ns->name, bool_val[ns->udf_sub_benchmarks_enabled], v);
			ns->udf_sub_benchmarks_enabled = false;
			udf_sub_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-write", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-write of ns %s from %s to %s",
					ns->name, bool_val[ns->write_benchmarks_enabled], v);
			if (! ns->write_benchmarks_enabled) {
				write_benchmarks_histogram_clear_all(ns);
			}
			ns->write_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-write of ns %s from %s to %s",
					ns->name, bool_val[ns->write_benchmarks_enabled], v);
			ns->write_benchmarks_enabled = false;
			write_benchmarks_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-hist-proxy", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-hist-proxy of ns %s from %s to %s",
					ns->name, bool_val[ns->proxy_hist_enabled], v);
			if (! ns->proxy_hist_enabled) {
				histogram_clear(ns->proxy_hist);
			}
			ns->proxy_hist_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-hist-proxy of ns %s from %s to %s",
					ns->name, bool_val[ns->proxy_hist_enabled], v);
			ns->proxy_hist_enabled = false;
			histogram_clear(ns->proxy_hist);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "evict-hist-buckets", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 100 || val > 10000000) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of evict-hist-buckets of ns %s from %u to %d ",
				ns->name, ns->evict_hist_buckets, val);
		ns->evict_hist_buckets = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "evict-tenths-pct", v, &v_len) == 0) {
		cf_info(AS_INFO, "Changing value of evict-tenths-pct memory of ns %s from %d to %d ",
				ns->name, ns->evict_tenths_pct, atoi(v));
		ns->evict_tenths_pct = atoi(v);
	}
	else if (as_info_parameter_get(cmd, "high-water-disk-pct", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 0 || val > 100) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of high-water-disk-pct of ns %s from %u to %d ",
				ns->name, ns->hwm_disk_pct, val);
		ns->hwm_disk_pct = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "high-water-memory-pct", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 0 || val > 100) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of high-water-memory-pct memory of ns %s from %u to %d ",
				ns->name, ns->hwm_memory_pct, val);
		ns->hwm_memory_pct = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "ignore-migrate-fill-delay", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "ignore-migrate-fill-delay is enterprise-only");
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of ignore-migrate-fill-delay of ns %s to %s",
					ns->name, v);
			ns->ignore_migrate_fill_delay = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of ignore-migrate-fill-delay of ns %s to %s",
					ns->name, v);
			ns->ignore_migrate_fill_delay = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "max-record-size", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 0) {
			return false;
		}
		if (val != 0) {
			if (ns->storage_type == AS_STORAGE_ENGINE_MEMORY &&
					val > 128 * 1024 * 1024) { // PROTO_SIZE_MAX
				cf_warning(AS_INFO, "max-record-size can't be bigger than 128M");
				return false;
			}
			if (ns->storage_type == AS_STORAGE_ENGINE_PMEM &&
					val > 8 * 1024 * 1024) { // PMEM_WRITE_BLOCK_SIZE
				cf_warning(AS_INFO, "max-record-size can't be bigger than 8M");
				return false;
			}
			if (ns->storage_type == AS_STORAGE_ENGINE_SSD &&
					val > ns->storage_write_block_size) {
				cf_warning(AS_INFO, "max-record-size can't be bigger than write-block-size");
				return false;
			}
		}
		cf_info(AS_INFO, "Changing value of max-record-size of ns %s from %u to %d",
				ns->name, ns->max_record_size, val);
		ns->max_record_size = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "memory-size", v, &v_len) == 0) {
		uint64_t val;
		if (cf_str_atoi_u64(v, &val) != 0) {
			return false;
		}
		cf_debug(AS_INFO, "memory-size = %lu", val);
		if (val > ns->memory_size)
			ns->memory_size = val;
		if (val < (ns->memory_size / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
			return false;
		}
		cf_info(AS_INFO, "Changing value of memory-size of ns %s from %lu to %lu",
				ns->name, ns->memory_size, val);
		ns->memory_size = val;
	}
	else if (as_info_parameter_get(cmd, "migrate-order", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 10) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-order of ns %s from %u to %d",
				ns->name, ns->migrate_order, val);
		ns->migrate_order = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "migrate-retransmit-ms", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-retransmit-ms of ns %s from %u to %d",
				ns->name, ns->migrate_retransmit_ms, val);
		ns->migrate_retransmit_ms = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "migrate-sleep", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of migrate-sleep of ns %s from %u to %d",
				ns->name, ns->migrate_sleep, val);
		ns->migrate_sleep = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "mounts-high-water-pct", v,
			&v_len) == 0) {
		if (! as_namespace_index_persisted(ns)) {
			cf_warning(AS_INFO, "mounts-high-water-pct is not relevant for this index-type");
			return false;
		}
		if (cf_str_atoi(v, &val) != 0 || val < 0 || val > 100) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of mounts-high-water-pct of ns %s from %u to %d ",
				ns->name, ns->pi_mounts_hwm_pct, val);
		ns->pi_mounts_hwm_pct = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "mounts-size-limit", v, &v_len) == 0) {
		if (! as_namespace_index_persisted(ns)) {
			cf_warning(AS_INFO, "mounts-size-limit is not relevant for this index-type");
			return false;
		}
		uint64_t val;
		uint64_t min = (ns->pi_xmem_type == CF_XMEM_TYPE_FLASH ? 4 : 1) *
				1024UL * 1024UL * 1024UL;
		if (cf_str_atoi_u64(v, &val) != 0 || val < min) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of mounts-size-limit of ns %s from %lu to %lu",
				ns->name, ns->pi_mounts_size_limit, val);
		ns->pi_mounts_size_limit = val;
	}
	else if (as_info_parameter_get(cmd, "nsup-hist-period", v, &v_len) == 0) {
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of nsup-hist-period of ns %s from %u to %u",
				ns->name, ns->nsup_hist_period, val);
		ns->nsup_hist_period = val;
	}
	else if (as_info_parameter_get(cmd, "nsup-period", v, &v_len) == 0) {
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of nsup-period of ns %s from %u to %u",
				ns->name, ns->nsup_period, val);
		ns->nsup_period = val;
	}
	else if (as_info_parameter_get(cmd, "nsup-threads", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 128) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of nsup-threads of ns %s from %u to %d",
				ns->name, ns->n_nsup_threads, val);
		ns->n_nsup_threads = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "prefer-uniform-balance", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "prefer-uniform-balance is enterprise-only");
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of prefer-uniform-balance of ns %s from %s to %s",
					ns->name, bool_val[ns->cfg_prefer_uniform_balance], v);
			ns->cfg_prefer_uniform_balance = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of prefer-uniform-balance of ns %s from %s to %s",
					ns->name, bool_val[ns->cfg_prefer_uniform_balance], v);
			ns->cfg_prefer_uniform_balance = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "rack-id", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "rack-id is enterprise-only");
			return false;
		}
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if ((uint32_t)val > MAX_RACK_ID) {
			cf_warning(AS_INFO, "rack-id %d must be >= 0 and <= %u",
					val, MAX_RACK_ID);
			return false;
		}
		cf_info(AS_INFO, "Changing value of rack-id of ns %s from %u to %d",
				ns->name, ns->rack_id, val);
		ns->rack_id = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "read-consistency-level-override", v,
			&v_len) == 0) {
		if (ns->cp) {
			cf_warning(AS_INFO, "{%s} 'read-consistency-level-override' is not applicable with 'strong-consistency'",
					ns->name);
			return false;
		}
		char* original_value = NS_READ_CONSISTENCY_LEVEL_NAME();
		if (strcmp(v, "all") == 0) {
			ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ALL;
		}
		else if (strcmp(v, "off") == 0) {
			ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_PROTO;
		}
		else if (strcmp(v, "one") == 0) {
			ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ONE;
		}
		else {
			return false;
		}
		if (strcmp(original_value, v)) {
			cf_info(AS_INFO, "Changing value of read-consistency-level-override of ns %s from %s to %s",
					ns->name, original_value, v);
		}
	}
	else if (as_info_parameter_get(cmd, "reject-non-xdr-writes", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of reject-non-xdr-writes of ns %s from %s to %s",
					ns->name, bool_val[ns->reject_non_xdr_writes], v);
			ns->reject_non_xdr_writes = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of reject-non-xdr-writes of ns %s from %s to %s",
					ns->name, bool_val[ns->reject_non_xdr_writes], v);
			ns->reject_non_xdr_writes = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "reject-xdr-writes", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of reject-xdr-writes of ns %s from %s to %s",
					ns->name, bool_val[ns->reject_xdr_writes], v);
			ns->reject_xdr_writes = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of reject-xdr-writes of ns %s from %s to %s",
					ns->name, bool_val[ns->reject_xdr_writes], v);
			ns->reject_xdr_writes = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "replication-factor", v, &v_len) == 0) {
		if (ns->cp) {
			cf_warning(AS_INFO, "{%s} 'replication-factor' is not yet dynamic with 'strong-consistency'",
					ns->name);
			return false;
		}
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > AS_CLUSTER_SZ) {
			cf_warning(AS_INFO, "replication-factor must be between 1 and %u",
					AS_CLUSTER_SZ);
			return false;
		}
		cf_info(AS_INFO, "Changing value of replication-factor of ns %s from %u to %d",
				ns->name, ns->cfg_replication_factor, val);
		ns->cfg_replication_factor = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "single-query-threads", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 128) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of single-query-threads of ns %s from %u to %d ",
				ns->name, ns->n_single_query_threads, val);
		ns->n_single_query_threads = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "stop-writes-pct", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0 || val < 0 || val > 100) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of stop-writes-pct memory of ns %s from %u to %d ",
				ns->name, ns->stop_writes_pct, val);
		ns->stop_writes_pct = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "strong-consistency-allow-expunge", v,
			&v_len) == 0) {
		if (! ns->cp) {
			cf_warning(AS_INFO, "{%s} 'strong-consistency-allow-expunge' is only applicable with 'strong-consistency'",
					ns->name);
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of strong-consistency-allow-expunge of ns %s from %s to %s",
					ns->name, bool_val[ns->cp_allow_drops], v);
			ns->cp_allow_drops = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of strong-consistency-allow-expunge of ns %s from %s to %s",
					ns->name, bool_val[ns->cp_allow_drops], v);
			ns->cp_allow_drops = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "tomb-raider-eligible-age", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "tomb-raider-eligible-age is enterprise-only");
			return false;
		}
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			cf_warning(AS_INFO, "tomb-raider-eligible-age must be an unsigned number with time unit (s, m, h, or d)");
			return false;
		}
		cf_info(AS_INFO, "Changing value of tomb-raider-eligible-age of ns %s from %u to %u",
				ns->name, ns->tomb_raider_eligible_age, val);
		ns->tomb_raider_eligible_age = val;
	}
	else if (as_info_parameter_get(cmd, "tomb-raider-period", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "tomb-raider-period is enterprise-only");
			return false;
		}
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			cf_warning(AS_INFO, "tomb-raider-period must be an unsigned number with time unit (s, m, h, or d)");
			return false;
		}
		cf_info(AS_INFO, "Changing value of tomb-raider-period of ns %s from %u to %u",
				ns->name, ns->tomb_raider_period, val);
		ns->tomb_raider_period = val;
	}
	else if (as_info_parameter_get(cmd, "transaction-pending-limit", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of transaction-pending-limit of ns %s from %d to %d ",
				ns->name, ns->transaction_pending_limit, val);
		ns->transaction_pending_limit = val;
	}
	else if (as_info_parameter_get(cmd, "truncate-threads", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		if (val > MAX_TRUNCATE_THREADS || val < 1) {
			cf_warning(AS_INFO, "truncate-threads %d must be >= 1 and <= %u",
					val, MAX_TRUNCATE_THREADS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of truncate-threads of ns %s from %u to %d ",
				ns->name, ns->n_truncate_threads, val);
		ns->n_truncate_threads = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "write-commit-level-override", v,
			&v_len) == 0) {
		if (ns->cp) {
			cf_warning(AS_INFO, "{%s} 'write-commit-level-override' is not applicable with 'strong-consistency'",
					ns->name);
			return false;
		}
		char* original_value = NS_WRITE_COMMIT_LEVEL_NAME();
		if (strcmp(v, "all") == 0) {
			ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_ALL;
		}
		else if (strcmp(v, "master") == 0) {
			ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_MASTER;
		}
		else if (strcmp(v, "off") == 0) {
			ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_PROTO;
		}
		else {
			return false;
		}
		if (strcmp(original_value, v)) {
			cf_info(AS_INFO, "Changing value of write-commit-level-override of ns %s from %s to %s",
					ns->name, original_value, v);
		}
	}
	else if (as_info_parameter_get(cmd, "xdr-bin-tombstone-ttl", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "xdr-bin-tombstone-ttl is enterprise-only");
			return false;
		}
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			cf_warning(AS_INFO, "xdr-bin-tombstone-ttl must be an unsigned number with time unit (s, m, h, or d)");
			return false;
		}
		if (val > MAX_ALLOWED_TTL) {
			cf_warning(AS_INFO, "xdr-bin-tombstone-ttl must be <= %u seconds",
					MAX_ALLOWED_TTL);
			return false;
		}
		cf_info(AS_INFO, "Changing value of xdr-bin-tombstone-ttl of ns %s from %lu to %u",
				ns->name, ns->xdr_bin_tombstone_ttl_ms / 1000, val);
		ns->xdr_bin_tombstone_ttl_ms = val * 1000UL;
	}
	else if (as_info_parameter_get(cmd, "xdr-tomb-raider-period", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "xdr-tomb-raider-period is enterprise-only");
			return false;
		}
		uint32_t val;
		if (cf_str_atoi_seconds(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of xdr-tomb-raider-period of ns %s from %u to %u",
				ns->name, ns->xdr_tomb_raider_period, val);
		ns->xdr_tomb_raider_period = val;
	}
	else if (as_info_parameter_get(cmd, "xdr-tomb-raider-threads", v,
			&v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "xdr-tomb-raider-threads is enterprise-only");
			return false;
		}
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 128) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of xdr-tomb-raider-threads of ns %s from %u to %d",
				ns->name, ns->n_xdr_tomb_raider_threads, val);
		ns->n_xdr_tomb_raider_threads = (uint32_t)val;
	}

	//------------------------------------------------------
	// storage-engine is sub-context in cfg file:
	//

	else if (as_info_parameter_get(cmd, "cache-replica-writes", v,
			&v_len) == 0) {
		if (ns->storage_data_in_memory) {
			cf_warning(AS_INFO, "ns %s, can't set cache-replica-writes if data-in-memory",
					ns->name);
			return false;
		}
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of cache-replica-writes of ns %s to %s",
					ns->name, v);
			ns->storage_cache_replica_writes = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of cache-replica-writes of ns %s to %s",
					ns->name, v);
			ns->storage_cache_replica_writes = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "compression", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "compression is enterprise-only");
			return false;
		}
		if (as_config_error_enterprise_feature_only("compression")) {
			cf_warning(AS_INFO, "{%s} feature key does not allow compression",
					ns->name);
			return false;
		}
		if (ns->storage_type == AS_STORAGE_ENGINE_MEMORY) {
			// Note - harmful to configure compression for memory-only!
			cf_warning(AS_INFO, "{%s} compression is not available for storage-engine memory",
					ns->name);
			return false;
		}
		const char* orig = NS_COMPRESSION();
		if (strcmp(v, "none") == 0) {
			ns->storage_compression = AS_COMPRESSION_NONE;
		}
		else if (strcmp(v, "lz4") == 0) {
			ns->storage_compression = AS_COMPRESSION_LZ4;
		}
		else if (strcmp(v, "snappy") == 0) {
			ns->storage_compression = AS_COMPRESSION_SNAPPY;
		}
		else if (strcmp(v, "zstd") == 0) {
			ns->storage_compression = AS_COMPRESSION_ZSTD;
		}
		else {
			return false;
		}
		cf_info(AS_INFO, "Changing value of compression of ns %s from %s to %s",
				ns->name, orig, v);
	}
	else if (as_info_parameter_get(cmd, "compression-level", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "compression-level is enterprise-only");
			return false;
		}
		if (cf_str_atoi(v, &val) != 0 || val < 1 || val > 9) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of compression-level of ns %s from %u to %d",
				ns->name, ns->storage_compression_level, val);
		ns->storage_compression_level = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "defrag-lwm-pct", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of defrag-lwm-pct of ns %s from %d to %d ",
				ns->name, ns->storage_defrag_lwm_pct, val);
		uint32_t old_val = ns->storage_defrag_lwm_pct;
		ns->storage_defrag_lwm_pct = val;
		ns->defrag_lwm_size = (ns->storage_write_block_size *
				ns->storage_defrag_lwm_pct) / 100;
		if (ns->storage_defrag_lwm_pct > old_val) {
			as_storage_defrag_sweep(ns);
		}
	}
	else if (as_info_parameter_get(cmd, "defrag-queue-min", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of defrag-queue-min of ns %s from %u to %d",
				ns->name, ns->storage_defrag_queue_min, val);
		ns->storage_defrag_queue_min = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "defrag-sleep", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of defrag-sleep of ns %s from %u to %d",
				ns->name, ns->storage_defrag_sleep, val);
		ns->storage_defrag_sleep = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "enable-benchmarks-storage", v,
			&v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-storage of ns %s from %s to %s",
					ns->name, bool_val[ns->storage_benchmarks_enabled], v);
			if (! ns->storage_benchmarks_enabled) {
				as_storage_histogram_clear_all(ns);
			}
			ns->storage_benchmarks_enabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-benchmarks-storage of ns %s from %s to %s",
					ns->name, bool_val[ns->storage_benchmarks_enabled], v);
			ns->storage_benchmarks_enabled = false;
			as_storage_histogram_clear_all(ns);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "flush-max-ms", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of flush-max-ms of ns %s from %lu to %d",
				ns->name, ns->storage_flush_max_us / 1000, val);
		ns->storage_flush_max_us = (uint64_t)val * 1000;
	}
	else if (as_info_parameter_get(cmd, "max-write-cache", v, &v_len) == 0) {
		uint64_t val_u64;
		if (cf_str_atoi_u64(v, &val_u64) != 0) {
			return false;
		}
		if (val_u64 < DEFAULT_MAX_WRITE_CACHE) {
			cf_warning(AS_INFO, "can't set max-write-cache < %luM",
					DEFAULT_MAX_WRITE_CACHE / (1024 * 1024));
			return false;
		}
		cf_info(AS_INFO, "Changing value of max-write-cache of ns %s from %lu to %lu ",
				ns->name, ns->storage_max_write_cache, val_u64);
		ns->storage_max_write_cache = val_u64;
		ns->storage_max_write_q = (uint32_t)(as_namespace_device_count(ns) *
				ns->storage_max_write_cache / ns->storage_write_block_size);
	}
	else if (as_info_parameter_get(cmd, "min-avail-pct", v, &v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			cf_warning(AS_INFO, "ns %s, min-avail-pct %s is not a number",
					ns->name, v);
			return false;
		}
		if (val > 100 || val < 0) {
			cf_warning(AS_INFO, "ns %s, min-avail-pct %d must be between 0 and 100",
					ns->name, val);
			return false;
		}
		cf_info(AS_INFO, "Changing value of min-avail-pct of ns %s from %u to %d ",
				ns->name, ns->storage_min_avail_pct, val);
		ns->storage_min_avail_pct = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "post-write-queue", v, &v_len) == 0) {
		if (ns->storage_data_in_memory) {
			cf_warning(AS_INFO, "ns %s, can't set post-write-queue if data-in-memory",
					ns->name);
			return false;
		}
		if (cf_str_atoi(v, &val) != 0) {
			cf_warning(AS_INFO, "ns %s, post-write-queue %s is not a number",
					ns->name, v);
			return false;
		}
		if ((uint32_t)val > MAX_POST_WRITE_QUEUE) {
			cf_warning(AS_INFO, "ns %s, post-write-queue %u must be < %u",
					ns->name, val, MAX_POST_WRITE_QUEUE);
			return false;
		}
		cf_info(AS_INFO, "Changing value of post-write-queue of ns %s from %d to %d ",
				ns->name, ns->storage_post_write_queue, val);
		ns->storage_post_write_queue = (uint32_t)val;
	}
	else if (as_info_parameter_get(cmd, "read-page-cache", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of read-page-cache of ns %s from %s to %s",
					ns->name, bool_val[ns->storage_read_page_cache], v);
			ns->storage_read_page_cache = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of read-page-cache of ns %s from %s to %s",
					ns->name, bool_val[ns->storage_read_page_cache], v);
			ns->storage_read_page_cache = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "tomb-raider-sleep", v, &v_len) == 0) {
		if (as_config_error_enterprise_only()) {
			cf_warning(AS_INFO, "tomb-raider-sleep is enterprise-only");
			return false;
		}
		if (cf_str_atoi(v, &val) != 0) {
			return false;
		}
		cf_info(AS_INFO, "Changing value of tomb-raider-sleep of ns %s from %u to %d",
				ns->name, ns->storage_tomb_raider_sleep, val);
		ns->storage_tomb_raider_sleep = (uint32_t)val;
	}

	//------------------------------------------------------
	// geo2dsphere-within is sub-context in cfg file:
	//

	else if (as_info_parameter_get(cmd, "geo2dsphere-within-max-cells", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-cells %s is not a number",
					ns->name, v);
			return false;
		}
		if (val < 1 || val > MAX_REGION_CELLS) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-cells %d must be between %u and %u",
					ns->name, val, 1, MAX_REGION_CELLS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of geo2dsphere-within-max-cells of ns %s from %u to %d ",
				ns->name, ns->geo2dsphere_within_max_cells, val);
		ns->geo2dsphere_within_max_cells = val;
	}
	else if (as_info_parameter_get(cmd, "geo2dsphere-within-max-level", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-level %s is not a number",
					ns->name, v);
			return false;
		}
		if (val < 0 || val > MAX_REGION_LEVELS) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-level %d must be between %u and %u",
					ns->name, val, 0, MAX_REGION_LEVELS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of geo2dsphere-within-max-level of ns %s from %u to %d ",
				ns->name, ns->geo2dsphere_within_max_level, val);
		ns->geo2dsphere_within_max_level = val;
	}
	else if (as_info_parameter_get(cmd, "geo2dsphere-within-min-level", v,
			&v_len) == 0) {
		if (cf_str_atoi(v, &val) != 0) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-min-level %s is not a number",
					ns->name, v);
			return false;
		}
		if (val < 0 || val > MAX_REGION_LEVELS) {
			cf_warning(AS_INFO, "ns %s, geo2dsphere-within-min-level %d must be between %u and %u",
					ns->name, val, 0, MAX_REGION_LEVELS);
			return false;
		}
		cf_info(AS_INFO, "Changing value of geo2dsphere-within-min-level of ns %s from %u to %d ",
				ns->name, ns->geo2dsphere_within_min_level, val);
		ns->geo2dsphere_within_min_level = val;
	}
	else {
		return false;
	}

	return true;
}

static bool
cfg_set_set(const char* cmd, as_namespace* ns, const char* set_name,
		size_t set_name_len)
{
	if (set_name_len == 0 || set_name_len >= AS_SET_NAME_MAX_SIZE) {
		cf_warning(AS_INFO, "illegal length %zu for set name %s", set_name_len,
				set_name);
		return false;
	}

	// Valid configurations should create set if it doesn't exist. Checks if
	// there is a vmap set with the same name and if so returns a ptr to it. If
	// not, it creates an set structure, initializes it and returns a ptr to it.
	as_set* p_set = NULL;
	uint16_t set_id;

	if (as_namespace_get_create_set_w_len(ns, set_name, set_name_len, &p_set,
			&set_id) != 0) {
		return false;
	}

	char v[1024];
	int v_len = sizeof(v);

	if (as_info_parameter_get(cmd, "disable-eviction", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of disable-eviction of ns %s set %s to %s",
					ns->name, p_set->name, v);
			p_set->eviction_disabled = true;
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of disable-eviction of ns %s set %s to %s",
					ns->name, p_set->name, v);
			p_set->eviction_disabled = false;
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "enable-index", v, &v_len) == 0) {
		if (strncmp(v, "true", 4) == 0 || strncmp(v, "yes", 3) == 0) {
			cf_info(AS_INFO, "Changing value of enable-index of ns %s set %s to %s",
					ns->name, p_set->name, v);
			as_set_index_enable(ns, p_set, set_id);
		}
		else if (strncmp(v, "false", 5) == 0 || strncmp(v, "no", 2) == 0) {
			cf_info(AS_INFO, "Changing value of enable-index of ns %s set %s to %s",
					ns->name, p_set->name, v);
			as_set_index_disable(ns, p_set, set_id);
		}
		else {
			return false;
		}
	}
	else if (as_info_parameter_get(cmd, "stop-writes-count", v, &v_len) == 0) {
		uint64_t val = atoll(v);
		cf_info(AS_INFO, "Changing value of stop-writes-count of ns %s set %s to %lu",
				ns->name, p_set->name, val);
		p_set->stop_writes_count = val;
	}
	else {
		return false;
	}

	return true;
}


//==========================================================
// Local helpers - histogram helpers.
//

static void
fabric_histogram_clear_all(void)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(
			g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_BULK], scale);
	histogram_rescale(
			g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_BULK], scale);
	histogram_rescale(
			g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_BULK], scale);
	histogram_rescale(
			g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_BULK], scale);
	histogram_rescale(
			g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_CTRL], scale);
	histogram_rescale(
			g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_CTRL], scale);
	histogram_rescale(
			g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_CTRL], scale);
	histogram_rescale(
			g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_CTRL], scale);
	histogram_rescale(
			g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_META], scale);
	histogram_rescale(
			g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_META], scale);
	histogram_rescale(
			g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_META], scale);
	histogram_rescale(
			g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_META], scale);
	histogram_rescale(
			g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_RW], scale);
	histogram_rescale(
			g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_RW], scale);
	histogram_rescale(
			g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_RW], scale);
	histogram_rescale(
			g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_RW], scale);
}

static void
read_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->read_start_hist, scale);
	histogram_rescale(ns->read_restart_hist, scale);
	histogram_rescale(ns->read_dup_res_hist, scale);
	histogram_rescale(ns->read_repl_ping_hist, scale);
	histogram_rescale(ns->read_local_hist, scale);
	histogram_rescale(ns->read_response_hist, scale);
}

static void
write_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->write_start_hist, scale);
	histogram_rescale(ns->write_restart_hist, scale);
	histogram_rescale(ns->write_dup_res_hist, scale);
	histogram_rescale(ns->write_master_hist, scale);
	histogram_rescale(ns->write_repl_write_hist, scale);
	histogram_rescale(ns->write_response_hist, scale);
}

static void
udf_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->udf_start_hist, scale);
	histogram_rescale(ns->udf_restart_hist, scale);
	histogram_rescale(ns->udf_dup_res_hist, scale);
	histogram_rescale(ns->udf_master_hist, scale);
	histogram_rescale(ns->udf_repl_write_hist, scale);
	histogram_rescale(ns->udf_response_hist, scale);
}

static void
batch_sub_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->batch_sub_prestart_hist, scale);
	histogram_rescale(ns->batch_sub_start_hist, scale);
	histogram_rescale(ns->batch_sub_restart_hist, scale);
	histogram_rescale(ns->batch_sub_dup_res_hist, scale);
	histogram_rescale(ns->batch_sub_repl_ping_hist, scale);
	histogram_rescale(ns->batch_sub_read_local_hist, scale);
	histogram_rescale(ns->batch_sub_write_master_hist, scale);
	histogram_rescale(ns->batch_sub_udf_master_hist, scale);
	histogram_rescale(ns->batch_sub_repl_write_hist, scale);
	histogram_rescale(ns->batch_sub_response_hist, scale);
}

static void
udf_sub_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->udf_sub_start_hist, scale);
	histogram_rescale(ns->udf_sub_restart_hist, scale);
	histogram_rescale(ns->udf_sub_dup_res_hist, scale);
	histogram_rescale(ns->udf_sub_master_hist, scale);
	histogram_rescale(ns->udf_sub_repl_write_hist, scale);
	histogram_rescale(ns->udf_sub_response_hist, scale);
}

static void
ops_sub_benchmarks_histogram_clear_all(as_namespace* ns)
{
	histogram_scale scale = as_config_histogram_scale();

	histogram_rescale(ns->ops_sub_start_hist, scale);
	histogram_rescale(ns->ops_sub_restart_hist, scale);
	histogram_rescale(ns->ops_sub_dup_res_hist, scale);
	histogram_rescale(ns->ops_sub_master_hist, scale);
	histogram_rescale(ns->ops_sub_repl_write_hist, scale);
	histogram_rescale(ns->ops_sub_response_hist, scale);
}

static bool
any_benchmarks_enabled(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		if (ns->read_benchmarks_enabled ||
				ns->write_benchmarks_enabled ||
				ns->udf_benchmarks_enabled ||
				ns->batch_sub_benchmarks_enabled ||
				ns->udf_sub_benchmarks_enabled ||
				ns->ops_sub_benchmarks_enabled) {
			return true;
		}
	}

	return g_config.fabric_benchmarks_enabled;
}
