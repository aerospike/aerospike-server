/*
 * cfg_tree_handlers.cc
 *
 * Copyright (C) 2025 Aerospike, Inc.
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

#include "base/cfg_tree_handlers.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <sstream>
#include <syslog.h>

#include "nlohmann/json.hpp"

extern "C" {
#include "log.h"
#include "os.h"
#include "socket.h"
#include "tls.h"

#include "cf_str.h"
#include "secrets.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/security_config.h"
#include "base/thr_info.h"
}


//==========================================================
// Typedefs & constants.
//

using json = nlohmann::json;

// NO_OFFSET is usually used for fields with custom handlers
// that do not use the offset descriptor field
#define NO_OFFSET 0

namespace cfg_handlers
{
	//==========================================================
	// Forward declerations.
	//

	struct FieldDescriptor;

	using FieldHandler = std::function<void(void* target, const FieldDescriptor& desc, const nlohmann::json& value)>;

	// Tag types to disambiguate constructors
	struct EnterpriseOnly {};
	struct Deprecated { std::string msg; };

	struct FieldDescriptor {
		std::string json_path;  // JSON pointer path like "/service/batch-index-threads"
		size_t offset;          // offsetof(target_struct, field_name)
		FieldHandler handler;   // optional custom handler
		bool enterprise_only;
		std::string deprecation_warning;
		UnitType unit_type;  // Unit type for this field (NONE if not applicable)

		// Basic constructor (no units)
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h)
			: json_path(path), offset(off),
			handler(h), enterprise_only(false),
			deprecation_warning(""), unit_type(UnitType::NONE) {}

		// Constructor with unit type
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h,
				UnitType unit)
			: json_path(path), offset(off),
			handler(h), enterprise_only(false),
			deprecation_warning(""), unit_type(unit) {}

		// Enterprise-only constructor (no units)
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h,
				EnterpriseOnly)
			: json_path(path), offset(off),
			handler(h), enterprise_only(true),
			deprecation_warning(""), unit_type(UnitType::NONE) {}

		// Enterprise-only with unit type
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h,
				EnterpriseOnly, UnitType unit)
			: json_path(path), offset(off),
			handler(h), enterprise_only(true),
			deprecation_warning(""), unit_type(unit) {}

		// Deprecated constructor (no units)
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h,
				Deprecated depr)
			: json_path(path), offset(off),
			handler(h), enterprise_only(false),
			deprecation_warning(std::move(depr.msg)), unit_type(UnitType::NONE) {}

		// Deprecated with unit type
		FieldDescriptor(const std::string& path, size_t off, FieldHandler h,
				Deprecated depr, UnitType unit)
			: json_path(path), offset(off),
			handler(h), enterprise_only(false),
			deprecation_warning(std::move(depr.msg)), unit_type(unit) {}
	};

	// Edition detection
	static bool is_community_edition();

	// Helper functions for generic field application
	static bool get_json_value(const std::string& path,
			const nlohmann::json& source,
			nlohmann::json& result);

	// Handler forward declarations
	static void apply_field(void* target, const nlohmann::json& source, const FieldDescriptor& desc);
	static bool try_expand_unit_value(const FieldDescriptor& desc,
			const nlohmann::json& in,
			uint64_t& out);

	// Type-specific field appliers
	static void apply_uint16_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void apply_uint32_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void apply_uint64_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void apply_bool_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void apply_cstring_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void apply_pct_w_minus_1_field(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// Service context handlers
	static void handle_service(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_advertise_ipv6(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_auto_pin(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_cluster_name(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_feature_key_file(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_feature_key_files(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_group(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_info_max_ms(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_log_local_time(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_log_milliseconds(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_node_id(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_os_group_perms(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_secret_address_port(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_secret_tls_context(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_secret_uds_path(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_tls_refresh_period(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_user(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// mod-lua field handlers
	static void handle_mod_lua(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_mod_lua_user_path(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// namespace field handlers
	static void handle_namespaces(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_type(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_compression(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_devices(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_encryption(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_files(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_storage_engine_flush_max_ms(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_sindex_mounts(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_sindex_type(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_index_mounts(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_index_type(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_sets(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_write_commit_level_override(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_read_consistency_level_override(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_xdr_bin_tombstone_ttl(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_namespace_conflict_resolution_policy(void* ns, const FieldDescriptor& desc, const nlohmann::json& value);

	// network field handlers
	static void handle_network(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_admin(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_admin_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_admin_tls_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_admin_tls_authenticate_client(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_access_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_alternate_access_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_tls_access_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_tls_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_tls_alternate_access_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_service_tls_authenticate_client(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_mesh_seed_address_ports(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_mode(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_multicast_groups(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_protocol(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_tls_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_heartbeat_tls_mesh_seed_address_ports(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_fabric(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_fabric_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_fabric_tls_addresses(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_network_tls(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// xdr field handlers
	static void handle_xdr(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_auth_mode(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_node_address_ports(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_period_ms(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_namespaces(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_bin_policy(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ship_versions_policy(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_write_policy(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ignore_bins(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ignore_sets(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ship_bins(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ship_sets(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_xdr_dc_ns_ship_versions_interval(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// security field handlers
	static void handle_security(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_ldap(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_log(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_ldap_token_hash_method(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_ldap_role_query_patterns(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_log_report_data_op(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_log_report_data_op_role(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_security_log_report_data_op_user(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// logging field handlers
	static void handle_logging(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_logging_facility(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_logging_syslog_path(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_logging_syslog_tag(void* target, const FieldDescriptor& desc, const nlohmann::json& value);
	static void handle_logging_context_level(void* target, const FieldDescriptor& desc, const nlohmann::json& value);

	// Helper functions for applying specific fields
	static void apply_xdr_dc(const std::string& name,
			const nlohmann::json& dc_json, as_config* config);
	static void apply_xdr_dc_namespace(const std::string& name,
			const nlohmann::json& dc_ns_json, void* dc_cfg_ptr);
	static void apply_logging_sink(int index, const nlohmann::json& sink_json);
	static void apply_namespace_set(const std::string& name,
			const nlohmann::json& set_json, as_namespace* namespace_struct);

	//==========================================================
	// Field descriptor tables.
	//


	static const std::vector<FieldDescriptor> TOP_LEVEL_CONTEXT_DESCRIPTORS = {
		{"/service", NO_OFFSET, handle_service},
		{"/network", NO_OFFSET, handle_network},
		{"/xdr", NO_OFFSET, handle_xdr, EnterpriseOnly{}}, // enterprise-only
		{"/namespaces", NO_OFFSET, handle_namespaces},
		{"/mod-lua", NO_OFFSET, handle_mod_lua},
		{"/security", NO_OFFSET, handle_security, EnterpriseOnly{}}, // enterprise-only
		{"/logging", NO_OFFSET, handle_logging},
	};

	// Main service field descriptors /service
	static const std::vector<FieldDescriptor> SERVICE_FIELD_DESCRIPTORS = {
		{"/advertise-ipv6", NO_OFFSET, handle_advertise_ipv6},
		{"/auto-pin", offsetof(as_config, auto_pin), handle_auto_pin},
		{"/batch-index-threads", offsetof(as_config, n_batch_index_threads), apply_uint32_field},
		{"/batch-max-buffers-per-queue", offsetof(as_config, batch_max_buffers_per_queue), apply_uint32_field, UnitType::SIZE_U32},
		{"/batch-max-requests", offsetof(as_config, batch_max_requests), apply_uint32_field, UnitType::SIZE_U32},
		{"/batch-max-unused-buffers", offsetof(as_config, batch_max_unused_buffers), apply_uint32_field, UnitType::SIZE_U32},
		{"/cgroup-mem-tracking", offsetof(as_config, cgroup_mem_tracking), apply_bool_field},
		{"/cluster-name", NO_OFFSET, handle_cluster_name},
		{"/debug-allocations", offsetof(as_config, debug_allocations), apply_bool_field},
		{"/disable-udf-execution", offsetof(as_config, udf_execution_disabled), apply_bool_field},
		{"/enable-benchmarks-fabric", offsetof(as_config, fabric_benchmarks_enabled), apply_bool_field},
		{"/enable-health-check", offsetof(as_config, health_check_enabled), apply_bool_field},
		{"/enable-hist-info", offsetof(as_config, info_hist_enabled), apply_bool_field},
		{"/enforce-best-practices", offsetof(as_config, enforce_best_practices), apply_bool_field},
		{"/feature-key-file", NO_OFFSET, handle_feature_key_file, EnterpriseOnly{}}, // enterprise-only
		{"/feature-key-files", NO_OFFSET, handle_feature_key_files, EnterpriseOnly{}}, // enterprise-only
		{"/group", NO_OFFSET, handle_group, Deprecated{"service/group is deprecated."}},
		{"/indent-allocations", offsetof(as_config, indent_allocations), apply_bool_field},
		{"/info-max-ms", NO_OFFSET, handle_info_max_ms, UnitType::SIZE_U64},
		{"/info-threads", offsetof(as_config, n_info_threads), apply_uint32_field},
		{"/keep-caps-ssd-health", offsetof(as_config, keep_caps_ssd_health), apply_bool_field},
		{"/log-local-time", NO_OFFSET, handle_log_local_time},
		{"/log-milliseconds", NO_OFFSET, handle_log_milliseconds},
		{"/microsecond-histograms", offsetof(as_config, microsecond_histograms), apply_bool_field},
		{"/migrate-fill-delay", offsetof(as_config, migrate_fill_delay), apply_uint32_field, EnterpriseOnly{}, UnitType::TIME_DURATION}, // enterprise-only
		{"/migrate-max-num-incoming", offsetof(as_config, migrate_max_num_incoming), apply_uint32_field},
		{"/migrate-threads", offsetof(as_config, n_migrate_threads), apply_uint32_field},
		{"/min-cluster-size", offsetof(as_config, clustering_config.cluster_size_min), apply_uint32_field},
		{"/node-id", NO_OFFSET, handle_node_id},
		{"/node-id-interface", offsetof(as_config, node_id_interface), apply_cstring_field},
		{"/os-group-perms", NO_OFFSET, handle_os_group_perms},
		{"/pidfile", offsetof(as_config, pidfile), apply_cstring_field, Deprecated{"service/pidfile is deprecated."}},
		{"/poison-allocations", offsetof(as_config, poison_allocations), apply_bool_field},
		{"/proto-fd-idle-ms", offsetof(as_config, proto_fd_idle_ms), apply_uint32_field, Deprecated{"service/proto-fd-idle-ms is deprecated."}},
		{"/proto-fd-max", offsetof(as_config, n_proto_fd_max), apply_uint32_field, UnitType::SIZE_U32},
		{"/quarantine-allocations", offsetof(as_config, quarantine_allocations), apply_uint32_field, UnitType::SIZE_U32},
		{"/query-max-done", offsetof(as_config, query_max_done), apply_uint32_field},
		{"/query-threads-limit", offsetof(as_config, n_query_threads_limit), apply_uint32_field},
		{"/run-as-daemon", offsetof(as_config, run_as_daemon), apply_bool_field},
		{"/secret-address-port", NO_OFFSET, handle_secret_address_port},
		{"/secret-tls-context", NO_OFFSET, handle_secret_tls_context},
		{"/secret-uds-path", NO_OFFSET, handle_secret_uds_path},
		{"/service-threads", offsetof(as_config, n_service_threads), apply_uint32_field},
		{"/sindex-builder-threads", offsetof(as_config, sindex_builder_threads), apply_uint32_field},
		{"/sindex-gc-period", offsetof(as_config, sindex_gc_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/stay-quiesced", offsetof(as_config, stay_quiesced), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/ticker-interval", offsetof(as_config, ticker_interval), apply_uint32_field, UnitType::TIME_DURATION},
		{"/tls-refresh-period", NO_OFFSET, handle_tls_refresh_period, EnterpriseOnly{}, UnitType::TIME_DURATION}, // enterprise-only
		// TODO: this needs to be multiplied by 1000000
		{"/transaction-max-ms", offsetof(as_config, transaction_max_ns), apply_uint64_field, UnitType::SIZE_U64},
		{"/transaction-retry-ms", offsetof(as_config, transaction_retry_ms), apply_uint32_field},
		{"/user", NO_OFFSET, handle_user, Deprecated{"service/user is deprecated."}},
		{"/work-directory", offsetof(as_config, work_directory), apply_cstring_field},
	};


	// Network sub-context field descriptors /network
	static const std::vector<FieldDescriptor> NETWORK_FIELD_DESCRIPTORS = {
		{"/admin", NO_OFFSET, handle_network_admin},
		{"/service", NO_OFFSET, handle_network_service},
		{"/heartbeat", NO_OFFSET, handle_network_heartbeat},
		{"/fabric", NO_OFFSET, handle_network_fabric},
		{"/tls", NO_OFFSET, handle_network_tls, EnterpriseOnly{}}, // enterprise-only
	};

	// Network admin field descriptors /network/admin
	static const std::vector<FieldDescriptor> NETWORK_ADMIN_FIELD_DESCRIPTORS = {
		{"/addresses", NO_OFFSET, handle_network_admin_addresses},
		{"/disable-localhost", offsetof(as_config, admin_localhost_disabled), apply_bool_field},
		{"/port", offsetof(as_config, admin.bind_port), apply_uint16_field},
		{"/tls-addresses", NO_OFFSET, handle_network_admin_tls_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-authenticate-client", NO_OFFSET, handle_network_admin_tls_authenticate_client, EnterpriseOnly{}}, // enterprise-only
		{"/tls-name", offsetof(as_config, tls_admin.tls_our_name), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-port", offsetof(as_config, tls_admin.bind_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
	};

	// Network service field descriptors /network/service
	static const std::vector<FieldDescriptor> NETWORK_SERVICE_FIELD_DESCRIPTORS = {
		{"/access-addresses", NO_OFFSET, handle_network_service_access_addresses},
		{"/access-port", offsetof(as_config, service.std_port), apply_uint16_field},
		{"/addresses", NO_OFFSET, handle_network_service_addresses},
		{"/alternate-access-addresses", NO_OFFSET, handle_network_service_alternate_access_addresses},
		{"/alternate-access-port", offsetof(as_config, service.alt_port), apply_uint16_field},
		{"/disable-localhost", offsetof(as_config, service_localhost_disabled), apply_bool_field},
		{"/port", offsetof(as_config, service.bind_port), apply_uint16_field},
		{"/tls-access-addresses", NO_OFFSET, handle_network_service_tls_access_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-access-port", offsetof(as_config, tls_service.std_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-addresses", NO_OFFSET, handle_network_service_tls_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-alternate-access-addresses", NO_OFFSET, handle_network_service_tls_alternate_access_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-alternate-access-port", offsetof(as_config, tls_service.alt_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-authenticate-client", NO_OFFSET, handle_network_service_tls_authenticate_client, EnterpriseOnly{}}, // enterprise-only
		{"/tls-name", offsetof(as_config, tls_service.tls_our_name), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-port", offsetof(as_config, tls_service.bind_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
	};

	// Network heartbeat field descriptors /network/heartbeat
	static const std::vector<FieldDescriptor> NETWORK_HEARTBEAT_FIELD_DESCRIPTORS = {
		{"/addresses", NO_OFFSET, handle_network_heartbeat_addresses, Deprecated{"network/heartbeat/addresses is deprecated"}},
		{"/connect-timeout-ms", offsetof(as_config, hb_config.connect_timeout_ms), apply_uint32_field},
		{"/interval", offsetof(as_config, hb_config.tx_interval), apply_uint32_field},
		{"/mesh-seed-address-ports", NO_OFFSET, handle_network_heartbeat_mesh_seed_address_ports},
		{"/mode", NO_OFFSET, handle_network_heartbeat_mode},
		{"/mtu", offsetof(as_config, hb_config.override_mtu), apply_uint32_field},
		{"/multicast-groups", NO_OFFSET, handle_network_heartbeat_multicast_groups},
		{"/multicast-ttl", offsetof(as_config, hb_config.multicast_ttl), apply_uint32_field},
		{"/port", offsetof(as_config, hb_serv_spec.bind_port), apply_uint16_field},
		{"/protocol", NO_OFFSET, handle_network_heartbeat_protocol},
		{"/timeout", offsetof(as_config, hb_config.max_intervals_missed), apply_uint32_field},
		{"/tls-addresses", NO_OFFSET, handle_network_heartbeat_tls_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-mesh-seed-address-ports", NO_OFFSET, handle_network_heartbeat_tls_mesh_seed_address_ports},
		{"/tls-name", offsetof(as_config, hb_tls_serv_spec.tls_our_name), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-port", offsetof(as_config, hb_tls_serv_spec.bind_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
	};

	// Network fabric field descriptors /network/fabric
	static const std::vector<FieldDescriptor> NETWORK_FABRIC_FIELD_DESCRIPTORS = {
		{"/addresses", NO_OFFSET, handle_network_fabric_addresses},
		{"/channel-bulk-fds", offsetof(as_config, n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK]), apply_uint32_field},
		{"/channel-bulk-recv-threads", offsetof(as_config, n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK]), apply_uint32_field},
		{"/channel-ctrl-fds", offsetof(as_config, n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL]), apply_uint32_field},
		{"/channel-ctrl-recv-threads", offsetof(as_config, n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL]), apply_uint32_field},
		{"/channel-meta-fds", offsetof(as_config, n_fabric_channel_fds[AS_FABRIC_CHANNEL_META]), apply_uint32_field},
		{"/channel-meta-recv-threads", offsetof(as_config, n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META]), apply_uint32_field},
		{"/channel-rw-fds", offsetof(as_config, n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW]), apply_uint32_field},
		{"/channel-rw-recv-pools", offsetof(as_config, n_fabric_channel_recv_pools[AS_FABRIC_CHANNEL_RW]), apply_uint32_field},
		{"/channel-rw-recv-threads", offsetof(as_config, n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW]), apply_uint32_field},
		{"/keepalive-enabled", offsetof(as_config, fabric_keepalive_enabled), apply_bool_field},
		{"/keepalive-intvl", offsetof(as_config, fabric_keepalive_intvl), apply_uint32_field},
		{"/keepalive-probes", offsetof(as_config, fabric_keepalive_probes), apply_uint32_field},
		{"/keepalive-time", offsetof(as_config, fabric_keepalive_time), apply_uint32_field},
		{"/latency-max-ms", offsetof(as_config, fabric_latency_max_ms), apply_uint32_field},
		{"/port", offsetof(as_config, fabric.bind_port), apply_uint16_field},
		{"/recv-rearm-threshold", offsetof(as_config, fabric_recv_rearm_threshold), apply_uint32_field},
		{"/send-threads", offsetof(as_config, n_fabric_send_threads), apply_uint32_field},
		{"/tls-addresses", NO_OFFSET, handle_network_fabric_tls_addresses, EnterpriseOnly{}}, // enterprise-only
		{"/tls-name", offsetof(as_config, tls_fabric.tls_our_name), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/tls-port", offsetof(as_config, tls_fabric.bind_port), apply_uint16_field, EnterpriseOnly{}}, // enterprise-only
	};

	// Network TLS field descriptors /network/tls
	static const std::vector<FieldDescriptor> NETWORK_TLS_FIELD_DESCRIPTORS = {
		{"/ca-file", offsetof(cf_tls_spec, ca_file), apply_cstring_field},
		{"/ca-path", offsetof(cf_tls_spec, ca_path), apply_cstring_field},
		{"/cert-blacklist", offsetof(cf_tls_spec, cert_blacklist), apply_cstring_field},
		{"/cert-file", offsetof(cf_tls_spec, cert_file), apply_cstring_field},
		{"/cipher-suite", offsetof(cf_tls_spec, cipher_suite), apply_cstring_field},
		{"/key-file", offsetof(cf_tls_spec, key_file), apply_cstring_field},
		{"/key-file-password", offsetof(cf_tls_spec, key_file_password), apply_cstring_field},
		{"/pki-user-append-ou", offsetof(cf_tls_spec, pki_user_append_ou), apply_bool_field},
		{"/protocols", offsetof(cf_tls_spec, protocols), apply_cstring_field},
	};

	// Mod-lua field descriptors /mod-lua
	static const std::vector<FieldDescriptor> MOD_LUA_FIELD_DESCRIPTORS = {
		{"/cache-enabled", offsetof(as_config, mod_lua.cache_enabled), apply_bool_field},
		{"/user-path", NO_OFFSET, handle_mod_lua_user_path},
	};

	// Namespace field descriptors /namespaces
	static const std::vector<FieldDescriptor> NAMESPACE_FIELD_DESCRIPTORS = {
		{"/active-rack", offsetof(as_namespace, cfg_active_rack), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
		{"/allow-ttl-without-nsup", offsetof(as_namespace, allow_ttl_without_nsup), apply_bool_field},
		{"/apply-ttl-reductions", offsetof(as_namespace, apply_ttl_reductions), apply_bool_field},
		{"/auto-revive", offsetof(as_namespace, auto_revive), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/background-query-max-rps", offsetof(as_namespace, background_query_max_rps), apply_uint32_field},
		{"/conflict-resolution-policy", offsetof(as_namespace, conflict_resolution_policy), handle_namespace_conflict_resolution_policy},
		{"/conflict-resolve-writes", offsetof(as_namespace, conflict_resolve_writes), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/default-read-touch-ttl-pct", offsetof(as_namespace, default_read_touch_ttl_pct), apply_uint32_field},
		{"/default-ttl", offsetof(as_namespace, default_ttl), apply_uint32_field, UnitType::TIME_DURATION},
		{"/disable-cold-start-eviction", offsetof(as_namespace, cold_start_eviction_disabled), apply_bool_field},
		{"/disable-mrt-writes", offsetof(as_namespace, mrt_writes_disabled), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/disable-write-dup-res", offsetof(as_namespace, write_dup_res_disabled), apply_bool_field},
		{"/disallow-expunge", offsetof(as_namespace, ap_disallow_drops), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/disallow-null-setname", offsetof(as_namespace, disallow_null_setname), apply_bool_field},
		{"/enable-benchmarks-batch-sub", offsetof(as_namespace, batch_sub_benchmarks_enabled), apply_bool_field},
		{"/enable-benchmarks-ops-sub", offsetof(as_namespace, ops_sub_benchmarks_enabled), apply_bool_field},
		{"/enable-benchmarks-read", offsetof(as_namespace, read_benchmarks_enabled), apply_bool_field},
		{"/enable-benchmarks-udf", offsetof(as_namespace, udf_benchmarks_enabled), apply_bool_field},
		{"/enable-benchmarks-udf-sub", offsetof(as_namespace, udf_sub_benchmarks_enabled), apply_bool_field},
		{"/enable-benchmarks-write", offsetof(as_namespace, write_benchmarks_enabled), apply_bool_field},
		{"/enable-hist-proxy", offsetof(as_namespace, proxy_hist_enabled), apply_bool_field},
		{"/evict-hist-buckets", offsetof(as_namespace, evict_hist_buckets), apply_uint32_field},
		{"/evict-indexes-memory-pct", offsetof(as_namespace, evict_indexes_memory_pct), apply_uint32_field},
		{"/evict-tenths-pct", offsetof(as_namespace, evict_tenths_pct), apply_uint32_field},
		{"/ignore-migrate-fill-delay", offsetof(as_namespace, ignore_migrate_fill_delay), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/index-stage-size", offsetof(as_namespace, index_stage_size), apply_uint64_field, UnitType::SIZE_U64},
		{"/indexes-memory-budget", offsetof(as_namespace, indexes_memory_budget), apply_uint64_field, UnitType::SIZE_U64},
		{"/inline-short-queries", offsetof(as_namespace, inline_short_queries), apply_bool_field},
		{"/max-record-size", offsetof(as_namespace, max_record_size), apply_uint32_field, UnitType::SIZE_U32},
		{"/migrate-order", offsetof(as_namespace, migrate_order), apply_uint32_field},
		{"/migrate-retransmit-ms", offsetof(as_namespace, migrate_retransmit_ms), apply_uint32_field},
		{"/migrate-skip-unreadable", offsetof(as_namespace, migrate_skip_unreadable), apply_bool_field},
		{"/migrate-sleep", offsetof(as_namespace, migrate_sleep), apply_uint32_field},
		{"/mrt-duration", offsetof(as_namespace, mrt_duration), apply_uint32_field, EnterpriseOnly{}, UnitType::TIME_DURATION}, // enterprise-only
		{"/nsup-hist-period", offsetof(as_namespace, nsup_hist_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/nsup-period", offsetof(as_namespace, nsup_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/nsup-threads", offsetof(as_namespace, n_nsup_threads), apply_uint32_field},
		{"/partition-tree-sprigs", offsetof(as_namespace, tree_shared.n_sprigs), apply_uint32_field},
		{"/prefer-uniform-balance", offsetof(as_namespace, cfg_prefer_uniform_balance), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/rack-id", offsetof(as_namespace, rack_id), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
		{"/read-consistency-level-override", NO_OFFSET, handle_namespace_read_consistency_level_override},
		{"/reject-non-xdr-writes", offsetof(as_namespace, reject_non_xdr_writes), apply_bool_field},
		{"/reject-xdr-writes", offsetof(as_namespace, reject_xdr_writes), apply_bool_field},
		{"/replication-factor", offsetof(as_namespace, replication_factor), apply_uint32_field},
		{"/sindex-stage-size", offsetof(as_namespace, sindex_stage_size), apply_uint64_field, UnitType::SIZE_U64},
		{"/single-query-threads", offsetof(as_namespace, n_single_query_threads), apply_uint32_field},
		{"/stop-writes-sys-memory-pct", offsetof(as_namespace, stop_writes_sys_memory_pct), apply_uint32_field},
		{"/strong-consistency", offsetof(as_namespace, cp), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/strong-consistency-allow-expunge", offsetof(as_namespace, cp_allow_drops), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/tomb-raider-eligible-age", offsetof(as_namespace, tomb_raider_eligible_age), apply_uint32_field, EnterpriseOnly{}, UnitType::TIME_DURATION}, // enterprise-only
		{"/tomb-raider-period", offsetof(as_namespace, tomb_raider_period), apply_uint32_field, EnterpriseOnly{}, UnitType::TIME_DURATION}, // enterprise-only
		{"/tomb-raider-unmark-threads", offsetof(as_namespace, n_tomb_raider_unmark_threads), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
		{"/transaction-pending-limit", offsetof(as_namespace, transaction_pending_limit), apply_uint32_field},
		{"/truncate-threads", offsetof(as_namespace, n_truncate_threads), apply_uint32_field},
		{"/write-commit-level-override", offsetof(as_namespace, write_commit_level), handle_namespace_write_commit_level_override},
		{"/xdr-bin-tombstone-ttl", NO_OFFSET, handle_namespace_xdr_bin_tombstone_ttl, UnitType::TIME_DURATION},
		{"/xdr-tomb-raider-period", offsetof(as_namespace, xdr_tomb_raider_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/xdr-tomb-raider-threads", offsetof(as_namespace, n_xdr_tomb_raider_threads), apply_uint32_field},
		{"/geo2dsphere-within/strict", offsetof(as_namespace, geo2dsphere_within_strict), apply_bool_field},
		{"/geo2dsphere-within/min-level", offsetof(as_namespace, geo2dsphere_within_min_level), apply_uint16_field},
		{"/geo2dsphere-within/max-level", offsetof(as_namespace, geo2dsphere_within_max_level), apply_uint16_field},
		{"/geo2dsphere-within/max-cells", offsetof(as_namespace, geo2dsphere_within_max_cells), apply_uint16_field},
		{"/geo2dsphere-within/level-mod", offsetof(as_namespace, geo2dsphere_within_level_mod), apply_uint16_field},
		{"/geo2dsphere-within/earth-radius-meters", offsetof(as_namespace, geo2dsphere_within_earth_radius_meters), apply_uint32_field},
		{"/index-type/type", NO_OFFSET, handle_namespace_index_type},
		{"/index-type/evict-mounts-pct", offsetof(as_namespace, pi_evict_mounts_pct), apply_uint32_field},
		{"/index-type/mounts", NO_OFFSET, handle_namespace_index_mounts},
		{"/index-type/mounts-budget", offsetof(as_namespace, pi_mounts_budget), apply_uint64_field, UnitType::SIZE_U64},
		{"/sets", NO_OFFSET, handle_namespace_sets},
		{"/sindex-type/type", NO_OFFSET, handle_namespace_sindex_type},
		{"/sindex-type/mounts", NO_OFFSET, handle_namespace_sindex_mounts},
		{"/sindex-type/mounts-budget", offsetof(as_namespace, si_mounts_budget), apply_uint64_field, UnitType::SIZE_U64},
		{"/sindex-type/evict-mounts-pct", offsetof(as_namespace, si_evict_mounts_pct), apply_uint32_field},
		{"/storage-engine/type", NO_OFFSET, handle_namespace_storage_engine_type},
		{"/storage-engine/cache-replica-writes", offsetof(as_namespace, storage_cache_replica_writes), apply_bool_field},
		{"/storage-engine/cold-start-empty", offsetof(as_namespace, storage_cold_start_empty), apply_bool_field},
		{"/storage-engine/commit-to-device", offsetof(as_namespace, storage_commit_to_device), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/compression", NO_OFFSET, handle_namespace_storage_engine_compression, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/compression-acceleration", offsetof(as_namespace, storage_compression_acceleration), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/compression-level", offsetof(as_namespace, storage_compression_level), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/data-size", offsetof(as_namespace, storage_data_size), apply_uint64_field, UnitType::SIZE_U64},
		{"/storage-engine/defrag-lwm-pct", offsetof(as_namespace, storage_defrag_lwm_pct), apply_uint32_field},
		{"/storage-engine/defrag-queue-min", offsetof(as_namespace, storage_defrag_queue_min), apply_uint32_field},
		{"/storage-engine/defrag-sleep", offsetof(as_namespace, storage_defrag_sleep), apply_uint32_field},
		{"/storage-engine/defrag-startup-minimum", offsetof(as_namespace, storage_defrag_startup_minimum), apply_uint32_field},
		{"/storage-engine/devices", NO_OFFSET, handle_namespace_storage_engine_devices},
		{"/storage-engine/direct-files", offsetof(as_namespace, storage_direct_files), apply_bool_field},
		{"/storage-engine/disable-odsync", offsetof(as_namespace, storage_disable_odsync), apply_bool_field},
		{"/storage-engine/enable-benchmarks-storage", offsetof(as_namespace, storage_benchmarks_enabled), apply_bool_field},
		{"/storage-engine/encryption", NO_OFFSET, handle_namespace_storage_engine_encryption, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/encryption-key-file", offsetof(as_namespace, storage_encryption_key_file), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/encryption-old-key-file", offsetof(as_namespace, storage_encryption_old_key_file), apply_cstring_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/evict-used-pct", offsetof(as_namespace, storage_evict_used_pct), apply_uint32_field},
		{"/storage-engine/files", NO_OFFSET, handle_namespace_storage_engine_files},
		{"/storage-engine/filesize", offsetof(as_namespace, storage_filesize), apply_uint64_field, UnitType::SIZE_U64},
		{"/storage-engine/flush-max-ms", NO_OFFSET, handle_namespace_storage_engine_flush_max_ms},
		{"/storage-engine/flush-size", offsetof(as_namespace, storage_flush_size), apply_uint32_field, UnitType::SIZE_U32},
		{"/storage-engine/max-write-cache", offsetof(as_namespace, storage_max_write_cache), apply_uint64_field, UnitType::SIZE_U64},
		{"/storage-engine/post-write-cache", offsetof(as_namespace, storage_post_write_cache), apply_uint64_field, UnitType::SIZE_U64},
		{"/storage-engine/read-page-cache", offsetof(as_namespace, storage_read_page_cache), apply_bool_field},
		{"/storage-engine/serialize-tomb-raider", offsetof(as_namespace, storage_serialize_tomb_raider), apply_bool_field, EnterpriseOnly{}}, // enterprise-only
		{"/storage-engine/sindex-startup-device-scan", offsetof(as_namespace, storage_sindex_startup_device_scan), apply_bool_field},
		{"/storage-engine/stop-writes-avail-pct", offsetof(as_namespace, storage_stop_writes_avail_pct), apply_uint32_field},
		{"/storage-engine/stop-writes-used-pct", offsetof(as_namespace, storage_stop_writes_used_pct), apply_uint32_field},
		{"/storage-engine/tomb-raider-sleep", offsetof(as_namespace, storage_tomb_raider_sleep), apply_uint32_field, EnterpriseOnly{}}, // enterprise-only
	};

	// Set field descriptors /namespaces/sets
	static const std::vector<FieldDescriptor> NAMESPACE_SET_FIELD_DESCRIPTORS = {
		{"/default-read-touch-ttl-pct", offsetof(as_set, default_read_touch_ttl_pct), apply_pct_w_minus_1_field},
		{"/default-ttl", offsetof(as_set, default_ttl), apply_uint32_field, UnitType::TIME_DURATION},
		{"/disable-eviction", offsetof(as_set, eviction_disabled), apply_bool_field},
		{"/enable-index", offsetof(as_set, index_enabled), apply_bool_field},
		{"/stop-writes-count", offsetof(as_set, stop_writes_count), apply_uint64_field, UnitType::SIZE_U64},
		{"/stop-writes-size", offsetof(as_set, stop_writes_size), apply_uint64_field, UnitType::SIZE_U64},
	};

	// XDR DC field descriptors /xdr/dc
	static const std::vector<FieldDescriptor> XDR_DC_FIELD_DESCRIPTORS = {
		{"/auth-mode", NO_OFFSET, handle_xdr_dc_auth_mode},
		{"/auth-password-file", offsetof(as_xdr_dc_cfg, auth_password_file), apply_cstring_field},
		{"/auth-user", offsetof(as_xdr_dc_cfg, auth_user), apply_cstring_field},
		{"/connector", offsetof(as_xdr_dc_cfg, connector), apply_bool_field},
		{"/max-recoveries-interleaved", offsetof(as_xdr_dc_cfg, max_recoveries_interleaved), apply_uint32_field},
		{"/node-address-ports", NO_OFFSET, handle_xdr_dc_node_address_ports},
		{"/period-ms", offsetof(as_xdr_dc_cfg, period_us), handle_xdr_dc_period_ms},
		{"/recovery-threads", offsetof(as_xdr_dc_cfg, n_recovery_threads), apply_uint32_field},
		{"/tls-name", offsetof(as_xdr_dc_cfg, tls_our_name), apply_cstring_field},
		{"/use-alternate-access-address", offsetof(as_xdr_dc_cfg, use_alternate_access_address), apply_bool_field},
		{"/namespaces", NO_OFFSET, handle_xdr_dc_namespaces},
	};

	// XDR DC namespace field descriptors /xdr/dc/namespaces
	static const std::vector<FieldDescriptor> XDR_DC_NS_FIELD_DESCRIPTORS = {
		{"/bin-policy", NO_OFFSET, handle_xdr_dc_ns_bin_policy},
		{"/compression-level", offsetof(as_xdr_dc_ns_cfg, compression_level), apply_uint32_field},
		{"/compression-threshold", offsetof(as_xdr_dc_ns_cfg, compression_threshold), apply_uint32_field},
		{"/delay-ms", offsetof(as_xdr_dc_ns_cfg, delay_ms), apply_uint32_field},
		{"/enable-compression", offsetof(as_xdr_dc_ns_cfg, compression_enabled), apply_bool_field},
		{"/forward", offsetof(as_xdr_dc_ns_cfg, forward), apply_bool_field},
		{"/hot-key-ms", offsetof(as_xdr_dc_ns_cfg, hot_key_ms), apply_uint32_field},
		{"/ignore-bins", NO_OFFSET, handle_xdr_dc_ns_ignore_bins},
		{"/ignore-expunges", offsetof(as_xdr_dc_ns_cfg, ignore_expunges), apply_bool_field},
		{"/ignore-sets", NO_OFFSET, handle_xdr_dc_ns_ignore_sets},
		{"/max-throughput", offsetof(as_xdr_dc_ns_cfg, max_throughput), apply_uint32_field},
		{"/remote-namespace", offsetof(as_xdr_dc_ns_cfg, remote_namespace), apply_cstring_field},
		{"/sc-replication-wait-ms", offsetof(as_xdr_dc_ns_cfg, sc_replication_wait_ms), apply_uint32_field},
		{"/ship-bins", NO_OFFSET, handle_xdr_dc_ns_ship_bins},
		{"/ship-bin-luts", offsetof(as_xdr_dc_ns_cfg, ship_bin_luts), apply_bool_field},
		{"/ship-nsup-deletes", offsetof(as_xdr_dc_ns_cfg, ship_nsup_deletes), apply_bool_field},
		{"/ship-only-specified-sets", offsetof(as_xdr_dc_ns_cfg, ship_only_specified_sets), apply_bool_field},
		{"/ship-sets", NO_OFFSET, handle_xdr_dc_ns_ship_sets},
		{"/ship-versions-interval", NO_OFFSET, handle_xdr_dc_ns_ship_versions_interval, UnitType::TIME_DURATION},
		{"/ship-versions-policy", NO_OFFSET, handle_xdr_dc_ns_ship_versions_policy},
		{"/transaction-queue-limit", offsetof(as_xdr_dc_ns_cfg, transaction_queue_limit), apply_uint32_field},
		{"/write-policy", NO_OFFSET, handle_xdr_dc_ns_write_policy},
	};

	// Security field descriptors /security
	static const std::vector<FieldDescriptor> SECURITY_FIELD_DESCRIPTORS = {
		{"/default-password-file", offsetof(as_sec_config, default_password_file), apply_cstring_field},
		{"/enable-quotas", offsetof(as_sec_config, quotas_enabled), apply_bool_field},
		{"/privilege-refresh-period", offsetof(as_sec_config, privilege_refresh_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/session-ttl", offsetof(as_sec_config, session_ttl), apply_uint32_field, UnitType::TIME_DURATION},
		{"/tps-weight", offsetof(as_sec_config, tps_weight), apply_uint32_field},
		{"/ldap", NO_OFFSET, handle_security_ldap},
		{"/log", NO_OFFSET, handle_security_log},
	};

	// Security LDAP field descriptors /security/ldap
	static const std::vector<FieldDescriptor> SECURITY_LDAP_FIELD_DESCRIPTORS = {
		{"/disable-tls", offsetof(as_sec_config, ldap_tls_disabled), apply_bool_field},
		{"/login-threads", offsetof(as_sec_config, n_ldap_login_threads), apply_uint32_field},
		{"/polling-period", offsetof(as_sec_config, ldap_polling_period), apply_uint32_field, UnitType::TIME_DURATION},
		{"/query-base-dn", offsetof(as_sec_config, ldap_query_base_dn), apply_cstring_field},
		{"/query-user-dn", offsetof(as_sec_config, ldap_query_user_dn), apply_cstring_field},
		{"/query-user-password-file", offsetof(as_sec_config, ldap_query_user_password_file), apply_cstring_field},
		{"/role-query-base-dn", offsetof(as_sec_config, ldap_role_query_base_dn), apply_cstring_field},
		{"/role-query-patterns", NO_OFFSET, handle_security_ldap_role_query_patterns},
		{"/role-query-search-ou", offsetof(as_sec_config, ldap_role_query_search_ou), apply_bool_field},
		{"/server", offsetof(as_sec_config, ldap_server), apply_cstring_field},
		{"/tls-ca-file", offsetof(as_sec_config, ldap_tls_ca_file), apply_cstring_field},
		{"/token-hash-method", NO_OFFSET, handle_security_ldap_token_hash_method},
		{"/user-dn-pattern", offsetof(as_sec_config, ldap_user_dn_pattern), apply_cstring_field},
		{"/user-query-pattern", offsetof(as_sec_config, ldap_user_query_pattern), apply_cstring_field},
	};

	// Security log field descriptors /security/log
	static const std::vector<FieldDescriptor> SECURITY_LOG_FIELD_DESCRIPTORS = {
		{"/report-authentication", offsetof(as_sec_config, report.authentication), apply_bool_field},
		{"/report-data-op", NO_OFFSET, handle_security_log_report_data_op},
		{"/report-data-op-role", NO_OFFSET, handle_security_log_report_data_op_role},
		{"/report-data-op-user", NO_OFFSET, handle_security_log_report_data_op_user},
		{"/report-sys-admin", offsetof(as_sec_config, report.sys_admin), apply_bool_field},
		{"/report-user-admin", offsetof(as_sec_config, report.user_admin), apply_bool_field},
		{"/report-violation", offsetof(as_sec_config, report.violation), apply_bool_field},
	};

	// Logging field descriptors /logging
	// Note: path, tag, facility are at top level; contexts are nested under /contexts
	// IMPORTANT: /contexts/any must come FIRST so it sets all contexts,
	// then individual contexts can override it.
	static const std::vector<FieldDescriptor> LOGGING_FIELD_DESCRIPTORS = {
		{"/path", NO_OFFSET, handle_logging_syslog_path},
		{"/tag", NO_OFFSET, handle_logging_syslog_tag},
		{"/facility", NO_OFFSET, handle_logging_facility},
		{"/contexts/any", NO_OFFSET, handle_logging_context_level},
		{"/contexts/misc", NO_OFFSET, handle_logging_context_level},
		{"/contexts/alloc", NO_OFFSET, handle_logging_context_level},
		{"/contexts/arenax", NO_OFFSET, handle_logging_context_level},
		{"/contexts/hardware", NO_OFFSET, handle_logging_context_level},
		{"/contexts/msg", NO_OFFSET, handle_logging_context_level},
		{"/contexts/os", NO_OFFSET, handle_logging_context_level},
		{"/contexts/secrets", NO_OFFSET, handle_logging_context_level},
		{"/contexts/socket", NO_OFFSET, handle_logging_context_level},
		{"/contexts/tls", NO_OFFSET, handle_logging_context_level},
		{"/contexts/vault", NO_OFFSET, handle_logging_context_level},
		{"/contexts/vmapx", NO_OFFSET, handle_logging_context_level},
		{"/contexts/xmem", NO_OFFSET, handle_logging_context_level},
		{"/contexts/aggr", NO_OFFSET, handle_logging_context_level},
		{"/contexts/appeal", NO_OFFSET, handle_logging_context_level},
		{"/contexts/as", NO_OFFSET, handle_logging_context_level},
		{"/contexts/audit", NO_OFFSET, handle_logging_context_level},
		{"/contexts/batch", NO_OFFSET, handle_logging_context_level},
		{"/contexts/batch-sub", NO_OFFSET, handle_logging_context_level},
		{"/contexts/bin", NO_OFFSET, handle_logging_context_level},
		{"/contexts/config", NO_OFFSET, handle_logging_context_level},
		{"/contexts/clustering", NO_OFFSET, handle_logging_context_level},
		{"/contexts/drv-mem", NO_OFFSET, handle_logging_context_level},
		{"/contexts/drv_pmem", NO_OFFSET, handle_logging_context_level},
		{"/contexts/drv_ssd", NO_OFFSET, handle_logging_context_level},
		{"/contexts/exchange", NO_OFFSET, handle_logging_context_level},
		{"/contexts/exp", NO_OFFSET, handle_logging_context_level},
		{"/contexts/fabric", NO_OFFSET, handle_logging_context_level},
		{"/contexts/flat", NO_OFFSET, handle_logging_context_level},
		{"/contexts/geo", NO_OFFSET, handle_logging_context_level},
		{"/contexts/hb", NO_OFFSET, handle_logging_context_level},
		{"/contexts/health", NO_OFFSET, handle_logging_context_level},
		{"/contexts/hlc", NO_OFFSET, handle_logging_context_level},
		{"/contexts/index", NO_OFFSET, handle_logging_context_level},
		{"/contexts/info", NO_OFFSET, handle_logging_context_level},
		{"/contexts/info-command", NO_OFFSET, handle_logging_context_level},
		{"/contexts/info-port", NO_OFFSET, handle_logging_context_level},
		{"/contexts/key-busy", NO_OFFSET, handle_logging_context_level},
		{"/contexts/migrate", NO_OFFSET, handle_logging_context_level},
		{"/contexts/mrt-audit", NO_OFFSET, handle_logging_context_level},
		{"/contexts/mrt-monitor", NO_OFFSET, handle_logging_context_level},
		{"/contexts/namespace", NO_OFFSET, handle_logging_context_level},
		{"/contexts/nsup", NO_OFFSET, handle_logging_context_level},
		{"/contexts/particle", NO_OFFSET, handle_logging_context_level},
		{"/contexts/partition", NO_OFFSET, handle_logging_context_level},
		{"/contexts/proto", NO_OFFSET, handle_logging_context_level},
		{"/contexts/proxy", NO_OFFSET, handle_logging_context_level},
		{"/contexts/proxy-divert", NO_OFFSET, handle_logging_context_level},
		{"/contexts/query", NO_OFFSET, handle_logging_context_level},
		{"/contexts/record", NO_OFFSET, handle_logging_context_level},
		{"/contexts/roster", NO_OFFSET, handle_logging_context_level},
		{"/contexts/rw", NO_OFFSET, handle_logging_context_level},
		{"/contexts/rw-client", NO_OFFSET, handle_logging_context_level},
		{"/contexts/security", NO_OFFSET, handle_logging_context_level},
		{"/contexts/service", NO_OFFSET, handle_logging_context_level},
		{"/contexts/service-list", NO_OFFSET, handle_logging_context_level},
		{"/contexts/sindex", NO_OFFSET, handle_logging_context_level},
		{"/contexts/skew", NO_OFFSET, handle_logging_context_level},
		{"/contexts/smd", NO_OFFSET, handle_logging_context_level},
		{"/contexts/storage", NO_OFFSET, handle_logging_context_level},
		{"/contexts/truncate", NO_OFFSET, handle_logging_context_level},
		{"/contexts/tsvc", NO_OFFSET, handle_logging_context_level},
		{"/contexts/udf", NO_OFFSET, handle_logging_context_level},
		{"/contexts/xdr", NO_OFFSET, handle_logging_context_level},
		{"/contexts/xdr-client", NO_OFFSET, handle_logging_context_level},
		{"/contexts/masking", NO_OFFSET, handle_logging_context_level},
	};


//==========================================================
// Public API.
//

	void
	apply_config(as_config* config, const nlohmann::json& source)
	{
		for (const auto& desc : TOP_LEVEL_CONTEXT_DESCRIPTORS) {
			apply_field(config, source, desc);
		}
	}

//==========================================================
// Local helpers.
//

	static bool
	get_json_value(const std::string& path, const nlohmann::json& source,
			nlohmann::json& result)
	{
		try {
			result = source.at(nlohmann::json::json_pointer(path));
			return true;
		} catch (const nlohmann::json::exception&) {
			return false;
		}
	}

	static bool
	is_community_edition()
	{
		// In community edition, as_error_enterprise_only() returns true.
		// In enterprise edition, it would return false (but this function
		// doesn't exist in EE).
		return as_error_enterprise_only();
	}


	static void
	apply_field(void* target, const nlohmann::json& source,
			const FieldDescriptor& desc)
	{
		nlohmann::json value;

		// Skip if field is not present (optional fields)
		if (! get_json_value(desc.json_path, source, value)) {
			// Field not found - this is okay for optional fields
			return;
		}

		// Check if this is an enterprise-only field in community edition
		if (desc.enterprise_only && is_community_edition()) {
			throw config_error(desc.json_path, "is enterprise-only");
		}

		if (! desc.deprecation_warning.empty()) {
			as_info_warn_deprecated(desc.deprecation_warning.c_str());
		}

		// If this field supports units (e.g. seconds, mibibytes, etc),
		// accept the new schema's object form:
		//   { "value": <int>, "unit": "<suffix>" }
		// and expand it to the base-unit integer the existing handlers expect.
		if (desc.unit_type != UnitType::NONE) {
			uint64_t expanded;
			if (try_expand_unit_value(desc, value, expanded)) {
				value = expanded;
			}
		}

		desc.handler(target, desc, value);
	}

	// Unit expansion for schema object form: {value, unit}
	//
	// Returns true if 'in' was recognized as a unit-bearing representation and
	// successfully expanded into 'out'. Throws config_error on malformed unit
	// objects/strings for unit-capable fields.
	//
	static bool
	try_expand_unit_value(const FieldDescriptor& desc, const nlohmann::json& in,
			uint64_t& out)
	{
		if (desc.unit_type == UnitType::NONE) {
			return false;
		}

		// value might be a unit-bearing object: {"value": <int>, "unit": "<suffix>"}.
		if (in.is_object()) {
			if (! in.contains("value") || ! in.contains("unit")) {
				// Not our object form - let the specific handler validate.
				return false;
			}

			const nlohmann::json& v = in.at("value");
			const nlohmann::json& u = in.at("unit");

			if (! (v.is_number_integer() || v.is_number_unsigned())) {
				throw config_error(desc.json_path, "unit object 'value' must be an integer");
			}
			if (! u.is_string()) {
				throw config_error(desc.json_path, "unit object 'unit' must be a string");
			}

			// Treat negative integers as invalid (schema minimums are almost always non-negative).
			int64_t v_i = v.get<int64_t>();
			if (v_i < 0) {
				throw config_error(desc.json_path, "unit object 'value' must be non-negative");
			}

			std::string suffix = u.get<std::string>();
			if (suffix.empty()) {
				throw config_error(desc.json_path, "unit object 'unit' must be non-empty");
			}

			std::string combined = std::to_string(static_cast<uint64_t>(v_i)) + suffix;

			switch (desc.unit_type) {
				case UnitType::TIME_DURATION: {
					uint32_t seconds;
					if (cf_str_atoi_seconds(combined.c_str(), &seconds) != 0) {
						throw config_error(desc.json_path,
								"invalid time unit object (expected e.g. {value: 1, unit: s|m|h|d})");
					}
					out = seconds;
					return true;
				}
				case UnitType::SIZE_U32:
				case UnitType::SIZE_U64: {
					uint64_t size;
					if (cf_str_atoi_size(combined.c_str(), &size) != 0) {
						throw config_error(desc.json_path,
								"invalid size unit object (expected e.g. {value: 1, unit: k|m|g|t|p|ki|mi|gi|ti|pi})");
					}
					out = size;
					return true;
				}
				case UnitType::NONE:
				default:
					return false;
			}
		}

		return false;
	}

	static void
	apply_uint16_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		uint64_t val;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			val = value.get<uint64_t>();
		}
		else {
			throw config_error(desc.json_path, "must be a positive integer");
		}

		if (val > std::numeric_limits<uint16_t>::max()) {
			throw config_error(desc.json_path, "value too large for uint16_t");
		}

		uint16_t* field_ptr = reinterpret_cast<uint16_t*>(
				static_cast<char*>(target) + desc.offset);
		*field_ptr = static_cast<uint16_t>(val);
	}

	static void
	apply_pct_w_minus_1_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_number_unsigned() && ! value.is_number_integer()) {
			throw config_error(desc.json_path, "must be an integer");
		}

		int32_t val = value.get<int32_t>();

		if (val > 100 || val < -1) {
			throw config_error(desc.json_path,
					"value must be between 0 and 100 or -1");
		}

		uint32_t* field_ptr = reinterpret_cast<uint32_t*>(
				static_cast<char*>(target) + desc.offset);
		*field_ptr = static_cast<uint32_t>(val);
	}

	static void
	apply_uint32_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		uint64_t val;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			val = value.get<uint64_t>();
		}
		else {
			throw config_error(desc.json_path, "must be a positive integer");
		}

		if (val > std::numeric_limits<uint32_t>::max()) {
			throw config_error(desc.json_path, "value too large for uint32_t");
		}

		uint32_t* field_ptr = reinterpret_cast<uint32_t*>(
				static_cast<char*>(target) + desc.offset);
		*field_ptr = static_cast<uint32_t>(val);
	}

	static void
	apply_uint64_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		uint64_t val;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			val = value.get<uint64_t>();
		}
		else {
			throw config_error(desc.json_path, "must be a positive integer");
		}

		uint64_t* field_ptr = reinterpret_cast<uint64_t*>(
				static_cast<char*>(target) + desc.offset);
		*field_ptr = val;
	}

	static void
	apply_bool_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_boolean()) {
			throw config_error(desc.json_path, "must be a boolean");
		}

		bool* field_ptr = reinterpret_cast<bool*>(
				static_cast<char*>(target) + desc.offset);
		*field_ptr = value.get<bool>();
	}

	static void
	apply_cstring_field(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error(desc.json_path, "must be a string");
		}

		std::string str_val = value.get<std::string>();

		// For C strings, we need to allocate memory and copy the string.
		// This assumes the target field is a char* that should be allocated.
		char** field_ptr = reinterpret_cast<char**>(
				static_cast<char*>(target) + desc.offset);

		// Free existing string if any
		if (*field_ptr != NULL) {
			cf_free(*field_ptr);
		}

		// Allocate and copy new string.
		*field_ptr = cf_strdup(str_val.c_str());
	}

	//------------------------------------------------
	// Mod Lua Handlers.
	//

	static void
	handle_mod_lua(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : MOD_LUA_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_mod_lua_user_path(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_string()) {
			throw config_error("/mod-lua/user-path", "must be a string");
		}

		std::string str_val = value.get<std::string>();

		if (str_val.length() >= sizeof(config->mod_lua.user_path)) {
			throw config_error("/mod-lua/user-path",
					"string too long (max " +
					std::to_string(sizeof(config->mod_lua.user_path) - 1) +
					" characters)");
		}

		strcpy(config->mod_lua.user_path, str_val.c_str());
	}

	//------------------------------------------------
	// Service Handlers.
	//

	static void
	handle_service(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : SERVICE_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_user(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_string()) {
			throw config_error("/service/user", "must be a string");
		}

		{
			as_info_warn_deprecated("'user' is deprecated");

			struct passwd* pwd;

			if (NULL == (pwd = getpwnam(value.get<std::string>().c_str()))) {
				throw config_error("/service/user",
						"user not found: " + value.get<std::string>());
			}

			config->uid = pwd->pw_uid;
			endpwent();
		}
	}

	static void
	handle_tls_refresh_period(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		uint32_t resolved_val;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			resolved_val = value.get<uint32_t>();
		}
		else {
			throw config_error("/service/tls-refresh-period",
					"must be a positive integer");
		}

		tls_set_refresh_period(resolved_val);
	}

	static void
	handle_secret_address_port(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/secret-address-port",
					"must be a string");
		}

		// format is "host:port:tls_name"
		// host and port are required, tls_name is optional
		std::string secrets_string = value.get<std::string>();
		std::stringstream ss(secrets_string);
		std::string host, port_str, tls_name;

		std::getline(ss, host, ':');
		std::getline(ss, port_str, ':');
		std::getline(ss, tls_name, ':');

		if ( host.empty() || port_str.empty()) {
			throw config_error("/service/secret-address-port",
					"invalid address: " + secrets_string +
					" (expected 'host:port[:tls_name]')");
		}

		char* host_dup = cf_strdup(host.c_str());
		char* port_dup = cf_strdup(port_str.c_str());
		char* tls_name_dup = tls_name.empty() ?
				NULL : cf_strdup(tls_name.c_str());

		cfg_add_secrets_addr_port(host_dup, port_dup, tls_name_dup);
	}

	static void
	handle_secret_tls_context(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/secret-tls-context",
				"must be a string");
		}

		char* tls_context_dup = cf_strdup(value.get<std::string>().c_str());
		g_secrets_cfg.tls_context = tls_context_dup;
	}

	static void
	handle_secret_uds_path(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/secret-uds-path", "must be a string");
		}

		char* uds_path_dup = cf_strdup(value.get<std::string>().c_str());
		g_secrets_cfg.uds_path = uds_path_dup;
	}

	static void
	handle_node_id(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/node-id", "must be a string");
		}

		as_config* config = static_cast<as_config*>(target);

		// node-id is a hex string
		if (0 != cf_strtoul_x64(value.get<std::string>().c_str(),
				&config->self_node)) {
			throw config_error("/service/node-id",
					"failed to parse node-id as hex string");
		}
	}

	static void
	handle_os_group_perms(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_boolean()) {
			throw config_error("/service/os-group-perms", "must be a boolean");
		}

		cf_os_use_group_perms(value.get<bool>());
	}

	static void
	handle_log_milliseconds(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_boolean()) {
			throw config_error("/service/log-milliseconds",
					"must be a boolean");
		}

		cf_log_use_millis(value.get<bool>());
	}

	static void
	handle_log_local_time(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_boolean()) {
			throw config_error("/service/log-local-time", "must be a boolean");
		}

		cf_log_use_local_time(value.get<bool>());
	}

	static void
	handle_group(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_info_warn_deprecated("'group' is deprecated");

		if (! value.is_string()) {
			throw config_error("/service/group", "must be a string");
		}

		struct group* grp;

		if (NULL == (grp = getgrnam(value.get<std::string>().c_str()))) {
			throw config_error("/service/group",
					"group not found: " + value.get<std::string>());
		}

		as_config* config = static_cast<as_config*>(target);
		config->gid = grp->gr_gid;
		endgrent();
	}

	static void
	handle_info_max_ms(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		uint64_t info_max_ms;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			info_max_ms = value.get<uint64_t>();
		}
		else {
			throw config_error("/service/info-max-ms",
					"must be a positive integer or an object with 'value' and 'unit' properties");
		}

		if (info_max_ms > MAX_INFO_MAX_MS) {
			throw config_error("/service/info-max-ms",
					"value must be less than " +
					std::to_string(MAX_INFO_MAX_MS) +
					" milliseconds");
		}

		as_config* config = static_cast<as_config*>(target);
		config->info_max_ns = info_max_ms * 1000000;
	}

	static void
	handle_feature_key_files(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/service/feature-key-files",
					"must be an array of strings");
		}

		for (const auto& item : value) {
			if (! item.is_string()) {
				throw config_error("/service/feature-key-files",
						"must be an array of strings");
			}

			// cfg_add_feature_key_file does NOT strdup
			// it stores the pointer directly
			// so we must strdup to ensure the string outlives the temporary.
			const char* path_copy = cf_strdup(item.get<std::string>().c_str());
			cfg_add_feature_key_file(path_copy);
		}
	}

	static void
	handle_feature_key_file(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/feature-key-file",
					"must be a string");
		}

		// cfg_add_feature_key_file does NOT strdup
		// it stores the pointer directly
		// so we must strdup to ensure the string outlives the temporary.
		const char* path_copy = cf_strdup(value.get<std::string>().c_str());
		cfg_add_feature_key_file(path_copy);
	}

	static void
	handle_auto_pin(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_string()) {
			throw config_error("/service/auto-pin", "must be a string");
		}

		std::string auto_pin = value.get<std::string>();

		if (auto_pin == "none") {
			config->auto_pin = CF_TOPO_AUTO_PIN_NONE;
		}
		else if (auto_pin == "cpu") {
			config->auto_pin = CF_TOPO_AUTO_PIN_CPU;
		}
		else if (auto_pin == "numa") {
			config->auto_pin = CF_TOPO_AUTO_PIN_NUMA;
		}
		else if (auto_pin == "adq") {
			as_info_warn_deprecated("'auto-pin-adq' is deprecated");
			config->auto_pin = CF_TOPO_AUTO_PIN_ADQ;
		}
		else {
			throw config_error("/service/auto-pin", "invalid value: "
					+ auto_pin);
		}
	}

	static void
	handle_advertise_ipv6(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_boolean()) {
			throw config_error("/service/advertise-ipv6", "must be a boolean");
		}

		cf_socket_set_advertise_ipv6(value.get<bool>());
	}

	static void
	handle_cluster_name(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_string()) {
			throw config_error("/service/cluster-name", "must be a string");
		}

		std::string cluster_name = value.get<std::string>();

		if (cluster_name.length() >= AS_CLUSTER_NAME_SZ) {
			throw config_error("/service/cluster-name",
					"string too long (max " +
					std::to_string(AS_CLUSTER_NAME_SZ - 1) + " characters)");
		}

		as_config* config = static_cast<as_config*>(target);
		std::strncpy(config->cluster_name, cluster_name.c_str(),
				AS_CLUSTER_NAME_SZ - 1);
		config->cluster_name[AS_CLUSTER_NAME_SZ - 1] = '\0';
	}

	static void
	apply_network_tls_context(std::string name, const nlohmann::json& tls_json,
			as_config* config)
	{
		if (! tls_json.is_object()) {
			throw config_error("/network/tls/" + name,
					"must be an object");
		}

		if (name.empty()) {
			throw config_error("/network/tls/" + name,
					"name must be a non-empty string");
		}

		auto tls_spec = cfg_create_tls_spec(config, name.c_str());

		for (const auto& desc : NETWORK_TLS_FIELD_DESCRIPTORS) {
			apply_field(tls_spec, tls_json, desc);
		}
	}

	//------------------------------------------------
	// Namespace Handlers.
	//

	static void
	apply_namespace(std::string name, const nlohmann::json& namespace_json)
	{
		if (! namespace_json.is_object()) {
			throw config_error("/namespaces/" + name,
					"must be an object");
		}

		auto namespace_struct = as_namespace_create(name.c_str());

		for (const auto& desc : NAMESPACE_FIELD_DESCRIPTORS) {
			apply_field(namespace_struct, namespace_json, desc);
		}
	}

	static void
	handle_namespaces(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_object()) {
			throw config_error("/namespaces", "must be an object");
		}

		// rely on config being initialized to 0
		// config->n_namespaces = 0;

		for (auto& el: value.items()) {
			apply_namespace(el.key(), el.value());
		}
	}

	static void
	handle_namespace_write_commit_level_override(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/write-commit-level-override",
					"must be a string");
		}

		std::string write_commit_level_override = value.get<std::string>();

		if (write_commit_level_override == "off") {
			namespace_struct->write_commit_level = AS_WRITE_COMMIT_LEVEL_PROTO;
		}
		else if (write_commit_level_override == "master") {
			namespace_struct->write_commit_level = AS_WRITE_COMMIT_LEVEL_MASTER;
		}
		else if (write_commit_level_override == "all") {
			namespace_struct->write_commit_level = AS_WRITE_COMMIT_LEVEL_ALL;
		}
		else {
			throw config_error("/namespaces/write-commit-level-override",
					"invalid value: " + write_commit_level_override);
		}
	}

	static void
	handle_namespace_xdr_bin_tombstone_ttl(void* ns,
			const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		uint32_t ttl;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			ttl = value.get<uint32_t>();
		}
		else {
			throw config_error("/namespaces/xdr-bin-tombstone-ttl",
					"must be a positive integer");
		}

		if (ttl > MAX_ALLOWED_TTL) {
			throw config_error("/namespaces/xdr-bin-tombstone-ttl",
					"value must be less than " +
					std::to_string(MAX_ALLOWED_TTL) +
					" seconds");
		}

		namespace_struct->xdr_bin_tombstone_ttl_ms = ttl * 1000;
	}

	static void
	handle_namespace_read_consistency_level_override(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/read-consistency-level-override",
					"must be a string");
		}

		std::string read_consistency_level_override = value.get<std::string>();

		if (read_consistency_level_override == "off") {
			namespace_struct->read_consistency_level =
					AS_READ_CONSISTENCY_LEVEL_PROTO;
		}
		else if (read_consistency_level_override == "one") {
			namespace_struct->read_consistency_level =
					AS_READ_CONSISTENCY_LEVEL_ONE;
		}
		else if (read_consistency_level_override == "all") {
			namespace_struct->read_consistency_level =
					AS_READ_CONSISTENCY_LEVEL_ALL;
		}
		else {
			throw config_error("/namespaces/read-consistency-level-override",
					"invalid value: " + read_consistency_level_override);
		}
	}

	static void
	handle_namespace_conflict_resolution_policy(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/conflict-resolution-policy",
					"must be a string");
		}

		std::string policy = value.get<std::string>();

		if (policy == "generation") {
			namespace_struct->conflict_resolution_policy =
					AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
		}
		else if (policy == "last-update-time") {
			namespace_struct->conflict_resolution_policy =
					AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
		}
		else {
			throw config_error("/namespaces/conflict-resolution-policy",
					"invalid value: " + policy);
		}
	}

	//------------------------------------------------
	// Namespace Sindex-Type Handlers.
	//

	static void
	handle_namespace_sindex_mounts(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_array()) {
			throw config_error("/namespaces/sindex-type/mounts",
					"must be an array");
		}

		for (const auto& mount : value) {
			if (! mount.is_string()) {
				throw config_error("/namespaces/sindex-type/mounts",
						"entries must be a string");
			}

			// cfg_add_si_xmem_mount does NOT strdup
			// it stores the pointer directly.
			const char* mount_str = cf_strdup(mount.get<std::string>().c_str());
			cfg_add_si_xmem_mount(namespace_struct, mount_str);
		}
	}

	static void
	handle_namespace_sindex_type(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/sindex-type/type",
					"must be a string");
		}

		std::string sindex_type = value.get<std::string>();

		if (sindex_type == "shmem") {
			namespace_struct->si_xmem_type = CF_XMEM_TYPE_SHMEM;
		}
		else if (sindex_type == "pmem") {
			namespace_struct->si_xmem_type = CF_XMEM_TYPE_PMEM;
		}
		else if (sindex_type == "flash") {
			namespace_struct->si_xmem_type = CF_XMEM_TYPE_FLASH;
		}
		else {
			throw config_error("/namespaces/sindex-type/type",
					"invalid value: " + sindex_type);
		}
	}

	//------------------------------------------------
	// Namespace Index-Type Handlers.
	//

	static void
	handle_namespace_index_mounts(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_array()) {
			throw config_error("/namespaces/index-type/mounts",
					"must be an array");
		}

		for (const auto& mount : value) {
			if (! mount.is_string()) {
				throw config_error("/namespaces/index-type/mounts",
						"entries must be a string");
			}

			// cfg_add_pi_xmem_mount does NOT strdup
			// it stores the pointer directly.
			const char* mount_str = cf_strdup(mount.get<std::string>().c_str());
			cfg_add_pi_xmem_mount(namespace_struct, mount_str);
		}
	}

	static void
	handle_namespace_index_type(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/index-type/type",
					"must be a string");
		}

		std::string index_type = value.get<std::string>();

		if (index_type == "shmem") {
			namespace_struct->pi_xmem_type = CF_XMEM_TYPE_SHMEM;
		}
		else if (index_type == "pmem") {
			namespace_struct->pi_xmem_type = CF_XMEM_TYPE_PMEM;
		}
		else if (index_type == "flash") {
			namespace_struct->pi_xmem_type = CF_XMEM_TYPE_FLASH;
		}
		else {
			throw config_error("/namespaces/index-type/type",
					"invalid value: " + index_type);
		}
	}

	//------------------------------------------------
	// Namespace Set Handlers.
	//

	static void
	apply_namespace_set(const std::string& name, const nlohmann::json& set_json,
			as_namespace* namespace_struct)
	{
		if (! set_json.is_object()) {
			throw config_error("/namespaces/sets", "set must be an object");
		}

		if (namespace_struct == NULL) {
			throw config_error("/namespaces/sets", "namespace struct is null");
		}

		as_set* set_struct = cfg_add_set(namespace_struct);

		if (name.empty()) {
			throw config_error("namespaces/sets/",
					"name must be a non-empty string");
		}

		if (name.size() > AS_SET_NAME_MAX_SIZE) {
			throw config_error("namespaces/sets/" + name,
					"name must be less than " +
					std::to_string(AS_SET_NAME_MAX_SIZE) + " characters");
		}

		strcpy(set_struct->name, name.c_str());

		for (const auto& desc : NAMESPACE_SET_FIELD_DESCRIPTORS) {
			apply_field(set_struct, set_json, desc);
		}
	}

	static void
	handle_namespace_sets(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_object()) {
			throw config_error("/namespaces/sets", "must be an object");
		}

		// NOTE: this function relies on namespace_struct->sets_cfg_count
		// and namespace_struct->sets_cfg_array being initialized to 0 and NULL

		for (auto& el: value.items()) {
			apply_namespace_set(el.key(), el.value(), namespace_struct);
		}
	}

	//------------------------------------------------
	// Namespace Storage-Engine Handlers.
	//

	static void
	handle_namespace_storage_engine_type(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/storage-engine/type",
					"must be a string");
		}

		if (namespace_struct->storage_type != AS_STORAGE_ENGINE_UNDEFINED) {
			throw config_error("/namespaces/storage-engine/type",
					"can only configure one 'storage-engine'");
		}

		std::string storage_engine_type = value.get<std::string>();

		if (storage_engine_type == "memory") {
			namespace_struct->storage_type = AS_STORAGE_ENGINE_MEMORY;
			// Override non-0 default for info purposes.
			namespace_struct->storage_post_write_cache = 0;
		}
		else if (storage_engine_type == "pmem") {
			namespace_struct->storage_type = AS_STORAGE_ENGINE_PMEM;
			// Override non-0 default for info purposes.
			namespace_struct->storage_post_write_cache = 0;
		}
		else if (storage_engine_type == "device") {
			namespace_struct->storage_type = AS_STORAGE_ENGINE_SSD;
			namespace_struct->storage_flush_size = 0;
		}
		else {
			throw config_error("/namespaces/storage-engine/type",
					"invalid value: " + storage_engine_type);
		}
	}

	static void
	handle_namespace_storage_engine_compression(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/storage-engine/compression",
					"must be a string");
		}

		std::string compression_type = value.get<std::string>();

		if (compression_type == "none") {
			namespace_struct->storage_compression = AS_COMPRESSION_NONE;
		}
		else if (compression_type == "lz4") {
			namespace_struct->storage_compression = AS_COMPRESSION_LZ4;
		}
		else if (compression_type == "snappy") {
			namespace_struct->storage_compression = AS_COMPRESSION_SNAPPY;
		}
		else if (compression_type == "zstd") {
			namespace_struct->storage_compression = AS_COMPRESSION_ZSTD;
		}
		else {
			throw config_error("/namespaces/storage-engine/compression",
					"invalid value: " + compression_type);
		}
	}

	static void
	handle_namespace_storage_engine_devices(void* ns,
			const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_array()) {
			throw config_error("/namespaces/storage-engine/devices",
					"must be an array");
		}

		for (const auto& device : value) {
			if (! device.is_string()) {
				throw config_error("/namespaces/storage-engine/devices",
						"entries must be a string");
			}

			// format is "device_name[:shadow_name]"
			std::string device_str = device.get<std::string>();
			std::string device_name, shadow_name;
			device_name = device_str.substr(0, device_str.find(':'));
			shadow_name = device_str.substr(device_str.find(':') + 1);

			// cfg_add_storage_device does NOT strdup
			// it stores the pointer directly.
			const char* device_name_cpy = cf_strdup(device_name.c_str());
			const char* shadow_name_cpy =
					shadow_name.empty() ? NULL : cf_strdup(shadow_name.c_str());

			cfg_add_storage_device(namespace_struct,
					device_name_cpy, shadow_name_cpy);
		}
	}

	static void
	handle_namespace_storage_engine_encryption(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_string()) {
			throw config_error("/namespaces/storage-engine/encryption",
					"must be a string");
		}

		std::string encryption_type = value.get<std::string>();

		if (encryption_type == "aes-128") {
			namespace_struct->storage_encryption = AS_ENCRYPTION_AES_128;
		}
		else if (encryption_type == "aes-256") {
			namespace_struct->storage_encryption = AS_ENCRYPTION_AES_256;
		}
		else {
			throw config_error("/namespaces/storage-engine/encryption",
					"invalid value: " + encryption_type);
		}
	}

	static void
	handle_namespace_storage_engine_files(void* ns, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_array()) {
			throw config_error("/namespaces/storage-engine/files",
					"must be an array");
		}

		for (const auto& file : value) {
			if (! file.is_string()) {
				throw config_error("/namespaces/storage-engine/files",
						"entries must be a string");
			}

			// The format is "file_name[:shadow_name]".
			std::string file_str = file.get<std::string>();
			std::string file_name, shadow_name;
			file_name = file_str.substr(0, file_str.find(':'));
			shadow_name = file_str.substr(file_str.find(':') + 1);

			// Pointer is stored directly by cfg_add_storage_file,
			// which does NOT strdup.
			const char* file_name_cpy = cf_strdup(file_name.c_str());
			const char* shadow_name_cpy = shadow_name.empty() ?
					NULL : cf_strdup(shadow_name.c_str());

			cfg_add_storage_file(namespace_struct, file_name_cpy,
					shadow_name_cpy);
		}
	}

	static void
	handle_namespace_storage_engine_flush_max_ms(void* ns,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_namespace* namespace_struct = static_cast<as_namespace*>(ns);

		if (! value.is_number_unsigned() && ! value.is_number_integer()) {
			throw config_error("/namespaces/storage-engine/flush-max-ms",
					"must be a positive integer");
		}

		// Convert from milliseconds to microseconds as stored in the struct.
		namespace_struct->storage_flush_max_us = value.get<uint64_t>() * 1000;
	}

	//------------------------------------------------
	// Network Handlers.
	//

	static void
	handle_network(void* target, const FieldDescriptor& desc,
			const nlohmann::json& source)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : NETWORK_FIELD_DESCRIPTORS) {
			try {
				apply_field(config, source, desc);
			}
			catch (const std::exception& e) {
				throw config_error("/network", "error applying field: " +
						std::string(e.what()));
			}
		}
	}

	//------------------------------------------------
	// Network Admin Handlers.
	//

	static void
	handle_network_admin(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : NETWORK_ADMIN_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_network_admin_addresses(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/admin/addresses", "must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/admin/addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->admin);
		}
	}

	static void
	handle_network_admin_tls_addresses(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/admin/tls-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/admin/tls-addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->tls_admin);
		}
	}

	static void
	handle_network_admin_tls_authenticate_client(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (value.is_string()) {
			// add_tls_peer_name copies its input so no need to strdup here.
			std::string address_str = value.get<std::string>();
			add_tls_peer_name(address_str.c_str(), &config->tls_admin);
		}
		else if (value.is_array()) {
			for (const auto& address : value) {
				if (! address.is_string()) {
					throw config_error("/network/admin/tls-authenticate-client",
							"entries must be a string");
				}

				// add_tls_peer_name copies its input so no need to strdup here.
				std::string address_str = address.get<std::string>();
				add_tls_peer_name(address_str.c_str(), &config->tls_admin);
			}
		}
		else {
			throw config_error("/network/admin/tls-authenticate-client",
					"must be a string or array");
		}
	}

	//------------------------------------------------
	// Network Heartbeat Handlers.
	//

	static void
	handle_network_heartbeat(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : NETWORK_HEARTBEAT_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_network_heartbeat_mode(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_string()) {
			throw config_error("/network/heartbeat/mode", "must be a string");
		}

		std::string mode = value.get<std::string>();

		if (mode == "mesh") {
			config->hb_config.mode = AS_HB_MODE_MESH;
		}
		else if (mode == "multicast") {
			as_info_warn_deprecated("'multicast' is deprecated");
			config->hb_config.mode = AS_HB_MODE_MULTICAST;
		}
		else {
			throw config_error("/network/heartbeat/mode",
					"invalid value: " + mode);
		}
	}

	static void
	handle_network_heartbeat_protocol(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_string()) {
			throw config_error("/network/heartbeat/protocol",
					"must be a string");
		}

		std::string protocol = value.get<std::string>();

		if (protocol == "none") {
			config->hb_config.protocol = AS_HB_PROTOCOL_NONE;
		}
		else if (protocol == "v3") {
			config->hb_config.protocol = AS_HB_PROTOCOL_V3;
		}
		else {
			throw config_error("/network/heartbeat/protocol",
					"invalid value: " + protocol);
		}
	}

	static void
	handle_network_heartbeat_addresses(void* target,
			const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/heartbeat/addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/heartbeat/addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->hb_serv_spec);
		}
	}

	static void
	handle_network_heartbeat_mesh_seed_address_ports(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/network/heartbeat/mesh-seed-address-ports",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/heartbeat/mesh-seed-address-ports",
						"entries must be a string");
			}

			// these addresses come in the format "hostname:port"
			// so we split on the colon and get the port
			std::string addr_str = address.get<std::string>();
			std::stringstream ss(addr_str);
			std::string host, port_str;

			std::getline(ss, host, ':');
			std::getline(ss, port_str, ':');

			if (host.empty() || port_str.empty()) {
				throw config_error("/network/heartbeat/mesh-seed-address-ports",
						"invalid address: " + addr_str +
						" (expected 'host:port')");
			}

			uint16_t port = atoi(port_str.c_str());
			// cfg_add_mesh_seed_addr_port takes ownership of the host string
			// and does not copy it
			char* host_dup = cf_strdup(host.c_str());
			cfg_add_mesh_seed_addr_port(host_dup, port, false);
		}
	}

	static void
	handle_network_heartbeat_multicast_groups(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/heartbeat/multicast-groups",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/heartbeat/multicast-groups",
						"entries must be a string");
			}

			// cfg_add_addr_alt copies its input so no need to strdup here
			std::string address_str = address.get<std::string>();
			add_addr(address_str.c_str(), &config->hb_multicast_groups);
		}
	}

	static void
	handle_network_heartbeat_tls_addresses(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/heartbeat/tls-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/heartbeat/tls-addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->hb_tls_serv_spec);
		}
	}

	static void
	handle_network_heartbeat_tls_mesh_seed_address_ports(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/network/heartbeat/tls-mesh-seed-address-ports",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error(
						"/network/heartbeat/tls-mesh-seed-address-ports",
						"entries must be a string");
			}

			// these addresses come in the format "hostname:port"
			// so we split on the colon and get the port
			std::string addr_str = address.get<std::string>();
			std::stringstream ss(addr_str);
			std::string host, port_str;

			std::getline(ss, host, ':');
			std::getline(ss, port_str, ':');

			if (host.empty() || port_str.empty()) {
				throw config_error(
						"/network/heartbeat/tls-mesh-seed-address-ports",
						"invalid address: " + addr_str +
						" (expected 'host:port')");
			}

			uint16_t port = atoi(port_str.c_str());
			// cfg_add_mesh_seed_addr_port takes ownership of the host string
			// and frees it internally, so we need to strdup it
			char* host_dup = cf_strdup(host.c_str());
			cfg_add_mesh_seed_addr_port(host_dup, port, true);
		}
	}

	//------------------------------------------------
	// Network Service Handlers.
	//

	static void
	handle_network_service(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		for (const auto& desc : NETWORK_SERVICE_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_network_service_access_addresses(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/service/access-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/service/access-addresses",
						"entries must be a string");
			}

			// cfg_add_addr_std copies its input so no need to strdup here
			std::string address_str = address.get<std::string>();
			cfg_add_addr_std(address_str.c_str(), &config->service);
		}
	}

	static void
	handle_network_service_addresses(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/service/addresses",
					"must be an array of strings");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/service/addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->service);
		}
	}

	static void
	handle_network_service_alternate_access_addresses(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/service/alternate-access-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error(
						"/network/service/alternate-access-addresses",
						"entries must be a string");
			}

			// cfg_add_addr_alt copies its input so no need to strdup here.
			std::string address_str = address.get<std::string>();
			cfg_add_addr_alt(address_str.c_str(), &config->service);
		}
	}

	static void
	handle_network_service_tls_access_addresses(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/service/tls-access-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/service/tls-access-addresses",
						"entries must be a string");
			}

			// cfg_add_addr_std copies its input so no need to strdup here.
			std::string address_str = address.get<std::string>();
			cfg_add_addr_std(address_str.c_str(), &config->tls_service);
		}
	}

	static void
	handle_network_service_tls_addresses(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/service/tls-addresses",
					"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/service/tls-addresses",
						"entries must be a string");
			}

			// cfg_add_addr_bind copies its input so no need to strdup here.
			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->tls_service);
		}
	}

	void
	handle_network_service_tls_alternate_access_addresses(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error(
				"/network/service/tls-alternate-access-addresses",
				"must be an array of strings");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error(
					"/network/service/tls-alternate-access-addresses",
					"entries must be a string");
			}

			// cfg_add_addr_alt copies its input so no need to strdup here.
			std::string address_str = address.get<std::string>();
			cfg_add_addr_alt(address_str.c_str(), &config->tls_service);
		}
	}

	static void
	handle_network_service_tls_authenticate_client(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (value.is_string()) {
			// add_tls_peer_name copies its input so no need to strdup here.
			std::string address_str = value.get<std::string>();
			add_tls_peer_name(address_str.c_str(), &config->tls_service);
		}
		else if (value.is_array()) {
			for (const auto& address : value) {
				if (! address.is_string()) {
					throw config_error(
						"/network/service/tls-authenticate-client",
						"entries must be a string");
				}

				// add_tls_peer_name copies its input so no need to strdup here.
				std::string address_str = address.get<std::string>();
				add_tls_peer_name(address_str.c_str(), &config->tls_service);
			}
		}
		else {
			throw config_error("/network/service/tls-authenticate-client",
				"must be a string or array");
		}
	}

	//------------------------------------------------
	// Network Fabric Handlers.
	//

	static void
	handle_network_fabric(void* target, const FieldDescriptor& desc,
				const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);
		for (const auto& desc : NETWORK_FABRIC_FIELD_DESCRIPTORS) {
			apply_field(config, value, desc);
		}
	}

	static void
	handle_network_fabric_addresses(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/fabric/addresses", "must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/fabric/addresses",
						"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->fabric);
		}
	}

	static void
	handle_network_fabric_tls_addresses(void* target,
				const FieldDescriptor& desc, const nlohmann::json& value) {
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_array()) {
			throw config_error("/network/fabric/tls-addresses",
				"must be an array");
		}

		for (const auto& address : value) {
			if (! address.is_string()) {
				throw config_error("/network/fabric/tls-addresses",
					"entries must be a string");
			}

			std::string address_str = address.get<std::string>();
			cfg_add_addr_bind(address_str.c_str(), &config->tls_fabric);
		}
	}

	//------------------------------------------------
	// Network TLS Handlers.
	//

	static void
	handle_network_tls(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_config* config = static_cast<as_config*>(target);

		if (! value.is_object()) {
			throw config_error("/network/tls",
					"must be an object containing TLS context");
		}

		for (auto& el: value.items()) {
			apply_network_tls_context(el.key(), el.value(), config);
		}
	}

	//------------------------------------------------
	// XDR Handlers.
	//

	static void
	apply_xdr_dc(const std::string& name, const nlohmann::json& dc_json,
			as_config* config)
	{
		if (! dc_json.is_object()) {
			throw config_error("/xdr/dc/" + name,
					"dc must be an object");
		}

		auto dc_cfg = as_xdr_startup_create_dc(name.c_str());

		for (const auto& desc : XDR_DC_FIELD_DESCRIPTORS) {
			apply_field(dc_cfg, dc_json, desc);
		}
	}

	static void
	handle_xdr(void* target, const FieldDescriptor& desc,
			const nlohmann::json& source)
	{
		if (is_community_edition()) {
			throw config_error("/xdr", "is enterprise-only");
		}

		as_config* config = static_cast<as_config*>(target);

		if (! source.is_object()) {
			throw config_error("/xdr", "must be an object");
		}

		// TODO: handle this and similar fields with
		// field descriptors if possible.
		nlohmann::json src_id_value;

		if (get_json_value("/src-id", source, src_id_value)) {
			if (! src_id_value.is_number_unsigned() &&
					! src_id_value.is_number_integer()) {
				throw config_error("/xdr/src-id", "must be a positive integer");
			}

			uint64_t val = src_id_value.get<uint64_t>();

			if (val < 1 || val > 255) {
				throw config_error("/xdr/src-id", "must be between 1 and 255");
			}

			config->xdr_cfg.src_id = static_cast<uint8_t>(val);
		}

		// Handle DC contexts.
		nlohmann::json dc_value;

		if (get_json_value("/dcs", source, dc_value)) {
			if (! dc_value.is_object()) {
				throw config_error("/xdr/dcs", "must be an object");
			}

			for (auto& el: dc_value.items()) {
				apply_xdr_dc(el.key(), el.value(), config);
			}
		}
	}

	//------------------------------------------------
	// XDR DC Handlers.
	//

	static void
	handle_xdr_dc_auth_mode(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_cfg* dc_cfg = static_cast<as_xdr_dc_cfg*>(target);

		if (! value.is_string()) {
			throw config_error("/xdr/dc/auth-mode", "must be a string");
		}

		std::string auth_mode = value.get<std::string>();

		if (auth_mode == "none") {
			dc_cfg->auth_mode = XDR_AUTH_NONE;
		}
		else if (auth_mode == "internal") {
			dc_cfg->auth_mode = XDR_AUTH_INTERNAL;
		}
		else if (auth_mode == "external") {
			dc_cfg->auth_mode = XDR_AUTH_EXTERNAL;
		}
		else if (auth_mode == "external-insecure") {
			dc_cfg->auth_mode = XDR_AUTH_EXTERNAL_INSECURE;
		}
		else if (auth_mode == "pki") {
			dc_cfg->auth_mode = XDR_AUTH_PKI;
		}
		else {
			throw config_error("/xdr/dc/auth-mode",
					"invalid value: " + auth_mode);
		}
	}

	static void
	handle_xdr_dc_node_address_ports(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_cfg* dc_cfg = static_cast<as_xdr_dc_cfg*>(target);

		if (! value.is_array()) {
			throw config_error("/xdr/dc/node-address-ports",
					"must be an array of strings");
		}

		for (const auto& address_port : value) {
			if (! address_port.is_string()) {
				throw config_error("/xdr/dc/node-address-ports",
						"entries must be a string");
			}

			// Parse "host:port[:tls_name]" format.
			std::string addr_port_str = address_port.get<std::string>();
			std::stringstream ss(addr_port_str);
			std::string host, port_str, tls_name;

			std::getline(ss, host, ':');
			std::getline(ss, port_str, ':');
			std::getline(ss, tls_name, ':');

			if (host.empty() || port_str.empty()) {
				throw config_error("/xdr/dc/node-address-ports",
						"invalid format: " + addr_port_str +
						" (expected 'host:port[:tls_name]')");
			}

			char* host_dup = cf_strdup(host.c_str());
			char* port_dup = cf_strdup(port_str.c_str());
			// tls_name is optional.
			char* tls_name_dup = tls_name.empty() ? NULL :
					cf_strdup(tls_name.c_str());

			as_xdr_startup_add_seed(dc_cfg, host_dup, port_dup, tls_name_dup);
		}
	}

	static void
	handle_xdr_dc_period_ms(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_cfg* dc_cfg = static_cast<as_xdr_dc_cfg*>(target);

		if (! value.is_number_unsigned() && ! value.is_number_integer()) {
			throw config_error("/xdr/dc/period-ms",
					"must be a positive integer");
		}

		uint32_t period_ms = value.get<uint32_t>();

		if (period_ms < AS_XDR_MIN_PERIOD_MS ||
				period_ms > AS_XDR_MAX_PERIOD_MS) {
			throw config_error("/xdr/dc/period-ms",
					"must be between " + std::to_string(AS_XDR_MIN_PERIOD_MS) +
					" and " + std::to_string(AS_XDR_MAX_PERIOD_MS));
		}

		// Convert milliseconds to microseconds.
		dc_cfg->period_us = period_ms * 1000;
	}

	//------------------------------------------------
	// XDR DC Namespace Handlers.
	//

	static void
	handle_xdr_dc_namespaces(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_cfg* dc_cfg = static_cast<as_xdr_dc_cfg*>(target);

		if (! value.is_object()) {
			throw config_error("/xdr/dc/namespaces", "must be an object");
		}

		for (auto& el: value.items()) {
			apply_xdr_dc_namespace(el.key(), el.value(), dc_cfg);
		}
	}

	static void
	apply_xdr_dc_namespace(const std::string& name,
			const nlohmann::json& dc_ns_json,
			void* dc_cfg_ptr)
	{
		as_xdr_dc_cfg* dc_cfg = static_cast<as_xdr_dc_cfg*>(dc_cfg_ptr);

		if (! dc_ns_json.is_object()) {
			throw config_error("/xdr/dc/namespaces/" + name,
					"must be an object");
		}

		if (name.empty()) {
			throw config_error("/xdr/dc/namespaces/" + name,
					"namespace name must be a non-empty string");
		}

		// as_dc_create_ns_cfg inside as_xdr_startup_create_dc_ns_cfg strdups
		// the ns_name, so no need to strdup here.
		auto dc_ns_cfg = as_xdr_startup_create_dc_ns_cfg(name.c_str());
		cf_vector_append_ptr(dc_cfg->ns_cfg_v, dc_ns_cfg);

		for (const auto& desc : XDR_DC_NS_FIELD_DESCRIPTORS) {
			apply_field(dc_ns_cfg, dc_ns_json, desc);
		}
	}

	static void
	handle_xdr_dc_ns_bin_policy(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_string()) {
			throw config_error("/xdr/dc/namespaces/bin-policy",
					"must be a string");
		}

		std::string bin_policy = value.get<std::string>();

		if (bin_policy == "all") {
			dc_ns_cfg->bin_policy = XDR_BIN_POLICY_ALL;
		}
		else if (bin_policy == "no-bins") {
			dc_ns_cfg->bin_policy = XDR_BIN_POLICY_NO_BINS;
		}
		else if (bin_policy == "only-changed") {
			dc_ns_cfg->bin_policy = XDR_BIN_POLICY_ONLY_CHANGED;
		}
		else if (bin_policy == "changed-and-specified") {
			dc_ns_cfg->bin_policy = XDR_BIN_POLICY_CHANGED_AND_SPECIFIED;
		}
		else if (bin_policy == "changed-or-specified") {
			dc_ns_cfg->bin_policy = XDR_BIN_POLICY_CHANGED_OR_SPECIFIED;
		}
		else {
			throw config_error("/xdr/dc/namespaces/bin-policy",
					"invalid value: " + bin_policy);
		}
	}

	static void
	handle_xdr_dc_ns_ship_versions_policy(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_string()) {
			throw config_error("/xdr/dc/namespaces/ship-versions-policy",
					"must be a string");
		}

		std::string policy = value.get<std::string>();

		if (policy == "latest") {
			dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_LATEST;
		}
		else if (policy == "all") {
			dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_ALL;
		}
		else if (policy == "interval") {
			dc_ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_INTERVAL;
		}
		else {
			throw config_error("/xdr/dc/namespaces/ship-versions-policy",
					"invalid value: " + policy);
		}
	}

	static void
	handle_xdr_dc_ns_write_policy(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_string()) {
			throw config_error("/xdr/dc/namespaces/write-policy",
					"must be a string");
		}

		std::string policy = value.get<std::string>();

		if (policy == "auto") {
			dc_ns_cfg->write_policy = XDR_WRITE_POLICY_AUTO;
		}
		else if (policy == "update") {
			dc_ns_cfg->write_policy = XDR_WRITE_POLICY_UPDATE;
		}
		else if (policy == "replace") {
			dc_ns_cfg->write_policy = XDR_WRITE_POLICY_REPLACE;
		}
		else {
			throw config_error("/xdr/dc/namespaces/write-policy",
					"invalid value: " + policy);
		}
	}

	static void
	handle_xdr_dc_ns_ignore_bins(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_array()) {
			throw config_error("/xdr/dc/namespaces/ignore-bins",
					"must be an array");
		}

		for (const auto& bin : value) {
			if (! bin.is_string()) {
				throw config_error("/xdr/dc/namespaces/ignore-bins",
						"entries must be a string");
			}

			std::string bin_name = bin.get<std::string>();

			if (bin_name.length() > AS_BIN_NAME_MAX_SZ) {
				throw config_error("/xdr/dc/namespaces/ignore-bins",
						"bin name too long: " + bin_name);
			}

			cf_vector_append_ptr(dc_ns_cfg->ignored_bins,
					cf_strdup(bin_name.c_str()));
		}
	}

	static void
	handle_xdr_dc_ns_ignore_sets(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_array()) {
			throw config_error("/xdr/dc/namespaces/ignore-sets",
					"must be an array");
		}

		for (const auto& set : value) {
			if (! set.is_string()) {
				throw config_error("/xdr/dc/namespaces/ignore-sets",
						"entries must be a string");
			}

			std::string set_name = set.get<std::string>();

			if (set_name.length() > AS_SET_NAME_MAX_SIZE) {
				throw config_error("/xdr/dc/namespaces/ignore-sets",
						"set name too long: " + set_name);
			}

			cf_vector_append_ptr(dc_ns_cfg->ignored_sets,
					cf_strdup(set_name.c_str()));
		}
	}

	static void
	handle_xdr_dc_ns_ship_bins(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_array()) {
			throw config_error("/xdr/dc/namespaces/ship-bins",
					"must be an array");
		}

		for (const auto& bin : value) {
			if (! bin.is_string()) {
				throw config_error("/xdr/dc/namespaces/ship-bins",
						"entries must be a string");
			}

			std::string bin_name = bin.get<std::string>();

			if (bin_name.length() > AS_BIN_NAME_MAX_SZ) {
				throw config_error("/xdr/dc/namespaces/ship-bins",
						"bin name too long: " + bin_name);
			}

			cf_vector_append_ptr(dc_ns_cfg->shipped_bins,
					cf_strdup(bin_name.c_str()));
		}
	}

	static void
	handle_xdr_dc_ns_ship_sets(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		if (! value.is_array()) {
			throw config_error("/xdr/dc/namespaces/ship-sets",
					"must be an array");
		}

		for (const auto& set : value) {
			if (! set.is_string()) {
				throw config_error("/xdr/dc/namespaces/ship-sets",
						"entries must be a string");
			}

			std::string set_name = set.get<std::string>();

			if (set_name.length() > AS_SET_NAME_MAX_SIZE) {
				throw config_error("/xdr/dc/namespaces/ship-sets",
						"set name too long: " + set_name);
			}

			cf_vector_append_ptr(dc_ns_cfg->shipped_sets,
					cf_strdup(set_name.c_str()));
		}
	}

	static void
	handle_xdr_dc_ns_ship_versions_interval(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_xdr_dc_ns_cfg* dc_ns_cfg = static_cast<as_xdr_dc_ns_cfg*>(target);

		uint32_t interval_seconds;
		if (value.is_number_unsigned() || value.is_number_integer()) {
			interval_seconds = value.get<uint32_t>();
		}
		else {
			throw config_error("/xdr/dc/namespaces/ship-versions-interval",
					"must be a positive integer");
		}

		if (interval_seconds < AS_XDR_MIN_SHIP_VERSIONS_INTERVAL ||
				interval_seconds > AS_XDR_MAX_SHIP_VERSIONS_INTERVAL) {
			throw config_error("/xdr/dc/namespaces/ship-versions-interval",
					"must be between " +
					std::to_string(AS_XDR_MIN_SHIP_VERSIONS_INTERVAL) +
					" and " +
					std::to_string(AS_XDR_MAX_SHIP_VERSIONS_INTERVAL) +
					" seconds");
		}


		uint64_t interval_ms = interval_seconds * 1000;

		if (interval_ms > std::numeric_limits<uint32_t>::max()) {
			throw config_error("/xdr/dc/namespaces/ship-versions-interval",
					"value too large");
		}

		dc_ns_cfg->ship_versions_interval_ms = static_cast<uint32_t>(interval_ms);
	}

	//------------------------------------------------
	// Security Handlers.
	//

	static void
	handle_security(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (is_community_edition()) {
			throw config_error("/security", "is enterprise-only");
		}

		as_config* config = static_cast<as_config*>(target);
		as_sec_config* sec_cfg = &config->sec_cfg;

		if (! value.is_object()) {
			throw config_error("/security", "must be an object");
		}

		// Set security_configured flag when security context is parsed.
		sec_cfg->security_configured = true;

		for (const auto& security_desc : SECURITY_FIELD_DESCRIPTORS) {
			apply_field(sec_cfg, value, security_desc);
		}
	}

	static void
	handle_security_ldap(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_sec_config* sec_config = static_cast<as_sec_config*>(target);

		if (! value.is_object()) {
			throw config_error("/security/ldap", "must be an object");
		}

		// Set ldap_configured flag when ldap context is parsed.
		sec_config->ldap_configured = true;

		for (const auto& ldap_desc : SECURITY_LDAP_FIELD_DESCRIPTORS) {
			apply_field(sec_config, value, ldap_desc);
		}
	}

	static void
	handle_security_log(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		as_sec_config* sec_config = static_cast<as_sec_config*>(target);

		if (! value.is_object()) {
			throw config_error("/security/log", "must be an object");
		}

		for (const auto& log_desc : SECURITY_LOG_FIELD_DESCRIPTORS) {
			apply_field(sec_config, value, log_desc);
		}
	}

	static void
	handle_security_ldap_token_hash_method(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_sec_config* sec_config = static_cast<as_sec_config*>(target);

		if (! value.is_string()) {
			throw config_error("/security/ldap/token-hash-method",
					"must be a string");
		}

		std::string hash_method = value.get<std::string>();

		if (hash_method == "sha-256") {
			sec_config->ldap_token_hash_method = AS_LDAP_EVP_SHA_256;
		}
		else if (hash_method == "sha-512") {
			sec_config->ldap_token_hash_method = AS_LDAP_EVP_SHA_512;
		}
		else {
			throw config_error("/security/ldap/token-hash-method",
					"invalid value: " + hash_method);
		}
	}

	static void
	handle_security_ldap_role_query_patterns(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		as_sec_config* sec_config = static_cast<as_sec_config*>(target);

		if (! value.is_array()) {
			throw config_error("/security/ldap/role-query-patterns",
					"must be an array");
		}

		int pattern_index = 0;

		for (const auto& pattern : value) {
			if (! pattern.is_string()) {
				throw config_error("/security/ldap/role-query-patterns",
						"entries must be a string");
			}

			if (pattern_index >= MAX_ROLE_QUERY_PATTERNS) {
				throw config_error("/security/ldap/role-query-patterns",
						"too many patterns (max " +
						std::to_string(MAX_ROLE_QUERY_PATTERNS) + ")");
			}

			std::string pattern_str = pattern.get<std::string>();
			sec_config->ldap_role_query_patterns[pattern_index] =
					cf_strdup(pattern_str.c_str());
			pattern_index++;
		}

		// Ensure null termination, this is relied on in the
		// security_info::as_security_get_config function.
		if (pattern_index < MAX_ROLE_QUERY_PATTERNS) {
			sec_config->ldap_role_query_patterns[pattern_index] = NULL;
		}
	}

	static void
	handle_security_log_report_data_op(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/security/log/report-data-op",
					"must be an array");
		}

		for (const auto& scope : value) {
			if (! scope.is_string()) {
				throw config_error("/security/log/report-data-op",
						"entries must be a string");
			}

			std::string scope_str = scope.get<std::string>();

			std::istringstream iss(scope_str);
			std::string ns_name, set_name;

			if (!(iss >> ns_name)) {
				throw config_error("/security/log/report-data-op",
						"invalid format: " + scope_str +
						" (expected 'namespace [set]')");
			}

			// Set name is optional.
			iss >> set_name;

			as_security_config_log_scope(ns_name.c_str(),
					set_name.empty() ? NULL : set_name.c_str());
		}
	}

	static void
	handle_security_log_report_data_op_role(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/security/log/report-data-op-role",
					"must be an array");
		}

		for (const auto& role : value) {
			if (! role.is_string()) {
				throw config_error("/security/log/report-data-op-role",
						"entries must be a string");
			}

			std::string role_str = role.get<std::string>();
			as_security_config_log_role(role_str.c_str());
		}
	}

	void
	handle_security_log_report_data_op_user(void* target,
			const FieldDescriptor& desc, const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/security/log/report-data-op-user",
					"must be an array");
		}

		for (const auto& user : value) {
			if (! user.is_string()) {
				throw config_error("/security/log/report-data-op-user",
						"entries must be a string");
			}

			std::string user_str = user.get<std::string>();
			as_security_config_log_user(user_str.c_str());
		}
	}

	//------------------------------------------------
	// Logging Handlers.
	//

	static void
	handle_logging(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		if (! value.is_array()) {
			throw config_error("/logging", "must be an array");
		}

		for (int i = 0; i < value.size(); ++i) {
			apply_logging_sink(i, value[i]);
		}
	}

	static void
	apply_logging_sink(int index, const nlohmann::json& sink_json)
	{
		if (! sink_json.is_object()) {
			throw config_error("/logging/" + std::to_string(index),
					"must be an object");
		}

		if (! sink_json.contains("type") || ! sink_json["type"].is_string()) {
			throw config_error("/logging/" + std::to_string(index),
					"must have a 'type' field");
		}

		std::string sink_type = sink_json["type"].get<std::string>();
		cf_log_sink* sink = NULL;

		if (sink_type == "console") {
			sink = cf_log_init_sink(NULL, -1, NULL);
		}
		else if (sink_type == "file") {
			if (! sink_json.contains("path") || ! sink_json["path"].is_string()) {
				throw config_error("/logging/" + std::to_string(index),
						"must have a 'path' field");
			}

			std::string path = sink_json["path"].get<std::string>();

			sink = cf_log_init_sink(path.c_str(), -1, NULL);
		}
		else if (sink_type == "syslog") {
			sink = cf_log_init_sink(DEFAULT_SYSLOG_PATH, LOG_LOCAL0, DEFAULT_SYSLOG_TAG);
		}
		else {
			throw config_error("/logging/" + std::to_string(index),
					"invalid sink type: " + sink_type);
		}

		if (sink == NULL) {
			throw config_error("/logging/" + std::to_string(index),
					"failed to create log sink");
		}

		for (const auto& logging_desc : LOGGING_FIELD_DESCRIPTORS) {
			apply_field(static_cast<void*>(sink), sink_json, logging_desc);
		}
	}

	static void
	handle_logging_facility(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		cf_log_sink* sink = static_cast<cf_log_sink*>(target);

		if (! value.is_string()) {
			throw config_error(desc.json_path, "must be a string");
		}

		if (! cf_log_init_facility(sink, value.get<std::string>().c_str())) {
			throw config_error(desc.json_path,
					"invalid facility: " + value.get<std::string>());
		}
	}

	static void
	handle_logging_syslog_path(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		cf_log_sink* sink = static_cast<cf_log_sink*>(target);

		if (! value.is_string()) {
			throw config_error(desc.json_path, "must be a string");
		}

		cf_log_init_path(sink, value.get<std::string>().c_str());
	}

	static void
	handle_logging_syslog_tag(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		cf_log_sink* sink = static_cast<cf_log_sink*>(target);

		if (! value.is_string()) {
			throw config_error(desc.json_path, "must be a string");
		}

		cf_log_init_tag(sink, value.get<std::string>().c_str());
	}

	static void
	handle_logging_context_level(void* target, const FieldDescriptor& desc,
			const nlohmann::json& value)
	{
		cf_log_sink* sink = static_cast<cf_log_sink*>(target);

		if (! value.is_string()) {
			throw config_error(desc.json_path, "log level must be a string");
		}

		std::string level_str = value.get<std::string>();
		// Extract context name from the JSON path (e.g., "/contexts/any" -> "any")
		size_t last_slash = desc.json_path.rfind('/');
		std::string context_name =
			(last_slash == std::string::npos)
				? desc.json_path
				: desc.json_path.substr(last_slash + 1);

		if (! cf_log_init_level(sink, context_name.c_str(),
				level_str.c_str())) {
			throw config_error(desc.json_path, "invalid context '" +
					context_name + "' or level '" + level_str + "'");
		}
	}
} // namespace cfg_handlers
