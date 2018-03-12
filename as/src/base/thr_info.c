/*
 * thr_info.c
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

#include "base/thr_info.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <malloc.h>
#include <mcheck.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_vector.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "fault.h"
#include "meminfo.h"
#include "shash.h"
#include "socket.h"

#include "ai_obj.h"
#include "ai_btree.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/features.h"
#include "base/index.h"
#include "base/monitor.h"
#include "base/scan.h"
#include "base/thr_batch.h"
#include "base/thr_demarshal.h"
#include "base/thr_info_port.h"
#include "base/thr_sindex.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/secondary_index.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/system_metadata.h"
#include "base/truncate.h"
#include "base/udf_cask.h"
#include "base/xdr_config.h"
#include "base/xdr_serverside.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/hlc.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "fabric/roster.h"
#include "fabric/skew_monitor.h"
#include "transaction/proxy.h"
#include "transaction/rw_request_hash.h"

#define STR_NS              "ns"
#define STR_SET             "set"
#define STR_INDEXNAME       "indexname"
#define STR_NUMBIN          "numbins"
#define STR_INDEXDATA       "indexdata"
#define STR_TYPE_NUMERIC    "numeric"
#define STR_TYPE_STRING     "string"
#define STR_ITYPE           "indextype"
#define STR_ITYPE_DEFAULT   "DEFAULT"
#define STR_ITYPE_LIST      "LIST"
#define STR_ITYPE_MAPKEYS   "MAPKEYS"
#define STR_ITYPE_MAPVALUES "MAPVALUES"
#define STR_BINTYPE         "bintype"

extern int as_nsup_queue_get_size();

int info_get_objects(char *name, cf_dyn_buf *db);
int info_get_tree_sets(char *name, char *subtree, cf_dyn_buf *db);
int info_get_tree_bins(char *name, char *subtree, cf_dyn_buf *db);
int info_get_tree_sindexes(char *name, char *subtree, cf_dyn_buf *db);
int info_get_tree_statistics(char *name, char *subtree, cf_dyn_buf *db);
void as_storage_show_wblock_stats(as_namespace *ns);
void as_storage_summarize_wblock_stats(as_namespace *ns);
int as_storage_analyze_wblock(as_namespace* ns, int device_index, uint32_t wblock_id);


as_stats g_stats = { 0 }; // separate .c file not worth it

uint64_t g_start_ms; // start time of the server

static cf_queue *g_info_work_q = 0;

//
// Info has its own fabric service
// which allows it to communicate things like the IP addresses of
// all the other nodes
//

#define INFO_FIELD_OP	0
#define INFO_FIELD_GENERATION 1
#define INFO_FIELD_SERVICE_ADDRESS 2
#define INFO_FIELD_ALT_ADDRESS 3
#define INFO_FIELD_SERVICES_CLEAR_STD 4
#define INFO_FIELD_SERVICES_TLS_STD 5
#define INFO_FIELD_SERVICES_CLEAR_ALT 6
#define INFO_FIELD_SERVICES_TLS_ALT 7
#define INFO_FIELD_TLS_NAME 8

#define INFO_OP_UPDATE 0
#define INFO_OP_ACK 1
#define INFO_OP_UPDATE_REQ 2

msg_template info_mt[] = {
	{ INFO_FIELD_OP,	M_FT_UINT32 },
	{ INFO_FIELD_GENERATION, M_FT_UINT32 },
	{ INFO_FIELD_SERVICE_ADDRESS, M_FT_STR },
	{ INFO_FIELD_ALT_ADDRESS, M_FT_STR },
	{ INFO_FIELD_SERVICES_CLEAR_STD, M_FT_STR },
	{ INFO_FIELD_SERVICES_TLS_STD, M_FT_STR },
	{ INFO_FIELD_SERVICES_CLEAR_ALT, M_FT_STR },
	{ INFO_FIELD_SERVICES_TLS_ALT, M_FT_STR },
	{ INFO_FIELD_TLS_NAME, M_FT_STR }
};

#define INFO_MSG_SCRATCH_SIZE 512


//
// The dynamic list has a name, and a function to call
//

typedef struct info_static_s {
	struct info_static_s	*next;
	bool   def; // default, but default is a reserved word
	char *name;
	char *value;
	size_t	value_sz;
} info_static;


typedef struct info_dynamic_s {
	struct info_dynamic_s *next;
	bool 	def;  // default, but that's a reserved word
	char *name;
	as_info_get_value_fn	value_fn;
} info_dynamic;

typedef struct info_command_s {
	struct info_command_s *next;
	char *name;
	as_info_command_fn 		command_fn;
	as_sec_perm				required_perm; // required security permission
} info_command;

typedef struct info_tree_s {
	struct info_tree_s *next;
	char *name;
	as_info_get_tree_fn	tree_fn;
} info_tree;


#define EOL		'\n' // incoming commands are separated by EOL
#define SEP		'\t'
#define TREE_SEP		'/'

#define INFO_COMMAND_SINDEX_FAILCODE(num, message)	\
	if (db) { \
		cf_dyn_buf_append_string(db, "FAIL:");			\
		cf_dyn_buf_append_int(db, num); 				\
		cf_dyn_buf_append_string(db, ": ");				\
		cf_dyn_buf_append_string(db, message);          \
	}


void
info_get_aggregated_namespace_stats(cf_dyn_buf *db)
{
	uint64_t total_objects = 0;
	uint64_t total_tombstones = 0;

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		total_objects += ns->n_objects;
		total_tombstones += ns->n_tombstones;
	}

	info_append_uint64(db, "objects", total_objects);
	info_append_uint64(db, "tombstones", total_tombstones);
}

// #define INFO_SEGV_TEST 1
#ifdef INFO_SEGV_TEST
char *segv_test = "segv test";
int
info_segv_test(char *name, cf_dyn_buf *db)
{
	*segv_test = 'E';
	cf_dyn_buf_append_string(db, "segv");
	return(0);
}
#endif

int
info_get_stats(char *name, cf_dyn_buf *db)
{
	info_append_uint32(db, "cluster_size", as_exchange_cluster_size());
	info_append_uint64_x(db, "cluster_key", as_exchange_cluster_key()); // not in ticker
	info_append_bool(db, "cluster_integrity", as_clustering_has_integrity()); // not in ticker
	info_append_bool(db, "cluster_is_member", ! as_clustering_is_orphan()); // not in ticker
	as_hb_info_duplicates_get(db); // not in ticker
	info_append_uint32(db, "cluster_clock_skew_stop_writes_sec", clock_skew_stop_writes_sec()); // not in ticker
	info_append_uint64(db, "cluster_clock_skew_ms", as_skew_monitor_skew());
	as_skew_monitor_info(db);

	info_append_uint64(db, "uptime", (cf_getms() - g_start_ms) / 1000); // not in ticker

	int freepct;
	bool swapping;

	cf_meminfo(NULL, NULL, &freepct, &swapping);
	info_append_int(db, "system_free_mem_pct", freepct);
	info_append_bool(db, "system_swapping", swapping);

	size_t allocated_kbytes;
	size_t active_kbytes;
	size_t mapped_kbytes;
	double efficiency_pct;
	uint32_t site_count;

	cf_alloc_heap_stats(&allocated_kbytes, &active_kbytes, &mapped_kbytes, &efficiency_pct,
			&site_count);
	info_append_uint64(db, "heap_allocated_kbytes", allocated_kbytes);
	info_append_uint64(db, "heap_active_kbytes", active_kbytes);
	info_append_uint64(db, "heap_mapped_kbytes", mapped_kbytes);
	info_append_int(db, "heap_efficiency_pct", (int)(efficiency_pct + 0.5));
	info_append_uint32(db, "heap_site_count", site_count);

	info_get_aggregated_namespace_stats(db);

	info_append_int(db, "tsvc_queue", as_tsvc_queue_get_size());
	info_append_int(db, "info_queue", as_info_queue_get_size());
	info_append_int(db, "delete_queue", as_nsup_queue_get_size());
	info_append_uint32(db, "rw_in_progress", rw_request_hash_count());
	info_append_uint32(db, "proxy_in_progress", as_proxy_hash_count());
	info_append_int(db, "tree_gc_queue", as_index_tree_gc_queue_size());

	info_append_uint64(db, "client_connections", g_stats.proto_connections_opened - g_stats.proto_connections_closed);
	info_append_uint64(db, "heartbeat_connections", g_stats.heartbeat_connections_opened - g_stats.heartbeat_connections_closed);
	info_append_uint64(db, "fabric_connections", g_stats.fabric_connections_opened - g_stats.fabric_connections_closed);

	info_append_uint64(db, "heartbeat_received_self", g_stats.heartbeat_received_self);
	info_append_uint64(db, "heartbeat_received_foreign", g_stats.heartbeat_received_foreign);


	info_append_uint64(db, "reaped_fds", g_stats.reaper_count); // not in ticker

	info_append_uint64(db, "info_complete", g_stats.info_complete); // not in ticker

	info_append_uint64(db, "demarshal_error", g_stats.n_demarshal_error);
	info_append_uint64(db, "early_tsvc_client_error", g_stats.n_tsvc_client_error);
	info_append_uint64(db, "early_tsvc_batch_sub_error", g_stats.n_tsvc_batch_sub_error);
	info_append_uint64(db, "early_tsvc_udf_sub_error", g_stats.n_tsvc_udf_sub_error);

	info_append_uint64(db, "batch_index_initiate", g_stats.batch_index_initiate); // not in ticker

	cf_dyn_buf_append_string(db, "batch_index_queue=");
	as_batch_queues_info(db); // not in ticker
	cf_dyn_buf_append_char(db, ';');

	info_append_uint64(db, "batch_index_complete", g_stats.batch_index_complete);
	info_append_uint64(db, "batch_index_error", g_stats.batch_index_errors);
	info_append_uint64(db, "batch_index_timeout", g_stats.batch_index_timeout);

	// Everything below is not in ticker...

	info_append_int(db, "batch_index_unused_buffers", as_batch_unused_buffers());
	info_append_uint64(db, "batch_index_huge_buffers", g_stats.batch_index_huge_buffers);
	info_append_uint64(db, "batch_index_created_buffers", g_stats.batch_index_created_buffers);
	info_append_uint64(db, "batch_index_destroyed_buffers", g_stats.batch_index_destroyed_buffers);

	info_append_uint64(db, "batch_initiate", g_stats.batch_initiate);
	info_append_int(db, "batch_queue", as_batch_direct_queue_size());
	info_append_uint64(db, "batch_error", g_stats.batch_errors);
	info_append_uint64(db, "batch_timeout", g_stats.batch_timeout);

	info_append_int(db, "scans_active", as_scan_get_active_job_count());

	info_append_uint32(db, "query_short_running", g_query_short_running);
	info_append_uint32(db, "query_long_running", g_query_long_running);

	info_append_uint64(db, "sindex_ucgarbage_found", g_stats.query_false_positives);
	info_append_uint64(db, "sindex_gc_locktimedout", g_stats.sindex_gc_timedout);
	info_append_uint64(db, "sindex_gc_list_creation_time", g_stats.sindex_gc_list_creation_time);
	info_append_uint64(db, "sindex_gc_list_deletion_time", g_stats.sindex_gc_list_deletion_time);
	info_append_uint64(db, "sindex_gc_objects_validated", g_stats.sindex_gc_objects_validated);
	info_append_uint64(db, "sindex_gc_garbage_found", g_stats.sindex_gc_garbage_found);
	info_append_uint64(db, "sindex_gc_garbage_cleaned", g_stats.sindex_gc_garbage_cleaned);

	char paxos_principal[16 + 1];
	sprintf(paxos_principal, "%lX", as_exchange_principal());
	info_append_string(db, "paxos_principal", paxos_principal);

	info_append_bool(db, "migrate_allowed", as_partition_balance_are_migrations_allowed());
	info_append_uint64(db, "migrate_partitions_remaining", as_partition_balance_remaining_migrations());

	info_append_uint64(db, "fabric_bulk_send_rate", g_stats.fabric_bulk_s_rate);
	info_append_uint64(db, "fabric_bulk_recv_rate", g_stats.fabric_bulk_r_rate);
	info_append_uint64(db, "fabric_ctrl_send_rate", g_stats.fabric_ctrl_s_rate);
	info_append_uint64(db, "fabric_ctrl_recv_rate", g_stats.fabric_ctrl_r_rate);
	info_append_uint64(db, "fabric_meta_send_rate", g_stats.fabric_meta_s_rate);
	info_append_uint64(db, "fabric_meta_recv_rate", g_stats.fabric_meta_r_rate);
	info_append_uint64(db, "fabric_rw_send_rate", g_stats.fabric_rw_s_rate);
	info_append_uint64(db, "fabric_rw_recv_rate", g_stats.fabric_rw_r_rate);

	as_xdr_get_stats(db);

	cf_dyn_buf_chomp(db);

	return 0;
}

cf_atomic32	 g_node_info_generation = 0;


int
info_get_cluster_generation(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_int(db, g_node_info_generation);

	return(0);
}

void
info_get_printable_cluster_name(char *cluster_name)
{
	as_config_cluster_name_get(cluster_name);
	if (cluster_name[0] == '\0'){
		strcpy(cluster_name, "null");
	}
}

int
info_get_cluster_name(char *name, cf_dyn_buf *db)
{
	char cluster_name[AS_CLUSTER_NAME_SZ];
	info_get_printable_cluster_name(cluster_name);
	cf_dyn_buf_append_string(db, cluster_name);

	return 0;
}

int
info_get_features(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, as_features_info());

	return 0;
}

static cf_ip_port
bind_to_port(cf_serv_cfg *cfg, cf_sock_owner owner)
{
	for (uint32_t i = 0; i < cfg->n_cfgs; ++i) {
		if (cfg->cfgs[i].owner == owner) {
			return cfg->cfgs[i].port;
		}
	}

	return 0;
}

char *
as_info_bind_to_string(const cf_serv_cfg *cfg, cf_sock_owner owner)
{
	cf_dyn_buf_define_size(db, 2500);
	uint32_t count = 0;

	for (uint32_t i = 0; i < cfg->n_cfgs; ++i) {
		if (cfg->cfgs[i].owner != owner) {
			continue;
		}

		if (count > 0) {
			cf_dyn_buf_append_char(&db, ',');
		}

		cf_dyn_buf_append_string(&db, cf_ip_addr_print(&cfg->cfgs[i].addr));
		++count;
	}

	char *string = cf_dyn_buf_strdup(&db);
	cf_dyn_buf_free(&db);
	return string != NULL ? string : cf_strdup("null");
}

static char *
access_to_string(cf_addr_list *addrs)
{
	cf_dyn_buf_define_size(db, 2500);

	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {
		if (i > 0) {
			cf_dyn_buf_append_char(&db, ',');
		}

		cf_dyn_buf_append_string(&db, addrs->addrs[i]);
	}

	char *string = cf_dyn_buf_strdup(&db);
	cf_dyn_buf_free(&db);
	return string != NULL ? string : cf_strdup("null");
}

int
info_get_endpoints(char *name, cf_dyn_buf *db)
{
	cf_ip_port port = bind_to_port(&g_service_bind, CF_SOCK_OWNER_SERVICE);
	info_append_int(db, "service.port", port);

	char *string = as_info_bind_to_string(&g_service_bind, CF_SOCK_OWNER_SERVICE);
	info_append_string(db, "service.addresses", string);
	cf_free(string);

	info_append_int(db, "service.access-port", g_access.service.port);

	string = access_to_string(&g_access.service.addrs);
	info_append_string(db, "service.access-addresses", string);
	cf_free(string);

	info_append_int(db, "service.alternate-access-port", g_access.alt_service.port);

	string = access_to_string(&g_access.alt_service.addrs);
	info_append_string(db, "service.alternate-access-addresses", string);
	cf_free(string);

	port = bind_to_port(&g_service_bind, CF_SOCK_OWNER_SERVICE_TLS);
	info_append_int(db, "service.tls-port", port);

	string = as_info_bind_to_string(&g_service_bind, CF_SOCK_OWNER_SERVICE_TLS);
	info_append_string(db, "service.tls-addresses", string);
	cf_free(string);

	info_append_int(db, "service.tls-access-port", g_access.tls_service.port);

	string = access_to_string(&g_access.tls_service.addrs);
	info_append_string(db, "service.tls-access-addresses", string);
	cf_free(string);

	info_append_int(db, "service.tls-alternate-access-port", g_access.alt_tls_service.port);

	string = access_to_string(&g_access.alt_tls_service.addrs);
	info_append_string(db, "service.tls-alternate-access-addresses", string);
	cf_free(string);

	as_hb_info_endpoints_get(db);

	port = bind_to_port(&g_fabric_bind, CF_SOCK_OWNER_FABRIC);
	info_append_int(db, "fabric.port", port);

	string = as_info_bind_to_string(&g_fabric_bind, CF_SOCK_OWNER_FABRIC);
	info_append_string(db, "fabric.addresses", string);
	cf_free(string);

	port = bind_to_port(&g_fabric_bind, CF_SOCK_OWNER_FABRIC_TLS);
	info_append_int(db, "fabric.tls-port", port);

	string = as_info_bind_to_string(&g_fabric_bind, CF_SOCK_OWNER_FABRIC_TLS);
	info_append_string(db, "fabric.tls-addresses", string);
	cf_free(string);

	as_fabric_info_peer_endpoints_get(db);

	info_append_int(db, "info.port", g_info_port);

	string = as_info_bind_to_string(&g_info_bind, CF_SOCK_OWNER_INFO);
	info_append_string(db, "info.addresses", string);
	cf_free(string);

	cf_dyn_buf_chomp(db);
	return(0);
}

int
info_get_partition_generation(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_int(db, (int)g_partition_generation);

	return(0);
}

int
info_get_partition_info(char *name, cf_dyn_buf *db)
{
	as_partition_getinfo_str(db);

	return(0);
}

// Deprecate in "six months".
int
info_get_replicas_prole(char *name, cf_dyn_buf *db)
{
	as_partition_get_replicas_prole_str(db);

	return(0);
}

int
info_get_replicas_master(char *name, cf_dyn_buf *db)
{
	as_partition_get_replicas_master_str(db);

	return(0);
}

int
info_get_replicas_all(char *name, cf_dyn_buf *db)
{
	as_partition_get_replicas_all_str(db, false);

	return(0);
}

int
info_get_replicas(char *name, cf_dyn_buf *db)
{
	as_partition_get_replicas_all_str(db, true);

	return(0);
}

//
// COMMANDS
//

int
info_command_get_sl(char *name, char *params, cf_dyn_buf *db)
{
	// Command Format:  "get-sl:"

	as_exchange_info_get_succession(db);

	return 0;
}

int
info_command_tip(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "tip command received: params %s", params);

	char host_str[50];
	int  host_str_len = sizeof(host_str);

	char port_str[50];
	int  port_str_len = sizeof(port_str);
	int rv = -1;

	char tls_str[50];
	int  tls_str_len = sizeof(tls_str);

	/*
	 *  Command Format:  "tip:host=<IPAddr>;port=<PortNum>[;tls=<Bool>]"
	 *
	 *  where <IPAddr> is an IP address and <PortNum> is a valid TCP port number.
	 */

	if (0 != as_info_parameter_get(params, "host", host_str, &host_str_len)) {
		cf_warning(AS_INFO, "tip command: no host, must add a host parameter");
		goto Exit;
	}

	if (0 != as_info_parameter_get(params, "port", port_str, &port_str_len)) {
		cf_warning(AS_INFO, "tip command: no port, must have port");
		goto Exit;
	}

	if (0 != as_info_parameter_get(params, "tls", tls_str, &tls_str_len)) {
		strcpy(tls_str, "false");
	}

	int port = 0;
	if (0 != cf_str_atoi(port_str, &port)) {
		cf_warning(AS_INFO, "tip command: port must be an integer in: %s", port_str);
		goto Exit;
	}

	bool tls;
	if (strcmp(tls_str, "true") == 0) {
		tls = true;
	}
	else if (strcmp(tls_str, "false") == 0) {
		tls = false;
	}
	else {
		cf_warning(AS_INFO, "The \"%s:\" command argument \"tls\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, tls_str);
		goto Exit;
	}

	rv = as_hb_mesh_tip(host_str, port, tls);

Exit:
	if (0 == rv) {
		cf_dyn_buf_append_string(db, "ok");
	} else {
		cf_dyn_buf_append_string(db, "error");
	}

	return(0);
}

/*
 *  Command Format:  "tip-clear:{host-port-list=<hpl>}"
 *
 *  where <hpl> is either "all" or else a comma-separated list of items of the form: <HostIPAddr>:<PortNum>
 */
int32_t
info_command_tip_clear(char* name, char* params, cf_dyn_buf* db)
{
	cf_info(AS_INFO, "tip clear command received: params %s", params);

	// Command Format:  "tip-clear:{host-port-list=<hpl>}" [the
	// "host-port-list" argument is optional]
	// where <hpl> is either "all" or else a comma-separated list of items
	// of the form: <HostIPv4Addr>:<PortNum> or [<HostIPv6Addr>]:<PortNum>

	char host_port_list[3000];
	int host_port_list_len = sizeof(host_port_list);
	host_port_list[0] = '\0';
	bool success = true;
	uint32_t cleared = 0, not_found = 0;

	if (as_info_parameter_get(params, "host-port-list", host_port_list,
				  &host_port_list_len) == 0) {
		if (0 != strcmp(host_port_list, "all")) {
			char* save_ptr = NULL;
			int port = -1;
			char* host_port =
			  strtok_r(host_port_list, ",", &save_ptr);

			while (host_port != NULL) {
				char* host_port_delim = ":";
				if (*host_port == '[') {
					// Parse IPv6 address differently.
					host_port++;
					host_port_delim = "]";
				}

				char* host_port_save_ptr = NULL;
				char* host =
				  strtok_r(host_port, host_port_delim, &host_port_save_ptr);

				if (host == NULL) {
					cf_warning(AS_INFO, "tip clear command: invalid host:port string: %s", host_port);
					success = false;
					break;
				}

				char* port_str =
				  strtok_r(NULL, host_port_delim, &host_port_save_ptr);

				if (port_str != NULL && *port_str == ':') {
					// IPv6 case
					port_str++;
				}
				if (port_str == NULL ||
					0 != cf_str_atoi(port_str, &port)) {
					cf_warning(AS_INFO, "tip clear command: port must be an integer in: %s", port_str);
					success = false;
					break;
				}

				if (as_hb_mesh_tip_clear(host, port) == -1) {
					success = false;
					not_found++;
					cf_warning(AS_INFO, "seed node %s:%d does not exist", host, port);
				} else {
					cleared++;
				}

				host_port = strtok_r(NULL, ",", &save_ptr);
			}
		} else {
			if (as_hb_mesh_tip_clear_all(&cleared)) {
				success = false;
			}
		}
	} else {
		success = false;
	}

	if (success) {
		cf_info(AS_INFO, "tip clear command executed: cleared %"PRIu32", params %s", cleared, params);
		cf_dyn_buf_append_string(db, "ok");
	} else {
		cf_info(AS_INFO, "tip clear command failed: cleared %"PRIu32", params %s", cleared, params);
		char error_msg[1024];
		sprintf(error_msg, "error: %"PRIu32" cleared, %"PRIu32" not found", cleared, not_found);
		cf_dyn_buf_append_string(db, error_msg);
	}

	return (0);
}

int
info_command_show_devices(char *name, char *params, cf_dyn_buf *db)
{
	char ns_str[512];
	int  ns_len = sizeof(ns_str);

	if (0 != as_info_parameter_get(params, "namespace", ns_str, &ns_len)) {
		cf_info(AS_INFO, "show-devices requires namespace parameter");
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "show-devices: namespace %s not found", ns_str);
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}
	as_storage_show_wblock_stats(ns);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}

int
info_command_dump_cluster(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-cluster:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_clustering_dump(verbose);
	as_exchange_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_fabric(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-fabric:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_fabric_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_hb(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-hb:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_hb_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_hlc(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-hlc:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_hlc_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}


int
info_command_dump_migrates(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-migrates:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_migrate_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_msgs(char *name, char *params, cf_dyn_buf *db)
{
	bool once = true;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-msgs:{mode=<mode>}" [the "mode" argument is optional]
	 *
	 *   where <mode> is one of:  {"on" | "off" | "once"} and defaults to "once".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "mode", param_str, &param_str_len)) {
		if (!strncmp(param_str, "on", 3)) {
			g_config.fabric_dump_msgs = true;
		} else if (!strncmp(param_str, "off", 4)) {
			g_config.fabric_dump_msgs = false;
			once = false;
		} else if (!strncmp(param_str, "once", 5)) {
			once = true;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"mode\" value must be one of {\"on\", \"off\", \"once\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	if (once) {
		as_fabric_msg_queue_dump();
	}

	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

static int
is_numeric_string(char *str)
{
	if (!*str)
		return 0;

	while (isdigit(*str))
		str++;

	return (!*str);
}

int
info_command_dump_wb(char *name, char *params, cf_dyn_buf *db)
{
	as_namespace *ns;
	int device_index, wblock_id;
	char param_str[100];
	int param_str_len;

	/*
	 *  Command Format:  "dump-wb:ns=<Namespace>;dev=<DeviceID>;id=<WBlockId>"
	 *
	 *   where <Namespace> is the name of the namespace,
	 *         <DeviceID> is the drive number (a non-negative integer), and
	 *         <WBlockID> is a non-negative integer corresponding to an active wblock.
	 */
	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "ns", param_str, &param_str_len)) {
		if (!(ns = as_namespace_get_byname(param_str))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"ns\" value must be the name of an existing namespace, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"ns=<Namespace>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "dev", param_str, &param_str_len)) {
		if (!is_numeric_string(param_str) || (0 > (device_index = atoi(param_str)))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"dev\" value must be a non-negative integer, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"dev=<DeviceID>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "id", param_str, &param_str_len)) {
		if (!is_numeric_string(param_str) || (0 > (wblock_id = atoi(param_str)))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"id\" value must be a non-negative integer, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"id=<WBlockID>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	if (!as_storage_analyze_wblock(ns, device_index, (uint32_t) wblock_id))
		cf_dyn_buf_append_string(db, "ok");
	else
		cf_dyn_buf_append_string(db, "error");

	return(0);
}

int
info_command_dump_wb_summary(char *name, char *params, cf_dyn_buf *db)
{
	as_namespace *ns;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-wb-summary:ns=<Namespace>"
	 *
	 *  where <Namespace> is the name of an existing namespace.
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "ns", param_str, &param_str_len)) {
		if (!(ns = as_namespace_get_byname(param_str))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"ns\" value must be the name of an existing namespace, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return(0);
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"ns=<Namespace>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	as_storage_summarize_wblock_stats(ns);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}

int
info_command_dump_rw_request_hash(char *name, char *params, cf_dyn_buf *db)
{
	rw_request_hash_dump();
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

typedef struct rack_node_s {
	uint32_t rack_id;
	cf_node node;
} rack_node;

// A comparison_fn_t used with qsort() - yields ascending rack-id order.
static inline int
compare_rack_nodes(const void* pa, const void* pb)
{
	uint32_t a = ((const rack_node*)pa)->rack_id;
	uint32_t b = ((const rack_node*)pb)->rack_id;

	return a > b ? 1 : (a == b ? 0 : -1);
}

void
namespace_rack_info(as_namespace *ns, cf_dyn_buf *db, uint32_t *rack_ids,
		uint32_t n_nodes, const char *tag)
{
	if (n_nodes == 0) {
		return;
	}

	rack_node rack_nodes[n_nodes];

	for (uint32_t i = 0; i < n_nodes; i++) {
		rack_nodes[i].rack_id = rack_ids[i];
		rack_nodes[i].node = ns->succession[i];
	}

	qsort(rack_nodes, n_nodes, sizeof(rack_node), compare_rack_nodes);

	uint32_t cur_id = rack_nodes[0].rack_id;

	cf_dyn_buf_append_string(db, tag);
	cf_dyn_buf_append_uint32(db, cur_id);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint64_x(db, rack_nodes[0].node);

	for (uint32_t i = 1; i < n_nodes; i++) {
		if (rack_nodes[i].rack_id == cur_id) {
			cf_dyn_buf_append_char(db, ',');
			cf_dyn_buf_append_uint64_x(db, rack_nodes[i].node);
			continue;
		}

		cur_id = rack_nodes[i].rack_id;

		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, tag);
		cf_dyn_buf_append_uint32(db, cur_id);
		cf_dyn_buf_append_char(db, '=');
		cf_dyn_buf_append_uint64_x(db, rack_nodes[i].node);
	}
}

int
info_command_racks(char *name, char *params, cf_dyn_buf *db)
{
	// Command format: "racks:{namespace=<namespace-name>}"

	char param_str[AS_ID_NAMESPACE_SZ] = { 0 };
	int param_str_len = (int)sizeof(param_str);
	int rv = as_info_parameter_get(params, "namespace", param_str, &param_str_len);

	if (rv == -2) {
		cf_warning(AS_INFO, "namespace parameter value too long");
		cf_dyn_buf_append_string(db, "ERROR::bad-namespace");
		return 0;
	}

	if (rv == 0) {
		as_namespace *ns = as_namespace_get_byname(param_str);

		if (! ns) {
			cf_warning(AS_INFO, "unknown namespace %s", param_str);
			cf_dyn_buf_append_string(db, "ERROR::unknown-namespace");
			return 0;
		}

		as_exchange_info_lock();

		namespace_rack_info(ns, db, ns->rack_ids, ns->cluster_size, "rack_");

		if (ns->roster_count != 0) {
			cf_dyn_buf_append_char(db, ':');
			namespace_rack_info(ns, db, ns->roster_rack_ids, ns->roster_count, "roster_rack_");
		}

		as_exchange_info_unlock();

		return 0;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, "ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		as_exchange_info_lock();

		namespace_rack_info(ns, db, ns->rack_ids, ns->cluster_size, "rack_");

		if (ns->roster_count != 0) {
			cf_dyn_buf_append_char(db, ':');
			namespace_rack_info(ns, db, ns->roster_rack_ids, ns->roster_count, "roster_rack_");
		}

		as_exchange_info_unlock();

		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_chomp(db);

	return 0;
}

int
info_command_recluster(char *name, char *params, cf_dyn_buf *db)
{
	// Command format: "recluster:"

	int rv = as_clustering_cluster_reform();

	// TODO - resolve error condition further?
	cf_dyn_buf_append_string(db,
			rv == 0 ? "ok" : (rv == 1 ? "ignored-by-non-principal" : "ERROR"));

	return 0;
}

int
info_command_jem_stats(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "jem_stats command received: params %s", params);

	/*
	 *	Command Format:	 "jem-stats:{file=<string>;options=<string>;sites=<string>}" [the "file", "options", and "sites" arguments are optional]
	 *
	 *  Logs the JEMalloc statistics to the console or an optionally-specified file pathname.
	 *  Options may be a string containing any of the characters "gmablh", as defined by jemalloc(3) man page.
	 *  The "sites" parameter optionally specifies a file to dump memory accounting information to.
	 *  [Note:  Any options are only used if an output file is specified.]
	 */

	char param_str[100];
	int param_str_len = sizeof(param_str);
	char *file = NULL, *options = NULL, *sites = NULL;

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "file", param_str, &param_str_len)) {
		file = cf_strdup(param_str);
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "options", param_str, &param_str_len)) {
		options = cf_strdup(param_str);
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "sites", param_str, &param_str_len)) {
		sites = cf_strdup(param_str);
	}

	cf_alloc_log_stats(file, options);

	if (file) {
		cf_free(file);
	}

	if (options) {
		cf_free(options);
	}

	if (sites) {
		cf_alloc_log_site_infos(sites);
		cf_free(sites);
	}

	cf_dyn_buf_append_string(db, "ok");
	return 0;
}

/*
 *  Print out System Metadata info.
 */
int
info_command_dump_smd(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "dump-smd command received: params %s", params);

	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-smd:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	as_smd_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

/*
 *  Print out Secondary Index info.
 */
int
info_command_dump_si(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "dump-si command received: params %s", params);

	char param_str[100];
	int param_str_len = sizeof(param_str);
	char *nsname = NULL, *indexname = NULL, *filename = NULL;
	bool verbose = false;

	/*
	 *  Command Format:  "dump-si:ns=<string>;indexname=<string>;filename=<string>;{verbose=<opt>}" [the "file" and "verbose" arguments are optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "ns", param_str, &param_str_len)) {
		nsname = cf_strdup(param_str);
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an \"ns\" parameter", name);
		cf_dyn_buf_append_string(db, "error");
		goto cleanup;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "indexname", param_str, &param_str_len)) {
		indexname = cf_strdup(param_str);
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires a \"indexname\" parameter", name);
		cf_dyn_buf_append_string(db, "error");
		goto cleanup;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "file", param_str, &param_str_len)) {
		filename = cf_strdup(param_str);
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires a \"filename\" parameter", name);
		cf_dyn_buf_append_string(db, "error");
		goto cleanup;
	}


	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			goto cleanup;
		}
	}

	as_sindex_dump(nsname, indexname, filename, verbose);
	cf_dyn_buf_append_string(db, "ok");


 cleanup:
	if (nsname) {
		cf_free(nsname);
	}

	if (indexname) {
		cf_free(indexname);
	}

	if (filename) {
		cf_free(filename);
	}

	return 0;
}

/*
 *  Print out clock skew information.
 */
int
info_command_dump_skew(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "dump-skew command received: params %s", params);

	/*
	 *  Command Format:  "dump-skew:"
	 */
	as_skew_monitor_dump();
	cf_dyn_buf_append_string(db, "ok");
	return 0;
}

int
info_command_mon_cmd(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "add-module command received: params %s", params);

	/*
	 *  Command Format:  "jobs:[module=<string>;cmd=<command>;<parameters>]"
	 *                   asinfo -v 'jobs'              -> list all jobs
	 *                   asinfo -v 'jobs:module=query' -> list all jobs for query module
	 *                   asinfo -v 'jobs:module=query;cmd=kill-job;trid=<trid>'
	 *                   asinfo -v 'jobs:module=query;cmd=set-priority;trid=<trid>;value=<val>'
	 *
	 *  where <module> is one of following:
	 *      - query
	 *      - scan
	 */

	char cmd[13];
	char module[21];
	char job_id[24];
	char val_str[11];
	int cmd_len       = sizeof(cmd);
	int module_len    = sizeof(module);
	int job_id_len    = sizeof(job_id);
	int val_len       = sizeof(val_str);
	uint64_t trid     = 0;
	uint32_t value    = 0;

	cmd[0]     = '\0';
	module[0]  = '\0';
	job_id[0]  = '\0';
	val_str[0] = '\0';

	// Read the parameters: module cmd trid value
	int rv = as_info_parameter_get(params, "module", module, &module_len);
	if (rv == -1) {
		as_mon_info_cmd(NULL, NULL, 0, 0, db);
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"module\" parameter too long (> ");
		cf_dyn_buf_append_int(db, module_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "cmd", cmd, &cmd_len);
	if (rv == -1) {
		as_mon_info_cmd(module, NULL, 0, 0, db);
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"cmd\" parameter too long (> ");
		cf_dyn_buf_append_int(db, cmd_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "trid", job_id, &job_id_len);
	if (rv == 0) {
		trid  = strtoull(job_id, NULL, 10);
	}
	else if (rv == -1) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":no \"trid\" parameter specified");
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"trid\" parameter too long (> ");
		cf_dyn_buf_append_int(db, job_id_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "value", val_str, &val_len);
	if (rv == 0) {
		value = strtoul(val_str, NULL, 10);
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"value\" parameter too long (> ");
		cf_dyn_buf_append_int(db, val_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	cf_info(AS_INFO, "%s %s %lu %u", module, cmd, trid, value);
	as_mon_info_cmd(module, cmd, trid, value, db);
	return 0;
}


static const char *
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

static const char *
auto_pin_string(void)
{
	switch (g_config.auto_pin) {
	case CF_TOPO_AUTO_PIN_NONE:
		return "none";

	case CF_TOPO_AUTO_PIN_CPU:
		return "cpu";

	case CF_TOPO_AUTO_PIN_NUMA:
		return "numa";

	default:
		cf_crash(CF_ALLOC, "invalid CF_TOPO_AUTO_* value");
		return NULL;
	}
}

void
info_service_config_get(cf_dyn_buf *db)
{
	// Note - no user, group.
	info_append_uint32(db, "paxos-single-replica-limit", g_config.paxos_single_replica_limit);
	info_append_string_safe(db, "pidfile", g_config.pidfile);
	info_append_int(db, "proto-fd-max", g_config.n_proto_fd_max);

	info_append_bool(db, "advertise-ipv6", cf_socket_advertises_ipv6());
	info_append_string(db, "auto-pin", auto_pin_string());
	info_append_int(db, "batch-threads", g_config.n_batch_threads);
	info_append_uint32(db, "batch-max-buffers-per-queue", g_config.batch_max_buffers_per_queue);
	info_append_uint32(db, "batch-max-requests", g_config.batch_max_requests);
	info_append_uint32(db, "batch-max-unused-buffers", g_config.batch_max_unused_buffers);
	info_append_uint32(db, "batch-priority", g_config.batch_priority);
	info_append_uint32(db, "batch-index-threads", g_config.n_batch_index_threads);

	char cluster_name[AS_CLUSTER_NAME_SZ];
	info_get_printable_cluster_name(cluster_name);
	info_append_string(db, "cluster-name", cluster_name);

	info_append_bool(db, "enable-benchmarks-fabric", g_config.fabric_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-svc", g_config.svc_benchmarks_enabled);
	info_append_bool(db, "enable-hist-info", g_config.info_hist_enabled);
	info_append_string(db, "feature-key-file", g_config.feature_key_file);
	info_append_uint32(db, "hist-track-back", g_config.hist_track_back);
	info_append_uint32(db, "hist-track-slice", g_config.hist_track_slice);
	info_append_string_safe(db, "hist-track-thresholds", g_config.hist_track_thresholds);
	info_append_int(db, "info-threads", g_config.n_info_threads);
	info_append_bool(db, "log-local-time", cf_fault_is_using_local_time());
	info_append_uint32(db, "migrate-max-num-incoming", g_config.migrate_max_num_incoming);
	info_append_uint32(db, "migrate-threads", g_config.n_migrate_threads);
	info_append_uint32(db, "min-cluster-size", g_config.clustering_config.cluster_size_min);
	info_append_uint64_x(db, "node-id", g_config.self_node); // may be configured or auto-generated
	info_append_string_safe(db, "node-id-interface", g_config.node_id_interface);
	info_append_uint32(db, "nsup-delete-sleep", g_config.nsup_delete_sleep);
	info_append_uint32(db, "nsup-period", g_config.nsup_period);
	info_append_bool(db, "nsup-startup-evict", g_config.nsup_startup_evict);
	info_append_int(db, "proto-fd-idle-ms", g_config.proto_fd_idle_ms);
	info_append_int(db, "proto-slow-netio-sleep-ms", g_config.proto_slow_netio_sleep_ms); // dynamic only
	info_append_uint32(db, "query-batch-size", g_config.query_bsize);
	info_append_uint32(db, "query-buf-size", g_config.query_buf_size); // dynamic only
	info_append_uint32(db, "query-bufpool-size", g_config.query_bufpool_size);
	info_append_bool(db, "query-in-transaction-thread", g_config.query_in_transaction_thr);
	info_append_uint32(db, "query-long-q-max-size", g_config.query_long_q_max_size);
	info_append_bool(db, "query-microbenchmark", g_config.query_enable_histogram); // dynamic only
	info_append_bool(db, "query-pre-reserve-partitions", g_config.partitions_pre_reserved);
	info_append_uint32(db, "query-priority", g_config.query_priority);
	info_append_uint64(db, "query-priority-sleep-us", g_config.query_sleep_us);
	info_append_uint64(db, "query-rec-count-bound", g_config.query_rec_count_bound);
	info_append_bool(db, "query-req-in-query-thread", g_config.query_req_in_query_thread);
	info_append_uint32(db, "query-req-max-inflight", g_config.query_req_max_inflight);
	info_append_uint32(db, "query-short-q-max-size", g_config.query_short_q_max_size);
	info_append_uint32(db, "query-threads", g_config.query_threads);
	info_append_uint32(db, "query-threshold", g_config.query_threshold);
	info_append_uint64(db, "query-untracked-time-ms", g_config.query_untracked_time_ms);
	info_append_uint32(db, "query-worker-threads", g_config.query_worker_threads);
	info_append_bool(db, "run-as-daemon", g_config.run_as_daemon);
	info_append_uint32(db, "scan-max-active", g_config.scan_max_active);
	info_append_uint32(db, "scan-max-done", g_config.scan_max_done);
	info_append_uint32(db, "scan-max-udf-transactions", g_config.scan_max_udf_transactions);
	info_append_uint32(db, "scan-threads", g_config.scan_threads);
	info_append_uint32(db, "service-threads", g_config.n_service_threads);
	info_append_uint32(db, "sindex-builder-threads", g_config.sindex_builder_threads);
	info_append_uint32(db, "sindex-gc-max-rate", g_config.sindex_gc_max_rate);
	info_append_uint32(db, "sindex-gc-period", g_config.sindex_gc_period);
	info_append_uint32(db, "ticker-interval", g_config.ticker_interval);
	info_append_int(db, "transaction-max-ms", (int)(g_config.transaction_max_ns / 1000000));
	info_append_uint32(db, "transaction-pending-limit", g_config.transaction_pending_limit);
	info_append_uint32(db, "transaction-queues", g_config.n_transaction_queues);
	info_append_uint32(db, "transaction-retry-ms", g_config.transaction_retry_ms);
	info_append_uint32(db, "transaction-threads-per-queue", g_config.n_transaction_threads_per_queue);
	info_append_string_safe(db, "work-directory", g_config.work_directory);

	info_append_string(db, "debug-allocations", debug_allocations_string());
	info_append_bool(db, "fabric-dump-msgs", g_config.fabric_dump_msgs);
	info_append_uint32(db, "prole-extra-ttl", g_config.prole_extra_ttl);
}

static void
append_addrs(cf_dyn_buf *db, const char *name, const cf_addr_list *list)
{
	for (uint32_t i = 0; i < list->n_addrs; ++i) {
		info_append_string(db, name, list->addrs[i]);
	}
}

void
info_network_config_get(cf_dyn_buf *db)
{
	// Service:

	info_append_int(db, "service.port", g_config.service.bind_port);
	append_addrs(db, "service.address", &g_config.service.bind);
	info_append_int(db, "service.access-port", g_config.service.std_port);
	append_addrs(db, "service.access-address", &g_config.service.std);
	info_append_int(db, "service.alternate-access-port", g_config.service.alt_port);
	append_addrs(db, "service.alternate-access-address", &g_config.service.alt);

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

	// Heartbeat:

	as_hb_info_config_get(db);

	// Fabric:

	append_addrs(db, "fabric.address", &g_config.fabric.bind);
	info_append_int(db, "fabric.port", g_config.fabric.bind_port);
	append_addrs(db, "fabric.tls-address", &g_config.tls_fabric.bind);
	info_append_int(db, "fabric.tls-port", g_config.tls_fabric.bind_port);
	info_append_string_safe(db, "fabric.tls-name", g_config.tls_fabric.tls_our_name);
	info_append_int(db, "fabric.channel-bulk-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_BULK]);
	info_append_int(db, "fabric.channel-bulk-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK]);
	info_append_int(db, "fabric.channel-ctrl-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_CTRL]);
	info_append_int(db, "fabric.channel-ctrl-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL]);
	info_append_int(db, "fabric.channel-meta-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_META]);
	info_append_int(db, "fabric.channel-meta-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META]);
	info_append_int(db, "fabric.channel-rw-fds", g_config.n_fabric_channel_fds[AS_FABRIC_CHANNEL_RW]);
	info_append_int(db, "fabric.channel-rw-recv-threads", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW]);
	info_append_bool(db, "fabric.keepalive-enabled", g_config.fabric_keepalive_enabled);
	info_append_int(db, "fabric.keepalive-intvl", g_config.fabric_keepalive_intvl);
	info_append_int(db, "fabric.keepalive-probes", g_config.fabric_keepalive_probes);
	info_append_int(db, "fabric.keepalive-time", g_config.fabric_keepalive_time);
	info_append_int(db, "fabric.latency-max-ms", g_config.fabric_latency_max_ms);
	info_append_int(db, "fabric.recv-rearm-threshold", g_config.fabric_recv_rearm_threshold);
	info_append_int(db, "fabric.send-threads", g_config.n_fabric_send_threads);

	// Info:

	append_addrs(db, "info.address", &g_config.info.bind);
	info_append_int(db, "info.port", g_config.info.bind_port);

	// TLS:

	for (uint32_t i = 0; i < g_config.n_tls_specs; ++i) {
		cf_tls_spec *spec = g_config.tls_specs + i;
		char key[100];

		snprintf(key, sizeof(key), "tls[%u].name", i);
		info_append_string_safe(db, key, spec->name);

		snprintf(key, sizeof(key), "tls[%u].cert_file", i);
		info_append_string_safe(db, key, spec->cert_file);

		snprintf(key, sizeof(key), "tls[%u].key_file", i);
		info_append_string_safe(db, key, spec->key_file);

		snprintf(key, sizeof(key), "tls[%u].ca_file", i);
		info_append_string_safe(db, key, spec->ca_file);

		snprintf(key, sizeof(key), "tls[%u].ca_path", i);
		info_append_string_safe(db, key, spec->ca_path);

		snprintf(key, sizeof(key), "tls[%u].cert_blacklist", i);
		info_append_string_safe(db, key, spec->cert_blacklist);

		snprintf(key, sizeof(key), "tls[%u].protocols", i);
		info_append_string_safe(db, key, spec->protocols);

		snprintf(key, sizeof(key), "tls[%u].cipher_suite", i);
		info_append_string_safe(db, key, spec->cipher_suite);
	}
}


void
info_namespace_config_get(char* context, cf_dyn_buf *db)
{
	as_namespace *ns = as_namespace_get_byname(context);

	if (! ns) {
		cf_dyn_buf_append_string(db, "namespace not found;"); // TODO - start with "error"?
		return;
	}

	info_append_uint32(db, "replication-factor", ns->cfg_replication_factor);
	info_append_uint64(db, "memory-size", ns->memory_size);
	info_append_uint64(db, "default-ttl", ns->default_ttl);

	info_append_bool(db, "enable-xdr", ns->enable_xdr);
	info_append_bool(db, "sets-enable-xdr", ns->sets_enable_xdr);
	info_append_bool(db, "ns-forward-xdr-writes", ns->ns_forward_xdr_writes);
	info_append_bool(db, "allow-nonxdr-writes", ns->ns_allow_nonxdr_writes);
	info_append_bool(db, "allow-xdr-writes", ns->ns_allow_xdr_writes);

	// Not true config, but act as config overrides:
	cf_hist_track_get_settings(ns->read_hist, db);
	cf_hist_track_get_settings(ns->query_hist, db);
	cf_hist_track_get_settings(ns->udf_hist, db);
	cf_hist_track_get_settings(ns->write_hist, db);

	info_append_uint32(db, "cold-start-evict-ttl", ns->cold_start_evict_ttl);

	if (ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION) {
		info_append_string(db, "conflict-resolution-policy", "generation");
	}
	else if (ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME) {
		info_append_string(db, "conflict-resolution-policy", "last-update-time");
	}
	else {
		info_append_string(db, "conflict-resolution-policy", "undefined");
	}

	info_append_bool(db, "data-in-index", ns->data_in_index);
	info_append_bool(db, "disable-write-dup-res", ns->write_dup_res_disabled);
	info_append_bool(db, "disallow-null-setname", ns->disallow_null_setname);
	info_append_bool(db, "enable-benchmarks-batch-sub", ns->batch_sub_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-read", ns->read_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-udf", ns->udf_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-udf-sub", ns->udf_sub_benchmarks_enabled);
	info_append_bool(db, "enable-benchmarks-write", ns->write_benchmarks_enabled);
	info_append_bool(db, "enable-hist-proxy", ns->proxy_hist_enabled);
	info_append_uint32(db, "evict-hist-buckets", ns->evict_hist_buckets);
	info_append_uint32(db, "evict-tenths-pct", ns->evict_tenths_pct);
	info_append_uint32(db, "high-water-disk-pct", ns->hwm_disk_pct);
	info_append_uint32(db, "high-water-memory-pct", ns->hwm_memory_pct);
	info_append_uint64(db, "max-ttl", ns->max_ttl);
	info_append_uint32(db, "migrate-order", ns->migrate_order);
	info_append_uint32(db, "migrate-retransmit-ms", ns->migrate_retransmit_ms);
	info_append_uint32(db, "migrate-sleep", ns->migrate_sleep);
	info_append_uint32(db, "obj-size-hist-max", ns->obj_size_hist_max); // not original, may have been rounded
	info_append_uint32(db, "partition-tree-locks", ns->tree_shared.n_lock_pairs);
	info_append_uint32(db, "partition-tree-sprigs", ns->tree_shared.n_sprigs);
	info_append_uint32(db, "rack-id", ns->rack_id);
	info_append_string(db, "read-consistency-level-override", NS_READ_CONSISTENCY_LEVEL_NAME());
	info_append_bool(db, "single-bin", ns->single_bin);
	info_append_uint32(db, "stop-writes-pct", ns->stop_writes_pct);
	info_append_bool(db, "strong-consistency", ns->cp);
	info_append_bool(db, "strong-consistency-allow-expunge", ns->cp_allow_drops);
	info_append_uint32(db, "tomb-raider-eligible-age", ns->tomb_raider_eligible_age);
	info_append_uint32(db, "tomb-raider-period", ns->tomb_raider_period);
	info_append_string(db, "write-commit-level-override", NS_WRITE_COMMIT_LEVEL_NAME());

	info_append_string(db, "storage-engine",
			(ns->storage_type == AS_STORAGE_ENGINE_MEMORY ? "memory" :
				(ns->storage_type == AS_STORAGE_ENGINE_SSD ? "device" : "illegal")));

	if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
		for (int i = 0; i < AS_STORAGE_MAX_DEVICES; i++) {
			if (! ns->storage_devices[i]) {
				break;
			}

			info_append_string(db, "storage-engine.device", ns->storage_devices[i]);
		}

		for (int i = 0; i < AS_STORAGE_MAX_FILES; i++) {
			if (! ns->storage_files[i]) {
				break;
			}

			info_append_string(db, "storage-engine.file", ns->storage_files[i]);
		}

		// TODO - how to report the shadows?

		info_append_uint64(db, "storage-engine.filesize", ns->storage_filesize);
		info_append_string_safe(db, "storage-engine.scheduler-mode", ns->storage_scheduler_mode);
		info_append_uint32(db, "storage-engine.write-block-size", ns->storage_write_block_size);
		info_append_bool(db, "storage-engine.data-in-memory", ns->storage_data_in_memory);
		info_append_bool(db, "storage-engine.cold-start-empty", ns->storage_cold_start_empty);
		info_append_bool(db, "storage-engine.commit-to-device", ns->storage_commit_to_device);
		info_append_uint32(db, "storage-engine.commit-min-size", ns->storage_commit_min_size);
		info_append_uint32(db, "storage-engine.defrag-lwm-pct", ns->storage_defrag_lwm_pct);
		info_append_uint32(db, "storage-engine.defrag-queue-min", ns->storage_defrag_queue_min);
		info_append_uint32(db, "storage-engine.defrag-sleep", ns->storage_defrag_sleep);
		info_append_int(db, "storage-engine.defrag-startup-minimum", ns->storage_defrag_startup_minimum);
		info_append_bool(db, "storage-engine.disable-odirect", ns->storage_disable_odirect);
		info_append_bool(db, "storage-engine.enable-benchmarks-storage", ns->storage_benchmarks_enabled);
		info_append_bool(db, "storage-engine.enable-osync", ns->storage_enable_osync);
		info_append_string_safe(db, "storage-engine.encryption-key-file", ns->storage_encryption_key_file);
		info_append_uint64(db, "storage-engine.flush-max-ms", ns->storage_flush_max_us / 1000);
		info_append_uint64(db, "storage-engine.fsync-max-sec", ns->storage_fsync_max_us / 1000000);
		info_append_uint64(db, "storage-engine.max-write-cache", ns->storage_max_write_cache);
		info_append_uint32(db, "storage-engine.min-avail-pct", ns->storage_min_avail_pct);
		info_append_uint32(db, "storage-engine.post-write-queue", ns->storage_post_write_queue);
		info_append_uint32(db, "storage-engine.tomb-raider-sleep", ns->storage_tomb_raider_sleep);
		info_append_uint32(db, "storage-engine.write-threads", ns->storage_write_threads);
	}

	info_append_uint32(db, "sindex.num-partitions", ns->sindex_num_partitions);

	info_append_bool(db, "geo2dsphere-within.strict", ns->geo2dsphere_within_strict);
	info_append_uint32(db, "geo2dsphere-within.min-level", (uint32_t)ns->geo2dsphere_within_min_level);
	info_append_uint32(db, "geo2dsphere-within.max-level", (uint32_t)ns->geo2dsphere_within_max_level);
	info_append_uint32(db, "geo2dsphere-within.max-cells", (uint32_t)ns->geo2dsphere_within_max_cells);
	info_append_uint32(db, "geo2dsphere-within.level-mod", (uint32_t)ns->geo2dsphere_within_level_mod);
	info_append_uint32(db, "geo2dsphere-within.earth-radius-meters", ns->geo2dsphere_within_earth_radius_meters);
}


// TODO - security API?
void
info_security_config_get(cf_dyn_buf *db)
{
	info_append_bool(db, "enable-security", g_config.sec_cfg.security_enabled);
	info_append_uint32(db, "privilege-refresh-period", g_config.sec_cfg.privilege_refresh_period);
	info_append_uint32(db, "report-authentication-sinks", g_config.sec_cfg.report.authentication);
	info_append_uint32(db, "report-data-op-sinks", g_config.sec_cfg.report.data_op);
	info_append_uint32(db, "report-sys-admin-sinks", g_config.sec_cfg.report.sys_admin);
	info_append_uint32(db, "report-user-admin-sinks", g_config.sec_cfg.report.user_admin);
	info_append_uint32(db, "report-violation-sinks", g_config.sec_cfg.report.violation);
	info_append_int(db, "syslog-local", g_config.sec_cfg.syslog_local);
}


void
info_command_config_get_with_params(char *name, char *params, cf_dyn_buf *db)
{
	char context[1024];
	int context_len = sizeof(context);

	if (as_info_parameter_get(params, "context", context, &context_len) != 0) {
		cf_dyn_buf_append_string(db, "Error: Invalid get-config parameter;");
		return;
	}

	if (strcmp(context, "service") == 0) {
		info_service_config_get(db);
	}
	else if (strcmp(context, "network") == 0) {
		info_network_config_get(db);
	}
	else if (strcmp(context, "namespace") == 0) {
		context_len = sizeof(context);

		if (as_info_parameter_get(params, "id", context, &context_len) != 0) {
			cf_dyn_buf_append_string(db, "Error:invalid id;");
			return;
		}

		info_namespace_config_get(context, db);
	}
	else if (strcmp(context, "security") == 0) {
		info_security_config_get(db);
	}
	else if (strcmp(context, "xdr") == 0) {
		as_xdr_get_config(db);
	}
	else {
		cf_dyn_buf_append_string(db, "Error:Invalid context;");
	}
}


int
info_command_config_get(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "config-get command received: params %s", params);

	if (params && *params != 0) {
		info_command_config_get_with_params(name, params, db);
		cf_dyn_buf_chomp(db);
		return 0;
	}

	// We come here when context is not mentioned.
	// In that case we want to print everything.
	info_service_config_get(db);
	info_network_config_get(db);
	info_security_config_get(db);
	as_xdr_get_config(db);

	cf_dyn_buf_chomp(db);

	return 0;
}


//
// config-set:context=service;variable=value;
// config-set:context=network;variable=heartbeat.value;
// config-set:context=namespace;id=test;variable=value;
//
int
info_command_config_set_threadsafe(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "config-set command received: params %s", params);

	char context[1024];
	int  context_len = sizeof(context);
	int val;
	char bool_val[2][6] = {"false", "true"};

	if (0 != as_info_parameter_get(params, "context", context, &context_len))
		goto Error;
	if (strcmp(context, "service") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "advertise-ipv6", context, &context_len)) {
			if (strcmp(context, "true") == 0 || strcmp(context, "yes") == 0) {
				cf_socket_set_advertise_ipv6(true);
			}
			else if (strcmp(context, "false") == 0 || strcmp(context, "no") == 0) {
				cf_socket_set_advertise_ipv6(false);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "transaction-threads-per-queue", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < 1 || val > MAX_TRANSACTION_THREADS_PER_QUEUE) {
				cf_warning(AS_INFO, "transaction-threads-per-queue must be between 1 and %u", MAX_TRANSACTION_THREADS_PER_QUEUE);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of transaction-threads-per-queue from %u to %d ", g_config.n_transaction_threads_per_queue, val);
			as_tsvc_set_threads_per_queue((uint32_t)val);
		}
		else if (0 == as_info_parameter_get(params, "transaction-retry-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (val == 0)
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-retry-ms from %d to %d ", g_config.transaction_retry_ms, val);
			g_config.transaction_retry_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "transaction-max-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-retry-ms from %"PRIu64" to %d ", (g_config.transaction_max_ns / 1000000), val);
			g_config.transaction_max_ns = (uint64_t)val * 1000000;
		}
		else if (0 == as_info_parameter_get(params, "transaction-pending-limit", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-pending-limit from %d to %d ", g_config.transaction_pending_limit, val);
			g_config.transaction_pending_limit = val;
		}
		else if (0 == as_info_parameter_get(params, "ticker-interval", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of ticker-interval from %d to %d ", g_config.ticker_interval, val);
			g_config.ticker_interval = val;
		}
		else if (0 == as_info_parameter_get(params, "scan-max-active", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (val < 0 || val > 200) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of scan-max-active from %d to %d ", g_config.scan_max_active, val);
			g_config.scan_max_active = val;
			as_scan_limit_active_jobs(g_config.scan_max_active);
		}
		else if (0 == as_info_parameter_get(params, "scan-max-done", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (val < 0 || val > 1000) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of scan-max-done from %d to %d ", g_config.scan_max_done, val);
			g_config.scan_max_done = val;
			as_scan_limit_finished_jobs(g_config.scan_max_done);
		}
		else if (0 == as_info_parameter_get(params, "scan-max-udf-transactions", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of scan-max-udf-transactions from %d to %d ", g_config.scan_max_udf_transactions, val);
			g_config.scan_max_udf_transactions = val;
		}
		else if (0 == as_info_parameter_get(params, "scan-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (val < 0 || val > 128) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of scan-threads from %d to %d ", g_config.scan_threads, val);
			g_config.scan_threads = val;
			as_scan_resize_thread_pool(g_config.scan_threads);
		}
		else if (0 == as_info_parameter_get(params, "batch-index-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (0 != as_batch_threads_resize(val))
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "batch-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (0 != as_batch_direct_threads_resize(val))
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "batch-max-requests", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-max-requests from %d to %d ", g_config.batch_max_requests, val);
			g_config.batch_max_requests = val;
		}
		else if (0 == as_info_parameter_get(params, "batch-max-buffers-per-queue", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-max-buffers-per-queue from %d to %d ", g_config.batch_max_buffers_per_queue, val);
			g_config.batch_max_buffers_per_queue = val;
		}
		else if (0 == as_info_parameter_get(params, "batch-max-unused-buffers", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-max-unused-buffers from %d to %d ", g_config.batch_max_unused_buffers, val);
			g_config.batch_max_unused_buffers = val;
		}
		else if (0 == as_info_parameter_get(params, "batch-priority", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-priority from %d to %d ", g_config.batch_priority, val);
			g_config.batch_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-fd-max", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-fd-max from %d to %d ", g_config.n_proto_fd_max, val);
			g_config.n_proto_fd_max = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-fd-idle-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-fd-idle-ms from %d to %d ", g_config.proto_fd_idle_ms, val);
			g_config.proto_fd_idle_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-slow-netio-sleep-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-slow-netio-sleep-ms from %d to %d ", g_config.proto_slow_netio_sleep_ms, val);
			g_config.proto_slow_netio_sleep_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "nsup-delete-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of nsup-delete-sleep from %d to %d ", g_config.nsup_delete_sleep, val);
			g_config.nsup_delete_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "nsup-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of nsup-period from %d to %d ", g_config.nsup_period, val);
			g_config.nsup_period = val;
		}
		else if (0 == as_info_parameter_get( params, "cluster-name", context, &context_len)){
			if (!as_config_cluster_name_set(context)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of cluster-name to '%s'", context);
		}
		else if (0 == as_info_parameter_get(params, "migrate-max-num-incoming", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if ((uint32_t)val > AS_MIGRATE_LIMIT_MAX_NUM_INCOMING) {
				cf_warning(AS_INFO, "migrate-max-num-incoming %d must be >= 0 and <= %u", val, AS_MIGRATE_LIMIT_MAX_NUM_INCOMING);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of migrate-max-num-incoming from %u to %d ", g_config.migrate_max_num_incoming, val);
			g_config.migrate_max_num_incoming = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if ((uint32_t)val > MAX_NUM_MIGRATE_XMIT_THREADS) {
				cf_warning(AS_INFO, "migrate-threads %d must be >= 0 and <= %u", val, MAX_NUM_MIGRATE_XMIT_THREADS);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of migrate-threads from %u to %d ", g_config.n_migrate_threads, val);
			as_migrate_set_num_xmit_threads(val);
		}
		else if (0 == as_info_parameter_get(params, "min-cluster-size", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || (0 > val) || (as_clustering_cluster_size_min_set(val) < 0))
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "prole-extra-ttl", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of prole-extra-ttl from %d to %d ", g_config.prole_extra_ttl, val);
			g_config.prole_extra_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "query-buf-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-buf-size = %"PRIu64"", val);
			if (val < 1024) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-buf-size from %"PRIu64" to %"PRIu64"", g_config.query_buf_size, val);
			g_config.query_buf_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-threshold", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-threshold = %"PRIu64"", val);
			if ((int64_t)val <= 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-threshold from %u to %"PRIu64, g_config.query_threshold, val);
			g_config.query_threshold = val;
		}
		else if (0 == as_info_parameter_get(params, "query-untracked-time-ms", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-untracked-time = %"PRIu64" milli seconds", val);
			if ((int64_t)val < 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-untracked-time from %"PRIu64" milli seconds to %"PRIu64" milli seconds",
						g_config.query_untracked_time_ms, val);
			g_config.query_untracked_time_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "query-rec-count-bound", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-rec-count-bound = %"PRIu64"", val);
			if ((int64_t)val <= 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-rec-count-bound from %"PRIu64" to %"PRIu64" ", g_config.query_rec_count_bound, val);
			g_config.query_rec_count_bound = val;
		}
		else if (0 == as_info_parameter_get(params, "sindex-builder-threads", context, &context_len)) {
			int val = 0;
			if (0 != cf_str_atoi(context, &val) || (val > MAX_SINDEX_BUILDER_THREADS)) {
				cf_warning(AS_INFO, "sindex-builder-threads: value must be <= %d, not %s", MAX_SINDEX_BUILDER_THREADS, context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of sindex-builder-threads from %u to %d", g_config.sindex_builder_threads, val);
			g_config.sindex_builder_threads = (uint32_t)val;
			as_sbld_resize_thread_pool(g_config.sindex_builder_threads);
		}
		else if (0 == as_info_parameter_get(params, "sindex-gc-max-rate", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of sindex-gc-max-rate from %d to %d ", g_config.sindex_gc_max_rate, val);
			g_config.sindex_gc_max_rate = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "sindex-gc-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of sindex-gc-period from %d to %d ", g_config.sindex_gc_period, val);
			g_config.sindex_gc_period = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "query-threads", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-threads = %"PRIu64, val);
			if (val == 0) {
				cf_warning(AS_INFO, "query-threads should be a number %s", context);
				goto Error;
			}
			int old_val = g_config.query_threads;
			int new_val = 0;
			if (as_query_reinit(val, &new_val) != AS_QUERY_OK) {
				cf_warning(AS_INFO, "Config not changed.");
				goto Error;
			}

			cf_info(AS_INFO, "Changing value of query-threads from %d to %d",
					old_val, new_val);
		}
		else if (0 == as_info_parameter_get(params, "query-worker-threads", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-worker-threads = %"PRIu64, val);
			if (val == 0) {
				cf_warning(AS_INFO, "query-worker-threads should be a number %s", context);
				goto Error;
			}
			int old_val = g_config.query_threads;
			int new_val = 0;
			if (as_query_worker_reinit(val, &new_val) != AS_QUERY_OK) {
				cf_warning(AS_INFO, "Config not changed.");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-worker-threads from %d to %d",
					old_val, new_val);
		}
		else if (0 == as_info_parameter_get(params, "query-priority", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query_priority = %"PRIu64, val);
			if (val == 0) {
				cf_warning(AS_INFO, "query_priority should be a number %s", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-priority from %d to %"PRIu64, g_config.query_priority, val);
			g_config.query_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "query-priority-sleep-us", context, &context_len)) {
			uint64_t val = atoll(context);
			if(val == 0) {
				cf_warning(AS_INFO, "query_sleep should be a number %s", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-sleep from %"PRIu64" uSec to %"PRIu64" uSec ", g_config.query_sleep_us, val);
			g_config.query_sleep_us = val;
		}
		else if (0 == as_info_parameter_get(params, "query-batch-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-batch-size = %"PRIu64, val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-batch-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-batch-size from %d to %"PRIu64, g_config.query_bsize, val);
			g_config.query_bsize = val;
		}
		else if (0 == as_info_parameter_get(params, "query-req-max-inflight", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-req-max-inflight = %"PRIu64, val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-req-max-inflight should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-req-max-inflight from %d to %"PRIu64, g_config.query_req_max_inflight, val);
			g_config.query_req_max_inflight = val;
		}
		else if (0 == as_info_parameter_get(params, "query-bufpool-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-bufpool-size = %"PRIu64, val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-bufpool-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-bufpool-size from %d to %"PRIu64, g_config.query_bufpool_size, val);
			g_config.query_bufpool_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-in-transaction-thread", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-in-transaction-thread  from %s to %s", bool_val[g_config.query_in_transaction_thr], context);
				g_config.query_in_transaction_thr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-in-transaction-thread  from %s to %s", bool_val[g_config.query_in_transaction_thr], context);
				g_config.query_in_transaction_thr = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "query-req-in-query-thread", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-req-in-query-thread from %s to %s", bool_val[g_config.query_req_in_query_thread], context);
				g_config.query_req_in_query_thread = true;

			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-req-in-query-thread from %s to %s", bool_val[g_config.query_req_in_query_thread], context);
				g_config.query_req_in_query_thread = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "query-short-q-max-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-short-q-max-size = %"PRIu64, val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-short-q-max-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-short-q-max-size from %d to %"PRIu64, g_config.query_short_q_max_size, val);
			g_config.query_short_q_max_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-long-q-max-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-long-q-max-size = %"PRIu64, val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-long-q-max-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-longq-max-size from %d to %"PRIu64, g_config.query_long_q_max_size, val);
			g_config.query_long_q_max_size = val;
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-fabric", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-fabric to %s", context);
				g_config.fabric_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-fabric to %s", context);
				g_config.fabric_benchmarks_enabled = false;
				histogram_clear(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_BULK]);
				histogram_clear(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_BULK]);
				histogram_clear(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_BULK]);
				histogram_clear(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_BULK]);
				histogram_clear(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_CTRL]);
				histogram_clear(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_CTRL]);
				histogram_clear(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_CTRL]);
				histogram_clear(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_CTRL]);
				histogram_clear(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_META]);
				histogram_clear(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_META]);
				histogram_clear(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_META]);
				histogram_clear(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_META]);
				histogram_clear(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_RW]);
				histogram_clear(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_RW]);
				histogram_clear(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_RW]);
				histogram_clear(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_RW]);
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-svc", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-svc to %s", context);
				g_config.svc_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-svc to %s", context);
				g_config.svc_benchmarks_enabled = false;
				histogram_clear(g_stats.svc_demarshal_hist);
				histogram_clear(g_stats.svc_queue_hist);
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-hist-info", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-hist-info to %s", context);
				g_config.info_hist_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-hist-info to %s", context);
				g_config.info_hist_enabled = false;
				histogram_clear(g_stats.info_hist);
			}
		}
		else if (0 == as_info_parameter_get(params, "query-microbenchmark", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-enable-histogram to %s", context);
				g_config.query_enable_histogram = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-enable-histogram to %s", context);
				g_config.query_enable_histogram = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "query-pre-reserve-partitions", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-pre-reserve-partitions to %s", context);
				g_config.partitions_pre_reserved = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-pre-reserve-partitions to %s", context);
				g_config.partitions_pre_reserved = false;
			}
			else {
				goto Error;
			}
		}
		else {
			goto Error;
		}
	}
	else if (strcmp(context, "network") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "heartbeat.interval", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (as_hb_tx_interval_set(val) != 0) {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "heartbeat.timeout", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (as_hb_max_intervals_missed_set(val) != 0){
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "heartbeat.mtu", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			as_hb_override_mtu_set(val);
		}
		else if (0 == as_info_parameter_get(params, "heartbeat.protocol", context, &context_len)) {
			as_hb_protocol protocol =	(!strcmp(context, "v3") ? AS_HB_PROTOCOL_V3 :
											(!strcmp(context, "reset") ? AS_HB_PROTOCOL_RESET :
												(!strcmp(context, "none") ? AS_HB_PROTOCOL_NONE :
													AS_HB_PROTOCOL_UNDEF)));
			if (AS_HB_PROTOCOL_UNDEF == protocol) {
				cf_warning(AS_INFO, "heartbeat protocol version %s not supported", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of heartbeat protocol version to %s", context);
			if (0 > as_hb_protocol_set(protocol))
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "fabric.channel-bulk-recv-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
				cf_warning(AS_INFO, "fabric.channel-bulk-recv-threads must be between 1 and %u", MAX_FABRIC_CHANNEL_THREADS);
				goto Error;
			}
			cf_info(AS_FABRIC, "changing fabric.channel-bulk-recv-threads from %u to %d", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK], val);
			as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_BULK, val);
		}
		else if (0 == as_info_parameter_get(params, "fabric.channel-ctrl-recv-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
				cf_warning(AS_INFO, "fabric.channel-ctrl-recv-threads must be between 1 and %u", MAX_FABRIC_CHANNEL_THREADS);
				goto Error;
			}
			cf_info(AS_FABRIC, "changing fabric.channel-ctrl-recv-threads from %u to %d", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_CTRL], val);
			as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_CTRL, val);
		}
		else if (0 == as_info_parameter_get(params, "fabric.channel-meta-recv-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
				cf_warning(AS_INFO, "fabric.channel-meta-recv-threads must be between 1 and %u", MAX_FABRIC_CHANNEL_THREADS);
				goto Error;
			}
			cf_info(AS_FABRIC, "changing fabric.channel-meta-recv-threads from %u to %d", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_META], val);
			as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_META, val);
		}
		else if (0 == as_info_parameter_get(params, "fabric.channel-rw-recv-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < 1 || val > MAX_FABRIC_CHANNEL_THREADS) {
				cf_warning(AS_INFO, "fabric.channel-rw-recv-threads must be between 1 and %u", MAX_FABRIC_CHANNEL_THREADS);
				goto Error;
			}
			cf_info(AS_FABRIC, "changing fabric.channel-rw-recv-threads from %u to %d", g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW], val);
			as_fabric_set_recv_threads(AS_FABRIC_CHANNEL_RW, val);
		}
		else if (0 == as_info_parameter_get(params, "fabric.recv-rearm-threshold", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}

			if (val < 0 || val > 1024 * 1024) {
				goto Error;
			}

			g_config.fabric_recv_rearm_threshold = (uint32_t)val;
		}
		else
			goto Error;
	}
	else if (strcmp(context, "namespace") == 0) {
		context_len = sizeof(context);
		if (0 != as_info_parameter_get(params, "id", context, &context_len))
			goto Error;
		as_namespace *ns = as_namespace_get_byname(context);
		if (!ns)
			goto Error;

		context_len = sizeof(context);
		// configure namespace/set related parameters:
		if (0 == as_info_parameter_get(params, "set", context, &context_len)) {
			if (context_len == 0 || context_len >= AS_SET_NAME_MAX_SIZE) {
				cf_warning(AS_INFO, "illegal length %d for set name %s",
						context_len, context);
				goto Error;
			}

			char set_name[AS_SET_NAME_MAX_SIZE];
			size_t set_name_len = (size_t)context_len;

			strcpy(set_name, context);

			// Ideally, set operations should not be part of configs. But,
			// set-delete is exception for historical reasons. Do an early check
			// and bail out if set doesn't exist.
			uint16_t set_id = as_namespace_get_set_id(ns, set_name);
			if (set_id == INVALID_SET_ID) {
				context_len = sizeof(context);
				if (0 == as_info_parameter_get(params, "set-delete", context,
						&context_len)) {
					cf_warning(AS_INFO, "set-delete failed because set %s doesn't exist in ns %s",
							set_name, ns->name);
					goto Error;
				}
			}

			// configurations should create set if it doesn't exist.
			// checks if there is a vmap set with the same name and if so returns
			// a ptr to it. if not, it creates an set structure, initializes it
			// and returns a ptr to it.
			as_set *p_set = NULL;
			if (as_namespace_get_create_set_w_len(ns, set_name, set_name_len,
					&p_set, NULL) != 0) {
				goto Error;
			}

			context_len = sizeof(context);
			if (0 == as_info_parameter_get(params, "set-enable-xdr", context, &context_len)) {
				// TODO - make sure context is null-terminated.
				if ((strncmp(context, "true", 4) == 0) || (strncmp(context, "yes", 3) == 0)) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_TRUE);
				}
				else if ((strncmp(context, "false", 5) == 0) || (strncmp(context, "no", 2) == 0)) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_FALSE);
				}
				else if (strncmp(context, "use-default", 11) == 0) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_DEFAULT);
				}
				else {
					goto Error;
				}
			}
			else if (0 == as_info_parameter_get(params, "set-disable-eviction", context, &context_len)) {
				if ((strncmp(context, "true", 4) == 0) || (strncmp(context, "yes", 3) == 0)) {
					cf_info(AS_INFO, "Changing value of set-disable-eviction of ns %s set %s to %s", ns->name, p_set->name, context);
					DISABLE_SET_EVICTION(p_set, true);
				}
				else if ((strncmp(context, "false", 5) == 0) || (strncmp(context, "no", 2) == 0)) {
					cf_info(AS_INFO, "Changing value of set-disable-eviction of ns %s set %s to %s", ns->name, p_set->name, context);
					DISABLE_SET_EVICTION(p_set, false);
				}
				else {
					goto Error;
				}
			}
			else if (0 == as_info_parameter_get(params, "set-stop-writes-count", context, &context_len)) {
				uint64_t val = atoll(context);
				cf_info(AS_INFO, "Changing value of set-stop-writes-count of ns %s set %s to %lu", ns->name, p_set->name, val);
				cf_atomic64_set(&p_set->stop_writes_count, val);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "memory-size", context, &context_len)) {
			uint64_t val;

			if (0 != cf_str_atoi_u64(context, &val)) {
				goto Error;
			}
			cf_debug(AS_INFO, "memory-size = %"PRIu64"", val);
			if (val > ns->memory_size)
				ns->memory_size = val;
			if (val < (ns->memory_size / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of memory-size of ns %s from %"PRIu64" to %"PRIu64, ns->name, ns->memory_size, val);
			ns->memory_size = val;
		}
		else if (0 == as_info_parameter_get(params, "high-water-disk-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 0 || val > 100) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of high-water-disk-pct of ns %s from %u to %d ", ns->name, ns->hwm_disk_pct, val);
			ns->hwm_disk_pct = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "high-water-memory-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 0 || val > 100) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of high-water-memory-pct memory of ns %s from %u to %d ", ns->name, ns->hwm_memory_pct, val);
			ns->hwm_memory_pct = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "evict-tenths-pct", context, &context_len)) {
			cf_info(AS_INFO, "Changing value of evict-tenths-pct memory of ns %s from %d to %d ", ns->name, ns->evict_tenths_pct, atoi(context));
			ns->evict_tenths_pct = atoi(context);
		}
		else if (0 == as_info_parameter_get(params, "evict-hist-buckets", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 100 || val > 10000000) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of evict-hist-buckets of ns %s from %u to %d ", ns->name, ns->evict_hist_buckets, val);
			ns->evict_hist_buckets = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "stop-writes-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 0 || val > 100) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of stop-writes-pct memory of ns %s from %u to %d ", ns->name, ns->stop_writes_pct, val);
			ns->stop_writes_pct = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "default-ttl", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "default-ttl must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			if (val > ns->max_ttl) {
				cf_warning(AS_INFO, "default-ttl must be <= max-ttl (%lu seconds)", ns->max_ttl);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of default-ttl memory of ns %s from %"PRIu64" to %"PRIu64" ", ns->name, ns->default_ttl, val);
			ns->default_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "max-ttl", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "max-ttl must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			if (val == 0 || val > MAX_ALLOWED_TTL) {
				cf_warning(AS_INFO, "max-ttl must be non-zero and <= %u seconds", MAX_ALLOWED_TTL);
				goto Error;
			}
			if (val < ns->default_ttl) {
				cf_warning(AS_INFO, "max-ttl must be >= default-ttl (%lu seconds)", ns->default_ttl);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of max-ttl memory of ns %s from %"PRIu64" to %"PRIu64" ", ns->name, ns->max_ttl, val);
			ns->max_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-order", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 1 || val > 10) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of migrate-order of ns %s from %u to %d", ns->name, ns->migrate_order, val);
			ns->migrate_order = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-retransmit-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of migrate-retransmit-ms of ns %s from %u to %d", ns->name, ns->migrate_retransmit_ms, val);
			ns->migrate_retransmit_ms = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of migrate-sleep of ns %s from %u to %d", ns->name, ns->migrate_sleep, val);
			ns->migrate_sleep = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "tomb-raider-eligible-age", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "tomb-raider-eligible-age must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of tomb-raider-eligible-age of ns %s from %u to %lu", ns->name, ns->tomb_raider_eligible_age, val);
			ns->tomb_raider_eligible_age = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "tomb-raider-period", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "tomb-raider-period must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of tomb-raider-period of ns %s from %u to %lu", ns->name, ns->tomb_raider_period, val);
			ns->tomb_raider_period = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "tomb-raider-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of tomb-raider-sleep of ns %s from %u to %d", ns->name, ns->storage_tomb_raider_sleep, val);
			ns->storage_tomb_raider_sleep = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "obj-size-hist-max", context, &context_len)) {
			uint32_t hist_max = (uint32_t)atoi(context);
			uint32_t round_to = OBJ_SIZE_HIST_NUM_BUCKETS;
			uint32_t round_max = hist_max ? ((hist_max + round_to - 1) / round_to) * round_to : round_to;
			if (round_max != hist_max) {
				cf_info(AS_INFO, "rounding obj-size-hist-max %u up to %u", hist_max, round_max);
			}
			cf_info(AS_INFO, "Changing value of obj-size-hist-max of ns %s to %u", ns->name, round_max);
			cf_atomic32_set(&ns->obj_size_hist_max, round_max); // in 128-byte blocks
		}
		else if (0 == as_info_parameter_get(params, "rack-id", context, &context_len)) {
			if (as_config_error_enterprise_only()) {
				cf_warning(AS_INFO, "rack-id is enterprise-only");
				goto Error;
			}
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if ((uint32_t)val > MAX_RACK_ID) {
				cf_warning(AS_INFO, "rack-id %d must be >= 0 and <= %u", val, MAX_RACK_ID);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of rack-id of ns %s from %u to %d", ns->name, ns->rack_id, val);
			ns->rack_id = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "conflict-resolution-policy", context, &context_len)) {
			if (ns->cp) {
				cf_warning(AS_INFO, "{%s} 'conflict-resolution-policy' is not applicable with 'strong-consistency'", ns->name);
				goto Error;
			}
			if (strncmp(context, "generation", 10) == 0) {
				cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s", ns->name, ns->conflict_resolution_policy, context);
				ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
			}
			else if (strncmp(context, "last-update-time", 16) == 0) {
				cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s", ns->name, ns->conflict_resolution_policy, context);
				ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "defrag-lwm-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-lwm-pct of ns %s from %d to %d ", ns->name, ns->storage_defrag_lwm_pct, val);

			uint32_t old_val = ns->storage_defrag_lwm_pct;

			ns->storage_defrag_lwm_pct = val;
			ns->defrag_lwm_size = (ns->storage_write_block_size * ns->storage_defrag_lwm_pct) / 100;

			if (ns->storage_defrag_lwm_pct > old_val) {
				as_storage_defrag_sweep(ns);
			}
		}
		else if (0 == as_info_parameter_get(params, "defrag-queue-min", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-queue-min of ns %s from %u to %d", ns->name, ns->storage_defrag_queue_min, val);
			ns->storage_defrag_queue_min = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "defrag-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-sleep of ns %s from %u to %d", ns->name, ns->storage_defrag_sleep, val);
			ns->storage_defrag_sleep = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "flush-max-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of flush-max-ms of ns %s from %lu to %d", ns->name, ns->storage_flush_max_us / 1000, val);
			ns->storage_flush_max_us = (uint64_t)val * 1000;
		}
		else if (0 == as_info_parameter_get(params, "fsync-max-sec", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of fsync-max-sec of ns %s from %lu to %d", ns->name, ns->storage_fsync_max_us / 1000000, val);
			ns->storage_fsync_max_us = (uint64_t)val * 1000000;
		}
		else if (0 == as_info_parameter_get(params, "enable-xdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->enable_xdr], context);
				ns->enable_xdr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->enable_xdr], context);
				ns->enable_xdr = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "sets-enable-xdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->sets_enable_xdr], context);
				ns->sets_enable_xdr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->sets_enable_xdr], context);
				ns->sets_enable_xdr = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "ns-forward-xdr-writes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of ns-forward-xdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_forward_xdr_writes], context);
				ns->ns_forward_xdr_writes = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of ns-forward-xdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_forward_xdr_writes], context);
				ns->ns_forward_xdr_writes = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "allow-nonxdr-writes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of allow-nonxdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_allow_nonxdr_writes], context);
				ns->ns_allow_nonxdr_writes = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of allow-nonxdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_allow_nonxdr_writes], context);
				ns->ns_allow_nonxdr_writes = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "allow-xdr-writes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of allow-xdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_allow_xdr_writes], context);
				ns->ns_allow_xdr_writes = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of allow-xdr-writes of ns %s from %s to %s", ns->name, bool_val[ns->ns_allow_xdr_writes], context);
				ns->ns_allow_xdr_writes = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "strong-consistency-allow-expunge", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of strong-consistency-allow-expunge of ns %s from %s to %s", ns->name, bool_val[ns->cp_allow_drops], context);
				ns->cp_allow_drops = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of strong-consistency-allow-expunge of ns %s from %s to %s", ns->name, bool_val[ns->cp_allow_drops], context);
				ns->cp_allow_drops = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "disable-write-dup-res", context, &context_len)) {
			if (ns->cp) {
				cf_warning(AS_INFO, "{%s} 'disable-write-dup-res' is not applicable with 'strong-consistency'", ns->name);
				goto Error;
			}
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of disable-write-dup-res of ns %s from %s to %s", ns->name, bool_val[ns->write_dup_res_disabled], context);
				ns->write_dup_res_disabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of disable-write-dup-res of ns %s from %s to %s", ns->name, bool_val[ns->write_dup_res_disabled], context);
				ns->write_dup_res_disabled = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "disallow-null-setname", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s", ns->name, bool_val[ns->disallow_null_setname], context);
				ns->disallow_null_setname = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s", ns->name, bool_val[ns->disallow_null_setname], context);
				ns->disallow_null_setname = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-batch-sub", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-batch-sub of ns %s from %s to %s", ns->name, bool_val[ns->batch_sub_benchmarks_enabled], context);
				ns->batch_sub_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-batch-sub of ns %s from %s to %s", ns->name, bool_val[ns->batch_sub_benchmarks_enabled], context);
				ns->batch_sub_benchmarks_enabled = false;
				histogram_clear(ns->batch_sub_start_hist);
				histogram_clear(ns->batch_sub_restart_hist);
				histogram_clear(ns->batch_sub_dup_res_hist);
				histogram_clear(ns->batch_sub_repl_ping_hist);
				histogram_clear(ns->batch_sub_read_local_hist);
				histogram_clear(ns->batch_sub_response_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-read", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-read of ns %s from %s to %s", ns->name, bool_val[ns->read_benchmarks_enabled], context);
				ns->read_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-read of ns %s from %s to %s", ns->name, bool_val[ns->read_benchmarks_enabled], context);
				ns->read_benchmarks_enabled = false;
				histogram_clear(ns->read_start_hist);
				histogram_clear(ns->read_restart_hist);
				histogram_clear(ns->read_dup_res_hist);
				histogram_clear(ns->read_repl_ping_hist);
				histogram_clear(ns->read_local_hist);
				histogram_clear(ns->read_response_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-storage", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-storage of ns %s from %s to %s", ns->name, bool_val[ns->storage_benchmarks_enabled], context);
				ns->storage_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-storage of ns %s from %s to %s", ns->name, bool_val[ns->storage_benchmarks_enabled], context);
				ns->storage_benchmarks_enabled = false;
				as_storage_histogram_clear_all(ns);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-udf", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-udf of ns %s from %s to %s", ns->name, bool_val[ns->udf_benchmarks_enabled], context);
				ns->udf_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-udf of ns %s from %s to %s", ns->name, bool_val[ns->udf_benchmarks_enabled], context);
				ns->udf_benchmarks_enabled = false;
				histogram_clear(ns->udf_start_hist);
				histogram_clear(ns->udf_restart_hist);
				histogram_clear(ns->udf_dup_res_hist);
				histogram_clear(ns->udf_master_hist);
				histogram_clear(ns->udf_repl_write_hist);
				histogram_clear(ns->udf_response_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-udf-sub", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-udf-sub of ns %s from %s to %s", ns->name, bool_val[ns->udf_sub_benchmarks_enabled], context);
				ns->udf_sub_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-udf-sub of ns %s from %s to %s", ns->name, bool_val[ns->udf_sub_benchmarks_enabled], context);
				ns->udf_sub_benchmarks_enabled = false;
				histogram_clear(ns->udf_sub_start_hist);
				histogram_clear(ns->udf_sub_restart_hist);
				histogram_clear(ns->udf_sub_dup_res_hist);
				histogram_clear(ns->udf_sub_master_hist);
				histogram_clear(ns->udf_sub_repl_write_hist);
				histogram_clear(ns->udf_sub_response_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-benchmarks-write", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-write of ns %s from %s to %s", ns->name, bool_val[ns->write_benchmarks_enabled], context);
				ns->write_benchmarks_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-benchmarks-write of ns %s from %s to %s", ns->name, bool_val[ns->write_benchmarks_enabled], context);
				ns->write_benchmarks_enabled = false;
				histogram_clear(ns->write_start_hist);
				histogram_clear(ns->write_restart_hist);
				histogram_clear(ns->write_dup_res_hist);
				histogram_clear(ns->write_master_hist);
				histogram_clear(ns->write_repl_write_hist);
				histogram_clear(ns->write_response_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "enable-hist-proxy", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-hist-proxy of ns %s from %s to %s", ns->name, bool_val[ns->proxy_hist_enabled], context);
				ns->proxy_hist_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-hist-proxy of ns %s from %s to %s", ns->name, bool_val[ns->proxy_hist_enabled], context);
				ns->proxy_hist_enabled = false;
				histogram_clear(ns->proxy_hist);
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "max-write-cache", context, &context_len)) {
			uint64_t val_u64;

			if (0 != cf_str_atoi_u64(context, &val_u64)) {
				goto Error;
			}
			if (val_u64 < (1024 * 1024 * 4)) { // TODO - why enforce this? And here, but not cfg.c?
				cf_warning(AS_INFO, "can't set max-write-cache less than 4M");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of max-write-cache of ns %s from %lu to %lu ", ns->name, ns->storage_max_write_cache, val_u64);
			ns->storage_max_write_cache = val_u64;
			ns->storage_max_write_q = (int)(ns->storage_max_write_cache / ns->storage_write_block_size);
		}
		else if (0 == as_info_parameter_get(params, "min-avail-pct", context, &context_len)) {
			ns->storage_min_avail_pct = atoi(context);
			cf_info(AS_INFO, "Changing value of min-avail-pct of ns %s from %u to %u ", ns->name, ns->storage_min_avail_pct, atoi(context));
		}
		else if (0 == as_info_parameter_get(params, "post-write-queue", context, &context_len)) {
			if (ns->storage_data_in_memory) {
				cf_warning(AS_INFO, "ns %s, can't set post-write-queue if data-in-memory", ns->name);
				goto Error;
			}
			if (0 != cf_str_atoi(context, &val)) {
				cf_warning(AS_INFO, "ns %s, post-write-queue %s is not a number", ns->name, context);
				goto Error;
			}
			if ((uint32_t)val > (4 * 1024)) {
				cf_warning(AS_INFO, "ns %s, post-write-queue %u must be < 4K", ns->name, val);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of post-write-queue of ns %s from %d to %d ", ns->name, ns->storage_post_write_queue, val);
			cf_atomic32_set(&ns->storage_post_write_queue, (uint32_t)val);
		}
		else if (0 == as_info_parameter_get(params, "read-consistency-level-override", context, &context_len)) {
			if (ns->cp) {
				cf_warning(AS_INFO, "{%s} 'read-consistency-level-override' is not applicable with 'strong-consistency'", ns->name);
				goto Error;
			}
			char *original_value = NS_READ_CONSISTENCY_LEVEL_NAME();
			if (strcmp(context, "all") == 0) {
				ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ALL;
			}
			else if (strcmp(context, "off") == 0) {
				ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_PROTO;
			}
			else if (strcmp(context, "one") == 0) {
				ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_ONE;
			}
			else {
				goto Error;
			}
			if (strcmp(original_value, context)) {
				cf_info(AS_INFO, "Changing value of read-consistency-level-override of ns %s from %s to %s", ns->name, original_value, context);
			}
		}
		else if (0 == as_info_parameter_get(params, "write-commit-level-override", context, &context_len)) {
			if (ns->cp) {
				cf_warning(AS_INFO, "{%s} 'write-commit-level-override' is not applicable with 'strong-consistency'", ns->name);
				goto Error;
			}
			char *original_value = NS_WRITE_COMMIT_LEVEL_NAME();
			if (strcmp(context, "all") == 0) {
				ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_ALL;
			}
			else if (strcmp(context, "master") == 0) {
				ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_MASTER;
			}
			else if (strcmp(context, "off") == 0) {
				ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_PROTO;
			}
			else {
				goto Error;
			}
			if (strcmp(original_value, context)) {
				cf_info(AS_INFO, "Changing value of write-commit-level-override of ns %s from %s to %s", ns->name, original_value, context);
			}
		}
		else if (0 == as_info_parameter_get(params, "geo2dsphere-within-max-cells", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-cells %s is not a number", ns->name, context);
				goto Error;
			}
			if (val <= 0) {
				cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-cells %u must be > 0", ns->name, val);
				goto Error;
			}
			if ((uint32_t)val > (MAX_REGION_CELLS)) {
				cf_warning(AS_INFO, "ns %s, geo2dsphere-within-max-cells %u must be <= %u", ns->name, val, MAX_REGION_CELLS);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of geo2dsphere-within-max-cells of ns %s from %d to %d ",
					ns->name, ns->geo2dsphere_within_max_cells, val);
			ns->geo2dsphere_within_max_cells = val;
		}
		else {
			if (as_xdr_set_config_ns(ns->name, params) == false) {
				goto Error;
			}
		}
	} // end of namespace stanza
	else if (strcmp(context, "security") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "privilege-refresh-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 10 || val > 60 * 60 * 24) {
				cf_warning(AS_INFO, "privilege-refresh-period must be an unsigned integer between 10 and 86400");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of privilege-refresh-period from %u to %d", g_config.sec_cfg.privilege_refresh_period, val);
			g_config.sec_cfg.privilege_refresh_period = (uint32_t)val;
		}
		else {
			goto Error;
		}
	}
	else if (strcmp(context, "xdr") == 0) {
		if (as_xdr_set_config(params) == false) {
			goto Error;
		}
	}
	else
		goto Error;

	cf_info(AS_INFO, "config-set command completed: params %s",params);
	cf_dyn_buf_append_string(db, "ok");
	return(0);

Error:
	cf_dyn_buf_append_string(db, "error");
	return(0);
}

// Protect all set-config commands from concurrency issues.
static pthread_mutex_t g_set_cfg_lock = PTHREAD_MUTEX_INITIALIZER;

int
info_command_config_set(char *name, char *params, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_set_cfg_lock);

	int result = info_command_config_set_threadsafe(name, params, db);

	pthread_mutex_unlock(&g_set_cfg_lock);

	return result;
}

//
// log-set:log=id;context=foo;level=bar
// ie:
//   log-set:log=0;context=rw;level=debug


int
info_command_log_set(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "log-set command received: params %s", params);

	char id_str[50];
	int  id_str_len = sizeof(id_str);
	int  id = -1;
	bool found_id = true;
	cf_fault_sink *s = 0;

	if (0 != as_info_parameter_get(params, "id", id_str, &id_str_len)) {
		if (0 != as_info_parameter_get(params, "log", id_str, &id_str_len)) {
			cf_debug(AS_INFO, "log set command: no log id to be set - doing all");
			found_id = false;
		}
	}
	if (found_id == true) {
		if (0 != cf_str_atoi(id_str, &id) ) {
			cf_info(AS_INFO, "log set command: id must be an integer, is: %s", id_str);
			cf_dyn_buf_append_string(db, "error-id-not-integer");
			return(0);
		}
		s = cf_fault_sink_get_id(id);
		if (!s) {
			cf_info(AS_INFO, "log set command: sink id %d invalid", id);
			cf_dyn_buf_append_string(db, "error-bad-id");
			return(0);
		}
	}

	// now, loop through all context strings. If we find a known context string,
	// do the set
	for (int c_id = 0; c_id < CF_FAULT_CONTEXT_UNDEF; c_id++) {

		char level_str[50];
		int  level_str_len = sizeof(level_str);
		char *context = cf_fault_context_strings[c_id];
		if (0 != as_info_parameter_get(params, context, level_str, &level_str_len)) {
			continue;
		}
		for (uint32_t i = 0; level_str[i]; i++) level_str[i] = toupper(level_str[i]);

		if (0 != cf_fault_sink_addcontext(s, context, level_str)) {
			cf_info(AS_INFO, "log set command: addcontext failed: context %s level %s", context, level_str);
			cf_dyn_buf_append_string(db, "error-invalid-context-or-level");
			return(0);
		}
	}

	cf_info(AS_INFO, "log-set command executed: params %s", params);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}


// latency:hist=reads;back=180;duration=60;slice=10;
// throughput:hist=reads;back=180;duration=60;slice=10;
// hist-track-start:hist=reads;back=43200;slice=30;thresholds=1,4,16,64;
// hist-track-stop:hist=reads;
//
// hist     - optional histogram name - if none, command applies to all cf_hist_track objects
//
// for start command:
// back     - total time span in seconds over which to cache data
// slice    - period in seconds at which to cache histogram data
// thresholds - comma-separated bucket (ms) values to track, must be powers of 2. e.g:
//				1,4,16,64
// defaults are:
// - config value for back - mandatory, serves as flag for tracking
// - config value if it exists for slice, otherwise 10 seconds
// - config value if it exists for thresholds, otherwise internal defaults (1,8,64)
//
// for query commands:
// back     - start search this many seconds before now, default: minimum to get last slice
//			  using back=0 will get cached data from oldest cached data
// duration - seconds (forward) from start to search, default 0: everything to present
// slice    - intervals (in seconds) to analyze, default 0: everything as one slice
//
// e.g. query:
// latency:hist=reads;back=180;duration=60;slice=10;
// output (CF_HIST_TRACK_FMT_PACKED format) is:
// requested value  latency:hist=reads;back=180;duration=60;slice=10
// value is  reads:23:26:24-GMT,ops/sec,>1ms,>8ms,>64ms;23:26:34,30618.2,0.05,0.00,0.00;
// 23:26:44,31942.1,0.02,0.00,0.00;23:26:54,30966.9,0.01,0.00,0.00;23:27:04,30380.4,0.01,0.00,0.00;
// 23:27:14,37833.6,0.01,0.00,0.00;23:27:24,38502.7,0.01,0.00,0.00;23:27:34,39191.4,0.02,0.00,0.00;
//
// explanation:
// 23:26:24-GMT - timestamp of histogram starting first slice
// ops/sec,>1ms,>8ms,>64ms - labels for the columns: throughput, and which thresholds
// 23:26:34,30618.2,0.05,0.00,0.00; - timestamp of histogram ending slice, throughput, latencies

int
info_command_hist_track(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "hist track %s command received: params %s", name, params);

	char value_str[50];
	int  value_str_len = sizeof(value_str);
	cf_hist_track* hist_p = NULL;

	if (0 != as_info_parameter_get(params, "hist", value_str, &value_str_len)) {
		cf_debug(AS_INFO, "hist track %s command: no histogram specified - doing all", name);
	}
	else {
		if (*value_str == '{') {
			char* ns_name = value_str + 1;
			char* ns_name_end = strchr(ns_name, '}');
			as_namespace* ns = as_namespace_get_bybuf((uint8_t*)ns_name, ns_name_end - ns_name);

			if (! ns) {
				cf_info(AS_INFO, "hist track %s command: unrecognized histogram: %s", name, value_str);
				cf_dyn_buf_append_string(db, "error-bad-hist-name");
				return 0;
			}

			char* hist_name = ns_name_end + 1;

			if (*hist_name++ != '-') {
				cf_info(AS_INFO, "hist track %s command: unrecognized histogram: %s", name, value_str);
				cf_dyn_buf_append_string(db, "error-bad-hist-name");
				return 0;
			}

			if (0 == strcmp(hist_name, "read")) {
				hist_p = ns->read_hist;
			}
			else if (0 == strcmp(hist_name, "write")) {
				hist_p = ns->write_hist;
			}
			else if (0 == strcmp(hist_name, "udf")) {
				hist_p = ns->udf_hist;
			}
			else if (0 == strcmp(hist_name, "query")) {
				hist_p = ns->query_hist;
			}
			else {
				cf_info(AS_INFO, "hist track %s command: unrecognized histogram: %s", name, value_str);
				cf_dyn_buf_append_string(db, "error-bad-hist-name");
				return 0;
			}
		}
		else {
			cf_info(AS_INFO, "hist track %s command: unrecognized histogram: %s", name, value_str);
			cf_dyn_buf_append_string(db, "error-bad-hist-name");
			return 0;
		}
	}

	if (0 == strcmp(name, "hist-track-stop")) {
		if (hist_p) {
			cf_hist_track_stop(hist_p);
		}
		else {
			for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
				as_namespace* ns = g_config.namespaces[i];

				cf_hist_track_stop(ns->read_hist);
				cf_hist_track_stop(ns->write_hist);
				cf_hist_track_stop(ns->udf_hist);
				cf_hist_track_stop(ns->query_hist);
			}
		}

		cf_dyn_buf_append_string(db, "ok");

		return 0;
	}

	bool start_cmd = 0 == strcmp(name, "hist-track-start");

	// Note - default query params will get the most recent saved slice.
	uint32_t back_sec = start_cmd ? g_config.hist_track_back : (g_config.hist_track_slice * 2) - 1;
	uint32_t slice_sec = start_cmd ? g_config.hist_track_slice : 0;
	int i;

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "back", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			back_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: back is not a number, using default", name);
		}
	}

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "slice", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			slice_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: slice is not a number, using default", name);
		}
	}

	if (start_cmd) {
		char* thresholds = g_config.hist_track_thresholds;

		value_str_len = sizeof(value_str);

		if (0 == as_info_parameter_get(params, "thresholds", value_str, &value_str_len)) {
			thresholds = value_str;
		}

		cf_debug(AS_INFO, "hist track start command: back %u, slice %u, thresholds %s",
				back_sec, slice_sec, thresholds ? thresholds : "null");

		if (hist_p) {
			if (cf_hist_track_start(hist_p, back_sec, slice_sec, thresholds)) {
				cf_dyn_buf_append_string(db, "ok");
			}
			else {
				cf_dyn_buf_append_string(db, "error-bad-start-params");
			}
		}
		else {
			for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
				as_namespace* ns = g_config.namespaces[i];

				if ( ! (cf_hist_track_start(ns->read_hist, back_sec, slice_sec, thresholds) &&
						cf_hist_track_start(ns->write_hist, back_sec, slice_sec, thresholds) &&
						cf_hist_track_start(ns->udf_hist, back_sec, slice_sec, thresholds) &&
						cf_hist_track_start(ns->query_hist, back_sec, slice_sec, thresholds))) {

					cf_dyn_buf_append_string(db, "error-bad-start-params");
					return 0;
				}
			}

			cf_dyn_buf_append_string(db, "ok");
		}

		return 0;
	}

	// From here on it's latency or throughput...

	uint32_t duration_sec = 0;

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "duration", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			duration_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: duration is not a number, using default", name);
		}
	}

	bool throughput_only = 0 == strcmp(name, "throughput");

	cf_debug(AS_INFO, "hist track %s command: back %u, duration %u, slice %u",
			name, back_sec, duration_sec, slice_sec);

	if (hist_p) {
		cf_hist_track_get_info(hist_p, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
	}
	else {
		for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
			as_namespace* ns = g_config.namespaces[i];

			cf_hist_track_get_info(ns->read_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
			cf_hist_track_get_info(ns->write_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
			cf_hist_track_get_info(ns->udf_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
			cf_hist_track_get_info(ns->query_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		}
	}

	cf_dyn_buf_chomp(db);

	return 0;
}

// TODO - separate all these CP-related info commands.

// Format is:
//
//	revive:{namespace=<ns-name>}
//
int
info_command_revive(char *name, char *params, cf_dyn_buf *db)
{
	char ns_name[AS_ID_NAMESPACE_SZ] = { 0 };
	int ns_name_len = (int)sizeof(ns_name);
	int rv = as_info_parameter_get(params, "namespace", ns_name, &ns_name_len);

	if (rv == -2) {
		cf_warning(AS_INFO, "revive: namespace parameter value too long");
		cf_dyn_buf_append_string(db, "ERROR::bad-namespace");
		return 0;
	}

	if (rv == 0) {
		as_namespace *ns = as_namespace_get_byname(ns_name);

		if (! ns) {
			cf_warning(AS_INFO, "revive: unknown namespace %s", ns_name);
			cf_dyn_buf_append_string(db, "ERROR::unknown-namespace");
			return 0;
		}

		if (! as_partition_balance_revive(ns)) {
			cf_warning(AS_INFO, "revive: failed - recluster in progress");
			cf_dyn_buf_append_string(db, "ERROR::failed-revive");
			return 0;
		}

		cf_info(AS_INFO, "revive: complete - issue 'recluster:' command");
		cf_dyn_buf_append_string(db, "ok");
		return 0;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (! as_partition_balance_revive(ns)) {
			cf_warning(AS_INFO, "revive: failed - recluster in progress");
			cf_dyn_buf_append_string(db, "ERROR::failed-revive");
			return 0;
		}
	}

	cf_info(AS_INFO, "revive: complete - issue 'recluster:' command");
	cf_dyn_buf_append_string(db, "ok");
	return 0;
}

void
namespace_roster_info(as_namespace *ns, cf_dyn_buf *db)
{
	as_exchange_info_lock();

	cf_dyn_buf_append_string(db, "roster=");

	if (ns->roster_count == 0) {
		cf_dyn_buf_append_string(db, "null");
	}
	else {
		for (uint32_t n = 0; n < ns->roster_count; n++) {
			cf_dyn_buf_append_uint64_x(db, ns->roster[n]);

			if (ns->roster_rack_ids[n] != 0) {
				cf_dyn_buf_append_char(db, ROSTER_ID_PAIR_SEPARATOR);
				cf_dyn_buf_append_uint32(db, ns->roster_rack_ids[n]);
			}

			cf_dyn_buf_append_char(db, ',');
		}

		cf_dyn_buf_chomp(db);
	}

	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "pending_roster=");

	if (ns->smd_roster_count == 0) {
		cf_dyn_buf_append_string(db, "null");
	}
	else {
		for (uint32_t n = 0; n < ns->smd_roster_count; n++) {
			cf_dyn_buf_append_uint64_x(db, ns->smd_roster[n]);

			if (ns->smd_roster_rack_ids[n] != 0) {
				cf_dyn_buf_append_char(db, ROSTER_ID_PAIR_SEPARATOR);
				cf_dyn_buf_append_uint32(db, ns->smd_roster_rack_ids[n]);
			}

			cf_dyn_buf_append_char(db, ',');
		}

		cf_dyn_buf_chomp(db);
	}

	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "observed_nodes=");

	if (ns->observed_cluster_size == 0) {
		cf_dyn_buf_append_string(db, "null");
	}
	else {
		for (uint32_t n = 0; n < ns->observed_cluster_size; n++) {
			cf_dyn_buf_append_uint64_x(db, ns->observed_succession[n]);

			if (ns->rack_ids[n] != 0) {
				cf_dyn_buf_append_char(db, ROSTER_ID_PAIR_SEPARATOR);
				cf_dyn_buf_append_uint32(db, ns->rack_ids[n]);
			}

			cf_dyn_buf_append_char(db, ',');
		}

		cf_dyn_buf_chomp(db);
	}

	as_exchange_info_unlock();
}

// Format is:
//
//	roster:{namespace=<ns-name>}
//
int
info_command_roster(char *name, char *params, cf_dyn_buf *db)
{
	char ns_name[AS_ID_NAMESPACE_SZ] = { 0 };
	int ns_name_len = (int)sizeof(ns_name);
	int rv = as_info_parameter_get(params, "namespace", ns_name, &ns_name_len);

	if (rv == -2) {
		cf_warning(AS_INFO, "namespace parameter value too long");
		cf_dyn_buf_append_string(db, "ERROR::bad-namespace");
		return 0;
	}

	if (rv == 0) {
		as_namespace *ns = as_namespace_get_byname(ns_name);

		if (! ns) {
			cf_warning(AS_INFO, "unknown namespace %s", ns_name);
			cf_dyn_buf_append_string(db, "ERROR::unknown-namespace");
			return 0;
		}

		namespace_roster_info(ns, db);

		return 0;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, "ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		namespace_roster_info(ns, db);

		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_chomp(db);

	return 0;
}

// Format is:
//
//	roster-set:namespace=<ns-name>;nodes=<nodes-string>
//
// where <nodes-string> is comma-separated list of node-id:rack-id pairs, and
// the :rack-id may be absent, indicating a rack-id of 0.
//
int
info_command_roster_set(char *name, char *params, cf_dyn_buf *db)
{
	// Get the namespace name.

	char ns_name[AS_ID_NAMESPACE_SZ];
	int ns_name_len = (int)sizeof(ns_name);
	int ns_rv = as_info_parameter_get(params, "namespace", ns_name, &ns_name_len);

	if (ns_rv != 0 || ns_name_len == 0) {
		cf_warning(AS_INFO, "roster-set command: missing or invalid namespace name in command");
		cf_dyn_buf_append_string(db, "ERROR::namespace-name");
		return 0;
	}

	// Get the nodes list.

	char nodes[AS_CLUSTER_SZ * ROSTER_STRING_ELE_LEN];
	int nodes_len = (int)sizeof(nodes);
	int nodes_rv = as_info_parameter_get(params, "nodes", nodes, &nodes_len);

	if (nodes_rv == -2 || (nodes_rv == 0 && nodes_len == 0)) {
		cf_warning(AS_INFO, "roster-set command: invalid nodes in command");
		cf_dyn_buf_append_string(db, "ERROR::nodes");
		return 0;
	}

	// Issue the roster-set command.

	bool ok = as_roster_set_nodes_cmd(ns_name, nodes);

	cf_dyn_buf_append_string(db, ok ? "ok" : "ERROR::roster-set");

	return 0;
}

// Format is one of:
//
//	truncate:namespace=<ns-name>;set=<set-name>;lut=<UTC-nanosec-string>
//	truncate:namespace=<ns-name>;set=<set-name>
//
//	truncate:namespace=<ns-name>;lut=<UTC-nanosec-string>
//	truncate:namespace=<ns-name>
//
int
info_command_truncate(char *name, char *params, cf_dyn_buf *db)
{
	// Get the namespace name.

	char ns_name[AS_ID_NAMESPACE_SZ];
	int ns_name_len = (int)sizeof(ns_name);
	int ns_rv = as_info_parameter_get(params, "namespace", ns_name, &ns_name_len);

	if (ns_rv != 0 || ns_name_len == 0) {
		cf_warning(AS_INFO, "truncate command: missing or invalid namespace name in command");
		cf_dyn_buf_append_string(db, "ERROR::namespace-name");
		return 0;
	}

	// Get the set-name if there is one.

	char set_name[AS_SET_NAME_MAX_SIZE];
	int set_name_len = (int)sizeof(set_name);
	int set_rv = as_info_parameter_get(params, "set", set_name, &set_name_len);

	if (set_rv == -2 || (set_rv == 0 && set_name_len == 0)) {
		cf_warning(AS_INFO, "truncate command: invalid set name in command");
		cf_dyn_buf_append_string(db, "ERROR::set-name");
		return 0;
	}

	// Get the threshold last-update-time if there is one.

	char lut_str[24]; // allow decimal, hex or octal in C constant format
	int lut_str_len = (int)sizeof(lut_str);
	int lut_rv = as_info_parameter_get(params, "lut", lut_str, &lut_str_len);

	if (lut_rv == -2 || (lut_rv == 0 && lut_str_len == 0)) {
		cf_warning(AS_INFO, "truncate command: invalid last-update-time in command");
		cf_dyn_buf_append_string(db, "ERROR::last-update-time");
		return 0;
	}

	// Issue the truncate command.

	bool ok = as_truncate_cmd(ns_name,
			set_rv == 0 ? set_name : NULL,
			lut_rv == 0 ? lut_str : NULL);

	cf_dyn_buf_append_string(db, ok ? "ok" : "ERROR::truncate");

	return 0;
}

// Format is one of:
//
//	truncate-undo:namespace=<ns-name>;set=<set-name>
//
//	truncate-undo:namespace=<ns-name>
//
int
info_command_truncate_undo(char *name, char *params, cf_dyn_buf *db)
{
	// Get the namespace name.

	char ns_name[AS_ID_NAMESPACE_SZ];
	int ns_name_len = (int)sizeof(ns_name);
	int ns_rv = as_info_parameter_get(params, "namespace", ns_name, &ns_name_len);

	if (ns_rv != 0 || ns_name_len == 0) {
		cf_warning(AS_INFO, "truncate-undo command: missing or invalid namespace name in command");
		cf_dyn_buf_append_string(db, "ERROR::namespace-name");
		return 0;
	}

	// Get the set-name if there is one.

	char set_name[AS_SET_NAME_MAX_SIZE];
	int set_name_len = (int)sizeof(set_name);
	int set_rv = as_info_parameter_get(params, "set", set_name, &set_name_len);

	if (set_rv == -2 || (set_rv == 0 && set_name_len == 0)) {
		cf_warning(AS_INFO, "truncate-undo command: invalid set name in command");
		cf_dyn_buf_append_string(db, "ERROR::set-name");
		return 0;
	}

	// Issue the truncate-undo command.

	as_truncate_undo_cmd(ns_name, set_rv == 0 ? set_name : NULL);

	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

//
// Log a message to the server.
// Limited to 2048 characters.
//
// Format:
//	log-message:message=<MESSAGE>[;who=<WHO>]
//
// Example:
// 	log-message:message=Example Log Message;who=Aerospike User
//
int
info_command_log_message(char *name, char *params, cf_dyn_buf *db)
{
	char who[128];
	int who_len = sizeof(who);
	if (0 != as_info_parameter_get(params, "who", who, &who_len)) {
		strcpy(who, "unknown");
	}

	char message[2048];
	int message_len = sizeof(message);
	if (0 == as_info_parameter_get(params, "message", message, &message_len)) {
		cf_info(AS_INFO, "%s: %s", who, message);
	}

	return 0;
}

// Generic info system functions
// These functions act when an INFO message comes in over the PROTO pipe
// collects the static and dynamic portions, puts it in a 'dyn buf',
// and sends a reply
//

// Error strings for security check results.
static void
append_sec_err_str(cf_dyn_buf *db, uint32_t result, as_sec_perm cmd_perm) {
	switch (result) {
	case AS_SEC_ERR_NOT_AUTHENTICATED:
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":not authenticated");
		return;
	case AS_SEC_ERR_ROLE_VIOLATION:
		switch (cmd_perm) {
		case PERM_INDEX_MANAGE:
			INFO_COMMAND_SINDEX_FAILCODE(result, "role violation");
			return;
		case PERM_UDF_MANAGE:
			cf_dyn_buf_append_string(db, "error=role_violation");
			return;
		default:
			break;
		}
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":role violation");
		return;
	default:
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":unexpected security error");
		return;
	}
}

static pthread_mutex_t		g_info_lock = PTHREAD_MUTEX_INITIALIZER;
info_static		*static_head = 0;
info_dynamic	*dynamic_head = 0;
info_tree		*tree_head = 0;
info_command	*command_head = 0;
//
// Pull up all elements in both list into the buffers
// (efficient enough if you're looking for lots of things)
// But only gets 'default' values
//

int
info_all(const as_file_handle* fd_h, cf_dyn_buf *db)
{
	uint8_t auth_result = as_security_check(fd_h, PERM_NONE);

	if (auth_result != AS_PROTO_RESULT_OK) {
		as_security_log(fd_h, auth_result, PERM_NONE, "info-all request", NULL);
		append_sec_err_str(db, auth_result, PERM_NONE);
		cf_dyn_buf_append_char(db, EOL);
		return 0;
	}

	info_static *s = static_head;
	while (s) {
		if (s->def == true) {
			cf_dyn_buf_append_string( db, s->name);
			cf_dyn_buf_append_char( db, SEP );
			cf_dyn_buf_append_buf( db, (uint8_t *) s->value, s->value_sz);
			cf_dyn_buf_append_char( db, EOL );
		}
		s = s->next;
	}

	info_dynamic *d = dynamic_head;
	while (d) {
		if (d->def == true) {
			cf_dyn_buf_append_string( db, d->name);
			cf_dyn_buf_append_char(db, SEP );
			d->value_fn(d->name, db);
			cf_dyn_buf_append_char(db, EOL);
		}
		d = d->next;
	}

	return(0);
}

//
// Parse the input buffer. It contains a list of keys that should be spit back.
// Do the parse, call the necessary function collecting the information in question
// Filling the dynbuf

int
info_some(char *buf, char *buf_lim, const as_file_handle* fd_h, cf_dyn_buf *db)
{
	uint8_t auth_result = as_security_check(fd_h, PERM_NONE);

	if (auth_result != AS_PROTO_RESULT_OK) {
		// TODO - log null-terminated buf as detail?
		as_security_log(fd_h, auth_result, PERM_NONE, "info request", NULL);
		append_sec_err_str(db, auth_result, PERM_NONE);
		cf_dyn_buf_append_char(db, EOL);
		return 0;
	}

	// For each incoming name
	char	*c = buf;
	char	*tok = c;

	while (c < buf_lim) {

		if ( *c == EOL ) {
			*c = 0;
			char *name = tok;
			bool handled = false;

			// search the static queue first always
			info_static *s = static_head;
			while (s) {
				if (strcmp(s->name, name) == 0) {
					// return exact command string received from client
					cf_dyn_buf_append_string( db, name);
					cf_dyn_buf_append_char( db, SEP );
					cf_dyn_buf_append_buf( db, (uint8_t *) s->value, s->value_sz);
					cf_dyn_buf_append_char( db, EOL );
					handled = true;
					break;
				}
				s = s->next;
			}

			// didn't find in static, try dynamic
			if (!handled) {
				info_dynamic *d = dynamic_head;
				while (d) {
					if (strcmp(d->name, name) == 0) {
						// return exact command string received from client
						cf_dyn_buf_append_string( db, d->name);
						cf_dyn_buf_append_char(db, SEP );
						d->value_fn(d->name, db);
						cf_dyn_buf_append_char(db, EOL);
						handled = true;
						break;
					}
					d = d->next;
				}
			}

			// search the tree
			if (!handled) {

				// see if there's a '/',
				char *branch = strchr( name, TREE_SEP);
				if (branch) {
					*branch = 0;
					branch++;

					info_tree *t = tree_head;
					while (t) {
						if (strcmp(t->name, name) == 0) {
							// return exact command string received from client
							cf_dyn_buf_append_string( db, t->name);
							cf_dyn_buf_append_char( db, TREE_SEP);
							cf_dyn_buf_append_string( db, branch);
							cf_dyn_buf_append_char(db, SEP );
							t->tree_fn(t->name, branch, db);
							cf_dyn_buf_append_char(db, EOL);
							break;
						}
						t = t->next;
					}
				}
			}

			tok = c + 1;
		}
		// commands have parameters
		else if ( *c == ':' ) {
			*c = 0;
			char *name = tok;

			// parse parameters
			tok = c + 1;
			// make sure c doesn't go beyond buf_lim
			while (*c != EOL && c < buf_lim-1) c++;
			if (*c != EOL) {
				cf_warning(AS_INFO, "Info '%s' parameter not terminated with '\\n'.", name);
				break;
			}
			*c = 0;
			char *param = tok;

			// search the command list
			info_command *cmd = command_head;
			while (cmd) {
				if (strcmp(cmd->name, name) == 0) {
					// return exact command string received from client
					cf_dyn_buf_append_string( db, name);
					cf_dyn_buf_append_char( db, ':');
					cf_dyn_buf_append_string( db, param);
					cf_dyn_buf_append_char( db, SEP );

					uint8_t result = as_security_check(fd_h, cmd->required_perm);

					as_security_log(fd_h, result, cmd->required_perm, name, param);

					if (result == AS_PROTO_RESULT_OK) {
						cmd->command_fn(cmd->name, param, db);
					}
					else {
						append_sec_err_str(db, result, cmd->required_perm);
					}

					cf_dyn_buf_append_char( db, EOL );
					break;
				}
				cmd = cmd->next;
			}

			if (!cmd) {
				cf_info(AS_INFO, "received command %s, not registered", name);
			}

			tok = c + 1;
		}

		c++;

	}
	return(0);
}

int
as_info_buffer(uint8_t *req_buf, size_t req_buf_len, cf_dyn_buf *rsp)
{
	// Either we'e doing all, or doing some
	if (req_buf_len == 0) {
		info_all(NULL, rsp);
	}
	else {
		info_some((char *)req_buf, (char *)(req_buf + req_buf_len), NULL, rsp);
	}

	return(0);
}

//
// Worker threads!
// these actually do the work. There is a lot of network activity,
// writes and such, don't want to clog up the main queue
//

void *
thr_info_fn(void *unused)
{
	for ( ; ; ) {

		as_info_transaction it;

		if (0 != cf_queue_pop(g_info_work_q, &it, CF_QUEUE_FOREVER)) {
			cf_crash(AS_TSVC, "unable to pop from info work queue");
		}

		as_file_handle *fd_h = it.fd_h;
		as_proto *pr = it.proto;

		// Allocate an output buffer sufficiently large to avoid ever resizing
		cf_dyn_buf_define_size(db, 128 * 1024);
		// write space for the header
		uint64_t	h = 0;
		cf_dyn_buf_append_buf(&db, (uint8_t *) &h, sizeof(h));

		// Either we'e doing all, or doing some
		if (pr->sz == 0) {
			info_all(fd_h, &db);
		}
		else {
			info_some((char *)pr->data, (char *)pr->data + pr->sz, fd_h, &db);
		}

		// write the proto header in the space we pre-wrote
		db.buf[0] = 2;
		db.buf[1] = 1;
		uint64_t	sz = db.used_sz - 8;
		db.buf[4] = (sz >> 24) & 0xff;
		db.buf[5] = (sz >> 16) & 0xff;
		db.buf[6] = (sz >> 8) & 0xff;
		db.buf[7] = sz & 0xff;

		// write the data buffer
		if (cf_socket_send_all(&fd_h->sock, db.buf, db.used_sz,
				MSG_NOSIGNAL, CF_SOCKET_TIMEOUT) < 0) {
			cf_info(AS_INFO, "thr_info: can't write all bytes, fd %d error %d",
					CSFD(&fd_h->sock), errno);
			as_end_of_transaction_force_close(fd_h);
			fd_h = NULL;
		}

		cf_dyn_buf_free(&db);

		cf_free(pr);

		if (fd_h) {
			as_end_of_transaction_ok(fd_h);
			fd_h = NULL;
		}

		G_HIST_INSERT_DATA_POINT(info_hist, it.start_time);
		cf_atomic64_incr(&g_stats.info_complete);
	}

	return NULL;
}

//
// received an info request from a file descriptor
// Called by the thr_tsvc when an info message is seen
// calls functions info_all or info_some to collect the response
// calls write to send the response back
//
// Proto will be freed by the caller
//

void
as_info(as_info_transaction *it)
{
	cf_queue_push(g_info_work_q, it);
}

// Return the number of pending Info requests in the queue.
int
as_info_queue_get_size()
{
	return cf_queue_sz(g_info_work_q);
}

// Registers a dynamic name-value calculator.
// the get_value_fn will be called if a request comes in for this name.
// only does the registration!
// def means it's part of the default results - will get invoked for a blank info command (asinfo -v "")


int
as_info_set_dynamic(char *name, as_info_get_value_fn gv_fn, bool def)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_dynamic *e = dynamic_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->value_fn = gv_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_dynamic));
		e->def = def;
		e->name = cf_strdup(name);
		e->value_fn = gv_fn;
		e->next = dynamic_head;
		dynamic_head = e;
	}
	rv = 0;

	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}


// Registers a tree-based name-value calculator.
// the get_value_fn will be called if a request comes in for this name.
// only does the registration!


int
as_info_set_tree(char *name, as_info_get_tree_fn gv_fn)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_tree *e = tree_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->tree_fn = gv_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_tree));
		e->name = cf_strdup(name);
		e->tree_fn = gv_fn;
		e->next = tree_head;
		tree_head = e;
	}
	rv = 0;

	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}


// Registers a command handler
// the get_value_fn will be called if a request comes in for this name, and
// parameters will be passed in
// This function only does the registration!

int
as_info_set_command(char *name, as_info_command_fn command_fn, as_sec_perm required_perm)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_command *e = command_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->command_fn = command_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_command));
		e->name = cf_strdup(name);
		e->command_fn = command_fn;
		e->required_perm = required_perm;
		e->next = command_head;
		command_head = e;
	}
	rv = 0;

	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}



//
// Sets a static name-value pair
// def means it's part of the default set - will get returned if nothing is passed

int
as_info_set_buf(const char *name, const uint8_t *value, size_t value_sz, bool def)
{
	pthread_mutex_lock(&g_info_lock);

	// Delete case
	if (value_sz == 0 || value == 0) {

		info_static *p = 0;
		info_static *e = static_head;

		while (e) {
			if (strcmp(name, e->name) == 0) {
				if (p) {
					p->next = e->next;
					cf_free(e->name);
					cf_free(e->value);
					cf_free(e);
				}
				else {
					info_static *_t = static_head->next;
					cf_free(e->name);
					cf_free(e->value);
					cf_free(static_head);
					static_head = _t;
				}
				break;
			}
			p = e;
			e = e->next;
		}
	}
	// insert case
	else {

		info_static *e = static_head;

		// search for old value and overwrite
		while(e) {
			if (strcmp(name, e->name) == 0) {
				cf_free(e->value);
				e->value = cf_malloc(value_sz);
				memcpy(e->value, value, value_sz);
				e->value_sz = value_sz;
				break;
			}
			e = e->next;
		}

		// not found, insert fresh
		if (e == 0) {
			info_static *_t = cf_malloc(sizeof(info_static));
			_t->next = static_head;
			_t->def = def;
			_t->name = cf_strdup(name);
			_t->value = cf_malloc(value_sz);
			memcpy(_t->value, value, value_sz);
			_t->value_sz = value_sz;
			static_head = _t;
		}
	}

	pthread_mutex_unlock(&g_info_lock);
	return(0);

}

//
// A helper function. Commands have the form:
// cmd:param=value;param=value
//
// The main parser gives us the entire parameter string
// so use this function to scan through and get the particular parameter value
// you're looking for
//
// The 'param_string' is the param passed by the command parser into a command
//
// @return  0 : success
//         -1 : parameter not found
//         -2 : parameter found but value is too long
//

int
as_info_parameter_get(char *param_str, char *param, char *value, int *value_len)
{
	cf_detail(AS_INFO, "parameter get: paramstr %s seeking param %s", param_str, param);

	char *c = param_str;
	char *tok = param_str;
	int param_len = strlen(param);

	while (*c) {
		if (*c == '=') {
			if ( ( param_len == c - tok) && (0 == memcmp(tok, param, param_len) ) ) {
				c++;
				tok = c;
				while ( *c != 0 && *c != ';') c++;
				if (*value_len <= c - tok)	{
					// The found value is too long.
					return(-2);
				}
				*value_len = c - tok;
				memcpy(value, tok, *value_len);
				value[*value_len] = 0;
				return(0);
			}
			c++;
		}
		else if (*c == ';') {
			c++;
			tok = c;
		}
		else c++;

	}

	return(-1);
}

int
as_info_set(const char *name, const char *value, bool def)
{
	return(as_info_set_buf(name, (const uint8_t *) value, strlen(value), def ) );
}

//
//
// service interfaces management
//
// There's a worker thread - info_interfaces_fn ---
// which continually polls the interfaces to see if anything changed.
// When it changes, it updates a generation count.
// There's a hash table of all the other nodes in the cluster, and a counter
// to see that they're all up-to-date on the generation
//
//
// The fabric message in question can be expanded to do more than service interfaces.
// By expanding the 'info_node_info' structure, and the fabric_msg, you can carry
// more dynamic information than just the remote node's interfaces
// But that's all that we can think of at the moment - the paxos communication method
// makes sure that the distributed key system is properly distributed
//

static pthread_mutex_t g_serv_lock = PTHREAD_MUTEX_INITIALIZER;
static char *g_serv_legacy = NULL;
static char *g_serv_clear_std = NULL;
static char *g_serv_clear_alt = NULL;
static char *g_serv_tls_std = NULL;
static char *g_serv_tls_alt = NULL;
static char *g_serv_tls_name = NULL;
static uint32_t g_serv_gen = 0;
static cf_atomic64 g_peers_gen = 1;

//
// What other nodes are out there, and what are their ip addresses?
//

typedef struct info_node_info_s {
	char     *service_addr;       // string representing the service address
	char     *alternate_addr;     // string representing the alternate address
	uint32_t generation;          // acked generation counter
	char     *services_clear_std; // non-TLS standard services list
	char     *services_tls_std;   // TLS standard services list
	char     *services_clear_alt; // non-TLS alternate services list
	char     *services_tls_alt;   // TLS alternate services list
	char     *tls_name;           // TLS name
	uint64_t last_changed;        // generation count of last modification (for delta updates)
} info_node_info;

typedef const char *(*info_node_proj_fn)(info_node_info *info);

typedef struct services_printer_s {
	info_node_proj_fn proj;
	cf_dyn_buf        *db;
	const char        *strip;
	uint64_t          since;
	bool              with_tls_name;
	int32_t           count;
} services_printer;

typedef struct port_savings_context_s {
	info_node_proj_fn proj;
	uint64_t          since;
	uint32_t          port_savings[65536];
} port_savings_context;

// To avoid the services bug, g_info_node_info_hash should *always* be a subset
// of g_info_node_info_history_hash. In order to ensure this, every modification
// of g_info_node_info_hash should first involve grabbing the lock for the same
// key in g_info_node_info_history_hash.
cf_shash *g_info_node_info_history_hash = NULL;
cf_shash *g_info_node_info_hash = NULL;

int info_node_info_reduce_fn(const void *key, void *data, void *udata);

static char *
format_services_string(const char **addrs, uint32_t n_addrs, cf_ip_port port, char sep)
{
	if (n_addrs == 0) {
		return NULL;
	}

	cf_dyn_buf_define(db);

	for (uint32_t i = 0; i < n_addrs; ++i) {
		if (cf_ip_addr_is_dns_name(addrs[i])) {
			cf_dyn_buf_append_string(&db, addrs[i]);
			cf_dyn_buf_append_char(&db, ':');
			cf_dyn_buf_append_string(&db, cf_ip_port_print(port));
		}
		else {
			cf_sock_addr addr;
			CF_NEVER_FAILS(cf_sock_addr_from_host_port(addrs[i], port, &addr));
			cf_dyn_buf_append_string(&db, cf_sock_addr_print(&addr));
		}

		cf_dyn_buf_append_char(&db, sep);
	}

	if (n_addrs > 0) {
		cf_dyn_buf_chomp(&db);
	}

	char *res = cf_dyn_buf_strdup(&db);
	cf_dyn_buf_free(&db);
	return res;
}

static char *
format_services_addr(cf_ip_addr *addrs, int32_t n_addrs, cf_ip_port port, char sep)
{
	if (n_addrs == 0) {
		return NULL;
	}

	cf_dyn_buf_define(db);

	for (int32_t i = 0; i < n_addrs; ++i) {
		cf_sock_addr addr;
		cf_sock_addr_from_addr_port(&addrs[i], port, &addr);
		cf_dyn_buf_append_string(&db, cf_sock_addr_print(&addr));
		cf_dyn_buf_append_char(&db, sep);
	}

	if (n_addrs > 0) {
		cf_dyn_buf_chomp(&db);
	}

	char *res = cf_dyn_buf_strdup(&db);
	cf_dyn_buf_free(&db);
	return res;
}

static bool
detect_name_change(char **tls_name)
{
	char *node_name = cf_node_name();

	if (node_name[0] == 0) {
		cf_free(node_name);
		node_name = NULL;
	}

	if (*tls_name == NULL && node_name == NULL) {
		return false;
	}

	if (*tls_name != NULL && node_name != NULL && strcmp(*tls_name, node_name) == 0) {
		cf_free(node_name);
		return false;
	}

	if (*tls_name != NULL) {
		cf_free(*tls_name);
	}

	*tls_name = node_name;
	return true;
}

static uint32_t
filter_legacy(const char **from, uint32_t n_from, const char **to)
{
	uint32_t n_to = 0;

	for (uint32_t i = 0; i < n_from; ++i) {
		if (cf_ip_addr_str_is_legacy(from[i])) {
			to[n_to] = from[i];
			++n_to;
		}
	}

	return n_to;
}

static void
set_static_services(void)
{
	const char *filter[CF_SOCK_CFG_MAX];
	uint32_t n_filter;

	if (g_access.service.addrs.n_addrs > 0) {
		n_filter = filter_legacy(g_access.service.addrs.addrs, g_access.service.addrs.n_addrs,
				filter);
		g_serv_legacy = format_services_string(filter, n_filter, g_access.service.port, ';');

		if (cf_ip_addr_legacy_only()) {
			g_serv_clear_std = format_services_string(filter, n_filter, g_access.service.port, ',');
		}
		else {
			g_serv_clear_std = format_services_string(g_access.service.addrs.addrs,
					g_access.service.addrs.n_addrs, g_access.service.port, ',');
		}
	}

	if (g_access.alt_service.addrs.n_addrs > 0) {
		if (cf_ip_addr_legacy_only()) {
			n_filter = filter_legacy(g_access.alt_service.addrs.addrs,
					g_access.alt_service.addrs.n_addrs, filter);
			g_serv_clear_alt = format_services_string(filter, n_filter, g_access.alt_service.port,
					',');
		}
		else {
			g_serv_clear_alt = format_services_string(g_access.alt_service.addrs.addrs,
					g_access.alt_service.addrs.n_addrs, g_access.alt_service.port, ',');
		}
	}

	if (g_access.tls_service.addrs.n_addrs > 0 && g_access.tls_service.port != 0) {
		if (cf_ip_addr_legacy_only()) {
			n_filter = filter_legacy(g_access.tls_service.addrs.addrs,
					g_access.tls_service.addrs.n_addrs, filter);
			g_serv_tls_std = format_services_string(filter, n_filter, g_access.tls_service.port,
					',');
		}
		else {
			g_serv_tls_std = format_services_string(g_access.tls_service.addrs.addrs,
					g_access.tls_service.addrs.n_addrs, g_access.tls_service.port, ',');
		}
	}

	if (g_access.alt_tls_service.addrs.n_addrs > 0 && g_access.alt_tls_service.port != 0) {
		if (cf_ip_addr_legacy_only()) {
			n_filter = filter_legacy(g_access.alt_tls_service.addrs.addrs,
					g_access.alt_tls_service.addrs.n_addrs, filter);
			g_serv_tls_alt = format_services_string(filter, n_filter, g_access.alt_tls_service.port,
					',');
		}
		else {
			g_serv_tls_alt = format_services_string(g_access.alt_tls_service.addrs.addrs,
					g_access.alt_tls_service.addrs.n_addrs, g_access.alt_tls_service.port, ',');
		}
	}
}

void
info_node_info_tend()
{
	cf_shash_reduce(g_info_node_info_hash, info_node_info_reduce_fn, 0);
}

void *
info_interfaces_fn(void *unused)
{
	cf_ip_addr legacy[CF_SOCK_CFG_MAX];
	uint32_t n_legacy = 0;

	cf_ip_addr addrs[CF_SOCK_CFG_MAX];
	uint32_t n_addrs = 0;

	char *tls_name = NULL;
	bool flag = cf_ip_addr_legacy_only();

	while (true) {
		bool chg_flag = cf_ip_addr_legacy_only() != flag;
		bool chg_legacy = cf_inter_detect_changes_legacy(legacy, &n_legacy, CF_SOCK_CFG_MAX);
		bool chg_any;

		if (cf_ip_addr_legacy_only()) {
			chg_any = cf_inter_detect_changes_legacy(addrs, &n_addrs, CF_SOCK_CFG_MAX);
		}
		else {
			chg_any = cf_inter_detect_changes(addrs, &n_addrs, CF_SOCK_CFG_MAX);
		}

		if (n_legacy + n_addrs == 0) {
			cf_warning(AS_INFO, "No network interface addresses detected for client access");
		}

		bool chg_name = detect_name_change(&tls_name);

		if (chg_flag || chg_legacy || chg_any || chg_name) {
			pthread_mutex_lock(&g_serv_lock);

			if (chg_flag) {
				set_static_services();
				flag = cf_ip_addr_legacy_only();
			}

			if (chg_legacy && g_access.service.addrs.n_addrs == 0) {
				if (g_serv_legacy != NULL) {
					cf_free(g_serv_legacy);
				}

				g_serv_legacy = format_services_addr(legacy, n_legacy, g_access.service.port, ';');
			}

			if (chg_any && g_access.service.addrs.n_addrs == 0) {
				if (g_serv_clear_std != NULL) {
					cf_free(g_serv_clear_std);
				}

				g_serv_clear_std = format_services_addr(addrs, n_addrs, g_access.service.port, ',');
			}

			if (chg_any && g_access.tls_service.port != 0 &&
					g_access.tls_service.addrs.n_addrs == 0) {
				if (g_serv_tls_std != NULL) {
					cf_free(g_serv_tls_std);
				}

				g_serv_tls_std = format_services_addr(addrs, n_addrs, g_access.tls_service.port,
						',');
			}

			if (chg_name && g_config.tls_service.tls_our_name == NULL) {
				g_serv_tls_name = tls_name;
			}

			++g_serv_gen;
			pthread_mutex_unlock(&g_serv_lock);
		}

		info_node_info_tend();
		sleep(2);
	}

	return NULL;
}

// Free the service strings of an info node.

static void
free_node_info_service(char **string)
{
	if (*string) {
		cf_free(*string);
		*string = 0;
	}
}

static void
free_node_info_services(info_node_info *info)
{
	free_node_info_service(&info->service_addr);
	free_node_info_service(&info->alternate_addr);
	free_node_info_service(&info->services_clear_std);
	free_node_info_service(&info->services_tls_std);
	free_node_info_service(&info->services_clear_alt);
	free_node_info_service(&info->services_tls_alt);
	free_node_info_service(&info->tls_name);
}

// Resets the service strings of an info node without freeing them.

static void
reset_node_info_services(info_node_info *info)
{
	info->service_addr = 0;
	info->alternate_addr = 0;
	info->services_clear_std = 0;
	info->services_tls_std = 0;
	info->services_clear_alt = 0;
	info->services_tls_alt = 0;
	info->tls_name = 0;
}

// Clone the service strings of an info node.

static char *
clone_node_info_service(const char *string)
{
	return string ? cf_strdup(string) : 0;
}

static void
clone_node_info_services(info_node_info *from, info_node_info *to)
{
	to->service_addr = clone_node_info_service(from->service_addr);
	to->alternate_addr = clone_node_info_service(from->alternate_addr);
	to->services_clear_std = clone_node_info_service(from->services_clear_std);
	to->services_tls_std = clone_node_info_service(from->services_tls_std);
	to->services_clear_alt = clone_node_info_service(from->services_clear_alt);
	to->services_tls_alt = clone_node_info_service(from->services_tls_alt);
	to->tls_name = clone_node_info_service(from->tls_name);
}

// Compare the service strings of two info nodes.

static bool
compare_node_info_service(const char *lhs, const char *rhs)
{
	if (!lhs || !rhs) {
		return !lhs && !rhs;
	}

	return strcmp(lhs, rhs) == 0;
}

static bool
compare_node_info_services(info_node_info *lhs, info_node_info *rhs)
{
	return compare_node_info_service(lhs->service_addr, rhs->service_addr) &&
			compare_node_info_service(lhs->alternate_addr, rhs->alternate_addr) &&
			compare_node_info_service(lhs->services_clear_std, rhs->services_clear_std) &&
			compare_node_info_service(lhs->services_tls_std, rhs->services_tls_std) &&
			compare_node_info_service(lhs->services_clear_alt, rhs->services_clear_alt) &&
			compare_node_info_service(lhs->services_tls_alt, rhs->services_tls_alt) &&
			compare_node_info_service(lhs->tls_name, rhs->tls_name);
}

// Dump the service strings of an info node.

static void
dump_node_info_services(info_node_info *info)
{
	cf_debug(AS_INFO, "Service address:   %s", cf_str_safe_as_null(info->service_addr));
	cf_debug(AS_INFO, "Alternate address: %s", cf_str_safe_as_null(info->alternate_addr));
	cf_debug(AS_INFO, "Clear, standard:   %s", cf_str_safe_as_null(info->services_clear_std));
	cf_debug(AS_INFO, "TLS, standard:     %s", cf_str_safe_as_null(info->services_tls_std));
	cf_debug(AS_INFO, "Clear, alternate:  %s", cf_str_safe_as_null(info->services_clear_alt));
	cf_debug(AS_INFO, "TLS, alternate:    %s", cf_str_safe_as_null(info->services_tls_alt));
	cf_debug(AS_INFO, "TLS name:          %s", cf_str_safe_as_null(info->tls_name));
}

// This reduce function will eliminate elements from the info hash
// which are no longer in the succession list

typedef struct reduce_context_s {
	uint32_t cluster_size;
	cf_node *succession;
	uint32_t n_deleted;
	cf_node deleted[AS_CLUSTER_SZ];
} reduce_context;

int32_t
info_clustering_event_reduce_fn(const void *key, void *data, void *udata)
{
	const cf_node *node = key;
	info_node_info *info = data;
	reduce_context *context = udata;

	for (uint32_t i = 0; i < context->cluster_size; ++i) {
		if (*node == context->succession[i]) {
			return CF_SHASH_OK;
		}
	}

	cf_debug(AS_INFO, "Clustering event reduce: removing node %" PRIx64, *node);

	uint32_t n = context->n_deleted;
	context->deleted[n] = *node;
	++context->n_deleted;

	free_node_info_services(info);
	return CF_SHASH_REDUCE_DELETE;
}

//
// Maintain the info_node_info hash as a shadow of the succession list
//
static void
info_clustering_event_listener(const as_exchange_cluster_changed_event* event, void* udata)
{
	uint64_t start_ms = cf_getms();
	cf_debug(AS_INFO, "Info received new clustering state");

	info_node_info temp;
	temp.generation = 0;
	temp.last_changed = 0;
	reset_node_info_services(&temp);

	uint32_t i;

	for (i = 0; i < event->cluster_size; ++i) {
		cf_node member_nodeid = event->succession[i];

		if (member_nodeid == g_config.self_node) {
			continue;
		}

		info_node_info *info_history;
		pthread_mutex_t *vlock_history;

		if (cf_shash_get_vlock(g_info_node_info_history_hash, &member_nodeid, (void **)&info_history,
				&vlock_history) != CF_SHASH_OK) {
			// This may fail, but this is OK. This should only fail when info_msg_fn is also trying
			// to add this key, so either way the entry will be in the hash table.
			cf_shash_put_unique(g_info_node_info_history_hash, &member_nodeid, &temp);

			if (cf_shash_get_vlock(g_info_node_info_history_hash, &member_nodeid,
					(void **)&info_history, &vlock_history) != CF_SHASH_OK) {
				cf_crash(AS_INFO,
						"Could not create info history hash entry for %" PRIx64, member_nodeid);
				continue;
			}
		}

		info_node_info *info;
		pthread_mutex_t *vlock;

		if (cf_shash_get_vlock(g_info_node_info_hash, &member_nodeid, (void **)&info,
				&vlock) != CF_SHASH_OK) {
			clone_node_info_services(info_history, &temp);
			temp.last_changed = cf_atomic64_incr(&g_peers_gen);

			if (cf_shash_put_unique(g_info_node_info_hash, &member_nodeid, &temp) == CF_SHASH_OK) {
				reset_node_info_services(&temp);
				info_history->last_changed = 0; // See info_clustering_event_reduce_fn().
				cf_debug(AS_INFO, "Peers generation %" PRId64 ": added node %" PRIx64,
						temp.last_changed, member_nodeid);
			}
			else {
				free_node_info_services(&temp);
				cf_crash(AS_INFO,
						"Could not insert node %" PRIx64 " from clustering notification", member_nodeid);
			}

			temp.last_changed = 0;
		}
		else {
			pthread_mutex_unlock(vlock);
		}

		pthread_mutex_unlock(vlock_history);
	}

	uint32_t before = cf_shash_get_size(g_info_node_info_hash);
	cf_debug(AS_INFO, "Clustering succession list has %d element(s), info hash has %u", i, before);

	reduce_context cont = { .cluster_size = event->cluster_size, .succession = event->succession, .n_deleted = 0 };
	cf_shash_reduce(g_info_node_info_hash, info_clustering_event_reduce_fn, &cont);

	// While an alumni is gone, its last_changed field is non-zero. When it comes back, the
	// field goes back to zero.

	for (uint32_t i = 0; i < cont.n_deleted; ++i) {
		cf_debug(AS_INFO, "Updating alumni %" PRIx64, cont.deleted[i]);
		info_node_info *info_history;
		pthread_mutex_t *vlock_history;

		if (cf_shash_get_vlock(g_info_node_info_history_hash, &cont.deleted[i],
				(void **)&info_history, &vlock_history) != CF_SHASH_OK) {
			cf_crash(AS_INFO, "Removing a node (%" PRIx64 ") that is not an alumni",
					cont.deleted[i]);
		}

		info_history->last_changed = cf_atomic64_incr(&g_peers_gen);
		cf_debug(AS_INFO, "Peers generation %" PRId64 ": removed node %" PRIx64,
				info_history->last_changed, cont.deleted[i]);
		pthread_mutex_unlock(vlock_history);
	}

	uint32_t after = cf_shash_get_size(g_info_node_info_hash);
	cf_debug(AS_INFO, "After removal, info hash has %u element(s)", after);

	cf_atomic32_incr(&g_node_info_generation);
	cf_debug(AS_INFO, "info_clustering_event_listener took %" PRIu64 " ms", cf_getms() - start_ms);

	// Trigger an immediate tend to start peer list update across the cluster.
	info_node_info_tend();
}

// This goes in a reduce function for retransmitting my information to another node

int
info_node_info_reduce_fn(const void *key, void *data, void *udata)
{
	const cf_node *node = (const cf_node *)key;
	info_node_info *infop = (info_node_info *) data;

	if (infop->generation < g_serv_gen) {

		cf_debug(AS_INFO, "sending service string %s to node %"PRIx64, g_serv_legacy, *node);

		pthread_mutex_lock(&g_serv_lock);

		msg *m = as_fabric_msg_get(M_TYPE_INFO);

		// If we don't have the remote node's service address, request it via our update info. msg.
		msg_set_uint32(m, INFO_FIELD_OP, infop->service_addr && infop->services_clear_std ?
				INFO_OP_UPDATE : INFO_OP_UPDATE_REQ);
		msg_set_uint32(m, INFO_FIELD_GENERATION, g_serv_gen);

		if (g_serv_legacy) {
			msg_set_str(m, INFO_FIELD_SERVICE_ADDRESS, g_serv_legacy, MSG_SET_COPY);
		}

		// Legacy alternate address field.
		for (uint32_t i = 0; i < g_access.alt_service.addrs.n_addrs; ++i) {
			if (cf_ip_addr_str_is_legacy(g_access.alt_service.addrs.addrs[i])) {
				char tmp[250];
				snprintf(tmp, sizeof(tmp), "%s:%d", g_access.alt_service.addrs.addrs[i],
						g_access.service.port);
				msg_set_str(m, INFO_FIELD_ALT_ADDRESS, tmp, MSG_SET_COPY);
				break;
			}
		}

		if (g_serv_clear_std) {
			msg_set_str(m, INFO_FIELD_SERVICES_CLEAR_STD, g_serv_clear_std, MSG_SET_COPY);
		}

		if (g_serv_tls_std) {
			msg_set_str(m, INFO_FIELD_SERVICES_TLS_STD, g_serv_tls_std, MSG_SET_COPY);
		}

		if (g_serv_clear_alt) {
			msg_set_str(m, INFO_FIELD_SERVICES_CLEAR_ALT, g_serv_clear_alt, MSG_SET_COPY);
		}

		if (g_serv_tls_alt) {
			msg_set_str(m, INFO_FIELD_SERVICES_TLS_ALT, g_serv_tls_alt, MSG_SET_COPY);
		}

		if (g_serv_tls_name) {
			msg_set_str(m, INFO_FIELD_TLS_NAME, g_serv_tls_name, MSG_SET_COPY);
		}

		pthread_mutex_unlock(&g_serv_lock);

		if (as_fabric_send(*node, m, AS_FABRIC_CHANNEL_CTRL) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}
	}

	return(0);
}

static char *
convert_legacy_services(const char *legacy)
{
	if (legacy == NULL) {
		return NULL;
	}

	char *res = cf_strdup(legacy);

	for (size_t i = 0; res[i] != 0; ++i) {
		if (res[i] == ';') {
			res[i] = ',';
		}
	}

	return res;
}

//
// Receive a message from a remote node, jam it in my table
//

int
info_msg_fn(cf_node node, msg *m, void *udata)
{
	uint32_t op;

	if (msg_get_uint32(m, INFO_FIELD_OP, &op) != 0) {
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	case INFO_OP_UPDATE:
	case INFO_OP_UPDATE_REQ:
		{
			cf_debug(AS_INFO, "Received service address from node %" PRIx64 "; op = %u", node, op);
			info_node_info temp;
			temp.generation = 0;
			temp.last_changed = 0;
			reset_node_info_services(&temp);
			bool node_info_tend_required = false;

			info_node_info *info_history;
			pthread_mutex_t *vlock_history;

			if (cf_shash_get_vlock(g_info_node_info_history_hash, &node, (void **)&info_history,
					&vlock_history) != CF_SHASH_OK) {
				// This may fail, but this is ok. This should only fail when as_info_paxos_event
				// is concurrently trying to add this key, so either way the entry will be in the
				// hash table.
				cf_shash_put_unique(g_info_node_info_history_hash, &node, &temp);

				if (cf_shash_get_vlock(g_info_node_info_history_hash, &node, (void **)&info_history,
						&vlock_history) != CF_SHASH_OK) {
					cf_crash(AS_INFO,
							"Could not create info history hash entry for %" PRIx64, node);
					break;
				}
			}

			free_node_info_services(info_history);

			if (msg_get_str(m, INFO_FIELD_SERVICE_ADDRESS, &info_history->service_addr,
					0, MSG_GET_COPY_MALLOC) != 0 || !info_history->service_addr) {
				cf_debug(AS_INFO, "No service address in message from node %" PRIx64, node);
			}

			if (msg_get_str(m, INFO_FIELD_ALT_ADDRESS, &info_history->alternate_addr,
					0, MSG_GET_COPY_MALLOC) != 0) {
				cf_debug(AS_INFO, "No alternate address message from node %" PRIx64, node);
			}

			if (msg_get_str(m, INFO_FIELD_SERVICES_CLEAR_STD, &info_history->services_clear_std,
					0, MSG_GET_COPY_MALLOC) != 0 || !info_history->services_clear_std) {
				cf_debug(AS_INFO, "No services-clear-std in message from node %" PRIx64, node);
				info_history->services_clear_std =
						convert_legacy_services(info_history->service_addr);
			}

			if (msg_get_str(m, INFO_FIELD_SERVICES_TLS_STD, &info_history->services_tls_std,
					0, MSG_GET_COPY_MALLOC) != 0) {
				cf_debug(AS_INFO, "No services-tls-std in message from node %" PRIx64, node);
			}

			if (msg_get_str(m, INFO_FIELD_SERVICES_CLEAR_ALT, &info_history->services_clear_alt,
					0, MSG_GET_COPY_MALLOC) != 0) {
				cf_debug(AS_INFO, "No services-clear-alt in message from node %" PRIx64, node);
				info_history->services_clear_alt =
						convert_legacy_services(info_history->alternate_addr);
			}

			if (msg_get_str(m, INFO_FIELD_SERVICES_TLS_ALT, &info_history->services_tls_alt,
					0, MSG_GET_COPY_MALLOC) != 0) {
				cf_debug(AS_INFO, "No services-tls-alt in message from node %" PRIx64, node);
			}

			if (msg_get_str(m, INFO_FIELD_TLS_NAME, &info_history->tls_name,
					0, MSG_GET_COPY_MALLOC) != 0) {
				cf_debug(AS_INFO, "No tls-name in message from node %" PRIx64, node);
			}

			dump_node_info_services(info_history);

			info_node_info *info;
			pthread_mutex_t *vlock;
			info_node_info info_to_tend = { 0 };

			if (cf_shash_get_vlock(g_info_node_info_hash, &node, (void **)&info, &vlock) == CF_SHASH_OK) {
				if (!compare_node_info_services(info_history, info)) {
					cf_debug(AS_INFO, "Changed node info entry, was:");
					dump_node_info_services(info);
					info->last_changed = cf_atomic64_incr(&g_peers_gen);
					cf_debug(AS_INFO, "Peers generation %" PRId64 ": updated node %" PRIx64,
							info->last_changed, node);
				}

				free_node_info_services(info);
				clone_node_info_services(info_history, info);
				if (INFO_OP_UPDATE_REQ == op) {
					cf_debug(AS_INFO, "Received request for info update from node %" PRIx64 " ~~ setting node's info generation to 0!", node);
					info->generation = 0;
					node_info_tend_required = true;
					memcpy(&info_to_tend, info, sizeof(info_to_tend));
				}

				pthread_mutex_unlock(vlock);
			}
			else {
				// Before history hash was added to the code base, we would throw away the message
				// in this case.
				cf_debug(AS_INFO, "Node %" PRIx64 " not in info hash, saving service address in info history hash", node);
			}

			pthread_mutex_unlock(vlock_history);

			// Send the ACK.
			msg_preserve_fields(m, 1, INFO_FIELD_GENERATION);
			msg_set_uint32(m, INFO_FIELD_OP, INFO_OP_ACK);

			int rv = as_fabric_send(node, m, AS_FABRIC_CHANNEL_CTRL);

			if (rv != AS_FABRIC_SUCCESS) {
				cf_warning(AS_INFO, "Failed to send message %p with type %d to node %"PRIu64" (rv %d)",
						m, (int32_t)m->type, node, rv);
				as_fabric_msg_put(m);
			}

			if (node_info_tend_required) {
				// Send our service update to the source.
				info_node_info_reduce_fn(&node, &info_to_tend, NULL);
			}
		}

		break;

	case INFO_OP_ACK:
		{

			cf_debug(AS_INFO, " received ACK from node %"PRIx64, node);

			// TODO - dangerous to continue if no generation ???
			uint32_t gen = 0;
			msg_get_uint32(m, INFO_FIELD_GENERATION, &gen);
			info_node_info	*info;
			pthread_mutex_t	*vlock;
			if (0 == cf_shash_get_vlock(g_info_node_info_hash, &node, (void **) &info, &vlock)) {

				info->generation = gen;

				pthread_mutex_unlock(vlock);
			}

			as_fabric_msg_put(m);

		}
		break;

	default:
		as_fabric_msg_put(m);
		break;
	}

	return(0);
}

//
// This dynamic function reduces the info_node_info hash and builds up the string of services
//

int32_t
info_get_x_legacy_reduce_fn(const void *key, void *data, void *udata)
{
	services_printer *sp = udata;
	info_node_info *info = data;

	info_node_proj_fn proj = sp->proj;
	cf_dyn_buf *db = sp->db;
	const char *services = proj(info);

	if (services == NULL) {
		return 0;
	}

	if (sp->count > 0) {
		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_append_string(db, services);
	++sp->count;
	return 0;
}

int32_t
info_get_x_legacy_reduce(cf_shash *h, info_node_proj_fn proj, cf_dyn_buf *db)
{
	services_printer sp = { .proj = proj, .db = db };
	cf_shash_reduce(h, info_get_x_legacy_reduce_fn, (void *)&sp);
	return 0;
}

static const char *
project_services(info_node_info *info)
{
	return info->service_addr;
}

int32_t
info_get_services(char *name, cf_dyn_buf *db)
{
	return info_get_x_legacy_reduce(g_info_node_info_hash, project_services, db);
}

int32_t
info_get_services_alumni(char *name, cf_dyn_buf *db)
{
	return info_get_x_legacy_reduce(g_info_node_info_history_hash, project_services, db);
}

static const char *
project_alt_addr(info_node_info *info)
{
	return info->alternate_addr;
}

int32_t
info_get_alt_addr(char *name, cf_dyn_buf *db)
{
	return info_get_x_legacy_reduce(g_info_node_info_hash, project_alt_addr, db);
}

int32_t
info_port_savings_reduce_fn(const void *key, void *data, void *udata)
{
	port_savings_context *psc = udata;
	info_node_info *info = data;

	if (info->last_changed <= psc->since) {
		return 0;
	}

	const char *services = psc->proj(info);

	if (services == NULL) {
		return 0;
	}

	int32_t curr;

	for (int32_t end = strlen(services); end > 0; end = curr) {
		int32_t mult = 1;
		int32_t port = 0;

		for (curr = end - 1; curr >= 0; --curr) {
			char ch = services[curr];

			if (ch == ':') {
				break;
			}

			if (ch < '0' || ch > '9') {
				cf_warning(AS_INFO, "Invalid port number in services string: %s", services);
				return 0;
			}

			port += (ch - '0') * mult;
			mult *= 10;
		}

		int32_t savings = end - curr;
		cf_debug(AS_INFO, "Default port %d saves %d byte(s)", port, savings);
		psc->port_savings[port] += savings;

		while (curr >= 0 && services[curr] != ',') {
			--curr;
		}
	}

	return 0;
}

static char *
strip_service_suffixes(const char *services, const char *strip)
{
	const int32_t services_len = strlen(services);
	const int32_t strip_len = strlen(strip);

	char *clone = cf_strdup(services);

	int32_t left = services_len;
	int32_t right = services_len;

	while (left >= strip_len) {
		if (memcmp(clone + left - strip_len, strip, strip_len) == 0) {
			left -= strip_len;
		}

		while (left > 0) {
			clone[--right] = clone[--left];

			if (clone[left] == ',') {
				break;
			}
		}
	}

	memmove(clone, clone + right, services_len - right + 1);
	return clone;
}

int32_t
info_get_services_x_reduce_fn(const void *key, void *data, void *udata)
{
	services_printer *sp = udata;
	const cf_node *node = key;
	info_node_info *info = data;

	if (info->last_changed <= sp->since) {
		return 0;
	}

	const char *services = sp->proj(info);

	if (services == NULL) {
		return 0;
	}

	cf_dyn_buf *db = sp->db;

	if (sp->count > 0) {
		cf_dyn_buf_append_char(db, ',');
	}

	char node_id[17];
	cf_str_itoa_u64(*node, node_id, 16);

	cf_dyn_buf_append_char(db, '[');
	cf_dyn_buf_append_string(db, node_id);
	cf_dyn_buf_append_char(db, ',');

	if (sp->with_tls_name && info->tls_name) {
		cf_dyn_buf_append_string(db, info->tls_name);
	}

	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_char(db, '[');

	if (sp->strip != NULL) {
		char *stripped = strip_service_suffixes(services, sp->strip);
		cf_dyn_buf_append_string(db, stripped);
		cf_free(stripped);
	}
	else {
		cf_dyn_buf_append_string(db, services);
	}

	cf_dyn_buf_append_char(db, ']');
	cf_dyn_buf_append_char(db, ']');

	++sp->count;
	return 0;
}

int32_t
info_get_services_x(cf_shash *h, info_node_proj_fn proj, cf_dyn_buf *db, uint64_t since,
		bool with_tls_name)
{
	// Pick the default port that saves us the most space.
	port_savings_context psc = { .proj = proj, .since = since };
	cf_shash_reduce(h, info_port_savings_reduce_fn, &psc);

	int32_t best_savings = 0;
	int32_t best_port = 0;

	for (int32_t i = 0; i < 65536; ++i) {
		if (psc.port_savings[i] > best_savings) {
			best_savings = psc.port_savings[i];
			best_port = i;
		}
	}

	cf_debug(AS_INFO, "Best default port is %d, saves %d byte(s)", best_port, best_savings);

	cf_dyn_buf_append_uint64(db, cf_atomic64_get(g_peers_gen));
	cf_dyn_buf_append_char(db, ',');

	if (best_port > 0) {
		cf_dyn_buf_append_int(db, best_port);
	}

	cf_dyn_buf_append_char(db, ',');

	cf_dyn_buf_append_char(db, '[');

	char strip[20];
	snprintf(strip, sizeof(strip), ":%d", best_port);

	services_printer sp = { .proj = proj, .db = db, .strip = strip, .since = since,
			.with_tls_name = with_tls_name };
	cf_shash_reduce(h, info_get_services_x_reduce_fn, (void *)&sp);

	cf_dyn_buf_append_char(db, ']');
	return sp.count;
}

int32_t
info_get_services_x_gone_reduce_fn(const void *key, void *data, void *udata)
{
	services_printer *sp = udata;
	const cf_node *node = key;
	info_node_info *info = data;

	if (info->last_changed <= sp->since || sp->proj(info) == NULL) {
		return 0;
	}

	cf_dyn_buf *db = sp->db;

	if (sp->count > 0) {
		cf_dyn_buf_append_char(db, ',');
	}

	char node_id[17];
	cf_str_itoa_u64(*node, node_id, 16);

	cf_dyn_buf_append_char(db, '[');
	cf_dyn_buf_append_string(db, node_id);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_char(db, ']');

	++sp->count;
	return 0;
}

void
info_get_services_x_delta(info_node_proj_fn proj, cf_dyn_buf *db, char *params, bool with_tls_name)
{
	uint64_t since;

	if (cf_str_atoi_64(params, (int64_t *)&since) < 0) {
		cf_warning(AS_INFO, "Invalid peers generation %s", params);
		cf_dyn_buf_append_string(db, "ERROR");
		return;
	}

	uint64_t orig_gen = cf_atomic64_get(g_peers_gen);

	while (true) {
		int32_t count = info_get_services_x(g_info_node_info_hash, proj, db, since, with_tls_name);
		cf_dyn_buf_chomp(db); // Remove the "]".

		services_printer sp = { .proj = proj, .db = db, .since = since, .count = count };
		cf_shash_reduce(g_info_node_info_history_hash, info_get_services_x_gone_reduce_fn, &sp);

		cf_dyn_buf_append_char(db, ']'); // Re-add the "]".

		// Doing the above two reductions doesn't happen atomically. Theoretically, peers can
		// arrive or leave between the two invocations, leading to duplicate or missing peers in
		// the list. In this case, simply try again.

		uint64_t gen = cf_atomic64_get(g_peers_gen);

		if (gen == orig_gen) {
			break;
		}

		db->used_sz = 0;
		orig_gen = gen;
	}
}

static const char *
project_services_clear_std(info_node_info *info)
{
	return info->services_clear_std;
}

int32_t
info_get_services_clear_std(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_hash, project_services_clear_std, db, 0, false);
	return 0;
}

int32_t
info_get_services_clear_std_delta(char *name, char *params, cf_dyn_buf *db)
{
	info_get_services_x_delta(project_services_clear_std, db, params, false);
	return 0;
}

int32_t
info_get_alumni_clear_std(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_history_hash, project_services_clear_std, db, 0, false);
	return 0;
}

static const char *
project_services_tls_std(info_node_info *info)
{
	return info->services_tls_std;
}

int32_t
info_get_services_tls_std(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_hash, project_services_tls_std, db, 0, true);
	return 0;
}

int32_t
info_get_services_tls_std_delta(char *name, char *params, cf_dyn_buf *db)
{
	info_get_services_x_delta(project_services_tls_std, db, params, true);
	return 0;
}

int32_t
info_get_alumni_tls_std(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_history_hash, project_services_tls_std, db, 0, true);
	return 0;
}

static const char *
project_services_clear_alt(info_node_info *info)
{
	return info->services_clear_alt;
}

int32_t
info_get_services_clear_alt(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_hash, project_services_clear_alt, db, 0, false);
	return 0;
}

int32_t
info_get_services_clear_alt_delta(char *name, char *params, cf_dyn_buf *db)
{
	info_get_services_x_delta(project_services_clear_alt, db, params, false);
	return 0;
}

static const char *
project_services_tls_alt(info_node_info *info)
{
	return info->services_tls_alt;
}

int32_t
info_get_services_tls_alt(char *name, cf_dyn_buf *db)
{
	info_get_services_x(g_info_node_info_hash, project_services_tls_alt, db, 0, true);
	return 0;
}

int32_t
info_get_services_tls_alt_delta(char *name, char *params, cf_dyn_buf *db)
{
	info_get_services_x_delta(project_services_tls_alt, db, params, true);
	return 0;
}

int32_t
info_get_services_generation(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(g_peers_gen));
	return 0;
}

//
// This dynamic function removes nodes from g_info_node_info_history_hash that
// aren't present in g_info_node_info_hash.
//
int
history_purge_reduce_fn(const void *key, void *data, void *udata)
{
	return CF_SHASH_OK == cf_shash_get(g_info_node_info_hash, key, NULL) ? CF_SHASH_OK : CF_SHASH_REDUCE_DELETE;
}

int
info_services_alumni_reset(char *name, cf_dyn_buf *db)
{
	cf_shash_reduce(g_info_node_info_history_hash, history_purge_reduce_fn, NULL);
	cf_info(AS_INFO, "services alumni list reset");
	cf_dyn_buf_append_string(db, "ok");

	return(0);
}



//
// Iterate through the current namespace list and cons up a string
//

int
info_get_namespaces(char *name, cf_dyn_buf *db)
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		cf_dyn_buf_append_string(db, g_config.namespaces[i]->name);
		cf_dyn_buf_append_char(db, ';');
	}

	if (g_config.n_namespaces > 0) {
		cf_dyn_buf_chomp(db);
	}

	return(0);
}

int
info_get_logs(char *name, cf_dyn_buf *db)
{
	cf_fault_sink_strlist(db);
	return(0);
}

int
info_get_objects(char *name, cf_dyn_buf *db)
{
	uint64_t	objects = 0;

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		objects += g_config.namespaces[i]->n_objects;
	}

	cf_dyn_buf_append_uint64(db, objects);
	return(0);
}

int
info_get_sets(char *name, cf_dyn_buf *db)
{
	return info_get_tree_sets(name, "", db);
}

int
info_get_bins(char *name, cf_dyn_buf *db)
{
	return info_get_tree_bins(name, "", db);
}

int
info_get_config( char* name, cf_dyn_buf *db)
{
	return info_command_config_get(name, NULL, db);
}

int
info_get_sindexes(char *name, cf_dyn_buf *db)
{
	return info_get_tree_sindexes(name, "", db);
}


void
info_get_namespace_info(as_namespace *ns, cf_dyn_buf *db)
{
	// Cluster size.

	// Using ns_ prefix to avoid confusion with global cluster_size.
	info_append_uint32(db, "ns_cluster_size", ns->cluster_size);

	// Using effective_ prefix to avoid confusion with configured value.
	info_append_uint32(db, "effective_replication_factor", ns->replication_factor);

	// Object counts.

	info_append_uint64(db, "objects", ns->n_objects);
	info_append_uint64(db, "tombstones", ns->n_tombstones);

	repl_stats mp;
	as_partition_get_replica_stats(ns, &mp);

	info_append_uint64(db, "master_objects", mp.n_master_objects);
	info_append_uint64(db, "master_tombstones", mp.n_master_tombstones);
	info_append_uint64(db, "prole_objects", mp.n_prole_objects);
	info_append_uint64(db, "prole_tombstones", mp.n_prole_tombstones);
	info_append_uint64(db, "non_replica_objects", mp.n_non_replica_objects);
	info_append_uint64(db, "non_replica_tombstones", mp.n_non_replica_tombstones);

	// Consistency info.

	info_append_uint32(db, "dead_partitions", ns->n_dead_partitions);
	info_append_uint32(db, "unavailable_partitions", ns->n_unavailable_partitions);
	info_append_bool(db, "clock_skew_stop_writes", ns->clock_skew_stop_writes);

	// Expiration & eviction (nsup) stats.

	info_append_bool(db, "stop_writes", ns->stop_writes != 0);
	info_append_bool(db, "hwm_breached", ns->hwm_breached != 0);

	info_append_uint64(db, "current_time", as_record_void_time_get());
	info_append_uint64(db, "non_expirable_objects", ns->non_expirable_objects);
	info_append_uint64(db, "expired_objects", ns->n_expired_objects);
	info_append_uint64(db, "evicted_objects", ns->n_evicted_objects);
	info_append_uint64(db, "evict_ttl", ns->evict_ttl);
	info_append_uint32(db, "nsup_cycle_duration", ns->nsup_cycle_duration);
	info_append_uint32(db, "nsup_cycle_sleep_pct", ns->nsup_cycle_sleep_pct);

	// Truncate stats.

	info_append_uint64(db, "truncate_lut", ns->truncate.lut);
	info_append_uint64(db, "truncated_records", ns->truncate.n_records);

	// Memory usage stats.

	uint64_t data_memory = ns->n_bytes_memory;
	uint64_t index_memory = as_index_size_get(ns) * (ns->n_objects + ns->n_tombstones);
	uint64_t sindex_memory = ns->n_bytes_sindex_memory;
	uint64_t used_memory = data_memory + index_memory + sindex_memory;

	info_append_uint64(db, "memory_used_bytes", used_memory);
	info_append_uint64(db, "memory_used_data_bytes", data_memory);
	info_append_uint64(db, "memory_used_index_bytes", index_memory);
	info_append_uint64(db, "memory_used_sindex_bytes", sindex_memory);

	uint64_t free_pct = (ns->memory_size != 0 && (ns->memory_size > used_memory)) ?
			((ns->memory_size - used_memory) * 100L) / ns->memory_size : 0;

	info_append_uint64(db, "memory_free_pct", free_pct);

	// Persistent memory block keys' namespace ID (enterprise only).
	info_append_uint32(db, "xmem_id", ns->xmem_id);

	// Remaining bin-name slots (yes, this can be negative).
	if (! ns->single_bin) {
		info_append_int(db, "available_bin_names", BIN_NAMES_QUOTA - (int)cf_vmapx_count(ns->p_bin_name_vmap));
	}

	// Persistent storage stats.

	if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
		int available_pct = 0;
		uint64_t inuse_disk_bytes = 0;
		as_storage_stats(ns, &available_pct, &inuse_disk_bytes);

		info_append_uint64(db, "device_total_bytes", ns->ssd_size);
		info_append_uint64(db, "device_used_bytes", inuse_disk_bytes);

		free_pct = (ns->ssd_size != 0 && (ns->ssd_size > inuse_disk_bytes)) ?
				((ns->ssd_size - inuse_disk_bytes) * 100L) / ns->ssd_size : 0;

		info_append_uint64(db, "device_free_pct", free_pct);
		info_append_int(db, "device_available_pct", available_pct);

		if (! ns->storage_data_in_memory) {
			info_append_int(db, "cache_read_pct", (int)(ns->cache_read_pct + 0.5));
		}
	}

	// Migration stats.

	info_append_uint64(db, "migrate_tx_partitions_imbalance", ns->migrate_tx_partitions_imbalance);

	info_append_uint64(db, "migrate_tx_instances", ns->migrate_tx_instance_count);
	info_append_uint64(db, "migrate_rx_instances", ns->migrate_rx_instance_count);

	info_append_uint64(db, "migrate_tx_partitions_active", ns->migrate_tx_partitions_active);
	info_append_uint64(db, "migrate_rx_partitions_active", ns->migrate_rx_partitions_active);

	info_append_uint64(db, "migrate_tx_partitions_initial", ns->migrate_tx_partitions_initial);
	info_append_uint64(db, "migrate_tx_partitions_remaining", ns->migrate_tx_partitions_remaining);

	info_append_uint64(db, "migrate_rx_partitions_initial", ns->migrate_rx_partitions_initial);
	info_append_uint64(db, "migrate_rx_partitions_remaining", ns->migrate_rx_partitions_remaining);

	info_append_uint64(db, "migrate_records_skipped", ns->migrate_records_skipped);
	info_append_uint64(db, "migrate_records_transmitted", ns->migrate_records_transmitted);
	info_append_uint64(db, "migrate_record_retransmits", ns->migrate_record_retransmits);
	info_append_uint64(db, "migrate_record_receives", ns->migrate_record_receives);

	info_append_uint64(db, "migrate_signals_active", ns->migrate_signals_active);
	info_append_uint64(db, "migrate_signals_remaining", ns->migrate_signals_remaining);

	info_append_uint64(db, "appeals_tx_active", ns->appeals_tx_active);
	info_append_uint64(db, "appeals_rx_active", ns->appeals_rx_active);

	info_append_uint64(db, "appeals_tx_remaining", ns->appeals_tx_remaining);

	info_append_uint64(db, "appeals_records_exonerated", ns->appeals_records_exonerated);

	// From-client transaction stats.

	info_append_uint64(db, "client_tsvc_error", ns->n_client_tsvc_error);
	info_append_uint64(db, "client_tsvc_timeout", ns->n_client_tsvc_timeout);

	info_append_uint64(db, "client_proxy_complete", ns->n_client_proxy_complete);
	info_append_uint64(db, "client_proxy_error", ns->n_client_proxy_error);
	info_append_uint64(db, "client_proxy_timeout", ns->n_client_proxy_timeout);

	info_append_uint64(db, "client_read_success", ns->n_client_read_success);
	info_append_uint64(db, "client_read_error", ns->n_client_read_error);
	info_append_uint64(db, "client_read_timeout", ns->n_client_read_timeout);
	info_append_uint64(db, "client_read_not_found", ns->n_client_read_not_found);

	info_append_uint64(db, "client_write_success", ns->n_client_write_success);
	info_append_uint64(db, "client_write_error", ns->n_client_write_error);
	info_append_uint64(db, "client_write_timeout", ns->n_client_write_timeout);

	// Subset of n_client_write_... above, respectively.
	info_append_uint64(db, "xdr_write_success", ns->n_xdr_write_success);
	info_append_uint64(db, "xdr_write_error", ns->n_xdr_write_error);
	info_append_uint64(db, "xdr_write_timeout", ns->n_xdr_write_timeout);

	info_append_uint64(db, "client_delete_success", ns->n_client_delete_success);
	info_append_uint64(db, "client_delete_error", ns->n_client_delete_error);
	info_append_uint64(db, "client_delete_timeout", ns->n_client_delete_timeout);
	info_append_uint64(db, "client_delete_not_found", ns->n_client_delete_not_found);

	// Subset of n_client_delete_... above, respectively.
	info_append_uint64(db, "xdr_delete_success", ns->n_xdr_delete_success);
	info_append_uint64(db, "xdr_delete_error", ns->n_xdr_delete_error);
	info_append_uint64(db, "xdr_delete_timeout", ns->n_xdr_delete_timeout);
	info_append_uint64(db, "xdr_delete_not_found", ns->n_xdr_delete_not_found);

	info_append_uint64(db, "client_udf_complete", ns->n_client_udf_complete);
	info_append_uint64(db, "client_udf_error", ns->n_client_udf_error);
	info_append_uint64(db, "client_udf_timeout", ns->n_client_udf_timeout);

	info_append_uint64(db, "client_lang_read_success", ns->n_client_lang_read_success);
	info_append_uint64(db, "client_lang_write_success", ns->n_client_lang_write_success);
	info_append_uint64(db, "client_lang_delete_success", ns->n_client_lang_delete_success);
	info_append_uint64(db, "client_lang_error", ns->n_client_lang_error);

	// Batch sub-transaction stats.

	info_append_uint64(db, "batch_sub_tsvc_error", ns->n_batch_sub_tsvc_error);
	info_append_uint64(db, "batch_sub_tsvc_timeout", ns->n_batch_sub_tsvc_timeout);

	info_append_uint64(db, "batch_sub_proxy_complete", ns->n_batch_sub_proxy_complete);
	info_append_uint64(db, "batch_sub_proxy_error", ns->n_batch_sub_proxy_error);
	info_append_uint64(db, "batch_sub_proxy_timeout", ns->n_batch_sub_proxy_timeout);

	info_append_uint64(db, "batch_sub_read_success", ns->n_batch_sub_read_success);
	info_append_uint64(db, "batch_sub_read_error", ns->n_batch_sub_read_error);
	info_append_uint64(db, "batch_sub_read_timeout", ns->n_batch_sub_read_timeout);
	info_append_uint64(db, "batch_sub_read_not_found", ns->n_batch_sub_read_not_found);

	// Internal-UDF sub-transaction stats.

	info_append_uint64(db, "udf_sub_tsvc_error", ns->n_udf_sub_tsvc_error);
	info_append_uint64(db, "udf_sub_tsvc_timeout", ns->n_udf_sub_tsvc_timeout);

	info_append_uint64(db, "udf_sub_udf_complete", ns->n_udf_sub_udf_complete);
	info_append_uint64(db, "udf_sub_udf_error", ns->n_udf_sub_udf_error);
	info_append_uint64(db, "udf_sub_udf_timeout", ns->n_udf_sub_udf_timeout);

	info_append_uint64(db, "udf_sub_lang_read_success", ns->n_udf_sub_lang_read_success);
	info_append_uint64(db, "udf_sub_lang_write_success", ns->n_udf_sub_lang_write_success);
	info_append_uint64(db, "udf_sub_lang_delete_success", ns->n_udf_sub_lang_delete_success);
	info_append_uint64(db, "udf_sub_lang_error", ns->n_udf_sub_lang_error);

	// Transaction retransmit stats.

	info_append_uint64(db, "retransmit_client_read_dup_res", ns->n_retransmit_client_read_dup_res);

	info_append_uint64(db, "retransmit_client_write_dup_res", ns->n_retransmit_client_write_dup_res);
	info_append_uint64(db, "retransmit_client_write_repl_write", ns->n_retransmit_client_write_repl_write);

	info_append_uint64(db, "retransmit_client_delete_dup_res", ns->n_retransmit_client_delete_dup_res);
	info_append_uint64(db, "retransmit_client_delete_repl_write", ns->n_retransmit_client_delete_repl_write);

	info_append_uint64(db, "retransmit_client_udf_dup_res", ns->n_retransmit_client_udf_dup_res);
	info_append_uint64(db, "retransmit_client_udf_repl_write", ns->n_retransmit_client_udf_repl_write);

	info_append_uint64(db, "retransmit_batch_sub_dup_res", ns->n_retransmit_batch_sub_dup_res);

	info_append_uint64(db, "retransmit_udf_sub_dup_res", ns->n_retransmit_udf_sub_dup_res);
	info_append_uint64(db, "retransmit_udf_sub_repl_write", ns->n_retransmit_udf_sub_repl_write);

	// Scan stats.

	info_append_uint64(db, "scan_basic_complete", ns->n_scan_basic_complete);
	info_append_uint64(db, "scan_basic_error", ns->n_scan_basic_error);
	info_append_uint64(db, "scan_basic_abort", ns->n_scan_basic_abort);

	info_append_uint64(db, "scan_aggr_complete", ns->n_scan_aggr_complete);
	info_append_uint64(db, "scan_aggr_error", ns->n_scan_aggr_error);
	info_append_uint64(db, "scan_aggr_abort", ns->n_scan_aggr_abort);

	info_append_uint64(db, "scan_udf_bg_complete", ns->n_scan_udf_bg_complete);
	info_append_uint64(db, "scan_udf_bg_error", ns->n_scan_udf_bg_error);
	info_append_uint64(db, "scan_udf_bg_abort", ns->n_scan_udf_bg_abort);

	// Query stats.

	uint64_t agg			= ns->n_aggregation;
	uint64_t agg_success	= ns->n_agg_success;
	uint64_t agg_err		= ns->n_agg_errs;
	uint64_t agg_abort		= ns->n_agg_abort;
	uint64_t agg_records	= ns->agg_num_records;

	uint64_t lkup			= ns->n_lookup;
	uint64_t lkup_success	= ns->n_lookup_success;
	uint64_t lkup_err		= ns->n_lookup_errs;
	uint64_t lkup_abort		= ns->n_lookup_abort;
	uint64_t lkup_records	= ns->lookup_num_records;

	info_append_uint64(db, "query_reqs", ns->query_reqs);
	info_append_uint64(db, "query_fail", ns->query_fail);

	info_append_uint64(db, "query_short_queue_full", ns->query_short_queue_full);
	info_append_uint64(db, "query_long_queue_full", ns->query_long_queue_full);
	info_append_uint64(db, "query_short_reqs", ns->query_short_reqs);
	info_append_uint64(db, "query_long_reqs", ns->query_long_reqs);

	info_append_uint64(db, "query_agg", agg);
	info_append_uint64(db, "query_agg_success", agg_success);
	info_append_uint64(db, "query_agg_error", agg_err);
	info_append_uint64(db, "query_agg_abort", agg_abort);
	info_append_uint64(db, "query_agg_avg_rec_count", agg ? agg_records / agg : 0);

	info_append_uint64(db, "query_lookups", lkup);
	info_append_uint64(db, "query_lookup_success", lkup_success);
	info_append_uint64(db, "query_lookup_error", lkup_err);
	info_append_uint64(db, "query_lookup_abort", lkup_abort);
	info_append_uint64(db, "query_lookup_avg_rec_count", lkup ? lkup_records / lkup : 0);

	info_append_uint64(db, "query_udf_bg_success", ns->n_query_udf_bg_success);
	info_append_uint64(db, "query_udf_bg_failure", ns->n_query_udf_bg_failure);

	// Geospatial query stats:
	info_append_uint64(db, "geo_region_query_reqs", ns->geo_region_query_count);
	info_append_uint64(db, "geo_region_query_cells", ns->geo_region_query_cells);
	info_append_uint64(db, "geo_region_query_points", ns->geo_region_query_points);
	info_append_uint64(db, "geo_region_query_falsepos", ns->geo_region_query_falsepos);

	// Re-replication stats - relevant only for enterprise edition.

	info_append_uint64(db, "re_repl_success", ns->n_re_repl_success);
	info_append_uint64(db, "re_repl_error", ns->n_re_repl_error);
	info_append_uint64(db, "re_repl_timeout", ns->n_re_repl_timeout);

	// Special errors that deserve their own counters:

	info_append_uint64(db, "fail_xdr_forbidden", ns->n_fail_xdr_forbidden);
	info_append_uint64(db, "fail_key_busy", ns->n_fail_key_busy);
	info_append_uint64(db, "fail_generation", ns->n_fail_generation);
	info_append_uint64(db, "fail_record_too_big", ns->n_fail_record_too_big);

	// Special non-error counters:

	info_append_uint64(db, "deleted_last_bin", ns->n_deleted_last_bin);
}

//
// Iterate through the current namespace list and cons up a string
//

int
info_get_tree_namespace(char *name, char *subtree, cf_dyn_buf *db)
{
	as_namespace *ns = as_namespace_get_byname(subtree);

	if (! ns)   {
		cf_dyn_buf_append_string(db, "type=unknown"); // TODO - better message?
		return 0;
	}

	info_get_namespace_info(ns, db);
	info_namespace_config_get(ns->name, db);

	cf_dyn_buf_chomp(db);

	return 0;
}

int
info_get_tree_sets(char *name, char *subtree, cf_dyn_buf *db)
{
	char *set_name    = NULL;
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		// see if subtree has a sep as well
		set_name = strchr(subtree, TREE_SEP);

		// pull out namespace, and namespace name...
		if (set_name) {
			int ns_name_len = (set_name - subtree);
			char ns_name[ns_name_len + 1];
			memcpy(ns_name, subtree, ns_name_len);
			ns_name[ns_name_len] = '\0';
			ns = as_namespace_get_byname(ns_name);
			set_name++; // currently points to the TREE_SEP, which is not what we want.
		}
		else {
			ns = as_namespace_get_byname(subtree);
		}

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return(0);
		}
	}

	// format w/o namespace is ns1:set1:prop1=val1:prop2=val2:..propn=valn;ns1:set2...;ns2:set1...;
	if (!ns) {
		for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
			as_namespace_get_set_info(g_config.namespaces[i], set_name, db);
		}
	}
	// format w namespace w/o set name is ns:set1:prop1=val1:prop2=val2...propn=valn;ns:set2...;
	// format w namespace & set name is prop1=val1:prop2=val2...propn=valn;
	else {
		as_namespace_get_set_info(ns, set_name, db);
	}
	return(0);
}

int
info_get_tree_statistics(char *name, char *subtree, cf_dyn_buf *db)
{
	if (strcmp(subtree, "xdr") == 0) {
		as_xdr_get_stats(db);
		cf_dyn_buf_chomp(db);
		return 0;
	}

	cf_dyn_buf_append_string(db, "error");
	return -1;
}

int
info_get_tree_bins(char *name, char *subtree, cf_dyn_buf *db)
{
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		ns = as_namespace_get_byname(subtree);

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return 0;
		}
	}

	// format w/o namespace is
	// ns:num-bin-names=val1,bin-names-quota=val2,name1,name2,...;ns:...
	if (!ns) {
		for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
			as_namespace_get_bins_info(g_config.namespaces[i], db, true);
		}
	}
	// format w/namespace is
	// num-bin-names=val1,bin-names-quota=val2,name1,name2,...
	else {
		as_namespace_get_bins_info(ns, db, false);
	}

	return 0;
}

int
info_command_hist_dump(char *name, char *params, cf_dyn_buf *db)
{
	char value_str[128];
	int  value_str_len = sizeof(value_str);

	if (0 != as_info_parameter_get(params, "ns", value_str, &value_str_len)) {
		cf_info(AS_INFO, "hist-dump %s command: no namespace specified", name);
		cf_dyn_buf_append_string(db, "error-no-namespace");
		return 0;
	}

	as_namespace *ns = as_namespace_get_byname(value_str);

	if (!ns) {
		cf_info(AS_INFO, "hist-dump %s command: unknown namespace: %s", name, value_str);
		cf_dyn_buf_append_string(db, "error-unknown-namespace");
		return 0;
	}

	value_str_len = sizeof(value_str);

	if (0 != as_info_parameter_get(params, "hist", value_str, &value_str_len)) {
		cf_info(AS_INFO, "hist-dump %s command:", name);
		cf_dyn_buf_append_string(db, "error-no-hist-name");

		return 0;
	}

	// get optional set field
	char set_name_str[AS_SET_NAME_MAX_SIZE];
	int set_name_str_len = sizeof(set_name_str);
	set_name_str[0] = 0;

	as_info_parameter_get(params, "set", set_name_str, &set_name_str_len);

	// format is ns1:ns_hist1=bucket_count,offset,b1,b2,b3...;
	as_namespace_get_hist_info(ns, set_name_str, value_str, db, true);

	return 0;
}


int
info_get_tree_log(char *name, char *subtree, cf_dyn_buf *db)
{
	// see if subtree has a sep as well
	int sink_id;
	char *context = strchr(subtree, TREE_SEP);
	if (context) { // this means: log/id/context ,
		*context = 0;
		context++;

		if (0 != cf_str_atoi(subtree, &sink_id)) return(-1);

		cf_fault_sink_context_strlist(sink_id, context, db);
	}
	else { // this means just: log/id , so get all contexts
		if (0 != cf_str_atoi(subtree, &sink_id)) return(-1);

		cf_fault_sink_context_all_strlist(sink_id, db);
	}

	return(0);
}


int
info_get_tree_sindexes(char *name, char *subtree, cf_dyn_buf *db)
{
	char *index_name    = NULL;
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		// see if subtree has a sep as well
		index_name = strchr(subtree, TREE_SEP);

		// pull out namespace, and namespace name...
		if (index_name) {
			int ns_name_len = (index_name - subtree);
			char ns_name[ns_name_len + 1];
			memcpy(ns_name, subtree, ns_name_len);
			ns_name[ns_name_len] = '\0';
			ns = as_namespace_get_byname(ns_name);
			index_name++; // currently points to the TREE_SEP, which is not what we want.
		}
		else {
			ns = as_namespace_get_byname(subtree);
		}

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return(0);
		}
	}

	// format w/o namespace is:
	//    ns=ns1:set=set1:indexname=index1:prop1=val1:...:propn=valn;ns=ns1:set=set2:indexname=index2:...;ns=ns2:set=set1:...;
	if (!ns) {
		for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
			as_sindex_list_str(g_config.namespaces[i], db);
		}
	}
	// format w namespace w/o index name is:
	//    ns=ns1:set=set1:indexname=index1:prop1=val1:...:propn=valn;ns=ns1:set=set2:indexname=indexname2:...;
	else if (!index_name) {
		as_sindex_list_str(ns, db);
	}
	else {
		// format w namespace & index name is:
		//    prop1=val1;prop2=val2;...;propn=valn
		int resp = as_sindex_stats_str(ns, index_name, db);
		if (resp) {
			cf_warning(AS_INFO, "Failed to get statistics for index %s: err = %d", index_name, resp);
			INFO_COMMAND_SINDEX_FAILCODE(
					as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
					as_sindex_err_str(resp));
		}
	}
	return(0);
}

int32_t
info_get_service(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_serv_lock);
	cf_dyn_buf_append_string(db, g_serv_legacy != NULL ? g_serv_legacy : "");
	pthread_mutex_unlock(&g_serv_lock);
	return 0;
}

int32_t
info_get_service_clear_std(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_serv_lock);
	cf_dyn_buf_append_string(db, g_serv_clear_std != NULL ? g_serv_clear_std : "");
	pthread_mutex_unlock(&g_serv_lock);
	return 0;
}

int32_t
info_get_service_tls_std(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_serv_lock);
	cf_dyn_buf_append_string(db, g_serv_tls_std != NULL ? g_serv_tls_std : "");
	pthread_mutex_unlock(&g_serv_lock);
	return 0;
}

int32_t
info_get_service_clear_alt(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_serv_lock);
	cf_dyn_buf_append_string(db, g_serv_clear_alt != NULL ? g_serv_clear_alt : "");
	pthread_mutex_unlock(&g_serv_lock);
	return 0;
}

int32_t
info_get_service_tls_alt(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_serv_lock);
	cf_dyn_buf_append_string(db, g_serv_tls_alt != NULL ? g_serv_tls_alt : "");
	pthread_mutex_unlock(&g_serv_lock);
	return 0;
}

// SINDEX wire protocol examples:
// 1.) NUMERIC:    sindex-create:ns=usermap;set=demo;indexname=um_age;indexdata=age,numeric
// 2.) STRING:     sindex-create:ns=usermap;set=demo;indexname=um_state;indexdata=state,string
/*
 *  Parameters:
 *  	params --- string passed to asinfo call
 *  	imd    --  parses the params and fills this sindex struct.
 *
 *  Returns
 *  	AS_SINDEX_OK if it successfully fills up imd
 *      AS_SINDEX_ERR_PARAM otherwise
 *     TODO REVIEW  : send cmd as argument
 */
int
as_info_parse_params_to_sindex_imd(char* params, as_sindex_metadata *imd, cf_dyn_buf* db,
		bool is_create, bool *is_smd_op, char * OP)
{
	if (! imd) {
		cf_warning(AS_INFO, "%s : Failed. internal error.", OP);
		return AS_SINDEX_ERR_PARAM;
	}

	char indexname_str[AS_ID_INAME_SZ];
	int  indname_len  = sizeof(indexname_str);
	int ret = as_info_parameter_get(params, STR_INDEXNAME, indexname_str,
			&indname_len);
	if ( ret == -1 ) {
		cf_warning(AS_INFO, "%s : Failed. Missing Index name.", OP);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Missing Index name");
		return AS_SINDEX_ERR_PARAM;
	}
	else if ( ret == -2 ) {
		cf_warning(AS_INFO, "%s : Failed. Index name longer than allowed %d.",
				OP, AS_ID_INAME_SZ-1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index name too long");
		return AS_SINDEX_ERR_PARAM;
	}

	char cmd[128];
	snprintf(cmd, 128, "%s %s", OP, indexname_str);

	char ns_str[AS_ID_NAMESPACE_SZ];
	int ns_len       = sizeof(ns_str);
	ret = as_info_parameter_get(params, STR_NS, ns_str, &ns_len);
	if ( ret == -1 ) {
		cf_warning(AS_INFO, "%s : Failed. Missing Namespace name.", cmd);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Missing Namespace name");
		return AS_SINDEX_ERR_PARAM;
	}
	else if (ret == -2 ) {
		cf_warning(AS_INFO, "%s : Failed. Namespace name longer than allowed %d.",
				cmd, AS_ID_NAMESPACE_SZ - 1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace name too long");
		return AS_SINDEX_ERR_PARAM;
	}

	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (! ns) {
		cf_warning(AS_INFO, "%s : Failed. Namespace '%s' not found %d",
				cmd, ns_str, ns_len);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Namespace Not Found");
		return AS_SINDEX_ERR_PARAM;
	}
	if (ns->single_bin) {
		cf_warning(AS_INFO, "%s : Failed. Secondary Index is not allowed on single bin "
				"namespace '%s'.", cmd, ns_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Single bin namespace");
		return AS_SINDEX_ERR_PARAM;
	}

	char set_str[AS_SET_NAME_MAX_SIZE];
	int set_len  = sizeof(set_str);
	if (imd->set) {
		cf_free(imd->set);
		imd->set = NULL;
	}
	ret = as_info_parameter_get(params, STR_SET, set_str, &set_len);
	if (!ret && set_len != 0) {
		if (as_namespace_get_create_set_w_len(ns, set_str, set_len, NULL, NULL)
				!= 0) {
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"Set name quota full");
			return AS_SINDEX_ERR_PARAM;
		}
		imd->set = cf_strdup(set_str);
	} else if (ret == -2) {
		cf_warning(AS_INFO, "%s : Failed. Setname longer than %d for index.",
				cmd, AS_SET_NAME_MAX_SIZE - 1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Set name too long");
		return AS_SINDEX_ERR_PARAM;
	}

	char cluster_op[6];
	int cluster_op_len = sizeof(cluster_op);
	if (as_info_parameter_get(params, "cluster_op", cluster_op, &cluster_op_len)
			!= 0) {
		*is_smd_op = true;
	}
	else if (strcmp(cluster_op, "true") == 0) {
		*is_smd_op = true;
	}
	else if (strcmp(cluster_op, "false") == 0) {
		*is_smd_op = false;
	}

	// Delete only need parsing till here
	if (!is_create) {
		imd->ns_name = cf_strdup(ns->name);
		imd->iname   = cf_strdup(indexname_str);
		return 0;
	}

	char indextype_str[AS_SINDEX_TYPE_STR_SIZE];
	int  indtype_len = sizeof(indextype_str);
	ret = as_info_parameter_get(params, STR_ITYPE, indextype_str, &indtype_len);
	if (ret == -1) {
		// if not specified the index type is DEFAULT
		imd->itype = AS_SINDEX_ITYPE_DEFAULT;
	}
	else if (ret == -2) {
		cf_warning(AS_INFO, "%s : Failed. Indextype str longer than allowed %d.",
				cmd, AS_SINDEX_TYPE_STR_SIZE-1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Indextype is too long");
		return AS_SINDEX_ERR_PARAM;

	}
	else {
		if (strncasecmp(indextype_str, STR_ITYPE_DEFAULT, 7) == 0) {
			imd->itype = AS_SINDEX_ITYPE_DEFAULT;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_LIST, 4) == 0) {
			imd->itype = AS_SINDEX_ITYPE_LIST;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_MAPKEYS, 7) == 0) {
			imd->itype = AS_SINDEX_ITYPE_MAPKEYS;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_MAPVALUES, 9) == 0) {
			imd->itype = AS_SINDEX_ITYPE_MAPVALUES;
		}
		else {
			cf_warning(AS_INFO, "%s : Failed. Invalid indextype '%s'.", cmd,
					indextype_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"Invalid indextype. Should be one of [DEFAULT, LIST, MAPKEYS, MAPVALUES]");
			return AS_SINDEX_ERR_PARAM;
		}
	}

	// Indexdata = binpath,keytype
	char indexdata_str[AS_SINDEXDATA_STR_SIZE];
	int  indexdata_len = sizeof(indexdata_str);
	if (as_info_parameter_get(params, STR_INDEXDATA, indexdata_str,
				&indexdata_len)) {
		cf_warning(AS_INFO, "%s : Failed. Invalid indexdata '%s'.", cmd,
				indexdata_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Invalid indexdata");
		return AS_SINDEX_ERR_PARAM;
	}

	cf_vector *str_v = cf_vector_create(sizeof(void *), 10, VECTOR_FLAG_INITZERO);
	cf_str_split(",", indexdata_str, str_v);
	if ((cf_vector_size(str_v)) > 2) {
		cf_warning(AS_INFO, "%s : Failed. >1 bins specified in indexdata.",
				cmd);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Number of bins more than 1");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}

	char *path_str = NULL;
	cf_vector_get(str_v, 0, &path_str);
	if (! path_str) {
		cf_warning(AS_INFO, "%s : Failed. Missing Bin Name.", cmd);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Missing Bin name");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}

	if (as_sindex_extract_bin_path(imd, path_str)
			|| ! imd->bname) {
		cf_warning(AS_INFO, "%s : Failed. Invalid Bin Path '%s'.", cmd, path_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Invalid Bin path");
		return AS_SINDEX_ERR_PARAM;
	}

	if (imd->bname && strlen(imd->bname) >= AS_ID_BIN_SZ) {
		cf_warning(AS_INFO, "%s : Failed. Bin Name longer than allowed %d",
				cmd, AS_ID_BIN_SZ - 1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Bin Name too long");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}

	char *type_str = NULL;
	cf_vector_get(str_v, 1, &type_str);
	if (! type_str) {
		cf_warning(AS_INFO, "%s : Failed. Missing Bin type", cmd);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Missing Bin Type.");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}

	as_sindex_ktype ktype = as_sindex_ktype_from_string(type_str);
	if (ktype == COL_TYPE_INVALID) {
		cf_warning(AS_INFO, "%s : Failed. Invalid Bin type '%s'.", cmd, type_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Invalid Bin type. Supported types [Numeric, String, Geo2dsphere]");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}
	imd->sktype = ktype;



	cf_vector_destroy(str_v);

	if (is_create) {
		imd->ns_name = cf_strdup(ns->name);
		imd->iname   = cf_strdup(indexname_str);
	}
	imd->path_str = cf_strdup(path_str);
	return AS_SINDEX_OK;
}

int info_command_sindex_create(char *name, char *params, cf_dyn_buf *db)
{
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	bool is_smd_op = true;

	// Check info-command params for correctness.
	int res = as_info_parse_params_to_sindex_imd(params, &imd, db, true, &is_smd_op, "SINDEX CREATE");

	if (res != 0) {
		goto ERR;
	}

	as_namespace *ns = as_namespace_get_byname(imd.ns_name);
	res = as_sindex_create_check_params(ns, &imd);

	if (res == AS_SINDEX_ERR_FOUND) {
		cf_warning(AS_INFO, "SINDEX CREATE: Index already exists on namespace '%s', either with same name '%s' or same bin '%s' / type '%s' combination.",
				imd.ns_name, imd.iname, imd.bname,
				as_sindex_ktype_str(imd.sktype));
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_FOUND,
				"Index with the same name already exists or this bin has already been indexed.");
		goto ERR;
	}
	else if (res == AS_SINDEX_ERR_MAXCOUNT) {
		cf_warning(AS_INFO, "SINDEX CREATE : More than %d index are not allowed per namespace.", AS_SINDEX_MAX);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_MAXCOUNT,
				"Reached maximum number of sindex allowed");
		goto ERR;
	}

	if (is_smd_op == true)
	{
		cf_info(AS_INFO, "SINDEX CREATE : Request received for %s:%s via SMD", imd.ns_name, imd.iname);

		char smd_key[SINDEX_SMD_KEY_SIZE];

		as_sindex_imd_to_smd_key(&imd, smd_key);
		res = as_smd_set_metadata(SINDEX_MODULE, smd_key, imd.iname);

		if (res != 0) {
			cf_warning(AS_INFO, "SINDEX CREATE : Queuing the index %s metadata to SMD failed with error %s",
					imd.iname, as_sindex_err_str(res));
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, as_sindex_err_str(res));
			goto ERR;
		}
	}
	else if (is_smd_op == false) {
		cf_info(AS_INFO, "SINDEX CREATE : Request received for %s:%s via info", imd.ns_name, imd.iname);
		res = as_sindex_create(ns, &imd);
		if (0 != res) {
			cf_warning(AS_INFO, "SINDEX CREATE : Failed with error %s for index %s",
					as_sindex_err_str(res), imd.iname);
			INFO_COMMAND_SINDEX_FAILCODE(as_sindex_err_to_clienterr(res, __FILE__, __LINE__),
					as_sindex_err_str(res));
			goto ERR;
		}
	}
	cf_dyn_buf_append_string(db, "OK");
ERR:
	as_sindex_imd_free(&imd);
	return(0);

}

int info_command_sindex_delete(char *name, char *params, cf_dyn_buf *db) {
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	bool is_smd_op = true;
	int res = as_info_parse_params_to_sindex_imd(params, &imd, db, false, &is_smd_op, "SINDEX DROP");

	if (res != 0) {
		goto ERR;
	}

	as_namespace *ns = as_namespace_get_byname(imd.ns_name);

	// Do not use as_sindex_exists_by_defn() here, it'll fail because bname is null.
	if (!as_sindex_delete_checker(ns, &imd)) {
		cf_warning(AS_INFO, "SINDEX DROP : Index %s:%s does not exist on the system",
				imd.ns_name, imd.iname);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND,
				"Index does not exist on the system.");
		goto ERR;
	}

	if (is_smd_op == true)
	{
		cf_info(AS_INFO, "SINDEX DROP : Request received for %s:%s via SMD", imd.ns_name, imd.iname);

		char smd_key[SINDEX_SMD_KEY_SIZE];

		if (as_sindex_delete_imd_to_smd_key(ns, &imd, smd_key)) {
			res = as_smd_delete_metadata(SINDEX_MODULE, smd_key);
		}
		else {
			res = AS_SINDEX_ERR_NOTFOUND;
		}

		if (0 != res) {
			cf_warning(AS_INFO, "SINDEX DROP : Queuing the index %s metadata to SMD failed with error %s",
					imd.iname, as_sindex_err_str(res));
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, as_sindex_err_str(res));
			goto ERR;
		}
	}
	else if(is_smd_op == false)
	{
		cf_info(AS_INFO, "SINDEX DROP : Request received for %s:%s via info", imd.ns_name, imd.iname);
		res = as_sindex_destroy(ns, &imd);
		if (0 != res) {
			cf_warning(AS_INFO, "SINDEX DROP : Failed with error %s for index %s",
					as_sindex_err_str(res), imd.iname);
			INFO_COMMAND_SINDEX_FAILCODE(as_sindex_err_to_clienterr(res, __FILE__, __LINE__),
					as_sindex_err_str(res));
			goto ERR;
		}
	}

	cf_dyn_buf_append_string(db, "OK");
ERR:
	as_sindex_imd_free(&imd);
	return 0;
}

int
as_info_parse_ns_iname(char* params, as_namespace ** ns, char ** iname, cf_dyn_buf* db, char * sindex_cmd)
{
	char ns_str[AS_ID_NAMESPACE_SZ];
	int ns_len = sizeof(ns_str);
	int ret    = 0;

	ret = as_info_parameter_get(params, "ns", ns_str, &ns_len);
	if (ret) {
		if (ret == -2) {
			cf_warning(AS_INFO, "%s : namespace name exceeds max length %d",
				sindex_cmd, AS_ID_NAMESPACE_SZ);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace name exceeds max length");
		}
		else {
			cf_warning(AS_INFO, "%s : invalid namespace %s", sindex_cmd, ns_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Specified");
		}
		return -1;
	}

	*ns = as_namespace_get_byname(ns_str);
	if (!*ns) {
		cf_warning(AS_INFO, "%s : namespace %s not found", sindex_cmd, ns_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return -1;
	}

	// get indexname
	char index_name_str[AS_ID_INAME_SZ];
	int  index_len = sizeof(index_name_str);
	ret = as_info_parameter_get(params, "indexname", index_name_str, &index_len);
	if (ret) {
		if (ret == -2) {
			cf_warning(AS_INFO, "%s : indexname exceeds max length %d", sindex_cmd, AS_ID_INAME_SZ);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index Name exceeds max length");
		}
		else {
			cf_warning(AS_INFO, "%s : invalid indexname %s", sindex_cmd, index_name_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index Name Not Specified");
		}
		return -1;
	}

	cf_info(AS_SINDEX, "%s : received request on index %s - namespace %s",
			sindex_cmd, index_name_str, ns_str);

	*iname = cf_strdup(index_name_str);

	return 0;
}

int info_command_abort_scan(char *name, char *params, cf_dyn_buf *db) {
	char context[100];
	int  context_len = sizeof(context);
	int rv = -1;
	if (0 == as_info_parameter_get(params, "id", context, &context_len)) {
		uint64_t trid;
		trid = strtoull(context, NULL, 10);
		if (trid != 0) {
			rv = as_scan_abort(trid);
		}
	}

	if (rv != 0) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":Transaction Not Found");
	}
	else {
		cf_dyn_buf_append_string(db, "OK");
	}

	return 0;
}

int info_command_abort_all_scans(char *name, char *params, cf_dyn_buf *db) {

	int n_scans_killed = as_scan_abort_all();

	cf_dyn_buf_append_string(db, "OK - number of scans killed: ");
	cf_dyn_buf_append_int(db, n_scans_killed);

	return 0;
}

int info_command_query_kill(char *name, char *params, cf_dyn_buf *db) {
	char context[100];
	int  context_len = sizeof(context);
	int  rv          = AS_QUERY_ERR;
	if (0 == as_info_parameter_get(params, "trid", context, &context_len)) {
		uint64_t trid;
		trid = strtoull(context, NULL, 10);
		if (trid != 0) {
			rv = as_query_kill(trid);
		}
	}

	if (AS_QUERY_OK != rv) {
		cf_dyn_buf_append_string(db, "Transaction Not Found");
	}
	else {
		cf_dyn_buf_append_string(db, "Ok");
	}

	return 0;



}
int info_command_sindex_stat(char *name, char *params, cf_dyn_buf *db) {
	as_namespace  *ns = NULL;
	char * iname = NULL;

	if (as_info_parse_ns_iname(params, &ns, &iname, db, "SINDEX STAT")) {
		return 0;
	}

	int resp = as_sindex_stats_str(ns, iname, db);
	if (resp)  {
		cf_warning(AS_INFO, "SINDEX STAT : for index %s - ns %s failed with error %d",
			iname, ns->name, resp);
		INFO_COMMAND_SINDEX_FAILCODE(
				as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
				as_sindex_err_str(resp));
	}

	if (iname) {
		cf_free(iname);
	}
	return(0);
}


// sindex-histogram:ns=test_D;indexname=indname;enable=true/false
int info_command_sindex_histogram(char *name, char *params, cf_dyn_buf *db)
{
	as_namespace * ns = NULL;
	char * iname = NULL;
	if (as_info_parse_ns_iname(params, &ns, &iname, db, "SINDEX HISTOGRAM")) {
		return 0;
	}

	char op[10];
	int op_len = sizeof(op);

	if (as_info_parameter_get(params, "enable", op, &op_len)) {
		cf_info(AS_INFO, "SINDEX HISTOGRAM : invalid OP");
		cf_dyn_buf_append_string(db, "Invalid Op");
		goto END;
	}

	bool enable = false;
	if (!strncmp(op, "true", 5) && op_len != 5) {
		enable = true;
	}
	else if (!strncmp(op, "false", 6) && op_len != 6) {
		enable = false;
	}
	else {
		cf_info(AS_INFO, "SINDEX HISTOGRAM : invalid OP");
		cf_dyn_buf_append_string(db, "Invalid Op");
		goto END;
	}

	int resp = as_sindex_histogram_enable(ns, iname, enable);
	if (resp) {
		cf_warning(AS_INFO, "SINDEX HISTOGRAM : for index %s - ns %s failed with error %d",
			iname, ns->name, resp);
		INFO_COMMAND_SINDEX_FAILCODE(
				as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
				as_sindex_err_str(resp));
	} else {
		cf_dyn_buf_append_string(db, "Ok");
		cf_info(AS_INFO, "SINDEX HISTOGRAM : for index %s - ns %s histogram is set as %s",
			iname, ns->name, op);
	}

END:
	if (iname) {
		cf_free(iname);
	}
	return(0);
}

int info_command_sindex_list(char *name, char *params, cf_dyn_buf *db) {
	bool listall = true;
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (!as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		listall = false;
	}

	if (listall) {
		bool found = false;
		for (int i = 0; i < g_config.n_namespaces; i++) {
			as_namespace *ns = g_config.namespaces[i];
			if (ns) {
				if (!as_sindex_list_str(ns, db)) {
					found = true;
				}
				else {
					cf_detail(AS_INFO, "No indexes for namespace %s", ns->name);
				}
			}
		}

		if (found) {
			cf_dyn_buf_chomp(db);
		}
		else {
			cf_dyn_buf_append_string(db, "Empty");
		}
	}
	else {
		as_namespace *ns = as_namespace_get_byname(ns_str);
		if (!ns) {
			cf_warning(AS_INFO, "SINDEX LIST : ns %s not found", ns_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Namespace Not Found");
			return 0;
		} else {
			if (as_sindex_list_str(ns, db)) {
				cf_info(AS_INFO, "ns not found");
				cf_dyn_buf_append_string(db, "Empty");
			}
			return 0;
		}
	}
	return(0);
}

// Defined in "make_in/version.c" (auto-generated by the build system.)
extern const char aerospike_build_id[];
extern const char aerospike_build_time[];
extern const char aerospike_build_type[];
extern const char aerospike_build_os[];
extern const char aerospike_build_features[];

int
as_info_init()
{
	// g_info_node_info_history_hash is a hash of all nodes that have ever been
	// recognized by this node - either via paxos or info messages.
	g_info_node_info_history_hash = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node), sizeof(info_node_info), 64, CF_SHASH_BIG_LOCK);

	// g_info_node_info_hash is a hash of all nodes *currently* in the cluster.
	// This hash should *always* be a subset of g_info_node_info_history_hash -
	// to ensure this, you should take the lock on the corresponding key in
	// info_history_hash before modifying an element in this hash table. This
	// hash is used to create the services list.
	g_info_node_info_hash = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node), sizeof(info_node_info), 64, CF_SHASH_BIG_LOCK);

	// create worker threads
	g_info_work_q = cf_queue_create(sizeof(as_info_transaction), true);

	char vstr[64];
	sprintf(vstr, "%s build %s", aerospike_build_type, aerospike_build_id);

	// Set some basic values
	as_info_set("version", vstr, true);                  // Returns the edition and build number.
	as_info_set("build", aerospike_build_id, true);      // Returns the build number for this server.
	as_info_set("build_os", aerospike_build_os, true);   // Return the OS used to create this build.
	as_info_set("build_time", aerospike_build_time, true); // Return the creation time of this build.
	as_info_set("edition", aerospike_build_type, true);  // Return the edition of this build.
	as_info_set("digests", "RIPEMD160", false);          // Returns the hashing algorithm used by the server for key hashing.
	as_info_set("status", "ok", false);                  // Always returns ok, used to verify service port is open.
	as_info_set("STATUS", "OK", false);                  // Always returns OK, used to verify service port is open.

	char istr[1024];
	cf_str_itoa(AS_PARTITIONS, istr, 10);
	as_info_set("partitions", istr, false);              // Returns the number of partitions used to hash keys across.

	cf_str_itoa_u64(g_config.self_node, istr, 16);
	as_info_set("node", istr, true);                     // Node ID. Unique 15 character hex string for each node based on the mac address and port.
	as_info_set("name", istr, false);                    // Alias to 'node'.
	// Returns list of features supported by this server
	static char features[1024];
	strcat(features, "peers;cdt-list;cdt-map;pipelining;geo;float;batch-index;replicas;replicas-all;replicas-master;replicas-prole;udf");
	strcat(features, aerospike_build_features);
	as_info_set("features", features, true);
	as_hb_mode hb_mode;
	as_hb_info_listen_addr_get(&hb_mode, istr, sizeof(istr));
	as_info_set( hb_mode == AS_HB_MODE_MESH ? "mesh" :  "mcast", istr, false);

	// All commands accepted by asinfo/telnet
	as_info_set("help", "alloc-info;asm;bins;build;build_os;build_time;cluster-name;config-get;config-set;"
				"df;digests;dump-cluster;dump-fabric;dump-hb;dump-migrates;dump-msgs;dump-rw;"
				"dump-si;dump-skew;dump-smd;dump-wb;dump-wb-summary;feature-key;get-config;get-sl;hist-dump;"
				"hist-track-start;hist-track-stop;jem-stats;jobs;latency;log;log-set;"
				"log-message;logs;mcast;mem;mesh;mstats;mtrace;name;namespace;namespaces;node;"
				"racks;recluster;revive;roster;roster-set;service;services;services-alumni;services-alumni-reset;set-config;"
				"set-log;sets;set-sl;show-devices;sindex;sindex-create;sindex-delete;"
				"sindex-histogram;"
				"smd;statistics;status;tip;tip-clear;truncate;truncate-undo;version;",
				false);
	/*
	 * help intentionally does not include the following:
	 * cluster-generation;features;objects;
	 * partition-generation;partition-info;partitions;replicas-master;
	 * replicas-prole;replicas-read;replicas-write;throughput
	 */

	// Set up some dynamic functions
	as_info_set_dynamic("alumni-clear-std", info_get_alumni_clear_std, false);        // Supersedes "services-alumni" for non-TLS service.
	as_info_set_dynamic("alumni-tls-std", info_get_alumni_tls_std, false);            // Supersedes "services-alumni" for TLS service.
	as_info_set_dynamic("bins", info_get_bins, false);                                // Returns bin usage information and used bin names.
	as_info_set_dynamic("cluster-generation", info_get_cluster_generation, true);     // Returns cluster generation.
	as_info_set_dynamic("cluster-name", info_get_cluster_name, false);                // Returns cluster name.
	as_info_set_dynamic("endpoints", info_get_endpoints, false);                      // Returns the expanded bind / access address configuration.
	as_info_set_dynamic("feature-key", info_get_features, false);                     // Returns the contents of the feature key (except signature).
	as_info_set_dynamic("get-config", info_get_config, false);                        // Returns running config for specified context.
	as_info_set_dynamic("logs", info_get_logs, false);                                // Returns a list of log file locations in use by this server.
	as_info_set_dynamic("namespaces", info_get_namespaces, false);                    // Returns a list of namespace defined on this server.
	as_info_set_dynamic("objects", info_get_objects, false);                          // Returns the number of objects stored on this server.
	as_info_set_dynamic("partition-generation", info_get_partition_generation, true); // Returns the current partition generation.
	as_info_set_dynamic("partition-info", info_get_partition_info, false);            // Returns partition ownership information.
	as_info_set_dynamic("peers-clear-alt", info_get_services_clear_alt, false);       // Supersedes "services-alternate" for non-TLS, alternate addresses.
	as_info_set_dynamic("peers-clear-std", info_get_services_clear_std, false);       // Supersedes "services" for non-TLS, standard addresses.
	as_info_set_dynamic("peers-generation", info_get_services_generation, false);     // Returns the generation of the peers-*-* services lists.
	as_info_set_dynamic("peers-tls-alt", info_get_services_tls_alt, false);           // Supersedes "services-alternate" for TLS, alternate addresses.
	as_info_set_dynamic("peers-tls-std", info_get_services_tls_std, false);           // Supersedes "services" for TLS, standard addresses.
	as_info_set_dynamic("replicas", info_get_replicas, false);                        // Same as replicas-all, but includes regime.
	as_info_set_dynamic("replicas-all", info_get_replicas_all, false);                // Base 64 encoded binary representation of partitions this node is replica for.
	as_info_set_dynamic("replicas-master", info_get_replicas_master, false);          // Base 64 encoded binary representation of partitions this node is master (replica) for.
	as_info_set_dynamic("replicas-prole", info_get_replicas_prole, false);            // Base 64 encoded binary representation of partitions this node is prole (replica) for.
	as_info_set_dynamic("service", info_get_service, false);                          // IP address and server port for this node, expected to be a single.
	                                                                                  // address/port per node, may be multiple address if this node is configured.
	                                                                                  // to listen on multiple interfaces (typically not advised).
	as_info_set_dynamic("service-clear-alt", info_get_service_clear_alt, false);      // Supersedes "service". The alternate address and port for this node's non-TLS
	                                                                                  // client service.
	as_info_set_dynamic("service-clear-std", info_get_service_clear_std, false);      // Supersedes "service". The address and port for this node's non-TLS client service.
	as_info_set_dynamic("service-tls-alt", info_get_service_tls_alt, false);          // Supersedes "service". The alternate address and port for this node's TLS
	                                                                                  // client service.
	as_info_set_dynamic("service-tls-std", info_get_service_tls_std, false);          // Supersedes "service". The address and port for this node's TLS client service.
	as_info_set_dynamic("services", info_get_services, true);                         // List of addresses of neighbor cluster nodes to advertise for Application to connect.
	as_info_set_dynamic("services-alternate", info_get_alt_addr, false);              // IP address mapping from internal to public ones
	as_info_set_dynamic("services-alumni", info_get_services_alumni, true);           // All neighbor addresses (services) this server has ever know about.
	as_info_set_dynamic("services-alumni-reset", info_services_alumni_reset, false);  // Reset the services alumni to equal services.
	as_info_set_dynamic("sets", info_get_sets, false);                                // Returns set statistics for all or a particular set.
	as_info_set_dynamic("statistics", info_get_stats, true);                          // Returns system health and usage stats for this server.

#ifdef INFO_SEGV_TEST
	as_info_set_dynamic("segvtest", info_segv_test, true);
#endif

	// Tree-based names
	as_info_set_tree("bins", info_get_tree_bins);           // Returns bin usage information and used bin names for all or a particular namespace.
	as_info_set_tree("log", info_get_tree_log);             //
	as_info_set_tree("namespace", info_get_tree_namespace); // Returns health and usage stats for a particular namespace.
	as_info_set_tree("sets", info_get_tree_sets);           // Returns set statistics for all or a particular set.
	as_info_set_tree("statistics", info_get_tree_statistics);

	// Define commands
	as_info_set_command("config-get", info_command_config_get, PERM_NONE);                    // Returns running config for specified context.
	as_info_set_command("config-set", info_command_config_set, PERM_SET_CONFIG);              // Set a configuration parameter at run time, configuration parameter must be dynamic.
	as_info_set_command("dump-cluster", info_command_dump_cluster, PERM_LOGGING_CTRL);        // Print debug information about clustering and exchange to the log file.
	as_info_set_command("dump-fabric", info_command_dump_fabric, PERM_LOGGING_CTRL);          // Print debug information about fabric to the log file.
	as_info_set_command("dump-hb", info_command_dump_hb, PERM_LOGGING_CTRL);                  // Print debug information about heartbeat state to the log file.
	as_info_set_command("dump-hlc", info_command_dump_hlc, PERM_LOGGING_CTRL);                // Print debug information about Hybrid Logical Clock to the log file.
	as_info_set_command("dump-migrates", info_command_dump_migrates, PERM_LOGGING_CTRL);      // Print debug information about migration.
	as_info_set_command("dump-msgs", info_command_dump_msgs, PERM_LOGGING_CTRL);              // Print debug information about existing 'msg' objects and queues to the log file.
	as_info_set_command("dump-rw", info_command_dump_rw_request_hash, PERM_LOGGING_CTRL);     // Print debug information about transaction hash table to the log file.
	as_info_set_command("dump-si", info_command_dump_si, PERM_LOGGING_CTRL);                  // Print information about a Secondary Index
	as_info_set_command("dump-skew", info_command_dump_skew, PERM_LOGGING_CTRL);              // Print information about clock skew
	as_info_set_command("dump-smd", info_command_dump_smd, PERM_LOGGING_CTRL);                // Print information about System Metadata (SMD) to the log file.
	as_info_set_command("dump-wb", info_command_dump_wb, PERM_LOGGING_CTRL);                  // Print debug information about Write Bocks (WB) to the log file.
	as_info_set_command("dump-wb-summary", info_command_dump_wb_summary, PERM_LOGGING_CTRL);  // Print summary information about all Write Blocks (WB) on a device to the log file.
	as_info_set_command("get-config", info_command_config_get, PERM_NONE);                    // Returns running config for all or a particular context.
	as_info_set_command("get-sl", info_command_get_sl, PERM_NONE);                            // Get the Paxos succession list.
	as_info_set_command("hist-dump", info_command_hist_dump, PERM_NONE);                      // Returns a histogram snapshot for a particular histogram.
	as_info_set_command("hist-track-start", info_command_hist_track, PERM_SERVICE_CTRL);      // Start or Restart histogram tracking.
	as_info_set_command("hist-track-stop", info_command_hist_track, PERM_SERVICE_CTRL);       // Stop histogram tracking.
	as_info_set_command("jem-stats", info_command_jem_stats, PERM_LOGGING_CTRL);              // Print JEMalloc statistics to the log file.
	as_info_set_command("latency", info_command_hist_track, PERM_NONE);                       // Returns latency and throughput information.
	as_info_set_command("log-message", info_command_log_message, PERM_NONE);                  // Log a message.
	as_info_set_command("log-set", info_command_log_set, PERM_LOGGING_CTRL);                  // Set values in the log system.
	as_info_set_command("peers-clear-alt", info_get_services_clear_alt_delta, PERM_NONE);     // The delta update version of "peers-clear-alt".
	as_info_set_command("peers-clear-std", info_get_services_clear_std_delta, PERM_NONE);     // The delta update version of "peers-clear-std".
	as_info_set_command("peers-tls-alt", info_get_services_tls_alt_delta, PERM_NONE);         // The delta update version of "peers-tls-alt".
	as_info_set_command("peers-tls-std", info_get_services_tls_std_delta, PERM_NONE);         // The delta update version of "peers-tls-std".
	as_info_set_command("racks", info_command_racks, PERM_NONE);                              // Rack-aware information.
	as_info_set_command("recluster", info_command_recluster, PERM_NONE);                      // Force cluster to re-form. FIXME - what permission?
	as_info_set_command("revive", info_command_revive, PERM_NONE);                            // Mark all partitions as "trusted".
	as_info_set_command("roster", info_command_roster, PERM_NONE);                            // Roster information.
	as_info_set_command("roster-set", info_command_roster_set, PERM_NONE);                    // Set the entire roster. FIXME - what permission?
	as_info_set_command("set-config", info_command_config_set, PERM_SET_CONFIG);              // Set config values.
	as_info_set_command("set-log", info_command_log_set, PERM_LOGGING_CTRL);                  // Set values in the log system.
	as_info_set_command("show-devices", info_command_show_devices, PERM_LOGGING_CTRL);        // Print snapshot of wblocks to the log file.
	as_info_set_command("throughput", info_command_hist_track, PERM_NONE);                    // Returns throughput info.
	as_info_set_command("tip", info_command_tip, PERM_SERVICE_CTRL);                          // Add external IP to mesh-mode heartbeats.
	as_info_set_command("tip-clear", info_command_tip_clear, PERM_SERVICE_CTRL);              // Clear tip list from mesh-mode heartbeats.
	as_info_set_command("truncate", info_command_truncate, PERM_TRUNCATE);                    // Truncate a namespace or set.
	as_info_set_command("truncate-undo", info_command_truncate_undo, PERM_TRUNCATE);          // Undo a truncate command.
	as_info_set_command("xdr-command", as_info_command_xdr, PERM_SERVICE_CTRL);               // Command to XDR module.

	// SINDEX
	as_info_set_dynamic("sindex", info_get_sindexes, false);
	as_info_set_tree("sindex", info_get_tree_sindexes);
	as_info_set_command("sindex-create", info_command_sindex_create, PERM_INDEX_MANAGE);  // Create a secondary index.
	as_info_set_command("sindex-delete", info_command_sindex_delete, PERM_INDEX_MANAGE);  // Delete a secondary index.

	// UDF
	as_info_set_dynamic("udf-list", udf_cask_info_list, false);
	as_info_set_command("udf-put", udf_cask_info_put, PERM_UDF_MANAGE);
	as_info_set_command("udf-get", udf_cask_info_get, PERM_NONE);
	as_info_set_command("udf-remove", udf_cask_info_remove, PERM_UDF_MANAGE);
	as_info_set_command("udf-clear-cache", udf_cask_info_clear_cache, PERM_UDF_MANAGE);

	// JOBS
	as_info_set_command("jobs", info_command_mon_cmd, PERM_JOB_MONITOR);  // Manipulate the multi-key lookup monitoring infrastructure.

	// Undocumented Secondary Index Command
	as_info_set_command("sindex-histogram", info_command_sindex_histogram, PERM_SERVICE_CTRL);

	as_info_set_dynamic("query-list", as_query_list, false);
	as_info_set_command("query-kill", info_command_query_kill, PERM_QUERY_MANAGE);
	as_info_set_command("scan-abort", info_command_abort_scan, PERM_SCAN_MANAGE);            // Abort a scan with a given id.
	as_info_set_command("scan-abort-all", info_command_abort_all_scans, PERM_SCAN_MANAGE);   // Abort all scans.
	as_info_set_dynamic("scan-list", as_scan_list, false);                                   // List info for all scan jobs.
	as_info_set_command("sindex-stat", info_command_sindex_stat, PERM_NONE);
	as_info_set_command("sindex-list", info_command_sindex_list, PERM_NONE);
	as_info_set_dynamic("sindex-builder-list", as_sbld_list, false);                         // List info for all secondary index builder jobs.

	as_xdr_info_init();

	// Spin up the Info threads *after* all static and dynamic Info commands have been added
	// so we can guarantee that the static and dynamic lists will never again be changed.
	pthread_attr_t thr_attr;
	pthread_attr_init(&thr_attr);
	pthread_attr_setdetachstate(&thr_attr, PTHREAD_CREATE_DETACHED);

	for (int i = 0; i < g_config.n_info_threads; i++) {
		pthread_t tid;
		if (0 != pthread_create(&tid, &thr_attr, thr_info_fn, (void *) 0 )) {
			cf_crash(AS_INFO, "pthread_create: %s", cf_strerror(errno));
		}
	}

	as_fabric_register_msg_fn(M_TYPE_INFO, info_mt, sizeof(info_mt), INFO_MSG_SCRATCH_SIZE, info_msg_fn, 0 /* udata */ );

	as_exchange_register_listener(info_clustering_event_listener, NULL);

	// Initialize services info exchange machinery.
	set_static_services();

	if (g_config.tls_service.tls_our_name != NULL) {
		g_serv_tls_name = g_config.tls_service.tls_our_name;
	}

	++g_serv_gen;

	pthread_t info_interfaces_th;
	pthread_create(&info_interfaces_th, &thr_attr, info_interfaces_fn, 0);
	return(0);
}
