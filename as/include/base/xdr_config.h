/*
 * xdr_config.h
 *
 * Copyright (C) 2011-2016 Aerospike, Inc.
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

#include "citrusleaf/cf_vector.h"

#include "node.h"
#include "tls.h"

//==========================================================
// Forward declarations.
//

//==========================================================
// Constants & typedefs.
//

// Length definitions. This should be in sync with the server definitions.
// It is bad that we are not using a common header file for all this.
#define CLUSTER_MAX_SZ		128
#define NAMESPACE_MAX_NUM	32
#define DC_MAX_NUM			32

typedef struct xdr_node_lst_s {
	cf_node		node;
	uint64_t	time[DC_MAX_NUM];
} xdr_node_lst;

typedef struct node_addr_port_s {
	char            *addr;
	char            *tls_name;
	int             port;
} node_addr_port;

// Config option in case the configuration value is changed
typedef struct xdr_new_config_s {
	bool	skip_outstanding;
} xdr_new_config;

// Config option which is maintained both by the server and the XDR module
typedef struct xdr_config_s {

	bool		xdr_section_configured;
	bool		xdr_global_enabled;

	// Ring buffer configuration
	char		*xdr_digestlog_path;
	uint64_t	xdr_digestlog_file_size;

	uint32_t	xdr_info_port;
	uint32_t	xdr_max_ship_throughput;
	uint32_t	xdr_max_ship_bandwidth;
	uint32_t	xdr_min_dlog_free_pct;
	uint32_t	xdr_hotkey_time_ms;
	uint32_t	xdr_read_threads;
	uint32_t	xdr_write_timeout;
	uint32_t	xdr_client_threads;
	uint32_t	xdr_forward_xdrwrites;
	uint32_t	xdr_internal_shipping_delay;
	uint32_t	xdr_info_request_timeout_ms;
	uint32_t	xdr_compression_threshold;
	uint32_t	xdr_digestlog_iowait_ms;

	bool		xdr_shipping_enabled;
	bool		xdr_delete_shipping_enabled;
	bool		xdr_nsup_deletes_enabled;
	bool		xdr_ship_bins;
	bool		xdr_handle_failednode;
	bool		xdr_handle_linkdown;

	// Internal
	bool		xdr_conf_change_flag;
	xdr_new_config xdr_new_cfg;
} xdr_config;

typedef struct xdr_security_config_s {
	char		*sec_config_file;
	char		*username;
	char		*password;
} xdr_security_config;

typedef struct dc_config_opt_s {
	 char					*dc_name;
	 int					dc_id;
	 cf_vector				dc_node_v;
	 cf_vector				dc_addr_map_v;
	 uint32_t				dc_connections;
	 uint32_t				dc_connections_idle_ms;
	 xdr_security_config	dc_security_cfg;
	 bool					dc_use_alternate_services;
	 char					*tls_our_name;
	 cf_tls_spec			*tls_spec;
} dc_config_opt;

//==========================================================
// Public API.
//

void xdr_config_defaults();
bool xdr_read_security_configfile(xdr_security_config* sc);

extern xdr_config g_xcfg;
extern int g_dc_count;
extern dc_config_opt g_dc_xcfg_opt[DC_MAX_NUM];
