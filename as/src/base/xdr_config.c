/*
 * xdr_config.c
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

/*
 *  Configuration file-related routines shared between the server and XDR.
 */

#include <string.h>

#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/xdr_config.h"

void
xdr_config_defaults()
{
	xdr_config *c = &g_xcfg;
	memset(c, 0, sizeof(xdr_config));

	c->xdr_section_configured = false;	// Indicates if XDR is configured or not
	c->xdr_global_enabled = false;		// This config option overrides the enable-xdr setting of the namespace(s)
	c->xdr_enable_change_notification = false;	// Static config which spawns http machinery and also checks feature key
	c->xdr_digestlog_path = NULL;		// Path where the digest information is written to the disk
	c->xdr_info_port = 0;
	c->xdr_max_ship_throughput = 0;		// XDR TPS limit
	c->xdr_max_ship_bandwidth = 0;		// XDR bandwidth limit
	c->xdr_min_dlog_free_pct = 0;		// Namespace writes are stopped below this limit
	c->xdr_hotkey_time_ms = 100;		// Expiration time for the de-duplication cache
	c->xdr_read_threads = 4;			// Number of XDR read threads.
	c->xdr_write_timeout = 10000;		// Timeout for each element that is shipped.
	c->xdr_client_threads = 3;			// Number of async client threads (event loops)
	c->xdr_forward_xdrwrites = false;	// If the writes due to xdr should be forwarded
	c->xdr_nsup_deletes_enabled = false;// Shall XDR ship deletes of evictions or expiration
	c->xdr_internal_shipping_delay = 0;	// Default sleep between shipping each batch is 0 seconds
	c->xdr_conf_change_flag = false;
	c->xdr_shipping_enabled = true;
	c->xdr_delete_shipping_enabled = true;
	c->xdr_ship_bins = false;
	c->xdr_info_request_timeout_ms = 10000;
	c->xdr_compression_threshold = 0; 	// 0 disables compressed shipping, > 0 specifies minimum request size for compression
	c->xdr_handle_failednode = true;
	c->xdr_handle_linkdown = true;
	c->xdr_digestlog_iowait_ms = 500;
}

void
xdr_config_dest_defaults(xdr_dest_config *dest_cfg)
{
	// Assume its aerospike dest type unless otherwise specified
	dest_cfg->dc_type = XDR_CFG_DEST_AEROSPIKE;

	// Common
	dest_cfg->dc_security_cfg.sec_config_file = NULL;
	dest_cfg->dc_tls_spec_name = NULL;
	dest_cfg->dc_tls_spec = NULL;
	dest_cfg->dc_ship_bins = true;

	// Aerospike destination
	cf_vector_pointer_init(&dest_cfg->aero.dc_nodes, 10, 0);
	xdr_dest_aero_config *aero_conf = &dest_cfg->aero;
	aero_conf->dc_use_alternate_services = false;
	aero_conf->dc_connections = 64;
	aero_conf->dc_connections_idle_ms = 55000;
	cf_vector_pointer_init(&aero_conf->dc_addr_map_v, 10, 0);

	// HTTP destination
	xdr_dest_http_config *http_conf = &dest_cfg->http;
	http_conf->verbose = false;
	http_conf->version_str = cf_strdup(XDR_CFG_HTTP_VERSION_2);
	cf_vector_init(&http_conf->urls, sizeof(void *), 10, 0); // pointer vector
}

xdr_config		g_xcfg = { 0 };
xdr_dest_config	g_dest_xcfg_opt[DC_MAX_NUM];
int				g_dc_count = 0;
