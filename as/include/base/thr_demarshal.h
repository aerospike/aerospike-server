/*
 * thr_demarshal.h
 *
 * Copyright (C) 2015 Aerospike, Inc.
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

#include "socket.h"
#include "tls.h"
#include "base/cfg.h"
#include "base/transaction.h"

typedef struct as_info_endpoint_s {
	cf_addr_list addrs;
	cf_ip_port port;
} as_info_endpoint;

typedef struct as_info_access_s {
	as_info_endpoint service;
	as_info_endpoint alt_service;
	as_info_endpoint tls_service;
	as_info_endpoint alt_tls_service;
} as_info_access;

extern as_info_access g_access;
extern cf_serv_cfg g_service_bind;
extern cf_tls_info *g_service_tls;

void thr_demarshal_rearm(as_file_handle *fd_h);
