/*
 * service.h
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

#pragma once

//==========================================================
// Includes.
//

#include "socket.h"
#include "tls.h"


//==========================================================
// Forward declarations.
//

struct as_file_handle_s;


//==========================================================
// Typedefs & constants.
//

typedef struct as_service_endpoint_s {
	cf_addr_list addrs;
	cf_ip_port port;
} as_service_endpoint;

typedef struct as_service_access_s {
	as_service_endpoint service;
	as_service_endpoint alt_service;
	as_service_endpoint tls_service;
	as_service_endpoint alt_tls_service;
} as_service_access;

#define MAX_SERVICE_THREADS 256
#define MIN_PROTO_FD_MAX 1024


//==========================================================
// Globals.
//

extern as_service_access g_access;
extern cf_serv_cfg g_service_bind;
extern cf_tls_info* g_service_tls;


//==========================================================
// Public API.
//

void as_service_start(void);
void as_service_rearm(struct as_file_handle_s* fd_h);
