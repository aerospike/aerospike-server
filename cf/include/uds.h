/*
 * uds.h
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

typedef struct cf_uds_s {
	int32_t fd;
} cf_uds;


//==========================================================
// Public API.
//

bool cf_uds_connect(const char* path, cf_uds* sock);
int32_t cf_uds_send_all(cf_uds* sock, const void* buf, size_t size, int32_t flags);
int32_t cf_uds_recv_all(cf_uds* sock, void* buf, size_t size, int32_t flags);
