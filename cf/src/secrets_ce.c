/*
 * secrets_ce.c
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

//==========================================================
// Includes.
//

#include "secrets.h"

#include <stddef.h>
#include <stdint.h>

#include "log.h"


//==========================================================
// Globals.
//

cf_secrets_config g_secrets_cfg = { 0 };


//==========================================================
// Public API.
//

uint8_t*
cf_secrets_fetch_bytes(const char* path, size_t* size_r)
{
	cf_crash(CF_SECRETS, "unreachable function for CE");
	return NULL;
}
