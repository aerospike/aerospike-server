/*
 * vault.h
 *
 * Copyright (C) 2020 Aerospike, Inc.
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
#include <string.h>


//==========================================================
// Typedefs & constants.
//

#define CF_VAULT_PATH_PREFIX "vault:"

typedef struct cf_vault_config_s {
	const char* ca;
	const char* namespace;
	const char* path;
	char* token_file;
	const char* url;
} cf_vault_config;


//==========================================================
// Public API.
//

bool cf_vault_is_configured(void);
uint8_t* cf_vault_fetch_bytes(const char* path, size_t* size_r);
bool cf_vault_update_token(const char* path);

static inline bool
cf_vault_is_vault_path(const char* path)
{
	return strncmp(path, CF_VAULT_PATH_PREFIX,
			sizeof(CF_VAULT_PATH_PREFIX) - 1) == 0;
}

extern cf_vault_config g_vault_cfg;
