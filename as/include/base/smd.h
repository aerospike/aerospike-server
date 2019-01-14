/*
 * smd.h
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

// These values are used on the wire - don't change them.
typedef enum {
	AS_SMD_MODULE_EVICT,
	AS_SMD_MODULE_ROSTER,
	AS_SMD_MODULE_SECURITY,
	AS_SMD_MODULE_SINDEX,
	AS_SMD_MODULE_TRUNCATE,
	AS_SMD_MODULE_UDF,

	AS_SMD_NUM_MODULES
} as_smd_id;

typedef struct as_smd_item_s {
	char* key;
	char* value; // NULL means delete
	uint64_t timestamp;
	uint32_t generation;
} as_smd_item;

typedef enum {
	AS_SMD_ACCEPT_OPT_START,
	AS_SMD_ACCEPT_OPT_SET
} as_smd_accept_type;

typedef void (*as_smd_accept_fn)(const cf_vector* items, as_smd_accept_type accept_type);
// Return false to choose item0, true to choose item1.
typedef bool (*as_smd_conflict_fn)(const as_smd_item* item0, const as_smd_item* item1);
typedef void (*as_smd_get_all_fn)(const cf_vector* items, void* udata);
typedef void (*as_smd_set_fn)(bool result, void* udata);


//==========================================================
// Public API.
//

void as_smd_module_load(as_smd_id id, as_smd_accept_fn accept_cb, as_smd_conflict_fn conflict_cb, const cf_vector* default_items);
void as_smd_start(void);
// timeout 0 is default timeout in msec.
void as_smd_set(as_smd_id id, const char* key, const char* value, as_smd_set_fn set_cb, void* udata, uint64_t timeout);
bool as_smd_set_blocking(as_smd_id id, const char* key, const char* value, uint64_t timeout);
void as_smd_get_all(as_smd_id id, as_smd_get_all_fn cb, void* udata);
void as_smd_get_info(cf_dyn_buf* db);

static inline void
as_smd_set_and_forget(as_smd_id id, const char* key, const char* value)
{
	as_smd_set(id, key, value, NULL, NULL, 0);
}

static inline void
as_smd_delete(as_smd_id id, const char* key, as_smd_set_fn set_cb, void* udata, uint64_t timeout)
{
	as_smd_set(id, key, NULL, set_cb, udata, timeout);
}

static inline bool
as_smd_delete_blocking(as_smd_id id, const char* key, uint64_t timeout)
{
	return as_smd_set_blocking(id, key, NULL, timeout);
}

static inline void
as_smd_delete_and_forget(as_smd_id id, const char* key)
{
	as_smd_set(id, key, NULL, NULL, NULL, 0);
}
