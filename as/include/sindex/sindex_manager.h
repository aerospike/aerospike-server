/*
 * sindex_manager.h
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_ll.h"
#include "vector.h"
#include "base/smd.h"
#include "base/datamodel.h"


//==========================================================
// Typedefs & constants.
//

#define INAME_MAX_SZ 64

typedef enum {
	AS_SINDEX_ITYPE_DEFAULT   = 0,
	AS_SINDEX_ITYPE_LIST      = 1,
	AS_SINDEX_ITYPE_MAPKEYS   = 2,
	AS_SINDEX_ITYPE_MAPVALUES = 3,
	AS_SINDEX_ITYPE_SET       = 4,

	AS_SINDEX_N_ITYPES        = 5
} as_sindex_type;

static const char* const as_sindex_type_names[] = {
	"default",
	"list",
	"mapkeys",
	"mapvalues",
	"set"
};

ARRAY_ASSERT(as_sindex_type_names, AS_SINDEX_N_ITYPES);

typedef struct as_sindex_s {
	struct as_namespace_s* ns;
	char iname[INAME_MAX_SZ];

	char set_name[AS_SET_NAME_MAX_SIZE];
	uint16_t set_id;

	char bin_name[AS_BIN_NAME_MAX_SZ];

	as_particle_type ktype;
	as_sindex_type itype;

	char* ctx_b64;
	uint8_t* ctx_buf;
	uint32_t ctx_buf_sz;

	struct as_exp_s* exp; // built exp points to exp_buf
	uint8_t* exp_buf;
	uint32_t exp_buf_sz;
	char* exp_b64;
	cf_vector* exp_bins_info;

	uint32_t id;

	bool readable; // false while building sindex
	bool dropped;
	bool error;
	uint32_t n_jobs;

	uint64_t keys_per_bval;
	uint64_t keys_per_rec;
	uint64_t load_time;
	uint32_t populate_pct;
	uint64_t n_gc_cleaned;

	uint32_t n_btrees;
	struct si_btree_s** btrees;
} as_sindex;

typedef struct as_sindex_def_s {
	as_namespace* ns;
	char iname[INAME_MAX_SZ];
	char set_name[AS_SET_NAME_MAX_SIZE];
	char bin_name[AS_BIN_NAME_MAX_SZ];
	as_particle_type ktype;
	as_sindex_type itype;
	char* ctx_b64;
	char* exp_b64;
} as_sindex_def;

typedef struct defn_hash_key_s {
	uint16_t set_id;
	char bin_name[AS_BIN_NAME_MAX_SZ]; // will be hash of exp for SI on exp
} defn_hash_key;

typedef struct defn_hash_ele_s {
	cf_ll_element ele;
	as_sindex* si;
} defn_hash_ele;

typedef struct find_sindex_key_udata_s {
	const char* ns_name;
	const char* index_name;
	const char* smd_key;
	char* found_key;
	uint32_t n_name_matches;
	uint32_t n_sindexes;
	uint32_t n_setindexes;
	bool has_smd_key;
} find_sindex_key_udata;


//==========================================================
// Globals.
//

extern pthread_rwlock_t g_sindex_rwlock;

#define SINDEX_GRLOCK() pthread_rwlock_rdlock(&g_sindex_rwlock)
#define SINDEX_GRUNLOCK() pthread_rwlock_unlock(&g_sindex_rwlock)
#define SINDEX_GUNLOCK() pthread_rwlock_unlock(&g_sindex_rwlock)  // Alias for GRUNLOCK
#define SINDEX_GWLOCK() pthread_rwlock_wrlock(&g_sindex_rwlock)
#define SINDEX_GWUNLOCK() pthread_rwlock_unlock(&g_sindex_rwlock)


//==========================================================
// Forward declarations.
//

struct as_exp_s;
struct as_info_cmd_args_s;
struct as_namespace_s;
struct cf_vector_s;
struct si_btree_s;
struct cf_dyn_buf_s;


//==========================================================
// Public API.
//

void as_sindex_manager_init(void);
void as_sindex_manager_start(void);

void as_sindex_manager_sindex_create(struct as_info_cmd_args_s* args);
void as_sindex_manager_sindex_delete(struct as_info_cmd_args_s* args);
void as_sindex_manager_sindex_exists(struct as_info_cmd_args_s* args);
bool as_sindex_manager_stats_str(struct as_namespace_s* ns, char* iname, struct cf_dyn_buf_s* db);
void as_sindex_manager_list_str(struct as_info_cmd_args_s* args);

void as_sindex_manager_handle_smd_item(const as_smd_item* item, as_smd_accept_type accept_type);
void as_sindex_manager_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);
void as_sindex_manager_smd_create(as_sindex_def* def, bool startup);
void as_sindex_manager_smd_drop(as_sindex_def* def);
bool as_sindex_manager_smd_item_to_def(const char* smd_key, const char* smd_value, as_sindex_def* def);

// TODO:- (daud) carve out sindex_util.c.
void as_rename_sindex(as_sindex* si, const char* iname);
void as_add_sindex(as_sindex* si);
void as_delete_sindex(as_sindex* si);

as_sindex* as_si_by_iname(const struct as_namespace_s* ns, const char* iname);
as_sindex* as_si_by_defn(const struct as_namespace_s* ns, uint16_t set_id, const char* bin_name, as_particle_type ktype, as_sindex_type itype, const uint8_t* exp_buf, uint32_t exp_buf_sz, const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
cf_ll* as_si_list_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name, const uint8_t* exp_buf, uint32_t exp_buf_sz);

