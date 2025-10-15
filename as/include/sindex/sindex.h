/*
 * sindex.h
 *
 * Copyright (C) 2022 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"

#include "arenax.h"
#include "dynbuf.h"
#include "shash.h"
#include "vector.h"

#include "base/datamodel.h"
#include "sindex/populate.h"
#include "sindex/sindex_arena.h"


//==========================================================
// Forward declarations.
//

struct as_exp_s;
struct as_index_ref_s;
struct as_namespace_s;
struct as_storage_rd_s;
struct si_btree_s;


//==========================================================
// Typedefs & constants.
//

#define INAME_MAX_SZ 64

// Sanity check limits.
#define MAX_STRING_KSIZE 2048
#define MAX_BLOB_KSIZE 2048
#define MAX_GEOJSON_KSIZE (1024 * 1024)

// Info command parsing buffer sizes.
#define INDEXTYPE_MAX_SZ 10 // (default/list/mapkeys/mapvalues)
#define KTYPE_MAX_SZ (11 + 1) // (string/blob/numeric/geo2dsphere)
#define INDEXDATA_MAX_SZ (AS_BIN_NAME_MAX_SZ + KTYPE_MAX_SZ) // bin-name,type
#define CTX_B64_MAX_SZ 2048
#define EXP_B64_MAX_SZ (16 * 1024)
#define SINDEX_SMD_KEY_MAX_SZ (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + AS_BIN_NAME_MAX_SZ + 2 + 2 + CTX_B64_MAX_SZ + EXP_B64_MAX_SZ)

typedef enum {
	AS_SINDEX_OP_DELETE = 0,
	AS_SINDEX_OP_INSERT = 1
} as_sindex_op;

typedef enum {
	AS_SINDEX_ITYPE_DEFAULT   = 0,
	AS_SINDEX_ITYPE_LIST      = 1,
	AS_SINDEX_ITYPE_MAPKEYS   = 2,
	AS_SINDEX_ITYPE_MAPVALUES = 3,

	AS_SINDEX_N_ITYPES        = 4
} as_sindex_type;

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
	cf_vector* exp_bin_names;

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

typedef struct as_sindex_bin_s {
	as_sindex* si;
	as_sindex_op op;

	uint32_t n_values;
	int64_t val; // optimize for non-CDT use case which needs only one value
	int64_t* values;
	uint32_t capacity;
} as_sindex_bin;


//==========================================================
// Public API.
//

// Startup.
void as_sindex_init(void);
void as_sindex_resume(void);
void as_sindex_load(void);
void as_sindex_start(void);
void as_sindex_shutdown(struct as_namespace_s* ns);

// Populate sindexes.
void as_sindex_put_all_rd(struct as_namespace_s* ns, struct as_storage_rd_s* rd, struct as_index_ref_s* r_ref);
void as_sindex_put_rd(as_sindex* si, struct as_storage_rd_s* rd, struct as_index_ref_s* r_ref);

// Modify sindexes from writes/deletes.
uint32_t as_sindex_populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbins, as_sindex_op op);
uint32_t as_sindex_populate_sbins(struct as_namespace_s* ns, uint16_t set_id, const as_bin* b, as_sindex_bin* sbins, as_sindex_op op);
void as_sindex_update_by_sbin(as_sindex_bin* sbins, uint32_t n_sbins, cf_arenax_handle r_h);
void as_sindex_sbin_free_all(as_sindex_bin* sbins, uint32_t n_sbins);

// Lookup.
as_sindex* as_sindex_lookup_by_defn(const struct as_namespace_s* ns, uint16_t set_id, const char* bin_name, as_particle_type ktype, as_sindex_type itype, const uint8_t* exp_buf, uint32_t exp_buf_sz, const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
as_sindex* as_sindex_lookup_by_iname(const struct as_namespace_s* ns, const char* iname);

// Info & stats.
as_particle_type as_sindex_ktype_from_string(const char* ktype_str);
as_sindex_type as_sindex_itype_from_string(const char* itype_str);
bool as_sindex_exists(const struct as_namespace_s* ns, const char* iname);
bool as_sindex_stats_str(struct as_namespace_s* ns, char* iname, cf_dyn_buf* db);
void as_sindex_list_str(const struct as_namespace_s* ns, bool b64, cf_dyn_buf* db);
void as_sindex_build_smd_key(const char* ns_name, const char* set_name, const char* bin_name, const char* cdt_ctx, const char* exp, as_sindex_type itype, as_particle_type ktype, char* smd_key);
int32_t as_sindex_cdt_ctx_b64_decode(const char* ctx_b64, uint32_t ctx_b64_len, uint8_t** buf_r);
int32_t as_sindex_exp_b64_decode(const char* exp_b64, uint32_t exp_b64_len, uint8_t** buf_r);
bool as_sindex_validate_exp(const char* exp_b64, uint8_t* expected_type_r, cf_dyn_buf* db);
bool as_sindex_validate_exp_type(const char* iname, as_sindex_type itype, as_particle_type ktype, uint8_t exp_type, cf_dyn_buf* db);

static inline uint32_t
as_sindex_n_sindexes(const as_namespace* ns)
{
	return cf_shash_get_size(ns->sindex_iname_hash);
}

static inline int64_t
as_sindex_string_to_bval(const char* s, size_t len)
{
	return (int64_t)cf_wyhash64((const void*)s, len);
}

static inline int64_t
as_sindex_blob_to_bval(const uint8_t* blob, size_t sz)
{
	return (int64_t)cf_wyhash64((const void*)blob, sz);
}

static inline uint64_t
as_sindex_used_bytes(const as_namespace* ns)
{
	return ns->si_arena->n_used_eles * ns->si_arena->ele_sz;
}

static inline void
as_sindex_reserve(as_sindex* si)
{
	cf_rc_reserve(si);
}

static inline void
as_sindex_release(as_sindex* si)
{
	if (cf_rc_release(si) == 0) {
		as_sindex_populate_destroy(si);
	}
}

static inline void
as_sindex_job_reserve(as_sindex* si)
{
	as_incr_uint32(&si->n_jobs);
}

static inline void
as_sindex_job_release(as_sindex* si)
{
	uint32_t rc = as_aaf_uint32_rls(&si->n_jobs, -1);

	cf_assert(rc != (uint32_t)-1, AS_SINDEX, "reference count underflow");
}

// Lifecycle lock.
extern pthread_rwlock_t g_sindex_rwlock;

#define SINDEX_GRLOCK() pthread_rwlock_rdlock(&g_sindex_rwlock)
#define SINDEX_GWLOCK() pthread_rwlock_wrlock(&g_sindex_rwlock)
#define SINDEX_GRUNLOCK() pthread_rwlock_unlock(&g_sindex_rwlock)
#define SINDEX_GWUNLOCK() pthread_rwlock_unlock(&g_sindex_rwlock)


//==========================================================
// Private API - for enterprise separation only.
//

void add_to_sindexes(as_sindex* si);
void drop_from_sindexes(struct as_sindex_s* si);
void as_sindex_resume_check(void);
