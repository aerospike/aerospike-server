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
#include "sindex/sindex_manager.h"

//==========================================================
// Forward declarations.
//

struct as_exp_s;
struct as_index_ref_s;
struct as_namespace_s;
struct as_sindex_s;
struct as_storage_rd_s;
struct si_btree_s;

//==========================================================
// Typedefs & constants.
//

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
#define SINDEX_SMD_KEY_MAX_SZ                                                  \
	(AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + AS_BIN_NAME_MAX_SZ + 2 + 2 +  \
			CTX_B64_MAX_SZ + EXP_B64_MAX_SZ)
#define SET_INDEX_SMD_KEY_MAX_SZ (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + 3)

typedef enum { AS_SINDEX_OP_DELETE = 0, AS_SINDEX_OP_INSERT = 1 } as_sindex_op;

typedef struct as_sindex_bin_s {
	struct as_sindex_s* si;
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
void as_sindex_put_all_rd(struct as_namespace_s* ns, struct as_storage_rd_s* rd,
		struct as_index_ref_s* r_ref);
void as_sindex_put_rd(struct as_sindex_s* si, struct as_storage_rd_s* rd,
		struct as_index_ref_s* r_ref);

// Modify sindexes from writes/deletes.
uint32_t as_sindex_populate_sbin_si(struct as_sindex_s* si, const as_bin* b,
		as_sindex_bin* sbins, as_sindex_op op);
uint32_t as_sindex_populate_sbins(struct as_namespace_s* ns, uint16_t set_id,
		const as_bin* b, as_sindex_bin* sbins, as_sindex_op op);
void as_sindex_update_by_sbin(as_sindex_bin* sbins, uint32_t n_sbins,
		cf_arenax_handle r_h);
void as_sindex_sbin_free_all(as_sindex_bin* sbins, uint32_t n_sbins);

// Lookup.
struct as_sindex_s* as_sindex_lookup_by_defn(const struct as_namespace_s* ns,
		uint16_t set_id, const char* bin_name, as_particle_type ktype,
		as_sindex_type itype, const uint8_t* exp_buf, uint32_t exp_buf_sz,
		const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
struct as_sindex_s* as_sindex_lookup_by_iname(const struct as_namespace_s* ns,
		const char* iname);

// Info & stats.
as_particle_type as_sindex_ktype_from_string(const char* ktype_str);
as_sindex_type as_sindex_itype_from_string(const char* itype_str);
void as_sindex_list_str(const struct as_namespace_s* ns, bool b64,
		cf_dyn_buf* db);
bool as_sindex_stats_str(const struct as_sindex_s* si, cf_dyn_buf* db);
int32_t as_sindex_cdt_ctx_b64_decode(const char* ctx_b64, uint32_t ctx_b64_len,
		uint8_t** buf_r);
int32_t as_sindex_exp_b64_decode(const char* exp_b64, uint32_t exp_b64_len,
		uint8_t** buf_r);
bool as_sindex_validate_exp(const char* exp_b64, uint8_t* expected_type_r,
		cf_dyn_buf* db);
bool as_sindex_validate_exp_type(const char* iname, as_sindex_type itype,
		as_particle_type ktype, uint8_t exp_type, cf_dyn_buf* db);

// SMD helpers
void smd_sindex_create(as_sindex_def* def, bool startup);
void smd_sindex_drop(as_sindex_def* def);

static inline uint32_t
as_sindex_n_sindexes(const as_namespace* ns)
{
	return ns->n_sindexes;
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
as_sindex_reserve(struct as_sindex_s* si)
{
	cf_rc_reserve(si);
}

static inline void
as_sindex_release(struct as_sindex_s* si)
{
	if (cf_rc_release(si) == 0) {
		as_sindex_populate_destroy(si);
	}
}

static inline void
as_sindex_job_reserve(struct as_sindex_s* si)
{
	as_incr_uint32(&si->n_jobs);
}

static inline void
as_sindex_job_release(struct as_sindex_s* si)
{
	uint32_t rc = as_aaf_uint32_rls(&si->n_jobs, -1);

	cf_assert(rc != (uint32_t)-1, AS_SINDEX, "reference count underflow");
}

//==========================================================
// Private API - for enterprise separation only.
//

void add_to_sindexes(struct as_sindex_s* si);
void drop_from_sindexes(struct as_sindex_s* si);
void as_sindex_resume_check(void);
