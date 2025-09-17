/*
 * datamodel.h
 *
 * Copyright (C) 2008-2022 Aerospike, Inc.
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

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "aerospike/as_atomic.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "dynbuf.h"
#include "hist.h"
#include "linear_hist.h"
#include "log.h" // TODO - for development only?
#include "msg.h"
#include "msgpack_in.h"
#include "node.h"
#include "shash.h"
#include "vector.h"
#include "vmapx.h"
#include "xmem.h"

#include "base/cfg.h"
#include "base/proto.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "fabric/hb.h"
#include "fabric/partition.h"
#include "storage/flat.h"
#include "storage/storage.h"


#define OBJ_SIZE_HIST_NUM_BUCKETS 1024
#define TTL_HIST_NUM_BUCKETS 100

#define MAX_ALLOWED_TTL (3600 * 24 * 365 * 10) // 10 years
#define TTL_USE_DEFAULT 0
#define TTL_NEVER_EXPIRE ((uint32_t)-1) // config for set (but not namespace)
#define TTL_DONT_UPDATE  ((uint32_t)-2) // not config - in client msg only

// [0-1] for partition-id
// [1-4] for tree sprigs and locks
// [5-7] unused
// [8-11] for SSD device hash
#define DIGEST_STORAGE_BASE_BYTE	8
// [12-15] for rw_request hash
#define DIGEST_HASH_BASE_BYTE		12
// [12-19] for XDR connector affinity and [12-15] for tracing
#define DIGEST_XDR_BASE_BYTE		12


/* Forward declarations */
typedef struct as_index_s as_record;

struct as_exp_ctx_s;
struct as_index_ref_s;
struct as_index_tree_s;
struct as_msg_s;
struct as_msg_field_s;
struct as_msg_op_s;
struct as_namespace_s;
struct as_record_version_s;
struct as_set_s;
struct as_sindex_s;
struct as_sindex_arena_s;
struct as_sindex_config_s;
struct as_storage_rd_s;
struct cdt_payload_s;
struct index_metadata_s;
struct iops_expop_s;
struct rollback_alloc_s;

#define AS_ID_NAMESPACE_SZ 32

#define AS_BIN_NAME_MAX_SZ 16 // changing this breaks warm restart
#define RECORD_MAX_BINS 0X7FFF

#define MAX_N_SINDEXES 256

#define SI_ARENA_ELE_SZ 4096 // b-tree node size - may come from config someday

/*
 * Compare two 16-bit generation counts, allowing wrap-arounds.
 * Works correctly, if:
 *
 *   - rhs is ahead of lhs, but rhs isn't ahead more than 32,768.
 *   - lhs is ahead of rhs, but lhs isn't ahead more than 32,767.
 */

static inline bool
as_gen_less_than(uint16_t lhs, uint16_t rhs)
{
	return (uint16_t)(lhs - rhs) >= 32768;
}


// Particle and bin types.

typedef enum {
	AS_PARTICLE_TYPE_NULL = 0,
	AS_PARTICLE_TYPE_INTEGER = 1,
	AS_PARTICLE_TYPE_FLOAT = 2,
	AS_PARTICLE_TYPE_STRING = 3,
	AS_PARTICLE_TYPE_BLOB = 4,
	AS_PARTICLE_TYPE_JAVA_BLOB = 7,
	AS_PARTICLE_TYPE_CSHARP_BLOB = 8,
	AS_PARTICLE_TYPE_PYTHON_BLOB = 9,
	AS_PARTICLE_TYPE_RUBY_BLOB = 10,
	AS_PARTICLE_TYPE_PHP_BLOB = 11,
	AS_PARTICLE_TYPE_ERLANG_BLOB = 12,
	AS_PARTICLE_TYPE_VECTOR = 16,
	AS_PARTICLE_TYPE_BOOL = 17,
	AS_PARTICLE_TYPE_HLL = 18,
	AS_PARTICLE_TYPE_MAP = 19,
	AS_PARTICLE_TYPE_LIST = 20,
	AS_PARTICLE_TYPE_GEOJSON = 23,
	AS_PARTICLE_TYPE_MAX = 24,
	AS_PARTICLE_TYPE_BAD = AS_PARTICLE_TYPE_MAX
} as_particle_type;

typedef struct as_particle_s {
	uint8_t type; // type for non-embedded particles
	uint8_t data[0];
} __attribute__ ((__packed__)) as_particle;

// Constants used for the as_bin state value (4 bits, 16 values).
#define AS_BIN_STATE_UNUSED			0
#define AS_BIN_STATE_INUSE_INTEGER	1
#define AS_BIN_STATE_TOMBSTONE		2 // enterprise only
#define AS_BIN_STATE_INUSE_OTHER	3
#define AS_BIN_STATE_INUSE_FLOAT	4
#define AS_BIN_STATE_INUSE_BOOL		5

typedef struct as_bin_s {
	uint8_t : 4;
	uint8_t state: 4;		// see AS_BIN_STATE_...
	as_particle* particle;	// for embedded particle this is value, not pointer
	char name[AS_BIN_NAME_MAX_SZ];

	uint8_t xdr_write: 1;	// enterprise only
	uint8_t unused_flags: 7;
	uint64_t lut: 40;
	uint8_t src_id;
} __attribute__ ((__packed__)) as_bin;

/* Particle function declarations */

static inline bool
is_embedded_particle_type(as_particle_type type)
{
	return type == AS_PARTICLE_TYPE_INTEGER || type == AS_PARTICLE_TYPE_FLOAT ||
			type == AS_PARTICLE_TYPE_BOOL;
}

as_particle_type as_particle_type_from_asval(const as_val *val);
as_particle_type as_particle_type_from_msgpack(const uint8_t *packed, uint32_t packed_size);
const char* as_particle_type_str(as_particle_type type);

uint32_t as_particle_size_from_asval(const as_val *val);

uint32_t as_particle_asval_client_value_size(const as_val *val);
uint32_t as_particle_asval_to_client(const as_val *val, struct as_msg_op_s *op);

const uint8_t *as_particle_skip_flat(const uint8_t *flat, const uint8_t *end);

// as_bin particle function declarations

void as_bin_particle_destroy(as_bin *b);

// wire:
int as_bin_particle_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, const struct as_msg_op_s *op);
int as_bin_particle_from_client(as_bin *b, cf_ll_buf *particles_llb, const struct as_msg_op_s *op);
uint32_t as_bin_particle_client_value_size(const as_bin *b);
uint32_t as_bin_particle_to_client(const as_bin *b, struct as_msg_op_s *op);

// Different for blob bitwise operations - we don't use the normal APIs and
// particle table functions.
int as_bin_bits_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, struct as_msg_op_s *op);
int as_bin_bits_read_from_client(const as_bin *b, struct as_msg_op_s *op, as_bin *result);

// Different for HLL operations - we don't use the normal APIs and particle
// table functions.
int as_bin_hll_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, struct as_msg_op_s *op, as_bin *rb);
int as_bin_hll_read_from_client(const as_bin *b, struct as_msg_op_s *op, as_bin *rb);

// Different for CDTs - the operations may return results, so we don't use the
// normal APIs and particle table functions.
int as_bin_cdt_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, struct as_msg_op_s *op, as_bin *result);
int as_bin_cdt_read_from_client(const as_bin *b, struct as_msg_op_s *op, as_bin *result);

// as_val:
void as_bin_particle_from_asval(as_bin *b, uint8_t* stack, const as_val *val);
as_val *as_bin_particle_to_asval(const as_bin *b);

// Different for expression operations - we don't use the normal APIs and
// particle table functions.
int as_bin_exp_modify_from_client(const struct as_exp_ctx_s* ctx, as_bin *b, cf_ll_buf *particles_llb, struct as_msg_op_s *op, const struct iops_expop_s* expop);
int as_bin_exp_read_from_client(const struct as_exp_ctx_s* ctx, struct as_msg_op_s *op, as_bin *rb);

// msgpack:
int32_t as_particle_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
bool as_bin_particle_from_msgpack(as_bin *b, const uint8_t *packed, uint32_t packed_size, void *mem);

// flat:
const uint8_t *as_bin_particle_from_flat(as_bin *b, const uint8_t *flat, const uint8_t *end);
uint32_t as_bin_particle_flat_size(as_bin *b);
uint32_t as_bin_particle_to_flat(const as_bin *b, uint8_t *flat);

// odd as_bin particle functions for specific particle types

// integer:
int64_t as_bin_particle_integer_value(const as_bin *b);
void as_bin_particle_integer_set(as_bin *b, int64_t i);

// float:
double as_bin_particle_float_value(const as_bin *b);

// bool:
bool as_bin_particle_bool_value(const as_bin *b);

// string:
uint32_t as_bin_particle_string_ptr(const as_bin *b, char **p_value);

// blob:
uint32_t as_bin_particle_blob_ptr(const as_bin *b, uint8_t **p_value);
int as_bin_bits_modify_tr(as_bin *b, const struct as_msg_op_s *msg_op, cf_ll_buf *particles_llb);
int as_bin_bits_read_tr(const as_bin *b, const struct as_msg_op_s *msg_op, as_bin *result);
int as_bin_bits_modify_exp(as_bin *b, msgpack_in_vec* mv);
int as_bin_bits_read_exp(const as_bin *b, msgpack_in_vec* mv, as_bin *rb);
const char* as_bits_op_name(uint32_t op_code, bool is_modify);

// HLL:
int as_bin_hll_modify_tr(as_bin *b, const struct as_msg_op_s *msg_op, cf_ll_buf *particles_llb, as_bin* rb);
int as_bin_hll_read_tr(const as_bin *b, const struct as_msg_op_s *msg_op, as_bin *rb);
int as_bin_hll_modify_exp(as_bin *b, msgpack_in_vec* mv, as_bin *rb);
int as_bin_hll_read_exp(const as_bin *b, msgpack_in_vec* mv, as_bin *rb);
const char* as_hll_op_name(uint32_t op_code, bool is_modify);

// geojson:
typedef void *geo_region_t;
#define MAX_REGION_CELLS    256
#define MAX_REGION_LEVELS   30
size_t as_bin_particle_geojson_cellids(const as_bin *b, uint64_t **pp_cells);
bool as_particle_geojson_match(as_particle *p, uint64_t cellid, geo_region_t region, bool is_strict);
bool as_particle_geojson_match_msgpack(msgpack_in* element, uint64_t cellid, geo_region_t region, bool is_strict);
char const *as_geojson_mem_jsonstr(const as_particle *p, size_t *p_jsonsz);
bool as_geojson_match(bool candidate_is_region, uint64_t candidate_cellid, geo_region_t candidate_region, uint64_t query_cellid, geo_region_t query_region, bool is_strict);
uint32_t as_geojson_particle_sz(uint32_t ncells, size_t jlen);
bool as_geojson_parse(const struct as_namespace_s *ns, const char *json, uint32_t jlen, uint64_t *cellid, geo_region_t *region);
bool as_geojson_to_particle(const char *json, uint32_t jlen, as_particle **pp);
bool as_bin_cdt_context_geojson_parse(as_bin *b);

// list:
void as_bin_particle_list_get_packed_val(const as_bin *b, struct cdt_payload_s *packed);

// map:
void as_bin_particle_map_get_packed_val(const as_bin *b, struct cdt_payload_s *packed);

int as_bin_cdt_modify_tr(as_bin *b, const struct as_msg_op_s *op, as_bin *result, cf_ll_buf *particles_llb);
int as_bin_cdt_read_tr(const as_bin *b, const struct as_msg_op_s *op, as_bin *result);
int as_bin_cdt_modify_exp(as_bin *b, msgpack_in_vec* mv, as_bin *result);
int as_bin_cdt_read_exp(const as_bin *b, msgpack_in_vec* mv, as_bin *result);
bool as_bin_cdt_get_by_context(const as_bin *b, const uint8_t* ctx, uint32_t ctx_sz, as_bin *result);

static inline bool
as_bin_name_check(const uint8_t* name, uint32_t len)
{
	if (len >= AS_BIN_NAME_MAX_SZ) {
		return false;
	}

	for (uint32_t i = 0; i < len; i++) {
		if (name[i] == '\0') {
			return false;
		}
	}

	return true;
}

static inline bool
as_bin_has_meta(const as_bin *b)
{
	return b->lut != 0; // cheating - we happen to know this is true for now
}

static inline bool
as_bin_is_used(const as_bin *b)
{
	// TODO - for development only?
	cf_assert(b->state != AS_BIN_STATE_TOMBSTONE, AS_BIN, "unexpected tombstone");

	return b->state != AS_BIN_STATE_UNUSED;
}

static inline bool
as_bin_is_unused(const as_bin *b)
{
	return b->state == AS_BIN_STATE_UNUSED; // note - tombstones are used bins
}

static inline void
as_bin_state_set_from_type(as_bin *b, as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		b->state = AS_BIN_STATE_UNUSED; // should never happen
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		b->state = AS_BIN_STATE_INUSE_INTEGER;
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		b->state = AS_BIN_STATE_INUSE_FLOAT;
		break;
	case AS_PARTICLE_TYPE_BOOL:
		b->state = AS_BIN_STATE_INUSE_BOOL;
		break;
	default:
		b->state = AS_BIN_STATE_INUSE_OTHER;
		break;
	}
}

static inline void
as_bin_set_empty(as_bin *b)
{
	b->state = AS_BIN_STATE_UNUSED;
}

static inline void
as_bin_remove(as_storage_rd *rd, uint32_t i)
{
	rd->n_bins--;

	if (i < rd->n_bins) {
		rd->bins[i] = rd->bins[rd->n_bins];
	}
}

static inline bool
as_bin_is_embedded_particle(const as_bin *b)
{
	return b->state == AS_BIN_STATE_INUSE_INTEGER ||
			b->state == AS_BIN_STATE_INUSE_FLOAT ||
			b->state == AS_BIN_STATE_INUSE_BOOL;
}

static inline bool
as_bin_is_external_particle(const as_bin *b)
{
	return b->state == AS_BIN_STATE_INUSE_OTHER;
}

static inline as_particle *
as_bin_get_particle(as_bin *b)
{
	return as_bin_is_embedded_particle(b) ?
			(as_particle *)&b->particle : b->particle;
}

static inline void
as_bin_destroy_all(as_bin* bins, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin_particle_destroy(&bins[i]);
	}
}

/* Bin function declarations */
uint8_t as_bin_get_particle_type(const as_bin* b);
bool as_bin_particle_is_tombstone(as_particle_type type);
bool as_bin_is_tombstone(const as_bin* b);
bool as_bin_is_live(const as_bin* b);
void as_bin_set_tombstone(as_bin* b);
bool as_bin_empty_if_all_tombstones(struct as_storage_rd_s* rd, bool is_dd);
void as_bin_clear_meta(as_bin* b);
int as_storage_rd_load_bins(struct as_storage_rd_s *rd, as_bin *stack_bins);
as_bin *as_bin_get(struct as_storage_rd_s *rd, const char *name);
as_bin *as_bin_get_w_len(struct as_storage_rd_s *rd, const uint8_t *name, size_t len);
as_bin *as_bin_get_live(struct as_storage_rd_s *rd, const char *name);
as_bin *as_bin_get_live_w_len(struct as_storage_rd_s *rd, const uint8_t *name, size_t len);
as_bin *as_bin_get_or_create(struct as_storage_rd_s *rd, const char *name);
as_bin *as_bin_get_or_create_w_len(struct as_storage_rd_s *rd, const uint8_t *name, size_t len);
void as_bin_delete(struct as_storage_rd_s* rd, const char* name);
void as_bin_delete_w_len(struct as_storage_rd_s* rd, const uint8_t* name, size_t len);
int as_storage_rd_lazy_load_bins(struct as_storage_rd_s *rd, as_bin* stack_bins);


typedef enum {
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF = 0,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION = 1,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME = 2,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_CP = 3
} conflict_resolution_pol;

/* Record function declarations */
uint32_t clock_skew_stop_writes_sec(void);
bool as_record_handle_clock_skew(struct as_namespace_s* ns, uint64_t skew_ms);
uint16_t plain_generation(uint16_t regime_generation, const struct as_namespace_s* ns);
void as_record_set_lut(as_record *r, uint32_t regime, uint64_t now_ms, const struct as_namespace_s* ns);
void as_record_increment_generation(as_record *r, const struct as_namespace_s* ns);
bool as_record_is_binless(const as_record *r);
bool as_record_is_live(const as_record *r);
int as_record_get_create(struct as_index_tree_s *tree, const cf_digest *keyd, struct as_index_ref_s *r_ref, struct as_namespace_s *ns);
int as_record_get(struct as_index_tree_s *tree, const cf_digest *keyd, struct as_index_ref_s *r_ref);
void as_record_rescue(struct as_index_ref_s *r_ref, struct as_namespace_s *ns);

void as_record_destroy(as_record *r, struct as_namespace_s *ns);
void as_record_done(struct as_index_ref_s *r_ref, struct as_namespace_s *ns);

int as_record_fix_setless_tombstone(as_record* r, struct as_namespace_s* ns, const char* set_name, uint32_t len, bool apply_count_limit);

void as_record_drop_stats(as_record* r, struct as_namespace_s* ns);
void as_record_transition_stats(as_record* r, struct as_namespace_s* ns, const as_record* old_r);

void as_record_transition_set_index(struct as_index_tree_s* tree, struct as_index_ref_s* r_ref, struct as_namespace_s* ns, uint16_t n_bins, const as_record* old_r);

void as_record_finalize_key(as_record* r, const uint8_t* key, uint32_t key_size);
int as_record_resolve_conflict(conflict_resolution_pol policy, uint16_t left_gen, uint64_t left_lut, uint16_t right_gen, uint64_t right_lut);

void as_record_advance_void_time(as_record* r, uint32_t record_ttl, uint64_t now, struct as_namespace_s* ns);

static inline int
resolve_last_update_time(uint64_t left, uint64_t right)
{
	return left == right ? 0 : (right > left ? 1 : -1);
}

typedef enum {
	VIA_REPLICATION,
	VIA_MIGRATION,
	VIA_DUPLICATE_RESOLUTION
} record_via;

typedef struct as_remote_record_s {
	record_via via;
	cf_node src;
	as_partition_reservation *rsv;
	cf_digest *keyd;

	uint8_t *pickle;
	size_t pickle_sz;

	uint32_t generation;
	uint32_t void_time;
	uint64_t last_update_time;

	const char *set_name;
	uint32_t set_name_len;

	const uint8_t *key;
	uint32_t key_size;

	uint16_t n_bins;
	as_flat_comp_meta cm;
	uint32_t meta_sz;

	uint32_t regime; // relevant only for enterprise edition
	uint8_t repl_state; // relevant only for enterprise edition
	bool xdr_write; // relevant only for enterprise edition
	bool xdr_tombstone; // relevant only for enterprise edition
	bool xdr_nsup_tombstone; // relevant only for enterprise edition
	bool xdr_bin_cemetery; // relevant only for enterprise edition

	uint64_t mrt_id; // relevant only for enterprise edition
	struct as_record_version_s* mrt_orig_v; // relevant only for enterprise edition
	uint8_t *orig_pickle; // relevant only for enterprise edition
	size_t orig_pickle_sz; // relevant only for enterprise edition
} as_remote_record;

int as_record_replace_if_better(as_remote_record *rr);
int as_record_apply(as_remote_record* rr, struct as_index_ref_s* r_ref, struct as_storage_rd_s* rd);

// For enterprise split only.
int record_resolve_conflict_cp(uint16_t left_gen, uint64_t left_lut, uint16_t right_gen, uint64_t right_lut);
void replace_index_metadata(const as_remote_record *rr, as_record *r);

uint32_t as_record_stored_size(const as_record* r); // TODO - eventually inline

#define as_record_void_time_get() cf_clepoch_seconds()
bool as_record_is_expired(const as_record *r); // TODO - eventually inline

// TODO - split and include MRT logic?
static inline bool
as_record_is_doomed(const as_record *r, struct as_namespace_s *ns)
{
	// WRITES
	// ------
	// Expired
	//                Ordinary write         MRT write
	// r normal       new                    new
	// r orig         -                      -
	// r prov         -                      update (absurd)
	//
	// Truncated
	//                Ordinary write         MRT write
	// r normal       new                    new
	// r orig         -                      -
	// r prov         -                      update

	// READS
	// -----
	// Expired
	//                Ordinary read          MRT read
	// r normal       NF                     NF*
	// r orig         NF                     -
	// r prov         -                      read (absurd)
	//
	// Truncated
	//                Ordinary read          MRT read
	// r normal       NF                     NF*
	// r orig         NF                     -
	// r prov         -                      read
	//
	// * Will return the doomed record's version - if this is GC'd before we
	// read-verify, we will fail the MRT. For now, don't worry about this.

	return as_record_is_expired(r) || as_truncate_record_is_truncated(r, ns);
}

// Quietly trim void-time. (Clock on remote node different?) TODO - best way?
static inline uint32_t
trim_void_time(uint32_t void_time)
{
	uint32_t max_void_time = as_record_void_time_get() + MAX_ALLOWED_TTL;

	return void_time > max_void_time ? max_void_time : void_time;
}


#define AS_SET_MAX_COUNT ((1 << 12) - 1) // ID 0 means no set


// TODO - would be nice to put this in as_index.h:
// Callback invoked when as_index is destroyed.
typedef void (*as_index_value_destructor) (struct as_index_s* v, void* udata);

// TODO - would be nice to put this in as_index.h:
typedef struct as_index_tree_shared_s {
	cf_arenax*		arena;

	as_index_value_destructor destructor;
	void*			destructor_udata;

	// Number of sprigs per partition tree.
	uint32_t		n_sprigs;

	// Bit-shifts used to calculate indexes from digest bits.
	uint32_t		locks_shift;
	uint32_t		sprigs_shift;

	// Offsets into as_index_tree struct's variable-sized data.
	uint32_t		sprigs_offset;
	uint32_t		puddles_offset;
} as_index_tree_shared;


typedef struct as_sprigx_s {
	uint64_t root_h: 40;
} __attribute__ ((__packed__)) as_sprigx;

typedef struct as_treex_s {
	int block_ix[AS_PARTITIONS];
	as_sprigx sprigxs[0];
} as_treex;


typedef struct as_namespace_s {
	//--------------------------------------------
	// Data partitions - first, to 64-byte align.
	//

	as_partition partitions[AS_PARTITIONS];

	//--------------------------------------------
	// Name & ID.
	//

	char name[AS_ID_NAMESPACE_SZ];
	uint32_t ix; // this is 0-based

	//--------------------------------------------
	// Persistent memory.
	//

	// Index persistent memory type (default is shmem).
	cf_xmem_type	pi_xmem_type;
	const void*		pi_xmem_type_cfg;

	// Persistent memory "base" block ID for this namespace.
	uint32_t		xmem_id;

	// RAM copy of the persistent memory "base" block.
	uint8_t*		xmem_base;

	// Pointer to partition tree info in persistent memory "treex" block.
	struct as_treex_s* xmem_trees;

	// Pointer to arena structure (not stages) in persistent memory base block.
	cf_arenax*		arena;

	// Pointer to set information vmap in persistent memory base block.
	cf_vmapx*		p_sets_vmap;

	// Temporary array of sets to hold config values until sets vmap is ready.
	struct as_set_s* sets_cfg_array;
	uint32_t		sets_cfg_count;

	// Sindex persistent memory type (default is shmem).
	cf_xmem_type	si_xmem_type;
	const void*		si_xmem_type_cfg;

	// Pointer to sindex persistent memory "meta" block.
	uint8_t*		xmem_si_meta;

	// Pointer to array in sindex persistent memory "meta" block.
	struct as_flat_sindex_s* flat_sindexes;
	uint32_t		n_flat_sindexes;

	// Configuration flags relevant for warm restart.
	uint32_t		xmem_flags;

	// Data shmem key base for this namespace.
	key_t			data_key_base;

	// Temporary array of data shmem "stripes" used at startup.
	void*			stripes;
	size_t			stripe_sz;

	// Temporary list of "original" index refs to be freed during warm restart.
	cf_queue*		origs_gc_q;

	//--------------------------------------------
	// Cold start.
	//

	// If true, read storage devices to build index at startup.
	bool			cold_start;

	// If true, device headers indicate previous shutdown was not clean.
	bool			dirty_restart;

	// Flag for ticker during initial loading of records from device.
	bool			loading_records;

	// For cold start eviction.
	cf_mutex		cold_start_evict_lock;
	uint32_t		cold_start_record_add_count;
	uint32_t		cold_start_now;

	// For sanity checking at startup (also used during warm restart).
	uint32_t		startup_max_void_time;

	cf_shash*		mrt_cold_start_hash; // relevant only for enterprise edition
	bool			mrt_cold_start_2nd_pass; // relevant only for enterprise edition

	//--------------------------------------------
	// Memory management.
	//

	// Cached partition ownership info for clients.
	struct client_replica_map_s* replica_maps;

	// Common partition tree information. Contains two configuration items.
	as_index_tree_shared tree_shared;

	//--------------------------------------------
	// Storage management.
	//

	// This is typecast to (drv_ssds*) in storage code.
	void*			storage_private;

	uint64_t		drives_size; // usable size of all drives
	uint32_t		storage_max_write_q; // storage_max_write_cache is converted to this
	uint32_t	 	post_write_q_limit; // storage_post_write_cache is converted to this
	uint32_t		n_wblocks_to_flush; // on write queues or shadow queues
	uint32_t		saved_defrag_sleep; // restore after defrag at startup is done
	uint32_t		defrag_lwm_size; // storage_defrag_lwm_pct % of storage_write_block_size

	// For data-not-in-memory, we optionally cache swbs after writing to device.
	// To track fraction of reads from cache:
	uint32_t		n_reads_from_cache;
	uint32_t		n_reads_from_device;

	uint8_t			storage_encryption_key[64];
	uint8_t			storage_encryption_old_key[64];

	//--------------------------------------------
	// Eviction.
	//

	uint32_t		smd_evict_void_time;
	uint32_t		evict_void_time;

	//--------------------------------------------
	// Truncate records.
	//

	uint64_t		truncate_lut;
	cf_shash*		truncate_startup_set_hash; // relevant only for enterprise edition
	bool			truncating;

	//--------------------------------------------
	// Short query reservations.
	//

	cf_mutex		query_rsvs_lock;
	as_partition_reservation* query_rsvs;

	//--------------------------------------------
	// Secondary index.
	//

	struct as_sindex_arena_s* si_arena;

	struct as_sindex_s* sindexes[MAX_N_SINDEXES];
	uint32_t		si_ids[MAX_N_SINDEXES];

	uint8_t*		si_startup_gc_bitmap; // optimize sindex startup GC
	bool			si_startup_gc_needed; // may not need sindex startup GC
	bool			sindexes_resumed_readable; // optimize startup populate
	uint64_t		si_n_recs_checked; // used only by startup ticker

	uint32_t		n_setless_sindexes;
	cf_shash*		sindex_defn_hash;
	cf_shash*		sindex_iname_hash;

	bool			si_gc_rlist_full;
	cf_mutex		si_gc_list_mutex;
	// The queues are not threadsafe - protected by si_gc_list_mutex.
	cf_queue*		si_gc_rlist;
	cf_queue*		si_gc_tlist;
	bool			si_gc_tlist_map[AS_PARTITIONS][MAX_NUM_TREE_IDS];

	//--------------------------------------------
	// XDR.
	//

	cf_vector*		xdr_dc_names; // allocated for use at startup only
	bool			xdr_ships_drops; // at least one DC ships drops
	bool			xdr_ships_nsup_drops; // at least one DC ships nsup drops
	bool			xdr_ships_changed_bins; // at least one DC ships changed bins (invokes bin xdr_write flag and tombstones)
	bool			xdr_ships_bin_luts; // at least one DC ships changed bin LUTs (changed bins plus src-id)

	//--------------------------------------------
	// MRTs.
	//

	cf_shash*		mrt_monitor_hash;
	uint32_t		mrt_monitor_set_id;
	uint64_t		mrt_start_lut;

	//--------------------------------------------
	// Configuration.
	//

	uint32_t		cfg_active_rack; // relevant only for enterprise edition
	uint32_t		active_rack; // indirect config - can become disabled if any other node reports a different value
	bool			allow_ttl_without_nsup;
	bool			apply_ttl_reductions;
	bool			auto_revive;
	uint32_t		background_query_max_rps;
	conflict_resolution_pol conflict_resolution_policy;
	bool			conflict_resolve_writes;
	bool			cp; // relevant only for enterprise edition
	bool			cp_allow_drops; // relevant only for enterprise edition
	uint32_t		default_read_touch_ttl_pct;
	uint32_t		default_ttl;
	bool			cold_start_eviction_disabled;
	bool			mrt_writes_disabled; // relevant only for enterprise edition
	bool			write_dup_res_disabled;
	bool			ap_disallow_drops;
	bool			disallow_null_setname;
	bool			batch_sub_benchmarks_enabled;
	bool			ops_sub_benchmarks_enabled;
	bool			read_benchmarks_enabled;
	bool			udf_benchmarks_enabled;
	bool			udf_sub_benchmarks_enabled;
	bool			write_benchmarks_enabled;
	bool			proxy_hist_enabled;
	uint32_t		evict_hist_buckets;
	uint32_t		evict_indexes_memory_pct;
	uint32_t		evict_tenths_pct;
	bool			force_long_queries; // for debugging only
	bool			ignore_migrate_fill_delay;
	uint64_t		index_stage_size;
	uint64_t		indexes_memory_budget;
	bool			inline_short_queries;
	uint32_t		max_record_size;
	uint32_t		migrate_order;
	uint32_t		migrate_retransmit_ms;
	bool			migrate_skip_unreadable;
	uint32_t		migrate_sleep;
	uint32_t		mrt_duration; // relevant only for enterprise edition
	uint32_t		nsup_hist_period;
	uint32_t		nsup_period;
	uint32_t		n_nsup_threads;
	bool			cfg_prefer_uniform_balance; // relevant only for enterprise edition
	bool			prefer_uniform_balance; // indirect config - can become disabled if any other node reports disabled
	uint32_t		rack_id;
	as_read_consistency_level read_consistency_level;
	bool			reject_non_xdr_writes;
	bool			reject_xdr_writes;
	uint32_t		cfg_replication_factor;
	uint32_t		replication_factor; // indirect config - can become less than cfg_replication_factor
	uint64_t		sindex_stage_size;
	uint32_t		n_single_query_threads;
	uint32_t		stop_writes_sys_memory_pct;
	uint32_t		tomb_raider_eligible_age; // relevant only for enterprise edition
	uint32_t		tomb_raider_period; // relevant only for enterprise edition
	uint32_t		transaction_pending_limit; // 0 means no limit
	uint32_t		n_truncate_threads;
	as_write_commit_level write_commit_level;
	uint64_t		xdr_bin_tombstone_ttl_ms;
	uint32_t		xdr_tomb_raider_period;
	uint32_t		n_xdr_tomb_raider_threads;

	const char*		pi_xmem_mounts[CF_XMEM_MAX_MOUNTS];
	uint32_t		n_pi_xmem_mounts; // indirect config
	uint32_t		pi_evict_mounts_pct;
	uint64_t		pi_mounts_budget;

	const char*		si_xmem_mounts[CF_XMEM_MAX_MOUNTS];
	uint32_t		n_si_xmem_mounts; // indirect config
	uint32_t		si_evict_mounts_pct;
	uint64_t		si_mounts_budget;

	as_storage_type storage_type;

	const char*		storage_devices[AS_STORAGE_MAX_DEVICES];
	uint32_t		n_storage_stripes; // indirect config - if devices array contains memory stripes
	uint32_t		n_storage_devices; // indirect config - if devices array contains raw devices (or partitions)
	uint32_t		n_storage_files; // indirect config - if devices array contains files

	const char*		storage_shadows[AS_STORAGE_MAX_DEVICES];
	uint32_t		n_storage_shadows; // indirect config

	bool			storage_cache_replica_writes;
	bool			storage_cold_start_empty;
	bool			storage_commit_to_device; // relevant only for enterprise edition
	as_compression_method storage_compression; // relevant only for enterprise edition
	uint32_t		storage_compression_acceleration; // relevant only for enterprise edition
	uint32_t		storage_compression_level; // relevant only for enterprise edition
	uint32_t		storage_defrag_lwm_pct;
	uint32_t		storage_defrag_queue_min;
	uint32_t		storage_defrag_sleep;
	uint32_t		storage_defrag_startup_minimum;
	bool			storage_direct_files;
	bool			storage_disable_odsync;
	bool			storage_benchmarks_enabled; // histograms are per-drive except device-read-size & device-write-size
	as_encryption_method storage_encryption; // relevant only for enterprise edition
	char*			storage_encryption_key_file; // relevant only for enterprise edition
	char*			storage_encryption_old_key_file; // relevant only for enterprise edition
	uint32_t		storage_evict_used_pct;
	uint64_t		storage_filesize;
	uint64_t		storage_flush_max_us;
	uint32_t		storage_flush_size;
	uint64_t		storage_max_write_cache;
	uint64_t		storage_post_write_cache;
	bool			storage_read_page_cache;
	bool			storage_serialize_tomb_raider; // relevant only for enterprise edition
	bool			storage_sindex_startup_device_scan;
	uint32_t		storage_stop_writes_avail_pct;
	uint32_t		storage_stop_writes_used_pct;
	uint32_t		storage_tomb_raider_sleep; // relevant only for enterprise edition
	uint64_t		storage_data_size;

	bool			geo2dsphere_within_strict;
	uint16_t		geo2dsphere_within_min_level;
	uint16_t		geo2dsphere_within_max_level;
	uint16_t		geo2dsphere_within_max_cells;
	uint16_t		geo2dsphere_within_level_mod;
	uint32_t		geo2dsphere_within_earth_radius_meters;

	//--------------------------------------------
	// Statistics and histograms.
	//

	// Object counts.

	uint64_t		n_objects;
	uint64_t		n_tombstones; // relevant only for enterprise edition
	uint64_t		n_xdr_tombstones; // subset of n_tombstones
	uint64_t		n_durable_tombstones; // subset of n_tombstones
	uint64_t		n_xdr_bin_cemeteries; // subset of n_tombstones
	uint64_t		n_mrt_provisionals; // relevant only for enterprise edition

	// Consistency info.

	uint64_t		n_unreplicated_records;
	uint32_t		n_dead_partitions;
	uint32_t		n_unavailable_partitions;
	uint32_t		n_auto_revived_partitions;
	bool			clock_skew_stop_writes;

	// Expiration & eviction (nsup) stats.

	bool			stop_writes;
	bool			hwm_breached;
	bool			memory_breached; // stop-writes-sys-memory-pct or indexes-memory-budget

	uint64_t		non_expirable_objects;

	uint64_t		n_expired_objects;
	uint64_t		n_evicted_objects;

	int32_t			evict_ttl; // signed - possible (but weird) it's negative

	uint32_t		nsup_cycle_duration; // seconds taken for most recent nsup cycle
	double			nsup_cycle_deleted_pct; // percentage of namespace expired/evicted for most recent nsup cycle

	uint64_t		n_nsup_xdr_key_busy;

	// Sindex GC stats.

	uint64_t		n_sindex_gc_cleaned;

	// Persistent storage stats.

	double			comp_avg_orig_sz; // relevant only for enterprise edition
	double			comp_avg_comp_sz; // relevant only for enterprise edition
	float			cache_read_pct;

	// Proto-compression stats.

	as_proto_comp_stat record_comp_stat; // relevant only for enterprise edition
	as_proto_comp_stat query_comp_stat; // relevant only for enterprise edition

	// Migration stats.

	uint64_t		migrate_tx_partitions_imbalance; // debug only
	uint64_t		migrate_tx_instance_count; // debug only
	uint64_t		migrate_rx_instance_count; // debug only
	uint64_t		migrate_tx_partitions_active;
	uint64_t		migrate_rx_partitions_active;
	uint64_t		migrate_tx_partitions_initial;
	uint64_t		migrate_tx_partitions_remaining;
	uint64_t		migrate_tx_partitions_lead_remaining;
	uint64_t		migrate_rx_partitions_initial;
	uint64_t		migrate_rx_partitions_remaining;
	uint64_t		migrate_signals_active;
	uint64_t		migrate_signals_remaining;
	uint64_t		migrate_fresh_partitions;
	uint64_t		appeals_tx_active; // relevant only for enterprise edition
	uint64_t		appeals_rx_active; // relevant only for enterprise edition
	uint64_t		appeals_tx_remaining; // relevant only for enterprise edition

	// Per-record migration stats:
	uint64_t		migrate_records_skipped; // relevant only for enterprise edition
	uint64_t		migrate_records_transmitted;
	uint64_t		migrate_record_retransmits;
	uint64_t		migrate_record_receives;
	uint64_t		appeals_records_exonerated; // relevant only for enterprise edition
	uint64_t		migrate_records_unreadable;

	// From-client transaction stats.

	uint64_t		n_client_tsvc_error;
	uint64_t		n_client_tsvc_timeout;

	uint64_t		n_client_proxy_complete;
	uint64_t		n_client_proxy_error;
	uint64_t		n_client_proxy_timeout;

	uint64_t		n_client_read_success;
	uint64_t		n_client_read_error;
	uint64_t		n_client_read_timeout;
	uint64_t		n_client_read_not_found;
	uint64_t		n_client_read_filtered_out;

	uint64_t		n_client_write_success;
	uint64_t		n_client_write_error;
	uint64_t		n_client_write_timeout;
	uint64_t		n_client_write_filtered_out;

	// Subset of n_client_write_... above, respectively.
	uint64_t		n_xdr_client_write_success;
	uint64_t		n_xdr_client_write_error;
	uint64_t		n_xdr_client_write_timeout;

	uint64_t		n_client_delete_success;
	uint64_t		n_client_delete_error;
	uint64_t		n_client_delete_timeout;
	uint64_t		n_client_delete_not_found;
	uint64_t		n_client_delete_filtered_out;

	// Subset of n_client_delete_... above, respectively.
	uint64_t		n_xdr_client_delete_success;
	uint64_t		n_xdr_client_delete_error;
	uint64_t		n_xdr_client_delete_timeout;
	uint64_t		n_xdr_client_delete_not_found;

	uint64_t		n_client_udf_complete;
	uint64_t		n_client_udf_error;
	uint64_t		n_client_udf_timeout;
	uint64_t		n_client_udf_filtered_out;

	uint64_t		n_client_lang_read_success;
	uint64_t		n_client_lang_write_success;
	uint64_t		n_client_lang_delete_success;
	uint64_t		n_client_lang_error;

	// From-proxy transaction stats.

	uint64_t		n_from_proxy_tsvc_error;
	uint64_t		n_from_proxy_tsvc_timeout;

	uint64_t		n_from_proxy_read_success;
	uint64_t		n_from_proxy_read_error;
	uint64_t		n_from_proxy_read_timeout;
	uint64_t		n_from_proxy_read_not_found;
	uint64_t		n_from_proxy_read_filtered_out;

	uint64_t		n_from_proxy_write_success;
	uint64_t		n_from_proxy_write_error;
	uint64_t		n_from_proxy_write_timeout;
	uint64_t		n_from_proxy_write_filtered_out;

	// Subset of n_from_proxy_write_... above, respectively.
	uint64_t		n_xdr_from_proxy_write_success;
	uint64_t		n_xdr_from_proxy_write_error;
	uint64_t		n_xdr_from_proxy_write_timeout;

	uint64_t		n_from_proxy_delete_success;
	uint64_t		n_from_proxy_delete_error;
	uint64_t		n_from_proxy_delete_timeout;
	uint64_t		n_from_proxy_delete_not_found;
	uint64_t		n_from_proxy_delete_filtered_out;

	// Subset of n_from_proxy_delete_... above, respectively.
	uint64_t		n_xdr_from_proxy_delete_success;
	uint64_t		n_xdr_from_proxy_delete_error;
	uint64_t		n_xdr_from_proxy_delete_timeout;
	uint64_t		n_xdr_from_proxy_delete_not_found;

	uint64_t		n_from_proxy_udf_complete;
	uint64_t		n_from_proxy_udf_error;
	uint64_t		n_from_proxy_udf_timeout;
	uint64_t		n_from_proxy_udf_filtered_out;

	uint64_t		n_from_proxy_lang_read_success;
	uint64_t		n_from_proxy_lang_write_success;
	uint64_t		n_from_proxy_lang_delete_success;
	uint64_t		n_from_proxy_lang_error;

	// Batch sub-transaction stats.

	uint64_t		n_batch_sub_tsvc_error;
	uint64_t		n_batch_sub_tsvc_timeout;

	uint64_t		n_batch_sub_proxy_complete;
	uint64_t		n_batch_sub_proxy_error;
	uint64_t		n_batch_sub_proxy_timeout;

	uint64_t		n_batch_sub_read_success;
	uint64_t		n_batch_sub_read_error;
	uint64_t		n_batch_sub_read_timeout;
	uint64_t		n_batch_sub_read_not_found;
	uint64_t		n_batch_sub_read_filtered_out;

	uint64_t		n_batch_sub_write_success;
	uint64_t		n_batch_sub_write_error;
	uint64_t		n_batch_sub_write_timeout;
	uint64_t		n_batch_sub_write_filtered_out;

	uint64_t		n_batch_sub_delete_success;
	uint64_t		n_batch_sub_delete_error;
	uint64_t		n_batch_sub_delete_timeout;
	uint64_t		n_batch_sub_delete_not_found;
	uint64_t		n_batch_sub_delete_filtered_out;

	uint64_t		n_batch_sub_udf_complete;
	uint64_t		n_batch_sub_udf_error;
	uint64_t		n_batch_sub_udf_timeout;
	uint64_t		n_batch_sub_udf_filtered_out;

	uint64_t		n_batch_sub_lang_read_success;
	uint64_t		n_batch_sub_lang_write_success;
	uint64_t		n_batch_sub_lang_delete_success;
	uint64_t		n_batch_sub_lang_error;

	// From-proxy batch sub-transaction stats.

	uint64_t		n_from_proxy_batch_sub_tsvc_error;
	uint64_t		n_from_proxy_batch_sub_tsvc_timeout;

	uint64_t		n_from_proxy_batch_sub_read_success;
	uint64_t		n_from_proxy_batch_sub_read_error;
	uint64_t		n_from_proxy_batch_sub_read_timeout;
	uint64_t		n_from_proxy_batch_sub_read_not_found;
	uint64_t		n_from_proxy_batch_sub_read_filtered_out;

	uint64_t		n_from_proxy_batch_sub_write_success;
	uint64_t		n_from_proxy_batch_sub_write_error;
	uint64_t		n_from_proxy_batch_sub_write_timeout;
	uint64_t		n_from_proxy_batch_sub_write_filtered_out;

	uint64_t		n_from_proxy_batch_sub_delete_success;
	uint64_t		n_from_proxy_batch_sub_delete_error;
	uint64_t		n_from_proxy_batch_sub_delete_timeout;
	uint64_t		n_from_proxy_batch_sub_delete_not_found;
	uint64_t		n_from_proxy_batch_sub_delete_filtered_out;

	uint64_t		n_from_proxy_batch_sub_udf_complete;
	uint64_t		n_from_proxy_batch_sub_udf_error;
	uint64_t		n_from_proxy_batch_sub_udf_timeout;
	uint64_t		n_from_proxy_batch_sub_udf_filtered_out;

	uint64_t		n_from_proxy_batch_sub_lang_read_success;
	uint64_t		n_from_proxy_batch_sub_lang_write_success;
	uint64_t		n_from_proxy_batch_sub_lang_delete_success;
	uint64_t		n_from_proxy_batch_sub_lang_error;

	// Internal-UDF sub-transaction stats.

	uint64_t		n_udf_sub_tsvc_error;
	uint64_t		n_udf_sub_tsvc_timeout;

	uint64_t		n_udf_sub_udf_complete;
	uint64_t		n_udf_sub_udf_error;
	uint64_t		n_udf_sub_udf_timeout;
	uint64_t		n_udf_sub_udf_filtered_out;

	uint64_t		n_udf_sub_lang_read_success;
	uint64_t		n_udf_sub_lang_write_success;
	uint64_t		n_udf_sub_lang_delete_success;
	uint64_t		n_udf_sub_lang_error;

	// Internal-ops sub-transaction stats.

	uint64_t		n_ops_sub_tsvc_error;
	uint64_t		n_ops_sub_tsvc_timeout;

	uint64_t		n_ops_sub_write_success;
	uint64_t		n_ops_sub_write_error;
	uint64_t		n_ops_sub_write_timeout;
	uint64_t		n_ops_sub_write_filtered_out;

	// Duplicate resolution stats.

	uint64_t		n_dup_res_ask;

	uint64_t		n_dup_res_respond_read;
	uint64_t		n_dup_res_respond_no_read;

	// Transaction retransmit stats - 'all' means both client & proxy origins.

	uint64_t		n_retransmit_all_read_dup_res;
	uint64_t		n_retransmit_all_write_dup_res;
	uint64_t		n_retransmit_all_delete_dup_res;
	uint64_t		n_retransmit_all_udf_dup_res;
	uint64_t		n_retransmit_all_batch_sub_read_dup_res;
	uint64_t		n_retransmit_all_batch_sub_write_dup_res;
	uint64_t		n_retransmit_all_batch_sub_delete_dup_res;
	uint64_t		n_retransmit_all_batch_sub_udf_dup_res;
	uint64_t		n_retransmit_udf_sub_dup_res;
	uint64_t		n_retransmit_ops_sub_dup_res;

	uint64_t		n_retransmit_all_read_repl_ping;
	uint64_t		n_retransmit_all_batch_sub_read_repl_ping;

	uint64_t		n_retransmit_all_write_repl_write;
	uint64_t		n_retransmit_all_delete_repl_write;
	uint64_t		n_retransmit_all_udf_repl_write;
	uint64_t		n_retransmit_all_batch_sub_write_repl_write;
	uint64_t		n_retransmit_all_batch_sub_delete_repl_write;
	uint64_t		n_retransmit_all_batch_sub_udf_repl_write;
	uint64_t		n_retransmit_udf_sub_repl_write;
	uint64_t		n_retransmit_ops_sub_repl_write;

	// Primary index query (formerly scan) stats.

	uint64_t		n_pi_query_short_basic_complete;
	uint64_t		n_pi_query_short_basic_error;
	uint64_t		n_pi_query_short_basic_timeout;

	uint64_t		n_pi_query_long_basic_complete;
	uint64_t		n_pi_query_long_basic_error;
	uint64_t		n_pi_query_long_basic_abort;

	uint64_t		n_pi_query_aggr_complete;
	uint64_t		n_pi_query_aggr_error;
	uint64_t		n_pi_query_aggr_abort;

	uint64_t		n_pi_query_udf_bg_complete;
	uint64_t		n_pi_query_udf_bg_error;
	uint64_t		n_pi_query_udf_bg_abort;

	uint64_t		n_pi_query_ops_bg_complete;
	uint64_t		n_pi_query_ops_bg_error;
	uint64_t		n_pi_query_ops_bg_abort;

	// Secondary index query stats.

	uint64_t		n_si_query_short_basic_complete;
	uint64_t		n_si_query_short_basic_error;
	uint64_t		n_si_query_short_basic_timeout;

	uint64_t		n_si_query_long_basic_complete;
	uint64_t		n_si_query_long_basic_error;
	uint64_t		n_si_query_long_basic_abort;

	uint64_t		n_si_query_aggr_complete;
	uint64_t		n_si_query_aggr_error;
	uint64_t		n_si_query_aggr_abort;

	uint64_t		n_si_query_udf_bg_complete;
	uint64_t		n_si_query_udf_bg_error;
	uint64_t		n_si_query_udf_bg_abort;

	uint64_t		n_si_query_ops_bg_complete;
	uint64_t		n_si_query_ops_bg_error;
	uint64_t		n_si_query_ops_bg_abort;

	// Geospatial query stats:
	uint64_t		geo_region_query_count;		// number of region queries
	uint64_t		geo_region_query_cells;		// number of cells used by region queries
	uint64_t		geo_region_query_points;	// number of valid points found
	uint64_t		geo_region_query_falsepos;	// number of false positives found

	// Read-touch stats.

	uint64_t		n_read_touch_tsvc_error;
	uint64_t		n_read_touch_tsvc_timeout;

	uint64_t		n_read_touch_success;
	uint64_t		n_read_touch_error;
	uint64_t		n_read_touch_timeout;
	uint64_t		n_read_touch_skip;

	// Re-replication stats - relevant only for enterprise edition.

	uint64_t		n_re_repl_tsvc_error;
	uint64_t		n_re_repl_tsvc_timeout;

	uint64_t		n_re_repl_success;
	uint64_t		n_re_repl_error;
	uint64_t		n_re_repl_timeout;

	// MRT verify read stats - relevant only for enterprise edition.

	uint64_t		n_mrt_verify_read_success;
	uint64_t		n_mrt_verify_read_error;
	uint64_t		n_mrt_verify_read_timeout;

	// MRT roll forward/back stats - relevant only for enterprise edition.

	uint64_t		n_mrt_roll_forward_success;
	uint64_t		n_mrt_roll_forward_error;
	uint64_t		n_mrt_roll_forward_timeout;

	// Subset of n_mrt_roll_forward... above, respectively
	uint64_t		n_mrt_monitor_roll_forward_success;
	uint64_t		n_mrt_monitor_roll_forward_error;
	uint64_t		n_mrt_monitor_roll_forward_timeout;

	uint64_t		n_mrt_roll_back_success;
	uint64_t		n_mrt_roll_back_error;
	uint64_t		n_mrt_roll_back_timeout;

	// Subset of n_mrt_roll_back... above, respectively
	uint64_t		n_mrt_monitor_roll_back_success;
	uint64_t		n_mrt_monitor_roll_back_error;
	uint64_t		n_mrt_monitor_roll_back_timeout;

	// Special errors that deserve their own counters:

	uint64_t		n_fail_xdr_forbidden;
	uint64_t		n_fail_key_busy;
	uint64_t		n_fail_xdr_key_busy;
	uint64_t		n_fail_generation;
	uint64_t		n_fail_record_too_big;
	uint64_t		n_fail_client_lost_conflict;
	uint64_t		n_fail_xdr_lost_conflict;
	uint64_t		n_fail_mrt_blocked;
	uint64_t		n_fail_mrt_version_mismatch;

	// Special non-error counters:

	uint64_t		n_deleted_last_bin;
	uint64_t		n_mrt_monitor_roll_tombstone_creates; // relevant only for enterprise edition
	uint64_t		n_ttl_reductions_ignored;
	uint64_t		n_ttl_reductions_applied;

	// One-way automatically activated histograms.

	histogram*		read_hist;
	histogram*		write_hist;
	histogram*		udf_hist;
	histogram*		batch_sub_read_hist;
	histogram*		batch_sub_write_hist;
	histogram*		batch_sub_udf_hist;
	histogram*		pi_query_hist;
	histogram*		pi_query_rec_count_hist; // not tracked
	histogram*		si_query_hist;
	histogram*		si_query_rec_count_hist; // not tracked
	histogram*		read_touch_hist;
	histogram*		re_repl_hist; // relevant only for enterprise edition
	histogram*		writes_per_mrt_hist; // relevant only for enterprise edition, not tracked

	bool			read_hist_active;
	bool			write_hist_active;
	bool			udf_hist_active;
	bool			batch_sub_read_hist_active;
	bool			batch_sub_write_hist_active;
	bool			batch_sub_udf_hist_active;
	bool			pi_query_hist_active;
	bool			pi_query_rec_count_hist_active;
	bool			si_query_hist_active;
	bool			si_query_rec_count_hist_active;
	bool			read_touch_hist_active;
	bool			re_repl_hist_active; // relevant only for enterprise edition
	bool			writes_per_mrt_hist_active; // relevant only for enterprise edition

	// Activate-by-config histograms.

	histogram*		proxy_hist;

	histogram*		read_start_hist;
	histogram*		read_restart_hist;
	histogram*		read_dup_res_hist;
	histogram*		read_repl_ping_hist;
	histogram*		read_local_hist;
	histogram*		read_response_hist;

	histogram*		write_start_hist;
	histogram*		write_restart_hist;
	histogram*		write_dup_res_hist;
	histogram*		write_master_hist; // split this?
	histogram*		write_repl_write_hist;
	histogram*		write_response_hist;

	histogram*		udf_start_hist;
	histogram*		udf_restart_hist;
	histogram*		udf_dup_res_hist;
	histogram*		udf_master_hist; // split this?
	histogram*		udf_repl_write_hist;
	histogram*		udf_response_hist;

	histogram*		batch_sub_prestart_hist;
	histogram*		batch_sub_start_hist;
	histogram*		batch_sub_restart_hist;
	histogram*		batch_sub_dup_res_hist;
	histogram*		batch_sub_repl_ping_hist;
	histogram*		batch_sub_read_local_hist;
	histogram*		batch_sub_write_master_hist;
	histogram*		batch_sub_udf_master_hist;
	histogram*		batch_sub_repl_write_hist;
	histogram*		batch_sub_response_hist;

	histogram*		udf_sub_start_hist;
	histogram*		udf_sub_restart_hist;
	histogram*		udf_sub_dup_res_hist;
	histogram*		udf_sub_master_hist; // split this?
	histogram*		udf_sub_repl_write_hist;
	histogram*		udf_sub_response_hist;

	histogram*		ops_sub_start_hist;
	histogram*		ops_sub_restart_hist;
	histogram*		ops_sub_dup_res_hist;
	histogram*		ops_sub_master_hist; // split this?
	histogram*		ops_sub_repl_write_hist;
	histogram*		ops_sub_response_hist;

	histogram*		device_read_size_hist; // not tracked
	histogram*		device_write_size_hist; // not tracked

	// Histograms of object storage sizes. (Meaningful for drive-backed
	// namespaces only.)
	histogram*		obj_size_log_hist; // not tracked
	histogram*		set_obj_size_log_hists[AS_SET_MAX_COUNT + 1]; // not tracked
	linear_hist*	obj_size_lin_hist;
	linear_hist*	set_obj_size_lin_hists[AS_SET_MAX_COUNT + 1];

	// Histograms used for general eviction and expiration.
	linear_hist*	evict_hist; // not just for info
	linear_hist*	ttl_hist;
	linear_hist*	set_ttl_hists[AS_SET_MAX_COUNT + 1];

	//--------------------------------------------
	// Information for rebalancing.
	//

	uint32_t cluster_size;
	cf_node succession[AS_CLUSTER_SZ];
	cf_node hub; // relevant only for XDR
	as_partition_version cluster_versions[AS_CLUSTER_SZ][AS_PARTITIONS];
	uint32_t lo_repl_factor;
	uint32_t hi_repl_factor;
	uint32_t rack_ids[AS_CLUSTER_SZ]; // is observed-rack-ids in CP mode

	// Quiescence - relevant only for enterprise edition.
	uint32_t active_size; // number of nodes that are not quiesced
	bool pending_quiesce;
	bool is_quiesced;
	bool quiesced[AS_CLUSTER_SZ];

	// Observed nodes - relevant only for enterprise edition.
	uint32_t observed_cluster_size;
	cf_node observed_succession[AS_CLUSTER_SZ];

	// Roster management - relevant only for enterprise edition.
	uint32_t smd_roster_generation;
	uint32_t smd_roster_active_rack;
	uint32_t smd_roster_count;
	cf_node smd_roster[AS_CLUSTER_SZ];
	uint32_t smd_roster_rack_ids[AS_CLUSTER_SZ];
	uint32_t roster_generation;
	uint32_t roster_active_rack;
	uint32_t roster_count;
	cf_node roster[AS_CLUSTER_SZ];
	uint32_t roster_rack_ids[AS_CLUSTER_SZ];

	// Master regimes - relevant only for enterprise edition.
	uint32_t eventual_regime;
	uint32_t rebalance_regime;
	uint32_t rebalance_regimes[AS_CLUSTER_SZ];

}  as_namespace;

#define AS_SET_NAME_MAX_SIZE	64		// includes space for null-terminator

#define INVALID_SET_ID 0

// Caution - changing the size of this struct will break warm restart.
typedef struct as_set_s {
	char			name[AS_SET_NAME_MAX_SIZE];

	// Only name survives warm restart - these are all reset.
	uint64_t		n_objects;
	uint64_t		n_tombstones;		// relevant only for enterprise edition
	uint64_t		data_used_bytes;	// sets's total record data size
	uint64_t		stop_writes_count;	// restrict number of records in a set
	uint64_t		stop_writes_size;	// restrict record data size (bytes) in a set
	uint64_t		truncate_lut;		// records with last-update-time less than this are truncated
	uint32_t		n_sindexes;
	uint32_t		default_ttl;		// can override namespace default_ttl
	uint32_t		default_read_touch_ttl_pct; // can override namespace cfg
	bool			eviction_disabled;	// don't evict anything in this set (note - expiration still works)
	bool			index_enabled;
	bool			index_populating;
	bool			truncating;
} as_set;

COMPILER_ASSERT(sizeof(as_set) == 128);

static inline bool
as_set_count_stop_writes(const as_set* p_set)
{
	uint64_t stop_writes_count = as_load_uint64(&p_set->stop_writes_count);

	return stop_writes_count != 0 && p_set->n_objects >= stop_writes_count;
}

static inline bool
as_set_size_stop_writes(const as_set* p_set)
{
	if (p_set == NULL) {
		return false;
	}

	uint64_t stop_writes_size = as_load_uint64(&p_set->stop_writes_size);

	return stop_writes_size != 0 && p_set->data_used_bytes > stop_writes_size;
}

// These bin functions must be below definition of struct as_namespace_s:

static inline size_t
as_bin_memcpy_name(uint8_t* buf, as_bin* b)
{
	const char* name = b->name;
	char* to = (char*)buf;

	while (*name != '\0') {
		*to++ = *name++;
	}

	return (uint8_t*)to - buf;
}

/* Namespace function declarations */
as_namespace *as_namespace_create(char *name);
void as_namespaces_setup(bool cold_start_cmd, uint32_t instance);
void as_namespaces_init(bool cold_start_cmd, uint32_t instance);
void as_namespace_finish_setup(as_namespace *ns, uint32_t instance);
bool as_namespace_configure_sets(as_namespace *ns);
as_namespace *as_namespace_get_byname(const char *name);
as_namespace *as_namespace_get_bybuf(const uint8_t *name, size_t len);
as_namespace *as_namespace_get_bymsgfield(struct as_msg_field_s *fp);
const char *as_namespace_get_set_name(const as_namespace *ns, uint16_t set_id);
uint16_t as_namespace_get_set_id(as_namespace *ns, const char *set_name);
uint16_t as_namespace_get_create_set_id(as_namespace *ns, const char *set_name);
int as_namespace_set_set_w_len(as_namespace *ns, const char *set_name, size_t len, uint16_t *p_set_id, bool apply_count_limit);
int as_namespace_get_create_set_w_len(as_namespace *ns, const char *set_name, size_t len, struct as_set_s **pp_set, uint16_t *p_set_id);
struct as_set_s *as_namespace_get_set_by_name(as_namespace *ns, const char *set_name);
struct as_set_s* as_namespace_get_set_by_id(const as_namespace* ns, uint16_t set_id);
struct as_set_s* as_namespace_get_record_set(const as_namespace *ns, const as_record *r);
void as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db);
void as_namespace_adjust_set_data_used_bytes(as_namespace *ns, uint16_t set_id, int64_t delta_bytes);
void as_namespace_release_set_id(as_namespace *ns, uint16_t set_id);
void as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name, cf_dyn_buf *db);

static inline bool
as_namespace_is_memory_only(const as_namespace *ns)
{
	return ns->storage_type == AS_STORAGE_ENGINE_MEMORY &&
			ns->n_storage_shadows == 0;
}

static inline uint32_t
as_namespace_device_count(const as_namespace *ns)
{
	// Only one of them will ever be non-zero.
	return ns->n_storage_devices + ns->n_storage_files;
}

static inline bool
as_namespace_index_persisted(const as_namespace *ns)
{
	return ns->pi_xmem_type == CF_XMEM_TYPE_PMEM ||
			ns->pi_xmem_type == CF_XMEM_TYPE_FLASH;
}

static inline bool
as_namespace_sindex_persisted(const as_namespace *ns)
{
	return ns->si_xmem_type == CF_XMEM_TYPE_PMEM ||
			ns->si_xmem_type == CF_XMEM_TYPE_FLASH;
}

// Persistent Memory Management
bool as_namespace_xmem_shutdown(as_namespace *ns, uint32_t instance);
