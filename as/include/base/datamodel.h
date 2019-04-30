/*
 * datamodel.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

/*
 * core data model structures and definitions
 */

#pragma once

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_val.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "dynbuf.h"
#include "hist.h"
#include "hist_track.h"
#include "linear_hist.h"
#include "msg.h"
#include "node.h"
#include "shash.h"
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

// [0-1] for partition-id
// [1-4] for tree sprigs and locks
// [5-7] unused
// [8-11] for SSD device hash
#define DIGEST_STORAGE_BASE_BYTE	8
// [12-15] for rw_request hash
#define DIGEST_HASH_BASE_BYTE		12
// [16-19] for pred-exp filter


/* Forward declarations */
typedef struct as_namespace_s as_namespace;
typedef struct as_index_s as_record;
typedef struct as_bin_s as_bin;
typedef struct as_index_ref_s as_index_ref;
typedef struct as_set_s as_set;

struct as_index_tree_s;


#define AS_ID_NAMESPACE_SZ 32

#define AS_ID_INAME_SZ 256

#define AS_BIN_NAME_MAX_SZ 16 // changing this breaks warm restart
#define MAX_BIN_NAMES 0x10000 // no need for more - numeric ID is 16 bits
#define BIN_NAMES_QUOTA (MAX_BIN_NAMES / 2) // don't add more names than this via client transactions

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


/* as_particle_type
 * Particles are typed, which reflects their contents:
 *    NULL: no associated content (not sure I really need this internally?)
 *    INTEGER: a signed, 64-bit integer
 *    FLOAT: a floating point
 *    STRING: a null-terminated UTF-8 string
 *    BLOB: arbitrary-length binary data
 *    TIMESTAMP: milliseconds since 1 January 1970, 00:00:00 GMT
 *    DIGEST: an internal Aerospike key digest */
typedef enum {
	AS_PARTICLE_TYPE_NULL = 0,
	AS_PARTICLE_TYPE_INTEGER = 1,
	AS_PARTICLE_TYPE_FLOAT = 2,
	AS_PARTICLE_TYPE_STRING = 3,
	AS_PARTICLE_TYPE_BLOB = 4,
	AS_PARTICLE_TYPE_TIMESTAMP = 5,
	AS_PARTICLE_TYPE_UNUSED_6 = 6,
	AS_PARTICLE_TYPE_JAVA_BLOB = 7,
	AS_PARTICLE_TYPE_CSHARP_BLOB = 8,
	AS_PARTICLE_TYPE_PYTHON_BLOB = 9,
	AS_PARTICLE_TYPE_RUBY_BLOB = 10,
	AS_PARTICLE_TYPE_PHP_BLOB = 11,
	AS_PARTICLE_TYPE_ERLANG_BLOB = 12,
	AS_PARTICLE_TYPE_MAP = 19,
	AS_PARTICLE_TYPE_LIST = 20,
	AS_PARTICLE_TYPE_GEOJSON = 23,
	AS_PARTICLE_TYPE_MAX = 24,
	AS_PARTICLE_TYPE_BAD = AS_PARTICLE_TYPE_MAX
} as_particle_type;

/* as_particle
 * The common part of a particle
 * this is poor man's subclassing - IE, how to do a subclassed interface in C
 * Go look in particle.c to see all the subclass implementation and structure */
typedef struct as_particle_s {
	uint8_t		metadata;		// used by the iparticle for is_integer and inuse, as well as version in multi bin mode only
								// used by *particle for type
	uint8_t		data[0];
} __attribute__ ((__packed__)) as_particle;

// Bit Flag constants used for the particle state value (4 bits, 16 values)
#define AS_BIN_STATE_UNUSED			0
#define AS_BIN_STATE_INUSE_INTEGER	1
#define AS_BIN_STATE_RECYCLE_ME		2 // was - hidden bin
#define AS_BIN_STATE_INUSE_OTHER	3
#define AS_BIN_STATE_INUSE_FLOAT	4

typedef struct as_particle_iparticle_s {
	uint8_t		version: 4;		// now unused - and can't be used in single-bin config
	uint8_t		state: 4;		// see AS_BIN_STATE_...
	uint8_t		data[0];
} __attribute__ ((__packed__)) as_particle_iparticle;

/* Particle function declarations */

static inline bool
is_embedded_particle_type(as_particle_type type)
{
	return type == AS_PARTICLE_TYPE_INTEGER || type == AS_PARTICLE_TYPE_FLOAT;
}

extern as_particle_type as_particle_type_from_asval(const as_val *val);
extern as_particle_type as_particle_type_from_msgpack(const uint8_t *packed, uint32_t packed_size);

extern uint32_t as_particle_size_from_asval(const as_val *val);

extern uint32_t as_particle_asval_client_value_size(const as_val *val);
extern uint32_t as_particle_asval_to_client(const as_val *val, as_msg_op *op);

extern const uint8_t *as_particle_skip_flat(const uint8_t *flat, const uint8_t *end);

// as_bin particle function declarations

extern void as_bin_particle_destroy(as_bin *b, bool free_particle);
extern uint32_t as_bin_particle_size(as_bin *b);

// wire:
extern int as_bin_particle_alloc_modify_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op);
extern int as_bin_particle_alloc_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op);
extern int as_bin_particle_alloc_from_pickled(as_bin *b, const uint8_t **p_pickled, const uint8_t *end);
extern int as_bin_particle_stack_from_pickled(as_bin *b, cf_ll_buf *particles_llb, const uint8_t **p_pickled, const uint8_t *end);
extern uint32_t as_bin_particle_client_value_size(const as_bin *b);
extern uint32_t as_bin_particle_to_client(const as_bin *b, as_msg_op *op);
extern uint32_t as_bin_particle_pickled_size(const as_bin *b);
extern uint32_t as_bin_particle_to_pickled(const as_bin *b, uint8_t *pickled);

// Different for CDTs - the operations may return results, so we don't use the
// normal APIs and particle table functions.
extern int as_bin_cdt_read_from_client(const as_bin *b, as_msg_op *op, as_bin *result);
extern int as_bin_cdt_alloc_modify_from_client(as_bin *b, as_msg_op *op, as_bin *result);
extern int as_bin_cdt_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, as_bin *result);

// as_val:
extern int as_bin_particle_replace_from_asval(as_bin *b, const as_val *val);
extern void as_bin_particle_stack_from_asval(as_bin *b, uint8_t* stack, const as_val *val);
extern as_val *as_bin_particle_to_asval(const as_bin *b);

// msgpack:
extern int as_bin_particle_alloc_from_msgpack(as_bin *b, const uint8_t *packed, uint32_t packed_size);

// flat:
extern const uint8_t *as_bin_particle_cast_from_flat(as_bin *b, const uint8_t *flat, const uint8_t *end);
extern const uint8_t *as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, const uint8_t *end);
extern uint32_t as_bin_particle_flat_size(as_bin *b);
extern uint32_t as_bin_particle_to_flat(const as_bin *b, uint8_t *flat);

// odd as_bin particle functions for specific particle types

// integer:
extern int64_t as_bin_particle_integer_value(const as_bin *b);
extern void as_bin_particle_integer_set(as_bin *b, int64_t i);

// string:
extern uint32_t as_bin_particle_string_ptr(const as_bin *b, char **p_value);

// geojson:
typedef void * geo_region_t;
#define MAX_REGION_CELLS    256
#define MAX_REGION_LEVELS   30
extern size_t as_bin_particle_geojson_cellids(const as_bin *b, uint64_t **pp_cells);
extern bool as_particle_geojson_match(as_particle *p, uint64_t cellid, geo_region_t region, bool is_strict);
extern bool as_particle_geojson_match_asval(const as_val *val, uint64_t cellid, geo_region_t region, bool is_strict);
char const *as_geojson_mem_jsonstr(const as_particle *p, size_t *p_jsonsz);

// list:
struct cdt_payload_s;
struct rollback_alloc_s;
extern void as_bin_particle_list_get_packed_val(const as_bin *b, struct cdt_payload_s *packed);
extern int as_bin_cdt_packed_read(const as_bin *b, const as_msg_op *op, as_bin *result);
extern int as_bin_cdt_packed_modify(as_bin *b, const as_msg_op *op, as_bin *result, cf_ll_buf *particles_llb);


/* as_bin
 * A bin container - null name means unused */
struct as_bin_s {
	as_particle	iparticle;	// 1 byte
	as_particle	*particle;	// for embedded particle this is value, not pointer

	// Never read or write these bytes in single-bin configuration:
	uint16_t	id;			// ID of bin name
	uint8_t		unused;		// pad to 12 bytes (multiple of 4) - legacy
} __attribute__ ((__packed__)) ;

// For data-in-memory namespaces in multi-bin mode, we keep an array of as_bin
// structs in memory, accessed via this struct.
typedef struct as_bin_space_s {
	uint16_t	n_bins;
	as_bin		bins[0];
} __attribute__ ((__packed__)) as_bin_space;

// TODO - Do we really need to pad as_bin to 12 bytes for thread safety?
// Do we ever write & read adjacent as_bin structures in a bins array from
// different threads when not under the record lock? And if we're worried about
// 4-byte alignment for that or any other reason, wouldn't we also have to pad
// after n_bins in as_bin_space?

// For data-in-memory namespaces in multi-bin mode, if we're storing extra
// record metadata, we access it via this struct. In this case the index points
// here instead of directly to an as_bin_space.
typedef struct as_rec_space_s {
	as_bin_space*	bin_space;

	// So far the key is the only extra record metadata we store in memory.
	uint32_t		key_size;
	uint8_t			key[0];
} __attribute__ ((__packed__)) as_rec_space;

// For copying as_bin structs without the last 3 bytes.
static inline void
as_single_bin_copy(as_bin *to, const as_bin *from)
{
	to->iparticle = from->iparticle;
	to->particle = from->particle;
}

static inline bool
as_bin_inuse(const as_bin *b)
{
	return (((as_particle_iparticle *)b)->state);
}

static inline uint8_t
as_bin_state(const as_bin *b)
{
	return ((as_particle_iparticle *)b)->state;
}

static inline void
as_bin_state_set(as_bin *b, uint8_t val)
{
	((as_particle_iparticle *)b)->state = val;
}

static inline void
as_bin_state_set_from_type(as_bin *b, as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_INTEGER;
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_FLOAT;
		break;
	case AS_PARTICLE_TYPE_TIMESTAMP:
		// TODO - unsupported
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	default:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_OTHER;
		break;
	}
}

static inline bool
as_bin_inuse_has(const as_storage_rd *rd)
{
	// In-use bins are at the beginning - only need to check the first bin.
	return rd->n_bins != 0 && (rd->pickle != NULL || as_bin_inuse(rd->bins));
}

static inline uint16_t
as_bin_inuse_count(const as_storage_rd *rd)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			return i;
		}
	}

	return rd->n_bins;
}

static inline void
as_bin_set_empty(as_bin *b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
}

static inline void
as_bin_set_empty_shift(as_storage_rd *rd, uint32_t i)
{
	// Shift the bins over, so there's no space between used bins.
	// This can overwrite the "emptied" bin, and that's fine.

	uint16_t j;

	for (j = i + 1; j < rd->n_bins; j++) {
		if (! as_bin_inuse(&rd->bins[j])) {
			break;
		}
	}

	uint16_t n = j - (i + 1);

	if (n) {
		memmove(&rd->bins[i], &rd->bins[i + 1], n * sizeof(as_bin));
	}

	// Mark the last bin that was *formerly* in use as null.
	as_bin_set_empty(&rd->bins[j - 1]);
}

static inline void
as_bin_set_empty_from(as_storage_rd *rd, uint16_t from) {
	for (uint16_t i = from; i < rd->n_bins; i++) {
		as_bin_set_empty(&rd->bins[i]);
	}
}

static inline void
as_bin_set_all_empty(as_storage_rd *rd) {
	as_bin_set_empty_from(rd, 0);
}

static inline bool
as_bin_is_embedded_particle(const as_bin *b) {
	return ((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_INTEGER ||
			((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_FLOAT;
}

static inline bool
as_bin_is_external_particle(const as_bin *b) {
	return ((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_OTHER;
}

static inline as_particle *
as_bin_get_particle(as_bin *b) {
	return as_bin_is_embedded_particle(b) ? &b->iparticle : b->particle;
}

// "Embedded" types like integer are stored directly, but other bin types
// ("other") must follow an indirection to get the actual type.
static inline uint8_t
as_bin_get_particle_type(const as_bin *b) {
	switch (((as_particle_iparticle *)b)->state) {
		case AS_BIN_STATE_INUSE_INTEGER:
			return AS_PARTICLE_TYPE_INTEGER;
		case AS_BIN_STATE_INUSE_FLOAT:
			return AS_PARTICLE_TYPE_FLOAT;
		case AS_BIN_STATE_INUSE_OTHER:
			return b->particle->metadata;
		default:
			return AS_PARTICLE_TYPE_NULL;
	}
}


/* Bin function declarations */
extern int16_t as_bin_get_id(as_namespace *ns, const char *name);
extern bool as_bin_get_or_assign_id_w_len(as_namespace *ns, const char *name, size_t len, uint16_t *id);
extern const char* as_bin_get_name_from_id(as_namespace *ns, uint16_t id);
extern bool as_bin_name_within_quota(as_namespace *ns, const char *name);
extern void as_bin_copy(as_namespace *ns, as_bin *to, const as_bin *from);
extern int as_storage_rd_load_n_bins(as_storage_rd *rd);
extern int as_storage_rd_load_bins(as_storage_rd *rd, as_bin *stack_bins);
extern void as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs);
extern as_bin *as_bin_get_by_id(as_storage_rd *rd, uint32_t id);
extern as_bin *as_bin_get(as_storage_rd *rd, const char *name);
extern as_bin *as_bin_get_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len);
extern as_bin *as_bin_create_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len, int *result);
extern as_bin *as_bin_get_or_create(as_storage_rd *rd, const char *name);
extern as_bin *as_bin_get_or_create_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len, int *result);
extern int32_t as_bin_get_index(as_storage_rd *rd, const char *name);
extern int32_t as_bin_get_index_from_buf(as_storage_rd *rd, const uint8_t *name, size_t len);
extern void as_bin_destroy(as_storage_rd *rd, uint16_t i);
extern void as_bin_allocate_bin_space(as_storage_rd *rd, int32_t delta);


typedef enum {
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF = 0,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION = 1,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME = 2,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_CP = 3
} conflict_resolution_pol;

/* Record function declarations */
extern uint32_t clock_skew_stop_writes_sec();
extern bool as_record_handle_clock_skew(as_namespace* ns, uint64_t skew_ms);
extern uint16_t plain_generation(uint16_t regime_generation, const as_namespace* ns);
extern void as_record_set_lut(as_record *r, uint32_t regime, uint64_t now_ms, const as_namespace* ns);
extern void as_record_increment_generation(as_record *r, const as_namespace* ns);
extern bool as_record_is_live(const as_record *r);
extern int as_record_get_create(struct as_index_tree_s *tree, const cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns);
extern int as_record_get(struct as_index_tree_s *tree, const cf_digest *keyd, as_index_ref *r_ref);
extern int as_record_get_live(struct as_index_tree_s *tree, const cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns);
extern int as_record_exists(struct as_index_tree_s *tree, const cf_digest *keyd);
extern int as_record_exists_live(struct as_index_tree_s *tree, const cf_digest *keyd, as_namespace *ns);
extern void as_record_rescue(as_index_ref *r_ref, as_namespace *ns);

extern void as_record_destroy_bins_from(as_storage_rd *rd, uint16_t from);
extern void as_record_destroy_bins(as_storage_rd *rd);
extern void as_record_free_bin_space(as_record *r);

extern void as_record_destroy(as_record *r, as_namespace *ns);
extern void as_record_done(as_index_ref *r_ref, as_namespace *ns);

void as_record_drop_stats(as_record* r, as_namespace* ns);

extern void as_record_finalize_key(as_record* r, as_namespace* ns, const uint8_t* key, uint32_t key_size);
extern void as_record_allocate_key(as_record* r, const uint8_t* key, uint32_t key_size);
extern int as_record_resolve_conflict(conflict_resolution_pol policy, uint16_t left_gen, uint64_t left_lut, uint16_t right_gen, uint64_t right_lut);
extern uint8_t *as_record_pickle(as_storage_rd *rd, size_t *len_r);
extern int as_record_write_from_pickle(as_storage_rd *rd);
extern int as_record_set_set_from_msg(as_record *r, as_namespace *ns, as_msg *m);

static inline bool
as_record_pickle_is_binless(const uint8_t *buf)
{
	return *(uint16_t *)buf == 0;
}

// For enterprise split only.
int record_resolve_conflict_cp(uint16_t left_gen, uint64_t left_lut, uint16_t right_gen, uint64_t right_lut);

static inline int
resolve_last_update_time(uint64_t left, uint64_t right)
{
	return left == right ? 0 : (right > left ? 1 : -1);
}

typedef struct as_remote_record_s {
	cf_node src;
	as_partition_reservation *rsv;
	cf_digest *keyd;

	uint8_t *pickle;
	size_t pickle_sz;

	uint32_t generation;
	uint32_t void_time;
	uint64_t last_update_time;

	const char *set_name;
	size_t set_name_len;

	const uint8_t *key;
	size_t key_size;

	bool is_old_pickle; // TODO - old pickle - remove in "six months"

	uint16_t n_bins;
	as_flat_comp_meta cm;
	uint32_t meta_sz;

	uint8_t repl_state; // relevant only for enterprise edition
} as_remote_record;

int as_record_replace_if_better(as_remote_record *rr, bool is_repl_write, bool skip_sindex, bool do_xdr_write);

// a simpler call that gives seconds in the right epoch
#define as_record_void_time_get() cf_clepoch_seconds()
bool as_record_is_expired(const as_record *r); // TODO - eventually inline

static inline bool
as_record_is_doomed(const as_record *r, struct as_namespace_s *ns)
{
	return as_record_is_expired(r) || as_truncate_record_is_truncated(r, ns);
}

#define AS_SINDEX_MAX		256

#define MIN_PARTITIONS_PER_INDEX 1
#define MAX_PARTITIONS_PER_INDEX 256
#define DEFAULT_PARTITIONS_PER_INDEX 32
#define MAX_PARTITIONS_PER_INDEX_CHAR 3 // Number of characters in max paritions per index

// as_sindex structure which hangs from the ns.
#define AS_SINDEX_INACTIVE			1 // On init, pre-loading
#define AS_SINDEX_ACTIVE			2 // On creation and afterwards
#define AS_SINDEX_DESTROY			3 // On destroy
// dummy sindex state when ai_btree_create() returns error this
// sindex is not available for any of the DML operations
#define AS_SINDEX_NOTCREATED		4 // Un-used flag
#define AS_SINDEX_FLAG_WACTIVE			0x01 // On ai btree create of sindex, never reset
#define AS_SINDEX_FLAG_RACTIVE			0x02 // When sindex scan of database is completed
#define AS_SINDEX_FLAG_DESTROY_CLEANUP 	0x04 // Called for AI clean-up during si deletion
#define AS_SINDEX_FLAG_MIGRATE_CLEANUP  0x08 // Un-used
#define AS_SINDEX_FLAG_POPULATING		0x10 // Indicates current si scan job, reset when scan is done.

struct as_sindex_s;
struct as_sindex_config_s;

#define AS_SET_MAX_COUNT 0x3FF	// ID's 10 bits worth minus 1 (ID 0 means no set)
#define AS_BINID_HAS_SINDEX_SIZE  MAX_BIN_NAMES / ( sizeof(uint32_t) * CHAR_BIT )


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


struct as_namespace_s {
	//--------------------------------------------
	// Data partitions - first, to 64-byte align.
	//

	as_partition partitions[AS_PARTITIONS];

	//--------------------------------------------
	// Name & ID.
	//

	char name[AS_ID_NAMESPACE_SZ];
	uint32_t id; // this is 1-based
	uint32_t namehash;

	//--------------------------------------------
	// Persistent memory.
	//

	// Persistent memory type (default is shmem).
	cf_xmem_type	xmem_type;
	const void*		xmem_type_cfg;

	// Persistent memory "base" block ID for this namespace.
	uint32_t		xmem_id;

	// Pointer to the persistent memory "base" block.
	uint8_t*		xmem_base;

	// Pointer to partition tree info in persistent memory "treex" block.
	as_treex*		xmem_trees;

	// Pointer to arena structure (not stages) in persistent memory base block.
	cf_arenax*		arena;

	// Pointer to bin name vmap in persistent memory base block.
	cf_vmapx*		p_bin_name_vmap;

	// Pointer to set information vmap in persistent memory base block.
	cf_vmapx*		p_sets_vmap;

	// Temporary array of sets to hold config values until sets vmap is ready.
	as_set*			sets_cfg_array;
	uint32_t		sets_cfg_count;

	// Configuration flags relevant for warm or cool restart.
	uint32_t		xmem_flags;

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

	// For sanity checking at startup (also used during warm or cool restart).
	uint32_t		startup_max_void_time;

	//--------------------------------------------
	// Memory management.
	//

	// JEMalloc arena to be used for long-term storage in this namespace (-1 if nonexistent.)
	int jem_arena;

	// Cached partition ownership info for clients.
	client_replica_map* replica_maps;

	// Common partition tree information. Contains two configuration items.
	as_index_tree_shared tree_shared;

	//--------------------------------------------
	// Storage management.
	//

	// This is typecast to (drv_ssds*) in storage code.
	void*			storage_private;

	uint64_t		ssd_size; // discovered (and rounded) size of drive
	int				storage_last_avail_pct; // most recently calculated available percent
	int				storage_max_write_q; // storage_max_write_cache is converted to this
	uint32_t		saved_defrag_sleep; // restore after defrag at startup is done
	uint32_t		defrag_lwm_size; // storage_defrag_lwm_pct % of storage_write_block_size

	// For data-not-in-memory, we optionally cache swbs after writing to device.
	// To track fraction of reads from cache:
	cf_atomic32		n_reads_from_cache;
	cf_atomic32		n_reads_from_device;

	uint8_t			storage_encryption_key[64];

	//--------------------------------------------
	// Eviction.
	//

	uint32_t		smd_evict_void_time;
	uint32_t		evict_void_time;

	//--------------------------------------------
	// Truncate records.
	//

	as_truncate		truncate;

	//--------------------------------------------
	// Secondary index.
	//

	int				sindex_cnt;
	uint32_t		n_setless_sindexes;
	struct as_sindex_s* sindex; // array with AS_MAX_SINDEX metadata
	cf_shash*		sindex_set_binid_hash;
	cf_shash*		sindex_iname_hash;
	uint32_t		binid_has_sindex[AS_BINID_HAS_SINDEX_SIZE];

	//--------------------------------------------
	// Configuration.
	//

	uint32_t		cfg_replication_factor;
	uint32_t		replication_factor; // indirect config - can become less than cfg_replication_factor
	uint64_t		memory_size;
	uint32_t		default_ttl;

	bool			enable_xdr;
	bool			sets_enable_xdr; // namespace-level flag to enable set-based xdr shipping
	bool			ns_forward_xdr_writes; // namespace-level flag to enable forwarding of xdr writes
	bool			ns_allow_nonxdr_writes; // namespace-level flag to allow nonxdr writes or not
	bool			ns_allow_xdr_writes; // namespace-level flag to allow xdr writes or not

	conflict_resolution_pol conflict_resolution_policy;
	bool			cp; // relevant only for enterprise edition
	bool			cp_allow_drops; // relevant only for enterprise edition
	bool			data_in_index; // with single-bin, allows warm restart for data-in-memory (with storage-engine device)
	bool			cold_start_eviction_disabled;
	bool			write_dup_res_disabled;
	bool			disallow_null_setname;
	bool			batch_sub_benchmarks_enabled;
	bool			read_benchmarks_enabled;
	bool			udf_benchmarks_enabled;
	bool			udf_sub_benchmarks_enabled;
	bool			write_benchmarks_enabled;
	bool			proxy_hist_enabled;
	uint32_t		evict_hist_buckets;
	uint32_t		evict_tenths_pct;
	uint32_t		hwm_disk_pct;
	uint32_t		hwm_memory_pct;
	uint64_t		index_stage_size;
	uint32_t		migrate_order;
	uint32_t		migrate_retransmit_ms;
	uint32_t		migrate_sleep;
	uint32_t		nsup_hist_period;
	uint32_t		nsup_period;
	uint32_t		n_nsup_threads;
	bool			cfg_prefer_uniform_balance; // relevant only for enterprise edition
	bool			prefer_uniform_balance; // indirect config - can become disabled if any other node reports disabled
	uint32_t		rack_id;
	as_read_consistency_level read_consistency_level;
	bool			single_bin; // restrict the namespace to objects with exactly one bin
	uint32_t		stop_writes_pct;
	uint32_t		tomb_raider_eligible_age; // relevant only for enterprise edition
	uint32_t		tomb_raider_period; // relevant only for enterprise edition
	uint32_t		transaction_pending_limit; // 0 means no limit
	as_write_commit_level write_commit_level;
	cf_vector		xdr_dclist_v;

	const char*		xmem_mounts[CF_XMEM_MAX_MOUNTS];
	uint32_t		n_xmem_mounts; // indirect config
	uint32_t		mounts_hwm_pct;
	uint64_t		mounts_size_limit;

	as_storage_type storage_type;

	const char*		storage_devices[AS_STORAGE_MAX_DEVICES];
	uint32_t		n_storage_devices; // indirect config - if devices array contains raw devices (or partitions)
	uint32_t		n_storage_files; // indirect config - if devices array contains files
	const char*		storage_shadows[AS_STORAGE_MAX_DEVICES];
	uint32_t		n_storage_shadows; // indirect config
	uint64_t		storage_filesize;
	char*			storage_scheduler_mode; // relevant for devices only, not files
	uint32_t		storage_write_block_size;
	bool			storage_data_in_memory;

	bool			storage_cold_start_empty;
	bool			storage_commit_to_device; // relevant only for enterprise edition
	uint32_t		storage_commit_min_size; // relevant only for enterprise edition
	as_compression_method storage_compression; // relevant only for enterprise edition
	uint32_t		storage_compression_level; // relevant only for enterprise edition
	uint32_t		storage_defrag_lwm_pct;
	uint32_t		storage_defrag_queue_min;
	uint32_t		storage_defrag_sleep;
	int				storage_defrag_startup_minimum;
	bool			storage_direct_files;
	bool			storage_benchmarks_enabled; // histograms are per-drive except device-read-size & device-write-size
	as_encryption_method storage_encryption; // relevant only for enterprise edition
	char*			storage_encryption_key_file; // relevant only for enterprise edition
	uint64_t		storage_flush_max_us;
	uint64_t		storage_max_write_cache;
	uint32_t		storage_min_avail_pct;
	cf_atomic32 	storage_post_write_queue; // number of swbs/device held after writing to device
	bool			storage_read_page_cache;
	bool			storage_serialize_tomb_raider; // relevant only for enterprise edition
	uint32_t		storage_tomb_raider_sleep; // relevant only for enterprise edition

	uint32_t		sindex_num_partitions;

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

	cf_atomic64		n_objects;
	cf_atomic64		n_tombstones; // relevant only for enterprise edition

	// Consistency info.

	uint32_t		n_dead_partitions;
	uint32_t		n_unavailable_partitions;
	bool			clock_skew_stop_writes;

	// Expiration & eviction (nsup) stats.

	bool			stop_writes;
	bool			hwm_breached;

	uint64_t		non_expirable_objects;

	uint64_t		n_expired_objects;
	uint64_t		n_evicted_objects;

	int32_t			evict_ttl; // signed - possible (but weird) it's negative

	uint32_t		nsup_cycle_duration; // seconds taken for most recent nsup cycle

	// Memory usage stats.

	cf_atomic_int	n_bytes_memory;
	cf_atomic64		n_bytes_sindex_memory;

	// Persistent storage stats.

	double			comp_avg_orig_sz; // relevant only for enterprise edition
	double			comp_avg_comp_sz; // relevant only for enterprise edition
	float			cache_read_pct;

	// Migration stats.

	cf_atomic_int	migrate_tx_partitions_imbalance; // debug only
	cf_atomic_int	migrate_tx_instance_count; // debug only
	cf_atomic_int	migrate_rx_instance_count; // debug only
	cf_atomic_int	migrate_tx_partitions_active;
	cf_atomic_int	migrate_rx_partitions_active;
	cf_atomic_int	migrate_tx_partitions_initial;
	cf_atomic_int	migrate_tx_partitions_remaining;
	cf_atomic_int	migrate_tx_partitions_lead_remaining;
	cf_atomic_int	migrate_rx_partitions_initial;
	cf_atomic_int	migrate_rx_partitions_remaining;
	cf_atomic_int	migrate_signals_active;
	cf_atomic_int	migrate_signals_remaining;
	cf_atomic_int	appeals_tx_active; // relevant only for enterprise edition
	cf_atomic_int	appeals_rx_active; // relevant only for enterprise edition
	cf_atomic_int	appeals_tx_remaining; // relevant only for enterprise edition

	// Per-record migration stats:
	cf_atomic_int	migrate_records_skipped; // relevant only for enterprise edition
	cf_atomic_int	migrate_records_transmitted;
	cf_atomic_int	migrate_record_retransmits;
	cf_atomic_int	migrate_record_receives;
	cf_atomic_int	appeals_records_exonerated; // relevant only for enterprise edition

	// From-client transaction stats.

	cf_atomic64		n_client_tsvc_error;
	cf_atomic64		n_client_tsvc_timeout;

	cf_atomic64		n_client_proxy_complete;
	cf_atomic64		n_client_proxy_error;
	cf_atomic64		n_client_proxy_timeout;

	cf_atomic64		n_client_read_success;
	cf_atomic64		n_client_read_error;
	cf_atomic64		n_client_read_timeout;
	cf_atomic64		n_client_read_not_found;

	cf_atomic64		n_client_write_success;
	cf_atomic64		n_client_write_error;
	cf_atomic64		n_client_write_timeout;

	// Subset of n_client_write_... above, respectively.
	cf_atomic64		n_xdr_client_write_success;
	cf_atomic64		n_xdr_client_write_error;
	cf_atomic64		n_xdr_client_write_timeout;

	cf_atomic64		n_client_delete_success;
	cf_atomic64		n_client_delete_error;
	cf_atomic64		n_client_delete_timeout;
	cf_atomic64		n_client_delete_not_found;

	// Subset of n_client_delete_... above, respectively.
	cf_atomic64		n_xdr_client_delete_success;
	cf_atomic64		n_xdr_client_delete_error;
	cf_atomic64		n_xdr_client_delete_timeout;
	cf_atomic64		n_xdr_client_delete_not_found;

	cf_atomic64		n_client_udf_complete;
	cf_atomic64		n_client_udf_error;
	cf_atomic64		n_client_udf_timeout;

	cf_atomic64		n_client_lang_read_success;
	cf_atomic64		n_client_lang_write_success;
	cf_atomic64		n_client_lang_delete_success;
	cf_atomic64		n_client_lang_error;

	// From-proxy transaction stats.

	cf_atomic64		n_from_proxy_tsvc_error;
	cf_atomic64		n_from_proxy_tsvc_timeout;

	cf_atomic64		n_from_proxy_read_success;
	cf_atomic64		n_from_proxy_read_error;
	cf_atomic64		n_from_proxy_read_timeout;
	cf_atomic64		n_from_proxy_read_not_found;

	cf_atomic64		n_from_proxy_write_success;
	cf_atomic64		n_from_proxy_write_error;
	cf_atomic64		n_from_proxy_write_timeout;

	// Subset of n_from_proxy_write_... above, respectively.
	cf_atomic64		n_xdr_from_proxy_write_success;
	cf_atomic64		n_xdr_from_proxy_write_error;
	cf_atomic64		n_xdr_from_proxy_write_timeout;

	cf_atomic64		n_from_proxy_delete_success;
	cf_atomic64		n_from_proxy_delete_error;
	cf_atomic64		n_from_proxy_delete_timeout;
	cf_atomic64		n_from_proxy_delete_not_found;

	// Subset of n_from_proxy_delete_... above, respectively.
	cf_atomic64		n_xdr_from_proxy_delete_success;
	cf_atomic64		n_xdr_from_proxy_delete_error;
	cf_atomic64		n_xdr_from_proxy_delete_timeout;
	cf_atomic64		n_xdr_from_proxy_delete_not_found;

	cf_atomic64		n_from_proxy_udf_complete;
	cf_atomic64		n_from_proxy_udf_error;
	cf_atomic64		n_from_proxy_udf_timeout;

	cf_atomic64		n_from_proxy_lang_read_success;
	cf_atomic64		n_from_proxy_lang_write_success;
	cf_atomic64		n_from_proxy_lang_delete_success;
	cf_atomic64		n_from_proxy_lang_error;

	// Batch sub-transaction stats.

	cf_atomic64		n_batch_sub_tsvc_error;
	cf_atomic64		n_batch_sub_tsvc_timeout;

	cf_atomic64		n_batch_sub_proxy_complete;
	cf_atomic64		n_batch_sub_proxy_error;
	cf_atomic64		n_batch_sub_proxy_timeout;

	cf_atomic64		n_batch_sub_read_success;
	cf_atomic64		n_batch_sub_read_error;
	cf_atomic64		n_batch_sub_read_timeout;
	cf_atomic64		n_batch_sub_read_not_found;

	// From-proxy batch sub-transaction stats.

	cf_atomic64		n_from_proxy_batch_sub_tsvc_error;
	cf_atomic64		n_from_proxy_batch_sub_tsvc_timeout;

	cf_atomic64		n_from_proxy_batch_sub_read_success;
	cf_atomic64		n_from_proxy_batch_sub_read_error;
	cf_atomic64		n_from_proxy_batch_sub_read_timeout;
	cf_atomic64		n_from_proxy_batch_sub_read_not_found;

	// Internal-UDF sub-transaction stats.

	cf_atomic64		n_udf_sub_tsvc_error;
	cf_atomic64		n_udf_sub_tsvc_timeout;

	cf_atomic64		n_udf_sub_udf_complete;
	cf_atomic64		n_udf_sub_udf_error;
	cf_atomic64		n_udf_sub_udf_timeout;

	cf_atomic64		n_udf_sub_lang_read_success;
	cf_atomic64		n_udf_sub_lang_write_success;
	cf_atomic64		n_udf_sub_lang_delete_success;
	cf_atomic64		n_udf_sub_lang_error;

	// Transaction retransmit stats - 'all' means both client & proxy origins.

	uint64_t		n_retransmit_all_read_dup_res;

	uint64_t		n_retransmit_all_write_dup_res;
	uint64_t		n_retransmit_all_write_repl_write;

	uint64_t		n_retransmit_all_delete_dup_res;
	uint64_t		n_retransmit_all_delete_repl_write;

	uint64_t		n_retransmit_all_udf_dup_res;
	uint64_t		n_retransmit_all_udf_repl_write;

	uint64_t		n_retransmit_all_batch_sub_dup_res;

	uint64_t		n_retransmit_udf_sub_dup_res;
	uint64_t		n_retransmit_udf_sub_repl_write;

	// Scan stats.

	cf_atomic64		n_scan_basic_complete;
	cf_atomic64		n_scan_basic_error;
	cf_atomic64		n_scan_basic_abort;

	cf_atomic64		n_scan_aggr_complete;
	cf_atomic64		n_scan_aggr_error;
	cf_atomic64		n_scan_aggr_abort;

	cf_atomic64		n_scan_udf_bg_complete;
	cf_atomic64		n_scan_udf_bg_error;
	cf_atomic64		n_scan_udf_bg_abort;

	// Query stats.

	cf_atomic64		query_reqs;
	cf_atomic64		query_fail;
	cf_atomic64		query_short_queue_full;
	cf_atomic64		query_long_queue_full;
	cf_atomic64		query_short_reqs;
	cf_atomic64		query_long_reqs;

	cf_atomic64		n_lookup;
	cf_atomic64		n_lookup_success;
	cf_atomic64		n_lookup_abort;
	cf_atomic64		n_lookup_errs;
	cf_atomic64		lookup_response_size;
	cf_atomic64		lookup_num_records;

	cf_atomic64		n_aggregation;
	cf_atomic64		n_agg_success;
	cf_atomic64		n_agg_abort;
	cf_atomic64		n_agg_errs;
	cf_atomic64		agg_response_size;
	cf_atomic64		agg_num_records;

	cf_atomic64		n_query_udf_bg_success;
	cf_atomic64		n_query_udf_bg_failure;

	// Geospatial query stats:
	cf_atomic64		geo_region_query_count;		// number of region queries
	cf_atomic64		geo_region_query_cells;		// number of cells used by region queries
	cf_atomic64		geo_region_query_points;	// number of valid points found
	cf_atomic64		geo_region_query_falsepos;	// number of false positives found

	// Re-replication stats - relevant only for enterprise edition.

	cf_atomic64		n_re_repl_success;
	cf_atomic64		n_re_repl_error;
	cf_atomic64		n_re_repl_timeout;

	// Special errors that deserve their own counters:

	cf_atomic64		n_fail_xdr_forbidden;
	cf_atomic64		n_fail_key_busy;
	cf_atomic64		n_fail_generation;
	cf_atomic64		n_fail_record_too_big;

	// Special non-error counters:

	cf_atomic64		n_deleted_last_bin;

	// One-way automatically activated histograms.

	cf_hist_track*	read_hist;
	cf_hist_track*	write_hist;
	cf_hist_track*	udf_hist;
	cf_hist_track*	query_hist;
	histogram*		query_rec_count_hist;
	histogram*		re_repl_hist; // relevant only for enterprise edition

	bool			read_hist_active;
	bool			write_hist_active;
	bool			udf_hist_active;
	bool			query_hist_active;
	bool			query_rec_count_hist_active;
	bool			re_repl_hist_active; // relevant only for enterprise edition

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

	histogram*		batch_sub_start_hist;
	histogram*		batch_sub_restart_hist;
	histogram*		batch_sub_dup_res_hist;
	histogram*		batch_sub_repl_ping_hist;
	histogram*		batch_sub_read_local_hist;
	histogram*		batch_sub_response_hist;

	histogram*		udf_sub_start_hist;
	histogram*		udf_sub_restart_hist;
	histogram*		udf_sub_dup_res_hist;
	histogram*		udf_sub_master_hist; // split this?
	histogram*		udf_sub_repl_write_hist;
	histogram*		udf_sub_response_hist;

	histogram*		device_read_size_hist;
	histogram*		device_write_size_hist;

	// Histograms of object storage sizes. (Meaningful for drive-backed
	// namespaces only.)
	histogram*		obj_size_log_hist;
	histogram*		set_obj_size_log_hists[AS_SET_MAX_COUNT + 1];
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
	as_partition_version cluster_versions[AS_CLUSTER_SZ][AS_PARTITIONS];
	uint32_t rack_ids[AS_CLUSTER_SZ]; // is observed-rack-ids in CP mode

	// Quiescence - relevant only for enterprise edition.
	uint32_t active_size;
	bool pending_quiesce;
	bool is_quiesced;
	bool quiesced[AS_CLUSTER_SZ];

	// Observed nodes - relevant only for enterprise edition.
	uint32_t observed_cluster_size;
	cf_node observed_succession[AS_CLUSTER_SZ];

	// Roster management - relevant only for enterprise edition.
	uint32_t smd_roster_generation;
	uint32_t smd_roster_count;
	cf_node smd_roster[AS_CLUSTER_SZ];
	uint32_t smd_roster_rack_ids[AS_CLUSTER_SZ];
	uint32_t roster_generation;
	uint32_t roster_count;
	cf_node roster[AS_CLUSTER_SZ];
	uint32_t roster_rack_ids[AS_CLUSTER_SZ];

	// Master regimes - relevant only for enterprise edition.
	uint32_t eventual_regime;
	uint32_t rebalance_regime;
	uint32_t rebalance_regimes[AS_CLUSTER_SZ];
};

#define AS_SET_NAME_MAX_SIZE	64		// includes space for null-terminator

#define INVALID_SET_ID 0

#define IS_SET_EVICTION_DISABLED(p_set)		(cf_atomic32_get(p_set->disable_eviction) == 1)
#define DISABLE_SET_EVICTION(p_set, on_off)	(cf_atomic32_set(&p_set->disable_eviction, on_off ? 1 : 0))

typedef enum {
	AS_SET_ENABLE_XDR_DEFAULT = 0,
	AS_SET_ENABLE_XDR_TRUE = 1,
	AS_SET_ENABLE_XDR_FALSE = 2
} as_set_enable_xdr_flag;

// Caution - changing this struct could break warm or cool restart.
struct as_set_s {
	char			name[AS_SET_NAME_MAX_SIZE];
	cf_atomic64		n_objects;
	cf_atomic64		n_tombstones;		// relevant only for enterprise edition
	cf_atomic64		n_bytes_memory;		// for data-in-memory only - sets's total record data size
	cf_atomic64		stop_writes_count;	// restrict number of records in a set
	uint64_t		truncate_lut;		// records with last-update-time less than this are truncated
	cf_atomic32		disable_eviction;	// don't evict anything in this set (note - expiration still works)
	cf_atomic32		enable_xdr;			// white-list (AS_SET_ENABLE_XDR_TRUE) or black-list (AS_SET_ENABLE_XDR_FALSE) a set for XDR replication
	uint32_t		n_sindexes;
	uint8_t padding[12];
};

static inline bool
as_set_stop_writes(as_set *p_set) {
	uint64_t n_objects = cf_atomic64_get(p_set->n_objects);
	uint64_t stop_writes_count = cf_atomic64_get(p_set->stop_writes_count);

	return stop_writes_count != 0 && n_objects >= stop_writes_count;
}

// These bin functions must be below definition of struct as_namespace_s:

static inline bool
as_bin_set_id_from_name_w_len(as_namespace *ns, as_bin *b, const uint8_t *buf,
		size_t len) {
	return as_bin_get_or_assign_id_w_len(ns, (const char *)buf, len, &b->id);
}

static inline size_t
as_bin_memcpy_name(as_namespace *ns, uint8_t *buf, as_bin *b) {
	size_t len = 0;

	if (! ns->single_bin) {
		const char *name = as_bin_get_name_from_id(ns, b->id);

		len = strlen(name);
		memcpy(buf, name, len);
	}

	return len;
}

// forward ref
struct as_msg_field_s;

/* Namespace function declarations */
extern as_namespace *as_namespace_create(char *name);
extern void as_namespaces_init(bool cold_start_cmd, uint32_t instance);
extern void as_namespaces_setup(bool cold_start_cmd, uint32_t instance);
extern void as_namespace_finish_setup(as_namespace *ns, uint32_t instance);
extern bool as_namespace_configure_sets(as_namespace *ns);
extern as_namespace *as_namespace_get_byname(char *name);
extern as_namespace *as_namespace_get_byid(uint32_t id);
extern as_namespace *as_namespace_get_bybuf(uint8_t *name, size_t len);
extern as_namespace *as_namespace_get_bymsgfield(struct as_msg_field_s *fp);
extern const char *as_namespace_get_set_name(as_namespace *ns, uint16_t set_id);
extern uint16_t as_namespace_get_set_id(as_namespace *ns, const char *set_name);
extern uint16_t as_namespace_get_create_set_id(as_namespace *ns, const char *set_name);
extern int as_namespace_set_set_w_len(as_namespace *ns, const char *set_name, size_t len, uint16_t *p_set_id, bool apply_restrictions);
extern int as_namespace_get_create_set_w_len(as_namespace *ns, const char *set_name, size_t len, as_set **pp_set, uint16_t *p_set_id);
extern as_set *as_namespace_get_set_by_name(as_namespace *ns, const char *set_name);
extern as_set* as_namespace_get_set_by_id(as_namespace* ns, uint16_t set_id);
extern as_set* as_namespace_get_record_set(as_namespace *ns, const as_record *r);
extern void as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db);
extern void as_namespace_adjust_set_memory(as_namespace *ns, uint16_t set_id, int64_t delta_bytes);
extern void as_namespace_release_set_id(as_namespace *ns, uint16_t set_id);
extern void as_namespace_get_bins_info(as_namespace *ns, cf_dyn_buf *db, bool show_ns);
extern void as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name, cf_dyn_buf *db);

static inline bool
as_namespace_cool_restarts(const as_namespace *ns)
{
	return ns->storage_data_in_memory && ! ns->data_in_index;
}

static inline uint32_t
as_namespace_device_count(const as_namespace *ns)
{
	// Only one of them will ever be non-zero.
	return ns->n_storage_devices + ns->n_storage_files;
}

static inline const char*
as_namespace_start_mode_str(const as_namespace *ns)
{
	return as_namespace_cool_restarts(ns) ? "cool" : "warm";
}

static inline bool
as_namespace_index_persisted(const as_namespace *ns)
{
	return ns->xmem_type == CF_XMEM_TYPE_PMEM ||
			ns->xmem_type == CF_XMEM_TYPE_FLASH;
}

// Persistent Memory Management
void as_namespace_xmem_shutdown(as_namespace *ns, uint32_t instance);
