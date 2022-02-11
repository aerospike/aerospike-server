/*
 * secondary_index.h
 *
 * Copyright (C) 2012-2021 Aerospike, Inc.
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
 *  SYNOPSIS
 *  Abstraction to support secondary indexes with multiple implementations.
 */

#pragma once

#include "base/datamodel.h"
#include "base/index.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/smd.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"

#include "arenax.h"
#include "dynbuf.h"
#include "shash.h"

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hash_math.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// **************************************************************************************************
// TODO: half of these used in single source file only
#define AS_SINDEX_MAX 256
#define AS_SINDEX_MAX_STRING_KSIZE 2048
#define AS_SINDEX_MAX_GEOJSON_KSIZE (1024 * 1024)
#define SINDEX_SMD_KEY_SIZE (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + AS_SINDEX_MAX_PATH_LENGTH + 1 + 2 + 2)
#define AS_SINDEX_MAX_PATH_LENGTH 256
#define AS_SINDEX_MAX_DEPTH 10
#define AS_SINDEX_TYPE_STR_SIZE 20 // LIST / MAPKEYS / MAPVALUES / DEFAULT(NONE)
#define AS_SINDEXDATA_STR_SIZE AS_SINDEX_MAX_PATH_LENGTH + 1 + 8 // binpath + separator (,) + keytype (string/numeric)

#define MIN_PARTITIONS_PER_SINDEX 1
#define MAX_PARTITIONS_PER_SINDEX 256

#define DEFAULT_PARTITIONS_PER_SINDEX 32

#define AS_SINDEX_INACTIVE 1
#define AS_SINDEX_ACTIVE 2
#define AS_SINDEX_DESTROY 3

#define AS_SINDEX_LOOKUP_SKIP_ACTIVE_CHECK 0x01
#define AS_SINDEX_LOOKUP_SKIP_RESERVATION 0x02

// FIXME - where best for these?
#define COL_TYPE_INVALID  0
#define COL_TYPE_LONG     1
#define COL_TYPE_DIGEST   2
#define COL_TYPE_GEOJSON  3
#define COL_TYPE_MAX      4
// **************************************************************************************************

/* 
 * Return status codes for index object functions.
 *
 * NB: When adding error code add the string in the as_sindex_err_str 
 * in secondary_index.c 
 *
 * Negative > 10 are the ones which show up and goes till client
 *
 * Positive are < 10 are something which are internal
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_ERR_MAXCOUNT         = -16,
	AS_SINDEX_ERR_UNKNOWN_KEYTYPE  = -14, // unused
	AS_SINDEX_ERR_BIN_NOTFOUND     = -13, // unused

	// Needed when attempting index create/query
	AS_SINDEX_ERR_FOUND            = -6,
	AS_SINDEX_ERR_NOTFOUND         = -5,
	AS_SINDEX_ERR_NO_MEMORY        = -4, // unused
	AS_SINDEX_ERR_PARAM            = -3, // unused
	AS_SINDEX_ERR_NOT_READABLE     = -2, // unused
	AS_SINDEX_ERR                  = -1, // ? not used by name
	AS_SINDEX_OK                   =  0
} as_sindex_status;
// **************************************************************************************************

/*
 * SINDEX OP TYPES.
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_OP_DELETE = 0,
	AS_SINDEX_OP_INSERT = 1
} as_sindex_op;
// **************************************************************************************************

/*
 * SECONDARY INDEX KEY TYPES same as COL_TYPE*
 */
// **************************************************************************************************
typedef uint8_t as_sindex_ktype;
// **************************************************************************************************

/*
 * SINDEX TYPES.
 * THEY WOULD BE IN SYNC WITH THE CLIENTS.
 * Do not change the order of this enum, keep in sync with
 * secondary_index.c:sindex_itypes.
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_ITYPE_DEFAULT   = 0,
	AS_SINDEX_ITYPE_LIST      = 1,
	AS_SINDEX_ITYPE_MAPKEYS   = 2,
	AS_SINDEX_ITYPE_MAPVALUES = 3,
	AS_SINDEX_ITYPE_MAX       = 4
} as_sindex_type;
// **************************************************************************************************

/* 
 * STRUCTURES FROM ALCHEMY
 */
// *****************************
struct btree;
// **************************************************************************************************


/*
 * SINDEX METADATAS
 */
// **************************************************************************************************

typedef struct as_sindex_path_s {
	as_particle_type type;  // MAP/LIST
	union {
		uint32_t index;     // For index of lists.
		char   * key_str;   // For string type keys in maps.
		uint64_t key_int;   // For integer type keys in maps.
	} value;
	as_particle_type mapkey_type;  // This could be either string or integer type
} as_sindex_path;

struct si_btree_s;

typedef struct as_sindex_metadata_s {
	struct si_btree_s** btrees;

	// Static Data. Does not need protection
	struct as_sindex_s  * si;
	char                * ns_name;
	char                * set;
	char                * iname;
	char                * bname;
	uint32_t              binid; // Redundant info to aid search
	as_sindex_ktype       sktype; // Same as Aerospike Index type
	as_sindex_type        itype;
	as_sindex_path        path[AS_SINDEX_MAX_DEPTH];
	int                   path_length;
	char                * path_str;
	uint32_t              n_pimds;
} as_sindex_metadata;

typedef struct as_sindex_s {
	uint32_t si_ix; // position of self - used for validation
	uint8_t state; // protected by SI_GWLOCK
	bool readable; // false while building sindex

	as_namespace *ns;

	// Protected by si reference.
	struct as_sindex_metadata_s *imd;
	struct as_sindex_metadata_s *recreate_imd;

	uint64_t load_time;
	uint32_t populate_pct;
	uint64_t n_defrag_records;
} as_sindex;

// **************************************************************************************************
/*
 * SBINS STRUCTURES
 */
typedef struct sbin_value_pool_s{
	uint32_t used_sz;
	uint8_t  *value;
} sbin_value_pool;

#define AS_SINDEX_VALUESZ_ON_STACK (16 * 1000)
#define SINDEX_BINS_SETUP(skey_bin, size)                      \
	sbin_value_pool value_pool;                                    \
	value_pool.value   = alloca(AS_SINDEX_VALUESZ_ON_STACK);       \
	value_pool.used_sz = 0;                    \
	as_sindex_bin skey_bin[(size)];                            \
	for (int id = 0; id < (size); id++) {         \
			skey_bin[id].si = NULL;         \
			skey_bin[id].stack_buf = &value_pool; \
	}

/*
 * Used as structure to call into secondary indexes sindex_* interface
 * TODO: as_sindex_bin is not appropriate name for this structure.
 * maybe as_sindex_transaction 
 */
typedef struct as_sindex_bin_s {
	int64_t           val;    // accessing this is much faster than accessing any other value

	uint32_t          num_values;
	int64_t         * values;     // If there are more than 1 value in the sbin, we use this to
	as_particle_type  type;       // point to them. the type of data which is going to get indexed
	as_sindex_op      op;         // (STRING or INTEGER). Should we delete or insert this values
	bool              to_free;    // from/into the secondary index tree. If the values are malloced.
	as_sindex       * si;         // si_ix of the si this bin is pointing to.
	sbin_value_pool * stack_buf;
	uint32_t          heap_capacity;
} as_sindex_bin;
// **************************************************************************************************


// **************************************************************************************************

// APIs exposed to other modules
// TODO return values is actually enum. 

/*
 * MODULE INIT AND SHUTDOWN
 */
// **************************************************************************************************

/* Index abstraction layer functions. */
/*
 * Initialize an instantiation of the index abstraction layer
 * using the array of index type-specific parameters passed in.
 *
 * All indexes created during this instantiation will use these type-specific
 * parameters (e.g., maximum data structure sizes, allocation policies, and any
 * other tuning parameters.)
 *
 * Call once before creating any type of index object.
 */
void as_sindex_init(void);
void as_sindex_load(void);
void as_sindex_start(void);


// **************************************************************************************************

/* 
 * DDL AND METADATA QUERY
 * 
*/
// **************************************************************************************************
extern void as_sindex_create_lockless(as_namespace *ns, as_sindex_metadata *imd);
// **************************************************************************************************


/*
 * CREATION AND UPDATION OF SINDEX BIN 
 */
// **************************************************************************************************
extern uint32_t as_sindex_sbins_from_bin(as_namespace *ns, const char *set, const as_bin *b, as_sindex_bin *start_sbin, as_sindex_op op);
extern void as_sindex_update_by_sbin(as_sindex_bin *start_sbin, uint32_t num_sbins, cf_arenax_handle r_h);
extern uint32_t as_sindex_sbins_populate(as_sindex_bin *sbins, as_namespace *ns, const char *set_name, const as_bin *b_old, const as_bin *b_new);
// **************************************************************************************************


/*
 * DMLs USING RECORDS
 */
// **************************************************************************************************
bool  as_sindex_put_rd(as_sindex *si, as_storage_rd *rd, struct as_index_ref_s *r_ref);
void as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd, struct as_index_ref_s *r_ref);
// **************************************************************************************************


/* 
 * UTILS
 */
// **************************************************************************************************
const char * as_sindex_err_str(as_sindex_status status);
uint8_t as_sindex_err_to_clienterr(int err, char *fname, int lineno);
bool as_sindex_isactive(const as_sindex *si);
bool as_sindex_can_query(const as_sindex *si);
void as_sindex_delete_defn(as_namespace *ns, as_sindex_metadata *imd);
as_val *as_sindex_extract_val_from_path(const as_sindex_metadata *imd, as_val *v);
bool as_sindex_can_defrag_record(const as_namespace *ns, cf_arenax_handle r_h);
bool as_sindex_extract_bin_path(as_sindex_metadata *imd, const char *path_str);
as_sindex_status as_sindex_create_check_params(const as_namespace *ns, const as_sindex_metadata *imd);
as_particle_type as_sindex_pktype(const as_sindex_metadata *imd);
const char * as_sindex_ktype_str(as_sindex_ktype type);
as_sindex_ktype as_sindex_ktype_from_string(const char *type_str);
as_sindex_ktype as_sindex_sktype_from_pktype(as_particle_type t);
as_sindex_type as_sindex_itype_from_string(const char *itype_str);
uint32_t as_sindex_arr_lookup_by_set_binid_lockfree(const as_namespace *ns, const char *set, uint16_t binid, as_sindex **si_arr);
as_sindex * as_sindex_lookup_by_iname(const as_namespace *ns, const char *iname, char flag);
as_sindex * as_sindex_lookup_by_defn(const as_namespace *ns, const char *set, uint16_t binid, as_sindex_ktype type, as_sindex_type itype, const char *path, char flag);

#define AS_SINDEX_ERR_TO_CLIENTERR(err) \
	as_sindex_err_to_clienterr(err, __FILE__, __LINE__)

// **************************************************************************************************

/*
 * INFO AND CONFIGS
 */
// **************************************************************************************************
extern void as_sindex_list_str(const as_namespace *ns, cf_dyn_buf *db);
extern int as_sindex_stats_str(as_namespace *ns, char * iname, cf_dyn_buf *db);
extern void as_sindex_gconfig_default(struct as_config_s *c);
// **************************************************************************************************

/*
 * RESERVE, RELEASE AND FREE
 */
// **************************************************************************************************
extern void as_sindex_reserve(as_sindex *si);
extern void as_sindex_release(as_sindex *si);
extern void as_sindex_imd_free(as_sindex_metadata *imd);
extern void as_sindex_sbin_freeall(as_sindex_bin *sbin, uint32_t numval);
extern void as_sindex_release_arr(as_sindex *si_arr[], uint32_t si_arr_sz);
// **************************************************************************************************

/*
 * SINDEX LOCKS
 */
// **************************************************************************************************
extern pthread_rwlock_t g_sindex_rwlock;

#define SINDEX_GRLOCK()                                                        \
do {                                                                           \
	int lock_rv = pthread_rwlock_rdlock(&g_sindex_rwlock);                     \
	if (lock_rv) {                                                             \
		cf_warning(AS_SINDEX, "GRLOCK %d %s:%d", lock_rv, __FILE__, __LINE__); \
	}                                                                          \
} while (false)

#define SINDEX_GWLOCK()                                                        \
do {                                                                           \
	int lock_rv = pthread_rwlock_wrlock(&g_sindex_rwlock);                     \
	if (lock_rv) {                                                             \
		cf_warning(AS_SINDEX, "GWLOCK %d %s:%d", lock_rv, __FILE__, __LINE__); \
	}                                                                          \
} while (false)

#define SINDEX_GRUNLOCK()                                                       \
do {                                                                            \
	int lock_rv = pthread_rwlock_unlock(&g_sindex_rwlock);                      \
	if (lock_rv) {                                                              \
		cf_warning(AS_SINDEX, "GRUNLOCK %d %s:%d", lock_rv, __FILE__, __LINE__);\
	}                                                                           \
} while (false)

#define SINDEX_GWUNLOCK()                                                       \
do {                                                                            \
	int lock_rv = pthread_rwlock_unlock(&g_sindex_rwlock);                      \
	if (lock_rv) {                                                              \
		cf_warning(AS_SINDEX, "GWUNLOCK %d %s:%d", lock_rv, __FILE__, __LINE__);\
	}                                                                           \
} while (false)

// **************************************************************************************************

/*
 * APIs for SMD
 */
// **************************************************************************************************
extern void as_sindex_imd_to_smd_key(const as_sindex_metadata *imd, char *smd_key);
extern bool as_sindex_iname_to_smd_key(const as_namespace *ns, const char *iname, char *smd_key);
// **************************************************************************************************

static inline int64_t
as_sindex_string_to_bval(const char* s, size_t len)
{
	return (int64_t)cf_wyhash64((const void*)s, len);
}
