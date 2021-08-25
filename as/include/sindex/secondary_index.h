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

#include "ai_types.h"

#include "arenax.h"
#include "dynbuf.h"
#include "hist.h"
#include "shash.h"

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"

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
#define AS_INDEX_KEYS_PER_ARR 204

#define MIN_PARTITIONS_PER_SINDEX 1
#define MAX_PARTITIONS_PER_SINDEX 256

#define DEFAULT_PARTITIONS_PER_SINDEX 32

#define AS_SINDEX_INACTIVE 1
#define AS_SINDEX_ACTIVE 2
#define AS_SINDEX_DESTROY 3

#define AS_SINDEX_LOOKUP_SKIP_ACTIVE_CHECK 0x01
#define AS_SINDEX_LOOKUP_SKIP_RESERVATION 0x02
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
	AS_SINDEX_ERR_UNKNOWN_KEYTYPE  = -14,
	AS_SINDEX_ERR_BIN_NOTFOUND     = -13,

	// Needed when attempting index create/query
	AS_SINDEX_ERR_FOUND            = -6,
	AS_SINDEX_ERR_NOTFOUND         = -5,
	AS_SINDEX_ERR_NO_MEMORY        = -4,
	AS_SINDEX_ERR_PARAM            = -3,
	AS_SINDEX_ERR_NOT_READABLE     = -2,
	AS_SINDEX_ERR                  = -1,
	AS_SINDEX_OK                   =  0,

	// Internal Not needed
	AS_SINDEX_CONTINUE             = 1,
	AS_SINDEX_DONE                 = 2,
	// Needed when inserting object in the btree.
	AS_SINDEX_KEY_FOUND            = 3,
	AS_SINDEX_KEY_NOTFOUND         = 4
} as_sindex_status;
// **************************************************************************************************

/*
 * SINDEX OP TYPES.
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_OP_DELETE = 0,
	AS_SINDEX_OP_INSERT = 1,
	AS_SINDEX_OP_READ = 2
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
 * STATS AND CONFIG STRUCTURE
 * Stats are collected about memory utilization based on simple index
 * overhead. Any insert delete from the secondary index would update
 * this number and the memory management folks has to use this info.
 */
// **************************************************************************************************
typedef struct as_sindex_stat_s {
	cf_atomic64        n_objects;

	cf_atomic64        n_writes;
	cf_atomic64        write_errs;
	histogram *        _write_hist;         // Histogram to track time spend writing to the sindex
	histogram *        _si_prep_hist;

	cf_atomic64        n_deletes;
	cf_atomic64        delete_errs;
	histogram *        _delete_hist;        // Histogram to track time spend deleting from sindex

	// Background thread stats
	cf_atomic64        loadtime;
	uint32_t           populate_pct;

	cf_atomic64        n_defrag_records;
	
	// Query Stats
	histogram *       _query_hist;            // Histogram to track query latency
	histogram *       _query_batch_lookup;    // Histogram to track latency of batch request from sindex tree.
	histogram *       _query_batch_io;        // Histogram to track time spend doing I/O per batch

	// Query basic stats
	cf_atomic64        n_query_basic_complete;
	cf_atomic64        n_query_basic_error;
	cf_atomic64        n_query_basic_abort;
	cf_atomic64        n_query_basic_records;

	histogram *       _query_rcnt_hist;       // Histogram to track record counts from queries
	histogram *       _query_diff_hist;       // Histogram to track the false positives found by queries
} as_sindex_stat;

// **************************************************************************************************


/*
 * SINDEX METADATAS
 */
// **************************************************************************************************
typedef struct as_sindex_physical_metadata_s {
	pthread_rwlock_t    slock;
	struct btree       *ibtr;
} as_sindex_pmetadata;


typedef struct as_sindex_path_s {
	as_particle_type type;  // MAP/LIST
	union {
		uint32_t index;     // For index of lists.
		char   * key_str;   // For string type keys in maps.
		uint64_t key_int;   // For integer type keys in maps.
	} value;
	as_particle_type mapkey_type;  // This could be either string or integer type
} as_sindex_path;

typedef struct as_sindex_metadata_s {
	as_sindex_pmetadata * pimd;

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
	bool enable_histogram;

	as_namespace *ns;

	// Protected by si reference.
	struct as_sindex_metadata_s *imd;
	struct as_sindex_metadata_s *recreate_imd;

	as_sindex_stat stats;
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
	union {                       // we use this if we need to store only one value inside sbin.
		int64_t       int_val;    // accessing this is much faster than accessing any other value
		cf_digest     str_val;    // value on the stack.
	} value;
	uint32_t          num_values;
	void            * values;     // If there are more than 1 value in the sbin, we use this to
	as_particle_type  type;       // point to them. the type of data which is going to get indexed
	as_sindex_op      op;         // (STRING or INTEGER). Should we delete or insert this values
	bool              to_free;    // from/into the secondary index tree. If the values are malloced.
	as_sindex       * si;         // si_ix of the si this bin is pointing to.
	sbin_value_pool * stack_buf;
	uint32_t          heap_capacity;
} as_sindex_bin;

// Caution: Using this will waste 12 bytes per long type skey 
typedef struct as_sindex_key_s {
	union {
		cf_digest str_key;
		uint64_t  int_key;
	} key;
} as_sindex_key;
// **************************************************************************************************


// **************************************************************************************************

/*
 * STRUCTUES FOR QUERY MODULE
 */
// **************************************************************************************************
struct ai_obj;
typedef struct as_sindex_qctx_s {
	uint64_t bsize;
	cf_ll *recl;
	uint64_t n_recs;

	int range_index;

	// Physical Tree offset.
	bool new_ibtr; // if new tree
	int pimd_ix;

	// IBTR offset.
	bool nbtr_done; // if nbtr done resume from key next to ibtr_last_key
	struct ai_obj *ibtr_last_key; // offset in ibtr

	// NBTR offset.
	uint64_t nbtr_last_key;

	cf_shash *r_h_hash;

	// Cache information about query-able partitions.
	as_index_tree *reserved_trees[AS_PARTITIONS]; // no lock, no parallel access
} as_sindex_qctx;

typedef struct as_query_range_start_end_s {
	int64_t start;
	int64_t end;  // -1 means infinity
} as_query_range_start_end;

typedef struct as_query_geo_range_s {
	uint64_t cellid;  // target of regions-containing-point query
	geo_region_t region;  // target of points-in-region query
	as_query_range_start_end *r;
	uint8_t num_r;
} as_query_geo_range;

typedef struct as_query_range_s {
	union {
		as_query_range_start_end r;
		as_query_geo_range geo;
		cf_digest digest;
	} u;

	uint32_t bin_id;
	as_particle_type bin_type;
	as_sindex_type itype;
	bool isrange;
	char bin_path[AS_SINDEX_MAX_PATH_LENGTH];
} as_query_range;

/*
 * sindex_keys  are used by Secondary index queries to validate the keys against
 * the values of bins
 * ALl the jobs which runs over these queries also uses them
 * Like - Aggregation Query
 */
typedef struct as_index_keys_arr_s {
	uint32_t num;
	union {
		uint64_t handles[AS_INDEX_KEYS_PER_ARR];
		cf_digest digests[AS_INDEX_KEYS_PER_ARR];
	} u;
} as_index_keys_arr;

COMPILER_ASSERT(sizeof(as_index_keys_arr) <= 4096);

typedef struct as_index_keys_ll_element_s {
	cf_ll_element ele;
	as_index_keys_arr *keys_arr;
} as_index_keys_ll_element;


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
extern void as_sindex_destroy_pmetadata(as_sindex *si);
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
void as_sindex_process_ret(as_sindex *si, int ret, as_sindex_op op, uint64_t starttime, int pos);
uint32_t as_sindex_arr_lookup_by_set_binid_lockfree(const as_namespace *ns, const char *set, uint32_t binid, as_sindex **si_arr);
as_sindex * as_sindex_lookup_by_iname(const as_namespace *ns, const char *iname, char flag);
as_sindex * as_sindex_lookup_by_defn(const as_namespace *ns, const char *set, uint32_t binid, as_sindex_ktype type, as_sindex_type itype, const char *path, char flag);

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
 * HISTOGRAMS
 */
// **************************************************************************************************
extern int as_sindex_histogram_enable(as_namespace *ns, char * iname, bool enable);
extern void as_sindex_histogram_dumpall(as_namespace *ns);
#define SINDEX_HIST_INSERT_DATA_POINT(si, type, start_time_ns)                          \
do {                                                                                    \
	if (si->enable_histogram && start_time_ns != 0) {                                   \
		histogram_insert_data_point(si->stats._ ##type, start_time_ns);                 \
	}                                                                                   \
} while(0);

#define SINDEX_HIST_INSERT_RAW(si, type, value)                                         \
do {                                                                                    \
	if (si->enable_histogram) {                                                         \
		histogram_insert_raw(si->stats._ ##type, value);                                \
	}                                                                                   \
} while(0);
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

#define PIMD_RLOCK(l)                                                          \
do {                                                                           \
	int lock_rv = pthread_rwlock_rdlock(l);                                    \
	if (lock_rv) {                                                             \
		cf_warning(AS_SINDEX, "PRLOCK %d %s:%d", lock_rv, __FILE__, __LINE__); \
	}                                                                          \
} while (false)

#define PIMD_WLOCK(l)                                                          \
do {                                                                           \
	int lock_rv = pthread_rwlock_wrlock(l);                                    \
	if (lock_rv) {                                                             \
		cf_warning(AS_SINDEX, "PWLOCK %d %s:%d", lock_rv, __FILE__, __LINE__); \
	}                                                                          \
} while (false)

#define PIMD_RUNLOCK(l)                                                        \
do {                                                                           \
	int lock_rv = pthread_rwlock_unlock(l);                                    \
	if (lock_rv) {                                                             \
		cf_warning(AS_SINDEX, "PRUNLOCK %d %s:%d",lock_rv, __FILE__, __LINE__);\
	}                                                                          \
} while (false)

#define PIMD_WUNLOCK(l)                                                         \
do {                                                                            \
	int lock_rv = pthread_rwlock_unlock(l);                                     \
	if (lock_rv) {                                                              \
		cf_warning(AS_SINDEX, "PWUNLOCK %d %s:%d", lock_rv, __FILE__, __LINE__);\
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

static inline int
as_sindex_get_pimd_ix(const as_sindex_metadata *imd, const void *buf)
{
	return C_IS_DG(imd->sktype) ?
			(int)*(uint128 *)buf % imd->n_pimds :
			(int)*(int64_t *)buf % imd->n_pimds;
}
