/*
 * secondary_index.c
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
 * SYNOPSIS
 * Abstraction to support secondary indexes with multiple implementations.
 * Currently there are two variants of secondary indexes supported.
 *
 * -  Aerospike Index B-tree, this is full fledged index implementation and
 *    maintains its own metadata and data structure for list of those indexes.
 *
 * -  Citrusleaf foundation indexes which are bare bone tree implementation
 *    with ability to insert delete update indexes. For these the current code
 *    manage all the data structure to manage different trees. [Will be
 *    implemented when required]
 *
 * This file implements all the translation function which can be called from
 * citrusleaf to prepare to do the operations on secondary index. Also
 * implements locking to make Aerospike Index (single threaded) code multi threaded.
 *
 */

/* Code flow --
 *
 * DDLs
 *
 * as_sindex_create --> ai_btree_create
 *
 * as_sindex_destroy --> Releases the si and change the state to AS_SINDEX_DESTROY
 *
 * BOOT INDEX
 *
 * as_sindex_boot_populateall --> If fast restart or data in memory and load at start up --> as_sbld_build_all
 *
 * SBIN creation
 *
 * as_sindex_sbins_from_rd  --> (For every bin in the record) as_sindex_sbins_from_bin
 *
 * as_sindex_sbins_from_bin -->  sbins_from_bin_buf
 *
 * sbins_from_bin_buf --> (For every macthing sindex) --> sbin_from_sindex
 *
 * sbin_from_sindex --> (If bin value macthes with sindex defn) --> as_sindex_add_asval_to_itype_sindex
 *
 * SBIN updates
 *
 * as_sindex_update_by_sbin --> For every sbin --> op_by_sbin
 *
 * as_sindex__op_by_sbin --> If op == AS_SINDEX_OP_INSERT --> ai_btree_put
 *                       |
 *                       --> If op == AS_SINDEX_OP_DELETE --> ai_btree_delete
 *
 * DMLs using RECORD
 *
 * as_sindex_put_rd --> For each bin in the record --> sbin_from_sindex
 *
 * as_sindex_putall_rd --> For each sindex --> as_sindex_put_rd
 *
 */

#include "sindex/secondary_index.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "aerospike/as_arraylist.h"
#include "aerospike/as_arraylist_iterator.h"
#include "aerospike/as_atomic.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_hashmap.h"
#include "aerospike/as_hashmap_iterator.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_pair.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"

#include "arenax.h"
#include "cf_str.h"
#include "cf_thread.h"
#include "log.h"
#include "shash.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/smd.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "sindex/gc.h"
#include "sindex/populate.h"
#include "transaction/udf.h"

#include "ai_btree.h"
#include "ai_types.h"
#include "bt_iterator.h"

#include "warnings.h"


#define SINDEX_CRASH(str, ...) \
	cf_crash(AS_SINDEX, "SINDEX_ASSERT: "str, ##__VA_ARGS__);

#define PROP_KEY_SIZE (AS_SET_NAME_MAX_SIZE + 20) // setname_binid_typeid
#define SKEY_MAX_SIZE MAX(sizeof(cf_digest), sizeof(int64_t))

pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;


// ************************************************************************************************
//                                        BINID HAS SINDEX
// Maintains a bit array where binid'th bit represents the existence of atleast one index over the
// bin with bin id as binid.
// Set, reset should be called under SINDEX_GWLOCK
// get should be called under SINDEX_GRLOCK

static void
binid_set_bit(as_namespace *ns, uint32_t binid)
{
	uint32_t index = binid / 32;

	uint32_t temp = ns->sindex_binid_bitmap[index];
	temp |= (1U << (binid % 32));

	ns->sindex_binid_bitmap[index] = temp;
}

static void
binid_clear_bit(as_namespace *ns, uint32_t binid)
{
	for (uint32_t i = 0, a = 0; i < AS_SINDEX_MAX && a < ns->sindex_cnt; i++) {
		as_sindex *si = &ns->sindex[i];

		if (as_sindex_isactive(si)) {
			a++;

			if (si->imd->binid == binid) {
				return;
			}
		}
	}

	uint32_t index = binid / 32;

	uint32_t temp = ns->sindex_binid_bitmap[index];
	temp &= ~(1U << (binid % 32));

	ns->sindex_binid_bitmap[index] = temp;
}

static bool
binid_bit_is_set(const as_namespace *ns, uint32_t binid)
{
	uint32_t index = binid / 32;

	uint32_t temp = ns->sindex_binid_bitmap[index];

	return (temp & (1U << (binid % 32))) != 0;
}

//                                     END - BINID HAS SINDEX
// ************************************************************************************************
// ************************************************************************************************
//                                             UTILITY
// Translation from sindex error code to string. In alphabetic order
const char *
as_sindex_err_str(as_sindex_status status) {
	switch (status) {
		case AS_SINDEX_ERR:                     return "ERR GENERIC";
		case AS_SINDEX_ERR_BIN_NOTFOUND:        return "BIN NOT FOUND";
		case AS_SINDEX_ERR_FOUND:               return "INDEX FOUND";
		case AS_SINDEX_ERR_MAXCOUNT:            return "INDEX COUNT EXCEEDS MAX LIMIT";
		case AS_SINDEX_ERR_NOTFOUND:            return "NO INDEX";
		case AS_SINDEX_ERR_NOT_READABLE:        return "INDEX NOT READABLE";
		case AS_SINDEX_ERR_NO_MEMORY:           return "NO MEMORY";
		case AS_SINDEX_ERR_PARAM:               return "ERR PARAM";
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:     return "UNKNOWN KEYTYPE";
		case AS_SINDEX_OK:                      return "OK";
		default:                                return "Unknown Code";
	}
}

bool
as_sindex_isactive(const as_sindex *si)
{
	return si->state == AS_SINDEX_ACTIVE;
}

bool
as_sindex_can_query(const as_sindex *si)
{
	return si->readable;
}

// Translation from sindex internal error code to generic client visible Aerospike error code
uint8_t
as_sindex_err_to_clienterr(int err, char *fname, int lineno) {
	switch (err) {
		case AS_SINDEX_ERR_FOUND:        return AS_ERR_SINDEX_FOUND;
		case AS_SINDEX_ERR_MAXCOUNT:     return AS_ERR_SINDEX_MAX_COUNT;
		case AS_SINDEX_ERR_NOTFOUND:     return AS_ERR_SINDEX_NOT_FOUND;
		case AS_SINDEX_ERR_NOT_READABLE: return AS_ERR_SINDEX_NOT_READABLE;
		case AS_SINDEX_ERR_NO_MEMORY:    return AS_ERR_SINDEX_OOM;
		case AS_SINDEX_ERR_PARAM:        return AS_ERR_PARAMETER;
		case AS_SINDEX_OK:               return AS_OK;

		// Defensive internal error
		case AS_SINDEX_ERR:
		case AS_SINDEX_ERR_BIN_NOTFOUND:
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:
		default: cf_warning(AS_SINDEX, "%s %d Error at %s,%d",
				as_sindex_err_str(err), err, fname, lineno);
		return AS_ERR_SINDEX_GENERIC;
	}
}

static bool
setname_match(const as_sindex_metadata *imd, const char *setname)
{
	if (imd->set != NULL) {
		return setname == NULL ? false : strcmp(imd->set, setname) == 0;
	}
	else {
		return setname == NULL;
	}
}

// No record lock - once gen == 0 it can never go back.
// No tree reservation - tree is destroyed only in next gc cycle.
bool
as_sindex_can_defrag_record(const as_namespace *ns, cf_arenax_handle r_h)
{
	as_index *r = (as_index *)cf_arenax_resolve(ns->arena, r_h);

	if (r->generation == 0) {
		return true;
	}

	uint32_t pid = as_partition_getid(&r->keyd);

	return ns->si_gc_tlist_map[pid][r->tree_id];
}

as_particle_type
as_sindex_pktype(const as_sindex_metadata *imd)
{
	switch (imd->sktype) {
		case COL_TYPE_LONG: {
			return AS_PARTICLE_TYPE_INTEGER;
		}
		case COL_TYPE_DIGEST: {
			return AS_PARTICLE_TYPE_STRING;
		}
		case COL_TYPE_GEOJSON: {
			return AS_PARTICLE_TYPE_GEOJSON;
		}
		default: {
			cf_warning(AS_SINDEX, "UNKNOWN KEY TYPE FOUND. VERY BAD STATE");
			return AS_PARTICLE_TYPE_BAD;
		}
	}
}

/*
 * Function as_sindex_key_str
 *     Returns a static string representing the key type
 *
 */
char const *
as_sindex_ktype_str(as_sindex_ktype type)
{
	switch (type) {
	case COL_TYPE_LONG:    return "NUMERIC";
	case COL_TYPE_DIGEST:  return "STRING";
	case COL_TYPE_GEOJSON: return "GEOJSON";
	default:
		cf_warning(AS_SINDEX, "UNSUPPORTED KEY TYPE %d", type);
		return "??????";
	}
}

as_sindex_ktype
as_sindex_ktype_from_string(const char *type_str)
{
	if (strcasecmp(type_str, "string") == 0) {
		return COL_TYPE_DIGEST;
	}

	if (strcasecmp(type_str, "numeric") == 0) {
		return COL_TYPE_LONG;
	}

	if (strcasecmp(type_str, "geo2dsphere") == 0) {
		return COL_TYPE_GEOJSON;
	}

	cf_warning(AS_SINDEX, "invalid key type %s", type_str);
	return COL_TYPE_INVALID;
}

as_sindex_ktype
as_sindex_sktype_from_pktype(as_particle_type t)
{
	switch (t) {
	case AS_PARTICLE_TYPE_INTEGER: return COL_TYPE_LONG;
	case AS_PARTICLE_TYPE_STRING: return COL_TYPE_DIGEST;
	case AS_PARTICLE_TYPE_GEOJSON: return COL_TYPE_GEOJSON;
	default: return COL_TYPE_INVALID;
	}
}

// Keep in sync with as_sindex_type.
static const char *sindex_itypes[] =
{	"DEFAULT", "LIST", "MAPKEYS", "MAPVALUES"
};

as_sindex_type
as_sindex_itype_from_string(const char *itype_str) {
	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_DEFAULT]) == 0) {
		return AS_SINDEX_ITYPE_DEFAULT;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_LIST]) == 0) {
		return AS_SINDEX_ITYPE_LIST;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_MAPKEYS]) == 0) {
		return AS_SINDEX_ITYPE_MAPKEYS;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_MAPVALUES]) == 0) {
		return AS_SINDEX_ITYPE_MAPVALUES;
	}

	return AS_SINDEX_ITYPE_MAX;
}

/*
 * Create duplicate copy of sindex metadata. New lock is created
 * used by index create by user at runtime or index creation at the boot time
 */
static void
clone_meta(const as_sindex_metadata *imd, as_sindex_metadata **qimd)
{
	if (!imd) return;

	as_sindex_metadata *qimdp = cf_rc_alloc(sizeof(as_sindex_metadata));

	memset(qimdp, 0, sizeof(as_sindex_metadata));

	qimdp->ns_name = cf_strdup(imd->ns_name);

	// Set name is optional for create
	if (imd->set) {
		qimdp->set = cf_strdup(imd->set);
	} else {
		qimdp->set = NULL;
	}

	qimdp->iname       = cf_strdup(imd->iname);
	qimdp->itype       = imd->itype;
	qimdp->n_pimds     = imd->n_pimds;
	qimdp->path_str    = cf_strdup(imd->path_str);
	qimdp->path_length = imd->path_length;
	memcpy(qimdp->path, imd->path, AS_SINDEX_MAX_DEPTH*sizeof(as_sindex_path));
	qimdp->bname       = cf_strdup(imd->bname);
	qimdp->sktype       = imd->sktype;
	qimdp->binid       = imd->binid;

	*qimd = qimdp;
}

/*
 * Function to perform validation check on the return type and increment
 * decrement all the statistics.
 */
void
as_sindex_process_ret(as_sindex *si, int ret, as_sindex_op op,
		uint64_t starttime, int pos)
{
	switch (op) {
		case AS_SINDEX_OP_INSERT:
			if (ret && ret != AS_SINDEX_KEY_FOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Insert into %s failed at %d with %d",
						si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.write_errs);
			} else if (!ret) {
				cf_atomic64_incr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_writes);
			SINDEX_HIST_INSERT_DATA_POINT(si, write_hist, starttime);
			break;
		case AS_SINDEX_OP_DELETE:
			if (ret && ret != AS_SINDEX_KEY_NOTFOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Delete from %s failed at %d with %d",
	                    si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.delete_errs);
			} else if (!ret) {
				cf_atomic64_decr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_deletes);
			SINDEX_HIST_INSERT_DATA_POINT(si, delete_hist, starttime);
			break;
		case AS_SINDEX_OP_READ:
			if (ret < 0) { // AS_SINDEX_CONTINUE(1) also OK
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Read from %s failed at %d with %d",
						si->imd->iname, pos, ret);
			}
			break;
		default:
			cf_crash(AS_SINDEX, "Invalid op");
	}
}

static bool
populate_binid(as_namespace *ns, as_sindex_metadata *imd)
{
	size_t len = strlen(imd->bname);

	if (len >= AS_BIN_NAME_MAX_SZ) {
		cf_warning(AS_SINDEX, "bin name %s of len %zu too big. Max len allowed is %d",
				imd->bname, len, AS_BIN_NAME_MAX_SZ - 1);
		return false;
	}

	uint16_t id;

	if (! as_bin_get_or_assign_id_w_len(ns, imd->bname, len, &id)) {
		cf_warning(AS_SINDEX, "Bin %s not added. Assign id failed", imd->bname);
		return false;
	}

	imd->binid = id;

	return true;
}

// Free if IMD has allocated the info in it
void
as_sindex_imd_free(as_sindex_metadata *imd)
{
	if (imd->ns_name) {
		cf_free(imd->ns_name);
	}

	if (imd->iname) {
		cf_free(imd->iname);
	}

	if (imd->set) {
		cf_free(imd->set);
	}

	if (imd->path_str) {
		cf_free(imd->path_str);
	}

	if (imd->bname) {
		cf_free(imd->bname);
	}

	for (int i = 0; i < imd->path_length; i++) {
		if (imd->path[i].type == AS_PARTICLE_TYPE_MAP
				&& imd->path[i].mapkey_type == AS_PARTICLE_TYPE_STRING) {
			cf_free(imd->path[i].value.key_str);
		}
	}
}
//                                           END - UTILITY
// ************************************************************************************************
// ************************************************************************************************
//                                           METADATA
typedef struct set_binid_hash_ele_s {
	cf_ll_element ele;
	uint32_t si_ix;
} set_binid_hash_ele;

static void
set_binid_hash_destroy(cf_ll_element * ele) {
	cf_free((set_binid_hash_ele * ) ele);
}

/*
 * Should happen under SINDEX_GWLOCK.
 */
static bool
put_in_set_binid_hash(as_namespace *ns, char *set, uint32_t binid,
		uint32_t si_ix)
{
	cf_ll *si_ix_ll = NULL;

	// Create fixed size key for hash
	char si_prop[PROP_KEY_SIZE];

	memset(si_prop, 0, PROP_KEY_SIZE);
	sprintf(si_prop, "%s_%u", set == NULL ? "" : set, binid);

	int rv = cf_shash_get(ns->sindex_set_binid_hash, (void *)si_prop,
			(void *)&si_ix_ll);

	if (rv == CF_SHASH_ERR_NOT_FOUND) {
		si_ix_ll = cf_malloc(sizeof(cf_ll));
		cf_ll_init(si_ix_ll, set_binid_hash_destroy, false);
		cf_shash_put(ns->sindex_set_binid_hash, si_prop, &si_ix_ll);
	}
	else if (rv != CF_SHASH_OK) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return false;
	}

	set_binid_hash_ele *ele = cf_malloc(sizeof(set_binid_hash_ele));

	ele->si_ix = si_ix;
	cf_ll_append(si_ix_ll, (cf_ll_element *)ele);

	return true;
}

/*
 * Should happen under SINDEX_GWLOCK.
 */
static bool
delete_from_set_binid_hash(as_namespace *ns, as_sindex_metadata *imd)
{
	char si_prop[PROP_KEY_SIZE];

	memset(si_prop, 0, PROP_KEY_SIZE);
	sprintf(si_prop, "%s_%u", imd->set == NULL ? "" : imd->set, imd->binid);

	// Get the sindex list corresponding to key.
	cf_ll *si_ix_ll = NULL;
	int rv = cf_shash_get(ns->sindex_set_binid_hash, si_prop, &si_ix_ll);

	if (rv == CF_SHASH_ERR_NOT_FOUND) {
		return false;
	}

	if (rv != CF_SHASH_OK) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return false;
	};

	// Match the path & type of incoming si to sindexes in the list.
	bool to_delete = false;
	cf_ll_element *ele = cf_ll_get_head(si_ix_ll);

	while (ele != NULL) {
		set_binid_hash_ele *prop_ele = (set_binid_hash_ele *) ele;
		as_sindex *si = &(ns->sindex[prop_ele->si_ix]);

		if (si->imd->sktype == imd->sktype && si->imd->itype == imd->itype &&
				strcmp(si->imd->path_str, imd->path_str) == 0) {
			to_delete = true;
			break;
		}

		ele = ele->next;
	}

	if (! to_delete) {
		return false;
	}

	cf_ll_delete(si_ix_ll, ele);

	// If the list size becomes 0, delete the entry from the hash.
	if (cf_ll_size(si_ix_ll) == 0) {
		rv = cf_shash_delete(ns->sindex_set_binid_hash, si_prop);

		if (rv != CF_SHASH_OK) {
			cf_warning(AS_SINDEX, "shash_delete fails with error %d for index %s",
					rv, imd->iname);
		}
	}

	return true;
}

static bool
add_defn(as_namespace *ns, as_sindex_metadata *imd, uint32_t si_ix)
{
	if (! put_in_set_binid_hash(ns, imd->set, imd->binid, si_ix)) {
		cf_warning(AS_SINDEX, "failed to insert index %s in the set_binid hash",
				imd->iname);
		return false;
	}

	char padded_iname[AS_ID_INAME_SZ] = { 0 }; // must pad key

	strcpy(padded_iname, imd->iname);
	cf_shash_put(ns->sindex_iname_hash, padded_iname, &si_ix);

	binid_set_bit(ns, imd->binid);

	return true;
}

void
as_sindex_delete_defn(as_namespace *ns, as_sindex_metadata *imd)
{
	if (! delete_from_set_binid_hash(ns, imd)) {
		cf_warning(AS_SINDEX, "index %s not found in the set_binid hash",
				imd->iname);
	}

	char padded_iname[AS_ID_INAME_SZ] = { 0 };

	strcpy(padded_iname, imd->iname);
	cf_shash_delete(ns->sindex_iname_hash, padded_iname);

	binid_clear_bit(ns, imd->binid);
}


//                                         END - METADATA
// ************************************************************************************************
// ************************************************************************************************
//                                             LOOKUP
/*
 * Should happen under SINDEX_GRLOCK.
 */
static bool
si_ix_list_by_defn(const as_namespace *ns, const char *set, uint32_t binid,
		cf_ll **si_ix_ll)
{
	// Make the fixed size key (set_binid)
	char si_prop[PROP_KEY_SIZE];

	memset(si_prop, 0, PROP_KEY_SIZE);
	sprintf(si_prop, "%s_%u", set == NULL ? "" : set, binid);

	int rv = cf_shash_get(ns->sindex_set_binid_hash, si_prop, si_ix_ll);


	if (rv != CF_SHASH_OK) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return false;
	}

	return true;
}

// Should happen under SINDEX_GRLOCK.
static bool
si_ix_by_iname(const as_namespace *ns, const char *iname, uint32_t *si_ix_r)
{
	size_t iname_len = strlen(iname);

	if (iname_len == 0 || iname_len >= AS_ID_INAME_SZ) {
		cf_warning(AS_SINDEX, "bad index name size %zu", iname_len);
		return false;
	}

	char padded_iname[AS_ID_INAME_SZ] = { 0 };

	strcpy(padded_iname, iname);

	int rv = cf_shash_get(ns->sindex_iname_hash, padded_iname, si_ix_r);

	return rv == CF_SHASH_OK;
}

// Should happen under SINDEX_GRLOCK.
static bool
si_ix_by_defn(const as_namespace *ns, const char *set, uint32_t binid,
		as_sindex_ktype type, as_sindex_type itype, const char *path,
		uint32_t *si_ix_r)
{
	cf_ll *si_ix_ll = NULL;

	if (! si_ix_list_by_defn(ns, set, binid, &si_ix_ll)) {
		return false;
	}

	cf_ll_element *ele = cf_ll_get_head(si_ix_ll);

	while (ele != NULL) {
		set_binid_hash_ele *prop_ele = (set_binid_hash_ele *) ele;
		as_sindex *si = &(ns->sindex[prop_ele->si_ix]);

		if (si->imd->sktype == type && si->imd->itype == itype &&
				strcmp(si->imd->path_str, path) == 0) {
			*si_ix_r = prop_ele->si_ix;
			return true;
		}

		ele = ele->next;
	}

	return false;
}

// Populates the si_arr with all the sindexes which matches set and binid
// Each sindex is reserved as well. Enough space is provided by caller in si_arr
// Currently only 8 sindexes can be create on one combination of set and binid
// i.e number_of_sindex_types * number_of_sindex_data_type (4 * 2)
uint32_t
as_sindex_arr_lookup_by_set_binid_lockfree(const as_namespace *ns,
		const char *set, uint32_t binid, as_sindex **si_arr)
{
	if (! binid_bit_is_set(ns, binid)) {
		return 0;
	}

	cf_ll *si_ix_ll = NULL;

	if (! si_ix_list_by_defn(ns, set, binid, &si_ix_ll)) {
		return 0;
	}

	cf_ll_element *ele = cf_ll_get_head(si_ix_ll);
	uint32_t sindex_count = 0;

	while (ele != NULL) {
		set_binid_hash_ele *si_ele = (set_binid_hash_ele *)ele;
		uint32_t si_ix = si_ele->si_ix;
		as_sindex *si = &ns->sindex[si_ix];

		// Reserve only active sindexes. Do not break this rule.
		if (! as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}

		if (si_ix != si->si_ix) {
			cf_warning(AS_SINDEX, "expected si_ix %u but got %u", si_ix,
					si->si_ix);
			ele = ele->next;
			continue;
		}

		as_sindex_reserve(si);

		si_arr[sindex_count++] = si;
		ele = ele->next;
	}

	return sindex_count;
}

static bool
validate_and_reserve_si(const as_namespace *ns, uint32_t si_ix, char flag)
{
	as_sindex *si = &ns->sindex[si_ix];

	if ((flag & AS_SINDEX_LOOKUP_SKIP_ACTIVE_CHECK) == 0 &&
			! as_sindex_isactive(si)) {
		return false;
	}

	if (si_ix != si->si_ix) {
		cf_warning(AS_SINDEX, "expected si_ix %u but got %u", si_ix, si->si_ix);
	}

	if ((flag & AS_SINDEX_LOOKUP_SKIP_RESERVATION) == 0) {
		as_sindex_reserve(si);
	}

	return true;
}

static as_sindex *
lookup_by_iname_lockfree(const as_namespace *ns, const char *iname, char flag)
{
	uint32_t si_ix;

	if (! si_ix_by_iname(ns, iname, &si_ix)) {
		return NULL;
	}

	if (! validate_and_reserve_si(ns, si_ix, flag)) {
		return NULL;
	}

	return &ns->sindex[si_ix];
}

as_sindex *
as_sindex_lookup_by_iname(const as_namespace *ns, const char *iname, char flag)
{
	SINDEX_GRLOCK();
	as_sindex *si = lookup_by_iname_lockfree(ns, iname, flag);
	SINDEX_GRUNLOCK();
	return si;
}

static as_sindex *
lookup_by_defn_lockfree(const as_namespace *ns, const char *set, uint32_t binid,
		as_sindex_ktype type, as_sindex_type itype, const char *path, char flag)
{
	if (! binid_bit_is_set(ns, binid)) {
		return NULL;
	}

	uint32_t si_ix;

	if (! si_ix_by_defn(ns, set, binid, type, itype, path, &si_ix)) {
		return NULL;
	}

	if (! validate_and_reserve_si(ns, si_ix, flag)) {
		return NULL;
	}

	return &ns->sindex[si_ix];
}

as_sindex *
as_sindex_lookup_by_defn(const as_namespace *ns, const char *set, uint32_t binid,
		as_sindex_ktype type, as_sindex_type itype, const char *path, char flag)
{
	SINDEX_GRLOCK();
	as_sindex *si = lookup_by_defn_lockfree(ns, set, binid, type, itype, path,
			flag);
	SINDEX_GRUNLOCK();
	return si;
}
//                                           END LOOKUP
// ************************************************************************************************
// ************************************************************************************************
//                                          STAT/CONFIG/HISTOGRAM
static void
stats_clear(as_sindex *si) {
	as_sindex_stat *s = &si->stats;

	s->n_objects = 0;

	s->n_writes = 0;
	s->write_errs = 0;

	s->n_deletes = 0;
	s->delete_errs = 0;

	s->loadtime = 0;
	s->populate_pct = 0;

	s->n_defrag_records = 0;

	s->n_query_basic_complete = 0;
	s->n_query_basic_error = 0;
	s->n_query_basic_abort = 0;
	s->n_query_basic_records = 0;

	si->enable_histogram = false;

	histogram_clear(s->_write_hist);
	histogram_clear(s->_si_prep_hist);
	histogram_clear(s->_delete_hist);
	histogram_clear(s->_query_hist);
	histogram_clear(s->_query_batch_io);
	histogram_clear(s->_query_batch_lookup);
	histogram_clear(s->_query_rcnt_hist);
	histogram_clear(s->_query_diff_hist);
}

void
as_sindex_gconfig_default(as_config *c)
{
	c->sindex_builder_threads = 4;
	c->sindex_gc_period = 10; // every 10 seconds
}

static void
setup_histogram(as_sindex *si)
{
	char hist_name[AS_ID_INAME_SZ + 64];

	sprintf(hist_name, "%s_write_us", si->imd->iname);
	si->stats._write_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "%s_si_prep_us", si->imd->iname);
	si->stats._si_prep_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "%s_delete_us", si->imd->iname);
	si->stats._delete_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "%s_query", si->imd->iname);
	si->stats._query_hist = histogram_create(hist_name, HIST_MILLISECONDS);

	sprintf(hist_name, "%s_query_batch_lookup_us", si->imd->iname);
	si->stats._query_batch_lookup = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "%s_query_batch_io_us", si->imd->iname);
	si->stats._query_batch_io = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "%s_query_row_count", si->imd->iname);
	si->stats._query_rcnt_hist = histogram_create(hist_name, HIST_COUNT);

	sprintf(hist_name, "%s_query_diff_count", si->imd->iname);
	si->stats._query_diff_hist = histogram_create(hist_name, HIST_COUNT);
}

static void
destroy_histogram(as_sindex *si)
{
	cf_free(si->stats._write_hist);
	cf_free(si->stats._si_prep_hist);
	cf_free(si->stats._delete_hist);
	cf_free(si->stats._query_hist);
	cf_free(si->stats._query_batch_lookup);
	cf_free(si->stats._query_batch_io);
	cf_free(si->stats._query_rcnt_hist);
	cf_free(si->stats._query_diff_hist);
}

int
as_sindex_stats_str(as_namespace *ns, char * iname, cf_dyn_buf *db)
{
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, 0);

	if (!si) {
		cf_warning(AS_SINDEX, "SINDEX STAT : sindex %s not found", iname);
		return AS_SINDEX_ERR_NOTFOUND;
	}

	info_append_uint64(db, "keys", ai_btree_get_numkeys(si->imd));
	info_append_uint64(db, "entries", cf_atomic64_get(si->stats.n_objects));

	info_append_uint64(db, "ibtr_memory_used", ai_btree_get_isize(si->imd));
	info_append_uint64(db, "nbtr_memory_used", ai_btree_get_nsize(si->imd));

	info_append_uint32(db, "load_pct", si->stats.populate_pct);
	info_append_uint64(db, "loadtime", cf_atomic64_get(si->stats.loadtime));

	info_append_uint64(db, "write_success", cf_atomic64_get(si->stats.n_writes) - cf_atomic64_get(si->stats.write_errs));
	info_append_uint64(db, "write_error", cf_atomic64_get(si->stats.write_errs));

	info_append_uint64(db, "delete_success", cf_atomic64_get(si->stats.n_deletes) - cf_atomic64_get(si->stats.delete_errs));
	info_append_uint64(db, "delete_error", cf_atomic64_get(si->stats.delete_errs));

	info_append_uint64(db, "stat_gc_recs", cf_atomic64_get(si->stats.n_defrag_records));

	uint64_t complete = cf_atomic64_get(si->stats.n_query_basic_complete);
	uint64_t error = cf_atomic64_get(si->stats.n_query_basic_error);
	uint64_t abort = cf_atomic64_get(si->stats.n_query_basic_abort);
	uint64_t total = complete + error + abort;
	uint64_t records = cf_atomic64_get(si->stats.n_query_basic_records);

	info_append_uint64(db, "query_basic_complete", complete);
	info_append_uint64(db, "query_basic_error", error);
	info_append_uint64(db, "query_basic_abort", abort);
	info_append_uint64(db, "query_basic_avg_rec_count", total ? records / total : 0);

	info_append_bool(db, "histogram", si->enable_histogram);

	cf_dyn_buf_chomp(db);

	as_sindex_release(si);

	return AS_SINDEX_OK;
}

void
as_sindex_histogram_dumpall(as_namespace *ns)
{
	SINDEX_GRLOCK();

	for (uint32_t i = 0, a = 0; i < AS_SINDEX_MAX && a < ns->sindex_cnt; i++) {
		as_sindex *si = &ns->sindex[i];

		if (as_sindex_isactive(si)) {
			a++;

			if (! si->enable_histogram) {
				continue;
			}

			histogram_dump(si->stats._write_hist);
			histogram_dump(si->stats._si_prep_hist);
			histogram_dump(si->stats._delete_hist);
			histogram_dump(si->stats._query_hist);
			histogram_dump(si->stats._query_batch_lookup);
			histogram_dump(si->stats._query_batch_io);
			histogram_dump(si->stats._query_rcnt_hist);
			histogram_dump(si->stats._query_diff_hist);
		}
	}

	SINDEX_GRUNLOCK();
}

int
as_sindex_histogram_enable(as_namespace *ns, char * iname, bool enable)
{
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, 0);
	if (!si) {
		cf_warning(AS_SINDEX, "SINDEX HISTOGRAM : sindex %s not found", iname);
		return AS_SINDEX_ERR_NOTFOUND;
	}

	si->enable_histogram = enable;
	as_sindex_release(si);
	return AS_SINDEX_OK;
}

/*
 * Client API to list all the indexes in a namespace, returns list of imd with
 * index information, caller should free it up.
 */
void
as_sindex_list_str(const as_namespace *ns, cf_dyn_buf *db)
{
	SINDEX_GRLOCK();

	for (uint32_t i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		as_sindex_metadata *imd = si->imd;

		if (imd == NULL) {
			continue;
		}

		cf_dyn_buf_append_string(db, "ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_string(db, ":set=");
		cf_dyn_buf_append_string(db, (imd->set) ? imd->set : "NULL");
		cf_dyn_buf_append_string(db, ":indexname=");
		cf_dyn_buf_append_string(db, imd->iname);
		cf_dyn_buf_append_string(db, ":bin=");
		cf_dyn_buf_append_string(db, imd->bname);
		cf_dyn_buf_append_string(db, ":type=");
		cf_dyn_buf_append_string(db, as_sindex_ktype_str(imd->sktype));
		cf_dyn_buf_append_string(db, ":indextype=");
		cf_dyn_buf_append_string(db, sindex_itypes[imd->itype]);
		cf_dyn_buf_append_string(db, ":path=");
		cf_dyn_buf_append_string(db, imd->path_str);

		// Inactive indexes are skipped above by imd == NULL check.
		if (si->state == AS_SINDEX_ACTIVE) {
			if (si->readable) {
				cf_dyn_buf_append_string(db, ":state=RW");
			}
			else {
				cf_dyn_buf_append_string(db, ":state=WO");
			}
		}
		else {
			cf_dyn_buf_append_string(db, ":state=D");
		}

		cf_dyn_buf_append_char(db, ';');
	}

	SINDEX_GRUNLOCK();
}
//                                  END - STAT/CONFIG/HISTOGRAM
// ************************************************************************************************
// ************************************************************************************************
//                                         SI REFERENCE
// Reserve the sindex so it does not get deleted under the hood
void
as_sindex_reserve(as_sindex *si)
{
	cf_assert(as_sindex_isactive(si), AS_SINDEX, "sindex %s - state %d",
			si->imd->iname, si->state);

	cf_rc_reserve(si->imd);
}

/*
 * Release, queue up the request for the destroy to clean up Aerospike Index thread,
 * Not done inline because main write thread could release the last reference.
 */
void
as_sindex_release(as_sindex *si)
{
	int32_t val = cf_rc_release(si->imd);

	if (val == 0) {
		as_sindex_populate_destroy(si);
	}
}

// Complementary function of as_sindex_arr_lookup_by_set_binid
void
as_sindex_release_arr(as_sindex *si_arr[], uint32_t si_arr_sz)
{
	for (uint32_t i = 0; i < si_arr_sz; i++) {
		as_sindex_release(si_arr[i]);
	}
}

//                                    END - SI REFERENCE
// ************************************************************************************************
// ************************************************************************************************
//                                          SINDEX CREATE
static void
create_pmeta(as_sindex *si, uint32_t n_pimd)
{
	si->imd->pimd = cf_calloc(n_pimd, sizeof(as_sindex_pmetadata));

	pthread_rwlockattr_t rwattr;

	if (pthread_rwlockattr_init(&rwattr)) {
		cf_crash(AS_SINDEX, "pthread_rwlockattr_init: %s", cf_strerror(errno));
	}

	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
		cf_crash(AS_SINDEX, "pthread_rwlockattr_setkind_np: %s",
				cf_strerror(errno));
	}

	for (uint32_t i = 0; i < n_pimd; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];

		if (pthread_rwlock_init(&pimd->slock, &rwattr)) {
			cf_crash(AS_SINDEX, "pthread_rwlock_init: %s", cf_strerror(errno));
		}
	}
}

as_sindex_status
as_sindex_create_check_params(const as_namespace *ns,
		const as_sindex_metadata *imd)
{
	SINDEX_GRLOCK();

	if (ns->sindex_cnt >= AS_SINDEX_MAX) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_ERR_MAXCOUNT;
	}

	uint32_t dummy_val;

	if (si_ix_by_iname(ns, imd->iname, &dummy_val)) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_ERR_FOUND;
	}

	int32_t binid = as_bin_get_id(ns, imd->bname);

	if (binid != -1 && si_ix_by_defn(ns, imd->set, (uint32_t)binid,
			imd->sktype, imd->itype, imd->path_str, &dummy_val)) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_ERR_FOUND;
	}

	SINDEX_GRUNLOCK();

	return AS_SINDEX_OK;
}

// FIXME - what sanity checks & failure behavior really?
static void
smd_startup_create(as_namespace *ns, as_sindex_metadata *imd)
{
	if (lookup_by_iname_lockfree(ns, imd->iname,
			AS_SINDEX_LOOKUP_SKIP_ACTIVE_CHECK |
			AS_SINDEX_LOOKUP_SKIP_RESERVATION) != NULL) {
		cf_crash(AS_SINDEX,"{%s} duplicate sindex %s", ns->name, imd->iname);
	}

	if (ns->sindex_cnt == AS_SINDEX_MAX) {
		cf_crash(AS_SINDEX,"{%s} too many sindexes", ns->name);
	}

	as_set *p_set = NULL;

	if (imd->set != NULL && as_namespace_get_create_set_w_len(ns, imd->set,
			strlen(imd->set), &p_set, NULL) != 0) {
		cf_crash(AS_SINDEX, "{%s} failed for set %s", ns->name, imd->set);
	}

	if (! populate_binid(ns, imd)) {
		cf_crash(AS_SINDEX, "{%s} failed for bin %s", ns->name, imd->bname);
	}

	imd->n_pimds = ns->sindex_num_partitions;

	uint32_t si_ix = ns->sindex_cnt;

	add_defn(ns, imd, si_ix);

	// Init si.
	as_sindex *si = &ns->sindex[si_ix]; // memset 0 at startup, + state INACTIVE

	si->ns = ns;
	si->si_ix = si_ix;
	si->state = AS_SINDEX_ACTIVE;

	// Either populate will assert or be successful, so ok to assume success:
	si->readable = true;
	si->stats.populate_pct = 100;

	as_sindex_metadata *qimd;

	clone_meta(imd, &qimd);
	qimd->si = si;

	si->imd = qimd;

	// Init pimd.
	create_pmeta(si, imd->n_pimds);
	ai_btree_create(si->imd);

	setup_histogram(si);
	stats_clear(si);

	ns->sindex_cnt++;

	if (p_set != NULL) {
		p_set->n_sindexes++;
	}
	else {
		ns->n_setless_sindexes++;
	}

	cf_atomic64_add(&ns->n_bytes_sindex_memory,
			(int64_t)ai_btree_get_isize(si->imd));
}

void
as_sindex_create_lockless(as_namespace *ns, as_sindex_metadata *imd)
{
	uint32_t si_ix = AS_SINDEX_MAX;
	as_sindex *si = NULL;

	for (uint32_t i = 0; i < AS_SINDEX_MAX; i++) {
		if (ns->sindex[i].state == AS_SINDEX_INACTIVE) {
			si = &ns->sindex[i];
			si_ix = i;
			break;
		}
	}

	if (si_ix == AS_SINDEX_MAX)  {
		cf_warning(AS_SINDEX, "SINDEX CREATE : maxed out secondary index limit no more indexes allowed");
		return;
	}

	as_set *p_set = NULL;

	if (imd->set != NULL && as_namespace_get_create_set_w_len(ns, imd->set,
			strlen(imd->set), &p_set, NULL) != 0) {
		cf_warning(AS_SINDEX, "SINDEX CREATE : failed get-create set %s",
				imd->set);
		return;
	}

	imd->n_pimds = ns->sindex_num_partitions;

	if (! populate_binid(ns, imd)) {
		cf_warning(AS_SINDEX, "SINDEX CREATE : populating bin id failed");
		return;
	}

	if (! add_defn(ns, imd, si_ix)) {
		return;
	}

	// Init SI.
	si = &ns->sindex[si_ix];
	si->ns = ns;
	si->si_ix = si_ix;
	si->state = AS_SINDEX_ACTIVE;
	si->readable = false;
	si->recreate_imd = NULL;

	// Init IMD.
	as_sindex_metadata *qimd;
	clone_meta(imd, &qimd);
	si->imd = qimd;
	qimd->si = si;

	// Init PIMD.
	create_pmeta(si, imd->n_pimds);
	ai_btree_create(si->imd);

	// Init stats.
	setup_histogram(si);
	stats_clear(si);

	ns->sindex_cnt++;

	if (p_set != NULL) {
		p_set->n_sindexes++;
	}
	else {
		ns->n_setless_sindexes++;
	}

	cf_atomic64_add(&ns->n_bytes_sindex_memory,
			(int64_t)ai_btree_get_isize(si->imd));

	if (p_set != NULL && p_set->n_objects == 0) {
		// Shortcut if the set is empty. (Not bothering to handle the set of
		// things not in a set.)

		si->readable = true;
		si->stats.populate_pct = 100;
		si->stats.loadtime = 0; // shortcut takes no time

		cf_info(AS_SINDEX, "{%s} empty sindex %s ready", ns->name, imd->iname);
	}
	else {
		as_sindex_populate_add(si);
	}
}

static void
smd_create(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();

	bool found_exact_defn = false; // ns:iname   ns:binid / set / sktype / itype / path_str
	bool found_defn = false;       //            ns:binid / set / sktype / itype / path_str
	bool found_iname = false;      // ns:iname

	uint32_t si_ix_defn = 0;
	int32_t binid = as_bin_get_id(ns, imd->bname);

	if (binid != -1 && si_ix_by_defn(ns, imd->set, (uint32_t)binid, imd->sktype,
			imd->itype, imd->path_str, &si_ix_defn)) {
		as_sindex *si = &ns->sindex[si_ix_defn];

		if (strcmp(si->imd->iname, imd->iname) == 0) {
			found_exact_defn = true;
		} else {
			found_defn = true;
		}
	}

	uint32_t si_ix_iname;

	if (si_ix_by_iname(ns, imd->iname, &si_ix_iname)) {
		found_iname = true;
	}

	if (found_exact_defn) {
		as_sindex *si = &ns->sindex[si_ix_defn];

		if (si->state == AS_SINDEX_ACTIVE) {
			SINDEX_GWUNLOCK();
			return;
		}
	}

	cf_info(AS_SINDEX, "SINDEX CREATE: request received for %s:%s via smd",
			ns->name, imd->iname);

	if (found_defn) {
		as_sindex *si = &ns->sindex[si_ix_defn];

		if (si->state == AS_SINDEX_ACTIVE) {
			si->state = AS_SINDEX_DESTROY;
			as_sindex_release(si);
		}
	}

	if (found_iname) {
		as_sindex *si = &ns->sindex[si_ix_iname];

		if (si->state == AS_SINDEX_ACTIVE) {
			si->state = AS_SINDEX_DESTROY;
			as_sindex_release(si);
		}
	}

	if (found_defn || found_exact_defn) {
		as_sindex *si = &ns->sindex[si_ix_defn];

		clone_meta(imd, &si->recreate_imd);
		SINDEX_GWUNLOCK();
		return;
	}

	if (found_iname) {
		as_sindex *si = &ns->sindex[si_ix_iname];

		clone_meta(imd, &si->recreate_imd);
		SINDEX_GWUNLOCK();
		return;
	}

	// Not found.
	as_sindex_create_lockless(ns, imd);

	SINDEX_GWUNLOCK();
}

//                                       END - SINDEX CREATE
// ************************************************************************************************
// ************************************************************************************************
//                                         SINDEX DELETE

void
as_sindex_destroy_pmetadata(as_sindex *si)
{
	for (uint32_t i = 0; i < si->imd->n_pimds; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		pthread_rwlock_destroy(&pimd->slock);
	}
	destroy_histogram(si);
	cf_free(si->imd->pimd);
	si->imd->pimd = NULL;
}

// Lookup by defn as imd->iname is always NULL coming from the only smd caller.
static void
smd_drop(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();

	int32_t bin_id = as_bin_get_id(ns, imd->bname);

	if (bin_id == -1) {
		SINDEX_GWUNLOCK();
		cf_warning(AS_SINDEX, "SINDEX DROP: bin '%s' not found", imd->bname);
		return;
	}

	as_sindex *si = lookup_by_defn_lockfree(ns, imd->set, (uint32_t)bin_id,
			imd->sktype, imd->itype, imd->path_str,
			AS_SINDEX_LOOKUP_SKIP_RESERVATION |
			AS_SINDEX_LOOKUP_SKIP_ACTIVE_CHECK);

	if (si == NULL) {
		SINDEX_GWUNLOCK();
		cf_warning(AS_SINDEX, "SINDEX DROP: defn not found");
		return;
	}

	cf_info(AS_SINDEX, "SINDEX DROP: request received for %s:%s via smd",
			ns->name, si->imd->iname);

	if (si->state == AS_SINDEX_ACTIVE) {
		si->state = AS_SINDEX_DESTROY;
		as_sindex_release(si); // looked without reservation, releasing last ref
	}

	as_sindex_metadata *recreate_imd = si->recreate_imd;

	// Cancel any pending recreation as this delete is latest.
	if (recreate_imd != NULL) {
		as_sindex_imd_free(recreate_imd);
		cf_rc_free(recreate_imd);
		si->recreate_imd = NULL;
	}

	SINDEX_GWUNLOCK();
}

//                                        END - SINDEX DELETE
// ************************************************************************************************
// ************************************************************************************************
//                                       SINDEX BIN PATH
static void
add_map_key_in_path(as_sindex_metadata *imd, const char *path_str,
		uint32_t start, uint32_t end)
{
	// Path element cannot be longer than AS_SINDEX_MAX_PATH_LENGTH - 1.
	char ele_str[AS_SINDEX_MAX_PATH_LENGTH];

	memcpy(ele_str, path_str + start, end - start);
	ele_str[end - start] = '\0';

	as_sindex_path *path_ele = &imd->path[imd->path_length - 1];

	if (cf_strtoul_u64(ele_str, &path_ele->value.key_int) == 0) {
		path_ele->mapkey_type = AS_PARTICLE_TYPE_INTEGER;
	}
	else {
		path_ele->mapkey_type = AS_PARTICLE_TYPE_STRING;
		path_ele->value.key_str = strdup(ele_str);
	}
}

static bool
add_list_element_in_path(as_sindex_metadata *imd, const char *path_str,
		uint32_t start, uint32_t end)
{
	int path_length = imd->path_length;
	// Path element cannot be longer than AS_SINDEX_MAX_PATH_LENGTH - 1.
	char ele_str[AS_SINDEX_MAX_PATH_LENGTH];

	memcpy(ele_str, path_str + start, end - start);
	ele_str[end - start] = '\0';

	return cf_strtoul_u32(ele_str, &imd->path[path_length - 1].value.index) == 0;
}

static bool
parse_subpath(as_sindex_metadata *imd, const char *path_str, size_t path_str_len,
		uint32_t start, uint32_t end)
{
	bool reached_end = end == path_str_len;

	if (start == 0) {
		if (reached_end) {
			imd->bname = strdup(path_str);
		}
		else if (path_str[end] == '.') {
			imd->bname = strndup(path_str, end);
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			imd->bname = strndup(path_str, end);
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return false;
		}

		return true;
	}

	if (path_str[start] == '.') {
		if (reached_end) {
			add_map_key_in_path(imd, path_str, start + 1, end);
		}
		else if (path_str[end] == '.') {
			add_map_key_in_path(imd, path_str, start + 1, end);

			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			add_map_key_in_path(imd, path_str, start + 1, end);

			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return false;
		}

		return true;
	}

	if (path_str[start] == '[') {
		if (reached_end || path_str[end] != ']') {
			return false;
		}

		return add_list_element_in_path(imd, path_str, start + 1, end);
	}

	if (path_str[start] == ']') {
		if (end - start != 1) {
			return false;
		}

		if (reached_end) {
			return true;
		}

		if (path_str[end] == '.') {
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			imd->path_length++;
			imd->path[imd->path_length - 1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return false;
		}

		return true;
	}

	return false;
}

/*
 * This function parses the path_str and populate array of path structure in
 * imd.
 * Each element of the path is the way to reach the the next path.
 * For e.g
 * bin.k1[1][0]
 * array of the path structure would be like -
 * path[0].type = AS_PARTICLE_TYPE_MAP . path[0].value.key_str = k1  path[0].value.ke
 * path[1].type = AS_PARTICLE_TYPE_LIST . path[1].value.index  = 1
 * path[2].type = AS_PARTICLE_TYPE_LIST . path[2].value.index  = 0
*/
bool
as_sindex_extract_bin_path(as_sindex_metadata *imd, const char *path_str)
{
	size_t path_str_len = strlen(path_str);

	if (path_str_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_SINDEX, "bin path length exceeds the maximum allowed.");
		return false;
	}

	uint32_t start = 0;
	uint32_t end = 0;

	// Iterate through the path_str and search for character (., [, ]) which
	// leads to sublevels in maps and lists.
	while (end < path_str_len) {
		char at = path_str[end];

		if (at == '.' || at == '[' || at == ']') {
			if (end - start == 1) {
				// Need at least one character between delimiters.
				return false;
			}

			if (! parse_subpath(imd, path_str, path_str_len, start, end)) {
				return false;
			}

			start = end;

			if (imd->path_length >= AS_SINDEX_MAX_DEPTH) {
				cf_warning(AS_SINDEX, "bin position depth level exceeds the max depth allowed %d",
						AS_SINDEX_MAX_DEPTH);
				return false;
			}
		}

		end++;
	}

	if (! parse_subpath(imd, path_str, path_str_len, start, end)) {
		return false;
	}

	return true;
}

/*
 * This function checks the existence of path stored in the sindex metadata
 * in a bin
 */
as_val *
as_sindex_extract_val_from_path(const as_sindex_metadata *imd, as_val *val)
{
	if (val == NULL) {
		return NULL;
	}

	as_particle_type imd_sktype = as_sindex_pktype(imd);
	const as_sindex_path *path = imd->path;

	// imd->path_length will be 0 for simple paths, skipping the loop.
	for (int i = 0; i < imd->path_length; i++) {
		const as_sindex_path *p = &path[i];

		switch (val->type) {
		case AS_STRING:
		case AS_INTEGER:
			return NULL;
		case AS_LIST:
			if (p->type != AS_PARTICLE_TYPE_LIST) {
				return NULL;
			}

			uint32_t index = p->value.index;
			as_arraylist *list = (as_arraylist *)as_list_fromval(val);

			val = as_arraylist_get(list, index);

			if (val == NULL) {
				return NULL;
			}

			break;
		case AS_MAP:
			if (p->type != AS_PARTICLE_TYPE_MAP) {
				return NULL;
			}

			as_map *map = as_map_fromval(val);
			as_val *key;

			if (p->mapkey_type == AS_PARTICLE_TYPE_STRING) {
				key = (as_val *)as_string_new(p->value.key_str, false);
			}
			else if (p->mapkey_type == AS_PARTICLE_TYPE_INTEGER) {
				key = (as_val *)as_integer_new((int64_t)p->value.key_int);
			}
			else {
				cf_warning(AS_SINDEX, "Invalid map key type");
				return NULL;
			}

			val = as_map_get(map, key);

			as_val_destroy(key);

			if (val == NULL) {
				return NULL;
			}
			break;
		default:
			return NULL;
		}
	}

	switch (imd->itype) {
	case AS_SINDEX_ITYPE_DEFAULT:
		if (val->type == AS_INTEGER && imd_sktype == AS_PARTICLE_TYPE_INTEGER) {
			return val;
		}
		else if (val->type == AS_STRING &&
				imd_sktype == AS_PARTICLE_TYPE_STRING) {
			return val;
		}

		return NULL;
	case AS_SINDEX_ITYPE_MAPKEYS:
	case AS_SINDEX_ITYPE_MAPVALUES:
		if (val->type == AS_MAP) {
			return val;
		}

		return NULL;
	case AS_SINDEX_ITYPE_LIST:
		if (val->type == AS_LIST) {
			return val;
		}

		return NULL;
	default:
		return NULL;
	}
}
//                                        END - SINDEX BIN PATH
// ************************************************************************************************
// ************************************************************************************************
//                                          SBIN UTILITY
static void
init_sbin(as_sindex_bin *sbin, as_sindex_op op, as_particle_type type,
		as_sindex *si)
{
	sbin->si = si;
	sbin->to_free = false;
	sbin->num_values = 0;
	sbin->op = op;
	sbin->heap_capacity = 0;
	sbin->type = type;
	sbin->values = NULL;
}

static void
sbin_free(as_sindex_bin *sbin)
{
	if (sbin->to_free) {
		if (sbin->values) {
			cf_free(sbin->values);
		}
	}
}

// Will not free the space consumed from stack_buf. In case of a rollback, the
// space will be wasted but it is acceptable.
void
as_sindex_sbin_freeall(as_sindex_bin *sbin, uint32_t numbins)
{
	for (uint32_t i = 0; i < numbins; i++)  {
		sbin_free(&sbin[i]);
	}
}

static void
op_by_sbin(as_sindex_bin *sbin, cf_arenax_handle r_h)
{
	as_sindex *si = sbin->si;
	as_sindex_metadata *imd = si->imd;
	as_sindex_op op = sbin->op;

	for (uint32_t i = 0; i < sbin->num_values; i++) {
		void *skey;

		switch (sbin->type) {
		case AS_PARTICLE_TYPE_INTEGER:
		case AS_PARTICLE_TYPE_GEOJSON:
			skey = i == 0 ? (void *)&(sbin->value.int_val) :
					(void *)((uint64_t *)(sbin->values) + i);
			break;
		case AS_PARTICLE_TYPE_STRING:
			skey = i == 0 ? (void *)&(sbin->value.str_val) :
					(void *)((cf_digest *)(sbin->values) + i);
			break;
		default:
			cf_crash(AS_SINDEX, "bad sbin type %d", sbin->type);
			return;
		}

		as_sindex_pmetadata *pimd = &imd->pimd[as_sindex_get_pimd_ix(imd, skey)];
		uint64_t starttime = si->enable_histogram ? cf_getns() : 0;

		PIMD_WLOCK(&pimd->slock);

		as_sindex_status ret = op == AS_SINDEX_OP_DELETE ?
				ai_btree_delete(imd, pimd, skey, r_h) :
				ai_btree_put(imd, pimd, skey, r_h);

		PIMD_WUNLOCK(&pimd->slock);

		as_sindex_process_ret(si, ret, op, starttime, __LINE__);
	}
}

static uint8_t
sbin_data_size(as_particle_type type) {
	if (type == AS_PARTICLE_TYPE_INTEGER || type == AS_PARTICLE_TYPE_GEOJSON) {
		return sizeof(uint64_t);
	}

	if (type == AS_PARTICLE_TYPE_STRING) {
		return sizeof(cf_digest);
	}

	cf_crash(AS_SINDEX, "bad bin type to index %d", type);
}

//                                       END - SBIN UTILITY
// ************************************************************************************************
// ************************************************************************************************
//                                          ADD TO SBIN

static void
add_sbin_value_in_heap(as_sindex_bin *sbin, const void *val)
{
	uint32_t data_sz = sbin_data_size(sbin->type);
	uint32_t size = 0;
	bool to_copy = false;
	void *tmp_value = NULL;

	// If to_free = false, this means this is the first time we are storing
	// value for this sbin to heap. Check if there is need to copy the existing
	// data from stack_buf.
	if (! sbin->to_free) {
		if (sbin->num_values == 0) {
			size = 2;
		}
		else if (sbin->num_values == 1) {
			to_copy = true;
			size = 2;
			tmp_value = &sbin->value;
		}
		else {
			to_copy = true;
			size = 2 * sbin->num_values;
			tmp_value = sbin->values;
		}

		sbin->values = cf_malloc(data_sz * size);
		sbin->to_free = true;
		sbin->heap_capacity = size;

		if (to_copy) {
			memcpy(sbin->values, tmp_value, data_sz * sbin->num_values);

			if (sbin->num_values != 1) {
				sbin->stack_buf->used_sz -= (sbin->num_values * data_sz);
			}
		}
	}
	else if (sbin->heap_capacity == sbin->num_values) {
		sbin->heap_capacity = 2 * sbin->heap_capacity;
		sbin->values = cf_realloc(sbin->values, sbin->heap_capacity * data_sz);
	}

	if (sbin->type == AS_PARTICLE_TYPE_INTEGER ||
			sbin->type == AS_PARTICLE_TYPE_GEOJSON) {
		memcpy(((uint64_t *)sbin->values + sbin->num_values), val, data_sz);
	}
	else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		memcpy(((cf_digest *)sbin->values + sbin->num_values), val, data_sz);
	}

	sbin->num_values++;
}

static void
add_value_to_sbin(as_sindex_bin *sbin, const uint8_t *val)
{
	// If this is the first value, assign the value to the embedded field.
	if (sbin->num_values == 0) {
		if (sbin->type == AS_PARTICLE_TYPE_STRING) {
			sbin->value.str_val = *(cf_digest *)val;
		}
		else { // INTEGER or GEO
			sbin->value.int_val = *(int64_t *)val;
		}

		sbin->num_values++;

		return;
	}

	uint32_t data_sz = sbin_data_size(sbin->type);
	sbin_value_pool *stack_buf = sbin->stack_buf;

	if (sbin->num_values == 1) {
		if ((stack_buf->used_sz + data_sz + data_sz) >
				AS_SINDEX_VALUESZ_ON_STACK) {
			add_sbin_value_in_heap(sbin, (const void *)val);
		}
		else {
			sbin->values = stack_buf->value + stack_buf->used_sz;

			memcpy(sbin->values, &sbin->value, data_sz);
			stack_buf->used_sz += data_sz;

			memcpy(((uint8_t *)sbin->values + data_sz * sbin->num_values), val,
					data_sz);
			stack_buf->used_sz += data_sz;

			sbin->num_values++;
		}
	}
	else {
		if (sbin->to_free ||
				(stack_buf->used_sz + data_sz ) > AS_SINDEX_VALUESZ_ON_STACK) {
			add_sbin_value_in_heap(sbin, (const void *)val);
		}
		else {
			memcpy(((uint8_t *)sbin->values + data_sz * sbin->num_values), val,
					data_sz);
			sbin->num_values++;
			stack_buf->used_sz += data_sz;
		}
	}
}

static void
add_integer_to_sbin(as_sindex_bin *sbin, uint64_t val)
{
	add_value_to_sbin(sbin, (uint8_t *)&val);
}

static void
add_digest_to_sbin(as_sindex_bin *sbin, cf_digest val_dig)
{
	add_value_to_sbin(sbin, (uint8_t *)&val_dig);
}

static void
add_string_to_sbin(as_sindex_bin *sbin, char *val)
{
	// Calculate digest and cal add_digest_to_sbin
	cf_digest val_dig;
	cf_digest_compute(val, strlen(val), &val_dig);
	add_digest_to_sbin(sbin, val_dig);
}
//                                       END - ADD TO SBIN
// ************************************************************************************************
// ************************************************************************************************
//                                 ADD KEYTYPE FROM BASIC TYPE ASVAL
static bool
add_long_from_asval(const as_val *val, as_sindex_bin *sbin)
{
	as_integer *i = as_integer_fromval(val);

	if (i == NULL) {
		return false;
	}

	uint64_t int_val = (uint64_t)as_integer_get(i);

	add_integer_to_sbin(sbin, int_val);

	return true;
}

static bool
add_digest_from_asval(const as_val *val, as_sindex_bin *sbin)
{
	as_string *s = as_string_fromval(val);

	if (s == NULL) {
		return false;
	}

	char * str_val = as_string_get(s);

	if (str_val == NULL) {
		return false;
	}

	add_string_to_sbin(sbin, str_val);

	return true;
}

static bool
add_geo2dsphere_from_as_val(const as_val *val, as_sindex_bin *sbin)
{
	as_geojson *g = as_geojson_fromval(val);

	if (g == NULL) {
		return false;
	}

	const char *s = as_geojson_get(g);
	size_t jsonsz = as_geojson_len(g);
	uint64_t parsed_cellid = 0;
	geo_region_t parsed_region = NULL;

	if (! geo_parse(NULL, s, jsonsz, &parsed_cellid, &parsed_region)) {
		cf_warning(AS_PARTICLE, "geo_parse() failed - unexpected");
		geo_region_destroy(parsed_region);
		return false;
	}

	if (parsed_cellid != 0) {
		if (parsed_region != NULL) {
			geo_region_destroy(parsed_region);
			cf_warning(AS_PARTICLE, "geo_parse found both point and region");
			return false;
		}

		// POINT
		add_integer_to_sbin(sbin, parsed_cellid);

		return true;
	}

	if (parsed_region != NULL) {
		// REGION
		uint32_t numcells;
		uint64_t outcells[MAX_REGION_CELLS];

		if (! geo_region_cover(NULL, parsed_region, MAX_REGION_CELLS, outcells,
				NULL, NULL, &numcells)) {
			geo_region_destroy(parsed_region);
			cf_warning(AS_PARTICLE, "geo_region_cover failed");
			return false;
		}

		geo_region_destroy(parsed_region);

		for (uint32_t i = 0; i < numcells; i++) {
			add_integer_to_sbin(sbin, outcells[i]);
		}

		return true;
	}

	cf_warning(AS_PARTICLE, "geo_parse found neither point nor region");
	return false;
}

typedef bool (*add_keytype_from_asval_fn)(const as_val *val, as_sindex_bin *sbin);
static const add_keytype_from_asval_fn add_keytype_from_asval[COL_TYPE_MAX] = {
	NULL,
	add_long_from_asval,
	add_digest_from_asval,
	add_geo2dsphere_from_as_val
};

//                             END - ADD KEYTYPE FROM BASIC TYPE ASVAL
// ************************************************************************************************
// ************************************************************************************************
//                                    ADD ASVAL TO SINDEX TYPE
static bool
add_asval_to_default_sindex(as_val *val, as_sindex_bin *sbin)
{
	as_sindex_ktype sktype = as_sindex_sktype_from_pktype(sbin->type);
	return add_keytype_from_asval[sktype](val, sbin);
}

static bool
add_listvalues_foreach(as_val *element, void *udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_ktype sktype = as_sindex_sktype_from_pktype(sbin->type);
	add_keytype_from_asval[sktype](element, sbin);
	return true;
}

static bool
add_asval_to_list_sindex(as_val *val, as_sindex_bin *sbin)
{
	const as_list *list = as_list_fromval(val);

	if (as_list_foreach(list, add_listvalues_foreach, sbin)) {
		return true;
	}

	return false;
}

static bool
add_mapkeys_foreach(const as_val *key, const as_val *val, void *udata)
{
	(void)val;

	as_sindex_bin *sbin = (as_sindex_bin *)udata;
	as_sindex_ktype sktype = as_sindex_sktype_from_pktype(sbin->type);
	add_keytype_from_asval[sktype]((as_val *)key, sbin);
	return true;
}

static bool
add_mapvalues_foreach(const as_val *key, const as_val *val, void *udata)
{
	(void)key;

	as_sindex_bin *sbin = (as_sindex_bin *)udata;
	as_sindex_ktype sktype = as_sindex_sktype_from_pktype(sbin->type);
	add_keytype_from_asval[sktype]((as_val *)val, sbin);
	return true;
}

static bool
add_asval_to_mapkeys_sindex(as_val *val, as_sindex_bin *sbin)
{
	as_map *map = as_map_fromval(val);

	if (as_map_foreach(map, add_mapkeys_foreach, sbin)) {
		return true;
	}

	return false;
}

static bool
add_asval_to_mapvalues_sindex(as_val *val, as_sindex_bin *sbin)
{
	as_map *map = as_map_fromval(val);

	if (as_map_foreach(map, add_mapvalues_foreach, sbin)) {
		return true;
	}

	return false;
}

typedef bool (*add_asval_to_itype_sindex_fn)(as_val *val, as_sindex_bin *sbin);
static const add_asval_to_itype_sindex_fn
add_asval_to_itype_sindex[AS_SINDEX_ITYPE_MAX] = {
	add_asval_to_default_sindex,
	add_asval_to_list_sindex,
	add_asval_to_mapkeys_sindex,
	add_asval_to_mapvalues_sindex
};
//                                   END - ADD ASVAL TO SINDEX TYPE
// ************************************************************************************************
// ************************************************************************************************
// DIFF FROM BIN TO SINDEX

static void
bin_add_skey(as_sindex_bin *sbin, const void *skey)
{
	if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		add_digest_to_sbin(sbin, *((cf_digest *)skey));
	}
	else {
		add_integer_to_sbin(sbin, *((uint64_t *)skey));
	}
}

static void
packed_val_init_unpacker(const cdt_payload *val, msgpack_in *pk)
{
	pk->buf = val->ptr;
	pk->buf_sz = val->sz;
	pk->offset = 0;
}

static bool
packed_val_make_skey(const cdt_payload *val, as_val_t type, void *skey)
{
	msgpack_in mp;
	packed_val_init_unpacker(val, &mp);

	msgpack_type packed_type = msgpack_peek_type(&mp);

	if (type == AS_STRING && packed_type == MSGPACK_TYPE_STRING) {
		uint32_t size;
		const uint8_t *ptr = msgpack_get_bin(&mp, &size);

		if (ptr == NULL) {
			return false;
		}

		if (*ptr++ != AS_BYTES_STRING) {
			return false;
		}

		cf_digest_compute(ptr, size - 1, (cf_digest *)skey);
	}
	else if (type == AS_INTEGER && msgpack_type_is_int(packed_type)) {
		if (! msgpack_get_int64(&mp, (int64_t *)skey)) {
			return false;
		}
	}
	else {
		return false;
	}

	return true;
}

static void
packed_val_add_sbin_or_update_shash(const cdt_payload *val, as_sindex_bin *sbin,
		cf_shash *hash, as_val_t type)
{
	uint8_t skey[SKEY_MAX_SIZE];

	if (! packed_val_make_skey(val, type, skey)) {
		// packed_vals that aren't of type are ignored.
		return;
	}

	bool found = false;

	if (cf_shash_get(hash, skey, &found) != CF_SHASH_OK) {
		// Item not in hash, add to sbin.
		bin_add_skey(sbin, skey);
		return;
	}

	// Item is in hash, set it to true.
	found = true;
	cf_shash_put(hash, skey, &found);
}

static void
shash_add_packed_val(cf_shash *h, const cdt_payload *val, as_val_t type, bool value)
{
	uint8_t skey[SKEY_MAX_SIZE];

	if (! packed_val_make_skey(val, type, skey)) {
		// packed_vals that aren't of type are ignored.
		return;
	}

	cf_shash_put(h, skey, &value);
}

static int
shash_diff_reduce_fn(const void *skey, void *data, void *udata)
{
	bool value = *(bool *)data;
	as_sindex_bin *sbin = (as_sindex_bin *)udata;

	if (! sbin) {
		cf_debug(AS_SINDEX, "SBIN sent as NULL");
		return -1;
	}

	if (! value) {
		// Add in the sbin.
		if (sbin->type == AS_PARTICLE_TYPE_STRING) {
			add_digest_to_sbin(sbin, *(const cf_digest*)skey);
		}
		else if (sbin->type == AS_PARTICLE_TYPE_INTEGER) {
			add_integer_to_sbin(sbin, *(const uint64_t*)skey);
		}
	}

	return 0;
}

// Find delta list elements and put them into sbins.
// Currently supports only string/integer index types.
static int32_t
as_sindex_sbins_sindex_list_diff_populate(as_sindex_bin *sbins, as_sindex *si, const as_bin *b_old, const as_bin *b_new)
{
	// Algorithm
	//	Add elements of short_list into hash with value = false
	//	Iterate through all the values in the long_list
	//		For all elements of long_list in hash, set value = true
	//		For all elements of long_list not in hash, add to sbin (insert or delete)
	//	Iterate through all the elements of hash
	//		For all elements where value == false, add to sbin (insert or delete)

	as_particle_type type = as_sindex_pktype(si->imd);

	if (type == AS_PARTICLE_TYPE_GEOJSON) {
		return -1;
	}

	cdt_payload old_val;
	cdt_payload new_val;

	as_bin_particle_list_get_packed_val(b_old, &old_val);
	as_bin_particle_list_get_packed_val(b_new, &new_val);

	msgpack_in pk_old;
	msgpack_in pk_new;

	packed_val_init_unpacker(&old_val, &pk_old);
	packed_val_init_unpacker(&new_val, &pk_new);

	uint32_t old_list_count = 0;
	uint32_t new_list_count = 0;

	if (! msgpack_get_list_ele_count(&pk_old, &old_list_count) ||
			! msgpack_get_list_ele_count(&pk_new, &new_list_count)) {
		return -1;
	}

	// Skip msgpack ext if it exist as the first element.
	if (old_list_count != 0 && msgpack_peek_is_ext(&pk_old)) {
		if (msgpack_sz(&pk_old) == 0) {
			return -1;
		}

		old_list_count--;
	}

	if (new_list_count != 0 && msgpack_peek_is_ext(&pk_new)) {
		if (msgpack_sz(&pk_new) == 0) {
			return -1;
		}

		new_list_count--;
	}

	bool old_list_is_short = old_list_count < new_list_count;

	uint32_t short_list_count;
	uint32_t long_list_count;
	msgpack_in *pk_short;
	msgpack_in *pk_long;

	if (old_list_is_short) {
		short_list_count	= (uint32_t)old_list_count;
		long_list_count		= (uint32_t)new_list_count;
		pk_short			= &pk_old;
		pk_long				= &pk_new;
	}
	else {
		short_list_count	= (uint32_t)new_list_count;
		long_list_count		= (uint32_t)old_list_count;
		pk_short			= &pk_new;
		pk_long				= &pk_old;
	}

	as_val_t val_type =
			(type == AS_PARTICLE_TYPE_STRING) ? AS_STRING : AS_INTEGER;

	if (short_list_count == 0) {
		if (long_list_count == 0) {
			return 0;
		}

		init_sbin(sbins, old_list_is_short ?
				AS_SINDEX_OP_INSERT : AS_SINDEX_OP_DELETE, type, si);

		for (uint32_t i = 0; i < long_list_count; i++) {
			cdt_payload ele;

			ele.ptr = pk_long->buf + pk_long->offset;
			ele.sz = msgpack_sz(pk_long);

			uint8_t skey[SKEY_MAX_SIZE];

			if (! packed_val_make_skey(&ele, val_type, skey)) {
				// packed_vals that aren't of type are ignored.
				continue;
			}

			bin_add_skey(sbins, skey);
		}

		return sbins->num_values == 0 ? 0 : 1;
	}

	cf_shash *hash = cf_shash_create(cf_shash_fn_u32, sbin_data_size(type), 1,
			short_list_count, 0);

	// Add elements of shorter list into hash with value = false.
	for (uint32_t i = 0; i < short_list_count; i++) {
		cdt_payload ele = {
				.ptr = pk_short->buf + pk_short->offset
		};

		uint32_t size = msgpack_sz(pk_short);

		if (size == 0) {
			cf_warning(AS_SINDEX, "as_sindex_sbins_sindex_list_diff_populate() list unpack failed");
			cf_shash_destroy(hash);
			return -1;
		}

		ele.sz = size;
		shash_add_packed_val(hash, &ele, val_type, false);
	}

	init_sbin(sbins, old_list_is_short ?
			AS_SINDEX_OP_INSERT : AS_SINDEX_OP_DELETE, type, si);

	for (uint32_t i = 0; i < long_list_count; i++) {
		cdt_payload ele;

		ele.ptr = pk_long->buf + pk_long->offset;
		ele.sz = msgpack_sz(pk_long);

		packed_val_add_sbin_or_update_shash(&ele, sbins, hash, val_type);
	}

	// Need to keep track of start for unwinding on error.
	as_sindex_bin *start_sbin = sbins;
	uint32_t found = 0;

	if (sbins->num_values > 0) {
		sbins++;
		found++;
	}

	init_sbin(sbins, old_list_is_short ?
			AS_SINDEX_OP_DELETE : AS_SINDEX_OP_INSERT, type, si);

	// Iterate through all the elements of hash.
	if (cf_shash_reduce(hash, shash_diff_reduce_fn, sbins) != 0) {
		as_sindex_sbin_freeall(start_sbin, found + 1);
		cf_shash_destroy(hash);
		return -1;
	}

	if (sbins->num_values > 0) {
		found++;
	}

	cf_shash_destroy(hash);

	return (int32_t)found;
}

// Assumes b_old and b_new are AS_PARTICLE_TYPE_LIST bins.
// Assumes b_old and b_new have the same id.
static int32_t
as_sindex_sbins_list_diff_populate(as_sindex_bin *sbins, as_namespace *ns, const char *set_name, const as_bin *b_old, const as_bin *b_new)
{
	uint16_t id = b_new->id;

	if (! binid_bit_is_set(ns, id)) {
		return 0;
	}

	cf_ll *si_ix_ll = NULL;

	if (! si_ix_list_by_defn(ns, set_name, id, &si_ix_ll)) {
		return 0;
	}

	int32_t populated = 0;

	for (cf_ll_element *ele = cf_ll_get_head(si_ix_ll); ele; ele = ele->next) {
		set_binid_hash_ele *si_ele = (set_binid_hash_ele *)ele;
		as_sindex *si = &ns->sindex[si_ele->si_ix];

		if (! as_sindex_isactive(si)) {
			continue;
		}

		// Nested CDT paths cannot be handled by this function.
		if (si->imd->path_length > 0) {
			as_sindex_sbin_freeall(sbins, (uint32_t)populated);
			return -1;
		}

		int32_t delta = as_sindex_sbins_sindex_list_diff_populate(&sbins[populated], si, b_old, b_new);

		if (delta < 0) {
			as_sindex_sbin_freeall(sbins, (uint32_t)populated);
			return -1;
		}

		populated += delta;
	}

	return populated;
}

uint32_t
as_sindex_sbins_populate(as_sindex_bin *sbins, as_namespace *ns, const char *set_name, const as_bin *b_old, const as_bin *b_new)
{
	if (as_bin_get_particle_type(b_old) == AS_PARTICLE_TYPE_LIST && as_bin_get_particle_type(b_new) == AS_PARTICLE_TYPE_LIST) {
		int32_t ret = as_sindex_sbins_list_diff_populate(sbins, ns, set_name, b_old, b_new);

		if (ret >= 0) {
			return (uint32_t)ret;
		}
	}

	uint32_t populated = 0;

	// TODO - might want an optimization that detects the (rare) case when a
	// particle was rewritten with the exact old value.
	populated += as_sindex_sbins_from_bin(ns, set_name, b_old, &sbins[populated], AS_SINDEX_OP_DELETE);
	populated += as_sindex_sbins_from_bin(ns, set_name, b_new, &sbins[populated], AS_SINDEX_OP_INSERT);

	return populated;
}
// DIFF FROM BIN TO SINDEX
// ************************************************************************************************
// ************************************************************************************************
//                                     SBIN INTERFACE FUNCTIONS
static uint32_t
sbin_from_sindex(as_sindex *si, const as_bin *b, as_sindex_bin *sbin,
		as_val **cdt_asval)
{
	as_sindex_metadata * imd    = si->imd;
	as_particle_type imd_sktype  = as_sindex_pktype(imd);
	as_val * cdt_val            = * cdt_asval;
	uint32_t  valsz             = 0;
	uint32_t sindex_found       = 0;
	as_particle_type bin_type   = 0;
	bool found = false;

	bin_type = as_bin_get_particle_type(b);

	//		Prepare si
	// 		If path_length == 0
	if (imd->path_length == 0) {
		// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
		// 				Add the value to the sbin.
		if (imd->itype == AS_SINDEX_ITYPE_DEFAULT && bin_type == imd_sktype) {
			if (bin_type == AS_PARTICLE_TYPE_INTEGER) {
				found = true;
				sbin->value.int_val = as_bin_particle_integer_value(b);

				add_integer_to_sbin(sbin, (uint64_t)sbin->value.int_val);

				if (sbin->num_values) {
					sindex_found++;
				}
			}
			else if (bin_type == AS_PARTICLE_TYPE_STRING) {
				found = true;
				char* bin_val;
				valsz = as_bin_particle_string_ptr(b, &bin_val);

				if (valsz > AS_SINDEX_MAX_STRING_KSIZE) {
					cf_ticker_warning(AS_SINDEX, "failed sindex on bin %s - string longer than %u",
							imd->bname, AS_SINDEX_MAX_STRING_KSIZE);
				}
				else {
					cf_digest buf_dig;
					cf_digest_compute(bin_val, valsz, &buf_dig);

					add_digest_to_sbin(sbin, buf_dig);

					if (sbin->num_values) {
						sindex_found++;
					}
				}
			}
			else if (bin_type == AS_PARTICLE_TYPE_GEOJSON) {
				// GeoJSON is like AS_PARTICLE_TYPE_STRING when
				// reading the value and AS_PARTICLE_TYPE_INTEGER for
				// adding the result to the index.
				found = true;
				uint64_t * cells;
				size_t ncells = as_bin_particle_geojson_cellids(b, &cells);
				for (size_t ndx = 0; ndx < ncells; ++ndx) {
					add_integer_to_sbin(sbin, cells[ndx]);
				}
				if (sbin->num_values) {
					sindex_found++;
				}
			}
		}
	}
	// 		Else if path_length > 0 OR type == MAP or LIST
	// 			Deserialize the bin if have not deserialized it yet.
	//			Extract as_val from path within the bin.
	//			Add the values to the sbin.
	if (!found) {
		if (bin_type == AS_PARTICLE_TYPE_MAP || bin_type == AS_PARTICLE_TYPE_LIST) {
			if (! cdt_val) {
				cdt_val = as_bin_particle_to_asval(b);
			}
			as_val * res_val   = as_sindex_extract_val_from_path(imd, cdt_val);
			if (!res_val) {
				goto END;
			}
			if (add_asval_to_itype_sindex[imd->itype](res_val, sbin)) {
				if (sbin->num_values) {
					sindex_found++;
				}
			}
		}
	}
END:
	*cdt_asval = cdt_val;
	return sindex_found;
}

// Returns the number of sindex found
// TODO - deprecate and conflate body with as_sindex_sbins_from_bin() below.
static uint32_t
sbins_from_bin_buf(as_namespace *ns, const char *set, const as_bin *b,
		as_sindex_bin *start_sbin, as_sindex_op op)
{
	if (as_bin_is_tombstone(b)) {
		return 0;
	}

	if (! binid_bit_is_set(ns, b->id)) {
		return 0;
	}

	cf_ll *si_ix_ll = NULL;

	if (! si_ix_list_by_defn(ns, set, b->id, &si_ix_ll)) {
		return 0;
	}

	cf_ll_element *ele = cf_ll_get_head(si_ix_ll);
	set_binid_hash_ele *si_ele = NULL;
	as_sindex *si = NULL;
	as_val *cdt_val = NULL;
	uint32_t sindex_found = 0;

	while (ele != NULL) {
		si_ele = (set_binid_hash_ele *)ele;
		si = &ns->sindex[si_ele->si_ix];

		if (! as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}

		init_sbin(&start_sbin[sindex_found], op, as_sindex_pktype(si->imd), si);

		uint64_t s_time = cf_getns();
		uint32_t sbins_in_si = sbin_from_sindex(si, b,
				&start_sbin[sindex_found], &cdt_val);

		if (sbins_in_si == 1) {
			sindex_found += sbins_in_si;
			// sbin free will happen once sbin is updated in sindex tree
			SINDEX_HIST_INSERT_DATA_POINT(si, si_prep_hist, s_time);
		}
		else {
			sbin_free(&start_sbin[sindex_found]);

			if (sbins_in_si != 0) {
				cf_warning(AS_SINDEX, "sbins found in si is neither 1 nor 0. It is %u",
						sbins_in_si);
			}
		}

		ele = ele->next;
	}

	if (cdt_val != NULL) {
		as_val_destroy(cdt_val);
	}

	return sindex_found;
}

uint32_t
as_sindex_sbins_from_bin(as_namespace *ns, const char *set, const as_bin *b,
		as_sindex_bin *start_sbin, as_sindex_op op)
{
	return sbins_from_bin_buf(ns, set, b, start_sbin, op);
}

void
as_sindex_update_by_sbin(as_sindex_bin *start_sbin, uint32_t num_sbins,
		cf_arenax_handle r_h)
{
	// Need to address sbins which have OP as AS_SINDEX_OP_DELETE before the
	// ones which have OP as AS_SINDEX_OP_INSERT. This is because same secondary
	// index key can exist in sbins with different OPs.

	for (uint32_t i = 0; i < num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_DELETE) {
			op_by_sbin(&start_sbin[i], r_h);
		}
	}

	for (uint32_t i = 0; i < num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_INSERT) {
			op_by_sbin(&start_sbin[i], r_h);
		}
	}
}
//                                 END - SBIN INTERFACE FUNCTIONS
// ************************************************************************************************
// ************************************************************************************************
//                                      PUT RD IN SINDEX
// Takes a record and tries to populate it in every sindex present in the namespace.
void
as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd, as_index_ref *r_ref)
{
	// Only called at the boot time, ns->sindex in not changed in parallel.
	for (uint32_t i = 0, a = 0; i < AS_SINDEX_MAX && a < ns->sindex_cnt; i++) {
		as_sindex *si = &ns->sindex[i];

		if (as_sindex_isactive(si)) {
			a++;

			const char *setname = as_index_get_set_name(rd->r, ns);

			// TODO: change it to set id based comparison. No set id in si.
			if (! setname_match(si->imd, setname)) {
				continue;
			}

			as_sindex_put_rd(si, rd, r_ref);
		}
	}
}

bool
as_sindex_put_rd(as_sindex *si, as_storage_rd *rd, as_index_ref *r_ref)
{
	SINDEX_GRLOCK();

	if (! as_sindex_isactive(si)) {
		SINDEX_GRUNLOCK();
		return false;
	}

	SINDEX_BINS_SETUP(sbins, 1);

	as_bin *b = as_bin_get_live(rd, si->imd->bname);

	if (b == NULL) {
		SINDEX_GRUNLOCK();
		return true;
	}

	uint32_t n_populated = 0;
	as_val *cdt_val = NULL;

	init_sbin(&sbins[n_populated], AS_SINDEX_OP_INSERT,
			as_sindex_pktype(si->imd), si);
	n_populated = sbin_from_sindex(si, b, &sbins[n_populated], &cdt_val);

	SINDEX_GRUNLOCK();

	if (cdt_val) {
		as_val_destroy(cdt_val);
	}

	if (n_populated != 0) {
		// Mark record for sindex before insertion.
		as_index *r = r_ref->r;

		as_index_set_in_sindex(r);

		as_sindex_update_by_sbin(sbins, n_populated, r_ref->r_h);
		as_sindex_sbin_freeall(sbins, n_populated);
	}

	return true;
}
//                                    END - PUT RD IN SINDEX
// ************************************************************************************************


// ************************************************************************************************
//                                           SMD CALLBACKS
/*
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *                     /|\
 *                      | 4 accept
 *                  +----------+   2
 *                  |          |<-------   +------------------+ 1 request
 *                  | SMD      | 3 merge   |  Secondary Index | <------------|
 *                  |          |<------->  |                  | 5 response   | CLIENT
 *                  |          | 4 accept  |                  | ------------>|
 *                  |          |-------->  +------------------+
 *                  +----------+
 *                     |   4 accept
 *                    \|/
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *
 *
 *  System Metadta module sits in the middle of multiple secondary index
 *  module on multiple nodes. The changes which eventually are made to the
 *  secondary index are always triggerred from SMD. Here is the flow.
 *
 *  Step1: Client send (could possibly be secondary index thread) triggers
 *         create / delete / update related to secondary index metadata.
 *
 *  Step2: The request passed through secondary index module (may be few
 *         node specific info is added on the way) to the SMD.
 *
 *  Step3: SMD send out the request to the paxos master.
 *
 *  Step4: Paxos master request the relevant metadata info from all the
 *         nodes in the cluster once it has all the data... [SMD always
 *         stores copy of the data, it is stored when the first time
 *         create happens]..it call secondary index merge callback
 *         function. The function is responsible for resolving the winning
 *         version ...
 *
 *  Step5: Once winning version is decided for all the registered module
 *         the changes are sent to all the node.
 *
 *  Step6: At each node accept_fn is called for each module. Which triggers
 *         the call to the secondary index create/delete/update functions
 *         which would be used to in-memory operation and make it available
 *         for the system.
 *
 *  There are two types of operations which look at the secondary index
 *  operations.
 *
 *  a) Normal operation .. they all look a the in-memory structure and
 *     data which is in sindex and ai_btree layer.
 *
 *  b) Other part which do DDL operation like which work through the SMD
 *     layer. Multiple operation happening from the multiple nodes which
 *     come through this layer. The synchronization is responsible of
 *     SMD layer. The part sindex / ai_btree code is responsible is to
 *     make sure when the call from the SMD comes there is proper sync
 *     between this and operation in section a
 *
 */

/*
 * This function is called when the SMD has resolved the correct state of
 * metadata. This function needs to, based on the value, looks at the current
 * state of the index and trigger requests to secondary index to do the
 * needful. At the start of time there is nothing in sindex and this code
 * comes and setup indexes
 *
 * Expectation. SMD is responsible for persisting data and communicating back
 *              to sindex layer to create in-memory structures
 *
 *
 * Description: To perform sindex operations(ADD,MODIFY,DELETE), through SMD
 * 				This function called on every node, after paxos master decides
 * 				the final version of the sindex to be created. This is the final
 *				version and the only allowed version in the sindex.Operations coming
 *				to this function are least expected to fail, ideally they should
 *				never fail.
 *
 * Parameters:
 * 		module:             SINDEX_MODULE
 * 		as_smd_item_list_t: list of action items, to be performed on sindex.
 * 		udata:              ??
 *
 * Returns:
 * 		always 0
 *
 * Synchronization:
 * 		underlying secondary index all needs to take corresponding lock and
 * 		SMD is today single threaded no sync needed there
 */

static as_sindex_ktype
ktype_from_smd_char(char c)
{
	switch (c) {
	case 'I': return COL_TYPE_LONG;
	case 'S': return COL_TYPE_DIGEST;
	case 'G': return COL_TYPE_GEOJSON;
	default:
		cf_warning(AS_SINDEX, "unknown smd ktype %c", c);
		return COL_TYPE_INVALID;
	}
}

static char
ktype_to_smd_char(as_sindex_ktype ktype)
{
	switch (ktype) {
	case COL_TYPE_LONG: return 'I';
	case COL_TYPE_DIGEST: return 'S';
	case COL_TYPE_GEOJSON: return 'G';
	default:
		cf_crash(AS_SINDEX, "unknown ktype %d", ktype);
	}
}

static as_sindex_type
itype_from_smd_char(char c)
{
	switch (c) {
	case '.': return AS_SINDEX_ITYPE_DEFAULT;
	case 'L': return AS_SINDEX_ITYPE_LIST;
	case 'K': return AS_SINDEX_ITYPE_MAPKEYS;
	case 'V': return AS_SINDEX_ITYPE_MAPVALUES;
	default:
		cf_warning(AS_SINDEX, "unknown smd type %c", c);
		return AS_SINDEX_ITYPE_MAX; // since there's no named illegal value
	}
}

static char
itype_to_smd_char(as_sindex_type itype)
{
	switch (itype) {
	case AS_SINDEX_ITYPE_DEFAULT: return '.';
	case AS_SINDEX_ITYPE_LIST: return 'L';
	case AS_SINDEX_ITYPE_MAPKEYS: return 'K';
	case AS_SINDEX_ITYPE_MAPVALUES: return 'V';
	default:
		cf_crash(AS_SINDEX, "unknown type %d", itype);
	}
}

#define TOK_CHAR_DELIMITER '|'

static bool
smd_key_to_imd(const char *smd_key, as_sindex_metadata *imd)
{
	// ns-name|<set-name>|path|itype|sktype
	// Note - sktype a.k.a. ktype and dtype.

	const char *read = smd_key;
	const char *tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - namespace name missing delimiter");
		return false;
	}

	uint32_t ns_name_len = (uint32_t)(tok - read);

	imd->ns_name = cf_malloc(ns_name_len + 1);
	memcpy(imd->ns_name, read, ns_name_len);
	imd->ns_name[ns_name_len] = 0;

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - set name missing delimiter");
		return false;
	}

	uint32_t set_name_len = (uint32_t)(tok - read);

	if (set_name_len != 0) {
		imd->set = cf_malloc(set_name_len + 1);
		memcpy(imd->set, read, set_name_len);
		imd->set[set_name_len] = 0;
	}
	// else - imd->set remains NULL.

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - path missing delimiter");
		return false;
	}

	uint32_t path_len = (uint32_t)(tok - read);

	imd->path_str = cf_malloc(path_len + 1);
	memcpy(imd->path_str, read, path_len);
	imd->path_str[path_len] = 0;

	if (! as_sindex_extract_bin_path(imd, imd->path_str)) {
		cf_warning(AS_SINDEX, "smd - can't parse path");
		return false;
	}

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - itype missing delimiter");
		return false;
	}

	imd->itype = itype_from_smd_char(*read);

	if (imd->itype == AS_SINDEX_ITYPE_MAX) {
		cf_warning(AS_SINDEX, "smd - bad itype");
		return false;
	}

	read = tok + 1;
	imd->sktype = ktype_from_smd_char(*read);

	if (imd->sktype == COL_TYPE_INVALID) {
		cf_warning(AS_SINDEX, "smd - bad sktype");
		return false;
	}

	return true;
}

static void
smd_value_to_imd(const char *smd_value, as_sindex_metadata *imd)
{
	// For now, it's only index-name
	imd->iname = cf_strdup(smd_value);
}

void
as_sindex_imd_to_smd_key(const as_sindex_metadata *imd, char *smd_key)
{
	// ns-name|<set-name>|path|itype|sktype
	// Note - sktype a.k.a. ktype and dtype.

	sprintf(smd_key, "%s|%s|%s|%c|%c",
			imd->ns_name,
			imd->set ? imd->set : "",
			imd->path_str,
			itype_to_smd_char(imd->itype),
			ktype_to_smd_char(imd->sktype));
}

bool
as_sindex_iname_to_smd_key(const as_namespace *ns, const char *iname,
		char *smd_key)
{
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, 0);

	if (! si) {
		return false;
	}

	as_sindex_imd_to_smd_key(si->imd, smd_key);

	as_sindex_release(si);

	return true;
}

static void
as_sindex_smd_accept_cb(const cf_vector *items, as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		const as_smd_item *item = cf_vector_get_ptr(items, i);
		as_sindex_metadata imd;

		memset(&imd, 0, sizeof(imd)); // TODO - arrange to use { 0 } ???

		if (! smd_key_to_imd(item->key, &imd)) {
			as_sindex_imd_free(&imd);
			continue;
		}

		as_namespace *ns = as_namespace_get_byname(imd.ns_name);

		if (! ns) {
			cf_detail(AS_SINDEX, "skipping invalid namespace %s", imd.ns_name);
			as_sindex_imd_free(&imd);
			continue;
		}

		if (ns->single_bin) {
			cf_warning(AS_SINDEX, "skipping secondary index %s on single-bin namespace %s",
					item->value != NULL ? item->value : "(null)", imd.ns_name);
			as_sindex_imd_free(&imd);
			continue;
		}

		if (item->value != NULL) {
			smd_value_to_imd(item->value, &imd); // sets index name

			if (accept_type == AS_SMD_ACCEPT_OPT_START) {
				smd_startup_create(ns, &imd);
			}
			else {
				smd_create(ns, &imd);
			}
		}
		else {
			smd_drop(ns, &imd);
		}

		as_sindex_imd_free(&imd);
	}
}
//                                     END - SMD CALLBACKS
// ************************************************************************************************

void
as_sindex_init(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->sindex = cf_calloc(AS_SINDEX_MAX, sizeof(as_sindex));
		// Note - ns->sindex_cnt will be 0.

		// TODO - if INACTIVE was zero we could skip this...
		for (uint32_t i = 0; i < AS_SINDEX_MAX; i++) {
			ns->sindex[i].state = AS_SINDEX_INACTIVE;
		}

		ns->sindex_set_binid_hash = cf_shash_create(cf_shash_fn_zstr,
				PROP_KEY_SIZE, sizeof(cf_ll *), AS_SINDEX_MAX, 0);

		ns->sindex_iname_hash = cf_shash_create(cf_shash_fn_zstr,
				AS_ID_INAME_SZ, sizeof(uint32_t), AS_SINDEX_MAX, 0);

		as_sindex_gc_ns_init(ns);
	}

	pthread_rwlockattr_t rwattr;

	if (pthread_rwlockattr_init(&rwattr) != 0) {
		cf_crash(AS_SINDEX, "pthread_rwlockattr_init: %s", cf_strerror(errno));
	}

	if (pthread_rwlockattr_setkind_np(&rwattr,
			PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP) != 0) {
		cf_crash(AS_SINDEX, "pthread_rwlockattr_setkind_np: %s",
				cf_strerror(errno));
	}

	if (pthread_rwlock_init(&g_sindex_rwlock, &rwattr) != 0) {
		cf_crash(AS_SINDEX, "pthread_rwlock_init: %s", cf_strerror(errno));
	}

	as_smd_module_load(AS_SMD_MODULE_SINDEX, as_sindex_smd_accept_cb, NULL,
			NULL);
}

// FIXME - go straight to renamed as_sindex_populate_startup()?
void
as_sindex_load(void)
{
	as_sindex_populate_startup();
}

void
as_sindex_start(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		cf_thread_create_detached(as_sindex_run_gc, g_config.namespaces[ns_ix]);
	}
}
