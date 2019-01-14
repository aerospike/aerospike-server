/*
 * secondary_index.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
 * as_sindex_sbins_from_bin -->  as_sindex_sbins_from_bin_buf
 *
 * as_sindex_sbins_from_bin_buf --> (For every macthing sindex) --> as_sindex_sbin_from_sindex
 *
 * as_sindex_sbin_from_sindex --> (If bin value macthes with sindex defn) --> as_sindex_add_asval_to_itype_sindex
 *
 * SBIN updates
 *
 * as_sindex_update_by_sbin --> For every sbin --> as_sindex__op_by_sbin
 *
 * as_sindex__op_by_sbin --> If op == AS_SINDEX_OP_INSERT --> ai_btree_put
 *                       |
 *                       --> If op == AS_SINDEX_OP_DELETE --> ai_btree_delete
 *
 * DMLs using RECORD
 *
 * as_sindex_put_rd --> For each bin in the record --> as_sindex_sbin_from_sindex
 *
 * as_sindex_putall_rd --> For each sindex --> as_sindex_put_rd
 *
 */

#include "base/secondary_index.h"

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "aerospike/as_arraylist.h"
#include "aerospike/as_arraylist_iterator.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_hashmap.h"
#include "aerospike/as_hashmap_iterator.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_pair.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"

#include "ai_btree.h"
#include "bt_iterator.h"
#include "cf_str.h"
#include "fault.h"
#include "shash.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/smd.h"
#include "base/stats.h"
#include "base/thr_sindex.h"
#include "base/thr_info.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "transaction/udf.h"


#define SINDEX_CRASH(str, ...) \
	cf_crash(AS_SINDEX, "SINDEX_ASSERT: "str, ##__VA_ARGS__);

#define AS_SINDEX_PROP_KEY_SIZE (AS_SET_NAME_MAX_SIZE + 20) // setname_binid_typeid


// ************************************************************************************************
//                                        BINID HAS SINDEX
// Maintains a bit array where binid'th bit represents the existence of atleast one index over the
// bin with bin id as binid.
// Set, reset should be called under SINDEX_GWLOCK
// get should be called under SINDEX_GRLOCK

void
as_sindex_set_binid_has_sindex(as_namespace *ns, int binid)
{
	int index     = binid / 32;
	uint32_t temp = ns->binid_has_sindex[index];
	temp         |= (1 << (binid % 32));
	ns->binid_has_sindex[index] = temp;
}

void
as_sindex_reset_binid_has_sindex(as_namespace *ns, int binid)
{
	int i          = 0;
	int j          = 0;
	as_sindex * si = NULL;

	while (i < AS_SINDEX_MAX && j < ns->sindex_cnt) {
		si = &ns->sindex[i];
		if (si != NULL) {
			if (si->state == AS_SINDEX_ACTIVE) {
				j++;
				if (si->imd->binid == binid) {
					return;
				}
			}
		}
		i++;
	}

	int index     = binid / 32;
	uint32_t temp = ns->binid_has_sindex[index];
	temp         &= ~(1 << (binid % 32));
	ns->binid_has_sindex[index] = temp;
}

bool
as_sindex_binid_has_sindex(as_namespace *ns, int binid)
{
	int index      = binid / 32;
	uint32_t temp  = ns->binid_has_sindex[index];
	return (temp & (1 << (binid % 32))) ? true : false;
}
//                                     END - BINID HAS SINDEX
// ************************************************************************************************
// ************************************************************************************************
//                                             UTILITY
// Translation from sindex error code to string. In alphabetic order
const char *as_sindex_err_str(int op_code) {
	switch (op_code) {
		case AS_SINDEX_ERR:                     return "ERR GENERIC";
		case AS_SINDEX_ERR_BIN_NOTFOUND:        return "BIN NOT FOUND";
		case AS_SINDEX_ERR_FOUND:               return "INDEX FOUND";
		case AS_SINDEX_ERR_INAME_MAXLEN:        return "INDEX NAME EXCEED MAX LIMIT";
		case AS_SINDEX_ERR_MAXCOUNT:            return "INDEX COUNT EXCEEDS MAX LIMIT";
		case AS_SINDEX_ERR_NOTFOUND:            return "NO INDEX";
		case AS_SINDEX_ERR_NOT_READABLE:        return "INDEX NOT READABLE";
		case AS_SINDEX_ERR_NO_MEMORY:           return "NO MEMORY";
		case AS_SINDEX_ERR_PARAM:               return "ERR PARAM";
		case AS_SINDEX_ERR_SET_MISMATCH:        return "SET MISMATCH";
		case AS_SINDEX_ERR_TYPE_MISMATCH:       return "KEY TYPE MISMATCH";
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:     return "UNKNOWN KEYTYPE";
		case AS_SINDEX_OK:                      return "OK";
		default:                                return "Unknown Code";
	}
}

inline bool as_sindex_isactive(as_sindex *si)
{
	if (! si) {
		cf_warning(AS_SINDEX, "si is null in as_sindex_isactive");
		return false;
	}

	return si->state == AS_SINDEX_ACTIVE;
}

// Translation from sindex internal error code to generic client visible Aerospike error code
uint8_t as_sindex_err_to_clienterr(int err, char *fname, int lineno) {
	switch (err) {
		case AS_SINDEX_ERR_FOUND:        return AS_ERR_SINDEX_FOUND;
		case AS_SINDEX_ERR_INAME_MAXLEN: return AS_ERR_SINDEX_NAME;
		case AS_SINDEX_ERR_MAXCOUNT:     return AS_ERR_SINDEX_MAX_COUNT;
		case AS_SINDEX_ERR_NOTFOUND:     return AS_ERR_SINDEX_NOT_FOUND;
		case AS_SINDEX_ERR_NOT_READABLE: return AS_ERR_SINDEX_NOT_READABLE;
		case AS_SINDEX_ERR_NO_MEMORY:    return AS_ERR_SINDEX_OOM;
		case AS_SINDEX_ERR_PARAM:        return AS_ERR_PARAMETER;
		case AS_SINDEX_OK:               return AS_OK;

		// Defensive internal error
		case AS_SINDEX_ERR:
		case AS_SINDEX_ERR_BIN_NOTFOUND:
		case AS_SINDEX_ERR_SET_MISMATCH:
		case AS_SINDEX_ERR_TYPE_MISMATCH:
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:
		default: cf_warning(AS_SINDEX, "%s %d Error at %s,%d",
				as_sindex_err_str(err), err, fname, lineno);
		return AS_ERR_SINDEX_GENERIC;
	}
}

bool
as_sindex__setname_match(as_sindex_metadata *imd, const char *setname)
{
	// NULL SET being a valid set, logic is a bit complex
	if (setname && ((!imd->set) || strcmp(imd->set, setname))) {
		goto Fail;
	}
	else if (!setname && imd->set) {
		goto Fail;
	}
	return true;
Fail:
	cf_debug(AS_SINDEX, "Index Mismatch %s %s", imd->set, setname);
	return false;
}

/* Returns
 * AS_SINDEX_GC_ERROR if cannot defrag
 * AS_SINDEX_GC_OK if can defrag
 * AS_SINDEX_GC_SKIP_ITERATION if partition lock timed out
 */
as_sindex_gc_status
as_sindex_can_defrag_record(as_namespace *ns, cf_digest *keyd)
{
	as_partition_reservation rsv;
	uint32_t pid = as_partition_getid(keyd);

	as_partition_reserve(ns, pid, &rsv);

	int rv;
	switch (as_record_exists_live(rsv.tree, keyd, rsv.ns)) {
	case 0: // found (will pass)
		rv = AS_SINDEX_GC_ERROR;
		break;
	case -1: // not found (will garbage collect)
		rv = AS_SINDEX_GC_OK;
		break;
	case -2: // can't lock (may deadlock)
		rv = AS_SINDEX_GC_SKIP_ITERATION;
		cf_atomic64_incr(&g_stats.sindex_gc_retries);
		break;
	default:
		cf_crash(AS_SINDEX, "unexpected return code");
		rv = AS_SINDEX_GC_ERROR; // shut up compiler
		break;
	}
	as_partition_release(&rsv);
	return rv;

}

/*
 * Function as_sindex_pktype
 * 		Returns the type of particle indexed
 *
 * 	Returns -
 * 		On failure - AS_SINDEX_ERR_UNKNOWN_KEYTYPE
 */
as_particle_type
as_sindex_pktype(as_sindex_metadata * imd)
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
		}
	}
	return AS_SINDEX_ERR_UNKNOWN_KEYTYPE;
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
as_sindex_ktype_from_string(char const * type_str)
{
	if (! type_str) {
		cf_warning(AS_SINDEX, "missing secondary index key type");
		return COL_TYPE_INVALID;
	}
	else if (strncasecmp(type_str, "string", 6) == 0) {
		return COL_TYPE_DIGEST;
	}
	else if (strncasecmp(type_str, "numeric", 7) == 0) {
		return COL_TYPE_LONG;
	}
	else if (strncasecmp(type_str, "geo2dsphere", 11) == 0) {
		return COL_TYPE_GEOJSON;
	}
	else {
		cf_warning(AS_SINDEX, "UNRECOGNIZED KEY TYPE %s", type_str);
		return COL_TYPE_INVALID;
	}
}

as_sindex_ktype
as_sindex_sktype_from_pktype(as_particle_type t)
{
	switch (t) {
		case AS_PARTICLE_TYPE_INTEGER :     return COL_TYPE_LONG;
		case AS_PARTICLE_TYPE_STRING  :     return COL_TYPE_DIGEST;
		case AS_PARTICLE_TYPE_GEOJSON :     return COL_TYPE_GEOJSON;
		default                       :     return COL_TYPE_INVALID;
	}
	return COL_TYPE_INVALID;
}

/*
 * Client API to check if there is secondary index on given namespace
 */
int
as_sindex_ns_has_sindex(as_namespace *ns)
{
	return (ns->sindex_cnt > 0);
}

char *as_sindex_type_defs[] =
{	"NONE", "LIST", "MAPKEYS", "MAPVALUES"
};

bool
as_sindex_can_query(as_sindex *si)
{
	// Still building. Do not allow reads
	return (si->flag & AS_SINDEX_FLAG_RACTIVE) ? true : false;
}

/*
 * Create duplicate copy of sindex metadata. New lock is created
 * used by index create by user at runtime or index creation at the boot time
 */
void
as_sindex__dup_meta(as_sindex_metadata *imd, as_sindex_metadata **qimd)
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
	qimdp->nprts       = imd->nprts;
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
as_sindex__process_ret(as_sindex *si, int ret, as_sindex_op op,
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
				cf_atomic64_incr(&si->stats.read_errs);
			}
			cf_atomic64_incr(&si->stats.n_reads);
			break;
		default:
			cf_crash(AS_SINDEX, "Invalid op");
	}
}

// Bin id should be around
// if not create it
// TODO is it not needed
int
as_sindex__populate_binid(as_namespace *ns, as_sindex_metadata *imd)
{
	size_t len = strlen(imd->bname);
	if (len >= AS_BIN_NAME_MAX_SZ) {
		cf_warning(AS_SINDEX, "bin name %s of len %zu too big. Max len allowed is %d",
							imd->bname, len, AS_BIN_NAME_MAX_SZ - 1);
		return AS_SINDEX_ERR;
	}

	if(!as_bin_name_within_quota(ns, imd->bname)) {
		cf_warning(AS_SINDEX, "Bin %s not added. Quota is full", imd->bname);
		return AS_SINDEX_ERR;
	}

	uint16_t id;

	if (! as_bin_get_or_assign_id_w_len(ns, imd->bname, len, &id)) {
		cf_warning(AS_SINDEX, "Bin %s not added. Assign id failed", imd->bname);
		return AS_SINDEX_ERR;
	}

	imd->binid = id;

	return AS_SINDEX_OK;
}

// Free if IMD has allocated the info in it
int
as_sindex_imd_free(as_sindex_metadata *imd)
{
	if (!imd) {
		cf_warning(AS_SINDEX, "imd is null in as_sindex_imd_free");
		return AS_SINDEX_ERR;
	}

	if (imd->ns_name) {
		cf_free(imd->ns_name);
		imd->ns_name = NULL;
	}

	if (imd->iname) {
		cf_free(imd->iname);
		imd->iname = NULL;
	}

	if (imd->set) {
		cf_free(imd->set);
		imd->set = NULL;
	}

	if (imd->path_str) {
		cf_free(imd->path_str);
		imd->path_str = NULL;
	}

	if (imd->bname) {
		cf_free(imd->bname);
		imd->bname = NULL;
	}

	return AS_SINDEX_OK;
}
//                                           END - UTILITY
// ************************************************************************************************
// ************************************************************************************************
//                                           METADATA
typedef struct sindex_set_binid_hash_ele_s {
	cf_ll_element ele;
	int           simatch;
} sindex_set_binid_hash_ele;

void
as_sindex__set_binid_hash_destroy(cf_ll_element * ele) {
	cf_free((sindex_set_binid_hash_ele * ) ele);
}

/*
 * Should happen under SINDEX_GWLOCK
 */
as_sindex_status
as_sindex__put_in_set_binid_hash(as_namespace * ns, char * set, int binid, int chosen_id)
{
	// Create fixed size key for hash
	// Get the linked list from the hash
	// If linked list does not exist then make one and put it in the hash
	// Append the chosen id in the linked list

	if (chosen_id < 0 || chosen_id > AS_SINDEX_MAX) {
		cf_debug(AS_SINDEX, "Put in set_binid hash got invalid simatch %d", chosen_id);
		return AS_SINDEX_ERR;
	}
	cf_ll * simatch_ll = NULL;
	// Create fixed size key for hash
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);

	if (set == NULL ) {
		sprintf(si_prop, "_%d", binid);
	}
	else {
		sprintf(si_prop, "%s_%d", set, binid);
	}

	// Get the linked list from the hash
	int rv      = cf_shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll);

	// If linked list does not exist then make one and put it in the hash
	if (rv && rv != CF_SHASH_ERR_NOT_FOUND) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR;
	};
	if (rv == CF_SHASH_ERR_NOT_FOUND) {
		simatch_ll = cf_malloc(sizeof(cf_ll));
		cf_ll_init(simatch_ll, as_sindex__set_binid_hash_destroy, false);
		cf_shash_put(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll);
	}
	if (!simatch_ll) {
		return AS_SINDEX_ERR;
	}

	// Append the chosen id in the linked list
	sindex_set_binid_hash_ele * ele = cf_malloc(sizeof(sindex_set_binid_hash_ele));
	ele->simatch                    = chosen_id;
	cf_ll_append(simatch_ll, (cf_ll_element*)ele);
	return AS_SINDEX_OK;
}

/*
 * Should happen under SINDEX_GWLOCK
 */
as_sindex_status
as_sindex__delete_from_set_binid_hash(as_namespace * ns, as_sindex_metadata * imd)
{
	// Make a key
	// Get the sindex list corresponding to key
	// If the list does not exist, return does not exist
	// If the list exist
	// 		match the path and type of incoming si to the existing sindexes in the list
	// 		If any element matches
	// 			Delete from the list
	// 			If the list size becomes 0
	// 				Delete the entry from the hash
	// 		If none of the element matches, return does not exist.
	//

	// Make a key
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);
	if (imd->set == NULL ) {
		sprintf(si_prop, "_%d", imd->binid);
	}
	else {
		sprintf(si_prop, "%s_%d", imd->set, imd->binid);
	}

	// Get the sindex list corresponding to key
	cf_ll * simatch_ll = NULL;
	int rv             = cf_shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll);

	// If the list does not exist, return does not exist
	if (rv && rv != CF_SHASH_ERR_NOT_FOUND) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR_NOTFOUND;
	};
	if (rv == CF_SHASH_ERR_NOT_FOUND) {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	// If the list exist
	// 		match the path and type of incoming si to the existing sindexes in the list
	bool    to_delete                    = false;
	cf_ll_element * ele                  = NULL;
	sindex_set_binid_hash_ele * prop_ele = NULL;
	if (simatch_ll) {
		ele = cf_ll_get_head(simatch_ll);
		while (ele) {
			prop_ele       = ( sindex_set_binid_hash_ele * ) ele;
			as_sindex * si = &(ns->sindex[prop_ele->simatch]);
			if (strcmp(si->imd->path_str, imd->path_str) == 0 &&
				si->imd->sktype == imd->sktype && si->imd->itype == imd->itype) {
				to_delete  = true;
				break;
			}
			ele = ele->next;
		}
	}
	else {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	// 		If any element matches
	// 			Delete from the list
	if (to_delete && ele) {
		cf_ll_delete(simatch_ll, ele);
	}

	// 			If the list size becomes 0
	// 				Delete the entry from the hash
	if (cf_ll_size(simatch_ll) == 0) {
		rv = cf_shash_delete(ns->sindex_set_binid_hash, si_prop);
		if (rv) {
			cf_debug(AS_SINDEX, "shash_delete fails with error %d", rv);
		}
	}

	// 		If none of the element matches, return does not exist.
	if (!to_delete) {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	return AS_SINDEX_OK;
}


//                                         END - METADATA
// ************************************************************************************************
// ************************************************************************************************
//                                             LOOKUP
/*
 * Should happen under SINDEX_GRLOCK if called directly.
 */
as_sindex_status
as_sindex__simatch_list_by_set_binid(as_namespace * ns, const char *set, int binid, cf_ll ** simatch_ll)
{
	// Make the fixed size key (set_binid)
	// Look for the key in set_binid_hash
	// If found return the value (list of simatches)
	// Else return NULL

	// Make the fixed size key (set_binid)
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);
	if (!set) {
		sprintf(si_prop, "_%d", binid);
	}
	else {
		sprintf(si_prop, "%s_%d", set, binid);
	}

	// Look for the key in set_binid_hash
	int rv             = cf_shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)simatch_ll);

	// If not found return NULL
	if (rv || !(*simatch_ll)) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR_NOTFOUND;
	};

	// Else return simatch_ll
	return AS_SINDEX_OK;
}

/*
 * Should happen under SINDEX_GRLOCK
 */
int
as_sindex__simatch_by_set_binid(as_namespace *ns, char * set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path)
{
	// get the list corresponding to the list from the hash
	// if list does not exist return -1
	// If list exist
	// 		Iterate through all the elements in the list and match the path and type
	// 		If matches
	// 			return the simatch
	// 	If none of the si matches
	// 		return -1

	cf_ll * simatch_ll = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set, binid, &simatch_ll);

	// If list exist
	// 		Iterate through all the elements in the list and match the path and type
	int     simatch                      = -1;
	sindex_set_binid_hash_ele * prop_ele = NULL;
	cf_ll_element * ele                  = NULL;
	if (simatch_ll) {
		ele = cf_ll_get_head(simatch_ll);
		while (ele) {
			prop_ele = ( sindex_set_binid_hash_ele * ) ele;
			as_sindex * si = &(ns->sindex[prop_ele->simatch]);
			if (strcmp(si->imd->path_str, path) == 0 &&
				si->imd->sktype == type && si->imd->itype == itype) {
				simatch  = prop_ele->simatch;
				break;
			}
			ele = ele->next;
		}
	}
	else {
		return -1;
	}

	// 		If matches
	// 			return the simatch
	// 	If none of the si matches
	// 		return -1
	return simatch;
}

// Populates the si_arr with all the sindexes which matches set and binid
// Each sindex is reserved as well. Enough space is provided by caller in si_arr
// Currently only 8 sindexes can be create on one combination of set and binid
// i.e number_of_sindex_types * number_of_sindex_data_type (4 * 2)
int
as_sindex_arr_lookup_by_set_binid_lockfree(as_namespace * ns, const char *set, int binid, as_sindex ** si_arr)
{
	cf_ll * simatch_ll=NULL;

	int sindex_count = 0;
	if (!as_sindex_binid_has_sindex(ns, binid) ) {
		return sindex_count;
	}

	as_sindex__simatch_list_by_set_binid(ns, set, binid, &simatch_ll);
	if (!simatch_ll) {
		return sindex_count;
	}

	cf_ll_element             * ele    = cf_ll_get_head(simatch_ll);
	sindex_set_binid_hash_ele * si_ele = NULL;
	int                        simatch = -1;
	as_sindex                 * si     = NULL;
	while (ele) {
		si_ele                         = (sindex_set_binid_hash_ele *) ele;
		simatch                        = si_ele->simatch;

		if (simatch == -1) {
			cf_warning(AS_SINDEX, "A matching simatch comes out to be -1.");
			ele = ele->next;
			continue;
		}

		si                             = &ns->sindex[simatch];
		// Reserve only active sindexes.
		// Do not break this rule
		if (!as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}

		if (simatch != si->simatch) {
			cf_warning(AS_SINDEX, "Inconsistent simatch reference between simatch stored in"
									"si and simatch stored in hash");
			ele = ele->next;
			continue;
		}

		AS_SINDEX_RESERVE(si);

		si_arr[sindex_count++] = si;
		ele = ele->next;
	}
	return sindex_count;
}

// Populates the si_arr with all the sindexes which matches setname
// Each sindex is reserved as well. Enough space is provided by caller in si_arr
int
as_sindex_arr_lookup_by_setname_lockfree(as_namespace * ns, const char *setname, as_sindex ** si_arr)
{
	int sindex_count = 0;
	as_sindex * si = NULL;

	for (int i=0; i<AS_SINDEX_MAX; i++) {
		if (sindex_count >= ns->sindex_cnt) {
			break;
		}
		si = &ns->sindex[i];
		// Reserve only active sindexes.
		// Do not break this rule
		if (!as_sindex_isactive(si)) {
			continue;
		}

		if (!as_sindex__setname_match(si->imd, setname)) {
			continue;
		}

		AS_SINDEX_RESERVE(si);

		si_arr[sindex_count++] = si;
	}

	return sindex_count;
}
int
as_sindex__simatch_by_iname(as_namespace *ns, char *idx_name)
{
	if (strlen(idx_name) >= AS_ID_INAME_SZ) {
		return -1;
	}

	char iname[AS_ID_INAME_SZ] = { 0 }; // must pad key
	strcpy(iname, idx_name);

	int simatch = -1;
	int rv = cf_shash_get(ns->sindex_iname_hash, (void *)iname, (void *)&simatch);
	cf_detail(AS_SINDEX, "Found iname simatch %s->%d rv=%d", iname, simatch, rv);

	if (rv) {
		return -1;
	}
	return simatch;
}
/*
 * Single cluttered interface for lookup. iname precedes binid
 * i.e if both are specified search is done with iname
 */
#define AS_SINDEX_LOOKUP_FLAG_SETCHECK     0x01
#define AS_SINDEX_LOOKUP_FLAG_ISACTIVE     0x02
#define AS_SINDEX_LOOKUP_FLAG_NORESERVE    0x04
as_sindex *
as_sindex__lookup_lockfree(as_namespace *ns, char *iname, char *set, int binid,
								as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{

	// If iname is not null then search in iname hash and store the simatch
	// Else then
	// 		Check the possible existence of sindex over bin in the bit array
	//		If no possibility return NULL
	//		Search in the set_binid hash using setname, binid, itype and binid
	//		If found store simatch
	//		If not found return NULL
	//			Get the sindex corresponding to the simatch.
	// 			Apply the flags applied by caller.
	//          Validate the simatch

	int simatch   = -1;
	as_sindex *si = NULL;
	// If iname is not null then search in iname hash and store the simatch
	if (iname) {
		simatch   = as_sindex__simatch_by_iname(ns, iname);
	}
	// Else then
	// 		Check the possible existence of sindex over bin in the bit array
	else {
		if (!as_sindex_binid_has_sindex(ns,  binid) ) {
	//		If no possibility return NULL
			goto END;
		}
	//		Search in the set_binid hash using setname, binid, itype and binid
	//		If found store simatch
		simatch   = as_sindex__simatch_by_set_binid(ns, set, binid, type, itype, path);
	}
	//		If not found return NULL
	// 			Get the sindex corresponding to the simatch.
	if (simatch != -1) {
		si      = &ns->sindex[simatch];
	// 			Apply the flags applied by caller.
		if ((flag & AS_SINDEX_LOOKUP_FLAG_ISACTIVE)
			&& !as_sindex_isactive(si)) {
			si = NULL;
			goto END;
		}
	//          Validate the simatch
		if (simatch != si->simatch) {
			cf_warning(AS_SINDEX, "Inconsistent simatch reference between simatch stored in"
									"si and simatch stored in hash");
		}
		if (!(flag & AS_SINDEX_LOOKUP_FLAG_NORESERVE))
			AS_SINDEX_RESERVE(si);
	}
END:
	return si;
}

as_sindex *
as_sindex__lookup(as_namespace *ns, char *iname, char *set, int binid, as_sindex_ktype type,
						as_sindex_type itype, char * path, char flag)
{
	SINDEX_GRLOCK();
	as_sindex *si = as_sindex__lookup_lockfree(ns, iname, set, binid, type, itype, path, flag);
	SINDEX_GRUNLOCK();
	return si;
}

as_sindex *
as_sindex_lookup_by_iname(as_namespace *ns, char * iname, char flag)
{
	return as_sindex__lookup(ns, iname, NULL, -1, 0, 0, NULL, flag);
}

as_sindex *
as_sindex_lookup_by_defns(as_namespace *ns, char *set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{
	return as_sindex__lookup(ns, NULL, set, binid, type, itype, path, flag);
}

as_sindex *
as_sindex_lookup_by_iname_lockfree(as_namespace *ns, char * iname, char flag)
{
	return as_sindex__lookup_lockfree(ns, iname, NULL, -1, 0, 0, NULL, flag);
}

as_sindex *
as_sindex_lookup_by_defns_lockfree(as_namespace *ns, char *set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{
	return as_sindex__lookup_lockfree(ns, NULL, set, binid, type, itype, path, flag);
}


//                                           END LOOKUP
// ************************************************************************************************
// ************************************************************************************************
//                                          STAT/CONFIG/HISTOGRAM
void
as_sindex__stats_clear(as_sindex *si) {
	as_sindex_stat *s = &si->stats;

	s->n_objects            = 0;

	s->n_reads              = 0;
	s->read_errs            = 0;

	s->n_writes             = 0;
	s->write_errs           = 0;

	s->n_deletes            = 0;
	s->delete_errs          = 0;

	s->loadtime             = 0;
	s->recs_pending         = 0;

	s->n_defrag_records     = 0;
	s->defrag_time          = 0;

	// Aggregation stat
	s->n_aggregation        = 0;
	s->agg_response_size    = 0;
	s->agg_num_records      = 0;
	s->agg_errs             = 0;
	// Lookup stats
	s->n_lookup             = 0;
	s->lookup_response_size = 0;
	s->lookup_num_records   = 0;
	s->lookup_errs          = 0;

	si->enable_histogram = false;
	if (s->_write_hist) {
		histogram_clear(s->_write_hist);
	}
	if (s->_si_prep_hist) {
		histogram_clear(s->_si_prep_hist);
	}
	if (s->_delete_hist) {
		histogram_clear(s->_delete_hist);
	}
	if (s->_query_hist) {
		histogram_clear(s->_query_hist);
	}
	if (s->_query_batch_io) {
		histogram_clear(s->_query_batch_io);
	}
	if (s->_query_batch_lookup) {
		histogram_clear(s->_query_batch_lookup);
	}
	if (s->_query_rcnt_hist) {
		histogram_clear(s->_query_rcnt_hist);
	}
	if (s->_query_diff_hist) {
		histogram_clear(s->_query_diff_hist);
	}
}

void
as_sindex_gconfig_default(as_config *c)
{
	c->sindex_builder_threads = 4;
	c->sindex_gc_max_rate = 50000; // 50,000 per second
	c->sindex_gc_period = 10; // every 10 seconds
}

void
as_sindex__config_default(as_sindex *si)
{
	si->config.flag = AS_SINDEX_FLAG_WACTIVE;
}

void
as_sindex__setup_histogram(as_sindex *si)
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

int
as_sindex__destroy_histogram(as_sindex *si)
{
	if (si->stats._write_hist)            cf_free(si->stats._write_hist);
	if (si->stats._si_prep_hist)          cf_free(si->stats._si_prep_hist);
	if (si->stats._delete_hist)           cf_free(si->stats._delete_hist);
	if (si->stats._query_hist)            cf_free(si->stats._query_hist);
	if (si->stats._query_batch_lookup)    cf_free(si->stats._query_batch_lookup);
	if (si->stats._query_batch_io)        cf_free(si->stats._query_batch_io);
	if (si->stats._query_rcnt_hist)       cf_free(si->stats._query_rcnt_hist);
	if (si->stats._query_diff_hist)       cf_free(si->stats._query_diff_hist);
	return 0;
}

int
as_sindex_stats_str(as_namespace *ns, char * iname, cf_dyn_buf *db)
{
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (!si) {
		cf_warning(AS_SINDEX, "SINDEX STAT : sindex %s not found", iname);
		return AS_SINDEX_ERR_NOTFOUND;
	}

	// A good thing to cache the stats first.
	uint64_t ns_objects  = ns->n_objects;
	uint64_t si_objects  = cf_atomic64_get(si->stats.n_objects);
	uint64_t pending     = cf_atomic64_get(si->stats.recs_pending);

	uint64_t n_keys      = ai_btree_get_numkeys(si->imd);
	uint64_t i_size      = ai_btree_get_isize(si->imd);
	uint64_t n_size      = ai_btree_get_nsize(si->imd);

	info_append_uint64(db, "keys", n_keys);
	info_append_uint64(db, "entries", si_objects);
	info_append_uint64(db, "ibtr_memory_used", i_size);
	info_append_uint64(db, "nbtr_memory_used", n_size);
	info_append_uint64(db, "si_accounted_memory", i_size + n_size);
	if (si->flag & AS_SINDEX_FLAG_RACTIVE) {
		info_append_string(db, "load_pct", "100");
	} else {
		if (pending > ns_objects) {
			info_append_uint64(db, "load_pct", 100);
		} else {
			info_append_uint64(db, "load_pct", (ns_objects == 0) ? 100 : 100 - ((100 * pending) / ns_objects));
		}
	}

	info_append_uint64(db, "loadtime", cf_atomic64_get(si->stats.loadtime));
	// writes
	info_append_uint64(db, "write_success", cf_atomic64_get(si->stats.n_writes) - cf_atomic64_get(si->stats.write_errs));
	info_append_uint64(db, "write_error", cf_atomic64_get(si->stats.write_errs));
	// delete
	info_append_uint64(db, "delete_success", cf_atomic64_get(si->stats.n_deletes) - cf_atomic64_get(si->stats.delete_errs));
	info_append_uint64(db, "delete_error", cf_atomic64_get(si->stats.delete_errs));
	// defrag
	info_append_uint64(db, "stat_gc_recs", cf_atomic64_get(si->stats.n_defrag_records));
	info_append_uint64(db, "stat_gc_time", cf_atomic64_get(si->stats.defrag_time));

	// Cache values
	uint64_t agg        = cf_atomic64_get(si->stats.n_aggregation);
	uint64_t agg_rec    = cf_atomic64_get(si->stats.agg_num_records);
	uint64_t agg_size   = cf_atomic64_get(si->stats.agg_response_size);
	uint64_t lkup       = cf_atomic64_get(si->stats.n_lookup);
	uint64_t lkup_rec   = cf_atomic64_get(si->stats.lookup_num_records);
	uint64_t lkup_size  = cf_atomic64_get(si->stats.lookup_response_size);
	uint64_t query      = agg      + lkup;
	uint64_t query_rec  = agg_rec  + lkup_rec;
	uint64_t query_size = agg_size + lkup_size;

	// Query
	info_append_uint64(db, "query_reqs", query);
	info_append_uint64(db, "query_avg_rec_count", query ? query_rec / query : 0);
	info_append_uint64(db, "query_avg_record_size", query_rec ? query_size / query_rec : 0);
	// Aggregation
	info_append_uint64(db, "query_agg", agg);
	info_append_uint64(db, "query_agg_avg_rec_count", agg ? agg_rec / agg : 0);
	info_append_uint64(db, "query_agg_avg_record_size", agg_rec ? agg_size / agg_rec : 0);
	//Lookup
	info_append_uint64(db, "query_lookups", lkup);
	info_append_uint64(db, "query_lookup_avg_rec_count", lkup ? lkup_rec / lkup : 0);
	info_append_uint64(db, "query_lookup_avg_record_size", lkup_rec ? lkup_size / lkup_rec : 0);

	info_append_bool(db, "histogram", si->enable_histogram);

	cf_dyn_buf_chomp(db);

	AS_SINDEX_RELEASE(si);
	// Release reference
	return AS_SINDEX_OK;
}

int
as_sindex_histogram_dumpall(as_namespace *ns)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	SINDEX_GRLOCK();

	for (int i = 0; i < ns->sindex_cnt; i++) {
		if (ns->sindex[i].state != AS_SINDEX_ACTIVE) continue;
		if (!ns->sindex[i].enable_histogram)         continue;
		as_sindex *si = &ns->sindex[i];
		if (si->stats._write_hist)
			histogram_dump(si->stats._write_hist);
		if (si->stats._si_prep_hist)
			histogram_dump(si->stats._si_prep_hist);
		if (si->stats._delete_hist)
			histogram_dump(si->stats._delete_hist);
		if (si->stats._query_hist)
			histogram_dump(si->stats._query_hist);
		if (si->stats._query_batch_lookup)
			histogram_dump(si->stats._query_batch_lookup);
		if (si->stats._query_batch_io)
			histogram_dump(si->stats._query_batch_io);
		if (si->stats._query_rcnt_hist)
			histogram_dump(si->stats._query_rcnt_hist);
		if (si->stats._query_diff_hist)
			histogram_dump(si->stats._query_diff_hist);
	}
	SINDEX_GRUNLOCK();
	return AS_SINDEX_OK;
}

int
as_sindex_histogram_enable(as_namespace *ns, char * iname, bool enable)
{
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		cf_warning(AS_SINDEX, "SINDEX HISTOGRAM : sindex %s not found", iname);
		return AS_SINDEX_ERR_NOTFOUND;
	}

	si->enable_histogram = enable;
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;
}

/*
 * Client API to list all the indexes in a namespace, returns list of imd with
 * index information, Caller should free it up
 */
int
as_sindex_list_str(as_namespace *ns, cf_dyn_buf *db)
{
	SINDEX_GRLOCK();
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		if (&(ns->sindex[i]) && (ns->sindex[i].imd)) {
			as_sindex si = ns->sindex[i];

			cf_dyn_buf_append_string(db, "ns=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ":set=");
			cf_dyn_buf_append_string(db, (si.imd->set) ? si.imd->set : "NULL");
			cf_dyn_buf_append_string(db, ":indexname=");
			cf_dyn_buf_append_string(db, si.imd->iname);
			cf_dyn_buf_append_string(db, ":bin=");
			cf_dyn_buf_append_buf(db, (uint8_t *)si.imd->bname, strlen(si.imd->bname));
			cf_dyn_buf_append_string(db, ":type=");
			cf_dyn_buf_append_string(db, as_sindex_ktype_str(si.imd->sktype));
			cf_dyn_buf_append_string(db, ":indextype=");
			cf_dyn_buf_append_string(db, as_sindex_type_defs[si.imd->itype]);

			cf_dyn_buf_append_string(db, ":path=");
			cf_dyn_buf_append_string(db, si.imd->path_str);

			// Index State
			if (si.state == AS_SINDEX_ACTIVE) {
				if (si.flag & AS_SINDEX_FLAG_RACTIVE) {
					cf_dyn_buf_append_string(db, ":state=RW;");
				}
				else if (si.flag & AS_SINDEX_FLAG_WACTIVE) {
					cf_dyn_buf_append_string(db, ":state=WO;");
				}
				else {
					// should never come here.
					cf_dyn_buf_append_string(db, ":state=A;");
				}
			}
			else if (si.state == AS_SINDEX_INACTIVE) {
				cf_dyn_buf_append_string(db, ":state=I;");
			}
			else {
				cf_dyn_buf_append_string(db, ":state=D;");
			}
		}
	}
	SINDEX_GRUNLOCK();
	return AS_SINDEX_OK;
}
//                                  END - STAT/CONFIG/HISTOGRAM
// ************************************************************************************************
// ************************************************************************************************
//                                         SI REFERENCE
// Reserve the sindex so it does not get deleted under the hood
int
as_sindex_reserve(as_sindex *si, char *fname, int lineno)
{
	if (! as_sindex_isactive(si)) {
		cf_warning(AS_SINDEX, "Trying to reserve sindex %s in a state other than active. State is %d",
							si->imd->iname, si->state);
	}

	if (si->imd) {
		cf_rc_reserve(si->imd);
	}

	return AS_SINDEX_OK;
}

/*
 * Release, queue up the request for the destroy to clean up Aerospike Index thread,
 * Not done inline because main write thread could release the last reference.
 */
void
as_sindex_release(as_sindex *si, char *fname, int lineno)
{
	if (! si) {
	   	return;
	}

	uint64_t val = cf_rc_release(si->imd);

	if (val == 0) {
		si->flag |= AS_SINDEX_FLAG_DESTROY_CLEANUP;
		cf_queue_push(g_sindex_destroy_q, &si);
	}
}

as_sindex_status
as_sindex_populator_reserve_all(as_namespace * ns)
{
	if (!ns) {
		cf_warning(AS_SINDEX, "namespace found NULL");
		return AS_SINDEX_ERR;
	}

	int count = 0 ;
	int valid = 0;
	SINDEX_GRLOCK();
	while (valid < ns->sindex_cnt && count < AS_SINDEX_MAX) {
		as_sindex * si = &ns->sindex[count];
		if (as_sindex_isactive(si)) {
			AS_SINDEX_RESERVE(si);
			valid++;
		}
		count++;
	}
	SINDEX_GRUNLOCK();
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_populator_release_all(as_namespace * ns)
{
	if (!ns) {
		cf_warning(AS_SINDEX, "namespace found NULL");
		return AS_SINDEX_ERR;
	}

	int count = 0 ;
	int valid = 0;
	SINDEX_GRLOCK();
	while (valid < ns->sindex_cnt && count < AS_SINDEX_MAX) {
		as_sindex * si = &ns->sindex[count];
		if (as_sindex_isactive(si)) {
			AS_SINDEX_RELEASE(si);
			valid++;
		}
		count++;
	}
	SINDEX_GRUNLOCK();
	return AS_SINDEX_OK;
}

// Complementary function of as_sindex_arr_lookup_by_set_binid
void
as_sindex_release_arr(as_sindex *si_arr[], int si_arr_sz)
{
	for (int i=0; i<si_arr_sz; i++) {
		if (si_arr[i]) {
			AS_SINDEX_RELEASE(si_arr[i]);
		}
		else {
			cf_warning(AS_SINDEX, "SI is null");
		}
	}
}

//                                    END - SI REFERENCE
// ************************************************************************************************
// ************************************************************************************************
//                                          SINDEX CREATE
// simatch is index in sindex array
// nptr is index of pimd in imd
void
as_sindex__create_pmeta(as_sindex *si, int simatch, int nptr)
{
	if (!si) {
		cf_warning(AS_SINDEX, "SI is null");
		return;
	}

	if (nptr == 0) {
		cf_warning(AS_SINDEX, "nptr is 0");
		return;
	}

	si->imd->pimd = cf_malloc(nptr * sizeof(as_sindex_pmetadata));
	memset(si->imd->pimd, 0, nptr*sizeof(as_sindex_pmetadata));

	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,
				"pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash(AS_TSVC,
				"pthread_rwlockattr_setkind_np: %s",cf_strerror(errno));

	for (int i = 0; i < nptr; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		if (pthread_rwlock_init(&pimd->slock, &rwattr)) {
			cf_crash(AS_SINDEX,
					"Could not create secondary index dml mutex ");
		}
	}
}

/*
 * Description :
 *  	Checks the parameters passed to as_sindex_create function
 *
 * Parameters:
 * 		namespace, index metadata
 *
 * Returns:
 * 		AS_SINDEX_OK            - for valid parameters.
 * 		Appropriate error codes - otherwise
 *
 * Synchronization:
 * 		This function does not explicitly acquire any lock.
 * TODO : Check if exits_by_defn can be used instead of this
 */
int
as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata* imd)
{
	SINDEX_GRLOCK();

	int ret     = AS_SINDEX_OK;
	if (ns->sindex_cnt >= AS_SINDEX_MAX) {
		ret = AS_SINDEX_ERR_MAXCOUNT;
		goto END;
	}

	int simatch = as_sindex__simatch_by_iname(ns, imd->iname);
	if (simatch != -1) {
		ret = AS_SINDEX_ERR_FOUND;
	} else {
		int16_t binid = as_bin_get_id(ns, imd->bname);
		if (binid != -1)
		{
			int simatch = as_sindex__simatch_by_set_binid(ns, imd->set, binid, imd->sktype, imd->itype, imd->path_str);
			if (simatch != -1) {
				ret = AS_SINDEX_ERR_FOUND;
				goto END;
			}
		}
	}

END:
	SINDEX_GRUNLOCK();
    return ret;
}

static int
sindex_create_lockless(as_namespace *ns, as_sindex_metadata *imd)
{
	int chosen_id = AS_SINDEX_MAX;
	as_sindex *si = NULL;
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		if (ns->sindex[i].state == AS_SINDEX_INACTIVE) {
			si = &ns->sindex[i];
			chosen_id = i;
			break;
		}
	}

	if (! si || (chosen_id == AS_SINDEX_MAX))  {
		cf_warning(AS_SINDEX, "SINDEX CREATE : Maxed out secondary index limit no more indexes allowed");
		return AS_SINDEX_ERR;
	}

	as_set *p_set = NULL;

	if (imd->set) {
		if (as_namespace_get_create_set_w_len(ns, imd->set, strlen(imd->set), &p_set, NULL) != 0) {
			cf_warning(AS_SINDEX, "SINDEX CREATE : failed get-create set %s", imd->set);
			return AS_SINDEX_ERR;
		}
	}

	imd->nprts  = ns->sindex_num_partitions;
	int id      = chosen_id;
	si          = &ns->sindex[id];
	as_sindex_metadata *qimd;

	if (as_sindex__populate_binid(ns, imd)) {
		cf_warning(AS_SINDEX, "SINDEX CREATE : Popluating bin id failed");
		return AS_SINDEX_ERR_PARAM;
	}

	as_sindex_status rv = as_sindex__put_in_set_binid_hash(ns, imd->set, imd->binid, id);
	if (rv != AS_SINDEX_OK) {
		cf_warning(AS_SINDEX, "SINDEX CREATE : Put in set_binid hash fails with error %d", rv);
		return AS_SINDEX_ERR;
	}

	cf_detail(AS_SINDEX, "Put binid simatch %d->%d", imd->binid, chosen_id);

	char iname[AS_ID_INAME_SZ];
	memset(iname, 0, AS_ID_INAME_SZ);
	snprintf(iname, strlen(imd->iname)+1, "%s", imd->iname);
	cf_shash_put(ns->sindex_iname_hash, (void *)iname, (void *)&chosen_id);
	cf_detail(AS_SINDEX, "Put iname simatch %s:%zu->%d", iname, strlen(imd->iname), chosen_id);

	// Init SI
	si->ns          = ns;
	si->simatch     = chosen_id;
	si->state       = AS_SINDEX_ACTIVE;
	si->flag        = AS_SINDEX_FLAG_WACTIVE;
	si->recreate_imd     = NULL;
	as_sindex__config_default(si);

	// Init IMD
	as_sindex__dup_meta(imd, &qimd);
	si->imd = qimd;
	qimd->si = si;

	// Init PIMD
	as_sindex__create_pmeta(si, id, imd->nprts);
	ai_btree_create(si->imd);
	as_sindex_set_binid_has_sindex(ns, si->imd->binid);


	// Update Counter
	as_sindex__setup_histogram(si);
	as_sindex__stats_clear(si);
	ns->sindex_cnt++;
	if (p_set) {
		p_set->n_sindexes++;
	} else {
		ns->n_setless_sindexes++;
	}
	cf_atomic64_add(&ns->n_bytes_sindex_memory, ai_btree_get_isize(si->imd));

	// Queue this for secondary index builder if create is done after boot.
	// At the boot time single builder request is queued for entire namespace.
	if (g_sindex_boot_done) {
		// Reserve for ref in queue
		AS_SINDEX_RESERVE(si);
		cf_queue_push(g_sindex_populate_q, &si);
	}

	return AS_SINDEX_OK;
}

int
as_sindex_create(as_namespace *ns, as_sindex_metadata *imd)
{
	// Ideally there should be one lock per namespace, but because the
	// Aerospike Index metadata is single global structure we need a overriding
	// lock for that. NB if it becomes per namespace have a file lock
	SINDEX_GWLOCK();
	if (as_sindex_lookup_by_iname_lockfree(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_NORESERVE)) {
		cf_detail(AS_SINDEX,"Index %s already exists", imd->iname);
		SINDEX_GWUNLOCK();
		return AS_SINDEX_ERR_FOUND;
	}

	int rv = sindex_create_lockless(ns, imd);
	SINDEX_GWUNLOCK();
	return rv;
}

void
as_sindex_smd_create(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();

	// FIXME - wrong place for check
	// If one node cannot have > AS_SINDEX_MAX then neither
	// can majority in cluster.
	// if (ns->sindex_cnt >= AS_SINDEX_MAX) {
	//     cf_warning(AS_SINDEX, "Failed to SMD create index '%s' on namespace '%s', maximum allowed number of indexes %d reached !!",
	//			imd->ns_name, imd->iname, ns->sindex_cnt);
	//     SINDEX_GWUNLOCK();
	//	   return;
	// }

	bool found_exact_defn = false; // ns:iname   ns:binid / set / sktype / itype / path_str
	bool found_defn = false;       //            ns:binid / set / sktype / itype / path_str
	bool found_iname = false;      // ns:iname

	int simatch_defn = -1;
	int16_t binid = as_bin_get_id(ns, imd->bname);
	if (binid != -1) {
		simatch_defn = as_sindex__simatch_by_set_binid(ns, imd->set, binid,
				imd->sktype, imd->itype, imd->path_str);
		if (simatch_defn != -1) {
			as_sindex *si = &ns->sindex[simatch_defn];
			if (! strcmp(si->imd->iname, imd->iname)) {
				found_exact_defn = true;
			} else {
				found_defn = true;
			}
		}
	}

	int simatch_iname = as_sindex__simatch_by_iname(ns, imd->iname);
	if (simatch_iname != -1) {
		found_iname = true;
	}

	if (found_exact_defn) {
		as_sindex *si = &ns->sindex[simatch_defn];
		if (si->state == AS_SINDEX_ACTIVE) {
			SINDEX_GWUNLOCK();
			return;
		}
	}

	if (found_defn) {
		as_sindex *si = &ns->sindex[simatch_defn];
		if (si->state == AS_SINDEX_ACTIVE) {
			si->state = AS_SINDEX_DESTROY;
			as_sindex_reset_binid_has_sindex(ns, si->imd->binid);
			AS_SINDEX_RELEASE(si);
		}
	}

	if (found_iname) {
		as_sindex *si = &ns->sindex[simatch_iname];
		if (si->state == AS_SINDEX_ACTIVE) {
			si->state = AS_SINDEX_DESTROY;
			as_sindex_reset_binid_has_sindex(ns, si->imd->binid);
			AS_SINDEX_RELEASE(si);
		}
	}

	// If found set setop; Use si found with same definition to set op.
	if (found_defn || found_exact_defn || found_iname) {
		if (simatch_defn != -1) {
			as_sindex *si = &ns->sindex[simatch_defn];
			as_sindex__dup_meta(imd, &si->recreate_imd);
			SINDEX_GWUNLOCK();
			return;
		}

		as_sindex *si = &ns->sindex[simatch_iname];
		as_sindex__dup_meta(imd, &si->recreate_imd);
		SINDEX_GWUNLOCK();
		return;
	}

	// Not found.
	sindex_create_lockless(ns, imd);
	SINDEX_GWUNLOCK();
	return;
}

/*
 * Description     : When a index has to be dropped and recreated during cluster state change
 * 				     this function is called.
 * Parameters      : imd, which is constructed from the final index defn given by paxos principal.
 *
 * Returns         : 0 on all cases. Check log for errors.
 *
 * Synchronization : Does not explicitly take any locks
 */
int
as_sindex_recreate(as_sindex_metadata* imd)
{
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);
	int ret          = as_sindex_create(ns, imd);
	if (ret != 0) {
		cf_warning(AS_SINDEX,"Index %s creation failed at the accept callback", imd->iname);
	}
	return 0;
}
//                                       END - SINDEX CREATE
// ************************************************************************************************
// ************************************************************************************************
//                                         SINDEX DELETE

void
as_sindex_destroy_pmetadata(as_sindex *si)
{
	for (int i = 0; i < si->imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		pthread_rwlock_destroy(&pimd->slock);
	}
	as_sindex__destroy_histogram(si);
	cf_free(si->imd->pimd);
	si->imd->pimd = NULL;
}

// TODO : Will not harm if it reserves and releases the sindex
// Keep it simple
bool
as_sindex_delete_checker(as_namespace *ns, as_sindex_metadata *imd)
{
	if (as_sindex_lookup_by_iname_lockfree(ns, imd->iname,
			AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE)) {
		return true;
	} else {
		return false;
	}
}

/*
 * Client API to destroy secondary index, mark destroy
 * Deletes via smd or info-command user-delete requests.
 */
int
as_sindex_destroy(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();
	as_sindex *si = NULL;

	if (imd->iname) {
		si = as_sindex_lookup_by_iname_lockfree(ns, imd->iname,
				AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	}
	else {
		int16_t bin_id = as_bin_get_id(ns, imd->bname);

		if (bin_id == -1) {
			SINDEX_GWUNLOCK();
			return AS_SINDEX_ERR_NOTFOUND;
		}

		si = as_sindex_lookup_by_defns_lockfree(ns, imd->set, (int)bin_id,
				imd->sktype, imd->itype, imd->path_str,
				AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	}

	if (si) {
		si->state = AS_SINDEX_DESTROY;
		as_sindex_reset_binid_has_sindex(ns, si->imd->binid);
		AS_SINDEX_RELEASE(si);
		SINDEX_GWUNLOCK();
		return AS_SINDEX_OK;
	}

	SINDEX_GWUNLOCK();
	return AS_SINDEX_ERR_NOTFOUND;
}

// On emptying a index
// 		reset objects and keys
// 		reset memory used
// 		add previous number of objects as deletes
void
as_sindex_clear_stats_on_empty_index(as_sindex *si)
{
	cf_atomic64_add(&si->stats.n_deletes, cf_atomic64_get(si->stats.n_objects));
	cf_atomic64_set(&si->stats.n_keys, 0);
	cf_atomic64_set(&si->stats.n_objects, 0);
}

void
as_sindex_empty_index(as_sindex_metadata * imd)
{
	as_sindex_pmetadata * pimd;
	cf_atomic64_sub(&imd->si->ns->n_bytes_sindex_memory,
			ai_btree_get_isize(imd) + ai_btree_get_nsize(imd));
	for (int i=0; i<imd->nprts; i++) {
		pimd = &imd->pimd[i];
		PIMD_WLOCK(&pimd->slock);
		struct btree * ibtr = pimd->ibtr;
		ai_btree_reinit_pimd(pimd, imd->sktype);
		PIMD_WUNLOCK(&pimd->slock);
		ai_btree_delete_ibtr(ibtr);
	}
	cf_atomic64_add(&imd->si->ns->n_bytes_sindex_memory,
			ai_btree_get_isize(imd));
	as_sindex_clear_stats_on_empty_index(imd->si);
}

// TODO - formerly used during set deletion - leaving it for now, but if nothing
// needs it going forward, we'll remove it.
void
as_sindex_delete_set(as_namespace * ns, char * set_name)
{
	SINDEX_GRLOCK();
	as_sindex * si_arr[ns->sindex_cnt];
	int sindex_count = as_sindex_arr_lookup_by_setname_lockfree(ns, set_name, si_arr);

	for (int i=0; i<sindex_count; i++) {
		cf_info(AS_SINDEX, "Initiating si set delete for index %s in set %s", si_arr[i]->imd->iname, set_name);
		as_sindex_empty_index(si_arr[i]->imd);
		cf_info(AS_SINDEX, "Finished si set delete for index %s in set %s", si_arr[i]->imd->iname, set_name);
	}
	SINDEX_GRUNLOCK();
	as_sindex_release_arr(si_arr, sindex_count);
}
//                                        END - SINDEX DELETE
// ************************************************************************************************
// ************************************************************************************************
//                                         SINDEX POPULATE
/*
 * Client API to mark index population finished, tick it ready for read
 */
int
as_sindex_populate_done(as_sindex *si)
{
	// Setting flag is atomic: meta lockless
	si->flag |= AS_SINDEX_FLAG_RACTIVE;
	si->flag &= ~AS_SINDEX_FLAG_POPULATING;
	return AS_SINDEX_OK;
}
/*
 * Client API to start namespace scan to populate secondary index. The scan
 * is only performed in the namespace is warm start or if its data is not in
 * memory and data is loaded from. For cold start with data in memory the indexes
 * are populate upfront.
 *
 * This call is only made at the boot time.
 */
int
as_sindex_boot_populateall()
{
	// Initialize the secondary index builder. The thread pool is initialized
	// with maximum threads to go full throttle, then down-sized to the
	// configured number after the startup population job is done.
	as_sbld_init();

	int ns_cnt = 0;

	// Trigger namespace scan to populate all secondary indexes
	// mark all secondary index for a namespace as populated
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		if (!ns || (ns->sindex_cnt == 0)) {
			continue;
		}

		if (! ns->storage_data_in_memory) {
			// Data-not-in-memory (cold or warm restart) - have not yet built
			// sindex, build it now.
			as_sindex_populator_reserve_all(ns);
			as_sbld_build_all(ns);
			cf_info(AS_SINDEX, "Queuing namespace %s for sindex population ", ns->name);
		} else {
			// Data-in-memory (cold or cool restart) - already built sindex.
			as_sindex_boot_populateall_done(ns);
		}
		ns_cnt++;
	}
	for (int i = 0; i < ns_cnt; i++) {
		int ret;
		// blocking call, wait till an item is popped out of Q :
		cf_queue_pop(g_sindex_populateall_done_q, &ret, CF_QUEUE_FOREVER);
		// TODO: Check for failure .. is generally fatal if it fails
	}

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		if (!ns || (ns->sindex_cnt == 0)) {
			continue;
		}

		if (! ns->storage_data_in_memory) {
			// Data-not-in-memory - finished sindex building job.
			as_sindex_populator_release_all(ns);
		}
	}

	// Down-size builder thread pool to configured value.
	as_sbld_resize_thread_pool(g_config.sindex_builder_threads);

	g_sindex_boot_done = true;

	return AS_SINDEX_OK;
}

/*
 * Client API to mark all the indexes in namespace populated and ready for read
 */
int
as_sindex_boot_populateall_done(as_namespace *ns)
{
	SINDEX_GWLOCK();
	int ret = AS_SINDEX_OK;

	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		if (!as_sindex_isactive(si))  continue;
		// This sindex is getting populating by it self scan
		if (si->flag & AS_SINDEX_FLAG_POPULATING) continue;
		si->flag |= AS_SINDEX_FLAG_RACTIVE;
	}
	SINDEX_GWUNLOCK();
	cf_queue_push(g_sindex_populateall_done_q, &ret);
	cf_info(AS_SINDEX, "Namespace %s sindex population done", ns->name);
	return ret;
}

//                                            END - SINDEX POPULATE
// ************************************************************************************************
// ************************************************************************************************
//                                       SINDEX BIN PATH
as_sindex_status
as_sindex_add_mapkey_in_path(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	if (end < start) {
		return AS_SINDEX_ERR;
	}

	int path_length = imd->path_length;
	char int_str[20];
	strncpy(int_str, path_str+start, end-start+1);
	int_str[end-start+1] = '\0';
	char * str_part;
	imd->path[path_length-1].value.key_int = strtol(int_str, &str_part, 10);
	if (str_part == int_str || (*str_part != '\0')) {
		imd->path[path_length-1].value.key_str  = cf_strndup(int_str, strlen(int_str)+1);
		imd->path[path_length-1].mapkey_type = AS_PARTICLE_TYPE_STRING;
	}
	else {
		imd->path[path_length-1].mapkey_type = AS_PARTICLE_TYPE_INTEGER;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_listelement_in_path(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	if (end < start) {
		return AS_SINDEX_ERR;
	}
	int path_length = imd->path_length;
	char int_str[10];
	strncpy(int_str, path_str+start, end-start+1);
	int_str[end-start+1] = '\0';
	char * str_part;
	imd->path[path_length-1].value.index = strtol(int_str, &str_part, 10);
	if (str_part == int_str || (*str_part != '\0')) {
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_parse_subpath(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	int path_len = strlen(path_str);
	bool overflow = end >= path_len ? true : false;

	if (start == 0 ) {
		if (overflow) {
			imd->bname = cf_strndup(path_str+start, end-start);
		}
		else if (path_str[end] == '.') {
			imd->bname = cf_strndup(path_str+start, end-start);
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			imd->bname = cf_strndup(path_str+start, end-start);
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == '.') {
		if (overflow) {
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
		}
		else if (path_str[end] == '.') {
			// take map value
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			// value
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == '[') {
		if (!overflow && path_str[end] == ']') {
			//take list value
			if (as_sindex_add_listelement_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == ']') {
		if (end - start != 1) {
			return AS_SINDEX_ERR;
		}
		else if (overflow) {
			return AS_SINDEX_OK;
		}
		if (path_str[end] == '.') {
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else {
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
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
as_sindex_status
as_sindex_extract_bin_path(as_sindex_metadata * imd, char * path_str)
{
	int    path_len    = strlen(path_str);
	int    start       = 0;
	int    end         = 0;
	if (path_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_SINDEX, "Bin path length exceeds the maximum allowed.");
		return AS_SINDEX_ERR;
	}
	// Iterate through the path_str and search for character (., [, ])
	// which leads to sublevels in maps and lists
	while (end < path_len) {
		if (path_str[end] == '.' || path_str[end] == '[' || path_str[end] == ']') {
			if (as_sindex_parse_subpath(imd, path_str, start, end)!=AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			start = end;
			if (imd->path_length >= AS_SINDEX_MAX_DEPTH) {
				cf_warning(AS_SINDEX, "Bin position depth level exceeds the max depth allowed %d", AS_SINDEX_MAX_DEPTH);
				return AS_SINDEX_ERR;
			}
		}
		end++;
	}
	if (as_sindex_parse_subpath(imd, path_str, start, end)!=AS_SINDEX_OK) {
		return AS_SINDEX_ERR;
	}
/*
// For debugging
	cf_info(AS_SINDEX, "After parsing : bin name: %s", imd->bname);
	for (int i=0; i<imd->path_length; i++) {
		if(imd->path[i].type == AS_PARTICLE_TYPE_MAP ) {
			if (imd->path[i].key_type == AS_PARTICLE_TYPE_INTEGER) {
				cf_info(AS_SINDEX, "map key_int %d", imd->path[i].value.key_int);
			}
			else if (imd->path[i].key_type == AS_PARTICLE_TYPE_STRING){
				cf_info(AS_SINDEX, "map key_str %s", imd->path[i].value.key_str);
			}
			else {
				cf_info(AS_SINDEX, "ERROR EEROR EERROR ERRROR REERROR");
			}
		}
		else{
			cf_info(AS_SINDEX, "list index %d", imd->path[i].value.index);
		}
	}
*/
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_extract_bin_from_path(char * path_str, char *bin)
{
	int    path_len    = strlen(path_str);
	int    end         = 0;
	if (path_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_SINDEX, "Bin path length exceeds the maximum allowed.");
		return AS_SINDEX_ERR;
	}

	while (end < path_len && path_str[end] != '.' && path_str[end] != '[' && path_str[end] != ']') {
		end++;
	}

	if (end > 0 && end < AS_BIN_NAME_MAX_SZ) {
		strncpy(bin, path_str, end);
		bin[end] = '\0';
	}
	else {
		return AS_SINDEX_ERR;
	}

	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_destroy_value_path(as_sindex_metadata * imd)
{
	for (int i=0; i<imd->path_length; i++) {
		if (imd->path[i].type == AS_PARTICLE_TYPE_MAP &&
				imd->path[i].mapkey_type == AS_PARTICLE_TYPE_STRING) {
			cf_free(imd->path[i].value.key_str);
		}
	}
	return AS_SINDEX_OK;
}

/*
 * This function checks the existence of path stored in the sindex metadata
 * in a bin
 */
as_val *
as_sindex_extract_val_from_path(as_sindex_metadata * imd, as_val * v)
{
	if (!v) {
		return NULL;
	}

	as_val * val = v;

	as_particle_type imd_sktype = as_sindex_pktype(imd);
	if (imd->path_length == 0) {
		goto END;
	}
	as_sindex_path *path = imd->path;
	for (int i=0; i<imd->path_length; i++) {
		switch (val->type) {
			case AS_STRING:
			case AS_INTEGER:
				return NULL;
			case AS_LIST: {
				if (path[i].type != AS_PARTICLE_TYPE_LIST) {
					return NULL;
				}
				int index = path[i].value.index;
				as_arraylist* list  = (as_arraylist*) as_list_fromval(val);
				as_arraylist_iterator it;
				as_arraylist_iterator_init( &it, list);
				int j = 0;
				while( as_arraylist_iterator_has_next( &it) && j<=index) {
					val = (as_val*) as_arraylist_iterator_next( &it);
					j++;
				}
				if (j-1 != index ) {
					return NULL;
				}
				break;
			}
			case AS_MAP: {
				if (path[i].type != AS_PARTICLE_TYPE_MAP) {
					return NULL;
				}
				as_map * map = as_map_fromval(val);
				as_val * key;
				if (path[i].mapkey_type == AS_PARTICLE_TYPE_STRING) {
					key = (as_val *)as_string_new(path[i].value.key_str, false);
				}
				else if (path[i].mapkey_type == AS_PARTICLE_TYPE_INTEGER) {
					key = (as_val *)as_integer_new(path[i].value.key_int);
				}
				else {
					cf_warning(AS_SINDEX, "Possible false data in sindex metadata");
					return NULL;
				}
				val = as_map_get(map, key);
				if (key) {
					as_val_destroy(key);
				}
				if ( !val ) {
					return NULL;
				}
				break;
			}
			default:
				return NULL;
		}
	}

END:
	if (imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
		if (val->type == AS_INTEGER && imd_sktype == AS_PARTICLE_TYPE_INTEGER) {
			return val;
		}
		else if (val->type == AS_STRING && imd_sktype == AS_PARTICLE_TYPE_STRING) {
			return val;
		}
	}
	else if (imd->itype == AS_SINDEX_ITYPE_MAPKEYS ||  imd->itype == AS_SINDEX_ITYPE_MAPVALUES) {
		if (val->type == AS_MAP) {
			return val;
		}
	}
	else if (imd->itype == AS_SINDEX_ITYPE_LIST) {
		if (val->type == AS_LIST) {
			return val;
		}
	}
	return NULL;
}
//                                        END - SINDEX BIN PATH
// ************************************************************************************************
// ************************************************************************************************
//                                                SINDEX QUERY
/*
 * Returns -
 * 		NULL - On failure
 * 		si   - On success.
 * Notes -
 * 		Reserves the si if found in the srange
 * 		Releases the si if imd is null or bin type is mis matched.
 *
 */
as_sindex *
as_sindex_from_range(as_namespace *ns, char *set, as_sindex_range *srange)
{
	cf_debug(AS_SINDEX, "as_sindex_from_range");
	if (ns->single_bin) {
		cf_warning(AS_SINDEX, "Secondary index query not allowed on single bin namespace %s", ns->name);
		return NULL;
	}
	as_sindex *si = as_sindex_lookup_by_defns(ns, set, srange->start.id,
						as_sindex_sktype_from_pktype(srange->start.type), srange->itype, srange->bin_path,
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (si && si->imd) {
		// Do the type check
		as_sindex_metadata *imd = si->imd;
		if ((imd->binid == srange->start.id) && (srange->start.type != as_sindex_pktype(imd))) {
			cf_warning(AS_SINDEX, "Query and Index Bin Type Mismatch: "
					"[binid %d : Index Bin type %d : Query Bin Type %d]",
					imd->binid, as_sindex_pktype(imd), srange->start.type );
			AS_SINDEX_RELEASE(si);
			return NULL;
		}
	}
	return si;
}

/*
 * The way to filter out imd information from the as_msg which is primarily
 * query with all the details. For the normal operations the imd is formed out
 * of the as_op.
 */
/*
 * Returns -
 * 		NULL      - On failure.
 * 		as_sindex - On success.
 *
 * Description -
 * 		Firstly obtains the simatch using ns name and set name.
 * 		Then returns the corresponding slot from sindex array.
 *
 * TODO
 * 		log messages
 */
as_sindex *
as_sindex_from_msg(as_namespace *ns, as_msg *msgp)
{
	cf_debug(AS_SINDEX, "as_sindex_from_msg");
	as_msg_field *ifp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_NAME);

	if (!ifp) {
		cf_debug(AS_SINDEX, "Index name not found in the query request");
		return NULL;
	}

	uint32_t iname_len = as_msg_field_get_value_sz(ifp);

	if (iname_len >= AS_ID_INAME_SZ) {
		cf_warning(AS_SINDEX, "index name too long");
		return NULL;
	}

	char iname[AS_ID_INAME_SZ];

	memcpy(iname, ifp->data, iname_len);
	iname[iname_len] = 0;

	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		cf_detail(AS_SINDEX, "Search did not find index ");
	}

	return si;
}


/*
 * Internal Function - as_sindex_range_free
 * 		frees the sindex range
 *
 * Returns
 * 		AS_SINDEX_OK - In every case
 */
int
as_sindex_range_free(as_sindex_range **range)
{
	cf_debug(AS_SINDEX, "as_sindex_range_free");
	as_sindex_range *sk = (*range);
	if (sk->region) {
		geo_region_destroy(sk->region);
	}
	cf_free(sk);
	return AS_SINDEX_OK;
}

/*
 * Extract out range information from the as_msg and create the irange structure
 * if required allocates the memory.
 * NB: It is responsibility of caller to call the cleanup routine to clean the
 * range structure up and free up its memory
 *
 * query range field layout: contains - numranges, binname, start, end
 *
 * generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numranges
 * 5   1 numranges (max 255 ranges)
 *
 * binname
 * 6   1 binnamelen b
 * 7   b binname
 *
 * particle (start & end)
 * +b    1 particle_type
 * +b+1  4 start_particle_size x
 * +b+5  x start_particle_data
 * +b+5+x      4 end_particle_size y
 * +b+5+x+y+4   y end_particle_data
 *
 * repeat "numranges" times from "binname"
 */

/*
 * Function as_sindex_binlist_from_msg
 *
 * Returns -
 * 		binlist - On success
 * 		NULL    - On failure
 *
 */
cf_vector *
as_sindex_binlist_from_msg(as_namespace *ns, as_msg *msgp, int * num_bins)
{
	cf_debug(AS_SINDEX, "as_sindex_binlist_from_msg");
	as_msg_field *bfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_QUERY_BINLIST);
	if (!bfp) {
		return NULL;
	}
	const uint8_t *data = bfp->data;
	int numbins         = *data++;
	*num_bins           = numbins;

	cf_vector *binlist  = cf_vector_create(AS_BIN_NAME_MAX_SZ, numbins, 0);

	for (int i = 0; i < numbins; i++) {
		int binnamesz = *data++;
		if (binnamesz <= 0 || binnamesz >= AS_BIN_NAME_MAX_SZ) {
			cf_warning(AS_SINDEX, "Size of the bin name in bin list of sindex query is out of bounds. Size %d", binnamesz);
			cf_vector_destroy(binlist);
			return NULL;
		}
		char binname[AS_BIN_NAME_MAX_SZ];
		memcpy(&binname, data, binnamesz);
		binname[binnamesz] = 0;
		cf_vector_set(binlist, i, (void *)binname);
		data     += binnamesz;
	}

	cf_debug(AS_SINDEX, "Queried Bin List %d ", numbins);
	for (int i = 0; i < cf_vector_size(binlist); i++) {
		char binname[AS_BIN_NAME_MAX_SZ];
		cf_vector_get(binlist, i, (void*)&binname);
		cf_debug(AS_SINDEX,  " String Queried is |%s| \n", binname);
	}

	return binlist;
}

/*
 * Returns -
 *		AS_SINDEX_OK        - On success.
 *		AS_SINDEX_ERR_PARAM - On failure.
 *		AS_SINDEX_ERR_BIN_NOTFOUND - On failure.
 *
 * Description -
 *		Frames a sane as_sindex_range from msg.
 *
 *		We are not supporting multiranges right now. So numrange is always expected to be 1.
 */
int
as_sindex_range_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range *srange)
{
	cf_debug(AS_SINDEX, "as_sindex_range_from_msg");
	srange->num_binval = 0;
	// Ensure region is initialized in case we need to return an error code early.
	srange->region = NULL;

	// getting ranges
	as_msg_field *itype_fp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_TYPE);
	as_msg_field *rfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_RANGE);
	if (!rfp) {
		cf_warning(AS_SINDEX, "Required Index Range Not Found");
		return AS_SINDEX_ERR_PARAM;
	}
	const uint8_t *data = rfp->data;
	int numrange        = *data++;

	if (numrange != 1) {
		cf_warning(AS_SINDEX,
					"can't handle multiple ranges right now %d", rfp->data[0]);
		return AS_SINDEX_ERR_PARAM;
	}
	// NOTE - to support geospatial queries the srange object is actually a vector
	// of MAX_REGION_CELLS elements.  Normal queries only use the first element.
	// Geospatial queries use multiple elements.
	//
	memset(srange, 0, sizeof(as_sindex_range) * MAX_REGION_CELLS);
	if (itype_fp) {
		srange->itype = *itype_fp->data;
	}
	else {
		srange->itype = AS_SINDEX_ITYPE_DEFAULT;
	}
	for (int i = 0; i < numrange; i++) {
		as_sindex_bin_data *start = &(srange->start);
		as_sindex_bin_data *end   = &(srange->end);
		// Populate Bin id
		uint8_t bin_path_len         = *data++;
		if (bin_path_len >= AS_SINDEX_MAX_PATH_LENGTH) {
			cf_warning(AS_SINDEX, "Index position size %d exceeds the max length %d", bin_path_len, AS_SINDEX_MAX_PATH_LENGTH);
			return AS_SINDEX_ERR_PARAM;
		}

		strncpy(srange->bin_path, (char *)data, bin_path_len);
		srange->bin_path[bin_path_len] = '\0';

		char binname[AS_BIN_NAME_MAX_SZ];
		if (as_sindex_extract_bin_from_path(srange->bin_path, binname) == AS_SINDEX_OK) {
			int16_t id = as_bin_get_id(ns, binname);
			if (id != -1) {
				start->id   = id;
				end->id     = id;
			} else {
				return AS_SINDEX_ERR_BIN_NOTFOUND;
			}
		}
		else {
			return AS_SINDEX_ERR_PARAM;
		}

		data       += bin_path_len;

		// Populate type
		int type    = *data++;
		start->type = type;
		end->type   = start->type;

		// TODO - Refactor these into generic conversion from
		// buffer to as_sindex_bin_data functions. Can be used
		// by write code path as well.
		if ((type == AS_PARTICLE_TYPE_INTEGER)) {
			// get start point
			uint32_t startl  = ntohl(*((uint32_t *)data));
			data            += sizeof(uint32_t);
			if (startl != 8) {
				cf_warning(AS_SINDEX,
					"Can only handle 8 byte numerics right now %u", startl);
				goto Cleanup;
			}
			start->u.i64  = cf_swap_from_be64(*((uint64_t *)data));
			data         += sizeof(uint64_t);

			// get end point
			uint32_t endl = ntohl(*((uint32_t *)data));
			data         += sizeof(uint32_t);
			if (endl != 8) {
				cf_warning(AS_SINDEX,
						"can only handle 8 byte numerics right now %u", endl);
				goto Cleanup;
			}
			end->u.i64  = cf_swap_from_be64(*((uint64_t *)data));
			data       += sizeof(uint64_t);
			if (start->u.i64 > end->u.i64) {
				cf_warning(AS_SINDEX,
                     "Invalid range from %ld to %ld", start->u.i64, end->u.i64);
				goto Cleanup;
			} else {
				srange->isrange = start->u.i64 != end->u.i64;
			}
			cf_debug(AS_SINDEX, "Range is equal  %"PRId64", %"PRId64"",
								start->u.i64, end->u.i64);
		} else if (type == AS_PARTICLE_TYPE_STRING) {
			// get start point
			uint32_t startl    = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			char* start_binval       = (char *)data;
			data              += startl;
			srange->isrange    = false;

			if (startl >= AS_SINDEX_MAX_STRING_KSIZE) {
				cf_warning(AS_SINDEX, "Query on bin %s fails. Value length %u too long.", binname, startl);
				goto Cleanup;
			}
			uint32_t endl	   = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			char * end_binval        = (char *)data;
			if (startl != endl && strncmp(start_binval, end_binval, startl)) {
				cf_warning(AS_SINDEX,
                           "Only Equality Query Supported in Strings %s-%s",
                           start_binval, end_binval);
				goto Cleanup;
			}
			cf_digest_compute(start_binval, startl, &(start->digest));
			cf_debug(AS_SINDEX, "Range is equal %s ,%s",
					 start_binval, end_binval);
		} else if (type == AS_PARTICLE_TYPE_GEOJSON) {
			// get start point
			uint32_t startl = ntohl(*((uint32_t *)data));
			data += sizeof(uint32_t);
			char* start_binval = (char *)data;
			data += startl;

			if ((startl == 0) || (startl >= AS_SINDEX_MAX_GEOJSON_KSIZE)) {
				cf_warning(AS_SINDEX, "Out of bound query key size %u", startl);
				goto Cleanup;
			}
			uint32_t endl = ntohl(*((uint32_t *)data));
			data += sizeof(uint32_t);
			char * end_binval = (char *)data;
			if (startl != endl && strncmp(start_binval, end_binval, startl)) {
				cf_warning(AS_SINDEX,
						   "Only Geospatial Query Supported on GeoJSON %s-%s",
						   start_binval, end_binval);
				goto Cleanup;
			}

			srange->cellid = 0;
			if (!geo_parse(ns, start_binval, startl,
						   &srange->cellid, &srange->region)) {
				cf_warning(AS_GEO, "failed to parse query GeoJSON");
				goto Cleanup;
			}

			if (srange->cellid && srange->region) {
				geo_region_destroy(srange->region);
				srange->region = NULL;
				cf_warning(AS_GEO, "query geo_parse: both point and region");
				goto Cleanup;
			}

			if (!srange->cellid && !srange->region) {
				cf_warning(AS_GEO, "query geo_parse: neither point nor region");
				goto Cleanup;
			}

			if (srange->cellid) {
				// REGIONS-CONTAINING-POINT QUERY

				uint64_t center[MAX_REGION_LEVELS];
				int numcenters;
				if (!geo_point_centers(ns, srange->cellid, MAX_REGION_LEVELS,
									   center, &numcenters)) {
					cf_warning(AS_GEO, "Query point invalid");
					goto Cleanup;
				}

				// Geospatial queries use multiple srange elements.	 Many
				// of the fields are copied from the first cell because
				// they were filled in above.
				for (int ii = 0; ii < numcenters; ++ii) {
					srange[ii].num_binval = 1;
					srange[ii].isrange = true;
					srange[ii].start.id = srange[0].start.id;
					srange[ii].start.type = srange[0].start.type;
					srange[ii].start.u.i64 = center[ii];
					srange[ii].end.id = srange[0].end.id;
					srange[ii].end.type = srange[0].end.type;
					srange[ii].end.u.i64 = center[ii];
					srange[ii].itype = srange[0].itype;
				}
			} else {
				// POINTS-INSIDE-REGION QUERY

				uint64_t cellmin[MAX_REGION_CELLS];
				uint64_t cellmax[MAX_REGION_CELLS];
				int numcells;
				if (!geo_region_cover(ns, srange->region, MAX_REGION_CELLS,
									  NULL, cellmin, cellmax, &numcells)) {
					cf_warning(AS_GEO, "Query region invalid.");
					goto Cleanup;
				}

				cf_atomic64_incr(&ns->geo_region_query_count);
				cf_atomic64_add(&ns->geo_region_query_cells, numcells);

				// Geospatial queries use multiple srange elements.	 Many
				// of the fields are copied from the first cell because
				// they were filled in above.
				for (int ii = 0; ii < numcells; ++ii) {
					srange[ii].num_binval = 1;
					srange[ii].isrange = true;
					srange[ii].start.id = srange[0].start.id;
					srange[ii].start.type = srange[0].start.type;
					srange[ii].start.u.i64 = cellmin[ii];
					srange[ii].end.id = srange[0].end.id;
					srange[ii].end.type = srange[0].end.type;
					srange[ii].end.u.i64 = cellmax[ii];
					srange[ii].itype = srange[0].itype;
				}
			}
		} else {
			cf_warning(AS_SINDEX, "Only handle String, Numeric and GeoJSON type");
			goto Cleanup;
		}
		srange->num_binval = numrange;
	}
	return AS_SINDEX_OK;

Cleanup:
	return AS_SINDEX_ERR_PARAM;
}

/*
 * Function as_sindex_rangep_from_msg
 *
 * Arguments
 * 		ns     - the namespace on which srange has to be build
 * 		msgp   - the msgp from which sent
 * 		srange - it builds this srange
 *
 * Returns
 * 		AS_SINDEX_OK - On success
 * 		else the return value of as_sindex_range_from_msg
 *
 * Description
 * 		Allocating space for srange and then calling as_sindex_range_from_msg.
 */
int
as_sindex_rangep_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range **srange)
{
	cf_debug(AS_SINDEX, "as_sindex_rangep_from_msg");

	// NOTE - to support geospatial queries we allocate an array of
	// MAX_REGION_CELLS length.	 Nongeospatial queries use only the
	// first element.  Geospatial queries use one element per region
	// cell, up to MAX_REGION_CELLS.
	*srange         = cf_malloc(sizeof(as_sindex_range) * MAX_REGION_CELLS);

	int ret = as_sindex_range_from_msg(ns, msgp, *srange);
	if (AS_SINDEX_OK != ret) {
		as_sindex_range_free(srange);
		*srange = NULL;
		return ret;
	}
	return AS_SINDEX_OK;
}

/*
 * Returns -
 * 		AS_SINDEX_ERR_PARAM
 *		o/w return value from ai_btree_query
 *
 * Notes -
 * 		Client API to do range get from index based on passed in range key, returns
 * 		digest list
 *
 * Synchronization -
 *
 */
int
as_sindex_query(as_sindex *si, as_sindex_range *srange, as_sindex_qctx *qctx)
{
	if (! si || ! srange) {
		return AS_SINDEX_ERR_PARAM;
	}

	as_sindex_metadata *imd = si->imd;
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_idx];

	if (! as_sindex_can_query(si)) {
		return AS_SINDEX_ERR_NOT_READABLE;
	}

	PIMD_RLOCK(&pimd->slock);
	int ret = ai_btree_query(imd, srange, qctx);
	PIMD_RUNLOCK(&pimd->slock);

	as_sindex__process_ret(si, ret, AS_SINDEX_OP_READ,
			0 /* No histogram for query per call */, __LINE__);

	return ret;
}
//                                        END -  SINDEX QUERY
// ************************************************************************************************
// ************************************************************************************************
//                                          SBIN UTILITY
void
as_sindex_init_sbin(as_sindex_bin * sbin, as_sindex_op op, as_particle_type type, as_sindex * si)
{
	sbin->si              = si;
	sbin->to_free         = false;
	sbin->num_values      = 0;
	sbin->op              = op;
	sbin->heap_capacity   = 0;
	sbin->type            = type;
	sbin->values          = NULL;
}

int
as_sindex_sbin_free(as_sindex_bin *sbin)
{
	if (sbin->to_free) {
		if (sbin->values) {
			cf_free(sbin->values);
		}
	}
    return AS_SINDEX_OK;
}

int
as_sindex_sbin_freeall(as_sindex_bin *sbin, int numbins)
{
	for (int i = 0; i < numbins; i++)  {
		as_sindex_sbin_free(&sbin[i]);
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex__op_by_sbin(as_namespace *ns, const char *set, int numbins, as_sindex_bin *start_sbin, cf_digest * pkey)
{
	// If numbins == 0 return AS_SINDEX_OK
	// Iterate through sbins
	// 		Reserve the SI.
	// 		Take the read lock on imd
	//		Get a value from sbin
	//			Get the related pimd
	//			Get the pimd write lock
	//			If op is DELETE delete the values from sbin from sindex
	//			If op is INSERT put all the values from bin in sindex.
	//			Release the pimd lock
	//		Release the imd lock.
	//		Release the SI.

	as_sindex_status retval = AS_SINDEX_OK;
	if (!ns || !start_sbin) {
		return AS_SINDEX_ERR;
	}

	// If numbins != 1 return AS_SINDEX_OK
	if (numbins != 1 ) {
		return AS_SINDEX_OK;
	}

	as_sindex * si             = NULL;
	as_sindex_bin * sbin   = NULL;
	as_sindex_metadata * imd   = NULL;
	as_sindex_pmetadata * pimd = NULL;
	as_sindex_op op;
	// Iterate through sbins
	for (int i=0; i<numbins; i++) {
	// 		Reserve the SI.
		sbin = &start_sbin[i];
		si = sbin->si;
		if (!si) {
			cf_warning(AS_SINDEX, "as_sindex_op_by_sbin : si is null in sbin");
			return AS_SINDEX_ERR;
		}
		imd =  si->imd;
		op = sbin->op;
	// 		Take the read lock on imd
		for (int j=0; j<sbin->num_values; j++) {

	//		Get a value from sbin
			void * skey;
			switch (sbin->type) {
			case AS_PARTICLE_TYPE_INTEGER:
			case AS_PARTICLE_TYPE_GEOJSON:
				if (j==0) {
					skey = (void *)&(sbin->value.int_val);
				}
				else {
					skey = (void *)((uint64_t *)(sbin->values) + j);
				}
				break;
			case AS_PARTICLE_TYPE_STRING:
				if (j==0) {
					skey = (void *)&(sbin->value.str_val);
				}
				else {
					skey = (void *)((cf_digest *)(sbin->values) + j);
				}
				break;
			default:
				retval = AS_SINDEX_ERR;
				goto Cleanup;
			}
	//			Get the related pimd
			pimd = &imd->pimd[ai_btree_key_hash(imd, skey)];
			uint64_t starttime = 0;
			if (si->enable_histogram) {
				starttime = cf_getns();
			}

	//			Get the pimd write lock
			PIMD_WLOCK(&pimd->slock);

	//			If op is DELETE delete the value from sindex
			int ret = AS_SINDEX_OK;
			if (op == AS_SINDEX_OP_DELETE) {
				ret = ai_btree_delete(imd, pimd, skey, pkey);
			}
			else if (op == AS_SINDEX_OP_INSERT) {
	//			If op is INSERT put the value in sindex.
				ret = ai_btree_put(imd, pimd, skey, pkey);
			}

	//			Release the pimd lock
			PIMD_WUNLOCK(&pimd->slock);
			as_sindex__process_ret(si, ret, op, starttime, __LINE__);
		}
		cf_debug(AS_SINDEX, " Secondary Index Op Finish------------- ");

	//		Release the imd lock.
	//		Release the SI.

	}
Cleanup:
	return retval;
}
//                                       END - SBIN UTILITY
// ************************************************************************************************
// ************************************************************************************************
//                                          ADD TO SBIN


as_sindex_status
as_sindex_add_sbin_value_in_heap(as_sindex_bin * sbin, void * val)
{
	// Get the size of the data we are going to store
	// If to_free = false, this means this is the first
	// time we are storing value for this sbin to heap
	// Check if there is need to copy the existing data from stack_buf
	// 		init_storage(num_values)
	// 		If num_values != 0
	//			Copy the existing data from stack to heap
	//			reduce the used stack_buf size
	// 		to_free = true;
	// 	Else
	// 		If (num_values == heap_capacity)
	// 			extend the allocation and capacity
	// 	Copy the value to the appropriate position.

	uint32_t   size = 0;
	bool    to_copy = false;
	uint8_t    data_sz = 0;
	void * tmp_value = NULL;
	sbin_value_pool * stack_buf = sbin->stack_buf;

	// Get the size of the data we are going to store
	if (sbin->type == AS_PARTICLE_TYPE_INTEGER ||
		sbin->type == AS_PARTICLE_TYPE_GEOJSON) {
		data_sz = sizeof(uint64_t);
	}
	else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		data_sz = sizeof(cf_digest);
	}
	else {
		cf_warning(AS_SINDEX, "Bad type of data to index %d", sbin->type);
		return AS_SINDEX_ERR;
	}

	// If to_free = false, this means this is the first
	// time we are storing value for this sbin to heap
	// Check if there is need to copy the existing data from stack_buf
	if (!sbin->to_free) {
		if (sbin->num_values == 0) {
			size = 2;
		}
		else if (sbin->num_values == 1) {
			to_copy = true;
			size = 2;
			tmp_value = &sbin->value;
		}
		else if (sbin->num_values > 1) {
			to_copy = true;
			size = 2 * sbin->num_values;
			tmp_value = sbin->values;
		}
		else {
			cf_warning(AS_SINDEX, "num_values in sbin is less than 0  %"PRIu64"", sbin->num_values);
			return AS_SINDEX_ERR;
		}

		sbin->values  = cf_malloc(data_sz * size);
		sbin->to_free = true;
		sbin->heap_capacity = size;

	//			Copy the existing data from stack to heap
	//			reduce the used stack_buf size
		if (to_copy) {
			if (!memcpy(sbin->values, tmp_value, data_sz * sbin->num_values)) {
				cf_warning(AS_SINDEX, "memcpy failed");
				return AS_SINDEX_ERR;
			}
			if (sbin->num_values != 1) {
				stack_buf->used_sz -= (sbin->num_values * data_sz);
			}
		}
	}
	else
	{
	// 	Else
	// 		If (num_values == heap_capacity)
	// 			extend the allocation and capacity
		if (sbin->heap_capacity ==  sbin->num_values) {
			sbin->heap_capacity = 2 * sbin->heap_capacity;
			sbin->values = cf_realloc(sbin->values, sbin->heap_capacity * data_sz);
		}
	}

	// 	Copy the value to the appropriate position.
	if (sbin->type == AS_PARTICLE_TYPE_INTEGER ||
		sbin->type == AS_PARTICLE_TYPE_GEOJSON) {
		if (!memcpy((void *)((uint64_t *)sbin->values + sbin->num_values), (void *)val, data_sz)) {
			cf_warning(AS_SINDEX, "memcpy failed");
			return AS_SINDEX_ERR;
		}
	}
	else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		if (!memcpy((void *)((cf_digest *)sbin->values + sbin->num_values), (void *)val, data_sz)) {
			cf_warning(AS_SINDEX, "memcpy failed");
			return AS_SINDEX_ERR;
		}
	}
	else {
		cf_warning(AS_SINDEX, "Bad type of data to index %d", sbin->type);
		return AS_SINDEX_ERR;
	}

	sbin->num_values++;
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_value_to_sbin(as_sindex_bin * sbin, uint8_t * val)
{
	// If this is the first value coming to the  sbin
	// 		assign the value to the local variable of struct.
	// Else
	// 		If to_free is true or stack_buf is full
	// 			add value to the heap
	// 		else
	// 			If needed copy the values stored in sbin to stack_buf
	// 			add the value to end of stack buf

	int data_sz = 0;
	if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		data_sz = sizeof(cf_digest);
	}
	else if (sbin->type == AS_PARTICLE_TYPE_INTEGER ||
			 sbin->type == AS_PARTICLE_TYPE_GEOJSON) {
		data_sz = sizeof(uint64_t);
	}
	else {
		cf_warning(AS_SINDEX, "sbin type is invalid %d", sbin->type);
		return AS_SINDEX_ERR;
	}

	sbin_value_pool * stack_buf = sbin->stack_buf;
	if (sbin->num_values == 0 ) {
		if (sbin->type == AS_PARTICLE_TYPE_STRING) {
			sbin->value.str_val = *(cf_digest *)val;
		}
		else if (sbin->type == AS_PARTICLE_TYPE_INTEGER ||
				 sbin->type == AS_PARTICLE_TYPE_GEOJSON) {
			sbin->value.int_val = *(int64_t *)val;
		}
		sbin->num_values++;
	}
	else if (sbin->num_values == 1) {
		if ((stack_buf->used_sz + data_sz + data_sz) > AS_SINDEX_VALUESZ_ON_STACK ) {
			if (as_sindex_add_sbin_value_in_heap(sbin, (void *)val)) {
				cf_warning(AS_SINDEX, "Adding value in sbin failed.");
				return AS_SINDEX_ERR;
			}
		}
		else {
			// sbin->values gets initiated here
			sbin->values = stack_buf->value + stack_buf->used_sz;

			if (!memcpy(sbin->values, (void *)&sbin->value, data_sz)) {
				cf_warning(AS_SINDEX, "Memcpy failed");
				return AS_SINDEX_ERR;
			}
			stack_buf->used_sz += data_sz;

			if (!memcpy((void *)((uint8_t *)sbin->values + data_sz * sbin->num_values), (void *)val, data_sz)) {
				cf_warning(AS_SINDEX, "Memcpy failed");
				return AS_SINDEX_ERR;
			}
			sbin->num_values++;
			stack_buf->used_sz += data_sz;
		}
	}
	else if (sbin->num_values > 1) {
		if (sbin->to_free || (stack_buf->used_sz + data_sz ) > AS_SINDEX_VALUESZ_ON_STACK ) {
			if (as_sindex_add_sbin_value_in_heap(sbin, (void *)val)) {
				cf_warning(AS_SINDEX, "Adding value in sbin failed.");
				return AS_SINDEX_ERR;
			}
		}
		else {
			if (!memcpy((void *)((uint8_t *)sbin->values + data_sz * sbin->num_values), (void *)val, data_sz)) {
				cf_warning(AS_SINDEX, "Memcpy failed");
				return AS_SINDEX_ERR;
			}
			sbin->num_values++;
			stack_buf->used_sz += data_sz;
		}
	}
	else {
		cf_warning(AS_SINDEX, "numvalues is coming as negative. Possible memory corruption in sbin.");
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_integer_to_sbin(as_sindex_bin * sbin, uint64_t val)
{
	return as_sindex_add_value_to_sbin(sbin, (uint8_t * )&val);
}

as_sindex_status
as_sindex_add_digest_to_sbin(as_sindex_bin * sbin, cf_digest val_dig)
{
	return as_sindex_add_value_to_sbin(sbin, (uint8_t * )&val_dig);
}

as_sindex_status
as_sindex_add_string_to_sbin(as_sindex_bin * sbin, char * val)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	// Calculate digest and cal add_digest_to_sbin
	cf_digest val_dig;
	cf_digest_compute(val, strlen(val), &val_dig);
	return as_sindex_add_digest_to_sbin(sbin, val_dig);
}
//                                       END - ADD TO SBIN
// ************************************************************************************************
// ************************************************************************************************
//                                 ADD KEYTYPE FROM BASIC TYPE ASVAL
as_sindex_status
as_sindex_add_long_from_asval(as_val *val, as_sindex_bin *sbin)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (sbin->type != AS_PARTICLE_TYPE_INTEGER) {
		return AS_SINDEX_ERR;
	}

	as_integer *i = as_integer_fromval(val);
	if (!i) {
		return AS_SINDEX_ERR;
	}
	uint64_t int_val = (uint64_t)as_integer_get(i);
	return as_sindex_add_integer_to_sbin(sbin, int_val);
}

as_sindex_status
as_sindex_add_digest_from_asval(as_val *val, as_sindex_bin *sbin)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (sbin->type != AS_PARTICLE_TYPE_STRING) {
		return AS_SINDEX_ERR;
	}

	as_string *s = as_string_fromval(val);
	if (!s) {
		return AS_SINDEX_ERR;
	}
	char * str_val = as_string_get(s);
	return as_sindex_add_string_to_sbin(sbin, str_val);
}

as_sindex_status
as_sindex_add_geo2dsphere_from_as_val(as_val *val, as_sindex_bin *sbin)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (sbin->type != AS_PARTICLE_TYPE_GEOJSON) {
		return AS_SINDEX_ERR;
	}

	as_geojson *g = as_geojson_fromval(val);
	if (!g) {
		return AS_SINDEX_ERR;
	}

	const char *s = as_geojson_get(g);
	size_t jsonsz = as_geojson_len(g);
	uint64_t parsed_cellid = 0;
	geo_region_t parsed_region = NULL;

	if (! geo_parse(NULL, s, jsonsz, &parsed_cellid, &parsed_region)) {
		cf_warning(AS_PARTICLE, "geo_parse() failed - unexpected");
		geo_region_destroy(parsed_region);
		return AS_SINDEX_ERR;
	}

	if (parsed_cellid) {
		if (parsed_region) {
			geo_region_destroy(parsed_region);
			cf_warning(AS_PARTICLE, "geo_parse found both point and region");
			return AS_SINDEX_ERR;
		}

		// POINT
		if (as_sindex_add_integer_to_sbin(sbin, parsed_cellid) != AS_SINDEX_OK) {
			cf_warning(AS_PARTICLE, "as_sindex_add_integer_to_sbin() failed - unexpected");
			return AS_SINDEX_ERR;
		}
	}
	else if (parsed_region) {
		// REGION
		int numcells;
		uint64_t outcells[MAX_REGION_CELLS];

		if (! geo_region_cover(NULL, parsed_region, MAX_REGION_CELLS, outcells, NULL, NULL, &numcells)) {
			geo_region_destroy(parsed_region);
			cf_warning(AS_PARTICLE, "geo_region_cover failed");
			return AS_SINDEX_ERR;
		}

		geo_region_destroy(parsed_region);

		int added = 0;
		for (size_t i = 0; i < numcells; i++) {
			if (as_sindex_add_integer_to_sbin(sbin, outcells[i]) == AS_SINDEX_OK) {
				added++;
			}
			else {
				cf_warning(AS_PARTICLE, "as_sindex_add_integer_to_sbin() failed - unexpected");
			}
		}

		if (added == 0 && numcells > 0) {
			return AS_SINDEX_ERR;
		}
	}
	else {
		cf_warning(AS_PARTICLE, "geo_parse found neither point nor region");
		return AS_SINDEX_ERR;
	}

	return AS_SINDEX_OK;
}

typedef as_sindex_status (*as_sindex_add_keytype_from_asval_fn)
(as_val *val, as_sindex_bin * sbin);
static const as_sindex_add_keytype_from_asval_fn
			 as_sindex_add_keytype_from_asval[COL_TYPE_MAX] = {
	NULL,
	as_sindex_add_long_from_asval,
	as_sindex_add_digest_from_asval,
	as_sindex_add_geo2dsphere_from_as_val // 3
};

//                             END - ADD KEYTYPE FROM BASIC TYPE ASVAL
// ************************************************************************************************
// ************************************************************************************************
//                                    ADD ASVAL TO SINDEX TYPE
as_sindex_status
as_sindex_add_asval_to_default_sindex(as_val *val, as_sindex_bin * sbin)
{
	return as_sindex_add_keytype_from_asval[as_sindex_sktype_from_pktype(sbin->type)](val, sbin);
}

static bool as_sindex_add_listvalues_foreach(as_val * element, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_sktype_from_pktype(sbin->type)](element, sbin);
	return true;
}

as_sindex_status
as_sindex_add_asval_to_list_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_LIST
	// 		return AS_SINDEX_ERR
	// Else iterate through all values of list
	// 		If type == AS_PARTICLE_TYPE_STRING
	// 			add all string type values to the sbin
	// 		If type == AS_PARTICLE_TYPE_INTEGER
	// 			add all integer type values to the sbin

	// If val type is not AS_LIST
	// 		return AS_SINDEX_ERR
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_LIST) {
		return AS_SINDEX_ERR;
	}
	// Else iterate through all elements of map
	as_list * list               = as_list_fromval(val);
	if (as_list_foreach(list, as_sindex_add_listvalues_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

static bool as_sindex_add_mapkeys_foreach(const as_val * key, const as_val * val, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_sktype_from_pktype(sbin->type)]((as_val *)key, sbin);
	return true;
}

static bool as_sindex_add_mapvalues_foreach(const as_val * key, const as_val * val, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_sktype_from_pktype(sbin->type)]((as_val *)val, sbin);
	return true;
}

as_sindex_status
as_sindex_add_asval_to_mapkeys_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	// 		Defensive check. Should not happen.
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_MAP) {
		cf_warning(AS_SINDEX, "Unexpected wrong type %d", val->type);
		return AS_SINDEX_ERR;
	}

	// Else iterate through all keys of map
	as_map * map                   = as_map_fromval(val);
	if (as_map_foreach(map, as_sindex_add_mapkeys_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

as_sindex_status
as_sindex_add_asval_to_mapvalues_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	// Else iterate through all values of all keys of the map
	// 		If type == AS_PARTICLE_TYPE_STRING
	// 			add all string type values to the sbin
	// 		If type == AS_PARTICLE_TYPE_INTEGER
	// 			add all integer type values to the sbin

	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_MAP) {
		return AS_SINDEX_ERR;
	}
	// Else iterate through all keys, values of map
	as_map * map                  = as_map_fromval(val);
	if (as_map_foreach(map, as_sindex_add_mapvalues_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

typedef as_sindex_status (*as_sindex_add_asval_to_itype_sindex_fn)
(as_val *val, as_sindex_bin * sbin);
static const as_sindex_add_asval_to_itype_sindex_fn
			 as_sindex_add_asval_to_itype_sindex[AS_SINDEX_ITYPE_MAX] = {
	as_sindex_add_asval_to_default_sindex,
	as_sindex_add_asval_to_list_sindex,
	as_sindex_add_asval_to_mapkeys_sindex,
	as_sindex_add_asval_to_mapvalues_sindex
};
//                                   END - ADD ASVAL TO SINDEX TYPE
// ************************************************************************************************
// ************************************************************************************************
// DIFF FROM BIN TO SINDEX

static bool
as_sindex_bin_add_skey(as_sindex_bin *sbin, const void *skey, as_val_t type)
{
	if (type == AS_STRING) {
		if (as_sindex_add_digest_to_sbin(sbin, *((cf_digest *)skey)) == AS_SINDEX_OK) {
			return true;
		}
	}
	else if (type == AS_INTEGER) {
		if (as_sindex_add_integer_to_sbin(sbin, *((uint64_t *)skey)) == AS_SINDEX_OK) {
			return true;
		}
	}

	return false;
}

static void
packed_val_init_unpacker(const cdt_payload *val, as_unpacker *pk)
{
	pk->buffer = val->ptr;
	pk->length = val->sz;
	pk->offset = 0;
}

static bool
packed_val_make_skey(const cdt_payload *val, as_val_t type, void *skey)
{
	as_unpacker pk;
	packed_val_init_unpacker(val, &pk);

	as_val_t packed_type = as_unpack_peek_type(&pk);

	if (packed_type != type) {
		return false;
	}

	if (type == AS_STRING) {
		int32_t size = as_unpack_blob_size(&pk);

		if (size < 0) {
			return false;
		}

		if (pk.buffer[pk.offset++] != AS_BYTES_STRING) {
			return false;
		}

		cf_digest_compute(pk.buffer + pk.offset, pk.length - pk.offset, (cf_digest *)skey);
	}
	else if (type == AS_INTEGER) {
		if (as_unpack_int64(&pk, (int64_t *)skey) < 0) {
			return false;
		}
	}
	else {
		return false;
	}

	return true;
}

static bool
packed_val_add_sbin_or_update_shash(cdt_payload *val, as_sindex_bin *sbin, cf_shash *hash, as_val_t type)
{
	uint8_t skey[sizeof(cf_digest)];

	if (! packed_val_make_skey(val, type, skey)) {
		// packed_vals that aren't of type are ignored.
		return true;
	}

	bool found = false;

	if (cf_shash_get(hash, skey, &found) != CF_SHASH_OK) {
		// Item not in hash, add to sbin.
		return as_sindex_bin_add_skey(sbin, skey, type);
	}
	else {
		// Item is in hash, set it to true.
		found = true;
		cf_shash_put(hash, skey, &found);

		return true;
	}

	return false;
}

static void
shash_add_packed_val(cf_shash *h, const cdt_payload *val, as_val_t type, bool value)
{
	uint8_t skey[sizeof(cf_digest)];

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
			as_sindex_add_digest_to_sbin(sbin, *(const cf_digest*)skey);
		}
		else if (sbin->type == AS_PARTICLE_TYPE_INTEGER) {
			as_sindex_add_integer_to_sbin(sbin, *(const uint64_t*)skey);
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
	int data_size;
	as_val_t expected_type;

	if (type == AS_PARTICLE_TYPE_STRING) {
		data_size = 20;
		expected_type = AS_STRING;
	}
	else if (type == AS_PARTICLE_TYPE_INTEGER) {
		data_size = 8;
		expected_type = AS_INTEGER;
	}
	else {
		cf_debug(AS_SINDEX, "Invalid data type %d", type);
		return -1;
	}

	cdt_payload old_val;
	cdt_payload new_val;

	as_bin_particle_list_get_packed_val(b_old, &old_val);
	as_bin_particle_list_get_packed_val(b_new, &new_val);

	as_unpacker pk_old;
	as_unpacker pk_new;

	packed_val_init_unpacker(&old_val, &pk_old);
	packed_val_init_unpacker(&new_val, &pk_new);

	int64_t old_list_count = as_unpack_list_header_element_count(&pk_old);
	int64_t new_list_count = as_unpack_list_header_element_count(&pk_new);

	if (old_list_count < 0 || new_list_count < 0) {
		return -1;
	}

	// Skip msgpack ext if it exist as the first element.
	if (old_list_count != 0 && as_unpack_peek_is_ext(&pk_old)) {
		if (as_unpack_size(&pk_old) < 0) {
			return -1;
		}

		old_list_count--;
	}

	if (new_list_count != 0 && as_unpack_peek_is_ext(&pk_new)) {
		if (as_unpack_size(&pk_new) < 0) {
			return -1;
		}

		new_list_count--;
	}

	bool old_list_is_short = old_list_count < new_list_count;

	uint32_t short_list_count;
	uint32_t long_list_count;
	as_unpacker *pk_short;
	as_unpacker *pk_long;

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

	if (short_list_count == 0) {
		if (long_list_count == 0) {
			return 0;
		}

		as_sindex_init_sbin(sbins, old_list_is_short ? AS_SINDEX_OP_INSERT : AS_SINDEX_OP_DELETE, type, si);

		for (uint32_t i = 0; i < long_list_count; i++) {
			cdt_payload ele;

			ele.ptr = pk_long->buffer + pk_long->offset;
			ele.sz = as_unpack_size(pk_long);

			// sizeof(cf_digest) is big enough for all key types we support so far.
			uint8_t skey[sizeof(cf_digest)];

			if (! packed_val_make_skey(&ele, expected_type, skey)) {
				// packed_vals that aren't of type are ignored.
				continue;
			}

			if (! as_sindex_bin_add_skey(sbins, skey, expected_type)) {
				cf_warning(AS_SINDEX, "as_sindex_sbins_sindex_list_diff_populate() as_sindex_bin_add_skey failed");
				as_sindex_sbin_free(sbins);
				return -1;
			}
		}

		return sbins->num_values == 0 ? 0 : 1;
	}

	cf_shash *hash = cf_shash_create(cf_shash_fn_u32, data_size, 1, short_list_count, 0);

	// Add elements of shorter list into hash with value = false.
	for (uint32_t i = 0; i < short_list_count; i++) {
		cdt_payload ele = {
				.ptr = pk_short->buffer + pk_short->offset
		};

		int size = as_unpack_size(pk_short);

		if (size < 0) {
			cf_warning(AS_SINDEX, "as_sindex_sbins_sindex_list_diff_populate() list unpack failed");
			cf_shash_destroy(hash);
			return -1;
		}

		ele.sz = size;
		shash_add_packed_val(hash, &ele, expected_type, false);
	}

	as_sindex_init_sbin(sbins, old_list_is_short ? AS_SINDEX_OP_INSERT : AS_SINDEX_OP_DELETE, type, si);

	for (uint32_t i = 0; i < long_list_count; i++) {
		cdt_payload ele;

		ele.ptr = pk_long->buffer + pk_long->offset;
		ele.sz = as_unpack_size(pk_long);

		if (! packed_val_add_sbin_or_update_shash(&ele, sbins, hash, expected_type)) {
			cf_warning(AS_SINDEX, "as_sindex_sbins_sindex_list_diff_populate() hash update failed");
			as_sindex_sbin_free(sbins);
			cf_shash_destroy(hash);
			return -1;
		}
	}

	// Need to keep track of start for unwinding on error.
	as_sindex_bin *start_sbin = sbins;
	int found = 0;

	if (sbins->num_values > 0) {
		sbins++;
		found++;
	}

	as_sindex_init_sbin(sbins, old_list_is_short ? AS_SINDEX_OP_DELETE : AS_SINDEX_OP_INSERT, type, si);

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

	return found;
}

void
as_sindex_sbins_debug_print(as_sindex_bin *sbins, uint32_t count)
{
	cf_warning( AS_SINDEX, "as_sindex_sbins_list_update_diff() found=%d", count);
	for (uint32_t i = 0; i < count; i++) {
		as_sindex_bin *p = sbins + i;

		cf_warning( AS_SINDEX, "  %d: values= %"PRIu64" type=%d op=%d",
				i, p->num_values, p->type, p->op);

		if (p->type == AS_PARTICLE_TYPE_INTEGER) {
			int64_t *values = (int64_t *)p->values;

			if (p->num_values == 1) {
				cf_warning( AS_SINDEX, "    %ld", p->value.int_val);
			}
			else {
				for (uint64_t j = 0; j < p->num_values; j++) {
					cf_warning( AS_SINDEX, "     %"PRIu64":  %"PRId64"", j, values[j]);
				}
			}
		}
	}
}

// Assumes b_old and b_new are AS_PARTICLE_TYPE_LIST bins.
// Assumes b_old and b_new have the same id.
static int32_t
as_sindex_sbins_list_diff_populate(as_sindex_bin *sbins, as_namespace *ns, const char *set_name, const as_bin *b_old, const as_bin *b_new)
{
	uint16_t id = b_new->id;

	if (! as_sindex_binid_has_sindex(ns, id)) {
		return 0;
	}

	cf_ll *simatch_ll = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set_name, id, &simatch_ll);

	if (! simatch_ll) {
		return 0;
	}

	uint32_t populated = 0;

	for (cf_ll_element *ele = cf_ll_get_head(simatch_ll); ele; ele = ele->next) {
		sindex_set_binid_hash_ele *si_ele = (sindex_set_binid_hash_ele *)ele;
		int simatch = si_ele->simatch;
		as_sindex *si = &ns->sindex[simatch];

		if (! as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}

		int32_t delta = as_sindex_sbins_sindex_list_diff_populate(&sbins[populated], si, b_old, b_new);

		if (delta < 0) {
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
int
as_sindex_sbin_from_sindex(as_sindex * si, const as_bin *b, as_sindex_bin * sbin, as_val ** cdt_asval)
{
	as_sindex_metadata * imd    = si->imd;
	as_particle_type imd_sktype  = as_sindex_pktype(imd);
	as_val * cdt_val            = * cdt_asval;
	uint32_t  valsz             = 0;
	int sindex_found            = 0;
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

				if (as_sindex_add_integer_to_sbin(sbin, (uint64_t)sbin->value.int_val) == AS_SINDEX_OK) {
					if (sbin->num_values) {
						sindex_found++;
					}
				}
			}
			else if (bin_type == AS_PARTICLE_TYPE_STRING) {
				found = true;
				char* bin_val;
				valsz = as_bin_particle_string_ptr(b, &bin_val);

				if (valsz > AS_SINDEX_MAX_STRING_KSIZE) {
					cf_warning( AS_SINDEX, "sindex key size out of bounds %d ", valsz);
					cf_warning(AS_SINDEX, "Sindex on bin %s fails. Value length %u too long.", imd->bname, valsz);
				}
				else {
					cf_digest buf_dig;
					cf_digest_compute(bin_val, valsz, &buf_dig);

					if (as_sindex_add_digest_to_sbin(sbin, buf_dig) == AS_SINDEX_OK) {
						if (sbin->num_values) {
							sindex_found++;
						}
					}
				}
			}
			else if (bin_type == AS_PARTICLE_TYPE_GEOJSON) {
				// GeoJSON is like AS_PARTICLE_TYPE_STRING when
				// reading the value and AS_PARTICLE_TYPE_INTEGER for
				// adding the result to the index.
				found = true;
				bool added = false;
				uint64_t * cells;
				size_t ncells = as_bin_particle_geojson_cellids(b, &cells);
				for (size_t ndx = 0; ndx < ncells; ++ndx) {
					if (as_sindex_add_integer_to_sbin(sbin, cells[ndx]) == AS_SINDEX_OK) {
						added = true;
					}
				}
				if (added && sbin->num_values) {
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
			if (as_sindex_add_asval_to_itype_sindex[imd->itype](res_val, sbin) == AS_SINDEX_OK) {
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
int
as_sindex_sbins_from_bin_buf(as_namespace *ns, const char *set, const as_bin *b, as_sindex_bin * start_sbin,
					as_sindex_op op)
{
	// Check the sindex bit array.
	// If there is not sindex present on this bin return 0
	// Get the simatch_ll from set_binid_hash
	// If simatch_ll is NULL return 0
	// Iterate through simatch_ll
	// 		If path_length == 0
	// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
	// 				Add the value to the sbin.
	//			If itype == AS_SINDEX_ITYPE_MAP or AS_SINDEX_ITYPE_INVMAP and type = MAP
	//	 			Deserialize the bin if have not deserialized it yet.
	//				Extract as_val from path within the bin
	//				Add them to the sbin.
	// 			If itype == AS_SINDEX_ITYPE_LIST and type = LIST
	//	 			Deserialize the bin if have not deserialized it yet.
	//				Extract as_val from path within the bin.
	//				Add the values to the sbin.
	// 		Else if path_length > 0 and type == MAP or LIST
	// 			Deserialize the bin if have not deserialized it yet.
	//			Extract as_val from path within the bin.
	//			Add the values to the sbin.
	// Return the number of sbins found.

	int sindex_found = 0;
	if (!b) {
		cf_warning(AS_SINDEX, "Null Bin Passed, No sbin created");
		return sindex_found;
	}
	if (!ns) {
		cf_warning(AS_SINDEX, "NULL Namespace Passed");
		return sindex_found;
	}
	if (!as_bin_inuse(b)) {
		return sindex_found;
	}

	// Check the sindex bit array.
	// If there is not sindex present on this bin return 0
	if (!as_sindex_binid_has_sindex(ns, b->id) ) {
		return sindex_found;
	}

	// Get the simatch_ll from set_binid_hash
	cf_ll * simatch_ll  = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set, b->id, &simatch_ll);

	// If simatch_ll is NULL return 0
	if (!simatch_ll) {
		return sindex_found;
	}

	// Iterate through simatch_ll
	cf_ll_element             * ele    = cf_ll_get_head(simatch_ll);
	sindex_set_binid_hash_ele * si_ele = NULL;
	int                        simatch = -1;
	as_sindex                 * si     = NULL;
	as_val                   * cdt_val = NULL;
	int                   sbins_in_si  = 0;
	while (ele) {
		si_ele                = (sindex_set_binid_hash_ele *) ele;
		simatch               = si_ele->simatch;
		si                    = &ns->sindex[simatch];
		if (!as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}
		as_sindex_init_sbin(&start_sbin[sindex_found], op,  as_sindex_pktype(si->imd), si);
		uint64_t s_time = cf_getns();
		sbins_in_si          = as_sindex_sbin_from_sindex(si, b, &start_sbin[sindex_found], &cdt_val);
		if (sbins_in_si == 1) {
			sindex_found += sbins_in_si;
			// sbin free will happen once sbin is updated in sindex tree
			SINDEX_HIST_INSERT_DATA_POINT(si, si_prep_hist, s_time);
		}
		else {
			as_sindex_sbin_free(&start_sbin[sindex_found]);
			if (sbins_in_si) {
				cf_warning(AS_SINDEX, "sbins found in si is neither 1 nor 0. It is %d", sbins_in_si);
			}
		}
		ele                   = ele->next;
	}

	// FREE as_val
	if (cdt_val) {
		as_val_destroy(cdt_val);
	}
	// Return the number of sbin found.
	return sindex_found;
}

int
as_sindex_sbins_from_bin(as_namespace *ns, const char *set, const as_bin *b, as_sindex_bin * start_sbin, as_sindex_op op)
{
	return as_sindex_sbins_from_bin_buf(ns, set, b, start_sbin, op);
}

/*
 * returns number of sbins found.
 */
int
as_sindex_sbins_from_rd(as_storage_rd *rd, uint16_t from_bin, uint16_t to_bin, as_sindex_bin sbins[], as_sindex_op op)
{
	uint16_t count  = 0;
	for (uint16_t i = from_bin; i < to_bin; i++) {
		as_bin *b   = &rd->bins[i];
		count      += as_sindex_sbins_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), b, &sbins[count], op);
	}
	return count;
}

// Needs comments
int
as_sindex_update_by_sbin(as_namespace *ns, const char *set, as_sindex_bin *start_sbin, int num_sbins, cf_digest * pkey)
{
	cf_debug(AS_SINDEX, "as_sindex_update_by_sbin");

	// Need to address sbins which have OP as AS_SINDEX_OP_DELETE before the ones which have
	// OP as AS_SINDEX_OP_INSERT. This is because same secondary index key can exist in sbins
	// with different OPs
	int sindex_ret = AS_SINDEX_OK;
	for (int i=0; i<num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_DELETE) {
			sindex_ret = as_sindex__op_by_sbin(ns, set, 1, &start_sbin[i], pkey);
		}
	}
	for (int i=0; i<num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_INSERT) {
			sindex_ret = as_sindex__op_by_sbin(ns, set, 1, &start_sbin[i], pkey);
		}
	}
	return sindex_ret;
}
//                                 END - SBIN INTERFACE FUNCTIONS
// ************************************************************************************************
// ************************************************************************************************
//                                      PUT RD IN SINDEX
// Takes a record and tries to populate it in every sindex present in the namespace.
void
as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd)
{
	int count = 0;
	int valid = 0;

	// Only called at the boot time. No writer is expected to
	// change ns->sindex in parallel.
	while (count < AS_SINDEX_MAX && valid < ns->sindex_cnt) {
		as_sindex *si = &ns->sindex[count];
		if (! as_sindex_put_rd(si, rd)) {
			valid++;
		}
		count++;
	}
}

as_sindex_status
as_sindex_put_rd(as_sindex *si, as_storage_rd *rd)
{
	// Proceed only if sindex is active
	SINDEX_GRLOCK();
	if (! as_sindex_isactive(si)) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_ERR;
	}

	as_sindex_metadata *imd = si->imd;
	// Validate Set name. Other function do this check while
	// performing searching for simatch.
	const char *setname = as_index_get_set_name(rd->r, si->ns);

	if (!as_sindex__setname_match(imd, setname)) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_OK;
	}

	// collect sbins
	SINDEX_BINS_SETUP(sbins, 1);

	int sbins_populated = 0;
	as_val * cdt_val = NULL;

	as_bin *b = as_bin_get(rd, imd->bname);

	if (!b) {
		SINDEX_GRUNLOCK();
		return AS_SINDEX_OK;
	}

	as_sindex_init_sbin(&sbins[sbins_populated], AS_SINDEX_OP_INSERT,
												as_sindex_pktype(si->imd), si);
	sbins_populated = as_sindex_sbin_from_sindex(si, b, &sbins[sbins_populated], &cdt_val);

	// Only 1 sbin should be populated here.
	// If populated should be freed after sindex update
	if (sbins_populated != 1) {
		as_sindex_sbin_free(&sbins[sbins_populated]);
		if (sbins_populated) {
			cf_warning(AS_SINDEX, "Number of sbins found for 1 sindex is neither 1 nor 0. It is %d",
					sbins_populated);
		}
	}
	SINDEX_GRUNLOCK();

	if (cdt_val) {
		as_val_destroy(cdt_val);
	}

	if (sbins_populated) {
		as_sindex_update_by_sbin(rd->ns, setname, sbins, sbins_populated, &rd->r->keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);
	}

	return AS_SINDEX_OK;
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

void
as_sindex_init_smd()
{
	as_smd_module_load(AS_SMD_MODULE_SINDEX, as_sindex_smd_accept_cb, NULL,
			NULL);
}

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

as_sindex_ktype
as_sindex_ktype_from_smd_char(char c)
{
	if (c == 'I') {
		return COL_TYPE_LONG;
	}
	else if (c == 'S') {
		return COL_TYPE_DIGEST;
	}
	else if (c == 'G') {
		return COL_TYPE_GEOJSON;
	}
	else {
		cf_warning(AS_SINDEX, "unknown smd ktype %c", c);
		return COL_TYPE_INVALID;
	}
}

char
as_sindex_ktype_to_smd_char(as_sindex_ktype ktype)
{
	if (ktype == COL_TYPE_LONG) {
		return 'I';
	}
	else if (ktype == COL_TYPE_DIGEST) {
		return 'S';
	}
	else if (ktype == COL_TYPE_GEOJSON) {
		return 'G';
	}
	else {
		cf_crash(AS_SINDEX, "unknown ktype %d", ktype);
		return '?';
	}
}

as_sindex_type
as_sindex_type_from_smd_char(char c)
{
	if (c == '.') {
		return AS_SINDEX_ITYPE_DEFAULT; // or - "scalar"
	}
	else if (c == 'L') {
		return AS_SINDEX_ITYPE_LIST;
	}
	else if (c == 'K') {
		return AS_SINDEX_ITYPE_MAPKEYS;
	}
	else if (c == 'V') {
		return AS_SINDEX_ITYPE_MAPVALUES;
	}
	else {
		cf_warning(AS_SINDEX, "unknown smd type %c", c);
		return AS_SINDEX_ITYPE_MAX; // since there's no named illegal value
	}
}

char
as_sindex_type_to_smd_char(as_sindex_type itype)
{
	if (itype == AS_SINDEX_ITYPE_DEFAULT) {
		return '.';
	}
	else if (itype == AS_SINDEX_ITYPE_LIST) {
		return 'L';
	}
	else if (itype == AS_SINDEX_ITYPE_MAPKEYS) {
		return 'K';
	}
	else if (itype == AS_SINDEX_ITYPE_MAPVALUES) {
		return 'V';
	}
	else {
		cf_crash(AS_SINDEX, "unknown type %d", itype);
		return '?';
	}
}

#define TOK_CHAR_DELIMITER '|'

bool
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

	uint32_t ns_name_len = tok - read;

	imd->ns_name = cf_malloc(ns_name_len + 1);
	memcpy(imd->ns_name, read, ns_name_len);
	imd->ns_name[ns_name_len] = 0;

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - set name missing delimiter");
		return false;
	}

	uint32_t set_name_len = tok - read;

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

	uint32_t path_len = tok - read;

	imd->path_str = cf_malloc(path_len + 1);
	memcpy(imd->path_str, read, path_len);
	imd->path_str[path_len] = 0;

	if (as_sindex_extract_bin_path(imd, imd->path_str) != AS_SINDEX_OK) {
		cf_warning(AS_SINDEX, "smd - can't parse path");
		return false;
	}

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - itype missing delimiter");
		return false;
	}

	if ((imd->itype = as_sindex_type_from_smd_char(*read)) ==
			AS_SINDEX_ITYPE_MAX) {
		cf_warning(AS_SINDEX, "smd - bad itype");
		return false;
	}

	read = tok + 1;

	if ((imd->sktype = as_sindex_ktype_from_smd_char(*read)) ==
			COL_TYPE_INVALID) {
		cf_warning(AS_SINDEX, "smd - bad sktype");
		return false;
	}

	return true;
}

void
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
			as_sindex_type_to_smd_char(imd->itype),
			as_sindex_ktype_to_smd_char(imd->sktype));
}

bool
as_sindex_delete_imd_to_smd_key(as_namespace *ns, as_sindex_metadata *imd, char *smd_key)
{
	// ns-name|<set-name>|path|sktype|<itype>
	// Note - sktype a.k.a. ktype and dtype.

	// The imd passed in doesn't have enough to make SMD key - use a full imd
	// from the existing sindex, if it's there.

	// TODO - takes lock - is this ok? Flags ok?
	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname,
			AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (! si) {
		return false;
	}

	as_sindex_imd_to_smd_key(si->imd, smd_key);

	return true;
}

void
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

		if (item->value != NULL) {
			smd_value_to_imd(item->value, &imd); // sets index name
			as_sindex_smd_create(ns, &imd);
		}
		else {
			as_sindex_destroy(ns, &imd);
		}

		as_sindex_imd_free(&imd);
	}
}
//                                     END - SMD CALLBACKS
// ************************************************************************************************
// ************************************************************************************************
//                                         SINDEX TICKER
// Sindex ticker start
void
as_sindex_ticker_start(as_namespace * ns, as_sindex * si)
{
	cf_info(AS_SINDEX, "Sindex-ticker start: ns=%s si=%s job=%s", ns->name ? ns->name : "<all>",
			si ? si->imd->iname : "<all>", si ? "SINDEX_POPULATE" : "SINDEX_POPULATEALL");

}
// Sindex ticker
void
as_sindex_ticker(as_namespace * ns, as_sindex * si, uint64_t n_obj_scanned, uint64_t start_time)
{
	const uint64_t sindex_ticker_obj_count = 500000;

	if (n_obj_scanned % sindex_ticker_obj_count == 0 && n_obj_scanned != 0) {
		// Ticker can be dumped from here, we'll be in this place for both
		// sindex populate and populate-all.
		// si memory gets set from as_sindex_reserve_data_memory() which in turn gets set from :
		// ai_btree_put() <- for every single sindex insertion (boot-time/dynamic)
		// as_sindex_create() : for dynamic si creation, cluster change, smd on boot-up.

		uint64_t si_memory = 0;
		char   * si_name = NULL;

		if (si) {
			si_memory += ai_btree_get_isize(si->imd);
			si_memory += ai_btree_get_nsize(si->imd);
			si_name = si->imd->iname;
		}
		else {
			si_memory = (uint64_t)cf_atomic64_get(ns->n_bytes_sindex_memory);
			si_name = "<all>";
		}

		uint64_t n_objects       = cf_atomic64_get(ns->n_objects);
		uint64_t pct_obj_scanned = n_objects == 0 ? 100 : ((n_obj_scanned * 100) / n_objects);
		uint64_t elapsed         = (cf_getms() - start_time);
		uint64_t est_time        = (elapsed * n_objects)/n_obj_scanned - elapsed;

		cf_info(AS_SINDEX, " Sindex-ticker: ns=%s si=%s obj-scanned=%"PRIu64" si-mem-used=%"PRIu64""
				" progress= %"PRIu64"%% est-time=%"PRIu64" ms",
				ns->name, si_name, n_obj_scanned, si_memory, pct_obj_scanned, est_time);
	}
}

// Sindex ticker end
void
as_sindex_ticker_done(as_namespace * ns, as_sindex * si, uint64_t start_time)
{
	uint64_t si_memory   = 0;
	char   * si_name     = NULL;

	if (si) {
		si_memory += ai_btree_get_isize(si->imd);
		si_memory += ai_btree_get_nsize(si->imd);
		si_name = si->imd->iname;
	}
	else {
		si_memory = (uint64_t)cf_atomic64_get(ns->n_bytes_sindex_memory);
		si_name = "<all>";
	}

	cf_info(AS_SINDEX, "Sindex-ticker done: ns=%s si=%s si-mem-used=%"PRIu64" elapsed=%"PRIu64" ms",
				ns->name, si_name, si_memory, cf_getms() - start_time);

}
//                                       END - SINDEX TICKER
// ************************************************************************************************
// ************************************************************************************************
//                                         INDEX KEYS ARR
// Functions are not used in this file.
static cf_queue *g_q_index_keys_arr = NULL;
int
as_index_keys_ll_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
as_index_keys_ll_destroy_fn(cf_ll_element *ele)
{
	as_index_keys_ll_element * node = (as_index_keys_ll_element *) ele;
	if (node) {
		if (node->keys_arr) {
			as_index_keys_release_arr_to_queue(node->keys_arr);
			node->keys_arr = NULL;
		}
		cf_free(node);
	}
}

as_index_keys_arr *
as_index_get_keys_arr(void)
{
	as_index_keys_arr *keys_arr;
	if (cf_queue_pop(g_q_index_keys_arr, &keys_arr, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		keys_arr = cf_malloc(sizeof(as_index_keys_arr));
	}
	keys_arr->num = 0;
	return keys_arr;
}

void
as_index_keys_release_arr_to_queue(as_index_keys_arr *v)
{
	as_index_keys_arr * keys_arr = (as_index_keys_arr *)v;
	if (cf_queue_sz(g_q_index_keys_arr) < AS_INDEX_KEYS_ARRAY_QUEUE_HIGHWATER) {
		cf_queue_push(g_q_index_keys_arr, &keys_arr);
	}
	else {
		cf_free(keys_arr);
	}

}
//                                      END - INDEX KEYS ARR
// ************************************************************************************************

/*
 * Main initialization function. Talks to Aerospike Index to pull up all the indexes
 * and populates sindex hanging from namespace
 */
int
as_sindex_init(as_namespace *ns)
{
	ns->sindex = cf_malloc(sizeof(as_sindex) * AS_SINDEX_MAX);

	ns->sindex_cnt = 0;
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si                    = &ns->sindex[i];
		memset(si, 0, sizeof(as_sindex));
		si->state                        = AS_SINDEX_INACTIVE;
		si->stats._delete_hist           = NULL;
		si->stats._query_hist            = NULL;
		si->stats._query_batch_lookup    = NULL;
		si->stats._query_batch_io        = NULL;
		si->stats._query_rcnt_hist       = NULL;
		si->stats._query_diff_hist       = NULL;
	}

	// binid to simatch lookup
	ns->sindex_set_binid_hash = cf_shash_create(cf_shash_fn_zstr,
			AS_SINDEX_PROP_KEY_SIZE, sizeof(cf_ll *), AS_SINDEX_MAX, 0);

	// iname to simatch lookup
	ns->sindex_iname_hash = cf_shash_create(cf_shash_fn_zstr, AS_ID_INAME_SZ,
			sizeof(uint32_t), AS_SINDEX_MAX, 0);

	// Init binid_has_sindex to zero
	memset(ns->binid_has_sindex, 0, sizeof(uint32_t)*AS_BINID_HAS_SINDEX_SIZE);
	if (!g_q_index_keys_arr) {
		g_q_index_keys_arr = cf_queue_create(sizeof(void *), true);
	}
	return AS_SINDEX_OK;
}

void
as_sindex_dump(char *nsname, char *iname, char *fname, bool verbose)
{
	as_namespace *ns = as_namespace_get_byname(nsname);
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	ai_btree_dump(si->imd, fname, verbose);
	AS_SINDEX_RELEASE(si);
}
