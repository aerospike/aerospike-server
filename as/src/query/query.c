/*
 * query.c
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
 * along with this program. If not, see http://www.gnu.org/licenses/
 */

//==============================================================================
// Includes.
//

#include "query/query.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "aerospike/as_list.h"
#include "aerospike/as_module.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "hist.h"
#include "log.h"
#include "msgpack_in.h"
#include "socket.h"
#include "vector.h"

#include "base/aggr.h"
#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/expop.h"
#include "base/index.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/service.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "query/query_job.h"
#include "query/query_manager.h"
#include "sindex/sindex.h"
#include "sindex/sindex_tree.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"
#include "transaction/write.h"

//#include "warnings.h"


//==============================================================================
// Typedefs & constants.
//

typedef enum {
	QUERY_TYPE_BASIC	= 0,
	QUERY_TYPE_AGGR		= 1,
	QUERY_TYPE_UDF_BG	= 2,
	QUERY_TYPE_OPS_BG	= 3,

	QUERY_TYPE_UNKNOWN	= -1
} query_type;

#define RECORD_CHANGED_SAFETY_MS 3000 // clock skew & bins change after LUT

#define INIT_BUF_BUILDER_SIZE (1024U * 1024U * 7U / 4U) // 1.75M
#define QUERY_CHUNK_LIMIT (1024U * 1024U)

#define MAX_ACTIVE_TRANSACTIONS 200

#define DEFAULT_TTL_NS 1000000000 // 1 second


//==========================================================
// Forward declarations.
//

//----------------------------------------------------------
// query_job - derived classes' public methods.
//

static int basic_query_job_start(as_transaction* tr, as_namespace* ns);
static int aggr_query_job_start(as_transaction* tr, as_namespace* ns);
static int udf_bg_query_job_start(as_transaction* tr, as_namespace* ns);
static int ops_bg_query_job_start(as_transaction* tr, as_namespace* ns);

//----------------------------------------------------------
// Non-class-specific utilities.
//

static query_type get_query_type(const as_transaction* tr);

static bool get_query_set(const as_transaction* tr, as_namespace* ns, char* set_name, uint16_t* set_id);
static bool get_query_pids(const as_transaction* tr, as_query_pid** p_pids, uint16_t* n_pids_requested, uint16_t* n_keyds_requested);
static bool get_query_range(const as_transaction* tr, as_namespace* ns, as_query_range** range_r);
static bool get_query_rps(const as_transaction* tr, uint32_t* rps);

static bool get_query_socket_timeout(const as_transaction* tr, int32_t* timeout);

static bool get_query_sample_max(const as_transaction* tr, uint64_t* sample_max);
static bool get_query_filter_exp(const as_transaction* tr, as_exp** exp);

static bool range_from_msg_integer(const uint8_t* data, as_query_range* range, uint32_t len);
static bool range_from_msg_string(const uint8_t* data, as_query_range* range, uint32_t len);
static bool range_from_msg_blob(const uint8_t* data, as_query_range* range, uint32_t len);
static bool range_from_msg_geojson(as_namespace* ns, const uint8_t* data, as_query_range* range, uint32_t len);
static void sort_geo_range(as_query_geo_range* geo);

static bool find_sindex(as_query_job* _job);
static bool validate_background_query_rps(const as_namespace* ns, uint32_t* rps);

static size_t send_blocking_response_chunk(as_file_handle* fd_h, uint8_t* buf, size_t size, int32_t timeout, bool compress, as_proto_comp_stat* comp_stat);

static bool record_matches_query(as_query_job* _job, as_storage_rd* rd);
static bool record_matches_query_cdt(as_query_job* _job, const as_bin* b);
static bool match_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata);
static bool match_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata);
static bool match_listeles_foreach(msgpack_in* element, void* udata);
static bool match_integer_element(const as_query_job* _job, msgpack_in* element);
static bool match_string_element(const as_query_job* _job, msgpack_in* element);
static bool match_blob_element(const as_query_job* _job, msgpack_in* element);
static bool match_geojson_element(const as_query_job* _job, msgpack_in* element);


//==========================================================
// Inlines & macros.
//

static inline bool
record_changed_since_start(const as_query_job* _job, const as_record *r)
{
	return r->last_update_time + RECORD_CHANGED_SAFETY_MS >
			_job->start_ms_clepoch;
}

static inline bool
strings_match(const as_query_range* range, const char* str, size_t len)
{
	return range->blob_sz == len &&
			range->u.r.start == as_sindex_string_to_bval(str, len) &&
			memcmp(range->stub, str, len < sizeof(range->stub) ?
					len : sizeof(range->stub)) == 0;
}

static inline bool
blobs_match(const as_query_range* range, const uint8_t* blob, size_t sz)
{
	return range->blob_sz == sz &&
			range->u.r.start == as_sindex_blob_to_bval(blob, sz) &&
			memcmp(range->stub, blob, sz < sizeof(range->stub) ?
					sz : sizeof(range->stub)) == 0;
}

static inline const char*
query_type_str(query_type type)
{
	switch (type) {
	case QUERY_TYPE_BASIC:
		return "basic";
	case QUERY_TYPE_AGGR:
		return "aggregation";
	case QUERY_TYPE_UDF_BG:
		return "background-udf";
	case QUERY_TYPE_OPS_BG:
		return "background-ops";
	default:
		return "?";
	}
}

static inline bool
excluded_set(as_index* r, uint16_t set_id)
{
	// Note - INVALID_SET_ID at this point must mean scan whole namespace.
	return set_id != INVALID_SET_ID && set_id != as_index_get_set_id(r);
}

static inline void
throttle_sleep(as_query_job* _job)
{
	uint32_t sleep_us = as_query_job_throttle(_job);

	if (sleep_us != 0) {
		usleep(sleep_us);
	}
}


//==============================================================================
// Public API.
//

int
as_query(as_transaction* tr, as_namespace* ns)
{
	switch (get_query_type(tr)) {
	case QUERY_TYPE_BASIC:
		return basic_query_job_start(tr, ns);
	case QUERY_TYPE_AGGR:
		return aggr_query_job_start(tr, ns);
	case QUERY_TYPE_UDF_BG:
		return udf_bg_query_job_start(tr, ns);
	case QUERY_TYPE_OPS_BG:
		return ops_bg_query_job_start(tr, ns);
	default:
		return AS_ERR_PARAMETER;
	}
}


//==============================================================================
// Non-class-specific utilities.
//

static query_type
get_query_type(const as_transaction* tr)
{
	if (! as_transaction_is_udf(tr)) {
		return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) != 0 ?
				QUERY_TYPE_OPS_BG : QUERY_TYPE_BASIC;
	}
	// else - UDF query.

	const as_msg_field* udf_op_f =
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP);

	if (udf_op_f == NULL) {
		cf_warning(AS_QUERY, "missing udf-op");
		return QUERY_TYPE_UNKNOWN;
	}

	if (as_msg_field_get_value_sz(udf_op_f) != sizeof(uint8_t)) {
		cf_warning(AS_QUERY, "udf-op field size not %zu", sizeof(uint8_t));
		return QUERY_TYPE_UNKNOWN;
	}

	switch ((as_udf_op)*udf_op_f->data) {
	case AS_UDF_OP_AGGREGATE:
		return QUERY_TYPE_AGGR;
	case AS_UDF_OP_BACKGROUND:
		return QUERY_TYPE_UDF_BG;
	default:
		cf_warning(AS_QUERY, "unknown udf-op %u", *udf_op_f->data);
		return QUERY_TYPE_UNKNOWN;
	}
}

static bool
get_query_set(const as_transaction* tr, as_namespace* ns, char* set_name,
		uint16_t* set_id)
{
	if (! as_transaction_has_set(tr)) {
		set_name[0] = '\0';
		*set_id = INVALID_SET_ID;
		return true; // will query whole namespace
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_SET);
	uint32_t len = as_msg_field_get_value_sz(f);

	if (len == 0) {
		set_name[0] = '\0';
		*set_id = INVALID_SET_ID;
		return true; // as if no set name sent - will query whole namespace
	}

	if (len >= AS_SET_NAME_MAX_SIZE) {
		cf_warning(AS_QUERY, "query msg set name too long %u", len);
		return false;
	}

	memcpy(set_name, f->data, len);
	set_name[len] = '\0';

	if ((*set_id = as_namespace_get_set_id(ns, set_name)) == INVALID_SET_ID) {
		cf_warning(AS_QUERY, "query msg from %s has unrecognized set %s",
				tr->from.proto_fd_h->client, set_name);
		// Continue anyway - need to send per-partition results.
	}

	return true;
}

static bool
get_query_pids(const as_transaction* tr, as_query_pid** p_pids,
		uint16_t* n_pids_requested, uint16_t* n_keyds_requested)
{
	if (! as_transaction_has_pids(tr) && ! as_transaction_has_digests(tr)) {
		return true;
	}

	as_query_pid* pids = cf_calloc(AS_PARTITIONS, sizeof(as_query_pid));
	const as_msg* m = &tr->msgp->msg;

	if (as_transaction_has_pids(tr)) {
		const as_msg_field* f = as_msg_field_get(m,
				AS_MSG_FIELD_TYPE_PID_ARRAY);

		uint32_t n_pids = as_msg_field_get_value_sz(f) / sizeof(uint16_t);

		if (n_pids == 0 || n_pids > AS_PARTITIONS) {
			cf_warning(AS_QUERY, "pid array empty or too big");
			cf_free(pids);
			return false;
		}

		const uint16_t* data = (uint16_t*)f->data;

		for (uint32_t i = 0; i < n_pids; i++) {
			uint16_t pid = cf_swap_from_le16(data[i]);

			if (pid >= AS_PARTITIONS || pids[pid].requested) {
				cf_warning(AS_QUERY, "bad or duplicate pid %hu", pid);
				cf_free(pids);
				return false;
			}

			pids[pid].requested = true;
			(*n_pids_requested)++;
		}
	}

	bool has_where_clause = as_transaction_has_where_clause(tr);

	if (as_transaction_has_digests(tr)) {
		const as_msg_field* digest_f = as_msg_field_get(m,
				AS_MSG_FIELD_TYPE_DIGEST_ARRAY);

		uint32_t n_digests = as_msg_field_get_value_sz(digest_f) /
				sizeof(cf_digest);

		if (n_digests == 0 || n_digests > AS_PARTITIONS) {
			cf_warning(AS_QUERY, "digest array empty or too big");
			cf_free(pids);
			return false;
		}

		const uint64_t* bval_data = NULL;

		if (has_where_clause) {
			if (! as_transaction_has_bval_array(tr)) {
				cf_warning(AS_QUERY, "got digests but no bval array");
				cf_free(pids);
				return false;
			}

			const as_msg_field* bval_f =
					as_msg_field_get(m, AS_MSG_FIELD_TYPE_BVAL_ARRAY);
			uint32_t n_bvals =
					as_msg_field_get_value_sz(bval_f) / sizeof(uint64_t);

			if (n_bvals != n_digests) {
				cf_warning(AS_QUERY, "n_bvals %u != n_digests %u", n_bvals,
						n_digests);
				cf_free(pids);
				return false;
			}

			bval_data = (uint64_t*)bval_f->data;
		}

		const cf_digest* digest_data = (cf_digest*)digest_f->data;

		for (uint32_t i = 0; i < n_digests; i++) {
			const cf_digest* keyd = &digest_data[i];
			uint32_t pid = as_partition_getid(keyd);

			if (pid >= AS_PARTITIONS || pids[pid].requested) {
				cf_warning(AS_QUERY, "bad or duplicate digest pid %u", pid);
				cf_free(pids);
				return false;
			}

			pids[pid] = (as_query_pid){
					.requested = true,
					.has_resume = true,
					.keyd = *keyd,
					.bval = bval_data == NULL ?
							0 : (int64_t)cf_swap_from_le64(bval_data[i])
			};

			(*n_pids_requested)++;
			(*n_keyds_requested)++;
		}
	}
	else if (as_transaction_has_bval_array(tr)) {
		cf_warning(AS_QUERY, "got bval array but no digests");
		cf_free(pids);
		return false;
	}

	*p_pids = pids;

	return true;
}

static bool
get_query_range(const as_transaction* tr, as_namespace* ns,
		as_query_range** range_r)
{
	if (! as_transaction_has_where_clause(tr)) {
		return true;
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_INDEX_RANGE);
	const uint8_t* data = f->data;
	uint32_t len = as_msg_field_get_value_sz(f);

	if (len == 0) {
		cf_warning(AS_QUERY, "cannot parse index range");
		return false;
	}

	uint8_t n_ranges = *data++;
	len--;

	if (n_ranges != 1) {
		cf_warning(AS_QUERY, "%u ranges - only 1 supported", n_ranges);
		return false;
	}

	if (len == 0) {
		cf_warning(AS_QUERY, "cannot parse bin name");
		return false;
	}

	uint8_t bin_name_len = *data++;
	len--;

	if (bin_name_len == 0 || bin_name_len >= AS_BIN_NAME_MAX_SZ) {
		cf_warning(AS_QUERY, "invalid bin name length %u", bin_name_len);
		return false;
	}

	if (len < bin_name_len) {
		cf_warning(AS_QUERY, "cannot parse bin name");
		return false;
	}

	as_query_range* range = cf_calloc(1, sizeof(as_query_range));
	*range_r = range; // link it to the job so that as_job_destroy will clean it

	memcpy(range->bin_name, data, bin_name_len); // null-terminated via calloc

	data += bin_name_len;
	len -= bin_name_len;

	if (len == 0) {
		cf_warning(AS_QUERY, "cannot parse particle type");
		return false;
	}

	range->bin_type = *data++;
	len--;

	bool success;

	switch (range->bin_type) {
	case AS_PARTICLE_TYPE_INTEGER:
		success = range_from_msg_integer(data, range, len);
		break;
	case AS_PARTICLE_TYPE_STRING:
		success = range_from_msg_string(data, range, len);
		break;
	case AS_PARTICLE_TYPE_BLOB:
		success = range_from_msg_blob(data, range, len);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		success = range_from_msg_geojson(ns, data, range, len);
		break;
	default:
		cf_warning(AS_QUERY, "invalid particle type %u", range->bin_type);
		success = false;
	}

	if (! success) {
		return false;
	}

	f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_INDEX_TYPE);

	if (f != NULL && as_msg_field_get_value_sz(f) == 0) {
		cf_warning(AS_QUERY, "cannot parse itype");
		return false;
	}

	range->itype = f == NULL ? AS_SINDEX_ITYPE_DEFAULT : *f->data;

	range->de_dup = range->isrange && range->itype != AS_SINDEX_ITYPE_DEFAULT;

	f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_INDEX_CONTEXT);

	if (f != NULL) {
		if (as_msg_field_get_value_sz(f) == 0) {
			cf_warning(AS_QUERY, "cannot parse index context");
			return false;
		}

		range->ctx_buf_sz = as_msg_field_get_value_sz(f);

		if (range->ctx_buf_sz == 0) {
			cf_warning(AS_QUERY, "invalid index context");
			return false;
		}

		range->ctx_buf = cf_malloc(range->ctx_buf_sz);
		memcpy(range->ctx_buf, f->data, range->ctx_buf_sz);

		bool was_modified;

		range->ctx_buf_sz = msgpack_compactify(range->ctx_buf,
				range->ctx_buf_sz, &was_modified);

		if (range->ctx_buf_sz == 0) {
			cf_warning(AS_QUERY, "invalid index context - could not compactify");
			return false;
		}

		if (was_modified) {
			cf_warning(AS_QUERY, "invalid index context - msgpack not normalized");
			return false;
		}

		msgpack_vec vecs[1];
		msgpack_in_vec mv = {
				.n_vecs = 1,
				.vecs = vecs
		};

		vecs[0].buf = range->ctx_buf;
		vecs[0].buf_sz = range->ctx_buf_sz;
		vecs[0].offset = 0;

		if (! cdt_context_read_check_peek(&mv)) {
			cf_warning(AS_QUERY, "invalid index context");
			return false;
		}
	}

	return true;
}

static bool
get_query_rps(const as_transaction* tr, uint32_t* rps)
{
	if (! as_transaction_has_recs_per_sec(tr)) {
		return true;
	}

	if (as_transaction_is_short_query(tr)) {
		cf_warning(AS_QUERY, "short query has recs-per-sec field");
		return false;
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_RECS_PER_SEC);

	if (as_msg_field_get_value_sz(f) != sizeof(uint32_t)) {
		cf_warning(AS_QUERY, "recs-per-sec field size not %zu",
				sizeof(uint32_t));
		return false;
	}

	*rps = cf_swap_from_be32(*(uint32_t*)f->data);

	return true;
}

static bool
get_query_socket_timeout(const as_transaction* tr, int32_t* timeout)
{
	if (! as_transaction_has_socket_timeout(tr)) {
		return true;
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_SOCKET_TIMEOUT);

	if (as_msg_field_get_value_sz(f) != sizeof(uint32_t)) {
		cf_warning(AS_QUERY, "socket-timeout field size not %zu",
				sizeof(uint32_t));
		return false;
	}

	uint32_t t = cf_swap_from_be32(*(uint32_t*)f->data);

	*timeout = t == 0 ? -1 : (int32_t)t;

	return true;
}

static bool
get_query_sample_max(const as_transaction* tr, uint64_t* sample_max)
{
	if (! as_transaction_has_sample_max(tr)) {
		return true;
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_SAMPLE_MAX);

	if (as_msg_field_get_value_sz(f) != sizeof(uint64_t)) {
		cf_warning(AS_QUERY, "sample-max field size not %zu", sizeof(uint64_t));
		return false;
	}

	*sample_max = cf_swap_from_be64(*(uint64_t*)f->data);

	return true;
}

static bool
get_query_filter_exp(const as_transaction* tr, as_exp** exp)
{
	if (! as_transaction_has_predexp(tr)) {
		return true;
	}

	const as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
			AS_MSG_FIELD_TYPE_PREDEXP);

	*exp = as_exp_filter_build(f, true);

	return *exp != NULL;
}

static bool
range_from_msg_integer(const uint8_t* data, as_query_range* range, uint32_t len)
{
	if (len < (sizeof(uint32_t) * 2) + (sizeof(uint64_t) * 2)) {
		cf_warning(AS_QUERY, "cannot parse integer range");
		return false;
	}

	uint32_t startl = cf_swap_from_be32(*((uint32_t*)data));

	if (startl != 8) {
		cf_warning(AS_QUERY, "can only handle 8 byte integers %u", startl);
		return false;
	}

	data += sizeof(uint32_t);

	int64_t start = (int64_t)cf_swap_from_be64(*((uint64_t*)data));

	data += sizeof(uint64_t);

	uint32_t endl = cf_swap_from_be32(*((uint32_t*)data));

	if (endl != 8) {
		cf_warning(AS_QUERY, "can only handle 8 byte integers %u", endl);
		return false;
	}

	data += sizeof(uint32_t);

	int64_t end = (int64_t)cf_swap_from_be64(*((uint64_t*)data));

	if (start > end) {
		cf_warning(AS_QUERY, "invalid range - %ld ... %ld", start, end);
		return false;
	}

	range->u.r.start = start;
	range->u.r.end = end;

	range->isrange = start != end;

	cf_debug(AS_QUERY, "query range - %ld ... %ld", start, end);

	return true;
}

static bool
range_from_msg_string(const uint8_t* data, as_query_range* range, uint32_t len)
{
	if (len < sizeof(uint32_t)) {
		cf_warning(AS_QUERY, "cannot parse string range");
		return false;
	}

	uint32_t startl = cf_swap_from_be32(*((uint32_t*)data));

	if (startl >= MAX_STRING_KSIZE) {
		cf_warning(AS_QUERY, "query string too long - %u", startl);
		return false;
	}

	data += sizeof(uint32_t);
	len -= (uint32_t)sizeof(uint32_t);

	if (len < startl) {
		cf_warning(AS_QUERY, "cannot parse string range");
		return false;
	}

	const char* startp = (const char*)data;

	// Currently, clients also send an 'end' string which is identical to the
	// 'start' string. Ignore that here for performance, since it's redundant.

	range->u.r.start = as_sindex_string_to_bval(startp, startl);
	range->u.r.end = range->u.r.start;

	range->blob_sz = startl;
	memcpy(range->stub, startp, startl < sizeof(range->stub) ?
			startl : sizeof(range->stub));

	range->isrange = false;

	cf_debug(AS_QUERY, "query on string %.*s", startl, startp);

	return true;
}

static bool
range_from_msg_blob(const uint8_t* data, as_query_range* range, uint32_t len)
{
	if (len < sizeof(uint32_t)) {
		cf_warning(AS_QUERY, "cannot parse blob range");
		return false;
	}

	uint32_t startl = cf_swap_from_be32(*((uint32_t*)data));

	if (startl >= MAX_BLOB_KSIZE) {
		cf_warning(AS_QUERY, "query blob too long - %u", startl);
		return false;
	}

	data += sizeof(uint32_t);
	len -= (uint32_t)sizeof(uint32_t);

	if (len < startl) {
		cf_warning(AS_QUERY, "cannot parse blob range");
		return false;
	}

	const uint8_t* startp = data;

	// Currently, clients also send an 'end' blob which is identical to the
	// 'start' blob. Ignore that here for performance, since it's redundant.

	range->u.r.start = as_sindex_blob_to_bval(startp, startl);
	range->u.r.end = range->u.r.start;

	range->blob_sz = startl;
	memcpy(range->stub, startp, startl < sizeof(range->stub) ?
			startl : sizeof(range->stub));

	range->isrange = false;

	cf_debug(AS_QUERY, "query on blob %.*s", startl, startp);

	return true;
}

static bool
range_from_msg_geojson(as_namespace* ns, const uint8_t* data,
		as_query_range* range, uint32_t len)
{
	if (len < sizeof(uint32_t)) {
		cf_warning(AS_QUERY, "cannot parse geojson range");
		return false;
	}

	uint32_t startl = cf_swap_from_be32(*((uint32_t*)data));

	if (startl == 0 || startl >= MAX_GEOJSON_KSIZE) {
		cf_warning(AS_QUERY, "invalid query key size - %u", startl);
		return false;
	}

	data += sizeof(uint32_t);
	len -= (uint32_t)sizeof(uint32_t);

	if (len < startl) {
		cf_warning(AS_QUERY, "cannot parse geojson range");
		return false;
	}

	const char* startp = (const char*)data;

	// Currently, clients also send an 'end' string which is identical to the
	// 'start' string. Ignore that here for performance, since it's redundant.

	as_query_geo_range* geo = &range->u.geo;

	if (! as_geojson_parse(ns, startp, startl, &geo->cellid, &geo->region)) {
		// as_geojson_parse will have printed a warning.
		return false;
	}

	if (geo->cellid != 0) { // regions-containing-point query
		uint64_t center[MAX_REGION_LEVELS];
		uint32_t ncenters;

		if (! geo_point_centers(geo->cellid, MAX_REGION_LEVELS, center,
				&ncenters)) {
			// geo_point_centers will have printed a warning.
			return false;
		}

		geo->r = cf_calloc(ncenters, sizeof(as_query_range_start_end));
		geo->num_r = (uint8_t)ncenters;

		// Geospatial queries use multiple srange elements.
		for (uint32_t i = 0; i < ncenters; i++) {
			geo->r[i].start = (int64_t)center[i];
			geo->r[i].end = (int64_t)center[i];
		}
	}
	else { // points-inside-region query
		uint64_t cellmin[MAX_REGION_CELLS];
		uint64_t cellmax[MAX_REGION_CELLS];
		uint32_t ncells;

		if (! geo_region_cover(ns, geo->region, MAX_REGION_CELLS, NULL, cellmin,
				cellmax, &ncells)) {
			cf_warning(AS_QUERY, "geo_region_cover failed");
			return false;
		}

		geo->r = cf_calloc(ncells, sizeof(as_query_range_start_end));
		geo->num_r = (uint8_t)ncells;

		as_incr_uint64(&ns->geo_region_query_count);
		as_add_uint64(&ns->geo_region_query_cells, (int64_t)ncells);

		// Geospatial queries use multiple srange elements. Many of the fields
		// are copied from the first cell because they were filled in above.
		for (uint32_t i = 0; i < ncells; i++) {
			geo->r[i].start = (int64_t)cellmin[i];
			geo->r[i].end = (int64_t)cellmax[i];
		}
	}

	// Sort the GEO ranges so that the client can resume based on the bval.
	sort_geo_range(geo);

	range->isrange = true;

	return true;
}

static void
sort_geo_range(as_query_geo_range* geo)
{
	if (geo->num_r == 1) {
		return;
	}

	// Using bubble sort - if the array is pre-sorted the cost is O(n).
	while (true) {
		bool swap = false;

		for (uint8_t i = 0; i < geo->num_r - 1; i++) {
			if ((uint64_t)geo->r[i].start > (uint64_t)geo->r[i + 1].start) {
				as_query_range_start_end temp = geo->r[i];

				geo->r[i] = geo->r[i + 1];
				geo->r[i + 1] = temp;

				swap = true;
			}
		}

		if (! swap) {
			return;
		}
	}
}

static bool
find_sindex(as_query_job* _job)
{
	as_query_range* range = _job->range;

	if (range == NULL) {
		return true;
	}

	_job->si = as_sindex_lookup_by_defn(_job->ns, _job->set_id, range->bin_name,
			range->bin_type, range->itype, range->ctx_buf, range->ctx_buf_sz);

	if (_job->si == NULL) {
		return false;
	}

	if (! _job->is_short) {
		strcpy(_job->si_name, _job->si->iname);
	}

	return true;
}

static bool
validate_background_query_rps(const as_namespace* ns, uint32_t* rps)
{
	uint32_t max_rps = as_load_uint32(&ns->background_query_max_rps);

	if (*rps > max_rps) {
		cf_warning(AS_QUERY, "query rps %u exceeds 'background-query-max-rps' %u",
				*rps, max_rps);
		return false;
	}

	if (*rps == 0) {
		*rps = max_rps;
	}

	return true;
}

static size_t
send_blocking_response_chunk(as_file_handle* fd_h, uint8_t* buf, size_t size,
		int32_t timeout, bool compress, as_proto_comp_stat* comp_stat)
{
	cf_socket* sock = &fd_h->sock;
	as_proto* proto = (as_proto*)buf;

	proto->version = PROTO_VERSION;
	proto->type = PROTO_TYPE_AS_MSG;
	proto->sz = size - sizeof(as_proto);
	as_proto_swap(proto);

	const uint8_t* msgp = (const uint8_t*)buf;

	if (compress) {
		msgp = as_proto_compress(msgp, &size, comp_stat);
	}

	if (cf_socket_send_all(sock, msgp, size, MSG_NOSIGNAL, timeout) < 0) {
		cf_warning(AS_QUERY, "error sending to %s - fd %d sz %lu %s",
				fd_h->client, CSFD(sock), size, cf_strerror(errno));
		return 0;
	}

	return sizeof(as_proto) + size;
}

static bool
record_matches_query(as_query_job* _job, as_storage_rd* rd)
{
	if (! record_changed_since_start(_job, rd->r) &&
			_job->si->ktype != AS_PARTICLE_TYPE_GEOJSON && // geo needs to check bounds anyway
			_job->si->ktype != AS_PARTICLE_TYPE_STRING &&
			_job->si->ktype != AS_PARTICLE_TYPE_BLOB) { // strings & blobs need to check for hash collisions
		return true;
	}

	as_bin stack_bins[RECORD_MAX_BINS];

	if (as_storage_rd_load_bins(rd, stack_bins) < 0) {
		return false; // for now - not separating error from false positive
	}

	const as_query_range* range = _job->range;
	const as_sindex* si = _job->si;

	const as_bin* b = as_bin_get_live(rd, si->bin_name);

	if (b == NULL) {
		return false;
	}

	as_particle_type type = as_bin_get_particle_type(b);
	as_bin ctx_bin = { 0 };

	if (range->ctx_buf != NULL) {
		if (type != AS_PARTICLE_TYPE_LIST && type != AS_PARTICLE_TYPE_MAP) {
			return false;
		}

		if (! as_bin_cdt_get_by_context(b, si->ctx_buf, si->ctx_buf_sz,
				&ctx_bin)) {
			return false;
		}

		type = as_bin_get_particle_type(&ctx_bin);

		if (type == AS_PARTICLE_TYPE_GEOJSON &&
				! as_bin_cdt_context_geojson_parse(&ctx_bin)) {
			return false;
		}

		b = &ctx_bin;
	}

	bool ret = false;

	switch (type) {
		case AS_PARTICLE_TYPE_INTEGER:
			if (type != si->ktype || si->itype != AS_SINDEX_ITYPE_DEFAULT) {
				break;
			}

			int64_t i = as_bin_particle_integer_value(b);

			// Start and end are same for point query.
			ret = range->u.r.start <= i && i <= range->u.r.end;
			break;
		case AS_PARTICLE_TYPE_STRING:
			if (type != si->ktype || si->itype != AS_SINDEX_ITYPE_DEFAULT) {
				break;
			}

			char* str;
			uint32_t len = as_bin_particle_string_ptr(b, &str);

			ret = strings_match(range, str, len);
			break;
		case AS_PARTICLE_TYPE_BLOB:
			if (type != si->ktype || si->itype != AS_SINDEX_ITYPE_DEFAULT) {
				break;
			}

			uint8_t* blob;
			uint32_t sz = as_bin_particle_blob_ptr(b, &blob);

			ret = blobs_match(range, blob, sz);
			break;
		case AS_PARTICLE_TYPE_GEOJSON:
			if (type != si->ktype || si->itype != AS_SINDEX_ITYPE_DEFAULT) {
				break;
			}

			as_namespace* ns = _job->ns;

			bool iswithin = as_particle_geojson_match(b->particle,
					range->u.geo.cellid, range->u.geo.region,
					ns->geo2dsphere_within_strict);

			if (iswithin) {
				as_incr_uint64(&ns->geo_region_query_points);
			}
			else {
				as_incr_uint64(&ns->geo_region_query_falsepos);
			}

			ret = iswithin;
			break;
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST:
			// For CDT bins, we need to see if any value within the CDT matches
			// the query. This can be performance hit for big lists and maps.
			ret = record_matches_query_cdt(_job, b);
			break;
		default:
			break;
	}

	if (range->ctx_buf != NULL) {
		as_bin_particle_destroy(&ctx_bin);
	}

	return ret;
}

static bool
record_matches_query_cdt(as_query_job* _job, const as_bin* b)
{
	const as_sindex* si = _job->si;
	bool match = false;
	as_particle_type type = as_bin_get_particle_type(b);

	if (type == AS_PARTICLE_TYPE_MAP) {
		if (si->itype == AS_SINDEX_ITYPE_MAPKEYS) {
			match = ! as_bin_map_foreach(b, match_mapkeys_foreach, _job);
		}
		else if (si->itype == AS_SINDEX_ITYPE_MAPVALUES) {
			match = ! as_bin_map_foreach(b, match_mapvalues_foreach, _job);
		}
	}
	else if (type == AS_PARTICLE_TYPE_LIST) {
		if (si->itype == AS_SINDEX_ITYPE_LIST) {
			match = ! as_bin_list_foreach(b, match_listeles_foreach, _job);
		}
	}

	return match;
}

// Stop iterating if a match is found (by returning false).
static bool
match_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)val;

	const as_query_job* _job = (as_query_job*)udata;
	msgpack_type type = msgpack_peek_type(key);

	switch (type) {
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
		return ! match_integer_element(_job, key);
	case MSGPACK_TYPE_STRING:
		return ! match_string_element(_job, key);
	case MSGPACK_TYPE_BYTES:
		return ! match_blob_element(_job, key);
	case MSGPACK_TYPE_GEOJSON:
		return ! match_geojson_element(_job, key);
	default:
		return true; // true means didn't match
	}
}

// Stop iterating if a match is found (by returning false).
static bool
match_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)key;

	const as_query_job* _job = (as_query_job*)udata;
	msgpack_type type = msgpack_peek_type(val);

	switch (type) {
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
		return ! match_integer_element(_job, val);
	case MSGPACK_TYPE_STRING:
		return ! match_string_element(_job, val);
	case MSGPACK_TYPE_BYTES:
		return ! match_blob_element(_job, val);
	case MSGPACK_TYPE_GEOJSON:
		return ! match_geojson_element(_job, val);
	default:
		return true; // true means didn't match
	}
}

// Stop iterating if a match is found (by returning false).
static bool
match_listeles_foreach(msgpack_in* element, void* udata)
{
	const as_query_job* _job = (as_query_job*)udata;
	msgpack_type type = msgpack_peek_type(element);

	switch (type) {
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
		return ! match_integer_element(_job, element);
	case MSGPACK_TYPE_STRING:
		return ! match_string_element(_job, element);
	case MSGPACK_TYPE_BYTES:
		return ! match_blob_element(_job, element);
	case MSGPACK_TYPE_GEOJSON:
		return ! match_geojson_element(_job, element);
	default:
		return true; // true means didn't match
	}
}

static bool
match_integer_element(const as_query_job* _job, msgpack_in* element)
{
	const as_query_range* range = _job->range;

	if (_job->si->ktype != AS_PARTICLE_TYPE_INTEGER) {
		return false;
	}

	int64_t i;

	if (! msgpack_get_int64(element, &i)) {
		return false;
	}

	// Start and end are same for point query.
	return range->u.r.start <= i && i <= range->u.r.end;
}

static bool
match_string_element(const as_query_job* _job, msgpack_in* element)
{
	const as_query_range* range = _job->range;

	if (_job->si->ktype != AS_PARTICLE_TYPE_STRING) {
		return false;
	}

	uint32_t str_sz;
	const uint8_t* str = msgpack_get_bin(element, &str_sz);

	if (str == NULL || str_sz == 0 || *str != AS_PARTICLE_TYPE_STRING) {
		return false;
	}

	// Skip as_bytes type.
	str++;
	str_sz--;

	return strings_match(range, (const char*)str, str_sz);
}

static bool
match_blob_element(const as_query_job* _job, msgpack_in* element)
{
	const as_query_range* range = _job->range;

	if (_job->si->ktype != AS_PARTICLE_TYPE_BLOB) {
		return false;
	}

	uint32_t blob_sz;
	const uint8_t* blob = msgpack_get_bin(element, &blob_sz);

	if (blob == NULL || blob_sz == 0 || *blob != AS_PARTICLE_TYPE_BLOB) {
		return false;
	}

	// Skip as_bytes type.
	blob++;
	blob_sz--;

	return blobs_match(range, blob, blob_sz);
}

static bool
match_geojson_element(const as_query_job* _job, msgpack_in* element)
{
	const as_query_range* range = _job->range;

	if (_job->si->ktype != AS_PARTICLE_TYPE_GEOJSON) {
		return false;
	}

	return as_particle_geojson_match_msgpack(element, range->u.geo.cellid,
			range->u.geo.region, _job->ns->geo2dsphere_within_strict);
}


//==============================================================================
// conn_query_job derived class implementation - not final class.
//

//----------------------------------------------------------
// conn_query_job typedefs and forward declarations.
//

typedef struct conn_query_job_s {
	// Base object must be first:
	as_query_job _base;

	// Derived class data:
	cf_mutex fd_lock;
	as_file_handle* fd_h;
	int32_t fd_timeout;

	bool compress_response;
	uint64_t net_io_bytes;
	uint64_t net_io_ns;
} conn_query_job;

static void conn_query_job_init(conn_query_job* job, const as_transaction* tr);
static void conn_query_job_destroy(conn_query_job* job);
static void conn_query_job_finish(conn_query_job* job);
static bool conn_query_job_send_response(conn_query_job* job, uint8_t* buf, size_t size);
static void conn_query_job_release_fd(conn_query_job* job, bool force_close);
static void conn_query_job_info(conn_query_job* job, cf_dyn_buf* db);

//----------------------------------------------------------
// conn_query_job API.
//

static void
conn_query_job_init(conn_query_job* job, const as_transaction* tr)
{
	cf_mutex_init(&job->fd_lock);

	job->fd_h = tr->from.proto_fd_h;
	job->fd_timeout = CF_SOCKET_TIMEOUT;

	job->compress_response = as_transaction_compress_response(tr);
}

// Note - for now this is not part of the vtable destroy chain, it is only an
// early exit helper (which is why the lock is also destroyed in finish).
static void
conn_query_job_destroy(conn_query_job* job)
{
	// Just undo conn_query_job_init(), nothing more.

	cf_mutex_destroy(&job->fd_lock);
}

static void
conn_query_job_finish(conn_query_job* job)
{
	as_query_job* _job = (as_query_job*)job;

	if (job->fd_h) {
		if (_job->is_short) {
			conn_query_job_release_fd(job, false);
		}
		else {
			uint64_t before_ns = cf_getns();

			// TODO - perhaps reflect in monitor if send fails?
			size_t size_sent = as_msg_send_fin_timeout(&job->fd_h->sock,
					(uint32_t)_job->abandoned, job->fd_timeout);

			if (size_sent != 0) {
				job->net_io_ns += cf_getns() - before_ns;
				job->net_io_bytes += size_sent;
			}

			conn_query_job_release_fd(job, size_sent == 0);
		}
	}

	cf_mutex_destroy(&job->fd_lock);
}

static bool
conn_query_job_send_response(conn_query_job* job, uint8_t* buf, size_t size)
{
	as_query_job* _job = (as_query_job*)job;

	cf_mutex_lock(&job->fd_lock);

	if (! job->fd_h) {
		cf_mutex_unlock(&job->fd_lock);
		// Job already abandoned.
		return false;
	}

	uint64_t before_ns = 0;

	if (! _job->is_short) {
		before_ns = cf_getns();
	}

	size_t size_sent = send_blocking_response_chunk(job->fd_h, buf, size,
			job->fd_timeout, job->compress_response,
			&_job->ns->query_comp_stat);

	if (size_sent == 0) {
		int reason = errno == ETIMEDOUT ?
				AS_QUERY_RESPONSE_TIMEOUT : AS_QUERY_RESPONSE_ERROR;

		conn_query_job_release_fd(job, true);
		cf_mutex_unlock(&job->fd_lock);
		as_query_manager_abandon_job(_job, reason);
		return false;
	}

	if (before_ns != 0) {
		job->net_io_ns += cf_getns() - before_ns;
	}

	job->net_io_bytes += size_sent;

	cf_mutex_unlock(&job->fd_lock);
	return true;
}

static void
conn_query_job_release_fd(conn_query_job* job, bool force_close)
{
	job->fd_h->last_used = cf_getns();
	as_end_of_transaction(job->fd_h, force_close);
	job->fd_h = NULL;
}

static void
conn_query_job_info(conn_query_job* job, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, ":net-io-bytes=");
	cf_dyn_buf_append_uint64(db, job->net_io_bytes);

	cf_dyn_buf_append_string(db, ":net-io-time=");
	cf_dyn_buf_append_uint64(db, job->net_io_ns / 1000000);

	cf_dyn_buf_append_string(db, ":socket-timeout=");
	cf_dyn_buf_append_uint32(db,
			job->fd_timeout == -1 ? 0 : (uint32_t)job->fd_timeout);
}



//==============================================================================
// basic_query_job derived class implementation.
//

//----------------------------------------------------------
// basic_query_job typedefs and forward declarations.
//

typedef struct basic_query_job_s {
	// Base object must be first:
	conn_query_job _base;

	// Derived class data:
	uint64_t end_ns;
	bool no_bin_data;
	uint64_t sample_max;
	uint64_t sample_count;
	as_exp* filter_exp;
	cf_vector* bin_names;
} basic_query_job;

static void basic_query_job_slice(as_query_job* _job, as_partition_reservation* rsv, cf_buf_builder** bb_r);
static void basic_query_job_finish(as_query_job* _job);
static void basic_query_job_destroy(as_query_job* _job);
static void basic_query_job_info(as_query_job* _job, cf_dyn_buf* db);

static const as_query_vtable basic_query_job_vtable = {
	basic_query_job_slice,
	basic_query_job_finish,
	basic_query_job_destroy,
	basic_query_job_info
};

typedef struct basic_query_slice_s {
	basic_query_job* job;
	cf_buf_builder** bb_r;
} basic_query_slice;

static void basic_query_job_init(basic_query_job* job);
static bool basic_query_get_bin_names(const as_transaction* tr, as_namespace* ns, cf_vector** bin_names);
static bool basic_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata);
static bool basic_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata);
static bool basic_query_filter_meta(const basic_query_job* job, const as_record* r, as_exp** exp);

//----------------------------------------------------------
// basic_query_job public API.
//

static int
basic_query_job_start(as_transaction* tr, as_namespace* ns)
{
	as_msg* m = &tr->msgp->msg;

	// TODO - phase out now? Or if/when future clients stop sending bit?
	if ((m->info3 & AS_MSG_INFO3_PARTITION_DONE) == 0) {
		cf_warning(AS_QUERY, "basic query expects unsupported pid-done-ok");
		return AS_ERR_UNSUPPORTED_FEATURE;
	}

	basic_query_job* job = cf_calloc(1, sizeof(basic_query_job));
	conn_query_job* conn_job = (conn_query_job*)job;
	as_query_job* _job = (as_query_job*)job;

	// Short queries only for basic queries, but use base job member.
	if (as_transaction_is_short_query(tr) && ! ns->force_long_queries) {
		_job->is_short = true;
		_job->do_inline = ns->inline_short_queries;
	}
	else {
		_job->relax = (m->info2 & AS_MSG_INFO2_RELAX_AP_LONG_QUERY) != 0;

		if (_job->relax && ns->cp) {
			cf_warning(AS_QUERY, "basic query in SC can't use 'relax' policy");
			cf_free(job);
			return AS_ERR_PARAMETER;
		}
	}

	as_query_job_init(_job, &basic_query_job_vtable, tr, ns);

	if (! get_query_set(tr, ns, _job->set_name, &_job->set_id) ||
			! get_query_pids(tr, &_job->pids, &_job->n_pids_requested,
					&_job->n_keyds_requested) ||
			! get_query_range(tr, ns, &_job->range) ||
			! get_query_rps(tr, &_job->rps)) {
		cf_warning(AS_QUERY, "basic query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	// TODO - roll into above, return AS_ERR_PARAMETER?
	if (_job->pids == NULL) {
		cf_warning(AS_QUERY, "basic query missing pids and digests fields");
		as_query_job_destroy(_job);
		return AS_ERR_UNSUPPORTED_FEATURE;
	}

	if (! find_sindex(_job)) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (_job->si != NULL && ! _job->si->readable) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_READABLE;
	}

	// Take ownership of socket from transaction.
	conn_query_job_init(conn_job, tr);

	if (! get_query_socket_timeout(tr, &conn_job->fd_timeout)) {
		cf_warning(AS_QUERY, "basic query job failed msg field processing");
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	basic_query_job_init(job);

	if (! get_query_sample_max(tr, &job->sample_max) ||
			! basic_query_get_bin_names(tr, ns, &job->bin_names) ||
			! get_query_filter_exp(tr, &job->filter_exp)) {
		cf_warning(AS_QUERY, "basic query job failed msg field processing");
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	job->no_bin_data = (m->info1 & AS_MSG_INFO1_GET_NO_BINS) != 0;

	int result = as_security_check_rps(tr->from.proto_fd_h, _job->rps,
			PERM_QUERY, false, &_job->rps_udata);

	if (result != AS_OK) {
		cf_warning(AS_QUERY, "basic query job failed quota %d", result);
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return result;
	}

	if (_job->is_short) {
		if (m->transaction_ttl != 0) {
			job->end_ns = tr->start_time +
					((uint64_t)m->transaction_ttl * 1000000);
		}
		else {
			job->end_ns = tr->start_time + DEFAULT_TTL_NS;
		}
	}
	else {
		cf_debug(AS_QUERY, "starting basic query job %lu {%s:%s:%s} n-pids-requested (%hu,%hu) rps %u sample-max %lu%s socket-timeout %d from %s",
				_job->trid, ns->name, _job->set_name,
				_job->si != NULL ? _job->si_name : "<pi-query>",
				_job->n_pids_requested, _job->n_keyds_requested, _job->rps,
				job->sample_max, job->no_bin_data ? " metadata-only" : "",
				conn_job->fd_timeout, _job->client);
	}

	result = as_query_manager_start_job(_job);

	if (result != AS_OK) {
		cf_warning(AS_QUERY, "basic query job %lu failed to start (%d)",
				_job->trid, result);
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return result;
	}

	return AS_OK;
}

//----------------------------------------------------------
// basic_query_job mandatory query_job interface.
//

static void
basic_query_job_slice(as_query_job* _job, as_partition_reservation* rsv,
		cf_buf_builder** bb_r)
{
	basic_query_job* job = (basic_query_job*)_job;

	if (*bb_r == NULL) {
		*bb_r = cf_buf_builder_create(INIT_BUF_BUILDER_SIZE);
		cf_buf_builder_reserve(bb_r, (int)sizeof(as_proto), NULL);
	}
	else if (rsv == NULL) { // this thread finished all its partitions
		if (_job->is_short) {
			as_msg_fin_bufbuilder(bb_r, _job->abandoned);
			// Won't send fin later in finish().
		}

		cf_buf_builder* bb = *bb_r;

		if (bb->used_sz > sizeof(as_proto)) {
			conn_query_job_send_response((conn_query_job*)job, bb->buf,
					bb->used_sz);
		}

		return;
	}

	as_index_tree* tree = rsv->tree;

	if (tree == NULL) {
		as_msg_pid_done_bufbuilder(bb_r, rsv->p->id, AS_ERR_UNAVAILABLE);
		return;
	}

	if (_job->set_id == INVALID_SET_ID && _job->set_name[0] != '\0') {
		return;
	}

	uint64_t slice_start = cf_getns();

	if (_job->is_short && slice_start > job->end_ns) {
		as_query_manager_abandon_job(_job, AS_ERR_TIMEOUT);
		return;
	}

	basic_query_slice slice = { job, bb_r };

	if (job->sample_max == 0 || job->sample_count < job->sample_max) {
		int64_t bval = 0;
		cf_digest* keyd = NULL;
		as_query_pid* qp = &_job->pids[rsv->p->id];

		if (qp->has_resume) {
			bval = qp->bval;
			keyd = &qp->keyd;
		}

		if (_job->si != NULL) {
			as_sindex_tree_query(_job->si, _job->range, rsv, bval, keyd,
					basic_query_job_reduce_cb, (void*)&slice);
		}
		else {
			if (! as_set_index_reduce(_job->ns, tree, _job->set_id, keyd,
					basic_pi_query_job_reduce_cb, (void*)&slice)) {
				as_index_reduce_from(tree, keyd, basic_pi_query_job_reduce_cb,
						(void*)&slice);
			}
		}
	}

	if (! _job->is_short) {
		cf_detail(AS_QUERY, "basic query job %lu pid %u took %lu us",
				_job->trid, rsv->p->id, (cf_getns() - slice_start) / 1000);
	}
}

static void
basic_query_job_finish(as_query_job* _job)
{
	conn_query_job_finish((conn_query_job*)_job);

	as_namespace* ns = _job->ns;

	switch (_job->abandoned) {
	case 0:
		if (_job->is_short) {
			if (_job->si == NULL) {
				ns->pi_query_hist_active = true;
				histogram_insert_data_point(ns->pi_query_hist, _job->start_ns);

				ns->pi_query_rec_count_hist_active = true;
				histogram_insert_raw(ns->pi_query_rec_count_hist,
						_job->n_succeeded);

				as_incr_uint64(&ns->n_pi_query_short_basic_complete);
			}
			else {
				ns->si_query_hist_active = true;
				histogram_insert_data_point(ns->si_query_hist, _job->start_ns);

				ns->si_query_rec_count_hist_active = true;
				histogram_insert_raw(ns->si_query_rec_count_hist,
						_job->n_succeeded);

				as_incr_uint64(&ns->n_si_query_short_basic_complete);
			}
		}
		else {
			as_incr_uint64(_job->si == NULL ?
					&ns->n_pi_query_long_basic_complete :
					&ns->n_si_query_long_basic_complete);
		}
		break;
	case AS_ERR_TIMEOUT:
		// Note - can't timeout long queries.
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_short_basic_timeout :
				&ns->n_si_query_short_basic_timeout);
		break;
	case AS_ERR_QUERY_ABORT:
		// Note - can't abort short queries.
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_long_basic_abort :
				&ns->n_si_query_long_basic_abort);
		break;
	case AS_ERR_UNKNOWN:
	case AS_QUERY_RESPONSE_ERROR:
	case AS_QUERY_RESPONSE_TIMEOUT:
	default:
		if (_job->is_short) {
			as_incr_uint64(_job->si == NULL ?
					&ns->n_pi_query_short_basic_error :
					&ns->n_si_query_short_basic_error);
		}
		else {
			as_incr_uint64(_job->si == NULL ?
					&ns->n_pi_query_long_basic_error :
					&ns->n_si_query_long_basic_error);
		}
		break;
	}

	if (! _job->is_short) {
		cf_debug(AS_QUERY, "finished basic query job %lu (%d) in %lu ms",
				_job->trid, _job->abandoned,
				(cf_getns() - _job->start_ns) / 1000000);
	}
}

static void
basic_query_job_destroy(as_query_job* _job)
{
	basic_query_job* job = (basic_query_job*)_job;

	if (job->bin_names != NULL) {
		cf_vector_destroy(job->bin_names);
	}

	as_exp_destroy(job->filter_exp);
}

static void
basic_query_job_info(as_query_job* _job, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, ":job-type=");
	cf_dyn_buf_append_string(db, query_type_str(QUERY_TYPE_BASIC));

	conn_query_job_info((conn_query_job*)_job, db);
}

//----------------------------------------------------------
// basic_query_job utilities.
//

static void
basic_query_job_init(basic_query_job* job)
{
	(void)job;
}

static bool
basic_query_get_bin_names(const as_transaction* tr, as_namespace* ns,
		cf_vector** bin_names)
{
	bool has_binlist = as_transaction_has_query_binlist(tr);
	const as_msg* m = &tr->msgp->msg;
	bool has_ops = m->n_ops > 0;

	if (has_binlist && has_ops) {
		cf_warning(AS_QUERY, "both binlist and ops cannot be specified");
		return false;
	}

	if (! has_binlist && ! has_ops) {
		return true;
	}
	// else - bin selection requested.

	// TODO - temporary - won't need bin-list support after January 2023.
	if (has_binlist) {
		const as_msg_field* f = as_msg_field_get(m,
				AS_MSG_FIELD_TYPE_QUERY_BINLIST);
		const uint8_t* data = f->data;
		uint32_t n_bins = *data++; // only <= 255 bins (ops do more)

		*bin_names = cf_vector_create(AS_BIN_NAME_MAX_SZ, n_bins, 0);

		for (uint32_t i = 0; i < n_bins; ++i) {
			uint32_t len = *data++;

			if (! as_bin_name_check(data, len)) {
				cf_warning(AS_QUERY, "ignoring bad bin name %.*s (%u)", len,
						data, len);
				continue;
			}

			char bin_name[AS_BIN_NAME_MAX_SZ];

			memcpy(bin_name, data, len);
			bin_name[len] = '\0';

			cf_vector_append(*bin_names, bin_name);

			data += len;
		}

		return true;
	}
	// else - has ops

	*bin_names = cf_vector_create(AS_BIN_NAME_MAX_SZ, m->n_ops, 0);

	as_msg_op* op = NULL;
	uint16_t n = 0;

	while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
		if (! as_bin_name_check(op->name, op->name_sz)) {
			cf_warning(AS_QUERY, "ignoring bad bin name %.*s (%u)", op->name_sz,
					op->name, op->name_sz);
			continue;
		}

		char bin_name[AS_BIN_NAME_MAX_SZ];

		memcpy(bin_name, op->name, op->name_sz);
		bin_name[op->name_sz] = '\0';

		cf_vector_append(*bin_names, bin_name);
	}

	return true;
}

static bool
basic_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	return basic_query_job_reduce_cb(r_ref, 0, udata);
}

static bool
basic_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata)
{
	basic_query_slice* slice = (basic_query_slice*)udata;
	basic_query_job* job = slice->job;
	as_query_job* _job = (as_query_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return false;
	}

	as_record* r = read_r(ns, r_ref->r, false); // is not an MRT

	if (r == NULL) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (_job->set_id == INVALID_SET_ID &&
			as_mrt_monitor_is_monitor_record(ns, r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (excluded_set(r, _job->set_id)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	as_exp* filter_exp = NULL;

	if (! basic_query_filter_meta(job, r, &filter_exp)) {
		as_record_done(r_ref, ns);
		as_incr_uint64(&_job->n_filtered_meta);
		return true;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	if (filter_exp != NULL && read_and_filter_bins(&rd, filter_exp) != 0) {
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		as_incr_uint64(&_job->n_filtered_bins);

		if (ns->storage_type == AS_STORAGE_ENGINE_SSD && ! _job->is_short) {
			throttle_sleep(_job);
		}

		return true;
	}

	if (_job->si != NULL && ! record_matches_query(_job, &rd)) {
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return true;
	}

	bool last_sample = false;

	if (job->sample_max != 0) { // sample-max checks post-filters
		uint64_t count = as_aaf_uint64(&job->sample_count, 1);

		if (count > job->sample_max) {
			as_storage_record_close(&rd);
			as_record_done(r_ref, ns);
			return false;
		}

		if (count == job->sample_max) {
			last_sample = true;
		}
	}

	if (job->no_bin_data) {
		as_msg_make_response_bufbuilder(slice->bb_r, &rd, true, NULL,
				_job->si != NULL, bval);
	}
	else {
		as_bin stack_bins[RECORD_MAX_BINS];

		if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
			cf_warning(AS_QUERY, "job %lu - record unreadable", _job->trid);
			as_storage_record_close(&rd);
			as_record_done(r_ref, ns);
			as_incr_uint64(&_job->n_failed);
			return true;
		}

		as_msg_make_response_bufbuilder(slice->bb_r, &rd, false, job->bin_names,
				_job->si != NULL, bval);
	}

	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);
	as_incr_uint64(&_job->n_succeeded);

	if (last_sample) {
		return false;
	}

	if (! _job->is_short) {
		throttle_sleep(_job);
	}

	cf_buf_builder* bb = *slice->bb_r;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > QUERY_CHUNK_LIMIT) {
		if (! conn_query_job_send_response((conn_query_job*)job, bb->buf,
				bb->used_sz)) {
			return true;
		}

		cf_buf_builder_reset(bb);
		cf_buf_builder_reserve(slice->bb_r, (int)sizeof(as_proto), NULL);
	}

	return true;
}

static bool
basic_query_filter_meta(const basic_query_job* job, const as_record* r,
		as_exp** exp)
{
	*exp = job->filter_exp;

	if (*exp == NULL) {
		return true;
	}

	as_namespace* ns = ((as_query_job*)job)->ns;
	as_exp_ctx ctx = { .ns = ns, .r = (as_record*)r };
	as_exp_trilean tv = as_exp_matches_metadata(*exp, &ctx);

	if (tv == AS_EXP_UNK) {
		return true; // caller must later check bins using *exp
	}
	// else - caller will not need to apply filter later.

	*exp = NULL;

	return tv == AS_EXP_TRUE;
}


//==============================================================================
// aggr_query_job derived class implementation.
//

//----------------------------------------------------------
// aggr_query_job typedefs and forward declarations.
//

typedef struct aggr_query_job_s {
	// Base object must be first:
	conn_query_job _base;

	// Derived class data:
	as_aggr_call aggr_call;
	bool ostream_failed; // TODO - hack for hotfix - replace eventually
} aggr_query_job;

static void aggr_query_job_slice(as_query_job* _job, as_partition_reservation* rsv, cf_buf_builder** bb_r);
static void aggr_query_job_finish(as_query_job* _job);
static void aggr_query_job_destroy(as_query_job* _job);
static void aggr_query_job_info(as_query_job* _job, cf_dyn_buf* db);

static const as_query_vtable aggr_query_job_vtable = {
	aggr_query_job_slice,
	aggr_query_job_finish,
	aggr_query_job_destroy,
	aggr_query_job_info
};

typedef struct aggr_query_slice_s {
	aggr_query_job* job;
	cf_ll* ll;
	cf_buf_builder** bb_r;
} aggr_query_slice;

static void aggr_query_job_init(aggr_query_job* job);
static bool aggr_query_init(as_aggr_call* call, const as_transaction* tr);
static bool aggr_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata);
static bool aggr_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata);
static void aggr_query_add_digest(const as_namespace* ns, cf_ll* ll, as_index_ref* r_ref);
static as_stream_status aggr_query_ostream_write(void* udata, as_val* val);
static bool aggr_query_pre_check(void* udata, udf_record* urecord);
static void aggr_query_add_val_response(aggr_query_slice* slice, const as_val* val, bool success);

static const as_aggr_hooks pi_query_aggr_hooks = {
	.ostream_write = aggr_query_ostream_write,
	.pre_check     = NULL
};

static const as_aggr_hooks si_query_aggr_hooks = {
	.ostream_write = aggr_query_ostream_write,
	.pre_check     = aggr_query_pre_check
};

//----------------------------------------------------------
// aggr_query_job public API.
//

static int
aggr_query_job_start(as_transaction* tr, as_namespace* ns)
{
	// Temporary security vulnerability protection.
	if (g_config.udf_execution_disabled) {
		cf_warning(AS_QUERY, "aggregation query job forbidden");
		return AS_ERR_FORBIDDEN;
	}

	if (as_transaction_is_short_query(tr)) {
		cf_warning(AS_QUERY, "aggregation queries can't be 'short' queries");
		return AS_ERR_PARAMETER;
	}

	if (as_transaction_has_predexp(tr)) {
		cf_warning(AS_QUERY, "aggregation queries do not support predexp filters");
		return AS_ERR_UNSUPPORTED_FEATURE;
	}

	aggr_query_job* job = cf_calloc(1, sizeof(aggr_query_job));
	conn_query_job* conn_job = (conn_query_job*)job;
	as_query_job* _job = (as_query_job*)job;

	as_query_job_init(_job, &aggr_query_job_vtable, tr, ns);

	if (! get_query_set(tr, ns, _job->set_name, &_job->set_id) ||
			! get_query_range(tr, ns, &_job->range) ||
			! get_query_rps(tr, &_job->rps)) {
		cf_warning(AS_QUERY, "aggregation query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	if (_job->set_id == INVALID_SET_ID && _job->set_name[0] != '\0') {
		as_query_job_destroy(_job);
		return AS_ERR_NOT_FOUND;
	}

	if (! find_sindex(_job)) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (_job->si != NULL && ! _job->si->readable) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_READABLE;
	}

	// Take ownership of socket from transaction.
	conn_query_job_init(conn_job, tr);

	if (! get_query_socket_timeout(tr, &conn_job->fd_timeout)) {
		cf_warning(AS_QUERY, "aggregation query job failed msg field processing");
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	aggr_query_job_init(job);

	int result = as_security_check_rps(tr->from.proto_fd_h, _job->rps,
			PERM_UDF_QUERY, false, &_job->rps_udata);

	if (result != AS_OK) {
		cf_warning(AS_QUERY, "aggregation query job failed quota %d", result);
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return result;
	}

	if (! aggr_query_init(&job->aggr_call, tr)) {
		cf_warning(AS_QUERY, "aggregation query job failed call init");
		conn_query_job_destroy(conn_job);
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	cf_debug(AS_QUERY, "starting aggregation query job %lu {%s:%s:%s} rps %u socket-timeout %d from %s",
			_job->trid, ns->name, _job->set_name,
			_job->si != NULL ? _job->si_name : "<pi-query>",
			_job->rps, conn_job->fd_timeout, _job->client);

	if ((result = as_query_manager_start_job(_job)) != AS_OK) {
		cf_warning(AS_QUERY, "aggregation query job %lu failed to start (%d)",
				_job->trid, result);
		conn_query_job_destroy((conn_query_job*)job);
		as_query_job_destroy(_job);
		return result;
	}

	return AS_OK;
}

//----------------------------------------------------------
// aggr_query_job mandatory query_job interface.
//

static void
aggr_query_job_slice(as_query_job* _job, as_partition_reservation* rsv,
		cf_buf_builder** bb_r)
{
	if (rsv == NULL) { // this thread finished all its partitions
		return;
	}

	aggr_query_job* job = (aggr_query_job*)_job;
	cf_ll ll;

	cf_ll_init(&ll, as_aggr_keys_destroy_cb, false);

	cf_buf_builder* bb = *bb_r;

	if (bb == NULL) {
		bb = cf_buf_builder_create(INIT_BUF_BUILDER_SIZE);
	}
	else {
		cf_buf_builder_reset(bb);
	}

	cf_buf_builder_reserve(&bb, (int)sizeof(as_proto), NULL);

	aggr_query_slice slice = { job, &ll, &bb };

	if (_job->si != NULL) {
		as_sindex_tree_query(_job->si, _job->range, rsv, 0, NULL,
				aggr_query_job_reduce_cb, (void*)&slice);
	}
	else {
		if (! as_set_index_reduce(_job->ns, rsv->tree, _job->set_id, NULL,
				aggr_pi_query_job_reduce_cb, (void*)&slice)) {
			as_index_reduce(rsv->tree, aggr_pi_query_job_reduce_cb,
					(void*)&slice);
		}
	}

	if (cf_ll_size(&ll) != 0) {
		as_result result;
		as_result_init(&result);

		int ret = as_aggr_process(_job->ns, rsv, &job->aggr_call, &ll,
				(void*)&slice, &result);

		if (ret != 0) {
			char* rs = as_module_err_string(ret);

			if (result.value) {
				as_string* lua_s = as_string_fromval(result.value);
				char* lua_err = (char*)as_string_tostring(lua_s);

				if (lua_err) {
					size_t l_rs_len = strlen(rs);

					rs = cf_realloc(rs, l_rs_len + strlen(lua_err) + 4);
					sprintf(&rs[l_rs_len], " : %s", lua_err);
				}
			}

			const as_val* v = (as_val*)as_string_new(rs, false);

			aggr_query_add_val_response(&slice, v, false);
			as_val_destroy(v);
			cf_free(rs);
			as_query_manager_abandon_job(_job, AS_ERR_UNKNOWN);
		}
		// TODO - hack for hotfix - replace eventually ...
		else if (job->ostream_failed) {
			char* rs = as_module_err_string(1); // TODO - anything better?
			const as_val* v = (as_val*)as_string_new(rs, false);

			aggr_query_add_val_response(&slice, v, false);
			as_val_destroy(v);
			cf_free(rs);
			as_query_manager_abandon_job(_job, AS_ERR_UNKNOWN);
		}

		as_result_destroy(&result);
	}

	as_aggr_release_udata udata = { .ns = _job->ns, .tree = rsv->tree };

	cf_ll_reduce(&ll, true, as_aggr_keys_release_cb, &udata);

	if (bb->used_sz > sizeof(as_proto)) {
		conn_query_job_send_response((conn_query_job*)job, bb->buf,
				bb->used_sz);
	}

	*bb_r = bb;
}

static void
aggr_query_job_finish(as_query_job* _job)
{
	aggr_query_job* job = (aggr_query_job*)_job;

	conn_query_job_finish((conn_query_job*)job);

	if (job->aggr_call.def.arglist) {
		as_list_destroy(job->aggr_call.def.arglist);
		job->aggr_call.def.arglist = NULL;
	}

	as_namespace* ns = _job->ns;

	switch (_job->abandoned) {
	case 0:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_aggr_complete : &ns->n_si_query_aggr_complete);
		break;
	case AS_ERR_QUERY_ABORT:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_aggr_abort : &ns->n_si_query_aggr_abort);
		break;
	case AS_ERR_UNKNOWN:
	case AS_QUERY_RESPONSE_ERROR:
	case AS_QUERY_RESPONSE_TIMEOUT:
	default:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_aggr_error : &ns->n_si_query_aggr_error);
		break;
	}

	cf_debug(AS_QUERY, "finished aggregation query job %lu (%d)", _job->trid,
			_job->abandoned);
}

static void
aggr_query_job_destroy(as_query_job* _job)
{
	aggr_query_job* job = (aggr_query_job*)_job;

	if (job->aggr_call.def.arglist) {
		as_list_destroy(job->aggr_call.def.arglist);
	}
}

static void
aggr_query_job_info(as_query_job* _job, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, ":job-type=");
	cf_dyn_buf_append_string(db, query_type_str(QUERY_TYPE_AGGR));

	conn_query_job_info((conn_query_job*)_job, db);
}

//----------------------------------------------------------
// aggr_query_job utilities.
//

static void
aggr_query_job_init(aggr_query_job* job)
{
	(void)job;
}

static bool
aggr_query_init(as_aggr_call* call, const as_transaction* tr)
{
	if (! udf_def_init_from_msg(&call->def, tr)) {
		return false;
	}

	call->aggr_hooks = as_transaction_has_where_clause(tr) ?
			&si_query_aggr_hooks : &pi_query_aggr_hooks;

	return true;
}

static bool
aggr_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	return aggr_query_job_reduce_cb(r_ref, 0, udata);
}

static bool
aggr_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata)
{
	(void)bval;

	aggr_query_slice* slice = (aggr_query_slice*)udata;
	aggr_query_job* job = slice->job;
	as_query_job* _job = (as_query_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return false;
	}

	as_record* r = read_r(ns, r_ref->r, false); // is not an MRT

	if (r == NULL) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (_job->set_id == INVALID_SET_ID &&
			as_mrt_monitor_is_monitor_record(ns, r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (excluded_set(r, _job->set_id)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	aggr_query_add_digest(ns, slice->ll, r_ref);

	as_record_done(r_ref, ns);
	as_incr_uint64(&_job->n_succeeded);

	throttle_sleep(_job);

	return true;
}

static void
aggr_query_add_digest(const as_namespace* ns, cf_ll* ll, as_index_ref* r_ref)
{
	as_aggr_keys_ll_element* tail_e = (as_aggr_keys_ll_element*)ll->tail;
	as_aggr_keys_arr* keys_arr;

	if (tail_e != NULL) {
		keys_arr = tail_e->keys_arr;

		if (keys_arr->num == AS_AGGR_KEYS_PER_ARR) {
			tail_e = NULL;
		}
	}

	if (tail_e == NULL) {
		keys_arr = cf_malloc(sizeof(as_aggr_keys_arr));
		keys_arr->num = 0;

		tail_e = cf_malloc(sizeof(as_aggr_keys_ll_element));
		tail_e->keys_arr = keys_arr;

		cf_ll_append(ll, (cf_ll_element*)tail_e);
	}

	as_record* r = r_ref->r; // (for MRT dual, this is tree r, with good rc)

	// TODO - proper EE split.
	if (ns->pi_xmem_type == CF_XMEM_TYPE_FLASH) {
		keys_arr->u.digests[keys_arr->num] = r->keyd;
	}
	else {
		if (r->rc > 50000) {
			return; // hack - avoid ref-count overflow
		}

		as_index_reserve(r);
		keys_arr->u.handles[keys_arr->num] = (uint64_t)r_ref->r_h;
	}

	keys_arr->num++;
}

static as_stream_status
aggr_query_ostream_write(void* udata, as_val* val)
{
	aggr_query_slice* slice = (aggr_query_slice*)udata;

	if (val != NULL) {
		aggr_query_add_val_response(slice, val, true);
		as_val_destroy(val);
	}

	return AS_STREAM_OK;
}

static bool
aggr_query_pre_check(void* udata, udf_record* urecord)
{
	if (udf_record_load(urecord) != 0) {
		return false;
	}

	aggr_query_slice* slice = (aggr_query_slice*)udata;
	as_query_job* _job = (as_query_job*)slice->job;

	if (! record_matches_query(_job, urecord->rd)) {
		return false;
	}

	return true;
}

static void
aggr_query_add_val_response(aggr_query_slice* slice, const as_val* val,
		bool success)
{
	uint32_t size = as_particle_asval_client_value_size(val);
	cf_buf_builder* bb = *slice->bb_r;

	if ((size == 0 && (val->type == AS_LIST || val->type == AS_MAP)) ||
			// 7 is name length of SUCCESS or FAILURE bin. TODO - clean up.
			size + sizeof(as_msg) + sizeof(as_msg_op) + 7 + bb->used_sz >
			PROTO_SIZE_MAX) {
		cf_warning(AS_QUERY, "aggregation output too big (%u)", size);

		// TODO - hack for hotfix - replace eventually ...
		// (We'd like to return AS_STREAM_ERR from aggr_query_ostream_write()
		// instead, but currently that has no effect.)
		slice->job->ostream_failed = true;
		return;
	}

	as_msg_make_val_response_bufbuilder(val, slice->bb_r, size, success);
	bb = *slice->bb_r;

	conn_query_job* conn_job = (conn_query_job*)slice->job;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > QUERY_CHUNK_LIMIT) {
		if (! conn_query_job_send_response(conn_job, bb->buf, bb->used_sz)) {
			return;
		}

		cf_buf_builder_reset(bb);
		cf_buf_builder_reserve(slice->bb_r, (int)sizeof(as_proto), NULL);
	}
}


//==============================================================================
// udf_bg_query_job derived class implementation.
//

//----------------------------------------------------------
// udf_bg_query_job typedefs and forward declarations.
//

typedef struct udf_bg_query_job_s {
	// Base object must be first:
	as_query_job _base;

	// Derived class data:
	iudf_origin origin;
	uint32_t n_active_tr;
} udf_bg_query_job;

static void udf_bg_query_job_slice(as_query_job* _job, as_partition_reservation* rsv, cf_buf_builder** bb_r);
static void udf_bg_query_job_finish(as_query_job* _job);
static void udf_bg_query_job_destroy(as_query_job* _job);
static void udf_bg_query_job_info(as_query_job* _job, cf_dyn_buf* db);

static const as_query_vtable udf_bg_query_job_vtable = {
	udf_bg_query_job_slice,
	udf_bg_query_job_finish,
	udf_bg_query_job_destroy,
	udf_bg_query_job_info
};

static void udf_bg_query_job_init(udf_bg_query_job* job);
static bool udf_bg_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata);
static bool udf_bg_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata);
static bool udf_bg_query_record_check(void* udata, as_storage_rd* rd);
static void udf_bg_query_tr_complete(void* udata, int result);

//----------------------------------------------------------
// udf_bg_query_job public API.
//

int
udf_bg_query_job_start(as_transaction* tr, as_namespace* ns)
{
	// Temporary security vulnerability protection.
	if (g_config.udf_execution_disabled) {
		cf_warning(AS_QUERY, "udf-bg query job forbidden");
		return AS_ERR_FORBIDDEN;
	}

	if (as_transaction_is_short_query(tr)) {
		cf_warning(AS_QUERY, "udf-bg queries can't be 'short' queries");
		return AS_ERR_PARAMETER;
	}

	udf_bg_query_job* job = cf_calloc(1, sizeof(udf_bg_query_job));
	as_query_job* _job = (as_query_job*)job;

	as_query_job_init(_job, &udf_bg_query_job_vtable, tr, ns);

	if (! get_query_set(tr, ns, _job->set_name, &_job->set_id) ||
			! get_query_range(tr, ns, &_job->range) ||
			! get_query_rps(tr, &_job->rps)) {
		cf_warning(AS_QUERY, "udf-bg query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	if (_job->set_id == INVALID_SET_ID && _job->set_name[0] != '\0') {
		as_query_job_destroy(_job);
		return AS_ERR_NOT_FOUND;
	}

	if (! validate_background_query_rps(ns, &_job->rps)) {
		cf_warning(AS_QUERY, "udf-bg query job failed rps check");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	if (! find_sindex(_job)) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (_job->si != NULL && ! _job->si->readable) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_READABLE;
	}

	udf_bg_query_job_init(job);

	if (! get_query_filter_exp(tr, &job->origin.filter_exp)) {
		cf_warning(AS_QUERY, "udf-bg query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	int result = as_security_check_rps(tr->from.proto_fd_h, _job->rps,
			PERM_UDF_QUERY, true, &_job->rps_udata);

	if (result != AS_OK) {
		cf_warning(AS_QUERY, "udf-bg query job failed quota %d", result);
		as_query_job_destroy(_job);
		return result;
	}

	job->n_active_tr = 0;

	if (! udf_def_init_from_msg(&job->origin.def, tr)) {
		cf_warning(AS_QUERY, "udf-bg query job failed def init");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	as_msg* om = &tr->msgp->msg;
	uint8_t info2 = (uint8_t)(AS_MSG_INFO2_WRITE |
			(om->info2 & AS_MSG_INFO2_DURABLE_DELETE));

	job->origin.msgp = as_msg_create_internal(ns->name, 0, info2, 0,
			om->record_ttl, 0, NULL, 0);

	if (_job->si != NULL) {
		job->origin.check_cb = udf_bg_query_record_check;
	}

	job->origin.done_cb = udf_bg_query_tr_complete;
	job->origin.udata = (void*)job;

	cf_info(AS_QUERY, "starting udf-bg query job %lu {%s:%s:%s} rps %u from %s",
			_job->trid, ns->name, _job->set_name,
			_job->si != NULL ? _job->si_name : "<pi-query>", _job->rps,
			_job->client);

	if ((result = as_query_manager_start_job(_job)) != AS_OK) {
		cf_warning(AS_QUERY, "udf-bg query job %lu failed to start (%d)",
				_job->trid, result);
		as_query_job_destroy(_job);
		return result;
	}

	if (as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_OK)) {
		tr->from.proto_fd_h->last_used = cf_getns();
		as_end_of_transaction_ok(tr->from.proto_fd_h);
	}
	else {
		cf_warning(AS_QUERY, "udf-bg query job error sending fin");
		as_end_of_transaction_force_close(tr->from.proto_fd_h);
		// No point returning an error - it can't be reported on this socket.
	}

	tr->from.proto_fd_h = NULL;

	return AS_OK;
}

//----------------------------------------------------------
// udf_bg_query_job mandatory query_job interface.
//

static void
udf_bg_query_job_slice(as_query_job* _job, as_partition_reservation* rsv,
		cf_buf_builder** bb_r)
{
	(void)bb_r;

	if (_job->si != NULL) {
		as_sindex_tree_query(_job->si, _job->range, rsv, 0, NULL,
				udf_bg_query_job_reduce_cb, (void*)_job);
	}
	else {
		if (! as_set_index_reduce(_job->ns, rsv->tree, _job->set_id, NULL,
				udf_bg_pi_query_job_reduce_cb, (void*)_job)) {
			as_index_reduce(rsv->tree, udf_bg_pi_query_job_reduce_cb,
					(void*)_job);
		}
	}
}

static void
udf_bg_query_job_finish(as_query_job* _job)
{
	udf_bg_query_job* job = (udf_bg_query_job*)_job;

	while (job->n_active_tr != 0) {
		usleep(100);
	}

	// Subsequent activity may require an 'acquire' barrier.
	as_fence_acq();

	as_namespace* ns = _job->ns;

	switch (_job->abandoned) {
	case 0:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_udf_bg_complete :
				&ns->n_si_query_udf_bg_complete);
		break;
	case AS_ERR_QUERY_ABORT:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_udf_bg_abort : &ns->n_si_query_udf_bg_abort);
		break;
	case AS_ERR_UNKNOWN:
	default:
		// Note - this is unreachable for now - we only abandon via abort.
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_udf_bg_error : &ns->n_si_query_udf_bg_error);
		break;
	}

	cf_info(AS_QUERY, "finished udf-bg query job %lu (%d)", _job->trid,
			_job->abandoned);
}

static void
udf_bg_query_job_destroy(as_query_job* _job)
{
	udf_bg_query_job* job = (udf_bg_query_job*)_job;

	iudf_origin_destroy(&job->origin);
}

static void
udf_bg_query_job_info(as_query_job* _job, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, ":job-type=");
	cf_dyn_buf_append_string(db, query_type_str(QUERY_TYPE_UDF_BG));

	cf_dyn_buf_append_string(db, ":net-io-bytes=");
	cf_dyn_buf_append_uint64(db, sizeof(cl_msg)); // original synchronous fin

	cf_dyn_buf_append_string(db, ":socket-timeout=");
	cf_dyn_buf_append_uint32(db, CF_SOCKET_TIMEOUT);

	udf_bg_query_job* job = (udf_bg_query_job*)_job;

	cf_dyn_buf_append_format(db,
			":udf-filename=%s:udf-function=%s:udf-active=%u",
			job->origin.def.filename, job->origin.def.function,
			job->n_active_tr);
}

//----------------------------------------------------------
// udf_bg_query_job utilities.
//

static void
udf_bg_query_job_init(udf_bg_query_job* job)
{
	(void)job;
}

static bool
udf_bg_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	return udf_bg_query_job_reduce_cb(r_ref, 0, udata);
}

static bool
udf_bg_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata)
{
	(void)bval;

	as_query_job* _job = (as_query_job*)udata;
	udf_bg_query_job* job = (udf_bg_query_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return false;
	}

	as_index* r = r_ref->r;

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (_job->set_id == INVALID_SET_ID &&
			as_mrt_monitor_is_monitor_record(ns, r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (excluded_set(r, _job->set_id)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	as_exp_ctx ctx = { .ns = ns, .r = r };

	if (job->origin.filter_exp != NULL &&
			as_exp_matches_metadata(job->origin.filter_exp, &ctx) ==
					AS_EXP_FALSE) {
		as_record_done(r_ref, ns);
		as_incr_uint64(&_job->n_filtered_meta);
		as_incr_uint64(&ns->n_udf_sub_udf_filtered_out);
		return true;
	}

	// Save this before releasing record.
	cf_digest keyd = r->keyd;

	// Release record lock before throttling and enqueuing transaction.
	as_record_done(r_ref, ns);

	// Prefer not reaching target RPS to queue buildup and transaction timeouts.
	while (as_load_uint32(&job->n_active_tr) > MAX_ACTIVE_TRANSACTIONS) {
		usleep(1000);
	}

	throttle_sleep(_job);

	as_transaction tr;
	as_transaction_init_iudf(&tr, ns, &keyd, &job->origin);

	as_incr_uint32(&job->n_active_tr);
	as_service_enqueue_internal(&tr);

	return true;
}

static bool
udf_bg_query_record_check(void* udata, as_storage_rd* rd)
{
	return record_matches_query((as_query_job*)udata, rd);
}

static void
udf_bg_query_tr_complete(void* udata, int result)
{
	as_query_job* _job = (as_query_job*)udata;
	udf_bg_query_job* job = (udf_bg_query_job*)_job;

	switch (result) {
	case AS_OK:
		as_incr_uint64(&_job->n_succeeded);
		break;
	case AS_ERR_NOT_FOUND: // record deleted after generating tr
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&_job->n_filtered_bins);
		break;
	default:
		as_incr_uint64(&_job->n_failed);
		break;
	}

	as_decr_uint32_rls(&job->n_active_tr);
}



//==============================================================================
// ops_bg_query_job derived class implementation.
//

//----------------------------------------------------------
// ops_bg_query_job typedefs and forward declarations.
//

typedef struct ops_bg_query_job_s {
	// Base object must be first:
	as_query_job _base;

	// Derived class data:
	iops_origin origin;
	uint32_t n_active_tr;
} ops_bg_query_job;

static void ops_bg_query_job_slice(as_query_job* _job, as_partition_reservation* rsv, cf_buf_builder** bb_r);
static void ops_bg_query_job_finish(as_query_job* _job);
static void ops_bg_query_job_destroy(as_query_job* _job);
static void ops_bg_query_job_info(as_query_job* _job, cf_dyn_buf* db);

static const as_query_vtable ops_bg_query_job_vtable = {
	ops_bg_query_job_slice,
	ops_bg_query_job_finish,
	ops_bg_query_job_destroy,
	ops_bg_query_job_info
};

static void ops_bg_query_job_init(ops_bg_query_job* job);
static uint8_t* ops_bg_query_get_ops(const as_msg* m, iops_expop** expops);
static bool ops_bg_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata);
static bool ops_bg_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata);
static bool ops_bg_query_record_check(void* udata, as_storage_rd* rd);
static void ops_bg_query_tr_complete(void* udata, int result);

//----------------------------------------------------------
// ops_bg_query_job public API.
//

static int
ops_bg_query_job_start(as_transaction* tr, as_namespace* ns)
{
	if (as_transaction_is_short_query(tr)) {
		cf_warning(AS_QUERY, "ops-bg queries can't be 'short' queries");
		return AS_ERR_PARAMETER;
	}

	ops_bg_query_job* job = cf_calloc(1, sizeof(ops_bg_query_job));
	as_query_job* _job = (as_query_job*)job;

	as_query_job_init(_job, &ops_bg_query_job_vtable, tr, ns);

	if (! get_query_set(tr, ns, _job->set_name, &_job->set_id) ||
			! get_query_range(tr, ns, &_job->range) ||
			! get_query_rps(tr, &_job->rps)) {
		cf_warning(AS_QUERY, "ops-bg query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	if (_job->set_id == INVALID_SET_ID && _job->set_name[0] != '\0') {
		as_query_job_destroy(_job);
		return AS_ERR_NOT_FOUND;
	}

	if (! validate_background_query_rps(ns, &_job->rps)) {
		cf_warning(AS_QUERY, "ops-bg query job failed rps check");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	if (! find_sindex(_job)) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (_job->si != NULL && ! _job->si->readable) {
		as_query_job_destroy(_job);
		return AS_ERR_SINDEX_NOT_READABLE;
	}

	ops_bg_query_job_init(job);

	if (! get_query_filter_exp(tr, &job->origin.filter_exp)) {
		cf_warning(AS_QUERY, "ops-bg query job failed msg field processing");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	as_msg* om = &tr->msgp->msg;
	uint8_t* ops = ops_bg_query_get_ops(om, &job->origin.expops);

	if (ops == NULL) {
		cf_warning(AS_QUERY, "ops-bg query job failed get ops");
		as_query_job_destroy(_job);
		return AS_ERR_PARAMETER;
	}

	int result = as_security_check_rps(tr->from.proto_fd_h, _job->rps,
			PERM_OPS_QUERY, true, &_job->rps_udata);

	if (result != AS_OK) {
		cf_warning(AS_QUERY, "ops-bg query job failed quota %d", result);
		as_query_job_destroy(_job);
		return result;
	}

	job->n_active_tr = 0;

	uint8_t info2 = (uint8_t)(AS_MSG_INFO2_WRITE |
			(om->info2 & AS_MSG_INFO2_DURABLE_DELETE));
	uint8_t info3 = (uint8_t)(AS_MSG_INFO3_UPDATE_ONLY |
			(om->info3 & AS_MSG_INFO3_REPLACE_ONLY));

	job->origin.msgp = as_msg_create_internal(ns->name, 0, info2, info3,
			om->record_ttl, om->n_ops, ops,
			tr->msgp->proto.sz - (uint64_t)(ops - (uint8_t*)om));

	if (_job->si != NULL) {
		job->origin.check_cb = ops_bg_query_record_check;
	}

	job->origin.done_cb = ops_bg_query_tr_complete;
	job->origin.udata = (void*)job;

	cf_info(AS_QUERY, "starting ops-bg query job %lu {%s:%s:%s} rps %u from %s",
			_job->trid, ns->name, _job->set_name,
			_job->si != NULL ? _job->si_name : "<pi-query>", _job->rps,
			_job->client);

	if ((result = as_query_manager_start_job(_job)) != AS_OK) {
		cf_warning(AS_QUERY, "ops-bg query job %lu failed to start (%d)",
				_job->trid, result);
		as_query_job_destroy(_job);
		return result;
	}

	if (as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_OK)) {
		tr->from.proto_fd_h->last_used = cf_getns();
		as_end_of_transaction_ok(tr->from.proto_fd_h);
	}
	else {
		cf_warning(AS_QUERY, "ops-bg query job error sending fin");
		as_end_of_transaction_force_close(tr->from.proto_fd_h);
		// No point returning an error - it can't be reported on this socket.
	}

	tr->from.proto_fd_h = NULL;

	return AS_OK;
}

//----------------------------------------------------------
// ops_bg_query_job mandatory query_job interface.
//

static void
ops_bg_query_job_slice(as_query_job* _job, as_partition_reservation* rsv,
		cf_buf_builder** bb_r)
{
	(void)bb_r;

	if (_job->si != NULL) {
		as_sindex_tree_query(_job->si, _job->range, rsv, 0, NULL,
				ops_bg_query_job_reduce_cb, (void*)_job);
	}
	else {
		if (! as_set_index_reduce(_job->ns, rsv->tree, _job->set_id, NULL,
				ops_bg_pi_query_job_reduce_cb, (void*)_job)) {
			as_index_reduce(rsv->tree, ops_bg_pi_query_job_reduce_cb,
					(void*)_job);
		}
	}
}

static void
ops_bg_query_job_finish(as_query_job* _job)
{
	ops_bg_query_job* job = (ops_bg_query_job*)_job;

	while (job->n_active_tr != 0) {
		usleep(100);
	}

	// Subsequent activity may require an 'acquire' barrier.
	as_fence_acq();

	as_namespace* ns = _job->ns;

	switch (_job->abandoned) {
	case 0:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_ops_bg_complete :
				&ns->n_si_query_ops_bg_complete);
		break;
	case AS_ERR_QUERY_ABORT:
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_ops_bg_abort : &ns->n_si_query_ops_bg_abort);
		break;
	case AS_ERR_UNKNOWN:
	default:
		// Note - this is unreachable for now - we only abandon via abort.
		as_incr_uint64(_job->si == NULL ?
				&ns->n_pi_query_ops_bg_error : &ns->n_si_query_ops_bg_error);
		break;
	}

	cf_info(AS_QUERY, "finished ops-bg query job %lu (%d)", _job->trid,
			_job->abandoned);
}

static void
ops_bg_query_job_destroy(as_query_job* _job)
{
	ops_bg_query_job* job = (ops_bg_query_job*)_job;

	iops_origin_destroy(&job->origin);
}

static void
ops_bg_query_job_info(as_query_job* _job, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, ":job-type=");
	cf_dyn_buf_append_string(db, query_type_str(QUERY_TYPE_OPS_BG));

	cf_dyn_buf_append_string(db, ":net-io-bytes=");
	cf_dyn_buf_append_uint64(db, sizeof(cl_msg)); // original synchronous fin

	cf_dyn_buf_append_string(db, ":socket-timeout=");
	cf_dyn_buf_append_uint32(db, CF_SOCKET_TIMEOUT);

	ops_bg_query_job* job = (ops_bg_query_job*)_job;

	cf_dyn_buf_append_format(db, ":ops-active=%u", job->n_active_tr);
}

//----------------------------------------------------------
// ops_bg_query_job utilities.
//

static void
ops_bg_query_job_init(ops_bg_query_job* job)
{
	(void)job;
}

static uint8_t*
ops_bg_query_get_ops(const as_msg* m, iops_expop** p_expops)
{
	if ((m->info1 & AS_MSG_INFO1_READ) != 0) {
		cf_warning(AS_QUERY, "ops not write only");
		return NULL;
	}

	if (m->n_ops == 0) {
		cf_warning(AS_QUERY, "ops query has no ops");
		return NULL;
	}

	as_msg_op* op = NULL;
	uint8_t* first = NULL;
	uint16_t i = 0;
	bool has_expop = false;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (OP_IS_READ(op->op)) {
			cf_warning(AS_QUERY, "ops query has read op");
			return NULL;
		}

		if (first == NULL) {
			first = (uint8_t*)op;
		}

		if (op->op == AS_MSG_OP_EXP_MODIFY) {
			has_expop = true;
		}
	}

	if (has_expop) {
		iops_expop* expops = cf_malloc(sizeof(iops_expop) * m->n_ops);
		op = NULL;
		i = 0;

		while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
			if (op->op == AS_MSG_OP_EXP_MODIFY) {
				if (! as_exp_op_parse(op, &expops[i].exp, &expops[i].flags,
						true, true)) {
					cf_warning(AS_QUERY, "ops query failed exp parse");
					iops_expops_destroy(expops, i);
					return NULL;
				}
			}
			else {
				expops[i].exp = NULL;
			}
		}

		*p_expops = expops;
	}

	return first;
}

static bool
ops_bg_pi_query_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	return ops_bg_query_job_reduce_cb(r_ref, 0, udata);
}

static bool
ops_bg_query_job_reduce_cb(as_index_ref* r_ref, int64_t bval, void* udata)
{
	(void)bval;

	as_query_job* _job = (as_query_job*)udata;
	ops_bg_query_job* job = (ops_bg_query_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return false;
	}

	as_index* r = r_ref->r;

	if (! as_record_is_live(r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (_job->set_id == INVALID_SET_ID &&
			as_mrt_monitor_is_monitor_record(ns, r)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (excluded_set(r, _job->set_id)) {
		as_record_done(r_ref, ns);
		return true;
	}

	if (as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return true;
	}

	as_exp_ctx ctx = { .ns = ns, .r = r };

	if (job->origin.filter_exp != NULL &&
			as_exp_matches_metadata(job->origin.filter_exp, &ctx) ==
					AS_EXP_FALSE) {
		as_record_done(r_ref, ns);
		as_incr_uint64(&_job->n_filtered_meta);
		as_incr_uint64(&ns->n_ops_sub_write_filtered_out);
		return true;
	}

	// Save this before releasing record.
	cf_digest keyd = r->keyd;

	// Release record lock before throttling and enqueuing transaction.
	as_record_done(r_ref, ns);

	// Prefer not reaching target RPS to queue buildup and transaction timeouts.
	while (as_load_uint32(&job->n_active_tr) > MAX_ACTIVE_TRANSACTIONS) {
		usleep(1000);
	}

	throttle_sleep(_job);

	as_transaction tr;
	as_transaction_init_iops(&tr, ns, &keyd, &job->origin);

	as_incr_uint32(&job->n_active_tr);
	as_service_enqueue_internal(&tr);

	return true;
}

static bool
ops_bg_query_record_check(void* udata, as_storage_rd* rd)
{
	return record_matches_query((as_query_job*)udata, rd);
}

static void
ops_bg_query_tr_complete(void* udata, int result)
{
	as_query_job* _job = (as_query_job*)udata;
	ops_bg_query_job* job = (ops_bg_query_job*)_job;

	switch (result) {
	case AS_OK:
		as_incr_uint64(&_job->n_succeeded);
		break;
	case AS_ERR_NOT_FOUND: // record deleted after generating tr
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&_job->n_filtered_bins);
		break;
	default:
		as_incr_uint64(&_job->n_failed);
		break;
	}

	as_decr_uint32_rls(&job->n_active_tr);
}
