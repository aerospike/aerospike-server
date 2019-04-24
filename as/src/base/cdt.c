/*
 * cdt.c
 *
 * Copyright (C) 2015-2018 Aerospike, Inc.
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

#include "base/cdt.h"

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_byte_order.h"

#include "bits.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/particle.h"


//==========================================================
// Typedefs & constants.
//

#define VA_FIRST(first, ...)	first
#define VA_REST(first, ...)		__VA_ARGS__

#define CDT_OP_ENTRY(op, type, ...) [op].name = # op, [op].args = (const as_cdt_paramtype[]){VA_REST(__VA_ARGS__, 0)}, [op].count = VA_NARGS(__VA_ARGS__) - 1, [op].opt_args = VA_FIRST(__VA_ARGS__)

const cdt_op_table_entry cdt_op_table[] = {

	//============================================
	// LIST

	//--------------------------------------------
	// Modify OPs

	CDT_OP_ENTRY(AS_CDT_OP_LIST_SET_TYPE,		AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS),

	// Adds
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND,			AS_OPERATOR_CDT_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND_ITEMS,	AS_OPERATOR_CDT_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT,			AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT_ITEMS,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),

	// Removes
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP_RANGE,		AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_RANGE,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	// Modifies
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SET,			AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_TRIM,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_CLEAR,			AS_OPERATOR_CDT_MODIFY, 0),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INCREMENT,		AS_OPERATOR_CDT_MODIFY, 3, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),

	CDT_OP_ENTRY(AS_CDT_OP_LIST_SORT,			AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_FLAGS),

	//--------------------------------------------
	// Read OPs

	CDT_OP_ENTRY(AS_CDT_OP_LIST_SIZE,			AS_OPERATOR_CDT_READ, 0),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET,			AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_RANGE,		AS_OPERATOR_CDT_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	//--------------------------------------------
	// GET/REMOVE

	// GET_BYs
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_INDEX,				AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_VALUE,				AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_RANK,				AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_ALL_BY_VALUE,			AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST,		AS_OPERATOR_CDT_READ, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_INDEX_RANGE,			AS_OPERATOR_CDT_READ, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL,		AS_OPERATOR_CDT_READ, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_RANK_RANGE,			AS_OPERATOR_CDT_READ, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE,	AS_OPERATOR_CDT_READ, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	// REMOVE_BYs
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_INDEX,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_VALUE,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_RANK,				AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE,		AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST,	AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE,		AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE,		AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	//============================================
	// MAP

	//--------------------------------------------
	// Create and flags

	CDT_OP_ENTRY(AS_CDT_OP_MAP_SET_TYPE,					AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_FLAGS),

	//--------------------------------------------
	// Modify OPs

	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD,							AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD_ITEMS,					AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT,							AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT_ITEMS,					AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE,						AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE_ITEMS,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_INCREMENT,					AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_DECREMENT,					AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_CLEAR,						AS_OPERATOR_MAP_MODIFY, 0),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_INDEX,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_RANK,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST,			AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE,			AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST,		AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL,		AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE,		AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL,	AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE,		AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE,	AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE,	AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	//--------------------------------------------
	// Read OPs

	CDT_OP_ENTRY(AS_CDT_OP_MAP_SIZE,						AS_OPERATOR_MAP_READ, 0),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY,					AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_INDEX,				AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE,				AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_RANK,					AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_ALL_BY_VALUE,			AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL,			AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_INDEX_RANGE,			AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL,		AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_RANK_RANGE,			AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY_LIST,				AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE_LIST,			AS_OPERATOR_MAP_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE,	AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE,	AS_OPERATOR_MAP_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

};

static const size_t cdt_op_table_size = sizeof(cdt_op_table) / sizeof(cdt_op_table_entry);

extern const as_particle_vtable *particle_vtable[];

static __thread rollback_alloc *cdt_alloc_heap = NULL;

typedef struct index_pack24_s {
	uint32_t value:24;
} __attribute__ ((__packed__)) index_pack24;

typedef struct {
	const order_index *ordidx;
	bool error;
} index_sort_userdata;


//==========================================================
// Forward declares.
//

static bool unpack_list_value(as_unpacker *pk, cdt_payload *payload_r);
static bool unpack_map_key(as_unpacker *pk, cdt_payload *payload_r);
static bool unpack_map_value(as_unpacker *pk, cdt_payload *payload_r);

inline static void cdt_payload_pack_val(cdt_payload *value, const as_val *val);

static inline uint32_t order_index_ele_sz(uint32_t max_idx);


//==========================================================
// CDT helpers.
//

// Calculate count given index and max_index.
// Assumes index < ele_count.
static uint32_t
calc_count(uint32_t index, uint64_t in_count, uint32_t max_index)
{
	// Since we assume index < ele_count, (max - index) will never overflow.
	if (in_count >= (uint64_t)max_index - index) {
		return max_index - index;
	}

	return (uint32_t)in_count;
}

static void
calc_index_count_multi(int64_t in_index, uint64_t in_count, uint32_t ele_count,
		uint32_t *out_index, uint32_t *out_count)
{
	if (in_index >= ele_count) {
		*out_index = ele_count;
		*out_count = 0;
	}
	else if ((in_index = calc_index(in_index, ele_count)) < 0) {
		if ((uint64_t)(-in_index) < in_count) {
			uint64_t out64 = in_count + in_index;

			if (out64 > (uint64_t)ele_count) {
				out64 = ele_count;
			}

			*out_count = (uint32_t)out64;
		}
		else {
			*out_count = 0;
		}

		*out_index = 0;
	}
	else {
		*out_index = (uint32_t)in_index;
		*out_count = calc_count((uint32_t)in_index, in_count, ele_count);
	}
}

// Transform to absolute (uint32_t) index/count bounded by ele_count.
bool
calc_index_count(int64_t in_index, uint64_t in_count, uint32_t ele_count,
		uint32_t *out_index, uint32_t *out_count, bool is_multi)
{
	if (is_multi) {
		calc_index_count_multi(in_index, in_count, ele_count, out_index,
				out_count);
		return true;
	}

	if (in_index >= (int64_t)ele_count ||
			(in_index = calc_index(in_index, ele_count)) < 0) {
		return false;
	}

	*out_index = (uint32_t)in_index;
	*out_count = calc_count((uint32_t)in_index, in_count, ele_count);

	return true;
}

void
calc_rel_index_count(int64_t in_index, uint64_t in_count, uint32_t rel_index,
		int64_t *out_index, uint64_t *out_count)
{
	in_index += rel_index;

	if (in_index < 0) {
		in_index *= -1;

		if (in_count > in_index) {
			in_count -= in_index;
		}
		else {
			in_count = 0;
		}

		in_index = 0;
	}

	*out_index = in_index;
	*out_count = in_count;
}

static bool
unpack_list_value(as_unpacker *pk, cdt_payload *payload_r)
{
	payload_r->ptr = pk->buffer + pk->offset;

	int64_t sz = as_unpack_size(pk);

	if (sz <= 0) {
		cf_warning(AS_PARTICLE, "unpack_list_value() invalid msgpack");
		return false;
	}

	payload_r->sz = (uint32_t)sz;

	return true;
}

static bool
unpack_map_key(as_unpacker *pk, cdt_payload *payload_r)
{
	payload_r->ptr = pk->buffer + pk->offset;

	int64_t sz = as_unpack_size(pk);

	if (sz <= 0) {
		cf_warning(AS_PARTICLE, "unpack_map_key() invalid msgpack");
		return false;
	}

	payload_r->sz = (uint32_t)sz;

	if (as_unpack_size(pk) <= 0) { // skip value
		cf_warning(AS_PARTICLE, "unpack_map_key() invalid msgpack");
		return false;
	}

	return true;
}

static bool
unpack_map_value(as_unpacker *pk, cdt_payload *payload_r)
{
	if (as_unpack_size(pk) <= 0) { // skip key
		cf_warning(AS_PARTICLE, "unpack_map_value() invalid msgpack");
		return false;
	}

	payload_r->ptr = pk->buffer + pk->offset;

	int64_t sz = as_unpack_size(pk);

	if (sz <= 0) {
		cf_warning(AS_PARTICLE, "unpack_map_value() invalid msgpack");
		return false;
	}

	payload_r->sz = (uint32_t)sz;

	return true;
}

uint32_t
cdt_get_storage_value_sz(as_unpacker *pk)
{
	if (as_unpack_peek_is_ext(pk)) {
		as_msgpack_ext ext;
		uint32_t offset = pk->offset;

		if (as_unpack_ext(pk, &ext) != 0) {
			return 0;
		}

		if (ext.type == 0xff) {
			return 0;
		}

		return pk->offset - offset;
	}

	int64_t sz = as_unpack_size(pk);

	return sz > 0 ? (uint32_t)sz : 0;
}

uint32_t
cdt_get_msgpack_sz(as_unpacker *pk, bool check_storage)
{
	if (check_storage) {
		return cdt_get_storage_value_sz(pk);
	}

	int64_t sz = as_unpack_size(pk);

	return sz > 0 ? (uint32_t)sz : 0;
}

uint32_t
cdt_get_storage_list_sz(as_unpacker *pk, uint32_t count)
{
	uint32_t sum = 0;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t ret = cdt_get_storage_value_sz(pk);

		if (ret == 0) {
			return 0;
		}

		sum += ret;
	}

	return sum;
}

bool
cdt_check_storage_list_contents(const uint8_t *buf, uint32_t sz, uint32_t count)
{
	as_unpacker pk = {
			.buffer = buf,
			.length = sz
	};

	if (cdt_get_storage_list_sz(&pk, count) != pk.length) {
		cf_warning(AS_PARTICLE, "cdt_check_storage_list_content() invalid msgpack: count %u offset %u length %u", count, pk.offset, pk.length);
		return false;
	}

	return true;
}


//==========================================================
// cdt_result_data
//

bool
result_data_set_not_found(cdt_result_data *rd, int64_t index)
{
	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		result_data_set_list_int2x(rd, index, 0);
		break;
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		if (rd->is_multi) {
			as_bin_set_unordered_empty_list(rd->result, rd->alloc);
			break;
		}

		as_bin_set_int(rd->result, -1);
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(rd->result, 0);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
		if (rd->is_multi) {
			as_bin_set_unordered_empty_list(rd->result, rd->alloc);
		}
		break;
	case RESULT_TYPE_MAP:
		as_bin_set_empty_packed_map(rd->result, rd->alloc,
				AS_PACKED_MAP_FLAG_PRESERVE_ORDER);
		break;
	default:
		cf_warning(AS_PARTICLE, "result_data_set_not_found() invalid result type %d", rd->type);
		return false;
	}

	return true;
}

void
result_data_set_list_int2x(cdt_result_data *rd, int64_t i1, int64_t i2)
{
	define_int_list_builder(builder, rd->alloc, 2);

	cdt_container_builder_add_int64(&builder, i1);
	cdt_container_builder_add_int64(&builder, i2);
	cdt_container_builder_set_result(&builder, rd);
}

int
result_data_set_index_rank_count(cdt_result_data *rd, uint32_t start,
		uint32_t count, uint32_t ele_count)
{
	bool is_rev = false;
	bool inverted = result_data_is_inverted(rd);

	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(rd->result, inverted ? ele_count - count : count);
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_REVRANK:
		is_rev = true;
		/* no break */
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_RANK: {
		if (! rd->is_multi) {
			if (count == 0) {
				as_bin_set_int(rd->result, -1);
				break;
			}

			if (is_rev) {
				start = ele_count - start - 1;
			}

			as_bin_set_int(rd->result, start);
			break;
		}

		cdt_container_builder builder;

		if (inverted) {
			uint32_t inv_count = ele_count - count;

			cdt_int_list_builder_start(&builder, rd->alloc, inv_count);
			cdt_container_builder_add_int_range(&builder, 0, start, ele_count,
					is_rev);
			cdt_container_builder_add_int_range(&builder, start + count,
					ele_count - start - count, ele_count, is_rev);
		}
		else {
			cdt_int_list_builder_start(&builder, rd->alloc, count);
			cdt_container_builder_add_int_range(&builder, start, count,
					ele_count, is_rev);
		}

		cdt_container_builder_set_result(&builder, rd);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "result_data_set_index_rank_count() invalid return type %d", rd->type);
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

int
result_data_set_range(cdt_result_data *rd, uint32_t start, uint32_t count,
		uint32_t ele_count)
{
	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_RANK:
		return result_data_set_index_rank_count(rd, start, count, ele_count);
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		start = ele_count - start - count;
		/* no break */
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_RANK_RANGE: {
		if (result_data_is_inverted(rd)) {
			cf_warning(AS_PARTICLE, "result_data_set_range() result_type %d not supported with INVERTED flag", rd->type);
			return -AS_ERR_PARAMETER;
		}

		result_data_set_list_int2x(rd, start, count);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "result_data_set_range() invalid return type %d", rd->type);
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

// Does not respect inverted flag.
void
result_data_set_by_irc(cdt_result_data *rd,
		const order_index *irc, const order_index *idx_map,
		uint32_t total_count)
{
	bool is_rev = rd->type == RESULT_TYPE_REVINDEX ||
			rd->type == RESULT_TYPE_REVRANK;
	uint32_t items_count = irc->_.ele_count / 2;
	define_int_list_builder(builder, rd->alloc, total_count);

	for (uint32_t i = 0; i < items_count; i++) {
		uint32_t count = order_index_get(irc, (2 * i) + 1);

		if (count == 0) {
			continue;
		}

		uint32_t rank = order_index_get(irc, 2 * i);

		if (idx_map) {
			for (uint32_t j = rank; j < rank + count; j++) {
				cdt_container_builder_add_int_range(&builder,
						order_index_get(idx_map, j), 1, irc->max_idx, is_rev);
			}
		}
		else {
			cdt_container_builder_add_int_range(&builder, rank, count,
					irc->max_idx, is_rev);
		}
	}

	cdt_container_builder_set_result(&builder, rd);
}

void
result_data_set_by_itemlist_irc(cdt_result_data *rd,
		const order_index *items_ord, order_index *irc,
		uint32_t total_count)
{
	cdt_container_builder builder;
	bool inverted = result_data_is_inverted(rd);
	uint32_t items_count = items_ord->_.ele_count;
	uint32_t ele_count = irc->max_idx;
	bool is_rev = rd->type == RESULT_TYPE_REVINDEX ||
			rd->type == RESULT_TYPE_REVRANK;

	if (! inverted) {
		cdt_int_list_builder_start(&builder, rd->alloc, total_count);

		for (uint32_t i = 0; i < items_count; i++) {
			uint32_t count = order_index_get(irc, (i * 2) + 1);

			if (count == 0) {
				continue;
			}

			uint32_t rank = order_index_get(irc, i * 2);

			for (uint32_t j = 0; j < count; j++) {
				cdt_container_builder_add_int_range(&builder,
						rank + j, 1, ele_count, is_rev);
			}
		}
	}
	else {
		cdt_int_list_builder_start(&builder, rd->alloc, total_count);

		uint32_t prev = 0;

		for (uint32_t i = 0; i < items_count; i++) {
			uint32_t kl_idx = order_index_get(items_ord, i);
			uint32_t count = order_index_get(irc, (kl_idx * 2) + 1);

			if (count == 0) {
				continue;
			}

			uint32_t index = order_index_get(irc, kl_idx * 2);

			cdt_container_builder_add_int_range(&builder, prev,
					index - prev, ele_count, is_rev);
			prev = index + count;
		}

		cdt_container_builder_add_int_range(&builder, prev,
				ele_count - prev, ele_count, is_rev);
	}

	cdt_container_builder_set_result(&builder, rd);
}

// Does not respect inverted flag.
void
result_data_set_int_list_by_mask(cdt_result_data *rd, const uint64_t *mask,
		uint32_t count, uint32_t ele_count)
{
	bool is_rev = rd->type == RESULT_TYPE_REVINDEX ||
			rd->type == RESULT_TYPE_REVRANK;

	if (! rd->is_multi) {
		uint32_t idx = cdt_idx_mask_find(mask, 0, ele_count, false);

		if (is_rev) {
			idx = ele_count - idx - 1;
		}

		as_bin_set_int(rd->result, (int64_t)idx);
		return;
	}

	define_int_list_builder(builder, rd->alloc, count);
	uint32_t idx = 0;

	for (uint32_t i = 0; i < count; i++) {
		idx = cdt_idx_mask_find(mask, idx, ele_count, false);

		int64_t val = (is_rev ? ele_count - idx - 1 : idx);

		cdt_container_builder_add_int64(&builder, val);
		idx++;
	}

	cdt_container_builder_set_result(&builder, rd);
}


//==========================================================
// as_bin functions.
//

void
as_bin_set_int(as_bin *b, int64_t value)
{
	b->particle = (as_particle *)value;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_INTEGER);
}

void
as_bin_set_double(as_bin *b, double value)
{
	*((double *)(&b->particle)) = value;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_FLOAT);
}


//==========================================================
//cdt_calc_delta
//

bool
cdt_calc_delta_init(cdt_calc_delta *cdv, const cdt_payload *delta_value,
		bool is_decrement)
{
	if (delta_value && delta_value->ptr) {
		as_unpacker pk_delta_value = {
				.buffer = delta_value->ptr,
				.length = delta_value->sz
		};

		cdv->type = as_unpack_peek_type(&pk_delta_value);

		if (cdv->type == AS_INTEGER) {
			if (as_unpack_int64(&pk_delta_value, &cdv->incr_int) != 0) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_init() invalid packed delta value");
				return false;
			}
		}
		else if (cdv->type == AS_DOUBLE) {
			if (as_unpack_double(&pk_delta_value, &cdv->incr_double) != 0) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_init() invalid packed delta value");
				return false;
			}
		}
		else {
			cf_warning(AS_PARTICLE, "cdt_delta_value_init() delta is not int/double");
			return false;
		}
	}
	else {
		cdv->type = AS_UNDEF;
		cdv->incr_int = 1;
		cdv->incr_double = 1;
	}

	if (is_decrement) {
		cdv->incr_int = -cdv->incr_int;
		cdv->incr_double = -cdv->incr_double;
	}

	cdv->value_int = 0;
	cdv->value_double = 0;

	return true;
}

bool
cdt_calc_delta_add(cdt_calc_delta *cdv, as_unpacker *pk_value)
{
	if (pk_value) {
		as_val_t packed_value_type = as_unpack_peek_type(pk_value);

		if (packed_value_type == AS_INTEGER) {
			if (as_unpack_int64(pk_value, &cdv->value_int) != 0) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_add() invalid packed int");
				return false;
			}

			if (cdv->type == AS_DOUBLE) {
				cdv->value_int += (int64_t)cdv->incr_double;
			}
			else {
				cdv->value_int += cdv->incr_int;
			}
		}
		else if (packed_value_type == AS_DOUBLE) {
			if (as_unpack_double(pk_value, &cdv->value_double) != 0) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_add() invalid packed double");
				return false;
			}

			if (cdv->type == AS_DOUBLE) {
				cdv->value_double += cdv->incr_double;
			}
			else {
				cdv->value_double += (double)cdv->incr_int;
			}
		}
		else {
			cf_warning(AS_PARTICLE, "cdt_delta_value_add() only valid for int/double");
			return false;
		}

		cdv->type = packed_value_type;
	}
	else if (cdv->type == AS_DOUBLE) {
		cdv->value_double += cdv->incr_double;
	}
	else {
		cdv->type = AS_INTEGER; // default to AS_INTEGER if UNDEF
		cdv->value_int += cdv->incr_int;
	}

	return true;
}

void
cdt_calc_delta_pack_and_result(cdt_calc_delta *cdv, cdt_payload *value,
		as_bin *result)
{
	if (cdv->type == AS_DOUBLE) {
		cdt_payload_pack_double(value, cdv->value_double);
		as_bin_set_double(result, cdv->value_double);
	}
	else {
		cdt_payload_pack_int(value, cdv->value_int);
		as_bin_set_int(result, cdv->value_int);
	}
}


//==========================================================
// cdt_payload functions.
//

bool
cdt_payload_is_int(const cdt_payload *payload)
{
	return as_unpack_buf_peek_type(payload->ptr, payload->sz) == AS_INTEGER;
}

int64_t
cdt_payload_get_int64(const cdt_payload *payload)
{
	int64_t ret = 0;
	as_unpacker pk = {
			.buffer = payload->ptr,
			.offset = 0,
			.length = payload->sz
	};

	as_unpack_int64(&pk, &ret);

	return ret;
}

inline static void
cdt_payload_pack_val(cdt_payload *value, const as_val *val)
{
	as_serializer ser;
	as_msgpack_init(&ser);

	value->sz = as_serializer_serialize_presized(&ser, val,
			(uint8_t *)value->ptr);

	as_serializer_destroy(&ser);
}

void
cdt_payload_pack_int(cdt_payload *packed, int64_t value)
{
	as_integer val;
	as_integer_init(&val, value);

	cdt_payload_pack_val(packed, (as_val *)&val);
}

void
cdt_payload_pack_double(cdt_payload *packed, double value)
{
	as_double val;
	as_double_init(&val, value);

	return cdt_payload_pack_val(packed, (as_val *)&val);
}


//==========================================================
// cdt_container_builder functions.
//

void
cdt_container_builder_add(cdt_container_builder *builder, const uint8_t *buf,
		uint32_t sz)
{
	memcpy(builder->write_ptr, buf, sz);
	builder->write_ptr += sz;
	*builder->sz += sz;
	builder->ele_count++;
}

void
cdt_container_builder_add_n(cdt_container_builder *builder, const uint8_t *buf,
		uint32_t count, uint32_t sz)
{
	if (buf) {
		memcpy(builder->write_ptr, buf, sz);
	}

	builder->write_ptr += sz;
	*builder->sz += sz;
	builder->ele_count += count;
}

void
cdt_container_builder_add_int64(cdt_container_builder *builder, int64_t value)
{
	as_integer val64;

	as_packer pk = {
			.buffer = builder->write_ptr,
			.capacity = INT_MAX
	};

	as_integer_init(&val64, value);
	as_pack_val(&pk, (const as_val *)&val64);
	builder->write_ptr += pk.offset;
	*builder->sz += (uint32_t)pk.offset;
	builder->ele_count++;
}

void
cdt_container_builder_add_int_range(cdt_container_builder *builder,
		uint32_t start, uint32_t count, uint32_t ele_count, bool is_rev)
{
	if (is_rev) {
		start = ele_count - start - count;
	}

	for (uint32_t i = 0; i < count; i++) {
		cdt_container_builder_add_int64(builder, (int64_t)(start + i));
	}
}

void
cdt_container_builder_set_result(cdt_container_builder *builder,
		cdt_result_data *result)
{
	result->result->particle = builder->particle;
	as_bin_state_set_from_type(result->result, (as_particle_type)((uint8_t *)builder->particle)[0]);
}


//==========================================================
// cdt_process_state functions.
//

bool
cdt_process_state_init(cdt_process_state *cdt_state, const as_msg_op *op)
{
	const uint8_t *data = op->name + op->name_sz;
	uint32_t sz = op->op_sz - OP_FIXED_SZ - op->name_sz;

	if (data[0] == 0) { // TODO - deprecate this in "6 months"
		if (sz < sizeof(uint16_t)) {
			cf_warning(AS_PARTICLE, "cdt_parse_state_init() as_msg_op data too small to be valid: size=%u", sz);
			return false;
		}

		const uint16_t *type_ptr = (const uint16_t *)data;

		cdt_state->type = cf_swap_from_be16(*type_ptr);
		cdt_state->pk.buffer = data + sizeof(uint16_t);
		cdt_state->pk.length = sz - sizeof(uint16_t);
		cdt_state->pk.offset = 0;

		int64_t ele_count = (cdt_state->pk.length == 0) ?
				0 : as_unpack_list_header_element_count(&cdt_state->pk);

		if (ele_count < 0) {
			cf_warning(AS_PARTICLE, "cdt_parse_state_init() unpack list header failed: size=%u type=%u ele_count=%ld", sz, cdt_state->type, ele_count);
			return false;
		}

		cdt_state->ele_count = (uint32_t)ele_count;

		return true;
	}

	cdt_state->pk.buffer = data;
	cdt_state->pk.length = sz;
	cdt_state->pk.offset = 0;

	int64_t ele_count = as_unpack_list_header_element_count(&cdt_state->pk);
	uint64_t type64;

	if (ele_count < 1 || as_unpack_uint64(&cdt_state->pk, &type64) != 0) {
		cf_warning(AS_PARTICLE, "cdt_parse_state_init() unpack parameters failed: size=%u ele_count=%ld", sz, ele_count);
		return false;
	}

	cdt_state->type = (as_cdt_optype)type64;
	cdt_state->ele_count = (uint32_t)ele_count - 1;

	return true;
}

bool
cdt_process_state_get_params(cdt_process_state *state, size_t n, ...)
{
	as_cdt_optype op = state->type;

	if (op >= cdt_op_table_size) {
		return false;
	}

	const cdt_op_table_entry *entry = &cdt_op_table[op];
	uint32_t required_count = entry->count - entry->opt_args;

	cf_assert(n >= (size_t)required_count, AS_PARTICLE, "cdt_process_state_get_params() called with %zu params, require at least %u - %u = %u params", n, entry->count, entry->opt_args, required_count);

	if (n == 0 || entry->args[0] == 0) {
		return true;
	}

	if (state->ele_count < required_count) {
		cf_warning(AS_PARTICLE, "cdt_process_state_get_params() count mismatch: got %u from client < expected %u", state->ele_count, required_count);
		return false;
	}

	if (state->ele_count > (uint32_t)entry->count) {
		cf_warning(AS_PARTICLE, "cdt_process_state_get_params() count mismatch: got %u from client > expected %u", state->ele_count, entry->count);
		return false;
	}

	va_list vl;
	va_start(vl, n);

	for (uint32_t i = 0; i < state->ele_count; i++) {
		switch (entry->args[i]) {
		case AS_CDT_PARAM_PAYLOAD: {
			cdt_payload *arg = va_arg(vl, cdt_payload *);

			arg->ptr = state->pk.buffer + state->pk.offset;

			int64_t sz = as_unpack_size(&state->pk);

			if (sz <= 0) {
				va_end(vl);
				return false;
			}

			arg->sz = (uint32_t)sz;

			break;
		}
		case AS_CDT_PARAM_FLAGS:
		case AS_CDT_PARAM_COUNT: {
			uint64_t *arg = va_arg(vl, uint64_t *);

			if (as_unpack_uint64(&state->pk, arg) != 0) {
				va_end(vl);
				return false;
			}

			break;
		}
		case AS_CDT_PARAM_INDEX: {
			int64_t *arg = va_arg(vl, int64_t *);

			if (as_unpack_int64(&state->pk, arg) != 0) {
				va_end(vl);
				return false;
			}

			break;
		}
		default:
			va_end(vl);
			return false;
		}
	}

	va_end(vl);

	return true;
}

const char *
cdt_process_state_get_op_name(const cdt_process_state *state)
{
	as_cdt_optype op = state->type;

	if (op >= cdt_op_table_size) {
		return NULL;
	}

	const cdt_op_table_entry *entry = &cdt_op_table[op];

	return entry->name;
}


//==========================================================
// rollback_alloc functions.
//

void
rollback_alloc_push(rollback_alloc *packed_alloc, void *ptr)
{
	if (packed_alloc->malloc_list_sz >= packed_alloc->malloc_list_cap) {
		cf_crash(AS_PARTICLE, "rollback_alloc_push() need to make rollback list larger: cap=%zu", packed_alloc->malloc_list_cap);
	}

	packed_alloc->malloc_list[packed_alloc->malloc_list_sz++] = ptr;
}

uint8_t *
rollback_alloc_reserve(rollback_alloc *alloc_buf, size_t size)
{
	cf_assert(alloc_buf, AS_PARTICLE, "alloc_buf NULL");

	uint8_t *ptr;

	if (alloc_buf->ll_buf) {
		cf_ll_buf_reserve(alloc_buf->ll_buf, size, &ptr);
	}
	else {
		ptr = alloc_buf->malloc_ns ? cf_malloc_ns(size) : cf_malloc(size);
		rollback_alloc_push(alloc_buf, ptr);
	}

	return ptr;
}

void
rollback_alloc_rollback(rollback_alloc *alloc_buf)
{
	if (alloc_buf->ll_buf) {
		return;
	}

	for (size_t i = 0; i < alloc_buf->malloc_list_sz; i++) {
		cf_free(alloc_buf->malloc_list[i]);
	}

	alloc_buf->malloc_list_sz = 0;
}

bool
rollback_alloc_from_msgpack(rollback_alloc *alloc_buf, as_bin *b,
		const cdt_payload *seg)
{
	// We assume the bin is empty.

	as_particle_type type = as_particle_type_from_msgpack(seg->ptr, seg->sz);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return false;
	}

	if (type == AS_PARTICLE_TYPE_NULL) {
		return true;
	}

	uint32_t sz =
			particle_vtable[type]->size_from_msgpack_fn(seg->ptr, seg->sz);

	if (sz != 0) {
		b->particle = (as_particle *)rollback_alloc_reserve(alloc_buf, sz);

		if (! b->particle) {
			return false;
		}
	}

	particle_vtable[type]->from_msgpack_fn(seg->ptr, seg->sz, &b->particle);

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	return true;
}


//==========================================================
// as_bin_cdt_packed functions.
//

int
as_bin_cdt_packed_modify(as_bin *b, const as_msg_op *op, as_bin *result,
		cf_ll_buf *particles_llb)
{
	cdt_process_state state;

	if (! cdt_process_state_init(&state, op)) {
		return -AS_ERR_PARAMETER;
	}

	cdt_modify_data udata = {
			.b = b,
			.result = result,
			.alloc_buf = particles_llb,
			.ret_code = AS_OK,
	};

	bool success;
	define_rollback_alloc(alloc_idx, NULL, 4, false); // for temp indexes

	cdt_idx_set_alloc(alloc_idx);

	if (IS_CDT_LIST_OP(state.type)) {
		success = cdt_process_state_packed_list_modify_optype(&state, &udata);
	}
	else {
		success = cdt_process_state_packed_map_modify_optype(&state, &udata);
	}

	cdt_idx_clear();

	if (! success) {
		as_bin_set_empty(b);
		as_bin_set_empty(result);
	}

	return udata.ret_code;
}

int
as_bin_cdt_packed_read(const as_bin *b, const as_msg_op *op, as_bin *result)
{
	cdt_process_state state;

	if (! cdt_process_state_init(&state, op)) {
		return -AS_ERR_PARAMETER;
	}

	cdt_read_data udata = {
			.b = b,
			.result = result,
			.ret_code = AS_OK,
	};

	bool success;
	define_rollback_alloc(alloc_idx, NULL, 4, false); // for temp indexes

	cdt_idx_set_alloc(alloc_idx);

	if (IS_CDT_LIST_OP(state.type)) {
		success = cdt_process_state_packed_list_read_optype(&state, &udata);
	}
	else {
		success = cdt_process_state_packed_map_read_optype(&state, &udata);
	}

	cdt_idx_clear();

	if (! success) {
		as_bin_set_empty(result);
	}

	return udata.ret_code;
}


//==========================================================
// msgpacked_index
//

void
msgpacked_index_set(msgpacked_index *idxs, uint32_t index, uint32_t value)
{
	switch (idxs->ele_sz) {
	case 1:
		idxs->ptr[index] = (uint8_t)value;
		break;
	case 2:
		((uint16_t *)idxs->ptr)[index] = (uint16_t)value;
		break;
	case 3:
		((index_pack24 *)idxs->ptr)[index].value = value;
		break;
	default:
		((uint32_t *)idxs->ptr)[index] = value;
		break;
	}
}

void
msgpacked_index_incr(msgpacked_index *idxs, uint32_t index)
{
	switch (idxs->ele_sz) {
	case 1:
		idxs->ptr[index]++;
		break;
	case 2:
		((uint16_t *)idxs->ptr)[index]++;
		break;
	case 3:
		((index_pack24 *)idxs->ptr)[index].value++;
		break;
	default:
		((uint32_t *)idxs->ptr)[index]++;
		break;
	}
}

void
msgpacked_index_set_ptr(msgpacked_index *idxs, uint8_t *ptr)
{
	idxs->ptr = ptr;
}

// Get pointer at index.
void *
msgpacked_index_get_mem(const msgpacked_index *idxs, uint32_t index)
{
	return (void *)(idxs->ptr + idxs->ele_sz * index);
}

uint32_t
msgpacked_index_size(const msgpacked_index *idxs)
{
	return idxs->ele_sz * idxs->ele_count;
}

uint32_t
msgpacked_index_ptr2value(const msgpacked_index *idxs, const void *ptr)
{
	switch (idxs->ele_sz) {
	case 1:
		return *((const uint8_t *)ptr);
	case 2:
		return *((const uint16_t *)ptr);
	case 3:
		return ((const index_pack24 *)ptr)->value;
	default:
		break;
	}

	return *((const uint32_t *)ptr);
}

uint32_t
msgpacked_index_get(const msgpacked_index *idxs, uint32_t index)
{
	switch (idxs->ele_sz) {
	case 1:
		return idxs->ptr[index];
	case 2:
		return ((const uint16_t *)idxs->ptr)[index];
	case 3:
		return ((const index_pack24 *)idxs->ptr)[index].value;
	default:
		break;
	}

	return ((const uint32_t *)idxs->ptr)[index];
}

// Find find_index in a list of sorted_indexes.
// *where will be the location where find_index is (if exist) or is suppose to
// be (if not exist).
// Return true if find_index is in sorted_indexes.
bool
msgpacked_index_find_index_sorted(const msgpacked_index *sorted_indexes,
		uint32_t find_index, uint32_t count, uint32_t *where)
{
	if (count == 0) {
		*where = 0;
		return false;
	}

	uint32_t upper = count;
	uint32_t lower = 0;
	uint32_t i = count / 2;

	while (true) {
		uint32_t index = msgpacked_index_get(sorted_indexes, i);

		if (find_index == index) {
			*where = i;
			return true;
		}

		if (find_index > index) {
			if (i >= upper - 1) {
				*where = i + 1;
				break;
			}

			lower = i + 1;
			i += upper;
			i /= 2;
		}
		else {
			if (i <= lower) {
				*where = i;
				break;
			}

			upper = i;
			i += lower;
			i /= 2;
		}
	}

	return false;
}

void
msgpacked_index_print(const msgpacked_index *idxs, const char *name)
{
	size_t ele_count = idxs->ele_count;
	char buf[1024];
	char *ptr = buf;

	if (idxs->ptr) {
		for (size_t i = 0; i < ele_count; i++) {
			if (buf + 1024 - ptr < 12) {
				break;
			}

			ptr += sprintf(ptr, "%u, ", msgpacked_index_get(idxs, i));
		}

		if (ele_count > 0) {
			ptr -= 2;
		}

		*ptr = '\0';
	}
	else {
		strcpy(buf, "(null)");
	}

	cf_warning(AS_PARTICLE, "%s: index[%zu]={%s}", name, ele_count, buf);
}


//==========================================================
// offset_index
//

void
offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr,
		uint32_t ele_count, const uint8_t *contents, uint32_t content_sz)
{
	offidx->_.ele_count = ele_count;
	offidx->content_sz = content_sz;

	if (content_sz < (1 << 8)) {
		offidx->_.ele_sz = 1;
	}
	else if (content_sz < (1 << 16)) {
		offidx->_.ele_sz = 2;
	}
	else if (content_sz < (1 << 24)) {
		offidx->_.ele_sz = 3;
	}
	else {
		offidx->_.ele_sz = 4;
	}

	offidx->_.ptr = idx_mem_ptr;
	offidx->contents = contents;
	offidx->is_partial = false;
}

void
offset_index_set(offset_index *offidx, uint32_t index, uint32_t value)
{
	if (index == 0 || index == offidx->_.ele_count) {
		return;
	}

	msgpacked_index_set((msgpacked_index *)offidx, index, value);
}

bool
offset_index_set_next(offset_index *offidx, uint32_t index, uint32_t value)
{
	if (index >= offidx->_.ele_count) {
		return true;
	}

	uint32_t filled = offset_index_get_filled(offidx);

	if (index == filled) {
		offset_index_set(offidx, index, value);
		offset_index_set_filled(offidx, filled + 1);

		return true;
	}

	if (index < filled) {
		return value == offset_index_get_const(offidx, index);
	}

	return false;
}

void
offset_index_set_filled(offset_index *offidx, uint32_t ele_filled)
{
	if (offidx->_.ele_count == 0) {
		return;
	}

	cf_assert(ele_filled <= offidx->_.ele_count, AS_PARTICLE, "ele_filled(%u) > ele_count(%u)", ele_filled, offidx->_.ele_count);
	msgpacked_index_set((msgpacked_index *)offidx, 0, ele_filled);
}

void
offset_index_set_ptr(offset_index *offidx, uint8_t *idx_mem,
		const uint8_t *packed_mem)
{
	msgpacked_index_set_ptr((msgpacked_index *)offidx, idx_mem);
	offidx->contents = packed_mem;
}

void
offset_index_copy(offset_index *dest, const offset_index *src, uint32_t d_start,
		uint32_t s_start, uint32_t count, int delta)
{
	cf_assert(d_start + count <= dest->_.ele_count, AS_PARTICLE, "d_start(%u) + count(%u) > dest.ele_count(%u)", d_start, count, dest->_.ele_count);
	cf_assert(s_start + count <= src->_.ele_count, AS_PARTICLE, "s_start(%u) + count(%u) > src.ele_count(%u)", s_start, count, src->_.ele_count);

	if (dest->_.ele_sz == src->_.ele_sz && delta == 0) {
		memcpy(offset_index_get_mem(dest, d_start),
				offset_index_get_mem(src, s_start),
				dest->_.ele_sz * count);
	}
	else {
		for (size_t i = 0; i < count; i++) {
			uint32_t value = offset_index_get_const(src, s_start + i);

			value += delta;
			offset_index_set(dest, d_start + i, value);
		}
	}
}

void
offset_index_append_size(offset_index *offidx, uint32_t delta)
{
	uint32_t filled = offset_index_get_filled(offidx);

	if (filled == offidx->_.ele_count) {
		return;
	}

	uint32_t last = offset_index_get_const(offidx, filled - 1);

	offset_index_set_filled(offidx, filled + 1);
	offset_index_set(offidx, filled, last + delta);
}

bool
offset_index_find_items(offset_index *full_offidx,
		cdt_find_items_idxs_type find_type, as_unpacker *items_pk,
		order_index *items_ordidx_r, bool inverted, uint64_t *rm_mask,
		uint32_t *rm_count_r, order_index *rm_ranks_r)
{
	bool (*unpack_fn)(as_unpacker *pk, cdt_payload *payload_r);
	uint32_t items_count = items_ordidx_r->_.ele_count;
	define_offset_index(items_offidx, items_pk->buffer + items_pk->offset,
			items_pk->length - items_pk->offset, items_count);

	switch (find_type) {
	case CDT_FIND_ITEMS_IDXS_FOR_LIST_VALUE:
		unpack_fn = unpack_list_value;
		break;
	case CDT_FIND_ITEMS_IDXS_FOR_MAP_KEY:
		unpack_fn = unpack_map_key;
		break;
	case CDT_FIND_ITEMS_IDXS_FOR_MAP_VALUE:
		unpack_fn = unpack_map_value;
		break;
	default:
		cf_crash(AS_PARTICLE, "bad input");
		return false; // dummy return to quash warning
	}

	if (! list_full_offset_index_fill_all(&items_offidx, false)) {
		cf_warning(AS_PARTICLE, "offset_index_find_items() invalid parameter key list");
		return false;
	}

	bool success = list_order_index_sort(items_ordidx_r, &items_offidx,
			AS_CDT_SORT_ASCENDING);

	cf_assert(success, AS_PARTICLE, "offset_index_find_items() sort failed after index filled");

	uint32_t rm_count = 0;

	as_unpacker pk = {
			.buffer = full_offidx->contents,
			.length = full_offidx->content_sz
	};

	if (rm_ranks_r) {
		order_index_clear(rm_ranks_r);
	}

	for (uint32_t i = 0; i < full_offidx->_.ele_count; i++) {
		cdt_payload value;

		if (! unpack_fn(&pk, &value)) {
			cf_warning(AS_PARTICLE, "offset_index_find_items() invalid msgpack in unpack_fn()");
			return false;
		}

		if (! offset_index_set_next(full_offidx, i + 1, (uint32_t)pk.offset)) {
			cf_warning(AS_PARTICLE, "offset_index_find_items() invalid msgpack in offset_index_set_next()");
			return false;
		}

		order_index_find find = {
				.count = items_count,
				.target = items_count + (rm_ranks_r != NULL ? 0 : 1)
		};

		if (! order_index_find_rank_by_value(items_ordidx_r, &value,
				&items_offidx, &find)) {
			cf_warning(AS_PARTICLE, "offset_index_find_items() invalid items list");
			return false;
		}

		if (rm_ranks_r) {
			uint32_t vl_rank = find.result;

			if (find.found) {
				uint32_t idx = order_index_get(items_ordidx_r, find.result);

				order_index_incr(rm_ranks_r, (idx * 2) + 1);
				vl_rank++;
			}

			if (vl_rank != items_count) {
				uint32_t idx = order_index_get(items_ordidx_r, vl_rank);

				order_index_incr(rm_ranks_r, idx * 2);
			}
		}

		if (! inverted) {
			if (find.found) {
				cdt_idx_mask_set(rm_mask, i);
				rm_count++;
			}
		}
		else if (! find.found) {
			cdt_idx_mask_set(rm_mask, i);
			rm_count++;
		}
	}

	if (rm_ranks_r) {
		for (uint32_t i = 1; i < items_count; i++) {
			uint32_t idx0 = order_index_get(items_ordidx_r, i - 1);
			uint32_t idx1 = order_index_get(items_ordidx_r, i);
			uint32_t rank0 = order_index_get(rm_ranks_r, idx0 * 2);
			uint32_t rank1 = order_index_get(rm_ranks_r, idx1 * 2);

			order_index_set(rm_ranks_r, idx1 * 2, rank0 + rank1);
		}
	}

	*rm_count_r = rm_count;

	return true;
}

void *
offset_index_get_mem(const offset_index *offidx, uint32_t index)
{
	return msgpacked_index_get_mem((msgpacked_index *)offidx, index);
}

uint32_t
offset_index_size(const offset_index *offidx)
{
	return msgpacked_index_size((const msgpacked_index *)offidx);
}

bool
offset_index_is_null(const offset_index *offidx)
{
	return offidx->_.ptr == NULL;
}

bool
offset_index_is_valid(const offset_index *offidx)
{
	return offidx->_.ptr != NULL;
}

bool
offset_index_is_full(const offset_index *offidx)
{
	if (offset_index_is_null(offidx)) {
		return false;
	}

	if (offidx->_.ele_count == 0) {
		return true;
	}

	uint32_t filled = offset_index_get_filled(offidx);

	cf_assert(filled <= offidx->_.ele_count, AS_PARTICLE, "filled(%u) > ele_count(%u)", filled, offidx->_.ele_count);

	if (filled == offidx->_.ele_count) {
		return true;
	}

	return false;
}

uint32_t
offset_index_get_const(const offset_index *offidx, uint32_t idx)
{
	if (idx == 0) {
		return 0;
	}

	if (idx == offidx->_.ele_count) {
		return offidx->content_sz;
	}

	if (idx >= offset_index_get_filled(offidx)) {
		offset_index_print(offidx, "offset_index_get_const() offidx");
		print_packed(offidx->contents, offidx->content_sz, "offset_index_get_const() offidx->ele_start");
		cf_crash(AS_PARTICLE, "offset_index_get_const() idx=%u >= filled=%u ele_count=%u", idx, offset_index_get_filled(offidx), offidx->_.ele_count);
	}

	return msgpacked_index_get((const msgpacked_index *)offidx, idx);
}

uint32_t
offset_index_get_delta_const(const offset_index *offidx, uint32_t index)
{
	uint32_t offset = offset_index_get_const(offidx, index);

	if (index == offidx->_.ele_count - 1) {
		return offidx->content_sz - offset;
	}

	return offset_index_get_const(offidx, index + 1) - offset;
}

uint32_t
offset_index_get_filled(const offset_index *offidx)
{
	if (offidx->_.ele_count == 0) {
		return 1;
	}

	return msgpacked_index_get((const msgpacked_index *)offidx, 0);
}

uint32_t
offset_index_vla_sz(const offset_index *offidx)
{
	if (offset_index_is_valid(offidx)) {
		return 0;
	}

	uint32_t sz = offset_index_size(offidx);

	return cdt_vla_sz(sz);
}

void
offset_index_alloc_temp(offset_index *offidx, uint8_t *mem_temp)
{
	if (! offset_index_is_valid(offidx)) {
		uint32_t sz = offset_index_size(offidx);

		if (sz > CDT_MAX_STACK_OBJ_SZ) {
			offidx->_.ptr = cdt_idx_alloc(sz);
		}
		else {
			offidx->_.ptr = mem_temp;
		}

		offset_index_set_filled(offidx, 1);
	}
}

void
offset_index_print(const offset_index *offidx, const char *name)
{
	if (! name) {
		name = "offset";
	}

	msgpacked_index_print((msgpacked_index *)offidx, name);
}

void
offset_index_delta_print(const offset_index *offidx, const char *name)
{
	size_t ele_count = offidx->_.ele_count;
	char buf[1024];
	char *ptr = buf;

	if (offidx->_.ptr) {
		for (size_t i = 0; i < ele_count; i++) {
			if (buf + 1024 - ptr < 12) {
				break;
			}

			ptr += sprintf(ptr, "%u, ", offset_index_get_delta_const(offidx, i));
		}

		if (ele_count > 0) {
			ptr -= 2;
		}

		*ptr = '\0';
	}
	else {
		strcpy(buf, "(null)");
	}

	cf_warning(AS_PARTICLE, "%s: delta_off[%zu]={%s} %u", name, ele_count, buf, offidx->content_sz);
}


//==========================================================
// order_index
//

static inline uint32_t
order_index_ele_sz(uint32_t max_idx)
{
	// Allow for values [0, ele_count] for ele_count to indicate invalid values.
	if (max_idx < (1 << 8)) {
		return 1;
	}
	else if (max_idx < (1 << 16)) {
		return 2;
	}
	else if (max_idx < (1 << 24)) {
		return 3;
	}

	return 4;
}

void
order_index_init(order_index *ordidx, uint8_t *ptr, uint32_t ele_count)
{
	ordidx->_.ele_count = ele_count;
	ordidx->_.ele_sz = order_index_ele_sz(ele_count);
	ordidx->_.ptr = ptr;
	ordidx->max_idx = ele_count;
}

void
order_index_init2(order_index *ordidx, uint8_t *ptr, uint32_t max_idx,
		uint32_t ele_count)
{
	ordidx->_.ele_count = ele_count;
	ordidx->_.ele_sz = order_index_ele_sz(max_idx);
	ordidx->_.ptr = ptr;
	ordidx->max_idx = max_idx;
}

void
order_index_init2_temp(order_index *ordidx, uint8_t *mem_temp, uint32_t max_idx,
		uint32_t ele_count)
{
	order_index_init2(ordidx, mem_temp, max_idx, ele_count);
	uint32_t sz = order_index_size(ordidx);

	if (sz > CDT_MAX_STACK_OBJ_SZ) {
		order_index_set_ptr(ordidx, cdt_idx_alloc(sz));
	}
}

void
order_index_init_ref(order_index *dst, const order_index *src, uint32_t start,
		uint32_t count)
{
	order_index_init2(dst, order_index_get_mem(src, start), src->max_idx,
			count);
}

void
order_index_set(order_index *ordidx, uint32_t idx, uint32_t value)
{
	msgpacked_index_set((msgpacked_index *)ordidx, idx, value);
}

void
order_index_set_ptr(order_index *ordidx, uint8_t *ptr)
{
	msgpacked_index_set_ptr((msgpacked_index *)ordidx, ptr);
}

void
order_index_incr(order_index *ordidx, uint32_t idx)
{
	msgpacked_index_incr((msgpacked_index *)ordidx, idx);
}

void
order_index_clear(order_index *ordidx)
{
	memset(ordidx->_.ptr, 0, order_index_size(ordidx));
}

bool
order_index_sorted_mark_dup_eles(order_index *ordidx,
		const offset_index *full_offidx, uint32_t *count_r, uint32_t *sz_r)
{
	cf_assert(count_r, AS_PARTICLE, "count_r NULL");
	cf_assert(sz_r, AS_PARTICLE, "sz_r NULL");

	as_unpacker pk = {
			.buffer = full_offidx->contents,
			.length = full_offidx->content_sz
	};

	as_unpacker prev = pk;
	uint32_t prev_idx = order_index_get(ordidx, 0);
	uint32_t ele_count = full_offidx->_.ele_count;

	prev.offset = offset_index_get_const(full_offidx, prev_idx);
	*count_r = 0;
	*sz_r = 0;

	for (uint32_t i = 1; i < ele_count; i++) {
		uint32_t idx = order_index_get(ordidx, i);
		uint32_t off = offset_index_get_const(full_offidx, idx);

		pk.offset = off;

		msgpack_compare_t cmp = as_unpack_compare(&prev, &pk);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			(*sz_r) += pk.offset - off;
			(*count_r)++;
			order_index_set(ordidx, i, ele_count);
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			// no-op
		}
		else {
			return false;
		}

		prev.offset = off;
	}

	return true;
}

uint32_t
order_index_size(const order_index *ordidx)
{
	return msgpacked_index_size((const msgpacked_index *)ordidx);
}

bool
order_index_is_null(const order_index *ordidx)
{
	return ordidx->_.ptr == NULL;
}

bool
order_index_is_valid(const order_index *ordidx)
{
	return ordidx->_.ptr != NULL;
}

bool
order_index_is_filled(const order_index *ordidx)
{
	if (! order_index_is_valid(ordidx)) {
		return false;
	}

	if (ordidx->_.ele_count > 0 &&
			order_index_get(ordidx, 0) >= ordidx->_.ele_count) {
		return false;
	}

	return true;
}

// Get pointer at index.
void *
order_index_get_mem(const order_index *ordidx, uint32_t index)
{
	return msgpacked_index_get_mem((const msgpacked_index *)ordidx, index);
}

uint32_t
order_index_ptr2value(const order_index *ordidx, const void *ptr)
{
	return msgpacked_index_ptr2value((const msgpacked_index *)ordidx, ptr);
}

uint32_t
order_index_get(const order_index *ordidx, uint32_t index)
{
	return msgpacked_index_get((const msgpacked_index *)ordidx, index);
}

// Find (closest) rank given value.
// Find closest rank for find->idx.
//  target == 0 means find first instance of value.
//  target == ele_count means find last instance of value.
//  target > ele_count means don't check idx.
// Return true success.
bool
order_index_find_rank_by_value(const order_index *ordidx,
		const cdt_payload *value, const offset_index *full_offidx,
		order_index_find *find)
{
	uint32_t ele_count = full_offidx->_.ele_count;

	find->found = false;

	if (ele_count == 0 || find->count == 0) {
		find->result = ele_count;
		return true;
	}

	uint32_t lower = find->start;
	uint32_t upper = find->start + find->count;
	uint32_t rank = find->start + find->count / 2;

	as_unpacker pk_value = {
			.buffer = value->ptr,
			.length = value->sz
	};

	as_unpacker pk_buf = {
			.buffer = full_offidx->contents,
			.length = full_offidx->content_sz
	};

	while (true) {
		uint32_t idx = ordidx ? order_index_get(ordidx, rank) : rank;

		pk_value.offset = 0; // reset
		pk_buf.offset = offset_index_get_const(full_offidx, idx);

		msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk_buf);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			find->found = true;

			if (find->target > ele_count) { // means don't check
				break;
			}

			if (find->target < idx) {
				cmp = MSGPACK_COMPARE_LESS;
			}
			else if (find->target > idx) {
				if (rank == upper - 1) {
					break;
				}

				cmp = MSGPACK_COMPARE_GREATER;
			}
			else {
				break;
			}
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (rank >= upper - 1) {
				rank++;
				break;
			}

			lower = rank + (find->found ? 0 : 1);
			rank += upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (rank == lower) {
				break;
			}

			upper = rank;
			rank += lower;
			rank /= 2;
		}
		else {
			return false;
		}
	}

	find->result = rank;

	return true;
}

uint32_t
order_index_get_ele_size(const order_index *ordidx, uint32_t count,
		const offset_index *full_offidx)
{
	uint32_t sz = 0;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		if (idx == ordidx->max_idx) {
			continue;
		}

		sz += offset_index_get_delta_const(full_offidx, idx);
	}

	return sz;
}

uint8_t *
order_index_write_eles(const order_index *ordidx, uint32_t count,
		const offset_index *full_offidx, uint8_t *ptr, bool invert)
{
	uint32_t start = 0;
	uint32_t offset = 0;
	uint32_t sz = 0;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		if (idx == ordidx->max_idx) {
			continue;
		}

		offset = offset_index_get_const(full_offidx, idx);
		sz = offset_index_get_delta_const(full_offidx, idx);

		if (! invert) {
			memcpy(ptr, full_offidx->contents + offset, sz);
			ptr += sz;
		}
		else {
			uint32_t invert_sz = offset - start;

			if (invert_sz != 0) {
				memcpy(ptr, full_offidx->contents + start, invert_sz);
				ptr += invert_sz;
			}
		}

		start = offset + sz;
	}

	if (! invert) {
		return ptr;
	}

	uint32_t invert_sz = full_offidx->content_sz - start;

	memcpy(ptr, full_offidx->contents + start, invert_sz);

	return ptr + invert_sz;
}

uint32_t
order_index_adjust_value(const order_index_adjust *via, uint32_t src)
{
	if (via) {
		return via->f(via, src);
	}

	return src;
}

void
order_index_copy(order_index *dest, const order_index *src, uint32_t d_start,
		uint32_t s_start, uint32_t count, const order_index_adjust *adjust)
{
	if (dest->_.ele_sz == src->_.ele_sz && ! adjust) {
		memcpy(order_index_get_mem(dest, d_start),
				order_index_get_mem(src, s_start),
				src->_.ele_sz * count);
	}
	else {
		for (uint32_t i = 0; i < count; i++) {
			uint32_t value = order_index_get(src, s_start + i);

			value = order_index_adjust_value(adjust, value);
			order_index_set(dest, d_start + i, value);
		}
	}
}

size_t
order_index_calc_size(uint32_t max_idx, uint32_t ele_count)
{
	return order_index_ele_sz(max_idx) * ele_count;
}

void
order_index_print(const order_index *ordidx, const char *name)
{
	if (! name) {
		name = "value";
	}

	msgpacked_index_print(&ordidx->_, name);
}


//==========================================================
// order_heap
//

bool
order_heap_init_build_by_range_temp(order_heap *heap, uint8_t *mem_temp,
		uint32_t idx, uint32_t count, uint32_t ele_count,
		order_heap_compare_fn cmp_fn, const void *udata)
{
	uint32_t tail_distance = ele_count - idx - count;
	uint32_t discard;
	msgpack_compare_t cmp;

	if (idx <= tail_distance) {
		cmp = MSGPACK_COMPARE_LESS; // min k
		discard = idx;
	}
	else {
		cmp = MSGPACK_COMPARE_GREATER; // max k
		discard = tail_distance;
	}

	order_index_init2_temp(&heap->_, mem_temp, ele_count, ele_count);
	heap->filled = 0;
	heap->userdata = udata;
	heap->cmp = cmp;
	heap->cmp_fn = cmp_fn;
	order_heap_build(heap, true);

	if (! order_heap_order_at_end(heap, count + discard)) {
		return false;
	}

	return true;
}

void
order_heap_swap(order_heap *heap, uint32_t index1, uint32_t index2)
{
	uint32_t temp = order_heap_get(heap, index1);
	order_heap_set(heap, index1, order_heap_get(heap, index2));
	order_heap_set(heap, index2, temp);
}

bool
order_heap_remove_top(order_heap *heap)
{
	if (heap->filled == 0) {
		return true;
	}

	uint32_t index = order_heap_get(heap, (heap->filled--) - 1);

	return order_heap_replace_top(heap, index);
}

bool
order_heap_replace_top(order_heap *heap, uint32_t value)
{
	order_heap_set(heap, 0, value);

	return order_heap_heapify(heap, 0);
}

bool
order_heap_heapify(order_heap *heap, uint32_t index)
{
	while (true) {
		uint32_t child1 = 2 * index + 1;
		uint32_t child2 = 2 * index + 2;
		uint32_t child;

		if (child1 >= heap->filled) {
			break;
		}

		if (child2 >= heap->filled) {
			child = child1;
		}
		else {
			msgpack_compare_t cmp = heap->cmp_fn(heap->userdata,
					order_heap_get(heap, child1),
					order_heap_get(heap, child2));

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			if (cmp == heap->cmp || cmp == MSGPACK_COMPARE_EQUAL) {
				child = child1;
			}
			else {
				child = child2;
			}
		}

		msgpack_compare_t cmp = heap->cmp_fn(heap->userdata,
				order_heap_get(heap, child),
				order_heap_get(heap, index));

		if (cmp == MSGPACK_COMPARE_ERROR) {
			return false;
		}

		if (cmp == heap->cmp) {
			order_heap_swap(heap, index, child);
			index = child;
		}
		else {
			break;
		}
	}

	return true;
}

// O(n)
bool
order_heap_build(order_heap *heap, bool init)
{
	if (init) {
		heap->filled = heap->_._.ele_count;

		for (size_t i = 0; i < heap->filled; i++) {
			order_heap_set(heap, i, i);
		}
	}

	int64_t start = (int64_t)heap->filled / 2 - 1;

	for (int64_t i = start; i >= 0; i--) {
		if (! order_heap_heapify(heap, (uint32_t)i)) {
			return false;
		}
	}

	return true;
}

bool
order_heap_order_at_end(order_heap *heap, uint32_t count)
{
	uint32_t end_index = heap->filled - 1;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t value = order_heap_get(heap, 0);

		if (! order_heap_remove_top(heap)) {
			return false;
		}

		order_heap_set(heap, end_index--, value);
	}

	cf_assert(heap->filled == end_index + 1, AS_PARTICLE, "FIXME"); // FIXME
	heap->filled = end_index + 1;

	return true;
}

// Reverse order of end indexes.
void
order_heap_reverse_end(order_heap *heap, uint32_t count)
{
	uint32_t start = heap->filled;
	uint32_t end = start + count;
	uint32_t stop = (start + end) / 2;

	end--;

	for (uint32_t i = start; i < stop; i++) {
		uint32_t left = order_heap_get(heap, i);
		uint32_t right = order_heap_get(heap, end);

		order_heap_set(heap, end--, left);
		order_heap_set(heap, i, right);
	}
}

void
order_heap_print(const order_heap *heap)
{
	order_index_print(&heap->_, "heap");
}


//==========================================================
// cdt_idx_mask
//

size_t
cdt_idx_mask_count(uint32_t ele_count)
{
	return (ele_count + 63) / 64;
}

void
cdt_idx_mask_init(uint64_t *mask, uint32_t ele_count)
{
	memset(mask, 0, cdt_idx_mask_count(ele_count) * sizeof(uint64_t));
}

void
cdt_idx_mask_set(uint64_t *mask, uint32_t idx)
{
	uint32_t shift = idx % 64;

	mask[idx / 64] |= 1ULL << shift;
}

void
cdt_idx_mask_set_by_ordidx(uint64_t *mask, const order_index *ordidx,
		uint32_t start, uint32_t count, bool inverted)
{
	for (uint32_t i = 0; i < count; i++) {
		cdt_idx_mask_set(mask, order_index_get(ordidx, start + i));
	}

	if (inverted) {
		cdt_idx_mask_invert(mask, ordidx->max_idx);
	}
}

void
cdt_idx_mask_set_by_irc(uint64_t *mask, const order_index *irc,
		const order_index *idx_map, bool inverted)
{
	uint32_t items_count = irc->_.ele_count / 2;

	for (uint32_t i = 0; i < items_count; i++) {
		uint32_t rank = order_index_get(irc, 2 * i);
		uint32_t count = order_index_get(irc, (2 * i) + 1);

		if (count == 0) {
			continue;
		}

		uint32_t end = rank + count;

		for (uint32_t j = rank; j < end; j++) {
			cdt_idx_mask_set(mask, idx_map ? order_index_get(idx_map, j) : j);
		}
	}

	if (inverted) {
		cdt_idx_mask_invert(mask, irc->max_idx);
	}
}

void
cdt_idx_mask_invert(uint64_t *mask, uint32_t ele_count)
{
	uint32_t mask_count = cdt_idx_mask_count(ele_count);

	for (uint32_t i = 0; i < mask_count; i++) {
		mask[i] = ~mask[i];
	}
}

uint64_t
cdt_idx_mask_get(const uint64_t *mask, uint32_t idx)
{
	return mask[idx / 64];
}

size_t
cdt_idx_mask_bit_count(const uint64_t *mask, uint32_t ele_count)
{
	size_t mask_count = cdt_idx_mask_count(ele_count);

	if (mask_count == 0) {
		return 0;
	}

	size_t sum = 0;

	if (ele_count % 64 != 0) {
		uint64_t last_mask = (1ULL << (ele_count % 64)) - 1;

		mask_count--;
		sum = cf_bit_count64(mask[mask_count] & last_mask);
	}

	for (size_t i = 0; i < mask_count; i++) {
		sum += cf_bit_count64(mask[i]);
	}

	return sum;
}

bool
cdt_idx_mask_is_set(const uint64_t *mask, uint32_t idx)
{
	uint32_t shift = idx % 64;

	return (mask[idx / 64] & (1ULL << shift)) != 0;
}

// Find first 1 or 0.
uint32_t
cdt_idx_mask_find(const uint64_t *mask, uint32_t start, uint32_t end,
		bool is_find0)
{
	cf_assert(start <= end, AS_PARTICLE, "start %u > end %u", start, end);

	if (start == end) {
		return end;
	}

	uint32_t offset = start % 64;
	uint32_t i = start / 64;
	uint64_t bit_mask = ~((1ULL << offset) - 1);
	uint64_t bits = (is_find0 ? ~mask[i] : mask[i]) & bit_mask;
	uint32_t count = cf_lsb64(bits);

	if (count != 64) {
		offset = start - offset + count;

		if (offset > end) {
			return end;
		}

		return offset;
	}

	uint32_t i_end = (end + 63) / 64;

	for (i++; i < i_end; i++) {
		count = cf_lsb64(is_find0 ? ~mask[i] : mask[i]);

		if (count != 64) {
			break;
		}
	}

	offset = (i * 64) + count;

	if (offset > end) {
		return end;
	}

	return offset;
}

uint8_t *
cdt_idx_mask_write_eles(const uint64_t *mask, uint32_t count,
		const offset_index *full_offidx, uint8_t *ptr, bool invert)
{
	if (count == 0) {
		if (! invert) {
			return ptr;
		}

		memcpy(ptr, full_offidx->contents, full_offidx->content_sz);
		return ptr + full_offidx->content_sz;
	}

	uint32_t ele_count = full_offidx->_.ele_count;
	uint32_t start_offset = 0;
	uint32_t idx = 0;
	uint32_t count_left = count;

	while (idx < ele_count) {
		uint32_t idx0 = cdt_idx_mask_find(mask, idx, ele_count, false);

		cf_assert(idx0 < ele_count, AS_PARTICLE, "idx0 %u out of bounds from idx %u ele_count %u", idx0, idx, ele_count);
		idx = cdt_idx_mask_find(mask, idx0 + 1, ele_count, true);

		if (idx - idx0 > count_left) {
			idx = idx0 + count_left;
		}

		uint32_t offset0 = offset_index_get_const(full_offidx, idx0);
		uint32_t offset1 = offset_index_get_const(full_offidx, idx);

		if (invert) {
			uint32_t sz = offset0 - start_offset;

			memcpy(ptr, full_offidx->contents + start_offset, sz);
			ptr += sz;
			start_offset = offset1;
		}
		else {
			uint32_t sz = offset1 - offset0;

			memcpy(ptr, full_offidx->contents + offset0, sz);
			ptr += sz;
		}

		count_left -= idx - idx0;

		if (count_left == 0) {
			break;
		}

		idx++;
	}

	if (invert) {
		uint32_t sz = full_offidx->content_sz - start_offset;

		memcpy(ptr, full_offidx->contents + start_offset, sz);
		ptr += sz;
	}

	return ptr;
}

uint32_t
cdt_idx_mask_get_content_sz(const uint64_t *mask, uint32_t count,
		const offset_index *full_offidx)
{
	uint32_t sz = 0;
	uint32_t idx = 0;
	uint32_t ele_count = full_offidx->_.ele_count;

	for (uint32_t i = 0; i < count; i++) {
		idx = cdt_idx_mask_find(mask, idx, ele_count, false);
		sz += offset_index_get_delta_const(full_offidx, idx);
		idx++;
	}

	return sz;
}

void
cdt_idx_mask_print(const uint64_t *mask, uint32_t ele_count, const char *name)
{
	if (! name) {
		name = "mask";
	}

	size_t max = (ele_count + 63) / 64;
	char buf[1024];
	char *ptr = buf;

	for (size_t i = 0; i < max; i++) {
		if (buf + 1024 - ptr < 18) {
			break;
		}

		ptr += sprintf(ptr, "%016lX, ", mask[i]);
	}

	if (ele_count != 0) {
		ptr -= 2;
	}

	*ptr = '\0';

	cf_warning(AS_PARTICLE, "%s: index[%u]={%s}", name, ele_count, buf);
}


//==========================================================
// list
//

bool
list_param_parse(const cdt_payload *items, as_unpacker *pk, uint32_t *count_r)
{
	pk->buffer = items->ptr;
	pk->offset = 0;
	pk->length = items->sz;

	int64_t items_hdr = as_unpack_list_header_element_count(pk);

	if (items_hdr < 0 || items_hdr > CDT_MAX_PARAM_LIST_COUNT) {
		cf_warning(AS_PARTICLE, "list_param_parse() invalid param items_hdr %ld", items_hdr);
		return false;
	}

	*count_r = (uint32_t)items_hdr;

	return true;
}


//==========================================================
// cdt_idx
//

void
cdt_idx_set_alloc(rollback_alloc *alloc)
{
	cf_assert(cdt_alloc_heap == NULL, AS_PARTICLE, "cdt_alloc_heap not NULL");
	cdt_alloc_heap = alloc;
}

void
cdt_idx_clear()
{
	if (cdt_alloc_heap) {
		rollback_alloc_rollback(cdt_alloc_heap);
		cdt_alloc_heap = NULL;
	}
}

uint8_t *
cdt_idx_alloc(uint32_t sz)
{
	cf_assert(cdt_alloc_heap != NULL, AS_PARTICLE, "cdt_alloc_heap not set");
	return rollback_alloc_reserve(cdt_alloc_heap, sz);
}


//==========================================================
// Debugging support.
//

void
print_hex(const uint8_t *packed, uint32_t packed_sz, char *buf, uint32_t buf_sz)
{
	uint32_t n = (buf_sz - 3) / 2;

	if (n > packed_sz) {
		n = packed_sz;
		buf[buf_sz - 3] = '.';
		buf[buf_sz - 2] = '.';
		buf[buf_sz - 1] = '\0';
	}

	char *ptr = (char *)buf;

	for (int i = 0; i < n; i++) {
		sprintf(ptr, "%02X", packed[i]);
		ptr += 2;
	}
}

void
print_packed(const uint8_t *packed, uint32_t sz, const char *name)
{
	cf_warning(AS_PARTICLE, "%s: data=%p sz=%u", name, packed, sz);

	const uint32_t limit = 256;
	uint32_t n = (sz + limit - 1) / limit;
	uint32_t line_sz = limit;
	char mem[1024];

	for (uint32_t i = 0; i < n; i++) {
		if (i == n - 1) {
			line_sz = sz % limit;
		}

		print_hex(packed + limit * i, line_sz, mem, sizeof(mem));
		cf_warning(AS_PARTICLE, "%s:%0X: [%s]", name, i, mem);
	}
}

void
cdt_bin_print(const as_bin *b, const char *name)
{
	typedef struct {
		uint8_t type;
		uint32_t sz;
		uint8_t data[];
	} __attribute__ ((__packed__)) cdt_mem;

	const cdt_mem *p = (const cdt_mem *)b->particle;
	uint8_t bintype = as_bin_get_particle_type(b);

	if (! p || (bintype != AS_PARTICLE_TYPE_MAP &&
			bintype != AS_PARTICLE_TYPE_LIST)) {
		cf_warning(AS_PARTICLE, "%s: particle NULL type %u", name, bintype);
		return;
	}

	cf_warning(AS_PARTICLE, "%s: btype %u data=%p sz=%u type=%d", name, bintype, p->data, p->sz, p->type);
	char buf[4096];
	print_hex(p->data, p->sz, buf, 4096);
	cf_warning(AS_PARTICLE, "%s: buf=%s", name, buf);
}
