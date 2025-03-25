/*
 * cdt.c
 *
 * Copyright (C) 2015-2022 Aerospike, Inc.
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
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_map_iterator.h"
#include "aerospike/as_msgpack.h"
#include "citrusleaf/cf_byte_order.h"

#include "bits.h"
#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"

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
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND,			AS_OPERATOR_CDT_MODIFY, 2, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND_ITEMS,	AS_OPERATOR_CDT_MODIFY, 2, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT,			AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT_ITEMS,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS),

	// Removes
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP_RANGE,		AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE,			AS_OPERATOR_CDT_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_RANGE,	AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	// Modifies
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SET,			AS_OPERATOR_CDT_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS),
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

	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD,							AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD_ITEMS,					AS_OPERATOR_MAP_MODIFY, 1, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT,							AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT_ITEMS,					AS_OPERATOR_MAP_MODIFY, 2, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_FLAGS, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE,						AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_STORAGE, AS_CDT_PARAM_STORAGE),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE_ITEMS,				AS_OPERATOR_MAP_MODIFY, 0, AS_CDT_PARAM_STORAGE),

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

typedef struct index_pack24_s {
	uint32_t value:24;
} __attribute__ ((__packed__)) index_pack24;

typedef struct {
	const order_index *ordidx;
	bool error;
} index_sort_userdata;

typedef struct {
	offset_index offidx;
	uint32_t n_msgpack;
	uint32_t ix;
	uint8_t *ext_start;
	uint8_t *new_contents;
	uint32_t ext_content_sz;
	msgpack_in prev;
	uint8_t type;
	uint8_t ext_type;
	bool need_sort;
} cdt_stack_entry;

typedef struct {
	cdt_stack_entry entries0[8];
	cdt_stack_entry *entries;
	uint32_t entries_cap;
	uint32_t ilevel;
	msgpack_type toplvl_type;
	bool has_toplvl;
} cdt_stack;

static const char* cdt_exp_display_names[] = {
		[AS_CDT_OP_LIST_APPEND] = "list_append",
		[AS_CDT_OP_LIST_APPEND_ITEMS] = "list_append_items",
		[AS_CDT_OP_LIST_CLEAR] = "list_clear",
		[AS_CDT_OP_LIST_INCREMENT] = "list_increment",
		[AS_CDT_OP_LIST_INSERT] = "list_insert",
		[AS_CDT_OP_LIST_INSERT_ITEMS] = "list_insert_items",
		[AS_CDT_OP_LIST_REMOVE_BY_INDEX] = "list_remove_by_index",
		[AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE] = "list_remove_by_index_range",
		[AS_CDT_OP_LIST_REMOVE_BY_RANK] = "list_remove_by_rank",
		[AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE] = "list_remove_by_rank_range",
		[AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE] = "list_remove_by_rel_rank_range",
		[AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE] = "list_remove_by_value",
		[AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST] = "list_remove_by_value_list",
		[AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL] = "list_remove_by_value_range",
		[AS_CDT_OP_LIST_SET] = "list_set",
		[AS_CDT_OP_LIST_SORT] = "list_sort",

		[AS_CDT_OP_LIST_GET_BY_INDEX] = "list_get_by_index",
		[AS_CDT_OP_LIST_GET_BY_INDEX_RANGE] = "list_get_by_index_range",
		[AS_CDT_OP_LIST_GET_BY_RANK] = "list_get_by_rank",
		[AS_CDT_OP_LIST_GET_BY_RANK_RANGE] = "list_get_by_rank_range",
		[AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE] = "list_get_by_rel_rank_range",
		[AS_CDT_OP_LIST_GET_BY_VALUE] = "list_get_by_value",
		[AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST] = "list_get_by_value_list",
		[AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL] = "list_get_by_value_range",
		[AS_CDT_OP_LIST_SIZE] = "list_size",

		[AS_CDT_OP_MAP_CLEAR] = "map_clear",
		[AS_CDT_OP_MAP_INCREMENT] = "map_increment",
		[AS_CDT_OP_MAP_PUT] = "map_put",
		[AS_CDT_OP_MAP_PUT_ITEMS] = "map_put_items",
		[AS_CDT_OP_MAP_REMOVE_BY_INDEX] = "map_remove_by_index",
		[AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE] = "map_remove_by_index_range",
		[AS_CDT_OP_MAP_REMOVE_BY_KEY] = "map_remove_by_key",
		[AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST] = "map_remove_by_key_list",
		[AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL] = "map_remove_by_key_range",
		[AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE] = "map_remove_by_rel_index_range",
		[AS_CDT_OP_MAP_REMOVE_BY_RANK] = "map_remove_by_rank",
		[AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE] = "map_remove_by_rank_range",
		[AS_CDT_OP_MAP_REMOVE_BY_VALUE] = "map_remove_by_value",
		[AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST] = "map_remove_by_value_list",
		[AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL] = "map_remove_by_value_range",
		[AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE] = "map_remove_by_rel_rank_range",

		[AS_CDT_OP_MAP_GET_BY_INDEX] = "map_get_by_index",
		[AS_CDT_OP_MAP_GET_BY_INDEX_RANGE] = "map_get_by_index_range",
		[AS_CDT_OP_MAP_GET_BY_KEY] = "map_get_by_key",
		[AS_CDT_OP_MAP_GET_BY_KEY_LIST] = "map_get_by_key_list",
		[AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL] = "map_get_by_key_range",
		[AS_CDT_OP_MAP_GET_BY_RANK] = "map_get_by_rank",
		[AS_CDT_OP_MAP_GET_BY_RANK_RANGE] = "map_get_by_rank_range",
		[AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE] = "map_get_by_rel_index_range",
		[AS_CDT_OP_MAP_GET_ALL_BY_VALUE] = "map_get_by_value",
		[AS_CDT_OP_MAP_GET_BY_VALUE_LIST] = "map_get_by_value_list",
		[AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL] = "map_get_by_value_range",
		[AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE] = "map_get_by_rel_rank_range",
		[AS_CDT_OP_MAP_SIZE] = "map_size",
};

static const size_t n_cdt_exp_display_names = sizeof(cdt_exp_display_names) / sizeof(char*);


//==========================================================
// Forward declares.
//

static uint32_t calc_count(uint32_t index, uint64_t in_count, uint32_t max_index);
static void calc_index_count_multi(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count);

static uint8_t *shrink_ext_offidx(uint8_t *start, const uint8_t *end, uint32_t ele_count, uint32_t old_content_sz, uint32_t new_content_sz);

static bool unpack_list_value(msgpack_in *mp, cdt_payload *payload_r);
static bool unpack_map_key(msgpack_in *mp, cdt_payload *payload_r);
static bool unpack_map_value(msgpack_in *mp, cdt_payload *payload_r);

static inline uint8_t *buf_pack_nil_rep(uint8_t *buf, uint32_t rep);
static inline void pack_nil_rep(as_packer *pk, uint32_t rep);

// asval
static bool asval_serialize_internal(const as_val *val, as_packer *pk, as_serializer *s);

// cdt_process_state
static bool cdt_process_state_init_from_vec(cdt_process_state *cdt_state, msgpack_in_vec* mv);

// order_index
static inline uint32_t order_index_ele_sz(uint32_t max_idx);

// bin
static bool bin_cdt_get_by_context_vec(const as_bin *b, msgpack_in_vec *ctx_mv, as_bin *result);

// cdt_context
static bool cdt_context_ctx_type_create_sz(msgpack_in_vec *mv, uint32_t *sz, uint64_t ctx_type);
static bool cdt_context_count_create_sz(msgpack_in_vec *mv, uint32_t *sz, uint32_t param_count);
static uint16_t cdt_context_get_toplvl_type_int(const cdt_context *ctx, int64_t *index_r);
static uint8_t *cdt_context_fill_create(const cdt_context *ctx, uint8_t *to_ptr, bool write_tophdr);
static uint8_t *cdt_context_create_new_particle_crnew(cdt_context *ctx, uint32_t subctx_sz);
static uint8_t *cdt_context_create_new_particle_crtop(cdt_context *ctx, uint32_t subctx_sz);
static void cdt_context_fill_unpacker(cdt_context *ctx, msgpack_in *mp);

static void cdt_context_unwind(cdt_context *ctx);

static bool cdt_context_type_is_read(uint8_t ctx_type);

// as_bin_cdt_packed functions
static int cdt_packed_modify(cdt_process_state *state, as_bin *b, as_bin *result, cf_ll_buf *particles_llb);
static int cdt_packed_read(cdt_process_state *state, const as_bin *b, as_bin *result);


//==========================================================
// Local helpers.
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

static uint8_t *
shrink_ext_offidx(uint8_t *start, const uint8_t *end, uint32_t ele_count,
		uint32_t old_content_sz, uint32_t new_content_sz)
{
	offset_index old_idx;
	offset_index new_idx;

	offset_index_init(&old_idx, NULL, ele_count, NULL, old_content_sz);
	offset_index_init(&new_idx, NULL, ele_count, NULL, new_content_sz);

	uint32_t old_idx_sz = offset_index_size(&old_idx);
	uint32_t new_idx_sz = offset_index_size(&new_idx);
	uint32_t delta_off_sz = old_idx_sz - new_idx_sz;

	if (delta_off_sz == 0) {
		return (uint8_t *)end;
	}

	cf_assert(old_idx_sz >= new_idx_sz, AS_PARTICLE, "unsupported old %u new %u", old_idx_sz, new_idx_sz);

	msgpack_ext ext;
	uint32_t ext_hdr_sz = msgpack_buf_get_ext(start, UINT32_MAX, &ext);

	as_packer pk = {
			.buffer = start,
			.capacity = end - start
	};

	as_pack_ext_header(&pk, ext.size - delta_off_sz, ext.type);

	uint32_t delta_hdr_sz = ext_hdr_sz - pk.offset;
	uint8_t *p_new = start + pk.offset;

	offset_index_set_ptr(&old_idx, (uint8_t *)ext.data, NULL);
	offset_index_set_ptr(&new_idx, p_new, NULL);
	// NOTE: Copy in place works for shrinking only.
	offset_index_set_filled(&new_idx, ele_count);

	for (uint32_t i = 1; i < ele_count; i++) {
		uint32_t value = msgpacked_index_get(&old_idx._, i);

		msgpacked_index_set(&new_idx._, i, value);
	}

	p_new += new_idx_sz;

	uint8_t *p_src = p_new + delta_off_sz + delta_hdr_sz;
	size_t mv_sz = end - p_src;

	memmove(p_new, p_src, mv_sz);

	return p_new + mv_sz;
}

static bool
unpack_list_value(msgpack_in *mp, cdt_payload *payload_r)
{
	payload_r->ptr = mp->buf + mp->offset;

	uint32_t sz = msgpack_sz(mp);

	if (sz == 0) {
		cf_warning(AS_PARTICLE, "unpack_list_value() invalid msgpack");
		return false;
	}

	payload_r->sz = sz;

	return true;
}

static bool
unpack_map_key(msgpack_in *mp, cdt_payload *payload_r)
{
	payload_r->ptr = mp->buf + mp->offset;

	uint32_t sz = msgpack_sz(mp);

	if (sz == 0) {
		cf_warning(AS_PARTICLE, "unpack_map_key() invalid msgpack");
		return false;
	}

	payload_r->sz = sz;

	if (msgpack_sz(mp) == 0) { // skip value
		cf_warning(AS_PARTICLE, "unpack_map_key() invalid msgpack");
		return false;
	}

	return true;
}

static bool
unpack_map_value(msgpack_in *mp, cdt_payload *payload_r)
{
	if (msgpack_sz(mp) == 0) { // skip key
		cf_warning(AS_PARTICLE, "unpack_map_value() invalid msgpack");
		return false;
	}

	payload_r->ptr = mp->buf + mp->offset;

	uint32_t sz = msgpack_sz(mp);

	if (sz == 0) {
		cf_warning(AS_PARTICLE, "unpack_map_value() invalid msgpack");
		return false;
	}

	payload_r->sz = sz;

	return true;
}

static inline uint8_t *
buf_pack_nil_rep(uint8_t *buf, uint32_t rep)
{
	memset(buf, 0xc0, rep);
	return buf + rep;
}

static inline void
pack_nil_rep(as_packer *pk, uint32_t rep)
{
	memset(pk->buffer + pk->offset, 0xc0, rep);
	pk->offset += rep;
}


//==========================================================
// asval
//

static bool
asval_serialize_internal(const as_val *val, as_packer *pk, as_serializer *s)
{
	switch (as_val_type(val)) {
	case AS_NIL:
	case AS_BOOLEAN:
	case AS_INTEGER:
	case AS_DOUBLE:
	case AS_STRING:
	case AS_BYTES:
	case AS_GEOJSON:
	case AS_CMP_WILDCARD:
	case AS_CMP_INF: {
		uint8_t *wptr = (pk->buffer == NULL) ? NULL : pk->buffer + pk->offset;
		int sz = as_serializer_serialize_presized(s, val, wptr);

		if (sz > 0) {
			pk->offset += sz;
			return true;
		}

		cf_warning(AS_PARTICLE, "asval_serialize_internal() failed to parse type %d", as_val_type(val));
		return false;
	}
	case AS_LIST: {
		as_list *plist = (as_list *)val;
		uint32_t ele_count = as_list_size(plist);
		uint8_t flags = plist->flags;

		if (pk->offset != 0) { // top level check
			flags &= ~AS_PACKED_PERSIST_INDEX;
		}

		flags &= AS_PACKED_LIST_FLAG_ORDERED | AS_PACKED_PERSIST_INDEX;

		if (flags != 0) {
			as_pack_list_header(pk, ele_count + 1);
			as_pack_ext_header(pk, 0, flags);
		}
		else {
			as_pack_list_header(pk, ele_count);
		}

		msgpack_in prev = {
				.buf = pk->buffer + pk->offset,
				.buf_sz = UINT32_MAX
		};

		bool is_ordered = (flags & AS_PACKED_LIST_FLAG_ORDERED) != 0;
		bool is_write = (pk->buffer != NULL);
		bool need_sort = false;

		for (uint32_t i = 0; i < ele_count; i++) {
			const as_val *ele = as_list_get(plist, i);
			uint8_t *start = pk->buffer + pk->offset;

			if (! asval_serialize_internal(ele, pk, s)) {
				return false;
			}

			if (i != 0 && is_ordered && ! need_sort && is_write) {
				msgpack_in mp = {
						.buf = start,
						.buf_sz = UINT32_MAX
				};

				msgpack_cmp_type cmp = msgpack_cmp(&prev, &mp);

				switch (cmp) {
				case MSGPACK_CMP_GREATER:
					need_sort = true;
					break;
				default:
					break;
				}
			}
		}

		bool is_post_sizer = ! is_write && flags_is_persist(flags);

		if (need_sort || flags_is_persist(flags)) {
			// Adjust sizer for top level offset indexes.
			uint8_t *contents = (uint8_t *)prev.buf;
			uint32_t content_sz =
					(uint32_t)(pk->buffer + pk->offset - contents);
			uint32_t ext_content_sz = list_calc_ext_content_sz(flags, ele_count,
					content_sz);
			uint32_t delta = as_pack_ext_header_get_size(ext_content_sz) +
					ext_content_sz - as_pack_ext_header_get_size(0);

			if (is_post_sizer) {
				pk->offset += delta;
			}
			else if (need_sort) {
				offset_index offidx;
				order_index ordidx;

				offset_index_init(&offidx, NULL, ele_count, NULL, content_sz);
				order_index_init(&ordidx, NULL, ele_count);

				uint8_t *temp_mem = cf_malloc(content_sz +
						offset_index_size(&offidx) + order_index_size(&ordidx));
				uint8_t *write_mem = temp_mem;

				memcpy(temp_mem, contents, content_sz);
				write_mem += content_sz;
				offset_index_set_ptr(&offidx, write_mem, temp_mem);
				write_mem += offset_index_size(&offidx);
				order_index_set_ptr(&ordidx, write_mem);
				offset_index_set_filled(&offidx, 1);

				if (! offset_index_fill(&offidx, false, true) ||
						! list_order_index_sort(&ordidx, &offidx,
								AS_CDT_SORT_ASCENDING)) {
					cf_free(temp_mem);
					cf_warning(AS_PARTICLE, "asval_serialize_internal() failed to sort list");
					return false;
				}

				if (flags_is_persist(flags)) {
					as_packer pk2 = {
							.buffer = pk->buffer,
							.offset =
									as_pack_list_header_get_size(ele_count + 1),
							.capacity = UINT32_MAX
					};

					offset_index new_offidx;

					as_pack_ext_header(&pk2, ext_content_sz, flags);
					offset_index_init(&new_offidx, pk2.buffer + pk2.offset,
							ele_count, temp_mem, content_sz);

					uint8_t *check = order_index_write_eles(&ordidx, ele_count,
							&offidx, contents + delta, &new_offidx, false);

					pk->offset += delta;
					cf_assert(check == contents + delta + content_sz, AS_PARTICLE, "content mismatch %p != %p", check, contents + delta + content_sz);
				}
				else {
					uint8_t *check = order_index_write_eles(&ordidx, ele_count,
							&offidx, contents, NULL, false);

					cf_assert(check == contents + content_sz, AS_PARTICLE, "content mismatch %p != %p", check, contents + content_sz);
				}

				cf_free(temp_mem);
			}
			else { // persist index
				memmove(contents + delta, contents, content_sz);
				pk->offset += delta;

				as_packer pk2 = {
						.buffer = pk->buffer,
						.offset =
								as_pack_list_header_get_size(ele_count + 1),
						.capacity = UINT32_MAX
				};

				as_pack_ext_header(&pk2, ext_content_sz, flags);

				offset_index offidx;

				offset_index_init(&offidx, pk2.buffer + pk2.offset, ele_count,
						contents + delta, content_sz);

				return offset_index_fill(&offidx, false, true);
			}
		}

		break;
	}
	case AS_MAP: {
		as_map *pmap = (as_map *)val;
		uint32_t ele_count = as_map_size(pmap);
		uint8_t flags = pmap->flags;

		if (pk->offset != 0) { // top level check
			flags &= ~AS_PACKED_PERSIST_INDEX;
		}

		flags &= AS_PACKED_MAP_FLAG_KV_ORDERED | AS_PACKED_PERSIST_INDEX;

		as_pack_map_header(pk, ele_count + (flags == 0 ? 0 : 1));

		uint32_t ext_offset = pk->offset;

		if (flags != 0) {
			as_pack_ext_header(pk, 0, flags);
			as_pack_nil(pk);
		}

		uint32_t contents_offset = pk->offset;
		as_map_iterator it;

		as_map_iterator_init(&it, pmap);

		for (uint32_t i = 0; i < ele_count; i++) {
			as_pair *pair = (as_pair *)as_iterator_next((as_iterator *)&it);

			if (! asval_serialize_internal(as_pair_1(pair), pk, s)) {
				return false;
			}

			if (! asval_serialize_internal(as_pair_2(pair), pk, s)) {
				return false;
			}
		}

		if (flags_is_persist(flags)) {
			uint32_t content_sz = pk->offset - contents_offset;
			uint32_t ext_content_sz = map_calc_ext_content_sz(flags, ele_count,
					content_sz);
			uint32_t delta = as_pack_ext_header_get_size(ext_content_sz) +
					ext_content_sz - as_pack_ext_header_get_size(0);

			pk->offset += delta;

			if (pk->buffer != NULL) { // write mode
				memmove(pk->buffer + contents_offset + delta,
						pk->buffer + contents_offset, content_sz);

				as_packer pk2 = {
						.buffer = pk->buffer,
						.offset = ext_offset,
						.capacity = UINT32_MAX
				};

				as_pack_ext_header(&pk2, ext_content_sz, flags);
				as_pack_nil(&pk2);

				offset_index offidx;
				offset_index_init(&offidx, pk2.buffer + pk2.offset, ele_count,
						pk->buffer + contents_offset + delta, content_sz);

				return offset_index_fill(&offidx, true, true);
			}
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "asval_serialize_internal() as_val %p buf %p offset %u unexpected type %d", val, pk->buffer, pk->offset, as_val_type(val));
		return false;
	}

	return true;
}

uint32_t
asval_serialize(const as_val *val, uint8_t *buf)
{
	as_packer pk = {
			.buffer = buf,
			.capacity = INT_MAX
	};

	as_serializer s;
	as_msgpack_init(&s);

	if (! asval_serialize_internal(val, &pk, &s)) {
		return 0;
	}

	return pk.offset;
}


//==========================================================
// Global helpers.
//

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
			as_bin_set_empty_list(rd->result, 0, rd->alloc);
			break;
		}

		as_bin_set_int(rd->result, -1);
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(rd->result, 0);
		break;
	case RESULT_TYPE_EXISTS:
		as_bin_set_bool(rd->result, false);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
		if (rd->is_multi) {
			as_bin_set_empty_list(rd->result, 0, rd->alloc);
		}
		break;
	case RESULT_TYPE_KEY_VALUE_MAP:
	case RESULT_TYPE_UNORDERED_MAP:
	case RESULT_TYPE_ORDERED_MAP:
		as_bin_set_empty_map(rd->result, result_map_type_to_map_flags(rd->type),
				rd->alloc);
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
	case RESULT_TYPE_EXISTS:
		as_bin_set_bool(rd->result, inverted ? count == 0 : count != 0);
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
		return -AS_ERR_OP_NOT_APPLICABLE;
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
	case RESULT_TYPE_EXISTS:
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
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		result_data_set_list_int2x(rd, start, count);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "result_data_set_range() invalid return type %d", rd->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
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

void
as_bin_set_bool(as_bin *b, bool value)
{
	b->particle = (as_particle *)(uint64_t)(value ? 1 : 0);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_BOOL);
}


//==========================================================
// cdt_strip
//

uint32_t
cdt_strip_indexes_from_particle(const as_particle *p, uint8_t *dest,
		msgpack_type expected_type)
{
	const cdt_mem *p_cdt_mem = (const cdt_mem *)p;

	cf_assert(p_cdt_mem->sz != 0, AS_PARTICLE, "invalid particle");

	while (true) {
		const uint8_t *b = p_cdt_mem->data;
		const uint8_t *end = b + p_cdt_mem->sz;
		uint32_t count = 1;
		msgpack_type type;
		bool has_nonstorage = false;
		bool not_compact = false;
		uint32_t old_count = count;

		b = msgpack_parse(b, end, &count, &type, &has_nonstorage, &not_compact);

		uint32_t ele_count = count - old_count;

		cf_assert(! has_nonstorage && b != NULL, AS_PARTICLE, "invalid msgpack: has_nonstorage %d b %p", has_nonstorage, b);

		if (expected_type != 0) {
			cf_assert(type == expected_type, AS_PARTICLE, "invalid cdt type %d", type);
		}

		if (old_count == count) { // not list/map or empty list/map
			break;
		}

		msgpack_type next_type = msgpack_buf_peek_type(b, end - b);

		if (next_type != MSGPACK_TYPE_EXT) {
			break;
		}

		msgpack_ext ext;
		uint32_t ext_sz = msgpack_buf_get_ext(b, end - b, &ext);

		cf_assert(ext_sz != 0, AS_PARTICLE, "invalid msgpack: b %lx", *(uint64_t*)b);

		if (ext.size == 0 && ! flags_is_persist(ext.type)) {
			break;
		}

		ext.type &= ~AS_PACKED_PERSIST_INDEX;
		b += ext_sz;

		as_packer pk = {
				.buffer = dest,
				.capacity = UINT32_MAX
		};

		if (type == MSGPACK_TYPE_MAP) {
			ele_count /= 2;

			if (ext.type == 0) {
				as_pack_map_header(&pk, ele_count - 1);
				b = msgpack_parse(b, end, &count, &type, &has_nonstorage,
						&not_compact);
			}
			else {
				as_pack_map_header(&pk, ele_count);
				as_pack_ext_header(&pk, 0, ext.type);
			}
		}
		else { // LIST
			if (ext.type == 0) {
				as_pack_list_header(&pk, ele_count - 1);
			}
			else {
				as_pack_list_header(&pk, ele_count);
				as_pack_ext_header(&pk, 0, ext.type);
			}
		}

		as_pack_append(&pk, b, end - b);

		return pk.offset;
	}

	if (dest != NULL) {
		memcpy(dest, p_cdt_mem->data, p_cdt_mem->sz);
	}

	return p_cdt_mem->sz;
}


//==========================================================
// cdt_calc_delta
//

bool
cdt_calc_delta_init(cdt_calc_delta *cdv, const cdt_payload *delta_value,
		bool is_decrement)
{
	cdv->incr_int = 1;
	cdv->incr_double = 1;

	if (delta_value && delta_value->ptr) {
		msgpack_in mp_delta_value = {
				.buf = delta_value->ptr,
				.buf_sz = delta_value->sz
		};

		cdv->type = msgpack_peek_type(&mp_delta_value);

		if (msgpack_type_is_int(cdv->type)) {
			if (! msgpack_get_int64(&mp_delta_value, &cdv->incr_int)) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_init() invalid packed delta value");
				return false;
			}
		}
		else if (cdv->type == MSGPACK_TYPE_DOUBLE) {
			if (! msgpack_get_double(&mp_delta_value, &cdv->incr_double)) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_init() invalid packed delta value");
				return false;
			}
		}
		else if (cdv->type == MSGPACK_TYPE_NIL) {
			cdv->type = MSGPACK_TYPE_NIL;
		}
		else {
			cf_warning(AS_PARTICLE, "cdt_delta_value_init() delta is not int/double");
			return false;
		}
	}
	else {
		cdv->type = MSGPACK_TYPE_NIL;
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
cdt_calc_delta_add(cdt_calc_delta *cdv, msgpack_in *mp_value)
{
	if (mp_value) {
		msgpack_type packed_value_type = msgpack_peek_type(mp_value);

		if (msgpack_type_is_int(packed_value_type)) {
			if (! msgpack_get_int64(mp_value, &cdv->value_int)) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_add() invalid packed int");
				return false;
			}

			if (cdv->type == MSGPACK_TYPE_DOUBLE) {
				cdv->value_int += (int64_t)cdv->incr_double;
			}
			else {
				cdv->value_int += cdv->incr_int;
			}
		}
		else if (packed_value_type == MSGPACK_TYPE_DOUBLE) {
			if (! msgpack_get_double(mp_value, &cdv->value_double)) {
				cf_warning(AS_PARTICLE, "cdt_delta_value_add() invalid packed double");
				return false;
			}

			if (cdv->type == MSGPACK_TYPE_DOUBLE) {
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
	else if (cdv->type == MSGPACK_TYPE_DOUBLE) {
		cdv->value_double += cdv->incr_double;
	}
	else {
		cdv->type = MSGPACK_TYPE_INT; // default to integer
		cdv->value_int += cdv->incr_int;
	}

	return true;
}

void
cdt_calc_delta_pack_and_result(cdt_calc_delta *cdv, cdt_payload *value,
		as_bin *result)
{
	if (cdv->type == MSGPACK_TYPE_DOUBLE) {
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

void
cdt_payload_pack_int(cdt_payload *packed, int64_t value)
{
	as_packer pk = {
			.buffer = (uint8_t *)packed->ptr,
			.capacity = packed->sz
	};

	as_pack_int64(&pk, value);
	packed->sz = pk.offset;
}

void
cdt_payload_pack_double(cdt_payload *packed, double value)
{
	as_packer pk = {
			.buffer = (uint8_t *)packed->ptr,
			.capacity = packed->sz
	};

	as_pack_double(&pk, value);
	packed->sz = pk.offset;
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
	as_packer pk = {
			.buffer = builder->write_ptr,
			.capacity = INT_MAX
	};

	as_pack_int64(&pk, value);
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
	as_bin_state_set_from_type(result->result,
			(as_particle_type)((uint8_t *)builder->particle)[0]);
}


//==========================================================
// cdt_process_state functions.
//

static bool
cdt_process_state_init_from_vec(cdt_process_state *cdt_state,
		msgpack_in_vec* mv)
{
	const uint8_t* data = mv->vecs[0].buf;
	uint32_t sz = mv->vecs[0].buf_sz;

	cdt_state->mv = mv;

	if (data[0] == 0) { // TODO - deprecate this in "6 months"
		if (sz < sizeof(uint16_t)) {
			cf_warning(AS_PARTICLE, "cdt_parse_state_init() as_msg_op data too small to be valid: size=%u", sz);
			return false;
		}

		const uint16_t *type_ptr = (const uint16_t *)data;

		cdt_state->type = cf_swap_from_be16(*type_ptr);
		cdt_state->mv->vecs[0].offset += sizeof(uint16_t);
		cdt_state->ele_count = 0;

		if (sz - sizeof(uint16_t) != 0 &&
				! msgpack_get_list_ele_count_vec(cdt_state->mv,
						&cdt_state->ele_count)) {
			cf_warning(AS_PARTICLE, "cdt_parse_state_init() unpack list header failed: size=%u type=%u ele_count=%u", sz, cdt_state->type, cdt_state->ele_count);
			return false;
		}

		return true;
	}

	uint32_t ele_count = 0;
	uint64_t t64;

	if (! msgpack_get_list_ele_count_vec(cdt_state->mv, &ele_count) ||
			ele_count == 0 || ! msgpack_get_uint64_vec(cdt_state->mv, &t64)) {
		cf_warning(AS_PARTICLE, "cdt_parse_state_init() unpack parameters failed: size=%u ele_count=%u", sz, ele_count);
		return false;
	}

	cdt_state->type = (as_cdt_optype)t64;
	cdt_state->ele_count = ele_count - 1; // does not include op type

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
		case AS_CDT_PARAM_PAYLOAD:
		case AS_CDT_PARAM_STORAGE: {
			cdt_payload *arg = va_arg(vl, cdt_payload *);

			arg->ptr = msgpack_get_ele_vec(state->mv, &arg->sz);

			if (arg->ptr == NULL || (entry->args[i] == AS_CDT_PARAM_STORAGE &&
					state->mv->has_nonstorage)) {
				va_end(vl);
				return false;
			}

			break;
		}
		case AS_CDT_PARAM_FLAGS:
		case AS_CDT_PARAM_COUNT: {
			uint64_t *arg = va_arg(vl, uint64_t *);

			if (! msgpack_get_uint64_vec(state->mv, arg)) {
				va_end(vl);
				return false;
			}

			break;
		}
		case AS_CDT_PARAM_INDEX: {
			int64_t *arg = va_arg(vl, int64_t *);

			if (! msgpack_get_int64_vec(state->mv, arg)) {
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
// cdt_process_state_context_eval
//

bool
cdt_process_state_context_eval(cdt_process_state *state, cdt_op_mem *com)
{
	if (state->ele_count != 2) {
		cf_warning(AS_PARTICLE, "cdt_process_state_context_eval() param count %u != 2", state->ele_count);
		com->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	if ((com->ret_code = cdt_context_dig(&com->ctx, state->mv,
			cdt_op_is_modify(com))) != AS_OK) {
		return false;
	}

	uint32_t ele_count;
	uint64_t type64;

	if (! msgpack_get_list_ele_count_vec(state->mv, &ele_count) ||
			ele_count == 0 || ! msgpack_get_uint64_vec(state->mv, &type64)) {
		cf_warning(AS_PARTICLE, "cdt_process_state_context_eval() unpack parameters failed: size=%u ele_count=%u",
				state->mv->vecs[state->mv->idx].buf_sz, ele_count);
		com->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	state->type = (as_cdt_optype)type64;
	state->ele_count = ele_count - 1;

	if (! com->ctx.create_triggered) {
		msgpack_in mp;
		msgpack_type ctx_type;
		msgpack_type expected = IS_CDT_LIST_OP(state->type) ?
				MSGPACK_TYPE_LIST : MSGPACK_TYPE_MAP;

		cdt_context_fill_unpacker(&com->ctx, &mp);
		ctx_type = msgpack_peek_type(&mp);

		if (ctx_type != expected) {
			const char *name = IS_CDT_LIST_OP(state->type) ? "list" : "map";

			cf_warning(AS_PARTICLE, "subcontext type %d != expected type %d (%s)", ctx_type, expected, name);
			com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
			return false;
		}
	}

	if (cdt_op_is_modify(com)) {
		bool ret;

		if (IS_CDT_LIST_OP(state->type)) {
			ret = cdt_process_state_packed_list_modify_optype(state, com);
		}
		else {
			ret = cdt_process_state_packed_map_modify_optype(state, com);
		}

		if (ret) {
			cdt_context_unwind(&com->ctx);

#if defined(CDT_DEBUG_VERIFY)
			com->ctx.create_triggered = false;
			com->ctx.data_offset = 0;
			com->ctx.data_sz = 0;
			if (! cdt_verify(&com->ctx)) {
				cdt_context_print(&com->ctx, "ctx");
				cf_crash(AS_PARTICLE, "cdt_process_state_context_eval");
			}
#endif
		}

		return ret;
	}
	else {
		if (IS_CDT_LIST_OP(state->type)) {
			return cdt_process_state_packed_list_read_optype(state, com);
		}
		else {
			return cdt_process_state_packed_map_read_optype(state, com);
		}
	}

	return false; // can't get here
}


//==========================================================
// bin
//

static bool
bin_cdt_get_by_context_vec(const as_bin *b, msgpack_in_vec *ctx_mv,
		as_bin *result)
{
	if (! cdt_context_read_check_peek(ctx_mv)) {
		return false;
	}

	define_rollback_alloc(alloc_result, NULL, 1);

	cdt_context ctx = {
			.b = (as_bin *)b,
			.alloc_buf = NULL
	};

	if (cdt_context_dig(&ctx, ctx_mv, false) != AS_OK) {
		return false;
	}

	msgpack_in mp;

	cdt_context_fill_unpacker(&ctx, &mp);

	const cdt_payload cp = {
			.ptr = mp.buf,
			.sz = mp.buf_sz
	};

	return rollback_alloc_from_msgpack(alloc_result, result, &cp);
}


//==========================================================
// cdt_context
//

static bool
cdt_context_ctx_create_type_check(uint64_t ctx_type)
{
	uint8_t masked_type = (uint8_t)ctx_type & AS_CDT_CTX_TYPE_MASK;
	uint16_t cr_type = (uint16_t)ctx_type & AS_CDT_CTX_CREATE_MASK;

	if ((masked_type & AS_CDT_CTX_LIST) != 0 ||
			(masked_type & AS_CDT_CTX_MAP) != 0) {
		return true;
	}

	// Auto ctx type cannot have create flags.
	return cr_type == 0;
}

static bool
cdt_context_ctx_type_create_sz(msgpack_in_vec *mv, uint32_t *sz,
		uint64_t ctx_type)
{
	uint8_t masked_type = (uint8_t)(ctx_type & AS_CDT_CTX_TYPE_MASK);

	if (masked_type == (AS_CDT_CTX_KEY | AS_CDT_CTX_MAP)) {
		mv->has_nonstorage = false;

		uint32_t key_sz;
		const uint8_t *key = msgpack_get_ele_vec(mv, &key_sz);

		if (key == NULL || mv->has_nonstorage) {
			cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid context key");
			return false;
		}

		if ((key_sz = cdt_untrusted_get_size(key, key_sz, NULL, false)) == 0) {
			cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid context key");
			return false;
		}

		*sz += key_sz;

		if (map_get_ext_flags(ctx_type, true) != 0) {
			*sz += 3 + 1; // ext element pair size
		}
	}
	else if (masked_type == (AS_CDT_CTX_INDEX | AS_CDT_CTX_LIST)) {
		int64_t idx;
		uint16_t cr_type = (uint16_t)ctx_type & AS_CDT_CTX_CREATE_MASK;

		if (! msgpack_get_int64_vec(mv, &idx) || idx < -1) {
			cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid context index");
			return false;
		}

		if (cr_type == 0) {
			if (idx > 0) {
				cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid context index %ld", idx);
				return false;
			}
		}
		else {
			cr_type &= ~AS_CDT_CTX_CREATE_PERSIST_INDEX;

			if (cr_type == AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND) {
				if ((ctx_type & AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0) {
					*sz += 3; // ext element size
					*sz += as_pack_list_header_get_size(idx + 2);
				}
				else {
					*sz += as_pack_list_header_get_size(idx + 1);
				}

				*sz += idx - 1; // size of nil elements, minus 1 to be added again before return
			}
			else if (cr_type == AS_CDT_CTX_CREATE_LIST_ORDERED ||
					(ctx_type & AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0) {
				*sz += 3; // ext element size
			}
			else if (idx > 0) {
				cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid context index %ld", idx);
				return false;
			}
		}
	}
	else {
		cf_warning(AS_PARTICLE, "cdt_context_ctx_type_create_sz() invalid create context 0x%lx", ctx_type);
		return false;
	}

	*sz += 1; // map or list hdr size to add

	return true;
}

static bool
cdt_context_count_create_sz(msgpack_in_vec *mv, uint32_t *sz,
		uint32_t param_count)
{
	for (uint32_t i = 0; i < param_count; i++) {
		uint64_t ctx_type;

		if (! msgpack_get_uint64_vec(mv, &ctx_type)) {
			cf_warning(AS_PARTICLE, "cdt_context_count_create_sz() param %u expected int", i);
			return false;
		}

		if (! cdt_context_ctx_create_type_check(ctx_type)) {
			cf_warning(AS_PARTICLE, "cdt_context_count_create_sz() invalid context type 0x%lx", ctx_type);
			return false;
		}

		if ((ctx_type & AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0) {
			cf_warning(AS_PARTICLE, "cdt_context_count_create_sz() persist index not allowed for sub-context");
			return false;
		}

		if (! cdt_context_ctx_type_create_sz(mv, sz, ctx_type)) {
			return false;
		}
	}

	return true;
}

static uint16_t
cdt_context_get_toplvl_type_int(const cdt_context *ctx, int64_t *index_r)
{
	msgpack_in mp = {
			.buf = ctx->create_ctx_start,
			.buf_sz = UINT32_MAX
	};

	uint64_t ctx_type;

	if (! msgpack_get_uint64(&mp, &ctx_type)) {
		cf_crash(AS_PARTICLE, "cdt_context_get_toplvl_type() param pair 0 expected int");
	}

	if (index_r) {
		msgpack_get_int64(&mp, index_r);
	}

	return (uint16_t)ctx_type;
}

static uint8_t *
cdt_context_fill_create(const cdt_context *ctx, uint8_t *to_ptr,
		bool write_tophdr)
{
	msgpack_in mp = {
			.buf = ctx->create_ctx_start,
			.buf_sz = UINT32_MAX
	};

	uint64_t ctx_type;

	if (! msgpack_get_uint64(&mp, &ctx_type)) {
		cf_crash(AS_PARTICLE, "cdt_context_fill_create() param pair 0 expected int");
	}

	uint8_t masked_type = (uint8_t)(ctx_type & AS_CDT_CTX_TYPE_MASK);
	uint16_t cr_type = (uint16_t)(ctx_type & AS_CDT_CTX_CREATE_MASK);

	if (masked_type == (AS_CDT_CTX_KEY | AS_CDT_CTX_MAP)) {
		if (write_tophdr) {
			as_packer pk = {
					.buffer = to_ptr,
					.capacity = UINT32_MAX
			};

			uint8_t flags = map_get_ext_flags(cr_type, true);

			if (flags == 0) {
				as_pack_map_header(&pk, 1);
			}
			else {
				as_pack_map_header(&pk, 2);
				as_pack_ext_header(&pk, 0, flags);
				as_pack_nil(&pk);
			}

			to_ptr += pk.offset;
		}

		const uint8_t *key_ptr = mp.buf + mp.offset;
		uint32_t key_sz = msgpack_sz(&mp);

		if (key_sz == 0 || mp.has_nonstorage) {
			cf_crash(AS_PARTICLE, "cdt_context_fill_create() invalid context key");
		}

		uint32_t to_sz = cdt_untrusted_rewrite(to_ptr, key_ptr, key_sz, false);

		if (to_sz == 0) {
			return NULL;
		}

		to_ptr += to_sz;
	}
	else if (masked_type == (AS_CDT_CTX_INDEX | AS_CDT_CTX_LIST)) {
		int64_t idx;

		if (! msgpack_get_int64(&mp, &idx) || idx < -1) {
			cf_crash(AS_PARTICLE, "cdt_context_fill_create() invalid context index");
		}

		if (write_tophdr) {
			as_packer pk = {
					.buffer = to_ptr,
					.capacity = UINT32_MAX
			};

			if (idx == -1) {
				idx = 0;
			}

			bool is_persist = (cr_type & AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0;
			bool is_ordered = (cr_type & AS_CDT_CTX_CREATE_LIST_ORDERED) ==
					AS_CDT_CTX_CREATE_LIST_ORDERED;

			cr_type &= ~AS_CDT_CTX_CREATE_PERSIST_INDEX;

			uint8_t flags = list_get_ext_flags(is_ordered, is_persist);

			if (cr_type == AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND) {
				if (is_persist) {
					as_pack_list_header(&pk, idx + 2);
					as_pack_ext_header(&pk, 0, flags);
				}
				else {
					as_pack_list_header(&pk, idx + 1);
				}

				memset(pk.buffer + pk.offset, 0xc0, idx);
				pk.offset += idx;
			}
			else if (flags != AS_PACKED_LIST_FLAG_NONE) {
				as_pack_list_header(&pk, 2);
				as_pack_ext_header(&pk, 0, flags);
				idx = 0;
			}
			else {
				cf_assert(idx == 0, AS_PARTICLE, "cdt_context_fill_create() invalid context index %ld", idx);
				as_pack_list_header(&pk, 1);
			}

			to_ptr += pk.offset;
		}
	}
	else if (write_tophdr) {
		cf_warning(AS_PARTICLE, "cdt_context_fill_create() invalid ctx_type %lx", ctx_type);
		return NULL;
	}

	for (uint32_t i = 1; i < ctx->create_ctx_count; i++) {
		if (! msgpack_get_uint64(&mp, &ctx_type)) {
			cf_warning(AS_PARTICLE, "cdt_context_fill_create() param pair %u expected int", i);
			return NULL;
		}

		masked_type = ctx_type & AS_CDT_CTX_TYPE_MASK;
		cr_type = (uint16_t)(ctx_type & AS_CDT_CTX_CREATE_MASK);

		if ((cr_type & AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0) {
			cf_warning(AS_PARTICLE, "cdt_context_fill_create() PERSIST_INDEX only allowed for top level list/map");
			return NULL;
		}

		if (masked_type == (AS_CDT_CTX_KEY | AS_CDT_CTX_MAP)) {
			as_packer pk = {
					.buffer = to_ptr,
					.capacity = UINT32_MAX
			};


			uint8_t flags = map_get_ext_flags(ctx_type, false);

			if (flags == 0) {
				as_pack_map_header(&pk, 1);
			}
			else {
				as_pack_map_header(&pk, 2);
				as_pack_ext_header(&pk, 0, flags);
				as_pack_nil(&pk);
			}

			to_ptr += pk.offset;

			const uint8_t *key_ptr = mp.buf + mp.offset;
			uint32_t key_sz = msgpack_sz(&mp);

			if (key_sz == 0 || mp.has_nonstorage) {
				cf_crash(AS_PARTICLE, "cdt_context_fill_create() invalid context key");
			}

			uint32_t to_sz = cdt_untrusted_rewrite(to_ptr, key_ptr, key_sz,
					false);

			if (to_sz == 0) {
				return NULL;
			}

			to_ptr += to_sz;
		}
		else if (masked_type == (AS_CDT_CTX_INDEX | AS_CDT_CTX_LIST)) {
			int64_t idx;

			if (! msgpack_get_int64(&mp, &idx) || idx < -1) {
				cf_crash(AS_PARTICLE, "cdt_context_fill_create() invalid context index");
			}

			if (idx == -1) {
				idx = 0;
			}

			as_packer pk = {
					.buffer = to_ptr,
					.capacity = UINT32_MAX
			};

			if (cr_type == AS_CDT_CTX_CREATE_LIST_ORDERED) {
				as_pack_list_header(&pk, 2);
				as_pack_ext_header(&pk, 0, list_get_ext_flags(true, false));
			}
			else if (cr_type == AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND) {
				as_pack_list_header(&pk, idx + 1);
				pack_nil_rep(&pk, idx);
			}
			else {
				cf_assert(idx == 0, AS_PARTICLE, "cdt_context_fill_create() invalid context index %ld", idx);
				as_pack_list_header(&pk, 1);
			}

			to_ptr += pk.offset;
		}
		else {
			cf_crash(AS_PARTICLE, "cdt_context_fill_create() invalid create context 0x%lx", ctx_type);
		}
	}

	return to_ptr;
}

static uint8_t *
cdt_context_create_new_particle_crnew(cdt_context *ctx, uint32_t subctx_sz)
{
	uint32_t new_sz = ctx->create_sz + subctx_sz;
	int64_t idx = 0;
	uint16_t ctx_type = cdt_context_get_toplvl_type_int(ctx, &idx);
	// Most cases don't need indexes because they are ele_count 1. The only case
	// that can exceed ele_count 1 is LIST_UNORDERED_UNBOUND.
	bool need_ext_contents = (ctx_type & AS_CDT_CTX_LIST) != 0 &&
			(ctx_type & AS_CDT_CTX_CREATE_MASK) ==
					(AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND |
							AS_CDT_CTX_CREATE_PERSIST_INDEX);
	uint32_t ele_count = idx + 1;
	uint32_t ext_content_sz = 0;
	uint32_t content_sz = 0;

	if (need_ext_contents) {
		uint32_t hdr_sz = as_pack_list_header_get_size(ele_count);
		offset_index off;

		content_sz = new_sz - hdr_sz;
		list_partial_offset_index_init(&off, NULL, ele_count, NULL, content_sz);
		ext_content_sz = offset_index_size(&off);

		if (ext_content_sz == 0) {
			need_ext_contents = false;
		}
		else {
			uint32_t new_hdr_sz = as_pack_list_header_get_size(ele_count + 1);

			new_sz += new_hdr_sz - hdr_sz;
			new_sz += as_pack_ext_header_get_size(ext_content_sz);
			new_sz += ext_content_sz;
		}
	}

	cdt_mem *p_cdt_mem = (cdt_mem *)rollback_alloc_reserve(ctx->alloc_buf,
			sizeof(cdt_mem) + new_sz);
	uint8_t *to_ptr = p_cdt_mem->data;

	if (need_ext_contents) {
		as_packer pk = {
				.buffer = to_ptr,
				.capacity = UINT32_MAX
		};

		offset_index off;

		as_pack_list_header(&pk, ele_count + 1);
		as_pack_ext_header(&pk, ext_content_sz, AS_PACKED_PERSIST_INDEX);

		list_partial_offset_index_init(&off, pk.buffer + pk.offset, ele_count,
				NULL, content_sz);
		offset_index_set_filled(&off, 1);
		pk.offset += offset_index_size(&off);

		pack_nil_rep(&pk, ele_count - 1);
		to_ptr = cdt_context_fill_create(ctx, pk.buffer + pk.offset, false);
	}
	else {
		to_ptr = cdt_context_fill_create(ctx, to_ptr, true);
	}

	p_cdt_mem->sz = new_sz;
	ctx->b->particle = (as_particle *)p_cdt_mem;

	if (msgpack_buf_peek_type(p_cdt_mem->data, 5) == MSGPACK_TYPE_LIST) {
		p_cdt_mem->type = AS_PARTICLE_TYPE_LIST;
		as_bin_state_set_from_type(ctx->b, AS_PARTICLE_TYPE_LIST);
	}
	else { // must be map
		p_cdt_mem->type = AS_PARTICLE_TYPE_MAP;
		as_bin_state_set_from_type(ctx->b, AS_PARTICLE_TYPE_MAP);
	}

	cf_assert(new_sz == (uint32_t)(to_ptr - p_cdt_mem->data) + subctx_sz, AS_PARTICLE, "cdt_context_create_new_particle_crnew() size mismatch %u != %u",
			new_sz, (uint32_t)(to_ptr - p_cdt_mem->data) + subctx_sz);

	return to_ptr;
}

static uint8_t *
cdt_context_copy_head(cdt_context *ctx, uint8_t *to_ptr,
		const uint8_t *from_ptr, uint32_t sz)
{
	if (ctx->create_hdr_ptr == NULL) {
		memcpy(to_ptr, from_ptr, sz);
		return to_ptr + sz;
	}

	uint32_t head_sz = (uint32_t)(ctx->create_hdr_ptr - from_ptr);

	msgpack_in mp = {
			.buf = ctx->create_hdr_ptr,
			.buf_sz = sz - head_sz
	};

	msgpack_type orig_type = msgpack_peek_type(&mp);
	uint32_t ele_count;

	memcpy(to_ptr, from_ptr, head_sz);
	to_ptr += head_sz;

	as_packer pk = {
			.buffer = to_ptr,
			.capacity = 8 + ctx->list_nil_pad
	};

	if (orig_type == MSGPACK_TYPE_LIST) {
		msgpack_get_list_ele_count(&mp, &ele_count);
		as_pack_list_header(&pk, ele_count + 1 + ctx->list_nil_pad);
	}
	else if (orig_type == MSGPACK_TYPE_MAP) {
		msgpack_get_map_ele_count(&mp, &ele_count);
		as_pack_map_header(&pk, ele_count + 1);
	}
	else {
		cf_crash(AS_PARTICLE, "unexpected type %d", (int)orig_type);
	}

	uint32_t tail_sz = mp.buf_sz - mp.offset;

	memcpy(pk.buffer + pk.offset, mp.buf + mp.offset, tail_sz);
	pk.offset += tail_sz;
	pack_nil_rep(&pk, ctx->list_nil_pad);

	return pk.buffer + pk.offset;
}

static uint8_t *
cdt_context_create_new_particle_crtop(cdt_context *ctx, uint32_t subctx_sz)
{
	const uint8_t *orig_data = cdt_context_get_data(ctx);
	uint32_t orig_sz = cdt_context_get_sz(ctx);
	uint32_t new_sz = orig_sz + ctx->delta_sz;
	msgpack_ext ext;
	offset_index newoff;
	uint32_t new_content_sz = ctx->top_content_sz + ctx->delta_sz;

	msgpack_in mp = {
			.buf = orig_data,
			.buf_sz = orig_sz
	};

	msgpack_type orig_type = msgpack_peek_type(&mp);
	uint32_t new_ext_cont_sz = 0;
	uint32_t orig_hdr_count = 0;

	if (orig_type == MSGPACK_TYPE_LIST) {
		bool check = msgpack_get_list_ele_count(&mp, &orig_hdr_count);
		uint32_t hdr_sz = mp.offset;
		cf_assert(check, AS_PARTICLE, "msgpack_get_list_ele_count failed");

		if (! msgpack_peek_is_ext(&mp)) {
			ext.type = 0;
		}
		else if ((check = msgpack_get_ext(&mp, &ext)) &&
				flags_is_persist(ext.type)) {
			if ((ext.type & AS_PACKED_LIST_FLAG_ORDERED) != 0) {
				offset_index_init(&newoff, NULL, ctx->top_ele_count + 1, NULL,
						new_content_sz);
				new_ext_cont_sz = offset_index_size(&newoff);
				new_sz += as_pack_ext_header_get_size(new_ext_cont_sz) +
						new_ext_cont_sz - mp.offset + hdr_sz;
			}
			else {
				list_partial_offset_index_init(&newoff, NULL,
						ctx->top_ele_count + ctx->list_nil_pad + 1, NULL,
						new_content_sz);
				new_ext_cont_sz = offset_index_size(&newoff);

				if (new_ext_cont_sz != 0) {
					new_sz += as_pack_list_header_get_size(ctx->top_ele_count +
							ctx->list_nil_pad + 1 + 1);
					new_sz += as_pack_ext_header_get_size(new_ext_cont_sz);
					new_sz += new_ext_cont_sz;
					new_sz -= mp.offset;

					// Undo delta hdr calculation from
					// list_subcontext_by_index() because non-top-level was
					// assumed.
					new_sz += as_pack_list_header_get_size(ctx->top_ele_count);
					new_sz -= as_pack_list_header_get_size(ctx->top_ele_count +
							ctx->list_nil_pad + 1);
				}
				// else -- already taken care of by list_subcontext_by_index().
			}
		}
		else {
			cf_assert(check, AS_PARTICLE, "list as_unpack_ext failed");
		}
	}
	else if (orig_type == MSGPACK_TYPE_MAP) {
		bool check = msgpack_get_map_ele_count(&mp, &orig_hdr_count);
		uint32_t hdr_sz = mp.offset;
		cf_assert(check, AS_PARTICLE, "msgpack_get_map_ele_count failed");

		if (! msgpack_peek_is_ext(&mp)) {
			ext.type = 0;
		}
		else if (msgpack_get_ext(&mp, &ext)) {
			if (flags_is_persist(ext.type)) {
				offset_index_init(&newoff, NULL, ctx->top_ele_count + 1, NULL,
						new_content_sz);
				new_ext_cont_sz = offset_index_size(&newoff);

				if ((ext.type & AS_PACKED_MAP_FLAG_V_ORDERED) != 0) {
					order_index neword;

					order_index_init(&neword, NULL, ctx->top_ele_count + 1);
					new_ext_cont_sz += order_index_size(&neword);
				}

				new_sz += as_pack_ext_header_get_size(new_ext_cont_sz) +
						new_ext_cont_sz - mp.offset + hdr_sz;
			}
		}
		else {
			cf_crash(AS_PARTICLE, "map as_unpack_ext failed");
		}
	}
	else {
		cf_crash(AS_PARTICLE, "unexpected type %d", orig_type);
	}

	cdt_mem *p_cdt_mem = (cdt_mem *)rollback_alloc_reserve(ctx->alloc_buf,
			sizeof(cdt_mem) + new_sz);
	uint8_t *to_ptr = p_cdt_mem->data;

	as_packer pk = {
			.buffer = to_ptr,
			.capacity = new_sz
	};

	if (orig_type == MSGPACK_TYPE_LIST) {
		if (ext.type != 0) {
			as_pack_list_header(&pk, ctx->top_ele_count + ctx->list_nil_pad +
					1 + 1); // 1 for ext, 1 for created element
			as_pack_ext_header(&pk, new_ext_cont_sz, ext.type);

			if (flags_is_persist(ext.type)) {
				offset_index_set_ptr(&newoff, pk.buffer + pk.offset, NULL);
				offset_index_set_filled(&newoff, 1); // TODO - patch newoff
				pk.offset += offset_index_size(&newoff);
			}
		}
		else {
			as_pack_list_header(&pk, ctx->top_ele_count + ctx->list_nil_pad +
					1);
		}

		to_ptr += pk.offset;
	}
	else if (orig_type == MSGPACK_TYPE_MAP) {
		if (ext.type != 0) {
			as_pack_map_header(&pk, ctx->top_ele_count + 1 + 1); // 1 for ext, 1 for created element
			as_pack_ext_header(&pk, new_ext_cont_sz, ext.type);

			if (flags_is_persist(ext.type)) {
				offset_index_set_ptr(&newoff, pk.buffer + pk.offset, NULL);
				offset_index_set_filled(&newoff, 1); // TODO - patch newoff
				pk.offset += offset_index_size(&newoff);

				if ((ext.type & AS_PACKED_MAP_FLAG_V_ORDERED) != 0) {
					order_index neword;

					order_index_init(&neword, pk.buffer + pk.offset,
							ctx->top_ele_count + 1); // 1 for created element
					order_index_set(&neword, 0, ctx->top_ele_count + 1);
					pk.offset += order_index_size(&neword);
				}
			}
		}
		else {
			as_pack_map_header(&pk, ctx->top_ele_count + 1); // +1 for created element
		}

		to_ptr += pk.offset;
	}
	else {
		cf_crash(AS_PARTICLE, "unexpected type %d", (int)orig_type);
	}

	const uint8_t *from_ptr = mp.buf + mp.offset;
	uint32_t from_sz = orig_data + ctx->data_offset - from_ptr;

	memcpy(to_ptr, from_ptr, from_sz);
	to_ptr += from_sz;

	if (ctx->list_nil_pad != 0) {
		to_ptr = buf_pack_nil_rep(to_ptr, ctx->list_nil_pad);
	}

	to_ptr = cdt_context_fill_create(ctx, to_ptr, false);

	if (to_ptr == NULL) {
		return NULL;
	}

	memcpy(to_ptr + subctx_sz,
			orig_data + ctx->data_offset + ctx->data_sz,
			orig_sz - ctx->data_sz - ctx->data_offset);

	p_cdt_mem->sz = new_sz;
	p_cdt_mem->type = ((cdt_mem *)ctx->b->particle)->type;

	ctx->b->particle = (as_particle *)p_cdt_mem;

	return to_ptr;
}

static void
cdt_context_fill_unpacker(cdt_context *ctx, msgpack_in *mp)
{
	mp->offset = 0;

	if (cdt_context_is_toplvl(ctx)) {
		mp->buf = ((cdt_mem *)ctx->b->particle)->data;
		mp->buf_sz = ((cdt_mem *)ctx->b->particle)->sz;
		return;
	}

	mp->buf = ((cdt_mem *)ctx->b->particle)->data + ctx->data_offset;
	mp->buf_sz = ctx->data_sz;
}

uint32_t
cdt_context_get_sz(cdt_context *ctx)
{
	cdt_mem *p_cdt_mem = (cdt_mem *)ctx->b->particle;
	return p_cdt_mem->sz;
}

const uint8_t *
cdt_context_get_data(cdt_context *ctx)
{
	cdt_mem *p_cdt_mem = (cdt_mem *)ctx->b->particle;
	return p_cdt_mem->data;
}

uint8_t *
cdt_context_create_new_particle(cdt_context *ctx, uint32_t subctx_sz)
{
	ctx->delta_sz = subctx_sz - ctx->data_sz + ctx->create_sz;

	if (! as_bin_is_live(ctx->b)) { // bin did not exist
		return cdt_context_create_new_particle_crnew(ctx, subctx_sz);
	}

	const uint8_t *orig_data = cdt_context_get_data(ctx);
	uint32_t orig_sz = cdt_context_get_sz(ctx);
	uint32_t new_sz = orig_sz + ctx->delta_sz;
	cdt_mem *p_cdt_mem;
	uint8_t *to_ptr;

	if (ctx->top_content_off != 0) { // has top level indexes
		msgpack_ext ext;
		offset_index topoff;
		offset_index newoff;

		if (orig_data == ctx->create_hdr_ptr) { // has create at this level (top)
			return cdt_context_create_new_particle_crtop(ctx, subctx_sz);
		}

		uint32_t new_content_sz = ctx->top_content_sz + ctx->delta_sz;

		msgpack_in mp = {
				.buf = orig_data,
				.buf_sz = orig_sz
		};

		msgpack_type orig_type = msgpack_peek_type(&mp);
		uint32_t ele_count;

		if (orig_type == MSGPACK_TYPE_LIST) {
			msgpack_get_list_ele_count(&mp, &ele_count);
		}
		else if (orig_type == MSGPACK_TYPE_MAP) {
			msgpack_get_map_ele_count(&mp, &ele_count);
		}
		else {
			cf_crash(AS_PARTICLE, "unexpected type %d", (int)orig_type);
		}

		uint32_t hdr_sz = mp.offset;
		uint32_t new_ext_cont_sz = 0;

		if (orig_type == MSGPACK_TYPE_LIST) {
			bool is_ordered = false;

			if (msgpack_peek_is_ext(&mp)) {
				bool check = msgpack_get_ext(&mp, &ext);
				cf_assert(check, AS_PARTICLE, "as_unpack_ext failed");
				is_ordered = (ext.type & AS_PACKED_LIST_FLAG_ORDERED) != 0;
			}

			if (is_ordered) {
				offset_index_init(&topoff, (uint8_t *)ext.data,
						ctx->top_ele_count, NULL, ctx->top_content_sz);
				offset_index_init(&newoff, NULL, ctx->top_ele_count, NULL,
						new_content_sz);
			}
			else {
				list_partial_offset_index_init(&topoff, (uint8_t *)ext.data,
						ctx->top_ele_count, NULL, ctx->top_content_sz);
				list_partial_offset_index_init(&newoff, NULL,
						ctx->top_ele_count, NULL, new_content_sz);
			}

			new_ext_cont_sz = offset_index_size(&newoff);

			uint32_t new_ext_hdr_sz = 0;

			if (new_ext_cont_sz != 0) {
				new_ext_hdr_sz = as_pack_ext_header_get_size(new_ext_cont_sz);
			}
			else if (is_ordered) {
				new_ext_hdr_sz = as_pack_ext_header_get_size(0);
			}

			ctx->delta_off = new_ext_hdr_sz + new_ext_cont_sz -
					mp.offset + hdr_sz;
		}
		else if (orig_type == MSGPACK_TYPE_MAP) {
			bool check = msgpack_get_ext(&mp, &ext);
			cf_assert(check, AS_PARTICLE, "as_unpack_ext failed");

			offset_index_init(&topoff, (uint8_t *)ext.data, ctx->top_ele_count,
					NULL, ctx->top_content_sz);
			offset_index_init(&newoff, NULL, ctx->top_ele_count, NULL,
					new_content_sz);

			new_ext_cont_sz = ext.size + // ext.size may include ordidx for maps
					offset_index_size(&newoff) - offset_index_size(&topoff);

			uint32_t new_ext_hdr_sz =
					as_pack_ext_header_get_size(new_ext_cont_sz);

			ctx->delta_off = new_ext_hdr_sz + new_ext_cont_sz - mp.offset +
					hdr_sz;
		}
		else {
			cf_crash(AS_PARTICLE, "unexpected type %d", (int)orig_type);
		}

		new_sz += ctx->delta_off;
		p_cdt_mem = (cdt_mem *)rollback_alloc_reserve(ctx->alloc_buf,
				sizeof(cdt_mem) + new_sz);
		to_ptr = p_cdt_mem->data;

		if (ctx->delta_off != 0) {
			memcpy(to_ptr, orig_data, hdr_sz);
			to_ptr += hdr_sz;

			as_packer pk = {
					.buffer = to_ptr,
					.capacity = new_sz - hdr_sz
			};

			as_pack_ext_header(&pk, new_ext_cont_sz, ext.type);
			offset_index_set_ptr(&newoff, pk.buffer + pk.offset, NULL);
			offset_index_set_filled(&newoff, 1);
			to_ptr += pk.offset + offset_index_size(&newoff);

			const uint8_t *from_ptr = ext.data + offset_index_size(&topoff);
			uint32_t from_sz =
					(uint32_t)(orig_data + ctx->data_offset - from_ptr);

			to_ptr = cdt_context_copy_head(ctx, to_ptr, from_ptr, from_sz);
		}
		else {
			to_ptr = cdt_context_copy_head(ctx, to_ptr, orig_data,
					ctx->data_offset);
		}
	}
	else {
		p_cdt_mem = (cdt_mem *)rollback_alloc_reserve(ctx->alloc_buf,
				sizeof(cdt_mem) + new_sz);
		to_ptr = cdt_context_copy_head(ctx, p_cdt_mem->data, orig_data,
				ctx->data_offset);
	}

	if (ctx->create_triggered) {
		to_ptr = cdt_context_fill_create(ctx, to_ptr, false);
	}

	if (to_ptr == NULL) {
		return NULL;
	}

	uint32_t tail_sz = orig_sz - ctx->data_sz - ctx->data_offset;
	uint8_t *write_tail = to_ptr + subctx_sz;

	memcpy(write_tail, orig_data + ctx->data_offset + ctx->data_sz, tail_sz);
	write_tail += tail_sz;

	p_cdt_mem->sz = new_sz;
	p_cdt_mem->type = ((cdt_mem *)ctx->b->particle)->type;
	ctx->b->particle = (as_particle *)p_cdt_mem;

	cf_assert(new_sz == (uint32_t)(write_tail - p_cdt_mem->data), AS_PARTICLE, "size mismatch %u != %u",
			new_sz, (uint32_t)(write_tail - p_cdt_mem->data));

	return to_ptr;
}

static inline cdt_ctx_list_stack_entry *
cdt_context_get_stack(cdt_context *ctx)
{
	if (ctx->stack_idx < 2) {
		return &ctx->stack[ctx->stack_idx];
	}

	uint32_t stack_i = ctx->stack_idx - 2;

	if (stack_i >= ctx->stack_cap) {
		ctx->stack_cap += 10;
		ctx->pstack = cf_realloc(ctx->pstack,
				ctx->stack_cap * sizeof(cdt_ctx_list_stack_entry));
	}

	return &ctx->pstack[stack_i];
}

void
cdt_context_push(cdt_context *ctx, uint32_t idx, uint8_t *idx_mem, uint8_t type)
{
	cdt_ctx_list_stack_entry *p = cdt_context_get_stack(ctx);

	p->data_offset = ctx->data_offset;
	p->data_sz = ctx->data_sz;
	p->idx = idx;
	p->type = type;
	p->idx_mem = idx_mem;
	ctx->stack_idx++;
}

int
cdt_context_dig(cdt_context *ctx, msgpack_in_vec *mv, bool is_modify)
{
	static cdt_subcontext_fn list_table[AS_CDT_MAX_CTX] = {
			[AS_CDT_CTX_INDEX] = list_subcontext_by_index,
			[AS_CDT_CTX_RANK] = list_subcontext_by_rank,
			[AS_CDT_CTX_KEY] = list_subcontext_by_key,
			[AS_CDT_CTX_VALUE] = list_subcontext_by_value,
	};

	static cdt_subcontext_fn map_table[AS_CDT_MAX_CTX] = {
			[AS_CDT_CTX_INDEX] = map_subcontext_by_index,
			[AS_CDT_CTX_RANK] = map_subcontext_by_rank,
			[AS_CDT_CTX_KEY] = map_subcontext_by_key,
			[AS_CDT_CTX_VALUE] = map_subcontext_by_value,
	};

	uint8_t bin_type = as_bin_get_particle_type(ctx->b);
	bool bin_was_empty = false;

	if (bin_type == AS_PARTICLE_TYPE_NULL && is_modify) {
		bin_was_empty = true;
	}
	else if (bin_type != AS_PARTICLE_TYPE_LIST &&
			bin_type != AS_PARTICLE_TYPE_MAP) {
		cf_detail(AS_PARTICLE, "cdt_context_dig() bin type %u is not list or map", bin_type);
		return -AS_ERR_PARAMETER;
	}

	uint32_t ctx_param_count = 0;

	msgpack_vec* vec = &mv->vecs[mv->idx];

	if (! msgpack_get_list_ele_count_vec(mv, &ctx_param_count) ||
			ctx_param_count == 0 || (ctx_param_count & 1) == 1) {
		cf_warning(AS_PARTICLE, "cdt_context_dig() bad context param count %u", ctx_param_count);
		return -AS_ERR_PARAMETER;
	}

	for (uint32_t i = 0; i < ctx_param_count; i += 2) {
		uint64_t ctx_type;
		bool ret;
		uint32_t start_off = vec->offset;

		if (! msgpack_get_uint64_vec(mv, &ctx_type)) {
			cf_warning(AS_PARTICLE, "cdt_context_dig() param %u expected int", i);
			return -AS_ERR_PARAMETER;
		}

		uint8_t table_i = (uint8_t)ctx_type & AS_CDT_CTX_BASE_MASK;
		uint16_t cr_ctx_type = (uint16_t)ctx_type & AS_CDT_CTX_CREATE_MASK;

		if (table_i >= AS_CDT_MAX_CTX ||
				! cdt_context_ctx_create_type_check(ctx_type)) {
			cf_warning(AS_PARTICLE, "cdt_context_dig() invalid context type 0x%lx", ctx_type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		ctx->create_ctx_type = cr_ctx_type;
		ctx->create_flag_on =
				(cr_ctx_type & ~AS_CDT_CTX_CREATE_PERSIST_INDEX) != 0;

		if (bin_was_empty) {
			if (! ctx->create_flag_on) {
				cf_detail(AS_PARTICLE, "cdt_context_dig() bin is empty and op has no create flag(s)");
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			ctx->create_triggered = true;
			ctx->create_ctx_start = vec->buf + start_off;
			ctx->create_ctx_count = (ctx_param_count - i) / 2;

			if (! cdt_context_ctx_type_create_sz(mv, &ctx->create_sz,
					ctx_type)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			if (! cdt_context_count_create_sz(mv, &ctx->create_sz,
					ctx->create_ctx_count - 1)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			break;
		}

		msgpack_in mp;

		cdt_context_fill_unpacker(ctx, &mp);
		cf_assert(mp.buf_sz != 0, AS_PARTICLE, "invalid mp.buf_sz due to cdt_context_fill_unpacker being called in not-yet-existent context");

		msgpack_type type = msgpack_peek_type(&mp);

		if (type != MSGPACK_TYPE_MAP && type != MSGPACK_TYPE_LIST) {
			cf_detail(AS_PARTICLE, "cdt_context_dig() type %d is not list or map", type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (type == MSGPACK_TYPE_LIST) {
			if ((ctx_type & AS_CDT_CTX_MAP) != 0) {
				cf_detail(AS_PARTICLE, "cdt_context_dig() invalid context type 0x%lx for list element", ctx_type);
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			ret = list_table[table_i](ctx, mv);
		}
		else { // map
			if ((ctx_type & AS_CDT_CTX_LIST) != 0) {
				cf_detail(AS_PARTICLE, "cdt_context_dig() invalid context type 0x%lx for map element", ctx_type);
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			ret = map_table[table_i](ctx, mv);
		}

		if (! ret) {
			cf_detail(AS_PARTICLE, "cdt_context_dig() invalid context at param %u", i);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (ctx->create_triggered) {
			ctx->create_ctx_start = vec->buf + start_off;
			ctx->create_ctx_count = (ctx_param_count - i) / 2;

			if (! is_modify ||
					! cdt_context_count_create_sz(mv, &ctx->create_sz,
							ctx->create_ctx_count - 1)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			break;
		}
	}

	return AS_OK;
}

bool
cdt_context_read_check_peek(const msgpack_in_vec *ctx)
{
	define_msgpack_vec_copy(mv, ctx);
	uint32_t count;

	if (! msgpack_get_list_ele_count_vec(&mv, &count)) {
		return false;
	}

	if (count == 0 || count % 2 != 0) {
		return false;
	}

	count /= 2;

	for (uint32_t i = 0; i < count; i++) {
		uint64_t type;

		if (! msgpack_get_uint64_vec(&mv, &type) ||
				! cdt_context_type_is_read((uint8_t)type)) {
			return false;
		}

		if (msgpack_sz_vec(&mv) == 0) {
			return false;
		}
	}

	return true;
}

static inline void
cdt_context_destroy(cdt_context *ctx)
{
	while (ctx->stack_idx != 0) {
		ctx->stack_idx--;

		cdt_ctx_list_stack_entry *p = cdt_context_get_stack(ctx);

		cf_free(p->idx_mem);
	}

	cf_free(ctx->pstack);
}

static void
cdt_context_unwind(cdt_context *ctx)
{
	while (ctx->stack_idx != 0) {
		ctx->stack_idx--;

		cdt_ctx_list_stack_entry *p = cdt_context_get_stack(ctx);

		ctx->data_offset = p->data_offset;
		ctx->data_sz = p->data_sz;

		if (p->type == AS_LIST) {
			cdt_context_unwind_list(ctx, p);
		}
		else {
			cdt_context_unwind_map(ctx, p);
		}

		cf_free(p->idx_mem);
	}

	cf_free(ctx->pstack);
}

static bool
cdt_context_type_is_read(uint8_t ctx_type)
{
	return (ctx_type <= 0x23 && (ctx_type & 0xf) <= 3 && ctx_type != 0x12);
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

	if (size == 0) {
		return NULL;
	}

	uint8_t *ptr;

	if (alloc_buf->ll_buf) {
		cf_ll_buf_reserve(alloc_buf->ll_buf, size, &ptr);
	}
	else {
		ptr = cf_malloc(size);
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
	cf_assert(as_bin_is_unused(b), AS_PARTICLE, "bin not empty");

	int32_t sz = as_particle_size_from_msgpack(seg->ptr, seg->sz);

	if (sz < 0) {
		return false;
	}

	uint8_t *mem = (sz == 0) ? NULL : rollback_alloc_reserve(alloc_buf, sz);

	return as_bin_particle_from_msgpack(b, seg->ptr, seg->sz, mem);
}

void *
rollback_alloc_copy(rollback_alloc *alloc_buf, void *buf, uint32_t buf_sz)
{
	void *mem = rollback_alloc_reserve(alloc_buf, buf_sz);
	memcpy(mem, buf, buf_sz);
	return mem;
}


//==========================================================
// as_bin_cdt_packed functions.
//

static int
cdt_packed_modify(cdt_process_state *state, as_bin *b, as_bin *result,
		cf_ll_buf *particles_llb)
{
	define_rollback_alloc(alloc_buf, particles_llb, 1);
	define_rollback_alloc(alloc_result, NULL, 1); // results always on the heap
	define_rollback_alloc(alloc_idx, NULL, 8); // for temp indexes
	define_rollback_alloc(alloc_convert, NULL, 2); // for converting to internal order

	cdt_op_mem com = {
			.ctx = {
					.b = b,
					.orig = b->particle,
					.alloc_buf = alloc_buf
			},
			.result = {
					.result = result,
					.alloc = alloc_result
			},
			.alloc_idx = alloc_idx,
			.alloc_convert = alloc_convert,
			.ret_code = AS_OK,
	};

	as_bin old_bin = *b;
	bool success;

	if (state->type == AS_CDT_OP_CONTEXT_EVAL) {
		success = cdt_process_state_context_eval(state, &com);
	}
	else if (IS_CDT_LIST_OP(state->type)) {
		success = cdt_process_state_packed_list_modify_optype(state, &com);
	}
	else {
		success = cdt_process_state_packed_map_modify_optype(state, &com);
	}

	rollback_alloc_rollback(alloc_idx);
	rollback_alloc_rollback(alloc_convert);

	if (! success) {
		*b = old_bin;
		as_bin_set_empty(result);
		rollback_alloc_rollback(alloc_buf);
		rollback_alloc_rollback(alloc_result);
		cdt_context_destroy(&com.ctx);
	}

	return com.ret_code;
}

static int
cdt_packed_read(cdt_process_state *state, const as_bin *b, as_bin *result)
{
	define_rollback_alloc(alloc_result, NULL, 1); // results always on the heap
	define_rollback_alloc(alloc_idx, NULL, 8); // for temp indexes

	cdt_op_mem com = {
			.ctx = {
					.b = (as_bin *)b,
					.alloc_buf = NULL
			},
			.result = {
					.result = result,
					.alloc = alloc_result
			},
			.alloc_idx = alloc_idx,
			.ret_code = AS_OK,
	};

	bool success;

	if (state->type == AS_CDT_OP_CONTEXT_EVAL) {
		success = cdt_process_state_context_eval(state, &com);
	}
	else if (IS_CDT_LIST_OP(state->type)) {
		success = cdt_process_state_packed_list_read_optype(state, &com);
	}
	else {
		success = cdt_process_state_packed_map_read_optype(state, &com);
	}

	rollback_alloc_rollback(alloc_idx);

	if (! success) {
		as_bin_set_empty(result);
		rollback_alloc_rollback(alloc_result);
	}

	return com.ret_code;
}

int
as_bin_cdt_modify_tr(as_bin *b, const as_msg_op *op, as_bin *result,
		cf_ll_buf *particles_llb)
{
	cdt_process_state state;

	msgpack_vec vecs = {
			.buf = as_msg_op_get_value_p(op),
			.buf_sz = as_msg_op_get_value_sz(op)
	};

	msgpack_in_vec mv = {
			.n_vecs = 1,
			.vecs = &vecs
	};

	if (! cdt_process_state_init_from_vec(&state, &mv)) {
		return -AS_ERR_PARAMETER;
	}

	return cdt_packed_modify(&state, b, result, particles_llb);
}

int
as_bin_cdt_read_tr(const as_bin *b, const as_msg_op *op, as_bin *result)
{
	cdt_process_state state;

	msgpack_vec vecs = {
			.buf = as_msg_op_get_value_p(op),
			.buf_sz = as_msg_op_get_value_sz(op)
	};

	msgpack_in_vec mv = {
			.n_vecs = 1,
			.vecs = &vecs
	};

	if (! cdt_process_state_init_from_vec(&state, &mv)) {
		return -AS_ERR_PARAMETER;
	}

	return cdt_packed_read(&state, b, result);
}

int
as_bin_cdt_modify_exp(as_bin *b, msgpack_in_vec* mv, as_bin *result)
{
	cdt_process_state state;

	if (! cdt_process_state_init_from_vec(&state, mv)) {
		return -AS_ERR_PARAMETER;
	}

	return cdt_packed_modify(&state, b, result, NULL);
}

int
as_bin_cdt_read_exp(const as_bin *b, msgpack_in_vec* mv, as_bin *result)
{
	cdt_process_state state;

	if (! cdt_process_state_init_from_vec(&state, mv)) {
		return -AS_ERR_PARAMETER;
	}

	return cdt_packed_read(&state, b, result);
}

bool
as_bin_cdt_get_by_context(const as_bin *b, const uint8_t* ctx, uint32_t ctx_sz,
		as_bin *result)
{
	msgpack_vec vecs[1];
	msgpack_in_vec mv = {
			.n_vecs = 1,
			.vecs = vecs
	};

	vecs[0].buf = ctx;
	vecs[0].buf_sz = ctx_sz;
	vecs[0].offset = 0;

	return bin_cdt_get_by_context_vec(b, &mv, result);
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
	if (offidx->_.ele_count <= 1) {
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
	if (count == 0) {
		return;
	}

	cf_assert(d_start + count <= dest->_.ele_count, AS_PARTICLE, "d_start(%u) + count(%u) > dest.ele_count(%u)", d_start, count, dest->_.ele_count);
	cf_assert(s_start + count <= src->_.ele_count, AS_PARTICLE, "s_start(%u) + count(%u) > src.ele_count(%u)", s_start, count, src->_.ele_count);

	if (src->_.ptr == NULL) {
		cf_assert(src->_.ele_count == 1 && count == 1, AS_PARTICLE, "null src offidx");
		cf_assert(s_start == 0, AS_PARTICLE, "invalid s_start %u", s_start);
		offset_index_set(dest, d_start, delta);
	}
	else if (dest->_.ele_sz == src->_.ele_sz && delta == 0) {
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
offset_index_add_ele(offset_index *dest, const offset_index *src,
		uint32_t dest_idx)
{
	cf_assert(dest->content_sz >= src->content_sz, AS_PARTICLE, "delta < 0 dest->content_sz %u src->content_sz %u", dest->content_sz, src->content_sz);

	uint32_t add_sz = dest->content_sz - src->content_sz;
	uint32_t src_ele_count = src->_.ele_count;

	// Insert at end.
	if (dest_idx == src_ele_count) {
		offset_index_copy(dest, src, 0, 0, src_ele_count, 0);
		offset_index_set(dest, src_ele_count, src->content_sz);
	}
	// Insert at offset.
	else {
		offset_index_copy(dest, src, 0, 0, dest_idx + 1, 0);
		offset_index_copy(dest, src, dest_idx + 1, dest_idx,
				src_ele_count - dest_idx, add_sz);
	}

	offset_index_set_filled(dest, dest->_.ele_count);
}

void
offset_index_move_ele(offset_index *dest, const offset_index *src,
		uint32_t ele_idx, uint32_t to_idx)
{
	int32_t delta = dest->content_sz - src->content_sz;

	if (ele_idx == to_idx) {
		offset_index_copy(dest, src, 1, 1, ele_idx, 0);
		offset_index_copy(dest, src, ele_idx + 1, ele_idx + 1,
				src->_.ele_count - ele_idx - 1, delta);
	}
	else if (ele_idx < to_idx) {
		uint32_t sz0 = offset_index_get_delta_const(src, ele_idx);
		uint32_t count = to_idx - ele_idx - 1;

		offset_index_copy(dest, src, 1, 1, ele_idx, 0);

		for (uint32_t i = 0; i < count; i++) {
			uint32_t sz1 = offset_index_get_delta_const(src, ele_idx + i + 1);
			uint32_t value = offset_index_get_const(src, ele_idx + i + 1);

			value -= sz0;
			value += sz1;

			offset_index_set(dest, ele_idx + i + 1, value);
		}

		offset_index_copy(dest, src, to_idx, to_idx, src->_.ele_count - to_idx,
				delta);
	}
	else {
		uint32_t sz0 = offset_index_get_delta_const(src, ele_idx) + delta;
		uint32_t count = ele_idx - to_idx;

		offset_index_copy(dest, src, 1, 1, to_idx, 0);

		for (uint32_t i = 0; i < count; i++) {
			uint32_t sz1 = offset_index_get_delta_const(src, to_idx + i);
			uint32_t value = offset_index_get_const(src, to_idx + i + 1);

			value += sz0;
			value -= sz1;

			offset_index_set(dest, to_idx + i + 1, value);
		}

		offset_index_copy(dest, src, ele_idx + 1, ele_idx + 1,
				src->_.ele_count - ele_idx - 1, delta);
	}

	offset_index_set_filled(dest, dest->_.ele_count);
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
		cdt_find_items_idxs_type find_type, msgpack_in *mp_items,
		order_index *items_ordidx_r, bool inverted, uint64_t *rm_mask,
		uint32_t *rm_count_r, order_index *rm_ranks_r, rollback_alloc *alloc,
		bool exit_early)
{
	bool (*unpack_fn)(msgpack_in *mp, cdt_payload *payload_r);
	uint32_t items_count = items_ordidx_r->_.ele_count;
	define_offset_index(items_offidx, mp_items->buf + mp_items->offset,
			mp_items->buf_sz - mp_items->offset, items_count, alloc);

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
	}

	if (! offset_index_fill(&items_offidx, false, false)) {
		cf_warning(AS_PARTICLE, "offset_index_find_items() invalid parameter list");
		return false;
	}

	bool success = list_order_index_sort(items_ordidx_r, &items_offidx,
			AS_CDT_SORT_ASCENDING);

	cf_assert(success, AS_PARTICLE, "offset_index_find_items() sort failed after index filled");

	uint32_t rm_count = 0;

	msgpack_in mp = {
			.buf = full_offidx->contents,
			.buf_sz = full_offidx->content_sz
	};

	if (rm_ranks_r) {
		cf_assert(! exit_early, AS_PARTICLE, "invalid usage");
		order_index_clear(rm_ranks_r);
	}

	for (uint32_t i = 0; i < full_offidx->_.ele_count; i++) {
		cdt_payload value;

		if (! unpack_fn(&mp, &value)) {
			cf_warning(AS_PARTICLE, "offset_index_find_items() invalid msgpack in unpack_fn()");
			return false;
		}

		if (! offset_index_set_next(full_offidx, i + 1, mp.offset)) {
			cf_warning(AS_PARTICLE, "offset_index_find_items() invalid msgpack in offset_index_set_next() i %u offset %u", i, mp.offset);
			return false;
		}

		order_index_find find = {
				.count = items_count,
				.target = items_count + (rm_ranks_r != NULL ? 0 : 1)
		};

		order_index_find_rank_by_value(items_ordidx_r, &value, &items_offidx,
				&find, false);

		if (rm_ranks_r) {
			if (find.found) {
				uint32_t idx = order_index_get(items_ordidx_r, find.result - 1);

				order_index_incr(rm_ranks_r, (idx * 2) + 1);
			}

			if (find.result != items_count) {
				uint32_t idx = order_index_get(items_ordidx_r, find.result);

				order_index_incr(rm_ranks_r, idx * 2);
			}
		}

		if (! inverted) {
			if (find.found) {
				cdt_idx_mask_set(rm_mask, i);
				rm_count++;

				if (exit_early) {
					*rm_count_r = rm_count;
					return true;
				}
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
	return offidx->_.ele_count <= 1 ?
			0 :  msgpacked_index_size((const msgpacked_index *)offidx);
}

bool
offset_index_is_null(const offset_index *offidx)
{
	return offidx->_.ptr == NULL;
}

bool
offset_index_is_valid(const offset_index *offidx)
{
	return offidx->_.ele_count <= 1 ? true : offidx->_.ptr != NULL;
}

bool
offset_index_is_full(const offset_index *offidx)
{
	if (offidx->_.ele_count <= 1) {
		return true;
	}

	if (offset_index_is_null(offidx)) {
		return false;
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
		print_packed(offidx->contents, offidx->content_sz, "offset_index_get_const() offidx->contents");
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
	if (offidx->_.ele_count <= 1) {
		return 1;
	}

	return msgpacked_index_get((const msgpacked_index *)offidx, 0);
}

bool
offset_index_fill(offset_index *offidx, bool is_map, bool check_storage)
{
	uint32_t start = offset_index_get_filled(offidx);
	uint32_t ele_count = offidx->_.ele_count;
	uint32_t rep = is_map ? 2 : 1;

	if (ele_count <= 1 || start == ele_count) {
		return true;
	}

	if (! offset_index_is_valid(offidx)) {
		return false;
	}

	msgpack_in mp = {
			.buf = offidx->contents,
			.buf_sz = offidx->content_sz,
			.offset = offset_index_get_const(offidx, start - 1)
	};

	for (uint32_t i = start; i < ele_count; i++) {
		if (msgpack_sz_rep(&mp, rep) == 0 ||
				(check_storage && mp.has_nonstorage)) {
			return false;
		}

		offset_index_set(offidx, i, mp.offset);
	}

	offset_index_set_filled(offidx, ele_count);

	return true;
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
offset_index_alloc_temp(offset_index *offidx, uint8_t *mem_temp,
		rollback_alloc *alloc)
{
	if (! offset_index_is_valid(offidx)) {
		uint32_t sz = offset_index_size(offidx);

		offidx->_.ptr = (sz == 0) ? NULL : ((sz > CDT_MAX_STACK_OBJ_SZ) ?
				rollback_alloc_reserve(alloc, sz) : mem_temp);
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

			ptr += sprintf(ptr, "%u, ",
					offset_index_get_delta_const(offidx, i));
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
order_index_init2_temp(order_index *ordidx, uint8_t *mem_temp,
		rollback_alloc *alloc_idx, uint32_t max_idx, uint32_t ele_count)
{
	order_index_init2(ordidx, mem_temp, max_idx, ele_count);
	uint32_t sz = order_index_size(ordidx);

	if (sz > CDT_MAX_STACK_OBJ_SZ) {
		order_index_set_ptr(ordidx, rollback_alloc_reserve(alloc_idx, sz));
	}
	else if (sz == 0) {
		order_index_set_ptr(ordidx, NULL);
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

void
order_index_init_values(order_index *ordidx)
{
	for (uint32_t i = 0; i < ordidx->_.ele_count; i++) {
		order_index_set(ordidx, i, i);
	}
}

bool
order_index_sorted_mark_dup_eles(order_index *ordidx,
		const offset_index *full_offidx, uint32_t *count_r, uint32_t *sz_r)
{
	cf_assert(count_r, AS_PARTICLE, "count_r NULL");
	cf_assert(sz_r, AS_PARTICLE, "sz_r NULL");

	msgpack_in mp = {
			.buf = full_offidx->contents,
			.buf_sz = full_offidx->content_sz
	};

	msgpack_in prev = mp;
	uint32_t prev_idx = order_index_get(ordidx, 0);
	uint32_t ele_count = full_offidx->_.ele_count;

	prev.offset = offset_index_get_const(full_offidx, prev_idx);
	*count_r = 0;
	*sz_r = 0;

	for (uint32_t i = 1; i < ele_count; i++) {
		uint32_t idx = order_index_get(ordidx, i);
		uint32_t off = offset_index_get_const(full_offidx, idx);

		mp.offset = off;

		msgpack_cmp_type cmp = msgpack_cmp(&prev, &mp);

		if (cmp == MSGPACK_CMP_EQUAL) {
			(*sz_r) += offset_index_get_delta_const(full_offidx, idx);
			(*count_r)++;
			order_index_set(ordidx, i, ele_count);
		}
		else if (cmp == MSGPACK_CMP_LESS) {
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
	return ordidx->_.ptr != NULL ? true : (ordidx->max_idx <= 1 ? true : false);
}

bool
order_index_is_filled(const order_index *ordidx)
{
	if (! order_index_is_valid(ordidx)) {
		return false;
	}

	if (ordidx->_.ele_count > 1 &&
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
	if (ordidx->_.ptr != NULL) {
		cf_assert(index < ordidx->_.ele_count, AS_PARTICLE, "index %u >= ele_count %u", index, ordidx->_.ele_count);
		return msgpacked_index_get((const msgpacked_index *)ordidx, index);
	}

	cf_assert(ordidx->max_idx <= 1, AS_PARTICLE, "attempting to access invalid order index");

	return 0;
}

// Find (closest) rank given value.
// Find closest rank for find->idx.
//  target == 0 means find first instance of value.
//  target == ele_count means find last instance of value.
//  target > ele_count means don't check idx.
void
order_index_find_rank_by_value(const order_index *ordidx,
		const cdt_payload *value, const offset_index *full_offidx,
		order_index_find *find, bool skip_key)
{
	uint32_t ele_count = full_offidx->_.ele_count;

	find->found = false;

	if (ele_count == 0 || find->count == 0) {
		find->result = ele_count;
		return;
	}

	uint32_t lower = find->start;
	uint32_t upper = find->start + find->count;
	uint32_t rank = find->start + find->count / 2;

	msgpack_in mp_value = {
			.buf = value->ptr,
			.buf_sz = value->sz
	};

	msgpack_in mp_buf = {
			.buf = full_offidx->contents,
			.buf_sz = full_offidx->content_sz
	};

	while (true) {
		uint32_t idx = ordidx ? order_index_get(ordidx, rank) : rank;

		mp_buf.offset = offset_index_get_const(full_offidx, idx);

		if (skip_key && msgpack_sz(&mp_buf) == 0) { // skip key
			cf_crash(AS_PARTICLE, "invalid packed map");
		}

		msgpack_cmp_type cmp = msgpack_cmp_peek(&mp_value, &mp_buf);

		if (cmp == MSGPACK_CMP_EQUAL) {
			find->found = true;

			if (find->target > ele_count) { // means don't check
				break;
			}

			if (find->target < idx) {
				cmp = MSGPACK_CMP_LESS;
			}
			else if (find->target > idx) {
				if (rank == upper - 1) {
					rank++;
					break;
				}

				cmp = MSGPACK_CMP_GREATER;
			}
			else {
				break;
			}
		}

		if (cmp == MSGPACK_CMP_GREATER) {
			if (rank >= upper - 1) {
				rank++;
				break;
			}

			lower = rank + (find->found ? 0 : 1);
			rank += upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_CMP_LESS) {
			if (rank == lower) {
				break;
			}

			upper = rank;
			rank += lower;
			rank /= 2;
		}
		else {
			print_packed(mp_value.buf, mp_value.buf_sz, "mp_value");
			print_packed(mp_buf.buf, mp_buf.buf_sz, "mp_buf");
			cf_crash(AS_PARTICLE, "invalid element offset %u idx %u rank %u start %u count %u ele_count %u", mp_buf.offset, idx, rank, find->start, find->count, ele_count);
		}
	}

	find->result = rank;
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
		const offset_index *full_offidx, uint8_t *buf, offset_index *new_offidx,
		bool invert)
{
	if (new_offidx != NULL && offset_index_is_null(new_offidx)) {
		new_offidx = NULL;
	}

	cf_assert(! invert || new_offidx == NULL, AS_PARTICLE, "unsupported: only set 1 of {new_offidx, inverted}");

	uint32_t start = 0;
	uint32_t buf_off = 0;
	uint32_t write_count = 0;

	if (new_offidx != NULL) {
		offset_index_set_filled(new_offidx, full_offidx->_.ele_count);
	}

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		if (idx == ordidx->max_idx) {
			continue;
		}

		uint32_t offset = offset_index_get_const(full_offidx, idx);
		uint32_t sz = offset_index_get_delta_const(full_offidx, idx);

		if (! invert) {
			memcpy(buf, full_offidx->contents + offset, sz);
			buf_off += sz;
			buf += sz;
		}
		else {
			uint32_t invert_sz = offset - start;

			if (invert_sz != 0) {
				memcpy(buf, full_offidx->contents + start, invert_sz);
				buf_off += invert_sz;
				buf += invert_sz;
			}

			start = offset + sz;
		}

		if (new_offidx == NULL) {
			continue;
		}

		write_count++;

		if (! new_offidx->is_partial) {
			offset_index_set(new_offidx, write_count, buf_off);
		}
		else if (write_count % PACKED_LIST_INDEX_STEP == 0 &&
				new_offidx->_.ele_count != 0) {
			uint32_t new_idx = write_count / PACKED_LIST_INDEX_STEP;

			offset_index_set(new_offidx, new_idx, buf_off);
		}
	}

	if (! invert) {
		if (new_offidx != NULL) {
			offset_index_set_filled(new_offidx, (new_offidx->is_partial ?
					new_offidx->_.ele_count : write_count));
		}

		return buf;
	}

	uint32_t invert_sz = full_offidx->content_sz - start;

	memcpy(buf, full_offidx->contents + start, invert_sz);

	return buf + invert_sz;
}

bool
order_index_check_order(const order_index *ordidx,
		const offset_index *full_offidx)
{
	uint32_t ele_count = full_offidx->_.ele_count;
	uint32_t idx = order_index_get(ordidx, 0);

	if (ele_count <= 1) {
		return true;
	}

	if (idx >= ordidx->max_idx) {
		return false;
	}

	uint32_t offset = offset_index_get_const(full_offidx, idx);
	uint32_t sz = offset_index_get_delta_const(full_offidx, idx);

	msgpack_in prev = {
			.buf = full_offidx->contents + offset,
			.buf_sz = sz
	};

	for (uint32_t i = 1; i < ele_count; i++) {
		idx = order_index_get(ordidx, i);

		if (idx >= ordidx->max_idx) {
			return false;
		}

		offset = offset_index_get_const(full_offidx, idx);
		sz = offset_index_get_delta_const(full_offidx, idx);

		msgpack_in mp = {
				.buf = full_offidx->contents + offset,
				.buf_sz = sz
		};

		msgpack_cmp_type cmp = msgpack_cmp(&prev, &mp);

		switch (cmp) {
		case MSGPACK_CMP_LESS:
		case MSGPACK_CMP_EQUAL:
			break;
		case MSGPACK_CMP_GREATER:
			return false;
		case MSGPACK_CMP_ERROR:
		case MSGPACK_CMP_END:
		default:
			return false;
		}

		prev = mp;
	}

	return true;
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
	if (count == 0) {
		return;
	}

	if (src->_.ptr == NULL && ! adjust) {
		cf_assert(src->_.ele_count == 1 && count == 1, AS_PARTICLE, "null src offidx");
		cf_assert(s_start == 0, AS_PARTICLE, "invalid s_start %u", s_start);
		order_index_set(dest, d_start, 0);
	}
	else if (dest->_.ele_sz == src->_.ele_sz && ! adjust) {
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
order_heap_init_build_by_range_temp(order_heap *heap, uint8_t *heap_mem,
		rollback_alloc *alloc_idx, uint32_t idx, uint32_t count,
		uint32_t ele_count, order_heap_compare_fn cmp_fn, const void *udata)
{
	uint32_t tail_distance = ele_count - idx - count;
	uint32_t discard;
	msgpack_cmp_type cmp;

	if (idx <= tail_distance) {
		cmp = MSGPACK_CMP_LESS; // min k
		discard = idx;
	}
	else {
		cmp = MSGPACK_CMP_GREATER; // max k
		discard = tail_distance;
	}

	order_index_init2_temp(&heap->_, heap_mem, alloc_idx, ele_count, ele_count);
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
			msgpack_cmp_type cmp = heap->cmp_fn(heap->userdata,
					order_heap_get(heap, child1),
					order_heap_get(heap, child2));

			if (cmp == MSGPACK_CMP_ERROR) {
				return false;
			}

			if (cmp == heap->cmp || cmp == MSGPACK_CMP_EQUAL) {
				child = child1;
			}
			else {
				child = child2;
			}
		}

		msgpack_cmp_type cmp = heap->cmp_fn(heap->userdata,
				order_heap_get(heap, child),
				order_heap_get(heap, index));

		if (cmp == MSGPACK_CMP_ERROR) {
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
	cf_assert(count <= heap->filled, AS_PARTICLE, "count %u > heap_filled %u", count, heap->filled);

	uint32_t end_index = heap->filled - 1;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t value = order_heap_get(heap, 0);

		if (! order_heap_remove_top(heap)) {
			return false;
		}

		order_heap_set(heap, end_index--, value);
	}

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

void
cdt_idx_mask_init_temp(uint64_t **mask, uint32_t ele_count,
		rollback_alloc *alloc)
{
	uint32_t sz = cdt_idx_mask_count(ele_count) * sizeof(uint64_t);

	if (sz > CDT_MAX_STACK_OBJ_SZ) {
		*mask = (uint64_t *)rollback_alloc_reserve(alloc, sz);
	}

	memset(*mask, 0, sz);
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

		if (idx == ele_count) {
			print_packed(full_offidx->contents, full_offidx->content_sz, "full_offidx->contents");
			cdt_idx_mask_print(mask, ele_count, "mask");
			offset_index_print(full_offidx, "full_offidx");
			cf_crash(AS_PARTICLE, "count %u ele_count %u", count, ele_count);
		}

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
list_param_parse(const cdt_payload *items, msgpack_in *mp, uint32_t *count_r)
{
	mp->buf = items->ptr;
	mp->offset = 0;
	mp->buf_sz = items->sz;

	if (! msgpack_get_list_ele_count(mp, count_r) ||
			*count_r > CDT_MAX_PARAM_LIST_COUNT) {
		cf_warning(AS_PARTICLE, "list_param_parse() invalid param items hdr count %u", *count_r);
		return false;
	}

	return true;
}


//==========================================================
// cdt_untrusted
//

uint32_t
cdt_untrusted_get_size(const uint8_t *buf, uint32_t buf_sz, msgpack_type *ptype,
		bool has_toplvl)
{
	if (buf_sz == 0) {
		return 0; // error
	}

	const uint8_t *next_b = buf;
	const uint8_t *end = buf + buf_sz;
	uint32_t count = 1;
	uint8_t top_flags = 0;
	uint32_t ret_sz = 0;
	uint32_t top_ele_count = 0; // set to 0 to shut the compiler up
	msgpack_type dummy_type;

	if (ptype == NULL) {
		ptype = &dummy_type;
	}

	for (uint32_t i = 0; i < count; i++) {
		const uint8_t *b = next_b;
		uint32_t old_count = count;
		msgpack_type type;
		bool has_nonstorage = false;
		bool not_compact = false;

		next_b = msgpack_parse(b, end, &count, &type, &has_nonstorage,
				&not_compact);

		uint32_t ele_count = count - old_count;
		uint32_t parse_sz = (uint32_t)(next_b - b);

		if (has_nonstorage || next_b == NULL) {
			cf_warning(AS_PARTICLE, "invalid msgpack: has_nonstorage %d b %p", has_nonstorage, b);
			return 0;
		}

		if (type == MSGPACK_TYPE_MAP) {
			ele_count /= 2;
		}

		if (i == 0) {
			top_ele_count = ele_count;
			*ptype = type;
		}

		if (old_count == count || msgpack_buf_peek_type(next_b, end - next_b) !=
				MSGPACK_TYPE_EXT) {
			if (not_compact) {
				ret_sz += msgpack_compactify_element(NULL, b);
			}
			else {
				ret_sz += parse_sz;
			}

			continue;
		}

		msgpack_ext ext;
		uint32_t ext_sz = msgpack_buf_get_ext(next_b, end - next_b, &ext);

		if (ext_sz == 0) {
			cf_warning(AS_PARTICLE, "invalid msgpack: b %lx", *(uint64_t*)b);
			return 0;
		}

		next_b += ext_sz;
		count--; // ext element was parsed

		if (i == 0 && has_toplvl) {
			top_flags = ext.type;
		}
		else {
			ext.type &= ~AS_PACKED_PERSIST_INDEX;
		}

		if (type == MSGPACK_TYPE_MAP) {
			msgpack_type temp_type;
			uint32_t temp_count = 1;

			next_b = msgpack_parse(next_b, end, &temp_count, &temp_type,
					&has_nonstorage, &not_compact);
			count--; // meta-pair 2nd element skipped
			ext.type &= AS_PACKED_PERSIST_INDEX | AS_PACKED_MAP_FLAG_KV_ORDERED;

			if (next_b == NULL) {
				cf_warning(AS_PARTICLE, "invalid msgpack");
				return 0;
			}

			if (ext.type == 0) {
				ret_sz += as_pack_map_header_get_size(ele_count - 1);
			}
			else {
				ret_sz += as_pack_map_header_get_size(ele_count);
				ret_sz += as_pack_ext_header_get_size(0);
				ret_sz += as_pack_nil_size();
			}
		}
		else { // LIST
			ext.type &= AS_PACKED_PERSIST_INDEX | AS_PACKED_LIST_FLAG_ORDERED;

			if (ext.type == 0) {
				ret_sz += as_pack_list_header_get_size(ele_count - 1);
			}
			else {
				ret_sz += as_pack_list_header_get_size(ele_count);
				ret_sz += as_pack_ext_header_get_size(0);
			}
		}
	}

	if (flags_is_persist(top_flags)) {
		uint32_t content_sz = ret_sz - as_pack_ext_header_get_size(0);
		uint32_t ext_content_sz;

		if (*ptype == MSGPACK_TYPE_MAP) {
			content_sz -= as_pack_map_header_get_size(top_ele_count);
			content_sz -= as_pack_nil_size();
			ext_content_sz = map_calc_ext_content_sz(top_flags,
					top_ele_count - 1, content_sz);
		}
		else { // LIST
			content_sz -= as_pack_list_header_get_size(top_ele_count);
			ext_content_sz = list_calc_ext_content_sz(top_flags,
					top_ele_count - 1, content_sz);
		}

		ret_sz -= as_pack_ext_header_get_size(0);
		ret_sz += as_pack_ext_header_get_size(ext_content_sz);
		ret_sz += ext_content_sz;
	}

	return ret_sz;
}

static cdt_stack_entry *
cdt_stack_get_entry(cdt_stack *cs)
{
	return &cs->entries[cs->ilevel];
}

static cdt_stack_entry *
cdt_stack_incr_level(cdt_stack *cs)
{
	cs->ilevel++;

	if (cs->ilevel >= cs->entries_cap) {
		cs->entries_cap *= 2;

		size_t new_sz = sizeof(cdt_stack_entry) * cs->entries_cap;

		if (cs->entries == cs->entries0) {
			cs->entries = cf_malloc(new_sz);
			memcpy(cs->entries, cs->entries0, sizeof(cs->entries0));
		}
		else {
			cs->entries = cf_realloc(cs->entries, new_sz);
		}
	}

	return cdt_stack_get_entry(cs);
}

static cdt_stack_entry *
cdt_stack_decr_level(cdt_stack *cs)
{
	cf_assert(cs->ilevel != 0, AS_PARTICLE, "ilevel == 0");
	cs->ilevel--;

	return &cs->entries[cs->ilevel];
}

static uint32_t
cdt_stack_untrusted_rewrite(cdt_stack *cs, uint8_t *dest, const uint8_t *src,
		uint32_t src_sz)
{
	uint8_t *wptr = dest;
	const uint8_t *b = src;
	const uint8_t *end = src + src_sz;
	uint32_t count = 1;
	bool has_nonstorage = false;

	if (src_sz == 0) {
		return 0;
	}

	for (uint32_t i = 0; i < count; i++) {
		bool not_compact = false;
		uint32_t old_count = count;
		msgpack_type type;
		const uint8_t *next_b = msgpack_parse(b, end, &count, &type,
				&has_nonstorage, &not_compact);
		uint32_t parse_sz = next_b - b;
		uint32_t ele_count = count - old_count;

		if (i == 0) {
			cs->toplvl_type = type;
		}

		if (type == MSGPACK_TYPE_MAP) {
			ele_count /= 2;
		}

		if (has_nonstorage || next_b == NULL) {
			cf_detail(AS_PARTICLE, "untrusted_rewrite() has_nonstorage %d b %p sz %u i %u", has_nonstorage, b,
					(uint32_t)(end - b), i);
			return 0;
		}

		cdt_stack_entry *pe = cdt_stack_get_entry(cs);
		bool do_incr_ix = (i != 0);

		as_packer pk = {
				.buffer = wptr,
				.capacity = UINT32_MAX
		};

		if (type != MSGPACK_TYPE_LIST && type != MSGPACK_TYPE_MAP) {
			if (not_compact) {
				uint32_t new_sz = msgpack_compactify_element(wptr, b);

				pk.offset += new_sz;
			}
			else { // TODO - optimize for fewer memcpy call(s)
				as_pack_append(&pk, b, parse_sz);
			}

			if (i == 0) {
				b = next_b;
				wptr += pk.offset;
				break;
			}
		}
		else if (ele_count == 0) { // empty list/map
			switch (type) {
			case MSGPACK_TYPE_LIST:
				as_pack_list_header(&pk, 0);
				break;
			case MSGPACK_TYPE_MAP:
				as_pack_map_header(&pk, 0);
				break;
			default:
				break;
			}

			if (i == 0) {
				b = next_b;
				wptr += pk.offset;
				break;
			}
		}
		else { // non-empty list/map
			if (i != 0) {
				pe = cdt_stack_incr_level(cs);
			}

			uint32_t tail_sz = (uint32_t)(end - next_b);

			pe->ix = 0;
			do_incr_ix = false;

			if (msgpack_buf_peek_type(next_b, tail_sz) == MSGPACK_TYPE_EXT) {
				msgpack_ext ext;
				uint32_t ext_sz = msgpack_buf_get_ext(next_b, tail_sz, &ext);

				if (ext_sz == 0) {
					cf_warning(AS_PARTICLE, "invalid msgpack: b %lx", *(uint64_t*)b);
					return 0;
				}

				next_b += ext_sz;
				ele_count--;

				if (i != 0 || ! cs->has_toplvl) {
					// Quietly ignore when asking to persist index at sub-level.
					ext.type &= ~AS_PACKED_PERSIST_INDEX;
				}

				if (type == MSGPACK_TYPE_MAP) { // parse 2nd meta element for maps
					msgpack_type next_type;
					uint32_t temp_count = 0;

					count--;
					next_b = msgpack_parse(next_b, end, &temp_count, &next_type,
							&has_nonstorage, &not_compact);
					tail_sz = (uint32_t)(end - next_b);
					ext.type &= AS_PACKED_MAP_FLAG_KV_ORDERED |
							AS_PACKED_PERSIST_INDEX;
					as_pack_map_header(&pk,
							ele_count + (ext.type == 0 ? 0 : 1));

					if (ele_count != 0 && ! map_is_key(next_b, tail_sz)) {
						cf_warning(AS_PARTICLE, "map has invalid key type");
						return 0;
					}
				}
				else { // LIST
					ext.type &= AS_PACKED_LIST_FLAG_ORDERED |
							AS_PACKED_PERSIST_INDEX;
					as_pack_list_header(&pk,
							ele_count + (ext.type == 0 ? 0 : 1));
				}

				uint32_t est_content_sz = end - next_b; // maybe inaccurate due to padding

				if (flags_is_persist(ext.type)) {
					uint32_t ext_content_sz;

					if (type == MSGPACK_TYPE_MAP) {
						ext_content_sz = map_calc_ext_content_sz(ext.type,
								ele_count, est_content_sz);
					}
					else {
						ext_content_sz = list_calc_ext_content_sz(ext.type,
								ele_count, est_content_sz);
					}

					pe->ext_start = pk.buffer + pk.offset;
					as_pack_ext_header(&pk, ext_content_sz, ext.type);

					uint8_t *idx_mem = pk.buffer + pk.offset;

					pk.offset += ext_content_sz;

					if (type == MSGPACK_TYPE_MAP) {
						as_pack_nil(&pk);
					}

					uint8_t *contents = pk.buffer + pk.offset;

					if (type == MSGPACK_TYPE_LIST &&
							ext.type == AS_PACKED_PERSIST_INDEX) {
						// Set partial indexes to empty state.
						list_partial_offset_index_init(&pe->offidx, idx_mem,
								ele_count, contents, est_content_sz);
						offset_index_set_filled(&pe->offidx, 1);
						offset_index_set_ptr(&pe->offidx, NULL, NULL);
					}
					else {
						offset_index_init(&pe->offidx, idx_mem, ele_count,
								contents, est_content_sz);
						offset_index_set_filled(&pe->offidx, ele_count);
					}

					pe->ext_content_sz = ext_content_sz;
				}
				else { // not persist
					if (ext.type != 0) {
						pe->ext_start = wptr;
						as_pack_ext_header(&pk, 0, ext.type);

						if (type == MSGPACK_TYPE_MAP) {
							as_pack_nil(&pk);
						}
					}

					offset_index_init(&pe->offidx, NULL, ele_count,
							pk.buffer + pk.offset, est_content_sz);
				}

				pe->new_contents = pk.buffer + pk.offset;
				pe->ext_type = ext.type;
				count--;
			}
			else { // ! MSGPACK_TYPE_EXT
				if (type == MSGPACK_TYPE_MAP) {
					as_pack_map_header(&pk, ele_count);
					pe->ext_type = 0;

					if (ele_count != 0 && ! map_is_key(next_b, tail_sz)) {
						cf_warning(AS_PARTICLE, "map has invalid key type");
						return 0;
					}
				}
				else { // list
					as_pack_list_header(&pk, ele_count);
					pe->ext_type = 0;
				}

				pe->new_contents = pk.buffer + pk.offset;
				offset_index_init(&pe->offidx, NULL, ele_count,
						pe->new_contents, end - b);
			}

			if (ele_count == 0) {
				if (i != 0) {
					pe = cdt_stack_decr_level(cs); // no indexes to handle for this case
					do_incr_ix = true;
				}
			}
			else {
				pe->type = (uint8_t)type;
				pe->n_msgpack = ele_count *
						((type == MSGPACK_TYPE_MAP) ? 2 : 1);
				pe->prev.buf = next_b;
				pe->prev.buf_sz = UINT32_MAX;
				pe->need_sort = false;
			}
		}

		b = next_b;
		wptr += pk.offset;

		while (do_incr_ix) {
			uint32_t offset = (uint32_t)(wptr - pe->new_contents);

			ele_count = pe->offidx._.ele_count;
			pe->ix++;

			if (pe->ix >= pe->n_msgpack) {
				if (pe->need_sort) {
					define_rollback_alloc(alloc_idx, NULL, 2);
					define_order_index(ordidx, ele_count, alloc_idx);
					define_offset_index(new_offidx, pe->new_contents, offset,
							ele_count, alloc_idx);

					if (pe->type == MSGPACK_TYPE_LIST) {
						// TODO - track list sorting
						if (! offset_index_is_valid(&pe->offidx)) {
							list_full_offset_index_fill_all(&new_offidx);
						}
						else {
							offset_index_copy(&new_offidx, &pe->offidx, 0, 0,
									ele_count, 0);
						}

						if (! list_order_index_sort(&ordidx, &new_offidx,
								AS_CDT_SORT_ASCENDING)) {
							cf_crash(AS_PARTICLE, "unexpected");
						}
					}
					else {
						// TODO - track map sorting
						if (! offset_index_is_valid(&pe->offidx)) {
							map_offset_index_check_and_fill(&new_offidx,
									ele_count);
						}
						else {
							offset_index_copy(&new_offidx, &pe->offidx, 0, 0,
									ele_count, 0);
						}

						map_order_index_set_sorted(&ordidx, &new_offidx,
								pe->new_contents, pe->offidx.content_sz,
								MAP_SORT_BY_KEY);

						if (map_order_index_has_dups(&ordidx, &new_offidx)) {
							cf_warning(AS_PARTICLE, "map has duplicate keys");
							return 0;
						}
					}

					uint8_t *sort_contents = cf_malloc(offset);

					wptr = order_index_write_eles(&ordidx, ele_count,
							&new_offidx, sort_contents, &pe->offidx, false);
					cf_assert(wptr - sort_contents == offset, AS_PARTICLE, "write mismatch %lu != %u", wptr - sort_contents, offset);
					memcpy(pe->new_contents, sort_contents, offset);
					wptr = pe->new_contents + offset;
					cf_free(sort_contents);
					rollback_alloc_rollback(alloc_idx);
				}

				uint8_t ext_type_pkv = AS_PACKED_PERSIST_INDEX |
						AS_PACKED_MAP_FLAG_KV_ORDERED;

				if (pe->type == MSGPACK_TYPE_MAP &&
						(pe->ext_type & ext_type_pkv) == ext_type_pkv) { // has order_index
					uint8_t *ordidx_ptr = pe->offidx._.ptr +
							offset_index_size(&pe->offidx);
					order_index ordidx;
					bool ord_need_sort = true;

					order_index_init(&ordidx, ordidx_ptr, ele_count);

					if (order_index_is_filled(&ordidx)) {
						ord_need_sort = ! order_index_check_order(&ordidx,
								&pe->offidx);
					}

					if (ord_need_sort) {
						map_order_index_set_sorted(&ordidx, &pe->offidx,
								pe->new_contents, pe->offidx.content_sz,
								MAP_SORT_BY_VALUE);
					}
				}

				if (pe->offidx.content_sz != offset &&
						offset_index_is_valid(&pe->offidx)) {
					wptr = shrink_ext_offidx(pe->ext_start, wptr, ele_count,
							pe->offidx.content_sz, offset);
				}

				if (cs->ilevel == 0) {
					if (b != end) {
						cf_warning(AS_PARTICLE, "list/map rejected padding size %lu != 0", end - b);
						return 0;
					}

					break;
				}
				else {
					pe = cdt_stack_decr_level(cs);
				}

				continue;
			}

			uint32_t idx = pe->ix;
			bool is_ele_key = true;
			bool check_ordered = false;

			if (pe->type == MSGPACK_TYPE_MAP) {
				idx /= 2;
				check_ordered = ! pe->need_sort;

				if (pe->ix % 2 != 0) {
					is_ele_key = false;
				}
				else if (! map_is_key(b, end - b)) {
					cf_warning(AS_PARTICLE, "map has invalid key type");
					return 0;
				}
			}
			else if (pe->type == MSGPACK_TYPE_LIST) {
				if ((pe->ext_type & AS_PACKED_LIST_FLAG_ORDERED) != 0) {
					check_ordered = true;
				}
			}

			if (check_ordered && is_ele_key) {
				pe->prev.offset = 0;

				msgpack_in mp = {
						.buf = b,
						.buf_sz = UINT32_MAX
				};

				msgpack_cmp_type cmp = msgpack_cmp(&pe->prev, &mp);

				switch (cmp) {
				case MSGPACK_CMP_LESS:
					break;
				case MSGPACK_CMP_EQUAL:
					if (pe->type == MSGPACK_TYPE_MAP) {
						cf_warning(AS_PARTICLE, "map has duplicate keys");
						return 0;
					}
					break;
				case MSGPACK_CMP_GREATER:
					switch (pe->type) {
					case MSGPACK_TYPE_LIST:
						if ((pe->ext_type & AS_PACKED_LIST_FLAG_ORDERED) != 0) {
							cf_warning(AS_PARTICLE, "list not ordered as expected");
							return 0;
						}
						break;
					case MSGPACK_TYPE_MAP:
						if ((pe->ext_type & AS_PACKED_MAP_FLAG_K_ORDERED) !=
								0) {
							cf_warning(AS_PARTICLE, "map not ordered as expected");
							return 0;
						}
						break;
					}

					pe->need_sort = true;
					break;
				case MSGPACK_CMP_ERROR:
				case MSGPACK_CMP_END:
				default:
					cf_crash(AS_PARTICLE, "unexpected %d", cmp);
				}

				pe->prev = mp;
			}

			if (offset_index_is_valid(&pe->offidx) && is_ele_key) {
				offset_index_set(&pe->offidx, idx, offset);
			}

			break;
		}
	}

	if (b != end) {
		cf_warning(AS_PARTICLE, "list/map rejected padding size %lu != 0", end - b);
		return 0;
	}

	return wptr - dest;
}

uint32_t
cdt_untrusted_rewrite(uint8_t *dest, const uint8_t *src, uint32_t src_sz,
		bool has_toplvl)
{
	cdt_stack cs;

	cs.entries = cs.entries0;
	cs.entries_cap = sizeof(cs.entries0) / sizeof(cdt_stack_entry);
	cs.ilevel = 0;
	cs.entries->n_msgpack = 1;
	cs.entries->ix = 0;
	cs.has_toplvl = has_toplvl;

	uint32_t ret = cdt_stack_untrusted_rewrite(&cs, dest, src, src_sz);

	if (cs.entries != cs.entries0) {
		cf_free(cs.entries);
	}

	return ret;
}


//==========================================================
// cdt_check
//

bool
cdt_check_flags(uint8_t flags, msgpack_type type)
{
	if (type == MSGPACK_TYPE_LIST) {
		uint8_t valid = AS_PACKED_LIST_FLAG_ORDERED | AS_PACKED_PERSIST_INDEX;
		return (flags & ~valid) == 0;
	}
	else if (type == MSGPACK_TYPE_MAP) {
		uint8_t valid = AS_PACKED_MAP_FLAG_KV_ORDERED | AS_PACKED_PERSIST_INDEX;
		return (flags & ~valid) == 0;
	}

	return false;
}


//==========================================================
// display
//

const char *
cdt_exp_display_name(as_cdt_optype op)
{
	const char* name = NULL;

	if ((uint32_t)op < n_cdt_exp_display_names) { // (uint32_t) cast because enum can be signed
		name = cdt_exp_display_names[op];
	}

	return name != NULL ? name : "INVALID_CDT_OP";
}

bool
cdt_ctx_to_dynbuf(const uint8_t *ctx, uint32_t ctx_sz, cf_dyn_buf *db)
{
	msgpack_in mp = {
			.buf = ctx,
			.buf_sz = ctx_sz
	};

	return cdt_msgpack_ctx_to_dynbuf(&mp, db);
}

bool
cdt_msgpack_ctx_to_dynbuf(msgpack_in *mp, cf_dyn_buf *db)
{
	static const char *ctx_names[] = {
			[AS_CDT_CTX_INDEX] = "index",
			[AS_CDT_CTX_RANK] = "rank",
			[AS_CDT_CTX_KEY] = "key",
			[AS_CDT_CTX_VALUE] = "value"
	};

	uint32_t ele_count;

	if (! msgpack_get_list_ele_count(mp, &ele_count) || (ele_count & 1) != 0) {
		return false;
	}

	cf_dyn_buf_append_string(db, "[");

	for (uint32_t i = 0; i < ele_count / 2; i++) {
		int64_t ctx_type;

		if (! msgpack_get_int64(mp, &ctx_type)) {
			return false;
		}

		uint8_t table_i = (uint8_t)ctx_type & AS_CDT_CTX_BASE_MASK;

		if (table_i >= AS_CDT_MAX_CTX) {
			return false;
		}

		if (i != 0) {
			cf_dyn_buf_append_string(db, ", ");
		}

		if ((ctx_type & AS_CDT_CTX_LIST) != 0) {
			cf_dyn_buf_append_string(db, "list_");
		}
		else if ((ctx_type & AS_CDT_CTX_MAP) != 0) {
			cf_dyn_buf_append_string(db, "map_");
		}

		cf_dyn_buf_append_string(db, ctx_names[table_i]);

		msgpack_display_str s;

		if (! msgpack_display(mp, &s)) {
			return false;
		}

		cf_dyn_buf_append_format(db, "(%s)", s.str);
	}

	cf_dyn_buf_append_string(db, "]");
	return true;
}


//==========================================================
// Debugging support.
//

bool
cdt_verify(cdt_context *ctx)
{
	cf_assert(ctx != NULL, AS_PARTICLE, "ctx NULL");

	if (! as_bin_is_live(ctx->b)) {
		return true;
	}

	uint8_t type = as_bin_get_particle_type(ctx->b);

	if (type == AS_PARTICLE_TYPE_LIST) {
		return list_verify(ctx);
	}
	else if (type == AS_PARTICLE_TYPE_MAP) {
		return map_verify(ctx);
	}

	cf_warning(AS_PARTICLE, "cdt_verify() non-cdt type: %u", type);
	return false;
}

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
			line_sz = sz - i * limit;
		}

		print_hex(packed + limit * i, line_sz, mem, sizeof(mem));
		cf_warning(AS_PARTICLE, "%s:%0X: [%s]", name, i, mem);
	}
}

void
cdt_bin_print(const as_bin *b, const char *name)
{
	const cdt_mem *p = (const cdt_mem *)b->particle;
	uint8_t bintype = as_bin_get_particle_type(b);

	if (! p || (bintype != AS_PARTICLE_TYPE_MAP &&
			bintype != AS_PARTICLE_TYPE_LIST)) {
		cf_warning(AS_PARTICLE, "%s: particle NULL type %u", name, bintype);
		return;
	}

	cf_warning(AS_PARTICLE, "%s: btype %u data=%p sz=%u type=%d", name, bintype, p->data, p->sz, p->type);
	print_packed(p->data, p->sz, name);
}

void
cdt_context_print(const cdt_context *ctx, const char *name)
{
	cf_warning(AS_PARTICLE, "cdt_context: offset %u sz %u bin_type %d delta_off %d delta_sz %d", ctx->data_offset, ctx->data_sz, as_bin_get_particle_type(ctx->b), ctx->delta_off, ctx->delta_sz);

	const cdt_mem *p = (const cdt_mem *)ctx->b->particle;
	const cdt_mem *orig = (const cdt_mem *)ctx->orig;

	if (orig != NULL) {
		print_packed(orig->data, orig->sz, "ctx->orig");
	}

	if (p == NULL) {
		print_packed(NULL, 0, name);
		cf_warning(AS_PARTICLE, "cdt_mem: %p sz %u", p, 0);
	}
	else {
		print_packed(p->data, p->sz, name);
		cf_warning(AS_PARTICLE, "cdt_mem: %p sz %u", p, p->sz);
	}
}
