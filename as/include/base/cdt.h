/*
 * cdt.h
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

#pragma once

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "base/datamodel.h"
#include "base/proto.h"

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

#define CDT_MAX_PACKED_INT_SZ (sizeof(uint64_t) + 1)
#define CDT_MAX_STACK_OBJ_SZ  (1024 * 1024)
#define CDT_MAX_PARAM_LIST_COUNT (1024 * 1024)

typedef struct rollback_alloc_s {
	cf_ll_buf *ll_buf;
	size_t malloc_list_sz;
	size_t malloc_list_cap;
	bool malloc_ns;
	void *malloc_list[];
} rollback_alloc;

#define define_rollback_alloc(__name, __alloc_buf, __rollback_size, __malloc_ns) \
	uint8_t __name ## __mem[sizeof(rollback_alloc) + sizeof(void *) * (__alloc_buf ? 0 : __rollback_size)]; \
	rollback_alloc *__name = (rollback_alloc *)__name ## __mem; \
	__name->ll_buf = __alloc_buf; \
	__name->malloc_list_sz = 0; \
	__name->malloc_list_cap = (__alloc_buf ? 0 : __rollback_size); \
	__name->malloc_ns = __malloc_ns;

typedef struct cdt_process_state_s {
	as_cdt_optype type;
	as_unpacker pk;
	uint32_t ele_count;
} cdt_process_state;

typedef struct cdt_payload_s {
	const uint8_t *ptr;
	uint32_t sz;
} cdt_payload;

typedef struct result_data_s {
	as_bin *result;
	rollback_alloc *alloc;
	result_type_t type;
	as_cdt_op_flags flags;
	bool is_multi;
} cdt_result_data;

typedef struct cdt_modify_data_s {
	as_bin *b;
	as_bin *result;
	cf_ll_buf *alloc_buf;

	int ret_code;
} cdt_modify_data;

typedef struct cdt_read_data_s {
	const as_bin *b;
	as_bin *result;

	int ret_code;
} cdt_read_data;

typedef struct cdt_container_builder_s {
	as_particle *particle;
	uint8_t *write_ptr;
	uint32_t *sz;
	uint32_t ele_count;
} cdt_container_builder;

typedef struct cdt_op_table_entry_s {
	uint32_t count;
	uint32_t opt_args;
	const char *name;
	const as_cdt_paramtype *args;
} cdt_op_table_entry;

typedef struct cdt_calc_delta_s {
	int64_t incr_int;
	double incr_double;

	as_val_t type;

	int64_t value_int;
	double value_double;
} cdt_calc_delta;

typedef struct msgpacked_index_s {
	uint8_t *ptr;
	uint32_t ele_sz;
	uint32_t ele_count;
} msgpacked_index;

typedef struct offset_index_s {
	msgpacked_index _;

	const uint8_t *contents;
	uint32_t content_sz;
	bool is_partial;
} offset_index;

// Value order index.
typedef struct order_index_s {
	msgpacked_index _;
	uint32_t max_idx;
} order_index;

typedef struct order_index_find_s {
	uint32_t start;
	uint32_t count;
	uint32_t target;
	uint32_t result;
	bool found;
} order_index_find;

typedef msgpack_compare_t (*order_heap_compare_fn)(const void *ptr, uint32_t index0, uint32_t index1);

// Value order heap.
typedef struct order_heap_s {
	order_index _;
	const void *userdata;
	order_heap_compare_fn cmp_fn;
	msgpack_compare_t cmp;
	uint32_t filled;
} order_heap;

typedef struct cdt_packed_op_s {
	// Input.
	const uint8_t *packed;
	uint32_t packed_sz;

	// Parsed.
	uint32_t ele_count;
	const uint8_t *contents;
	uint32_t content_sz;

	// Result.
	uint32_t new_ele_count;
} cdt_packed_op;

struct order_index_adjust_s;
typedef uint32_t (*order_index_adjust_func)(const struct order_index_adjust_s *via, uint32_t src);

typedef struct order_index_adjust_s {
	order_index_adjust_func f;
	uint32_t upper;
	uint32_t lower;
	int32_t delta;
} order_index_adjust;

typedef enum {
	CDT_FIND_ITEMS_IDXS_FOR_LIST_VALUE,
	CDT_FIND_ITEMS_IDXS_FOR_MAP_KEY,
	CDT_FIND_ITEMS_IDXS_FOR_MAP_VALUE
} cdt_find_items_idxs_type;

#define cdt_vla_sz(__in_sz) ((__in_sz) > CDT_MAX_STACK_OBJ_SZ ? 0 : (__in_sz))

#define define_offset_index(__name, __contents, __content_sz, __ele_count) \
		offset_index __name; \
		offset_index_init(&__name, NULL, __ele_count, __contents, __content_sz); \
		uint8_t __name ## __offset_index_mem__[offset_index_vla_sz(&__name)]; \
		offset_index_alloc_temp(&__name, __name ## __offset_index_mem__)

#define cond_vla_order_index2(__name, __max_idx, __alloc_count, __cond) \
		union { \
			order_index ordidx; \
			uint8_t mem_temp[sizeof(order_index) + ((__cond) ? cdt_vla_sz(order_index_calc_size(__max_idx, __alloc_count)) : 0)]; \
		} __name; \
		order_index_init2_temp(&__name.ordidx, __name.mem_temp + sizeof(order_index), __max_idx, __alloc_count)

#define define_order_index(__name, __ele_count) \
		order_index __name; \
		uint8_t __name ## __order_index_mem__[cdt_vla_sz(order_index_calc_size(__ele_count, __ele_count))]; \
		order_index_init2_temp(&__name, __name ## __order_index_mem__, __ele_count, __ele_count)

#define define_order_index2(__name, __max_idx, __alloc_count) \
		order_index __name; \
		uint8_t __name ## __order_index_mem__[cdt_vla_sz(order_index_calc_size(__max_idx, __alloc_count))]; \
		order_index_init2_temp(&__name, __name ## __order_index_mem__, __max_idx, __alloc_count)

#define define_int_list_builder(__name, __alloc, __count) \
		cdt_container_builder __name; \
		cdt_int_list_builder_start(&__name, __alloc, __count)

#define define_cdt_idx_mask(__name, __ele_count) \
		uint64_t __name[cdt_idx_mask_count(__ele_count)]; \
		cdt_idx_mask_init(__name, __ele_count)

#define cond_define_cdt_idx_mask(__name, __ele_count, __cond) \
		uint64_t __name[__cond ? cdt_idx_mask_count(__ele_count) : 1]; \
		if (__cond) { \
			cdt_idx_mask_init(__name, __ele_count); \
		}

#define define_build_order_heap_by_range(__name, __idx, __count, __ele_count, __udata, __cmp_fn, __success) \
		order_heap __name; \
		uint8_t __name ## __order_heap_mem__[cdt_vla_sz(order_index_calc_size(__ele_count, __ele_count))]; \
		bool __success = order_heap_init_build_by_range_temp(&__name, __name ## __order_heap_mem__, __idx, __count, __ele_count, __cmp_fn, __udata)

#define VA_NARGS_SEQ 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
#define VA_NARGS_EXTRACT_N(_9, _8, _7, _6, _5, _4, _3, _2, _1, _0, N, ...) N
#define VA_NARGS_SEQ2N(...) VA_NARGS_EXTRACT_N(__VA_ARGS__)
#define VA_NARGS(...) VA_NARGS_SEQ2N(_, ##__VA_ARGS__, VA_NARGS_SEQ)

// Get around needing to pass last named arg to va_start().
#define CDT_OP_TABLE_GET_PARAMS(state, ...) cdt_process_state_get_params(state, VA_NARGS(__VA_ARGS__), __VA_ARGS__)

static const uint8_t msgpack_nil[1] = {0xC0};


//==========================================================
// Function declarations.
//

bool calc_index_count(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count, bool is_multi);
void calc_rel_index_count(int64_t in_index, uint64_t in_count, uint32_t rel_index, int64_t *out_index, uint64_t *out_count);

uint32_t cdt_get_storage_value_sz(as_unpacker *pk);
uint32_t cdt_get_msgpack_sz(as_unpacker *pk, bool check_storage);
uint32_t cdt_get_storage_list_sz(as_unpacker *pk, uint32_t count);
bool cdt_check_storage_list_contents(const uint8_t *buf, uint32_t sz, uint32_t count);

// cdt_result_data
bool result_data_set_not_found(cdt_result_data *rd, int64_t index);
void result_data_set_list_int2x(cdt_result_data *rd, int64_t i1, int64_t i2);
int result_data_set_index_rank_count(cdt_result_data *rd, uint32_t start, uint32_t count, uint32_t ele_count);
int result_data_set_range(cdt_result_data *rd, uint32_t start, uint32_t count, uint32_t ele_count);
void result_data_set_by_irc(cdt_result_data *rd, const order_index *ordidx, const order_index *idx_map, uint32_t total_count);
void result_data_set_by_itemlist_irc(cdt_result_data *rd, const order_index *items_ord, order_index *ranks, uint32_t total_count);
void result_data_set_int_list_by_mask(cdt_result_data *rd, const uint64_t *mask, uint32_t count, uint32_t ele_count);

// as_bin
void as_bin_set_int(as_bin *b, int64_t value);
void as_bin_set_double(as_bin *b, double value);
void as_bin_set_unordered_empty_list(as_bin *b, rollback_alloc *alloc_buf);
void as_bin_set_empty_packed_map(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags);

// cdt_delta_value
bool cdt_calc_delta_init(cdt_calc_delta *cdv, const cdt_payload *delta_value, bool is_decrement);
bool cdt_calc_delta_add(cdt_calc_delta *cdv, as_unpacker *pk_value);
void cdt_calc_delta_pack_and_result(cdt_calc_delta *cdv, cdt_payload *value, as_bin *result);

// cdt_payload
bool cdt_payload_is_int(const cdt_payload *payload);
int64_t cdt_payload_get_int64(const cdt_payload *payload);
void cdt_payload_pack_int(cdt_payload *packed, int64_t value);
void cdt_payload_pack_double(cdt_payload *packed, double value);

// cdt_process_state
bool cdt_process_state_init(cdt_process_state *cdt_state, const as_msg_op *op);
bool cdt_process_state_get_params(cdt_process_state *state, size_t n, ...);
const char *cdt_process_state_get_op_name(const cdt_process_state *state);

// cdt_process_state_packed_list
bool cdt_process_state_packed_list_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata);
bool cdt_process_state_packed_list_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata);

void cdt_container_builder_add(cdt_container_builder *builder, const uint8_t *buf, uint32_t sz);
void cdt_container_builder_add_n(cdt_container_builder *builder, const uint8_t *buf, uint32_t count, uint32_t sz);
void cdt_container_builder_add_int64(cdt_container_builder *builder, int64_t value);
void cdt_container_builder_add_int_range(cdt_container_builder *builder, uint32_t start, uint32_t count, uint32_t ele_count, bool reverse);
void cdt_container_builder_set_result(cdt_container_builder *builder, cdt_result_data *result);

void cdt_list_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t max_sz);
void cdt_map_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_max_sz, uint8_t flags);

// cdt_process_state_packed_map
bool cdt_process_state_packed_map_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata);
bool cdt_process_state_packed_map_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata);

// rollback_alloc
void rollback_alloc_push(rollback_alloc *packed_alloc, void *ptr);
uint8_t *rollback_alloc_reserve(rollback_alloc *alloc_buf, size_t sz);
void rollback_alloc_rollback(rollback_alloc *alloc_buf);
bool rollback_alloc_from_msgpack(rollback_alloc *alloc_buf, as_bin *b, const cdt_payload *seg);

// msgpacked_index
void msgpacked_index_set(msgpacked_index *idxs, uint32_t index, uint32_t value);
void msgpacked_index_incr(msgpacked_index *idxs, uint32_t index);
void msgpacked_index_set_ptr(msgpacked_index *idxs, uint8_t *ptr);
void *msgpacked_index_get_mem(const msgpacked_index *idxs, uint32_t index);
uint32_t msgpacked_index_size(const msgpacked_index *idxs);
uint32_t msgpacked_index_ptr2value(const msgpacked_index *idxs, const void *ptr);
uint32_t msgpacked_index_get(const msgpacked_index *idxs, uint32_t index);
void msgpacked_index_print(const msgpacked_index *idxs, const char *name);
bool msgpacked_index_find_index_sorted(const msgpacked_index *sorted_indexes, uint32_t find_index, uint32_t count, uint32_t *where);

// offset_index
void offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz);
void offset_index_set(offset_index *offidx, uint32_t index, uint32_t value);
bool offset_index_set_next(offset_index *offidx, uint32_t index, uint32_t value);
void offset_index_set_filled(offset_index *offidx, uint32_t ele_filled);
void offset_index_set_ptr(offset_index *offidx, uint8_t *idx_mem, const uint8_t *packed_mem);
void offset_index_copy(offset_index *dest, const offset_index *src, uint32_t d_start, uint32_t s_start, uint32_t count, int delta);
void offset_index_append_size(offset_index *offidx, uint32_t delta);

bool offset_index_find_items(offset_index *full_offidx, cdt_find_items_idxs_type find_type, as_unpacker *items_pk, order_index *items_ordidx_r, bool inverted, uint64_t *rm_mask, uint32_t *rm_count_r, order_index *rm_ranks_r);

void *offset_index_get_mem(const offset_index *offidx, uint32_t index);
uint32_t offset_index_size(const offset_index *offidx);
bool offset_index_is_null(const offset_index *offidx);
bool offset_index_is_valid(const offset_index *offidx);
bool offset_index_is_full(const offset_index *offidx);
uint32_t offset_index_get_const(const offset_index *offidx, uint32_t idx);
uint32_t offset_index_get_delta_const(const offset_index *offidx, uint32_t index);
uint32_t offset_index_get_filled(const offset_index *offidx);

uint32_t offset_index_vla_sz(const offset_index *offidx);
void offset_index_alloc_temp(offset_index *offidx, uint8_t *mem_temp);

void offset_index_print(const offset_index *offidx, const char *name);
void offset_index_delta_print(const offset_index *offidx, const char *name);

// order_index
void order_index_init(order_index *ordidx, uint8_t *ptr, uint32_t ele_count);
void order_index_init2(order_index *ordidx, uint8_t *ptr, uint32_t max_idx, uint32_t ele_count);
void order_index_init2_temp(order_index *ordidx, uint8_t *mem_temp, uint32_t max_idx, uint32_t ele_count);
void order_index_init_ref(order_index *dst, const order_index *src, uint32_t start, uint32_t count);
void order_index_set(order_index *ordidx, uint32_t index, uint32_t value);
void order_index_set_ptr(order_index *ordidx, uint8_t *ptr);
void order_index_incr(order_index *ordidx, uint32_t index);
void order_index_clear(order_index *ordidx);
bool order_index_sorted_mark_dup_eles(order_index *ordidx, const offset_index *full_offidx, uint32_t *count_r, uint32_t *sz_r);

uint32_t order_index_size(const order_index *ordidx);
bool order_index_is_null(const order_index *ordidx);
bool order_index_is_valid(const order_index *ordidx);
bool order_index_is_filled(const order_index *ordidx);

void *order_index_get_mem(const order_index *ordidx, uint32_t index);
uint32_t order_index_ptr2value(const order_index *ordidx, const void *ptr);
uint32_t order_index_get(const order_index *ordidx, uint32_t index);

bool order_index_find_rank_by_value(const order_index *ordidx, const cdt_payload *value, const offset_index *full_offidx, order_index_find *find);

uint32_t order_index_get_ele_size(const order_index *ordidx, uint32_t count, const offset_index *full_offidx);
uint8_t *order_index_write_eles(const order_index *ordidx, uint32_t count, const offset_index *full_offidx, uint8_t *ptr, bool invert);

uint32_t order_index_adjust_value(const order_index_adjust *via, uint32_t src);
void order_index_copy(order_index *dest, const order_index *src, uint32_t d_start, uint32_t s_start, uint32_t count, const order_index_adjust *adjust);
size_t order_index_calc_size(uint32_t max_idx, uint32_t ele_count);

void order_index_print(const order_index *ordidx, const char *name);

// order_heap
bool order_heap_init_build_by_range_temp(order_heap *heap, uint8_t *heap_mem, uint32_t idx, uint32_t count, uint32_t ele_count, order_heap_compare_fn cmp_fn, const void *udata);
void order_heap_swap(order_heap *heap, uint32_t index1, uint32_t index2);
bool order_heap_remove_top(order_heap *heap);
bool order_heap_replace_top(order_heap *heap, uint32_t value);
bool order_heap_heapify(order_heap *heap, uint32_t index);
bool order_heap_build(order_heap *heap, bool init);
bool order_heap_order_at_end(order_heap *heap, uint32_t count);
void order_heap_reverse_end(order_heap *heap, uint32_t count);

void order_heap_print(const order_heap *heap);

// cdt_idx_mask
size_t cdt_idx_mask_count(uint32_t ele_count);
void cdt_idx_mask_init(uint64_t *mask, uint32_t ele_count);
void cdt_idx_mask_set(uint64_t *mask, uint32_t idx);
void cdt_idx_mask_set_by_ordidx(uint64_t *mask, const order_index *ordidx, uint32_t start, uint32_t count, bool inverted);
void cdt_idx_mask_set_by_irc(uint64_t *mask, const order_index *rankcount, const order_index *idx_map, bool inverted);
void cdt_idx_mask_invert(uint64_t *mask, uint32_t ele_count);

uint64_t cdt_idx_mask_get(const uint64_t *mask, uint32_t idx);
size_t cdt_idx_mask_bit_count(const uint64_t *mask, uint32_t ele_count);

bool cdt_idx_mask_is_set(const uint64_t *mask, uint32_t idx);

uint32_t cdt_idx_mask_find(const uint64_t *mask, uint32_t start, uint32_t end, bool is_find0);
uint8_t *cdt_idx_mask_write_eles(const uint64_t *mask, uint32_t count, const offset_index *full_offidx, uint8_t *ptr, bool invert);
uint32_t cdt_idx_mask_get_content_sz(const uint64_t *mask, uint32_t count, const offset_index *full_offidx);

void cdt_idx_mask_print(const uint64_t *mask, uint32_t ele_count, const char *name);

// list
bool list_full_offset_index_fill_all(offset_index *offidx, bool is_storage);
bool list_order_index_sort(order_index *ordidx, const offset_index *full_offidx, as_cdt_sort_flags flags);

bool list_param_parse(const cdt_payload *items, as_unpacker *pk, uint32_t *count_r);

// cdt_idx
void cdt_idx_set_alloc(rollback_alloc *alloc);
void cdt_idx_clear();
uint8_t *cdt_idx_alloc(uint32_t sz);

// Debugging support
void print_hex(const uint8_t *packed, uint32_t packed_sz, char *buf, uint32_t buf_sz);
void print_packed(const uint8_t *packed, uint32_t sz, const char *name);
void cdt_bin_print(const as_bin *b, const char *name);


//==========================================================
// Inline functions.
//

static inline bool
result_data_is_inverted(cdt_result_data *rd)
{
	return (rd->flags & AS_CDT_OP_FLAG_INVERTED) != 0;
}

static inline void
result_data_set(cdt_result_data *rd, uint64_t result_type, bool is_multi)
{
	rd->type = (result_type_t)(result_type & AS_CDT_OP_FLAG_RESULT_MASK);
	rd->flags = (as_cdt_op_flags)(result_type & (~AS_CDT_OP_FLAG_RESULT_MASK));
	rd->is_multi = is_multi;
}

static inline void
result_data_set_int(cdt_result_data *rd, int64_t value)
{
	if (rd) {
		as_bin_set_int(rd->result, value);
	}
}

static inline bool
result_data_is_return_elements(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_KEY || rd->type == RESULT_TYPE_VALUE ||
			rd->type == RESULT_TYPE_MAP);
}

static inline bool
result_data_is_return_index(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_INDEX || rd->type == RESULT_TYPE_REVINDEX);
}

static inline bool
result_data_is_return_index_range(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_INDEX_RANGE ||
			rd->type == RESULT_TYPE_REVINDEX_RANGE);
}

static inline bool
result_data_is_return_rank(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_REVRANK || rd->type == RESULT_TYPE_RANK);
}

static inline bool
result_data_is_return_rank_range(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_REVRANK_RANGE ||
			rd->type == RESULT_TYPE_RANK_RANGE);
}

static inline void
order_heap_set(order_heap *heap, uint32_t index, uint32_t value)
{
	order_index_set((order_index *)heap, index, value);
}

static inline uint32_t
order_heap_get(const order_heap *heap, uint32_t index)
{
	return order_index_get((const order_index *)heap, index);
}

// Calculate index given index and max_index.
static inline int64_t
calc_index(int64_t index, uint32_t max_index)
{
	return index < 0 ? (int64_t)max_index + index : index;
}

static inline void
cdt_int_list_builder_start(cdt_container_builder *builder,
		rollback_alloc *alloc_buf, uint32_t ele_count)
{
	cdt_list_builder_start(builder, alloc_buf, ele_count,
			CDT_MAX_PACKED_INT_SZ * ele_count);
}
