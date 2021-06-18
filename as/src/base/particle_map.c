/*
 * particle_map.c
 *
 * Copyright (C) 2015-2020 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "bits.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"


//==========================================================
// MAP particle interface - function declarations.
//

// Destructor, etc.
void map_destruct(as_particle *p);
uint32_t map_size(const as_particle *p);

// Handle "wire" format.
int32_t map_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t map_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int map_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t map_wire_size(const as_particle *p);
uint32_t map_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t map_size_from_asval(const as_val *val);
void map_from_asval(const as_val *val, as_particle **pp);
as_val *map_to_asval(const as_particle *p);
uint32_t map_asval_wire_size(const as_val *val);
uint32_t map_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t map_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void map_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);

// Handle on-device "flat" format.
const uint8_t *map_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp);
uint32_t map_flat_size(const as_particle *p);
uint32_t map_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// MAP particle interface - vtable.
//

const as_particle_vtable map_vtable = {
		map_destruct,
		map_size,

		map_concat_size_from_wire,
		map_append_from_wire,
		map_prepend_from_wire,
		map_incr_from_wire,
		map_size_from_wire,
		map_from_wire,
		map_compare_from_wire,
		map_wire_size,
		map_to_wire,

		map_size_from_asval,
		map_from_asval,
		map_to_asval,
		map_asval_wire_size,
		map_asval_to_wire,

		map_size_from_msgpack,
		map_from_msgpack,

		blob_skip_flat,
		blob_cast_from_flat,
		map_from_flat,
		map_flat_size,
		map_to_flat
};


//==========================================================
// Typedefs & constants.
//

#if defined(CDT_DEBUG_VERIFY)
#define MAP_DEBUG_VERIFY
#endif

#define LINEAR_FIND_RANK_MAX_COUNT		16 // switch to linear search when the count drops to this number

#define AS_PACKED_MAP_FLAG_RESERVED_0	0x04 // placeholder for multimap
#define AS_PACKED_MAP_FLAG_OFF_IDX		0x10 // has list offset index
#define AS_PACKED_MAP_FLAG_ORD_IDX		0x20 // has value order index
#define AS_PACKED_MAP_FLAG_ON_STACK		0x40 // map on stack

#define AS_PACKED_MAP_FLAG_VALID_MASK	AS_PACKED_MAP_FLAG_KV_ORDERED

struct packed_map_s;

typedef void (*packed_map_get_by_idx_func)(const struct packed_map_s *userdata, cdt_payload *contents, uint32_t index);

typedef struct packed_map_s {
	const uint8_t *packed;
	const uint8_t *contents; // where elements start (excludes ext)
	uint32_t packed_sz;
	uint32_t content_sz;

	// Mutable field member (Is considered mutable in const objects).
	offset_index offidx; // offset start at contents (excluding ext metadata pair)
	uint8_t flags;
	// Mutable field member.
	order_index ordidx;

	uint32_t ele_count; // excludes ext pair
} packed_map;

typedef struct packed_map_op_s {
	const packed_map *map;

	uint32_t new_ele_count;
	uint32_t ele_removed;

	uint32_t seg1_sz;
	uint32_t seg2_offset;
	uint32_t seg2_sz;

	uint32_t key1_offset;
	uint32_t key1_sz;
	uint32_t key2_offset;
	uint32_t key2_sz;
} packed_map_op;

typedef struct map_packer_s {
	uint8_t *write_ptr;
	const uint8_t *contents;

	offset_index offidx; // offset start at contents (excluding ext metadata pair)
	order_index ordidx;

	uint32_t ele_count;
	uint32_t content_sz; // does not include map header or ext
	uint32_t ext_content_sz;

	uint32_t ext_sz;
	uint32_t ext_header_sz;

	uint8_t flags;
} map_packer;

typedef struct map_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) map_mem;

typedef struct map_flat_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) map_flat;

typedef enum {
	MAP_EMPTY_MAP_HDR = 0,
	MAP_EMPTY_EXT_HDR,
	MAP_EMPTY_EXT_SZ,
	MAP_EMPTY_EXT_FLAGS,
	MAP_EMPTY_NIL,
	MAP_EMPTY_FLAGED_SIZE
} map_empty_bytes;

#define MAP_MEM_EMPTY_FLAGGED_ENTRY(__flags) \
		.type = AS_PARTICLE_TYPE_MAP, \
		.sz = MAP_EMPTY_FLAGED_SIZE, \
		.data = { \
				[MAP_EMPTY_MAP_HDR] = 0x81, \
				[MAP_EMPTY_EXT_HDR] = 0xC7, \
				[MAP_EMPTY_EXT_SZ] = 0, \
				[MAP_EMPTY_EXT_FLAGS] = __flags, \
				[MAP_EMPTY_NIL] = 0xC0 \
		}

static const map_mem map_mem_empty_k = {
		MAP_MEM_EMPTY_FLAGGED_ENTRY(AS_PACKED_MAP_FLAG_K_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX)
};

static const map_mem map_mem_empty_kv = {
		MAP_MEM_EMPTY_FLAGGED_ENTRY(AS_PACKED_MAP_FLAG_KV_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX)
};

static const map_mem map_mem_empty = {
		.type = AS_PARTICLE_TYPE_MAP,
		.sz = 1,
		.data = {0x80}
};

typedef enum sort_by_e {
	SORT_BY_KEY,
	SORT_BY_VALUE
} sort_by_t;

typedef struct index_sort_userdata_s {
	const offset_index *offsets;
	order_index *order;
	const uint8_t *contents;
	uint32_t content_sz;
	sort_by_t sort_by;
} index_sort_userdata;

typedef struct map_add_control_s {
	bool allow_overwrite;	// if key exists and map is unique-keyed - may overwrite
	bool allow_create;		// if key does not exist - may create
	bool no_fail;			// do not fail on policy violation
	bool do_partial;		// do the parts that did not fail
} map_add_control;

typedef struct map_ele_find_s {
	bool found_key;
	bool found_value;

	uint32_t idx;
	uint32_t rank;

	uint32_t key_offset;	// offset start at map header
	uint32_t value_offset;	// offset start at map header
	uint32_t sz;

	uint32_t upper;
	uint32_t lower;
} map_ele_find;

typedef struct {
	offset_index *offidx;
	uint8_t mem_temp[];
} __attribute__ ((__packed__)) map_vla_offidx_cast;

typedef struct {
	offset_index *offidx;
	order_index *ordidx;
	uint8_t mem_temp[];
} __attribute__ ((__packed__)) map_vla_offord_cast;

#define setup_map_must_have_offidx(__name, __map_p, __alloc) \
		uint8_t __name ## __vlatemp[sizeof(offset_index *) + offset_index_vla_sz(&(__map_p)->offidx)]; \
		map_vla_offidx_cast *__name = (map_vla_offidx_cast *)__name ## __vlatemp; \
		__name->offidx = (offset_index *)&(__map_p)->offidx; \
		offset_index_alloc_temp(__name->offidx, __name->mem_temp, __alloc)

#define setup_map_must_have_all_idx(__name, __map_p, __alloc) \
		uint8_t __name ## __vlatemp[sizeof(offset_index *) + sizeof(order_index *) + map_allidx_vla_sz(__map_p)]; \
		map_vla_offord_cast *__name = (map_vla_offord_cast *)__name ## __vlatemp; \
		__name->offidx = (offset_index *)&(__map_p)->offidx; \
		__name->ordidx = (order_index *)&(__map_p)->ordidx; \
		map_allidx_alloc_temp(__map_p, __name->mem_temp, __alloc)

#define define_map_msgpack_in(__name, __map_ptr) \
		msgpack_in __name = { \
				.buf = (__map_ptr)->contents, \
				.buf_sz = (__map_ptr)->content_sz \
		}

#define define_packed_map_op(__name, __map_ptr) \
		packed_map_op __name; \
		packed_map_op_init(&__name, __map_ptr)

#define define_map_packer(__name, __ele_count, __flags, __content_sz) \
		map_packer __name; \
		map_packer_init(&__name, __ele_count, __flags, __content_sz)


//==========================================================
// Forward declarations.
//

static inline bool is_map_type(uint8_t type);
static inline bool is_k_ordered(uint8_t flags);
static inline bool is_kv_ordered(uint8_t flags);
static uint32_t map_calc_ext_content_sz(uint8_t flags, uint32_t ele_count, uint32_t content_sz);
static uint8_t map_adjust_incoming_flags(uint8_t flags);

static inline uint32_t map_allidx_vla_sz(const packed_map *map);
static inline void map_allidx_alloc_temp(const packed_map *map, uint8_t *mem_temp, rollback_alloc *alloc);

static inline uint32_t map_ext_content_sz(const packed_map *map);
static inline bool map_is_k_ordered(const packed_map *map);
static inline bool map_is_kv_ordered(const packed_map *map);
static inline bool map_has_offidx(const packed_map *map);

// cdt_context
static inline void cdt_context_use_static_map_if_notinuse(cdt_context *ctx, uint8_t flags);
static inline void cdt_context_set_empty_packed_map(cdt_context *ctx, uint8_t flags);
static inline void cdt_context_set_by_map_idx(cdt_context *ctx, const packed_map *map, uint32_t idx);
static inline void cdt_context_set_by_map_create(cdt_context *ctx, const packed_map *map, uint32_t idx);

static inline void cdt_context_map_push(cdt_context *ctx, const packed_map *map, uint32_t idx);
static inline void cdt_context_map_handle_possible_noop(cdt_context *ctx);

// map_packer
static as_particle *map_packer_create_particle(map_packer *mpk, rollback_alloc *alloc_buf);
static void map_packer_create_particle_ctx(map_packer *mpk, cdt_context *ctx);
static void map_packer_init(map_packer *mpk, uint32_t ele_count, uint8_t flags, uint32_t content_sz);

static void map_packer_setup_bin(map_packer *mpk, cdt_op_mem *com);

static void map_packer_write_hdridx(map_packer *mpk);
static void map_packer_fill_offset_index(map_packer *mpk);
static int map_packer_fill_index_sort_compare(const void *x, const void *y, void *p);
static void map_packer_fill_ordidx(map_packer *mpk, const uint8_t *contents, uint32_t content_sz);
static void map_packer_add_op_copy_index(map_packer *mpk, const packed_map_op *add_op, map_ele_find *remove_info, uint32_t kv_sz, const cdt_payload *value);
static inline void map_packer_write_seg1(map_packer *mpk, const packed_map_op *op);
static inline void map_packer_write_seg2(map_packer *mpk, const packed_map_op *op);
static inline void map_packer_write_msgpack_seg(map_packer *mpk, const cdt_payload *seg);

// map_op
static int map_set_flags(cdt_op_mem *com, uint8_t set_flags);
static int map_increment(cdt_op_mem *com, const cdt_payload *key, const cdt_payload *delta_value, bool is_decrement);

static int map_add(cdt_op_mem *com, const cdt_payload *key, const cdt_payload *value, const map_add_control *control, bool set_result);
static int map_add_items(cdt_op_mem *com, const cdt_payload *items, const map_add_control *control);
static int map_add_items_ordered(const packed_map *map, cdt_op_mem *com, const offset_index *val_off, order_index *val_ord, const map_add_control *control);
static int map_add_items_unordered(const packed_map *map, cdt_op_mem *com, const offset_index *val_off, order_index *val_ord, const map_add_control *control);

static int map_remove_by_key_interval(cdt_op_mem *com, const cdt_payload *key_start, const cdt_payload *key_end);
static int map_remove_by_index_range(cdt_op_mem *com, int64_t index, uint64_t count);
static int map_remove_by_value_interval(cdt_op_mem *com, const cdt_payload *value_start, const cdt_payload *value_end);
static int map_remove_by_rank_range(cdt_op_mem *com, int64_t rank, uint64_t count);

static int map_remove_by_rel_index_range(cdt_op_mem *com, const cdt_payload *value, int64_t index, uint64_t count);
static int map_remove_by_rel_rank_range(cdt_op_mem *com, const cdt_payload *value, int64_t rank, uint64_t count);

static int map_remove_all_by_key_list(cdt_op_mem *com, const cdt_payload *key_list);
static int map_remove_all_by_value_list(cdt_op_mem *com, const cdt_payload *value_list);

static int map_clear(cdt_op_mem *com);

// packed_map
static bool packed_map_init(packed_map *map, const uint8_t *buf, uint32_t sz, bool fill_idxs);
static inline bool packed_map_init_from_particle(packed_map *map, const as_particle *p, bool fill_idxs);
static bool packed_map_init_from_bin(packed_map *map, const as_bin *b, bool fill_idxs);
static bool packed_map_init_from_ctx(packed_map *map, const cdt_context *ctx, bool fill_idxs);
static inline bool packed_map_init_from_com(packed_map *map, cdt_op_mem *com, bool fill_idxs);
static bool packed_map_unpack_hdridx(packed_map *map, bool fill_idxs);

static void packed_map_init_indexes(const packed_map *map, as_packer *pk, offset_index *offidx, order_index *ordidx);

static void packed_map_ensure_ordidx_filled(const packed_map *map);
static bool packed_map_check_and_fill_offidx(const packed_map *map);

static uint32_t packed_map_find_index_by_idx_unordered(const packed_map *map, uint32_t idx);
static uint32_t packed_map_find_index_by_key_unordered(const packed_map *map, const cdt_payload *key);

static void packed_map_find_rank_indexed_linear(const packed_map *map, map_ele_find *find, uint32_t start, uint32_t len);
static void packed_map_find_rank_indexed(const packed_map *map, map_ele_find *find);
static void packed_map_find_rank_by_value_indexed(const packed_map *map, map_ele_find *find, const cdt_payload *value);
static void packed_map_find_rank_range_by_value_interval_indexed(const packed_map *map, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, bool is_multi);
static void packed_map_find_rank_range_by_value_interval_unordered(const packed_map *map, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, uint64_t *mask, bool is_multi);
static void packed_map_find_key_indexed(const packed_map *map, map_ele_find *find, const cdt_payload *key);
static bool packed_map_find_key(const packed_map *map, map_ele_find *find, const cdt_payload *key);

static int packed_map_get_remove_by_key_interval(const packed_map *map, cdt_op_mem *com, const cdt_payload *key_start, const cdt_payload *key_end);
static int packed_map_trim_ordered(const packed_map *map, cdt_op_mem *com, uint32_t index, uint32_t count);
static int packed_map_get_remove_by_index_range(const packed_map *map, cdt_op_mem *com, int64_t index, uint64_t count);

static int packed_map_get_remove_by_value_interval(const packed_map *map, cdt_op_mem *com, const cdt_payload *value_start, const cdt_payload *value_end);
static int packed_map_get_remove_by_rank_range(const packed_map *map, cdt_op_mem *com, int64_t rank, uint64_t count);

static int packed_map_get_remove_all_by_key_list(const packed_map *map, cdt_op_mem *com, const cdt_payload *key_list);
static int packed_map_get_remove_all_by_key_list_ordered(const packed_map *map, cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count);
static int packed_map_get_remove_all_by_key_list_unordered(const packed_map *map, cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count);
static int packed_map_get_remove_all_by_value_list(const packed_map *map, cdt_op_mem *com, const cdt_payload *value_list);
static int packed_map_get_remove_all_by_value_list_ordered(const packed_map *map, cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count);

static int packed_map_get_remove_by_rel_index_range(const packed_map *map, cdt_op_mem *com, const cdt_payload *key, int64_t index, uint64_t count);
static int packed_map_get_remove_by_rel_rank_range(const packed_map *map, cdt_op_mem *com, const cdt_payload *value, int64_t rank, uint64_t count);

static int packed_map_get_remove_all(const packed_map *map, cdt_op_mem *com);

static void packed_map_remove_by_mask(const packed_map *map, cdt_op_mem *com, const uint64_t *rm_mask, uint32_t count, uint32_t *rm_sz_r);
static void packed_map_remove_idx_range(const packed_map *map, cdt_op_mem *com, uint32_t idx, uint32_t count);


static void packed_map_get_range_by_key_interval_unordered(const packed_map *map, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count, uint64_t *mask);
static void packed_map_get_range_by_key_interval_ordered(const packed_map *map, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count);
static void packed_map_build_rank_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static void packed_map_build_rank_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, cdt_result_data *result);
static void packed_map_build_rank_result_by_index_range(const packed_map *map, uint32_t index, uint32_t count, cdt_result_data *result);

static void packed_map_get_key_by_idx(const packed_map *map, cdt_payload *key, uint32_t index);
static void packed_map_get_value_by_idx(const packed_map *map, cdt_payload *value, uint32_t idx);
static void packed_map_get_pair_by_idx(const packed_map *map, cdt_payload *value, uint32_t index);

static void packed_map_build_index_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static void packed_map_build_index_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, cdt_result_data *result);
static bool packed_map_build_ele_result_by_idx_range(const packed_map *map, uint32_t start_idx, uint32_t count, cdt_result_data *result);
static bool packed_map_build_ele_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, uint32_t rm_sz, cdt_result_data *result);
static bool packed_map_build_ele_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, uint32_t rm_sz, cdt_result_data *result);
static int packed_map_build_result_by_key(const packed_map *map, const cdt_payload *key, uint32_t idx, uint32_t count, cdt_result_data *result);

static uint32_t packed_map_get_rank_by_idx(const packed_map *map, uint32_t idx);
static void packed_map_build_rank_result_by_idx(const packed_map *map, uint32_t idx, cdt_result_data *result);
static void packed_map_build_rank_result_by_idx_range(const packed_map *map, uint32_t idx, uint32_t count, cdt_result_data *result);

static msgpack_cmp_type packed_map_compare_key_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2);
static msgpack_cmp_type packed_map_compare_values(msgpack_in *mp1, msgpack_in *mp2);
static msgpack_cmp_type packed_map_compare_value_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2);

static void packed_map_write_k_ordered(const packed_map *map, uint8_t *write_ptr, offset_index *offsets_new);

// packed_map_op
static void packed_map_op_init(packed_map_op *op, const packed_map *map);
static uint32_t packed_map_op_add(packed_map_op *op, const map_ele_find *found);
static uint32_t packed_map_op_remove(packed_map_op *op, const map_ele_find *found, uint32_t count, uint32_t remove_sz);

static uint8_t *packed_map_op_write_seg1(const packed_map_op *op, uint8_t *buf);
static uint8_t *packed_map_op_write_seg2(const packed_map_op *op, uint8_t *buf);
static void packed_map_op_write_new_offidx(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, offset_index *new_offidx, uint32_t kv_sz);
static void packed_map_op_write_new_ordidx(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, order_index *new_ordidx);

// map_particle
static as_particle *map_particle_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t content_sz, uint8_t flags);
static uint32_t map_particle_strip_indexes(const as_particle *p, uint8_t *dest);

// map_ele_find
static void map_ele_find_init(map_ele_find *find, const packed_map *map);
static void map_ele_find_continue_from_lower(map_ele_find *find, const map_ele_find *found, uint32_t ele_count);
static void map_ele_find_init_from_idx(map_ele_find *find, const packed_map *map, uint32_t idx);

// map_offset_index
static bool map_offset_index_check_and_fill(offset_index *offidx, uint32_t index);

static void map_offset_index_copy_rm_mask(offset_index *dest, const offset_index *src, const uint64_t *rm_mask, uint32_t rm_count);
static void map_offset_index_copy_rm_range(offset_index *dest, const offset_index *src, uint32_t rm_idx, uint32_t rm_count);

// order_index
static void order_index_sort(order_index *ordidx, const offset_index *offsets, const uint8_t *contents, uint32_t content_sz, sort_by_t sort_by);
static inline void order_index_set_sorted(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by);
static void order_index_set_sorted_with_offsets(order_index *ordidx, const offset_index *offsets, sort_by_t sort_by);

static uint32_t order_index_find_idx(const order_index *ordidx, uint32_t idx, uint32_t start, uint32_t len);

// order_index_adjust
static uint32_t order_index_adjust_lower(const order_index_adjust *via, uint32_t src);

// order_index_op
static inline void order_index_op_add(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank);
static inline void order_index_op_replace1_internal(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank, const order_index_adjust *adjust);
static inline void order_index_op_replace1(order_index *dest, const order_index *src, uint32_t add_rank, uint32_t remove_rank);
static void order_index_op_remove_idx_mask(order_index *dest, const order_index *src, const uint64_t *mask, uint32_t count);

// result_data
static bool result_data_set_key_not_found(cdt_result_data *rd, int64_t index);
static bool result_data_set_value_not_found(cdt_result_data *rd, int64_t rank);

// Debugging support
void map_print(const packed_map *map, const char *name);


//==========================================================
// MAP particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
map_destruct(as_particle *p)
{
	cf_free(p);
}

uint32_t
map_size(const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;
	return (uint32_t)sizeof(map_mem) + p_map_mem->sz;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
map_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for map");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
map_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to map");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
map_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to map");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
map_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "increment of map");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int32_t
map_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// TODO - CDT can't determine in memory or not.
	packed_map map;

	if (! packed_map_init(&map, wire_value, value_size, false)) {
		cf_warning(AS_PARTICLE, "map_size_from_wire() invalid packed map");
		return -AS_ERR_UNKNOWN;
	}

	if ((map.flags & AS_PACKED_MAP_FLAG_VALID_MASK) != map.flags) {
		cf_warning(AS_PARTICLE, "map_size_from_wire() unsupported flags %x", map.flags);
		return -AS_ERR_UNSUPPORTED_FEATURE;
	}

	if (map.flags == 0) {
		return (int32_t)(sizeof(map_mem) + value_size);
	}

	uint32_t hdr_sz = map_ext_content_sz(&map);

	hdr_sz += as_pack_ext_header_get_size(hdr_sz) + 1;
	hdr_sz += as_pack_map_header_get_size(map.ele_count + 1);

	return (int32_t)(sizeof(map_mem) + map.content_sz + hdr_sz);
}

int
map_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	// TODO - CDT can't determine in memory or not.
	// It works for data-not-in-memory but we'll incur a memcpy that could be
	// eliminated.
	packed_map map;
	bool is_valid = packed_map_init(&map, wire_value, value_size, false);

	cf_assert(is_valid, AS_PARTICLE, "map_from_wire() invalid packed map");

	map_mem *p_map_mem = (map_mem *)*pp;

	p_map_mem->type = wire_type;

	if (map.flags == 0) {
		if (! cdt_check_buf(map.packed, map.packed_sz)) {
			cf_warning(AS_PARTICLE, "map_from_wire() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		p_map_mem->sz = value_size;
		memcpy(p_map_mem->data, wire_value, value_size);

#ifdef MAP_DEBUG_VERIFY
		{
			as_bin b;
			b.particle = *pp;
			as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);

			const cdt_context ctx = {
					.b = &b,
					.orig = b.particle,
			};

			if (! map_verify(&ctx)) {
				offset_index_print(&map.offidx, "verify");
				map_print(&map, "original");
				cf_crash(AS_PARTICLE, "map_from_wire: pp=%p wire_value=%p", pp, wire_value);
			}
		}
#endif

		return AS_OK;
	}

	uint32_t ext_content_sz = map_ext_content_sz(&map);
	// 1 byte for header, 1 byte for type, 1 byte for length for existing ext.
	uint32_t extra_sz = as_pack_ext_header_get_size(ext_content_sz) - 3;

	as_packer pk = {
			.buffer = p_map_mem->data,
			.capacity = value_size + extra_sz
	};

	offset_index offidx;
	order_index ordidx;

	as_pack_map_header(&pk, map.ele_count + 1);
	as_pack_ext_header(&pk, ext_content_sz,
			map_adjust_incoming_flags(map.flags));

	uint32_t hdr_sz = pk.offset + ext_content_sz + 1; // 1 for NIL

	packed_map_init_indexes(&map, &pk, &offidx, &ordidx);
	as_pack_val(&pk, &as_nil);
	memcpy(pk.buffer + pk.offset, map.contents, map.content_sz);
	p_map_mem->sz = map.content_sz + hdr_sz;

	if (! offset_index_deep_check_order_and_fill(&offidx, true)) {
		cf_warning(AS_PARTICLE, "map_from_wire() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = *pp;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);

		const cdt_context ctx = {
				.b = &b,
				.orig = b.particle,
		};

		if (! map_verify(&ctx)) {
			offset_index_print(&map.offidx, "verify");
			cf_crash(AS_PARTICLE, "map_from_wire: pp=%p wire_value=%p", pp, wire_value);
		}
	}
#endif

	return AS_OK;
}

int
map_compare_from_wire(const as_particle *p, as_particle_type wire_type,
		const uint8_t *wire_value, uint32_t value_size)
{
	cf_warning(AS_PARTICLE, "map_compare_from_wire() not implemented");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

uint32_t
map_wire_size(const as_particle *p)
{
	packed_map map;

	if (! packed_map_init_from_particle(&map, p, false)) {
		as_bin b = {
				.particle = (as_particle *)p
		};

		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);
		cdt_bin_print(&b, "map");
		cf_crash(AS_PARTICLE, "map_wire_size() invalid packed map");
	}

	if (map.flags == 0) {
		return map.packed_sz;
	}

	uint32_t sz = map.content_sz;
	sz += as_pack_list_header_get_size(map.ele_count + 1);
	sz += 3 + 1; // 3 for min ext hdr and 1 for nil pair

	return sz;
}

uint32_t
map_to_wire(const as_particle *p, uint8_t *wire)
{
	return map_particle_strip_indexes(p, wire);
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
map_size_from_asval(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t sz = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	const as_map *map = (const as_map *)val;

	if (map->flags == 0) {
		return (uint32_t)sizeof(map_mem) + sz;
	}

	uint32_t ele_count = as_map_size(map);
	uint32_t map_hdr_sz = as_pack_list_header_get_size(ele_count);
	uint32_t content_sz = sz - map_hdr_sz;
	uint32_t ext_content_sz = map_calc_ext_content_sz(map->flags, ele_count,
			content_sz);

	sz = (uint32_t)sizeof(map_mem);
	sz += as_pack_list_header_get_size(ele_count + 1) + content_sz;
	sz += as_pack_ext_header_get_size(ext_content_sz);	// ext header and length field
	sz += ext_content_sz;								// ext content
	sz++;												// nil pair

	return (uint32_t)sizeof(map_mem) + sz;
}

void
map_from_asval(const as_val *val, as_particle **pp)
{
	map_mem *p_map_mem = (map_mem *)*pp;
	const as_map *av_map = (const as_map *)val;

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;

	as_serializer s;
	as_msgpack_init(&s);

	int32_t sz = as_serializer_serialize_presized(&s, val, p_map_mem->data);

	cf_assert(sz >= 0, AS_PARTICLE, "map_from_asval() failed to presize");
	as_serializer_destroy(&s);

	if (av_map->flags == 0) {
		p_map_mem->sz = (uint32_t)sz;
		return;
	}

	uint8_t *temp_mem = NULL;
	uint8_t buf[sizeof(packed_map) + (sz < CDT_MAX_STACK_OBJ_SZ ? sz : 0)];
	packed_map *map = (packed_map *)buf;
	bool success;

	if (sz < CDT_MAX_STACK_OBJ_SZ) {
		memcpy(buf + sizeof(packed_map), p_map_mem->data, sz);
		success = packed_map_init(map, buf + sizeof(packed_map), sz, false);
	}
	else {
		temp_mem = cf_malloc(sz);
		memcpy(temp_mem, p_map_mem->data, sz);
		success = packed_map_init(map, temp_mem, sz, false);
	}

	cf_assert(success, AS_PARTICLE, "map_from_asval() failed to unpack header");

	uint8_t map_flags = map_adjust_incoming_flags(av_map->flags);
	define_map_packer(mpk, map->ele_count, map_flags, map->content_sz);
	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes

	mpk.write_ptr = p_map_mem->data;
	map_packer_write_hdridx(&mpk);

	setup_map_must_have_offidx(old, map, alloc_idx);
	bool check = packed_map_check_and_fill_offidx(map);
	cf_assert(check, AS_PARTICLE, "invalid map");

	packed_map_write_k_ordered(map, mpk.write_ptr, &mpk.offidx);

	p_map_mem->sz =
			(uint32_t)(mpk.contents - p_map_mem->data + map->content_sz);

	if (! order_index_is_null(&mpk.ordidx)) {
		order_index_set(&mpk.ordidx, 0, map->ele_count);
	}

	cf_free(temp_mem);
	rollback_alloc_rollback(alloc_idx);

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = (as_particle *)p_map_mem;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);

		const cdt_context ctx = {
				.b = &b,
				.orig = b.particle,
		};

		if (! map_verify(&ctx)) {
			cdt_bin_print(&b, "map_from_asval");
			cf_crash(AS_PARTICLE, "map_from_asval: ele_count %u", map->ele_count);
		}
	}
#endif
}

as_val *
map_to_asval(const as_particle *p)
{
	map_mem *p_map_mem = (map_mem *)p;

	as_buffer buf = {
			.capacity = p_map_mem->sz,
			.size = p_map_mem->sz,
			.data = p_map_mem->data
	};

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val = NULL;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

	if (! val) {
		return (as_val *)as_hashmap_new(0);
	}

	packed_map map;

	packed_map_init_from_particle(&map, p, false);
	((as_map *)val)->flags = (uint32_t)map.flags;

	return val;
}

uint32_t
map_asval_wire_size(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t sz = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	return sz;
}

uint32_t
map_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_serializer s;
	as_msgpack_init(&s);

	int32_t sz = as_serializer_serialize_presized(&s, val, wire);

	as_serializer_destroy(&s);
	cf_assert(sz > 0, AS_PARTICLE, "map_asval_to_wire() sz %d failed to serialize", sz);

	return (uint32_t)sz;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
map_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	return (uint32_t)sizeof(map_mem) + packed_size;
}

void
map_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	map_mem *p_map_mem = (map_mem *)*pp;

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = packed_size;
	memcpy(p_map_mem->data, packed, p_map_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t *
map_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp)
{
	if (flat + sizeof(map_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat map");
		return NULL;
	}

	const map_flat *p_map_flat = (const map_flat *)flat;

	flat += sizeof(map_flat) + p_map_flat->sz;

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat map");
		return NULL;
	}

	packed_map map;

	if (! packed_map_init(&map, p_map_flat->data, p_map_flat->sz, false)) {
		cf_warning(AS_PARTICLE, "map_from_flat() invalid packed map");
		return NULL;
	}

	uint8_t flags = map_adjust_incoming_flags(map.flags);
	define_map_packer(mpk, map.ele_count, flags, map.content_sz);
	as_particle *p = map_packer_create_particle(&mpk, NULL);

	map_packer_write_hdridx(&mpk);
	memcpy(mpk.write_ptr, map.contents, map.content_sz);

	if (! offset_index_is_null(&mpk.offidx) &&
			! offset_index_check_order_and_fill(&mpk.offidx, true)) {
		cf_warning(AS_PARTICLE, "map_from_flat() invalid packed map");
		cf_free(p);
		return NULL;
	}

	if (! order_index_is_null(&mpk.ordidx)) {
		order_index_set_sorted(&mpk.ordidx, &mpk.offidx, map.contents,
				map.content_sz, SORT_BY_VALUE);
	}

	*pp = p;

	return flat;
}

uint32_t
map_flat_size(const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	packed_map map;

	if (! packed_map_init_from_particle(&map, p, false)) {
		as_bin b = {
				.particle = (as_particle *)p
		};

		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);
		cdt_bin_print(&b, "map");
		cf_crash(AS_PARTICLE, "map_flat_size() invalid packed map");
	}

	if (map.flags == 0) {
		return sizeof(map_flat) + p_map_mem->sz;
	}

	uint32_t sz = map.content_sz;
	sz += as_pack_list_header_get_size(map.ele_count + 1);
	sz += 3 + 1; // 3 for min ext hdr and 1 for nil pair

	return (uint32_t)sizeof(map_flat) + sz;
}

uint32_t
map_to_flat(const as_particle *p, uint8_t *flat)
{
	map_flat *p_map_flat = (map_flat *)flat;

	p_map_flat->sz = map_particle_strip_indexes(p, p_map_flat->data);

	// Already wrote the type.

	return sizeof(map_flat) + p_map_flat->sz;
}


//==========================================================
// Global API.
//

void
as_bin_set_empty_packed_map(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags)
{
	b->particle = map_particle_create(alloc_buf, 0, NULL, 0, flags);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_MAP);
}

bool
map_subcontext_by_index(cdt_context *ctx, msgpack_in_vec *val)
{
	int64_t index;
	packed_map map;
	uint32_t uindex;
	uint32_t count32;

	if (! msgpack_get_int64_vec(val, &index)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_index() invalid subcontext");
		return false;
	}

	if (! packed_map_init_from_ctx(&map, ctx, false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_index() invalid packed map");
		return false;
	}

	if (! calc_index_count(index, 1, map.ele_count, &uindex, &count32,
			false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_index() index %ld out of bounds for ele_count %u", index, map.ele_count);
		return false;
	}

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp idx
	setup_map_must_have_offidx(u, &map, alloc_idx);

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_index() invalid packed map");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t idx = uindex;

	if (! map_is_k_ordered(&map)) {
		define_build_order_heap_by_range(heap, uindex, count32, map.ele_count,
				&map, packed_map_compare_key_by_idx, success, alloc_idx);

		if (! success) {
			cf_warning(AS_PARTICLE, "map_subcontext_by_index() invalid packed map");
			rollback_alloc_rollback(alloc_idx);
			return false;
		}

		idx = order_index_get(&heap._, heap.filled);
	}
	else {
		cdt_context_map_push(ctx, &map, idx);
	}

	cdt_context_set_by_map_idx(ctx, &map, idx);
	rollback_alloc_rollback(alloc_idx);

	return true;
}

bool
map_subcontext_by_rank(cdt_context *ctx, msgpack_in_vec *val)
{
	int64_t rank;
	packed_map map;
	uint32_t urank;
	uint32_t count32;

	if (! msgpack_get_int64_vec(val, &rank)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_rank() invalid subcontext");
		return false;
	}

	if (! packed_map_init_from_ctx(&map, ctx, false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_rank() invalid packed map");
		return false;
	}

	if (! calc_index_count(rank, 1, map.ele_count, &urank, &count32, false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_rank() rank %ld out of bounds for ele_count %u", rank, map.ele_count);
		return false;
	}

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp idx
	setup_map_must_have_offidx(u, &map, alloc_idx);
	const order_index *ordidx = &map.ordidx;

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_rank() invalid packed map");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t idx;

	if (order_index_is_valid(ordidx)) {
		packed_map_ensure_ordidx_filled(&map);
		idx = order_index_get(ordidx, urank);
	}
	else {
		define_build_order_heap_by_range(heap, urank, count32, map.ele_count,
				&map, packed_map_compare_value_by_idx, success, alloc_idx);

		if (! success) {
			cf_warning(AS_PARTICLE, "map_subcontext_by_rank() invalid packed map");
			rollback_alloc_rollback(alloc_idx);
			return false;
		}

		idx = order_index_get(&heap._, heap.filled);
	}

	cdt_context_map_push(ctx, &map, idx);
	cdt_context_set_by_map_idx(ctx, &map, idx);
	rollback_alloc_rollback(alloc_idx);

	return true;
}

bool
map_subcontext_by_key(cdt_context *ctx, msgpack_in_vec *val)
{
	cdt_payload key;
	packed_map map;

	val->has_nonstorage = false;
	key.ptr = msgpack_get_ele_vec(val, &key.sz);

	if (key.ptr == NULL) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_key() invalid subcontext");
		return false;
	}

	if (! packed_map_init_from_ctx(&map, ctx, false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_key() invalid packed map");
		return false;
	}

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp idx
	setup_map_must_have_offidx(u, &map, alloc_idx);
	uint32_t index = 0;
	uint32_t count = 0;
	uint32_t hdr_sz = map.packed_sz - map.content_sz;

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_key() invalid packed map");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	if (map_is_k_ordered(&map)) {
		packed_map_get_range_by_key_interval_ordered(&map, &key, &key, &index,
				&count);

		if (count == 0) {
			if (ctx->create_flag_on && ! val->has_nonstorage) {
				ctx->create_triggered = true;
				ctx->create_hdr_ptr = map.packed;
				ctx->create_sz += key.sz;
				ctx->create_sz += cdt_hdr_delta_sz(map.ele_count + 1, 1); // +1 to include ext element pair

				cdt_context_map_push(ctx, &map, index);
				cdt_context_set_by_map_create(ctx, &map, index);

				return true;
			}

			if (ctx->create_flag_on && val->has_nonstorage) {
				cf_warning(AS_PARTICLE, "map_subcontext_by_key() cannot create key with non-storage element(s)");
			}
			else {
				cf_warning(AS_PARTICLE, "map_subcontext_by_key() key not found");
			}

			rollback_alloc_rollback(alloc_idx);

			return false;
		}

		cdt_context_map_push(ctx, &map, index);
		cdt_context_set_by_map_idx(ctx, &map, index);
	}
	else {
		map_ele_find find_key;

		map_ele_find_init(&find_key, &map);
		packed_map_find_key(&map, &find_key, &key);

		if (! find_key.found_key) {
			if (ctx->create_flag_on && ! val->has_nonstorage) {
				ctx->create_triggered = true;
				ctx->create_hdr_ptr = map.packed;
				ctx->create_sz += key.sz;
				ctx->create_sz += cdt_hdr_delta_sz(map.ele_count, 1);

				cdt_context_set_by_map_create(ctx, &map, 0);

				return true;
			}

			cf_warning(AS_PARTICLE, "map_subcontext_by_key() key not found");
			rollback_alloc_rollback(alloc_idx);
			return false;
		}

		ctx->data_offset += hdr_sz + find_key.value_offset;
		ctx->data_sz = find_key.sz -
				(find_key.value_offset - find_key.key_offset);
	}

	rollback_alloc_rollback(alloc_idx);

	return true;
}

bool
map_subcontext_by_value(cdt_context *ctx, msgpack_in_vec *val)
{
	cdt_payload value;
	packed_map map;

	value.ptr = msgpack_get_ele_vec(val, &value.sz);

	if (value.ptr == NULL) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_value() invalid subcontext");
		return false;
	}

	if (! packed_map_init_from_ctx(&map, ctx, false)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_value() invalid packed map");
		return false;
	}

	if (map.ele_count == 0) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_value() map is empty");
		return false;
	}

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp idx
	setup_map_must_have_offidx(u, &map, alloc_idx);

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_value() invalid packed map");
		return false;
	}

	uint32_t rank = 0;
	uint32_t count = 0;
	uint32_t idx;

	if (order_index_is_valid(&map.ordidx)) {
		packed_map_ensure_ordidx_filled(&map);
		packed_map_find_rank_range_by_value_interval_indexed(&map, &value,
				&value, &rank, &count, false);

		idx = order_index_get(&map.ordidx, rank);
	}
	else {
		uint64_t idx64;

		packed_map_find_rank_range_by_value_interval_unordered(&map, &value,
				&value, &rank, &count, &idx64, false);
		idx = (uint32_t)idx64;
	}

	if (count == 0) {
		cf_warning(AS_PARTICLE, "map_subcontext_by_value() value not found");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	cdt_context_map_push(ctx, &map, idx);
	cdt_context_set_by_map_idx(ctx, &map, idx);
	rollback_alloc_rollback(alloc_idx);

	return true;
}

void
cdt_context_unwind_map(cdt_context *ctx, cdt_ctx_list_stack_entry *p)
{
	if (ctx->b->particle == ctx->orig) { // no-op happened
		return;
	}

	packed_map map;
	packed_map orig;

	packed_map_init_from_ctx(&map, ctx, false);
	packed_map_init_from_particle(&orig, ctx->orig, false);

	bool make_new_indexes = (orig.ele_count <= 1 && map.ele_count > 1);

	if (make_new_indexes) {
		packed_map_check_and_fill_offidx(&map);
	}
	else if (orig.ele_count == map.ele_count) {
		offset_index_move_ele(&map.offidx, &orig.offidx, p->idx, p->idx);
	}
	else if (orig.ele_count + 1 == map.ele_count) {
		offset_index_add_ele(&map.offidx, &orig.offidx, p->idx);
	}
	else {
		cf_crash(AS_PARTICLE, "unwind doesn't support ele_count %u -> %u", orig.ele_count, map.ele_count);
	}

	if (! is_kv_ordered(orig.flags)) {
		return;
	}

	if (make_new_indexes || ! order_index_is_filled(&orig.ordidx)) {
		packed_map_ensure_ordidx_filled(&map);
		return;
	}

	uint32_t off = offset_index_get_const(&map.offidx, p->idx);

	msgpack_in mp = {
			.buf = map.contents,
			.buf_sz = map.content_sz,
			.offset = off
	};

	uint32_t check = msgpack_sz(&mp); // skip key
	cf_assert(check != 0, AS_PARTICLE, "invalid msgpack");

	cdt_payload v = {
			.ptr = mp.buf + mp.offset,
			.sz = mp.buf_sz - mp.offset
	};

	order_index_find find = {
			.target = p->idx,
			.count = orig.ele_count
	};

	order_index_find_rank_by_value(&orig.ordidx, &v, &orig.offidx, &find, true);

	uint32_t add_rank = find.result;
	uint32_t rm_rank = order_index_find_idx(&orig.ordidx, p->idx, 0,
			orig.ele_count);

	if (orig.ele_count + 1 == map.ele_count) {
		order_index_op_add(&map.ordidx, &orig.ordidx, p->idx, add_rank);
		return;
	}

	if (add_rank == rm_rank || add_rank == rm_rank + 1) {
		return;
	}

	order_index_op_replace1(&map.ordidx, &orig.ordidx, add_rank, rm_rank);
}

uint8_t
map_get_ctx_flags(uint8_t ctx_type, bool is_toplvl)
{
	ctx_type &= AS_CDT_CTX_CREATE_MASK;

	if (ctx_type == AS_CDT_CTX_CREATE_MAP_UNORDERED) {
		return 0;
	}

	if ((ctx_type & AS_CDT_CTX_CREATE_MAP_K_ORDERED) != 0) {
		uint8_t flags = is_toplvl ?
				(AS_PACKED_MAP_FLAG_K_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX):
				AS_PACKED_MAP_FLAG_K_ORDERED;

		if (ctx_type == AS_CDT_CTX_CREATE_MAP_KV_ORDERED) {
			flags |= is_toplvl ? (AS_PACKED_MAP_FLAG_V_ORDERED |
					AS_PACKED_MAP_FLAG_ORD_IDX) : AS_PACKED_MAP_FLAG_V_ORDERED;
		}

		return flags;
	}

	cf_crash(AS_PARTICLE, "unexpected");
	return 0;
}


//==========================================================
// Local helpers.
//

static inline bool
is_map_type(uint8_t type)
{
	return type == AS_PARTICLE_TYPE_MAP;
}

static inline bool
is_k_ordered(uint8_t flags)
{
	return flags & AS_PACKED_MAP_FLAG_K_ORDERED;
}

static inline bool
is_kv_ordered(uint8_t flags)
{
	return (flags & AS_PACKED_MAP_FLAG_KV_ORDERED) ==
			AS_PACKED_MAP_FLAG_KV_ORDERED;
}

static uint32_t
map_calc_ext_content_sz(uint8_t flags, uint32_t ele_count, uint32_t content_sz)
{
	uint32_t sz = 0;

	if (is_k_ordered(flags)) {
		offset_index offidx;

		offset_index_init(&offidx, NULL, ele_count, NULL, content_sz);
		sz += offset_index_size(&offidx);
	}

	if (is_kv_ordered(flags)) {
		order_index ordidx;

		order_index_init(&ordidx, NULL, ele_count);

		if (ele_count > 1) {
			sz += order_index_size(&ordidx);
		}
	}

	return sz;
}

static uint8_t
map_adjust_incoming_flags(uint8_t flags)
{
	static const uint8_t mask = AS_PACKED_MAP_FLAG_KV_ORDERED |
			AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX;

	if (is_k_ordered(flags)) {
		flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
	}

	if (is_kv_ordered(flags)) {
		flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
	}

	return flags & mask;
}

static inline uint32_t
map_allidx_vla_sz(const packed_map *map)
{
	uint32_t sz = 0;

	if (! offset_index_is_valid(&map->offidx)) {
		sz = offset_index_size(&map->offidx) +
				order_index_size(&map->ordidx);
	}
	else if (! order_index_is_valid(&map->ordidx)) {
		sz = order_index_size(&map->ordidx);
	}

	return cdt_vla_sz(sz);
}

static inline void
map_allidx_alloc_temp(const packed_map *map, uint8_t *mem_temp,
		rollback_alloc *alloc)
{
	offset_index *offidx = (offset_index *)&map->offidx;
	order_index *ordidx = (order_index *)&map->ordidx;

	if (! offset_index_is_valid(offidx)) {
		uint32_t off_sz = offset_index_size(offidx);
		uint32_t ord_sz = order_index_size(ordidx);

		if (off_sz + ord_sz > CDT_MAX_STACK_OBJ_SZ) {
			offidx->_.ptr = rollback_alloc_reserve(alloc, off_sz);
			ordidx->_.ptr = rollback_alloc_reserve(alloc, ord_sz);
		}
		else {
			offidx->_.ptr = mem_temp;
			ordidx->_.ptr = mem_temp + offset_index_size(offidx);
		}

		offset_index_set_filled(offidx, 1);
		order_index_set(ordidx, 0, map->ele_count);
	}
	else if (! order_index_is_valid(ordidx)) {
		uint32_t ord_sz = order_index_size(ordidx);

		if (ord_sz > CDT_MAX_STACK_OBJ_SZ) {
			ordidx->_.ptr = rollback_alloc_reserve(alloc, ord_sz);
		}
		else {
			ordidx->_.ptr = mem_temp;
		}

		order_index_set(ordidx, 0, map->ele_count);
	}
}

static inline uint32_t
map_ext_content_sz(const packed_map *map)
{
	return map_calc_ext_content_sz(map->flags, map->ele_count, map->content_sz);
}

static inline bool
map_is_k_ordered(const packed_map *map)
{
	return is_k_ordered(map->flags);
}

static inline bool
map_is_kv_ordered(const packed_map *map)
{
	return is_kv_ordered(map->flags);
}

static inline bool
map_has_offidx(const packed_map *map)
{
	return offset_index_is_valid(&map->offidx);
}

//------------------------------------------------
// cdt_context
//

static inline void
cdt_context_use_static_map_if_notinuse(cdt_context *ctx, uint8_t flags)
{
	if (ctx->create_triggered) {
		ctx->create_flags = flags;
		return;
	}

	if (! ctx->create_triggered && ! as_bin_is_live(ctx->b)) {
		if (is_kv_ordered(flags)) {
			ctx->b->particle = (as_particle *)&map_mem_empty_kv;
		}
		else if (is_k_ordered(flags)) {
			ctx->b->particle = (as_particle *)&map_mem_empty_k;
		}
		else {
			ctx->b->particle = (as_particle *)&map_mem_empty;
		}

		as_bin_state_set_from_type(ctx->b, AS_PARTICLE_TYPE_MAP);
	}
}

static inline void
cdt_context_set_empty_packed_map(cdt_context *ctx, uint8_t flags)
{
	if (cdt_context_is_toplvl(ctx) && ! ctx->create_triggered) {
		as_bin_set_empty_packed_map(ctx->b, ctx->alloc_buf, flags);
		return;
	}

	define_map_packer(mpk, 0, flags, 0);

	map_packer_create_particle_ctx(&mpk, ctx);
	map_packer_write_hdridx(&mpk);
}

static inline void
cdt_context_set_by_map_idx(cdt_context *ctx, const packed_map *map,
		uint32_t idx)
{
	msgpack_in mp = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = offset_index_get_const(&map->offidx, idx)
	};

	uint32_t endoff = offset_index_get_const(&map->offidx, idx + 1);

	msgpack_sz(&mp);
	ctx->data_offset += map->packed_sz - map->content_sz + mp.offset;
	ctx->data_sz = endoff - mp.offset;
}

static inline void
cdt_context_set_by_map_create(cdt_context *ctx, const packed_map *map,
		uint32_t idx)
{
	ctx->data_offset += map->packed_sz - map->content_sz +
			offset_index_get_const(&map->offidx, idx);
	ctx->data_sz = 0;
}

static inline void
cdt_context_map_push(cdt_context *ctx, const packed_map *map, uint32_t idx)
{
	if (cdt_context_is_modify(ctx) && cdt_context_is_toplvl(ctx) &&
			(map->flags & AS_PACKED_MAP_FLAG_OFF_IDX) != 0 &&
			(map->ele_count > 1 ||
					(map->ele_count == 1 && ctx->create_triggered))) {
		cdt_context_push(ctx, idx, NULL, AS_MAP);

		ctx->top_content_sz = map->content_sz;
		ctx->top_content_off = map->contents - map->packed;
		ctx->top_ele_count = map->ele_count;
	}
}

static inline void
cdt_context_map_handle_possible_noop(cdt_context *ctx)
{
	if (ctx->create_triggered) {
		cdt_context_set_empty_packed_map(ctx, ctx->create_flags);
	}
}

//------------------------------------------------
// map_packer

static as_particle *
map_packer_create_particle(map_packer *mpk, rollback_alloc *alloc_buf)
{
	uint32_t sz = mpk->ext_sz + mpk->content_sz +
			as_pack_map_header_get_size(mpk->ele_count + (mpk->flags ? 1 : 0));
	map_mem *p_map_mem = (map_mem *)(alloc_buf != NULL ?
			rollback_alloc_reserve(alloc_buf, sizeof(map_mem) + sz) :
			cf_malloc_ns(sizeof(map_mem) + sz));

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = sz;
	mpk->write_ptr = p_map_mem->data;

	return (as_particle *)p_map_mem;
}

static void
map_packer_create_particle_ctx(map_packer *mpk, cdt_context *ctx)
{
	if (ctx->data_sz == 0 && ! ctx->create_triggered) {
		ctx->b->particle = map_packer_create_particle(mpk, ctx->alloc_buf);
		return;
	}

	uint32_t map_sz = mpk->ext_sz + mpk->content_sz +
			as_pack_map_header_get_size(mpk->ele_count + (mpk->flags ? 1 : 0));
	mpk->write_ptr = cdt_context_create_new_particle(ctx, map_sz);
	mpk->ext_content_sz = 0; // no indexes for non-top context levels
	mpk->flags &= ~(AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX);
}

static void
map_packer_init(map_packer *mpk, uint32_t ele_count, uint8_t flags,
		uint32_t content_sz)
{
	mpk->ele_count = ele_count;
	mpk->content_sz = content_sz;
	mpk->ext_content_sz = 0;

	offset_index_init(&mpk->offidx, NULL, ele_count, NULL, content_sz);

	if (flags & AS_PACKED_MAP_FLAG_OFF_IDX) {
		mpk->ext_content_sz += offset_index_size(&mpk->offidx);
	}

	order_index_init(&mpk->ordidx, NULL, ele_count);

	if ((flags & AS_PACKED_MAP_FLAG_ORD_IDX) != 0 && ele_count > 1) {
		mpk->ext_content_sz += order_index_size(&mpk->ordidx);
	}

	mpk->flags = flags;

	if (flags == AS_PACKED_MAP_FLAG_NONE) {
		mpk->ext_header_sz = 0;
		mpk->ext_sz = 0;
	}
	else {
		mpk->ext_header_sz = as_pack_ext_header_get_size(mpk->ext_content_sz);
		mpk->ext_sz = mpk->ext_header_sz + mpk->ext_content_sz + 1; // +1 for packed nil
	}

	mpk->write_ptr = NULL;
	mpk->contents = NULL;
}

static void
map_packer_setup_bin(map_packer *mpk, cdt_op_mem *com)
{
	map_packer_create_particle_ctx(mpk, &com->ctx);
	map_packer_write_hdridx(mpk);
}

static void
map_packer_write_hdridx(map_packer *mpk)
{
	as_packer write = {
			.buffer = mpk->write_ptr,
			.capacity = INT_MAX
	};

	as_pack_map_header(&write, mpk->ele_count +
			(mpk->flags == AS_PACKED_MAP_FLAG_NONE ? 0 : 1));

	if (mpk->flags == AS_PACKED_MAP_FLAG_NONE) {
		mpk->write_ptr += write.offset;
		mpk->contents = mpk->write_ptr;
		mpk->offidx.contents = mpk->contents;
		return;
	}

	as_pack_ext_header(&write, mpk->ext_content_sz, mpk->flags);

	if (mpk->ext_content_sz > 0) {
		uint8_t *ptr = mpk->write_ptr + write.offset;
		uint32_t index_sz_left = mpk->ext_content_sz;
		uint32_t sz = offset_index_size(&mpk->offidx);

		if ((mpk->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_sz_left >= sz &&
				sz != 0) {
			offset_index_set_ptr(&mpk->offidx, ptr,
					ptr + mpk->ext_content_sz + 1); // +1 for nil pair
			ptr += sz;
			index_sz_left -= sz;
			offset_index_set_filled(&mpk->offidx, 1);
		}

		sz = order_index_size(&mpk->ordidx);

		if ((mpk->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_sz_left >= sz &&
				sz != 0 && mpk->ele_count > 1) {
			order_index_set_ptr(&mpk->ordidx, ptr);
			order_index_set(&mpk->ordidx, 0, mpk->ele_count);
		}
	}

	// Pack nil.
	write.offset += mpk->ext_content_sz;
	write.buffer[write.offset++] = msgpack_nil[0];

	mpk->write_ptr += write.offset;
	mpk->contents = mpk->write_ptr;
	mpk->offidx.contents = mpk->contents;
}

static void
map_packer_fill_offset_index(map_packer *mpk)
{
	cf_assert(offset_index_is_valid(&mpk->offidx), AS_PARTICLE, "invalid offidx");
	offset_index_set_filled(&mpk->offidx, 1);

	bool check = map_offset_index_check_and_fill(&mpk->offidx, mpk->ele_count);
	cf_assert(check, AS_PARTICLE, "invalid offidx");
}

// qsort_r callback function.
static int
map_packer_fill_index_sort_compare(const void *x, const void *y, void *p)
{
	index_sort_userdata *udata = (index_sort_userdata *)p;
	order_index *ordidx = udata->order;
	uint32_t x_idx = order_index_ptr2value(ordidx, x);
	uint32_t y_idx = order_index_ptr2value(ordidx, y);
	const offset_index *offidx = udata->offsets;
	const uint8_t *contents = udata->contents;
	uint32_t content_sz = udata->content_sz;
	uint32_t x_off = offset_index_get_const(offidx, x_idx);
	uint32_t y_off = offset_index_get_const(offidx, y_idx);

	msgpack_in x_mp = {
			.buf = contents,
			.buf_sz = content_sz,
			.offset = x_off
	};

	msgpack_in y_mp = {
			.buf = contents,
			.buf_sz = content_sz,
			.offset = y_off
	};

	msgpack_cmp_type cmp;

	if (udata->sort_by == SORT_BY_VALUE) {
		// Skip keys.
		if (msgpack_sz(&x_mp) == 0) {
			cf_crash(AS_PARTICLE, "invalid msgpack");
		}

		if (msgpack_sz(&y_mp) == 0) {
			cf_crash(AS_PARTICLE, "invalid msgpack");
		}

		cmp = msgpack_cmp_peek(&x_mp, &y_mp);
	}
	else if ((cmp = msgpack_cmp(&x_mp, &y_mp)) == MSGPACK_CMP_EQUAL) {
		cmp = msgpack_cmp_peek(&x_mp, &y_mp);
	}

	if (cmp == MSGPACK_CMP_LESS) {
		return -1;
	}

	if (cmp == MSGPACK_CMP_GREATER) {
		return 1;
	}

	return 0;
}

static void
map_packer_fill_ordidx(map_packer *mpk, const uint8_t *contents,
		uint32_t content_sz)
{
	if (order_index_is_null(&mpk->ordidx)) {
		return;
	}

	order_index_set_sorted(&mpk->ordidx, &mpk->offidx, contents, content_sz,
			SORT_BY_VALUE);
}

static void
map_packer_add_op_copy_index(map_packer *mpk, const packed_map_op *add_op,
		map_ele_find *remove_info, uint32_t kv_sz, const cdt_payload *value)
{
	if (add_op->new_ele_count == 0) { // no elements left
		return;
	}

	map_ele_find add_info;

	map_ele_find_init(&add_info, add_op->map);
	add_info.idx = remove_info->idx; // Find closest matching position for multiple same values.

	if (! offset_index_is_null(&mpk->offidx)) {
		if (! offset_index_is_full(&add_op->map->offidx)) {
			map_packer_fill_offset_index(mpk);
		}
		else {
			packed_map_op_write_new_offidx(add_op, remove_info, &add_info,
				&mpk->offidx, kv_sz);
		}
	}

	if (! order_index_is_null(&mpk->ordidx)) {
		if (! order_index_is_filled(&add_op->map->ordidx)) {
			map_packer_fill_ordidx(mpk, mpk->contents, mpk->content_sz);
			return;
		}

		if (remove_info->found_key) {
			packed_map_find_rank_indexed(add_op->map, remove_info);
			cf_assert(remove_info->found_value, AS_PARTICLE, "map_packer_add_op_copy_index() remove_info rank not found: idx=%u found=%d ele_count=%u", remove_info->idx, remove_info->found_key, add_op->map->ele_count);
		}

		packed_map_find_rank_by_value_indexed(add_op->map, &add_info, value);
		packed_map_op_write_new_ordidx(add_op, remove_info, &add_info,
				&mpk->ordidx);
	}
}

static inline void
map_packer_write_seg1(map_packer *mpk, const packed_map_op *op)
{
	mpk->write_ptr = packed_map_op_write_seg1(op, mpk->write_ptr);
}

static inline void
map_packer_write_seg2(map_packer *mpk, const packed_map_op *op)
{
	mpk->write_ptr = packed_map_op_write_seg2(op, mpk->write_ptr);
}

static inline void
map_packer_write_msgpack_seg(map_packer *mpk, const cdt_payload *seg)
{
	memcpy(mpk->write_ptr, seg->ptr, seg->sz);
	mpk->write_ptr += seg->sz;
}

//------------------------------------------------
// map_op

static int
map_set_flags(cdt_op_mem *com, uint8_t set_flags)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, false)) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint8_t map_flags = map.flags;
	uint32_t ele_count = map.ele_count;
	bool reorder = false;

	if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) ==
			AS_PACKED_MAP_FLAG_V_ORDERED) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid flags 0x%x", set_flags);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (is_kv_ordered(set_flags)) {
		if (! is_kv_ordered(map_flags)) {
			if (ele_count > 1 && ! is_k_ordered(map_flags)) {
				reorder = true;
			}

			map_flags |= AS_PACKED_MAP_FLAG_KV_ORDERED;

			if (cdt_context_is_toplvl(&com->ctx)) {
				map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
				map_flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
			}
		}
	}
	else if (is_k_ordered(set_flags)) {
		if (is_kv_ordered(map_flags)) {
			map_flags &= ~AS_PACKED_MAP_FLAG_V_ORDERED;
			map_flags &= ~AS_PACKED_MAP_FLAG_ORD_IDX;
		}
		else if (! is_k_ordered(map_flags)) {
			if (ele_count > 1) {
				reorder = true;
			}

			map_flags |= AS_PACKED_MAP_FLAG_K_ORDERED;

			if (cdt_context_is_toplvl(&com->ctx)) {
				map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
			}
		}
	}
	else if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) == 0) {
		map_flags &= ~AS_PACKED_MAP_FLAG_KV_ORDERED;
		map_flags &= ~AS_PACKED_MAP_FLAG_OFF_IDX;
		map_flags &= ~AS_PACKED_MAP_FLAG_ORD_IDX;
	}

	define_map_packer(mpk, ele_count, map_flags, map.content_sz);

	map_packer_setup_bin(&mpk, com);

	if (reorder) {
		setup_map_must_have_offidx(u, &map, com->alloc_idx);

		if (! packed_map_check_and_fill_offidx(&map)) {
			cf_warning(AS_PARTICLE, "map_set_flags() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		packed_map_write_k_ordered(&map, mpk.write_ptr, &mpk.offidx);
	}
	else {
		memcpy(mpk.write_ptr, map.contents, map.content_sz);

		if (! offset_index_is_null(&mpk.offidx)) {
			setup_map_must_have_offidx(u, &map, com->alloc_idx);

			if (! packed_map_check_and_fill_offidx(&map)) {
				cf_warning(AS_PARTICLE, "map_set_flags() invalid packed map");
				return -AS_ERR_PARAMETER;
			}

			offset_index_copy(&mpk.offidx, &map.offidx, 0, 0, ele_count, 0);
		}
	}

	if (! order_index_is_null(&mpk.ordidx)) {
		if (order_index_is_filled(&map.ordidx)) {
			order_index_copy(&mpk.ordidx, &map.ordidx, 0, 0, ele_count, NULL);
		}
		else {
			map_packer_fill_ordidx(&mpk, mpk.contents, mpk.content_sz);
		}
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_set_flags");
		map_print(&map, "original");
		cf_crash(AS_PARTICLE, "map_set_flags: ele_count %u", map.ele_count);
	}
#endif

	return AS_OK;
}

static int
map_increment(cdt_op_mem *com, const cdt_payload *key,
		const cdt_payload *delta_value, bool is_decrement)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_increment() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	setup_map_must_have_offidx(u, &map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_increment() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	map_ele_find find_key;
	cdt_calc_delta calc_delta;

	map_ele_find_init(&find_key, &map);

	if (! packed_map_find_key(&map, &find_key, key)) {
		cf_warning(AS_PARTICLE, "map_increment() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (! cdt_calc_delta_init(&calc_delta, delta_value, is_decrement)) {
		return -AS_ERR_PARAMETER;
	}

	if (find_key.found_key) {
		define_map_msgpack_in(mp_map_value, &map);

		mp_map_value.offset = find_key.value_offset;

		if (! cdt_calc_delta_add(&calc_delta, &mp_map_value)) {
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		if (! cdt_calc_delta_add(&calc_delta, NULL)) {
			return -AS_ERR_PARAMETER;
		}
	}

	uint8_t value_buf[CDT_MAX_PACKED_INT_SZ];
	cdt_payload value = { value_buf, CDT_MAX_PACKED_INT_SZ };

	cdt_calc_delta_pack_and_result(&calc_delta, &value, com->result.result);

	map_add_control control = {
			.allow_overwrite = true,
			.allow_create = true,
	};

	// TODO - possible improvement: offidx isn't saved for data-NOT-in-memory so
	// it will be recalculated again in map_add.
	return map_add(com, key, &value, &control, false);
}

static int
map_add(cdt_op_mem *com, const cdt_payload *key, const cdt_payload *value,
		const map_add_control *control, bool set_result)
{
	if (! cdt_check_storage_list_contents(key->ptr, key->sz, 1) ||
			! cdt_check_storage_list_contents(value->ptr, value->sz, 1)) {
		cf_warning(AS_PARTICLE, "map_add() invalid params");
		return -AS_ERR_PARAMETER;
	}

	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_add() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	setup_map_must_have_offidx(u, &map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_add() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	map_ele_find find_key_to_remove;
	map_ele_find_init(&find_key_to_remove, &map);

	if (! packed_map_find_key(&map, &find_key_to_remove, key)) {
		cf_warning(AS_PARTICLE, "map_add() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (find_key_to_remove.found_key) {
		// ADD for [unique] & [key exist].
		if (! control->allow_overwrite) {
			if (control->no_fail) {
				if (set_result) {
					as_bin_set_int(com->result.result, map.ele_count);
				}

				return AS_OK;
			}

			return -AS_ERR_ELEMENT_EXISTS;
		}
	}
	else {
		// REPLACE for ![key exist].
		if (! control->allow_create) {
			if (control->no_fail) {
				if (set_result) {
					as_bin_set_int(com->result.result, map.ele_count);
				}

				return AS_OK;
			}

			return -AS_ERR_ELEMENT_NOT_FOUND;
		}

		// Normal cases handled by packed_map_op_add():
		//  ADD for (![unique] & [key exist]) or ![key exist]
		//  PUT for all cases
		//  REPLACE for ([unique] & [key exist])
		//  UPDATE for ([unique] & [key exist]) or ![key exist]
	}

	define_packed_map_op(op, &map);
	uint32_t new_sz = packed_map_op_add(&op, &find_key_to_remove);
	uint32_t content_sz = new_sz + key->sz + value->sz;
	define_map_packer(mpk, op.new_ele_count, map.flags, content_sz);

	map_packer_setup_bin(&mpk, com);

	map_packer_write_seg1(&mpk, &op);
	map_packer_write_msgpack_seg(&mpk, key);
	map_packer_write_msgpack_seg(&mpk, value);
	map_packer_write_seg2(&mpk, &op);

	map_packer_add_op_copy_index(&mpk, &op, &find_key_to_remove,
			key->sz + value->sz, value);

	if (set_result) {
		as_bin_set_int(com->result.result, op.new_ele_count);
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "map_add");
		map_print(&map, "original");
		offset_index_print(&map.offidx, "map.offidx");
		order_index_print(&map.ordidx, "map.ordidx");
		offset_index_print(&mpk.offidx, "mpk.offidx");
		order_index_print(&mpk.ordidx, "mpk.ordidx");
		cf_crash(AS_PARTICLE, "map_add: ele_count %u no_fail %d do_partial %d allow_create %d allow_overwrite %d", map.ele_count, control->no_fail, control->do_partial, control->allow_create, control->allow_overwrite);
	}
#endif

	return AS_OK;
}

static int
map_add_items_unordered(const packed_map *map, cdt_op_mem *com,
		const offset_index *val_off, order_index *val_ord,
		const map_add_control *control)
{
	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
	define_cdt_idx_mask(found_mask, val_ord->max_idx, com->alloc_idx);
	uint32_t rm_count = 0;
	uint32_t rm_sz = 0;

	for (uint32_t i = 0; i < map->ele_count; i++) {
		uint32_t offset = offset_index_get_const(&map->offidx, i);

		cdt_payload value = {
				.ptr = map->contents + offset,
				.sz = map->content_sz - offset
		};

		order_index_find find = {
				.count = val_ord->max_idx,
				.target = 0 // find first occurrence of value
		};

		order_index_find_rank_by_value(val_ord, &value, val_off, &find, false);

		if (find.found) {
			cdt_idx_mask_set(found_mask, find.result);

			// ADD for [key exist].
			if (! control->allow_overwrite) {
				if (control->no_fail) {
					if (control->do_partial) {
						continue;
					}

					result_data_set_int(&com->result, map->ele_count);
					return AS_OK;
				}

				return -AS_ERR_ELEMENT_EXISTS;
			}

			cdt_idx_mask_set(rm_mask, i);
			rm_count++;
			rm_sz += offset_index_get_delta_const(&map->offidx, i);
		}
	}

	uint32_t dup_count;
	uint32_t dup_sz;

	order_index_sorted_mark_dup_eles(val_ord, val_off, &dup_count, &dup_sz);

	if (! control->allow_overwrite && control->no_fail && control->do_partial) {
		for (uint32_t i = 0; i < val_ord->max_idx; i++) {
			uint32_t idx = order_index_get(val_ord, i);

			if (idx == val_ord->max_idx) {
				continue;
			}

			if (cdt_idx_mask_is_set(found_mask, i)) {
				order_index_set(val_ord, i, val_ord->max_idx);
				dup_count++;
				dup_sz += offset_index_get_delta_const(val_off, idx);
			}
		}
	}

	if (! control->allow_create) {
		// REPLACE for ![key exist].
		if (cdt_idx_mask_bit_count(found_mask, val_ord->max_idx) !=
				val_ord->max_idx - dup_count) {
			if (control->no_fail) {
				if (control->do_partial) {
					for (uint32_t i = 0; i < val_ord->max_idx; i++) {
						uint32_t idx = order_index_get(val_ord, i);

						if (idx == val_ord->max_idx) {
							continue;
						}

						if (! cdt_idx_mask_is_set(found_mask, i)) {
							order_index_set(val_ord, i, val_ord->max_idx);
							dup_count++;
							dup_sz += offset_index_get_delta_const(val_off,
									idx);
						}
					}
				}
				else {
					result_data_set_int(&com->result, map->ele_count);
					return AS_OK;
				}
			}
			else {
				return -AS_ERR_ELEMENT_NOT_FOUND;
			}
		}
	}

	uint32_t new_ele_count = map->ele_count - rm_count +
			val_ord->max_idx - dup_count;
	uint32_t new_content_sz = map->content_sz - rm_sz +
			val_off->content_sz - dup_sz;
	define_map_packer(mpk, new_ele_count, map->flags, new_content_sz);

	map_packer_setup_bin(&mpk, com);
	mpk.write_ptr = cdt_idx_mask_write_eles(rm_mask, rm_count, &map->offidx,
			mpk.write_ptr, true);
	mpk.write_ptr = order_index_write_eles(val_ord, val_ord->max_idx, val_off,
			mpk.write_ptr, false);
	result_data_set_int(&com->result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "map_add_items_unordered");
		map_print(map, "original");
		offset_index_print(val_off, "val_off");
		order_index_print(val_ord, "val_ord");
		cf_crash(AS_PARTICLE, "ele_count %u dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", map->ele_count, dup_count, dup_sz, new_ele_count, new_content_sz);
	}
#endif

	return AS_OK;
}

static int
map_add_items_ordered(const packed_map *map, cdt_op_mem *com,
		const offset_index *val_off, order_index *val_ord,
		const map_add_control *control)
{
	uint32_t dup_count;
	uint32_t dup_sz;

	order_index_sorted_mark_dup_eles(val_ord, val_off, &dup_count, &dup_sz);

	if (map->ele_count == 0) {
		if (! control->allow_create) {
			if (! control->no_fail) {
				return -AS_ERR_ELEMENT_NOT_FOUND;
			}

			if (! control->do_partial) {
				result_data_set_int(&com->result, map->ele_count);
				return AS_OK;
			}
		}

		uint32_t new_content_sz = order_index_get_ele_size(val_ord,
				val_ord->max_idx, val_off);
		uint32_t new_ele_count = val_ord->max_idx - dup_count;
		define_map_packer(mpk, new_ele_count, map->flags, new_content_sz);

		map_packer_setup_bin(&mpk, com);
		order_index_write_eles(val_ord, val_ord->max_idx, val_off,
				mpk.write_ptr, false);

		if (! offset_index_is_null(&mpk.offidx)) {
			offset_index_set_filled(&mpk.offidx, 1);

			for (uint32_t i = 0; i < val_ord->max_idx; i++) {
				uint32_t val_idx = order_index_get(val_ord, i);

				if (val_idx == val_ord->max_idx) {
					continue;
				}

				uint32_t sz = offset_index_get_delta_const(val_off, val_idx);

				offset_index_append_size(&mpk.offidx, sz);
			}
		}

		if (! order_index_is_null(&mpk.ordidx)) {
			order_index_set(&mpk.ordidx, 0, new_ele_count);
		}

		result_data_set_int(&com->result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
		if (! map_verify(&com->ctx)) {
			cdt_bin_print(com->ctx.b, "map_add_items_ordered");
			map_print(map, "original");
			offset_index_print(val_off, "val_off");
			order_index_print(val_ord, "val_ord");
			cf_crash(AS_PARTICLE, "ele_count 0 dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", dup_count, dup_sz, new_ele_count, new_content_sz);
		}
#endif

		return AS_OK;
	}

	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
	uint32_t rm_count = 0;
	uint32_t rm_sz = 0;
	define_order_index2(insert_idx, map->ele_count, val_ord->max_idx,
			com->alloc_idx);

	for (uint32_t i = 0; i < val_ord->max_idx; i++) {
		uint32_t val_idx = order_index_get(val_ord, i);

		if (val_idx == val_ord->max_idx) {
			continue;
		}

		uint32_t off = offset_index_get_const(val_off, val_idx);
		uint32_t sz = offset_index_get_delta_const(val_off, val_idx);

		const cdt_payload value = {
				.ptr = val_off->contents + off,
				.sz = sz
		};

		map_ele_find find;
		map_ele_find_init(&find, map);

		packed_map_find_key_indexed(map, &find, &value);

		if (find.found_key) {
			// ADD for [key exist].
			if (! control->allow_overwrite) {
				if (control->no_fail) {
					if (control->do_partial) {
						order_index_set(val_ord, i, val_ord->max_idx); // skip this value
						dup_count++;
						dup_sz += sz;
						continue;
					}

					result_data_set_int(&com->result, map->ele_count);
					return AS_OK;
				}

				return -AS_ERR_ELEMENT_EXISTS;
			}

			if (! cdt_idx_mask_is_set(rm_mask, find.idx)) {
				cdt_idx_mask_set(rm_mask, find.idx);
				rm_count++;
				rm_sz += offset_index_get_delta_const(&map->offidx, find.idx);
			}
		}
		else {
			// REPLACE for ![key exist].
			if (! control->allow_create) {
				if (control->no_fail) {
					if (control->do_partial) {
						order_index_set(val_ord, i, val_ord->max_idx); // skip this value
						dup_count++;
						dup_sz += sz;
						continue;
					}

					result_data_set_int(&com->result, map->ele_count);
					return AS_OK;
				}

				return -AS_ERR_ELEMENT_NOT_FOUND;
			}
		}

		cf_assert(find.idx <= map->ele_count, AS_PARTICLE, "Invalid find.idx %u > ele_count %u", find.idx, map->ele_count);
		order_index_set(&insert_idx, i, find.idx);
	}

	uint32_t new_ele_count = map->ele_count - rm_count + val_ord->max_idx -
			dup_count;
	uint32_t new_content_sz = map->content_sz - rm_sz + val_off->content_sz -
			dup_sz;
	define_map_packer(mpk, new_ele_count, map->flags, new_content_sz);
	uint32_t start_off = 0;

	map_packer_setup_bin(&mpk, com);

	for (uint32_t i = 0; i < val_ord->max_idx; i++) {
		uint32_t val_idx = order_index_get(val_ord, i);

		if (val_idx == val_ord->max_idx) {
			continue;
		}

		uint32_t index = order_index_get(&insert_idx, i);
		uint32_t off = offset_index_get_const(&map->offidx, index);

		if (start_off < off) {
			uint32_t sz = off - start_off;

			memcpy(mpk.write_ptr, map->contents + start_off, sz);
			mpk.write_ptr += sz;

			if (index == map->ele_count) {
				start_off = map->content_sz;
			}
			else if (cdt_idx_mask_is_set(rm_mask, index)) {
				start_off = offset_index_get_const(&map->offidx, index + 1);
			}
			else {
				start_off = off;
			}
		}
		else if (index == map->ele_count) {
			start_off = map->content_sz;
		}
		else if (start_off == off && cdt_idx_mask_is_set(rm_mask, index)) {
			start_off = offset_index_get_const(&map->offidx, index + 1);
		}

		uint32_t val_offset = offset_index_get_const(val_off, val_idx);
		uint32_t val_sz = offset_index_get_delta_const(val_off, val_idx);

		memcpy(mpk.write_ptr, val_off->contents + val_offset, val_sz);
		mpk.write_ptr += val_sz;
	}

	uint32_t sz = map->content_sz - start_off;

	if (sz != 0) {
		memcpy(mpk.write_ptr, map->contents + start_off, sz);
	}

	if (! offset_index_is_null(&mpk.offidx)) {
		uint32_t read_index = 0;
		uint32_t write_index = 1;
		int delta = 0;

		offset_index_set_filled(&mpk.offidx, 1);

		for (uint32_t i = 0; i < val_ord->max_idx; i++) {
			uint32_t val_idx = order_index_get(val_ord, i);

			if (val_idx == val_ord->max_idx) {
				continue;
			}

			uint32_t index = order_index_get(&insert_idx, i);

			if (index > read_index) {
				uint32_t count = index - read_index;

				if (read_index + count == map->ele_count) {
					count--;
				}

				offset_index_copy(&mpk.offidx, &map->offidx, write_index,
						read_index + 1, count, delta);
				write_index += count;
				read_index += count;
				offset_index_set_filled(&mpk.offidx, write_index);

				if (index != map->ele_count &&
						cdt_idx_mask_is_set(rm_mask, index)) {
					read_index++;
					delta -= offset_index_get_delta_const(&map->offidx, index);
				}
			}
			else if (index != map->ele_count && index == read_index &&
					cdt_idx_mask_is_set(rm_mask, index)) {
				read_index++;
				delta -= offset_index_get_delta_const(&map->offidx, index);
			}

			uint32_t sz = offset_index_get_delta_const(val_off, val_idx);

			offset_index_append_size(&mpk.offidx, sz);
			write_index++;
			delta += sz;
		}

		if (read_index + 1 < map->ele_count && write_index < new_ele_count) {
			offset_index_copy(&mpk.offidx, &map->offidx, write_index,
					read_index + 1, map->ele_count - read_index - 1, delta);
		}

		offset_index_set_filled(&mpk.offidx, map->ele_count);
	}

	if (! order_index_is_null(&mpk.ordidx)) {
		order_index_set(&mpk.ordidx, 0, new_ele_count);
	}

	result_data_set_int(&com->result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "map_add_items_ordered");
		map_print(map, "original");
		offset_index_print(val_off, "val_off");
		order_index_print(val_ord, "val_ord");
		cf_crash(AS_PARTICLE, "ele_count %u dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", map->ele_count, dup_count, dup_sz, new_ele_count, new_content_sz);
	}
#endif

	return AS_OK;
}

static int
map_add_items(cdt_op_mem *com, const cdt_payload *items,
		const map_add_control *control)
{
	msgpack_in mp = {
			.buf = items->ptr,
			.buf_sz = items->sz
	};

	uint32_t items_count;

	if (! msgpack_get_map_ele_count(&mp, &items_count)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid parameter, expected packed map");
		return -AS_ERR_PARAMETER;
	}

	if (items_count != 0 && msgpack_peek_is_ext(&mp)) {
		if (msgpack_sz(&mp) == 0 || msgpack_sz(&mp) == 0) {
			cf_warning(AS_PARTICLE, "map_add_items() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		items_count--;
	}

	const uint8_t *val_contents = mp.buf + mp.offset;
	uint32_t val_content_sz = mp.buf_sz - mp.offset;
	uint32_t val_count = items_count;
	define_order_index(val_ord, val_count, com->alloc_idx);
	define_offset_index(val_off, val_contents, val_content_sz, val_count,
			com->alloc_idx);

	if (! map_offset_index_check_and_fill(&val_off, val_count)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid parameter");
		return -AS_ERR_PARAMETER;
	}

	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (val_count == 0) {
		result_data_set_int(&com->result, map.ele_count);
		cdt_context_map_handle_possible_noop(&com->ctx);
		return AS_OK; // no-op
	}

	setup_map_must_have_offidx(u, &map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(&map)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	order_index_set_sorted(&val_ord, &val_off, val_contents, val_content_sz,
			SORT_BY_KEY);

	if (map_is_k_ordered(&map)) {
		return map_add_items_ordered(&map, com, &val_off, &val_ord, control);
	}

	return map_add_items_unordered(&map, com, &val_off, &val_ord, control);
}

static int
map_remove_by_key_interval(cdt_op_mem *com, const cdt_payload *key_start,
		const cdt_payload *key_end)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_key_interval() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_key_interval(&map, com, key_start, key_end);
}

static int
map_remove_by_index_range(cdt_op_mem *com, int64_t index, uint64_t count)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_index_range(&map, com, index, count);
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
map_remove_by_value_interval(cdt_op_mem *com, const cdt_payload *value_start,
		const cdt_payload *value_end)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_value_interval() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_value_interval(&map, com, value_start,
			value_end);
}

static int
map_remove_by_rank_range(cdt_op_mem *com, int64_t rank, uint64_t count)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rank_range(&map, com, rank, count);
}

static int
map_remove_by_rel_index_range(cdt_op_mem *com, const cdt_payload *key,
		int64_t index, uint64_t count)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_remove_by_rel_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rel_index_range(&map, com, key, index,
			count);
}

static int
map_remove_by_rel_rank_range(cdt_op_mem *com, const cdt_payload *value,
		int64_t rank, uint64_t count)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_remove_by_rel_rank_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rel_rank_range(&map, com, value, rank,
			count);
}

static int
map_remove_all_by_key_list(cdt_op_mem *com, const cdt_payload *key_list)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_remove_all_by_key_list() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_all_by_key_list(&map, com, key_list);
}

static int
map_remove_all_by_value_list(cdt_op_mem *com, const cdt_payload *value_list)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, true)) {
		cf_warning(AS_PARTICLE, "map_get_remove_all_value_items() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_all_by_value_list(&map, com, value_list);
}

static int
map_clear(cdt_op_mem *com)
{
	packed_map map;

	if (! packed_map_init_from_com(&map, com, false)) {
		cf_warning(AS_PARTICLE, "packed_map_clear() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	define_map_packer(mpk, 0, map.flags, 0);

	map_packer_setup_bin(&mpk, com);

	return AS_OK;
}

//------------------------------------------------
// packed_map

static bool
packed_map_init(packed_map *map, const uint8_t *buf, uint32_t sz,
		bool fill_idxs)
{
	map->packed = buf;
	map->packed_sz = sz;

	map->ele_count = 0;

	return packed_map_unpack_hdridx(map, fill_idxs);
}

static inline bool
packed_map_init_from_particle(packed_map *map, const as_particle *p,
		bool fill_idxs)
{
	const map_mem *p_map_mem = (const map_mem *)p;
	return packed_map_init(map, p_map_mem->data, p_map_mem->sz, fill_idxs);
}

static bool
packed_map_init_from_bin(packed_map *map, const as_bin *b, bool fill_idxs)
{
	uint8_t type = as_bin_get_particle_type(b);

	cf_assert(is_map_type(type), AS_PARTICLE, "packed_map_init_from_bin() invalid type %d", type);

	return packed_map_init_from_particle(map, b->particle, fill_idxs);
}

static bool
packed_map_init_from_ctx(packed_map *map, const cdt_context *ctx,
		bool fill_idxs)
{
	if (cdt_context_is_toplvl(ctx)) {
		return packed_map_init_from_bin(map, ctx->b, fill_idxs);
	}

	const cdt_mem *p_cdt_mem = (const cdt_mem *)ctx->b->particle;

	map->packed = p_cdt_mem->data + ctx->data_offset + ctx->delta_off;
	map->packed_sz = ctx->data_sz + ctx->delta_sz;
	map->ele_count = 0;

	return packed_map_unpack_hdridx(map, fill_idxs);
}

static inline bool
packed_map_init_from_com(packed_map *map, cdt_op_mem *com, bool fill_idxs)
{
	const cdt_context *ctx = &com->ctx;

	if (ctx->create_triggered) {
		if (is_kv_ordered(ctx->create_flags)) {
			map->packed = map_mem_empty_kv.data;
			map->packed_sz = map_mem_empty_kv.sz;
		}
		else if (is_k_ordered(ctx->create_flags)) {
			map->packed = map_mem_empty_k.data;
			map->packed_sz = map_mem_empty_k.sz;
		}
		else {
			map->packed = map_mem_empty.data;
			map->packed_sz = map_mem_empty.sz;
		}

		if (! packed_map_unpack_hdridx(map, false)) {
			cf_crash(AS_PARTICLE, "unexpected");
		}

		map->flags &=
				~(AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX); // strip index flags since created context is never top level

		return true;
	}

	return packed_map_init_from_ctx(map, &com->ctx, fill_idxs);
}

static bool
packed_map_unpack_hdridx(packed_map *map, bool fill_idxs)
{
	msgpack_in mp = {
			.buf = map->packed,
			.buf_sz = map->packed_sz
	};

	if (map->packed_sz == 0) {
		map->flags = 0;
		return false;
	}

	uint32_t ele_count;

	if (! msgpack_get_map_ele_count(&mp, &ele_count)) {
		return false;
	}

	map->ele_count = ele_count;

	if (ele_count != 0 && msgpack_peek_is_ext(&mp)) {
		msgpack_ext ext;

		if (! msgpack_get_ext(&mp, &ext)) {
			return false;
		}

		if (msgpack_sz(&mp) == 0) { // skip the packed nil
			return false;
		}

		map->flags = ext.type;
		map->ele_count--;

		map->contents = map->packed + mp.offset;
		map->content_sz = map->packed_sz - mp.offset;
		offset_index_init(&map->offidx, NULL, map->ele_count, map->contents,
				map->content_sz);
		order_index_init(&map->ordidx, NULL, map->ele_count);

		uint32_t index_sz_left = ext.size;
		uint8_t *ptr = (uint8_t *)ext.data;
		uint32_t sz = offset_index_size(&map->offidx);

		if ((map->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_sz_left >= sz &&
				sz != 0) {
			offset_index_set_ptr(&map->offidx, ptr, map->packed + mp.offset);
			ptr += sz;
			index_sz_left -= sz;

			if (fill_idxs && ! packed_map_check_and_fill_offidx(map)) {
				return false;
			}
		}

		sz = order_index_size(&map->ordidx);

		if ((map->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_sz_left >= sz &&
				map->ele_count > 1) {
			order_index_set_ptr(&map->ordidx, ptr);
		}
	}
	else {
		map->contents = map->packed + mp.offset;
		map->content_sz = map->packed_sz - mp.offset;

		offset_index_init(&map->offidx, NULL, ele_count, map->contents,
				map->content_sz);
		order_index_init(&map->ordidx, NULL, ele_count);
		map->flags = AS_PACKED_MAP_FLAG_NONE;
	}

	return true;
}

static void
packed_map_init_indexes(const packed_map *map, as_packer *pk,
		offset_index *offidx, order_index *ordidx)
{
	cf_assert(map_is_k_ordered(map), AS_PARTICLE, "not at least k ordered");

	uint8_t *ptr = pk->buffer + pk->offset;

	offset_index_init(offidx, ptr, map->ele_count, map->contents,
			map->content_sz);

	uint32_t offidx_sz = offset_index_size(offidx);

	ptr += offidx_sz;
	offset_index_set_filled(offidx, 1);
	pk->offset += offidx_sz;

	if (map_is_kv_ordered(map) && map->ele_count > 1) {
		order_index_init(ordidx, ptr, map->ele_count);
		pk->offset += order_index_size(ordidx);

		if (ordidx->max_idx != 0) {
			order_index_set(ordidx, 0, map->ele_count);
		}
	}
}

static void
packed_map_ensure_ordidx_filled(const packed_map *map)
{
	cf_assert(offset_index_is_full(&map->offidx), AS_PARTICLE, "offidx not full");
	order_index *ordidx = (order_index *)&map->ordidx;

	if (! order_index_is_filled(ordidx)) {
		order_index_set_sorted(ordidx, &map->offidx, map->contents,
				map->content_sz, SORT_BY_VALUE);
	}
}

static bool
packed_map_check_and_fill_offidx(const packed_map *map)
{
	return map_offset_index_check_and_fill((offset_index *)&map->offidx,
			map->ele_count);
}

static uint32_t
packed_map_find_index_by_idx_unordered(const packed_map *map, uint32_t idx)
{
	uint32_t offset = offset_index_get_const(&map->offidx, idx);

	cdt_payload key = {
			.ptr = map->contents + offset,
			.sz = map->content_sz - offset
	};

	return packed_map_find_index_by_key_unordered(map, &key);
}

static uint32_t
packed_map_find_index_by_key_unordered(const packed_map *map,
		const cdt_payload *key)
{
	msgpack_in mp_key = {
			.buf = key->ptr,
			.buf_sz = key->sz
	};

	uint32_t index = 0;
	define_map_msgpack_in(mp, map);

	for (uint32_t i = 0; i < map->ele_count; i++) {
		mp_key.offset = 0;
		msgpack_cmp_type cmp = msgpack_cmp(&mp, &mp_key);

		if (cmp == MSGPACK_CMP_ERROR) {
			return map->ele_count;
		}

		if (cmp == MSGPACK_CMP_LESS) {
			index++;
		}

		if (msgpack_sz(&mp) == 0) {
			return map->ele_count;
		}
	}

	return index;
}

static void
packed_map_find_rank_indexed_linear(const packed_map *map, map_ele_find *find,
		uint32_t start, uint32_t len)
{
	uint32_t rank = order_index_find_idx(&map->ordidx, find->idx, start,
			len);

	if (rank < start + len) {
		find->found_value = true;
		find->rank = rank;
	}
}

// Find rank given index (find->idx).
static void
packed_map_find_rank_indexed(const packed_map *map, map_ele_find *find)
{
	uint32_t ele_count = map->ele_count;

	if (ele_count == 0) {
		return;
	}

	if (find->idx >= ele_count) {
		find->found_value = false;
		return;
	}

	const offset_index *offset_idx = &map->offidx;
	const order_index *value_idx = &map->ordidx;

	uint32_t rank = ele_count / 2;
	uint32_t upper = ele_count;
	uint32_t lower = 0;

	msgpack_in mp_value = {
			.buf = map->contents + find->value_offset,
			.buf_sz = find->key_offset + find->sz - find->value_offset
	};

	find->found_value = false;

	while (true) {
		if (upper - lower < LINEAR_FIND_RANK_MAX_COUNT) {
			packed_map_find_rank_indexed_linear(map, find, lower,
					upper - lower);
			return;
		}

		uint32_t idx = order_index_get(value_idx, rank);

		if (find->idx == idx) {
			find->found_value = true;
			find->rank = rank;
			break;
		}

		msgpack_in mp_buf = {
				.buf = map->contents,
				.buf_sz = map->content_sz,
				.offset = offset_index_get_const(offset_idx, idx)
		};

		if (msgpack_sz(&mp_buf) == 0) { // skip key
			cf_crash(AS_PARTICLE, "packed_map_find_rank_indexed() unpack key failed at rank=%u", rank);
		}

		msgpack_cmp_type cmp = msgpack_cmp_peek(&mp_value, &mp_buf);

		if (cmp == MSGPACK_CMP_EQUAL) {
			if (find->idx < idx) {
				cmp = MSGPACK_CMP_LESS;
			}
			else if (find->idx > idx) {
				cmp = MSGPACK_CMP_GREATER;
			}

			find->found_value = true;
		}

		if (cmp == MSGPACK_CMP_EQUAL) {
			find->rank = rank;
			break;
		}

		if (cmp == MSGPACK_CMP_GREATER) {
			if (rank >= upper - 1) {
				find->rank = rank + 1;
				break;
			}

			lower = rank + 1;
			rank += upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_CMP_LESS) {
			if (rank == lower) {
				find->rank = rank;
				break;
			}

			upper = rank;
			rank += lower;
			rank /= 2;
		}
		else {
			cf_crash(AS_PARTICLE, "packed_map_find_rank_indexed() error=%d lower=%u rank=%u upper=%u", (int)cmp, lower, rank, upper);
		}
	}
}

static void
packed_map_find_rank_by_value_indexed(const packed_map *map,
		map_ele_find *map_find, const cdt_payload *value)
{
	order_index_find find = {
			.target = map_find->idx,
			.count = map->ele_count
	};

	order_index_find_rank_by_value(&map->ordidx, value, &map->offidx, &find,
			true);

	map_find->found_value = find.found;
	map_find->rank = find.result;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static void
packed_map_find_rank_range_by_value_interval_indexed(const packed_map *map,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank, uint32_t *count, bool is_multi)
{
	cf_assert(map_has_offidx(map), AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_indexed() offset_index needs to be valid");

	map_ele_find find_start;

	map_ele_find_init(&find_start, map);
	find_start.idx = 0; // find least ranked entry with value == value_start
	packed_map_find_rank_by_value_indexed(map, &find_start, value_start);

	*rank = find_start.rank;
	*count = 1;

	if (! value_end || ! value_end->ptr) {
		*count = map->ele_count - *rank;
	}
	else {
		map_ele_find find_end;

		map_ele_find_init(&find_end, map);

		if (value_end != value_start) {
			find_end.idx = 0;
			packed_map_find_rank_by_value_indexed(map, &find_end, value_end);
			*count = (find_end.rank > find_start.rank) ?
					find_end.rank - find_start.rank : 0;
		}
		else {
			if (! find_start.found_value) {
				*count = 0;
			}
			else if (is_multi) {
				find_end.idx = map->ele_count; // find highest ranked entry with value == value_start
				packed_map_find_rank_by_value_indexed(map, &find_end,
						value_start);
				*count = find_end.rank - find_start.rank;
			}
		}
	}
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static void
packed_map_find_rank_range_by_value_interval_unordered(const packed_map *map,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank, uint32_t *count, uint64_t *mask, bool is_multi)
{
	cf_assert(value_end, AS_PARTICLE, "value_end == NULL");

	msgpack_in mp_start = {
			.buf = value_start->ptr,
			.buf_sz = value_start->sz
	};

	msgpack_in mp_end = {
			.buf = value_end->ptr,
			.buf_sz = value_end->sz
	};

	bool can_early_exit = false;
	uint32_t temp_rank;

	if (rank == NULL) {
		rank = &temp_rank;
		can_early_exit = true;
	}

	*rank = 0;
	*count = 0;

	define_map_msgpack_in(mp, map);

	cf_assert(offset_index_is_full(&map->offidx), AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() offset_index needs to be full");

	for (uint32_t i = 0; i < map->ele_count; i++) {
		if (msgpack_sz(&mp) == 0) { // skip key
			cf_crash(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);
		}

		uint32_t value_offset = mp.offset; // save for mp_end

		mp_start.offset = 0; // reset

		msgpack_cmp_type cmp_start = msgpack_cmp(&mp, &mp_start);

		cf_assert(cmp_start != MSGPACK_CMP_ERROR, AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);

		if (cmp_start == MSGPACK_CMP_LESS) {
			(*rank)++;
		}
		else if (value_start != value_end) {
			msgpack_cmp_type cmp_end = MSGPACK_CMP_LESS;

			// NULL value_end means largest possible value.
			if (value_end->ptr) {
				mp.offset = value_offset;
				mp_end.offset = 0;
				cmp_end = msgpack_cmp(&mp, &mp_end);
			}

			if (cmp_end == MSGPACK_CMP_LESS) {
				if (mask != NULL) {
					cdt_idx_mask_set(mask, i);
				}

				(*count)++;
			}
		}
		// Single value case.
		else if (cmp_start == MSGPACK_CMP_EQUAL) {
			if (! is_multi) {
				if (*count == 0) {
					if (mask != NULL) {
						*mask = i;
					}

					*count = 1;

					if (can_early_exit) {
						break;
					}
				}

				continue;
			}

			if (mask != NULL) {
				cdt_idx_mask_set(mask, i);
			}

			(*count)++;
		}
	}
}

// Find key given list index.
static void
packed_map_find_key_indexed(const packed_map *map, map_ele_find *find,
		const cdt_payload *key)
{
	const offset_index *offidx = &map->offidx;
	uint32_t ele_count = map->ele_count;

	find->lower = 0;
	find->upper = ele_count;

	uint32_t idx = (find->lower + find->upper) / 2;

	msgpack_in mp_key = {
			.buf = key->ptr,
			.buf_sz = key->sz
	};

	find->found_key = false;

	if (ele_count == 0) {
		find->idx = 0;
		return;
	}

	while (true) {
		uint32_t offset = offset_index_get_const(offidx, idx);
		uint32_t content_sz = map->content_sz;
		uint32_t sz = content_sz - offset;

		msgpack_in mp_buf = {
				.buf = map->contents + offset,
				.buf_sz = sz
		};

		mp_key.offset = 0; // reset

		msgpack_cmp_type cmp = msgpack_cmp(&mp_key, &mp_buf);
		uint32_t key_sz = mp_buf.offset;

		if (cmp == MSGPACK_CMP_EQUAL) {
			if (! find->found_key) {
				find->found_key = true;
				find->key_offset = offset;
				find->value_offset = offset + key_sz;
				find->idx = idx++;
				find->sz = (idx >= ele_count) ?
						sz : offset_index_get_const(offidx, idx) - offset;
			}

			break;
		}

		if (cmp == MSGPACK_CMP_GREATER) {
			if (idx >= find->upper - 1) {
				if (++idx >= ele_count) {
					find->key_offset = content_sz;
					find->value_offset = content_sz;
					find->idx = idx;
					find->sz = 0;
					break;
				}

				if (! find->found_key) {
					uint32_t offset = offset_index_get_const(offidx, idx);
					uint32_t tail = content_sz - offset;

					msgpack_in mp = {
							.buf = map->contents + offset,
							.buf_sz = tail
					};

					if (msgpack_sz(&mp) == 0) {
						cf_crash(AS_PARTICLE, "packed_map_find_key_indexed() invalid packed map");
					}

					find->key_offset = offset;
					find->value_offset = offset + mp.offset;
					find->idx = idx++;
					find->sz = (idx >= ele_count) ?
							tail : offset_index_get_const(offidx, idx) - offset;
				}

				break;
			}

			find->lower = idx + 1;
			idx += find->upper;
			idx /= 2;
		}
		else if (cmp == MSGPACK_CMP_LESS) {
			if (idx == find->lower) {
				find->key_offset = offset;
				find->value_offset = offset + key_sz;
				find->idx = idx++;
				find->sz = (idx >= ele_count) ?
						sz : offset_index_get_const(offidx, idx) - offset;
				break;
			}

			find->upper = idx;
			idx += find->lower;
			idx /= 2;
		}
		else {
			cf_crash(AS_PARTICLE, "packed_map_find_key_indexed() compare error=%d", (int)cmp);
		}
	}
}

static bool
packed_map_find_key(const packed_map *map, map_ele_find *find,
		const cdt_payload *key)
{
	uint32_t ele_count = map->ele_count;
	const offset_index *offidx = (const offset_index *)&map->offidx;

	if (ele_count == 0) {
		return true;
	}

	cf_assert(offset_index_is_full(offidx), AS_PARTICLE, "offidx not full");

	if (map_is_k_ordered(map)) {
		packed_map_find_key_indexed(map, find, key);
		return true;
	}

	msgpack_in mp_key = {
			.buf = key->ptr,
			.buf_sz = key->sz
	};

	find->found_key = false;

	define_map_msgpack_in(mp, map);
	uint32_t content_sz = mp.buf_sz;

	// Unordered compare.
	// Assumes same keys are clustered (for future multi-map support).
	for (uint32_t i = 0; i < ele_count; i++) {
		mp.offset = offset_index_get_const(offidx, i);

		uint32_t key_offset = mp.offset;
		msgpack_cmp_type cmp = msgpack_cmp_peek(&mp_key, &mp);

		cf_assert(cmp != MSGPACK_CMP_ERROR, AS_PARTICLE, "compare error");

		if (cmp == MSGPACK_CMP_EQUAL) {
			// Get value offset.
			if (msgpack_sz(&mp) == 0) {
				cf_warning(AS_PARTICLE, "msgpack error at offset %u", mp.offset);
				return false;
			}

			if (! find->found_key) {
				find->found_key = true;
				find->idx = i;
				find->key_offset = key_offset;
				find->value_offset = mp.offset;
				find->sz = offset_index_get_const(offidx, i + 1) - key_offset;
			}

			return true;
		}
		else if (find->found_key) {
			return true;
		}
	}

	find->key_offset = content_sz;
	find->value_offset = content_sz;
	find->sz = 0;
	find->idx = ele_count;
	return true;
}

static int
packed_map_get_remove_by_key_interval(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *key_start, const cdt_payload *key_end)
{
	cdt_result_data *result = &com->result;

	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);
	uint32_t index = 0;
	uint32_t count = 0;

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	cf_assert(key_start->ptr != NULL, AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid start key");

	if (map_is_k_ordered(map)) {
		packed_map_get_range_by_key_interval_ordered(map, key_start, key_end,
				&index, &count);

		if (count == 0 && ! result->is_multi) {
			if (! result_data_set_key_not_found(result, -1)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid result_type %d", result->type);
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			return AS_OK;
		}

		return packed_map_get_remove_by_index_range(map, com, index, count);
	}

	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (key_start == key_end) {
		map_ele_find find_key;

		map_ele_find_init(&find_key, map);
		packed_map_find_key(map, &find_key, key_start);

		if (! find_key.found_key) {
			if (! result_data_set_key_not_found(result, -1)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid result_type %d", result->type);
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			return AS_OK;
		}

		if (cdt_op_is_modify(com)) {
			define_packed_map_op(op, map);
			uint32_t new_sz = packed_map_op_remove(&op, &find_key, 1,
					find_key.sz);
			define_map_packer(mpk, op.new_ele_count, map->flags, new_sz);

			map_packer_setup_bin(&mpk, com);
			map_packer_write_seg1(&mpk, &op);
			map_packer_write_seg2(&mpk, &op);
		}

#ifdef MAP_DEBUG_VERIFY
		if (! map_verify(&com->ctx)) {
			cdt_bin_print(com->ctx.b, "packed_map_get_remove_by_key_interval");
			map_print(map, "original");
			cf_crash(AS_PARTICLE, "ele_count %u index %u count 1 is_multi %d inverted %d", map->ele_count, index, result->is_multi, inverted);
		}
#endif

		return packed_map_build_result_by_key(map, key_start, find_key.idx,
				1, result);
	}

	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);

	packed_map_get_range_by_key_interval_unordered(map, key_start, key_end,
			&index, &count, rm_mask);

	uint32_t rm_count = count;

	if (inverted) {
		rm_count = map->ele_count - count;
		cdt_idx_mask_invert(rm_mask, map->ele_count);
	}

	uint32_t rm_sz = 0;
	int ret = AS_OK;

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	if (result_data_is_return_elements(result)) {
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count, rm_sz,
				result)) {
			return -AS_ERR_UNKNOWN;
		}
	}
	else if (result_data_is_return_rank(result)) {
		packed_map_build_rank_result_by_mask(map, rm_mask, rm_count,
				result);
	}
	else {
		ret = result_data_set_range(result, index, count, map->ele_count);
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_by_key_interval");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u index %u count %u rm_count %u inverted %d", map->ele_count, index, count, rm_count, inverted);
	}
#endif

	return AS_OK;
}

static int
packed_map_trim_ordered(const packed_map *map, cdt_op_mem *com, uint32_t index,
		uint32_t count)
{
	cdt_result_data *result = &com->result;

	cf_assert(result->is_multi, AS_PARTICLE, "packed_map_trim_ordered() required to be a multi op");
	cf_assert(! result_data_is_inverted(result), AS_PARTICLE, "packed_map_trim_ordered() INVERTED flag not supported");

	setup_map_must_have_offidx(u, map, com->alloc_idx);
	uint32_t rm_count = map->ele_count - count;
	uint32_t index1 = index + count;

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_trim_ordered() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint32_t offset0 = offset_index_get_const(u->offidx, index);
	uint32_t offset1 = offset_index_get_const(u->offidx, index1);
	uint32_t content_sz = offset1 - offset0;

	if (cdt_op_is_modify(com)) {
		define_map_packer(mpk, count, map->flags, content_sz);

		map_packer_setup_bin(&mpk, com);
		memcpy(mpk.write_ptr, map->contents + offset0, content_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		bool is_rev = (result->type == RESULT_TYPE_REVINDEX);
		define_int_list_builder(builder, result->alloc, rm_count);

		cdt_container_builder_add_int_range(&builder, 0, index, map->ele_count,
				is_rev);
		cdt_container_builder_add_int_range(&builder, index1,
				map->ele_count - index1, map->ele_count, is_rev);
		cdt_container_builder_set_result(&builder, result);
		break;
	}
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		result->flags = AS_CDT_OP_FLAG_INVERTED;
		packed_map_build_rank_result_by_index_range(map, index, count, result);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP:
		result->flags = AS_CDT_OP_FLAG_INVERTED;

		if (! packed_map_build_ele_result_by_idx_range(map, index, count,
				result)) {
			return -AS_ERR_UNKNOWN;
		}

		break;
	default:
		cf_warning(AS_PARTICLE, "packed_map_trim_ordered() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

// Set b = NULL for get_by_index_range operation.
static int
packed_map_get_remove_by_index_range(const packed_map *map, cdt_op_mem *com,
		int64_t index, uint64_t count)
{
	cdt_result_data *result = &com->result;
	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, map->ele_count, &uindex, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() index %ld out of bounds for ele_count %u", index, map->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		if (count32 == 0) {
			// Reduce to remove all.
			uindex = 0;
			count32 = map->ele_count;
		}
		else if (uindex == 0) {
			// Reduce to remove tail section.
			uindex = count32;
			count32 = map->ele_count - count32;
		}
		else if (uindex + count32 >= map->ele_count) {
			// Reduce to remove head section.
			count32 = uindex;
			uindex = 0;
		}
		else if (map_is_k_ordered(map)) {
			return packed_map_trim_ordered(map, com, uindex, count32);
		}
		else {
			result->flags |= AS_CDT_OP_FLAG_INVERTED;
		}
	}

	if (count32 == 0) {
		if (! result_data_set_key_not_found(result, uindex)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid result type %d", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		return AS_OK;
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (count32 == map->ele_count) {
		return packed_map_get_remove_all(map, com);
	}

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	int ret = AS_OK;

	if (map_is_k_ordered(map)) {
		if (cdt_op_is_modify(com)) {
			packed_map_remove_idx_range(map, com, uindex, count32);
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_build_ele_result_by_idx_range(map, uindex, count32,
					result)) {
				return -AS_ERR_UNKNOWN;
			}
		}
		else if (result_data_is_return_rank(result)) {
			packed_map_build_rank_result_by_index_range(map, uindex, count32,
					result);
		}
		else {
			ret = result_data_set_range(result, uindex, count32,
					map->ele_count);
		}
	}
	else {
		define_build_order_heap_by_range(heap, uindex, count32, map->ele_count,
				map, packed_map_compare_key_by_idx, success, com->alloc_idx);

		if (! success) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		uint32_t rm_sz = 0;
		bool inverted = result_data_is_inverted(result);
		define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
		uint32_t rm_count = (inverted ? map->ele_count - count32 : count32);

		cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
				inverted);

		if (cdt_op_is_modify(com)) {
			packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
		}

		switch (result->type) {
		case RESULT_TYPE_RANK:
		case RESULT_TYPE_REVRANK:
			if (inverted) {
				packed_map_build_rank_result_by_mask(map, rm_mask, rm_count,
						result);
			}
			else {
				if (heap.cmp == MSGPACK_CMP_LESS) {
					order_heap_reverse_end(&heap, count32);
				}

				packed_map_build_rank_result_by_ele_idx(map, &heap._,
						heap.filled, count32, result);
			}
			break;
		case RESULT_TYPE_KEY:
		case RESULT_TYPE_VALUE:
		case RESULT_TYPE_MAP: {
			bool success;

			if (inverted) {
				success = packed_map_build_ele_result_by_mask(map, rm_mask,
						rm_count, rm_sz, result);
			}
			else {
				if (heap.cmp == MSGPACK_CMP_LESS) {
					order_heap_reverse_end(&heap, count32);
				}

				success = packed_map_build_ele_result_by_ele_idx(map, &heap._,
						heap.filled, count32, rm_sz, result);
			}

			if (! success) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
				return -AS_ERR_PARAMETER;
			}

			break;
		}
		default:
			ret = result_data_set_range(result, uindex, count32,
					map->ele_count);
			break;
		}
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_by_index_range");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u uindex %u count32 %u", map->ele_count, uindex, count32);
	}
#endif

	return AS_OK;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_map_get_remove_by_value_interval(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *value_start, const cdt_payload *value_end)
{
	cdt_result_data *result = &com->result;

	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (map->ele_count == 0) {
		if (! result_data_set_value_not_found(result, -1)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		return AS_OK;
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rank = 0;
	uint32_t count = 0;
	int ret = AS_OK;

	if (! order_index_is_null(&map->ordidx)) {
		packed_map_ensure_ordidx_filled(map);
		packed_map_find_rank_range_by_value_interval_indexed(map, value_start,
				value_end, &rank, &count, result->is_multi);

		uint32_t rm_count = (inverted ? map->ele_count - count : count);
		bool need_mask = cdt_op_is_modify(com) ||
				(inverted && (result_data_is_return_elements(result) ||
						result_data_is_return_index(result)));
		define_cond_cdt_idx_mask(rm_mask, map->ele_count, need_mask,
				com->alloc_idx);
		uint32_t rm_sz = 0;

		if (need_mask) {
			cdt_idx_mask_set_by_ordidx(rm_mask, &map->ordidx, rank, count,
					inverted);
		}

		if (cdt_op_is_modify(com)) {
			packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
		}

		if (result_data_is_return_elements(result)) {
			if (inverted) {
				if (! packed_map_build_ele_result_by_mask(map, rm_mask,
						rm_count, rm_sz, result)) {
					cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
					return -AS_ERR_PARAMETER;
				}
			}
			else if (! packed_map_build_ele_result_by_ele_idx(map, &map->ordidx,
					rank, count, rm_sz, result)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
				return -AS_ERR_PARAMETER;
			}
		}
		else if (result_data_is_return_index(result)) {
			if (inverted) {
				packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
						result);
			}
			else {
				packed_map_build_index_result_by_ele_idx(map, &map->ordidx,
						rank, count, result);
			}
		}
		else {
			ret = result_data_set_range(result, rank, count, map->ele_count);
		}
	}
	else {
		define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
		bool is_ret_rank = result_data_is_return_rank(result) ||
				result_data_is_return_rank_range(result);

		packed_map_find_rank_range_by_value_interval_unordered(map, value_start,
				value_end, is_ret_rank ? &rank : NULL, &count, rm_mask,
						result->is_multi);

		if (count == 0) {
			if (inverted) {
				result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

				return packed_map_get_remove_all(map, com);
			}
			else if (! result_data_set_value_not_found(result, rank)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}
		}
		else {
			if (! result->is_multi) {
				if (value_start == value_end) {
					uint64_t idx = rm_mask[0];

					rm_mask[0] = 0;
					cdt_idx_mask_set(rm_mask, idx);
				}

				count = 1;
			}

			uint32_t rm_sz = 0;
			uint32_t rm_count = count;

			if (inverted) {
				cdt_idx_mask_invert(rm_mask, map->ele_count);
				rm_count = map->ele_count - count;
			}

			if (cdt_op_is_modify(com)) {
				packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
			}

			if (result_data_is_return_elements(result)) {
				if (! packed_map_build_ele_result_by_mask(map, rm_mask,
						rm_count, rm_sz, result)) {
					return -AS_ERR_UNKNOWN;
				}
			}
			else if (result_data_is_return_index(result)) {
				packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
						result);
			}
			else {
				ret = result_data_set_range(result, rank, count,
						map->ele_count);
			}
		}
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_by_value_interval");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u rank %u count %u inverted %d", map->ele_count, rank, count, inverted);
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_by_rank_range(const packed_map *map, cdt_op_mem *com,
		int64_t rank, uint64_t count)
{
	cdt_result_data *result = &com->result;
	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, map->ele_count, &urank, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() rank %ld out of bounds for ele_count %u", rank, map->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		if (count32 == 0) {
			// Reduce to remove all.
			urank = 0;
			count32 = map->ele_count;
		}
		else if (urank == 0) {
			// Reduce to remove tail section.
			urank = count32;
			count32 = map->ele_count - count32;
		}
		else if (urank + count32 >= map->ele_count) {
			// Reduce to remove head section.
			count32 = urank;
			urank = 0;
		}
		else {
			result->flags |= AS_CDT_OP_FLAG_INVERTED;
		}
	}

	if (count32 == 0) {
		if (! result_data_set_value_not_found(result, urank)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		return AS_OK;
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);
	uint32_t rm_count = inverted ? map->ele_count - count32 : count32;
	const order_index *ordidx = &map->ordidx;
	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
	order_index ret_idxs;

	if (! order_index_is_null(ordidx)) {
		packed_map_ensure_ordidx_filled(map);
		cdt_idx_mask_set_by_ordidx(rm_mask, ordidx, urank, count32, inverted);
		order_index_init_ref(&ret_idxs, ordidx, urank, count32);
	}
	else {
		define_build_order_heap_by_range(heap, urank, count32, map->ele_count,
				map, packed_map_compare_value_by_idx, success, com->alloc_idx);

		if (! success) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
				inverted);

		if (! inverted) {
			if (heap.cmp == MSGPACK_CMP_LESS) {
				// Reorder results from lowest to highest order.
				order_heap_reverse_end(&heap, count32);
			}

			if (result_data_is_return_index(result)) {
				packed_map_build_index_result_by_ele_idx(map, &heap._,
						heap.filled, count32, result);
			}
			else if (result_data_is_return_elements(result)) {
				if (! packed_map_build_ele_result_by_ele_idx(map, &heap._,
						heap.filled, count32, 0, result)) {
					cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
					return -AS_ERR_PARAMETER;
				}
			}
		}
	}

	uint32_t rm_sz = 0;
	int ret = AS_OK;

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
	case RESULT_TYPE_COUNT:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		ret = result_data_set_index_rank_count(result, urank, count32,
				map->ele_count);
		break;
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
		if (inverted) {
			packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
					result);
		}
		else if (! order_index_is_null(ordidx)) {
			packed_map_build_index_result_by_ele_idx(map, &ret_idxs, 0,
					rm_count, result);
		}
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP:
		if (inverted) {
			if (! packed_map_build_ele_result_by_mask(map, rm_mask,
					rm_count, rm_sz, result)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
				return -AS_ERR_PARAMETER;
			}
		}
		else if (! order_index_is_null(ordidx) &&
				! packed_map_build_ele_result_by_ele_idx(map, &ret_idxs, 0,
						rm_count, rm_sz, result)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
		break;
	case RESULT_TYPE_REVRANK_RANGE:
	case RESULT_TYPE_RANK_RANGE:
		ret = result_data_set_range(result, urank, rm_count, map->ele_count);
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_by_rank_range");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "packed_map_get_remove_all_by_value_list");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_key_list(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *key_list)
{
	cdt_result_data *result = &com->result;
	msgpack_in items_mp;
	uint32_t items_count;

	if (! list_param_parse(key_list, &items_mp, &items_count)) {
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);

	if (items_count == 0) {
		switch (result->type) {
		case RESULT_TYPE_RANK:
		case RESULT_TYPE_REVRANK:
		case RESULT_TYPE_INDEX_RANGE:
		case RESULT_TYPE_REVINDEX_RANGE:
		case RESULT_TYPE_RANK_RANGE:
		case RESULT_TYPE_REVRANK_RANGE:
			cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list() invalid result type %d", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		default:
			break;
		}

		if (! inverted) {
			result_data_set_key_not_found(result, 0);

			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_map_get_remove_all(map, com);
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (map_is_k_ordered(map)) {
		return packed_map_get_remove_all_by_key_list_ordered(map, com,
				&items_mp, items_count);
	}

	return packed_map_get_remove_all_by_key_list_unordered(map, com, &items_mp,
			items_count);
}

static int
packed_map_get_remove_all_by_key_list_ordered(const packed_map *map,
		cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count)
{
	cdt_result_data *result = &com->result;
	define_order_index2(rm_ic, map->ele_count, 2 * items_count, com->alloc_idx);
	uint32_t rc_count = 0;

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload key = {
				.ptr = items_mp->buf + items_mp->offset,
				.sz = items_mp->offset
		};

		if (msgpack_sz(items_mp) == 0) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		key.sz = items_mp->offset - key.sz;

		map_ele_find find_key;

		map_ele_find_init(&find_key, map);
		packed_map_find_key(map, &find_key, &key);

		uint32_t count = find_key.found_key ? 1 : 0;

		order_index_set(&rm_ic, 2 * i, find_key.idx);
		order_index_set(&rm_ic, (2 * i) + 1, count);
		rc_count += count;
	}

	bool inverted = result_data_is_inverted(result);
	bool need_mask = cdt_op_is_modify(com) ||
			result_data_is_return_elements(result) ||
			result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE);
	define_cond_cdt_idx_mask(rm_mask, map->ele_count, need_mask,
			com->alloc_idx);
	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_ic, NULL, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, map->ele_count);
	}

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX:
		if (inverted) {
			result_data_set_int_list_by_mask(result, rm_mask, rm_count,
					map->ele_count);
		}
		else {
			result_data_set_by_irc(result, &rm_ic, NULL, rc_count);
		}
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(com->result.result, rm_count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP:
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count,
				rm_sz, result)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
		break;
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK:
	default:
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() invalid return type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_all_by_key_list_ordered");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_key_list_unordered(const packed_map *map,
		cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count)
{
	cdt_result_data *result = &com->result;
	bool inverted = result_data_is_inverted(result);
	bool is_ret_index = result_data_is_return_index(result);
	uint32_t rm_count;
	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
	define_order_index(key_list_ordidx, items_count, com->alloc_idx);
	definep_cond_order_index2(ic, map->ele_count, items_count * 2,
			is_ret_index, com->alloc_idx);

	if (! offset_index_find_items((offset_index *)&map->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_MAP_KEY, items_mp, &key_list_ordidx,
			inverted, rm_mask, &rm_count, ic, com->alloc_idx)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		result_data_set_by_itemlist_irc(result, &key_list_ordidx, ic, rm_count);
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(com->result.result, rm_count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count, rm_sz,
				result)) {
			return -AS_ERR_UNKNOWN;
		}
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_unordered() invalid return type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_all_by_key_list_unordered");
		map_print(map, "original");
		cdt_idx_mask_print(rm_mask, map->ele_count, "rm_mask");
		print_packed(items_mp->buf, items_mp->buf_sz, "items_mp");
		cf_crash(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_unordered: items_count %u ele_count %u", items_count, map->ele_count);
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_value_list(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *value_list)
{
	cdt_result_data *result = &com->result;
	msgpack_in items_mp;
	uint32_t items_count;

	if (! list_param_parse(value_list, &items_mp, &items_count)) {
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);

	if (items_count == 0) {
		switch (result->type) {
		case RESULT_TYPE_INDEX_RANGE:
		case RESULT_TYPE_REVINDEX_RANGE:
		case RESULT_TYPE_RANK_RANGE:
		case RESULT_TYPE_REVRANK_RANGE:
			cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_value_list() invalid result type %d", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		default:
			break;
		}

		if (! inverted) {
			result_data_set_not_found(result, 0);
			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_map_get_remove_all(map, com);
	}

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_value_list() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (order_index_is_valid(&map->ordidx)) {
		return packed_map_get_remove_all_by_value_list_ordered(map, com,
				&items_mp, items_count);
	}

	bool is_ret_rank = result_data_is_return_rank(result);
	define_cdt_idx_mask(rm_mask, map->ele_count, com->alloc_idx);
	uint32_t rm_count = 0;
	define_order_index(value_list_ordidx, items_count, com->alloc_idx);
	definep_cond_order_index2(rc, map->ele_count, items_count * 2, is_ret_rank,
			com->alloc_idx);

	if (! offset_index_find_items(u->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_MAP_VALUE, &items_mp, &value_list_ordidx,
			inverted, rm_mask, &rm_count, rc, com->alloc_idx)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		packed_map_build_index_result_by_mask(map, rm_mask, rm_count, result);
		break;
	}
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK: {
		result_data_set_by_itemlist_irc(result, &value_list_ordidx, rc,
				rm_count);
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count, rm_sz,
				result)) {
			return -AS_ERR_UNKNOWN;
		}
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_value_list() invalid return type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_all_by_value_list");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "packed_map_get_remove_all_by_value_list");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_value_list_ordered(const packed_map *map,
		cdt_op_mem *com, msgpack_in *items_mp, uint32_t items_count)
{
	cdt_result_data *result = &com->result;
	define_order_index2(rm_rc, map->ele_count, 2 * items_count, com->alloc_idx);
	uint32_t rc_count = 0;

	packed_map_ensure_ordidx_filled(map);

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload value = {
				.ptr = items_mp->buf + items_mp->offset,
				.sz = items_mp->offset
		};

		if (msgpack_sz(items_mp) == 0) {
			cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items_ordered() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		value.sz = items_mp->offset - value.sz;

		uint32_t rank = 0;
		uint32_t count = 0;

		packed_map_find_rank_range_by_value_interval_indexed(map, &value,
				&value, &rank, &count, result->is_multi);

		order_index_set(&rm_rc, 2 * i, rank);
		order_index_set(&rm_rc, (2 * i) + 1, count);
		rc_count += count;
	}

	bool inverted = result_data_is_inverted(result);
	bool need_mask = cdt_op_is_modify(com) ||
			result_data_is_return_elements(result) ||
			result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE);
	define_cond_cdt_idx_mask(rm_mask, map->ele_count, need_mask,
			com->alloc_idx);
	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_rc, &map->ordidx, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, map->ele_count);
	}

	if (cdt_op_is_modify(com)) {
		packed_map_remove_by_mask(map, com, rm_mask, rm_count, &rm_sz);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		if (inverted) {
			packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
					result);
		}
		else {
			result_data_set_by_irc(result, &rm_rc, &map->ordidx, rc_count);
		}
		break;
	}
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK:
		if (inverted) {
			packed_map_build_rank_result_by_mask(map, rm_mask, rm_count,
					result);
		}
		else {
			result_data_set_by_irc(result, &rm_rc, NULL, rc_count);
		}
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count, rm_sz,
				result)) {
			cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items_ordered() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items_ordered() invalid return type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_remove_all_value_items_ordered");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "packed_map_remove_all_value_items_ordered");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_by_rel_index_range(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *key, int64_t index, uint64_t count)
{
	uint32_t rel_index;
	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_index_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (map_is_k_ordered(map)) {
		uint32_t temp;

		packed_map_get_range_by_key_interval_ordered(map, key, key, &rel_index,
				&temp);
	}
	else {
		uint32_t temp;

		packed_map_get_range_by_key_interval_unordered(map, key, key,
				&rel_index, &temp, NULL);
	}

	calc_rel_index_count(index, count, rel_index, &index, &count);

	return packed_map_get_remove_by_index_range(map, com, index, count);
}

static int
packed_map_get_remove_by_rel_rank_range(const packed_map *map, cdt_op_mem *com,
		const cdt_payload *value, int64_t rank, uint64_t count)
{
	cdt_result_data *result = &com->result;
	uint32_t rel_rank;

	setup_map_must_have_offidx(u, map, com->alloc_idx);

	if (! packed_map_check_and_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_rank_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (! order_index_is_null(&map->ordidx)) {
		uint32_t temp;

		packed_map_ensure_ordidx_filled(map);
		packed_map_find_rank_range_by_value_interval_indexed(map, value, value,
				&rel_rank, &temp, result->is_multi);
	}
	else {
		uint32_t temp;

		packed_map_find_rank_range_by_value_interval_unordered(map, value,
				value, &rel_rank, &temp, NULL, true);
	}

	calc_rel_index_count(rank, count, rel_rank, &rank, &count);

	return packed_map_get_remove_by_rank_range(map, com, rank, count);
}

static int
packed_map_get_remove_all(const packed_map *map, cdt_op_mem *com)
{
	cdt_result_data *result = &com->result;

	cf_assert(! result_data_is_inverted(result), AS_PARTICLE, "packed_map_get_remove_all() INVERTED flag is invalid here");

	if (cdt_op_is_modify(com)) {
		cdt_context_set_empty_packed_map(&com->ctx, map->flags);
	}

	bool is_rev = false;

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_REVRANK:
		is_rev = true;
		// no break
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_RANK: {
		if (! result->is_multi) {
			cf_assert(map->ele_count == 1, AS_PARTICLE, "single result op with multiple results %u", map->ele_count);
			as_bin_set_int(result->result, 0);
			break;
		}

		define_int_list_builder(builder, result->alloc, map->ele_count);

		cdt_container_builder_add_int_range(&builder, 0, map->ele_count,
				map->ele_count, is_rev);
		cdt_container_builder_set_result(&builder, result);
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, map->ele_count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		if (! packed_map_build_ele_result_by_idx_range(map, 0, map->ele_count,
				result)) {
			return -AS_ERR_UNKNOWN;
		}
		break;
	}
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		result_data_set_list_int2x(result, 0, map->ele_count);
		break;
	default:
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all() invalid return type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_map_get_remove_all");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "packed_map_get_remove_all_by_value_list");
	}
#endif

	return AS_OK;
}

static void
packed_map_remove_by_mask(const packed_map *map, cdt_op_mem *com,
		const uint64_t *rm_mask, uint32_t count, uint32_t *rm_sz_r)
{
	if (count == 0) {
		return;
	}

	const offset_index *offidx = &map->offidx;
	uint32_t rm_sz = cdt_idx_mask_get_content_sz(rm_mask, count, offidx);

	if (rm_sz_r) {
		*rm_sz_r = rm_sz;
	}

	uint32_t new_ele_count = map->ele_count - count;
	uint32_t content_sz = map->content_sz - rm_sz;
	define_map_packer(mpk, new_ele_count, map->flags, content_sz);

	map_packer_setup_bin(&mpk, com);
	mpk.write_ptr = cdt_idx_mask_write_eles(rm_mask, count, offidx,
			mpk.write_ptr, true);

	if (! offset_index_is_null(&mpk.offidx)) {
		cf_assert(offset_index_is_full(offidx), AS_PARTICLE, "offidx not full");
		map_offset_index_copy_rm_mask(&mpk.offidx, offidx, rm_mask, count);
	}

	if (! order_index_is_null(&mpk.ordidx)) {
		if (order_index_is_filled(&map->ordidx)) {
			order_index_op_remove_idx_mask(&mpk.ordidx, &map->ordidx, rm_mask,
					count);
		}
		else {
			order_index_set_sorted(&mpk.ordidx, &mpk.offidx, mpk.contents,
					mpk.content_sz, SORT_BY_VALUE);
		}
	}
}

static void
packed_map_remove_idx_range(const packed_map *map, cdt_op_mem *com,
		uint32_t idx, uint32_t count)
{
	offset_index *offidx = (offset_index *)&map->offidx;
	uint32_t offset0 = offset_index_get_const(offidx, idx);
	uint32_t idx_end = idx + count;
	uint32_t offset1 = offset_index_get_const(offidx, idx_end);
	uint32_t content_sz = map->content_sz - offset1 + offset0;
	uint32_t new_ele_count = map->ele_count - count;
	define_map_packer(mpk, new_ele_count, map->flags, content_sz);

	map_packer_setup_bin(&mpk, com);

	uint32_t tail_sz = map->content_sz - offset1;

	memcpy(mpk.write_ptr, map->contents, offset0);
	mpk.write_ptr += offset0;
	memcpy(mpk.write_ptr, map->contents + offset1, tail_sz);

	if (! offset_index_is_null(&mpk.offidx)) {
		cf_assert(offset_index_is_full(offidx), AS_PARTICLE, "offidx not full");
		map_offset_index_copy_rm_range(&mpk.offidx, offidx, idx, count);
	}

	if (! order_index_is_null(&mpk.ordidx)) {
		if (order_index_is_filled(&map->ordidx)) {
			uint32_t count0 = 0;

			for (uint32_t i = 0; i < map->ele_count; i++) {
				uint32_t idx0 = order_index_get(&map->ordidx, i);

				if (idx0 >= idx && idx0 < idx_end) {
					continue;
				}

				if (idx0 >= idx_end) {
					idx0 -= count;
				}

				order_index_set(&mpk.ordidx, count0++, idx0);
			}
		}
		else {
			order_index_set_sorted(&mpk.ordidx, &mpk.offidx, mpk.contents,
					mpk.content_sz, SORT_BY_VALUE);
		}
	}

	return;
}

static void
packed_map_get_range_by_key_interval_unordered(const packed_map *map,
		const cdt_payload *key_start, const cdt_payload *key_end,
		uint32_t *index, uint32_t *count, uint64_t *mask)
{
	cf_assert(key_end, AS_PARTICLE, "key_end == NULL");

	msgpack_in mp_start = {
			.buf = key_start->ptr,
			.buf_sz = key_start->sz
	};

	msgpack_in mp_end = {
			.buf = key_end->ptr,
			.buf_sz = key_end->sz
	};

	*index = 0;
	*count = 0;

	offset_index *offidx = (offset_index *)&map->offidx;
	define_map_msgpack_in(mp, map);

	for (uint32_t i = 0; i < map->ele_count; i++) {
		uint32_t key_offset = mp.offset; // start of key

		offset_index_set(offidx, i, key_offset);

		mp_start.offset = 0;

		msgpack_cmp_type cmp_start = msgpack_cmp(&mp, &mp_start);

		cf_assert(cmp_start != MSGPACK_CMP_ERROR, AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid packed map at index %u", i);

		if (cmp_start == MSGPACK_CMP_LESS) {
			(*index)++;
		}
		else {
			msgpack_cmp_type cmp_end = MSGPACK_CMP_LESS;

			// NULL key_end->ptr means largest possible value.
			if (key_end->ptr) {
				mp.offset = key_offset;
				mp_end.offset = 0;
				cmp_end = msgpack_cmp(&mp, &mp_end);
			}

			if (cmp_end == MSGPACK_CMP_LESS) {
				cdt_idx_mask_set(mask, i);
				(*count)++;
			}
		}

		// Skip value.
		if (msgpack_sz(&mp) == 0) {
			cf_crash(AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid packed map at index %u", i);
		}
	}

	offset_index_set_filled(offidx, map->ele_count);
}

static void
packed_map_get_range_by_key_interval_ordered(const packed_map *map,
		const cdt_payload *key_start, const cdt_payload *key_end,
		uint32_t *index, uint32_t *count)
{
	map_ele_find find_key_start;

	map_ele_find_init(&find_key_start, map);
	packed_map_find_key(map, &find_key_start, key_start);

	*index = find_key_start.idx;

	if (key_start == key_end) {
		if (find_key_start.found_key) {
			*count = 1;
		}
		else {
			*count = 0;
		}
	}
	else if (key_end && key_end->ptr) {
		map_ele_find find_key_end;

		map_ele_find_continue_from_lower(&find_key_end, &find_key_start,
				map->ele_count);
		packed_map_find_key(map, &find_key_end, key_end);

		if (find_key_end.idx <= find_key_start.idx) {
			*count = 0;
		}
		else {
			*count = find_key_end.idx - find_key_start.idx;
		}
	}
	else {
		*count = map->ele_count - find_key_start.idx;
	}
}

// Does not respect invert flag.
static void
packed_map_build_rank_result_by_ele_idx(const packed_map *map,
		const order_index *ele_idx, uint32_t start, uint32_t count,
		cdt_result_data *result)
{
	if (! result->is_multi) {
		uint32_t idx = order_index_get(ele_idx, start);

		packed_map_build_rank_result_by_idx(map, idx, result);

		return;
	}

	define_int_list_builder(builder, result->alloc, count);
	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
	setup_map_must_have_all_idx(uv, map, alloc_idx);
	packed_map_ensure_ordidx_filled(map);

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ele_idx, start + i);
		map_ele_find find;

		map_ele_find_init_from_idx(&find, map, idx);
		packed_map_find_rank_indexed(map, &find);

		cf_assert(find.found_value, AS_PARTICLE, "packed_map_build_rank_result_by_ele_idx() idx %u not found find.rank %u", idx, find.rank);

		cdt_container_builder_add_int_range(&builder, find.rank, 1,
				map->ele_count, is_rev);
	}

	cdt_container_builder_set_result(&builder, result);
	rollback_alloc_rollback(alloc_idx);
}

// Does not respect invert flag.
static void
packed_map_build_rank_result_by_mask(const packed_map *map,
		const uint64_t *mask, uint32_t count, cdt_result_data *result)
{
	uint32_t idx = 0;

	if (! result->is_multi) {
		idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);
		packed_map_build_rank_result_by_idx(map, idx, result);
		return;
	}

	define_int_list_builder(builder, result->alloc, count);
	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
	setup_map_must_have_all_idx(uv, map, alloc_idx);
	packed_map_ensure_ordidx_filled(map);

	for (uint32_t i = 0; i < count; i++) {
		idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);

		map_ele_find find;

		map_ele_find_init_from_idx(&find, map, idx);
		packed_map_find_rank_indexed(map, &find);

		cf_assert(find.found_value, AS_PARTICLE, "packed_map_build_rank_result_by_mask() idx %u not found find.rank %u", idx, find.rank);

		cdt_container_builder_add_int_range(&builder, find.rank, 1,
				map->ele_count, is_rev);
		idx++;
	}

	cdt_container_builder_set_result(&builder, result);
	rollback_alloc_rollback(alloc_idx);
}

static void
packed_map_build_rank_result_by_index_range(const packed_map *map,
		uint32_t index, uint32_t count, cdt_result_data *result)
{
	if (! result->is_multi) {
		packed_map_build_rank_result_by_idx(map, index, result);
		return;
	}

	cf_assert(map_is_k_ordered(map), AS_PARTICLE, "map must be K_ORDERED");

	bool inverted = result_data_is_inverted(result);
	uint32_t ret_count = (inverted ? map->ele_count - count : count);
	define_int_list_builder(builder, result->alloc, ret_count);

	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
	setup_map_must_have_all_idx(uv, map, alloc_idx);
	packed_map_ensure_ordidx_filled(map);

	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	if (inverted) {
		for (uint32_t i = 0; i < index; i++) {
			map_ele_find find;

			map_ele_find_init_from_idx(&find, map, i);
			packed_map_find_rank_indexed(map, &find);

			cf_assert(find.found_value, AS_PARTICLE, "packed_map_build_rank_result_by_index_range() idx %u count %u not found find.rank %u", index, count, find.rank);

			uint32_t rank = find.rank;

			if (is_rev) {
				rank = map->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}

		for (uint32_t i = index + count; i < map->ele_count; i++) {
			map_ele_find find;

			map_ele_find_init_from_idx(&find, map, i);
			packed_map_find_rank_indexed(map, &find);

			cf_assert(find.found_value, AS_PARTICLE, "packed_map_build_rank_result_by_index_range() idx %u count %u not found find.rank %u", index, count, find.rank);

			uint32_t rank = find.rank;

			if (is_rev) {
				rank = map->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}
	}
	else {
		for (uint32_t i = 0; i < count; i++) {
			map_ele_find find;

			map_ele_find_init_from_idx(&find, map, index + i);
			packed_map_find_rank_indexed(map, &find);

			cf_assert(find.found_value, AS_PARTICLE, "packed_map_build_rank_result_by_index_range() idx %u count %u not found find.rank %u", index, count, find.rank);

			uint32_t rank = find.rank;

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = map->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}
	}

	cdt_container_builder_set_result(&builder, result);
	rollback_alloc_rollback(alloc_idx);
}

static void
packed_map_get_key_by_idx(const packed_map *map, cdt_payload *key,
		uint32_t index)
{
	uint32_t mp_offset = offset_index_get_const(&map->offidx, index);

	msgpack_in mp = {
			.buf = map->contents + mp_offset,
			.buf_sz = map->content_sz - mp_offset
	};

	key->ptr = mp.buf;
	key->sz = msgpack_sz(&mp); // key

	cf_assert(key->sz != 0, AS_PARTICLE, "packed_map_get_key_by_idx() read key failed offset %u", mp.offset);
}

static void
packed_map_get_value_by_idx(const packed_map *map, cdt_payload *value,
		uint32_t idx)
{
	uint32_t offset = offset_index_get_const(&map->offidx, idx);
	uint32_t sz = offset_index_get_delta_const(&map->offidx, idx);

	msgpack_in mp = {
			.buf = map->contents + offset,
			.buf_sz = map->content_sz - offset
	};

	uint32_t key_sz = msgpack_sz(&mp); // key

	cf_assert(key_sz != 0, AS_PARTICLE, "packed_map_get_value_by_idx() read key failed offset %u", mp.offset);

	value->ptr = mp.buf + key_sz;
	value->sz = sz - key_sz;
}

static void
packed_map_get_pair_by_idx(const packed_map *map, cdt_payload *value,
		uint32_t index)
{
	uint32_t offset = offset_index_get_const(&map->offidx, index);
	uint32_t sz = offset_index_get_delta_const(&map->offidx, index);

	value->ptr = map->contents + offset;
	value->sz = sz;
}

// Does not respect invert flag.
static void
packed_map_build_index_result_by_ele_idx(const packed_map *map,
		const order_index *ele_idx, uint32_t start, uint32_t count,
		cdt_result_data *result)
{
	if (count == 0) {
		if (! result_data_set_not_found(result, start)) {
			cf_crash(AS_PARTICLE, "packed_map_build_index_result_by_ele_idx() invalid result type %d", result->type);
		}

		return;
	}

	if (! result->is_multi) {
		uint32_t index = order_index_get(ele_idx, start);

		if (! map_is_k_ordered(map)) {
			index = packed_map_find_index_by_idx_unordered(map, index);
		}

		if (result->type == RESULT_TYPE_REVINDEX) {
			index = map->ele_count - index - 1;
		}

		as_bin_set_int(result->result, index);

		return;
	}

	define_int_list_builder(builder, result->alloc, count);

	if (map_is_k_ordered(map)) {
		for (uint32_t i = 0; i < count; i++) {
			uint32_t index = order_index_get(ele_idx, start + i);

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
		}
	}
	else {
		define_rollback_alloc(alloc_idx, NULL, 2, false);
		define_order_index(keyordidx, map->ele_count, alloc_idx);

		cf_assert(offset_index_is_full(&map->offidx), AS_PARTICLE, "offidx not full");
		order_index_set_sorted(&keyordidx, &map->offidx, map->contents,
				map->content_sz, SORT_BY_KEY);

		for (uint32_t i = 0; i < count; i++) {
			uint32_t idx = order_index_get(ele_idx, start + i);
			uint32_t index = order_index_find_idx(&keyordidx, idx, 0,
					map->ele_count);

			cf_assert(index < map->ele_count, AS_PARTICLE, "idx %u index %u >= ele_count %u", idx, index, map->ele_count);

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
		}

		rollback_alloc_rollback(alloc_idx);
	}

	cdt_container_builder_set_result(&builder, result);
}

// Does not respect invert flag.
static void
packed_map_build_index_result_by_mask(const packed_map *map,
		const uint64_t *mask, uint32_t count, cdt_result_data *result)
{
	if (count == 0) {
		result_data_set_not_found(result, -1);
		return;
	}

	if (! result->is_multi) {
		uint32_t index = cdt_idx_mask_find(mask, 0, map->ele_count, false);

		if (! map_is_k_ordered(map)) {
			index = packed_map_find_index_by_idx_unordered(map, index);
		}

		if (result->type == RESULT_TYPE_REVINDEX) {
			index = map->ele_count - index - 1;
		}

		as_bin_set_int(result->result, index);

		return;
	}

	define_int_list_builder(builder, result->alloc, count);

	if (map_is_k_ordered(map)) {
		uint32_t index = 0;

		for (uint32_t i = 0; i < count; i++) {
			index = cdt_idx_mask_find(mask, index, map->ele_count, false);
			cdt_container_builder_add_int64(&builder,
					result->type == RESULT_TYPE_REVINDEX ?
							map->ele_count - index - 1 : index);
			index++;
		}
	}
	else {
		define_rollback_alloc(alloc_idx, NULL, 2, false);
		define_order_index(keyordidx, map->ele_count, alloc_idx);
		uint32_t idx = 0;

		cf_assert(offset_index_is_full(&map->offidx), AS_PARTICLE, "offidx not full");
		order_index_set_sorted(&keyordidx, &map->offidx, map->contents,
				map->content_sz, SORT_BY_KEY);

		for (uint32_t i = 0; i < count; i++) {
			idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);

			uint32_t index = order_index_find_idx(&keyordidx, idx, 0,
					map->ele_count);

			cf_assert(index < map->ele_count, AS_PARTICLE, "idx %u index %u >= ele_count %u", idx, index, map->ele_count);

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
			idx++;
		}

		rollback_alloc_rollback(alloc_idx);
	}

	cdt_container_builder_set_result(&builder, result);
}

// Build by map ele_idx range.
static bool
packed_map_build_ele_result_by_idx_range(const packed_map *map,
		uint32_t start_idx, uint32_t count, cdt_result_data *result)
{
	if (! packed_map_check_and_fill_offidx(map)) {
		return false;
	}

	bool inverted = result_data_is_inverted(result);
	uint32_t offset0 = offset_index_get_const(&map->offidx, start_idx);
	uint32_t offset1 = offset_index_get_const(&map->offidx, start_idx + count);
	uint32_t max_sz = offset1 - offset0;
	uint32_t ret_count = count;
	cdt_container_builder builder;

	if (inverted) {
		ret_count = map->ele_count - count;
		max_sz = map->content_sz - max_sz;
	}

	if (result->type == RESULT_TYPE_MAP) {
		cdt_map_builder_start(&builder, result->alloc, ret_count, max_sz,
				AS_PACKED_MAP_FLAG_PRESERVE_ORDER);

		if (inverted) {
			uint32_t tail_sz = map->content_sz - offset1;

			memcpy(builder.write_ptr, map->contents, offset0);
			builder.write_ptr += offset0;
			memcpy(builder.write_ptr, map->contents + offset1, tail_sz);
		}
		else {
			memcpy(builder.write_ptr, map->contents + offset0, max_sz);
		}

		*builder.sz += max_sz;
		cdt_container_builder_set_result(&builder, result);

		return true;
	}

	packed_map_get_by_idx_func get_by_idx_func;

	if (result->type == RESULT_TYPE_KEY) {
		get_by_idx_func = packed_map_get_key_by_idx;
	}
	else {
		get_by_idx_func = packed_map_get_value_by_idx;
	}

	if (result->is_multi) {
		cdt_list_builder_start(&builder, result->alloc, ret_count, max_sz);
	}
	else {
		cdt_payload packed;

		get_by_idx_func(map, &packed, start_idx);

		return rollback_alloc_from_msgpack(result->alloc, result->result,
				&packed);
	}

	if (inverted) {
		for (uint32_t i = 0; i < start_idx; i++) {
			cdt_payload packed;

			get_by_idx_func(map, &packed, i);
			cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		}

		for (uint32_t i = start_idx + count; i < map->ele_count; i++) {
			cdt_payload packed;

			get_by_idx_func(map, &packed, i);
			cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		}
	}
	else {
		for (uint32_t i = 0; i < count; i++) {
			cdt_payload packed;

			get_by_idx_func(map, &packed, start_idx + i);
			cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		}
	}

	cdt_container_builder_set_result(&builder, result);

	return true;
}

// Does not respect invert flag.
static bool
packed_map_build_ele_result_by_ele_idx(const packed_map *map,
		const order_index *ele_idx, uint32_t start, uint32_t count,
		uint32_t rm_sz, cdt_result_data *result)
{
	if (rm_sz == 0) {
		if (start != 0) {
			order_index ref;

			order_index_init_ref(&ref, ele_idx, start, count);
			rm_sz = order_index_get_ele_size(&ref, count, &map->offidx);
		}
		else {
			rm_sz = order_index_get_ele_size(ele_idx, count, &map->offidx);
		}
	}

	packed_map_get_by_idx_func get_by_index_func;
	cdt_container_builder builder;
	uint32_t max_sz = (count != 0 ? rm_sz : 0);

	if (result->type == RESULT_TYPE_MAP) {
		get_by_index_func = packed_map_get_pair_by_idx;

		cdt_map_builder_start(&builder, result->alloc, count, max_sz,
				AS_PACKED_MAP_FLAG_PRESERVE_ORDER);
	}
	else {
		if (result->type == RESULT_TYPE_KEY) {
			get_by_index_func = packed_map_get_key_by_idx;
		}
		else {
			get_by_index_func = packed_map_get_value_by_idx;
		}

		if (result->is_multi) {
			cdt_list_builder_start(&builder, result->alloc, count,
					max_sz - count);
		}
		else if (count == 0) {
			return true;
		}
		else {
			uint32_t index = order_index_get(ele_idx, start);
			cdt_payload packed;

			get_by_index_func(map, &packed, index);

			return rollback_alloc_from_msgpack(result->alloc, result->result,
					&packed);
		}
	}

	for (uint32_t i = 0; i < count; i++) {
		uint32_t index = order_index_get(ele_idx, i + start);
		cdt_payload packed;

		get_by_index_func(map, &packed, index);
		cdt_container_builder_add(&builder, packed.ptr, packed.sz);
	}

	cdt_container_builder_set_result(&builder, result);

	return true;
}

// Does not respect invert flag.
static bool
packed_map_build_ele_result_by_mask(const packed_map *map, const uint64_t *mask,
		uint32_t count, uint32_t rm_sz, cdt_result_data *result)
{
	if (! result->is_multi) {
		uint32_t idx = cdt_idx_mask_find(mask, 0, map->ele_count, false);
		define_order_index2(ele_idx, map->ele_count, 1, NULL);

		order_index_set(&ele_idx, 0, idx);

		return packed_map_build_ele_result_by_ele_idx(map, &ele_idx, 0, 1,
				rm_sz, result);
	}

	if (rm_sz == 0) {
		rm_sz = cdt_idx_mask_get_content_sz(mask, count, &map->offidx);
	}

	packed_map_get_by_idx_func get_by_index_func;
	cdt_container_builder builder;
	uint32_t max_sz = (count != 0 ? rm_sz : 0);

	if (result->type == RESULT_TYPE_MAP) {
		get_by_index_func = packed_map_get_pair_by_idx;

		cdt_map_builder_start(&builder, result->alloc, count, max_sz,
				AS_PACKED_MAP_FLAG_PRESERVE_ORDER);
	}
	else {
		if (result->type == RESULT_TYPE_KEY) {
			get_by_index_func = packed_map_get_key_by_idx;
		}
		else {
			get_by_index_func = packed_map_get_value_by_idx;
		}

		cdt_list_builder_start(&builder, result->alloc, count, max_sz - count);
	}

	uint32_t index = 0;

	for (uint32_t i = 0; i < count; i++) {
		cdt_payload packed;

		index = cdt_idx_mask_find(mask, index, map->ele_count, false);

		get_by_index_func(map, &packed, index);
		cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		index++;
	}

	cdt_container_builder_set_result(&builder, result);

	return true;
}

static int
packed_map_build_result_by_key(const packed_map *map, const cdt_payload *key,
		uint32_t idx, uint32_t count, cdt_result_data *result)
{
	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX: {
		uint32_t index = idx;

		if (! map_is_k_ordered(map)) {
			index = packed_map_find_index_by_key_unordered(map, key);
		}

		if (result_data_is_return_index_range(result)) {
			if (result->type == RESULT_TYPE_REVINDEX_RANGE) {
				index = map->ele_count - index - count;
			}

			result_data_set_list_int2x(result, index, count);
		}
		else {
			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - count;
			}

			as_bin_set_int(result->result, index);
		}

		break;
	}
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		if (result->is_multi) {
			packed_map_build_rank_result_by_idx_range(map, idx, count, result);
			break;
		}

		packed_map_build_rank_result_by_idx(map, idx, result);
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP:
		if (! packed_map_build_ele_result_by_idx_range(map, idx, count,
				result)) {
			return -AS_ERR_UNKNOWN;
		}

		break;
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_map_build_result_by_key() invalid result_type %d", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

// Return negative codes on error.
static uint32_t
packed_map_get_rank_by_idx(const packed_map *map, uint32_t idx)
{
	cf_assert(map_has_offidx(map), AS_PARTICLE, "packed_map_get_rank_by_idx() offset_index needs to be valid");

	uint32_t rank;

	if (! order_index_is_null(&map->ordidx)) {
		packed_map_ensure_ordidx_filled(map);

		map_ele_find find_key;
		map_ele_find_init_from_idx(&find_key, map, idx);

		packed_map_find_rank_indexed(map, &find_key);
		cf_assert(find_key.found_value, AS_PARTICLE, "rank not found, idx=%u rank=%u", find_key.idx, find_key.rank);
		rank = find_key.rank;
	}
	else {
		const offset_index *offidx = &map->offidx;
		uint32_t offset = offset_index_get_const(offidx, idx);
		define_map_msgpack_in(mp, map);

		msgpack_in mp_entry = {
				.buf = map->contents + offset,
				.buf_sz = map->content_sz - offset
		};

		rank = 0;

		for (uint32_t i = 0; i < map->ele_count; i++) {
			mp_entry.offset = 0;

			msgpack_cmp_type cmp = packed_map_compare_values(&mp, &mp_entry);

			cf_assert(cmp != MSGPACK_CMP_ERROR, AS_PARTICLE, "offset %u/%u", mp.offset, mp.buf_sz);

			if (cmp == MSGPACK_CMP_LESS) {
				rank++;
			}
		}
	}

	return rank;
}

static void
packed_map_build_rank_result_by_idx(const packed_map *map, uint32_t idx,
		cdt_result_data *result)
{
	uint32_t rank = packed_map_get_rank_by_idx(map, idx);

	if (result->type == RESULT_TYPE_REVRANK) {
		as_bin_set_int(result->result, (int64_t)map->ele_count - rank - 1);
	}
	else {
		as_bin_set_int(result->result, rank);
	}
}

static void
packed_map_build_rank_result_by_idx_range(const packed_map *map, uint32_t idx,
		uint32_t count, cdt_result_data *result)
{
	define_int_list_builder(builder, result->alloc, count);

	for (uint32_t i = 0; i < count; i++) {
		uint32_t rank = packed_map_get_rank_by_idx(map, idx);

		if (result->type == RESULT_TYPE_REVRANK) {
			rank = map->ele_count - rank - 1;
		}

		cdt_container_builder_add_int64(&builder, rank);
	}

	cdt_container_builder_set_result(&builder, result);
}

static msgpack_cmp_type
packed_map_compare_key_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2)
{
	const packed_map *map = ptr;
	const offset_index *offidx = &map->offidx;

	msgpack_in mp1 = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = offset_index_get_const(offidx, idx1)
	};

	msgpack_in mp2 = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = offset_index_get_const(offidx, idx2)
	};

	msgpack_cmp_type ret = msgpack_cmp(&mp1, &mp2);

	if (ret == MSGPACK_CMP_EQUAL) {
		ret = msgpack_cmp_peek(&mp1, &mp2);
	}

	return ret;
}

static msgpack_cmp_type
packed_map_compare_values(msgpack_in *mp1, msgpack_in *mp2)
{
	msgpack_cmp_type keycmp = msgpack_cmp(mp1, mp2);

	if (keycmp == MSGPACK_CMP_ERROR) {
		return MSGPACK_CMP_ERROR;
	}

	msgpack_cmp_type ret = msgpack_cmp(mp1, mp2);

	if (ret == MSGPACK_CMP_EQUAL) {
		return keycmp;
	}

	return ret;
}

static msgpack_cmp_type
packed_map_compare_value_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2)
{
	const packed_map *map = ptr;
	const offset_index *offidx = &map->offidx;

	msgpack_in mp1 = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = offset_index_get_const(offidx, idx1)
	};

	msgpack_in mp2 = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = offset_index_get_const(offidx, idx2)
	};

	return packed_map_compare_values(&mp1, &mp2);
}

static void
packed_map_write_k_ordered(const packed_map *map, uint8_t *write_ptr,
		offset_index *offsets_new)
{
	uint32_t ele_count = map->ele_count;
	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
	define_order_index(key_ordidx, ele_count, alloc_idx);

	order_index_set_sorted_with_offsets(&key_ordidx, &map->offidx, SORT_BY_KEY);

	const uint8_t *ptr = map->offidx.contents;
	bool has_new_offsets = ! offset_index_is_null(offsets_new);

	if (has_new_offsets) {
		offset_index_set_filled(offsets_new, 1);
	}

	for (uint32_t i = 0; i < ele_count; i++) {
		uint32_t index = order_index_get(&key_ordidx, i);
		uint32_t offset = offset_index_get_const(&map->offidx, index);
		uint32_t sz = offset_index_get_delta_const(&map->offidx, index);

		memcpy(write_ptr, ptr + offset, sz);
		write_ptr += sz;

		if (has_new_offsets) {
			offset_index_append_size(offsets_new, sz);
		}
	}

	rollback_alloc_rollback(alloc_idx);
}

//------------------------------------------------
// packed_map_op

static void
packed_map_op_init(packed_map_op *op, const packed_map *map)
{
	op->map = map;

	op->new_ele_count = 0;
	op->ele_removed = 0;

	op->seg1_sz = 0;
	op->seg2_offset = 0;
	op->seg2_sz = 0;

	op->key1_offset = 0;
	op->key1_sz = 0;
	op->key2_offset = 0;
	op->key2_sz = 0;
}

// Return new size of map elements.
static uint32_t
packed_map_op_add(packed_map_op *op, const map_ele_find *found)
{
	// Replace at offset.
	if (found->found_key) {
		op->new_ele_count = op->map->ele_count;
		op->seg2_offset = found->key_offset + found->sz;
	}
	// Insert at offset.
	else {
		op->new_ele_count = op->map->ele_count + 1;
		op->seg2_offset = found->key_offset;
	}

	op->seg1_sz = found->key_offset;
	op->seg2_sz = op->map->content_sz - op->seg2_offset;

	return op->seg1_sz + op->seg2_sz;
}

static uint32_t
packed_map_op_remove(packed_map_op *op, const map_ele_find *found,
		uint32_t count, uint32_t remove_sz)
{
	op->new_ele_count = op->map->ele_count - count;
	op->seg1_sz = found->key_offset;
	op->seg2_offset = found->key_offset + remove_sz;
	op->seg2_sz = op->map->content_sz - op->seg2_offset;

	op->ele_removed = count;

	return op->seg1_sz + op->seg2_sz;
}

static uint8_t *
packed_map_op_write_seg1(const packed_map_op *op, uint8_t *buf)
{
	const uint8_t *src = op->map->contents;

	memcpy(buf, src, op->seg1_sz);
	memcpy(buf + op->seg1_sz, src + op->key1_offset, op->key1_sz);

	return buf + op->seg1_sz + op->key1_sz;
}

static uint8_t *
packed_map_op_write_seg2(const packed_map_op *op, uint8_t *buf)
{
	const uint8_t *src = op->map->contents;

	memcpy(buf, src + op->key2_offset, op->key2_sz);
	memcpy(buf + op->key2_sz, src + op->seg2_offset, op->seg2_sz);

	return buf + op->key2_sz + op->seg2_sz;
}

static void
packed_map_op_write_new_offidx(const packed_map_op *op,
		const map_ele_find *remove_info, const map_ele_find *add_info,
		offset_index *new_offidx, uint32_t kv_sz)
{
	const offset_index *offidx = &op->map->offidx;

	cf_assert(offset_index_is_full(offidx), AS_PARTICLE, "offidx not full");
	cf_assert(op->new_ele_count >= op->map->ele_count, AS_PARTICLE, "op->new_ele_count %u < op->map->ele_count %u", op->new_ele_count, op->map->ele_count);

	if (op->new_ele_count - op->map->ele_count != 0) { // add 1
		offset_index_add_ele(new_offidx, offidx, add_info->idx);
	}
	else { // replace 1
		cf_assert(remove_info->idx == add_info->idx, AS_PARTICLE, "remove_info->idx %u != add_info->idx %u", remove_info->idx, add_info->idx);

		offset_index_move_ele(new_offidx, offidx, remove_info->idx,
				add_info->idx);
	}
}

static void
packed_map_op_write_new_ordidx(const packed_map_op *op,
		const map_ele_find *remove_info, const map_ele_find *add_info,
		order_index *new_ordidx)
{
	const order_index *ordidx = &op->map->ordidx;

	cf_assert(order_index_is_valid(ordidx), AS_PARTICLE, "ordidx invalid");
	cf_assert(op->new_ele_count >= op->map->ele_count, AS_PARTICLE, "op->new_ele_count %u < op->map->ele_count %u", op->new_ele_count, op->map->ele_count);

	if (op->new_ele_count - op->map->ele_count != 0) { // add 1
		order_index_op_add(new_ordidx, ordidx, add_info->idx, add_info->rank);
	}
	else { // replace 1
		cf_assert(remove_info->idx == add_info->idx, AS_PARTICLE, "remove_info->idx %u != add_info->idx %u", remove_info->idx, add_info->idx);

		order_index_op_replace1(new_ordidx, ordidx, add_info->rank,
				remove_info->rank);
	}
}

//------------------------------------------------
// map_particle

static as_particle *
map_particle_create(rollback_alloc *alloc_buf, uint32_t ele_count,
		const uint8_t *buf, uint32_t content_sz, uint8_t flags)
{
	define_map_packer(mpk, ele_count, flags, content_sz);
	map_mem *p_map_mem = (map_mem *)map_packer_create_particle(&mpk, alloc_buf);

	map_packer_write_hdridx(&mpk);

	if (buf) {
		memcpy(mpk.write_ptr, buf, content_sz);
	}

	return (as_particle *)p_map_mem;
}

// Return new size on success, negative values on failure.
static uint32_t
map_particle_strip_indexes(const as_particle *p, uint8_t *dest)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	if (p_map_mem->sz == 0) {
		return 0;
	}

	msgpack_in mp = {
			.buf = p_map_mem->data,
			.buf_sz = p_map_mem->sz
	};

	uint32_t ele_count = 0;

	if (! msgpack_get_map_ele_count(&mp, &ele_count)) {
		cf_crash(AS_PARTICLE, "map_particle_strip_indexes() hdr fail ele_count %u", ele_count);
	}

	as_packer pk = {
			.buffer = dest,
			.capacity = INT_MAX
	};

	if (ele_count != 0 && msgpack_peek_is_ext(&mp)) {
		msgpack_ext ext;

		if (! msgpack_get_ext(&mp, &ext)) {
			cf_crash(AS_PARTICLE, "map_particle_strip_indexes() invalid ext");
		}

		// Skip nil val.
		if (msgpack_sz(&mp) == 0) {
			cf_crash(AS_PARTICLE, "map_particle_strip_indexes() expected nil");
		}

		uint8_t flags = ext.type;

		if (flags != AS_PACKED_MAP_FLAG_NONE) {
			ele_count--;
		}

		flags &= ~(AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX);

		if (flags != AS_PACKED_MAP_FLAG_NONE) {
			as_pack_map_header(&pk, (uint32_t)ele_count + 1);
			as_pack_ext_header(&pk, 0, flags);
			pk.buffer[pk.offset++] = msgpack_nil[0];
		}
		else {
			as_pack_map_header(&pk, (uint32_t)ele_count);
		}
	}
	else {
		// Copy header.
		as_pack_map_header(&pk, (uint32_t)ele_count);
	}

	// Copy elements.
	uint32_t ele_sz = mp.buf_sz - mp.offset;

	memcpy(pk.buffer + pk.offset, mp.buf + mp.offset, ele_sz);

	return pk.offset + ele_sz;
}

//------------------------------------------------
// map_ele_find

static void
map_ele_find_init(map_ele_find *find, const packed_map *map)
{
	find->found_key = false;
	find->found_value = false;
	find->idx = map->ele_count;
	find->rank = map->ele_count;

	find->key_offset = 0;
	find->value_offset = 0;
	find->sz = 0;

	find->lower = 0;
	find->upper = map->ele_count;
}

static void
map_ele_find_continue_from_lower(map_ele_find *find, const map_ele_find *found,
		uint32_t ele_count)
{
	find->found_key = false;
	find->found_value = false;

	find->idx = ele_count + found->idx;
	find->idx /= 2;
	find->rank = find->idx;

	find->key_offset = found->key_offset;
	find->value_offset = found->value_offset;
	find->sz = found->sz;

	find->lower = found->idx;
	find->upper = ele_count;
}

static void
map_ele_find_init_from_idx(map_ele_find *find, const packed_map *map,
		uint32_t idx)
{
	map_ele_find_init(find, map);
	find->found_key = true;
	find->idx = idx;
	find->key_offset = offset_index_get_const(&map->offidx, idx);

	msgpack_in mp = {
			.buf = map->contents,
			.buf_sz = map->content_sz,
			.offset = find->key_offset
	};

	msgpack_sz(&mp);
	find->value_offset = mp.offset;
	find->sz = offset_index_get_const(&map->offidx, idx + 1) - find->key_offset;
}

//------------------------------------------------
// map_offset_index

static bool
map_offset_index_check_and_fill(offset_index *offidx, uint32_t index)
{
	uint32_t ele_filled = offset_index_get_filled(offidx);

	if (offidx->_.ele_count <= 1 || index < ele_filled ||
			offidx->_.ele_count == ele_filled) {
		return true;
	}

	msgpack_in mp = {
			.buf = offidx->contents,
			.buf_sz = offidx->content_sz
	};

	mp.offset = offset_index_get_const(offidx, ele_filled - 1);

	for (uint32_t i = ele_filled; i < index; i++) {
		if (msgpack_sz_rep(&mp, 2) == 0) {
			return false;
		}

		offset_index_set(offidx, i, mp.offset);
	}

	if (msgpack_sz_rep(&mp, 2) == 0) {
		return false;
	}

	// Make sure last iteration is in range for set.
	if (index < offidx->_.ele_count) {
		offset_index_set(offidx, index, mp.offset);
		offset_index_set_filled(offidx, index + 1);
	}
	else if (mp.offset != offidx->content_sz) { // size doesn't match
		cf_warning(AS_PARTICLE, "map_offset_index_fill() offset mismatch %u, expected %u", mp.offset, offidx->content_sz);
		return false;
	}
	else {
		offset_index_set_filled(offidx, offidx->_.ele_count);
	}

	return ! mp.has_nonstorage;
}

static void
map_offset_index_copy_rm_mask(offset_index *dest, const offset_index *src,
		const uint64_t *rm_mask, uint32_t rm_count)
{
	uint32_t ele_count = src->_.ele_count;
	uint32_t rm_idx = 0;
	uint32_t d_i = 0;
	uint32_t s_i = 0;
	int delta_sz = 0;

	for (uint32_t i = 0; i < rm_count; i++) {
		rm_idx = cdt_idx_mask_find(rm_mask, rm_idx, ele_count, false);

		uint32_t cpy_count = rm_idx - s_i;
		uint32_t rm_sz = offset_index_get_delta_const(src, rm_idx);

		offset_index_copy(dest, src, d_i, s_i, cpy_count, delta_sz);
		delta_sz -= rm_sz;
		d_i += cpy_count;
		s_i += cpy_count + 1;

		rm_idx++;
	}

	uint32_t tail_count = ele_count - s_i;

	offset_index_copy(dest, src, d_i, s_i, tail_count, delta_sz);
	d_i += tail_count;
	offset_index_set_filled(dest, d_i);
}

static void
map_offset_index_copy_rm_range(offset_index *dest, const offset_index *src,
		uint32_t rm_idx, uint32_t rm_count)
{
	offset_index_copy(dest, src, 0, 0, rm_idx, 0); // copy head

	uint32_t rm_end = rm_idx + rm_count;
	uint32_t tail_count = src->_.ele_count - rm_end;
	uint32_t mem_sz = offset_index_get_const(src, rm_end) -
			offset_index_get_const(src, rm_idx);

	offset_index_copy(dest, src, rm_idx, rm_end, tail_count, -(int)mem_sz); // copy tail
	offset_index_set_filled(dest, rm_idx + tail_count);
}

//------------------------------------------------
// order_index

void
order_index_map_sort(order_index *ordidx, const offset_index *offidx)
{
	for (uint32_t i = 0; i < ordidx->_.ele_count; i++) {
		order_index_set(ordidx, i, i);
	}

	order_index_sort(ordidx, offidx, offidx->contents, offidx->content_sz,
			SORT_BY_KEY);
}

static void
order_index_sort(order_index *ordidx, const offset_index *offsets,
		const uint8_t *contents, uint32_t content_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;

	index_sort_userdata udata = {
			.order = ordidx,
			.offsets = offsets,
			.contents = contents,
			.content_sz = content_sz,
			.sort_by = sort_by
	};

	if (ele_count <= 1) {
		return;
	}

	qsort_r(order_index_get_mem(ordidx, 0), ele_count, ordidx->_.ele_sz,
			map_packer_fill_index_sort_compare, (void *)&udata);
}

static inline void
order_index_set_sorted(order_index *ordidx, const offset_index *offsets,
		const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;

	if (ele_count <= 1) {
		if (order_index_is_null(ordidx)) {
			return;
		}

		if (ele_count == 1) {
			order_index_set(ordidx, 0, 0);
		}

		return;
	}

	cf_assert(! order_index_is_null(ordidx), AS_PARTICLE, "ordidx NULL");

	for (uint32_t i = 0; i < ele_count; i++) {
		order_index_set(ordidx, i, i);
	}

	order_index_sort(ordidx, offsets, ele_start, tot_ele_sz, sort_by);
}

static void
order_index_set_sorted_with_offsets(order_index *ordidx,
		const offset_index *offsets, sort_by_t sort_by)
{
	order_index_set_sorted(ordidx, offsets, offsets->contents,
			offsets->content_sz, sort_by);
}

static uint32_t
order_index_find_idx(const order_index *ordidx, uint32_t idx, uint32_t start,
		uint32_t len)
{
	for (uint32_t i = start; i < start + len; i++) {
		if (order_index_get(ordidx, i) == idx) {
			return i;
		}
	}

	return start + len;
}

//------------------------------------------------
// order_index_adjust

static uint32_t
order_index_adjust_lower(const order_index_adjust *via, uint32_t src)
{
	if (src >= via->lower) {
		return src + via->delta;
	}

	return src;
}

//------------------------------------------------
// order_index_op

static inline void
order_index_op_add(order_index *dest, const order_index *src, uint32_t add_idx,
		uint32_t add_rank)
{
	uint32_t ele_count = src->_.ele_count;

	order_index_adjust adjust = {
			.f = order_index_adjust_lower,
			.lower = add_idx,
			.upper = 0,
			.delta = 1
	};

	cf_assert(add_rank <= ele_count, AS_PARTICLE, "order_index_op_add() add_rank(%u) > ele_count(%u)", add_rank, ele_count);
	order_index_copy(dest, src, 0, 0, add_rank, &adjust);
	order_index_set(dest, add_rank, add_idx);
	order_index_copy(dest, src, add_rank + 1, add_rank, ele_count - add_rank,
			&adjust);
}

static inline void
order_index_op_replace1_internal(order_index *dest, const order_index *src,
		uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank,
		const order_index_adjust *adjust)
{
	uint32_t ele_count = src->_.ele_count;

	if (add_rank == remove_rank) {
		order_index_copy(dest, src, 0, 0, ele_count, NULL);
	}
	else if (add_rank > remove_rank) {
		order_index_copy(dest, src, 0, 0, remove_rank, adjust);
		order_index_copy(dest, src, remove_rank, remove_rank + 1,
				add_rank - remove_rank - 1, adjust);
		order_index_set(dest, add_rank - 1, add_idx);
		order_index_copy(dest, src, add_rank, add_rank, ele_count - add_rank,
				adjust);
	}
	else {
		order_index_copy(dest, src, 0, 0, add_rank, adjust);
		order_index_set(dest, add_rank, add_idx);
		order_index_copy(dest, src, add_rank + 1, add_rank,
				remove_rank - add_rank, adjust);
		order_index_copy(dest, src, remove_rank + 1, remove_rank + 1,
				ele_count - remove_rank - 1, adjust);
	}
}

// Replace remove_rank with add_rank in dest.
static inline void
order_index_op_replace1(order_index *dest, const order_index *src,
		uint32_t add_rank, uint32_t remove_rank)
{
	uint32_t add_idx = order_index_get(src, remove_rank);

	order_index_op_replace1_internal(dest, src, add_idx, add_rank, remove_rank,
			NULL);
}

static void
order_index_op_remove_idx_mask(order_index *dest, const order_index *src,
		const uint64_t *mask, uint32_t count)
{
	if (count == 0) {
		return;
	}

	uint32_t ele_count = src->max_idx;
	uint32_t mask_count = cdt_idx_mask_count(ele_count);
	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
	define_order_index2(cntidx, ele_count, mask_count, alloc_idx);

	order_index_set(&cntidx, 0, cf_bit_count64(mask[0]));

	for (uint32_t i = 1; i < mask_count; i++) {
		uint32_t prev = order_index_get(&cntidx, i - 1);

		order_index_set(&cntidx, i, prev + cf_bit_count64(mask[i]));
	}

	uint32_t di = 0;

	for (uint32_t i = 0; i < ele_count; i++) {
		uint32_t idx = order_index_get(src, i);

		if (idx >= ele_count || cdt_idx_mask_is_set(mask, idx)) {
			continue;
		}

		uint32_t mask_i = idx / 64;
		uint32_t offset = idx % 64;
		uint64_t bits = cdt_idx_mask_get(mask, idx) & ((1ULL << offset) - 1);

		if (mask_i == 0) {
			idx -= cf_bit_count64(bits);
		}
		else {
			idx -= cf_bit_count64(bits) + order_index_get(&cntidx, mask_i - 1);
		}

		order_index_set(dest, di++, idx);
	}

	rollback_alloc_rollback(alloc_idx);
	cf_assert(dest->_.ele_count == di, AS_PARTICLE, "count mismatch ele_count %u != di %u", dest->_.ele_count, di);
}


//==========================================================
// result_data

static bool
result_data_set_key_not_found(cdt_result_data *rd, int64_t index)
{
	switch (rd->type) {
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		break;
	default:
		return result_data_set_not_found(rd, index);
	}

	return false;
}

static bool
result_data_set_value_not_found(cdt_result_data *rd, int64_t rank)
{
	switch (rd->type) {
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX_RANGE:
		return false;
	default:
		return result_data_set_not_found(rd, rank);
	}

	return true;
}


//==========================================================
// cdt_map_builder
//

void
cdt_map_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf,
		uint32_t ele_count, uint32_t max_sz, uint8_t flags)
{
	uint32_t sz = sizeof(map_mem) + sizeof(uint64_t) + 1 + 3 + max_sz;
	map_mem *p_map_mem = (map_mem *)rollback_alloc_reserve(alloc_buf, sz);

	as_packer pk = {
			.buffer = p_map_mem->data,
			.capacity = INT_MAX
	};

	if (flags != AS_PACKED_MAP_FLAG_NONE) {
		as_pack_map_header(&pk, ele_count + 1);
		as_pack_ext_header(&pk, 0, flags);
		pk.buffer[pk.offset++] = msgpack_nil[0];
	}
	else {
		as_pack_map_header(&pk, ele_count);
	}

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = pk.offset;

	builder->particle = (as_particle *)p_map_mem;
	builder->write_ptr = p_map_mem->data + p_map_mem->sz;
	builder->ele_count = 0;
	builder->sz = &p_map_mem->sz;
}


//==========================================================
// cdt_process_state_packed_map
//

bool
cdt_process_state_packed_map_modify_optype(cdt_process_state *state,
		cdt_op_mem *com)
{
	cdt_context *ctx = &com->ctx;
	as_cdt_optype optype = state->type;

	if (ctx->data_sz == 0 && cdt_context_inuse(ctx) &&
			! is_map_type(as_bin_get_particle_type(ctx->b))) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid type %d", as_bin_get_particle_type(ctx->b));
		com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	int ret = AS_OK;

	switch (optype) {
	case AS_CDT_OP_MAP_SET_TYPE: {
		uint64_t flags;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &flags)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_map_if_notinuse(ctx, 0);
		ret = map_set_flags(com, (uint8_t)flags);
		break;
	}
	case AS_CDT_OP_MAP_ADD: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		cdt_context_use_static_map_if_notinuse(ctx, flags);
		ret = map_add(com, &key, &value, &control, true);
		break;
	}
	case AS_CDT_OP_MAP_ADD_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		cdt_context_use_static_map_if_notinuse(ctx, flags);
		ret = map_add_items(com, &items, &control);
		break;
	}
	case AS_CDT_OP_MAP_PUT: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;
		uint64_t modify = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = ! (modify & AS_CDT_MAP_NO_OVERWRITE),
				.allow_create = ! (modify & AS_CDT_MAP_NO_CREATE),
				.no_fail = modify & AS_CDT_MAP_NO_FAIL,
				.do_partial = modify & AS_CDT_MAP_DO_PARTIAL
		};

		cdt_context_use_static_map_if_notinuse(ctx, flags);
		ret = map_add(com, &key, &value, &control, true);
		break;
	}
	case AS_CDT_OP_MAP_PUT_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;
		uint64_t modify = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = ! (modify & AS_CDT_MAP_NO_OVERWRITE),
				.allow_create = ! (modify & AS_CDT_MAP_NO_CREATE),
				.no_fail = modify & AS_CDT_MAP_NO_FAIL,
				.do_partial = modify & AS_CDT_MAP_DO_PARTIAL
		};

		cdt_context_use_static_map_if_notinuse(ctx, flags);
		ret = map_add_items(com, &items, &control);
		break;
	}
	case AS_CDT_OP_MAP_REPLACE: {
		cdt_payload key;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		cdt_context_use_static_map_if_notinuse(ctx, 0);
		ret = map_add(com, &key, &value, &control, true);
		break;
	}
	case AS_CDT_OP_MAP_REPLACE_ITEMS: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			com->ret_code = -AS_ERR_ELEMENT_NOT_FOUND;
			return false;
		}

		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		ret = map_add_items(com, &items, &control);
		break;
	}
	case AS_CDT_OP_MAP_INCREMENT:
	case AS_CDT_OP_MAP_DECREMENT: {
		cdt_payload key;
		cdt_payload delta_value = { 0 };
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &delta_value, &flags)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_map_if_notinuse(ctx, flags);
		ret = map_increment(com, &key, &delta_value,
				optype == AS_CDT_OP_MAP_DECREMENT);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t op_flags;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &op_flags, &key)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, op_flags, false);
		ret = map_remove_by_key_interval(com, &key, &key);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = map_remove_by_index_range(com, index, 1);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = map_remove_by_value_interval(com, &value, &value);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = map_remove_by_rank_range(com, index, 1);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_all_by_key_list(com, &items);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_value_interval(com, &value, &value);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_all_by_value_list(com, &items);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end = { 0 };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start,
				&key_end)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_key_interval(com, &key_start, &key_end);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_index_range(com, index, count);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { 0 };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_value_interval(com, &value_start, &value_end);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_rank_range(com, rank, count);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &index,
				&count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_rel_index_range(com, &value, index, count);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &rank,
				&count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = map_remove_by_rel_rank_range(com, &value, rank, count);
		break;
	}
	case AS_CDT_OP_MAP_CLEAR: {
		if (ctx->create_triggered || ! cdt_context_inuse(ctx)) {
			return true; // no-op
		}

		ret = map_clear(com);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid cdt op: %d", optype);
		com->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	if (ret != AS_OK) {
		if (ret == AS_ERR_ELEMENT_NOT_FOUND || ret == AS_ERR_ELEMENT_EXISTS) {
			cf_detail(AS_PARTICLE, "%s: failed", cdt_process_state_get_op_name(state));
		}
		else {
			cf_warning(AS_PARTICLE, "%s: failed", cdt_process_state_get_op_name(state));
		}

		com->ret_code = ret;
		return false;
	}

	if (ctx->b->particle == (const as_particle *)&map_mem_empty) {
		cdt_context_set_empty_packed_map(ctx, 0);
	}
	else if (ctx->b->particle == (const as_particle *)&map_mem_empty_k) {
		cdt_context_set_empty_packed_map(ctx,
				map_mem_empty_k.data[MAP_EMPTY_EXT_FLAGS]);
	}
	else if (ctx->b->particle == (const as_particle *)&map_mem_empty_kv) {
		cdt_context_set_empty_packed_map(ctx,
				map_mem_empty_kv.data[MAP_EMPTY_EXT_FLAGS]);
	}

	return true;
}

bool
cdt_process_state_packed_map_read_optype(cdt_process_state *state,
		cdt_op_mem *com)
{
	const cdt_context *ctx = &com->ctx;
	as_cdt_optype optype = state->type;

	if (ctx->data_sz == 0 && ! is_map_type(as_bin_get_particle_type(ctx->b))) {
		com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	packed_map map;

	if (! packed_map_init_from_com(&map, com, false)) {
		cf_warning(AS_PARTICLE, "%s: invalid map", cdt_process_state_get_op_name(state));
		com->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	int ret = AS_OK;
	cdt_result_data *result = &com->result;

	switch (optype) {
	case AS_CDT_OP_MAP_SIZE: {
		as_bin_set_int(result->result, map.ele_count);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY: {
		uint64_t op_flags;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &op_flags, &key)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, op_flags, false);
		ret = packed_map_get_remove_by_key_interval(&map, com, &key, &key);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = packed_map_get_remove_by_value_interval(&map, com, &value,
				&value);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX: {
		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = packed_map_get_remove_by_index_range(&map, com, index, 1);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK: {
		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, false);
		ret = packed_map_get_remove_by_rank_range(&map, com, rank, 1);
		break;
	}
	case AS_CDT_OP_MAP_GET_ALL_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_value_interval(&map, com, &value,
				&value);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL: {
		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end = { 0 };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start,
				&key_end)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_key_interval(&map, com, &key_start,
				&key_end);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL: {
		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { 0 };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_value_interval(&map, com, &value_start,
				&value_end);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX_RANGE: {
		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_index_range(&map, com, index, count);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK_RANGE: {
		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_rank_range(&map, com, rank, count);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_LIST: {
		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_all_by_key_list(&map, com, &items);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_LIST: {
		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_all_by_value_list(&map, com, &items);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE: {
		uint64_t result_type;
		cdt_payload value;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &index,
				&count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_rel_index_range(&map, com, &value, index,
				count);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE: {
		uint64_t result_type;
		cdt_payload value;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &rank,
				&count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&com->result, result_type, true);
		ret = packed_map_get_remove_by_rel_rank_range(&map, com, &value, rank,
				count);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_read_optype() invalid cdt op: %d", optype);
		com->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	if (ret != AS_OK) {
		cf_warning(AS_PARTICLE, "%s: failed", cdt_process_state_get_op_name(state));
		com->ret_code = ret;
		return false;
	}

	return true;
}


//==========================================================
// Debugging support.
//

void
map_print(const packed_map *map, const char *name)
{
	print_packed(map->packed, map->packed_sz, name);
}

static bool
map_verify_fn(const cdt_context *ctx, rollback_alloc *alloc_idx)
{
	if (! ctx->b) {
		return true;
	}

	if (ctx->create_triggered) {
		return true; // check after unwind
	}

	packed_map map;
	uint8_t type = as_bin_get_particle_type(ctx->b);

	if (type != AS_PARTICLE_TYPE_LIST && type != AS_PARTICLE_TYPE_MAP) {
		cf_warning(AS_PARTICLE, "map_verify() non-map type: %u", type);
		return false;
	}

	// Check header.
	if (! packed_map_init_from_ctx(&map, ctx, false)) {
		cf_warning(AS_PARTICLE, "map_verify() invalid packed map");
		return false;
	}

	if (map.flags != 0) {
		const uint8_t *byte = map.contents - 1;

		if (*byte != 0xC0) {
			cf_warning(AS_PARTICLE, "map_verify() invalid ext header, expected C0 for pair.2");
		}
	}

	const order_index *ordidx = &map.ordidx;
	bool check_offidx = map_has_offidx(&map);
	define_map_msgpack_in(mp, &map);
	setup_map_must_have_offidx(u, &map, alloc_idx);

	uint32_t filled = offset_index_get_filled(u->offidx);
	define_offset_index(temp_offidx, u->offidx->contents, u->offidx->content_sz,
			u->offidx->_.ele_count, alloc_idx);

	if (map.ele_count > 1) {
		offset_index_copy(&temp_offidx, u->offidx, 0, 0, filled, 0);
	}

	// Check offsets.
	for (uint32_t i = 0; i < map.ele_count; i++) {
		uint32_t offset;

		if (check_offidx) {
			if (i < filled) {
				offset = offset_index_get_const(u->offidx, i);

				if (mp.offset != offset) {
					cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u expected=%d", i, offset, mp.offset);
					return false;
				}
			}
			else {
				offset_index_set(&temp_offidx, i, mp.offset);
			}
		}
		else {
			offset_index_set(u->offidx, i, mp.offset);
		}

		offset = mp.offset;

		if (msgpack_sz(&mp) == 0) {
			cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u invalid key", i, offset, mp.offset);
			return false;
		}

		offset = mp.offset;

		if (msgpack_sz(&mp) == 0) {
			cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u invalid value", i, offset, mp.offset);
			return false;
		}
	}

	if (! check_offidx) {
		offset_index_set_filled(u->offidx, map.ele_count);
	}
	else if (filled < map.ele_count) {
		u->offidx->_.ptr = temp_offidx._.ptr;
	}

	// Check packed size.
	if (map.content_sz != mp.offset) {
		cf_warning(AS_PARTICLE, "map_verify() content_sz=%u expected=%u", map.content_sz, mp.offset);
		return false;
	}

	// Check key orders.
	if (map_is_k_ordered(&map) && map.ele_count > 0) {
		mp.offset = 0;

		if (msgpack_sz_rep(&mp, 2) == 0) {
			cf_warning(AS_PARTICLE, "map_verify() i=0 pk.offset=%u invalid pair", mp.offset);
		}

		define_map_msgpack_in(mp_key, &map);

		for (uint32_t i = 1; i < map.ele_count; i++) {
			uint32_t offset = mp.offset;
			msgpack_cmp_type cmp = msgpack_cmp(&mp_key, &mp);

			if (cmp == MSGPACK_CMP_ERROR) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u invalid key", i, offset, mp.offset);
				return false;
			}

			if (cmp == MSGPACK_CMP_GREATER) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u keys not in order", i, offset, mp.offset);
				return false;
			}

			mp_key.offset = offset;

			if (msgpack_sz(&mp) == 0) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u invalid value", i, offset, mp.offset);
				return false;
			}
		}
	}
	else if (map.ele_count > 0) { // check unordered key uniqueness
		define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes
		define_order_index(key_ordidx, map.ele_count, alloc_idx);

		order_index_set_sorted_with_offsets(&key_ordidx, u->offidx,
				SORT_BY_KEY);

		uint32_t dup_count;
		uint32_t dup_sz;

		if (! order_index_sorted_mark_dup_eles(&key_ordidx, u->offidx,
				&dup_count, &dup_sz)) {
			cf_warning(AS_PARTICLE, "map_verify() mark dup failed");
			return false;
		}

		if (dup_count != 0) {
			cf_warning(AS_PARTICLE, "map_verify() dup key");
			return false;
		}
	}

	// Check value orders.
	if (order_index_is_filled(ordidx) && map.ele_count > 1) {
		// Compare with freshly sorted.
		define_order_index(cmp_order, map.ele_count, alloc_idx);

		order_index_set_sorted(&cmp_order, u->offidx, map.contents,
				map.content_sz, SORT_BY_VALUE);

		for (uint32_t i = 0; i < map.ele_count; i++) {
			uint32_t expected = order_index_get(&cmp_order, i);
			uint32_t index = order_index_get(ordidx, i);

			if (index != expected) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u index=%u expected=%u invalid order index", i, index, expected);
				return false;
			}
		}

		// Walk index and check value order.
		mp.offset = 0;

		define_map_msgpack_in(prev_value, &map);
		uint32_t index = order_index_get(ordidx, 0);

		prev_value.offset = offset_index_get_const(u->offidx, index);

		if (msgpack_sz(&prev_value) == 0) {
			cf_warning(AS_PARTICLE, "map_verify() index=%u mp.offset=%u invalid key", index, mp.offset);
			return false;
		}

		for (uint32_t i = 1; i < map.ele_count; i++) {
			index = order_index_get(ordidx, i);
			mp.offset = offset_index_get_const(u->offidx, index);

			if (msgpack_sz(&mp) == 0) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u index=%u mp.offset=%u invalid key", i, index, mp.offset);
				return false;
			}

			uint32_t offset = mp.offset;
			msgpack_cmp_type cmp = msgpack_cmp(&prev_value, &mp);

			if (cmp == MSGPACK_CMP_ERROR) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u invalid value", i, offset, mp.offset);
				return false;
			}

			if (cmp == MSGPACK_CMP_GREATER) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u mp.offset=%u value index not in order", i, offset, mp.offset);
				return false;
			}

			prev_value.offset = offset;
		}
	}

	return true;
}

bool
map_verify(const cdt_context *ctx)
{
	define_rollback_alloc(alloc_idx, NULL, 8, false); // for temp indexes
	bool ret = map_verify_fn(ctx, alloc_idx);

	rollback_alloc_rollback(alloc_idx);
	return ret;
}
