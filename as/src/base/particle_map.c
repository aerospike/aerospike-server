/*
 * particle_map.c
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
#include "fault.h"

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

//#define MAP_DEBUG_VERIFY

#define LINEAR_FIND_RANK_MAX_COUNT		16 // switch to linear search when the count drops to this number

#define AS_PACKED_MAP_FLAG_RESERVED_0	0x04 // placeholder for multimap
#define AS_PACKED_MAP_FLAG_OFF_IDX		0x10 // has list offset index
#define AS_PACKED_MAP_FLAG_ORD_IDX		0x20 // has value order index
#define AS_PACKED_MAP_FLAG_ON_STACK		0x40 // map on stack

struct packed_map_s;

typedef bool (*packed_map_get_by_idx_func)(const struct packed_map_s *userdata, cdt_payload *contents, uint32_t index);

typedef struct offidx_op_s {
	offset_index *dest;
	const offset_index *src;
	uint32_t d_i;
	uint32_t s_i;
	int delta;
} offidx_op;

typedef struct packed_map_s {
	const uint8_t *packed;
	const uint8_t *contents; // where elements start (excludes ext)
	uint32_t packed_sz;
	uint32_t content_sz;

	// Mutable field member (Is considered mutable in const objects).
	offset_index offidx; // offset start at contents (excluding ext metadata pair)
	uint8_t flags;
	// Mutable field member.
	order_index value_idx;

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

	offset_index offset_idx;	// offset start at ele_start (excluding ext metadata pair)
	order_index value_idx;

	uint32_t ele_count;
	uint32_t content_sz;		// does not include map header or ext
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

typedef struct msgpack_map_empty_flagged_s {
	uint8_t		map_hdr;
	uint8_t		ext_hdr;
	uint8_t		ext_sz;
	uint8_t		ext_flags;
	uint8_t		nil;
} __attribute__ ((__packed__)) msgpack_map_empty_flagged;

typedef struct map_mem_empty_flagged_s {
	map_mem mem;
	msgpack_map_empty_flagged map;
} map_mem_empty_flagged;

#define MSGPACK_MAP_FLAGGED(__flags) { \
		.map_hdr = 0x81, \
		.ext_hdr = 0xC7, \
		.ext_sz = 0, \
		.ext_flags = __flags, \
		.nil = 0xC0 \
}

#define MAP_MEM_EMPTY_FLAGGED_ENTRY(__flag) { \
	{ \
			.type = AS_PARTICLE_TYPE_MAP, \
			.sz = sizeof(msgpack_map_empty_flagged) \
	}, \
	MSGPACK_MAP_FLAGGED(__flag) \
}

static const map_mem_empty_flagged map_mem_empty_flagged_table[] = {
		MAP_MEM_EMPTY_FLAGGED_ENTRY(AS_PACKED_MAP_FLAG_K_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX),
		MAP_MEM_EMPTY_FLAGGED_ENTRY(AS_PACKED_MAP_FLAG_KV_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX),
};
static const map_mem map_mem_empty = {
		.type = AS_PARTICLE_TYPE_MAP,
		.sz = 1,
		.data = {0x80},
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
	bool error;
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

// TODO - refactor params using this.
typedef struct map_getrem_s {
	const packed_map *map;
	as_bin *b;
	rollback_alloc *alloc_buf;
	cdt_result_data *result;
} map_getrem;

typedef struct {
	offset_index *offidx;
	uint8_t mem_temp[];
} __attribute__ ((__packed__)) map_vla_off;

typedef struct {
	offset_index *offidx;
	order_index *ordidx;
	uint8_t mem_temp[];
} __attribute__ ((__packed__)) map_vla_offord;

#define as_bin_use_static_map_mem_if_notinuse(__b, __flags) \
		if (! as_bin_inuse(b)) { \
			if (is_kv_ordered(__flags)) { \
				(__b)->particle = (as_particle *)(map_mem_empty_flagged_table + 1); \
			} \
			else if (is_k_ordered(__flags)) { \
				(__b)->particle = (as_particle *)map_mem_empty_flagged_table; \
			} \
			else { \
				(__b)->particle = (as_particle *)&map_mem_empty; \
			} \
			as_bin_state_set_from_type(__b, AS_PARTICLE_TYPE_MAP); \
		}

#define vla_map_offidx_if_invalid(__name, __map_p) \
		uint8_t __name ## __vlatemp[sizeof(offset_index *) + offset_index_vla_sz(&(__map_p)->offidx)]; \
		map_vla_off *__name = (map_vla_off *)__name ## __vlatemp; \
		__name->offidx = (offset_index *)&(__map_p)->offidx; \
		offset_index_alloc_temp(__name->offidx, __name->mem_temp);

#define vla_map_allidx_if_invalid(__name, __map_p) \
		uint8_t __name ## __vlatemp[sizeof(offset_index *) + sizeof(order_index *) + map_allidx_vla_sz(__map_p)]; \
		map_vla_offord *__name = (map_vla_offord *)__name ## __vlatemp; \
		__name->offidx = (offset_index *)&(__map_p)->offidx; \
		__name->ordidx = (order_index *)&(__map_p)->value_idx; \
		map_allidx_alloc_temp(__map_p, __name->mem_temp);

#define define_map_unpacker(__name, __map_ptr) \
		as_unpacker __name = { \
				.buffer = (__map_ptr)->contents, \
				.length = (__map_ptr)->content_sz \
		}

#define define_map_op(__name, __map_ptr) \
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
static inline void map_allidx_alloc_temp(const packed_map *map, uint8_t *mem_temp);

static inline uint32_t map_ext_content_sz(const packed_map *map);
static inline bool map_is_k_ordered(const packed_map *map);
static inline bool map_is_kv_ordered(const packed_map *map);
static inline bool map_has_offidx(const packed_map *map);
static inline bool map_fill_offidx(const packed_map *map);

static inline bool skip_map_pair(as_unpacker *pk);

// map_packer
static as_particle *map_packer_create_particle(map_packer *pk, rollback_alloc *alloc_buf);
static void map_packer_init(map_packer *pk, uint32_t ele_count, uint8_t flags, uint32_t content_sz);
static void map_packer_setup_bin(map_packer *pk, as_bin *b, rollback_alloc *alloc_buf);
static void map_packer_write_hdridx(map_packer *pk);
static bool map_packer_fill_offset_index(map_packer *mpk);
static int map_packer_fill_index_sort_compare(const void *x, const void *y, void *p);
static bool map_packer_fill_ordidx(map_packer *mpk, const uint8_t *contents, uint32_t content_sz);
static bool map_packer_add_op_copy_index(map_packer *mpk, const packed_map_op *add_op, map_ele_find *remove_info, const map_ele_find *add_info, uint32_t kv_sz);
static inline void map_packer_write_seg1(map_packer *pk, const packed_map_op *op);
static inline void map_packer_write_seg2(map_packer *pk, const packed_map_op *op);
static inline void map_packer_write_msgpack_seg(map_packer *pk, const cdt_payload *seg);

// map
static int map_set_flags(as_bin *b, rollback_alloc *alloc_buf, as_bin *result, uint8_t set_flags);
static int map_increment(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *delta_value, as_bin *result, bool is_decrement);
static int map_add(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *value, as_bin *result, const map_add_control *control);
static int map_add_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, as_bin *result, const map_add_control *control);

static int map_remove_by_key_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result);
static int map_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result);
static int map_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int map_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result);

static int map_remove_by_rel_index_range(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value, int64_t index, uint64_t count, cdt_result_data *result);
static int map_remove_by_rel_rank_range(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank, uint64_t count, cdt_result_data *result);

static int map_remove_all_by_key_list(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_list, cdt_result_data *result);
static int map_remove_all_by_value_list(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_list, cdt_result_data *result);

static int map_clear(as_bin *b, rollback_alloc *alloc_buf, as_bin *result);

// packed_map
static bool packed_map_init(packed_map *map, const uint8_t *buf, uint32_t sz, bool fill_idxs);
static inline bool packed_map_init_from_particle(packed_map *map, const as_particle *p, bool fill_idxs);
static bool packed_map_init_from_bin(packed_map *map, const as_bin *b, bool fill_idxs);
static bool packed_map_unpack_hdridx(packed_map *map, bool fill_idxs);

static void packed_map_init_indexes(const packed_map *map, as_packer *pk);

static bool packed_map_ensure_ordidx_filled(const packed_map *op);

static uint32_t packed_map_find_index_by_idx_unordered(const packed_map *map, uint32_t idx);
static uint32_t packed_map_find_index_by_key_unordered(const packed_map *map, const cdt_payload *key);

static void packed_map_find_rank_indexed_linear(const packed_map *map, map_ele_find *find, uint32_t start, uint32_t len);
static bool packed_map_find_rank_indexed(const packed_map *map, map_ele_find *find);
static bool packed_map_find_rank_by_value_indexed(const packed_map *map, map_ele_find *find, const cdt_payload *value);
static bool packed_map_find_rank_range_by_value_interval_indexed(const packed_map *map, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, bool is_multi);
static bool packed_map_find_rank_range_by_value_interval_unordered(const packed_map *map, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, uint64_t *mask);
static bool packed_map_find_key_indexed(const packed_map *map, map_ele_find *find, const cdt_payload *key);
static bool packed_map_find_key(const packed_map *map, map_ele_find *find, const cdt_payload *key);

static int packed_map_get_remove_by_key_interval(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result);
static int packed_map_get_remove_by_index_range(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result);

static int packed_map_get_remove_by_value_interval(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int packed_map_get_remove_by_rank_range(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result);

static int packed_map_get_remove_all_by_key_list(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_list, cdt_result_data *result);
static int packed_map_get_remove_all_by_key_list_ordered(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk, uint32_t items_count, cdt_result_data *result);
static int packed_map_get_remove_all_by_key_list_unordered(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk, uint32_t items_count, cdt_result_data *result);
static int packed_map_get_remove_all_by_value_list(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_list, cdt_result_data *result);
static int packed_map_get_remove_all_by_value_list_ordered(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk, uint32_t items_count, cdt_result_data *result);

static int packed_map_get_remove_by_rel_index_range(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, int64_t index, uint64_t count, cdt_result_data *result);
static int packed_map_get_remove_by_rel_rank_range(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank, uint64_t count, cdt_result_data *result);

static int packed_map_get_remove_all(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, cdt_result_data *result);

static int packed_map_remove_by_mask(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, const uint64_t *rm_mask, uint32_t count, uint32_t *rm_sz_r);
static int packed_map_remove_idx_range(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf, uint32_t idx, uint32_t count);

static bool packed_map_get_range_by_key_interval_unordered(const packed_map *map, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count, uint64_t *mask);
static bool packed_map_get_range_by_key_interval_ordered(const packed_map *map, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count);
static int packed_map_build_rank_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static int packed_map_build_rank_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, cdt_result_data *result);
static int packed_map_build_rank_result_by_index_range(const packed_map *map, uint32_t index, uint32_t count, cdt_result_data *result);

static bool packed_map_get_key_by_idx(const packed_map *map, cdt_payload *key, uint32_t index);
static bool packed_map_get_value_by_idx(const packed_map *map, cdt_payload *value, uint32_t idx);
static bool packed_map_get_pair_by_idx(const packed_map *map, cdt_payload *value, uint32_t index);

static int packed_map_build_index_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static int packed_map_build_index_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, cdt_result_data *result);
static bool packed_map_build_ele_result_by_idx_range(const packed_map *map, uint32_t start_idx, uint32_t count, cdt_result_data *result);
static bool packed_map_build_ele_result_by_ele_idx(const packed_map *map, const order_index *ele_idx, uint32_t start, uint32_t count, uint32_t rm_sz, cdt_result_data *result);
static bool packed_map_build_ele_result_by_mask(const packed_map *map, const uint64_t *mask, uint32_t count, uint32_t rm_sz, cdt_result_data *result);
static int packed_map_build_result_by_key(const packed_map *map, const cdt_payload *key, uint32_t idx, uint32_t count, cdt_result_data *result);

static int64_t packed_map_get_rank_by_idx(const packed_map *map, uint32_t idx);
static int packed_map_build_rank_result_by_idx(const packed_map *map, uint32_t idx, cdt_result_data *result);
static int packed_map_build_rank_result_by_idx_range(const packed_map *map, uint32_t idx, uint32_t count, cdt_result_data *result);

static msgpack_compare_t packed_map_compare_key_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2);
static msgpack_compare_t packed_map_compare_values(as_unpacker *pk1, as_unpacker *pk2);
static msgpack_compare_t packed_map_compare_value_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2);

static bool packed_map_write_k_ordered(const packed_map *map, uint8_t *write_ptr, offset_index *offsets_new);

// packed_map_op
static void packed_map_op_init(packed_map_op *op, const packed_map *map);
static int32_t packed_map_op_add(packed_map_op *op, const map_ele_find *found);
static int32_t packed_map_op_remove(packed_map_op *op, const map_ele_find *found, uint32_t count, uint32_t remove_sz);

static uint8_t *packed_map_op_write_seg1(const packed_map_op *op, uint8_t *buf);
static uint8_t *packed_map_op_write_seg2(const packed_map_op *op, uint8_t *buf);
static bool packed_map_op_write_new_offidx(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, offset_index *new_offidx, uint32_t kv_sz);
static bool packed_map_op_write_new_ordidx(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, order_index *value_idx);

// map_particle
static as_particle *map_particle_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t content_sz, uint8_t flags);
static int64_t map_particle_strip_indexes(const as_particle *p, uint8_t *dest);

// map_ele_find
static void map_ele_find_init(map_ele_find *find, const packed_map *map);
static void map_ele_find_continue_from_lower(map_ele_find *find, const map_ele_find *found, uint32_t ele_count);
static void map_ele_find_init_from_idx(map_ele_find *find, const packed_map *map, uint32_t idx);

// map_offset_index
static bool map_offset_index_fill(offset_index *offidx, uint32_t index);
static int64_t map_offset_index_get(offset_index *offidx, uint32_t index);
static int64_t map_offset_index_get_delta(offset_index *offidx, uint32_t index);

// offidx_op
static void offidx_op_init(offidx_op *op, offset_index *dest, const offset_index *src);
static void offidx_op_remove(offidx_op *op, uint32_t index);
static void offidx_op_remove_range(offidx_op *op, uint32_t index, uint32_t count);
static void offidx_op_end(offidx_op *op);

// order_index
static bool order_index_sort(order_index *ordidx, const offset_index *offsets, const uint8_t *contents, uint32_t content_sz, sort_by_t sort_by);
static inline bool order_index_set_sorted(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by);
static bool order_index_set_sorted_with_offsets(order_index *ordidx, const offset_index *offsets, sort_by_t sort_by);

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
static void map_print(const packed_map *map, const char *name);
static bool map_verify(const as_bin *b);


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

	as_unpacker pk = {
			.buffer = map.contents,
			.length = map.content_sz
	};

	if (cdt_get_storage_list_sz(&pk, 2 * map.ele_count) != map.content_sz) {
		cf_warning(AS_PARTICLE, "map_size_from_wire() invalid packed map: ele_count %u offset %u content_sz %u", map.ele_count, pk.offset, map.content_sz);
		return -AS_ERR_PARAMETER;
	}

	if (map.flags == 0) {
		return (int32_t)(sizeof(map_mem) + value_size);
	}

	uint32_t extra_sz = map_ext_content_sz(&map);

	// 1 byte for header, 1 byte for type, 1 byte for length for existing ext.
	extra_sz += as_pack_ext_header_get_size(extra_sz) - 3;

	return (int32_t)(sizeof(map_mem) + value_size + extra_sz);
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
		p_map_mem->sz = value_size;
		memcpy(p_map_mem->data, wire_value, value_size);
		return AS_OK;
	}

	// TODO - May want to check key order here but for now we'll trust the client/other node.
	uint32_t ext_content_sz = map_ext_content_sz(&map);
	// 1 byte for header, 1 byte for type, 1 byte for length for existing ext.
	uint32_t extra_sz = as_pack_ext_header_get_size(ext_content_sz) - 3;

	as_packer pk = {
			.buffer = p_map_mem->data,
			.capacity = value_size + extra_sz
	};

	as_pack_map_header(&pk, map.ele_count + 1);
	as_pack_ext_header(&pk, ext_content_sz,
			map_adjust_incoming_flags(map.flags));
	packed_map_init_indexes(&map, &pk);
	as_pack_val(&pk, &as_nil);
	memcpy(pk.buffer + pk.offset, map.contents, map.content_sz);
	p_map_mem->sz = value_size + ext_content_sz + extra_sz;

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = *pp;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);

		if (! map_verify(&b)) {
			offset_index_print(&map.offidx, "verify");
			cf_warning(AS_PARTICLE, "map_from_wire: pp=%p wire_value=%p", pp, wire_value);
		}
	}
#endif

	return AS_OK;
}

int
map_compare_from_wire(const as_particle *p, as_particle_type wire_type,
		const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	cf_warning(AS_PARTICLE, "map_compare_from_wire() not implemented");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

uint32_t
map_wire_size(const as_particle *p)
{
	packed_map map;

	if (! packed_map_init_from_particle(&map, p, false)) {
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
	int64_t sz = map_particle_strip_indexes(p, wire);
	cf_assert(sz >= 0, AS_PARTICLE, "map_to_wire() strip failed with sz %ld", sz);
	return (uint32_t)sz;
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

	mpk.write_ptr = p_map_mem->data;
	map_packer_write_hdridx(&mpk);
	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes

	cdt_idx_set_alloc(alloc_idx);

	if (! packed_map_write_k_ordered(map, mpk.write_ptr, &mpk.offset_idx)) {
		cf_crash(AS_PARTICLE, "map_from_asval() sort on key failed");
	}

	p_map_mem->sz =
			(uint32_t)(mpk.contents - p_map_mem->data + map->content_sz);

	if (order_index_is_valid(&mpk.value_idx)) {
		order_index_set(&mpk.value_idx, 0, map->ele_count);
	}

	cf_free(temp_mem);
	cdt_idx_clear();

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = (as_particle *)p_map_mem;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);
		if (! map_verify(&b)) {
			cdt_bin_print(&b, "map_from_asval");
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

	// This path implies disk-backed data-in-memory so fill_idxs -> true.
	if (! packed_map_init(&map, p_map_flat->data, p_map_flat->sz, true)) {
		cf_warning(AS_PARTICLE, "map_from_flat() invalid packed map");
		return NULL;
	}

	if (map.flags == 0) {
		// Convert temp buffer from disk to data-in-memory.
		map_mem *p_map_mem = cf_malloc_ns(sizeof(map_mem) + p_map_flat->sz);

		p_map_mem->type = p_map_flat->type;
		p_map_mem->sz = p_map_flat->sz;
		memcpy(p_map_mem->data, p_map_flat->data, p_map_mem->sz);

		*pp = (as_particle *)p_map_mem;

		return flat;
	}

	uint8_t flags = map_adjust_incoming_flags(map.flags);
	define_map_packer(mpk, map.ele_count, flags, map.content_sz);
	as_particle *p = map_packer_create_particle(&mpk, NULL);

	map_packer_write_hdridx(&mpk);
	memcpy(mpk.write_ptr, map.contents, map.content_sz);

	if (! map_packer_fill_offset_index(&mpk)) {
		cf_free(p);
		return NULL;
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (! order_index_set_sorted(&mpk.value_idx, &mpk.offset_idx,
				map.contents, map.content_sz, SORT_BY_VALUE)) {
			cf_free(p);
			return NULL;
		}
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
		const as_bin b = {
				.particle = (as_particle *)p
		};

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
	int64_t sz = map_particle_strip_indexes(p, p_map_flat->data);

	cf_assert(sz >= 0, AS_PARTICLE, "map_to_flat() strip indexes failed with sz %ld", sz);
	p_map_flat->sz = (uint32_t)sz;

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
		sz += order_index_size(&ordidx);
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

	if (offset_index_is_null(&map->offidx)) {
		sz = offset_index_size(&map->offidx) +
				order_index_size(&map->value_idx);
	}
	else if (order_index_is_null(&map->value_idx)) {
		sz = order_index_size(&map->value_idx);
	}

	return cdt_vla_sz(sz);
}

static inline void
map_allidx_alloc_temp(const packed_map *map, uint8_t *mem_temp)
{
	offset_index *offidx = (offset_index *)&map->offidx;
	order_index *ordidx = (order_index *)&map->value_idx;

	if (offset_index_is_null(offidx)) {
		uint32_t off_sz = offset_index_size(offidx);
		uint32_t ord_sz = order_index_size(ordidx);

		if (off_sz + ord_sz > CDT_MAX_STACK_OBJ_SZ) {
			offidx->_.ptr = cdt_idx_alloc(off_sz);
			ordidx->_.ptr = cdt_idx_alloc(ord_sz);
		}
		else {
			offidx->_.ptr = mem_temp;
			ordidx->_.ptr = mem_temp + offset_index_size(offidx);
		}

		offset_index_set_filled(offidx, 1);
		order_index_set(ordidx, 0, map->ele_count);
	}
	else if (order_index_is_null(ordidx)) {
		uint32_t ord_sz = order_index_size(ordidx);

		if (ord_sz > CDT_MAX_STACK_OBJ_SZ) {
			ordidx->_.ptr = cdt_idx_alloc(ord_sz);
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

static inline bool
map_fill_offidx(const packed_map *map)
{
	offset_index *offidx = (offset_index *)&map->offidx;
	return map_offset_index_fill(offidx, map->ele_count);
}

static inline bool
skip_map_pair(as_unpacker *pk)
{
	if (as_unpack_size(pk) <= 0) {
		return false;
	}

	if (as_unpack_size(pk) <= 0) {
		return false;
	}

	return true;
}

//------------------------------------------------
// map_packer

static as_particle *
map_packer_create_particle(map_packer *pk, rollback_alloc *alloc_buf)
{
	uint32_t sz = pk->ext_sz + pk->content_sz +
			as_pack_map_header_get_size(pk->ele_count + (pk->flags ? 1 : 0));
	map_mem *p_map_mem = (map_mem *)(alloc_buf
			? rollback_alloc_reserve(alloc_buf, sizeof(map_mem) + sz)
			: cf_malloc(sizeof(map_mem) + sz)); // response, so not cf_malloc_ns()

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = sz;
	pk->write_ptr = p_map_mem->data;

	return (as_particle *)p_map_mem;
}

static void
map_packer_init(map_packer *pk, uint32_t ele_count, uint8_t flags,
		uint32_t content_sz)
{
	pk->ele_count = ele_count;
	pk->content_sz = content_sz;
	pk->ext_content_sz = 0;

	offset_index_init(&pk->offset_idx, NULL, ele_count, NULL, content_sz);

	if (flags & AS_PACKED_MAP_FLAG_OFF_IDX) {
		pk->ext_content_sz += offset_index_size(&pk->offset_idx);
	}

	order_index_init(&pk->value_idx, NULL, ele_count);

	if (flags & AS_PACKED_MAP_FLAG_ORD_IDX) {
		pk->ext_content_sz += order_index_size(&pk->value_idx);
	}

	pk->flags = flags;

	if (flags == AS_PACKED_MAP_FLAG_NONE) {
		pk->ext_header_sz = 0;
		pk->ext_sz = 0;
	}
	else {
		pk->ext_header_sz = as_pack_ext_header_get_size(pk->ext_content_sz);
		pk->ext_sz = pk->ext_header_sz + pk->ext_content_sz + 1; // +1 for packed nil
	}

	pk->write_ptr = NULL;
	pk->contents = NULL;
}

static void
map_packer_setup_bin(map_packer *pk, as_bin *b, rollback_alloc *alloc_buf)
{
	b->particle = map_packer_create_particle(pk, alloc_buf);
}

static void
map_packer_write_hdridx(map_packer *pk)
{
	as_packer write = {
			.buffer = pk->write_ptr,
			.capacity = INT_MAX
	};

	as_pack_map_header(&write, pk->ele_count +
			(pk->flags == AS_PACKED_MAP_FLAG_NONE ? 0 : 1));

	if (pk->flags == AS_PACKED_MAP_FLAG_NONE) {
		pk->write_ptr += write.offset;
		pk->contents = pk->write_ptr;

		return;
	}

	as_pack_ext_header(&write, pk->ext_content_sz, pk->flags);

	if (pk->ext_content_sz > 0) {
		uint8_t *ptr = pk->write_ptr + write.offset;
		uint32_t index_sz_left = pk->ext_content_sz;
		uint32_t sz = offset_index_size(&pk->offset_idx);

		if ((pk->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_sz_left >= sz) {
			offset_index_set_ptr(&pk->offset_idx, ptr,
					ptr + pk->ext_content_sz + 1); // +1 for nil pair
			ptr += sz;
			index_sz_left -= sz;
		}

		sz = order_index_size(&pk->value_idx);

		if ((pk->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_sz_left >= sz) {
			order_index_set_ptr(&pk->value_idx, ptr);
		}
	}

	// Pack nil.
	write.offset += pk->ext_content_sz;
	write.buffer[write.offset++] = msgpack_nil[0];

	pk->write_ptr += write.offset;
	pk->contents = pk->write_ptr;
	pk->offset_idx.contents = pk->contents;
}

static bool
map_packer_fill_offset_index(map_packer *mpk)
{
	if (offset_index_is_null(&mpk->offset_idx)) {
		return true;
	}

	offset_index_set_filled(&mpk->offset_idx, 1);

	return map_offset_index_fill(&mpk->offset_idx, mpk->ele_count);
}

// qsort_r callback function.
static int
map_packer_fill_index_sort_compare(const void *x, const void *y, void *p)
{
	index_sort_userdata *udata = (index_sort_userdata *)p;

	if (udata->error) {
		return 0;
	}

	order_index *ordidx = udata->order;
	uint32_t x_idx = order_index_ptr2value(ordidx, x);
	uint32_t y_idx = order_index_ptr2value(ordidx, y);
	const offset_index *offidx = udata->offsets;
	const uint8_t *contents = udata->contents;
	uint32_t content_sz = udata->content_sz;
	uint32_t x_off = offset_index_get_const(offidx, x_idx);
	uint32_t y_off = offset_index_get_const(offidx, y_idx);

	as_unpacker x_pk = {
			.buffer = contents,
			.offset = x_off,
			.length = content_sz
	};

	as_unpacker y_pk = {
			.buffer = contents,
			.offset = y_off,
			.length = content_sz
	};

	if (udata->sort_by == SORT_BY_VALUE) {
		// Skip keys.
		if (as_unpack_size(&x_pk) <= 0) {
			udata->error = true;
			return 0;
		}

		if (as_unpack_size(&y_pk) <= 0) {
			udata->error = true;
			return 0;
		}
	}

	msgpack_compare_t cmp = as_unpack_compare(&x_pk, &y_pk);

	if (cmp == MSGPACK_COMPARE_EQUAL) {
		if (udata->sort_by == SORT_BY_KEY) {
			if ((cmp = as_unpack_compare(&x_pk, &y_pk)) ==
					MSGPACK_COMPARE_EQUAL) {
				return 0;
			}
		}
		else {
			return 0;
		}
	}

	if (cmp == MSGPACK_COMPARE_LESS) {
		return -1;
	}

	if (cmp == MSGPACK_COMPARE_GREATER) {
		return 1;
	}

	udata->error = true;

	return 0;
}

static bool
map_packer_fill_ordidx(map_packer *mpk, const uint8_t *contents,
		uint32_t content_sz)
{
	if (order_index_is_null(&mpk->value_idx)) {
		return true;
	}

	return order_index_set_sorted(&mpk->value_idx, &mpk->offset_idx, contents,
			content_sz, SORT_BY_VALUE);
}

static bool
map_packer_add_op_copy_index(map_packer *mpk, const packed_map_op *add_op,
		map_ele_find *remove_info, const map_ele_find *add_info, uint32_t kv_sz)
{
	// No elements left.
	if (add_op->new_ele_count == 0) {
		return true;
	}

	if (offset_index_is_valid(&mpk->offset_idx)) {
		if (! packed_map_op_write_new_offidx(add_op, remove_info, add_info,
				&mpk->offset_idx, kv_sz) &&
				! map_packer_fill_offset_index(mpk)) {
			return false;
		}
	}

	if (order_index_is_valid(&mpk->value_idx)) {
		if (remove_info->found_key &&
				order_index_is_filled(&add_op->map->value_idx)) {
			if (! packed_map_find_rank_indexed(add_op->map, remove_info)) {
				cf_warning(AS_PARTICLE, "map_packer_add_op_copy_index() remove_info find rank failed");
				return false;
			}

			if (! remove_info->found_value) {
				cf_warning(AS_PARTICLE, "map_packer_add_op_copy_index() remove_info rank not found: idx=%u found=%d ele_count=%u", remove_info->idx, remove_info->found_key, add_op->map->ele_count);
				return false;
			}
		}

		if (! packed_map_op_write_new_ordidx(
				add_op, remove_info, add_info, &mpk->value_idx) &&
				! map_packer_fill_ordidx(mpk, mpk->contents, mpk->content_sz)) {
			return false;
		}
	}

	return true;
}

static inline void
map_packer_write_seg1(map_packer *pk, const packed_map_op *op)
{
	pk->write_ptr = packed_map_op_write_seg1(op, pk->write_ptr);
}

static inline void
map_packer_write_seg2(map_packer *pk, const packed_map_op *op)
{
	pk->write_ptr = packed_map_op_write_seg2(op, pk->write_ptr);
}

static inline void
map_packer_write_msgpack_seg(map_packer *pk, const cdt_payload *seg)
{
	memcpy(pk->write_ptr, seg->ptr, seg->sz);
	pk->write_ptr += seg->sz;
}

//------------------------------------------------
// map

static int
map_set_flags(as_bin *b, rollback_alloc *alloc_buf, as_bin *result,
		uint8_t set_flags)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, false)) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint8_t map_flags = map.flags;
	uint32_t ele_count = map.ele_count;
	bool reorder = false;

	if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) ==
			AS_PACKED_MAP_FLAG_V_ORDERED) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid flags 0x%x", set_flags);
		return -AS_ERR_PARAMETER;
	}

	if (is_kv_ordered(set_flags)) {
		if (! is_kv_ordered(map_flags)) {
			if (ele_count > 1 && ! is_k_ordered(map_flags)) {
				reorder = true;
			}

			map_flags |= AS_PACKED_MAP_FLAG_KV_ORDERED;
			map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
			map_flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
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
			map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
		}
	}
	else if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) == 0) {
		map_flags &= ~AS_PACKED_MAP_FLAG_KV_ORDERED;
		map_flags &= ~AS_PACKED_MAP_FLAG_OFF_IDX;
		map_flags &= ~AS_PACKED_MAP_FLAG_ORD_IDX;
	}

	define_map_packer(mpk, ele_count, map_flags, map.content_sz);

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);

	if (reorder) {
		vla_map_offidx_if_invalid(u, &map);

		if (! packed_map_write_k_ordered(&map, mpk.write_ptr,
				&mpk.offset_idx)) {
			cf_warning(AS_PARTICLE, "packed_map_set_flags() sort on key failed, set_flags = 0x%x", set_flags);
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		memcpy(mpk.write_ptr, map.contents, map.content_sz);

		if (offset_index_is_valid(&mpk.offset_idx)) {
			if (offset_index_is_full(&map.offidx)) {
				offset_index_copy(&mpk.offset_idx, &map.offidx, 0, 0,
						ele_count, 0);
			}
			else if (! map_packer_fill_offset_index(&mpk)) {
				cf_warning(AS_PARTICLE, "packed_map_set_flags() fill index failed");
				return -AS_ERR_UNKNOWN;
			}
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (order_index_is_filled(&map.value_idx)) {
			order_index_copy(&mpk.value_idx, &map.value_idx, 0, 0, ele_count,
					NULL);
		}
		else {
			map_packer_fill_ordidx(&mpk, mpk.contents, mpk.content_sz);
		}
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(b)) {
		cdt_bin_print(b, "packed_map_set_flags");
	}
#endif

	return AS_OK;
}

static int
map_increment(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key,
		const cdt_payload *delta_value, as_bin *result, bool is_decrement)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	map_ele_find find_key;
	map_ele_find_init(&find_key, &map);

	if (! packed_map_find_key(&map, &find_key, key)) {
		cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	cdt_calc_delta calc_delta;

	if (! cdt_calc_delta_init(&calc_delta, delta_value, is_decrement)) {
		return -AS_ERR_PARAMETER;
	}

	if (find_key.found_key) {
		define_map_unpacker(pk_map_value, &map);

		pk_map_value.offset = find_key.value_offset;

		if (! cdt_calc_delta_add(&calc_delta, &pk_map_value)) {
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		if (! cdt_calc_delta_add(&calc_delta, NULL)) {
			return -AS_ERR_PARAMETER;
		}
	}

	uint8_t value_buf[CDT_MAX_PACKED_INT_SZ];

	cdt_payload value = {
			.ptr = value_buf,
			.sz = 0
	};

	cdt_calc_delta_pack_and_result(&calc_delta, &value, result);

	map_add_control control = {
			.allow_overwrite = true,
			.allow_create = true,
	};

	return map_add(b, alloc_buf, key, &value, NULL, &control);
}

static int
map_add(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key,
		const cdt_payload *value, as_bin *result,
		const map_add_control *control)
{
	if (! cdt_check_storage_list_contents(key->ptr, key->sz, 1) ||
			! cdt_check_storage_list_contents(value->ptr, value->sz, 1)) {
		cf_warning(AS_PARTICLE, "map_add() invalid params");
		return -AS_ERR_PARAMETER;
	}

	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_add() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	map_ele_find find_key_to_remove;
	map_ele_find_init(&find_key_to_remove, &map);

	if (! packed_map_find_key(&map, &find_key_to_remove, key)) {
		cf_warning(AS_PARTICLE, "map_add() find key failed, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (find_key_to_remove.found_key) {
		// ADD for [unique] & [key exist].
		if (! control->allow_overwrite) {
			if (control->no_fail) {
				if (result) {
					as_bin_set_int(result, map.ele_count);
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
				if (result) {
					as_bin_set_int(result, map.ele_count);
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

	define_map_op(op, &map);
	int32_t new_sz = packed_map_op_add(&op, &find_key_to_remove);

	if (new_sz < 0) {
		cf_warning(AS_PARTICLE, "map_add() failed with ret=%d, ele_count=%u", new_sz, map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	uint32_t content_sz = (uint32_t)new_sz + key->sz + value->sz;
	define_map_packer(mpk, op.new_ele_count, map.flags, content_sz);

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);

	map_ele_find find_value_to_add;

	map_ele_find_init(&find_value_to_add, &map);
	find_value_to_add.idx = find_key_to_remove.idx;	// Find closest matching position for multiple same values.

	if (order_index_is_valid(&mpk.value_idx) &&
			order_index_is_filled(&map.value_idx)) {
		if (! packed_map_find_rank_by_value_indexed(&map,
				&find_value_to_add, value)) {
			cf_warning(AS_PARTICLE, "map_add() find_value_to_add rank failed");
			return -AS_ERR_UNKNOWN;
		}
	}

	map_packer_write_seg1(&mpk, &op);
	map_packer_write_msgpack_seg(&mpk, key);
	map_packer_write_msgpack_seg(&mpk, value);
	map_packer_write_seg2(&mpk, &op);

	if (! map_packer_add_op_copy_index(&mpk, &op, &find_key_to_remove,
			&find_value_to_add, key->sz + value->sz)) {
		cf_warning(AS_PARTICLE, "map_add() copy index failed");
		return -AS_ERR_UNKNOWN;
	}

	if (result) {
		as_bin_set_int(result, op.new_ele_count);
	}

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(b)) {
		cdt_bin_print(b, "map_add");
	}
#endif

	return AS_OK;
}

static int
map_add_items_unordered(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const offset_index *val_off,
		order_index *val_ord, as_bin *result, const map_add_control *control)
{
	define_cdt_idx_mask(rm_mask, map->ele_count);
	define_cdt_idx_mask(found_mask, val_ord->max_idx);
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

		order_index_find_rank_by_value(val_ord, &value, val_off, &find);

		if (find.found) {
			cdt_idx_mask_set(found_mask, find.result);

			// ADD for [key exist].
			if (! control->allow_overwrite) {
				if (control->no_fail) {
					if (control->do_partial) {
						continue;
					}

					as_bin_set_int(result, map->ele_count);
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
					as_bin_set_int(result, map->ele_count);
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

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);
	mpk.write_ptr = cdt_idx_mask_write_eles(rm_mask, rm_count, &map->offidx,
			mpk.write_ptr, true);
	mpk.write_ptr = order_index_write_eles(val_ord, val_ord->max_idx, val_off,
			mpk.write_ptr, false);
	as_bin_set_int(result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(b)) {
		cdt_bin_print(b, "map_add_items_unordered");
		offset_index_print(val_off, "val_off");
		order_index_print(val_ord, "val_ord");
		cf_crash(AS_PARTICLE, "ele_count %u dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", map->ele_count, dup_count, dup_sz, new_ele_count, new_content_sz);
	}
#endif

	return AS_OK;
}

static int
map_add_items_ordered(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const offset_index *val_off,
		order_index *val_ord, as_bin *result, const map_add_control *control)
{
	uint32_t dup_count;
	uint32_t dup_sz;

	order_index_sorted_mark_dup_eles(val_ord, val_off, &dup_count, &dup_sz);

	if (map->ele_count == 0) {
		uint32_t new_content_sz = order_index_get_ele_size(val_ord,
				val_ord->max_idx, val_off);
		uint32_t new_ele_count = val_ord->max_idx - dup_count;
		define_map_packer(mpk, new_ele_count, map->flags, new_content_sz);

		map_packer_setup_bin(&mpk, b, alloc_buf);
		map_packer_write_hdridx(&mpk);
		order_index_write_eles(val_ord, val_ord->max_idx, val_off,
				mpk.write_ptr, false);

		if (offset_index_is_valid(&mpk.offset_idx)) {
			offset_index_set_filled(&mpk.offset_idx, 1);

			for (uint32_t i = 0; i < val_ord->max_idx; i++) {
				uint32_t val_idx = order_index_get(val_ord, i);

				if (val_idx == val_ord->max_idx) {
					continue;
				}

				uint32_t sz = offset_index_get_delta_const(val_off, val_idx);

				offset_index_append_size(&mpk.offset_idx, sz);
			}
		}

		if (order_index_is_valid(&mpk.value_idx)) {
			order_index_set(&mpk.value_idx, 0, new_ele_count);
		}

		as_bin_set_int(result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
		if (! map_verify(b)) {
			cdt_bin_print(b, "map_add_items_ordered");
			map_print(map, "original");
			offset_index_print(val_off, "val_off");
			order_index_print(val_ord, "val_ord");
			cf_crash(AS_PARTICLE, "ele_count 0 dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", dup_count, dup_sz, new_ele_count, new_content_sz);
		}
#endif

		return AS_OK;
	}

	define_cdt_idx_mask(rm_mask, map->ele_count);
	uint32_t rm_count = 0;
	uint32_t rm_sz = 0;
	define_order_index2(insert_idx, map->ele_count, val_ord->max_idx);

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

		if (! packed_map_find_key_indexed(map, &find, &value)) {
			cf_warning(AS_PARTICLE, "map_add_items_ordered() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

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

					as_bin_set_int(result, map->ele_count);
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

					as_bin_set_int(result, map->ele_count);
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
	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);
	uint32_t start_off = 0;

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

	if (offset_index_is_valid(&mpk.offset_idx)) {
		uint32_t read_index = 0;
		uint32_t write_index = 1;
		int delta = 0;

		offset_index_set_filled(&mpk.offset_idx, 1);

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

				offset_index_copy(&mpk.offset_idx, &map->offidx, write_index,
						read_index + 1, count, delta);
				write_index += count;
				read_index += count;
				offset_index_set_filled(&mpk.offset_idx, write_index);

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

			offset_index_append_size(&mpk.offset_idx, sz);
			write_index++;
			delta += sz;
		}

		if (read_index + 1 < map->ele_count && write_index < new_ele_count) {
			offset_index_copy(&mpk.offset_idx, &map->offidx, write_index,
					read_index + 1, map->ele_count - read_index - 1, delta);
		}

		offset_index_set_filled(&mpk.offset_idx, map->ele_count);
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		order_index_set(&mpk.value_idx, 0, new_ele_count);
	}

	as_bin_set_int(result, new_ele_count);

#ifdef MAP_DEBUG_VERIFY
	if (! map_verify(b)) {
		cdt_bin_print(b, "map_add_items_ordered");
		map_print(map, "original");
		offset_index_print(val_off, "val_off");
		order_index_print(val_ord, "val_ord");
		cf_crash(AS_PARTICLE, "ele_count %u dup_count %u dup_sz %u new_ele_count %u new_content_sz %u", map->ele_count, dup_count, dup_sz, new_ele_count, new_content_sz);
	}
#endif

	return AS_OK;
}

static int
map_add_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items,
		as_bin *result, const map_add_control *control)
{
	as_unpacker pk = {
			.buffer = items->ptr,
			.length = items->sz
	};

	int64_t items_count = as_unpack_map_header_element_count(&pk);

	if (items_count < 0) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid parameter, expected packed map");
		return -AS_ERR_PARAMETER;
	}

	if (items_count > 0 && as_unpack_peek_is_ext(&pk)) {
		if (! skip_map_pair(&pk)) {
			cf_warning(AS_PARTICLE, "map_add_items() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		items_count--;
	}

	if (! cdt_check_storage_list_contents(pk.buffer + pk.offset,
			pk.length - pk.offset, (uint32_t)items_count * 2)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid parameter");
		return -AS_ERR_PARAMETER;
	}

	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (items_count == 0) {
		as_bin_set_int(result, map.ele_count);
		return AS_OK; // no-op
	}

	vla_map_offidx_if_invalid(u, &map);

	// Pre-fill index.
	if (! map_offset_index_fill(u->offidx, map.ele_count)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	const uint8_t *val_contents = pk.buffer + pk.offset;
	uint32_t val_content_sz = pk.length - pk.offset;
	uint32_t val_count = (uint32_t)items_count;
	define_order_index(val_ord, val_count);
	define_offset_index(val_off, val_contents, val_content_sz, val_count);

	// Sort items to add.
	if (! map_offset_index_fill(&val_off, val_count) ||
			! order_index_set_sorted(&val_ord, &val_off, val_contents,
					val_content_sz, SORT_BY_KEY)) {
		cf_warning(AS_PARTICLE, "map_add_items() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (map_is_k_ordered(&map)) {
		return map_add_items_ordered(&map, b, alloc_buf, &val_off, &val_ord,
				result, control);
	}

	return map_add_items_unordered(&map, b, alloc_buf, &val_off, &val_ord,
			result, control);
}

static int
map_remove_by_key_interval(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *key_start, const cdt_payload *key_end,
		cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_key_interval() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_key_interval(&map, b, alloc_buf, key_start,
			key_end, result);
}

static int
map_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index,
		uint64_t count, cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_index_range(&map, b, alloc_buf, index,
			count, result);
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
map_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value_start, const cdt_payload *value_end,
		cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_value_interval() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_value_interval(&map, b, alloc_buf,
			value_start, value_end, result);
}

static int
map_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf,
		int64_t rank, uint64_t count, cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rank_range(&map, b, alloc_buf, rank, count,
			result);
}

static int
map_remove_by_rel_index_range(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *key, int64_t index, uint64_t count,
		cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_remove_by_rel_index_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rel_index_range(&map, b, alloc_buf, key,
			index, count, result);
}

static int
map_remove_by_rel_rank_range(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value, int64_t rank, uint64_t count,
		cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_remove_by_rel_rank_range() invalid packed map index, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_by_rel_rank_range(&map, b, alloc_buf, value,
			rank, count, result);
}

static int
map_remove_all_by_key_list(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *key_list, cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_remove_all_by_key_list() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_all_by_key_list(&map, b, alloc_buf, key_list,
			result);
}

static int
map_remove_all_by_value_list(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value_list, cdt_result_data *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, true)) {
		cf_warning(AS_PARTICLE, "map_get_remove_all_value_items() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_map_get_remove_all_by_value_list(&map, b, alloc_buf,
			value_list, result);
}

static int
map_clear(as_bin *b, rollback_alloc *alloc_buf, as_bin *result)
{
	packed_map map;

	if (! packed_map_init_from_bin(&map, b, false)) {
		cf_warning(AS_PARTICLE, "packed_map_clear() invalid packed map, ele_count=%u", map.ele_count);
		return -AS_ERR_PARAMETER;
	}

	define_map_packer(mpk, 0, map.flags, 0);

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);

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

	cf_assert(is_map_type(type), AS_PARTICLE, "as_packed_map_init_from_bin() invalid type %d", type);

	return packed_map_init_from_particle(map, b->particle, fill_idxs);
}

static bool
packed_map_unpack_hdridx(packed_map *map, bool fill_idxs)
{
	as_unpacker pk = {
			.buffer = map->packed,
			.length = map->packed_sz
	};

	if (map->packed_sz == 0) {
		map->flags = 0;
		return false;
	}

	int64_t ele_count = as_unpack_map_header_element_count(&pk);

	if (ele_count < 0) {
		return false;
	}

	map->ele_count = (uint32_t)ele_count;

	if (ele_count != 0 && as_unpack_peek_is_ext(&pk)) {
		as_msgpack_ext ext;

		if (as_unpack_ext(&pk, &ext) != 0) {
			return false;
		}

		if (as_unpack_size(&pk) <= 0) { // skip the packed nil
			return false;
		}

		map->flags = ext.type;
		map->ele_count--;

		map->contents = map->packed + pk.offset;
		map->content_sz = map->packed_sz - pk.offset;
		offset_index_init(&map->offidx, NULL, map->ele_count, map->contents,
				map->content_sz);
		order_index_init(&map->value_idx, NULL, map->ele_count);

		uint32_t index_sz_left = ext.size;
		uint8_t *ptr = (uint8_t *)ext.data;
		uint32_t sz = offset_index_size(&map->offidx);

		if ((map->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_sz_left >= sz) {
			offset_index_set_ptr(&map->offidx, ptr, map->packed + pk.offset);
			ptr += sz;
			index_sz_left -= sz;

			if (fill_idxs) {
				map_fill_offidx(map);
			}
		}

		sz = order_index_size(&map->value_idx);

		if ((map->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_sz_left >= sz) {
			order_index_set_ptr(&map->value_idx, ptr);
		}
	}
	else {
		map->contents = map->packed + pk.offset;
		map->content_sz = map->packed_sz - pk.offset;

		offset_index_init(&map->offidx, NULL, ele_count, map->contents,
				map->content_sz);
		order_index_init(&map->value_idx, NULL, ele_count);
		map->flags = AS_PACKED_MAP_FLAG_NONE;
	}

	return true;
}

static void
packed_map_init_indexes(const packed_map *map, as_packer *pk)
{
	uint8_t *ptr = pk->buffer + pk->offset;

	if (map_is_k_ordered(map)) {
		offset_index offidx;

		offset_index_init(&offidx, ptr, map->ele_count, map->contents,
				map->content_sz);

		uint32_t offidx_sz = offset_index_size(&offidx);

		ptr += offidx_sz;
		offset_index_set_filled(&offidx, 1);
		pk->offset += offidx_sz;
	}

	if (map_is_kv_ordered(map)) {
		order_index ordidx;

		order_index_init(&ordidx, ptr, map->ele_count);
		order_index_set(&ordidx, 0, map->ele_count);
		pk->offset += order_index_size(&ordidx);
	}
}

static bool
packed_map_ensure_ordidx_filled(const packed_map *op)
{
	order_index *ordidx = (order_index *)&op->value_idx;

	if (! order_index_is_filled(ordidx)) {
		if (! map_fill_offidx(op)) {
			cf_warning(AS_PARTICLE, "packed_map_ensure_ordidx_filled() failed to fill offset_idx");
			return false;
		}

		return order_index_set_sorted(ordidx, &op->offidx,
				op->contents, op->content_sz, SORT_BY_VALUE);
	}

	return true;
}

static uint32_t
packed_map_find_index_by_idx_unordered(const packed_map *map, uint32_t idx)
{
	uint32_t pk_offset = offset_index_get_const(&map->offidx, idx);

	cdt_payload key = {
			.ptr = map->contents + pk_offset,
			.sz = map->content_sz - pk_offset
	};

	return packed_map_find_index_by_key_unordered(map, &key);
}

static uint32_t
packed_map_find_index_by_key_unordered(const packed_map *map,
		const cdt_payload *key)
{
	as_unpacker pk_key = {
			.buffer = key->ptr,
			.length = key->sz
	};

	uint32_t index = 0;
	define_map_unpacker(pk, map);

	for (uint32_t i = 0; i < map->ele_count; i++) {
		pk_key.offset = 0;
		msgpack_compare_t cmp = as_unpack_compare(&pk, &pk_key);

		if (cmp == MSGPACK_COMPARE_ERROR) {
			return map->ele_count;
		}

		if (cmp == MSGPACK_COMPARE_LESS) {
			index++;
		}

		if (as_unpack_size(&pk) <= 0) {
			return map->ele_count;
		}
	}

	return index;
}

static void
packed_map_find_rank_indexed_linear(const packed_map *map, map_ele_find *find,
		uint32_t start, uint32_t len)
{
	uint32_t rank = order_index_find_idx(&map->value_idx, find->idx, start,
			len);

	if (rank < start + len) {
		find->found_value = true;
		find->rank = rank;
	}
}

// Find rank given index (find->idx).
// Return true on success.
static bool
packed_map_find_rank_indexed(const packed_map *map, map_ele_find *find)
{
	uint32_t ele_count = map->ele_count;

	if (ele_count == 0) {
		return true;
	}

	if (find->idx >= ele_count) {
		find->found_value = false;
		return true;
	}

	const offset_index *offset_idx = &map->offidx;
	const order_index *value_idx = &map->value_idx;

	uint32_t rank = ele_count / 2;
	uint32_t upper = ele_count;
	uint32_t lower = 0;

	as_unpacker pk_value = {
			.buffer = map->contents + find->value_offset,
			.length = find->key_offset + find->sz - find->value_offset
	};

	find->found_value = false;

	while (true) {
		if (upper - lower < LINEAR_FIND_RANK_MAX_COUNT) {
			packed_map_find_rank_indexed_linear(map, find, lower,
					upper - lower);
			return true;
		}

		uint32_t idx = order_index_get(value_idx, rank);

		if (find->idx == idx) {
			find->found_value = true;
			find->rank = rank;
			break;
		}

		as_unpacker pk_buf = {
				.buffer = map->contents,
				.offset = offset_index_get_const(offset_idx, idx),
				.length = map->content_sz
		};

		if (as_unpack_size(&pk_buf) <= 0) { // skip key
			cf_warning(AS_PARTICLE, "packed_map_find_rank_indexed() unpack key failed at rank=%u", rank);
			return false;
		}

		pk_value.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk_buf);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			if (find->idx < idx) {
				cmp = MSGPACK_COMPARE_LESS;
			}
			else if (find->idx > idx) {
				cmp = MSGPACK_COMPARE_GREATER;
			}

			find->found_value = true;
		}

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			find->rank = rank;
			break;
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (rank >= upper - 1) {
				find->rank = rank + 1;
				break;
			}

			lower = rank + 1;
			rank += upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (rank == lower) {
				find->rank = rank;
				break;
			}

			upper = rank;
			rank += lower;
			rank /= 2;
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_find_rank_indexed() error=%d lower=%u rank=%u upper=%u", (int)cmp, lower, rank, upper);
			return false;
		}
	}

	return true;
}

// Find (closest) rank given value.
// Find closest rank for find->idx (0 means first instance of value).
// FIXME - this is mechanically different from order_index_find_rank_by_value()
//  where target = ele_count finds the largest rank; here it finds the largest
//  rank + 1 in the case that the value exist; fix to conform.
// Return true on success.
static bool
packed_map_find_rank_by_value_indexed(const packed_map *map, map_ele_find *find,
		const cdt_payload *value)
{
	const offset_index *offset_idx = &map->offidx;
	const order_index *value_idx = &map->value_idx;

	find->found_value = false;

	if (map->ele_count == 0) {
		return true;
	}

	uint32_t rank = map->ele_count / 2;

	as_unpacker pk_value = {
			.buffer = value->ptr,
			.length = value->sz
	};

	while (true) {
		uint32_t idx = order_index_get(value_idx, rank);
		uint32_t pk_offset = offset_index_get_const(offset_idx, idx);

		as_unpacker pk_buf = {
				.buffer = map->contents + pk_offset,
				.length = map->content_sz - pk_offset
		};

		if (as_unpack_size(&pk_buf) <= 0) { // skip key
			return false;
		}

		pk_value.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk_buf);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			if (find->idx < idx) {
				cmp = MSGPACK_COMPARE_LESS;
			}
			else if (find->idx > idx) {
				cmp = MSGPACK_COMPARE_GREATER;
			}

			find->found_value = true;
		}

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			find->found_value = true;
			find->rank = rank;
			break;
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (rank >= find->upper - 1) {
				find->rank = rank + 1;
				break;
			}

			find->lower = rank + 1;
			rank += find->upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (rank == find->lower) {
				find->rank = rank;
				break;
			}

			find->upper = rank;
			rank += find->lower;
			rank /= 2;
		}
		else {
			return false;
		}
	}

	return true;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static bool
packed_map_find_rank_range_by_value_interval_indexed(const packed_map *map,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank, uint32_t *count, bool is_multi)
{
	cf_assert(map_has_offidx(map), AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_indexed() offset_index needs to be valid");

	map_ele_find find_start;

	map_ele_find_init(&find_start, map);
	find_start.idx = 0; // find least ranked entry with value == value_start

	if (! packed_map_find_rank_by_value_indexed(map, &find_start,
			value_start)) {
		cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_indexed() invalid packed map");
		return false;
	}

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

			if (! packed_map_find_rank_by_value_indexed(map, &find_end,
					value_end)) {
				cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_indexed() invalid packed map");
				return false;
			}

			*count = (find_end.rank > find_start.rank) ?
					find_end.rank - find_start.rank : 0;
		}
		else {
			if (! find_start.found_value) {
				*count = 0;
			}
			else if (is_multi) {
				find_end.idx = map->ele_count; // find highest ranked entry with value == value_start

				if (! packed_map_find_rank_by_value_indexed(map, &find_end,
						value_start)) {
					cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_indexed() invalid packed map");
					return false;
				}

				*count = find_end.rank - find_start.rank;
			}
		}
	}

	return true;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static bool
packed_map_find_rank_range_by_value_interval_unordered(const packed_map *map,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank, uint32_t *count, uint64_t *mask)
{
	cf_assert(map_has_offidx(map), AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() offset_index needs to be valid");
	cf_assert(value_end, AS_PARTICLE, "value_end == NULL");

	as_unpacker pk_start = {
			.buffer = value_start->ptr,
			.length = value_start->sz
	};

	as_unpacker pk_end = {
			.buffer = value_end->ptr,
			.length = value_end->sz
	};

	// Pre-check parameters.
	if (as_unpack_size(&pk_start) <= 0) {
		cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid start value");
		return false;
	}

	if (value_end != value_start) {
		// Pre-check parameters.
		if (value_end->ptr && as_unpack_size(&pk_end) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid end value");
			return false;
		}
	}

	*rank = 0;
	*count = 0;

	offset_index *offidx = (offset_index *)&map->offidx;
	define_map_unpacker(pk, map);

	for (uint32_t i = 0; i < map->ele_count; i++) {
		offset_index_set(offidx, i, pk.offset);

		if (as_unpack_size(&pk) <= 0) { // skip key
			cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		uint32_t value_offset = pk.offset; // save for pk_end

		pk_start.offset = 0; // reset

		msgpack_compare_t cmp_start = as_unpack_compare(&pk, &pk_start);

		if (cmp_start == MSGPACK_COMPARE_ERROR) {
			cf_warning(AS_PARTICLE, "packed_map_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_COMPARE_LESS) {
			(*rank)++;
		}
		else if (value_start != value_end) {
			msgpack_compare_t cmp_end = MSGPACK_COMPARE_LESS;

			// NULL value_end means largest possible value.
			if (value_end->ptr) {
				pk.offset = value_offset;
				pk_end.offset = 0;
				cmp_end = as_unpack_compare(&pk, &pk_end);
			}

			if (cmp_end == MSGPACK_COMPARE_LESS) {
				if (mask) {
					cdt_idx_mask_set(mask, i);
				}

				(*count)++;
			}
		}
		// Single value case.
		else if (cmp_start == MSGPACK_COMPARE_EQUAL) {
			if (mask) {
				cdt_idx_mask_set(mask, i);
			}

			(*count)++;
		}
	}

	offset_index_set_filled(offidx, map->ele_count);

	return true;
}

// Find key given list index.
// Return true on success.
static bool
packed_map_find_key_indexed(const packed_map *map, map_ele_find *find,
		const cdt_payload *key)
{
	const offset_index *offidx = &map->offidx;
	uint32_t ele_count = map->ele_count;

	find->lower = 0;
	find->upper = ele_count;

	uint32_t idx = (find->lower + find->upper) / 2;

	as_unpacker pk_key = {
			.buffer = key->ptr,
			.length = key->sz
	};

	find->found_key = false;

	if (ele_count == 0) {
		find->idx = 0;
		return true;
	}

	while (true) {
		uint32_t offset = offset_index_get_const(offidx, idx);
		uint32_t content_sz = map->content_sz;
		uint32_t sz = content_sz - offset;

		as_unpacker pk_buf = {
				.buffer = map->contents + offset,
				.length = sz
		};

		pk_key.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk_buf);
		uint32_t key_sz = pk_buf.offset;

		if (cmp == MSGPACK_COMPARE_EQUAL) {
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

		if (cmp == MSGPACK_COMPARE_GREATER) {
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

					as_unpacker pk = {
							.buffer = map->contents + offset,
							.length = tail
					};

					if (as_unpack_size(&pk) <= 0) {
						cf_warning(AS_PARTICLE, "packed_map_find_key_indexed() invalid packed map");
						return false;
					}

					find->key_offset = offset;
					find->value_offset = offset + pk.offset;
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
		else if (cmp == MSGPACK_COMPARE_LESS) {
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
			cf_warning(AS_PARTICLE, "packed_map_find_key_indexed() compare error=%d", (int)cmp);
			return false;
		}
	}

	return true;
}

static bool
packed_map_find_key(const packed_map *map, map_ele_find *find,
		const cdt_payload *key)
{
	uint32_t ele_count = map->ele_count;
	offset_index *offidx = (offset_index *)&map->offidx;

	if (ele_count == 0) {
		return true;
	}

	if (map_is_k_ordered(map) && offset_index_is_full(offidx)) {
		if (! packed_map_find_key_indexed(map, find, key)) {
			cf_warning(AS_PARTICLE, "packed_map_find_key() packed_map_op_find_key_indexed failed");
			return false;
		}

		return true;
	}

	as_unpacker pk_key = {
			.buffer = key->ptr,
			.length = key->sz
	};

	find->found_key = false;

	define_map_unpacker(pk, map);
	uint32_t content_sz = pk.length;

	if (! offset_index_is_valid(offidx)) {
		offidx = NULL;
	}

	if (map_is_k_ordered(map)) {
		// Ordered compare.

		// Allows for continuation from last search.
		if (find->lower > 0) {
			pk.offset = find->key_offset;
		}

		for (uint32_t i = find->lower; i < find->upper; i++) {
			uint32_t key_offset = pk.offset;
			uint32_t sz;

			pk_key.offset = 0; // reset

			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			find->value_offset = pk.offset;

			if (offidx) {
				int64_t ret = map_offset_index_get_delta(offidx, i);

				if (ret < 0) {
					return false;
				}

				pk.offset = (uint32_t)map_offset_index_get(offidx, i + 1);
				sz = (uint32_t)ret;
			}
			else {
				// Skip value.
				if (as_unpack_size(&pk) <= 0) {
					return false;
				}

				sz = pk.offset - key_offset;
			}

			if (cmp != MSGPACK_COMPARE_GREATER) {
				if (cmp == MSGPACK_COMPARE_EQUAL) {
					find->found_key = true;
				}

				find->idx = i;
				find->key_offset = key_offset;
				find->sz = sz;

				return true;
			}
		}

		if (find->upper == ele_count) {
			find->key_offset = content_sz;
			find->value_offset = content_sz;
			find->sz = 0;
		}
		else {
			if (offidx && ! offset_index_set_next(offidx, find->upper,
					pk.offset)) {
				cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%u offidx_offset=%u", find->upper, pk.offset, offset_index_get_const(offidx, find->upper));
			}

			find->key_offset = pk.offset;

			// Skip key.
			if (as_unpack_size(&pk) <= 0) {
				return false;
			}

			find->value_offset = pk.offset;

			// Skip value.
			if (as_unpack_size(&pk) <= 0) {
				return false;
			}

			find->sz = pk.offset - find->key_offset;
		}

		find->idx = find->upper;
	}
	else {
		// Unordered compare.
		// Assumes same keys are clustered.
		for (uint32_t i = 0; i < ele_count; i++) {
			uint32_t offset = pk.offset;

			pk_key.offset = 0; // reset

			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			uint32_t value_offset = pk.offset;

			if (cmp == MSGPACK_COMPARE_EQUAL) {
				// Skip value.
				if (as_unpack_size(&pk) <= 0) {
					return false;
				}

				if (! find->found_key) {
					find->found_key = true;
					find->idx = i;
					find->key_offset = offset;
					find->value_offset = value_offset;
					find->sz = pk.offset - offset;
				}

				if (offidx && ! offset_index_set_next(offidx, i + 1,
						pk.offset)) {
					cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%u offidx_offset=%u", i + 1, pk.offset, offset_index_get_const(offidx, i + 1));
				}

				return true;
			}
			else if (find->found_key) {
				return true;
			}
			else if (as_unpack_size(&pk) <= 0) { // skip value
				return false;
			}

			if (offidx && ! offset_index_set_next(offidx, i + 1, pk.offset)) {
				cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%u offidx_offset=%u", i + 1, pk.offset, offset_index_get_const(offidx, i + 1));
			}
		}

		find->key_offset = content_sz;
		find->value_offset = content_sz;
		find->sz = 0;
		find->idx = ele_count;
	}

	return true;
}

static int
packed_map_get_remove_by_key_interval(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *key_start,
		const cdt_payload *key_end, cdt_result_data *result)
{
	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	vla_map_offidx_if_invalid(u, map);
	uint32_t index = 0;
	uint32_t count = 0;

	if (map_is_k_ordered(map)) {
		if (! packed_map_get_range_by_key_interval_ordered(map, key_start,
				key_end, &index, &count)) {
			return -AS_ERR_PARAMETER;
		}

		if (count == 0 && ! result->is_multi) {
			if (! result_data_set_key_not_found(result, -1)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid result_type %d", result->type);
				return -AS_ERR_PARAMETER;
			}

			return AS_OK;
		}

		return packed_map_get_remove_by_index_range(map, b, alloc_buf, index,
				count, result);
	}

	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_PARAMETER;
	}

	if (key_start == key_end) {
		map_ele_find find_key;
		map_ele_find_init(&find_key, map);

		if (! packed_map_find_key(map, &find_key, key_start)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() find key failed, ele_count=%u", map->ele_count);
			return -AS_ERR_PARAMETER;
		}

		if (! find_key.found_key) {
			if (! result_data_set_key_not_found(result, -1)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() invalid result_type %d", result->type);
				return -AS_ERR_PARAMETER;
			}

			return AS_OK;
		}

		if (b) {
			define_map_op(op, map);
			int32_t new_sz = packed_map_op_remove(&op, &find_key, 1,
					find_key.sz);

			if (new_sz < 0) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_key_interval() packed_map_transform_remove_key failed with ret=%d, ele_count=%u", new_sz, map->ele_count);
				return -AS_ERR_PARAMETER;
			}

			define_map_packer(mpk, op.new_ele_count, map->flags,
					(uint32_t)new_sz);

			map_packer_setup_bin(&mpk, b, alloc_buf);
			map_packer_write_hdridx(&mpk);
			map_packer_write_seg1(&mpk, &op);
			map_packer_write_seg2(&mpk, &op);
		}

#ifdef MAP_DEBUG_VERIFY
		if (b && ! map_verify(b)) {
			cdt_bin_print(b, "packed_map_get_remove_by_key_interval");
			map_print(map, "original");
			cf_crash(AS_PARTICLE, "ele_count %u index %u count 1 is_multi %d inverted %d", map->ele_count, index, result->is_multi, inverted);
		}
#endif

		return packed_map_build_result_by_key(map, key_start, find_key.idx,
				1, result);
	}

	define_cdt_idx_mask(rm_mask, map->ele_count);

	if (! packed_map_get_range_by_key_interval_unordered(map, key_start,
			key_end, &index, &count, rm_mask)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_count = count;

	if (inverted) {
		rm_count = map->ele_count - count;
		cdt_idx_mask_invert(rm_mask, map->ele_count);
	}

	int ret = AS_OK;
	uint32_t rm_sz = 0;

	if (b) {
		if ((ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
				rm_count, &rm_sz)) != AS_OK) {
			return ret;
		}
	}

	if (result_data_is_return_elements(result)) {
		if (! packed_map_build_ele_result_by_mask(map, rm_mask, rm_count, rm_sz,
				result)) {
			return -AS_ERR_UNKNOWN;
		}
	}
	else if (result_data_is_return_rank(result)) {
		ret = packed_map_build_rank_result_by_mask(map, rm_mask, rm_count,
				result);
	}
	else {
		ret = result_data_set_range(result, index, count, map->ele_count);
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_by_key_interval");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u index %u count %u rm_count %u inverted %d", map->ele_count, index, count, rm_count, inverted);
	}
#endif

	return AS_OK;
}

static int
packed_map_trim_ordered(const packed_map *map, as_bin *b, rollback_alloc *alloc_buf,
		uint32_t index, uint32_t count, cdt_result_data *result)
{
	cf_assert(result->is_multi, AS_PARTICLE, "packed_map_trim_ordered() required to be a multi op");
	cf_assert(! result_data_is_inverted(result), AS_PARTICLE, "packed_map_trim_ordered() INVERTED flag not supported");

	vla_map_offidx_if_invalid(u, map);
	uint32_t rm_count = map->ele_count - count;
	uint32_t index1 = index + count;

	// Pre-fill index.
	if (! map_offset_index_fill(u->offidx, index + count)) {
		cf_warning(AS_PARTICLE, "packed_map_trim_ordered() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint32_t offset0 = offset_index_get_const(u->offidx, index);
	uint32_t offset1 = offset_index_get_const(u->offidx, index1);
	uint32_t content_sz = offset1 - offset0;

	if (b) {
		define_map_packer(mpk, count, map->flags, content_sz);

		map_packer_setup_bin(&mpk, b, alloc_buf);
		map_packer_write_hdridx(&mpk);
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

		return packed_map_build_rank_result_by_index_range(map, index, count,
				result);
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
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

// Set b = NULL for get_by_index_range operation.
static int
packed_map_get_remove_by_index_range(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, int64_t index, uint64_t count,
		cdt_result_data *result)
{
	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, map->ele_count, &uindex, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() index %ld out of bounds for ele_count %u", index, map->ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_PARAMETER;
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
			return packed_map_trim_ordered(map, b, alloc_buf, uindex, count32,
					result);
		}
		else {
			result->flags |= AS_CDT_OP_FLAG_INVERTED;
		}
	}

	if (count32 == 0) {
		if (! result_data_set_key_not_found(result, uindex)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid result type %d", result->type);
			return -AS_ERR_PARAMETER;
		}

		return AS_OK;
	}

	vla_map_offidx_if_invalid(u, map);

	if (count32 == map->ele_count) {
		return packed_map_get_remove_all(map, b, alloc_buf, result);
	}

	int ret = AS_OK;

	if (map_is_k_ordered(map)) {
		// Pre-fill index.
		if (! map_offset_index_fill(u->offidx, uindex + count32)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		if (b) {
			ret = packed_map_remove_idx_range(map, b, alloc_buf, uindex,
					count32);

			if (ret != AS_OK) {
				return ret;
			}
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_build_ele_result_by_idx_range(map, uindex, count32,
					result)) {
				return -AS_ERR_UNKNOWN;
			}
		}
		else if (result_data_is_return_rank(result)) {
			ret = packed_map_build_rank_result_by_index_range(map, uindex,
					count32, result);
		}
		else {
			ret = result_data_set_range(result, uindex, count32,
					map->ele_count);
		}
	}
	else {
		// Pre-fill index.
		if (! map_fill_offidx(map)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		define_build_order_heap_by_range(heap, uindex, count32, map->ele_count,
				map, packed_map_compare_key_by_idx, success);

		if (! success) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		uint32_t rm_sz = 0;
		bool inverted = result_data_is_inverted(result);
		define_cdt_idx_mask(rm_mask, map->ele_count);
		uint32_t rm_count = (inverted ? map->ele_count - count32 : count32);

		cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
				inverted);

		if (b) {
			int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
					rm_count, &rm_sz);

			if (ret != AS_OK) {
				return ret;
			}
		}

		switch (result->type) {
		case RESULT_TYPE_RANK:
		case RESULT_TYPE_REVRANK:
			if (inverted) {
				ret = packed_map_build_rank_result_by_mask(map, rm_mask,
						rm_count, result);
			}
			else {
				if (heap.cmp == MSGPACK_COMPARE_LESS) {
					order_heap_reverse_end(&heap, count32);
				}

				ret = packed_map_build_rank_result_by_ele_idx(map, &heap._,
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
				if (heap.cmp == MSGPACK_COMPARE_LESS) {
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
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_by_index_range");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u uindex %u count32 %u", map->ele_count, uindex, count32);
	}
#endif

	return AS_OK;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_map_get_remove_by_value_interval(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value_start,
		const cdt_payload *value_end, cdt_result_data *result)
{
	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_PARAMETER;
	}

	if (map->ele_count == 0) {
		if (! result_data_set_value_not_found(result, -1)) {
			return -AS_ERR_PARAMETER;
		}

		return AS_OK;
	}

	vla_map_offidx_if_invalid(u, map);

	// Pre-fill index.
	if (! map_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rank = 0;
	uint32_t count = 0;
	int ret = AS_OK;

	if (order_index_is_valid(&map->value_idx)) {
		if (! packed_map_ensure_ordidx_filled(map)) {
			return -AS_ERR_PARAMETER;
		}

		if (! packed_map_find_rank_range_by_value_interval_indexed(map,
				value_start, value_end, &rank, &count, result->is_multi)) {
			return -AS_ERR_PARAMETER;
		}

		uint32_t rm_count = (inverted ? map->ele_count - count : count);
		bool need_mask = b || (inverted &&
				(result_data_is_return_elements(result) ||
						result_data_is_return_index(result)));
		cond_define_cdt_idx_mask(rm_mask, map->ele_count, need_mask);
		uint32_t rm_sz = 0;

		if (need_mask) {
			cdt_idx_mask_set_by_ordidx(rm_mask, &map->value_idx, rank, count,
					inverted);
		}

		if (b) {
			int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
					rm_count, &rm_sz);

			if (ret != AS_OK) {
				return ret;
			}
		}

		if (result_data_is_return_elements(result)) {
			if (inverted) {
				if (! packed_map_build_ele_result_by_mask(map, rm_mask,
						rm_count, rm_sz, result)) {
					cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
					return -AS_ERR_PARAMETER;
				}
			}
			else if (! packed_map_build_ele_result_by_ele_idx(map,
					&map->value_idx, rank, count, rm_sz, result)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
				return -AS_ERR_PARAMETER;
			}
		}
		else if (result_data_is_return_index(result)) {
			if (inverted) {
				ret = packed_map_build_index_result_by_mask(map, rm_mask,
						rm_count, result);
			}
			else {
				ret = packed_map_build_index_result_by_ele_idx(map,
						&map->value_idx, rank, count, result);
			}
		}
		else {
			ret = result_data_set_range(result, rank, count, map->ele_count);
		}
	}
	else {
		define_cdt_idx_mask(rm_mask, map->ele_count);

		if (! packed_map_find_rank_range_by_value_interval_unordered(map,
				value_start, value_end, &rank, &count, rm_mask)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_value_interval() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		if (count == 0) {
			if (inverted) {
				result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

				return packed_map_get_remove_all(map, b, alloc_buf, result);
			}
			else if (! result_data_set_value_not_found(result, rank)) {
				return -AS_ERR_PARAMETER;
			}
		}
		else {
			if (! result->is_multi) {
				count = 1;
			}

			uint32_t rm_sz = 0;
			uint32_t rm_count = count;

			if (inverted) {
				cdt_idx_mask_invert(rm_mask, map->ele_count);
				rm_count = map->ele_count - count;
			}

			if (b) {
				ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
						rm_count, &rm_sz);
			}

			if (result_data_is_return_elements(result)) {
				if (! packed_map_build_ele_result_by_mask(map, rm_mask,
						rm_count, rm_sz, result)) {
					return -AS_ERR_UNKNOWN;
				}
			}
			else if (result_data_is_return_index(result)) {
				ret = packed_map_build_index_result_by_mask(map, rm_mask,
						rm_count, result);
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
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_by_value_interval");
		map_print(map, "original");
		cf_crash(AS_PARTICLE, "ele_count %u rank %u count %u inverted %d", map->ele_count, rank, count, inverted);
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_by_rank_range(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, int64_t rank, uint64_t count,
		cdt_result_data *result)
{
	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, map->ele_count, &urank, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() rank %ld out of bounds for ele_count %u", rank, map->ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_index_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_PARAMETER;
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
			return -AS_ERR_PARAMETER;
		}

		return AS_OK;
	}

	vla_map_offidx_if_invalid(u, map);

	if (! map_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);
	uint32_t rm_count = inverted ? map->ele_count - count32 : count32;
	const order_index *ordidx = &map->value_idx;
	define_cdt_idx_mask(rm_mask, map->ele_count);
	order_index ret_idxs;

	if (order_index_is_valid(ordidx)) {
		if (! packed_map_ensure_ordidx_filled(map)) {
			return -AS_ERR_PARAMETER;
		}

		cdt_idx_mask_set_by_ordidx(rm_mask, ordidx, urank, count32, inverted);
		order_index_init_ref(&ret_idxs, ordidx, urank, count32);
	}
	else {
		define_build_order_heap_by_range(heap, urank, count32, map->ele_count,
						map, packed_map_compare_value_by_idx, success);

		if (! success) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
				inverted);

		if (! inverted) {
			if (heap.cmp == MSGPACK_COMPARE_LESS) {
				// Reorder results from lowest to highest order.
				order_heap_reverse_end(&heap, count32);
			}

			if (result_data_is_return_index(result)) {
				int ret = packed_map_build_index_result_by_ele_idx(map,
						&heap._, heap.filled, count32, result);

				if (ret != AS_OK) {
					cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rank_range() build index result failed");
					return ret;
				}
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

	if (b) {
		ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask, rm_count,
				&rm_sz);

		if (ret != AS_OK) {
			return ret;
		}
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
			ret = packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
					result);
		}
		else if (! as_bin_inuse(result->result)) {
			ret = packed_map_build_index_result_by_ele_idx(map, &ret_idxs,
					0, rm_count, result);
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
		else if (! as_bin_inuse(result->result) &&
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
		return -AS_ERR_PARAMETER;
	}

	if (ret != AS_OK) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_by_rank_range");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_key_list(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *key_list,
		cdt_result_data *result)
{
	as_unpacker items_pk;
	uint32_t items_count;

	if (! list_param_parse(key_list, &items_pk, &items_count)) {
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
			return -AS_ERR_PARAMETER;
		default:
			break;
		}

		if (! inverted) {
			result_data_set_key_not_found(result, 0);

			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_map_get_remove_all(map, b, alloc_buf, result);
	}

	vla_map_offidx_if_invalid(u, map);

	if (map_is_k_ordered(map)) {
		return packed_map_get_remove_all_by_key_list_ordered(map, b, alloc_buf,
				&items_pk, items_count, result);
	}

	return packed_map_get_remove_all_by_key_list_unordered(map, b, alloc_buf,
			&items_pk, items_count, result);
}

static int
packed_map_get_remove_all_by_key_list_ordered(const packed_map *map,
		as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk,
		uint32_t items_count, cdt_result_data *result)
{
	define_order_index2(rm_ic, map->ele_count, 2 * items_count);

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload key = {
				.ptr = items_pk->buffer + items_pk->offset,
				.sz = items_pk->offset
		};

		if (as_unpack_size(items_pk) <= 0) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		key.sz = items_pk->offset - key.sz;

		map_ele_find find_key;
		map_ele_find_init(&find_key, map);

		if (! packed_map_find_key(map, &find_key, &key)) {
			if (cdt_payload_is_int(&key)) {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() find key=%ld failed, ele_count=%u", cdt_payload_get_int64(&key), map->ele_count);
			}
			else {
				cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_ordered() find key failed, ele_count=%u", map->ele_count);
			}

			return -AS_ERR_PARAMETER;
		}

		uint32_t count = find_key.found_key ? 1 : 0;

		order_index_set(&rm_ic, 2 * i, find_key.idx);
		order_index_set(&rm_ic, (2 * i) + 1, count);
	}

	bool inverted = result_data_is_inverted(result);
	bool need_mask = b || result_data_is_return_elements(result) ||
			result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE);
	cond_define_cdt_idx_mask(rm_mask, map->ele_count, need_mask);
	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_ic, NULL, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, map->ele_count);
	}

	if (b) {
		int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
				rm_count, &rm_sz);

		if (ret != AS_OK) {
			return ret;
		}
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
			result_data_set_by_irc(result, &rm_ic, NULL, items_count);
		}
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
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
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_all_by_key_list_ordered");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_key_list_unordered(const packed_map *map,
		as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk,
		uint32_t items_count, cdt_result_data *result)
{
	bool inverted = result_data_is_inverted(result);
	bool is_ret_index = result_data_is_return_index(result);
	uint32_t rm_count;
	define_cdt_idx_mask(rm_mask, map->ele_count);
	define_order_index(key_list_ordidx, items_count);
	cond_vla_order_index2(ic, map->ele_count, items_count * 2, is_ret_index);

	if (! offset_index_find_items((offset_index *)&map->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_MAP_KEY, items_pk, &key_list_ordidx,
			inverted, rm_mask, &rm_count, is_ret_index ? &ic.ordidx : NULL)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (b) {
		int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
				rm_count, &rm_sz);

		if (ret < 0) {
			return ret;
		}
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		result_data_set_by_itemlist_irc(result, &key_list_ordidx, &ic.ordidx,
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
		cf_warning(AS_PARTICLE, "packed_map_get_remove_all_by_key_list_unordered() invalid return type %d", result->type);
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_all_by_key_list_unordered");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_value_list(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value_list,
		cdt_result_data *result)
{
	as_unpacker items_pk;
	uint32_t items_count;

	if (! list_param_parse(value_list, &items_pk, &items_count)) {
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
			return -AS_ERR_PARAMETER;
		default:
			break;
		}

		if (! inverted) {
			result_data_set_not_found(result, 0);
			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_map_get_remove_all(map, b, alloc_buf, result);
	}

	vla_map_offidx_if_invalid(u, map);

	if (order_index_is_valid(&map->value_idx)) {
		return packed_map_get_remove_all_by_value_list_ordered(map, b,
				alloc_buf, &items_pk, items_count, result);
	}

	bool is_ret_rank = result_data_is_return_rank(result);
	define_cdt_idx_mask(rm_mask, map->ele_count);
	uint32_t rm_count = 0;
	define_order_index(value_list_ordidx, items_count);
	cond_vla_order_index2(rc, map->ele_count, items_count * 2, is_ret_rank);

	if (! offset_index_find_items(u->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_MAP_VALUE, &items_pk, &value_list_ordidx,
			inverted, rm_mask, &rm_count, is_ret_rank ? &rc.ordidx : NULL)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (b) {
		int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
				rm_count, &rm_sz);

		if (ret < 0) {
			return ret;
		}
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		int ret = packed_map_build_index_result_by_mask(map, rm_mask, rm_count,
				result);

		if (ret != AS_OK) {
			return ret;
		}

		break;
	}
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK: {
		result_data_set_by_itemlist_irc(result, &value_list_ordidx,
				&rc.ordidx, rm_count);
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
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_all_by_value_list");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_all_by_value_list_ordered(const packed_map *map,
		as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk,
		uint32_t items_count, cdt_result_data *result)
{
	define_order_index2(rm_rc, map->ele_count, 2 * items_count);

	if (! packed_map_ensure_ordidx_filled(map)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rc_count = 0;

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload value = {
				.ptr = items_pk->buffer + items_pk->offset,
				.sz = items_pk->offset
		};

		if (as_unpack_size(items_pk) <= 0) {
			cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items_ordered() invalid parameter");
			return -AS_ERR_PARAMETER;
		}

		value.sz = items_pk->offset - value.sz;

		uint32_t rank = 0;
		uint32_t count = 0;

		if (! packed_map_find_rank_range_by_value_interval_indexed(map,
				&value, &value, &rank, &count, result->is_multi)) {
			return -AS_ERR_PARAMETER;
		}

		order_index_set(&rm_rc, 2 * i, rank);
		order_index_set(&rm_rc, (2 * i) + 1, count);
		rc_count += count;
	}

	bool inverted = result_data_is_inverted(result);
	bool need_mask = b || result_data_is_return_elements(result) ||
			result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE);
	cond_define_cdt_idx_mask(rm_mask, map->ele_count, need_mask);
	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_rc, &map->value_idx, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, map->ele_count);
	}

	if (b) {
		int ret = packed_map_remove_by_mask(map, b, alloc_buf, rm_mask,
				rm_count, &rm_sz);

		if (ret != AS_OK) {
			return ret;
		}
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		if (inverted) {
			int ret = packed_map_build_index_result_by_mask(map, rm_mask,
					rm_count, result);

			if (ret != AS_OK) {
				return ret;
			}
		}
		else {
			result_data_set_by_irc(result, &rm_rc, &map->value_idx, rc_count);
		}
		break;
	}
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK:
		if (inverted) {
			int ret = packed_map_build_rank_result_by_mask(map, rm_mask,
					rm_count, result);

			if (ret != AS_OK) {
				return ret;
			}
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
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_remove_all_value_items_ordered");
	}
#endif

	return AS_OK;
}

static int
packed_map_get_remove_by_rel_index_range(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *key, int64_t index,
		uint64_t count, cdt_result_data *result)
{
	uint32_t rel_index;
	vla_map_offidx_if_invalid(u, map);

	if (map_is_k_ordered(map)) {
		uint32_t temp;

		if (! packed_map_get_range_by_key_interval_ordered(map, key, key,
				&rel_index, &temp)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		uint32_t temp;

		if (! packed_map_get_range_by_key_interval_unordered(map, key, key,
				&rel_index, &temp, NULL)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_index_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
	}

	calc_rel_index_count(index, count, rel_index, &index, &count);

	return packed_map_get_remove_by_index_range(map, b, alloc_buf, index, count,
			result);
}

static int
packed_map_get_remove_by_rel_rank_range(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank,
		uint64_t count, cdt_result_data *result)
{
	vla_map_offidx_if_invalid(u, map);
	uint32_t rel_rank;

	// Pre-fill index.
	if (! map_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_rank_range() invalid packed map");
		return -AS_ERR_PARAMETER;
	}

	if (order_index_is_valid(&map->value_idx)) {
		uint32_t temp;

		if (! packed_map_ensure_ordidx_filled(map)) {
			return -AS_ERR_PARAMETER;
		}

		if (! packed_map_find_rank_range_by_value_interval_indexed(map, value,
				value, &rel_rank, &temp, result->is_multi)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_rank_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		uint32_t temp;

		if (! packed_map_find_rank_range_by_value_interval_unordered(map, value,
				value, &rel_rank, &temp, NULL)) {
			cf_warning(AS_PARTICLE, "packed_map_get_remove_by_rel_rank_range() invalid packed map");
			return -AS_ERR_PARAMETER;
		}
	}

	calc_rel_index_count(rank, count, rel_rank, &rank, &count);

	return packed_map_get_remove_by_rank_range(map, b, alloc_buf, rank, count,
			result);
}

static int
packed_map_get_remove_all(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, cdt_result_data *result)
{
	cf_assert(! result_data_is_inverted(result), AS_PARTICLE, "packed_map_get_remove_all() INVERTED flag is invalid here");

	if (b) {
		as_bin_set_empty_packed_map(b, alloc_buf, map->flags);
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
		return -AS_ERR_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! map_verify(b)) {
		cdt_bin_print(b, "packed_map_get_remove_all");
	}
#endif

	return AS_OK;
}

static int
packed_map_remove_by_mask(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, const uint64_t *rm_mask, uint32_t count,
		uint32_t *rm_sz_r)
{
	if (count == 0) {
		return AS_OK;
	}

	const offset_index *offidx = &map->offidx;
	uint32_t rm_sz = cdt_idx_mask_get_content_sz(rm_mask, count, offidx);

	if (rm_sz_r) {
		*rm_sz_r = rm_sz;
	}

	uint32_t new_ele_count = map->ele_count - count;
	uint32_t content_sz = map->content_sz - rm_sz;
	define_map_packer(mpk, new_ele_count, map->flags, content_sz);

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);
	mpk.write_ptr = cdt_idx_mask_write_eles(rm_mask, count, offidx,
			mpk.write_ptr, true);

	if (offset_index_is_valid(&mpk.offset_idx)) {
		if (offset_index_is_full(offidx)) {
			offidx_op off_op;
			offidx_op_init(&off_op, &mpk.offset_idx, offidx);
			uint32_t rm_idx = 0;

			for (uint32_t i = 0; i < count; i++) {
				rm_idx = cdt_idx_mask_find(rm_mask, rm_idx, map->ele_count,
						false);
				offidx_op_remove(&off_op, rm_idx);
				rm_idx++;
			}

			offidx_op_end(&off_op);
		}
		else {
			offset_index_set_filled(&mpk.offset_idx, 1);
			map_offset_index_fill(&mpk.offset_idx, new_ele_count);
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (order_index_is_filled(&map->value_idx)) {
			order_index_op_remove_idx_mask(&mpk.value_idx, &map->value_idx,
					rm_mask, count);
		}
		else if (! order_index_set_sorted(&mpk.value_idx, &mpk.offset_idx,
				mpk.contents, mpk.content_sz, SORT_BY_VALUE)) {
			cf_warning(AS_PARTICLE, "packed_map_remove_indexes() failed to sort new value_idex");
			return -AS_ERR_UNKNOWN;
		}
	}

	return AS_OK;
}

static int
packed_map_remove_idx_range(const packed_map *map, as_bin *b,
		rollback_alloc *alloc_buf, uint32_t idx, uint32_t count)
{
	offset_index *offidx = (offset_index *)&map->offidx;
	uint32_t offset0 = offset_index_get_const(offidx, idx);
	uint32_t idx_end = idx + count;
	uint32_t offset1 = offset_index_get_const(offidx, idx_end);
	uint32_t content_sz = map->content_sz - offset1 + offset0;
	uint32_t new_ele_count = map->ele_count - count;
	define_map_packer(mpk, new_ele_count, map->flags, content_sz);

	map_packer_setup_bin(&mpk, b, alloc_buf);
	map_packer_write_hdridx(&mpk);

	uint32_t tail_sz = map->content_sz - offset1;

	memcpy(mpk.write_ptr, map->contents, offset0);
	mpk.write_ptr += offset0;
	memcpy(mpk.write_ptr, map->contents + offset1, tail_sz);

	if (offset_index_is_valid(&mpk.offset_idx)) {
		if (offset_index_is_full(offidx)) {
			offidx_op offop;

			offidx_op_init(&offop, &mpk.offset_idx, offidx);
			offidx_op_remove_range(&offop, idx, count);
			offidx_op_end(&offop);
		}
		else {
			offset_index_set_filled(&mpk.offset_idx, 1);
			map_offset_index_fill(&mpk.offset_idx, new_ele_count);
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (order_index_is_filled(&map->value_idx)) {
			uint32_t count0 = 0;

			for (uint32_t i = 0; i < map->ele_count; i++) {
				uint32_t idx0 = order_index_get(&map->value_idx, i);

				if (idx0 >= idx && idx0 < idx_end) {
					continue;
				}

				if (idx0 >= idx_end) {
					idx0 -= count;
				}

				order_index_set(&mpk.value_idx, count0++, idx0);
			}
		}
		else if (! order_index_set_sorted(&mpk.value_idx, &mpk.offset_idx,
				mpk.contents, mpk.content_sz, SORT_BY_VALUE)) {
			cf_warning(AS_PARTICLE, "packed_map_remove_idx_range() failed to sort new value_idex");
			return -AS_ERR_UNKNOWN;
		}
	}

	return AS_OK;
}

static bool
packed_map_get_range_by_key_interval_unordered(const packed_map *map,
		const cdt_payload *key_start, const cdt_payload *key_end,
		uint32_t *index, uint32_t *count, uint64_t *mask)
{
	cf_assert(key_end, AS_PARTICLE, "key_end == NULL");

	as_unpacker pk_start = {
			.buffer = key_start->ptr,
			.length = key_start->sz
	};

	as_unpacker pk_end = {
			.buffer = key_end->ptr,
			.length = key_end->sz
	};

	// Pre-check parameters.
	if (as_unpack_size(&pk_start) <= 0) {
		cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid start key");
		return false;
	}

	if (key_end->ptr) {
		// Pre-check parameters.
		if (as_unpack_size(&pk_end) <= 0) {
			cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid end key");
			return false;
		}
	}

	*index = 0;
	*count = 0;

	offset_index *offidx = (offset_index *)&map->offidx;
	define_map_unpacker(pk, map);

	for (uint32_t i = 0; i < map->ele_count; i++) {
		uint32_t key_offset = pk.offset; // start of key

		offset_index_set(offidx, i, key_offset);

		pk_start.offset = 0;

		msgpack_compare_t cmp_start = as_unpack_compare(&pk, &pk_start);

		if (cmp_start == MSGPACK_COMPARE_ERROR) {
			cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_COMPARE_LESS) {
			(*index)++;
		}
		else {
			msgpack_compare_t cmp_end = MSGPACK_COMPARE_LESS;

			// NULL key_end->ptr means largest possible value.
			if (key_end->ptr) {
				pk.offset = key_offset;
				pk_end.offset = 0;
				cmp_end = as_unpack_compare(&pk, &pk_end);
			}

			if (cmp_end == MSGPACK_COMPARE_LESS) {
				cdt_idx_mask_set(mask, i);
				(*count)++;
			}
		}

		// Skip value.
		if (as_unpack_size(&pk) <= 0) {
			cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_unordered() invalid packed map at index %u", i);
			return false;
		}
	}

	offset_index_set_filled(offidx, map->ele_count);

	return true;
}

static bool
packed_map_get_range_by_key_interval_ordered(const packed_map *map,
		const cdt_payload *key_start, const cdt_payload *key_end,
		uint32_t *index, uint32_t *count)
{
	map_ele_find find_key_start;
	map_ele_find_init(&find_key_start, map);

	if (! packed_map_find_key(map, &find_key_start, key_start)) {
		cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_ordered() find key failed, ele_count=%u", map->ele_count);
		return false;
	}

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

		if (! packed_map_find_key(map, &find_key_end, key_end)) {
			cf_warning(AS_PARTICLE, "packed_map_get_range_by_key_interval_ordered() find key failed, ele_count=%u", map->ele_count);
			return false;
		}

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

	return true;
}

// Does not respect invert flag.
static int
packed_map_build_rank_result_by_ele_idx(const packed_map *map,
		const order_index *ele_idx, uint32_t start, uint32_t count,
		cdt_result_data *result)
{
	if (! result->is_multi) {
		uint32_t idx = order_index_get(ele_idx, start);

		return packed_map_build_rank_result_by_idx(map, idx, result);
	}

	define_int_list_builder(builder, result->alloc, count);
	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	vla_map_allidx_if_invalid(uv, map);

	if (! packed_map_ensure_ordidx_filled(map)) {
		cf_warning(AS_PARTICLE, "packed_map_build_rank_result_by_ele_idx() ordidx fill failed");
		return -AS_ERR_PARAMETER;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ele_idx, start + i);
		map_ele_find find;

		map_ele_find_init_from_idx(&find, map, idx);
		packed_map_find_rank_indexed(map, &find);

		if (! find.found_value) {
			cf_warning(AS_PARTICLE, "packed_map_build_rank_result_by_ele_idx() idx %u not found find.rank %u", idx, find.rank);
			return -AS_ERR_PARAMETER;
		}

		uint32_t rank = find.rank;

		cdt_container_builder_add_int_range(&builder, rank, 1, map->ele_count,
				is_rev);
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

// Does not respect invert flag.
static int
packed_map_build_rank_result_by_mask(const packed_map *map,
		const uint64_t *mask, uint32_t count, cdt_result_data *result)
{
	uint32_t idx = 0;

	if (! result->is_multi) {
		idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);

		return packed_map_build_rank_result_by_idx(map, idx, result);
	}

	define_int_list_builder(builder, result->alloc, count);
	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	vla_map_allidx_if_invalid(uv, map);

	if (! packed_map_ensure_ordidx_filled(map)) {
		cf_warning(AS_PARTICLE, "packed_map_build_rank_result_by_mask() ordidx fill failed");
		return -AS_ERR_PARAMETER;
	}

	for (uint32_t i = 0; i < count; i++) {
		idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);

		map_ele_find find;

		map_ele_find_init_from_idx(&find, map, idx);
		packed_map_find_rank_indexed(map, &find);

		if (! find.found_value) {
			cf_warning(AS_PARTICLE, "packed_map_build_rank_result_by_mask() idx %u not found find.rank %u", idx, find.rank);
			return -AS_ERR_PARAMETER;
		}

		uint32_t rank = find.rank;

		cdt_container_builder_add_int_range(&builder, rank, 1, map->ele_count,
				is_rev);
		idx++;
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

static int
packed_map_build_rank_result_by_index_range(const packed_map *map,
		uint32_t index, uint32_t count, cdt_result_data *result)
{
	if (! result->is_multi) {
		return packed_map_build_rank_result_by_idx(map, index, result);
	}

	cf_assert(map_is_k_ordered(map), AS_PARTICLE, "map must be K_ORDERED");

	bool inverted = result_data_is_inverted(result);
	uint32_t ret_count = (inverted ? map->ele_count - count : count);
	define_int_list_builder(builder, result->alloc, ret_count);
	vla_map_allidx_if_invalid(uv, map);

	if (! packed_map_ensure_ordidx_filled(map)) {
		return -AS_ERR_PARAMETER;
	}

	bool is_rev = result->type == RESULT_TYPE_REVRANK;

	if (inverted) {
		for (uint32_t i = 0; i < index; i++) {
			map_ele_find find;

			map_ele_find_init_from_idx(&find, map, i);
			packed_map_find_rank_indexed(map, &find);

			if (! find.found_value) {
				return -AS_ERR_PARAMETER;
			}

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

			if (! find.found_value) {
				return -AS_ERR_PARAMETER;
			}

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

			if (! find.found_value) {
				return -AS_ERR_PARAMETER;
			}

			uint32_t rank = find.rank;

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = map->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

static bool
packed_map_get_key_by_idx(const packed_map *map, cdt_payload *key,
		uint32_t index)
{
	uint32_t pk_offset = offset_index_get_const(&map->offidx, index);

	as_unpacker pk = {
			.buffer = map->contents + pk_offset,
			.length = map->content_sz - pk_offset
	};

	int64_t sz = as_unpack_size(&pk); // read key

	if (sz <= 0) {
		cf_warning(AS_PARTICLE, "packed_map_get_key_by_idx() read key failed sz %ld", sz);
		return false;
	}

	key->ptr = pk.buffer;
	key->sz = (uint32_t)sz;

	return true;
}

static bool
packed_map_get_value_by_idx(const packed_map *map, cdt_payload *value,
		uint32_t idx)
{
	uint32_t pk_offset = offset_index_get_const(&map->offidx, idx);
	uint32_t sz = offset_index_get_delta_const(&map->offidx, idx);

	as_unpacker pk = {
			.buffer = map->contents + pk_offset,
			.length = map->content_sz - pk_offset
	};

	int64_t key_sz = as_unpack_size(&pk); // read key

	if (key_sz <= 0) {
		cf_warning(AS_PARTICLE, "packed_map_get_value_by_idx() read key failed key_sz %ld", key_sz);
		return false;
	}

	value->ptr = pk.buffer + (uint32_t)key_sz;
	value->sz = sz - (uint32_t)key_sz;

	return true;
}

static bool
packed_map_get_pair_by_idx(const packed_map *map, cdt_payload *value,
		uint32_t index)
{
	uint32_t pk_offset = offset_index_get_const(&map->offidx, index);
	uint32_t sz = offset_index_get_delta_const(&map->offidx, index);

	value->ptr = map->contents + pk_offset;
	value->sz = sz;

	return true;
}

// Does not respect invert flag.
static int
packed_map_build_index_result_by_ele_idx(const packed_map *map,
		const order_index *ele_idx, uint32_t start, uint32_t count,
		cdt_result_data *result)
{
	if (count == 0) {
		if (! result_data_set_not_found(result, start)) {
			return -AS_ERR_PARAMETER;
		}

		return AS_OK;
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

		return AS_OK;
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
		offset_index *offidx = (offset_index *)&map->offidx;

		// Preset offsets if necessary.
		if (! map_offset_index_fill(offidx, map->ele_count)) {
			cf_warning(AS_PARTICLE, "packed_map_build_index_result_by_ele_idx() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		// Make order index on stack.
		define_order_index(keyordidx, map->ele_count);
		bool success = order_index_set_sorted(&keyordidx, offidx, map->contents,
				map->content_sz, SORT_BY_KEY);

		cf_assert(success, AS_PARTICLE, "invalid packed map with full offidx");

		for (uint32_t i = 0; i < count; i++) {
			uint32_t idx = order_index_get(ele_idx, start + i);
			uint32_t index = order_index_find_idx(&keyordidx, idx, 0,
					map->ele_count);

			if (index >= map->ele_count) {
				return -AS_ERR_PARAMETER;
			}

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
		}
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

// Does not respect invert flag.
static int
packed_map_build_index_result_by_mask(const packed_map *map,
		const uint64_t *mask, uint32_t count, cdt_result_data *result)
{
	if (count == 0) {
		result_data_set_not_found(result, -1);
		return AS_OK;
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

		return AS_OK;
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
		offset_index *offidx = (offset_index *)&map->offidx;

		// Preset offsets if necessary.
		if (! map_offset_index_fill(offidx, map->ele_count)) {
			cf_warning(AS_PARTICLE, "packed_map_build_index_result_by_ele_idx() invalid packed map");
			return -AS_ERR_PARAMETER;
		}

		// Make order index on stack.
		define_order_index(keyordidx, map->ele_count);
		bool success = order_index_set_sorted(&keyordidx, offidx, map->contents,
				map->content_sz, SORT_BY_KEY);
		uint32_t idx = 0;

		cf_assert(success, AS_PARTICLE, "invalid packed map with full offidx");

		for (uint32_t i = 0; i < count; i++) {
			idx = cdt_idx_mask_find(mask, idx, map->ele_count, false);

			uint32_t index = order_index_find_idx(&keyordidx, idx, 0,
					map->ele_count);

			if (index >= map->ele_count) {
				return -AS_ERR_PARAMETER;
			}

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = map->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
			idx++;
		}
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

// Build by map ele_idx range.
static bool
packed_map_build_ele_result_by_idx_range(const packed_map *map,
		uint32_t start_idx, uint32_t count, cdt_result_data *result)
{
	offset_index *offidx = (offset_index *)&map->offidx;

	if (! map_offset_index_fill(offidx, map->ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_build_ele_result_by_idx_range() invalid packed map");
		return false;
	}

	bool inverted = result_data_is_inverted(result);
	uint32_t offset0 = offset_index_get_const(offidx, start_idx);
	uint32_t offset1 = offset_index_get_const(offidx, start_idx + count);
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

		if (! get_by_idx_func(map, &packed, start_idx)) {
			return false;
		}

		return rollback_alloc_from_msgpack(result->alloc, result->result,
				&packed);
	}

	if (inverted) {
		for (uint32_t i = 0; i < start_idx; i++) {
			cdt_payload packed;

			if (! get_by_idx_func(map, &packed, i)) {
				return false;
			}

			cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		}

		for (uint32_t i = start_idx + count; i < map->ele_count; i++) {
			cdt_payload packed;

			if (! get_by_idx_func(map, &packed, i)) {
				return false;
			}

			cdt_container_builder_add(&builder, packed.ptr, packed.sz);
		}
	}
	else {
		for (uint32_t i = 0; i < count; i++) {
			cdt_payload packed;

			if (! get_by_idx_func(map, &packed, start_idx + i)) {
				return false;
			}

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

			if (! get_by_index_func(map, &packed, index)) {
				return false;
			}

			return rollback_alloc_from_msgpack(result->alloc, result->result,
					&packed);
		}
	}

	for (uint32_t i = 0; i < count; i++) {
		uint32_t index = order_index_get(ele_idx, i + start);
		cdt_payload packed;

		if (! get_by_index_func(map, &packed, index)) {
			return false;
		}

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
		define_order_index2(ele_idx, map->ele_count, 1);

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

		if (! get_by_index_func(map, &packed, index)) {
			return false;
		}

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
			return packed_map_build_rank_result_by_idx_range(map, idx, count,
					result);
		}

		return packed_map_build_rank_result_by_idx(map, idx, result);
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
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

// Return negative codes on error.
static int64_t
packed_map_get_rank_by_idx(const packed_map *map, uint32_t idx)
{
	cf_assert(map_has_offidx(map), AS_PARTICLE, "packed_map_get_rank_by_idx() offset_index needs to be valid");

	uint32_t rank;

	if (order_index_is_valid(&map->value_idx)) {
		if (! packed_map_ensure_ordidx_filled(map)) {
			return -AS_ERR_PARAMETER;
		}

		map_ele_find find_key;
		map_ele_find_init_from_idx(&find_key, map, idx);

		if (! packed_map_find_rank_indexed(map, &find_key)) {
			cf_warning(AS_PARTICLE, "packed_map_get_rank_by_idx() packed_map_find_rank_indexed failed");
			return -AS_ERR_PARAMETER;
		}

		if (! find_key.found_value) {
			cf_warning(AS_PARTICLE, "packed_map_get_rank_by_idx() rank not found, idx=%u rank=%u", find_key.idx, find_key.rank);
			return -AS_ERR_PARAMETER;
		}

		rank = find_key.rank;
	}
	else {
		const offset_index *offidx = &map->offidx;
		uint32_t pk_offset = offset_index_get_const(offidx, idx);
		define_map_unpacker(pk, map);

		as_unpacker pk_entry = {
				.buffer = map->contents + pk_offset,
				.length = map->content_sz - pk_offset
		};

		rank = 0;

		for (uint32_t i = 0; i < map->ele_count; i++) {
			pk_entry.offset = 0;

			msgpack_compare_t cmp = packed_map_compare_values(&pk, &pk_entry);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return -AS_ERR_PARAMETER;
			}

			if (cmp == MSGPACK_COMPARE_LESS) {
				rank++;
			}
		}
	}

	return (int64_t)rank;
}

static int
packed_map_build_rank_result_by_idx(const packed_map *map, uint32_t idx,
		cdt_result_data *result)
{
	int64_t rank = packed_map_get_rank_by_idx(map, idx);

	if (rank < 0) {
		return (int)rank;
	}

	if (result->type == RESULT_TYPE_REVRANK) {
		as_bin_set_int(result->result, (int64_t)map->ele_count - rank - 1);
	}
	else {
		as_bin_set_int(result->result, rank);
	}

	return AS_OK;
}

static int
packed_map_build_rank_result_by_idx_range(const packed_map *map, uint32_t idx,
		uint32_t count, cdt_result_data *result)
{
	define_int_list_builder(builder, result->alloc, count);

	for (uint32_t i = 0; i < count; i++) {
		int64_t rank = packed_map_get_rank_by_idx(map, idx);

		if (rank < 0) {
			return (int)rank;
		}

		if (result->type == RESULT_TYPE_REVRANK) {
			rank = (int64_t)map->ele_count - rank - 1;
		}

		cdt_container_builder_add_int64(&builder, rank);
	}

	cdt_container_builder_set_result(&builder, result);

	return AS_OK;
}

static msgpack_compare_t
packed_map_compare_key_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2)
{
	const packed_map *map = ptr;
	const offset_index *offidx = &map->offidx;

	as_unpacker pk1 = {
			.buffer = map->contents,
			.offset = offset_index_get_const(offidx, idx1),
			.length = map->content_sz
	};

	as_unpacker pk2 = {
			.buffer = map->contents,
			.offset = offset_index_get_const(offidx, idx2),
			.length = map->content_sz
	};

	msgpack_compare_t ret = as_unpack_compare(&pk1, &pk2);

	if (ret == MSGPACK_COMPARE_EQUAL) {
		ret = as_unpack_compare(&pk1, &pk2);
	}

	return ret;
}

static msgpack_compare_t
packed_map_compare_values(as_unpacker *pk1, as_unpacker *pk2)
{
	msgpack_compare_t keycmp = as_unpack_compare(pk1, pk2);

	if (keycmp == MSGPACK_COMPARE_ERROR) {
		return MSGPACK_COMPARE_ERROR;
	}

	msgpack_compare_t ret = as_unpack_compare(pk1, pk2);

	if (ret == MSGPACK_COMPARE_EQUAL) {
		return keycmp;
	}

	return ret;
}

static msgpack_compare_t
packed_map_compare_value_by_idx(const void *ptr, uint32_t idx1, uint32_t idx2)
{
	const packed_map *map = ptr;
	const offset_index *offidx = &map->offidx;

	as_unpacker pk1 = {
			.buffer = map->contents,
			.offset = offset_index_get_const(offidx, idx1),
			.length = map->content_sz
	};

	as_unpacker pk2 = {
			.buffer = map->contents,
			.offset = offset_index_get_const(offidx, idx2),
			.length = map->content_sz
	};

	return packed_map_compare_values(&pk1, &pk2);
}

static bool
packed_map_write_k_ordered(const packed_map *map, uint8_t *write_ptr,
		offset_index *offsets_new)
{
	uint32_t ele_count = map->ele_count;
	define_order_index(key_ordidx, ele_count);
	vla_map_offidx_if_invalid(old, map);

	if (! map_fill_offidx(map)) {
		cf_warning(AS_PARTICLE, "packed_map_op_write_k_ordered() offset fill failed");
		return false;
	}

	if (! order_index_set_sorted_with_offsets(&key_ordidx, old->offidx,
			SORT_BY_KEY)) {
		return false;
	}

	const uint8_t *ptr = old->offidx->contents;

	offset_index_set_filled(offsets_new, 1);

	for (uint32_t i = 0; i < ele_count; i++) {
		uint32_t index = order_index_get(&key_ordidx, i);
		uint32_t offset = offset_index_get_const(old->offidx, index);
		uint32_t sz = offset_index_get_delta_const(old->offidx, index);

		memcpy(write_ptr, ptr + offset, sz);
		write_ptr += sz;
		offset_index_append_size(offsets_new, sz);
	}

	return true;
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
static int32_t
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

	return (int32_t)(op->seg1_sz + op->seg2_sz);
}

static int32_t
packed_map_op_remove(packed_map_op *op, const map_ele_find *found,
		uint32_t count, uint32_t remove_sz)
{
	op->new_ele_count = op->map->ele_count - count;
	op->seg1_sz = found->key_offset;
	op->seg2_offset = found->key_offset + remove_sz;
	op->seg2_sz = op->map->content_sz - op->seg2_offset;

	op->ele_removed = count;

	return (int32_t)(op->seg1_sz + op->seg2_sz);
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

static bool
packed_map_op_write_new_offidx(const packed_map_op *op,
		const map_ele_find *remove_info, const map_ele_find *add_info,
		offset_index *new_offidx, uint32_t kv_sz)
{
	const offset_index *offidx = &op->map->offidx;

	if (! offset_index_is_full(offidx)) {
		return false;
	}

	cf_assert(op->new_ele_count >= op->map->ele_count, AS_PARTICLE, "op->new_ele_count %u < op->map->ele_count %u", op->new_ele_count, op->map->ele_count);

	uint32_t ele_count = op->map->ele_count;

	if (op->new_ele_count - op->map->ele_count != 0) { // add 1
		// Insert at end.
		if (remove_info->idx == ele_count) {
			offset_index_copy(new_offidx, offidx, 0, 0, ele_count, 0);
			offset_index_set(new_offidx, ele_count, op->seg1_sz + op->seg2_sz);
		}
		// Insert at offset.
		else {
			offset_index_copy(new_offidx, offidx, 0, 0,
					remove_info->idx + 1, 0);
			offset_index_copy(new_offidx, offidx, remove_info->idx + 1,
					remove_info->idx, (ele_count - remove_info->idx), kv_sz);
		}
	}
	else { // replace 1
		cf_assert(remove_info->idx == add_info->idx, AS_PARTICLE, "remove_info->idx %u != add_info->idx %u", remove_info->idx, add_info->idx);

		offset_index_copy(new_offidx, offidx, 0, 0, remove_info->idx, 0);
		offset_index_set(new_offidx, remove_info->idx, remove_info->key_offset);

		int delta = (int)kv_sz - (int)remove_info->sz;

		offset_index_copy(new_offidx, offidx, remove_info->idx + 1,
				remove_info->idx + 1, ele_count - remove_info->idx - 1, delta);
	}

	offset_index_set_filled(new_offidx, op->new_ele_count);

	return true;
}

static bool
packed_map_op_write_new_ordidx(const packed_map_op *op,
		const map_ele_find *remove_info, const map_ele_find *add_info,
		order_index *value_idx)
{
	const order_index *ordidx = &op->map->value_idx;

	if (order_index_is_null(ordidx)) {
		return false;
	}

	cf_assert(op->new_ele_count >= op->map->ele_count, AS_PARTICLE, "op->new_ele_count %u < op->map->ele_count %u", op->new_ele_count, op->map->ele_count);

	if (op->new_ele_count - op->map->ele_count != 0) { // add 1
		order_index_op_add(value_idx, ordidx, add_info->idx, add_info->rank);
	}
	else { // replace 1
		cf_assert(remove_info->idx == add_info->idx, AS_PARTICLE, "remove_info->idx %u != add_info->idx %u", remove_info->idx, add_info->idx);

		order_index_op_replace1(value_idx, ordidx, add_info->rank,
				remove_info->rank);
	}

	return true;
}

//------------------------------------------------
// map_particle

static as_particle *
map_particle_create(rollback_alloc *alloc_buf, uint32_t ele_count,
		const uint8_t *buf, uint32_t content_sz, uint8_t flags)
{
	define_map_packer(mpk, ele_count, flags, content_sz);
	map_mem *p_map_mem = (map_mem *)map_packer_create_particle(&mpk, alloc_buf);

	if (! p_map_mem) {
		return NULL;
	}

	map_packer_write_hdridx(&mpk);

	if (buf) {
		memcpy(mpk.write_ptr, buf, content_sz);
	}

	return (as_particle *)p_map_mem;
}

// Return new size on success, negative values on failure.
static int64_t
map_particle_strip_indexes(const as_particle *p, uint8_t *dest)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	if (p_map_mem->sz == 0) {
		return 0;
	}

	as_unpacker upk = {
			.buffer = p_map_mem->data,
			.length = p_map_mem->sz
	};

	int64_t ele_count = as_unpack_map_header_element_count(&upk);

	if (ele_count < 0) {
		return -1;
	}

	as_packer pk = {
			.buffer = dest,
			.capacity = INT_MAX
	};

	if (ele_count > 0 && as_unpack_peek_is_ext(&upk)) {
		as_msgpack_ext ext;

		if (as_unpack_ext(&upk, &ext) != 0) {
			return -2;
		}

		// Skip nil val.
		if (as_unpack_size(&upk) <= 0) {
			return -3;
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
	size_t ele_sz = (size_t)(upk.length - upk.offset);

	memcpy(pk.buffer + pk.offset, upk.buffer + upk.offset, ele_sz);

	return (int64_t)pk.offset + (int64_t)ele_sz;
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

	as_unpacker pk = {
			.buffer = map->contents,
			.offset = find->key_offset,
			.length = map->content_sz
	};

	as_unpack_size(&pk);
	find->value_offset = pk.offset;
	find->sz = offset_index_get_const(&map->offidx, idx + 1) - find->key_offset;
}

//------------------------------------------------
// map_offset_index

static bool
map_offset_index_fill(offset_index *offidx, uint32_t index)
{
	uint32_t ele_filled = offset_index_get_filled(offidx);

	if (index < ele_filled || offidx->_.ele_count == ele_filled) {
		return true;
	}

	as_unpacker pk = {
			.buffer = offidx->contents,
			.length = offidx->content_sz
	};

	pk.offset = offset_index_get_const(offidx, ele_filled - 1);

	for (uint32_t i = ele_filled; i < index; i++) {
		if (as_unpack_size(&pk) <= 0) {
			return false;
		}

		if (as_unpack_size(&pk) <= 0) {
			return false;
		}

		offset_index_set(offidx, i, pk.offset);
	}

	if (as_unpack_size(&pk) <= 0) {
		return false;
	}

	if (as_unpack_size(&pk) <= 0) {
		return false;
	}

	// Make sure last iteration is in range for set.
	if (index < offidx->_.ele_count) {
		offset_index_set(offidx, index, pk.offset);
		offset_index_set_filled(offidx, index + 1);
	}
	// Check if sizes match.
	else if (pk.offset != offidx->content_sz) {
		cf_warning(AS_PARTICLE, "map_offset_index_fill() offset mismatch %u, expected %u", pk.offset, offidx->content_sz);
		return false;
	}
	else {
		offset_index_set_filled(offidx, offidx->_.ele_count);
	}

	return true;
}

static int64_t
map_offset_index_get(offset_index *offidx, uint32_t index)
{
	if (index > offidx->_.ele_count) {
		index = offidx->_.ele_count;
	}

	if (! map_offset_index_fill(offidx, index)) {
		return -1;
	}

	return (int64_t)offset_index_get_const(offidx, index);
}

static int64_t
map_offset_index_get_delta(offset_index *offidx, uint32_t index)
{
	int64_t offset = map_offset_index_get(offidx, index);

	if (offset < 0) {
		return offset;
	}

	if (index == offidx->_.ele_count - 1) {
		return (int64_t)offidx->content_sz - offset;
	}

	return map_offset_index_get(offidx, index + 1) - offset;
}

//------------------------------------------------
// offidx_op

static void
offidx_op_init(offidx_op *op, offset_index *dest, const offset_index *src)
{
	op->dest = dest;
	op->src = src;
	op->d_i = 0;
	op->s_i = 0;
	op->delta = 0;
}

static void
offidx_op_remove(offidx_op *op, uint32_t index)
{
	uint32_t count = index - op->s_i;
	uint32_t mem_sz = offset_index_get_delta_const(op->src, index);

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count, op->delta);

	op->delta -= mem_sz;
	op->d_i += count;
	op->s_i += count + 1;
}

static void
offidx_op_remove_range(offidx_op *op, uint32_t index, uint32_t count)
{
	uint32_t ele_count = op->src->_.ele_count;
	uint32_t delta_count = index - op->s_i;
	uint32_t offset = offset_index_get_const(op->src, index);
	uint32_t mem_sz;

	if (index + count == ele_count) {
		mem_sz = op->src->content_sz - offset;
	}
	else {
		mem_sz = offset_index_get_const(op->src, index + count) - offset;
	}

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, delta_count,
			op->delta);

	op->delta -= mem_sz;
	op->d_i += delta_count;
	op->s_i += delta_count + count;
}

static void
offidx_op_end(offidx_op *op)
{
	uint32_t ele_count = op->src->_.ele_count;
	uint32_t count = ele_count - op->s_i;

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count, op->delta);
	op->d_i += count;
	offset_index_set_filled(op->dest, op->d_i);
}

//------------------------------------------------
// order_index

static bool
order_index_sort(order_index *ordidx, const offset_index *offsets,
		const uint8_t *contents, uint32_t content_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;

	index_sort_userdata udata = {
			.order = ordidx,
			.offsets = offsets,
			.contents = contents,
			.content_sz = content_sz,
			.error = false,
			.sort_by = sort_by
	};

	qsort_r(order_index_get_mem(ordidx, 0), ele_count, ordidx->_.ele_sz,
			map_packer_fill_index_sort_compare, (void *)&udata);

	if (udata.error) {
		return false;
	}

	return true;
}

static inline bool
order_index_set_sorted(order_index *ordidx, const offset_index *offsets,
		const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		order_index_set(ordidx, i, i);
	}

	return order_index_sort(ordidx, offsets, ele_start, tot_ele_sz, sort_by);
}

static bool
order_index_set_sorted_with_offsets(order_index *ordidx,
		const offset_index *offsets, sort_by_t sort_by)
{
	return order_index_set_sorted(ordidx, offsets, offsets->contents,
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
	define_order_index2(cntidx, ele_count, mask_count);

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
		cdt_modify_data *cdt_udata)
{
	as_bin *b = cdt_udata->b;
	as_cdt_optype optype = state->type;

	if (! is_map_type(as_bin_get_particle_type(b)) && as_bin_inuse(b)) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid type %d", as_bin_get_particle_type(b));
		cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	define_rollback_alloc(alloc_buf, cdt_udata->alloc_buf, 1, true);
	// Results always on the heap.
	define_rollback_alloc(alloc_result, NULL, 1, false);
	int ret = AS_OK;

	cdt_result_data result = {
			.result = cdt_udata->result,
			.alloc = alloc_result,
	};

	switch (optype) {
	case AS_CDT_OP_MAP_SET_TYPE: {
		uint64_t flags;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &flags)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_use_static_map_mem_if_notinuse(b, 0);
		ret = map_set_flags(b, alloc_buf, result.result, (uint8_t)flags);
		break;
	}
	case AS_CDT_OP_MAP_ADD: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		as_bin_use_static_map_mem_if_notinuse(b, flags);
		ret = map_add(b, alloc_buf, &key, &value, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_ADD_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		as_bin_use_static_map_mem_if_notinuse(b, flags);
		ret = map_add_items(b, alloc_buf, &items, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_PUT: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;
		uint64_t modify = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = ! (modify & AS_CDT_MAP_NO_OVERWRITE),
				.allow_create = ! (modify & AS_CDT_MAP_NO_CREATE),
				.no_fail = modify & AS_CDT_MAP_NO_FAIL,
				.do_partial = modify & AS_CDT_MAP_DO_PARTIAL
		};

		as_bin_use_static_map_mem_if_notinuse(b, flags);
		ret = map_add(b, alloc_buf, &key, &value, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_PUT_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;
		uint64_t modify = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = ! (modify & AS_CDT_MAP_NO_OVERWRITE),
				.allow_create = ! (modify & AS_CDT_MAP_NO_CREATE),
				.no_fail = modify & AS_CDT_MAP_NO_FAIL,
				.do_partial = modify & AS_CDT_MAP_DO_PARTIAL
		};

		as_bin_use_static_map_mem_if_notinuse(b, flags);
		ret = map_add_items(b, alloc_buf, &items, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_REPLACE: {
		cdt_payload key;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		as_bin_use_static_map_mem_if_notinuse(b, 0);
		ret = map_add(b, alloc_buf, &key, &value, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_REPLACE_ITEMS: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_ERR_ELEMENT_NOT_FOUND;
			return false;
		}

		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		ret = map_add_items(b, alloc_buf, &items, result.result, &control);
		break;
	}
	case AS_CDT_OP_MAP_INCREMENT:
	case AS_CDT_OP_MAP_DECREMENT: {
		cdt_payload key;
		cdt_payload delta_value = { NULL };
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &delta_value, &flags)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_use_static_map_mem_if_notinuse(b, flags);
		ret = map_increment(b, alloc_buf, &key, &delta_value, result.result,
				optype == AS_CDT_OP_MAP_DECREMENT);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t op_flags;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &op_flags, &key)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, op_flags, false);
		ret = map_remove_by_key_interval(b, alloc_buf, &key, &key, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = map_remove_by_index_range(b, alloc_buf, index, 1, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = map_remove_by_value_interval(b, alloc_buf, &value, &value,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = map_remove_by_rank_range(b, alloc_buf, index, 1, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_all_by_key_list(b, alloc_buf, &items, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_value_interval(b, alloc_buf, &value, &value,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_all_by_value_list(b, alloc_buf, &items, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end = { NULL };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start,
				&key_end)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_key_interval(b, alloc_buf, &key_start, &key_end,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_index_range(b, alloc_buf, index, count, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { NULL };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_value_interval(b, alloc_buf, &value_start,
				&value_end, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_rank_range(b, alloc_buf, rank, count, &result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &index,
				&count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_rel_index_range(b, alloc_buf, &value, index, count,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &rank,
				&count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = map_remove_by_rel_rank_range(b, alloc_buf, &value, rank, count,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_CLEAR: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		ret = map_clear(b, alloc_buf, result.result);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	if (ret != AS_OK) {
		cf_warning(AS_PARTICLE, "%s: failed", cdt_process_state_get_op_name(state));
		cdt_udata->ret_code = ret;
		rollback_alloc_rollback(alloc_result);
		rollback_alloc_rollback(alloc_buf);
		return false;
	}

	if (b->particle == (const as_particle *)&map_mem_empty) {
		as_bin_set_empty_packed_map(b, alloc_buf, 0);
	}
	else if (b->particle == (const as_particle *)map_mem_empty_flagged_table) {
		as_bin_set_empty_packed_map(b, alloc_buf,
				map_mem_empty_flagged_table[0].map.ext_flags);
	}
	else if (b->particle ==
			(const as_particle *)(map_mem_empty_flagged_table + 1)) {
		as_bin_set_empty_packed_map(b, alloc_buf,
				map_mem_empty_flagged_table[1].map.ext_flags);
	}

	return true;
}

bool
cdt_process_state_packed_map_read_optype(cdt_process_state *state,
		cdt_read_data *cdt_udata)
{
	const as_bin *b = cdt_udata->b;
	as_cdt_optype optype = state->type;

	if (! is_map_type(as_bin_get_particle_type(b))) {
		cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	packed_map map;

	if (! packed_map_init_from_bin(&map, b, false)) {
		cf_warning(AS_PARTICLE, "%s: invalid map", cdt_process_state_get_op_name(state));
		cdt_udata->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	// Just one entry needed for results bin.
	define_rollback_alloc(alloc_result, NULL, 1, false);
	int ret = AS_OK;

	cdt_result_data result = {
			.result = cdt_udata->result,
			.alloc = alloc_result,
	};

	switch (optype) {
	case AS_CDT_OP_MAP_SIZE: {
		as_bin_set_int(result.result, map.ele_count);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY: {
		uint64_t op_flags;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &op_flags, &key)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, op_flags, false);
		ret = packed_map_get_remove_by_key_interval(&map, NULL, NULL, &key,
				&key, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = packed_map_get_remove_by_value_interval(&map, NULL, NULL,
				&value, &value, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX: {
		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = packed_map_get_remove_by_index_range(&map, NULL, NULL, index, 1,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK: {
		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = packed_map_get_remove_by_rank_range(&map, NULL, NULL, rank, 1,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_GET_ALL_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_value_interval(&map, NULL, NULL,
				&value, &value, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL: {
		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end = { NULL };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start,
				&key_end)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_key_interval(&map, NULL, NULL,
				&key_start, &key_end, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL: {
		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { NULL };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_value_interval(&map, NULL, NULL,
				&value_start, &value_end, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX_RANGE: {
		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_index_range(&map, NULL, NULL, index,
				count, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK_RANGE: {
		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_rank_range(&map, NULL, NULL, rank, count,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_LIST: {
		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_all_by_key_list(&map, NULL, NULL, &items,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_LIST: {
		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_all_by_value_list(&map, NULL, NULL, &items,
				&result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE: {
		uint64_t result_type;
		cdt_payload value;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &index,
				&count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_rel_index_range(&map, NULL, NULL,
				&value, index, count, &result);
		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE: {
		uint64_t result_type;
		cdt_payload value;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &rank,
				&count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_map_get_remove_by_rel_rank_range(&map, NULL, NULL,
				&value, rank, count, &result);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_read_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_ERR_PARAMETER;
		return false;
	}

	if (ret != AS_OK) {
		cf_warning(AS_PARTICLE, "%s: failed", cdt_process_state_get_op_name(state));
		cdt_udata->ret_code = ret;
		rollback_alloc_rollback(alloc_result);
		return false;
	}

	return true;
}


//==========================================================
// Debugging support.
//

static void
map_print(const packed_map *map, const char *name)
{
	print_packed(map->packed, map->packed_sz, name);
}

static bool
map_verify_fn(const as_bin *b)
{
	packed_map map;

	uint8_t type = as_bin_get_particle_type(b);

	if (type != AS_PARTICLE_TYPE_MAP) {
		cf_warning(AS_PARTICLE, "map_verify() non-map type: %u", type);
		return false;
	}

	// Check header.
	if (! packed_map_init_from_bin(&map, b, false)) {
		cf_warning(AS_PARTICLE, "map_verify() invalid packed map");
		return false;
	}

	if (map.flags != 0) {
		const uint8_t *byte = map.contents - 1;

		if (*byte != 0xC0) {
			cf_warning(AS_PARTICLE, "map_verify() invalid ext header, expected C0 for pair.2");
		}
	}

	const order_index *ordidx = &map.value_idx;
	bool check_offidx = map_has_offidx(&map);
	define_map_unpacker(pk, &map);
	vla_map_offidx_if_invalid(u, &map);

	uint32_t filled = offset_index_get_filled(u->offidx);
	define_offset_index(temp_offidx, u->offidx->contents, u->offidx->content_sz,
			u->offidx->_.ele_count);

	if (map.ele_count != 0) {
		offset_index_copy(&temp_offidx, u->offidx, 0, 0, filled, 0);
	}

	// Check offsets.
	for (uint32_t i = 0; i < map.ele_count; i++) {
		uint32_t offset;

		if (check_offidx) {
			if (i < filled) {
				offset = offset_index_get_const(u->offidx, i);

				if (pk.offset != offset) {
					cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u expected=%d", i, offset, pk.offset);
					return false;
				}
			}
			else {
				offset_index_set(&temp_offidx, i, pk.offset);
			}
		}
		else {
			offset_index_set(u->offidx, i, pk.offset);
		}

		offset = pk.offset;

		if (as_unpack_size(&pk) <= 0) {
			cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u invalid key", i, offset, pk.offset);
			return false;
		}

		offset = pk.offset;

		if (as_unpack_size(&pk) <= 0) {
			cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u invalid value", i, offset, pk.offset);
			return false;
		}
	}

	if (check_offidx && filled < map.ele_count) {
		u->offidx->_.ptr = temp_offidx._.ptr;
	}

	// Check packed size.
	if (map.content_sz != pk.offset) {
		cf_warning(AS_PARTICLE, "map_verify() content_sz=%u expected=%u", map.content_sz, pk.offset);
		return false;
	}

	// Check key orders.
	if (map_is_k_ordered(&map) && map.ele_count > 0) {
		pk.offset = 0;

		define_map_unpacker(pk_key, &map);

		for (uint32_t i = 1; i < map.ele_count; i++) {
			uint32_t offset = pk.offset;
			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u invalid key", i, offset, pk.offset);
				return false;
			}

			if (cmp == MSGPACK_COMPARE_GREATER) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u keys not in order", i, offset, pk.offset);
				return false;
			}

			pk_key.offset = offset;

			if (as_unpack_size(&pk) <= 0) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u invalid value", i, offset, pk.offset);
				return false;
			}
		}
	}

	// Check value orders.
	if (order_index_is_filled(ordidx) && map.ele_count > 0) {
		// Compare with freshly sorted.
		define_order_index(cmp_order, map.ele_count);

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
		pk.offset = 0;

		define_map_unpacker(prev_value, &map);
		uint32_t index = order_index_get(ordidx, 0);

		prev_value.offset = offset_index_get_const(u->offidx, index);

		if (as_unpack_size(&prev_value) <= 0) {
			cf_warning(AS_PARTICLE, "map_verify() index=%u pk.offset=%u invalid key", index, pk.offset);
			return false;
		}

		for (uint32_t i = 1; i < map.ele_count; i++) {
			index = order_index_get(ordidx, i);
			pk.offset = offset_index_get_const(u->offidx, index);

			if (as_unpack_size(&pk) <= 0) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u index=%u pk.offset=%u invalid key", i, index, pk.offset);
				return false;
			}

			uint32_t offset = pk.offset;
			msgpack_compare_t cmp = as_unpack_compare(&prev_value, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u invalid value", i, offset, pk.offset);
				return false;
			}

			if (cmp == MSGPACK_COMPARE_GREATER) {
				cf_warning(AS_PARTICLE, "map_verify() i=%u offset=%u pk.offset=%u value index not in order", i, offset, pk.offset);
				return false;
			}

			prev_value.offset = offset;
		}
	}

	return true;
}

static bool
map_verify(const as_bin *b)
{
	define_rollback_alloc(alloc_idx, NULL, 2, false); // for temp indexes

	cdt_idx_clear();
	cdt_idx_set_alloc(alloc_idx);

	bool ret = map_verify_fn(b);

	cdt_idx_clear();

	return ret;
}

// Quash warnings for debug function.
void
as_cdt_map_debug_dummy()
{
	map_verify(NULL);
	map_print(NULL, NULL);
}
