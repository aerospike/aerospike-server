/*
 * particle_list.c
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

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/param.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"


//==========================================================
// LIST particle interface - function declarations.
//

// Destructor, etc.
void list_destruct(as_particle *p);
uint32_t list_size(const as_particle *p);

// Handle "wire" format.
int32_t list_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t list_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int list_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t list_wire_size(const as_particle *p);
uint32_t list_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t list_size_from_asval(const as_val *val);
void list_from_asval(const as_val *val, as_particle **pp);
as_val *list_to_asval(const as_particle *p);
uint32_t list_asval_wire_size(const as_val *val);
uint32_t list_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t list_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void list_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);

// Handle on-device "flat" format.
const uint8_t *list_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp);
uint32_t list_flat_size(const as_particle *p);
uint32_t list_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// LIST particle interface - vtable.
//

const as_particle_vtable list_vtable = {
		list_destruct,
		list_size,

		list_concat_size_from_wire,
		list_append_from_wire,
		list_prepend_from_wire,
		list_incr_from_wire,
		list_size_from_wire,
		list_from_wire,
		list_compare_from_wire,
		list_wire_size,
		list_to_wire,

		list_size_from_asval,
		list_from_asval,
		list_to_asval,
		list_asval_wire_size,
		list_asval_to_wire,

		list_size_from_msgpack,
		list_from_msgpack,

		blob_skip_flat,
		blob_cast_from_flat,
		list_from_flat,
		list_flat_size,
		list_to_flat
};


//==========================================================
// Typedefs & constants.
//

#if defined(CDT_DEBUG_VERIFY)
#define LIST_DEBUG_VERIFY
#endif

#define PACKED_LIST_INDEX_STEP 128

typedef struct packed_list_s {
	const uint8_t *packed;
	uint32_t packed_sz;

	uint32_t ele_count; // excludes ext ele
	// Mutable state member (is considered mutable in const objects).
	offset_index offidx; // offset start at contents (excluding ext metadata ele)
	// Mutable state member (is considered mutable in const objects).
	offset_index full_offidx; // index at every element
	uint8_t ext_flags;

	const uint8_t *contents; // where elements start (excludes ext)
	uint32_t content_sz;
} packed_list;

typedef struct packed_list_op_s {
	const packed_list *list;

	uint32_t new_ele_count;
	uint32_t new_content_sz;

	uint32_t seg1_sz;
	uint32_t seg2_offset;
	uint32_t seg2_sz;
	uint32_t nil_ele_sz; // number of nils we need to insert
} packed_list_op;

typedef struct list_mem_s {
	uint8_t type;
	uint32_t sz;
	uint8_t data[];
} __attribute__ ((__packed__)) list_mem;

typedef struct list_flat_s {
	uint8_t type;
	uint32_t sz; // host order on device and in memory
	uint8_t data[];
} __attribute__ ((__packed__)) list_flat;

typedef enum {
	LIST_EMPTY_LIST_HDR = 0,
	LIST_EMPTY_EXT_HDR,
	LIST_EMPTY_EXT_SZ,
	LIST_EMPTY_EXT_FLAGS,
	LIST_EMPTY_FLAGED_SIZE
} list_empty_bytes;

static const list_mem list_ordered_empty = {
		.type = AS_PARTICLE_TYPE_LIST,
		.sz = LIST_EMPTY_FLAGED_SIZE,
		.data = {
				[LIST_EMPTY_LIST_HDR] = 0x91,
				[LIST_EMPTY_EXT_HDR] = 0xC7,
				[LIST_EMPTY_EXT_SZ] = 0,
				[LIST_EMPTY_EXT_FLAGS] = AS_PACKED_LIST_FLAG_ORDERED
		}
};

static const list_mem list_mem_empty = {
		.type = AS_PARTICLE_TYPE_LIST,
		.sz = 1,
		.data = {0x90}
};

typedef struct {
	const offset_index *offsets;
	const order_index *order;
	as_cdt_sort_flags flags;
	bool error;
} list_order_index_sort_userdata;

typedef struct {
	offset_index *offidx;
	uint8_t mem_temp[];
} __attribute__ ((__packed__)) list_vla_offidx_cast;

#define define_packed_list_op(__name, __list_p) \
	packed_list_op __name; \
	packed_list_op_init(&__name, __list_p)

#define list_full_offidx_p(__list_p) \
	((offset_index *)(list_is_ordered(__list_p) ? &(__list_p)->offidx : &(__list_p)->full_offidx))

#define setup_list_must_have_full_offidx(__name, __list_p, __alloc) \
	uint8_t __name ## __vlatemp[sizeof(offset_index *) + offset_index_vla_sz(list_full_offidx_p(__list_p))]; \
	list_vla_offidx_cast *__name = (list_vla_offidx_cast *)__name ## __vlatemp; \
	__name->offidx = list_full_offidx_p(__list_p); \
	offset_index_alloc_temp(list_full_offidx_p(__list_p), __name->mem_temp, __alloc)

#define setup_list_context_full_offidx(__name, __list_p, __alloc, __need_idx_mem) \
	uint8_t __name ## __vlatemp[sizeof(offset_index *) + (need_idx_mem ? 0 : offset_index_vla_sz(list_full_offidx_p(__list_p)))]; \
	list_vla_offidx_cast *__name = (list_vla_offidx_cast *)__name ## __vlatemp; \
	__name->offidx = list_full_offidx_p(__list_p); \
	if (__need_idx_mem) { \
		__name->offidx->_.ptr = rollback_alloc_reserve(__alloc, offset_index_size(__name->offidx)); \
		offset_index_set_filled(__name->offidx, 1); \
	} \
	else { \
		offset_index_alloc_temp(list_full_offidx_p(__list_p), __name->mem_temp, __alloc); \
	}

#define define_packed_list_particle(__name, __particle, __ret) \
	packed_list __name; \
	bool __ret = packed_list_init_from_particle(&__name, __particle)


//==========================================================
// Forward declarations.
//

static inline bool is_list_type(uint8_t type);
static inline bool flags_is_ordered(uint8_t flags);
static inline bool list_is_ordered(const packed_list *list);
static inline uint8_t get_ext_flags(bool ordered);
static uint32_t list_calc_ext_content_sz(uint32_t ele_count, uint32_t content_sz, bool ordered);

static uint32_t list_pack_header(uint8_t *buf, uint32_t ele_count);
static void list_pack_empty_index(as_packer *pk, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz, bool is_ordered);

// as_bin
static void as_bin_set_ordered_empty_list(as_bin *b, rollback_alloc *alloc_buf);

// cdt_context
static inline void cdt_context_set_empty_list(cdt_context *ctx, bool is_ordered);
static inline void cdt_context_use_static_list_if_notinuse(cdt_context *ctx, uint64_t create_flags);

static inline bool cdt_context_list_need_idx_mem(const cdt_context *ctx, const packed_list *list, bool is_dim);
static inline void cdt_context_list_push(cdt_context *ctx, const packed_list *list, uint32_t idx, rollback_alloc *alloc_idx, bool is_dim, bool need_idx_mem);
static inline void cdt_context_list_handle_possible_noop(cdt_context *ctx);

// packed_list
static bool packed_list_init(packed_list *list, const uint8_t *buf, uint32_t sz);
static inline bool packed_list_init_from_particle(packed_list *list, const as_particle *p);
static bool packed_list_init_from_bin(packed_list *list, const as_bin *b);
static bool packed_list_init_from_ctx(packed_list *list, const cdt_context *ctx);
static inline bool packed_list_init_from_com(packed_list *list, cdt_op_mem *com);
static bool packed_list_init_from_ctx_orig(packed_list *list, const cdt_context *ctx);
static bool packed_list_unpack_hdridx(packed_list *list);
static void packed_list_partial_offidx_update(const packed_list *list);

static void packed_list_find_by_value_ordered(const packed_list *list, const cdt_payload *value, order_index_find *find);
static uint32_t packed_list_find_idx_offset(const packed_list *list, uint32_t index);
static uint32_t packed_list_find_idx_offset_continue(const packed_list *list, uint32_t index, uint32_t index0, uint32_t offset0);
static void packed_list_find_rank_range_by_value_interval_ordered(const packed_list *list, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank_r, uint32_t *count_r, bool is_multi);
static bool packed_list_find_rank_range_by_value_interval_unordered(const packed_list *list, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, uint64_t *mask_val, bool inverted, bool is_multi);

static uint32_t packed_list_mem_sz(const packed_list *list, bool has_ext, uint32_t *ext_content_sz_r);
static uint32_t packed_list_pack_buf(const packed_list *list, uint8_t *buf, uint32_t sz, uint32_t ext_content_sz, bool strip_flags);
static list_mem *packed_list_pack_mem(const packed_list *list, list_mem *p_list_mem);
static void packed_list_content_pack(const packed_list *list, as_packer *pk);
static int packed_list_remove_by_idx(const packed_list *list, cdt_op_mem *com, const uint64_t rm_idx, uint32_t *rm_sz);
static int packed_list_remove_by_mask(const packed_list *list, cdt_op_mem *com, const uint64_t *rm_mask, uint32_t rm_count, uint32_t *rm_sz);

static int packed_list_trim(const packed_list *list, cdt_op_mem *com, uint32_t index, uint32_t count);
static int packed_list_get_remove_by_index_range(const packed_list *list, cdt_op_mem *com, int64_t index, uint64_t count);
static int packed_list_get_remove_by_value_interval(const packed_list *list, cdt_op_mem *com, const cdt_payload *value_start, const cdt_payload *value_end);
static int packed_list_get_remove_by_rank_range(const packed_list *list, cdt_op_mem *com, int64_t rank, uint64_t count);
static int packed_list_get_remove_all_by_value_list(const packed_list *list, cdt_op_mem *com, const cdt_payload *value_list);
static int packed_list_get_remove_by_rel_rank_range(const packed_list *list, cdt_op_mem *com, const cdt_payload *value, int64_t rank, uint64_t count);

static int packed_list_insert(const packed_list *list, cdt_op_mem *com, int64_t index, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags, bool set_result);
static int packed_list_add_ordered(const packed_list *list, cdt_op_mem *com, const cdt_payload *payload, uint64_t mod_flags);
static int packed_list_add_items_ordered(const packed_list *list, cdt_op_mem *com, const cdt_payload *items, uint64_t mod_flags);
static int packed_list_replace_ordered(const packed_list *list, cdt_op_mem *com, uint32_t index, const cdt_payload *value, uint64_t mod_flags);

static bool packed_list_check_order_and_fill_offidx(const packed_list *list);
static bool packed_list_deep_check_order_and_fill_offidx(const packed_list *list);

// packed_list_op
static void packed_list_op_init(packed_list_op *op, const packed_list *list);
static bool packed_list_op_insert(packed_list_op *op, uint32_t index, uint32_t count, uint32_t insert_sz);
static bool packed_list_op_remove(packed_list_op *op, uint32_t index, uint32_t count);

static uint32_t packed_list_op_write_seg1(const packed_list_op *op, uint8_t *buf);
static uint32_t packed_list_op_write_seg2(const packed_list_op *op, uint8_t *buf);

static bool packed_list_builder_add_ranks_by_range(const packed_list *list, cdt_container_builder *builder, msgpack_in *start, uint32_t count, bool reverse);

// list
static list_mem *list_create(rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_sz);
static as_particle *list_simple_create_from_buf(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz);
static as_particle *list_simple_create(rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_sz, uint8_t **contents_r);

static int list_set_flags(cdt_op_mem *com, uint8_t flags);
static int list_append(cdt_op_mem *com, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags);
static int list_insert(cdt_op_mem *com, int64_t index, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags);
static int list_set(cdt_op_mem *com, int64_t index, const cdt_payload *value, uint64_t mod_flags);
static int list_increment(cdt_op_mem *com, int64_t index, cdt_payload *delta_value, uint64_t mod_flags);
static int list_sort(cdt_op_mem *com, as_cdt_sort_flags sort_flags);

static int list_remove_by_index_range(cdt_op_mem *com, int64_t index, uint64_t count);
static int list_remove_by_value_interval(cdt_op_mem *com, const cdt_payload *value_start, const cdt_payload *value_end);
static int list_remove_by_rank_range(cdt_op_mem *com, int64_t rank, uint64_t count);
static int list_remove_all_by_value_list(cdt_op_mem *com, const cdt_payload *value_list);
static int list_remove_by_rel_rank_range(cdt_op_mem *com, const cdt_payload *value, int64_t rank, uint64_t count);

static uint8_t *list_setup_bin(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags, uint32_t content_sz, uint32_t ele_count, uint32_t idx_trunc, const offset_index *old_offidx, offset_index *new_offidx);
static uint8_t *list_setup_bin_ctx(cdt_context *ctx, uint8_t flags, uint32_t content_sz, uint32_t ele_count, uint32_t idx_trunc, const offset_index *old_offidx, offset_index *new_offidx);

// list_offset_index
static inline uint32_t list_offset_partial_index_count(uint32_t ele_count);
static void list_offset_index_rm_mask_cpy(offset_index *dst, const offset_index *full_src, const uint64_t *rm_mask, uint32_t rm_count);

// list_order_index
static int list_order_index_sort_cmp_fn(const void *x, const void *y, void *p);
static uint8_t *list_order_index_pack(const order_index *ordidx, const offset_index *full_offidx, uint8_t *buf, offset_index *new_offidx);

// list_order_heap
static msgpack_cmp_type list_order_heap_cmp_fn(const void *udata, uint32_t idx1, uint32_t idx2);

// list_result_data
static bool list_result_data_set_not_found(cdt_result_data *rd, int64_t index);
static void list_result_data_set_values_by_mask(cdt_result_data *rd, const uint64_t *mask, const offset_index *full_offidx, uint32_t count, uint32_t sz);
static void list_result_data_set_values_by_idxcount(cdt_result_data *rd, const order_index *idxcnt, const offset_index *full_offidx);
static bool list_result_data_set_values_by_ordidx(cdt_result_data *rd, const order_index *ordidx, const offset_index *full_offidx, uint32_t count, uint32_t sz);

// Debugging support
void list_print(const packed_list *list, const char *name);


//==========================================================
// LIST particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
list_destruct(as_particle *p)
{
	cf_free(p);
}

uint32_t
list_size(const as_particle *p)
{
	const list_mem *p_list_mem = (const list_mem *)p;
	return (uint32_t)sizeof(list_mem) + p_list_mem->sz;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
list_concat_size_from_wire(as_particle_type wire_type,
		const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for list");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to list");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to list");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "increment of list");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int32_t
list_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// TODO - CDT can't determine in memory or not.
	packed_list list;

	if (! packed_list_init(&list, wire_value, value_size)) {
		cf_warning(AS_PARTICLE, "list_size_from_wire() invalid packed list");
		return -AS_ERR_UNKNOWN;
	}

	return (int32_t)(sizeof(list_mem) + packed_list_mem_sz(&list, true, NULL));
}

int
list_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	// TODO - CDT can't determine in memory or not.
	// It works for data-not-in-memory but we'll incur a memcpy that could be
	// eliminated.
	packed_list list;
	bool is_valid = packed_list_init(&list, wire_value, value_size);

	cf_assert(is_valid, AS_PARTICLE, "list_from_wire() invalid packed list");

	list_mem *p_list_mem = packed_list_pack_mem(&list, (list_mem *)*pp);

	p_list_mem->type = wire_type;

	packed_list new_list;
	bool check = packed_list_init_from_particle(&new_list, *pp);

	cf_assert(check, AS_PARTICLE, "list_from_wire() invalid list");

	if (! packed_list_deep_check_order_and_fill_offidx(&new_list)) {
		cf_warning(AS_PARTICLE, "list_from_wire() invalid packed list");
		return -AS_ERR_UNKNOWN;
	}

	return AS_OK;
}

int
list_compare_from_wire(const as_particle *p, as_particle_type wire_type,
		const uint8_t *wire_value, uint32_t value_size)
{
	cf_warning(AS_PARTICLE, "list_compare_from_wire() not implemented");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

uint32_t
list_wire_size(const as_particle *p)
{
	define_packed_list_particle(list, p, success);
	cf_assert(success, AS_PARTICLE, "list_wire_size() invalid packed list");

	return packed_list_mem_sz(&list, false, NULL);
}

uint32_t
list_to_wire(const as_particle *p, uint8_t *wire)
{
	define_packed_list_particle(list, p, success);
	cf_assert(success, AS_PARTICLE, "list_to_wire() invalid packed list");

	return packed_list_pack_buf(&list, wire, INT_MAX, 0, true);
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
list_size_from_asval(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t sz = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	const as_list *list = (const as_list *)val;

	uint32_t ele_count = as_list_size(list);
	uint32_t base_hdr_sz = as_pack_list_header_get_size(ele_count);
	uint32_t content_sz = sz - base_hdr_sz;
	bool is_ordered = flags_is_ordered((uint8_t)list->flags);
	uint32_t ext_content_sz = list_calc_ext_content_sz(ele_count, content_sz,
			is_ordered);
	uint32_t hdr_sz = (is_ordered || ext_content_sz != 0) ?
			as_pack_list_header_get_size(ele_count + 1) : base_hdr_sz;

	return (uint32_t)sizeof(list_mem) + hdr_sz +
			as_pack_ext_header_get_size(ext_content_sz) + ext_content_sz +
			content_sz;
}

void
list_from_asval(const as_val *val, as_particle **pp)
{
	as_serializer s;
	as_msgpack_init(&s);

	list_mem *p_list_mem = (list_mem *)*pp;
	int32_t sz = as_serializer_serialize_presized(&s, val, p_list_mem->data);

	cf_assert(sz >= 0, AS_PARTICLE, "list_from_asval() failed to presize");
	as_serializer_destroy(&s);

	const as_list *list = (const as_list *)val;

	uint32_t ele_count = as_list_size(list);
	uint32_t base_hdr_sz = as_pack_list_header_get_size(ele_count);
	uint32_t content_sz = (uint32_t)sz - base_hdr_sz;
	bool is_ordered = flags_is_ordered((uint8_t)list->flags);
	uint32_t ext_content_sz = list_calc_ext_content_sz(ele_count, content_sz,
			is_ordered);

	if (is_ordered || ext_content_sz != 0) {
		uint32_t hdr_sz = as_pack_list_header_get_size(ele_count + 1);
		uint32_t ele_start = hdr_sz +
				as_pack_ext_header_get_size(ext_content_sz) + ext_content_sz;

		// Prefer memmove over 2x serialize.
		memmove(p_list_mem->data + ele_start, p_list_mem->data + base_hdr_sz,
				content_sz);

		as_packer pk = {
				.buffer = p_list_mem->data,
				.capacity = ele_start
		};

		as_pack_list_header(&pk, ele_count + 1);
		as_pack_ext_header(&pk, ext_content_sz, get_ext_flags(is_ordered));

		if (ext_content_sz != 0) {
			list_pack_empty_index(&pk, ele_count, NULL, content_sz, is_ordered);
		}

		cf_assert(pk.offset == ele_start, AS_PARTICLE, "size mismatch pk.offset(%d) != ele_start(%u)", pk.offset, ele_start);
		p_list_mem->sz = ele_start + content_sz;

		if (is_ordered && ele_count > 1) {
			packed_list new_list;
			bool check = packed_list_init(&new_list, p_list_mem->data,
					p_list_mem->sz);

			cf_assert(check, AS_PARTICLE, "invalid list");

			if (! packed_list_check_order_and_fill_offidx(&new_list)) {
				uint8_t *temp_mem = NULL;
				uint8_t buf[sizeof(packed_list) +
							(p_list_mem->sz < CDT_MAX_STACK_OBJ_SZ ?
									p_list_mem->sz : 0)];
				uint8_t *ptr;

				if (p_list_mem->sz < CDT_MAX_STACK_OBJ_SZ) {
					ptr = buf;
				}
				else {
					temp_mem = cf_malloc(p_list_mem->sz);
					ptr = temp_mem;
				}

				memcpy(ptr, p_list_mem->data, p_list_mem->sz);

				define_rollback_alloc(alloc_idx, NULL, 1, false); // for temp indexes
				define_order_index(ordidx, new_list.ele_count, alloc_idx);
				packed_list not_ordered_list;

				packed_list_init(&not_ordered_list, ptr, p_list_mem->sz);
				list_full_offset_index_fill_all(&not_ordered_list.offidx);

				if (! list_order_index_sort(&ordidx, &not_ordered_list.offidx,
						AS_CDT_SORT_ASCENDING)) {
					cf_crash(AS_PARTICLE, "list_sort() invalid list");
				}

				list_order_index_pack(&ordidx, &not_ordered_list.offidx,
						(uint8_t *)new_list.contents, &new_list.offidx);

				rollback_alloc_rollback(alloc_idx);
				cf_free(temp_mem);
			}
		}
	}
	else {
		p_list_mem->sz = (uint32_t)sz;
	}

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
}

as_val *
list_to_asval(const as_particle *p)
{
	list_mem *p_list_mem = (list_mem *)p;

	as_buffer buf = {
			.capacity = p_list_mem->sz,
			.size = p_list_mem->sz,
			.data = p_list_mem->data
	};

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val = NULL;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

	if (! val) {
		return (as_val *)as_arraylist_new(0, 1);
	}

	return val;
}

uint32_t
list_asval_wire_size(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t sz = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	return sz;
}

uint32_t
list_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_serializer s;
	as_msgpack_init(&s);

	int32_t sz = as_serializer_serialize_presized(&s, val, wire);

	as_serializer_destroy(&s);
	cf_assert(sz > 0, AS_PARTICLE, "list_asval_to_wire() sz %d failed to serialize", sz);

	return (uint32_t)sz;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
list_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	return (uint32_t)sizeof(list_mem) + packed_size;
}

void
list_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	list_mem *p_list_mem = (list_mem *)*pp;

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
	p_list_mem->sz = packed_size;
	memcpy(p_list_mem->data, packed, p_list_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t *
list_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp)
{
	if (flat + sizeof(list_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat list");
		return NULL;
	}

	// Convert temp buffer from disk to data-in-memory.
	const list_flat *p_list_flat = (const list_flat *)flat;

	flat += sizeof(list_flat) + p_list_flat->sz;

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat list");
		return NULL;
	}

	packed_list list;

	if (! packed_list_init(&list, p_list_flat->data, p_list_flat->sz)) {
		cf_warning(AS_PARTICLE, "list_from_flat() invalid packed list");
		return NULL;
	}

	list_mem *p_list_mem = packed_list_pack_mem(&list, NULL);

	p_list_mem->type = p_list_flat->type;
	*pp = (as_particle *)p_list_mem;

	packed_list new_list;
	bool check = packed_list_init_from_particle(&new_list, *pp);

	cf_assert(check, AS_PARTICLE, "list_from_flat() invalid list");

	if (! packed_list_check_order_and_fill_offidx(&new_list)) {
		cf_warning(AS_PARTICLE, "list_from_flat() invalid packed list");
		return NULL;
	}

	return flat;
}

uint32_t
list_flat_size(const as_particle *p)
{
	define_packed_list_particle(list, p, success);
	cf_assert(success, AS_PARTICLE, "list_to_flat() invalid packed list");

	return sizeof(list_flat) + packed_list_mem_sz(&list, false, NULL);
}

uint32_t
list_to_flat(const as_particle *p, uint8_t *flat)
{
	define_packed_list_particle(list, p, success);
	list_flat *p_list_flat = (list_flat *)flat;

	cf_assert(success, AS_PARTICLE, "list_to_flat() invalid packed list");
	p_list_flat->sz = packed_list_mem_sz(&list, false, NULL);

	uint32_t check = packed_list_pack_buf(&list, p_list_flat->data,
			p_list_flat->sz, 0, true);

	cf_assert(check == p_list_flat->sz, AS_PARTICLE, "size mismatch check(%u) != sz(%u), ele_count %u content_sz %u flags 0x%x", check, p_list_flat->sz, list.ele_count, list.content_sz, list.ext_flags);

	// Already wrote the type.

	return sizeof(list_flat) + p_list_flat->sz;
}


//==========================================================
// as_bin particle functions specific to LIST.
//

void
as_bin_particle_list_get_packed_val(const as_bin *b, cdt_payload *packed)
{
	const list_mem *p_list_mem = (const list_mem *)b->particle;

	packed->ptr = (uint8_t *)p_list_mem->data;
	packed->sz = p_list_mem->sz;
}

bool
list_subcontext_by_index(cdt_context *ctx, msgpack_in_vec *val)
{
	int64_t index;
	packed_list list;
	uint32_t uindex;
	uint32_t count32;

	if (! msgpack_get_int64_vec(val, &index)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_index() invalid subcontext");
		return false;
	}

	if (! packed_list_init_from_ctx(&list, ctx)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_index() invalid list");
		return false;
	}

	if (! calc_index_count(index, 1, list.ele_count, &uindex, &count32,
			false)) {
		if (ctx->create_flag_on) {
			if (list.ele_count == 0 && index == -1) {
				uindex = 0;
			}
			else if (index == (int64_t)list.ele_count) {
				uindex = list.ele_count;
			}
			else { // index > list.ele_count
				if (list_is_ordered(&list)) {
					uindex = list.ele_count;
				}
				else if ((ctx->create_ctx_type & AS_CDT_CTX_CREATE_MASK) ==
						AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND) {
					ctx->list_nil_pad = (uint32_t)index - list.ele_count;
					uindex = list.ele_count;
				}
				else {
					cf_warning(AS_PARTICLE, "list_subcontext_by_index() index %ld out of bounds for ele_count %u", index, list.ele_count);
					return false;
				}
			}

			// Assume not top level.
			uint32_t delta_hdr_sz = cdt_hdr_delta_sz(list.ele_count +
					(list_is_ordered(&list) ? 1 : 0), ctx->list_nil_pad + 1);

			ctx->create_sz += delta_hdr_sz + ctx->list_nil_pad;
			ctx->create_triggered = true;
			ctx->create_hdr_ptr = list.packed;

			bool is_dim = ! offset_index_is_null(&list.offidx);
			define_rollback_alloc(alloc_idx, NULL, 1, false);

			cdt_context_list_push(ctx, &list, uindex, alloc_idx, is_dim, false);
			ctx->data_offset += list.packed_sz;
			ctx->data_sz = 0;

			return true;
		}
		else {
			cf_warning(AS_PARTICLE, "list_subcontext_by_index() index %ld out of bounds for ele_count %u", index, list.ele_count);
			return false;
		}
	}

	bool is_dim = ! offset_index_is_null(&list.offidx);
	bool need_idx_mem = cdt_context_list_need_idx_mem(ctx, &list, is_dim);
	define_rollback_alloc(alloc_idx, NULL, 1, false); // for temp indexes
	setup_list_context_full_offidx(full, &list, alloc_idx, need_idx_mem);

	if (! list_full_offset_index_fill_to(full->offidx,
			list_is_ordered(&list) ? list.ele_count : uindex + 1, true)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_index() invalid packed list");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t offset0 = packed_list_find_idx_offset(&list, uindex);

	if (uindex != 0 && offset0 == 0) {
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t offset1 = packed_list_find_idx_offset_continue(&list, uindex + 1,
			uindex, offset0);

	if (offset1 == 0) {
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	cdt_context_list_push(ctx, &list, uindex, alloc_idx, is_dim, need_idx_mem);

	ctx->data_offset += list.packed_sz - list.content_sz + offset0;
	ctx->data_sz = offset1 - offset0;

	return true;
}

bool
list_subcontext_by_rank(cdt_context *ctx, msgpack_in_vec *val)
{
	packed_list list;

	if (! packed_list_init_from_ctx(&list, ctx)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_rank() invalid list");
		return false;
	}

	if (list_is_ordered(&list)) {
		// idx == rank for ordered lists.
		return list_subcontext_by_index(ctx, val);
	}

	int64_t rank;
	uint32_t urank;
	uint32_t count32;

	if (! msgpack_get_int64_vec(val, &rank)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_rank() invalid subcontext");
		return false;
	}

	if (! calc_index_count(rank, 1, list.ele_count, &urank, &count32,
			false)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_rank() rank %ld out of bounds for ele_count %u", rank, list.ele_count);
		return false;
	}

	bool is_dim = ! offset_index_is_null(&list.offidx);
	bool need_idx_mem = cdt_context_list_need_idx_mem(ctx, &list, is_dim);
	define_rollback_alloc(alloc_idx, NULL, 8, false); // for temp indexes
	setup_list_context_full_offidx(full, &list, alloc_idx, need_idx_mem);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_rank() invalid packed list");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	define_build_order_heap_by_range(heap, urank, count32, list.ele_count,
			&list, list_order_heap_cmp_fn, success, alloc_idx);

	if (! success) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_rank() invalid packed list");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t idx = order_index_get(&heap._, heap.filled);
	uint32_t offset0 = offset_index_get_const(full->offidx, idx);
	uint32_t offset1 = offset_index_get_const(full->offidx, idx + 1);

	cdt_context_list_push(ctx, &list, idx, alloc_idx, is_dim, need_idx_mem);

	ctx->data_offset += list.packed_sz - list.content_sz + offset0;
	ctx->data_sz = offset1 - offset0;

	return true;
}

bool
list_subcontext_by_key(cdt_context *ctx, msgpack_in_vec *val)
{
	cf_warning(AS_PARTICLE, "list_subcontext_by_key() Not supported");
	return false;
}

bool
list_subcontext_by_value(cdt_context *ctx, msgpack_in_vec *val)
{
	cdt_payload value;
	packed_list list;

	value.ptr = msgpack_get_ele_vec(val, &value.sz);

	if (value.ptr == NULL) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_value() invalid subcontext");
		return false;
	}

	if (! packed_list_init_from_ctx(&list, ctx)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_value() invalid packed list");
		return false;
	}

	if (list.ele_count == 0) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_value() list is empty");
		return false;
	}

	bool is_dim = ! offset_index_is_null(&list.offidx);
	bool need_idx_mem = cdt_context_list_need_idx_mem(ctx, &list, is_dim);
	define_rollback_alloc(alloc_idx, NULL, 8, false); // for temp indexes
	setup_list_context_full_offidx(full, &list, alloc_idx, need_idx_mem);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_value() invalid packed list");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	uint32_t rank;
	uint32_t count;
	uint32_t idx;

	if (list_is_ordered(&list)) {
		packed_list_find_rank_range_by_value_interval_ordered(&list, &value,
				&value, &rank, &count, false);
		idx = rank;
	}
	else {
		uint64_t idx64;

		if (! packed_list_find_rank_range_by_value_interval_unordered(&list,
				&value, &value, &rank, &count, &idx64, false, false)) {
			rollback_alloc_rollback(alloc_idx);
			return false;
		}

		idx = (uint32_t)idx64;
	}

	if (count == 0) {
		cf_warning(AS_PARTICLE, "list_subcontext_by_value() value not found");
		rollback_alloc_rollback(alloc_idx);
		return false;
	}

	cdt_context_list_push(ctx, &list, idx, alloc_idx, is_dim, need_idx_mem);

	uint32_t offset0 = offset_index_get_const(full->offidx, idx);
	uint32_t offset1 = offset_index_get_const(full->offidx, idx + 1);

	ctx->data_offset += list.packed_sz - list.content_sz + offset0;
	ctx->data_sz = offset1 - offset0;

	return true;
}

void
cdt_context_unwind_list(cdt_context *ctx, cdt_ctx_list_stack_entry *p)
{
	if (ctx->b->particle == ctx->orig) { // no-op happened
		return;
	}

	packed_list list;

	packed_list_init_from_ctx(&list, ctx);

	if (! list_is_ordered(&list)) { // unordered, top level, dim case
		if (! offset_index_is_null(&list.offidx)) {
			uint32_t fill = offset_index_get_filled(&list.offidx);

			if (fill > p->idx / PACKED_LIST_INDEX_STEP) {
				fill = p->idx / PACKED_LIST_INDEX_STEP;

				if (fill == 0) {
					fill = 1;
				}
			}

			offset_index_set_filled(&list.offidx, fill);
		}

		return;
	}

	packed_list orig;

	packed_list_init_from_ctx_orig(&orig, ctx);

	if (! offset_index_is_valid(&orig.offidx)) {
		offset_index_set_ptr(&orig.offidx, p->idx_mem, orig.contents);
	}

	define_rollback_alloc(alloc_idx, NULL, 1, false); // for temp indexes
	setup_list_must_have_full_offidx(full, &orig, alloc_idx);

	if (! list_full_offset_index_fill_to(full->offidx, orig.ele_count, true)) {
		cf_crash(AS_PARTICLE, "cdt_context_unwind_list() invalid packed list");
	}

	uint32_t orig_off = offset_index_get_const(&orig.offidx, p->idx);
	uint32_t rank;
	uint32_t count;
	bool is_toplvl = cdt_context_is_toplvl(ctx);
	uint32_t orig_sz = (p->idx == orig.ele_count) ?
			0 : offset_index_get_delta_const(&orig.offidx, p->idx);
	uint32_t add_sz = orig_sz + list.content_sz - orig.content_sz;

	cdt_payload value = {
			.ptr = list.contents + orig_off,
			.sz = add_sz
	};

	packed_list_find_rank_range_by_value_interval_ordered(&orig, &value, &value,
			&rank, &count, false);

	if (rank == p->idx || rank == p->idx + 1) { // no rank change
		if (! is_toplvl || offset_index_is_null(&list.offidx)) {
			rollback_alloc_rollback(alloc_idx);
			return;
		}

		if (orig.ele_count + 1 == list.ele_count) {
			offset_index_add_ele(&list.offidx, &orig.offidx, p->idx);
		}
		else {
			offset_index_move_ele(&list.offidx, &orig.offidx, p->idx, p->idx);
		}

		rollback_alloc_rollback(alloc_idx);
		return;
	}

	uint8_t *dest_contents = (uint8_t *)list.contents;

	if (rank < p->idx) {
		uint32_t begin_off = offset_index_get_const(&orig.offidx, rank);

		memmove(dest_contents + begin_off, list.contents + orig_off, add_sz);
		memcpy(dest_contents + begin_off + add_sz, orig.contents + begin_off,
				orig_off - begin_off);
	}
	else {
		uint32_t end_off = offset_index_get_const(&orig.offidx, rank);
		uint32_t dest_off = end_off - orig_sz;

		memmove(dest_contents + dest_off, list.contents + orig_off, add_sz);
		memcpy(dest_contents + orig_off, orig.contents + orig_off + orig_sz,
				end_off - orig_off - orig_sz);
	}

	if (! is_toplvl || offset_index_is_null(&list.offidx)) {
		rollback_alloc_rollback(alloc_idx);
		return;
	}

	if (orig.ele_count + 1 == list.ele_count) {
		offset_index_add_ele(&list.offidx, &orig.offidx, rank);
	}
	else {
		offset_index_move_ele(&list.offidx, &orig.offidx, p->idx, rank);
	}

	rollback_alloc_rollback(alloc_idx);
}

uint8_t
list_get_ctx_flags(bool is_ordered, bool is_toplvl)
{
	if (is_toplvl) {
		return is_ordered ?
				AS_PACKED_LIST_FLAG_ORDERED | AS_PACKED_LIST_FLAG_FULLOFF_IDX :
				AS_PACKED_LIST_FLAG_NONE | AS_PACKED_LIST_FLAG_OFF_IDX;
	}

	return is_ordered ? AS_PACKED_LIST_FLAG_ORDERED : AS_PACKED_LIST_FLAG_NONE;
}


//==========================================================
// Local helpers.
//

static inline bool
is_list_type(uint8_t type)
{
	return type == AS_PARTICLE_TYPE_LIST;
}

static inline bool
flags_is_ordered(uint8_t flags)
{
	return (flags & AS_PACKED_LIST_FLAG_ORDERED) != 0;
}

static inline bool
list_is_ordered(const packed_list *list)
{
	return flags_is_ordered(list->ext_flags);
}

static inline bool
mod_flags_is_unique(uint64_t flags)
{
	return (flags & AS_CDT_LIST_ADD_UNIQUE) != 0;
}

static inline bool
mod_flags_is_bounded(uint64_t flags)
{
	return (flags & AS_CDT_LIST_INSERT_BOUNDED) != 0;
}

static inline bool
mod_flags_is_do_partial(uint64_t flags)
{
	return (flags & (AS_CDT_LIST_NO_FAIL | AS_CDT_LIST_DO_PARTIAL)) ==
			(AS_CDT_LIST_NO_FAIL | AS_CDT_LIST_DO_PARTIAL);
}

static inline bool
mod_flags_is_no_fail(uint64_t flags)
{
	return (flags & AS_CDT_LIST_NO_FAIL) != 0;
}

static inline int
mod_flags_return_exists(uint64_t flags)
{
	if (mod_flags_is_no_fail(flags)) {
		return AS_OK;
	}

	return -AS_ERR_ELEMENT_EXISTS;
}

static inline uint8_t
strip_ext_flags(uint8_t flags)
{
	return flags & AS_PACKED_LIST_FLAG_ORDERED;
}

static inline uint8_t
get_ext_flags(bool ordered)
{
	return ordered ?
			(AS_PACKED_LIST_FLAG_ORDERED | AS_PACKED_LIST_FLAG_FULLOFF_IDX) :
			AS_PACKED_LIST_FLAG_OFF_IDX;
}

static uint32_t
list_calc_ext_content_sz(uint32_t ele_count, uint32_t content_sz, bool ordered)
{
	if (ele_count <= 1) {
		return 0;
	}

	offset_index offidx;

	if (! ordered) {
		list_partial_offset_index_init(&offidx, NULL, ele_count, NULL,
				content_sz);
	}
	else {
		offset_index_init(&offidx, NULL, ele_count, NULL, content_sz);
	}

	return offset_index_size(&offidx);
}

static uint32_t
list_pack_header(uint8_t *buf, uint32_t ele_count)
{
	as_packer pk = {
			.buffer = buf,
			.capacity = INT_MAX,
	};

	if (as_pack_list_header(&pk, ele_count) != 0) {
		cf_crash(AS_PARTICLE, "as_pack_list_header() unexpected failure");
	}

	return pk.offset;
}

static void
list_pack_empty_index(as_packer *pk, uint32_t ele_count,
		const uint8_t *contents, uint32_t content_sz, bool is_ordered)
{
	offset_index offidx;

	if (is_ordered) {
		offset_index_init(&offidx, pk->buffer + pk->offset, ele_count, contents,
				content_sz);
	}
	else {
		list_partial_offset_index_init(&offidx, pk->buffer + pk->offset,
				ele_count, contents, content_sz);
	}

	offset_index_set_filled(&offidx, 1);
	pk->offset += offset_index_size(&offidx);
}

//------------------------------------------------
// as_bin
//

void
as_bin_set_unordered_empty_list(as_bin *b, rollback_alloc *alloc_buf)
{
	b->particle = list_simple_create_from_buf(alloc_buf, 0, NULL, 0);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
}

static void
as_bin_set_ordered_empty_list(as_bin *b, rollback_alloc *alloc_buf)
{
	b->particle = list_simple_create_from_buf(alloc_buf, 1,
			list_ordered_empty.data + 1, LIST_EMPTY_FLAGED_SIZE - 1);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
}

//------------------------------------------------
// cdt_context
//

static inline void
cdt_context_set_empty_list(cdt_context *ctx, bool is_ordered)
{
	if (cdt_context_is_toplvl(ctx) && ! ctx->create_triggered) {
		if (is_ordered) {
			as_bin_set_ordered_empty_list(ctx->b, ctx->alloc_buf);
		}
		else {
			as_bin_set_unordered_empty_list(ctx->b, ctx->alloc_buf);
		}
	}
	else {
		list_setup_bin_ctx(ctx, is_ordered ?
				AS_PACKED_LIST_FLAG_ORDERED : AS_PACKED_LIST_FLAG_NONE,
				0, 0, 0, NULL, NULL);
	}
}

static inline void
cdt_context_use_static_list_if_notinuse(cdt_context *ctx, uint64_t create_flags)
{
	if (ctx->create_triggered) {
		ctx->create_flags = create_flags;
		return;
	}

	if (! as_bin_is_live(ctx->b)) {
		cf_assert(ctx->data_sz == 0, AS_PARTICLE, "invalid state");
		ctx->b->particle = flags_is_ordered(create_flags) ?
				(as_particle *)&list_ordered_empty :
				(as_particle *)&list_mem_empty;
		as_bin_state_set_from_type(ctx->b, AS_PARTICLE_TYPE_LIST);
	}
}

static inline bool
cdt_context_list_need_idx_mem(const cdt_context *ctx, const packed_list *list,
		bool is_dim)
{
	return ! (cdt_context_is_toplvl(ctx) && is_dim) && list_is_ordered(list) &&
			list->ele_count > 1;
}

static inline void
cdt_context_list_push(cdt_context *ctx, const packed_list *list, uint32_t idx,
		rollback_alloc *alloc_idx, bool is_dim, bool need_idx_mem)
{
	if (cdt_context_is_modify(ctx)) {
		if (list_is_ordered(list)) {
			if (list->ele_count > 1 ||
					(list->ele_count == 1 && ctx->create_triggered)) {
				if (need_idx_mem) {
					cdt_context_push(ctx, idx, list_full_offidx_p(list)->_.ptr,
							AS_LIST);

					return;
				}
				else {
					cdt_context_push(ctx, idx, NULL, AS_LIST);

					if (cdt_context_is_toplvl(ctx)) {
						ctx->top_content_sz = list->content_sz;
						ctx->top_content_off = list->contents - list->packed;
						ctx->top_ele_count = list->ele_count;
					}
				}
			}
		}
		else if (cdt_context_is_toplvl(ctx)) {
			if (is_dim) {
				cdt_context_push(ctx, idx, NULL, AS_LIST);
			}
			else if (ctx->create_triggered) {
				uint32_t count = list_offset_partial_index_count(
						list->ele_count);
				uint32_t new_count = list_offset_partial_index_count(
						list->ele_count + ctx->list_nil_pad + 1);

				if (count == 0 && new_count != 0) {
					ctx->top_content_sz = list->content_sz;
					ctx->top_content_off = list->contents - list->packed;
					ctx->top_ele_count = list->ele_count;
				}
			}
		}
	}

	rollback_alloc_rollback(alloc_idx);
}

static inline void
cdt_context_list_handle_possible_noop(cdt_context *ctx)
{
	if (ctx->create_triggered) {
		list_setup_bin_ctx(ctx, ctx->create_flags, 0, 0, 0, NULL, NULL);
	}
}

//----------------------------------------------------------
// packed_list
//

static bool
packed_list_init(packed_list *list, const uint8_t *buf, uint32_t sz)
{
	list->packed = buf;
	list->packed_sz = sz;

	list->ele_count = 0;
	list->ext_flags = 0;
	list->contents = NULL;

	return packed_list_unpack_hdridx(list);
}

static inline bool
packed_list_init_from_particle(packed_list *list, const as_particle *p)
{
	const list_mem *p_list_mem = (const list_mem *)p;
	return packed_list_init(list, p_list_mem->data, p_list_mem->sz);
}

static bool
packed_list_init_from_bin(packed_list *list, const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);
	cf_assert(is_list_type(type), AS_PARTICLE, "packed_list_init_from_bin() invalid type %d", type);
	return packed_list_init_from_particle(list, b->particle);
}

static bool
packed_list_init_from_ctx(packed_list *list, const cdt_context *ctx)
{
	if (cdt_context_is_toplvl(ctx)) {
		return packed_list_init_from_bin(list, ctx->b);
	}

	const cdt_mem *p_cdt_mem = (const cdt_mem *)ctx->b->particle;

	return packed_list_init(list,
			p_cdt_mem->data + ctx->data_offset + ctx->delta_off,
			ctx->data_sz + ctx->delta_sz);
}

static inline bool
packed_list_init_from_com(packed_list *list, cdt_op_mem *com)
{
	const cdt_context *ctx = &com->ctx;

	if (ctx->create_triggered) {
		list->ele_count = 0;
		list->contents = NULL;

		if (flags_is_ordered(ctx->create_flags)) {
			list->packed = list_ordered_empty.data;
			list->packed_sz = list_ordered_empty.sz;
		}
		else {
			list->packed = list_mem_empty.data;
			list->packed_sz = list_mem_empty.sz;
		}

		return packed_list_unpack_hdridx(list);
	}

	return packed_list_init_from_ctx(list, &com->ctx);
}

static bool
packed_list_init_from_ctx_orig(packed_list *list, const cdt_context *ctx)
{
	if (ctx->data_sz == 0) {
		return packed_list_init_from_particle(list, ctx->orig);
	}

	const cdt_mem *p_cdt_mem = (const cdt_mem *)ctx->orig;

	return packed_list_init(list, p_cdt_mem->data + ctx->data_offset,
			ctx->data_sz);
}

static bool
packed_list_unpack_hdridx(packed_list *list)
{
	if (list->packed_sz == 0) {
		list->ext_flags = 0;
		return false;
	}

	msgpack_in mp = {
			.buf = list->packed,
			.buf_sz = list->packed_sz
	};

	if (! msgpack_get_list_ele_count(&mp, &list->ele_count)) {
		return false;
	}

	if (list->ele_count != 0 && msgpack_peek_is_ext(&mp)) {
		msgpack_ext ext;

		if (! msgpack_get_ext(&mp, &ext)) {
			return false;
		}

		list->ext_flags = ext.type;
		list->ele_count--;
		list->contents = list->packed + mp.offset;
		list->content_sz = list->packed_sz - mp.offset;

		if (list_is_ordered(list)) {
			offset_index_init(&list->offidx, NULL, list->ele_count,
					list->contents, list->content_sz);
		}
		else {
			list_partial_offset_index_init(&list->offidx, NULL, list->ele_count,
					list->contents, list->content_sz);
		}

		offset_index_init(&list->full_offidx, NULL, list->ele_count,
				list->contents, list->content_sz);

		if (ext.size >= offset_index_size(&list->offidx)) {
			offset_index_set_ptr(&list->offidx, (uint8_t *)ext.data,
					list->packed + mp.offset);
		}
	}
	else {
		list->contents = list->packed + mp.offset;
		list->content_sz = list->packed_sz - mp.offset;
		list->ext_flags = 0;

		list_partial_offset_index_init(&list->offidx, NULL, list->ele_count,
				list->contents, list->content_sz);
		offset_index_init(&list->full_offidx, NULL, list->ele_count,
				list->contents, list->content_sz);
	}

	return true;
}

static void
packed_list_partial_offidx_update(const packed_list *list)
{
	if (list_is_ordered(list) || ! offset_index_is_valid(&list->full_offidx) ||
			offset_index_is_null(&list->offidx)) {
		return;
	}

	offset_index *full = (offset_index *)&list->full_offidx;
	offset_index *part = (offset_index *)&list->offidx;
	uint32_t filled = offset_index_get_filled(part);
	uint32_t max =
			list_offset_partial_index_count(offset_index_get_filled(full));

	if (filled >= max) {
		return;
	}

	for (uint32_t j = filled; j < max; j++) {
		uint32_t off = offset_index_get_const(full, j * PACKED_LIST_INDEX_STEP);
		offset_index_set(part, j, off);
	}

	offset_index_set_filled(part, max);
}

static void
packed_list_find_by_value_ordered(const packed_list *list,
		const cdt_payload *value, order_index_find *find)
{
	if (list->ele_count == 0) {
		find->found = false;
		find->result = 0;
		return;
	}

	offset_index *offidx = list_full_offidx_p(list);

	cf_assert(offset_index_is_full(offidx), AS_PARTICLE, "invalid offidx");
	find->count = list->ele_count - find->start;

	order_index_find_rank_by_value(NULL, value, offidx, find, false);
}

static uint32_t
packed_list_find_idx_offset(const packed_list *list, uint32_t index)
{
	if (index == 0) {
		return 0;
	}

	if (list_is_ordered(list)) {
		cf_assert(offset_index_is_valid(&list->offidx), AS_PARTICLE, "invalid offidx");

		offset_index *offidx = (offset_index *)&list->offidx;

		if (! list_full_offset_index_fill_to(offidx, index, false)) {
			return 0;
		}

		return offset_index_get_const(offidx, index);
	}
	else if (offset_index_is_valid(&list->full_offidx) &&
			index < offset_index_get_filled(&list->full_offidx)) {
		return offset_index_get_const(&list->full_offidx, index);
	}

	msgpack_in mp = {
			.buf = list->contents,
			.buf_sz = list->content_sz
	};

	uint32_t steps = index;

	if (offset_index_is_valid(&list->offidx)) {
		uint32_t pt_idx = index / PACKED_LIST_INDEX_STEP;
		uint32_t pt_filled = offset_index_get_filled(&list->offidx);

		if (pt_idx >= pt_filled) {
			cf_assert(pt_filled != 0, AS_PARTICLE, "packed_list_op_find_idx_offset() filled is zero");
			pt_idx = pt_filled - 1;
		}

		mp.offset = offset_index_get_const(&list->offidx, pt_idx);
		steps -= pt_idx * PACKED_LIST_INDEX_STEP;

		offset_index *offidx = (offset_index *)&list->offidx; // mutable struct variable
		uint32_t blocks = steps / PACKED_LIST_INDEX_STEP;

		steps %= PACKED_LIST_INDEX_STEP;

		for (uint32_t i = 0; i < blocks; i++) {
			if (msgpack_sz_rep(&mp, PACKED_LIST_INDEX_STEP) == 0) {
				return 0;
			}

			pt_idx++;
			offset_index_set_next(offidx, pt_idx, mp.offset);
		}
	}

	if (steps != 0 && msgpack_sz_rep(&mp, steps) == 0) {
		return 0;
	}

	return mp.offset;
}

static uint32_t
packed_list_find_idx_offset_continue(const packed_list *list, uint32_t index,
		uint32_t index0, uint32_t offset0)
{
	if (list_is_ordered(list)) {
		return packed_list_find_idx_offset(list, index);
	}
	else if (offset_index_is_valid(&list->full_offidx) &&
			index < offset_index_get_filled(&list->full_offidx)) {
		return offset_index_get_const(&list->full_offidx, index);
	}

	msgpack_in mp = {
			.buf = list->contents,
			.buf_sz = list->content_sz,
			.offset = offset0
	};

	uint32_t steps = index - index0;

	if (offset_index_is_valid(&list->offidx)) {
		uint32_t pt_idx0 = index0 / PACKED_LIST_INDEX_STEP;
		uint32_t pt_idx = index / PACKED_LIST_INDEX_STEP;
		uint32_t pt_filled = offset_index_get_filled(&list->offidx);

		if (pt_idx0 != pt_idx) {
			if (pt_idx0 < pt_filled - 1) {
				return packed_list_find_idx_offset(list, index);
			}

			uint32_t mod0 = index0 % PACKED_LIST_INDEX_STEP;
			offset_index *offidx = (offset_index *)&list->offidx;

			if (mod0 != 0) {
				uint32_t rep = PACKED_LIST_INDEX_STEP - mod0;

				if (msgpack_sz_rep(&mp, rep) == 0) {
					return 0;
				}

				steps -= rep;
				pt_idx0++;
				offset_index_set_next(offidx, pt_idx0, mp.offset);
			}

			uint32_t blocks = pt_idx - pt_idx0;

			for (uint32_t i = 0; i < blocks; i++) {
				if (msgpack_sz_rep(&mp, PACKED_LIST_INDEX_STEP) == 0) {
					return 0;
				}

				pt_idx0++;
				offset_index_set_next(offidx, pt_idx0, mp.offset);
			}

			steps -= blocks * PACKED_LIST_INDEX_STEP;
		}
	}

	if (steps != 0 && msgpack_sz_rep(&mp, steps) == 0) {
		return 0;
	}

	return mp.offset;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value:
//  [value_start, value_start].
static void
packed_list_find_rank_range_by_value_interval_ordered(const packed_list *list,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank_r, uint32_t *count_r, bool is_multi)
{
	cf_assert(offset_index_is_valid(list_full_offidx_p(list)), AS_PARTICLE, "packed_list_find_rank_range_by_value_interval_ordered() invalid full offset_index");
	cf_assert(value_end, AS_PARTICLE, "value_end == NULL");

	order_index_find find = {
			.target = 0
	};

	packed_list_find_by_value_ordered(list, value_start, &find);
	*rank_r = find.result;

	if (value_end == value_start) {
		if (! find.found) {
			*count_r = 0;
		}
		else if (is_multi) {
			find.start = find.result + 1;
			find.target = list->ele_count;
			packed_list_find_by_value_ordered(list, value_start, &find);

			if (find.found) {
				*count_r = find.result - *rank_r;
			}
			else {
				*count_r = 1;
			}
		}
		else {
			*count_r = 1;
		}

		return;
	}

	if (! value_end->ptr) {
		*count_r = list->ele_count - *rank_r;
		return;
	}

	msgpack_in mp_start = {
			.buf = value_start->ptr,
			.buf_sz = value_start->sz
	};

	msgpack_in mp_end = {
			.buf = value_end->ptr,
			.buf_sz = value_end->sz
	};

	msgpack_cmp_type cmp = msgpack_cmp_peek(&mp_start, &mp_end);

	if (cmp == MSGPACK_CMP_GREATER || cmp == MSGPACK_CMP_EQUAL) {
		*count_r = 0;
		return;
	}

	find.start = find.result;
	packed_list_find_by_value_ordered(list, value_end, &find);
	*count_r = find.result - *rank_r;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value:
//  [value_start, value_start].
// mask_val is a mask for is_multi case and a uint64_t[1] value for ! is_multi.
static bool
packed_list_find_rank_range_by_value_interval_unordered(const packed_list *list,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank, uint32_t *count, uint64_t *mask_val, bool inverted,
		bool is_multi)
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

	offset_index *full_offidx = list_full_offidx_p(list);

	if (offset_index_is_null(full_offidx)) {
		full_offidx = NULL;
	}

	*rank = 0;
	*count = 0;

	msgpack_in mp = {
			.buf = list->contents,
			.buf_sz = list->content_sz
	};

	for (uint32_t i = 0; i < list->ele_count; i++) {
		uint32_t value_offset = mp.offset; // save for pk_end

		mp_start.offset = 0; // reset

		msgpack_cmp_type cmp_start = msgpack_cmp(&mp, &mp_start);

		if (full_offidx) {
			offset_index_set(full_offidx, i + 1, mp.offset);
		}

		if (cmp_start == MSGPACK_CMP_ERROR) {
			cf_warning(AS_PARTICLE, "packed_list_op_find_rank_range_by_value_interval_unordered() invalid packed list at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_CMP_LESS) {
			(*rank)++;

			if (inverted) {
				if (mask_val) {
					cdt_idx_mask_set(mask_val, i);
				}

				(*count)++;
			}
		}
		else if (value_start != value_end) {
			msgpack_cmp_type cmp_end = MSGPACK_CMP_LESS;

			// NULL value_end means largest possible value.
			if (value_end->ptr) {
				mp.offset = value_offset;
				mp_end.offset = 0;
				cmp_end = msgpack_cmp(&mp, &mp_end);
			}

			if ((cmp_end == MSGPACK_CMP_LESS && ! inverted) ||
					((cmp_end == MSGPACK_CMP_GREATER ||
							cmp_end == MSGPACK_CMP_EQUAL) && inverted)) {
				if (mask_val) {
					cdt_idx_mask_set(mask_val, i);
				}

				(*count)++;
			}
		}
		// Single value case.
		else if (cmp_start == MSGPACK_CMP_EQUAL) {
			if (is_multi) {
				if (! inverted) {
					if (mask_val) {
						cdt_idx_mask_set(mask_val, i);
					}

					(*count)++;
				}
			}
			else if (*count == 0) {
				if (mask_val) {
					*mask_val = i;
				}

				(*count)++;
			}
		}
		else if (inverted && is_multi) {
			if (mask_val) {
				cdt_idx_mask_set(mask_val, i);
			}

			(*count)++;
		}
	}

	if (full_offidx) {
		offset_index_set_filled(full_offidx, list->ele_count);
	}

	return true;
}

static uint32_t
packed_list_mem_sz(const packed_list *list, bool has_ext,
		uint32_t *ext_content_sz_r)
{
	bool ordered = list_is_ordered(list);
	uint32_t ext_cont_sz = 0;

	if (has_ext) {
		ext_cont_sz = list_calc_ext_content_sz(list->ele_count,
				list->content_sz, ordered);

		if (ext_content_sz_r) {
			*ext_content_sz_r = ext_cont_sz;
		}
	}
	else if (! ordered) {
		return as_pack_list_header_get_size(list->ele_count) + list->content_sz;
	}

	if (! ordered && ext_cont_sz == 0) {
		return as_pack_list_header_get_size(list->ele_count) + list->content_sz;
	}

	return as_pack_list_header_get_size(list->ele_count + 1) +
			as_pack_ext_header_get_size(ext_cont_sz) + ext_cont_sz +
			list->content_sz;
}

static uint32_t
packed_list_pack_buf(const packed_list *list, uint8_t *buf, uint32_t sz,
		uint32_t ext_content_sz, bool strip_flags)
{
	as_packer pk = {
			.buffer = buf,
			.capacity = sz
	};

	bool ordered = list_is_ordered(list);

	if (ordered || ext_content_sz != 0) {
		as_pack_list_header(&pk, list->ele_count + 1);
		as_pack_ext_header(&pk, ext_content_sz, strip_flags ?
				strip_ext_flags(list->ext_flags) : get_ext_flags(ordered));

		if (ext_content_sz != 0) {
			list_pack_empty_index(&pk, list->ele_count, NULL, list->content_sz,
					ordered);
		}
	}
	else {
		as_pack_list_header(&pk, list->ele_count);
	}

	packed_list_content_pack(list, &pk);

	return pk.offset;
}

static list_mem *
packed_list_pack_mem(const packed_list *list, list_mem *p_list_mem)
{
	uint32_t ext_content_sz = 0;
	uint32_t sz = packed_list_mem_sz(list, true, &ext_content_sz);

	if (! p_list_mem) {
		p_list_mem = cf_malloc_ns(sizeof(list_mem) + sz);
	}

	p_list_mem->sz = sz;
	packed_list_pack_buf(list, p_list_mem->data, sz, ext_content_sz, false);

	return p_list_mem;
}

static void
packed_list_content_pack(const packed_list *list, as_packer *pk)
{
	uint8_t *ptr = pk->buffer + pk->offset;

	memcpy(ptr, list->contents, list->content_sz);
	pk->offset += list->content_sz;
}

static int
packed_list_remove_by_idx(const packed_list *list, cdt_op_mem *com,
		const uint64_t rm_idx, uint32_t *rm_sz)
{
	define_packed_list_op(op, list);

	if (! packed_list_op_remove(&op, rm_idx, 1)) {
		cf_warning(AS_PARTICLE, "packed_list_remove_by_idx() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	if (op.new_ele_count == 0) {
		cdt_context_set_empty_list(&com->ctx, list_is_ordered(list));
	}
	else {
		uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
				op.new_content_sz, op.new_ele_count, rm_idx, &list->offidx,
				NULL);

		ptr += packed_list_op_write_seg1(&op, ptr);
		packed_list_op_write_seg2(&op, ptr);
	}

	*rm_sz = list->content_sz - op.new_content_sz;

	return AS_OK;
}

static int
packed_list_remove_by_mask(const packed_list *list, cdt_op_mem *com,
		const uint64_t *rm_mask, uint32_t rm_count, uint32_t *rm_sz)
{
	offset_index *full_offidx = list_full_offidx_p(list);

	*rm_sz = cdt_idx_mask_get_content_sz(rm_mask, rm_count, full_offidx);

	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
			list->content_sz - *rm_sz, list->ele_count - rm_count, 0, NULL,
			&new_offidx);

	ptr = cdt_idx_mask_write_eles(rm_mask, rm_count, full_offidx, ptr, true);

	if (! offset_index_is_null(&new_offidx)) {
		list_offset_index_rm_mask_cpy(&new_offidx, full_offidx, rm_mask,
				rm_count);
	}

	return AS_OK;
}

// Assumes index/count(non-zero) is surrounded by other elements.
static int
packed_list_trim(const packed_list *list, cdt_op_mem *com, uint32_t index,
		uint32_t count)
{
	cdt_result_data *result = &com->result;
	cf_assert(result->is_multi, AS_PARTICLE, "packed_list_trim() required to be a multi op");

	uint32_t rm_count = list->ele_count - count;
	uint32_t index1 = index + count;
	uint32_t offset0 = packed_list_find_idx_offset(list, index);
	uint32_t offset1 = packed_list_find_idx_offset_continue(list, index1,
			index, offset0);
	uint32_t content_sz = offset1 - offset0;

	if ((offset0 == 0 && index != 0) || offset1 == 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (cdt_op_is_modify(com)) {
		uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
				content_sz, count, 0, &list->offidx, NULL);

		memcpy(ptr, list->contents + offset0, content_sz);
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

		cdt_container_builder_add_int_range(&builder, 0, index,
				list->ele_count, is_rev);
		cdt_container_builder_add_int_range(&builder, index1,
				list->ele_count - index1, list->ele_count, is_rev);
		cdt_container_builder_set_result(&builder, result);
		break;
	}
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK: {
		define_int_list_builder(builder, result->alloc, rm_count);

		if (list_is_ordered(list)) {
			cdt_container_builder_add_int_range(&builder, 0, index,
					list->ele_count, result->type == RESULT_TYPE_REVRANK);
			cdt_container_builder_add_int_range(&builder, index + count,
					rm_count - index, list->ele_count,
					result->type == RESULT_TYPE_REVRANK);
			cdt_container_builder_set_result(&builder, result);
			break;
		}

		msgpack_in mp = {
				.buf = list->contents,
				.buf_sz = list->content_sz
		};

		if (! packed_list_builder_add_ranks_by_range(list, &builder, &mp, index,
				result->type == RESULT_TYPE_REVRANK)) {
			cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
			return -AS_ERR_PARAMETER;
		}

		mp.offset = offset1;

		if (! packed_list_builder_add_ranks_by_range(list, &builder, &mp,
				rm_count - index, result->type == RESULT_TYPE_REVRANK)) {
			cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
			return -AS_ERR_PARAMETER;
		}

		cdt_container_builder_set_result(&builder, result);
		break;
	}
	case RESULT_TYPE_VALUE: {
		uint32_t tail_sz = list->content_sz - offset1;
		list_mem *p_list_mem = list_create(result->alloc, rm_count,
				offset0 + tail_sz);

		cf_assert(p_list_mem, AS_PARTICLE, "NULL list");
		result->result->particle = (as_particle *)p_list_mem;

		uint8_t *ptr = p_list_mem->data;
		uint32_t hdr_sz = list_pack_header(ptr, rm_count);

		ptr += hdr_sz;
		memcpy(ptr, list->contents, offset0);
		ptr += offset0;
		memcpy(ptr, list->contents + offset1, tail_sz);

		as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_list_trim() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_index_range(const packed_list *list, cdt_op_mem *com,
		int64_t index, uint64_t count)
{
	cdt_result_data *result = &com->result;
	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, list->ele_count, &uindex, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() index %ld out of bounds for ele_count %u", index, list->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (result_data_is_return_index_range(result) ||
				result_data_is_return_rank_range(result)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() result_type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		if (count32 == 0) {
			// Reduce to remove all.
			uindex = 0;
			count32 = list->ele_count;
		}
		else if (uindex == 0) {
			// Reduce to remove tail section.
			uindex = count32;
			count32 = list->ele_count - count32;
		}
		else if (uindex + count32 >= list->ele_count) {
			// Reduce to remove head section.
			count32 = uindex;
			uindex = 0;
		}
		else {
			setup_list_must_have_full_offidx(full, list, com->alloc_idx);

			return packed_list_trim(list, com, uindex, count32);
		}
	}

	if (count32 == 0) {
		if (! list_result_data_set_not_found(result, uindex)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		return AS_OK;
	}

	define_packed_list_op(op, list);
	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (! packed_list_op_remove(&op, uindex, count32)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	if (cdt_op_is_modify(com)) {
		if (op.new_ele_count == 0) {
			cdt_context_set_empty_list(&com->ctx, list_is_ordered(list));
		}
		else {
			uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
					op.new_content_sz, op.new_ele_count, uindex, &list->offidx,
					NULL);

			ptr += packed_list_op_write_seg1(&op, ptr);
			packed_list_op_write_seg2(&op, ptr);
		}
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
		return result_data_set_index_rank_count(result, uindex, count32,
				list->ele_count);
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK: {
		if (op.new_ele_count == 0) {
			return result_data_set_index_rank_count(result, 0, count32,
					list->ele_count);
		}

		if (! result->is_multi) {
			uint32_t rank;

			if (list_is_ordered(list)) {
				rank = uindex;
			}
			else {
				uint32_t rcount;

				cdt_payload value = {
						.ptr = list->contents + op.seg1_sz,
						.sz = list->content_sz - op.new_content_sz
				};

				if (! packed_list_find_rank_range_by_value_interval_unordered(
						list, &value, &value, &rank, &rcount, NULL, false,
						false)) {
					return -AS_ERR_PARAMETER;
				}
			}

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = list->ele_count - rank - 1;
			}

			as_bin_set_int(result->result, (int64_t)rank);
			break;
		}

		msgpack_in mp = {
				.buf = list->contents + op.seg1_sz,
				.buf_sz = list->content_sz - op.new_content_sz
		};

		uint32_t rm_count = list->ele_count - op.new_ele_count;
		define_int_list_builder(builder, result->alloc, rm_count);

		if (list_is_ordered(list)) {
			cdt_container_builder_add_int_range(&builder, uindex, count32,
					list->ele_count, result->type == RESULT_TYPE_REVRANK);
		}
		else if (! packed_list_builder_add_ranks_by_range(list, &builder, &mp,
				rm_count, result->type == RESULT_TYPE_REVRANK)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() invalid list");
			return -AS_ERR_PARAMETER;
		}

		cdt_container_builder_set_result(&builder, result);
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, list->ele_count - op.new_ele_count);
		break;
	case RESULT_TYPE_VALUE: {
		const uint8_t *result_ptr = list->contents + op.seg1_sz;
		uint32_t end = (op.seg2_sz != 0) ? op.seg2_offset : list->content_sz;
		uint32_t result_sz = end - op.seg1_sz;
		uint32_t result_count = list->ele_count - op.new_ele_count;

		if (result->is_multi) {
			result->result->particle =
					list_simple_create_from_buf(result->alloc,
							result_count, result_ptr, result_sz);

			if (! result->result->particle) {
				return -AS_ERR_UNKNOWN;
			}

			as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);
		}
		else if (result_sz != 0) {
			cf_assert(count32 <= 1, AS_PARTICLE, "packed_list_get_remove_by_index_range() result must be list for count > 1");
			as_bin_particle_alloc_from_msgpack(result->result, result_ptr,
					result_sz);
		}
		// else - leave result bin empty because result_size is 0.
		break;
	}
	case RESULT_TYPE_REVINDEX_RANGE:
		if (result->type == RESULT_TYPE_REVINDEX_RANGE) {
			uindex = list->ele_count - uindex - count32;
		}
		// no break
	case RESULT_TYPE_INDEX_RANGE:
		result_data_set_list_int2x(result, uindex, count32);
		break;
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		if (list_is_ordered(list)) {
			return result_data_set_range(result, uindex, count32,
					list->ele_count);
		}
		// no break
	default:
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_list_get_remove_by_index_range");
		cf_crash(AS_PARTICLE, "packed_list_get_remove_by_index_range: index %ld count %lu", index, count);
	}
#endif

	return AS_OK;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_list_get_remove_by_value_interval(const packed_list *list,
		cdt_op_mem *com, const cdt_payload *value_start,
		const cdt_payload *value_end)
{
	cdt_result_data *result = &com->result;
	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_value_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	uint32_t rank;
	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (list_is_ordered(list)) {
		uint32_t count;

		if (! list_full_offset_index_fill_all(full->offidx)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_value_interval() invalid list");
			return -AS_ERR_PARAMETER;
		}

		packed_list_find_rank_range_by_value_interval_ordered(list, value_start,
				value_end, &rank, &count, result->is_multi);

		if (count == 0 && ! result->is_multi) {
			if (! list_result_data_set_not_found(result, 0)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			return AS_OK;
		}

		return packed_list_get_remove_by_index_range(list, com, (int64_t)rank,
				(uint64_t)count);
	}

	uint32_t rm_count;
	define_cdt_idx_mask(rm_mask, result->is_multi ? list->ele_count : 1,
			com->alloc_idx);

	if (! packed_list_find_rank_range_by_value_interval_unordered(list,
			value_start, value_end, &rank, &rm_count, rm_mask, inverted,
			result->is_multi)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (cdt_op_is_modify(com)) {
		if (rm_count == list->ele_count) {
			cdt_context_set_empty_list(&com->ctx, false);
		}
		else if (rm_count != 0) {
			int ret;

			if (result->is_multi) {
				ret = packed_list_remove_by_mask(list, com, rm_mask, rm_count,
						&rm_sz);
			}
			else {
				// rm_mask[0] is an idx for single value finds.
				ret = packed_list_remove_by_idx(list, com, rm_mask[0], &rm_sz);
			}

			if (ret != AS_OK) {
				return ret;
			}
		}
		else {
			packed_list_partial_offidx_update(list);
		}
	}
	else {
		packed_list_partial_offidx_update(list);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
	case RESULT_TYPE_COUNT:
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK_RANGE:
	case RESULT_TYPE_RANK_RANGE:
		return result_data_set_range(result, rank, inverted ?
				list->ele_count - rm_count : rm_count, list->ele_count);
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
		if (result->is_multi) {
			result_data_set_int_list_by_mask(result, rm_mask, rm_count,
					list->ele_count);
		}
		else {
			result_data_set_index_rank_count(result, rm_mask[0], rm_count,
					list->ele_count);
		}
		break;
	case RESULT_TYPE_VALUE:
		if (result->is_multi) {
			list_result_data_set_values_by_mask(result, rm_mask,
					list_full_offidx_p(list), rm_count, rm_sz);
		}
		else {
			define_order_index2(rm_idx, list->ele_count, 1, com->alloc_idx);

			order_index_set(&rm_idx, 0, rm_mask[0]);
			list_result_data_set_values_by_ordidx(result, &rm_idx, full->offidx,
					rm_count, rm_sz);
		}
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_value_interval() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_rank_range(const packed_list *list, cdt_op_mem *com,
		int64_t rank, uint64_t count)
{
	cdt_result_data *result = &com->result;
	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() INVERTED flag not supported for single result ops");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (list_is_ordered(list)) {
		// idx == rank for ordered lists.
		return packed_list_get_remove_by_index_range(list, com, rank, count);
	}

	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, list->ele_count, &urank, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() rank %u out of bounds for ele_count %u", urank, list->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	define_build_order_heap_by_range(heap, urank, count32, list->ele_count,
			list, list_order_heap_cmp_fn, success, com->alloc_idx);

	if (! success) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_count = inverted ? list->ele_count - count32 : count32;

	if (rm_count == 0) {
		if (! list_result_data_set_not_found(result, urank)) {
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		packed_list_partial_offidx_update(list);

		return AS_OK;
	}

	define_cdt_idx_mask(rm_mask, list->ele_count, com->alloc_idx);
	order_index ret_idx;

	cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
			inverted);

	if (inverted) {
		if (result_data_is_return_rank_range(result)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() result_type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}

		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() singe result type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_OP_NOT_APPLICABLE;
		}
	}
	else {
		order_index_init_ref(&ret_idx, &heap._, heap.filled, rm_count);
	}

	uint32_t rm_sz = 0;

	if (cdt_op_is_modify(com)) {
		int ret = packed_list_remove_by_mask(list, com, rm_mask, rm_count,
				&rm_sz);

		if (ret != AS_OK) {
			return ret;
		}
	}
	else {
		packed_list_partial_offidx_update(list);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
	case RESULT_TYPE_COUNT:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		return result_data_set_range(result, urank, count32, list->ele_count);
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
		result_data_set_int_list_by_mask(result, rm_mask, rm_count,
				list->ele_count);
		break;
	case RESULT_TYPE_VALUE:
		if (inverted) {
			list_result_data_set_values_by_mask(result, rm_mask,
					&list->full_offidx, rm_count, rm_sz);
		}
		else if (! list_result_data_set_values_by_ordidx(result, &ret_idx,
				&list->full_offidx, rm_count, rm_sz)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() invalid packed list");
			return -AS_ERR_PARAMETER;
		}
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

static int
packed_list_get_remove_all_by_value_list_ordered(const packed_list *list,
		cdt_op_mem *com, msgpack_in *mp_items, uint32_t items_count)
{
	cdt_result_data *result = &com->result;
	cf_assert(result->is_multi, AS_PARTICLE, "not supported");

	define_order_index2(rm_rc, list->ele_count, 2 * items_count,
			com->alloc_idx);
	uint32_t rc_count = 0;

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload value = {
				.ptr = mp_items->buf + mp_items->offset,
				.sz = msgpack_sz(mp_items)
		};

		if (value.sz == 0) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() invalid list");
			return -AS_ERR_PARAMETER;
		}

		uint32_t rank;
		uint32_t count;

		packed_list_find_rank_range_by_value_interval_ordered(list, &value,
				&value, &rank, &count, true);

		order_index_set(&rm_rc, 2 * i, rank);
		order_index_set(&rm_rc, (2 * i) + 1, count);
		rc_count += count;
	}

	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;
	bool inverted = result_data_is_inverted(result);
	bool need_mask = (cdt_op_is_modify(com) ||
			result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE));
	define_cond_cdt_idx_mask(rm_mask, list->ele_count, need_mask,
			com->alloc_idx);

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_rc, NULL, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, list->ele_count);
	}

	if (cdt_op_is_modify(com)) {
		if (rm_count == list->ele_count) {
			cdt_context_set_empty_list(&com->ctx, true);
		}
		else if (rm_count != 0) {
			int ret = packed_list_remove_by_mask(list, com, rm_mask, rm_count,
					&rm_sz);

			if (ret != AS_OK) {
				return ret;
			}
		}
		else {
			packed_list_partial_offidx_update(list);
		}
	}
	else {
		packed_list_partial_offidx_update(list);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
		break;
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		if (inverted) {
			result_data_set_int_list_by_mask(result, rm_mask, rm_count,
					list->ele_count);
		}
		else {
			result_data_set_by_irc(result, &rm_rc, NULL, rc_count);
		}
		break;
	case RESULT_TYPE_VALUE: {
		if (inverted) {
			list_result_data_set_values_by_mask(result, rm_mask, &list->offidx,
					rm_count, rm_sz);
		}
		else {
			list_result_data_set_values_by_idxcount(result, &rm_rc,
					&list->offidx);
		}
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_list_get_remove_all_by_value_list_ordered");
		list_print(list, "original");
		cf_crash(AS_PARTICLE, "all_by_value_list_ordered: ele_count %u items_count %u rm_count %u", list->ele_count, items_count, rm_count);
	}
#endif

	return AS_OK;
}

static int
packed_list_get_remove_all_by_value_list(const packed_list *list,
		cdt_op_mem *com, const cdt_payload *value_list)
{
	cdt_result_data *result = &com->result;

	if (result_data_is_return_rank_range(result) ||
			result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_list_op_get_remove_all_by_value_list() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	msgpack_in mp_items;
	uint32_t items_count;

	if (! list_param_parse(value_list, &mp_items, &items_count)) {
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);

	if (items_count == 0) {
		if (! inverted) {
			if (! list_result_data_set_not_found(result, 0)) {
				cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list() invalid result type %d", result->type);
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_list_get_remove_by_index_range(list, com, 0,
				list->ele_count);
	}

	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (list_is_ordered(list)) {
		if (! list_full_offset_index_fill_all(full->offidx)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() invalid list");
			return -AS_ERR_PARAMETER;
		}

		return packed_list_get_remove_all_by_value_list_ordered(list, com,
				&mp_items, items_count);
	}

	bool is_ret_rank = result_data_is_return_rank(result);
	uint32_t rm_count = 0;
	define_order_index(value_list_ordidx, items_count, com->alloc_idx);
	define_cdt_idx_mask(rm_mask, list->ele_count, com->alloc_idx);
	definep_cond_order_index2(rc, list->ele_count, items_count * 2,
			is_ret_rank, com->alloc_idx);

	if (! offset_index_find_items(full->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_LIST_VALUE, &mp_items, &value_list_ordidx,
			inverted, rm_mask, &rm_count, rc, com->alloc_idx)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (cdt_op_is_modify(com)) {
		if (rm_count == list->ele_count) {
			cdt_context_set_empty_list(&com->ctx, false);
		}
		else if (rm_count != 0) {
			int ret = packed_list_remove_by_mask(list, com, rm_mask, rm_count,
					&rm_sz);

			if (ret != AS_OK) {
				return ret;
			}
		}
		else {
			packed_list_partial_offidx_update(list);
		}
	}
	else {
		packed_list_partial_offidx_update(list);
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
		result_data_set_int_list_by_mask(result, rm_mask, rm_count,
				list->ele_count);
		break;
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		result_data_set_by_itemlist_irc(result, &value_list_ordidx, rc,
				rm_count);
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, rm_count);
		break;
	case RESULT_TYPE_VALUE: {
		list_result_data_set_values_by_mask(result, rm_mask, full->offidx,
				rm_count, rm_sz);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "packed_list_op_get_remove_all_by_value_list() result_type %d not supported", result->type);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_rel_rank_range(const packed_list *list,
		cdt_op_mem *com, const cdt_payload *value, int64_t rank, uint64_t count)
{
	setup_list_must_have_full_offidx(full, list, com->alloc_idx);
	cdt_result_data *result = &com->result;

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rel_rank_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(list)) {
		uint32_t rel_rank;
		uint32_t temp;

		packed_list_find_rank_range_by_value_interval_ordered(list, value,
				value, &rel_rank, &temp, result->is_multi);

		calc_rel_index_count(rank, count, rel_rank, &rank, &count);

		return packed_list_get_remove_by_index_range(list, com, rank, count);
	}

	uint32_t rel_rank;
	uint32_t temp;

	if (! packed_list_find_rank_range_by_value_interval_unordered(list,
			value, value, &rel_rank, &temp, NULL,
			result_data_is_inverted(result), result->is_multi)) {
		return -AS_ERR_PARAMETER;
	}

	calc_rel_index_count(rank, count, rel_rank, &rank, &count);

	return packed_list_get_remove_by_rank_range(list, com, rank, count);
}

static int
packed_list_insert(const packed_list *list, cdt_op_mem *com, int64_t index,
		const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags,
		bool set_result)
{
	cdt_result_data *result = set_result ? &com->result : NULL;
	uint32_t param_count = 1;
	uint32_t payload_hdr_sz = 0;

	if (payload_is_list) {
		if (! msgpack_buf_get_list_ele_count(payload->ptr, payload->sz,
				&param_count)) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid payload, expected a list");
			return -AS_ERR_PARAMETER;
		}

		if (param_count == 0) {
			result_data_set_int(result, list->ele_count);
			cdt_context_list_handle_possible_noop(&com->ctx);
			return AS_OK; // no-op
		}

		payload_hdr_sz = as_pack_list_header_get_size(param_count);

		if (payload_hdr_sz > payload->sz) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid list header: payload->size=%d", payload->sz);
			return -AS_ERR_PARAMETER;
		}
	}

	if (index > INT32_MAX || (index = calc_index(index, list->ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() index %ld out of bounds for ele_count %d", index > 0 ? index : index - list->ele_count, list->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (mod_flags_is_bounded(mod_flags) && (uint32_t)index > list->ele_count) {
		if (mod_flags_is_no_fail(mod_flags)) {
			result_data_set_int(result, list->ele_count);
			return AS_OK; // no-op
		}

		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;
	bool is_unique = mod_flags_is_unique(mod_flags);
	define_cond_cdt_idx_mask(pfound, param_count, is_unique, com->alloc_idx);

	if (is_unique) {
		// Assume only here for the unordered case.
		if (payload_is_list) {
			msgpack_in mp = {
					.buf = payload->ptr + payload_hdr_sz,
					.buf_sz = payload->sz - payload_hdr_sz
			};

			for (uint32_t i = 0; i < param_count; i++) {
				cdt_payload val;
				uint32_t rank;
				uint32_t count;

				val.ptr = mp.buf + mp.offset;
				val.sz = msgpack_sz(&mp);

				if (val.sz == 0) {
					cf_warning(AS_PARTICLE, "packed_list_insert() invalid parameters");
					return -AS_ERR_PARAMETER;
				}

				if (! packed_list_find_rank_range_by_value_interval_unordered(
						list, &val, &val, &rank, &count, NULL, false, false)) {
					return -AS_ERR_PARAMETER;
				}

				if (count == 0) {
					msgpack_in cmp0 = {
							.buf = val.ptr,
							.buf_sz = val.sz
					};

					msgpack_in cmp1 = mp;
					bool found = false;

					cmp1.offset = 0;

					for (uint32_t j = 0; j < i; j++) {
						cmp0.offset = 0;

						msgpack_cmp_type cmp = msgpack_cmp(&cmp0, &cmp1);

						if (cmp == MSGPACK_CMP_EQUAL) {
							if (! mod_flags_is_no_fail(mod_flags)) {
								return -AS_ERR_OP_NOT_APPLICABLE;
							}

							if (! mod_flags_is_do_partial(mod_flags)) {
								result_data_set_int(result, list->ele_count);
								return AS_OK;
							}

							rm_sz += val.sz;
							rm_count++;
							found = true;
							break;
						}
					}

					if (! found) {
						cdt_idx_mask_set(pfound, i);
					}
				}
				else {
					if (mod_flags_is_do_partial(mod_flags)) {
						rm_sz += val.sz;
						rm_count++;
					}
					else {
						result_data_set_int(result, list->ele_count);
						return mod_flags_return_exists(mod_flags);
					}
				}
			}

			if (param_count == rm_count) {
				result_data_set_int(result, list->ele_count);
				return mod_flags_return_exists(mod_flags);
			}
		}
		else {
			uint32_t rank;
			uint32_t count;

			if (! packed_list_find_rank_range_by_value_interval_unordered(list,
					payload, payload, &rank, &count, NULL, false, false)) {
				return -AS_ERR_PARAMETER;
			}

			if (count != 0) {
				result_data_set_int(result, list->ele_count);
				return mod_flags_return_exists(mod_flags);
			}
		}
	}

	uint32_t uindex = (uint32_t)index;
	define_packed_list_op(op, list);
	uint32_t insert_sz = payload->sz - payload_hdr_sz - rm_sz;
	uint32_t add_count = param_count - rm_count;

	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (! packed_list_op_insert(&op, uindex, add_count, insert_sz)) {
		cf_warning(AS_PARTICLE, "packed_list_insert() packed_list_op_insert failed");
		return -AS_ERR_PARAMETER;
	}

	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
			op.new_content_sz, op.new_ele_count, uindex, &list->offidx, NULL);

	ptr += packed_list_op_write_seg1(&op, ptr);

	const uint8_t *p = payload->ptr + payload_hdr_sz;

	if (rm_sz == 0) {
		uint32_t sz = payload->sz - payload_hdr_sz;

		memcpy(ptr, p, sz);
		ptr += sz;
	}
	else {
		msgpack_in mp = {
				.buf = payload->ptr + payload_hdr_sz,
				.buf_sz = payload->sz - payload_hdr_sz
		};

		uint32_t idx = 0;

		for (uint32_t i = 0; i < add_count; i++) {
			uint32_t next = cdt_idx_mask_find(pfound, idx, param_count, false);
			uint32_t skip = next - idx;

			for (uint32_t j = 0; j < skip; j++) {
				msgpack_sz(&mp);
			}

			const uint8_t *begin = mp.buf + mp.offset;
			size_t sz = (size_t)msgpack_sz(&mp);

			memcpy(ptr, begin, sz);
			ptr += sz;
			idx = next + 1;
		}
	}

	packed_list_op_write_seg2(&op, ptr);
	result_data_set_int(result, op.new_ele_count);

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_list_insert");
		cdt_context_print(&com->ctx, "ctx");
		cf_crash(AS_PARTICLE, "packed_list_insert: index %ld payload_is_list %d mod_flags 0x%lX", index, payload_is_list, mod_flags);
	}
#endif

	return AS_OK;
}

static int
packed_list_add_ordered(const packed_list *list, cdt_op_mem *com,
		const cdt_payload *payload, uint64_t mod_flags)
{
	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "packed_list_add_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	order_index_find find = {
			.target = list->ele_count + 1
	};

	packed_list_find_by_value_ordered(list, payload, &find);

	if (find.found && mod_flags_is_unique(mod_flags)) {
		result_data_set_int(&com->result, list->ele_count);
		return mod_flags_return_exists(mod_flags);
	}

	return packed_list_insert(list, com, (int64_t)find.result, payload,
			false, AS_CDT_LIST_MODIFY_DEFAULT, true);
}

static int
packed_list_add_items_ordered(const packed_list *list, cdt_op_mem *com,
		const cdt_payload *items, uint64_t mod_flags)
{
	cdt_result_data *result = &com->result;
	uint32_t val_count;

	if (! msgpack_buf_get_list_ele_count(items->ptr, items->sz, &val_count)) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid payload, expected a list");
		return -AS_ERR_PARAMETER;
	}

	if (val_count == 0) {
		result_data_set_int(result, list->ele_count);
		cdt_context_list_handle_possible_noop(&com->ctx);
		return AS_OK; // no-op
	}

	uint32_t hdr_sz = as_pack_list_header_get_size(val_count);

	if (hdr_sz > items->sz) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list header: payload->size=%d", items->sz);
		return -AS_ERR_PARAMETER;
	}

	// Sort items to add.
	define_order_index(val_ord, val_count, com->alloc_idx);
	define_offset_index(val_off, items->ptr + hdr_sz, items->sz - hdr_sz,
			val_count, com->alloc_idx);

	if (! list_full_offset_index_fill_all(&val_off) ||
			! list_order_index_sort(&val_ord, &val_off,
					AS_CDT_SORT_ASCENDING)) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	bool unique = mod_flags_is_unique(mod_flags);

	if (unique) {
		uint32_t rm_count;
		uint32_t rm_sz;
		bool success = order_index_sorted_mark_dup_eles(&val_ord, &val_off,
				&rm_count, &rm_sz);
		cf_assert(success, AS_PARTICLE, "remove dup failed");

		if (rm_count != 0) {
			if (! mod_flags_is_no_fail(mod_flags)) {
				return -AS_ERR_OP_NOT_APPLICABLE;
			}

			if (! mod_flags_is_do_partial(mod_flags)) {
				as_bin_set_int(result->result, list->ele_count);
				return AS_OK;
			}
		}
	}

	setup_list_must_have_full_offidx(full, list, com->alloc_idx);
	define_order_index2(insert_idx, list->ele_count, val_count, com->alloc_idx);
	uint32_t new_content_sz = list->content_sz;
	uint32_t new_ele_count = list->ele_count;

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	for (uint32_t i = 0; i < val_count; i++) {
		uint32_t val_idx = order_index_get(&val_ord, i);

		if (val_idx == val_count) {
			continue;
		}

		uint32_t off = offset_index_get_const(&val_off, val_idx);
		uint32_t sz = offset_index_get_delta_const(&val_off, val_idx);

		const cdt_payload value = {
				.ptr = items->ptr + hdr_sz + off,
				.sz = sz
		};

		order_index_find find = {
				.target = list->ele_count + 1
		};

		packed_list_find_by_value_ordered(list, &value, &find);

		if (unique && find.found) {
			if (mod_flags_is_do_partial(mod_flags)) {
				order_index_set(&val_ord, i, val_count);
			}
			else {
				as_bin_set_int(result->result, list->ele_count);
				return mod_flags_return_exists(mod_flags);
			}
		}
		else {
			order_index_set(&insert_idx, i, find.result);
			new_content_sz += sz;
			new_ele_count++;
		}
	}

	// Construct new list.
	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
			new_content_sz, new_ele_count, 0, &list->offidx, &new_offidx);
	uint32_t list_start = 0;

	bool has_new_offidx = ! offset_index_is_null(&new_offidx);
	uint32_t new_idx = 0;
	uint32_t cpy_delta = 0;
	uint32_t cur_offset = 0;

	for (uint32_t i = 0; i < val_count; i++) {
		uint32_t val_idx = order_index_get(&val_ord, i);

		if (val_idx == val_count) {
			continue;
		}

		uint32_t list_idx = order_index_get(&insert_idx, i);

		if (list_idx > list_start) {
			uint32_t off0 = offset_index_get_const(&list->offidx, list_start);
			uint32_t off1 = offset_index_get_const(&list->offidx, list_idx);
			uint32_t seg_count = list_idx - list_start;
			uint32_t seg_sz = off1 - off0;

			memcpy(ptr, list->contents + off0, seg_sz);
			ptr += seg_sz;

			if (has_new_offidx) {
				offset_index_copy(&new_offidx, &list->offidx, new_idx,
						list_start, seg_count, cpy_delta);
				new_idx += seg_count;
				cur_offset = off1 + cpy_delta;
			}

			list_start = list_idx;
		}

		uint32_t off = offset_index_get_const(&val_off, val_idx);
		uint32_t val_sz = offset_index_get_delta_const(&val_off, val_idx);

		memcpy(ptr, items->ptr + hdr_sz + off, val_sz);
		ptr += val_sz;

		if (has_new_offidx) {
			offset_index_set(&new_offidx, new_idx++, cur_offset);
			cpy_delta += val_sz;
			cur_offset += val_sz;
		}
	}

	if (list_start < list->ele_count && list->ele_count != 0) {
		uint32_t off = offset_index_get_const(&list->offidx, list_start);
		uint32_t seg_count = list->ele_count - list_start;

		memcpy(ptr, list->contents + off, list->content_sz - off);

		if (has_new_offidx) {
			offset_index_copy(&new_offidx, &list->offidx, new_idx, list_start,
					seg_count, cpy_delta);
		}
	}

	if (has_new_offidx) {
		offset_index_set_filled(&new_offidx, new_ele_count);
	}

	result_data_set_int(result, new_ele_count);

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "packed_list_add_items_ordered");
		list_print(list, "original");
		cf_crash(AS_PARTICLE, "add_items_ordered: val_count %u", val_count);
	}
#endif

	return AS_OK;
}

static int
packed_list_replace_ordered(const packed_list *list, cdt_op_mem *com,
		uint32_t index, const cdt_payload *value, uint64_t mod_flags)
{
	uint32_t rank;
	uint32_t count;

	setup_list_must_have_full_offidx(full, list, com->alloc_idx);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "packed_list_replace_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	packed_list_find_rank_range_by_value_interval_ordered(list, value, value,
			&rank, &count, false);

	define_packed_list_op(op, list);

	if (index > list->ele_count) {
		cf_warning(AS_PARTICLE, "packed_list_replace_ordered() index %u > ele_count %u out of bounds not allowed for ORDERED lists", index, list->ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (! packed_list_op_remove(&op, index, 1)) {
		cf_warning(AS_PARTICLE, "packed_list_replace_ordered() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	if (mod_flags_is_unique(mod_flags) && count != 0) {
		if (rank == index) { // uniquely replacing element with same value
			return AS_OK; // no-op
		}

		return mod_flags_return_exists(mod_flags);
	}

	uint32_t new_ele_count = list->ele_count;

	op.new_content_sz += value->sz;

	if (index == list->ele_count) {
		new_ele_count++;
	}

	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list->ext_flags,
			op.new_content_sz, new_ele_count, (rank < index) ? rank : index,
					&list->offidx, NULL);
	uint32_t offset = offset_index_get_const(full->offidx, rank);

	if (rank <= index) {
		uint32_t tail_sz = op.seg1_sz - offset;

		memcpy(ptr, list->contents, offset);
		ptr += offset;
		memcpy(ptr, value->ptr, value->sz);
		ptr += value->sz;
		memcpy(ptr, list->contents + offset, tail_sz);
		ptr += tail_sz;
		packed_list_op_write_seg2(&op, ptr);
	}
	else if (op.seg2_sz == 0) {
		ptr += packed_list_op_write_seg1(&op, ptr);
		memcpy(ptr, value->ptr, value->sz);
	}
	else {
		uint32_t head_sz = offset - op.seg2_offset;
		uint32_t tail_sz = op.seg2_sz - head_sz;

		ptr += packed_list_op_write_seg1(&op, ptr);
		memcpy(ptr, list->contents + op.seg2_offset, head_sz);
		ptr += head_sz;
		memcpy(ptr, value->ptr, value->sz);
		ptr += value->sz;
		memcpy(ptr, list->contents + offset, tail_sz);
	}

	return AS_OK;
}

static bool
packed_list_check_order_and_fill_offidx(const packed_list *list)
{
	if (list->ele_count == 0) {
		return true;
	}

	offset_index *offidx = (offset_index *)&list->offidx;

	if (list_is_ordered(list)) {
		if (! offset_index_check_order_and_fill(offidx, false)) {
			return false;
		}
	}
	else if (! offset_index_is_null(offidx)) {
		msgpack_in mp = {
				.buf = list->contents,
				.buf_sz = list->content_sz
		};

		uint32_t blocks = list_offset_partial_index_count(list->ele_count);

		for (uint32_t j = 1; j < blocks; j++) {
			if (msgpack_sz_rep(&mp, PACKED_LIST_INDEX_STEP) == 0 ||
					mp.has_nonstorage) {
				return false;
			}

			offset_index_set(offidx, j, mp.offset);
		}

		offset_index_set_filled(offidx, blocks);

		uint32_t mod_count = list->ele_count % PACKED_LIST_INDEX_STEP;

		if (mod_count != 0 &&
				(msgpack_sz_rep(&mp, mod_count) == 0 || mp.has_nonstorage)) {
			return false;
		}
	}
	else {
		msgpack_in mp = {
				.buf = list->contents,
				.buf_sz = list->content_sz
		};

		if (msgpack_sz_rep(&mp, list->ele_count) == 0 || mp.has_nonstorage) {
			return false;
		}
	}

	return true;
}

static bool
packed_list_deep_check_order_and_fill_offidx(const packed_list *list)
{
	if (list->ele_count == 0) {
		return true;
	}

	offset_index *offidx = (offset_index *)&list->offidx;

	if (list_is_ordered(list)) {
		if (! offset_index_deep_check_order_and_fill(offidx, false)) {
			return false;
		}

		return true;
	}

	msgpack_in mp = {
			.buf = list->contents,
			.buf_sz = list->content_sz
	};

	if (! offset_index_is_null(offidx)) {
		uint32_t blocks = list_offset_partial_index_count(list->ele_count);

		for (uint32_t j = 1; j < blocks; j++) {
			if (! cdt_check_rep(&mp, PACKED_LIST_INDEX_STEP)) {
				return false;
			}

			offset_index_set(offidx, j, mp.offset);
		}

		offset_index_set_filled(offidx, blocks);

		if (blocks != 0) {
			blocks--;
		}

		uint32_t mod_count = list->ele_count - blocks * PACKED_LIST_INDEX_STEP;

		if (mod_count != 0 && ! cdt_check_rep(&mp, mod_count)) {
			return false;
		}
	}
	else if (! cdt_check_rep(&mp, list->ele_count)) {
		return false;
	}

	if (mp.offset != mp.buf_sz) {
		cf_warning(AS_PARTICLE, "packed_list_deep_check_order_and_fill_offidx() padding not allowed, size %u", mp.buf_sz - mp.offset);
		return false;
	}

	return true;
}

//----------------------------------------------------------
// packed_list_op
//

static void
packed_list_op_init(packed_list_op *op, const packed_list *list)
{
	memset(op, 0, sizeof(packed_list_op));
	op->list = list;
}

// Calculate a packed list split via insert op.
// Return true on success.
static bool
packed_list_op_insert(packed_list_op *op, uint32_t index, uint32_t count,
		uint32_t insert_sz)
{
	uint32_t ele_count = op->list->ele_count;

	if (index >= ele_count) { // insert off the end
		if (index + count >= INT32_MAX) {
			cf_warning(AS_PARTICLE, "as_packed_list_insert() index %u + count %u overflow", index, count);
			return false;
		}

		op->new_ele_count = index + count;
		op->nil_ele_sz = index - ele_count;

		op->seg1_sz = op->list->content_sz;
		op->seg2_sz = 0;
	}
	else { // insert front or middle
		op->new_ele_count = ele_count + count;
		op->nil_ele_sz = 0;
		uint32_t offset = packed_list_find_idx_offset(op->list, index);

		if (index != 0 && offset == 0) {
			return false;
		}

		op->seg1_sz = offset;
		op->seg2_offset = offset;
		op->seg2_sz = op->list->content_sz - offset;
	}

	op->new_content_sz = op->seg1_sz + op->nil_ele_sz + insert_sz + op->seg2_sz;

	return true;
}

// Calculate a packed list split via remove op.
// Assume count != 0.
// Return true on success.
static bool
packed_list_op_remove(packed_list_op *op, uint32_t index, uint32_t count)
{
	uint32_t ele_count = op->list->ele_count;

	if (index >= ele_count) { // nothing to remove
		op->seg1_sz = op->list->content_sz;
		op->seg2_sz = 0;
		op->new_ele_count = ele_count;
		op->new_content_sz = op->list->content_sz;

		return true;
	}

	uint32_t offset = packed_list_find_idx_offset(op->list, index);

	if (index != 0 && offset == 0) {
		return false;
	}

	if (count >= ele_count - index) { // remove tail elements
		op->new_ele_count = index;

		op->seg1_sz = offset;
		op->seg2_offset = 0;
		op->seg2_sz = 0;
	}
	else { // remove front or middle
		op->new_ele_count = ele_count - count;
		op->seg1_sz = offset;

		uint32_t end_off = packed_list_find_idx_offset_continue(op->list,
				index + count, index, offset);

		if (end_off == 0) {
			return false;
		}

		op->seg2_offset = end_off;
		op->seg2_sz = op->list->content_sz - end_off;
	}

	op->new_content_sz = op->seg1_sz + op->seg2_sz;

	return true;
}

// Write segment 1 and trailing nils if any.
// Return number of bytes written.
static uint32_t
packed_list_op_write_seg1(const packed_list_op *op, uint8_t *buf)
{
	memcpy(buf, op->list->contents, op->seg1_sz);

	if (op->nil_ele_sz == 0) {
		return op->seg1_sz;
	}

	buf += op->seg1_sz;
	memset(buf, msgpack_nil[0], op->nil_ele_sz);

	return op->seg1_sz + op->nil_ele_sz;
}

// Write segment 2 if any.
// Return number of bytes written.
static uint32_t
packed_list_op_write_seg2(const packed_list_op *op, uint8_t *buf)
{
	if (op->seg2_sz == 0) {
		return 0;
	}

	memcpy(buf, op->list->contents + op->seg2_offset, op->seg2_sz);

	return op->seg2_sz;
}

static bool
packed_list_builder_add_ranks_by_range(const packed_list *list,
		cdt_container_builder *builder, msgpack_in *start, uint32_t count,
		bool reverse)
{
	for (uint32_t i = 0; i < count; i++) {
		cdt_payload value = {
				.ptr = start->buf + start->offset
		};

		value.sz = msgpack_sz(start);

		if (value.sz == 0) {
			return false;
		}

		uint32_t rank;
		uint32_t rcount;

		if (! packed_list_find_rank_range_by_value_interval_unordered(list,
				&value, &value, &rank, &rcount, NULL, false, false)) {
			return false;
		}

		cdt_container_builder_add_int64(builder,
				reverse ? list->ele_count - rank - 1 : rank);
	}

	return true;
}

//----------------------------------------------------------
// list
//

// Create a non-indexed list.
// If alloc_buf is NULL, memory is reserved using cf_malloc.
static list_mem *
list_create(rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_sz)
{
	uint32_t hdr_sz = as_pack_list_header_get_size(ele_count);
	uint32_t sz = hdr_sz + content_sz;
	list_mem *p_list_mem = (list_mem *)rollback_alloc_reserve(alloc_buf,
			sizeof(list_mem) + sz);

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
	p_list_mem->sz = sz;

	return p_list_mem;
}

static as_particle *
list_simple_create_from_buf(rollback_alloc *alloc_buf, uint32_t ele_count,
		const uint8_t *contents, uint32_t content_sz)
{
	list_mem *p_list_mem = list_create(alloc_buf, ele_count, content_sz);

	if (p_list_mem) {
		uint32_t hdr_sz = list_pack_header(p_list_mem->data, ele_count);

		if (content_sz > 0 && contents) {
			memcpy(p_list_mem->data + hdr_sz, contents, content_sz);
		}
	}

	return (as_particle *)p_list_mem;
}

static as_particle *
list_simple_create(rollback_alloc *alloc_buf, uint32_t ele_count,
		uint32_t content_sz, uint8_t **contents_r)
{
	list_mem *p_list_mem = list_create(alloc_buf, ele_count, content_sz);
	uint32_t hdr_sz = list_pack_header(p_list_mem->data, ele_count);

	*contents_r = p_list_mem->data + hdr_sz;

	return (as_particle *)p_list_mem;
}

static int
list_set_flags(cdt_op_mem *com, uint8_t set_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_set_flags() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	bool reorder = false;
	bool was_ordered = list_is_ordered(&list);

	if (flags_is_ordered(set_flags)) {
		if (was_ordered) {
			return AS_OK; // no-op
		}

		if (list.ele_count > 1) {
			reorder = true;
		}
	}
	else {
		if (! was_ordered) {
			cdt_context_list_handle_possible_noop(&com->ctx);
			return AS_OK; // no-op
		}
	}

	offset_index new_offidx;
	uint8_t * const ptr = list_setup_bin_ctx(&com->ctx, set_flags,
			list.content_sz, list.ele_count, reorder ? 0 : list.ele_count,
					&list.offidx, &new_offidx);

	if (! reorder) {
		memcpy(ptr, list.contents, list.content_sz);
	}
	else {
		setup_list_must_have_full_offidx(full, &list, com->alloc_idx);

		if (! list_full_offset_index_fill_all(full->offidx)) {
			cf_warning(AS_PARTICLE, "list_set_flags() invalid list");
			return -AS_ERR_PARAMETER;
		}

		define_order_index(ordidx, list.ele_count, com->alloc_idx);

		if (! list_order_index_sort(&ordidx, full->offidx,
				AS_CDT_SORT_ASCENDING)) {
			cf_warning(AS_PARTICLE, "list_set_flags() invalid list");
			return -AS_ERR_PARAMETER;
		}

		list_order_index_pack(&ordidx, full->offidx, ptr, &new_offidx);
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "set_flags");
		cdt_context_print(&com->ctx, "ctx");
		cdt_mem *mem = (cdt_mem *)com->ctx.orig;
		print_packed(mem->data, mem->sz, "orig particle");
		list_print(&list, "original list");
		cf_crash(AS_PARTICLE, "set_flags: set_flags %u", set_flags);
	}
#endif

	return AS_OK;
}

static int
list_append(cdt_op_mem *com, const cdt_payload *payload, bool payload_is_list,
		uint64_t mod_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_append() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		if (! payload_is_list) {
			return packed_list_add_ordered(&list, com, payload, mod_flags);
		}

		return packed_list_add_items_ordered(&list, com, payload, mod_flags);
	}

	return packed_list_insert(&list, com, (int64_t)list.ele_count, payload,
			payload_is_list, mod_flags, true);
}

static int
list_insert(cdt_op_mem *com, int64_t index, const cdt_payload *payload,
		bool payload_is_list, uint64_t mod_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_insert() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		cf_warning(AS_PARTICLE, "list_insert() invalid op on ORDERED list");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return packed_list_insert(&list, com, index, payload, payload_is_list,
			mod_flags, true);
}

static int
list_set(cdt_op_mem *com, int64_t index, const cdt_payload *value,
		uint64_t mod_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_set() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		cf_warning(AS_PARTICLE, "list_set() invalid op on ORDERED list");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	uint32_t ele_count = list.ele_count;

	if (index >= ele_count) {
		return packed_list_insert(&list, com, index, value, false, mod_flags,
				false);
	}

	if (index > UINT32_MAX || (index = calc_index(index, ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "list_set() index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	if (mod_flags_is_unique(mod_flags)) {
		uint32_t rank;
		uint32_t count;
		uint64_t idx;

		// Use non-multi-find scan to optimize for 0 or 1 copies of element.
		// 2 or more copies will result in an additional multi-find scan below.
		if (! packed_list_find_rank_range_by_value_interval_unordered(&list,
				value, value, &rank, &count, &idx, false, false)) {
			return -AS_ERR_PARAMETER;
		}

		if (count != 0) {
			if (idx != (uint64_t)index) {
				return mod_flags_return_exists(mod_flags);
			}

			// Need second scan since the dup found is at the index being set.
			if (! packed_list_find_rank_range_by_value_interval_unordered(&list,
					value, value, &rank, &count, NULL, false, true)) {
				return -AS_ERR_PARAMETER;
			}

			if (count > 1) {
				return mod_flags_return_exists(mod_flags);
			}
		}
	}

	uint32_t uindex = (uint32_t)index;
	define_packed_list_op(op, &list);
	setup_list_must_have_full_offidx(full, &list, com->alloc_idx);

	if (! packed_list_op_remove(&op, uindex, 1)) {
		cf_warning(AS_PARTICLE, "list_set() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	op.new_content_sz += value->sz;

	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list.ext_flags,
			op.new_content_sz, ele_count, uindex, &list.offidx, NULL);

	ptr += packed_list_op_write_seg1(&op, ptr);

	memcpy(ptr, value->ptr, value->sz);
	ptr += value->sz;

	packed_list_op_write_seg2(&op, ptr);

	return AS_OK;
}

static int
list_increment(cdt_op_mem *com, int64_t index, cdt_payload *delta_value,
		uint64_t mod_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_increment() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (index > INT32_MAX || (index = calc_index(index, list.ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "list_increment() index %ld out of bounds for ele_count %d", index > 0 ? index : index - list.ele_count, list.ele_count);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	uint32_t uindex = (uint32_t)index;
	cdt_calc_delta calc_delta;

	if (! cdt_calc_delta_init(&calc_delta, delta_value, false)) {
		return -AS_ERR_PARAMETER;
	}

	setup_list_must_have_full_offidx(full, &list, com->alloc_idx);

	if (uindex < list.ele_count) {
		uint32_t offset = packed_list_find_idx_offset(&list, uindex);

		if (uindex != 0 && offset == 0) {
			cf_warning(AS_PARTICLE, "list_increment() unable to unpack element at %u", uindex);
			return -AS_ERR_PARAMETER;
		}

		msgpack_in mp = {
				.buf = list.contents + offset,
				.buf_sz = list.content_sz - offset
		};

		if (! cdt_calc_delta_add(&calc_delta, &mp)) {
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

	if (list_is_ordered(&list)) {
		return packed_list_replace_ordered(&list, com, uindex, &value,
				mod_flags);
	}

	return list_set(com, (int64_t)uindex, &value, mod_flags);
}

static int
list_sort(cdt_op_mem *com, as_cdt_sort_flags sort_flags)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list.ele_count <= 1) {
		return AS_OK;
	}

	setup_list_must_have_full_offidx(full, &list, com->alloc_idx);

	if (! list_full_offset_index_fill_all(full->offidx)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	define_order_index(ordidx, list.ele_count, com->alloc_idx);

	if (list_is_ordered(&list)) {
		for (uint32_t i = 0; i < list.ele_count; i++) {
			order_index_set(&ordidx, i, i);
		}
	}
	else if (! list_order_index_sort(&ordidx, full->offidx, sort_flags)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_count = 0;
	uint32_t rm_sz = 0;

	if ((sort_flags & AS_CDT_SORT_DROP_DUPLICATES) != 0 &&
			! order_index_sorted_mark_dup_eles(&ordidx, full->offidx, &rm_count,
					&rm_sz)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin_ctx(&com->ctx, list.ext_flags,
			list.content_sz - rm_sz, list.ele_count - rm_count, 0, &list.offidx,
			&new_offidx);

	ptr = list_order_index_pack(&ordidx, full->offidx, ptr, &new_offidx);

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(&com->ctx)) {
		cdt_bin_print(com->ctx.b, "list_sort");
		list_print(&list, "original");
		cf_crash(AS_PARTICLE, "list_sort: sort_flags %d", sort_flags);
	}
#endif

	return AS_OK;
}

static int
list_remove_by_index_range(cdt_op_mem *com, int64_t index, uint64_t count)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_remove_by_index_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_index_range(&list, com, index, count);
}

static int
list_remove_by_value_interval(cdt_op_mem *com, const cdt_payload *value_start,
		const cdt_payload *value_end)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_remove_by_value_interval() invalid packed list, ele_count=%d", list.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_value_interval(&list, com, value_start,
			value_end);
}

static int
list_remove_by_rank_range(cdt_op_mem *com, int64_t rank, uint64_t count)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_remove_by_rank_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_rank_range(&list, com, rank, count);
}

static int
list_remove_all_by_value_list(cdt_op_mem *com, const cdt_payload *value_list)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_remove_all_by_value_list() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_all_by_value_list(&list, com, value_list);
}

static int
list_remove_by_rel_rank_range(cdt_op_mem *com, const cdt_payload *value,
		int64_t rank, uint64_t count)
{
	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "list_remove_by_rel_rank_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_rel_rank_range(&list, com, value, rank,
			count);
}

// Return ptr to packed + ele_start.
static uint8_t *
list_setup_bin(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags,
		uint32_t content_sz, uint32_t ele_count, uint32_t idx_trunc,
		const offset_index *old_offidx, offset_index *new_offidx)
{
	bool set_ordered = flags_is_ordered(flags);
	uint32_t ext_content_sz = list_calc_ext_content_sz(ele_count, content_sz,
			set_ordered);
	uint32_t ext_sz = (ext_content_sz == 0 && ! set_ordered) ?
			0 : as_pack_ext_header_get_size(ext_content_sz) + ext_content_sz;
	list_mem *p_list_mem = list_create(alloc_buf,
			ele_count + (ext_sz == 0 ? 0 : 1), ext_sz + content_sz);

	cf_assert(p_list_mem, AS_PARTICLE, "p_list_mem NULL");
	b->particle = (as_particle *)p_list_mem;

	as_packer pk = {
			.buffer = p_list_mem->data,
			.capacity = p_list_mem->sz
	};

	if (ext_sz == 0) {
		as_pack_list_header(&pk, ele_count);

		if (new_offidx) {
			list_partial_offset_index_init(new_offidx, NULL, ele_count, NULL,
					content_sz);
		}

		return pk.buffer + pk.offset;
	}

	as_pack_list_header(&pk, ele_count + 1);
	as_pack_ext_header(&pk, ext_content_sz, get_ext_flags(set_ordered));

	uint8_t * const ptr = pk.buffer + pk.offset;
	offset_index offidx_temp;
	uint8_t * const contents = pk.buffer + pk.offset + ext_content_sz;

	if (! new_offidx) {
		new_offidx = &offidx_temp;
	}

	if (! set_ordered) {
		list_partial_offset_index_init(new_offidx, ptr, ele_count, contents,
				content_sz);
		idx_trunc /= PACKED_LIST_INDEX_STEP;
	}
	else {
		offset_index_init(new_offidx, ptr, ele_count, contents, content_sz);
	}

	if (idx_trunc == 0 || ! old_offidx || offset_index_is_null(old_offidx)) {
		offset_index_set_filled(new_offidx, 1);
	}
	else {
		idx_trunc = MIN(idx_trunc, offset_index_get_filled(old_offidx));
		offset_index_copy(new_offidx, old_offidx, 0, 0, idx_trunc, 0);
		offset_index_set_filled(new_offidx, idx_trunc);
	}

	return contents;
}

static uint8_t *
list_setup_bin_ctx(cdt_context *ctx, uint8_t flags, uint32_t content_sz,
		uint32_t ele_count, uint32_t idx_trunc, const offset_index *old_offidx,
		offset_index *new_offidx)
{
	if (ctx->data_sz == 0 && ! ctx->create_triggered) {
		return list_setup_bin(ctx->b, ctx->alloc_buf, flags, content_sz,
				ele_count, idx_trunc, old_offidx, new_offidx);
	}

	bool set_ordered = flags_is_ordered(flags);
	uint32_t ext_sz = (! set_ordered) ? 0 : as_pack_ext_header_get_size(0);
	uint32_t hdr_count = ele_count + (! set_ordered ? 0 : 1);
	uint32_t list_sz = as_pack_list_header_get_size(hdr_count) + ext_sz +
			content_sz;
	uint8_t *ptr = cdt_context_create_new_particle(ctx, list_sz);

	as_packer pk = {
			.buffer = ptr,
			.capacity = list_sz
	};

	int check = as_pack_list_header(&pk, hdr_count);
	cf_assert(check == 0, AS_PARTICLE, "pack list header failed");

	if (set_ordered) {
		as_pack_ext_header(&pk, 0,
				set_ordered ? AS_PACKED_LIST_FLAG_ORDERED : 0);
	}

	if (new_offidx) {
		offset_index_init(new_offidx, NULL, ele_count, NULL, content_sz);
	}

	return pk.buffer + pk.offset;
}


//==========================================================
// cdt_list_builder
//

void
cdt_list_builder_start(cdt_container_builder *builder,
		rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t max_sz)
{
	uint32_t sz = sizeof(list_mem) + sizeof(uint64_t) + 1 + max_sz;
	list_mem *p_list_mem = (list_mem *)rollback_alloc_reserve(alloc_buf, sz);

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
	p_list_mem->sz = list_pack_header(p_list_mem->data, ele_count);

	builder->particle = (as_particle *)p_list_mem;
	builder->write_ptr = p_list_mem->data + p_list_mem->sz;
	builder->ele_count = 0;
	builder->sz = &p_list_mem->sz;
}


//==========================================================
// cdt_process_state_packed_list
//

bool
cdt_process_state_packed_list_modify_optype(cdt_process_state *state,
		cdt_op_mem *com)
{
	cdt_context *ctx = &com->ctx;
	as_cdt_optype optype = state->type;

	if (ctx->data_sz == 0 && cdt_context_inuse(ctx) &&
			! is_list_type(as_bin_get_particle_type(ctx->b))) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid type %d", as_bin_get_particle_type(ctx->b));
		com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	int ret = AS_OK;
	cdt_result_data *result = &com->result;

	switch (optype) {
	case AS_CDT_OP_LIST_SET_TYPE: {
		uint64_t list_type;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &list_type)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, AS_PACKED_LIST_FLAG_NONE);
		ret = list_set_flags(com, (uint8_t)list_type);
		break;
	}
	case AS_CDT_OP_LIST_APPEND: {
		cdt_payload value;
		uint64_t create_type = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &value, &create_type, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, create_type);
		ret = list_append(com, &value, false, modify);
		break;
	}
	case AS_CDT_OP_LIST_APPEND_ITEMS: {
		cdt_payload items;
		uint64_t create_type = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &create_type, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, create_type);
		ret = list_append(com, &items, true, modify);
		break;
	}
	case AS_CDT_OP_LIST_INSERT: {
		int64_t index;
		cdt_payload value;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &value, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, AS_PACKED_LIST_FLAG_NONE);
		ret = list_insert(com, index, &value, false, modify);
		break;
	}
	case AS_CDT_OP_LIST_INSERT_ITEMS: {
		int64_t index;
		cdt_payload items;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &items, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, AS_PACKED_LIST_FLAG_NONE);
		ret = list_insert(com, index, &items, true, modify);
		break;
	}
	case AS_CDT_OP_LIST_SET: {
		int64_t index;
		cdt_payload value;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &value, &modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, AS_PACKED_LIST_FLAG_NONE);
		ret = list_set(com, index, &value, modify);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE:
	case AS_CDT_OP_LIST_POP: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, optype == AS_CDT_OP_LIST_REMOVE ?
				RESULT_TYPE_COUNT : RESULT_TYPE_VALUE, false);
		ret = list_remove_by_index_range(com, index, 1);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_RANGE:
	case AS_CDT_OP_LIST_POP_RANGE: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, optype == AS_CDT_OP_LIST_REMOVE_RANGE ?
				RESULT_TYPE_COUNT : RESULT_TYPE_VALUE, true);
		ret = list_remove_by_index_range(com, index, count);
		break;
	}
	case AS_CDT_OP_LIST_TRIM: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result->type = RESULT_TYPE_COUNT;
		result->flags = AS_CDT_OP_FLAG_INVERTED;
		result->is_multi = true;
		ret = list_remove_by_index_range(com, index, count);
		break;
	}
	case AS_CDT_OP_LIST_CLEAR: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		packed_list list;

		if (! packed_list_init_from_com(&list, com)) {
			cf_warning(AS_PARTICLE, "LIST_CLEAR: invalid list");
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_set_empty_list(ctx, list_is_ordered(&list));
		break;
	}
	case AS_CDT_OP_LIST_INCREMENT: {
		int64_t index;
		cdt_payload delta = { 0 };
		uint64_t create = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &delta, &create,
				&modify)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		cdt_context_use_static_list_if_notinuse(ctx, create);
		ret = list_increment(com, index, &delta, modify);
		break;
	}
	case AS_CDT_OP_LIST_SORT: {
		if (! as_bin_is_live(ctx->b)) {
			com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &flags)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		ret = list_sort(com, (as_cdt_sort_flags)flags);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_INDEX: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, false);
		ret = list_remove_by_index_range(com, index, 1);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE:
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type,
				optype == AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE);
		ret = list_remove_by_value_interval(com, &value, &value);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_RANK: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, false);
		ret = list_remove_by_rank_range(com, rank, 1);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = list_remove_all_by_value_list(com, &items);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = list_remove_by_index_range(com, index, count);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
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

		result_data_set(result, result_type, true);
		ret = list_remove_by_value_interval(com, &value_start, &value_end);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = list_remove_by_rank_range(com, rank, count);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE: {
		if (ctx->create_triggered || ! as_bin_is_live(ctx->b)) {
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

		result_data_set(result, result_type, true);
		ret = list_remove_by_rel_rank_range(com, &value, rank, count);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid cdt op: %d", optype);
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

	// In case of no-op.
	if (ctx->b->particle == (const as_particle *)&list_mem_empty) {
		cdt_context_set_empty_list(ctx, false);
	}
	else if (ctx->b->particle == (const as_particle *)&list_ordered_empty) {
		cdt_context_set_empty_list(ctx, true);
	}

	return true;
}

bool
cdt_process_state_packed_list_read_optype(cdt_process_state *state,
		cdt_op_mem *com)
{
	const cdt_context *ctx = &com->ctx;
	as_cdt_optype optype = state->type;

	if (ctx->data_sz == 0 && ! is_list_type(as_bin_get_particle_type(ctx->b))) {
		com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	packed_list list;

	if (! packed_list_init_from_com(&list, com)) {
		cf_warning(AS_PARTICLE, "%s: invalid list", cdt_process_state_get_op_name(state));
		com->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	int ret = AS_OK;
	cdt_result_data *result = &com->result;

	switch (optype) {
	case AS_CDT_OP_LIST_GET: {
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, RESULT_TYPE_VALUE, false);
		ret = packed_list_get_remove_by_index_range(&list, com, index, 1);
		break;
	}
	case AS_CDT_OP_LIST_GET_RANGE: {
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, RESULT_TYPE_VALUE, true);
		ret = packed_list_get_remove_by_index_range(&list, com, index, count);
		break;
	}
	case AS_CDT_OP_LIST_SIZE: {
		as_bin_set_int(result->result, list.ele_count);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_INDEX: {
		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, false);
		ret = packed_list_get_remove_by_index_range(&list, com, index, 1);
		break;
	}
	case AS_CDT_OP_LIST_GET_ALL_BY_VALUE:
	case AS_CDT_OP_LIST_GET_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type,
				optype == AS_CDT_OP_LIST_GET_ALL_BY_VALUE);
		ret = packed_list_get_remove_by_value_interval(&list, com, &value,
				&value);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_RANK: {
		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, false);
		ret = packed_list_get_remove_by_rank_range(&list, com, rank, 1);
		break;
	}
	case AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST: {
		uint64_t result_type;
		cdt_payload value_list;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_list)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = packed_list_get_remove_all_by_value_list(&list, com,
				&value_list);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_INDEX_RANGE: {
		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = packed_list_get_remove_by_index_range(&list, com, index, count);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL: {
		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { 0 };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = packed_list_get_remove_by_value_interval(&list, com,
				&value_start, &value_end);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_RANK_RANGE: {
		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = packed_list_get_remove_by_rank_range(&list, com, rank, count);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE: {
		uint64_t result_type;
		cdt_payload value;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value, &rank,
				&count)) {
			com->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(result, result_type, true);
		ret = packed_list_get_remove_by_rel_rank_range(&list, com, &value,
				rank, count);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_read_optype() invalid cdt op: %d", optype);
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
// list_offset_index
//

static inline uint32_t
list_offset_partial_index_count(uint32_t ele_count)
{
	if (ele_count != 0) {
		ele_count--;
		ele_count /= PACKED_LIST_INDEX_STEP;

		if (ele_count != 0) {
			ele_count++;
		}
	}

	return ele_count;
}

void
list_partial_offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr,
		uint32_t ele_count, const uint8_t *contents, uint32_t content_sz)
{
	ele_count = list_offset_partial_index_count(ele_count);
	offset_index_init(offidx, idx_mem_ptr, ele_count, contents, content_sz);
	offidx->is_partial = true;
}

static void
list_offset_index_rm_mask_cpy(offset_index *dst, const offset_index *full_src,
		const uint64_t *rm_mask, uint32_t rm_count)
{
	cf_assert(rm_mask && rm_count != 0, AS_PARTICLE, "list_offset_index_rm_mask_cpy() should not do no-op copy");

	uint32_t ele_count = full_src->_.ele_count;

	if (! dst->is_partial) {
		uint32_t delta = 0;
		uint32_t prev = 0;
		uint32_t idx = 0;

		for (uint32_t i = 0; i < rm_count; i++) {
			idx = cdt_idx_mask_find(rm_mask, idx, ele_count, false);
			uint32_t sz = offset_index_get_delta_const(full_src, idx);
			uint32_t diff = idx - prev;

			for (uint32_t j = 1; j < diff; j++) {
				uint32_t offset = offset_index_get_const(full_src, prev + j);

				offset_index_set(dst, prev + j - i, offset - delta);
			}

			prev = idx;
			delta += sz;
			idx++;
		}

		uint32_t diff = full_src->_.ele_count - prev;

		for (uint32_t i = 1; i < diff; i++) {
			uint32_t offset = offset_index_get_const(full_src, prev + i);
			offset_index_set(dst, prev + i - rm_count, offset - delta);
		}

		offset_index_set_filled(dst, dst->_.ele_count);
		return;
	}

	uint32_t delta = 0;
	uint32_t prev_pt_idx = 0;
	uint32_t idx = 0;

	for (uint32_t i = 0; i < rm_count; i++) {
		idx = cdt_idx_mask_find(rm_mask, idx, ele_count, false);
		uint32_t sz = offset_index_get_delta_const(full_src, idx);
		uint32_t pt_idx = (idx - i) / PACKED_LIST_INDEX_STEP;
		uint32_t diff = pt_idx - prev_pt_idx + 1;

		for (uint32_t j = 1; j < diff; j++) {
			uint32_t offset = offset_index_get_const(full_src,
					(prev_pt_idx + j) * PACKED_LIST_INDEX_STEP + i);
			offset_index_set(dst, prev_pt_idx + j, offset - delta);
		}

		prev_pt_idx = pt_idx;
		delta += sz;
		idx++;
	}

	uint32_t new_ele_count = full_src->_.ele_count - rm_count;
	uint32_t max_idx = list_offset_partial_index_count(new_ele_count);
	uint32_t diff = max_idx - prev_pt_idx;

	for (uint32_t j = 1; j < diff; j++) {
		uint32_t offset = offset_index_get_const(full_src,
				(prev_pt_idx + j) * PACKED_LIST_INDEX_STEP + rm_count);
		offset_index_set(dst, prev_pt_idx + j, offset - delta);
	}

	offset_index_set_filled(dst, max_idx);
}


//==========================================================
// list_full_offset_index
//

bool
list_full_offset_index_fill_to(offset_index *offidx, uint32_t index,
		bool check_storage)
{
	uint32_t start = offset_index_get_filled(offidx);

	index = MIN(index + 1, offidx->_.ele_count);

	if (start >= index) {
		return true;
	}

	msgpack_in mp = {
			.buf = offidx->contents,
			.buf_sz = offidx->content_sz,
			.offset = offset_index_get_const(offidx, start - 1)
	};

	for (uint32_t i = start; i < index; i++) {
		if (msgpack_sz(&mp) == 0 || (check_storage && mp.has_nonstorage)) {
			return false;
		}

		offset_index_set(offidx, i, mp.offset);
	}

	offset_index_set_filled(offidx, index);

	return true;
}

bool
list_full_offset_index_fill_all(offset_index *offidx)
{
	return list_full_offset_index_fill_to(offidx, offidx->_.ele_count, true);
}


//==========================================================
// list_order_index
//

static int
list_order_index_sort_cmp_fn(const void *x, const void *y, void *p)
{
	list_order_index_sort_userdata *udata = p;

	if (udata->error) {
		return 0;
	}

	const order_index *order = udata->order;
	uint32_t a = order_index_ptr2value(order, x);
	uint32_t b = order_index_ptr2value(order, y);

	const offset_index *offsets = udata->offsets;
	const uint8_t *buf = udata->offsets->contents;
	uint32_t len = udata->offsets->content_sz;
	uint32_t x_off = offset_index_get_const(offsets, a);
	uint32_t y_off = offset_index_get_const(offsets, b);

	msgpack_in x_mp = {
			.buf = buf + x_off,
			.buf_sz = len - x_off
	};

	msgpack_in y_mp = {
			.buf = buf + y_off,
			.buf_sz = len - y_off
	};

	msgpack_cmp_type cmp = msgpack_cmp_peek(&x_mp, &y_mp);

	switch (cmp) {
	case MSGPACK_CMP_EQUAL:
		return 0;
	case MSGPACK_CMP_LESS:
		if (udata->flags & AS_CDT_SORT_DESCENDING) {
			cmp = MSGPACK_CMP_GREATER;
		}
		break;
	case MSGPACK_CMP_GREATER:
		if (udata->flags & AS_CDT_SORT_DESCENDING) {
			cmp = MSGPACK_CMP_LESS;
		}
		break;
	default:
		udata->error = true;
		return 0;
	}

	return (cmp == MSGPACK_CMP_LESS) ? -1 : 1;
}

bool
list_order_index_sort(order_index *ordidx, const offset_index *full_offidx,
		as_cdt_sort_flags flags)
{
	uint32_t ele_count = ordidx->_.ele_count;
	list_order_index_sort_userdata udata = {
			.order = ordidx,
			.offsets = full_offidx,
			.flags = flags
	};

	for (uint32_t i = 0; i < ele_count; i++) {
		order_index_set(ordidx, i, i);
	}

	if (ele_count <= 1) {
		return true;
	}

	qsort_r(order_index_get_mem(ordidx, 0), ele_count, ordidx->_.ele_sz,
			list_order_index_sort_cmp_fn, (void *)&udata);

	return ! udata.error;
}

static uint8_t *
list_order_index_pack(const order_index *ordidx,
		const offset_index *full_offidx, uint8_t *buf, offset_index *new_offidx)
{
	cf_assert(new_offidx, AS_PARTICLE, "new_offidx null");
	cf_assert(full_offidx->_.ele_count != 0, AS_PARTICLE, "ele_count == 0");

	const uint8_t *contents = full_offidx->contents;
	uint32_t buf_off = 0;
	uint32_t write_count = 0;

	for (uint32_t i = 0; i < full_offidx->_.ele_count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		if (idx == full_offidx->_.ele_count) {
			continue;
		}

		uint32_t off = offset_index_get_const(full_offidx, idx);
		uint32_t sz = offset_index_get_delta_const(full_offidx, idx);

		memcpy(buf + buf_off, contents + off, sz);
		buf_off += sz;
		write_count++;

		if (offset_index_is_null(new_offidx)) {
			continue;
		}

		if (! new_offidx->is_partial) {
			offset_index_set(new_offidx, write_count, buf_off);
		}
		else if (write_count % PACKED_LIST_INDEX_STEP == 0 &&
				new_offidx->_.ele_count != 0) {
			uint32_t new_idx = write_count / PACKED_LIST_INDEX_STEP;
			offset_index_set(new_offidx, new_idx, buf_off);
		}
	}

	if (! offset_index_is_null(new_offidx)) {
		offset_index_set_filled(new_offidx, (new_offidx->is_partial ?
				new_offidx->_.ele_count : write_count));
	}

	return buf + buf_off;
}


//==========================================================
// list_order_heap
//

static msgpack_cmp_type
list_order_heap_cmp_fn(const void *udata, uint32_t idx1, uint32_t idx2)
{
	const packed_list *list = (const packed_list *)udata;
	const offset_index *offidx = &list->full_offidx;

	msgpack_in mp1 = {
			.buf = list->contents,
			.buf_sz = list->content_sz,
			.offset = offset_index_get_const(offidx, idx1)
	};

	msgpack_in mp2 = {
			.buf = list->contents,
			.buf_sz = list->content_sz,
			.offset = offset_index_get_const(offidx, idx2)
	};

	return msgpack_cmp_peek(&mp1, &mp2);
}


//==========================================================
// list_result_data
//

static bool
list_result_data_set_not_found(cdt_result_data *rd, int64_t index)
{
	switch (rd->type) {
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_MAP:
		return false;
	default:
		break;
	}

	return result_data_set_not_found(rd, index);
}

// Does not respect inverted flag.
static void
list_result_data_set_values_by_mask(cdt_result_data *rd, const uint64_t *mask,
		const offset_index *full_offidx, uint32_t count, uint32_t sz)
{
	if (sz == 0) {
		sz = cdt_idx_mask_get_content_sz(mask, count, full_offidx);
	}

	cdt_container_builder builder;
	cdt_list_builder_start(&builder, rd->alloc, count, sz);

	const uint8_t *end = cdt_idx_mask_write_eles(mask, count, full_offidx,
			builder.write_ptr, false);

	cf_assert(end - builder.write_ptr == sz, AS_PARTICLE, "size mismatch end - ptr %zu != sz %u", end - builder.write_ptr, sz);
	cdt_container_builder_add_n(&builder, NULL, count, sz);
	cdt_container_builder_set_result(&builder, rd);
}

// Does not respect inverted flag.
static void
list_result_data_set_values_by_idxcount(cdt_result_data *rd,
		const order_index *idxcnt, const offset_index *full_offidx)
{
	uint32_t items_count = idxcnt->_.ele_count / 2;
	uint32_t sz = 0;
	uint32_t ret_count = 0;

	for (uint32_t i = 0; i < items_count; i++) {
		uint32_t idx = order_index_get(idxcnt, 2 * i);
		uint32_t count = order_index_get(idxcnt, (2 * i) + 1);

		ret_count += count;

		for (uint32_t j = 0; j < count; j++) {
			sz += offset_index_get_delta_const(full_offidx, idx + j);
		}
	}

	cdt_container_builder builder;
	cdt_list_builder_start(&builder, rd->alloc, ret_count, sz);

	for (uint32_t i = 0; i < items_count; i++) {
		uint32_t idx = order_index_get(idxcnt, 2 * i);
		uint32_t count = order_index_get(idxcnt, (2 * i) + 1);

		if (count == 0) {
			continue;
		}

		uint32_t offset = offset_index_get_const(full_offidx, idx);
		uint32_t end = offset_index_get_const(full_offidx, idx + count);

		cdt_container_builder_add_n(&builder, full_offidx->contents + offset,
				count, end - offset);
	}

	cdt_container_builder_set_result(&builder, rd);
}

// Does not respect inverted flag.
static bool
list_result_data_set_values_by_ordidx(cdt_result_data *rd,
		const order_index *ordidx, const offset_index *full_offidx,
		uint32_t count, uint32_t sz)
{
	if (! rd->is_multi) {
		if (count != 0) {
			uint32_t i = order_index_get(ordidx, 0);
			uint32_t offset = offset_index_get_const(full_offidx, i);
			uint32_t sz = offset_index_get_delta_const(full_offidx, i);

			return as_bin_particle_alloc_from_msgpack(rd->result,
					full_offidx->contents + offset, sz) == AS_OK;
		}

		return true;
	}

	if (sz == 0) {
		sz = order_index_get_ele_size(ordidx, count, full_offidx);
	}

	uint8_t *ptr;

	rd->result->particle = list_simple_create(rd->alloc, count, sz,
			&ptr);
	order_index_write_eles(ordidx, count, full_offidx, ptr, false);
	as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);

	return true;
}


//==========================================================
// Debugging support.
//

void
list_print(const packed_list *list, const char *name)
{
	print_packed(list->packed, list->packed_sz, name);
}

static bool
list_verify_fn(const cdt_context *ctx, rollback_alloc *alloc_idx)
{
	if (! ctx->b) {
		return true;
	}

	if (ctx->create_triggered) {
		return true; // check after unwind
	}

	packed_list list;
	uint8_t type = as_bin_get_particle_type(ctx->b);

	if (type != AS_PARTICLE_TYPE_LIST && type != AS_PARTICLE_TYPE_MAP) {
		cf_warning(AS_PARTICLE, "list_verify() non-cdt type: %u", type);
		return false;
	}

	// Check header.
	if (! packed_list_init_from_ctx(&list, ctx)) {
		cdt_context_print(ctx, "ctx");
		cf_warning(AS_PARTICLE, "list_verify() invalid packed list");
		return false;
	}

	offset_index *offidx = list_full_offidx_p(&list);
	bool check_offidx = offset_index_is_valid(offidx);
	uint32_t filled = 0;
	define_offset_index(temp_offidx, list.contents, list.content_sz,
			list.ele_count, alloc_idx);

	msgpack_in mp = {
			.buf = list.contents,
			.buf_sz = list.content_sz
	};

	if (check_offidx) {
		filled = offset_index_get_filled(offidx);
		cf_assert(filled != 0, AS_PARTICLE, "filled should be at least 1 for valid offsets");

		if (list.ele_count > 1) {
			offset_index_copy(&temp_offidx, offidx, 0, 0, filled, 0);
		}
	}

	// Check offsets.
	for (uint32_t i = 0; i < list.ele_count; i++) {
		uint32_t offset;

		if (check_offidx) {
			if (list_is_ordered(&list)) {
				if (i < filled) {
					offset = offset_index_get_const(offidx, i);

					if (mp.offset != offset) {
						cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u expected=%u", i, offset, mp.offset);
						return false;
					}
				}
				else {
					offset_index_set(&temp_offidx, i, mp.offset);
				}
			}
			else if ((i % PACKED_LIST_INDEX_STEP) == 0) {
				uint32_t step_i = i / PACKED_LIST_INDEX_STEP;

				if (i < filled) {
					offset = offset_index_get_const(offidx, i);

					if (mp.offset != offset) {
						cf_warning(AS_PARTICLE, "list_verify() i=%u step %u offset=%u expected=%u", i, step_i, offset, mp.offset);
						return false;
					}
				}
			}
		}
		else {
			offset_index_set(&temp_offidx, i, mp.offset);
		}

		offset = mp.offset;

		if (msgpack_sz(&mp) == 0) {
			cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u mp.offset=%u invalid element", i, offset, mp.offset);
			return false;
		}
	}

	// Check packed size.
	if (list.content_sz != mp.offset) {
		cf_warning(AS_PARTICLE, "list_verify() content_sz=%u expected=%u", list.content_sz, mp.offset);
		cdt_context_print(ctx, "ctx");
		return false;
	}

	mp.offset = 0;

	msgpack_in mp_value = mp;

	// Check ordered list.
	if (list_is_ordered(&list) && list.ele_count > 0) {
		if (msgpack_sz(&mp) == 0) {
			cf_warning(AS_PARTICLE, "list_verify() mp.offset=%u invalid value", mp.offset);
			return false;
		}

		for (uint32_t i = 1; i < list.ele_count; i++) {
			uint32_t offset = mp.offset;
			msgpack_cmp_type cmp = msgpack_cmp(&mp_value, &mp);

			if (cmp == MSGPACK_CMP_ERROR) {
				cf_warning(AS_PARTICLE, "list_verify() i=%u/%u offset=%u mp.offset=%u invalid element", i, list.ele_count, offset, mp.offset);
				return false;
			}

			if (cmp == MSGPACK_CMP_GREATER) {
				cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u mp.offset=%u ele_count=%u element not in order", i, offset, mp.offset, list.ele_count);
				return false;
			}
		}
	}

	return true;
}

bool
list_verify(const cdt_context *ctx)
{
	define_rollback_alloc(alloc_idx, NULL, 8, false); // for temp indexes
	bool ret = list_verify_fn(ctx, alloc_idx);

	rollback_alloc_rollback(alloc_idx);

	return ret;
}
