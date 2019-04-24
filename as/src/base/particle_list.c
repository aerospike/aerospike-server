/*
 * particle_list.c
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

#include "fault.h"

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

//#define LIST_DEBUG_VERIFY

#define PACKED_LIST_INDEX_STEP 128

#define AS_PACKED_LIST_FLAG_NONE     0x00
#define AS_PACKED_LIST_FLAG_ORDERED  0x01

#define PACKED_LIST_FLAG_OFF_IDX     0x10
#define PACKED_LIST_FLAG_FULLOFF_IDX 0x20

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

typedef struct msgpack_list_empty_flagged_s {
	uint8_t list_hdr;
	uint8_t ext_hdr;
	uint8_t ext_sz;
	uint8_t ext_flags;
} __attribute__ ((__packed__)) msgpack_list_empty_flagged;

typedef struct list_mem_empty_flagged_s {
	list_mem mem;
	msgpack_list_empty_flagged list;
} list_mem_empty_flagged;

static const list_mem_empty_flagged list_ordered_empty = {
		.mem = {
				.type = AS_PARTICLE_TYPE_LIST,
				.sz = sizeof(msgpack_list_empty_flagged)
		},
		.list = {
				.list_hdr = 0x91,
				.ext_hdr = 0xC7,
				.ext_sz = 0,
				.ext_flags = AS_PACKED_LIST_FLAG_ORDERED
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
} __attribute__ ((__packed__)) list_vla;

#define define_packed_list_op(__name, __list_p) \
	packed_list_op __name; \
	packed_list_op_init(&__name, __list_p)

#define list_full_offidx_p(__list_p) \
	(offset_index *)(list_is_ordered(__list_p) ? &(__list_p)->offidx : &(__list_p)->full_offidx)

#define vla_list_full_offidx_if_invalid(__name, __list_p) \
	uint8_t __name ## __vlatemp[sizeof(offset_index *) + offset_index_vla_sz(list_full_offidx_p(__list_p))]; \
	list_vla *__name = (list_vla *)__name ## __vlatemp; \
	__name->offidx = list_full_offidx_p(__list_p); \
	offset_index_alloc_temp(list_full_offidx_p(__list_p), __name->mem_temp)

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
static inline void as_bin_set_empty_list(as_bin *b, rollback_alloc *alloc_buf, bool is_ordered);
static void as_bin_set_ordered_empty_list(as_bin *b, rollback_alloc *alloc_buf);
static inline void as_bin_set_temp_list_if_notinuse(as_bin *b, uint64_t create_flags);

// packed_list
static bool packed_list_init(packed_list *list, const uint8_t *buf, uint32_t sz);
static inline bool packed_list_init_from_particle(packed_list *list, const as_particle *p);
static bool packed_list_init_from_bin(packed_list *list, const as_bin *b);
static bool packed_list_unpack_hdridx(packed_list *list);
static void packed_list_partial_offidx_update(const packed_list *list);

static bool packed_list_find_by_value_ordered(const packed_list *list, const cdt_payload *value, order_index_find *find);
static uint32_t packed_list_find_idx_offset(const packed_list *list, uint32_t index);
static bool packed_list_find_rank_range_by_value_interval_ordered(const packed_list *list, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank_r, uint32_t *count_r, bool is_multi);
static bool packed_list_find_rank_range_by_value_interval_unordered(const packed_list *list, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, uint64_t *mask_val, bool inverted, bool is_multi);

static uint32_t packed_list_mem_sz(const packed_list *list, bool has_ext, uint32_t *ext_content_sz_r);
static uint32_t packed_list_pack_buf(const packed_list *list, uint8_t *buf, uint32_t sz, uint32_t ext_content_sz, bool strip_flags);
static list_mem *packed_list_pack_mem(const packed_list *list, list_mem *p_list_mem);
static void packed_list_content_pack(const packed_list *list, as_packer *pk);
static int packed_list_remove_by_idx(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const uint64_t rm_idx, uint32_t *rm_sz);
static int packed_list_remove_by_mask(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const uint64_t *rm_mask, uint32_t rm_count, uint32_t *rm_sz);

static int packed_list_trim(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, uint32_t index, uint32_t count, cdt_result_data *result);
static int packed_list_get_remove_by_index_range(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result);
static int packed_list_get_remove_by_value_interval(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int packed_list_get_remove_by_rank_range(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result);
static int packed_list_get_remove_all_by_value_list(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_list, cdt_result_data *result);
static int packed_list_get_remove_by_rel_rank_range(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank, uint64_t count, cdt_result_data *result);

static int packed_list_insert(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, int64_t index, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags, cdt_result_data *result);
static int packed_list_add_ordered(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, uint64_t mod_flags, cdt_result_data *result);
static int packed_list_add_items_ordered(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, uint64_t mod_flags, cdt_result_data *result);
static int packed_list_replace_ordered(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf, uint32_t index, const cdt_payload *value, uint64_t mod_flags);

// packed_list_op
static void packed_list_op_init(packed_list_op *op, const packed_list *list);
static bool packed_list_op_insert(packed_list_op *op, uint32_t index, uint32_t count, uint32_t insert_sz);
static bool packed_list_op_remove(packed_list_op *op, uint32_t index, uint32_t count);

static uint32_t packed_list_op_write_seg1(const packed_list_op *op, uint8_t *buf);
static uint32_t packed_list_op_write_seg2(const packed_list_op *op, uint8_t *buf);

static bool packed_list_builder_add_ranks_by_range(const packed_list *list, cdt_container_builder *builder, as_unpacker *start, uint32_t count, bool reverse);

// list
static list_mem *list_create(rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_sz);
static as_particle *list_simple_create_from_buf(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz);
static as_particle *list_simple_create(rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_sz, uint8_t **contents_r);

static int list_set_flags(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags, cdt_result_data *result);
static int list_append(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags, cdt_result_data *result);
static int list_insert(as_bin *b, rollback_alloc *alloc_buf, int64_t index, const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags, cdt_result_data *result);
static int list_set(as_bin *b, rollback_alloc *alloc_buf, int64_t index, const cdt_payload *value, uint64_t mod_flags);
static int list_increment(as_bin *b, rollback_alloc *alloc_buf, int64_t index, cdt_payload *delta_value, uint64_t mod_flags, cdt_result_data *result);
static int list_sort(as_bin *b, rollback_alloc *alloc_buf, as_cdt_sort_flags sort_flags);

static int list_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result);
static int list_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int list_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result);
static int list_remove_all_by_value_list(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_list, cdt_result_data *result);
static int list_remove_by_rel_rank_range(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank, uint64_t count, cdt_result_data *result);

static uint8_t *list_setup_bin(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags, uint32_t content_sz, uint32_t ele_count, uint32_t idx_trunc, const offset_index *old_offidx, offset_index *new_offidx);

// list_offset_index
static inline void list_offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz);
static void list_offset_index_rm_mask_cpy(offset_index *dst, const offset_index *full_src, const uint64_t *rm_mask, uint32_t rm_count);

// list_full_offset_index
static inline void list_full_offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr, uint32_t ele_count, const uint8_t *contents, uint32_t content_sz);
static bool list_full_offset_index_fill_to(offset_index *offidx, uint32_t index, bool check_storage);

// list_order_index
static int list_order_index_sort_cmp_fn(const void *x, const void *y, void *p);
static uint8_t *list_order_index_pack(const order_index *ordidx, const offset_index *full_offidx, uint8_t *buf, offset_index *new_offidx);

// list_order_heap
static msgpack_compare_t list_order_heap_cmp_fn(const void *udata, uint32_t idx1, uint32_t idx2);

// list_result_data
static bool list_result_data_set_not_found(cdt_result_data *rd, int64_t index);
static void list_result_data_set_values_by_mask(cdt_result_data *rd, const uint64_t *mask, const offset_index *full_offidx, uint32_t count, uint32_t sz);
static void list_result_data_set_values_by_idxcount(cdt_result_data *rd, const order_index *idxcnt, const offset_index *full_offidx);
static bool list_result_data_set_values_by_ordidx(cdt_result_data *rd, const order_index *ordidx, const offset_index *full_offidx, uint32_t count, uint32_t sz);

// Debugging support
static void list_print(const packed_list *list, const char *name);
static bool list_verify(const as_bin *b);


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

	as_unpacker pk = {
			.buffer = list.contents,
			.length = list.content_sz
	};

	if (cdt_get_storage_list_sz(&pk, list.ele_count) != list.content_sz) {
		cf_warning(AS_PARTICLE, "list_size_from_wire() invalid packed list: ele_count %u offset %u content_sz %u", list.ele_count, pk.offset, list.content_sz);
		return -AS_ERR_PARAMETER;
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

	return AS_OK;
}

int
list_compare_from_wire(const as_particle *p, as_particle_type wire_type,
		const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
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
		list_pack_empty_index(&pk, ele_count, NULL, content_sz, is_ordered);
		cf_assert(pk.offset == ele_start, AS_PARTICLE, "size mismatch pk.offset(%d) != ele_start(%u)", pk.offset, ele_start);
		p_list_mem->sz = ele_start + content_sz;
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

	if (! p_list_mem) {
		cf_warning(AS_PARTICLE, "list_from_flat() failed to create particle");
		return NULL;
	}

	p_list_mem->type = p_list_flat->type;
	*pp = (as_particle *)p_list_mem;

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
			(AS_PACKED_LIST_FLAG_ORDERED | PACKED_LIST_FLAG_FULLOFF_IDX) :
			PACKED_LIST_FLAG_OFF_IDX;
}

static uint32_t
list_calc_ext_content_sz(uint32_t ele_count, uint32_t content_sz, bool ordered)
{
	offset_index offidx;

	if (! ordered) {
		list_offset_index_init(&offidx, NULL, ele_count, NULL, content_sz);
	}
	else {
		list_full_offset_index_init(&offidx, NULL, ele_count, NULL, content_sz);
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
		list_full_offset_index_init(&offidx, pk->buffer + pk->offset, ele_count,
				contents, content_sz);
	}
	else {
		list_offset_index_init(&offidx, pk->buffer + pk->offset, ele_count,
				contents, content_sz);
	}

	offset_index_set_filled(&offidx, 1);
	pk->offset += offset_index_size(&offidx);
}

//------------------------------------------------
// as_bin
//

static inline void
as_bin_set_empty_list(as_bin *b, rollback_alloc *alloc_buf, bool is_ordered)
{
	if (is_ordered) {
		as_bin_set_ordered_empty_list(b, alloc_buf);
	}
	else {
		as_bin_set_unordered_empty_list(b, alloc_buf);
	}
}

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
			(const uint8_t *)&list_ordered_empty.list.ext_hdr,
			sizeof(msgpack_list_empty_flagged) - 1);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
}

static inline void
as_bin_set_temp_list_if_notinuse(as_bin *b, uint64_t create_flags)
{
	if (! as_bin_inuse(b)) {
		b->particle = (create_flags & AS_PACKED_LIST_FLAG_ORDERED) != 0 ?
				(as_particle *)&list_ordered_empty :
				(as_particle *)&list_mem_empty;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
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
packed_list_unpack_hdridx(packed_list *list)
{
	if (list->packed_sz == 0) {
		list->ext_flags = 0;
		return false;
	}

	as_unpacker pk = {
			.buffer = list->packed,
			.length = list->packed_sz
	};

	int64_t ele_count = as_unpack_list_header_element_count(&pk);

	if (ele_count < 0) {
		return false;
	}

	list->ele_count = (uint32_t)ele_count;

	if (ele_count != 0 && as_unpack_peek_is_ext(&pk)) {
		as_msgpack_ext ext;

		if (as_unpack_ext(&pk, &ext) != 0) {
			return false;
		}

		list->ext_flags = ext.type;
		list->ele_count--;
		list->contents = list->packed + pk.offset;
		list->content_sz = list->packed_sz - pk.offset;

		if (list_is_ordered(list)) {
			list_full_offset_index_init(&list->offidx, NULL, list->ele_count,
					list->contents, list->content_sz);
		}
		else {
			list_offset_index_init(&list->offidx, NULL, list->ele_count,
					list->contents, list->content_sz);
		}

		list_full_offset_index_init(&list->full_offidx, NULL, list->ele_count,
				list->contents, list->content_sz);

		if (ext.size >= offset_index_size(&list->offidx)) {
			offset_index_set_ptr(&list->offidx, (uint8_t *)ext.data,
					list->packed + pk.offset);
		}
	}
	else {
		list->contents = list->packed + pk.offset;
		list->content_sz = list->packed_sz - pk.offset;
		list->ext_flags = 0;

		list_offset_index_init(&list->offidx, NULL, list->ele_count,
				list->contents, list->content_sz);
		list_full_offset_index_init(&list->full_offidx, NULL, list->ele_count,
				list->contents, list->content_sz);
	}

	return true;
}

static void
packed_list_partial_offidx_update(const packed_list *list)
{
	if (list_is_ordered(list) || ! offset_index_is_valid(&list->full_offidx) ||
			! offset_index_is_valid(&list->offidx)) {
		return;
	}

	offset_index *full = (offset_index *)&list->full_offidx;
	offset_index *part = (offset_index *)&list->offidx;
	uint32_t filled = offset_index_get_filled(part);
	uint32_t max = (offset_index_get_filled(full) / PACKED_LIST_INDEX_STEP) + 1;

	if (filled >= max) {
		return;
	}

	for (uint32_t j = filled; j < max; j++) {
		uint32_t off = offset_index_get_const(full, j * PACKED_LIST_INDEX_STEP);
		offset_index_set(part, j, off);
	}

	offset_index_set_filled(part, max);
}

static bool
packed_list_find_by_value_ordered(const packed_list *list,
		const cdt_payload *value, order_index_find *find)
{
	if (list->ele_count == 0) {
		find->found = false;
		find->result = 0;
		return true;
	}

	offset_index *offidx = list_full_offidx_p(list);
	cf_assert(offset_index_is_valid(offidx), AS_PARTICLE, "invalid offidx");
	uint32_t last = offset_index_get_filled(offidx);

	find->count = last - find->start;

	if (! order_index_find_rank_by_value(NULL, value, offidx, find)) {
		return false;
	}

	if (offset_index_is_full(offidx) || find->result < last - 1 ||
			(! find->found && find->result < last) || (find->found &&
					(find->target > list->ele_count ||
							find->result >= find->target))) {
		return true;
	}

	if (find->result == list->ele_count || find->result == last ||
			find->result < find->target) {
		as_unpacker pk_start = {
				.buffer = value->ptr,
				.length = value->sz
		};

		as_unpacker pk_buf = {
				.buffer = list->contents,
				.offset = offset_index_get_const(offidx, last - 1),
				.length = list->content_sz
		};

		if (as_unpack_size(&pk_buf) <= 0) {
			return false;
		}

		offset_index_set(offidx, last, pk_buf.offset);
		find->result = list->ele_count;

		for (uint32_t i = last; i < list->ele_count; i++) {
			pk_start.offset = 0; // reset

			msgpack_compare_t cmp = as_unpack_compare(&pk_start, &pk_buf);

			offset_index_set(offidx, i + 1, pk_buf.offset);

			if (cmp == MSGPACK_COMPARE_EQUAL) {
				find->found = true;

				if (i != list->ele_count - 1 && i < find->target &&
						find->target <= list->ele_count) {
					continue;
				}

				find->result = i;
				offset_index_set_filled(offidx, MIN(i + 2, list->ele_count));
				break;
			}

			if (cmp == MSGPACK_COMPARE_LESS) {
				find->result = i - (find->found ? 1 : 0);
				offset_index_set_filled(offidx, MIN(i + 2, list->ele_count));
				break;
			}

			if (cmp == MSGPACK_COMPARE_END || cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}
		}

		if (find->result == list->ele_count) {
			offset_index_set_filled(offidx, list->ele_count);
		}
	}

	return true;
}

static uint32_t
packed_list_find_idx_offset(const packed_list *list, uint32_t index)
{
	if (index == 0) {
		return 0;
	}

	if (list_is_ordered(list)) {
		if (offset_index_is_valid(&list->offidx)) {
			offset_index *offidx = (offset_index *)&list->offidx;

			if (! list_full_offset_index_fill_to(offidx, index, false)) {
				return 0;
			}

			return offset_index_get_const(offidx, index);
		}

		define_offset_index(offidx, list->contents, list->content_sz,
				list->ele_count);

		if (! list_full_offset_index_fill_to(&offidx, index, false)) {
			return 0;
		}

		return offset_index_get_const(&offidx, index);
	}
	else if (offset_index_is_valid(&list->full_offidx) &&
			index < offset_index_get_filled(&list->full_offidx)) {
		return offset_index_get_const(&list->full_offidx, index);
	}

	as_unpacker pk = {
			.buffer = list->contents,
			.length = list->content_sz
	};

	uint32_t steps = index;

	if (offset_index_is_valid(&list->offidx)) {
		uint32_t idx = index / PACKED_LIST_INDEX_STEP;
		uint32_t filled = offset_index_get_filled(&list->offidx);

		if (idx >= filled) {
			cf_assert(filled != 0, AS_PARTICLE, "packed_list_op_find_idx_offset() filled is zero");
			idx = filled - 1;
		}

		pk.offset = offset_index_get_const(&list->offidx, idx);
		steps -= idx * PACKED_LIST_INDEX_STEP;

		offset_index *offidx = (offset_index *)&list->offidx; // mutable struct variable
		uint32_t blocks = steps / PACKED_LIST_INDEX_STEP;

		steps %= PACKED_LIST_INDEX_STEP;

		for (uint32_t i = 0; i < blocks; i++) {
			for (uint32_t j = 0; j < PACKED_LIST_INDEX_STEP; j++) {
				if (as_unpack_size(&pk) <= 0) {
					return 0;
				}
			}

			idx++;
			offset_index_set_next(offidx, idx, pk.offset);
		}
	}

	for (uint32_t i = 0; i < steps; i++) {
		if (as_unpack_size(&pk) <= 0) {
			return 0;
		}
	}

	return pk.offset;
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

	as_unpacker pk = {
			.buffer = list->contents,
			.offset = offset0,
			.length = list->content_sz
	};

	uint32_t steps = index - index0;

	if (offset_index_is_valid(&list->offidx)) {
		uint32_t idx0 = index0 / PACKED_LIST_INDEX_STEP;
		uint32_t idx = index / PACKED_LIST_INDEX_STEP;
		uint32_t filled = offset_index_get_filled(&list->offidx);

		if (idx0 != idx) {
			if (idx0 < filled - 1) {
				return packed_list_find_idx_offset(list, index);
			}

			uint32_t mod0 = index0 % PACKED_LIST_INDEX_STEP;
			offset_index *offidx = (offset_index *)&list->offidx;

			if (mod0 != 0) {
				for (uint32_t i = mod0; i < PACKED_LIST_INDEX_STEP; i++) {
					if (as_unpack_size(&pk) <= 0) {
						return 0;
					}

					steps--;
				}

				idx0++;
				offset_index_set_next(offidx, idx0, pk.offset);
			}

			uint32_t blocks = idx - idx0;

			for (uint32_t i = 0; i < blocks; i++) {
				for (uint32_t j = 0; j < PACKED_LIST_INDEX_STEP; j++) {
					if (as_unpack_size(&pk) <= 0) {
						return 0;
					}
				}

				idx0++;
				offset_index_set_next(offidx, idx0, pk.offset);
			}

			steps -= blocks * PACKED_LIST_INDEX_STEP;
		}
	}

	for (uint32_t i = 0; i < steps; i++) {
		if (as_unpack_size(&pk) <= 0) {
			return 0;
		}
	}

	return pk.offset;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value:
//  [value_start, value_start].
static bool
packed_list_find_rank_range_by_value_interval_ordered(const packed_list *list,
		const cdt_payload *value_start, const cdt_payload *value_end,
		uint32_t *rank_r, uint32_t *count_r, bool is_multi)
{
	cf_assert(offset_index_is_valid(list_full_offidx_p(list)), AS_PARTICLE, "packed_list_find_rank_range_by_value_interval_ordered() invalid full offset_index");
	cf_assert(value_end, AS_PARTICLE, "value_end == NULL");

	order_index_find find = {
			.target = 0
	};

	if (! packed_list_find_by_value_ordered(list, value_start, &find)) {
		return false;
	}

	*rank_r = find.result;

	if (value_end == value_start) {
		if (! find.found) {
			*count_r = 0;
		}
		else if (is_multi) {
			find.start = find.result + 1;
			find.target = list->ele_count;

			if (! packed_list_find_by_value_ordered(list, value_start, &find)) {
				return false;
			}

			if (find.found) {
				*count_r = find.result - *rank_r + 1;
			}
			else {
				*count_r = 1;
			}
		}
		else {
			*count_r = 1;
		}

		return true;
	}

	if (! value_end->ptr) {
		*count_r = list->ele_count - *rank_r;
		return true;
	}

	as_unpacker pk_start = {
			.buffer = value_start->ptr,
			.length = value_start->sz
	};

	as_unpacker pk_end = {
			.buffer = value_end->ptr,
			.length = value_end->sz
	};

	msgpack_compare_t cmp = as_unpack_compare(&pk_start, &pk_end);

	if (cmp == MSGPACK_COMPARE_GREATER || cmp == MSGPACK_COMPARE_EQUAL) {
		*count_r = 0;
		return true;
	}

	find.start = find.result;

	if (! packed_list_find_by_value_ordered(list, value_end, &find)) {
		return false;
	}

	*count_r = find.result - *rank_r;

	return true;
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

	as_unpacker pk_start = {
			.buffer = value_start->ptr,
			.length = value_start->sz
	};

	as_unpacker pk_end = {
			.buffer = value_end->ptr,
			.length = value_end->sz
	};

	offset_index *full_offidx = list_full_offidx_p(list);

	if (! offset_index_is_valid(full_offidx)) {
		full_offidx = NULL;
	}

	// Pre-check parameters.
	if (as_unpack_size(&pk_start) <= 0) {
		cf_warning(AS_PARTICLE, "packed_list_op_find_rank_range_by_value_interval_unordered() invalid start value");
		return false;
	}

	if (value_end != value_start && value_end->ptr &&
			as_unpack_size(&pk_end) <= 0) {
		cf_warning(AS_PARTICLE, "packed_list_op_find_rank_range_by_value_interval_unordered() invalid end value");
		return false;
	}

	*rank = 0;
	*count = 0;

	as_unpacker pk = {
			.buffer = list->contents,
			.length = list->content_sz
	};

	for (uint32_t i = 0; i < list->ele_count; i++) {
		uint32_t value_offset = pk.offset; // save for pk_end

		pk_start.offset = 0; // reset

		msgpack_compare_t cmp_start = as_unpack_compare(&pk, &pk_start);

		if (full_offidx) {
			offset_index_set(full_offidx, i + 1, pk.offset);
		}

		if (cmp_start == MSGPACK_COMPARE_ERROR) {
			cf_warning(AS_PARTICLE, "packed_list_op_find_rank_range_by_value_interval_unordered() invalid packed list at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_COMPARE_LESS) {
			(*rank)++;

			if (inverted) {
				if (mask_val) {
					cdt_idx_mask_set(mask_val, i);
				}

				(*count)++;
			}
		}
		else if (value_start != value_end) {
			msgpack_compare_t cmp_end = MSGPACK_COMPARE_LESS;

			// NULL value_end means largest possible value.
			if (value_end->ptr) {
				pk.offset = value_offset;
				pk_end.offset = 0;
				cmp_end = as_unpack_compare(&pk, &pk_end);
			}

			if ((cmp_end == MSGPACK_COMPARE_LESS && ! inverted) ||
					((cmp_end == MSGPACK_COMPARE_GREATER ||
							cmp_end == MSGPACK_COMPARE_EQUAL) && inverted)) {
				if (mask_val) {
					cdt_idx_mask_set(mask_val, i);
				}

				(*count)++;
			}
		}
		// Single value case.
		else if (cmp_start == MSGPACK_COMPARE_EQUAL) {
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
packed_list_remove_by_idx(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const uint64_t rm_idx, uint32_t *rm_sz)
{
	define_packed_list_op(op, list);

	if (! packed_list_op_remove(&op, rm_idx, 1)) {
		cf_warning(AS_PARTICLE, "packed_list_remove_by_idx() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	if (op.new_ele_count == 0) {
		as_bin_set_empty_list(b, alloc_buf, list_is_ordered(list));
	}
	else {
		uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags,
				op.new_content_sz, op.new_ele_count, rm_idx, &list->offidx,
				NULL);

		ptr += packed_list_op_write_seg1(&op, ptr);
		packed_list_op_write_seg2(&op, ptr);
	}

	*rm_sz = list->content_sz - op.new_content_sz;

	return AS_OK;
}

static int
packed_list_remove_by_mask(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const uint64_t *rm_mask, uint32_t rm_count,
		uint32_t *rm_sz)
{
	offset_index *full_offidx = list_full_offidx_p(list);

	*rm_sz = cdt_idx_mask_get_content_sz(rm_mask, rm_count, full_offidx);

	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags,
			list->content_sz - *rm_sz, list->ele_count - rm_count, 0, NULL,
			&new_offidx);

	ptr = cdt_idx_mask_write_eles(rm_mask, rm_count, full_offidx, ptr, true);
	cf_assert(ptr == ((list_mem *)b->particle)->data + ((list_mem *)b->particle)->sz, AS_PARTICLE,
			"packed_list_remove_idx_mask() pack mismatch ptr %p data %p sz %u [%p]", ptr, ((list_mem *)b->particle)->data, ((list_mem *)b->particle)->sz, ((list_mem *)b->particle)->data + ((list_mem *)b->particle)->sz);

	if (offset_index_is_valid(&new_offidx)) {
		list_offset_index_rm_mask_cpy(&new_offidx, full_offidx, rm_mask,
				rm_count);
	}

	return AS_OK;
}

// Assumes index/count(non-zero) is surrounded by other elements.
static int
packed_list_trim(const packed_list *list, as_bin *b, rollback_alloc *alloc_buf,
		uint32_t index, uint32_t count, cdt_result_data *result)
{
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

	if (b) {
		uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags, content_sz,
				count, 0, &list->offidx, NULL);

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

		as_unpacker pk = {
				.buffer = list->contents,
				.length = list->content_sz
		};

		if (! packed_list_builder_add_ranks_by_range(list, &builder, &pk, index,
				result->type == RESULT_TYPE_REVRANK)) {
			cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
			return -AS_ERR_PARAMETER;
		}

		pk.offset = offset1;

		if (! packed_list_builder_add_ranks_by_range(list, &builder, &pk,
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
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_index_range(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, int64_t index, uint64_t count,
		cdt_result_data *result)
{
	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, list->ele_count, &uindex, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() index %ld out of bounds for ele_count %u", index, list->ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (result_data_is_inverted(result)) {
		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() INVERTED flag not supported for single result ops");
			return -AS_ERR_PARAMETER;
		}

		if (result_data_is_return_index_range(result) ||
				result_data_is_return_rank_range(result)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() result_type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_PARAMETER;
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
			return packed_list_trim(list, b, alloc_buf, uindex, count32,
					result);
		}
	}

	if (count32 == 0) {
		if (! list_result_data_set_not_found(result, uindex)) {
			return -AS_ERR_PARAMETER;
		}

		return AS_OK;
	}

	define_packed_list_op(op, list);

	if (! packed_list_op_remove(&op, uindex, count32)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_index_range() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	if (b) {
		if (op.new_ele_count == 0) {
			as_bin_set_empty_list(b, alloc_buf, list_is_ordered(list));
		}
		else {
			uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags,
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

		as_unpacker pk = {
				.buffer = list->contents + op.seg1_sz,
				.length = list->content_sz - op.new_content_sz
		};

		uint32_t rm_count = list->ele_count - op.new_ele_count;
		define_int_list_builder(builder, result->alloc, rm_count);

		if (list_is_ordered(list)) {
			cdt_container_builder_add_int_range(&builder, uindex, count32,
					list->ele_count, result->type == RESULT_TYPE_REVRANK);
		}
		else if (! packed_list_builder_add_ranks_by_range(list, &builder, &pk,
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
		return -AS_ERR_PARAMETER;
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(b)) {
		cdt_bin_print(b, "packed_list_get_remove_by_index_range");
	}
#endif

	return AS_OK;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_list_get_remove_by_value_interval(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value_start,
		const cdt_payload *value_end, cdt_result_data *result)
{
	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_value_interval() INVERTED flag not supported for single result ops");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rank;
	vla_list_full_offidx_if_invalid(u, list);

	if (list_is_ordered(list)) {
		uint32_t count;

		if (! packed_list_find_rank_range_by_value_interval_ordered(list,
				value_start, value_end, &rank, &count, result->is_multi)) {
			return -AS_ERR_PARAMETER;
		}

		if (count == 0 && ! result->is_multi) {
			if (! list_result_data_set_not_found(result, 0)) {
				return -AS_ERR_PARAMETER;
			}

			return AS_OK;
		}

		return packed_list_get_remove_by_index_range(list, b, alloc_buf,
				(int64_t)rank, (uint64_t)count, result);
	}

	uint32_t rm_count;
	define_cdt_idx_mask(rm_mask, result->is_multi ? list->ele_count : 1);

	if (! packed_list_find_rank_range_by_value_interval_unordered(list,
			value_start, value_end, &rank, &rm_count, rm_mask, inverted,
			result->is_multi)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (b) {
		if (rm_count == list->ele_count) {
			as_bin_set_unordered_empty_list(b, alloc_buf);
		}
		else if (rm_count != 0) {
			int ret;

			if (result->is_multi) {
				ret = packed_list_remove_by_mask(list, b, alloc_buf, rm_mask,
					rm_count, &rm_sz);
			}
			else {
				// rm_mask[0] is an idx for single value finds.
				ret = packed_list_remove_by_idx(list, b, alloc_buf, rm_mask[0],
						&rm_sz);
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
			define_order_index2(rm_idx, list->ele_count, 1);

			order_index_set(&rm_idx, 0, rm_mask[0]);
			list_result_data_set_values_by_ordidx(result, &rm_idx, u->offidx,
					rm_count, rm_sz);
		}
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_value_interval() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_rank_range(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, int64_t rank, uint64_t count,
		cdt_result_data *result)
{
	bool inverted = result_data_is_inverted(result);

	if (inverted && ! result->is_multi) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() INVERTED flag not supported for single result ops");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(list)) {
		// idx == rank for ordered lists.
		return packed_list_get_remove_by_index_range(list, b, alloc_buf, rank,
				count, result);
	}

	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, list->ele_count, &urank, &count32,
			result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() rank %u out of bounds for ele_count %u", urank, list->ele_count);
		return -AS_ERR_PARAMETER;
	}

	vla_list_full_offidx_if_invalid(full, list);

	if (! list_full_offset_index_fill_all(full->offidx, false)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	define_build_order_heap_by_range(heap, urank, count32, list->ele_count,
			list, list_order_heap_cmp_fn, success);

	if (! success) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_count = inverted ? list->ele_count - count32 : count32;

	if (rm_count == 0) {
		if (! list_result_data_set_not_found(result, urank)) {
			return -AS_ERR_PARAMETER;
		}

		packed_list_partial_offidx_update(list);

		return AS_OK;
	}

	define_cdt_idx_mask(rm_mask, list->ele_count);
	order_index ret_idx;

	cdt_idx_mask_set_by_ordidx(rm_mask, &heap._, heap.filled, count32,
			inverted);

	if (inverted) {
		if (result_data_is_return_rank_range(result)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() result_type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_PARAMETER;
		}

		if (! result->is_multi) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_by_rank_range() singe result type %d not supported with INVERTED flag", result->type);
			return -AS_ERR_PARAMETER;
		}
	}
	else {
		order_index_init_ref(&ret_idx, &heap._, heap.filled, rm_count);
	}

	uint32_t rm_sz = 0;

	if (b) {
		int ret = packed_list_remove_by_mask(list, b, alloc_buf, rm_mask,
				rm_count, &rm_sz);

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
		return result_data_set_range(result, rank, count32, list->ele_count);
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
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

static int
packed_list_get_remove_all_by_value_list_ordered(const packed_list *list,
		as_bin *b, rollback_alloc *alloc_buf, as_unpacker *items_pk,
		uint32_t items_count, cdt_result_data *result)
{
	cf_assert(result->is_multi, AS_PARTICLE, "not supported");

	if (! list_full_offset_index_fill_all(list_full_offidx_p(list), false)) {
		cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	define_order_index2(rm_rc, list->ele_count, 2 * items_count);
	uint32_t rc_count = 0;

	for (uint32_t i = 0; i < items_count; i++) {
		cdt_payload value = { items_pk->buffer + items_pk->offset };
		int64_t sz = as_unpack_size(items_pk);

		if (sz <= 0) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() invalid list");
			return -AS_ERR_PARAMETER;
		}

		value.sz = (uint32_t)sz;

		uint32_t rank;
		uint32_t count;

		if (! packed_list_find_rank_range_by_value_interval_ordered(list,
				&value, &value, &rank, &count, true)) {
			cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list_ordered() invalid list");
			return -AS_ERR_PARAMETER;
		}

		order_index_set(&rm_rc, 2 * i, rank);
		order_index_set(&rm_rc, (2 * i) + 1, count);
		rc_count += count;
	}

	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;
	bool inverted = result_data_is_inverted(result);
	bool need_mask = (b || result->type == RESULT_TYPE_COUNT ||
			(inverted && result->type != RESULT_TYPE_NONE));
	cond_define_cdt_idx_mask(rm_mask, list->ele_count, need_mask);

	if (need_mask) {
		cdt_idx_mask_set_by_irc(rm_mask, &rm_rc, NULL, inverted);
		rm_count = cdt_idx_mask_bit_count(rm_mask, list->ele_count);
	}

	if (b) {
		if (rm_count == list->ele_count) {
			as_bin_set_ordered_empty_list(b, alloc_buf);
		}
		else if (rm_count != 0) {
			int ret = packed_list_remove_by_mask(list, b, alloc_buf, rm_mask,
					rm_count, &rm_sz);

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
		return -AS_ERR_PARAMETER;
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(b)) {
		cdt_bin_print(b, "packed_list_get_remove_all_by_value_list_ordered");
		list_print(list, "original");
		cf_crash(AS_PARTICLE, "all_by_value_list_ordered: ele_count %u items_count %u rm_count %u", list->ele_count, items_count, rm_count);
	}
#endif

	return AS_OK;
}

static int
packed_list_get_remove_all_by_value_list(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value_list,
		cdt_result_data *result)
{
	if (result_data_is_return_rank_range(result) ||
			result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_list_op_get_remove_all_by_value_list() result_type %d not supported", result->type);
		return -AS_ERR_PARAMETER;
	}

	as_unpacker items_pk;
	uint32_t items_count;

	if (! list_param_parse(value_list, &items_pk, &items_count)) {
		return -AS_ERR_PARAMETER;
	}

	bool inverted = result_data_is_inverted(result);

	if (items_count == 0) {
		if (! inverted) {
			if (! list_result_data_set_not_found(result, 0)) {
				cf_warning(AS_PARTICLE, "packed_list_get_remove_all_by_value_list() invalid result type %d", result->type);
				return -AS_ERR_PARAMETER;
			}

			return AS_OK;
		}

		result->flags &= ~AS_CDT_OP_FLAG_INVERTED;

		return packed_list_get_remove_by_index_range(list, b, alloc_buf, 0,
				list->ele_count, result);
	}

	vla_list_full_offidx_if_invalid(full, list);

	if (list_is_ordered(list)) {
		return packed_list_get_remove_all_by_value_list_ordered(list, b,
				alloc_buf, &items_pk, items_count, result);
	}

	bool is_ret_rank = result_data_is_return_rank(result);
	uint32_t rm_count = 0;
	define_order_index(value_list_ordidx, items_count);
	define_cdt_idx_mask(rm_mask, list->ele_count);
	cond_vla_order_index2(rc, list->ele_count, items_count * 2, is_ret_rank);

	if (! offset_index_find_items(full->offidx,
			CDT_FIND_ITEMS_IDXS_FOR_LIST_VALUE, &items_pk, &value_list_ordidx,
			inverted, rm_mask, &rm_count, is_ret_rank ? &rc.ordidx : NULL)) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;

	if (b) {
		if (rm_count == list->ele_count) {
			as_bin_set_unordered_empty_list(b, alloc_buf);
		}
		else if (rm_count != 0) {
			int ret = packed_list_remove_by_mask(list, b, alloc_buf, rm_mask,
					rm_count, &rm_sz);

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
		result_data_set_by_itemlist_irc(result, &value_list_ordidx,
				&rc.ordidx, rm_count);
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
		return -AS_ERR_PARAMETER;
	}

	return AS_OK;
}

static int
packed_list_get_remove_by_rel_rank_range(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *value, int64_t rank,
		uint64_t count, cdt_result_data *result)
{
	vla_list_full_offidx_if_invalid(u, list);

	if (list_is_ordered(list)) {
		uint32_t rel_rank;
		uint32_t temp;

		if (! packed_list_find_rank_range_by_value_interval_ordered(list,
				value, value, &rel_rank, &temp, result->is_multi)) {
			return -AS_ERR_PARAMETER;
		}

		calc_rel_index_count(rank, count, rel_rank, &rank, &count);

		return packed_list_get_remove_by_index_range(list, b, alloc_buf, rank,
				count, result);
	}

	uint32_t rel_rank;
	uint32_t temp;

	if (! packed_list_find_rank_range_by_value_interval_unordered(list,
			value, value, &rel_rank, &temp, NULL,
			result_data_is_inverted(result), result->is_multi)) {
		return -AS_ERR_PARAMETER;
	}

	calc_rel_index_count(rank, count, rel_rank, &rank, &count);

	return packed_list_get_remove_by_rank_range(list, b, alloc_buf, rank,
			count, result);
}

static int
packed_list_insert(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, int64_t index, const cdt_payload *payload,
		bool payload_is_list, uint64_t mod_flags, cdt_result_data *result)
{
	uint32_t param_count = 1;
	uint32_t payload_hdr_sz = 0;

	if (payload_is_list) {
		int64_t payload_count =
				as_unpack_buf_list_element_count(payload->ptr, payload->sz);

		if (payload_count < 0) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid payload, expected a list");
			return -AS_ERR_PARAMETER;
		}

		if (payload_count == 0) {
			result_data_set_int(result, list->ele_count);
			return AS_OK;
		}

		param_count = (uint32_t)payload_count;
		payload_hdr_sz = as_pack_list_header_get_size(param_count);

		if (payload_hdr_sz > payload->sz) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid list header: payload->size=%d", payload->sz);
			return -AS_ERR_PARAMETER;
		}
	}

	if (! cdt_check_storage_list_contents(payload->ptr + payload_hdr_sz,
			payload->sz - payload_hdr_sz, param_count)) {
		cf_warning(AS_PARTICLE, "packed_list_insert() invalid payload");
		return -AS_ERR_PARAMETER;
	}

	if (index > INT32_MAX || (index = calc_index(index, list->ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() index %ld out of bounds for ele_count %d", index > 0 ? index : index - list->ele_count, list->ele_count);
		return -AS_ERR_PARAMETER;
	}

	if (mod_flags_is_bounded(mod_flags) && (uint32_t)index > list->ele_count) {
		if (mod_flags_is_no_fail(mod_flags)) {
			result_data_set_int(result, list->ele_count);
			return AS_OK; // no-op
		}

		return -AS_ERR_PARAMETER;
	}

	uint32_t rm_sz = 0;
	uint32_t rm_count = 0;
	bool is_unique = mod_flags_is_unique(mod_flags);
	cond_define_cdt_idx_mask(pfound, param_count, is_unique);

	if (is_unique) {
		// Assume only here for the unordered case.
		if (payload_is_list) {
			as_unpacker pk = {
					.buffer = payload->ptr + payload_hdr_sz,
					.length = payload->sz - payload_hdr_sz
			};

			for (uint32_t i = 0; i < param_count; i++) {
				cdt_payload val = { pk.buffer + pk.offset };
				int64_t sz = as_unpack_size(&pk);
				uint32_t rank;
				uint32_t count;

				if (sz <= 0) {
					cf_warning(AS_PARTICLE, "packed_list_insert() invalid parameters");
					return -AS_ERR_PARAMETER;
				}

				val.sz = (uint32_t)sz;

				if (! packed_list_find_rank_range_by_value_interval_unordered(
						list, &val, &val, &rank, &count, NULL, false, false)) {
					return -AS_ERR_PARAMETER;
				}

				if (count == 0) {
					as_unpacker cmp0 = {
							.buffer = val.ptr,
							.length = val.sz
					};

					as_unpacker cmp1 = pk;
					bool found = false;

					cmp1.offset = 0;

					for (uint32_t j = 0; j < i; j++) {
						cmp0.offset = 0;

						msgpack_compare_t cmp = as_unpack_compare(&cmp0, &cmp1);

						if (cmp == MSGPACK_COMPARE_EQUAL) {
							if (! mod_flags_is_no_fail(mod_flags)) {
								return -AS_ERR_PARAMETER;
							}

							if (! mod_flags_is_do_partial(mod_flags)) {
								as_bin_set_int(result->result, list->ele_count);
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
						as_bin_set_int(result->result, list->ele_count);
						return mod_flags_return_exists(mod_flags);
					}
				}
			}

			if (param_count == rm_count) {
				as_bin_set_int(result->result, list->ele_count);
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
				as_bin_set_int(result->result, list->ele_count);
				return mod_flags_return_exists(mod_flags);
			}
		}
	}

	uint32_t uindex = (uint32_t)index;
	define_packed_list_op(op, list);
	uint32_t insert_sz = payload->sz - payload_hdr_sz - rm_sz;
	uint32_t add_count = param_count - rm_count;

	if (! packed_list_op_insert(&op, uindex, add_count, insert_sz)) {
		cf_warning(AS_PARTICLE, "packed_list_insert() packed_list_op_insert failed");
		return -AS_ERR_PARAMETER;
	}

	uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags,
			op.new_content_sz, op.new_ele_count, uindex, &list->offidx, NULL);

	ptr += packed_list_op_write_seg1(&op, ptr);

	const uint8_t *p = payload->ptr + payload_hdr_sz;

	if (rm_sz == 0) {
		uint32_t sz = payload->sz - payload_hdr_sz;

		memcpy(ptr, p, sz);
		ptr += sz;
	}
	else {
		as_unpacker pk = {
				.buffer = payload->ptr + payload_hdr_sz,
				.length = payload->sz - payload_hdr_sz
		};

		uint32_t idx = 0;

		for (uint32_t i = 0; i < add_count; i++) {
			uint32_t next = cdt_idx_mask_find(pfound, idx, param_count, false);
			uint32_t skip = next - idx;

			for (uint32_t j = 0; j < skip; j++) {
				as_unpack_size(&pk);
			}

			const uint8_t *begin = pk.buffer + pk.offset;
			size_t sz = (size_t)as_unpack_size(&pk);

			memcpy(ptr, begin, sz);
			ptr += sz;
			idx = next + 1;
		}
	}

	packed_list_op_write_seg2(&op, ptr);
	result_data_set_int(result, op.new_ele_count);

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(b)) {
		cdt_bin_print(b, "packed_list_insert");
	}
#endif

	return AS_OK;
}

static int
packed_list_add_ordered(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *payload,
		uint64_t mod_flags, cdt_result_data *result)
{
	vla_list_full_offidx_if_invalid(full, list);

	order_index_find find = {
			.target = list->ele_count + 1
	};

	if (! packed_list_find_by_value_ordered(list, payload, &find)) {
		cf_warning(AS_PARTICLE, "packed_list_add_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (find.found && mod_flags_is_unique(mod_flags)) {
		as_bin_set_int(result->result, list->ele_count);
		return mod_flags_return_exists(mod_flags);
	}

	return packed_list_insert(list, b, alloc_buf, (int64_t)find.result, payload,
			false, AS_CDT_LIST_MODIFY_DEFAULT, result);
}

static int
packed_list_add_items_ordered(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, const cdt_payload *items, uint64_t mod_flags,
		cdt_result_data *result)
{
	int64_t add_count = as_unpack_buf_list_element_count(items->ptr, items->sz);

	if (add_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid payload, expected a list");
		return -AS_ERR_PARAMETER;
	}

	if (add_count == 0) {
		result_data_set_int(result, list->ele_count);
		return AS_OK; // no-op
	}

	uint32_t val_count = (uint32_t)add_count;
	uint32_t hdr_sz = as_pack_list_header_get_size(val_count);

	if (hdr_sz > items->sz) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list header: payload->size=%d", items->sz);
		return -AS_ERR_PARAMETER;
	}

	// Sort items to add.
	define_order_index(val_ord, val_count);
	define_offset_index(val_off, items->ptr + hdr_sz, items->sz - hdr_sz,
			val_count);

	if (! list_full_offset_index_fill_all(&val_off, true) ||
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
				return -AS_ERR_PARAMETER;
			}

			if (! mod_flags_is_do_partial(mod_flags)) {
				as_bin_set_int(result->result, list->ele_count);
				return AS_OK;
			}
		}
	}

	vla_list_full_offidx_if_invalid(full, list);
	define_order_index2(insert_idx, list->ele_count, val_count);
	uint32_t new_content_sz = list->content_sz;
	uint32_t new_ele_count = list->ele_count;

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

		if (! packed_list_find_by_value_ordered(list, &value, &find)) {
			cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list");
			return -AS_ERR_PARAMETER;
		}

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

	if (! list_full_offset_index_fill_all(full->offidx, false)) {
		cf_warning(AS_PARTICLE, "packed_list_add_items_ordered() invalid list");
		return -AS_ERR_PARAMETER;
	}

	// Construct new list.
	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags, new_content_sz,
			new_ele_count, 0, &list->offidx, &new_offidx);

	uint32_t list_start = 0;
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
			offset_index_copy(&new_offidx, &list->offidx, new_idx, list_start,
					seg_count, cpy_delta);
			list_start = list_idx;
			new_idx += seg_count;
			cur_offset = off1 + cpy_delta;
		}

		offset_index_set(&new_offidx, new_idx++, cur_offset);

		uint32_t off = offset_index_get_const(&val_off, val_idx);
		uint32_t val_sz = offset_index_get_delta_const(&val_off, val_idx);

		memcpy(ptr, items->ptr + hdr_sz + off, val_sz);
		ptr += val_sz;
		cpy_delta += val_sz;
		cur_offset += val_sz;
	}

	if (list_start < list->ele_count && list->ele_count != 0) {
		uint32_t off = offset_index_get_const(&list->offidx, list_start);
		uint32_t seg_count = list->ele_count - list_start;

		memcpy(ptr, list->contents + off, list->content_sz - off);
		offset_index_copy(&new_offidx, &list->offidx, new_idx, list_start,
				seg_count, cpy_delta);
	}

	offset_index_set_filled(&new_offidx, new_ele_count);
	result_data_set_int(result, new_ele_count);

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(b)) {
		cdt_bin_print(b, "packed_list_add_items_ordered");
		list_print(list, "original");
		cf_crash(AS_PARTICLE, "add_items_ordered: val_count %u", val_count);
	}
#endif

	return AS_OK;
}

static int
packed_list_replace_ordered(const packed_list *list, as_bin *b,
		rollback_alloc *alloc_buf, uint32_t index, const cdt_payload *value,
		uint64_t mod_flags)
{
	uint32_t rank;
	uint32_t count;
	vla_list_full_offidx_if_invalid(u, list);

	if (! packed_list_find_rank_range_by_value_interval_ordered(list,
			value, value, &rank, &count, false)) {
		return -AS_ERR_PARAMETER;
	}

	define_packed_list_op(op, list);

	if (index > list->ele_count) {
		cf_warning(AS_PARTICLE, "packed_list_replace_ordered() index %u > ele_count %u out of bounds not allowed for ORDERED lists", index, list->ele_count);
		return -AS_ERR_PARAMETER;
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

	uint8_t *ptr = list_setup_bin(b, alloc_buf, list->ext_flags,
			op.new_content_sz, new_ele_count, (rank < index) ? rank : index,
					&list->offidx, NULL);
	uint32_t offset = offset_index_get_const(u->offidx, rank);

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
		cdt_container_builder *builder, as_unpacker *start, uint32_t count,
		bool reverse)
{
	for (uint32_t i = 0; i < count; i++) {
		cdt_payload value = {
				.ptr = start->buffer + start->offset
		};

		int64_t sz = as_unpack_size(start);
		uint32_t rank;
		uint32_t rcount;

		if (sz <= 0) {
			return false;
		}

		value.sz = (uint32_t)sz;

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
list_set_flags(as_bin *b, rollback_alloc *alloc_buf, uint8_t set_flags,
		cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
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
			return AS_OK; // no-op
		}
	}

	offset_index new_offidx;
	uint8_t * const ptr = list_setup_bin(b, alloc_buf, set_flags,
			list.content_sz, list.ele_count, reorder ? 0 : list.ele_count,
					&list.offidx, &new_offidx);

	if (! reorder) {
		memcpy(ptr, list.contents, list.content_sz);
	}
	else {
		vla_list_full_offidx_if_invalid(full, &list);

		if (! list_full_offset_index_fill_all(full->offidx, false)) {
			cf_warning(AS_PARTICLE, "list_set_flags() invalid list");
			return -AS_ERR_PARAMETER;
		}

		define_order_index(ordidx, list.ele_count);

		if (! list_order_index_sort(&ordidx, full->offidx,
				AS_CDT_SORT_ASCENDING)) {
			cf_warning(AS_PARTICLE, "list_set_flags() invalid list");
			return -AS_ERR_PARAMETER;
		}

		list_order_index_pack(&ordidx, full->offidx, ptr, &new_offidx);
	}

#ifdef LIST_DEBUG_VERIFY
	if (! list_verify(b)) {
		cdt_bin_print(b, "set_flags");
		list_print(&list, "original");
		cf_crash(AS_PARTICLE, "set_flags: set_flags %u", set_flags);
	}
#endif

	return AS_OK;
}

static int
list_append(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload,
		bool payload_is_list, uint64_t mod_flags, cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_append() invalid packed list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		if (! payload_is_list) {
			return packed_list_add_ordered(&list, b, alloc_buf, payload,
					mod_flags, result);
		}

		return packed_list_add_items_ordered(&list, b, alloc_buf, payload,
				mod_flags, result);
	}

	return packed_list_insert(&list, b, alloc_buf, (int64_t)list.ele_count,
			payload, payload_is_list, mod_flags, result);
}

static int
list_insert(as_bin *b, rollback_alloc *alloc_buf, int64_t index,
		const cdt_payload *payload, bool payload_is_list, uint64_t mod_flags,
		cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_insert() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		cf_warning(AS_PARTICLE, "list_insert() invalid op on ORDERED list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_insert(&list, b, alloc_buf, index, payload,
			payload_is_list, mod_flags, result);
}

static int
list_set(as_bin *b, rollback_alloc *alloc_buf, int64_t index,
		const cdt_payload *value, uint64_t mod_flags)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_set() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list_is_ordered(&list)) {
		cf_warning(AS_PARTICLE, "list_set() invalid op on ORDERED list");
		return -AS_ERR_PARAMETER;
	}

	uint32_t ele_count = list.ele_count;

	if (index >= ele_count) {
		return packed_list_insert(&list, b, alloc_buf, index, value, false,
				mod_flags, NULL);
	}

	if (index > UINT32_MAX || (index = calc_index(index, ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "list_set() index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
		return -AS_ERR_PARAMETER;
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

	if (! packed_list_op_remove(&op, uindex, 1)) {
		cf_warning(AS_PARTICLE, "list_set() as_packed_list_remove failed");
		return -AS_ERR_PARAMETER;
	}

	op.new_content_sz += value->sz;

	uint8_t *ptr = list_setup_bin(b, alloc_buf, list.ext_flags,
			op.new_content_sz, ele_count, uindex, &list.offidx, NULL);

	ptr += packed_list_op_write_seg1(&op, ptr);

	memcpy(ptr, value->ptr, value->sz);
	ptr += value->sz;

	packed_list_op_write_seg2(&op, ptr);

	return AS_OK;
}

static int
list_increment(as_bin *b, rollback_alloc *alloc_buf, int64_t index,
		cdt_payload *delta_value, uint64_t mod_flags, cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_increment() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (index > INT32_MAX || (index = calc_index(index, list.ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "list_increment() index %ld out of bounds for ele_count %d", index > 0 ? index : index - list.ele_count, list.ele_count);
		return -AS_ERR_PARAMETER;
	}

	uint32_t uindex = (uint32_t)index;
	cdt_calc_delta calc_delta;

	if (! cdt_calc_delta_init(&calc_delta, delta_value, false)) {
		return -AS_ERR_PARAMETER;
	}

	if (uindex < list.ele_count) {
		uint32_t offset = packed_list_find_idx_offset(&list, uindex);

		if (uindex != 0 && offset == 0) {
			cf_warning(AS_PARTICLE, "list_increment() unable to unpack element at %u", uindex);
			return -AS_ERR_PARAMETER;
		}

		as_unpacker pk = {
				.buffer = list.contents + offset,
				.length = list.content_sz - offset
		};

		if (! cdt_calc_delta_add(&calc_delta, &pk)) {
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

	cdt_calc_delta_pack_and_result(&calc_delta, &value, result->result);

	if (list_is_ordered(&list)) {
		return packed_list_replace_ordered(&list, b, alloc_buf, uindex, &value,
				mod_flags);
	}

	return list_set(b, alloc_buf, (int64_t)uindex, &value, mod_flags);
}

static int
list_sort(as_bin *b, rollback_alloc *alloc_buf, as_cdt_sort_flags sort_flags)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	if (list.ele_count <= 1) {
		return AS_OK;
	}

	vla_list_full_offidx_if_invalid(full, &list);

	if (! list_full_offset_index_fill_all(full->offidx, false)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	define_order_index(ordidx, list.ele_count);

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
			! order_index_sorted_mark_dup_eles(&ordidx, full->offidx,
					&rm_count, &rm_sz)) {
		cf_warning(AS_PARTICLE, "list_sort() invalid list");
		return -AS_ERR_PARAMETER;
	}

	offset_index new_offidx;
	uint8_t *ptr = list_setup_bin(b, alloc_buf, list.ext_flags,
			list.content_sz - rm_sz, list.ele_count - rm_count, 0, &list.offidx,
			&new_offidx);

	ptr = list_order_index_pack(&ordidx, full->offidx, ptr, &new_offidx);
	cf_assert(ptr == ((list_mem *)b->particle)->data + ((list_mem *)b->particle)->sz, AS_PARTICLE,
			"list_sort() pack mismatch ptr %p data %p sz %u [%p]", ptr, ((list_mem *)b->particle)->data, ((list_mem *)b->particle)->sz, ((list_mem *)b->particle)->data + ((list_mem *)b->particle)->sz);

	return AS_OK;
}

static int
list_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index,
		uint64_t count, cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_remove_by_index_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_index_range(&list, b, alloc_buf, index,
			count, result);
}

static int
list_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value_start, const cdt_payload *value_end,
		cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_remove_by_value_interval() invalid packed list, ele_count=%d", list.ele_count);
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_value_interval(&list, b, alloc_buf,
			value_start, value_end, result);
}

static int
list_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf, int64_t rank,
		uint64_t count, cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_remove_by_rank_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_rank_range(&list, b, alloc_buf, rank,
			count, result);
}

static int
list_remove_all_by_value_list(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value_list, cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_remove_all_by_value_list() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_all_by_value_list(&list, b, alloc_buf,
			value_list, result);
}

static int
list_remove_by_rel_rank_range(as_bin *b, rollback_alloc *alloc_buf,
		const cdt_payload *value, int64_t rank, uint64_t count,
		cdt_result_data *result)
{
	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_remove_by_rel_rank_range() invalid list");
		return -AS_ERR_PARAMETER;
	}

	return packed_list_get_remove_by_rel_rank_range(&list, b, alloc_buf, value,
			rank, count, result);
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
			list_offset_index_init(new_offidx, NULL, ele_count, NULL,
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
		list_offset_index_init(new_offidx, ptr, ele_count, contents,
				content_sz);
		idx_trunc /= PACKED_LIST_INDEX_STEP;
	}
	else {
		list_full_offset_index_init(new_offidx, ptr, ele_count, contents,
				content_sz);
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
		cdt_modify_data *cdt_udata)
{
	as_bin *b = cdt_udata->b;
	as_cdt_optype optype = state->type;

	if (as_bin_inuse(b) && ! is_list_type(as_bin_get_particle_type(b))) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid type %d", as_bin_get_particle_type(b));
		cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	define_rollback_alloc(alloc_buf, cdt_udata->alloc_buf, 5, true);
	// Results always on the heap.
	define_rollback_alloc(alloc_result, NULL, 1, false);
	int ret = AS_OK;

	cdt_result_data result = {
			.result = cdt_udata->result,
			.alloc = alloc_result,
	};

	switch (optype) {
	case AS_CDT_OP_LIST_SET_TYPE: {
		uint64_t list_type;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &list_type)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, AS_PACKED_LIST_FLAG_NONE);
		ret = list_set_flags(b, alloc_buf, (uint8_t)list_type, &result);
		break;
	}
	case AS_CDT_OP_LIST_APPEND: {
		cdt_payload value;
		uint64_t create_type = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &value, &create_type, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, create_type);
		ret = list_append(b, alloc_buf, &value, false, modify, &result);
		break;
	}
	case AS_CDT_OP_LIST_APPEND_ITEMS: {
		cdt_payload items;
		uint64_t create_type = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &create_type, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, create_type);
		ret = list_append(b, alloc_buf, &items, true, modify, &result);
		break;
	}
	case AS_CDT_OP_LIST_INSERT: {
		int64_t index;
		cdt_payload value;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &value, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, AS_PACKED_LIST_FLAG_NONE);
		ret = list_insert(b, alloc_buf, index, &value, false, modify, &result);
		break;
	}
	case AS_CDT_OP_LIST_INSERT_ITEMS: {
		int64_t index;
		cdt_payload items;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &items, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, AS_PACKED_LIST_FLAG_NONE);
		ret = list_insert(b, alloc_buf, index, &items, true, modify, &result);
		break;
	}
	case AS_CDT_OP_LIST_SET: {
		int64_t index;
		cdt_payload value;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &value, &modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, AS_PACKED_LIST_FLAG_NONE);
		ret = list_set(b, alloc_buf, index, &value, modify);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE:
	case AS_CDT_OP_LIST_POP: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, optype == AS_CDT_OP_LIST_REMOVE ?
				RESULT_TYPE_COUNT : RESULT_TYPE_VALUE, false);
		ret = list_remove_by_index_range(b, alloc_buf, index, 1, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_RANGE:
	case AS_CDT_OP_LIST_POP_RANGE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, optype == AS_CDT_OP_LIST_REMOVE_RANGE ?
				RESULT_TYPE_COUNT : RESULT_TYPE_VALUE, true);
		ret = list_remove_by_index_range(b, alloc_buf, index, count, &result);
		break;
	}
	case AS_CDT_OP_LIST_TRIM: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result.type = RESULT_TYPE_COUNT;
		result.flags = AS_CDT_OP_FLAG_INVERTED;
		result.is_multi = true;
		ret = list_remove_by_index_range(b, alloc_buf, index, count, &result);
		break;
	}
	case AS_CDT_OP_LIST_CLEAR: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		packed_list list;

		if (! packed_list_init_from_bin(&list, b)) {
			cf_warning(AS_PARTICLE, "LIST_CLEAR: invalid list");
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_empty_list(b, alloc_buf, list_is_ordered(&list));
		break;
	}
	case AS_CDT_OP_LIST_INCREMENT: {
		int64_t index;
		cdt_payload delta = { NULL };
		uint64_t create = AS_PACKED_LIST_FLAG_NONE;
		uint64_t modify = AS_CDT_LIST_MODIFY_DEFAULT;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &delta, &create,
				&modify)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		as_bin_set_temp_list_if_notinuse(b, create);
		ret = list_increment(b, alloc_buf, index, &delta, modify, &result);
		break;
	}
	case AS_CDT_OP_LIST_SORT: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &flags)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		ret = list_sort(b, alloc_buf, (as_cdt_sort_flags)flags);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_INDEX: {
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
		ret = list_remove_by_index_range(b, alloc_buf, index, 1, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE:
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type,
				optype == AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE);
		ret = list_remove_by_value_interval(b, alloc_buf, &value, &value,
				&result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_RANK: {
		if (! as_bin_inuse(b)) {
			return true; // no-op
		}

		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = list_remove_by_rank_range(b, alloc_buf, rank, 1, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST: {
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
		ret = list_remove_all_by_value_list(b, alloc_buf, &items, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE: {
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
		ret = list_remove_by_index_range(b, alloc_buf, index, count, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL: {
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
		ret = list_remove_by_value_interval(b, alloc_buf, &value_start,
				&value_end, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE: {
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
		ret = list_remove_by_rank_range(b, alloc_buf, rank, count, &result);
		break;
	}
	case AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE: {
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
		ret = list_remove_by_rel_rank_range(b, alloc_buf, &value, rank, count,
				&result);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid cdt op: %d", optype);
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

	// In case of no-op.
	if (b->particle == (const as_particle *)&list_mem_empty) {
		as_bin_set_unordered_empty_list(b, alloc_buf);
	}
	else if (b->particle == (const as_particle *)&list_ordered_empty) {
		as_bin_set_ordered_empty_list(b, alloc_buf);
	}

	return true;
}

bool
cdt_process_state_packed_list_read_optype(cdt_process_state *state,
		cdt_read_data *cdt_udata)
{
	const as_bin *b = cdt_udata->b;
	as_cdt_optype optype = state->type;

	if (! is_list_type(as_bin_get_particle_type(b))) {
		cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
		return false;
	}

	packed_list list;

	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "%s: invalid list", cdt_process_state_get_op_name(state));
		cdt_udata->ret_code = -AS_ERR_INCOMPATIBLE_TYPE;
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
	case AS_CDT_OP_LIST_GET: {
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, RESULT_TYPE_VALUE, false);
		ret = packed_list_get_remove_by_index_range(&list, NULL, NULL, index,
				1, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_RANGE: {
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, RESULT_TYPE_VALUE, true);
		ret = packed_list_get_remove_by_index_range(&list, NULL, NULL, index,
				count, &result);
		break;
	}
	case AS_CDT_OP_LIST_SIZE: {
		as_bin_set_int(result.result, list.ele_count);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_INDEX: {
		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = packed_list_get_remove_by_index_range(&list, NULL, NULL, index,
				1, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_ALL_BY_VALUE:
	case AS_CDT_OP_LIST_GET_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type,
				optype == AS_CDT_OP_LIST_GET_ALL_BY_VALUE);
		ret = packed_list_get_remove_by_value_interval(&list, NULL, NULL,
				&value, &value, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_RANK: {
		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, false);
		ret = packed_list_get_remove_by_rank_range(&list, NULL, NULL, rank, 1,
				&result);
		break;
	}
	case AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST: {
		uint64_t result_type;
		cdt_payload value_list;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_list)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_list_get_remove_all_by_value_list(&list, NULL, NULL,
				&value_list, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_INDEX_RANGE: {
		uint64_t result_type;
		int64_t index;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_list_get_remove_by_index_range(&list, NULL, NULL, index,
				count, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL: {
		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end = { NULL };

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start,
				&value_end)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_list_get_remove_by_value_interval(&list, NULL, NULL,
				&value_start, &value_end, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_RANK_RANGE: {
		uint64_t result_type;
		int64_t rank;
		uint64_t count = UINT32_MAX;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			cdt_udata->ret_code = -AS_ERR_PARAMETER;
			return false;
		}

		result_data_set(&result, result_type, true);
		ret = packed_list_get_remove_by_rank_range(&list, NULL, NULL, rank,
				count, &result);
		break;
	}
	case AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE: {
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
		ret = packed_list_get_remove_by_rel_rank_range(&list, NULL, NULL,
				&value, rank, count, &result);
		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_read_optype() invalid cdt op: %d", optype);
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
// list_offset_index
//

static inline void
list_offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr,
		uint32_t ele_count, const uint8_t *contents, uint32_t content_sz)
{
	ele_count /= PACKED_LIST_INDEX_STEP;

	if (ele_count != 0) {
		ele_count++;
	}

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
	uint32_t prev_par_idx = 0;
	uint32_t idx = 0;

	for (uint32_t i = 0; i < rm_count; i++) {
		idx = cdt_idx_mask_find(rm_mask, idx, ele_count, false);
		uint32_t sz = offset_index_get_delta_const(full_src, idx);
		uint32_t par_idx = (idx - i) / PACKED_LIST_INDEX_STEP;
		uint32_t diff = par_idx - prev_par_idx + 1;

		for (uint32_t j = 1; j < diff; j++) {
			uint32_t offset = offset_index_get_const(full_src,
					(prev_par_idx + j) * PACKED_LIST_INDEX_STEP + i);
			offset_index_set(dst, prev_par_idx + j, offset - delta);
		}

		prev_par_idx = par_idx;
		delta += sz;
		idx++;
	}

	uint32_t par_idx = (full_src->_.ele_count - rm_count) /
			PACKED_LIST_INDEX_STEP;
	uint32_t diff = par_idx - prev_par_idx + 1;

	for (uint32_t j = 1; j < diff; j++) {
		uint32_t offset = offset_index_get_const(full_src,
				(prev_par_idx + j) * PACKED_LIST_INDEX_STEP + rm_count);
		offset_index_set(dst, prev_par_idx + j, offset - delta);
	}

	offset_index_set_filled(dst, par_idx + 1);
}


//==========================================================
// list_full_offset_index
//

static inline void
list_full_offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr,
		uint32_t ele_count, const uint8_t *contents, uint32_t content_sz)
{
	offset_index_init(offidx, idx_mem_ptr, ele_count, contents, content_sz);
}

static bool
list_full_offset_index_fill_to(offset_index *offidx, uint32_t index,
		bool check_storage)
{
	uint32_t start = offset_index_get_filled(offidx);

	index = MIN(index + 1, offidx->_.ele_count);

	if (start >= index) {
		return true;
	}

	as_unpacker pk = {
			.buffer = offidx->contents,
			.offset = offset_index_get_const(offidx, start - 1),
			.length = offidx->content_sz
	};

	for (uint32_t i = start; i < index; i++) {
		if (cdt_get_msgpack_sz(&pk, check_storage) == 0) {
			return false;
		}

		offset_index_set(offidx, i, pk.offset);
	}

	offset_index_set_filled(offidx, index);

	return true;
}

bool
list_full_offset_index_fill_all(offset_index *offidx, bool check_storage)
{
	return list_full_offset_index_fill_to(offidx, offidx->_.ele_count,
			check_storage);
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

	as_unpacker x_pk = {
			.buffer = buf + x_off,
			.offset = 0,
			.length = len - x_off
	};

	as_unpacker y_pk = {
			.buffer = buf + y_off,
			.offset = 0,
			.length = len - y_off
	};

	msgpack_compare_t cmp = as_unpack_compare(&x_pk, &y_pk);

	switch (cmp) {
	case MSGPACK_COMPARE_EQUAL:
		return 0;
	case MSGPACK_COMPARE_LESS:
		if (udata->flags & AS_CDT_SORT_DESCENDING) {
			cmp = MSGPACK_COMPARE_GREATER;
		}
		break;
	case MSGPACK_COMPARE_GREATER:
		if (udata->flags & AS_CDT_SORT_DESCENDING) {
			cmp = MSGPACK_COMPARE_LESS;
		}
		break;
	default:
		udata->error = true;
		return 0;
	}

	return (cmp == MSGPACK_COMPARE_LESS) ? -1 : 1;
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
		else if (write_count % PACKED_LIST_INDEX_STEP == 0) {
			uint32_t new_idx = write_count / PACKED_LIST_INDEX_STEP;
			offset_index_set(new_offidx, new_idx, buf_off);
		}
	}

	if (offset_index_is_valid(new_offidx)) {
		offset_index_set_filled(new_offidx, (new_offidx->is_partial ?
				(write_count / PACKED_LIST_INDEX_STEP) + 1 : write_count));
	}

	return buf + buf_off;
}


//==========================================================
// list_order_heap
//

static msgpack_compare_t
list_order_heap_cmp_fn(const void *udata, uint32_t idx1, uint32_t idx2)
{
	const packed_list *list = (const packed_list *)udata;
	const offset_index *offidx = &list->full_offidx;

	as_unpacker pk1 = {
			.buffer = list->contents,
			.offset = offset_index_get_const(offidx, idx1),
			.length = list->content_sz
	};

	as_unpacker pk2 = {
			.buffer = list->contents,
			.offset = offset_index_get_const(offidx, idx2),
			.length = list->content_sz
	};

	return as_unpack_compare(&pk1, &pk2);
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

static void
list_print(const packed_list *list, const char *name)
{
	print_packed(list->packed, list->packed_sz, name);
}

static bool
list_verify(const as_bin *b)
{
	if (! b) {
		return true;
	}

	packed_list list;
	uint8_t type = as_bin_get_particle_type(b);

	if (type != AS_PARTICLE_TYPE_LIST) {
		cf_warning(AS_PARTICLE, "list_verify() non-list type: %u", type);
		return false;
	}

	// Check header.
	if (! packed_list_init_from_bin(&list, b)) {
		cf_warning(AS_PARTICLE, "list_verify() invalid packed list");
		return false;
	}

	offset_index *offidx = list_full_offidx_p(&list);
	bool check_offidx = offset_index_is_valid(offidx);
	uint32_t filled = 0;
	define_offset_index(temp_offidx, list.contents, list.content_sz,
			list.ele_count);

	as_unpacker pk = {
			.buffer = list.contents,
			.length = list.content_sz
	};

	if (check_offidx) {
		filled = offset_index_get_filled(offidx);

		if (list.ele_count != 0) {
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

					if (pk.offset != offset) {
						cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u expected=%u", i, offset, pk.offset);
						return false;
					}
				}
				else {
					offset_index_set(&temp_offidx, i, pk.offset);
				}
			}
			else if ((i % PACKED_LIST_INDEX_STEP) == 0) {
				uint32_t step_i = i / PACKED_LIST_INDEX_STEP;

				if (i < filled) {
					offset = offset_index_get_const(offidx, i);

					if (pk.offset != offset) {
						cf_warning(AS_PARTICLE, "list_verify() i=%u step %u offset=%u expected=%u", i, step_i, offset, pk.offset);
						return false;
					}
				}
			}
		}
		else {
			offset_index_set(&temp_offidx, i, pk.offset);
		}

		offset = pk.offset;

		if (as_unpack_size(&pk) <= 0) {
			cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u pk.offset=%u invalid key", i, offset, pk.offset);
			return false;
		}
	}

	// Check packed size.
	if (list.content_sz != pk.offset) {
		cf_warning(AS_PARTICLE, "list_verify() content_sz=%u expected=%u", list.content_sz, pk.offset);
		return false;
	}

	pk.offset = 0;

	as_unpacker pk_value = pk;

	// Check ordered list.
	if (list_is_ordered(&list) && list.ele_count > 0) {
		if (as_unpack_size(&pk) <= 0) {
			cf_warning(AS_PARTICLE, "list_verify() pk.offset=%u invalid value", pk.offset);
			return false;
		}

		for (uint32_t i = 1; i < list.ele_count; i++) {
			uint32_t offset = pk.offset;
			msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u pk.offset=%u invalid key", i, offset, pk.offset);
				return false;
			}

			if (cmp == MSGPACK_COMPARE_GREATER) {
				cf_warning(AS_PARTICLE, "list_verify() i=%u offset=%u pk.offset=%u keys not in order", i, offset, pk.offset);
				return false;
			}
		}
	}

	return true;
}

// Quash warnings for debug function.
void
as_cdt_list_debug_dummy()
{
	list_verify(NULL);
	list_print(NULL, NULL);
}
