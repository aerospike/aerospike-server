/*
 * exp.c
 *
 * Copyright (C) 2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike,),Inc. under one or more contributor
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

//==========================================================
// Includes.
//

#include "base/exp.h"

#include <inttypes.h>
#include <regex.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_map.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

#define EXP_MAX_SIZE (1 * 1024 * 1024) // 1 MiB

typedef enum {
	GEO_CELL,
	GEO_REGION,
	GEO_REGION_NEED_FREE
} geo_type;

typedef enum {
	EXP_CMP_EQ = 1,
	EXP_CMP_NE = 2,
	EXP_CMP_GT = 3,
	EXP_CMP_GE = 4,
	EXP_CMP_LT = 5,
	EXP_CMP_LE = 6,

	EXP_CMP_REGEX = 7,
	EXP_CMP_GEO = 8,

	EXP_AND = 16,
	EXP_OR = 17,
	EXP_NOT = 18,

	EXP_META_DIGEST_MOD = 64,
	EXP_META_DEVICE_SIZE = 65,
	EXP_META_LAST_UPDATE = 66,
	EXP_META_SINCE_UPDATE = 67,
	EXP_META_VOID_TIME = 68,
	EXP_META_TTL = 69,
	EXP_META_SET_NAME = 70,
	EXP_META_KEY_EXISTS = 71,
	EXP_META_IS_TOMBSTONE = 72,
	EXP_META_MEMORY_SIZE = 73,

	EXP_REC_KEY = 80,
	EXP_BIN = 81,
	EXP_BIN_TYPE = 82,

	EXP_QUOTE = 126,
	EXP_CALL = 127,

	EXP_OP_CODE_END,

	// Begin virtual ops.
	VOP_VALUE_NIL,
	VOP_VALUE_INT,
	VOP_VALUE_FLOAT,
	VOP_VALUE_STR,
	VOP_VALUE_BLOB,
	VOP_VALUE_GEO,
	VOP_VALUE_HLL,
	VOP_VALUE_MAP,
	VOP_VALUE_LIST,
	VOP_VALUE_MSGPACK
} exp_op_code;

typedef enum {
	TYPE_NIL = 0,
	TYPE_TRILEAN = 1,
	TYPE_INT = 2,
	TYPE_STR = 3,
	TYPE_LIST = 4,
	TYPE_MAP = 5,
	TYPE_BLOB = 6,
	TYPE_FLOAT = 7,
	TYPE_GEOJSON = 8,
	TYPE_HLL = 9,

	TYPE_END
} result_type;

typedef enum {
	CALL_CDT = 0,
	CALL_BITS = 1,
	CALL_HLL = 2,

	CALL_FLAG_MODIFY_LOCAL = 0x40
} call_system_type;

typedef enum {
	RT_NIL = 0,
	RT_INT = AS_PARTICLE_TYPE_INTEGER,
	RT_FLOAT = AS_PARTICLE_TYPE_FLOAT,
	RT_STR = AS_PARTICLE_TYPE_STRING,
	RT_BLOB = AS_PARTICLE_TYPE_BLOB,
	RT_HLL = AS_PARTICLE_TYPE_HLL,
	RT_GEO_STR = AS_PARTICLE_TYPE_GEOJSON, // only used in call

	RT_TRILEAN,
	RT_GEO_CONST, // from wire, compiled and msgpack
	RT_MSGPACK,
	RT_GEO_COMPILED, // compiled point or region
	RT_BIN_PTR, // bin -> list, map
	RT_BIN, // call -> list, map, string

	RT_END
} runtime_type;

typedef struct op_base_mem_s {
	exp_op_code code:16;
} __attribute__ ((__packed__)) op_base_mem;

typedef struct op_cmp_regex_s {
	op_base_mem base;
	regex_t regex;
} op_cmp_regex;

typedef struct op_logical_s { // and, or
	op_base_mem base;
	uint32_t instr_end_ix;
} op_logical;

typedef struct op_bin_name_s {
	op_base_mem base;
	const uint8_t* name;
	uint32_t name_sz;
} op_bin_type;

typedef struct op_rec_digest_modulo_s {
	op_base_mem base;
	int32_t mod;
} op_meta_digest_modulo;

typedef struct op_rec_key_s {
	op_base_mem base;
	runtime_type type;
} op_rec_key;

typedef struct op_bin_s {
	op_base_mem base;
	const uint8_t* name;
	uint32_t name_sz;
	result_type type;
} op_bin;

#define OP_CALL_MAX_VEC_IDX 18

typedef struct op_vec_s {
	const uint8_t* buf;
	uint32_t buf_sz;
} op_vec;

typedef struct op_call_s {
	op_base_mem base;
	uint32_t instr_end_ix;
	op_vec vecs[OP_CALL_MAX_VEC_IDX + 2];
	uint32_t n_vecs;
	uint32_t eval_count;
	bool is_cdt_context_eval;
	call_system_type system_type;
	result_type type;
} op_call;

typedef struct geo_compiled_s {
	geo_type type:8;

	union {
		uint64_t cellid;
		geo_region_t region;
	};
} geo_compiled;

typedef struct op_value_geo_s {
	op_base_mem base;
	geo_compiled compiled;
	const uint8_t* contents;
	uint32_t content_sz;
} op_value_geo;

typedef struct geo_data_s {
	uint64_t cellid;
	geo_region_t region;
} geo_data;

typedef struct op_value_bytes_s {
	op_base_mem base;
	const uint8_t* value;
	uint32_t value_sz;
} op_value_blob;

typedef struct op_value_int_s {
	op_base_mem base;
	int64_t value;
} op_value_int;

typedef struct op_value_float_s {
	op_base_mem base;
	double value;
} op_value_float;

typedef struct op_table_entry_s op_table_entry;

typedef struct build_args_s {
	as_exp* exp;
	uint8_t* mem;
	msgpack_in mp;

	const op_table_entry* entry;
	uint32_t ele_count;
	uint32_t instr_ix;
} build_args;

typedef struct geo_entry_s {
	geo_compiled compiled;
	const uint8_t* contents;
	uint32_t content_sz;
} geo_entry;

typedef struct rt_value_s {
	uint8_t type;
	uint8_t pad0;
	uint16_t pad1;

	union {
		as_bin r_bin;

		struct r_bytes_s {
			uint32_t sz;
			const uint8_t* contents;
		} r_bytes;

		struct r_geo_const_s {
			op_value_geo* op;
		} r_geo_const;

		geo_compiled r_geo;

		struct {
			uint32_t pad2;

			union {
				as_exp_trilean r_trilean;
				int64_t r_int;
				double r_float;
				op_base_mem* r_const_p;
				as_bin* r_bin_p;
				char* r_cstr_p;
			};
		};
	};
} __attribute__ ((__packed__)) rt_value;

typedef struct rt_stack_s {
	rt_value* stack;
	uint32_t stack_ix;
} rt_stack;

typedef struct runtime_s {
	const as_exp_ctx* ctx;
	const uint8_t* instr_ptr;
	uint32_t op_ix;
} runtime;

typedef bool (*op_table_build_cb)(build_args* args);
typedef void (*op_table_eval_cb)(runtime* rt, const op_base_mem* ob, rt_value* ret_val);

typedef enum {
	INSTR_SZ_CONST,
	INSTR_SZ_4BYTEVAR,
	INSTR_SZ_MSGPACK
} instr_sz_type;

struct op_table_entry_s {
	exp_op_code code;
	uint32_t size;
	op_table_build_cb build_cb;
	op_table_eval_cb eval_cb;
	uint32_t static_param_count;
	uint32_t eval_param_count;
	result_type r_type;
	const char* name;
};

#define OP_TABLE_ENTRY(__code, __size_name, __build_name, __eval_name, __static_param_count, __eval_param_count, __r_type) \
		[__code].code = __code, \
		[__code].name = #__code, \
		[__code].size = (uint32_t)sizeof(__size_name), \
		[__code].build_cb = __build_name, \
		[__code].eval_cb = __eval_name, \
		[__code].static_param_count = __static_param_count, \
		[__code].eval_param_count = __eval_param_count, \
		[__code].r_type = __r_type

#if 0
static const char* type_names[] = {
	"NIL",
	"TRILEAN",
	"INT",
	"STR",
	"LIST",
	"MAP",
	"BLOB",
	"DOUBLE",
	"GEOJSON",
};
#endif

static const char* trilean_names[] = {
		"FALSE", "TRUE", "UNKNOWN"
};

static const uint8_t* EMPTY_STRING = (uint8_t*)"";
static const uint8_t call_eval_token[1] = "";


//==========================================================
// Forward declarations.
//

// Build.
static as_exp* build_internal(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire);
static bool build_next(build_args* args);
static const op_table_entry* build_get_entry(result_type type);
static bool build_default(build_args* args);
static bool build_compare(build_args* args);
static bool build_cmp_regex(build_args* args);
static bool build_cmp_geo(build_args* args);
static bool build_logical(build_args* args);
static bool build_logical_not(build_args* args);
static bool build_meta_digest_mod(build_args* args);
static bool build_rec_key(build_args* args);
static bool build_bin(build_args* args);
static bool build_bin_type(build_args* args);
static bool build_quote(build_args* args);
static bool build_call(build_args* args);
static bool build_value_nil(build_args* args);
static bool build_value_int(build_args* args);
static bool build_value_float(build_args* args);
static bool build_value_blob(build_args* args);
static bool build_value_geo(build_args* args);
static bool build_value_msgpack(build_args* args);

// Build utilities.
static bool parse_op_call(op_call* op, build_args* args);

// Runtime.
static as_exp_trilean match_internal(const as_exp* predexp, const as_exp_ctx* ctx);
static void rt_eval(runtime* rt, rt_value* ret_val);
static void eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_cmp_regex(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_digest_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_device_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_last_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_since_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_void_time(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_ttl(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_set_name(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_key_exists(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_is_tombstone(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_meta_memory_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_rec_key(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_bin(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_bin_type(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_call(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_value(runtime* rt, const op_base_mem* ob, rt_value* ret_val);

// Runtime utilities.
static void json_to_rt_geo(const uint8_t* json, uint32_t jsonsz, rt_value* val);
static void particle_to_rt_geo(const as_particle* p, rt_value* val);
static bool bin_is_type(const as_bin* b, result_type type);
static void rt_skip(runtime* rt, uint32_t instr_count);
static void rt_value_cmp_translate(rt_value* to, const rt_value* from);
static void rt_value_destroy(rt_value* val);
static void rt_value_get_geo(rt_value* val, geo_data* result);
static as_bin* get_live_bin(as_storage_rd* rd, const uint8_t* name, size_t len);

// Runtime compare utilities.
static as_exp_trilean cmp_trilean(exp_op_code code, const rt_value* e0, const rt_value* e1);
static as_exp_trilean cmp_int(exp_op_code code, const rt_value* e0, const rt_value* e1);
static as_exp_trilean cmp_float(exp_op_code code, const rt_value* e0, const rt_value* e1);
static const uint8_t* rt_value_get_str(const rt_value* val, uint32_t* sz_r);
static as_exp_trilean cmp_bytes(exp_op_code code, const rt_value* e0, const rt_value* e1);
static as_exp_trilean cmp_msgpack(exp_op_code code, const rt_value* v0, const rt_value* v1);

// Runtime call utilities.
static void call_cleanup(void** blob, uint32_t blob_ix, as_bin** bin, uint32_t bin_ix);
static void pack_typed_str(as_packer* pk, const uint8_t* buf, uint32_t sz, uint8_t type);
static bool rt_value_bin_translate(rt_value* to, const rt_value* from);

//==========================================================
// Inlines & macros.
//

static inline bool
rt_value_is_unknown(const rt_value* entry)
{
	return entry->type == RT_TRILEAN && entry->r_trilean == AS_EXP_UNK;
}

static inline bool
build_args_setup(build_args* args, const char* name)
{
	const op_table_entry* entry = args->entry;

	if (args->ele_count != entry->eval_param_count +
			entry->static_param_count) {
		cf_warning(AS_EXP, "%s - error %u terms %u != %u", name,
				AS_ERR_PARAMETER, args->ele_count,
				entry->eval_param_count + entry->static_param_count);
		return false;
	}

	op_base_mem* ob = (op_base_mem*)args->mem;

	ob->code = entry->code;
	args->mem += entry->size;

	return true;
}


//==========================================================
// Op table.
//

static const op_table_entry op_table[] = {
		OP_TABLE_ENTRY(EXP_CMP_EQ, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_NE, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_GT, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_GE, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_LT, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_LE, op_base_mem, build_compare, eval_compare, 0, 2, TYPE_TRILEAN),

		OP_TABLE_ENTRY(EXP_CMP_REGEX, op_cmp_regex, build_cmp_regex, eval_cmp_regex, 2, 1, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_CMP_GEO, op_base_mem, build_cmp_geo, eval_compare, 0, 2, TYPE_TRILEAN),

		OP_TABLE_ENTRY(EXP_AND, op_logical, build_logical, eval_and, 0, 0, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_OR, op_logical, build_logical, eval_or, 0, 0, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_NOT, op_base_mem, build_logical_not, eval_not, 0, 1, TYPE_TRILEAN),

		OP_TABLE_ENTRY(EXP_META_DIGEST_MOD, op_meta_digest_modulo, build_meta_digest_mod, eval_meta_digest_mod, 1, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_DEVICE_SIZE, op_base_mem, build_default, eval_meta_device_size, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_LAST_UPDATE, op_base_mem, build_default, eval_meta_last_update, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_SINCE_UPDATE, op_base_mem, build_default, eval_meta_since_update, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_VOID_TIME, op_base_mem, build_default, eval_meta_void_time, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_TTL, op_base_mem, build_default, eval_meta_ttl, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(EXP_META_SET_NAME, op_base_mem, build_default, eval_meta_set_name, 0, 0, TYPE_STR),
		OP_TABLE_ENTRY(EXP_META_KEY_EXISTS, op_base_mem, build_default, eval_meta_key_exists, 0, 0, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_META_IS_TOMBSTONE, op_base_mem, build_default, eval_meta_is_tombstone, 0, 0, TYPE_TRILEAN),
		OP_TABLE_ENTRY(EXP_META_MEMORY_SIZE, op_base_mem, build_default, eval_meta_memory_size, 0, 0, TYPE_INT),

		OP_TABLE_ENTRY(EXP_REC_KEY, op_rec_key, build_rec_key, eval_rec_key, 1, 0, TYPE_END),
		OP_TABLE_ENTRY(EXP_BIN, op_bin, build_bin, eval_bin, 2, 0, TYPE_END),
		OP_TABLE_ENTRY(EXP_BIN_TYPE, op_bin_type, build_bin_type, eval_bin_type, 1, 0, TYPE_INT),

		OP_TABLE_ENTRY(EXP_QUOTE, op_value_blob, build_quote, eval_value, 1, 0, TYPE_LIST),
		OP_TABLE_ENTRY(EXP_CALL, op_call, build_call, eval_call, 2, 2, TYPE_END),

		OP_TABLE_ENTRY(VOP_VALUE_NIL, op_base_mem, build_value_nil, eval_value, 0, 0, TYPE_NIL),
		OP_TABLE_ENTRY(VOP_VALUE_INT, op_value_int, build_value_int, eval_value, 0, 0, TYPE_INT),
		OP_TABLE_ENTRY(VOP_VALUE_FLOAT, op_value_float, build_value_float, eval_value, 0, 0, TYPE_FLOAT),
		OP_TABLE_ENTRY(VOP_VALUE_STR, op_value_blob, build_value_blob, eval_value, 0, 0, TYPE_STR),
		OP_TABLE_ENTRY(VOP_VALUE_BLOB, op_value_blob, build_value_blob, eval_value, 0, 0, TYPE_BLOB),
		OP_TABLE_ENTRY(VOP_VALUE_GEO, op_value_geo, build_value_geo, eval_value, 0, 0, TYPE_GEOJSON),
		OP_TABLE_ENTRY(VOP_VALUE_MSGPACK, op_value_blob, build_value_msgpack, eval_value, 0, 0, TYPE_END),

		OP_TABLE_ENTRY(VOP_VALUE_HLL, op_base_mem, NULL, eval_value, 0, 0, TYPE_HLL),
		OP_TABLE_ENTRY(VOP_VALUE_MAP, op_base_mem, NULL, eval_value, 0, 0, TYPE_MAP),
		OP_TABLE_ENTRY(VOP_VALUE_LIST, op_value_blob, NULL, eval_value, 0, 0, TYPE_LIST)
};


//==========================================================
// Public API.
//

as_exp*
exp_build_base64(const char* buf64, uint32_t buf64_sz)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(buf64_sz);
	uint8_t* buf = cf_malloc(buf_sz);
	uint32_t buf_sz_out;

	if (! cf_b64_validate_and_decode(buf64, buf64_sz, buf, &buf_sz_out)) {
		cf_free(buf);
		return NULL;
	}

	cf_assert(buf_sz_out == buf_sz, AS_EXP, "buf_sz_out %u buf_sz %u",
			buf_sz_out, buf_sz);

	as_exp* p = build_internal(buf, buf_sz, false);

	if (p != NULL) {
		p->buf_cleanup = buf;
	}

	return p;
}

as_exp*
as_exp_build(const as_msg_field* msg, bool cpy_wire)
{
	cf_debug(AS_EXP, "as_exp_build - msg_sz %u msg", msg->field_sz);
	cf_debug(AS_EXP, "as_exp_build - msg-dump:\n%*pH",
			as_msg_field_get_value_sz(msg), msg->data);

	// TODO - Remove in "six months".
	if (msg->data[0] == 0) {
		return predexp_build_old(msg);
	}

	return build_internal(msg->data, as_msg_field_get_value_sz(msg), cpy_wire);
}

as_exp_trilean
as_exp_matches_metadata(const as_exp* predexp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd == NULL, AS_EXP, "invalid parameter");

	// TODO - Remove in "six months".
	if (predexp->version != 2) {
		cf_debug(AS_EXP, "as_exp_matches_metadata - v1");
		return predexp_matches_metadata_old(predexp, ctx);
	}

	cf_debug(AS_EXP, "as_exp_matches_metadata - v2 start");

	as_exp_trilean ret = match_internal(predexp, ctx);

	cf_debug(AS_EXP, "as_exp_matches_metadata - result %s", trilean_names[ret]);

	return ret;
}

bool
as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd != NULL, AS_EXP, "invalid parameter");

	// TODO - Remove in "six months".
	if (predexp->version != 2) {
		cf_debug(AS_EXP, "as_exp_matches_record - v1");
		return predexp_matches_record_old(predexp, ctx);
	}

	cf_debug(AS_EXP, "as_exp_matches_record - v2 start");

	as_exp_trilean ret = match_internal(predexp, ctx);

	cf_debug(AS_EXP, "as_exp_matches_record - result %s", trilean_names[ret]);

	return ret == AS_EXP_TRUE;
}

void
as_exp_destroy(as_exp* exp)
{
	if (exp == NULL) {
		return;
	}

	// TODO - Remove in "six months".
	if (exp->version != 2) {
		return predexp_destroy_old(exp);
	}

	for (uint32_t i = 0; i < exp->cleanup_stack_ix; i++) {
		op_base_mem* ob = exp->cleanup_stack[i];

		switch (ob->code) {
		case VOP_VALUE_GEO:
			cf_assert(((op_value_geo*)ob)->compiled.type == GEO_REGION, AS_EXP,
					"unexpected");
			geo_region_destroy(((op_value_geo*)ob)->compiled.region);
			break;
		case EXP_CMP_REGEX:
			regfree(&((op_cmp_regex*)ob)->regex);
			break;
		default:
			break;
		}
	}

	cf_free(exp->buf_cleanup);
	cf_free(exp);
}


//==========================================================
// Local helpers - build.
//

static as_exp*
build_internal(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire)
{
	msgpack_in mp = {
			.buf = buf,
			.buf_sz = buf_sz
	};
	uint32_t top_count;

	if (! msgpack_buf_get_list_ele_count(mp.buf, mp.buf_sz, &top_count) ||
			top_count == 0) {
		cf_warning(AS_EXP, "must begin with non empty list");
		return NULL;
	}

	uint32_t total_sz = 0;
	uint32_t cleanup_count = 0;
	uint32_t counter = 1;

	while (mp.offset < mp.buf_sz) {
		msgpack_type type = msgpack_peek_type(&mp);

		if (type == MSGPACK_TYPE_ERROR) {
			cf_warning(AS_EXP, "invalid instruction at offset %u",
					mp.offset);
			return NULL;
		}

		counter--;

		uint64_t op_code;

		if (type != MSGPACK_TYPE_LIST) {
			switch (type) {
			case MSGPACK_TYPE_NIL:
				op_code = VOP_VALUE_NIL;
				break;
			case MSGPACK_TYPE_NEGINT:
			case MSGPACK_TYPE_INT:
				op_code = VOP_VALUE_INT;
				break;
			case MSGPACK_TYPE_DOUBLE:
				op_code = VOP_VALUE_FLOAT;
				break;
			case MSGPACK_TYPE_STRING:
				op_code = VOP_VALUE_STR;
				break;
			case MSGPACK_TYPE_BYTES:
				op_code = VOP_VALUE_BLOB;
				break;
			case MSGPACK_TYPE_GEOJSON:
				op_code = VOP_VALUE_GEO;
				break;
			default:
				op_code = VOP_VALUE_MSGPACK;
				break;
			}

			cf_debug(AS_EXP, "op_code %lu", op_code);

			if (type == MSGPACK_TYPE_MAP) {
				uint32_t count;

				if (! msgpack_buf_get_map_ele_count(mp.buf + mp.offset,
						mp.buf_sz - mp.offset, &count)) {
					cf_warning(AS_EXP, "invalid instruction at offset %u",
							mp.offset);
					return NULL;
				}

				counter += 2 * count;
			}

			if (type == MSGPACK_TYPE_BYTES) {
				uint32_t temp_sz;
				const uint8_t* buf = msgpack_get_bin(&mp, &temp_sz);

				if (buf == NULL || temp_sz == 0) {
					cf_warning(AS_EXP, "invalid blob at offset %u", mp.offset);
					return NULL;
				}

				if (*buf != AS_BYTES_BLOB) {
					cf_warning(AS_EXP, "invalid blob type %d at offset %u", *buf, mp.offset);
					return NULL;
				}
			}
			else if (msgpack_sz(&mp) == 0) {
				cf_warning(AS_EXP, "invalid instruction at offset %u",
						mp.offset);
				return NULL;
			}
		}
		else {
			uint32_t ele_count;

			if (! msgpack_get_list_ele_count(&mp, &ele_count) ||
					ele_count == 0) {
				cf_warning(AS_EXP, "invalid instruction at offset %u",
						mp.offset);
				return NULL;
			}

			counter += ele_count - 1;

			if (! msgpack_get_uint64(&mp, &op_code)) {
				cf_warning(AS_EXP, "invalid instruction at offset %u",
						mp.offset);
				return NULL;
			}

			if (op_code >= EXP_OP_CODE_END) {
				cf_warning(AS_EXP, "invalid op_code %lu", op_code);
				return NULL;
			}

			cf_debug(AS_EXP, "ele_count %u op_code %lu", ele_count, op_code);
		}

		const op_table_entry* entry = &op_table[op_code];

		if (entry->size == 0) {
			cf_warning(AS_EXP, "invalid op_code %lu size %u",
				op_code, entry->size);
			return NULL;
		}

		if (entry->static_param_count != 0 &&
				msgpack_sz_rep(&mp, entry->static_param_count) == 0) {
			cf_warning(AS_EXP, "invalid instruction at offset %u",
					mp.offset);
			return NULL;
		}

		counter -= entry->static_param_count;

		if (op_code == EXP_CALL) {
			uint32_t ele_count;
			int64_t op_code;

			counter -= 1;

			if (! msgpack_get_list_ele_count(&mp, &ele_count) ||
					ele_count == 0 || ! msgpack_get_int64(&mp, &op_code)) {
				cf_warning(AS_EXP, "invalid instruction at offset %u",
						mp.offset);
				return NULL;
			}

			if (op_code == AS_CDT_OP_CONTEXT_EVAL && (ele_count != 3 ||
					msgpack_sz(&mp) == 0 || // skip context
					! msgpack_get_list_ele_count(&mp, &ele_count) ||
					! msgpack_get_int64(&mp, &op_code))) {
				cf_warning(AS_EXP, "invalid instruction at offset %u",
						mp.offset);
				return NULL;
			}

			for (uint32_t i = 1; i < ele_count; i++) {
				// TODO - Skip allocating space until we reach the first list.
				// Non lists after the first list will result in over-allocation
				// of op space. May want to improve accounting in the future.
				if (msgpack_peek_type(&mp) == MSGPACK_TYPE_LIST) {
					counter += ele_count - i;
					break;
				}

				if (msgpack_sz(&mp) == 0) {
					cf_warning(AS_EXP, "invalid instruction at offset %u",
							mp.offset);
					return NULL;
				}
			}
		}

		total_sz += entry->size;

		switch (op_code) {
		case EXP_CMP_REGEX:
		case VOP_VALUE_GEO:
			cleanup_count++;
			break;
		default:
			break;
		}
	}

	if (total_sz >= EXP_MAX_SIZE) {
		cf_warning(AS_EXP, "expression size exceeds limit of %u bytes",
				EXP_MAX_SIZE);
		return NULL;
	}

	if (mp.offset != mp.buf_sz) {
		cf_warning(AS_EXP, "malformed expression field");
		return NULL;
	}

	if (counter != 0) {
		cf_warning(AS_EXP, "incomplete expression field expected %u more elements",
				counter);
		return NULL;
	}

	uint32_t cleanup_offset = total_sz;

	total_sz += cleanup_count * sizeof(void*);

	if (cpy_wire) {
		total_sz += mp.buf_sz;
	}

	build_args args = {
			.exp = cf_calloc(1, sizeof(as_exp) + total_sz)
	};

	args.mem = args.exp->mem;
	args.exp->version = 2;
	args.exp->cleanup_stack = (void**)(args.mem + cleanup_offset);

	if (cpy_wire) {
		uint8_t* wire_mem = args.mem + total_sz - mp.buf_sz;

		memcpy(wire_mem, buf, mp.buf_sz);
		args.mp.buf = wire_mem;
	}
	else {
		args.mp.buf = buf;
	}

	args.mp.buf_sz = mp.buf_sz;

	if (! build_next(&args)) {
		as_exp_destroy(args.exp);
		return NULL;
	}

	return args.exp;
}

static bool
build_next(build_args* args)
{
	msgpack_type type = msgpack_peek_type(&args->mp);

	cf_assert(type != MSGPACK_TYPE_ERROR, AS_EXP, "unexpected");

	uint64_t op_code;
	uint32_t ele_count = 0;

	if (type != MSGPACK_TYPE_LIST) {
		switch (type) {
		case MSGPACK_TYPE_NIL:
			op_code = VOP_VALUE_NIL;
			break;
		case MSGPACK_TYPE_NEGINT:
		case MSGPACK_TYPE_INT:
			op_code = VOP_VALUE_INT;
			break;
		case MSGPACK_TYPE_DOUBLE:
			op_code = VOP_VALUE_FLOAT;
			break;
		case MSGPACK_TYPE_STRING:
			op_code = VOP_VALUE_STR;
			break;
		case MSGPACK_TYPE_BYTES:
			op_code = VOP_VALUE_BLOB;
			break;
		case MSGPACK_TYPE_GEOJSON:
			op_code = VOP_VALUE_GEO;
			break;
		default:
			op_code = VOP_VALUE_MSGPACK;
			break;
		}
	}
	else {
		msgpack_get_list_ele_count(&args->mp, &ele_count);
		msgpack_get_uint64(&args->mp, &op_code);
		ele_count--; // -1 for op_code

		cf_assert(AS_EXP, op_code < EXP_OP_CODE_END &&
				op_table[op_code].code != 0, "invalid expression op %lu",
				op_code);
	}

	args->entry = &op_table[op_code];
	args->ele_count = ele_count;
	args->instr_ix++;

	cf_debug(AS_EXP, "build_next op %s ele_count %u ix %u", args->entry->name,
			ele_count, args->instr_ix);

	return op_table[op_code].build_cb(args);
}

static const op_table_entry*
build_get_entry(result_type type)
{
	switch (type) {
	case TYPE_INT:
		return &op_table[VOP_VALUE_INT];
	case TYPE_FLOAT:
		return &op_table[VOP_VALUE_FLOAT];
	case TYPE_STR:
		return &op_table[VOP_VALUE_STR];
	case TYPE_BLOB:
		return &op_table[VOP_VALUE_BLOB];
	case TYPE_GEOJSON:
		return &op_table[VOP_VALUE_GEO];
	case TYPE_HLL:
		return &op_table[VOP_VALUE_HLL];
	case TYPE_MAP:
		return &op_table[VOP_VALUE_MAP];
	case TYPE_LIST:
		return &op_table[VOP_VALUE_LIST];
	default:
		break;
	}

	return NULL;
}

static bool
build_default(build_args* args)
{
	return build_args_setup(args, "build_default");
}

static bool
build_compare(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_compare")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type ltype = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	if (ltype != rtype) {
		cf_warning(AS_EXP, "build_compare - error %u mismatched types ltype %u rtype %u",
				AS_ERR_PARAMETER, ltype, rtype);
		return false;
	}

	switch (ltype) {
	case TYPE_MAP:
	case TYPE_GEOJSON:
	case TYPE_HLL:
		cf_warning(AS_EXP, "build_compare - error %u cannot compare type %u",
				AS_ERR_PARAMETER, ltype);
		return false;
	default:
		break;
	}

	args->entry = entry;

	return true;
}

static bool
build_cmp_regex(build_args* args)
{
	op_cmp_regex* op = (op_cmp_regex*)args->mem;
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_cmp_regex")) {
		return false;
	}

	uint64_t regex_options;

	if (! msgpack_get_uint64(&args->mp, &regex_options)) {
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid regex options",
				AS_ERR_PARAMETER);
		return false;
	}

	uint32_t regex_str_sz;
	const uint8_t* regex_str = msgpack_get_bin(&args->mp, &regex_str_sz);

	if (regex_str == NULL) {
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid regex string",
				AS_ERR_PARAMETER);
		return false;
	}

	int rv;

	if (! build_next(args)) {
		return false;
	}

	if (args->entry->r_type != TYPE_STR) {
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid parameter type %u != STRING",
				AS_ERR_PARAMETER, args->entry->r_type);
		return false;
	}

	if (regex_str_sz == 0 || regex_str[regex_str_sz - 1] != '\0') {
		char temp[regex_str_sz + 1];

		memcpy(temp, regex_str, regex_str_sz);
		temp[regex_str_sz] = '\0';
		rv = regcomp(&op->regex, temp, regex_options);
	}
	else {
		rv = regcomp(&op->regex, (const char*)regex_str, regex_options);
	}

	if (rv != 0) {
		char errbuf[1024];

		regerror(rv, &op->regex, errbuf, sizeof(errbuf));
		cf_warning(AS_EXP, "build_cmp_regex - error %u regex compile %s",
				AS_ERR_PARAMETER, errbuf);

		return false;
	}

	args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = op;
	args->entry = entry;

	return true;
}

static bool
build_cmp_geo(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_cmp_geo")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type ltype = args->entry->r_type;

	if (ltype != TYPE_GEOJSON) {
		cf_warning(AS_EXP, "build_cmp_geo - error %u mismatched types ltype %u != %u",
				AS_ERR_PARAMETER, ltype, TYPE_GEOJSON);
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	if (ltype != rtype) {
		cf_warning(AS_EXP, "build_cmp_geo - error %u mismatched types ltype %u rtype %u",
				AS_ERR_PARAMETER, ltype, rtype);
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_logical(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_logical* op = (op_logical*)args->mem;

	op->base.code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 2) {
		cf_warning(AS_EXP, "build_logical - error %u too few arguments %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != TYPE_TRILEAN) {
			cf_warning(AS_EXP, "build_logical - error %u invalid type at parameter %u",
					AS_ERR_PARAMETER, i + 1);
			return false;
		}
	}

	op->instr_end_ix = args->instr_ix;
	args->entry = entry;

	return true;
}

static bool
build_logical_not(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_logical_not")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	if (args->entry->r_type != TYPE_TRILEAN) {
		cf_warning(AS_EXP, "build_logical_not - error %u invalid parameter type %u",
				AS_ERR_PARAMETER, args->entry->r_type);
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_meta_digest_mod(build_args* args)
{
	op_meta_digest_modulo* op = (op_meta_digest_modulo*)args->mem;

	if (! build_args_setup(args, "build_rec_digest_modulo")) {
		return false;
	}

	int64_t mod64;

	if (! msgpack_get_int64(&args->mp, &mod64)) {
		cf_warning(AS_EXP, "build_rec_digest_modulo - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	op->mod = (int32_t)mod64;

	if (op->mod == 0) {
		cf_warning(AS_EXP, "build_rec_digest_modulo - error %u cannot modulo by zero",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_rec_key(build_args* args)
{
	op_rec_key* op = (op_rec_key*)args->mem;

	if (! build_args_setup(args, "build_rec_key")) {
		return false;
	}

	int64_t type64;

	if (! msgpack_get_int64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_rec_key - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	switch ((result_type)type64) {
	case TYPE_INT:
		op->type = RT_INT;
		break;
	case TYPE_STR:
		op->type = RT_STR;
		break;
	case TYPE_BLOB:
		op->type = RT_BLOB;
		break;
	default:
		cf_warning(AS_EXP, "build_rec_key - error %u invalid result_type %ld",
				AS_ERR_PARAMETER, type64);
		return false;
	}

	args->entry = build_get_entry((result_type)type64);

	return true;
}


static bool
build_bin(build_args* args)
{
	op_bin* op = (op_bin*)args->mem;

	if (! build_args_setup(args, "build_bin")) {
		return false;
	}

	int64_t type64;

	if (! msgpack_get_int64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_bin - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	op->type = (result_type)type64;
	op->name = msgpack_get_bin(&args->mp, &op->name_sz);

	if (op->name == NULL) {
		cf_warning(AS_EXP, "build_bin - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	if ((args->entry = build_get_entry(op->type)) == NULL) {
		cf_warning(AS_EXP, "build_bin - error %u invalid result_type %d",
				AS_ERR_PARAMETER, op->type);
		return false;
	}

	return true;
}

static bool
build_bin_type(build_args* args)
{
	op_bin_type* op = (op_bin_type*)args->mem;

	if (! build_args_setup(args, "build_bin_type")) {
		return false;
	}

	op->name = msgpack_get_bin(&args->mp, &op->name_sz);

	if (op->name == NULL) {
		cf_warning(AS_EXP, "build_bin_name - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_quote(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_quote")) {
		return false;
	}

	if (msgpack_peek_type(&args->mp) != MSGPACK_TYPE_LIST ||
			(op->value = msgpack_get_ele(&args->mp, &op->value_sz)) == NULL) {
		cf_warning(AS_EXP, "build_quote - error %u invalid list arg",
				AS_ERR_PARAMETER);
		return false;
	}

	op->base.code = VOP_VALUE_LIST;

	return true;
}

static bool
build_call(build_args* args)
{
	op_call* op = (op_call*)args->mem;

	if (! build_args_setup(args, "build_call")) {
		return false;
	}

	int64_t type64;

	if (! msgpack_get_int64(&args->mp, &type64)) {
		cf_warning(AS_EXP, "build_call - error %u invalid result_type arg",
				AS_ERR_PARAMETER);
		return false;
	}

	int64_t system_type64;

	if (! msgpack_get_int64(&args->mp, &system_type64)) {
		cf_warning(AS_EXP, "build_call - error %u invalid system_type arg",
				AS_ERR_PARAMETER);
		return false;
	}

	op->type = (result_type)type64;
	op->system_type = (call_system_type)system_type64;

	if (op->type >= TYPE_END) {
		cf_warning(AS_EXP, "build_call - error %u invalid type %u",
				AS_ERR_PARAMETER, op->type);
		return false;
	}

	if (! parse_op_call(op, args)) {
		cf_warning(AS_EXP, "build_call - error %u invalid msgpack list",
				AS_ERR_PARAMETER);
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	switch (op->system_type & ~CALL_FLAG_MODIFY_LOCAL) {
	case CALL_CDT:
		if (args->entry->r_type != TYPE_LIST && args->entry->r_type != TYPE_MAP) {
			cf_warning(AS_EXP, "build_call - error %u operand is not list or map",
					AS_ERR_PARAMETER);
			return false;
		}
		break;
	case CALL_HLL:
		if (args->entry->r_type != TYPE_HLL) {
			cf_warning(AS_EXP, "build_call - error %u operand is not hll",
					AS_ERR_PARAMETER);
			return false;
		}
		break;
	case CALL_BITS:
		if (args->entry->r_type != TYPE_BLOB) {
			cf_warning(AS_EXP, "build_call - error %u operand is not blob",
					AS_ERR_PARAMETER);
			return false;
		}
		break;
	default:
		cf_warning(AS_EXP, "build_call - error %u invalid system %d",
				AS_ERR_PARAMETER, op->system_type);
		return false;
	}

	if ((args->entry = build_get_entry(op->type)) == NULL) {
		cf_warning(AS_EXP, "build_call - error %u invalid result_type %d",
				AS_ERR_PARAMETER, op->type);
		return false;
	}

	op->instr_end_ix = args->instr_ix;

	return true;
}

static bool
build_value_nil(build_args* args)
{
	if (! build_args_setup(args, "build_value_nil")) {
		return false;
	}

	if (msgpack_sz(&args->mp) == 0) {
		cf_warning(AS_EXP, "build_value_nil - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_int(build_args* args)
{
	op_value_int* op = (op_value_int*)args->mem;

	if (! build_args_setup(args, "build_value_int")) {
		return false;
	}

	if (! msgpack_get_int64(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_int - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_float(build_args* args)
{
	op_value_float* op = (op_value_float*)args->mem;

	if (! build_args_setup(args, "build_value_float")) {
		return false;
	}

	if (! msgpack_get_double(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_float - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_blob(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_value_str")) {
		return false;
	}

	op->value = msgpack_get_bin(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP, "build_value_str - error %u invalid msgpack string",
				AS_ERR_PARAMETER);
		return false;
	}

	cf_assert(op->value_sz != 0, AS_EXP, "unexpected");

	op->value++;
	op->value_sz--;

	return true;
}

static bool
build_value_geo(build_args* args)
{
	op_value_geo* op = (op_value_geo*)args->mem;

	if (! build_args_setup(args, "build_value_geo")) {
		return false;
	}

	op->contents = args->mp.buf + args->mp.offset;

	uint32_t json_sz;
	const uint8_t* json = msgpack_get_bin(&args->mp, &json_sz);

	cf_assert(json_sz != 0, AS_EXP, "unexpected");
	op->content_sz = (uint32_t)(args->mp.buf + args->mp.offset - op->contents);

	if (json == NULL) {
		cf_warning(AS_EXP, "build_value_geo - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	json_sz--;
	json++; // skip as_bytes type

	uint64_t cellid;
	geo_region_t region;

	op->compiled.region = NULL;

	if (! geo_parse(NULL, (const char*)json, json_sz, &cellid, &region)) {
		cf_warning(AS_EXP, "build_value_geo - error %u invalid geojson",
				AS_ERR_PARAMETER);
		return false;
	}

	if (region != NULL) {
		op->compiled.type = GEO_REGION;
		op->compiled.region = region;
		args->exp->cleanup_stack[args->exp->cleanup_stack_ix++] = op;
	}
	else {
		op->compiled.type = GEO_CELL;
		op->compiled.cellid = cellid;
	}

	return true;
}

static bool
build_value_msgpack(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_value_msgpack")) {
		return false;
	}

	op->value = msgpack_get_ele(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP, "build_value_msgpack - error %u invalid msgpack",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}


//==========================================================
// Local helpers - build utilities.
//

static bool
parse_op_call(op_call* op, build_args* args)
{
	msgpack_in* mp = &args->mp;
	uint32_t ele_count;
	uint32_t offset_start = mp->offset;

	op->eval_count = 0;
	op->vecs[0].buf = mp->buf + mp->offset;

	if (! msgpack_get_list_ele_count(mp, &ele_count)) {
		return false;
	}

	int64_t op_code;

	if (! msgpack_get_int64(mp, &op_code)) {
		return false;
	}

	op->vecs[0].buf_sz = mp->offset - offset_start;

	if (op_code == AS_CDT_OP_CONTEXT_EVAL) {
		op->is_cdt_context_eval = true;

		if (ele_count != 3 || msgpack_sz(mp) == 0) { // skip context
			return false;
		}

		if (! msgpack_get_list_ele_count(mp, &ele_count)) {
			return false;
		}

		if (! msgpack_get_int64(mp, &op_code)) {
			return false;
		}

		op->vecs[0].buf_sz = mp->offset - offset_start;
	}
	else {
		op->is_cdt_context_eval = false;
	}

	uint32_t idx = 0;

	for (uint32_t i = 1; i < ele_count; i++) {
		msgpack_type type = msgpack_peek_type(mp);
		uint32_t sz;

		switch (type) {
		case MSGPACK_TYPE_LIST:
			if (op->vecs[idx].buf_sz != 0) {
				idx++;
			}

			op->vecs[idx].buf = call_eval_token;
			op->vecs[idx].buf_sz = 0;
			idx++;
			op->eval_count++;

			if (! build_next(args)) {
				return false;
			}

			op->vecs[idx].buf = mp->buf + mp->offset; // next vector
			op->vecs[idx].buf_sz = 0;
			break;
		case MSGPACK_TYPE_ERROR:
			return false;
		default:
			sz = msgpack_sz(mp);
			op->vecs[idx].buf_sz += sz;

			if (sz == 0) {
				return false;
			}

			break;
		}

		if (idx >= OP_CALL_MAX_VEC_IDX) {
			return false;
		}
	}

	if (op->vecs[idx].buf_sz != 0) {
		idx++;
	}

	op->n_vecs = idx;

	return true;
}


//==========================================================
// Local helpers - runtime.
//

static as_exp_trilean
match_internal(const as_exp* predexp, const as_exp_ctx* ctx)
{
	runtime rt = { .ctx = ctx, .instr_ptr = predexp->mem };
	rt_value ret_val;

	rt_eval(&rt, &ret_val);

	if (ret_val.type != RT_TRILEAN) {
		rt_value_destroy(&ret_val);
		return AS_EXP_FALSE;
	}

	if (ret_val.r_trilean == AS_EXP_UNK) {
		return ctx->rd == NULL ? AS_EXP_UNK : AS_EXP_FALSE;
	}

	return ret_val.r_trilean;
}

static void
rt_eval(runtime* rt, rt_value* ret_val)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];

	cf_debug(AS_EXP, "eval %s", entry->name);

	rt->op_ix++;
	rt->instr_ptr += entry->size;
	entry->eval_cb(rt, ob, ret_val);

	cf_debug(AS_EXP, "eval %s result_type %u", entry->name, ret_val->type);
}

static void
eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	rt_value arg0;
	rt_value arg1;

	rt_eval(rt, &arg0);
	rt_eval(rt, &arg1);

	rt_value v0;
	rt_value v1;

	rt_value_cmp_translate(&v0, &arg0);
	rt_value_cmp_translate(&v1, &arg1);

	cf_debug(AS_EXP, "cmp type %u to %u", v0.type, v1.type);

	if (rt_value_is_unknown(&v0) || rt_value_is_unknown(&v1)) {
		rt_value_destroy(&arg0);
		rt_value_destroy(&arg1);
		rt_value_destroy(&v0);
		rt_value_destroy(&v1);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	cf_assert(v0.type == v1.type, AS_EXP, "unexpected");
	ret_val->type = RT_TRILEAN;

	switch (v0.type) {
	case RT_NIL:
		ret_val->r_trilean = AS_EXP_TRUE;
		break;
	case RT_TRILEAN:
		ret_val->r_trilean = cmp_trilean(ob->code, &v0, &v1);
		break;
	case RT_INT:
		ret_val->r_trilean = cmp_int(ob->code, &v0, &v1);
		break;
	case RT_FLOAT:
		ret_val->r_trilean = cmp_float(ob->code, &v0, &v1);
		break;
	case RT_STR:
	case RT_BLOB:
		ret_val->r_trilean = cmp_bytes(ob->code, &v0, &v1);
		break;
	case RT_GEO_COMPILED: {
		geo_data gd0;
		geo_data gd1;

		rt_value_get_geo(&v0, &gd0);
		rt_value_get_geo(&v1, &gd1);

		ret_val->r_trilean = as_geojson_match(gd0.region != NULL,
				gd0.cellid, gd0.region, gd1.cellid, gd1.region, true);

		break;
	}
	case RT_MSGPACK:
		ret_val->r_trilean = cmp_msgpack(ob->code, &v0, &v1);
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %u", v0.type);
	}

	rt_value_destroy(&arg0);
	rt_value_destroy(&arg1);
	rt_value_destroy(&v0);
	rt_value_destroy(&v1);
}

static void
eval_cmp_regex(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	rt_eval(rt, ret_val);

	if (rt_value_is_unknown(ret_val)) {
		return;
	}

	uint32_t str_sz = 0; // initialized for centos 6
	const uint8_t* str = rt_value_get_str(ret_val, &str_sz);

	if (str == NULL) {
		rt_value_destroy(ret_val);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	op_cmp_regex* op = (op_cmp_regex*)ob;
	char* tmp = cf_strndup((const char*)str, str_sz); // TODO - maybe improve this
	int rv = regexec(&op->regex, tmp, 0, NULL, 0);

	cf_free(tmp);

	rt_value_destroy(ret_val);
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (rv == 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
}

static void
eval_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_logical* op = (op_logical*)ob;
	as_exp_trilean ret = AS_EXP_TRUE;

	while (rt->op_ix < op->instr_end_ix) {
		rt_eval(rt, ret_val);

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (rt_value_is_unknown(ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		if (ret_val->r_trilean == AS_EXP_FALSE) {
			ret = AS_EXP_FALSE;
			rt_skip(rt, op->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

static void
eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_logical* op = (op_logical*)ob;
	as_exp_trilean ret = AS_EXP_FALSE;

	while (rt->op_ix < op->instr_end_ix) {
		rt_eval(rt, ret_val);

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (rt_value_is_unknown(ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			ret = AS_EXP_TRUE;
			rt_skip(rt, op->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

static void
eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	rt_eval(rt, ret_val);

	cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

	if (rt_value_is_unknown(ret_val)) {
		return;
	}

	ret_val->r_trilean = (ret_val->r_trilean == AS_EXP_TRUE) ?
			AS_EXP_FALSE : AS_EXP_TRUE;
}

static void
eval_meta_digest_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_meta_digest_modulo* op = (op_meta_digest_modulo*)ob;
	uint32_t val = *(uint32_t*)&rt->ctx->r->keyd.digest[16];

	ret_val->type = RT_INT;
	ret_val->r_int = val % op->mod;
}

static void
eval_meta_device_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)as_storage_record_device_size(rt->ctx->ns, rt->ctx->r);
}

static void
eval_meta_last_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)cf_utc_ns_from_clepoch_ms(rt->ctx->r->last_update_time);
}

static void
eval_meta_since_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	uint64_t now = cf_clepoch_milliseconds();
	uint64_t lut = rt->ctx->r->last_update_time;

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)(now > lut ? now - lut : 0);
}

static void
eval_meta_void_time(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_INT;
	ret_val->r_int = (rt->ctx->r->void_time == 0) ?
			-1 : (int64_t)cf_utc_ns_from_clepoch_sec(rt->ctx->r->void_time);
}

static void
eval_meta_ttl(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)(int32_t)
			cf_server_void_time_to_ttl(rt->ctx->r->void_time);
}

static void
eval_meta_set_name(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	uint16_t set_id = as_index_get_set_id(rt->ctx->r);

	if (set_id == 0) {
		ret_val->type = RT_STR;
		ret_val->r_bytes.contents = EMPTY_STRING;
		ret_val->r_bytes.sz = 0;
		return;
	}

	as_set* set = as_namespace_get_set_by_id(rt->ctx->ns, set_id);

	ret_val->type = RT_STR;
	ret_val->r_bytes.contents = (const uint8_t*)set->name;
	ret_val->r_bytes.sz = strlen(set->name);
}

static void
eval_meta_key_exists(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (rt->ctx->r->key_stored == 0 ?
			AS_EXP_FALSE : AS_EXP_TRUE);
}

static void
eval_meta_is_tombstone(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (as_record_is_live(rt->ctx->r) ?
			AS_EXP_FALSE : AS_EXP_TRUE);
}

static void
eval_meta_memory_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)as_storage_record_mem_size(rt->ctx->ns, rt->ctx->r);
}

static void
eval_rec_key(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt->ctx->rd == NULL) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (! as_storage_rd_load_key(rt->ctx->rd)) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	const op_rec_key* op = (const op_rec_key*)ob;
	const uint8_t* key = rt->ctx->rd->key;
	uint32_t key_sz = rt->ctx->rd->key_size;

	cf_assert(key_sz != 0, AS_EXP, "key_size can be 0?");

	switch (key[0]) {
	case AS_PARTICLE_TYPE_INTEGER:
		if (key_sz != sizeof(int64_t) + 1) {
			cf_warning(AS_EXP, "eval_rec_key - unexpected integer key size %u",
					key_sz);
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
			return;
		}

		ret_val->type = RT_INT;
		ret_val->r_int = (int64_t)cf_swap_from_be64(*(uint64_t*)(key + 1));
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
		ret_val->type = (key[0] == AS_PARTICLE_TYPE_STRING) ? RT_STR : RT_BLOB;
		ret_val->r_bytes.contents = key + 1;
		ret_val->r_bytes.sz = key_sz - 1;
		break;
	default:
		cf_warning(AS_EXP, "eval_rec_key - invalid key type %u", key[0]);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (op->type != ret_val->type) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
	}
}

static void
eval_bin(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt->ctx->rd == NULL) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	op_bin* op = (op_bin*)ob;
	as_bin* bin = get_live_bin(rt->ctx->rd, op->name, op->name_sz);

	if (bin == NULL) {
		cf_debug(AS_EXP, "eval_bin - bin (%.*s) not found",
				op->name_sz, op->name);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	as_particle_type bin_type = as_bin_get_particle_type(bin);

	if (! bin_is_type(bin, op->type)) {
		cf_debug(AS_EXP, "eval_bin - bin (%.*s) type mismatch %u does not map to %u",
				op->name_sz, op->name, bin_type, op->type);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	switch (bin_type) {
	case AS_PARTICLE_TYPE_INTEGER:
		ret_val->type = RT_INT;
		ret_val->r_int = as_bin_particle_integer_value(bin);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = as_bin_particle_float_value(bin);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
		ret_val->type = RT_BIN_PTR;
		ret_val->r_bin_p = bin;
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

static void
eval_bin_type(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_bin_type* op = (op_bin_type*)ob;

	if (rt->ctx->rd == NULL) {
		cf_debug(AS_EXP, "eval_bin - bin (%.*s) not found",
				op->name_sz, op->name);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	as_bin* b = get_live_bin(rt->ctx->rd, op->name, op->name_sz);

	ret_val->type = RT_INT;
	ret_val->r_int = (b == NULL) ?
			AS_PARTICLE_TYPE_NULL : (uint64_t)as_bin_get_particle_type(b);
}

static void
eval_call(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	const op_call* op = (const op_call*)ob;
	msgpack_vec vecs[op->n_vecs];
	msgpack_in_vec mv = {
			.n_vecs = op->n_vecs + 1,
			.vecs = vecs
	};

	cf_debug(AS_EXP, "eval_call sys %u n_vecs %u", op->system_type, mv.n_vecs);

	vecs[0].buf = op->vecs[0].buf;
	vecs[0].buf_sz = op->vecs[0].buf_sz;
	vecs[0].offset = 0;

	uint32_t vec_ix = 1;
	uint32_t op_ix = 1;
	uint8_t buf[1024];

	as_packer pk = {
			.buffer = buf,
			.capacity = sizeof(buf)
	};

	uint32_t param_idx = 0;
	rt_value param_ret_vals[op->eval_count];
	uint32_t blob_cleanup_ix = 0;
	void* blob_cleanup[op->eval_count];
	uint32_t bin_cleanup_ix = 0;
	as_bin* bin_cleanup[op->eval_count];

	for (; op_ix < op->n_vecs; op_ix++) {
		if (op->vecs[op_ix].buf == call_eval_token) {
			rt_value* from = &param_ret_vals[param_idx++];
			rt_value to;

			rt_eval(rt, from);

			vecs[vec_ix].buf = pk.buffer + pk.offset;
			vecs[vec_ix].offset = 0;

			cf_debug(AS_EXP, "rt_eval -> type %d (29=RT_BIN)", from->type);

			if (from->type == RT_BIN) {
				cf_debug(AS_EXP, "RT_BIN -> cleanup %p(%p)", &from->r_bin,
						from->r_bin.particle);
				bin_cleanup[bin_cleanup_ix++] = &from->r_bin;
			}

			if (rt_value_is_unknown(from) ||
					! rt_value_bin_translate(&to, from)) {
				cf_debug(AS_EXP, "CALL UNK");
				ret_val->type = RT_TRILEAN;
				ret_val->r_trilean = AS_EXP_UNK;
				rt_skip(rt, op->instr_end_ix);
				call_cleanup(blob_cleanup, blob_cleanup_ix, bin_cleanup,
						bin_cleanup_ix);
				return;
			}

			switch (to.type) {
			case RT_NIL:
				as_pack_nil(&pk);
				vecs[vec_ix].buf_sz =
						(uint32_t)(pk.buffer + pk.offset - vecs[vec_ix].buf);
				break;
			case RT_TRILEAN:
				as_pack_bool(&pk, to.r_trilean == AS_EXP_TRUE);
				vecs[vec_ix].buf_sz =
						(uint32_t)(pk.buffer + pk.offset - vecs[vec_ix].buf);
				break;
			case RT_INT:
				as_pack_int64(&pk, to.r_int);
				vecs[vec_ix].buf_sz =
						(uint32_t)(pk.buffer + pk.offset - vecs[vec_ix].buf);
				break;
			case RT_FLOAT:
				as_pack_int64(&pk, to.r_float);
				vecs[vec_ix].buf_sz =
						(uint32_t)(pk.buffer + pk.offset - vecs[vec_ix].buf);
				break;
			case RT_GEO_CONST:
				vecs[vec_ix].buf = to.r_geo_const.op->contents;
				vecs[vec_ix].buf_sz = to.r_geo_const.op->content_sz;
				break;
			case RT_STR:
			case RT_BLOB:
			case RT_HLL:
			case RT_GEO_STR: {
				uint8_t* p = cf_malloc(as_pack_str_size(to.r_bytes.sz + 1));
				as_packer strpk = { .buffer = p, .capacity = UINT32_MAX };

				pack_typed_str(&strpk, to.r_bytes.contents, to.r_bytes.sz,
						to.type);

				vecs[vec_ix].buf = p;
				vecs[vec_ix].buf_sz = strpk.offset;
				blob_cleanup[blob_cleanup_ix++] = p;
				break;
			}
			case RT_MSGPACK:
				vecs[vec_ix].buf = to.r_bytes.contents;
				vecs[vec_ix].buf_sz = to.r_bytes.sz;
				break;
			case RT_BIN_PTR:
			case RT_BIN:
			case RT_GEO_COMPILED:
			default:
				cf_crash(AS_EXP, "unexpected type %d", to.type);
			}
		}
		else {
			vecs[vec_ix].buf = op->vecs[op_ix].buf;
			vecs[vec_ix].buf_sz = op->vecs[op_ix].buf_sz;
			vecs[vec_ix].offset = 0;
		}

		vec_ix++;
	}

	as_bin* b;
	rt_value bin_arg;
	as_bin old;
	bool is_modify_local = ((op->system_type & CALL_FLAG_MODIFY_LOCAL) != 0);

	rt_eval(rt, &bin_arg);

	switch (bin_arg.type) {
	case RT_BIN_PTR: // from bin
		cf_debug(AS_EXP, "RT_BIN_PTR: %p(%p)", bin_arg.r_bin_p,
				bin_arg.r_bin_p->particle);
		if (is_modify_local) {
			old = *bin_arg.r_bin_p;
			b = &old; // must not modify bin_arg.r_bin_p
		}
		else {
			b = bin_arg.r_bin_p;
		}
		break;
	case RT_BIN: // from call
		cf_debug(AS_EXP, "RT_BIN: %p(%p)", &bin_arg.r_bin,
				bin_arg.r_bin.particle);
		b = &bin_arg.r_bin;
		old = *b;
		break;
	default:
		cf_debug(AS_EXP, "CALL UNK");
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		call_cleanup(blob_cleanup, blob_cleanup_ix, bin_cleanup,
				bin_cleanup_ix);
		return;
	}

	as_bin rb = { 0 };
	int ret = AS_OK;

	switch (op->system_type & ~CALL_FLAG_MODIFY_LOCAL) {
	case CALL_CDT:
		if (is_modify_local) {
			as_particle* saved = b->particle; // for detail only
			cf_debug(AS_EXP, "call_cdt_modify(b=%p(%p) rb=%p(%p))",
					b, b->particle, &rb, rb.particle);
			ret = as_bin_cdt_modify_exp(b, &mv, &rb);
			cf_debug(AS_EXP, " -> %s(b=%p(%p) new rb=%p(%p))",
					b->particle == saved ? "no-op" : "new",
							b, b->particle, &rb, rb.particle);
		}
		else {
			cf_debug(AS_EXP, "as_bin_call_cdt_read(b=%p(%p) rb=%p(%p) type %d",
					b, b->particle, &rb, rb.particle,
					as_bin_get_particle_type(&rb));
			ret = as_bin_cdt_read_exp(b, &mv, &rb);
			cf_debug(AS_EXP, " -> new rb=%p(%p) type %d", &rb, rb.particle,
					as_bin_get_particle_type(&rb));
		}
		break;
	case CALL_BITS:
		if (is_modify_local) {
			ret = as_bin_bits_modify_exp(b, &mv);
		}
		else {
			ret = as_bin_bits_read_exp(b, &mv, &rb);
		}
		break;
	case CALL_HLL:
		if (is_modify_local) {
			ret = as_bin_hll_modify_exp(b, &mv, &rb);
		}
		else {
			ret = as_bin_hll_read_exp(b, &mv, &rb);
		}
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}

	call_cleanup(blob_cleanup, blob_cleanup_ix, bin_cleanup, bin_cleanup_ix);

	if (ret != AS_OK) {
		rt_value_destroy(&bin_arg);
		cf_debug(AS_EXP, "CALL UNK ret %d", ret);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (is_modify_local) {
		if (old.particle != b->particle && bin_arg.type == RT_BIN) {
			cf_debug(AS_EXP, "bin destroy1 %p(%p) -- RT_BIN case", &old,
					old.particle);
			as_bin_particle_destroy(&old);
		}

		cf_debug(AS_EXP, "rbin destroy2 %p(%p) type %d", &rb, rb.particle,
				as_bin_get_particle_type(&rb));
		as_bin_particle_destroy(&rb);
	}
	else {
		rt_value_destroy(&bin_arg);
		b = &rb;
	}

	if (! bin_is_type(b, op->type)) {
		if (! is_modify_local || bin_arg.type == RT_BIN ||
				old.particle != b->particle) {
			cf_debug(AS_EXP, "bin destroy3 %p(%p)", b, b->particle);
			as_bin_particle_destroy(b);
		}

		cf_debug(AS_EXP, "CALL UNK type %d - %d", as_bin_get_particle_type(b),
				op->type);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (is_modify_local) {
		if (bin_arg.type == RT_BIN_PTR &&
				bin_arg.r_bin_p->particle == b->particle) { // no-op case from bin
			ret_val->type = RT_BIN_PTR;
			ret_val->r_bin_p = bin_arg.r_bin_p;
			return;
		}

		ret_val->type = RT_BIN;
		ret_val->r_bin = *b;
		return;
	}

	uint8_t type = as_bin_get_particle_type(b);

	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		ret_val->type = RT_NIL;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		ret_val->type = RT_INT;
		ret_val->r_int = as_bin_particle_integer_value(b);
		cf_debug(AS_EXP, "bin destroy4 %p(%p)", b, b->particle);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = as_bin_particle_float_value(b);
		cf_debug(AS_EXP, "bin destroy5 %p(%p)", b, b->particle);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
		ret_val->type = RT_BIN;
		ret_val->r_bin = *b;
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

static void
eval_value(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	switch (ob->code) {
	case VOP_VALUE_NIL:
		ret_val->type = RT_NIL;
		break;
	case VOP_VALUE_INT:
		ret_val->type = RT_INT;
		ret_val->r_int = ((op_value_int*)ob)->value;
		break;
	case VOP_VALUE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = ((op_value_float*)ob)->value;
		break;
	case VOP_VALUE_GEO:
		ret_val->type = RT_GEO_CONST;
		ret_val->r_geo_const.op = (op_value_geo*)ob;
		break;
	case VOP_VALUE_STR:
		ret_val->type = RT_STR;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case VOP_VALUE_BLOB:
		ret_val->type = RT_BLOB;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case VOP_VALUE_LIST:
		ret_val->type = RT_MSGPACK;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	default:
		cf_crash(AS_EXP, "unexpected code %u", ob->code);
	}
}


//==========================================================
// Local helpers - runtime utilities.
//

static void
json_to_rt_geo(const uint8_t* json, uint32_t jsonsz, rt_value* val)
{
	uint64_t cellid;
	geo_region_t region;

	if (! geo_parse(NULL, (const char*)json, jsonsz, &cellid, &region)) {
		val->type = RT_TRILEAN;
		val->r_trilean = AS_EXP_UNK;
		return;
	}

	val->type = RT_GEO_COMPILED;

	if (region != NULL) {
		val->r_geo.type = GEO_REGION_NEED_FREE;
		val->r_geo.region = region;
	}
	else {
		val->r_geo.type = GEO_CELL;
		val->r_geo.cellid = cellid;
	}
}

static void
particle_to_rt_geo(const as_particle* p, rt_value* val)
{
	size_t sz;
	const uint8_t* ptr = (const uint8_t*)as_geojson_mem_jsonstr(p, &sz);

	json_to_rt_geo(ptr, sz, val);
}

static bool
bin_is_type(const as_bin* b, result_type type)
{
	as_particle_type expected;

	switch (type) {
	case TYPE_NIL:
		expected = AS_PARTICLE_TYPE_NULL;
		break;
	case TYPE_INT:
		expected = AS_PARTICLE_TYPE_INTEGER;
		break;
	case TYPE_FLOAT:
		expected = AS_PARTICLE_TYPE_FLOAT;
		break;
	case TYPE_STR:
		expected = AS_PARTICLE_TYPE_STRING;
		break;
	case TYPE_BLOB:
		expected = AS_PARTICLE_TYPE_BLOB;
		break;
	case TYPE_GEOJSON:
		expected = AS_PARTICLE_TYPE_GEOJSON;
		break;
	case TYPE_HLL:
		expected = AS_PARTICLE_TYPE_HLL;
		break;
	case TYPE_LIST:
		expected = AS_PARTICLE_TYPE_LIST;
		break;
	case TYPE_MAP:
		expected = AS_PARTICLE_TYPE_MAP;
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %u", type);
	}

	return as_bin_get_particle_type(b) == expected;
}

static void
rt_skip(runtime* rt, uint32_t instr_end_ix)
{
	while (rt->op_ix < instr_end_ix) {
		op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
		const op_table_entry* entry = &op_table[ob->code];

		rt->op_ix++;
		rt->instr_ptr += entry->size;
	}
}

static void
rt_value_cmp_translate(rt_value* to, const rt_value* from)
{
	cf_debug(AS_EXP, "cmp_translate %u", from->type);

	const as_bin* bin;

	switch (from->type) {
	case RT_BIN:
		bin = &from->r_bin;
		break;
	case RT_BIN_PTR:
		bin = from->r_bin_p;
		break;
	case RT_GEO_CONST:
		to->type = RT_GEO_COMPILED;
		to->r_geo = from->r_geo_const.op->compiled;
		return;
	default:
		*to = *from;

		if (to->type == RT_GEO_COMPILED &&
				to->r_geo.type == GEO_REGION_NEED_FREE) {
			to->r_geo.type = GEO_REGION;
		}

		return;
	}

	uint8_t type = as_bin_get_particle_type(bin);

	cf_debug(AS_EXP, "cmp_translate %u bin_type %u", from->type, type);

	switch (type) {
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_STRING: {
		char* ptr;

		memset(&to->r_bytes, 0, sizeof(to->r_bytes)); // for GCC warning
		to->type = (type == AS_PARTICLE_TYPE_BLOB ? RT_BLOB : RT_STR);
		to->r_bytes.sz = as_bin_particle_string_ptr(bin, &ptr);
		to->r_bytes.contents = (uint8_t*)ptr;

		break;
	}
	case AS_PARTICLE_TYPE_GEOJSON:
		particle_to_rt_geo(bin->particle, to);
		break;
	case AS_PARTICLE_TYPE_LIST: {
		cdt_payload val;

		as_bin_particle_list_get_packed_val(bin, &val);
		to->type = RT_MSGPACK;
		to->r_bytes.sz = val.sz;
		to->r_bytes.contents = val.ptr;
		break;
	}
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

static void
rt_value_destroy(rt_value* val)
{
	if (val->type == RT_GEO_COMPILED && val->r_geo.type == GEO_REGION_NEED_FREE) {
		geo_region_destroy(val->r_geo.region);
	}
	else if (val->type == RT_BIN) {
		cf_debug(AS_EXP, "bin destroyRT %p(%p)", &val->r_bin,
				val->r_bin.particle);
		as_bin_particle_destroy(&val->r_bin);
	}
}

static void
rt_value_get_geo(rt_value* val, geo_data* result)
{
	cf_debug(AS_EXP, "get_geo type %u", val->type);
	cf_assert(val->type == RT_GEO_COMPILED, AS_EXP, "unexpected");

	if (val->r_geo.type == GEO_CELL) {
		result->cellid = val->r_geo.cellid;
		result->region = NULL;
	}
	else {
		result->cellid = 0;
		result->region = val->r_geo.region;
	}
}

static as_bin*
get_live_bin(as_storage_rd* rd, const uint8_t* name, size_t len)
{
	if (rd->ns->single_bin) {
		if (len != 0) {
			return NULL;
		}
	}
	else if (len == 0) {
		return NULL;
	}

	return as_bin_get_live_w_len(rd, name, len);
}


//==========================================================
// Local helpers - runtime compare utilities.
//

static as_exp_trilean
cmp_trilean(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	cf_debug(AS_EXP, "cmp_trilean - lv %u rv %u",
			e0->r_trilean, e1->r_trilean);

	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_trilean == e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_trilean != e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_trilean > e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_trilean >= e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_trilean < e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_trilean <= e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_int(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	cf_debug(AS_EXP, "cmp_int - lv %ld rv %ld", e0->r_int, e1->r_int);

	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_int == e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_int != e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_int > e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_int >= e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_int < e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_int <= e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_float(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	cf_debug(AS_EXP, "cmp_float - e0 %lf e1 %lf", e0->r_float, e1->r_float);

	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_float == e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_float != e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_float > e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_float >= e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_float < e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_float <= e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static const uint8_t*
rt_value_get_str(const rt_value* val, uint32_t* sz_r)
{
	if (val->type == RT_STR) {
		*sz_r = val->r_bytes.sz;
		return val->r_bytes.contents;
	}

	if (val->type == RT_BIN || val->type == RT_BIN_PTR) {
		const as_bin* bp = val->type == RT_BIN ? &val->r_bin : val->r_bin_p;

		if (as_bin_get_particle_type(bp) != AS_PARTICLE_TYPE_STRING) {
			return NULL;
		}

		char* p;

		*sz_r = as_bin_particle_string_ptr(bp, &p);

		return (const uint8_t*)p;
	}

	return NULL;
}

static as_exp_trilean
cmp_bytes(exp_op_code code, const rt_value* v0, const rt_value* v1)
{
	uint32_t s0_sz = v0->r_bytes.sz;
	uint32_t s1_sz = v1->r_bytes.sz;
	const uint8_t* s0 = v0->r_bytes.contents;
	const uint8_t* s1 = v1->r_bytes.contents;
	uint32_t min_sz = (s0_sz < s1_sz) ? s0_sz : s1_sz;
	int cmp = memcmp(s0, s1, min_sz);

	if (cmp == 0 && s0_sz != s1_sz) {
		cmp = (s0_sz < s1_sz) ? -1 : 1;
	}

	switch (code) {
	case EXP_CMP_EQ:
		return (cmp == 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (cmp != 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (cmp > 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (cmp >= 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (cmp < 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (cmp <= 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_msgpack(exp_op_code code, const rt_value* v0, const rt_value* v1)
{
	msgpack_in mp0 = {
			.buf = v0->r_bytes.contents,
			.buf_sz = v0->r_bytes.sz
	};

	msgpack_in mp1 = {
			.buf = v1->r_bytes.contents,
			.buf_sz = v1->r_bytes.sz
	};

	msgpack_cmp_type cmp = msgpack_cmp(&mp0, &mp1);

	cf_debug(AS_EXP, "cmp_msgpack %d\nlv %*pH\nrv %*pH", cmp, mp0.buf_sz,
			mp0.buf, mp1.buf_sz, mp1.buf);

	if (cmp == MSGPACK_CMP_ERROR) {
		return AS_EXP_UNK;
	}

	switch (code) {
	case EXP_CMP_EQ:
		return (cmp == MSGPACK_CMP_EQUAL) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (cmp != MSGPACK_CMP_EQUAL) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (cmp == MSGPACK_CMP_GREATER) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (cmp == MSGPACK_CMP_EQUAL || cmp == MSGPACK_CMP_GREATER) ?
				AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (cmp == MSGPACK_CMP_LESS) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (cmp == MSGPACK_CMP_EQUAL || cmp == MSGPACK_CMP_LESS) ?
				AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}


//==========================================================
// Local helpers - runtime call utilities
//

static void
call_cleanup(void** blob, uint32_t blob_ix, as_bin** bin, uint32_t bin_ix)
{
	for (uint32_t i = 0; i < blob_ix; i++) {
		cf_free(blob[i]);
	}

	for (uint32_t i = 0; i < bin_ix; i++) {
		cf_debug(AS_EXP, "bin destroyC %p(%p)", bin[i], bin[i]->particle);
		as_bin_particle_destroy(bin[i]);
	}
}

static void
pack_typed_str(as_packer* pk, const uint8_t* buf, uint32_t sz, uint8_t type)
{
	uint8_t* ptr = pk->buffer + pk->offset;

	sz++; // +1 for type byte

	if (sz < 32) {
		pk->offset += 1;
		*ptr = (uint8_t)(0xa0 | sz);
	}
	else if (sz < (1 << 8)) {
		pk->offset += 2;
		*ptr++ = 0xd9;
		*ptr = (uint8_t)sz;
	}
	else if (sz < (1 << 16)) {
		pk->offset += 3;
		*ptr++ = 0xda;
		*(uint16_t*)ptr = cf_swap_to_be16(sz);
	}
	else {
		pk->offset += 5;
		*ptr++ = 0xdb;
		*(uint32_t*)ptr = cf_swap_to_be32(sz);
	}

	pk->buffer[pk->offset++] = type; // include type in header
	memcpy(pk->buffer + pk->offset, buf, sz - 1);
	pk->offset += sz - 1;
}

static bool
rt_value_bin_translate(rt_value* to, const rt_value* from)
{
	as_bin b;

	if (from->type == RT_BIN_PTR) {
		b = *from->r_bin_p;
	}
	else if (from->type == RT_BIN) {
		b = from->r_bin;
	}
	else {
		*to = *from;
		return true;
	}

	uint8_t type = as_bin_get_particle_type(&b);

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
		to->type = RT_INT;
		to->r_int = as_bin_particle_integer_value(&b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		to->type = RT_FLOAT;
		to->r_float = as_bin_particle_float_value(&b);
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_GEOJSON:
		to->type = type;
		to->r_bytes.sz = as_bin_particle_string_ptr(&b,
				(char**)&to->r_bytes.contents);
		break;
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST: {
		cdt_payload packed;

		as_bin_particle_list_get_packed_val(&b, &packed);

		to->type = RT_MSGPACK;
		to->r_bytes.contents = packed.ptr;
		to->r_bytes.sz = packed.sz;
		break;
	}
	default:
		return false;
	}

	return true;
}
