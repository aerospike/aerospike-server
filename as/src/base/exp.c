/*
 * exp.c
 *
 * Copyright (C) 2020-2023 Aerospike, Inc.
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

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <regex.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "bits.h"
#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"

// #include "warnings.h"


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
	EXP_UNK = 0,

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
	EXP_EXCLUSIVE = 19,

	EXP_ADD = 20,
	EXP_SUB = 21,
	EXP_MUL = 22,
	EXP_DIV = 23,
	EXP_POW = 24,
	EXP_LOG = 25,
	EXP_MOD = 26,
	EXP_ABS = 27,
	EXP_FLOOR = 28,
	EXP_CEIL = 29,

	EXP_TO_INT = 30,
	EXP_TO_FLOAT = 31,

	EXP_INT_AND = 32,
	EXP_INT_OR = 33,
	EXP_INT_XOR = 34,
	EXP_INT_NOT = 35,
	EXP_INT_LSHIFT = 36,
	EXP_INT_RSHIFT = 37,
	EXP_INT_ARSHIFT = 38,
	EXP_INT_COUNT = 39,
	EXP_INT_LSCAN = 40,
	EXP_INT_RSCAN = 41,

	EXP_MIN = 50,
	EXP_MAX = 51,

	EXP_META_DIGEST_MOD = 64,
	EXP_META_DEVICE_SIZE = 65, // deprecated
	EXP_META_LAST_UPDATE = 66,
	EXP_META_SINCE_UPDATE = 67,
	EXP_META_VOID_TIME = 68,
	EXP_META_TTL = 69,
	EXP_META_SET_NAME = 70,
	EXP_META_KEY_EXISTS = 71,
	EXP_META_IS_TOMBSTONE = 72,
	EXP_META_MEMORY_SIZE = 73, // deprecated
	EXP_META_RECORD_SIZE = 74,

	EXP_REC_KEY = 80,
	EXP_BIN = 81,
	EXP_BIN_TYPE = 82,

	EXP_COND = 123,
	EXP_VAR = 124,
	EXP_LET = 125,
	EXP_QUOTE = 126,
	EXP_CALL = 127,

	EXP_OP_CODE_END, // for wire size, resist this becoming > 128

	// Begin virtual ops - values not on the wire.
	VOP_VALUE_NIL,
	VOP_VALUE_BOOL,
	VOP_VALUE_TRILEAN,
	VOP_VALUE_INT,
	VOP_VALUE_FLOAT,
	VOP_VALUE_STR,
	VOP_VALUE_BLOB,
	VOP_VALUE_GEO,
	VOP_VALUE_HLL,
	VOP_VALUE_MAP,
	VOP_VALUE_LIST,
	VOP_VALUE_MSGPACK,

	VOP_COND_CASE
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

static const char* result_type_str[] = {
		[TYPE_NIL] = "nil",
		[TYPE_TRILEAN] = "bool",
		[TYPE_INT] = "int",
		[TYPE_STR] = "str",
		[TYPE_LIST] = "list",
		[TYPE_MAP] = "map",
		[TYPE_BLOB] = "blob",
		[TYPE_FLOAT] = "float",
		[TYPE_GEOJSON] = "geojson",
		[TYPE_HLL] = "hll"
};

ARRAY_ASSERT(result_type_str, TYPE_END);

static const exp_op_code result_type_to_op_code[] = {
		[TYPE_NIL] = VOP_VALUE_NIL,
		[TYPE_TRILEAN] = VOP_VALUE_TRILEAN,
		[TYPE_INT] = VOP_VALUE_INT,
		[TYPE_STR] = VOP_VALUE_STR,
		[TYPE_LIST] = VOP_VALUE_LIST,
		[TYPE_MAP] = VOP_VALUE_MAP,
		[TYPE_BLOB] = VOP_VALUE_BLOB,
		[TYPE_FLOAT] = VOP_VALUE_FLOAT,
		[TYPE_GEOJSON] = VOP_VALUE_GEO,
		[TYPE_HLL] = VOP_VALUE_HLL
};

ARRAY_ASSERT(result_type_to_op_code, TYPE_END);

typedef struct op_base_mem_s {
	uint32_t instr_end_ix;
	exp_op_code code;
} op_base_mem;

typedef struct op_cmp_regex_s {
	op_base_mem base;
	regex_t regex;
	uint32_t regex_str_sz;
	int32_t flags;
} op_cmp_regex;

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

typedef struct op_cond_s {
	op_base_mem base;
	uint32_t case_count;
} op_cond;

typedef struct op_var_s {
	op_base_mem base;
	uint32_t idx;
	result_type type;
} op_var;

typedef struct op_let_s {
	op_base_mem base;
	uint32_t var_idx;
	uint32_t n_vars;
	result_type type;
} op_let;

#define OP_CALL_MAX_VEC_IDX 18

typedef struct op_vec_s {
	const uint8_t* buf;
	uint32_t buf_sz;
} op_vec;

typedef struct op_call_s {
	op_base_mem base;
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

typedef struct op_value_bool_s {
	op_base_mem base;
	bool value;
} op_value_bool;

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

typedef struct geo_entry_s {
	geo_compiled compiled;
	const uint8_t* contents;
	uint32_t content_sz;
} geo_entry;

typedef struct rt_value_s {
	uint8_t type;
	uint8_t do_not_destroy;
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

typedef struct var_entry_s {
	const uint8_t* name;
	uint32_t name_sz;
	uint32_t idx;
	result_type r_type;
} var_entry;

typedef struct var_scope_s {
	struct var_scope_s* parent;
	uint32_t n_entries;
	var_entry* entries;
} var_scope;

typedef struct runtime_s {
	const as_exp_ctx* ctx;
	const uint8_t* instr_ptr;
	rt_value* vars;
	uint32_t op_ix;
} runtime;

typedef struct op_table_entry_s op_table_entry;

typedef struct build_args_s {
	as_exp* exp;
	uint8_t* mem;
	msgpack_in mp;

	const op_table_entry* entry;
	uint32_t ele_count;
	uint32_t instr_ix;

	uint32_t var_idx;
	uint32_t max_var_idx;
	var_scope* current;
} build_args;

typedef bool (*op_table_build_cb)(build_args* args);
typedef void (*op_table_eval_cb)(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
typedef void (*op_table_display_cb)(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);

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
	op_table_display_cb display_cb;
	uint32_t static_param_count;
	uint32_t eval_param_count;
	result_type r_type;
	const char* name;
};

#define OP_TABLE_ENTRY(__code, __name, __size_name, __build_name, __eval_name, __display_name, __static_param_count, __eval_param_count, __r_type) \
		[__code].code = __code, \
		[__code].name = __name, \
		[__code].size = (uint32_t)sizeof(__size_name), \
		[__code].build_cb = __build_name, \
		[__code].eval_cb = __eval_name, \
		[__code].display_cb = __display_name, \
		[__code].static_param_count = __static_param_count, \
		[__code].eval_param_count = __eval_param_count, \
		[__code].r_type = __r_type,

#define result_type_to_str(__type) (__type >= 0 && __type < TYPE_END ? \
		result_type_str[__type] : "invalid")

static const uint8_t* EMPTY_STRING = (uint8_t*)"";
static const uint8_t call_eval_token[1] = "";


//==========================================================
// Forward declarations.
//

// Build.
static as_exp* build_internal(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire);
static bool build_next(build_args* args);
static const op_table_entry* build_get_entry(result_type type);
static bool build_count_sz(msgpack_in* mp, uint32_t* total_sz, uint32_t* cleanup_count, uint32_t* counter_r);
static var_entry* build_find_var_entry(build_args* args, const uint8_t* name, uint32_t name_sz);
static bool build_default(build_args* args);
static bool build_compare(build_args* args);
static bool build_cmp_regex(build_args* args);
static bool build_cmp_geo(build_args* args);
static bool build_logical_vargs(build_args* args);
static bool build_logical_not(build_args* args);
static bool build_math_vargs(build_args* args);
static bool build_number_op(build_args* args);
static bool build_float_op(build_args* args);
static bool build_int_op(build_args* args);
static bool build_math_pow(build_args* args);
static bool build_math_log(build_args* args);
static bool build_math_mod(build_args* args);
static bool build_int_vargs(build_args* args);
static bool build_int_one(build_args* args);
static bool build_int_shift(build_args* args);
static bool build_int_scan(build_args* args);
static bool build_meta_digest_mod(build_args* args);
static bool build_rec_key(build_args* args);
static bool build_bin(build_args* args);
static bool build_bin_type(build_args* args);
static bool build_cond(build_args* args);
static bool build_var(build_args* args);
static bool build_let(build_args* args);
static bool build_quote(build_args* args);
static bool build_call(build_args* args);
static bool build_value_nil(build_args* args);
static bool build_value_bool(build_args* args);
static bool build_value_int(build_args* args);
static bool build_value_float(build_args* args);
static bool build_value_blob(build_args* args);
static bool build_value_geo(build_args* args);
static bool build_value_msgpack(build_args* args);

// Build utilities.
static bool parse_op_call(op_call* op, build_args* args);
static bool build_set_expected_particle_type(build_args* args);
static as_exp* check_filter_exp(as_exp* exp);

// Runtime.
static as_exp_trilean match_internal(const as_exp* predexp, const as_exp_ctx* ctx);
static bool rt_eval(runtime* rt, rt_value* ret_val);
static void eval_unknown(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_cmp_regex(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_exclusive(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_add(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_sub(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_mul(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_div(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_pow(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_log(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_floor(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_abs(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_ceil(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_to_int(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_to_float(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_xor(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_lshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_rshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_arshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_count(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_lscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_int_rscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_min(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_max(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
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
static void eval_meta_record_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_rec_key(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_bin(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_bin_type(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_cond(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_var(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_let(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_call(runtime* rt, const op_base_mem* ob, rt_value* ret_val);
static void eval_value(runtime* rt, const op_base_mem* ob, rt_value* ret_val);

// Runtime utilities.
static void rt_value_bin_ptr_to_bin(runtime* rt, as_bin* rb, const rt_value* from, cf_ll_buf* ll_buf);
static void json_to_rt_geo(const uint8_t* json, size_t jsonsz, rt_value* val);
static void particle_to_rt_geo(const as_particle* p, rt_value* val);
static bool bin_is_type(const as_bin* b, result_type type);
static void rt_skip(runtime* rt, uint32_t instr_count);
static void rt_value_translate(rt_value* to, const rt_value* from);
static void rt_value_destroy(rt_value* val);
static void rt_value_get_geo(rt_value* val, geo_data* result);
static bool get_live_bin(as_storage_rd* rd, const uint8_t* name, size_t len, as_bin** p_bin);

// Runtime compare utilities.
static as_exp_trilean cmp_nil(exp_op_code code, const rt_value* e0, const rt_value* e1);
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
static void* rt_alloc_mem(runtime* rt, size_t sz, cf_ll_buf* ll_buf);
static bool msgpack_to_bin(runtime* rt, as_bin* to, rt_value* from, cf_ll_buf* ll_buf);

// Runtime runtime display.
static void rt_display(runtime* rt, cf_dyn_buf* db);
static void display_0_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_1_arg(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_2_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_cmp_regex(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_logical_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_math_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_int_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_meta_digest_mod(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_bin(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_bin_type(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_cond(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_var(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_let(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_call(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_value(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);
static void display_case(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db);

static void display_msgpack(msgpack_in* mp, cf_dyn_buf* db);

// Debug utilities.
static void debug_exp_check(const as_exp* exp);


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
		cf_warning(AS_EXP, "%s - error %u expected %u args found %u", name,
				AS_ERR_PARAMETER,
				entry->eval_param_count + entry->static_param_count,
				args->ele_count);
		return false;
	}

	op_base_mem* ob = (op_base_mem*)args->mem;

	ob->code = entry->code;
	args->mem += entry->size;

	return true;
}

static inline bool
rt_value_need_destroy(const rt_value* bin_arg, const as_bin* old,
		const as_bin* b)
{
	return bin_arg->type == RT_BIN && ! bin_arg->do_not_destroy &&
			old->particle != b->particle;
}

static inline bool
rt_value_keep_do_not_destroy(const rt_value* bin_arg, const as_bin* b)
{
	return bin_arg->type == RT_BIN && bin_arg->do_not_destroy &&
			bin_arg->r_bin.particle == b->particle;
}


//==========================================================
// Op table.
//

static const op_table_entry op_table[] = {
		OP_TABLE_ENTRY(EXP_UNK, "unknown", op_base_mem, build_default, eval_unknown, display_0_args, 0, 0, TYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_CMP_EQ, "eq", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_NE, "ne", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GT, "gt", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GE, "ge", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_LT, "lt", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_LE, "le", op_base_mem, build_compare, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_CMP_REGEX, "cmp_regex", op_cmp_regex, build_cmp_regex, eval_cmp_regex, display_cmp_regex, 2, 1, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_CMP_GEO, "cmp_geo", op_base_mem, build_cmp_geo, eval_compare, display_2_args, 0, 2, TYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_AND, "and", op_base_mem, build_logical_vargs, eval_and, display_logical_vargs, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_OR, "or", op_base_mem, build_logical_vargs, eval_or, display_logical_vargs, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_NOT, "not", op_base_mem, build_logical_not, eval_not, display_1_arg, 0, 1, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_EXCLUSIVE, "exclusive", op_base_mem, build_logical_vargs, eval_exclusive, display_logical_vargs, 0, 0, TYPE_TRILEAN)

		OP_TABLE_ENTRY(EXP_ADD, "add", op_base_mem, build_math_vargs, eval_add, display_math_vargs, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_SUB, "sub", op_base_mem, build_math_vargs, eval_sub, display_math_vargs, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_MUL, "mul", op_base_mem, build_math_vargs, eval_mul, display_math_vargs, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_DIV, "div", op_base_mem, build_math_vargs, eval_div, display_math_vargs, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_POW, "pow", op_base_mem, build_math_pow, eval_pow, display_2_args, 0, 2, TYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_LOG, "log", op_base_mem, build_math_log, eval_log, display_2_args, 0, 2, TYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_MOD, "mod", op_base_mem, build_math_mod, eval_mod, display_2_args, 0, 2, TYPE_INT)
		OP_TABLE_ENTRY(EXP_ABS, "abs", op_base_mem, build_number_op, eval_abs, display_1_arg, 0, 1, TYPE_END)
		OP_TABLE_ENTRY(EXP_FLOOR, "floor", op_base_mem, build_float_op, eval_floor, display_1_arg, 0, 1, TYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_CEIL, "ceil", op_base_mem, build_float_op, eval_ceil, display_1_arg, 0, 1, TYPE_FLOAT)
		OP_TABLE_ENTRY(EXP_TO_INT, "to_int", op_base_mem, build_float_op, eval_to_int, display_1_arg, 0, 1, TYPE_INT)
		OP_TABLE_ENTRY(EXP_TO_FLOAT, "to_float", op_base_mem, build_int_op, eval_to_float, display_1_arg, 0, 1, TYPE_FLOAT)

		OP_TABLE_ENTRY(EXP_INT_AND, "int_and", op_base_mem, build_int_vargs, eval_int_and, display_int_vargs, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_OR, "int_or", op_base_mem, build_int_vargs, eval_int_or, display_int_vargs, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_XOR, "int_xor", op_base_mem, build_int_vargs, eval_int_xor, display_int_vargs, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_NOT, "int_not", op_base_mem, build_int_one, eval_int_not, display_1_arg, 0, 1, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_LSHIFT, "int_lshift", op_base_mem, build_int_shift, eval_int_lshift, display_2_args, 0, 2, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_RSHIFT, "int_rshift", op_base_mem, build_int_shift, eval_int_rshift, display_2_args, 0, 2, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_ARSHIFT, "int_arshift", op_base_mem, build_int_shift, eval_int_arshift, display_2_args, 0, 2, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_COUNT, "int_count", op_base_mem, build_int_one, eval_int_count, display_1_arg, 0, 1, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_LSCAN, "int_lscan", op_base_mem, build_int_scan, eval_int_lscan, display_2_args, 0, 2, TYPE_INT)
		OP_TABLE_ENTRY(EXP_INT_RSCAN, "int_rscan", op_base_mem, build_int_scan, eval_int_rscan, display_2_args, 0, 2, TYPE_INT)

		OP_TABLE_ENTRY(EXP_MIN, "min", op_base_mem, build_math_vargs, eval_min, display_math_vargs, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_MAX, "max", op_base_mem, build_math_vargs, eval_max, display_math_vargs, 0, 0, TYPE_END)

		OP_TABLE_ENTRY(EXP_META_DIGEST_MOD, "digest_modulo", op_meta_digest_modulo, build_meta_digest_mod, eval_meta_digest_mod, display_meta_digest_mod, 1, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_DEVICE_SIZE, "device_size", op_base_mem, build_default, eval_meta_device_size, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_LAST_UPDATE, "last_update", op_base_mem, build_default, eval_meta_last_update, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_SINCE_UPDATE, "since_update", op_base_mem, build_default, eval_meta_since_update, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_VOID_TIME, "void_time", op_base_mem, build_default, eval_meta_void_time, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_TTL, "ttl", op_base_mem, build_default, eval_meta_ttl, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_SET_NAME, "set_name", op_base_mem, build_default, eval_meta_set_name, display_0_args, 0, 0, TYPE_STR)
		OP_TABLE_ENTRY(EXP_META_KEY_EXISTS, "key_exists", op_base_mem, build_default, eval_meta_key_exists, display_0_args, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_META_IS_TOMBSTONE, "is_tombstone", op_base_mem, build_default, eval_meta_is_tombstone, display_0_args, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(EXP_META_MEMORY_SIZE, "memory_size", op_base_mem, build_default, eval_meta_memory_size, display_0_args, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(EXP_META_RECORD_SIZE, "record_size", op_base_mem, build_default, eval_meta_record_size, display_0_args, 0, 0, TYPE_INT)

		OP_TABLE_ENTRY(EXP_REC_KEY, "key", op_rec_key, build_rec_key, eval_rec_key, display_0_args, 1, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_BIN, "bin", op_bin, build_bin, eval_bin, display_bin, 2, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_BIN_TYPE, "bin_type", op_bin_type, build_bin_type, eval_bin_type, display_bin_type, 1, 0, TYPE_INT)

		OP_TABLE_ENTRY(EXP_COND, "cond", op_cond, build_cond, eval_cond, display_cond, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_VAR, "var", op_var, build_var, eval_var, display_var, 1, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_LET, "let", op_let, build_let, eval_let, display_let, 0, 0, TYPE_END)
		OP_TABLE_ENTRY(EXP_QUOTE, "quote", op_value_blob, build_quote, eval_value, display_value, 1, 0, TYPE_LIST)
		OP_TABLE_ENTRY(EXP_CALL, "call", op_call, build_call, eval_call, display_call, 2, 2, TYPE_END)

		OP_TABLE_ENTRY(VOP_VALUE_NIL, "nil", op_base_mem, build_value_nil, eval_value, display_value, 0, 0, TYPE_NIL)
		OP_TABLE_ENTRY(VOP_VALUE_BOOL, "bool", op_value_bool, build_value_bool, eval_value, display_value, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(VOP_VALUE_TRILEAN, "trilean", op_base_mem, NULL, eval_value, display_value, 0, 0, TYPE_TRILEAN)
		OP_TABLE_ENTRY(VOP_VALUE_INT, "int", op_value_int, build_value_int, eval_value, display_value, 0, 0, TYPE_INT)
		OP_TABLE_ENTRY(VOP_VALUE_FLOAT, "float", op_value_float, build_value_float, eval_value, display_value, 0, 0, TYPE_FLOAT)
		OP_TABLE_ENTRY(VOP_VALUE_STR, "str", op_value_blob, build_value_blob, eval_value, display_value, 0, 0, TYPE_STR)
		OP_TABLE_ENTRY(VOP_VALUE_BLOB, "blob", op_value_blob, build_value_blob, eval_value, display_value, 0, 0, TYPE_BLOB)
		OP_TABLE_ENTRY(VOP_VALUE_GEO, "geo", op_value_geo, build_value_geo, eval_value, display_value, 0, 0, TYPE_GEOJSON)
		OP_TABLE_ENTRY(VOP_VALUE_MSGPACK, "msgpack", op_value_blob, build_value_msgpack, eval_value, display_value, 0, 0, TYPE_END)

		OP_TABLE_ENTRY(VOP_VALUE_HLL, "hll", op_value_blob, NULL, eval_value, display_value, 0, 0, TYPE_HLL)
		OP_TABLE_ENTRY(VOP_VALUE_MAP, "map", op_base_mem, NULL, eval_value, display_value, 0, 0, TYPE_MAP)
		OP_TABLE_ENTRY(VOP_VALUE_LIST, "list", op_value_blob, NULL, eval_value, display_value, 0, 0, TYPE_LIST)

		OP_TABLE_ENTRY(VOP_COND_CASE, "case", op_base_mem, NULL, NULL, display_case, 0, 0, TYPE_END)
};


//==========================================================
// Public API.
//

as_exp*
as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(buf64_sz);
	uint8_t* buf = cf_malloc(buf_sz);
	uint32_t buf_sz_out;

	if (! cf_b64_validate_and_decode(buf64, buf64_sz, buf, &buf_sz_out)) {
		cf_free(buf);
		return NULL;
	}

	cf_assert(buf_sz_out <= buf_sz, AS_EXP, "buf_sz_out %u buf_sz %u",
			buf_sz_out, buf_sz);

	cf_debug(AS_EXP, "as_exp_filter_build_base64 - buf_sz %u msg-dump:\n%*pH",
			buf_sz_out, buf_sz_out, buf);

	as_exp* exp = build_internal(buf, buf_sz_out, true);

	cf_free(buf);

	return exp == NULL ? NULL : check_filter_exp(exp);
}

as_exp*
as_exp_filter_build(const as_msg_field* m, bool cpy_wire)
{
	cf_debug(AS_EXP, "as_exp_filter_build - msg_field_sz %u msg-dump\n%*pH",
			m->field_sz, as_msg_field_get_value_sz(m), m->data);

	as_exp* exp = build_internal(m->data, as_msg_field_get_value_sz(m),
			cpy_wire);

	if (exp == NULL) {
		return NULL;
	}

	return check_filter_exp(exp);
}

as_exp*
as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire)
{
	cf_debug(AS_EXP, "as_exp_build_buf - buf_sz %u buf-dump:\n%*pH",
			buf_sz, buf_sz, buf);

	return build_internal(buf, buf_sz, cpy_wire);
}

bool
as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb,
		cf_ll_buf* particles_llb)
{
	rt_value vars[exp->max_var_count];
	rt_value ret_val;

	runtime rt = {
			.ctx = ctx,
			.instr_ptr = exp->mem,
			.vars = vars,
	};

	rt_eval(&rt, &ret_val);

	if (ret_val.type == RT_BIN_PTR) {
		rt_value_bin_ptr_to_bin(&rt, rb, &ret_val, particles_llb);
		return true;
	}

	switch (ret_val.type) {
	case RT_NIL:
		as_bin_set_empty(rb);
		break;
	case RT_INT:
		rb->particle = (as_particle*)ret_val.r_int;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
		break;
	case RT_FLOAT:
		*((double *)(&rb->particle)) = ret_val.r_float;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_FLOAT);
		break;
	case RT_TRILEAN:
		if (ret_val.r_trilean == AS_EXP_UNK) {
			return false;
		}

		rb->particle = (as_particle*)(uint64_t)
				(ret_val.r_trilean == AS_EXP_TRUE ? 1 : 0);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		break;
	case RT_GEO_CONST:
		rb->particle = rt_alloc_mem(&rt, as_geojson_particle_sz(
				MAX_REGION_CELLS, ret_val.r_geo_const.op->content_sz),
				particles_llb); // over allocate
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_GEOJSON);
		((cdt_mem*)rb->particle)->type = AS_PARTICLE_TYPE_GEOJSON;

		msgpack_in mp = {
				.buf = ret_val.r_geo_const.op->contents,
				.buf_sz = ret_val.r_geo_const.op->content_sz
		};

		uint32_t json_sz = 0;
		const char* json = (const char*)msgpack_get_bin(&mp, &json_sz);

		// Skip as_bytes type.
		json++;
		json_sz--;

		// TODO - already checked in eval_bin?
		if (! as_geojson_to_particle(json, json_sz, &rb->particle)) {
			cf_warning(AS_EXP, "as_exp_eval - invalid geojson");

			if (particles_llb == NULL) {
				as_bin_particle_destroy(rb);
			}

			return false;
		}

		break;
	case RT_STR:
	case RT_BLOB:
	case RT_HLL: {
		as_particle_type particle_type = ret_val.type == RT_STR ?
			AS_PARTICLE_TYPE_STRING : (ret_val.type == RT_BLOB ?
				AS_PARTICLE_TYPE_BLOB : AS_PARTICLE_TYPE_HLL);

		rb->particle = rt_alloc_mem(&rt, sizeof(cdt_mem) + ret_val.r_bytes.sz,
				particles_llb);

		((cdt_mem*)rb->particle)->sz = ret_val.r_bytes.sz;
		((cdt_mem*)rb->particle)->type = (uint8_t)particle_type;
		memcpy(((cdt_mem*)rb->particle)->data, ret_val.r_bytes.contents,
				ret_val.r_bytes.sz);
		as_bin_state_set_from_type(rb, particle_type);
		break;
	}
	case RT_BIN:
		rb->state = ret_val.r_bin.state;

		if (particles_llb == NULL) {
			rb->particle = ret_val.r_bin.particle;
		}
		else {
			as_bin* b = &ret_val.r_bin;
			uint32_t sz = sizeof(cdt_mem) + ((cdt_mem*)b->particle)->sz;

			rb->particle = rt_alloc_mem(&rt, sz, particles_llb);
			memcpy(rb->particle, b->particle, sz);

			as_bin_particle_destroy(b);
		}

		break;
	case RT_MSGPACK:
		if (! msgpack_to_bin(&rt, rb, &ret_val, particles_llb)) {
			cf_warning(AS_EXP, "as_exp_eval - invalid msgpack");
			return false;
		}

		break;
	default:
		cf_warning(AS_EXP, "as_exp_eval - unexpected result type (%u)",
				ret_val.type);
		rt_value_destroy(&ret_val);
		return false;
	}

	return true;
}

as_exp_trilean
as_exp_matches_metadata(const as_exp* predexp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd == NULL, AS_EXP, "invalid parameter");

	as_exp_trilean ret = match_internal(predexp, ctx);

	return ret;
}

bool
as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd != NULL, AS_EXP, "invalid parameter");

	as_exp_trilean ret = match_internal(predexp, ctx);

	return ret == AS_EXP_TRUE;
}

bool
as_exp_display(const as_exp* exp, cf_dyn_buf *db)
{
	if (exp == NULL) {
		cf_warning(AS_EXP, "as_exp_display - could not parse expressions");
		return false;
	}

	runtime rt = { .instr_ptr = exp->mem };

	rt_display(&rt, db);

	return true;
}

void
as_exp_destroy(as_exp* exp)
{
	if (exp == NULL) {
		return;
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

	if (msgpack_buf_get_list_ele_count(mp.buf, mp.buf_sz, &top_count) &&
			top_count == 0) {
		cf_warning(AS_EXP, "build_internal - invalid expression or empty list");
		return NULL;
	}

	uint32_t total_sz = 0;
	uint32_t cleanup_count = 0;
	uint32_t counter = 1;

	if (! build_count_sz(&mp, &total_sz, &cleanup_count, &counter)) {
		return NULL;
	}

	if (counter != 0) {
		cf_warning(AS_EXP, "build_internal - incomplete expression field expected %u more elements",
				counter);
		return false;
	}

	if (total_sz >= EXP_MAX_SIZE) {
		cf_warning(AS_EXP, "build_internal - expression size exceeds limit of %u bytes",
				EXP_MAX_SIZE);
		return NULL;
	}

	if (mp.offset != mp.buf_sz) {
		cf_warning(AS_EXP, "build_internal - malformed expression field");
		return NULL;
	}

	uint32_t cleanup_offset = total_sz;

	total_sz += cleanup_count * (uint32_t)sizeof(void*);

	if (cpy_wire) {
		total_sz += mp.buf_sz;
	}

	build_args args = {
			.exp = cf_calloc(1, sizeof(as_exp) + total_sz)
	};

	args.mem = args.exp->mem;
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

	debug_exp_check(args.exp);

	if (! build_next(&args)) {
		as_exp_destroy(args.exp);
		return NULL;
	}

	cf_assert(args.mem <= (uint8_t*)args.exp->cleanup_stack, AS_EXP, "read past cleanup_stack %p > %p",
			args.mem, args.exp->cleanup_stack);
	cf_assert(args.exp->cleanup_stack_ix <= cleanup_count, AS_EXP, "cleanup_stack_ix (%u) not equal to cleanup_count (%u)",
			args.exp->cleanup_stack_ix, cleanup_count);

	args.exp->max_var_count = args.max_var_idx;

	if (! build_set_expected_particle_type(&args)) {
		as_exp_destroy(args.exp);
		return NULL;
	}

	if (cf_log_check_level(AS_EXP, CF_DETAIL)) {
		cf_dyn_buf_define_size(db, 10240);
		runtime rt = { .instr_ptr = args.exp->mem };

		rt_display(&rt, &db);

		cf_detail(AS_EXP, "parsed exp: %.*s", (int)db.used_sz, db.buf);

		cf_dyn_buf_free(&db);
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
		case MSGPACK_TYPE_FALSE:
		case MSGPACK_TYPE_TRUE:
			op_code = VOP_VALUE_BOOL;
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

	op_base_mem* op = (op_base_mem*)args->mem;
	bool rv = op_table[op_code].build_cb(args);

	op->instr_end_ix = args->instr_ix;

	debug_exp_check(args->exp);

	return rv;
}

static const op_table_entry*
build_get_entry(result_type type)
{
	if ((uint32_t)type >= TYPE_END) {
		return NULL;
	}

	return &op_table[result_type_to_op_code[type]];
}

static bool
build_count_sz(msgpack_in* mp, uint32_t* total_sz, uint32_t* cleanup_count,
		uint32_t* counter_r)
{
	while (mp->offset < mp->buf_sz) {
		msgpack_type type = msgpack_peek_type(mp);

		if (type == MSGPACK_TYPE_ERROR) {
			cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
					mp->offset);
			return false;
		}

		(*counter_r)--;

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

			if (type == MSGPACK_TYPE_BYTES) {
				uint32_t temp_sz;
				const uint8_t* buf = msgpack_get_bin(mp, &temp_sz);

				if (buf == NULL || temp_sz == 0) {
					cf_warning(AS_EXP, "build_count_sz - invalid blob at offset %u",
							mp->offset);
					return false;
				}

				if (*buf != AS_BYTES_BLOB && *buf != AS_BYTES_HLL) {
					cf_warning(AS_EXP, "build_count_sz - invalid blob type %d at offset %u",
							*buf, mp->offset);
					return false;
				}
			}
			else if (msgpack_sz(mp) == 0) {
				cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}
		}
		else {
			if (! msgpack_get_list_ele_count(mp, &ele_count) ||
					ele_count == 0) {
				cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			*counter_r += ele_count - 1;

			if (! msgpack_get_uint64(mp, &op_code)) {
				cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			if (op_code >= EXP_OP_CODE_END) {
				cf_warning(AS_EXP, "build_count_sz - invalid op_code %lu", op_code);
				return false;
			}
		}

		const op_table_entry* entry = &op_table[op_code];

		if (entry->size == 0) {
			cf_warning(AS_EXP, "build_count_sz - invalid op_code %lu size %u",
				op_code, entry->size);
			return false;
		}

		if (entry->static_param_count != 0 &&
				msgpack_sz_rep(mp, entry->static_param_count) == 0) {
			cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
					mp->offset);
			return false;
		}

		*counter_r -= entry->static_param_count;
		*total_sz += entry->size;

		if (op_code == EXP_CALL) {
			uint32_t param_count;
			int64_t call_op_code;

			(*counter_r)--;

			if (! msgpack_get_list_ele_count(mp, &param_count) ||
					param_count == 0 || ! msgpack_get_int64(mp, &call_op_code)) {
				cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			if (call_op_code == AS_CDT_OP_CONTEXT_EVAL && (param_count != 3 ||
					msgpack_sz(mp) == 0 || // skip context
					! msgpack_get_list_ele_count(mp, &param_count) ||
					! msgpack_get_int64(mp, &call_op_code))) {
				cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
						mp->offset);
				return false;
			}

			for (uint32_t i = 1; i < param_count; i++) {
				// TODO - Skip allocating space until we reach the first list.
				// Non lists after the first list will result in over-allocation
				// of op space. May want to improve accounting in the future.
				if (msgpack_peek_type(mp) == MSGPACK_TYPE_LIST) {
					*counter_r += param_count - i;
					break;
				}

				if (msgpack_sz(mp) == 0) {
					cf_warning(AS_EXP, "build_count_sz - invalid instruction at offset %u",
							mp->offset);
					return false;
				}
			}
		}
		else if (op_code == EXP_COND) {
			*total_sz += (ele_count / 2) * op_table[VOP_COND_CASE].size;
		}
		else if (op_code == EXP_LET) {
			if (ele_count % 2 == 1) {
				cf_warning(AS_EXP, "build_count_sz - invalid 'let' op at offset %u ele_count %u",
						mp->offset, ele_count);
				return false;
			}

			for (uint32_t i = 0; i < (ele_count - 1) / 2; i++) {
				uint32_t sz;

				(*counter_r)--;

				if (msgpack_get_bin(mp, &sz) == NULL) {
					cf_warning(AS_EXP, "build_count_sz - invalid 'let' var at offset %u",
							mp->offset);
					return false;
				}

				const uint8_t* start = mp->buf + mp->offset;

				msgpack_in mp_var = {
						.buf = start,
						.buf_sz = msgpack_sz(mp)
				};

				if (mp_var.buf_sz == 0) {
					cf_warning(AS_EXP, "build_count_sz - invalid msgpack at offset %u",
							mp->offset);
					return false;
				}

				if (! build_count_sz(&mp_var, total_sz, cleanup_count,
						counter_r)) {
					cf_warning(AS_EXP, "build_count_sz - invalid 'let' value at offset %u",
							mp->offset);
					return false;
				}
			}
		}

		switch (op_code) {
		case EXP_CMP_REGEX:
		case VOP_VALUE_GEO:
			(*cleanup_count)++;
			break;
		default:
			break;
		}
	}

	return true;
}

static var_entry*
build_find_var_entry(build_args* args, const uint8_t* name, uint32_t name_sz)
{
	for (var_scope* cur = args->current; cur != NULL; cur = cur->parent) {
		for (uint32_t i = 0; i < cur->n_entries; i++) {
			if (cur->entries[i].name_sz != name_sz) {
				continue;
			}

			if (memcmp(cur->entries[i].name, name, name_sz) == 0) {
				return &cur->entries[i];
			}
		}
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
		cf_warning(AS_EXP, "build_compare - error %u mismatched arg types ltype %u (%s) rtype %u (%s)",
				AS_ERR_PARAMETER, ltype, result_type_to_str(ltype),
				rtype, result_type_to_str(rtype));
		return false;
	}

	switch (ltype) {
	case TYPE_GEOJSON:
	case TYPE_HLL:
		cf_warning(AS_EXP, "build_compare - error %u cannot compare arg type %u (%s)",
				AS_ERR_PARAMETER, ltype, result_type_to_str(ltype));
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

	int64_t regex_options;

	if (! msgpack_get_int64(&args->mp, &regex_options)) {
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
		cf_warning(AS_EXP, "build_cmp_regex - error %u invalid arg type %u (%s) != %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type, result_type_to_str(args->entry->r_type),
				TYPE_STR, result_type_str[TYPE_STR]);
		return false;
	}

	if (regex_str_sz == 0 || regex_str[regex_str_sz - 1] != '\0') {
		char temp[regex_str_sz + 1];

		memcpy(temp, regex_str, regex_str_sz);
		temp[regex_str_sz] = '\0';
		rv = regcomp(&op->regex, temp, (int)regex_options);
	}
	else {
		rv = regcomp(&op->regex, (const char*)regex_str, (int)regex_options);
	}

	if (rv != 0) {
		char errbuf[1024];

		regerror(rv, &op->regex, errbuf, sizeof(errbuf));
		cf_warning(AS_EXP, "build_cmp_regex - error %u regex compile %s",
				AS_ERR_PARAMETER, errbuf);

		return false;
	}

	op->regex_str_sz = regex_str_sz;
	op->flags = (int32_t)regex_options;

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
		cf_warning(AS_EXP, "build_cmp_geo - error %u mismatched arg types ltype %u (%s) != %u (%s)",
				AS_ERR_PARAMETER, ltype, result_type_to_str(ltype),
				TYPE_GEOJSON, result_type_str[TYPE_GEOJSON]);
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	if (ltype != rtype) {
		cf_warning(AS_EXP, "build_cmp_geo - error %u mismatched arg types ltype %u (%s) rtype %u (%s)",
				AS_ERR_PARAMETER, ltype, result_type_to_str(ltype), rtype,
				result_type_to_str(rtype));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_logical_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 2) {
		cf_warning(AS_EXP, "build_logical - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != TYPE_TRILEAN) {
			cf_warning(AS_EXP, "build_logical - error %u invalid type at arg %u",
					AS_ERR_PARAMETER, i + 1);
			return false;
		}
	}

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
		cf_warning(AS_EXP, "build_logical_not - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				result_type_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 1) {
		cf_warning(AS_EXP, "build_math - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;
	result_type expected_type = TYPE_END;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		switch (args->entry->r_type) {
		case TYPE_INT:
		case TYPE_FLOAT:
			if (expected_type == TYPE_END) {
				expected_type = args->entry->r_type;
				break;
			}
			else if (expected_type == args->entry->r_type) {
				break;
			}

			cf_warning(AS_EXP, "build_math - error %u mixed types at arg %u",
					AS_ERR_PARAMETER, i + 1);

			return false;
		default:
			cf_warning(AS_EXP, "build_math - error %u invalid type %u (%s) at arg %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type), i + 1);
			return false;
		}
	}

	args->entry = &op_table[expected_type == TYPE_FLOAT ?
			VOP_VALUE_FLOAT : VOP_VALUE_INT];

	return true;
}

static bool
build_math_pow(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_pow")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type arg1 = args->entry->r_type;

	if (! (arg0 == TYPE_FLOAT && arg1 == TYPE_FLOAT)) {
		cf_warning(AS_EXP, "build_math_pow - error %u args are not numeric or different types - base %u (%s) exponent %u (%s)",
				AS_ERR_PARAMETER, arg0, result_type_to_str(arg0), arg1,
				result_type_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_log(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_log")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type arg1 = args->entry->r_type;

	if (! (arg0 == TYPE_FLOAT && arg1 == TYPE_FLOAT)) {
		cf_warning(AS_EXP, "build_math_log - error %u args are not numeric or different types - num %u (%s) base %u (%s)",
				AS_ERR_PARAMETER, arg0, result_type_to_str(arg0), arg1, result_type_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_math_mod(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_math_mod")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type arg1 = args->entry->r_type;

	if (! (arg0 == TYPE_INT && arg1 == TYPE_INT)) {
		cf_warning(AS_EXP, "build_math_mod - error %u args are not integers - numerator %u (%s) denominator %u (%s)",
				AS_ERR_PARAMETER, arg0, result_type_to_str(arg0), arg1, result_type_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_number_op(build_args* args)
{
	if (! build_args_setup(args, "build_number_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	switch (rtype) {
	case TYPE_FLOAT:
		args->entry = &op_table[VOP_VALUE_FLOAT];
		break;
	case TYPE_INT:
		args->entry = &op_table[VOP_VALUE_INT];
		break;
	default:
		cf_warning(AS_EXP, "build_number_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				result_type_to_str(args->entry->r_type));
		return false;
	}

	return true;
}


static bool
build_float_op(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_float_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	if (rtype != TYPE_FLOAT) {
		cf_warning(AS_EXP, "build_float_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				result_type_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_op(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_op")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type rtype = args->entry->r_type;

	if (rtype != TYPE_INT) {
		cf_warning(AS_EXP, "build_int_op - error %u invalid arg type %u (%s)",
				AS_ERR_PARAMETER, args->entry->r_type,
				result_type_to_str(args->entry->r_type));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_vargs(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_base_mem* op = (op_base_mem*)args->mem;

	op->code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 1) {
		cf_warning(AS_EXP, "build_int_vargs - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t ele_count = args->ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != TYPE_INT) {
			cf_warning(AS_EXP, "build_int_vargs - error %u invalid type %u (%s) at arg %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type), i + 1);
			return false;
		}
	}

	args->entry = entry;

	return true;
}

static bool
build_int_one(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_one")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (arg0 != TYPE_INT) {
		cf_warning(AS_EXP, "build_int_one - error %u arg type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg0, result_type_to_str(arg0), TYPE_INT,
				result_type_str[TYPE_INT]);
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_shift(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_shift")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type arg1 = args->entry->r_type;

	if (! (arg0 == TYPE_INT && arg1 == TYPE_INT)) {
		cf_warning(AS_EXP, "build_int_shift - error %u all args are type %u (%s) - arg0 %u (%s) arg1 %u (%s)",
				AS_ERR_PARAMETER, TYPE_INT, result_type_str[TYPE_INT],
				arg0, result_type_to_str(arg0), arg1, result_type_to_str(arg1));
		return false;
	}

	args->entry = entry;

	return true;
}

static bool
build_int_scan(build_args* args)
{
	const op_table_entry* entry = args->entry;

	if (! build_args_setup(args, "build_int_scan")) {
		return false;
	}

	if (! build_next(args)) {
		return false;
	}

	result_type arg0 = args->entry->r_type;

	if (! build_next(args)) {
		return false;
	}

	result_type arg1 = args->entry->r_type;

	if (arg0 != TYPE_INT) {
		cf_warning(AS_EXP, "build_int_scan - error %u arg0 type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg0, result_type_to_str(arg0), TYPE_INT,
				result_type_str[TYPE_INT]);
		return false;
	}

	if (arg1 != TYPE_TRILEAN) {
		cf_warning(AS_EXP, "build_int_scan - error %u arg1 type %u (%s) is not %u (%s)",
				AS_ERR_PARAMETER, arg1, result_type_to_str(arg1), TYPE_TRILEAN,
				result_type_str[TYPE_TRILEAN]);
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
		cf_warning(AS_EXP, "build_rec_digest_modulo - error %u failed to parse an integer",
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
		cf_warning(AS_EXP, "build_rec_key - error %u failed to parse an integer",
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
		cf_warning(AS_EXP, "build_rec_key - error %u invalid result_type %ld (%s)",
				AS_ERR_PARAMETER, type64, result_type_to_str(type64));
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
		cf_warning(AS_EXP, "build_bin - error %u failed to parse an integer",
				AS_ERR_PARAMETER);
		return false;
	}

	op->type = (result_type)type64;
	op->name = msgpack_get_bin(&args->mp, &op->name_sz);

	if (op->name == NULL) {
		cf_warning(AS_EXP, "build_bin - error %u failed to parse a string",
				AS_ERR_PARAMETER);
		return false;
	}

	if (! as_bin_name_check(op->name, op->name_sz)) {
		cf_warning(AS_EXP, "build_bin - error %u parsed invalid bin name",
				AS_ERR_PARAMETER);
		return false;
	}

	if ((args->entry = build_get_entry(op->type)) == NULL) {
		cf_warning(AS_EXP, "build_bin - error %u invalid result_type %d (%s)",
				AS_ERR_PARAMETER, op->type, result_type_to_str(op->type));
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
		cf_warning(AS_EXP, "build_bin_type - error %u failed to parse a string",
				AS_ERR_PARAMETER);
		return false;
	}

	if (! as_bin_name_check(op->name, op->name_sz)) {
		cf_warning(AS_EXP, "build_bin_type - error %u parsed invalid bin name",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_cond(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_cond* op = (op_cond*)args->mem;

	op->base.code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 3) {
		cf_warning(AS_EXP, "build_cond - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	if (args->ele_count % 2 == 0) {
		cf_warning(AS_EXP, "build_cond - error %u requires default case",
				AS_ERR_PARAMETER);
		return false;
	}

	uint32_t ele_count = args->ele_count;
	result_type type = TYPE_END;

	op->case_count = ele_count / 2;

	for (uint32_t i = 0; i < op->case_count; i++) {
		if (! build_next(args)) {
			return false;
		}

		if (args->entry->r_type != TYPE_TRILEAN) {
			cf_warning(AS_EXP, "build_cond - error %u invalid type %u (%s) at condition %u",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type), i + 1);
			return false;
		}

		op_base_mem* op_case = (op_base_mem*)args->mem;

		op_case->code = VOP_COND_CASE;
		args->instr_ix++;
		args->mem += op_table[VOP_COND_CASE].size;

		if (! build_next(args)) {
			return false;
		}

		// Allow returning AS_EXP_UNK and any other return type.
		if (args->entry->code != EXP_UNK) {
			if (type == TYPE_END) {
				type = args->entry->r_type;
			}

			if (args->entry->r_type != type) {
				cf_warning(AS_EXP, "build_cond - error %u mismatched arg type %d (%s) expected type %d (%s) at condition %u",
						AS_ERR_PARAMETER, args->entry->r_type,
						result_type_to_str(args->entry->r_type), type,
						result_type_to_str(type), i + 1);
				return false;
			}
		}

		op_case->instr_end_ix = args->instr_ix;
	}

	if (! build_next(args)) {
		return false;
	}

	if (type == TYPE_END) {
		type = args->entry->r_type;
	}
	else if (args->entry->code != EXP_UNK && args->entry->r_type != type) {
		cf_warning(AS_EXP, "build_cond - error %u mismatched type %d (%s) expected type %d (%s) at default condition",
				AS_ERR_PARAMETER, args->entry->r_type,
				result_type_to_str(args->entry->r_type), type,
				result_type_to_str(type));
		return false;
	}

	args->entry = build_get_entry(type);
	cf_assert(args->entry != NULL, AS_EXP, "unexpected type %d", type);

	return true;
}

static bool
build_var(build_args* args)
{
	op_var* op = (op_var*)args->mem;

	if (! build_args_setup(args, "build_var")) {
		return false;
	}

	uint32_t name_sz;
	const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

	if (name == NULL) {
		cf_warning(AS_EXP, "build_var - error %u failed to parse a string at offset %u",
				AS_ERR_PARAMETER, args->mp.offset);
		return false;
	}

	var_entry* entry = build_find_var_entry(args, name, name_sz);

	if (entry == NULL) {
		cf_warning(AS_EXP, "build_var - error %u undefined var name %.*s at offset %u",
				AS_ERR_PARAMETER, name_sz, name, args->mp.offset);
		return false;
	}

	op->idx = entry->idx;
	op->type = entry->r_type;

	switch (op->type) {
	case TYPE_END:
		cf_warning(AS_EXP, "build_var - error %u using var name %.*s while defining it at offset %u",
				AS_ERR_PARAMETER, name_sz, name, args->mp.offset);
		return false;
	case TYPE_NIL:
		args->entry = &op_table[VOP_VALUE_NIL];
		break;
	case TYPE_TRILEAN:
		args->entry = &op_table[VOP_VALUE_TRILEAN];
		break;
	default:
		if ((args->entry = build_get_entry(op->type)) == NULL) {
			cf_warning(AS_EXP, "build_var - error %u var name %.*s unknown entry type %d (%s)",
					AS_ERR_PARAMETER, name_sz, name, op->type,
					result_type_to_str(op->type));
			return false;
		}
	}

	return true;
}

static bool
build_let(build_args* args)
{
	const op_table_entry* entry = args->entry;
	op_let* op = (op_let*)args->mem;

	op->base.code = entry->code;
	args->mem += entry->size;

	if (args->ele_count < 3) {
		cf_warning(AS_EXP, "build_let - error %u too few args %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	if (args->ele_count % 2 == 0) {
		cf_warning(AS_EXP, "build_let - error %u invalid arg count %u",
				AS_ERR_PARAMETER, args->ele_count);
		return false;
	}

	uint32_t n_vars = args->ele_count / 2;
	var_entry entries[n_vars];
	var_scope scope = {
			.parent = args->current,
			.n_entries = 0,
			.entries = entries
	};

	args->current = &scope;
	op->var_idx = args->var_idx;
	op->n_vars = n_vars;

	for (uint32_t i = 0; i < n_vars; i++) {
		uint32_t name_sz;
		const uint8_t* name = msgpack_get_bin(&args->mp, &name_sz);

		if (name == NULL) {
			cf_warning(AS_EXP, "build_let - error %u failed to parse blob - var at %u",
					AS_ERR_PARAMETER, i);
			return false;
		}

		if (name_sz == 0) {
			cf_warning(AS_EXP, "build_let - error %u variable name length is 0.",
					AS_ERR_PARAMETER);
			return false;
		}

		if (name[0] != '_' && isalpha(name[0]) == 0) {
			cf_warning(AS_EXP, "build_let - error %u illegal variable name '%.*s' at %u - must begin with an alpha or underscore",
					AS_ERR_PARAMETER, name_sz, name, i);
			return false;
		}

		for (uint32_t j = 1; j < name_sz; j++) {
			if (name[j] != '_' && isalnum(name[j]) == 0) {
				cf_warning(AS_EXP, "build_let - error %u illegal variable name '%.*s' at %u - must contain only alpha, digits or underscore",
						AS_ERR_PARAMETER, name_sz, name, i);
				return false;
			}
		}

		if (build_find_var_entry(args, name, name_sz) != NULL) {
			cf_warning(AS_EXP, "build_let - error %u duplicate var name '%.*s' at %u",
					AS_ERR_PARAMETER, name_sz, name, i);
			return false;
		}

		scope.entries[i].name = name;
		scope.entries[i].name_sz = name_sz;
		scope.entries[i].idx = args->var_idx++;
		scope.n_entries++;
		scope.entries[i].r_type = TYPE_END;

		if (! build_next(args)) {
			return false;
		}

		scope.entries[i].r_type = args->entry->r_type;
	}

	if (! build_next(args)) {
		return false;
	}

	op->type = args->entry->r_type;

	if (args->max_var_idx < args->var_idx) {
		args->max_var_idx = args->var_idx;
	}

	args->current = scope.parent;
	args->var_idx -= scope.n_entries;

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

	if ((uint32_t)op->type >= TYPE_END) { // (uint32_t) cast because enum can be signed
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

	if (args->entry->r_type == TYPE_NIL) {
		return true;
	}

	switch (op->system_type & (uint32_t)~CALL_FLAG_MODIFY_LOCAL) {
	case CALL_CDT:
		if (args->entry->r_type != TYPE_LIST &&
				args->entry->r_type != TYPE_MAP) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not list or map",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type));
			return false;
		}
		break;
	case CALL_HLL:
		if (args->entry->r_type != TYPE_HLL) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not hll",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type));
			return false;
		}
		break;
	case CALL_BITS:
		if (args->entry->r_type != TYPE_BLOB) {
			cf_warning(AS_EXP, "build_call - error %u arg %u (%s) is not blob",
					AS_ERR_PARAMETER, args->entry->r_type,
					result_type_to_str(args->entry->r_type));
			return false;
		}
		break;
	default:
		cf_warning(AS_EXP, "build_call - error %u invalid system %d",
				AS_ERR_PARAMETER, op->system_type);
		return false;
	}

	if ((args->entry = build_get_entry(op->type)) == NULL) {
		cf_warning(AS_EXP, "build_call - error %u invalid result_type %d (%s)",
				AS_ERR_PARAMETER, op->type, result_type_to_str(op->type));
		return false;
	}

	return true;
}

static bool
build_value_nil(build_args* args)
{
	if (! build_args_setup(args, "build_value_nil")) {
		return false;
	}

	if (msgpack_sz(&args->mp) == 0) {
		cf_warning(AS_EXP, "build_value_nil - error %u failed to parse nil",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_bool(build_args* args)
{
	op_value_bool* op = (op_value_bool*)args->mem;

	if (! build_args_setup(args, "build_value_bool")) {
		return false;
	}

	if (! msgpack_get_bool(&args->mp, &op->value)) {
		cf_warning(AS_EXP, "build_value_bool - error %u failed to parse bool",
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
		cf_warning(AS_EXP, "build_value_int - error %u failed to parse integer",
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
		cf_warning(AS_EXP, "build_value_float - error %u failed to parse float",
				AS_ERR_PARAMETER);
		return false;
	}

	return true;
}

static bool
build_value_blob(build_args* args)
{
	op_value_blob* op = (op_value_blob*)args->mem;

	if (! build_args_setup(args, "build_value_blob")) {
		return false;
	}

	op->value = msgpack_get_bin(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP, "build_value_blob - error %u failed to parse bin",
				AS_ERR_PARAMETER);
		return false;
	}

	cf_assert(op->value_sz != 0, AS_EXP, "unexpected");

	as_bytes_type type = *op->value++;
	op->value_sz--;

	switch (type) {
	case AS_BYTES_BLOB:
		return true; // already set
	case AS_BYTES_STRING:
		op->base.code = VOP_VALUE_STR;
		break;
	case AS_BYTES_HLL:
		op->base.code = VOP_VALUE_HLL;
		break;
	default:
		cf_warning(AS_EXP, "unexpected blob type %u", type);
		return false;
	}

	args->entry = &op_table[op->base.code];

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
		cf_warning(AS_EXP, "build_value_geo - error %u failed to parse string",
				AS_ERR_PARAMETER);
		return false;
	}

	// Skip as_bytes type.
	json++;
	json_sz--;

	uint64_t cellid;
	geo_region_t region;

	op->compiled.region = NULL;

	if (! as_geojson_parse(NULL, (const char*)json, json_sz, &cellid,
			&region)) {
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

	msgpack_type type = msgpack_peek_type(&args->mp);

	op->value = msgpack_get_ele(&args->mp, &op->value_sz);

	if (op->value == NULL) {
		cf_warning(AS_EXP, "build_value_msgpack - error %u failed to parse element from type %u",
				AS_ERR_PARAMETER, type);
		return false;
	}

	if (type == MSGPACK_TYPE_MAP) {
		args->entry = &op_table[VOP_VALUE_MAP];
	}
	else if (type == MSGPACK_TYPE_BYTES) {
		cf_warning(AS_EXP, "build_value_msgpack - error %u unexpected msgpack blob",
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

bool
build_set_expected_particle_type(build_args* args)
{
	switch (args->entry->r_type) {
	case TYPE_NIL:
		args->exp->expected_type = AS_PARTICLE_TYPE_NULL;
		break;
	case TYPE_TRILEAN:
		args->exp->expected_type = AS_PARTICLE_TYPE_BOOL;
		break;
	case TYPE_INT:
		args->exp->expected_type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case TYPE_STR:
		args->exp->expected_type = AS_PARTICLE_TYPE_STRING;
		break;
	case TYPE_LIST:
		args->exp->expected_type = AS_PARTICLE_TYPE_LIST;
		break;
	case TYPE_MAP:
		args->exp->expected_type = AS_PARTICLE_TYPE_MAP;
		break;
	case TYPE_BLOB:
		args->exp->expected_type = AS_PARTICLE_TYPE_BLOB;
		break;
	case TYPE_FLOAT:
		args->exp->expected_type = AS_PARTICLE_TYPE_FLOAT;
		break;
	case TYPE_GEOJSON:
		args->exp->expected_type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	case TYPE_HLL:
		args->exp->expected_type = AS_PARTICLE_TYPE_HLL;
		break;
	case TYPE_END:
	default:
		cf_warning(AS_EXP, "build_set_expected_particle_type - unexpected result_type %u",
				args->entry->r_type);
		return false;
	}

	return true;
}

static as_exp*
check_filter_exp(as_exp* exp)
{
	if ((as_particle_type)exp->expected_type != AS_PARTICLE_TYPE_BOOL) {
		cf_warning(AS_EXP, "check_filter_exp - filters must return type %u (bool) found %u",
				AS_PARTICLE_TYPE_BOOL, exp->expected_type);
		as_exp_destroy(exp);
		return NULL;
	}

	return exp;;
}


//==========================================================
// Local helpers - runtime.
//

static as_exp_trilean
match_internal(const as_exp* predexp, const as_exp_ctx* ctx)
{
	rt_value vars[predexp->max_var_count];
	rt_value ret_val;

	runtime rt = {
			.ctx = ctx,
			.instr_ptr = predexp->mem,
			.vars = vars
	};

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

static bool
rt_eval(runtime* rt, rt_value* ret_val)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];

	rt->op_ix++;
	rt->instr_ptr += entry->size;
	*ret_val = (rt_value){ 0 };
	entry->eval_cb(rt, ob, ret_val);

	bool ret = rt_value_is_unknown(ret_val);

	return ret;
}

static void
eval_unknown(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)rt;
	(void)ob;

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = AS_EXP_UNK;
}

static void
eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	rt_value arg0;
	rt_value arg1;

	if (rt_eval(rt, &arg0)) {
		*ret_val = arg0;
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	if (rt_eval(rt, &arg1)) {
		rt_value_destroy(&arg0);
		*ret_val = arg1;
		return;
	}

	rt_value v0;
	rt_value v1;

	rt_value_translate(&v0, &arg0);
	rt_value_translate(&v1, &arg1);

	cf_assert(v0.type == v1.type, AS_EXP, "unexpected %u %u", v0.type, v1.type);
	ret_val->type = RT_TRILEAN;

	switch (v0.type) {
	case RT_NIL:
		ret_val->r_trilean = cmp_nil(ob->code, &v0, &v1);
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
	if (rt_eval(rt, ret_val)) {
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
	as_exp_trilean ret = AS_EXP_TRUE;

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected - type %u",
				ret_val->type);

		if (ret_val->r_trilean == AS_EXP_FALSE) {
			ret = AS_EXP_FALSE;
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

static void
eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	as_exp_trilean ret = AS_EXP_FALSE;

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			ret = AS_EXP_TRUE;
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

static void
eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

	ret_val->r_trilean = (ret_val->r_trilean == AS_EXP_TRUE) ?
			AS_EXP_FALSE : AS_EXP_TRUE;
}

static void
eval_exclusive(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// Logical xor is implemented to mean that exactly one arg is true.
	as_exp_trilean ret = AS_EXP_FALSE;

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			if (ret == AS_EXP_TRUE) {
				ret_val->r_trilean = AS_EXP_FALSE;
				rt_skip(rt, ob->instr_end_ix);
				return;
			}

			ret = AS_EXP_TRUE;
			continue;
		}
	}

	ret_val->r_trilean = ret;
}

static void
eval_add(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float += arg.r_float;
		}
		else {
			ret_val->r_int += arg.r_int;
		}
	}
}

static void
eval_sub(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	if (rt->op_ix == ob->instr_end_ix) {
		if (is_float) {
			ret_val->r_float = 0.0 - ret_val->r_float;
		}
		else {
			ret_val->r_int = 0 - ret_val->r_int;
		}

		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float -= arg.r_float;
		}
		else {
			ret_val->r_int -= arg.r_int ;
		}
	}
}

static void
eval_mul(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float *= arg.r_float;
		}
		else {
			ret_val->r_int *= arg.r_int;
		}
	}
}

static void
eval_div(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	uint32_t n_args = 1;
	double dproduct = 1.0;
	int64_t iproduct = 1;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		n_args++;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			dproduct *= arg.r_float;
		}
		else {
			iproduct *= arg.r_int;
		}
	}

	if (is_float) {
		if (n_args == 1) {
			dproduct = ret_val->r_float;
			ret_val->r_float = 1.0;
		}

		ret_val->r_float /= dproduct;
	}
	else {
		if (n_args == 1) {
			iproduct = ret_val->r_int;
			ret_val->r_int = 1;
		}

		if (iproduct == 0) {
			cf_warning(AS_EXP, "eval_div - integer division by zero");
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
			return;
		}

		ret_val->r_int /= iproduct;
	}
}

static void
eval_pow(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	ret_val->r_float = pow(ret_val->r_float, arg1.r_float);
}

static void
eval_log(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	ret_val->r_float = log(ret_val->r_float) / log(arg1.r_float);
}

static void
eval_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1) || arg1.r_int == 0) {
		*ret_val = arg1;
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = ret_val->r_int % arg1.r_int;
}

static void
eval_floor(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_float = floor(ret_val->r_float);
}

static void
eval_abs(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	if (ret_val->type == RT_FLOAT) {
		ret_val->r_float = fabs(ret_val->r_float);
	}
	else {
		ret_val->r_int = labs(ret_val->r_int);
	}
}

static void
eval_ceil(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_float = ceil(ret_val->r_float);
}

static void
eval_to_int(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	// Intermediate variable to satisfy Coverity...
	int64_t int_val = (int64_t)ret_val->r_float;

	ret_val->r_int = int_val;
	ret_val->type = RT_INT;
}

static void
eval_to_float(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	// Intermediate variable to satisfy Coverity...
	double float_val = (double)ret_val->r_int;

	ret_val->r_float = float_val;
	ret_val->type = RT_FLOAT;
}

static void
eval_int_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int &= arg.r_int;
	}
}

static void
eval_int_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int |= arg.r_int;
	}
}

static void
eval_int_xor(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int ^= arg.r_int;
	}
}

static void
eval_int_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_int = ~ret_val->r_int;
}

static void
eval_int_lshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int <<= shift.r_int;
}

static void
eval_int_rshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int = (int64_t)(((uint64_t)ret_val->r_int) >> shift.r_int);
}

static void
eval_int_arshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int >>= shift.r_int;
}

static void
eval_int_count(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_int = (int64_t)cf_bit_count64((uint64_t)(ret_val->r_int));
}

static void
eval_int_lscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value is_set_val;

	if (rt_eval(rt, &is_set_val)) {
		*ret_val = is_set_val;
		return;
	}

	if (is_set_val.r_trilean == AS_EXP_FALSE) {
		ret_val->r_int = ~ret_val->r_int;
	}

	ret_val->r_int = (int64_t)cf_msb64((uint64_t)(ret_val->r_int));

	if (ret_val->r_int == 64) {
		ret_val->r_int = -1;
	}
}

static void
eval_int_rscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value is_set_val;

	if (rt_eval(rt, &is_set_val)) {
		*ret_val = is_set_val;
		return;
	}

	if (is_set_val.r_trilean == AS_EXP_FALSE) {
		ret_val->r_int = ~ret_val->r_int;
	}

	ret_val->r_int = 63 - (int64_t)cf_lsb64((uint64_t)(ret_val->r_int));
}

void
eval_min(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			if (ret_val->r_float > arg.r_float) {
				ret_val->r_float = arg.r_float;
			}
		}
		else {
			if (ret_val->r_int > arg.r_int) {
				ret_val->r_int = arg.r_int;
			}
		}
	}
}

static void
eval_max(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			if (ret_val->r_float < arg.r_float) {
				ret_val->r_float = arg.r_float;
			}
		}
		else {
			if (ret_val->r_int < arg.r_int) {
				ret_val->r_int = arg.r_int;
			}
		}
	}
}

static void
eval_meta_digest_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_meta_digest_modulo* op = (op_meta_digest_modulo*)ob;
	uint32_t val = *(uint32_t*)&rt->ctx->r->keyd.digest[16];

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)val % op->mod;
}

// Deprecated - replaced with eval_meta_record_size().
static void
eval_meta_device_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_INT;
	ret_val->r_int = as_namespace_is_memory_only(rt->ctx->ns) ?
			0 : (int64_t)as_record_stored_size(rt->ctx->r);
}

static void
eval_meta_last_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;
	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)cf_utc_ns_from_clepoch_ms(rt->ctx->r->last_update_time);
}

static void
eval_meta_since_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	uint64_t now = cf_clepoch_milliseconds();
	uint64_t lut = rt->ctx->r->last_update_time;

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)(now > lut ? now - lut : 0);
}

static void
eval_meta_void_time(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_INT;
	ret_val->r_int = (rt->ctx->r->void_time == 0) ?
			-1 : (int64_t)cf_utc_ns_from_clepoch_sec(rt->ctx->r->void_time);
}

static void
eval_meta_ttl(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)(int32_t)
			cf_server_void_time_to_ttl(rt->ctx->r->void_time);
}

static void
eval_meta_set_name(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

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
	ret_val->r_bytes.sz = (uint32_t)strlen(set->name);
}

static void
eval_meta_key_exists(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (rt->ctx->r->key_stored == 0 ?
			AS_EXP_FALSE : AS_EXP_TRUE);
}

static void
eval_meta_is_tombstone(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (as_record_is_live(rt->ctx->r) ?
			AS_EXP_FALSE : AS_EXP_TRUE);
}

// Deprecated - replaced with eval_meta_record_size().
static void
eval_meta_memory_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_INT;
	ret_val->r_int = rt->ctx->ns->storage_type == AS_STORAGE_ENGINE_MEMORY ?
			(int64_t)as_record_stored_size(rt->ctx->r) : 0;
}

static void
eval_meta_record_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)as_record_stored_size(rt->ctx->r);
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
	as_bin* bin;

	if (! get_live_bin(rt->ctx->rd, op->name, op->name_sz, &bin)) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (bin == NULL) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	as_particle_type bin_type = as_bin_get_particle_type(bin);

	if (! bin_is_type(bin, op->type)) {
		cf_detail(AS_EXP, "eval_bin - bin (%.*s) type mismatch %u does not map to %u",
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
	case AS_PARTICLE_TYPE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = as_bin_particle_bool_value(bin);
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_GEOJSON:
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
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	as_bin* b;

	if (! get_live_bin(rt->ctx->rd, op->name, op->name_sz, &b)) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = (b == NULL) ?
			AS_PARTICLE_TYPE_NULL : (uint64_t)as_bin_get_particle_type(b);
}

static void
eval_cond(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_cond* op = (op_cond*)ob;

	for (uint32_t i = 0; i < op->case_count; i++) {
		if (rt_eval(rt, ret_val)) {
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected type %d",
				ret_val->type);

		op_base_mem* op_case = (op_base_mem*)rt->instr_ptr;

		cf_assert(op_case->code == VOP_COND_CASE, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			rt_skip(rt, rt->op_ix + 1);
			rt_eval(rt, ret_val);
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		rt_skip(rt, op_case->instr_end_ix);
	}

	rt_eval(rt, ret_val);
}

static void
eval_var(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_var* op = (op_var*)ob;

	*ret_val = rt->vars[op->idx];
	ret_val->do_not_destroy = 1;
}

static void
eval_let(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_let* op = (op_let*)ob;

	for (uint32_t i = 0; i < op->n_vars; i++) {
		rt_eval(rt, &rt->vars[op->var_idx + i]);
	}

	rt_eval(rt, ret_val);

	for (uint32_t i = 0; i < op->n_vars; i++) {
		rt_value* val = &rt->vars[op->var_idx + i];

		if (ret_val->type == RT_BIN && val->type == RT_BIN &&
				ret_val->r_bin.particle == val->r_bin.particle) {
			ret_val->do_not_destroy = 0;
			continue;
		}

		if (ret_val->type == RT_GEO_COMPILED && val->type == RT_GEO_COMPILED &&
				ret_val->r_geo.region == val->r_geo.region) {
			ret_val->do_not_destroy = 0;
			continue;
		}

		rt_value_destroy(val);
	}
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

			if (from->type == RT_BIN && ! from->do_not_destroy) {
				bin_cleanup[bin_cleanup_ix++] = &from->r_bin;
			}

			if (rt_value_is_unknown(from) ||
					! rt_value_bin_translate(&to, from)) {
				ret_val->type = RT_TRILEAN;
				ret_val->r_trilean = AS_EXP_UNK;
				rt_skip(rt, ob->instr_end_ix);
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
				as_pack_double(&pk, to.r_float);
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
		if (is_modify_local) {
			old = *bin_arg.r_bin_p;
			b = &old; // must not modify bin_arg.r_bin_p
		}
		else {
			b = bin_arg.r_bin_p;
		}
		break;
	case RT_NIL:
		as_bin_set_empty(&bin_arg.r_bin);
		b = &bin_arg.r_bin;
		old = *b;
		break;
	case RT_BIN: // from call
		b = &bin_arg.r_bin; // particle on heap
		old = *b;
		break;
	case RT_BLOB:
	case RT_HLL: { // from client
		rt_value temp = bin_arg;

		b = &bin_arg.r_bin;
		bin_arg.type = RT_BIN;
		bin_arg.r_bin.particle = rt_alloc_mem(rt, (size_t)temp.r_bytes.sz +
				sizeof(cdt_mem), NULL);

		cdt_mem* p_cdt_mem = (cdt_mem*)bin_arg.r_bin.particle;

		p_cdt_mem->type = temp.type == RT_BLOB ?
				AS_PARTICLE_TYPE_BLOB : AS_PARTICLE_TYPE_HLL;
		as_bin_state_set_from_type(&bin_arg.r_bin, p_cdt_mem->type);
		p_cdt_mem->sz = temp.r_bytes.sz;
		memcpy(p_cdt_mem->data, temp.r_bytes.contents, p_cdt_mem->sz);

		old = *b;
		rt_value_destroy(&temp);
		break;
	}
	case RT_MSGPACK: {
		rt_value temp = bin_arg;

		b = &bin_arg.r_bin;
		bin_arg.type = RT_BIN;

		if (! msgpack_to_bin(rt, &bin_arg.r_bin, &temp, NULL)) {
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
			call_cleanup(blob_cleanup, blob_cleanup_ix, bin_cleanup,
					bin_cleanup_ix);
			return;
		}

		old = *b;
		rt_value_destroy(&temp);

		break;
	}
	default:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		call_cleanup(blob_cleanup, blob_cleanup_ix, bin_cleanup,
				bin_cleanup_ix);
		return;
	}

	as_bin rb = { 0 };
	int ret = AS_OK;

	switch (op->system_type & (uint32_t)~CALL_FLAG_MODIFY_LOCAL) {
	case CALL_CDT:
		if (is_modify_local) {
			ret = as_bin_cdt_modify_exp(b, &mv, &rb);
		}
		else {
			ret = as_bin_cdt_read_exp(b, &mv, &rb);
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
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (is_modify_local) {
		if (rt_value_need_destroy(&bin_arg, &old, b)) {
			as_bin_particle_destroy(&old);
		}

		as_bin_particle_destroy(&rb);
	}
	else {
		rt_value_destroy(&bin_arg);
		b = &rb;
	}

	if (! bin_is_type(b, op->type)) {
		if (! is_modify_local || bin_arg.type == RT_BIN ||
				old.particle != b->particle) {
			if (rt_value_need_destroy(&bin_arg, &old, b)) {
				as_bin_particle_destroy(b);
			}
		}

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
		ret_val->do_not_destroy = rt_value_keep_do_not_destroy(&bin_arg, b);
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
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = as_bin_particle_float_value(b);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = as_bin_particle_bool_value(b);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		if (! as_bin_cdt_context_geojson_parse(b)) {
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
			as_bin_particle_destroy(b);
			return;
		}

		// no break
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
		ret_val->type = RT_BIN;
		ret_val->r_bin = *b;
		// Don't suspect this is exploitable, added for future proofing.
		ret_val->do_not_destroy = rt_value_keep_do_not_destroy(&bin_arg, b);
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

static void
eval_value(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)rt;

	switch (ob->code) {
	case VOP_VALUE_NIL:
		ret_val->type = RT_NIL;
		break;
	case VOP_VALUE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = ((op_value_bool*)ob)->value ?
				AS_EXP_TRUE : AS_EXP_FALSE;
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
	case VOP_VALUE_HLL:
		ret_val->type = RT_HLL;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case VOP_VALUE_LIST:
	case VOP_VALUE_MSGPACK:
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
rt_value_bin_ptr_to_bin(runtime* rt, as_bin* rb, const rt_value* from,
		cf_ll_buf* ll_buf)
{
	as_bin b = *from->r_bin_p;
	uint8_t type = as_bin_get_particle_type(&b);

	rb->state = b.state;

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
		rb->particle = b.particle;
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_GEOJSON:
		;
		uint32_t sz = sizeof(cdt_mem) + ((cdt_mem*)b.particle)->sz;

		rb->particle = rt_alloc_mem(rt, sz, ll_buf);
		memcpy(rb->particle, b.particle, sz);
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %u", type);
	}
}

static void
json_to_rt_geo(const uint8_t* json, size_t jsonsz, rt_value* val)
{
	uint64_t cellid;
	geo_region_t region;

	*val = (rt_value){ 0 };

	if (! as_geojson_parse(NULL, (const char*)json, jsonsz, &cellid, &region)) {
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
	case TYPE_TRILEAN:
		expected = AS_PARTICLE_TYPE_BOOL;
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
rt_value_translate(rt_value* to, const rt_value* from)
{
	const as_bin* bin;

	*to = (rt_value){ 0 };

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

	switch (type) {
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_STRING: {
		char* ptr;

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
	case AS_PARTICLE_TYPE_MAP: {
		cdt_payload val;

		as_bin_particle_map_get_packed_val(bin, &val);
		to->type = RT_MSGPACK;
		to->r_bytes.sz = val.sz;
		to->r_bytes.contents = val.ptr;
		break;
	}
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_HLL:
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

static void
rt_value_destroy(rt_value* val)
{
	if (val == NULL) {
		return;
	}

	if (val->do_not_destroy != 0) {
		return;
	}

	if (val->type == RT_GEO_COMPILED &&
			val->r_geo.type == GEO_REGION_NEED_FREE) {
		geo_region_destroy(val->r_geo.region);
	}
	else if (val->type == RT_BIN) {
		as_bin_particle_destroy(&val->r_bin);
	}
}

static void
rt_value_get_geo(rt_value* val, geo_data* result)
{
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

static bool
get_live_bin(as_storage_rd* rd, const uint8_t* name, size_t len, as_bin** p_bin)
{
	// Note - empty bin name ok for now - single-bin "soft landing".
//	if (len == 0) {
//		cf_warning(AS_EXP, "get_live_bin - illegal zero length bin name for multi-bin");
//		return false;
//	}

	*p_bin = as_bin_get_live_w_len(rd, name, len);

	return true;
}


//==========================================================
// Local helpers - runtime compare utilities.
//

static as_exp_trilean
cmp_nil(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	switch (code) {
	case EXP_CMP_EQ:
	case EXP_CMP_GE:
	case EXP_CMP_LE:
		return AS_EXP_TRUE;
	case EXP_CMP_NE:
	case EXP_CMP_GT:
	case EXP_CMP_LT:
		return AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_trilean(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
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

	if (cmp == MSGPACK_CMP_ERROR) {
		return AS_EXP_UNK;
	}

	if (mp0.has_unordered_map || mp1.has_unordered_map) {
		cf_debug(AS_EXP, "illegal comparison of structure containing unordered map - arg0 %s arg1 %s",
				mp0.has_unordered_map ? "has unordered" : "is ok",
				mp1.has_unordered_map ? "has unordered" : "is ok");
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
		*(uint16_t*)ptr = cf_swap_to_be16((uint16_t)sz);
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

	*to = (rt_value){ 0 };

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
		to->type = type; // runtime_type matches particle type for these types
		to->r_bytes.sz = as_bin_particle_string_ptr(&b,
				(char**)&to->r_bytes.contents);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		to->type = type; // runtime_type matches particle type for AS_PARTICLE_TYPE_GEOJSON

		size_t sz;

		to->r_bytes.contents =
				(const uint8_t*)as_geojson_mem_jsonstr(b.particle, &sz);
		to->r_bytes.sz = (uint32_t)sz;
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

static void*
rt_alloc_mem(runtime* rt, size_t sz, cf_ll_buf* ll_buf)
{
	if (ll_buf != NULL) {
		uint8_t *ptr;

		cf_ll_buf_reserve(ll_buf, sz, &ptr);

		return ptr;
	}

	return cf_malloc(sz);
}

static bool
msgpack_to_bin(runtime* rt, as_bin* to, rt_value* from, cf_ll_buf* ll_buf)
{
	msgpack_type type = msgpack_buf_peek_type(from->r_bytes.contents,
			from->r_bytes.sz);
	uint8_t p_type;

	switch (type) {
	case MSGPACK_TYPE_LIST:
		p_type = AS_PARTICLE_TYPE_LIST;
		break;
	case MSGPACK_TYPE_MAP:
		p_type = AS_PARTICLE_TYPE_MAP;
		break;
	case MSGPACK_TYPE_BYTES:
		p_type = AS_PARTICLE_TYPE_BLOB;
		break;
	default:
		return false;
	}

	to->particle = rt_alloc_mem(rt, from->r_bytes.sz + sizeof(cdt_mem), ll_buf);

	cdt_mem* p_cdt_mem = (cdt_mem*)to->particle;

	p_cdt_mem->type = p_type;
	as_bin_state_set_from_type(to, p_cdt_mem->type);
	p_cdt_mem->sz = from->r_bytes.sz;
	memcpy(p_cdt_mem->data, from->r_bytes.contents, p_cdt_mem->sz);

	return true;
}


//==========================================================
// Local helpers - runtime display.
//

static void
rt_display(runtime* rt, cf_dyn_buf* db)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];

	rt->op_ix++;
	rt->instr_ptr += entry->size;
	entry->display_cb(rt, ob, db);
}

static void
display_0_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	cf_dyn_buf_append_format(db, "%s()", op_table[ob->code].name);
}

static void
display_1_arg(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

static void
display_2_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	rt_display(rt, db);
	cf_dyn_buf_append_string(db, ", ");
	rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

static void
display_cmp_regex(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_cmp_regex* op = (op_cmp_regex*)ob;

	cf_dyn_buf_append_format(db, "%s(<string#%u>, %u, ",
			op_table[ob->code].name, op->regex_str_sz, op->flags);
	rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

static void
display_logical_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

static void
display_math_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

static void
display_int_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

static void
display_meta_digest_mod(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_meta_digest_modulo* op = (op_meta_digest_modulo*)ob;

	cf_dyn_buf_append_format(db, "%s(%u)", op_table[ob->code].name, op->mod);
}

static void
display_bin(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_bin* op = (op_bin*)ob;

	cf_dyn_buf_append_format(db, "%s_%s(\"%.*s\")",
			op_table[ob->code].name, result_type_str[op->type], op->name_sz,
			op->name);
}

static void
display_bin_type(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_bin* op = (op_bin*)ob;

	cf_dyn_buf_append_format(db, "%s(\"%.*s\")",
			op_table[ob->code].name, op->name_sz, op->name);
}

static void
display_cond(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_cond* op = (op_cond*)ob;

	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	for (uint32_t i = 0; i < op->case_count; i++) {
		rt_display(rt, db);
		cf_dyn_buf_append_string(db, ", ");
		rt_display(rt, db);
		rt_display(rt, db);
		cf_dyn_buf_append_string(db, ", ");
	}

	rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

static void
display_var(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_var* op = (op_var*)ob;

	cf_dyn_buf_append_format(db, "%s(\"var_%u\")",
			op_table[ob->code].name, op->idx);
}

static void
display_let(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_let* op = (op_let*)ob;

	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	for (uint32_t i = 0; i < op->n_vars; i++) {
		cf_dyn_buf_append_format(db, "def(var_%u, ", op->var_idx + i);
		rt_display(rt, db);
		cf_dyn_buf_append_string(db, "), ");
	}

	rt_display(rt, db);
	cf_dyn_buf_append_char(db, ')');
}

static void
display_call(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_call* op = (op_call*)ob;

	call_system_type system_type = op->system_type &
			(uint32_t)~CALL_FLAG_MODIFY_LOCAL;
	bool is_modify = op->system_type != system_type;
	msgpack_in mp = {
			.buf = op->vecs[0].buf,
			.buf_sz = op->vecs[0].buf_sz
	};
	uint32_t ele_count;

	if (! msgpack_get_list_ele_count(&mp, &ele_count)) {
		cf_crash(AS_EXP, "unexpected");
	}

	int64_t op_code;

	if (! msgpack_get_int64(&mp, &op_code)) {
		cf_crash(AS_EXP, "unexpected");
	}

	switch (system_type) {
	case CALL_CDT:
		if (op_code == AS_CDT_OP_CONTEXT_EVAL) {
			msgpack_in mp_ctx = mp; // save ctx position to print later

			msgpack_sz(&mp); // skip ctx to print call name first

			if (! msgpack_get_list_ele_count(&mp, &ele_count)) {
				cf_crash(AS_EXP, "unexpected");
			}

			if (! msgpack_get_int64(&mp, &op_code)) {
				cf_crash(AS_EXP, "unexpected");
			}

			cf_dyn_buf_append_format(db, "%s(", cdt_exp_display_name(op_code));

			if (! cdt_msgpack_ctx_to_dynbuf(&mp_ctx, db)) {
				cf_crash(AS_EXP, "unexpected");
			}

			cf_dyn_buf_append_string(db, ", ");
		}
		else {
			cf_dyn_buf_append_format(db, "%s(NULL, ",
					cdt_exp_display_name(op_code));
		}

		break;
	case CALL_BITS:
		cf_dyn_buf_append_format(db, "%s(",
				as_bits_op_name((uint32_t)op_code, is_modify));
		break;
	case CALL_HLL:
		cf_dyn_buf_append_format(db, "%s(", as_hll_op_name((uint32_t)op_code,
				is_modify));
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}

	uint32_t idx = 0;

	while (true) {
		while (mp.offset != mp.buf_sz) {
			display_msgpack(&mp, db);
			cf_dyn_buf_append_string(db, ", ");
		}

		idx++;

		if (idx == op->n_vecs) {
			break;
		}

		mp.buf = op->vecs[idx].buf;
		mp.buf_sz = op->vecs[idx].buf_sz;
		mp.offset = 0;

		if (mp.buf == call_eval_token) {
			rt_display(rt, db);
			cf_dyn_buf_append_string(db, ", ");
		}
	}

	rt_display(rt, db);
	cf_dyn_buf_append_char(db, ')');
}

static void
display_value(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	switch (ob->code) {
	case VOP_VALUE_NIL:
		cf_dyn_buf_append_string(db, "nil");
		break;
	case VOP_VALUE_GEO:
		;
		uint32_t sz;
		msgpack_in mp = {
				.buf = ((op_value_geo*)ob)->contents,
				.buf_sz = ((op_value_geo*)ob)->content_sz
		};

		msgpack_get_bin(&mp, &sz);
		cf_dyn_buf_append_format(db, "<geojson#%u>", sz - 1);
		break;
	case VOP_VALUE_LIST: {
		op_value_blob* op_b = (op_value_blob*)ob;
		uint32_t ele_count = UINT32_MAX;

		msgpack_buf_get_list_ele_count(op_b->value, op_b->value_sz, &ele_count);
		cf_dyn_buf_append_format(db, "<list#%u>", ele_count);
		break;
	}
	case VOP_VALUE_MSGPACK: {
		op_value_blob* op_b = (op_value_blob*)ob;
		uint32_t ele_count = UINT32_MAX;

		if (msgpack_buf_get_map_ele_count(op_b->value, op_b->value_sz,
				&ele_count)) {
			cf_dyn_buf_append_format(db, "<map#%u>", ele_count);
		}
		else {
			cf_dyn_buf_append_format(db, "<ext#%u>", op_b->value_sz);
		}

		break;
	}
	case VOP_VALUE_BOOL:
		cf_dyn_buf_append_bool(db, ((op_value_bool*)ob)->value);
		break;
	case VOP_VALUE_INT:
		cf_dyn_buf_append_format(db, "%ld", ((op_value_int*)ob)->value);
		break;
	case VOP_VALUE_FLOAT:
		cf_dyn_buf_append_format(db, "%f", ((op_value_float*)ob)->value);
		break;
	case VOP_VALUE_STR:
		cf_dyn_buf_append_format(db, "<string#%u>",
				((op_value_blob*)ob)->value_sz);
		break;
	case VOP_VALUE_BLOB:
		cf_dyn_buf_append_format(db, "<blob#%u>",
				((op_value_blob*)ob)->value_sz);
		break;
	case VOP_VALUE_HLL:
		cf_dyn_buf_append_format(db, "<hll#%u>",
				((op_value_blob*)ob)->value_sz);
		break;
	default:
		cf_crash(AS_EXP, "unexpected code %u", ob->code);
	}
}

static void
display_case(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;
	(void)ob;
	(void)db;
}

static void
display_msgpack(msgpack_in* mp, cf_dyn_buf* db)
{
	msgpack_type type = msgpack_peek_type(mp);

	switch (type) {
	case MSGPACK_TYPE_NIL:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "nil");
		break;
	case MSGPACK_TYPE_FALSE:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "false");
		break;
	case MSGPACK_TYPE_TRUE:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "true");
		break;
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT: {
		int64_t ctx_int;

		msgpack_get_int64(mp, &ctx_int);
		cf_dyn_buf_append_format(db, "%ld", ctx_int);
		break;
	}
	case MSGPACK_TYPE_DOUBLE: {
		double ctx_double;

		msgpack_get_double(mp, &ctx_double);
		cf_dyn_buf_append_format(db, "%f", ctx_double);
		break;
	}
	case MSGPACK_TYPE_STRING: {
		uint32_t str_sz;

		msgpack_get_bin(mp, &str_sz);
		cf_dyn_buf_append_format(db, "<string#%u>", str_sz - 1);
		break;
	}
	case MSGPACK_TYPE_BYTES: {
		uint32_t blob_sz;
		const uint8_t* blob = msgpack_get_bin(mp, &blob_sz);

		if (blob_sz == 0) {
			cf_dyn_buf_append_format(db, "<blob#_>");
			break;
		}

		if (blob[0] == AS_BYTES_HLL) {
			cf_dyn_buf_append_format(db, "<hll#%u>", blob_sz - 1);
			break;
		}

		cf_dyn_buf_append_format(db, "<blob#%u>", blob_sz - 1);
		break;
	}
	case MSGPACK_TYPE_GEOJSON: {
		uint32_t geo_sz;

		msgpack_get_bin(mp, &geo_sz);
		cf_dyn_buf_append_format(db, "<geojson#%u>", geo_sz - 1);
		break;
	}
	default:
		msgpack_sz(mp);
		cf_dyn_buf_append_format(db, "<msgpack/%u>", type);
		break;
	}
}


//==========================================================
// Local helpers - debug utilites.
//

static void
debug_exp_check(const as_exp* exp)
{
#ifndef exp_check
	(void)exp;
#else
	cf_assert(exp != NULL, AS_EXP, "exp was null");

	if (exp->version != 2) {
		return;
	}

	cf_validate_pointer(exp->buf_cleanup);
	cf_validate_pointer(exp);
#endif
}
