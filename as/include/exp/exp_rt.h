/*
 * exp_rt.h
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

//==========================================================
// Expression runtime types shared by exp.c (build + op_table) and exp_rt.c
// (eval). These are the in-memory layout of the compiled op nodes plus the
// eval-time scratch types. exp.c writes op nodes during the build/AEL passes;
// exp_rt.c reads them during eval -- so the definitions must be visible to
// both TUs, hence this header.
//
// Naming: types carry an `exp_` prefix here. Each .c keeps its bodies
// churn-free with a local short alias, e.g.
//   typedef struct exp_op_base_mem_s op_base_mem;
//

#include <regex.h>
#include <stdbool.h>
#include <stdint.h>

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"
#include "exp/exp.h"
#include "geospatial/geospatial.h"

//==========================================================
// Enums.
//

typedef enum {
	GEO_CELL,
	GEO_REGION,
	GEO_REGION_NEED_FREE
} __attribute__((packed)) exp_geo_type;

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

	RT_RESULT_REMOVE,
	RT_END
} __attribute__((packed)) exp_rt_type;

//==========================================================
// Op-node memory layouts. Every real op node begins with exp_op_base_mem.
// (exp_op_base_mem is forward-declared in exp.h for the callback typedefs;
// its body is completed here.)
//

typedef struct exp_op_base_mem_s {
	uint32_t instr_end_ix;
	exp_op_code code;
	// Tag slots hoisted into this struct's padding so the ops that carry them
	// don't grow their own node. `type` = runtime type (e.g. rec_key); `rtype`
	// = wire result type (e.g. compare / var / let). Zero (calloc) for ops that
	// use neither.
	exp_rt_type type;
	exp_rtype rtype;
} exp_op_base_mem;

typedef struct exp_op_cmp_regex_s {
	exp_op_base_mem base;
	regex_t regex;
	uint32_t regex_str_sz;
	int32_t flags;
} exp_op_cmp_regex;

typedef struct exp_op_meta_digest_modulo_s {
	exp_op_base_mem base;
	int32_t mod;
} exp_op_meta_digest_modulo;

// rec-key and comparison ops carry no fields beyond the base tag slots, so
// they use exp_op_base_mem directly (base.type / base.rtype) -- no node type.

typedef struct exp_op_cond_s {
	exp_op_base_mem base;
	uint32_t case_count;
} exp_op_cond;

typedef struct exp_op_var_s {
	exp_op_base_mem base;
	uint32_t idx;
} exp_op_var;

typedef struct exp_op_let_s {
	exp_op_base_mem base;
	uint32_t var_idx;
	uint32_t n_vars;
} exp_op_let;

#define EXP_CALL_MAX_VEC_IDX 18

typedef struct exp_op_vec_s {
	const uint8_t* buf;
	uint32_t buf_sz;
} exp_op_vec;

typedef struct exp_op_call_s {
	exp_op_base_mem base;
	exp_op_vec vecs[EXP_CALL_MAX_VEC_IDX + 2];
	uint32_t n_vecs;
	uint32_t eval_count;
	exp_call_stype system_type;
	exp_rtype type;
} exp_op_call;

typedef struct exp_geo_compiled_s {
	union {
		uint64_t cellid;
		geo_region_t region;
	};

	exp_geo_type type;
} exp_geo_compiled;

typedef struct exp_op_value_geo_s {
	exp_op_base_mem base;
	exp_geo_compiled compiled;
	const uint8_t* contents;
	uint32_t content_sz;
} exp_op_value_geo;

typedef struct exp_op_value_bool_s {
	exp_op_base_mem base;
	bool value;
} exp_op_value_bool;

typedef struct exp_op_value_blob_s {
	exp_op_base_mem base;
	const uint8_t* value;
	uint32_t value_sz;
} exp_op_value_blob;

typedef struct exp_op_value_int_s {
	exp_op_base_mem base;
	int64_t value;
} exp_op_value_int;

typedef struct exp_op_value_float_s {
	exp_op_base_mem base;
	double value;
} exp_op_value_float;

//==========================================================
// Eval-time scratch types. (exp_rt_value / exp_runtime are forward-declared in
// exp.h; bodies completed here.)
//

typedef struct exp_rt_value_s {
	exp_rt_type type;
	uint8_t do_not_destroy;
	uint16_t pad1;

	union {
		as_bin r_bin;

		struct r_bytes_s {
			uint32_t sz;
			const uint8_t* contents;
		} r_bytes;

		struct r_geo_const_s {
			exp_op_value_geo* op;
		} r_geo_const;

		exp_geo_compiled r_geo;

		struct {
			uint32_t pad2;

			union {
				as_exp_trilean r_trilean;
				int64_t r_int;
				double r_float;
				exp_op_base_mem* r_const_p;
				as_bin* r_bin_p;
				char* r_cstr_p;
			};
		};
	};
} __attribute__((__packed__)) exp_rt_value;

// A distinct bin referenced by an expression. Bins are deduped into a table
// (see build_bin_table / exp_rt_bin_table); the name bytes live at base + off.
typedef struct exp_bin_name_entry_s {
	uint32_t off : 27; // 27 bits spans PROTO_SIZE_MAX (128 MiB)
	uint8_t sz : 4; // bin name length 0..AS_BIN_NAME_MAX_SZ-1
	bool need_memcpy : 1;
} __attribute__((__packed__)) exp_bin_name_entry;

COMPILER_ASSERT(sizeof(exp_bin_name_entry) == 4);
// off must span the wire limit; widen it if PROTO_SIZE_MAX grows.
COMPILER_ASSERT(PROTO_SIZE_MAX <= (1u << 27));
COMPILER_ASSERT(AS_BIN_NAME_MAX_SZ <= (1u << 4));

// Runtime bin table: the distinct bins an expression reads, laid out in the
// compiled buffer. table[idx] locates name bytes at base + off; idx also
// indexes rt->vars[] (bins occupy the first n_bins var slots).
typedef struct exp_rt_bin_table_s {
	const uint8_t* base;
	uint32_t n_bins;
	exp_bin_name_entry table[];
} exp_rt_bin_table;

typedef struct exp_runtime_s {
	const as_exp_ctx* ctx;
	const uint8_t* instr_ptr;
	exp_rt_value* vars;
	const exp_rt_bin_table* bin_table;
	exp_rt_value vars_builtin[AS_EXP_BUILTIN_COUNT];
	uint32_t op_ix;
} exp_runtime;

//==========================================================
// Eval / display callbacks (defined in exp_rt.c) -- op_table in exp.c wires
// these, and each family cross-references within exp_rt.c. Uniform signatures:
//   eval_cb:    void (exp_runtime*, const exp_op_base_mem*, exp_rt_value*)
//   display_cb: void (exp_runtime*, const exp_op_base_mem*, cf_dyn_buf*)
//

#define EXP_EVAL_DECL(_name)                                                   \
	void exp_eval_##_name(exp_runtime* rt, const exp_op_base_mem* ob,          \
			exp_rt_value* v)

EXP_EVAL_DECL(unknown);
EXP_EVAL_DECL(compare);
EXP_EVAL_DECL(cmp_regex);
EXP_EVAL_DECL(in_list);
EXP_EVAL_DECL(and);
EXP_EVAL_DECL(or);
EXP_EVAL_DECL(not );
EXP_EVAL_DECL(exclusive);
EXP_EVAL_DECL(add);
EXP_EVAL_DECL(sub);
EXP_EVAL_DECL(mul);
EXP_EVAL_DECL(div);
EXP_EVAL_DECL(pow);
EXP_EVAL_DECL(log);
EXP_EVAL_DECL(mod);
EXP_EVAL_DECL(floor);
EXP_EVAL_DECL(abs);
EXP_EVAL_DECL(ceil);
EXP_EVAL_DECL(to_int);
EXP_EVAL_DECL(to_float);
EXP_EVAL_DECL(to_string);
EXP_EVAL_DECL(int_and);
EXP_EVAL_DECL(int_or);
EXP_EVAL_DECL(int_xor);
EXP_EVAL_DECL(int_not);
EXP_EVAL_DECL(int_lshift);
EXP_EVAL_DECL(int_rshift);
EXP_EVAL_DECL(int_arshift);
EXP_EVAL_DECL(int_count);
EXP_EVAL_DECL(int_lscan);
EXP_EVAL_DECL(int_rscan);
EXP_EVAL_DECL(min);
EXP_EVAL_DECL(max);
EXP_EVAL_DECL(meta_digest_mod);
EXP_EVAL_DECL(meta_device_size);
EXP_EVAL_DECL(meta_last_update);
EXP_EVAL_DECL(meta_since_update);
EXP_EVAL_DECL(meta_void_time);
EXP_EVAL_DECL(meta_ttl);
EXP_EVAL_DECL(meta_set_name);
EXP_EVAL_DECL(meta_key_exists);
EXP_EVAL_DECL(meta_is_tombstone);
EXP_EVAL_DECL(meta_memory_size);
EXP_EVAL_DECL(meta_record_size);
EXP_EVAL_DECL(rec_key);
EXP_EVAL_DECL(bin);
EXP_EVAL_DECL(bin_type);
EXP_EVAL_DECL(bin_exists);
EXP_EVAL_DECL(result_remove);
EXP_EVAL_DECL(map_keys);
EXP_EVAL_DECL(map_values);
EXP_EVAL_DECL(cond);
EXP_EVAL_DECL(var_builtin);
EXP_EVAL_DECL(var);
EXP_EVAL_DECL(let);
EXP_EVAL_DECL(call);
EXP_EVAL_DECL(value);

#define EXP_DISPLAY_DECL(_name)                                                \
	void exp_display_##_name(exp_runtime* rt, const exp_op_base_mem* ob,       \
			cf_dyn_buf* db)

EXP_DISPLAY_DECL(0_args);
EXP_DISPLAY_DECL(1_arg);
EXP_DISPLAY_DECL(2_args);
EXP_DISPLAY_DECL(cmp_regex);
EXP_DISPLAY_DECL(logical_vargs);
EXP_DISPLAY_DECL(math_vargs);
EXP_DISPLAY_DECL(int_vargs);
EXP_DISPLAY_DECL(meta_digest_mod);
EXP_DISPLAY_DECL(bin);
EXP_DISPLAY_DECL(bin_type);
EXP_DISPLAY_DECL(bin_exists);
EXP_DISPLAY_DECL(cond);
EXP_DISPLAY_DECL(var_builtin);
EXP_DISPLAY_DECL(var);
EXP_DISPLAY_DECL(let);
EXP_DISPLAY_DECL(call);
EXP_DISPLAY_DECL(value);
EXP_DISPLAY_DECL(case);

// Debug display driver -- built in exp_rt.c, also invoked by exp.c's
// build_internal_ael CF_DETAIL block.
void exp_rt_display(exp_runtime* rt, cf_dyn_buf* db);

//==========================================================
// Symbols defined in exp_rt.c that the build path (exp.c) also uses. (exp.c's
// own shared helpers -- exp_rtype_to_str, exp_geo_mp_to_op -- are declared in
// exp.h alongside the other exp.c exports.)
//

// Sentinel marking a call vec slot that eval fills from a sub-expression.
// One shared symbol so build (exp.c) sets and eval compares the SAME address.
extern const uint8_t exp_call_eval_token[1];

as_particle_type exp_rtype_to_particle_type(exp_rtype type);
