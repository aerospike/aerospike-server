/*
 * exp.h
 *
 * Copyright (C) 2016-2026 Aerospike, Inc.
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
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "dynbuf.h"
#include "msgpack_in.h"
#include "vector.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "exp/exp_wire.h"

//==========================================================
// Typedefs & constants.
//

#define AS_EXP_HAS_DIGEST_MOD (1 << 0)
#define AS_EXP_HAS_NON_DIGEST_META (1 << 1)
#define AS_EXP_HAS_REC_KEY (1 << 2)

typedef struct as_exp_s {
	uint8_t expected_type;
	uint8_t flags;
	const void* bin_table;
	void** cleanup_stack;
	uint32_t cleanup_stack_ix;
	uint32_t max_var_count;
	uint8_t mem[];
} as_exp;

struct exp_runtime_s;
struct exp_rt_value_s;
struct exp_op_base_mem_s;
struct exp_op_value_geo_s;
struct exp_build_args_s;

typedef bool (*exp_op_table_build_cb)(struct exp_build_args_s* args);
typedef void (*exp_op_table_eval_cb)(struct exp_runtime_s* rt,
		const struct exp_op_base_mem_s* ob, struct exp_rt_value_s* ret_val);
typedef void (*exp_op_table_display_cb)(struct exp_runtime_s* rt,
		const struct exp_op_base_mem_s* ob, cf_dyn_buf* db);

typedef struct exp_op_table_entry_s {
	exp_op_code code;
	uint32_t size;
	exp_op_table_build_cb build_cb;
	exp_op_table_eval_cb eval_cb;
	exp_op_table_display_cb display_cb;
	uint32_t static_param_count;
	uint32_t eval_param_count;
	exp_rtype r_type;
	const char* name;
} exp_op_table_entry;

typedef struct as_exp_ctx_s {
	as_namespace* ns;
	as_record* r;
	as_storage_rd* rd; // NULL during metadata phase

	msgpack_in** vars_table;
} as_exp_ctx;

typedef enum {
	AS_EXP_FALSE = 0,
	AS_EXP_TRUE = 1,
	AS_EXP_UNK = 2
} __attribute__((__packed__)) as_exp_trilean;

typedef enum {
	AS_EXP_RESULT_MP_SMALL,
	AS_EXP_RESULT_MSGPACK,
	AS_EXP_RESULT_STR,
	AS_EXP_RESULT_BIN,
	AS_EXP_RESULT_REMOVE
} __attribute__((__packed__)) exp_result_type;

typedef struct as_exp_result_s {
	union {
		exp_result_type type; // which variant below is active

		struct { // mp_small_s
			uint16_t pad;
			uint16_t sz;
			uint8_t buf[1 + sizeof(uint64_t)];
		} __attribute__((__packed__)) mp_small;

		struct { // msgpack_s
			uint16_t pad;
			uint16_t has_nonstorage;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__((__packed__)) msgpack;

		struct { // str_s
			uint8_t pad[3];
			uint8_t bytes_type;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__((__packed__)) str;

		struct { // particle_s
			uint64_t pad;
			as_particle* ptr;
		} __attribute__((__packed__)) particle;
	};
} __attribute__((__packed__)) as_exp_result;

extern const exp_op_table_entry exp_op_table[];

// Build/compile helpers defined in exp.c, also called from exp_rt.c (eval).
const char* exp_rtype_to_str(exp_rtype type);

// Parse a GeoJSON literal from an expression's msgpack into a compiled geo op.
// pre:  mp is positioned at the value bin (as_bytes blob: a type byte then
//       GeoJSON text); op is the target node; debug_str labels warnings.
// post: op->contents/content_sz span the raw bin and mp advances past it;
//       op->compiled is a GEO_CELL point or GEO_REGION (region owned by the
//       node). Returns false, with a warning, on a non-bin or bad GeoJSON.
bool exp_geo_mp_to_op(msgpack_in* mp, struct exp_op_value_geo_s* op,
		const char* debug_str);

//==========================================================
// Public API.
//

as_exp* as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz);
as_exp* as_exp_filter_build_ael(const uint8_t* ael, uint32_t ael_sz);
as_exp* as_exp_filter_build(const as_msg_field* msg, bool cpy_instr);
as_exp* as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire,
		cf_vector* bin_names_r);
bool as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb,
		cf_ll_buf* particles_llb);
as_exp_trilean as_exp_matches_metadata(const as_exp* predexp,
		const as_exp_ctx* ctx);
bool as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_display(const as_exp* exp, cf_dyn_buf* db);
void as_exp_destroy(as_exp* exp);

uint32_t as_exp_result_msgpack_sz(const as_exp_result* res);
void as_exp_result_msgpack_write(const as_exp_result* res, uint8_t* wptr);
void as_exp_result_msgpack_pack(const as_exp_result* res, as_packer* pk);
bool as_exp_result_has_nonstorage(const as_exp_result* res);

bool as_exp_eval_to_result(const as_exp* exp, const as_exp_ctx* ctx,
		as_exp_result* res);
void as_exp_result_destroy(as_exp_result* res);

static inline bool
as_exp_result_is_remove(const as_exp_result* res)
{
	return res->type == AS_EXP_RESULT_REMOVE;
}
