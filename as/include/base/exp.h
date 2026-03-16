/*
 * exp.h
 *
 * Copyright (C) 2016-2023 Aerospike, Inc.
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

#include "base/datamodel.h"
#include "base/index.h"


//==========================================================
// Typedefs & constants.
//

#define AS_EXP_HAS_DIGEST_MOD      (1 << 0)
#define AS_EXP_HAS_NON_DIGEST_META (1 << 1)
#define AS_EXP_HAS_REC_KEY         (1 << 2)

typedef struct as_exp_s {
	uint8_t expected_type;
	uint8_t flags;
	void** cleanup_stack;
	uint32_t cleanup_stack_ix;
	uint32_t max_var_count;
	uint8_t mem[];
} as_exp;

typedef enum {
	AS_EXP_BUILTIN_KEY = 0,
	AS_EXP_BUILTIN_VALUE = 1,
	AS_EXP_BUILTIN_INDEX = 2,

	AS_EXP_BUILTIN_COUNT
} as_exp_builtin;

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
} as_exp_trilean;

typedef enum {
	AS_EXP_RESULT_MP_SMALL,
	AS_EXP_RESULT_MSGPACK,
	AS_EXP_RESULT_STR,
	AS_EXP_RESULT_BIN,
	AS_EXP_RESULT_REMOVE
} exp_result_type;

typedef struct as_exp_result_s {
	union {
		uint8_t type;

		struct { // mp_small_s
			uint16_t pad;
			uint16_t sz;
			uint8_t buf[1 + sizeof(uint64_t)];
		} __attribute__ ((__packed__)) mp_small;

		struct { // msgpack_s
			uint16_t pad;
			uint16_t has_nonstorage;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__ ((__packed__)) msgpack;

		struct { // str_s
			uint8_t pad[3];
			uint8_t bytes_type;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__ ((__packed__)) str;

		struct { // particle_s
			uint64_t pad;
			as_particle* ptr;
		} __attribute__ ((__packed__)) particle;
	};
} __attribute__ ((__packed__)) as_exp_result;


//==========================================================
// Public API.
//

as_exp* as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz);
as_exp* as_exp_filter_build(const as_msg_field* msg, bool cpy_instr);
as_exp* as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire, cf_vector* bin_names_r);
bool as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb, cf_ll_buf* particles_llb);
as_exp_trilean as_exp_matches_metadata(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_display(const as_exp* exp, cf_dyn_buf* db);
void as_exp_destroy(as_exp* exp);

uint32_t as_exp_result_msgpack_sz(const as_exp_result* res);
void as_exp_result_msgpack_write(const as_exp_result* res, uint8_t* wptr);
void as_exp_result_msgpack_pack(const as_exp_result* res, as_packer* pk);
bool as_exp_result_has_nonstorage(const as_exp_result* res);
static inline bool
as_exp_result_is_remove(const as_exp_result* res) {
	return res->type == AS_EXP_RESULT_REMOVE;
}

bool as_exp_eval_to_result(const as_exp* exp, const as_exp_ctx* ctx, as_exp_result* res);
void as_exp_result_destroy(as_exp_result* res);
