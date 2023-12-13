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

#include "dynbuf.h"

#include "base/datamodel.h"
#include "base/index.h"


//==========================================================
// Typedefs & constants.
//

typedef struct as_exp_s {
	uint32_t expected_type;
	void** cleanup_stack;
	uint32_t cleanup_stack_ix;
	uint32_t max_var_count;
	uint8_t mem[];
} as_exp;

typedef struct as_exp_ctx_s {
	as_namespace* ns;
	as_record* r;
	as_storage_rd* rd; // NULL during metadata phase
} as_exp_ctx;

typedef enum {
	AS_EXP_FALSE = 0,
	AS_EXP_TRUE = 1,
	AS_EXP_UNK = 2
} as_exp_trilean;


//==========================================================
// Public API.
//

as_exp* as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz);
as_exp* as_exp_filter_build(const as_msg_field* msg, bool cpy_instr);
as_exp* as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire);
bool as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb, cf_ll_buf* particles_llb);
as_exp_trilean as_exp_matches_metadata(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_display(const as_exp* exp, cf_dyn_buf* db);
void as_exp_destroy(as_exp* exp);
