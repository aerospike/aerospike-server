/*
 * exp.h
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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

#include "base/datamodel.h"
#include "base/index.h"


//==========================================================
// Typedefs & constants.
//

typedef struct as_exp_s {
	uint8_t version;
	void** cleanup_stack;
	uint32_t cleanup_stack_ix;
	uint8_t* buf_cleanup;
	uint8_t mem[];
} as_exp;

typedef struct exp_ctx_s {
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

as_exp* exp_build_base64(const char* buf64, uint32_t buf64_sz);
as_exp* as_exp_build(const as_msg_field* msg, bool cpy_instr);
as_exp_trilean as_exp_matches_metadata(const as_exp* predexp, const as_exp_ctx* ctx);
bool as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx);
void as_exp_destroy(as_exp* exp);

as_exp* predexp_build_old(const as_msg_field* pfp);
as_exp_trilean predexp_matches_metadata_old(const as_exp* predexp, const as_exp_ctx* ctx);
bool predexp_matches_record_old(const as_exp* predexp, const as_exp_ctx* ctx);
void predexp_destroy_old(as_exp* predexp);
