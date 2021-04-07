/*
 * expop.h
 *
 * Copyright (C) 2021 Aerospike, Inc.
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


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_exp_s;
struct as_exp_ctx_s;
struct as_msg_op_s;
struct iops_expop_s;


//==========================================================
// Public API.
//

bool as_exp_op_parse(const struct as_msg_op_s* msg_op, struct as_exp_s** exp, uint64_t* flags, bool is_modify, bool cpy_wire);
int as_exp_modify_tr(const struct as_exp_ctx_s* ctx, struct as_bin_s* b, const struct as_msg_op_s* msg_op, cf_ll_buf* particles_llb, const struct iops_expop_s* expop);
int as_exp_read_tr(const struct as_exp_ctx_s* ctx, const struct as_msg_op_s* msg_op, struct as_bin_s* rb);
