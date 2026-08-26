/*
 * exp_ael.h
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

#include <stdint.h>

#include "exp/exp.h"

// AEL source -> as_exp, without the msgpack round trip the wire path takes.
// The only entry point into exp_ael.c; everything else there is static.
//
// pre:  ael_str/ael_sz is the AEL source. bins_info_r is NULL, or an
//       as_bin_info vector to fill for the sindex/masking consumers.
// post: returns a built as_exp, or NULL with the failure recorded via
//       ael_build_err_record() for as_exp_take_build_error() to collect.
as_exp* exp_build_internal_ael(const uint8_t* ael_str, uint32_t ael_sz,
		cf_vector* bins_info_r);
