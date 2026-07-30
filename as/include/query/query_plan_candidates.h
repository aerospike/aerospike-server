/*
 * query_plan_candidates.h
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
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "vector.h"

#include "base/datamodel.h"
#include "sindex/sindex_manager.h"

//==========================================================
// Forward declarations.
//

struct as_exp_s;

//==========================================================
// Typedefs & constants.
//

// Matches MAX_STRING_KSIZE / MAX_BLOB_KSIZE in sindex.h.
#define AS_EXP_SINDEX_BOUND_VAL_MAX 2048

typedef struct as_exp_sindex_candidate_s {
	char bin_name[AS_BIN_NAME_MAX_SZ];
	uint32_t bin_name_sz;
	as_particle_type ktype;
	as_sindex_type itype;
	bool is_range;
	int64_t bval_low; // inclusive lower bound (hash for string/blob)
	int64_t bval_high; // inclusive upper bound (hash for string/blob)
	uint32_t bound_val_sz; // 0 for integer candidates
	uint8_t bound_val[AS_EXP_SINDEX_BOUND_VAL_MAX]; // original bytes for wire replay
} as_exp_sindex_candidate;

typedef enum as_exp_sindex_extract_e {
	AS_EXP_SINDEX_EXTRACT_CANDIDATES, // walk done - candidates ready for selection
	AS_EXP_SINDEX_EXTRACT_PI, // primary-index scan
	AS_EXP_SINDEX_EXTRACT_FILTERED_OUT, // filter unsatisfiable - zero rows
	AS_EXP_SINDEX_EXTRACT_ERROR, // malformed expression
} as_exp_sindex_extract_result;

//==========================================================
// Public API.
//

as_exp_sindex_extract_result as_exp_get_sindex_candidates(const struct as_exp_s* exp,
		cf_vector* candidates);
