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

// Matches MAX_STRING_KSIZE / MAX_BLOB_KSIZE
#define AS_EXP_SINDEX_BOUND_VAL_MAX 2048

// Matches MAX_GEOJSON_KSIZE
#define AS_EXP_SINDEX_GEO_BOUND_VAL_MAX (1024 * 1024)

// Matches CTX_B64_MAX_SZ of base64, 4:3
#define AS_EXP_SINDEX_CTX_MAX 1536

typedef struct as_exp_sindex_candidate_s {
	char bin_name[AS_BIN_NAME_MAX_SZ];
	uint32_t bin_name_sz;
	as_particle_type ktype;
	as_sindex_type itype;
	bool is_range;
	int64_t bval_low;
	int64_t bval_high;
	uint32_t bound_val_sz;
	uint8_t bound_val[AS_EXP_SINDEX_BOUND_VAL_MAX]; // STRING/BLOB bytes

	// GEO bytes (borrowed into filter's as_exp->mem), NULL for non-GEO.
	const uint8_t* geo_bound_val;

	uint32_t ctx_buf_sz; // 0 for top-level (no-ctx) candidates
	uint8_t ctx_buf[AS_EXP_SINDEX_CTX_MAX]; // packed ctx, byte-comparable with si->ctx_buf

	// Set for exp=... candidates, matched via subtree_equals_exp (sindex.c).
	// Pointers below are borrowed into the filter's as_exp->mem - never
	// persist past this call.
	bool is_exp;
	const uint8_t* exp_subtree_ptr;
	uint32_t exp_subtree_start_ix;
	uint32_t exp_subtree_end_ix;
	const struct as_exp_s* owner_exp; // back-reference - resolves bin-name table during matching
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
