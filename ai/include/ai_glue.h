/*
 * ai_glue.h
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

#include "sindex/btree.h"

#include "citrusleaf/cf_digest.h"


//==========================================================
// Typedefs & constants.
//

typedef struct ai_arr_s {
	uint8_t capacity;
	uint8_t used;
	uint64_t data[];
} __attribute__((__packed__)) ai_arr;

#define AI_ARR_MAX_SIZE 255

typedef struct ai_nbtr_s {
	union {
		ai_arr* arr;
		as_btree* nbtr;
	} u;
	bool is_btree;
} __attribute__((__packed__)) ai_nbtr;

#define L_SIZE sizeof(uint64_t)

typedef union ai_obj_u {
	int64_t integer;
	cf_digest digest;
} ai_obj;

typedef __uint128_t uint128;

#define COL_TYPE_INVALID  0
#define COL_TYPE_LONG     1
#define COL_TYPE_DIGEST   2
#define COL_TYPE_GEOJSON  3
#define COL_TYPE_MAX      4


//==========================================================
// Public API.
//

#define C_IS_LONG(ctype)    (ctype == COL_TYPE_LONG)
#define C_IS_DIGEST(ctype)  (ctype == COL_TYPE_DIGEST)
#define C_IS_GEOJSON(ctype) (ctype == COL_TYPE_GEOJSON)
