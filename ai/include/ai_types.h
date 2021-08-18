/*
 * ai_types.h
 *
 * Copyright (C) 2013-2021 Aerospike, Inc.
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
/*
 *  SYNOPSIS
 *    This file provides common declarations and definitions for
 *    the Aerospike Index module.
 */

#pragma once

#include <inttypes.h>

#include <citrusleaf/cf_digest.h>

typedef unsigned char uchar;
typedef unsigned short ushort16;
typedef unsigned int uint32;
typedef unsigned long long ull;
typedef __uint128_t uint128;

typedef cf_digest uint160;

// Same as as_sindex_ktype
typedef uint8_t col_type_t;
#define COL_TYPE_INVALID  0
#define COL_TYPE_LONG     1
#define COL_TYPE_DIGEST   2
#define COL_TYPE_GEOJSON  3
#define COL_TYPE_MAX      4

#define C_IS_L(ctype)    (ctype == COL_TYPE_LONG)
#define C_IS_DG(ctype)   (ctype == COL_TYPE_DIGEST)
#define C_IS_G(ctype)    (ctype == COL_TYPE_GEOJSON)
// TODO - should this have C_IS_G as well
#define C_IS_NUM(ctype)  (C_IS_L(ctype))

/***************** Opaque Forward Type Declarations *****************/

/*
 *  B-Tree Object [Implementation defined in "btreepriv.h".]
 */
typedef struct btree bt;
