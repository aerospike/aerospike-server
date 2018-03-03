/*
 * ai_types.h
 *
 * Copyright (C) 2013-2015 Aerospike, Inc.
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

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>

#include <citrusleaf/cf_ll.h>
#include <base/secondary_index.h>

#define uchar    unsigned char
#define ushort16 unsigned short
#define uint32   unsigned int
#define ull      unsigned long long
#define uint128  __uint128_t

#define AS_DIGEST_KEY_SZ 20
typedef struct uint160 {
	char digest[AS_DIGEST_KEY_SZ];
} uint160;

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

#define VOIDINT (void *) (long)

#define SPLICE_160(num)											\
	ull ubh, ubm; uint32 u;										\
	char *pbu = (char *) &num;									\
	memcpy(&ubh, pbu + 12, 8);									\
	memcpy(&ubm, pbu + 4,  8);									\
	memcpy(&u,   pbu,      4);

#define DEBUG_U160(fp, num)										\
	{															\
		SPLICE_160(num);										\
		fprintf(fp, "DEBUG_U160: high: %llu mid: %llu low: %u", ubh, ubm, u); \
	}

/***************** Opaque Forward Type Declarations *****************/

/*
 *  B-Tree Object [Implementation defined in "btreepriv.h".]
 */
typedef struct btree bt;


/***************** Type Declarations *****************/
typedef struct ai_obj {
	ulong    l;
	uint160  y;
	col_type_t type;
} ai_obj;

typedef struct filter {
	ai_obj   alow;
	ai_obj   ahigh;
} f_t;

typedef struct check_sql_where_clause {
	f_t     wf;
} cswc_t;
