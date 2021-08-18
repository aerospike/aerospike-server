/*
 * ai_obj.h
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
 *  Aerospike Index Object Declarations.
 */

#pragma once

#include <sys/types.h>

#include "ai_types.h"

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


void init_ai_obj(ai_obj *a);

void init_ai_objLong(ai_obj *a, ulong l);

void init_ai_objU160(ai_obj *a, uint160 y);

void ai_objClone(ai_obj *dest, ai_obj *src);

bool ai_objEQ(ai_obj *a, ai_obj *b);
