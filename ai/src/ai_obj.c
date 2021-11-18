/*
 * ai_obj.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
 *  Aerospike Index Object Implementation.
 */

#include "ai_obj.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "ai_glue.h"

#include "log.h"

void init_ai_obj(ai_obj *a)
{
	memset(a, 0, sizeof(ai_obj));
}

void init_ai_objInteger(ai_obj *a, uint64_t x)
{
	init_ai_obj(a);
	a->integer = x;
}

void init_ai_objDigest(ai_obj *a, const cf_digest *x) {
	a->digest = *x;
}

void ai_objClone(ai_obj *dest, const ai_obj *src)
{
	memcpy(dest, src, sizeof(ai_obj));
}
