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

#include <stdint.h>

#include "ai_types.h"

#include <citrusleaf/cf_digest.h>

typedef union ai_obj_u {
	uint64_t integer;
	cf_digest digest;
} ai_obj;

void init_ai_obj(ai_obj *a);
void init_ai_objInteger(ai_obj *a, uint64_t x);
void init_ai_objDigest(ai_obj *a, const cf_digest *x);
void ai_objClone(ai_obj *dest, const ai_obj *src);
