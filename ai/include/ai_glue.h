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

#include "ai_obj.h"
#include "ai_types.h"

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


//==========================================================
// Public API.
//

int32_t comp_digest(const void* key_1, const void* key_2);
int32_t comp_integer(const void* key_1, const void* key_2);

as_btree* createIBT(col_type_t type);
as_btree* createNBT(void);

void btIndAdd(as_btree* ibt, const ai_obj* ikey, const ai_nbtr* nbt);
ai_nbtr* btIndFind(const as_btree* ibt, const ai_obj* ikey);
void btIndDelete(as_btree* ibt, const ai_obj* ikey);

void btIndNodeAdd(as_btree* nbt, const ai_obj* nkey);
bool btIndNodeExist(const as_btree* nbt, const ai_obj* nkey);
void btIndNodeDelete(as_btree* nbt, const ai_obj* nkey);
