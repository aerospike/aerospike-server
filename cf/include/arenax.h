/*
 * arenax.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>


//==========================================================
// Typedefs & constants.
//

#define CF_ARENAX_BIGLOCK	(1 << 0)
#define CF_ARENAX_CALLOC	(1 << 1)

#ifndef CF_ARENAX_MAX_STAGES
#define CF_ARENAX_MAX_STAGES 256
#endif

typedef uint64_t cf_arenax_handle;

// Must be in-sync with internal array ARENAX_ERR_STRINGS[]:
typedef enum {
	CF_ARENAX_OK = 0,
	CF_ARENAX_ERR_BAD_PARAM,
	CF_ARENAX_ERR_STAGE_CREATE,
	CF_ARENAX_ERR_STAGE_ATTACH,
	CF_ARENAX_ERR_STAGE_DETACH,
	CF_ARENAX_ERR_UNKNOWN
} cf_arenax_err;

//------------------------------------------------
// For enterprise separation only.
//

// Element is indexed by 24 bits.
#define ELEMENT_ID_NUM_BITS 24
#define ELEMENT_ID_MASK ((1UL << ELEMENT_ID_NUM_BITS) - 1) // 0xFFffff

#define MAX_STAGE_CAPACITY (1 << ELEMENT_ID_NUM_BITS) // 16 M

// DO NOT access this member data directly - use the API!
typedef struct cf_arenax_s {
	// Configuration (passed in constructors).
	key_t				key_base;
	uint32_t			element_size;
	uint32_t			stage_capacity;
	uint32_t			max_stages;
	uint32_t			flags;

	// Configuration (derived).
	size_t				stage_size;

	// Free-element list.
	cf_arenax_handle	free_h;

	// Where to end-allocate.
	uint32_t			at_stage_id;
	uint32_t			at_element_id;

	// Thread safety.
	pthread_mutex_t		lock;

	// Current stages.
	uint32_t			stage_count;
	uint8_t*			stages[CF_ARENAX_MAX_STAGES];
} cf_arenax;

typedef struct free_element_s {
	uint32_t			magic;
	cf_arenax_handle	next_h;
} free_element;

#define FREE_MAGIC 0xff1234ff


//==========================================================
// Public API.
//

size_t cf_arenax_sizeof();
const char* cf_arenax_errstr(cf_arenax_err err);

void cf_arenax_init(cf_arenax* arena, key_t key_base, uint32_t element_size,
		uint32_t stage_capacity, uint32_t max_stages, uint32_t flags);

cf_arenax_handle cf_arenax_alloc(cf_arenax* arena);
void cf_arenax_free(cf_arenax* arena, cf_arenax_handle h);

void* cf_arenax_resolve(cf_arenax* arena, cf_arenax_handle h);


//==========================================================
// Private API - for enterprise separation only.
//

static inline void
cf_arenax_set_handle(cf_arenax_handle* h, uint32_t stage_id,
		uint32_t element_id)
{
	*h = ((uint64_t)stage_id << ELEMENT_ID_NUM_BITS) | element_id;
}

cf_arenax_err cf_arenax_add_stage(cf_arenax* arena);
