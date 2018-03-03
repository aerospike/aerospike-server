/*
 * rec_props.h
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

/*
 * A list of record properties.
 *
 */

#pragma once


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdint.h>


//==========================================================
// Typedefs
//

// Values stored on drive - be careful.
typedef enum {
	CL_REC_PROPS_FIELD_SET_NAME	= 0,
	CL_REC_PROPS_FIELD_UNUSED_1	= 1,
	CL_REC_PROPS_FIELD_KEY		= 2,
	CL_REC_PROPS_FIELD_LAST_PLUS_1
} as_rec_props_field_id;

//------------------------------------------------
// Class Member Data
//
typedef struct as_rec_props_s {
	uint8_t*	p_data;
	uint32_t	size;
} as_rec_props;


//==========================================================
// Public API
//

void as_rec_props_clear(as_rec_props *_this);
int as_rec_props_get_value(const as_rec_props *_this,
		as_rec_props_field_id id, uint32_t *p_value_size, uint8_t **pp_value);
uint32_t as_rec_props_sizeof_field(uint32_t value_size);
void as_rec_props_init(as_rec_props *_this, uint8_t *p_data);
void as_rec_props_init_malloc(as_rec_props *_this, uint32_t malloc_size);
void as_rec_props_add_field(as_rec_props *_this,
		as_rec_props_field_id id, uint32_t value_size, const uint8_t *p_value);
void as_rec_props_add_field_null_terminate(as_rec_props *_this,
		as_rec_props_field_id id, uint32_t value_len, const uint8_t *p_value);

size_t as_rec_props_size_all(const uint8_t *set_name, size_t set_name_len,
		const uint8_t *key, size_t key_size);
void as_rec_props_fill_all(as_rec_props *_this, uint8_t *p_data,
		const uint8_t *set_name, size_t set_name_len, const uint8_t *key,
		size_t key_size);
