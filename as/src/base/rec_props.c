/*
 * rec_props.c
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

//==========================================================
// Includes
//

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "base/rec_props.h"


//==========================================================
// Private "Class Members"
//

//------------------------------------------------
// Function Declarations
//

//------------------------------------------------
// Data
//

//------------------------------------------------
// Constants
//


//==========================================================
// Typedefs
//

typedef struct as_rec_prop_field_s {
	as_rec_props_field_id	id;
	uint32_t				value_size;
	uint8_t					value[];
} __attribute__ ((__packed__)) as_rec_prop_field;


//==========================================================
// Public API
//

//------------------------------------------------
// Clear the object.
//
void
as_rec_props_clear(as_rec_props *this)
{
	this->p_data = NULL;
	this->size = 0;
}

//------------------------------------------------
// Parse a specific field.
//
int
as_rec_props_get_value(const as_rec_props *this,
		as_rec_props_field_id id, uint32_t *p_value_size, uint8_t **pp_value)
{
	const uint8_t *p_read = this->p_data;
	const uint8_t *p_end = p_read + this->size - sizeof(as_rec_prop_field);

	while (p_read < p_end) {
		as_rec_prop_field* p_field = (as_rec_prop_field*)p_read;

		if (p_field->id == id) {
			if (p_value_size) {
				*p_value_size = p_field->value_size;
			}

			if (pp_value) {
				*pp_value = p_field->value;
			}

			return 0;
		}

		p_read += sizeof(as_rec_prop_field) + p_field->value_size;
	}

	return -1;
}

//------------------------------------------------
// Get packed size of field, given value size.
//
uint32_t
as_rec_props_sizeof_field(uint32_t value_size)
{
	return sizeof(as_rec_prop_field) + value_size;
}

//------------------------------------------------
// Set p_data member to external buffer. (The size
// member will be used like a write pointer in add
// methods, so it starts at 0 here.)
//
void
as_rec_props_init(as_rec_props *this, uint8_t *p_data)
{
	this->p_data = p_data;
	this->size = 0;
}

//------------------------------------------------
// Allocate memory for data. (The size member will
// be used like a write pointer in add methods, so
// it starts at 0 here.)
//
void
as_rec_props_init_malloc(as_rec_props *this, uint32_t malloc_size)
{
	this->p_data = cf_malloc(malloc_size);
	this->size = 0;
}

//------------------------------------------------
// Append a field, trusting that:
// - this->p_data has been allocated big enough
// - this->size is the size added so far
//
void
as_rec_props_add_field(as_rec_props *this,
		as_rec_props_field_id id, uint32_t value_size, const uint8_t *p_value)
{
	as_rec_prop_field* p_field =
			(as_rec_prop_field*)(this->p_data + this->size);

	p_field->id = id;
	p_field->value_size = value_size;
	memcpy(p_field->value, p_value, value_size);

	this->size += as_rec_props_sizeof_field(value_size);
}

//------------------------------------------------
// Same as as_rec_props_add_field(), but where
// p_value is to be a null-terminated string.
//
void
as_rec_props_add_field_null_terminate(as_rec_props *this,
		as_rec_props_field_id id, uint32_t value_len, const uint8_t *p_value)
{
	as_rec_prop_field* p_field =
			(as_rec_prop_field*)(this->p_data + this->size);

	p_field->id = id;
	p_field->value_size = value_len + 1;
	memcpy(p_field->value, p_value, value_len);
	p_field->value[value_len] = 0;

	this->size += as_rec_props_sizeof_field(p_field->value_size);
}

//------------------------------------------------
// Returns size required for as_rec_props p_data
// buffer for specified fields.
//
size_t
as_rec_props_size_all(const uint8_t *set_name, size_t set_name_len,
		const uint8_t *key, size_t key_size)
{
	size_t rec_props_data_size = 0;

	if (set_name) {
		rec_props_data_size += as_rec_props_sizeof_field(set_name_len + 1);
	}

	if (key) {
		rec_props_data_size += as_rec_props_sizeof_field(key_size);
	}

	return rec_props_data_size;
}

//------------------------------------------------
// Add all specified fields, trusting that:
// - this->p_data has been allocated big enough
//
void
as_rec_props_fill_all(as_rec_props *this, uint8_t *p_data,
		const uint8_t *set_name, size_t set_name_len, const uint8_t *key,
		size_t key_size)
{
	as_rec_props_init(this, p_data);

	if (set_name) {
		as_rec_props_add_field_null_terminate(this, CL_REC_PROPS_FIELD_SET_NAME,
				set_name_len, set_name);
	}

	if (key) {
		as_rec_props_add_field(this, CL_REC_PROPS_FIELD_KEY, key_size, key);
	}
}


//==========================================================
// Private Functions
//
