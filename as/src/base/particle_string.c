/*
 * particle_string.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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


#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_string.h"
#include "aerospike/as_val.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"


//==========================================================
// STRING particle interface - function declarations.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle as_val translation.
uint32_t string_size_from_asval(const as_val *val);
void string_from_asval(const as_val *val, as_particle **pp);
as_val *string_to_asval(const as_particle *p);
uint32_t string_asval_wire_size(const as_val *val);
uint32_t string_asval_to_wire(const as_val *val, uint8_t *wire);


//==========================================================
// STRING particle interface - vtable.
//

const as_particle_vtable string_vtable = {
		blob_destruct,
		blob_size,

		blob_concat_size_from_wire,
		blob_append_from_wire,
		blob_prepend_from_wire,
		blob_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		string_size_from_asval,
		string_from_asval,
		string_to_asval,
		string_asval_wire_size,
		string_asval_to_wire,

		blob_size_from_msgpack,
		blob_from_msgpack,

		blob_skip_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// Same as related BLOB struct. TODO - just expose BLOB structs?

typedef struct string_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) string_mem;


//==========================================================
// STRING particle interface - function definitions.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
string_size_from_asval(const as_val *val)
{
	return (uint32_t)(sizeof(string_mem) + as_string_len(as_string_fromval(val)));
}

void
string_from_asval(const as_val *val, as_particle **pp)
{
	string_mem *p_string_mem = (string_mem *)*pp;

	as_string *string = as_string_fromval(val);

	p_string_mem->type = AS_PARTICLE_TYPE_STRING;
	p_string_mem->sz = (uint32_t)as_string_len(string);
	memcpy(p_string_mem->data, as_string_tostring(string), p_string_mem->sz);
}

as_val *
string_to_asval(const as_particle *p)
{
	string_mem *p_string_mem = (string_mem *)p;

	uint8_t *value = cf_malloc(p_string_mem->sz + 1);

	memcpy(value, p_string_mem->data, p_string_mem->sz);
	value[p_string_mem->sz] = 0;

	return (as_val *)as_string_new_wlen((char *)value, p_string_mem->sz, true);
}

uint32_t
string_asval_wire_size(const as_val *val)
{
	return as_string_len(as_string_fromval(val));
}

uint32_t
string_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_string *string = as_string_fromval(val);
	uint32_t size = (uint32_t)as_string_len(string);

	memcpy(wire, as_string_tostring(string), size);

	return size;
}


//==========================================================
// as_bin particle functions specific to STRING.
//

uint32_t
as_bin_particle_string_ptr(const as_bin *b, char **p_value)
{
	// Caller must ensure this is called only for STRING particles.
	string_mem *p_string_mem = (string_mem *)b->particle;

	*p_value = (char *)p_string_mem->data;

	return p_string_mem->sz;
}
