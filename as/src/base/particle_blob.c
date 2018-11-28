/*
 * particle_blob.c
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


#include "base/particle_blob.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_bytes.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"


// BLOB particle interface function declarations are in particle_blob.h since
// BLOB functions are used by other particles derived from BLOB.


//==========================================================
// BLOB particle interface - vtable.
//

const as_particle_vtable blob_vtable = {
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

		blob_size_from_asval,
		blob_from_asval,
		blob_to_asval,
		blob_asval_wire_size,
		blob_asval_to_wire,

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

typedef struct blob_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) blob_mem;

typedef struct blob_flat_s {
	uint8_t		type;
	uint32_t	size; // host order on device
	uint8_t		data[];
} __attribute__ ((__packed__)) blob_flat;


//==========================================================
// Forward declarations.
//

static inline as_particle_type blob_bytes_type_to_particle_type(as_bytes_type type);


//==========================================================
// BLOB particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
blob_destruct(as_particle *p)
{
	cf_free(p);
}

uint32_t
blob_size(const as_particle *p)
{
	return (uint32_t)(sizeof(blob_mem) + ((blob_mem *)p)->sz);
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
blob_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch concat sizing blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	return (int32_t)(sizeof(blob_mem) + p_blob_mem->sz + value_size);
}

int
blob_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch appending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	memcpy(p_blob_mem->data + p_blob_mem->sz, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch prepending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	memmove(p_blob_mem->data + value_size, p_blob_mem->data, p_blob_mem->sz);
	memcpy(p_blob_mem->data, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected increment of blob/string");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int32_t
blob_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// Wire value is same as in-memory value.
	return (int32_t)(sizeof(blob_mem) + value_size);
}

int
blob_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	p_blob_mem->type = wire_type;
	p_blob_mem->sz = value_size;
	memcpy(p_blob_mem->data, wire_value, p_blob_mem->sz);

	return 0;
}

int
blob_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	return (wire_type == p_blob_mem->type &&
			value_size == p_blob_mem->sz &&
			memcmp(wire_value, p_blob_mem->data, value_size) == 0) ? 0 : 1;
}

uint32_t
blob_wire_size(const as_particle *p)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	return p_blob_mem->sz;
}

uint32_t
blob_to_wire(const as_particle *p, uint8_t *wire)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	memcpy(wire, p_blob_mem->data, p_blob_mem->sz);

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
blob_size_from_asval(const as_val *val)
{
	return (uint32_t)sizeof(blob_mem) + as_bytes_size(as_bytes_fromval(val));
}

void
blob_from_asval(const as_val *val, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	as_bytes *bytes = as_bytes_fromval(val);

	p_blob_mem->type = (uint8_t)blob_bytes_type_to_particle_type(bytes->type);
	p_blob_mem->sz = as_bytes_size(bytes);
	memcpy(p_blob_mem->data, as_bytes_get(bytes), p_blob_mem->sz);
}

as_val *
blob_to_asval(const as_particle *p)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	uint8_t *value = cf_malloc(p_blob_mem->sz);

	memcpy(value, p_blob_mem->data, p_blob_mem->sz);

	return (as_val *)as_bytes_new_wrap(value, p_blob_mem->sz, true);
}

uint32_t
blob_asval_wire_size(const as_val *val)
{
	return as_bytes_size(as_bytes_fromval(val));
}

uint32_t
blob_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_bytes *bytes = as_bytes_fromval(val);
	uint32_t size = as_bytes_size(bytes);

	memcpy(wire, as_bytes_get(bytes), size);

	return size;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
blob_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	// Ok to oversize by a few bytes - only used for allocation sizing.
	// -1 for blob internal type and -1 for blob header.
	return (uint32_t)sizeof(blob_mem) + packed_size - 2;
}

void
blob_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	as_unpacker pk = {
			.buffer = packed,
			.offset = 0,
			.length = packed_size
	};

	int64_t blob_size = as_unpack_blob_size(&pk);
	const uint8_t *ptr = pk.buffer + pk.offset;

	uint8_t type = *ptr;

	// Adjust for type (1 byte).
	ptr++;
	blob_size--;

	blob_mem *p_blob_mem = (blob_mem *)*pp;

	p_blob_mem->type = (uint8_t)blob_bytes_type_to_particle_type((as_bytes_type)type);
	p_blob_mem->sz = blob_size;
	memcpy(p_blob_mem->data, ptr, p_blob_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t *
blob_skip_flat(const uint8_t *flat, const uint8_t *end)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	blob_flat *p_blob_flat = (blob_flat *)flat;

	return flat + sizeof(blob_flat) + p_blob_flat->size;
}

const uint8_t *
blob_cast_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	const blob_flat *p_blob_flat = (const blob_flat *)flat;

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle *)p_blob_flat;

	return flat + sizeof(blob_flat) + p_blob_flat->size;
}

const uint8_t *
blob_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp)
{
	if (flat + sizeof(blob_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	const blob_flat *p_blob_flat = (const blob_flat *)flat;

	// Flat value is same as in-memory value.
	size_t mem_size = sizeof(blob_mem) + p_blob_flat->size;

	flat += mem_size; // blob_mem same size as blob_flat

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat blob/string");
		return NULL;
	}

	blob_mem *p_blob_mem = (blob_mem *)cf_malloc_ns(mem_size);

	p_blob_mem->type = p_blob_flat->type;
	p_blob_mem->sz = p_blob_flat->size;
	memcpy(p_blob_mem->data, p_blob_flat->data, p_blob_mem->sz);

	*pp = (as_particle *)p_blob_mem;

	return flat;
}

uint32_t
blob_flat_size(const as_particle *p)
{
	return (uint32_t)(sizeof(blob_flat) + ((blob_mem *)p)->sz);
}

uint32_t
blob_to_flat(const as_particle *p, uint8_t *flat)
{
	blob_mem *p_blob_mem = (blob_mem *)p;
	blob_flat *p_blob_flat = (blob_flat *)flat;

	// Already wrote the type.
	p_blob_flat->size = p_blob_mem->sz;
	memcpy(p_blob_flat->data, p_blob_mem->data, p_blob_flat->size);

	return blob_flat_size(p);
}


//==========================================================
// Local helpers.
//

static inline as_particle_type
blob_bytes_type_to_particle_type(as_bytes_type type)
{
	switch (type) {
	case AS_BYTES_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case AS_BYTES_BLOB:
		return AS_PARTICLE_TYPE_BLOB;
	case AS_BYTES_JAVA:
		return AS_PARTICLE_TYPE_JAVA_BLOB;
	case AS_BYTES_CSHARP:
		return AS_PARTICLE_TYPE_CSHARP_BLOB;
	case AS_BYTES_PYTHON:
		return AS_PARTICLE_TYPE_PYTHON_BLOB;
	case AS_BYTES_RUBY:
		return AS_PARTICLE_TYPE_RUBY_BLOB;
	case AS_BYTES_PHP:
		return AS_PARTICLE_TYPE_PHP_BLOB;
	case AS_BYTES_ERLANG:
		return AS_PARTICLE_TYPE_ERLANG_BLOB;
	case AS_BYTES_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case AS_BYTES_INTEGER:
	case AS_BYTES_DOUBLE:
	case AS_BYTES_MAP:
	case AS_BYTES_LIST:
	case AS_BYTES_UNDEF:
	default:
		break;
	}

	// Invalid blob types remain as blobs.
	return AS_PARTICLE_TYPE_BLOB;
}
