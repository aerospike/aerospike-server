/*
 * particle_integer.c
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


#include "base/particle_integer.h"

#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_boolean.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"


// Some INTEGER particle interface function declarations are in
// particle_integer.h since INTEGER functions are used by other particles.

// Handle on-device "flat" format.
const uint8_t *integer_skip_flat(const uint8_t *flat, const uint8_t *end);
const uint8_t *integer_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp);
uint32_t integer_flat_size(const as_particle *p);
uint32_t integer_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// INTEGER particle interface - vtable.
//

const as_particle_vtable integer_vtable = {
		integer_destruct,
		integer_size,

		integer_concat_size_from_wire,
		integer_append_from_wire,
		integer_prepend_from_wire,
		integer_incr_from_wire,
		integer_size_from_wire,
		integer_from_wire,
		integer_compare_from_wire,
		integer_wire_size,
		integer_to_wire,

		integer_size_from_asval,
		integer_from_asval,
		integer_to_asval,
		integer_asval_wire_size,
		integer_asval_to_wire,

		integer_size_from_msgpack,
		integer_from_msgpack,

		integer_skip_flat,
		integer_from_flat, // cast copies embedded value out
		integer_from_flat,
		integer_flat_size,
		integer_to_flat
};


//==========================================================
// Typedefs & constants.
//

typedef struct integer_mem_s {
	uint8_t		do_not_use;	// already know it's an int type
	uint64_t	i;
} __attribute__ ((__packed__)) integer_mem;

typedef struct integer_flat_s {
	uint8_t		type;
	uint8_t		size;
	uint8_t		data[];
} __attribute__ ((__packed__)) integer_flat;


//==========================================================
// INTEGER particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
integer_destruct(as_particle *p)
{
	// Nothing to do - integer values live in the as_bin.
}

uint32_t
integer_size(const as_particle *p)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
integer_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for integer/float");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
integer_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to integer/float");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
integer_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to integer/float");
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
integer_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	if (wire_type != AS_PARTICLE_TYPE_INTEGER) {
		cf_warning(AS_PARTICLE, "increment with non integer type %u", wire_type);
		return -AS_ERR_INCOMPATIBLE_TYPE;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	case 16: // memcache increment - it's special
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		// For memcache, decrements floor at 0.
		if ((int64_t)i < 0 && *(uint64_t *)pp + i > *(uint64_t *)pp) {
			*pp = 0;
			return 0;
		}
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_ERR_PARAMETER;
	}

	(*(uint64_t *)pp) += i;

	return 0;
}

int32_t
integer_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

int
integer_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_ERR_PARAMETER;
	}

	*pp = (as_particle *)i;

	return 0;
}

int
integer_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	if (wire_type != AS_PARTICLE_TYPE_INTEGER) {
		return 1;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	default:
		return -AS_ERR_UNKNOWN;
	}

	return (uint64_t)p == i ? 0 : 1;
}

uint32_t
integer_wire_size(const as_particle *p)
{
	return (uint32_t)sizeof(uint64_t);
}

uint32_t
integer_to_wire(const as_particle *p, uint8_t *wire)
{
	*(uint64_t *)wire = cf_swap_to_be64((uint64_t)p);

	return (uint32_t)sizeof(uint64_t);
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
integer_size_from_asval(const as_val *val)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

void
integer_from_asval(const as_val *val, as_particle **pp)
{
	// Unfortunately AS_BOOLEANs (as well as AS_INTEGERs) become INTEGER
	// particles, so we have to check the as_val type here.

	as_val_t vtype = as_val_type(val);
	int64_t i;

	switch (vtype) {
	case AS_INTEGER:
		i = as_integer_get(as_integer_fromval(val));
		break;
	case AS_BOOLEAN:
		i = as_boolean_get(as_boolean_fromval(val)) ? 1 : 0;
		break;
	default:
		cf_crash(AS_PARTICLE, "unexpected as_val_t %d", vtype);
		return;
	}

	*pp = (as_particle *)i;
}

as_val *
integer_to_asval(const as_particle *p)
{
	return (as_val *)as_integer_new((uint64_t)p);
}

uint32_t
integer_asval_wire_size(const as_val *val)
{
	return (uint32_t)sizeof(uint64_t);
}

uint32_t
integer_asval_to_wire(const as_val *val, uint8_t *wire)
{
	// Unfortunately AS_BOOLEANs (as well as AS_INTEGERs) become INTEGER
	// particles, so we have to check the as_val type here.

	as_val_t vtype = as_val_type(val);
	int64_t i;

	switch (vtype) {
	case AS_INTEGER:
		i = as_integer_get(as_integer_fromval(val));
		break;
	case AS_BOOLEAN:
		i = as_boolean_get(as_boolean_fromval(val)) ? 1 : 0;
		break;
	default:
		cf_crash(AS_PARTICLE, "unexpected as_val_t %d", vtype);
		return 0;
	}

	*(uint64_t *)wire = cf_swap_to_be64((uint64_t)i);

	return (uint32_t)sizeof(uint64_t);
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
integer_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

void
integer_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	int64_t i;
	as_unpacker pk = {
			.buffer = packed,
			.offset = 0,
			.length = packed_size
	};

	as_unpack_int64(&pk, &i);

	*pp = (as_particle *)i;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t *
integer_skip_flat(const uint8_t *flat, const uint8_t *end)
{
	if (flat + sizeof(integer_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat integer");
		return NULL;
	}

	integer_flat *p_int_flat = (integer_flat *)flat;

	return flat + sizeof(integer_flat) + p_int_flat->size;
}

const uint8_t *
integer_from_flat(const uint8_t *flat, const uint8_t *end, as_particle **pp)
{
	if (flat + sizeof(integer_flat) > end) {
		cf_warning(AS_PARTICLE, "incomplete flat integer");
		return NULL;
	}

	const integer_flat *p_int_flat = (const integer_flat *)flat;
	// Type is correct, since we got here - no need to check against end.

	flat += sizeof(integer_flat) + p_int_flat->size;

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat integer");
		return NULL;
	}

	uint64_t i;

	switch (p_int_flat->size) {
	case 8:
		i = *(uint64_t *)p_int_flat->data;
		break;
	case 4:
		i = *(uint32_t *)p_int_flat->data;
		break;
	case 2:
		i = *(uint16_t *)p_int_flat->data;
		break;
	case 1:
		i = *(uint8_t *)p_int_flat->data;
		break;
	default:
		cf_warning(AS_PARTICLE, "invalid flat integer size");
		return NULL;
	}

	// Integer values live in an as_bin instead of a pointer. Also, flat
	// integers are host order, so no byte swap.
	*pp = (as_particle *)i;

	return flat;
}

uint32_t
integer_flat_size(const as_particle *p)
{
	uint64_t i = (uint64_t)p;

	if ((i & ~0xFFFFffffL) != 0) {
		return (uint32_t)(sizeof(integer_flat) + 8);
	}

	if ((i & ~0xFFFFL) != 0) {
		return (uint32_t)(sizeof(integer_flat) + 4);
	}

	if ((i & ~0xFFL) != 0) {
		return (uint32_t)(sizeof(integer_flat) + 2);
	}

	return (uint32_t)(sizeof(integer_flat) + 1);
}

uint32_t
integer_to_flat(const as_particle *p, uint8_t *flat)
{
	integer_flat *p_int_flat = (integer_flat *)flat;

	// Already wrote the type.

	uint64_t i = (uint64_t)p;

	if ((i & ~0xFFFFffffL) != 0) {
		p_int_flat->size = 8;
		*(uint64_t *)p_int_flat->data = i;
	}
	else if ((i & ~0xFFFFL) != 0) {
		p_int_flat->size = 4;
		*(uint32_t *)p_int_flat->data = (uint32_t)i;
	}
	else if ((i & ~0xFFL) != 0) {
		p_int_flat->size = 2;
		*(uint16_t *)p_int_flat->data = (uint16_t)i;
	}
	else {
		p_int_flat->size = 1;
		*(uint8_t *)p_int_flat->data = (uint8_t)i;
	}

	return (uint32_t)(sizeof(integer_flat) + p_int_flat->size);
}


//==========================================================
// as_bin particle functions specific to INTEGER.
//

int64_t
as_bin_particle_integer_value(const as_bin *b)
{
	// Caller must ensure this is called only for INTEGER particles.
	return (int64_t)b->particle;
}

void
as_bin_particle_integer_set(as_bin *b, int64_t i)
{
	b->particle = (as_particle *)i;
}
