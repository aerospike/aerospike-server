/*
 * particle_float.c
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

#include "aerospike/as_double.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_integer.h"
#include "base/proto.h"


//==========================================================
// FLOAT particle interface - function declarations.
//

// Most FLOAT particle table functions just use the equivalent INTEGER particle
// functions. Here are the differences...

// Handle "wire" format.
int float_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int float_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int float_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);

// Handle as_val translation.
void float_from_asval(const as_val *val, as_particle **pp);
as_val *float_to_asval(const as_particle *p);
uint32_t float_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
void float_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);


//==========================================================
// FLOAT particle interface - vtable.
//

const as_particle_vtable float_vtable = {
		integer_destruct,
		integer_size,

		integer_concat_size_from_wire,
		integer_append_from_wire,
		integer_prepend_from_wire,
		float_incr_from_wire,
		integer_size_from_wire,
		float_from_wire,
		float_compare_from_wire,
		integer_wire_size,
		integer_to_wire,

		integer_size_from_asval,
		float_from_asval,
		float_to_asval,
		integer_asval_wire_size,
		float_asval_to_wire,

		integer_size_from_msgpack,
		float_from_msgpack,

		integer_size_from_flat,
		integer_cast_from_flat,
		integer_from_flat,
		integer_flat_size,
		integer_to_flat
};


//==========================================================
// FLOAT particle interface - function definitions.
//

// Most FLOAT particle table functions just use the equivalent INTEGER particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

int
float_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// For now we won't allow adding integers (or anything else) to floats.
	if (wire_type != AS_PARTICLE_TYPE_FLOAT) {
		cf_warning(AS_PARTICLE, "increment with non float type %u", wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	(*(double *)pp) += *(double *)&i;

	return 0;
}

int
float_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	if (value_size != 8) {
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return integer_from_wire(wire_type, wire_value, value_size, pp);
}

int
float_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	if (wire_type != AS_PARTICLE_TYPE_FLOAT) {
		return 1;
	}

	if (value_size != 8) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	return integer_compare_from_wire(p, AS_PARTICLE_TYPE_INTEGER, wire_value, value_size);
}

//------------------------------------------------
// Handle as_val translation.
//

void
float_from_asval(const as_val *val, as_particle **pp)
{
	*(double *)pp = as_double_get(as_double_fromval(val));
}

as_val *
float_to_asval(const as_particle *p)
{
	return (as_val *)as_double_new(*(double *)&p);
}

uint32_t
float_asval_to_wire(const as_val *val, uint8_t *wire)
{
	double x = as_double_get(as_double_fromval(val));

	*(uint64_t *)wire = cf_swap_to_be64(*(uint64_t *)&x);

	return (uint32_t)sizeof(uint64_t);
}

//------------------------------------------------
// Handle msgpack translation.
//

void
float_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	double x;
	as_unpacker pk = {
			.buffer = packed,
			.offset = 0,
			.length = packed_size
	};

	as_unpack_double(&pk, &x);

	*(double *)pp = x;
}
