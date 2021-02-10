/*
 * particle_bool.c
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


#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_boolean.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_byte_order.h"

#include "log.h"
#include "msgpack_in.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_integer.h"
#include "base/proto.h"


//==========================================================
// BOOL particle interface - function declarations.
//

// Most BOOL particle table functions just use the equivalent INTEGER particle
// functions. Here are the differences...

// Handle "wire" format.
int bool_incr_from_wire(as_particle_type wire_type, const uint8_t* wire_value, uint32_t value_size, as_particle** pp);
int bool_from_wire(as_particle_type wire_type, const uint8_t* wire_value, uint32_t value_size, as_particle** pp);
int bool_compare_from_wire(const as_particle* p, as_particle_type wire_type, const uint8_t* wire_value, uint32_t value_size);
uint32_t bool_wire_size(const as_particle* p);
uint32_t bool_to_wire(const as_particle* p, uint8_t* wire);

// Handle as_val translation.
void bool_from_asval(const as_val* val, as_particle** pp);
as_val* bool_to_asval(const as_particle* p);
uint32_t bool_asval_wire_size(const as_val* val);
uint32_t bool_asval_to_wire(const as_val* val, uint8_t* wire);

// Handle msgpack translation.
void bool_from_msgpack(const uint8_t* packed, uint32_t packed_size, as_particle** pp);

// Handle on-device "flat" format.
const uint8_t* bool_skip_flat(const uint8_t* flat, const uint8_t* end);
const uint8_t* bool_from_flat(const uint8_t* flat, const uint8_t* end, as_particle** pp);
uint32_t bool_flat_size(const as_particle* p);
uint32_t bool_to_flat(const as_particle* p, uint8_t* flat);


//==========================================================
// BOOL particle interface - vtable.
//

const as_particle_vtable bool_vtable = {
		integer_destruct,
		integer_size,

		integer_concat_size_from_wire,
		integer_append_from_wire,
		integer_prepend_from_wire,
		bool_incr_from_wire,
		integer_size_from_wire,
		bool_from_wire,
		bool_compare_from_wire,
		bool_wire_size,
		bool_to_wire,

		integer_size_from_asval,
		bool_from_asval,
		bool_to_asval,
		bool_asval_wire_size,
		bool_asval_to_wire,

		integer_size_from_msgpack,
		bool_from_msgpack,

		bool_skip_flat,
		bool_from_flat, // cast copies embedded value out
		bool_from_flat,
		bool_flat_size,
		bool_to_flat
};


//==========================================================
// Typedefs & constants.
//

typedef struct bool_flat_s {
	uint8_t		type;
	uint8_t		i;
} __attribute__ ((__packed__)) bool_flat;


//==========================================================
// BOOL particle interface - function definitions.
//

// Most BOOL particle table functions just use the equivalent INTEGER particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

int
bool_incr_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	(void)wire_type;
	(void)wire_value;
	(void)value_size;
	(void)pp;
	cf_warning(AS_PARTICLE, "error %u unexpected increment of bool",
			AS_ERR_INCOMPATIBLE_TYPE);
	return -AS_ERR_INCOMPATIBLE_TYPE;
}

int
bool_from_wire(as_particle_type wire_type, const uint8_t* wire_value,
		uint32_t value_size, as_particle** pp)
{
	if (value_size != 1) {
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_ERR_PARAMETER;
	}

	if (*wire_value > 1) {
		cf_warning(AS_PARTICLE, "bad bool value %u", (uint32_t)*wire_value);
		return -AS_ERR_PARAMETER;
	}

	*pp = (as_particle*)(uint64_t)*wire_value;

	return 0;
}

int
bool_compare_from_wire(const as_particle* p, as_particle_type wire_type,
		const uint8_t* wire_value, uint32_t value_size)
{
	if (wire_type != AS_PARTICLE_TYPE_BOOL) {
		return 1;
	}

	if (value_size != 1 || *wire_value > 1) {
		return -AS_ERR_UNKNOWN;
	}

	return (uint64_t)p == (uint64_t)*wire_value ? 0 : 1;
}

uint32_t
bool_wire_size(const as_particle* p)
{
	return 1;
}

uint32_t
bool_to_wire(const as_particle* p, uint8_t* wire)
{
	*wire = (uint8_t)(uint64_t)p;

	return 1;
}

//------------------------------------------------
// Handle as_val translation.
//

void
bool_from_asval(const as_val* val, as_particle** pp)
{
	*pp = (as_particle*)(uint64_t)
			(as_boolean_get(as_boolean_fromval(val)) ? 1 : 0);
}

as_val*
bool_to_asval(const as_particle* p)
{
	return (as_val*)as_boolean_new((uint64_t)p != 0);
}

uint32_t
bool_asval_wire_size(const as_val* val)
{
	return 1;
}

uint32_t
bool_asval_to_wire(const as_val* val, uint8_t* wire)
{
	*wire = as_boolean_get(as_boolean_fromval(val)) ? 1 : 0;

	return 1;
}

//------------------------------------------------
// Handle msgpack translation.
//

void
bool_from_msgpack(const uint8_t* packed, uint32_t packed_size, as_particle** pp)
{
	uint64_t i;

	if (*packed == 0xc2) { // false
		i = 0;
	}
	else if (*packed == 0xc3) { // true
		i = 1;
	}
	else {
		cf_crash(AS_PARTICLE, "invalid bool");
	}

	*pp = (as_particle*)i;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t*
bool_skip_flat(const uint8_t* flat, const uint8_t* end)
{
	// Type is correct, since we got here - no need to check against end.
	return flat + sizeof(bool_flat);
}

const uint8_t*
bool_from_flat(const uint8_t* flat, const uint8_t* end, as_particle** pp)
{
	const bool_flat* p_bool_flat = (const bool_flat*)flat;

	flat += sizeof(bool_flat);

	if (flat > end) {
		cf_warning(AS_PARTICLE, "incomplete flat bool");
		return NULL;
	}

	if (p_bool_flat->i > 1) {
		cf_warning(AS_PARTICLE, "bad flat bool %u", (uint32_t)p_bool_flat->i);
		return NULL;
	}

	// Bool values live in an as_bin instead of a pointer.
	*pp = (as_particle*)(uint64_t)p_bool_flat->i;

	return flat;
}

uint32_t
bool_flat_size(const as_particle* p)
{
	return sizeof(bool_flat);
}

uint32_t
bool_to_flat(const as_particle* p, uint8_t* flat)
{
	bool_flat* p_bool_flat = (bool_flat*)flat;

	// Already wrote the type.
	p_bool_flat->i = (uint8_t)(uint64_t)p;

	return bool_flat_size(p);
}


//==========================================================
// as_bin particle functions specific to BOOL.
//

bool
as_bin_particle_bool_value(const as_bin* b)
{
	// Caller must ensure this is called only for BOOL particles.
	return (uint64_t)b->particle != 0;
}
