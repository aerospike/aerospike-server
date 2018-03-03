/*
 * particle.h
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

#pragma once

#include <stdint.h>
#include "aerospike/as_val.h"
#include "base/datamodel.h"

//------------------------------------------------
// Particle interface specification - functions.
//

// Destructor, etc.
typedef void (*as_particle_destructor_fn) (as_particle *p);
typedef uint32_t (*as_particle_size_fn) (const as_particle *p);

// Handle "wire" format.
typedef int32_t (*as_particle_concat_size_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
typedef int (*as_particle_append_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
typedef int (*as_particle_prepend_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
typedef int (*as_particle_incr_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
typedef int32_t (*as_particle_size_from_wire_fn) (const uint8_t *wire_value, uint32_t value_size);
typedef int (*as_particle_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
typedef int (*as_particle_compare_from_wire_fn) (const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
typedef uint32_t (*as_particle_wire_size_fn) (const as_particle *p);
typedef uint32_t (*as_particle_to_wire_fn) (const as_particle *p, uint8_t *wire);

// Handle as_val translation.
typedef uint32_t (*as_particle_size_from_asval_fn) (const as_val *val);
typedef void (*as_particle_from_asval_fn) (const as_val *val, as_particle **pp);
typedef as_val *(*as_particle_to_asval_fn) (const as_particle *p);
typedef uint32_t (*as_particle_asval_wire_size_fn) (const as_val *val);
typedef uint32_t (*as_particle_asval_to_wire_fn) (const as_val *val, uint8_t *wire);

// Handle msgpack translation.
typedef uint32_t (*as_particle_size_from_msgpack_fn) (const uint8_t *packed, uint32_t packed_size);
typedef void (*as_particle_from_msgpack_fn) (const uint8_t *packed, uint32_t packed_size, as_particle **pp);

// Handle on-device "flat" format.
typedef int32_t (*as_particle_size_from_flat_fn) (const uint8_t *flat, uint32_t flat_size);
typedef int (*as_particle_cast_from_flat_fn) (uint8_t *flat, uint32_t flat_size, as_particle **pp);
typedef int (*as_particle_from_flat_fn) (const uint8_t *flat, uint32_t flat_size, as_particle **pp);
typedef uint32_t (*as_particle_flat_size_fn) (const as_particle *p);
typedef uint32_t (*as_particle_to_flat_fn) (const as_particle *p, uint8_t *flat);

//------------------------------------------------
// Particle interface specification - vtable.
//

typedef struct as_particle_vtable_s {
	as_particle_destructor_fn				destructor_fn;
	as_particle_size_fn						size_fn;

	as_particle_concat_size_from_wire_fn	concat_size_from_wire_fn;
	as_particle_append_from_wire_fn			append_from_wire_fn;
	as_particle_prepend_from_wire_fn		prepend_from_wire_fn;
	as_particle_incr_from_wire_fn			incr_from_wire_fn;
	as_particle_size_from_wire_fn			size_from_wire_fn;
	as_particle_from_wire_fn				from_wire_fn;
	as_particle_compare_from_wire_fn		compare_from_wire_fn;
	as_particle_wire_size_fn				wire_size_fn;
	as_particle_to_wire_fn					to_wire_fn;

	as_particle_size_from_asval_fn			size_from_asval_fn;
	as_particle_from_asval_fn				from_asval_fn;
	as_particle_to_asval_fn					to_asval_fn;
	as_particle_asval_wire_size_fn			asval_wire_size_fn;
	as_particle_asval_to_wire_fn			asval_to_wire_fn;

	as_particle_size_from_msgpack_fn		size_from_msgpack_fn;
	as_particle_from_msgpack_fn				from_msgpack_fn;

	as_particle_size_from_flat_fn			size_from_flat_fn; // TODO - unused - remove?
	as_particle_cast_from_flat_fn			cast_from_flat_fn;
	as_particle_from_flat_fn				from_flat_fn;
	as_particle_flat_size_fn				flat_size_fn;
	as_particle_to_flat_fn					to_flat_fn;
} as_particle_vtable;
