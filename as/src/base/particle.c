/*
 * particle.c
 *
 * Copyright (C) 2008-2023 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "base/particle.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/datamodel.h"
#include "base/exp.h"
#include "base/expop.h"
#include "base/proto.h"
#include "base/thr_info.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/write.h"


//==========================================================
// Typedefs & constants.
//

extern const as_particle_vtable integer_vtable;
extern const as_particle_vtable float_vtable;
extern const as_particle_vtable string_vtable;
extern const as_particle_vtable blob_vtable;
extern const as_particle_vtable bool_vtable;
extern const as_particle_vtable hll_vtable;
extern const as_particle_vtable map_vtable;
extern const as_particle_vtable list_vtable;
extern const as_particle_vtable geojson_vtable;

// Array of particle vtable pointers.
const as_particle_vtable *particle_vtable[] = {
		[AS_PARTICLE_TYPE_NULL]			= NULL,
		[AS_PARTICLE_TYPE_INTEGER]		= &integer_vtable,
		[AS_PARTICLE_TYPE_FLOAT]		= &float_vtable,
		[AS_PARTICLE_TYPE_STRING]		= &string_vtable,
		[AS_PARTICLE_TYPE_BLOB]			= &blob_vtable,
		[AS_PARTICLE_TYPE_JAVA_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_CSHARP_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_PYTHON_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_RUBY_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_PHP_BLOB]		= &blob_vtable,
		[AS_PARTICLE_TYPE_ERLANG_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_VECTOR]		= &blob_vtable,
		[AS_PARTICLE_TYPE_BOOL]			= &bool_vtable,
		[AS_PARTICLE_TYPE_HLL]			= &hll_vtable,
		[AS_PARTICLE_TYPE_MAP]			= &map_vtable,
		[AS_PARTICLE_TYPE_LIST]			= &list_vtable,
		[AS_PARTICLE_TYPE_GEOJSON]		= &geojson_vtable
};

static const char* particle_strings[] = {
	"null",
	"numeric",
	"float",
	"string",
	"blob",
	"unused",
	"unused",
	"blob",
	"blob",
	"blob",
	"blob",
	"blob",
	"blob",
	"unused",
	"unused",
	"unused",
	"vector",
	"bool",
	"hll",
	"map",
	"list",
	"unused",
	"unused",
	"geo2dsphere",
};

ARRAY_ASSERT(particle_strings, AS_PARTICLE_TYPE_MAX);


//==========================================================
// Local utilities.
//

// Particle type check.
static inline as_particle_type
safe_particle_type(uint8_t type)
{
	switch ((as_particle_type)type) {
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_VECTOR:
	case AS_PARTICLE_TYPE_BOOL:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_GEOJSON:
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_JAVA_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_JAVA_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_CSHARP_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_CSHARP_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_PYTHON_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_PYTHON_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_RUBY_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_RUBY_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_PHP_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_PHP_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	case AS_PARTICLE_TYPE_ERLANG_BLOB:
		as_info_warn_deprecated("AS_PARTICLE_TYPE_ERLANG_BLOB is deprecated - upgrade your client");
		return (as_particle_type)type;
	// Note - AS_PARTICLE_TYPE_NULL is considered bad here.
	default:
		cf_warning(AS_PARTICLE, "encountered bad particle type %u", type);
		return AS_PARTICLE_TYPE_BAD;
	}
}


//==========================================================
// Particle "class static" functions.
//

as_particle_type
as_particle_type_from_asval(const as_val *val)
{
	as_val_t vtype = as_val_type(val);

	switch (vtype) {
	case AS_UNDEF: // if val was null - handle quietly
	case AS_NIL:
		return AS_PARTICLE_TYPE_NULL;
	case AS_BOOLEAN:
		return AS_PARTICLE_TYPE_BOOL;
	case AS_INTEGER:
		return AS_PARTICLE_TYPE_INTEGER;
	case AS_DOUBLE:
		return AS_PARTICLE_TYPE_FLOAT;
	case AS_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case AS_BYTES:
		return AS_PARTICLE_TYPE_BLOB;
	case AS_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case AS_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case AS_MAP:
		return AS_PARTICLE_TYPE_MAP;
	case AS_REC:
	case AS_PAIR:
	default:
		cf_warning(AS_PARTICLE, "no particle type for as_val_t %d", vtype);
		return AS_PARTICLE_TYPE_NULL;
	}
}

as_particle_type
as_particle_type_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	msgpack_type vtype = msgpack_buf_peek_type(packed, packed_size);

	switch (vtype) {
	case AS_NIL:
		return AS_PARTICLE_TYPE_NULL;
	case MSGPACK_TYPE_FALSE:
	case MSGPACK_TYPE_TRUE:
		return AS_PARTICLE_TYPE_BOOL;
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
		return AS_PARTICLE_TYPE_INTEGER;
	case MSGPACK_TYPE_DOUBLE:
		return AS_PARTICLE_TYPE_FLOAT;
	case MSGPACK_TYPE_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case MSGPACK_TYPE_BYTES:
		return AS_PARTICLE_TYPE_BLOB;
	// TODO - for now HLL cannot be in CDT.
	case MSGPACK_TYPE_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case MSGPACK_TYPE_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case MSGPACK_TYPE_MAP:
		return AS_PARTICLE_TYPE_MAP;
	default:
		cf_warning(AS_PARTICLE, "encountered bad msgpack_type %d", vtype);
		return AS_PARTICLE_TYPE_BAD;
	}
}

const char*
as_particle_type_str(as_particle_type type)
{
	cf_assert(type > AS_PARTICLE_TYPE_NULL && type < AS_PARTICLE_TYPE_MAX,
			AS_PARTICLE, "bad particle type %u", type);

	return particle_strings[type];
}

uint32_t
as_particle_size_from_asval(const as_val *val)
{
	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just skips unmanageable as_val types.
		return 0;
	}

	return particle_vtable[type]->size_from_asval_fn(val);
}

uint32_t
as_particle_asval_client_value_size(const as_val *val)
{
	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just sends bin-op with NULL particle to client.
		return 0;
	}

	return particle_vtable[type]->asval_wire_size_fn(val);
}

uint32_t
as_particle_asval_to_client(const as_val *val, as_msg_op *op)
{
	as_particle_type type = as_particle_type_from_asval(val);

	op->particle_type = type;

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just sends bin-op with NULL particle to client.
		return 0;
	}

	uint8_t *value = (uint8_t *)as_msg_op_get_value_p(op);
	uint32_t added_size = particle_vtable[type]->asval_to_wire_fn(val, value);

	op->op_sz += added_size;

	return added_size;
}

const uint8_t *
as_particle_skip_flat(const uint8_t *flat, const uint8_t *end)
{
	if (flat >= end) {
		cf_warning(AS_PARTICLE, "incomplete flat particle");
		return NULL;
	}

	if (as_bin_particle_is_tombstone((as_particle_type)*flat)) {
		return flat + 1;
	}

	as_particle_type type = safe_particle_type(*flat);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return NULL;
	}

	// Skip the flat particle.
	return particle_vtable[type]->skip_flat_fn(flat, end);
}


//==========================================================
// as_bin particle functions.
//

//------------------------------------------------
// Destructor, etc.
//

void
as_bin_particle_destroy(as_bin *b)
{
	if (as_bin_is_external_particle(b) && b->particle != NULL) {
		particle_vtable[as_bin_get_particle_type(b)]->destructor_fn(b->particle);
	}

	// Probably unnecessary, but...
	as_bin_set_empty(b);
	b->particle = NULL;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_particle_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type == AS_PARTICLE_TYPE_BAD) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	const uint8_t *op_value = as_msg_op_get_value_p(op);

	// Currently all operations become creates if there's no existing particle.
	if (! as_bin_is_live(b)) {
		int32_t mem_size = particle_vtable[op_type]->size_from_wire_fn(op_value, op_value_size);

		if (mem_size < 0) {
			return (int)mem_size;
		}

		as_particle *old_particle = b->particle; // CLEANUP? - not needed

		// Instead of allocating, we use the stack buffer provided. (Note that
		// embedded types like integer will overwrite this with the value.)
		cf_ll_buf_reserve(particles_llb, (size_t)mem_size, (uint8_t **)&b->particle);

		// Load the new particle into the bin.
		int result = particle_vtable[op_type]->from_wire_fn(op_type, op_value, op_value_size, &b->particle);

		// Set the bin's state member.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			b->particle = old_particle; // CLEANUP? - just set NULL
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;

	as_particle *old_particle = b->particle;
	int result = 0;

	switch (operation) {
	case AS_MSG_OP_INCR:
		result = particle_vtable[existing_type]->incr_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_APPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		cf_ll_buf_reserve(particles_llb, (size_t)new_mem_size, (uint8_t **)&b->particle);
		memcpy(b->particle, old_particle, particle_vtable[existing_type]->size_fn(old_particle));
		result = particle_vtable[existing_type]->append_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_PREPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		cf_ll_buf_reserve(particles_llb, (size_t)new_mem_size, (uint8_t **)&b->particle);
		memcpy(b->particle, old_particle, particle_vtable[existing_type]->size_fn(old_particle));
		result = particle_vtable[existing_type]->prepend_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	default:
		// TODO - just crash?
		return -AS_ERR_UNKNOWN;
	}

	if (result < 0) {
		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	as_particle_type type = safe_particle_type(op->particle_type);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_ERR_PARAMETER;
	}

	uint32_t value_size = as_msg_op_get_value_sz(op);
	const uint8_t *value = as_msg_op_get_value_p(op);
	int32_t mem_size = particle_vtable[type]->size_from_wire_fn(value, value_size);

	if (mem_size < 0) {
		return (int)mem_size;
	}

	as_particle *old_particle = b->particle;

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	cf_ll_buf_reserve(particles_llb, (size_t)mem_size, (uint8_t **)&b->particle);

	// Load the new particle into the bin.
	int result = particle_vtable[type]->from_wire_fn(type, value, value_size, &b->particle);

	// Set the bin's state member.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		b->particle = old_particle;
	}

	return result;
}

uint32_t
as_bin_particle_client_value_size(const as_bin *b)
{
	cf_assert(b != NULL, AS_PARTICLE, "null response bin");

	if (as_bin_is_tombstone(b)) {
		return 0; // XDR will ship bin tombstones
	}

	uint8_t type = as_bin_get_particle_type(b);

	return particle_vtable[type]->wire_size_fn(b->particle);
}

uint32_t
as_bin_particle_to_client(const as_bin *b, as_msg_op *op)
{
	if (b == NULL || as_bin_is_tombstone(b)) {
		// Ordered ops that find no bin will get here with b == NULL.
		// XDR will ship bin tombstones.
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	op->particle_type = type;

	uint8_t *value = (uint8_t *)as_msg_op_get_value_p(op);
	uint32_t added_size = particle_vtable[type]->to_wire_fn(b->particle, value);

	op->op_sz += added_size;

	return added_size;
}

//------------------------------------------------
// Handle as_val translation.
//

void
as_bin_particle_from_asval(as_bin *b, uint8_t *stack, const as_val *val)
{
	// We're using stack particles, so the old particle is either nonexistent or
	// also a stack particle - either way, don't destroy.

	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just skips unmanageable as_val types.
		return;
	}

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	b->particle = (as_particle *)stack;

	// Load the new particle into the bin.
	particle_vtable[type]->from_asval_fn(val, &b->particle);
	// TODO - could this ever fail?

	// Set the bin's state member.
	as_bin_state_set_from_type(b, type);

	// TODO - we don't bother returning size written, since nothing yet needs
	// it and it's very expensive for CDTs to do an extra size_from_asval_fn()
	// call. Perhaps we could have from_asval_fn() return the size if needed?
}

as_val *
as_bin_particle_to_asval(const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	// Caller is responsible for freeing as_val returned here.
	return particle_vtable[type]->to_asval_fn(b->particle);
}

//------------------------------------------------
// Handle msgpack translation.
//

int32_t
as_particle_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	as_particle_type type = as_particle_type_from_msgpack(packed, packed_size);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -1;
	}

	if (type == AS_PARTICLE_TYPE_NULL) {
		return 0;
	}

	return particle_vtable[type]->size_from_msgpack_fn(packed, packed_size);
}

bool
as_bin_particle_from_msgpack(as_bin *b, const uint8_t *packed, uint32_t packed_size, void *mem)
{
	as_particle_type type = as_particle_type_from_msgpack(packed, packed_size);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return false;
	}

	if (type == AS_PARTICLE_TYPE_NULL) {
		return true;
	}

	if (mem != NULL) {
		b->particle = mem;
	}

	particle_vtable[type]->from_msgpack_fn(packed, packed_size, &b->particle);

	// Set the bin's state member.
	as_bin_state_set_from_type(b, type);

	return true;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

const uint8_t *
as_bin_particle_from_flat(as_bin *b, const uint8_t *flat, const uint8_t *end)
{
	// We assume the bin is empty.

	if (flat >= end) {
		cf_warning(AS_PARTICLE, "incomplete flat particle");
		return NULL;
	}

	if (as_bin_particle_is_tombstone((as_particle_type)*flat)) {
		as_bin_set_tombstone(b);
		return flat + 1;
	}

	as_particle_type type = safe_particle_type(*flat);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return NULL;
	}

	// Cast the new particle into the bin.
	flat = particle_vtable[type]->from_flat_fn(flat, end, &b->particle);

	// Set the bin's state member.
	if (flat) {
		as_bin_state_set_from_type(b, type);
	}
	// else - bin remains unpopulated.

	return flat;
}

uint32_t
as_bin_particle_flat_size(as_bin *b)
{
	if (as_bin_is_tombstone(b)) {
		return 1; // particle type only (AS_PARTICLE_TYPE_NULL)
	}

	uint8_t type = as_bin_get_particle_type(b);

	return particle_vtable[type]->flat_size_fn(b->particle);
}

uint32_t
as_bin_particle_to_flat(const as_bin *b, uint8_t *flat)
{
	if (as_bin_is_tombstone(b)) {
		*flat = AS_PARTICLE_TYPE_NULL;
		return 1;
	}

	uint8_t type = as_bin_get_particle_type(b);

	*flat = type;

	return particle_vtable[type]->to_flat_fn(b->particle, flat);
}


//==========================================================
// as_bin particle functions specific to blobs.
//

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_bits_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op)
{
	return as_bin_bits_modify_tr(b, op, particles_llb);
}

int
as_bin_bits_read_from_client(const as_bin *b, as_msg_op *op, as_bin *result)
{
	return as_bin_bits_read_tr(b, op, result);
}


//==========================================================
// as_bin particle functions specific to HLL.
//

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_hll_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, as_bin *rb)
{
	return as_bin_hll_modify_tr(b, op, particles_llb, rb);
}

int
as_bin_hll_read_from_client(const as_bin *b, as_msg_op *op, as_bin *rb)
{
	return as_bin_hll_read_tr(b, op, rb);
}


//==========================================================
// as_bin particle functions specific to CDTs.
//

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_cdt_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, as_bin *result)
{
	return as_bin_cdt_modify_tr(b, op, result, particles_llb);
}

int
as_bin_cdt_read_from_client(const as_bin *b, as_msg_op *op, as_bin *result)
{
	return as_bin_cdt_read_tr(b, op, result);
}


//==========================================================
// as_bin particle functions specific to expressions.
//

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_exp_modify_from_client(const as_exp_ctx* ctx, as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, const iops_expop* expop)
{
	return as_exp_modify_tr(ctx, b, op, particles_llb, expop);
}

int
as_bin_exp_read_from_client(const as_exp_ctx* ctx, as_msg_op *op, as_bin *rb)
{
	return as_exp_read_tr(ctx, op, rb);
}
