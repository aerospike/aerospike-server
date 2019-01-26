/*
 * particle_geojson.c
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
#include <string.h>

#include "aerospike/as_geojson.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"
#include "geospatial/geospatial.h"


//==========================================================
// GEOJSON particle interface - function declarations.
//

// Most GEOJSON particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle "wire" format.
int32_t geojson_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t geojson_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int geojson_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
uint32_t geojson_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t geojson_size_from_asval(const as_val *val);
void geojson_from_asval(const as_val *val, as_particle **pp);
as_val *geojson_to_asval(const as_particle *p);
uint32_t geojson_asval_wire_size(const as_val *val);
uint32_t geojson_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t geojson_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void geojson_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);


//==========================================================
// GEOJSON particle interface - vtable.
//

const as_particle_vtable geojson_vtable = {
		blob_destruct,
		blob_size,

		geojson_concat_size_from_wire,
		geojson_append_from_wire,
		geojson_prepend_from_wire,
		geojson_incr_from_wire,
		geojson_size_from_wire,
		geojson_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		geojson_to_wire,

		geojson_size_from_asval,
		geojson_from_asval,
		geojson_to_asval,
		geojson_asval_wire_size,
		geojson_asval_to_wire,

		geojson_size_from_msgpack,
		geojson_from_msgpack,

		blob_skip_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// GEOJSON particle flag bit-fields.
#define GEOJSON_ISREGION	0x1

// The GEOJSON particle structs overlay the related BLOB structs.

typedef struct geojson_mem_s {
	uint8_t		type;	// IMPORTANT: overlay blob_mem!
	uint32_t	sz;		// IMPORTANT: overlay blob_mem!
	uint8_t		flags;
	uint16_t	ncells;
	uint8_t		data[];	// (ncells * uint64_t) + jsonstr
} __attribute__ ((__packed__)) geojson_mem;

typedef struct geojson_flat_s {
	uint8_t		type;	// IMPORTANT: overlay blob_flat!
	uint32_t	size;	// IMPORTANT: overlay blob_flat!
	uint8_t		flags;
	uint16_t	ncells;
	uint8_t		data[];	// (ncells * uint64_t) + jsonstr
} __attribute__ ((__packed__)) geojson_flat;


//==========================================================
// Forward declarations.
//

static bool geojson_match(bool particle_is_region, uint64_t particle_cellid, geo_region_t particle_region, uint64_t query_cellid, geo_region_t query_region, bool is_strict);
static inline uint32_t geojson_mem_sz(uint32_t ncells, size_t jlen);
static inline uint32_t geojson_particle_sz(uint32_t ncells, size_t jlen);
static inline bool geojson_parse(const char *json, uint32_t jlen, uint64_t *cellid, geo_region_t *region);
static bool geojson_to_particle(const char *json, uint32_t jlen, as_particle **pp);


//==========================================================
// GEOJSON particle interface - function definitions.
//

// Most GEOJSON particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

int32_t
geojson_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// NOTE - Unfortunately we would need to run the JSON parser and region
	// coverer to find out exactly how many cells we need to allocate for this
	// particle.
	//
	// For now we always allocate the maximum number of cells (MAX_REGION_CELLS)
	// for the in-memory particle.
	//
	// For now also ignore any incoming cells entirely.

	const uint16_t *p_cells = (const uint16_t *)(wire_value + 1);
	uint16_t ncells = cf_swap_from_be16(*p_cells);
	size_t cellsz = ncells * sizeof(uint64_t);

	if ((size_t)value_size < sizeof(uint8_t) + sizeof(uint16_t) + cellsz) {
		cf_warning(AS_PARTICLE, "geojson_size_from_wire() invalid geojson wire_sz %u < cellsz %zu + 3", value_size, cellsz);
		return -AS_ERR_GEO_INVALID_GEOJSON;
	}

	size_t jlen = value_size - sizeof(uint8_t) - sizeof(uint16_t) - cellsz;

	return (int32_t)geojson_particle_sz(MAX_REGION_CELLS, jlen);
}

int
geojson_from_wire(as_particle_type wire_type, const uint8_t *wire_value,
		uint32_t value_size, as_particle **pp)
{
	const uint16_t *p_cells = (const uint16_t *)(wire_value + 1);
	uint16_t ncells = cf_swap_from_be16(*p_cells);
	size_t cellsz = ncells * sizeof(uint64_t);
	char const *json = (char const *)p_cells + sizeof(uint16_t) + cellsz;
	size_t jlen = value_size - sizeof(uint8_t) - sizeof(uint16_t) - cellsz;
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;

	p_geojson_mem->type = wire_type;

	// We ignore any incoming cells entirely.

	if (! geojson_to_particle(json, jlen, pp)) {
		cf_warning(AS_PARTICLE, "geojson_from_wire() failed");
		return -AS_ERR_GEO_INVALID_GEOJSON;
	}

	return AS_OK;
}

uint32_t
geojson_to_wire(const as_particle *p, uint8_t *wire)
{
	// Use blob routine first.
	uint32_t sz = blob_to_wire(p, wire);

	// Swap ncells.
	uint16_t *p_ncells = (uint16_t *)(wire + sizeof(uint8_t));
	uint16_t ncells = *p_ncells;

	*p_ncells = cf_swap_to_be16(*p_ncells);
	++p_ncells;

	// Swap the cells.
	uint64_t *p_cell_begin = (uint64_t *)p_ncells;
	uint64_t *p_cell_end = p_cell_begin + ncells;

	for (uint64_t *p_cell = p_cell_begin; p_cell < p_cell_end; ++p_cell) {
		*p_cell = cf_swap_to_be64(*p_cell);
	}

	return sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
geojson_size_from_asval(const as_val *val)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jsz = as_geojson_len(pg);

	return geojson_particle_sz(MAX_REGION_CELLS, jsz);
}

void
geojson_from_asval(const as_val *val, as_particle **pp)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;
	as_geojson *pg = as_geojson_fromval(val);
	size_t jlen = as_geojson_len(pg);

	p_geojson_mem->type = AS_PARTICLE_TYPE_GEOJSON;

	if (! geojson_to_particle(as_geojson_get(pg), jlen, pp)) {
		cf_warning(AS_PARTICLE, "geojson_from_asval() failed");
	}
}

as_val *
geojson_to_asval(const as_particle *p)
{
	size_t jlen;
	char const *json = as_geojson_mem_jsonstr(p, &jlen);
	char *buf = cf_malloc(jlen + 1);

	memcpy(buf, json, jlen);
	buf[jlen] = '\0';

	return (as_val *)as_geojson_new_wlen(buf, jlen, true);
}

uint32_t
geojson_asval_wire_size(const as_val *val)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jlen = as_geojson_len(pg);

	// We won't be writing any cellids ...
	return geojson_mem_sz(0, jlen);
}

uint32_t
geojson_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jlen = as_geojson_len(pg);

	uint8_t *p8 = wire;

	*p8++ = 0;						// flags

	uint16_t *p16 = (uint16_t *)p8;

	*p16++ = cf_swap_to_be16(0);	// no cells on output to client
	p8 = (uint8_t *)p16;
	memcpy(p8, as_geojson_get(pg), jlen);

	return geojson_mem_sz(0, jlen);
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
geojson_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	// Oversize by a few bytes doing the easy thing.
	size_t jsz = (size_t)packed_size;

	// Compute the size; we won't be writing any cellids ...
	return geojson_particle_sz(0, jsz);
}

void
geojson_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;

	as_unpacker pk = {
			.buffer = packed,
			.offset = 0,
			.length = packed_size
	};

	int64_t blob_size = as_unpack_blob_size(&pk);
	const uint8_t *ptr = pk.buffer + pk.offset;

	// *ptr should be AS_BYTES_GEOJSON at this point.

	// Adjust for type (1 byte).
	ptr++;
	blob_size--;

	size_t jsz = (size_t)blob_size;

	p_geojson_mem->type = AS_PARTICLE_TYPE_GEOJSON;
	p_geojson_mem->sz = geojson_mem_sz(0, jsz);
	p_geojson_mem->flags = 0;
	p_geojson_mem->ncells = 0;

	uint8_t *p8 = (uint8_t *)p_geojson_mem->data;
	memcpy(p8, ptr, jsz);
}


//==========================================================
// Particle functions specific to GEOJSON.
//

size_t
as_bin_particle_geojson_cellids(const as_bin *b, uint64_t **ppcells)
{
	geojson_mem *gp = (geojson_mem *)b->particle;

	*ppcells = (uint64_t *)gp->data;

	return (size_t)gp->ncells;
}

bool
as_particle_geojson_match(as_particle *particle, uint64_t query_cellid,
		geo_region_t query_region, bool is_strict)
{
	// Determine whether the candidate particle geometry is a match
	// for the query geometry.
	//
	// If query_cellid is non-zero this is a regions-containing-point query.
	//
	// If query_region is non-null this is a points-in-region query.
	//
	// Candidate geometry can either be a point or a region.  Regions
	// will have the GEOJSON_ISREGION flag set.

	geojson_mem *p_geojson_mem = (geojson_mem *)particle;
	uint64_t *cells = (uint64_t *)p_geojson_mem->data;
	uint64_t candidate_cellid = p_geojson_mem->ncells == 0 ? 0 : cells[0];
	geo_region_t candidate_region = NULL;
	bool candidate_is_region = (p_geojson_mem->flags & GEOJSON_ISREGION) != 0;

	// If we are a strict RCP query on a region candidate we need to
	// run the parser to obtain a candidate_region for the matcher.
	if (query_cellid != 0 && candidate_is_region && is_strict) {
		size_t jsonsz;
		char const *jsonptr = as_geojson_mem_jsonstr(particle, &jsonsz);

		if (! geo_parse(NULL, jsonptr, jsonsz, &candidate_cellid,
				&candidate_region)) {
			cf_warning(AS_PARTICLE, "geo_parse() failed - unexpected");
			geo_region_destroy(candidate_region);
			return false;
		}
	}

	bool ismatch = geojson_match(candidate_is_region, candidate_cellid,
			candidate_region, query_cellid, query_region, is_strict);

	geo_region_destroy(candidate_region);

	return ismatch;
}

bool
as_particle_geojson_match_asval(const as_val *val, uint64_t query_cellid,
		geo_region_t query_region, bool is_strict)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jlen = as_geojson_len(pg);
	const char *json = as_geojson_get(pg);

	uint64_t candidate_cellid = 0;
	geo_region_t candidate_region = NULL;

	if (! geo_parse(NULL, json, jlen, &candidate_cellid, &candidate_region)) {
		cf_warning(AS_PARTICLE, "geo_parse() failed - unexpected");
		geo_region_destroy(candidate_region);
		return false;
	}

	bool ismatch = geojson_match(candidate_cellid == 0, candidate_cellid,
			candidate_region, query_cellid, query_region, is_strict);

	geo_region_destroy(candidate_region);

	return ismatch;
}

const char *
as_geojson_mem_jsonstr(const as_particle *particle, size_t *p_jlen)
{
	const geojson_mem *p_geojson_mem = (const geojson_mem *)particle;
	size_t cellsz = p_geojson_mem->ncells * sizeof(uint64_t);

	*p_jlen = p_geojson_mem->sz - sizeof(uint8_t) - sizeof(uint16_t) - cellsz;

	return (const char *)p_geojson_mem->data + cellsz;
}


//==========================================================
// Local helpers.
//

static bool
geojson_match(bool candidate_is_region, uint64_t candidate_cellid, geo_region_t candidate_region, uint64_t query_cellid, geo_region_t query_region, bool is_strict)
{
	// Determine whether the candidate geometry is a match for the
	// query geometry.
	//
	// If query_cellid is non-zero this is a regions-containing-point query.
	//
	// If query_region is non-null this is a points-in-region query.
	//
	// Candidate geometry can either be a point or a region.  Regions
	// will have the GEOJSON_ISREGION flag set.

	// Is this a REGIONS-CONTAINING-POINT query?
	//
	if (query_cellid != 0) {

		if (candidate_is_region) {
			// Candidate is a REGION.

			// Shortcut, if we aren't strict just return true.
			if (! is_strict) {
				return true;
			}

			return geo_point_within(query_cellid, candidate_region);
		}
		else {
			// Candidate is a POINT, skip it.
			return false;
		}
	}

	// Is this a POINTS-IN-REGION query?
	//
	if (query_region) {

		if (candidate_is_region) {
			// Candidate is a REGION, skip it.
			return false;
		}
		else {
			// Sanity check, make sure this geometry has been processed.
			if (candidate_cellid == 0) {
				cf_warning(AS_PARTICLE, "candidate cellid has no value");
				return false;
			}

			// Candidate is a POINT.
			if (is_strict) {
				return geo_point_within(candidate_cellid, query_region);
			}
			else {
				return true;
			}
		}
	}

	return false;
}

static inline uint32_t
geojson_mem_sz(uint32_t ncells, size_t jlen)
{
	return (uint32_t)(
			sizeof(uint8_t) +				// flags
			sizeof(uint16_t) +				// ncells (always 0 here)
			(ncells * sizeof(uint64_t)) +	// cell array
			jlen);							// json string
}

static inline uint32_t
geojson_particle_sz(uint32_t ncells, size_t jlen)
{
	return (uint32_t)(
			sizeof(geojson_mem) +
			(ncells * sizeof(uint64_t)) +	// cell array
			jlen);							// json string
}

static inline bool
geojson_parse(const char *json, uint32_t jlen, uint64_t *cellid,
		geo_region_t *region)
{
	*cellid = 0;
	*region = NULL;

	if (! geo_parse(NULL, json, jlen, cellid, region)) {
		cf_warning(AS_PARTICLE, "geo_parse failed");
		return false;
	}

	if (*cellid != 0 && *region != NULL) {
		geo_region_destroy(region);
		cf_warning(AS_PARTICLE, "geo_parse found both point and region");
		*cellid = 0;
		*region = NULL;
		return false;
	}

	if (*cellid == 0 && *region == NULL) {
		cf_warning(AS_PARTICLE, "geo_parse found neither point nor region");
		return false;
	}

	return true;
}

static bool
geojson_to_particle(const char *json, uint32_t jlen, as_particle **pp)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;
	uint64_t cellid;
	geo_region_t region;
	bool ret = true;
	uint64_t *p_outcells = (uint64_t *)p_geojson_mem->data;

	if (! geojson_parse(json, jlen, &cellid, &region)) {
		ret = false;
	}

	p_geojson_mem->flags = 0;

	if (cellid) { // POINT
		p_geojson_mem->ncells = 1;
		p_outcells[0] = cellid;
	}
	else if (region) { // REGION
		p_geojson_mem->flags |= GEOJSON_ISREGION;

		int numcells;

		if (! geo_region_cover(NULL, region, MAX_REGION_CELLS, p_outcells, NULL,
				NULL, &numcells)) {
			cf_warning(AS_PARTICLE, "geo_region_cover failed");
			ret = false;
			numcells = 0;
		}

		p_geojson_mem->ncells = (uint16_t)numcells;
		geo_region_destroy(region);
	}
	else {
		p_geojson_mem->ncells = 0;
	}

	uint8_t *p_out = (uint8_t *)(p_outcells + p_geojson_mem->ncells);

	memcpy(p_out, json, jlen);

	// Set the actual size; we will waste some space at the end of the allocated
	// particle.
	p_geojson_mem->sz = geojson_mem_sz(p_geojson_mem->ncells, jlen);

	return ret;
}
