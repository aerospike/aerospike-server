/*
 * geospatial.h
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

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#include "base/datamodel.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool geo_parse(as_namespace * ns,
					  const char * buf,
					  size_t bufsz,
					  uint64_t * cellidp,
					  geo_region_t * regionp);
	
extern bool geo_region_cover(as_namespace * ns,
							 geo_region_t region,
							 int maxnumcells,
							 uint64_t * cellctrp,
							 uint64_t * cellminp,
							 uint64_t * cellmaxp,
							 int * numcellsp);

extern bool geo_point_centers(as_namespace * ns,
							  uint64_t cellidval,
							  int maxnumcenters,
							  uint64_t * center,
							  int * numcentersp);

extern bool geo_point_within(uint64_t cellidval, geo_region_t region);

extern void geo_region_destroy(geo_region_t region);

#ifdef __cplusplus
} // end extern "C"
#endif
