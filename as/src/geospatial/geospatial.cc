/*
 * geospatial.cpp
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
 * along with this program.	 If not, see http://www.gnu.org/licenses/
 */

#include <errno.h>
#include <limits.h>
#include <string.h>

#include <stdexcept>

#include <s2regioncoverer.h>

extern "C" {
#include "fault.h"
#include "base/datamodel.h"
} // end extern "C"

#include "geospatial/geospatial.h"
#include "geospatial/geojson.h"

using namespace std;

class PointRegionHandler: public GeoJSON::GeometryHandler
{
public:
	PointRegionHandler(as_namespace * ns)
		: m_cellid(0)
		, m_regionp(NULL)
	{
		m_earth_radius_meters =
			ns ? double(ns->geo2dsphere_within_earth_radius_meters) : 6371000;
	}

	virtual void handle_point(S2CellId const & cellid) {
		m_cellid = cellid;
	}

	virtual bool handle_region(S2Region * regionp) {
		m_regionp = regionp;
		return false;	// Don't delete this region, please.
	}

	virtual double earth_radius_meters() {
		return m_earth_radius_meters;
	}

	double m_earth_radius_meters;
	S2CellId	m_cellid;
	S2Region * m_regionp;
};

bool
geo_parse(as_namespace * ns,
		  const char * buf,
		  size_t bufsz,
		  uint64_t * cellidp,
		  geo_region_t * regionp)
{
	try
	{
		PointRegionHandler prhandler(ns);
		GeoJSON::parse(prhandler, string(buf, bufsz));
		*cellidp = prhandler.m_cellid.id();
		*regionp = (geo_region_t) prhandler.m_regionp;
		return true;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "failed to parse point: %s", ex.what());
		return false;
	}
}
	
bool
geo_region_cover(as_namespace * ns,
				 geo_region_t region,
				 int maxnumcells,
				 uint64_t * cellctrp,
				 uint64_t * cellminp,
				 uint64_t * cellmaxp,
				 int * numcellsp)
{
	try
	{
		S2Region * regionp = (S2Region *) region;

		S2RegionCoverer coverer;
		if (ns) {
			coverer.set_min_level(ns->geo2dsphere_within_min_level);
			coverer.set_max_level(ns->geo2dsphere_within_max_level);
			coverer.set_max_cells(ns->geo2dsphere_within_max_cells);
			coverer.set_level_mod(ns->geo2dsphere_within_level_mod);
		}
		else {
			// FIXME - we really don't want to hardcode these values, but
			// some callers can't provide the namespace context ...
			coverer.set_min_level(1);
			coverer.set_max_level(30);
			coverer.set_max_cells(12);
			coverer.set_level_mod(1);
		}
		vector<S2CellId> covering;
		coverer.GetCovering(*regionp, &covering);

		// The coverer can always return 6 cells, even when max cells is
		// less (regions which intersect all cube faces).  If we get more
		// then we asked for and it's greater then 6 something is wrong.
		if (covering.size() > max(size_t(6), size_t(coverer.max_cells()))) {
			return false;
		}
	
		for (size_t ii = 0; ii < covering.size(); ++ii)
		{
			if (ii == (size_t) maxnumcells)
			{
				cf_warning(AS_GEO, (char *) "region covered with %zu cells, "
						   "only %d allowed", covering.size(), maxnumcells);
				return false;
			}

			if (cellctrp) {
				cellctrp[ii] = covering[ii].id();
			}
			if (cellminp) {
				cellminp[ii] = covering[ii].range_min().id();
			}
			if (cellmaxp) {
				cellmaxp[ii] = covering[ii].range_max().id();
			}

			if (cellctrp) {
				cf_detail(AS_GEO, (char *) "cell[%zu]: 0x%lx",
						  ii, cellctrp[ii]);
			}

			if (cellminp && cellmaxp) {
				cf_detail(AS_GEO, (char *) "cell[%zu]: [0x%lx, 0x%lx]",
						  ii, cellminp[ii], cellmaxp[ii]);
			}
		}

		*numcellsp = covering.size();
		return true;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "geo_region_cover failed: %s", ex.what());
		return false;
	}
}

bool
geo_point_centers(as_namespace * ns,
				  uint64_t cellidval,
				  int maxnumcenters,
				  uint64_t * center,
				  int * numcentersp)
{
	try
	{
		S2CellId incellid(cellidval);

		*numcentersp = 0;
	
		for (S2CellId cellid = incellid;
			 cellid.level() > 0;
			 cellid = cellid.parent())
		{
			// Make sure we don't overwrite the output array.
			if (*numcentersp == maxnumcenters) {
				break;
			}
			center[*numcentersp] = cellid.id();
			*numcentersp += 1;
		}
		return true;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "geo_point_centers failed: %s", ex.what());
		return false;
	}
}

bool
geo_point_within(uint64_t cellidval, geo_region_t region)
{
	try
	{
		S2Region * regionp = (S2Region *) region;
		S2CellId cellid(cellidval);
		bool iswithin = regionp->VirtualContainsPoint(cellid.ToPoint());
		return iswithin;
	}
	catch (exception const & ex)
	{
		cf_warning(AS_GEO, (char *) "exception in geo_point_within: %s",
				   ex.what());
		return false;
	}
}

void
geo_region_destroy(geo_region_t region)
{
	S2Region * regionp = (S2Region *) region;
	if (regionp) {
		delete regionp;
	}
}
