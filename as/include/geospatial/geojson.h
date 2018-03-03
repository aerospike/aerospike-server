/* 
 * Copyright 2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef __geojson_h
#define __geojson_h		1

#include <string>

#include <jansson.h>

#include <s2cellid.h>
#include <s2region.h>

namespace GeoJSON {

class GeometryHandler
{
public:
	virtual ~GeometryHandler() {}

	virtual void handle_point(S2CellId const & cellid);

	virtual bool handle_region(S2Region * regionp);

	virtual double earth_radius_meters() {
		return 6371000.0;		// Wikipedia, mean radius.
	}

	void set_json(json_t * i_jsonp) { m_jsonp = i_jsonp; }

	json_t * get_json() { return m_jsonp; }

private:
	json_t * m_jsonp;
};

void parse(GeometryHandler & geohand, std::string const & geostr);

} // end namespace GeoJSON

#endif // __geojson_h
