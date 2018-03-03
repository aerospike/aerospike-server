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

#include <memory>
#include <iostream>
#include <iomanip>
#include <stdexcept>

#include <jansson.h>

#include <s2.h>
#include <s2cap.h>
#include <s2cellid.h>
#include <s2polygon.h>
#include <s2regionunion.h>
#include <s2latlng.h>

#include "geospatial/scoped.h"
#include "geospatial/throwstream.h"
#include "geospatial/geojson.h"

using namespace std;

namespace {

S2Point
traverse_point(json_t * coord)
{
	if (! coord) {
		throwstream(runtime_error, "missing coordinates");
    }

	if (! json_is_array(coord)) {
		throwstream(runtime_error, "coordinates are not array");
    }

	if (json_array_size(coord) != 2) {
		throwstream(runtime_error, "expected 2 coordinates, saw "
					<< json_array_size(coord));
    }

	double lngval;
	json_t * lng = json_array_get(coord, 0);
	if (json_is_real(lng)) {
		lngval = json_real_value(lng);
	}
	else if (json_is_integer(lng)) {
		lngval = double(json_integer_value(lng));
	}
	else {
		throwstream(runtime_error, "longitude not numeric value");
    }
	
	double latval;
	json_t * lat = json_array_get(coord, 1);
	if (json_is_real(lat)) {
		latval = json_real_value(lat);
	}
	else if (json_is_integer(lat)) {
		latval = double(json_integer_value(lat));
	}
	else {
		throwstream(runtime_error, "latitude not numeric value");
    }
	
	// cout << setprecision(15) << latval << ", " << lngval << endl;

	S2LatLng latlng = S2LatLng::FromDegrees(latval, lngval).Normalized();
	if (! latlng.is_valid()) {
		throwstream(runtime_error, "invalid latitude-longitude");
	}
	return latlng.ToPoint();
}

S2Loop *
traverse_loop(json_t * vertices)
{
	if (! vertices) {
		throwstream(runtime_error, "missing vertices");
    }

	if (! json_is_array(vertices)) {
		throwstream(runtime_error, "vertices are not array");
    }

	vector<S2Point> points;

	for (size_t ii = 0; ii < json_array_size(vertices); ++ii) {
		points.push_back(traverse_point(json_array_get(vertices, ii)));
    }

	// Remove duplicate points.
	for (size_t ii = 1; ii < points.size(); ++ii) {
		if (points[ii - 1] == points[ii]) {
			points.erase(points.begin() + ii);
			--ii;
		}
	}

	if (points.size() < 4) {
		throwstream(runtime_error, "loop contains less than 4 points");
	}
	if (points[0] != points[points.size()-1]) {
		throwstream(runtime_error, "loop not closed");
	}
	points.pop_back();

	auto_ptr<S2Loop> loop(new S2Loop(points));
	loop->Normalize();
	return loop.release();
}

S2Polygon *
traverse_polygon(json_t * loops)
{
	if (! loops) {
		throwstream(runtime_error, "missing polygon body");
    }

	if (! json_is_array(loops)) {
		throwstream(runtime_error, "polygon body is not array");
    }

	vector<S2Loop *> loopv;
	try
	{
		for (size_t ii = 0; ii < json_array_size(loops); ++ii) {
			loopv.push_back(traverse_loop(json_array_get(loops, ii)));
        }
		
		return new S2Polygon(&loopv);
	}
	catch (...)
	{
		for (size_t ii = 0; ii < loopv.size(); ++ii) {
			delete loopv[ii];
        }
		throw;
	}
}

void process_point(GeoJSON::GeometryHandler & geohand, json_t * coord)
{
	geohand.handle_point(S2CellId::FromPoint(traverse_point(coord)));
}

void
process_polygon(GeoJSON::GeometryHandler & geohand, json_t * coord)
{
	if (! coord) {
		throwstream(runtime_error, "missing coordinates");
    }

	if (! json_is_array(coord)) {
		throwstream(runtime_error, "coordinates are not array");
    }

	S2Polygon * poly = traverse_polygon(coord);
	if (geohand.handle_region(poly)) {
		delete poly;
    }
}

void
process_multipolygon(GeoJSON::GeometryHandler & geohand, json_t * coord)
{
	if (! coord) {
		throwstream(runtime_error, "missing coordinates");
    }

	if (! json_is_array(coord)) {
		throwstream(runtime_error, "coordinates are not array");
    }

	auto_ptr<S2RegionUnion> regionsp(new S2RegionUnion);

	for (size_t ii = 0; ii < json_array_size(coord); ++ii) {
		regionsp->Add(traverse_polygon(json_array_get(coord, ii)));
    }

	if (! geohand.handle_region(regionsp.get())) {
		// Handler took ownership.
		regionsp.release();
    }
}

void
process_circle(GeoJSON::GeometryHandler & geohand, json_t * coord)
{
	// {
	//	   "type": "AeroCircle",
	//	   "coordinates": [[-122.097837, 37.421363], 1000.0]
	// }

	if (! coord) {
		throwstream(runtime_error, "missing coordinates");
    }

	if (! json_is_array(coord)) {
		throwstream(runtime_error, "coordinates are not array");
    }

	if (json_array_size(coord) != 2) {
		throwstream(runtime_error, "malformed circle coordinate array");
    }

	S2Point center = traverse_point(json_array_get(coord, 0));

	double radius;
	json_t * radiusobj = json_array_get(coord, 1);
	if (json_is_real(radiusobj)) {
		radius = json_real_value(radiusobj);
	}
	else if (json_is_integer(radiusobj)) {
		radius = double(json_integer_value(radiusobj));
	}
	else {
		throwstream(runtime_error, "radius not numeric value");
    }

	S1Angle angle = S1Angle::Radians(radius / geohand.earth_radius_meters());

	auto_ptr<S2Cap> capp(S2Cap::FromAxisAngle(center, angle).Clone());

	if (! geohand.handle_region(capp.get())) {
		// Handler took ownership.
		capp.release();
    }
}

void traverse_geometry(GeoJSON::GeometryHandler & geohand, json_t * geom)
{
	if (! geom) {
		throwstream(runtime_error, "missing geometry element");
    }

	if (! json_is_object(geom)) {
		throwstream(runtime_error, "geometry is not object");
    }

	json_t * type = json_object_get(geom, "type");
	if (! type) {
		throwstream(runtime_error, "missing geometry type");
    }
	
	if (! json_is_string(type)) {
		throwstream(runtime_error, "geometry type is not string");
    }

	string typestr(json_string_value(type));
	if (typestr == "Point") {
		process_point(geohand, json_object_get(geom, "coordinates"));
    }
	else if (typestr == "Polygon") {
		process_polygon(geohand, json_object_get(geom, "coordinates"));
    }
	else if (typestr == "MultiPolygon") {
		process_multipolygon(geohand, json_object_get(geom, "coordinates"));
    }
	else if (typestr == "AeroCircle") {
		process_circle(geohand, json_object_get(geom, "coordinates"));
    }
	else {
		throwstream(runtime_error, "unknown geometry type: " << typestr);
    }
}

} // end namespace

namespace GeoJSON {

void GeometryHandler::handle_point(S2CellId const & i_cellid)
{
	// nothing by default
}

bool GeometryHandler::handle_region(S2Region * i_regionp)
{
	// By default, caller should delete the region.
	return true;
}

void parse(GeometryHandler & geohand, string const & geostr)
{
	json_error_t err;
	Scoped<json_t *> geojson(json_loadb(geostr.data(), geostr.size(), 0, &err),
							 NULL, json_decref);
	if (! geojson) {
		throwstream(runtime_error, "failed to parse geojson: "
					<< err.line << ": " << err.text);
    }

	geohand.set_json(geojson);

	if (! json_is_object(geojson)) {
		throwstream(runtime_error, "top level geojson element not object");
    }
	
	json_t * type = json_object_get(geojson, "type");
	if (! type) {
		throwstream(runtime_error, "missing top-level type in geojson element");
    }
	
	if (! json_is_string(type)) {
		throwstream(runtime_error, "top-level type is not string");
    }

	string typestr(json_string_value(type));
	if (typestr == "Feature") {
		traverse_geometry(geohand, json_object_get(geojson, "geometry"));
    }
	else if (typestr == "Point") {
		process_point(geohand, json_object_get(geojson, "coordinates"));
    }
	else if (typestr == "Polygon") {
		process_polygon(geohand, json_object_get(geojson, "coordinates"));
    }
	else if (typestr == "MultiPolygon") {
		process_multipolygon(geohand, json_object_get(geojson, "coordinates"));
    }
	else if (typestr == "AeroCircle") {
		process_circle(geohand, json_object_get(geojson, "coordinates"));
    }
	else {
		throwstream(runtime_error, "unknown top-level type: " << typestr);
    }
}

} // end namespace GeoJSON
