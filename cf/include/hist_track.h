/*
 * hist_track.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stdint.h>
#include "dynbuf.h"
#include "hist.h"


//==========================================================
// Typedefs
//

typedef struct cf_hist_track_s cf_hist_track;

typedef enum {
	CF_HIST_TRACK_FMT_PACKED,
	CF_HIST_TRACK_FMT_TABLE
} cf_hist_track_info_format;


//==========================================================
// Public API
//

//------------------------------------------------
// Constructor/Destructor
//
cf_hist_track* cf_hist_track_create(const char* name, histogram_scale scale);
void cf_hist_track_destroy(cf_hist_track* _this);

//------------------------------------------------
// Start/Stop Caching Data
//
bool cf_hist_track_start(cf_hist_track* _this, uint32_t back_sec,
		uint32_t slice_sec, const char* thresholds);
void cf_hist_track_stop(cf_hist_track* _this);

//------------------------------------------------
// Histogram API "Overrides"
//
void cf_hist_track_clear(cf_hist_track* _this);
void cf_hist_track_dump(cf_hist_track* _this);

// These are just pass-throughs to histogram insertion methods:
uint64_t cf_hist_track_insert_data_point(cf_hist_track* _this,
		uint64_t start_ns);
void cf_hist_track_insert_raw(cf_hist_track* _this, uint64_t value);

//------------------------------------------------
// Get Statistics from Cached Data
//
void cf_hist_track_get_info(cf_hist_track* _this, uint32_t back_sec,
		uint32_t duration_sec, uint32_t slice_sec, bool throughput_only,
		cf_hist_track_info_format info_fmt, cf_dyn_buf* db_p);

//------------------------------------------------
// Get Current Settings
//
void cf_hist_track_get_settings(cf_hist_track* _this, cf_dyn_buf* db_p);
