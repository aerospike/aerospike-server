/*
 * hist.h
 *
 * Copyright (C) 2009-2020 Aerospike, Inc.
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
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

#define HISTOGRAM_NAME_SIZE 512

typedef enum {
	HIST_MILLISECONDS,
	HIST_MICROSECONDS,
	HIST_SIZE,
	HIST_COUNT,
	HIST_SCALE_MAX_PLUS_1
} histogram_scale;

typedef struct histogram_s histogram;


//==========================================================
// Public API.
//

histogram* histogram_create(const char* name, histogram_scale scale);
void histogram_clear(histogram* h);
void histogram_rescale(histogram* h, histogram_scale scale);
void histogram_dump(histogram* h );

uint64_t histogram_insert_data_point(histogram* h, uint64_t start_ns);
void histogram_insert_raw(histogram* h, uint64_t value);
void histogram_insert_raw_unsafe(histogram* h, uint64_t value);

void histogram_save_info(histogram* h);
void histogram_get_info(histogram* h, cf_dyn_buf* db);

void histogram_get_latencies(histogram* h, bool legacy, cf_dyn_buf* db);
