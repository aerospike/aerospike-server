/*
 * hist.h
 *
 * Copyright (C) 2009-2016 Aerospike, Inc.
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

#include "cf_mutex.h"
#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

#define N_BUCKETS (1 + 64)
#define HISTOGRAM_NAME_SIZE 512

typedef enum {
	HIST_MILLISECONDS,
	HIST_MICROSECONDS,
	HIST_SIZE,
	HIST_COUNT,
	HIST_SCALE_MAX_PLUS_1
} histogram_scale;

#define HIST_TAG_MILLISECONDS	"msec"
#define HIST_TAG_MICROSECONDS	"usec"
#define HIST_TAG_SIZE			"bytes"
#define HIST_TAG_COUNT			"count"

// DO NOT access this member data directly - use the API!
// (Except for cf_hist_track, for which histogram is a base class.)
typedef struct histogram_s {
	char name[HISTOGRAM_NAME_SIZE];
	const char* scale_tag;
	uint32_t time_div;
	cf_mutex info_lock;
	char* info_snapshot;
	uint64_t counts[N_BUCKETS];
} histogram;


//==========================================================
// Public API.
//

histogram *histogram_create(const char *name, histogram_scale scale);
void histogram_clear(histogram *h);
void histogram_dump(histogram *h );

uint64_t histogram_insert_data_point(histogram *h, uint64_t start_ns);
void histogram_insert_raw(histogram *h, uint64_t value);
void histogram_insert_raw_unsafe(histogram *h, uint64_t value);

void histogram_save_info(histogram *h);
void histogram_get_info(histogram *h, cf_dyn_buf *db);
