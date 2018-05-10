/*
 * linear_hist.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include <stdint.h>

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

typedef struct linear_hist_s linear_hist;

typedef enum {
	LINEAR_HIST_SECONDS,
	LINEAR_HIST_SIZE,
	LINEAR_HIST_SCALE_MAX_PLUS_1
} linear_hist_scale;

typedef struct linear_hist_threshold_s {
	uint32_t value;
	uint32_t bucket_index;
	uint32_t bucket_width;
	uint64_t bucket_count;
	uint64_t target_count;
} linear_hist_threshold;


//==========================================================
// Public API.
//

// These must all be called from the same thread!

linear_hist *linear_hist_create(const char *name, linear_hist_scale scale, uint32_t start, uint32_t max_offset, uint32_t num_buckets);
void linear_hist_destroy(linear_hist *h);
void linear_hist_reset(linear_hist *h, uint32_t start, uint32_t max_offset, uint32_t num_buckets);
void linear_hist_clear(linear_hist *h, uint32_t start, uint32_t max_offset);

uint64_t linear_hist_get_total(linear_hist *h);
void linear_hist_merge(linear_hist *h1, linear_hist *h2);
void linear_hist_insert_data_point(linear_hist *h, uint32_t point);
uint64_t linear_hist_get_threshold_for_fraction(linear_hist *h, uint32_t tenths_pct, linear_hist_threshold *p_threshold);
uint64_t linear_hist_get_threshold_for_subtotal(linear_hist *h, uint64_t subtotal, linear_hist_threshold *p_threshold);

void linear_hist_dump(linear_hist *h);
void linear_hist_save_info(linear_hist *h);

// This call is thread-safe.
void linear_hist_get_info(linear_hist *h, cf_dyn_buf *db);
