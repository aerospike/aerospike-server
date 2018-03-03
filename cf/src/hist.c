/*
 * hist.c
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include "hist.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "dynbuf.h"
#include "fault.h"


//==========================================================
// Histogram with logarithmic buckets.
//

//------------------------------------------------
// Create a histogram. There's no destroy(), but
// you can just cf_free() the histogram.
//
histogram*
histogram_create(const char *name, histogram_scale scale)
{
	cf_assert(name, AS_INFO, "null histogram name");
	cf_assert(strlen(name) < HISTOGRAM_NAME_SIZE, AS_INFO,
			"bad histogram name %s", name);
	cf_assert(scale >= 0 && scale < HIST_SCALE_MAX_PLUS_1, AS_INFO,
			"bad histogram scale %d", scale);

	histogram *h = cf_malloc(sizeof(histogram));

	strcpy(h->name, name);
	memset((void *)&h->counts, 0, sizeof(h->counts));

	// If histogram_insert_data_point() is called for a size or count histogram,
	// the divide by 0 will crash - consider that a high-performance assert.

	switch (scale) {
	case HIST_MILLISECONDS:
		h->scale_tag = HIST_TAG_MILLISECONDS;
		h->time_div = 1000 * 1000;
		break;
	case HIST_MICROSECONDS:
		h->scale_tag = HIST_TAG_MICROSECONDS;
		h->time_div = 1000;
		break;
	case HIST_SIZE:
		h->scale_tag = HIST_TAG_SIZE;
		h->time_div = 0;
		break;
	case HIST_COUNT:
		h->scale_tag = HIST_TAG_COUNT;
		h->time_div = 0;
		break;
	default:
		cf_crash(AS_INFO, "%s: unrecognized histogram scale %d", name, scale);
		break;
	}

	return h;
}

//------------------------------------------------
// Clear a histogram.
//
void
histogram_clear(histogram *h)
{
	for (int i = 0; i < N_BUCKETS; i++) {
		cf_atomic64_set(&h->counts[i], 0);
	}
}

//------------------------------------------------
// Dump a histogram to log.
//
// Note - DO NOT change the log output format in
// this method - tools such as as_log_latency
// assume this format.
//
void
histogram_dump(histogram *h)
{
	int b;
	uint64_t counts[N_BUCKETS];

	for (b = 0; b < N_BUCKETS; b++) {
		counts[b] = cf_atomic64_get(h->counts[b]);
	}

	int i = N_BUCKETS;
	int j = 0;
	uint64_t total_count = 0;

	for (b = 0; b < N_BUCKETS; b++) {
		if (counts[b] != 0) {
			if (i > b) {
				i = b;
			}

			j = b;
			total_count += counts[b];
		}
	}

	char buf[100];
	int pos = 0;
	int k = 0;

	buf[0] = '\0';

	cf_info(AS_INFO, "histogram dump: %s (%lu total) %s", h->name, total_count,
			h->scale_tag);

	for ( ; i <= j; i++) {
		if (counts[i] == 0) { // print only non-zero columns
			continue;
		}

		int bytes = sprintf(buf + pos, " (%02d: %010lu)", i, counts[i]);

		if (bytes <= 0) {
			cf_info(AS_INFO, "histogram dump error");
			return;
		}

		pos += bytes;

		if ((k & 3) == 3) { // maximum of 4 printed columns per log line
			 cf_info(AS_INFO, "%s", buf);
			 pos = 0;
			 buf[0] = '\0';
		}

		k++;
	}

	if (pos > 0) {
		cf_info(AS_INFO, "%s", buf);
	}
}

//------------------------------------------------
// BYTE_MSB[n] returns the position of the most
// significant bit. If no bits are set (n = 0) it
// returns 0. Otherwise the positions are 1 ... 8
// from low to high, so e.g. n = 13 returns 4:
//
//		bits:		0  0  0  0  1  1  0  1
//		position:	8  7  6  5 [4] 3  2  1
//
static const char BYTE_MSB[] = {
		0, 1, 2, 2, 3, 3, 3, 3,  4, 4, 4, 4, 4, 4, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5,  5, 5, 5, 5, 5, 5, 5, 5,
		6, 6, 6, 6, 6, 6, 6, 6,  6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6,  6, 6, 6, 6, 6, 6, 6, 6,

		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,

		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,

		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8
};

//------------------------------------------------
// Returns the position of the most significant
// bit of n. Positions are 1 ... 64 from low to
// high, so:
//
//		n			msb(n)
//		--------	------
//		0			0
//		1			1
//		2 ... 3		2
//		4 ... 7		3
//		8 ... 15	4
//		etc.
//
static int
msb(uint64_t n)
{
	int shift = 0;

	while (true) {
		uint64_t n_div_256 = n >> 8;

		if (n_div_256 == 0) {
			return shift + (int)BYTE_MSB[n];
		}

		n = n_div_256;
		shift += 8;
	}

	// Should never get here.
	cf_crash(AS_INFO, "end of msb()");
	return -1;
}

//------------------------------------------------
// Insert a time interval data point. The interval
// is time elapsed since start_ns, converted to
// milliseconds or microseconds as appropriate.
// Assumes start_ns was obtained via cf_getns()
// some time ago. Generates a histogram with
// either:
//
//		bucket	millisecond range
//		------	-----------------
//		0		0 to 1  (more exactly, 0.999999)
//		1		1 to 2  (more exactly, 1.999999)
//		2		2 to 4  (more exactly, 3.999999)
//		3		4 to 8  (more exactly, 7.999999)
//		4		8 to 16 (more exactly, 15.999999)
//		etc.
//
// or:
//
//		bucket	microsecond range
//		------	-----------------
//		0		0 to 1  (more exactly, 0.999)
//		1		1 to 2  (more exactly, 1.999)
//		2		2 to 4  (more exactly, 3.999)
//		3		4 to 8  (more exactly, 7.999)
//		4		8 to 16 (more exactly, 15.999)
//		etc.
//
uint64_t
histogram_insert_data_point(histogram *h, uint64_t start_ns)
{
	uint64_t end_ns = cf_getns();
	uint64_t delta_t = (end_ns - start_ns) / h->time_div;

	int bucket = 0;

	if (delta_t != 0) {
		bucket = msb(delta_t);

		if (start_ns > end_ns) {
			// Either the clock went backwards, or wrapped. (Assume the former,
			// since it takes ~580 years from 0 to wrap.)
			cf_warning(AS_INFO, "%s - clock went backwards: start %lu end %lu",
					h->name, start_ns, end_ns);
			bucket = 0;
		}
	}

	cf_atomic64_incr(&h->counts[bucket]);

	return end_ns;
}

//------------------------------------------------
// Insert a raw data point. Generates a histogram
// with:
//
//		bucket	value range
//		------	-----------
//		0		0
//		1		1
//		2		2, 3
//		3		4 to 7
//		4		8 to 15
//		etc.
//
void
histogram_insert_raw(histogram *h, uint64_t value)
{
	cf_atomic64_incr(&h->counts[msb(value)]);
}
