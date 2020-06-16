/*
 * hist.c
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

//==========================================================
// Includes.
//

#include "hist.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define N_BUCKETS (1 + 64)
#define N_LATENCY_COLS (1 + 16)

struct histogram_s {
	char name[HISTOGRAM_NAME_SIZE];
	histogram_scale scale;
	const char* scale_tag;
	uint32_t time_div;
	uint64_t counts[N_BUCKETS];

	// Size histogram related.
	cf_mutex info_lock;
	char* info_snapshot;

	// Tracking related.
	time_t timestamp;
	uint64_t total;
	uint64_t overs[N_LATENCY_COLS];
	bool read_a;
	char out_a[150];
	char out_b[150];

	// TODO - legacy, remove after 5.1.
	char legacy_out_a[80];
	char legacy_out_b[80];
};

#define HIST_TAG_MILLISECONDS	"msec"
#define HIST_TAG_MICROSECONDS	"usec"
#define HIST_TAG_SIZE			"bytes"
#define HIST_TAG_COUNT			"count"

// e.g. units=bytes:[128K-256K)=12345:[256K-512K)=54321 ... \0
#define SNAPSHOT_SIZE (5 + 1 + 5 + 1 + (N_BUCKETS * (1 + 11 + 1 + 20)) + 1)

static const char* SNAPSHOT_TAGS[] = {
		"0",
		"1",  "2",  "4",  "8",  "16",  "32",  "64",  "128",  "256",  "512",
		"1K", "2K", "4K", "8K", "16K", "32K", "64K", "128K", "256K", "512K",
		"1M", "2M", "4M", "8M", "16M", "32M", "64M", "128M", "256M", "512M",
		"1G", "2G", "4G", "8G", "16G", "32G", "64G", "128G", "256G", "512G",
		"1T", "2T", "4T", "8T", "16T", "32T", "64T", "128T", "256T", "512T",
		"1P", "2P", "4P", "8P", "16P", "32P", "64P", "128P", "256P", "512P",
		"1E", "2E", "4E", "8E", "inf"
};

COMPILER_ASSERT(sizeof(SNAPSHOT_TAGS) / sizeof(const char*) == N_BUCKETS + 1);

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


//==========================================================
// Inlines & macros.
//

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
static inline int
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


//==========================================================
// Public API.
//

//------------------------------------------------
// Create a histogram. There's no destroy(), but
// you can just cf_free() the histogram.
//
histogram*
histogram_create(const char* name, histogram_scale scale)
{
	cf_assert(name, AS_INFO, "null histogram name");
	cf_assert(strlen(name) < HISTOGRAM_NAME_SIZE, AS_INFO,
			"bad histogram name %s", name);
	cf_assert(scale >= 0 && scale < HIST_SCALE_MAX_PLUS_1, AS_INFO,
			"bad histogram scale %d", scale);

	histogram* h = cf_malloc(sizeof(histogram));

	strcpy(h->name, name);
	h->scale = scale;

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
		cf_crash(AS_INFO, "%s: invalid scale %d", name, scale);
		break;
	}

	memset((void*)h->counts, 0, sizeof(h->counts));

	cf_mutex_init(&h->info_lock);
	h->info_snapshot = NULL;

	h->timestamp = 0;
	h->total = 0;
	memset((void*)h->overs, 0, sizeof(h->overs));
	h->read_a = true;
	h->out_a[0] = '\0';
	h->out_b[0] = '\0';

	// TODO - legacy, remove after 5.1.
	h->legacy_out_a[0] = '\0';
	h->legacy_out_b[0] = '\0';

	return h;
}

//------------------------------------------------
// Clear a histogram.
//
void
histogram_clear(histogram* h)
{
	memset((void*)h->counts, 0, sizeof(h->counts));

	h->timestamp = 0;
	h->total = 0;
	memset((void*)h->overs, 0, sizeof(h->overs));
	h->read_a = true;
	h->out_a[0] = '\0';
	h->out_b[0] = '\0';

	// TODO - legacy, remove after 5.1.
	h->legacy_out_a[0] = '\0';
	h->legacy_out_b[0] = '\0';
}

//------------------------------------------------
// Dump a histogram to log.
//
// Note - DO NOT change the log output format in
// this method - tools such as as_log_latency
// assume this format.
//
void
histogram_dump(histogram* h)
{
	uint64_t counts[N_BUCKETS];
	uint32_t i = N_BUCKETS;
	uint32_t j = 0;
	uint64_t total = 0;
	uint64_t subtotals[N_BUCKETS];

	for (uint32_t b = 0; b < N_BUCKETS; b++) {
		counts[b] = as_load_uint64(&h->counts[b]);

		if (counts[b] != 0) {
			if (i > b) {
				i = b;
			}

			j = b;
			total += counts[b];
		}

		subtotals[b] = total;
	}

	char buf[200];
	int pos = 0;
	uint32_t k = 0;

	buf[0] = '\0';

	cf_info(AS_INFO, "histogram dump: %s (%lu total) %s", h->name, total,
			h->scale_tag);

	for ( ; i <= j; i++) {
		if (counts[i] == 0) { // print only non-zero columns
			continue;
		}

		pos += sprintf(buf + pos, " (%02u: %010lu)", i, counts[i]);

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

	// Tracking related.

	switch (h->scale) {
	case HIST_MILLISECONDS:
	case HIST_MICROSECONDS:
		break;
	case HIST_SIZE:
	case HIST_COUNT:
		return; // tracking is only for latency histograms
	default:
		cf_crash(AS_INFO, "%s: invalid scale %d", h->name, h->scale);
		break;
	}

	char* out = h->read_a ? h->out_b : h->out_a;
	char* legacy_out = h->read_a ? h->legacy_out_b : h->legacy_out_a;

	time_t now = time(NULL);
	time_t slice = now - h->timestamp;

	uint64_t diff_total = total - h->total;
	double tps = (double)diff_total / (double)slice;

	if (h->timestamp != 0) {
		out += sprintf(out, "%s,%.1f", h->scale_tag, tps);

		// TODO - legacy, remove after 5.1.

		struct tm start_tm;
		struct tm end_tm;

		gmtime_r(&h->timestamp, &start_tm);
		gmtime_r(&now, &end_tm);

		char start[32];
		char end[32];

		strftime(start, sizeof(start), "%T", &start_tm);
		strftime(end, sizeof(end), "%T", &end_tm);

		const char* thresholds = h->scale == HIST_MILLISECONDS ?
				">1ms,>8ms,>64ms" : ">1us,>8us,>64us";

		legacy_out += sprintf(legacy_out, "%s-GMT,ops/sec,%s;%s,%.1f", start,
				thresholds, end, tps);
	}

	// b's "over" is total minus sum of values in all buckets 0 thru b.
	for (uint32_t b = 0; b < N_LATENCY_COLS; b++) {
		uint64_t over = total - subtotals[b];

		uint64_t diff_overs = over - h->overs[b];
		double pct_over_b = diff_total != 0 ?
				(double)(diff_overs * 100) / (double)diff_total : 0;

		if (h->timestamp != 0) {
			out += sprintf(out, ",%.2f", pct_over_b);

			// TODO - legacy, remove after 5.1.
			if (b == 0 || b == 3 || b == 6) {
				legacy_out += sprintf(legacy_out, ",%.2f", pct_over_b);
			}
		}

		// Store for next time.
		h->overs[b] = over;
	}

	*out++ = ';';
	*out = '\0';

	// TODO - legacy, remove after 5.1.
	*legacy_out++ = ';';
	*legacy_out = '\0';

	// Store for next time.
	h->timestamp = now;
	h->total = total;

	h->read_a = ! h->read_a;
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
histogram_insert_data_point(histogram* h, uint64_t start_ns)
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

	as_incr_uint64(&h->counts[bucket]);

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
histogram_insert_raw(histogram* h, uint64_t value)
{
	as_incr_uint64(&h->counts[msb(value)]);
}

//------------------------------------------------
// Same as above, but not thread safe.
//
void
histogram_insert_raw_unsafe(histogram* h, uint64_t value)
{
	h->counts[msb(value)]++;
}

//------------------------------------------------
// Save a snapshot of this histogram. This should
// be done rarely, preferably from one thread.
//
void
histogram_save_info(histogram* h)
{
	cf_mutex_lock(&h->info_lock);

	// Allocate lazily so all histograms don't incur the ~2K penalty.
	if (h->info_snapshot == NULL) {
		h->info_snapshot = cf_malloc(SNAPSHOT_SIZE);
	}

	int prefix_len = sprintf(h->info_snapshot, "units=%s", h->scale_tag);
	char* at = h->info_snapshot + prefix_len;

	for (uint32_t b = 0; b < N_BUCKETS; b++) {
		uint64_t count = h->counts[b];

		if (count != 0) {
			at += sprintf(at, ":[%s-%s)=%lu", SNAPSHOT_TAGS[b],
					SNAPSHOT_TAGS[b + 1], count);
		}
	}

	cf_mutex_unlock(&h->info_lock);
}

//------------------------------------------------
// Retrieve a snapshot of this histogram.
//
void
histogram_get_info(histogram* h, cf_dyn_buf* db)
{
	cf_mutex_lock(&h->info_lock);

	cf_dyn_buf_append_string(db,
			h->info_snapshot != NULL ? h->info_snapshot : "");

	cf_mutex_unlock(&h->info_lock);
}

//------------------------------------------------
// Retrieve threshold info for one time-slice.
//
void
histogram_get_latencies(histogram* h, bool legacy, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, h->name);
	cf_dyn_buf_append_char(db, ':');

	if (legacy) {
		// TODO - legacy, remove after 5.1.
		cf_dyn_buf_append_string(db,
				h->read_a ? h->legacy_out_a : h->legacy_out_b);
	}
	else {
		cf_dyn_buf_append_string(db, h->read_a ? h->out_a : h->out_b);
	}
}
