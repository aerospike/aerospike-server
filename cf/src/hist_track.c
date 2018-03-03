/*
 * hist_track.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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

/*
 * A histogram with cached data.
 */


//==========================================================
// Includes
//

#include "hist_track.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/alloc.h>

#include "dynbuf.h"
#include "fault.h"
#include "hist.h"


//==========================================================
// Private "Class Members"
//

//------------------------------------------------
// Constants
//

// More than one day of 10 second slices uses too much memory.
const uint32_t MAX_NUM_ROWS = (24 * 60 * 60) / 10;

// Caching this few is legal but silly.
const uint32_t MIN_NUM_ROWS = 2;

// Don't track/report thresholds with a larger bucket index than this.
// This corresponds to the 32 second threshold - that should be big enough.
#define MAX_BUCKET 15

// Don't track/report more than this many thresholds.
// This could in principle be less than (MAX_BUCKET + 1), e.g. it could be
// 4, and we could track buckets 0, 5, 10, 15.
#define MAX_NUM_COLS (MAX_BUCKET + 1)

#define DEFAULT_NUM_COLS 3
const uint32_t default_buckets[DEFAULT_NUM_COLS] = { 0, 3, 6 };
// For our standard latency histograms, 0: >1ms, 3: >8ms, 6: >64ms.

// No output line can be longer than this.
#define MAX_FORMATTED_ROW_SIZE 512
#define MAX_FORMATTED_SETTINGS_SIZE 512

//------------------------------------------------
// Data
//

typedef struct row_s {
	uint32_t		timestamp;
	uint64_t		total;
	uint64_t		overs[];
} row;

struct cf_hist_track_s {
	// Base Histogram (must be first)
	histogram		hist;

	// Tracking-related
	row*			rows;
	size_t			row_size;
	uint32_t		num_rows;
	uint32_t		write_row_n;
	uint32_t		oldest_row_n;
	pthread_mutex_t	rows_lock;
	uint32_t		slice_sec;
	uint32_t		buckets[MAX_NUM_COLS];
	uint32_t		num_cols;
};

//------------------------------------------------
// Function Declarations
//

static inline row* get_row(cf_hist_track* this, uint32_t row_n);
static uint32_t get_start_row_n(cf_hist_track* this, uint32_t back_sec);
static void output_header(cf_hist_track* this, uint32_t start_ts,
		uint32_t num_cols, cf_hist_track_info_format info_fmt,
		cf_dyn_buf* db_p);
static void output_slice(cf_hist_track* this, row* prev_row_p, row* row_p,
		uint32_t diff_sec, uint32_t num_cols,
		cf_hist_track_info_format info_fmt, cf_dyn_buf* db_p);
static int threshold_to_bucket(int threshold);
static int thresholds_to_buckets(const char* thresholds, uint32_t buckets[]);


//==========================================================
// Public API
//

//------------------------------------------------
// Create a cf_hist_track object.
//
cf_hist_track*
cf_hist_track_create(const char* name, histogram_scale scale)
{
	cf_assert(name, AS_INFO, "null histogram name");
	cf_assert(strlen(name) < HISTOGRAM_NAME_SIZE, AS_INFO,
			"bad histogram name %s", name);
	cf_assert(scale >= 0 && scale < HIST_SCALE_MAX_PLUS_1, AS_INFO,
			"bad histogram scale %d", scale);

	cf_hist_track* this = (cf_hist_track*)cf_malloc(sizeof(cf_hist_track));

	pthread_mutex_init(&this->rows_lock, NULL);

	// Base histogram setup, same as in histogram_create():
	strcpy(this->hist.name, name);
	memset((void*)this->hist.counts, 0, sizeof(this->hist.counts));

	// If cf_hist_track_insert_data_point() is called for a size or count
	// histogram, the divide by 0 will crash - consider that a high-performance
	// assert.

	switch (scale) {
	case HIST_MILLISECONDS:
		this->hist.scale_tag = HIST_TAG_MILLISECONDS;
		this->hist.time_div = 1000 * 1000;
		break;
	case HIST_MICROSECONDS:
		this->hist.scale_tag = HIST_TAG_MICROSECONDS;
		this->hist.time_div = 1000;
		break;
	case HIST_SIZE:
		this->hist.scale_tag = HIST_TAG_SIZE;
		this->hist.time_div = 0;
		break;
	case HIST_COUNT:
		this->hist.scale_tag = HIST_TAG_COUNT;
		this->hist.time_div = 0;
		break;
	default:
		cf_crash(AS_INFO, "%s: unrecognized histogram scale %d", name, scale);
		break;
	}

	// Start with tracking off.
	this->rows = NULL;

	return this;
}

//------------------------------------------------
// Destroy a cf_hist_track object.
//
void
cf_hist_track_destroy(cf_hist_track* this)
{
	cf_hist_track_stop(this);
	pthread_mutex_destroy(&this->rows_lock);
	cf_free(this);
}

//------------------------------------------------
// Start tracking. May call this again without
// first calling cf_hist_track_disable() to use
// different caching parameters, but previous
// cache is lost.
//
// TODO - resolve errors ???
bool
cf_hist_track_start(cf_hist_track* this, uint32_t back_sec, uint32_t slice_sec,
		const char* thresholds)
{
	if (slice_sec == 0) {
		return false;
	}

	uint32_t num_rows = back_sec / slice_sec;

	// Check basic sanity of row-related parameters.
	if (num_rows > MAX_NUM_ROWS || num_rows < MIN_NUM_ROWS) {
		return false;
	}

	// If thresholds aren't specified, use defaults.
	uint32_t* buckets = (uint32_t*)default_buckets;
	int num_cols = DEFAULT_NUM_COLS;

	// Parse non-default thresholds and check resulting buckets.
	uint32_t parsed_buckets[MAX_NUM_COLS];

	if (thresholds) {
		buckets = parsed_buckets;
		num_cols = thresholds_to_buckets(thresholds, buckets);

		if (num_cols < 0) {
			return false;
		}
	}

	pthread_mutex_lock(&this->rows_lock);

	if (this->rows) {
		cf_free(this->rows);
	}

	this->row_size = sizeof(row) + (num_cols * sizeof(uint64_t));
	this->rows = (row*)cf_malloc(num_rows * this->row_size);
	this->num_rows = num_rows;
	this->write_row_n = 0;
	this->oldest_row_n = 0;
	this->slice_sec = slice_sec;

	for (int i = 0; i < num_cols; i++) {
		this->buckets[i] = buckets[i];
	}

	this->num_cols = (uint32_t)num_cols;

	pthread_mutex_unlock(&this->rows_lock);

	return true;
}

//------------------------------------------------
// Stop tracking, freeing cache.
//
void
cf_hist_track_stop(cf_hist_track* this)
{
	pthread_mutex_lock(&this->rows_lock);

	if (this->rows) {
		cf_free(this->rows);
		this->rows = NULL;
	}

	pthread_mutex_unlock(&this->rows_lock);
}

//------------------------------------------------
// Clear histogram buckets, and if tracking, stop.
// Must call cf_hist_track_enable() to start
// tracking again.
//
void
cf_hist_track_clear(cf_hist_track* this)
{
	cf_hist_track_stop(this);
	histogram_clear((histogram*)this);
}

//------------------------------------------------
// Print all non-zero histogram buckets, and if
// tracking, cache timestamp, total data points,
// and threshold data.
//
void
cf_hist_track_dump(cf_hist_track* this)
{
	// Always print the histogram.
	histogram_dump((histogram*)this);

	// If tracking is enabled, save a row in the cache.
	pthread_mutex_lock(&this->rows_lock);

	if (! this->rows) {
		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	uint32_t now_ts = (uint32_t)time(NULL);

	// But don't save row if slice_sec hasn't elapsed since last saved row.
	if (this->write_row_n != 0 &&
			now_ts - get_row(this, this->write_row_n - 1)->timestamp <
			this->slice_sec) {
		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	row* row_p = get_row(this, this->write_row_n);

	// "Freeze" the histogram for consistency of total.
	uint64_t counts[N_BUCKETS];
	uint64_t total_count = 0;

	for (int j = 0; j < N_BUCKETS; j++) {
		counts[j] = cf_atomic64_get(this->hist.counts[j]);
		total_count += counts[j];
	}

	uint64_t subtotal = 0;

	// b's "over" is total minus sum of values in all buckets 0 thru b.
	for (int i = 0, b = 0; i < this->num_cols; b++) {
		subtotal += counts[b];

		if (this->buckets[i] == b) {
			row_p->overs[i++] = total_count - subtotal;
		}
	}

	row_p->total = total_count;
	row_p->timestamp = now_ts;

	// Increment the current and oldest row indexes.
	this->write_row_n++;

	if (this->write_row_n > this->num_rows) {
		this->oldest_row_n++;
	}

	pthread_mutex_unlock(&this->rows_lock);
}

//------------------------------------------------
// Pass-through to base histogram.
//
uint64_t
cf_hist_track_insert_data_point(cf_hist_track* this, uint64_t start_ns)
{
	return histogram_insert_data_point((histogram*)this, start_ns);
}

//------------------------------------------------
// Pass-through to base histogram.
//
void
cf_hist_track_insert_raw(cf_hist_track* this, uint64_t value)
{
	histogram_insert_raw((histogram*)this, value);
}

//------------------------------------------------
// Get time-sliced info from cache.
//
void
cf_hist_track_get_info(cf_hist_track* this, uint32_t back_sec,
		uint32_t duration_sec, uint32_t slice_sec, bool throughput_only,
		cf_hist_track_info_format info_fmt, cf_dyn_buf* db_p)
{
	pthread_mutex_lock(&this->rows_lock);

	if (! this->rows) {
		cf_dyn_buf_append_string(db_p, "error-not-tracking;");
		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	uint32_t start_row_n = get_start_row_n(this, back_sec);

	if (start_row_n == -1) {
		cf_dyn_buf_append_string(db_p, "error-no-data-yet-or-back-too-small;");
		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	uint32_t num_cols = throughput_only ? 0 : this->num_cols;
	row* prev_row_p = get_row(this, start_row_n);

	output_header(this, prev_row_p->timestamp, num_cols, info_fmt, db_p);

	if (slice_sec == 0) {
		row* row_p = get_row(this, this->write_row_n - 1);
		uint32_t diff_sec = row_p->timestamp - prev_row_p->timestamp;

		output_slice(this, prev_row_p, row_p, diff_sec, num_cols, info_fmt,
				db_p);

		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	uint32_t start_ts = prev_row_p->timestamp;
	bool no_slices = true;

	for (uint32_t row_n = start_row_n + 1; row_n < this->write_row_n; row_n++) {
		row* row_p = get_row(this, row_n);

		uint32_t diff_sec = row_p->timestamp - prev_row_p->timestamp;

		if (diff_sec < slice_sec) {
			continue;
		}

		output_slice(this, prev_row_p, row_p, diff_sec, num_cols, info_fmt,
				db_p);
		no_slices = false;

		// Doing this at the end guarantees we get at least one slice.
		if (duration_sec != 0 && row_p->timestamp - start_ts > duration_sec) {
			break;
		}

		prev_row_p = row_p;
	}

	if (no_slices) {
		cf_dyn_buf_append_string(db_p,
				"error-slice-too-big-or-back-too-small;");
	}

	pthread_mutex_unlock(&this->rows_lock);
}

//------------------------------------------------
// Get current settings which were passed into
// cf_hist_track_start(), in format suitable for
// info_command_config_get().
//
void
cf_hist_track_get_settings(cf_hist_track* this, cf_dyn_buf* db_p)
{
	pthread_mutex_lock(&this->rows_lock);

	if (! this->rows) {
		pthread_mutex_unlock(&this->rows_lock);
		return;
	}

	const char* name = ((histogram*)this)->name;
	char output[MAX_FORMATTED_SETTINGS_SIZE];
	char* write_p = output;
	char* end_p = output + MAX_FORMATTED_SETTINGS_SIZE - 2;

	write_p += snprintf(output, MAX_FORMATTED_SETTINGS_SIZE - 2,
			"%s-hist-track-back=%u;"
			"%s-hist-track-slice=%u;"
			"%s-hist-track-thresholds=",
			name, this->num_rows * this->slice_sec,
			name, this->slice_sec,
			name);

	for (int i = 0; i < this->num_cols; i++) {
		write_p += snprintf(write_p, end_p - write_p, "%u,",
				(uint32_t)1 << this->buckets[i]);
	}

	if (this->num_cols > 0) {
		write_p--;
	}

	*write_p++ = ';';
	*write_p = 0;

	cf_dyn_buf_append_string(db_p, output);

	pthread_mutex_unlock(&this->rows_lock);
}


//==========================================================
// Private Functions
//

//------------------------------------------------
// Get row pointer for specified row count. Note
// that row_size is determined dynamically, so we
// can't just do rows[i].
//
static inline row*
get_row(cf_hist_track* this, uint32_t row_n)
{
	return (row*)((uint8_t*)this->rows +
			((row_n % this->num_rows) * this->row_size));
}

//------------------------------------------------
// Find row at or after timestamp specified by
// back_sec.
//
static uint32_t
get_start_row_n(cf_hist_track* this, uint32_t back_sec)
{
	// Must be at least two rows to get a slice.
	if (this->write_row_n < 2) {
		return -1;
	}

	uint32_t now_ts = (uint32_t)time(NULL);

	// In case we call this with default back_sec (0) or back_sec more than UTC
	// epoch to now - start from the beginning.
	if (back_sec == 0 || back_sec >= now_ts) {
		return this->oldest_row_n;
	}

	uint32_t start_ts = now_ts - back_sec;

	// Find the most recent slice interval.
	uint32_t last_row_n = this->write_row_n - 1;
	uint32_t slice_sec = get_row(this, last_row_n)->timestamp -
			get_row(this, last_row_n - 1)->timestamp;

	// Use recent slice interval to guess how many rows back to look.
	uint32_t back_row_n = back_sec / slice_sec;
	uint32_t guess_row_n = last_row_n > back_row_n ?
			last_row_n - back_row_n : 0;

	if (guess_row_n < this->oldest_row_n) {
		guess_row_n = this->oldest_row_n;
	}

	// Begin at guessed row, and iterate to find exact row to start at.
	uint32_t guess_ts = get_row(this, guess_row_n)->timestamp;
	uint32_t start_row_n;

	if (guess_ts < start_ts) {
		for (start_row_n = guess_row_n + 1; start_row_n < last_row_n;
				start_row_n++) {
			if (get_row(this, start_row_n)->timestamp >= start_ts) {
				break;
			}
		}
	}
	else if (guess_ts > start_ts) {
		for (start_row_n = guess_row_n; start_row_n > this->oldest_row_n;
				start_row_n--) {
			if (get_row(this, start_row_n - 1)->timestamp < start_ts) {
				break;
			}
		}
	}
	else {
		start_row_n = guess_row_n;
	}

	// Make sure when default query is run (e.g. latency:), we return at least
	// valid last data instead of returning an error. This case happens when the
	// query is timed such that it's right when histogram is being dumped.
	if (start_row_n == last_row_n) {
		start_row_n = last_row_n - 1;
	}

	// Can't get a slice if there isn't at least one row after the start row.
	return start_row_n < last_row_n ? start_row_n : -1;
}

//------------------------------------------------
// Make info "header" and append it to db_p.
//
static void
output_header(cf_hist_track* this, uint32_t start_ts, uint32_t num_cols,
		cf_hist_track_info_format info_fmt, cf_dyn_buf* db_p)
{
	cf_dyn_buf_append_string(db_p, ((histogram*)this)->name);

	const char* time_fmt;
	const char* rate_fmt;
	const char* pcts_fmt;
	char line_sep;

	switch (info_fmt) {
	case CF_HIST_TRACK_FMT_PACKED:
	default:
		time_fmt = ":%T-GMT";
		rate_fmt = ",ops/sec";
		pcts_fmt = ",>%ums";
		line_sep = ';';
		break;
	case CF_HIST_TRACK_FMT_TABLE:
		time_fmt = ":\n%T GMT       % > (ms)";
		rate_fmt = "\n   to      ops/sec";
		pcts_fmt = " %6u";
		line_sep = '\n';
		break;
	}

	char output[MAX_FORMATTED_ROW_SIZE];
	char* write_p = output;
	char* end_p = output + MAX_FORMATTED_ROW_SIZE - 2;
	time_t start_ts_time_t = (time_t)start_ts;
	struct tm start_tm;

	gmtime_r(&start_ts_time_t, &start_tm);
	write_p += strftime(output, MAX_FORMATTED_ROW_SIZE - 2, time_fmt, &start_tm);
	write_p += snprintf(write_p, end_p - write_p, "%s", rate_fmt);

	for (int i = 0; i < num_cols; i++) {
		write_p += snprintf(write_p, end_p - write_p, pcts_fmt,
				(uint32_t)(1 << this->buckets[i]));
	}

	*write_p++ = line_sep;
	*write_p = 0;

	cf_dyn_buf_append_string(db_p, output);
}

//------------------------------------------------
// Calculate output info for slice defined by two
// rows, and append to db_p.
//
static void
output_slice(cf_hist_track* this, row* prev_row_p, row* row_p,
		uint32_t diff_sec, uint32_t num_cols,
		cf_hist_track_info_format info_fmt, cf_dyn_buf* db_p)
{
	const char* time_fmt;
	const char* rate_fmt;
	const char* pcts_fmt;
	char line_sep;

	switch (info_fmt) {
	case CF_HIST_TRACK_FMT_PACKED:
	default:
		time_fmt = "%T";
		rate_fmt = ",%.1f";
		pcts_fmt = ",%.2f";
		line_sep = ';';
		break;
	case CF_HIST_TRACK_FMT_TABLE:
		time_fmt = "%T";
		rate_fmt = " %9.1f";
		pcts_fmt = " %6.2f";
		line_sep = '\n';
		break;
	}

	char output[MAX_FORMATTED_ROW_SIZE];
	char* write_p = output;
	char* end_p = output + MAX_FORMATTED_ROW_SIZE - 2;
	time_t row_ts_time_t = (time_t)row_p->timestamp;
	struct tm row_tm;

	gmtime_r(&row_ts_time_t, &row_tm);
	write_p += strftime(output, MAX_FORMATTED_ROW_SIZE - 2, time_fmt, &row_tm);

	uint64_t diff_total = row_p->total - prev_row_p->total;
	double ops_per_sec = (double)(diff_total) / diff_sec;

	write_p += snprintf(write_p, end_p - write_p, rate_fmt, ops_per_sec);

	for (int i = 0; i < num_cols; i++) {
		// We "freeze" the histogram to calculate "overs", so it shouldn't be
		// possible for an "over" to be less than the one in the previous row.
		uint64_t diff_overs = row_p->overs[i] - prev_row_p->overs[i];
		double pcts_over_i = diff_total != 0 ?
				(double)(diff_overs * 100) / diff_total : 0;

		write_p += snprintf(write_p, end_p - write_p, pcts_fmt, pcts_over_i);
	}

	*write_p++ = line_sep;
	*write_p = 0;

	cf_dyn_buf_append_string(db_p, output);
}

//------------------------------------------------
// Convert threshold milliseconds to bucket index.
//
static int
threshold_to_bucket(int threshold)
{
	if (threshold < 1) {
		return -1;
	}

	int n = threshold;
	int b = 0;

	while (n > 1) {
		n >>= 1;
		b++;
	}

	// Check that threshold is an exact power of 2.
	return (1 << b) == threshold ? b : -1;
}

//------------------------------------------------
// Convert thresholds string to buckets array.
//
static int
thresholds_to_buckets(const char* thresholds, uint32_t buckets[])
{
	// Copy since strtok() is destructive.
	char toks[strlen(thresholds) + 1];

	strcpy(toks, thresholds);

	int i = 0;
	char* tok = strtok(toks, ",");

	while (tok) {
		if (i == MAX_NUM_COLS) {
			return -1;
		}

		int b = threshold_to_bucket(atoi(tok));

		// Make sure it's a rising sequence of valid bucket indexes.
		if (b < 0 || b > MAX_BUCKET || (i > 0 && b <= buckets[i - 1])) {
			return -1;
		}

		buckets[i++] = (uint32_t)b;

		tok = strtok(NULL, ",");
	}

	return i;
}
