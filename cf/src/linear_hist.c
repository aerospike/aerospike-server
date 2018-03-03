/*
 * linear_hist.c
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


//==========================================================
// Includes.
//

#include "linear_hist.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "dynbuf.h"
#include "fault.h"


//==========================================================
// Private class data.
//

#define LINEAR_HIST_NAME_SIZE 512
#define INFO_SNAPSHOT_SIZE 2048

struct linear_hist_s {
	char name[LINEAR_HIST_NAME_SIZE];

	pthread_mutex_t info_lock;
	char info_snapshot[INFO_SNAPSHOT_SIZE];

	uint32_t num_buckets;
	uint64_t *counts;

	uint32_t start;
	uint32_t bucket_width;
};


//==========================================================
// Public API.
//

//------------------------------------------------
// Create a linear histogram.
//
linear_hist*
linear_hist_create(const char *name, uint32_t start, uint32_t max_offset,
		uint32_t num_buckets)
{
	if (! (name && strlen(name) < LINEAR_HIST_NAME_SIZE)) {
		cf_crash(AS_INFO, "linear_hist_create - bad name %s",
				name ? name : "<null>");
	}

	if (start + max_offset < start) {
		cf_crash(AS_INFO, "linear_hist_create - max_offset overflow");
	}

	if (num_buckets == 0) {
		cf_crash(AS_INFO, "linear_hist_create - 0 num_buckets");
	}

	linear_hist *h = cf_malloc(sizeof(linear_hist));

	strcpy(h->name, name);

	if (0 != pthread_mutex_init(&h->info_lock, NULL)) {
		cf_crash(AS_INFO, "linear_hist_create - mutex init failed");
	}

	h->info_snapshot[0] = 0;

	h->num_buckets = num_buckets;
	h->counts = cf_malloc(sizeof(uint64_t) * num_buckets);

	linear_hist_clear(h, start, max_offset);

	return h;
}

//------------------------------------------------
// Destroy a linear histogram.
//
void
linear_hist_destroy(linear_hist *h)
{
	pthread_mutex_destroy(&h->info_lock);
	cf_free(h->counts);
	cf_free(h);
}

//------------------------------------------------
// Clear, re-scale/re-size a linear histogram.
//
void
linear_hist_reset(linear_hist *h, uint32_t start, uint32_t max_offset,
		uint32_t num_buckets)
{
	if (h->num_buckets == num_buckets) {
		linear_hist_clear(h, start, max_offset);
		return;
	}

	h->num_buckets = num_buckets;
	h->counts = cf_realloc(h->counts, sizeof(uint64_t) * num_buckets);
	linear_hist_clear(h, start, max_offset);
}

//------------------------------------------------
// Clear and (re-)scale a linear histogram.
//
void
linear_hist_clear(linear_hist *h, uint32_t start, uint32_t max_offset)
{
	h->start = start;
	h->bucket_width = (max_offset + (h->num_buckets - 1)) / h->num_buckets;

	// Only needed to protect against max_offset 0.
	if (h->bucket_width == 0) {
		h->bucket_width = 1;
	}

	memset((void *)h->counts, 0, sizeof(uint64_t) * h->num_buckets);
}

//------------------------------------------------
// Access method for total count.
//
uint64_t
linear_hist_get_total(linear_hist *h)
{
	uint64_t total_count = 0;

	for (uint32_t i = 0; i < h->num_buckets; i++) {
		total_count += h->counts[i];
	}

	return total_count;
}

//------------------------------------------------
// Merge h2 into h1.
//
void
linear_hist_merge(linear_hist *h1, linear_hist *h2)
{
	if (! (h1->num_buckets == h2->num_buckets && h1->start == h2->start &&
			h1->bucket_width == h2->bucket_width)) {
		cf_crash(AS_INFO, "linear_hist_merge - dissimilar histograms");
	}

	for (uint32_t i = 0; i < h1->num_buckets; i++) {
		h1->counts[i] += h2->counts[i];
	}
}

//------------------------------------------------
// Insert a data point. Points out of range will
// end up in the bucket at the appropriate end.
//
void
linear_hist_insert_data_point(linear_hist *h, uint32_t point)
{
	int32_t offset = (int32_t)(point - h->start);
	int32_t bucket = 0;

	if (offset > 0) {
		bucket = offset / h->bucket_width;

		if (bucket >= (int32_t)h->num_buckets) {
			bucket = h->num_buckets - 1;
		}
	}

	h->counts[bucket]++;
}

//------------------------------------------------
// Get the low edge of the "threshold" bucket -
// the bucket in which the specified percentage of
// total count is exceeded (accumulating from low
// bucket).
//
uint64_t
linear_hist_get_threshold_for_fraction(linear_hist *h, uint32_t tenths_pct,
		linear_hist_threshold *p_threshold)
{
	return linear_hist_get_threshold_for_subtotal(h,
			(linear_hist_get_total(h) * (uint64_t)tenths_pct) / 1000,
			p_threshold);
}

//------------------------------------------------
// Get the low edge of the "threshold" bucket -
// the bucket in which the specified subtotal
// count is exceeded (accumulating from low
// bucket).
//
uint64_t
linear_hist_get_threshold_for_subtotal(linear_hist *h, uint64_t subtotal,
		linear_hist_threshold *p_threshold)
{
	p_threshold->bucket_width = h->bucket_width;
	p_threshold->target_count = subtotal;

	uint64_t count = 0;
	uint32_t i;

	for (i = 0; i < h->num_buckets; i++) {
		count += h->counts[i];

		if (count > subtotal) {
			break;
		}
	}

	if (i == h->num_buckets) {
		// This means subtotal >= h->total_count.
		p_threshold->value = 0xFFFFffff;
		p_threshold->bucket_index = 0; // irrelevant
		p_threshold->bucket_count = 0; // irrelevant
		return count;
	}

	p_threshold->value = h->start + (i * h->bucket_width);
	p_threshold->bucket_index = i;
	p_threshold->bucket_count = h->counts[i];

	// Return subtotal of everything below "threshold" bucket.
	return count - h->counts[i];
}

//------------------------------------------------
// Dump a linear histogram to log.
//
// Note - DO NOT change the log output format in
// this method - public documentation assumes this
// format.
//
void
linear_hist_dump(linear_hist *h)
{
	uint32_t i = h->num_buckets;
	uint32_t j = 0;
	uint32_t k = 0;
	uint64_t total_count = 0;

	for (uint32_t b = 0; b < h->num_buckets; b++) {
		if (h->counts[b] != 0) {
			if (i > b) {
				i = b;
			}

			j = b;
			k++;
			total_count += h->counts[b];
		}
	}

	char buf[100];
	int pos = 0;
	int n = 0;

	buf[0] = '\0';

	cf_debug(AS_NSUP, "linear histogram dump: %s [%u %u]/[%u] (%lu total)",
			h->name, h->start, h->start + (h->num_buckets * h->bucket_width),
			h->bucket_width, total_count);

	if (k > 100) {
		// For now, just don't bother if there's too much to dump.
		cf_debug(AS_NSUP, "... (%u buckets with non-zero count)", k);
		return;
	}

	for ( ; i <= j; i++) {
		if (h->counts[i] == 0) { // print only non-zero columns
			continue;
		}

		int bytes = sprintf(buf + pos, " (%02u: %010lu)", i, h->counts[i]);

		if (bytes <= 0) {
			cf_debug(AS_NSUP, "linear histogram dump error");
			return;
		}

		pos += bytes;

		if ((n & 3) == 3) { // maximum of 4 printed columns per log line
			 cf_debug(AS_NSUP, "%s", buf);
			 pos = 0;
			 buf[0] = '\0';
		}

		n++;
	}

	if (pos > 0) {
		cf_debug(AS_NSUP, "%s", buf);
	}
}

//------------------------------------------------
// Save a linear histogram "snapshot".
//
void
linear_hist_save_info(linear_hist *h)
{
	pthread_mutex_lock(&h->info_lock);

	if (h->num_buckets > 100) {
		// For now, just don't bother if there's too much to save.
		sprintf(h->info_snapshot, "%u,%u ...", h->num_buckets, h->bucket_width);

		pthread_mutex_unlock(&h->info_lock);
		return;
	}

	// Write num_buckets, the bucket width, and the first bucket's count.
	int i = 0;
	int pos = snprintf(h->info_snapshot, INFO_SNAPSHOT_SIZE, "%u,%u,%lu",
			h->num_buckets, h->bucket_width, h->counts[i++]);

	while (pos < INFO_SNAPSHOT_SIZE && i < h->num_buckets) {
		pos += snprintf(h->info_snapshot + pos, INFO_SNAPSHOT_SIZE - pos,
				",%lu", h->counts[i++]);
	}

	pthread_mutex_unlock(&h->info_lock);
}

//------------------------------------------------
// Append a linear histogram "snapshot" to db.
//
void
linear_hist_get_info(linear_hist *h, cf_dyn_buf *db)
{
	pthread_mutex_lock(&h->info_lock);
	cf_dyn_buf_append_string(db, h->info_snapshot);
	pthread_mutex_unlock(&h->info_lock);
}
