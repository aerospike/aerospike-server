/*
 * zstd_wire.h
 *
 * Copyright (C) 2025 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "hist.h"

//==========================================================
// Constants.
//

// Minimum payload size worth wire-compressing per record. Below this a full
// zstd compress + ZSTD_compressBound allocation can't plausibly beat zstd's
// framing overhead, so the per-record paths (replica write, migration) skip the
// work entirely rather than compress-then-discard.
#define ZSTD_WIRE_COMPRESS_MIN_SZ 256

//==========================================================
// Public API.
//

typedef struct zstd_wire_stats_s {
	uint64_t make_patch_calls;
	uint64_t make_patch_cpu_ns;
	uint64_t make_patch_cpu_pct_x100;
	uint64_t apply_patch_calls;
	uint64_t apply_patch_cpu_ns;
	uint64_t apply_patch_cpu_pct_x100;
	uint64_t compress_calls;
	uint64_t compress_cpu_ns;
	uint64_t compress_cpu_pct_x100;
	uint64_t decompress_calls;
	uint64_t decompress_cpu_ns;
	uint64_t decompress_cpu_pct_x100;
	uint64_t cpu_pct_x100;
} zstd_wire_stats;

// Creates a binary patch using zstd compression with the old buffer as the
// reference dictionary. Caller must cf_free(*patch_buf) on success. Returns
// the patch size in bytes, or 0 on error. 'level' is -10..22 (negative values
// are zstd's "fast" levels; 10 recommended).
size_t zstd_wire_make_patch(void** patch_buf, const void* old_buf,
		size_t old_size, const void* new_buf, size_t new_size, int level);

// Applies a binary patch using zstd decompression with the old buffer as the
// reference dictionary. Caller must cf_free(*out_buf) on success. Returns the
// restored size in bytes, or 0 on error.
size_t zstd_wire_apply_patch(void** out_buf, const void* old_buf,
		size_t old_size, const void* patch_buf, size_t patch_size);

// Compresses a full buffer using zstd, no reference dictionary. Caller must
// cf_free(*out_buf) on success. Returns compressed size, or 0 on error.
// 'level' is -10..22 (negative values are zstd's "fast" levels; 10
// recommended).
size_t zstd_wire_compress_buffer(void** out_buf, const void* in_buf,
		size_t in_size, int level);

// Decompresses a full zstd-compressed buffer, no reference dictionary. Caller
// must cf_free(*out_buf) on success. Returns decompressed size, or 0 on error.
// The decompressed output is capped at the maximum record size (WBLOCK_SZ,
// 8 MiB); larger frames are rejected up-front and the streaming decoder will
// abort if the running total exceeds the cap. Use this for replica/migration
// payloads, which are always single-record sized.
size_t zstd_wire_decompress_buffer(void** out_buf, const void* in_buf,
		size_t in_size);

// Same as zstd_wire_decompress_buffer but with a caller-supplied cap. Use for
// payloads that are not record-sized — most notably SMD full-sync messages,
// which can grow well past WBLOCK_SZ as a cluster accumulates UDFs, sindexes,
// and roster entries. Returns 0 on error or if the decompressed output would
// exceed max_decompressed_size.
size_t zstd_wire_decompress_buffer_capped(void** out_buf, const void* in_buf,
		size_t in_size, size_t max_decompressed_size);

// Gate for the per-op CPU-time measurement (enable-benchmarks-wire-compression,
// default off). When off, the codec entry points skip the two
// clock_gettime(CLOCK_THREAD_CPUTIME_ID) syscalls - not vDSO-served on Linux
// x86-64 - that otherwise bracket every compress/decompress, so the default
// per-record cost is a plain function call. Turning it on begins populating the
// CPU histograms and the wire_comp_*_cpu_pct stats. Set from config (static and
// dynamic); the getter backs get-config reporting.
void zstd_wire_set_benchmarks_enabled(bool enabled);
bool zstd_wire_get_benchmarks_enabled(void);

// Initialize and dump global CPU-time microbenchmark histograms. Histogram
// values are CPU microseconds spent in each zstd_wire public entry point.
void zstd_wire_histograms_init(histogram_scale scale);
void zstd_wire_dump_histograms(void);

// Update and report process-wide 10-second rolling CPU share used by zstd_wire
// work. *_cpu_pct_x100 is percent * 100, e.g. 1234 == 12.34%.
void zstd_wire_ticker(uint64_t delta_time);
void zstd_wire_get_stats(zstd_wire_stats* stats);
void zstd_wire_info(cf_dyn_buf* db);
void zstd_wire_statistics(cf_dyn_buf* db);
