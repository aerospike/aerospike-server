/*
 * zstd_wire_ce.c
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

//==========================================================
// Includes.
//

#include "base/zstd_wire.h"

//==========================================================
// Public API - stats stubs.
//

void
zstd_wire_set_benchmarks_enabled(bool enabled)
{
	(void)enabled;
}

bool
zstd_wire_get_benchmarks_enabled(void)
{
	return false;
}

void
zstd_wire_histograms_init(histogram_scale scale)
{
	(void)scale;
}

void
zstd_wire_dump_histograms(void)
{
}

void
zstd_wire_ticker(uint64_t delta_time)
{
	(void)delta_time;
}

void
zstd_wire_get_stats(zstd_wire_stats* stats)
{
	if (stats == NULL) {
		return;
	}

	*stats = (zstd_wire_stats){ 0 };
}

void
zstd_wire_info(cf_dyn_buf* db)
{
	info_append_format(db, "wire_comp_cpu_pct", "%.2f", 0.0);
	info_append_uint64(db, "make_patch_calls", 0);
	info_append_uint64(db, "make_patch_cpu_ns", 0);
	info_append_format(db, "make_patch_cpu_pct", "%.2f", 0.0);
	info_append_uint64(db, "apply_patch_calls", 0);
	info_append_uint64(db, "apply_patch_cpu_ns", 0);
	info_append_format(db, "apply_patch_cpu_pct", "%.2f", 0.0);
	info_append_uint64(db, "compress_calls", 0);
	info_append_uint64(db, "compress_cpu_ns", 0);
	info_append_format(db, "compress_cpu_pct", "%.2f", 0.0);
	info_append_uint64(db, "decompress_calls", 0);
	info_append_uint64(db, "decompress_cpu_ns", 0);
	info_append_format(db, "decompress_cpu_pct", "%.2f", 0.0);
	cf_dyn_buf_chomp_char(db, ';');
}

void
zstd_wire_statistics(cf_dyn_buf* db)
{
	info_append_format(db, "wire_comp_cpu_pct", "%.2f", 0.0);
	info_append_format(db, "wire_comp_make_patch_cpu_pct", "%.2f", 0.0);
	info_append_format(db, "wire_comp_apply_patch_cpu_pct", "%.2f", 0.0);
	info_append_format(db, "wire_comp_compress_cpu_pct", "%.2f", 0.0);
	info_append_format(db, "wire_comp_decompress_cpu_pct", "%.2f", 0.0);
}

//==========================================================
// Public API - CE stubs.
//

size_t
zstd_wire_make_patch(void** patch_buf, const void* old_buf, size_t old_size,
		const void* new_buf, size_t new_size, int level)
{
	// CE stub - zstd delta compression not available. Match the EE
	// post-condition: a 0 return leaves the caller's out-pointer NULL.
	if (patch_buf != NULL) {
		*patch_buf = NULL;
	}

	(void)old_buf;
	(void)old_size;
	(void)new_buf;
	(void)new_size;
	(void)level;

	return 0;
}

size_t
zstd_wire_apply_patch(void** out_buf, const void* old_buf, size_t old_size,
		const void* patch_buf, size_t patch_size)
{
	// CE stub - zstd delta compression not available. Match the EE
	// post-condition: a 0 return leaves the caller's out-pointer NULL.
	if (out_buf != NULL) {
		*out_buf = NULL;
	}

	(void)old_buf;
	(void)old_size;
	(void)patch_buf;
	(void)patch_size;

	return 0;
}

size_t
zstd_wire_compress_buffer(void** out_buf, const void* in_buf, size_t in_size,
		int level)
{
	// CE stub - zstd compression not available. Match the EE post-condition:
	// a 0 return leaves the caller's out-pointer NULL.
	if (out_buf != NULL) {
		*out_buf = NULL;
	}

	(void)in_buf;
	(void)in_size;
	(void)level;

	return 0;
}

size_t
zstd_wire_decompress_buffer(void** out_buf, const void* in_buf, size_t in_size)
{
	// CE stub - zstd decompression not available. Match the EE post-condition:
	// a 0 return leaves the caller's out-pointer NULL.
	if (out_buf != NULL) {
		*out_buf = NULL;
	}

	(void)in_buf;
	(void)in_size;

	return 0;
}

size_t
zstd_wire_decompress_buffer_capped(void** out_buf, const void* in_buf,
		size_t in_size, size_t max_decompressed_size)
{
	// CE stub - zstd decompression not available. Match the EE post-condition:
	// a 0 return leaves the caller's out-pointer NULL.
	if (out_buf != NULL) {
		*out_buf = NULL;
	}

	(void)in_buf;
	(void)in_size;
	(void)max_decompressed_size;

	return 0;
}
