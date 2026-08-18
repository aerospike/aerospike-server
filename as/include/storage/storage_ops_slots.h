/*
 * storage_ops_slots.h
 *
 * Copyright (C) 2026 Aerospike, Inc.
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
 *
 * X-macro list for as_storage_ops: one row drives struct fields, extern ops
 * tables, and PMEM-CE crash stubs.
 *
 * Expand AS_STORAGE_OPS_LIST only where as_storage_rd and storage_device_stats
 * are already defined (storage.h does this).
 *
 * AS_STORAGE_OPS_LIST(S) — S(name, return_type, parameter_list)
 */

#pragma once

// clang-format off
#define AS_STORAGE_OPS_LIST(S)                                                      \
	S(init, void, (struct as_namespace_s* ns))                                      \
	S(load, void, (struct as_namespace_s* ns, cf_queue* complete_q))                \
	S(load_ticker, void, (const struct as_namespace_s* ns))                         \
	S(activate, void, (struct as_namespace_s* ns))                                  \
	S(wait_for_defrag, bool, (struct as_namespace_s* ns))                           \
	S(start_tomb_raider, void, (struct as_namespace_s* ns))                         \
	S(shutdown, void, (struct as_namespace_s* ns))                                  \
	S(destroy_record, void, (struct as_namespace_s* ns, struct as_index_s* r))      \
	S(record_open, void, (as_storage_rd* rd))                                       \
	S(record_load_bins, int, (as_storage_rd* rd))                                   \
	S(record_load_key, bool, (as_storage_rd* rd))                                   \
	S(record_load_pickle, bool, (as_storage_rd* rd))                                \
	S(record_load_raw, bool, (as_storage_rd* rd, bool leave_encrypted))             \
	S(record_write, int, (as_storage_rd* rd))                                       \
	S(defrag_sweep, void, (struct as_namespace_s* ns))                              \
	S(load_regime, void, (struct as_namespace_s* ns))                               \
	S(save_regime, void, (struct as_namespace_s* ns))                               \
	S(load_roster_generation, void, (struct as_namespace_s* ns))                    \
	S(save_roster_generation, void, (struct as_namespace_s* ns))                    \
	S(load_pmeta, void, (struct as_namespace_s* ns, struct as_partition_s* p))      \
	S(save_pmeta, void,                                                             \
			(struct as_namespace_s* ns, const struct as_partition_s* p))            \
	S(cache_pmeta, void,                                                            \
			(struct as_namespace_s* ns, const struct as_partition_s* p))            \
	S(flush_pmeta, void,                                                            \
			(struct as_namespace_s* ns, uint32_t start_pid,                         \
					uint32_t n_partitions))                                         \
	S(stats, void,                                                                  \
			(struct as_namespace_s* ns, uint32_t* avail_pct, uint64_t* used_bytes)) \
	S(device_stats, void,                                                           \
			(const struct as_namespace_s* ns, uint32_t device_ix,                   \
					storage_device_stats* stats))                                   \
	S(ticker_stats, void, (struct as_namespace_s* ns))                              \
	S(dump_wb_summary, void, (const struct as_namespace_s* ns, bool verbose))       \
	S(histogram_clear, void, (struct as_namespace_s* ns))
// clang-format on
