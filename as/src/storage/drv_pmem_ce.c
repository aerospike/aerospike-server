/*
 * drv_pmem_ce.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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
 * ANY WARRANTY
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#include "storage/storage.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "citrusleaf/cf_queue.h"

#include "log.h"

#include "base/datamodel.h"
#include "fabric/partition.h"


//==========================================================
// Inlines & macros.
//

__attribute__((noreturn)) static inline void
pmem_crash_ce(void)
{
	cf_crash(AS_DRV_PMEM, "community edition using pmem");
	// Not reached - just for noreturn.
	abort();
}


//==========================================================
// Public API.
//

void
as_storage_init_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_load_pmem(as_namespace* ns, cf_queue* complete_q)
{
	pmem_crash_ce();
}

void
as_storage_load_ticker_pmem(const as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_activate_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

bool
as_storage_wait_for_defrag_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_start_tomb_raider_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_shutdown_pmem(struct as_namespace_s* ns)
{
	pmem_crash_ce();
}

void
as_storage_destroy_record_pmem(as_namespace* ns, as_record* r)
{
	pmem_crash_ce();
}

void
as_storage_record_create_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

void
as_storage_record_open_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

void
as_storage_record_close_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

int
as_storage_record_load_bins_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

bool
as_storage_record_load_key_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

bool
as_storage_record_load_pickle_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

bool
as_storage_record_load_raw_pmem(as_storage_rd* rd, bool leave_encrypted)
{
	pmem_crash_ce();
}

int
as_storage_record_write_pmem(as_storage_rd* rd)
{
	pmem_crash_ce();
}

void
as_storage_defrag_sweep_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_load_regime_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_save_regime_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_load_roster_generation_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_save_roster_generation_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_load_pmeta_pmem(as_namespace* ns, as_partition* p)
{
	pmem_crash_ce();
}

void
as_storage_save_pmeta_pmem(as_namespace* ns, const as_partition* p)
{
	pmem_crash_ce();
}

void
as_storage_cache_pmeta_pmem(as_namespace* ns, const as_partition* p)
{
	pmem_crash_ce();
}

void
as_storage_flush_pmeta_pmem(as_namespace* ns, uint32_t start_pid,
		uint32_t n_partitions)
{
	pmem_crash_ce();
}

void
as_storage_stats_pmem(as_namespace* ns, uint32_t* avail_pct,
		uint64_t* used_bytes)
{
	pmem_crash_ce();
}

void
as_storage_device_stats_pmem(const as_namespace* ns, uint32_t device_ix,
		storage_device_stats* stats)
{
	pmem_crash_ce();
}

void
as_storage_ticker_stats_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}

void
as_storage_dump_wb_summary_pmem(const as_namespace* ns, bool verbose)
{
	pmem_crash_ce();
}

void
as_storage_histogram_clear_pmem(as_namespace* ns)
{
	pmem_crash_ce();
}
