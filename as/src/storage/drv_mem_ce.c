/*
 * drv_mem_ce.c
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include "storage/drv_mem.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


//==========================================================
// Inlines & macros.
//

static inline char*
stripe_name(int ix)
{
	char* str = cf_malloc(6 + 1 + 3 + 1);

	sprintf(str, "stripe-%d", ix);

	return str;
}


//==========================================================
// Public API.
//

void
as_storage_start_tomb_raider_mem(as_namespace* ns)
{
}

int
as_storage_record_write_mem(as_storage_rd* rd)
{
	// No-op for drops, caller will drop record.
	return rd->pickle != NULL || rd->n_bins != 0 ? write_record(rd) : 0;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
init_commit(drv_mem* mem)
{
}

void
resume_or_create_stripe(drv_mem* mem, const drv_header* shadow_header)
{
	mem->mem_base_addr = cf_valloc(mem->file_size);
	memset(mem->mem_base_addr, 0, DRV_HEADER_SIZE);

	mem->name = stripe_name(mem->file_id);
}

void
header_validate_cfg(const as_namespace* ns, drv_mem* mem, drv_header* header)
{
	if ((header->generic.prefix.flags & DRV_HEADER_FLAG_SINGLE_BIN) != 0) {
		cf_crash(AS_DRV_MEM, "device has 'single-bin' data but 'single-bin' is no longer supported");
	}
}

void
header_init_cfg(const as_namespace* ns, drv_mem* mem, drv_header* header)
{
}

void
cleanup_unmatched_stripes(as_namespace* ns)
{
}

void
clear_encryption_keys(as_namespace* ns)
{
}

void
flush_final_cfg(as_namespace* ns)
{
}

void
cold_start_sweep_device(drv_mems* mems, drv_mem* mem)
{
	cold_start_sweep(mems, mem);
}

void
cold_start_fill_orig(drv_mem* mem, const as_flat_record* flat,
		uint64_t rblock_id, const as_flat_opt_meta* opt_meta,
		as_index_tree* tree, as_index_ref* r_ref)
{
}

void
cold_start_record_update(drv_mems* mems, const as_flat_record* flat,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_index* r = r_ref->r;

	cf_assert(r->rblock_id != 0, AS_DRV_MEM, "invalid rblock-id");

	block_free(&mems->mems[r->file_id], r->rblock_id, r->n_rblocks,
			"record-add");
}

conflict_resolution_pol
cold_start_policy(const as_namespace* ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}

void
cold_start_adjust_cenotaph(const as_namespace* ns, const as_flat_record* flat,
		uint32_t block_void_time, as_record* r)
{
}

void
cold_start_init_repl_state(const as_namespace* ns, as_record* r)
{
}

void
cold_start_set_unrepl_stat(as_namespace* ns)
{
}

void
cold_start_init_xdr_state(const as_flat_record* flat, as_record* r)
{
}

void
cold_start_drop_cenotaphs(as_namespace* ns)
{
}

void
resume_devices(drv_mems* mems)
{
	cf_crash(AS_DRV_MEM, "community edition called resume_devices()");
}

int
write_bins(as_storage_rd* rd)
{
	return buffer_bins(rd);
}

void
set_wblock_flags(as_storage_rd* rd, mem_write_block* mwb)
{
}

void
mrt_block_free_orig(drv_mems* mems, as_record* r)
{
}

void
mrt_rd_block_free_orig(drv_mems* mems, as_storage_rd* rd)
{
}

void
decrypt_record(drv_mem* mem, uint64_t off, as_flat_record* flat)
{
}

uint8_t*
encrypt_wblock(mem_write_block* mwb, uint64_t off)
{
	return mwb->base_addr;
}
