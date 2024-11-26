/*
 * drv_ssd_ce.c
 *
 * Copyright (C) 2014-2024 Aerospike, Inc.
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

#include "storage/drv_ssd.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


void
ssd_resume_devices(drv_ssds* ssds)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called ssd_resume_devices()");
}

void
ssd_header_init_cfg(const as_namespace* ns, drv_ssd* ssd, drv_header* header)
{
}

void
ssd_header_validate_cfg(const as_namespace* ns, drv_ssd* ssd,
		drv_header* header)
{
	if ((header->generic.prefix.flags & DRV_HEADER_FLAG_SINGLE_BIN) != 0) {
		cf_crash(AS_DRV_SSD, "device has 'single-bin' data but 'single-bin' is no longer supported");
	}
}

void
ssd_clear_encryption_keys(as_namespace* ns)
{
}

void
ssd_flush_final_cfg(as_namespace* ns)
{
}

void
ssd_cold_start_sweep_device(drv_ssds* ssds, drv_ssd* ssd)
{
	ssd_cold_start_sweep(ssds, ssd);
}

void
ssd_cold_start_fill_orig(drv_ssd* ssd, const as_flat_record* flat,
		uint64_t rblock_id, const as_flat_opt_meta* opt_meta,
		as_index_tree* tree, as_index_ref* r_ref)
{
}

void
ssd_cold_start_record_update(drv_ssds* ssds, const as_flat_record* flat,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_index* r = r_ref->r;

	cf_assert(r->rblock_id != 0, AS_DRV_SSD, "invalid rblock-id");

	ssd_block_free(&ssds->ssds[r->file_id], r->rblock_id, r->n_rblocks,
			"record-add");
}

void
ssd_cold_start_adjust_cenotaph(as_namespace* ns, const as_flat_record* flat,
		uint32_t block_void_time, as_record* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_drop_cenotaphs(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

conflict_resolution_pol
ssd_cold_start_policy(const as_namespace *ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}

void
ssd_cold_start_init_repl_state(as_namespace* ns, as_record* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_set_unrepl_stat(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_init_xdr_state(const as_flat_record* flat, as_record* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_init_commit(drv_ssd *ssd)
{
	// Nothing to do - relevant for enterprise version only.
}

uint64_t
ssd_flush_max_us(const as_namespace *ns)
{
	return ns->storage_flush_max_us;
}

int
ssd_write_bins(as_storage_rd *rd)
{
	return ssd_buffer_bins(rd);
}

void
ssd_mrt_block_free_orig(drv_ssds* ssds, as_record* r)
{
}

void
ssd_mrt_rd_block_free_orig(drv_ssds* ssds, as_storage_rd* rd)
{
}

void
ssd_set_wblock_flags(as_storage_rd* rd, ssd_write_buf* swb)
{
	swb->use_post_write_q = rd->which_current_swb == SWB_MASTER ||
				(rd->which_current_swb == SWB_PROLE &&
						rd->ns->storage_cache_replica_writes);
}

void
as_storage_start_tomb_raider_ssd(as_namespace* ns)
{
	// Tomb raider is for enterprise version only.
}

int
as_storage_record_write_ssd(as_storage_rd* rd)
{
	// No-op for drops, caller will drop record.
	return rd->pickle != NULL || rd->n_bins != 0 ? ssd_write(rd) : 0;
}

uint8_t*
ssd_encrypt_wblock(ssd_write_buf *swb, uint64_t off)
{
	return swb->buf;
}

void
ssd_decrypt(drv_ssd *ssd, uint64_t off, as_flat_record *flat)
{
}

void
ssd_decrypt_whole(drv_ssd *ssd, uint64_t off, uint32_t n_rblocks,
		as_flat_record *flat)
{
}

void
ssd_prefetch_wblock(drv_ssd *ssd, uint64_t file_offset, uint8_t *read_buf)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called ssd_prefetch_wblock()");
}
