/*
 * drv_ssd_ce.c
 *
 * Copyright (C) 2014-2020 Aerospike, Inc.
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

void*
run_ssd_cool_start(void* udata)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called run_ssd_cool_start()");

	return NULL;
}

void
ssd_header_init_cfg(const as_namespace* ns, drv_ssd* ssd, drv_header* header)
{
	if (ns->single_bin) {
		header->generic.prefix.flags |= DRV_HEADER_FLAG_SINGLE_BIN;
	}
}

void
ssd_header_validate_cfg(const as_namespace* ns, drv_ssd* ssd,
		drv_header* header)
{
	if ((header->generic.prefix.flags & DRV_HEADER_FLAG_SINGLE_BIN) != 0) {
		if (! ns->single_bin) {
			cf_crash(AS_DRV_SSD, "device has 'single-bin' data but 'single-bin' is not configured");
		}
	}
	else {
		if (ns->single_bin) {
			cf_crash(AS_DRV_SSD, "device has multi-bin data but 'single-bin' is configured");
		}
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
ssd_cold_start_adjust_cenotaph(as_namespace* ns, const as_flat_record* flat,
		uint32_t block_void_time, as_record* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_transition_record(as_namespace* ns, const as_flat_record* flat,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref, bool is_create)
{
	index_metadata old_metadata = {
			// Note - other members irrelevant.
			.generation = is_create ? 0 : 1, // fake to transition set-index
	};

	as_record_transition_set_index(tree, r_ref, ns, opt_meta->n_bins,
			&old_metadata);
}

void
ssd_cold_start_drop_cenotaphs(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_adjust_versions(as_namespace* ns, drv_pmeta* pmeta)
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

void
as_storage_cfg_init_ssd(as_namespace* ns)
{
}

void
ssd_encrypt(drv_ssd *ssd, uint64_t off, as_flat_record *flat)
{
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
