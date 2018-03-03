/*
 * drv_ssd_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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
#include "fault.h"
#include "base/datamodel.h"
#include "base/rec_props.h"
#include "storage/storage.h"


void
ssd_resume_devices(drv_ssds* ssds)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "cold start called ssd_resume_devices()");
}

void*
run_ssd_cool_start(void* udata)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called run_ssd_cool_start()");

	return NULL;
}

void
ssd_header_init_cfg(const as_namespace* ns, ssd_device_header* header)
{
}

bool
ssd_header_is_valid_cfg(const as_namespace* ns, const ssd_device_header* header)
{
	return true;
}

bool
ssd_cold_start_is_valid_n_bins(uint32_t n_bins)
{
	// FIXME - what should we do here?
	cf_assert(n_bins != 0, AS_DRV_SSD,
			"community edition found tombstone - erase drive and restart");

	return n_bins <= BIN_NAMES_QUOTA;
}

bool
ssd_cold_start_is_record_truncated(as_namespace* ns, const drv_ssd_block* block,
		const as_rec_props* p_props)
{
	return false;
}

void
ssd_cold_start_adjust_cenotaph(as_namespace* ns, const drv_ssd_block* block,
		as_record* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_transition_record(as_namespace* ns, const drv_ssd_block* block,
		as_record* r, bool is_create)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_cold_start_drop_cenotaphs(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

void
ssd_adjust_versions(as_namespace* ns, ssd_device_header* header)
{
	// Nothing to do - relevant for enterprise version only.
}

conflict_resolution_pol
ssd_cold_start_policy(as_namespace *ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}

void
ssd_cold_start_init_repl_state(as_namespace* ns, as_record* r)
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
ssd_init_trusted(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

bool
ssd_is_untrusted(as_namespace *ns, uint8_t header_flags)
{
	return false;
}

void
ssd_set_trusted(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

void
as_storage_start_tomb_raider_ssd(as_namespace* ns)
{
	// Tomb raider is for enterprise version only.
}

int
as_storage_record_write_ssd(as_storage_rd* rd)
{
	// All record writes except defrag come through here!
	return as_bin_inuse_has(rd) ? ssd_write(rd) : 0;
}

void
ssd_init_encryption_key(as_namespace* ns)
{
}

void
ssd_do_encrypt(const uint8_t* key, uint64_t off, drv_ssd_block* block)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called ssd_do_encrypt()");
}

void
ssd_do_decrypt(const uint8_t* key, uint64_t off, drv_ssd_block* block)
{
	// Should not get here - for enterprise version only.
	cf_crash(AS_DRV_SSD, "community edition called ssd_do_decrypt()");
}
