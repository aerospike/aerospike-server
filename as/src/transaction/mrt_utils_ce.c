/*
 * mrt_utils_ce.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
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

#include "transaction/mrt_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "arenax.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "storage/storage.h"


//==========================================================
// Public API.
//

bool
is_mrt_provisional(const as_record* r)
{
	return false;
}

bool
is_mrt_original(const as_record* r)
{
	return false;
}

bool
is_mrt_monitor_write(const as_namespace* ns, const as_record* r)
{
	return false;
}

bool
is_mrt_setless_tombstone(const as_namespace* ns, const as_record* r)
{
	return false;
}

void
mrt_free_orig(cf_arenax* arena, as_record* r, cf_arenax_puddle* puddle)
{
}

int
mrt_allow_read(as_transaction* tr, const as_record* r)
{
	if (as_transaction_has_mrt_id(tr)) {
		cf_warning(AS_RW, "MRTs are enterprise only");
		return AS_ERR_ENTERPRISE_ONLY;
	}

	return AS_OK;
}

int
mrt_allow_write(as_transaction* tr, const as_record* r)
{
	if (as_transaction_has_mrt_id(tr)) {
		cf_warning(AS_RW, "MRTs are enterprise only");
		return AS_ERR_ENTERPRISE_ONLY;
	}

	return AS_OK;
}

bool
mrt_write_on_locking_only(const as_transaction* tr, const as_record* r)
{
	// No checks - have already called mrt_allow_write().

	return true;
}

int
mrt_allow_udf_write(as_transaction* tr, const as_record* r)
{
	// No mrt-id check - have already called mrt_allow_read().

	return AS_OK;
}

as_record*
read_r(as_namespace* ns, as_record* r, bool is_mrt)
{
	return r;
}

bool
mrt_skip_cleanup(as_namespace* ns, as_index_tree* tree, as_index_ref* r_ref)
{
	return false;
}

int
set_mrt_id_from_msg(as_storage_rd* rd, const as_transaction* tr)
{
	return AS_OK;
}

void
set_mrt_id(as_storage_rd* rd, uint64_t mrt_id)
{
	if (mrt_id != 0) {
		cf_warning(AS_RW, "unexpected MRT id received");
	}
}

bool
is_first_mrt(const as_storage_rd* rd)
{
	return false;
}

void
finish_replace_mrt(as_storage_rd* rd, const as_record* old_r,
		cf_arenax_puddle* puddle)
{
}

as_record_version*
mrt_write_fill_version(as_record_version* v, const as_transaction* tr)
{
	return NULL;
}

as_record_version*
mrt_read_fill_version(as_record_version* v, const as_transaction* tr)
{
	return NULL;
}

bool
mrt_load_orig_pickle(as_namespace* ns, as_record* r, uint8_t** pickle,
		uint32_t* pickle_sz)
{
	return true;
}

bool
is_rr_mrt(const as_remote_record* rr)
{
	return false;
}

bool
is_rr_mrt_monitor_write(const as_remote_record* rr, uint32_t set_id)
{
	return false;
}

int
mrt_apply_original(as_remote_record* rr, as_index_ref* r_ref)
{
	return AS_OK;
}

int
mrt_apply_roll(as_remote_record* rr, as_index_ref* r_ref, as_storage_rd* rd)
{
	cf_crash(AS_RW, "CE code called mrt_apply_roll()");

	return AS_ERR_ENTERPRISE_ONLY;
}

void
finish_first_mrt(as_storage_rd* rd, const as_record* old_r,
		cf_arenax_puddle* puddle)
{
}
