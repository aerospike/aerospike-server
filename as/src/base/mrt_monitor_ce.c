/*
 * mrt_monitor_ce.c
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

#include "base/mrt_monitor.h"

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "log.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "storage/storage.h"


//==========================================================
// Public API.
//

void
as_mrt_monitor_init(void)
{
}

bool
as_mrt_monitor_is_monitor_set_id(const as_namespace* ns, uint32_t set_id)
{
	return false;
}

bool
as_mrt_monitor_is_monitor_record(const as_namespace* ns, const as_record* r)
{
	return false;
}

bool
as_mrt_monitor_check_set_name(const as_namespace* ns, const uint8_t* name,
		uint32_t len)
{
	return true;
}

int
as_mrt_monitor_write_check(as_transaction* tr, as_storage_rd* rd)
{
	return AS_OK;
}

uint32_t
as_mrt_monitor_compute_deadline(const as_transaction* tr)
{
	return 0;
}

int
as_mrt_monitor_check_writes_limit(as_storage_rd* rd)
{
	return AS_OK;
}

void
as_mrt_monitor_update_hist(as_storage_rd* rd)
{
}

void
as_mrt_monitor_roll_done(monitor_roll_origin* orig, const cf_digest* keyd,
		uint8_t result)
{
	cf_crash(AS_MRT_MONITOR, "CE code called as_mrt_monitor_delete_done()");
}

void
as_mrt_monitor_proxyer_roll_done(msg* m, msg* fab_msg,
		monitor_roll_origin* orig)
{
	cf_crash(AS_MRT_MONITOR, "CE code called as_mrt_monitor_proxyer_roll_done()");
}

void
as_mrt_monitor_proxyer_roll_timeout(msg* fab_msg, monitor_roll_origin* orig)
{
	cf_crash(AS_MRT_MONITOR, "CE code called as_mrt_monitor_proxyer_roll_timeout()");
}

uint32_t
as_mrt_monitor_n_active(const as_namespace* ns)
{
	return 0;
}
