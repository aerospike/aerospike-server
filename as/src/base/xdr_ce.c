/*
 * xdr_ce.c
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include "base/xdr.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "log.h"
#include "socket.h"

#include "base/datamodel.h"
#include "base/transaction.h"


//==========================================================
// Public API.
//

void
as_xdr_init(void)
{
}

void
as_xdr_start(void)
{
}

as_xdr_dc_cfg*
as_xdr_startup_create_dc(const char* dc_name)
{
	cf_crash(AS_XDR, "unreachable function for CE");
	return NULL;
}

void
as_xdr_link_tls(void)
{
}

void
as_xdr_get_submit_info(const as_record* r, uint64_t prev_lut,
		as_xdr_submit_info* info)
{
}

void
as_xdr_submit(const as_namespace* ns, const as_xdr_submit_info* info)
{
}

void
as_xdr_ticker(uint64_t delta_time)
{
}

void
as_xdr_cleanup_tl_stats(void)
{
}

void
as_xdr_startup_add_seed(as_xdr_dc_cfg* cfg, char* host, char* port,
		char* tls_name)
{
	cf_crash(AS_XDR, "unreachable function for CE");
}

as_xdr_dc_ns_cfg*
as_xdr_startup_create_dc_ns_cfg(const char* ns_name)
{
	cf_crash(AS_XDR, "unreachable function for CE");
	return NULL;
}

void
as_xdr_read(as_transaction* tr)
{
}

void
as_xdr_init_poll(cf_poll poll)
{
}

void
as_xdr_shutdown_poll(void)
{
}

void
as_xdr_io_event(uint32_t mask, void* data)
{
}

void
as_xdr_timer_event(uint32_t sid, cf_poll_event* events, int32_t n_events,
		uint32_t e_ix)
{
}

void
as_xdr_get_config(const char* cmd, cf_dyn_buf* db)
{
}

bool
as_xdr_set_config(const char* cmd)
{
	cf_crash(AS_XDR, "unreachable function for CE");
	return false;
}

void
as_xdr_get_stats(const char* cmd, cf_dyn_buf* db)
{
}

int
as_xdr_dc_state(char* name, char* cmd, cf_dyn_buf* db)
{
	return 0;
}

int
as_xdr_get_filter(char* name, char* cmd, cf_dyn_buf* db)
{
	return 0;
}

int
as_xdr_set_filter(char* name, char* cmd, cf_dyn_buf* db)
{
	return 0;
}
