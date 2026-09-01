/*
 * index_checkpoint_ce.c
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
 */

//==========================================================
// Includes.
//

#include "base/index_checkpoint.h"

#include "log.h"

//==========================================================
// Public API - community edition stubs.
//
// The index-checkpoint feature is enterprise-only. The info commands are gated
// 'ee_only' in the command table, so these handlers are never dispatched in a
// CE build - reaching them is a logic error.
//

void
as_index_checkpoint_save_cmd(struct as_info_cmd_args_s* args)
{
	(void)args;
	cf_crash(AS_INFO, "CE build called as_index_checkpoint_save_cmd()");
}

void
as_index_checkpoint_status_cmd(struct as_info_cmd_args_s* args)
{
	(void)args;
	cf_crash(AS_INFO, "CE build called as_index_checkpoint_status_cmd()");
}

void
as_index_checkpoint_park_on_shutdown(void)
{
	// No checkpoint feature in CE - nothing to park for, exit normally.
}

bool
as_index_checkpoint_in_shutdown(void)
{
	return false; // no checkpoint feature in CE
}

bool
as_index_checkpoint_is_parked(void)
{
	return false; // no checkpoint feature in CE
}

bool
as_index_checkpoint_any_failed(void)
{
	return false; // no checkpoint feature in CE
}

void
as_index_checkpoint_validate_path_writable(void)
{
	// No checkpoint feature in CE - no path to validate.
}

void
as_index_checkpoint_note_reap_signal(void)
{
	// No checkpoint feature in CE - no park to reap.
}

void
as_index_checkpoint_apply_skip(struct as_namespace_s* ns)
{
	(void)ns;
	// No checkpoint feature in CE - index_checkpoint_path stays NULL.
}

void
as_index_checkpoint_delete_on_startup(void)
{
	// No checkpoint feature in CE - nothing to delete.
}
