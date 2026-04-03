/*
 * masking_ce.c
 *
 * Copyright (C) 2025 Aerospike, Inc.
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

#include "base/masking.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "log.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"

//==========================================================
// Forward declarations.
//

struct as_file_handle_s;

//==========================================================
// Public API.
//

// Masking is an enterprise feature - here, no masking context.
bool
as_masking_ctx_init(as_masking_ctx* state, const char* ns_name,
		const as_set* p_set, const char* username, const as_transaction* tr)
{
	return false; // No masking rules in CE
}

// Masking is an enterprise feature - here, no rules exist.
bool
as_masking_has_rule(as_masking_ctx* state, const char* bin_name,
		as_particle_type bin_type)
{
	return false;
}

// Masking is an enterprise feature - here, no type mismatch.
bool
as_masking_type_mismatch(as_masking_ctx* state, const as_bin* b)
{
	return false;
}

// Masking is an enterprise feature - here, no masking applied.
bool
as_masking_apply(as_masking_ctx* state, as_bin* dst, const as_bin* src)
{
	return false;
}

// Masking is an enterprise feature - here, no violations.
uint8_t
as_masking_log_violation(const as_transaction* tr, const char* action,
		const char* detail, const void* bin_name, size_t bin_name_sz)
{
	return AS_OK;
}

// Masking is an enterprise feature - shouldn't get here.
void
as_masking_info_cmd(struct as_info_cmd_args_s* args)
{
	(void)args;
	cf_crash(AS_INFO, "CE build called as_masking_info_cmd()");
}
