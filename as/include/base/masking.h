/*
 * masking.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"

#include "base/datamodel.h"

//==========================================================
// Forward declarations.
//

struct as_transaction_s;
struct as_info_cmd_args_s;

//==========================================================
// Typedefs & constants.
//

// NOTE: Before changing this be sure it does not break masking_path_hash_fn.
typedef struct as_masking_key_s {
	char ns_name[AS_ID_NAMESPACE_SZ];
	char set_name[AS_SET_NAME_MAX_SIZE];
	char bin_name[AS_BIN_NAME_MAX_SZ];
} __attribute__((__packed__)) as_masking_key;

typedef struct as_masking_ctx_s {
	as_masking_key key;
	bool mask_reads;
	bool mask_writes;
	bool set_uses_masking;
} as_masking_ctx;

//==========================================================
// Public API.
//

void as_masking_init(void);
void as_masking_start(void);

bool as_masking_ctx_init(as_masking_ctx* state, const char* ns_name,
		const as_set* p_set, const char* username,
		const struct as_transaction_s* tr);
bool as_masking_has_rule(as_masking_ctx* state, const char* bin_name,
		as_particle_type bin_type);
bool as_masking_type_mismatch(as_masking_ctx* state, const as_bin* b);
bool as_masking_apply(as_masking_ctx* state, as_bin* dst, const as_bin* src);

uint8_t as_masking_log_violation(const struct as_transaction_s* tr,
		const char* action, const char* detail, const void* bin_name,
		size_t bin_name_sz);

// Info commands.
void as_masking_info_cmd(struct as_info_cmd_args_s* args);

//==========================================================
// Inlines & macros.
//

static inline bool
as_masking_must_mask(const as_masking_ctx* state, bool is_write)
{
	return state != NULL && (is_write ? state->mask_writes : state->mask_reads);
}
