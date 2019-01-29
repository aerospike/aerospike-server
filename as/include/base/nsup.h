/*
 * nsup.h
 *
 * Copyright (C) 2019 Aerospike, Inc.
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
#include <stdint.h>


//==========================================================
// Forward declarations.
//

struct as_namespace_s;


//==========================================================
// Public API.
//

void as_nsup_init(void);
void as_nsup_start(void);

bool as_nsup_handle_clock_skew(struct as_namespace_s* ns, uint64_t skew_ms);

bool as_nsup_eviction_reset_cmd(const char* ns_name, const char* ttl_str);

bool as_cold_start_evict_if_needed(struct as_namespace_s* ns);
