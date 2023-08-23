/*
 * trace.h
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

#define MAX_BACKTRACE_DEPTH 50

// We assume this is big enough (for max path length, max identifier length,
// line number, overhead).
#define SYM_STR_MAX_SZ (8 * 1024)


//==========================================================
// Public API.
//

void cf_trace_init(void);
int cf_backtrace(void** buf, int sz);
uint64_t cf_strip_aslr(const void* addr);
void cf_addr_to_sym_str(char* buf, const void* addr);
