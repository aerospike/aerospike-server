/*
 * cf_defer.h
 *
 * Copyright (C) 2025-2026 Aerospike, Inc.
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

#include <stddef.h>

//==========================================================
// Typedefs & constants.
//

//==========================================================
// Public API.
//

#define DEFER_GLUE2(_a, _b) _a##_b
#define DEFER_GLUE(_a, _b) DEFER_GLUE2(_a, _b)

#define DEFER_ATTR(_func) __attribute__((cleanup(_func)))

#define DEFER_FN(_x, _func)                                                    \
	DEFER_ATTR(_func)                                                          \
	__auto_type DEFER_GLUE(_defer_fn_, __LINE__) = &(_x)
