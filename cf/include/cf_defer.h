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

#ifdef __cplusplus
# define cf_defer static_assert(0, "cf_defer / define_deferred_memory is C-only")
#else
# if defined(__clang__)
#  if (__clang_major__ >= 22)
// godbolt.org tests: Works on armv8-a clang 22+, x86-64 clang 22+.
#   include <stddefer.h>
#   ifndef __STDC_DEFER_TS25755__
#    error "clang needs -fdefer-ts (and C mode) for defer; add -fdefer-ts to compile flags / .clangd"
#   endif
#   define cf_defer defer
#  else
#   error "cf_defer requires Clang 22+ (stddefer / defer)"
#  endif
# elif (__GNUC__ >= 5)
// This is supposed to be 3.0+ but in practice (empirically using godbolt.org):
// Works on x86-64 gcc 5.1+, does not work on versions prior.
// Works on ARM64 gcc 4.9.4+, MinGW gcc 11.3+.
// Does not work on every other gcc in the godbolt.org selection.
#  define cf_defer DEFER_COUNTER_(__COUNTER__)
#  define DEFER_COUNTER_(CNTR) DEFER_EXPAND_(CNTR)
#  define DEFER_EXPAND_(CNTR) DEFER_FUNC_(DEFER_FUNC_##CNTR, DEFER_VAR_##CNTR)
#  define DEFER_FUNC_(F, V)                                               \
	auto void F(int*);                                                    \
	__attribute__((__cleanup__(F), __deprecated__, __unused__)) int V;    \
	__attribute__((__always_inline__, __deprecated__, __unused__))        \
	inline auto void F(__attribute__((__unused__)) int* V)
# else
#  error "cf_defer -- compiler not supported"
# endif
#endif // __cplusplus
