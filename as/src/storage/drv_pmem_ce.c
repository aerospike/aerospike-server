/*
 * drv_pmem_ce.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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
 * ANY WARRANTY
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "citrusleaf/cf_queue.h"

#include "log.h"

#include "base/datamodel.h"
#include "fabric/partition.h"
#include "storage/drv_common.h"
#include "storage/storage.h"

//==========================================================
// Inlines & macros.
//

__attribute__((noreturn)) static inline void
pmem_crash_ce(void)
{
	cf_crash(AS_DRV_PMEM, "community edition using pmem");
	// Not reached - just for noreturn.
	abort();
}

#define PMEM_CE_STUB(name, ret, params)                                        \
	ret as_storage_##name##_pmem params { pmem_crash_ce(); }

//==========================================================
// Public API.
//

AS_STORAGE_OPS_LIST(PMEM_CE_STUB)
#undef PMEM_CE_STUB

const as_storage_ops as_storage_ops_pmem = {
#define AS_STORAGE_OPS_ASSIGN(name, ret, params)                               \
	.name = as_storage_##name##_pmem,
	AS_STORAGE_OPS_LIST(AS_STORAGE_OPS_ASSIGN)
#undef AS_STORAGE_OPS_ASSIGN
};
