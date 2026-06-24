/*
 * drv_pmem.h
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

#pragma once

//
// Public PMEM types shared between the storage engine implementation
// (drv_pmem_ee.c) and shared cross-engine code in drv_common.{h,c}.
//
// Mirrors drv_mem.h and drv_ssd.h: extension structs that compose on top of
// the unified drv_write_buffer / drv_wblock_state / drv_current_wb bases.
//

#include <stdbool.h>
#include <stdint.h>

#include "storage/drv_common.h"

//==========================================================
// Forward declarations.
//

struct drv_pmem_s;

//==========================================================
// Typedefs.
//

// Where records accumulate until flushed to device. Common fields live in
// the embedded drv_write_buffer base (see drv_common.h); only fields that
// are PMEM-specific (mmap base address, dirty bookkeeping) live in this
// extension. PMEM does not use the base's flush_pos / rc fields - those
// are dead-storage here, kept for layout uniformity.
typedef struct pmem_write_block_s {
	drv_write_buffer base;
	uint8_t* base_addr; // mmap pointer into the device's mapped region
	uint32_t first_dirty_pos;
	bool dirty; // written to since last flushed
} pmem_write_block;

// Per-wblock information. Currently identical across engines, so it is just
// a typedef of the unified base.
typedef drv_wblock_state pmem_wblock_state;

// Per current write buffer information. PMEM has only the base today; if EE
// adds extension fields later they would go here.
typedef struct current_pwb_s {
	drv_current_wb base;
} current_pwb;

//==========================================================
// Inlines.
//

// Convenience accessor for the engine-typed back-pointer kept on the base.
static inline struct drv_pmem_s*
pwb_dev(const pmem_write_block* pwb)
{
	return pwb->base.dev.pmem;
}

// Convenience accessor for the engine-typed write-buffer pointer in wblock
// state. The base stores it as drv_write_buffer*; engine code consumes a
// pmem_write_block*.
static inline pmem_write_block*
pwb_of(const pmem_wblock_state* state)
{
	return (pmem_write_block*)state->wb;
}

// Convenience accessor for the engine-typed write-buffer pointer in
// current_pwb. The base stores it as drv_write_buffer*.
static inline pmem_write_block*
pwb_of_cur(const current_pwb* cur)
{
	return (pmem_write_block*)cur->base.wb;
}

// Layout asserts: shared code casts pmem_write_block* to drv_write_buffer*
// and current_pwb* to drv_current_wb*, so the embedded base must be the
// first member. The wblock-state typedef is trivially equivalent.
COMPILER_ASSERT(offsetof(pmem_write_block, base) == 0);
COMPILER_ASSERT(offsetof(current_pwb, base) == 0);
