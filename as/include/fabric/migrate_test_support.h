/*
 * migrate_test_support.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
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
// Migration retransmit internals — NOT part of the migration public API. This
// type is private to fabric/migrate.c; this header exists ONLY so the unit
// tests can exercise it directly. Include it from migrate.c and from the
// migration unit tests, never from production code that just needs the
// migration public interface (fabric/migrate.h). Keeping it here rather than in
// fabric/migrate.h prevents the per-record reinsert bookkeeping from becoming
// part of the module's public contract.
//

#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "fabric/migrate.h" // emigration, msg

//==========================================================
// Reinsert (retransmit) bookkeeping — one per in-flight migrated record.
//

typedef struct emigration_reinsert_ctrl_s {
	uint64_t xmit_ms;
	uint64_t start_ns;
	emigration* emig;
	msg* m;
	cf_digest keyd;
	uint64_t lut;
	// Bytes saved on the wire by compressing this record - 0 if it went plain.
	// Credited to ns->migrate_wire_comp_stat.bytes_saved when the insert is
	// acked, so the counter reflects bytes actually saved (not bytes we hoped
	// to save).
	uint64_t bytes_saved;
} emigration_reinsert_ctrl;
