/*
 * replica_write_test_support.h
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
// Replica-write internals — NOT part of the replica-write public API. These
// three helpers have no caller outside transaction/replica_write.c; they are
// non-static only so the unit tests can drive them as pure functions. Include
// this from replica_write.c and from the replica-write unit tests, never from
// production code that just needs the replica-write interface
// (transaction/replica_write.h).
//
// repl_write_get_orig_pickle() in particular is safe only in the sequence the
// three receive handlers use it in - see its definition - so it must not become
// callable from anywhere that includes the public header.
//
// Same quarantine pattern as fabric/migrate_test_support.h, which keeps the
// migration-internal emigration_reinsert_ctrl out of fabric/migrate.h. That
// header is struct-only - the test-visible migration helpers it used to declare
// went away with the NACK fallback - so it is the pattern that is shared here,
// not the contents.
//

#include <stdint.h>

#include "msg.h"

//==========================================================
// Forward declarations.
//

struct as_remote_record_s;
struct rw_request_s;

//==========================================================
// Test-visible internals.
//

bool repl_write_needs_pickle_fallback(const struct rw_request_s* rw,
		uint32_t result_code);
void repl_write_get_orig_pickle(msg* m, struct as_remote_record_s* rr);
bool repl_write_ack_credits_bytes_saved(bool wire_compressed_op_sent,
		uint32_t result_code, uint64_t per_dest_bytes_saved);
