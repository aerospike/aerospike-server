/*
 * mrt_monitor.h
 *
 * Copyright (C) 2024-2025 Aerospike, Inc.
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

#include "citrusleaf/cf_digest.h"

#include "msg.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_namespace_s;
struct as_storage_rd_s;
struct as_transaction_s;
struct keyd_tracker_s;


//==========================================================
// Typedefs & constants.
//

typedef struct monitor_roll_origin_s {
	uint64_t mrt_id;
	bool active;
	bool fwd;
	uint32_t n_kts;
	struct keyd_tracker_s* kts;
} monitor_roll_origin;

#define MONITOR_SET_NAME "<ERO~MRT"
#define MONITOR_SET_NAME_LEN (sizeof(MONITOR_SET_NAME) - 1)

#define MIN_MRT_DURATION 1
#define MAX_MRT_DURATION (2 * 60)
#define DEFAULT_MRT_DURATION 10

#define MAX_MONITOR_RECORD_SZ (90 * 1024)


//==========================================================
// Public API.
//

void as_mrt_monitor_init(void);

bool as_mrt_monitor_is_monitor_set_id(const struct as_namespace_s* ns, uint32_t set_id);
bool as_mrt_monitor_is_monitor_record(const struct as_namespace_s* ns, const struct as_index_s* r);

bool as_mrt_monitor_check_set_name(const struct as_namespace_s* ns, const uint8_t* name, uint32_t len);
int as_mrt_monitor_write_check(struct as_transaction_s* tr, struct as_storage_rd_s* rd);
uint32_t as_mrt_monitor_compute_deadline(const struct as_transaction_s* tr);
int as_mrt_monitor_check_writes_limit(struct as_storage_rd_s* rd);
void as_mrt_monitor_update_hist(struct as_storage_rd_s* rd);

void as_mrt_monitor_roll_done(monitor_roll_origin* roll_orig, const cf_digest* keyd, uint8_t result);
void as_mrt_monitor_proxyer_roll_done(msg* m, msg* fab_msg, monitor_roll_origin* roll_orig);
void as_mrt_monitor_proxyer_roll_timeout(msg* fab_msg, monitor_roll_origin* roll_orig);

uint32_t as_mrt_monitor_n_active(const struct as_namespace_s* ns);
uint64_t as_mrt_monitor_n_present(const struct as_namespace_s* ns);
