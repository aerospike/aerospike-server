/*
 * xdr.h
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include "cf_mutex.h"
#include "dynbuf.h"
#include "socket.h"
#include "tls.h"
#include "vector.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_namespace_s;
struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

#define AS_XDR_MAX_DCS 64

#define AS_XDR_MIN_PERIOD_MS 5
#define AS_XDR_MAX_PERIOD_MS 1000

#define AS_XDR_MAX_HOT_KEY_MS 5000

#define AS_XDR_MIN_SC_REPLICATION_WAIT_MS 5
#define AS_XDR_MAX_SC_REPLICATION_WAIT_MS 1000

#define AS_XDR_MIN_TRANSACTION_QUEUE_LIMIT 1024
#define AS_XDR_MAX_TRANSACTION_QUEUE_LIMIT (1024 * 1024)

typedef enum {
	XDR_AUTH_NONE,
	XDR_AUTH_INTERNAL,
	XDR_AUTH_EXTERNAL,
	XDR_AUTH_EXTERNAL_INSECURE,
	XDR_AUTH_PKI,
	XDR_AUTH_KERBEROS_LOCAL, // TODO - not yet implemented
	XDR_AUTH_KERBEROS_LDAP // TODO - not yet implemented
} as_xdr_auth_mode;

typedef enum {
	XDR_BIN_POLICY_ALL,
	XDR_BIN_POLICY_ONLY_CHANGED,
	XDR_BIN_POLICY_CHANGED_AND_SPECIFIED,
	XDR_BIN_POLICY_CHANGED_OR_SPECIFIED
} as_xdr_bin_policy;

typedef enum {
	XDR_WRITE_POLICY_AUTO, // TODO - is this mode worth the complication?
	XDR_WRITE_POLICY_UPDATE,
	XDR_WRITE_POLICY_REPLACE
} as_xdr_write_policy;

typedef struct as_xdr_dc_ns_cfg_s {
	char* ns_name;

	as_xdr_bin_policy bin_policy;
	uint32_t compression_level;
	uint32_t delay_ms;
	bool compression_enabled;
	bool forward;
	uint32_t hot_key_ms;
	bool ignore_expunges;
	uint32_t max_throughput;
	char* remote_namespace;
	uint32_t sc_replication_wait_ms;
	bool ship_bin_luts;
	bool ship_nsup_deletes;
	bool ship_only_specified_sets;
	uint32_t transaction_queue_limit;
	as_xdr_write_policy write_policy;

	cf_vector* ignored_sets; // startup only
	cf_vector* shipped_sets; // startup only
	uint8_t sets[1024]; // AS_SET_MAX_COUNT + 1

	cf_vector* ignored_bins; // startup only
	cf_vector* shipped_bins; // startup only
	uint8_t bins[(64 * 1024) - 1]; // MAX_BIN_NAMES
} as_xdr_dc_ns_cfg;

typedef struct as_xdr_dc_cfg_s {
	char* name;

	cf_mutex seed_lock;
	cf_vector seed_nodes; // from 'node-address-port'

	as_xdr_auth_mode auth_mode; // Aerospike destinations only
	char* auth_password_file; // Aerospike destinations only
	char* auth_user; // Aerospike destinations only

	bool connector;
	uint32_t max_recoveries_interleaved;
	uint32_t max_used_service_threads;
	uint32_t period_us;

	char* tls_our_name;
	cf_tls_spec* tls_spec; // from 'tls-name'

	bool use_alternate_access_address; // Aerospike destinations only

	cf_vector* ns_cfg_v; // startup only
	as_xdr_dc_ns_cfg* ns_cfgs[32]; // AS_NAMESPACE_SZ - TODO - fix include loop
} as_xdr_dc_cfg;

typedef struct as_xdr_config_s {
	uint8_t src_id;

	// For debugging.
	uint32_t trace_sample; // dynamic only
} as_xdr_config;

typedef struct as_xdr_submit_info_s {
	cf_digest keyd;
	uint64_t lut;

	uint64_t prev_lut; // "hot key" handling

	// Filter info.
	uint16_t set_id;
	bool xdr_write;
	bool xdr_tombstone;
	bool xdr_nsup_tombstone;
} as_xdr_submit_info;


//==========================================================
// Public API.
//

// Generic.
void as_xdr_init(void);
void as_xdr_start(void);

// Manager.
as_xdr_dc_cfg* as_xdr_startup_create_dc(const char* name);
as_xdr_dc_ns_cfg* as_xdr_startup_create_dc_ns_cfg(const char* ns_name);
void as_xdr_startup_add_seed(as_xdr_dc_cfg* cfg, char* host, char* port, char* tls_name);
void as_xdr_link_tls(void);
void as_xdr_get_submit_info(const struct as_index_s* r, uint64_t prev_lut, as_xdr_submit_info* info);
void as_xdr_submit(const struct as_namespace_s* ns, const as_xdr_submit_info* info);
void as_xdr_ticker(uint64_t delta_time);
void as_xdr_cleanup_tl_stats(void);

// Reader.
void as_xdr_read(struct as_transaction_s* tr);

// Ship.
void as_xdr_init_poll(cf_poll poll);
void as_xdr_shutdown_poll(void);
void as_xdr_io_event(uint32_t mask, void* data);
void as_xdr_timer_event(uint32_t sid, cf_poll_event* events, int32_t n_events, uint32_t e_ix);

// Info.
void as_xdr_get_config(const char* cmd, cf_dyn_buf* db);
bool as_xdr_set_config(const char* cmd);
void as_xdr_get_stats(const char* cmd, cf_dyn_buf* db);
int as_xdr_dc_state(char* name, char* cmd, cf_dyn_buf* db);
int as_xdr_get_filter(char* name, char* cmd, cf_dyn_buf* db);
int as_xdr_set_filter(char* name, char* cmd, cf_dyn_buf* db);
