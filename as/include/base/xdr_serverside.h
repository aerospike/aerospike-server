/*
 * xdr_serverside.h
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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

#include "dynbuf.h"
#include "node.h"
#include "socket.h"

#include "base/datamodel.h"
#include "base/transaction.h"

//==========================================================
// Forward declarations.
//

//==========================================================
// Constants & typedefs.
//

typedef enum {
	XDR_OP_TYPE_WRITE,
	XDR_OP_TYPE_DROP,
	XDR_OP_TYPE_DURABLE_DELETE
} xdr_op_type;

typedef uint64_t xdr_dirty_bins[2];

//==========================================================
// Public API.
//

int as_xdr_init();
void xdr_config_post_process();
void as_xdr_start();
int as_xdr_shutdown();
void xdr_sig_handler(int signum);

void xdr_clear_dirty_bins(xdr_dirty_bins *dirty);
void xdr_fill_dirty_bins(xdr_dirty_bins *dirty);
void xdr_copy_dirty_bins(xdr_dirty_bins *from, xdr_dirty_bins *to);
void xdr_add_dirty_bin(as_namespace *ns, xdr_dirty_bins *dirty, const char *name, size_t name_len);
void xdr_write(as_namespace *ns, cf_digest *keyd, uint16_t generation, cf_node masternode, xdr_op_type op_type, uint16_t set_id, xdr_dirty_bins *dirty);
void as_xdr_read_txn(as_transaction *txn);

void as_xdr_info_init(void);
void as_xdr_info_port(cf_serv_cfg *serv_cfg);
int as_info_command_xdr(char *name, char *params, cf_dyn_buf *db);
void as_xdr_get_stats(cf_dyn_buf *db);
void as_xdr_get_config(cf_dyn_buf *db);
bool as_xdr_set_config(char *params);
bool as_xdr_set_config_ns(char *ns_name, char *params);

bool is_xdr_delete_shipping_enabled();
bool is_xdr_digestlog_low(as_namespace *ns);
bool is_xdr_forwarding_enabled();
bool is_xdr_nsup_deletes_enabled();

void xdr_cfg_add_int_ext_mapping(dc_config_opt *dc_cfg, char* orig, char* alt);
