/*
 * proxy.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include "dynbuf.h"
#include "node.h"


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_msg_op_s;
struct as_namespace_s;
struct as_proto_comp_stat_s;
struct as_record_version_s;
struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	PROXY_FIELD_OP,
	PROXY_FIELD_TID,
	PROXY_FIELD_DIGEST,
	PROXY_FIELD_REDIRECT,
	PROXY_FIELD_AS_PROTO, // request as_proto - currently contains only as_msg's
	PROXY_FIELD_UNUSED_5,
	PROXY_FIELD_UNUSED_6,
	PROXY_FIELD_UNUSED_7,

	NUM_PROXY_FIELDS
} proxy_msg_field;


//==========================================================
// Public API.
//

void as_proxy_init();

uint32_t as_proxy_hash_count();

void as_proxy_divert(cf_node dst, struct as_transaction_s* tr, struct as_namespace_s* ns);
void as_proxy_return_to_sender(const struct as_transaction_s* tr, struct as_namespace_s* ns);

void as_proxy_send_response(cf_node dst, uint32_t proxy_tid,
		uint32_t result_code, uint32_t generation, uint32_t void_time,
		struct as_msg_op_s** ops, struct as_bin_s** bins, uint16_t bin_count,
		struct as_namespace_s* ns, struct as_record_version_s* v);
void as_proxy_send_ops_response(cf_node dst, uint32_t proxy_tid, cf_dyn_buf* db, bool compress, struct as_proto_comp_stat_s* comp_stat);
