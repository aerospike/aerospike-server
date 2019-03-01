/*
 * fabric.h
 *
 * Copyright (C) 2008-2018 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "msg.h"
#include "node.h"
#include "socket.h"
#include "tls.h"


//==========================================================
// Forward declarations.
//

struct as_endpoint_list_s;
struct as_hb_plugin_node_data_s;


//==========================================================
// Typedefs & constants.
//

#define AS_FABRIC_SUCCESS			(0)
#define AS_FABRIC_ERR_UNKNOWN		(-1)	// used by transact
#define AS_FABRIC_ERR_NO_NODE		(-3)
#define AS_FABRIC_ERR_TIMEOUT		(-6)	// used by transact

typedef enum {
	AS_FABRIC_CHANNEL_RW = 0,	// duplicate resolution and replica writes
	AS_FABRIC_CHANNEL_CTRL = 1,	// clustering, migration ctrl and services info
	AS_FABRIC_CHANNEL_BULK = 2,	// migrate records
	AS_FABRIC_CHANNEL_META = 3,	// smd

	AS_FABRIC_N_CHANNELS
} as_fabric_channel;

#define MAX_FABRIC_CHANNEL_THREADS 128
#define MAX_FABRIC_CHANNEL_SOCKETS 128

typedef struct fabric_rate_s {
	uint64_t s_bytes[AS_FABRIC_N_CHANNELS];
	uint64_t r_bytes[AS_FABRIC_N_CHANNELS];
} fabric_rate;

typedef int (*as_fabric_msg_fn) (cf_node node_id, msg *m, void *udata);
typedef int (*as_fabric_transact_recv_fn) (cf_node node_id, msg *m, void *transact_data, void *udata);
typedef int (*as_fabric_transact_complete_fn) (msg *rsp, void *udata, int err);


//==========================================================
// Globals.
//

extern cf_serv_cfg g_fabric_bind;
extern cf_tls_info *g_fabric_tls;


//==========================================================
// Public API.
//

//------------------------------------------------
// msg
//

void as_fabric_msg_queue_dump(void);

static inline msg *
as_fabric_msg_get(msg_type type)
{
	// Never returns NULL. Will assert if type is not registered.
	return msg_create(type);
}

static inline void
as_fabric_msg_put(msg *m)
{
	if (cf_rc_release(m) == 0) {
		msg_reset(m);
		msg_put(m);
	}
}

//------------------------------------------------
// as_fabric
//

void as_fabric_init(void);
void as_fabric_start(void);
void as_fabric_set_recv_threads(as_fabric_channel channel, uint32_t count);
int as_fabric_send(cf_node node_id, msg *m, as_fabric_channel channel);
int as_fabric_send_list(const cf_node *nodes, uint32_t node_count, msg *m, as_fabric_channel channel);
int as_fabric_retransmit(cf_node node_id, msg *m, as_fabric_channel channel);
void as_fabric_register_msg_fn(msg_type type, const msg_template *mt, size_t mt_sz, size_t scratch_sz, as_fabric_msg_fn msg_cb, void *msg_udata);
void as_fabric_info_peer_endpoints_get(cf_dyn_buf *db);
bool as_fabric_is_published_endpoint_list(const struct as_endpoint_list_s *list);
struct as_endpoint_list_s *as_fabric_hb_plugin_get_endpoint_list(struct as_hb_plugin_node_data_s *plugin_data);
void as_fabric_rate_capture(fabric_rate *rate);
void as_fabric_dump(bool verbose);
