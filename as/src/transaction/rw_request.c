/*
 * rw_request.c
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

//==========================================================
// Includes.
//

#include "transaction/rw_request.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"


//==========================================================
// Typedefs & constants.
//

typedef struct rw_wait_ele_s {
	uint8_t tr_head[AS_TRANSACTION_HEAD_SIZE];
	struct rw_wait_ele_s* next;
} rw_wait_ele;


//==========================================================
// Globals.
//

static cf_atomic32 g_rw_tid = 0;


//==========================================================
// Public API.
//

rw_request*
rw_request_create(cf_digest* keyd)
{
	rw_request* rw = cf_rc_alloc(sizeof(rw_request));

	// as_transaction look-alike:
	rw->msgp				= NULL;
	rw->msg_fields			= 0;
	rw->origin				= 0;
	rw->from_flags			= 0;
	rw->from.any			= NULL;
	rw->from_data.any		= 0;
	rw->keyd				= *keyd;
	rw->start_time			= 0;
	rw->benchmark_time		= 0;

	AS_PARTITION_RESERVATION_INIT(rw->rsv);

	rw->end_time			= 0;
	rw->result_code			= AS_OK;
	rw->flags				= 0;
	rw->generation			= 0;
	rw->void_time			= 0;
	rw->last_update_time	= 0;
	// End of as_transaction look-alike.

	cf_mutex_init(&rw->lock);

	rw->wait_queue_head = NULL;
	rw->wait_queue_tail = NULL;
	rw->wait_queue_depth = 0;

	rw->is_set_up = false;

	rw->is_old_pickle = false;
	rw->pickle = NULL;
	rw->pickle_sz = 0;
	rw->set_name = NULL;
	rw->set_name_len = 0;
	rw->key = NULL;
	rw->key_size = 0;

	rw->response_db.buf = NULL;
	rw->response_db.is_stack = false;
	rw->response_db.alloc_sz = 0;
	rw->response_db.used_sz = 0;

	rw->tid = cf_atomic32_incr(&g_rw_tid);
	rw->dup_res_complete = false;
	rw->repl_write_complete = false;
	rw->repl_ping_complete = false;
	rw->dup_res_cb = NULL;
	rw->repl_write_cb = NULL;
	rw->repl_ping_cb = NULL;
	rw->timeout_cb = NULL;

	rw->dest_msg = NULL;
	rw->xmit_ms = 0;
	rw->retry_interval_ms = 0;

	rw->n_dest_nodes = 0;

	rw->best_dup_msg = NULL;
	rw->best_dup_result_code = AS_OK;
	rw->best_dup_gen = 0;
	rw->best_dup_lut = 0;

	rw->tie_was_replicated = false;

	rw->repl_start_us = 0;

	return rw;
}


void
rw_request_destroy(rw_request* rw)
{
	// Paranoia:
	if (rw->from.any) {
		cf_crash(AS_RW, "rw_request_destroy: origin %d has non-null 'from'",
				rw->origin);
	}

	if (rw->msgp && rw->origin != FROM_BATCH) {
		cf_free(rw->msgp);
	}

	if (rw->pickle) {
		cf_free(rw->pickle);
	}

	if (rw->key) {
		cf_free(rw->key);
	}

	cf_dyn_buf_free(&rw->response_db);

	if (rw->dest_msg) {
		as_fabric_msg_put(rw->dest_msg);
	}

	if (rw->is_set_up) {
		if (rw->best_dup_msg) {
			as_fabric_msg_put(rw->best_dup_msg);
		}

		as_partition_release(&rw->rsv);
	}

	cf_mutex_destroy(&rw->lock);

	rw_wait_ele* e = rw->wait_queue_head;

	while (e) {
		rw_wait_ele* next = e->next;
		as_transaction* tr = (as_transaction*)e->tr_head;

		tr->from_flags |= FROM_FLAG_RESTART;
		as_tsvc_enqueue(tr);

		cf_free(e);
		e = next;
	}
}


void
rw_request_wait_q_push(rw_request* rw, as_transaction* tr)
{
	rw_wait_ele* e = cf_malloc(sizeof(rw_wait_ele));

	as_transaction_copy_head((as_transaction*)e->tr_head, tr);
	tr->from.any = NULL;
	tr->msgp = NULL;

	e->next = NULL;

	if (rw->wait_queue_tail) {
		rw->wait_queue_tail->next = e;
		rw->wait_queue_tail = e;
	}
	else {
		rw->wait_queue_head = e;
		rw->wait_queue_tail = e;
	}

	rw->wait_queue_depth++;
}


void
rw_request_wait_q_push_head(rw_request* rw, as_transaction* tr)
{
	rw_wait_ele* e = cf_malloc(sizeof(rw_wait_ele));
	cf_assert(e, AS_RW, "alloc rw_wait_ele");

	as_transaction_copy_head((as_transaction*)e->tr_head, tr);
	tr->from.any = NULL;
	tr->msgp = NULL;

	e->next = rw->wait_queue_head;
	rw->wait_queue_head = e;

	if (! rw->wait_queue_tail) {
		rw->wait_queue_tail = e;
	}

	rw->wait_queue_depth++;
}
