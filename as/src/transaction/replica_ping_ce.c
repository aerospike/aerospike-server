/*
 * replica_ping_ce.c
 *
 * Copyright (C) 2017-2018 Aerospike, Inc.
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

#include "transaction/replica_ping.h"

#include <stdbool.h>

#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/datamodel.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

bool
repl_ping_check(as_transaction* tr)
{
	if (as_transaction_is_linearized_read(tr)) {
		cf_warning(AS_RW, "linearized read is an enterprise feature");
		tr->result_code = AS_ERR_ENTERPRISE_ONLY;
		return false;
	}

	return true;
}

void
repl_ping_make_message(rw_request* rw, as_transaction* tr)
{
	cf_crash(AS_RW, "CE code called repl_ping_make_message()");
}

void
repl_ping_setup_rw(rw_request* rw, as_transaction* tr,
		repl_ping_done_cb repl_ping_cb, timeout_done_cb timeout_cb)
{
	cf_crash(AS_RW, "CE code called repl_ping_setup_rw()");
}

void
repl_ping_reset_rw(rw_request* rw, as_transaction* tr, repl_ping_done_cb cb)
{
	cf_crash(AS_RW, "CE code called repl_ping_reset_rw()");
}

void
repl_ping_handle_op(cf_node node, msg* m)
{
	cf_warning(AS_RW, "CE code called repl_ping_handle_op()");
	as_fabric_msg_put(m);
}

void
repl_ping_handle_ack(cf_node node, msg* m)
{
	cf_warning(AS_RW, "CE code called repl_ping_handle_ack()");
	as_fabric_msg_put(m);
}
