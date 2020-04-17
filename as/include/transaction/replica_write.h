/*
 * replica_write.h
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

#include "msg.h"
#include "node.h"

#include "transaction/rw_request.h"


//==========================================================
// Forward declarations.
//

struct as_transaction_s;
struct rw_request_s;


//==========================================================
// Public API.
//

void repl_write_make_message(struct rw_request_s* rw, struct as_transaction_s* tr);
void repl_write_setup_rw(struct rw_request_s* rw, struct as_transaction_s* tr, repl_write_done_cb repl_write_cb, timeout_done_cb timeout_cb);
void repl_write_reset_rw(struct rw_request_s* rw, struct as_transaction_s* tr, repl_write_done_cb cb);
void repl_write_handle_op(cf_node node, msg* m);
void repl_write_handle_ack(cf_node node, msg* m);
