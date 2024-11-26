/*
 * mrt_roll_ce.c
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

//==========================================================
// Includes.
//

#include "transaction/mrt_roll.h"

#include <stddef.h>

#include "log.h"

#include "base/batch.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "transaction/proxy.h"


//==========================================================
// Public API.
//

transaction_status
as_mrt_roll_start(as_transaction* tr)
{
	cf_warning(AS_RW, "MRTs are enterprise only");

	tr->result_code = AS_ERR_ENTERPRISE_ONLY;

	switch (tr->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(tr->from.proto_fd_h, tr->result_code, tr->generation,
				tr->void_time, NULL, NULL, 0, tr->rsv.ns, NULL);
		break;
	case FROM_PROXY:
		as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
				tr->result_code, tr->generation, tr->void_time, NULL, NULL, 0,
				tr->rsv.ns, NULL);
		break;
	case FROM_BATCH:
		as_batch_add_ack(tr, NULL);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	return TRANS_DONE;
}
