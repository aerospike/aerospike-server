/*
 * delete_ce.c
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

#include "transaction/delete.h"

#include <stdbool.h>

#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "transaction/rw_request.h"


//==========================================================
// Private API - for enterprise separation only.
//

bool
delete_storage_overloaded(as_transaction* tr)
{
	return false;
}


transaction_status
delete_master(as_transaction* tr, rw_request* rw)
{
	if (as_transaction_is_durable_delete(tr)) {
		cf_warning(AS_RW, "durable delete is an enterprise feature");
		tr->result_code = AS_PROTO_RESULT_FAIL_ENTERPRISE_ONLY;
		return TRANS_DONE_ERROR;
	}

	as_index_ref r_ref;
	r_ref.skip_lock = false;

	if (0 != as_record_get(tr->rsv.tree, &tr->keyd, &r_ref)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_NOT_FOUND;
		return TRANS_DONE_ERROR;
	}

	return drop_master(tr, &r_ref, rw);
}
