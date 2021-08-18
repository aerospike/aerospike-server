/*
 * delete_ce.c
 *
 * Copyright (C) 2016-2021 Aerospike, Inc.
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

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "transaction/rw_request.h"
#include "transaction/rw_utils.h"


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
	as_index_ref r_ref;

	if (as_record_get(tr->rsv.tree, &tr->keyd, &r_ref) != 0) {
		tr->result_code = AS_ERR_NOT_FOUND;
		return TRANS_DONE_ERROR;
	}

	// Make sure the message set name (if it's there) is correct.
	if (! set_name_check(tr, r_ref.r)) {
		as_record_done(&r_ref, tr->rsv.ns);
		tr->result_code = AS_ERR_PARAMETER;
		return TRANS_DONE_ERROR;
	}

	return drop_master(tr, &r_ref, rw);
}

bool
drop_local(as_namespace* ns, as_partition_reservation* rsv, as_index_ref* r_ref)
{
	as_record* r = r_ref->r;

	if (ns->storage_data_in_memory) {
		remove_from_sindex(ns, r_ref);
	}

	as_set_index_delete(ns, rsv->tree, as_index_get_set_id(r), r_ref->r_h);
	as_index_delete(rsv->tree, &r->keyd);
	as_record_done(r_ref, ns);

	return true;
}
