/*
 * re_replicate_ce.c
 *
 * Copyright (C) 2017-2019 Aerospike, Inc.
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

#include "transaction/re_replicate.h"

#include "fault.h"

#include "base/transaction.h"


//==========================================================
// Public API.
//

transaction_status
as_re_replicate_start(as_transaction* tr)
{
	cf_crash(AS_RW, "CE code called as_re_replicate_start()");
	return TRANS_DONE_ERROR;
}
