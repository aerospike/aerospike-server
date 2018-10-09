/* migrate_ce.c
 *
 * Copyright (C) 2016-2018 Aerospike, Inc.
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

#include "fabric/migrate.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/datamodel.h"
#include "fabric/fabric.h"


//==========================================================
// Typedefs & constants.
//

const uint32_t MY_MIG_FEATURES = 0;


//==========================================================
// Community Edition API.
//

void
emigrate_fill_queue_init()
{
}

void
emigrate_queue_push(emigration *emig)
{
	cf_queue_push(&g_emigration_q, &emig);
}

bool
should_emigrate_record(emigration *emig, as_index_ref *r_ref)
{
	return true;
}

uint32_t
emigration_pack_info(const emigration *emig, const as_record *r)
{
	return 0;
}

void
emigration_handle_meta_batch_request(cf_node src, msg *m)
{
	cf_warning(AS_MIGRATE, "CE node received meta-batch request - unexpected");
	as_fabric_msg_put(m);
}

bool
immigration_ignore_pickle(const uint8_t *buf, uint32_t info)
{
	return as_record_pickle_is_binless(buf);
}

void
immigration_init_repl_state(as_remote_record* rr, uint32_t info)
{
}

void
immigration_handle_meta_batch_ack(cf_node src, msg *m)
{
	cf_warning(AS_MIGRATE, "CE node received meta-batch ack - unexpected");
	as_fabric_msg_put(m);
}

bool
immigration_start_meta_sender(immigration *immig, uint32_t emig_features,
		uint64_t emig_partition_sz)
{
	return false;
}
