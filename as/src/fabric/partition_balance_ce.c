/*
 * partition_balance_ce.c
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

#include "fabric/partition_balance.h"

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_queue.h"

#include "dynbuf.h"
#include "fault.h"
#include "node.h"

#include "base/datamodel.h"
#include "fabric/partition.h"
#include "fabric/migrate.h"


//==========================================================
// Public API.
//

void
as_partition_balance_emigration_yield()
{
}

bool
as_partition_balance_revive(as_namespace* ns)
{
	cf_warning(AS_PARTITION, "revive is an enterprise feature");
	return true;
}

void
as_partition_balance_protect_roster_set(as_namespace* ns)
{
}

void
as_partition_balance_effective_rack_ids(cf_dyn_buf* db)
{
	cf_crash(AS_PARTITION, "CE code called as_partition_balance_effective_rack_ids()");
}

bool
as_partition_pre_emigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t tx_flags)
{
	return true;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
partition_balance_init()
{
}

void
balance_namespace(as_namespace* ns, cf_queue* mq)
{
	balance_namespace_ap(ns, mq);
}

void
prepare_for_appeals()
{
}

void
process_pb_tasks(cf_queue* tq)
{
	pb_task task;

	while (cf_queue_pop(tq, &task, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
		as_migrate_emigrate(&task);
	}
}

void
set_active_size(as_namespace* ns)
{
	ns->active_size = ns->cluster_size;
}

uint32_t
rack_count(const as_namespace* ns)
{
	return 1;
}

void
init_target_claims_ap(const as_namespace* ns, const int translation[],
		uint32_t* target_claims)
{
	cf_crash(AS_PARTITION, "CE code called init_target_claims_ap()");
}

void
quiesce_adjust_row(cf_node* ns_node_seq, sl_ix_t* ns_sl_ix, as_namespace* ns)
{
	cf_crash(AS_PARTITION, "CE code called quiesce_adjust_row()");
}

void
uniform_adjust_row(cf_node* node_seq, uint32_t n_nodes, sl_ix_t* ns_sl_ix,
		uint32_t n_replicas, uint32_t* claims, const uint32_t* target_claims,
		const uint32_t* rack_ids, uint32_t n_racks)
{
	cf_crash(AS_PARTITION, "CE code called uniform_adjust_row()");
}

void
rack_aware_adjust_row(cf_node* ns_node_seq, sl_ix_t* ns_sl_ix,
		uint32_t replication_factor, const uint32_t* rack_ids, uint32_t n_ids,
		uint32_t n_racks, uint32_t start_n)
{
	cf_crash(AS_PARTITION, "CE code called rack_aware_adjust_row()");
}

void
emig_lead_flags_ap(const as_partition* p, const sl_ix_t* ns_sl_ix,
		const as_namespace* ns, uint32_t lead_flags[])
{
	for (uint32_t repl_ix = 0; repl_ix < ns->replication_factor; repl_ix++) {
		lead_flags[repl_ix] = TX_FLAGS_LEAD;
	}
}

bool
drop_superfluous_version(as_partition* p, as_namespace* ns)
{
	p->version = ZERO_VERSION;

	return true;
}

bool
adjust_superfluous_version(as_partition* p, as_namespace* ns)
{
	cf_crash(AS_PARTITION, "CE code called adjust_superfluous_version()");
	return false;
}

void
emigrate_done_advance_non_master_version(as_namespace* ns, as_partition* p,
		uint32_t tx_flags)
{
	emigrate_done_advance_non_master_version_ap(ns, p, tx_flags);
}

void
immigrate_start_advance_non_master_version(as_namespace* ns, as_partition* p)
{
	immigrate_start_advance_non_master_version_ap(p);
}

void
immigrate_done_advance_final_master_version(as_namespace* ns, as_partition* p)
{
	immigrate_done_advance_final_master_version_ap(ns, p);
}

bool
immigrate_yield()
{
	return false;
}
