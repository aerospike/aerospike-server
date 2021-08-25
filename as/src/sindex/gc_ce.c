/*
 * gc_ce.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include "sindex/gc.h"

#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"


//==========================================================
// Private API - for enterprise separation only.
//

void
create_rlist(as_namespace* ns)
{
	ns->si_gc_rlist = cf_queue_create(sizeof(rlist_ele), false);
}

void
push_to_rlist(as_namespace* ns, as_index_ref* r_ref)
{
	rlist_ele ele = { .r_h = r_ref->r_h };

	cf_queue_push(ns->si_gc_rlist, &ele);
}

void
purge_rlist(as_namespace* ns, cf_queue* rlist)
{
	rlist_ele ele;

	while (cf_queue_pop(rlist, &ele, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
		as_index* r = (as_index*)cf_arenax_resolve(ns->arena, ele.r_h);

		cf_assert(r->in_sindex == 1, AS_SINDEX, "bad in_sindex bit");
		cf_assert(r->rc == 1, AS_SINDEX, "bad ref count %u", r->rc);

		as_record_destroy(r, ns);
		cf_arenax_free(ns->arena, ele.r_h, NULL);
	}

	cf_queue_destroy(rlist);
}
