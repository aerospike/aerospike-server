/*
 * aggr.c
 *
 * Copyright (C) 2014-2015 Aerospike, Inc.
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

#include "base/aggr.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>


#include "aerospike/as_val.h"
#include "aerospike/mod_lua.h"
#include "citrusleaf/cf_ll.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/udf_arglist.h"
#include "base/udf_memtracker.h"
#include "base/udf_record.h"
#include "fabric/partition.h"


#define AS_AGGR_ERR  -1
#define AS_AGGR_OK    0

/*
 * Aggregation Stream Object
 */
// **************************************************************************************************
typedef struct {
	// Iteration
	cf_ll_iterator        * iter;
	as_index_keys_arr     * keys_arr;
	int                     keys_arr_offset;

	// Record
	bool                       rec_open; // Record in stream open
	as_rec                   * urec;     // UDF record cloak
	as_namespace             * ns;
	as_partition_reservation * rsv;      // Reservation Object

	// Module Data
	as_aggr_call          * call;   // Aggregation info
	void                  * udata;  // Execution context
} aggr_state;

static as_partition_reservation *
ptn_reserve(aggr_state *astate, uint32_t pid, as_partition_reservation *rsv)
{
	as_aggr_call *call = astate->call;
	if (call && call->aggr_hooks && call->aggr_hooks->ptn_reserve) {
		return call->aggr_hooks->ptn_reserve(astate->udata, astate->ns, pid, rsv);
	}
	return NULL;
}

static void
ptn_release(aggr_state *astate)
{
	as_aggr_call  *call = astate->call;
	if (call && call->aggr_hooks && call->aggr_hooks->ptn_release) {
		call->aggr_hooks->ptn_release(astate->udata, astate->rsv);
	}
}

#if 0
// In case we ever need this hook...
static void
set_error(aggr_state *astate, int err)
{
	as_aggr_call  *call = astate->call;
	if (call && call->aggr_hooks && call->aggr_hooks->set_error) {
		call->aggr_hooks->set_error(astate->udata, err);
	}
}
#endif // 0

static bool
pre_check(aggr_state *astate, void *skey)
{
	as_aggr_call  *call = astate->call;
	if (call && call->aggr_hooks && call->aggr_hooks->pre_check) {
		return call->aggr_hooks->pre_check(astate->udata, as_rec_source(astate->urec), skey);
	}
	return true; // if not defined pre_check succeeds
}

static int
aopen(aggr_state *astate, const cf_digest *digest)
{
	udf_record   * urecord  = as_rec_source(astate->urec);
	as_index_ref   * r_ref  = urecord->r_ref;
	as_transaction * tr     = urecord->tr;

	int pid                = as_partition_getid(digest);
	urecord->keyd = *digest;

	astate->rsv        = ptn_reserve(astate, pid, &tr->rsv);
	if (!astate->rsv) {
		cf_debug(AS_AGGR, "Reservation not done for partition %d", pid);
		return -1;
	}

	// NB: Partial Initialization due to heaviness. Not everything needed
	// TODO: Make such initialization Commodity
	tr->rsv.ns          = astate->rsv->ns;
	tr->rsv.p           = astate->rsv->p;
	tr->rsv.tree        = astate->rsv->tree;
	tr->keyd            = urecord->keyd;

	r_ref->skip_lock    = false;
	if (udf_record_open(urecord) == 0) {
		astate->rec_open   = true;
		return 0;
	}
	ptn_release(astate);
	return -1;
}

void
aclose(aggr_state *astate)
{
	// Bypassing doing the direct destroy because we need to
	// avoid reducing the ref count. This rec (query_record
	// implementation of as_rec) is ref counted when passed from
	// here to Lua. If Lua access it even after moving to next
	// element in the stream it does it at its own risk. Record
	// may have changed under the hood.
	if (astate->rec_open) {
		udf_record_close(as_rec_source(astate->urec));
		ptn_release(astate);
		astate->rec_open = false;
	}
	return;
}

void
acleanup(aggr_state *astate)
{
	if (astate->iter) {
		cf_ll_releaseIterator(astate->iter);
		astate->iter = NULL;
	}
	aclose(astate);

	as_rec_destroy(astate->urec);
}

// **************************************************************************************************

/*
 * Aggregation Input Stream
 */
// **************************************************************************************************
cf_digest *
get_next(aggr_state *astate)
{
	astate->keys_arr_offset++;
	if (!astate->keys_arr || (astate->keys_arr_offset == astate->keys_arr->num)) {

		cf_ll_element * ele = cf_ll_getNext(astate->iter);

		// if NULL or number of element 0. No holes expected
		if (!ele) {
			return NULL;
		}

		astate->keys_arr    = ((as_index_keys_ll_element*)ele)->keys_arr;
		if (!astate->keys_arr || (astate->keys_arr->num < 1)) {
			astate->keys_arr = NULL;
			return NULL;
		}

		astate->keys_arr_offset = 0;
	}
	return &astate->keys_arr->pindex_digs[astate->keys_arr_offset];
}

// only operates on the record as_val in the stream points to
// and updates the references ... this function has to acquire
// partition reservation and also the object lock. So if the UDF
// does something stupid the object lock is gonna get held for
// a while ... there has to be timeout mechanism in here I think
static as_val *
istream_read(const as_stream *s)
{
	aggr_state *astate = as_stream_source(s);

	aclose(astate);

	// Iterate through stream to get next digest and
	// populate record with it
	while (!astate->rec_open) {

		if (get_next(astate) == NULL) {
			return NULL;
		}

		if (!aopen(astate, &astate->keys_arr->pindex_digs[astate->keys_arr_offset])) {
			if (!pre_check(astate, &astate->keys_arr->sindex_keys[astate->keys_arr_offset])) {
				aclose(astate);
			}
		}
	}
	return (as_val *)astate->urec;
}

const as_stream_hooks istream_hooks = {
		.destroy	= NULL,
		.read		= istream_read,
		.write		= NULL
};
// **************************************************************************************************



/*
 * Aggregation Output Stream
 */
// **************************************************************************************************
as_stream_status
ostream_write(const as_stream *s, as_val *val)
{
	aggr_state *astate = (aggr_state *)as_stream_source(s);
	return astate->call->aggr_hooks->ostream_write(astate->udata, val);
}

const as_stream_hooks ostream_hooks = {
		.destroy	= NULL,
		.read		= NULL,
		.write		= ostream_write
};
// **************************************************************************************************


/*
 * Aggregation AS_AEROSPIKE interface for LUA
 */
// **************************************************************************************************
static int
as_aggr_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	cf_fault_event(AS_AGGR, lvl, file, line, "%s", (char *) msg);
	return 0;
}

static const as_aerospike_hooks as_aggr_aerospike_hooks = {
	.rec_update       = NULL,
	.rec_remove       = NULL,
	.rec_exists       = NULL,
	.log              = as_aggr_aerospike_log,
	.get_current_time = NULL,
	.destroy          = NULL
};
// **************************************************************************************************



int
as_aggr_process(as_namespace *ns, as_aggr_call * ag_call, cf_ll * ap_recl, void * udata, as_result * ap_res)
{
	as_index_ref    r_ref;
	r_ref.skip_lock   = false;
	as_storage_rd   rd;
	bzero(&rd, sizeof(as_storage_rd));
	as_transaction  tr;


	udf_record urecord;
	udf_record_init(&urecord, false);
	urecord.tr      = &tr;
	urecord.r_ref   = &r_ref;
	urecord.rd      = &rd;
	as_rec   * urec = as_rec_new(&urecord, &udf_record_hooks);

	aggr_state astate = {
		.iter            = cf_ll_getIterator(ap_recl, true /*forward*/),
		.urec            = urec,
		.keys_arr        = NULL,
		.keys_arr_offset = 0,
		.call            = ag_call,
		.udata           = udata,
		.rec_open        = false,
		.rsv             = &tr.rsv,
		.ns              = ns
	};

	if (!astate.iter) {
		cf_warning (AS_AGGR, "Could not set up iterator .. possibly out of memory .. Aborting Query !!");
		as_rec_destroy(urec);
		return AS_AGGR_ERR;
	}

	as_aerospike as;
	as_aerospike_init(&as, NULL, &as_aggr_aerospike_hooks);

	// Input Stream
	as_stream istream;
	as_stream_init(&istream, &astate, &istream_hooks);

	// Output stream
	as_stream ostream;
	as_stream_init(&ostream, &astate, &ostream_hooks);

	as_udf_context ctx = {
		.as         = &as,
		.timer      = NULL,
		.memtracker = NULL
	};
	int ret = as_module_apply_stream(&mod_lua, &ctx, ag_call->def.filename, ag_call->def.function, &istream, ag_call->def.arglist, &ostream, ap_res);

	acleanup(&astate);
	return ret;
}
