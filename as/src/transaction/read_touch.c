/*
 * read_touch.c
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

#include "transaction/read_touch.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/mrt_utils.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

#define USE_CONFIG_DEFAULT 0
#define NEVER_TOUCH ((uint32_t)-1)


//==========================================================
// Forward declarations.
//

static void rt_enqueue(as_namespace* ns, const cf_digest* keyd, uint32_t ttl);

static void start_rt_repl_write(rw_request* rw, as_transaction* tr);
static void rt_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r);
static bool rt_dup_res_cb(rw_request* rw);
static void rt_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
static void rt_repl_write_cb(rw_request* rw);

static void rt_done(as_transaction* tr);
static void rt_timeout_cb(rw_request* rw);

static transaction_status read_touch_master(rw_request* rw, as_transaction* tr);
static void read_touch_master_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd, int result_code);


//==========================================================
// Inlines & macros.
//

static inline uint32_t
default_rt_ttl_pct(const as_namespace* ns, const as_set* p_set)
{
	if (p_set == NULL) {
		return ns->default_read_touch_ttl_pct;
	}

	uint32_t set_pct = as_load_uint32(&p_set->default_read_touch_ttl_pct);

	return set_pct == 0 ?
			ns->default_read_touch_ttl_pct :
			(set_pct == NEVER_TOUCH ? 0 : set_pct);
}

static inline void
rt_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_read_touch_success);
		break;
	default:
		as_incr_uint64(&ns->n_read_touch_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_read_touch_timeout);
		break;
	case AS_ERR_NOT_FOUND:
	case AS_ERR_KEY_BUSY: // internal only - closest to what really happens
		as_incr_uint64(&ns->n_read_touch_skip);
		break;
	}
}


//==========================================================
// Public API.
//

// Returns:
//  0: finish read, don't touch
//  1: need touch but reading prole - re-queue read and force proxy to master
// -1: bad input from client
int
as_read_touch_check(as_record* r, as_transaction* tr)
{
	if (r->void_time == 0) {
		return 0; // no need to touch
	}

	bool is_mrt = as_transaction_has_mrt_id(tr);

	// Ignore provisional/dual records for both MRT and ordinary reads.
	if ((is_mrt && is_mrt_provisional(r)) || (! is_mrt && is_mrt_original(r))) {
		return 0;
	}

	as_namespace* ns = tr->rsv.ns;
	uint32_t rt_ttl_pct = tr->msgp->msg.record_ttl;

	if (rt_ttl_pct == NEVER_TOUCH) {
		return 0; // overrides config
	}

	if (rt_ttl_pct == USE_CONFIG_DEFAULT) {
		rt_ttl_pct = default_rt_ttl_pct(ns, as_namespace_get_record_set(ns, r));

		if (rt_ttl_pct == 0) {
			return 0;
		}
	}
	else if (rt_ttl_pct > 100) {
		cf_warning(AS_RW, "{%s} bad read-touch-ttl-pct %u reading %pD",
				ns->name, rt_ttl_pct, &r->keyd);
		return -1;
	}

	uint32_t write_ttl = r->void_time - (r->last_update_time / 1000);
	uint32_t threshold_ttl = (uint32_t)((uint64_t)write_ttl * rt_ttl_pct / 100);
	uint32_t threshold_void_time = r->void_time - threshold_ttl;

	uint32_t now = cf_clepoch_seconds();

	if (now < threshold_void_time) {
		return 0;
	}
	// else - need a touch.

	// TODO - tweak void-time in index to avoid extra proxies/touches?

	if ((tr->flags & AS_TRANSACTION_FLAG_RSV_PROLE) != 0) {
		tr->from_flags |= FROM_FLAG_RESTART_STRICT;
		as_transaction_retry_self(tr); // retry the read
		return 1;
	}

	rt_enqueue(ns, &r->keyd, write_ttl);

	return 0;
}

transaction_status
as_read_touch_start(as_transaction* tr)
{
	// We think it's best to bypass the write queue backup check.

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->ix, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		cf_assert(status == TRANS_DONE, AS_RW, "read-touch not done");
		rw_request_release(rw);
		rt_done(tr);
		return TRANS_DONE;
	}
	// else - rw_request is now in hash, continue...

	// If there are duplicates to resolve, start doing so.
	if (tr->rsv.n_dupl != 0 && dup_res_start(rw, tr, rt_dup_res_start_cb)) {
		return TRANS_IN_PROGRESS; // started duplicate resolution
	}
	// else - no duplicate resolution phase, apply operation to master.

	if ((status = read_touch_master(rw, tr)) != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);

		if (status != TRANS_WAITING) {
			rt_done(tr);
		}

		return status;
	}

	if (rw->n_dest_nodes == 0) {
		rw_request_hash_delete(&hkey, rw);
		rt_done(tr);
		return TRANS_DONE;
	}

	start_rt_repl_write(rw, tr);

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction trigger.
//

static void
rt_enqueue(as_namespace* ns, const cf_digest* keyd, uint32_t ttl)
{
	as_transaction tr;
	uint8_t info2 = AS_MSG_INFO2_WRITE;

	// Note - digest is on transaction head before it's enqueued.
	as_transaction_init_head(&tr, keyd,
			as_msg_create_internal(ns->name, 0, info2, 0, ttl, 0, NULL, 0));

	as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_NAMESPACE);

	tr.origin = FROM_READ_TOUCH;
	tr.from.internal_origin = (void*)1; // anything so from.any is not NULL

	// Do this last, to exclude the setup time in this function.
	tr.start_time = cf_getns();

	as_service_enqueue_internal(&tr);
}


//==========================================================
// Local helpers - transaction flow.
//

static void
rt_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr, r);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, rt_dup_res_cb, rt_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_rt_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, rt_repl_write_cb, rt_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static bool
rt_dup_res_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		rt_done(&tr);
		return true;
	}

	transaction_status status = read_touch_master(rw, &tr);

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
		return true;
	}

	if (status == TRANS_DONE) {
		rt_done(&tr);
		return true;
	}

	if (rw->n_dest_nodes == 0) {
		rt_done(&tr);
		return true;
	}

	rt_repl_write_after_dup_res(rw, &tr);

	// Started replica write - don't delete rw_request from hash.
	return false;
}

static void
rt_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, rt_repl_write_cb);
	send_rw_messages(rw);
}

static void
rt_repl_write_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	// Only get here on success.
	rt_done(&tr);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

static void
rt_done(as_transaction* tr)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	cf_assert(tr->origin == FROM_READ_TOUCH, AS_RW,
			"unexpected transaction origin %u", tr->origin);

	HIST_ACTIVATE_INSERT_DATA_POINT(tr, read_touch_hist);
	rt_update_stats(tr->rsv.ns, tr->result_code);

//	tr->from.any = NULL; // no respond-on-master-complete
}

static void
rt_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	cf_assert(rw->origin == FROM_READ_TOUCH, AS_RW,
			"unexpected transaction origin %u", rw->origin);

	// Timeouts aren't included in histograms.
	rt_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - read touch master.
//

static transaction_status
read_touch_master(rw_request* rw, as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;

	if (ns->clock_skew_stop_writes) {
		tr->result_code = AS_ERR_FORBIDDEN;
		return TRANS_DONE;
	}

	as_index_tree* tree = tr->rsv.tree;
	as_index_ref r_ref;

	if (as_record_get(tree, &tr->keyd, &r_ref) != 0) {
		tr->result_code = AS_ERR_NOT_FOUND;
		return TRANS_DONE;
	}

	as_record* r = r_ref.r;

	if (r->void_time == 0 || is_mrt_provisional(r)) {
		read_touch_master_done(tr, &r_ref, NULL, AS_ERR_KEY_BUSY);
		return TRANS_DONE;
	}

	as_xdr_ship_status ship_status = as_xdr_ship_check(r, tr);

	if (ship_status == XDR_SHIP_NEAR) {
		as_record_done(&r_ref, ns);
		return TRANS_WAITING;
	}

	if (ship_status == XDR_SHIP_FAR) {
		read_touch_master_done(tr, &r_ref, NULL, AS_ERR_XDR_KEY_BUSY);
		return TRANS_DONE;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	// Set up the nodes to which we'll write replicas.
	if (! set_replica_destinations(tr, rw)) {
		read_touch_master_done(tr, &r_ref, &rd, AS_ERR_UNAVAILABLE);
		return TRANS_DONE;
	}

	// Will we need a pickle?
	rd.keep_pickle = rw->n_dest_nodes != 0;

	// Stack space for resulting record's bins.
	as_bin stack_bins[RECORD_MAX_BINS];

	int result = as_storage_rd_load_bins(&rd, stack_bins);

	if (result < 0) {
		read_touch_master_done(tr, &r_ref, &rd, -result);
		return TRANS_DONE;
	}

	// Shortcut for set name storage.
	as_storage_record_get_set_name(&rd);

	// Deal with key storage as needed.
	if (r->key_stored == 1 && ! as_storage_rd_load_key(&rd)) {
		read_touch_master_done(tr, &r_ref, &rd, AS_ERR_UNKNOWN);
		return TRANS_DONE;
	}

	uint64_t old_last_update_time = r->last_update_time;
	uint16_t old_generation = r->generation; // includes regime!
	uint32_t old_void_time = r->void_time;

	uint64_t now = as_transaction_epoch_ms(tr);

	as_record_advance_void_time(r, tr->msgp->msg.record_ttl, now, ns);
	as_record_set_lut(r, tr->rsv.regime, now, ns);
	// Don't increment generation - causes extraneous gen-check failures.

	// If a write extended the void-time we don't need to touch.
	if (old_void_time >= r->void_time) {
		r->last_update_time = old_last_update_time;
		r->generation = old_generation;
		r->void_time = old_void_time;
		read_touch_master_done(tr, &r_ref, &rd, AS_ERR_KEY_BUSY);
		return TRANS_DONE;
	}

	if ((result = as_storage_record_write(&rd)) < 0) {
		r->last_update_time = old_last_update_time;
		r->generation = old_generation;
		r->void_time = old_void_time;
		read_touch_master_done(tr, &r_ref, &rd, -result);
		return TRANS_DONE;
	}

	pickle_all(&rd, rw);

	// Make sure these go in the replica write.
	tr->generation = r->generation;
	tr->void_time = r->void_time;
	tr->last_update_time = r->last_update_time;

	// Save for XDR submit outside record lock.
	as_xdr_submit_info submit_info;

	as_xdr_get_submit_info(r, old_last_update_time, &submit_info);

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	// Note - not bothering to check ns->xdr_ships_drops.
	as_xdr_submit(ns, &submit_info);

	return TRANS_IN_PROGRESS;
}

static void
read_touch_master_done(as_transaction* tr, as_index_ref* r_ref,
		as_storage_rd* rd, int result_code)
{
	if (rd != NULL) {
		as_storage_record_close(rd);
	}

	as_record_done(r_ref, tr->rsv.ns);

	tr->result_code = (uint8_t)result_code;
}
