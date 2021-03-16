/*
 * rw_utils_ce.c
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "log.h"
#include "msg.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

bool
convert_to_write(as_transaction* tr, cl_msg** p_msgp)
{
	return false;
}


int
validate_delete_durability(as_transaction* tr)
{
	if (as_transaction_is_durable_delete(tr)) {
		cf_warning(AS_RW, "durable delete is an enterprise feature");
		return AS_ERR_ENTERPRISE_ONLY;
	}

	return AS_OK;
}


int
repl_state_check(as_record* r, as_transaction* tr)
{
	return 0;
}


void
will_replicate(as_record* r, as_namespace* ns)
{
}


bool
write_is_full_drop(const as_transaction* tr)
{
	return IS_DROP(tr);
}


bool
sufficient_replica_destinations(const as_namespace* ns, uint32_t n_dests)
{
	return true;
}


bool
set_replica_destinations(as_transaction* tr, rw_request* rw)
{
	rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p,
			rw->dest_nodes);

	return true;
}


void
finished_replicated(as_transaction* tr)
{
}


void
finished_not_replicated(rw_request* rw)
{
}


bool
generation_check(const as_record* r, const as_msg* m, const as_namespace* ns)
{
	if ((m->info2 & AS_MSG_INFO2_GENERATION) != 0) {
		return m->generation == r->generation;
	}

	if ((m->info2 & AS_MSG_INFO2_GENERATION_GT) != 0) {
		return m->generation > r->generation;
	}

	return true; // no generation requirement
}


bool
forbid_replace(const as_namespace* ns)
{
	return false;
}


void
prepare_bin_metadata(const as_transaction* tr, as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;

	rd->bin_luts = false; // no usage with independent LUTs yet

	if (rd->bin_luts) {
		for (uint32_t i = 0; i < rd->n_bins; i++) {
			as_bin* b = &rd->bins[i];

			// Preserve LUT shared with record in case bin is not written.
			if (b->lut == 0) {
				b->lut = r->last_update_time;
			}
		}

		return;
	}

	if (! ns->storage_data_in_memory || r->has_bin_meta) {
		// Remove all metadata.
		for (uint32_t i = 0; i < rd->n_bins; i++) {
			rd->bins[i].lut = 0;
		}
	}
}


void
stash_index_metadata(const as_record* r, index_metadata* old)
{
	old->void_time = r->void_time;
	old->last_update_time = r->last_update_time;
	old->generation = r->generation;

	old->has_bin_meta = r->has_bin_meta == 1;
}


void
unwind_index_metadata(const index_metadata* old, as_record* r)
{
	r->void_time = old->void_time;
	r->last_update_time = old->last_update_time;
	r->generation = old->generation;

	r->has_bin_meta = old->has_bin_meta ? 1 : 0;
}


void
set_xdr_write(const as_transaction* tr, as_record* r)
{
}


void
touch_bin_metadata(as_storage_rd* rd)
{
	if (rd->bin_luts) { // no usage with independent LUTs yet
		for (uint32_t i = 0; i < rd->n_bins; i++) {
			rd->bins[i].lut = 0;
		}
	}
}


void
transition_delete_metadata(as_transaction* tr, as_record* r, bool is_delete,
		bool is_bin_cemetery)
{
}


bool
forbid_resolve(const as_transaction* tr, const as_storage_rd* rd,
		uint64_t msg_lut)
{
	return false;
}


bool
resolve_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut,
		uint16_t n_ops, uint16_t* n_won, int* result)
{
	return true;
}


bool
udf_resolve_bin(as_storage_rd* rd, const char* name)
{
	return true;
}


bool
delete_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut,
		as_bin* cleanup_bins, uint32_t* p_n_cleanup_bins, int* result)
{
	as_bin cleanup_bin;

	if (as_bin_pop_w_len(rd, op->name, op->name_sz, &cleanup_bin) &&
			rd->ns->storage_data_in_memory) {
		append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
	}

	return true;
}


bool
udf_delete_bin(as_storage_rd* rd, const char* name, as_bin* cleanup_bins,
		uint32_t* p_n_cleanup_bins, int* result)
{
	as_bin cleanup_bin;

	if (as_bin_pop(rd, name, &cleanup_bin) && rd->ns->storage_data_in_memory) {
		append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
	}

	return true;
}


void
write_resolved_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut,
		as_bin* b)
{
}


// Caller has already handled destroying all bins' particles.
void
delete_all_bins(as_storage_rd* rd)
{
	rd->n_bins = 0;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
write_delete_record(as_record* r, as_index_tree* tree)
{
	as_index_delete(tree, &r->keyd);
}


uint32_t
dup_res_pack_repl_state_info(const as_record* r, const as_namespace* ns)
{
	return 0;
}


bool
dup_res_should_retry_transaction(rw_request* rw, uint32_t result_code)
{
	return false;
}


void
dup_res_handle_tie(rw_request* rw, const msg* m, uint32_t result_code)
{
}


void
apply_if_tie(rw_request* rw)
{
}


void
dup_res_translate_result_code(rw_request* rw)
{
	rw->result_code = AS_OK;
}


void
dup_res_init_repl_state(as_remote_record* rr, uint32_t info)
{
}


void
repl_write_init_repl_state(as_remote_record* rr, bool from_replica)
{
}


conflict_resolution_pol
repl_write_conflict_resolution_policy(const as_namespace* ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}


bool
repl_write_should_retransmit_replicas(rw_request* rw, uint32_t result_code)
{
	switch (result_code) {
	case AS_ERR_CLUSTER_KEY_MISMATCH:
		rw->xmit_ms = 0; // force retransmit on next cycle
		return true;
	default:
		return false;
	}
}


void
repl_write_send_confirmation(rw_request* rw)
{
}


void
repl_write_handle_confirmation(msg* m)
{
}


int
record_replace_check(as_record* r, as_namespace* ns)
{
	return 0;
}


void
record_replaced(as_record* r, as_remote_record* rr)
{
}
