/*
 * rw_utils.c
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

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "msg.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "sindex/secondary_index.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"
#include "transaction/udf.h"
#include "transaction/write.h"


//==========================================================
// Public API.
//

// TODO - really? we can't hide this behind an XDR stub?
bool
xdr_allows_write(as_transaction* tr)
{
	if (as_transaction_is_xdr(tr)) {
		if (! tr->rsv.ns->reject_xdr_writes) {
			return true;
		}
	}
	else {
		if (! tr->rsv.ns->reject_non_xdr_writes) {
			return true;
		}
	}

	cf_atomic_int_incr(&tr->rsv.ns->n_fail_xdr_forbidden);

	return false;
}


void
send_rw_messages(rw_request* rw)
{
	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		if (rw->dest_complete[i]) {
			continue;
		}

		msg_incr_ref(rw->dest_msg);

		if (as_fabric_send(rw->dest_nodes[i], rw->dest_msg,
				AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(rw->dest_msg);
			rw->xmit_ms = 0; // force a retransmit on next cycle
		}
	}
}


void
send_rw_messages_forget(rw_request* rw)
{
	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		msg_incr_ref(rw->dest_msg);

		if (as_fabric_send(rw->dest_nodes[i], rw->dest_msg,
				AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(rw->dest_msg);
		}
	}
}


bool
set_name_check(const as_transaction* tr, const as_record* r)
{
	if (! as_transaction_has_set(tr)) {
		return true; // allowed to not send set name in read or delete message
	}

	as_msg_field* f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET);
	uint32_t msg_set_name_len = as_msg_field_get_value_sz(f);

	if (msg_set_name_len == 0) {
		return true; // treat the same as no set name
	}

	as_namespace* ns = tr->rsv.ns;
	const char* set_name = as_index_get_set_name(r, ns);

	if (set_name == NULL ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		cf_warning(AS_RW, "{%s} set name mismatch %s %.*s (%u)", ns->name,
				set_name == NULL ? "(null)" : set_name, msg_set_name_len,
						f->data, msg_set_name_len);
		return false;
	}

	return true;
}


int
set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
	size_t name_len = (size_t)as_msg_field_get_value_sz(f);

	if (name_len == 0) {
		return 0;
	}

	// Given the name, find/assign the set-ID and write it in the as_index.
	return as_index_set_set_w_len(r, ns, (const char*)f->data, name_len, true);
}


int
handle_meta_filter(const as_transaction* tr, const as_record* r, as_exp** exp)
{
	switch (tr->origin) {
	case FROM_BATCH:
		if ((*exp = as_batch_get_predexp(tr->from.batch_shared)) == NULL) {
			return AS_OK;
		}
		break;
	case FROM_IUDF:
		*exp = tr->from.iudf_orig->filter_exp;
		return AS_OK; // meta filter was applied upstream - no need here
	case FROM_IOPS:
		*exp = tr->from.iops_orig->filter_exp;
		return AS_OK; // meta filter was applied upstream - no need here
	default:
		if (! as_transaction_has_predexp(tr)) {
			*exp = NULL;
			return AS_OK;
		}
		as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
				AS_MSG_FIELD_TYPE_PREDEXP);
		if ((*exp = as_exp_filter_build(f, false)) == NULL) {
			return AS_ERR_PARAMETER;
		}
		break;
	}

	// TODO - perhaps fields of as_exp_ctx should be const?
	as_exp_ctx ctx = { .ns = tr->rsv.ns, .r = (as_record*)r };
	as_exp_trilean tv = as_exp_matches_metadata(*exp, &ctx);

	if (tv == AS_EXP_UNK) {
		return AS_OK; // caller must later check bins using *exp
	}
	// else - caller will not need to apply filter later.

	destroy_filter_exp(tr, *exp);
	*exp = NULL;

	return tv == AS_EXP_TRUE ? AS_OK : AS_ERR_FILTERED_OUT;
}


void
destroy_filter_exp(const as_transaction* tr, as_exp* exp)
{
	if (SHARED_MSGP(tr)) {
		return;
	}

	as_exp_destroy(exp);
}


int
read_and_filter_bins(as_storage_rd* rd, as_exp* exp)
{
	as_namespace* ns = rd->ns;

	as_bin stack_bins[ns->single_bin ? 1 : RECORD_MAX_BINS];

	int result = as_storage_rd_load_bins(rd, stack_bins);

	if (result < 0) {
		return -result;
	}

	as_exp_ctx ctx = { .ns = ns, .r = rd->r, .rd = rd };

	if (! as_exp_matches_record(exp, &ctx)) {
		return AS_ERR_FILTERED_OUT;
	}

	return AS_OK;
}


// Caller must have checked that key is present in message.
bool
check_msg_key(as_msg* m, as_storage_rd* rd)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
	uint32_t key_size = as_msg_field_get_value_sz(f);
	uint8_t* key = f->data;

	if (key_size != rd->key_size || memcmp(key, rd->key, key_size) != 0) {
		cf_warning(AS_RW, "key mismatch - end of universe?");
		return false;
	}

	return true;
}


bool
get_msg_key(as_transaction* tr, as_storage_rd* rd)
{
	if (! as_transaction_has_key(tr)) {
		return true;
	}

	if (rd->ns->single_bin && rd->ns->storage_data_in_memory) {
		cf_warning(AS_RW, "{%s} can't store key if data-in-memory & single-bin",
				tr->rsv.ns->name);
		return false;
	}

	as_msg_field* f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_KEY);

	if ((rd->key_size = as_msg_field_get_value_sz(f)) == 0) {
		cf_warning(AS_RW, "msg flat key size is 0");
		return false;
	}

	rd->key = f->data;

	if (*rd->key == AS_PARTICLE_TYPE_INTEGER &&
			rd->key_size != 1 + sizeof(uint64_t)) {
		cf_warning(AS_RW, "bad msg integer key flat size %u", rd->key_size);
		return false;
	}

	return true;
}


int
handle_msg_key(as_transaction* tr, as_storage_rd* rd)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	if (rd->r->key_stored == 1) {
		// Key stored for this record - be sure it gets rewritten.

		// This will force a device read for non-data-in-memory, even if
		// must_fetch_data is false! Since there's no advantage to using the
		// loaded block after this if must_fetch_data is false, leave the
		// subsequent code as-is.
		if (! as_storage_rd_load_key(rd)) {
			cf_warning(AS_RW, "{%s} can't get stored key %pD", ns->name,
					&tr->keyd);
			return AS_ERR_UNKNOWN;
		}

		// Check the client-sent key, if any, against the stored key.
		if (as_transaction_has_key(tr) && ! check_msg_key(m, rd)) {
			cf_warning(AS_RW, "{%s} key mismatch %pD", ns->name, &tr->keyd);
			return AS_ERR_KEY_MISMATCH;
		}
	}
	else {
		// Key not stored for this record - store one if sent from client. For
		// data-in-memory, don't allocate the key until we reach the point of no
		// return. Also don't set AS_INDEX_FLAG_KEY_STORED flag until then.
		if (! get_msg_key(tr, rd)) {
			return AS_ERR_UNSUPPORTED_FEATURE;
		}
	}

	return 0;
}


void
advance_record_version(const as_transaction* tr, as_record* r)
{
	// Shortcut pointers.
	const as_msg* m = &tr->msgp->msg;
	const as_namespace* ns = tr->rsv.ns;

	uint64_t now = cf_clepoch_milliseconds();

	switch (m->record_ttl) {
	case TTL_NAMESPACE_DEFAULT:
		if (ns->default_ttl != 0) {
			// Set record void-time using default TTL value.
			r->void_time = (now / 1000) + ns->default_ttl;
		}
		else {
			// Default TTL is "never expire".
			r->void_time = 0;
		}
		break;
	case TTL_NEVER_EXPIRE:
		// Set record to "never expire".
		r->void_time = 0;
		break;
	case TTL_DONT_UPDATE:
		// Do not change record's void time.
		break;
	default:
		// Apply non-special m->record_ttl directly. Have already checked
		// m->record_ttl <= 10 years, so no overflow etc.
		r->void_time = (now / 1000) + m->record_ttl;
		break;
	}

	as_record_set_lut(r, tr->rsv.regime, now, ns);
	as_record_increment_generation(r, ns);
}


void
pickle_all(as_storage_rd* rd, rw_request* rw)
{
	if (rd->keep_pickle) {
		rw->pickle = rd->pickle;
		rw->pickle_sz = rd->pickle_sz;
	}
	// else - no destination node(s).
}


void
update_sindex(as_namespace* ns, as_index_ref* r_ref, as_bin* old_bins,
		uint32_t n_old_bins, as_bin* new_bins, uint32_t n_new_bins)
{
	uint32_t n_populated = 0;
	bool bin_name_in_both[n_new_bins];

	// Initialize before the critical section to make it shorter.
	memset(bin_name_in_both, 0, sizeof(bin_name_in_both));

	SINDEX_GRLOCK();

	// At max we will do both insert & delete for every sindex in the namespace.
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);

	as_index* r = r_ref->r;
	const char* set_name = as_index_get_set_name(r, ns);
	as_sindex* si_arr[2 * ns->sindex_cnt];
	uint32_t si_arr_index = 0;

	// Reserve matching SIs.

	for (uint32_t i = 0; i < n_old_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				old_bins[i].id, &si_arr[si_arr_index]);
	}

	for (uint32_t i = 0; i < n_new_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				new_bins[i].id, &si_arr[si_arr_index]);
	}

	if (si_arr_index == 0) {
		SINDEX_GRUNLOCK();
		as_index_clear_in_sindex(r); // no sindex corresponding to old/new bins
		return;
	}

	bool record_in_sindex = false;

	// For every old bin, find the corresponding new bin (if any) and adjust the
	// secondary index if the bin was modified. If no corresponding new bin is
	// found, it means the old bin was deleted - also adjust the secondary index
	// accordingly.
	for (uint32_t i_old = 0; i_old < n_old_bins; i_old++) {
		as_bin* b_old = &old_bins[i_old];
		as_bin* b_new = NULL;
		bool found = false;

		// Check same slot first. Optimize for bin list remaining same.
		if (i_old < n_new_bins) {
			uint32_t i_new = i_old;

			b_new = &new_bins[i_new];

			if (b_old->id == b_new->id) {
				found = true;
				bin_name_in_both[i_new] = true;
			}
		}

		if (! found) {
			for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
				b_new = &new_bins[i_new];

				if (b_old->id == b_new->id) {
					found = true;
					bin_name_in_both[i_new] = true;

					break;
				}
			}
		}

		if (found) {
			if (as_bin_get_particle_type(b_old) !=
					as_bin_get_particle_type(b_new) ||
					b_old->particle != b_new->particle) {
				n_populated += as_sindex_sbins_from_bin(ns, set_name, b_old,
						&sbins[n_populated], AS_SINDEX_OP_DELETE);

				uint32_t n = as_sindex_sbins_from_bin(ns, set_name, b_new,
						&sbins[n_populated], AS_SINDEX_OP_INSERT);

				if (n != 0) {
					record_in_sindex = true;
				}

				n_populated += n;
			}
			else if (r->in_sindex == 1 && ! record_in_sindex) {
				// We only need to see whether this bin is in any sindex...

				SINDEX_BINS_SETUP(dummy_sbins, ns->sindex_cnt);

				uint32_t n = as_sindex_sbins_from_bin(ns, set_name, b_new,
						dummy_sbins, AS_SINDEX_OP_INSERT);

				if (n != 0) {
					record_in_sindex = true;
				}

				as_sindex_sbin_freeall(dummy_sbins, n);
			}
		}
		else {
			n_populated += as_sindex_sbins_from_bin(ns, set_name, b_old,
					&sbins[n_populated], AS_SINDEX_OP_DELETE);
		}
	}

	// Now find the new bins that are just-created bins. We've marked the others
	// in the loop above, so any left are just-created.

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		if (bin_name_in_both[i_new]) {
			continue;
		}

		uint32_t n = as_sindex_sbins_from_bin(ns, set_name, &new_bins[i_new],
				&sbins[n_populated], AS_SINDEX_OP_INSERT);

		if (n != 0) {
			record_in_sindex = true;
		}

		n_populated += n;
	}

	SINDEX_GRUNLOCK();

	if (record_in_sindex) {
		// Mark record for sindex before insertion.
		as_index_set_in_sindex(r);
	}

	if (n_populated != 0) {
		CF_ALLOC_SET_NS_ARENA(ns);

		as_sindex_update_by_sbin(sbins, n_populated, r_ref->r_h);
		as_sindex_sbin_freeall(sbins, n_populated);
	}

	if (! record_in_sindex) {
		// Unmark record for sindex after deletion. in_sindex may not be set
		// if the sindex building is in progress.
		as_index_clear_in_sindex(r);
	}

	as_sindex_release_arr(si_arr, si_arr_index);
}


void
remove_from_sindex(as_namespace* ns, as_index_ref* r_ref)
{
	as_record* r = r_ref->r;

	if (r->in_sindex == 0) {
		return;
	}

	if (! set_has_sindex(r, ns)) {
		// Sindex drop will leave in_sindex bit. Good opportunity to clear.
		as_index_clear_in_sindex(r);
		return;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS];

	as_storage_rd_load_bins(&rd, stack_bins);
	remove_from_sindex_bins(ns, r_ref, rd.bins, rd.n_bins);

	as_storage_record_close(&rd);
}


void
remove_from_sindex_bins(as_namespace* ns, as_index_ref* r_ref, as_bin* bins,
		uint32_t n_bins)
{
	SINDEX_GRLOCK();

	SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);

	as_sindex* si_arr[ns->sindex_cnt];
	uint32_t si_arr_index = 0;
	uint32_t n_populated = 0;
	as_index* r = r_ref->r;
	const char* set_name = as_index_get_set_name(r, ns);

	// Reserve matching sindexes.
	for (uint32_t i = 0; i < n_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				bins[i].id, &si_arr[si_arr_index]);
	}

	for (uint32_t i = 0; i < n_bins; i++) {
		n_populated += as_sindex_sbins_from_bin(ns, set_name, &bins[i],
				&sbins[n_populated], AS_SINDEX_OP_DELETE);
	}

	SINDEX_GRUNLOCK();

	if (n_populated != 0) {
		CF_ALLOC_SET_NS_ARENA(ns);

		as_sindex_update_by_sbin(sbins, n_populated, r_ref->r_h);
		as_sindex_sbin_freeall(sbins, n_populated);
	}

	// Unmark record for sindex after deletion.
	as_index_clear_in_sindex(r);

	as_sindex_release_arr(si_arr, si_arr_index);
}


void
write_dim_single_bin_unwind(as_bin* old_bin, uint32_t n_old_bins,
		as_bin* new_bin, uint32_t n_new_bins, as_bin* cleanup_bins,
		uint32_t n_cleanup_bins)
{
	if (n_new_bins == 1 && as_bin_is_live(new_bin) &&
			! as_bin_is_embedded_particle(new_bin)) {
		if (n_old_bins == 1) {
			if (new_bin->particle != as_bin_get_particle(old_bin)) {
				as_bin_particle_destroy(new_bin);
			}
		}
		else {
			as_bin_particle_destroy(new_bin);
		}
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin* b_cleanup = &cleanup_bins[i_cleanup];
		as_particle* p_cleanup = b_cleanup->particle;

		if (n_old_bins == 1) {
			if (p_cleanup != as_bin_get_particle(old_bin)) {
				as_bin_particle_destroy(b_cleanup);
			}
		}
		else {
			as_bin_particle_destroy(b_cleanup);
		}
	}

	// The index element's embedded bin is still old_bins.
}


void
write_dim_unwind(as_bin* old_bins, uint32_t n_old_bins, as_bin* new_bins,
		uint32_t n_new_bins, as_bin* cleanup_bins, uint32_t n_cleanup_bins)
{
	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		as_bin* b_new = &new_bins[i_new];

		if (! as_bin_is_live(b_new)) {
			continue;
		}

		// Embedded particles have no-op destructors - skip loop over old bins.
		if (as_bin_is_embedded_particle(b_new)) {
			continue;
		}

		as_particle* p_new = b_new->particle;
		uint32_t i_old;

		for (i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin* b_old = &old_bins[i_old];

			if (b_new->id == b_old->id) {
				if (p_new != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_new);
				}

				break;
			}
		}

		if (i_old == n_old_bins) {
			as_bin_particle_destroy(b_new);
		}
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin* b_cleanup = &cleanup_bins[i_cleanup];
		as_particle* p_cleanup = b_cleanup->particle;
		uint32_t i_old;

		for (i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin* b_old = &old_bins[i_old];

			if (b_cleanup->id == b_old->id) {
				if (p_cleanup != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_cleanup);
				}

				break;
			}
		}

		if (i_old == n_old_bins) {
			as_bin_particle_destroy(b_cleanup);
		}
	}

	// The index element's as_bin_space pointer still points at old bins.
}
