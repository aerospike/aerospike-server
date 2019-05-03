/*
 * rw_utils.c
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

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h" // xdr_allows_write
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "msg.h"

#include "base/cfg.h" // xdr_allows_write
#include "base/datamodel.h"
#include "base/proto.h" // xdr_allows_write
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/xdr_serverside.h"
#include "fabric/fabric.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

// TODO - really? we can't hide this behind an XDR stub?
bool
xdr_allows_write(as_transaction* tr)
{
	if (as_transaction_is_xdr(tr)) {
		if (tr->rsv.ns->ns_allow_xdr_writes) {
			return true;
		}
	}
	else {
		if (tr->rsv.ns->ns_allow_nonxdr_writes) {
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
		if (! as_storage_record_get_key(rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} can't get stored key ",
					ns->name);
			return AS_ERR_UNKNOWN;
		}

		// Check the client-sent key, if any, against the stored key.
		if (as_transaction_has_key(tr) && ! check_msg_key(m, rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} key mismatch ", ns->name);
			return AS_ERR_KEY_MISMATCH;
		}
	}
	// If we got a key without a digest, it's an old client, not a cue to store
	// the key. (Remove this check when we're sure all old C clients are gone.)
	else if (as_transaction_has_digest(tr)) {
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
update_metadata_in_index(as_transaction* tr, as_record* r)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

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
		return;
	}
	// else - new protocol with no destination node(s), or old protocol.

	if (rw->n_dest_nodes == 0) {
		return;
	}
	// else - old protocol with destination node(s).

	// TODO - old pickle - remove in "six months".

	rw->is_old_pickle = true;
	rw->pickle = as_record_pickle(rd, &rw->pickle_sz);

	rw->set_name = rd->set_name;
	rw->set_name_len = rd->set_name_len;

	if (rd->key) {
		rw->key = cf_malloc(rd->key_size);
		rw->key_size = rd->key_size;
		memcpy(rw->key, rd->key, rd->key_size);
	}
}


bool
write_sindex_update(as_namespace* ns, const char* set_name, cf_digest* keyd,
		as_bin* old_bins, uint32_t n_old_bins, as_bin* new_bins,
		uint32_t n_new_bins)
{
	int n_populated = 0;
	bool not_just_created[n_new_bins];

	memset(not_just_created, 0, sizeof(not_just_created));

	// Maximum number of sindexes which can be changed in one transaction is
	// 2 * ns->sindex_cnt.

	SINDEX_GRLOCK();
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);
	as_sindex* si_arr[2 * ns->sindex_cnt];
	int si_arr_index = 0;

	// Reserve matching SIs.

	for (int i = 0; i < n_old_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				old_bins[i].id, &si_arr[si_arr_index]);
	}

	for (int i = 0; i < n_new_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				new_bins[i].id, &si_arr[si_arr_index]);
	}

	// For every old bin, find the corresponding new bin (if any) and adjust the
	// secondary index if the bin was modified. If no corresponding new bin is
	// found, it means the old bin was deleted - also adjust the secondary index
	// accordingly.

	for (int32_t i_old = 0; i_old < (int32_t)n_old_bins; i_old++) {
		as_bin* b_old = &old_bins[i_old];
		bool found = false;

		// Loop over new bins. Start at old bin index (if possible) and go down,
		// wrapping around to do the higher indexes last. This will find a match
		// (if any) very quickly - instantly, unless there were bins deleted.

		bool any_new = n_new_bins != 0;
		int32_t n_new_minus_1 = (int32_t)n_new_bins - 1;
		int32_t i_new = n_new_minus_1 < i_old ? n_new_minus_1 : i_old;

		while (any_new) {
			as_bin* b_new = &new_bins[i_new];

			if (b_old->id == b_new->id) {
				if (as_bin_get_particle_type(b_old) !=
						as_bin_get_particle_type(b_new) ||
								b_old->particle != b_new->particle) {
					n_populated += as_sindex_sbins_populate(
							&sbins[n_populated], ns, set_name, b_old, b_new);
				}

				found = true;
				not_just_created[i_new] = true;
				break;
			}

			if (--i_new < 0 && (i_new = n_new_minus_1) <= i_old) {
				break;
			}

			if (i_new == i_old) {
				break;
			}
		}

		if (! found) {
			n_populated += as_sindex_sbins_from_bin(ns, set_name, b_old,
					&sbins[n_populated], AS_SINDEX_OP_DELETE);
		}
	}

	// Now find the new bins that are just-created bins. We've marked the others
	// in the loop above, so any left are just-created.

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		if (not_just_created[i_new]) {
			continue;
		}

		n_populated += as_sindex_sbins_from_bin(ns, set_name, &new_bins[i_new],
				&sbins[n_populated], AS_SINDEX_OP_INSERT);
	}

	SINDEX_GRUNLOCK();

	if (n_populated != 0) {
		as_sindex_update_by_sbin(ns, set_name, sbins, n_populated, keyd);
		as_sindex_sbin_freeall(sbins, n_populated);
	}

	as_sindex_release_arr(si_arr, si_arr_index);

	return n_populated != 0;
}


// If called for data-not-in-memory, this may read record from drive!
// TODO - rename as as_record_... and move to record.c?
void
record_delete_adjust_sindex(as_record* r, as_namespace* ns)
{
	if (! record_has_sindex(r, ns)) {
		return;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);
	as_storage_rd_load_n_bins(&rd);

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd.n_bins];

	as_storage_rd_load_bins(&rd, stack_bins);

	remove_from_sindex(ns, as_index_get_set_name(r, ns), &r->keyd, rd.bins,
			rd.n_bins);

	as_storage_record_close(&rd);
}


// Remove record from secondary index. Called only for data-in-memory. If
// data-not-in-memory, existing record is not read, and secondary index entry is
// cleaned up by background sindex defrag thread.
// TODO - rename as as_record_... and move to record.c?
void
delete_adjust_sindex(as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;

	if (! record_has_sindex(rd->r, ns)) {
		return;
	}

	as_storage_rd_load_n_bins(rd);
	as_storage_rd_load_bins(rd, NULL);

	remove_from_sindex(ns, as_index_get_set_name(rd->r, ns), &rd->r->keyd,
			rd->bins, rd->n_bins);
}


// TODO - rename as as_record_..., move to record.c, take r instead of set_name,
// and lose keyd parameter?
void
remove_from_sindex(as_namespace* ns, const char* set_name, cf_digest* keyd,
		as_bin* bins, uint32_t n_bins)
{
	SINDEX_GRLOCK();

	SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);

	as_sindex* si_arr[ns->sindex_cnt];
	int si_arr_index = 0;
	int sbins_populated = 0;

	// Reserve matching sindexes.
	for (int i = 0; i < (int)n_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				bins[i].id, &si_arr[si_arr_index]);
	}

	for (int i = 0; i < (int)n_bins; i++) {
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, &bins[i],
				&sbins[sbins_populated], AS_SINDEX_OP_DELETE);
	}

	SINDEX_GRUNLOCK();

	if (sbins_populated) {
		as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);
	}

	as_sindex_release_arr(si_arr, si_arr_index);
}


bool
xdr_must_ship_delete(as_namespace* ns, bool is_xdr_op)
{
	if (! is_xdr_delete_shipping_enabled()) {
		return false;
	}

	return ! is_xdr_op ||
			// If this delete is a result of XDR shipping, don't ship it unless
			// configured to do so.
			is_xdr_forwarding_enabled() || ns->ns_forward_xdr_writes;
}
