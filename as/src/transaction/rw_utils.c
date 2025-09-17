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

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "msg.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "sindex/sindex.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_request.h"
#include "transaction/udf.h"
#include "transaction/write.h"


//==========================================================
// Typedefs & constants.
//

typedef struct bins_old_new_s {
	as_namespace* ns;
	as_record* r;

	as_bin* old_bins;
	uint16_t n_old_bins;
	as_bin* new_bins;
	uint16_t n_new_bins;
} bins_old_new;


//==========================================================
// Forward declarations.
//

static uint32_t eval_and_populate_sbin(as_exp_ctx* ctx, as_sindex* si, as_sindex_bin* sbins, as_sindex_op op);
static uint32_t update_sindex_exp(bins_old_new* old_new, as_bin** match_bins, uint32_t n_match_bins,  as_sindex_bin* sbins, bool* record_in_sindex_r);


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

	as_incr_uint64(&tr->rsv.ns->n_fail_xdr_forbidden);

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

	if (is_mrt_setless_tombstone(ns, r)) {
		return true;
	}

	const char* set_name = as_index_get_set_name(r, ns);

	if (set_name == NULL ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		cf_warning(AS_RW, "{%s} set name mismatch %s %.*s (%u) %pD", ns->name,
				set_name == NULL ? "(null)" : set_name, msg_set_name_len,
						f->data, msg_set_name_len, &tr->keyd);
		return false;
	}

	return true;
}

int
set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
	uint32_t name_len = as_msg_field_get_value_sz(f);

	if (name_len == 0) {
		return AS_OK;
	}

	if (! as_mrt_monitor_check_set_name(ns, f->data, name_len)) {
		return AS_ERR_UNSUPPORTED_FEATURE;
	}

	// Given the name, find/assign the set-ID and write it in the as_index.
	return as_index_set_set_w_len(r, ns, (const char*)f->data, name_len, true);
}

int
set_name_check_on_update(const as_transaction* tr, as_record* r)
{
	as_namespace* ns = tr->rsv.ns;
	const char* set_name = as_index_get_set_name(r, ns);

	as_msg_field* f = as_transaction_has_set(tr) ?
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET) : NULL;

	uint32_t msg_set_name_len = f != NULL ? as_msg_field_get_value_sz(f) : 0;

	if (msg_set_name_len == 0) {
		if (set_name == NULL) {
			return AS_OK; // record not in a set
		}

		cf_warning(AS_RW, "{%s} set name mismatch %s (null) (0) %pD", ns->name,
				set_name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (is_mrt_setless_tombstone(ns, r)) {
		return as_record_fix_setless_tombstone(r, ns, (const char*)f->data,
				msg_set_name_len, true);
	}

	if (set_name == NULL ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		cf_warning(AS_RW, "{%s} set name mismatch %s %.*s (%u) %pD", ns->name,
				set_name ? set_name : "(null)", msg_set_name_len,
						(const char*)f->data, msg_set_name_len, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	return AS_OK;
}

int
handle_meta_filter(const as_transaction* tr, const as_record* r, as_exp** exp)
{
	switch (tr->origin) {
	case FROM_BATCH:
		if (as_transaction_has_predexp(tr)) {
			as_msg_field* f = as_msg_field_get(&tr->msgp->msg,
					AS_MSG_FIELD_TYPE_PREDEXP);
			if ((*exp = as_exp_filter_build(f, false)) == NULL) {
				return AS_ERR_PARAMETER;
			}
		}
		else if ((*exp = as_batch_get_predexp(tr->from.batch_shared)) == NULL) {
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
	switch (tr->origin) {
	case FROM_BATCH:
		if (as_transaction_has_predexp(tr)) {
			as_exp_destroy(exp);
		}
		break;
	case FROM_IUDF:
	case FROM_IOPS:
		break;
	default:
		as_exp_destroy(exp);
		break;
	}
}

int
read_and_filter_bins(as_storage_rd* rd, as_exp* exp)
{
	as_namespace* ns = rd->ns;

	as_bin stack_bins[RECORD_MAX_BINS];

	int result = as_storage_rd_lazy_load_bins(rd, stack_bins);

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
advance_record_version(as_transaction* tr, as_record* r)
{
	const as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	uint64_t now = as_transaction_epoch_ms(tr);

	as_record_advance_void_time(r, m->record_ttl, now, ns);
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
	as_index* r = r_ref->r;
	uint16_t set_id = as_index_get_set_id(r);

	bool bin_name_in_both[n_new_bins];
	as_bin* changed_bins[n_old_bins + n_new_bins]; // only the 'name' is used
	uint32_t n_changed_bins = 0;

	// Initialize before the critical section to make it shorter.
	memset(bin_name_in_both, 0, sizeof(bin_name_in_both));
	memset(changed_bins, 0, sizeof(changed_bins));

	SINDEX_GRLOCK();

	// At max we will do both insert & delete for every sindex in the namespace.
	uint32_t n_sindexes = as_sindex_n_sindexes(ns);
	as_sindex_bin sbins[2 * n_sindexes];
	uint32_t n_populated = 0;
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

			if (strcmp(b_old->name, b_new->name) == 0) {
				found = true;
				bin_name_in_both[i_new] = true;
			}
		}

		if (! found) {
			for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
				b_new = &new_bins[i_new];

				if (strcmp(b_old->name, b_new->name) == 0) {
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
				n_populated += as_sindex_populate_sbins(ns, set_id, b_old,
						&sbins[n_populated], AS_SINDEX_OP_DELETE);

				uint32_t n = as_sindex_populate_sbins(ns, set_id, b_new,
						&sbins[n_populated], AS_SINDEX_OP_INSERT);

				if (n != 0) {
					record_in_sindex = true;
				}

				changed_bins[n_changed_bins++] = b_new;
				n_populated += n;
			}
			else if (r->in_sindex == 1 && ! record_in_sindex) {
				// We only need to see whether this bin is in any sindex...

				as_sindex_bin dummy_sbins[n_sindexes];

				uint32_t n = as_sindex_populate_sbins(ns, set_id, b_new,
						dummy_sbins, AS_SINDEX_OP_INSERT);

				if (n != 0) {
					record_in_sindex = true;
				}

				as_sindex_sbin_free_all(dummy_sbins, n);
			}
		}
		else {
			changed_bins[n_changed_bins++] = b_old;
			n_populated += as_sindex_populate_sbins(ns, set_id, b_old,
					&sbins[n_populated], AS_SINDEX_OP_DELETE);
		}
	}

	// Now find the new bins that are just-created bins. We've marked the others
	// in the loop above, so any left are just-created.
	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		if (bin_name_in_both[i_new]) {
			continue;
		}

		as_bin* b_new = &new_bins[i_new];
		uint32_t n = as_sindex_populate_sbins(ns, set_id, b_new,
				&sbins[n_populated], AS_SINDEX_OP_INSERT);

		if (n != 0) {
			record_in_sindex = true;
		}

		changed_bins[n_changed_bins++] = b_new;
		n_populated += n;
	}

	bins_old_new old_new = {
			.ns = ns,
			.r = r,
			.old_bins = old_bins,
			.n_old_bins = n_old_bins,
			.new_bins = new_bins,
			.n_new_bins = n_new_bins
	};

	n_populated += update_sindex_exp(&old_new, changed_bins, n_changed_bins,
			&sbins[n_populated], &record_in_sindex);

	if (! record_in_sindex) {
		// The record may be in some sindex with exp based on unchanged bins.

		old_new.old_bins = NULL;
		old_new.n_old_bins = 0;

		as_sindex_bin dummy_sbins[n_sindexes];
		as_bin* p_new_bins[n_new_bins];

		for (uint32_t b_ix = 0; b_ix < n_new_bins; b_ix++) {
			p_new_bins[b_ix] = &new_bins[b_ix];
		}

		uint32_t n = update_sindex_exp(&old_new, p_new_bins, n_new_bins,
				dummy_sbins, &record_in_sindex);

		as_sindex_sbin_free_all(dummy_sbins, n);
	}

	SINDEX_GRUNLOCK();

	if (record_in_sindex) {
		// Mark record for sindex before insertion.
		as_index_set_in_sindex(r);
	}

	if (n_populated != 0) {
		as_sindex_update_by_sbin(sbins, n_populated, r_ref->r_h);
		as_sindex_sbin_free_all(sbins, n_populated);
	}

	if (! record_in_sindex) {
		// Unmark record for sindex after deletion. in_sindex may not be set
		// if the sindex building is in progress.
		as_index_clear_in_sindex(r);
	}
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

	if (as_storage_rd_load_bins(&rd, stack_bins) == 0) {
		remove_from_sindex_bins(ns, r_ref, rd.bins, rd.n_bins);
	}
	else {
		cf_warning(AS_RW, "failed removing record from sindex - sindex leak");
	}

	as_storage_record_close(&rd);
}

void
remove_from_sindex_bins(as_namespace* ns, as_index_ref* r_ref, as_bin* bins,
		uint32_t n_bins)
{
	as_index* r = r_ref->r;
	uint16_t set_id = as_index_get_set_id(r);

	as_bin* changed_bins[n_bins]; // only the name field is used
	uint32_t n_changed_bins = 0;

	SINDEX_GRLOCK();

	as_sindex_bin sbins[as_sindex_n_sindexes(ns)];
	uint32_t n_populated = 0;

	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin* old_bin = &bins[i];

		n_populated += as_sindex_populate_sbins(ns, set_id, old_bin,
				&sbins[n_populated], AS_SINDEX_OP_DELETE);

		changed_bins[n_changed_bins++] = old_bin; // consider all bins changed
	}

	bins_old_new old_new = {
			.ns = ns,
			.r = r,
			.old_bins = bins,
			.n_old_bins = n_bins
	};

	n_populated += update_sindex_exp(&old_new, changed_bins, n_changed_bins,
			&sbins[n_populated], NULL);

	SINDEX_GRUNLOCK();

	if (n_populated != 0) {
		as_sindex_update_by_sbin(sbins, n_populated, r_ref->r_h);
		as_sindex_sbin_free_all(sbins, n_populated);
	}

	// Unmark record for sindex after deletion.
	as_index_clear_in_sindex(r);
}


//==========================================================
// Local helpers.
//

static uint32_t
eval_and_populate_sbin(as_exp_ctx* ctx, as_sindex* si, as_sindex_bin* sbins,
		as_sindex_op op)
{
	if (ctx == NULL) {
		return 0;
	}

	as_bin rb;
	as_bin_set_empty(&rb);

	if (! as_exp_eval(si->exp, ctx, &rb, NULL)) {
		return 0;
	}

	uint32_t n_populated = 0;

	n_populated += as_sindex_populate_sbin_si(si, &rb, &sbins[n_populated], op);

	as_bin_particle_destroy(&rb);

	return n_populated;
}

static uint32_t
update_sindex_exp(bins_old_new* old_new, as_bin** match_bins,
		uint32_t n_match_bins, as_sindex_bin* sbins, bool* record_in_sindex_r)
{
	as_namespace* ns = old_new->ns;
	as_record* r = old_new->r;

	// Fake eval_rd for exp evaluation.
	as_storage_rd eval_old_rd = {
			.bins = old_new->old_bins,
			.n_bins = old_new->n_old_bins
	};
	as_exp_ctx ctx_old_rd = {
			.ns = ns,
			.r = r,
			.rd = &eval_old_rd
	};
	as_storage_rd eval_new_rd = {
			.bins = old_new->new_bins,
			.n_bins = old_new->n_new_bins
	};
	as_exp_ctx ctx_new_rd = {
			.ns = ns,
			.r = r,
			.rd = &eval_new_rd
	};
	uint32_t n_populated = 0;

	for (uint32_t si_ix = 0; si_ix < MAX_N_SINDEXES; si_ix++) {
		as_sindex* si = ns->sindexes[si_ix];

		if (si == NULL || si->exp == NULL) {
			continue;
		}

		uint16_t set_id = si->set_id;

		if (set_id != INVALID_SET_ID && set_id != as_index_get_set_id(r)) {
			continue;
		}

		bool matched = false;
		cf_vector* exp_bnames = si->exp_bin_names;
		uint32_t exp_bcount = cf_vector_size(exp_bnames);
		bool has_digest_mod = (si->exp->flags & AS_EXP_HAS_DIGEST_MOD) != 0;

		if (has_digest_mod) {
			matched = true; // need to update sindex, also skip bin name check
		}

		for (uint32_t b_ix = 0; b_ix < exp_bcount && ! matched; b_ix++) {
			char exp_bname[AS_BIN_NAME_MAX_SZ];

			cf_vector_get(exp_bnames, b_ix, exp_bname);

			for (uint32_t c_ix = 0; c_ix < n_match_bins; c_ix++) {
				if (strcmp(match_bins[c_ix]->name, exp_bname) == 0) {
					matched = true; // done with this si
					break;
				}
			}
		}

		if (! matched) {
			continue;
		}

		// Optimise if the sindex exp has a digest mod and no bin names.
		if (has_digest_mod && cf_vector_size(si->exp_bin_names) == 0 &&
				old_new->n_old_bins != 0 && old_new->n_new_bins != 0) {
			// Optimisation - as digest will never change, exp result will
			// be the same and we can skip sindex update. But, we need to know
			// if the record is in the sindex.

			as_sindex_bin dummy_sbin;

			as_exp_ctx dummy_ctx = {
				.ns = ns,
				.r = old_new->r,
				.rd = NULL
			};

			uint32_t n = eval_and_populate_sbin(&dummy_ctx, si,
					&dummy_sbin, AS_SINDEX_OP_INSERT);

			if (n != 0) {
				*record_in_sindex_r = true;
			}

			as_sindex_sbin_free_all(&dummy_sbin, n);
		}
		else {
			if (old_new->n_old_bins != 0) {
				n_populated += eval_and_populate_sbin(&ctx_old_rd, si,
						&sbins[n_populated], AS_SINDEX_OP_DELETE);
			}

			if (old_new->n_new_bins != 0) {
				uint32_t n = eval_and_populate_sbin(&ctx_new_rd, si,
						&sbins[n_populated], AS_SINDEX_OP_INSERT);

				if (n != 0) {
					// Must be update (not delete) - the flag will be non-NULL.
					*record_in_sindex_r = true;
				}

				n_populated += n;
			}
		}
	}

	return n_populated;
}
