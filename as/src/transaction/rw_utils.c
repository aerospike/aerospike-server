/*
 * rw_utils.c
 *
 * Copyright (C) 2016-2026 Aerospike, Inc.
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
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "enhanced_alloc.h"
#include "log.h"
#include "msg.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/masking.h"
#include "base/mrt_monitor.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/transaction.h"
#include "base/zstd_wire.h"
#include "exp/exp.h"
#include "fabric/fabric.h"
#include "sindex/sindex.h"
#include "storage/flat.h"
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

static uint32_t eval_and_populate_sbin(as_exp_ctx* ctx, as_sindex* si,
		as_sindex_bin* sbins, as_sindex_op op);
static uint32_t update_sindex_exp(bins_old_new* old_new, as_bin** match_bins,
		uint32_t n_match_bins, as_sindex_bin* sbins, bool* record_in_sindex_r);

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

// The fire-and-forget send, taken only when respond_on_master_complete() is
// true (write-commit-level master) - the replica suppresses its ack and the
// caller drops the rw_request from the hash immediately after this returns.
//
// So this is also where wire-compression savings are credited for these
// writes. The acked path credits in repl_write_handle_ack(), which is
// unreachable here in both directions - no ack is sent, and the rw_request is
// gone before one could be matched - so without this the counter would stay at
// zero for every commit-level-master namespace even though compressed payloads
// went out on the fabric. Crediting at send time is safe here precisely because
// the ack path cannot run: there is no second crediting site to double-count
// with, and no ack-driven fallback that could retract the saving, so the value
// is final once fabric has taken the message.
void
send_rw_messages_forget(rw_request* rw, as_namespace* ns)
{
	uint32_t n_sent = 0;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		msg_incr_ref(rw->dest_msg);

		if (as_fabric_send(rw->dest_nodes[i], rw->dest_msg,
					AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(rw->dest_msg);
			continue;
		}

		n_sent++;
	}

	// Same rule as repl_write_ack_credits_bytes_saved(): gated on what
	// fill_repl_write_message() actually put on the wire, not on what was
	// computed, so a compat-gated plain send credits nothing. Only the
	// destinations fabric accepted count - matching the acked path, which
	// accumulates once per acking destination.
	if (n_sent != 0 && rw->wire_compressed_op_sent &&
			rw->per_dest_bytes_saved != 0) {
		as_add_uint64(&ns->repl_wire_comp_stat.bytes_saved,
				rw->per_dest_bytes_saved * n_sent);
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
		as_error_details_set_fmt(AS_SUB_NONE,
				"message set name %.*s does not match record set %s",
				msg_set_name_len, (const char*)f->data,
				set_name != NULL ? set_name : "(null)");
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
		as_error_details_set_fmt(AS_SUB_UNSUPP_FEAT_GENERIC,
				"MRT monitor set name not supported in availability (AP) mode");
		return AS_ERR_UNSUPPORTED_FEATURE;
	}

	return as_index_set_set_w_len(r, ns, (const char*)f->data, name_len, true);
}

int
set_name_check_on_update(const as_transaction* tr, as_record* r)
{
	as_namespace* ns = tr->rsv.ns;
	const char* set_name = as_index_get_set_name(r, ns);

	as_msg_field* f = as_transaction_has_set(tr)
			? as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET)
			: NULL;

	uint32_t msg_set_name_len = f != NULL ? as_msg_field_get_value_sz(f) : 0;

	if (msg_set_name_len == 0) {
		if (set_name == NULL) {
			return AS_OK; // record not in a set
		}

		cf_warning(AS_RW, "{%s} set name mismatch %s (null) (0) %pD", ns->name,
				set_name, &tr->keyd);
		as_error_details_set_fmt(AS_SUB_NONE,
				"message has no set name but record is in set %s", set_name);
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
		as_error_details_set_fmt(AS_SUB_NONE,
				"message set name %.*s does not match record set %s",
				msg_set_name_len, (const char*)f->data,
				set_name ? set_name : "(null)");
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
			as_msg_field* f =
					as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_PREDEXP);
			if ((*exp = as_exp_filter_build(f, false)) == NULL) {
				as_exp_stage_build_error_details(
						"invalid filter expression in batch request");
				return AS_ERR_PARAMETER;
			}

			// Non-taking build - drop the accumulator's payload ref.
			as_exp_build_err_reset();
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
		as_msg_field* f =
				as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_PREDEXP);
		if ((*exp = as_exp_filter_build(f, false)) == NULL) {
			as_exp_stage_build_error_details("invalid filter expression in request");
			return AS_ERR_PARAMETER;
		}

		// Non-taking build - drop the accumulator's payload ref.
		as_exp_build_err_reset();
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
read_and_filter_bins(as_storage_rd* rd, as_exp* exp, bool explain,
		const as_transaction* tr)
{
	as_namespace* ns = rd->ns;

	as_bin stack_bins[RECORD_MAX_BINS];

	int result = as_storage_rd_lazy_load_bins(rd, stack_bins);

	if (result < 0) {
		as_error_details_set_fmt(AS_SUB_NONE, "failed to load bins from storage");
		return -result;
	}

	as_exp_ctx ctx = { .ns = ns, .r = rd->r, .rd = rd };

	if (! as_exp_matches_record(exp, &ctx)) {
		// Explain which sub-expression decided the non-match. Must run BEFORE
		// the generic fallback below - as_error_details_set_fmt assembles
		// eagerly and is first-set-wins, so the explainer's richer detail must
		// be authored first. The permission check is lazy, paid only when an
		// explanation can actually be produced - filtered out, at the trace
		// tier, with no trace already staged.
		//
		// PERM_READ gates the whole explanation, not just the operand values.
		// outcome (FALSE vs ABSENT) and the decisive op index are both
		// functions of the stored record - an and-chain reports which
		// conjunct failed, and ABSENT vs FALSE distinguishes "bin missing"
		// from "bin present but unequal". Disclosing those to a principal
		// authorized for write but not read would turn a filter into a
		// record oracle, so a non-reader falls through to the generic
		// message below - byte-identical to the pre-explainer reply.
		//
		// Scope: the gate covers the EXPLAINER only. An eval fault raised by
		// the authoritative as_exp_matches_record() above has already staged
		// its own trace, governed by the verbosity tier alone.
		if (explain && g_error_verbosity >= AS_ERROR_VERBOSITY_TRACE &&
				! g_error_exp_trace.set && as_exp_explain_allowed(tr, rd)) {
			as_exp_explain_filter(exp, &ctx);
		}

		as_error_details_set_fmt(AS_SUB_NONE, "filtered out by bins expression");

		return AS_ERR_FILTERED_OUT;
	}

	return AS_OK;
}

// May the principal behind 'tr' be shown an expression explanation over 'rd'?
// Everything an explainer discloses is a function of the stored record - the
// operand values render bin/key contents outright, and outcome (FALSE vs
// ABSENT) plus the decisive op index leak bin existence and which conjunct of
// an and-chain failed. A write, delete or expression-modify op is authorized
// for write and may lack read, so the whole explanation is gated on PERM_READ
// rather than just the operand tier.
//
// Fails closed on a NULL tr: no transaction means no principal to authorize
// against. Internal evals (sindex maintenance, query record bodies, XDR)
// leave as_exp_ctx.tr NULL and so never explain.
//
// Origin: iudf / iops bodies are excluded by contract - they are not client
// requests and their internal as_msg carries info4 = 0 anyway.
//
// Side-effect-free: does not set tr->result_code or log a violation. The CE
// stub returns AS_OK (no ACLs). Per-bin data masking still applies on top, at
// rt_load_bin.
//
// Conjunct ORDER is load-bearing, not stylistic: as_security_check_permission
// cf_crashes on an origin its switch does not list, and FROM_READ_TOUCH /
// FROM_RE_REPL / FROM_MONITOR_ROLL are not listed. They reach the write path,
// and reach here only if they ever carry a filter, which they do not today - so
// the origin test short-circuiting first is what keeps a widened predicate or a
// reordered && from becoming a node crash rather than a missing explanation.
bool
as_exp_explain_allowed(const as_transaction* tr, const as_storage_rd* rd)
{
	return tr != NULL && as_transaction_may_explain_filter(tr) &&
			as_security_check_permission(tr, NULL, rd->ns->ix,
					as_index_get_set_id(rd->r), PERM_READ) == AS_OK;
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
			as_error_details_set_fmt(AS_SUB_NONE,
					"could not read stored key from storage");
			return AS_ERR_UNKNOWN;
		}

		// Check the client-sent key, if any, against the stored key.
		if (as_transaction_has_key(tr) && ! check_msg_key(m, rd)) {
			cf_warning(AS_RW, "{%s} key mismatch %pD", ns->name, &tr->keyd);
			as_error_details_set_fmt(AS_SUB_NONE,
					"user key in request does not match stored key");
			return AS_ERR_KEY_MISMATCH;
		}
	}
	else {
		// Key not stored for this record - store one if sent from client. For
		// data-in-memory, don't allocate the key until we reach the point of no
		// return. Also don't set AS_INDEX_FLAG_KEY_STORED flag until then.
		if (! get_msg_key(tr, rd)) {
			as_error_details_set_fmt(AS_SUB_NONE,
					"invalid or malformed key in request");
			// TODO: Why is this unsupported feature?
			// Should this be AS_ERR_PARAMETER?
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

read_op_result
process_bin_read_op(const as_transaction* tr, as_storage_rd* rd, as_msg_op* op,
		bool respond_all_ops, as_bin* result_bins, uint32_t* p_n_result_bins,
		as_bin** result_bin_r, int* error_code)
{
	as_namespace* ns = rd->ns;
	*error_code = 0;

	switch (op->op) {
	case AS_MSG_OP_READ: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		if (b) {
			as_bin* masked_bin = &result_bins[*p_n_result_bins];

			if (as_masking_apply(rd->mask_ctx, masked_bin, b)) {
				(*p_n_result_bins)++;
				*result_bin_r = masked_bin;
			}
			else {
				*result_bin_r = b;
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		// Not setting error details for not found because it is such a common case.
		return READ_OP_RESULT_NOT_FOUND;
	}
	case AS_MSG_OP_BITS_READ: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		if (b) {
			as_bin* rb = &result_bins[*p_n_result_bins];
			as_bin_set_empty(rb);

			// Not setting error details here because it's already set in as_bin_bits_read_from_client().
			*error_code = as_bin_bits_read_from_client(b, op, rb);

			if (*error_code < 0) {
				cf_detail(AS_RW,
						"{%s} process_bin_read_op: failed as_bin_bits_read_from_client() %pD",
						ns->name, &rd->r->keyd);
				return READ_OP_RESULT_ERROR;
			}

			if (as_bin_is_used(rb)) {
				(*p_n_result_bins)++;
				*result_bin_r = rb;
			}
			else {
				*result_bin_r = NULL;
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		// Not setting error details for not found because it is such a common case.
		return READ_OP_RESULT_NOT_FOUND;
	}
	case AS_MSG_OP_HLL_READ: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		if (b) {
			as_bin* rb = &result_bins[*p_n_result_bins];
			as_bin_set_empty(rb);

			// Not setting error details here because it's already set in as_bin_hll_read_from_client().
			*error_code = as_bin_hll_read_from_client(b, op, rb);

			if (*error_code < 0) {
				cf_detail(AS_RW,
						"{%s} process_bin_read_op: failed as_bin_hll_read_from_client() %pD",
						ns->name, &rd->r->keyd);
				return READ_OP_RESULT_ERROR;
			}

			if (as_bin_is_used(rb)) {
				(*p_n_result_bins)++;
				*result_bin_r = rb;
			}
			else {
				*result_bin_r = NULL;
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		// Not setting error details for not found because it is such a common case.
		return READ_OP_RESULT_NOT_FOUND;
	}
	case AS_MSG_OP_CDT_READ: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		if (b) {
			as_bin* rb = &result_bins[*p_n_result_bins];
			as_bin_set_empty(rb);

			// Not setting error details here because it's already set in as_bin_cdt_read_from_client().
			*error_code = as_bin_cdt_read_from_client(b, op, rb);

			if (*error_code < 0) {
				cf_detail(AS_RW,
						"{%s} process_bin_read_op: failed as_bin_cdt_read_from_client() %pD",
						ns->name, &rd->r->keyd);
				return READ_OP_RESULT_ERROR;
			}

			if (as_bin_is_used(rb)) {
				(*p_n_result_bins)++;
				*result_bin_r = rb;
			}
			else {
				*result_bin_r = NULL;
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		// Not setting error details for not found because it is such a common case.
		return READ_OP_RESULT_NOT_FOUND;
	}
	case AS_MSG_OP_EXP_READ: {
		const as_exp_ctx exp_ctx = { .ns = ns, .rd = rd, .r = rd->r, .tr = tr };

		as_bin* rb = &result_bins[*p_n_result_bins];

		as_bin_set_empty(rb);
		*error_code = as_bin_exp_read_from_client(&exp_ctx, op, rb);

		if (*error_code < 0) {
			cf_detail(AS_RW,
					"{%s} process_bin_read_op: failed as_bin_exp_read_from_client() %pD",
					ns->name, &rd->r->keyd);
			return READ_OP_RESULT_ERROR;
		}

		if (as_bin_is_used(rb)) {
			(*p_n_result_bins)++;
			*result_bin_r = rb;
		}
		else {
			*result_bin_r = NULL;
		}

		return READ_OP_RESULT_SUCCESS;
	}
	case AS_MSG_OP_STRING_READ: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		// Use stack bin for masked source to avoid using two result_bins slots
		as_bin masked_src;
		bool masked = b && as_masking_apply(rd->mask_ctx, &masked_src, b);

		if (masked) {
			b = &masked_src;
		}

		if (b) {
			as_bin* rb = &result_bins[*p_n_result_bins];
			as_bin_set_empty(rb);

			*error_code = as_bin_string_read_from_client(b, op, rb);

			if (*error_code < 0) {
				cf_detail(AS_RW,
						"{%s} process_bin_read_op: "
						"failed as_bin_string_read_from_client() %pD",
						ns->name, &rd->r->keyd);

				if (masked) {
					as_bin_particle_destroy(&masked_src);
				}

				return READ_OP_RESULT_ERROR;
			}

			if (masked) {
				as_bin_particle_destroy(&masked_src);
			}

			if (as_bin_is_used(rb)) {
				(*p_n_result_bins)++;
				*result_bin_r = rb;
			}
			else {
				*result_bin_r = NULL;
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		return READ_OP_RESULT_NOT_FOUND;
	}
	case AS_MSG_OP_TO_STRING: {
		as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

		// Use stack bin for masked source to avoid using two result_bins slots
		as_bin masked_src;
		bool masked = b && as_masking_apply(rd->mask_ctx, &masked_src, b);
		if (masked) {
			b = &masked_src;
		}

		if (b) {
			as_bin* rb = &result_bins[*p_n_result_bins];
			as_bin_set_empty(rb);
			*error_code = as_bin_to_string(b, rb);
			if (*error_code < 0) {
				cf_detail(AS_RW,
						"{%s} process_bin_read_op: "
						"failed as_bin_to_string() %pD",
						ns->name, &rd->r->keyd);
				if (masked) {
					as_bin_particle_destroy(&masked_src);
				}
				return READ_OP_RESULT_ERROR;
			}
			if (as_bin_is_used(rb)) {
				(*p_n_result_bins)++;
				*result_bin_r = rb;
			}
			else {
				*result_bin_r = NULL;
			}
			if (masked) {
				as_bin_particle_destroy(&masked_src);
			}

			return READ_OP_RESULT_SUCCESS;
		}
		else if (respond_all_ops) {
			*result_bin_r = NULL;
			return READ_OP_RESULT_SUCCESS;
		}

		return READ_OP_RESULT_NOT_FOUND;
	}
	default:
		cf_warning(AS_RW, "{%s} process_bin_read_op: unsupported read op %u %pD",
				ns->name, op->op, &rd->r->keyd);
		as_error_details_set_fmt(AS_SUB_NONE, "unexpected read op %u", op->op);
		*error_code = -AS_ERR_PARAMETER;
		return READ_OP_RESULT_ERROR;
	}
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

	define_deferred_array(bin_name_in_both, bool, n_new_bins);
	define_deferred_array(changed_bins, as_bin*,
			n_old_bins + n_new_bins); // only the 'name' is used
	uint32_t n_changed_bins = 0;

	// Initialize before the critical section to make it shorter.
	memset(bin_name_in_both, 0, n_new_bins * sizeof(bool));
	memset(changed_bins, 0, (n_old_bins + n_new_bins) * sizeof(as_bin*));

	SINDEX_GRLOCK();

	// At max we will do both insert & delete for every sindex in the namespace.
	uint32_t n_sindexes = as_sindex_n_sindexes(ns);
	define_deferred_array(sbins, as_sindex_bin, 2 * n_sindexes);
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

				define_deferred_array(dummy_sbins, as_sindex_bin, n_sindexes);

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

	bins_old_new old_new = { .ns = ns,
		.r = r,
		.old_bins = old_bins,
		.n_old_bins = n_old_bins,
		.new_bins = new_bins,
		.n_new_bins = n_new_bins };

	n_populated += update_sindex_exp(&old_new, changed_bins, n_changed_bins,
			&sbins[n_populated], &record_in_sindex);

	if (! record_in_sindex) {
		// The record may be in some sindex with exp based on unchanged bins.

		old_new.old_bins = NULL;
		old_new.n_old_bins = 0;

		define_deferred_array(dummy_sbins, as_sindex_bin, n_sindexes);
		define_deferred_array(p_new_bins, as_bin*, n_new_bins);

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

	define_deferred_array(changed_bins, as_bin*,
			n_bins); // only the name field is used
	uint32_t n_changed_bins = 0;

	SINDEX_GRLOCK();

	uint32_t n_sindexes = as_sindex_n_sindexes(ns);
	define_deferred_array(sbins, as_sindex_bin, n_sindexes);
	uint32_t n_populated = 0;

	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin* old_bin = &bins[i];

		n_populated += as_sindex_populate_sbins(ns, set_id, old_bin,
				&sbins[n_populated], AS_SINDEX_OP_DELETE);

		changed_bins[n_changed_bins++] = old_bin; // consider all bins changed
	}

	bins_old_new old_new = {
		.ns = ns, .r = r, .old_bins = bins, .n_old_bins = n_bins
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

	// Internal sindex maintenance, not the client's operation: suppress
	// eval-fault staging (verbosity 0) so a faulting sindex expression can't
	// leak/mask a detail on the enclosing transaction. Only "did it produce a
	// value" matters - any non-TRUE result maps to the existing "no sbin"
	// outcome.
	uint8_t saved_verbosity = g_error_verbosity;

	g_error_verbosity = AS_ERROR_VERBOSITY_OFF;

	as_exp_trilean rv = as_exp_eval(si->exp, ctx, &rb, NULL);

	g_error_verbosity = saved_verbosity;

	if (rv != AS_EXP_TRUE) {
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
	as_storage_rd eval_old_rd = { .bins = old_new->old_bins,
		.n_bins = old_new->n_old_bins };
	as_exp_ctx ctx_old_rd = { .ns = ns, .r = r, .rd = &eval_old_rd };
	as_storage_rd eval_new_rd = { .bins = old_new->new_bins,
		.n_bins = old_new->n_new_bins };
	as_exp_ctx ctx_new_rd = { .ns = ns, .r = r, .rd = &eval_new_rd };
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
		cf_vector* exp_binfos = si->exp_bins_info;
		uint32_t exp_bcount = cf_vector_size(exp_binfos);
		bool has_digest_mod = (si->exp->flags & AS_EXP_HAS_DIGEST_MOD) != 0;

		if (has_digest_mod) {
			matched = true; // need to update sindex, also skip bin name check
		}

		for (uint32_t b_ix = 0; b_ix < exp_bcount && ! matched; b_ix++) {
			as_bin_info* exp_bin_info =
					(as_bin_info*)cf_vector_getp(exp_binfos, b_ix);

			for (uint32_t c_ix = 0; c_ix < n_match_bins; c_ix++) {
				if (strcmp(match_bins[c_ix]->name, exp_bin_info->name) == 0) {
					matched = true; // done with this si
					break;
				}
			}
		}

		if (! matched) {
			continue;
		}

		// Optimise if the sindex exp has a digest mod and no bin names.
		if (has_digest_mod && cf_vector_size(si->exp_bins_info) == 0 &&
				old_new->n_old_bins != 0 && old_new->n_new_bins != 0) {
			// Optimisation - as digest will never change, exp result will
			// be the same and we can skip sindex update. But, we need to know
			// if the record is in the sindex.

			as_sindex_bin dummy_sbin;

			as_exp_ctx dummy_ctx = { .ns = ns, .r = old_new->r, .rd = NULL };

			uint32_t n = eval_and_populate_sbin(&dummy_ctx, si, &dummy_sbin,
					AS_SINDEX_OP_INSERT);

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

void
repl_compression_pre_write(rw_request* rw, as_storage_rd* rd,
		as_transaction* tr, repl_compression_ctx* ctx)
{
	// The single mode read for this write - see the header. Everything below,
	// and everything in repl_compression_post_write(), works off this snapshot
	// and never re-reads ns->repl_compression_mode.
	ctx->mode = rd->ns->repl_compression_mode;

	// A commit-level-master write responds to the client on master completion
	// and the replica suppresses its ack, so a delta the replica cannot apply
	// against its base version is never reported and never rebuilt as a full
	// pickle - no deltas for these writes. Plain zstd is unaffected (it needs
	// nothing from the replica) - see compute_compression_for_replication.
	//
	// So this is read by the delta helpers only, and only in DELTA_ZSTD mode -
	// in plain-ZSTD mode it is computed and never consumed. Computed uniformly
	// anyway rather than under a mode test, so there is exactly one place that
	// decides it for this write.
	ctx->no_repl_ack =
			ctx->mode != AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_NONE &&
			respond_on_master_complete(tr);

	if (ctx->mode == AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_NONE) {
		return;
	}

	// Snapshot the old record's flat bytes for delta replication BEFORE the
	// storage write frees them. For in-memory namespaces rd->flat aliases the
	// arena block the write frees in place, so reading it post-write (in
	// compute_delta_for_replication) is a use-after-free.
	capture_delta_base_for_replication(rw, rd, ctx->mode, ctx->no_repl_ack);
}

void
repl_compression_post_write(rw_request* rw, as_storage_rd* rd,
		const as_record* old_r, const repl_compression_ctx* ctx)
{
	if (ctx->mode == AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_NONE) {
		return;
	}

	// Computed once and passed to both helpers; they would otherwise each re-run
	// the full flat meta-unpack on the same pickle (every fresh create in
	// delta-zstd mode hits both).
	bool pickle_storage_compressed = rd->pickle != NULL &&
			as_flat_pickle_is_storage_compressed(rd->pickle, rd->pickle_sz);

	compute_delta_for_replication(rw, rd, old_r, ctx->mode, ctx->no_repl_ack,
			pickle_storage_compressed);
	compute_compression_for_replication(rw, rd, ctx->mode,
			pickle_storage_compressed);
}

void
capture_delta_base_for_replication(rw_request* rw, as_storage_rd* rd,
		replication_compression_mode mode, bool no_repl_ack)
{
	// Snapshot the OLD record's flat bytes BEFORE as_storage_record_write()
	// runs. For in-memory namespaces rd->flat aliases the storage arena, and
	// the write frees that block in place - reading rd->flat afterward in
	// compute_delta_for_replication() is a use-after-free (device-backed
	// namespaces are safe because rd->flat is a private read buffer, but we
	// snapshot uniformly). compute_delta_for_replication() patches against this
	// snapshot.
	//
	// Mirrors the cheap, pre-write-knowable guards in
	// compute_delta_for_replication(); the storage-compressed check there needs
	// the post-write pickle, so a snapshot is occasionally taken and then
	// dropped - freed with the rw_request.
	if (no_repl_ack) {
		return; // no delta for these writes - see compute_delta_for_replication
	}

	// Caller snapshots repl_compression_mode once before the storage write and
	// passes the same value here and to compute_delta_for_replication(). If the
	// two disagreed - an info thread flipping the mode to delta-zstd in the
	// window between the calls - capture could skip the snapshot while compute
	// still ran the delta path, reading the freed-in-place rd->flat (a UAF for
	// in-memory namespaces). One snapshot for both closes that race.
	if (mode != AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_DELTA_ZSTD) {
		return;
	}

	// Delete ops never compute delta/compressed ops - don't pay a record-sized
	// snapshot for a delta the tombstone guard in
	// compute_delta_for_replication() will never compute. The caller runs
	// transition_delete_metadata() before this, so the tombstone bit already
	// reflects this write.
	if (rd->r->tombstone == 1) {
		return;
	}

	if (rw->n_dest_nodes == 0 || rd->flat == NULL) {
		return; // no destinations, or a new-record create (no old version)
	}

	// Storage-compressed old record: the bin-load path
	// (as_flat_decompress_bins()) repointed rd->flat_end and rd->flat_bins
	// into a separate decompression buffer, so rd->flat_end - rd->flat spans
	// two unrelated allocations and the memcpy below would run wild. There is
	// no coherent base to snapshot - and compressed bytes wouldn't match the
	// replica's stored flat anyway. Record the skip on the rw_request so
	// compute_delta_for_replication() also skips - post-write it cannot
	// safely re-inspect rd->flat (freed in place for in-memory namespaces).
	if (rd->flat->is_compressed == 1) {
		rw->delta_base_storage_compressed = true;
		return;
	}

	uint32_t flat_sz = rd->flat_end - (const uint8_t*)rd->flat;

	rw->delta_base = cf_malloc(flat_sz);
	memcpy(rw->delta_base, rd->flat, flat_sz);
	rw->delta_base_sz = flat_sz;
}

void
compute_delta_for_replication(rw_request* rw, as_storage_rd* rd,
		const as_record* old_r, replication_compression_mode mode,
		bool no_repl_ack, bool pickle_storage_compressed)
{
	// A commit-level-master (FROM_CLIENT) write responds to the client on
	// master completion, and the replica suppresses its ack
	// (RW_INFO_NO_REPL_ACK). A delta is the one wire form whose apply can fail
	// for an ordinary, expected reason - the replica must hold the exact base
	// version the patch was built against, and a replica that missed a prior
	// write, or never had the record, simply does not. Normally that miss is
	// reported and repl_write_handle_ack() rebuilds a full pickle; with no ack
	// the miss is silent and the replica stays divergent. So deltas are off for
	// these writes.
	//
	// This does NOT extend to plain zstd - see
	// compute_compression_for_replication. A compressed pickle is
	// self-contained: decoding it needs nothing from the replica, so there is no
	// expected-failure case for the missing ack to hide.
	if (no_repl_ack) {
		return;
	}

	// mode is the caller's pre-write snapshot, shared with
	// capture_delta_base_for_replication() so the two cannot disagree.
	if (mode != AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_DELTA_ZSTD) {
		return;
	}

	if (rw->n_dest_nodes == 0) {
		return;
	}

	// Delete ops never compute delta/compressed ops. Non-durable deletes never
	// get here (no tombstone is written, so rd->pickle stays NULL below), but
	// a durable delete writes a tombstone pickle and would otherwise fall
	// through - bumping delta_attempts and building a delta for a delete.
	if (rd->r->tombstone == 1) {
		return;
	}

	if (rd->pickle == NULL) {
		return;
	}

	if (pickle_storage_compressed) {
		cf_debug(AS_RW,
				"compute_delta_for_replication: skipping delta for storage-compressed pickle %pD",
				&rw->keyd);
		return;
	}

	if (rd->flat == NULL) {
		// New record creation, no old version
		return;
	}

	// Capture saw a storage-compressed old flat and took no snapshot - the
	// flat/flat_end pair is incoherent (flat_end points into the bin-
	// decompression buffer) so there is no base to patch against. Trust the
	// pre-write flag rather than re-inspecting rd->flat, which the storage
	// write may have freed in place. Fall through to the plain-zstd path via
	// compute_compression_for_replication().
	if (rw->delta_base_storage_compressed) {
		return;
	}

	// Patch against the pre-write snapshot when we took one - the only
	// arena-stable source of the old bytes for in-memory namespaces, where
	// rd->flat has been freed in place by as_storage_record_write().
	// Fall back to rd->flat when no snapshot exists (device-backed namespaces
	// keep it valid post-write; unit tests supply it directly).
	const uint8_t* base_flat;
	uint32_t flat_sz;

	if (rw->delta_base != NULL) {
		base_flat = rw->delta_base;
		flat_sz = rw->delta_base_sz;
	}
	else {
		// Direct-call path (no capture ran - unit tests supply rd->flat, and
		// device-backed namespaces keep it valid post-write): same skip as
		// the delta_base_storage_compressed flag above, derived from the flat itself.
		if (rd->flat->is_compressed == 1) {
			return;
		}

		base_flat = (const uint8_t*)rd->flat;
		flat_sz = rd->flat_end - (const uint8_t*)rd->flat;
	}

	if (flat_sz < sizeof(as_flat_record)) {
		// A valid flat is always at least the header; a shorter base means
		// local corruption. Skip the delta rather than zero a tree_id past the
		// end of the buffer in as_flat_*_canonical().
		return;
	}

	as_incr_uint64(&rd->ns->repl_wire_comp_stat.delta_attempts);

	// Strip master's tree_id from the source flat so the replica's apply,
	// running against its own (different) tree_id, sees byte-identical input.
	// When we own a private pre-write snapshot (rw->delta_base), canonicalize it
	// in place - no second record-sized copy. Only when falling back to
	// rd->flat (the stored record, which must not be mutated) do we copy.
	const uint8_t* canonical_base;
	uint8_t* canonical_owned = NULL;

	if (rw->delta_base != NULL) {
		as_flat_canonicalize_delta_inplace(rw->delta_base);
		canonical_base = rw->delta_base;
	}
	else {
		canonical_owned = cf_malloc(flat_sz);
		as_flat_make_delta_canonical(canonical_owned, base_flat, flat_sz);
		canonical_base = canonical_owned;
	}

	rw->delta_sz = zstd_wire_make_patch((void**)&rw->delta, canonical_base,
			flat_sz, rd->pickle, rd->pickle_sz, rd->ns->repl_compression_level);

	if (canonical_owned != NULL) {
		cf_free(canonical_owned);
	}

	if (rw->delta_sz == 0) {
		return;
	}

	// Don't ship a delta that's bigger than the plain pickle. Drop it
	// and let compute_compression_for_replication or the plain path take
	// over. delta_hits is NOT counted here - the increment below is after this
	// early return, so it only counts deltas we actually ship. The counter that
	// does include this case is delta_attempts, incremented above BEFORE
	// zstd_wire_make_patch() runs - so it also counts patches that failed to
	// build (delta_sz == 0, the early return just above). delta_not_beneficial
	// records how often this particular outcome fires, so
	// attempts - hits - not_beneficial isolates the other failure modes,
	// make_patch failures among them.
	if (rw->delta_sz >= rd->pickle_sz) {
		cf_free(rw->delta);
		rw->delta = NULL;
		rw->delta_sz = 0;
		as_incr_uint64(&rd->ns->repl_wire_comp_stat.delta_not_beneficial);
		return;
	}

	as_incr_uint64(&rd->ns->repl_wire_comp_stat.delta_hits);

	// Per-destination savings; credited to ns->bytes_saved at ack-time so the
	// counter only reflects bytes actually saved on the wire.
	rw->per_dest_bytes_saved = (uint64_t)(rd->pickle_sz - rw->delta_sz);

	rw->use_delta = true;
	rw->use_compressed = false;
	rw->base_generation = old_r->generation;
	rw->base_lut = old_r->last_update_time;
}

// Deliberately NOT gated on no_repl_ack (commit-level=master), unlike the delta
// helpers above. A compressed pickle carries the whole record, so the replica
// decodes it without needing any local state - there is no expected-failure case
// the missing ack could hide. The only way the decode fails is a fault we can't
// guard against anyway (allocation failure, memory or wire corruption, a codec
// bug), and commit-level=master is already fire-and-forget for those: a plain
// repl write refused for overload, a failed partition reservation, or a failed
// apply is dropped just as silently today. Nor is an acknowledged write at
// stake - strong consistency forces write-commit-level all (see cfg_ee.c), so
// respond_on_master_complete() is only ever true in an AP namespace, where a
// dropped replica write is repaired by migration/duplicate resolution.
//
// Gating compression on the ack would mean every commit-level=master namespace
// quietly gets no wire compression at all - the config would read as on and
// save nothing. Since these writes do compress, their savings are reported too:
// with no ack to credit them, send_rw_messages_forget() credits bytes_saved at
// send time. Only the delta counters stay at zero for these namespaces, which
// follows from deltas being off above.
void
compute_compression_for_replication(rw_request* rw, as_storage_rd* rd,
		replication_compression_mode mode, bool pickle_storage_compressed)
{
	// Delta path already produced a wire payload — leave it alone.
	if (rw->use_delta) {
		return;
	}

	// Plain-zstd is the wire compression for both ZSTD and DELTA_ZSTD
	// modes. In DELTA_ZSTD mode it's the fallback when the delta path
	// couldn't engage (no prior, storage-compressed pickle, zero dest
	// nodes, etc.) — every fresh-create record hits that path. Without
	// this fallback, the entire load phase of any workload, plus
	// migration receipt, XDR-receipt, and post-eviction recreates,
	// ship uncompressed on the wire.
	// mode is the caller's pre-write snapshot, shared with the delta helpers so
	// all three agree on a single value for this write.
	if (mode != AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_ZSTD &&
			mode != AS_NAMESPACE_REPLICATION_COMPRESSION_MODE_DELTA_ZSTD) {
		return;
	}

	if (rw->n_dest_nodes == 0 || rd->pickle == NULL) {
		return;
	}

	// Delete ops never compute delta/compressed ops - see
	// compute_delta_for_replication. A durable delete's tombstone pickle ships
	// plain.
	if (rd->r->tombstone == 1) {
		return;
	}

	if (pickle_storage_compressed) {
		cf_debug(AS_RW,
				"compute_compression_for_replication: skipping wire zstd for storage-compressed pickle %pD",
				&rw->keyd);
		return;
	}

	// Don't pay a full compress + ZSTD_compressBound allocation for a record
	// too small to plausibly beat zstd's framing overhead - common for small
	// OLTP writes. Skip outright rather than compress-then-discard.
	if (rd->pickle_sz < ZSTD_WIRE_COMPRESS_MIN_SZ) {
		return;
	}

	rw->compressed_sz = zstd_wire_compress_buffer((void**)&rw->compressed,
			rd->pickle, rd->pickle_sz, rd->ns->repl_compression_level);

	if (rw->compressed_sz == 0) {
		return;
	}

	// Don't ship a compressed payload that's bigger than the plain pickle.
	// Drop it and let the plain RW_OP_REPL_WRITE path take over.
	if (rw->compressed_sz >= rd->pickle_sz) {
		cf_free(rw->compressed);
		rw->compressed = NULL;
		rw->compressed_sz = 0;
		as_incr_uint64(&rd->ns->repl_wire_comp_stat.compression_not_beneficial);
		return;
	}

	// Per-destination savings; credited to ns->bytes_saved at ack-time
	rw->per_dest_bytes_saved = (uint64_t)(rd->pickle_sz - rw->compressed_sz);
	rw->use_compressed = true;
}
