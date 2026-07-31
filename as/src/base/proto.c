/*
 * proto.c
 *
 * Copyright (C) 2008-2026 Aerospike, Inc.
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

#include "base/proto.h"

#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "socket.h"
#include "vector.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/masking.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "storage/storage.h"

//==========================================================
// Typedefs & constants.
//

#define MSG_STACK_BUFFER_SZ (1024 * 16)

static const char SUCCESS_BIN_NAME[] = "SUCCESS";
static const char FAILURE_BIN_NAME[] = "FAILURE";

static __thread uint8_t g_error_details[AS_ERROR_DETAILS_MAX];
__thread uint32_t g_error_details_len;
__thread bool g_error_details_set;
__thread uint8_t g_error_verbosity;
__thread as_error_exp_trace g_error_exp_trace;

//==========================================================
// Forward declarations.
//

static int send_reply_buf(as_file_handle* fd_h, const uint8_t* msgp,
		size_t msg_sz);

//==========================================================
// Public API - byte swapping.
//

void
as_proto_swap(as_proto* proto)
{
	uint8_t version = proto->version;
	uint8_t type = proto->type;

	proto->version = proto->type = 0;
	proto->sz = cf_swap_from_be64(*(uint64_t*)proto);
	proto->version = version;
	proto->type = type;
}

void
as_msg_swap_header(as_msg* m)
{
	m->generation = cf_swap_from_be32(m->generation);
	m->record_ttl = cf_swap_from_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_from_be32(m->transaction_ttl);
	m->n_fields = cf_swap_from_be16(m->n_fields);
	m->n_ops = cf_swap_from_be16(m->n_ops);
}

void
as_msg_swap_field(as_msg_field* mf)
{
	mf->field_sz = cf_swap_from_be32(mf->field_sz);
}

void
as_msg_swap_op(as_msg_op* op)
{
	op->op_sz = cf_swap_from_be32(op->op_sz);

	if (op->has_lut == 1) {
		uint64_t* lut = (uint64_t*)(op->name + op->name_sz);

		*lut = cf_swap_from_be64(*lut);
	}
}

//==========================================================
// Public API - error message response support.
//

// The "middle elided" marker rendered into the path array when the real
// nesting depth exceeded AS_EXP_TRACE_MAX_FRAMES (between the outermost frames
// and the innermost failing op).
static const char EXP_TRACE_PATH_ELLIPSIS[] = "...";

// Upper bound on the bytes pack_exp_trace() will write for this trace,
// including the key-3 header. include_operands / include_snippet /
// include_path reflect the budget cascade's drop order (operands first, then
// snippet, then path) - a tier is dropped whole, never truncated.
static uint32_t
exp_trace_max_sz(const as_error_exp_trace* t, bool include_path,
		bool include_snippet, bool include_operands)
{
	// key 3 (1) + sub-map header (1) + phase k/v (2) = 4. The phase value packs
	// as 1 byte: phase is a small enum (AS_EXP_TRACE_PHASE_BUILD/EVAL, both
	// < 128), the only values any caller stages.
	uint32_t sz = 4;

	if (t->has_offset) {
		sz += 1 + 9; // key + uint value
	}

	if (t->has_op) {
		sz += 1 + 3 + t->op_len; // key + str header + bytes
	}

	// n_frames == 0 would make the truncation arithmetic below emit a list
	// header that over-claims its element count - guard it (the build path
	// never sets has_path without frames, but hand-staged traces could).
	if (include_path && t->has_path && t->n_frames != 0) {
		sz += 1 + 9; // depth key + uint value
		sz += 1 + 3; // path key + array header

		for (uint16_t i = 0; i < t->n_frames; i++) {
			sz += 3 + (uint32_t)strlen(t->path[i]); // str header + bytes
		}

		if (t->path_truncated) {
			sz += 3 + (uint32_t)(sizeof(EXP_TRACE_PATH_ELLIPSIS) - 1);
		}
	}

	if (include_snippet && t->has_snippet) {
		sz += 1 + 3 + t->snippet_len; // key + str header + bytes
	}

	// Operand values: key + 2-element str array [lhs, rhs].
	if (include_operands && t->has_operands) {
		sz += 1 + 3; // key + array header
		sz += 3 + t->lhs_len; // str header + bytes
		sz += 3 + t->rhs_len;
	}

	// outcome (small enum, structural - never dropped by the budget logic).
	if (t->has_outcome) {
		sz += 1 + 9; // key + uint value
	}

	// AEL locator keys (small, structural - never dropped by the budget logic).
	if (t->has_lang) {
		sz += 1 + 9; // key + uint value
	}

	if (t->has_ael_offset) {
		sz += 1 + 9;
	}

	if (t->has_ael_span) {
		sz += 1 + 9;
	}

	return sz;
}

// Pack the staged expression trace as field-45 key 3 (a nested map): phase,
// optional byte_offset + op, depth + path array root -> fault, the human-only
// snippet, outcome, the AEL lang/offset/span locator, and the decisive
// operands. include_path / include_snippet / include_operands reflect the
// budget decision made by the caller, which has already accounted for this key
// in the outer map header.
static void
pack_exp_trace(as_packer* pk, const as_error_exp_trace* t, bool include_path,
		bool include_snippet, bool include_operands)
{
	// Keep this condition identical to exp_trace_max_sz - n_frames == 0 would
	// over-claim the path list header (see the guard there).
	bool emit_path = include_path && t->has_path && t->n_frames != 0;
	bool emit_snippet = include_snippet && t->has_snippet;
	bool emit_operands = include_operands && t->has_operands;

	uint32_t n_sub = 1; // phase is always present

	if (t->has_offset) {
		n_sub++;
	}

	if (t->has_op) {
		n_sub++;
	}

	if (emit_path) {
		n_sub += 2; // depth + path
	}

	if (emit_snippet) {
		n_sub++;
	}

	if (emit_operands) {
		n_sub++;
	}

	if (t->has_outcome) {
		n_sub++;
	}

	if (t->has_lang) {
		n_sub++;
	}

	if (t->has_ael_offset) {
		n_sub++;
	}

	if (t->has_ael_span) {
		n_sub++;
	}

	// The caller's budget cascade (exp_trace_max_sz vs AS_ERROR_DETAILS_MAX)
	// is a strict upper bound on the bytes written below, so none of these
	// packs can overflow: the uint/header packs (key + value, map/list headers)
	// are guaranteed to fit and their returns aren't checked. Only the variable
	// str packs (op/path/snippet) carry a cf_crash backstop - that's deliberate
	// (str sizing has the most moving parts), not an inconsistency. If the
	// budget ever under-counts, the str crash fires; a header/uint would instead
	// truncate silently, so keep exp_trace_max_sz a true over-estimate.
	// exp_trace_max_sz hardcodes this header at one byte, which holds only
	// while the sub-map is a fixmap. Past 15 entries it widens to three and the
	// sizer would under-count by two - the silent-truncation class above, not
	// the str crash. 11 keys are reachable today, 2 more are reserved.
	cf_assert(n_sub <= 15, AS_PROTO,
			"exp trace sub-map outgrew fixmap (%u entries) - widen the header "
			"estimate in exp_trace_max_sz",
			n_sub);

	as_pack_uint64(pk, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	as_pack_map_header(pk, n_sub);

	as_pack_uint64(pk, AS_EXP_TRACE_KEY_PHASE);
	as_pack_uint64(pk, t->phase);

	if (t->has_offset) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_BYTE_OFFSET);
		as_pack_uint64(pk, t->byte_offset);
	}

	if (t->has_op) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_OP);

		if (as_pack_str(pk, (const uint8_t*)t->op, t->op_len) != 0) {
			cf_crash(AS_PROTO, "error detail exp-trace op didn't fit");
		}
	}

	if (emit_path) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_DEPTH);
		as_pack_uint64(pk, t->depth);

		as_pack_uint64(pk, AS_EXP_TRACE_KEY_PATH);

		// When truncated, the gap between the outermost frames and the innermost
		// failing op is rendered as an explicit "..." element.
		uint32_t n_eles = t->path_truncated ? t->n_frames + 1u : t->n_frames;
		uint32_t inner_ix = t->n_frames - 1u; // innermost failing op slot

		as_pack_list_header(pk, n_eles);

		for (uint16_t i = 0; i < t->n_frames; i++) {
			if (t->path_truncated && i == inner_ix) {
				if (as_pack_str(pk, (const uint8_t*)EXP_TRACE_PATH_ELLIPSIS,
							(uint32_t)(sizeof(EXP_TRACE_PATH_ELLIPSIS) - 1)) !=
						0) {
					cf_crash(AS_PROTO, "error detail exp-trace path didn't fit");
				}
			}

			if (as_pack_str(pk, (const uint8_t*)t->path[i],
						(uint32_t)strlen(t->path[i])) != 0) {
				cf_crash(AS_PROTO, "error detail exp-trace path didn't fit");
			}
		}
	}

	if (emit_snippet) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_SNIPPET);

		if (as_pack_str(pk, (const uint8_t*)t->snippet, t->snippet_len) != 0) {
			cf_crash(AS_PROTO, "error detail exp-trace snippet didn't fit");
		}
	}

	if (emit_operands) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_OPERANDS);
		as_pack_list_header(pk, 2);

		if (as_pack_str(pk, (const uint8_t*)t->lhs, t->lhs_len) != 0) {
			cf_crash(AS_PROTO, "error detail exp-trace operand didn't fit");
		}

		if (as_pack_str(pk, (const uint8_t*)t->rhs, t->rhs_len) != 0) {
			cf_crash(AS_PROTO, "error detail exp-trace operand didn't fit");
		}
	}

	if (t->has_outcome) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_OUTCOME);
		as_pack_uint64(pk, t->outcome);
	}

	if (t->has_lang) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_LANG);
		as_pack_uint64(pk, t->lang);
	}

	if (t->has_ael_offset) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_AEL_OFFSET);
		as_pack_uint64(pk, t->ael_offset);
	}

	if (t->has_ael_span) {
		as_pack_uint64(pk, AS_EXP_TRACE_KEY_AEL_SPAN);
		as_pack_uint64(pk, t->ael_span);
	}
}

// Decide which trace tiers fit in 'budget' bytes, dropping in fixed order
// (richest/least essential first): the operand values, then the snippet, then
// the path - never truncating a tier to something misleading. Returns false
// when even the phase/offset/op/depth core won't fit (emit no trace at all).
// Shared by as_error_details_set_fmt (budget = cap minus subcode/message) and
// the late-append path below (budget = cap minus the assembled payload).
static bool
exp_trace_fit(const as_error_exp_trace* t, uint32_t budget, bool* include_path,
		bool* include_snippet, bool* include_operands)
{
	*include_path = true;
	*include_snippet = true;
	*include_operands = true;

	if (exp_trace_max_sz(t, *include_path, *include_snippet, *include_operands) >
			budget) {
		*include_operands = false; // drop the operand values first
	}

	if (exp_trace_max_sz(t, *include_path, *include_snippet, *include_operands) >
			budget) {
		*include_snippet = false; // then the snippet
	}

	if (exp_trace_max_sz(t, *include_path, *include_snippet, *include_operands) >
			budget) {
		*include_path = false; // then the path
	}

	// If even the phase/offset/op/depth core won't fit, drop the trace.
	return exp_trace_max_sz(t, *include_path, *include_snippet,
				   *include_operands) <= budget;
}

// Late attach: serialize the just-staged trace into an ALREADY-assembled
// field-45 payload. Covers the message-first ordering: a subsystem deep
// inside expression evaluation (CDT ops, select) authors its message via
// as_error_details_set_fmt before the fault reaches the exp boundary that
// stages the trace. Subcode/message first-set-wins is untouched - the outer
// map is always a fixmap (<= 3 entries) with the trace last when present, so
// attaching = append key 3 at the end and bump the count byte. Safe because
// the payload is only ever read at reply-build time (error_msg_field_prep),
// never mid-eval - error_msg_field_write() asserts that, since prep hands out an
// alias plus a length snapshot and this mutation would invalidate the pair.
static void
error_details_append_trace(void)
{
	bool include_path;
	bool include_snippet;
	bool include_operands;

	if (g_error_details_len == 0) {
		// set_fmt's nothing-to-convey arm claimed the slot without producing
		// bytes - build a fresh trace-only map. If even the core won't fit
		// (can't happen at today's sizes), emit nothing rather than an empty
		// map: error_msg_field_prep relies on len > 0 <=> valid payload.
		if (! exp_trace_fit(&g_error_exp_trace, AS_ERROR_DETAILS_MAX - 1,
					&include_path, &include_snippet, &include_operands)) {
			return;
		}

		as_packer pk = { .buffer = g_error_details,
			.capacity = AS_ERROR_DETAILS_MAX };

		as_pack_map_header(&pk, 1);
		pack_exp_trace(&pk, &g_error_exp_trace, include_path, include_snippet,
				include_operands);

		cf_assert(pk.offset <= AS_ERROR_DETAILS_MAX, AS_PROTO,
				"exp trace overran error-details buffer");

		g_error_details_len = pk.offset;
		return;
	}

	// (0x80 | n) fixmap - set_fmt packs at most 3 entries, and a trace can't
	// already be among them: if set_fmt had packed one, g_error_exp_trace.set
	// would have blocked this staging before the append.
	cf_assert((g_error_details[0] & 0xf0) == 0x80, AS_PROTO,
			"unexpected error-details map header 0x%x", g_error_details[0]);

	if (! exp_trace_fit(&g_error_exp_trace,
				AS_ERROR_DETAILS_MAX - g_error_details_len, &include_path,
				&include_snippet, &include_operands)) {
		return; // whole trace dropped - leave the payload untouched (no bump)
	}

	as_packer pk = { .buffer = g_error_details,
		.capacity = AS_ERROR_DETAILS_MAX,
		.offset = g_error_details_len };

	pack_exp_trace(&pk, &g_error_exp_trace, include_path, include_snippet,
			include_operands);

	// The packer's capacity hard-bounds every write, so this documents the
	// commit-side invariant rather than catching a reachable overrun.
	cf_assert(pk.offset <= AS_ERROR_DETAILS_MAX, AS_PROTO,
			"exp trace overran error-details buffer");

	g_error_details[0]++; // count the appended entry - fixmap, so +1 is safe
	g_error_details_len = pk.offset;
}

void
as_error_exp_trace_set(const as_error_exp_trace* t)
{
	// First-set-wins (deepest build failure wins) and gated at the richest
	// verbosity tier - mirrors as_error_details_set_fmt's early-out. Below the
	// tier this function allocates nothing, takes no lock and logs nothing.
	//
	// It is not free tree-wide, but what remains below the tier is small and
	// per-op, never per-byte:
	//   - the eval path tests rt->explain, once universally in rt_eval and
	//     again per comparison / boolean op;
	//   - build_next builds a two-word ancestor frame and swaps the chain
	//     pointer in and back out on every op - four stores. That chain is not
	//     only for this: the build-failure path/depth needs it at every
	//     tier >= 1;
	//   - an AEL build sizes an op-ordinal -> source-span map into the
	//     expression's own allocation, 4 bytes per op, in addition to the AEL
	//     source text the runtime bin table already requires.
	// None of it scales with record or bin size, so an expression run with
	// error details off pays a small constant per op and no separate
	// allocation.
	if (g_error_exp_trace.set || g_error_verbosity < AS_ERROR_VERBOSITY_TRACE) {
		return;
	}

	g_error_exp_trace = *t;
	g_error_exp_trace.set = true;

	if (g_error_details_set) {
		// The message was authored first (e.g. a CDT op deep inside eval,
		// first-set-wins) - attach the trace to the assembled payload so the
		// stage/author ordering doesn't decide whether the trace ships.
		error_details_append_trace();
	}
}

void
as_error_details_set_fmt(uint32_t subcode, const char* format, ...)
{
	if (g_error_details_set || g_error_verbosity == AS_ERROR_VERBOSITY_OFF) {
		return;
	}

	// First-set-wins is tracked separately from the payload length: a
	// subcode-only call at verbosity 1 with AS_SUB_NONE produces no wire
	// bytes (see below), but it still claims the slot so that an outer
	// fallback can't overwrite a deeper site's deliberate "no subcode".
	g_error_details_set = true;

	bool has_subcode = subcode != AS_SUB_NONE;

	// Only verbosity >= 2 carries a message; verbosity 1 emits the subcode
	// alone. Both shapes are built by the single packer below - a verbosity-1
	// call just leaves message_len at 0.
	char message[AS_ERROR_MESSAGE_MAX];
	uint32_t message_len = 0;

	if (g_error_verbosity >= AS_ERROR_VERBOSITY_MESSAGES) {
		va_list ap;
		va_start(ap, format);
		int n = vsnprintf(message, sizeof(message), format, ap);
		va_end(ap);

		if (n > 0) {
			message_len = (uint32_t)n;

			if (message_len > sizeof(message) - 1) {
				message_len = sizeof(message) - 1;
			}
		}
	}

	bool has_message = message_len > 0;

	// The structured expression trace rides only at the richest tier; it is
	// staged by an in-scope caller (verbosity-gated, first-set-wins) before
	// this call. It composes with the subcode/message in the same field-45 map.
	bool has_trace = g_error_exp_trace.set &&
			g_error_verbosity >= AS_ERROR_VERBOSITY_TRACE;

	// Keep the whole map inside AS_ERROR_DETAILS_MAX. The phase/offset/op/depth
	// core is tiny, but the operand values, a long snippet, or a deep path can
	// push a near-max message over budget - exp_trace_fit owns the drop order.
	// The core is kept as long as the message itself fits.
	bool include_operands = true;
	bool include_snippet = true;
	bool include_path = true;

	if (has_trace) {
		uint32_t base_sz = 1; // outer map header
		base_sz += has_subcode ? 1 + 9 : 0; // subcode key + uint value
		base_sz += has_message ? 1 + 3 + message_len : 0; // key + str hdr + bytes

		has_trace = exp_trace_fit(&g_error_exp_trace,
				AS_ERROR_DETAILS_MAX - base_sz, &include_path, &include_snippet,
				&include_operands);
	}

	// Nothing to convey - omit field 45 entirely instead of an empty map.
	if (! has_subcode && ! has_message && ! has_trace) {
		return;
	}

	as_packer pk = { .buffer = g_error_details, .capacity = AS_ERROR_DETAILS_MAX };

	as_pack_map_header(&pk,
			(has_subcode ? 1 : 0) + (has_message ? 1 : 0) + (has_trace ? 1 : 0));

	if (has_subcode) {
		as_pack_uint64(&pk, AS_ERROR_DETAIL_KEY_SUBCODE);
		as_pack_uint64(&pk, subcode);
	}

	if (has_message) {
		as_pack_uint64(&pk, AS_ERROR_DETAIL_KEY_MESSAGE);

		if (as_pack_str(&pk, (const uint8_t*)message, message_len) != 0) {
			cf_crash(AS_PROTO,
					"error detail message didn't fit - "
					"check AS_ERROR_MSGPACK_OVERHEAD");
		}
	}

	if (has_trace) {
		pack_exp_trace(&pk, &g_error_exp_trace, include_path, include_snippet,
				include_operands);
	}

	g_error_details_len = pk.offset;
}

const uint8_t*
as_error_msg_peek(uint32_t* len)
{
	if (len != NULL) {
		*len = g_error_details_len;
	}

	return g_error_details_len == 0 ? NULL : g_error_details;
}

//==========================================================
// Public API - generating internal transactions.
//

// Allocates cl_msg returned - caller must free it. Everything is host-ordered.
// Will add more parameters (e.g. for set name) only as they become necessary.
cl_msg*
as_msg_create_internal(const char* ns_name, uint8_t info1, uint8_t info2,
		uint8_t info3, uint32_t record_ttl, uint16_t n_ops, uint8_t* ops,
		size_t ops_sz)
{
	size_t ns_name_len = strlen(ns_name);

	size_t msg_sz = sizeof(cl_msg) + sizeof(as_msg_field) + ns_name_len + ops_sz;

	cl_msg* msgp = (cl_msg*)cf_malloc(msg_sz);

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_msg* m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = info1;
	m->info2 = info2;
	m->info3 = info3;
	m->info4 = 0;
	m->result_code = 0;
	m->generation = 0;
	m->record_ttl = record_ttl;
	m->transaction_ttl = 0;
	m->n_fields = 1;
	m->n_ops = n_ops;

	as_msg_field* mf = (as_msg_field*)(m->data);

	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	mf->field_sz = (uint32_t)ns_name_len + 1;
	memcpy(mf->data, ns_name, ns_name_len);

	if (ops != NULL) {
		uint8_t* msg_ops = (uint8_t*)as_msg_field_get_next(mf);

		memcpy(msg_ops, ops, ops_sz);
	}

	return msgp;
}

//==========================================================
// Public API - packing responses.
//

// Allocates cl_msg returned - caller must free it.
cl_msg*
as_msg_make_response_msg(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op** ops, as_bin** bins, uint16_t bin_count,
		as_namespace* ns, cl_msg* msgp_in, size_t* msg_sz_in,
		as_record_version* v, uint32_t mrt_deadline, bool include_error_msg)
{
	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);
	error_msg_field err_field =
			error_msg_field_prep(include_error_msg, result_code != AS_OK);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (mrt_deadline != 0) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(uint32_t);
	}

	if (err_field.add) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + err_field.len;
	}

	msg_sz += sizeof(as_msg_op) * bin_count;

	for (uint16_t i = 0; i < bin_count; i++) {
		if (ops) {
			msg_sz += ops[i]->name_sz;
		}
		else if (bins[i]) {
			msg_sz += strlen(bins[i]->name);
		}
		else {
			cf_crash(AS_PROTO, "making response message with null bin and op");
		}

		if (bins[i]) {
			msg_sz += as_bin_particle_client_value_size(bins[i]);
		}
	}

	uint8_t* buf;

	if (! msgp_in || *msg_sz_in < msg_sz) {
		buf = cf_malloc(msg_sz);
	}
	else {
		buf = (uint8_t*)msgp_in;
	}

	*msg_sz_in = msg_sz;

	cl_msg* msgp = (cl_msg*)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg* m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation == 0 ? 0 : plain_generation(generation, ns);
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = bin_count;

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field* mf = (as_msg_field*)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version*)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (mrt_deadline != 0) {
		as_msg_field* mf = (as_msg_field*)buf;

		mf->field_sz = 1 + sizeof(uint32_t);
		mf->type = AS_MSG_FIELD_TYPE_MRT_DEADLINE;
		*(uint32_t*)mf->data = cf_swap_to_le32(mrt_deadline);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(uint32_t);
	}

	error_msg_field_write(&buf, &err_field);

	for (uint16_t i = 0; i < bin_count; i++) {
		as_msg_op* op = (as_msg_op*)buf;

		op->has_lut = 0;
		op->unused_flags = 0;

		if (ops) {
			op->op = ops[i]->op;
			memcpy(op->name, ops[i]->name, ops[i]->name_sz);
			op->name_sz = ops[i]->name_sz;
		}
		else {
			op->op = AS_MSG_OP_READ;
			op->name_sz = as_bin_memcpy_name(op->name, bins[i]);
		}

		op->op_sz = OP_FIXED_SZ + op->name_sz;

		buf += sizeof(as_msg_op) + op->name_sz;
		buf += as_bin_particle_to_client(bins[i], op);

		as_msg_swap_op(op);
	}

	return msgp;
}

// Pass NULL bb_r for sizing only. Return value is size if >= 0, error if < 0.
int32_t
as_msg_make_response_bufbuilder(cf_buf_builder** bb_r, as_storage_rd* rd,
		bool no_bin_data, const cf_vector* select_bins, bool send_bval,
		int64_t bval, bool include_error_msg)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;
	// This builder only ever emits a successful record read (result_code is
	// always AS_OK), so error details never belong on it - is_error is false.
	error_msg_field err_field = error_msg_field_prep(include_error_msg, false);

	size_t ns_len = strlen(ns->name);
	const char* set_name = as_index_get_set_name(r, ns);
	size_t set_name_len = set_name ? strlen(set_name) : 0;

	const uint8_t* key = NULL;
	uint32_t key_size = 0;

	if (r->key_stored == 1) {
		if (! as_storage_rd_load_key(rd)) {
			cf_warning(AS_PROTO, "can't get key - skipping record");
			as_error_msg_clear();
			return -1;
		}

		key = rd->key;
		key_size = rd->key_size;
	}

	uint16_t n_fields = 2; // always add namespace and digest
	size_t msg_sz = sizeof(as_msg) + sizeof(as_msg_field) + ns_len +
			sizeof(as_msg_field) + sizeof(cf_digest);

	if (set_name) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + set_name_len;
	}

	if (key) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + key_size;
	}

	if (send_bval) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(bval);
	}

	if (err_field.add) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + err_field.len;
	}

	uint32_t n_select_bins = 0;
	uint16_t n_bins_returned = 0;

	if (! no_bin_data) {
		if (select_bins) {
			n_select_bins = cf_vector_size(select_bins);

			for (uint32_t i = 0; i < n_select_bins; i++) {
				const char* bin_name =
						(const char*)cf_vector_getp((cf_vector*)select_bins, i);

				as_bin* b = as_bin_get_live(rd, bin_name);
				as_bin rb;

				if (! b) {
					continue;
				}

				if (as_masking_apply(rd->mask_ctx, &rb, b)) {
					b = &rb;
				}

				msg_sz += sizeof(as_msg_op);
				msg_sz += strlen(bin_name);
				msg_sz += as_bin_particle_client_value_size(b);

				if (b == &rb) {
					as_bin_particle_destroy(&rb);
				}

				n_bins_returned++;
			}

			// Don't return an empty record.
			if (n_bins_returned == 0) {
				as_error_msg_clear();
				return 0;
			}
		}
		else {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin* b = &rd->bins[i];
				as_bin rb;

				if (as_bin_is_tombstone(b)) {
					continue;
				}

				if (as_masking_apply(rd->mask_ctx, &rb, b)) {
					b = &rb;
				}

				msg_sz += sizeof(as_msg_op);
				msg_sz += strlen(b->name);
				msg_sz += as_bin_particle_client_value_size(b);

				if (b == &rb) {
					as_bin_particle_destroy(&rb);
				}

				n_bins_returned++;
			}
		}
	}

	uint8_t* buf;

	cf_buf_builder_reserve(bb_r, (int)msg_sz, &buf);

	as_msg* m = (as_msg*)buf;

	m->header_sz = sizeof(as_msg);
	m->info1 = no_bin_data ? AS_MSG_INFO1_GET_NO_BINS : 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = AS_OK;
	m->generation = plain_generation(r->generation, ns);
	m->record_ttl = r->void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;

	if (no_bin_data) {
		m->n_ops = 0;
	}
	else {
		m->n_ops = n_bins_returned;
	}

	as_msg_swap_header(m);

	buf = m->data;

	as_msg_field* mf = (as_msg_field*)buf;

	mf->field_sz = ns_len + 1;
	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, ns->name, ns_len);
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + ns_len;

	mf = (as_msg_field*)buf;
	mf->field_sz = sizeof(cf_digest) + 1;
	mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
	memcpy(mf->data, &r->keyd, sizeof(cf_digest));
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + sizeof(cf_digest);

	if (set_name) {
		mf = (as_msg_field*)buf;
		mf->field_sz = set_name_len + 1;
		mf->type = AS_MSG_FIELD_TYPE_SET;
		memcpy(mf->data, set_name, set_name_len);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + set_name_len;
	}

	if (key) {
		mf = (as_msg_field*)buf;
		mf->field_sz = key_size + 1;
		mf->type = AS_MSG_FIELD_TYPE_KEY;
		memcpy(mf->data, key, key_size);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + key_size;
	}

	if (send_bval) {
		mf = (as_msg_field*)buf;
		mf->field_sz = sizeof(bval) + 1;
		mf->type = AS_MSG_FIELD_TYPE_BVAL_ARRAY;
		*(uint64_t*)mf->data = cf_swap_to_le64((uint64_t)bval);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(bval);
	}

	error_msg_field_write(&buf, &err_field);

	if (no_bin_data) {
		return (int32_t)msg_sz;
	}

	if (select_bins) {
		for (uint32_t i = 0; i < n_select_bins; i++) {
			const char* bin_name =
					(const char*)cf_vector_getp((cf_vector*)select_bins, i);

			as_bin* b = as_bin_get_live(rd, bin_name);
			as_bin rb;

			if (! b) {
				continue;
			}

			if (as_masking_apply(rd->mask_ctx, &rb, b)) {
				b = &rb;
			}

			as_msg_op* op = (as_msg_op*)buf;

			op->op = AS_MSG_OP_READ;
			op->has_lut = 0;
			op->unused_flags = 0;
			op->name_sz = as_bin_memcpy_name(op->name, b);
			op->op_sz = OP_FIXED_SZ + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;
			buf += as_bin_particle_to_client(b, op);

			as_msg_swap_op(op);

			if (b == &rb) {
				as_bin_particle_destroy(&rb);
			}
		}
	}
	else {
		for (uint16_t i = 0; i < rd->n_bins; i++) {
			as_bin* b = &rd->bins[i];
			as_bin rb;

			if (as_bin_is_tombstone(b)) {
				continue;
			}

			if (as_masking_apply(rd->mask_ctx, &rb, b)) {
				b = &rb;
			}

			as_msg_op* op = (as_msg_op*)buf;

			op->op = AS_MSG_OP_READ;
			op->has_lut = 0;
			op->unused_flags = 0;
			op->name_sz = as_bin_memcpy_name(op->name, b);
			op->op_sz = OP_FIXED_SZ + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;
			buf += as_bin_particle_to_client(b, op);

			as_msg_swap_op(op);

			if (b == &rb) {
				as_bin_particle_destroy(&rb);
			}
		}
	}

	return (int32_t)msg_sz;
}

void
as_msg_pid_done_bufbuilder(cf_buf_builder** bb_r, uint32_t pid, int result)
{
	uint8_t* buf;

	cf_buf_builder_reserve(bb_r, (int)sizeof(as_msg), &buf);

	as_msg* m = (as_msg*)buf;

	*m = (as_msg){
		.header_sz = sizeof(as_msg),
		.info3 = AS_MSG_INFO3_PARTITION_DONE,
		.result_code = result,
		.generation = pid // HACK - more efficient than separate field
	};

	as_msg_swap_header(m);
}

void
as_msg_fin_bufbuilder(cf_buf_builder** bb_r, int result)
{
	uint8_t* buf;

	cf_buf_builder_reserve(bb_r, (int)sizeof(as_msg), &buf);

	as_msg* m = (as_msg*)buf;

	*m = (as_msg){ .header_sz = sizeof(as_msg),
		.info3 = AS_MSG_INFO3_LAST,
		.result_code = result };

	as_msg_swap_header(m);
}

cl_msg*
as_msg_make_no_val_response(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_record_version* v, size_t* p_msg_sz,
		bool include_error_msg)
{
	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);
	error_msg_field err_field =
			error_msg_field_prep(include_error_msg, result_code != AS_OK);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (err_field.add) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + err_field.len;
	}

	uint8_t* buf = cf_malloc(msg_sz);
	cl_msg* msgp = (cl_msg*)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg* m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation;
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = 0;

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field* mf = (as_msg_field*)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version*)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	error_msg_field_write(&buf, &err_field);

	*p_msg_sz = msg_sz;

	return msgp;
}

cl_msg*
as_msg_make_val_response(bool success, const as_val* val, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_record_version* v,
		size_t* p_msg_sz, bool include_error_msg)
{
	const char* bin_name;
	size_t bin_name_len;
	// A FAILURE bin (success false) is the error response; a SUCCESS bin must
	// not carry error details (e.g. a UDF that swallows a sub-error then
	// returns success).
	error_msg_field err_field =
			error_msg_field_prep(include_error_msg, ! success);

	if (success) {
		bin_name = SUCCESS_BIN_NAME;
		bin_name_len = sizeof(SUCCESS_BIN_NAME) - 1;
	}
	else {
		bin_name = FAILURE_BIN_NAME;
		bin_name_len = sizeof(FAILURE_BIN_NAME) - 1;
	}

	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (err_field.add) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + err_field.len;
	}

	msg_sz += sizeof(as_msg_op) + bin_name_len +
			as_particle_asval_client_value_size(val);

	uint8_t* buf = cf_malloc(msg_sz);
	cl_msg* msgp = (cl_msg*)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg* m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation;
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = 1; // only the one special bin

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field* mf = (as_msg_field*)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version*)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	error_msg_field_write(&buf, &err_field);

	as_msg_op* op = (as_msg_op*)buf;

	op->op = AS_MSG_OP_READ;
	op->name_sz = (uint8_t)bin_name_len;
	memcpy(op->name, bin_name, op->name_sz);
	op->op_sz = OP_FIXED_SZ + op->name_sz;
	op->has_lut = 0;
	op->unused_flags = 0;

	as_particle_asval_to_client(val, op);

	as_msg_swap_op(op);

	*p_msg_sz = msg_sz;

	return msgp;
}

// Caller-provided val_sz must be the result of calling
// as_particle_asval_client_value_size() for same val.
void
as_msg_make_val_response_bufbuilder(const as_val* val, cf_buf_builder** bb_r,
		uint32_t val_sz, bool success, bool include_error_msg)
{
	const char* bin_name;
	size_t bin_name_len;
	// A FAILURE bin (success false) is the error response; a SUCCESS bin must
	// not carry error details.
	error_msg_field err_field =
			error_msg_field_prep(include_error_msg, ! success);

	if (success) {
		bin_name = SUCCESS_BIN_NAME;
		bin_name_len = sizeof(SUCCESS_BIN_NAME) - 1;
	}
	else {
		bin_name = FAILURE_BIN_NAME;
		bin_name_len = sizeof(FAILURE_BIN_NAME) - 1;
	}

	size_t msg_sz = sizeof(as_msg) + sizeof(as_msg_op) + bin_name_len + val_sz;

	if (err_field.add) {
		msg_sz += sizeof(as_msg_field) + err_field.len;
	}

	uint8_t* buf;

	cf_buf_builder_reserve(bb_r, (int)msg_sz, &buf);

	as_msg* m = (as_msg*)buf;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = AS_OK;
	m->generation = 0;
	m->record_ttl = 0;
	m->transaction_ttl = 0;
	m->n_fields = err_field.add ? 1 : 0;
	m->n_ops = 1; // only the one special bin

	as_msg_swap_header(m);

	uint8_t* dbuf = m->data;

	error_msg_field_write(&dbuf, &err_field);

	as_msg_op* op = (as_msg_op*)dbuf;

	op->op = AS_MSG_OP_READ;
	op->name_sz = (uint8_t)bin_name_len;
	memcpy(op->name, bin_name, op->name_sz);
	op->op_sz = OP_FIXED_SZ + op->name_sz;
	op->has_lut = 0;
	op->unused_flags = 0;

	as_particle_asval_to_client(val, op);

	as_msg_swap_op(op);
}

//==========================================================
// Public API - sending responses to client.
//

// Make an individual transaction response and send it.
int
as_msg_send_reply(as_file_handle* fd_h, uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op** ops, as_bin** bins, uint16_t bin_count,
		as_namespace* ns, as_record_version* v, bool include_error_msg)
{
	uint8_t stack_buf[MSG_STACK_BUFFER_SZ];
	size_t msg_sz = sizeof(stack_buf);
	uint8_t* msgp = (uint8_t*)as_msg_make_response_msg(result_code, generation,
			void_time, ops, bins, bin_count, ns, (cl_msg*)stack_buf, &msg_sz, v,
			0, include_error_msg);

	int rv = send_reply_buf(fd_h, msgp, msg_sz);

	if (msgp != stack_buf) {
		cf_free(msgp);
	}

	return rv;
}

// Send a pre-made response saved in a dyn-buf.
int
as_msg_send_ops_reply(as_file_handle* fd_h, const cf_dyn_buf* db, bool compress,
		as_proto_comp_stat* comp_stat)
{
	if (! compress) {
		return send_reply_buf(fd_h, db->buf, db->used_sz);
	}

	size_t msg_sz = db->used_sz;
	const uint8_t* msgp = as_proto_compress(db->buf, &msg_sz, comp_stat);

	return send_reply_buf(fd_h, msgp, msg_sz);
}

// Send a blocking "fin" message with default timeout.
bool
as_msg_send_fin(cf_socket* sock, uint32_t result_code)
{
	return as_msg_send_fin_timeout(sock, result_code, CF_SOCKET_TIMEOUT) != 0;
}

// Send a blocking "fin" message with a specified timeout.
size_t
as_msg_send_fin_timeout(cf_socket* sock, uint32_t result_code, int32_t timeout)
{
	cl_msg msgp;

	msgp.proto.version = PROTO_VERSION;
	msgp.proto.type = PROTO_TYPE_AS_MSG;
	msgp.proto.sz = sizeof(as_msg);

	as_proto_swap(&msgp.proto);

	as_msg* m = &msgp.msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = AS_MSG_INFO3_LAST;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = 0;
	m->record_ttl = 0;
	m->transaction_ttl = 0;
	m->n_fields = 0;
	m->n_ops = 0;

	as_msg_swap_header(m);

	if (cf_socket_send_all(sock, (uint8_t*)&msgp, sizeof(msgp), MSG_NOSIGNAL,
				timeout) < 0) {
		cf_warning(AS_PROTO, "send error - fd %d %s", CSFD(sock),
				cf_strerror(errno));
		return 0;
	}

	return sizeof(cl_msg);
}

//==========================================================
// Local helpers.
//

static int
send_reply_buf(as_file_handle* fd_h, const uint8_t* msgp, size_t msg_sz)
{
	cf_assert(cf_socket_exists(&fd_h->sock), AS_PROTO, "fd is invalid");

	if (cf_socket_send_all(&fd_h->sock, msgp, msg_sz, MSG_NOSIGNAL,
				CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_PROTO, "protocol write fail: fd %d sz %zu errno %d",
				CSFD(&fd_h->sock), msg_sz, errno);

		as_end_of_transaction_force_close(fd_h);
		return -1;
	}

	as_end_of_transaction_ok(fd_h);
	return 0;
}
