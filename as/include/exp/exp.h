/*
 * exp.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "dynbuf.h"
#include "msgpack_in.h"
#include "vector.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "exp/exp_wire.h"

//==========================================================
// Typedefs & constants.
//

#define AS_EXP_HAS_DIGEST_MOD (1 << 0)
#define AS_EXP_HAS_NON_DIGEST_META (1 << 1)
#define AS_EXP_HAS_REC_KEY (1 << 2)

typedef struct as_exp_s {
	uint8_t expected_type;
	uint8_t flags;
	const void* bin_table;
	// AEL op-ordinal -> source-span map (ael_src_map, exp.c) laid out in mem[]
	// alongside the retained source, so runtime error-detail traces can render
	// the true AEL source slice. NULL for wire (msgpack) expressions - also the
	// runtime's "was this compiled from AEL" discriminator.
	const void* ael_map;
	void** cleanup_stack;
	uint32_t cleanup_stack_ix;
	uint32_t max_var_count;
	uint8_t mem[];
} as_exp;

struct exp_runtime_s;
struct exp_rt_value_s;
struct exp_op_base_mem_s;
struct exp_op_value_geo_s;
struct exp_build_args_s;

typedef bool (*exp_op_table_build_cb)(struct exp_build_args_s* args);
typedef void (*exp_op_table_eval_cb)(struct exp_runtime_s* rt,
		const struct exp_op_base_mem_s* ob, struct exp_rt_value_s* ret_val);
typedef void (*exp_op_table_display_cb)(struct exp_runtime_s* rt,
		const struct exp_op_base_mem_s* ob, cf_dyn_buf* db);

typedef struct exp_op_table_entry_s {
	exp_op_code code;
	uint32_t size;
	exp_op_table_build_cb build_cb;
	exp_op_table_eval_cb eval_cb;
	exp_op_table_display_cb display_cb;
	uint32_t static_param_count;
	uint32_t eval_param_count;
	exp_rtype r_type;
	const char* name;
} exp_op_table_entry;

typedef struct as_exp_ctx_s {
	as_namespace* ns;
	as_record* r;
	as_storage_rd* rd; // NULL during metadata phase

	// The client transaction this eval runs on behalf of, for the disclosure
	// gate on the value explainer (see as_exp_explain_value). NULL - the
	// default - on every internal eval (sindex maintenance, query record
	// bodies, XDR filters), which fails the gate closed.
	const struct as_transaction_s* tr;

	msgpack_in** vars_table;
} as_exp_ctx;

typedef enum {
	AS_EXP_FALSE = 0,
	AS_EXP_TRUE = 1,
	AS_EXP_UNK = 2,
	// A genuine evaluation fault (e.g. integer divide-by-zero,
	// corrupt stored key). Rides the existing UNK short-circuit channel; the
	// boundary reports it (status unchanged + eval-phase trace, outcome=fault)
	// rather than silently filtering. ABSENT/DEFER stay AS_EXP_UNK.
	AS_EXP_ERROR = 3
} __attribute__((__packed__)) as_exp_trilean;

typedef enum {
	AS_EXP_RESULT_MP_SMALL,
	AS_EXP_RESULT_MSGPACK,
	AS_EXP_RESULT_STR,
	AS_EXP_RESULT_BIN,
	AS_EXP_RESULT_REMOVE
} __attribute__((__packed__)) exp_result_type;

typedef struct as_exp_result_s {
	union {
		exp_result_type type; // which variant below is active

		struct { // mp_small_s
			uint16_t pad;
			uint16_t sz;
			uint8_t buf[1 + sizeof(uint64_t)];
		} __attribute__((__packed__)) mp_small;

		struct { // msgpack_s
			uint16_t pad;
			uint16_t has_nonstorage;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__((__packed__)) msgpack;

		struct { // str_s
			uint8_t pad[3];
			uint8_t bytes_type;
			uint32_t sz;
			const uint8_t* ptr;
		} __attribute__((__packed__)) str;

		struct { // particle_s
			uint64_t pad;
			as_particle* ptr;
		} __attribute__((__packed__)) particle;
	};
} __attribute__((__packed__)) as_exp_result;

extern const exp_op_table_entry exp_op_table[];

// Build/compile helpers defined in exp.c, also called from exp_rt.c (eval).
const char* exp_rtype_to_str(exp_rtype type);

// Parse a GeoJSON literal from an expression's msgpack into a compiled geo op.
// pre:  mp is positioned at the value bin (as_bytes blob: a type byte then
//       GeoJSON text); op is the target node; debug_str labels warnings.
// post: op->contents/content_sz span the raw bin and mp advances past it;
//       op->compiled is a GEO_CELL point or GEO_REGION (region owned by the
//       node). Returns false, with a warning, on a non-bin or bad GeoJSON.
bool exp_geo_mp_to_op(msgpack_in* mp, struct exp_op_value_geo_s* op,
		const char* debug_str);

// Rendered build-failure detail handed to an in-scope caller so it can stage
// a field-45 expression trace. The op name, ancestor path, and snippet are all
// pre-rendered here so callers (and proto.c) need no access to the exp.c op
// table. Carries the byte offset and failing op name, the depth +
// ancestor path root -> fault, and the human-only snippet.
#define AS_EXP_BUILD_ERROR_OP_MAX AS_EXP_TRACE_OP_MAX

// AEL compile diagnostics are short fixed strings; budget generously.
#define AS_EXP_BUILD_ERROR_MSG_MAX 128

typedef struct as_exp_build_error_s {
	uint8_t phase; // always AS_EXP_TRACE_PHASE_BUILD here
	bool has_offset;
	uint32_t byte_offset;
	bool has_op;
	uint16_t op_len; // length of op (no NUL)
	char op[AS_EXP_BUILD_ERROR_OP_MAX]; // NUL-terminated
	// Depth + ancestor path (root -> failing op).
	bool has_path;
	uint16_t depth;
	bool path_truncated;
	uint16_t n_frames;
	char path[AS_EXP_TRACE_MAX_FRAMES][AS_EXP_TRACE_OP_MAX]; // NUL-term
	// Human-only snippet - rendered from the msgpack payload for a wire build,
	// or the true source slice for an AEL build.
	bool has_snippet;
	uint16_t snippet_len; // length of snippet (no NUL)
	char snippet[AS_EXP_TRACE_SNIPPET_MAX]; // NUL-terminated
	// AEL build failure (lang == AS_EXP_TRACE_LANG_AEL; 0 for a wire build):
	// position + span into the AEL source text (never the msgpack byte_offset
	// above - the two coordinate spaces are distinct) and the compile
	// diagnostic, which the caller folds into its detailed message.
	uint8_t lang;
	bool has_ael_offset;
	uint32_t ael_offset;
	bool has_ael_span;
	uint32_t ael_span;
	bool has_msg;
	char msg[AS_EXP_BUILD_ERROR_MSG_MAX]; // NUL-terminated diagnostic
} as_exp_build_error;

// as_error_exp_trace_set_from_build() memcpy's the op/path/snippet buffers from
// this struct into proto.h's as_error_exp_trace by sizeof, so the two must keep
// identical buffer shapes. Enforce it at compile time rather than by comment.
COMPILER_ASSERT(sizeof(((as_exp_build_error*)0)->op) ==
		sizeof(((as_error_exp_trace*)0)->op));
COMPILER_ASSERT(sizeof(((as_exp_build_error*)0)->path) ==
		sizeof(((as_error_exp_trace*)0)->path));
COMPILER_ASSERT(sizeof(((as_exp_build_error*)0)->snippet) ==
		sizeof(((as_error_exp_trace*)0)->snippet));

//==========================================================
// Public API.
//

// Drain the thread-local build-failure accumulator into 'out', rendering the
// failing op name. Returns false (and leaves 'out' untouched) when nothing was
// captured. Clears the accumulator either way it had content.
bool as_exp_take_build_error(as_exp_build_error* out);

// Reset the build-failure accumulator on a build path that does NOT take (see
// exp.c). Taking paths clear via as_exp_take_build_error().
void as_exp_build_err_reset(void);

// Stage a rendered build error as a field-45 expression trace. Verbosity
// gating and first-set-wins live in as_error_exp_trace_set(), so this is a
// cheap no-op off the top tier. Callers pair it with their existing contextual
// as_error_details_set_fmt() message.
static inline void
as_error_exp_trace_set_from_build(const as_exp_build_error* be)
{
	as_error_exp_trace t = {
		.phase = be->phase,
		.has_offset = be->has_offset,
		.byte_offset = be->byte_offset,
		.has_op = be->has_op,
		.op_len = be->op_len,
		.has_path = be->has_path,
		.depth = be->depth,
		.path_truncated = be->path_truncated,
		.n_frames = be->n_frames,
		.has_snippet = be->has_snippet,
		.snippet_len = be->snippet_len,
		// A wire (msgpack) build leaves lang 0 - the wire default when the
		// lang key is absent - so it spends no extra bytes. An AEL build
		// carries lang=AEL plus the source-text offset/span keys.
		.has_lang = be->lang != 0,
		.lang = be->lang,
		.has_ael_offset = be->has_ael_offset,
		.ael_offset = be->ael_offset,
		.has_ael_span = be->has_ael_span,
		.ael_span = be->ael_span,
	};

	if (be->has_op) {
		// op buffers are the same fixed width in both structs.
		memcpy(t.op, be->op, sizeof(t.op));
	}

	if (be->has_path) {
		// path buffers are the same fixed shape in both structs.
		memcpy(t.path, be->path, sizeof(t.path));
	}

	if (be->has_snippet) {
		memcpy(t.snippet, be->snippet, sizeof(t.snippet));
	}

	as_error_exp_trace_set(&t);
}

// Drain a pending build failure into the transaction's error details: stage
// the field-45 trace, then author the caller-context message - folding in the
// compile diagnostic when the failed build produced one (AEL). Emits the bare
// context message when nothing was captured. The trace must be staged before
// the message because field-45 assembly is eager; this helper owns that
// ordering so callers can't get it wrong.
void as_exp_stage_build_error_details(const char* context_msg);

as_exp* as_exp_filter_build_base64(const char* buf64, uint32_t buf64_sz);
as_exp* as_exp_filter_build_ael(const uint8_t* ael, uint32_t ael_sz);
as_exp* as_exp_filter_build(const as_msg_field* msg, bool cpy_instr);
as_exp* as_exp_build_buf(const uint8_t* buf, uint32_t buf_sz, bool cpy_wire,
		cf_vector* bin_names_r);
// Tri-state result so a caller can tell a genuine eval fault
// (AS_EXP_ERROR - trace + message already staged) from a clean non-match
// (AS_EXP_UNK - was 'false'); AS_EXP_TRUE means a bin value was produced (was
// 'true'). Callers that only care "did it produce a value" treat anything other
// than AS_EXP_TRUE as their existing failure path.
as_exp_trilean as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb,
		cf_ll_buf* particles_llb);
// Metadata (pre-bin) phase filter match. Returns AS_EXP_TRUE / AS_EXP_FALSE /
// AS_EXP_UNK (defer to the record phase). NEVER returns AS_EXP_ERROR - a
// metadata-phase eval fault is deferred as AS_EXP_UNK (a bin-dependent
// sibling may still determine the result once bins load).
as_exp_trilean as_exp_matches_metadata(const as_exp* predexp,
		const as_exp_ctx* ctx);
bool as_exp_matches_record(const as_exp* predexp, const as_exp_ctx* ctx);
// Explain a clean single-record filter non-match by staging an eval-phase
// trace (outcome = false / absent) naming the deciding sub-expression. pre:
// record phase, after as_exp_matches_record() returned false, and the caller
// has established that the principal may read the record - everything staged
// here (outcome, decisive op, path, operand values) is a function of stored
// data. A no-op below the trace tier.
void as_exp_explain_filter(const as_exp* predexp, const as_exp_ctx* ctx);
// Value-read counterpart of as_exp_explain_filter: explain an expression-op
// that produced no value (as_exp_eval returned AS_EXP_UNK, mapped to
// AS_ERR_OP_NOT_APPLICABLE). Stages an eval-phase ABSENT trace naming the
// absent / wrong-type reference. No operand values, but which reference went
// absent is still stored-data-derived, so the caller must have established
// read permission - same rule as the filter explainer. Opt-in, top tier only;
// call from the expop boundary after as_exp_eval returned AS_EXP_UNK. A no-op
// below the trace tier.
void as_exp_explain_value(const as_exp* exp, const as_exp_ctx* ctx);
bool as_exp_display(const as_exp* exp, cf_dyn_buf* db);
void as_exp_destroy(as_exp* exp);

uint32_t as_exp_result_msgpack_sz(const as_exp_result* res);
void as_exp_result_msgpack_write(const as_exp_result* res, uint8_t* wptr);
void as_exp_result_msgpack_pack(const as_exp_result* res, as_packer* pk);
bool as_exp_result_has_nonstorage(const as_exp_result* res);

as_exp_trilean as_exp_eval_to_result(const as_exp* exp, const as_exp_ctx* ctx,
		as_exp_result* res);
void as_exp_result_destroy(as_exp_result* res);

static inline bool
as_exp_result_is_remove(const as_exp_result* res)
{
	return res->type == AS_EXP_RESULT_REMOVE;
}
