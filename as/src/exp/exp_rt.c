/*
 * exp_rt.c
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

#include "exp/exp_rt.h"

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <regex.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "bits.h"
#include "cf_defer.h"
#include "cf_str.h"
#include "dynbuf.h"
#include "enhanced_alloc.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/masking.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"
#include "base/thr_info.h"
#include "exp/exp.h"
#include "exp/exp_wire.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"

//==========================================================
// Type aliases.
//

typedef struct exp_op_base_mem_s op_base_mem;
typedef struct exp_op_cmp_regex_s op_cmp_regex;
typedef struct exp_op_meta_digest_modulo_s op_meta_digest_modulo;
typedef struct exp_op_cond_s op_cond;
typedef struct exp_op_var_s op_var;
typedef struct exp_op_let_s op_let;
typedef struct exp_op_vec_s op_vec;
typedef struct exp_op_call_s op_call;
typedef struct exp_geo_compiled_s geo_compiled;
typedef struct exp_op_value_geo_s op_value_geo;
typedef struct exp_op_value_bool_s op_value_bool;
typedef struct exp_op_value_blob_s op_value_blob;
typedef struct exp_op_value_int_s op_value_int;
typedef struct exp_op_value_float_s op_value_float;
typedef struct exp_rt_value_s rt_value;
typedef struct exp_runtime_s runtime;
typedef struct exp_explain_state_s explain_state;

// op_table lives in exp.c; reached here via the exp.h extern.
typedef exp_op_table_entry op_table_entry;
#define op_table exp_op_table

//==========================================================
// Eval-local types.
//

typedef struct {
	uint64_t cellid;
	geo_region_t region;
} geo_data;

typedef struct {
	as_bin** bin;
	uint32_t bin_ix;
} call_cleanup;

#define define_call_cleanup(_name, _count)                                     \
	define_deferred_array(_name##_bin, as_bin*, _count);                       \
	DEFER_ATTR(call_cleanup_fn)                                                \
	call_cleanup _name = { .bin = _name##_bin, .bin_ix = 0 }

#define call_cleanup_add(_name, _bin) _name.bin[_name.bin_ix++] = _bin;

#define defer_rt_value_destroy(_x)                                             \
	DEFER_ATTR(rt_defer_value_destroy)                                         \
	rt_value* DEFER_GLUE(_defer_rt_value_destroy, __LINE__) = &(_x)

//==========================================================
// Module constants.
//

static const uint8_t* EMPTY_STRING = (uint8_t*)"";
static const rt_value rt_unk = { .type = RT_TRILEAN, .r_trilean = AS_EXP_UNK };

const uint8_t exp_call_eval_token[1] = "";

// "Short-circuit" = UNK or ERROR - every op that propagates UNK propagates
// a fault the same way. Leave rt_value_is_unk() meaning UNK only, so a fault
// is never mistaken for an absent/deferred operand.
static inline bool
rt_value_is_short_circuit(const rt_value* entry)
{
	return entry->type == RT_TRILEAN &&
			(entry->r_trilean == AS_EXP_UNK || entry->r_trilean == AS_EXP_ERROR);
}

// Why a genuine eval fault occurred - drives the human message composed at
// the boundary (exp_err_reason_msg). Values are internal, never on the wire.
typedef enum {
	EXP_ERR_NONE = 0,
	EXP_ERR_DIV_ZERO, // integer division by zero
	EXP_ERR_DIV_OVERFLOW, // INT64_MIN / -1
	EXP_ERR_MOD_ZERO, // integer modulo by zero
	EXP_ERR_MOD_OVERFLOW, // INT64_MIN % -1
	EXP_ERR_REC_KEY, // corrupt stored record key
	EXP_ERR_REGEX_OPERAND, // regex operand is not a string
	EXP_ERR_NOT_A_LIST, // operand is not a list
	EXP_ERR_NOT_A_MAP, // operand is not a map
	EXP_ERR_BAD_MSGPACK, // malformed / untranslatable stored msgpack
	EXP_ERR_CALL_ARG, // invalid argument to a CDT/bits/HLL sub-op
	EXP_ERR_GEOJSON, // invalid geojson
	EXP_ERR_PRESERVE_ORDER_MAP // operand's element order is not key order
} exp_err_reason;

// Stamp an eval fault into the trilean rt_value. 'op_ix' is the FAILING op's
// own index - capture it as (rt->op_ix - 1) at eval_cb entry, because by
// fault-detection time rt->op_ix has advanced past the op's operands. The
// boundary reads err_op_ix/err_reason to reconstruct the trace and message.
static inline void
rt_set_error(rt_value* ret_val, uint32_t op_ix, exp_err_reason reason)
{
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = AS_EXP_ERROR;
	ret_val->err_op_ix = op_ix;
	ret_val->err_reason = (uint16_t)reason;
}

// In the explainer's re-eval (rt->explain set), stamp the decisive op's index
// onto a non-ERROR trilean result, riding the same err_op_ix slot the fault
// path uses. Used by the absent (UNK) sites; FALSE results are stamped
// centrally in rt_eval. A no-op on the hot path.
static inline void
rt_mark_decisive(const runtime* rt, rt_value* ret_val, uint32_t op_ix)
{
	if (rt->explain != NULL) {
		ret_val->err_op_ix = op_ix;
	}
}

// Render a comparison operand as a short human string for the explainer -
// scalars in full, collections/geo as a type placeholder. Truncates to 'cap'.
// Returns the written length (no NUL).
static uint16_t
rt_value_render(const rt_value* v, char* buf, uint32_t cap)
{
	int n;

	switch (v->type) {
	case RT_NIL:
		n = snprintf(buf, cap, "nil");
		break;
	case RT_INT:
		n = snprintf(buf, cap, "%ld", (long)v->r_int);
		break;
	case RT_FLOAT:
		n = snprintf(buf, cap, "%g", v->r_float);
		break;
	case RT_TRILEAN: {
		const char* s = "unknown";

		if (v->r_trilean == AS_EXP_TRUE) {
			s = "true";
		}
		else if (v->r_trilean == AS_EXP_FALSE) {
			s = "false";
		}

		n = snprintf(buf, cap, "%s", s);
		break;
	}
	case RT_STR: {
		// These are stored bin bytes, and they are packed as a msgpack str -
		// so they must actually be text. A string bin is only warned about,
		// not rejected, on the write path (string_from_wire), so the stored
		// value can be arbitrary bytes; and truncating at a byte count can
		// split a multi-byte code point in an otherwise valid value. Render a
		// placeholder rather than ship either past a client's str decoder.
		uint32_t sz = v->r_bytes.sz;

		if (sz > cap - 1) {
			sz = cap - 1;

			// Back off to a code-point boundary - never leave a partial
			// sequence at the end of the window.
			while (sz != 0 && (v->r_bytes.contents[sz] & 0xc0) == 0x80) {
				sz--;
			}
		}

		// sz == 0 here means the whole window was continuation bytes, and
		// cf_str_is_valid_utf8(_, 0) is vacuously true - so this must be
		// checked before the validity test, not folded into it.
		if (sz == 0 || ! cf_str_is_valid_utf8(v->r_bytes.contents, sz)) {
			n = snprintf(buf, cap, "<string#%u>", v->r_bytes.sz);
			break;
		}

		// Flatten control characters - they would otherwise ride out inside
		// the str and break line-oriented consumers of the message.
		for (uint32_t i = 0; i < sz; i++) {
			uint8_t c = v->r_bytes.contents[i];

			buf[i] = (c < 0x20 || c == 0x7f) ? ' ' : (char)c;
		}

		buf[sz] = '\0';
		return (uint16_t)sz;
	}
	case RT_BLOB:
		n = snprintf(buf, cap, "<blob>");
		break;
	case RT_GEO_CONST:
	case RT_GEO_COMPILED:
	case RT_GEO_STR:
		n = snprintf(buf, cap, "<geojson>");
		break;
	case RT_MSGPACK:
	case RT_BIN:
	case RT_BIN_PTR:
		n = snprintf(buf, cap, "<collection>");
		break;
	default:
		n = snprintf(buf, cap, "<value>");
		break;
	}

	if (n < 0) {
		n = 0;
	}
	else if ((uint32_t)n >= cap) {
		n = (int)(cap - 1); // snprintf truncated
	}

	return (uint16_t)n;
}

// Forward-declared for as_exp_eval / as_exp_eval_to_result, defined above it.
static void stage_eval_fault(const as_exp* exp, uint32_t fault_ix,
		exp_err_reason reason);

//==========================================================
// Forward declarations.
//

// Runtime.
static as_exp_trilean match_internal(const as_exp* exp, const as_exp_ctx* ctx);
static bool rt_eval(runtime* rt, rt_value* ret_val);
// Runtime utilities.
static void rt_value_bin_ptr_to_bin(runtime* rt, as_bin* rb,
		const rt_value* from, cf_ll_buf* ll_buf);
static void json_to_rt_geo(const uint8_t* json, size_t jsonsz, rt_value* val);
static void particle_to_rt_geo(const as_particle* p, rt_value* val);
static bool bin_is_type(const as_bin* b, exp_rtype type);
static void rt_skip(runtime* rt, uint32_t instr_count);
static void rt_value_translate(rt_value* to, const rt_value* from);
static void rt_defer_value_destroy(rt_value** p_val);
static void rt_value_destroy(rt_value* val);
static void rt_value_get_geo(rt_value* val, geo_data* result);
static void rt_bin_translate(rt_value* bin, rt_value* ret_val);
static void rt_load_bin(runtime* rt, uint32_t idx, rt_value* ret_val);
static bool rt_is_type(const rt_value* v, exp_rtype type);
static as_particle_type rt_value_particle_type(const rt_value* v);
static void rt_release_bins(runtime* rt, rt_value* ret_val);
static void rt_init_builtin_vars(runtime* rt, op_value_geo* geo_mem);

// Runtime compare utilities.
static as_exp_trilean cmp_nil(exp_op_code code, const rt_value* e0,
		const rt_value* e1);
static as_exp_trilean cmp_trilean(exp_op_code code, const rt_value* e0,
		const rt_value* e1);
static as_exp_trilean cmp_int(exp_op_code code, const rt_value* e0,
		const rt_value* e1);
static as_exp_trilean cmp_float(exp_op_code code, const rt_value* e0,
		const rt_value* e1);
static const uint8_t* rt_value_get_str(const rt_value* val, uint32_t* sz_r);
static as_exp_trilean cmp_bytes(exp_op_code code, const rt_value* e0,
		const rt_value* e1);
static as_exp_trilean cmp_msgpack(exp_op_code code, const rt_value* v0,
		const rt_value* v1, bool* preserve_order);

// Runtime call utilities.
static void call_cleanup_fn(call_cleanup* cc);
static void pack_typed_str(as_packer* pk, const uint8_t* buf, uint32_t sz,
		uint8_t type);
static bool rt_value_to_msgpack_vec(as_packer* pk, msgpack_vec* vec,
		rollback_alloc* alloc, const rt_value* from);
static bool rt_value_bin_translate(rt_value* to, const rt_value* from);
static void* rt_alloc_mem(runtime* rt, size_t sz, cf_ll_buf* ll_buf);
static bool msgpack_to_bin(runtime* rt, as_bin* to, rt_value* from,
		cf_ll_buf* ll_buf);

static void display_msgpack(msgpack_in* mp, cf_dyn_buf* db);

//==========================================================
// Public API.
//

// Tri-state so the caller can tell a fault from a plain non-match:
//   AS_EXP_TRUE  - produced a bin value
//   AS_EXP_UNK   - clean non-match, no detail staged
//   AS_EXP_ERROR - eval fault; stages the eval-phase trace + message (the
//                  caller's status is unchanged)
// A materialization fault here (bad geojson / msgpack / unexpected type) has
// no sub-op index to point at, so it reconstructs from the root (op_ix 0).
as_exp_trilean
as_exp_eval(const as_exp* exp, const as_exp_ctx* ctx, as_bin* rb,
		cf_ll_buf* particles_llb)
{
	define_deferred_array(vars, rt_value, exp->max_var_count);
	rt_value ret_val;

	runtime rt = {
		.ctx = ctx,
		.instr_ptr = exp->mem,
		.vars = vars,
		.bin_table = exp->bin_table,
		.vars_builtin = { [0 ...(AS_EXP_BUILTIN_COUNT - 1)] = rt_unk },
	};

	for (uint32_t i = 0; i < exp->max_var_count; i++) {
		rt.vars[i] = (rt_value){ .type = RT_END };
	}

	rt_eval(&rt, &ret_val);
	rt_release_bins(&rt, &ret_val);

	if (ret_val.type == RT_BIN_PTR) {
		rt_value_bin_ptr_to_bin(&rt, rb, &ret_val, particles_llb);
		return AS_EXP_TRUE;
	}

	switch (ret_val.type) {
	case RT_NIL:
		as_bin_set_empty(rb);
		break;
	case RT_INT:
		rb->particle = (as_particle*)ret_val.r_int;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_INTEGER);
		break;
	case RT_FLOAT:
		*((double*)(&rb->particle)) = ret_val.r_float;
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_FLOAT);
		break;
	case RT_TRILEAN:
		if (ret_val.r_trilean == AS_EXP_ERROR) {
			// A fault propagated up to the top. ret_val carries the failing
			// op's index (err_op_ix) and reason (err_reason) - reconstruct.
			stage_eval_fault(exp, ret_val.err_op_ix,
					(exp_err_reason)ret_val.err_reason);
			return AS_EXP_ERROR;
		}

		if (ret_val.r_trilean == AS_EXP_UNK) {
			return AS_EXP_UNK;
		}

		rb->particle =
				(as_particle*)(uint64_t)(ret_val.r_trilean == AS_EXP_TRUE ? 1
																		  : 0);
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_BOOL);
		break;
	case RT_GEO_CONST:
		rb->particle = rt_alloc_mem(&rt,
				as_geojson_particle_sz(MAX_REGION_CELLS,
						ret_val.r_geo_const.op->content_sz),
				particles_llb); // over allocate
		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_GEOJSON);
		((cdt_mem*)rb->particle)->type = AS_PARTICLE_TYPE_GEOJSON;

		msgpack_in mp = { .buf = ret_val.r_geo_const.op->contents,
			.buf_sz = ret_val.r_geo_const.op->content_sz };

		uint32_t json_sz = 0;
		const char* json = (const char*)msgpack_get_bin(&mp, &json_sz);

		// Skip as_bytes type.
		json++;
		json_sz--;

		if (! as_geojson_to_particle(json, json_sz, &rb->particle)) {
			cf_warning(AS_EXP, "as_exp_eval - invalid geojson");

			if (particles_llb == NULL) {
				as_bin_particle_destroy(rb);
			}

			stage_eval_fault(exp, 0, EXP_ERR_GEOJSON);
			return AS_EXP_ERROR;
		}

		break;
	case RT_STR: {
		// Validate before alloc: on failure eval_op returns without restoring
		// rb->particle (see as_exp_modify_tr), so we must not leave a new
		// particle allocated without bin state set.
		if (! cf_str_is_valid_utf8(ret_val.r_bytes.contents, ret_val.r_bytes.sz)) {
			cf_ticker_warning(AS_EXP,
					"as_exp_eval - invalid UTF-8 detected in string data");
			stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
			return AS_EXP_ERROR;
		}

		rb->particle = rt_alloc_mem(&rt, sizeof(cdt_mem) + ret_val.r_bytes.sz,
				particles_llb);

		((cdt_mem*)rb->particle)->sz = ret_val.r_bytes.sz;
		((cdt_mem*)rb->particle)->type = (uint8_t)AS_PARTICLE_TYPE_STRING;
		memcpy(((cdt_mem*)rb->particle)->data, ret_val.r_bytes.contents,
				ret_val.r_bytes.sz);

		as_bin_state_set_from_type(rb, AS_PARTICLE_TYPE_STRING);
		break;
	}
	case RT_BLOB:
	case RT_HLL: {
		as_particle_type particle_type = ret_val.type == RT_BLOB
				? AS_PARTICLE_TYPE_BLOB
				: AS_PARTICLE_TYPE_HLL;

		rb->particle = rt_alloc_mem(&rt, sizeof(cdt_mem) + ret_val.r_bytes.sz,
				particles_llb);

		((cdt_mem*)rb->particle)->sz = ret_val.r_bytes.sz;
		((cdt_mem*)rb->particle)->type = (uint8_t)particle_type;
		memcpy(((cdt_mem*)rb->particle)->data, ret_val.r_bytes.contents,
				ret_val.r_bytes.sz);
		as_bin_state_set_from_type(rb, particle_type);
		break;
	}
	case RT_BIN:
		rb->state = ret_val.r_bin.state;

		if (particles_llb == NULL) {
			rb->particle = ret_val.r_bin.particle;
		}
		else {
			as_bin* b = &ret_val.r_bin;
			uint32_t sz = sizeof(cdt_mem) + ((cdt_mem*)b->particle)->sz;

			rb->particle = rt_alloc_mem(&rt, sz, particles_llb);
			memcpy(rb->particle, b->particle, sz);

			as_bin_particle_destroy(b);
		}

		break;
	case RT_MSGPACK:
		if (! msgpack_to_bin(&rt, rb, &ret_val, particles_llb)) {
			cf_warning(AS_EXP, "as_exp_eval - invalid msgpack");
			stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
			return AS_EXP_ERROR;
		}

		break;
	default:
		cf_warning(AS_EXP, "as_exp_eval - unexpected result type (%u)",
				ret_val.type);
		rt_value_destroy(&ret_val);
		stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
		return AS_EXP_ERROR;
	}

	return AS_EXP_TRUE;
}

uint32_t
as_exp_result_msgpack_sz(const as_exp_result* res)
{
	switch (res->type) {
	case AS_EXP_RESULT_BIN: {
		as_bin b = {
			.particle = res->particle.ptr,
		};

		as_bin_state_set_from_type(&b, res->particle.ptr->type);

		switch (as_bin_get_particle_type(&b)) {
		case AS_PARTICLE_TYPE_STRING:
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_HLL: {
			char* temp;

			uint32_t sz = as_bin_particle_string_ptr(&b, &temp);

			return as_pack_str_size(sz + 1);
		}
		case AS_PARTICLE_TYPE_GEOJSON: {
			size_t sz;

			as_geojson_mem_jsonstr(b.particle, &sz);

			return as_pack_str_size(sz + 1);
		}
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST: {
			cdt_payload packed;

			as_bin_particle_list_get_packed_val(&b, &packed);

			return packed.sz;
		}
		default:
			break;
		}

		return 0;
	}
	case AS_EXP_RESULT_MP_SMALL:
		return res->mp_small.sz;
	case AS_EXP_RESULT_MSGPACK:
		return res->msgpack.sz;
	case AS_EXP_RESULT_STR:
		return as_pack_str_size(res->str.sz + 1);
	default:
		break;
	}

	return 0;
}

void
as_exp_result_msgpack_write(const as_exp_result* res, uint8_t* wptr)
{
	as_packer pk = { .buffer = wptr, .capacity = UINT32_MAX };

	as_exp_result_msgpack_pack(res, &pk);
}

// The legacy type byte a msgpack str carries in its payload is the particle
// type, so the two enumerations must agree for everything reaching the writer.
COMPILER_ASSERT((int)AS_PARTICLE_TYPE_STRING == AS_BYTES_STRING);
COMPILER_ASSERT((int)AS_PARTICLE_TYPE_BLOB == AS_BYTES_BLOB);
COMPILER_ASSERT((int)AS_PARTICLE_TYPE_HLL == AS_BYTES_HLL);
COMPILER_ASSERT((int)AS_PARTICLE_TYPE_GEOJSON == AS_BYTES_GEOJSON);

void
as_exp_result_msgpack_pack(const as_exp_result* res, as_packer* pk)
{
	switch (res->type) {
	case AS_EXP_RESULT_BIN: {
		as_bin b = {
			.particle = res->particle.ptr,
		};

		as_bin_state_set_from_type(&b, res->particle.ptr->type);

		as_particle_type ptype = as_bin_get_particle_type(&b);

		switch (ptype) {
		case AS_PARTICLE_TYPE_STRING:
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_HLL: {
			char* ptr;
			uint32_t sz = as_bin_particle_string_ptr(&b, &ptr);

			as_pack_str_with_type(pk, (uint8_t)ptype, (uint8_t*)ptr, sz);
			break;
		}
		case AS_PARTICLE_TYPE_GEOJSON: {
			size_t sz;
			const char* str = as_geojson_mem_jsonstr(b.particle, &sz);

			as_pack_str_with_type(pk, (uint8_t)ptype, (uint8_t*)str, sz);
			break;
		}
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST: {
			cdt_payload packed;

			as_bin_particle_list_get_packed_val(&b, &packed);
			as_pack_append(pk, packed.ptr, packed.sz);
			break;
		}
		default:
			break;
		}

		break;
	}
	case AS_EXP_RESULT_MP_SMALL:
		as_pack_append(pk, res->mp_small.buf, res->mp_small.sz);
		break;
	case AS_EXP_RESULT_MSGPACK:
		as_pack_append(pk, res->msgpack.ptr, res->msgpack.sz);
		break;
	case AS_EXP_RESULT_STR:
		as_pack_str_with_type(pk, res->str.bytes_type, res->str.ptr, res->str.sz);
		break;
	case AS_EXP_RESULT_REMOVE:
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %d", res->type);
	}
}

bool
as_exp_result_has_nonstorage(const as_exp_result* res)
{
	return res->type == AS_EXP_RESULT_MSGPACK && res->msgpack.has_nonstorage != 0;
}

bool
as_exp_result_has_preserve_order_map(const as_exp_result* res)
{
	if (res->type == AS_EXP_RESULT_MSGPACK) {
		return map_buf_is_preserve_order(res->msgpack.ptr, res->msgpack.sz);
	}

	if (res->type != AS_EXP_RESULT_BIN) {
		return false;
	}

	as_bin b = { .particle = res->particle.ptr };

	as_bin_state_set_from_type(&b, res->particle.ptr->type);

	if (as_bin_get_particle_type(&b) != AS_PARTICLE_TYPE_MAP) {
		return false;
	}

	cdt_payload packed;

	as_bin_particle_map_get_packed_val(&b, &packed);

	return map_buf_is_preserve_order(packed.ptr, packed.sz);
}

// Tri-state like as_exp_eval: TRUE produced a result, UNK is a clean
// non-match, ERROR is a fault that also stages the eval-phase trace + message.
as_exp_trilean
as_exp_eval_to_result(const as_exp* exp, const as_exp_ctx* ctx, as_exp_result* res)
{
	define_deferred_array(vars, rt_value, exp->max_var_count);
	rt_value ret_val;

	*res = (as_exp_result){ 0 };

	runtime rt = {
		.ctx = ctx,
		.instr_ptr = exp->mem,
		.vars = vars,
		.bin_table = exp->bin_table,
		.vars_builtin = { [0 ...(AS_EXP_BUILTIN_COUNT - 1)] = rt_unk },
	};

	for (uint32_t i = 0; i < exp->max_var_count; i++) {
		rt.vars[i] = (rt_value){ .type = RT_END };
	}

	op_value_geo geo_mem = {
		.contents = NULL
	}; // 1 object -- only value built-in var should have geo

	rt_init_builtin_vars(&rt, &geo_mem);
	rt_eval(&rt, &ret_val);

	if (geo_mem.contents != NULL && geo_mem.compiled.type == GEO_REGION) {
		geo_region_destroy(geo_mem.compiled.region);
	}

	rt_release_bins(&rt, &ret_val);

	if (ret_val.type == RT_BIN) {
		as_bin* b = &ret_val.r_bin;
		uint8_t btype = as_bin_get_particle_type(b);

		switch (btype) {
		case AS_PARTICLE_TYPE_STRING:
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_HLL:
		case AS_PARTICLE_TYPE_GEOJSON:
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST:
			res->type = AS_EXP_RESULT_BIN;
			res->particle.ptr = b->particle;
			return AS_EXP_TRUE;
		default:
			break;
		}
	}

	rt_value t_val;
	as_packer pk = { .capacity = UINT32_MAX };

	if (! rt_value_bin_translate(&t_val, &ret_val)) {
		cf_warning(AS_EXP, "as_exp_eval_to_result - unexpected result type (%u)",
				ret_val.type);
		stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
		return AS_EXP_ERROR;
	}

	switch (t_val.type) {
	case RT_NIL:
		res->type = AS_EXP_RESULT_MP_SMALL;
		res->mp_small.sz = as_pack_nil_size();
		pk.buffer = res->mp_small.buf;
		as_pack_nil(&pk);
		break;
	case RT_INT:
		res->type = AS_EXP_RESULT_MP_SMALL;
		res->mp_small.sz = as_pack_int64_size(t_val.r_int);
		pk.buffer = res->mp_small.buf;
		as_pack_int64(&pk, t_val.r_int);
		break;
	case RT_FLOAT:
		res->type = AS_EXP_RESULT_MP_SMALL;
		res->mp_small.sz = as_pack_double_size();
		pk.buffer = res->mp_small.buf;
		as_pack_double(&pk, t_val.r_float);
		break;
	case RT_TRILEAN:
		if (t_val.r_trilean == AS_EXP_ERROR) {
			stage_eval_fault(exp, t_val.err_op_ix,
					(exp_err_reason)t_val.err_reason);
			return AS_EXP_ERROR;
		}

		if (t_val.r_trilean == AS_EXP_UNK) {
			return AS_EXP_UNK;
		}

		res->type = AS_EXP_RESULT_MP_SMALL;
		res->mp_small.sz = as_pack_bool_size();
		pk.buffer = res->mp_small.buf;
		as_pack_bool(&pk, t_val.r_trilean == AS_EXP_TRUE);
		break;
	case RT_MSGPACK:
		res->type = AS_EXP_RESULT_MSGPACK;
		res->msgpack.sz = t_val.r_bytes.sz;
		res->msgpack.ptr = t_val.r_bytes.contents;

		msgpack_in mp = { .buf = res->msgpack.ptr, .buf_sz = res->msgpack.sz };

		if (msgpack_sz(&mp) == 0) {
			stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
			return AS_EXP_ERROR;
		}

		res->msgpack.has_nonstorage = (mp.has_nonstorage ? 1 : 0);
		break;
	case RT_GEO_CONST:
		res->type = AS_EXP_RESULT_MSGPACK;
		res->msgpack.sz = t_val.r_geo_const.op->content_sz;
		res->msgpack.ptr = t_val.r_geo_const.op->contents;
		res->msgpack.has_nonstorage = 0;
		break;
	case RT_STR:
		res->type = AS_EXP_RESULT_STR;
		res->str.bytes_type = AS_BYTES_STRING;
		res->str.sz = t_val.r_bytes.sz;
		res->str.ptr = t_val.r_bytes.contents;
		break;
	case RT_BLOB:
		res->type = AS_EXP_RESULT_STR;
		res->str.bytes_type = AS_BYTES_BLOB;
		res->str.sz = t_val.r_bytes.sz;
		res->str.ptr = t_val.r_bytes.contents;
		break;
	case RT_HLL:
		res->type = AS_EXP_RESULT_STR;
		res->str.bytes_type = AS_BYTES_HLL;
		res->str.sz = t_val.r_bytes.sz;
		res->str.ptr = t_val.r_bytes.contents;
		break;
	case RT_GEO_STR:
		res->type = AS_EXP_RESULT_STR;
		res->str.bytes_type = AS_BYTES_GEOJSON;
		res->str.sz = t_val.r_bytes.sz;
		res->str.ptr = t_val.r_bytes.contents;
		break;
	case RT_RESULT_REMOVE:
		res->type = AS_EXP_RESULT_REMOVE;
		break;
	default:
		cf_warning(AS_EXP, "as_exp_eval_to_result - unexpected result type (%u)",
				t_val.type);
		rt_value_destroy(&t_val);
		stage_eval_fault(exp, 0, EXP_ERR_BAD_MSGPACK);
		return AS_EXP_ERROR;
	}

	return AS_EXP_TRUE;
}

void
as_exp_result_destroy(as_exp_result* res)
{
	if (res->type == AS_EXP_RESULT_BIN) {
		as_bin b = {
			.particle = res->particle.ptr,
		};

		as_bin_state_set_from_type(&b, res->particle.ptr->type);
		as_bin_particle_destroy(&b);
	}
}

as_exp_trilean
as_exp_matches_metadata(const as_exp* exp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd == NULL, AS_EXP, "invalid parameter");

	as_exp_trilean ret = match_internal(exp, ctx);

	return ret;
}

bool
as_exp_matches_record(const as_exp* exp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd != NULL, AS_EXP, "invalid parameter");

	as_exp_trilean ret = match_internal(exp, ctx);

	return ret == AS_EXP_TRUE;
}

bool
as_exp_display(const as_exp* exp, cf_dyn_buf* db)
{
	if (exp == NULL) {
		cf_warning(AS_EXP, "as_exp_display - exp is NULL");
		return false;
	}

	runtime rt = { .instr_ptr = exp->mem, .bin_table = exp->bin_table };

	exp_rt_display(&rt, db);

	return true;
}

//==========================================================
// Local helpers - runtime.
//

static inline bool
rt_has_ns(const runtime* rt)
{
	return rt->ctx->ns != NULL;
}

static inline bool
rt_has_rd(const runtime* rt)
{
	return rt->ctx->rd != NULL;
}

static inline bool
rt_value_is_unknown(const rt_value* entry)
{
	return entry->type == RT_TRILEAN && entry->r_trilean == AS_EXP_UNK;
}

static inline bool
rt_value_need_destroy(const rt_value* bin_arg, const as_bin* old, const as_bin* b)
{
	return bin_arg->type == RT_BIN && ! bin_arg->do_not_destroy &&
			old->particle != b->particle;
}

static inline bool
rt_value_keep_do_not_destroy(const rt_value* bin_arg, const as_bin* b)
{
	return bin_arg->type == RT_BIN && bin_arg->do_not_destroy &&
			bin_arg->r_bin.particle == b->particle;
}

static inline const uint8_t*
rt_get_bin_name(const runtime* rt, uint32_t idx)
{
	return rt->bin_table->base + rt->bin_table->table[idx].off;
}

// Human message for a genuine eval fault, composed at the boundary
// (reason -> string). The structured trace carries op/path/depth/outcome
// separately. Shared by msgpack and AEL expressions - the eval path is common.
static const char*
exp_err_reason_msg(exp_err_reason reason)
{
	switch (reason) {
	case EXP_ERR_NONE:
		break;
	case EXP_ERR_DIV_ZERO:
		return "integer division by zero";
	case EXP_ERR_DIV_OVERFLOW:
		return "integer division overflow";
	case EXP_ERR_MOD_ZERO:
		return "integer modulo by zero";
	case EXP_ERR_MOD_OVERFLOW:
		return "integer modulo overflow";
	case EXP_ERR_REC_KEY:
		return "corrupt stored record key";
	case EXP_ERR_REGEX_OPERAND:
		return "regex operand is not a string";
	case EXP_ERR_NOT_A_LIST:
		return "operand is not a list";
	case EXP_ERR_NOT_A_MAP:
		return "operand is not a map";
	case EXP_ERR_BAD_MSGPACK:
		return "malformed stored value";
	case EXP_ERR_CALL_ARG:
		return "invalid argument to a collection operation";
	case EXP_ERR_GEOJSON:
		return "invalid GeoJSON";
	case EXP_ERR_PRESERVE_ORDER_MAP:
		return "cannot compare a map with preserved element order";
	}

	// EXP_ERR_NONE, or a value from outside the enum. No fault site leaves the
	// reason unstamped, so reaching here costs a vague message rather than a
	// wrong one -- and no label covers it, so -Wswitch names a new reason that
	// forgets its case.
	return "expression evaluation faulted";
}

// Render an op name into 'dst' (NUL-terminated, truncated to cap), returning
// the byte length.
static uint16_t
exp_render_op_name(exp_op_code code, char* dst, uint32_t cap)
{
	if (cap == 0) { // no room even for the NUL - avoid the cap - 1 underflow
		return 0;
	}

	const char* name = op_table[code].name;

	if (name == NULL) {
		name = "op";
	}

	size_t len = strlen(name);

	if (len > cap - 1) { // cap >= 1 here (guarded above), so cap - 1 is safe
		len = cap - 1;
	}

	memcpy(dst, name, len);
	dst[len] = '\0';

	return (uint16_t)len;
}

// Stage a field-45 eval-phase trace for op 'op_ix' with the given outcome
// (fault / false / absent), reconstructing the ancestor path and focusable
// snippet from the compiled op array. Trace-only (top verbosity tier) - the
// caller pairs it with any message it wants.
static void
stage_eval_trace(const as_exp* exp, uint32_t op_ix, uint8_t outcome,
		const explain_state* ex)
{
	// First-set-wins is enforced in as_error_exp_trace_set(), but check it
	// here too - everything below (the ancestor walk, the op-name renders and
	// the subtree display) would otherwise be built and then discarded.
	if (g_error_verbosity < AS_ERROR_VERBOSITY_TRACE || g_error_exp_trace.set) {
		return;
	}

	as_error_exp_trace t = { .phase = AS_EXP_TRACE_PHASE_EVAL,
		.has_outcome = true,
		.outcome = outcome };

	// Guard a corrupt index (defends against a dropped err_op_ix)
	// - the root op's instr_end_ix is the total op count.
	const op_base_mem* root = (const op_base_mem*)exp->mem;
	bool walk_ok = true;

	if (op_ix < root->instr_end_ix) {
		// Walk the preorder, variable-size op array collecting the ancestor
		// chain root -> op_ix: descend into an op whose subtree contains the
		// target, skip a sibling subtree entirely. Keep the outermost
		// (cap - 1) frames + the target op; depth is the true nesting depth.
		exp_op_code chain[AS_EXP_TRACE_MAX_FRAMES];
		uint16_t n_kept = 0;
		uint16_t depth = 0;
		const uint8_t* p = exp->mem;
		uint32_t cursor = 0;

		while (cursor < op_ix) {
			const op_base_mem* op = (const op_base_mem*)p;

			if (op_ix < op->instr_end_ix) { // ancestor - descend
				if (n_kept < AS_EXP_TRACE_MAX_FRAMES - 1) {
					chain[n_kept++] = op->code;
				}

				depth++;
				p += op_table[op->code].size;
				cursor++;
			}
			else { // sibling subtree - skip [cursor, end)
				uint32_t end = op->instr_end_ix;

				// Every op's subtree includes itself and ends within the op
				// array, so cursor < end <= root->instr_end_ix always holds
				// for a well-formed one. Too small (the calloc'd 0 an omitted
				// instr_end_ix stamp would leave) advances neither p nor
				// cursor - an unkillable spin. Too large walks p off the end
				// of the array and indexes op_table with whatever it reads.
				// Bail out with no path on either.
				if (end <= cursor || end > root->instr_end_ix) {
					walk_ok = false;
					break;
				}

				while (cursor < end) {
					const op_base_mem* s = (const op_base_mem*)p;
					p += op_table[s->code].size;
					cursor++;
				}
			}
		}

		// Malformed op array - 'p' is not the target op, so naming it would be
		// worse than saying nothing. Skip the op/path/snippet block but fall
		// through to the operand attach below, matching what the sibling
		// corrupt-op_ix guard does.
		if (walk_ok) {
			exp_op_code op_code = ((const op_base_mem*)p)->code;

			depth++; // the target op itself

			t.op_len = exp_render_op_name(op_code, t.op, sizeof(t.op));
			t.has_op = true;

			for (uint16_t i = 0; i < n_kept; i++) {
				exp_render_op_name(chain[i], t.path[i], sizeof(t.path[i]));
			}

			exp_render_op_name(op_code, t.path[n_kept], sizeof(t.path[n_kept]));

			t.n_frames = (uint16_t)(n_kept + 1);
			t.depth = depth;
			t.path_truncated = depth > AS_EXP_TRACE_MAX_FRAMES;
			t.has_path = true;

			const ael_src_map* ael_map = (const ael_src_map*)exp->ael_map;

			if (ael_map != NULL) {
				// AEL expression: resolve the op's ordinal through the
				// compiled-in source map and render the focus-marked source
				// slice in place of the op-stream disassembly. byte_offset
				// (msgpack payload space) is deliberately never set for AEL.
				t.has_lang = true;
				t.lang = AS_EXP_TRACE_LANG_AEL;

				if (op_ix < ael_map->n_ops) {
					const ael_src_entry* e = &ael_map->entries[op_ix];

					t.has_ael_offset = true;
					t.ael_offset = e->offset;
					t.has_ael_span = e->sz != 0;
					t.ael_span = e->sz;

					t.snippet_len = exp_render_ael_src_snippet(ael_map->src,
							ael_map->src_sz, e->offset, e->sz, t.snippet,
							sizeof(t.snippet));
					t.has_snippet = t.snippet_len != 0;
				}
			}
			else {
				// Wire expression: render the target op's subtree as
				// op_name(arg, ...) via the display machinery. The scratch
				// runtime needs no ctx/record, but it MUST carry the compiled
				// bin_table - display_bin resolves bin names through it, so
				// leaving it NULL would crash on a subtree containing a bin
				// reference. On a render that overflows the cap, leave
				// has_snippet false.
				//
				// display_max_sz is what makes the render safe to run at all:
				// it bounds the OUTPUT. A single op can emit bytes
				// proportional to its own operands - a CDT context walks an
				// input-controlled element list - so subtree op count bounds
				// nothing. Without the budget a fault near the root of a
				// near-EXP_MAX_SIZE expression renders far more than the cap,
				// grows snip_db onto the heap, and has all of it thrown away
				// by the cap check below - once per record, and batch re-arms
				// per row. Every payload-driven loop consults it, so snip_db
				// stays within one element's overshoot of its stack buffer.
				//
				// The op-count skip stays as a cheap pre-filter: every op
				// contributes at least one byte, so a subtree of more ops than
				// the cap cannot fit, and skipping it drops no snippet the cap
				// check would have kept.
				uint32_t n_sub_ops =
						((const op_base_mem*)p)->instr_end_ix - op_ix;

				if (n_sub_ops < AS_EXP_TRACE_SNIPPET_MAX) {
					cf_dyn_buf_define_size(snip_db, AS_EXP_TRACE_SNIPPET_MAX);
					runtime snip_rt = { .instr_ptr = p,
						.op_ix = op_ix,
						.bin_table = exp->bin_table,
						.display_max_sz = AS_EXP_TRACE_SNIPPET_MAX };

					exp_rt_display(&snip_rt, &snip_db);

					// The UTF-8 test gates the wire, not the renderer: the display
					// callbacks copy bin names and string literals verbatim and
					// also feed the cf_detail log, where raw bytes are harmless.
					// An expression bin name is length-checked only
					// (as_bin_name_sz_check), so any non-NUL byte compiles -
					// checking here covers every display callback at once,
					// including ones added later. Key 6 is a msgpack str, so an
					// invalid one would fault the client's decoder on the very
					// error being reported; drop the snippet whole, as the budget
					// cap above already does.
					if (snip_db.used_sz != 0 &&
							snip_db.used_sz < AS_EXP_TRACE_SNIPPET_MAX &&
							cf_str_is_valid_utf8(snip_db.buf, snip_db.used_sz)) {
						// Flatten control characters - inside a str they would
						// break line-oriented consumers of the message.
						for (uint32_t i = 0; i < snip_db.used_sz; i++) {
							uint8_t c = snip_db.buf[i];
							bool is_ctl = c < 0x20 || c == 0x7f;

							t.snippet[i] = is_ctl ? ' ' : (char)c;
						}

						t.snippet[snip_db.used_sz] = '\0';
						t.snippet_len = (uint16_t)snip_db.used_sz;
						t.has_snippet = true;
					}

					cf_dyn_buf_free(&snip_db);
				}
			}
		}
	}

	// Attach the decisive comparison's operand values, but only when they
	// belong to the op we are tracing (ex->operands_ix).
	if (ex != NULL && ex->operands_set && ex->operands_ix == op_ix) {
		t.has_operands = true;
		t.lhs_len = ex->lhs_len;
		memcpy(t.lhs, ex->lhs, ex->lhs_len);
		t.lhs[ex->lhs_len] = '\0';
		t.rhs_len = ex->rhs_len;
		memcpy(t.rhs, ex->rhs, ex->rhs_len);
		t.rhs[ex->rhs_len] = '\0';
	}

	as_error_exp_trace_set(&t);
}

// Stage an eval fault - the eval-phase trace (outcome=fault, top tier) plus
// the per-reason message (tier 2+).
static void
stage_eval_fault(const as_exp* exp, uint32_t fault_ix, exp_err_reason reason)
{
	stage_eval_trace(exp, fault_ix, AS_EXP_TRACE_OUTCOME_FAULT, NULL);

	// Guarded so below the message tier the reason lookup + set_fmt are
	// skipped entirely (nothing would be emitted).
	if (g_error_verbosity >= AS_ERROR_VERBOSITY_MESSAGES) {
		as_error_details_set_fmt(AS_SUB_NONE, "%s", exp_err_reason_msg(reason));
	}
}

static as_exp_trilean
match_internal(const as_exp* exp, const as_exp_ctx* ctx)
{
	define_deferred_array(vars, rt_value, exp->max_var_count);
	rt_value ret_val;

	runtime rt = {
		.ctx = ctx,
		.instr_ptr = exp->mem,
		.vars = vars,
		.bin_table = exp->bin_table,
		.vars_builtin = { [0 ...(AS_EXP_BUILTIN_COUNT - 1)] = rt_unk },
	};

	for (uint32_t i = 0; i < exp->max_var_count; i++) {
		rt.vars[i] = (rt_value){ .type = RT_END };
	}

	op_value_geo geo_mem = {
		.contents = NULL
	}; // 1 object -- only value built-in var should have geo

	rt_init_builtin_vars(&rt, &geo_mem);
	rt_eval(&rt, &ret_val);

	if (geo_mem.contents != NULL &&
			geo_mem.compiled.type == GEO_REGION) { // geojson needs cleanup
		geo_region_destroy(geo_mem.compiled.region);
	}

	rt_release_bins(&rt, &ret_val);

	if (ret_val.type != RT_TRILEAN) {
		rt_value_destroy(&ret_val);
		return AS_EXP_FALSE;
	}

	if (ret_val.r_trilean == AS_EXP_UNK) {
		return ctx->rd == NULL ? AS_EXP_UNK : AS_EXP_FALSE;
	}

	if (ret_val.r_trilean == AS_EXP_ERROR) {
		if (ctx->rd == NULL) {
			// Metadata (pre-bin) phase - not authoritative. A sibling that is
			// UNK only because its bin is not loaded can still determine the
			// match once bins load (e.g. or(bin > 5, div(x, 0)) on a record
			// whose bin > 5), so surfacing the fault here could drop a record
			// that should match. Defer like a top-level UNK; the record phase
			// re-evaluates and surfaces the fault there.
			return AS_EXP_UNK;
		}

		// Record phase - authoritative. Stage the eval-phase trace + message;
		// the match status is unchanged (the caller maps ERROR to the same
		// non-match it returns for FALSE).
		stage_eval_fault(exp, ret_val.err_op_ix,
				(exp_err_reason)ret_val.err_reason);
	}

	return ret_val.r_trilean;
}

// The top-level result of an explainer re-eval, flattened to scalars.
typedef struct explain_top_s {
	bool is_trilean;
	as_exp_trilean trilean;
	uint32_t op_ix; // decisive / absent / fault op (valid for a trilean result)
	exp_err_reason err_reason; // valid only when trilean == AS_EXP_ERROR
} explain_top;

// Shared re-eval for the decision explainers. Re-runs the compiled expression
// with the explain machinery armed (runtime.explain) so ops record the
// decisive/absent op index - and a comparison its operand values - into *ex,
// then hands back the top result as scalars in *out (no pointers into the
// freed eval stack). Mirrors match_internal's runtime setup exactly.
static void
explain_reeval(const as_exp* exp, const as_exp_ctx* ctx, explain_state* ex,
		explain_top* out)
{
	define_deferred_array(vars, rt_value, exp->max_var_count);
	rt_value ret_val;

	runtime rt = {
		.ctx = ctx,
		.instr_ptr = exp->mem,
		.vars = vars,
		.bin_table = exp->bin_table,
		.explain = ex,
		.vars_builtin = { [0 ...(AS_EXP_BUILTIN_COUNT - 1)] = rt_unk },
	};

	// Bins occupy the first var slots and lazy-load on first access. The
	// .bin_table wiring and this RT_END init are both required, else the
	// re-eval's eval_bin reads uninitialized slots / a NULL bin_table.
	for (uint32_t i = 0; i < exp->max_var_count; i++) {
		rt.vars[i] = (rt_value){ .type = RT_END };
	}

	op_value_geo geo_mem = {
		.contents = NULL
	}; // 1 object -- only value built-in var should have geo

	// A no-op when ctx->vars_table is NULL (true for a top-level filter or value
	// read), so this faithfully reproduces as_exp_eval, which omits it.
	rt_init_builtin_vars(&rt, &geo_mem);
	rt_eval(&rt, &ret_val);

	if (geo_mem.contents != NULL &&
			geo_mem.compiled.type == GEO_REGION) { // geojson needs cleanup
		geo_region_destroy(geo_mem.compiled.region);
	}

	// Free the bin particles this re-eval materialized into rt.vars[] -
	// without this, every explain pass leaks a particle per touched bin.
	rt_release_bins(&rt, &ret_val);

	out->is_trilean = ret_val.type == RT_TRILEAN;

	if (out->is_trilean) {
		out->trilean = ret_val.r_trilean;
		out->op_ix = ret_val.err_op_ix;
		out->err_reason = (exp_err_reason)ret_val.err_reason;
	}
	else {
		rt_value_destroy(&ret_val);
	}
}

void
as_exp_explain_filter(const as_exp* exp, const as_exp_ctx* ctx)
{
	// Explain a clean single-record filter non-match: re-run the expression
	// with runtime.explain set, then stage an eval-phase trace naming the
	// deciding sub-expression and its outcome. pre: record phase (rd != NULL),
	// single-record request, non-match already returned - never per row in a
	// query/scan. A no-op below the trace tier.
	//
	// The caller must already have established that the principal may read
	// the record: everything staged here is stored-data-derived, including
	// the outcome (FALSE vs ABSENT reveals bin existence) and the decisive op
	// index (which conjunct of an and-chain failed), not just the operand
	// values. read_and_filter_bins() makes that check.
	cf_assert(ctx->rd != NULL, AS_EXP, "invalid parameter");

	if (g_error_verbosity < AS_ERROR_VERBOSITY_TRACE) {
		return;
	}

	// A fault during the authoritative match already staged a trace, which
	// outranks any explanation - skip the redundant re-eval.
	if (g_error_exp_trace.set) {
		return;
	}

	explain_state ex = { 0 };
	explain_top top;

	explain_reeval(exp, ctx, &ex, &top);

	// A non-boolean top result can't happen for a compiled filter, but if it
	// does there is nothing meaningful to explain - render the whole expression
	// as a plain FALSE.
	if (! top.is_trilean) {
		stage_eval_trace(exp, 0, AS_EXP_TRACE_OUTCOME_FALSE, NULL);
		as_error_details_set_fmt(AS_SUB_NONE,
				"filtered out - filter expression evaluated to false");
		return;
	}

	// Each arm stages the trace THEN authors the message, so
	// as_error_details_set_fmt's eager assembly picks up the just-staged trace.
	switch (top.trilean) {
	case AS_EXP_FALSE:
		// A real predicate FALSE: op_ix points at the deciding clause.
		stage_eval_trace(exp, top.op_ix, AS_EXP_TRACE_OUTCOME_FALSE, &ex);
		as_error_details_set_fmt(AS_SUB_NONE,
				"filtered out - filter expression evaluated to false");
		break;
	case AS_EXP_UNK:
		// A referenced bin/key was absent - op_ix points at the absent site.
		stage_eval_trace(exp, top.op_ix, AS_EXP_TRACE_OUTCOME_ABSENT, NULL);
		as_error_details_set_fmt(AS_SUB_NONE,
				"filtered out - filter references an absent bin or key");
		break;
	case AS_EXP_ERROR:
		// Shouldn't happen (the authoritative pass returned a clean
		// non-match), but stay correct: report a fault, not a false/absent
		// explanation.
		stage_eval_fault(exp, top.op_ix, top.err_reason);
		break;
	default: // AS_EXP_TRUE - the record actually matches; nothing to explain
		break;
	}
}

// The value-read counterpart of as_exp_explain_filter: explain why an
// expression op produced no value (a clean UNK from as_exp_eval). A value UNK
// is an absent / wrong-type reference (outcome ABSENT). No operand values are
// staged, but the outcome and the decisive op index are still functions of the
// stored record - which reference went absent tells the caller whether that
// bin exists and whether it has the referenced type - so the caller must have
// established read permission exactly as on the filter side. eval_op() makes
// that check. pre: record phase; a no-op below the trace tier or once a fault
// already staged its trace.
void
as_exp_explain_value(const as_exp* exp, const as_exp_ctx* ctx)
{
	cf_assert(ctx->rd != NULL, AS_EXP, "invalid parameter");

	if (g_error_verbosity < AS_ERROR_VERBOSITY_TRACE) {
		return;
	}

	// A fault already staged its trace in as_exp_eval - skip the redundant
	// re-eval.
	if (g_error_exp_trace.set) {
		return;
	}

	explain_state ex = { 0 };
	explain_top top;

	explain_reeval(exp, ctx, &ex, &top);

	// A value materialized on the re-eval - nothing to explain. Shouldn't happen
	// (eval_op only calls this after a clean AS_EXP_UNK), but stay correct.
	if (! top.is_trilean) {
		return;
	}

	switch (top.trilean) {
	case AS_EXP_UNK:
		// A referenced bin/key was absent or read at the wrong type - op_ix
		// points at the absent site.
		stage_eval_trace(exp, top.op_ix, AS_EXP_TRACE_OUTCOME_ABSENT, NULL);
		as_error_details_set_fmt(AS_SUB_NONE,
				"operation not applicable - expression references an absent bin or key");
		break;
	case AS_EXP_ERROR:
		// Defensive - report a fault, not an absent explanation.
		stage_eval_fault(exp, top.op_ix, top.err_reason);
		break;
	default: // AS_EXP_TRUE / AS_EXP_FALSE - a value materialized; nothing to do
		break;
	}
}

static bool
rt_eval(runtime* rt, rt_value* ret_val)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];
	uint32_t self_ix = rt->op_ix; // this op's index, before children advance it

	rt->op_ix++;
	rt->instr_ptr += entry->size;
	*ret_val = (rt_value){ 0 };
	entry->eval_cb(rt, ob, ret_val);

	// Short-circuit on UNK or ERROR - a parent that copies a short-circuit
	// child propagates a fault verbatim. The Kleene ops (eval_and / eval_or)
	// remember a fault but let a determining sibling override it, so the
	// match outcome is unchanged.
	bool ret = rt_value_is_short_circuit(ret_val);

	// Decisive-op stamp for the explainer. A definite FALSE never
	// short-circuits, so a FALSE out of any op is that op's own decision -
	// stamp self (also clears any stale err_op_ix a leaf left in the aliased
	// union). Excluded because their FALSE is a single child's decision,
	// already stamped in the value they pass up: 'and' (its deciding
	// conjunct) and the transparent wrappers 'let' / 'cond' - stamping the
	// wrapper would obscure the deciding comparison and detach its captured
	// operands (keyed to the comparison's op index). UNK is left alone: it is
	// propagated, or stamped at its absent site.
	if (rt->explain != NULL && ret_val->type == RT_TRILEAN &&
			ret_val->r_trilean == AS_EXP_FALSE && ob->code != EXP_AND &&
			ob->code != EXP_LET && ob->code != EXP_COND) {
		ret_val->err_op_ix = self_ix;
	}

	return ret;
}

void
exp_eval_unknown(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = AS_EXP_UNK;
	// An unknown() reference is an absent site - attribute it to itself.
	rt_mark_decisive(rt, ret_val, rt->op_ix - 1);
}

void
exp_eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	rt_value arg0;
	rt_value arg1;

	if (rt_eval(rt, &arg0)) {
		*ret_val = arg0;
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	defer_rt_value_destroy(arg0);

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	defer_rt_value_destroy(arg1);

	rt_value v0;
	rt_value v1;

	rt_value_translate(&v0, &arg0);
	rt_value_translate(&v1, &arg1);

	defer_rt_value_destroy(v0);
	defer_rt_value_destroy(v1);

	// A post-translate ERROR can only be a geo materialization fault (a
	// corrupt stored geojson bin - see json_to_rt_geo). Check ERROR
	// specifically: translate CAN yield a clean UNK (e.g. an HLL operand is
	// not comparable), which must fall through to the unknown check below,
	// not be misreported as a fault.
	if ((v0.type == RT_TRILEAN && v0.r_trilean == AS_EXP_ERROR) ||
			(v1.type == RT_TRILEAN && v1.r_trilean == AS_EXP_ERROR)) {
		rt_set_error(ret_val, self_ix, EXP_ERR_GEOJSON);
		return;
	}

	// A stored collection whose payload doesn't peek is corrupt storage, not
	// a type mismatch - classify it as a fault before the rt_is_type gate
	// below flattens it to a clean UNK. An empty payload is corrupt by the
	// same token, and msgpack_peek_type() already reports MSGPACK_TYPE_ERROR
	// for one - the sz == 0 tests are a NULL-safety belt, since contents may
	// be NULL when sz is 0, not a bounds check.
	if ((v0.type == RT_MSGPACK &&
				(v0.r_bytes.sz == 0 ||
						msgpack_buf_peek_type(v0.r_bytes.contents,
								v0.r_bytes.sz) == MSGPACK_TYPE_ERROR)) ||
			(v1.type == RT_MSGPACK &&
					(v1.r_bytes.sz == 0 ||
							msgpack_buf_peek_type(v1.r_bytes.contents,
									v1.r_bytes.sz) == MSGPACK_TYPE_ERROR))) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	ret_val->type = RT_TRILEAN;

	if (rt_value_is_unknown(&v0) || rt_value_is_unknown(&v1) ||
			! rt_is_type(&v0, ob->rtype) || ! rt_is_type(&v1, ob->rtype)) {
		ret_val->r_trilean = AS_EXP_UNK;
		// Stamp this comparison as the decisive op - without it err_op_ix
		// stays 0 and the explainer attributes an ABSENT outcome to the root,
		// highlighting the whole expression instead of this operand.
		rt_mark_decisive(rt, ret_val, self_ix);
		return;
	}

	switch (ob->rtype) {
	case EXP_RTYPE_NIL:
		ret_val->r_trilean = cmp_nil(ob->code, &v0, &v1);
		break;
	case EXP_RTYPE_TRILEAN:
		ret_val->r_trilean = cmp_trilean(ob->code, &v0, &v1);
		break;
	case EXP_RTYPE_INT:
		ret_val->r_trilean = cmp_int(ob->code, &v0, &v1);
		break;
	case EXP_RTYPE_FLOAT:
		ret_val->r_trilean = cmp_float(ob->code, &v0, &v1);
		break;
	case EXP_RTYPE_STR:
	case EXP_RTYPE_BLOB:
		ret_val->r_trilean = cmp_bytes(ob->code, &v0, &v1);
		break;
	case EXP_RTYPE_GEOJSON: {
		geo_data gd0;
		geo_data gd1;

		rt_value_get_geo(&v0, &gd0);
		rt_value_get_geo(&v1, &gd1);

		ret_val->r_trilean = as_geojson_match(gd0.region != NULL, gd0.cellid,
				gd0.region, gd1.cellid, gd1.region, true);

		break;
	}
	case EXP_RTYPE_LIST:
	case EXP_RTYPE_MAP: {
		bool preserve_order = false;

		ret_val->r_trilean = cmp_msgpack(ob->code, &v0, &v1, &preserve_order);

		if (ret_val->r_trilean == AS_EXP_ERROR) {
			rt_set_error(ret_val, self_ix,
					preserve_order ? EXP_ERR_PRESERVE_ORDER_MAP
								   : EXP_ERR_BAD_MSGPACK);
		}

		break;
	}
	default:
		cf_crash(AS_EXP, "unexpected type %u", ob->rtype);
	}

	// In explain mode a definite FALSE comparison is the decisive clause -
	// render its operand values before the deferred destroys run, keyed by
	// this op's index.
	if (ret_val->r_trilean == AS_EXP_FALSE && rt->explain != NULL) {
		explain_state* ex = rt->explain;

		ex->lhs_len = rt_value_render(&v0, ex->lhs, sizeof(ex->lhs));
		ex->rhs_len = rt_value_render(&v1, ex->rhs, sizeof(ex->rhs));
		ex->operands_ix = self_ix;
		ex->operands_set = true;
	}
}

void
exp_eval_cmp_regex(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	uint32_t str_sz = 0; // initialized for centos 6
	const uint8_t* str = rt_value_get_str(ret_val, &str_sz);

	if (str == NULL) {
		// Regex against a non-string operand is a genuine type fault (the
		// operand is a definite value here, not absent/deferred).
		rt_value_destroy(ret_val);
		rt_set_error(ret_val, self_ix, EXP_ERR_REGEX_OPERAND);
		return;
	}

	op_cmp_regex* op = (op_cmp_regex*)ob;

	define_deferred_memory(tmp, str_sz + 1);

	memcpy(tmp, str, str_sz);
	tmp[str_sz] = '\0';
	int rv = regexec(&op->regex, (const char*)tmp, 0, NULL, 0);

	rt_value_destroy(ret_val);
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = (rv == 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
}

void
exp_eval_in_list(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	rt_value arg0;
	rt_value arg1;

	// Copy a short-circuit operand verbatim so a propagated ERROR survives.
	if (rt_eval(rt, &arg0)) {
		*ret_val = arg0;
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	defer_rt_value_destroy(arg0);

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	defer_rt_value_destroy(arg1);
	rt_value list;

	// Arg1 is a definite value here - an untranslatable list operand is a
	// genuine fault, not an absent operand.
	if (! rt_value_bin_translate(&list, &arg1)) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	defer_rt_value_destroy(list);

	if (list.type != RT_MSGPACK) {
		cf_warning(AS_EXP, "exp_eval_in_list - list expected, got %u", list.type);
		rt_set_error(ret_val, self_ix, EXP_ERR_NOT_A_LIST);
		return;
	}

	uint8_t buf[1 + sizeof(uint64_t)];
	msgpack_vec vec;
	define_rollback_alloc(alloc, NULL, 1);
	DEFER_ROLLBACK_ALLOC(alloc);

	as_packer pk = { .buffer = buf, .capacity = sizeof(buf) };

	if (! rt_value_to_msgpack_vec(&pk, &vec, alloc, &arg0)) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	msgpack_in mp_list = { .buf = list.r_bytes.contents,
		.buf_sz = list.r_bytes.sz };

	uint32_t ele_count = 0;

	if (! msgpack_get_list_ele_count(&mp_list, &ele_count)) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	msgpack_in mp_ele = { .buf = vec.buf, .buf_sz = vec.buf_sz };

	for (uint32_t i = 0; i < ele_count; i++) {
		mp_ele.offset = 0;

		msgpack_cmp_type cmp = msgpack_cmp(&mp_ele, &mp_list);

		if (cmp == MSGPACK_CMP_ERROR) {
			rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
			return;
		}

		if (cmp == MSGPACK_CMP_EQUAL) {
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_TRUE;
			return;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = AS_EXP_FALSE;
}

void
exp_eval_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	as_exp_trilean ret = AS_EXP_TRUE;
	uint32_t err_op_ix = 0; // first fault's op index (if ret==ERROR)
	exp_err_reason err_reason = EXP_ERR_NONE;
	uint32_t dec_op_ix = 0; // First UNK child's decisive op (explain)

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			// Absorb a fault like UNK but remember it; a determining FALSE
			// still overrides it. Precedence FALSE > ERROR > UNK > TRUE.
			if (ret_val->r_trilean == AS_EXP_ERROR) {
				if (ret != AS_EXP_ERROR) { // keep the first fault
					err_op_ix = ret_val->err_op_ix;
					err_reason = (exp_err_reason)ret_val->err_reason;
				}

				ret = AS_EXP_ERROR;
			}
			else if (ret == AS_EXP_TRUE) {
				ret = AS_EXP_UNK; // UNK upgrades only the TRUE seed, not ERROR

				if (rt->explain != NULL) {
					// The first UNK conjunct's absent site is what the
					// explainer reports if the and stays indeterminate.
					dec_op_ix = ret_val->err_op_ix;
				}
			}

			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected - type %u",
				ret_val->type);

		if (ret_val->r_trilean == AS_EXP_FALSE) {
			ret = AS_EXP_FALSE; // determining: overrides a remembered fault
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	if (ret == AS_EXP_ERROR) {
		rt_set_error(ret_val, err_op_ix, err_reason);
	}
	else {
		// A determining FALSE broke out with ret_val holding the deciding
		// conjunct (its err_op_ix intact); an all-TRUE/UNK result overwrites
		// r_trilean here. For an ABSENT (UNK) result, restore the first UNK
		// conjunct's decisive op.
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = ret;

		if (rt->explain && ret == AS_EXP_UNK) {
			ret_val->err_op_ix = dec_op_ix;
		}
	}
}

void
exp_eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	as_exp_trilean ret = AS_EXP_FALSE;
	uint32_t err_op_ix = 0; // first fault's op index (if ret==ERROR)
	exp_err_reason err_reason = EXP_ERR_NONE;
	uint32_t dec_op_ix = 0; // First UNK child's decisive op (explain)

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			// Absorb a fault like UNK but remember it; a determining TRUE
			// still overrides it. Precedence TRUE > ERROR > UNK > FALSE.
			if (ret_val->r_trilean == AS_EXP_ERROR) {
				if (ret != AS_EXP_ERROR) { // keep the first fault
					err_op_ix = ret_val->err_op_ix;
					err_reason = (exp_err_reason)ret_val->err_reason;
				}

				ret = AS_EXP_ERROR;
			}
			else if (ret == AS_EXP_FALSE) {
				ret = AS_EXP_UNK; // UNK upgrades only the FALSE seed, not ERROR

				if (rt->explain != NULL) {
					dec_op_ix = ret_val->err_op_ix; // first UNK branch
				}
			}

			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			ret = AS_EXP_TRUE; // determining: overrides a remembered fault
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	if (ret == AS_EXP_ERROR) {
		rt_set_error(ret_val, err_op_ix, err_reason);
	}
	else {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = ret;

		// An indeterminate (UNK) or node reports its first absent
		// branch; a definite FALSE (all branches false) is stamped as the or node
		// by rt_eval (the design convention: report the or, not each branch).
		if (rt->explain && ret == AS_EXP_UNK) {
			ret_val->err_op_ix = dec_op_ix;
		}
	}
}

void
exp_eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return; // UNK/ERROR child propagates verbatim (keeps its decisive op)
	}

	cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

	// not(x)=FALSE means x was true; rt_eval stamps the not node as decisive.
	ret_val->r_trilean = (ret_val->r_trilean == AS_EXP_TRUE) ? AS_EXP_FALSE
															 : AS_EXP_TRUE;
}

void
exp_eval_exclusive(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// Logical xor is implemented to mean that exactly one arg is true.
	as_exp_trilean ret = AS_EXP_FALSE;

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			if (ret == AS_EXP_TRUE) {
				ret_val->r_trilean = AS_EXP_FALSE;
				rt_skip(rt, ob->instr_end_ix);
				return;
			}

			ret = AS_EXP_TRUE;
			continue;
		}
	}

	ret_val->r_trilean = ret;
}

void
exp_eval_add(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float += arg.r_float;
		}
		else {
			ret_val->r_int += arg.r_int;
		}
	}
}

void
exp_eval_sub(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	if (rt->op_ix == ob->instr_end_ix) {
		if (is_float) {
			ret_val->r_float = 0.0 - ret_val->r_float;
		}
		else {
			ret_val->r_int = 0 - ret_val->r_int;
		}

		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float -= arg.r_float;
		}
		else {
			ret_val->r_int -= arg.r_int;
		}
	}
}

void
exp_eval_mul(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			ret_val->r_float *= arg.r_float;
		}
		else {
			ret_val->r_int *= arg.r_int;
		}
	}
}

void
exp_eval_div(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	uint32_t n_args = 1;
	double dproduct = 1.0;
	int64_t iproduct = 1;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		n_args++;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			dproduct *= arg.r_float;
		}
		else {
			iproduct *= arg.r_int;
		}
	}

	if (is_float) {
		if (n_args == 1) {
			dproduct = ret_val->r_float;
			ret_val->r_float = 1.0;
		}

		ret_val->r_float /= dproduct;
	}
	else {
		if (n_args == 1) {
			iproduct = ret_val->r_int;
			ret_val->r_int = 1;
		}

		if (iproduct == 0) {
			cf_warning(AS_EXP, "exp_eval_div - integer division by zero");
			rt_set_error(ret_val, self_ix, EXP_ERR_DIV_ZERO);
			return;
		}

		if (ret_val->r_int == INT64_MIN && iproduct == -1) {
			cf_warning(AS_EXP, "exp_eval_div - integer division overflow");
			rt_set_error(ret_val, self_ix, EXP_ERR_DIV_OVERFLOW);
			return;
		}

		ret_val->r_int /= iproduct;
	}
}

void
exp_eval_pow(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	ret_val->r_float = pow(ret_val->r_float, arg1.r_float);
}

void
exp_eval_log(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	ret_val->r_float = log(ret_val->r_float) / log(arg1.r_float);
}

void
exp_eval_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	// Propagate a short-circuit divisor verbatim (preserving UNK or a
	// propagated ERROR); integer mod-by-zero and INT64_MIN % -1 overflow are
	// genuine faults.
	if (rt_eval(rt, &arg1)) {
		*ret_val = arg1;
		return;
	}

	if (arg1.r_int == 0) {
		rt_set_error(ret_val, self_ix, EXP_ERR_MOD_ZERO);
		return;
	}

	if (ret_val->r_int == INT64_MIN && arg1.r_int == -1) {
		rt_set_error(ret_val, self_ix, EXP_ERR_MOD_OVERFLOW);
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = ret_val->r_int % arg1.r_int;
}

void
exp_eval_floor(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_float = floor(ret_val->r_float);
}

void
exp_eval_abs(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	if (ret_val->type == RT_FLOAT) {
		ret_val->r_float = fabs(ret_val->r_float);
	}
	else {
		ret_val->r_int = labs(ret_val->r_int);
	}
}

void
exp_eval_ceil(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_float = ceil(ret_val->r_float);
}

void
exp_eval_to_int(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	// Intermediate variable to satisfy Coverity...
	int64_t int_val = (int64_t)ret_val->r_float;

	ret_val->r_int = int_val;
	ret_val->type = RT_INT;
}

void
exp_eval_to_float(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	// Intermediate variable to satisfy Coverity...
	double float_val = (double)ret_val->r_int;

	ret_val->r_float = float_val;
	ret_val->type = RT_FLOAT;
}

void
exp_eval_to_string(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	// This op's own index, before the operand rt_eval advances op_ix.
	uint32_t self_ix = rt->op_ix - 1;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	// A string operand is already the result -- toString is the identity.
	if (ret_val->type == RT_STR) {
		return;
	}

	rt_value operand = *ret_val;
	as_bin scratch = { 0 };
	as_bin* b = &scratch;

	// Only RT_BLOB below heap-allocates b->particle (INT/FLOAT/BOOL embed the
	// value in the pointer); track that alloc separately so the deferred free
	// hits just it (a no-op while blob_scratch is NULL).
	as_particle* blob_scratch = NULL;

	cf_defer { cf_free(blob_scratch); }

	switch (operand.type) {
	case RT_BIN_PTR:
		b = operand.r_bin_p;
		break;
	case RT_BIN:
		b = &operand.r_bin;
		break;
	case RT_INT:
		b->particle = (as_particle*)(uint64_t)operand.r_int;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_INTEGER);
		break;
	case RT_FLOAT: {
		double fval = operand.r_float;

		memcpy(&b->particle, &fval, sizeof(double));
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_FLOAT);
		break;
	}
	case RT_TRILEAN: {
		uint64_t bval = operand.r_trilean == AS_EXP_TRUE ? 1 : 0;

		b->particle = (as_particle*)bval;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_BOOL);
		break;
	}
	case RT_BLOB: {
		blob_scratch = rt_alloc_mem(rt,
				(size_t)operand.r_bytes.sz + sizeof(cdt_mem), NULL);
		b->particle = blob_scratch;

		cdt_mem* p_cdt_mem = (cdt_mem*)b->particle;

		p_cdt_mem->type = AS_PARTICLE_TYPE_BLOB;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_BLOB);
		p_cdt_mem->sz = operand.r_bytes.sz;
		memcpy(p_cdt_mem->data, operand.r_bytes.contents, p_cdt_mem->sz);
		break;
	}
	default:
		// RT_NIL / RT_MSGPACK / RT_HLL / RT_GEO* / ... cannot stringify.
		rt_value_destroy(&operand);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		// The operand's payload word aliases err_op_ix in the union - stamp
		// this op's index rather than leave that data-derived value for the
		// explainer to read as an op index.
		rt_mark_decisive(rt, ret_val, self_ix);
		return;
	}

	as_bin rb = { 0 };

	if (as_bin_to_string(b, &rb) != AS_OK) {
		rt_value_destroy(&operand);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		rt_mark_decisive(rt, ret_val, self_ix);
		return;
	}

	rt_value_destroy(&operand);

	ret_val->type = RT_BIN;
	ret_val->r_bin = rb;
	ret_val->do_not_destroy = 0;
}

void
exp_eval_int_and(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int &= arg.r_int;
	}
}

void
exp_eval_int_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int |= arg.r_int;
	}
}

void
exp_eval_int_xor(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		ret_val->r_int ^= arg.r_int;
	}
}

void
exp_eval_int_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_int = ~ret_val->r_int;
}

void
exp_eval_int_lshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int <<= shift.r_int;
}

void
exp_eval_int_rshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int = (int64_t)(((uint64_t)ret_val->r_int) >> shift.r_int);
}

void
exp_eval_int_arshift(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value shift;

	if (rt_eval(rt, &shift)) {
		*ret_val = shift;
		return;
	}

	ret_val->r_int >>= shift.r_int;
}

void
exp_eval_int_count(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	ret_val->r_int = (int64_t)cf_bit_count64((uint64_t)(ret_val->r_int));
}

void
exp_eval_int_lscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value is_set_val;

	if (rt_eval(rt, &is_set_val)) {
		*ret_val = is_set_val;
		return;
	}

	if (is_set_val.r_trilean == AS_EXP_FALSE) {
		ret_val->r_int = ~ret_val->r_int;
	}

	ret_val->r_int = (int64_t)cf_msb64((uint64_t)(ret_val->r_int));

	if (ret_val->r_int == 64) {
		ret_val->r_int = -1;
	}
}

void
exp_eval_int_rscan(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value is_set_val;

	if (rt_eval(rt, &is_set_val)) {
		*ret_val = is_set_val;
		return;
	}

	if (is_set_val.r_trilean == AS_EXP_FALSE) {
		ret_val->r_int = ~ret_val->r_int;
	}

	ret_val->r_int = 63 - (int64_t)cf_lsb64((uint64_t)(ret_val->r_int));
}

void
exp_eval_min(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			if (ret_val->r_float > arg.r_float) {
				ret_val->r_float = arg.r_float;
			}
		}
		else {
			if (ret_val->r_int > arg.r_int) {
				ret_val->r_int = arg.r_int;
			}
		}
	}
}

void
exp_eval_max(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	bool is_float = ret_val->type == RT_FLOAT;

	while (rt->op_ix < ob->instr_end_ix) {
		rt_value arg;

		if (rt_eval(rt, &arg)) {
			*ret_val = arg;
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		if (is_float) {
			if (ret_val->r_float < arg.r_float) {
				ret_val->r_float = arg.r_float;
			}
		}
		else {
			if (ret_val->r_int < arg.r_int) {
				ret_val->r_int = arg.r_int;
			}
		}
	}
}

void
exp_eval_meta_digest_mod(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	op_meta_digest_modulo* op = (op_meta_digest_modulo*)ob;
	uint32_t val = *(uint32_t*)&rt->ctx->r->keyd.digest[16];

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)val % op->mod;
}

// Deprecated - replaced with exp_eval_meta_record_size().
void
exp_eval_meta_device_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = as_namespace_is_memory_only(rt->ctx->ns)
			? 0
			: (int64_t)as_record_stored_size(rt->ctx->r);
}

void
exp_eval_meta_last_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)cf_utc_ns_from_clepoch_ms(rt->ctx->r->last_update_time);
}

void
exp_eval_meta_since_update(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	uint64_t now = cf_clepoch_milliseconds();
	uint64_t lut = rt->ctx->r->last_update_time;

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)(now > lut ? now - lut : 0);
}

void
exp_eval_meta_void_time(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = (rt->ctx->r->void_time == 0)
			? -1
			: (int64_t)cf_utc_ns_from_clepoch_sec(rt->ctx->r->void_time);
}

void
exp_eval_meta_ttl(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int =
			(int64_t)(int32_t)cf_server_void_time_to_ttl(rt->ctx->r->void_time);
}

void
exp_eval_meta_set_name(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	uint16_t set_id = as_index_get_set_id(rt->ctx->r);

	if (set_id == 0) {
		ret_val->type = RT_STR;
		ret_val->r_bytes.contents = EMPTY_STRING;
		ret_val->r_bytes.sz = 0;
		return;
	}

	as_set* set = as_namespace_get_set_by_id(rt->ctx->ns, set_id);

	ret_val->type = RT_STR;
	ret_val->r_bytes.contents = (const uint8_t*)set->name;
	ret_val->r_bytes.sz = (uint32_t)strlen(set->name);
}

void
exp_eval_meta_key_exists(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean =
			(rt->ctx->r->key_stored == 0 ? AS_EXP_FALSE : AS_EXP_TRUE);
}

void
exp_eval_meta_is_tombstone(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean =
			(as_record_is_live(rt->ctx->r) ? AS_EXP_FALSE : AS_EXP_TRUE);
}

// Deprecated - replaced with exp_eval_meta_record_size().
void
exp_eval_meta_memory_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = rt->ctx->ns->storage_type == AS_STORAGE_ENGINE_MEMORY
			? (int64_t)as_record_stored_size(rt->ctx->r)
			: 0;
}

void
exp_eval_meta_record_size(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (! rt_has_ns(rt)) {
		*ret_val = rt_unk;
		return;
	}

	ret_val->type = RT_INT;
	ret_val->r_int = (int64_t)as_record_stored_size(rt->ctx->r);
}

void
exp_eval_rec_key(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// key() takes no operands, so its own index is rt->op_ix - 1.
	uint32_t self_ix = rt->op_ix - 1;

	if (! rt_has_rd(rt)) {
		*ret_val = rt_unk;
		rt_mark_decisive(rt, ret_val, self_ix); // Absent key() site
		return;
	}

	// No-key-stored / device-load-fail stays ABSENT (UNK) - the storage API
	// can't split "no key" (the common case) from a genuine load failure.
	// Only the corrupt-key arms below become ERROR.
	if (! as_storage_rd_load_key(rt->ctx->rd)) {
		*ret_val = rt_unk;
		rt_mark_decisive(rt, ret_val, self_ix); // Absent key() site
		return;
	}

	const uint8_t* key = rt->ctx->rd->key;
	uint32_t key_sz = rt->ctx->rd->key_size;

	cf_assert(key_sz != 0, AS_EXP, "key_size can be 0?");

	switch (key[0]) {
	case AS_PARTICLE_TYPE_INTEGER:
		if (key_sz != sizeof(int64_t) + 1) {
			// Corrupt stored key (a key was loaded but is malformed).
			cf_warning(AS_EXP,
					"exp_eval_rec_key - unexpected integer key size %u", key_sz);
			rt_set_error(ret_val, self_ix, EXP_ERR_REC_KEY);
			return;
		}

		ret_val->type = RT_INT;
		ret_val->r_int = (int64_t)cf_swap_from_be64(*(uint64_t*)(key + 1));
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
		ret_val->type = (key[0] == AS_PARTICLE_TYPE_STRING) ? RT_STR : RT_BLOB;
		ret_val->r_bytes.contents = key + 1;
		ret_val->r_bytes.sz = key_sz - 1;
		break;
	default:
		// Corrupt stored key - invalid type byte.
		cf_warning(AS_EXP, "exp_eval_rec_key - invalid key type %u", key[0]);
		rt_set_error(ret_val, self_ix, EXP_ERR_REC_KEY);
		return;
	}

	if (ob->type != ret_val->type) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		// Wrong-typed key() is an absent site. The r_bytes.sz written above
		// aliases err_op_ix, so stamp this op's index rather than leave that
		// data-controlled value for the explainer.
		rt_mark_decisive(rt, ret_val, self_ix);
	}
}

void
exp_eval_bin(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index - in explain mode the absent / wrong-typed arms
	// below tag it so the explainer can point at the bin itself.
	uint32_t self_ix = rt->op_ix - 1;

	if (! rt_has_rd(rt)) {
		*ret_val = rt_unk;
		rt_mark_decisive(rt, ret_val, self_ix);
		return;
	}

	op_var* op = (op_var*)ob;

	// op->idx is a build-time bin slot index (see build_internal).
	cf_assert(op->idx < rt->bin_table->n_bins, AS_EXP,
			"bin idx %u out of range %u", op->idx, rt->bin_table->n_bins);

	rt_value* brt = &rt->vars[op->idx];

	if (brt->type == RT_END) {
		rt_load_bin(rt, op->idx, brt);
	}

	// Borrow the cached value; do_not_destroy carries to the copy so
	// rt_release_bins frees it. The per-reference type check is on the copy
	// and never poisons the shared slot.
	*ret_val = *brt;

	if (exp_rtype_to_particle_type(op->base.rtype) !=
			rt_value_particle_type(brt)) {
		*ret_val = rt_unk;
		// Absent / wrong-typed bin - tag it decisive so the explainer points
		// at the bin itself.
		rt_mark_decisive(rt, ret_val, self_ix);
	}
}

void
exp_eval_bin_type(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	uint32_t self_ix = rt->op_ix - 1; // bin_type() has no eval operands

	if (! rt_has_rd(rt)) {
		*ret_val = rt_unk;
		rt_mark_decisive(rt, ret_val, self_ix); // Absent site.
		return;
	}

	op_var* op = (op_var*)ob;

	// op->idx is a build-time bin slot index (see build_internal).
	cf_assert(op->idx < rt->bin_table->n_bins, AS_EXP,
			"bin idx %u out of range %u", op->idx, rt->bin_table->n_bins);

	rt_value* brt = &rt->vars[op->idx];

	if (brt->type == RT_END) {
		rt_load_bin(rt, op->idx, brt);
	}

	// Absent bin reports AS_PARTICLE_TYPE_NULL, not unknown -- preserves
	// bin-type existence-check semantics.
	ret_val->type = RT_INT;
	ret_val->r_int = (uint64_t)rt_value_particle_type(brt);
}

void
exp_eval_bin_exists(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt->ctx->rd == NULL) {
		*ret_val = rt_unk;
		return;
	}

	op_var* op = (op_var*)ob;

	// op->idx is a build-time bin slot index (see build_internal).
	cf_assert(op->idx < rt->bin_table->n_bins, AS_EXP,
			"bin idx %u out of range %u", op->idx, rt->bin_table->n_bins);

	rt_value* brt = &rt->vars[op->idx];

	if (brt->type == RT_END) {
		rt_load_bin(rt, op->idx, brt);
	}

	// An absent bin loads as unknown, which reports particle type NULL.
	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = rt_value_particle_type(brt) != AS_PARTICLE_TYPE_NULL
			? AS_EXP_TRUE
			: AS_EXP_FALSE;
}

void
exp_eval_result_remove(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	ret_val->type = RT_RESULT_REMOVE;
}

static void
eval_map_keys_or_values(runtime* rt, const op_base_mem* ob, rt_value* ret_val,
		bool is_values)
{
	(void)ob;

	// This op's own index, before the operand rt_evals advance op_ix.
	uint32_t self_ix = rt->op_ix - 1;
	rt_value arg;

	// Copy a short-circuit operand verbatim so a propagated ERROR survives.
	if (rt_eval(rt, &arg)) {
		*ret_val = arg;
		return;
	}

	defer_rt_value_destroy(arg);

	rt_value rt_map;

	// Arg is a definite value here - an untranslatable, non-msgpack, or
	// non-map operand is a genuine fault, not an absent operand.
	if (! rt_value_bin_translate(&rt_map, &arg)) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	if (rt_map.type != RT_MSGPACK ||
			msgpack_buf_peek_type(rt_map.r_bytes.contents, rt_map.r_bytes.sz) !=
					MSGPACK_TYPE_MAP) {
		rt_set_error(ret_val, self_ix, EXP_ERR_NOT_A_MAP);
		return;
	}

	// The one op that lifts literal bytes into a particle, so the marker has
	// to stop here - past this the value is an as_bin and the flag is gone.
	//
	// Keys need no such gate, but for two different reasons: a wire literal's
	// key type is refused at build, and an AEL literal's key cannot parse a
	// marker at all - the map_key grammar has no case for one. A grammar
	// change would leave the keys arm with no runtime backstop.
	if (is_values && rt_map.r_bytes.has_nonstorage) {
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		return;
	}

	as_bin rb;
	define_rollback_alloc(alloc, NULL, 1);

	cdt_result_data result = {
		.alloc = alloc,
		.type = is_values ? RESULT_TYPE_VALUE : RESULT_TYPE_KEY,
		.result = &rb,
		.is_multi = true,
	};

	if (! map_buf_get_all_k_or_v(rt_map.r_bytes.contents, rt_map.r_bytes.sz,
				&result)) {
		// Malformed stored map - a genuine fault.
		rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
		rollback_alloc_rollback(alloc);
		return;
	}

	*ret_val = (rt_value){
		.type = RT_BIN,
		.r_bin = rb,
	};
}

void
exp_eval_map_keys(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	eval_map_keys_or_values(rt, ob, ret_val, false);
}

void
exp_eval_map_values(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	eval_map_keys_or_values(rt, ob, ret_val, true);
}

void
exp_eval_cond(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_cond* op = (op_cond*)ob;

	for (uint32_t i = 0; i < op->case_count; i++) {
		if (rt_eval(rt, ret_val)) {
			rt_skip(rt, ob->instr_end_ix);
			return;
		}
		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected type %d",
				ret_val->type);

		op_base_mem* op_case = (op_base_mem*)rt->instr_ptr;

		cf_assert(op_case->code == EXP_VOP_COND_CASE, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			rt_skip(rt, rt->op_ix + 1);
			rt_eval(rt, ret_val);
			rt_skip(rt, ob->instr_end_ix);
			return;
		}

		rt_skip(rt, op_case->instr_end_ix);
	}

	rt_eval(rt, ret_val);
}

void
exp_eval_var_builtin(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_var* op = (op_var*)ob;

	*ret_val = rt->vars_builtin[op->idx];

	if (! rt_is_type(ret_val, op->base.rtype)) {
		*ret_val = rt_unk;
		// A wrong-typed / absent builtin var is an absent site - attribute
		// it to itself.
		rt_mark_decisive(rt, ret_val, rt->op_ix - 1);
	}
}

void
exp_eval_var(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_var* op = (op_var*)ob;

	*ret_val = rt->vars[op->idx];
	ret_val->do_not_destroy = 1;
}

void
exp_eval_let(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	op_let* op = (op_let*)ob;

	for (uint32_t i = 0; i < op->n_vars; i++) {
		rt_eval(rt, &rt->vars[op->var_idx + i]);
	}

	rt_eval(rt, ret_val);

	for (uint32_t i = 0; i < op->n_vars; i++) {
		rt_value* val = &rt->vars[op->var_idx + i];

		if (ret_val->type == RT_BIN && val->type == RT_BIN &&
				ret_val->r_bin.particle == val->r_bin.particle) {
			ret_val->do_not_destroy = 0;
			continue;
		}

		if (ret_val->type == RT_GEO_COMPILED && val->type == RT_GEO_COMPILED &&
				ret_val->r_geo.region == val->r_geo.region) {
			ret_val->do_not_destroy = 0;
			continue;
		}

		rt_value_destroy(val);
	}
}

void
exp_eval_call(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	// This op's own index, before the operand rt_evals advance op_ix. Named
	// distinctly from the local op_ix loop counter below (which walks
	// op->vecs, not the runtime op stream).
	uint32_t self_ix = rt->op_ix - 1;

	const op_call* op = (const op_call*)ob;
	define_deferred_array(vecs, msgpack_vec, op->n_vecs);
	msgpack_in_vec mv = { .n_vecs = op->n_vecs, .vecs = vecs };

	vecs[0].buf = op->vecs[0].buf;
	vecs[0].buf_sz = op->vecs[0].buf_sz;
	vecs[0].offset = 0;

	uint32_t vec_ix = 1;
	uint32_t op_ix = 1;
	uint8_t buf[1024];

	as_packer pk = { .buffer = buf, .capacity = sizeof(buf) };

	uint32_t param_idx = 0;
	define_deferred_array(param_ret_vals, rt_value, op->eval_count);
	define_call_cleanup(bin_cleanup, op->eval_count);
	define_rollback_alloc(alloc, NULL, op->eval_count);
	DEFER_ROLLBACK_ALLOC(alloc);

	for (; op_ix < op->n_vecs; op_ix++) {
		if (op->vecs[op_ix].buf == exp_call_eval_token) {
			rt_value* from = &param_ret_vals[param_idx++];

			rt_eval(rt, from);

			// A short-circuit param propagates verbatim - preserving a
			// child's UNK or ERROR. Only a genuine pack failure of a definite
			// value is a fault.
			if (rt_value_is_short_circuit(from)) {
				*ret_val = *from;
				rt_skip(rt, ob->instr_end_ix);
				return;
			}

			vecs[vec_ix].buf = pk.buffer + pk.offset;
			vecs[vec_ix].offset = 0;

			if (from->type == RT_BIN && ! from->do_not_destroy) {
				call_cleanup_add(bin_cleanup, &from->r_bin);
			}

			if (! rt_value_to_msgpack_vec(&pk, &vecs[vec_ix], alloc, from)) {
				rt_set_error(ret_val, self_ix, EXP_ERR_CALL_ARG);
				rt_skip(rt, ob->instr_end_ix);
				return;
			}
		}
		else {
			vecs[vec_ix].buf = op->vecs[op_ix].buf;
			vecs[vec_ix].buf_sz = op->vecs[op_ix].buf_sz;
			vecs[vec_ix].offset = 0;
		}

		vec_ix++;
	}

	as_bin* b;
	rt_value bin_arg;
	as_bin old;
	bool is_modify_local = ((op->system_type & EXP_CALL_FLAG_MODIFY_LOCAL) != 0);

	rt_eval(rt, &bin_arg);

	switch (bin_arg.type) {
	case RT_BIN_PTR: // from bin
		if (is_modify_local) {
			old = *bin_arg.r_bin_p;
			b = &old; // must not modify bin_arg.r_bin_p
		}
		else {
			b = bin_arg.r_bin_p;
		}
		break;
	case RT_NIL:
		as_bin_set_empty(&bin_arg.r_bin);
		b = &bin_arg.r_bin;
		old = *b;
		break;
	case RT_BIN: // from call
		b = &bin_arg.r_bin; // particle on heap
		old = *b;
		break;
	case RT_BLOB:
	case RT_HLL: { // from client
		rt_value temp = bin_arg;

		b = &bin_arg.r_bin;
		bin_arg.type = RT_BIN;
		bin_arg.do_not_destroy = 0;
		bin_arg.r_bin.particle = rt_alloc_mem(rt,
				(size_t)temp.r_bytes.sz + sizeof(cdt_mem), NULL);

		cdt_mem* p_cdt_mem = (cdt_mem*)bin_arg.r_bin.particle;

		p_cdt_mem->type = temp.type == RT_BLOB ? AS_PARTICLE_TYPE_BLOB
											   : AS_PARTICLE_TYPE_HLL;
		as_bin_state_set_from_type(&bin_arg.r_bin, p_cdt_mem->type);
		p_cdt_mem->sz = temp.r_bytes.sz;
		memcpy(p_cdt_mem->data, temp.r_bytes.contents, p_cdt_mem->sz);

		old = *b;
		rt_value_destroy(&temp);
		break;
	}
	case RT_MSGPACK: {
		rt_value temp = bin_arg;

		b = &bin_arg.r_bin;
		bin_arg.type = RT_BIN;
		bin_arg.do_not_destroy = 0;

		if (! msgpack_to_bin(rt, &bin_arg.r_bin, &temp, NULL)) {
			// A structurally invalid msgpack bin-arg is a fault.
			rt_value_destroy(&temp);
			rt_set_error(ret_val, self_ix, EXP_ERR_BAD_MSGPACK);
			return;
		}

		old = *b;
		rt_value_destroy(&temp);

		break;
	}
	// rt_eval may return scalar rt_values (not RT_BIN). Downstream exp_eval_call
	// handlers (the string ops) take an as_bin, dispatching on particle type,
	// so materialize scalars into bin_arg.r_bin.
	case RT_STR: {
		// Invalid UTF-8 is the only reject; a valid scalar string is
		// materialized for reads and modify-local alike, so the op runs on
		// this transient bin as it would on a from-call RT_BIN receiver.
		if (! cf_str_is_valid_utf8(bin_arg.r_bytes.contents, bin_arg.r_bytes.sz)) {
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
			// Absent call result - stamp this call op rather than leave the
			// aliased result bytes in err_op_ix.
			rt_mark_decisive(rt, ret_val, self_ix);
			call_cleanup_fn(&bin_cleanup);
			return;
		}

		rt_value temp = bin_arg;

		b = &bin_arg.r_bin;
		bin_arg.type = RT_BIN;
		bin_arg.do_not_destroy = 0;
		bin_arg.r_bin.particle = rt_alloc_mem(rt,
				(size_t)temp.r_bytes.sz + sizeof(cdt_mem), NULL);

		cdt_mem* p_cdt_mem = (cdt_mem*)bin_arg.r_bin.particle;

		p_cdt_mem->type = AS_PARTICLE_TYPE_STRING;
		as_bin_state_set_from_type(&bin_arg.r_bin, AS_PARTICLE_TYPE_STRING);
		p_cdt_mem->sz = temp.r_bytes.sz;
		memcpy(p_cdt_mem->data, temp.r_bytes.contents, p_cdt_mem->sz);

		old = *b;
		break;
	}
	case RT_INT:
		b = &bin_arg.r_bin;
		b->particle = (as_particle*)(uint64_t)bin_arg.r_int;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_INTEGER);
		old = *b;
		break;
	case RT_FLOAT: {
		b = &bin_arg.r_bin;
		double fval = bin_arg.r_float;
		memcpy(&b->particle, &fval, sizeof(double));
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_FLOAT);
		old = *b;
		break;
	}
	case RT_TRILEAN:
		// Propagate a short-circuit bin-arg (UNK or ERROR) verbatim.
		if (rt_value_is_short_circuit(&bin_arg)) {
			*ret_val = bin_arg;
			call_cleanup_fn(&bin_cleanup);
			return;
		}
		b = &bin_arg.r_bin;
		b->particle =
				(as_particle*)(uint64_t)(bin_arg.r_trilean == AS_EXP_TRUE ? 1
																		  : 0);
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_BOOL);
		old = *b;
		break;
	default:
		// An unexpected bin-arg type is a genuine fault.
		rt_set_error(ret_val, self_ix, EXP_ERR_CALL_ARG);
		return;
	}

	as_bin rb = { 0 };
	int ret = AS_OK;

	switch (op->system_type & (uint32_t)~EXP_CALL_FLAG_MODIFY_LOCAL) {
	case EXP_CALL_CDT:
		if (is_modify_local) {
			ret = as_bin_cdt_modify_exp(b, &mv, &rb);
		}
		else {
			ret = as_bin_cdt_read_exp(b, &mv, &rb);
		}
		break;
	case EXP_CALL_BITS:
		if (is_modify_local) {
			ret = as_bin_bits_modify_exp(b, &mv);
		}
		else {
			ret = as_bin_bits_read_exp(b, &mv, &rb);
		}
		break;
	case EXP_CALL_HLL:
		if (is_modify_local) {
			ret = as_bin_hll_modify_exp(b, &mv, &rb);
		}
		else {
			ret = as_bin_hll_read_exp(b, &mv, &rb);
		}
		break;
	case EXP_CALL_STRING:
		if (is_modify_local) {
			ret = as_bin_string_modify_exp(b, &mv);
		}
		else {
			ret = as_bin_string_read_exp(b, &mv, &rb);
		}
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}

	call_cleanup_fn(&bin_cleanup);

	if (ret != AS_OK) {
		// The CDT/bits/HLL/string sub-op failed - a fault. The sub-op may
		// have armed a more specific detail; first-set-wins keeps it.
		//
		rt_value_destroy(&bin_arg);
		rt_set_error(ret_val, self_ix, EXP_ERR_CALL_ARG);
		return;
	}

	if (is_modify_local) {
		if (rt_value_need_destroy(&bin_arg, &old, b)) {
			as_bin_particle_destroy(&old);
		}

		as_bin_particle_destroy(&rb);
	}
	else {
		rt_value_destroy(&bin_arg);
		b = &rb;
	}

	if (! bin_is_type(b, op->type)) {
		if (is_modify_local) {
			bool live_noop = bin_arg.type == RT_BIN_PTR &&
					bin_arg.r_bin_p->particle == b->particle;
			bool borrowed_unchanged = bin_arg.type == RT_BIN &&
					bin_arg.do_not_destroy && old.particle == b->particle;

			if (! live_noop && ! borrowed_unchanged) {
				as_bin_particle_destroy(b);
			}
		}
		else {
			const as_particle* bin_arg_p = bin_arg.type == RT_BIN_PTR
					? bin_arg.r_bin_p->particle
					: bin_arg.r_bin.particle;

			if (b->particle != bin_arg_p) { // not a no-op
				as_bin_particle_destroy(b);
			}
		}

		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		// Result-type mismatch is an absent call site - stamp this call op
		// rather than leave the aliased result in err_op_ix.
		rt_mark_decisive(rt, ret_val, self_ix);
		return;
	}

	if (is_modify_local) {
		if (bin_arg.type == RT_BIN_PTR &&
				bin_arg.r_bin_p->particle == b->particle) { // no-op case from bin
			ret_val->type = RT_BIN_PTR;
			ret_val->r_bin_p = bin_arg.r_bin_p;
			return;
		}

		ret_val->type = RT_BIN;
		ret_val->r_bin = *b;
		// Keep the borrowed (slot-owned) status only if the modify left the
		// particle unchanged; a reallocated result is owned by this value.
		// Only RT_BIN has a snapshot to compare against -- a bin reached by
		// pointer aliases old, so the test would always hold there and a
		// reallocated result would be kept borrowed and never freed.
		ret_val->do_not_destroy = bin_arg.type == RT_BIN &&
				bin_arg.do_not_destroy && old.particle == b->particle;
		return;
	}

	uint8_t type = as_bin_get_particle_type(b);

	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		ret_val->type = RT_NIL;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		ret_val->type = RT_INT;
		ret_val->r_int = as_bin_particle_integer_value(b);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = as_bin_particle_float_value(b);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = as_bin_particle_bool_value(b);
		as_bin_particle_destroy(b);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		if (! as_bin_cdt_context_geojson_parse(b)) {
			// The sub-op produced invalid geojson - a genuine fault.
			rt_set_error(ret_val, self_ix, EXP_ERR_GEOJSON);
			as_bin_particle_destroy(b);
			return;
		}

		// no break
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
		ret_val->type = RT_BIN;
		ret_val->r_bin = *b;
		// Don't suspect this is exploitable, added for future proofing.
		ret_val->do_not_destroy = rt_value_keep_do_not_destroy(&bin_arg, b);
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

void
exp_eval_value(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)rt;

	switch (ob->code) {
	case EXP_VOP_VALUE_NIL:
		ret_val->type = RT_NIL;
		break;
	case EXP_VOP_VALUE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = ((op_value_bool*)ob)->value ? AS_EXP_TRUE
														 : AS_EXP_FALSE;
		break;
	case EXP_VOP_VALUE_INT:
		ret_val->type = RT_INT;
		ret_val->r_int = ((op_value_int*)ob)->value;
		break;
	case EXP_VOP_VALUE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = ((op_value_float*)ob)->value;
		break;
	case EXP_VOP_VALUE_GEO:
		ret_val->type = RT_GEO_CONST;
		ret_val->r_geo_const.op = (op_value_geo*)ob;
		break;
	case EXP_VOP_VALUE_STR:
		ret_val->type = RT_STR;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case EXP_VOP_VALUE_BLOB:
		ret_val->type = RT_BLOB;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case EXP_VOP_VALUE_HLL:
		ret_val->type = RT_HLL;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		break;
	case EXP_VOP_VALUE_LIST:
	case EXP_VOP_VALUE_MSGPACK:
		ret_val->type = RT_MSGPACK;
		ret_val->r_bytes.contents = ((op_value_blob*)ob)->value;
		ret_val->r_bytes.sz = ((op_value_blob*)ob)->value_sz;
		ret_val->r_bytes.has_nonstorage = ((op_value_blob*)ob)->has_nonstorage;
		break;
	default:
		cf_crash(AS_EXP, "unexpected code %u", ob->code);
	}
}

//==========================================================
// Local helpers - runtime utilities.
//

static void
rt_value_bin_ptr_to_bin(runtime* rt, as_bin* rb, const rt_value* from,
		cf_ll_buf* ll_buf)
{
	as_bin b = *from->r_bin_p;
	uint8_t type = as_bin_get_particle_type(&b);

	rb->state = b.state;

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
		rb->particle = b.particle;
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_GEOJSON:;
		uint32_t sz = sizeof(cdt_mem) + ((cdt_mem*)b.particle)->sz;

		rb->particle = rt_alloc_mem(rt, sz, ll_buf);
		memcpy(rb->particle, b.particle, sz);
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %u", type);
	}
}

static void
json_to_rt_geo(const uint8_t* json, size_t jsonsz, rt_value* val)
{
	uint64_t cellid;
	geo_region_t region;

	*val = (rt_value){ 0 };

	if (! as_geojson_parse(NULL, (const char*)json, jsonsz, &cellid, &region)) {
		// Storage validates geojson on write, so a parse failure here means
		// on-disk corruption - a fault, not an absent operand. Emit the ERROR
		// sentinel; eval_compare stamps the op_ix + reason.
		val->type = RT_TRILEAN;
		val->r_trilean = AS_EXP_ERROR;
		return;
	}

	val->type = RT_GEO_COMPILED;

	if (region != NULL) {
		val->r_geo.type = GEO_REGION_NEED_FREE;
		val->r_geo.region = region;
	}
	else {
		val->r_geo.type = GEO_CELL;
		val->r_geo.cellid = cellid;
	}
}

static void
particle_to_rt_geo(const as_particle* p, rt_value* val)
{
	size_t sz;
	const uint8_t* ptr = (const uint8_t*)as_geojson_mem_jsonstr(p, &sz);

	json_to_rt_geo(ptr, sz, val);
}

as_particle_type
exp_rtype_to_particle_type(exp_rtype type)
{
	switch (type) {
	case EXP_RTYPE_NIL:
		return AS_PARTICLE_TYPE_NULL;
	case EXP_RTYPE_TRILEAN:
		return AS_PARTICLE_TYPE_BOOL;
	case EXP_RTYPE_INT:
		return AS_PARTICLE_TYPE_INTEGER;
	case EXP_RTYPE_FLOAT:
		return AS_PARTICLE_TYPE_FLOAT;
	case EXP_RTYPE_STR:
		return AS_PARTICLE_TYPE_STRING;
	case EXP_RTYPE_BLOB:
		return AS_PARTICLE_TYPE_BLOB;
	case EXP_RTYPE_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case EXP_RTYPE_HLL:
		return AS_PARTICLE_TYPE_HLL;
	case EXP_RTYPE_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case EXP_RTYPE_MAP:
		return AS_PARTICLE_TYPE_MAP;
	default:
		cf_crash(AS_EXP, "unexpected type %u", type);
	}
	return AS_PARTICLE_TYPE_NULL; // never reached
}

static bool
bin_is_type(const as_bin* b, exp_rtype type)
{
	return as_bin_get_particle_type(b) == exp_rtype_to_particle_type(type);
}

// Inverse of exp_rtype_to_particle_type() for the rt_value forms rt_load_bin
// caches; unknown/absent reports NULL.
static as_particle_type
rt_value_particle_type(const rt_value* v)
{
	switch (v->type) {
	case RT_INT:
		return AS_PARTICLE_TYPE_INTEGER;
	case RT_FLOAT:
		return AS_PARTICLE_TYPE_FLOAT;
	case RT_TRILEAN:
		if (v->r_trilean == AS_EXP_UNK) {
			return AS_PARTICLE_TYPE_NULL;
		}

		return AS_PARTICLE_TYPE_BOOL;
	case RT_BIN:
		return as_bin_get_particle_type(&v->r_bin);
	case RT_BIN_PTR:
		return as_bin_get_particle_type(v->r_bin_p);
	default:
		return AS_PARTICLE_TYPE_NULL;
	}
}

static void
rt_skip(runtime* rt, uint32_t instr_end_ix)
{
	while (rt->op_ix < instr_end_ix) {
		op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
		const op_table_entry* entry = &op_table[ob->code];

		rt->op_ix++;
		rt->instr_ptr += entry->size;
	}
}

static void
rt_value_translate(rt_value* to, const rt_value* from)
{
	const as_bin* bin;

	*to = (rt_value){ 0 };

	switch (from->type) {
	case RT_BIN:
		bin = &from->r_bin;
		break;
	case RT_BIN_PTR:
		bin = from->r_bin_p;
		break;
	case RT_GEO_CONST:
		to->type = RT_GEO_COMPILED;
		to->r_geo = from->r_geo_const.op->compiled;
		return;
	case RT_HLL:
		// Not comparable; yield unknown rather than fall through to the
		// compare switch, which has no HLL case.
		to->type = RT_TRILEAN;
		to->r_trilean = AS_EXP_UNK;
		return;
	default:
		*to = *from;

		if (to->type == RT_GEO_COMPILED &&
				to->r_geo.type == GEO_REGION_NEED_FREE) {
			to->r_geo.type = GEO_REGION;
		}

		return;
	}

	uint8_t type = as_bin_get_particle_type(bin);

	switch (type) {
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_STRING: {
		char* ptr;

		to->type = (type == AS_PARTICLE_TYPE_BLOB ? RT_BLOB : RT_STR);
		to->r_bytes.sz = as_bin_particle_string_ptr(bin, &ptr);
		to->r_bytes.contents = (uint8_t*)ptr;

		break;
	}
	case AS_PARTICLE_TYPE_GEOJSON:
		particle_to_rt_geo(bin->particle, to);
		break;
	case AS_PARTICLE_TYPE_LIST: {
		cdt_payload val;

		as_bin_particle_list_get_packed_val(bin, &val);
		to->type = RT_MSGPACK;
		to->r_bytes.sz = val.sz;
		to->r_bytes.contents = val.ptr;
		break;
	}
	case AS_PARTICLE_TYPE_MAP: {
		cdt_payload val;

		as_bin_particle_map_get_packed_val(bin, &val);
		to->type = RT_MSGPACK;
		to->r_bytes.sz = val.sz;
		to->r_bytes.contents = val.ptr;
		break;
	}
	case AS_PARTICLE_TYPE_INTEGER:
		to->type = RT_INT;
		to->r_int = as_bin_particle_integer_value(bin);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		to->type = RT_FLOAT;
		to->r_float = as_bin_particle_float_value(bin);
		break;
	case AS_PARTICLE_TYPE_HLL:
	default:
		// Not comparable; yield unknown rather than crash on a
		// wire-reachable operand.
		to->type = RT_TRILEAN;
		to->r_trilean = AS_EXP_UNK;
		break;
	}
}

static void
rt_defer_value_destroy(rt_value** p_val)
{
	rt_value_destroy(*p_val);
}

static void
rt_value_destroy(rt_value* val)
{
	if (val == NULL) {
		return;
	}

	if (val->do_not_destroy != 0) {
		return;
	}

	if (val->type == RT_GEO_COMPILED && val->r_geo.type == GEO_REGION_NEED_FREE) {
		geo_region_destroy(val->r_geo.region);
	}
	else if (val->type == RT_BIN) {
		as_bin_particle_destroy(&val->r_bin);
	}
}

// Free the per-bin slots after eval. Only masked or canonicalized object bins
// hold a particle; they're borrowed views (do_not_destroy=1) and this is their
// sole freer. If the result aliases a slot's particle, hand ownership to the
// result instead so it is freed exactly once.
static void
rt_release_bins(runtime* rt, rt_value* ret_val)
{
	for (uint32_t i = 0; i < rt->bin_table->n_bins; i++) {
		rt_value* val = &rt->vars[i];

		if (ret_val->type == RT_BIN && val->type == RT_BIN &&
				ret_val->r_bin.particle == val->r_bin.particle) {
			ret_val->do_not_destroy = 0;
			continue;
		}

		val->do_not_destroy = 0;
		rt_value_destroy(val);
	}
}

static void
rt_value_get_geo(rt_value* val, geo_data* result)
{
	cf_assert(val->type == RT_GEO_COMPILED, AS_EXP, "unexpected");

	if (val->r_geo.type == GEO_CELL) {
		result->cellid = val->r_geo.cellid;
		result->region = NULL;
	}
	else {
		result->cellid = 0;
		result->region = val->r_geo.region;
	}
}

static void
rt_bin_translate(rt_value* bin, rt_value* ret_val)
{
	cf_assert(bin->type == RT_BIN_PTR, AS_EXP, "unexpected");

	as_bin* b = bin->r_bin_p;
	as_particle_type type = as_bin_get_particle_type(b);

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
		ret_val->type = RT_INT;
		ret_val->r_int = as_bin_particle_integer_value(b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		ret_val->type = RT_FLOAT;
		ret_val->r_float = as_bin_particle_float_value(b);
		break;
	case AS_PARTICLE_TYPE_BOOL:
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = as_bin_particle_bool_value(b);
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_GEOJSON:
		ret_val->type = RT_BIN_PTR;
		ret_val->r_bin_p = b;
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}
}

// A stored CDT is never re-validated on read, so a bin's keys may not be in
// canonical order - and comparison relies on actual key order. Such a bin is
// rewritten into an exp-owned copy. Records hold them from two eras: particles
// predating sort-on-write (7.0), and map results an expression write op stored
// in the order their op selected.
//
// Once per bin slot per evaluation - the memo is the eval's own var array, so
// a record evaluated twice walks twice, as an expression sindex does on every
// write. Almost every bin is already canonical, but proving that walks the
// whole buffer: the cheap outcome is the absent copy, not absent work, and a
// load wanting only the particle type pays for it.
static void
rt_bin_canonicalize(rt_value* val)
{
	if (val->type != RT_BIN && val->type != RT_BIN_PTR) {
		return;
	}

	const as_bin* b = val->type == RT_BIN ? &val->r_bin : val->r_bin_p;
	as_particle_type type = as_bin_get_particle_type(b);
	cdt_payload packed;

	if (type == AS_PARTICLE_TYPE_MAP) {
		as_bin_particle_map_get_packed_val(b, &packed);
	}
	else if (type == AS_PARTICLE_TYPE_LIST) {
		as_bin_particle_list_get_packed_val(b, &packed);
	}
	else {
		return;
	}

	// toplvl_index keeps a persist index the source carried, matching how the
	// bin was sized and built on the way in. It matters because the copy can
	// outlive the eval - a returned bin's particle becomes the caller's - so
	// dropping the index would let a bin be persisted without the one it was
	// stored with. The copy is then not bounded by the stored size.
	//
	// trust_order_flags because a stored bin is exactly the source that
	// qualifies - and verifying instead costs a descent per level on a nested
	// ORDERED list, on a read.
	define_untrusted_info(info, packed.ptr, packed.sz, .toplvl_index = true,
			.trust_order_flags = true);

	// Not only malformation: ERR_DUP_KEY and ERR_ORDER are structurally valid,
	// storable maps - the relic shapes this function exists to repair. They
	// can't be repaired, so eval compares them on their stored bytes and still
	// reaches a verdict.
	if (! cdt_untrusted_check(&info)) {
		cf_detail(AS_EXP, "rt_bin_canonicalize - %s",
				cdt_untrusted_err_msg(info.err));
		return;
	}

	// Key order is owed to comparison, which reads keys in order, and to
	// storage. Not to a client - a collection op's result is routinely
	// unordered. The rest of byte canonicality is size: a relic's wide headers
	// and persist index pass through as stored.
	//
	// A declared preserved order is not repaired here: sorting it away would
	// make a comparison's verdict turn on whether the keys happened to be
	// sorted already, and would strip the marker the write path refuses on.
	if (cdt_untrusted_fully_checked(&info) ||
			map_buf_is_preserve_order(packed.ptr, packed.sz)) {
		return; // keep the zero-copy view - no scratch, no copy
	}

	define_deferred_memory(canon, info.sz);

	if (! cdt_untrusted_rewrite(canon, &info)) { // e.g. non-adjacent dup keys
		cf_detail(AS_EXP, "rt_bin_canonicalize - %s",
				cdt_untrusted_err_msg(info.err));
		return;
	}

	uint32_t canon_sz = info.sz;
	int32_t mem_sz = as_particle_size_from_msgpack(canon, canon_sz);

	cf_assert(mem_sz > 0, AS_EXP, "unexpected"); // bytes are freshly rewritten

	as_bin new_bin = { 0 };

	as_bin_particle_from_msgpack(&new_bin, canon, canon_sz,
			cf_malloc((size_t)mem_sz));

	if (val->type == RT_BIN) { // masked - swap out the slot-owned particle
		as_bin_particle_destroy(&val->r_bin);
	}

	val->type = RT_BIN;
	val->r_bin = new_bin;
	val->do_not_destroy = 1; // slot-owned, freed by rt_release_bins()
}

static void
rt_load_bin(runtime* rt, uint32_t idx, rt_value* ret_val)
{
	const uint8_t* name = rt_get_bin_name(rt, idx);
	size_t name_sz = rt->bin_table->table[idx].sz;
	as_bin* bin = as_bin_get_live_w_len(rt->ctx->rd, name, name_sz);

	if (bin == NULL) {
		*ret_val = rt_unk;
		return;
	}

	if (as_masking_apply(rt->ctx->rd->mask_ctx, &ret_val->r_bin, bin)) {
		// Reuse rt_bin_translate on the owned masked particle: scalars copy
		// their value out and we free the temporary here; objects keep the
		// particle (RT_BIN, freed by rt_release_bins).
		rt_value masked = { .type = RT_BIN_PTR, .r_bin_p = &ret_val->r_bin };
		rt_value translated = { 0 };

		rt_bin_translate(&masked, &translated);

		if (translated.type == RT_BIN_PTR) {
			ret_val->type = RT_BIN;
			ret_val->do_not_destroy = 1;
		}
		else {
			as_bin_particle_destroy(&ret_val->r_bin);
			*ret_val = translated;
		}
	}
	else {
		rt_value temp = { .type = RT_BIN_PTR, .r_bin_p = bin };

		rt_bin_translate(&temp, ret_val);
	}

	rt_bin_canonicalize(ret_val);
}

static bool
rt_is_type(const rt_value* v, exp_rtype type)
{
	switch (type) {
	case EXP_RTYPE_NIL:
		return v->type == RT_NIL;
	case EXP_RTYPE_TRILEAN:
		return v->type == RT_TRILEAN;
	case EXP_RTYPE_INT:
		return v->type == RT_INT;
	case EXP_RTYPE_STR:
		return v->type == RT_STR;
	case EXP_RTYPE_LIST:
		return (v->type == RT_MSGPACK)
				? msgpack_buf_peek_type(v->r_bytes.contents, v->r_bytes.sz) ==
						MSGPACK_TYPE_LIST
				: false;
	case EXP_RTYPE_MAP:
		return (v->type == RT_MSGPACK)
				? msgpack_buf_peek_type(v->r_bytes.contents, v->r_bytes.sz) ==
						MSGPACK_TYPE_MAP
				: false;
	case EXP_RTYPE_BLOB:
		return v->type == RT_BLOB;
	case EXP_RTYPE_FLOAT:
		return v->type == RT_FLOAT;
	case EXP_RTYPE_GEOJSON:
		return v->type == RT_GEO_STR || v->type == RT_GEO_CONST ||
				v->type == RT_GEO_COMPILED;
	case EXP_RTYPE_HLL:
		return v->type == RT_HLL;
	default:
		break;
	}

	return false;
}

static void
rt_init_builtin_vars(runtime* rt, op_value_geo* geo_mem)
{
	if (rt->ctx->vars_table == NULL) {
		return;
	}

	for (uint32_t i = 0; i < AS_EXP_BUILTIN_COUNT; i++) {
		msgpack_in* mp = rt->ctx->vars_table[i];
		rt_value* vp = &rt->vars_builtin[i];

		if (mp == NULL) {
			continue;
		}

		msgpack_type type = msgpack_peek_type(mp);

		if (i == AS_EXP_BUILTIN_INDEX && type != MSGPACK_TYPE_INT) {
			continue;
		}

		rt_value v = { .do_not_destroy = 1 };

		switch (type) {
		case MSGPACK_TYPE_NIL:
			vp->type = RT_NIL;
			break;
		case MSGPACK_TYPE_FALSE:
			vp->r_trilean = AS_EXP_FALSE;
			break;
		case MSGPACK_TYPE_TRUE:
			vp->r_trilean = AS_EXP_TRUE;
			break;
		case MSGPACK_TYPE_NEGINT:
		case MSGPACK_TYPE_INT:
			if (msgpack_get_int64(mp, &v.r_int)) {
				v.type = RT_INT;
				*vp = v;
			}

			break;
		case MSGPACK_TYPE_LIST:
		case MSGPACK_TYPE_MAP:
			v.r_bytes.contents = msgpack_get_ele(mp, &v.r_bytes.sz);

			if (v.r_bytes.contents != NULL) {
				v.type = RT_MSGPACK;
				*vp = v;
			}

			break;
		case MSGPACK_TYPE_STRING:
		case MSGPACK_TYPE_BYTES:
			v.r_bytes.contents = msgpack_get_bin(mp, &v.r_bytes.sz);

			if (v.r_bytes.contents == NULL || v.r_bytes.sz == 0) {
				break;
			}

			as_bytes_type btype = *v.r_bytes.contents++;
			v.r_bytes.sz--;

			switch (btype) {
			case AS_BYTES_BLOB:
				v.type = RT_BLOB;
				break;
			case AS_BYTES_STRING:
				v.type = RT_STR;
				break;
			case AS_BYTES_HLL:
				v.type = RT_HLL;
				break;
			default:
				continue;
			}

			*vp = v;
			break;
		case MSGPACK_TYPE_DOUBLE:
			if (msgpack_get_double(mp, &v.r_float)) {
				v.type = RT_FLOAT;
				*vp = v;
			}

			break;
		case MSGPACK_TYPE_GEOJSON:
			if (exp_geo_mp_to_op(mp, geo_mem, "rt_init_builtin_vars")) {
				v.type = RT_GEO_CONST;
				v.r_geo_const.op = geo_mem;
				*vp = v;
			}

			break;
		default:
			break;
		}
	}
}

//==========================================================
// Local helpers - runtime compare utilities.
//

static as_exp_trilean
cmp_nil(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	switch (code) {
	case EXP_CMP_EQ:
	case EXP_CMP_GE:
	case EXP_CMP_LE:
		return AS_EXP_TRUE;
	case EXP_CMP_NE:
	case EXP_CMP_GT:
	case EXP_CMP_LT:
		return AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_trilean(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_trilean == e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_trilean != e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_trilean > e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_trilean >= e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_trilean < e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_trilean <= e1->r_trilean) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_int(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_int == e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_int != e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_int > e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_int >= e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_int < e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_int <= e1->r_int) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static as_exp_trilean
cmp_float(exp_op_code code, const rt_value* e0, const rt_value* e1)
{
	switch (code) {
	case EXP_CMP_EQ:
		return (e0->r_float == e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (e0->r_float != e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (e0->r_float > e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (e0->r_float >= e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (e0->r_float < e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (e0->r_float <= e1->r_float) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

static const uint8_t*
rt_value_get_str(const rt_value* val, uint32_t* sz_r)
{
	if (val->type == RT_STR) {
		*sz_r = val->r_bytes.sz;
		return val->r_bytes.contents;
	}

	if (val->type == RT_BIN || val->type == RT_BIN_PTR) {
		const as_bin* bp = val->type == RT_BIN ? &val->r_bin : val->r_bin_p;

		if (as_bin_get_particle_type(bp) != AS_PARTICLE_TYPE_STRING) {
			return NULL;
		}

		char* p;

		*sz_r = as_bin_particle_string_ptr(bp, &p);

		return (const uint8_t*)p;
	}

	return NULL;
}

static as_exp_trilean
cmp_bytes(exp_op_code code, const rt_value* v0, const rt_value* v1)
{
	uint32_t s0_sz = v0->r_bytes.sz;
	uint32_t s1_sz = v1->r_bytes.sz;
	const uint8_t* s0 = v0->r_bytes.contents;
	const uint8_t* s1 = v1->r_bytes.contents;
	uint32_t min_sz = (s0_sz < s1_sz) ? s0_sz : s1_sz;
	int cmp = memcmp(s0, s1, min_sz);

	if (cmp == 0 && s0_sz != s1_sz) {
		cmp = (s0_sz < s1_sz) ? -1 : 1;
	}

	switch (code) {
	case EXP_CMP_EQ:
		return (cmp == 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (cmp != 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (cmp > 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (cmp >= 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (cmp < 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (cmp <= 0) ? AS_EXP_TRUE : AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

// Contents compare by actual byte order, so map operands must have their keys
// in canonical (sorted) order regardless of the K_ORDERED flag - guaranteed by
// sort-on-write for stored data, the relic repair at bin load for older bins,
// build/emit-time key sorts for literals, and, for a map built by a CDT call,
// the internal result type the expression entry points ask for.
//
// A PRESERVE_ORDER map is refused, not answered - it holds its op's selection
// order. Declaring no key order is a different thing, and is answered.
//
// Only the top level is tested, the same reach the write path's guards have -
// an operand carrying the flag deeper would have had to be stored that way.
//
// post: on ERROR, *preserve_order separates that refusal from malformed
// msgpack - the caller has the op index to stamp, this does not.
static as_exp_trilean
cmp_msgpack(exp_op_code code, const rt_value* v0, const rt_value* v1,
		bool* preserve_order)
{
	if (map_buf_is_preserve_order(v0->r_bytes.contents, v0->r_bytes.sz) ||
			map_buf_is_preserve_order(v1->r_bytes.contents, v1->r_bytes.sz)) {
		*preserve_order = true;
		return AS_EXP_ERROR;
	}

	msgpack_in mp0 = { .buf = v0->r_bytes.contents, .buf_sz = v0->r_bytes.sz };
	msgpack_in mp1 = { .buf = v1->r_bytes.contents, .buf_sz = v1->r_bytes.sz };

	msgpack_cmp_type cmp = msgpack_cmp(&mp0, &mp1);

	if (cmp == MSGPACK_CMP_ERROR) {
		return AS_EXP_ERROR;
	}

	switch (code) {
	case EXP_CMP_EQ:
		return (cmp == MSGPACK_CMP_EQUAL) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_NE:
		return (cmp != MSGPACK_CMP_EQUAL) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GT:
		return (cmp == MSGPACK_CMP_GREATER) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_GE:
		return (cmp == MSGPACK_CMP_EQUAL || cmp == MSGPACK_CMP_GREATER)
				? AS_EXP_TRUE
				: AS_EXP_FALSE;
	case EXP_CMP_LT:
		return (cmp == MSGPACK_CMP_LESS) ? AS_EXP_TRUE : AS_EXP_FALSE;
	case EXP_CMP_LE:
		return (cmp == MSGPACK_CMP_EQUAL || cmp == MSGPACK_CMP_LESS)
				? AS_EXP_TRUE
				: AS_EXP_FALSE;
	default:
		cf_crash(AS_EXP, "unexpected code %u", code);
	}

	return AS_EXP_UNK; // deadcode for eclipse
}

//==========================================================
// Local helpers - runtime call utilities
//

static void
call_cleanup_fn(call_cleanup* cc)
{
	for (uint32_t i = 0; i < cc->bin_ix; i++) {
		as_bin_particle_destroy(cc->bin[i]);
	}

	cc->bin_ix = 0;
}

static void
pack_typed_str(as_packer* pk, const uint8_t* buf, uint32_t sz, uint8_t type)
{
	uint8_t* ptr = pk->buffer + pk->offset;

	sz++; // +1 for type byte

	if (sz < 32) {
		pk->offset += 1;
		*ptr = (uint8_t)(0xa0 | sz);
	}
	else if (sz < (1 << 8)) {
		pk->offset += 2;
		*ptr++ = 0xd9;
		*ptr = (uint8_t)sz;
	}
	else if (sz < (1 << 16)) {
		pk->offset += 3;
		*ptr++ = 0xda;
		*(uint16_t*)ptr = cf_swap_to_be16((uint16_t)sz);
	}
	else {
		pk->offset += 5;
		*ptr++ = 0xdb;
		*(uint32_t*)ptr = cf_swap_to_be32(sz);
	}

	pk->buffer[pk->offset++] = type; // include type in header
	memcpy(pk->buffer + pk->offset, buf, sz - 1);
	pk->offset += sz - 1;
}

static bool
rt_value_to_msgpack_vec(as_packer* pk, msgpack_vec* vec, rollback_alloc* alloc,
		const rt_value* from)
{
	rt_value to;

	if (rt_value_is_unknown(from) || ! rt_value_bin_translate(&to, from)) {
		return false;
	}

	uint32_t offset = pk->offset;

	vec->buf = pk->buffer + offset;

	switch (to.type) {
	case RT_NIL:
		as_pack_nil(pk);
		break;
	case RT_TRILEAN:
		as_pack_bool(pk, to.r_trilean == AS_EXP_TRUE);
		break;
	case RT_INT:
		as_pack_int64(pk, to.r_int);
		break;
	case RT_FLOAT:
		as_pack_double(pk, to.r_float);
		break;
	case RT_GEO_CONST:
		vec->buf = to.r_geo_const.op->contents;
		vec->buf_sz = to.r_geo_const.op->content_sz;
		return true;
	case RT_STR:
	case RT_BLOB:
	case RT_HLL:
	case RT_GEO_STR: {
		uint8_t* p = rollback_alloc_reserve(alloc,
				as_pack_str_size(to.r_bytes.sz + 1));
		as_packer strpk = { .buffer = p, .capacity = UINT32_MAX };

		pack_typed_str(&strpk, to.r_bytes.contents, to.r_bytes.sz, to.type);

		vec->buf = p;
		vec->buf_sz = strpk.offset;
		return true;
	}
	case RT_MSGPACK:
		vec->buf = to.r_bytes.contents;
		vec->buf_sz = to.r_bytes.sz;
		return true;
	case RT_BIN_PTR:
	case RT_BIN:
	case RT_GEO_COMPILED:
	default:
		cf_crash(AS_EXP, "unexpected type %d", to.type);
	}

	vec->buf_sz = pk->offset - offset;
	return true;
}

static bool
rt_value_bin_translate(rt_value* to, const rt_value* from)
{
	as_bin b;

	if (from->type == RT_BIN_PTR) {
		b = *from->r_bin_p;
	}
	else if (from->type == RT_BIN) {
		b = from->r_bin;
	}
	else {
		*to = *from;
		return true;
	}

	uint8_t type = as_bin_get_particle_type(&b);

	*to = (rt_value){ 0 };

	switch (type) {
	case AS_PARTICLE_TYPE_BOOL:
		to->type = RT_TRILEAN;
		to->r_trilean = as_bin_particle_bool_value(&b) ? AS_EXP_TRUE
													   : AS_EXP_FALSE;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		to->type = RT_INT;
		to->r_int = as_bin_particle_integer_value(&b);
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		to->type = RT_FLOAT;
		to->r_float = as_bin_particle_float_value(&b);
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_HLL:
		to->type = type; // exp_rt_type matches particle type for these types
		to->r_bytes.sz =
				as_bin_particle_string_ptr(&b, (char**)&to->r_bytes.contents);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		to->type = type; // exp_rt_type matches particle type for AS_PARTICLE_TYPE_GEOJSON

		size_t sz;

		to->r_bytes.contents =
				(const uint8_t*)as_geojson_mem_jsonstr(b.particle, &sz);
		to->r_bytes.sz = (uint32_t)sz;
		break;
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST: {
		cdt_payload packed;

		as_bin_particle_list_get_packed_val(&b, &packed);

		to->type = RT_MSGPACK;
		to->r_bytes.contents = packed.ptr;
		to->r_bytes.sz = packed.sz;
		break;
	}
	default:
		return false;
	}

	return true;
}

static void*
rt_alloc_mem(runtime* rt, size_t sz, cf_ll_buf* ll_buf)
{
	if (ll_buf != NULL) {
		uint8_t* ptr;

		cf_ll_buf_reserve(ll_buf, sz, &ptr);

		return ptr;
	}

	return cf_malloc(sz);
}

static bool
msgpack_to_bin(runtime* rt, as_bin* to, rt_value* from, cf_ll_buf* ll_buf)
{
	// The storage boundary for a write result and a call receiver alike, so
	// the walk answers for itself rather than trusting where the bytes came
	// from.
	msgpack_in mp = { .buf = from->r_bytes.contents, .buf_sz = from->r_bytes.sz };
	msgpack_type type = msgpack_peek_type(&mp);
	uint8_t p_type;

	switch (type) {
	case MSGPACK_TYPE_LIST:
		p_type = AS_PARTICLE_TYPE_LIST;
		break;
	case MSGPACK_TYPE_MAP:
		p_type = AS_PARTICLE_TYPE_MAP;
		break;
	case MSGPACK_TYPE_BYTES:
		p_type = AS_PARTICLE_TYPE_BLOB;
		break;
	default:
		return false;
	}

	if (msgpack_sz(&mp) == 0 || mp.has_nonstorage) {
		return false;
	}

	to->particle = rt_alloc_mem(rt, from->r_bytes.sz + sizeof(cdt_mem), ll_buf);

	cdt_mem* p_cdt_mem = (cdt_mem*)to->particle;

	p_cdt_mem->type = p_type;
	as_bin_state_set_from_type(to, p_cdt_mem->type);
	p_cdt_mem->sz = from->r_bytes.sz;
	memcpy(p_cdt_mem->data, from->r_bytes.contents, p_cdt_mem->sz);

	return true;
}

//==========================================================
// Local helpers - runtime display.
//

// pre: rt->display_max_sz is the caller's output budget, or 0 for unbounded.
// post: on a budgeted render, db grows by at most one op's worth of output
// past the budget - the caller must treat "at or over budget" as no snippet,
// not as a truncated one.
void
exp_rt_display(runtime* rt, cf_dyn_buf* db)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];

	rt->op_ix++;
	rt->instr_ptr += entry->size;

	if (rt->display_max_sz != 0 && db->used_sz >= rt->display_max_sz) {
		// Over budget - emit nothing for this subtree, but still advance past
		// it so the caller's next child read stays aligned with the op stream.
		while (rt->op_ix < ob->instr_end_ix) {
			const op_base_mem* s = (const op_base_mem*)rt->instr_ptr;

			rt->instr_ptr += op_table[s->code].size;
			rt->op_ix++;
		}

		return;
	}

	entry->display_cb(rt, ob, db);
}

void
exp_display_0_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	cf_dyn_buf_append_format(db, "%s()", op_table[ob->code].name);
}

void
exp_display_1_arg(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	exp_rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_2_args(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	exp_rt_display(rt, db);
	cf_dyn_buf_append_string(db, ", ");
	exp_rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_cmp_regex(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_cmp_regex* op = (op_cmp_regex*)ob;

	cf_dyn_buf_append_format(db, "%s(<string#%u>, %u, ",
			op_table[ob->code].name, op->regex_str_sz, op->flags);
	exp_rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_logical_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	exp_rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		exp_rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_math_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	exp_rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		exp_rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_int_vargs(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	exp_rt_display(rt, db);

	while (rt->op_ix < ob->instr_end_ix) {
		cf_dyn_buf_append_string(db, ", ");
		exp_rt_display(rt, db);
	}

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_meta_digest_mod(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_meta_digest_modulo* op = (op_meta_digest_modulo*)ob;

	cf_dyn_buf_append_format(db, "%s(%u)", op_table[ob->code].name, op->mod);
}

void
exp_display_bin(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_var* op = (op_var*)ob;
	const exp_bin_name_entry* bn = &rt->bin_table->table[op->idx];

	cf_dyn_buf_append_format(db, "%s_%s(\"%.*s\")", op_table[ob->code].name,
			exp_rtype_to_str(op->base.rtype), bn->sz,
			rt_get_bin_name(rt, op->idx));
}

void
exp_display_bin_type(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_var* op = (op_var*)ob;
	const exp_bin_name_entry* bn = &rt->bin_table->table[op->idx];

	cf_dyn_buf_append_format(db, "%s(\"%.*s\")", op_table[ob->code].name,
			bn->sz, rt_get_bin_name(rt, op->idx));
}

void
exp_display_bin_exists(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_var* op = (op_var*)ob;
	const exp_bin_name_entry* bn = &rt->bin_table->table[op->idx];

	cf_dyn_buf_append_format(db, "%s(\"%.*s\")", op_table[ob->code].name,
			bn->sz, rt_get_bin_name(rt, op->idx));
}

void
exp_display_cond(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_cond* op = (op_cond*)ob;

	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	for (uint32_t i = 0; i < op->case_count; i++) {
		exp_rt_display(rt, db);
		cf_dyn_buf_append_string(db, ", ");
		exp_rt_display(rt, db);
		exp_rt_display(rt, db);
		cf_dyn_buf_append_string(db, ", ");
	}

	exp_rt_display(rt, db);

	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_var_builtin(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_var* op = (op_var*)ob;

	cf_dyn_buf_append_format(db, "%s(%u)", op_table[ob->code].name, op->idx);
}

void
exp_display_var(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	op_var* op = (op_var*)ob;

	cf_dyn_buf_append_format(db, "%s(\"var_%u\")", op_table[ob->code].name,
			op->idx);
}

void
exp_display_let(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_let* op = (op_let*)ob;

	cf_dyn_buf_append_string(db, op_table[ob->code].name);
	cf_dyn_buf_append_char(db, '(');

	for (uint32_t i = 0; i < op->n_vars; i++) {
		cf_dyn_buf_append_format(db, "def(var_%u, ", op->var_idx + i);
		exp_rt_display(rt, db);
		cf_dyn_buf_append_string(db, "), ");
	}

	exp_rt_display(rt, db);
	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_call(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	op_call* op = (op_call*)ob;

	exp_call_stype system_type =
			op->system_type & (uint32_t)~EXP_CALL_FLAG_MODIFY_LOCAL;
	bool is_modify = op->system_type != system_type;
	msgpack_in mp = { .buf = op->vecs[0].buf, .buf_sz = op->vecs[0].buf_sz };
	uint32_t ele_count;

	if (! msgpack_get_list_ele_count(&mp, &ele_count)) {
		cf_crash(AS_EXP, "unexpected");
	}

	uint64_t op_code;

	if (! msgpack_get_uint64(&mp, &op_code)) {
		cf_crash(AS_EXP, "unexpected");
	}

	switch (system_type) {
	case EXP_CALL_CDT:
		if (op_code == AS_CDT_OP_CONTEXT_EVAL) {
			msgpack_in mp_ctx = mp; // save ctx position to print later

			msgpack_sz(&mp); // skip ctx to print call name first

			if (! msgpack_get_list_ele_count(&mp, &ele_count)) {
				cf_crash(AS_EXP, "unexpected");
			}

			if (! msgpack_get_uint64(&mp, &op_code)) {
				cf_crash(AS_EXP, "unexpected");
			}

			cf_dyn_buf_append_format(db, "%s(", cdt_exp_display_name(op_code));
			cdt_msgpack_ctx_to_dynbuf(&mp_ctx, db, rt->display_max_sz);
			cf_dyn_buf_append_string(db, ", ");
		}
		else if (op_code == AS_CDT_OP_SELECT) {
			msgpack_in mp_ctx = mp;

			cf_dyn_buf_append_format(db, "%s(", cdt_exp_display_name(op_code));

			if (! cdt_msgpack_ctx_to_dynbuf(&mp, db, rt->display_max_sz)) {
				mp = mp_ctx;

				if (msgpack_sz(&mp) == 0) {
					cf_crash(AS_EXP, "unexpected");
				}
			}

			uint64_t flags;

			if (! msgpack_get_uint64(&mp, &flags)) {
				cf_crash(AS_EXP, "unexpected");
			}

			if (ele_count > 3) {
				uint32_t mod_exp_sz = msgpack_sz(&mp);

				cf_assert(mod_exp_sz != 0, AS_EXP, "unexpected");
				cf_dyn_buf_append_format(db, ", 0x%x, <mod_exp/%u>, ",
						(uint32_t)flags, mod_exp_sz);
			}
			else {
				cf_dyn_buf_append_format(db, ", 0x%x, ", (uint32_t)flags);
			}
		}
		else {
			cf_dyn_buf_append_format(db, "%s(NULL, ",
					cdt_exp_display_name(op_code));
		}

		break;
	case EXP_CALL_BITS:
		cf_dyn_buf_append_format(db, "%s(",
				as_bits_op_name((uint32_t)op_code, is_modify));
		break;
	case EXP_CALL_HLL:
		cf_dyn_buf_append_format(db, "%s(",
				as_hll_op_name((uint32_t)op_code, is_modify));
		break;
	case EXP_CALL_STRING: {
		// The sentinel names no op, so the name is on the inner list. The build
		// made these same reads and sized the vec past them, so a short read
		// means bytes no producer writes -- not worth aborting a display over.
		msgpack_in mp_ctx = mp;
		uint64_t inner_code;

		if (op_code == AS_STRING_OP_CONTEXT_EVAL && msgpack_sz(&mp) != 0 &&
				msgpack_get_list_ele_count(&mp, &ele_count) &&
				msgpack_get_uint64(&mp, &inner_code)) {
			cf_dyn_buf_append_format(db, "%s(",
					as_string_op_name((uint32_t)inner_code, is_modify));
			cdt_msgpack_ctx_to_dynbuf(&mp_ctx, db, rt->display_max_sz);
			cf_dyn_buf_append_string(db, ", ");
			break;
		}

		mp = mp_ctx;
		cf_dyn_buf_append_format(db, "%s(",
				as_string_op_name((uint32_t)op_code, is_modify));
		break;
	}
	default:
		cf_crash(AS_EXP, "unexpected");
	}

	uint32_t idx = 0;
	bool over = false;

	while (true) {
		// A call op's inline arguments are a client-supplied msgpack payload of
		// unbounded length, and they compile to no ops of their own - so
		// neither the per-op budget check in exp_rt_display() nor the op-count
		// pre-filter in stage_eval_trace() bounds this loop. Check per element.
		//
		// Stop appending, but keep walking the vecs below: each
		// exp_call_eval_token still needs its exp_rt_display() call so the op
		// stream stays aligned with the caller's next child read.
		//
		// Reading each vec on its own holds because the build walked every
		// element and refused a zero-sized read, so a vec is tiled exactly.
		while (mp.offset != mp.buf_sz) {
			if (rt->display_max_sz != 0 && db->used_sz >= rt->display_max_sz) {
				if (! over) {
					cf_dyn_buf_append_string(db, "...");
					over = true;
				}

				break;
			}

			display_msgpack(&mp, db);
			cf_dyn_buf_append_string(db, ", ");
		}

		idx++;

		if (idx == op->n_vecs) {
			break;
		}

		mp.buf = op->vecs[idx].buf;
		mp.buf_sz = op->vecs[idx].buf_sz;
		mp.offset = 0;

		if (mp.buf == exp_call_eval_token) {
			exp_rt_display(rt, db);
			cf_dyn_buf_append_string(db, ", ");
		}
	}

	exp_rt_display(rt, db);
	cf_dyn_buf_append_char(db, ')');
}

void
exp_display_value(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;

	switch (ob->code) {
	case EXP_VOP_VALUE_NIL:
		cf_dyn_buf_append_string(db, "nil");
		break;
	case EXP_VOP_VALUE_GEO:;
		uint32_t sz;
		msgpack_in mp = { .buf = ((op_value_geo*)ob)->contents,
			.buf_sz = ((op_value_geo*)ob)->content_sz };

		msgpack_get_bin(&mp, &sz);
		cf_dyn_buf_append_format(db, "<geojson#%u>", sz - 1);
		break;
	case EXP_VOP_VALUE_LIST: {
		op_value_blob* op_b = (op_value_blob*)ob;
		uint32_t ele_count = UINT32_MAX;

		msgpack_buf_get_list_ele_count(op_b->value, op_b->value_sz, &ele_count);
		cf_dyn_buf_append_format(db, "<list#%u>", ele_count);
		break;
	}
	case EXP_VOP_VALUE_MSGPACK: {
		op_value_blob* op_b = (op_value_blob*)ob;
		uint32_t ele_count = UINT32_MAX;

		if (msgpack_buf_get_map_ele_count(op_b->value, op_b->value_sz,
					&ele_count)) {
			cf_dyn_buf_append_format(db, "<map#%u>", ele_count);
		}
		else {
			cf_dyn_buf_append_format(db, "<ext#%u>", op_b->value_sz);
		}

		break;
	}
	case EXP_VOP_VALUE_BOOL:
		cf_dyn_buf_append_bool(db, ((op_value_bool*)ob)->value);
		break;
	case EXP_VOP_VALUE_INT:
		cf_dyn_buf_append_format(db, "%ld", ((op_value_int*)ob)->value);
		break;
	case EXP_VOP_VALUE_FLOAT:
		cf_dyn_buf_append_format(db, "%f", ((op_value_float*)ob)->value);
		break;
	case EXP_VOP_VALUE_STR:
		cf_dyn_buf_append_format(db, "<string#%u>",
				((op_value_blob*)ob)->value_sz);
		break;
	case EXP_VOP_VALUE_BLOB:
		cf_dyn_buf_append_format(db, "<blob#%u>", ((op_value_blob*)ob)->value_sz);
		break;
	case EXP_VOP_VALUE_HLL:
		cf_dyn_buf_append_format(db, "<hll#%u>", ((op_value_blob*)ob)->value_sz);
		break;
	case EXP_QUOTE: {
		// A quoted list literal (op arg). Reachable here now that
		// stage_eval_fault renders client-supplied expressions at eval time -
		// a faulting op whose subtree contains a quote must render, not crash.
		op_value_blob* op_b = (op_value_blob*)ob;
		uint32_t ele_count = UINT32_MAX;

		msgpack_buf_get_list_ele_count(op_b->value, op_b->value_sz, &ele_count);
		cf_dyn_buf_append_format(db, "<quote#%u>", ele_count);
		break;
	}
	default:
		cf_crash(AS_EXP, "unexpected code %u", ob->code);
	}
}

void
exp_display_case(runtime* rt, const op_base_mem* ob, cf_dyn_buf* db)
{
	(void)rt;
	(void)ob;
	(void)db;
}

void
display_msgpack(msgpack_in* mp, cf_dyn_buf* db)
{
	msgpack_type type = msgpack_peek_type(mp);

	switch (type) {
	case MSGPACK_TYPE_NIL:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "nil");
		break;
	case MSGPACK_TYPE_FALSE:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "false");
		break;
	case MSGPACK_TYPE_TRUE:
		msgpack_sz(mp);
		cf_dyn_buf_append_string(db, "true");
		break;
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT: {
		int64_t ctx_int;

		msgpack_get_int64(mp, &ctx_int);
		cf_dyn_buf_append_format(db, "%ld", ctx_int);
		break;
	}
	case MSGPACK_TYPE_DOUBLE: {
		double ctx_double;

		msgpack_get_double(mp, &ctx_double);
		cf_dyn_buf_append_format(db, "%f", ctx_double);
		break;
	}
	case MSGPACK_TYPE_STRING: {
		uint32_t str_sz;

		msgpack_get_bin(mp, &str_sz);
		cf_dyn_buf_append_format(db, "<string#%u>", str_sz - 1);
		break;
	}
	case MSGPACK_TYPE_BYTES: {
		uint32_t blob_sz;
		const uint8_t* blob = msgpack_get_bin(mp, &blob_sz);

		if (blob_sz == 0) {
			cf_dyn_buf_append_format(db, "<blob#_>");
			break;
		}

		if (blob[0] == AS_BYTES_HLL) {
			cf_dyn_buf_append_format(db, "<hll#%u>", blob_sz - 1);
			break;
		}

		cf_dyn_buf_append_format(db, "<blob#%u>", blob_sz - 1);
		break;
	}
	case MSGPACK_TYPE_GEOJSON: {
		uint32_t geo_sz;

		msgpack_get_bin(mp, &geo_sz);
		cf_dyn_buf_append_format(db, "<geojson#%u>", geo_sz - 1);
		break;
	}
	default:
		msgpack_sz(mp);
		cf_dyn_buf_append_format(db, "<msgpack/%u>", type);
		break;
	}
}
