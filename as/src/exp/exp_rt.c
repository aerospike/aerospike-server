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

// Shared sentinel (declared in exp/exp_rt.h) -- one address, so build (exp.c)
// sets and eval compares the same pointer.
const uint8_t exp_call_eval_token[1] = "";

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
		const rt_value* v1);

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

bool
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
		return true;
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
		if (ret_val.r_trilean == AS_EXP_UNK) {
			return false;
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

			return false;
		}

		break;
	case RT_STR: {
		// Validate before alloc: on failure eval_op returns without restoring
		// rb->particle (see as_exp_modify_tr), so we must not leave a new
		// particle allocated without bin state set.
		if (! cf_str_is_valid_utf8(ret_val.r_bytes.contents, ret_val.r_bytes.sz)) {
			cf_ticker_warning(AS_EXP,
					"as_exp_eval - invalid UTF-8 detected in string data");
			return false;
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
			return false;
		}

		break;
	default:
		cf_warning(AS_EXP, "as_exp_eval - unexpected result type (%u)",
				ret_val.type);
		rt_value_destroy(&ret_val);
		return false;
	}

	return true;
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

			return as_pack_bin_size(sz + 1);
		}
		case AS_PARTICLE_TYPE_GEOJSON: {
			size_t sz;

			as_geojson_mem_jsonstr(b.particle, &sz);

			return as_pack_bin_size(sz + 1);
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
		return as_pack_bin_size(res->str.sz);
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

void
as_exp_result_msgpack_pack(const as_exp_result* res, as_packer* pk)
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
			char* ptr;
			uint32_t sz = as_bin_particle_string_ptr(&b, &ptr);

			as_pack_str_with_type(pk, res->type, (uint8_t*)ptr, sz);
			break;
		}
		case AS_PARTICLE_TYPE_GEOJSON: {
			size_t sz;
			const char* str = as_geojson_mem_jsonstr(b.particle, &sz);

			as_pack_str_with_type(pk, res->type, (uint8_t*)str, sz);
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
as_exp_eval_to_result(const as_exp* exp, const as_exp_ctx* ctx, as_exp_result* res)
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
			return true;
		default:
			break;
		}
	}

	rt_value t_val;
	as_packer pk = { .capacity = UINT32_MAX };

	if (! rt_value_bin_translate(&t_val, &ret_val)) {
		cf_warning(AS_EXP, "as_exp_eval_to_result - unexpected result type (%u)",
				ret_val.type);
		return false;
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
		if (t_val.r_trilean == AS_EXP_UNK) {
			return false;
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
			return false;
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
		return false;
	}

	return true;
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

	return ret_val.r_trilean;
}

static bool
rt_eval(runtime* rt, rt_value* ret_val)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];
	rt->op_ix++;
	rt->instr_ptr += entry->size;
	*ret_val = (rt_value){ 0 };
	entry->eval_cb(rt, ob, ret_val);

	bool ret = rt_value_is_unknown(ret_val);

	return ret;
}

void
exp_eval_unknown(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)rt;
	(void)ob;

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = AS_EXP_UNK;
}

void
exp_eval_compare(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
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

	ret_val->type = RT_TRILEAN;

	if (rt_value_is_unknown(&v0) || rt_value_is_unknown(&v1) ||
			! rt_is_type(&v0, ob->rtype) || ! rt_is_type(&v1, ob->rtype)) {
		ret_val->r_trilean = AS_EXP_UNK;
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
	case EXP_RTYPE_MAP:
		ret_val->r_trilean = cmp_msgpack(ob->code, &v0, &v1);
		break;
	default:
		cf_crash(AS_EXP, "unexpected type %u", ob->rtype);
	}
}

void
exp_eval_cmp_regex(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	if (rt_eval(rt, ret_val)) {
		return;
	}

	uint32_t str_sz = 0; // initialized for centos 6
	const uint8_t* str = rt_value_get_str(ret_val, &str_sz);

	if (str == NULL) {
		rt_value_destroy(ret_val);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
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
	rt_value arg0;
	rt_value arg1;

	if (rt_eval(rt, &arg0)) {
		*ret_val = rt_unk;
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	defer_rt_value_destroy(arg0);

	if (rt_eval(rt, &arg1)) {
		*ret_val = rt_unk;
		return;
	}

	defer_rt_value_destroy(arg1);
	rt_value list;

	if (rt_value_is_unknown(&arg1) || ! rt_value_bin_translate(&list, &arg1)) {
		*ret_val = rt_unk;
		return;
	}

	defer_rt_value_destroy(list);

	if (list.type != RT_MSGPACK) {
		cf_warning(AS_EXP, "exp_eval_in_list - list expected, got %u", list.type);
		*ret_val = rt_unk;
		return;
	}

	uint8_t buf[1 + sizeof(uint64_t)];
	msgpack_vec vec;
	define_rollback_alloc(alloc, NULL, 1);
	DEFER_ROLLBACK_ALLOC(alloc);

	as_packer pk = { .buffer = buf, .capacity = sizeof(buf) };

	if (! rt_value_to_msgpack_vec(&pk, &vec, alloc, &arg0)) {
		*ret_val = rt_unk;
		return;
	}

	msgpack_in mp_list = { .buf = list.r_bytes.contents,
		.buf_sz = list.r_bytes.sz };

	uint32_t ele_count = 0;

	if (! msgpack_get_list_ele_count(&mp_list, &ele_count)) {
		*ret_val = rt_unk;
		return;
	}

	msgpack_in mp_ele = { .buf = vec.buf, .buf_sz = vec.buf_sz };

	for (uint32_t i = 0; i < ele_count; i++) {
		mp_ele.offset = 0;

		msgpack_cmp_type cmp = msgpack_cmp(&mp_ele, &mp_list);

		if (cmp == MSGPACK_CMP_ERROR) {
			*ret_val = rt_unk;
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

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected - type %u",
				ret_val->type);

		if (ret_val->r_trilean == AS_EXP_FALSE) {
			ret = AS_EXP_FALSE;
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

void
exp_eval_or(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	as_exp_trilean ret = AS_EXP_FALSE;

	while (rt->op_ix < ob->instr_end_ix) {
		if (rt_eval(rt, ret_val)) {
			ret = AS_EXP_UNK;
			continue;
		}

		cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

		if (ret_val->r_trilean == AS_EXP_TRUE) {
			ret = AS_EXP_TRUE;
			rt_skip(rt, ob->instr_end_ix);
			break;
		}
	}

	ret_val->type = RT_TRILEAN;
	ret_val->r_trilean = ret;
}

void
exp_eval_not(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
{
	(void)ob;

	if (rt_eval(rt, ret_val)) {
		return;
	}

	cf_assert(ret_val->type == RT_TRILEAN, AS_EXP, "unexpected");

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

		if (iproduct == 0 || (ret_val->r_int == INT64_MIN && iproduct == -1)) {
			cf_warning(AS_EXP,
					"exp_eval_div - integer division by zero or overflow");
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
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
	if (rt_eval(rt, ret_val)) {
		rt_skip(rt, ob->instr_end_ix);
		return;
	}

	rt_value arg1;

	if (rt_eval(rt, &arg1) || arg1.r_int == 0 ||
			(ret_val->r_int == INT64_MIN && arg1.r_int == -1)) {
		*ret_val = arg1;
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
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
		return;
	}

	as_bin rb = { 0 };

	if (as_bin_to_string(b, &rb) != AS_OK) {
		rt_value_destroy(&operand);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
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
	if (! rt_has_rd(rt)) {
		*ret_val = rt_unk;
		return;
	}

	if (! as_storage_rd_load_key(rt->ctx->rd)) {
		*ret_val = rt_unk;
		return;
	}

	const uint8_t* key = rt->ctx->rd->key;
	uint32_t key_sz = rt->ctx->rd->key_size;

	cf_assert(key_sz != 0, AS_EXP, "key_size can be 0?");

	switch (key[0]) {
	case AS_PARTICLE_TYPE_INTEGER:
		if (key_sz != sizeof(int64_t) + 1) {
			cf_warning(AS_EXP,
					"exp_eval_rec_key - unexpected integer key size %u", key_sz);
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
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
		cf_warning(AS_EXP, "exp_eval_rec_key - invalid key type %u", key[0]);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
		return;
	}

	if (ob->type != ret_val->type) {
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
	}
}

void
exp_eval_bin(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
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

	// Borrow the cached value; do_not_destroy carries to the copy so
	// rt_release_bins frees it. The per-reference type check is on the copy and
	// never poisons the shared slot.
	*ret_val = *brt;

	if (exp_rtype_to_particle_type(op->base.rtype) !=
			rt_value_particle_type(brt)) {
		*ret_val = rt_unk;
	}
}

void
exp_eval_bin_type(runtime* rt, const op_base_mem* ob, rt_value* ret_val)
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
	rt_value arg;

	if (rt_eval(rt, &arg)) {
		*ret_val = rt_unk;
		return;
	}

	defer_rt_value_destroy(arg);

	rt_value rt_map;

	if (! rt_value_bin_translate(&rt_map, &arg) || rt_map.type != RT_MSGPACK ||
			msgpack_buf_peek_type(rt_map.r_bytes.contents, rt_map.r_bytes.sz) !=
					MSGPACK_TYPE_MAP) {
		*ret_val = rt_unk;
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
		*ret_val = rt_unk;
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

			vecs[vec_ix].buf = pk.buffer + pk.offset;
			vecs[vec_ix].offset = 0;

			if (from->type == RT_BIN && ! from->do_not_destroy) {
				call_cleanup_add(bin_cleanup, &from->r_bin);
			}

			if (! rt_value_to_msgpack_vec(&pk, &vecs[vec_ix], alloc, from)) {
				ret_val->type = RT_TRILEAN;
				ret_val->r_trilean = AS_EXP_UNK;
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
			rt_value_destroy(&temp);
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
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
		if (bin_arg.r_trilean == AS_EXP_UNK) {
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
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
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
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
		rt_value_destroy(&bin_arg);
		ret_val->type = RT_TRILEAN;
		ret_val->r_trilean = AS_EXP_UNK;
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
			bool borrowed_unchanged = bin_arg.do_not_destroy &&
					old.particle == b->particle;

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
		ret_val->do_not_destroy = bin_arg.do_not_destroy &&
				old.particle == b->particle;
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
			ret_val->type = RT_TRILEAN;
			ret_val->r_trilean = AS_EXP_UNK;
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
		val->type = RT_TRILEAN;
		val->r_trilean = AS_EXP_UNK;
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

// Free the per-bin slots after eval. Only masked object bins hold a particle;
// they're borrowed views (do_not_destroy=1) and this is their sole freer. If
// the result aliases a slot's particle, hand ownership to the result instead
// so it is freed exactly once.
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

static as_exp_trilean
cmp_msgpack(exp_op_code code, const rt_value* v0, const rt_value* v1)
{
	msgpack_in mp0 = { .buf = v0->r_bytes.contents, .buf_sz = v0->r_bytes.sz };
	msgpack_in mp1 = { .buf = v1->r_bytes.contents, .buf_sz = v1->r_bytes.sz };

	msgpack_cmp_type cmp = msgpack_cmp(&mp0, &mp1);

	if (cmp == MSGPACK_CMP_ERROR) {
		return AS_EXP_UNK;
	}

	if (mp0.has_unordered_map || mp1.has_unordered_map) {
		cf_debug(AS_EXP,
				"illegal comparison of structure containing unordered map - arg0 %s arg1 %s",
				mp0.has_unordered_map ? "has unordered" : "is ok",
				mp1.has_unordered_map ? "has unordered" : "is ok");
		return AS_EXP_UNK;
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
	msgpack_type type =
			msgpack_buf_peek_type(from->r_bytes.contents, from->r_bytes.sz);
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

void
exp_rt_display(runtime* rt, cf_dyn_buf* db)
{
	op_base_mem* ob = (op_base_mem*)rt->instr_ptr;
	const op_table_entry* entry = &op_table[ob->code];

	rt->op_ix++;
	rt->instr_ptr += entry->size;
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
			cdt_msgpack_ctx_to_dynbuf(&mp_ctx, db);
			cf_dyn_buf_append_string(db, ", ");
		}
		else if (op_code == AS_CDT_OP_SELECT) {
			msgpack_in mp_ctx = mp;

			cf_dyn_buf_append_format(db, "%s(", cdt_exp_display_name(op_code));

			if (! cdt_msgpack_ctx_to_dynbuf(&mp, db)) {
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
	case EXP_CALL_STRING:
		cf_dyn_buf_append_format(db, "%s(",
				as_string_op_name((uint32_t)op_code, is_modify));
		break;
	default:
		cf_crash(AS_EXP, "unexpected");
	}

	uint32_t idx = 0;

	while (true) {
		while (mp.offset != mp.buf_sz) {
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
