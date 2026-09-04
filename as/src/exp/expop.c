/*
 * expop.c
 *
 * Copyright (C) 2021-2026 Aerospike, Inc.
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

#include "exp/expop.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "exp/exp.h"
#include "transaction/rw_utils.h"
#include "transaction/write.h"

//==========================================================
// Typedefs & constants.
//

#define INVALID_MODIFY_FLAGS                                                   \
	~(uint64_t)(AS_EXP_FLAG_CREATE_ONLY | AS_EXP_FLAG_UPDATE_ONLY |            \
			AS_EXP_FLAG_ALLOW_DELETE | AS_EXP_FLAG_POLICY_NO_FAIL |            \
			AS_EXP_FLAG_EVAL_NO_FAIL)
#define INVALID_READ_FLAGS ~(uint64_t)(AS_EXP_FLAG_EVAL_NO_FAIL)

//==========================================================
// Forward declarations.
//

static int eval_op(as_exp* exp, const as_exp_ctx* ctx, uint64_t flags,
		as_bin* rb, cf_ll_buf* particles_llb);

//==========================================================
// Inlines & macros.
//

static inline void
exp_destroy_if(const iops_expop* expop, as_exp* exp)
{
	if (expop == NULL) {
		as_exp_destroy(exp);
	}
}

//==========================================================
// Public API.
//

bool
as_exp_op_parse(const as_msg_op* msg_op, as_exp** exp, uint64_t* flags,
		bool is_modify, bool cpy_wire)
{
	msgpack_in mp = { .buf = as_msg_op_get_value_p(msg_op),
		.buf_sz = as_msg_op_get_value_sz(msg_op) };

	uint32_t ele_count = 0;

	if (! msgpack_get_list_ele_count(&mp, &ele_count) || ele_count == 0 ||
			ele_count > 2) {
		cf_warning(AS_PARTICLE,
				"parse_op - error %u received invalid expression op ele_count %u",
				AS_ERR_PARAMETER, ele_count);
		return false;
	}

	uint32_t exp_buf_sz = 0;
	const uint8_t* exp_buf = msgpack_get_ele(&mp, &exp_buf_sz);

	if (exp_buf == NULL) {
		cf_warning(AS_PARTICLE,
				"parse_op - error %u received invalid expression size %u",
				AS_ERR_PARAMETER, exp_buf_sz);
		return false;
	}

	*flags = 0; // default

	if (msgpack_get_uint64(&mp, flags)) {
		if (is_modify) {
			if (*flags & INVALID_MODIFY_FLAGS) {
				cf_warning(AS_PARTICLE,
						"parse_op - error %u received invalid modify flags %lx",
						AS_ERR_PARAMETER, *flags);
				return false;
			}
		}
		else {
			if (*flags & INVALID_READ_FLAGS) {
				cf_warning(AS_PARTICLE,
						"parse_op - error %u received invalid read flags %lx",
						AS_ERR_PARAMETER, *flags);
				return false;
			}
		}
	}

	*exp = as_exp_build_buf(exp_buf, exp_buf_sz, cpy_wire, NULL);

	if (*exp == NULL) {
		// Author the contextual detail here (paired with the staged trace) -
		// the write/operate caller does not set error details on this
		// AS_ERR_PARAMETER, so without this the staged trace would never be
		// serialized into field 45 and nothing would reach the client.
		as_exp_stage_build_error_details("invalid expression in operation request");

		cf_warning(AS_PARTICLE,
				"parse_op - error %u unable to build expression op",
				AS_ERR_PARAMETER);
		return false;
	}

	// Non-taking build - drop the accumulator's payload ref.
	as_exp_build_err_reset();

	return true;
}

int
as_exp_modify_tr(const as_exp_ctx* ctx, as_bin* b, const as_msg_op* msg_op,
		cf_ll_buf* particles_llb, const iops_expop* expop)
{
	as_exp* exp;
	uint64_t flags;

	if (expop != NULL) {
		exp = expop->exp;
		flags = expop->flags;
	}
	else if (! as_exp_op_parse(msg_op, &exp, &flags, true, false)) {
		return -AS_ERR_PARAMETER;
	}

	bool was_live = as_bin_is_live(b);

	if (was_live) {
		if ((flags & AS_EXP_FLAG_CREATE_ONLY) != 0) {
			if ((flags & AS_EXP_FLAG_POLICY_NO_FAIL) != 0) {
				exp_destroy_if(expop, exp);
				return AS_OK;
			}

			cf_detail(AS_PARTICLE,
					"as_exp_modify_tr - error %u bin %.*s would update",
					AS_ERR_BIN_EXISTS, (int)msg_op->name_sz, msg_op->name);
			exp_destroy_if(expop, exp);
			return -AS_ERR_BIN_EXISTS;
		}
		// else - there is an existing particle, which we will modify.
	}
	else {
		if ((flags & AS_EXP_FLAG_UPDATE_ONLY) != 0) {
			if ((flags & AS_EXP_FLAG_POLICY_NO_FAIL) != 0) {
				exp_destroy_if(expop, exp);
				return AS_OK;
			}

			cf_detail(AS_PARTICLE,
					"as_bin_exp_modify_tr - error %u bin %.*s would create",
					AS_ERR_BIN_NOT_FOUND, (int)msg_op->name_sz, msg_op->name);
			exp_destroy_if(expop, exp);
			return -AS_ERR_BIN_NOT_FOUND;
		}

		cf_assert(b->particle == NULL, AS_PARTICLE, "particle not null");
	}

	as_particle* old_particle = b->particle;
	uint8_t old_type = as_bin_get_particle_type(b);
	int rv = eval_op(exp, ctx, flags, b, particles_llb);

	exp_destroy_if(expop, exp);

	if (rv != AS_OK) {
		return rv;
	}

	if ((flags & AS_EXP_FLAG_ALLOW_DELETE) == 0 && was_live &&
			as_bin_is_unused(b)) {
		b->particle = old_particle;
		as_bin_state_set_from_type(b, old_type);

		if ((flags & AS_EXP_FLAG_POLICY_NO_FAIL) != 0) {
			return AS_OK;
		}

		cf_detail(AS_PARTICLE,
				"as_bin_exp_modify_tr - error %u bin %.*s would delete",
				AS_ERR_OP_NOT_APPLICABLE, (int)msg_op->name_sz, msg_op->name);
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	// A CDT call's map holds selection order, and nothing re-canonicalizes a
	// value on its way into a bin. Storing one is refused; returning one is not.
	if (as_bin_get_particle_type(b) == AS_PARTICLE_TYPE_MAP) {
		cdt_payload packed;

		as_bin_particle_map_get_packed_val(b, &packed);

		if (map_buf_is_preserve_order(packed.ptr, packed.sz)) {
			// A slab owns every particle it allocated; without one, the bin owns
			// this.
			if (particles_llb == NULL) {
				as_bin_particle_destroy(b);
			}

			b->particle = old_particle;
			as_bin_state_set_from_type(b, old_type);

			if ((flags & AS_EXP_FLAG_POLICY_NO_FAIL) != 0) {
				return AS_OK;
			}

			cf_detail(AS_PARTICLE,
					"as_bin_exp_modify_tr - error %u bin %.*s preserved order map",
					AS_ERR_INCOMPATIBLE_TYPE, (int)msg_op->name_sz, msg_op->name);
			as_error_details_set_fmt(AS_SUB_NONE,
					"cannot store a map with preserved element order");
			return -AS_ERR_INCOMPATIBLE_TYPE;
		}
	}

	return AS_OK;
}

int
as_exp_read_tr(const as_exp_ctx* ctx, const as_msg_op* msg_op, as_bin* rb)
{
	as_exp* exp;
	uint64_t flags;

	if (! as_exp_op_parse(msg_op, &exp, &flags, false, false)) {
		return -AS_ERR_PARAMETER;
	}

	int rv = eval_op(exp, ctx, flags, rb, NULL);

	as_exp_destroy(exp);

	return rv;
}

//==========================================================
// Local helpers.
//

static int
eval_op(as_exp* exp, const as_exp_ctx* ctx, uint64_t flags, as_bin* rb,
		cf_ll_buf* particles_llb)
{
	// EVAL_NO_FAIL makes a non-TRUE result an expected outcome, so suppress
	// detail staging for the duration of the eval rather than letting a fault
	// build a full trace that is then discarded. Suppressing at the source
	// also leaves an enclosing site's already-armed detail intact, which a
	// post-hoc as_error_msg_clear() would have wiped.
	bool no_fail = (flags & AS_EXP_FLAG_EVAL_NO_FAIL) != 0;
	uint8_t saved_verbosity = g_error_verbosity;

	if (no_fail) {
		g_error_verbosity = AS_ERROR_VERBOSITY_OFF;
	}

	// as_exp_eval is tri-state: TRUE produced a value; UNK is a clean
	// non-match; ERROR is a fault that already staged its trace + message.
	// Both non-TRUE results map to the unchanged status below.
	as_exp_trilean result = as_exp_eval(exp, ctx, rb, particles_llb);

	g_error_verbosity = saved_verbosity;

	if (result != AS_EXP_TRUE) {
		if (no_fail) {
			// Tolerated failure - nothing was staged (see above).
			// EVAL_NO_FAIL semantics are unchanged: return AS_OK.
			return AS_OK;
		}

		// A clean UNK produced no value and staged nothing - explain the
		// missing value by naming the absent / wrong-type reference.
		//
		// Gate order matters. as_exp_explain_value re-tests both thread-locals
		// itself, but as_exp_explain_allowed is not free: on a secured cluster
		// it takes the per-connection security-filter lock, and all rows of one
		// batch share a single fd_h, so concurrent rows would serialize on it.
		// UNK is routine absent-bin traffic, not a rare edge - test the cheap
		// conditions first so nothing is paid below the trace tier, or for a
		// fault that already staged its own trace.
		//
		// Which reference went absent is derived from the stored record, so
		// this needs the same PERM_READ gate as the filter explainer - an
		// expression-modify op is authorized for write and may lack read.
		// Fails closed on a NULL ctx->tr (internal evals).
		if (result == AS_EXP_UNK &&
				g_error_verbosity >= AS_ERROR_VERBOSITY_TRACE &&
				! g_error_exp_trace.set &&
				as_exp_explain_allowed(ctx->tr, ctx->rd)) {
			as_exp_explain_value(exp, ctx);
		}

		cf_detail(AS_PARTICLE,
				"eval_op - expression failed to evaluate to a bin type");
		return -AS_ERR_OP_NOT_APPLICABLE;
	}

	return AS_OK;
}
