/*
 * query_plan.c
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

#include "query/query_plan.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h> // MSG_NOSIGNAL

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "log.h"
#include "socket.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/transaction.h"
#include "exp/exp.h"
#include "query/query.h"
#include "query/query_where.h"
#include "sindex/sindex.h"
#include "sindex/sindex_manager.h"

#include "warnings.h"

//==========================================================
// Typedefs & constants.
//

// INDEX_RANGE header, excluding STRING/BLOB/GEOJSON bound bytes: n_ranges +
// bin_name + ktype + INTEGER's two length-prefixed int64 bounds (the worst
// case among all ktypes, so this is a true bound for every ktype on its own).
#define PLAN_INDEX_RANGE_HEADER_MAX                                            \
	(1 + 1 + AS_BIN_NAME_MAX_SZ + 1 + 2 * (sizeof(uint32_t) + sizeof(int64_t)))

#define PLAN_RESPONSE_HEADER_MAX                                               \
	(sizeof(cl_msg) + (sizeof(as_msg_field) + INAME_MAX_SZ) +                  \
			(sizeof(as_msg_field) + 1) +                                       \
			(sizeof(as_msg_field) + PLAN_INDEX_RANGE_HEADER_MAX))

// Stack-buffer size, header plus the STRING/BLOB inline bound. GEO instead
// heap-allocates PLAN_RESPONSE_HEADER_MAX + geo_extra (plan_send_response).
#define PLAN_RESPONSE_FIXED_MAX                                                \
	(PLAN_RESPONSE_HEADER_MAX + AS_EXP_SINDEX_BOUND_VAL_MAX)

// Planner-only state - decoupled from as_query_job, no scan is executed.
typedef struct plan_ctx_s {
	as_namespace* ns;
	uint16_t set_id;
	char set_name[AS_SET_NAME_MAX_SIZE];
	char iname_hint[INAME_MAX_SZ];
	char username[MAX_USER_SIZE];

	as_exp* exp;
	bool require_index;
	bool hard_hint;
	as_sindex_selection selection;
} plan_ctx;

//==========================================================
// Forward declarations.
//

static void plan_ctx_destroy(plan_ctx* ctx);
static bool plan_parse_request(as_transaction* tr, plan_ctx* ctx,
		as_query_where* where);
static as_query_plan_result plan_pick_sindex(plan_ctx* ctx);
static void plan_send_response(const as_transaction* tr, const plan_ctx* ctx);
static uint32_t plan_build_range_payload(uint8_t* buf,
		const as_exp_sindex_candidate* match);
static uint8_t* plan_append_bytes_bound(uint8_t* p, const uint8_t* val,
		uint32_t val_sz);
static uint8_t* plan_append_int64_bound(uint8_t* p, int64_t val);
static uint8_t plan_wire_result_code(const plan_ctx* ctx);
static const char* plan_result_str(as_query_plan_result result);

//==========================================================
// Public API.
//

int
as_query_plan(as_transaction* tr, as_namespace* ns, as_query_where* where)
{
	plan_ctx ctx = {
		.ns = ns,
		.require_index = as_query_where_require_index(where),
		.hard_hint = as_query_where_hard_hint(where),
	};

	if (! plan_parse_request(tr, &ctx, where)) {
		plan_ctx_destroy(&ctx);
		return AS_ERR_PARAMETER;
	}

	if (ctx.hard_hint && ctx.iname_hint[0] == '\0') {
		cf_debug(AS_QUERY,
				"{%s} query-plan: HARD_HINT rejected - missing index name hint",
				ns->name);
		plan_ctx_destroy(&ctx);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	as_query_plan_result result = plan_pick_sindex(&ctx);

	if (result == AS_QUERY_PLAN_ERROR) {
		plan_ctx_destroy(&ctx);
		return AS_ERR_PARAMETER;
	}

	if (ctx.hard_hint &&
			(result == AS_QUERY_PLAN_PI ||
					(result == AS_QUERY_PLAN_SINDEX && ctx.selection.si != NULL &&
							strcmp(ctx.selection.si->iname, ctx.iname_hint) !=
									0))) {
		cf_debug(AS_QUERY,
				"{%s} query-plan: HARD_HINT rejected result=%s set=%s hint=%s selected=%s",
				ns->name, plan_result_str(result),
				ctx.set_name[0] != '\0' ? ctx.set_name : "<none>",
				ctx.iname_hint[0] != '\0' ? ctx.iname_hint : "<none>",
				(result == AS_QUERY_PLAN_SINDEX && ctx.selection.si != NULL)
						? ctx.selection.si->iname
						: "<none>");
		plan_ctx_destroy(&ctx);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (ctx.require_index && result == AS_QUERY_PLAN_PI) {
		cf_debug(AS_QUERY,
				"{%s} query-plan: REQUIRE_INDEX rejected PI fallback", ns->name);
		plan_ctx_destroy(&ctx);
		return AS_ERR_SINDEX_NOT_FOUND;
	}

	if (result == AS_QUERY_PLAN_SINDEX && ctx.selection.si != NULL &&
			as_query_sindex_must_mask(ctx.ns, ctx.set_name, ctx.username,
					ctx.selection.si)) {
		cf_debug(AS_QUERY, "{%s} query-plan: masked sindex rejected for user",
				ns->name);
		plan_ctx_destroy(&ctx);
		return AS_SEC_ERR_ROLE_VIOLATION;
	}

	plan_send_response(tr, &ctx);
	plan_ctx_destroy(&ctx);

	return AS_OK;
}

//==========================================================
// Local helpers.
//

static void
plan_ctx_destroy(plan_ctx* ctx)
{
	if (ctx->exp != NULL) {
		as_exp_destroy(ctx->exp);
		ctx->exp = NULL;
	}

	if (ctx->selection.si != NULL) {
		as_sindex_job_release(ctx->selection.si);
		ctx->selection.si = NULL;
	}
}

static bool
plan_parse_request(as_transaction* tr, plan_ctx* ctx, as_query_where* where)
{
	ctx->set_id = INVALID_SET_ID;
	ctx->set_name[0] = '\0';
	ctx->iname_hint[0] = '\0';
	ctx->exp = NULL;

	as_security_copy_username(tr, ctx->username, sizeof(ctx->username));

	as_namespace* ns = ctx->ns;
	const as_msg* m = &tr->msgp->msg;

	if (as_transaction_has_set(tr)) {
		const as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
		uint32_t len = as_msg_field_get_value_sz(f);

		if (len >= AS_SET_NAME_MAX_SIZE) {
			cf_ticker_warning(AS_QUERY,
					"{%s} query-plan: set name too long, max %u", ns->name,
					AS_SET_NAME_MAX_SIZE - 1);
			return false;
		}

		if (len > 0) {
			memcpy(ctx->set_name, f->data, len);
			ctx->set_name[len] = '\0';
			ctx->set_id = as_namespace_get_set_id(ns, ctx->set_name);
		}
	}

	if (as_transaction_has_index_name(tr)) {
		const as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_INDEX_NAME);
		uint32_t len = as_msg_field_get_value_sz(f);

		if (len == 0) {
			cf_ticker_warning(AS_QUERY, "{%s} query-plan: empty index name",
					ns->name);
			return false;
		}

		if (len >= INAME_MAX_SZ) {
			cf_ticker_warning(AS_QUERY,
					"{%s} query-plan: index name too long, max %u", ns->name,
					INAME_MAX_SZ - 1);
			return false;
		}

		memcpy(ctx->iname_hint, f->data, len);
		ctx->iname_hint[len] = '\0';
	}

	ctx->exp = as_query_where_build_filter(where);

	if (ctx->exp == NULL) {
		cf_ticker_warning(AS_QUERY, "{%s} query-plan: bad AEL filter", ns->name);
		// Stage the build failure - this runs synchronously on the armed
		// service thread, and as_query_error() sends the detail with the
		// start-failure reply.
		as_exp_stage_build_error_details("invalid filter expression in query");
		return false;
	}

	cf_debug(AS_QUERY, "{%s} query-plan: parsed set=%s index name hint=%s",
			ns->name, ctx->set_name[0] != '\0' ? ctx->set_name : "<none>",
			ctx->iname_hint[0] != '\0' ? ctx->iname_hint : "<none>");

	return true;
}

static as_query_plan_result
plan_pick_sindex(plan_ctx* ctx)
{
	const char* hint = ctx->iname_hint[0] != '\0' ? ctx->iname_hint : NULL;

	ctx->selection =
			as_sindex_select_from_exp(ctx->ns, ctx->set_id, ctx->exp, hint);

	return ctx->selection.result;
}

static void
plan_send_response(const as_transaction* tr, const plan_ctx* ctx)
{
	as_file_handle* fd_h = tr->from.proto_fd_h;

	// geo_bound_val is a borrowed pointer that can carry up to 1 MiB (GEO's
	// real wire ceiling) - too big for a fixed stack buffer, so only heap
	// allocate the (rare) larger response; everything else keeps the cheap
	// stack path.
	uint32_t geo_extra =
			(ctx->selection.result == AS_QUERY_PLAN_SINDEX &&
					ctx->selection.match.ktype == AS_PARTICLE_TYPE_GEOJSON)
			? ctx->selection.match.bound_val_sz
			: 0;

	uint8_t stack_buf[PLAN_RESPONSE_FIXED_MAX];
	uint8_t* buf = geo_extra != 0
			? cf_malloc(PLAN_RESPONSE_HEADER_MAX + geo_extra)
			: stack_buf;

	uint8_t* p = buf + sizeof(cl_msg);
	uint16_t n_fields = 0;

	if (ctx->selection.result == AS_QUERY_PLAN_SINDEX) {
		const char* si_name = ctx->selection.si->iname;
		uint32_t name_len = (uint32_t)strlen(si_name);

		// INDEX_NAME.
		as_msg_field* name_f = (as_msg_field*)p;

		name_f->field_sz = name_len + 1;
		name_f->type = AS_MSG_FIELD_TYPE_INDEX_NAME;
		memcpy(name_f->data, si_name, name_len);
		as_msg_swap_field(name_f);
		p += sizeof(as_msg_field) + name_len;
		n_fields++;

		// INDEX_TYPE.
		as_msg_field* type_f = (as_msg_field*)p;

		type_f->field_sz = 2;
		type_f->type = AS_MSG_FIELD_TYPE_INDEX_TYPE;
		type_f->data[0] = (uint8_t)ctx->selection.si->itype;
		as_msg_swap_field(type_f);
		p += sizeof(as_msg_field) + 1;
		n_fields++;

		// INDEX_RANGE.
		as_msg_field* range_f = (as_msg_field*)p;
		uint32_t range_len =
				plan_build_range_payload(range_f->data, &ctx->selection.match);

		range_f->field_sz = range_len + 1;
		range_f->type = AS_MSG_FIELD_TYPE_INDEX_RANGE;
		as_msg_swap_field(range_f);
		p += sizeof(as_msg_field) + range_len;
		n_fields++;
	}

	size_t fields_sz = (size_t)(p - buf) - sizeof(cl_msg);
	size_t msg_sz = (size_t)(p - buf);

	cl_msg* msgp = (cl_msg*)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = (uint32_t)(sizeof(as_msg) + fields_sz);
	as_proto_swap(&msgp->proto);

	as_msg* m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = AS_MSG_INFO3_LAST;
	m->info4 = 0;
	m->result_code = plan_wire_result_code(ctx);
	m->generation = 0;
	m->record_ttl = 0;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = 0;

	as_msg_swap_header(m);

	int send_rv = cf_socket_send_all(&fd_h->sock, buf, msg_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT);

	if (geo_extra != 0) {
		cf_free(buf);
	}

	if (send_rv < 0) {
		cf_warning(AS_QUERY, "{%s} query-plan: send fail fd=%d sz=%zu",
				ctx->ns->name, CSFD(&fd_h->sock), msg_sz);
		as_end_of_transaction_force_close(fd_h);
		return;
	}

	as_end_of_transaction_ok(fd_h);
}

static uint32_t
plan_build_range_payload(uint8_t* buf, const as_exp_sindex_candidate* match)
{
	uint8_t* p = buf;

	*p++ = 1; // n_ranges = 1

	uint8_t bin_name_len = (uint8_t)match->bin_name_sz;

	*p++ = bin_name_len;
	memcpy(p, match->bin_name, bin_name_len);
	p += bin_name_len;

	*p++ = (uint8_t)match->ktype;

	switch (match->ktype) {
	case AS_PARTICLE_TYPE_INTEGER:
		p = plan_append_int64_bound(p, match->bval_low);
		p = plan_append_int64_bound(p, match->bval_high);
		break;
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_GEOJSON: {
		cf_assert(match->bval_low == match->bval_high, AS_QUERY,
				"query-plan: hash-type range bound must be EQ");
		cf_assert(match->ktype != AS_PARTICLE_TYPE_GEOJSON ||
						match->geo_bound_val != NULL,
				AS_QUERY, "query-plan: GEOJSON candidate missing geo_bound_val");

		const uint8_t* bound_val = match->geo_bound_val != NULL
				? match->geo_bound_val
				: match->bound_val;

		p = plan_append_bytes_bound(p, bound_val, match->bound_val_sz);
		break;
	}
	default:
		cf_crash(AS_QUERY,
				"query-plan: unsupported ktype %d in INDEX_RANGE payload",
				match->ktype);
	}

	return (uint32_t)(p - buf);
}

static uint8_t*
plan_append_bytes_bound(uint8_t* p, const uint8_t* val, uint32_t val_sz)
{
	*(uint32_t*)p = cf_swap_to_be32(val_sz);
	p += sizeof(uint32_t);

	if (val_sz != 0) {
		memcpy(p, val, val_sz);
		p += val_sz;
	}

	return p;
}

static uint8_t*
plan_append_int64_bound(uint8_t* p, int64_t val)
{
	uint64_t net = cf_swap_to_be64((uint64_t)val);

	return plan_append_bytes_bound(p, (const uint8_t*)&net, sizeof(net));
}

static uint8_t
plan_wire_result_code(const plan_ctx* ctx)
{
	switch (ctx->selection.result) {
	case AS_QUERY_PLAN_PI:
	case AS_QUERY_PLAN_SINDEX:
		return AS_OK;
	case AS_QUERY_PLAN_FILTERED_OUT:
		return AS_ERR_FILTERED_OUT;
	case AS_QUERY_PLAN_ERROR:
	default:
		cf_crash(AS_QUERY, "{%s} query-plan: unexpected result %s",
				ctx->ns->name, plan_result_str(ctx->selection.result));
	}
}

static const char*
plan_result_str(as_query_plan_result result)
{
	switch (result) {
	case AS_QUERY_PLAN_PI:
		return "PI";
	case AS_QUERY_PLAN_SINDEX:
		return "SINDEX";
	case AS_QUERY_PLAN_FILTERED_OUT:
		return "FILTERED_OUT";
	case AS_QUERY_PLAN_ERROR:
		return "ERROR";
	default:
		return "UNKNOWN";
	}
}
