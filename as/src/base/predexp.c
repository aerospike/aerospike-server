/*
 * predexp.c
 *
 * Copyright (C) 2016-2017 Aerospike, Inc.
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

#include "base/predexp.h"

#include <inttypes.h>
#include <regex.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_map.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"

#include "base/particle.h"
#include "geospatial/geospatial.h"
#include "storage/storage.h"

typedef enum {
	PREDEXP_FALSE = 0,		// Matching nodes only
	PREDEXP_TRUE = 1,		// Matching nodes only
	PREDEXP_UNKNOWN = 2,	// Matching nodes only
	PREDEXP_VALUE = 3,		// Value nodes only
	PREDEXP_NOVALUE = 4		// Value nodes only
} predexp_retval_t;

typedef struct wrapped_as_bin_s {
	as_bin	bin;
	bool	must_free;
} wrapped_as_bin_t;

// Called to destroy a predexp when no longer needed.
typedef void (*predexp_eval_dtor_fn)(predexp_eval_t* bp);

typedef predexp_retval_t (*predexp_eval_eval_fn)(predexp_eval_t* bp,
												 predexp_args_t* argsp,
												 wrapped_as_bin_t* wbinp);

// Convenience macro, converts boolean to retval.
#define PREDEXP_RETVAL(bb)	((bb) ? PREDEXP_TRUE : PREDEXP_FALSE)

#define PREDEXP_VALUE_NODE			0x01	// represents a value
#define PREDEXP_IMMEDIATE_NODE		0x02	// constant per-query value

struct predexp_eval_base_s {
	predexp_eval_t*			next;
	predexp_eval_dtor_fn	dtor_fn;
	predexp_eval_eval_fn	eval_fn;
	uint8_t					flags;
	uint8_t					type;
};

struct predexp_var_s {
	char					vname[AS_BIN_NAME_MAX_SZ];
	as_bin					bin;
	as_predexp_var_t*		next;
};

// This function can set bin values for all bloblike types (strings)

extern const as_particle_vtable *particle_vtable[];

#if 0
static void predexp_eval_base_dtor(predexp_eval_t* bp)
{
	cf_free(bp);
}
#endif

static void predexp_eval_base_init(predexp_eval_t* bp,
								   predexp_eval_dtor_fn	dtor_fn,
								   predexp_eval_eval_fn eval_fn,
								   uint8_t flags,
								   uint8_t type)
{
	bp->next = NULL;
	bp->dtor_fn = dtor_fn;
	bp->eval_fn = eval_fn;
	bp->flags = flags;
	bp->type = type;
}

// ----------------------------------------------------------------
// Helper Functions
// ----------------------------------------------------------------

static void
destroy_list(predexp_eval_t* bp)
{
	while (bp != NULL) {
		predexp_eval_t* next = bp->next;
		(*bp->dtor_fn)(bp);
		bp = next;
	}
}

// ----------------------------------------------------------------
// Tag Definitions
// ----------------------------------------------------------------

// TODO - put in common to share with clients?
//
#define AS_PREDEXP_AND					1
#define AS_PREDEXP_OR					2
#define AS_PREDEXP_NOT					3

#define AS_PREDEXP_INTEGER_VALUE		10
#define AS_PREDEXP_STRING_VALUE			11
#define AS_PREDEXP_GEOJSON_VALUE		12

#define AS_PREDEXP_INTEGER_BIN			100
#define AS_PREDEXP_STRING_BIN			101
#define AS_PREDEXP_GEOJSON_BIN			102
#define AS_PREDEXP_LIST_BIN				103
#define AS_PREDEXP_MAP_BIN				104

#define AS_PREDEXP_INTEGER_VAR			120
#define AS_PREDEXP_STRING_VAR			121
#define AS_PREDEXP_GEOJSON_VAR			122

#define AS_PREDEXP_REC_DEVICE_SIZE		150
#define AS_PREDEXP_REC_LAST_UPDATE		151
#define AS_PREDEXP_REC_VOID_TIME		152
#define AS_PREDEXP_REC_DIGEST_MODULO	153

#define AS_PREDEXP_INTEGER_EQUAL		200
#define AS_PREDEXP_INTEGER_UNEQUAL		201
#define AS_PREDEXP_INTEGER_GREATER		202
#define AS_PREDEXP_INTEGER_GREATEREQ	203
#define AS_PREDEXP_INTEGER_LESS			204
#define AS_PREDEXP_INTEGER_LESSEQ		205

#define AS_PREDEXP_STRING_EQUAL			210
#define AS_PREDEXP_STRING_UNEQUAL		211
#define AS_PREDEXP_STRING_REGEX			212

#define AS_PREDEXP_GEOJSON_WITHIN		220
#define AS_PREDEXP_GEOJSON_CONTAINS		221

#define AS_PREDEXP_LIST_ITERATE_OR		250
#define AS_PREDEXP_MAPKEY_ITERATE_OR	251
#define AS_PREDEXP_MAPVAL_ITERATE_OR	252
#define AS_PREDEXP_LIST_ITERATE_AND		253
#define AS_PREDEXP_MAPKEY_ITERATE_AND	254
#define AS_PREDEXP_MAPVAL_ITERATE_AND	255

// ----------------------------------------------------------------
// AS_PREDEXP_AND
// ----------------------------------------------------------------

typedef struct {
	predexp_eval_t		base;
	predexp_eval_t*		child;
} predexp_eval_and_t;

static void
destroy_and(predexp_eval_t* bp)
{
	predexp_eval_and_t* dp = (predexp_eval_and_t *) bp;
	destroy_list(dp->child);
	cf_free(dp);
}

static predexp_retval_t
eval_and(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	predexp_eval_and_t* dp = (predexp_eval_and_t *) bp;

	// Start optimistically.
	predexp_retval_t retval = PREDEXP_TRUE;

	// Scan the children.
	for (predexp_eval_t* cp = dp->child; cp != NULL; cp = cp->next) {

		switch ((*cp->eval_fn)(cp, argsp, NULL)) {
		case PREDEXP_FALSE:
			// Shortcut, skip remaining children.
			return PREDEXP_FALSE;
		case PREDEXP_UNKNOWN:
			// Downgrade our return value, continue scanning children.
			retval = PREDEXP_UNKNOWN;
			break;
		case PREDEXP_TRUE:
			// Continue scanning children.
			break;
		case PREDEXP_VALUE:
		case PREDEXP_NOVALUE:
			// Child can't be value node; shouldn't ever happen.
			cf_crash(AS_PREDEXP, "eval_and child was value node");
		}
	}

	return retval;
}

static bool
build_and(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	if (len != sizeof(uint16_t)) {
		cf_warning(AS_PREDEXP, "predexp_and: unexpected size %d", len);
		return false;
	}
	uint16_t nterms = cf_swap_from_be16(* (uint16_t *) pp);
	pp += sizeof(uint16_t);

	predexp_eval_and_t* dp =
		(predexp_eval_and_t *) cf_malloc(sizeof(predexp_eval_and_t));

	// Start optimistically.
	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_and,
						   eval_and,
						   0,
						   AS_PARTICLE_TYPE_NULL);
	dp->child = NULL;

	for (uint16_t ndx = 0; ndx < nterms; ++ndx) {
		// If there is not an available child expr cleanup and fail.
		if (! *stackpp) {
			cf_warning(AS_PREDEXP, "predexp_and: missing child %d", ndx);
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}

		// Transfer the expr at the top of the stack to our child list.
		predexp_eval_t* child;
		child = *stackpp;			// Child from the top of the stack.
		*stackpp = child->next;		// Stack points around the child.
		child->next = dp->child;	// Child now points to prior list head.
		dp->child = child;			// Child is now the top of our list.

		// Make sure the child is not a value node.
		if (dp->child->flags & PREDEXP_VALUE_NODE) {
			cf_warning(AS_PREDEXP, "predexp_and: child %d is value node", ndx);
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;			// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_and(%d)", stackpp, nterms);
	
	return true;
}

// ----------------------------------------------------------------
// AS_PREDEXP_OR
// ----------------------------------------------------------------

typedef struct {
	predexp_eval_t		base;
	predexp_eval_t*		child;
} predexp_eval_or_t;

static void
destroy_or(predexp_eval_t* bp)
{
	predexp_eval_or_t* dp = (predexp_eval_or_t *) bp;
	destroy_list(dp->child);
	cf_free(dp);
}

static predexp_retval_t
eval_or(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	predexp_eval_or_t* dp = (predexp_eval_or_t *) bp;

	// Start pessimistically.
	predexp_retval_t retval = PREDEXP_FALSE;

	// Scan the children.
	for (predexp_eval_t* cp = dp->child; cp != NULL; cp = cp->next) {
		switch ((*cp->eval_fn)(cp, argsp, NULL)) {
		case PREDEXP_TRUE:
			// Shortcut, skip remaining children.
			return PREDEXP_TRUE;
		case PREDEXP_UNKNOWN:
			// Upgrade our return value, continue scanning children.
			retval = PREDEXP_UNKNOWN;
			break;
		case PREDEXP_FALSE:
			// Continue scanning children.
			break;
		case PREDEXP_VALUE:
		case PREDEXP_NOVALUE:
			// Child can't be value node; shouldn't ever happen.
			cf_crash(AS_PREDEXP, "eval_or child was value node");
		}
	}

	return retval;
}

static bool
build_or(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	if (len != sizeof(uint16_t)) {
		cf_warning(AS_PREDEXP, "predexp_or: unexpected size %d", len);
		return false;
	}
	uint16_t nterms = cf_swap_from_be16(* (uint16_t *) pp);
	pp += sizeof(uint16_t);

	predexp_eval_or_t* dp =
		(predexp_eval_or_t *) cf_malloc(sizeof(predexp_eval_or_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_or,
						   eval_or,
						   0,
						   AS_PARTICLE_TYPE_NULL);
	dp->child = NULL;

	for (uint16_t ndx = 0; ndx < nterms; ++ndx) {
		// If there is not an available child expr cleanup and fail.
		if (! *stackpp) {
			cf_warning(AS_PREDEXP, "predexp_or: missing child %d", ndx);
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
		// Transfer the expr at the top of the stack to our child list.
		predexp_eval_t* child;
		child = *stackpp;			// Child from the top of the stack.
		*stackpp = child->next;		// Stack points around the child.
		child->next = dp->child;	// Child now points to prior list head.
		dp->child = child;			// Child is now the top of our list.

		// Make sure the child is not a value node.
		if (dp->child->flags & PREDEXP_VALUE_NODE) {
			cf_warning(AS_PREDEXP, "predexp_or: child %d is value node", ndx);
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;			// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_or(%d)", stackpp, nterms);
	
	return true;
}

// ----------------------------------------------------------------
// AS_PREDEXP_NOT
// ----------------------------------------------------------------

typedef struct {
	predexp_eval_t		base;
	predexp_eval_t*		child;
} predexp_eval_not_t;

static void
destroy_not(predexp_eval_t* bp)
{
	predexp_eval_not_t* dp = (predexp_eval_not_t *) bp;
	destroy_list(dp->child);
	cf_free(dp);
}

static predexp_retval_t
eval_not(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	predexp_eval_not_t* dp = (predexp_eval_not_t *) bp;

	predexp_eval_t* cp = dp->child;

	switch ((*cp->eval_fn)(cp, argsp, NULL)) {
	case PREDEXP_FALSE:
		return PREDEXP_TRUE;
	case PREDEXP_UNKNOWN:
		return PREDEXP_UNKNOWN;
	case PREDEXP_TRUE:
		return PREDEXP_FALSE;
	case PREDEXP_VALUE:
	case PREDEXP_NOVALUE:
		// Child can't be value node; shouldn't ever happen.
		cf_crash(AS_PREDEXP, "eval_not child was value node");
	}

	return PREDEXP_UNKNOWN;	// Can't get here, makes compiler happy.
}

static bool
build_not(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	if (len != 0) {
		cf_warning(AS_PREDEXP, "predexp_not: unexpected size %d", len);
		return false;
	}

	predexp_eval_not_t* dp =
		(predexp_eval_not_t *) cf_malloc(sizeof(predexp_eval_not_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_not,
						   eval_not,
						   0,
						   AS_PARTICLE_TYPE_NULL);
	dp->child = NULL;

	// If there is not an available child expr cleanup and fail.
	if (! *stackpp) {
		cf_warning(AS_PREDEXP, "predexp_not: missing child");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}
	// Transfer the expr at the top of the stack to our child list.
	predexp_eval_t* child;
	child = *stackpp;			// Child from the top of the stack.
	*stackpp = child->next;		// Stack points around the child.
	child->next = dp->child;	// Child now points to prior list head.
	dp->child = child;			// Child is now the top of our list.

	// Make sure the child is not a value node.
	if (dp->child->flags & PREDEXP_VALUE_NODE) {
		cf_warning(AS_PREDEXP, "predexp_not: child is value node");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;			// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_not", stackpp);
	
	return true;
}

// ----------------------------------------------------------------
// AS_PREDEXP_*_COMPARE
// ----------------------------------------------------------------

// GEOSPATIAL NOTES:
//
// We want to perform all possible computation on the query region
// once, prior to visiting all the points.  The current value
// interface is opaque; it returns a bin particle only; there is no
// way to pass associated precomputed state.  So we keep the
// precomputed region query state here in the comparison node instead.
//
// IMPROVEMENTS:
//
// We currently parse the incoming query (IMMEDIATE) region twice;
// once in the from_wire_fn and again explicitly in the build_compare
// routine, this time retaining the region.  Maybe we should make an
// exposed as_geojson_from_wire which additionally returns the
// computed region; the particle geojson_from_wire could call this
// routine and then discard the region.
//
// We can improve the performance of the comparison by covering the
// region at build time and saving all of the cell min/max ranges.
// Candidate points can first be checked against the list of ranges to
// make sure they are a rough match before performing the more
// expensive strict region match.  This change requires a bunch more
// state; probably we'll want a pointer to the
// predexp_eval_geojson_state_t instead of using a union at this
// point.

typedef struct predexp_eval_geojson_state_s {
	uint64_t				cellid;
	geo_region_t			region;
} predexp_eval_geojson_state_t;

typedef struct predexp_eval_regex_state_s {
	regex_t					regex;
	bool					iscompiled;
} predexp_eval_regex_state_t;

typedef struct predexp_eval_compare_s {
	predexp_eval_t			base;
	uint16_t				tag;
	uint8_t					type;
	predexp_eval_t*			lchild;
	predexp_eval_t*			rchild;
	union {
		predexp_eval_geojson_state_t	geojson;
		predexp_eval_regex_state_t		regex;
	} state;
} predexp_eval_compare_t;

static void
destroy_compare(predexp_eval_t* bp)
{
	predexp_eval_compare_t* dp = (predexp_eval_compare_t *) bp;
	if (dp->lchild) {
		(*dp->lchild->dtor_fn)(dp->lchild);
	}
	if (dp->rchild) {
		(*dp->rchild->dtor_fn)(dp->rchild);
	}
	if (dp->type == AS_PARTICLE_TYPE_GEOJSON && dp->state.geojson.region) {
		geo_region_destroy(dp->state.geojson.region);
	}
	if (dp->tag == AS_PREDEXP_STRING_REGEX && dp->state.regex.iscompiled) {
		regfree(&dp->state.regex.regex);
	}
	cf_free(dp);
}

static predexp_retval_t
eval_compare(predexp_eval_t* bp,
			 predexp_args_t* argsp,
			 wrapped_as_bin_t* wbinp)
{
	predexp_eval_compare_t* dp = (predexp_eval_compare_t *) bp;

	predexp_retval_t retval = PREDEXP_UNKNOWN;

	wrapped_as_bin_t lwbin;
	wrapped_as_bin_t rwbin;
	lwbin.must_free = false;
	rwbin.must_free = false;

	// Fetch the child values.  Are either of the values unknown?
	// During the metadata phase this returns PREDEXP_UNKNOWN.  During
	// the record phase we consider a comparison with an unknown value
	// to be PREDEXP_FALSE (missing bin or bin or wrong type).

	if ((*dp->lchild->eval_fn)(dp->lchild, argsp, &lwbin) ==
		PREDEXP_NOVALUE) {
		retval = argsp->rd ? PREDEXP_FALSE : PREDEXP_UNKNOWN;
		goto Cleanup;
	}

	if ((*dp->rchild->eval_fn)(dp->rchild, argsp, &rwbin) ==
		PREDEXP_NOVALUE) {
		retval = argsp->rd ? PREDEXP_FALSE : PREDEXP_UNKNOWN;
		goto Cleanup;
	}

	switch (dp->type) {
	case AS_PARTICLE_TYPE_INTEGER: {
		int64_t lval = as_bin_particle_integer_value(&lwbin.bin);
		int64_t rval = as_bin_particle_integer_value(&rwbin.bin);
		switch (dp->tag) {
		case AS_PREDEXP_INTEGER_EQUAL:
			retval = PREDEXP_RETVAL(lval == rval);
			goto Cleanup;
		case AS_PREDEXP_INTEGER_UNEQUAL:
			retval = PREDEXP_RETVAL(lval != rval);
			goto Cleanup;
		case AS_PREDEXP_INTEGER_GREATER:
			retval = PREDEXP_RETVAL(lval >  rval);
			goto Cleanup;
		case AS_PREDEXP_INTEGER_GREATEREQ:
			retval = PREDEXP_RETVAL(lval >= rval);
			goto Cleanup;
		case AS_PREDEXP_INTEGER_LESS:
			retval = PREDEXP_RETVAL(lval <  rval);
			goto Cleanup;
		case AS_PREDEXP_INTEGER_LESSEQ:
			retval = PREDEXP_RETVAL(lval <= rval);
			goto Cleanup;
		default:
			cf_crash(AS_PREDEXP, "eval_compare integer unknown tag %d",
					 dp->tag);
		}
	}
	case AS_PARTICLE_TYPE_STRING: {
		// We always need to fetch the left argument.
		char* lptr;
		uint32_t llen = as_bin_particle_string_ptr(&lwbin.bin, &lptr);
		char* rptr;
		uint32_t rlen;
		switch (dp->tag) {
		case AS_PREDEXP_STRING_EQUAL:
		case AS_PREDEXP_STRING_UNEQUAL:
			// These comparisons need the right argument too.
			rlen = as_bin_particle_string_ptr(&rwbin.bin, &rptr);
			bool isequal = (llen == rlen) && (memcmp(lptr, rptr, llen) == 0);
			switch (dp->tag) {
			case AS_PREDEXP_STRING_EQUAL:
				retval = isequal;
				goto Cleanup;
			case AS_PREDEXP_STRING_UNEQUAL:
				retval = ! isequal;
				goto Cleanup;
			default:
				cf_crash(AS_PREDEXP, "eval_compare string (eq) unknown tag %d",
						 dp->tag);
			}
		case AS_PREDEXP_STRING_REGEX: {
			char* tmpstr = cf_strndup(lptr, llen);
			int rv = regexec(&dp->state.regex.regex, tmpstr, 0, NULL, 0);
			cf_free(tmpstr);
			retval = rv == 0;
			goto Cleanup;
		}
		default:
			cf_crash(AS_PREDEXP, "eval_compare string unknown tag %d", dp->tag);
		}
	}
	case AS_PARTICLE_TYPE_GEOJSON: {
		// as_particle* lpart = lbinp->particle;
		// as_particle* rpart = rbinp->particle;

		switch (dp->tag) {
		case AS_PREDEXP_GEOJSON_WITHIN:
		case AS_PREDEXP_GEOJSON_CONTAINS: {
			bool isstrict = true;
			bool ismatch = as_particle_geojson_match(lwbin.bin.particle,
													 dp->state.geojson.cellid,
													 dp->state.geojson.region,
													 isstrict);
			retval = PREDEXP_RETVAL(ismatch);
			goto Cleanup;
		}
		default:
			cf_crash(AS_PREDEXP, "eval_compare geojson unknown tag %d",
					 dp->tag);
		}
	}
	default:
		cf_crash(AS_PREDEXP, "eval_compare unknown type %d", dp->type);
	}

 Cleanup:
	if (lwbin.must_free) {
		cf_crash(AS_PREDEXP, "eval_compare need bin cleanup, didn't before");
	}
	if (rwbin.must_free) {
		cf_crash(AS_PREDEXP, "eval_compare need bin cleanup, didn't before");
	}
	return retval;
}

static bool
build_compare(predexp_eval_t** stackpp,
					  uint32_t len,
					  uint8_t* pp,
					  uint16_t tag)
{
	predexp_eval_compare_t* dp = (predexp_eval_compare_t *)
			cf_malloc(sizeof(predexp_eval_compare_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_compare,
						   eval_compare,
						   0,
						   AS_PARTICLE_TYPE_NULL);

	dp->tag = tag;
	dp->lchild = NULL;
	dp->rchild = NULL;

	// IMPORTANT - If your state doesn't want to be initialized
	// to all 0 rethink this ...
	//
	memset(&dp->state, 0, sizeof(dp->state));

	switch (tag) {
	case AS_PREDEXP_INTEGER_EQUAL:
	case AS_PREDEXP_INTEGER_UNEQUAL:
	case AS_PREDEXP_INTEGER_GREATER:
	case AS_PREDEXP_INTEGER_GREATEREQ:
	case AS_PREDEXP_INTEGER_LESS:
	case AS_PREDEXP_INTEGER_LESSEQ:
		dp->type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case AS_PREDEXP_STRING_EQUAL:
	case AS_PREDEXP_STRING_UNEQUAL:
	case AS_PREDEXP_STRING_REGEX:
		dp->type = AS_PARTICLE_TYPE_STRING;
		break;
	case AS_PREDEXP_GEOJSON_WITHIN:
	case AS_PREDEXP_GEOJSON_CONTAINS:
		dp->type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	default:
		cf_crash(AS_PREDEXP, "build_compare called with bogus tag: %d", tag);
		break;
	}

	uint8_t* endp = pp + len;

	uint32_t regex_opts = 0;
	if (tag == AS_PREDEXP_STRING_REGEX) {
		// This comparison takes a uint32_t opts argument.
		if (pp + sizeof(uint32_t) > endp) {
			cf_warning(AS_PREDEXP, "build_compare: regex opts past end");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
		regex_opts = cf_swap_from_be32(* (uint32_t *) pp);
		pp += sizeof(uint32_t);
	}

	// No arguments.
	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_compare: msg unaligned");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	// ---- Pop the right child off the stack.

	if (! *stackpp) {
		cf_warning(AS_PREDEXP, "predexp_compare: missing right child");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	dp->rchild = *stackpp;
	*stackpp = dp->rchild->next;
	dp->rchild->next = NULL;

	if ((dp->rchild->flags & PREDEXP_VALUE_NODE) == 0) {
		cf_warning(AS_PREDEXP,
				   "predexp compare: right child is not value node");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	if (dp->rchild->type != dp->type) {
		cf_warning(AS_PREDEXP, "predexp compare: right child is wrong type");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	// ---- Pop the left child off the stack.

	if (! *stackpp) {
		cf_warning(AS_PREDEXP, "predexp_compare: missing left child");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	dp->lchild = *stackpp;
	*stackpp = dp->lchild->next;
	dp->lchild->next = NULL;

	if ((dp->lchild->flags & PREDEXP_VALUE_NODE) == 0) {
		cf_warning(AS_PREDEXP, "predexp compare: left child is not value node");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	if (dp->lchild->type != dp->type) {
		cf_warning(AS_PREDEXP, "predexp compare: left child is wrong type");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	switch (tag) {
	case AS_PREDEXP_GEOJSON_WITHIN:
	case AS_PREDEXP_GEOJSON_CONTAINS:
		// The right child needs to be an immediate value.
		if ((dp->rchild->flags & PREDEXP_IMMEDIATE_NODE) == 0) {
			cf_warning(AS_PREDEXP,
					   "predexp compare: within arg not immediate GeoJSON");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}

		// Extract the query GeoJSON value.
		predexp_args_t* argsp = NULL;	// immediate values don't need args
		wrapped_as_bin_t rwbin;
		rwbin.must_free = false;
		if ((*dp->rchild->eval_fn)(dp->rchild, argsp, &rwbin) ==
			PREDEXP_NOVALUE) {
			cf_warning(AS_PREDEXP,
					   "predexp compare: within arg had unknown value");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
		size_t sz;
		char const * ptr = as_geojson_mem_jsonstr(rwbin.bin.particle, &sz);

		// Parse the child, save the computed state.
		if (!geo_parse(NULL, ptr, sz,
					   &dp->state.geojson.cellid,
					   &dp->state.geojson.region)) {
			cf_warning(AS_PREDEXP, "predexp compare: failed to parse GeoJSON");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			if (rwbin.must_free) {
				cf_crash(AS_PREDEXP,
						 "predexp compare now needs bin destructor");
			}
			return false;
		}
		if (rwbin.must_free) {
			cf_crash(AS_PREDEXP, "predexp compare now needs bin destructor");
		}
		break;
	case AS_PREDEXP_STRING_REGEX:
		// The right child needs to be an immediate value.
		if ((dp->rchild->flags & PREDEXP_IMMEDIATE_NODE) == 0) {
			cf_warning(AS_PREDEXP,
					   "predexp compare: regex arg not immediate string");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}

		// Extract the query regex value.
		predexp_args_t* argsp2 = NULL;	// immediate values don't need args
		wrapped_as_bin_t rwbin2;
		rwbin2.must_free = false;
		if ((*dp->rchild->eval_fn)(dp->rchild, argsp2, &rwbin2) ==
			PREDEXP_NOVALUE) {
			cf_warning(AS_PREDEXP,
					   "predexp compare: regex arg had unknown value");
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			return false;
		}
		char* rptr;
		uint32_t rlen = as_bin_particle_string_ptr(&rwbin2.bin, &rptr);
		char* tmpregexp = cf_strndup(rptr, rlen);
		int rv = regcomp(&dp->state.regex.regex, tmpregexp, regex_opts);
		cf_free(tmpregexp);
		if (rv != 0) {
			char errbuf[1024];
			regerror(rv, &dp->state.regex.regex, errbuf, sizeof(errbuf));
			cf_warning(AS_PREDEXP, "predexp compare: regex compile failed: %s",
					   errbuf);
			(*dp->base.dtor_fn)((predexp_eval_t *) dp);
			if (rwbin2.must_free) {
				cf_crash(AS_PREDEXP,
						 "predexp compare now needs bin destructor");
			}
			return false;
		}
		dp->state.regex.iscompiled = true;
		if (rwbin2.must_free) {
			cf_crash(AS_PREDEXP, "predexp compare now needs bin destructor");
		}
		break;

	default:
		// Don't do anything for the others ...
		break;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;			// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	switch (tag) {
	case AS_PREDEXP_INTEGER_EQUAL:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_equal", stackpp);
		break;
	case AS_PREDEXP_INTEGER_UNEQUAL:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_unequal", stackpp);
		break;
	case AS_PREDEXP_INTEGER_GREATER:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_greater", stackpp);
		break;
	case AS_PREDEXP_INTEGER_GREATEREQ:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_greatereq", stackpp);
		break;
	case AS_PREDEXP_INTEGER_LESS:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_less", stackpp);
		break;
	case AS_PREDEXP_INTEGER_LESSEQ:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_lesseq", stackpp);
		break;
	case AS_PREDEXP_STRING_EQUAL:
		cf_debug(AS_PREDEXP, "%p: predexp_string_equal", stackpp);
		break;
	case AS_PREDEXP_STRING_UNEQUAL:
		cf_debug(AS_PREDEXP, "%p: predexp_string_unequal", stackpp);
		break;
	case AS_PREDEXP_STRING_REGEX:
		cf_debug(AS_PREDEXP, "%p: predexp_string_regex(%d)", stackpp,
				 regex_opts);
		break;
	case AS_PREDEXP_GEOJSON_WITHIN:
		cf_debug(AS_PREDEXP, "%p: predexp_geojson_within", stackpp);
		break;
	case AS_PREDEXP_GEOJSON_CONTAINS:
		cf_debug(AS_PREDEXP, "%p: predexp_geojson_contains", stackpp);
		break;
	default:
		cf_crash(AS_PREDEXP, "build_compare called with bogus tag: %d", tag);
		break;
	}

	return true;
}

// ----------------------------------------------------------------
// AS_PREDEXP_*_VALUE
// ----------------------------------------------------------------

typedef struct predexp_eval_value_s {
	predexp_eval_t			base;
	as_bin					bin;
	uint8_t					type;
} predexp_eval_value_t;

static void
destroy_value(predexp_eval_t* bp)
{
	predexp_eval_value_t* dp = (predexp_eval_value_t *) bp;
	as_bin_particle_destroy(&dp->bin, true);
	cf_free(dp);
}

static predexp_retval_t
eval_value(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP, "eval_value called outside value context");
	}

	predexp_eval_value_t* dp = (predexp_eval_value_t *) bp;
	// We don't have a ns in this context.  But the source bin doesn't
	// have any name index stuff anyway ...
	as_single_bin_copy(&wbinp->bin, &dp->bin);
	wbinp->must_free = false;	// bin is constant, destroyed after query above
	return PREDEXP_VALUE;
}

static bool
build_value(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp, uint16_t tag)
{
	predexp_eval_value_t* dp = (predexp_eval_value_t *)
			cf_malloc(sizeof(predexp_eval_value_t));

	uint8_t type;
	switch (tag) {
	case AS_PREDEXP_INTEGER_VALUE: type = AS_PARTICLE_TYPE_INTEGER; break;
	case AS_PREDEXP_STRING_VALUE: type = AS_PARTICLE_TYPE_STRING; break;
	case AS_PREDEXP_GEOJSON_VALUE: type = AS_PARTICLE_TYPE_GEOJSON; break;
	default:
		cf_crash(AS_PREDEXP, "build_value called with bogus tag: %d", tag);
		return false;
	}

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_value,
						   eval_value,
						   PREDEXP_VALUE_NODE | PREDEXP_IMMEDIATE_NODE,
						   type);

	as_bin_set_empty(&dp->bin);
	dp->bin.particle = NULL;

	uint8_t* endp = pp + len;

	size_t vallen = len;
	void* valptr = (char*) pp;
	pp += vallen;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "predexp value: msg unaligned");
		goto Failed;
	}

	int32_t mem_size = particle_vtable[type]->size_from_wire_fn(valptr, vallen);

	if (mem_size < 0) {
		goto Failed;
	}

	if (mem_size != 0) {
		dp->bin.particle = cf_malloc((size_t)mem_size);
	}

	int result = particle_vtable[type]->from_wire_fn(type,
													 valptr,
													 vallen,
													 &dp->bin.particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(&dp->bin, type);
	}
	else {
		cf_warning(AS_PREDEXP, "failed to build predexp value with err %d",
				   result);
		if (mem_size != 0) {
			cf_free(dp->bin.particle);
		}
		as_bin_set_empty(&dp->bin);
		dp->bin.particle = NULL;
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;		// We're the new top

	switch (tag) {
	case AS_PREDEXP_INTEGER_VALUE:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_value(%"PRId64")", stackpp,
				 (int64_t) dp->bin.particle);
		break;
	case AS_PREDEXP_STRING_VALUE: {
		cf_debug(AS_PREDEXP, "%p: predexp_string_value(\"%s\")", stackpp,
				 CF_ZSTR1K(valptr, vallen));
		break;
	}
	case AS_PREDEXP_GEOJSON_VALUE: {
		size_t jsonsz;
		char const * jsonptr =
			as_geojson_mem_jsonstr(dp->bin.particle, &jsonsz);
		cf_debug(AS_PREDEXP, "%p: predexp_geojson_value(%s)", stackpp,
				CF_ZSTR1K(jsonptr, jsonsz));
		break;
	}
	default:
		cf_crash(AS_PREDEXP, "build_value called with bogus tag: %d", tag);
		break;
	}

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_*_BIN
// ----------------------------------------------------------------

typedef struct predexp_eval_bin_s {
	predexp_eval_t			base;
	char					bname[AS_BIN_NAME_MAX_SZ];
	uint8_t					type;
} predexp_eval_bin_t;

static void
destroy_bin(predexp_eval_t* bp)
{
	predexp_eval_bin_t* dp = (predexp_eval_bin_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_bin(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP, "eval_bin called outside value context");
	}

	predexp_eval_bin_t* dp = (predexp_eval_bin_t *) bp;

	// We require record data to operate.
	if (! argsp->rd) {
		return PREDEXP_NOVALUE;
	}

	as_bin* bb = as_bin_get(argsp->rd, dp->bname);
	if (! bb) {
		return PREDEXP_NOVALUE;
	}

	if (as_bin_get_particle_type(bb) != dp->type) {
		return PREDEXP_NOVALUE;
	}

	as_bin_copy(argsp->ns, &wbinp->bin, bb);
	wbinp->must_free = false;	// bin is owned by record, in caller
	return PREDEXP_VALUE;
}

static bool
build_bin(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp, uint16_t tag)
{
	predexp_eval_bin_t* dp = (predexp_eval_bin_t *)
			cf_malloc(sizeof(predexp_eval_bin_t));

	switch (tag) {
	case AS_PREDEXP_INTEGER_BIN:
		dp->type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case AS_PREDEXP_STRING_BIN:
		dp->type = AS_PARTICLE_TYPE_STRING;
		break;
	case AS_PREDEXP_GEOJSON_BIN:
		dp->type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	case AS_PREDEXP_LIST_BIN:
		dp->type = AS_PARTICLE_TYPE_LIST;
		break;
	case AS_PREDEXP_MAP_BIN:
		dp->type = AS_PARTICLE_TYPE_MAP;
		break;
	default:
		cf_crash(AS_PREDEXP, "build_bin called with bogus tag: %d", tag);
		break;
	}

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_bin,
						   eval_bin,
						   PREDEXP_VALUE_NODE,
						   dp->type);

	uint8_t* endp = pp + len;

	if (len >= sizeof(dp->bname)) {
		cf_warning(AS_PREDEXP, "build_bin: binname too long");
		goto Failed;
	}
	uint8_t bnlen = (uint8_t) len;
	memcpy(dp->bname, pp, bnlen);
	dp->bname[bnlen] = '\0';
	pp += bnlen;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_bin: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	switch (tag) {
	case AS_PREDEXP_INTEGER_BIN:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_bin(\"%s\")", stackpp,
				 dp->bname);
		break;
	case AS_PREDEXP_STRING_BIN:
		cf_debug(AS_PREDEXP, "%p: predexp_string_bin(\"%s\")", stackpp,
				 dp->bname);
		break;
	case AS_PREDEXP_GEOJSON_BIN:
		cf_debug(AS_PREDEXP, "%p: predexp_geojson_bin(\"%s\")", stackpp,
				 dp->bname);
		break;
	case AS_PREDEXP_LIST_BIN:
		cf_debug(AS_PREDEXP, "%p: predexp_list_bin(\"%s\")", stackpp,
				 dp->bname);
		break;
	case AS_PREDEXP_MAP_BIN:
		cf_debug(AS_PREDEXP, "%p: predexp_map_bin(\"%s\")", stackpp,
				 dp->bname);
		break;
	default:
		cf_crash(AS_PREDEXP, "build_bin called with bogus tag: %d", tag);
		break;
	}

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_*_VAR
// ----------------------------------------------------------------

typedef struct predexp_eval_var_s {
	predexp_eval_t			base;
	char					vname[AS_BIN_NAME_MAX_SZ];
	uint8_t					type;
} predexp_eval_var_t;

static void
destroy_var(predexp_eval_t* bp)
{
	predexp_eval_var_t* dp = (predexp_eval_var_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_var(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP, "eval_var called outside value context");
	}

	predexp_eval_var_t* dp = (predexp_eval_var_t *) bp;

	for (as_predexp_var_t* vp = argsp->vl; vp != NULL; vp = vp->next) {
		if (strcmp(dp->vname, vp->vname) == 0) {
			// Is it the correct type?
			if (as_bin_get_particle_type(&vp->bin) != dp->type) {
				return PREDEXP_NOVALUE;
			}

			// Return it.
			as_bin_copy(argsp->ns, &wbinp->bin, &vp->bin);
			wbinp->must_free = false;	// bin is owned by iterator
			return PREDEXP_VALUE;
		}
	}

	// If we get here we didn't find the named variable in the list.
	return PREDEXP_NOVALUE;
}

static bool
build_var(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp, uint16_t tag)
{
	predexp_eval_var_t* dp = (predexp_eval_var_t *)
			cf_malloc(sizeof(predexp_eval_var_t));

	switch (tag) {
	case AS_PREDEXP_INTEGER_VAR:
		dp->type = AS_PARTICLE_TYPE_INTEGER;
		break;
	case AS_PREDEXP_STRING_VAR:
		dp->type = AS_PARTICLE_TYPE_STRING;
		break;
	case AS_PREDEXP_GEOJSON_VAR:
		dp->type = AS_PARTICLE_TYPE_GEOJSON;
		break;
	default:
		cf_crash(AS_PREDEXP, "build_var called with bogus tag: %d", tag);
		break;
	}

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_var,
						   eval_var,
						   PREDEXP_VALUE_NODE,
						   dp->type);

	uint8_t* endp = pp + len;

	if (len >= sizeof(dp->vname)) {
		cf_warning(AS_PREDEXP, "build_var: varname too long");
		goto Failed;
	}
	uint8_t bnlen = (uint8_t) len;
	memcpy(dp->vname, pp, bnlen);
	dp->vname[bnlen] = '\0';
	pp += bnlen;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_var: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	switch (tag) {
	case AS_PREDEXP_INTEGER_VAR:
		cf_debug(AS_PREDEXP, "%p: predexp_integer_var(\"%s\")", stackpp,
				 dp->vname);
		break;
	case AS_PREDEXP_STRING_VAR:
		cf_debug(AS_PREDEXP, "%p: predexp_string_var(\"%s\")", stackpp,
				 dp->vname);
		break;
	case AS_PREDEXP_GEOJSON_VAR:
		cf_debug(AS_PREDEXP, "%p: predexp_geojson_var(\"%s\")", stackpp,
				 dp->vname);
		break;
	default:
		cf_crash(AS_PREDEXP, "build_var called with bogus tag: %d", tag);
		break;
	}

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_REC_DEVICE_SIZE
// ----------------------------------------------------------------

typedef struct predexp_eval_rec_device_size_s {
	predexp_eval_t			base;
} predexp_eval_rec_device_size_t;

static void
destroy_rec_device_size(predexp_eval_t* bp)
{
	predexp_eval_rec_device_size_t* dp = (predexp_eval_rec_device_size_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_rec_device_size(predexp_eval_t* bp,
					 predexp_args_t* argsp,
					 wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP,
				 "eval_rec_device_size called outside value context");
	}

	// predexp_eval_rec_device_size_t* dp =
	//     (predexp_eval_rec_device_size_t *) bp;

	int64_t rec_device_size =
			(int64_t)as_storage_record_size(argsp->ns, argsp->md);

	as_bin_state_set_from_type(&wbinp->bin, AS_PARTICLE_TYPE_INTEGER);
	as_bin_particle_integer_set(&wbinp->bin, rec_device_size);
	return PREDEXP_VALUE;
}

static bool
build_rec_device_size(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	predexp_eval_rec_device_size_t* dp = (predexp_eval_rec_device_size_t *)
			cf_malloc(sizeof(predexp_eval_rec_device_size_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_rec_device_size,
						   eval_rec_device_size,
						   PREDEXP_VALUE_NODE,
						   AS_PARTICLE_TYPE_INTEGER);

	uint8_t* endp = pp + len;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_rec_device_size: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_rec_device_size()", stackpp);

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_REC_LAST_UPDATE
// ----------------------------------------------------------------

typedef struct predexp_eval_rec_last_update_s {
	predexp_eval_t			base;
	as_bin					bin;
} predexp_eval_rec_last_update_t;

static void
destroy_rec_last_update(predexp_eval_t* bp)
{
	predexp_eval_rec_last_update_t* dp = (predexp_eval_rec_last_update_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_rec_last_update(predexp_eval_t* bp,
					 predexp_args_t* argsp,
					 wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP,
				 "eval_rec_last_update called outside value context");
	}

	// predexp_eval_rec_last_update_t* dp =
	//     (predexp_eval_rec_last_update_t *) bp;

	int64_t rec_last_update_ns =
		(int64_t) cf_utc_ns_from_clepoch_ms(argsp->md->last_update_time);

	as_bin_state_set_from_type(&wbinp->bin, AS_PARTICLE_TYPE_INTEGER);
	as_bin_particle_integer_set(&wbinp->bin, rec_last_update_ns);
	return PREDEXP_VALUE;
}

static bool
build_rec_last_update(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	predexp_eval_rec_last_update_t* dp = (predexp_eval_rec_last_update_t *)
			cf_malloc(sizeof(predexp_eval_rec_last_update_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_rec_last_update,
						   eval_rec_last_update,
						   PREDEXP_VALUE_NODE,
						   AS_PARTICLE_TYPE_INTEGER);

	uint8_t* endp = pp + len;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_rec_last_update: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_rec_last_update()", stackpp);

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_REC_VOID_TIME
// ----------------------------------------------------------------

typedef struct predexp_eval_rec_void_time_s {
	predexp_eval_t			base;
	as_bin					bin;
} predexp_eval_rec_void_time_t;

static void
destroy_rec_void_time(predexp_eval_t* bp)
{
	predexp_eval_rec_void_time_t* dp = (predexp_eval_rec_void_time_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_rec_void_time(predexp_eval_t* bp,
				   predexp_args_t* argsp,
				   wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP, "eval_rec_void_time called outside value context");
	}

	// predexp_eval_rec_void_time_t* dp = (predexp_eval_rec_void_time_t *) bp;

	int64_t rec_void_time_ns =
			(int64_t) cf_utc_ns_from_clepoch_sec(argsp->md->void_time);

	// SPECIAL CASE - if the argsp->md->rec_void_time == 0 set the
	// rec_void_time_ns to 0 as well.
	//
	if (argsp->md->void_time == 0) {
		rec_void_time_ns = 0;
	}

	as_bin_state_set_from_type(&wbinp->bin, AS_PARTICLE_TYPE_INTEGER);
	as_bin_particle_integer_set(&wbinp->bin, rec_void_time_ns);
	return PREDEXP_VALUE;
}

static bool
build_rec_void_time(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	predexp_eval_rec_void_time_t* dp = (predexp_eval_rec_void_time_t *)
			cf_malloc(sizeof(predexp_eval_rec_void_time_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_rec_void_time,
						   eval_rec_void_time,
						   PREDEXP_VALUE_NODE,
						   AS_PARTICLE_TYPE_INTEGER);

	uint8_t* endp = pp + len;

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_rec_void_time: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_rec_void_time()", stackpp);

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_REC_DIGEST_MODULO
// ----------------------------------------------------------------

typedef struct predexp_eval_rec_digest_modulo_s {
	predexp_eval_t			base;
	int32_t					mod;
} predexp_eval_rec_digest_modulo_t;

static void
destroy_rec_digest_modulo(predexp_eval_t* bp)
{
	predexp_eval_rec_digest_modulo_t* dp =
		(predexp_eval_rec_digest_modulo_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_rec_digest_modulo(predexp_eval_t* bp,
				   predexp_args_t* argsp,
				   wrapped_as_bin_t* wbinp)
{
	if (wbinp == NULL) {
		cf_crash(AS_PREDEXP,
				 "eval_rec_digest_modulo called outside value context");
	}

	predexp_eval_rec_digest_modulo_t* dp =
		(predexp_eval_rec_digest_modulo_t *) bp;

	// We point at the last 4 bytes of the digest.
	uint32_t* valp = (uint32_t*) &argsp->md->keyd.digest[16];
	int64_t digest_modulo = *valp % dp->mod;

	as_bin_state_set_from_type(&wbinp->bin, AS_PARTICLE_TYPE_INTEGER);
	as_bin_particle_integer_set(&wbinp->bin, digest_modulo);
	return PREDEXP_VALUE;
}

static bool
build_rec_digest_modulo(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp)
{
	predexp_eval_rec_digest_modulo_t* dp = (predexp_eval_rec_digest_modulo_t *)
			cf_malloc(sizeof(predexp_eval_rec_digest_modulo_t));

	predexp_eval_base_init((predexp_eval_t *) dp,
						   destroy_rec_digest_modulo,
						   eval_rec_digest_modulo,
						   PREDEXP_VALUE_NODE,
						   AS_PARTICLE_TYPE_INTEGER);

	uint8_t* endp = pp + len;

	if (pp + sizeof(int32_t) > endp) {
		cf_warning(AS_PREDEXP, "build_rec_digest_modulo: msg too short");
		goto Failed;
	}

	dp->mod = cf_swap_from_be32(* (int32_t*) pp);
	pp += sizeof(int32_t);

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_rec_digest_modulo: msg unaligned");
		goto Failed;
	}

	if (dp->mod == 0) {
		cf_warning(AS_PREDEXP, "build_rec_digest_modulo: zero modulo invalid");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	cf_debug(AS_PREDEXP, "%p: predexp_rec_digest_modulo(%d)", stackpp, dp->mod);

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// AS_PREDEXP_*_ITERATE_*
// ----------------------------------------------------------------

typedef struct predexp_eval_iter_s {
	predexp_eval_t			base;
	uint16_t				tag;
	uint8_t					type;
	predexp_eval_t*			lchild;		// per-element expr
	predexp_eval_t*			rchild;		// collection
	char					vname[AS_BIN_NAME_MAX_SZ];
} predexp_eval_iter_t;

static void
destroy_iter(predexp_eval_t* bp)
{
	predexp_eval_iter_t* dp = (predexp_eval_iter_t *) bp;
	cf_free(dp);
}

static predexp_retval_t
eval_list_iter(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	predexp_eval_iter_t* dp = (predexp_eval_iter_t *) bp;

	predexp_retval_t retval = PREDEXP_UNKNOWN;  // init makes compiler happy
	switch (dp->tag) {
	case AS_PREDEXP_LIST_ITERATE_OR:
		// Start pessimistically.
		retval = PREDEXP_FALSE;
		break;
	case AS_PREDEXP_LIST_ITERATE_AND:
		// Start optimistically.
		retval = PREDEXP_TRUE;
		break;
	default:
		cf_crash(AS_PREDEXP,
				 "eval_list_iter called with bogus tag: %d", dp->tag);
	}

	wrapped_as_bin_t lwbin;
	lwbin.must_free = false;
	if ((*dp->rchild->eval_fn)(dp->rchild, argsp, &lwbin) ==
		PREDEXP_NOVALUE) {
		return argsp->rd ? PREDEXP_FALSE : PREDEXP_UNKNOWN;
	}

	as_predexp_var_t var;
	memcpy(var.vname, dp->vname, sizeof(var.vname));

	// Make sure our var starts out empty.
	as_bin_set_empty(&var.bin);
	var.bin.particle = NULL;

	// Prepend our var to the list.
	var.next = argsp->vl;
	argsp->vl = &var;

	// Traverse the collection.
	as_val* lval = as_bin_particle_to_asval(&lwbin.bin);
	as_arraylist* list = (as_arraylist*) as_list_fromval(lval);
	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, list);
	while (as_arraylist_iterator_has_next(&it)) {
		// Set our var to the element's value.
		as_val* val = (as_val*) as_arraylist_iterator_next(&it);
		int old_arena = cf_alloc_clear_ns_arena();
		int rv = as_bin_particle_replace_from_asval(&var.bin, val);
		cf_alloc_restore_ns_arena(old_arena);
		if (rv != 0) {
			cf_warning(AS_PREDEXP,
					   "eval_list_iter: particle from asval failed");
			continue;
		}

		switch (dp->tag) {
		case AS_PREDEXP_LIST_ITERATE_OR:
			switch ((*dp->lchild->eval_fn)(dp->lchild, argsp, NULL)) {
			case PREDEXP_TRUE:
				// Shortcut, skip remaining children.
				retval = PREDEXP_TRUE;
				goto Done;
			case PREDEXP_UNKNOWN:
				// Upgrade our return value, continue scanning children.
				retval = PREDEXP_UNKNOWN;
				break;
			case PREDEXP_FALSE:
				// Continue scanning children.
				break;
			case PREDEXP_VALUE:
			case PREDEXP_NOVALUE:
				// Child can't be value node; shouldn't ever happen.
				cf_crash(AS_PREDEXP, "eval_list_iter child was value node");
			}
			break;
		case AS_PREDEXP_LIST_ITERATE_AND:
			switch ((*dp->lchild->eval_fn)(dp->lchild, argsp, NULL)) {
			case PREDEXP_FALSE:
				// Shortcut, skip remaining children.
				retval = PREDEXP_FALSE;
				goto Done;
			case PREDEXP_UNKNOWN:
				// Downgrade our return value, continue scanning children.
				retval = PREDEXP_UNKNOWN;
				break;
			case PREDEXP_TRUE:
				// Continue scanning children.
				break;
			case PREDEXP_VALUE:
			case PREDEXP_NOVALUE:
				// Child can't be value node; shouldn't ever happen.
				cf_crash(AS_PREDEXP, "eval_list_iter child was value node");
			}
			break;
		default:
			cf_crash(AS_PREDEXP, "eval_list_iter called with bogus tag: %d",
					 dp->tag);
		}

	}

 Done:
	as_bin_particle_destroy(&var.bin, true);
	as_bin_set_empty(&var.bin);
	var.bin.particle = NULL;

	as_arraylist_iterator_destroy(&it);

	as_val_destroy(lval);

	// Remove our var from the list.
	argsp->vl = var.next;

	if (lwbin.must_free) {
		cf_crash(AS_PREDEXP, "eval_list_iter need bin cleanup, didn't before");
	}

	return retval;
}

static predexp_retval_t
eval_map_iter(predexp_eval_t* bp, predexp_args_t* argsp, wrapped_as_bin_t* wbinp)
{
	predexp_eval_iter_t* dp = (predexp_eval_iter_t *) bp;

	predexp_retval_t retval = PREDEXP_UNKNOWN; // init makes compiler happy
	switch (dp->tag) {
	case AS_PREDEXP_MAPKEY_ITERATE_OR:
	case AS_PREDEXP_MAPVAL_ITERATE_OR:
		// Start pessimistically.
		retval = PREDEXP_FALSE;
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_AND:
	case AS_PREDEXP_MAPVAL_ITERATE_AND:
		// Start optimistically.
		retval = PREDEXP_TRUE;
		break;
	default:
		cf_crash(AS_PREDEXP, "eval_map_iter called with bogus tag: %d",
				 dp->tag);
	}

	wrapped_as_bin_t lwbin;
	lwbin.must_free = false;
	if ((*dp->rchild->eval_fn)(dp->rchild, argsp, &lwbin) ==
		PREDEXP_NOVALUE) {
		return argsp->rd ? PREDEXP_FALSE : PREDEXP_UNKNOWN;
	}

	as_predexp_var_t var;
	memcpy(var.vname, dp->vname, sizeof(var.vname));

	// Make sure our var starts out empty.
	as_bin_set_empty(&var.bin);
	var.bin.particle = NULL;

	// Prepend our var to the list.
	var.next = argsp->vl;
	argsp->vl = &var;

	// Traverse the collection.
	as_val* mval = as_bin_particle_to_asval(&lwbin.bin);
	as_hashmap* map = (as_hashmap*) as_map_fromval(mval);
	as_hashmap_iterator it;
	as_hashmap_iterator_init(&it, map);
	while (as_hashmap_iterator_has_next(&it)) {
		// Set our var to the element's value.
		as_pair* pair = (as_pair*) as_hashmap_iterator_next(&it);
		as_val* val = NULL;  // init makes compiler happy
		switch (dp->tag) {
		case AS_PREDEXP_MAPKEY_ITERATE_OR:
		case AS_PREDEXP_MAPKEY_ITERATE_AND:
			val = as_pair_1(pair);
			break;
		case AS_PREDEXP_MAPVAL_ITERATE_OR:
		case AS_PREDEXP_MAPVAL_ITERATE_AND:
			val = as_pair_2(pair);
			break;
		default:
			cf_crash(AS_PREDEXP, "eval_map_iter called with bogus tag (2): %d",
					 dp->tag);
		}

		int old_arena = cf_alloc_clear_ns_arena();
		int rv = as_bin_particle_replace_from_asval(&var.bin, val);
		cf_alloc_restore_ns_arena(old_arena);
		if (rv != 0) {
			cf_warning(AS_PREDEXP, "eval_map_iter: particle from asval failed");
			continue;
		}

		switch (dp->tag) {
		case AS_PREDEXP_MAPKEY_ITERATE_OR:
		case AS_PREDEXP_MAPVAL_ITERATE_OR:
			switch ((*dp->lchild->eval_fn)(dp->lchild, argsp, NULL)) {
			case PREDEXP_TRUE:
				// Shortcut, skip remaining children.
				retval = PREDEXP_TRUE;
				goto Done;
			case PREDEXP_UNKNOWN:
				// Upgrade our return value, continue scanning children.
				retval = PREDEXP_UNKNOWN;
				break;
			case PREDEXP_FALSE:
				// Continue scanning children.
				break;
			case PREDEXP_VALUE:
			case PREDEXP_NOVALUE:
				// Child can't be value node; shouldn't ever happen.
				cf_crash(AS_PREDEXP, "eval_map_iter child was value node");
			}
			break;
		case AS_PREDEXP_MAPKEY_ITERATE_AND:
		case AS_PREDEXP_MAPVAL_ITERATE_AND:
			switch ((*dp->lchild->eval_fn)(dp->lchild, argsp, NULL)) {
			case PREDEXP_FALSE:
				// Shortcut, skip remaining children.
				retval = PREDEXP_FALSE;
				goto Done;
			case PREDEXP_UNKNOWN:
				// Downgrade our return value, continue scanning children.
				retval = PREDEXP_UNKNOWN;
				break;
			case PREDEXP_TRUE:
				// Continue scanning children.
				break;
			case PREDEXP_VALUE:
			case PREDEXP_NOVALUE:
				// Child can't be value node; shouldn't ever happen.
				cf_crash(AS_PREDEXP, "eval_map_iter child was value node");
			}
			break;
		default:
			cf_crash(AS_PREDEXP, "eval_map_iter called with bogus tag: %d",
					 dp->tag);
		}

	}

 Done:
	as_bin_particle_destroy(&var.bin, true);
	as_bin_set_empty(&var.bin);
	var.bin.particle = NULL;

	as_hashmap_iterator_destroy(&it);

	as_val_destroy(mval);

	// Remove our var from the list.
	argsp->vl = var.next;

	if (lwbin.must_free) {
		cf_crash(AS_PREDEXP, "eval_map_iter need bin cleanup, didn't before");
	}
	return retval;
}

static bool
build_iter(predexp_eval_t** stackpp, uint32_t len, uint8_t* pp, uint16_t tag)
{
	predexp_eval_iter_t* dp = (predexp_eval_iter_t *)
			cf_malloc(sizeof(predexp_eval_iter_t));

	switch (tag) {
	case AS_PREDEXP_LIST_ITERATE_OR:
	case AS_PREDEXP_LIST_ITERATE_AND:
		predexp_eval_base_init((predexp_eval_t *) dp,
							   destroy_iter,
							   eval_list_iter,
							   0,
							   AS_PARTICLE_TYPE_NULL);
		dp->type = AS_PARTICLE_TYPE_LIST;
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_OR:
	case AS_PREDEXP_MAPVAL_ITERATE_OR:
	case AS_PREDEXP_MAPKEY_ITERATE_AND:
	case AS_PREDEXP_MAPVAL_ITERATE_AND:
		predexp_eval_base_init((predexp_eval_t *) dp,
							   destroy_iter,
							   eval_map_iter,
							   0,
							   AS_PARTICLE_TYPE_NULL);
		dp->type = AS_PARTICLE_TYPE_MAP;
		break;
	default:
		cf_crash(AS_PREDEXP, "build_iter called with bogus tag: %d", tag);
	}

	dp->tag = tag;
	dp->lchild = NULL;
	dp->rchild = NULL;

	uint8_t* endp = pp + len;

	if (len >= sizeof(dp->vname)) {
		cf_warning(AS_PREDEXP, "build_iter: varname too long");
		goto Failed;
	}
	uint8_t vnlen = (uint8_t) len;
	memcpy(dp->vname, pp, vnlen);
	dp->vname[vnlen] = '\0';
	pp += vnlen;

	// ---- Pop the right child (collection) off the stack.

	if (! *stackpp) {
		cf_warning(AS_PREDEXP, "predexp_iterate: missing right child");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	dp->rchild = *stackpp;
	*stackpp = dp->rchild->next;
	dp->rchild->next = NULL;

	if ((dp->rchild->flags & PREDEXP_VALUE_NODE) == 0) {
		cf_warning(AS_PREDEXP,
				   "predexp iterate: right child is not value node");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	if (dp->rchild->type != dp->type) {
		cf_warning(AS_PREDEXP, "predexp iterate: right child is wrong type");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	// ---- Pop the left child (per-element expr) off the stack.

	if (! *stackpp) {
		cf_warning(AS_PREDEXP, "predexp_iterate: missing left child");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	dp->lchild = *stackpp;
	*stackpp = dp->lchild->next;
	dp->lchild->next = NULL;

	if ((dp->lchild->flags & PREDEXP_VALUE_NODE) == 1) {
		cf_warning(AS_PREDEXP, "predexp iterate: left child is value node");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	if (dp->lchild->type != AS_PARTICLE_TYPE_NULL) {
		cf_warning(AS_PREDEXP, "predexp iterate: left child is wrong type");
		(*dp->base.dtor_fn)((predexp_eval_t *) dp);
		return false;
	}

	if (pp != endp) {
		cf_warning(AS_PREDEXP, "build_iter: msg unaligned");
		goto Failed;
	}

	// Success, push ourself onto the stack.
	dp->base.next = *stackpp;				// We point next at the old top.
	*stackpp = (predexp_eval_t *) dp;	// We're the new top

	switch (tag) {
	case AS_PREDEXP_LIST_ITERATE_OR:
		cf_debug(AS_PREDEXP, "%p: predexp_list_iterate_or()", stackpp);
		break;
	case AS_PREDEXP_LIST_ITERATE_AND:
		cf_debug(AS_PREDEXP, "%p: predexp_list_iterate_and()", stackpp);
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_OR:
		cf_debug(AS_PREDEXP, "%p: predexp_mapkey_iterate_or()", stackpp);
		break;
	case AS_PREDEXP_MAPVAL_ITERATE_OR:
		cf_debug(AS_PREDEXP, "%p: predexp_mapval_iterate_or()", stackpp);
		break;
	case AS_PREDEXP_MAPKEY_ITERATE_AND:
		cf_debug(AS_PREDEXP, "%p: predexp_mapkey_iterate_and()", stackpp);
		break;
	case AS_PREDEXP_MAPVAL_ITERATE_AND:
		cf_debug(AS_PREDEXP, "%p: predexp_mapval_iterate_and()", stackpp);
		break;
	default:
		cf_crash(AS_PREDEXP, "build_iter called with bogus tag: %d", tag);
	}

	return true;

 Failed:
	(*dp->base.dtor_fn)((predexp_eval_t *) dp);
	return false;
}

// ----------------------------------------------------------------
// External Interface
// ----------------------------------------------------------------


static bool
build(predexp_eval_t** stackpp, uint16_t tag, uint32_t len, uint8_t* pp)
{
	switch (tag) {
	case AS_PREDEXP_AND:
		return build_and(stackpp, len, pp);
	case AS_PREDEXP_OR:
		return build_or(stackpp, len, pp);
	case AS_PREDEXP_NOT:
		return build_not(stackpp, len, pp);
	case AS_PREDEXP_INTEGER_EQUAL:
	case AS_PREDEXP_INTEGER_UNEQUAL:
	case AS_PREDEXP_INTEGER_GREATER:
	case AS_PREDEXP_INTEGER_GREATEREQ:
	case AS_PREDEXP_INTEGER_LESS:
	case AS_PREDEXP_INTEGER_LESSEQ:
	case AS_PREDEXP_STRING_EQUAL:
	case AS_PREDEXP_STRING_UNEQUAL:
	case AS_PREDEXP_STRING_REGEX:
	case AS_PREDEXP_GEOJSON_WITHIN:
	case AS_PREDEXP_GEOJSON_CONTAINS:
		return build_compare(stackpp, len, pp, tag);
	case AS_PREDEXP_INTEGER_VALUE:
	case AS_PREDEXP_STRING_VALUE:
	case AS_PREDEXP_GEOJSON_VALUE:
		return build_value(stackpp, len, pp, tag);
	case AS_PREDEXP_INTEGER_BIN:
	case AS_PREDEXP_STRING_BIN:
	case AS_PREDEXP_GEOJSON_BIN:
	case AS_PREDEXP_LIST_BIN:
	case AS_PREDEXP_MAP_BIN:
		return build_bin(stackpp, len, pp, tag);
	case AS_PREDEXP_INTEGER_VAR:
	case AS_PREDEXP_STRING_VAR:
	case AS_PREDEXP_GEOJSON_VAR:
		return build_var(stackpp, len, pp, tag);
	case AS_PREDEXP_REC_DEVICE_SIZE:
		return build_rec_device_size(stackpp, len, pp);
	case AS_PREDEXP_REC_LAST_UPDATE:
		return build_rec_last_update(stackpp, len, pp);
	case AS_PREDEXP_REC_VOID_TIME:
		return build_rec_void_time(stackpp, len, pp);
	case AS_PREDEXP_REC_DIGEST_MODULO:
		return build_rec_digest_modulo(stackpp, len, pp);
	case AS_PREDEXP_LIST_ITERATE_OR:
	case AS_PREDEXP_LIST_ITERATE_AND:
	case AS_PREDEXP_MAPKEY_ITERATE_OR:
	case AS_PREDEXP_MAPKEY_ITERATE_AND:
	case AS_PREDEXP_MAPVAL_ITERATE_OR:
	case AS_PREDEXP_MAPVAL_ITERATE_AND:
		return build_iter(stackpp, len, pp, tag);
	default:
		cf_warning(AS_PREDEXP, "unexpected predexp tag: %d", tag);
		return false;
	}
}

predexp_eval_t*
predexp_build(as_msg_field* pfp)
{
	predexp_eval_t* stackp = NULL;

	cf_debug(AS_PREDEXP, "%p: predexp_build starting", &stackp);

	uint8_t* pp = pfp->data;
	uint32_t pdsize = as_msg_field_get_value_sz(pfp);
	uint8_t* endp = pp + pdsize;

	// Minumum possible TLV token is 6 bytes.
	while (pp + 6 <= endp) {
		uint16_t tag = cf_swap_from_be16(* (uint16_t *) pp);
		pp += sizeof(uint16_t);

		uint32_t len = cf_swap_from_be32(* (uint32_t *) pp);
		pp += sizeof(uint32_t);

		if (pp + len > endp) {
			cf_warning(AS_PREDEXP, "malformed predexp field");
			goto FAILED;
		}

		if (!build(&stackp, tag, len, pp)) {
			// Warning should already have happened
			goto FAILED;
		}
		pp += len;
	}

	// The cursor needs to neatly point at the end pointer.
	if (pp != endp) {
		cf_warning(AS_PREDEXP, "malformed predexp field");
		goto FAILED;
	}

	// We'd better have exactly one node on the stack now.
	if (!stackp) {
		cf_warning(AS_PREDEXP, "no top level predexp");
		goto FAILED;
	}
	if (stackp->next) {
		cf_warning(AS_PREDEXP, "multiple top-level predexp");
		goto FAILED;
	}

	// The top node needs to be a matching node, not a value node.
	if (stackp->flags & PREDEXP_VALUE_NODE) {
		cf_warning(AS_PREDEXP, "top-level predexp is value node");
		goto FAILED;
	}

	cf_debug(AS_PREDEXP, "%p: predexp_build finished", &stackp);

	// Return the root of the predicate expression tree.
	return stackp;

 FAILED:
	cf_debug(AS_PREDEXP, "%p: predexp_build failed", &stackp);
	destroy_list(stackp);
	return NULL;
}

bool
predexp_matches_metadata(predexp_eval_t* bp, predexp_args_t* argsp)
{
	if (! bp) {
		return true;
	}

	return ((*bp->eval_fn)(bp, argsp, NULL) != PREDEXP_FALSE);
}

bool
predexp_matches_record(predexp_eval_t* bp, predexp_args_t* argsp)
{
	if (! bp) {
		return true;
	}

	switch ((*bp->eval_fn)(bp, argsp, NULL)) {
	case PREDEXP_TRUE:
		return true;
	case PREDEXP_FALSE:
		return false;
	default:
		cf_crash(AS_PREDEXP, "predexp eval returned other then true/false "
				 "with record data present");
		return false;	// makes compiler happy
	}
}

void
predexp_destroy(predexp_eval_t* bp)
{
	(*bp->dtor_fn)(bp);
}
