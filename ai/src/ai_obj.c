/*
 * ai_obj.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
/*
 *  Aerospike Index Object Implementation.
 */

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/param.h>  // For MIN().

#include "ai_obj.h"
#include "stream.h"

#include <citrusleaf/alloc.h>

void init_ai_obj(ai_obj *a)
{
	bzero(a, sizeof(ai_obj));
	a->type = COL_TYPE_INVALID;
}

void init_ai_objLong(ai_obj *a, ulong l)
{
	init_ai_obj(a);
	a->l = l;
	a->type = COL_TYPE_LONG;
}

void init_ai_objU160(ai_obj *a, uint160 y) {
	a->type = COL_TYPE_DIGEST;
	a->y = y;
}

void ai_objClone(ai_obj *dest, ai_obj *src)
{
	memcpy(dest, src, sizeof(ai_obj));
}

static int ai_objCmp(ai_obj *a, ai_obj *b)
{
	if (C_IS_L(a->type) || C_IS_G(a->type)) {
		return (a->l == b->l) ? 0 : ((a->l > b->l) ? 1 : -1);
	} else if (C_IS_DG(a->type)) {
		return u160Cmp(&a->y, &b->y);
	} else {
		assert(!"ai_objCmp ERROR");
	}
}

bool ai_objEQ(ai_obj *a, ai_obj *b)
{
	return !ai_objCmp(a, b);
}

static void dump_ai_obj_internal(FILE *fp, ai_obj *a, bool as_digest)
{
	if (C_IS_L(a->type) || C_IS_G(a->type)) {
		fprintf(fp, "\tLONG ai_obj: val: %lu\n", a->l);
	} else if (C_IS_DG(a->type)) {
		fprintf(fp, "\tU160 ai_obj:");
		if (as_digest) {
			const int len = 20;
			char digest_str[2 + (len * 2) + 1];
			digest_str[0] = '\0';
			generate_packed_hex_string((uint8_t *) &(a->y), len, digest_str);
			fprintf(fp, "%s\n", digest_str);
		} else {
			DEBUG_U160(fp, a->y);
			fprintf(fp, "\n");
		}
	} else {
		fprintf(fp, "\tUNINITIALISED ai_obj\n");
	}
}


void dump_ai_obj_as_digest(FILE *fp, ai_obj *a)
{
	dump_ai_obj_internal(fp, a, true);
}
