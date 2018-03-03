/*
 * udf.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include <stdint.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_list.h"

#include "base/predexp.h"
#include "base/transaction.h"


//==========================================================
// Forward declarations.
//

struct as_transaction_s;
struct predexp_eval_base_s;


//==========================================================
// Typedefs & constants.
//

typedef enum {
	UDF_OPTYPE_NONE,
	UDF_OPTYPE_WAITING,
	UDF_OPTYPE_READ,
	UDF_OPTYPE_WRITE,
	UDF_OPTYPE_DELETE
} udf_optype;

#define UDF_MAX_STRING_SZ 128

typedef struct udf_def_s {
	char			filename[UDF_MAX_STRING_SZ];
	char			function[UDF_MAX_STRING_SZ];
	as_list*		arglist;
	uint8_t			type;
} udf_def;

typedef int (*iudf_cb)(void* udata, int retcode);

typedef struct iudf_origin_s {
	udf_def			def;
	struct predexp_eval_base_s*	predexp;
	iudf_cb			cb;
	void*			udata;
} iudf_origin;


//==========================================================
// Public API.
//

static inline void
iudf_origin_destroy(iudf_origin* origin)
{
	if (origin->def.arglist) {
		as_list_destroy(origin->def.arglist);
	}

	if (origin->predexp) {
		predexp_destroy(origin->predexp);
	}
}

void as_udf_init();
udf_def* udf_def_init_from_msg(udf_def* def, const struct as_transaction_s* tr);

transaction_status as_udf_start(struct as_transaction_s* tr);

extern as_aerospike g_as_aerospike;
