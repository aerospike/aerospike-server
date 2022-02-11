/*
 * write.h
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "base/exp.h"
#include "base/transaction.h"


//==========================================================
// Forward declarations.
//

struct as_exp_s;
struct as_storage_rd_s;
struct as_transaction_s;
struct cl_msg_s;


//==========================================================
// Typedefs & constants.
//

typedef struct iops_expop_s {
	struct as_exp_s* exp;
	uint64_t flags;
} iops_expop;

typedef bool (*iops_check_cb)(void* udata, struct as_storage_rd_s* rd);
typedef void (*iops_done_cb)(void* udata, int result);

typedef struct iops_origin_s {
	struct cl_msg_s* msgp;
	struct as_exp_s* filter_exp;
	iops_expop* expops;
	iops_check_cb check_cb;
	iops_done_cb done_cb;
	void* udata;
} iops_origin;


//==========================================================
// Public API.
//

transaction_status as_write_start(struct as_transaction_s* tr);

static inline void
iops_expops_destroy(iops_expop* expops, uint16_t count)
{
	if (expops != NULL) {
		for (uint16_t i = 0; i < count; i++) {
			as_exp_destroy(expops[i].exp);
		}

		cf_free(expops);
	}
}

static inline void
iops_origin_destroy(iops_origin* origin)
{
	as_exp_destroy(origin->filter_exp);

	if (origin->msgp != NULL) {
		iops_expops_destroy(origin->expops, origin->msgp->msg.n_ops);
		cf_free(origin->msgp);
	}
}
