/*
 * aggr.h
 *
 * Copyright (C) 2014-2022 Aerospike, Inc.
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

#include "aerospike/as_stream.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"

#include "log.h"

#include "transaction/udf.h"


//==========================================================
// Forward declarations.
//

struct as_index_tree_s;
struct as_namespace_s;
struct as_partition_reservation_s;
struct as_result_s;
struct as_val_s;
struct udf_record_s;


//==========================================================
// Typedefs & constants.
//

#define AS_AGGR_KEYS_PER_ARR 204

typedef struct {
	as_stream_status (*ostream_write) (void*, struct as_val_s*);
	bool (*pre_check) (void*, struct udf_record_s*);
} as_aggr_hooks;

typedef struct {
	udf_def def;
	const as_aggr_hooks* aggr_hooks;
} as_aggr_call;

typedef struct as_aggr_keys_arr_s {
	uint32_t num;
	union {
		uint64_t handles[AS_AGGR_KEYS_PER_ARR];
		cf_digest digests[AS_AGGR_KEYS_PER_ARR];
	} u;
} as_aggr_keys_arr;

COMPILER_ASSERT(sizeof(as_aggr_keys_arr) <= 4096);

typedef struct as_aggr_keys_ll_element_s {
	cf_ll_element ele;
	as_aggr_keys_arr* keys_arr;
} as_aggr_keys_ll_element;

typedef struct as_aggr_release_udata_s {
	struct as_namespace_s* ns;
	struct as_index_tree_s* tree;
} as_aggr_release_udata;


//==========================================================
// Public API.
//

int as_aggr_process(struct as_namespace_s* ns, as_partition_reservation* rsv, as_aggr_call* ag_call, cf_ll* ap_recl, void* udata, struct as_result_s* ap_res);

int as_aggr_keys_release_cb(cf_ll_element* ele, void* udata);
void as_aggr_keys_destroy_cb(cf_ll_element* ele);
