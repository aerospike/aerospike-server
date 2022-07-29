/*
 * udf_record.h
 *
 * Copyright (C) 2013-2020 Aerospike, Inc.
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

#include "aerospike/as_rec.h"
#include "aerospike/as_hashmap.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_atomic.h"

#include "dynbuf.h"

#include "base/datamodel.h"


//==========================================================
// Forward declarations.
//

struct as_index_ref_s;
struct as_storage_rd_s;
struct as_transaction_s;


//==========================================================
// Typedefs & constants.
//

// UDFs cannot be applied to records with more than this many bins.
#define UDF_BIN_LIMIT 512

// Maximum number of updates that can be cached.
#define UDF_UPDATE_LIMIT (UDF_BIN_LIMIT * 2)

typedef struct udf_record_bin_s {
	char name[AS_BIN_NAME_MAX_SZ];
	as_val* value;
	bool dirty;
} udf_record_bin;

typedef struct udf_record_s {
	struct as_transaction_s* tr;
	struct as_index_ref_s* r_ref;
	struct as_storage_rd_s* rd;

	bool is_open;
	bool is_loaded;
	bool too_many_bins;
	bool has_updates;

	uint8_t result_code; // only set when we fail execute_updates()
	uint32_t old_memory_bytes; // DIM only

	cf_ll_buf particle_llb; // non-DIM only

	as_bin stack_bins[RECORD_MAX_BINS]; // new bins if writing

	uint32_t n_old_bins;
	uint32_t n_cleanup_bins; // DIM only

	as_bin old_bins[UDF_BIN_LIMIT];
	as_bin cleanup_bins[UDF_BIN_LIMIT]; // DIM only

	uint32_t n_inserts; // updates that are not deletes
	uint32_t n_updates;
	udf_record_bin updates[UDF_UPDATE_LIMIT]; // cached bin (as_val) values
} udf_record;


//==========================================================
// Public API.
//

void udf_record_init(udf_record* urecord);

void udf_record_cache_free(udf_record* urecord);
void udf_record_cache_set(udf_record* urecord, const char* name, as_val* value, bool dirty);
void udf_record_cache_reclaim(udf_record* urecord, uint32_t i);

int udf_record_open(udf_record* urecord);
void udf_record_close(udf_record* urecord);
int udf_record_load(udf_record* urecord);


//==========================================================
// Public API - rec hooks.
//

extern const as_rec_hooks udf_record_hooks;
extern const as_rec_hooks as_aggr_record_hooks;
