/*
 * mrt_utils.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
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

#include "arenax.h"

#include "base/transaction.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_index_ref_s;
struct as_index_tree_s;
struct as_namespace_s;
struct as_record_version_s;
struct as_remote_record_s;
struct as_storage_rd_s;
struct as_transaction_s;


//==========================================================
// Public API.
//

bool is_mrt_provisional(const struct as_index_s* r);
bool is_mrt_original(const struct as_index_s* r);
bool is_mrt_monitor_write(const struct as_namespace_s* ns, const struct as_index_s* r);
bool is_mrt_setless_tombstone(const struct as_namespace_s* ns, const struct as_index_s* r);
void mrt_free_orig(cf_arenax* arena, struct as_index_s* r, cf_arenax_puddle* puddle);

int mrt_allow_read(struct as_transaction_s* tr, const struct as_index_s* r);
int mrt_allow_write(struct as_transaction_s* tr, const struct as_index_s* r);
int mrt_allow_udf_write(struct as_transaction_s* tr, const struct as_index_s* r);
struct as_index_s* read_r(struct as_namespace_s* ns, struct as_index_s* r, bool is_mrt);

bool mrt_skip_cleanup(struct as_namespace_s* ns, struct as_index_tree_s* tree, struct as_index_ref_s* r_ref);

int set_mrt_id_from_msg(struct as_storage_rd_s* rd, const struct as_transaction_s* tr);
void set_mrt_id(struct as_storage_rd_s* rd, uint64_t mrt_id);
bool is_first_mrt(const struct as_storage_rd_s* rd);
void finish_first_mrt(struct as_storage_rd_s* rd, const struct as_index_s* old_r, cf_arenax_puddle* puddle);

struct as_record_version_s* mrt_write_fill_version(struct as_record_version_s* v, const struct as_transaction_s* tr);
struct as_record_version_s* mrt_read_fill_version(struct as_record_version_s* v, const struct as_transaction_s* tr);

bool mrt_load_orig_pickle(struct as_namespace_s* ns, struct as_index_s* r, uint8_t** pickle, uint32_t* pickle_sz);

bool is_rr_mrt(const struct as_remote_record_s* rr);
bool is_rr_mrt_monitor_write(const struct as_remote_record_s* rr, uint32_t set_id);
int mrt_apply_original(struct as_remote_record_s* rr, struct as_index_ref_s* r_ref);
int mrt_apply_roll(struct as_remote_record_s* rr, struct as_index_ref_s* r_ref, struct as_storage_rd_s* rd);
void finish_replace_mrt(struct as_storage_rd_s* rd, const struct as_index_s* old_r, cf_arenax_puddle* puddle);
