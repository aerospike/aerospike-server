/*
 * ai_btree.h
 *
 * Copyright (C) 2013-2021 Aerospike, Inc.
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

#include <stdint.h>

// TODO: replace includes with forward declarations
#include "sindex/secondary_index.h"
#include "ai_obj.h"
#include "btreepriv.h"

#include "arenax.h"

#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_ll.h>

void ai_btree_create(as_sindex_metadata *imd);

as_sindex_status ai_btree_put(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *key, cf_arenax_handle r_h);

as_sindex_status ai_btree_delete(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *key, cf_arenax_handle r_h);

int ai_btree_query(as_sindex_metadata *imd, const as_query_range *range, as_sindex_qctx *qctx);

uint64_t ai_btree_get_isize(as_sindex_metadata *imd);

uint64_t ai_btree_get_nsize(as_sindex_metadata *imd);

uint64_t ai_btree_get_numkeys(as_sindex_metadata *imd);

as_sindex_status ai_btree_build_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, struct ai_obj *ibtr_last_key, cf_arenax_handle *nbtr_last_key, ulong lim, cf_ll *apk2d);

bool ai_btree_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, cf_ll *apk2d, ulong n2del, ulong *deleted);

void ai_btree_gc_list_destroy_fn(cf_ll_element *ele);

void ai_btree_delete_ibtr(bt *ibtr);

void ai_btree_reset_pimd(as_sindex_pmetadata * pimd);
