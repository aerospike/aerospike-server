/*
 * ai_btree.h
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

#pragma once

#include "base/secondary_index.h"

#include "ai_obj.h"
#include "btreepriv.h"

#include <citrusleaf/cf_ll.h>

void ai_btree_create(as_sindex_metadata *imd);

void ai_btree_destroy(as_sindex_metadata *imd);

int ai_btree_put(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *key, cf_digest *value);

int ai_btree_delete(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, void *key, cf_digest *val);

int ai_btree_query(as_sindex_metadata *imd, as_sindex_range *range, as_sindex_qctx *qctx);

uint64_t ai_btree_get_isize(as_sindex_metadata *imd);

uint64_t ai_btree_get_nsize(as_sindex_metadata *imd);

uint64_t ai_btree_get_pimd_nsize(as_sindex_pmetadata *pimd);

uint64_t ai_btree_get_pimd_isize(as_sindex_pmetadata *pimd);

int ai_btree_list(char *ns, char *set, as_sindex_metadata **imds, int *num_indexes);

uint64_t ai_btree_get_numkeys(as_sindex_metadata *imd);

void ai_btree_dump(as_sindex_metadata *imd, char *fname, bool verbose);

int ai_btree_build_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, struct ai_obj *icol, ulong *nofst, ulong lim, uint64_t * tot_processed, uint64_t * tot_found, cf_ll *apk2d);

bool ai_btree_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, cf_ll *apk2d, ulong n2del, ulong *deleted);

int ai_btree_key_hash_from_sbin(as_sindex_metadata *imd, as_sindex_bin_data *sbin);

int ai_btree_key_hash(as_sindex_metadata *imd, void *skey);

void ai_btree_delete_ibtr(bt *ibtr);

void ai_btree_reinit_pimd(as_sindex_pmetadata *pimd, col_type_t sktype);

void ai_btree_reset_pimd(as_sindex_pmetadata * pimd);
