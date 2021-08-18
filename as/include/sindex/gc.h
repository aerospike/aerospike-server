/*
 * gc.h
 *
 * Copyright (C) 2021 Aerospike, Inc.
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

#include "citrusleaf/cf_queue.h"

#include "arenax.h"


//==========================================================
// Forward declarations.
//

struct as_index_ref_s;
struct as_index_tree_s;
struct as_namespace_s;


//==========================================================
// Typedefs & constants.
//

typedef struct rlist_ele_s {
	cf_arenax_handle r_h: 40;
} __attribute__ ((__packed__)) rlist_ele;


//==========================================================
// Public API.
//

void as_sindex_gc_ns_init(struct as_namespace_s* ns);
void* as_sindex_run_gc(void* udata);

void as_sindex_gc_record(struct as_namespace_s* ns, struct as_index_ref_s* r_ref);
void as_sindex_gc_tree(struct as_namespace_s* ns, struct as_index_tree_s* tree);


//==========================================================
// Private API - for enterprise separation only.
//

void create_rlist(struct as_namespace_s* ns);
void push_to_rlist(struct as_namespace_s* ns, struct as_index_ref_s* r_ref);
void purge_rlist(struct as_namespace_s* ns, cf_queue* rlist);
