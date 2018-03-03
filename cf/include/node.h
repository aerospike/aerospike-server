/*
 * node.h
 *
 * Copyright (C) 2017 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "compare.h"

typedef uint64_t cf_node;

uint32_t cf_nodeid_shash_fn(const void *key);
uint32_t cf_nodeid_rchash_fn(const void *key, uint32_t key_size);
char *cf_node_name();

static inline int
index_of_node(const cf_node* nodes, uint32_t n_nodes, cf_node node)
{
	for (uint32_t n = 0; n < n_nodes; n++) {
		if (node == nodes[n]) {
			return (int)n;
		}
	}

	return -1;
}

static inline bool
contains_node(const cf_node* nodes, uint32_t n_nodes, cf_node node)
{
	return index_of_node(nodes, n_nodes, node) != -1;
}

static inline uint32_t
remove_node(cf_node* nodes, uint32_t n_nodes, cf_node node)
{
	int n = index_of_node(nodes, n_nodes, node);

	if (n != -1) {
		nodes[n] = nodes[--n_nodes];
	}

	return n_nodes;
}

static inline int
cf_node_compare_desc(const void* pa, const void* pb)
{
	// Relies on cf_node being uint64_t.
	return cf_compare_uint64_desc(pa, pb);
}
