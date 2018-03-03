/*
 * namespace_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "arenax.h"
#include "fault.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"

static bool
check_capacity(uint32_t capacity)
{
	uint8_t* test_index_stages[g_config.n_namespaces];
	uint8_t* test_data_blocks[g_config.n_namespaces];
	uint32_t i;

	for (i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		uint64_t stage_size = (uint64_t)as_index_size_get(ns) * capacity;

		if ((test_index_stages[i] = cf_try_malloc(stage_size)) == NULL) {
			break;
		}

		// Memory for overhead and data, proportional to (= to) stage size.
		if ((test_data_blocks[i] = cf_try_malloc(stage_size)) == NULL) {
			cf_free(test_index_stages[i]);
			break;
		}
	}

	for (uint32_t j = 0; j < i; j++) {
		cf_free(test_index_stages[j]);
		cf_free(test_data_blocks[j]);
	}

	return i == g_config.n_namespaces;
}

#define MIN_STAGE_CAPACITY (MAX_STAGE_CAPACITY / 8)
#define NS_MIN_MB (((sizeof(as_index) * MIN_STAGE_CAPACITY) * 2) / (1024 * 1024))

uint32_t
as_mem_check()
{
	uint32_t capacity;

	for (capacity = MAX_STAGE_CAPACITY; capacity >= MIN_STAGE_CAPACITY; capacity /= 2) {
		if (check_capacity(capacity)) {
			break;
		}
	}

	if (capacity < MIN_STAGE_CAPACITY) {
		cf_crash_nostack(AS_NAMESPACE, "server requires at least %luMb of memory per namespace", NS_MIN_MB);
	}

	if (capacity < MAX_STAGE_CAPACITY) {
		cf_info(AS_NAMESPACE, "detected small memory profile - will size arena stages 1/%u max", MAX_STAGE_CAPACITY / capacity);
	}

	return capacity;
}

static void
setup_namespace(as_namespace* ns, uint32_t stage_capacity)
{
	ns->cold_start = true;

	cf_info(AS_NAMESPACE, "{%s} beginning cold start", ns->name);

	//--------------------------------------------
	// Set up the set name vmap.
	//

	ns->p_sets_vmap = (cf_vmapx*)cf_malloc(cf_vmapx_sizeof(sizeof(as_set), AS_SET_MAX_COUNT));

	cf_vmapx_init(ns->p_sets_vmap, sizeof(as_set), AS_SET_MAX_COUNT, 1024, AS_SET_NAME_MAX_SIZE);

	// Transfer configuration file information about sets.
	if (! as_namespace_configure_sets(ns)) {
		cf_crash(AS_NAMESPACE, "{%s} can't configure sets", ns->name);
	}

	//--------------------------------------------
	// Set up the bin name vmap.
	//

	if (! ns->single_bin) {
		ns->p_bin_name_vmap = (cf_vmapx*)cf_malloc(cf_vmapx_sizeof(VMAP_BIN_NAME_MAX_SZ, MAX_BIN_NAMES));

		cf_vmapx_init(ns->p_bin_name_vmap, VMAP_BIN_NAME_MAX_SZ, MAX_BIN_NAMES, 4096, VMAP_BIN_NAME_MAX_SZ);
	}

	//--------------------------------------------
	// Set up the index arena.
	//

	ns->arena = (cf_arenax*)cf_malloc(cf_arenax_sizeof());

	cf_arenax_init(ns->arena, 0, as_index_size_get(ns), stage_capacity, 0, CF_ARENAX_BIGLOCK);
}

void
as_namespaces_setup(bool cold_start_cmd, uint32_t instance, uint32_t stage_capacity)
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		setup_namespace(g_config.namespaces[i], stage_capacity);
	}
}

void
as_namespace_xmem_trusted(as_namespace *ns)
{
	// For enterprise version only.
}
