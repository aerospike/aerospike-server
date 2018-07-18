/*
 * namespace_ce.c
 *
 * Copyright (C) 2014-2018 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "arenax.h"
#include "fault.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"


//==========================================================
// Forward declarations.
//

static void setup_namespace(as_namespace* ns);


//==========================================================
// Public API.
//

void
as_namespace_xmem_shutdown(as_namespace *ns, uint32_t instance)
{
	// For enterprise version only.
}


//==========================================================
// Private API - for enterprise separation only.
//

void
as_namespaces_setup(bool cold_start_cmd, uint32_t instance)
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		setup_namespace(g_config.namespaces[i]);
	}
}

void
as_namespace_finish_setup(as_namespace *ns, uint32_t instance)
{
	// For enterprise version only.
}


//==========================================================
// Local helpers.
//

static void
setup_namespace(as_namespace* ns)
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
		ns->p_bin_name_vmap = (cf_vmapx*)cf_malloc(cf_vmapx_sizeof(AS_BIN_NAME_MAX_SZ, MAX_BIN_NAMES));

		cf_vmapx_init(ns->p_bin_name_vmap, AS_BIN_NAME_MAX_SZ, MAX_BIN_NAMES, 4096, AS_BIN_NAME_MAX_SZ);
	}

	//--------------------------------------------
	// Set up the index arena.
	//

	ns->arena = (cf_arenax*)cf_malloc(cf_arenax_sizeof());
	ns->tree_shared.arena = ns->arena;

	uint32_t element_size = (uint32_t)sizeof(as_index);
	uint32_t stage_capacity = (uint32_t)(ns->index_stage_size / element_size);

	cf_arenax_init(ns->arena, ns->xmem_type, ns->xmem_type_cfg, 0, element_size, 1, stage_capacity, 0, CF_ARENAX_BIGLOCK);
}
