/*
 * cfg_ce.c
 *
 * Copyright (C) 2016-2019 Aerospike, Inc.
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

#include "base/cfg.h"

#include <stdbool.h>
#include <stdint.h>

#include "fault.h"

#include "base/datamodel.h"


//==========================================================
// Forward declarations.
//

void post_process_namespace(as_namespace* ns);


//==========================================================
// Public API.
//

bool
as_config_error_enterprise_only()
{
	return true;
}

bool
as_config_error_enterprise_feature_only(const char* name)
{
	cf_crash(AS_CFG, "community edition checking enterprise feature");
	return true;
}

// TODO - until we have an info split.
bool
as_info_error_enterprise_only()
{
	return true;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
cfg_enterprise_only(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: '%s' is enterprise-only",
			p_line->num, p_line->name_tok);
}


void
cfg_post_process()
{
	// So far, no other context handled.

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		post_process_namespace(g_config.namespaces[ns_ix]);
	}
}


//==========================================================
// Local helpers.
//

void
post_process_namespace(as_namespace* ns)
{
	if (ns->conflict_resolution_policy ==
			AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF) {
		ns->conflict_resolution_policy =
				AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
	}
}
