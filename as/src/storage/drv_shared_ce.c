/*
 * drv_shared_ce.c
 *
 * Copyright (C) 2023-2026 Aerospike, Inc.
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
// Community Edition cold-start helpers. One body per concern, called by every
// storage engine - the enterprise bodies are in drv_shared_ee.c.
//

//==========================================================
// Includes.
//

#include <stdint.h>

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/drv_common.h"
#include "storage/flat.h"

//==========================================================
// CP enterprise separation API.
//

conflict_resolution_pol
drv_cold_start_policy(const as_namespace* ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}

void
drv_cold_start_adjust_cenotaph(const as_namespace* ns,
		const as_flat_record* flat, uint32_t block_void_time, as_index* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
drv_cold_start_init_repl_state(const as_namespace* ns, as_index* r)
{
	// Nothing to do - relevant for enterprise version only.
}

void
drv_cold_start_set_unrepl_stat(as_namespace* ns)
{
	// Nothing to do - relevant for enterprise version only.
}

//==========================================================
// XDR enterprise separation API.
//

void
drv_cold_start_init_xdr_state(const as_flat_record* flat, as_index* r)
{
	// Nothing to do - relevant for enterprise version only.
}
