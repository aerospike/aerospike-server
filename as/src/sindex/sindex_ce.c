/*
 * sindex_ce.c
 *
 * Copyright (C) 2022 Aerospike, Inc.
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

#include "sindex/sindex.h"

#include <stddef.h>
#include <stdint.h>

#include "log.h"

#include "base/datamodel.h"


//==========================================================
// Public API.
//

void
as_sindex_resume(void)
{
}

void
as_sindex_shutdown(as_namespace* ns)
{
}


//==========================================================
// Private API - for enterprise separation only.
//

void
add_to_sindexes(as_sindex* si)
{
	as_namespace* ns = si->ns;

	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		if (ns->sindexes[i] == NULL) {
			ns->sindexes[i] = si;
			ns->si_ids[i] = i;
			si->id = i;

			return;
		}
	}

	cf_crash(AS_SINDEX, "sindex array already full");
}

void
drop_from_sindexes(as_sindex* si)
{
	si->ns->sindexes[si->id] = NULL;
}

void
as_sindex_resume_check(void)
{
}
