/*
 * meta_batch.c
 *
 * Copyright (C) 2017-2018 Aerospike, Inc.
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

#include "fabric/meta_batch.h"

#include <stddef.h>


//==========================================================
// Public API.
//

struct meta_in_q_s *
meta_in_q_create()
{
	return NULL;
}


void
meta_in_q_destroy(struct meta_in_q_s *iq)
{
}


void
meta_in_q_rejected(struct meta_in_q_s *iq)
{
}


struct meta_out_q_s *
meta_out_q_create()
{
	return NULL;
}


void
meta_out_q_destroy(struct meta_out_q_s *oq)
{
}
