/*
 * msgpack_sz.h
 *
 * Copyright (C) 2019 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"


//==========================================================
// Typedefs & constants.
//

typedef struct {
	const uint8_t *buf;
	uint32_t buf_sz;
	uint32_t offset;
	bool has_nonstorage;
} msgpack_in;


//==========================================================
// Public API.
//

uint32_t msgpack_sz(msgpack_in *mp);
uint32_t msgpack_sz_rep(msgpack_in *mp, uint32_t rep_count);
msgpack_compare_t msgpack_cmp(msgpack_in *mp0, msgpack_in *mp1);
msgpack_compare_t msgpack_cmp_peek(const msgpack_in *mp0, const msgpack_in *mp1);
