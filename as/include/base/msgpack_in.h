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


//==========================================================
// Typedefs & constants.
//

typedef struct {
	const uint8_t *buf;
	uint32_t buf_sz;
	uint32_t offset;
	bool has_nonstorage;
} msgpack_in;

typedef struct msgpack_ext_s {
	const uint8_t *data;	// pointer to ext contents
	uint32_t size;			// size of ext contents
	uint32_t type_offset;	// offset where the type field is located
	uint8_t type;			// type of ext contents
} msgpack_ext;

typedef enum msgpack_cmp_e {
	MSGPACK_CMP_ERROR	= -2,
	MSGPACK_CMP_END		= -1,
	MSGPACK_CMP_LESS	= 0,
	MSGPACK_CMP_EQUAL	= 1,
	MSGPACK_CMP_GREATER = 2,
} msgpack_cmp_t;


//==========================================================
// Public API.
//

uint32_t msgpack_sz_rep(msgpack_in *mp, uint32_t rep_count);
static inline uint32_t
msgpack_sz(msgpack_in *mp)
{
	return msgpack_sz_rep(mp, 1);
}

msgpack_cmp_t msgpack_cmp(msgpack_in *mp0, msgpack_in *mp1);
msgpack_cmp_t msgpack_cmp_peek(const msgpack_in *mp0, const msgpack_in *mp1);
