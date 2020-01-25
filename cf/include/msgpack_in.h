/*
 * msgpack_in.h
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
} msgpack_cmp_type;

typedef enum {
	MSGPACK_TYPE_ERROR,
	MSGPACK_TYPE_NIL,
	MSGPACK_TYPE_FALSE,
	MSGPACK_TYPE_TRUE,
	MSGPACK_TYPE_NEGINT,
	MSGPACK_TYPE_INT,
	MSGPACK_TYPE_STRING,
	MSGPACK_TYPE_LIST,
	MSGPACK_TYPE_MAP,
	MSGPACK_TYPE_BYTES,
	MSGPACK_TYPE_DOUBLE,
	MSGPACK_TYPE_GEOJSON,

	MSGPACK_TYPE_EXT,
	// Non-storage types, need to be after storage types.
	MSGPACK_TYPE_CMP_WILDCARD, // not a storage type
	MSGPACK_TYPE_CMP_INF,      // not a storage type, must be last (biggest value)

	MSGPACK_N_TYPES
} msgpack_type;


//==========================================================
// Public API.
//

uint32_t msgpack_sz_rep(msgpack_in *mp, uint32_t rep_count);
static inline uint32_t
msgpack_sz(msgpack_in *mp)
{
	return msgpack_sz_rep(mp, 1);
}

msgpack_cmp_type msgpack_cmp(msgpack_in *mp0, msgpack_in *mp1);
msgpack_cmp_type msgpack_cmp_peek(const msgpack_in *mp0, const msgpack_in *mp1);

msgpack_type msgpack_peek_type(const msgpack_in *mp);
static inline msgpack_type
msgpack_buf_peek_type(const uint8_t *buf, uint32_t buf_sz)
{
	const msgpack_in mp = {
			.buf = buf,
			.buf_sz = buf_sz,
	};

	return msgpack_peek_type(&mp);
}
bool msgpack_peek_is_ext(const msgpack_in *mp);

bool msgpack_get_bool(msgpack_in *mp, bool *value);

bool msgpack_get_uint64(msgpack_in *mp, uint64_t *i);
static inline bool
msgpack_get_int64(msgpack_in *mp, int64_t *i)
{
	return msgpack_get_uint64(mp, (uint64_t *)i);
}
static inline bool
msgpack_type_is_int(msgpack_type type)
{
	return type == MSGPACK_TYPE_NEGINT || type == MSGPACK_TYPE_INT;
}

bool msgpack_get_double(msgpack_in *mp, double *x);

const uint8_t *msgpack_get_bin(msgpack_in *mp, uint32_t *sz_r);

bool msgpack_get_ext(msgpack_in *mp, msgpack_ext *ext);

bool msgpack_get_list_ele_count(msgpack_in *mp, uint32_t *count_r);
static inline bool
msgpack_buf_get_list_ele_count(const uint8_t *buf, uint32_t buf_sz,
		uint32_t *count_r)
{
	msgpack_in mp = {
			.buf = buf,
			.buf_sz = buf_sz
	};

	return msgpack_get_list_ele_count(&mp, count_r);
}
bool msgpack_get_map_ele_count(msgpack_in *mp, uint32_t *count_r);
