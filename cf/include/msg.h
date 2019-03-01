/*
 * msg.h
 *
 * Copyright (C) 2008-2018 Aerospike, Inc.
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

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

// These values are used on the wire - don't change them.
typedef enum {
	M_FT_UINT32 = 1,
	M_FT_UNUSED_2 = 2,
	M_FT_UINT64 = 3,
	M_FT_UNUSED_4 = 4,
	M_FT_STR = 5,
	M_FT_BUF = 6,
	M_FT_ARRAY_UINT32 = 7,
	M_FT_ARRAY_UINT64 = 8,
	M_FT_ARRAY_BUF = 9,
	M_FT_ARRAY_STR = 10,
	M_FT_MSGPACK = 11
} msg_field_type; // encoded in uint8_t

// These values are used on the wire - don't change them.
typedef enum {
	M_TYPE_FABRIC = 0,
	M_TYPE_HEARTBEAT_V2 = 1,
	M_TYPE_PAXOS = 2,
	M_TYPE_MIGRATE = 3,
	M_TYPE_PROXY = 4,
	M_TYPE_HEARTBEAT = 5,
	M_TYPE_CLUSTERING = 6,
	M_TYPE_RW = 7,
	M_TYPE_INFO = 8,
	M_TYPE_EXCHANGE = 9,
	M_TYPE_APPEAL = 10,
	M_TYPE_XDR = 11,
	M_TYPE_UNUSED_12 = 12,
	M_TYPE_UNUSED_13 = 13,
	M_TYPE_UNUSED_14 = 14,
	M_TYPE_SMD = 15,
	M_TYPE_UNUSED_16 = 16,
	M_TYPE_UNUSED_17 = 17,
	M_TYPE_MAX = 18
} msg_type; // encoded in uint16_t

typedef struct msg_template_s {
	uint16_t id;
	msg_field_type type;
} msg_template;

struct msg_str_array_s;
struct msg_buf_array_s;

typedef struct msg_field_s {
	uint16_t id;
	bool is_set;
	bool is_free;
	uint32_t field_sz;

	union {
		uint32_t ui32;
		uint64_t ui64;
		char *str;
		uint8_t *buf;
		uint32_t *ui32_a;
		uint64_t *ui64_a;
		struct msg_str_array_s *str_a;
		struct msg_buf_array_s *buf_a;
		void *any_buf;
	} u;
} msg_field;

typedef struct msg_s {
	msg_type type;
	uint16_t n_fields;
	bool just_parsed; // fields point into fabric buffer
	uint32_t bytes_used;
	uint32_t bytes_alloc;
	uint64_t benchmark_time;
	msg_field f[]; // indexed by id
} msg;

// msg header on wire.
typedef struct msg_hdr_s {
	uint32_t size;
	uint16_t type;
} __attribute__ ((__packed__)) msg_hdr;

typedef enum {
	MSG_GET_DIRECT,
	MSG_GET_COPY_MALLOC
} msg_get_type;

typedef enum {
	MSG_SET_HANDOFF_MALLOC,
	MSG_SET_COPY
} msg_set_type;

typedef struct msg_buf_ele_s {
	uint32_t sz;
	uint8_t *ptr;
} msg_buf_ele;


//==========================================================
// Globals.
//

extern cf_atomic_int g_num_msgs;
extern cf_atomic_int g_num_msgs_by_type[M_TYPE_MAX];


//==========================================================
// Public API.
//

//------------------------------------------------
// Object accounting.
//

// Free up a "msg" object. Call this function instead of freeing the msg
// directly in order to keep track of all msgs.
void msg_put(msg *m);

//------------------------------------------------
// Lifecycle.
//

void msg_type_register(msg_type type, const msg_template *mt, size_t mt_sz, size_t scratch_sz);
bool msg_type_is_valid(msg_type type);
msg *msg_create(msg_type type);
void msg_destroy(msg *m);
void msg_incr_ref(msg *m);

//------------------------------------------------
// Pack messages into flattened data.
//

size_t msg_get_wire_size(const msg *m);
size_t msg_get_template_fixed_sz(const msg_template *mt, size_t mt_count);
size_t msg_to_iov_buf(const msg *m, uint8_t *buf, size_t buf_sz, uint32_t *msg_sz_r);
size_t msg_to_wire(const msg *m, uint8_t *buf);

//------------------------------------------------
// Parse flattened data into messages.
//

bool msg_parse(msg *m, const uint8_t *buf, size_t bufsz);
bool msg_parse_hdr(uint32_t *size_r, msg_type *type_r, const uint8_t *buf, size_t sz);
bool msg_parse_fields(msg *m, const uint8_t *buf, size_t sz);

void msg_reset(msg *m);
void msg_preserve_fields(msg *m, uint32_t n_field_ids, ...);
void msg_preserve_all_fields(msg *m);

//------------------------------------------------
// Set fields in messages.
//

void msg_set_uint32(msg *m, int field_id, uint32_t v);
void msg_set_uint64(msg *m, int field_id, uint64_t v);
void msg_set_str(msg *m, int field_id, const char *v, msg_set_type type);
void msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t sz, msg_set_type type);

void msg_set_uint32_array_size(msg *m, int field_id, uint32_t count);
void msg_set_uint32_array(msg *m, int field_id, uint32_t idx, uint32_t v);
void msg_set_uint64_array_size(msg *m, int field_id, uint32_t count);
void msg_set_uint64_array(msg *m, int field_id, uint32_t idx, uint64_t v);

void msg_msgpack_list_set_uint32(msg *m, int field_id, const uint32_t *buf, uint32_t count);
void msg_msgpack_list_set_uint64(msg *m, int field_id, const uint64_t *buf, uint32_t count);
void msg_msgpack_list_set_buf(msg *m, int field_id, const cf_vector *v);

//------------------------------------------------
// Get fields from messages.
//

msg_field_type msg_field_get_type(const msg *m, int field_id);
bool msg_is_set(const msg *m, int field_id);
int msg_get_uint32(const msg *m, int field_id, uint32_t *val_r);
int msg_get_uint64(const msg *m, int field_id, uint64_t *val_r);
int msg_get_str(const msg *m, int field_id, char **str_r, msg_get_type type);
int msg_get_buf(const msg *m, int field_id, uint8_t **buf_r, size_t *sz_r, msg_get_type type);

int msg_get_uint32_array(const msg *m, int field_id, uint32_t idx, uint32_t *val_r);
int msg_get_uint64_array_count(const msg *m, int field_id, uint32_t *count_r);
int msg_get_uint64_array(const msg *m, int field_id, uint32_t idx, uint64_t *val_r);

bool msg_msgpack_list_get_count(const msg *m, int field_id, uint32_t *count_r);
bool msg_msgpack_list_get_uint32_array(const msg *m, int field_id, uint32_t *buf_r, uint32_t *count_r);
bool msg_msgpack_list_get_uint64_array(const msg *m, int field_id, uint64_t *buf_r, uint32_t *count_r);
bool msg_msgpack_list_get_buf_array(const msg *m, int field_id, cf_vector *v_r, bool init_vec);

static inline bool
msg_msgpack_list_get_buf_array_presized(const msg *m, int field_id, cf_vector *v_r)
{
	return msg_msgpack_list_get_buf_array(m, field_id, v_r, false);
}


//==========================================================
// Debugging API.
//

void msg_dump(const msg *m, const char *info);
