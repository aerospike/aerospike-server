/*
 * msgpack_sz.c
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

#include "base/msgpack_in.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_msgpack.h"
#include "aerospike/as_types.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"


//==========================================================
// Typedefs & constants.
//

#define CMP_EXT_TYPE 0xFF
#define CMP_WILDCARD 0x00
#define CMP_INF      0x01

#define STATE_BLOCK_SIZE 64

typedef enum {
	SIZE_CONST,
	SIZE_8,
	SIZE_16,
	SIZE_32,
	SIZE_LIST16,
	SIZE_LIST32,
	SIZE_MAP16,
	SIZE_MAP32,
	SIZE_CONST_EXT,
	SIZE_8_EXT,
	SIZE_16_EXT,
	SIZE_32_EXT,
} types;

typedef enum {
	TYPE_VOID,
	TYPE_NIL,
	TYPE_FALSE,
	TYPE_TRUE,
	TYPE_INT,
	TYPE_STRING,
	TYPE_LIST,
	TYPE_MAP,
	TYPE_BYTES,
	TYPE_DOUBLE,
	TYPE_GEOJSON,

	// Non-storage types, need to be after storage types.
	TYPE_CMP_WILDCARD, // not a storage type
	TYPE_CMP_INF,      // not a storage type, must be last (biggest value)

	TYPE_BLOB, // string or bytes
	TYPE_EXT,

	MSGPACK_TYPE_COUNT
} msgpack_type;

typedef struct {
	uint32_t sz;
	uint8_t sz_type;
	uint8_t type;
	uint16_t padding;
} entry;

// ((byte ^ 0xc0) & 0xe0) == 0 -- table[byte ^ 0xc0]
static const entry table1[] = {
		{1, SIZE_CONST, TYPE_NIL}, // 0: nil -- 0xC0
		{0, SIZE_CONST, MSGPACK_TYPE_COUNT}, // UNUSED
		{1, SIZE_CONST, TYPE_FALSE}, // false
		{1, SIZE_CONST, TYPE_TRUE}, // true
		{2, SIZE_8, TYPE_BLOB}, // bin 8
		{3, SIZE_16, TYPE_BLOB}, // bin 16
		{5, SIZE_32, TYPE_BLOB}, // bin 32

		{3, SIZE_8_EXT, TYPE_EXT}, // ext 8
		{4, SIZE_16_EXT, TYPE_EXT}, // ext 16
		{6, SIZE_32_EXT, TYPE_EXT}, // ext 32

		{5, SIZE_CONST, TYPE_DOUBLE}, // float 32
		{9, SIZE_CONST, TYPE_DOUBLE}, // float 64
		{2, SIZE_CONST, TYPE_INT}, // uint 8
		{3, SIZE_CONST, TYPE_INT}, // uint 16
		{5, SIZE_CONST, TYPE_INT}, // uint 32
		{9, SIZE_CONST, TYPE_INT}, // uint 64
		{2, SIZE_CONST, TYPE_INT}, // int 8
		{3, SIZE_CONST, TYPE_INT}, // int 16
		{5, SIZE_CONST, TYPE_INT}, // int 32
		{9, SIZE_CONST, TYPE_INT}, // int 64

		{3, SIZE_CONST_EXT, TYPE_EXT}, // fixext 1
		{4, SIZE_CONST_EXT, TYPE_EXT}, // fixext 2
		{6, SIZE_CONST_EXT, TYPE_EXT}, // fixext 4
		{10, SIZE_CONST_EXT, TYPE_EXT}, // fixext 8
		{18, SIZE_CONST_EXT, TYPE_EXT}, // fixext 16

		{2, SIZE_8, TYPE_BLOB}, // str 8
		{3, SIZE_16, TYPE_BLOB}, // str 16
		{5, SIZE_32, TYPE_BLOB}, // 1b: str 32
		{3, SIZE_LIST16, TYPE_LIST}, // array 16
		{5, SIZE_LIST32, TYPE_LIST}, // array 32
		{3, SIZE_MAP16, TYPE_MAP}, // map 16
		{5, SIZE_MAP32, TYPE_MAP}, // 1f: map 32 -- 0xdf
};

typedef struct {
	uint32_t idx;
	uint32_t count;
} state_entry;

typedef struct state_block_s {
	struct state_block_s *next;
	state_entry entries[STATE_BLOCK_SIZE];
} state_block;

typedef struct {
	state_block *current;
	uint32_t level;
} state;

typedef struct {
	msgpack_type type;
	uint32_t len;
	const uint8_t *start;
	const uint8_t *buf;
	const uint8_t *end;
} parse_meta;

typedef struct {
	state s0;
	state s1;
} cmp_state;


//==========================================================
// Forward declares.
//

static inline void state_block_init(state_block *block, uint32_t rep);
static inline msgpack_type bytes_internal_to_msgpack_type(uint8_t type);

static inline bool state_check_and_set_prev(state *s);
static state_entry *state_next(state *s);
static bool state_prev(state *s);
static inline state_entry *current_entry(state *s);
static void state_destroy(state *s);

static inline uint32_t msgpack_sz_internal(state *s, msgpack_in *mp);
static inline uint32_t calc_table1_sz(state *s, const uint8_t *buf, msgpack_in *mp, const entry *e);
static inline uint32_t calc_table2_sz(state *s, const uint8_t *buf);
static inline void handle_list_contents(state *s, uint32_t count);

static inline msgpack_compare_t msgpack_cmp_internal(cmp_state *s, msgpack_in *mp0, msgpack_in *mp1);
static inline msgpack_compare_t msgpack_cmp_peek_internal(cmp_state *s, msgpack_in *mp0, msgpack_in *mp1);
static inline bool msgpack_cmp_parse(state *s, msgpack_in *mp, parse_meta *meta);
static inline uint32_t calc_table1_cmp(state *s, msgpack_in *mp, const entry *e, parse_meta *meta);
static inline uint32_t calc_table2_cmp(state *s, parse_meta *meta);
static inline msgpack_compare_t msgpack_cmp_type(const parse_meta *meta0, const parse_meta *meta1);
static inline void parse_meta_get_int(const parse_meta *meta, uint8_t *num);


//==========================================================
// Macros.
//

#define MSGPACK_COMPARE_RETURN(__p0, __p1) \
	if ((__p0) > (__p1)) { \
		return MSGPACK_COMPARE_GREATER; \
	} \
	else if ((__p0) < (__p1)) { \
		return MSGPACK_COMPARE_LESS; \
	}


//==========================================================
// Public API.
//

uint32_t
msgpack_sz(msgpack_in *mp)
{
	state_block block;

	state s = {
			.current = &block,
	};

	state_block_init(&block, 1);

	return msgpack_sz_internal(&s, mp);
}

uint32_t
msgpack_sz_rep(msgpack_in *mp, uint32_t rep_count)
{
	if (rep_count == 0) {
		return 0;
	}

	state_block block;

	state s = {
			.current = &block,
	};

	state_block_init(&block, rep_count);

	return msgpack_sz_internal(&s, mp);
}

msgpack_compare_t
msgpack_cmp(msgpack_in *mp0, msgpack_in *mp1)
{
	state_block block0;
	state_block block1;

	cmp_state s = {
			.s0 = {
					.current = &block0,
			},
			.s1 = {
					.current = &block1,
			}
	};

	state_block_init(&block0, 1);
	state_block_init(&block1, 1);

	return msgpack_cmp_internal(&s, mp0, mp1);
}

msgpack_compare_t
msgpack_cmp_peek(const msgpack_in *mp0, const msgpack_in *mp1)
{
	state_block block0;
	state_block block1;

	cmp_state s = {
			.s0 = {
					.current = &block0,
			},
			.s1 = {
					.current = &block1,
			}
	};

	state_block_init(&block0, 1);
	state_block_init(&block1, 1);

	msgpack_in t0 = *mp0;
	msgpack_in t1 = *mp1;

	return msgpack_cmp_peek_internal(&s, &t0, &t1);
}


//==========================================================
// Local helpers
//

static inline void
state_block_init(state_block *block, uint32_t rep)
{
	block->next = NULL;
	block->entries[0].idx = 0;
	block->entries[0].count = rep;
}

static inline msgpack_type
bytes_internal_to_msgpack_type(uint8_t type)
{
	if (type == AS_BYTES_STRING) {
		return TYPE_STRING;
	}
	else if (type == AS_BYTES_GEOJSON) {
		return TYPE_GEOJSON;
	}

	// All other types are considered BYTES.
	return TYPE_BYTES;
}

static inline bool
state_check_and_set_prev(state *s)
{
	state_entry *e = current_entry(s);

	while (e->idx == e->count) {
		if (! state_prev(s)) {
			return false;
		}

		e = current_entry(s);
	}

	return true;
}

static state_entry *
state_next(state *s)
{
	s->level++;

	if (s->level % STATE_BLOCK_SIZE == 0) {
		state_block *new_block = cf_malloc(sizeof(state_block));

		new_block->next = s->current;
		s->current = new_block;

		return &new_block->entries[0];
	}

	return current_entry(s);
}

static bool
state_prev(state *s)
{
	if (s->level % STATE_BLOCK_SIZE == 0) {
		state_block *b = s->current->next;

		if (b == NULL) {
			return false;
		}

		cf_free(s->current);
		s->current = b;
	}

	s->level--;

	return true;
}

static inline state_entry *
current_entry(state *s)
{
	return &s->current->entries[s->level % STATE_BLOCK_SIZE];
}

static void
state_destroy(state *s)
{
	while (s->current->next) {
		state_block *block = s->current;

		s->current = s->current->next;
		cf_free(block);
	}
}

static uint32_t
msgpack_sz_internal(state *s, msgpack_in *mp)
{
	const uint8_t *buf = mp->buf + mp->offset;
	const uint8_t * const end = mp->buf + mp->buf_sz;
	uint32_t start = mp->offset;

	while (true) {
		state_entry *e = current_entry(s);
		uint8_t b = *buf ^ 0xc0;
		uint32_t sz;

		e->idx++;

		if ((b & 0xe0) == 0) {
			sz = calc_table1_sz(s, buf, mp, table1 + b);
		}
		else {
			sz = calc_table2_sz(s, buf);
		}

		mp->offset += sz;
		buf += sz;

		if (buf > end || sz == 0) {
			cf_warning(AS_PARTICLE, "msgpack invalid at offset %u sz %u buf %p buf_sz %u", mp->offset, sz, mp->buf, mp->buf_sz);
			state_destroy(s);
			break;
		}

		if (! state_check_and_set_prev(s)) {
			return mp->offset - start;
		}
	}

	return 0;
}

static inline uint32_t
calc_table1_sz(state *s, const uint8_t *buf, msgpack_in *mp, const entry *e)
{
	buf++;

	switch (e->sz_type) {
	case SIZE_CONST:
		break;
	case SIZE_8:
		return e->sz + *buf;
	case SIZE_16:
		return e->sz + cf_swap_from_be16(*(uint16_t *)buf);
	case SIZE_32:
		return e->sz + cf_swap_from_be32(*(uint32_t *)buf);
	case SIZE_LIST16:
		handle_list_contents(s, cf_swap_from_be16(*(uint16_t *)buf));
		break;
	case SIZE_LIST32:
		handle_list_contents(s, cf_swap_from_be32(*(uint32_t *)buf));
		break;
	case SIZE_MAP16:
		handle_list_contents(s, 2 * cf_swap_from_be16(*(uint16_t *)buf));
		break;
	case SIZE_MAP32:
		handle_list_contents(s, 2 * cf_swap_from_be32(*(uint32_t *)buf));
		break;
	case SIZE_CONST_EXT:
		if (*buf == 0xff && (*(buf - 1) == 0xd4 || *(buf - 1) == 0xd5)) {
			mp->has_nonstorage = true;
		}
		break;
	case SIZE_8_EXT: {
		if (*(buf + 1) == 0xff && *buf < 4 && *buf != 0) {
			mp->has_nonstorage = true;
		}
		return e->sz + *buf;
	}
	case SIZE_16_EXT: {
		uint32_t len = cf_swap_from_be16(*(uint16_t *)buf);

		if (*(buf + 2) == 0xff && len < 4 && len != 0) {
			mp->has_nonstorage = true;
		}

		return e->sz + len;
	}
	case SIZE_32_EXT: {
		uint32_t len = cf_swap_from_be32(*(uint32_t *)buf);

		if (*(buf + 4) == 0xff && len < 4 && len != 0) {
			mp->has_nonstorage = true;
		}

		return e->sz + len;
	}
	default:
		cf_warning(AS_PARTICLE, "msgpack invalid at offset %u", (uint32_t)(buf - mp->buf));
		return 0;
	}

	return e->sz;
}

static inline uint32_t
calc_table2_sz(state *s, const uint8_t *buf)
{
	uint8_t b = (*buf ^ 0x80) >> 4;

	switch (b) {
	case 0: // fixmap      -- 1000xxxx -- 0x80 - 0x8f
		handle_list_contents(s, 2 * (*buf & 0x0f));
		break;
	case 1: // fixarray    -- 1001xxxx -- 0x90 - 0x9f
		handle_list_contents(s, *buf & 0x0f);
		break;
	case 2:
	case 3: // fixstr      -- 101xxxxx -- 0xa0 - 0xbf
		return 1 + (*buf & 0x1f);
	case 4:
	case 5: // table1 items
		cf_warning(AS_PARTICLE, "unexpecteds bit in table2");
		return 0;
	case 6:
	case 7: // neg fixint  -- 111xxxxx -- 0xe0 - 0xff
	default: // pos fixint -- 0xxxxxxx -- 0x00 - 0x7f
		break;
	}

	return 1;
}

static inline void
handle_list_contents(state *s, uint32_t count)
{
	if (count != 0) {
		state_entry *e = state_next(s);

		e->idx = 0;
		e->count = count;
	}
}

static bool
msgpack_cmp_unwind_level(state *s, msgpack_in *mp, parse_meta *meta)
{
	if (meta->type == TYPE_LIST) {
		if (meta->len != 0 && msgpack_sz_rep(mp, meta->len) == 0) {
			return false;
		}

		if (! state_prev(s)) {
			cf_crash(AS_PARTICLE, "unexpected state");
		}
	}
	else if (meta->type == TYPE_MAP) {
		if (meta->len != 0 && msgpack_sz_rep(mp, meta->len * 2) == 0) {
			return false;
		}

		if (! state_prev(s)) {
			cf_crash(AS_PARTICLE, "unexpected state");
		}
	}

	state_entry *e = current_entry(s);
	uint32_t left = e->count - e->idx;

	if (left != 0 && msgpack_sz_rep(mp, left) == 0) {
		return false;
	}

	e->idx = e->count;
	meta->buf = mp->buf + mp->offset;

	return true;
}

static inline msgpack_compare_t
msgpack_cmp_list(cmp_state *s, parse_meta *meta0, parse_meta *meta1)
{
	if (meta0->len == 0) {
		if (meta1->len == 1) { // possibly meta
			state_entry *e = state_next(&s->s0);

			e->idx = 0;
			e->count = 0;
		}
		else if (meta1->len != 0) {
			return MSGPACK_COMPARE_LESS;
		}
	}
	else if (meta1->len == 0) {
		if (meta0->len == 1) { // possibly meta
			state_entry *e = state_next(&s->s1);

			e->idx = 0;
			e->count = 0;
		}
		else {
			return MSGPACK_COMPARE_GREATER;
		}
	}

	return MSGPACK_COMPARE_EQUAL;
}

static inline msgpack_compare_t
msgpack_cmp_internal(cmp_state *s, msgpack_in *mp0, msgpack_in *mp1)
{
	parse_meta meta0 = {
			.buf = mp0->buf + mp0->offset,
			.end = mp0->buf + mp0->buf_sz
	};

	parse_meta meta1 = {
			.buf = mp1->buf + mp1->offset,
			.end = mp1->buf + mp1->buf_sz
	};

	while (true) {
		if (! msgpack_cmp_parse(&s->s0, mp0, &meta0) ||
				! msgpack_cmp_parse(&s->s1, mp1, &meta1)) {
			state_destroy(&s->s0);
			state_destroy(&s->s1);
			return MSGPACK_COMPARE_ERROR;
		}

		msgpack_compare_t result;
		bool is_wildcard = false;

		if (meta0.type == TYPE_CMP_WILDCARD ||
				meta1.type == TYPE_CMP_WILDCARD) {
			if (! msgpack_cmp_unwind_level(&s->s0, mp0, &meta0) ||
					! msgpack_cmp_unwind_level(&s->s1, mp1, &meta1)) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}

			is_wildcard = true;
			result = MSGPACK_COMPARE_EQUAL;
		}
		else if (meta0.type == meta1.type) {
			if (meta0.type == TYPE_LIST) {
				result = msgpack_cmp_list(s, &meta0, &meta1);
			}
			else {
				result = msgpack_cmp_type(&meta0, &meta1);
			}

			if (result == MSGPACK_COMPARE_ERROR) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}
		}
		else {
			result = meta0.type < meta1.type ?
					MSGPACK_COMPARE_LESS : MSGPACK_COMPARE_GREATER;
		}

		if (result != MSGPACK_COMPARE_EQUAL) {
			if (state_check_and_set_prev(&s->s0) &&
					msgpack_sz_internal(&s->s0, mp0) == 0) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}

			if (state_check_and_set_prev(&s->s1) &&
					msgpack_sz_internal(&s->s1, mp1) == 0) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}

			return result;
		}

		const state_entry *e0 = current_entry(&s->s0);
		const state_entry *e1 = current_entry(&s->s1);
		uint32_t left0 = e0->count - e0->idx;
		uint32_t left1 = e1->count - e1->idx;

		if (! is_wildcard && left0 != 0 && left1 != 0) {
			continue;
		}
		else if (is_wildcard || left0 == left1) {
			bool has_prev0 = state_check_and_set_prev(&s->s0);
			bool has_prev1 = state_check_and_set_prev(&s->s1);

			if (! has_prev0 && ! has_prev1) {
				break;
			}

			if (has_prev0 && has_prev1) {
				if (s->s0.level == s->s1.level) {
					continue;
				}

				if (s->s0.level < s->s1.level) {
					result = MSGPACK_COMPARE_LESS;
				}
				else {
					result = MSGPACK_COMPARE_GREATER;
				}

				if (msgpack_sz_internal(&s->s0, mp0) == 0 ||
						msgpack_sz_internal(&s->s1, mp1) == 0) {
					result = MSGPACK_COMPARE_ERROR;
				}

				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return result;
			}

			if (has_prev0) {
				if (msgpack_sz_internal(&s->s0, mp0) == 0) {
					state_destroy(&s->s0);
					return MSGPACK_COMPARE_ERROR;
				}

				return MSGPACK_COMPARE_GREATER;
			}
			// else -- has_prev1

			if (msgpack_sz_internal(&s->s1, mp1) == 0) {
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}

			return MSGPACK_COMPARE_LESS;
		}
		else {
			if (left0 == 0) {
				if ((state_check_and_set_prev(&s->s0) &&
						msgpack_sz_internal(&s->s0, mp0) == 0) ||
						msgpack_sz_internal(&s->s1, mp1) == 0) {
					state_destroy(&s->s0);
					state_destroy(&s->s1);
					return MSGPACK_COMPARE_ERROR;
				}

				return MSGPACK_COMPARE_LESS;
			}

			if (left1 == 0) {
				if ((state_check_and_set_prev(&s->s1) &&
						msgpack_sz_internal(&s->s1, mp1) == 0) ||
						msgpack_sz_internal(&s->s0, mp0) == 0) {
					state_destroy(&s->s0);
					state_destroy(&s->s1);
					return MSGPACK_COMPARE_ERROR;
				}

				return MSGPACK_COMPARE_GREATER;
			}
		}
	}

	return MSGPACK_COMPARE_EQUAL;
}

static inline msgpack_compare_t
msgpack_cmp_peek_internal(cmp_state *s, msgpack_in *mp0, msgpack_in *mp1)
{
	parse_meta meta0 = {
			.buf = mp0->buf + mp0->offset,
			.end = mp0->buf + mp0->buf_sz
	};

	parse_meta meta1 = {
			.buf = mp1->buf + mp1->offset,
			.end = mp1->buf + mp1->buf_sz
	};

	while (true) {
		if (! msgpack_cmp_parse(&s->s0, mp0, &meta0) ||
				! msgpack_cmp_parse(&s->s1, mp1, &meta1)) {
			state_destroy(&s->s0);
			state_destroy(&s->s1);
			return MSGPACK_COMPARE_ERROR;
		}

		msgpack_compare_t result;
		bool is_wildcard = false;

		if (meta0.type == TYPE_CMP_WILDCARD ||
				meta1.type == TYPE_CMP_WILDCARD) {
			if (s->s0.level <= 1) {
				return MSGPACK_COMPARE_EQUAL; // common case quick return
			}

			if (! msgpack_cmp_unwind_level(&s->s0, mp0, &meta0) ||
					! msgpack_cmp_unwind_level(&s->s1, mp1, &meta1)) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_ERROR;
			}

			is_wildcard = true;
			result = MSGPACK_COMPARE_EQUAL;
		}
		else if (meta0.type == meta1.type) {
			if (meta0.type == TYPE_LIST) {
				result = msgpack_cmp_list(s, &meta0, &meta1);
			}
			else {
				result = msgpack_cmp_type(&meta0, &meta1);
			}
		}
		else {
			result = meta0.type < meta1.type ?
					MSGPACK_COMPARE_LESS : MSGPACK_COMPARE_GREATER;
		}

		if (result != MSGPACK_COMPARE_EQUAL) {
			state_destroy(&s->s0);
			state_destroy(&s->s1);
			return result;
		}

		const state_entry *e0 = current_entry(&s->s0);
		const state_entry *e1 = current_entry(&s->s1);
		uint32_t left0 = e0->count - e0->idx;
		uint32_t left1 = e1->count - e1->idx;

		if (! is_wildcard && left0 != 0 && left1 != 0) {
			continue;
		}
		else if (is_wildcard || left0 == left1) {
			bool has_next0 = state_check_and_set_prev(&s->s0);
			bool has_next1 = state_check_and_set_prev(&s->s1);

			if (! has_next0 && ! has_next1) {
				break;
			}

			if (has_next0 && has_next1) {
				if (s->s0.level == s->s1.level) {
					continue;
				}

				if (s->s0.level < s->s1.level) {
					result = MSGPACK_COMPARE_LESS;
				}
				else {
					result = MSGPACK_COMPARE_GREATER;
				}

				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return result;
			}

			if (has_next0) {
				state_destroy(&s->s0);
				return MSGPACK_COMPARE_GREATER;
			}
			// else -- has_next1

			state_destroy(&s->s1);
			return MSGPACK_COMPARE_LESS;
		}
		else {
			if (left0 == 0) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_LESS;
			}

			if (left1 == 0) {
				state_destroy(&s->s0);
				state_destroy(&s->s1);
				return MSGPACK_COMPARE_GREATER;
			}
		}
	}

	return MSGPACK_COMPARE_EQUAL;
}

static inline bool
msgpack_cmp_parse(state *s, msgpack_in *mp, parse_meta *meta)
{
	state_entry *e = current_entry(s);

	if (e->idx == e->count) {
		meta->type = TYPE_VOID;
		return true;
	}

	uint8_t b = *meta->buf ^ 0xc0;
	uint32_t sz;

	e->idx++;

	if ((b & 0xe0) == 0) {
		meta->type = table1[b].type;
		sz = calc_table1_cmp(s, mp, table1 + b, meta);
	}
	else {
		sz = calc_table2_cmp(s, meta);
	}

	mp->offset += sz;
	meta->buf += sz;

	if (meta->buf > meta->end || sz == 0) {
		cf_warning(AS_PARTICLE, "msgpack invalid at offset %u sz %u buf %p buf_sz %u", mp->offset, sz, mp->buf, mp->buf_sz);
		return false;
	}

	if (meta->type == TYPE_BLOB) {
		meta->type = (meta->len == 0) ?
				TYPE_BYTES : bytes_internal_to_msgpack_type(*meta->start);
	}
	else if (meta->type == TYPE_EXT) {
		if (*(meta->start - 1) == CMP_EXT_TYPE && meta->len == 1) {
			if (*meta->start == CMP_WILDCARD) {
				meta->type = TYPE_CMP_WILDCARD;
			}
			else if (*meta->start == CMP_INF) {
				meta->type = TYPE_CMP_INF;
			}
		}

		if (meta->type == TYPE_EXT && e->idx == 1) { // skip ordered list/map meta
			if (e->count == 1) {
				meta->type = TYPE_VOID;
				return true;
			}

			return msgpack_cmp_parse(s, mp, meta);
		}
	}

	return true;
}

static inline uint32_t
calc_table1_cmp(state *s, msgpack_in *mp, const entry *e, parse_meta *meta)
{
	const uint8_t *buf = meta->buf + 1;

	switch (e->sz_type) {
	case SIZE_CONST:
		meta->len = e->sz;
		meta->start = buf - 1;
		break;
	case SIZE_8:
		meta->len = *buf;
		meta->start = buf + 1;
		return e->sz + *buf;
	case SIZE_16:
		meta->len = cf_swap_from_be16(*(uint16_t *)buf);
		meta->start = buf + sizeof(uint16_t);
		return e->sz + meta->len;
	case SIZE_32:
		meta->len = cf_swap_from_be32(*(uint32_t *)buf);
		meta->start = buf + sizeof(uint32_t);
		return e->sz + meta->len;
	case SIZE_LIST16:
		meta->len = cf_swap_from_be16(*(uint16_t *)buf);
		handle_list_contents(s, meta->len);
		break;
	case SIZE_LIST32:
		meta->len = cf_swap_from_be32(*(uint32_t *)buf);
		handle_list_contents(s, meta->len);
		break;
	case SIZE_MAP16:
		meta->len = cf_swap_from_be16(*(uint16_t *)buf);
		handle_list_contents(s, 2 * meta->len);
		break;
	case SIZE_MAP32:
		meta->len = cf_swap_from_be32(*(uint32_t *)buf);
		handle_list_contents(s, 2 * meta->len);
		break;
	case SIZE_CONST_EXT:
		if (*buf == 0xff && (*(buf - 1) == 0xd4 || *(buf - 1) == 0xd5)) {
			mp->has_nonstorage = true;
		}

		meta->len = e->sz - 2;
		meta->start = buf + 1;
		break;
	case SIZE_8_EXT:
		meta->len = *buf;
		meta->start = buf + 2;

		if (*(buf + 1) == 0xff && *buf < 4 && *buf != 0) {
			mp->has_nonstorage = true;
		}

		return e->sz + *buf;
	case SIZE_16_EXT:
		meta->len = cf_swap_from_be16(*(uint16_t *)buf);
		meta->start = buf + sizeof(uint16_t) + 1;

		if (*(buf + 2) == 0xff && meta->len < 4 && meta->len != 0) {
			mp->has_nonstorage = true;
		}

		return e->sz + meta->len;
	case SIZE_32_EXT:
		meta->len = cf_swap_from_be32(*(uint32_t *)buf);
		meta->start = buf + sizeof(uint32_t) + 1;

		if (*(buf + 4) == 0xff && meta->len < 4 && meta->len != 0) {
			mp->has_nonstorage = true;
		}

		return e->sz + meta->len;
	default:
		cf_warning(AS_PARTICLE, "msgpack invalid at offset %u", (uint32_t)(buf - mp->buf));
		return 0;
	}

	return e->sz;
}

static inline uint32_t
calc_table2_cmp(state *s, parse_meta *meta)
{
	const uint8_t *buf = meta->buf;
	uint8_t b = (*buf ^ 0x80) >> 4;

	switch (b) {
	case 0: // fixmap      -- 1000xxxx -- 0x80 - 0x8f
		meta->type = TYPE_MAP;
		meta->len = *buf & 0x0f;
		handle_list_contents(s, 2 * meta->len);
		break;
	case 1: { // fixarray    -- 1001xxxx -- 0x90 - 0x9f
		meta->type = TYPE_LIST;
		meta->len = *buf & 0x0f;
		handle_list_contents(s, meta->len);
		break;
	}
	case 2:
	case 3: // fixstr      -- 101xxxxx -- 0xa0 - 0xbf
		meta->type = TYPE_BLOB;
		meta->len = *buf & 0x1f;
		meta->start = buf + 1;
		return 1 + meta->len;
	case 4:
	case 5: // table1 items
		cf_warning(AS_PARTICLE, "unexpecteds bit in table2");
		return 0;
	case 6:
	case 7: // neg fixint  -- 111xxxxx -- 0xe0 - 0xff
	default: // pos fixint -- 0xxxxxxx -- 0x00 - 0x7f
		meta->type = TYPE_INT;
		meta->len = 1;
		meta->start = buf;
		break;
	}

	return 1;
}

static inline msgpack_compare_t
msgpack_cmp_type(const parse_meta *meta0, const parse_meta *meta1)
{
	switch (meta0->type) {
	case TYPE_VOID:
	case TYPE_NIL:
	case TYPE_FALSE:
	case TYPE_TRUE:
	case TYPE_CMP_INF:
		break;
	case TYPE_INT: {
		uint8_t num0[9];
		uint8_t num1[9];

		parse_meta_get_int(meta0, num0);
		parse_meta_get_int(meta1, num1);

		int cmp = memcmp(num0, num1, 9);

		MSGPACK_COMPARE_RETURN(cmp, 0);

		break;
	}
	case TYPE_DOUBLE: {
		as_unpacker pk0 = {
				.buffer = meta0->start,
				.length = meta0->len
		};

		as_unpacker pk1 = {
				.buffer = meta1->start,
				.length = meta1->len
		};

		double num0;
		double num1;

		as_unpack_double(&pk0, &num0);
		as_unpack_double(&pk1, &num1);
		MSGPACK_COMPARE_RETURN(num0, num1);

		break;
	}
	case TYPE_MAP:
		MSGPACK_COMPARE_RETURN(meta0->len, meta1->len);
		break;
	case TYPE_EXT:
	case TYPE_STRING:
	case TYPE_BYTES:
	case TYPE_GEOJSON: {
		size_t len = (meta0->len < meta1->len) ? meta0->len : meta1->len;
		int cmp = memcmp(meta0->start, meta1->start, len);

		MSGPACK_COMPARE_RETURN(cmp, 0);
		MSGPACK_COMPARE_RETURN(meta0->len, meta1->len);

		break;
	}
	case TYPE_LIST:
	default:
		return MSGPACK_COMPARE_ERROR;
	}

	return MSGPACK_COMPARE_EQUAL;
}

// Return 1 for positive int and 0 for negative int.
static inline void
parse_meta_get_int(const parse_meta *meta, uint8_t *num)
{
	if (meta->len == 1) {
		if ((*meta->start & 0x80) != 0) { // neg int
			*((uint64_t *)num) = ~(0ULL) << 8; // sign extend little endian
			num[8] = *meta->start;
			return;
		}
		// else -- pos int

		*((uint64_t *)num) = 1ULL; // little endian
		num[8] = *meta->start;
		return;
	}

	const uint8_t *data = meta->start + 1; // +1 to skip header
	size_t len = meta->len - 1; // -1 for header
	uint8_t *num8_start = ((uint8_t *)num) + sizeof(uint64_t) + 1 - len;

	if ((*meta->start & 0xf0) == 0xd0 && (*data & 0x80) != 0) { // neg int
		*((uint64_t *)num) = ~(0ULL) << 8; // sign extend little endian
		memcpy(num8_start, data, len);
		return;
	}
	// else -- pos int

	*((uint64_t *)num) = 1ULL; // little endian
	memcpy(num8_start, data, len);
	return;
}
