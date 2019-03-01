/*
 * msg.c
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


//==========================================================
// Includes.
//

#include "msg.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>

#include "aerospike/as_msgpack.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"


//==========================================================
// Typedefs & constants.
//

typedef struct msg_type_entry_s {
	const msg_template *mt;
	uint16_t entry_count;
	uint32_t scratch_sz;
} msg_type_entry;

// msg field header on wire.
typedef struct msg_field_hdr_s {
	uint16_t id;
	uint8_t type;
	uint8_t content[];
} __attribute__ ((__packed__)) msg_field_hdr;

#define BUF_FIELD_HDR_SZ (sizeof(msg_field_hdr) + sizeof(uint32_t))


//==========================================================
// Globals.
//

// Total number of "msg" objects allocated:
cf_atomic_int g_num_msgs = 0;

// Total number of "msg" objects allocated per type:
cf_atomic_int g_num_msgs_by_type[M_TYPE_MAX] = { 0 };

static msg_type_entry g_mte[M_TYPE_MAX];


//==========================================================
// Forward declarations.
//

static uint32_t msg_get_field_wire_size(msg_field_type type, uint32_t field_sz);
static uint32_t msg_field_write_hdr(const msg_field *mf, msg_field_type type, uint8_t *buf);
static uint32_t msg_field_write_buf(const msg_field *mf, msg_field_type type, uint8_t *buf);
static void msg_field_save(msg *m, msg_field *mf);
static bool msgpack_list_unpack_hdr(as_unpacker *pk, const msg *m, int field_id, uint32_t *count_r);


//==========================================================
// Inlines.
//

static inline bool
mf_type_is_int(msg_field_type type)
{
	return type == M_FT_UINT32 || type == M_FT_UINT64;
}

static inline msg_field_type
mf_type(const msg_field *mf, msg_type type)
{
	return g_mte[type].mt[mf->id].type;
}

static inline void
mf_destroy(msg_field *mf)
{
	if (mf->is_set) {
		if (mf->is_free) {
			cf_free(mf->u.any_buf);
			mf->is_free = false;
		}

		mf->is_set = false;
	}
}


//==========================================================
// Public API - object accounting.
//

// Call this instead of freeing msg directly, to keep track of all msgs.
void
msg_put(msg *m)
{
	cf_atomic_int_decr(&g_num_msgs);
	cf_atomic_int_decr(&g_num_msgs_by_type[m->type]);
	cf_rc_free(m);
}


//==========================================================
// Public API - lifecycle.
//

void
msg_type_register(msg_type type, const msg_template *mt, size_t mt_sz,
		size_t scratch_sz)
{
	cf_assert(type < M_TYPE_MAX, CF_MSG, "unknown type %d", type);

	msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = (uint16_t)(mt_sz / sizeof(msg_template));

	if (mte->mt) {
		// This happens on the heartbeat version jump - handle gently for now.
		cf_info(CF_MSG, "msg_type_register() type %d already registered", type);
		return;
	}

	cf_assert(mt_count != 0, CF_MSG, "msg_type_register() empty template");

	uint16_t max_id = 0;

	for (uint16_t i = 0; i < mt_count; i++) {
		if (mt[i].id >= max_id) {
			max_id = mt[i].id;
		}
	}

	mte->entry_count = max_id + 1;

	msg_template *table = cf_calloc(mte->entry_count, sizeof(msg_template));

	for (uint16_t i = 0; i < mt_count; i++) {
		table[mt[i].id] = mt[i];
	}

	mte->mt = table;
	mte->scratch_sz = (uint32_t)scratch_sz;
}

bool
msg_type_is_valid(msg_type type)
{
	return type < M_TYPE_MAX && g_mte[type].mt != NULL;
}

msg *
msg_create(msg_type type)
{
	cf_assert(type < M_TYPE_MAX && g_mte[type].mt != NULL, CF_MSG, "invalid type %u", type);

	const msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = mte->entry_count;
	size_t u_sz = sizeof(msg) + (sizeof(msg_field) * mt_count);
	size_t a_sz = u_sz + (size_t)mte->scratch_sz;
	msg *m = cf_rc_alloc(a_sz);

	m->n_fields = mt_count;
	m->bytes_used = (uint32_t)u_sz;
	m->bytes_alloc = (uint32_t)a_sz;
	m->just_parsed = false;
	m->type = type;

	for (uint16_t i = 0; i < mt_count; i++) {
		msg_field *mf = &m->f[i];

		mf->id = i;
		mf->is_set = false;
		mf->is_free = false;
	}

	// Keep track of allocated msgs.
	cf_atomic_int_incr(&g_num_msgs);
	cf_atomic_int_incr(&g_num_msgs_by_type[type]);

	return m;
}

void
msg_destroy(msg *m)
{
	int cnt = cf_rc_release(m);

	if (cnt == 0) {
		for (uint32_t i = 0; i < m->n_fields; i++) {
			mf_destroy(&m->f[i]);
		}

		msg_put(m);
	}
	else {
		cf_assert(cnt > 0, CF_MSG, "msg_destroy(%p) extra call", m);
	}
}

void
msg_incr_ref(msg *m)
{
	cf_rc_reserve(m);
}


//==========================================================
// Public API - pack messages into flattened data.
//

size_t
msg_get_wire_size(const msg *m)
{
	size_t sz = sizeof(msg_hdr);

	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			sz += msg_get_field_wire_size(mf_type(mf, m->type), mf->field_sz);
		}
	}

	return sz;
}

size_t
msg_get_template_fixed_sz(const msg_template *mt, size_t mt_count)
{
	size_t sz = sizeof(msg_hdr);

	for (size_t i = 0; i < mt_count; i++) {
		sz += msg_get_field_wire_size(mt[i].type, 0);
	}

	return sz;
}

// Returns iovec count.
size_t
msg_to_iov_buf(const msg *m, uint8_t *buf, size_t buf_sz, uint32_t *msg_sz_r)
{
	uint32_t body_sz = 0;
	uint32_t int_fields = 0;
	uint32_t set_fields = 0;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_type type = mf_type(mf, m->type);

			set_fields++;
			body_sz += msg_get_field_wire_size(type, mf->field_sz);

			if (mf_type_is_int(type)) {
				int_fields++;
			}
		}
	}

	struct iovec *iov = (struct iovec *)buf;
	uint32_t buf_fields = set_fields - int_fields;
	uint32_t max_iov = MAX(1, 2 * buf_fields);

	cf_assert(buf_sz >= max_iov * sizeof(struct iovec) + sizeof(msg_hdr) +
			int_fields * (sizeof(msg_field_hdr) + sizeof(uint64_t)),
			AS_FABRIC, "buf_sz %lu too small", buf_sz);

	uint8_t *ptr = buf + max_iov * sizeof(struct iovec);
	msg_hdr * const hdr = (msg_hdr *)ptr;
	uint16_t first_buf = m->n_fields;

	hdr->size = cf_swap_to_be32(body_sz);
	hdr->type = cf_swap_to_be16(m->type);
	iov->iov_base = ptr;
	ptr += sizeof(msg_hdr);

	// Pack all INT types.
	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_type type = mf_type(mf, m->type);

			if (mf_type_is_int(type)) {
				ptr += msg_field_write_buf(mf, type, ptr);
			}
			else if (first_buf == m->n_fields) {
				first_buf = i;
			}
		}
	}

	iov->iov_len = ptr - (uint8_t *)iov->iov_base;

	if (buf_fields == 0) {
		*msg_sz_r = body_sz + sizeof(msg_hdr);
		return 1;
	}

	// Pack first buf.
	{
		const msg_field *mf = &m->f[first_buf];
		msg_field_type type = mf_type(mf, m->type);

		msg_field_write_hdr(mf, type, ptr);
		iov->iov_len += BUF_FIELD_HDR_SZ;
		ptr += BUF_FIELD_HDR_SZ;
		iov++;
		iov->iov_base = mf->u.any_buf;
		iov->iov_len = mf->field_sz;
		iov++;
	}

	// Pack remaining buf.
	for (uint16_t i = first_buf + 1; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (! mf->is_set) {
			continue;
		}

		msg_field_type type = mf_type(mf, m->type);

		if (! mf_type_is_int(type)) {
			msg_field_write_hdr(mf, type, ptr);
			iov->iov_base = ptr;
			iov->iov_len = BUF_FIELD_HDR_SZ;
			ptr += BUF_FIELD_HDR_SZ;
			iov++;
			iov->iov_base = mf->u.any_buf;
			iov->iov_len = mf->field_sz;
			iov++;
		}
	}

	cf_assert(ptr <= buf + buf_sz, AS_PARTICLE, "ptr out of bounds %p > buf %p + buf_sz %zu", ptr, buf, buf_sz);
	*msg_sz_r = body_sz + sizeof(msg_hdr);

	return iov - (struct iovec *)buf;
}

size_t
msg_to_wire(const msg *m, uint8_t *buf)
{
	msg_hdr *hdr = (msg_hdr *)buf;

	hdr->type = cf_swap_to_be16(m->type);

	buf += sizeof(msg_hdr);

	const uint8_t *body = buf;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];
		msg_field_type type = mf_type(mf, m->type);

		if (mf->is_set) {
			buf += msg_field_write_buf(mf, type, buf);
		}
	}

	uint32_t body_sz = (uint32_t)(buf - body);

	hdr->size = cf_swap_to_be32(body_sz);

	return sizeof(msg_hdr) + body_sz;
}


//==========================================================
// Public API - parse flattened data into messages.
//

bool
msg_parse(msg *m, const uint8_t *buf, size_t bufsz)
{
	uint32_t sz;
	msg_type type;

	if (! msg_parse_hdr(&sz, &type, buf, bufsz)) {
		cf_warning(CF_MSG, "msg_parse() invalid bufsz %zu too small", bufsz);
		return false;
	}

	if (bufsz < sz + sizeof(msg_hdr)) {
		cf_warning(CF_MSG, "msg_parse() bufsz %zu < msg sz %u + %zu", bufsz, sz, sizeof(msg_hdr));
		return false;
	}

	if (m->type != type) {
		cf_ticker_warning(CF_MSG, "parsed type %d for msg type %d", type, m->type);
		return false;
	}

	return msg_parse_fields(m, buf + sizeof(msg_hdr), sz);
}

bool
msg_parse_hdr(uint32_t *size_r, msg_type *type_r, const uint8_t *buf, size_t sz)
{
	if (sz < sizeof(msg_hdr)) {
		return false;
	}

	const msg_hdr *hdr = (const msg_hdr *)buf;

	*size_r = cf_swap_from_be32(hdr->size);
	*type_r = (msg_type)cf_swap_from_be16(hdr->type);

	return true;
}

bool
msg_parse_fields(msg *m, const uint8_t *buf, size_t sz)
{
	const uint8_t *eob = buf + sz;
	size_t left = sz;

	while (left != 0) {
		if (left < sizeof(msg_field_hdr) + sizeof(uint32_t)) {
			return false;
		}

		const msg_field_hdr *fhdr = (const msg_field_hdr *)buf;
		buf += sizeof(msg_field_hdr);

		uint32_t id = (uint32_t)cf_swap_from_be16(fhdr->id);
		msg_field_type ft = (msg_field_type)fhdr->type;
		size_t fsz;
		uint32_t fsz_sz = 0;

		switch (ft) {
		case M_FT_UINT32:
			fsz = sizeof(uint32_t);
			break;
		case M_FT_UINT64:
			fsz = sizeof(uint64_t);
			break;
		default:
			fsz = cf_swap_from_be32(*(const uint32_t *)buf);
			fsz_sz = sizeof(uint32_t);
			buf += sizeof(uint32_t);
			break;
		}

		if (left < sizeof(msg_field_hdr) + fsz_sz + fsz) {
			return false;
		}

		msg_field *mf;

		if (id >= m->n_fields) {
			mf = NULL;
		}
		else {
			mf = &m->f[id];
		}

		if (mf && ft != mf_type(mf, m->type)) {
			cf_ticker_warning(CF_MSG, "msg type %d: parsed type %d for field type %d", m->type, ft, mf_type(mf, m->type));
			mf = NULL;
		}

		if (mf) {
			mf->is_set = true;

			switch (mf_type(mf, m->type)) {
			case M_FT_UINT32:
				mf->u.ui32 = cf_swap_from_be32(*(uint32_t *)buf);
				break;
			case M_FT_UINT64:
				mf->u.ui64 = cf_swap_from_be64(*(uint64_t *)buf);
				break;
			case M_FT_STR:
			case M_FT_BUF:
			case M_FT_ARRAY_UINT32:
			case M_FT_ARRAY_UINT64:
			case M_FT_ARRAY_STR:
			case M_FT_ARRAY_BUF:
			case M_FT_MSGPACK:
				mf->field_sz = (uint32_t)fsz;
				mf->u.any_buf = (void *)buf;
				mf->is_free = false;
				break;
			default:
				cf_ticker_detail(CF_MSG, "msg_parse: field type %d not supported - skipping", mf_type(mf, m->type));
				mf->is_set = false;
				break;
			}
		}

		if (eob < buf) {
			break;
		}

		buf += fsz;
		left = (size_t)(eob - buf);
	}

	m->just_parsed = true;

	return true;
}

void
msg_reset(msg *m)
{
	m->bytes_used = (uint32_t)((m->n_fields * sizeof(msg_field)) + sizeof(msg));
	m->just_parsed = false;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		mf_destroy(&m->f[i]);
	}
}

void
msg_preserve_fields(msg *m, uint32_t n_field_ids, ...)
{
	bool reflect[m->n_fields];

	for (uint16_t i = 0; i < m->n_fields; i++) {
		reflect[i] = false;
	}

	va_list argp;
	va_start(argp, n_field_ids);

	for (uint32_t n = 0; n < n_field_ids; n++) {
		reflect[va_arg(argp, int)] = true;
	}

	va_end(argp);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			if (reflect[i]) {
				if (m->just_parsed) {
					msg_field_save(m, mf);
				}
			}
			else {
				mf->is_set = false;
			}
		}
	}

	m->just_parsed = false;
}

void
msg_preserve_all_fields(msg *m)
{
	if (! m->just_parsed) {
		return;
	}

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_save(m, mf);
		}
	}

	m->just_parsed = false;
}


//==========================================================
// Public API - set fields in messages.
//

void
msg_set_uint32(msg *m, int field_id, uint32_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.ui32 = v;
}

void
msg_set_uint64(msg *m, int field_id, uint64_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.ui64 = v;
}

void
msg_set_str(msg *m, int field_id, const char *v, msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	mf_destroy(mf);

	mf->field_sz = (uint32_t)strlen(v) + 1;

	if (type == MSG_SET_COPY) {
		uint32_t fsz = mf->field_sz;

		if (m->bytes_alloc - m->bytes_used >= fsz) {
			mf->u.str = (char *)m + m->bytes_used;
			m->bytes_used += fsz;
			mf->is_free = false;
			memcpy(mf->u.str, v, fsz);
		}
		else {
			mf->u.str = cf_strdup(v);
			mf->is_free = true;
		}
	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		mf->u.str = (char *)v;
		mf->is_free = (v != NULL);

		if (! v) {
			cf_warning(CF_MSG, "handoff malloc with null pointer");
		}
	}

	mf->is_set = true;
}

void
msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t sz,
		msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	mf_destroy(mf);

	mf->field_sz = (uint32_t)sz;

	if (type == MSG_SET_COPY) {
		if (m->bytes_alloc - m->bytes_used >= sz) {
			mf->u.buf = (uint8_t *)m + m->bytes_used;
			m->bytes_used += (uint32_t)sz;
			mf->is_free = false;
		}
		else {
			mf->u.buf = cf_malloc(sz);
			mf->is_free = true;
		}

		memcpy(mf->u.buf, v, sz);

	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		mf->u.buf = (void *)v;
		mf->is_free = (v != NULL);

		if (! v) {
			cf_warning(CF_MSG, "handoff malloc with null pointer");
		}
	}

	mf->is_set = true;
}

void
msg_set_uint32_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint32_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint32_t));
	mf->u.ui32_a = cf_malloc(mf->field_sz);
	mf->is_set = true;
	mf->is_free = true;
}

void
msg_set_uint32_array(msg *m, int field_id, uint32_t idx, uint32_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint32_array() field not set");
	cf_assert(idx < (mf->field_sz >> 2), CF_MSG, "msg_set_uint32_array() idx out of bounds");

	mf->u.ui32_a[idx] = cf_swap_to_be32(v);
}

void
msg_set_uint64_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint64_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint64_t));
	mf->u.ui64_a = cf_malloc(mf->field_sz);
	mf->is_set = true;
	mf->is_free = true;
}

void
msg_set_uint64_array(msg *m, int field_id, uint32_t idx, uint64_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint64_array() field not set");
	cf_assert(idx < (mf->field_sz >> 3), CF_MSG, "msg_set_uint64_array() idx out of bounds");

	mf->u.ui64_a[idx] = cf_swap_to_be64(v);
}

void
msg_msgpack_list_set_uint32(msg *m, int field_id, const uint32_t *buf,
		uint32_t count)
{
	msg_field *mf = &m->f[field_id];
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		a_sz += as_pack_uint64_size((uint64_t)buf[i]);
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		e = as_pack_uint64(&pk, (uint64_t)buf[i]);
		cf_assert(e == 0, CF_MSG, "as_pack_str failed");
	}

	mf->is_free = true;
	mf->is_set = true;
}

void
msg_msgpack_list_set_uint64(msg *m, int field_id, const uint64_t *buf,
		uint32_t count)
{
	msg_field *mf = &m->f[field_id];
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		a_sz += as_pack_uint64_size(buf[i]);
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		e = as_pack_uint64(&pk, buf[i]);
		cf_assert(e == 0, CF_MSG, "as_pack_str failed");
	}

	mf->is_free = true;
	mf->is_set = true;
}

void
msg_msgpack_list_set_buf(msg *m, int field_id, const cf_vector *v)
{
	msg_field *mf = &m->f[field_id];
	uint32_t count = cf_vector_size(v);
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		const msg_buf_ele *ele = cf_vector_getp((cf_vector *)v, i);

		if (! ele->ptr) {
			a_sz++; // TODO - add to common later
		}
		else {
			a_sz += as_pack_str_size(ele->sz);
		}
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		const msg_buf_ele *ele = cf_vector_getp((cf_vector *)v, i);

		if (! ele->ptr) {
			pk.buffer[pk.offset++] = 0xc0; // TODO - add to common later
		}
		else {
			e = as_pack_str(&pk, ele->ptr, ele->sz);
			cf_assert(e == 0, CF_MSG, "as_pack_str failed");
		}
	}

	mf->is_free = true;
	mf->is_set = true;
}


//==========================================================
// Public API - get fields from messages.
//

msg_field_type
msg_field_get_type(const msg *m, int field_id)
{
	return mf_type(&m->f[field_id], m->type);
}

bool
msg_is_set(const msg *m, int field_id)
{
	cf_assert(field_id >= 0 && field_id < (int)m->n_fields, CF_MSG, "invalid field_id %d", field_id);

	return m->f[field_id].is_set;
}

int
msg_get_uint32(const msg *m, int field_id, uint32_t *val_r)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.ui32;

	return 0;
}

int
msg_get_uint64(const msg *m, int field_id, uint64_t *val_r)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.ui64;

	return 0;
}

int
msg_get_str(const msg *m, int field_id, char **str_r, msg_get_type type)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	uint32_t sz = m->f[field_id].field_sz;

	if (sz == 0 || m->f[field_id].u.str[sz - 1] != '\0') {
		cf_warning(CF_MSG, "msg_get_str: invalid string");
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		*str_r = m->f[field_id].u.str;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		*str_r = cf_strdup(m->f[field_id].u.str);
	}
	else {
		cf_crash(CF_MSG, "msg_get_str: illegal msg_get_type");
	}

	return 0;
}

int
msg_get_buf(const msg *m, int field_id, uint8_t **buf_r, size_t *sz_r,
		msg_get_type type)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		*buf_r = m->f[field_id].u.buf;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		*buf_r = cf_malloc(m->f[field_id].field_sz);
		memcpy(*buf_r, m->f[field_id].u.buf, m->f[field_id].field_sz);
	}
	else {
		cf_crash(CF_MSG, "msg_get_buf: illegal msg_get_type");
	}

	if (sz_r) {
		*sz_r = m->f[field_id].field_sz;
	}

	return 0;
}

int
msg_get_uint32_array(const msg *m, int field_id, uint32_t index,
		uint32_t *val_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be32(mf->u.ui32_a[index]);

	return 0;
}

int
msg_get_uint64_array_count(const msg *m, int field_id, uint32_t *count_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*count_r = mf->field_sz >> 3;

	return 0;
}

int
msg_get_uint64_array(const msg *m, int field_id, uint32_t index,
		uint64_t *val_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be64(mf->u.ui64_a[index]);

	return 0;
}

bool
msg_msgpack_list_get_count(const msg *m, int field_id, uint32_t *count_r)
{
	as_unpacker pk;

	return msgpack_list_unpack_hdr(&pk, m, field_id, count_r);
}

bool
msg_msgpack_list_get_uint32_array(const msg *m, int field_id, uint32_t *buf_r,
		uint32_t *count_r)
{
	cf_assert(buf_r, CF_MSG, "buf_r is null");

	as_unpacker pk;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&pk, m, field_id, &count)) {
		return false;
	}

	if (*count_r < count) {
		cf_warning(CF_MSG, "count_r %u < %u - too small", *count_r, count);
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint64_t val;
		int ret = as_unpack_uint64(&pk, &val);

		if (ret != 0 || (val & (0xFFFFffffUL << 32)) != 0) {
			cf_warning(CF_MSG, "i %u/%u invalid packed uint32 ret %d val 0x%lx", i, count, ret, val);
			return false;
		}

		buf_r[i] = (uint32_t)val;
	}

	*count_r = count;

	return true;
}

bool
msg_msgpack_list_get_uint64_array(const msg *m, int field_id, uint64_t *buf_r,
		uint32_t *count_r)
{
	cf_assert(buf_r, CF_MSG, "buf_r is null");

	as_unpacker pk;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&pk, m, field_id, &count)) {
		return false;
	}

	if (*count_r < count) {
		cf_warning(CF_MSG, "count_r %u < %u - too small", *count_r, count);
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint64_t val;
		int ret = as_unpack_uint64(&pk, &val);

		if (ret != 0) {
			cf_warning(CF_MSG, "i %u/%u invalid packed uint64 ret %d val 0x%lx", i, count, ret, val);
			return false;
		}

		buf_r[i] = val;
	}

	*count_r = count;

	return true;
}

bool
msg_msgpack_list_get_buf_array(const msg *m, int field_id, cf_vector *v_r,
		bool init_vec)
{
	as_unpacker pk;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&pk, m, field_id, &count)) {
		return false;
	}

	if (init_vec) {
		if (cf_vector_init(v_r, sizeof(msg_buf_ele), count, 0) != 0) {
			cf_warning(CF_MSG, "vector malloc failed - count %u", count);
			return false;
		}
	}
	else if (count > v_r->capacity) { // TODO - wrap to avoid access of private members?
		cf_warning(CF_MSG, "count %u > vector cap %u", count, v_r->capacity);
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		msg_buf_ele ele;
		int saved_offset = pk.offset;

		ele.ptr = (uint8_t *)as_unpack_str(&pk, &ele.sz);

		if (! ele.ptr) {
			pk.offset = saved_offset;
			ele.sz = 0;

			if (as_unpack_size(&pk) <= 0) {
				if (init_vec) {
					cf_vector_destroy(v_r);
				}

				cf_warning(CF_MSG, "i %u/%u invalid packed buf", i, count);

				return false;
			}
		}

		cf_vector_append(v_r, &ele);
	}

	return true;
}


//==========================================================
// Public API - debugging only.
//

void
msg_dump(const msg *m, const char *info)
{
	cf_info(CF_MSG, "msg_dump: %s: msg %p rc %d n-fields %u bytes-used %u bytes-alloc'd %u type %d",
			info, m, (int)cf_rc_count((void*)m), m->n_fields, m->bytes_used,
			m->bytes_alloc, m->type);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf =  &m->f[i];

		cf_info(CF_MSG, "mf %02u: id %u is-set %d", i, mf->id, mf->is_set);

		if (mf->is_set) {
			switch (mf_type(mf, m->type)) {
			case M_FT_UINT32:
				cf_info(CF_MSG, "   type UINT32 value %u", mf->u.ui32);
				break;
			case M_FT_UINT64:
				cf_info(CF_MSG, "   type UINT64 value %lu", mf->u.ui64);
				break;
			case M_FT_STR:
				cf_info(CF_MSG, "   type STR sz %u free %c value %s",
						mf->field_sz, mf->is_free ? 't' : 'f', mf->u.str);
				break;
			case M_FT_BUF:
				cf_info_binary(CF_MSG, mf->u.buf, mf->field_sz,
						CF_DISPLAY_HEX_COLUMNS,
						"   type BUF sz %u free %c value ",
						mf->field_sz, mf->is_free ? 't' : 'f');
				break;
			case M_FT_ARRAY_UINT32:
				cf_info(CF_MSG, "   type ARRAY_UINT32: count %u n-uint32 %u free %c",
						mf->field_sz, mf->field_sz >> 2,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 2;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %u",
								j, cf_swap_from_be32(mf->u.ui32_a[j]));
					}
				}
				break;
			case M_FT_ARRAY_UINT64:
				cf_info(CF_MSG, "   type ARRAY_UINT64: count %u n-uint64 %u free %c",
						mf->field_sz, mf->field_sz >> 3,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 3;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %lu",
								j, cf_swap_from_be64(mf->u.ui64_a[j]));
					}
				}
				break;
			default:
				cf_info(CF_MSG, "   type %d unknown", mf_type(mf, m->type));
				break;
			}
		}
	}
}


//==========================================================
// Local helpers.
//

static uint32_t
msg_get_field_wire_size(msg_field_type type, uint32_t field_sz)
{
	switch (type) {
	case M_FT_UINT32:
		return sizeof(msg_field_hdr) + sizeof(uint32_t);
	case M_FT_UINT64:
		return sizeof(msg_field_hdr) + sizeof(uint64_t);
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
		break;
	}

	return BUF_FIELD_HDR_SZ + field_sz;
}

static uint32_t
msg_field_write_hdr(const msg_field *mf, msg_field_type type, uint8_t *buf)
{
	msg_field_hdr *hdr = (msg_field_hdr *)buf;

	buf += sizeof(msg_field_hdr);

	hdr->id = cf_swap_to_be16((uint16_t)mf->id);
	hdr->type = (uint8_t)type;

	switch (type) {
	case M_FT_UINT32:
		*(uint32_t *)buf = cf_swap_to_be32(mf->u.ui32);
		return sizeof(msg_field_hdr) + sizeof(uint32_t);
	case M_FT_UINT64:
		*(uint64_t *)buf = cf_swap_to_be64(mf->u.ui64);
		return sizeof(msg_field_hdr) + sizeof(uint64_t);
	default:
		break;
	}

	switch (type) {
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		*(uint32_t *)buf = cf_swap_to_be32(mf->field_sz);
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
	}

	return 0; // incomplete
}

// Returns the number of bytes written.
static uint32_t
msg_field_write_buf(const msg_field *mf, msg_field_type type, uint8_t *buf)
{
	uint32_t hdr_sz = msg_field_write_hdr(mf, type, buf);

	if (hdr_sz != 0) {
		return hdr_sz;
	}

	memcpy(buf + BUF_FIELD_HDR_SZ, mf->u.any_buf, mf->field_sz);

	return (uint32_t)(BUF_FIELD_HDR_SZ + mf->field_sz);
}

static void
msg_field_save(msg *m, msg_field *mf)
{
	switch (mf_type(mf, m->type)) {
	case M_FT_UINT32:
	case M_FT_UINT64:
		break;
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		// Should only preserve received messages where buffer pointers point
		// directly into a fabric buffer.
		cf_assert(! mf->is_free, CF_MSG, "invalid msg preserve");

		if (m->bytes_alloc - m->bytes_used >= mf->field_sz) {
			void *buf = ((uint8_t *)m) + m->bytes_used;

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			m->bytes_used += mf->field_sz;
			mf->is_free = false;
		}
		else {
			void *buf = cf_malloc(mf->field_sz);

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			mf->is_free = true;
		}
		break;
	default:
		break;
	}
}

static bool
msgpack_list_unpack_hdr(as_unpacker *pk, const msg *m, int field_id,
		uint32_t *count_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return false;
	}

	pk->buffer = (const uint8_t *)mf->u.any_buf;
	pk->offset = 0;
	pk->length = (int)mf->field_sz;

	int64_t count = as_unpack_list_header_element_count(pk);

	if (count < 0) {
		cf_ticker_warning(CF_MSG, "invalid packed list");
		return false;
	}

	*count_r = (uint32_t)count;

	return true;
}
