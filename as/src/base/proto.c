/*
 * proto.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include "base/proto.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "socket.h"
#include "vector.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

#define MSG_STACK_BUFFER_SZ (1024 * 16)

static const char SUCCESS_BIN_NAME[] = "SUCCESS";
static const char FAILURE_BIN_NAME[] = "FAILURE";


//==========================================================
// Forward declarations.
//

static int send_reply_buf(as_file_handle *fd_h, const uint8_t *msgp, size_t msg_sz);


//==========================================================
// Public API - byte swapping.
//

void
as_proto_swap(as_proto *proto)
{
	uint8_t version = proto->version;
	uint8_t type = proto->type;

	proto->version = proto->type = 0;
	proto->sz = cf_swap_from_be64(*(uint64_t *)proto);
	proto->version = version;
	proto->type = type;
}

void
as_msg_swap_header(as_msg *m)
{
	m->generation = cf_swap_from_be32(m->generation);
	m->record_ttl = cf_swap_from_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_from_be32(m->transaction_ttl);
	m->n_fields = cf_swap_from_be16(m->n_fields);
	m->n_ops = cf_swap_from_be16(m->n_ops);
}

void
as_msg_swap_field(as_msg_field *mf)
{
	mf->field_sz = cf_swap_from_be32(mf->field_sz);
}

void
as_msg_swap_op(as_msg_op *op)
{
	op->op_sz = cf_swap_from_be32(op->op_sz);

	if (op->has_lut == 1) {
		uint64_t *lut = (uint64_t *)(op->name + op->name_sz);

		*lut = cf_swap_from_be64(*lut);
	}
}


//==========================================================
// Public API - generating internal transactions.
//

// Allocates cl_msg returned - caller must free it. Everything is host-ordered.
// Will add more parameters (e.g. for set name) only as they become necessary.
cl_msg *
as_msg_create_internal(const char *ns_name, uint8_t info1, uint8_t info2,
		uint8_t info3, uint32_t record_ttl, uint16_t n_ops, uint8_t *ops,
		size_t ops_sz)
{
	size_t ns_name_len = strlen(ns_name);

	size_t msg_sz = sizeof(cl_msg) +
			sizeof(as_msg_field) + ns_name_len +
			ops_sz;

	cl_msg *msgp = (cl_msg *)cf_malloc(msg_sz);

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_msg *m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = info1;
	m->info2 = info2;
	m->info3 = info3;
	m->info4 = 0;
	m->result_code = 0;
	m->generation = 0;
	m->record_ttl = record_ttl;
	m->transaction_ttl = 0;
	m->n_fields = 1;
	m->n_ops = n_ops;

	as_msg_field *mf = (as_msg_field *)(m->data);

	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	mf->field_sz = (uint32_t)ns_name_len + 1;
	memcpy(mf->data, ns_name, ns_name_len);

	if (ops != NULL) {
		uint8_t *msg_ops = (uint8_t *)as_msg_field_get_next(mf);

		memcpy(msg_ops, ops, ops_sz);
	}

	return msgp;
}


//==========================================================
// Public API - packing responses.
//

// Allocates cl_msg returned - caller must free it.
cl_msg *
as_msg_make_response_msg(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op **ops, as_bin **bins, uint16_t bin_count,
		as_namespace *ns, cl_msg *msgp_in, size_t *msg_sz_in,
		as_record_version *v, uint32_t mrt_deadline)
{
	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (mrt_deadline != 0) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(uint32_t);
	}

	msg_sz += sizeof(as_msg_op) * bin_count;

	for (uint16_t i = 0; i < bin_count; i++) {
		if (ops) {
			msg_sz += ops[i]->name_sz;
		}
		else if (bins[i]) {
			msg_sz += strlen(bins[i]->name);
		}
		else {
			cf_crash(AS_PROTO, "making response message with null bin and op");
		}

		if (bins[i]) {
			msg_sz += as_bin_particle_client_value_size(bins[i]);
		}
	}

	uint8_t *buf;

	if (! msgp_in || *msg_sz_in < msg_sz) {
		buf = cf_malloc(msg_sz);
	}
	else {
		buf = (uint8_t *)msgp_in;
	}

	*msg_sz_in = msg_sz;

	cl_msg *msgp = (cl_msg *)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg *m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation == 0 ? 0 : plain_generation(generation, ns);
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = bin_count;

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field *mf = (as_msg_field *)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version *)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	if (mrt_deadline != 0) {
		as_msg_field *mf = (as_msg_field *)buf;

		mf->field_sz = 1 + sizeof(uint32_t);
		mf->type = AS_MSG_FIELD_TYPE_MRT_DEADLINE;
		*(uint32_t *)mf->data = cf_swap_to_le32(mrt_deadline);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(uint32_t);
	}

	for (uint16_t i = 0; i < bin_count; i++) {
		as_msg_op *op = (as_msg_op *)buf;

		op->has_lut = 0;
		op->unused_flags = 0;

		if (ops) {
			op->op = ops[i]->op;
			memcpy(op->name, ops[i]->name, ops[i]->name_sz);
			op->name_sz = ops[i]->name_sz;
		}
		else {
			op->op = AS_MSG_OP_READ;
			op->name_sz = as_bin_memcpy_name(op->name, bins[i]);
		}

		op->op_sz = OP_FIXED_SZ + op->name_sz;

		buf += sizeof(as_msg_op) + op->name_sz;
		buf += as_bin_particle_to_client(bins[i], op);

		as_msg_swap_op(op);
	}

	return msgp;
}

// Pass NULL bb_r for sizing only. Return value is size if >= 0, error if < 0.
int32_t
as_msg_make_response_bufbuilder(cf_buf_builder **bb_r, as_storage_rd *rd,
		bool no_bin_data, const cf_vector *select_bins, bool send_bval,
		int64_t bval)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;

	size_t ns_len = strlen(ns->name);
	const char *set_name = as_index_get_set_name(r, ns);
	size_t set_name_len = set_name ? strlen(set_name) : 0;

	const uint8_t* key = NULL;
	uint32_t key_size = 0;

	if (r->key_stored == 1) {
		if (! as_storage_rd_load_key(rd)) {
			cf_warning(AS_PROTO, "can't get key - skipping record");
			return -1;
		}

		key = rd->key;
		key_size = rd->key_size;
	}

	uint16_t n_fields = 2; // always add namespace and digest
	size_t msg_sz = sizeof(as_msg) +
			sizeof(as_msg_field) + ns_len +
			sizeof(as_msg_field) + sizeof(cf_digest);

	if (set_name) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + set_name_len;
	}

	if (key) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + key_size;
	}

	if (send_bval) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(bval);
	}

	uint32_t n_select_bins = 0;
	uint16_t n_bins_returned = 0;

	if (! no_bin_data) {
		if (select_bins) {
			n_select_bins = cf_vector_size(select_bins);

			for (uint32_t i = 0; i < n_select_bins; i++) {
				const char* bin_name =
						(const char*)cf_vector_getp((cf_vector*)select_bins, i);

				as_bin *b = as_bin_get_live(rd, bin_name);

				if (! b) {
					continue;
				}

				msg_sz += sizeof(as_msg_op);
				msg_sz += strlen(bin_name);
				msg_sz += as_bin_particle_client_value_size(b);

				n_bins_returned++;
			}

			// Don't return an empty record.
			if (n_bins_returned == 0) {
				return 0;
			}
		}
		else {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin *b = &rd->bins[i];

				if (as_bin_is_tombstone(b)) {
					continue;
				}

				msg_sz += sizeof(as_msg_op);
				msg_sz += strlen(b->name);
				msg_sz += as_bin_particle_client_value_size(b);

				n_bins_returned++;
			}
		}
	}

	uint8_t *buf;

	cf_buf_builder_reserve(bb_r, (int)msg_sz, &buf);

	as_msg *m = (as_msg *)buf;

	m->header_sz = sizeof(as_msg);
	m->info1 = no_bin_data ? AS_MSG_INFO1_GET_NO_BINS : 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = AS_OK;
	m->generation = plain_generation(r->generation, ns);
	m->record_ttl = r->void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;

	if (no_bin_data) {
		m->n_ops = 0;
	}
	else {
		m->n_ops = n_bins_returned;
	}

	as_msg_swap_header(m);

	buf = m->data;

	as_msg_field *mf = (as_msg_field *)buf;

	mf->field_sz = ns_len + 1;
	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, ns->name, ns_len);
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + ns_len;

	mf = (as_msg_field *)buf;
	mf->field_sz = sizeof(cf_digest) + 1;
	mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
	memcpy(mf->data, &r->keyd, sizeof(cf_digest));
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + sizeof(cf_digest);

	if (set_name) {
		mf = (as_msg_field *)buf;
		mf->field_sz = set_name_len + 1;
		mf->type = AS_MSG_FIELD_TYPE_SET;
		memcpy(mf->data, set_name, set_name_len);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + set_name_len;
	}

	if (key) {
		mf = (as_msg_field *)buf;
		mf->field_sz = key_size + 1;
		mf->type = AS_MSG_FIELD_TYPE_KEY;
		memcpy(mf->data, key, key_size);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + key_size;
	}

	if (send_bval) {
		mf = (as_msg_field *)buf;
		mf->field_sz = sizeof(bval) + 1;
		mf->type = AS_MSG_FIELD_TYPE_BVAL_ARRAY;
		*(uint64_t*)mf->data = cf_swap_to_le64((uint64_t)bval);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(bval);
	}

	if (no_bin_data) {
		return (int32_t)msg_sz;
	}

	if (select_bins) {
		for (uint32_t i = 0; i < n_select_bins; i++) {
			const char* bin_name =
					(const char*)cf_vector_getp((cf_vector*)select_bins, i);

			as_bin *b = as_bin_get_live(rd, bin_name);

			if (! b) {
				continue;
			}

			as_msg_op *op = (as_msg_op *)buf;

			op->op = AS_MSG_OP_READ;
			op->has_lut = 0;
			op->unused_flags = 0;
			op->name_sz = as_bin_memcpy_name(op->name, b);
			op->op_sz = OP_FIXED_SZ + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;
			buf += as_bin_particle_to_client(b, op);

			as_msg_swap_op(op);
		}
	}
	else {
		for (uint16_t i = 0; i < rd->n_bins; i++) {
			as_bin *b = &rd->bins[i];

			if (as_bin_is_tombstone(b)) {
				continue;
			}

			as_msg_op *op = (as_msg_op *)buf;

			op->op = AS_MSG_OP_READ;
			op->has_lut = 0;
			op->unused_flags = 0;
			op->name_sz = as_bin_memcpy_name(op->name, b);
			op->op_sz = OP_FIXED_SZ + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;
			buf += as_bin_particle_to_client(b, op);

			as_msg_swap_op(op);
		}
	}

	return (int32_t)msg_sz;
}

void
as_msg_pid_done_bufbuilder(cf_buf_builder **bb_r, uint32_t pid, int result)
{
	uint8_t *buf;

	cf_buf_builder_reserve(bb_r, (int)sizeof(as_msg), &buf);

	as_msg *m = (as_msg *)buf;

	*m = (as_msg){
			.header_sz = sizeof(as_msg),
			.info3 = AS_MSG_INFO3_PARTITION_DONE,
			.result_code = result,
			.generation = pid // HACK - more efficient than separate field
	};

	as_msg_swap_header(m);
}

void
as_msg_fin_bufbuilder(cf_buf_builder **bb_r, int result)
{
	uint8_t *buf;

	cf_buf_builder_reserve(bb_r, (int)sizeof(as_msg), &buf);

	as_msg *m = (as_msg *)buf;

	*m = (as_msg){
			.header_sz = sizeof(as_msg),
			.info3 = AS_MSG_INFO3_LAST,
			.result_code = result
	};

	as_msg_swap_header(m);
}

cl_msg *
as_msg_make_no_val_response(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_record_version *v, size_t *p_msg_sz)
{
	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	uint8_t *buf = cf_malloc(msg_sz);
	cl_msg *msgp = (cl_msg *)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg *m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation;
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = 0;

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field *mf = (as_msg_field *)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version *)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	*p_msg_sz = msg_sz;

	return msgp;
}

cl_msg *
as_msg_make_val_response(bool success, const as_val *val, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_record_version *v,
		size_t *p_msg_sz)
{
	const char *bin_name;
	size_t bin_name_len;

	if (success) {
		bin_name = SUCCESS_BIN_NAME;
		bin_name_len = sizeof(SUCCESS_BIN_NAME) - 1;
	}
	else {
		bin_name = FAILURE_BIN_NAME;
		bin_name_len = sizeof(FAILURE_BIN_NAME) - 1;
	}

	uint16_t n_fields = 0;
	size_t msg_sz = sizeof(cl_msg);

	if (v != NULL) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	msg_sz += sizeof(as_msg_op) + bin_name_len +
			as_particle_asval_client_value_size(val);

	uint8_t *buf = cf_malloc(msg_sz);
	cl_msg *msgp = (cl_msg *)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);

	as_proto_swap(&msgp->proto);

	as_msg *m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = generation;
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_fields = n_fields;
	m->n_ops = 1; // only the one special bin

	as_msg_swap_header(m);

	buf = m->data;

	if (v != NULL) {
		as_msg_field *mf = (as_msg_field *)buf;

		mf->field_sz = 1 + sizeof(as_record_version);
		mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
		*(as_record_version *)mf->data = *v;
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	as_msg_op *op = (as_msg_op *)buf;

	op->op = AS_MSG_OP_READ;
	op->name_sz = (uint8_t)bin_name_len;
	memcpy(op->name, bin_name, op->name_sz);
	op->op_sz = OP_FIXED_SZ + op->name_sz;
	op->has_lut = 0;
	op->unused_flags = 0;

	as_particle_asval_to_client(val, op);

	as_msg_swap_op(op);

	*p_msg_sz = msg_sz;

	return msgp;
}

// Caller-provided val_sz must be the result of calling
// as_particle_asval_client_value_size() for same val.
void
as_msg_make_val_response_bufbuilder(const as_val *val, cf_buf_builder **bb_r,
		uint32_t val_sz, bool success)
{
	const char *bin_name;
	size_t bin_name_len;

	if (success) {
		bin_name = SUCCESS_BIN_NAME;
		bin_name_len = sizeof(SUCCESS_BIN_NAME) - 1;
	}
	else {
		bin_name = FAILURE_BIN_NAME;
		bin_name_len = sizeof(FAILURE_BIN_NAME) - 1;
	}

	size_t msg_sz = sizeof(as_msg) + sizeof(as_msg_op) + bin_name_len + val_sz;

	uint8_t *buf;

	cf_buf_builder_reserve(bb_r, (int)msg_sz, &buf);

	as_msg *m = (as_msg *)buf;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->info4 = 0;
	m->result_code = AS_OK;
	m->generation = 0;
	m->record_ttl = 0;
	m->transaction_ttl = 0;
	m->n_fields = 0;
	m->n_ops = 1; // only the one special bin

	as_msg_swap_header(m);

	as_msg_op *op = (as_msg_op *)m->data;

	op->op = AS_MSG_OP_READ;
	op->name_sz = (uint8_t)bin_name_len;
	memcpy(op->name, bin_name, op->name_sz);
	op->op_sz = OP_FIXED_SZ + op->name_sz;
	op->has_lut = 0;
	op->unused_flags = 0;

	as_particle_asval_to_client(val, op);

	as_msg_swap_op(op);
}


//==========================================================
// Public API - sending responses to client.
//

// Make an individual transaction response and send it.
int
as_msg_send_reply(as_file_handle *fd_h, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op **ops, as_bin **bins,
		uint16_t bin_count, as_namespace *ns, as_record_version *v)
{
	uint8_t stack_buf[MSG_STACK_BUFFER_SZ];
	size_t msg_sz = sizeof(stack_buf);
	uint8_t *msgp = (uint8_t *)as_msg_make_response_msg(result_code, generation,
			void_time, ops, bins, bin_count, ns, (cl_msg *)stack_buf, &msg_sz,
			v, 0);

	int rv = send_reply_buf(fd_h, msgp, msg_sz);

	if (msgp != stack_buf) {
		cf_free(msgp);
	}

	return rv;
}

// Send a pre-made response saved in a dyn-buf.
int
as_msg_send_ops_reply(as_file_handle *fd_h, const cf_dyn_buf *db, bool compress,
		as_proto_comp_stat *comp_stat)
{
	if (! compress) {
		return send_reply_buf(fd_h, db->buf, db->used_sz);
	}

	size_t msg_sz = db->used_sz;
	const uint8_t *msgp = as_proto_compress(db->buf, &msg_sz, comp_stat);

	return send_reply_buf(fd_h, msgp, msg_sz);
}

// Send a blocking "fin" message with default timeout.
bool
as_msg_send_fin(cf_socket *sock, uint32_t result_code)
{
	return as_msg_send_fin_timeout(sock, result_code, CF_SOCKET_TIMEOUT) != 0;
}

// Send a blocking "fin" message with a specified timeout.
size_t
as_msg_send_fin_timeout(cf_socket *sock, uint32_t result_code, int32_t timeout)
{
	cl_msg msgp;

	msgp.proto.version = PROTO_VERSION;
	msgp.proto.type = PROTO_TYPE_AS_MSG;
	msgp.proto.sz = sizeof(as_msg);

	as_proto_swap(&msgp.proto);

	as_msg *m = &msgp.msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = AS_MSG_INFO3_LAST;
	m->info4 = 0;
	m->result_code = result_code;
	m->generation = 0;
	m->record_ttl = 0;
	m->transaction_ttl = 0;
	m->n_fields = 0;
	m->n_ops = 0;

	as_msg_swap_header(m);

	if (cf_socket_send_all(sock, (uint8_t*)&msgp, sizeof(msgp), MSG_NOSIGNAL,
			timeout) < 0) {
		cf_warning(AS_PROTO, "send error - fd %d %s", CSFD(sock),
				cf_strerror(errno));
		return 0;
	}

	return sizeof(cl_msg);
}


//==========================================================
// Local helpers.
//

static int
send_reply_buf(as_file_handle *fd_h, const uint8_t *msgp, size_t msg_sz)
{
	cf_assert(cf_socket_exists(&fd_h->sock), AS_PROTO, "fd is invalid");

	if (cf_socket_send_all(&fd_h->sock, msgp, msg_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_PROTO, "protocol write fail: fd %d sz %zu errno %d",
				CSFD(&fd_h->sock), msg_sz, errno);

		as_end_of_transaction_force_close(fd_h);
		return -1;
	}

	as_end_of_transaction_ok(fd_h);
	return 0;
}
