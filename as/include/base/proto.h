/*
 * proto.h
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
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

/*
 * wire protocol definition
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_val.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "socket.h"


// Forward declarations.
struct as_bin_s;
struct as_index_s;
struct as_storage_rd_s;
struct as_namespace_s;
struct as_file_handle_s;
struct as_transaction_s;

// These numbers match with cl_types.h on the client

#define AS_PROTO_RESULT_OK							0
#define AS_PROTO_RESULT_FAIL_UNKNOWN				1	// unknown failure - consider retry
#define AS_PROTO_RESULT_FAIL_NOT_FOUND				2
#define AS_PROTO_RESULT_FAIL_GENERATION				3
#define AS_PROTO_RESULT_FAIL_PARAMETER				4
#define AS_PROTO_RESULT_FAIL_RECORD_EXISTS			5	// if 'WRITE_ADD', could fail because already exists
#define AS_PROTO_RESULT_FAIL_UNUSED_6				6	// recycle - was AS_PROTO_RESULT_FAIL_BIN_EXISTS
#define AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH	7
#define AS_PROTO_RESULT_FAIL_OUT_OF_SPACE			8
#define AS_PROTO_RESULT_FAIL_TIMEOUT				9
#define AS_PROTO_RESULT_FAIL_ALWAYS_FORBIDDEN		10	// operation not allowed for current (static) configuration
#define AS_PROTO_RESULT_FAIL_UNAVAILABLE			11	// error returned during node down and partition isn't available
#define AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE		12	// op and bin type incompatibility
#define AS_PROTO_RESULT_FAIL_RECORD_TOO_BIG			13
#define AS_PROTO_RESULT_FAIL_KEY_BUSY				14
#define AS_PROTO_RESULT_FAIL_SCAN_ABORT				15
#define AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE	16	// asked to do something we don't do for a particular configuration
#define AS_PROTO_RESULT_FAIL_UNUSED_17				17	// recycle - was AS_PROTO_RESULT_FAIL_BIN_NOT_FOUND
#define AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD		18
#define AS_PROTO_RESULT_FAIL_KEY_MISMATCH			19
#define AS_PROTO_RESULT_FAIL_NAMESPACE				20
#define AS_PROTO_RESULT_FAIL_BIN_NAME				21
#define AS_PROTO_RESULT_FAIL_FORBIDDEN				22	// operation temporarily not possible
#define AS_PROTO_RESULT_FAIL_ELEMENT_NOT_FOUND		23
#define AS_PROTO_RESULT_FAIL_ELEMENT_EXISTS			24
#define AS_PROTO_RESULT_FAIL_ENTERPRISE_ONLY		25	// attempting enterprise functionality on community build

// Security result codes. Must be <= 255, to fit in one byte. Defined here to
// ensure no overlap with other result codes.
#define AS_SEC_RESULT_OK_LAST			50	// the last message
	// Security message errors.
#define AS_SEC_ERR_NOT_SUPPORTED		51	// security features not supported
#define AS_SEC_ERR_NOT_ENABLED			52	// security features not enabled
#define AS_SEC_ERR_SCHEME				53	// security scheme not supported
#define AS_SEC_ERR_COMMAND				54	// unrecognized command
#define AS_SEC_ERR_FIELD				55	// can't parse field
#define AS_SEC_ERR_STATE				56	// e.g. unexpected command
	// Security procedure errors.
#define AS_SEC_ERR_USER					60	// no user or unknown user
#define AS_SEC_ERR_USER_EXISTS			61	// user already exists
#define AS_SEC_ERR_PASSWORD				62	// no password or bad password
#define AS_SEC_ERR_EXPIRED_PASSWORD		63	// expired password
#define AS_SEC_ERR_FORBIDDEN_PASSWORD	64	// forbidden password (e.g. recently used)
#define AS_SEC_ERR_CREDENTIAL			65	// no credential or bad credential
#define AS_SEC_ERR_EXPIRED_SESSION		66	// expired session token
	// ... room for more ...
#define AS_SEC_ERR_ROLE					70	// no role(s) or unknown role(s)
#define AS_SEC_ERR_ROLE_EXISTS			71	// role already exists
#define AS_SEC_ERR_PRIVILEGE			72	// no privileges or unknown privileges
	// Permission errors.
#define AS_SEC_ERR_NOT_AUTHENTICATED	80	// socket not authenticated
#define AS_SEC_ERR_ROLE_VIOLATION		81	// role (privilege) violation
	// LDAP-related errors.
#define AS_SEC_ERR_LDAP_NOT_ENABLED		90	// LDAP features not enabled
#define AS_SEC_ERR_LDAP_SETUP			91	// LDAP setup error
#define AS_SEC_ERR_LDAP_TLS_SETUP		92	// LDAP TLS setup error
#define AS_SEC_ERR_LDAP_AUTHENTICATION	93	// error authenticating LDAP user
#define AS_SEC_ERR_LDAP_QUERY			94	// error querying LDAP server (e.g. polling for roles)

// UDF Errors (100 - 109)
#define AS_PROTO_RESULT_FAIL_UDF_EXECUTION     100

// Batch Errors (150 - 159)
#define AS_PROTO_RESULT_FAIL_BATCH_DISABLED		150 // batch functionality has been disabled
#define AS_PROTO_RESULT_FAIL_BATCH_MAX_REQUESTS	151 // batch-max-requests has been exceeded
#define AS_PROTO_RESULT_FAIL_BATCH_QUEUES_FULL	152 // all batch queues are full

// Geo Errors (160 - 169)
#define AS_PROTO_RESULT_FAIL_GEO_INVALID_GEOJSON 160 // Invalid GeoJSON on insert/update

// Secondary Index Query Failure Codes (200 - 219)
#define AS_PROTO_RESULT_FAIL_INDEX_FOUND       200
#define AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND    201
#define AS_PROTO_RESULT_FAIL_INDEX_OOM         202
#define AS_PROTO_RESULT_FAIL_INDEX_NOTREADABLE 203
#define AS_PROTO_RESULT_FAIL_INDEX_GENERIC     204
#define AS_PROTO_RESULT_FAIL_INDEX_NAME_MAXLEN 205
#define AS_PROTO_RESULT_FAIL_INDEX_MAXCOUNT    206

#define AS_PROTO_RESULT_FAIL_QUERY_USERABORT   210
#define AS_PROTO_RESULT_FAIL_QUERY_QUEUEFULL   211
#define AS_PROTO_RESULT_FAIL_QUERY_TIMEOUT     212
#define AS_PROTO_RESULT_FAIL_QUERY_CBERROR     213
#define AS_PROTO_RESULT_FAIL_QUERY_NETIO_ERR   214
#define AS_PROTO_RESULT_FAIL_QUERY_DUPLICATE   215

/* SYNOPSIS
 * Aerospike wire protocol
 *
 * Version 2
 *
 * Aerospike uses a message-oriented wire protocol to transfer information.
 * Each message consists of a header, which determines the type and the length
 * to follow. This is called the 'proto_msg'.
 *
 * these messages are vectored out to the correct handler. Over TCP, they can be
 * pipelined (but not out of order). If we wish to support out of order responses,
 * we should upgrade the protocol.
 *
 * the most common type of message is the as_msg, a message which reads or writes
 * a single row to the data store.
 *
 */

#define PROTO_VERSION					2

#define PROTO_TYPE_INFO					1 // ascii-format message for determining server info
#define PROTO_TYPE_SECURITY				2
#define PROTO_TYPE_AS_MSG				3
#define PROTO_TYPE_AS_MSG_COMPRESSED	4
#define PROTO_TYPE_INTERNAL_XDR			5
#define PROTO_TYPE_MAX					6 // if you see 6, it's illegal

#define PROTO_SIZE_MAX (128 * 1024 * 1024) // used simply for validation, as we've been corrupting msgp's

typedef struct as_proto_s {
	uint8_t		version;
	uint8_t		type;
	uint64_t	sz: 48;
	uint8_t		body[0];
} __attribute__ ((__packed__)) as_proto;

/*
 * zlib decompression API needs original size of the compressed data.
 * This structure packs together -
 * header + original size of data + compressed data
 */
typedef struct as_comp_proto_s {
	as_proto    proto;
	uint64_t    orig_sz;
	uint8_t data[0];
}  as_comp_proto;

/* as_msg_field
* Aerospike message field */
typedef struct as_msg_field_s {
#define AS_MSG_FIELD_TYPE_NAMESPACE				0
#define AS_MSG_FIELD_TYPE_SET					1
#define AS_MSG_FIELD_TYPE_KEY					2
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE			4
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY		6 // old batch - deprecated
#define AS_MSG_FIELD_TYPE_TRID					7
#define AS_MSG_FIELD_TYPE_SCAN_OPTIONS			8
#define AS_MSG_FIELD_TYPE_SOCKET_TIMEOUT		9

#define AS_MSG_FIELD_TYPE_INDEX_NAME			21
#define	AS_MSG_FIELD_TYPE_INDEX_RANGE			22
#define AS_MSG_FIELD_TYPE_INDEX_TYPE			26

// UDF RANGE: 30-39
#define AS_MSG_FIELD_TYPE_UDF_FILENAME			30
#define AS_MSG_FIELD_TYPE_UDF_FUNCTION			31
#define AS_MSG_FIELD_TYPE_UDF_ARGLIST			32
#define AS_MSG_FIELD_TYPE_UDF_OP				33

#define AS_MSG_FIELD_TYPE_QUERY_BINLIST			40
#define AS_MSG_FIELD_TYPE_BATCH					41
#define AS_MSG_FIELD_TYPE_BATCH_WITH_SET		42
#define AS_MSG_FIELD_TYPE_PREDEXP				43

	/* NB: field_sz is sizeof(type) + sizeof(data) */
	uint32_t field_sz; // get the data size through the accessor function, don't worry, it's a small macro
	uint8_t type;   // ordering matters :-( see as_transaction_prepare
	uint8_t data[0];
} __attribute__((__packed__)) as_msg_field;

// For as_transaction::field_types, a bit-field to mark which fields are in the
// as_msg.
#define AS_MSG_FIELD_BIT_NAMESPACE			0x00000001
#define AS_MSG_FIELD_BIT_SET				0x00000002
#define AS_MSG_FIELD_BIT_KEY				0x00000004
#define AS_MSG_FIELD_BIT_DIGEST_RIPE		0x00000008
#define AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY	0x00000010 // old batch - deprecated
#define AS_MSG_FIELD_BIT_TRID				0x00000020
#define AS_MSG_FIELD_BIT_SCAN_OPTIONS		0x00000040
#define AS_MSG_FIELD_BIT_SOCKET_TIMEOUT		0x00000080
#define AS_MSG_FIELD_BIT_INDEX_NAME			0x00000100
#define	AS_MSG_FIELD_BIT_INDEX_RANGE		0x00000200
#define AS_MSG_FIELD_BIT_INDEX_TYPE  		0x00000400
#define AS_MSG_FIELD_BIT_UDF_FILENAME		0x00000800
#define AS_MSG_FIELD_BIT_UDF_FUNCTION		0x00001000
#define AS_MSG_FIELD_BIT_UDF_ARGLIST		0x00002000
#define AS_MSG_FIELD_BIT_UDF_OP				0x00004000
#define AS_MSG_FIELD_BIT_QUERY_BINLIST		0x00008000
#define AS_MSG_FIELD_BIT_BATCH				0x00010000
#define AS_MSG_FIELD_BIT_BATCH_WITH_SET		0x00020000
#define AS_MSG_FIELD_BIT_PREDEXP			0x00040000

// as_msg ops

#define AS_MSG_OP_READ 1			// read the value in question
#define AS_MSG_OP_WRITE 2			// write the value in question

// Prospective CDT top-level ops:
#define AS_MSG_OP_CDT_READ 3
#define AS_MSG_OP_CDT_MODIFY 4

#define AS_MSG_OP_INCR 5			// arithmetically add a value to an existing value, works only on integers
// Unused - 6
// Unused - 7
// Unused - 8
#define AS_MSG_OP_APPEND 9			// append a value to an existing value, works on strings and blobs
#define AS_MSG_OP_PREPEND 10		// prepend a value to an existing value, works on strings and blobs
#define AS_MSG_OP_TOUCH 11			// touch a value without doing anything else to it - will increment the generation

#define AS_MSG_OP_MC_INCR 129		// Memcache-compatible version of the increment command
#define AS_MSG_OP_MC_APPEND 130		// append the value to an existing value, works only strings for now
#define AS_MSG_OP_MC_PREPEND 131	// prepend a value to an existing value, works only strings for now
#define AS_MSG_OP_MC_TOUCH 132		// Memcache-compatible touch - does not change generation

#define OP_IS_MODIFY(op) ( \
	   (op) == AS_MSG_OP_INCR \
	|| (op) == AS_MSG_OP_APPEND \
	|| (op) == AS_MSG_OP_PREPEND \
	|| (op) == AS_MSG_OP_MC_INCR \
    || (op) == AS_MSG_OP_MC_APPEND \
    || (op) == AS_MSG_OP_MC_PREPEND \
    )

#define OP_IS_TOUCH(op) ((op) == AS_MSG_OP_TOUCH || (op) == AS_MSG_OP_MC_TOUCH)

typedef struct as_msg_op_s {
	uint32_t op_sz;
	uint8_t  op;
	uint8_t  particle_type;
	uint8_t  version; // now unused
	uint8_t  name_sz;
	uint8_t	 name[0]; // UTF-8
	// there's also a value here but you can't have two variable size arrays
} __attribute__((__packed__)) as_msg_op;

static inline uint8_t * as_msg_op_get_value_p(as_msg_op *op)
{
	return (uint8_t*)op + sizeof(as_msg_op) + op->name_sz;
}

static inline uint32_t as_msg_op_get_value_sz(const as_msg_op *op)
{
	return op->op_sz - (4 + op->name_sz);
}

static inline uint32_t as_msg_field_get_value_sz(as_msg_field *f)
{
	return f->field_sz - 1;
}

static inline uint32_t as_msg_field_get_strncpy(as_msg_field *f, char *dst, int sz)
{
	int fsz = f->field_sz - 1;
	if (sz > fsz) {
		memcpy(dst, f->data, fsz);
		dst[fsz] = 0;
		return fsz;
	}
	else {
		memcpy(dst, f->data, sz - 1);
		dst[sz - 1] = 0;
		return sz - 1;
	}
}

typedef struct as_msg_s {
	/*00 [x00] (08) */	uint8_t		header_sz;	// number of bytes in this header - 22
	/*01 [x01] (09) */	uint8_t		info1;		// bitfield about this request
	/*02 [x02] (10) */	uint8_t		info2;		// filled up, need another
	/*03 [x03] (11) */	uint8_t		info3;		// nice extra space. Mmm, tasty extra space.
	/*04 [x04] (12) */	uint8_t		unused;
	/*05 [x05] (13) */	uint8_t		result_code;
	/*06 [x06] (14) */	uint32_t	generation;
	/*10 [x0A] (18) */	uint32_t	record_ttl;
	/*14 [x10] (22) */	uint32_t	transaction_ttl;
	/*18 [x12] (26) */	uint16_t	n_fields;	// number of fields
	/*20 [x14] (28) */	uint16_t	n_ops;		// number of operations
	/*22 [x16] (30) */	uint8_t		data[0];	// data contains first the fields, then the ops
} __attribute__((__packed__)) as_msg;

/* as_ms
 * Aerospike message
 * sz: size of the payload, not including the header */
typedef struct cl_msg_s {
	as_proto  	proto;
	as_msg		msg;
} __attribute__((__packed__)) cl_msg;

#define AS_MSG_INFO1_READ				(1 << 0) // contains a read operation
#define AS_MSG_INFO1_GET_ALL			(1 << 1) // get all bins, period
// (Note:  Bit 2 is unused.)
#define AS_MSG_INFO1_BATCH				(1 << 3) // new batch protocol
#define AS_MSG_INFO1_XDR				(1 << 4) // operation is being performed by XDR
#define AS_MSG_INFO1_GET_NO_BINS		(1 << 5) // get record metadata only - no bin metadata or data
#define AS_MSG_INFO1_CONSISTENCY_LEVEL_ALL	(1 << 6) // duplicate resolve reads
// (Note:  Bit 7 is unused.)

#define AS_MSG_INFO2_WRITE				(1 << 0) // contains a write semantic
#define AS_MSG_INFO2_DELETE				(1 << 1) // delete record
#define AS_MSG_INFO2_GENERATION			(1 << 2) // pay attention to the generation
#define AS_MSG_INFO2_GENERATION_GT		(1 << 3) // apply write if new generation > old, good for restore
#define AS_MSG_INFO2_DURABLE_DELETE		(1 << 4) // op resulting in record deletion leaves tombstone (Enterprise only)
#define AS_MSG_INFO2_CREATE_ONLY		(1 << 5) // write record only if it doesn't exist
// (Note:  Bit 6 is unused.)
#define AS_MSG_INFO2_RESPOND_ALL_OPS	(1 << 7) // all bin ops (read, write, or modify) require a response, in request order

#define AS_MSG_INFO3_LAST				(1 << 0) // this is the last of a multi-part message
#define AS_MSG_INFO3_COMMIT_LEVEL_MASTER  	(1 << 1) // "fire and forget" replica writes
// (Note:  Bit 2 is unused.)
#define AS_MSG_INFO3_UPDATE_ONLY		(1 << 3) // update existing record only, do not create new record
#define AS_MSG_INFO3_CREATE_OR_REPLACE	(1 << 4) // completely replace existing record, or create new record
#define AS_MSG_INFO3_REPLACE_ONLY		(1 << 5) // completely replace existing record, do not create new record
#define AS_MSG_INFO3_LINEARIZE_READ		(1 << 6) // enterprise only
// (Note:  Bit 7 is unused.)

#define AS_MSG_FIELD_SCAN_UNUSED_2					(0x02) // was - whether to send ldt bin data back to the client
#define AS_MSG_FIELD_SCAN_DISCONNECTED_JOB			(0x04) // for sproc jobs that won't be sending results back to the client [UNUSED]
#define AS_MSG_FIELD_SCAN_FAIL_ON_CLUSTER_CHANGE	(0x08) // if we should fail when cluster is migrating or cluster changes
#define AS_MSG_FIELD_SCAN_PRIORITY(__cl_byte)		((0xF0 & __cl_byte)>>4) // 4 bit value indicating the scan priority

static inline as_msg_field *
as_msg_field_get_next(as_msg_field *mf)
{
	return (as_msg_field*)(((uint8_t*)mf) + sizeof(mf->field_sz) + mf->field_sz);
}

static inline uint8_t *
as_msg_field_skip(as_msg_field *mf)
{
	// At least 1 byte always follow field_sz.
	return mf->field_sz == 0 ? NULL : (uint8_t*)mf + sizeof(mf->field_sz) + mf->field_sz;
}

/* as_msg_field_get
 * Retrieve a specific field from a message */
static inline as_msg_field *
as_msg_field_get(const as_msg *msg, uint8_t type)
{
	uint16_t n;
	as_msg_field *fp = NULL;

	fp = (as_msg_field*)msg->data;

	for (n = 0; n < msg->n_fields; n++) {

		if (fp->type == type) {
			break;
		}

		fp = as_msg_field_get_next(fp);
	}

	if (n == msg->n_fields) {
		return NULL;
	}
	else {
		return fp;
	}
}

static inline as_msg_op *
as_msg_op_get_next(as_msg_op *op)
{
	return (as_msg_op*)(((uint8_t*)op) + sizeof(uint32_t) + op->op_sz);
}

static inline uint8_t *
as_msg_op_skip(as_msg_op *op)
{
	// At least 4 bytes always follow op_sz.
	return (uint32_t)op->name_sz + 4 > op->op_sz ?
			NULL : (uint8_t*)op + sizeof(op->op_sz) + op->op_sz;
}

/* as_msg_field_getnext
 * Iterator for all fields of a particular type.
 * First time through: pass 0 as current, you'll get a field.
 * Next time through: pass the current as current, you'll get null when there
 * are no more.
 */
static inline as_msg_op *
as_msg_op_iterate(as_msg *msg, as_msg_op *current, int *n)
{
	// Skip over the fields the first time.
	if (! current) {
		if (msg->n_ops == 0) {
			return 0; // short cut
		}

		as_msg_field *mf = (as_msg_field*)msg->data;

		for (uint16_t i = 0; i < msg->n_fields; i++) {
			mf = as_msg_field_get_next(mf);
		}

		current = (as_msg_op*)mf;
		*n = 0;

		return current;
	}

	(*n)++;

	if (*n >= msg->n_ops) {
		return 0;
	}

	return as_msg_op_get_next(current);
}

static inline size_t
as_proto_size_get(const as_proto *proto)
{
	return sizeof(as_proto) + proto->sz;
}

static inline bool
as_proto_is_valid_type(const as_proto *proto)
{
	return proto->type != 0 && proto->type < PROTO_TYPE_MAX;
}

static inline bool
as_proto_wrapped_is_valid(const as_proto *proto, size_t size)
{
	return proto->version == PROTO_VERSION &&
			proto->type == PROTO_TYPE_AS_MSG && // currently we only wrap as_msg
			as_proto_size_get(proto) == size;
}

void as_proto_swap(as_proto *proto);
void as_msg_swap_header(as_msg *m);
void as_msg_swap_field(as_msg_field *mf);
void as_msg_swap_op(as_msg_op *op);

cl_msg *as_msg_create_internal(const char *ns_name, const cf_digest *keyd,
		uint8_t info1, uint8_t info2, uint8_t info3);

cl_msg *as_msg_make_response_msg(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op **ops, struct as_bin_s **bins,
		uint16_t bin_count, struct as_namespace_s *ns, cl_msg *msgp_in,
		size_t *msg_sz_in, uint64_t trid);
int32_t as_msg_make_response_bufbuilder(cf_buf_builder **bb_r,
		struct as_storage_rd_s *rd, bool no_bin_data, cf_vector *select_bins);
cl_msg *as_msg_make_val_response(bool success, const as_val *val,
		uint32_t result_code, uint32_t generation, uint32_t void_time,
		uint64_t trid, size_t *p_msg_sz);
void as_msg_make_val_response_bufbuilder(const as_val *val,
		cf_buf_builder **bb_r, uint32_t val_sz, bool);

int as_msg_send_reply(struct as_file_handle_s *fd_h, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op **ops,
		struct as_bin_s **bins, uint16_t bin_count, struct as_namespace_s *ns,
		uint64_t trid);
int as_msg_send_ops_reply(struct as_file_handle_s *fd_h, cf_dyn_buf *db);
bool as_msg_send_fin(cf_socket *sock, uint32_t result_code);
size_t as_msg_send_fin_timeout(cf_socket *sock, uint32_t result_code,
		int32_t timeout);

// Async IO
typedef int (* as_netio_finish_cb) (void *udata, int retcode);
typedef int (* as_netio_start_cb) (void *udata, int seq);
typedef struct as_netio_s {
	as_netio_finish_cb         finish_cb;
	as_netio_start_cb          start_cb;
	void                     * data;
	// fd and buffer
	struct as_file_handle_s  * fd_h;
	cf_buf_builder           * bb_r;
	uint32_t                   offset;
	uint32_t                   seq;
	bool                       slow;
	uint64_t                   start_time;
} as_netio;

void as_netio_init();
int as_netio_send(as_netio *io, bool slow, bool blocking);

#define AS_NETIO_OK        0
#define AS_NETIO_CONTINUE  1
#define AS_NETIO_ERR       2
#define AS_NETIO_IO_ERR    3

// These values correspond to client protocol values - do not change them!
typedef enum as_udf_op {
	AS_UDF_OP_KVS        = 0,
	AS_UDF_OP_AGGREGATE  = 1,
	AS_UDF_OP_BACKGROUND = 2,
	AS_UDF_OP_FOREGROUND = 3		// not supported yet
} as_udf_op;

#define CDT_MAGIC	0xC0 // so we know it can't be (first byte of) msgpack list/map

typedef enum as_cdt_paramtype_e {
	AS_CDT_PARAM_NONE		= 0,

	AS_CDT_PARAM_INDEX		= 1,
	AS_CDT_PARAM_COUNT		= 2,
	AS_CDT_PARAM_PAYLOAD	= 3,
	AS_CDT_PARAM_FLAGS		= 4,
} as_cdt_paramtype;

typedef enum result_type_e {
	RESULT_TYPE_NONE			= 0,
	RESULT_TYPE_INDEX			= 1,
	RESULT_TYPE_REVINDEX		= 2,
	RESULT_TYPE_RANK			= 3,
	RESULT_TYPE_REVRANK			= 4,
	RESULT_TYPE_COUNT			= 5,
	RESULT_TYPE_KEY				= 6,
	RESULT_TYPE_VALUE			= 7,
	RESULT_TYPE_MAP				= 8,
	RESULT_TYPE_INDEX_RANGE		= 9,
	RESULT_TYPE_REVINDEX_RANGE	= 10,
	RESULT_TYPE_RANK_RANGE		= 11,
	RESULT_TYPE_REVRANK_RANGE	= 12,
} result_type_t;

typedef enum {
	AS_CDT_OP_FLAG_RESULT_MASK = 0x0000ffff,
	AS_CDT_OP_FLAG_INVERTED = 0x00010000
} as_cdt_op_flags;

typedef enum {
	AS_CDT_SORT_ASCENDING = 0,
	AS_CDT_SORT_DESCENDING = 1,
	AS_CDT_SORT_DROP_DUPLICATES = 2
} as_cdt_sort_flags;

typedef enum {
	AS_CDT_LIST_MODIFY_DEFAULT = 0x00,
	AS_CDT_LIST_ADD_UNIQUE = 0x01,
	AS_CDT_LIST_INSERT_BOUNDED = 0x02,
	AS_CDT_LIST_NO_FAIL = 0x04,
	AS_CDT_LIST_DO_PARTIAL = 0x08,
} as_cdt_list_modify_flags;

typedef enum {
	AS_CDT_MAP_MODIFY_DEFAULT = 0x00,
	AS_CDT_MAP_NO_OVERWRITE = 0x01,
	AS_CDT_MAP_NO_CREATE = 0x02,
	AS_CDT_MAP_NO_FAIL = 0x04,
	AS_CDT_MAP_DO_PARTIAL = 0x08,
} as_cdt_map_modify_flags;

typedef enum as_cdt_optype_e {
	// ------------------------------------------------------------------------
	// List Operation

	AS_CDT_OP_LIST_SET_TYPE      = 0,

	// Adds
	AS_CDT_OP_LIST_APPEND        = 1,
	AS_CDT_OP_LIST_APPEND_ITEMS  = 2,
	AS_CDT_OP_LIST_INSERT        = 3,
	AS_CDT_OP_LIST_INSERT_ITEMS  = 4,

	// Removes
	AS_CDT_OP_LIST_POP           = 5,
	AS_CDT_OP_LIST_POP_RANGE     = 6,
	AS_CDT_OP_LIST_REMOVE        = 7,
	AS_CDT_OP_LIST_REMOVE_RANGE  = 8,

	// Modifies
	AS_CDT_OP_LIST_SET           = 9,
	AS_CDT_OP_LIST_TRIM          = 10,
	AS_CDT_OP_LIST_CLEAR         = 11,
	AS_CDT_OP_LIST_INCREMENT     = 12,

	AS_CDT_OP_LIST_SORT          = 13,

	// Reads
	AS_CDT_OP_LIST_SIZE          = 16,
	AS_CDT_OP_LIST_GET           = 17,
	AS_CDT_OP_LIST_GET_RANGE     = 18,

	// GET_BYs
	AS_CDT_OP_LIST_GET_BY_INDEX             = 19,
	AS_CDT_OP_LIST_GET_BY_VALUE             = 20,
	AS_CDT_OP_LIST_GET_BY_RANK              = 21,

	AS_CDT_OP_LIST_GET_ALL_BY_VALUE         = 22,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST    = 23,

	AS_CDT_OP_LIST_GET_BY_INDEX_RANGE       = 24,
	AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL    = 25,
	AS_CDT_OP_LIST_GET_BY_RANK_RANGE        = 26,
	AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE = 27,

	// REMOVE_BYs
	AS_CDT_OP_LIST_REMOVE_BY_INDEX          = 32,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE          = 33,
	AS_CDT_OP_LIST_REMOVE_BY_RANK           = 34,

	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE      = 35,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST = 36,

	AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE    = 37,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL = 38,
	AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE     = 39,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE = 40,

	// ------------------------------------------------------------------------
	// Map Operation

	// Create and flags
	AS_CDT_OP_MAP_SET_TYPE							= 64,

	// Modify Ops
	AS_CDT_OP_MAP_ADD								= 65,
	AS_CDT_OP_MAP_ADD_ITEMS							= 66,
	AS_CDT_OP_MAP_PUT								= 67,
	AS_CDT_OP_MAP_PUT_ITEMS							= 68,
	AS_CDT_OP_MAP_REPLACE							= 69,
	AS_CDT_OP_MAP_REPLACE_ITEMS						= 70,
	AS_CDT_OP_MAP_RESERVED_0						= 71,
	AS_CDT_OP_MAP_RESERVED_1						= 72,

	AS_CDT_OP_MAP_INCREMENT							= 73,
	AS_CDT_OP_MAP_DECREMENT							= 74,

	AS_CDT_OP_MAP_CLEAR								= 75,

	AS_CDT_OP_MAP_REMOVE_BY_KEY						= 76,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX					= 77,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE					= 78,
	AS_CDT_OP_MAP_REMOVE_BY_RANK					= 79,

	AS_CDT_OP_MAP_RESERVED_2						= 80,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST				= 81,
	AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE				= 82,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST				= 83,

	AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL			= 84,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE				= 85,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL			= 86,
	AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE				= 87,

	AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE		= 88,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE	= 89,

	// Read ops
	AS_CDT_OP_MAP_SIZE								= 96,

	AS_CDT_OP_MAP_GET_BY_KEY						= 97,
	AS_CDT_OP_MAP_GET_BY_INDEX						= 98,
	AS_CDT_OP_MAP_GET_BY_VALUE						= 99,
	AS_CDT_OP_MAP_GET_BY_RANK						= 100,

	AS_CDT_OP_MAP_RESERVED_3						= 101,
	AS_CDT_OP_MAP_GET_ALL_BY_VALUE					= 102,

	AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL				= 103,
	AS_CDT_OP_MAP_GET_BY_INDEX_RANGE				= 104,
	AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL				= 105,
	AS_CDT_OP_MAP_GET_BY_RANK_RANGE					= 106,

	AS_CDT_OP_MAP_GET_BY_KEY_LIST					= 107,
	AS_CDT_OP_MAP_GET_BY_VALUE_LIST					= 108,

	AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE		= 109,
	AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE		= 110

} as_cdt_optype;

#define AS_CDT_OP_LIST_LAST AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE
