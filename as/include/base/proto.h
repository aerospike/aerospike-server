/*
 * proto.h
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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_val.h"
#include "citrusleaf/cf_digest.h"

#include "dynbuf.h"
#include "socket.h"
#include "vector.h"


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_file_handle_s;
struct as_index_s;
struct as_namespace_s;
struct as_storage_rd_s;


//==========================================================
// Typedefs & constants.
//

//------------------------------------------------
// Result codes used in client protocol. Must
// match those in as_status.h in the client. Must
// be <= 255, to fit in one byte.
//

// Generic.
#define AS_OK                           0
#define AS_ERR_UNKNOWN                  1
#define AS_ERR_NOT_FOUND                2
#define AS_ERR_GENERATION               3
#define AS_ERR_PARAMETER                4
#define AS_ERR_RECORD_EXISTS            5
#define AS_ERR_BIN_EXISTS               6
#define AS_ERR_CLUSTER_KEY_MISMATCH     7
#define AS_ERR_OUT_OF_SPACE             8
#define AS_ERR_TIMEOUT                  9
#define AS_ERR_UNUSED_10                10 // recycle as XDR 'permanent' error
#define AS_ERR_UNAVAILABLE              11
#define AS_ERR_INCOMPATIBLE_TYPE        12
#define AS_ERR_RECORD_TOO_BIG           13
#define AS_ERR_KEY_BUSY                 14
#define AS_ERR_SCAN_ABORT               15
#define AS_ERR_UNSUPPORTED_FEATURE      16
#define AS_ERR_BIN_NOT_FOUND            17
#define AS_ERR_DEVICE_OVERLOAD          18
#define AS_ERR_KEY_MISMATCH             19
#define AS_ERR_NAMESPACE                20
#define AS_ERR_BIN_NAME                 21
#define AS_ERR_FORBIDDEN                22
#define AS_ERR_ELEMENT_NOT_FOUND        23
#define AS_ERR_ELEMENT_EXISTS           24
#define AS_ERR_ENTERPRISE_ONLY          25
#define AS_ERR_OP_NOT_APPLICABLE        26
#define AS_ERR_FILTERED_OUT             27
#define AS_ERR_LOST_CONFLICT            28

// Security. (Defined here to ensure no overlap with other result codes.)
#define AS_SEC_OK_LAST                  50 // the last message
	// Security message errors.
#define AS_SEC_ERR_NOT_SUPPORTED        51 // security features not supported
#define AS_SEC_ERR_NOT_CONFIGURED       52 // security features not configured
#define AS_SEC_ERR_SCHEME               53 // security scheme not supported
#define AS_SEC_ERR_COMMAND              54 // unrecognized command
#define AS_SEC_ERR_FIELD                55 // can't parse field
#define AS_SEC_ERR_STATE                56 // e.g. unexpected command
	// Security procedure errors.
#define AS_SEC_ERR_USER                 60 // no/unknown user
#define AS_SEC_ERR_USER_EXISTS          61 // user already exists
#define AS_SEC_ERR_PASSWORD             62 // no/bad password
#define AS_SEC_ERR_EXPIRED_PASSWORD     63 // expired password
#define AS_SEC_ERR_FORBIDDEN_PASSWORD   64 // e.g. recently used password
#define AS_SEC_ERR_CREDENTIAL           65 // no/bad credential
#define AS_SEC_ERR_EXPIRED_SESSION      66 // expired session token
	// ... room for more ...
#define AS_SEC_ERR_ROLE                 70 // no/unknown role(s)
#define AS_SEC_ERR_ROLE_EXISTS          71 // role already exists
#define AS_SEC_ERR_PRIVILEGE            72 // no/unknown privilege(s)
#define AS_SEC_ERR_WHITELIST            73 // bad whitelist
#define AS_SEC_ERR_QUOTAS_NOT_ENABLED   74 // quotas not enabled
#define AS_SEC_ERR_QUOTA                75 // bad quota value
	// Permission errors.
#define AS_SEC_ERR_NOT_AUTHENTICATED    80 // socket not authenticated
#define AS_SEC_ERR_ROLE_VIOLATION       81 // role (privilege) violation
#define AS_SEC_ERR_NOT_WHITELISTED      82 // client IP-addr not on whitelist
#define AS_SEC_ERR_QUOTA_EXCEEDED       83 // quota currently exceeded
	// LDAP-related errors.
#define AS_SEC_ERR_LDAP_NOT_CONFIGURED  90 // LDAP features not configured
#define AS_SEC_ERR_LDAP_SETUP           91 // LDAP setup error
#define AS_SEC_ERR_LDAP_TLS_SETUP       92 // LDAP TLS setup error
#define AS_SEC_ERR_LDAP_AUTHENTICATION  93 // error authenticating LDAP user
#define AS_SEC_ERR_LDAP_QUERY           94 // error querying LDAP server

// UDF.
#define AS_ERR_UDF_EXECUTION            100

// Batch.
#define AS_ERR_BATCH_DISABLED           150
#define AS_ERR_BATCH_MAX_REQUESTS       151
#define AS_ERR_BATCH_QUEUES_FULL        152

// Geo.
#define AS_ERR_GEO_INVALID_GEOJSON      160

// Secondary Index.
#define AS_ERR_SINDEX_FOUND             200
#define AS_ERR_SINDEX_NOT_FOUND         201
#define AS_ERR_SINDEX_OOM               202
#define AS_ERR_SINDEX_NOT_READABLE      203
#define AS_ERR_SINDEX_GENERIC           204
#define AS_ERR_SINDEX_NAME              205 // never used
#define AS_ERR_SINDEX_MAX_COUNT         206

// Query.
#define AS_ERR_QUERY_USER_ABORT         210
#define AS_ERR_QUERY_QUEUE_FULL         211
#define AS_ERR_QUERY_TIMEOUT            212
#define AS_ERR_QUERY_CB                 213
#define AS_ERR_QUERY_NET_IO             214
#define AS_ERR_QUERY_DUPLICATE          215

//------------------------------------------------
// as_proto.
//

typedef struct as_proto_s {
	uint8_t version;
	uint8_t type;
	uint64_t sz: 48; // body size
	uint8_t body[0];
} __attribute__ ((__packed__)) as_proto;

// Current version of as_proto header - not version of (body) message type.
#define PROTO_VERSION 2

// as_proto (body) message types.
#define PROTO_TYPE_INFO                 1
#define PROTO_TYPE_SECURITY             2
#define PROTO_TYPE_AS_MSG               3
#define PROTO_TYPE_AS_MSG_COMPRESSED    4
#define PROTO_TYPE_INTERNAL_XDR         5
#define PROTO_TYPE_LAST_PLUS_1          6

// Limit for sanity-checking.
#define PROTO_SIZE_MAX (128 * 1024 * 1024)

// Wrapper for compressed message.
typedef struct as_comp_proto_s {
	as_proto proto;
	uint64_t orig_sz;
	uint8_t data[0]; // compressed message (includes its own header)
}  as_comp_proto;

// Container for proto compression stats.
typedef struct as_proto_comp_stat_s {
	double uncomp_pct;  // percent of attempts that don't compress result
	double avg_orig_sz; // average original size (compressed results only)
	double avg_comp_sz; // average final size (compressed results only)
} as_proto_comp_stat;

//------------------------------------------------
// as_msg.
//

typedef struct as_msg_s {
	uint8_t header_sz; // size of this header - 22
	uint8_t info1;
	uint8_t info2;
	uint8_t info3;
	uint8_t unused;
	uint8_t result_code;
	uint32_t generation;
	uint32_t record_ttl;
	uint32_t transaction_ttl;
	uint16_t n_fields;
	uint16_t n_ops;
	uint8_t data[0]; // first fields, then ops
} __attribute__((__packed__)) as_msg;

// cl_msg - convenient wrapper for message with as_msg body.
typedef struct cl_msg_s {
	as_proto proto;
	as_msg msg;
} __attribute__((__packed__)) cl_msg;

// Bits in info1.
#define AS_MSG_INFO1_READ                   (1 << 0) // contains a read operation
#define AS_MSG_INFO1_GET_ALL                (1 << 1) // get all bins
	// Bit 2 is unused.
#define AS_MSG_INFO1_BATCH                  (1 << 3) // batch protocol
#define AS_MSG_INFO1_XDR                    (1 << 4) // operation is via XDR
#define AS_MSG_INFO1_GET_NO_BINS            (1 << 5) // get record metadata only - no bin metadata or data
#define AS_MSG_INFO1_CONSISTENCY_LEVEL_ALL  (1 << 6) // duplicate resolve reads
#define AS_MSG_INFO1_COMPRESS_RESPONSE      (1 << 7) // (enterprise only)

// Bits in info2.
#define AS_MSG_INFO2_WRITE                  (1 << 0) // contains a write semantic
#define AS_MSG_INFO2_DELETE                 (1 << 1) // delete record
#define AS_MSG_INFO2_GENERATION             (1 << 2) // pay attention to the generation
#define AS_MSG_INFO2_GENERATION_GT          (1 << 3) // apply write if new generation > old, good for restore
#define AS_MSG_INFO2_DURABLE_DELETE         (1 << 4) // op resulting in record deletion leaves tombstone (enterprise only)
#define AS_MSG_INFO2_CREATE_ONLY            (1 << 5) // write record only if it doesn't exist
	// Bit 6 is unused.
#define AS_MSG_INFO2_RESPOND_ALL_OPS        (1 << 7) // all bin ops (read, write, or modify) require a response, in request order

// Bits in info3.
#define AS_MSG_INFO3_LAST                   (1 << 0) // this is the last of a multi-part message
#define AS_MSG_INFO3_COMMIT_LEVEL_MASTER    (1 << 1) // "fire and forget" replica writes
#define AS_MSG_INFO3_PARTITION_DONE         (1 << 2) // in scan/query response, partition is done
#define AS_MSG_INFO3_UPDATE_ONLY            (1 << 3) // update existing record only, do not create new record
#define AS_MSG_INFO3_CREATE_OR_REPLACE      (1 << 4) // completely replace existing record, or create new record
#define AS_MSG_INFO3_REPLACE_ONLY           (1 << 5) // completely replace existing record, do not create new record
#define AS_MSG_INFO3_SC_READ_TYPE           (1 << 6) // (enterprise only)
#define AS_MSG_INFO3_SC_READ_RELAX          (1 << 7) // (enterprise only)

// Interpret SC_READ bits in info3.
//
// RELAX   TYPE
//                strict
//                ------
//   0      0     sequential (default)
//   0      1     linearize
//
//                relaxed
//                -------
//   1      0     allow prole
//   1      1     allow unavailable

//------------------------------------------------
// as_msg_field.
//

typedef struct as_msg_field_s {
	uint32_t field_sz; // includes type
	uint8_t type;
	uint8_t data[0];
} __attribute__((__packed__)) as_msg_field;

// Generic.
#define AS_MSG_FIELD_TYPE_NAMESPACE         0
#define AS_MSG_FIELD_TYPE_SET               1
#define AS_MSG_FIELD_TYPE_KEY               2
	// 3 is unused.
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE       4
	// 5 is unused.
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY 6 // old batch - unsupported
#define AS_MSG_FIELD_TYPE_TRID              7
#define AS_MSG_FIELD_TYPE_SCAN_OPTIONS      8 // old scan - unsupported
#define AS_MSG_FIELD_TYPE_SOCKET_TIMEOUT    9
#define AS_MSG_FIELD_TYPE_RECS_PER_SEC      10
#define AS_MSG_FIELD_TYPE_PID_ARRAY         11
#define AS_MSG_FIELD_TYPE_DIGEST_ARRAY      12
#define AS_MSG_FIELD_TYPE_SAMPLE_MAX        13
#define AS_MSG_FIELD_TYPE_LUT               14 // for XDR writes only

// Secondary index.
#define AS_MSG_FIELD_TYPE_INDEX_NAME        21
#define AS_MSG_FIELD_TYPE_INDEX_RANGE       22
#define AS_MSG_FIELD_TYPE_INDEX_TYPE        26

// UDF.
#define AS_MSG_FIELD_TYPE_UDF_FILENAME      30
#define AS_MSG_FIELD_TYPE_UDF_FUNCTION      31
#define AS_MSG_FIELD_TYPE_UDF_ARGLIST       32
#define AS_MSG_FIELD_TYPE_UDF_OP            33

// More generic.
#define AS_MSG_FIELD_TYPE_QUERY_BINLIST     40
#define AS_MSG_FIELD_TYPE_BATCH             41
#define AS_MSG_FIELD_TYPE_BATCH_WITH_SET    42
#define AS_MSG_FIELD_TYPE_PREDEXP           43

// Bits in as_transaction.msg_fields indicate which fields are present.
#define AS_MSG_FIELD_BIT_NAMESPACE          (1 << 0)
#define AS_MSG_FIELD_BIT_SET                (1 << 1)
#define AS_MSG_FIELD_BIT_KEY                (1 << 2)
#define AS_MSG_FIELD_BIT_DIGEST_RIPE        (1 << 3)
#define AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY  (1 << 4) // old batch - unsupported
#define AS_MSG_FIELD_BIT_TRID               (1 << 5)
#define AS_MSG_FIELD_BIT_SCAN_OPTIONS       (1 << 6) // old scan - unsupported
#define AS_MSG_FIELD_BIT_SOCKET_TIMEOUT     (1 << 7)
#define AS_MSG_FIELD_BIT_RECS_PER_SEC       (1 << 8)
#define AS_MSG_FIELD_BIT_PID_ARRAY          (1 << 9)
#define AS_MSG_FIELD_BIT_DIGEST_ARRAY       (1 << 10)
#define AS_MSG_FIELD_BIT_SAMPLE_MAX         (1 << 11)
#define AS_MSG_FIELD_BIT_LUT                (1 << 12) // for XDR writes only
#define AS_MSG_FIELD_BIT_INDEX_NAME         (1 << 13)
#define AS_MSG_FIELD_BIT_INDEX_RANGE        (1 << 14)
#define AS_MSG_FIELD_BIT_INDEX_TYPE         (1 << 15)
#define AS_MSG_FIELD_BIT_UDF_FILENAME       (1 << 16)
#define AS_MSG_FIELD_BIT_UDF_FUNCTION       (1 << 17)
#define AS_MSG_FIELD_BIT_UDF_ARGLIST        (1 << 18)
#define AS_MSG_FIELD_BIT_UDF_OP             (1 << 19)
#define AS_MSG_FIELD_BIT_QUERY_BINLIST      (1 << 20)
#define AS_MSG_FIELD_BIT_BATCH              (1 << 21)
#define AS_MSG_FIELD_BIT_BATCH_WITH_SET     (1 << 22)
#define AS_MSG_FIELD_BIT_PREDEXP            (1 << 23)

//------------------------------------------------
// as_msg_op.
//

typedef struct as_msg_op_s {
	uint32_t op_sz; // includes everything past this
	uint8_t op;
	uint8_t particle_type;
	uint8_t has_lut: 1;
	uint8_t unused_flags: 7;
	uint8_t name_sz;
	uint8_t name[0];
	// Note - optional metadata (lut) and op value follows name.
} __attribute__((__packed__)) as_msg_op;

#define OP_FIXED_SZ (offsetof(as_msg_op, name) - offsetof(as_msg_op, op))

#define AS_MSG_OP_READ          1
#define AS_MSG_OP_WRITE         2
#define AS_MSG_OP_CDT_READ      3 // CDT top-level op
#define AS_MSG_OP_CDT_MODIFY    4 // CDT top-level op
#define AS_MSG_OP_INCR          5 // arithmetic add - only for integers
	// 6 is unused.
#define AS_MSG_OP_EXP_READ      7
#define AS_MSG_OP_EXP_MODIFY    8
#define AS_MSG_OP_APPEND        9 // append to strings and blobs
#define AS_MSG_OP_PREPEND       10 // prepend to strings and blobs
#define AS_MSG_OP_TOUCH         11 // will increment the generation
#define AS_MSG_OP_BITS_READ     12 // blob bits top-level op
#define AS_MSG_OP_BITS_MODIFY   13 // blob bits top-level op
#define AS_MSG_OP_DELETE_ALL    14 // used without bin name
#define AS_MSG_OP_HLL_READ      15 // HLL top-level op
#define AS_MSG_OP_HLL_MODIFY    16 // HLL top-level op

//------------------------------------------------
// UDF ops.
//

// These values correspond to client protocol values - do not change them!
typedef enum {
	AS_UDF_OP_KVS           = 0,
	AS_UDF_OP_AGGREGATE     = 1,
	AS_UDF_OP_BACKGROUND    = 2
} as_udf_op;

//------------------------------------------------
// Blob bitwise ops.
//

typedef enum {
	AS_BITS_MODIFY_OP_START = 0,

	AS_BITS_OP_RESIZE       = AS_BITS_MODIFY_OP_START,
	AS_BITS_OP_INSERT       = 1,
	AS_BITS_OP_REMOVE       = 2,

	AS_BITS_OP_SET          = 3,
	AS_BITS_OP_OR           = 4,
	AS_BITS_OP_XOR          = 5,
	AS_BITS_OP_AND          = 6,
	AS_BITS_OP_NOT          = 7,
	AS_BITS_OP_LSHIFT       = 8,
	AS_BITS_OP_RSHIFT       = 9,
	AS_BITS_OP_ADD          = 10,
	AS_BITS_OP_SUBTRACT     = 11,
	AS_BITS_OP_SET_INT      = 12,

	AS_BITS_MODIFY_OP_END,

	AS_BITS_READ_OP_START   = 50,

	AS_BITS_OP_GET          = AS_BITS_READ_OP_START,
	AS_BITS_OP_COUNT        = 51,
	AS_BITS_OP_LSCAN        = 52,
	AS_BITS_OP_RSCAN        = 53,
	AS_BITS_OP_GET_INT      = 54,

	AS_BITS_READ_OP_END
} as_bits_op_type;

typedef enum {
	AS_BITS_FLAG_CREATE_ONLY    = 1 << 0,
	AS_BITS_FLAG_UPDATE_ONLY    = 1 << 1,
	AS_BITS_FLAG_NO_FAIL        = 1 << 2,
	AS_BITS_FLAG_PARTIAL        = 1 << 3
} as_bits_flags;

typedef enum {
	AS_BITS_INT_SUBFLAG_SIGNED      = 1 << 0,
	AS_BITS_INT_SUBFLAG_SATURATE    = 1 << 1,
	AS_BITS_INT_SUBFLAG_WRAP        = 1 << 2
} as_bits_int_flags;

typedef enum {
	AS_BITS_SUBFLAG_RESIZE_FROM_FRONT   = 1 << 0,
	AS_BITS_SUBFLAG_RESIZE_GROW_ONLY    = 1 << 1,
	AS_BITS_SUBFLAG_RESIZE_SHRINK_ONLY  = 1 << 2
} as_bits_resize_subflags;

//------------------------------------------------
// HLL ops.
//

typedef enum {
	AS_HLL_MODIFY_OP_START      = 0,

	AS_HLL_OP_INIT              = AS_HLL_MODIFY_OP_START,
	AS_HLL_OP_ADD               = 1,
	AS_HLL_OP_UNION             = 2,
	AS_HLL_OP_UPDATE_COUNT      = 3,
	AS_HLL_OP_FOLD              = 4,

	AS_HLL_MODIFY_OP_END,

	AS_HLL_READ_OP_START        = 50,

	AS_HLL_OP_COUNT             = AS_HLL_READ_OP_START,
	AS_HLL_OP_GET_UNION         = 51,
	AS_HLL_OP_UNION_COUNT       = 52,
	AS_HLL_OP_INTERSECT_COUNT   = 53,
	AS_HLL_OP_SIMILARITY        = 54,
	AS_HLL_OP_DESCRIBE          = 55,
	AS_HLL_OP_MAY_CONTAIN       = 56,

	AS_HLL_READ_OP_END
} as_hll_op_type;

typedef enum {
	AS_HLL_FLAG_CREATE_ONLY = 1 << 0,
	AS_HLL_FLAG_UPDATE_ONLY = 1 << 1,
	AS_HLL_FLAG_NO_FAIL     = 1 << 2,
	AS_HLL_FLAG_ALLOW_FOLD  = 1 << 3
} as_hll_flags;

//------------------------------------------------
// CDT ops.
//

// So we know it can't be (first byte of) msgpack list/map.
#define CDT_MAGIC 0xC0

typedef enum {
	AS_CDT_PARAM_NONE       = 0,
	AS_CDT_PARAM_INDEX      = 1,
	AS_CDT_PARAM_COUNT      = 2,
	AS_CDT_PARAM_PAYLOAD    = 3,
	AS_CDT_PARAM_FLAGS      = 4,
	AS_CDT_PARAM_STORAGE    = 5
} as_cdt_paramtype;

typedef enum {
	RESULT_TYPE_NONE            = 0,
	RESULT_TYPE_INDEX           = 1,
	RESULT_TYPE_REVINDEX        = 2,
	RESULT_TYPE_RANK            = 3,
	RESULT_TYPE_REVRANK         = 4,
	RESULT_TYPE_COUNT           = 5,
	RESULT_TYPE_KEY             = 6,
	RESULT_TYPE_VALUE           = 7,
	RESULT_TYPE_MAP             = 8,
	RESULT_TYPE_INDEX_RANGE     = 9,
	RESULT_TYPE_REVINDEX_RANGE  = 10,
	RESULT_TYPE_RANK_RANGE      = 11,
	RESULT_TYPE_REVRANK_RANGE   = 12
} result_type_t;

typedef enum {
	AS_CDT_OP_FLAG_RESULT_MASK  = 0x0000ffff,
	AS_CDT_OP_FLAG_INVERTED     = 0x00010000
} as_cdt_op_flags;

typedef enum {
	AS_CDT_SORT_ASCENDING       = 0,
	AS_CDT_SORT_DESCENDING      = 1,
	AS_CDT_SORT_DROP_DUPLICATES = 2
} as_cdt_sort_flags;

typedef enum {
	AS_CDT_LIST_MODIFY_DEFAULT  = 0x00,
	AS_CDT_LIST_ADD_UNIQUE      = 0x01,
	AS_CDT_LIST_INSERT_BOUNDED  = 0x02,
	AS_CDT_LIST_NO_FAIL         = 0x04,
	AS_CDT_LIST_DO_PARTIAL      = 0x08
} as_cdt_list_modify_flags;

typedef enum {
	AS_CDT_MAP_MODIFY_DEFAULT   = 0x00,
	AS_CDT_MAP_NO_OVERWRITE     = 0x01,
	AS_CDT_MAP_NO_CREATE        = 0x02,
	AS_CDT_MAP_NO_FAIL          = 0x04,
	AS_CDT_MAP_DO_PARTIAL       = 0x08
} as_cdt_map_modify_flags;

typedef enum {
	AS_CDT_CTX_INDEX = 0,
	AS_CDT_CTX_RANK  = 1,
	AS_CDT_CTX_KEY   = 2,
	AS_CDT_CTX_VALUE = 3,
	AS_CDT_MAX_CTX
} as_cdt_subcontext;

#define AS_CDT_CTX_LIST 0x10
#define AS_CDT_CTX_MAP 0x20

#define AS_CDT_CTX_BASE_MASK 0x0f
#define AS_CDT_CTX_TYPE_MASK 0x3f
#define AS_CDT_CTX_CREATE_MASK 0xc0

#define AS_CDT_CTX_CREATE_LIST_UNORDERED 0x40
#define AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND 0x80
#define AS_CDT_CTX_CREATE_LIST_ORDERED 0xc0

#define AS_CDT_CTX_CREATE_MAP_UNORDERED 0x40
#define AS_CDT_CTX_CREATE_MAP_K_ORDERED 0x80
#define AS_CDT_CTX_CREATE_MAP_KV_ORDERED 0xc0

typedef enum {
	// List operations.

	// Create and flags.
	AS_CDT_OP_LIST_SET_TYPE                         = 0,

	// Modify.
	AS_CDT_OP_LIST_APPEND                           = 1,
	AS_CDT_OP_LIST_APPEND_ITEMS                     = 2,
	AS_CDT_OP_LIST_INSERT                           = 3,
	AS_CDT_OP_LIST_INSERT_ITEMS                     = 4,
	AS_CDT_OP_LIST_POP                              = 5,
	AS_CDT_OP_LIST_POP_RANGE                        = 6,
	AS_CDT_OP_LIST_REMOVE                           = 7,
	AS_CDT_OP_LIST_REMOVE_RANGE                     = 8,
	AS_CDT_OP_LIST_SET                              = 9,
	AS_CDT_OP_LIST_TRIM                             = 10,
	AS_CDT_OP_LIST_CLEAR                            = 11,
	AS_CDT_OP_LIST_INCREMENT                        = 12,
	AS_CDT_OP_LIST_SORT                             = 13,

	// Read.
	AS_CDT_OP_LIST_SIZE                             = 16,
	AS_CDT_OP_LIST_GET                              = 17,
	AS_CDT_OP_LIST_GET_RANGE                        = 18,
	AS_CDT_OP_LIST_GET_BY_INDEX                     = 19,
	AS_CDT_OP_LIST_GET_BY_VALUE                     = 20,
	AS_CDT_OP_LIST_GET_BY_RANK                      = 21,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE                 = 22,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST            = 23,
	AS_CDT_OP_LIST_GET_BY_INDEX_RANGE               = 24,
	AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL            = 25,
	AS_CDT_OP_LIST_GET_BY_RANK_RANGE                = 26,
	AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE      = 27,

	// More modify - remove by.
	AS_CDT_OP_LIST_REMOVE_BY_INDEX                  = 32,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE                  = 33,
	AS_CDT_OP_LIST_REMOVE_BY_RANK                   = 34,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE              = 35,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST         = 36,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE            = 37,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL         = 38,
	AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE             = 39,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE   = 40,

	// Map operations.

	// Create and flags.
	AS_CDT_OP_MAP_SET_TYPE                          = 64,

	// Modify.
	AS_CDT_OP_MAP_ADD                               = 65,
	AS_CDT_OP_MAP_ADD_ITEMS                         = 66,
	AS_CDT_OP_MAP_PUT                               = 67,
	AS_CDT_OP_MAP_PUT_ITEMS                         = 68,
	AS_CDT_OP_MAP_REPLACE                           = 69,
	AS_CDT_OP_MAP_REPLACE_ITEMS                     = 70,
		// 71 is unused.
		// 72 is unused.
	AS_CDT_OP_MAP_INCREMENT                         = 73,
	AS_CDT_OP_MAP_DECREMENT                         = 74,
	AS_CDT_OP_MAP_CLEAR                             = 75,
	AS_CDT_OP_MAP_REMOVE_BY_KEY                     = 76,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX                   = 77,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE                   = 78,
	AS_CDT_OP_MAP_REMOVE_BY_RANK                    = 79,
		// 80 is unused.
	AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST                = 81,
	AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE               = 82,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST              = 83,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL            = 84,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE             = 85,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL          = 86,
	AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE              = 87,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE     = 88,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE    = 89,

	// Read.
	AS_CDT_OP_MAP_SIZE                              = 96,
	AS_CDT_OP_MAP_GET_BY_KEY                        = 97,
	AS_CDT_OP_MAP_GET_BY_INDEX                      = 98,
	AS_CDT_OP_MAP_GET_BY_VALUE                      = 99,
	AS_CDT_OP_MAP_GET_BY_RANK                       = 100,
		// 101 is unused.
	AS_CDT_OP_MAP_GET_ALL_BY_VALUE                  = 102,
	AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL               = 103,
	AS_CDT_OP_MAP_GET_BY_INDEX_RANGE                = 104,
	AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL             = 105,
	AS_CDT_OP_MAP_GET_BY_RANK_RANGE                 = 106,
	AS_CDT_OP_MAP_GET_BY_KEY_LIST                   = 107,
	AS_CDT_OP_MAP_GET_BY_VALUE_LIST                 = 108,
	AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE        = 109,
	AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE       = 110,

	AS_CDT_OP_CONTEXT_EVAL                          = 0xFF
} as_cdt_optype;

//------------------------------------------------
// Expression ops.
//

typedef enum {
	AS_EXP_FLAG_CREATE_ONLY     = 1 << 0,
	AS_EXP_FLAG_UPDATE_ONLY     = 1 << 1,
	AS_EXP_FLAG_ALLOW_DELETE    = 1 << 2,
	AS_EXP_FLAG_POLICY_NO_FAIL  = 1 << 3,
	AS_EXP_FLAG_EVAL_NO_FAIL    = 1 << 4
} as_exp_flags;

//------------------------------------------------
// Query responses.
//

typedef int (*as_netio_finish_cb) (void* udata, int retcode);
typedef int (*as_netio_start_cb) (void* udata, uint32_t seq);

typedef struct as_netio_s {
	as_netio_finish_cb finish_cb;
	as_netio_start_cb start_cb;
	void* data;
	struct as_file_handle_s* fd_h;
	cf_buf_builder* bb;
	uint32_t offset;
	uint32_t seq;
	bool slow;
	bool compress_response;
	as_proto_comp_stat* comp_stat;
	uint64_t start_time;
} as_netio;

#define AS_NETIO_OK         0
#define AS_NETIO_CONTINUE   1
#define AS_NETIO_ERR        2
#define AS_NETIO_IO_ERR     3


//==========================================================
// Public API.
//

void as_proto_swap(as_proto* proto);
void as_msg_swap_header(as_msg* m);
void as_msg_swap_field(as_msg_field* mf);
void as_msg_swap_op(as_msg_op* op);

const uint8_t* as_proto_compress(const uint8_t* original, size_t* sz,
		as_proto_comp_stat* comp_stat);
uint8_t* as_proto_compress_alloc(const uint8_t* original, size_t alloc_sz,
		size_t indent, size_t* sz, as_proto_comp_stat* comp_stat);
uint8_t* as_proto_compress_alloc_xdr(const uint8_t* original, size_t* sz,
		uint32_t level, as_proto_comp_stat* comp_stat);
uint32_t as_proto_uncompress(const as_comp_proto* cproto, as_proto** p_proto);

cl_msg* as_msg_create_internal(const char* ns_name, uint8_t info1,
		uint8_t info2, uint8_t info3, uint32_t record_ttl, uint16_t n_ops,
		uint8_t* ops, size_t ops_sz);

cl_msg* as_msg_make_response_msg(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op** ops, struct as_bin_s** bins,
		uint16_t bin_count, struct as_namespace_s* ns, cl_msg* msgp_in,
		size_t* msg_sz_in, uint64_t trid);
int32_t as_msg_make_response_bufbuilder(cf_buf_builder** bb_r,
		struct as_storage_rd_s* rd, bool no_bin_data, cf_vector* select_bins);
void as_msg_pid_done_bufbuilder(cf_buf_builder** bb_r, uint32_t pid,
		int result);
cl_msg* as_msg_make_val_response(bool success, const as_val* val,
		uint32_t result_code, uint32_t generation, uint32_t void_time,
		uint64_t trid, size_t* p_msg_sz);
void as_msg_make_val_response_bufbuilder(const as_val* val,
		cf_buf_builder** bb_r, uint32_t val_sz, bool);

int as_msg_send_reply(struct as_file_handle_s* fd_h, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op** ops,
		struct as_bin_s** bins, uint16_t bin_count, struct as_namespace_s* ns,
		uint64_t trid);
int as_msg_send_ops_reply(struct as_file_handle_s* fd_h, const cf_dyn_buf* db,
		bool compress, as_proto_comp_stat* comp_stat);
bool as_msg_send_fin(cf_socket* sock, uint32_t result_code);
size_t as_msg_send_fin_timeout(cf_socket* sock, uint32_t result_code,
		int32_t timeout);

void as_netio_init();
int as_netio_send(as_netio* io);

static inline bool
as_proto_is_valid_type(const as_proto* proto)
{
	return proto->type != 0 && proto->type < PROTO_TYPE_LAST_PLUS_1;
}

static inline bool
as_proto_wrapped_is_valid(const as_proto* proto, size_t size)
{
	return proto->version == PROTO_VERSION &&
			proto->type == PROTO_TYPE_AS_MSG && // currently only wrap as_msg
			sizeof(as_proto) + proto->sz == size;
}

static inline uint32_t
as_msg_field_get_value_sz(const as_msg_field* f)
{
	return f->field_sz - 1;
}

static inline as_msg_field*
as_msg_field_get_next(as_msg_field* f)
{
	return (as_msg_field*)(((uint8_t*)f) + sizeof(f->field_sz) + f->field_sz);
}

static inline uint8_t*
as_msg_field_skip(as_msg_field* f)
{
	return f->field_sz == 0 ? NULL : (uint8_t*)as_msg_field_get_next(f);
}

static inline as_msg_field*
as_msg_field_get(const as_msg* msg, uint8_t type)
{
	as_msg_field* f = (as_msg_field*)msg->data;

	for (uint16_t n = 0; n < msg->n_fields; n++) {
		if (f->type == type) {
			return f;
		}

		f = as_msg_field_get_next(f);
	}

	return NULL;
}

static inline uint32_t
as_msg_op_meta_sz(const as_msg_op* op)
{
	return op->has_lut == 1 ? sizeof(uint64_t) : 0;
}

static inline uint64_t
as_msg_op_get_lut(const as_msg_op* op)
{
	return op->has_lut == 1 ? *(uint64_t*)(op->name + op->name_sz) : 0;
}

static inline const uint8_t*
as_msg_op_get_value_p(const as_msg_op* op)
{
	return op->name + op->name_sz + as_msg_op_meta_sz(op);
}

static inline uint32_t
as_msg_op_get_value_sz(const as_msg_op* op)
{
	return op->op_sz - (OP_FIXED_SZ + op->name_sz + as_msg_op_meta_sz(op));
}

static inline as_msg_op*
as_msg_op_get_next(as_msg_op* op)
{
	return (as_msg_op*)(((uint8_t*)op) + sizeof(op->op_sz) + op->op_sz);
}

static inline uint8_t*
as_msg_op_skip(as_msg_op* op)
{
	// At least 4 bytes always follow op_sz.
	return OP_FIXED_SZ + op->name_sz + as_msg_op_meta_sz(op) > op->op_sz ?
			NULL : (uint8_t*)as_msg_op_get_next(op);
}

static inline as_msg_op*
as_msg_op_iterate(const as_msg* msg, as_msg_op* current, uint16_t* n)
{
	// Skip over the fields the first time.
	if (! current) {
		if (msg->n_ops == 0) {
			return 0; // short cut
		}

		as_msg_field* f = (as_msg_field*)msg->data;

		for (uint16_t i = 0; i < msg->n_fields; i++) {
			f = as_msg_field_get_next(f);
		}

		current = (as_msg_op*)f;
		*n = 0;

		return current;
	}

	(*n)++;

	if (*n >= msg->n_ops) {
		return 0;
	}

	return as_msg_op_get_next(current);
}

#define OP_IS_READ(op) ( \
		(op) == AS_MSG_OP_READ || \
		(op) == AS_MSG_OP_CDT_READ || \
		(op) == AS_MSG_OP_BITS_READ || \
		(op) == AS_MSG_OP_HLL_READ || \
		(op) == AS_MSG_OP_EXP_READ \
	)

#define OP_IS_MODIFY(op) ( \
		(op) == AS_MSG_OP_INCR || \
		(op) == AS_MSG_OP_APPEND || \
		(op) == AS_MSG_OP_PREPEND \
	)

#define IS_CDT_LIST_OP(op) ((op) < AS_CDT_OP_MAP_SET_TYPE)
