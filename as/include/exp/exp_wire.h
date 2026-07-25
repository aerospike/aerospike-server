/*
 * exp_wire.h
 *
 * Copyright (C) 2026 Aerospike, Inc.
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
// Expression wire-format opcodes.
//

typedef enum {
	EXP_UNK = 0,

	EXP_CMP_EQ = 1,
	EXP_CMP_NE = 2,
	EXP_CMP_GT = 3,
	EXP_CMP_GE = 4,
	EXP_CMP_LT = 5,
	EXP_CMP_LE = 6,

	EXP_CMP_REGEX = 7,
	EXP_CMP_GEO = 8,
	EXP_IN_LIST = 9,

	EXP_AND = 16,
	EXP_OR = 17,
	EXP_NOT = 18,
	EXP_EXCLUSIVE = 19,

	EXP_ADD = 20,
	EXP_SUB = 21,
	EXP_MUL = 22,
	EXP_DIV = 23,
	EXP_POW = 24,
	EXP_LOG = 25,
	EXP_MOD = 26,
	EXP_ABS = 27,
	EXP_FLOOR = 28,
	EXP_CEIL = 29,

	EXP_TO_INT = 30,
	EXP_TO_FLOAT = 31,

	EXP_INT_AND = 32,
	EXP_INT_OR = 33,
	EXP_INT_XOR = 34,
	EXP_INT_NOT = 35,
	EXP_INT_LSHIFT = 36,
	EXP_INT_RSHIFT = 37,
	EXP_INT_ARSHIFT = 38,
	EXP_INT_COUNT = 39,
	EXP_INT_LSCAN = 40,
	EXP_INT_RSCAN = 41,

	EXP_MIN = 50,
	EXP_MAX = 51,

	EXP_META_DIGEST_MOD = 64,
	EXP_META_DEVICE_SIZE = 65, // deprecated
	EXP_META_LAST_UPDATE = 66,
	EXP_META_SINCE_UPDATE = 67,
	EXP_META_VOID_TIME = 68,
	EXP_META_TTL = 69,
	EXP_META_SET_NAME = 70,
	EXP_META_KEY_EXISTS = 71,
	EXP_META_IS_TOMBSTONE = 72,
	EXP_META_MEMORY_SIZE = 73, // deprecated
	EXP_META_RECORD_SIZE = 74,

	EXP_REC_KEY = 80,
	EXP_BIN = 81,
	EXP_BIN_TYPE = 82,
	EXP_BIN_EXISTS = 83,

	// Unary type conversion, sibling of EXP_TO_INT / EXP_TO_FLOAT (parked at a
	// free wire slot); the toString() op.
	EXP_TO_STRING = 99,

	EXP_RESULT_REMOVE = 100,
	EXP_MAP_KEYS = 101,
	EXP_MAP_VALUES = 102,

	EXP_VAR_BUILTIN = 122,
	EXP_COND = 123,
	EXP_VAR = 124,
	EXP_LET = 125,
	EXP_QUOTE = 126,
	EXP_CALL = 127,

	EXP_AEL_COMPILE = 128,

	EXP_OP_CODE_END, // for wire size, resist this becoming > 128

	// Begin virtual ops - values not on the wire.
	EXP_VOP_VALUE_NIL,
	EXP_VOP_VALUE_BOOL,
	EXP_VOP_VALUE_TRILEAN,
	EXP_VOP_VALUE_INT,
	EXP_VOP_VALUE_FLOAT,
	EXP_VOP_VALUE_STR,
	EXP_VOP_VALUE_BLOB,
	EXP_VOP_VALUE_GEO,
	EXP_VOP_VALUE_HLL,
	EXP_VOP_VALUE_MAP,
	EXP_VOP_VALUE_LIST,
	EXP_VOP_VALUE_MSGPACK,

	EXP_VOP_COND_CASE
} __attribute__((packed)) exp_op_code;

//==========================================================
// Expression result types (wire-format type codes).
//

typedef enum {
	EXP_RTYPE_NIL = 0,
	EXP_RTYPE_TRILEAN = 1,
	EXP_RTYPE_INT = 2,
	EXP_RTYPE_STR = 3,
	EXP_RTYPE_LIST = 4,
	EXP_RTYPE_MAP = 5,
	EXP_RTYPE_BLOB = 6,
	EXP_RTYPE_FLOAT = 7,
	EXP_RTYPE_GEOJSON = 8,
	EXP_RTYPE_HLL = 9,

	EXP_RTYPE_INPUT_END,
	EXP_RTYPE_RESULT_REMOVE, // internal type

	EXP_RTYPE_END
} __attribute__((packed)) exp_rtype;

//==========================================================
// Expression call system types.
//

typedef enum {
	EXP_CALL_CDT = 0,
	EXP_CALL_BITS = 1,
	EXP_CALL_HLL = 2,
	EXP_CALL_STRING = 3,

	EXP_CALL_END, // 1 past the last base stype (validation bound)

	EXP_CALL_FLAG_MODIFY_LOCAL = 0x40
} __attribute__((packed)) exp_call_stype;

//==========================================================
// Expression builtin loop-var selectors (wire-format integer
// parameter to EXP_VAR_BUILTIN).
//

typedef enum {
	AS_EXP_BUILTIN_KEY = 0,
	AS_EXP_BUILTIN_VALUE = 1,
	AS_EXP_BUILTIN_INDEX = 2,

	AS_EXP_BUILTIN_COUNT // sentinel, not on the wire
} __attribute__((packed)) as_exp_builtin;
