/*
 * cdt_wire.h
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

#include <stdint.h>

//==========================================================
// CDT wire-format constants.
//
// Shared by the runtime CDT code paths (cdt.c, particle_list.c,
// particle_map.c) and the AEL compiler (ael_actions.c, ael_codegen.c,
// exp.c). Kept separate from the broader client protocol in proto.h
// so non-CDT consumers don't transitively pull these enums in.
//

// So we know it can't be (first byte of) msgpack list/map.
#define CDT_MAGIC 0xC0

typedef enum {
	AS_CDT_PARAM_NONE = 0,
	AS_CDT_PARAM_INDEX = 1,
	AS_CDT_PARAM_COUNT = 2,
	AS_CDT_PARAM_PAYLOAD = 3,
	AS_CDT_PARAM_FLAGS = 4,
	AS_CDT_PARAM_STORAGE = 5
} as_cdt_paramtype;

typedef enum {
	RESULT_TYPE_NONE = 0,
	RESULT_TYPE_INDEX = 1,
	RESULT_TYPE_REVINDEX = 2,
	RESULT_TYPE_RANK = 3,
	RESULT_TYPE_REVRANK = 4,
	RESULT_TYPE_COUNT = 5,
	RESULT_TYPE_KEY = 6,
	RESULT_TYPE_VALUE = 7,
	RESULT_TYPE_KEY_VALUE_MAP = 8,
	RESULT_TYPE_INDEX_RANGE = 9,
	RESULT_TYPE_REVINDEX_RANGE = 10,
	RESULT_TYPE_RANK_RANGE = 11,
	RESULT_TYPE_REVRANK_RANGE = 12,
	RESULT_TYPE_EXISTS = 13,
	RESULT_TYPE_UNUSED14 = 14,
	RESULT_TYPE_UNUSED15 = 15,
	RESULT_TYPE_UNORDERED_MAP = 16,
	RESULT_TYPE_ORDERED_MAP = 17
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
	AS_CDT_LIST_DO_PARTIAL = 0x08
} as_cdt_list_modify_flags;

typedef enum {
	AS_CDT_MAP_MODIFY_DEFAULT = 0x00,
	AS_CDT_MAP_NO_OVERWRITE = 0x01,
	AS_CDT_MAP_NO_CREATE = 0x02,
	AS_CDT_MAP_NO_FAIL = 0x04,
	AS_CDT_MAP_DO_PARTIAL = 0x08
} as_cdt_map_modify_flags;

typedef enum {
	AS_CDT_CTX_INDEX = 0,
	AS_CDT_CTX_RANK = 1,
	AS_CDT_CTX_KEY = 2,
	AS_CDT_CTX_VALUE = 3,
	AS_CDT_CTX_EXP = 4,
	AS_CDT_CTX_INDEX_RANGE = 8,
	AS_CDT_CTX_RANK_RANGE = 9,
	AS_CDT_CTX_KEY_LIST = 10,
	AS_CDT_CTX_VALUE_LIST = 11,
	AS_CDT_CTX_KEY_INTERVAL = 12,
	AS_CDT_CTX_VALUE_INTERVAL = 13,
	AS_CDT_CTX_KEY_REL_INDEX_RANGE = 14,
	AS_CDT_CTX_VALUE_REL_RANK_RANGE = 15,
	AS_CDT_MAX_CTX
} as_cdt_subcontext;

#define AS_CDT_CTX_LIST 0x10
#define AS_CDT_CTX_MAP 0x20

#define AS_CDT_CTX_BASE_MASK 0x0f
#define AS_CDT_CTX_CDT_TYPE_MASK 0x30
#define AS_CDT_CTX_TYPE_MASK 0x3f
#define AS_CDT_CTX_CREATE_MASK 0x1c0

#define AS_CDT_CTX_CREATE_LIST_UNORDERED 0x40
#define AS_CDT_CTX_CREATE_LIST_UNORDERED_UNBOUND 0x80
#define AS_CDT_CTX_CREATE_LIST_ORDERED 0xc0

#define AS_CDT_CTX_CREATE_MAP_UNORDERED 0x40
#define AS_CDT_CTX_CREATE_MAP_K_ORDERED 0x80
#define AS_CDT_CTX_CREATE_MAP_KV_ORDERED 0xc0

#define AS_CDT_CTX_CREATE_PERSIST_INDEX 0x100
#define AS_CDT_CTX_AND 0x200
#define AS_CDT_CTX_INVERTED 0x400

typedef enum {
	// List operations.

	// Create and flags.
	AS_CDT_OP_LIST_SET_TYPE = 0,

	// Modify.
	AS_CDT_OP_LIST_APPEND = 1,
	AS_CDT_OP_LIST_APPEND_ITEMS = 2,
	AS_CDT_OP_LIST_INSERT = 3,
	AS_CDT_OP_LIST_INSERT_ITEMS = 4,
	AS_CDT_OP_LIST_POP = 5,
	AS_CDT_OP_LIST_POP_RANGE = 6,
	AS_CDT_OP_LIST_REMOVE = 7,
	AS_CDT_OP_LIST_REMOVE_RANGE = 8,
	AS_CDT_OP_LIST_SET = 9,
	AS_CDT_OP_LIST_TRIM = 10,
	AS_CDT_OP_LIST_CLEAR = 11,
	AS_CDT_OP_LIST_INCREMENT = 12,
	AS_CDT_OP_LIST_SORT = 13,

	// Read.
	AS_CDT_OP_LIST_SIZE = 16,
	AS_CDT_OP_LIST_GET = 17,
	AS_CDT_OP_LIST_GET_RANGE = 18,
	AS_CDT_OP_LIST_GET_BY_INDEX = 19,
	AS_CDT_OP_LIST_GET_BY_VALUE = 20,
	AS_CDT_OP_LIST_GET_BY_RANK = 21,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE = 22,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE_LIST = 23,
	AS_CDT_OP_LIST_GET_BY_INDEX_RANGE = 24,
	AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL = 25,
	AS_CDT_OP_LIST_GET_BY_RANK_RANGE = 26,
	AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE = 27,
	AS_CDT_OP_STRING_LIST_JOIN = 28,

	// More modify - remove by.
	AS_CDT_OP_LIST_REMOVE_BY_INDEX = 32,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE = 33,
	AS_CDT_OP_LIST_REMOVE_BY_RANK = 34,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE = 35,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE_LIST = 36,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE = 37,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL = 38,
	AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE = 39,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE = 40,

	// Map operations.

	// Create and flags.
	AS_CDT_OP_MAP_SET_TYPE = 64,

	// Modify.
	AS_CDT_OP_MAP_ADD = 65,
	AS_CDT_OP_MAP_ADD_ITEMS = 66,
	AS_CDT_OP_MAP_PUT = 67,
	AS_CDT_OP_MAP_PUT_ITEMS = 68,
	AS_CDT_OP_MAP_REPLACE = 69,
	AS_CDT_OP_MAP_REPLACE_ITEMS = 70,
	// 71 is unused.
	// 72 is unused.
	AS_CDT_OP_MAP_INCREMENT = 73,
	AS_CDT_OP_MAP_DECREMENT = 74,
	AS_CDT_OP_MAP_CLEAR = 75,
	AS_CDT_OP_MAP_REMOVE_BY_KEY = 76,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX = 77,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE = 78,
	AS_CDT_OP_MAP_REMOVE_BY_RANK = 79,
	// 80 is unused.
	AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST = 81,
	AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE = 82,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST = 83,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL = 84,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE = 85,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL = 86,
	AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE = 87,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE = 88,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE = 89,

	// Read.
	AS_CDT_OP_MAP_SIZE = 96,
	AS_CDT_OP_MAP_GET_BY_KEY = 97,
	AS_CDT_OP_MAP_GET_BY_INDEX = 98,
	AS_CDT_OP_MAP_GET_BY_VALUE = 99,
	AS_CDT_OP_MAP_GET_BY_RANK = 100,
	// 101 is unused.
	AS_CDT_OP_MAP_GET_ALL_BY_VALUE = 102,
	AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL = 103,
	AS_CDT_OP_MAP_GET_BY_INDEX_RANGE = 104,
	AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL = 105,
	AS_CDT_OP_MAP_GET_BY_RANK_RANGE = 106,
	AS_CDT_OP_MAP_GET_BY_KEY_LIST = 107,
	AS_CDT_OP_MAP_GET_BY_VALUE_LIST = 108,
	AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE = 109,
	AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE = 110,

	// Polymorphic — element count of either a list or a map at the
	// navigated position. Errors on a scalar.
	AS_CDT_OP_SIZE = 0xFD,

	AS_CDT_OP_SELECT = 0xFE,
	AS_CDT_OP_CONTEXT_EVAL = 0xFF
} __attribute__((packed)) as_cdt_optype;

// SELECT op flags / return-type encoding (AS_CDT_OP_SELECT, see the CDT Wire
// Protocol spec). The return type occupies the low nibble
// (AS_CDT_SELECT_RTYPE_MASK); policy flags occupy the high nibble
// (AS_CDT_SELECT_FLAG_MASK). Wire-format constants — shared between the
// runtime (cdt.c) and the AEL compiler. Values 0-4 and NO_FAIL=0x10 are
// released (master) and frozen; the rest are reserved for future use and
// marked `placeholder` (not yet produced or accepted by the runtime).
typedef enum {
	// Return type — low nibble.
	AS_CDT_SELECT_TREE = 0, // structural: container shape
	AS_CDT_SELECT_LEAF_LIST = 1, // structural: leaf values
	AS_CDT_SELECT_LEAF_MAP_KEY = 2, // structural: leaf map keys
	AS_CDT_SELECT_LEAF_MAP_KEY_VALUE = 3, // structural: leaf [k,v] pairs
	AS_CDT_SELECT_APPLY = 4, // action: modify matched elements
	AS_CDT_SELECT_RECUR_FIND = 5, // placeholder
	AS_CDT_SELECT_INDEX = 6, // placeholder (ordinal)
	AS_CDT_SELECT_REVINDEX = 7, // placeholder (ordinal)
	AS_CDT_SELECT_RANK = 8, // placeholder (ordinal)
	AS_CDT_SELECT_REVRANK = 9, // placeholder (ordinal)
	AS_CDT_SELECT_COUNT = 10, // aggregate: match count
	AS_CDT_SELECT_EXISTS = 11, // aggregate: any match
	AS_CDT_SELECT_UNORDERED_MAP = 12, // placeholder (structural)
	AS_CDT_SELECT_ORDERED_MAP = 13, // placeholder (structural)
	AS_CDT_SELECT_KEY_VALUE_MAP = 14, // placeholder (structural)
	AS_CDT_SELECT_RTYPE_MASK = 0x0f, // return-type field mask

	// Policy flags — high nibble.
	AS_CDT_SELECT_NO_FAIL = 0x10, // tolerate missing / type errors
	AS_CDT_SELECT_DO_PARTIAL = 0x20, // placeholder
	AS_CDT_SELECT_FLAG_MASK = 0xf0 // policy-flag field mask
} __attribute__((packed)) as_cdt_select_flags;

#define IS_CDT_LIST_OP(op) ((op) < AS_CDT_OP_MAP_SET_TYPE)
