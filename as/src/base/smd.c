/*
 * smd.c
 *
 * Copyright (C) 2018 Aerospike, Inc.
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

#include "base/smd.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include "jansson.h"

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_vector.h"

#include "bits.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "shash.h"

#include "base/cfg.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

// These values are used on the wire - don't change them.
typedef enum {
	SMD_MSG_TID,
	SMD_MSG_VERSION,
	SMD_MSG_CLUSTER_KEY,
	SMD_MSG_OP,
	SMD_MSG_MODULE_ID,
	SMD_MSG_UNUSED_5, // used to be SMD_MSG_ACTION
	SMD_MSG_UNUSED_6, // used to be SMD_MSG_MODULE
	SMD_MSG_UNUSED_7, // used to be SMD_MSG_KEY
	SMD_MSG_UNUSED_8, // used to be SMD_MSG_VALUE
	SMD_MSG_UNUSED_9, // used to be SMD_MSG_GEN_ARRAY
	SMD_MSG_TS_ARRAY,
	SMD_MSG_UNUSED_11, // used to be SMD_MSG_MODULE_NAME
	SMD_MSG_UNUSED_12, // used to be SMD_MSG_OPTIONS

	SMD_MSG_VERSION_LIST,
	SMD_MSG_UNUSED_14, // used to be SMD_MSG_MODULE_COUNTS
	SMD_MSG_KEY_LIST,
	SMD_MSG_VALUE_LIST,
	SMD_MSG_GEN_LIST,

	SMD_MSG_SINGLE_KEY,
	SMD_MSG_SINGLE_VALUE,
	SMD_MSG_SINGLE_GENERATION,
	SMD_MSG_SINGLE_TIMESTAMP,

	SMD_MSG_COMMITTED_CL_KEY,

	NUM_SMD_FIELDS
} smd_msg_fields;

static const msg_template smd_mt[] = {
		{ SMD_MSG_TID, M_FT_UINT64 },
		{ SMD_MSG_VERSION, M_FT_UINT32 },
		{ SMD_MSG_CLUSTER_KEY, M_FT_UINT64 },
		{ SMD_MSG_OP, M_FT_UINT32 },
		{ SMD_MSG_MODULE_ID, M_FT_UINT32 },
		{ SMD_MSG_UNUSED_5, M_FT_ARRAY_UINT32 },
		{ SMD_MSG_UNUSED_6, M_FT_ARRAY_STR },
		{ SMD_MSG_UNUSED_7, M_FT_ARRAY_STR },
		{ SMD_MSG_UNUSED_8, M_FT_ARRAY_STR },
		{ SMD_MSG_UNUSED_9, M_FT_ARRAY_UINT32 },
		{ SMD_MSG_TS_ARRAY, M_FT_ARRAY_UINT64 },
		{ SMD_MSG_UNUSED_11, M_FT_STR },
		{ SMD_MSG_UNUSED_12, M_FT_UINT32 },

		{ SMD_MSG_VERSION_LIST, M_FT_MSGPACK },
		{ SMD_MSG_UNUSED_14, M_FT_MSGPACK },
		{ SMD_MSG_KEY_LIST, M_FT_MSGPACK },
		{ SMD_MSG_VALUE_LIST, M_FT_MSGPACK },
		{ SMD_MSG_GEN_LIST, M_FT_MSGPACK },

		{ SMD_MSG_SINGLE_KEY, M_FT_STR },
		{ SMD_MSG_SINGLE_VALUE, M_FT_STR },
		{ SMD_MSG_SINGLE_GENERATION, M_FT_UINT32 },
		{ SMD_MSG_SINGLE_TIMESTAMP, M_FT_UINT64 },

		{ SMD_MSG_COMMITTED_CL_KEY, M_FT_UINT64 }
};

COMPILER_ASSERT(sizeof(smd_mt) / sizeof(msg_template) == NUM_SMD_FIELDS);

#define SMD_MSG_SCRATCH_SIZE 64 // TODO - rethink... could be smaller?

typedef enum {
	// These values are used on the wire - don't change them.
	SMD_OP_SET_TO_PR = 0,
	SMD_OP_REPORT_ALL_VERS_TO_PR,
	SMD_OP_REPORT_VER_TO_PR,
	SMD_OP_FULL_TO_PR,
	SMD_OP_ACK_TO_PR,

	SMD_OP_SET_FROM_PR,
	SMD_OP_REQ_VER_FROM_PR,
	SMD_OP_FULL_FROM_PR,
	SMD_OP_REQ_FULL_FROM_PR,

	SMD_OP_SET_ACK,
	SMD_OP_SET_NACK,

	// Must be last - these are internal ops and don't go on the wire.
	SMD_OP_CLUSTER_CHANGED,
	SMD_OP_START_SET,

	NUM_SMD_OP_TYPES
} smd_op_type;

static const char* const op_type_str[] = {
		[SMD_OP_SET_TO_PR] = "set-to-pr",
		[SMD_OP_REPORT_ALL_VERS_TO_PR] = "report-all-vers-to-pr",
		[SMD_OP_REPORT_VER_TO_PR] = "report-ver-to-pr",
		[SMD_OP_FULL_TO_PR] = "full-to-pr",
		[SMD_OP_ACK_TO_PR] = "ack-to-pr",

		[SMD_OP_SET_FROM_PR] = "set-from-pr",
		[SMD_OP_REQ_VER_FROM_PR] = "req-ver-from-pr",
		[SMD_OP_FULL_FROM_PR] = "full-from-pr",
		[SMD_OP_REQ_FULL_FROM_PR] = "req-full-from-pr",

		[SMD_OP_SET_ACK] = "set-ack",
		[SMD_OP_SET_NACK] = "set-nack",

		[SMD_OP_CLUSTER_CHANGED] = "cluster-changed",
		[SMD_OP_START_SET] = "start-set"
};

COMPILER_ASSERT(sizeof(op_type_str) / sizeof(const char*) == NUM_SMD_OP_TYPES);

typedef enum {
	STATE_PR = 0,
	STATE_NPR,
	STATE_MERGING,
	STATE_DIRTY,
	STATE_CLEAN,
	STATE_SET,

	NUM_SMD_STATES
} smd_state;

static const char* const state_str[] = {
		[STATE_PR] = "pr",
		[STATE_NPR] = "npr",
		[STATE_MERGING] = "merging",
		[STATE_DIRTY] = "dirty",
		[STATE_CLEAN] = "clean",
		[STATE_SET] = "set"
};

COMPILER_ASSERT(sizeof(state_str) / sizeof(const char*) == NUM_SMD_STATES);

typedef struct smd_s {
	uint64_t cl_key;
	uint32_t node_count;
	cf_node succession[AS_CLUSTER_SZ]; // descending order

	cf_queue pending_set_q; // elements are (smd_op*)
	cf_queue event_q; // elements are (smd_op*)

	cf_mutex lock;

	uint32_t set_tid;
	cf_shash* set_h;
} smd;

typedef struct smd_hash_ele_s {
	struct smd_hash_ele_s* next;
	const char* key;
	uint32_t value;
} smd_hash_ele;

#define N_HASH_ROWS 256

typedef struct smd_hash_s {
	smd_hash_ele table[N_HASH_ROWS];
} smd_hash;

typedef struct smd_module_s {
	as_smd_id id;
	const char* name;
	uint64_t cv_key;
	uint64_t cv_tid;

	as_smd_accept_fn accept_cb;
	as_smd_conflict_fn conflict_cb;

	smd_hash db_h; // key is (char*), value is uint32_t
	cf_vector db;

	bool in_use; // EE modules may not be in use

	// For principal.
	uint64_t retry_next_ms;
	uint32_t retry_msg_count;
	msg* retry_msgs[AS_CLUSTER_SZ];

	uint64_t merge_tids[AS_CLUSTER_SZ];
	smd_hash merge_h;
	cf_vector merge;

	smd_state state;

	// For set ack/nack.
	cf_node set_src;
	uint64_t set_key;
	uint64_t set_tid;
} smd_module;

typedef struct smd_op_s {
	smd_op_type type;

	cf_node src;
	uint32_t node_index;

	smd_module* module;

	uint64_t cl_key;
	uint64_t committed_key;

	uint64_t tid;

	cf_vector items;

	// For cluster changed events.
	uint32_t node_count;
	cf_node* succession;

	// For report all versions events.
	uint32_t version_count;
	uint64_t* version_list;

	// For originator of set operations.
	as_smd_set_fn set_cb;
	void* set_udata;
	uint64_t set_timeout;
} smd_op;

typedef struct smd_set_entry_s {
	uint64_t cl_key;

	as_smd_set_fn cb;
	void* udata;
	uint64_t deadline_ms;

	uint64_t retry_next_ms;
	as_smd_item* item;
	smd_module* module;
} smd_set_entry;

typedef struct set_orig_reduce_udata_s {
	int wait_ms;
	uint64_t now_ms;
} set_orig_reduce_udata;

#define NUM_FUTURE_MODULES 10 // hopefully won't add more than this

#define MODULE_NAME_MAX_LEN 10
#define STATE_NAME_MAX_LEN 10

typedef struct smd_module_string_s {
	// To represent "%s:%s:%lx-%lu".
	char s[MODULE_NAME_MAX_LEN + 1 + STATE_NAME_MAX_LEN + 1 + 16 + 1 + 20 + 1];
} smd_module_string;

static const char smd_empty_value[] = "";

#define REPORT_VER_DELAY_US 50000 // 50 milliseconds
#define SMD_RETRY_MS 3000 // 3 seconds

#define DEFAULT_SET_TIMEOUT_MS 2000 // 2 seconds
#define SET_RETRY_MS 100

#define MAX_PATH_LEN 1024


//==========================================================
// Globals.
//

static smd g_smd = { .lock = CF_MUTEX_INIT };

// In alpha order.
static smd_module g_module_table[] = {
		[AS_SMD_MODULE_EVICT] = { .name = "evict" },
		[AS_SMD_MODULE_ROSTER] = { .name = "roster" },
		[AS_SMD_MODULE_SECURITY] = { .name = "security" },
		[AS_SMD_MODULE_SINDEX] = { .name = "sindex" },
		[AS_SMD_MODULE_TRUNCATE] = { .name = "truncate" },
		[AS_SMD_MODULE_UDF] = { .name = "UDF" }
};

COMPILER_ASSERT(sizeof(g_module_table) / sizeof(smd_module) ==
		AS_SMD_NUM_MODULES);


//==========================================================
// Forward declarations.
//

// Callbacks.
static int smd_msg_recv_cb(cf_node node_id, msg* m, void* udata);
static void smd_cluster_changed_cb(const as_exchange_cluster_changed_event* ex_event, void* udata);
static void smd_set_blocking_cb(bool result, void* udata);

// Parse fabric msg.
static bool smd_msg_parse(msg* m, smd_op* op);
static bool smd_msg_parse_items(msg* m, smd_op* op);

// Event loop.
static void* run_smd(void*);
static int pr_try_retransmit(void);
static int set_orig_try_retransmit_or_expire(void);
static int set_orig_reduce_cb(const void* key, void* value, void* udata);
static void smd_event(smd_op* op);

// Events.
static void op_cluster_changed(smd_op* op);
static void op_start_set(smd_op* op);

static void op_set_to_pr(smd_op* op);
static void op_report_all_vers_to_pr(smd_op* op);
static void op_report_ver_to_pr(smd_op* op);
static void op_full_to_pr(smd_op* op);
static void op_ack_to_pr(smd_op* op);

static void op_set_from_pr(smd_op* op);
static void op_req_ver_from_pr(smd_op* op);
static void op_full_from_pr(smd_op* op);
static void op_req_full_from_pr(smd_op* op);
static void op_finish_set(smd_op* op, bool success);

// Pending set queue.
static bool pending_set_q_contains(const smd_op* op);
static int pending_set_q_reduce_cb(void* ptr, void* udata);

// Fabric msg send/reply.
static void send_set_from_pr(smd_module* module, const as_smd_item* item);
static void send_full_from_pr(smd_module* module);
static void send_report_all_ver_to_pr(void);
static void send_report_ver_to_pr(smd_module* module);
static void send_ack_to_pr(smd_op* op);
static void send_set_reply(smd_module* module, bool success);
static void send_set_from_orig(uint32_t set_tid, smd_set_entry* entry);

// Fabric msg retransmit.
static void pr_send_msgs(smd_module* module);
static void pr_set_retry_msg(smd_module* module, msg* m);
static bool pr_mark_reply(smd_op* op, smd_state state);
static void pr_clear_retry_msgs(smd_module* module);

// Call module accept_cb.
static void module_accept_item(smd_module* module, const as_smd_item* item);
static void module_accept_list(smd_module* module, const cf_vector* list);
static void module_accept_startup(smd_module* module);

// Module.
static void module_regen_key2index(smd_module* module);
static void module_append_item(smd_module* module, as_smd_item* item);
static void module_fill_msg(smd_module* module, msg* m);
static void module_merge_list(smd_module* module, cf_vector* list);
static void module_set_npr(smd_module* module, as_smd_item* item);
static bool module_set_pr(smd_module* module, char* key, char* value);
static void module_restore_from_disk(smd_module* module);
static void module_commit_to_disk(smd_module* module);
static void module_set_default_items(smd_module* module, const cf_vector* default_items);

// Hash.
static void smd_hash_init(smd_hash* h);
static void smd_hash_clear(smd_hash* h);
static void smd_hash_put(smd_hash* h, const char* key, uint32_t value);
static bool smd_hash_get(const smd_hash* h, const char* key, uint32_t* value);
static uint32_t smd_hash_get_row_i(const char* key);

// as_smd_item.
static as_smd_item* smd_item_create_copy(const char* key, const char* value, uint64_t ts, uint32_t gen);
static as_smd_item* smd_item_create_handoff(char* key, char* value, uint64_t ts, uint32_t gen);
static bool smd_item_is_less(const as_smd_item* item0, const as_smd_item* item1);
static void smd_item_destroy(as_smd_item* item);

static char* smd_item_value_ndup(uint8_t* value, uint32_t sz);
static char* smd_item_value_dup(const char* value);
static void smd_item_value_destroy(char* value);


//==========================================================
// Inlines & macros.
//

#define JSON_ENFORCE(x) { \
	if ((x) != 0) { \
		cf_crash(AS_SMD, "json alloc error"); \
	} \
}

static inline smd_module*
smd_get_module(as_smd_id id)
{
	cf_assert(id < AS_SMD_NUM_MODULES, AS_SMD, "invalid id %d", id);
	return &g_module_table[id];
}

static inline bool
smd_is_pr(void)
{
	return g_smd.succession[0] == g_config.self_node;
}

static inline void
smd_lock()
{
	cf_mutex_lock(&g_smd.lock);
}

static inline void
smd_unlock()
{
	cf_mutex_unlock(&g_smd.lock);
}

static inline void
smd_set_entry_destroy(smd_set_entry* entry)
{
	if (entry != NULL) {
		smd_item_destroy(entry->item);
		cf_free(entry);
	}
}

#define item_vec_define(_x, _cnt) \
		cf_vector_define(_x, sizeof(as_smd_item*), _cnt, 0);

static inline void
item_vec_init(cf_vector* vec, uint32_t count)
{
	cf_vector_init(vec, sizeof(as_smd_item*), count, 0);
}

static inline const as_smd_item*
item_vec_get_const(const cf_vector* vec, uint32_t i)
{
	return (const as_smd_item*)cf_vector_get_ptr(vec, i);
}

static inline as_smd_item*
item_vec_get(cf_vector* vec, uint32_t i)
{
	return (as_smd_item*)cf_vector_get_ptr(vec, i);
}

static inline void
item_vec_set(cf_vector* vec, uint32_t i, const as_smd_item* item)
{
	cf_vector_set_ptr(vec, i, item);
}

static inline void
item_vec_append(cf_vector* vec, const as_smd_item* item)
{
	cf_vector_append_ptr(vec, item);
}

static inline void
item_vec_disown_items(cf_vector* vec)
{
	cf_vector_clear(vec);
}

static inline void
item_vec_handoff(cf_vector* dst, cf_vector* src)
{
	*dst = *src;
	memset(src, 0, sizeof(cf_vector)); // to zero .count and .vector
}

static inline void
item_vec_replace(cf_vector* vec, uint32_t i, as_smd_item* item)
{
	as_smd_item* old_item = item_vec_get(vec, i);
	char* tmp = item->key;

	item->key = old_item->key; // keep this since it's pointed to by the hash
	old_item->key = tmp; // to be destroyed below in smd_item_destroy()

	item_vec_set(vec, i, item);
	smd_item_destroy(old_item);
}

static inline void
item_vec_destroy(cf_vector* vec)
{
	for (uint32_t i = 0; i < cf_vector_size(vec); i++) {
		smd_item_destroy(item_vec_get(vec, i));
	}

	cf_vector_destroy(vec);
}

static inline smd_op*
smd_op_create(void)
{
	return (smd_op*)cf_calloc(1, sizeof(smd_op));
}

static inline void
smd_op_handoff(smd_op* dst, smd_op* src)
{
	*dst = *src; // includes .items vector
	memset(&src->items, 0, sizeof(src->items)); // to zero .count and .vector
}

static inline void
smd_op_destroy(smd_op* op)
{
	item_vec_destroy(&op->items);
	cf_free(op->succession);
	cf_free(op->version_list);
	cf_free(op);
}

// Use MODULE_AS_STRING() - see below.
static inline smd_module_string
smd_module_as_string(const smd_module* module)
{
	smd_module_string str;

	if (module == NULL) {
		strcpy(str.s, "all");
	}
	else {
		sprintf(str.s, "%s:%s:%lx-%lu", module->name, state_str[module->state],
				module->cv_key, module->cv_tid);
	}

	return str;
}

#define MODULE_AS_STRING(_module) (smd_module_as_string(_module).s)

#define OP_TYPE_AS_STRING(_type) \
	(((0 <= (_type) && (_type) < NUM_SMD_OP_TYPES)) ? \
			op_type_str[_type] : "INVALID_OP_TYPE")

#define OP_TYPE_DETAIL(_type, _format, ...) \
	cf_detail(AS_SMD, "{%s} %s - " _format, MODULE_AS_STRING(module), \
			OP_TYPE_AS_STRING(_type), ##__VA_ARGS__)

#define OP_DETAIL(_format, ...) OP_TYPE_DETAIL(op->type, _format, ##__VA_ARGS__)


//==========================================================
// Public API.
//

void
as_smd_module_load(as_smd_id id, as_smd_accept_fn accept_cb,
		as_smd_conflict_fn conflict_cb, const cf_vector* default_items)
{
	smd_module* module = smd_get_module(id);

	module->accept_cb = accept_cb;
	module->conflict_cb = conflict_cb == NULL ? smd_item_is_less : conflict_cb;

	smd_hash_init(&module->db_h);
	smd_hash_init(&module->merge_h);

	module->id = id;
	module->in_use = true;

	module_restore_from_disk(module);
	module_set_default_items(module, default_items);
	module_accept_startup(module);
}

void
as_smd_start(void)
{
	if (! cf_queue_init(&g_smd.pending_set_q, sizeof(smd_op*), CF_QUEUE_ALLOCSZ,
			true)) {
		cf_crash(AS_SMD, "failed to create set queue");
	}

	if (! cf_queue_init(&g_smd.event_q, sizeof(smd_op*), CF_QUEUE_ALLOCSZ,
			true)) {
		cf_crash(AS_SMD, "failed to create event queue");
	}

	g_smd.set_h = cf_shash_create(cf_shash_fn_u32, sizeof(uint32_t),
			sizeof(smd_set_entry*), 64, 0);

	as_fabric_register_msg_fn(M_TYPE_SMD, smd_mt, sizeof(smd_mt),
			SMD_MSG_SCRATCH_SIZE, smd_msg_recv_cb, NULL);

	as_exchange_register_listener(smd_cluster_changed_cb, NULL);

	cf_thread_create_detached(run_smd, NULL);
}

void
as_smd_set(as_smd_id id, const char* key, const char* value,
		as_smd_set_fn set_cb, void* udata, uint64_t timeout)
{
	smd_op* op = smd_op_create();

	op->type = SMD_OP_START_SET;
	op->src = g_config.self_node;
	op->module = smd_get_module(id);

	op->set_cb = set_cb;
	op->set_udata = udata;
	op->set_timeout = (timeout == 0 ? DEFAULT_SET_TIMEOUT_MS : timeout);

	item_vec_init(&op->items, 1);
	item_vec_append(&op->items, smd_item_create_copy(key, value, 0, 0));

	cf_queue_push(&g_smd.event_q, &op);
}

bool
as_smd_set_blocking(as_smd_id id, const char* key, const char* value,
		uint64_t timeout)
{
	cf_detail(AS_SMD, "{%d} blocking-set start - key %s", id, key);

	cf_queue q;

	cf_queue_init(&q, sizeof(bool), 1, true);
	as_smd_set(id, key, value, smd_set_blocking_cb, &q, timeout);

	bool result;

	cf_queue_pop(&q, &result, CF_QUEUE_FOREVER);
	cf_queue_destroy(&q);

	cf_detail(AS_SMD, "{%d} blocking-set finished - key %s success %s", id, key,
			(result ? "true" : "false"));

	return result;
}

void
as_smd_get_all(as_smd_id id, as_smd_get_all_fn cb, void* udata)
{
	smd_module* module = smd_get_module(id);

	smd_lock();
	cb(&module->db, udata);
	smd_unlock();
}

void
as_smd_get_info(cf_dyn_buf* db)
{
	smd_lock();

	cf_dyn_buf_append_string(db, "smd:");
	cf_dyn_buf_append_string(db, "n_pending_sets=");
	cf_dyn_buf_append_uint32(db, (uint32_t)cf_queue_sz(&g_smd.pending_set_q));
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "n_events=");
	cf_dyn_buf_append_uint32(db, (uint32_t)cf_queue_sz(&g_smd.event_q));
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "n_nodes=");
	cf_dyn_buf_append_uint32(db, g_smd.node_count);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "principal=");
	cf_dyn_buf_append_uint64_x(db, g_smd.succession[0]);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "cluster_key=");
	cf_dyn_buf_append_uint64_x(db, g_smd.cl_key);
	cf_dyn_buf_append_char(db, ';');

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module(i);

		if (! module->in_use) {
			continue;
		}

		cf_dyn_buf_append_string(db, module->name);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, "committed_key=");
		cf_dyn_buf_append_uint64_x(db, module->cv_key);
		cf_dyn_buf_append_char(db, ',');
		cf_dyn_buf_append_string(db, "committed_tid=");
		cf_dyn_buf_append_uint64(db, module->cv_tid);
		cf_dyn_buf_append_char(db, ',');
		cf_dyn_buf_append_string(db, "n_keys=");
		cf_dyn_buf_append_uint32(db, cf_vector_size(&module->db));
		cf_dyn_buf_append_char(db, ',');
		cf_dyn_buf_append_string(db, "state=");
		cf_dyn_buf_append_string(db, state_str[module->state]);
		cf_dyn_buf_append_char(db, ';');
	}

	smd_unlock();
}


//==========================================================
// Local helpers - callbacks.
//

static int
smd_msg_recv_cb(cf_node node_id, msg* m, void* udata)
{
	(void)udata;

	smd_op* op = smd_op_create();

	op->src = node_id;

	if (! smd_msg_parse(m, op)) {
		cf_warning(AS_SMD, "failed to parse msg op_type %d", op->type);
		smd_op_destroy(op);
		as_fabric_msg_put(m);
		return -1;
	}

	as_fabric_msg_put(m);
	cf_queue_push(&g_smd.event_q, &op);

	return 0;
}

static void
smd_cluster_changed_cb(const as_exchange_cluster_changed_event* ex_event,
		void* udata)
{
	(void)udata;

	size_t a_sz = ex_event->cluster_size * sizeof(cf_node);
	cf_node* succession = cf_malloc(a_sz);

	uint32_t* compatibility_ids = as_exchange_compatibility_ids();
	uint32_t n_compatible = 0;

	for (uint32_t n = 0; n < ex_event->cluster_size; n++) {
		if (compatibility_ids[n] >= 1) {
			succession[n_compatible++] = ex_event->succession[n];
		}
	}

	smd_op* op = smd_op_create();

	op->type = SMD_OP_CLUSTER_CHANGED;
	op->cl_key = ex_event->cluster_key;

	op->node_count = n_compatible;
	op->succession = succession;

	cf_queue_push(&g_smd.event_q, &op);
}

static void
smd_set_blocking_cb(bool result, void* udata)
{
	cf_queue_push((cf_queue*)udata, &result);
}


//==========================================================
// Local helpers - parse fabric msg.
//

static bool
smd_msg_parse(msg* m, smd_op* op)
{
	uint32_t version;

	if (msg_get_uint32(m, SMD_MSG_VERSION, &version) == 0) {
		cf_ticker_warning(AS_SMD, "incompatible msg version %u", version);
		return false;
	}

	uint32_t type;

	if (msg_get_uint32(m, SMD_MSG_OP, &type) != 0) {
		cf_warning(AS_SMD, "msg missing op type");
		return false;
	}

	op->type = (smd_op_type)type;

	if (msg_get_uint64(m, SMD_MSG_CLUSTER_KEY, &op->cl_key) != 0) {
		cf_warning(AS_SMD, "msg missing cluster key");
		return false;
	}

	if (op->type == SMD_OP_REPORT_ALL_VERS_TO_PR) {
		uint32_t count = (AS_SMD_NUM_MODULES + NUM_FUTURE_MODULES) * 3;
		uint64_t versions[count];

		if (! msg_msgpack_list_get_uint64_array(m, SMD_MSG_VERSION_LIST,
				versions, &count) || count == 0 || count % 3 != 0) {
			cf_warning(AS_SMD, "msg missing or invalid version list");
			return false;
		}

		op->version_count = count / 3;
		op->version_list = cf_malloc(count * sizeof(uint64_t));
		memcpy(op->version_list, versions, count * sizeof(uint64_t));

		return true;
	}

	uint32_t mod_id;

	if (msg_get_uint32(m, SMD_MSG_MODULE_ID, &mod_id) != 0 ||
			mod_id >= AS_SMD_NUM_MODULES) {
		cf_detail(AS_SMD, "msg missing or unknown module id");
		return false;
	}

	op->module = smd_get_module((as_smd_id)mod_id);

	if (! op->module->in_use) {
		cf_detail(AS_SMD, "module %s not in use", op->module->name);
		return false;
	}

	switch (op->type) {
	// To principal.
	case SMD_OP_SET_TO_PR:
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return smd_msg_parse_items(m, op);
	case SMD_OP_REPORT_VER_TO_PR:
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY,
				&op->committed_key) != 0) {
			cf_warning(AS_SMD, "msg missing committed cluster key");
			return false;
		}
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return true;
	case SMD_OP_FULL_TO_PR:
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY,
				&op->committed_key) != 0) {
			cf_warning(AS_SMD, "msg missing committed cluster key");
			return false;
		}
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return smd_msg_parse_items(m, op);
	case SMD_OP_ACK_TO_PR:
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return true;

	// From principal.
	case SMD_OP_SET_FROM_PR:
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return smd_msg_parse_items(m, op);
	case SMD_OP_REQ_VER_FROM_PR:
		return true;
	case SMD_OP_FULL_FROM_PR:
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY,
				&op->committed_key) != 0) {
			cf_warning(AS_SMD, "msg missing committed cluster key");
			return false;
		}
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return smd_msg_parse_items(m, op);
	case SMD_OP_REQ_FULL_FROM_PR:
		return true;

	case SMD_OP_SET_ACK:
	case SMD_OP_SET_NACK:
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return true;

	default:
		cf_warning(AS_SMD, "invalid type %d", op->type);
		break;
	}

	return false;
}

static bool
smd_msg_parse_items(msg* m, smd_op* op)
{
	char* key;

	if (msg_get_str(m, SMD_MSG_SINGLE_KEY, &key, MSG_GET_DIRECT) == 0) {
		char* value = NULL;
		uint32_t gen = 0;
		uint64_t ts = 0;

		msg_get_str(m, SMD_MSG_SINGLE_VALUE, &value, MSG_GET_DIRECT);
		msg_get_uint32(m, SMD_MSG_SINGLE_GENERATION, &gen);
		msg_get_uint64(m, SMD_MSG_SINGLE_TIMESTAMP, &ts);

		item_vec_init(&op->items, 1);
		item_vec_append(&op->items, smd_item_create_copy(key, value, ts, gen));

		return true;
	}
	// else - multiple items.

	uint32_t count;

	if (! msg_msgpack_list_get_count(m, SMD_MSG_KEY_LIST, &count)) {
		cf_warning(AS_SMD, "msg missing key list");
		return false;
	}

	if (count == 0) {
		item_vec_init(&op->items, 0);
		return true; // empty list - this can happen
	}

	uint32_t check;

	if (! msg_msgpack_list_get_count(m, SMD_MSG_VALUE_LIST, &check) &&
			check != count) {
		cf_warning(AS_SMD, "msg items count mismatch");
		return false;
	}

	if (! msg_get_uint64_array_count(m, SMD_MSG_TS_ARRAY, &check) &&
			check != count) {
		cf_warning(AS_SMD, "msg items count mismatch or missing ts array");
		return false;
	}

	cf_vector_define(key_vec, sizeof(msg_buf_ele), count, 0);
	cf_vector_define(value_vec, sizeof(msg_buf_ele), count, 0);
	uint32_t gen_list[count];

	if (! msg_msgpack_list_get_buf_array_presized(m, SMD_MSG_KEY_LIST,
			&key_vec)) {
		cf_warning(AS_SMD, "msg missing key list");
		return false;
	}

	if (! msg_msgpack_list_get_buf_array_presized(m, SMD_MSG_VALUE_LIST,
			&value_vec)) {
		cf_warning(AS_SMD, "msg missing value list");
		return false;
	}

	if (! msg_msgpack_list_get_uint32_array(m, SMD_MSG_GEN_LIST, gen_list,
			&check) && check != count) {
		cf_warning(AS_SMD, "msg missing gen list");
		return false;
	}

	item_vec_init(&op->items, count);

	for (uint32_t i = 0; i < count; i++) {
		msg_buf_ele* key_p = (msg_buf_ele*)cf_vector_getp(&key_vec, i);
		msg_buf_ele* val_p = (msg_buf_ele*)cf_vector_getp(&value_vec, i);
		uint64_t ts = 0;

		msg_get_uint64_array(m, SMD_MSG_TS_ARRAY, i, &ts);

		item_vec_append(&op->items, smd_item_create_handoff(
				cf_strndup((char*)key_p->ptr, key_p->sz),
				smd_item_value_ndup(val_p->ptr, val_p->sz), ts, gen_list[i]));
	}

	return true;
}


//==========================================================
// Local helpers - event loop.
//

static void*
run_smd(void* udata)
{
	(void)udata;

	while (true) {
		smd_op* op = NULL;
		int wait_ms = set_orig_try_retransmit_or_expire();

		if (smd_is_pr()) {
			int pr_wait_ms = pr_try_retransmit();

			if (pr_wait_ms == INT_MAX) {
				cf_queue_pop(&g_smd.pending_set_q, &op, CF_QUEUE_NOWAIT);
			}
			else if (pr_wait_ms < wait_ms) {
				wait_ms = pr_wait_ms;
			}
		}

		if (op == NULL) {
			cf_queue_pop(&g_smd.event_q, &op,
					wait_ms == INT_MAX ? CF_QUEUE_FOREVER : wait_ms);
		}

		if (op != NULL) {
			smd_lock();
			smd_event(op);
			smd_unlock();

			smd_op_destroy(op);
		}
	}

	return NULL;
}

static int
pr_try_retransmit(void)
{
	uint64_t next_ms = UINT64_MAX;
	uint64_t now_ms = cf_getms();

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (! module->in_use) {
			continue;
		}

		if (module->retry_next_ms != 0 && module->retry_next_ms < now_ms) {
			pr_send_msgs(module);
		}

		if (module->retry_next_ms != 0 && module->retry_next_ms < next_ms) {
			next_ms = module->retry_next_ms;
		}
	}

	return next_ms == UINT64_MAX ? INT_MAX : (int)(next_ms - now_ms);
}

static int
set_orig_try_retransmit_or_expire(void)
{
	set_orig_reduce_udata udata = {
			.wait_ms = INT_MAX,
			.now_ms = cf_getms()
	};

	cf_shash_reduce(g_smd.set_h, set_orig_reduce_cb, &udata);

	return udata.wait_ms;
}

static int
set_orig_reduce_cb(const void* key, void* value, void* udata)
{
	smd_set_entry* entry = *(smd_set_entry**)value;
	set_orig_reduce_udata* p = (set_orig_reduce_udata*)udata;

	if (entry->deadline_ms < p->now_ms) {
		if (entry->cb != NULL) {
			entry->cb(false, entry->udata);
		}

		smd_set_entry_destroy(entry);

		return CF_SHASH_REDUCE_DELETE;
	}

	if (entry->retry_next_ms != 0 && entry->retry_next_ms < p->now_ms) {
		send_set_from_orig(*(uint32_t*)key, entry);
	}

	uint64_t next_ms = entry->deadline_ms;

	if (entry->retry_next_ms != 0 && entry->retry_next_ms < next_ms) {
		next_ms = entry->retry_next_ms;
	}

	int wait_ms = (int)(next_ms - p->now_ms);

	if (wait_ms < p->wait_ms) {
		p->wait_ms = wait_ms;
	}

	return CF_SHASH_OK;
}

static void
smd_event(smd_op* op)
{
	smd_module* module = op->module;

	if (op->type == SMD_OP_CLUSTER_CHANGED) {
		OP_DETAIL("principal %lx -> %lx", g_smd.succession[0],
				op->succession[0]);
		op_cluster_changed(op);
		return;
	}

	OP_DETAIL("source %lx", op->src);

	if (op->type == SMD_OP_START_SET) {
		op_start_set(op);
		return;
	}

	int node_index = index_of_node(g_smd.succession, g_smd.node_count, op->src);

	switch (op->type) {
	case SMD_OP_SET_TO_PR:
	case SMD_OP_SET_ACK:
	case SMD_OP_SET_NACK:
		break; // these don't care about src or cluster key
	default:
		if (node_index < 0 || g_smd.cl_key != op->cl_key) {
			return;
		}
	}

	op->node_index = (uint32_t)node_index;

	switch (op->type) {
	// To principal.
	case SMD_OP_SET_TO_PR:
		op_set_to_pr(op);
		break;
	case SMD_OP_REPORT_ALL_VERS_TO_PR:
		op_report_all_vers_to_pr(op);
		break;
	case SMD_OP_REPORT_VER_TO_PR:
		op_report_ver_to_pr(op);
		break;
	case SMD_OP_FULL_TO_PR:
		op_full_to_pr(op);
		break;
	case SMD_OP_ACK_TO_PR:
		op_ack_to_pr(op);
		break;

	// From principal.
	case SMD_OP_SET_FROM_PR:
		op_set_from_pr(op);
		break;
	case SMD_OP_REQ_VER_FROM_PR:
		op_req_ver_from_pr(op);
		break;
	case SMD_OP_FULL_FROM_PR:
		op_full_from_pr(op);
		break;
	case SMD_OP_REQ_FULL_FROM_PR:
		op_req_full_from_pr(op);
		break;

	case SMD_OP_SET_ACK:
		op_finish_set(op, true);
		break;
	case SMD_OP_SET_NACK:
		op_finish_set(op, false);
		break;

	default:
		cf_ticker_warning(AS_SMD, "invalid op %d", op->type);
		break;
	}
}


//==========================================================
// Local helpers - events.
//

static void
op_cluster_changed(smd_op* op)
{
	cf_assert(op->node_count <= AS_CLUSTER_SZ, AS_SMD, "cluster count invalid %d > %d",
			g_smd.node_count, AS_CLUSTER_SZ);

	bool was_pr = smd_is_pr();

	g_smd.cl_key = op->cl_key;
	g_smd.node_count = op->node_count;
	memcpy(g_smd.succession, op->succession, sizeof(cf_node) * op->node_count);

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (! module->in_use) {
			continue;
		}

		if (was_pr && module->state == STATE_SET) {
			send_set_reply(module, false);
		}

		pr_clear_retry_msgs(module);

		if (g_smd.node_count == 1) {
			OP_DETAIL("single node move to state %s", state_str[STATE_PR]);

			module->state = STATE_PR;
		}
		else if (smd_is_pr()) {
			msg* m = as_fabric_msg_get(M_TYPE_SMD);

			msg_set_uint32(m, SMD_MSG_OP, SMD_OP_REQ_VER_FROM_PR);
			msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);
			msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);

			pr_set_retry_msg(module, m);
			module->retry_next_ms = cf_getms() + SMD_RETRY_MS;

			item_vec_destroy(&module->merge);
			item_vec_init(&module->merge, 16);
			smd_hash_clear(&module->merge_h);

			OP_DETAIL("move to state %s", state_str[STATE_MERGING]);

			module->state = STATE_MERGING;
		}
		else {
			OP_DETAIL("move to state %s", state_str[STATE_NPR]);

			module->state = STATE_NPR;
		}
	}

	if (! smd_is_pr()) {
		usleep(REPORT_VER_DELAY_US); // allow principal time to advance

		send_report_all_ver_to_pr();

		smd_op* pending_op;

		while (cf_queue_pop(&g_smd.pending_set_q, &pending_op,
				CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
			smd_op_destroy(pending_op);
		}
	}
}

static void
op_start_set(smd_op* op)
{
	if (++g_smd.set_tid == 0) {
		g_smd.set_tid = 1;
	}

	as_smd_item* item = item_vec_get(&op->items, 0);
	uint64_t now_ms = cf_getms();

	item_vec_set(&op->items, 0, NULL); // malloc handoff

	smd_set_entry* entry = (smd_set_entry*)cf_malloc(sizeof(smd_set_entry));

	entry->cl_key = g_smd.cl_key;
	entry->cb = op->set_cb;
	entry->udata = op->set_udata;
	entry->deadline_ms = now_ms + op->set_timeout;
	entry->retry_next_ms = 0;
	entry->item = item;
	entry->module = op->module;

	cf_shash_put(g_smd.set_h, &g_smd.set_tid, &entry);

	if (g_smd.node_count == 0) {
		entry->retry_next_ms = now_ms + SET_RETRY_MS;
		return;
	}

	send_set_from_orig(g_smd.set_tid, entry);
}

static void
op_set_to_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (cf_vector_size(&op->items) != 1) {
		cf_warning(AS_SMD, "bad msg item count %u", cf_vector_size(&op->items));
		return;
	}

	if (smd_is_pr() && module->state != STATE_PR) {
		if (op->tid == module->set_tid && op->src == module->set_src &&
				op->cl_key == module->set_key) {
			OP_DETAIL("already setting - ignoring");
			return;
		}

		if (pending_set_q_contains(op)) {
			OP_DETAIL("already pending - ignoring");
			return;
		}

		OP_DETAIL("move to pending");

		smd_op* new_op = smd_op_create();

		smd_op_handoff(new_op, op);
		cf_queue_push(&g_smd.pending_set_q, &new_op);
		return;
	}

	module->set_src = op->src;
	module->set_key = op->cl_key;
	module->set_tid = op->tid;

	if (! smd_is_pr()) {
		OP_DETAIL("not principal - ignoring");
		send_set_reply(module, false);
		return;
	}

	as_smd_item* item = item_vec_get(&op->items, 0);

	if (module_set_pr(module, item->key, item->value)) { // malloc handoff
		smd_state next_state = g_smd.node_count == 1 ? STATE_PR : STATE_SET;

		OP_DETAIL("new-key %s move to state %s", item->key,
				state_str[next_state]);

		module->state = next_state;

		if (g_smd.node_count == 1) {
			send_set_reply(module, true);
		}
	}
	else {
		send_set_reply(module, true);
	}

	item->key = NULL;
	item->value = NULL;
}

static void
op_report_all_vers_to_pr(smd_op* op)
{
	uint32_t count = op->version_count * 3;
	uint32_t ix = 0;

	while (ix < count) {
		uint64_t module_id = op->version_list[ix++];
		uint64_t cv_key = op->version_list[ix++];
		uint64_t cv_tid = op->version_list[ix++];

		if (module_id >= AS_SMD_NUM_MODULES) {
			cf_detail(AS_SMD, "unknown module %ld", module_id);
			continue;
		}

		smd_module* module = smd_get_module((as_smd_id)module_id);

		if (! module->in_use) {
			cf_detail(AS_SMD, "module %s not in use", module->name);
			continue;
		}

		smd_op module_op = {
				.type = op->type,
				.node_index = op->node_index,
				.module = module,
				.committed_key = cv_key,
				.tid = cv_tid
		};

		op_report_ver_to_pr(&module_op);
	}
}

static void
op_report_ver_to_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (! pr_mark_reply(op, STATE_MERGING)) {
		return;
	}

	bool pr_is_dirty = false;

	if ((op->committed_key == module->cv_key && module->cv_key != 0) ||
			(op->committed_key == 0 && op->tid == 0)) {
		// Note - committed_key is zero on older nodes.
		module->merge_tids[op->node_index] = op->tid;
	}
	else {
		pr_clear_retry_msgs(module);
		pr_is_dirty = true;
	}

	if (module->retry_msg_count != 0) {
		OP_DETAIL("pending replies %u", module->retry_msg_count);
		return;
	}
	// else - got all versions or is dirty.

	bool npr_is_dirty[AS_CLUSTER_SZ] = { false };
	bool npr_has_dirty = false;

	if (! pr_is_dirty) {
		for (uint32_t i = 1; i < g_smd.node_count; i++) {
			uint64_t tid = module->merge_tids[i];

			if (tid < module->cv_tid) {
				npr_is_dirty[i] = true;
				npr_has_dirty = true;
			}
			else if (tid > module->cv_tid) {
				pr_is_dirty = true;
				break;
			}
			// else - both still clean.
		}
	}

	if (pr_is_dirty) {
		OP_DETAIL("move to state %s", state_str[STATE_DIRTY]);

		module->state = STATE_DIRTY;

		msg* m = as_fabric_msg_get(M_TYPE_SMD);

		msg_set_uint32(m, SMD_MSG_OP, SMD_OP_REQ_FULL_FROM_PR);
		msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);

		msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);
		msg_set_uint64(m, SMD_MSG_COMMITTED_CL_KEY, module->cv_key);
		msg_set_uint64(m, SMD_MSG_TID, module->cv_tid);

		pr_set_retry_msg(module, m);
		pr_send_msgs(module);
		return;
	}

	if (! npr_has_dirty) {
		OP_DETAIL("move to state %s", state_str[STATE_PR]);

		module->state = STATE_PR;
		return;
	}

	OP_DETAIL("move to state %s", state_str[STATE_CLEAN]);

	module->state = STATE_CLEAN;

	msg* full = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(full, SMD_MSG_OP, SMD_OP_FULL_FROM_PR);
	module_fill_msg(module, full);

	module->retry_msg_count = 0;
	module->retry_msgs[0] = NULL;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (! npr_is_dirty[i]) {
			module->retry_msgs[i] = NULL;
		}
		else {
			msg_incr_ref(full);
			module->retry_msgs[i] = full;
			module->retry_msg_count++;
		}
	}

	as_fabric_msg_put(full);

	pr_send_msgs(module);
}

static void
op_full_to_pr(smd_op* op)
{
	if (! pr_mark_reply(op, STATE_DIRTY)) {
		return;
	}

	smd_module* module = op->module;

	module_merge_list(module, &op->items);

	if (module->retry_msg_count != 0) {
		OP_DETAIL("pending replies %u", module->retry_msg_count);
		return;
	}

	for (uint32_t i = 0; i < cf_vector_size(&module->merge); i++) {
		as_smd_item* new_item = item_vec_get(&module->merge, i);
		uint32_t ix;

		if (! smd_hash_get(&module->db_h, new_item->key, &ix)) { // new key
			module_append_item(module, new_item);
			continue;
		}
		// else - existing key.

		item_vec_replace(&module->db, ix, new_item);
	}

	if (cf_vector_size(&module->merge) != 0) {
		module_accept_list(module, &module->merge);
		item_vec_disown_items(&module->merge);
	}

	module->cv_tid = 1;
	module->cv_key = g_smd.cl_key;

	module_commit_to_disk(module);
	send_full_from_pr(module);

	OP_DETAIL("n-items %u - move to state %s", cf_vector_size(&module->merge),
			state_str[STATE_CLEAN]);

	module->state = STATE_CLEAN;
}

static void
op_ack_to_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_CLEAN && module->state != STATE_SET) {
		return;
	}

	if (op->tid != module->cv_tid) {
		OP_DETAIL("tid mismatch %lu != %lu", op->tid, module->cv_tid);
		return;
	}

	if (! pr_mark_reply(op, module->state)) {
		return;
	}

	if (module->retry_msg_count != 0) {
		OP_DETAIL("pending replies %u", module->retry_msg_count);
		return;
	}
	// else - got all acks.

	if (module->state == STATE_SET) {
		send_set_reply(module, true);
	}

	OP_DETAIL("move to state %s", state_str[STATE_PR]);

	module->state = STATE_PR;
}

static void
op_set_from_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_NPR) {
		return;
	}

	if (op->node_index != 0) {
		cf_warning(AS_SMD, "set not from principal - src %lx", op->src);
		return;
	}

	if (cf_vector_size(&op->items) != 1) {
		cf_warning(AS_SMD, "set items count %d != 1",
				cf_vector_size(&op->items));
		return;
	}

	module->cv_key = g_smd.cl_key;
	module->cv_tid = op->tid;

	module_set_npr(module, item_vec_get(&op->items, 0));
	item_vec_disown_items(&op->items);

	send_ack_to_pr(op); // last, so item is accepted before originator acks app
}

static void
op_req_ver_from_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_NPR) {
		return;
	}

	send_report_ver_to_pr(module);
}

static void
op_full_from_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_NPR) {
		return;
	}

	if (op->node_index != 0) {
		cf_warning(AS_SMD, "set full not from principal - src %lx", op->src);
		return;
	}

	send_ack_to_pr(op);

	if (op->committed_key == module->cv_key && op->tid == module->cv_tid) {
		return; // normal on retransmits
	}

	module->cv_key = op->committed_key;
	module->cv_tid = op->tid;

	OP_DETAIL("replacing all");

	cf_vector merge_list;
	item_vec_init(&merge_list, cf_vector_size(&op->items));

	for (uint32_t i = 0; i < cf_vector_size(&op->items); i++) {
		as_smd_item* new_item = item_vec_get(&op->items, i);
		uint32_t ix;

		if (! smd_hash_get(&module->db_h, new_item->key, &ix)) {
			item_vec_append(&merge_list, new_item);
			continue;
		}

		const as_smd_item* item = item_vec_get_const(&module->db, ix);

		if (smd_item_is_less(item, new_item)) {
			item_vec_append(&merge_list, new_item);
		}
	}

	module_accept_list(module, &merge_list);
	item_vec_disown_items(&merge_list);
	item_vec_destroy(&merge_list);

	item_vec_destroy(&module->db);
	item_vec_handoff(&module->db, &op->items);

	module_regen_key2index(module);

	module_commit_to_disk(module);
}

static void
op_req_full_from_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_NPR) {
		return;
	}

	OP_DETAIL("sending all");

	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_FULL_TO_PR);
	module_fill_msg(module, m);

	if (as_fabric_send(g_smd.succession[0], m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
op_finish_set(smd_op* op, bool success)
{
	smd_set_entry* entry;
	uint32_t tid = (uint32_t)op->tid;

	if (cf_shash_get(g_smd.set_h, &tid, &entry) != CF_SHASH_OK) {
		cf_detail(AS_SMD, "set-tid %u not in hash", tid);
		return;
	}

	if (op->cl_key != entry->cl_key) {
		cf_warning(AS_SMD, "mismatched cluster key %lx in set", op->cl_key);
		return;
	}

	if (! success) {
		entry->retry_next_ms = cf_getms() + SET_RETRY_MS;
		return;
	}

	int ret = cf_shash_delete(g_smd.set_h, &tid);

	cf_assert(ret == CF_SHASH_OK, AS_SMD, "shash_delete");

	if (entry->cb != NULL) {
		entry->cb(true, entry->udata);
	}

	smd_set_entry_destroy(entry);
}


//==========================================================
// Local helpers - pending set queue.
//

static bool
pending_set_q_contains(const smd_op* op)
{
	cf_queue_reduce(&g_smd.pending_set_q, pending_set_q_reduce_cb, &op);

	return op == NULL; // op is set NULL if it is found
}

static int
pending_set_q_reduce_cb(void* ptr, void* udata)
{
	const smd_op* op_in_q = *(const smd_op**)ptr;
	const smd_op** p_op = (const smd_op**)udata;
	const smd_op* op = *p_op;

	if (op->tid == op_in_q->tid && op->src == op_in_q->src &&
			op->cl_key == op_in_q->cl_key) {
		*p_op = NULL;
		return -1; // found match - stop reduce
	}

	return 0;
}


//==========================================================
// Local helpers - fabric msg send/reply.
//

static void
send_set_from_pr(smd_module* module, const as_smd_item* item)
{
	module->cv_key = g_smd.cl_key;
	module->cv_tid++;

	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_SET_FROM_PR);
	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);
	msg_set_uint64(m, SMD_MSG_TID, module->cv_tid);

	msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);
	msg_set_str(m, SMD_MSG_SINGLE_KEY, item->key, MSG_SET_COPY);

	if (item->value != NULL) {
		msg_set_str(m, SMD_MSG_SINGLE_VALUE, item->value, MSG_SET_COPY);
	}

	msg_set_uint32(m, SMD_MSG_SINGLE_GENERATION, item->generation);
	msg_set_uint64(m, SMD_MSG_SINGLE_TIMESTAMP, item->timestamp);

	pr_set_retry_msg(module, m);
	pr_send_msgs(module);
}

static void
send_full_from_pr(smd_module* module)
{
	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_FULL_FROM_PR);
	module_fill_msg(module, m);

	pr_set_retry_msg(module, m);
	pr_send_msgs(module);
}

static void
send_report_all_ver_to_pr(void)
{
	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_REPORT_ALL_VERS_TO_PR);

	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);

	uint32_t count = 0;
	uint64_t versions[AS_SMD_NUM_MODULES * 3];

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (module->in_use) {
			versions[count++] = (uint64_t)module->id;
			versions[count++] = module->cv_key;
			versions[count++] = module->cv_tid;
		}
	}

	msg_msgpack_list_set_uint64(m, SMD_MSG_VERSION_LIST, versions, count);

	if (as_fabric_send(g_smd.succession[0], m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
send_report_ver_to_pr(smd_module* module)
{
	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_REPORT_VER_TO_PR);
	msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);

	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);

	msg_set_uint64(m, SMD_MSG_COMMITTED_CL_KEY, module->cv_key);
	msg_set_uint64(m, SMD_MSG_TID, module->cv_tid);

	if (as_fabric_send(g_smd.succession[0], m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
send_ack_to_pr(smd_op* op)
{
	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_ACK_TO_PR);
	msg_set_uint32(m, SMD_MSG_MODULE_ID, op->module->id);

	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, op->cl_key);
	msg_set_uint64(m, SMD_MSG_TID, op->tid);

	if (as_fabric_send(g_smd.succession[0], m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
send_set_reply(smd_module* module, bool success)
{
	if (module->set_src == g_config.self_node) {
		smd_op op = {
				.cl_key = module->set_key,
				.tid = module->set_tid,
		};

		op_finish_set(&op, success);
		return;
	}

	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint64(m, SMD_MSG_TID, module->set_tid);
	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, module->set_key);
	msg_set_uint32(m, SMD_MSG_OP, success ? SMD_OP_SET_ACK : SMD_OP_SET_NACK);

	msg_set_uint32(m, SMD_MSG_MODULE_ID, (uint32_t)module->id);

	if (as_fabric_send(module->set_src, m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
send_set_from_orig(uint32_t set_tid, smd_set_entry* entry)
{
	const as_smd_item* item = entry->item;

	if (smd_is_pr()) {
		smd_op op = {
				.type = SMD_OP_SET_TO_PR,
				.src = g_config.self_node,
				.module = entry->module,
				.cl_key = entry->cl_key,
				.tid = set_tid
		};

		item_vec_init(&op.items, 1);
		item_vec_set(&op.items, 0,
				smd_item_create_copy(item->key, item->value, 0, 0));

		op_set_to_pr(&op);
		item_vec_destroy(&op.items);
		entry->retry_next_ms = 0;

		return;
	}

	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint64(m, SMD_MSG_TID, set_tid);
	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, entry->cl_key);
	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_SET_TO_PR);

	msg_set_uint32(m, SMD_MSG_MODULE_ID, (uint32_t)entry->module->id);
	msg_set_str(m, SMD_MSG_SINGLE_KEY, item->key, MSG_SET_COPY);

	if (item->value != NULL) {
		msg_set_str(m, SMD_MSG_SINGLE_VALUE, item->value, MSG_SET_COPY);
	}

	if (as_fabric_send(g_smd.succession[0], m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	entry->retry_next_ms = cf_getms() + SET_RETRY_MS;
}


//==========================================================
// Local helpers - fabric msg retransmit.
//

static void
pr_send_msgs(smd_module* module)
{
	if (module->retry_msg_count == 0) {
		module->retry_next_ms = 0;
		return;
	}

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (module->retry_msgs[i] == NULL) {
			continue;
		}

		msg_incr_ref(module->retry_msgs[i]);

		if (as_fabric_send(g_smd.succession[i], module->retry_msgs[i],
				AS_FABRIC_CHANNEL_META) != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(module->retry_msgs[i]);
		}
	}

	module->retry_next_ms = cf_getms() + SMD_RETRY_MS;
}

static void
pr_set_retry_msg(smd_module* module, msg* m)
{
	module->retry_msgs[0] = NULL;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		msg_incr_ref(m);
		module->retry_msgs[i] = m;
	}

	as_fabric_msg_put(m);
	module->retry_msg_count = g_smd.node_count - 1;
	module->retry_next_ms = 0;
}

// Return false when already marked or state doesn't match.
static bool
pr_mark_reply(smd_op* op, smd_state state)
{
	smd_module* module = op->module;

	if (module->state != state) {
		OP_DETAIL("wrong state %u", state);
		return false;
	}

	if (module->retry_msgs[op->node_index] == NULL) {
		OP_DETAIL("already marked");
		return false; // ignore retransmit
	}

	as_fabric_msg_put(module->retry_msgs[op->node_index]);
	module->retry_msgs[op->node_index] = NULL;
	module->retry_msg_count--;

	if (module->retry_msg_count == 0) {
		module->retry_next_ms = 0;
	}

	return true;
}

static void
pr_clear_retry_msgs(smd_module* module)
{
	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (module->retry_msgs[i] != NULL) {
			as_fabric_msg_put(module->retry_msgs[i]);
			module->retry_msgs[i] = NULL;
		}
	}

	module->retry_msg_count = 0;
	module->retry_next_ms = 0;
}


//==========================================================
// Local helpers - call module accept_cb.
//

static void
module_accept_item(smd_module* module, const as_smd_item* item)
{
	item_vec_define(vec, 1);

	item_vec_append(&vec, item);
	module->accept_cb(&vec, AS_SMD_ACCEPT_OPT_SET);
}

static void
module_accept_list(smd_module* module, const cf_vector* list)
{
	module->accept_cb(list, AS_SMD_ACCEPT_OPT_SET);
}

static void
module_accept_startup(smd_module* module)
{
	uint32_t count = cf_vector_size(&module->db);
	cf_vector vec;

	item_vec_init(&vec, count);

	for (uint32_t i = 0; i < count; i++) {
		const as_smd_item* item = item_vec_get_const(&module->db, i);

		if (item->value != NULL) {
			item_vec_append(&vec, item);
		}
	}

	module->accept_cb(&vec, AS_SMD_ACCEPT_OPT_START);
	item_vec_disown_items(&vec);
	item_vec_destroy(&vec);
}


//==========================================================
// Local helpers - module.
//

static void
module_regen_key2index(smd_module* module)
{
	smd_hash_clear(&module->db_h);

	for (uint32_t i = 0; i < cf_vector_size(&module->db); i++) {
		const char* key = item_vec_get_const(&module->db, i)->key;

		smd_hash_put(&module->db_h, key, i);
	}
}

static void
module_append_item(smd_module* module, as_smd_item* item)
{
	smd_hash_put(&module->db_h, item->key, cf_vector_size(&module->db));
	item_vec_append(&module->db, item);
}

static void
module_fill_msg(smd_module* module, msg* m)
{
	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);

	msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);

	msg_set_uint64(m, SMD_MSG_COMMITTED_CL_KEY, module->cv_key);
	msg_set_uint64(m, SMD_MSG_TID, module->cv_tid);

	uint32_t count = cf_vector_size(&module->db);

	cf_vector_define(key_vec, sizeof(msg_buf_ele), count, 0);
	cf_vector_define(val_vec, sizeof(msg_buf_ele), count, 0);
	uint32_t gen_list[count];

	msg_set_uint64_array_size(m, SMD_MSG_TS_ARRAY, count);

	for (uint32_t i = 0; i < count; i++) {
		const as_smd_item* item = item_vec_get_const(&module->db, i);

		msg_buf_ele key_e = {
				.sz = (uint32_t)strlen(item->key),
				.ptr = (uint8_t*)item->key
		};

		cf_vector_append(&key_vec, &key_e);

		msg_buf_ele val_e = {
				.sz = item->value != NULL ? (uint32_t)strlen(item->value) : 0,
				.ptr = (uint8_t*)item->value
		};

		cf_vector_append(&val_vec, &val_e);

		gen_list[i] = item->generation;
		msg_set_uint64_array(m, SMD_MSG_TS_ARRAY, i, item->timestamp);
	}

	msg_msgpack_list_set_buf(m, SMD_MSG_KEY_LIST, &key_vec);
	msg_msgpack_list_set_buf(m, SMD_MSG_VALUE_LIST, &val_vec);
	msg_msgpack_list_set_uint32(m, SMD_MSG_GEN_LIST, gen_list, count);
}

static void
module_merge_list(smd_module* module, cf_vector* list)
{
	smd_hash* orig_hash = &module->db_h;
	smd_hash* merge_hash = &module->merge_h;

	for (uint32_t i = 0; i < cf_vector_size(list); i++) {
		as_smd_item* new_item = item_vec_get(list, i);
		uint32_t ix;

		if (smd_hash_get(merge_hash, new_item->key, &ix)) {
			const as_smd_item* item = item_vec_get_const(&module->merge, ix);
			bool has_tombstone = new_item->value == NULL || item->value == NULL;
			as_smd_conflict_fn cb = has_tombstone ?
					smd_item_is_less : module->conflict_cb;

			if (! cb(item, new_item)) {
				continue;
			}

			item_vec_replace(&module->merge, ix, new_item);
			item_vec_set(list, i, NULL);
			continue;
		}

		if (smd_hash_get(orig_hash, new_item->key, &ix)) {
			const as_smd_item* item = item_vec_get_const(&module->db, ix);
			bool has_tombstone = new_item->value == NULL || item->value == NULL;
			as_smd_conflict_fn cb = has_tombstone ?
					smd_item_is_less : module->conflict_cb;

			if (! cb(item, new_item)) {
				continue;
			}
		}

		// New merge_hash key.
		smd_hash_put(merge_hash, new_item->key, cf_vector_size(&module->merge));
		item_vec_append(&module->merge, new_item);

		item_vec_set(list, i, NULL);
	}
}

static void
module_set_npr(smd_module* module, as_smd_item* item)
{
	uint32_t ix;

	if (! smd_hash_get(&module->db_h, item->key, &ix)) { // new key
		OP_TYPE_DETAIL(SMD_OP_SET_FROM_PR, "new key %s", item->key);

		module_append_item(module, item);
		module_commit_to_disk(module);
		module_accept_item(module, item);
		return;
	}
	// else - existing key.

	const as_smd_item* old = item_vec_get_const(&module->db, ix);

	if (item->generation != old->generation ||
			item->timestamp != old->timestamp) {
		OP_TYPE_DETAIL(SMD_OP_SET_FROM_PR, "key %s", item->key);

		item_vec_replace(&module->db, ix, item);

		module_commit_to_disk(module);
		module_accept_item(module, item);
		return;
	}

	OP_TYPE_DETAIL(SMD_OP_SET_FROM_PR, "key %s - ignoring unchanged value",
			item->key);

	smd_item_destroy(item);
}

// key and value are malloc handoffs
static bool
module_set_pr(smd_module* module, char* key, char* value)
{
	uint32_t ix;

	if (! smd_hash_get(&module->db_h, key, &ix)) { // new key
		as_smd_item* item = smd_item_create_handoff(key, value,
				cf_clepoch_milliseconds(), 1);

		module_append_item(module, item);
		send_set_from_pr(module, item);
		module_commit_to_disk(module);
		module_accept_item(module, item);
		return true;
	}
	// else - existing key.

	as_smd_item* item = item_vec_get(&module->db, ix);
	bool has_tombstone = item->value == NULL || value == NULL;

	if (! has_tombstone) {
		if (module->conflict_cb != smd_item_is_less) {
			as_smd_item check_item = { .key = key, .value = value };

			if (! module->conflict_cb(item, &check_item)) {
				OP_TYPE_DETAIL(SMD_OP_SET_TO_PR, "key %s - module rejected item",
						key);
				cf_free(key);
				smd_item_value_destroy(value);
				return false;
			}
		}

		if (strcmp(item->value, value) == 0) { // ignore if same value
			OP_TYPE_DETAIL(SMD_OP_SET_TO_PR, "key %s - rejected unchanged item",
					item->key);
			cf_free(key);
			smd_item_value_destroy(value);
			return false;
		}
	}
	else if (item->value == value) { // i.e. both are NULL
		OP_TYPE_DETAIL(SMD_OP_SET_TO_PR, "key %s - rejected unchanged tombstone",
				item->key);
		cf_free(key);
		smd_item_value_destroy(value);
		return false;
	}

	cf_free(key);
	smd_item_value_destroy(item->value);

	item->value = value; // malloc handoff
	item->generation++;
	item->timestamp = cf_clepoch_milliseconds();

	send_set_from_pr(module, item);
	module_commit_to_disk(module);
	module_accept_item(module, item);

	return true;
}

static void
module_restore_from_disk(smd_module* module)
{
	module->cv_key = 0;
	module->cv_tid = 0;

	char smd_path[MAX_PATH_LEN];

	sprintf(smd_path, "%s/smd/%s.smd", g_config.work_directory, module->name);

	struct stat buf;
	int ret = stat(smd_path, &buf);

	if (ret != 0) {
		if (ret == ENOENT) {
			cf_crash(AS_SMD, "failed to read file '%s' module '%s': %s (%d)",
					smd_path, module->name, cf_strerror(errno), errno);
		}

		cf_info(AS_SMD, "no file '%s' - starting empty", smd_path);
		item_vec_init(&module->db, 0);
		return;
	}

	size_t load_flags = JSON_REJECT_DUPLICATES;
	json_error_t json_error;
	json_t* j_file = json_load_file(smd_path, load_flags, &json_error);

	if (j_file == NULL) {
		cf_warning(AS_SMD, "invalid file '%s' - module '%s' with JSON error %s source %s line %d column %d position %d",
				smd_path, module->name, json_error.text, json_error.source,
				json_error.line, json_error.column, json_error.position);
		item_vec_init(&module->db, 0);
		return;
	}

	if (! json_is_array(j_file)) {
		cf_warning(AS_SMD, "invalid file '%s' - starting empty", smd_path);
		json_decref(j_file);
		item_vec_init(&module->db, 0);
		return;
	}

	size_t num_items = json_array_size(j_file);

	if (num_items == 0) {
		json_decref(j_file);
		item_vec_init(&module->db, 0);
		return;
	}

	json_t* j_item = json_array_get(j_file, 0);
	uint32_t start = 0;

	if (json_is_array(j_item)) {
		start = 1;

		json_t* j_ck = json_array_get(j_item, 0);
		json_t* j_tid = json_array_get(j_item, 1);

		if (j_ck != NULL && j_tid != NULL) {
			module->cv_key = (uint64_t)json_integer_value(j_ck);
			module->cv_tid = (uint64_t)json_integer_value(j_tid);
		}
	}
	else {
		module->cv_tid = 1; // key 0 tid 1 means old SMD db with entries > 0
	}

	cf_detail(AS_SMD, "{%s} module_restore_from_disk",
			MODULE_AS_STRING(module));

	item_vec_init(&module->db, (uint32_t)num_items - start);

	for (uint32_t i = start; i < num_items; i++) {
		j_item = json_array_get(j_file, i);
		cf_assert(json_is_object(j_item), AS_SMD, "invalid file '%s'",
				smd_path);

		const char* key = json_string_value(json_object_get(j_item, "key"));
		cf_assert(key != NULL, AS_SMD, "invalid file '%s'", smd_path);

		json_t* j_value = json_object_get(j_item, "value");
		cf_assert(j_value != NULL && (json_is_string(j_value) ||
				json_is_null(j_value)), AS_SMD, "invalid file '%s'", smd_path);

		json_t* j_gen = json_object_get(j_item, "generation");
		cf_assert(j_gen != NULL && json_is_integer(j_gen), AS_SMD, "invalid file '%s'",
				smd_path);

		json_t* j_ts = json_object_get(j_item, "timestamp");
		cf_assert(j_ts != NULL && json_is_integer(j_ts), AS_SMD, "invalid file '%s'",
				smd_path);

		item_vec_set(&module->db, i - start,
				smd_item_create_copy(key, json_string_value(j_value),
						(uint64_t)json_integer_value(j_ts),
						(uint32_t)json_integer_value(j_gen)));
	}

	json_decref(j_file);
	module_regen_key2index(module);
}

static void
module_commit_to_disk(smd_module* module)
{
	json_t* j_file = json_array();
	cf_assert(j_file, AS_SMD, "failed to create json array");

	json_t* j_ver = json_array();
	cf_assert(j_ver, AS_SMD, "failed to create json array");

	JSON_ENFORCE(json_array_append_new(j_ver,
			json_integer((json_int_t)module->cv_key)));
	JSON_ENFORCE(json_array_append_new(j_ver,
			json_integer((json_int_t)module->cv_tid)));

	JSON_ENFORCE(json_array_append_new(j_file, j_ver));

	for (uint32_t i = 0; i < cf_vector_size(&module->db); i++) {
		json_t* j_item = json_object();
		const as_smd_item* item = item_vec_get_const(&module->db, i);

		cf_assert(j_item, AS_SMD, "failed to create json object");

		JSON_ENFORCE(json_object_set_new(j_item, "key",
				json_string(item->key)));

		if (item->value == NULL) {
			JSON_ENFORCE(json_object_set_new(j_item, "value", json_null()));
		}
		else {
			JSON_ENFORCE(json_object_set_new(j_item, "value",
					json_string(item->value)));
		}

		JSON_ENFORCE(json_object_set_new(j_item, "generation",
				json_integer(item->generation)));
		JSON_ENFORCE(json_object_set_new(j_item, "timestamp",
				json_integer((json_int_t)item->timestamp)));

		JSON_ENFORCE(json_array_append_new(j_file, j_item));
	}

	char smd_path[MAX_PATH_LEN];
	char smd_save_path[MAX_PATH_LEN + 5];
	size_t flags = JSON_INDENT(3) | JSON_ENSURE_ASCII | JSON_PRESERVE_ORDER;

	sprintf(smd_path, "%s/smd/%s.smd", g_config.work_directory, module->name);
	sprintf(smd_save_path, "%s.save", smd_path);

	if (json_dump_file(j_file, smd_save_path, flags) != 0) {
		cf_warning(AS_SMD, "failed dump for module '%s' to file '%s': %s (%d)",
				module->name, smd_path, cf_strerror(errno), errno);
		json_decref(j_file);
		return;
	}

	json_decref(j_file);

	if (rename(smd_save_path, smd_path) != 0) {
		cf_warning(AS_SMD, "error on renaming existing file '%s': %s (%d)",
				smd_save_path, cf_strerror(errno), errno);
	}
}

static void
module_set_default_items(smd_module* module, const cf_vector* default_items)
{
	if (default_items == NULL) {
		return;
	}

	for (uint32_t i = 0; i < cf_vector_size(default_items); i++) {
		const as_smd_item* item = item_vec_get_const(default_items, i);

		if (! smd_hash_get(&module->db_h, item->key, NULL)) { // new key
			// Timestamp 0 means this loses to any non-default version.
			module_append_item(module,
					smd_item_create_copy(item->key, item->value, 0, 1));
		}
	}
}


//==========================================================
// Local helpers - hash.
//

static void
smd_hash_init(smd_hash* h)
{
	memset((void*)h->table, 0, sizeof(h->table));
}

static void
smd_hash_clear(smd_hash* h)
{
	for (uint32_t i = 0; i < N_HASH_ROWS; i++) {
		smd_hash_ele* e = h->table[i].next;

		while (e != NULL) {
			smd_hash_ele* t = e->next;
			cf_free(e);
			e = t;
		}
	}

	memset((void*)h->table, 0, sizeof(h->table));
}

static void
smd_hash_put(smd_hash* h, const char* key, uint32_t value)
{
	smd_hash_ele* e_head = &h->table[smd_hash_get_row_i(key)];

	// Nobody in row yet so just set that first element
	if (e_head->key == NULL) {
		e_head->key = key;
		e_head->value = value;
		return;
	}
	// else - allocate new element and insert next to head. Note - boldly
	// assume this key is not already in the hash.

	smd_hash_ele* e = (smd_hash_ele*)cf_malloc(sizeof(smd_hash_ele));

	e->key = key;
	e->value = value;

	e->next = e_head->next;
	e_head->next = e;
}

// Functions as a "has" if called with a null value.
static bool
smd_hash_get(const smd_hash* h, const char* key, uint32_t* value)
{
	const smd_hash_ele* e = &h->table[smd_hash_get_row_i(key)];

	if (e->key == NULL) {
		return false;
	}

	while (e) {
		if (strcmp(e->key, key) == 0) {
			if (value) {
				*value = e->value;
			}

			return true;
		}

		e = e->next;
	}

	return false;
}

static uint32_t
smd_hash_get_row_i(const char* key)
{
	uint64_t hashed_key = cf_hash_fnv32((const uint8_t*)key, strlen(key));

	return (uint32_t)(hashed_key % N_HASH_ROWS);
}


//==========================================================
// Local helpers - as_smd_item.
//

static as_smd_item*
smd_item_create_copy(const char* key, const char* value, uint64_t ts,
		uint32_t gen)
{
	return smd_item_create_handoff(cf_strdup(key), smd_item_value_dup(value),
			ts, gen);
}

static as_smd_item*
smd_item_create_handoff(char* key, char* value, uint64_t ts, uint32_t gen)
{
	as_smd_item* item = cf_malloc(sizeof(as_smd_item));

	item->key = key;
	item->value = value;
	item->timestamp = ts;
	item->generation = gen;

	return item;
}

static bool
smd_item_is_less(const as_smd_item* item0, const as_smd_item* item1)
{
	return item0->timestamp < item1->timestamp ||
			(item0->timestamp == item1->timestamp &&
					item0->generation < item1->generation);
}

static void
smd_item_destroy(as_smd_item* item)
{
	if (item != NULL) {
		cf_free(item->key);
		smd_item_value_destroy(item->value);
		cf_free(item);
	}
}

static char*
smd_item_value_ndup(uint8_t* value, uint32_t sz)
{
	if (value == NULL) {
		return NULL;
	}

	return sz == 0 ?
			(char*)smd_empty_value : cf_strndup((const char*)value, sz);
}

static char*
smd_item_value_dup(const char* value)
{
	if (value == NULL) {
		return NULL;
	}

	return value[0] == '\0' ? (char*)smd_empty_value : cf_strdup(value);
}

static void
smd_item_value_destroy(char* value)
{
	if (value != smd_empty_value) {
		cf_free(value);
	}
}
