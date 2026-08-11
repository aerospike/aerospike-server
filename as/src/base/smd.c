/*
 * smd.c
 *
 * Copyright (C) 2018-2020 Aerospike, Inc.
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

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "bits.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "jansson.h"
#include "log.h"
#include "msg.h"
#include "node.h"
#include "shash.h"
#include "vector.h"

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
	SMD_MSG_COMPRESSION, // used to be SMD_MSG_OPTIONS

	SMD_MSG_VERSION_LIST,
	SMD_MSG_COMPRESSED_ITEMS, // used to be SMD_MSG_MODULE_COUNTS
	SMD_MSG_KEY_LIST,
	SMD_MSG_VALUE_LIST,
	SMD_MSG_GEN_LIST,

	SMD_MSG_SINGLE_KEY,
	SMD_MSG_SINGLE_VALUE,
	SMD_MSG_SINGLE_GENERATION,
	SMD_MSG_SINGLE_TIMESTAMP,

	SMD_MSG_COMMITTED_CL_KEY,

	// Marks a FULL_FROM_PR as a cv_key/cv_tid advance only - receiver keeps
	// its (already current) items. Absent on a genuine full replace. Old nodes
	// skip unknown field ids on parse, but must never receive a message that
	// relies on this field - see smd_mixed_cluster() gating at the senders.
	SMD_MSG_CV_KEY_ONLY,

	NUM_SMD_FIELDS
} smd_msg_fields;

// clang-format off
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
		{ SMD_MSG_COMPRESSION, M_FT_UINT32 },

		{ SMD_MSG_VERSION_LIST, M_FT_MSGPACK },
		{ SMD_MSG_COMPRESSED_ITEMS, M_FT_BUF },
		{ SMD_MSG_KEY_LIST, M_FT_MSGPACK },
		{ SMD_MSG_VALUE_LIST, M_FT_MSGPACK },
		{ SMD_MSG_GEN_LIST, M_FT_MSGPACK },

		{ SMD_MSG_SINGLE_KEY, M_FT_STR },
		{ SMD_MSG_SINGLE_VALUE, M_FT_STR },
		{ SMD_MSG_SINGLE_GENERATION, M_FT_UINT32 },
		{ SMD_MSG_SINGLE_TIMESTAMP, M_FT_UINT64 },

		{ SMD_MSG_COMMITTED_CL_KEY, M_FT_UINT64 },

		{ SMD_MSG_CV_KEY_ONLY, M_FT_UINT32 }
};

COMPILER_ASSERT(sizeof(smd_mt) / sizeof(msg_template) == NUM_SMD_FIELDS);
// clang-format on

#define SMD_MSG_SCRATCH_SIZE 64 // TODO - rethink... could be smaller?
// Minimum cluster compatibility id at which compressed SMD full-sync messages
// may exist. Gates both the send side (smd_can_compress_full()) and the receive
// side (smd_msg_parse_items_compressed()). Distinct from the literal 16 in
// smd_mixed_cluster(), which means "has the SERVER-209 clean-path protocol" and
// must not move - see the reasoning on 17 in exchange.h.
#define SMD_FULL_ZSTD_COMPATIBILITY_ID 17

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

static const char* const op_type_str[] = { [SMD_OP_SET_TO_PR] = "set-to-pr",
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
	[SMD_OP_START_SET] = "start-set" };

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

typedef enum {
	SMD_COMPRESSION_NONE = 0,
	SMD_COMPRESSION_ZSTD = 1
} smd_compression_mode;

static const char* const state_str[] = { [STATE_PR] = "pr",
	[STATE_NPR] = "npr",
	[STATE_MERGING] = "merging",
	[STATE_DIRTY] = "dirty",
	[STATE_CLEAN] = "clean",
	[STATE_SET] = "set" };

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

	uint64_t compression_attempts;
	uint64_t compression_hits;
	uint64_t compression_bytes_saved;
	uint64_t compression_fallbacks;
} smd;

typedef struct smd_hash_ele_s {
	struct smd_hash_ele_s* next;
	const char* key;
	uint32_t value;
} smd_hash_ele;

#define SMD_HASH_INLINE_ROWS 256
#define SMD_HASH_BIG_ROWS 262144 // 2^18 - used once a module grows large
#define SMD_HASH_BIG_ROWS_THRESHOLD (SMD_HASH_INLINE_ROWS * 4)

// Hash table rows must be a power of two (see smd_hash_get_row_i()).
COMPILER_ASSERT((SMD_HASH_INLINE_ROWS & (SMD_HASH_INLINE_ROWS - 1)) == 0);
COMPILER_ASSERT((SMD_HASH_BIG_ROWS & (SMD_HASH_BIG_ROWS - 1)) == 0);

typedef struct smd_hash_s {
	smd_hash_ele table[SMD_HASH_INLINE_ROWS]; // always present, zero heap
	smd_hash_ele* big_table; // NULL: use table[] with n_rows rows
	uint32_t n_rows;
} smd_hash;

typedef struct smd_module_s {
	as_smd_id id;
	const char* name;

	// Committed version key/tid - the merge-protocol token identifying the
	// content this node last merge-committed to. Moves ONLY at a real merge
	// commit (op_full_to_pr, op_set_from_pr) - never on a clean cluster
	// change or a fail-open inference. This is a pure content-lineage token,
	// not a settle signal - see settle_confirmed below.
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

	// For NPR settle confirmation - true once this node has received an
	// authoritative signal (a full, a cv_key-only confirm, a set, or - in a
	// mixed cluster - fail-open inference) that the principal has spoken for
	// the current cluster key. This is the ONLY settle signal; cv_key is a
	// pure merge-protocol token and must never be used to infer settledness
	// (see smd_all_modules_settled_locked()).
	bool settle_confirmed;
	uint64_t fail_open_confirm_at_ms;

	// For set ack/nack.
	cf_node set_src;
	uint64_t set_key;
	uint64_t set_tid;

	uint64_t next_save_time_sec;
	uint32_t save_throttle_sec;
} smd_module;

typedef struct smd_op_s {
	smd_op_type type;

	cf_node src;
	uint32_t node_index;

	smd_module* module;

	uint64_t cl_key;
	uint64_t committed_key;

	uint64_t tid;
	bool cv_key_only; // FULL_FROM_PR only - see SMD_MSG_CV_KEY_ONLY

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

// Monotonic latch — set once when all in-use modules first reach settled state.
static bool g_smd_initial_sync_done = false;
static cf_mutex g_smd_sync_lock = CF_MUTEX_INIT;
static cf_condition g_smd_sync_cond = CF_CONDITION_INIT;

// In alpha order.
static smd_module g_module_table[] = { [AS_SMD_MODULE_EVICT] = { .name = "evict" },
	[AS_SMD_MODULE_ROSTER] = { .name = "roster" },
	[AS_SMD_MODULE_SECURITY] = { .name = "security" },
	[AS_SMD_MODULE_SINDEX] = { .name = "sindex" },
	[AS_SMD_MODULE_TRUNCATE] = { .name = "truncate" },
	[AS_SMD_MODULE_UDF] = { .name = "UDF" },
	[AS_SMD_MODULE_XDR] = { .name = "XDR", .save_throttle_sec = 30 },
	[AS_SMD_MODULE_MASKING] = { .name = "masking" } };

COMPILER_ASSERT(sizeof(g_module_table) / sizeof(smd_module) == AS_SMD_NUM_MODULES);

//==========================================================
// Forward declarations.
//

// SMD initial sync.
static bool smd_all_modules_settled_locked(void);
static void smd_maybe_set_initial_sync_done(void);
static bool smd_mixed_cluster(void);
static void npr_try_mixed_fail_open_confirm(void);
static int npr_mixed_fail_open_wait_ms(void);

// Callbacks.
static int smd_msg_recv_cb(cf_node node_id, msg* m, void* udata);
static void smd_cluster_changed_cb(const as_exchange_cluster_changed_event* ex_event,
		void* udata);
static void smd_set_blocking_cb(bool result, void* udata);

// Parse fabric msg.
static bool smd_msg_parse(msg* m, smd_op* op);
static bool smd_msg_parse_items(msg* m, smd_op* op);
static bool smd_msg_parse_items_compressed(msg* m, smd_op* op);

// Event loop.
static void* run_smd(void* udata);
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
static void send_full_from_pr(smd_module* module, uint32_t full_source_ix);
static msg* pr_make_cv_key_only_msg(smd_module* module);
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
static bool module_fill_msg_compressed(smd_module* module, msg* m);
static void module_regen_key2index(smd_module* module);
static void module_append_item(smd_module* module, as_smd_item* item);
static void module_fill_msg(smd_module* module, msg* m);
static void module_merge_list(smd_module* module, cf_vector* list);
static void module_set_npr(smd_module* module, as_smd_item* item);
static const as_smd_item* module_set_pr(smd_module* module, char* key,
		char* value);
static void module_restore_from_disk(smd_module* module);
static void module_commit_to_disk(smd_module* module);
static void module_set_default_items(smd_module* module,
		const cf_vector* default_items);

// Hash.
static void smd_hash_init(smd_hash* h);
static void smd_hash_clear(smd_hash* h);
static void smd_hash_grow_for(smd_hash* h, uint32_t n_items);
static void smd_hash_put(smd_hash* h, const char* key, uint32_t value);
static bool smd_hash_get(const smd_hash* h, const char* key, uint32_t* value);
static uint32_t smd_hash_get_row_i(const smd_hash* h, const char* key);

// as_smd_item.
static bool smd_can_compress_full(void);
static as_smd_item* smd_item_create_copy(const char* key, const char* value,
		uint64_t ts, uint32_t gen);
static as_smd_item* smd_item_create_handoff(char* key, char* value, uint64_t ts,
		uint32_t gen);
static bool smd_item_is_less(const as_smd_item* item0, const as_smd_item* item1);
static void smd_item_destroy(as_smd_item* item);

static char* smd_item_value_ndup(uint8_t* value, uint32_t sz);
static char* smd_item_value_dup(const char* value);
static void smd_item_value_destroy(char* value);

//==========================================================
// Inlines & macros.
//

#define JSON_ENFORCE(x)                                                        \
	{                                                                          \
		if ((x) != 0) {                                                        \
			cf_crash(AS_SMD, "json alloc error");                              \
		}                                                                      \
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

#define item_vec_define(_x, _cnt)                                              \
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

#define OP_TYPE_AS_STRING(_type)                                               \
	(((0 <= (_type) && (_type) < NUM_SMD_OP_TYPES)) ? op_type_str[_type]       \
													: "INVALID_OP_TYPE")

#define OP_TYPE_DETAIL(_type, _format, ...)                                    \
	cf_detail(AS_SMD, "{%s} %s - " _format, MODULE_AS_STRING(module),          \
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

	if (! cf_queue_init(&g_smd.event_q, sizeof(smd_op*), CF_QUEUE_ALLOCSZ, true)) {
		cf_crash(AS_SMD, "failed to create event queue");
	}

	g_smd.set_h = cf_shash_create(cf_shash_fn_u32, sizeof(uint32_t),
			sizeof(smd_set_entry*), 64, false);

	as_fabric_register_msg_fn(M_TYPE_SMD, smd_mt, sizeof(smd_mt),
			SMD_MSG_SCRATCH_SIZE, smd_msg_recv_cb, NULL);

	as_exchange_register_listener(smd_cluster_changed_cb, NULL);

	cf_thread_create_detached(run_smd, NULL);
}

void
as_smd_shutdown(void)
{
	smd_lock();

	cf_info(AS_SMD, "SMD module shut down");
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
	cf_dyn_buf_append_uint32(db, cf_queue_sz(&g_smd.pending_set_q));
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "n_events=");
	cf_dyn_buf_append_uint32(db, cf_queue_sz(&g_smd.event_q));
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "n_nodes=");
	cf_dyn_buf_append_uint32(db, g_smd.node_count);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "principal=");
	cf_dyn_buf_append_uint64_x(db, g_smd.succession[0]);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "cluster_key=");
	cf_dyn_buf_append_uint64_x(db, g_smd.cl_key);
	cf_dyn_buf_append_char(db, ',');
	uint64_t compression_attempts = as_load_uint64(&g_smd.compression_attempts);
	uint64_t compression_hits = as_load_uint64(&g_smd.compression_hits);

	cf_dyn_buf_append_string(db, "compression_hit_pct=");
	cf_dyn_buf_append_format(db, "%.3f",
			compression_attempts == 0 ? 0.0
									  : (double)compression_hits * 100.0 /
							(double)compression_attempts);
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "compression_bytes_saved=");
	cf_dyn_buf_append_uint64(db, as_load_uint64(&g_smd.compression_bytes_saved));
	cf_dyn_buf_append_char(db, ',');
	cf_dyn_buf_append_string(db, "compression_fallbacks=");
	cf_dyn_buf_append_uint64(db, as_load_uint64(&g_smd.compression_fallbacks));
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
		cf_dyn_buf_append_char(db, ',');
		cf_dyn_buf_append_string(db, "settled=");
		// Same predicate as smd_all_modules_settled_locked()'s per-module check
		// - settle_confirmed (not cv_key) is the settle signal for a NPR.
		cf_dyn_buf_append_string(db,
				module->state == STATE_PR ||
								(module->state == STATE_NPR &&
										module->settle_confirmed)
						? "true"
						: "false");
		cf_dyn_buf_append_char(db, ';');
	}

	smd_unlock();
}

// Info shares this listening port with client transactions, so a node that
// never settles couldn't be diagnosed via asinfo either - a stuck wait would
// otherwise block silently forever with a single log line at the start. Cap
// each wait in slices and log a ticker on every slice that times out, so the
// node stays diagnosable from logs alone.
#define SMD_WAIT_READY_TICKER_MS 10000

void
as_smd_wait_ready(void)
{
	if (as_load_bool_acq(&g_smd_initial_sync_done)) {
		return;
	}

	cf_info(AS_SMD, "waiting for initial SMD sync");

	uint64_t start_us = cf_getus();

	while (! as_load_bool_acq(&g_smd_initial_sync_done)) {
		cf_mutex_lock(&g_smd_sync_lock);

		if (! as_load_bool_acq(&g_smd_initial_sync_done)) {
			cf_condition_wait_timeout(&g_smd_sync_cond, &g_smd_sync_lock,
					SMD_WAIT_READY_TICKER_MS);
		}

		cf_mutex_unlock(&g_smd_sync_lock);

		if (! as_load_bool_acq(&g_smd_initial_sync_done)) {
			cf_warning(AS_SMD,
					"still waiting for initial SMD sync - elapsed %lu us",
					cf_getus() - start_us);
		}
	}

	cf_info(AS_SMD, "initial SMD sync done - elapsed %lu us",
			cf_getus() - start_us);
}

bool
as_smd_settled_for_migration(void)
{
	return as_load_bool_acq(&g_smd_initial_sync_done);
}

//==========================================================
// Local helpers - SMD initial sync.
//

static bool
smd_all_modules_settled_locked(void)
{
	// Before the first cluster-changed event, every module sits zero-init at
	// STATE_PR (0) and g_smd.node_count is 0 - which would otherwise read as
	// a vacuously-settled single-node cluster and latch the gate at boot,
	// before this node has joined anything. node_count is set exactly once
	// per cluster-changed event and is never 0 afterward (even a one-node
	// cluster reports node_count 1), so this is a precise "have we ever
	// processed a real cluster view" guard.
	if (g_smd.node_count == 0) {
		return false;
	}

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (! module->in_use) {
			continue;
		}

		// POLICY: this gate requires every in-use module to settle, including
		// leaving STATE_MERGING. A module that older peers lack (e.g. a module
		// added in a newer release) never leaves STATE_MERGING in a mixed
		// cluster - those peers drop its REQ_VER_FROM_PR for an unknown module
		// id and never report - which would wedge the whole node's client
		// service, converting a historically-tolerated degradation (that one
		// module doesn't sync until the cluster is homogeneous) into an outage.
		// So any new SMD module that older builds lack MUST gate its inclusion
		// here on as_exchange_min_compatibility_id() < <the compat id it ships
		// in>. All current modules predate this gate, so none need it yet.

		if (module->state == STATE_PR) {
			continue;
		}

		// settle_confirmed is the sole settle signal for a NPR - NOT cv_key,
		// which is a pure merge-protocol token (see the comment on cv_key's
		// declaration). Requiring cv_key == g_smd.cl_key here as well would
		// force cv_key to advance on every clean cluster change purely so this
		// check would pass - which is exactly the design mistake that produced
		// the restart-wedge, merge-storm, and crash-window divergence bugs
		// fixed in this round; see the PR discussion for the full analysis.
		if (module->state == STATE_NPR && module->settle_confirmed) {
			continue;
		}

		return false;
	}

	return true;
}

// A pre-SERVER-209 principal never sends anything on the clean merge path
// (it only speaks up when a NPR is dirty), so a NPR can't tell "cluster is
// clean" apart from "principal hasn't gotten to me yet" from protocol
// traffic alone under an old principal. Gate the extra confirmation this
// implies on cluster compatibility so a rolling upgrade can't wedge waiting
// for a signal an old principal will never send.
static bool
smd_mixed_cluster(void)
{
	// 16 is the compat id in which the SERVER-209 clean-path protocol (cv_key
	// advance + cv_key-only FULL_FROM_PR) shipped - a hard literal, per the
	// tree's convention (record.c < 11, partition_balance.c < 15), not the
	// AS_EXCHANGE_COMPATIBILITY_ID macro. Using the macro would re-flag the
	// cluster as "mixed" on every future unrelated bump (e.g. 16+17), needlessly
	// disabling the cv_key-only fast path during upgrades that all understand it.
	return as_exchange_min_compatibility_id() < 16;
}

// Fail-open inference for NPRs in a mixed cluster: if a report to an old
// principal hasn't drawn a FULL_FROM_PR within one principal retry interval,
// infer the module was clean (an old principal would have already responded
// if it had anything - dirty or full - to send) and mark it settle_confirmed.
// This ONLY sets the settle-confirm flag - it must never touch cv_key, which
// stays at its last real merge-commit value (see cv_key's declaration); an
// old principal never advances its own cv_key on the clean path either, so
// leaving it untouched here keeps this node's reported key matching what an
// old principal expects on every subsequent cluster change. A later, delayed
// FULL_FROM_PR still applies normally and simply re-confirms.
static void
npr_try_mixed_fail_open_confirm(void)
{
	if (smd_is_pr() || ! smd_mixed_cluster()) {
		return;
	}

	uint64_t now_ms = cf_getms();

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (! module->in_use || module->state != STATE_NPR ||
				module->fail_open_confirm_at_ms == 0 ||
				module->fail_open_confirm_at_ms > now_ms) {
			continue;
		}

		cf_detail(AS_SMD, "{%s} mixed cluster fail-open settle confirm",
				module->name);

		module->settle_confirmed = true;
		module->fail_open_confirm_at_ms = 0;
	}

	smd_maybe_set_initial_sync_done();
}

static int
npr_mixed_fail_open_wait_ms(void)
{
	if (smd_is_pr() || ! smd_mixed_cluster()) {
		return INT_MAX;
	}

	uint64_t next_ms = UINT64_MAX;
	uint64_t now_ms = cf_getms();

	for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
		smd_module* module = smd_get_module((as_smd_id)i);

		if (! module->in_use || module->state != STATE_NPR ||
				module->fail_open_confirm_at_ms == 0) {
			continue;
		}

		if (module->fail_open_confirm_at_ms <= now_ms) {
			return 0;
		}

		if (module->fail_open_confirm_at_ms < next_ms) {
			next_ms = module->fail_open_confirm_at_ms;
		}
	}

	return next_ms == UINT64_MAX ? INT_MAX : (int)(next_ms - now_ms);
}

static void
smd_maybe_set_initial_sync_done(void)
{
	// Called under g_smd.lock.
	if (as_load_bool_acq(&g_smd_initial_sync_done)) {
		return;
	}

	if (! smd_all_modules_settled_locked()) {
		return;
	}

	cf_info(AS_SMD, "initial SMD sync done for cluster key %lx", g_smd.cl_key);

	cf_mutex_lock(&g_smd_sync_lock);
	as_store_bool_rls(&g_smd_initial_sync_done, true);
	cf_condition_signal(&g_smd_sync_cond);
	cf_mutex_unlock(&g_smd_sync_lock);
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

	smd_op* op = smd_op_create();

	op->type = SMD_OP_CLUSTER_CHANGED;
	op->cl_key = ex_event->cluster_key;

	size_t a_sz = ex_event->cluster_size * sizeof(cf_node);

	op->node_count = ex_event->cluster_size;
	op->succession = cf_malloc(a_sz);
	memcpy(op->succession, ex_event->succession, a_sz);

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
		define_deferred_array(versions, uint64_t, count);

		if (! msg_msgpack_list_get_uint64_array(m, SMD_MSG_VERSION_LIST,
					versions, &count) ||
				count == 0 || count % 3 != 0) {
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
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY, &op->committed_key) != 0) {
			cf_warning(AS_SMD, "msg missing committed cluster key");
			return false;
		}
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		return true;
	case SMD_OP_FULL_TO_PR:
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY, &op->committed_key) != 0) {
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
		if (msg_get_uint64(m, SMD_MSG_COMMITTED_CL_KEY, &op->committed_key) != 0) {
			cf_warning(AS_SMD, "msg missing committed cluster key");
			return false;
		}
		if (msg_get_uint64(m, SMD_MSG_TID, &op->tid) != 0) {
			cf_warning(AS_SMD, "msg missing tid");
			return false;
		}
		{
			uint32_t cv_key_only = 0;

			// Optional - absent on a genuine full replace.
			msg_get_uint32(m, SMD_MSG_CV_KEY_ONLY, &cv_key_only);
			op->cv_key_only = cv_key_only != 0;
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

	// Single-key updates are never compressed - compression applies only to
	// full item-list messages (see module_fill_msg_compressed). The
	// SMD_MSG_COMPRESSION branch below is therefore reached only by the
	// multi-item path.
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

	if (msg_is_set(m, SMD_MSG_COMPRESSION)) {
		return smd_msg_parse_items_compressed(m, op);
	}

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

	cf_vector key_vec;
	cf_vector value_vec;

	cf_vector_init(&key_vec, sizeof(msg_buf_ele), count, 0);
	cf_vector_init(&value_vec, sizeof(msg_buf_ele), count, 0);

	uint32_t* gen_list = cf_malloc(count * sizeof(uint32_t));

	if (! msg_msgpack_list_get_buf_array_presized(m, SMD_MSG_KEY_LIST, &key_vec)) {
		cf_warning(AS_SMD, "msg missing key list");
		cf_vector_destroy(&key_vec);
		cf_vector_destroy(&value_vec);
		cf_free(gen_list);
		return false;
	}

	if (! msg_msgpack_list_get_buf_array_presized(m, SMD_MSG_VALUE_LIST,
				&value_vec)) {
		cf_warning(AS_SMD, "msg missing value list");
		cf_vector_destroy(&key_vec);
		cf_vector_destroy(&value_vec);
		cf_free(gen_list);
		return false;
	}

	if (! msg_msgpack_list_get_uint32_array(m, SMD_MSG_GEN_LIST, gen_list,
				&check) &&
			check != count) {
		cf_warning(AS_SMD, "msg missing gen list");
		cf_vector_destroy(&key_vec);
		cf_vector_destroy(&value_vec);
		cf_free(gen_list);
		return false;
	}

	item_vec_init(&op->items, count);

	for (uint32_t i = 0; i < count; i++) {
		msg_buf_ele* key_p = (msg_buf_ele*)cf_vector_getp(&key_vec, i);
		msg_buf_ele* val_p = (msg_buf_ele*)cf_vector_getp(&value_vec, i);
		uint64_t ts = 0;

		msg_get_uint64_array(m, SMD_MSG_TS_ARRAY, i, &ts);

		item_vec_append(&op->items,
				smd_item_create_handoff(cf_strndup((char*)key_p->ptr, key_p->sz),
						smd_item_value_ndup(val_p->ptr, val_p->sz), ts,
						gen_list[i]));
	}

	cf_vector_destroy(&key_vec);
	cf_vector_destroy(&value_vec);
	cf_free(gen_list);

	return true;
}

// Whether this op may legitimately carry a compressed item list, checked before
// decompressing anything.
//
// smd_msg_parse() runs inline on the fabric META receive thread, so without this
// a peer that has merely completed a fabric handshake - anyone who can reach the
// fabric port, on a cluster without fabric TLS - can make that thread allocate
// and zstd-decode up to SMD_FULL_ZSTD_MAX_DECOMPRESSED_SIZE (128 MiB) for a few
// KB on the wire, times n_fabric_channel_recv_threads[META], for as long as it
// likes. Every real SMD operation (namespace create, UDF register, sindex DDL,
// roster change, security policy) queues behind those threads cluster-wide.
//
// Three cheap checks, none of which needs the item list:
//
//   1. Op type. module_fill_msg_compressed() is reached only from the two
//      full-item-list sends, SMD_OP_FULL_TO_PR and SMD_OP_FULL_FROM_PR, so a
//      compressed list on any other op is malformed by construction. This is
//      also what removes the SMD_OP_SET_TO_PR exposure, which is the dangerous
//      one: SET_TO_PR is deliberately exempt from the membership check in
//      smd_event() ("these don't care about src or cluster key"), so it is the
//      one compressed-capable op with no downstream validation at all.
//
//   2. Compat gate. The send side already requires smd_can_compress_full();
//      applying it here too means a node never decodes a frame the cluster
//      should not be producing, whatever a peer claims.
//
//   3. Membership and cluster key. For FULL_TO_PR / FULL_FROM_PR this is not a
//      new rule - smd_event() applies exactly this check to both ops via its
//      default: branch and drops them on failure. Doing it here only moves an
//      existing drop ahead of the 128 MiB decode, so acceptance semantics are
//      unchanged and no legitimate startup or rejoin flow can be newly refused.
//      op->src is the fabric-level node id, not a peer-declared field.
//
// Check 3 reads g_smd.succession / node_count / cl_key from the receive thread
// while the SMD event thread may be updating them. That is deliberate: this is
// a pre-filter, not the authority. A stale view can only cost a spurious drop
// during a cluster change, which SMD's own retransmit recovers, and the
// authoritative check still runs later on the SMD thread. node_count is bounded
// by the fixed succession array, so a torn read cannot index out of range.
static bool
smd_compressed_items_allowed(const smd_op* op)
{
	if (op->type != SMD_OP_FULL_TO_PR && op->type != SMD_OP_FULL_FROM_PR) {
		cf_warning(AS_SMD, "compressed items on op %s from %lx - refusing",
				OP_TYPE_AS_STRING(op->type), op->src);
		return false;
	}

	if (! smd_can_compress_full()) {
		cf_warning(AS_SMD,
				"compressed items from %lx below compat id %u - refusing",
				op->src, SMD_FULL_ZSTD_COMPATIBILITY_ID);
		return false;
	}

	// Split from the cluster-key check below on purpose - the two mean opposite
	// things to an operator. This one says a peer that is not in our succession
	// list reached the fabric META receive thread, which is the case this
	// pre-filter exists for.
	if (index_of_node(g_smd.succession, g_smd.node_count, op->src) < 0) {
		cf_warning(AS_SMD, "compressed items from non-member %lx - refusing",
				op->src);
		return false;
	}

	// Whereas this one is an expected, benign transient during a cluster
	// change: a stale view costs a spurious drop that SMD's own retransmit
	// recovers. Same refusal, different thing to go look at.
	if (g_smd.cl_key != op->cl_key) {
		cf_warning(AS_SMD,
				"compressed items from %lx with stale key - refusing", op->src);
		return false;
	}

	return true;
}

static bool
smd_msg_parse_items_compressed(msg* m, smd_op* op)
{
	if (! smd_compressed_items_allowed(op)) {
		return false;
	}

	uint32_t compression = SMD_COMPRESSION_NONE;

	if (msg_get_uint32(m, SMD_MSG_COMPRESSION, &compression) != 0) {
		cf_warning(AS_SMD, "msg missing compression mode");
		return false;
	}

	if (compression != SMD_COMPRESSION_ZSTD) {
		cf_warning(AS_SMD, "invalid compression mode %u", compression);
		return false;
	}

	uint8_t* compressed = NULL;
	size_t compressed_sz = 0;

	if (msg_get_buf(m, SMD_MSG_COMPRESSED_ITEMS, &compressed, &compressed_sz,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_SMD, "msg missing compressed items");
		return false;
	}

	if (! as_smd_decompress_items(compressed, compressed_sz, &op->items)) {
		cf_warning(AS_SMD, "failed to decompress full items");
		return false;
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
		smd_lock();

		int wait_ms = set_orig_try_retransmit_or_expire();

		smd_unlock();

		if (smd_is_pr()) {
			int pr_wait_ms = pr_try_retransmit();

			if (pr_wait_ms < wait_ms) {
				wait_ms = pr_wait_ms;
			}

			uint32_t n_pending = cf_queue_sz(&g_smd.pending_set_q);

			for (uint32_t i = 0; i < n_pending; i++) {
				smd_op* pending_op;

				cf_queue_pop(&g_smd.pending_set_q, &pending_op, CF_QUEUE_NOWAIT);

				if (pending_op->module->state == STATE_PR) {
					smd_lock();
					smd_event(pending_op);
					smd_unlock();

					smd_op_destroy(pending_op);
				}
				else {
					cf_queue_push(&g_smd.pending_set_q, &pending_op);
				}
			}
		}
		else {
			// npr_try_mixed_fail_open_confirm() writes the module's settle
			// fields, and as_smd_get_info() reads module state under
			// g_smd.lock - so this must hold the lock too, like every other
			// module-state mutation. (pr_try_retransmit() above is lock-free
			// but read-only.)
			smd_lock();

			npr_try_mixed_fail_open_confirm();

			int npr_wait_ms = npr_mixed_fail_open_wait_ms();

			smd_unlock();

			if (npr_wait_ms < wait_ms) {
				wait_ms = npr_wait_ms;
			}
		}

		smd_op* op;

		if (cf_queue_pop(&g_smd.event_q, &op,
					wait_ms == INT_MAX ? CF_QUEUE_FOREVER : wait_ms) ==
				CF_QUEUE_OK) {
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

		if (module->retry_next_ms != 0 && module->retry_next_ms <= now_ms) {
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
	set_orig_reduce_udata udata = { .wait_ms = INT_MAX, .now_ms = cf_getms() };

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

	if (entry->retry_next_ms != 0 && entry->retry_next_ms <= p->now_ms) {
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
		OP_DETAIL("principal %lx -> %lx", g_smd.succession[0], op->succession[0]);
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
	cf_assert(op->node_count <= AS_CLUSTER_SZ, AS_SMD,
			"cluster count invalid %d > %d", g_smd.node_count, AS_CLUSTER_SZ);

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
			smd_hash_grow_for(&module->merge_h, cf_vector_size(&module->db));

			OP_DETAIL("move to state %s", state_str[STATE_MERGING]);

			module->state = STATE_MERGING;
		}
		else {
			OP_DETAIL("move to state %s", state_str[STATE_NPR]);

			module->state = STATE_NPR;
			module->settle_confirmed = false;
			module->fail_open_confirm_at_ms = 0;
		}
	}

	// Single-node cluster: all modules just moved to STATE_PR — latch done.
	smd_maybe_set_initial_sync_done();

	if (! smd_is_pr()) {
		usleep(REPORT_VER_DELAY_US); // allow principal time to advance

		send_report_all_ver_to_pr();

		if (smd_mixed_cluster()) {
			uint64_t advance_at_ms = cf_getms() + SMD_RETRY_MS;

			for (uint32_t i = 0; i < AS_SMD_NUM_MODULES; i++) {
				smd_module* module = smd_get_module((as_smd_id)i);

				if (module->in_use && module->state == STATE_NPR) {
					module->fail_open_confirm_at_ms = advance_at_ms;
				}
			}
		}

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

	as_smd_item* op_item = item_vec_get(&op->items, 0);
	const as_smd_item* item = module_set_pr(module, op_item->key, op_item->value);

	op_item->key = NULL;
	op_item->value = NULL;

	if (item != NULL) {
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

		smd_op module_op = { .type = op->type,
			.node_index = op->node_index,
			.module = module,
			.committed_key = cv_key,
			.tid = cv_tid };

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

	if (! pr_is_dirty) {
		for (uint32_t i = 1; i < g_smd.node_count; i++) {
			uint64_t tid = module->merge_tids[i];

			if (tid > module->cv_tid) {
				pr_is_dirty = true;
				break;
			}
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

	// cv_key deliberately does NOT advance here - it only ever moves at a real
	// merge commit (op_full_to_pr, op_set_from_pr), matching the pre-SERVER-209
	// invariant that committed keys only change on merges. Settle confirmation
	// for NPRs is carried entirely by settle_confirmed (set when the cv_key-only
	// confirm below, or a real full, is received) - see smd_all_modules_settled_locked().
	// Advancing cv_key here instead was tried and reverted: it forced every
	// restart after a clean change to full-merge (a restarted node reports the
	// pre-departure key, which no longer matches), and needed its own sidecar
	// persistence with its own crash-window divergence hazard.

	OP_DETAIL("move to state %s", state_str[STATE_CLEAN]);

	module->state = STATE_CLEAN;

	// Only NPRs that reported a stale tid actually need the (possibly large)
	// payload - others already match cv_tid and just need the lightweight
	// cv_key-only confirm below so they can mark settle_confirmed.
	// A pre-SERVER-209 NPR doesn't understand that empty message - it just
	// sees an empty payload with a new committed_key and replaces (wipes)
	// its local db - so in a mixed cluster fall back to sending the real
	// payload to every NPR, never the lightweight stand-in.
	bool mixed = smd_mixed_cluster();
	bool need_full = mixed;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (module->merge_tids[i] < module->cv_tid) {
			need_full = true;
			break;
		}
	}

	msg* full = NULL;

	if (need_full) {
		full = as_fabric_msg_get(M_TYPE_SMD);
		msg_set_uint32(full, SMD_MSG_OP, SMD_OP_FULL_FROM_PR);
		module_fill_msg(module, full);
	}

	module->retry_msgs[0] = NULL;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (mixed || module->merge_tids[i] < module->cv_tid) {
			msg_incr_ref(full);
			module->retry_msgs[i] = full;
		}
		else {
			module->retry_msgs[i] = pr_make_cv_key_only_msg(module);
		}
	}

	if (full != NULL) {
		as_fabric_msg_put(full);
	}

	module->retry_msg_count = g_smd.node_count - 1;
	module->retry_next_ms = 0;

	pr_send_msgs(module);
}

static void
op_full_to_pr(smd_op* op)
{
	if (! pr_mark_reply(op, STATE_DIRTY)) {
		return;
	}

	smd_module* module = op->module;

	// Only safe to skip resending the merged result back to this NPR below
	// if its payload became the *entire* new db (PR had nothing of its own
	// to contribute, and it's the only NPR so no other contributor's items
	// are missing from what it sent) - otherwise this NPR's payload was
	// incomplete relative to the merged result and it still needs the full
	// replay. The cv_key-only message it gets instead carries the explicit
	// SMD_MSG_CV_KEY_ONLY marker, so the NPR keeps its items even though
	// this path resets cv_tid out from under its accumulated value.
	bool safe_to_skip_source = cf_vector_size(&module->db) == 0 &&
			g_smd.node_count == 2;

	// Sizing at cluster-changed used the local db size alone, which misses
	// large incoming payloads, e.g. when this PR started with an empty db.
	// Cheap no-op if already grown; rehashes any earlier contributions.
	smd_hash_grow_for(&module->merge_h,
			cf_vector_size(&module->merge) + cf_vector_size(&op->items));

	module_merge_list(module, &op->items);

	if (module->retry_msg_count != 0) {
		OP_DETAIL("pending replies %u", module->retry_msg_count);
		return;
	}

	// db_h is only grown by module_regen_key2index(), which isn't called on
	// this path - grow it for the upcoming db size now, or a fresh-join db
	// (starts at 0) stays stuck at inline row count while every merge item
	// gets appended one by one, degenerating into O(n) chains per lookup.
	// db_h may already hold this PR's own keys - growth rehashes them.
	smd_hash_grow_for(&module->db_h,
			cf_vector_size(&module->db) + cf_vector_size(&module->merge));

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

	send_full_from_pr(module,
			safe_to_skip_source ? op->node_index : AS_CLUSTER_SZ);

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
	smd_maybe_set_initial_sync_done();
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
		cf_warning(AS_SMD, "set items count %d != 1", cf_vector_size(&op->items));
		return;
	}

	module->cv_key = g_smd.cl_key;
	module->cv_tid = op->tid;

	// A set landing here is authoritative content from the principal for the
	// current cluster key - as good a settle signal as a full or a cv_key-only
	// confirm (see smd_all_modules_settled_locked()).
	module->settle_confirmed = true;
	module->fail_open_confirm_at_ms = 0;

	module_set_npr(module, item_vec_get(&op->items, 0));
	item_vec_disown_items(&op->items);

	smd_maybe_set_initial_sync_done();
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

	// Any reply from the principal - real or old-code default-zero - confirms
	// this NPR is no longer waiting on a signal an old principal never sends.
	module->settle_confirmed = true;
	module->fail_open_confirm_at_ms = 0;

	if (op->committed_key == module->cv_key && op->tid == module->cv_tid) {
		smd_maybe_set_initial_sync_done(); // already applied, still check settle
		return;
	}

	module->cv_key = op->committed_key;
	module->cv_tid = op->tid;

	// Principal is advancing cv_key/cv_tid only - this NPR's data is already
	// current (clean path: its reported tid matched, or merge path: its own
	// payload seeded the entire merged db). The explicit marker tells this
	// apart from a genuinely-empty full replace - a tid comparison can't,
	// since the merge path resets cv_tid while this NPR's accumulated cv_tid
	// is arbitrary.
	if (op->cv_key_only) {
		OP_DETAIL("cv_key-only full-from-pr");

		// Content is unchanged - cv_key/cv_tid just moved to match the
		// principal's (e.g. this NPR joined mid-stream and hadn't merged
		// yet). Nothing to persist beyond the ordinary in-memory update -
		// module_commit_to_disk() is what makes cv_key/cv_tid durable, at
		// the next real content change; cv_key never advances independently
		// of content, so there is no separate value to persist here.
		smd_maybe_set_initial_sync_done();
		return;
	}

	OP_DETAIL("replacing all");

	// An empty payload with no cv_key-only marker is an authoritative empty full
	// that wipes local items and commits the empty db to disk. That is the
	// intended contract, but it is also the signature of every mis-gating bug in
	// this area (the old tid heuristic, the compat-id gap) - so make it loud.
	if (cf_vector_size(&op->items) == 0 && cf_vector_size(&module->db) != 0) {
		cf_warning(AS_SMD,
				"{%s} replacing %u items with empty full from principal",
				module->name, cf_vector_size(&module->db));
	}

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
	smd_maybe_set_initial_sync_done();
}

static void
op_req_full_from_pr(smd_op* op)
{
	smd_module* module = op->module;

	if (module->state != STATE_NPR) {
		return;
	}

	// Receiving REQ_FULL_FROM_PR is positive evidence the principal has a dirty
	// merge in flight - it collects fulls from every NPR, merges, and only then
	// sends FULL_FROM_PR, which can exceed the fail-open interval for the large
	// modules this stack targets. Cancel the mixed-cluster fail-open timer so it
	// can't fire mid-merge and settle this node on pre-merge metadata; the
	// upcoming FULL_FROM_PR is the authoritative signal and re-confirms settle.
	module->fail_open_confirm_at_ms = 0;

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
send_full_from_pr(smd_module* module, uint32_t full_source_ix)
{
	// Only build the (potentially large) full payload if at least one NPR
	// besides the contributing source actually needs it - e.g. the 2-node
	// case where the sole NPR *is* full_source_ix only needs a cv_key-only
	// advance, so serializing the full db here would be wasted work. In a
	// mixed cluster always build and send it: a pre-SERVER-209 NPR doesn't
	// understand the lightweight cv_key-only stand-in and would wipe its
	// local db on receiving an empty payload with a new committed_key.
	bool mixed = smd_mixed_cluster();
	bool need_full = mixed;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (i != full_source_ix) {
			need_full = true;
			break;
		}
	}

	msg* full = NULL;

	if (need_full) {
		full = as_fabric_msg_get(M_TYPE_SMD);
		msg_set_uint32(full, SMD_MSG_OP, SMD_OP_FULL_FROM_PR);
		module_fill_msg(module, full);
	}

	module->retry_msgs[0] = NULL;

	for (uint32_t i = 1; i < g_smd.node_count; i++) {
		if (! mixed && i == full_source_ix) {
			module->retry_msgs[i] = pr_make_cv_key_only_msg(module);
		}
		else {
			msg_incr_ref(full);
			module->retry_msgs[i] = full;
		}
	}

	if (full != NULL) {
		as_fabric_msg_put(full);
	}

	module->retry_msg_count = g_smd.node_count - 1;
	module->retry_next_ms = 0;

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
		smd_op op = { .type = SMD_OP_SET_TO_PR,
			.src = g_config.self_node,
			.module = entry->module,
			.cl_key = entry->cl_key,
			.tid = set_tid };

		item_vec_init(&op.items, 1);
		item_vec_set(&op.items, 0,
				smd_item_create_copy(item->key, item->value, 0, 0));

		// Set this before calling op_set_to_pr() - call may destroy entry.
		// (Also - call won't alter retry_next_ms since we are calling as pr.)
		entry->retry_next_ms = 0;

		op_set_to_pr(&op);
		item_vec_destroy(&op.items);

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
	// Iterate the whole array, not just g_smd.node_count: op_cluster_changed
	// updates node_count to the new (possibly smaller) count before calling
	// here, so on a cluster shrink the slots between the new and old counts
	// would otherwise never be put - leaking a full-payload msg ref per lost
	// node while the principal sits in STATE_CLEAN.
	for (uint32_t i = 1; i < AS_CLUSTER_SZ; i++) {
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
	smd_hash_grow_for(&module->db_h, cf_vector_size(&module->db));

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

	if (module_fill_msg_compressed(module, m)) {
		return;
	}

	uint32_t count = cf_vector_size(&module->db);

	cf_vector key_vec;
	cf_vector val_vec;

	cf_vector_init(&key_vec, sizeof(msg_buf_ele), count, 0);
	cf_vector_init(&val_vec, sizeof(msg_buf_ele), count, 0);

	uint32_t* gen_list = cf_malloc(count * sizeof(uint32_t));

	msg_set_uint64_array_size(m, SMD_MSG_TS_ARRAY, count);

	for (uint32_t i = 0; i < count; i++) {
		const as_smd_item* item = item_vec_get_const(&module->db, i);

		msg_buf_ele key_e = { .sz = (uint32_t)strlen(item->key),
			.ptr = (uint8_t*)item->key };

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

	cf_vector_destroy(&key_vec);
	cf_vector_destroy(&val_vec);
	cf_free(gen_list);
}

static bool
module_fill_msg_compressed(smd_module* module, msg* m)
{
	if (g_config.smd_compression_mode != AS_SMD_COMPRESSION_MODE_ZSTD) {
		return false;
	}

	if (! smd_can_compress_full()) {
		return false;
	}

	void* compressed = NULL;
	size_t compressed_sz = 0;
	size_t orig_sz = 0;

	// These counters are written here (SMD thread) and read by the info thread
	// in as_smd_get_info(); use atomics for a consistent cross-thread read,
	// matching the repl/migrate wire-comp stats.
	as_incr_uint64(&g_smd.compression_attempts);

	if (! as_smd_compress_items(&module->db, (uint8_t**)&compressed,
				&compressed_sz, &orig_sz, g_config.smd_compression_level)) {
		as_incr_uint64(&g_smd.compression_fallbacks);
		return false;
	}

	as_incr_uint64(&g_smd.compression_hits);

	if (compressed_sz < orig_sz) {
		// Bytes saved by compressing this full-sync message, credited per
		// message built. We deliberately do NOT multiply by a guessed recipient
		// count: module_fill_msg() doesn't know the caller's fan-out (a
		// single-peer answer vs the principal's broadcast), so the old
		// (orig - compressed) * node_count over-counted on the common
		// single-recipient paths and re-counted on every retransmit rebuild.
		// Cast is safe under the guard above (and silences -Wsign-conversion on
		// as_add_uint64's int64_t delta): compressed_sz < orig_sz, so the
		// difference is positive and far below INT64_MAX.
		as_add_uint64(&g_smd.compression_bytes_saved,
				(int64_t)(orig_sz - compressed_sz));
	}

	msg_set_uint32(m, SMD_MSG_COMPRESSION, SMD_COMPRESSION_ZSTD);
	msg_set_buf(m, SMD_MSG_COMPRESSED_ITEMS, compressed, compressed_sz,
			MSG_SET_HANDOFF_MALLOC);
	return true;
}

// Lightweight FULL_FROM_PR with no items - lets an NPR that already has
// current data advance cv_key/cv_tid without replaying the whole payload.
static msg*
pr_make_cv_key_only_msg(smd_module* module)
{
	msg* m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, SMD_MSG_OP, SMD_OP_FULL_FROM_PR);
	msg_set_uint64(m, SMD_MSG_CLUSTER_KEY, g_smd.cl_key);
	msg_set_uint32(m, SMD_MSG_MODULE_ID, module->id);
	msg_set_uint64(m, SMD_MSG_COMMITTED_CL_KEY, module->cv_key);
	msg_set_uint64(m, SMD_MSG_TID, module->cv_tid);
	msg_set_uint32(m, SMD_MSG_CV_KEY_ONLY, 1);

	cf_vector key_vec;
	cf_vector val_vec;

	cf_vector_init(&key_vec, sizeof(msg_buf_ele), 0, 0);
	cf_vector_init(&val_vec, sizeof(msg_buf_ele), 0, 0);

	// Defensive - the receiver's parse path returns before reading the value/
	// ts/gen fields once it sees the zero-count key list.
	msg_set_uint64_array_size(m, SMD_MSG_TS_ARRAY, 0);
	msg_msgpack_list_set_buf(m, SMD_MSG_KEY_LIST, &key_vec);
	msg_msgpack_list_set_buf(m, SMD_MSG_VALUE_LIST, &val_vec);
	msg_msgpack_list_set_uint32(m, SMD_MSG_GEN_LIST, NULL, 0);

	cf_vector_destroy(&key_vec);
	cf_vector_destroy(&val_vec);

	return m;
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
			as_smd_conflict_fn cb = has_tombstone ? smd_item_is_less
												  : module->conflict_cb;

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
			as_smd_conflict_fn cb = has_tombstone ? smd_item_is_less
												  : module->conflict_cb;

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

	if (item->generation != old->generation || item->timestamp != old->timestamp) {
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

// key and value are malloc handoffs.
static const as_smd_item*
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
		return item;
	}
	// else - existing key.

	as_smd_item* item = item_vec_get(&module->db, ix);
	bool has_tombstone = item->value == NULL || value == NULL;

	if (! has_tombstone) {
		if (module->conflict_cb != smd_item_is_less) {
			as_smd_item check_item = { .key = key, .value = value };

			if (! module->conflict_cb(item, &check_item)) {
				OP_TYPE_DETAIL(SMD_OP_SET_TO_PR,
						"key %s - module rejected item", key);
				cf_free(key);
				smd_item_value_destroy(value);
				return NULL;
			}
		}

		if (strcmp(item->value, value) == 0) { // ignore if same value
			OP_TYPE_DETAIL(SMD_OP_SET_TO_PR, "key %s - rejected unchanged item",
					item->key);
			cf_free(key);
			smd_item_value_destroy(value);
			return NULL;
		}
	}
	else if (item->value == value) { // i.e. both are NULL
		OP_TYPE_DETAIL(SMD_OP_SET_TO_PR,
				"key %s - rejected unchanged tombstone", item->key);
		cf_free(key);
		smd_item_value_destroy(value);
		return NULL;
	}

	cf_free(key);
	smd_item_value_destroy(item->value);

	item->value = value; // malloc handoff
	item->generation++;
	item->timestamp = cf_clepoch_milliseconds();

	send_set_from_pr(module, item);
	module_commit_to_disk(module);
	module_accept_item(module, item);

	return item;
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
		cf_warning(AS_SMD,
				"invalid file '%s' - module '%s' with JSON error %s source %s line %d column %d position %d",
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

	cf_detail(AS_SMD, "{%s} module_restore_from_disk", MODULE_AS_STRING(module));

	item_vec_init(&module->db, (uint32_t)num_items - start);

	for (uint32_t i = start; i < num_items; i++) {
		j_item = json_array_get(j_file, i);
		cf_assert(json_is_object(j_item), AS_SMD, "invalid file '%s'", smd_path);

		const char* key = json_string_value(json_object_get(j_item, "key"));
		cf_assert(key != NULL, AS_SMD, "invalid file '%s'", smd_path);

		json_t* j_value = json_object_get(j_item, "value");
		cf_assert(j_value != NULL &&
						(json_is_string(j_value) || json_is_null(j_value)),
				AS_SMD, "invalid file '%s'", smd_path);

		json_t* j_gen = json_object_get(j_item, "generation");
		cf_assert(j_gen != NULL && json_is_integer(j_gen), AS_SMD,
				"invalid file '%s'", smd_path);

		json_t* j_ts = json_object_get(j_item, "timestamp");
		cf_assert(j_ts != NULL && json_is_integer(j_ts), AS_SMD,
				"invalid file '%s'", smd_path);

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
	if (module->save_throttle_sec != 0) {
		cf_clock now = cf_get_seconds();

		if (module->next_save_time_sec > now) {
			return;
		}

		module->next_save_time_sec = now + module->save_throttle_sec;

		cf_detail(AS_SMD, "{%s} commit-to-disk - next_save_time_sec %lu",
				MODULE_AS_STRING(module), module->next_save_time_sec);
	}

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

		JSON_ENFORCE(json_object_set_new(j_item, "key", json_string(item->key)));

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

static inline smd_hash_ele*
smd_hash_rows(const smd_hash* h)
{
	return h->big_table != NULL ? h->big_table : (smd_hash_ele*)h->table;
}

static void
smd_hash_init(smd_hash* h)
{
	memset((void*)h->table, 0, sizeof(h->table));
	h->big_table = NULL;
	h->n_rows = SMD_HASH_INLINE_ROWS;
}

static void
smd_hash_clear(smd_hash* h)
{
	smd_hash_ele* rows = smd_hash_rows(h);

	for (uint32_t i = 0; i < h->n_rows; i++) {
		smd_hash_ele* e = rows[i].next;

		while (e != NULL) {
			smd_hash_ele* t = e->next;
			cf_free(e);
			e = t;
		}
	}

	if (h->big_table != NULL) {
		cf_free(h->big_table);
		h->big_table = NULL;
	}

	memset((void*)h->table, 0, sizeof(h->table));
	h->n_rows = SMD_HASH_INLINE_ROWS;
}

// Call with the final (or upper-bound) item count before a bulk-populate pass
// (e.g. replacing a module's whole db) - keeps large modules' lookups from
// degenerating into long per-row chains under the fixed inline row count.
// Safe on a populated hash - existing entries are rehashed into the big
// table (row placement depends on n_rows, so they can't just be copied).
static void
smd_hash_grow_for(smd_hash* h, uint32_t n_items)
{
	if (h->big_table != NULL || n_items <= SMD_HASH_BIG_ROWS_THRESHOLD) {
		return;
	}

	h->big_table =
			(smd_hash_ele*)cf_calloc(SMD_HASH_BIG_ROWS, sizeof(smd_hash_ele));
	h->n_rows = SMD_HASH_BIG_ROWS;
	// From here smd_hash_put() targets big_table.

	// Big table was just installed, so any existing entries live in the
	// inline table. Keys are borrowed pointers - re-insert them and free
	// only the heap-allocated chain nodes.
	for (uint32_t i = 0; i < SMD_HASH_INLINE_ROWS; i++) {
		smd_hash_ele* e_head = &h->table[i];

		if (e_head->key == NULL) {
			continue;
		}

		smd_hash_put(h, e_head->key, e_head->value);

		smd_hash_ele* e = e_head->next;

		while (e != NULL) {
			smd_hash_ele* e_next = e->next;

			smd_hash_put(h, e->key, e->value);
			cf_free(e);

			e = e_next;
		}
	}

	memset((void*)h->table, 0, sizeof(h->table));
}

static void
smd_hash_put(smd_hash* h, const char* key, uint32_t value)
{
	smd_hash_ele* e_head = &smd_hash_rows(h)[smd_hash_get_row_i(h, key)];

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
	const smd_hash_ele* e = &smd_hash_rows(h)[smd_hash_get_row_i(h, key)];

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
smd_hash_get_row_i(const smd_hash* h, const char* key)
{
	return cf_wyhash32((const uint8_t*)key, strlen(key)) & (h->n_rows - 1);
}

//==========================================================
// Local helpers - as_smd_item.
//

static bool
smd_can_compress_full(void)
{
	return as_exchange_min_compatibility_id() >= SMD_FULL_ZSTD_COMPATIBILITY_ID;
}

static as_smd_item*
smd_item_create_copy(const char* key, const char* value, uint64_t ts, uint32_t gen)
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

	return sz == 0 ? (char*)smd_empty_value : cf_strndup((const char*)value, sz);
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
