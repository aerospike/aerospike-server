/*
 * migrate.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "msg.h"
#include "node.h"
#include "rchash.h"
#include "shash.h"

#include "fabric/hb.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"


//==========================================================
// Forward declarations.
//

struct as_index_s;
struct as_index_ref_s;
struct as_namespace_s;
struct as_remote_record_s;
struct meta_in_q_s;
struct meta_out_q_s;
struct pb_task_s;


//==========================================================
// Typedefs & constants.
//

// For receiver-side migration flow-control.
// TODO - move to namespace? Go even lower than 4?
#define AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING 4
#define AS_MIGRATE_LIMIT_MAX_NUM_INCOMING 256

// Maximum permissible number of migrate xmit threads.
#define MAX_NUM_MIGRATE_XMIT_THREADS 100

#define TX_FLAGS_NONE           ((uint32_t) 0x0)
#define TX_FLAGS_ACTING_MASTER  ((uint32_t) 0x1)
#define TX_FLAGS_LEAD           ((uint32_t) 0x2)
#define TX_FLAGS_CONTINGENT     ((uint32_t) 0x4)


//==========================================================
// Public API.
//

void as_migrate_init();
void as_migrate_emigrate(const struct pb_task_s *task);
void as_migrate_set_num_xmit_threads(uint32_t n_threads);
void as_migrate_dump(bool verbose);


//==========================================================
// Private API - for enterprise separation only.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	MIG_FIELD_OP,
	MIG_FIELD_UNUSED_1,
	MIG_FIELD_EMIG_ID,
	MIG_FIELD_NAMESPACE,
	MIG_FIELD_PARTITION,
	MIG_FIELD_DIGEST, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_GENERATION, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_RECORD,
	MIG_FIELD_CLUSTER_KEY,
	MIG_FIELD_UNUSED_9,
	MIG_FIELD_VOID_TIME, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_UNUSED_11,
	MIG_FIELD_UNUSED_12,
	MIG_FIELD_INFO,
	MIG_FIELD_UNUSED_14,
	MIG_FIELD_UNUSED_15,
	MIG_FIELD_UNUSED_16,
	MIG_FIELD_UNUSED_17,
	MIG_FIELD_UNUSED_18,
	MIG_FIELD_LAST_UPDATE_TIME, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_FEATURES,
	MIG_FIELD_UNUSED_21,
	MIG_FIELD_META_RECORDS,
	MIG_FIELD_META_SEQUENCE,
	MIG_FIELD_META_SEQUENCE_FINAL,
	MIG_FIELD_PARTITION_SIZE,
	MIG_FIELD_SET_NAME, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_KEY, // TODO - old pickle - deprecate in "six months"
	MIG_FIELD_UNUSED_28,
	MIG_FIELD_EMIG_INSERT_ID,

	NUM_MIG_FIELDS
} migrate_msg_fields;

#define OPERATION_UNDEF 0
#define OPERATION_OLD_INSERT 1 // TODO - old pickle - deprecate in "six months"
#define OPERATION_INSERT_ACK 2
#define OPERATION_START 3
#define OPERATION_START_ACK_OK 4
#define OPERATION_START_ACK_EAGAIN 5
#define OPERATION_START_ACK_FAIL 6
#define OPERATION_INSERT 7
#define OPERATION_DONE 8
#define OPERATION_DONE_ACK 9
#define OPERATION_UNUSED_10 10 // deprecated
#define OPERATION_MERGE_META 11
#define OPERATION_MERGE_META_ACK 12
#define OPERATION_ALL_DONE 13
#define OPERATION_ALL_DONE_ACK 14

#define MIG_INFO_UNUSED_1       0x0001
#define MIG_INFO_UNUSED_2       0x0002
#define MIG_INFO_UNREPLICATED   0x0004 // enterprise only
#define MIG_INFO_TOMBSTONE      0x0008 // enterprise only

#define MIG_FEATURE_MERGE 0x00000001U
#define MIG_FEATURES_SEEN 0x80000000U // needed for backward compatibility
extern const uint32_t MY_MIG_FEATURES;

typedef struct emigration_s {
	cf_node     dest;
	uint64_t    cluster_key;
	uint32_t    id;
	pb_task_type type;
	uint32_t    tx_flags;
	cf_atomic32 state;
	bool        aborted;
	bool        from_replica;
	uint64_t    wait_until_ms;

	cf_atomic32 bytes_emigrating;
	cf_shash    *reinsert_hash;
	uint64_t    insert_id;
	cf_queue    *ctrl_q;
	struct meta_in_q_s *meta_q;

	as_partition_reservation rsv;
} emigration;

typedef struct immigration_s {
	cf_node          src;
	uint64_t         cluster_key;
	uint32_t         pid;

	cf_atomic32      done_recv;      // flag - 0 if not yet received, atomic counter for receives
	uint64_t         start_recv_ms;  // time the first START event was received
	uint64_t         done_recv_ms;   // time the first DONE event was received

	uint32_t         emig_id;
	struct meta_out_q_s *meta_q;

	as_migrate_result start_result;
	uint32_t        features;
	struct as_namespace_s *ns; // for statistics only

	as_partition_reservation rsv;
} immigration;

typedef struct immigration_hkey_s {
	cf_node src;
	uint32_t emig_id;
} __attribute__((__packed__)) immigration_hkey;


// Globals.
extern cf_rchash *g_emigration_hash;
extern cf_rchash *g_immigration_hash;
extern cf_queue g_emigration_q;


// Emigration, immigration, & pickled record destructors.
void emigration_release(emigration *emig);
void immigration_release(immigration *immig);

// Emigration.
void emigrate_fill_queue_init();
void emigrate_queue_push(emigration *emig);
bool should_emigrate_record(emigration *emig, struct as_index_ref_s *r_ref);
uint32_t emigration_pack_info(const emigration *emig, const struct as_index_s *r);

// Migrate fabric message handling.
void emigration_handle_meta_batch_request(cf_node src, msg *m);
bool immigration_ignore_pickle(const uint8_t *buf, uint32_t info);
void immigration_init_repl_state(struct as_remote_record_s* rr, uint32_t info);
void immigration_handle_meta_batch_ack(cf_node src, msg *m);

// Meta sender.
bool immigration_start_meta_sender(immigration *immig, uint32_t emig_features, uint64_t emig_n_recs);
