/*
 * rw_utils.h
 *
 * Copyright (C) 2016-2018 Aerospike, Inc.
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

#include "citrusleaf/cf_digest.h"

#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "transaction/rw_request.h"
#include "transaction/udf.h"


//==========================================================
// Forward declarations.
//

struct as_bin_s;
struct as_index_s;
struct as_index_tree_s;
struct as_msg_s;
struct as_namespace_s;
struct as_remote_record_s;
struct as_storage_rd_s;
struct as_transaction_s;
struct rw_request_s;
struct udf_record_s;


//==========================================================
// Typedefs & constants.
//

typedef struct index_metadata_s {
	uint32_t void_time;
	uint64_t last_update_time;
	uint16_t generation;
} index_metadata;

typedef struct now_times_s {
	uint64_t now_ns;
	uint64_t now_ms;
} now_times;

// For now, use only for as_msg record_ttl special values.
#define TTL_NAMESPACE_DEFAULT	0
#define TTL_NEVER_EXPIRE		((uint32_t)-1)
#define TTL_DONT_UPDATE			((uint32_t)-2)


//==========================================================
// Public API.
//

bool validate_delete_durability(struct as_transaction_s* tr);
bool xdr_allows_write(struct as_transaction_s* tr);
void send_rw_messages(struct rw_request_s* rw);
void send_rw_messages_forget(struct rw_request_s* rw);
int repl_state_check(struct as_index_s* r, struct as_transaction_s* tr);
void will_replicate(struct as_index_s* r, struct as_namespace_s* ns);
bool insufficient_replica_destinations(const struct as_namespace_s* ns, uint32_t n_dests);
void finished_replicated(struct as_transaction_s* tr);
void finished_not_replicated(struct rw_request_s* rw);
bool generation_check(const struct as_index_s* r, const struct as_msg_s* m, const struct as_namespace_s* ns);
int set_set_from_msg(struct as_index_s* r, struct as_namespace_s* ns, struct as_msg_s* m);
int set_delete_durablility(const struct as_transaction_s* tr, struct as_storage_rd_s* rd);
bool check_msg_key(struct as_msg_s* m, struct as_storage_rd_s* rd);
bool get_msg_key(struct as_transaction_s* tr, struct as_storage_rd_s* rd);
int handle_msg_key(struct as_transaction_s* tr, struct as_storage_rd_s* rd);
void update_metadata_in_index(struct as_transaction_s* tr, struct as_index_s* r);
void pickle_all(struct as_storage_rd_s* rd, struct rw_request_s* rw);
bool write_sindex_update(struct as_namespace_s* ns, const char* set_name, cf_digest* keyd, struct as_bin_s* old_bins, uint32_t n_old_bins, struct as_bin_s* new_bins, uint32_t n_new_bins);
void record_delete_adjust_sindex(struct as_index_s* r, struct as_namespace_s* ns);
void delete_adjust_sindex(struct as_storage_rd_s* rd);
void remove_from_sindex(struct as_namespace_s* ns, const char* set_name, cf_digest* keyd, struct as_bin_s* bins, uint32_t n_bins);
bool xdr_must_ship_delete(struct as_namespace_s* ns, bool is_xdr_op);


// TODO - rename as as_record_... and move to record.c?
static inline bool
record_has_sindex(const as_record* r, as_namespace* ns)
{
	if (! as_sindex_ns_has_sindex(ns)) {
		return false;
	}

	as_set* set = as_namespace_get_record_set(ns, r);

	return set ? set->n_sindexes != 0 : ns->n_setless_sindexes != 0;
}


static inline bool
respond_on_master_complete(as_transaction* tr)
{
	return tr->origin == FROM_CLIENT &&
			TR_WRITE_COMMIT_LEVEL(tr) == AS_WRITE_COMMIT_LEVEL_MASTER;
}


static inline void
destroy_stack_bins(as_bin* stack_bins, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin_particle_destroy(&stack_bins[i], true);
	}
}


// Not a nice way to specify a read-all op - dictated by backward compatibility.
// Note - must check this before checking for normal read op!
static inline bool
op_is_read_all(as_msg_op* op, as_msg* m)
{
	return op->name_sz == 0 && op->op == AS_MSG_OP_READ &&
			(m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
}


static inline bool
is_valid_ttl(uint32_t ttl)
{
	// Note - for now, ttl must be as_msg record_ttl.
	// Note - ttl <= MAX_ALLOWED_TTL includes ttl == TTL_NAMESPACE_DEFAULT.
	return ttl <= MAX_ALLOWED_TTL ||
			ttl == TTL_NEVER_EXPIRE || ttl == TTL_DONT_UPDATE;
}


static inline void
clear_delete_response_metadata(as_transaction* tr)
{
	// If write became delete, respond to origin with no metadata.
	if ((tr->flags & AS_TRANSACTION_FLAG_IS_DELETE) != 0) {
		tr->generation = 0;
		tr->void_time = 0;
		tr->last_update_time = 0;
	}
}


//==========================================================
// Private API - for enterprise separation only.
//

bool create_only_check(const struct as_index_s* r, const struct as_msg_s* m);
void write_delete_record(struct as_index_s* r, struct as_index_tree_s* tree);

udf_optype udf_finish_delete(struct udf_record_s* urecord);

uint32_t dup_res_pack_repl_state_info(const struct as_index_s* r, struct as_namespace_s* ns);
uint32_t dup_res_pack_info(const struct as_index_s* r, struct as_namespace_s* ns);
bool dup_res_should_retry_transaction(struct rw_request_s* rw, uint32_t result_code);
void dup_res_handle_tie(struct rw_request_s* rw, const msg* m, uint32_t result_code);
void apply_if_tie(struct rw_request_s* rw);
void dup_res_translate_result_code(struct rw_request_s* rw);
bool dup_res_ignore_pickle(const uint8_t* buf, uint32_t info);
void dup_res_init_repl_state(struct as_remote_record_s* rr, uint32_t info);

void repl_write_flag_pickle(const struct as_transaction_s* tr, const uint8_t* buf, uint32_t* info);
bool repl_write_pickle_is_drop(const uint8_t* buf, uint32_t info);
void repl_write_init_repl_state(struct as_remote_record_s* rr, bool from_replica);
conflict_resolution_pol repl_write_conflict_resolution_policy(const struct as_namespace_s* ns);
bool repl_write_should_retransmit_replicas(struct rw_request_s* rw, uint32_t result_code);
void repl_write_send_confirmation(struct rw_request_s* rw);
void repl_write_handle_confirmation(msg* m);

int record_replace_check(struct as_index_s* r, struct as_namespace_s* ns);
void record_replaced(struct as_index_s* r, struct as_remote_record_s* rr);
