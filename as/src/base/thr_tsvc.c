/*
 * thr_tsvc.c
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

//==========================================================
// Includes.
//

#include "base/thr_tsvc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "fault.h"
#include "hardware.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/scan.h"
#include "base/secondary_index.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/xdr_serverside.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/proxy.h"
#include "transaction/re_replicate.h"
#include "transaction/read.h"
#include "transaction/udf.h"
#include "transaction/write.h"


//==========================================================
// Globals.
//

static cf_queue* g_transaction_queues[MAX_TRANSACTION_QUEUES] = { NULL };

// Track number of threads for each queue independently.
static uint32_t g_queues_n_threads[MAX_TRANSACTION_QUEUES] = { 0 };

// It's ok for this to not be atomic - might not round-robin perfectly, but will
// be cache friendly.
static uint32_t g_current_q = 0;


//==========================================================
// Forward declarations.
//

void tsvc_add_threads(uint32_t qid, uint32_t n_threads);
void tsvc_remove_threads(uint32_t qid, uint32_t n_threads);
void *run_tsvc(void *arg);


//==========================================================
// Inlines & macros.
//

static inline bool
should_security_check_data_op(const as_transaction *tr)
{
	return tr->origin == FROM_CLIENT || tr->origin == FROM_BATCH;
}

static const char*
write_type_tag(const as_transaction *tr)
{
	return as_transaction_is_delete(tr) ? "delete" :
			(as_transaction_is_udf(tr) ? "udf" : "write");
}

static inline void
detail_unique_client_rw(const as_transaction *tr, bool is_write)
{
	if (tr->origin == FROM_CLIENT) {
		cf_detail_digest(AS_RW_CLIENT, &tr->keyd, "{%s} client %s %s ",
				tr->rsv.ns->name, tr->from.proto_fd_h->client,
				is_write ? write_type_tag(tr) : "read");
	}
}


//==========================================================
// Public API.
//

void
as_tsvc_init()
{
	cf_info(AS_TSVC, "%u transaction queues: starting %u threads per queue",
			g_config.n_transaction_queues,
			g_config.n_transaction_threads_per_queue);

	// Create the transaction queues.
	for (uint32_t qid = 0; qid < g_config.n_transaction_queues; qid++) {
		g_transaction_queues[qid] =
				cf_queue_create(AS_TRANSACTION_HEAD_SIZE, true);
	}

	// Start all the transaction threads.
	for (uint32_t qid = 0; qid < g_config.n_transaction_queues; qid++) {
		tsvc_add_threads(qid, g_config.n_transaction_threads_per_queue);
	}
}


// Decide which queue to use, and enqueue transaction.
void
as_tsvc_enqueue(as_transaction *tr)
{
	uint32_t qid;

	if (g_config.auto_pin == CF_TOPO_AUTO_PIN_NONE ||
			g_config.n_namespaces_not_inlined == 0) {
		cf_debug(AS_TSVC, "no CPU pinning - dispatching transaction round-robin");
		// Transaction can go on any queue - distribute evenly.
		qid = (g_current_q++) % g_config.n_transaction_queues;
	}
	else {
		qid = cf_topo_current_cpu();
		cf_debug(AS_TSVC, "transaction on CPU %u", qid);
	}

	cf_queue_push(g_transaction_queues[qid], tr);
}


// Triggered via dynamic configuration change.
void
as_tsvc_set_threads_per_queue(uint32_t target_n_threads)
{
	for (uint32_t qid = 0; qid < g_config.n_transaction_queues; qid++) {
		uint32_t current_n_threads = g_queues_n_threads[qid];

		if (target_n_threads > current_n_threads) {
			tsvc_add_threads(qid, target_n_threads - current_n_threads);
		}
		else {
			tsvc_remove_threads(qid, current_n_threads - target_n_threads);
		}
	}

	g_config.n_transaction_threads_per_queue = target_n_threads;
}


// Total transactions currently queued, for ticker and info statistics.
int
as_tsvc_queue_get_size()
{
	int current_total = 0;

	for (uint32_t qid = 0; qid < g_config.n_transaction_queues; qid++) {
		current_total += cf_queue_sz(g_transaction_queues[qid]);
	}

	return current_total;
}


// Handle the transaction, including proxy to another node if necessary.
void
as_tsvc_process_transaction(as_transaction *tr)
{
	if (tr->msgp->proto.type == PROTO_TYPE_INTERNAL_XDR) {
		as_xdr_read_txn(tr);
		return;
	}

	int rv;
	bool free_msgp = true;
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

	as_transaction_init_body(tr);

	// Check that the socket is authenticated.
	if (tr->origin == FROM_CLIENT) {
		uint8_t result = as_security_check(tr->from.proto_fd_h, PERM_NONE);

		if (result != AS_OK) {
			as_security_log(tr->from.proto_fd_h, result, PERM_NONE, NULL, NULL);
			as_transaction_error(tr, NULL, (uint32_t)result);
			goto Cleanup;
		}
	}

	// All transactions must have a namespace.
	as_msg_field *nf = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! nf) {
		cf_warning(AS_TSVC, "no namespace in protocol request");
		as_transaction_error(tr, NULL, AS_ERR_NAMESPACE);
		goto Cleanup;
	}

	as_namespace *ns = as_namespace_get_bymsgfield(nf);

	if (! ns) {
		uint32_t ns_sz = as_msg_field_get_value_sz(nf);
		CF_ZSTR_DEFINE(ns_name, AS_ID_NAMESPACE_SZ, nf->data, ns_sz);

		cf_warning(AS_TSVC, "unknown namespace %s (%u) in protocol request - check configuration file",
				ns_name, ns_sz);

		as_transaction_error(tr, NULL, AS_ERR_NAMESPACE);
		goto Cleanup;
	}

	// Have we finished the very first partition balance?
	if (! as_partition_balance_is_init_resolved()) {
		if (tr->origin == FROM_PROXY) {
			as_proxy_return_to_sender(tr, ns);
			tr->from.proxy_node = 0; // pattern, not needed
		}
		else {
			cf_debug(AS_TSVC, "rejecting transaction - initial partition balance unresolved");
			as_transaction_error(tr, NULL, AS_ERR_UNAVAILABLE);
			// Note that we forfeited namespace info above so scan & query don't
			// get counted as single-record error.
		}

		goto Cleanup;
	}

	//------------------------------------------------------
	// Multi-record transaction.
	//

	if (as_transaction_is_multi_record(tr)) {
		if (m->transaction_ttl != 0) {
			// Queries may specify transaction_ttl, but don't use
			// g_config.transaction_max_ns as a default. Assuming specified TTL
			// is large enough that it's not worth checking for timeout here.
			tr->end_time = tr->start_time +
					((uint64_t)m->transaction_ttl * 1000000);
		}

		if (as_transaction_is_batch_direct(tr)) {
			// Old batch - deprecated.
			as_multi_rec_transaction_error(tr, AS_ERR_UNSUPPORTED_FEATURE);
		}
		else if (as_transaction_is_query(tr)) {
			// Query.
			cf_atomic64_incr(&ns->query_reqs);

			if (! as_security_check_data_op(tr, ns,
					as_transaction_is_udf(tr) ? PERM_UDF_QUERY : PERM_QUERY)) {
				as_multi_rec_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if (as_query(tr, ns) != 0) {
				cf_atomic64_incr(&ns->query_fail);
				as_multi_rec_transaction_error(tr, tr->result_code);
			}
		}
		else {
			// Scan.
			if (! as_security_check_data_op(tr, ns,
					as_transaction_is_udf(tr) ? PERM_UDF_SCAN : PERM_SCAN)) {
				as_multi_rec_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if ((rv = as_scan(tr, ns)) != 0) {
				as_multi_rec_transaction_error(tr, rv);
			}
		}

		goto Cleanup;
	}

	//------------------------------------------------------
	// Single-record transaction.
	//

	// Calculate end_time based on message transaction TTL. May be recalculating
	// for re-queued transactions, but nice if end_time not copied on/off queue.
	if (m->transaction_ttl != 0) {
		tr->end_time = tr->start_time +
				((uint64_t)m->transaction_ttl * 1000000);
	}
	else {
		// Incorporate g_config.transaction_max_ns if appropriate.
		// TODO - should g_config.transaction_max_ns = 0 be special?
		tr->end_time = tr->start_time + g_config.transaction_max_ns;
	}

	// Did the transaction time out while on the queue?
	if (cf_getns() > tr->end_time) {
		cf_debug(AS_TSVC, "transaction timed out in queue");
		as_transaction_error(tr, ns, AS_ERR_TIMEOUT);
		goto Cleanup;
	}

	// All single-record transactions must have a digest, or a key from which
	// to calculate it.
	if (as_transaction_has_digest(tr)) {
		// Modern client - just copy digest into tr.

		as_msg_field *df = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
		uint32_t digest_sz = as_msg_field_get_value_sz(df);

		if (digest_sz != sizeof(cf_digest)) {
			cf_warning(AS_TSVC, "digest msg field size %u", digest_sz);
			as_transaction_error(tr, ns, AS_ERR_PARAMETER);
			goto Cleanup;
		}

		tr->keyd = *(cf_digest *)df->data;
	}
	else if (! as_transaction_is_batch_sub(tr)) {
		// Old client - calculate digest from key & set, directly into tr.

		as_msg_field *kf = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
		uint32_t key_sz = as_msg_field_get_value_sz(kf);

		as_msg_field *sf = as_transaction_has_set(tr) ?
				as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;
		uint32_t set_sz = sf ? as_msg_field_get_value_sz(sf) : 0;

		cf_digest_compute2(sf->data, set_sz, kf->data, key_sz, &tr->keyd);
	}
	// else - batch sub-transactions already (and only) have digest in tr.

	// Process the transaction.

	bool is_write = (m->info2 & AS_MSG_INFO2_WRITE) != 0;
	bool is_read = (m->info1 & AS_MSG_INFO1_READ) != 0;
	// Both can be set together, but is_write puts us on the 'write path' -
	// write reservation, replica writes, etc. Writes quickly get split into
	// write, delete, or UDF after the reservation.

	uint32_t pid = as_partition_getid(&tr->keyd);
	cf_node dest;

	if (is_write) {
		if (should_security_check_data_op(tr) &&
				! as_security_check_data_op(tr, ns, PERM_WRITE)) {
			as_transaction_error(tr, ns, tr->result_code);
			goto Cleanup;
		}

		rv = as_partition_reserve_write(ns, pid, &tr->rsv, &dest);
	}
	else if (is_read) {
		if (should_security_check_data_op(tr) &&
				! as_security_check_data_op(tr, ns, PERM_READ)) {
			as_transaction_error(tr, ns, tr->result_code);
			goto Cleanup;
		}

		rv = as_partition_reserve_read_tr(ns, pid, tr, &dest);
	}
	else {
		cf_warning(AS_TSVC, "transaction is neither read nor write - unexpected");
		as_transaction_error(tr, ns, AS_ERR_PARAMETER);
		goto Cleanup;
	}

	if (rv == -2) {
		// Partition is unavailable.
		as_transaction_error(tr, ns, AS_ERR_UNAVAILABLE);
		goto Cleanup;
	}

	if (dest == 0) {
		cf_crash(AS_TSVC, "invalid destination while reserving partition");
	}

	if (rv == 0) {
		// <><><><><><>  Reservation Succeeded  <><><><><><>

		if (! as_transaction_is_restart(tr)) {
			tr->benchmark_time = 0;
			detail_unique_client_rw(tr, is_write);
		}

		transaction_status status;

		if (is_write) {
			if (as_transaction_is_delete(tr)) {
				status = as_delete_start(tr);
			}
			else if (tr->origin == FROM_IUDF || as_transaction_is_udf(tr)) {
				status = as_udf_start(tr);
			}
			else if (tr->origin == FROM_RE_REPL) {
				status = as_re_replicate_start(tr);
			}
			else {
				status = as_write_start(tr);
			}
		}
		else {
			status = as_read_start(tr);
		}

		switch (status) {
		case TRANS_DONE_ERROR:
		case TRANS_DONE_SUCCESS:
			// Done, response already sent - free msg & release reservation.
			as_partition_release(&tr->rsv);
			break;
		case TRANS_IN_PROGRESS:
			// Don't free msg or release reservation - both owned by rw_request.
			free_msgp = false;
			break;
		case TRANS_WAITING:
			// Will be re-queued - don't free msg, but release reservation.
			free_msgp = false;
			as_partition_release(&tr->rsv);
			break;
		default:
			cf_crash(AS_TSVC, "invalid transaction status %d", status);
			break;
		}
	}
	else {
		// <><><><><><>  Reservation Failed  <><><><><><>

		switch (tr->origin) {
		case FROM_CLIENT:
		case FROM_BATCH:
			as_proxy_divert(dest, tr, ns);
			// CLIENT: fabric owns msgp, BATCH: it's shared, don't free it.
			free_msgp = false;
			break;
		case FROM_PROXY:
			as_proxy_return_to_sender(tr, ns);
			tr->from.proxy_node = 0; // pattern, not needed
			break;
		case FROM_IUDF:
			tr->from.iudf_orig->cb(tr->from.iudf_orig->udata, AS_ERR_UNKNOWN);
			tr->from.iudf_orig = NULL; // pattern, not needed
			break;
		case FROM_RE_REPL:
			tr->from.re_repl_orig_cb(tr);
			tr->from.re_repl_orig_cb = NULL; // pattern, not needed
			break;
		default:
			cf_crash(AS_PROTO, "unexpected transaction origin %u", tr->origin);
			break;
		}
	}

Cleanup:

	if (free_msgp && tr->origin != FROM_BATCH) {
		cf_free(msgp);
	}
} // end process_transaction()


//==========================================================
// Local helpers.
//

void
tsvc_add_threads(uint32_t qid, uint32_t n_threads)
{
	for (uint32_t n = 0; n < n_threads; n++) {
		cf_thread_create_detached(run_tsvc, (void*)(uint64_t)qid);
	}

	g_queues_n_threads[qid] += n_threads;
}


void
tsvc_remove_threads(uint32_t qid, uint32_t n_threads)
{
	as_transaction death_tr = { .msgp = NULL };

	for (uint32_t n = 0; n < n_threads; n++) {
		// Send terminator (transaction with NULL msgp).
		cf_queue_push(g_transaction_queues[qid], &death_tr);
	}

	g_queues_n_threads[qid] -= n_threads;
}


// Service transactions - arg is the queue we're to service.
void *
run_tsvc(void *arg)
{
	uint32_t qid = (uint32_t)(uint64_t)arg;

	if (g_config.auto_pin != CF_TOPO_AUTO_PIN_NONE &&
			g_config.n_namespaces_not_inlined != 0) {
		cf_detail(AS_TSVC, "pinning thread to CPU %u", qid);
		cf_topo_pin_to_cpu((cf_topo_cpu_index)qid);
	}

	cf_queue *q = g_transaction_queues[qid];

	while (true) {
		as_transaction tr;

		if (cf_queue_pop(q, &tr, CF_QUEUE_FOREVER) != CF_QUEUE_OK) {
			cf_crash(AS_TSVC, "unable to pop from transaction queue");
		}

		if (! tr.msgp) {
			break; // thread termination via configuration change
		}

		cf_debug(AS_TSVC, "running on CPU %hu", cf_topo_current_cpu());

		if (g_config.svc_benchmarks_enabled &&
				tr.benchmark_time != 0 && ! as_transaction_is_restart(&tr)) {
			histogram_insert_data_point(g_stats.svc_queue_hist,
					tr.benchmark_time);
		}

		as_tsvc_process_transaction(&tr);
	}

	return NULL;
}
