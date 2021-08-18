/*
 * thr_tsvc.c
 *
 * Copyright (C) 2008-2021 Aerospike, Inc.
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

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/scan.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/thr_query.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/xdr.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/proxy.h"
#include "transaction/re_replicate.h"
#include "transaction/read.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"
#include "transaction/write.h"


//==========================================================
// Inlines & macros.
//

static inline bool
should_security_check_data_op(const as_transaction *tr)
{
	return tr->origin == FROM_CLIENT || tr->origin == FROM_BATCH;
}

static inline as_sec_perm
scan_perm(const as_transaction *tr)
{
	if (as_transaction_is_udf(tr)) {
		return PERM_UDF_SCAN;
	}

	return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) != 0 ?
			PERM_OPS_SCAN : PERM_SCAN;
}

static inline as_sec_perm
query_perm(const as_transaction *tr)
{
	if (as_transaction_is_udf(tr)) {
		return PERM_UDF_QUERY;
	}

	return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) != 0 ?
			PERM_OPS_QUERY : PERM_QUERY;
}

static inline const char*
write_type_tag(const as_transaction *tr)
{
	return as_transaction_is_delete(tr) ? "delete" :
			(as_transaction_is_udf(tr) ? "udf" : "write");
}

static inline void
detail_unique_client_rw(const as_transaction *tr, bool is_write)
{
	if (! as_transaction_is_restart(tr) && tr->origin == FROM_CLIENT) {
		cf_detail(AS_RW_CLIENT, "{%s} digest %pD client %s %s",
				tr->rsv.ns->name, &tr->keyd, tr->from.proto_fd_h->client,
				is_write ? write_type_tag(tr) : "read");
	}
}


//==========================================================
// Public API.
//

// Handle the transaction, including proxy to another node if necessary.
void
as_tsvc_process_transaction(as_transaction *tr)
{
	if (tr->msgp->proto.type == PROTO_TYPE_INTERNAL_XDR) {
		as_xdr_read(tr);
		return;
	}

	int rv;
	bool free_msgp = true;
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

	as_transaction_init_body(tr);

	// Check that the socket is authenticated.
	if (tr->origin == FROM_CLIENT) {
		uint8_t result = as_security_check_auth(tr->from.proto_fd_h);

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

		cf_warning(AS_TSVC, "unknown namespace %.*s (%u) in protocol request - check configuration file",
				ns_sz, nf->data, ns_sz);

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
			// Old batch - unsupported.
			as_multi_rec_transaction_error(tr, AS_ERR_UNSUPPORTED_FEATURE);
		}
		else if (as_transaction_is_query(tr)) {
			// Query.
			cf_atomic64_incr(&ns->query_reqs);

			if (! as_security_check_data_op(tr, ns, query_perm(tr))) {
				as_multi_rec_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if (! as_query(tr, ns)) {
				cf_atomic64_incr(&ns->query_fail);
				as_multi_rec_transaction_error(tr, tr->result_code);
			}
		}
		else {
			// Scan.
			if (! as_security_check_data_op(tr, ns, scan_perm(tr))) {
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

	// Copy digest if not already in tr.
	if (as_transaction_has_digest(tr)) {
		as_msg_field *df = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
		uint32_t digest_sz = as_msg_field_get_value_sz(df);

		if (digest_sz != sizeof(cf_digest)) {
			cf_warning(AS_TSVC, "digest msg field size %u", digest_sz);
			as_transaction_error(tr, ns, AS_ERR_PARAMETER);
			goto Cleanup;
		}

		tr->keyd = *(cf_digest *)df->data;
	}
	else if (as_transaction_has_key(tr)) {
		// Old client - deprecated.
		cf_warning(AS_TSVC, "msg has key but no digest");
		as_transaction_error(tr, ns, AS_ERR_UNSUPPORTED_FEATURE);
		goto Cleanup;
	}
	// else - batch sub-transactions & all internal transactions have neither
	// digest nor key in the message - digest is already in tr.

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
				! as_security_check_data_op(tr, ns,
						PERM_WRITE | (is_read ? PERM_READ : 0))) {
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

		detail_unique_client_rw(tr, is_write);

		transaction_status status;

		if (is_write) {
			if (as_transaction_is_delete(tr)) {
				status = convert_to_write(tr, &msgp) ?
						as_write_start(tr) : as_delete_start(tr);
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
		case FROM_IOPS:
			tr->from.iops_orig->cb(tr->from.iops_orig->udata, AS_ERR_UNKNOWN);
			tr->from.iops_orig = NULL; // pattern, not needed
			break;
		case FROM_RE_REPL:
			tr->from.re_repl_orig_cb(tr);
			tr->from.re_repl_orig_cb = NULL; // pattern, not needed
			break;
		default:
			cf_crash(AS_TSVC, "unexpected transaction origin %u", tr->origin);
			break;
		}
	}

Cleanup:

	if (free_msgp && ! SHARED_MSGP(tr)) {
		cf_free(msgp);
	}
} // end process_transaction()
