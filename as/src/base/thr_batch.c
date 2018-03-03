/*
 * thr_batch.c
 *
 * Copyright (C) 2012-2015 Aerospike, Inc.
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

#include "base/thr_batch.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_thread_pool.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "dynbuf.h"
#include "hist.h"
#include "node.h"
#include "socket.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/stats.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"

typedef struct {
	cf_node node;
	cf_digest keyd;
	bool done;
} batch_digest;

typedef struct {
	int n_digests;
	batch_digest digest[];
} batch_digests;

typedef struct {
	uint64_t trid;
	uint64_t end_time;
	as_namespace* ns;
	as_file_handle* fd_h;
	batch_digests* digests;
	cf_vector* binlist;
	bool get_data;
	bool complete;
} batch_transaction;

static as_thread_pool batch_direct_thread_pool;

static void
as_msg_make_error_response_bufbuilder(cf_digest *keyd, int result_code,
		cf_buf_builder **bb_r, const char *ns_name)
{
	size_t ns_len = strlen(ns_name);
	size_t msg_sz = sizeof(as_msg) +
			sizeof(as_msg_field) + sizeof(cf_digest) +
			sizeof(as_msg_field) + ns_len;

	uint8_t *buf;
	cf_buf_builder_reserve(bb_r, (int)msg_sz, &buf);

	as_msg *msgp = (as_msg *)buf;

	msgp->header_sz = (uint8_t)sizeof(as_msg);
	msgp->info1 = 0;
	msgp->info2 = 0;
	msgp->info3 = 0;
	msgp->unused = 0;
	msgp->result_code = (uint8_t)result_code;
	msgp->generation = 0;
	msgp->record_ttl = 0;
	msgp->transaction_ttl = 0;
	msgp->n_fields = 2;
	msgp->n_ops = 0;
	as_msg_swap_header(msgp);

	buf += sizeof(as_msg);

	as_msg_field *mf = (as_msg_field *)buf;

	mf->field_sz = sizeof(cf_digest) + 1;
	mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
	memcpy(mf->data, keyd, sizeof(cf_digest));
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + sizeof(cf_digest);

	mf = (as_msg_field *)buf;
	mf->field_sz = (uint32_t)ns_len + 1;
	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, ns_name, ns_len);
	as_msg_swap_field(mf);
}

// Build response to batch request.
static void
batch_build_response(batch_transaction* btr, cf_buf_builder** bb_r)
{
	as_namespace* ns = btr->ns;
	batch_digests *bmds = btr->digests;
	bool get_data = btr->get_data;
	uint32_t yield_count = 0;

	for (int i = 0; i < bmds->n_digests; i++)
	{
		batch_digest *bmd = &bmds->digest[i];

		if (bmd->done == false) {
			// try to get the key
			as_partition_reservation rsv;
			cf_node other_node = 0;

			if (! *bb_r) {
				*bb_r = cf_buf_builder_create_size(1024 * 4);
			}

			int rv = as_partition_reserve_read(ns, as_partition_getid(&bmd->keyd), &rsv, false, &other_node);

			if (rv == 0) {
				as_index_ref r_ref;
				r_ref.skip_lock = false;
				int rec_rv = as_record_get_live(rsv.tree, &bmd->keyd, &r_ref, ns);

				if (rec_rv == 0) {
					as_index *r = r_ref.r;

					// Check to see this isn't a record waiting to die.
					if (as_record_is_doomed(r, ns)) {
						as_msg_make_error_response_bufbuilder(&bmd->keyd, AS_PROTO_RESULT_FAIL_NOT_FOUND, bb_r, ns->name);
					}
					else {
						// Make sure it's brought in from storage if necessary.
						as_storage_rd rd;
						as_storage_record_open(ns, r, &rd);

						if (get_data) {
							as_storage_rd_load_n_bins(&rd); // TODO - handle error returned
						}

						// Note: this array must stay in scope until the
						// response for this record has been built, since in the
						// get data w/ record on device case, it's copied by
						// reference directly into the record descriptor.
						as_bin stack_bins[!get_data || ns->storage_data_in_memory ? 0 : rd.n_bins];

						if (get_data) {
							// Figure out which bins you want - for now, all.
							as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned
							rd.n_bins = as_bin_inuse_count(&rd);
						}

						as_msg_make_response_bufbuilder(bb_r, &rd, !get_data, false, false, btr->binlist);

						as_storage_record_close(&rd);
					}
					as_record_done(&r_ref, ns);
				}
				else {
					// TODO - what about empty records?
					cf_debug(AS_BATCH, "batch_build_response: as_record_get returned %d : key %lx", rec_rv, *(uint64_t *)&bmd->keyd);
					as_msg_make_error_response_bufbuilder(&bmd->keyd, AS_PROTO_RESULT_FAIL_NOT_FOUND, bb_r, ns->name);
				}

				bmd->done = true;

				as_partition_release(&rsv);
			}
			else {
				cf_debug(AS_BATCH, "batch_build_response: partition reserve read failed: rv %d", rv);

				as_msg_make_error_response_bufbuilder(&bmd->keyd, AS_PROTO_RESULT_FAIL_NOT_FOUND, bb_r, ns->name);

				if (other_node != 0) {
					bmd->node = other_node;
					cf_debug(AS_BATCH, "other_node is:  %lx", other_node);
				} else {
					cf_debug(AS_BATCH, "other_node is NULL.");
				}
			}

			yield_count++;
			if (yield_count % g_config.batch_priority == 0) {
				usleep(1);
			}
		}
	}
}

// Send response to client socket.
static int
batch_send(cf_socket *sock, uint8_t* buf, size_t len, int flags)
{
	if (cf_socket_send_all(sock, buf, len, flags,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_BATCH, "batch send response error, errno %d fd %d",
				errno, CSFD(sock));
		return -1;
	}

	return 0;
}

// Send protocol header to the requesting client.
static int
batch_send_header(cf_socket *sock, size_t len)
{
	as_proto proto;
	proto.version = PROTO_VERSION;
	proto.type = PROTO_TYPE_AS_MSG;
	proto.sz = len;
	as_proto_swap(&proto);

	return batch_send(sock, (uint8_t*) &proto, 8, MSG_NOSIGNAL | MSG_MORE);
}

// Send protocol trailer to the requesting client.
static int
batch_send_final(cf_socket *sock, uint32_t result_code)
{
	cl_msg m;
	m.proto.version = PROTO_VERSION;
	m.proto.type = PROTO_TYPE_AS_MSG;
	m.proto.sz = sizeof(as_msg);
	as_proto_swap(&m.proto);
	m.msg.header_sz = sizeof(as_msg);
	m.msg.info1 = 0;
	m.msg.info2 = 0;
	m.msg.info3 = AS_MSG_INFO3_LAST;
	m.msg.unused = 0;
	m.msg.result_code = result_code;
	m.msg.generation = 0;
	m.msg.record_ttl = 0;
	m.msg.transaction_ttl = 0;
	m.msg.n_fields = 0;
	m.msg.n_ops = 0;
	as_msg_swap_header(&m.msg);

	return batch_send(sock, (uint8_t*) &m, sizeof(m), MSG_NOSIGNAL);
}


// Release memory for batch transaction.
static void
batch_transaction_done(batch_transaction* btr, bool force_close)
{
	if (btr->fd_h) {
		as_end_of_transaction(btr->fd_h, force_close);
		btr->fd_h = 0;
	}

	if (btr->digests) {
		cf_free(btr->digests);
		btr->digests = 0;
	}

	if (btr->binlist) {
		cf_vector_destroy(btr->binlist);
		btr->binlist = 0;
	}
}

// Process a batch request.
static void
batch_process_request(batch_transaction* btr)
{
	// Keep the reaper at bay.
	btr->fd_h->last_used = cf_getms();

	cf_buf_builder* bb = 0;
	batch_build_response(btr, &bb);

	cf_socket *sock = &btr->fd_h->sock;
	int brv;

	if (bb) {
		brv = batch_send_header(sock, bb->used_sz);

		if (brv == 0) {
			brv = batch_send(sock, bb->buf, bb->used_sz, MSG_NOSIGNAL | MSG_MORE);

			if (brv == 0) {
				brv = batch_send_final(sock, 0);
			}
		}
		cf_buf_builder_free(bb);
	}
	else {
		cf_info(AS_BATCH, " batch request: returned no local responses");
		brv = batch_send_final(sock, 0);
	}

	batch_transaction_done(btr, brv != 0);
}

// Process one queue's batch requests.
static void
batch_worker(void* udata)
{
	batch_transaction* btr = (batch_transaction*)udata;

	// Check for timeouts.
	if (btr->end_time != 0 && cf_getns() > btr->end_time) {
		cf_atomic64_incr(&g_stats.batch_timeout);

		if (btr->fd_h) {
			as_msg_send_reply(btr->fd_h, AS_PROTO_RESULT_FAIL_TIMEOUT,
					0, 0, 0, 0, 0, btr->ns, btr->trid);
			btr->fd_h = 0;
		}
		batch_transaction_done(btr, false);
		return;
	}

	// Process batch request.
	batch_process_request(btr);
}

// Create bin name list from message.
static cf_vector*
as_binlist_from_op(as_msg* msg)
{
	if (msg->n_ops == 0) {
		return 0;
	}

	cf_vector* binlist = cf_vector_create(AS_ID_BIN_SZ, 5, 0);
	as_msg_op* op = 0;
	int n = 0;
	int len;
	char name[AS_ID_BIN_SZ];

	while ((op = as_msg_op_iterate(msg, op, &n))) {
		len = (op->name_sz <= AS_ID_BIN_SZ - 1)? op->name_sz : AS_ID_BIN_SZ - 1;
		memcpy(name, op->name, len);
		name[len] = 0;
		cf_vector_append(binlist, name);
	}
	return binlist;
}

// Initialize batch queues and worker threads.
int
as_batch_direct_init()
{
	uint32_t threads = g_config.n_batch_threads;
	cf_info(AS_BATCH, "starting %u batch-threads", threads);
	int status = as_thread_pool_init_fixed(&batch_direct_thread_pool, threads, batch_worker, sizeof(batch_transaction), offsetof(batch_transaction,complete));

	if (status) {
		cf_warning(AS_BATCH, "Failed to initialize batch-threads to %u: %d", threads, status);
	}
	return status;
}

// Put batch request on a separate batch queue.
int
as_batch_direct_queue_task(as_transaction* tr, as_namespace *ns)
{
	cf_atomic64_incr(&g_stats.batch_initiate);

	if (g_config.n_batch_threads <= 0) {
		cf_warning(AS_BATCH, "batch-threads has been disabled.");
		return AS_PROTO_RESULT_FAIL_BATCH_DISABLED;
	}

	as_msg* msg = &tr->msgp->msg;

	as_msg_field* dfp = as_msg_field_get(msg, AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY);
	if (! dfp) {
		cf_warning(AS_BATCH, "Batch digests are required.");
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t n_digests = dfp->field_sz / sizeof(cf_digest);

	if (n_digests > g_config.batch_max_requests) {
		cf_warning(AS_BATCH, "Batch request size %u exceeds max %u.", n_digests, g_config.batch_max_requests);
		return AS_PROTO_RESULT_FAIL_BATCH_MAX_REQUESTS;
	}

	batch_transaction btr;
	btr.trid = as_transaction_trid(tr);
	btr.end_time = tr->end_time;
	btr.get_data = !(msg->info1 & AS_MSG_INFO1_GET_NO_BINS);
	btr.complete = false;
	btr.ns = ns;

	// Create the master digest table.
	btr.digests = (batch_digests*) cf_malloc(sizeof(batch_digests) + (sizeof(batch_digest) * n_digests));

	batch_digests* bmd = btr.digests;
	bmd->n_digests = n_digests;
	uint8_t* digest_field_data = dfp->data;

	for (int i = 0; i < n_digests; i++) {
		bmd->digest[i].done = false;
		bmd->digest[i].node = 0;
		memcpy(&bmd->digest[i].keyd, digest_field_data, sizeof(cf_digest));
		digest_field_data += sizeof(cf_digest);
	}

	btr.binlist = as_binlist_from_op(msg);
	btr.fd_h = tr->from.proto_fd_h;
	tr->from.proto_fd_h = NULL;
	btr.fd_h->last_used = cf_getms();

	int status = as_thread_pool_queue_task_fixed(&batch_direct_thread_pool, &btr);

	if (status) {
		cf_warning(AS_BATCH, "Batch enqueue failed");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}
	return 0;
}

int
as_batch_direct_queue_size()
{
	return batch_direct_thread_pool.dispatch_queue? cf_queue_sz(batch_direct_thread_pool.dispatch_queue) : 0;
}

int
as_batch_direct_threads_resize(uint32_t threads)
{
	if (threads > MAX_BATCH_THREADS) {
		cf_warning(AS_BATCH, "batch-threads %u exceeds max %u", threads, MAX_BATCH_THREADS);
		return -1;
	}

	cf_info(AS_BATCH, "Resize batch-threads from %u to %u", g_config.n_batch_threads, threads);
	int status = as_thread_pool_resize(&batch_direct_thread_pool, threads);
	g_config.n_batch_threads = batch_direct_thread_pool.thread_size;

	if (status) {
		cf_warning(AS_BATCH, "Failed to resize batch-threads. status=%d, batch-threads=%d",
				status, g_config.n_batch_threads);
	}
	return status;
}
