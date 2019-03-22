/*
 * batch.c
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
#include "base/batch.h"
#include "aerospike/as_buffer_pool.h"
#include "aerospike/as_thread_pool.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "cf_mutex.h"
#include "hardware.h"
#include "socket.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>

//---------------------------------------------------------
// MACROS
//---------------------------------------------------------

#define BATCH_BLOCK_SIZE (1024 * 128) // 128K
#define BATCH_MAX_TRANSACTION_SIZE (1024 * 1024 * 10) // 10MB
#define BATCH_REPEAT_SIZE 25  // index(4),digest(20) and repeat(1)

#define BATCH_ABANDON_LIMIT (30UL * 1000 * 1000 * 1000) // 30 seconds

#define BATCH_SUCCESS 0
#define BATCH_ERROR -1
#define BATCH_DELAY 1

//---------------------------------------------------------
// TYPES
//---------------------------------------------------------

// Pad batch input header to 30 bytes which is also the size of a transaction header.
// This allows the input memory to be used as transaction cl_msg memory.
// This saves a large number of memory allocations while allowing different
// namespaces/bin name filters to be in the same batch.
typedef struct {
	uint32_t index;
	cf_digest keyd;
	uint8_t repeat;
	uint8_t info1;
	uint16_t n_fields;
	uint16_t n_ops;
} __attribute__((__packed__)) as_batch_input;

typedef struct {
	uint32_t capacity;
	uint32_t size;
	uint32_t tran_count;
	cf_atomic32 writers;
	as_proto proto;
	uint8_t data[];
} __attribute__((__packed__)) as_batch_buffer;

struct as_batch_shared_s {
	cf_mutex lock;
	cf_queue* response_queue;
	as_file_handle* fd_h;
	cl_msg* msgp;
	as_batch_buffer* buffer;
	uint64_t start;
	uint64_t end;
	uint32_t tran_count_response;
	uint32_t tran_count;
	uint32_t tran_max;
	uint32_t buffer_offset;
	as_batch_buffer* delayed_buffer;
	int result_code;
	bool in_trailer;
	bool bad_response_fd;
};

typedef struct {
	as_batch_shared* shared;
	as_batch_buffer* buffer;
} as_batch_response;

typedef struct {
	cf_queue* response_queue;
	cf_queue* complete_queue;
	cf_atomic32 tran_count;
	uint32_t delay_count;
	volatile bool active;
} as_batch_queue;

typedef struct {
	as_batch_queue* batch_queue;
	bool complete;
} as_batch_work;

//---------------------------------------------------------
// STATIC DATA
//---------------------------------------------------------

static as_thread_pool batch_thread_pool;
static as_buffer_pool batch_buffer_pool;

static as_batch_queue batch_queues[MAX_BATCH_THREADS];
static cf_mutex batch_resize_lock;

//---------------------------------------------------------
// STATIC FUNCTIONS
//---------------------------------------------------------

static int
as_batch_send_error(as_transaction* btr, int result_code)
{
	// Send error back to client for batch transaction that failed before
	// placing any sub-transactions on transaction queue.
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

	cf_socket* sock = &btr->from.proto_fd_h->sock;
	int status = 0;

	// Use blocking send because error occured before batch transaction was
	// placed on batch queue.
	if (cf_socket_send_all(sock, (uint8_t*)&m, sizeof(m), MSG_NOSIGNAL, CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_BATCH, "Batch send response error, errno %d fd %d", errno, CSFD(sock));
		status = -1;
	}

	as_end_of_transaction(btr->from.proto_fd_h, status != 0);
	btr->from.proto_fd_h = NULL;

	cf_free(btr->msgp);
	btr->msgp = 0;

	if (result_code == AS_ERR_TIMEOUT) {
		cf_atomic64_incr(&g_stats.batch_index_timeout);
	}
	else {
		cf_atomic64_incr(&g_stats.batch_index_errors);
	}
	return status;
}

static int
as_batch_send_buffer(as_batch_shared* shared, as_batch_buffer* buffer, int32_t flags)
{
	cf_socket* sock = &shared->fd_h->sock;
	uint8_t* buf = (uint8_t*)&buffer->proto;
	size_t size = sizeof(as_proto) + buffer->size;
	size_t off = shared->buffer_offset;
	size_t remaining = size - off;

	ssize_t sent = cf_socket_try_send_all(sock, buf + off, remaining, flags);

	if (sent < 0) {
		shared->bad_response_fd = true;
		return BATCH_ERROR;
	}

	if (sent < remaining) {
		shared->buffer_offset += sent;
		return BATCH_DELAY;
	}

	return BATCH_SUCCESS;
}

static inline void
as_batch_release_buffer(as_batch_shared* shared, as_batch_buffer* buffer)
{
	if (as_buffer_pool_push_limit(&batch_buffer_pool, buffer, buffer->capacity,
			g_config.batch_max_unused_buffers) != 0) {
		// The push frees buffer on failure, so we just increment stat.
		cf_atomic64_incr(&g_stats.batch_index_destroyed_buffers);
	}
}

static void
as_batch_complete(as_batch_queue* queue, as_batch_shared* shared, int status)
{
	as_end_of_transaction(shared->fd_h, status != 0);
	shared->fd_h = NULL;

	// For now the model is timeouts don't appear in histograms.
	if (shared->result_code != AS_ERR_TIMEOUT) {
		G_HIST_ACTIVATE_INSERT_DATA_POINT(batch_index_hist, shared->start);
	}

	// Check return code in order to update statistics.
	if (status == 0 && shared->result_code == 0) {
		cf_atomic64_incr(&g_stats.batch_index_complete);
	}
	else {
		if (shared->result_code == AS_ERR_TIMEOUT) {
			cf_atomic64_incr(&g_stats.batch_index_timeout);
		}
		else {
			cf_atomic64_incr(&g_stats.batch_index_errors);
		}
	}

	// Destroy lock
	cf_mutex_destroy(&shared->lock);

	// Release memory
	cf_free(shared->msgp);
	cf_free(shared);

	// It's critical that this count is decremented after the transaction is
	// completely finished with the queue because "shutdown threads" relies
	// on this information when performing graceful shutdown.
	cf_atomic32_decr(&queue->tran_count);
}

static bool
as_batch_send_trailer(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer* buffer)
{
	// Use existing buffer to store trailer message.
	as_proto* proto = &buffer->proto;
	proto->version = PROTO_VERSION;
	proto->type = PROTO_TYPE_AS_MSG;
	proto->sz = sizeof(as_msg);
	as_proto_swap(proto);

	as_msg* msg = (as_msg*)buffer->data;
	msg->header_sz = sizeof(as_msg);
	msg->info1 = 0;
	msg->info2 = 0;
	msg->info3 = AS_MSG_INFO3_LAST;
	msg->unused = 0;
	msg->result_code = shared->result_code;
	msg->generation = 0;
	msg->record_ttl = 0;
	msg->transaction_ttl = 0;
	msg->n_fields = 0;
	msg->n_ops = 0;
	as_msg_swap_header(msg);

	buffer->size = sizeof(as_msg);
	shared->buffer_offset = 0;

	int status = as_batch_send_buffer(shared, buffer, MSG_NOSIGNAL);

	if (status == BATCH_DELAY) {
		return false;
	}

	as_batch_release_buffer(shared, buffer);
	as_batch_complete(queue, shared, status);
	return true;
}

static inline bool
as_batch_buffer_end(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer* buffer, int status)
{
	// If we're invoked for the trailer, we're done. Free buffer and batch.
	if (shared->in_trailer) {
		as_batch_release_buffer(shared, buffer);
		as_batch_complete(queue, shared, status);
		return true;
	}

	shared->tran_count_response += buffer->tran_count;

	// We haven't yet reached the last buffer. Free buffer.
	if (shared->tran_count_response < shared->tran_max) {
		as_batch_release_buffer(shared, buffer);
		return true;
	}

	// We've reached the last buffer. If we cannot send a trailer, then we're
	// done. Free buffer and batch.
	if (shared->bad_response_fd) {
		as_batch_release_buffer(shared, buffer);
		as_batch_complete(queue, shared, status);
		return true;
	}

	// Reuse the last buffer for the trailer.
	shared->in_trailer = true;
	return as_batch_send_trailer(queue, shared, buffer);
}

static inline bool
as_batch_abandon(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer* buffer)
{
	if (cf_getns() >= shared->end) {
		cf_warning(AS_BATCH, "abandoned batch from %s with %u transactions after %lu ms",
				shared->fd_h->client, shared->tran_max,
				(shared->end - shared->start) / 1000000);
		shared->bad_response_fd = true;
		as_batch_buffer_end(queue, shared, buffer, BATCH_ERROR);
		return true;
	}

	return false;
}

static bool
as_batch_send_delayed(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer* buffer)
{
	// If we get here, other buffers in this batch can't have affected or
	// reacted to this batch's status. All that happened is this buffer was
	// delayed. Therefore, we don't need to check for error conditions.

	int status = as_batch_send_buffer(shared, buffer, MSG_NOSIGNAL | MSG_MORE);

	if (status == BATCH_DELAY) {
		return false;
	}

	return as_batch_buffer_end(queue, shared, buffer, status);
}

static bool
as_batch_send_response(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer* buffer)
{
	cf_assert(buffer->capacity != 0, AS_BATCH, "buffer capacity 0");

	// Don't send buffer if an error has already occurred.
	if (shared->bad_response_fd || shared->result_code) {
		return as_batch_buffer_end(queue, shared, buffer, BATCH_ERROR);
	}

	shared->buffer_offset = 0;

	// Send buffer block to client socket.
	buffer->proto.version = PROTO_VERSION;
	buffer->proto.type = PROTO_TYPE_AS_MSG;
	buffer->proto.sz = buffer->size;
	as_proto_swap(&buffer->proto);

	int status = as_batch_send_buffer(shared, buffer, MSG_NOSIGNAL | MSG_MORE);

	if (status == BATCH_DELAY) {
		return false;
	}

	return as_batch_buffer_end(queue, shared, buffer, status);
}

static inline void
as_batch_delay_buffer(as_batch_queue* queue)
{
	cf_atomic64_incr(&g_stats.batch_index_delay);

	// If all batch transactions on this thread are delayed, avoid tight loop.
	if (queue->tran_count == queue->delay_count) {
		pthread_yield(); // not cf_thread_yield() - we're using as_thread_pool
	}
}

static void
as_batch_worker(void* udata)
{
	// Send batch data to client, one buffer block at a time.
	as_batch_work* work = (as_batch_work*)udata;
	as_batch_queue* batch_queue = work->batch_queue;
	cf_queue* response_queue = batch_queue->response_queue;
	as_batch_response response;
	as_batch_shared* shared;
	as_batch_buffer* buffer;

	while (cf_queue_pop(response_queue, &response, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// Check if this thread task should end.
		shared = response.shared;
		if (! shared) {
			break;
		}

		buffer = response.buffer;

		if (! shared->delayed_buffer) {
			if (as_batch_send_response(batch_queue, shared, buffer)) {
				continue;
			}

			if (as_batch_abandon(batch_queue, shared, buffer)) {
				continue;
			}

			// Socket blocked.
			shared->delayed_buffer = buffer;
			batch_queue->delay_count++;
			as_batch_delay_buffer(batch_queue);
		}
		else {
			// Batch is delayed - try only original delayed buffer.
			if (shared->delayed_buffer == buffer) {
				shared->delayed_buffer = NULL;

				if (as_batch_send_delayed(batch_queue, shared, buffer)) {
					batch_queue->delay_count--;
					continue;
				}

				if (as_batch_abandon(batch_queue, shared, buffer)) {
					batch_queue->delay_count--;
					continue;
				}

				// Socket blocked again.
				shared->delayed_buffer = buffer;
				as_batch_delay_buffer(batch_queue);
			}
			// else - delayed by another buffer in this batch, just re-queue.
		}

		cf_queue_push(response_queue, &response);
	}

	// Send back completion notification.
	uint32_t complete = 1;
	cf_queue_push(work->batch_queue->complete_queue, &complete);
}

static int
as_batch_create_thread_queues(uint32_t begin, uint32_t end)
{
	// Allocate one queue per batch response worker thread.
	int status = 0;

	as_batch_work work;
	work.complete = false;

	for (uint32_t i = begin; i < end; i++) {
		work.batch_queue = &batch_queues[i];
		work.batch_queue->response_queue = cf_queue_create(sizeof(as_batch_response), true);
		work.batch_queue->complete_queue = cf_queue_create(sizeof(uint32_t), true);
		work.batch_queue->tran_count = 0;
		work.batch_queue->delay_count = 0;
		work.batch_queue->active = true;

		int rc = as_thread_pool_queue_task_fixed(&batch_thread_pool, &work);

		if (rc) {
			cf_warning(AS_BATCH, "Failed to create batch thread %u: %d", i, rc);
			status = rc;
		}
	}
	return status;
}

static bool
as_batch_wait(uint32_t begin, uint32_t end)
{
	for (uint32_t i = begin; i < end; i++) {
		if (batch_queues[i].tran_count > 0) {
			return false;
		}
	}
	return true;
}

static int
as_batch_shutdown_thread_queues(uint32_t begin, uint32_t end)
{
	// Set excess queues to inactive.
	// Existing batch transactions will be allowed to complete.
	for (uint32_t i = begin; i < end; i++) {
		batch_queues[i].active = false;
	}

	// Wait till there are no more active batch transactions on the queues.
	// Timeout after 30 seconds.
	uint64_t limitus = cf_getus() + (1000 * 1000 * 30);
	usleep(50 * 1000);  // Sleep 50ms
	do {
		if (as_batch_wait(begin, end)) {
			break;
		}
		usleep(500 * 1000);  // Sleep 500ms

		if (cf_getus() > limitus) {
			cf_warning(AS_BATCH, "Batch shutdown threads failed on timeout. Transactions remain on queue.");
			// Reactivate queues.
			for (uint32_t i = begin; i < end; i++) {
				batch_queues[i].active = true;
			}
			return -1;
		}
	} while (true);

	// Send stop command to excess queues.
	as_batch_response response;
	memset(&response, 0, sizeof(as_batch_response));

	for (uint32_t i = begin; i < end; i++) {
		cf_queue_push(batch_queues[i].response_queue, &response);
	}

	// Wait for completion events.
	uint32_t complete;
	for (uint32_t i = begin; i < end; i++) {
		as_batch_queue* bq = &batch_queues[i];
		cf_queue_pop(bq->complete_queue, &complete, CF_QUEUE_FOREVER);
		cf_queue_destroy(bq->complete_queue);
		bq->complete_queue = 0;
		cf_queue_destroy(bq->response_queue);
		bq->response_queue = 0;
	}
	return 0;
}

static as_batch_queue*
as_batch_find_queue(int queue_index)
{
	// Search backwards for an active queue.
	for (int index = queue_index - 1; index >= 0; index--) {
		as_batch_queue* bq = &batch_queues[index];

		if (bq->active && cf_queue_sz(bq->response_queue) < g_config.batch_max_buffers_per_queue) {
			return bq;
		}
	}

	// Search forwards.
	for (int index = queue_index + 1; index < MAX_BATCH_THREADS; index++) {
		as_batch_queue* bq = &batch_queues[index];

		// If current queue is not active, future queues will not be active either.
		if (! bq->active) {
			break;
		}

		if (cf_queue_sz(bq->response_queue) < g_config.batch_max_buffers_per_queue) {
			return bq;
		}
	}
	return 0;
}

static as_batch_buffer*
as_batch_buffer_create(uint32_t size)
{
	as_batch_buffer* buffer = cf_malloc(size);
	buffer->capacity = size - batch_buffer_pool.header_size;
	cf_atomic64_incr(&g_stats.batch_index_created_buffers);
	return buffer;
}

static uint8_t*
as_batch_buffer_pop(as_batch_shared* shared, uint32_t size)
{
	as_batch_buffer* buffer;
	uint32_t mem_size = size + batch_buffer_pool.header_size;

	if (mem_size > batch_buffer_pool.buffer_size) {
		// Requested size is greater than fixed buffer size.
		// Allocate new buffer, but don't put back into pool.
		buffer = as_batch_buffer_create(mem_size);
		cf_atomic64_incr(&g_stats.batch_index_huge_buffers);
	}
	else {
		// Pop existing buffer from queue.
		// The extra lock here is unavoidable.
		int status = cf_queue_pop(batch_buffer_pool.queue, &buffer, CF_QUEUE_NOWAIT);

		if (status == CF_QUEUE_OK) {
			buffer->capacity = batch_buffer_pool.buffer_size - batch_buffer_pool.header_size;
		}
		else if (status == CF_QUEUE_EMPTY) {
			// Queue is empty.  Create new buffer.
			buffer = as_batch_buffer_create(batch_buffer_pool.buffer_size);
		}
		else {
			cf_crash(AS_BATCH, "Failed to pop new batch buffer: %d", status);
		}
	}

	// Reserve a slot in new buffer.
	buffer->size = size;
	buffer->tran_count = 1;
	buffer->writers = 2;
	shared->buffer = buffer;
	return buffer->data;
}

static inline void
as_batch_buffer_complete(as_batch_shared* shared, as_batch_buffer* buffer)
{
	// Flush when all writers have finished writing into the buffer.
	if (cf_atomic32_decr(&buffer->writers) == 0) {
		as_batch_response response = {.shared = shared, .buffer = buffer};
		cf_queue_push(shared->response_queue, &response);
	}
}

static uint8_t*
as_batch_reserve(as_batch_shared* shared, uint32_t size, int result_code, as_batch_buffer** buffer_out, bool* complete)
{
	as_batch_buffer* buffer;
	uint8_t* data;

	cf_mutex_lock(&shared->lock);
	*complete = (++shared->tran_count == shared->tran_max);
	buffer = shared->buffer;

	if (! buffer) {
		// No previous buffer.  Get new buffer.
		data = as_batch_buffer_pop(shared, size);
		*buffer_out = shared->buffer;
		cf_mutex_unlock(&shared->lock);
	}
	else if (buffer->size + size <= buffer->capacity) {
		// Result fits into existing block.  Reserve a slot.
		data = buffer->data + buffer->size;
		buffer->size += size;
		buffer->tran_count++;
		cf_atomic32_incr(&buffer->writers);
		*buffer_out = buffer;
		cf_mutex_unlock(&shared->lock);
	}
	else {
		// Result does not fit into existing block.
		// Make copy of existing buffer.
		as_batch_buffer* prev_buffer = buffer;

		// Get new buffer.
		data = as_batch_buffer_pop(shared, size);
		*buffer_out = shared->buffer;
		cf_mutex_unlock(&shared->lock);

		as_batch_buffer_complete(shared, prev_buffer);
	}

	if (! (result_code == AS_OK || result_code == AS_ERR_NOT_FOUND)) {
		// Result code can be set outside of lock because it doesn't matter which transaction's
		// result code is used as long as it's an error.
		shared->result_code = result_code;
	}
	return data;
}

static inline void
as_batch_transaction_end(as_batch_shared* shared, as_batch_buffer* buffer, bool complete)
{
	// This flush can only be triggered when the buffer is full.
	as_batch_buffer_complete(shared, buffer);

	if (complete) {
		// This flush only occurs when all transactions in batch have been processed.
		as_batch_buffer_complete(shared, buffer);
	}
}

static void
as_batch_terminate(as_batch_shared* shared, uint32_t tran_count, int result_code)
{
	// Terminate batch by adding phantom transactions to shared and buffer tran counts.
	// This is done so the memory is released at the end only once.
	as_batch_buffer* buffer;
	bool complete;

	cf_mutex_lock(&shared->lock);
	buffer = shared->buffer;
	shared->result_code = result_code;
	shared->tran_count += tran_count;
	complete = (shared->tran_count == shared->tran_max);

	if (! buffer) {
		// No previous buffer.  Get new buffer.
		as_batch_buffer_pop(shared, 0);
		buffer = shared->buffer;
		buffer->tran_count = tran_count;  // Override tran_count.
	}
	else {
		// Buffer exists. Add phantom transactions.
		buffer->tran_count += tran_count;
		cf_atomic32_incr(&buffer->writers);
	}
	cf_mutex_unlock(&shared->lock);
	as_batch_transaction_end(shared, buffer, complete);
}

//---------------------------------------------------------
// FUNCTIONS
//---------------------------------------------------------

int
as_batch_init()
{
	cf_mutex_init(&batch_resize_lock);

	// Default 'batch-index-threads' can't be set before call to cf_topo_init().
	if (g_config.n_batch_index_threads == 0) {
		g_config.n_batch_index_threads = cf_topo_count_cpus();
	}

	cf_info(AS_BATCH, "starting %u batch-index-threads", g_config.n_batch_index_threads);

	int rc = as_thread_pool_init_fixed(&batch_thread_pool, g_config.n_batch_index_threads, as_batch_worker,
			sizeof(as_batch_work), offsetof(as_batch_work,complete));

	if (rc) {
		cf_warning(AS_BATCH, "Failed to initialize batch-index-threads to %u: %d", g_config.n_batch_index_threads, rc);
		return rc;
	}

	rc = as_buffer_pool_init(&batch_buffer_pool, sizeof(as_batch_buffer), BATCH_BLOCK_SIZE);

	if (rc) {
		cf_warning(AS_BATCH, "Failed to initialize batch buffer pool: %d", rc);
		return rc;
	}

	rc = as_batch_create_thread_queues(0, g_config.n_batch_index_threads);

	if (rc) {
		return rc;
	}

	return 0;
}

int
as_batch_queue_task(as_transaction* btr)
{
	uint64_t counter = cf_atomic64_incr(&g_stats.batch_index_initiate);
	uint32_t thread_size = batch_thread_pool.thread_size;

	if (thread_size == 0 || thread_size > MAX_BATCH_THREADS) {
		cf_warning(AS_BATCH, "batch-index-threads has been disabled: %d", thread_size);
		return as_batch_send_error(btr, AS_ERR_BATCH_DISABLED);
	}
	uint32_t queue_index = counter % thread_size;

	// Validate batch transaction
	as_proto* bproto = &btr->msgp->proto;

	if (bproto->sz > PROTO_SIZE_MAX) {
		cf_warning(AS_BATCH, "can't process message: invalid size %lu should be %d or less",
				(uint64_t)bproto->sz, PROTO_SIZE_MAX);
		return as_batch_send_error(btr, AS_ERR_PARAMETER);
	}

	if (bproto->type != PROTO_TYPE_AS_MSG) {
		cf_warning(AS_BATCH, "Invalid proto type. Expected %d Received %d", PROTO_TYPE_AS_MSG, bproto->type);
		return as_batch_send_error(btr, AS_ERR_PARAMETER);
	}

	// Check that the socket is authenticated.
	uint8_t result = as_security_check(btr->from.proto_fd_h, PERM_NONE);

	if (result != AS_OK) {
		as_security_log(btr->from.proto_fd_h, result, PERM_NONE, NULL, NULL);
		return as_batch_send_error(btr, result);
	}

	// Parse header
	as_msg* bmsg = &btr->msgp->msg;
	as_msg_swap_header(bmsg);

	// Parse fields
	uint8_t* limit = (uint8_t*)bmsg + bproto->sz;
	as_msg_field* mf = (as_msg_field*)bmsg->data;
	as_msg_field* end;
	as_msg_field* bf = 0;

	for (int i = 0; i < bmsg->n_fields; i++) {
		if ((uint8_t*)mf >= limit) {
			cf_warning(AS_BATCH, "Batch field limit reached");
			return as_batch_send_error(btr, AS_ERR_PARAMETER);
		}
		as_msg_swap_field(mf);
		end = as_msg_field_get_next(mf);

		if (mf->type == AS_MSG_FIELD_TYPE_BATCH || mf->type == AS_MSG_FIELD_TYPE_BATCH_WITH_SET) {
			bf = mf;
		}
		mf = end;
	}

	if (! bf) {
		cf_warning(AS_BATCH, "Batch index field not found");
		return as_batch_send_error(btr, AS_ERR_PARAMETER);
	}

	// Parse batch field
	uint8_t* data = bf->data;
	uint32_t tran_count = cf_swap_from_be32(*(uint32_t*)data);
	data += sizeof(uint32_t);

	if (tran_count == 0) {
		cf_warning(AS_BATCH, "Batch request size is zero");
		return as_batch_send_error(btr, AS_ERR_PARAMETER);
	}

	if (tran_count > g_config.batch_max_requests) {
		cf_warning(AS_BATCH, "Batch request size %u exceeds max %u", tran_count, g_config.batch_max_requests);
		return as_batch_send_error(btr, AS_ERR_BATCH_MAX_REQUESTS);
	}

	// Initialize shared data
	as_batch_shared* shared = cf_malloc(sizeof(as_batch_shared));

	memset(shared, 0, sizeof(as_batch_shared));

	cf_mutex_init(&shared->lock);

	// Abandon batch at twice batch timeout if batch timeout is defined.
	// If batch timeout is zero, abandon after 30 seconds.
	shared->end = btr->start_time + ((bmsg->transaction_ttl != 0) ?
			((uint64_t)bmsg->transaction_ttl * 1000 * 1000 * 2) :
			BATCH_ABANDON_LIMIT);

	shared->start = btr->start_time;
	shared->fd_h = btr->from.proto_fd_h;
	shared->msgp = btr->msgp;
	shared->tran_max = tran_count;

	// Find batch queue to send transaction responses.
	as_batch_queue* batch_queue = &batch_queues[queue_index];

	// batch_max_buffers_per_queue is a soft limit, but still must be checked under lock.
	if (! (batch_queue->active && cf_queue_sz(batch_queue->response_queue) < g_config.batch_max_buffers_per_queue)) {
		// Queue buffer limit has been exceeded or thread has been shutdown (probably due to
		// downwards thread resize).  Search for an available queue.
		// cf_warning(AS_BATCH, "Queue %u full %d", queue_index, cf_queue_sz(batch_queue->response_queue));
		batch_queue = as_batch_find_queue(queue_index);

		if (! batch_queue) {
			cf_warning(AS_BATCH, "Failed to find active batch queue that is not full");
			cf_free(shared);
			return as_batch_send_error(btr, AS_ERR_BATCH_QUEUES_FULL);
		}
	}
	// Increment batch queue transaction count.
	cf_atomic32_incr(&batch_queue->tran_count);
	shared->response_queue = batch_queue->response_queue;

	// Initialize generic transaction.
	as_transaction tr;
	as_transaction_init_head(&tr, 0, 0);

	tr.origin = FROM_BATCH;
	tr.from_flags |= FROM_FLAG_BATCH_SUB;
	tr.start_time = btr->start_time;

	// Read batch keys and initialize generic transactions.
	as_batch_input* in;
	cl_msg* out = NULL;
	cl_msg* prev_msgp = NULL;
	as_msg_op* op;
	uint32_t tran_row = 0;
	uint8_t info = *data++;  // allow transaction inline.

	bool allow_inline = (g_config.n_namespaces_inlined != 0 && info);
	bool check_inline = (allow_inline && g_config.n_namespaces_not_inlined != 0);
	bool should_inline = (allow_inline && g_config.n_namespaces_not_inlined == 0);

	// Split batch rows into separate single record read transactions.
	// The read transactions are located in the same memory block as
	// the original batch transactions. This allows us to avoid performing
	// an extra malloc for each transaction.
	while (tran_row < tran_count && data + BATCH_REPEAT_SIZE <= limit) {
		// Copy transaction data before memory gets overwritten.
		in = (as_batch_input*)data;

		tr.from.batch_shared = shared; // is set NULL after sub-transaction
		tr.from_data.batch_index = cf_swap_from_be32(in->index);
		tr.keyd = in->keyd;
		tr.benchmark_time = btr->benchmark_time; // must reset for each usage

		if (in->repeat) {
			if (! prev_msgp) {
				break; // bad bytes from client - repeat set on first item
			}

			// Row should use previous namespace and bin names.
			data += BATCH_REPEAT_SIZE;
			tr.msgp = prev_msgp;
		}
		else {
			tr.msg_fields = 0; // erase previous AS_MSG_FIELD_BIT_SET flag, if any
			as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_NAMESPACE);

			// Row contains full namespace/bin names.
			out = (cl_msg*)data;

			if (data + sizeof(cl_msg) + sizeof(as_msg_field) > limit) {
				break;
			}

			out->msg.header_sz = sizeof(as_msg);
			out->msg.info1 = in->info1;
			out->msg.info2 = 0;
			out->msg.info3 = bmsg->info3 &
					(AS_MSG_INFO3_SC_READ_RELAX | AS_MSG_INFO3_SC_READ_TYPE);
			out->msg.unused = 0;
			out->msg.result_code = 0;
			out->msg.generation = 0;
			out->msg.record_ttl = 0;
			out->msg.transaction_ttl = bmsg->transaction_ttl; // already swapped
			// n_fields/n_ops is in exact same place on both input/output, but the value still
			// needs to be swapped.
			out->msg.n_fields = cf_swap_from_be16(in->n_fields);

			// Older clients sent zero, but always sent namespace.  Adjust this.
			if (out->msg.n_fields == 0) {
				out->msg.n_fields = 1;
			}

			out->msg.n_ops = cf_swap_from_be16(in->n_ops);

			// Namespace input is same as namespace field, so just leave in place and swap.
			data += sizeof(cl_msg);
			mf = (as_msg_field*)data;
			as_msg_swap_field(mf);
			if (check_inline) {
				as_namespace* ns = as_namespace_get_bymsgfield(mf);
				should_inline = ns && ns->storage_data_in_memory;
			}
			mf = as_msg_field_get_next(mf);
			data = (uint8_t*)mf;

			// Swap remaining fields.
			for (uint16_t j = 1; j < out->msg.n_fields; j++) {
				if (data + sizeof(as_msg_field) > limit) {
					goto TranEnd;
				}

				if (mf->type == AS_MSG_FIELD_TYPE_SET) {
					as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_SET);
				}

				as_msg_swap_field(mf);
				mf = as_msg_field_get_next(mf);
				data = (uint8_t*)mf;
			}

			if (out->msg.n_ops) {
				// Bin names input is same as transaction ops, so just leave in place and swap.
				uint16_t n_ops = out->msg.n_ops;
				for (uint16_t j = 0; j < n_ops; j++) {
					if (data + sizeof(as_msg_op) > limit) {
						goto TranEnd;
					}
					op = (as_msg_op*)data;
					as_msg_swap_op(op);
					op = as_msg_op_get_next(op);
					data = (uint8_t*)op;
				}
			}

			// Initialize msg header.
			out->proto.version = PROTO_VERSION;
			out->proto.type = PROTO_TYPE_AS_MSG;
			out->proto.sz = (data - (uint8_t*)&out->msg);
			tr.msgp = out;
			prev_msgp = out;
		}

		if (data > limit) {
			break;
		}

		// Submit transaction.
		if (should_inline) {
			as_tsvc_process_transaction(&tr);
		}
		else {
			// Queue transaction to be processed by a transaction thread.
			as_tsvc_enqueue(&tr);
		}
		tran_row++;
	}

TranEnd:
	if (tran_row < tran_count) {
		// Mismatch between tran_count and actual data.  Terminate transaction.
		cf_warning(AS_BATCH, "Batch keys mismatch. Expected %u Received %u", tran_count, tran_row);
		as_batch_terminate(shared, tran_count - tran_row, AS_ERR_PARAMETER);
	}

	// Reset original socket because socket now owned by batch shared.
	btr->from.proto_fd_h = NULL;
	return 0;
}

void
as_batch_add_result(as_transaction* tr, uint16_t n_bins, as_bin** bins,
		as_msg_op** ops)
{
	as_namespace* ns = tr->rsv.ns;

	// Calculate size.
	size_t size = sizeof(as_msg);
	size += sizeof(as_msg_field) + sizeof(cf_digest);

	uint16_t n_fields = 1;

	for (uint16_t i = 0; i < n_bins; i++) {
		as_bin* bin = bins[i];
		size += sizeof(as_msg_op);

		if (ops) {
			size += ops[i]->name_sz;
		}
		else if (bin) {
			size += ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(ns, bin->id));
		}
		else {
			cf_crash(AS_BATCH, "making response message with null bin and op");
		}

		if (bin) {
			size += as_bin_particle_client_value_size(bin);
		}
	}

	as_batch_shared* shared = tr->from.batch_shared;

	if (size > BATCH_MAX_TRANSACTION_SIZE) {
		cf_warning(AS_BATCH, "Record size %zu exceeds max %d", size, BATCH_MAX_TRANSACTION_SIZE);
		as_batch_add_error(shared, tr->from_data.batch_index, AS_ERR_RECORD_TOO_BIG);
		return;
	}

	as_batch_buffer* buffer;
	bool complete;
	uint8_t* data = as_batch_reserve(shared, size, tr->result_code, &buffer, &complete);

	if (data) {
		// Write header.
		uint8_t* p = data;
		as_msg* m = (as_msg*)p;
		m->header_sz = sizeof(as_msg);
		m->info1 = 0;
		m->info2 = 0;
		m->info3 = 0;
		m->unused = 0;
		m->result_code = tr->result_code;
		m->generation = plain_generation(tr->generation, ns);
		m->record_ttl = tr->void_time;

		// Overload transaction_ttl to store batch index.
		m->transaction_ttl = tr->from_data.batch_index;

		m->n_fields = n_fields;
		m->n_ops = n_bins;
		as_msg_swap_header(m);
		p += sizeof(as_msg);

		as_msg_field* field = (as_msg_field*)p;
		field->field_sz = sizeof(cf_digest) + 1;
		field->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
		memcpy(field->data, &tr->keyd, sizeof(cf_digest));
		as_msg_swap_field(field);
		p += sizeof(as_msg_field) + sizeof(cf_digest);

		for (uint16_t i = 0; i < n_bins; i++) {
			as_bin* bin = bins[i];
			as_msg_op* op = (as_msg_op*)p;
			op->op = AS_MSG_OP_READ;
			op->version = 0;

			if (ops) {
				as_msg_op* src = ops[i];
				memcpy(op->name, src->name, src->name_sz);
				op->name_sz = src->name_sz;
			}
			else {
				op->name_sz = as_bin_memcpy_name(ns, op->name, bin);
			}

			op->op_sz = OP_FIXED_SZ + op->name_sz;
			p += sizeof(as_msg_op) + op->name_sz;
			p += as_bin_particle_to_client(bin, op);
			as_msg_swap_op(op);
		}
	}
	as_batch_transaction_end(shared, buffer, complete);
}

void
as_batch_add_proxy_result(as_batch_shared* shared, uint32_t index, cf_digest* digest, cl_msg* cmsg, size_t proxy_size)
{
	as_msg* msg = &cmsg->msg;
	size_t size = proxy_size + sizeof(as_msg_field) + sizeof(cf_digest) - sizeof(as_proto);

	if (size > BATCH_MAX_TRANSACTION_SIZE) {
		cf_warning(AS_BATCH, "Record size %zu exceeds max %d", size, BATCH_MAX_TRANSACTION_SIZE);
		as_batch_add_error(shared, index, AS_ERR_RECORD_TOO_BIG);
		return;
	}

	as_batch_buffer* buffer;
	bool complete;
	uint8_t* data = as_batch_reserve(shared, size, msg->result_code, &buffer, &complete);

	if (data) {
		// Overload transaction_ttl to store batch index.
		msg->transaction_ttl = htonl(index);

		// Write header
		uint16_t n_fields = ntohs(msg->n_fields);
		msg->n_fields = htons(n_fields + 1);
		memcpy(data, msg, sizeof(as_msg));
		uint8_t* trg = data + sizeof(as_msg);

		// Write digest field
		as_msg_field* field = (as_msg_field*)trg;
		field->field_sz = sizeof(cf_digest) + 1;
		field->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
		memcpy(field->data, digest, sizeof(cf_digest));
		as_msg_swap_field(field);
		trg += sizeof(as_msg_field) + sizeof(cf_digest);

		// Copy others fields and ops.
		size = ((uint8_t*)cmsg + proxy_size) - msg->data;
		memcpy(trg, msg->data, size);
	}
	as_batch_transaction_end(shared, buffer, complete);
}

void
as_batch_add_error(as_batch_shared* shared, uint32_t index, int result_code)
{
	as_batch_buffer* buffer;
	bool complete;
	uint8_t* data = as_batch_reserve(shared, sizeof(as_msg), result_code, &buffer, &complete);

	if (data) {
		// Write error.
		as_msg* m = (as_msg*)data;
		m->header_sz = sizeof(as_msg);
		m->info1 = 0;
		m->info2 = 0;
		m->info3 = 0;
		m->unused = 0;
		m->result_code = result_code;
		m->generation = 0;
		m->record_ttl = 0;
		// Overload transaction_ttl to store batch index.
		m->transaction_ttl = index;
		m->n_fields = 0;
		m->n_ops = 0;
		as_msg_swap_header(m);
	}
	as_batch_transaction_end(shared, buffer, complete);
}

int
as_batch_threads_resize(uint32_t threads)
{
	if (threads > MAX_BATCH_THREADS) {
		cf_warning(AS_BATCH, "batch-index-threads %u exceeds max %u", threads, MAX_BATCH_THREADS);
		return -1;
	}

	cf_mutex_lock(&batch_resize_lock);

	// Resize thread pool.  The threads will wait for graceful shutdown on downwards resize.
	uint32_t threads_orig = batch_thread_pool.thread_size;
	cf_info(AS_BATCH, "Resize batch-index-threads from %u to %u", threads_orig, threads);
	int status = 0;

	if (threads != threads_orig) {
		if (threads > threads_orig) {
			// Increase threads before initializing queues.
			status = as_thread_pool_resize(&batch_thread_pool, threads);

			if (status == 0) {
				g_config.n_batch_index_threads = threads;
				// Adjust queues to match new thread size.
				status = as_batch_create_thread_queues(threads_orig, threads);
			}
			else {
				// Show warning, but keep going as some threads may have been successfully added/removed.
				cf_warning(AS_BATCH, "Failed to resize batch-index-threads. status=%d, batch-index-threads=%u",
						status, g_config.n_batch_index_threads);
				threads = batch_thread_pool.thread_size;

				if (threads > threads_orig) {
					g_config.n_batch_index_threads = threads;
					// Adjust queues to match new thread size.
					status = as_batch_create_thread_queues(threads_orig, threads);
				}
			}
		}
		else {
			// Shutdown queues before shutting down threads.
			status = as_batch_shutdown_thread_queues(threads, threads_orig);

			if (status == 0) {
				// Adjust threads to match new queue size.
				status = as_thread_pool_resize(&batch_thread_pool, threads);
				g_config.n_batch_index_threads = batch_thread_pool.thread_size;

				if (status) {
					cf_warning(AS_BATCH, "Failed to resize batch-index-threads. status=%d, batch-index-threads=%u",
							status, g_config.n_batch_index_threads);
				}
			}
		}
	}
	cf_mutex_unlock(&batch_resize_lock);
	return status;
}

void
as_batch_queues_info(cf_dyn_buf* db)
{
	cf_mutex_lock(&batch_resize_lock);

	uint32_t max = batch_thread_pool.thread_size;

	for (uint32_t i = 0; i < max; i++) {
		if (i > 0) {
			cf_dyn_buf_append_char(db, ',');
		}
		as_batch_queue* bq = &batch_queues[i];
		cf_dyn_buf_append_uint32(db, bq->tran_count);  // Batch count
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_int(db, cf_queue_sz(bq->response_queue));  // Buffer count
	}
	cf_mutex_unlock(&batch_resize_lock);
}

int
as_batch_unused_buffers()
{
	return cf_queue_sz(batch_buffer_pool.queue);
}

// Not currently called.  Put in this place holder in case server decides to
// implement clean shutdowns in the future.
void
as_batch_destroy()
{
	as_thread_pool_destroy(&batch_thread_pool);
	as_buffer_pool_destroy(&batch_buffer_pool);

	cf_mutex_lock(&batch_resize_lock);
	as_batch_shutdown_thread_queues(0, batch_thread_pool.thread_size);
	cf_mutex_unlock(&batch_resize_lock);
	cf_mutex_destroy(&batch_resize_lock);
}

as_file_handle*
as_batch_get_fd_h(as_batch_shared* shared)
{
	return shared->fd_h;
}
