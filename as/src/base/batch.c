/*
 * batch.c
 *
 * Copyright (C) 2012-2020 Aerospike, Inc.
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
#include "aerospike/as_atomic.h"
#include "aerospike/as_buffer_pool.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/service.h"
#include "base/stats.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "transaction/rw_utils.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "hardware.h"
#include "log.h"
#include "socket.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>

//---------------------------------------------------------
// MACROS
//---------------------------------------------------------

#define BATCH_BLOCK_SIZE (1024 * 128) // 128K
#define BATCH_REPEAT_SIZE 25  // index(4),digest(20) and repeat(1)

#define BATCH_ABANDON_LIMIT (30UL * 1000 * 1000 * 1000) // 30 seconds

#define BATCH_SUCCESS 0
#define BATCH_ERROR -1
#define BATCH_DELAY 1

// Batch parent flags.
#define INLINE_DATA_IN_MEMORY   0x1 // or data in pmem
#define INLINE_DATA_ON_DEVICE   0x2
#define REPORT_ALL_ERRORS       0x4
#define REPORT_ERROR_REC        0x8

// as_batch_input repeat (or not) flags.
#define REPEAT          0x1
#define HAS_EXTRA_INFO  0x2
#define HAS_GENERATION  0x4
#define HAS_RECORD_TTL  0x8
#define HAS_INFO4       0x10

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
	uint8_t variable_meta[];
} __attribute__((__packed__)) as_batch_input;

typedef struct {
	uint32_t capacity;
	uint32_t size;
	uint32_t tran_count;
	uint32_t writers;
	int result_code;
	uint32_t unused;
	as_proto proto;
	uint8_t data[];
} __attribute__((__packed__)) as_batch_buffer;

struct as_batch_shared_s {
	cf_mutex lock;
	int result_code;
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
	bool report_all_errors;
	bool report_error_rec;
	bool in_trailer;
	bool bad_response_fd;
	bool compress_response;
	as_msg_field* predexp_mf;
	as_exp* predexp;
	void* extra_msgps;
};

typedef struct {
	as_batch_shared* shared;
	as_batch_buffer* buffer;
} as_batch_response;

typedef struct {
	cf_queue* response_queue;
	cf_queue* complete_queue;
	uint32_t tran_count;
	uint32_t delay_count;
	volatile bool active;
} as_batch_queue;

typedef struct {
	as_batch_queue* batch_queue;
	bool complete;
} as_batch_work;

//--------------------------------------
// thread_pool class.
//

typedef void (*task_fn)(void* user_data);

typedef struct thread_pool_s {
	cf_queue* dispatch_q;
	cf_queue* complete_q;
	task_fn do_task;
	uint32_t task_size;
	uint32_t task_complete_offset;
	uint32_t n_threads;
} thread_pool;

static void thread_pool_init(thread_pool* pool, uint32_t n_threads, task_fn fn, uint32_t task_size, uint32_t task_complete_offset);
static void thread_pool_resize(thread_pool* pool, uint32_t n_threads);
static bool thread_pool_queue_task(thread_pool* pool, void* task);

static uint32_t thread_pool_create_threads(thread_pool* pool, uint32_t n_threads);
static void thread_pool_shutdown_threads(thread_pool* pool, uint32_t n_threads);
static void* run_pool_worker(void* data);

//---------------------------------------------------------
// STATIC DATA
//---------------------------------------------------------

static thread_pool batch_thread_pool;
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
	m.msg.info4 = 0;
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
		as_incr_uint64(&g_stats.batch_index_timeout);
	}
	else {
		as_incr_uint64(&g_stats.batch_index_errors);
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
		as_incr_uint64(&g_stats.batch_index_destroyed_buffers);
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
		as_incr_uint64(&g_stats.batch_index_complete);
	}
	else {
		if (shared->result_code == AS_ERR_TIMEOUT) {
			as_incr_uint64(&g_stats.batch_index_timeout);
		}
		else {
			as_incr_uint64(&g_stats.batch_index_errors);
		}
	}

	// Destroy lock
	cf_mutex_destroy(&shared->lock);

	// Release memory
	destroy_batch_extra_msgps(shared->extra_msgps);
	as_exp_destroy(shared->predexp);
	cf_free(shared->msgp);
	cf_free(shared);

	// It's critical that this count is decremented after the transaction is
	// completely finished with the queue because "shutdown threads" relies
	// on this information when performing graceful shutdown.
	// TODO - ARM TSO plugin - will need release semantic? Or new pattern?
	uint32_t tc = as_aaf_uint32(&queue->tran_count, -1);

	cf_assert(tc != (uint32_t)-1, AS_BATCH, "tran_count underflow");
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
	msg->info4 = 0;
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
as_batch_send_response(as_batch_queue* queue, as_batch_shared* shared, as_batch_buffer** p_buffer)
{
	as_batch_buffer* buffer = *p_buffer;

	cf_assert(buffer->capacity != 0, AS_BATCH, "buffer capacity 0");

	// Don't send buffer if an error has already occurred.
	if (shared->bad_response_fd || shared->result_code) {
		return as_batch_buffer_end(queue, shared, buffer, BATCH_ERROR);
	}

	if (shared->report_error_rec) {
		// If error, ensures this is the last buffer sent.
		shared->result_code = buffer->result_code;
	}

	shared->buffer_offset = 0;

	// Send buffer block to client socket.
	buffer->proto.version = PROTO_VERSION;
	buffer->proto.type = PROTO_TYPE_AS_MSG;
	buffer->proto.sz = buffer->size;
	as_proto_swap(&buffer->proto);

	if (shared->compress_response) {
		size_t proto_sz = sizeof(as_proto) + buffer->size;
		uint8_t* buf = as_proto_compress_alloc((const uint8_t*)buffer,
				batch_buffer_pool.header_size + buffer->capacity,
				offsetof(as_batch_buffer, proto), &proto_sz,
				&g_stats.batch_comp_stat);

		if (buf != (uint8_t*)buffer) {
			cf_free(buffer);

			buffer = (as_batch_buffer*)buf;
			buffer->size = proto_sz - sizeof(as_proto);

			*p_buffer = buffer;
		}
	}

	int status = as_batch_send_buffer(shared, buffer, MSG_NOSIGNAL | MSG_MORE);

	if (status == BATCH_DELAY) {
		return false;
	}

	return as_batch_buffer_end(queue, shared, buffer, status);
}

static inline void
as_batch_delay_buffer(as_batch_queue* queue)
{
	as_incr_uint64(&g_stats.batch_index_delay);

	// If all batch transactions on this thread are delayed, avoid tight loop.
	if (queue->tran_count == queue->delay_count) {
		cf_thread_yield();
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

		if (! shared->delayed_buffer) {
			if (as_batch_send_response(batch_queue, shared, &response.buffer)) {
				continue;
			}

			buffer = response.buffer;

			if (as_batch_abandon(batch_queue, shared, buffer)) {
				continue;
			}

			// Socket blocked.
			shared->delayed_buffer = buffer;
			batch_queue->delay_count++;
			as_batch_delay_buffer(batch_queue);
		}
		else {
			buffer = response.buffer;

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

		if (! thread_pool_queue_task(&batch_thread_pool, &work)) {
			cf_warning(AS_BATCH, "No batch threads running: %u", i);
			status = -1;
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
	as_incr_uint64(&g_stats.batch_index_created_buffers);
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
		as_incr_uint64(&g_stats.batch_index_huge_buffers);
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
	buffer->result_code = 0;
	shared->buffer = buffer;
	return buffer->data;
}

static inline void
as_batch_buffer_complete(as_batch_shared* shared, as_batch_buffer* buffer)
{
	// TODO - ARM TSO plugin - will need release semantic.
	uint32_t n_writers = as_aaf_uint32(&buffer->writers, -1);

	cf_assert(n_writers != (uint32_t)-1, AS_BATCH, "writers underflow");

	// Flush when all writers have finished writing into the buffer.
	if (n_writers == 0) {
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
		as_incr_uint32(&buffer->writers);
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

	if (! shared->report_all_errors &&
			! (result_code == AS_OK || result_code == AS_ERR_NOT_FOUND ||
					result_code == AS_ERR_FILTERED_OUT)) {
		if (shared->report_error_rec) {
			// When this buffer is processed, result_code transfers to shared,
			// which will ensure it's the last buffer in the response.
			(*buffer_out)->result_code = result_code;
		}
		else {
			shared->result_code = result_code;
		}
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
		as_incr_uint32(&buffer->writers);
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

		if (g_config.n_batch_index_threads > MAX_BATCH_THREADS) {
			g_config.n_batch_index_threads = MAX_BATCH_THREADS;
		}
	}

	cf_info(AS_BATCH, "starting %u batch-index-threads", g_config.n_batch_index_threads);

	thread_pool_init(&batch_thread_pool, g_config.n_batch_index_threads, as_batch_worker,
			sizeof(as_batch_work), offsetof(as_batch_work,complete));

	int rc = as_buffer_pool_init(&batch_buffer_pool, sizeof(as_batch_buffer), BATCH_BLOCK_SIZE);

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
	uint64_t counter = as_aaf_uint64(&g_stats.batch_index_initiate, 1);
	uint32_t thread_size = batch_thread_pool.n_threads;

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
	uint8_t result = as_security_check_auth(btr->from.proto_fd_h);

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
	as_msg_field* predexp_mf = 0;

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
		else if (mf->type == AS_MSG_FIELD_TYPE_PREDEXP) {
			predexp_mf = mf;
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

	uint32_t max_requests = as_load_uint32(&g_config.batch_max_requests);

	if (max_requests != 0 && tran_count > max_requests) {
		cf_warning(AS_BATCH, "Batch request size %u exceeds max %u", tran_count,
				max_requests);
		return as_batch_send_error(btr, AS_ERR_BATCH_MAX_REQUESTS);
	}

	uint8_t parent_flags = *data++;
	bool inline_dim = (parent_flags & INLINE_DATA_IN_MEMORY) != 0;
	bool inline_dev = (parent_flags & INLINE_DATA_ON_DEVICE) != 0;

	// Initialize shared data
	as_batch_shared* shared = cf_malloc(sizeof(as_batch_shared));

	memset(shared, 0, sizeof(as_batch_shared));

	cf_mutex_init(&shared->lock);

	// Abandon batch at batch timeout if batch timeout is defined.
	// If batch timeout is zero, abandon after 30 seconds.
	shared->end = btr->start_time + (bmsg->transaction_ttl != 0 ?
			(uint64_t)bmsg->transaction_ttl * 1000 * 1000 :
			BATCH_ABANDON_LIMIT);

	shared->start = btr->start_time;
	shared->fd_h = btr->from.proto_fd_h;
	shared->msgp = btr->msgp;
	shared->compress_response = as_transaction_compress_response(btr);
	shared->tran_max = tran_count;
	shared->report_all_errors = (parent_flags & REPORT_ALL_ERRORS) != 0;
	shared->report_error_rec = (parent_flags & REPORT_ERROR_REC) != 0;

	// Find batch queue to send transaction responses.
	as_batch_queue* batch_queue = &batch_queues[queue_index];

	// batch_max_buffers_per_queue is a soft limit, but still must be checked under lock.
	if (! (batch_queue->active && cf_queue_sz(batch_queue->response_queue) < g_config.batch_max_buffers_per_queue)) {
		// Queue buffer limit has been exceeded or thread has been shutdown (probably due to
		// downwards thread resize).  Search for an available queue.
		// cf_warning(AS_BATCH, "Queue %u full %d", queue_index, cf_queue_sz(batch_queue->response_queue));
		batch_queue = as_batch_find_queue(queue_index);

		if (! batch_queue) {
			cf_ticker_warning(AS_BATCH, "failed to find active batch queue that is not full");
			cf_free(shared);
			return as_batch_send_error(btr, AS_ERR_BATCH_QUEUES_FULL);
		}
	}

	if (predexp_mf != NULL) {
		shared->predexp_mf = predexp_mf;

		if ((shared->predexp = as_exp_filter_build(predexp_mf, true)) == NULL) {
			cf_warning(AS_BATCH, "Failed to build batch predexp");
			cf_free(shared);
			return as_batch_send_error(btr, AS_ERR_PARAMETER);
		}
	}

	g_stats.batch_rec_count_hist_active = true;
	histogram_insert_raw(g_stats.batch_rec_count_hist, tran_count);

	// Increment batch queue transaction count.
	as_incr_uint32(&batch_queue->tran_count);
	shared->response_queue = batch_queue->response_queue;

	// Initialize generic transaction.
	as_transaction tr;
	as_transaction_init_head(&tr, 0, 0);

	tr.origin = FROM_BATCH;
	tr.from_flags |= FROM_FLAG_BATCH_SUB;
	tr.start_time = btr->start_time;

	// Read batch keys and initialize generic transactions.
	cl_msg* prev_msgp = NULL;
	uint32_t tran_row = 0;

	as_namespace* ns = NULL; // namespace of current sub-transaction

	// Split batch rows into separate single record read transactions.
	// The read transactions are located in the same memory block as
	// the original batch transactions. This allows us to avoid performing
	// an extra malloc for each transaction.
	while (tran_row < tran_count && data + BATCH_REPEAT_SIZE <= limit) {
		// Copy transaction data before memory gets overwritten.
		as_batch_input* in = (as_batch_input*)data;

		tr.from.batch_shared = shared; // is set NULL after sub-transaction
		tr.from_data.batch_index = cf_swap_from_be32(in->index);
		tr.keyd = in->keyd;

		if ((in->repeat & REPEAT) != 0) {
			if (! prev_msgp) {
				break; // bad bytes from client - repeat set on first item
			}

			if (as_transaction_has_key(&tr)) {
				cf_warning(AS_BATCH, "batch must not repeat key");
				break;
			}

			// Row should use previous namespace and bin names.
			data += BATCH_REPEAT_SIZE;
			tr.msgp = prev_msgp;
		}
		else {
			tr.msg_fields = 0; // erase previous AS_MSG_FIELD_BIT_SET flag, if any
			as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_NAMESPACE);

			bool has_extra_info = (in->repeat & HAS_EXTRA_INFO) != 0;
			bool has_info4 = (in->repeat & HAS_INFO4) != 0;
			bool has_generation = (in->repeat & HAS_GENERATION) != 0;
			bool has_record_ttl = (in->repeat & HAS_RECORD_TTL) != 0;
			uint32_t cl_msg_indent = (has_extra_info ? 2 : 0) +
					(has_info4 ? 1 : 0) +
					(has_generation ? sizeof(uint16_t) : 0) +
					(has_record_ttl ? sizeof(uint32_t) : 0);

			// Row contains full namespace/bin names.
			cl_msg* out = (cl_msg*)(data + cl_msg_indent);

			if ((uint8_t*)out + sizeof(cl_msg) + sizeof(as_msg_field) > limit) {
				break;
			}

			data = in->variable_meta;

			out->msg.header_sz = sizeof(as_msg);
			out->msg.info1 = *data++ &
					// Compressed sub-transactions won't work - and proxied
					// sub-transactions wouldn't ignore flag if set - clear it.
					(~AS_MSG_INFO1_COMPRESS_RESPONSE) &
					// XDR sub-transactions (likely) won't work - clear flag.
					(~AS_MSG_INFO1_XDR);

			if (has_extra_info) {
				// Note - no checks - irrelevant flags will be ignored during
				// sub-transactions. Currently, none will do harm if present.
				out->msg.info2 = *data++;
				out->msg.info3 = *data++;
			}
			else { // old client - needs SC read flags from parent
				out->msg.info2 = 0;
				out->msg.info3 = bmsg->info3 &
						(AS_MSG_INFO3_SC_READ_RELAX | AS_MSG_INFO3_SC_READ_TYPE);
			}

			out->msg.info4 = has_info4 ? *data++ : 0;
			out->msg.result_code = 0;

			if (has_generation) {
				out->msg.generation = cf_swap_from_be16(*(uint16_t*)data);
				data += sizeof(uint16_t);
			}
			else {
				out->msg.generation = 0;
			}

			if (has_record_ttl) {
				out->msg.record_ttl = cf_swap_from_be32(*(uint32_t*)data);
				data += sizeof(uint32_t);
			}
			else {
				out->msg.record_ttl = 0;
			}

			out->msg.transaction_ttl = bmsg->transaction_ttl; // already swapped

			// n_fields/n_ops is in exact same place on both input/output, but the value still
			// needs to be swapped.
			out->msg.n_fields = cf_swap_from_be16(*(uint16_t*)data);
			data += sizeof(uint16_t);

			// Older clients sent zero, but always sent namespace.  Adjust this.
			if (out->msg.n_fields == 0) {
				out->msg.n_fields = 1;
			}

			out->msg.n_ops = cf_swap_from_be16(*(uint16_t*)data);
			data += sizeof(uint16_t);

			// Namespace input is same as namespace field, so just leave in place and swap.
			mf = (as_msg_field*)data;
			as_msg_swap_field(mf);

			// Set "current" namespace.
			if ((ns = as_namespace_get_bymsgfield(mf)) == NULL) {
				cf_warning(AS_BATCH, "batch request has unknown namespace");
				break;
			}

			mf = as_msg_field_get_next(mf);
			data = (uint8_t*)mf;

			// Swap remaining fields.
			for (uint16_t j = 1; j < out->msg.n_fields; j++) {
				if (data + sizeof(as_msg_field) > limit) {
					goto TranEnd;
				}

				// Note - no checks - irrelevant fields will be ignored during
				// sub-transactions. Currently, none will do harm if present.
				as_transaction_set_msg_field_flag(&tr, mf->type);

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

					as_msg_op* op = (as_msg_op*)data;

					// Swap can touch metadata bytes beyond as_msg_op struct.
					if (as_msg_op_get_value_p(op) > limit) {
						goto TranEnd;
					}

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

			if (as_transaction_is_delete(&tr)) {
				// If durable & 'ship-bin-luts', generate bin cemetery.
				convert_batched_to_write(ns, &tr, &shared->extra_msgps);
			}

			prev_msgp = tr.msgp;
		}

		if (data > limit) {
			break;
		}

		if (ns->batch_sub_benchmarks_enabled) {
			tr.benchmark_time = histogram_insert_data_point(
					ns->batch_sub_prestart_hist, tr.start_time);
		}
		else {
			tr.benchmark_time = 0;
		}

		// Submit transaction.
		if (tran_count == 1 || (ns->storage_type == AS_STORAGE_ENGINE_SSD ?
				inline_dev : inline_dim)) {
			as_tsvc_process_transaction(&tr);
		}
		else {
			// Queue transaction to be processed by a transaction thread.
			as_service_enqueue_internal(&tr);
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
		as_msg_op** ops, as_record_version* v)
{
	as_namespace* ns = tr->rsv.ns;

	// Calculate size.
	uint16_t n_fields = 0;
	size_t size = sizeof(as_msg);

	if (v != NULL) {
		n_fields++;
		size += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	for (uint16_t i = 0; i < n_bins; i++) {
		as_bin* bin = bins[i];
		size += sizeof(as_msg_op);

		if (ops) {
			size += ops[i]->name_sz;
		}
		else if (bin) {
			size += strlen(bin->name);
		}
		else {
			cf_crash(AS_BATCH, "making response message with null bin and op");
		}

		if (bin) {
			size += as_bin_particle_client_value_size(bin);
		}
	}

	as_batch_shared* shared = tr->from.batch_shared;

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
		m->info4 = 0;
		m->result_code = tr->result_code;
		m->generation = plain_generation(tr->generation, ns);
		m->record_ttl = tr->void_time;

		// Overload transaction_ttl to store batch index.
		m->transaction_ttl = tr->from_data.batch_index;

		m->n_fields = n_fields;
		m->n_ops = n_bins;
		as_msg_swap_header(m);

		p += sizeof(as_msg);

		if (v != NULL) {
			as_msg_field *mf = (as_msg_field *)p;

			mf->field_sz = 1 + sizeof(as_record_version);
			mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
			*(as_record_version *)mf->data = *v;
			as_msg_swap_field(mf);
			p += sizeof(as_msg_field) + sizeof(as_record_version);
		}

		for (uint16_t i = 0; i < n_bins; i++) {
			as_bin* bin = bins[i];
			as_msg_op* op = (as_msg_op*)p;
			op->op = AS_MSG_OP_READ;
			op->has_lut = 0;
			op->unused_flags = 0;

			if (ops) {
				as_msg_op* src = ops[i];
				memcpy(op->name, src->name, src->name_sz);
				op->name_sz = src->name_sz;
			}
			else {
				op->name_sz = as_bin_memcpy_name(op->name, bin);
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
as_batch_add_made_result(as_batch_shared* shared, uint32_t index, cl_msg* msgp,
		size_t msg_sz)
{
	as_msg* m = &msgp->msg;
	size_t sz = msg_sz - sizeof(as_proto);

	as_batch_buffer* buffer;
	bool complete;
	uint8_t* data = as_batch_reserve(shared, sz, m->result_code, &buffer,
			&complete);

	if (data != NULL) {
		m->transaction_ttl = cf_swap_to_be32(index);
		memcpy(data, m, sz);
	}

	as_batch_transaction_end(shared, buffer, complete);
}

// TODO - could also re-do this with explicit params to cover errors?
void
as_batch_add_ack(as_transaction* tr, as_record_version* v)
{
	// Calculate size.
	uint16_t n_fields = 0;
	size_t size = sizeof(as_msg);

	if (v != NULL) {
		n_fields++;
		size += sizeof(as_msg_field) + sizeof(as_record_version);
	}

	as_batch_shared* shared = tr->from.batch_shared;
	as_batch_buffer* buffer;
	bool complete;
	uint8_t* data = as_batch_reserve(shared, size, tr->result_code, &buffer,
			&complete);

	if (data != NULL) {
		as_msg* m = (as_msg*)data;
		m->header_sz = sizeof(as_msg);
		m->info1 = 0;
		m->info2 = 0;
		m->info3 = 0;
		m->info4 = 0;
		m->result_code = tr->result_code;
		m->generation = plain_generation(tr->generation, tr->rsv.ns);
		m->record_ttl = tr->void_time;
		// Overload transaction_ttl to store batch index.
		m->transaction_ttl = tr->from_data.batch_index;
		m->n_fields = n_fields;
		m->n_ops = 0;
		as_msg_swap_header(m);

		uint8_t* p = m->data;

		if (v != NULL) {
			as_msg_field *mf = (as_msg_field *)p;

			mf->field_sz = 1 + sizeof(as_record_version);
			mf->type = AS_MSG_FIELD_TYPE_RECORD_VERSION;
			*(as_record_version *)mf->data = *v;
			as_msg_swap_field(mf);
//			p += sizeof(as_msg_field) + sizeof(as_record_version);
		}
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
		m->info4 = 0;
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
	uint32_t threads_orig = batch_thread_pool.n_threads;
	cf_info(AS_BATCH, "Resize batch-index-threads from %u to %u", threads_orig, threads);
	int status = 0;

	if (threads != threads_orig) {
		if (threads > threads_orig) {
			// Increase threads before initializing queues.
			thread_pool_resize(&batch_thread_pool, threads);

			g_config.n_batch_index_threads = threads;
			// Adjust queues to match new thread size.
			status = as_batch_create_thread_queues(threads_orig, threads);
		}
		else {
			// Shutdown queues before shutting down threads.
			status = as_batch_shutdown_thread_queues(threads, threads_orig);

			if (status == 0) {
				// Adjust threads to match new queue size.
				thread_pool_resize(&batch_thread_pool, threads);

				g_config.n_batch_index_threads = batch_thread_pool.n_threads;
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

	uint32_t max = batch_thread_pool.n_threads;

	for (uint32_t i = 0; i < max; i++) {
		if (i > 0) {
			cf_dyn_buf_append_char(db, ',');
		}
		as_batch_queue* bq = &batch_queues[i];
		cf_dyn_buf_append_uint32(db, bq->tran_count);  // Batch count
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_uint32(db, cf_queue_sz(bq->response_queue));  // Buffer count
	}
	cf_mutex_unlock(&batch_resize_lock);
}

uint32_t
as_batch_unused_buffers()
{
	return cf_queue_sz(batch_buffer_pool.queue);
}

as_file_handle*
as_batch_get_fd_h(as_batch_shared* shared)
{
	return shared->fd_h;
}

as_msg_field*
as_batch_get_predexp_mf(as_batch_shared* shared)
{
	return shared->predexp_mf;
}

as_exp*
as_batch_get_predexp(as_batch_shared* shared)
{
	return shared->predexp;
}


//==========================================================
// thread_pool class.
//

static void
thread_pool_init(thread_pool* pool, uint32_t n_threads, task_fn fn,
		uint32_t task_size, uint32_t task_complete_offset)
{
	// Initialize queues.
	pool->dispatch_q = cf_queue_create(task_size, true);
	pool->complete_q = cf_queue_create(sizeof(uint32_t), true);
	pool->do_task = fn;
	pool->task_size = task_size;
	pool->task_complete_offset = task_complete_offset;

	// Start detached threads.
	pool->n_threads = thread_pool_create_threads(pool, n_threads);
}

// Called only via 'set-config' command, which is already synchronized.
static void
thread_pool_resize(thread_pool* pool, uint32_t n_threads)
{
	if (n_threads == pool->n_threads) {
		return;
	}

	if (n_threads < pool->n_threads) {
		// Shutdown excess threads.
		uint32_t threads_to_shutdown = pool->n_threads - n_threads;

		// Set pool n_threads before shutting down threads because we want to
		// disallow new tasks onto thread pool when n_threads is set to zero.
		// Therefore, set n_threads as early as possible.
		//
		// Note: There still is a slight possibility that new tasks can still be
		// queued after disabling thread pool because the n_threads check is not
		// done under lock. These tasks will either timeout or be suspended
		// until thread pool is resized to > 0 threads.
		pool->n_threads = n_threads;

		thread_pool_shutdown_threads(pool, threads_to_shutdown);
	}
	else {
		// Start new threads.
		pool->n_threads += thread_pool_create_threads(pool,
				n_threads - pool->n_threads);
	}
}

static bool
thread_pool_queue_task(thread_pool* pool, void* task)
{
	if (pool->n_threads == 0) {
		// No threads are running to process task.
		return false;
	}

	cf_queue_push(pool->dispatch_q, task);

	return true;
}

static uint32_t
thread_pool_create_threads(thread_pool* pool, uint32_t n_threads)
{
	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_create_transient(run_pool_worker, pool);
	}

	return n_threads;
}

static void
thread_pool_shutdown_threads(thread_pool* pool, uint32_t n_threads)
{
	// Send shutdown signal.
	char* task = alloca(pool->task_size);

	memset(task, 0, pool->task_size);
	*(bool*)(task + pool->task_complete_offset) = true;

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_queue_push(pool->dispatch_q, task);
	}

	// Wait till threads finish.
	uint32_t complete;

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_queue_pop(pool->complete_q, &complete, CF_QUEUE_FOREVER);
	}
}

static void*
run_pool_worker(void* data)
{
	thread_pool* pool = (thread_pool*)data;

	char* task = alloca(pool->task_size);
	bool* shutdown = (bool*)(task + pool->task_complete_offset);

	// Retrieve tasks from queue and execute.
	while (cf_queue_pop(pool->dispatch_q, task, CF_QUEUE_FOREVER) ==
			CF_QUEUE_OK) {
		// Check if thread should be shut down.
		if (*shutdown) {
			break;
		}

		// Run task.
		pool->do_task(task);
	}

	// Send thread completion event back to caller.
	uint32_t complete = 1;

	cf_queue_push(pool->complete_q, &complete);

	return NULL;
}
