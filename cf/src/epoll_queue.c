/*
 * epoll_queue.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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

#include "epoll_queue.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"

#include "log.h"
#include "socket.h"


//==========================================================
// Forward declarations.
//

static void resize_queue(cf_epoll_queue* q);
static void unwrap_queue(cf_epoll_queue* q);


//==========================================================
// Inlines & macros.
//

#define Q_N_ELES(_q) (_q->write_pos - _q->read_pos)
#define Q_ELE_PTR(_q, _i) (&_q->eles[(_i % _q->capacity) * _q->ele_sz])
#define Q_EMPTY(_q) (_q->write_pos == _q->read_pos)


//==========================================================
// Public API.
//

void
cf_epoll_queue_init(cf_epoll_queue* q, uint32_t ele_sz, uint32_t capacity)
{
	q->poll_data_type = CF_POLL_DATA_EPOLL_QUEUE;

	q->event_fd = eventfd(0, EFD_NONBLOCK);

	if (q->event_fd < 0) {
		cf_crash(CF_MISC, "eventfd() failed: %d (%s)", errno,
				cf_strerror(errno));
	}

	q->read_pos = 0;
	q->write_pos = 0;

	q->capacity = capacity;
	q->ele_sz = ele_sz;
	q->eles = cf_malloc(capacity * ele_sz);
}

void
cf_epoll_queue_destroy(cf_epoll_queue* q)
{
	CF_NEVER_FAILS(close(q->event_fd));
	cf_free(q->eles);
}

void
cf_epoll_queue_push(cf_epoll_queue* q, const void* ele)
{
	if (Q_N_ELES(q) == q->capacity) {
		resize_queue(q);
	}

	memcpy(Q_ELE_PTR(q, q->write_pos), ele, q->ele_sz);
	q->write_pos++;

	unwrap_queue(q);

	if (Q_N_ELES(q) == 1) {
		uint64_t val = 1;

		if (write(q->event_fd, &val, sizeof(val)) < 0) {
			cf_crash(CF_MISC, "write() failed: %d (%s)", errno,
					cf_strerror(errno));
		}
	}
}

bool
cf_epoll_queue_pop(cf_epoll_queue* q, void* ele)
{
	if (Q_EMPTY(q)) {
		return false;
	}

	memcpy(ele, Q_ELE_PTR(q, q->read_pos), q->ele_sz);
	q->read_pos++;

	if (Q_EMPTY(q)) {
		q->read_pos = 0;
		q->write_pos = 0;

		uint64_t val;

		if (read(q->event_fd, &val, sizeof(val)) < 0) {
			cf_crash(CF_MISC, "read() failed: %d (%s)", errno,
					cf_strerror(errno));
		}
	}

	return true;
}


//==========================================================
// Local helpers.
//

static void
resize_queue(cf_epoll_queue* q)
{
	uint32_t new_capacity = q->capacity * 2;
	uint32_t read_ix = q->read_pos % q->capacity;

	if (read_ix == 0) {
		q->eles = cf_realloc(q->eles, new_capacity * q->ele_sz);
	}
	else {
		uint8_t* new_eles = cf_malloc(new_capacity * q->ele_sz);

		uint32_t end_sz = (q->capacity - read_ix) * q->ele_sz;
		uint32_t total_sz = q->capacity * q->ele_sz;

		memcpy(new_eles, Q_ELE_PTR(q, q->read_pos), end_sz);
		memcpy(new_eles + end_sz, q->eles, total_sz - end_sz);

		cf_free(q->eles);
		q->eles = new_eles;
	}

	q->read_pos = 0;
	q->write_pos = q->capacity;
	q->capacity = new_capacity;
}

static void
unwrap_queue(cf_epoll_queue* q)
{
	if ((q->write_pos & 0xc0000000) == 0) {
		return;
	}

	uint32_t n_eles = Q_N_ELES(q);

	q->read_pos %= q->capacity;
	q->write_pos = q->read_pos + n_eles;
}
