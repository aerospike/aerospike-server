/*
 * epoll_queue.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

typedef struct cf_epoll_queue_s {
	uint8_t poll_data_type; // one of CF_POLL_DATA_* - must be first

	int32_t event_fd;

	uint32_t read_pos;
	uint32_t write_pos;

	uint32_t capacity;
	uint32_t ele_sz;
	uint8_t* eles;
} cf_epoll_queue;


//==========================================================
// Public API.
//

void cf_epoll_queue_init(cf_epoll_queue* q, uint32_t ele_sz, uint32_t capacity);
void cf_epoll_queue_destroy(cf_epoll_queue* q);
void cf_epoll_queue_push(cf_epoll_queue* q, const void* ele);
bool cf_epoll_queue_pop(cf_epoll_queue* q, void* ele);
