/*
 * batch.h
 *
 * Copyright (C) 2008-2015 Aerospike, Inc.
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

#include "base/transaction.h"
#include "dynbuf.h"

typedef struct as_batch_shared_s as_batch_shared;

int as_batch_init();
int as_batch_queue_task(as_transaction* tr);
void as_batch_add_result(as_transaction* tr, uint16_t n_bins, as_bin** bins, as_msg_op** ops);
void as_batch_add_proxy_result(as_batch_shared* shared, uint32_t index, cf_digest* digest, cl_msg* cmsg, size_t size);
void as_batch_add_error(as_batch_shared* shared, uint32_t index, int result_code);
int as_batch_threads_resize(uint32_t threads);
void as_batch_queues_info(cf_dyn_buf* db);
int as_batch_unused_buffers();
void as_batch_destroy();

as_file_handle* as_batch_get_fd_h(as_batch_shared* shared);
