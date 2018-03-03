/*
 * thr_query.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

/*
 * QUERY Engine Defaults
 */
// **************************************************************************************************
#define QUERY_BATCH_SIZE              100
#define AS_MAX_NUM_SCRIPT_PARAMS      10
#define AS_QUERY_BUF_SIZE             1024 * 1024 * 2 // At least 2 Meg
#define AS_QUERY_MAX_BUFS             256	// That makes it 512 meg max in steady state
#define AS_QUERY_MAX_QREQ             1024	// this is 4 kb
#define AS_QUERY_MAX_QTR_POOL		  128	// They are 4MB+ each ...
#define AS_QUERY_MAX_THREADS          32
#define AS_QUERY_MAX_WORKER_THREADS   15 * AS_QUERY_MAX_THREADS
#define AS_QUERY_MAX_QREQ_INFLIGHT    100	// worker queue capping per query
#define AS_QUERY_MAX_QUERY            500	// 32 MB be little generous for now!!
#define AS_QUERY_MAX_SHORT_QUEUE_SZ   500	// maximum 500 outstanding short running queries
#define AS_QUERY_MAX_LONG_QUEUE_SZ    500	// maximum 500 outstanding long  running queries
#define AS_QUERY_MAX_UDF_TRANSACTIONS 20	// Higher the value more aggressive it will be
#define AS_QUERY_UNTRACKED_TIME       1000 // (millisecond) 1 sec
#define AS_QUERY_WAIT_MAX_TRAN_US     1000
// **************************************************************************************************
