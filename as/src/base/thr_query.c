/*
 * thr_query.c
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

/*
 * This code is responsible for the query execution. Each query received
 * query transaction for the query threads to execute. Query has two parts
 * a) Generator  : This query the Aerospike Index B-tree and creates the digest list and
 *                 queues it up for LOOKUP / UDF / AGGREGATION
 * b) Aggregator : This does required processing of the record and send back
 *                 response to the clients.
 *                 LOOKUP:      Read the record from the disk and based on the
 *                              records selected by query packs it into the buffer
 *                              and returns it back to the client
 *                 UDF:         Reads the record from the disk and based on the
 *                              query applies UDF and packs the result back into
 *                              the buffer and returns it back to the client.
 *                 AGGREGATION: Creates istream(on the digstlist) and ostream(
 *                              over the network buffer) and applies aggregator
 *                              functions. For a single query this can be called
 *                              multiple times. The istream interface takes care
 *                              of partition reservation / record opening/ closing
 *                              and object lock synchronization. Whole of which
 *                              is driven by as_stream_read / as_stream_write from
 *                              inside aggregation UDF. ostream keeps sending by
 *                              batched result to the client.
 *
 *  Please note all these parts can either be performed under single thread
 *  context or by different set of threads. For the namespace with data on disk
 *  I/O is performed separately in different set of I/O pools
 *
 *  Flow of code looks like
 *
 *  1. thr_tsvc()
 *
 *                 ---------------------------------> query_generator
 *                /                                      /|\      |
 *  as_query -----                                        |       |   qtr released
 * (sets up qtr)  \   qtr reserved                        |      \|/
 *                 ----------------> g_query_q ------> query_th
 *
 *
 *  2. Query Threads
 *                          ---------------------------------> qwork_process
 *                        /                                          /|\      |
 *  query_generator --                                                |       |  qtr released
 *  (sets up qwork)        \  qtr reserved                            |      \|/
 *                          --------------> g_query_work_queue -> query_th
 *
 *
 *
 *  3. I/O threads
 *                                query_process_ioreq  --> query_io
 *                               /
 *  qwork_process -----------------query_process_udfreq --> internal txn
 *                               \
 *                                query_process_aggreq --> ag_aggr_process
 *
 *  (Releases all the resources qtr and qwork if allocated)
 *
 *  A query may be single thread execution or a multi threaded application. In the
 *  single thread execution all the functions are called in the single thread context
 *  and no queue is involved. In case of multi thread context qtr is setup by thr_tsvc
 *  and which is picked up by the query threads which could either service it in single
 *  thread or queue up to the I/O worker thread (done generally in case of data on ssd)
 *
 */

#include "base/thr_query.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/time.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_list.h"
#include "aerospike/as_map.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_stream.h"
#include "aerospike/as_string.h"
#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"
#include "aerospike/mod_lua.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_rchash.h"

#include "ai_btree.h"
#include "bt.h"
#include "bt_iterator.h"

#include "base/aggr.h"
#include "base/as_stap.h"
#include "base/datamodel.h"
#include "base/predexp.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/stats.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_memtracker.h"
#include "base/udf_record.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "transaction/udf.h"


/*
 * Query Transaction State
 */
// **************************************************************************************************
typedef enum {
	AS_QTR_STATE_INIT     = 0,
	AS_QTR_STATE_RUNNING  = 1,
	AS_QTR_STATE_ABORT    = 2,
	AS_QTR_STATE_ERR      = 3,
	AS_QTR_STATE_DONE     = 4,
} qtr_state;
// **************************************************************************************************

/*
 * Query Transcation Type
 */
// **************************************************************************************************
typedef enum {
	QUERY_TYPE_LOOKUP  = 0,
	QUERY_TYPE_AGGR    = 1,
	QUERY_TYPE_UDF_BG  = 2,
	QUERY_TYPE_UDF_FG  = 3,

	QUERY_TYPE_UNKNOWN  = -1
} query_type;



/*
 * Query Transaction Structure
 */
// **************************************************************************************************
typedef struct as_query_transaction_s {

	/*
 	* MT (Read Only) No protection required
 	*/
	/************************** Query Parameter ********************************/
	uint64_t                 trid;
	as_namespace           * ns;
	char                   * setname;
	as_sindex              * si;
	as_sindex_range        * srange;
	query_type               job_type;  // Job type [LOOKUP/AGG/UDF]
	bool                     no_bin_data;
	predexp_eval_t         * predexp_eval;
	cf_vector              * binlist;
	as_file_handle         * fd_h;      // ref counted nonetheless
	/************************** Run Time Data *********************************/
	bool                     blocking;
	uint32_t                 priority;
	uint64_t                 start_time;               // Start time
	uint64_t                 end_time;                 // timeout value

	/*
 	* MT (Single Writer / Single Threaded / Multiple Readers)
 	* Atomics or no Protection
 	*/
	/****************** Stats (only generator) ***********************/
	uint64_t                 querying_ai_time_ns;  // Time spent by query to run lookup secondary index trees.
	uint32_t                 n_digests;            // Digests picked by from secondary index
											   	   // including record read
	bool                     short_running;
	bool                     track;

	/*
 	* MT (Multiple Writers)
 	* These fields are either needs to be atomic or protected by lock.
 	*/
	/****************** Stats (worker threads) ***********************/
	cf_atomic64              n_result_records;     // Number of records returned as result
												   // if aggregation returns 1 record count
												   // is 1, irrelevant of number of record
												   // being touched.
	cf_atomic64              net_io_bytes;
	cf_atomic64              n_read_success;

	/********************** Query Progress ***********************************/
	cf_atomic32              n_qwork_active;
	cf_atomic32              n_io_outstanding;
	cf_atomic32              n_udf_tr_queued;    				// Throttling: max in flight scan

	/********************* Net IO packet order *******************************/
	cf_atomic32              netio_push_seq;
	cf_atomic32              netio_pop_seq;

	/********************** IO Buf Builder ***********************************/
	pthread_mutex_t          buf_mutex;
	cf_buf_builder         * bb_r;
	/****************** Query State and Result Code **************************/
	pthread_mutex_t          slock;
	bool                     do_requeue;
	qtr_state                state;
	int                      result_code;

    /********************* Fields Not Memzeroed **********************
	*
	* Empirically, some of the following fields *still* require memzero
	* initialization. Please test with a memset(qtr, 0xff, sizeof(*qtr))
	* right after allocation before you initialize before moving them
	* into the uninitialized section.
	*
	* NB: Read Only or Single threaded
	*/
	struct ai_obj            bkey;
	as_aggr_call             agg_call; // Stream UDF Details
	iudf_origin              origin;   // Record UDF Details
	bool                     is_durable_delete; // enterprise only
	as_sindex_qctx           qctx;     // Secondary Index details
	as_partition_reservation * rsv;
} as_query_transaction;
// **************************************************************************************************



/*
 * Query Request Type
 */
// **************************************************************************************************
typedef enum {
	QUERY_WORK_TYPE_NONE   = -1, // Request for I/O
	QUERY_WORK_TYPE_LOOKUP =  0, // Request for I/O
	QUERY_WORK_TYPE_AGG    =  1, // Request for Aggregation
	QUERY_WORK_TYPE_UDF_BG =  2, // Request for running UDF on query result
} query_work_type;
// **************************************************************************************************


/*
 * Query Request
 */
// **************************************************************************************************
typedef struct query_work_s {
	query_work_type        type;
	as_query_transaction * qtr;
	cf_ll                * recl;
	uint64_t               queued_time_ns;
} query_work;
// **************************************************************************************************


/*
 * Job Monitoring
 */
// **************************************************************************************************
typedef struct query_jobstat_s {
	int               index;
	as_mon_jobstat ** jobstat;
	int               max_size;
} query_jobstat;
// **************************************************************************************************

/*
 * Skey list
 */
// **************************************************************************************************
typedef struct qtr_skey_s {
	as_query_transaction * qtr;
	as_sindex_key        * skey;
} qtr_skey;
// **************************************************************************************************


/*
 * Query Engine Global
 */
// **************************************************************************************************
static int              g_current_queries_count = 0;
static pthread_rwlock_t g_query_lock
						= PTHREAD_RWLOCK_WRITER_NONRECURSIVE_INITIALIZER_NP;
static cf_rchash      * g_query_job_hash = NULL;
// Buf Builder Pool
static cf_queue       * g_query_response_bb_pool  = 0;
static cf_queue       * g_query_qwork_pool         = 0;
pthread_mutex_t         g_query_pool_mutex = PTHREAD_MUTEX_INITIALIZER;
as_query_transaction  * g_query_pool_head = NULL;
size_t                  g_query_pool_count = 0;
//
// GENERATOR
static pthread_t        g_query_threads[AS_QUERY_MAX_THREADS];
static pthread_attr_t   g_query_th_attr;
static cf_queue       * g_query_short_queue     = 0;
static cf_queue       * g_query_long_queue      = 0;
static cf_atomic32      g_query_threadcnt       = 0;

cf_atomic32             g_query_short_running   = 0;
cf_atomic32             g_query_long_running    = 0;

// I/O & AGGREGATOR
static pthread_t       g_query_worker_threads[AS_QUERY_MAX_WORKER_THREADS];
static pthread_attr_t  g_query_worker_th_attr;
static cf_queue     *  g_query_work_queue    = 0;
static cf_atomic32     g_query_worker_threadcnt = 0;
// **************************************************************************************************

/*
 * Extern Functions
 */
// **************************************************************************************************

extern cf_vector * as_sindex_binlist_from_msg(as_namespace *ns, as_msg *msgp, int * numbins);

// **************************************************************************************************

/*
 * Forward Declaration
 */
// **************************************************************************************************

static void qtr_finish_work(as_query_transaction *qtr, cf_atomic32 *stat, char *fname, int lineno, bool release);

// **************************************************************************************************

/*
 * Histograms
 */
// **************************************************************************************************
histogram * query_txn_q_wait_hist;               // Histogram to track time spend in trasaction queue. Transaction
												 // queue backing, it is busy. Check if query in transaction is
												 // true from query perspective.
histogram * query_query_q_wait_hist;             // Histogram to track time spend waiting in queue for query thread.
												 // Query queue backing up. Try increasing query thread in case CPU is
												 // not fully utilized or if system is not IO bound
histogram * query_prepare_batch_hist;            // Histogram to track time spend while preparing batches. Secondary index
												 // slow. Check batch is too big
histogram * query_batch_io_q_wait_hist;          // Histogram to track time spend waiting in queue for worker thread.
histogram * query_batch_io_hist;                 // Histogram to track time spend doing I/O per batch. This includes
												 // priority based sleep after n units of work.
												 // For above two Query worker thread busy if not IO bound then try bumping
												 // up the priority. Query thread may be yielding too much.
histogram * query_net_io_hist;                   // Histogram to track time spend sending results to client. Network problem!!
												 // or client too slow

#define QUERY_HIST_INSERT_DATA_POINT(type, start_time_ns)              \
do {                                                                   \
	if (g_config.query_enable_histogram && start_time_ns != 0) {       \
		if (type) {                                                    \
			histogram_insert_data_point(type, start_time_ns);          \
		}                                                              \
	}                                                                  \
} while(0);

#define QUERY_HIST_INSERT_RAW(type, time_ns)                      \
do {                                                                   \
	if (g_config.query_enable_histogram && time_ns != 0) {             \
		if (type) {                                                    \
			histogram_insert_raw(type, time_ns);                       \
		}                                                              \
	}                                                                  \
} while(0);

// **************************************************************************************************


/*
 * Query Locks
 */
// **************************************************************************************************
static void
qtr_lock(as_query_transaction *qtr) {
	if (qtr) {
		pthread_mutex_lock(&qtr->slock);
	}
}
static void
qtr_unlock(as_query_transaction *qtr) {
	if (qtr) {
		pthread_mutex_unlock(&qtr->slock);
	}
}
// **************************************************************************************************


/*
 * Query Transaction Pool
 */
// **************************************************************************************************
static as_query_transaction *
qtr_alloc()
{
	pthread_mutex_lock(&g_query_pool_mutex);

	as_query_transaction * qtr;

	if (!g_query_pool_head) {
		qtr = cf_rc_alloc(sizeof(as_query_transaction));
	} else {
		qtr = g_query_pool_head;
		g_query_pool_head = * (as_query_transaction **) qtr;
		--g_query_pool_count;
		cf_rc_reserve(qtr);
	}

	pthread_mutex_unlock(&g_query_pool_mutex);
	return qtr;
}

static void
qtr_free(as_query_transaction * qtr)
{
	pthread_mutex_lock(&g_query_pool_mutex);

	if (g_query_pool_count >= AS_QUERY_MAX_QTR_POOL) {
		cf_rc_free(qtr);
	}
	else {
		// Use the initial location as a next pointer.
		* (as_query_transaction **) qtr = g_query_pool_head;
		g_query_pool_head = qtr;
		++g_query_pool_count;
	}

	pthread_mutex_unlock(&g_query_pool_mutex);
}
// **************************************************************************************************


/*
 * Bufbuilder buffer pool
 */
// **************************************************************************************************
static int
bb_poolrelease(cf_buf_builder *bb_r)
{
	int ret = AS_QUERY_OK;
	if ((cf_queue_sz(g_query_response_bb_pool) > g_config.query_bufpool_size)
			|| g_config.query_buf_size != cf_buf_builder_size(bb_r)) {
		cf_detail(AS_QUERY, "Freed Buffer of Size %zu with", bb_r->alloc_sz + sizeof(as_msg));
		cf_buf_builder_free(bb_r);
	} else {
		cf_detail(AS_QUERY, "Pushed %p %"PRIu64" %d ", bb_r, g_config.query_buf_size, cf_buf_builder_size(bb_r));
		cf_queue_push(g_query_response_bb_pool, &bb_r);
	}
	return ret;
}

static cf_buf_builder *
bb_poolrequest()
{
	cf_buf_builder *bb_r;
	int rv = cf_queue_pop(g_query_response_bb_pool, &bb_r, CF_QUEUE_NOWAIT);
	if (rv == CF_QUEUE_EMPTY) {
		bb_r = cf_buf_builder_create_size(g_config.query_buf_size);
		if (!bb_r) {
			cf_crash(AS_QUERY, "Allocation Error in Buf builder Pool !!");
		}
	} else if (rv == CF_QUEUE_OK) {
		bb_r->used_sz = 0;
		cf_detail(AS_QUERY, "Popped %p", bb_r);
	} else {
		cf_warning(AS_QUERY, "Failed to find response buffer in the pool%d", rv);
		return NULL;
	}
	return bb_r;
};
// **************************************************************************************************

/*
 * Query Request Pool
 */
// **************************************************************************************************
static int
qwork_poolrelease(query_work *qwork)
{
	if (!qwork) return AS_QUERY_OK;
	qwork->qtr   = 0;
	qwork->type  = QUERY_WORK_TYPE_NONE;

	int ret = AS_QUERY_OK;
	if (cf_queue_sz(g_query_qwork_pool) < AS_QUERY_MAX_QREQ) {
		cf_detail(AS_QUERY, "Pushed qwork %p", qwork);
		cf_queue_push(g_query_qwork_pool, &qwork);
	} else {
		cf_detail(AS_QUERY, "Freed qwork %p", qwork);
		cf_free(qwork);
	}
	if (ret != CF_QUEUE_OK) ret = AS_QUERY_ERR;
	return ret;
}

static query_work *
qwork_poolrequest()
{
	query_work *qwork = NULL;
	int rv = cf_queue_pop(g_query_qwork_pool, &qwork, CF_QUEUE_NOWAIT);
	if (rv == CF_QUEUE_EMPTY) {
		qwork = cf_malloc(sizeof(query_work));
		memset(qwork, 0, sizeof(query_work));
	} else if (rv != CF_QUEUE_OK) {
		cf_warning(AS_QUERY, "Failed to find query work in the pool");
		return NULL;
	}
	qwork->qtr   = 0;
	qwork->type  = QUERY_WORK_TYPE_NONE;
	return qwork;
};
// **************************************************************************************************


/*
 * Query State set/get function
 */
// **************************************************************************************************
static void
qtr_set_running(as_query_transaction *qtr) {
	qtr_lock(qtr);
	if (qtr->state == AS_QTR_STATE_INIT) {
		qtr->state       = AS_QTR_STATE_RUNNING;
	} else {
		cf_crash(AS_QUERY, "Invalid Query state %d while moving to running state ...", qtr->state);
	}
	qtr_unlock(qtr);
}

/*
 * Query in non init state (picked up by generator) means it is
 * running. Could be RUNNING/ABORT/FAIL/DONE
 */
static bool
qtr_started(as_query_transaction *qtr) {
	qtr_lock(qtr);
	bool started = false;
	if (qtr->state != AS_QTR_STATE_INIT) {
		started = true;
	}
	qtr_unlock(qtr);
	return started;
}

static void
qtr_set_abort(as_query_transaction *qtr, int result_code, char *fname, int lineno)
{
	qtr_lock(qtr);
	if (qtr->state == AS_QTR_STATE_RUNNING
		|| qtr->state == AS_QTR_STATE_DONE) {
		cf_debug(AS_QUERY, "Query %p Aborted at %s:%d", qtr, fname, lineno);
		qtr->state       = AS_QTR_STATE_ABORT;
		qtr->result_code = result_code;
	}
	qtr_unlock(qtr);
}

static void
qtr_set_err(as_query_transaction *qtr, int result_code, char *fname, int lineno)
{
	qtr_lock(qtr);
	if (qtr->state == AS_QTR_STATE_RUNNING) {
		cf_debug(AS_QUERY, "Query %p Error at %s:%d", qtr, fname, lineno);
		qtr->state       = AS_QTR_STATE_ERR;
		qtr->result_code = result_code;
	}
	qtr_unlock(qtr);
}

static void
qtr_set_done(as_query_transaction *qtr, int result_code, char *fname, int lineno)
{
	qtr_lock(qtr);
	if (qtr->state == AS_QTR_STATE_RUNNING) {
		cf_debug(AS_QUERY, "Query %p Done at %s:%d", qtr, fname, lineno);
		qtr->state       = AS_QTR_STATE_DONE;
		qtr->result_code = result_code;
	}
	qtr_unlock(qtr);
}

static bool
qtr_failed(as_query_transaction *qtr)
{
	qtr_lock(qtr);
	bool abort = false;
	if ((qtr->state == AS_QTR_STATE_ABORT)
		 || (qtr->state == AS_QTR_STATE_ERR)) {
		abort = true;
	}
	qtr_unlock(qtr);
	return abort;
}

static bool
qtr_is_abort(as_query_transaction *qtr)
{
	qtr_lock(qtr);
	bool abort = false;
	if (qtr->state == AS_QTR_STATE_ABORT) {
		abort = true;
	}
	qtr_unlock(qtr);
	return abort;
}


static bool
qtr_finished(as_query_transaction *qtr)
{
	qtr_lock(qtr);
	bool finished = false;
	if ((qtr->state == AS_QTR_STATE_DONE)
		|| (qtr->state == AS_QTR_STATE_ERR)
		|| (qtr->state == AS_QTR_STATE_ABORT)) {
		finished = true;
	}
	qtr_unlock(qtr);
	return finished;
}

static void
query_check_timeout(as_query_transaction *qtr)
{
	if ((qtr)
		&& (qtr->end_time != 0)
		&& (cf_getns() > qtr->end_time)) {
		cf_debug(AS_QUERY, "Query Timed-out %lu %lu", cf_getns(), qtr->end_time);
		qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_TIMEOUT, __FILE__, __LINE__);
	}
}
// **************************************************************************************************


/*
 * Query Destructor Function
 */
// **************************************************************************************************
static void
query_release_prereserved_partitions(as_query_transaction * qtr)
{
	if (!qtr) {
		cf_warning(AS_QUERY, "qtr is NULL");
		return;
	}
	if (qtr->qctx.partitions_pre_reserved) {
		for (int i=0; i<AS_PARTITIONS; i++) {
			if (qtr->qctx.can_partition_query[i]) {
				as_partition_release(&qtr->rsv[i]);
			}
		}
		if (qtr->rsv) {
			cf_free(qtr->rsv);
		}
	}
}

/*
 * NB: These stats come into picture only if query really started
 * running. If it fails before even running it is accounted in
 * fail
 */
static inline void
query_update_stats(as_query_transaction *qtr)
{
	uint64_t rows = cf_atomic64_get(qtr->n_result_records);

	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&qtr->ns->n_lookup_abort);
			} else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&(qtr->si->stats.lookup_errs));
				cf_atomic64_incr(&qtr->ns->n_lookup_errs);
			}
			if (!qtr_failed(qtr))
				cf_atomic64_incr(&qtr->ns->n_lookup_success);
			cf_atomic64_incr(&qtr->si->stats.n_lookup);
			cf_atomic64_add(&qtr->si->stats.lookup_response_size, qtr->net_io_bytes);
			cf_atomic64_add(&qtr->si->stats.lookup_num_records, rows);
			cf_atomic64_add(&qtr->ns->lookup_response_size, qtr->net_io_bytes);
			cf_atomic64_add(&qtr->ns->lookup_num_records, rows);
			break;

		case QUERY_TYPE_AGGR:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&qtr->ns->n_agg_abort);
			} else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&(qtr->si->stats.agg_errs));
				cf_atomic64_incr(&qtr->ns->n_agg_errs);
			}
			if (!qtr_failed(qtr))
				cf_atomic64_incr(&qtr->ns->n_agg_success);
			cf_atomic64_incr(&qtr->si->stats.n_aggregation);
			cf_atomic64_add(&qtr->si->stats.agg_response_size, qtr->net_io_bytes);
			cf_atomic64_add(&qtr->si->stats.agg_num_records, rows);
			cf_atomic64_add(&qtr->ns->agg_response_size, qtr->net_io_bytes);
			cf_atomic64_add(&qtr->ns->agg_num_records, rows);
			break;

		case QUERY_TYPE_UDF_BG:
			if (qtr_failed(qtr)) {
				cf_atomic64_incr(&qtr->ns->n_query_udf_bg_failure);
			} else {
				cf_atomic64_incr(&qtr->ns->n_query_udf_bg_success);
			}
			break;

		default:
			cf_crash(AS_QUERY, "Unknown Query Type !!");
			break;
	}

	// Can't use macro that tr and rw use.
	qtr->ns->query_hist_active = true;
	cf_hist_track_insert_data_point(qtr->ns->query_hist, qtr->start_time);

	SINDEX_HIST_INSERT_DATA_POINT(qtr->si, query_hist, qtr->start_time);

	if (qtr->querying_ai_time_ns) {
		QUERY_HIST_INSERT_RAW(query_prepare_batch_hist, qtr->querying_ai_time_ns);
	}

	if (qtr->n_digests) {
		SINDEX_HIST_INSERT_RAW(qtr->si, query_rcnt_hist, qtr->n_digests);
		if (rows) {
			// Can't use macro that tr and rw use.
			qtr->ns->query_rec_count_hist_active = true;
			histogram_insert_raw(qtr->ns->query_rec_count_hist, rows);

			SINDEX_HIST_INSERT_RAW(qtr->si, query_diff_hist, qtr->n_digests - rows);
		}
	}



	uint64_t query_stop_time = cf_getns();
	uint64_t elapsed_us = (query_stop_time - qtr->start_time) / 1000;
	cf_detail(AS_QUERY,
			"Total time elapsed %"PRIu64" us, %"PRIu64" of %d read operations avg latency %"PRIu64" us",
			elapsed_us, rows, qtr->n_digests, rows > 0 ? elapsed_us / rows : 0);
}

static void
query_run_teardown(as_query_transaction *qtr)
{
	query_update_stats(qtr);

	if (qtr->n_udf_tr_queued != 0) {
		cf_warning(AS_QUERY, "QUEUED UDF not equal to zero when query transaction is done");
	}

	if (qtr->qctx.recl) {
		cf_ll_reduce(qtr->qctx.recl, true /*forward*/, as_index_keys_ll_reduce_fn, NULL);
		cf_free(qtr->qctx.recl);
		qtr->qctx.recl = NULL;
	}

	if (qtr->short_running) {
		cf_atomic32_decr(&g_query_short_running);
	} else {
		cf_atomic32_decr(&g_query_long_running);
	}

	// Release all the partitions
	query_release_prereserved_partitions(qtr);


	if (qtr->bb_r) {
		bb_poolrelease(qtr->bb_r);
		qtr->bb_r = NULL;
	}

	pthread_mutex_destroy(&qtr->buf_mutex);
}

static void
query_teardown(as_query_transaction *qtr)
{
	if (qtr->srange)      as_sindex_range_free(&qtr->srange);
	if (qtr->si)          AS_SINDEX_RELEASE(qtr->si);
	if (qtr->binlist)     cf_vector_destroy(qtr->binlist);
	if (qtr->setname)     cf_free(qtr->setname);
	if (qtr->predexp_eval) predexp_destroy(qtr->predexp_eval);
	if (qtr->job_type == QUERY_TYPE_AGGR && qtr->agg_call.def.arglist) {
		as_list_destroy(qtr->agg_call.def.arglist);
	}
	else if (qtr->job_type == QUERY_TYPE_UDF_BG) {
		iudf_origin_destroy(&qtr->origin);
	}
	pthread_mutex_destroy(&qtr->slock);
}

static void
query_release_fd(as_file_handle *fd_h, bool force_close)
{
	if (fd_h) {
		fd_h->fh_info &= ~FH_INFO_DONOT_REAP;                                  
		fd_h->last_used = cf_getms();                   
		as_end_of_transaction(fd_h, force_close);
	}
}

static void
query_transaction_done(as_query_transaction *qtr)
{

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	if (!qtr)
		return;

	ASD_QUERY_TRANS_DONE(nodeid, qtr->trid, (void *) qtr);

	if (qtr_started(qtr)) {
		query_run_teardown(qtr);
	}


	// if query is aborted force close connection.
	// Not to be reused
	query_release_fd(qtr->fd_h, qtr_is_abort(qtr));
	qtr->fd_h = NULL;
	query_teardown(qtr);

	ASD_QUERY_QTR_FREE(nodeid, qtr->trid, (void *) qtr);

	qtr_free(qtr);
}
// **************************************************************************************************


/*
 * Query Transaction Ref Counts
 */
// **************************************************************************************************
int
qtr_release(as_query_transaction *qtr, char *fname, int lineno)
{
	if (qtr) {
		int val = cf_rc_release(qtr);
		if (val == 0) {
			cf_detail(AS_QUERY, "Released qtr [%s:%d] %p %d ", fname, lineno, qtr, val);
			query_transaction_done(qtr);
		}
		cf_detail(AS_QUERY, "Released qtr [%s:%d] %p %d ", fname, lineno, qtr, val);
	}
	return AS_QUERY_OK;
}

static int
qtr_reserve(as_query_transaction *qtr, char *fname, int lineno)
{
	if (!qtr) {
		return AS_QUERY_ERR;
	}
	int val = cf_rc_reserve(qtr);
	cf_detail(AS_QUERY, "Reserved qtr [%s:%d] %p %d ", fname, lineno, qtr, val);
	return AS_QUERY_OK;
}
// **************************************************************************************************


/*
 * Async Network IO Entry Point
 */
// **************************************************************************************************
/* Call back function to determine if the IO should go ahead or not.
 * Purpose
 * 1. If our sequence number does not match requeue
 * 2. If query aborted fail IO.
 * 3. In all other cases let the IO go through. That would mean
 *    if IO is queued it will be done before the fin with error
 *    result_code is sent !!
 */
int
query_netio_start_cb(void *udata, int seq)
{
	as_netio *io               = (as_netio *)udata;
	as_query_transaction *qtr  = (as_query_transaction *)io->data;
	cf_detail(AS_QUERY, "Netio Started_CB %d %d %d %d ", io->offset, io->seq, qtr->netio_pop_seq, qtr->state);

	// It is needed to send all the packets in sequence
	// A packet can be requeued after being half sent.
	if (seq > cf_atomic32_get(qtr->netio_pop_seq)) {
		return AS_NETIO_CONTINUE;
	}

	if (qtr_is_abort(qtr)) {
		return AS_QUERY_ERR;
	}

	return AS_NETIO_OK;
}

/*
 * The function after the IO on the network has been done.
 * 1. If OK was done successfully bump up the sequence number and
 *    fix stats
 * 2. Release the qtr if something fails ... which would trigger
 *    fin packet send and eventually free up qtr
 * Abort it set if something goes wrong
 */
int
query_netio_finish_cb(void *data, int retcode)
{
	as_netio *io               = (as_netio *)data;
    cf_detail(AS_QUERY, "Query Finish Callback io seq %d with retCode %d", io->seq, retcode);
	as_query_transaction *qtr  = (as_query_transaction *)io->data;
	if (qtr && (retcode != AS_NETIO_CONTINUE)) {
		// If send success make stat is updated
		if (retcode == AS_NETIO_OK) {
			cf_atomic64_add(&qtr->net_io_bytes, io->bb_r->used_sz + 8);
		} else {
			qtr_set_abort(qtr, AS_PROTO_RESULT_FAIL_QUERY_NETIO_ERR, __FILE__, __LINE__);
		}
		QUERY_HIST_INSERT_DATA_POINT(query_net_io_hist, io->start_time);

		// Undo the increment from query_netio(). Cannot reach zero here: the
		// increment owned by the transaction will only be undone after all netio
		// is complete.
		cf_rc_release(io->fd_h);
		io->fd_h = NULL;
		bb_poolrelease(io->bb_r);

		cf_atomic32_incr(&qtr->netio_pop_seq);

		qtr_finish_work(qtr, &qtr->n_io_outstanding, __FILE__, __LINE__, true);
	}
	return retcode;
}

#define MAX_OUTSTANDING_IO_REQ 2
static int
query_netio_wait(as_query_transaction *qtr)
{
	return (cf_atomic32_get(qtr->n_io_outstanding) > MAX_OUTSTANDING_IO_REQ) ? AS_QUERY_ERR : AS_QUERY_OK;
}

// Returns AS_NETIO_OK always
static int
query_netio(as_query_transaction *qtr)
{
#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	ASD_QUERY_NETIO_STARTING(nodeid, qtr->trid);

	as_netio        io;

	io.finish_cb = query_netio_finish_cb;
	io.start_cb  = query_netio_start_cb;

	qtr_reserve(qtr, __FILE__, __LINE__);
	io.data        = qtr;

	io.bb_r        = qtr->bb_r;
	qtr->bb_r      = NULL;

	cf_rc_reserve(qtr->fd_h);
	io.fd_h        = qtr->fd_h;

	io.offset      = 0;

	cf_atomic32_incr(&qtr->n_io_outstanding);
	io.seq         = cf_atomic32_incr(&qtr->netio_push_seq);
	io.start_time  = cf_getns();

	int ret        = as_netio_send(&io, false, qtr->blocking);
	qtr->bb_r      = bb_poolrequest();
   	cf_buf_builder_reserve(&qtr->bb_r, 8, NULL);

	ASD_QUERY_NETIO_FINISHED(nodeid, qtr->trid);

	return ret;
}
// **************************************************************************************************


/*
 * Query Reservation Abstraction
 */
// **************************************************************************************************
// Returns NULL if partition with is 'pid' is not query-able Else
//      if all the partitions are reserved upfront returns the rsv used for reserving the partition
//      else reserves the partition and returns rsv
as_partition_reservation *
query_reserve_partition(as_namespace * ns, as_query_transaction * qtr, uint32_t pid, as_partition_reservation * rsv)
{
	if (qtr->qctx.partitions_pre_reserved) {
		if (!qtr->qctx.can_partition_query[pid]) {
			cf_debug(AS_QUERY, "Getting digest in rec list which do not belong to query-able partition.");
			return NULL;
		}
		return &qtr->rsv[pid];
	}

	// Works for scan aggregation
	if (!rsv) {
		cf_warning(AS_QUERY, "rsv is null while reserving partition.");
		return NULL;
	}

	if (0 != as_partition_reserve_query(ns, pid, rsv)) {
		return NULL;
	}

	return rsv;
}

void
query_release_partition(as_query_transaction * qtr, as_partition_reservation * rsv)
{
	if (!qtr->qctx.partitions_pre_reserved) {
		as_partition_release(rsv);
	}
}

// Pre reserves query-able partitions
void
as_query_pre_reserve_partitions(as_query_transaction * qtr)
{
	if (!qtr) {
		cf_warning(AS_QUERY, "qtr is NULL");
		return;
	}
	if (qtr->qctx.partitions_pre_reserved) {
		qtr->rsv = cf_malloc(sizeof(as_partition_reservation) * AS_PARTITIONS);
		as_partition_prereserve_query(qtr->ns, qtr->qctx.can_partition_query, qtr->rsv);
	} else {
		qtr->rsv = NULL;
	}
}

// **************************************************************************************************


/*
 * Query tracking
 */
// **************************************************************************************************
// Put qtr in a global hash
static int
hash_put_qtr(as_query_transaction * qtr)
{
	if (!qtr->track) {
		return AS_QUERY_CONTINUE;
	}

	int rc = cf_rchash_put_unique(g_query_job_hash, &qtr->trid, sizeof(qtr->trid), qtr);
	if (rc) {
		cf_warning(AS_SINDEX, "QTR Put in hash failed with error %d", rc);
	}

	return rc;
}

// Get Qtr from global hash
static int
hash_get_qtr(uint64_t trid, as_query_transaction ** qtr)
{
	int rv = cf_rchash_get(g_query_job_hash, &trid, sizeof(trid), (void **) qtr);
	if (CF_RCHASH_OK != rv) {
		cf_info(AS_SINDEX, "Query job with transaction id [%"PRIu64"] does not exist", trid );
	}
	return rv;
}

// Delete Qtr from global hash
static int
hash_delete_qtr(as_query_transaction *qtr)
{
	if (!qtr->track) {
		return AS_QUERY_CONTINUE;
	}

	int rv = cf_rchash_delete(g_query_job_hash, &qtr->trid, sizeof(qtr->trid));
	if (CF_RCHASH_OK != rv) {
		cf_warning(AS_SINDEX, "Failed to delete qtr from query hash.");
	}
	return rv;
}
// If any query run from more than g_config.query_untracked_time_ms
// 		we are going to track it
// else no.
int
hash_track_qtr(as_query_transaction *qtr)
{
	if (!qtr->track) {
		if ((cf_getns() - qtr->start_time) > (g_config.query_untracked_time_ms * 1000000)) {
			qtr->track = true;
			qtr_reserve(qtr, __FILE__, __LINE__);
			int ret = hash_put_qtr(qtr);
			if (ret != 0 && ret != AS_QUERY_CONTINUE) {
				// track should be disabled otherwise at the
				// qtr cleanup stage some other qtr with the same
				// trid can get cleaned up.
				qtr->track     = false;
				qtr_release(qtr, __FILE__, __LINE__);
				return AS_QUERY_ERR;
			}
		}
	}
	return AS_QUERY_OK;
}
// **************************************************************************************************



/*
 * Query Request IO functions
 */
// **************************************************************************************************
/*
 * Function query_add_response
 *
 * Returns -
 *		AS_QUERY_OK  - On success.
 *		AS_QUERY_ERR - On failure.
 *
 * Notes -
 *	Basic query call back function. Fills up the client response buffer;
 *	sends out buffer and then
 *	reinitializes the buf for the next set of requests,
 *	In case buffer is full Bail out quick if unable to send response back to client
 *
 *	On success, qtr->n_result_records is incremented by 1.
 *
 * Synchronization -
 * 		Takes a lock over qtr->buf
 */
static int
query_add_response(void *void_qtr, as_storage_rd *rd)
{
	as_query_transaction *qtr = (as_query_transaction *)void_qtr;

	// TODO - check and handle error result (< 0 - drive IO) explicitly?
	size_t msg_sz = (size_t)as_msg_make_response_bufbuilder(NULL, rd,
			qtr->no_bin_data, true, true, qtr->binlist);
	int ret = 0;

	pthread_mutex_lock(&qtr->buf_mutex);
	cf_buf_builder *bb_r = qtr->bb_r;
	if (bb_r == NULL) {
		// Assert that query is aborted if bb_r is found to be null
		pthread_mutex_unlock(&qtr->buf_mutex);
		return AS_QUERY_ERR;
	}

	if (msg_sz > (bb_r->alloc_sz - bb_r->used_sz) && bb_r->used_sz != 0) {
		query_netio(qtr);
	}

	int32_t result = as_msg_make_response_bufbuilder(&qtr->bb_r, rd,
			qtr->no_bin_data, true, true, qtr->binlist);

	if (result < 0) {
		ret = result;
		cf_warning(AS_QUERY, "Weird there is space but still the packing failed "
				"available = %zd msg size = %zu",
				bb_r->alloc_sz - bb_r->used_sz, msg_sz);
	}
	cf_atomic64_incr(&qtr->n_result_records);
	pthread_mutex_unlock(&qtr->buf_mutex);
	return ret;
}


static int
query_add_fin(as_query_transaction *qtr)
{

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif
	cf_detail(AS_QUERY, "Adding fin %p", qtr);
	uint8_t *b;
	// in case of aborted query, the bb_r is already released
	if (qtr->bb_r == NULL) {
		// Assert that query is aborted if bb_r is found to be null
		return AS_QUERY_ERR;
	}
	cf_buf_builder_reserve(&qtr->bb_r, sizeof(as_msg), &b);

	ASD_QUERY_ADDFIN(nodeid, qtr->trid);
	// set up the header
	uint8_t *buf      = b;
	as_msg *msgp      = (as_msg *) buf;
	msgp->header_sz   = sizeof(as_msg);
	msgp->info1       = 0;
	msgp->info2       = 0;
	msgp->info3       = AS_MSG_INFO3_LAST;
	msgp->unused      = 0;
	msgp->result_code = qtr->result_code;
	msgp->generation  = 0;
	msgp->record_ttl  = 0;
	msgp->n_fields    = 0;
	msgp->n_ops       = 0;
	msgp->transaction_ttl = 0;
	as_msg_swap_header(msgp);
	return AS_QUERY_OK;
}

static int
query_send_fin(as_query_transaction *qtr) {
	// Send out the final data back
	if (qtr->fd_h) {
		query_add_fin(qtr);
		query_netio(qtr);
	}
	return AS_QUERY_OK;
}

static void
query_send_bg_udf_response(as_transaction *tr)
{
	cf_detail(AS_QUERY, "Send Fin for Background UDF");
	bool force_close = ! as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_PROTO_RESULT_OK);
	query_release_fd(tr->from.proto_fd_h, force_close);
	tr->from.proto_fd_h = NULL;
}

static bool
query_match_integer_fromval(as_query_transaction * qtr, as_val *v, as_sindex_key *skey)
{
	as_sindex_bin_data *start = &qtr->srange->start;
	as_sindex_bin_data *end   = &qtr->srange->end;

	if ((AS_PARTICLE_TYPE_INTEGER != as_sindex_pktype(qtr->si->imd))
			|| (AS_PARTICLE_TYPE_INTEGER != start->type)
			|| (AS_PARTICLE_TYPE_INTEGER != end->type)) {
		cf_debug(AS_QUERY, "query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
				AS_PARTICLE_TYPE_INTEGER, start->type, end->type, as_sindex_pktype(qtr->si->imd),
				qtr->si->imd->bname, qtr->si->imd->iname);
		return false;
	}
	as_integer * i = as_integer_fromval(v);
	int64_t value  = as_integer_get(i);
	if (skey->key.int_key != value) {
		cf_debug(AS_QUERY, "query_record_matches: sindex key does "
			"not matches bin value in record. skey %ld bin value %ld", skey->key.int_key, value);
		return false;
	}

	return true;
}

static bool
query_match_string_fromval(as_query_transaction * qtr, as_val *v, as_sindex_key *skey)
{
	as_sindex_bin_data *start = &qtr->srange->start;
	as_sindex_bin_data *end   = &qtr->srange->end;

	if ((AS_PARTICLE_TYPE_STRING != as_sindex_pktype(qtr->si->imd))
			|| (AS_PARTICLE_TYPE_STRING != start->type)
			|| (AS_PARTICLE_TYPE_STRING != end->type)) {
		cf_debug(AS_QUERY, "query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
				AS_PARTICLE_TYPE_STRING, start->type, end->type, as_sindex_pktype(qtr->si->imd),
				qtr->si->imd->bname, qtr->si->imd->iname);
		return false;
	}

	char * str_val = as_string_get(as_string_fromval(v));
	cf_digest str_digest;
	cf_digest_compute(str_val, strlen(str_val), &str_digest);

	if (memcmp(&str_digest, &skey->key.str_key, AS_DIGEST_KEY_SZ)) {
		return false;
	}
	return true;
}

static bool
query_match_geojson_fromval(as_query_transaction * qtr, as_val *v, as_sindex_key *skey)
{
	as_sindex_bin_data *start = &qtr->srange->start;
	as_sindex_bin_data *end   = &qtr->srange->end;

	if ((AS_PARTICLE_TYPE_GEOJSON != as_sindex_pktype(qtr->si->imd))
			|| (AS_PARTICLE_TYPE_GEOJSON != start->type)
			|| (AS_PARTICLE_TYPE_GEOJSON != end->type)) {
		cf_debug(AS_QUERY, "query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
				AS_PARTICLE_TYPE_GEOJSON, start->type, end->type,
				as_sindex_pktype(qtr->si->imd), qtr->si->imd->bname,
				qtr->si->imd->iname);
		return false;
	}

	return as_particle_geojson_match_asval(v, qtr->srange->cellid,
			qtr->srange->region, qtr->ns->geo2dsphere_within_strict);
}

// If the value matches foreach should stop iterating the
bool
query_match_mapkeys_foreach(const as_val * key, const as_val * val, void * udata)
{
	qtr_skey * q_s = (qtr_skey *)udata;
	switch (key->type) {
	case AS_STRING:
		// If matches return false
		return !query_match_string_fromval(q_s->qtr, (as_val *)key, q_s->skey);
	case AS_INTEGER:
		// If matches return false
		return !query_match_integer_fromval(q_s->qtr,(as_val *) key, q_s->skey);
	case AS_GEOJSON:
		// If matches return false
		return !query_match_geojson_fromval(q_s->qtr,(as_val *) key, q_s->skey);
	default:
		// All others don't match
		return true;
	}
}

static bool
query_match_mapvalues_foreach(const as_val * key, const as_val * val, void * udata)
{
	qtr_skey * q_s = (qtr_skey *)udata;
	switch (val->type) {
	case AS_STRING:
		// If matches return false
		return !query_match_string_fromval(q_s->qtr, (as_val *)val, q_s->skey);
	case AS_INTEGER:
		// If matches return false
		return !query_match_integer_fromval(q_s->qtr, (as_val *)val, q_s->skey);
	case AS_GEOJSON:
		// If matches return false
		return !query_match_geojson_fromval(q_s->qtr, (as_val *)val, q_s->skey);
	default:
		// All others don't match
		return true;
	}
}

static bool
query_match_listele_foreach(as_val * val, void * udata)
{
	qtr_skey * q_s = (qtr_skey *)udata;
	switch (val->type) {
	case AS_STRING:
		// If matches return false
		return !query_match_string_fromval(q_s->qtr, val, q_s->skey);
	case AS_INTEGER:
		// If matches return false
		return !query_match_integer_fromval(q_s->qtr, val, q_s->skey);
	case AS_GEOJSON:
		// If matches return false
		return !query_match_geojson_fromval(q_s->qtr, val, q_s->skey);
	default:
		// All others don't match
		return true;
	}
}
/*
 * Validate record based on its content and query make sure it indeed should
 * be selected. Secondary index does lazy delete for the entries for the record
 * for which data is on ssd. See sindex design doc for details. Hence it is
 * possible that it returns digest for which record may have changed. Do the
 * validation before returning the row.
 */
static bool
query_record_matches(as_query_transaction *qtr, as_storage_rd *rd, as_sindex_key * skey)
{
	// TODO: Add counters and make sure it is not a performance hit
	as_sindex_bin_data *start = &qtr->srange->start;
	as_sindex_bin_data *end   = &qtr->srange->end;

	//TODO: Make it more general to support sindex over multiple bins	
	as_bin * b = as_bin_get_by_id(rd, qtr->si->imd->binid);

	if (!b) {
		cf_debug(AS_QUERY , "as_query_record_validation: "
				"Bin name %s not found ", qtr->si->imd->bname);
		// Possible bin may not be there anymore classic case of
		// bin delete.
		return false;
	}
	uint8_t type = as_bin_get_particle_type(b);

	// If the bin is of type cdt, we need to see if anyone of the value within cdt
	// matches the query.
	// This can be performance hit for big list and maps.
	as_val * res_val = NULL;
	as_val * val     = NULL;
	bool matches     = false;
	bool from_cdt    = false;
	switch (type) {
		case AS_PARTICLE_TYPE_INTEGER : {
			if ((type != as_sindex_pktype(qtr->si->imd))
			|| (type != start->type)
			|| (type != end->type)) {
				cf_debug(AS_QUERY, "query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
					type, start->type, end->type, as_sindex_pktype(qtr->si->imd),
					qtr->si->imd->bname, qtr->si->imd->iname);
				matches = false;
				break;
			}

			int64_t i = as_bin_particle_integer_value(b);
			if (skey->key.int_key != i) {
				cf_debug(AS_QUERY, "query_record_matches: sindex key does "
						"not matches bin value in record. bin value %ld skey value %ld", i, skey->key.int_key);
				matches = false;
				break;
			}
			matches = true;
			break;
		}
		case AS_PARTICLE_TYPE_STRING : {
			if ((type != as_sindex_pktype(qtr->si->imd))
			|| (type != start->type)
			|| (type != end->type)) {
				cf_debug(AS_QUERY, "query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
					type, start->type, end->type, as_sindex_pktype(qtr->si->imd),
					qtr->si->imd->bname, qtr->si->imd->iname);
				matches = false;
				break;
			}

			char * buf;
			uint32_t psz = as_bin_particle_string_ptr(b, &buf);
			cf_digest bin_digest;
			cf_digest_compute(buf, psz, &bin_digest);
			if (memcmp(&skey->key.str_key, &bin_digest, AS_DIGEST_KEY_SZ)) {
				matches = false;
				break;
			}
			matches = true;
			break;
		}
		case AS_PARTICLE_TYPE_GEOJSON : {
			if ((type != as_sindex_pktype(qtr->si->imd))
			|| (type != start->type)
			|| (type != end->type)) {
				cf_debug(AS_QUERY, "as_query_record_matches: Type mismatch %d!=%d!=%d!=%d  binname=%s index=%s",
					type, start->type, end->type, as_sindex_pktype(qtr->si->imd),
					qtr->si->imd->bname, qtr->si->imd->iname);
				return false;
			}

			bool iswithin = as_particle_geojson_match(b->particle,
					qtr->srange->cellid, qtr->srange->region,
					qtr->ns->geo2dsphere_within_strict);

			// We either found a valid point or a false positive.
			if (iswithin) {
				cf_atomic64_incr(&qtr->ns->geo_region_query_points);
			}
			else {
				cf_atomic64_incr(&qtr->ns->geo_region_query_falsepos);
			}

			return iswithin;
		}
		case AS_PARTICLE_TYPE_MAP : {
			val     = as_bin_particle_to_asval(b);
			res_val = as_sindex_extract_val_from_path(qtr->si->imd, val);
			if (!res_val) {
				matches = false;
				break;
			}
			from_cdt = true;
			break;
		}
		case AS_PARTICLE_TYPE_LIST : {
			val     = as_bin_particle_to_asval(b);
			res_val = as_sindex_extract_val_from_path(qtr->si->imd, val);
			if (!res_val) {
				matches = false;
				break;
			}
			from_cdt = true;
			break;
		}
		default: {
			break;
		}
	}

	if (from_cdt) {
		if (res_val->type == AS_INTEGER) {
			// Defensive check.
			if (qtr->si->imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
				matches = query_match_integer_fromval(qtr, res_val, skey);
			}
			else {
				matches = false;
			}
		}
		else if (res_val->type == AS_STRING) {
			// Defensive check.
			if (qtr->si->imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
				matches = query_match_string_fromval(qtr, res_val, skey);
			}
			else {
				matches = false;
			}
		}
		else if (res_val->type == AS_MAP) {
			qtr_skey q_s;
			q_s.qtr  = qtr;
			q_s.skey = skey;
			// Defensive check.
			if (qtr->si->imd->itype == AS_SINDEX_ITYPE_MAPKEYS) {
				as_map * map = as_map_fromval(res_val);
				matches = !as_map_foreach(map, query_match_mapkeys_foreach, &q_s);
			}
			else if (qtr->si->imd->itype == AS_SINDEX_ITYPE_MAPVALUES){
				as_map * map = as_map_fromval(res_val);
				matches = !as_map_foreach(map, query_match_mapvalues_foreach, &q_s);
			}
			else {
				matches = false;
			}
		}
		else if (res_val->type == AS_LIST) {
			qtr_skey q_s;
			q_s.qtr  = qtr;
			q_s.skey = skey;

			// Defensive check
			if (qtr->si->imd->itype == AS_SINDEX_ITYPE_LIST) {
				as_list * list = as_list_fromval(res_val);
				matches = !as_list_foreach(list, query_match_listele_foreach, &q_s);
			}
			else {
				matches = false;
			}
		}
	}

	if (val) {
		as_val_destroy(val);
	}
	return matches;
}



static int
query_io(as_query_transaction *qtr, cf_digest *dig, as_sindex_key * skey)
{
#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	as_namespace * ns = qtr->ns;
	as_partition_reservation rsv_stack;
	as_partition_reservation * rsv = &rsv_stack;

	// We make sure while making digest list that current partition is query-able
	// Attempt the query reservation here as well. If this partition is not
	// query-able anymore then no need to return anything
	// Since we are reserving all the partitions upfront, this is a defensive check
	uint32_t pid = as_partition_getid(dig);
	rsv = query_reserve_partition(ns, qtr, pid, rsv);
	if (!rsv) {
		return AS_QUERY_OK;
	}

	ASD_QUERY_IO_STARTING(nodeid, qtr->trid);

	as_index_ref r_ref;
	r_ref.skip_lock = false;
	int rec_rv      = as_record_get_live(rsv->tree, dig, &r_ref, ns);

	if (rec_rv == 0) {
		as_index *r = r_ref.r;

		predexp_args_t predargs = { .ns = ns, .md = r, .vl = NULL, .rd = NULL };

		if (qtr->predexp_eval &&
			! predexp_matches_metadata(qtr->predexp_eval, &predargs)) {
			as_record_done(&r_ref, ns);
			goto CLEANUP;
		}

		// check to see this isn't a record waiting to die
		if (as_record_is_doomed(r, ns)) {
			as_record_done(&r_ref, ns);
			cf_debug(AS_QUERY,
					"build_response: record expired. treat as not found");
			// Not sending error message to client as per the agreement
			// that server will never send a error result code to the query client.
			goto CLEANUP;
		}

		// make sure it's brought in from storage if necessary
		as_storage_rd rd;
		as_storage_record_open(ns, r, &rd);
		qtr->n_read_success += 1;

		// TODO - even if qtr->no_bin_data is true, we still read bins in order
		// to check via query_record_matches() below. If sindex evolves to not
		// have to do that, optimize this case and bypass reading bins.

		as_storage_rd_load_n_bins(&rd); // TODO - handle error returned

		// Note: This array must stay in scope until the response
		//       for this record has been built, since in the get
		//       data w/ record on device case, it's copied by
		//       reference directly into the record descriptor!
		as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

		// Figure out which bins you want - for now, all
		as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned
		rd.n_bins = as_bin_inuse_count(&rd);

		// Now we have a record.
		predargs.rd = &rd;

		if (qtr->predexp_eval &&
			 ! predexp_matches_record(qtr->predexp_eval, &predargs)) {
			as_storage_record_close(&rd);
			as_record_done(&r_ref, ns);
			goto CLEANUP;
		}

		// Call Back
		if (!query_record_matches(qtr, &rd, skey)) {
			as_storage_record_close(&rd);
			as_record_done(&r_ref, ns);
			query_release_partition(qtr, rsv);
			cf_atomic64_incr(&g_stats.query_false_positives);
			ASD_QUERY_IO_NOTMATCH(nodeid, qtr->trid);
			return AS_QUERY_OK;
		}

		int ret = query_add_response(qtr, &rd);
		if (ret != 0) {
			as_storage_record_close(&rd);
			as_record_done(&r_ref, ns);
			qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_CBERROR, __FILE__, __LINE__);
			query_release_partition(qtr, rsv);
			ASD_QUERY_IO_ERROR(nodeid, qtr->trid);
			return AS_QUERY_ERR;
		}
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
	} else {
		// What do we do about empty records?
		// 1. Should gin up an empty record
		// 2. Current error is returned back to the client.
		cf_detail(AS_QUERY, "query_generator: "
				"as_record_get returned %d : key %"PRIx64, rec_rv,
				*(uint64_t *)dig);
	}
CLEANUP :
	query_release_partition(qtr, rsv);

	ASD_QUERY_IO_FINISHED(nodeid, qtr->trid);

	return AS_QUERY_OK;
}
// **************************************************************************************************

/*
 * Query Aggregation Request Workhorse Function
 */
// **************************************************************************************************
static int
query_add_val_response(void *void_qtr, const as_val *val, bool success)
{
	as_query_transaction *qtr = (as_query_transaction *)void_qtr;
	if (!qtr) {
		return AS_QUERY_ERR;
	}

	uint32_t msg_sz = as_particle_asval_client_value_size(val);
	if (0 == msg_sz) {
		cf_warning(AS_PROTO, "particle to buf: could not copy data!");
	}

	pthread_mutex_lock(&qtr->buf_mutex);
	cf_buf_builder *bb_r = qtr->bb_r;
	if (bb_r == NULL) {
		// Assert that query is aborted if bb_r is found to be null
		pthread_mutex_unlock(&qtr->buf_mutex);
		return AS_QUERY_ERR;
	}

	if (msg_sz > (bb_r->alloc_sz - bb_r->used_sz) && bb_r->used_sz != 0) {
		query_netio(qtr);
	}

	as_msg_make_val_response_bufbuilder(val, &qtr->bb_r, msg_sz, success);
	cf_atomic64_incr(&qtr->n_result_records);

	pthread_mutex_unlock(&qtr->buf_mutex);
	return 0;
}


static void
query_add_result(char *res, as_query_transaction *qtr, bool success)
{
	const as_val * v = (as_val *) as_string_new (res, false);
	query_add_val_response((void *) qtr, v, success);
	as_val_destroy(v);
}


static int
query_process_aggreq(query_work *qagg)
{
	as_query_transaction *qtr = qagg->qtr;
	if (!qtr) {
		return AS_QUERY_ERR;
	}

	if (!cf_ll_size(qagg->recl)) {
		return AS_QUERY_ERR;
	}

	as_result   *res = as_result_new();
	int ret          = as_aggr_process(qtr->ns, &qtr->agg_call, qagg->recl, (void *)qtr, res);

	if (ret != 0) {
        char *rs = as_module_err_string(ret);
        if (res->value != NULL) {
            as_string * lua_s   = as_string_fromval(res->value);
            char *      lua_err  = (char *) as_string_tostring(lua_s);
            if (lua_err != NULL) {
                int l_rs_len = strlen(rs);
                rs = cf_realloc(rs,l_rs_len + strlen(lua_err) + 4);
                sprintf(&rs[l_rs_len]," : %s",lua_err);
            }
        }
        query_add_result(rs, qtr, false);
        cf_free(rs);
	}
    as_result_destroy(res);
	return ret;
}
// **************************************************************************************************


/*
 * Aggregation HOOKS
 */
// **************************************************************************************************
as_stream_status
agg_ostream_write(void *udata, as_val *v)
{
	as_query_transaction *qtr = (as_query_transaction *)udata;
	if (!v) {
		return AS_STREAM_OK;
	}
	int ret = AS_STREAM_OK;
	if (query_add_val_response((void *)qtr, v, true)) {
		ret = AS_STREAM_ERR;
	}
	as_val_destroy(v);
	return ret;
}

static as_partition_reservation *
agg_reserve_partition(void *udata, as_namespace *ns, uint32_t pid, as_partition_reservation *rsv)
{
	return query_reserve_partition(ns, (as_query_transaction *)udata, pid, rsv);
}

static void
agg_release_partition(void *udata, as_partition_reservation *rsv)
{
	query_release_partition((as_query_transaction *)udata, rsv);
}

static void
agg_set_error(void * udata, int err)
{
	qtr_set_err((as_query_transaction *)udata, AS_PROTO_RESULT_FAIL_QUERY_CBERROR, __FILE__, __LINE__);
}

// true if matches
static bool
agg_record_matches(void *udata, udf_record *urecord, void *key_data)
{
	as_query_transaction * qtr = (as_query_transaction*)udata;
	as_sindex_key *skey        = (void *)key_data;
	qtr->n_read_success++;
	if (query_record_matches(qtr, urecord->rd, skey) == false) {
		cf_atomic64_incr(&g_stats.query_false_positives); // PUT IT INSIDE PRE_CHECK
		return false;
	}
	return true;
}

const as_aggr_hooks query_aggr_hooks = {
	.ostream_write = agg_ostream_write,
	.set_error     = agg_set_error,
	.ptn_reserve   = agg_reserve_partition,
	.ptn_release   = agg_release_partition,
	.pre_check     = agg_record_matches
};
// **************************************************************************************************





/*
 * Query Request UDF functions
 */
// **************************************************************************************************
// NB: Caller holds a write hash lock _BE_CAREFUL_ if you intend to take
// lock inside this function
int
query_udf_bg_tr_complete(void *udata, int retcode)
{
	as_query_transaction *qtr = (as_query_transaction *)udata;
	if (!qtr) {
		cf_warning(AS_QUERY, "Complete called with invalid job id");
		return AS_QUERY_ERR;
	}

	qtr_finish_work(qtr, &qtr->n_udf_tr_queued, __FILE__, __LINE__, true);
	return AS_QUERY_OK;
}

// Creates a internal transaction for per record UDF execution triggered
// from inside generator. The generator could be scan job generating digest
// or query generating digest.
int
query_udf_bg_tr_start(as_query_transaction *qtr, cf_digest *keyd)
{
	if (qtr->origin.predexp) {
		as_partition_reservation rsv_stack;
		as_partition_reservation *rsv = &rsv_stack;
		uint32_t pid = as_partition_getid(keyd);

		if (! (rsv = query_reserve_partition(qtr->ns, qtr, pid, rsv))) {
			return AS_QUERY_OK;
		}

		as_index_ref r_ref;
		r_ref.skip_lock = false;

		if (as_record_get_live(rsv->tree, keyd, &r_ref, qtr->ns) != 0) {
			query_release_partition(qtr, rsv);
			return AS_QUERY_OK;
		}

		predexp_args_t predargs = {
				.ns = qtr->ns, .md = r_ref.r, .vl = NULL, .rd = NULL
		};

		if (qtr->origin.predexp &&
				! predexp_matches_metadata(qtr->origin.predexp, &predargs)) {
			as_record_done(&r_ref, qtr->ns);
			query_release_partition(qtr, rsv);
			return AS_QUERY_OK;
		}

		as_record_done(&r_ref, qtr->ns);
		query_release_partition(qtr, rsv);
	}

	as_transaction tr;

	as_transaction_init_iudf(&tr, qtr->ns, keyd, &qtr->origin, qtr->is_durable_delete);

	qtr_reserve(qtr, __FILE__, __LINE__);
	cf_atomic32_incr(&qtr->n_udf_tr_queued);

	as_tsvc_enqueue(&tr);

	return AS_QUERY_OK;
}

static int
query_process_udfreq(query_work *qudf)
{
	int ret               = AS_QUERY_OK;
	cf_ll_element  * ele  = NULL;
	cf_ll_iterator * iter = NULL;
	as_query_transaction *qtr = qudf->qtr;
	if (!qtr)           return AS_QUERY_ERR;
	cf_detail(AS_QUERY, "Performing UDF");
	iter                  = cf_ll_getIterator(qudf->recl, true /*forward*/);
	if (!iter) {
		ret              = AS_QUERY_ERR;
		qtr_set_err(qtr, AS_SINDEX_ERR_NO_MEMORY, __FILE__, __LINE__);
		goto Cleanup;
	}

	while ((ele = cf_ll_getNext(iter))) {
		as_index_keys_ll_element * node;
		node                         = (as_index_keys_ll_element *) ele;
		as_index_keys_arr * keys_arr  = node->keys_arr;
		if (!keys_arr) {
			continue;
		}
		node->keys_arr   =  NULL;

		for (int i = 0; i < keys_arr->num; i++) {

			while (cf_atomic32_get(qtr->n_udf_tr_queued) >= (AS_QUERY_MAX_UDF_TRANSACTIONS * (qtr->priority / 10 + 1))) {
				usleep(g_config.query_sleep_us);
				query_check_timeout(qtr);
				if (qtr_failed(qtr)) {
					ret = AS_QUERY_ERR;
					goto Cleanup;
				}
			}

			if (AS_QUERY_ERR == query_udf_bg_tr_start(qtr, &keys_arr->pindex_digs[i])) {
				as_index_keys_release_arr_to_queue(keys_arr);
				ret = AS_QUERY_ERR;
				goto Cleanup;
			}
		}
		as_index_keys_release_arr_to_queue(keys_arr);
	}
Cleanup:
	if (iter) {
		cf_ll_releaseIterator(iter);
		iter = NULL;
	}
	return ret;
}
// **************************************************************************************************




static int
query_process_ioreq(query_work *qio)
{

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	as_query_transaction *qtr = qio->qtr;
	if (!qtr) {
		return AS_QUERY_ERR;
	}

	ASD_QUERY_IOREQ_STARTING(nodeid, qtr->trid);

	cf_ll_element * ele   = NULL;
	cf_ll_iterator * iter = NULL;

	cf_detail(AS_QUERY, "Performing IO");
	uint64_t time_ns      = 0;
	if (g_config.query_enable_histogram || qtr->si->enable_histogram) {
		time_ns = cf_getns();
	}
	iter                  = cf_ll_getIterator(qio->recl, true /*forward*/);
	if (!iter) {
		cf_crash(AS_QUERY, "Cannot allocate iterator... out of memory !!");
	}

	while ((ele = cf_ll_getNext(iter))) {
		as_index_keys_ll_element * node;
		node                       = (as_index_keys_ll_element *) ele;
		as_index_keys_arr *keys_arr = node->keys_arr;
		if (!keys_arr) {
			continue;
		}
		node->keys_arr     = NULL;
		for (int i = 0; i < keys_arr->num; i++) {
			if (AS_QUERY_OK != query_io(qtr, &keys_arr->pindex_digs[i], &keys_arr->sindex_keys[i])) {
				as_index_keys_release_arr_to_queue(keys_arr);
				goto Cleanup;
			}

			int64_t nresults = cf_atomic64_get(qtr->n_result_records);
			if (nresults > 0 && (nresults % qtr->priority == 0))
			{
				usleep(g_config.query_sleep_us);
				query_check_timeout(qtr);
				if (qtr_failed(qtr)) {
					as_index_keys_release_arr_to_queue(keys_arr);
					goto Cleanup;
				}
			}
		}
		as_index_keys_release_arr_to_queue(keys_arr);
	}
Cleanup:

	if (iter) {
		cf_ll_releaseIterator(iter);
		iter = NULL;
	}
	QUERY_HIST_INSERT_DATA_POINT(query_batch_io_hist, time_ns);
	SINDEX_HIST_INSERT_DATA_POINT(qtr->si, query_batch_io, time_ns);

	ASD_QUERY_IOREQ_FINISHED(nodeid, qtr->trid);

	return AS_QUERY_OK;
}

// **************************************************************************************************


/*
 * Query Request Processing
 */
// **************************************************************************************************
static int
qwork_process(query_work *qworkp)
{
	QUERY_HIST_INSERT_DATA_POINT(query_batch_io_q_wait_hist, qworkp->queued_time_ns);
	cf_detail(AS_QUERY, "Processing Request %d", qworkp->type);
	if (qtr_failed(qworkp->qtr)) {
		return AS_QUERY_ERR;
	}
	int ret = AS_QUERY_OK;
	switch (qworkp->type) {
		case QUERY_WORK_TYPE_LOOKUP:
			ret = query_process_ioreq(qworkp);
			break;
		case QUERY_WORK_TYPE_UDF_BG: // Does it need different call ??
			ret = query_process_udfreq(qworkp);
			break;
		case QUERY_WORK_TYPE_AGG:
			ret = query_process_aggreq(qworkp);
			break;
		default:
			cf_warning(AS_QUERY, "Unsupported query type %d.. Dropping it", qworkp->type);
			break;
	}
	return ret;
}

static void
qwork_setup(query_work *qworkp, as_query_transaction *qtr)
{
	qtr_reserve(qtr, __FILE__, __LINE__);
	qworkp->qtr               = qtr;
	qworkp->recl              = qtr->qctx.recl;
	qtr->qctx.recl            = NULL;
	qworkp->queued_time_ns    = cf_getns();
	qtr->n_digests          += qtr->qctx.n_bdigs;
	qtr->qctx.n_bdigs        = 0;

	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
			qworkp->type          = QUERY_WORK_TYPE_LOOKUP;
			break;
		case QUERY_TYPE_AGGR:
			qworkp->type          = QUERY_WORK_TYPE_AGG;
			break;
		case QUERY_TYPE_UDF_BG:
			qworkp->type          = QUERY_WORK_TYPE_UDF_BG;
			break;
		default:
			cf_crash(AS_QUERY, "Unknown Query Type !!");
	}
}

static void
qwork_teardown(query_work *qworkp)
{
	if (qworkp->recl) {
		cf_ll_reduce(qworkp->recl, true /*forward*/, as_index_keys_ll_reduce_fn, NULL);
		cf_free(qworkp->recl);
		qworkp->recl = NULL;
	}
	qtr_release(qworkp->qtr, __FILE__, __LINE__);
	qworkp->qtr = NULL;
}
// **************************************************************************************************


void *
qwork_th(void *q_to_wait_on)
{
	unsigned int         thread_id = cf_atomic32_incr(&g_query_worker_threadcnt);
	cf_detail(AS_QUERY, "Created Query Worker Thread %d", thread_id);
	query_work   * qworkp     = NULL;
	int                  ret       = AS_QUERY_OK;

	while (1) {
		// Kill self if thread id is greater than that of number of configured
		// Config change should be flag for quick check
		if (thread_id > g_config.query_worker_threads) {
			pthread_rwlock_rdlock(&g_query_lock);
			if (thread_id > g_config.query_worker_threads) {
				cf_atomic32_decr(&g_query_worker_threadcnt);
				pthread_rwlock_unlock(&g_query_lock);
				cf_detail(AS_QUERY, "Query Worker thread %d exited", thread_id);
				return NULL;
			}
			pthread_rwlock_unlock(&g_query_lock);
		}
		if (cf_queue_pop(g_query_work_queue, &qworkp, CF_QUEUE_FOREVER) != 0) {
			cf_crash(AS_QUERY, "Failed to pop from Query worker queue.");
		}
		cf_detail(AS_QUERY, "Popped I/O work [%p,%p]", qworkp, qworkp->qtr);

		ret = qwork_process(qworkp);

		as_query_transaction *qtr = qworkp->qtr;
		if ((ret != AS_QUERY_OK) && !qtr_failed(qtr)) {
			cf_warning(AS_QUERY, "Request processing failed but query is not qtr_failed .... ret %d", ret);
		}
		qtr_finish_work(qtr, &qtr->n_qwork_active, __FILE__, __LINE__, false);
		qwork_teardown(qworkp);
		qwork_poolrelease(qworkp);
	}

	return NULL;
}

/*
 * Query Generator
 */
// **************************************************************************************************
/*
 * Function query_get_nextbatch
 *
 * Notes-
 *		Function generates the next batch of digest list after looking up
 * 		secondary index tree. The function populates qctx->recl with the
 * 		digest list.
 *
 * Returns
 * 		AS_QUERY_OK:  If the batch is full qctx->n_bdigs == qctx->bsize. The caller
 *  		   then processes the batch and reset the qctx->recl and qctx->n_bdigs.
 *
 * 		AS_QUERY_CONTINUE:  If the caller should continue calling this function.
 *
 * 		AS_QUERY_ERR: In case of error
 */
int
query_get_nextbatch(as_query_transaction *qtr)
{
	int              ret     = AS_QUERY_OK;
	as_sindex       *si      = qtr->si;
	as_sindex_qctx  *qctx    = &qtr->qctx;
	uint64_t         time_ns = 0;
	if (g_config.query_enable_histogram
		|| qtr->si->enable_histogram) {
		time_ns = cf_getns();
	}

	as_sindex_range *srange	 = &qtr->srange[qctx->range_index];

	if (qctx->pimd_idx == -1) {
		if (!srange->isrange) {
			qctx->pimd_idx	 = ai_btree_key_hash_from_sbin(si->imd, &srange->start);
		} else {
			qctx->pimd_idx	 = 0;
		}
	}

	if (!qctx->recl) {
		qctx->recl = cf_malloc(sizeof(cf_ll));
		cf_ll_init(qctx->recl, as_index_keys_ll_destroy_fn, false /*no lock*/);
		qctx->n_bdigs        = 0;
	} else {
		// Following condition may be true if the
		// query has moved from short query pool to
		// long running query pool
		if (qctx->n_bdigs >= qctx->bsize)
			return ret;
	}

	// Query Aerospike Index
	int      qret            = as_sindex_query(qtr->si, srange, &qtr->qctx);
	cf_detail(AS_QUERY, "start %ld end %ld @ %d pimd found %"PRIu64, srange->start.u.i64, srange->end.u.i64, qctx->pimd_idx, qctx->n_bdigs);

	qctx->new_ibtr           = false;
	if (qret < 0) { // [AS_SINDEX_OK, AS_SINDEX_CONTINUE] -> OK
		qtr_set_err(qtr, as_sindex_err_to_clienterr(qret, __FILE__, __LINE__), __FILE__, __LINE__);
		ret = AS_QUERY_ERR;
		goto batchout;
	}

	if (time_ns) {
		if (g_config.query_enable_histogram) {
			qtr->querying_ai_time_ns += cf_getns() - time_ns;
		} else if (qtr->si->enable_histogram) {
			SINDEX_HIST_INSERT_DATA_POINT(qtr->si, query_batch_lookup, time_ns);
		}
	}
	if (qctx->n_bdigs < qctx->bsize) {
		qctx->new_ibtr       = true;
		qctx->nbtr_done      = false;
		qctx->pimd_idx++;
		cf_detail(AS_QUERY, "All the Data finished moving to next tree %d", qctx->pimd_idx);
		if (!srange->isrange) {
			qtr->result_code = AS_PROTO_RESULT_OK;
			ret              = AS_QUERY_DONE;
			goto batchout;
		}
		if (qctx->pimd_idx == si->imd->nprts) {

			// Geospatial queries need to search multiple ranges.  The
			// srange object is a vector of MAX_REGION_CELLS elements.
			// We iterate over ranges until we encounter an empty
			// srange (num_binval == 0).
			//
			if (qctx->range_index == (MAX_REGION_CELLS - 1) ||
				qtr->srange[qctx->range_index+1].num_binval == 0) {
				qtr->result_code = AS_PROTO_RESULT_OK;
				ret              = AS_QUERY_DONE;
				goto batchout;
			}
			qctx->range_index++;
			qctx->pimd_idx = -1;
		}
		ret = AS_QUERY_CONTINUE;
		goto batchout;
	}
batchout:
	return ret;
}


/*
 * Phase II setup just after the generator picks up query for
 * the first time
 */
static int
query_run_setup(as_query_transaction *qtr)
{

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	QUERY_HIST_INSERT_DATA_POINT(query_query_q_wait_hist, qtr->start_time);
	cf_atomic64_set(&qtr->n_result_records, 0);
	qtr->track               = false;
	qtr->querying_ai_time_ns = 0;
	qtr->n_io_outstanding    = 0;
	qtr->netio_push_seq      = 0;
	qtr->netio_pop_seq       = 1;
	qtr->blocking            = false;
	pthread_mutex_init(&qtr->buf_mutex, NULL);

	// Aerospike Index object initialization
	qtr->result_code              = AS_PROTO_RESULT_OK;

	// Initialize qctx
	// start with the threshold value
	qtr->qctx.bsize               = g_config.query_threshold;
	qtr->qctx.new_ibtr            = true;
	qtr->qctx.nbtr_done           = false;
	qtr->qctx.pimd_idx            = -1;
	qtr->qctx.recl                = NULL;
	qtr->qctx.n_bdigs             = 0;
	qtr->qctx.range_index         = 0;
	qtr->qctx.partitions_pre_reserved = g_config.partitions_pre_reserved;
	qtr->qctx.bkey                = &qtr->bkey;
	init_ai_obj(qtr->qctx.bkey);
	bzero(&qtr->qctx.bdig, sizeof(cf_digest));
	// Populate all the paritions for which this partition is query-able
	as_query_pre_reserve_partitions(qtr);

	qtr->priority                 = g_config.query_priority;
	qtr->bb_r                     = bb_poolrequest();
	cf_buf_builder_reserve(&qtr->bb_r, 8, NULL);

	qtr_set_running(qtr);
	cf_atomic64_incr(&qtr->ns->query_short_reqs);
	cf_atomic32_incr(&g_query_short_running);

	// This needs to be distant from the initialization of nodeid to
	// workaround a lame systemtap/compiler interaction.
	ASD_QUERY_INIT(nodeid, qtr->trid);

	return AS_QUERY_OK;
}

static int
query_qtr_enqueue(as_query_transaction *qtr, bool is_requeue)
{
	uint64_t limit  = 0;
	uint32_t size   = 0;
	cf_queue    * q;
	cf_atomic64 * queue_full_err;
	if (qtr->short_running) {
		limit          = g_config.query_short_q_max_size;
		size           = cf_atomic32_get(g_query_short_running);
		q              = g_query_short_queue;
		queue_full_err = &qtr->ns->query_short_queue_full;
	}
	else {
		limit          = g_config.query_long_q_max_size;
		size           = cf_atomic32_get(g_query_long_running);
		q              = g_query_long_queue;
		queue_full_err = &qtr->ns->query_long_queue_full;
	}

	// Allow requeue without limit check, to cover for dynamic
	// config change while query
	if (!is_requeue && (size > limit)) {
		cf_atomic64_incr(queue_full_err);
		return AS_QUERY_ERR;
	} else {
		cf_queue_push(q, &qtr);
		cf_detail(AS_QUERY, "Logged query ");
	}

	return AS_QUERY_OK;
}

int
query_requeue(as_query_transaction *qtr)
{
	int ret = AS_QUERY_OK;
	if (query_qtr_enqueue(qtr, true) != 0) {
		cf_warning(AS_QUERY, "Queuing Error... continue!!");
		qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_QUEUEFULL, __FILE__, __LINE__);
		ret = AS_QUERY_ERR;
	} else {
		cf_detail(AS_QUERY, "Query Queued Due to Network");
		ret = AS_QUERY_OK;
	}
	return ret;
}

static void
qtr_finish_work(as_query_transaction *qtr, cf_atomic32 *stat, char *fname, int lineno, bool release)
{
	qtr_lock(qtr);
	uint32_t val = cf_atomic32_decr(stat);
	if ((val == 0) && qtr->do_requeue) {
		query_requeue(qtr);
		cf_detail(AS_QUERY, "(%s:%d) Job Requeued %p", fname, lineno, qtr);
		qtr->do_requeue = false;
	}
	qtr_unlock(qtr);
	if (release) {
		qtr_release(qtr, fname, lineno);
	}
}

//
// 0: Successfully requeued
// -1: Query Err
// 1: Not requeued continue
// 2: Query finished
//
static int
query_qtr_check_and_requeue(as_query_transaction *qtr)
{
	bool do_enqueue = false;
	// Step 1: If the query batch is done then wait for number of outstanding qwork to
	// finish. This may slow down query responses get the better model
	if (qtr_finished(qtr)) {
		if ((cf_atomic32_get(qtr->n_qwork_active) == 0)
				&& (cf_atomic32_get(qtr->n_io_outstanding) == 0)
				&& (cf_atomic32_get(qtr->n_udf_tr_queued) == 0)) {
			cf_detail(AS_QUERY, "Request is finished");
			return AS_QUERY_DONE;
		}
		do_enqueue = true;
		cf_detail(AS_QUERY, "Request not finished qwork(%d) io(%d)", cf_atomic32_get(qtr->n_qwork_active), cf_atomic32_get(qtr->n_io_outstanding));
	}

	// Step 2: Client is slow requeue
	if (query_netio_wait(qtr) != AS_QUERY_OK) {
		do_enqueue = true;
	}

	// Step 3: Check to see if this is long running query. This is determined by
	// checking number of records read. Please note that it makes sure the false
	// entries in secondary index does not effect this decision. All short running
	// queries perform I/O in the batch thread context.
	if ((cf_atomic64_get(qtr->n_result_records) >= g_config.query_threshold)
			&& qtr->short_running) {
		qtr->short_running       = false;
		// Change batch size to the long running job batch size value
		qtr->qctx.bsize          = g_config.query_bsize;
		cf_atomic32_decr(&g_query_short_running);
		cf_atomic32_incr(&g_query_long_running);
		cf_atomic64_incr(&qtr->ns->query_long_reqs);
		cf_atomic64_decr(&qtr->ns->query_short_reqs);
		cf_detail(AS_QUERY, "Query Queued Into Long running thread pool %ld %d", cf_atomic64_get(qtr->n_result_records), qtr->short_running);
		do_enqueue = true;
	}

	if (do_enqueue) {
		int ret = AS_QUERY_OK;
		qtr_lock(qtr);
		if ((cf_atomic32_get(qtr->n_qwork_active) != 0)
				|| (cf_atomic32_get(qtr->n_io_outstanding) != 0)
				|| (cf_atomic32_get(qtr->n_udf_tr_queued) != 0)) {
			cf_detail(AS_QUERY, "Job Setup for Requeue %p", qtr);

			// Release of one of the above will perform requeue... look for
			// qtr_finish_work();
			qtr->do_requeue = true;
			ret = AS_QUERY_OK;
		} else {
			ret = query_requeue(qtr);
		}
		qtr_unlock(qtr);
		return ret;
	}

	return AS_QUERY_CONTINUE;
}
static bool
query_process_inline(as_query_transaction *qtr)
{
	if (   g_config.query_req_in_query_thread
		|| (cf_atomic32_get((qtr)->n_qwork_active) > g_config.query_req_max_inflight)
		|| (qtr && qtr->short_running)
		|| (qtr && qtr_finished(qtr))) {
		return true;
	}
	else {
		return false;
	}
}
/*
 * Process the query work either inilne or pass it on to the
 * worker thread
 *
 * Returns
 *     -1 : Fail
 *     0  : Success
 */
static int
qtr_process(as_query_transaction *qtr)
{
	if (query_process_inline(qtr)) {
		query_work qwork;
		qwork_setup(&qwork, qtr);

		int ret = qwork_process(&qwork);

		qwork_teardown(&qwork);
		return ret;

	} else {
		query_work *qworkp = qwork_poolrequest();
		if (!qworkp) {
			cf_warning(AS_QUERY, "Could not allocate query "
					"request structure .. out of memory .. Aborting !!!");
			return AS_QUERY_ERR;
		}
		// Successfully queued
		cf_atomic32_incr(&qtr->n_qwork_active);
		qwork_setup(qworkp, qtr);
		cf_queue_push(g_query_work_queue, &qworkp);

	}
	return AS_QUERY_OK;
}

static int
query_check_bound(as_query_transaction *qtr)
{
	if (cf_atomic64_get(qtr->n_result_records) > g_config.query_rec_count_bound) {
		return AS_QUERY_ERR;
	}
	return AS_QUERY_OK;
}
/*
 * Function query_generator
 *
 * Does the following
 * 1. Calls the sindex layer for fetching digest list
 * 2. If short running query performs I/O inline and for long running query
 *    queues it up for work threads to execute.
 * 3. If the query is short_running and has hit threshold. Requeue it for
 *    long running generator threads
 *
 * Returns -
 * 		Nothing, sets the qtr status accordingly
 */
static void
query_generator(as_query_transaction *qtr)
{
#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
	uint64_t trid = qtr->trid;
	size_t nrecs = 0;
#endif

	// Query can get requeue for many different reason. Check if it is
	// already started before indulging in act to setting it up for run
	if (!qtr_started(qtr)) {
		query_run_setup(qtr);
	}

	int loop = 0;
	while (true) {

		// Step 1: Check for requeue
		int ret = query_qtr_check_and_requeue(qtr);
		if (ret == AS_QUERY_ERR) {
			cf_warning(AS_QUERY, "Unexpected requeue failure .. shutdown connection.. abort!!");
			qtr_set_abort(qtr, AS_PROTO_RESULT_FAIL_QUERY_NETIO_ERR, __FILE__, __LINE__);
			break;
		} else if (ret == AS_QUERY_DONE) {
			break;
		} else if (ret == AS_QUERY_OK) {
			return;
		}
		// Step 2: Check for timeout
		query_check_timeout(qtr);
		if (qtr_failed(qtr)) {
			qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_TIMEOUT, __FILE__, __LINE__);
			continue;
		}
		// Step 3: Conditionally track
		if (hash_track_qtr(qtr)) {
			qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_DUPLICATE, __FILE__, __LINE__);
			continue;
		}

		// Step 4: If needs user based abort
		if (query_check_bound(qtr)) {
			qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_USERABORT, __FILE__, __LINE__);
			continue;
		}

		// Step 5: Get Next Batch
		loop++;
		int qret    = query_get_nextbatch(qtr);

		cf_detail(AS_QUERY, "Loop=%d, Selected=%"PRIu64", ret=%d", loop, qtr->qctx.n_bdigs, qret);
		switch (qret) {
			case  AS_QUERY_OK:
			case  AS_QUERY_DONE:
				break;
			case  AS_QUERY_ERR:
				continue;
			case  AS_QUERY_CONTINUE:
				continue;
			default:
				cf_warning(AS_QUERY, "Unexpected return type");
				continue;
		}

		if (qret == AS_QUERY_DONE) {
			// In case all physical tree is done return. if not range loop
			// till less than batch size results are returned
#if defined(USE_SYSTEMTAP)
			nrecs = qtr->n_result_records;
#endif
			qtr_set_done(qtr, AS_PROTO_RESULT_OK, __FILE__, __LINE__);
		}

		// Step 6: Prepare Query Request either to process inline or for
		//         queueing up for offline processing
		if (qtr_process(qtr)) {
			qtr_set_err(qtr, AS_PROTO_RESULT_FAIL_QUERY_CBERROR, __FILE__, __LINE__);
			continue;
		}
	}

	if (!qtr_is_abort(qtr)) {
		// Send the fin packet in it is NOT a shutdown
		query_send_fin(qtr);
	}
	// deleting it from the global hash.
	hash_delete_qtr(qtr);
	qtr_release(qtr, __FILE__, __LINE__);
	ASD_QUERY_DONE(nodeid, trid, nrecs);
}

/*
 * Function as_query_worker
 *
 * Notes -
 * 		Process one queue's Query requests.
 * 			- Immediately fail if query has timed out
 * 			- Maximum queries that can be served is number of threads
 *
 * 		Releases the qtr, which will call as_query_trasaction_done
 *
 * Synchronization -
 * 		Takes a global query lock while
 */
void*
query_th(void* q_to_wait_on)
{
	cf_queue *           query_queue = (cf_queue*)q_to_wait_on;
	unsigned int         thread_id    = cf_atomic32_incr(&g_query_threadcnt);
	cf_detail(AS_QUERY, "Query Thread Created %d", thread_id);
	as_query_transaction *qtr         = NULL;

	while (1) {
		// Kill self if thread id is greater than that of number of configured
		// thread
		if (thread_id > g_config.query_threads) {
			pthread_rwlock_rdlock(&g_query_lock);
			if (thread_id > g_config.query_threads) {
				cf_atomic32_decr(&g_query_threadcnt);
				pthread_rwlock_unlock(&g_query_lock);
				cf_detail(AS_QUERY, "Query thread %d exited", thread_id);
				return NULL;
			}
			pthread_rwlock_unlock(&g_query_lock);
		}
		if (cf_queue_pop(query_queue, &qtr, CF_QUEUE_FOREVER) != 0) {
			cf_crash(AS_QUERY, "Failed to pop from Query worker queue.");
		}

		query_generator(qtr);
	}
	return AS_QUERY_OK;
}

/*
 * Parse the UDF OP type to find what type of UDF this is or otherwise not even
 * UDF
 */
query_type
query_get_type(as_transaction* tr)
{
	if (! as_transaction_is_udf(tr)) {
		return QUERY_TYPE_LOOKUP;
	}

	as_msg_field *udf_op_f = as_transaction_has_udf_op(tr) ?
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP) : NULL;

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_UDF_OP_AGGREGATE) {
		return QUERY_TYPE_AGGR;
	}

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_UDF_OP_BACKGROUND) {
		return QUERY_TYPE_UDF_BG;
	}
/*
	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_UDF_OP_FOREGROUND) {
		return QUERY_TYPE_UDF_FG;
	}
*/
	return QUERY_TYPE_UNKNOWN;
}

/*
 * Function aggr_query_init
 */
int
aggr_query_init(as_aggr_call * call, as_transaction *tr)
{
	if (! udf_def_init_from_msg(&call->def, tr)) {
		return AS_QUERY_ERR;
	}

	call->aggr_hooks    = &query_aggr_hooks;
	return AS_QUERY_OK;
}

static int
query_setup_udf_call(as_query_transaction *qtr, as_transaction *tr)
{
	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
			cf_atomic64_incr(&qtr->ns->n_lookup);
			break;
		case QUERY_TYPE_AGGR:
			if (aggr_query_init(&qtr->agg_call, tr) != AS_QUERY_OK) {
				tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
				return AS_QUERY_ERR;
			}
			cf_atomic64_incr(&qtr->ns->n_aggregation);
			break;
		case QUERY_TYPE_UDF_BG:
			if (! udf_def_init_from_msg(&qtr->origin.def, tr)) {
				tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
				return AS_QUERY_ERR;
			}
			break;
		default:
			cf_crash(AS_QUERY, "Invalid QUERY TYPE %d !!!", qtr->job_type);
			break;
	}
	return AS_QUERY_OK;
}

static void
query_setup_fd(as_query_transaction *qtr, as_transaction *tr)
{
	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
		case QUERY_TYPE_AGGR:
			qtr->fd_h                = tr->from.proto_fd_h;
			qtr->fd_h->fh_info      |= FH_INFO_DONOT_REAP;
			break;
		case QUERY_TYPE_UDF_BG:
			qtr->fd_h  = NULL;
			break;
		default:
			cf_crash(AS_QUERY, "Invalid QUERY TYPE %d !!!", qtr->job_type);
			break;
	}
}
/*
 * Phase I query setup which happens just before query is queued for generator
 * Populates valid qtrp in case of success and NULL in case of failure.
 * All the query related parsing code sits here
 *
 * Returns:
 *   AS_QUERY_OK in case of successful
 *   AS_QUERY_DONE in case nothing to be like scan on non-existent set
 *   AS_QUERY_ERR in case of parsing failure
 *
 */
static int
query_setup(as_transaction *tr, as_namespace *ns, as_query_transaction **qtrp)
{

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
	uint64_t trid = tr ? as_transaction_trid(tr) : 0;
#endif

	int rv = AS_QUERY_ERR;
	*qtrp  = NULL;

    ASD_QUERY_STARTING(nodeid, trid);

	uint64_t start_time     = cf_getns();
	as_sindex *si           = NULL;
	cf_vector *binlist      = 0;
	as_sindex_range *srange = 0;
	predexp_eval_t *predexp_eval = NULL;
	char *setname           = NULL;
	as_query_transaction *qtr = NULL;

	bool has_sindex   = as_sindex_ns_has_sindex(ns);
	if (!has_sindex) {
		tr->result_code = AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND;
		cf_debug(AS_QUERY, "No Secondary Index on namespace %s", ns->name);
		goto Cleanup;
	}

	as_msg *m = &tr->msgp->msg;

	// TODO - still lots of redundant msg field parsing (e.g. for set) - fix.
	if ((si = as_sindex_from_msg(ns, m)) == NULL) {
		cf_debug(AS_QUERY, "No Index Defined in the Query");
	}

    ASD_SINDEX_MSGRANGE_STARTING(nodeid, trid);
	int ret = as_sindex_rangep_from_msg(ns, m, &srange);
	if (AS_QUERY_OK != ret) {
		cf_debug(AS_QUERY, "Could not instantiate index range metadata... "
				"Err, %s", as_sindex_err_str(ret));
		tr->result_code = as_sindex_err_to_clienterr(ret, __FILE__, __LINE__);
		goto Cleanup;
	}

	ASD_SINDEX_MSGRANGE_FINISHED(nodeid, trid);
	// get optional set
	as_msg_field *sfp = as_transaction_has_set(tr) ?
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;

	if (sfp) {
		uint32_t setname_len = as_msg_field_get_value_sz(sfp);

		if (setname_len >= AS_SET_NAME_MAX_SIZE) {
			cf_warning(AS_QUERY, "set name too long");
			tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
			goto Cleanup;
		}

		if (setname_len != 0) {
			setname = cf_strndup((const char *)sfp->data, setname_len);
		}
	}

	if (si) {

		if (! as_sindex_can_query(si)) {
			tr->result_code = as_sindex_err_to_clienterr(
					AS_SINDEX_ERR_NOT_READABLE, __FILE__, __LINE__);
			goto Cleanup;
		}
	} else {
		// Look up sindex by bin in the query in case not
		// specified in query
		si = as_sindex_from_range(ns, setname, srange);
	}

	if (as_transaction_has_predexp(tr)) {
		as_msg_field * pfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_PREDEXP);
		predexp_eval = predexp_build(pfp);
		if (! predexp_eval) {
			cf_warning(AS_QUERY, "Failed to build predicate expression");
			tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
			goto Cleanup;
		}
	}
	
	int numbins = 0;
	// Populate binlist to be Projected by the Query
	binlist = as_sindex_binlist_from_msg(ns, m, &numbins);

	// If anyone of the bin in the bin is bad, fail the query
	if (numbins != 0 && !binlist) {
		tr->result_code = AS_PROTO_RESULT_FAIL_INDEX_GENERIC;
		goto Cleanup;
	}

	if (!has_sindex || !si) {
		tr->result_code = AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND;
		goto Cleanup;
	}

	// quick check if there is any data with the certain set name
	if (setname && as_namespace_get_set_id(ns, setname) == INVALID_SET_ID) {
		cf_info(AS_QUERY, "Query on non-existent set %s", setname);
		tr->result_code = AS_PROTO_RESULT_OK;
		rv              = AS_QUERY_DONE;
		goto Cleanup;
	}
	cf_detail(AS_QUERY, "Query on index %s ",
			((as_sindex_metadata *)si->imd)->iname);

	query_type qtype = query_get_type(tr);
	if (qtype == QUERY_TYPE_UNKNOWN) {
		tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
		rv              = AS_QUERY_ERR;
		goto Cleanup;
	}

	if (qtype == QUERY_TYPE_AGGR && as_transaction_has_predexp(tr)) {
		cf_warning(AS_QUERY, "aggregation queries do not support predexp filters");
		tr->result_code = AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
		rv              = AS_QUERY_ERR;
		goto Cleanup;
	}

	ASD_QUERY_QTRSETUP_STARTING(nodeid, trid);
	qtr = qtr_alloc();
	if (!qtr) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		goto Cleanup;
	}
	ASD_QUERY_QTR_ALLOC(nodeid, trid, (void *) qtr);
	// Be aware of the size of qtr
	// Memset it partial
	memset(qtr, 0, offsetof(as_query_transaction, bkey));

	ASD_QUERY_QTRSETUP_FINISHED(nodeid, trid);

	qtr->ns = ns;
	qtr->job_type = qtype;

	if (query_setup_udf_call(qtr, tr)) {
		rv = AS_QUERY_ERR;
		cf_free(qtr);
		goto Cleanup;
	}

	query_setup_fd(qtr, tr);

	if (qtr->job_type == QUERY_TYPE_LOOKUP) {
		qtr->predexp_eval = predexp_eval;
		qtr->no_bin_data = (m->info1 & AS_MSG_INFO1_GET_NO_BINS) != 0;
	}
	else if (qtr->job_type == QUERY_TYPE_UDF_BG) {
		qtr->origin.predexp = predexp_eval;
		qtr->origin.cb     = query_udf_bg_tr_complete;
		qtr->origin.udata  = (void *)qtr;
		qtr->is_durable_delete = as_transaction_is_durable_delete(tr);
	}

	// Consume everything from tr rest will be picked up in init
	qtr->trid                = as_transaction_trid(tr);
	qtr->setname             = setname;
	qtr->si                  = si;
	qtr->srange              = srange;
	qtr->binlist             = binlist;
	qtr->start_time          = start_time;
	qtr->end_time            = tr->end_time;
	qtr->rsv                 = NULL;

	rv = AS_QUERY_OK;

	pthread_mutex_init(&qtr->slock, NULL);
	qtr->state         = AS_QTR_STATE_INIT;
	qtr->do_requeue    = false;
	qtr->short_running = true;

	*qtrp = qtr;
	return rv;

Cleanup:
	// Pre Query Setup Failure
	if (setname)     cf_free(setname);
	if (si)          AS_SINDEX_RELEASE(si);
	if (predexp_eval) predexp_destroy(predexp_eval);
	if (srange)      as_sindex_range_free(&srange);
	if (binlist)     cf_vector_destroy(binlist);
	return rv;
}

/*
 *	Arguments -
 *		tr - transaction coming from the client.
 *
 *	Returns -
 *		AS_QUERY_OK  - on success. Responds, frees msgp and proto_fd
 *		AS_QUERY_ERR - on failure. That means the query was not even started.
 *		               frees msgp, response is responsibility of caller
 *
 * 	Notes -
 * 		Allocates and reserves the qtr if query_in_transaction_thr
 * 		is set to false or data is in not in memory.
 * 		Has the responsibility to free tr->msgp.
 * 		Either call query_transaction_done or Cleanup to free the msgp
 */
int
as_query(as_transaction *tr, as_namespace *ns)
{
	if (tr) {
		QUERY_HIST_INSERT_DATA_POINT(query_txn_q_wait_hist, tr->start_time);
	}

	as_query_transaction *qtr;
	int rv = query_setup(tr, ns, &qtr);

	if (rv == AS_QUERY_DONE) {
		// Send FIN packet to client to ignore this.
		bool force_close = ! as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_PROTO_RESULT_OK);
		query_release_fd(tr->from.proto_fd_h, force_close);
		tr->from.proto_fd_h = NULL; // Paranoid
		return AS_QUERY_OK;
	} else if (rv == AS_QUERY_ERR) {
		// tsvc takes care of managing fd
		return AS_QUERY_ERR;
	}

	if (g_config.query_in_transaction_thr) {
		if (qtr->job_type == QUERY_TYPE_UDF_BG) {
			query_send_bg_udf_response(tr);
		}
		query_generator(qtr);
	} else {
		if (query_qtr_enqueue(qtr, false)) {
			// This error will be accounted by thr_tsvc layer. Thus
			// reset fd_h before calling qtr release, and let the
			// transaction handler deal with the failure.
			qtr->fd_h           = NULL;
			qtr_release(qtr, __FILE__, __LINE__);
			tr->result_code     = AS_PROTO_RESULT_FAIL_QUERY_QUEUEFULL;
			return AS_QUERY_ERR;
		}
		// Respond after queuing is successfully.
		if (qtr->job_type == QUERY_TYPE_UDF_BG) {
			query_send_bg_udf_response(tr);
		}
	}

	// Query engine will reply to queued query as needed.
	tr->from.proto_fd_h = NULL;
	return AS_QUERY_OK;
}
// **************************************************************************************************


/*
 * Query Utility and Monitoring functions
 */
// **************************************************************************************************

// Find matching trid and kill the query
int
as_query_kill(uint64_t trid)
{
	as_query_transaction *qtr;
	int rv = hash_get_qtr(trid, &qtr);

	if (rv != AS_QUERY_OK) {
		cf_warning(AS_QUERY, "Cannot kill query with trid [%"PRIu64"]",  trid);
	} else {
		qtr_set_abort(qtr, AS_PROTO_RESULT_FAIL_QUERY_USERABORT, __FILE__, __LINE__);
		rv = AS_QUERY_OK;
		qtr_release(qtr, __FILE__, __LINE__);
	}

	return rv;
}

// Find matching trid and set priority
int
as_query_set_priority(uint64_t trid, uint32_t priority)
{
	as_query_transaction *qtr;
	int rv = hash_get_qtr(trid, &qtr);

	if (rv != AS_QUERY_OK) {
		cf_warning(AS_QUERY, "Cannot set priority for query with trid [%"PRIu64"]",  trid);
	} else {
		uint32_t old_priority = qtr->priority;
		qtr->priority = priority;
		cf_info(AS_QUERY, "Query priority changed from %d to %d", old_priority, priority);
		rv = AS_QUERY_OK;
		qtr_release(qtr, __FILE__, __LINE__);
	}
	return rv;
}

int
as_query_list_job_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	as_query_transaction * qtr = (as_query_transaction*)object;
	cf_dyn_buf * db = (cf_dyn_buf*) udata;

	cf_dyn_buf_append_string(db, "trid=");
	cf_dyn_buf_append_uint64(db, qtr->trid);
	cf_dyn_buf_append_string(db, ":job_type=");
	cf_dyn_buf_append_int(db, qtr->job_type);
	cf_dyn_buf_append_string(db, ":n_result_records=");
	cf_dyn_buf_append_uint64(db, cf_atomic_int_get(qtr->n_result_records));
	cf_dyn_buf_append_string(db, ":run_time=");
	cf_dyn_buf_append_uint64(db, (cf_getns() - qtr->start_time) / 1000);
	cf_dyn_buf_append_string(db, ":state=");
	if(qtr_failed(qtr)) {
		cf_dyn_buf_append_string(db, "ABORTED");
	} else {
		cf_dyn_buf_append_string(db, "RUNNING");
	}
	cf_dyn_buf_append_string(db, ";");
	return AS_QUERY_OK;
}

// Lists thr current running queries
int
as_query_list(char *name, cf_dyn_buf *db)
{
	uint32_t size = cf_rchash_get_size(g_query_job_hash);
	// No elements in the query job hash, return failure
	if (!size) {
		cf_dyn_buf_append_string(db, "No running queries");
	}
	// Else go through all the jobs in the hash and list their statistics
	else {
		cf_rchash_reduce(g_query_job_hash, as_query_list_job_reduce_fn, db);
		cf_dyn_buf_chomp(db);
	}
	return AS_QUERY_OK;
}


// query module to monitor
void
as_query_fill_jobstat(as_query_transaction *qtr, as_mon_jobstat *stat)
{
	stat->trid          = qtr->trid;
	stat->cpu           = 0;                               // not implemented
	stat->run_time      = (cf_getns() - qtr->start_time) / 1000000;
	stat->recs_read     = qtr->n_read_success;
	stat->net_io_bytes  = qtr->net_io_bytes;
	stat->priority      = qtr->priority;

	// Not implemented:
	stat->progress_pct    = 0;
	stat->time_since_done = 0;
	stat->job_type[0]     = '\0';

	strcpy(stat->ns, qtr->ns->name);

	if (qtr->setname) {
		strcpy(stat->set, qtr->setname);
	} else {
		strcpy(stat->set, "NULL");
	}

	strcpy(stat->status, "active");

	char *specific_data   = stat->jdata;
	sprintf(specific_data, ":sindex-name=%s:", qtr->si->imd->iname);
}

/*
 * Populates the as_mon_jobstat and returns to mult-key lookup monitoring infrastructure.
 * Serves as a callback function
 *
 * Returns -
 * 		NULL - In case of failure.
 * 		as_mon_jobstat - On success.
 */
as_mon_jobstat *
as_query_get_jobstat(uint64_t trid)
{
	as_mon_jobstat *stat;
	as_query_transaction *qtr;
	int rv = hash_get_qtr(trid, &qtr);

	if (rv != AS_QUERY_OK) {
		cf_warning(AS_MON, "No query was found with trid [%"PRIu64"]", trid);
		stat = NULL;
	}
	else {
		stat = cf_malloc(sizeof(as_mon_jobstat));
		as_query_fill_jobstat(qtr, stat);
		qtr_release(qtr, __FILE__, __LINE__);
	}
	return stat;
}


int
as_mon_query_jobstat_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	as_query_transaction * qtr = (as_query_transaction*)object;
	query_jobstat *job_pool = (query_jobstat*) udata;

	if ( job_pool->index >= job_pool->max_size) return AS_QUERY_OK;
	as_mon_jobstat * stat = *(job_pool->jobstat);
	stat                  = stat + job_pool->index;
	as_query_fill_jobstat(qtr, stat);
	(job_pool->index)++;
	return AS_QUERY_OK;
}

as_mon_jobstat *
as_query_get_jobstat_all(int * size)
{
	*size = cf_rchash_get_size(g_query_job_hash);
	if(*size == 0) return AS_QUERY_OK;

	as_mon_jobstat     * job_stats;
	query_jobstat     job_pool;

	job_stats          = (as_mon_jobstat *) cf_malloc(sizeof(as_mon_jobstat) * (*size));
	job_pool.jobstat  = &job_stats;
	job_pool.index    = 0;
	job_pool.max_size = *size;
	cf_rchash_reduce(g_query_job_hash, as_mon_query_jobstat_reduce_fn, &job_pool);
	*size              = job_pool.index;
	return job_stats;
}

void
as_query_histogram_dumpall()
{
	if (g_config.query_enable_histogram == false)
	{
		return;
	}

	if (query_txn_q_wait_hist) {
		histogram_dump(query_txn_q_wait_hist);
	}
	if (query_query_q_wait_hist) {
		histogram_dump(query_query_q_wait_hist);
	}
	if (query_prepare_batch_hist) {
		histogram_dump(query_prepare_batch_hist);
	}
	if (query_batch_io_q_wait_hist) {
		histogram_dump(query_batch_io_q_wait_hist);
	}
	if (query_batch_io_hist) {
		histogram_dump(query_batch_io_hist);
	}
	if (query_net_io_hist) {
		histogram_dump(query_net_io_hist);
	}
}


/*
 * Query Subsystem Initialization function
 */
// **************************************************************************************************
void
as_query_gconfig_default(as_config *c)
{
	// NB: Do not change query_threads default to odd. as_query_reinit code cannot
	// handle it. Code to handle it is unnecessarily complicated code, hence opted
	// to make the default value even.
	c->query_threads             = 6;
	c->query_worker_threads      = 15;
	c->query_priority            = 10;
	c->query_sleep_us            = 1;
	c->query_bsize               = QUERY_BATCH_SIZE;
	c->query_in_transaction_thr  = 0;
	c->query_req_max_inflight    = AS_QUERY_MAX_QREQ_INFLIGHT;
	c->query_bufpool_size        = AS_QUERY_MAX_BUFS;
	c->query_short_q_max_size    = AS_QUERY_MAX_SHORT_QUEUE_SZ;
	c->query_long_q_max_size     = AS_QUERY_MAX_LONG_QUEUE_SZ;
	c->query_buf_size            = AS_QUERY_BUF_SIZE;
	c->query_threshold           = 10;	// threshold after which the query is considered long running
										// no reason for choosing 10
	c->query_rec_count_bound     = UINT64_MAX; // Unlimited
	c->query_req_in_query_thread = 0;
	c->query_untracked_time_ms   = AS_QUERY_UNTRACKED_TIME;

	c->partitions_pre_reserved       = false;
}


void
as_query_init()
{
	g_current_queries_count = 0;
	cf_detail(AS_QUERY, "Initialize %d Query Worker threads.", g_config.query_threads);

	// global job hash to keep track of the query job
	cf_rchash_create(&g_query_job_hash, cf_rchash_fn_u32, NULL, sizeof(uint64_t), 64, CF_RCHASH_MANY_LOCK);

	// I/O threads
	g_query_qwork_pool = cf_queue_create(sizeof(query_work *), true);
	g_query_response_bb_pool = cf_queue_create(sizeof(void *), true);
	g_query_work_queue = cf_queue_create(sizeof(query_work *), true);

	// Create the query worker threads detached so we don't need to join with them.
	if (pthread_attr_init(&g_query_worker_th_attr)) {
		cf_crash(AS_SINDEX, "failed to initialize the query worker thread attributes");
	}
	if (pthread_attr_setdetachstate(&g_query_worker_th_attr, PTHREAD_CREATE_DETACHED)) {
		cf_crash(AS_SINDEX, "failed to set the query worker thread attributes to the detached state");
	}
	int max = g_config.query_worker_threads;
	for (int i = 0; i < max; i++) {
		pthread_create(&g_query_worker_threads[i], &g_query_worker_th_attr,
				qwork_th, (void*)g_query_work_queue);
	}

	g_query_short_queue = cf_queue_create(sizeof(as_query_transaction *), true);
	g_query_long_queue = cf_queue_create(sizeof(as_query_transaction *), true);

	// Create the query threads detached so we don't need to join with them.
	if (pthread_attr_init(&g_query_th_attr)) {
		cf_crash(AS_SINDEX, "failed to initialize the query thread attributes");
	}
	if (pthread_attr_setdetachstate(&g_query_th_attr, PTHREAD_CREATE_DETACHED)) {
		cf_crash(AS_SINDEX, "failed to set the query thread attributes to the detached state");
	}

	max = g_config.query_threads;
	for (int i = 0; i < max; i += 2) {
		if (pthread_create(&g_query_threads[i], &g_query_th_attr,
					query_th, (void*)g_query_short_queue)
				|| pthread_create(&g_query_threads[i + 1], &g_query_th_attr,
						query_th, (void*)g_query_long_queue)) {
			cf_crash(AS_QUERY, "Failed to create query transaction threads for query short queue");
		}
	}

	char hist_name[64];

	sprintf(hist_name, "query_txn_q_wait_us");
	query_txn_q_wait_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "query_query_q_wait_us");
	query_query_q_wait_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "query_prepare_batch_us");
	query_prepare_batch_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "query_batch_io_q_wait_us");
	query_batch_io_q_wait_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "query_batch_io_us");
	query_batch_io_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	sprintf(hist_name, "query_net_io_us");
	query_net_io_hist = histogram_create(hist_name, HIST_MICROSECONDS);

	g_config.query_enable_histogram	 = false;
}

/*
 * 	Description -
 * 		It tries to set the query_worker_threads to the given value.
 *
 * 	Synchronization -
 * 		Takes a global query lock to protect the config of
 *
 *	Arguments -
 *		set_size - Value which one want to assign to query_threads.
 *
 * 	Returns -
 * 		AS_QUERY_OK  - On successful resize of query threads.
 * 		AS_QUERY_ERR - Either the set_size exceeds AS_QUERY_MAX_THREADS
 * 					   OR Query threads were not initialized on the first place.
 */
int
as_query_worker_reinit(int set_size, int *actual_size)
{
	if (set_size > AS_QUERY_MAX_WORKER_THREADS) {
		cf_warning(AS_QUERY, "Cannot increase query threads more than %d",
				AS_QUERY_MAX_WORKER_THREADS);
		//unlock
		return AS_QUERY_ERR;
	}

	pthread_rwlock_wrlock(&g_query_lock);
	// Add threads if count is increased
	int i = cf_atomic32_get(g_query_worker_threadcnt);
	g_config.query_worker_threads = set_size;
	if (set_size > g_query_worker_threadcnt) {
		for (; i < set_size; i++) {
			cf_detail(AS_QUERY, "Creating thread %d", i);
			if (0 != pthread_create(&g_query_worker_threads[i], &g_query_worker_th_attr,
					qwork_th, (void*)g_query_work_queue)) {
				break;
			}
		}
		g_config.query_worker_threads = i;
	}
	*actual_size = g_config.query_worker_threads;

	pthread_rwlock_unlock(&g_query_lock);

	return AS_QUERY_OK;
}

/*
 * 	Description -
 * 		It tries to set the query_threads to the given value.
 *
 * 	Synchronization -
 * 		Takes a global query lock to protect the config of
 *
 *	Arguments -
 *		set_size - Value which one want to assign to query_threads.
 *
 * 	Returns -
 * 		AS_QUERY_OK  - On successful resize of query threads.
 * 		AS_QUERY_ERR - Either the set_size exceeds AS_QUERY_MAX_THREADS
 * 					   OR Query threads were not initialized on the first place.
 */
int
as_query_reinit(int set_size, int *actual_size)
{
	if (set_size > AS_QUERY_MAX_THREADS) {
		cf_warning(AS_QUERY, "Cannot increase query threads more than %d",
				AS_QUERY_MAX_THREADS);
		return AS_QUERY_ERR;
	}

	pthread_rwlock_wrlock(&g_query_lock);
	// Add threads if count is increased
	int i = cf_atomic32_get(g_query_threadcnt);

	// make it multiple of 2
	if (set_size % 2 != 0)
		set_size++;

	g_config.query_threads = set_size;
	if (set_size > g_query_threadcnt) {
		for (; i < set_size; i++) {
			cf_detail(AS_QUERY, "Creating thread %d", i);
			if (0 != pthread_create(&g_query_threads[i], &g_query_th_attr,
					query_th, (void*)g_query_short_queue)) {
				break;
			}
			i++;
			if (0 != pthread_create(&g_query_threads[i], &g_query_th_attr,
					query_th, (void*)g_query_long_queue)) {
				break;
			}
		}
		g_config.query_threads = i;
	}
	*actual_size = g_config.query_threads;

	pthread_rwlock_unlock(&g_query_lock);

	return AS_QUERY_OK;
}
// **************************************************************************************************
