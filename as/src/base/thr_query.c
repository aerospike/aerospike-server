/*
 * thr_query.c
 *
 * Copyright (C) 2012-2021 Aerospike, Inc.
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

#include "aerospike/as_atomic.h"
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
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "rchash.h"
#include "shash.h"

#include "base/aggr.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/expop.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/stats.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "geospatial/geospatial.h"
#include "sindex/secondary_index.h"
#include "transaction/udf.h"
#include "transaction/write.h"

#include "ai_btree.h"
#include "bt.h"
#include "bt_iterator.h"

#include "warnings.h"

#define MAX_OUTSTANDING_IO_REQ 2

#define QUERY_BATCH_SIZE 100
#define AS_QUERY_BUF_SIZE 1024 * 1024 * 7 / 4 // 1.75M is faster than 2M
#define AS_QUERY_MAX_QREQ_INFLIGHT 100 // worker queue capping per query
#define AS_QUERY_MAX_SHORT_QUEUE_SZ 500 // maximum 500 outstanding short running queries
#define AS_QUERY_MAX_LONG_QUEUE_SZ 500 // maximum 500 outstanding long  running queries
#define MAX_UDF_TRANSACTIONS 20 // Higher the value more aggressive it will be
#define MAX_OPS_TRANSACTIONS 20 // Higher the value more aggressive it will be
#define AS_QUERY_UNTRACKED_TIME 1000 // (millisecond) 1 sec

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
 * Query Transaction Type
 */
// **************************************************************************************************
typedef enum {
	QUERY_TYPE_LOOKUP  = 0,
	QUERY_TYPE_AGGR    = 1,
	QUERY_TYPE_UDF_BG  = 2,
	QUERY_TYPE_OPS_BG  = 3,

	QUERY_TYPE_UNKNOWN  = -1
} query_type;

typedef enum {
	AS_QUERY_ERR = -1,
	AS_QUERY_OK = 0,
	AS_QUERY_CONTINUE = 1,
	AS_QUERY_DONE = 2
} as_query_status;

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
	as_query_range         * srange;
	query_type               job_type;  // Job type [LOOKUP/AGG/UDF]
	bool                     no_bin_data;
	as_exp                 * filter_exp;
	cf_vector              * binlist;
	as_file_handle         * fd_h;      // ref counted nonetheless
	/************************** Run Time Data *********************************/
	uint32_t                 priority;
	uint64_t                 start_time;               // Start time
	uint64_t                 end_time;                 // timeout value
	uint64_t                 start_time_clepoch;

	/*
 	* MT (Single Writer / Single Threaded / Multiple Readers)
 	* Atomics or no Protection
 	*/
	/****************** Stats (only generator) ***********************/
	uint64_t                 querying_ai_time_ns;  // Time spent by query to run lookup secondary index trees.
	uint64_t                 n_recs;               // Record handles read from secondary index
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
	uint64_t                 n_filtered_meta;
	uint64_t                 n_filtered_bins;

	/********************** Query Progress ***********************************/
	cf_atomic32              n_qwork_active;
	cf_atomic32              n_io_outstanding;
	cf_atomic32              n_udf_tr_queued;    				// Throttling: max in flight scan
	cf_atomic32              n_ops_tr_queued;    				// Throttling: max in flight scan

	/********************* Net IO packet order *******************************/
	cf_atomic32              netio_push_seq;
	cf_atomic32              netio_pop_seq;

	/********************** IO Buf Builder ***********************************/
	cf_mutex                 buf_mutex;
	bool                     compress_response;
	cf_buf_builder         * bb;
	/****************** Query State and Result Code **************************/
	cf_mutex                 slock;
	bool                     do_requeue;
	qtr_state                state;
	uint8_t                  result_code;

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
	iudf_origin              iudf_orig; // Background UDF details
	iops_origin              iops_orig; // Background ops details
	as_sindex_qctx           qctx;     // Secondary Index details
} as_query_transaction;
// **************************************************************************************************

/*
 * Query Request
 */
// **************************************************************************************************
typedef struct query_work_s {
	as_query_transaction *qtr;
	cf_ll *recl;
	uint64_t queued_time_ns;
	cf_buf_builder *bb;
} query_work;
// **************************************************************************************************

/*
 * Job Monitoring
 */
// **************************************************************************************************
typedef struct query_jobstat_s {
	int               index;
	as_mon_jobstat   *jobstat;
	int               max_size;
} query_jobstat;
// **************************************************************************************************

/*
 * Query state source location helper macros.
 */
// **************************************************************************************************

#define QTR_SET_ABORT(qtr, res) qtr_set_abort(qtr, res, __FILE__, __LINE__)
#define QTR_SET_ERR(qtr, res) qtr_set_err(qtr, res, __FILE__, __LINE__)
#define QTR_SET_DONE(qtr, res) qtr_set_done(qtr, res, __FILE__, __LINE__)

#define QTR_RELEASE(qtr) qtr_release(qtr, __FILE__, __LINE__)
#define QTR_RESERVE(qtr) qtr_reserve(qtr, __FILE__, __LINE__)

#define QTR_FINISH_WORK(qtr, stat, release) \
	qtr_finish_work(qtr, stat, release, __FILE__, __LINE__)

// **************************************************************************************************

/*
 * Query Engine Global
 */
// **************************************************************************************************
static int              g_current_queries_count = 0;
static pthread_rwlock_t g_query_lock
						= PTHREAD_RWLOCK_WRITER_NONRECURSIVE_INITIALIZER_NP;
static cf_rchash      * g_query_job_hash = NULL;

// GENERATOR
static cf_queue       * g_query_short_queue     = 0;
static cf_queue       * g_query_long_queue      = 0;
static cf_atomic32      g_query_threadcnt       = 0;

cf_atomic32             g_query_short_running   = 0;
cf_atomic32             g_query_long_running    = 0;

// I/O & AGGREGATOR
static cf_queue     *  g_query_work_queue    = 0;
static cf_atomic32     g_query_worker_threadcnt = 0;
// **************************************************************************************************

/*
 * Forward Declaration
 */
// **************************************************************************************************

static bool process_keys_arr_ele(as_query_transaction *qtr, as_index_keys_arr *k_a, uint32_t ix, as_index_ref *r_ref);
static int release_r_h_cb(cf_ll_element *ele, void *udata);
static void qtr_finish_work(as_query_transaction *qtr, cf_atomic32 *stat, bool release, const char *fname, int lineno);

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
		cf_mutex_lock(&qtr->slock);
	}
}
static void
qtr_unlock(as_query_transaction *qtr) {
	if (qtr) {
		cf_mutex_unlock(&qtr->slock);
	}
}
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
qtr_set_abort(as_query_transaction *qtr, uint8_t result_code, const char *fname,
		int lineno)
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
qtr_set_err(as_query_transaction *qtr, uint8_t result_code, const char *fname,
		int lineno)
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
qtr_set_done(as_query_transaction *qtr, uint8_t result_code, const char *fname,
		int lineno)
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
	uint64_t deadline = qtr->end_time;

	if (deadline == 0) {
		return;
	}

	uint64_t now = cf_getns();

	if (now > deadline) {
		cf_debug(AS_QUERY, "Query timed out %lu %lu", now, deadline);
		QTR_SET_ERR(qtr, AS_ERR_QUERY_TIMEOUT);
	}
}
// **************************************************************************************************


/*
 * Query Destructor Function
 */
// **************************************************************************************************
/*
 * NB: These stats come into picture only if query really started
 * running. If it fails before even running it is accounted in
 * fail
 */
static inline void
query_update_stats(as_query_transaction *qtr)
{
	uint64_t rows = cf_atomic64_get(qtr->n_result_records);
	as_sindex_stat *si_stats = &qtr->si->stats;
	as_namespace *ns = qtr->ns;

	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&si_stats->n_query_basic_abort);
				cf_atomic64_incr(&ns->n_query_basic_abort);
			}
			else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&si_stats->n_query_basic_error);
				cf_atomic64_incr(&ns->n_query_basic_error);
			}
			else {
				cf_atomic64_incr(&si_stats->n_query_basic_complete);
				cf_atomic64_incr(&ns->n_query_basic_complete);
			}

			cf_atomic64_add(&si_stats->n_query_basic_records, (int64_t)rows);
			cf_atomic64_add(&ns->n_query_basic_records, (int64_t)rows);
			break;

		case QUERY_TYPE_AGGR:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&ns->n_query_aggr_abort);
			}
			else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&ns->n_query_aggr_error);
			}
			else {
				cf_atomic64_incr(&ns->n_query_aggr_complete);
			}

			cf_atomic64_add(&ns->n_query_aggr_records, (int64_t)rows);
			break;

		case QUERY_TYPE_UDF_BG:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&ns->n_query_udf_bg_abort);
			}
			else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&ns->n_query_udf_bg_error);
			}
			else {
				cf_atomic64_incr(&ns->n_query_udf_bg_complete);
			}
			break;

		case QUERY_TYPE_OPS_BG:
			if (qtr->state == AS_QTR_STATE_ABORT) {
				cf_atomic64_incr(&ns->n_query_ops_bg_abort);
			}
			else if (qtr->state == AS_QTR_STATE_ERR) {
				cf_atomic64_incr(&ns->n_query_ops_bg_error);
			}
			else {
				cf_atomic64_incr(&ns->n_query_ops_bg_complete);
			}
			break;

		default:
			cf_crash(AS_QUERY, "Unknown Query Type !!");
			break;
	}

	// Can't use macro that tr and rw use.
	ns->query_hist_active = true;
	histogram_insert_data_point(ns->query_hist, qtr->start_time);

	SINDEX_HIST_INSERT_DATA_POINT(qtr->si, query_hist, qtr->start_time);

	if (qtr->querying_ai_time_ns) {
		QUERY_HIST_INSERT_RAW(query_prepare_batch_hist, qtr->querying_ai_time_ns);
	}

	if (qtr->n_recs > 0) {
		SINDEX_HIST_INSERT_RAW(qtr->si, query_rcnt_hist, qtr->n_recs);
		if (rows) {
			// Can't use macro that tr and rw use.
			ns->query_rec_count_hist_active = true;
			histogram_insert_raw(ns->query_rec_count_hist, rows);

			SINDEX_HIST_INSERT_RAW(qtr->si, query_diff_hist,
					qtr->n_recs - rows);
		}
	}

	uint64_t query_stop_time = cf_getns();
	uint64_t elapsed_us = (query_stop_time - qtr->start_time) / 1000;

	cf_detail(AS_QUERY, "Total time elapsed %lu us, %lu of %lu read operations avg latency %lu us",
			elapsed_us, rows, qtr->n_recs, rows > 0 ? elapsed_us / rows : 0);
}

static void
query_run_teardown(as_query_transaction *qtr)
{
	query_update_stats(qtr);

	if (qtr->n_udf_tr_queued != 0) {
		cf_warning(AS_QUERY, "QUEUED UDF not equal to zero when query transaction is done");
	}

	as_sindex_qctx *qctx = &qtr->qctx;

	if (qctx->recl) {
		cf_ll_reduce(qctx->recl, true, release_r_h_cb, qtr);
		cf_free(qctx->recl);
	}

	if (qctx->r_h_hash != NULL) {
		cf_shash_destroy(qctx->r_h_hash);
	}

	// Handles both pre-reservation and lazy reservation.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_index_tree *tree = qctx->reserved_trees[pid];

		if (tree != NULL) {
			as_partition_tree_release(qtr->ns, tree);
		}
	}

	if (qtr->short_running) {
		cf_atomic32_decr(&g_query_short_running);
	} else {
		cf_atomic32_decr(&g_query_long_running);
	}

	if (qtr->bb != NULL) { // unclean finish
		cf_buf_builder_free(qtr->bb);
	}

	cf_mutex_destroy(&qtr->buf_mutex);
}

static void
srange_free(as_query_range *srange)
{
	if (srange->bin_type == AS_PARTICLE_TYPE_GEOJSON) {
		cf_free(srange->u.geo.r);

		if (srange->u.geo.region) {
			geo_region_destroy(srange->u.geo.region);
		}
	}

	cf_free(srange);
}

static void
query_teardown(as_query_transaction *qtr)
{
	if (qtr->srange)      srange_free(qtr->srange);
	if (qtr->si)          as_sindex_release(qtr->si);
	if (qtr->binlist)     cf_vector_destroy(qtr->binlist);
	if (qtr->setname)     cf_free(qtr->setname);

	as_exp_destroy(qtr->filter_exp);

	if (qtr->job_type == QUERY_TYPE_AGGR && qtr->agg_call.def.arglist) {
		as_list_destroy(qtr->agg_call.def.arglist);
	}
	else if (qtr->job_type == QUERY_TYPE_UDF_BG) {
		iudf_origin_destroy(&qtr->iudf_orig);
	}
	else if (qtr->job_type == QUERY_TYPE_OPS_BG) {
		iops_origin_destroy(&qtr->iops_orig);
	}

	cf_mutex_destroy(&qtr->slock);
}

static void
query_release_fd(as_file_handle *fd_h, bool force_close)
{
	if (fd_h) {
		fd_h->last_used = cf_getns();
		as_end_of_transaction(fd_h, force_close);
	}
}

static void
query_transaction_done(as_query_transaction *qtr)
{
	if (!qtr) {
		return;
	}

	if (qtr_started(qtr)) {
		query_run_teardown(qtr);
	}

	// if query is aborted force close connection.
	// Not to be reused
	query_release_fd(qtr->fd_h, qtr_is_abort(qtr));
	qtr->fd_h = NULL;

	query_teardown(qtr);

	cf_rc_free(qtr);
}

static bool
release_ref_lock_live(as_query_transaction *qtr, as_index_ref *r_ref)
{
	as_record *r = r_ref->r;
	uint32_t pid = as_partition_getid(&r->keyd);
	as_index_tree *tree = qtr->qctx.reserved_trees[pid];

	as_index_vlock(tree, r_ref);

	as_index_release(r);

	if (r->tombstone == 1 || ! as_index_is_valid_record(r)) {
		as_record_done(r_ref, qtr->ns);
		return false;
	}

	return true;
}

// **************************************************************************************************


/*
 * Query Transaction Ref Counts
 */
// **************************************************************************************************
static void
qtr_release(as_query_transaction *qtr, const char *fname, int lineno)
{
	int val = cf_rc_release(qtr);

	if (val == 0) {
		query_transaction_done(qtr);
	}

	cf_detail(AS_QUERY, "Released qtr [%s:%d] %p %d", fname, lineno, qtr, val);
}

static void
qtr_reserve(as_query_transaction *qtr, const char *fname, int lineno)
{
	int val = cf_rc_reserve(qtr);

	cf_detail(AS_QUERY, "Reserved qtr [%s:%d] %p %d ", fname, lineno, qtr, val);
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
static int
query_netio_start_cb(void *udata, uint32_t seq)
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
		return AS_NETIO_ERR;
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
static int
query_netio_finish_cb(void *data, int retcode)
{
	as_netio *io               = (as_netio *)data;
	cf_detail(AS_QUERY, "Query Finish Callback io seq %d with retCode %d", io->seq, retcode);
	as_query_transaction *qtr  = (as_query_transaction *)io->data;
	if (qtr && (retcode != AS_NETIO_CONTINUE)) {
		// If send success make stat is updated
		if (retcode == AS_NETIO_OK) {
			cf_atomic64_add(&qtr->net_io_bytes,
					(int64_t)(io->bb->used_sz + 8));
		} else {
			QTR_SET_ABORT(qtr, AS_ERR_QUERY_NET_IO);
		}
		QUERY_HIST_INSERT_DATA_POINT(query_net_io_hist, io->start_time);

		// Undo the increment from query_netio(). Cannot reach zero here: the
		// increment owned by the transaction will only be undone after all netio
		// is complete.
		cf_rc_release(io->fd_h);
		io->fd_h = NULL;
		cf_buf_builder_free(io->bb);

		cf_atomic32_incr(&qtr->netio_pop_seq);

		QTR_FINISH_WORK(qtr, &qtr->n_io_outstanding, true);
	}
	return retcode;
}

static bool
query_netio_wait(const as_query_transaction *qtr)
{
	return cf_atomic32_get(qtr->n_io_outstanding) <= MAX_OUTSTANDING_IO_REQ;
}

static int
query_netio(as_query_transaction *qtr, cf_buf_builder *bb)
{
	// Nothing to send - 8 bytes is for the header.
	if (bb->used_sz == 8) {
		cf_buf_builder_free(bb);
		return AS_NETIO_OK;
	}

	as_netio io;

	io.finish_cb = query_netio_finish_cb;
	io.start_cb = query_netio_start_cb;

	QTR_RESERVE(qtr);
	io.data = qtr;

	cf_rc_reserve(qtr->fd_h);
	io.fd_h = qtr->fd_h;

	io.bb = bb;
	io.offset = 0;

	cf_atomic32_incr(&qtr->n_io_outstanding);
	io.seq = (uint32_t)cf_atomic32_incr(&qtr->netio_push_seq);

	io.slow = false;
	io.compress_response = qtr->compress_response;
	io.comp_stat = &qtr->ns->query_comp_stat;
	io.start_time = cf_getns();

	return as_netio_send(&io);
}
// **************************************************************************************************


/*
 * Query Reservation Abstraction
 */
// **************************************************************************************************

bool
as_query_reserve_partition_tree(as_namespace *ns, as_sindex_qctx *qctx,
		uint32_t pid)
{
	if (qctx->reserved_trees[pid] != NULL) {
		return true;
	}

	qctx->reserved_trees[pid] = as_partition_tree_reserve_query(ns, pid);

	return qctx->reserved_trees[pid] != NULL;
}

// **************************************************************************************************


/*
 * Query tracking
 */
// **************************************************************************************************
// Put qtr in a global hash
static bool
hash_put_qtr(as_query_transaction * qtr)
{
	if (!qtr->track) {
		return true;
	}

	int rv = cf_rchash_put_unique(g_query_job_hash, &qtr->trid,
			sizeof(qtr->trid), qtr);

	if (rv != CF_RCHASH_OK) {
		cf_warning(AS_QUERY, "QTR Put in hash failed with error %d", rv);
		return false;
	}

	return true;
}

// Get Qtr from global hash
static bool
hash_get_qtr(uint64_t trid, as_query_transaction ** qtr)
{
	int rv = cf_rchash_get(g_query_job_hash, &trid, sizeof(trid), (void **) qtr);

	if (rv != CF_RCHASH_OK) {
		cf_info(AS_QUERY, "Query job with transaction id [%"PRIu64"] does not exist",
				trid);
		return false;
	}

	return true;
}

// Delete Qtr from global hash
static void
hash_delete_qtr(as_query_transaction *qtr)
{
	if (!qtr->track) {
		return;
	}

	int rv = cf_rchash_delete(g_query_job_hash, &qtr->trid, sizeof(qtr->trid));

	if (rv != CF_RCHASH_OK) {
		cf_warning(AS_QUERY, "Failed to delete qtr from query hash.");
	}
}

// If any query run from more than g_config.query_untracked_time_ms we are going
// to track it, else no.
static bool
hash_track_qtr(as_query_transaction *qtr)
{
	if (! qtr->track && (cf_getns() - qtr->start_time) >
			(g_config.query_untracked_time_ms * 1000000)) {
		qtr->track = true;
		QTR_RESERVE(qtr);

		if (! hash_put_qtr(qtr)) {
			// track should be disabled otherwise at the qtr cleanup stage some
			// other qtr with the same trid can get cleaned up.
			qtr->track     = false;
			QTR_RELEASE(qtr);
			return false;
		}
	}

	return true;
}
// **************************************************************************************************



/*
 * Query Request IO functions
 */
// **************************************************************************************************
static void
query_add_response(query_work *qwork, as_storage_rd *rd)
{
	as_query_transaction *qtr = qwork->qtr;

	if (qwork->bb != NULL) {
		// qwork->bb can be reallocated.
		as_msg_make_response_bufbuilder(&qwork->bb, rd, qtr->no_bin_data,
				qtr->binlist);

		cf_buf_builder *bb = qwork->bb;

		if (bb->used_sz >= g_config.query_buf_size) {
			query_netio(qtr, bb);

			qwork->bb = cf_buf_builder_create(g_config.query_buf_size);
			cf_buf_builder_reserve(&qwork->bb, 8, NULL);
		}
	}
	else {
		cf_mutex_lock(&qtr->buf_mutex);

		// qtr->bb can be reallocated.
		as_msg_make_response_bufbuilder(&qtr->bb, rd, qtr->no_bin_data,
				qtr->binlist);

		cf_buf_builder *bb = qtr->bb;

		if (bb->used_sz >= g_config.query_buf_size) {
			query_netio(qtr, bb);

			qtr->bb = cf_buf_builder_create(g_config.query_buf_size);
			cf_buf_builder_reserve(&qtr->bb, 8, NULL);
		}

		cf_mutex_unlock(&qtr->buf_mutex);
	}

	cf_atomic64_incr(&qtr->n_result_records);
}


static void
query_add_fin(as_query_transaction *qtr)
{
	uint8_t *b;

	cf_buf_builder_reserve(&qtr->bb, sizeof(as_msg), &b);

	// set up the header
	as_msg *msgp      = (as_msg *) b;

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
}

static void
query_send_fin(as_query_transaction *qtr)
{
	if (qtr->fd_h != NULL) {
		query_add_fin(qtr);
		query_netio(qtr, qtr->bb);
		qtr->bb = NULL;
	}
}

static void
query_send_bg_udf_response(as_transaction *tr)
{
	cf_detail(AS_QUERY, "Send Fin for Background UDF");
	bool force_close = ! as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_OK);
	query_release_fd(tr->from.proto_fd_h, force_close);
	tr->from.proto_fd_h = NULL;
}

static void
query_send_bg_ops_response(as_transaction *tr)
{
	cf_detail(AS_QUERY, "send fin for background ops");
	bool force_close = ! as_msg_send_fin(&tr->from.proto_fd_h->sock, AS_OK);
	query_release_fd(tr->from.proto_fd_h, force_close);
	tr->from.proto_fd_h = NULL;
}

static bool
query_match_integer_fromval(const as_query_transaction *qtr, const as_val *v)
{
	const as_query_range *srange = qtr->srange;
	const as_sindex_metadata *imd = qtr->si->imd;

	if ((as_sindex_pktype(imd) != AS_PARTICLE_TYPE_INTEGER) ||
			(srange->bin_type != AS_PARTICLE_TYPE_INTEGER)) {
		return false;
	}

	int64_t i = as_integer_get(as_integer_fromval(v));

	// Start and end are same for point query.
	return srange->u.r.start <= i && i <= srange->u.r.end;
}

static bool
query_match_string_fromval(const as_query_transaction *qtr, const as_val *v)
{
	const as_query_range *srange = qtr->srange;
	const as_sindex_metadata *imd = qtr->si->imd;

	if ((as_sindex_pktype(imd) != AS_PARTICLE_TYPE_STRING) ||
			(srange->bin_type != AS_PARTICLE_TYPE_STRING)) {
		return false;
	}

	const char *str = as_string_get(as_string_fromval(v));
	cf_digest str_digest;

	cf_digest_compute(str, strlen(str), &str_digest);

	return memcmp(&str_digest, &srange->u.digest, CF_DIGEST_KEY_SZ) == 0;
}

static bool
query_match_geojson_fromval(const as_query_transaction *qtr, const as_val *v)
{
	const as_query_range *srange = qtr->srange;
	const as_sindex_metadata *imd = qtr->si->imd;

	if ((as_sindex_pktype(imd) != AS_PARTICLE_TYPE_GEOJSON) ||
			(srange->bin_type != AS_PARTICLE_TYPE_GEOJSON)) {
		return false;
	}

	return as_particle_geojson_match_asval(v, qtr->srange->u.geo.cellid,
			qtr->srange->u.geo.region, qtr->ns->geo2dsphere_within_strict);
}

// Stop iterating if a match is found (by returning false).
static bool
query_match_mapkeys_foreach(const as_val *key, const as_val *val, void *udata)
{
	(void)val;

	const as_query_transaction *qtr = (as_query_transaction *)udata;

	switch (key->type) {
	case AS_STRING:
		// If matches return false
		return ! query_match_string_fromval(qtr, (as_val *)key);
	case AS_INTEGER:
		// If matches return false
		return ! query_match_integer_fromval(qtr, (as_val *)key);
	case AS_GEOJSON:
		// If matches return false
		return ! query_match_geojson_fromval(qtr, (as_val *)key);
	default:
		// All others don't match
		return true;
	}
}

// Stop iterating if a match is found (by returning false).
static bool
query_match_mapvalues_foreach(const as_val *key, const as_val *val, void *udata)
{
	(void)key;

	const as_query_transaction *qtr = (as_query_transaction *)udata;

	switch (val->type) {
	case AS_STRING:
		// If matches return false
		return ! query_match_string_fromval(qtr, (as_val *)val);
	case AS_INTEGER:
		// If matches return false
		return ! query_match_integer_fromval(qtr, (as_val *)val);
	case AS_GEOJSON:
		// If matches return false
		return ! query_match_geojson_fromval(qtr, (as_val *)val);
	default:
		// All others don't match
		return true;
	}
}

// Stop iterating if a match is found (by returning false).
static bool
query_match_listele_foreach(as_val * val, void * udata)
{
	const as_query_transaction *qtr = (as_query_transaction *)udata;

	switch (val->type) {
	case AS_STRING:
		// If matches return false
		return ! query_match_string_fromval(qtr, val);
	case AS_INTEGER:
		// If matches return false
		return ! query_match_integer_fromval(qtr, val);
	case AS_GEOJSON:
		// If matches return false
		return ! query_match_geojson_fromval(qtr, val);
	default:
		// All others don't match
		return true;
	}
}

static bool
record_matches_query_cdt(as_query_transaction *qtr, const as_bin *b)
{
	const as_sindex_metadata *imd = qtr->si->imd;
	as_val *bin_val = as_bin_particle_to_asval(b);
	as_val *val = as_sindex_extract_val_from_path(imd, bin_val);

	if (val == NULL) {
		as_val_destroy(bin_val);
		return false;
	}

	bool matches = false;

	if (val->type == AS_INTEGER) {
		if (imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
			matches = query_match_integer_fromval(qtr, val);
		}
	}
	else if (val->type == AS_STRING) {
		if (imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
			matches = query_match_string_fromval(qtr, val);
		}
	}
	else if (val->type == AS_MAP) {
		if (imd->itype == AS_SINDEX_ITYPE_MAPKEYS) {
			const as_map *map = as_map_fromval(val);
			matches = ! as_map_foreach(map, query_match_mapkeys_foreach, qtr);
		}
		else if (imd->itype == AS_SINDEX_ITYPE_MAPVALUES) {
			const as_map *map = as_map_fromval(val);
			matches = ! as_map_foreach(map, query_match_mapvalues_foreach, qtr);
		}
	}
	else if (val->type == AS_LIST) {
		if (imd->itype == AS_SINDEX_ITYPE_LIST) {
			const as_list *list = as_list_fromval(val);
			matches = ! as_list_foreach(list, query_match_listele_foreach, qtr);
		}
	}

	as_val_val_destroy(bin_val);

	return matches;
}

/*
 * Validate record based on its content and query make sure it indeed should
 * be selected. Secondary index does lazy delete for the entries for the record
 * for which data is on ssd. See sindex design doc for details. Hence it is
 * possible that it returns digest for which record may have changed. Do the
 * validation before returning the row.
 */
static bool
record_matches_query(as_query_transaction *qtr, as_storage_rd *rd)
{
	// TODO: Add counters and make sure it is not a performance hit
	const as_query_range *srange = qtr->srange;
	const as_sindex_metadata *imd = qtr->si->imd;

	as_bin *b = as_bin_get_by_id_live(rd, imd->binid);

	if (b == NULL) {
		return false;
	}

	as_particle_type type = as_bin_get_particle_type(b);

	switch (type) {
		case AS_PARTICLE_TYPE_INTEGER:
			if ((type != as_sindex_pktype(imd)) || (type != srange->bin_type)) {
				return false;
			}

			int64_t i = as_bin_particle_integer_value(b);

			// Start and end are same for point query.
			return srange->u.r.start <= i && i <= srange->u.r.end;
		case AS_PARTICLE_TYPE_STRING:
			if ((type != as_sindex_pktype(imd)) || (type != srange->bin_type)) {
				return false;
			}

			char * buf;
			uint32_t psz = as_bin_particle_string_ptr(b, &buf);
			cf_digest digest;
			cf_digest_compute(buf, psz, &digest);

			return memcmp(&srange->u.digest, &digest, CF_DIGEST_KEY_SZ) == 0;
		case AS_PARTICLE_TYPE_GEOJSON:
			if ((type != as_sindex_pktype(imd)) || (type != srange->bin_type)) {
				return false;
			}

			bool iswithin = as_particle_geojson_match(b->particle,
					srange->u.geo.cellid, srange->u.geo.region,
					qtr->ns->geo2dsphere_within_strict);

			if (iswithin) {
				cf_atomic64_incr(&qtr->ns->geo_region_query_points);
			}
			else {
				cf_atomic64_incr(&qtr->ns->geo_region_query_falsepos);
			}

			return iswithin;
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST:
			// For CDT bins, we need to see if any value within the CDT matches
			// the query. This can be performance hit for big lists and maps.
			return record_matches_query_cdt(qtr, b);
		default:
			return false;
	}
}

static bool
record_changed_since_start(const as_query_transaction *qtr, const as_record *r)
{
	return r->last_update_time > qtr->start_time_clepoch;
}

// Partition reservations are taken while building the record list (list of r_h)
// and released at the end of the query. We only need to take a lock before
// using the record.
static bool
query_io(query_work *qwork, as_record *r)
{
	as_query_transaction *qtr = qwork->qtr;
	as_namespace *ns = qtr->ns;
	as_exp_ctx ctx = { .ns = ns, .r = r };
	as_exp_trilean tv = AS_EXP_TRUE;
	as_bin stack_bins[RECORD_MAX_BINS];  // sindexes not allowed on single-bin ns

	if (qtr->filter_exp != NULL) {
		tv = as_exp_matches_metadata(qtr->filter_exp, &ctx);

		if (tv == AS_EXP_FALSE) {
			as_incr_uint64(&qtr->n_filtered_meta);
			return true;
		}
	}

	// check to see this isn't a record waiting to die
	if (as_record_is_doomed(r, ns)) {
		cf_debug(AS_QUERY,
				"build_response: record expired. treat as not found");
		// Not sending error message to client as per the agreement
		// that server will never send a error result code to the query client.
		return true;
	}

	// make sure it's brought in from storage if necessary
	as_storage_rd rd;
	as_storage_record_open(ns, r, &rd);
	qtr->n_read_success += 1;

	if (tv == AS_EXP_UNK) {
		if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
			as_storage_record_close(&rd);
			return true;
		}

		ctx.rd = &rd;

		if (! as_exp_matches_record(qtr->filter_exp, &ctx)) {
			as_storage_record_close(&rd);
			as_incr_uint64(&qtr->n_filtered_bins);
			return true;
		}
	}

	if (qtr->no_bin_data) {
		// Geo query always needs deeper check as it may return cell IDs outside
		// the query region depending on config (min-level, max-cells, ...).
		if (record_changed_since_start(qtr, r) ||
				qtr->si->imd->sktype == COL_TYPE_GEOJSON) {
			if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
				as_storage_record_close(&rd);
				return true;
			}

			if (! record_matches_query(qtr, &rd)) {
				as_storage_record_close(&rd);
				cf_atomic64_incr(&ns->query_false_positives);
				return true;
			}
		}
	}
	else {
		if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
			as_storage_record_close(&rd);
			return true;
		}

		// Geo query always needs deeper check as it may return cell IDs outside
		// the query region depending on config (min-level, max-cells, ...).
		if ((record_changed_since_start(qtr, r) ||
				qtr->si->imd->sktype == COL_TYPE_GEOJSON) &&
				! record_matches_query(qtr, &rd)) {
			as_storage_record_close(&rd);
			cf_atomic64_incr(&ns->query_false_positives);
			return true;
		}
	}

	query_add_response(qwork, &rd);
	as_storage_record_close(&rd);

	return true;
}
// **************************************************************************************************

static void
query_add_val_response(as_query_transaction *qtr, const as_val *val,
		bool success)
{
	uint32_t msg_sz = as_particle_asval_client_value_size(val);

	cf_mutex_lock(&qtr->buf_mutex);

	as_msg_make_val_response_bufbuilder(val, &qtr->bb, msg_sz, success);

	cf_buf_builder *bb = qtr->bb;

	if (bb->used_sz >= g_config.query_buf_size) {
		query_netio(qtr, bb);

		qtr->bb = cf_buf_builder_create(g_config.query_buf_size);
		cf_buf_builder_reserve(&qtr->bb, 8, NULL);
	}

	cf_mutex_unlock(&qtr->buf_mutex);

	cf_atomic64_incr(&qtr->n_result_records);
}


static void
query_add_failure(char *res, as_query_transaction *qtr)
{
	const as_val *v = (as_val *)as_string_new (res, false);
	query_add_val_response(qtr, v, false);
	as_val_destroy(v);
}


static bool
query_process_aggreq(query_work *qagg)
{
	if (cf_ll_size(qagg->recl) == 0) {
		return true;
	}

	as_query_transaction *qtr = qagg->qtr;
	as_result *res = as_result_new();
	int ret = as_aggr_process(qtr->ns, &qtr->agg_call, qagg->recl, (void *)qtr,
			res);

	if (ret != 0) {
		// FIXME: the error string doesn't look right.
		char *rs = as_module_err_string(ret);

		if (res->value != NULL) {
			as_string *lua_s = as_string_fromval(res->value);
			char *lua_err = (char *)as_string_tostring(lua_s);

			if (lua_err != NULL) {
				size_t l_rs_len = strlen(rs);

				rs = cf_realloc(rs, l_rs_len + strlen(lua_err) + 4);
				sprintf(&rs[l_rs_len], " : %s", lua_err);
			}
		}

		query_add_failure(rs, qtr);
		cf_free(rs);
	}

	as_result_destroy(res);

	return ret == 0;
}
// **************************************************************************************************


/*
 * Aggregation HOOKS
 */
// **************************************************************************************************
static as_stream_status
agg_ostream_write(void *udata, as_val *v)
{
	if (v != NULL) {
		as_query_transaction *qtr = (as_query_transaction *)udata;

		query_add_val_response(qtr, v, true);
		as_val_destroy(v);
	}

	return AS_STREAM_OK;
}

static as_partition_reservation *
agg_reserve_partition(void *udata, as_namespace *ns, uint32_t pid,
		as_partition_reservation *rsv)
{
	(void)udata;

	return as_partition_reserve_query(ns, pid, rsv) == 0 ? rsv : NULL;
}

static void
agg_release_partition(void *udata, as_partition_reservation *rsv)
{
	(void)udata;

	as_partition_release(rsv);
}

// true if matches
static bool
agg_record_matches(void *udata, udf_record *urecord)
{
	// Load (read from device) here instead of when opening record, so that
	// other UDFs like scan aggregations or record read UDFs can avoid loading
	// if possible (i.e. UDF requires record metadata only).
	if (udf_record_load(urecord) != 0) {
		return false;
	}

	as_query_transaction *qtr = (as_query_transaction *)udata;

	qtr->n_read_success++;

	as_storage_rd *rd = urecord->rd;

	// Geo query always needs deeper check as it may return cell IDs outside the
	// query region depending on config (min-level, max-cells, ...).
	if ((record_changed_since_start(qtr, rd->r) ||
			qtr->si->imd->sktype == COL_TYPE_GEOJSON) &&
			! record_matches_query(qtr, rd)) {
		cf_atomic64_incr(&qtr->ns->query_false_positives);
		return false;
	}

	return true;
}

static const as_aggr_hooks query_aggr_hooks = {
	.ostream_write = agg_ostream_write,
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
static void
query_udf_bg_tr_complete(void *udata, int result)
{
	(void)result;

	as_query_transaction *qtr = (as_query_transaction *)udata;

	cf_assert(qtr != NULL, AS_QUERY, "complete called with null udata");

	QTR_FINISH_WORK(qtr, &qtr->n_udf_tr_queued, true);
}

// Creates a internal transaction for per record UDF execution triggered
// from inside generator. The generator could be scan job generating digest
// or query generating digest.
static void
query_udf_bg_tr_start(as_query_transaction *qtr, cf_digest *keyd)
{
	as_namespace *ns = qtr->ns;
	as_transaction tr;

	as_transaction_init_iudf(&tr, ns, keyd, &qtr->iudf_orig);

	QTR_RESERVE(qtr);

	cf_atomic32_incr(&qtr->n_udf_tr_queued);
	cf_atomic64_incr(&qtr->n_result_records);

	as_service_enqueue_internal(&tr);
}

static bool
query_process_udfreq(query_work *qudf)
{
	cf_detail(AS_QUERY, "Performing UDF");

	as_query_transaction *qtr = qudf->qtr;
	cf_ll_element *ele = NULL;
	cf_ll_iterator *iter = cf_ll_getIterator(qudf->recl, true /*forward*/);
	as_namespace *ns = qtr->ns;

	while ((ele = cf_ll_getNext(iter)) != NULL) {
		as_index_keys_ll_element *node = (as_index_keys_ll_element *) ele;
		as_index_keys_arr *k_a = node->keys_arr;

		for (uint32_t i = 0; i < k_a->num; i++) {
			as_index_ref r_ref;

			if (! process_keys_arr_ele(qtr, k_a, i, &r_ref)) {
				continue;
			}

			as_exp *exp = qtr->iudf_orig.filter_exp;

			if (exp != NULL) {
				as_exp_ctx ctx = { .ns = ns, .r = r_ref.r };

				if (as_exp_matches_metadata(exp, &ctx) == AS_EXP_FALSE) {
					as_record_done(&r_ref, ns);
					as_incr_uint64(&ns->n_udf_sub_udf_filtered_out);
					continue;
				}
			}

			cf_digest keyd = r_ref.r->keyd;

			as_record_done(&r_ref, ns);

			// May fail but continue.
			query_udf_bg_tr_start(qtr, &keyd);
		}
	}

	if (iter != NULL) {
		cf_ll_releaseIterator(iter);
	}

	return true;
}
// **************************************************************************************************





/*
 * Query Request ops functions
 */
// **************************************************************************************************
// NB: Caller holds a write hash lock _BE_CAREFUL_ if you intend to take
// lock inside this function
static void
query_ops_bg_tr_complete(void *udata, int result)
{
	(void)result;

	as_query_transaction *qtr = (as_query_transaction *)udata;

	cf_assert(qtr != NULL, AS_QUERY, "complete called with null udata");

	QTR_FINISH_WORK(qtr, &qtr->n_ops_tr_queued, true);
}

// Creates a internal transaction for per record ops execution triggered
// from inside generator. The generator could be scan job generating digest
// or query generating digest.
static void
query_ops_bg_tr_start(as_query_transaction *qtr, cf_digest *keyd)
{
	as_namespace *ns = qtr->ns;
	as_transaction tr;

	as_transaction_init_iops(&tr, ns, keyd, &qtr->iops_orig);

	QTR_RESERVE(qtr);

	cf_atomic32_incr(&qtr->n_ops_tr_queued);
	cf_atomic64_incr(&qtr->n_result_records);

	as_service_enqueue_internal(&tr);
}

static bool
query_process_opsreq(query_work *qops)
{
	cf_detail(AS_QUERY, "Performing ops");

	as_query_transaction *qtr = qops->qtr;
	cf_ll_element *ele = NULL;
	cf_ll_iterator *iter = cf_ll_getIterator(qops->recl, true /*forward*/);
	as_namespace *ns = qtr->ns;

	while ((ele = cf_ll_getNext(iter)) != NULL) {
		as_index_keys_ll_element *node = (as_index_keys_ll_element *) ele;
		as_index_keys_arr *k_a = node->keys_arr;

		for (uint32_t i = 0; i < k_a->num; i++) {
			as_index_ref r_ref;

			if (! process_keys_arr_ele(qtr, k_a, i, &r_ref)) {
				continue;
			}

			as_exp *exp = qtr->iops_orig.filter_exp;

			if (exp != NULL) {
				as_exp_ctx ctx = { .ns = ns, .r = r_ref.r };

				if (as_exp_matches_metadata(exp, &ctx) == AS_EXP_FALSE) {
					as_record_done(&r_ref, ns);
					as_incr_uint64(&ns->n_ops_sub_write_filtered_out);
					continue;
				}
			}

			cf_digest keyd = r_ref.r->keyd;

			as_record_done(&r_ref, ns);

			// May fail but continue.
			query_ops_bg_tr_start(qtr, &keyd);
		}
	}

	if (iter != NULL) {
		cf_ll_releaseIterator(iter);
	}

	return true;
}
// **************************************************************************************************




static bool
query_process_ioreq(query_work *qio)
{
	cf_detail(AS_QUERY, "Performing IO");

	as_query_transaction *qtr = qio->qtr;
	uint64_t time_ns = (g_config.query_enable_histogram ||
			qtr->si->enable_histogram) ? cf_getns() : 0;

	bool ret = true;
	cf_ll_element *ele = NULL;
	cf_ll_iterator *iter = cf_ll_getIterator(qio->recl, true /*forward*/);
	as_namespace *ns = qtr->ns;

	while ((ele = cf_ll_getNext(iter))) {
		as_index_keys_ll_element *node = (as_index_keys_ll_element *) ele;
		as_index_keys_arr *k_a = node->keys_arr;

		for (uint32_t i = 0; i < k_a->num; i++) {
			uint64_t nresults = cf_atomic64_get(qtr->n_result_records);

			if (nresults > 0 && (nresults % qtr->priority == 0)) {
				usleep(g_config.query_sleep_us);
			}

			as_index_ref r_ref;

			if (! process_keys_arr_ele(qtr, k_a, i, &r_ref)) {
				continue;
			}

			ret = query_io(qio, r_ref.r);

			as_record_done(&r_ref, ns);

			if (! ret) {
				goto Cleanup;
			}
		}
	}

Cleanup:
	cf_ll_releaseIterator(iter);
	QUERY_HIST_INSERT_DATA_POINT(query_batch_io_hist, time_ns);
	SINDEX_HIST_INSERT_DATA_POINT(qtr->si, query_batch_io, time_ns);

	return ret;
}

// **************************************************************************************************


/*
 * Query Request Processing
 */
// **************************************************************************************************
static bool
qwork_process(query_work *qworkp)
{
	QUERY_HIST_INSERT_DATA_POINT(query_batch_io_q_wait_hist,
			qworkp->queued_time_ns);

	as_query_transaction *qtr = qworkp->qtr;

	cf_detail(AS_QUERY, "Processing Request %d", qtr->job_type);

	query_check_timeout(qtr);

	if (qtr_failed(qtr)) { // handle both timeout and abort
		return false;
	}

	switch (qtr->job_type) {
	case QUERY_TYPE_LOOKUP:
		return query_process_ioreq(qworkp);
	case QUERY_TYPE_AGGR:
		return query_process_aggreq(qworkp);
	case QUERY_TYPE_UDF_BG:
		return query_process_udfreq(qworkp);
	case QUERY_TYPE_OPS_BG:
		return query_process_opsreq(qworkp);
	default:
		cf_crash(AS_QUERY, "unsupported query type %d", qtr->job_type);
	}
}

static void
qwork_setup(query_work *qworkp, as_query_transaction *qtr)
{
	QTR_RESERVE(qtr);

	// fd_h will not be set for background operations. qwork->bb not needed.
	if (qtr->fd_h != NULL && cf_atomic32_get(qtr->n_qwork_active) > 1) {
		qworkp->bb = cf_buf_builder_create(g_config.query_buf_size);
		cf_buf_builder_reserve(&qworkp->bb, 8, NULL);
	}
	else {
		qworkp->bb = NULL;
	}

	qworkp->qtr = qtr;
	qworkp->recl = qtr->qctx.recl;
	qtr->qctx.recl = NULL;
	qworkp->queued_time_ns = cf_getns();
	qtr->n_recs += qtr->qctx.n_recs;
	qtr->qctx.n_recs = 0;
}

static void
qwork_teardown(query_work *qworkp)
{
	as_query_transaction *qtr = qworkp->qtr;

	if (qworkp->recl != NULL) {
		cf_ll_reduce(qworkp->recl, true, release_r_h_cb, qtr);
		cf_free(qworkp->recl);
		qworkp->recl = NULL;
	}

	// fd_h will not be set for background operations.
	if (qworkp->bb != NULL && qtr->fd_h != NULL) {
		query_netio(qtr, qworkp->bb);
	}

	QTR_RELEASE(qtr);

	qworkp->qtr = NULL;
}

static void *
qwork_th(void *udata)
{
	(void)udata;

	uint32_t thread_id = (uint32_t)cf_atomic32_incr(&g_query_worker_threadcnt);
	query_work *qworkp = NULL;

	cf_detail(AS_QUERY, "Created Query Worker Thread %d", thread_id);

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

		as_query_transaction *qtr = qworkp->qtr;

		cf_detail(AS_QUERY, "Popped I/O work [%p,%p]", qworkp, qtr);

		qwork_process(qworkp);
		qwork_teardown(qworkp);
		cf_free(qworkp);

		QTR_FINISH_WORK(qtr, &qtr->n_qwork_active, false);
	}

	return NULL;
}
// **************************************************************************************************

/*
 * Query Request Parsing
 */
// **************************************************************************************************
static as_sindex *
si_from_range(const as_namespace *ns, const char *set,
		const as_query_range *srange)
{
	as_sindex *si = as_sindex_lookup_by_defn(ns, set, srange->bin_id,
			as_sindex_sktype_from_pktype(srange->bin_type), srange->itype,
			srange->bin_path, 0);

	if (si == NULL) {
		return NULL;
	}

	return si;
}

static as_sindex *
si_from_msg(const as_namespace *ns, as_msg *msgp)
{
	cf_debug(AS_QUERY, "as_sindex_from_msg");
	as_msg_field *ifp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_NAME);

	if (!ifp) {
		cf_debug(AS_QUERY, "Index name not found in the query request");
		return NULL;
	}

	uint32_t iname_len = as_msg_field_get_value_sz(ifp);

	if (iname_len == 0 || iname_len >= AS_ID_INAME_SZ) {
		cf_warning(AS_QUERY, "bad index name size %u", iname_len);
		return NULL;
	}

	char iname[AS_ID_INAME_SZ];

	memcpy(iname, ifp->data, iname_len);
	iname[iname_len] = 0;

	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, 0);
	if (!si) {
		cf_detail(AS_QUERY, "Search did not find index ");
	}

	return si;
}

static bool
extract_bin_from_path(const char *path_str, size_t path_str_len, char *bin)
{
	if (path_str_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_QUERY, "Bin path length exceeds the maximum allowed.");
		return false;
	}

	uint32_t end = 0;

	while (end < path_str_len && path_str[end] != '.' && path_str[end] != '['
			&& path_str[end] != ']') {
		end++;
	}

	if (end > 0 && end < AS_BIN_NAME_MAX_SZ) {
		memcpy(bin, path_str, end);
		bin[end] = '\0';
		return true;
	}

	cf_warning(AS_QUERY, "bin name too long");
	return false;
}

static bool
srange_from_msg_integer(const uint8_t *data, as_query_range *srange)
{
	uint32_t startl = ntohl(*((uint32_t *)data));

	if (startl != 8) {
		cf_warning(AS_QUERY, "can only handle 8 byte numerics %u", startl);
		return false;
	}

	data += sizeof(uint32_t);

	int64_t start = (int64_t)cf_swap_from_be64(*((uint64_t *)data));
	srange->u.r.start = start;
	data += sizeof(uint64_t);

	uint32_t endl = ntohl(*((uint32_t *)data));

	if (endl != 8) {
		cf_warning(AS_QUERY, "can only handle 8 byte numerics %u", endl);
		return false;
	}

	data += sizeof(uint32_t);

	int64_t end = (int64_t)cf_swap_from_be64(*((uint64_t *)data));
	srange->u.r.end = end;
	data += sizeof(uint64_t);

	if (start > end) {
		cf_warning(AS_QUERY, "invalid range - %ld..%ld", start, end);
		return false;
	}

	srange->isrange = start != end;

	cf_debug(AS_QUERY, "range - %ld..%ld", start, end);

	return true;
}

static bool
srange_from_msg_string(const uint8_t *data, as_query_range *srange)
{
	uint32_t startl = ntohl(*((uint32_t *)data));

	if (startl >= AS_SINDEX_MAX_STRING_KSIZE) {
		cf_warning(AS_QUERY, "Value length %u too long", startl);
		return false;
	}

	data += sizeof(uint32_t);

	const char *start_binval = (const char *)data;

	data += startl;

	uint32_t endl = ntohl(*((uint32_t *)data));

	data += sizeof(uint32_t);

	const char *end_binval = (const char *)data;

	if (startl != endl || (memcmp(start_binval, end_binval, startl) != 0)) {
		cf_warning(AS_QUERY, "Only Equality Query Supported in Strings %s-%s",
				start_binval, end_binval);
		return false;
	}

	cf_digest_compute(start_binval, startl, &srange->u.digest);
	srange->isrange = false;

	cf_debug(AS_QUERY, "Range is equal %s, %s", start_binval, end_binval);

	return true;
}

static bool
srange_from_msg_geojson(as_namespace *ns, const uint8_t *data,
		as_query_range *srange)
{
	as_query_geo_range *geo = &srange->u.geo;

	uint32_t startl = ntohl(*((uint32_t *)data));

	if ((startl == 0) || (startl >= AS_SINDEX_MAX_GEOJSON_KSIZE)) {
		cf_warning(AS_QUERY, "Invalid query key size %u", startl);
		return false;
	}

	data += sizeof(uint32_t);

	const char *start_binval = (const char *)data;

	data += startl;

	uint32_t endl = ntohl(*((uint32_t *)data));

	data += sizeof(uint32_t);

	const char *end_binval = (const char *)data;

	// TODO: same JSON content may not be byte-identical.
	if (startl != endl || (memcmp(start_binval, end_binval, startl) != 0)) {
		cf_warning(AS_QUERY, "only Geospatial Query Supported on GeoJSON %s-%s",
				start_binval, end_binval);
		return false;
	}

	if (! geo_parse(ns, start_binval, startl, &geo->cellid, &geo->region)) {
		// geo_parse will have printed a warning.
		return false;
	}

	if (geo->cellid != 0 && geo->region != NULL) {
		geo_region_destroy(geo->region);
		cf_warning(AS_GEO, "query geo_parse: both point and region not allowed");
		return false;
	}

	if (geo->cellid == 0 && geo->region == NULL) {
		cf_warning(AS_GEO, "query geo_parse: neither point nor region present");
		return false;
	}

	if (geo->cellid != 0) {
		// regions-containing-point query

		uint64_t center[MAX_REGION_LEVELS];
		uint32_t numcenters;

		if (! geo_point_centers(geo->cellid, MAX_REGION_LEVELS, center,
				&numcenters)) {
			// geo_point_centers will have printed a warning.
			return false;
		}

		geo->r = cf_calloc(numcenters, sizeof(as_query_range_start_end));
		geo->num_r = (uint8_t)numcenters;

		// Geospatial queries use multiple srange elements.
		for (uint32_t i = 0; i < numcenters; i++) {
			geo->r[i].start = (int64_t)center[i];
			geo->r[i].end = (int64_t)center[i];
		}
	}
	else {
		// points-inside-region query

		uint64_t cellmin[MAX_REGION_CELLS];
		uint64_t cellmax[MAX_REGION_CELLS];
		uint32_t numcells;

		if (! geo_region_cover(ns, geo->region, MAX_REGION_CELLS, NULL, cellmin,
				cellmax, &numcells)) {
			geo_region_destroy(geo->region);
			// geo_point_centers will have printed a warning.
			return false;
		}

		geo->r = cf_calloc(numcells, sizeof(as_query_range_start_end));
		geo->num_r = (uint8_t)numcells;

		cf_atomic64_incr(&ns->geo_region_query_count);
		cf_atomic64_add(&ns->geo_region_query_cells, numcells);

		// Geospatial queries use multiple srange elements.	 Many
		// of the fields are copied from the first cell because
		// they were filled in above.
		for (uint32_t i = 0; i < numcells; i++) {
			geo->r[i].start = (int64_t)cellmin[i];
			geo->r[i].end = (int64_t)cellmax[i];
		}
	}

	srange->isrange = true;

	return true;
}

/*
 * Extract out range information from the as_msg and create the irange structure
 * if required allocates the memory.
 * NB: It is responsibility of caller to call the cleanup routine to clean the
 * range structure up and free up its memory
 *
 * query range field layout: contains - numranges, binname, start, end
 *
 * generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numranges
 * 5   1 numranges (max 255 ranges)
 *
 * binname
 * 6   1 binnamelen b
 * 7   b binname
 *
 * particle (start & end)
 * +b    1 particle_type
 * +b+1  4 start_particle_size x
 * +b+5  x start_particle_data
 * +b+5+x      4 end_particle_size y
 * +b+5+x+y+4   y end_particle_data
 *
 * repeat "numranges" times from "binname"
 */
static as_sindex_status
srange_from_msg(as_namespace *ns, as_msg *msgp, as_query_range **srange_r)
{
	// getting ranges
	const as_msg_field *rfp = as_msg_field_get(msgp,
			AS_MSG_FIELD_TYPE_INDEX_RANGE);

	if (rfp == NULL) {
		cf_warning(AS_QUERY, "required index range not found");
		return AS_SINDEX_ERR_PARAM;
	}

	const uint8_t *data = rfp->data;
	uint8_t numrange = *data++;

	if (numrange != 1) {
		cf_warning(AS_QUERY, "range must be 1, passed %u", numrange);
		return AS_SINDEX_ERR_PARAM;
	}

	as_query_range *srange = cf_calloc(1, sizeof(as_query_range));

	const as_msg_field *ifp = as_msg_field_get(msgp,
			AS_MSG_FIELD_TYPE_INDEX_TYPE);

	srange->itype = (ifp == NULL) ? AS_SINDEX_ITYPE_DEFAULT : *ifp->data;

	// Populate Bin id
	uint8_t bin_path_len = *data++;
	char binname[AS_BIN_NAME_MAX_SZ];

	memcpy(srange->bin_path, data, bin_path_len);
	srange->bin_path[bin_path_len] = '\0';

	if (extract_bin_from_path(srange->bin_path, bin_path_len, binname)) {
		int32_t id = as_bin_get_id(ns, binname);

		if (id != -1) {
			srange->bin_id = (uint32_t)id;
		}
		else {
			cf_ticker_warning(AS_QUERY, "bin %s not found", binname);
			srange_free(srange);
			return AS_SINDEX_ERR_BIN_NOTFOUND;
		}
	}
	else {
		srange_free(srange);
		return AS_SINDEX_ERR_PARAM;
	}

	data += bin_path_len;

	// Populate type
	as_particle_type type = *data++;

	srange->bin_type = type;

	bool success;

	switch (type) {
	case AS_PARTICLE_TYPE_INTEGER:
		success = srange_from_msg_integer(data, srange);
		break;
	case AS_PARTICLE_TYPE_STRING:
		success = srange_from_msg_string(data, srange);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		success = srange_from_msg_geojson(ns, data, srange);
		break;
	default:
		cf_warning(AS_QUERY, "invalid type");
		success = false;
	}

	if (! success) {
		srange_free(srange);
		return AS_SINDEX_ERR_PARAM;
	}

	*srange_r = srange;

	return AS_SINDEX_OK;
}

static int
query_pimd(as_sindex *si, const as_query_range *srange, as_sindex_qctx *qctx)
{
	if (! as_sindex_can_query(si)) {
		return AS_SINDEX_ERR_NOT_READABLE;
	}

	as_sindex_metadata *imd = si->imd;
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_ix];

	PIMD_RLOCK(&pimd->slock);
	int ret = ai_btree_query(imd, srange, qctx);
	PIMD_RUNLOCK(&pimd->slock);

	// No histogram for query per call.
	as_sindex_process_ret(si, ret, AS_SINDEX_OP_READ, 0, __LINE__);

	return ret;
}
//                                        END -  SINDEX QUERY
// ************************************************************************************************

/*
 * Query Generator
 */
// **************************************************************************************************
// Returns:
// AS_QUERY_OK: if the batch is full, qctx->n_bdigs == qctx->bsize. The caller
// then processes the batch and resets qctx->recl and qctx->n_bdigs.
//
// AS_QUERY_CONTINUE: the caller should continue calling this function.
//
// AS_QUERY_ERR: in case of error.
static as_query_status
query_get_nextbatch(as_query_transaction *qtr)
{
	as_sindex *si = qtr->si;
	as_sindex_qctx *qctx = &qtr->qctx;
	const as_query_range *srange = qtr->srange;
	uint64_t time_ns = (g_config.query_enable_histogram || si->enable_histogram)
			? cf_getns() : 0;

	if (qctx->pimd_ix == -1) {
		if (srange->isrange) {
			qctx->pimd_ix = 0;
		}
		else {
			// geo queries can never be point queries.
			const void *buf = srange->bin_type == AS_PARTICLE_TYPE_INTEGER ?
					(const void *)&srange->u.r.start :
					(const void *)&srange->u.digest;

			qctx->pimd_ix = as_sindex_get_pimd_ix(si->imd, buf);
		}
	}

	if (qctx->recl == NULL) {
		qctx->recl = cf_malloc(sizeof(cf_ll));
		cf_ll_init(qctx->recl, as_index_keys_ll_destroy_fn, false /*no lock*/);
		qctx->n_recs = 0;
	}
	else {
		// Following condition may be true if the query has moved from short
		// query pool to long running query pool.
		if (qctx->n_recs >= qctx->bsize) {
			return AS_QUERY_OK;
		}
	}

	int qret = query_pimd(si, srange, qctx);

	cf_detail(AS_QUERY, "%d pimd found %lu", qctx->pimd_ix, qctx->n_recs);

	if (qret < 0) {
		QTR_SET_ERR(qtr, AS_SINDEX_ERR_TO_CLIENTERR(qret));
		return AS_QUERY_ERR;
	}

	qctx->new_ibtr = false;

	if (time_ns) {
		if (g_config.query_enable_histogram) {
			qtr->querying_ai_time_ns += cf_getns() - time_ns;
		}
		else if (si->enable_histogram) {
			SINDEX_HIST_INSERT_DATA_POINT(si, query_batch_lookup, time_ns);
		}
	}

	if (qctx->n_recs < qctx->bsize) {
		qctx->new_ibtr = true;
		qctx->nbtr_done = false;
		qctx->pimd_ix++;

		cf_detail(AS_QUERY, "all the data finished, moving to next tree %d",
				qctx->pimd_ix);

		if (! srange->isrange) {
			qtr->result_code = AS_OK;
			return AS_QUERY_DONE;
		}

		if ((uint32_t)qctx->pimd_ix == si->imd->n_pimds) {
			// Integer range queries have only one range.
			if (qtr->srange->bin_type == AS_PARTICLE_TYPE_INTEGER ||
					(qctx->range_index == (qtr->srange->u.geo.num_r - 1))) {
				qtr->result_code = AS_OK;
				return AS_QUERY_DONE;
			}

			qctx->range_index++;
			qctx->pimd_ix = -1;
		}

		return AS_QUERY_CONTINUE;
	}

	return AS_QUERY_OK;
}



// Phase II setup just after the generator picks up query for the first time
static void
query_run_setup(as_query_transaction *qtr)
{
	QUERY_HIST_INSERT_DATA_POINT(query_query_q_wait_hist, qtr->start_time);
	cf_atomic64_set(&qtr->n_result_records, 0);
	qtr->track = false;
	qtr->querying_ai_time_ns = 0;
	qtr->n_io_outstanding = 0;
	qtr->netio_push_seq = 0;
	qtr->netio_pop_seq = 1;
	cf_mutex_init(&qtr->buf_mutex);

	// Aerospike Index object initialization
	qtr->result_code = AS_OK;

	// Initialize qctx
	// start with the threshold value
	qtr->qctx.bsize = g_config.query_threshold;
	qtr->qctx.new_ibtr = true;
	qtr->qctx.nbtr_done = false;
	qtr->qctx.pimd_ix = -1;
	qtr->qctx.recl = NULL;
	qtr->qctx.n_recs = 0;
	qtr->qctx.range_index = 0;
	qtr->qctx.ibtr_last_key = &qtr->bkey;
	init_ai_obj(qtr->qctx.ibtr_last_key);
	qtr->qctx.nbtr_last_key = 0;
	qtr->qctx.r_h_hash = NULL;

	// Populate all the partitions for which this partition is query-able
	memset(qtr->qctx.reserved_trees, 0, sizeof(qtr->qctx.reserved_trees));

	qtr->priority = g_config.query_priority;
	qtr->bb = cf_buf_builder_create(g_config.query_buf_size);

	cf_buf_builder_reserve(&qtr->bb, 8, NULL);

	qtr_set_running(qtr);
	cf_atomic64_incr(&qtr->ns->query_short_reqs);
	cf_atomic32_incr(&g_query_short_running);
}

static bool
query_qtr_enqueue(as_query_transaction *qtr, bool is_requeue)
{
	uint64_t limit;
	uint32_t size;
	cf_queue *q;
	cf_atomic64 *queue_full_err;

	if (qtr->short_running) {
		limit = g_config.query_short_q_max_size;
		size = cf_atomic32_get(g_query_short_running);
		q = g_query_short_queue;
		queue_full_err = &qtr->ns->query_short_queue_full;
	}
	else {
		limit = g_config.query_long_q_max_size;
		size = cf_atomic32_get(g_query_long_running);
		q = g_query_long_queue;
		queue_full_err = &qtr->ns->query_long_queue_full;
	}

	// Allow requeue without limit check, to cover for dynamic
	// config change while query
	if (! is_requeue && (size > limit)) {
		cf_atomic64_incr(queue_full_err);
		return false;
	}

	cf_queue_push(q, &qtr);
	cf_detail(AS_QUERY, "Query queued");

	return true;
}

static bool
query_requeue(as_query_transaction *qtr)
{
	if (! query_qtr_enqueue(qtr, true)) {
		cf_warning(AS_QUERY, "Query requeue failed");
		QTR_SET_ERR(qtr, AS_ERR_QUERY_QUEUE_FULL);
		return false;
	}

	cf_detail(AS_QUERY, "Query requeued");

	return true;
}

static void
qtr_finish_work(as_query_transaction *qtr, cf_atomic32 *stat, bool release,
		const char *fname, int lineno)
{
	qtr_lock(qtr);
	uint32_t val = (uint32_t)cf_atomic32_decr(stat);
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
static as_query_status
query_qtr_check_and_requeue(as_query_transaction *qtr)
{
	bool do_enqueue = false;
	// Step 1: If the query batch is done then wait for number of outstanding qwork to
	// finish. This may slow down query responses get the better model
	if (qtr_finished(qtr)) {
		if ((cf_atomic32_get(qtr->n_qwork_active) == 0)
				&& (cf_atomic32_get(qtr->n_io_outstanding) == 0)
				&& (cf_atomic32_get(qtr->n_udf_tr_queued) == 0)
				&& (cf_atomic32_get(qtr->n_ops_tr_queued) == 0)) {
			cf_detail(AS_QUERY, "Request is finished");
			return AS_QUERY_DONE;
		}
		do_enqueue = true;
		cf_detail(AS_QUERY, "Request not finished qwork(%d) io(%d)", cf_atomic32_get(qtr->n_qwork_active), cf_atomic32_get(qtr->n_io_outstanding));
	}

	// Step 2: Client is slow requeue
	if (! query_netio_wait(qtr)) {
		do_enqueue = true;
	}

	// Step 3: Check to see if this is long running query. This is determined by
	// checking number of records read. Please note that it makes sure the false
	// entries in secondary index does not effect this decision. All short running
	// queries perform I/O in the batch thread context.
	if (qtr->short_running && cf_atomic64_get(qtr->n_result_records) >=
			g_config.query_threshold) {
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

	if (qtr->job_type == QUERY_TYPE_UDF_BG) {
		if (cf_atomic32_get(qtr->n_udf_tr_queued) >=
				(MAX_UDF_TRANSACTIONS * (qtr->priority / 10 + 1))) {
			do_enqueue = true;
		}
	}
	else if (qtr->job_type == QUERY_TYPE_OPS_BG) {
		if (cf_atomic32_get(qtr->n_ops_tr_queued) >=
				(MAX_OPS_TRANSACTIONS * (qtr->priority / 10 + 1))) {
			do_enqueue = true;
		}
	}

	if (do_enqueue) {
		int ret = AS_QUERY_OK;
		qtr_lock(qtr);
		if ((cf_atomic32_get(qtr->n_qwork_active) != 0)
				|| (cf_atomic32_get(qtr->n_io_outstanding) != 0)
				|| (cf_atomic32_get(qtr->n_udf_tr_queued) != 0)
				|| (cf_atomic32_get(qtr->n_ops_tr_queued) != 0)) {
			cf_detail(AS_QUERY, "Job Setup for Requeue %p", qtr);

			// Release of one of the above will perform requeue... look for
			// qtr_finish_work();
			qtr->do_requeue = true;
			ret = AS_QUERY_OK;
		} else {
			ret = query_requeue(qtr) ? AS_QUERY_OK : AS_QUERY_ERR;
		}
		qtr_unlock(qtr);
		return ret;
	}

	return AS_QUERY_CONTINUE;
}

static bool
query_process_inline(as_query_transaction *qtr)
{
	return qtr->short_running ||
			qtr_finished(qtr) ||
			g_config.query_req_in_query_thread ||
			cf_atomic32_get(qtr->n_qwork_active) >
					g_config.query_req_max_inflight;
}

static bool
qtr_process(as_query_transaction *qtr)
{
	if (query_process_inline(qtr)) {
		query_work qwork;

		qwork_setup(&qwork, qtr);

		bool ret = qwork_process(&qwork);

		qwork_teardown(&qwork);

		return ret;
	}

	query_work *qworkp = cf_calloc(1, sizeof(query_work));

	// Successfully queued
	cf_atomic32_incr(&qtr->n_qwork_active);
	qwork_setup(qworkp, qtr);
	cf_queue_push(g_query_work_queue, &qworkp);

	return true;
}

static bool
query_check_bound(const as_query_transaction *qtr)
{
	return cf_atomic64_get(qtr->n_result_records) <=
			g_config.query_rec_count_bound;
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
	// Query can get requeue for many different reason. Check if it is
	// already started before indulging in act to setting it up for run
	if (!qtr_started(qtr)) {
		query_run_setup(qtr);
	}

	int loop = 0;
	while (true) {

		// Step 1: Check for requeue
		as_query_status ret = query_qtr_check_and_requeue(qtr);
		if (ret == AS_QUERY_ERR) {
			cf_warning(AS_QUERY, "Unexpected requeue failure .. shutdown connection.. abort!!");
			QTR_SET_ABORT(qtr, AS_ERR_QUERY_NET_IO);
			break;
		} else if (ret == AS_QUERY_DONE) {
			break;
		} else if (ret == AS_QUERY_OK) {
			return;
		}
		// Step 2: Check for timeout
		query_check_timeout(qtr);
		if (qtr_failed(qtr)) {
			continue;
		}
		// Step 3: Conditionally track
		if (! hash_track_qtr(qtr)) {
			QTR_SET_ERR(qtr, AS_ERR_QUERY_DUPLICATE);
			continue;
		}

		// Step 4: If needs user based abort
		if (! query_check_bound(qtr)) {
			QTR_SET_ERR(qtr, AS_ERR_QUERY_USER_ABORT);
			continue;
		}

		// Step 5: Get Next Batch
		loop++;
		as_query_status qret = query_get_nextbatch(qtr);

		cf_detail(AS_QUERY, "Loop=%d, Selected=%"PRIu64", ret=%d", loop, qtr->qctx.n_recs, qret);
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
			QTR_SET_DONE(qtr, AS_OK);
		}

		// Step 6: Prepare Query Request either to process inline or for
		//         queueing up for offline processing
		if (! qtr_process(qtr)) {
			QTR_SET_ERR(qtr, AS_ERR_QUERY_CB);
			continue;
		}
	}

	if (!qtr_is_abort(qtr)) {
		// Send the fin packet in it is NOT a shutdown
		query_send_fin(qtr);
	}
	// deleting it from the global hash.
	hash_delete_qtr(qtr);
	QTR_RELEASE(qtr);
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
static void*
query_th(void* udata)
{
	cf_queue *query_queue = (cf_queue *)udata;
	uint32_t thread_id = (uint32_t)cf_atomic32_incr(&g_query_threadcnt);
	as_query_transaction *qtr = NULL;

	cf_detail(AS_QUERY, "Query Thread Created %d", thread_id);

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
	return NULL;
}

static cf_vector *
binlist_from_msg(as_namespace *ns, as_msg *msgp, uint32_t *num_bins)
{
	as_msg_field *bfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_QUERY_BINLIST);

	if (bfp == NULL) {
		return NULL;
	}

	const uint8_t *data = bfp->data;
	uint32_t n_bins = *data++; // TODO - only <= 255 bins (scans can do more)

	*num_bins = n_bins;

	cf_vector *id_vec  = cf_vector_create(sizeof(uint16_t), n_bins, 0);

	for (uint32_t i = 0; i < n_bins; i++) {
		uint32_t len = *data++;
		uint16_t id;

		if (! as_bin_get_or_assign_id_w_len(ns, (const char*)data, len, &id)) {
			cf_warning(AS_QUERY, "query job bin not added");
			cf_vector_destroy(id_vec);
			return NULL;
		}

		cf_vector_append_unique(id_vec, (void *)&id);
		data += len;
	}

	return id_vec;
}

/*
 * Parse the UDF OP type to find what type of UDF this is or otherwise not even
 * UDF
 */
static query_type
query_get_type(as_transaction* tr)
{
	if (! as_transaction_is_udf(tr)) {
		return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) != 0 ?
				QUERY_TYPE_OPS_BG : QUERY_TYPE_LOOKUP;
	}

	as_msg_field *udf_op_f = as_transaction_has_udf_op(tr) ?
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP) : NULL;

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_UDF_OP_AGGREGATE) {
		return QUERY_TYPE_AGGR;
	}

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_UDF_OP_BACKGROUND) {
		return QUERY_TYPE_UDF_BG;
	}

	return QUERY_TYPE_UNKNOWN;
}

static bool
aggr_query_init(as_aggr_call *call, const as_transaction *tr)
{
	if (! udf_def_init_from_msg(&call->def, tr)) {
		return false;
	}

	call->aggr_hooks = &query_aggr_hooks;

	return true;
}

static bool
query_setup_udf_call(as_query_transaction *qtr, as_transaction *tr)
{
	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
			break;
		case QUERY_TYPE_AGGR:
			if (! aggr_query_init(&qtr->agg_call, tr)) {
				tr->result_code = AS_ERR_PARAMETER;
				return false;
			}
			break;
		case QUERY_TYPE_UDF_BG:
			if (! udf_def_init_from_msg(&qtr->iudf_orig.def, tr)) {
				tr->result_code = AS_ERR_PARAMETER;
				return false;
			}
			break;
		case QUERY_TYPE_OPS_BG:
			break;
		default:
			cf_crash(AS_QUERY, "Invalid QUERY TYPE %d !!!", qtr->job_type);
			break;
	}
	return true;
}

static bool
query_setup_shared_msgp(as_query_transaction *qtr, as_transaction *tr)
{
	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
		case QUERY_TYPE_AGGR:
			break;
		case QUERY_TYPE_UDF_BG: {
				as_msg* om = &tr->msgp->msg;
				uint8_t info2 = (uint8_t)(AS_MSG_INFO2_WRITE |
						(om->info2 & AS_MSG_INFO2_DURABLE_DELETE));

				qtr->iudf_orig.msgp = as_msg_create_internal(qtr->ns->name, 0,
						info2, 0, om->record_ttl, 0, NULL, 0);
			}
			break;
		case QUERY_TYPE_OPS_BG: {
				as_msg* om = &tr->msgp->msg;

				if ((om->info1 & AS_MSG_INFO1_READ) != 0) {
					cf_warning(AS_QUERY, "ops not write only");
					return false;
				}

				if (om->n_ops == 0) {
					cf_warning(AS_QUERY, "ops query has no ops");
					return false;
				}

				as_msg_op* op = NULL;
				uint8_t* first = NULL;
				uint16_t i = 0;
				bool has_expop = false;

				while ((op = as_msg_op_iterate(om, op, &i)) != NULL) {
					if (OP_IS_READ(op->op)) {
						cf_warning(AS_SCAN, "ops query has read op");
						return false;
					}

					if (first == NULL) {
						first = (uint8_t*)op;
					}

					if (op->op == AS_MSG_OP_EXP_MODIFY) {
						has_expop = true;
					}
				}

				iops_expop* expops = NULL;

				if (has_expop) {
					expops = cf_malloc(sizeof(iops_expop) * om->n_ops);
					op = NULL;
					i = 0;

					while ((op = as_msg_op_iterate(om, op, &i)) != NULL) {
						if (op->op == AS_MSG_OP_EXP_MODIFY) {
							if (! as_exp_op_parse(op, &expops[i].exp,
									&expops[i].flags, true, true)) {
								cf_warning(AS_SCAN, "ops query failed exp parse");
								iops_expops_destroy(expops, (uint16_t)i);
								return false;
							}
						}
						else {
							expops[i].exp = NULL;
						}
					}
				}

				qtr->iops_orig.expops = expops;

				uint8_t info2 = (uint8_t)(AS_MSG_INFO2_WRITE |
						(om->info2 & AS_MSG_INFO2_DURABLE_DELETE));
				uint8_t info3 = (uint8_t)(AS_MSG_INFO3_UPDATE_ONLY |
						(om->info3 & AS_MSG_INFO3_REPLACE_ONLY));

				qtr->iops_orig.msgp = as_msg_create_internal(qtr->ns->name, 0,
						info2, info3, om->record_ttl, om->n_ops, first,
						(size_t)(tr->msgp->proto.sz - (first - (uint8_t*)om)));
			}
			break;
		default:
			cf_crash(AS_QUERY, "Invalid QUERY TYPE %d !!!", qtr->job_type);
			break;
	}
	return true;
}

static void
query_setup_fd(as_query_transaction *qtr, as_transaction *tr)
{
	switch (qtr->job_type) {
		case QUERY_TYPE_LOOKUP:
		case QUERY_TYPE_AGGR:
			qtr->fd_h                = tr->from.proto_fd_h;
			break;
		case QUERY_TYPE_UDF_BG:
		case QUERY_TYPE_OPS_BG:
			qtr->fd_h  = NULL;
			break;
		default:
			cf_crash(AS_QUERY, "Invalid QUERY TYPE %d !!!", qtr->job_type);
			break;
	}
}

// Phase I query setup which parses the message.
static bool
query_setup(as_transaction *tr, as_namespace *ns, as_query_transaction **qtrp)
{
	uint64_t start_time = cf_getns();
	as_sindex *si = NULL;
	as_query_range *srange = NULL;
	char *setname = NULL;
	cf_vector *binlist = NULL;
	as_exp *filter_exp = NULL;

	if (ns->sindex_cnt == 0) {
		tr->result_code = AS_ERR_SINDEX_NOT_FOUND;
		cf_debug(AS_QUERY, "No Secondary Index on namespace %s", ns->name);
		goto Cleanup;
	}

	// TODO - still lots of redundant msg field parsing (e.g. for set) - fix.

	as_msg *m = &tr->msgp->msg;

	si = si_from_msg(ns, m);

	if (si == NULL) {
		cf_debug(AS_QUERY, "No Index Defined in the Query");
	}

	as_sindex_status ret = srange_from_msg(ns, m, &srange);

	if (ret != AS_SINDEX_OK) {
		cf_debug(AS_QUERY, "Could not instantiate index range metadata - %s",
				as_sindex_err_str(ret));
		tr->result_code = AS_SINDEX_ERR_TO_CLIENTERR(ret);
		goto Cleanup;
	}

	as_msg_field *sfp = as_transaction_has_set(tr) ?
			as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;

	if (sfp) {
		uint32_t setname_len = as_msg_field_get_value_sz(sfp);

		if (setname_len >= AS_SET_NAME_MAX_SIZE) {
			cf_warning(AS_QUERY, "set name too long");
			tr->result_code = AS_ERR_PARAMETER;
			goto Cleanup;
		}

		if (setname_len != 0) {
			setname = cf_strndup((const char *)sfp->data, setname_len);
		}
	}

	uint32_t n_bins = 0;
	binlist = binlist_from_msg(ns, m, &n_bins);

	// If any bin in the binlist is bad, fail the query.
	if (n_bins != 0 && binlist == NULL) {
		tr->result_code = AS_ERR_BIN_NAME;
		goto Cleanup;
	}

	query_type qtype = query_get_type(tr);

	if (qtype == QUERY_TYPE_UNKNOWN) {
		tr->result_code = AS_ERR_PARAMETER;
		goto Cleanup;
	}

	if (as_transaction_has_predexp(tr)) {
		if (qtype == QUERY_TYPE_AGGR) {
			cf_warning(AS_QUERY, "aggregation queries do not support predexp filters");
			tr->result_code = AS_ERR_UNSUPPORTED_FEATURE;
			goto Cleanup;
		}

		as_msg_field *pfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_PREDEXP);
		filter_exp = as_exp_filter_build(pfp, true);

		if (filter_exp == NULL) {
			cf_warning(AS_QUERY, "Failed to build filter-expression");
			tr->result_code = AS_ERR_PARAMETER;
			goto Cleanup;
		}
	}

	// Parsing done.

	// Security vulnerability protection.
	if (g_config.udf_execution_disabled &&
			(qtype == QUERY_TYPE_AGGR || qtype == QUERY_TYPE_UDF_BG)) {
		cf_warning(AS_QUERY, "queries with UDFs are forbidden");
		tr->result_code = AS_ERR_FORBIDDEN;
		goto Cleanup;
	}

	if (si == NULL) {
		// Look up sindex by bin in the query in case not specified in query.
		si = si_from_range(ns, setname, srange);

		if (si == NULL) {
			tr->result_code = AS_ERR_SINDEX_NOT_FOUND;
			goto Cleanup;
		}
	}

	if (! as_sindex_can_query(si)) {
		tr->result_code = AS_SINDEX_ERR_TO_CLIENTERR(AS_SINDEX_ERR_NOT_READABLE);
		goto Cleanup;
	}

	cf_detail(AS_QUERY, "Query on index %s ", si->imd->iname);

	as_query_transaction *qtr = cf_rc_alloc(sizeof(as_query_transaction));

	memset(qtr, 0, offsetof(as_query_transaction, bkey)); // partial memset

	qtr->ns = ns;
	qtr->job_type = qtype;

	if (! query_setup_udf_call(qtr, tr)) {
		cf_rc_free(qtr);
		goto Cleanup;
	}

	if (! query_setup_shared_msgp(qtr, tr)) {
		cf_rc_free(qtr);
		goto Cleanup;
		// Nothing to clean from udf setup - query types that allocate there
		// can't fail here.
	}

	query_setup_fd(qtr, tr);

	if (qtr->job_type == QUERY_TYPE_LOOKUP) {
		qtr->compress_response = as_transaction_compress_response(tr);
		qtr->filter_exp = filter_exp;
		qtr->no_bin_data = (m->info1 & AS_MSG_INFO1_GET_NO_BINS) != 0;
	}
	else if (qtr->job_type == QUERY_TYPE_AGGR) {
		qtr->compress_response = as_transaction_compress_response(tr);
	}
	else if (qtr->job_type == QUERY_TYPE_UDF_BG) {
		qtr->iudf_orig.filter_exp = filter_exp;
		qtr->iudf_orig.cb = query_udf_bg_tr_complete;
		qtr->iudf_orig.udata = (void*) qtr;
	}
	else if (qtr->job_type == QUERY_TYPE_OPS_BG) {
		qtr->iops_orig.filter_exp = filter_exp;
		qtr->iops_orig.cb = query_ops_bg_tr_complete;
		qtr->iops_orig.udata = (void*) qtr;
	}

	// Consume everything from tr, rest will be picked up in init.
	qtr->trid = as_transaction_trid(tr);
	qtr->setname = setname;
	qtr->si = si;
	qtr->srange = srange;
	qtr->binlist = binlist;
	qtr->start_time = start_time;
	qtr->end_time = tr->end_time;
	qtr->start_time_clepoch = cf_clepoch_milliseconds();
	qtr->state = AS_QTR_STATE_INIT;
	qtr->do_requeue = false;
	qtr->short_running = true;

	cf_mutex_init(&qtr->slock);

	*qtrp = qtr;

	return true;

Cleanup:
	if (si != NULL) {
		as_sindex_release(si);
	}

	if (srange != NULL) {
		srange_free(srange);
	}

	if (setname != NULL) {
		cf_free(setname);
	}

	if (binlist != NULL) {
		cf_vector_destroy(binlist);
	}

	as_exp_destroy(filter_exp);

	return false;
}

bool
as_query(as_transaction *tr, as_namespace *ns)
{
	QUERY_HIST_INSERT_DATA_POINT(query_txn_q_wait_hist, tr->start_time);

	as_query_transaction *qtr = NULL;

	if (! query_setup(tr, ns, &qtr)) {
		return false; // tsvc takes care of managing fd
	}

	if (g_config.query_in_transaction_thr) {
		if (qtr->job_type == QUERY_TYPE_UDF_BG) {
			query_send_bg_udf_response(tr);
		}
		else if (qtr->job_type == QUERY_TYPE_OPS_BG) {
			query_send_bg_ops_response(tr);
		}

		query_generator(qtr);
	}
	else {
		if (! query_qtr_enqueue(qtr, false)) {
			// This error will be accounted by thr_tsvc layer. Thus
			// reset fd_h before calling qtr release, and let the
			// transaction handler deal with the failure.
			qtr->fd_h = NULL;
			QTR_RELEASE(qtr);
			tr->result_code = AS_ERR_QUERY_QUEUE_FULL;
			return false;
		}

		// Respond after queuing is successfully.
		if (qtr->job_type == QUERY_TYPE_UDF_BG) {
			query_send_bg_udf_response(tr);
		}
		else if (qtr->job_type == QUERY_TYPE_OPS_BG) {
			query_send_bg_ops_response(tr);
		}
	}

	// Query engine will reply to queued query as needed.
	tr->from.proto_fd_h = NULL;

	return true;
}

// **************************************************************************************************


/*
 * Query Utility and Monitoring functions
 */
// **************************************************************************************************

// Find matching trid and kill the query
bool
as_query_kill(uint64_t trid)
{
	as_query_transaction *qtr;

	if (! hash_get_qtr(trid, &qtr)) {
		cf_warning(AS_QUERY, "Cannot kill query with trid [%"PRIu64"]",  trid);
		return false;
	}

	QTR_SET_ABORT(qtr, AS_ERR_QUERY_USER_ABORT);
	QTR_RELEASE(qtr);

	return true;
}

// Find matching trid and set priority
bool
as_query_set_priority(uint64_t trid, uint32_t priority)
{
	as_query_transaction *qtr;

	if (! hash_get_qtr(trid, &qtr)) {
		cf_warning(AS_QUERY, "Cannot set priority for query with trid [%"PRIu64"]",
				trid);
		return false;
	}

	uint32_t old_priority = qtr->priority;

	qtr->priority = priority;
	cf_info(AS_QUERY, "Query priority changed from %d to %d", old_priority,
			priority);
	QTR_RELEASE(qtr);

	return true;
}

// query module to monitor
static void
query_fill_jobstat(as_query_transaction *qtr, as_mon_jobstat *stat)
{
	stat->trid          = qtr->trid;
	stat->run_time      = (cf_getns() - qtr->start_time) / 1000000;
	stat->recs_succeeded = qtr->n_read_success; // TODO - this is not like scan
	stat->net_io_bytes  = qtr->net_io_bytes;
	stat->priority      = qtr->priority;

	// Not relevant:
	stat->active_threads  = 0;
	stat->socket_timeout  = 0;

	// Not implemented:
	stat->n_pids_requested = 0;
	stat->rps             = 0;
	stat->client[0]       = '\0';
	stat->progress_pct    = 0;
	stat->time_since_done = 0;
	stat->job_type[0]     = '\0';

	stat->recs_throttled = 0;
	stat->recs_filtered_meta = qtr->n_filtered_meta;
	stat->recs_filtered_bins = qtr->n_filtered_bins;
	stat->recs_failed = 0;

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
	as_query_transaction *qtr;

	if (! hash_get_qtr(trid, &qtr)) {
		cf_warning(AS_MON, "No query was found with trid [%"PRIu64"]", trid);
		return NULL;
	}

	as_mon_jobstat *stat = cf_malloc(sizeof(as_mon_jobstat));
	query_fill_jobstat(qtr, stat);
	QTR_RELEASE(qtr);

	return stat;
}


static int
mon_query_jobstat_reduce_fn(const void *key, uint32_t keylen, void *object,
		void *udata)
{
	(void)key;
	(void)keylen;

	as_query_transaction *qtr = (as_query_transaction *)object;
	query_jobstat *job_pool = (query_jobstat *)udata;

	if (job_pool->index >= job_pool->max_size) {
		return 0;
	}

	as_mon_jobstat *stat = &job_pool->jobstat[job_pool->index++];

	query_fill_jobstat(qtr, stat);

	return 0;
}

as_mon_jobstat *
as_query_get_jobstat_all(int * size)
{
	*size = (int)cf_rchash_get_size(g_query_job_hash);
	if(*size == 0) return NULL;

	as_mon_jobstat     * job_stats;
	query_jobstat     job_pool;

	job_stats          = (as_mon_jobstat *) cf_malloc(sizeof(as_mon_jobstat) * (size_t)(*size));
	job_pool.jobstat  = job_stats;
	job_pool.index    = 0;
	job_pool.max_size = *size;
	cf_rchash_reduce(g_query_job_hash, mon_query_jobstat_reduce_fn, &job_pool);
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
	c->query_short_q_max_size    = AS_QUERY_MAX_SHORT_QUEUE_SZ;
	c->query_long_q_max_size     = AS_QUERY_MAX_LONG_QUEUE_SZ;
	c->query_buf_size            = AS_QUERY_BUF_SIZE;
	c->query_threshold           = 10;	// threshold after which the query is considered long running
										// no reason for choosing 10
	c->query_rec_count_bound     = UINT64_MAX; // Unlimited
	c->query_req_in_query_thread = 0;
	c->query_untracked_time_ms   = AS_QUERY_UNTRACKED_TIME;
}


void
as_query_init()
{
	g_current_queries_count = 0;
	cf_detail(AS_QUERY, "Initialize %d Query Worker threads.", g_config.query_threads);

	// global job hash to keep track of the query job
	g_query_job_hash = cf_rchash_create(cf_rchash_fn_u32, NULL, sizeof(uint64_t), 64, CF_RCHASH_MANY_LOCK);

	// I/O threads
	g_query_work_queue = cf_queue_create(sizeof(query_work *), true);

	for (uint32_t i = 0; i < g_config.query_worker_threads; i++) {
		cf_thread_create_transient(qwork_th, NULL);
	}

	g_query_short_queue = cf_queue_create(sizeof(as_query_transaction *), true);
	g_query_long_queue = cf_queue_create(sizeof(as_query_transaction *), true);

	for (uint32_t i = 0; i < g_config.query_threads; i += 2) {
		cf_thread_create_transient(query_th, (void*)g_query_short_queue);
		cf_thread_create_transient(query_th, (void*)g_query_long_queue);
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

void
as_query_worker_reinit(uint32_t new_thread_count)
{
	pthread_rwlock_wrlock(&g_query_lock);

	uint32_t cur_threads = g_config.query_worker_threads;

	g_config.query_worker_threads = new_thread_count;

	// Add threads if count is increased
	for (uint32_t i = cur_threads; i < new_thread_count; i++) {
		cf_thread_create_transient(qwork_th, (void *)g_query_work_queue);
	}

	pthread_rwlock_unlock(&g_query_lock);
}

void
as_query_reinit(uint32_t new_thread_count)
{
	pthread_rwlock_wrlock(&g_query_lock);

	uint32_t cur_threads  = g_config.query_threads;

	g_config.query_threads = new_thread_count;

	for (uint32_t i = cur_threads; i < new_thread_count; i += 2) {
		cf_thread_create_transient(query_th, (void *)g_query_short_queue);
		cf_thread_create_transient(query_th, (void *)g_query_long_queue);
	}

	pthread_rwlock_unlock(&g_query_lock);
}
// **************************************************************************************************

// ************************************************************************************************
//                                         INDEX KEYS ARR

static bool
process_keys_arr_ele(as_query_transaction *qtr, as_index_keys_arr *k_a,
		uint32_t ix, as_index_ref *r_ref)
{
	as_namespace *ns = qtr->ns;

	// TODO - proper EE split.
	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		cf_digest *keyd = &k_a->u.digests[ix];

		uint32_t pid = as_partition_getid(keyd);
		as_index_tree *tree = qtr->qctx.reserved_trees[pid];

		if (as_record_get(tree, keyd, r_ref) != 0) {
			return false;
		}
	}
	else {
		cf_arenax_handle r_h = k_a->u.handles[ix];
		as_record *r = cf_arenax_resolve(ns->arena, r_h);

		*r_ref = (as_index_ref){ .r = r, .r_h = r_h };

		// Reset to indicate that the reference is released.
		k_a->u.handles[ix] = 0;

		if (! release_ref_lock_live(qtr, r_ref)) {
			return false;
		}
	}

	return true;
}

static int
release_r_h_cb(cf_ll_element *ele, void *udata)
{
	as_query_transaction *qtr = udata;
	as_namespace *ns = qtr->ns;

	// TODO - proper EE split.
	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		return CF_LL_REDUCE_DELETE;
	}

	as_index_keys_ll_element *node = (as_index_keys_ll_element *)ele;
	as_index_keys_arr *k_a = node->keys_arr;

	for (uint32_t i = 0; i < k_a->num; i++) {
		cf_arenax_handle r_h = k_a->u.handles[i];

		if (r_h == 0) {
			continue;
		}

		as_record *r = cf_arenax_resolve(ns->arena, r_h);
		as_index_ref r_ref = { .r = r, .r_h = r_h };

		as_query_release_ref(ns, &qtr->qctx, &r_ref);
	}

	return CF_LL_REDUCE_DELETE;
}

void
as_index_keys_ll_destroy_fn(cf_ll_element *ele)
{
	as_index_keys_ll_element *node = (as_index_keys_ll_element *) ele;

	cf_free(node->keys_arr);
	cf_free(node);
}

void
as_query_release_ref(as_namespace *ns, as_sindex_qctx *qctx,
		as_index_ref *r_ref)
{
	// TODO - proper EE split.
	if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		return;
	}

	as_record *r = r_ref->r;
	uint32_t pid = as_partition_getid(&r->keyd);
	as_index_tree *tree = qctx->reserved_trees[pid];

	as_index_vlock(tree, r_ref);

	as_index_release(r);
	as_record_done(r_ref, ns);
}

//                                      END - INDEX KEYS ARR
// ************************************************************************************************
