/*
 * thr_sindex.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
 * SYNOPSIS
 * This file implements supporting threads for the secondary index implementation.
 * Currently following two main threads are implemented here
 *
 * -  Secondary index gc thread which walks sweeps through secondary indexes
 *   and cleanup the stale entries by looking up digest in the primary index.
 *
 * -  Secondary index thread which cleans up secondary index entry for a particular
 *    partitions
 *
 */

#include "base/thr_sindex.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"

#include "ai_obj.h"
#include "ai_btree.h"
#include "cf_thread.h"
#include "fault.h"
#include "shash.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/job_manager.h"
#include "base/monitor.h"
#include "base/secondary_index.h"
#include "base/stats.h"
#include "fabric/partition.h"


int as_sbld_build(as_sindex* si);

// All this is global because Aerospike Index is single threaded
pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t g_ai_rwlock     = PTHREAD_RWLOCK_INITIALIZER;

cf_queue *g_sindex_populate_q;
cf_queue *g_sindex_destroy_q;
cf_queue *g_sindex_populateall_done_q;
cf_queue *g_q_objs_to_defrag;
bool      g_sindex_boot_done;

typedef struct as_sindex_set_s {
	as_namespace * ns;
	as_set * set;
} as_sindex_set;

int
ll_sindex_gc_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
as_sindex_gc_release_gc_arr_to_queue(void *v)
{
	objs_to_defrag_arr *dt = (objs_to_defrag_arr *)v;
	if (cf_queue_sz(g_q_objs_to_defrag) < SINDEX_GC_QUEUE_HIGHWATER) {
		cf_queue_push(g_q_objs_to_defrag, &dt);
	}
	else {
		cf_free(dt);
	}
}

void
ll_sindex_gc_destroy_fn(cf_ll_element *ele)
{
	ll_sindex_gc_element * node = (ll_sindex_gc_element *) ele;
	if (node) {
		as_sindex_gc_release_gc_arr_to_queue((void *)(node->objs_to_defrag));
		cf_free(node);
	}
}

objs_to_defrag_arr *
as_sindex_gc_get_defrag_arr(void)
{
	objs_to_defrag_arr *dt;
	if (cf_queue_pop(g_q_objs_to_defrag, &dt, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		dt = cf_malloc(sizeof(objs_to_defrag_arr));
	}
	dt->num = 0;
	return dt;
}

// Main thread which looks at the request of the populating index
void *
as_sindex__populate_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_populate_q, &si, CF_QUEUE_FOREVER);
		// TODO should check flag under a lock
		// conflict with as_sindex_repair
		if (si->flag & AS_SINDEX_FLAG_POPULATING) {
			// Earlier job to populate index is still going on, push it back
			// into the queue to look at it later. this is problem only when
			// there are multiple populating threads currently there is only 1.
			cf_queue_push(g_sindex_populate_q, &si);
		} else {
			cf_debug(AS_SINDEX, "Populating index %s", si->imd->iname);
			// should set under a lock
			si->flag |= AS_SINDEX_FLAG_POPULATING;
			si->stats.recs_pending = si->ns->n_objects;
			as_sbld_build(si);
		}
	}
	return NULL;
}


// Main thread which looks at the request of the destroy of index
void *
as_sindex__destroy_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_destroy_q, &si, CF_QUEUE_FOREVER);

		SINDEX_GWLOCK();
		cf_assert((si->state == AS_SINDEX_DESTROY),
				AS_SINDEX, " Invalid state %d at cleanup expected %d for %p and %s", si->state, AS_SINDEX_DESTROY, si, (si) ? ((si->imd) ? si->imd->iname : NULL) : NULL);
		int rv = as_sindex__delete_from_set_binid_hash(si->ns, si->imd);
		if (rv) {
			cf_warning(AS_SINDEX, "Delete from set_binid hash fails with error %d", rv);
		}
		// Free entire usage counter before tree destroy
		cf_atomic64_sub(&si->ns->n_bytes_sindex_memory,
				ai_btree_get_isize(si->imd) + ai_btree_get_nsize(si->imd));

		// Cache the ibtr pointers
		uint16_t nprts = si->imd->nprts;
		struct btree *ibtr[nprts];
		for (int i = 0; i < nprts; i++) {
			as_sindex_pmetadata *pimd = &si->imd->pimd[i];
			ibtr[i] = pimd->ibtr;
			ai_btree_reset_pimd(pimd);
		}

		as_sindex_destroy_pmetadata(si);
		si->state = AS_SINDEX_INACTIVE;
		si->flag  = 0;

		si->ns->sindex_cnt--;

		if (si->imd->set) {
			as_set *p_set = as_namespace_get_set_by_name(si->ns, si->imd->set);
			p_set->n_sindexes--;
		} else {
			si->ns->n_setless_sindexes--;
		}

		as_sindex_metadata *imd = si->imd;
		si->imd = NULL;

		char iname[AS_ID_INAME_SZ];
		memset(iname, 0, AS_ID_INAME_SZ);
		snprintf(iname, strlen(imd->iname) + 1, "%s", imd->iname);
		cf_shash_delete(si->ns->sindex_iname_hash, (void *)iname);


		as_namespace *ns = si->ns;
		si->ns      = NULL;
		si->simatch = -1;

		as_sindex_metadata *recreate_imd = NULL;
		if (si->recreate_imd) {
			recreate_imd = si->recreate_imd;
			si->recreate_imd = NULL;
		}

		// remember this is going to release the write lock
		// of meta-data first. This is the only special case
		// where both GLOCK and LOCK is called together
		SINDEX_GWUNLOCK();

		// Destroy cached ibtr pointer
		for (int i = 0; i < imd->nprts; i++) {
			ai_btree_delete_ibtr(ibtr[i]);
		}
		as_sindex_imd_free(imd);
		cf_rc_free(imd);

		if (recreate_imd) {
			as_sindex_create(ns, recreate_imd);
			as_sindex_imd_free(recreate_imd);
			cf_rc_free(recreate_imd);
		}
	}
	return NULL;
}

void
as_sindex_update_gc_stat(as_sindex *si, uint64_t r, uint64_t start_time_ms)
{
	cf_atomic64_add(&si->stats.n_deletes,        r);
	cf_atomic64_add(&si->stats.n_objects,        -r);
	cf_atomic64_add(&si->stats.n_defrag_records, r);
	cf_atomic64_add(&si->stats.defrag_time, cf_getms() - start_time_ms);
}

typedef struct gc_stat_s {
	uint64_t  processed;
	uint64_t  found;
	uint64_t  deleted;
	uint64_t  creation_time;
	uint64_t  deletion_time;
} gc_stat;

typedef struct gc_ctx_s {
	uint32_t      ns_id;
	as_sindex    *si;
	uint16_t      pimd_idx;

	// stat
	gc_stat      stat;

	// config
	uint64_t     start_time;
	uint32_t     gc_max_rate;
} gc_ctx;

typedef struct gc_offset_s {
	ai_obj    i_col;
	uint64_t  pos;  // uint actually
	bool      done;
} gc_offset;

static bool
can_gc_si(as_sindex *si, uint16_t pimd_idx)
{
	if (! as_sindex_isactive(si)) {
		return false;
	}

	if (si->state == AS_SINDEX_DESTROY) {
		return false;
	}

	// pimd_idx we are iterating does not
	// exist in this sindex.
	if (pimd_idx >= si->imd->nprts) {
		return false;
	}

	return true;
}

static bool
gc_getnext_si(gc_ctx *ctx)
{
	int16_t si_idx;
	as_namespace *ns = g_config.namespaces[ctx->ns_id];

	// From previous si_idx or 0
	if (ctx->si) {
		si_idx = ctx->si->simatch;
		AS_SINDEX_RELEASE(ctx->si);
		ctx->si = NULL;
	} else {
		si_idx = -1;
	}

	SINDEX_GRLOCK();

	while (true) {

		si_idx++;
		if (si_idx == AS_SINDEX_MAX) {
			SINDEX_GRUNLOCK();
			return false;
		}

		as_sindex *si = &ns->sindex[si_idx];

		if (! can_gc_si(si, ctx->pimd_idx)) {
			continue;
		}

		AS_SINDEX_RESERVE(si);
		ctx->si = si;
		SINDEX_GRUNLOCK();
		return true;
	}
}

static void
gc_print_ctx(gc_ctx *ctx)
{
	cf_detail(AS_SINDEX, "%s %s[%d]", g_config.namespaces[ctx->ns_id]->name,
			ctx->si ? ctx->si->imd->iname : "NULL", ctx->pimd_idx);
}

// TODO - Find the correct values
#define CREATE_LIST_PER_ITERATION_LIMIT   10000
#define PROCESS_LIST_PER_ITERATION_LIMIT  10

// true if tree is done
// false if more in tree
static bool
gc_create_list(as_sindex *si, as_sindex_pmetadata *pimd, cf_ll *gc_list,
		gc_offset *offsetp, gc_stat *statp)
{
	uint64_t processed = 0;
	uint64_t found = 0;
	uint64_t limit_per_iteration = CREATE_LIST_PER_ITERATION_LIMIT;

	uint64_t start_time = cf_getms();

	PIMD_RLOCK(&pimd->slock);
	as_sindex_status ret = ai_btree_build_defrag_list(si->imd, pimd,
			&offsetp->i_col, &offsetp->pos, limit_per_iteration,
			&processed, &found, gc_list);

	PIMD_RUNLOCK(&pimd->slock);

	statp->creation_time += (cf_getms() - start_time);
	statp->processed += processed;
	statp->found += found;

	if (ret == AS_SINDEX_DONE) {
		offsetp->done = true;
	}

	if (ret == AS_SINDEX_ERR) {
		return false;
	}

	return true;
}

static void
gc_process_list(as_sindex *si, as_sindex_pmetadata *pimd, cf_ll *gc_list,
		gc_offset *offsetp, gc_stat *statp)
{
	uint64_t deleted = 0;
	uint64_t start_time = cf_getms();
	uint64_t limit_per_iteration = PROCESS_LIST_PER_ITERATION_LIMIT;

	bool more = true;

	while (more) {

		PIMD_WLOCK(&pimd->slock);
		more = ai_btree_defrag_list(si->imd, pimd, gc_list,
				limit_per_iteration, &deleted);
		PIMD_WUNLOCK(&pimd->slock);
	}

	// Update secondary index object count
	// statistics aggressively.
	as_sindex_update_gc_stat(si, deleted, start_time);

	statp->deletion_time = cf_getms() - start_time;
	statp->deleted += deleted;
}

static void
gc_throttle(gc_ctx *ctx)
{
	while (true) {
		uint64_t expected_processed =
			(cf_get_seconds() - ctx->start_time) * ctx->gc_max_rate;

		// processed less than expected
		// no throttling needed.
		if (ctx->stat.processed <= expected_processed) {
			break;
		}

		usleep(10000); // 10 ms
	}
}

static void
do_gc(gc_ctx *ctx)
{
	// SKEY + Digest offset
	gc_offset offset;
	init_ai_obj(&offset.i_col);
	offset.pos = 0;
	offset.done = false;

	as_sindex *si = ctx->si;
	as_sindex_pmetadata *pimd = &si->imd->pimd[ctx->pimd_idx];

	cf_ll gc_list;
	cf_ll_init(&gc_list, &ll_sindex_gc_destroy_fn, false);

	while (true) {

		if (! gc_create_list(si, pimd, &gc_list, &offset, &ctx->stat)) {
			break;
		}

		if (cf_ll_size(&gc_list) > 0) {
			gc_process_list(si, pimd, &gc_list, &offset, &ctx->stat);
			cf_ll_reduce(&gc_list, true /*forward*/, ll_sindex_gc_reduce_fn, NULL);
		}

		if (offset.done) {
			break;
		}
	}

	cf_ll_reduce(&gc_list, true /*forward*/, ll_sindex_gc_reduce_fn, NULL);
}

static void
update_gc_stat(gc_stat *statp)
{
	g_stats.sindex_gc_objects_validated  += statp->processed;
	g_stats.sindex_gc_garbage_found      += statp->found;
	g_stats.sindex_gc_garbage_cleaned    += statp->deleted;
	g_stats.sindex_gc_list_deletion_time += statp->deletion_time;
	g_stats.sindex_gc_list_creation_time += statp->creation_time;
}

void *
as_sindex__gc_fn(void *udata)
{
	while (! g_sindex_boot_done) {
		sleep(10);
		continue;
	}

	cf_debug(AS_SINDEX, "Secondary index gc thread started !!");

	uint64_t last_time = cf_get_seconds();

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t period = (uint64_t)g_config.sindex_gc_period;
		uint64_t curr_time = cf_get_seconds();

		if (period == 0 || curr_time - last_time < period) {
			continue;
		}

		last_time = curr_time;

		for (int i = 0; i < g_config.n_namespaces; i++) {

			as_namespace *ns = g_config.namespaces[i];

			if (ns->sindex_cnt == 0) {
				continue;
			}

			cf_info(AS_NSUP, "{%s} sindex-gc start", ns->name);

			uint64_t start_time_ms = cf_getms();

			// gc_max_rate change at the namespace boundary
			gc_ctx ctx = {
				.ns_id = i,
				.si = NULL,
				.stat = { 0 },
				.start_time = cf_get_seconds(),
				.gc_max_rate = g_config.sindex_gc_max_rate
			};

			// Give one pimd quata of chance for every sindex
			// in a namespace in round robin manner.
			for (uint16_t pimd_idx = 0; pimd_idx < MAX_PARTITIONS_PER_INDEX;
					pimd_idx++) {

				ctx.pimd_idx = pimd_idx;

				while (gc_getnext_si(&ctx)) {
					gc_print_ctx(&ctx);
					do_gc(&ctx);

					// throttle after every quota (1 pimd)
					gc_throttle(&ctx);
				}
			}

			cf_info(AS_NSUP, "{%s} sindex-gc: Processed: %ld, found:%ld, deleted: %ld: Total time: %ld ms",
					ns->name, ctx.stat.processed, ctx.stat.found, ctx.stat.deleted,
					cf_getms() - start_time_ms);

			update_gc_stat(&ctx.stat);
		}
	}

	return NULL;
}


/*
 * Secondary index main gc thread, it keeps watching out for request to
 * the gc, Client API to set up aerospike facing meta data for the secondary index
 * and setting all the initial things
 *
 * Parameter:
 *		 sindex_metadata:  (in/out) Index meta-data structure
 *
 * Caller:
 *		aerospike
 * Return:
 *		0: On success
 *		-1: On failure
 * Synchronization:
 * 		Acquires the meta lock.
 */
void
as_sindex_thr_init()
{
	// Thread request read lock on this recursively could possibly cause deadlock. Caller
	// should be careful with that
	pthread_rwlockattr_t rwattr;
	if (!g_q_objs_to_defrag) {
		g_q_objs_to_defrag = cf_queue_create(sizeof(void *), true);
	}
	if (0 != pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_SINDEX, "pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash( AS_SINDEX, "pthread_rwlockattr_setkind_np: %s", cf_strerror(errno));

	// Aerospike Index Metadata lock
	if (0 != pthread_rwlock_init(&g_ai_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	// Sindex Metadata lock
	if (0 != pthread_rwlock_init(&g_sindex_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	g_sindex_populate_q = cf_queue_create(sizeof(as_sindex *), true);
	g_sindex_destroy_q = cf_queue_create(sizeof(as_sindex *), true);

	cf_thread_create_detached(as_sindex__populate_fn, NULL);
	cf_thread_create_detached(as_sindex__destroy_fn, NULL);
	cf_thread_create_detached(as_sindex__gc_fn, NULL);

	g_sindex_populateall_done_q = cf_queue_create(sizeof(int), true);
	// At the beginning it is false. It is set to true when all the sindex
	// are populated.
	g_sindex_boot_done = false;
}


//==============================================================================
// Secondary index builder.
//

// sbld_job - derived class header:
typedef struct sbld_job_s {
	// Base object must be first:
	as_job			_base;

	// Derived class data:
	as_sindex*		si;

	char*			si_name;
	cf_atomic64		n_reduced;
} sbld_job;

sbld_job* sbld_job_create(as_namespace* ns, uint16_t set_id, as_sindex* si);

// as_job_manager instance for secondary index builder:
static as_job_manager g_sbld_manager;


//------------------------------------------------
// Sindex builder public API.
//

void
as_sbld_init()
{
	// TODO - config for max done?
	// Initialize with maximum threads since first use is always build-all at
	// startup. The thread pool will be down-sized right after that.
	as_job_manager_init(&g_sbld_manager, UINT_MAX, 100, MAX_SINDEX_BUILDER_THREADS);
}

int
as_sbld_build(as_sindex* si)
{
	as_sindex_metadata *imd = si->imd;
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);

	if (! ns) {
		cf_warning(AS_SINDEX, "sindex build %s ns %s - unrecognized namespace", imd->iname, imd->ns_name);
		as_sindex_populate_done(si);
		AS_SINDEX_RELEASE(si);
		return -1;
	}

	uint16_t set_id = INVALID_SET_ID;

	if (imd->set && (set_id = as_namespace_get_set_id(ns, imd->set)) == INVALID_SET_ID) {
		cf_info(AS_SINDEX, "sindex build %s ns %s - set %s not found - assuming empty", imd->iname, imd->ns_name, imd->set);
		as_sindex_populate_done(si);
		AS_SINDEX_RELEASE(si);
		return -3;
	}

	sbld_job* job = sbld_job_create(ns, set_id, si);

	// Can't fail for this kind of job.
	as_job_manager_start_job(&g_sbld_manager, (as_job*)job);

	return 0;
}

void
as_sbld_build_all(as_namespace* ns)
{
	sbld_job* job = sbld_job_create(ns, INVALID_SET_ID, NULL);

	// Can't fail for this kind of job.
	as_job_manager_start_job(&g_sbld_manager, (as_job*)job);
}

void
as_sbld_resize_thread_pool(uint32_t n_threads)
{
	as_job_manager_resize_thread_pool(&g_sbld_manager, n_threads);
}

int
as_sbld_list(char* name, cf_dyn_buf* db)
{
	as_mon_info_cmd(AS_MON_MODULES[SBLD_MOD], NULL, 0, 0, db);
	return 0;
}

as_mon_jobstat*
as_sbld_get_jobstat(uint64_t trid)
{
	return as_job_manager_get_job_info(&g_sbld_manager, trid);
}

as_mon_jobstat*
as_sbld_get_jobstat_all(int* size)
{
	return as_job_manager_get_info(&g_sbld_manager, size);
}

int
as_sbld_abort(uint64_t trid)
{
	return as_job_manager_abort_job(&g_sbld_manager, trid) ? 0 : -1;
}


//------------------------------------------------
// sbld_job derived class implementation.
//

void sbld_job_slice(as_job* _job, as_partition_reservation* rsv);
void sbld_job_finish(as_job* _job);
void sbld_job_destroy(as_job* _job);
void sbld_job_info(as_job* _job, as_mon_jobstat* stat);

const as_job_vtable sbld_job_vtable = {
		sbld_job_slice,
		sbld_job_finish,
		sbld_job_destroy,
		sbld_job_info
};

void sbld_job_reduce_cb(as_index_ref* r_ref, void* udata);

//
// sbld_job creation.
//

sbld_job*
sbld_job_create(as_namespace* ns, uint16_t set_id, as_sindex* si)
{
	sbld_job* job = cf_malloc(sizeof(sbld_job));

	as_job_init((as_job*)job, &sbld_job_vtable, &g_sbld_manager,
			RSV_MIGRATE, 0, ns, set_id, AS_JOB_PRIORITY_MEDIUM, "");

	job->si = si;
	job->si_name = si ? cf_strdup(si->imd->iname) : NULL;
	job->n_reduced = 0;

	return job;
}

//
// sbld_job mandatory as_job interface.
//

void
sbld_job_slice(as_job* _job, as_partition_reservation* rsv)
{
	as_index_reduce_live(rsv->tree, sbld_job_reduce_cb, (void*)_job);
}

void
sbld_job_finish(as_job* _job)
{
	sbld_job* job = (sbld_job*)_job;

	as_sindex_ticker_done(_job->ns, job->si, _job->start_ms);

	if (job->si) {
		as_sindex_populate_done(job->si);
		job->si->stats.loadtime = cf_getms() - _job->start_ms;
		AS_SINDEX_RELEASE(job->si);
	}
	else {
		as_sindex_boot_populateall_done(_job->ns);
	}
}

void
sbld_job_destroy(as_job* _job)
{
	sbld_job* job = (sbld_job*)_job;

	if (job->si_name) {
		cf_free(job->si_name);
	}
}

void
sbld_job_info(as_job* _job, as_mon_jobstat* stat)
{
	sbld_job* job = (sbld_job*)_job;

	if (job->si_name) {
		strcpy(stat->job_type, "sindex-build");

		char *extra = stat->jdata + strlen(stat->jdata);

		sprintf(extra, ":sindex-name=%s", job->si_name);
	}
	else {
		strcpy(stat->job_type, "sindex-build-all");
	}
}

//
// sbld_job utilities.
//

void
sbld_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_job* _job = (as_job*)udata;
	sbld_job* job = (sbld_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->si) {
		cf_atomic64_decr(&job->si->stats.recs_pending);
	}

	as_sindex_ticker(ns, job->si, cf_atomic64_incr(&job->n_reduced), _job->start_ms);

	as_index *r = r_ref->r;

	if ((_job->set_id != INVALID_SET_ID && _job->set_id != as_index_get_set_id(r)) ||
			as_record_is_doomed(r, ns)) {
		as_record_done(r_ref, ns);
		return;
	}

	as_storage_rd rd;
	as_storage_record_open(ns, r, &rd);
	as_storage_rd_load_n_bins(&rd); // TODO - handle error returned
	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
	as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned

	if (job->si) {
		if (as_sindex_put_rd(job->si, &rd)) {
			as_record_done(r_ref, ns);
			as_job_manager_abandon_job(_job->mgr, _job, AS_JOB_FAIL_UNKNOWN);
			return;
		}
	}
	else {
		as_sindex_putall_rd(ns, &rd);
	}

	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);

	cf_atomic64_incr(&_job->n_records_read);
}
