/*
 * drv_mem.c
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include "storage/drv_mem.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h> // for BLKGETSIZE64
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <unistd.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_random.h"

#include "bits.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "hardware.h"
#include "hist.h"
#include "linear_hist.h"
#include "log.h"
#include "os.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/nsup.h"
#include "base/set_index.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

// For memory only with no backing.
#define MAX_STRIPE_SZ (2UL * 1024 * 1024 * 1024 * 1024) // 2T
#define MIN_N_STRIPES 8

typedef struct mem_load_records_info_s {
	drv_mems* mems;
	drv_mem* mem;
	cf_queue* complete_q;
	void* complete_rc;
} mem_load_records_info;

#define DEFRAG_PEN_INIT_CAPACITY (8 * 1024)

typedef struct defrag_pen_s {
	uint32_t n_ids;
	uint32_t capacity;
	uint32_t* ids;
	uint32_t stack_ids[DEFRAG_PEN_INIT_CAPACITY];
} defrag_pen;

#define LOG_STATS_INTERVAL_sec 20

// All in microseconds since we're using usleep().
#define MAX_INTERVAL		(1000 * 1000)
#define LOG_STATS_INTERVAL	(1000 * 1000 * LOG_STATS_INTERVAL_sec)
#define FREE_SWBS_INTERVAL	(1000 * 1000 * 20)

#define VACATED_CAPACITY_STEP 128 // allocate in 1K chunks


//==========================================================
// Forward declarations.
//

// Startup control.
static void init_shadow_devices(as_namespace* ns);
static void init_shadow_files(as_namespace* ns);
static void init_memory_only(as_namespace* ns);
static uint64_t check_file_size(uint64_t file_size, const char* tag);
static uint64_t find_io_min_size(int fd, const char* shadow_name);
static void init_synchronous(drv_mems* mems);
static drv_header* read_header(drv_mem* mem);
static drv_header* init_header(as_namespace* ns, drv_mem* mem);
static void wblock_init(drv_mem* mem);
static void flush_header(drv_mems* mems, drv_header** headers);
static void init_pristine_wblock_id(drv_mem* mem, uint64_t offset);
static void start_loading_records(drv_mems* mems, cf_queue* complete_q);
static void load_wblock_queues(drv_mems* mems);
static void* run_load_queues(void* pv_data);
static void defrag_pen_init(defrag_pen* pen);
static void defrag_pen_destroy(defrag_pen* pen);
static void defrag_pen_add(defrag_pen* pen, uint32_t wblock_id);
static void defrag_pen_transfer(defrag_pen* pen, drv_mem* mem);
static void defrag_pens_dump(defrag_pen pens[], uint32_t n_pens, const char* mem_name);
static void start_maintenance_threads(drv_mems* mems);
static void start_write_threads(drv_mems* mems);
static void start_defrag_threads(drv_mems* mems);

// Cold start.
static void* run_mem_cold_start(void* udata);
static void cold_start_add_record(drv_mems* mems, drv_mem* mem, const as_flat_record* flat, uint64_t rblock_id, uint32_t record_size);
static bool prefer_existing_record(const as_namespace* ns, const as_flat_record* flat, uint32_t block_void_time, const as_index* r);

// Shutdown.
static void set_pristine_offset(drv_mems* mems);
static void set_trusted(drv_mems* mems);

// Read record.
static int read_record(as_storage_rd* rd, bool pickle_only);
static bool sanity_check_flat(const drv_mem* mem, const as_record* r, uint64_t record_offset, const as_flat_record* flat);

// Write and recycle wblocks.
static void* run_shadow(void* arg);
static void write_sanity_checks(drv_mem* mem, mem_write_block* mwb);
static void push_wblock_to_defrag_q(drv_mem* mem, uint32_t wblock_id);
static void push_wblock_to_free_q(drv_mem* mem, uint32_t wblock_id);

// Write to header.
static void aligned_write_to_shadow(drv_mem* mem, const uint8_t* header, const uint8_t* from, size_t size);
static void flush_flags(drv_mems* mems);

// Defrag.
static void* run_defrag(void* pv_data);
static int defrag_wblock(drv_mem* mem, uint32_t wblock_id);
static int record_defrag(drv_mem* mem, uint32_t wblock_id, const as_flat_record* flat, uint64_t rblock_id);
static void defrag_move_record(drv_mem* src_mem, uint32_t src_wblock_id, const as_flat_record* flat, as_index* r);
static void release_vacated_wblock(drv_mem* mem, uint32_t wblock_id, mem_wblock_state* p_wblock_state);

// Maintenance.
static void* run_mem_maintenance(void* udata);
static void log_stats(drv_mem* mem, uint64_t* p_prev_n_total_writes, uint64_t* p_prev_n_defrag_reads, uint64_t* p_prev_n_defrag_writes, uint64_t* p_prev_n_defrag_io_skips, uint64_t* p_prev_n_direct_frees, uint64_t* p_prev_n_tomb_raider_reads);
static uint64_t next_time(uint64_t now, uint64_t job_interval, uint64_t next);
static void free_mwbs(drv_mem* mem);
static void flush_current_mwb(drv_mem* mem, uint8_t which, uint64_t* p_prev_n_writes);
static void flush_defrag_mwb(drv_mem* mem, uint64_t* p_prev_n_defrag_writes);
static void defrag_sweep(drv_mem* mem);

// mwb class.
static mem_write_block* mwb_create(drv_mem* mem);
static void mwb_destroy(mem_write_block* mwb);
static void mwb_reset(mem_write_block* mwb);
static bool pop_pristine_wblock_id(drv_mem* mem, uint32_t* wblock_id);
static bool mwb_add_unique_vacated_wblock(mem_write_block* mwb, uint32_t src_file_id, uint32_t src_wblock_id);
static void mwb_release_all_vacated_wblocks(mem_write_block* mwb);

// Persistence utilities.
static void prepare_for_first_write(mem_write_block* mwb);

// Shadow utilities.
static void shadow_fd_put(drv_mem* mem, int fd);
static void shadow_flush_mwb(drv_mem* mem, mem_write_block* mwb);
static void shadow_flush_buf(drv_mem* mem, const uint8_t* buf, off_t write_offset, uint64_t write_sz);


//==========================================================
// Inlines & macros.
//

// Decide which device a record belongs on.
static inline uint32_t
mem_get_file_id(const drv_mems* mems, const cf_digest* keyd)
{
	return *(uint32_t*)&keyd->digest[DIGEST_STORAGE_BASE_BYTE] % mems->n_mems;
}

static inline uint32_t
num_pristine_wblocks(const drv_mem* mem)
{
	return mem->n_wblocks - mem->pristine_wblock_id;
}

static inline uint32_t
num_free_wblocks(const drv_mem* mem)
{
	return cf_queue_sz(mem->free_wblock_q) + num_pristine_wblocks(mem);
}

static inline void
push_wblock_to_shadow_q(drv_mem* mem, const mem_write_block* mwb)
{
	as_incr_uint32(&mem->ns->n_wblocks_to_flush);
	cf_queue_push(mem->mwb_shadow_q, &mwb);
}

// Available contiguous size.
static inline uint64_t
available_size(const drv_mem* mem)
{
	// Note - returns 100% available during cold start, to make it irrelevant in
	// cold start eviction threshold check.

	return mem->free_wblock_q != NULL ?
			(uint64_t)num_free_wblocks(mem) * WBLOCK_SZ : mem->file_size;
}

static inline void
release_old_mwb(drv_mem* mem, mem_write_block* old_mwb)
{
	if (old_mwb != NULL) {
		mem_wait_writers_done(old_mwb);
		mem_mprotect(old_mwb->base_addr, WBLOCK_SZ, PROT_READ);
		mwb_release(mem, old_mwb);
	}
}


//==========================================================
// Public API.
//

void
as_storage_init_mem(as_namespace* ns)
{

	if (ns->n_storage_devices != 0) {
		init_shadow_devices(ns);
	}
	else if (ns->n_storage_files != 0) {
		init_shadow_files(ns);
	}
	else {
		init_memory_only(ns);
	}

	drv_mems* mems = (drv_mems*)ns->storage_private;

	g_unique_data_size += ns->drives_size / (2 * ns->cfg_replication_factor);

	cf_mutex_init(&mems->flush_lock);

	// The queue limit is more efficient to work with.
	ns->storage_max_write_q = (uint32_t)
			(mems->n_mems * ns->storage_max_write_cache / WBLOCK_SZ);

	// Minimize how often we recalculate this.
	ns->defrag_lwm_size = (WBLOCK_SZ * ns->storage_defrag_lwm_pct) / 100;

	char histname[HISTOGRAM_NAME_SIZE];

	snprintf(histname, sizeof(histname), "{%s}-device-read-size", ns->name);
	ns->device_read_size_hist = histogram_create(histname, HIST_SIZE);

	snprintf(histname, sizeof(histname), "{%s}-device-write-size", ns->name);
	ns->device_write_size_hist = histogram_create(histname, HIST_SIZE);

	uint32_t first_wblock_id = DRV_HEADER_SIZE / WBLOCK_SZ;
	histogram_scale scale = as_config_histogram_scale();

	// Finish initializing drv_mem structures (non-zero-value members).
	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		mem->ns = ns;
		mem->file_id = i;

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			cf_mutex_init(&mem->current_mwbs[c].lock);
		}

		cf_mutex_init(&mem->defrag_lock);

		if (mem->shadow_name != NULL) {
			mem->running_shadow = true;
		}

		// Some (non-dynamic) config shortcuts:
		mem->first_wblock_id = first_wblock_id;

		// Non-fresh devices will initialize this appropriately later.
		mem->pristine_wblock_id = first_wblock_id;

		// Note - delay wblock_init() until we have shmem keys for stripe names.

		// Note: free_wblock_q, defrag_wblock_q created after loading devices.

		if (mem->shadow_name != NULL) {
			mem->shadow_fd_q = cf_queue_create(sizeof(int), true);
		}

		if (mem->shadow_name != NULL) {
			mem->mwb_shadow_q = cf_queue_create(sizeof(void*), true);
		}

		mem->mwb_free_q = cf_queue_create(sizeof(void*), true);

		if (mem->shadow_name != NULL) {
			snprintf(histname, sizeof(histname), "{%s}-%s-write", ns->name,
					mem->shadow_name);
			mem->hist_shadow_write = histogram_create(histname, scale);
		}

		init_commit(mem);
	}

	// Will load headers and, if warm restart, resume persisted index.
	init_synchronous(mems);
}

void
as_storage_load_mem(as_namespace* ns, cf_queue* complete_q)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	// If devices have data, and it's cold start, scan devices.
	if (! mems->all_fresh && ns->cold_start) {
		// Fire off threads to scan devices to build index and/or load record
		// data into memory - will signal completion when threads are all done.
		start_loading_records(mems, complete_q);
		return;
	}
	// else - fresh devices or warm restart, this namespace is ready to roll.

	void* _t = NULL;

	cf_queue_push(complete_q, &_t);
}

void
as_storage_load_ticker_mem(const as_namespace* ns)
{
	char buf[1024];
	int pos = 0;
	const drv_mems* mems = (const drv_mems*)ns->storage_private;

	for (int i = 0; i < mems->n_mems; i++) {
		const drv_mem* mem = &mems->mems[i];
		uint32_t pct = (uint32_t)
				((mem->sweep_wblock_id * 100UL) / (mem->file_size / WBLOCK_SZ));

		pos += sprintf(buf + pos, "%u,", pct);
	}

	if (pos != 0) {
		buf[pos - 1] = '\0'; // chomp last comma
	}

	if (drv_mrt_2nd_pass_load_ticker(ns, buf)) {
		return;
	}

	if (ns->n_tombstones == 0) {
		cf_info(AS_DRV_MEM, "{%s} loaded: objects %lu device-pcts (%s)",
				ns->name, ns->n_objects, buf);
	}
	else {
		cf_info(AS_DRV_MEM, "{%s} loaded: objects %lu tombstones %lu device-pcts (%s)",
				ns->name, ns->n_objects, ns->n_tombstones, buf);
	}
}

void
as_storage_activate_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	load_wblock_queues(mems);

	start_maintenance_threads(mems);
	start_write_threads(mems);

	if (ns->storage_defrag_startup_minimum != 0) {
		// Allow defrag to go full speed during startup - restore configured
		// setting when startup is done.
		ns->saved_defrag_sleep = ns->storage_defrag_sleep;
		ns->storage_defrag_sleep = 0;
	}

	start_defrag_threads(mems);
}

bool
as_storage_wait_for_defrag_mem(as_namespace* ns)
{
	if (ns->storage_defrag_startup_minimum == 0) {
		return false; // nothing to do - don't wait
	}

	uint32_t avail_pct;

	as_storage_stats_mem(ns, &avail_pct, NULL);

	if (avail_pct >= ns->storage_defrag_startup_minimum) {
		// Restore configured defrag throttling values.
		ns->storage_defrag_sleep = ns->saved_defrag_sleep;
		return false; // done - don't wait
	}
	// else - not done - wait.

	cf_info(AS_DRV_MEM, "{%s} wait-for-defrag: avail-pct %u wait-for %u ...",
			ns->name, avail_pct, ns->storage_defrag_startup_minimum);

	return true;
}

void
as_storage_shutdown_mem(struct as_namespace_s* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			current_mwb* cur_mwb = &mem->current_mwbs[c];

			// Stop the maintenance thread from (also) flushing the mwbs.
			cf_mutex_lock(&cur_mwb->lock);

			mem_write_block* mwb = cur_mwb->mwb;

			// Flush current mwb by pushing it to shadow-q.
			if (mwb != NULL) {
				if (mem->shadow_name != NULL &&
						! ns->storage_commit_to_device) {
					push_wblock_to_shadow_q(mem, mwb);
				}

				cur_mwb->mwb = NULL;
			}
		}

		// Stop the maintenance thread from (also) flushing the defrag mwb.
		cf_mutex_lock(&mem->defrag_lock);

		// Flush defrag mwb by pushing it to shadow-q.
		if (mem->defrag_mwb) {
			if (mem->shadow_name != NULL) {
				push_wblock_to_shadow_q(mem, mem->defrag_mwb);
			}

			mem->defrag_mwb = NULL;
		}
	}

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		mem->running_shadow = false;
	}

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		if (mem->shadow_name != NULL) {
			cf_thread_join(mem->shadow_tid);
		}
	}

	set_pristine_offset(mems);
	set_trusted(mems);
}

// Note that this is *NOT* the counterpart to as_storage_record_create()!
// That would be as_storage_record_close_mem(). This is what gets called when a
// record is destroyed, to dereference storage.
void
as_storage_destroy_record_mem(as_namespace* ns, as_record* r)
{
	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id) && r->n_rblocks != 0) {
		drv_mems* mems = (drv_mems*)ns->storage_private;
		drv_mem* mem = &mems->mems[r->file_id];

		block_free(mem, r->rblock_id, r->n_rblocks, "destroy");

		as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
				-(int64_t)N_RBLOCKS_TO_SIZE(r->n_rblocks));

		r->rblock_id = 0;
		r->n_rblocks = 0;

		mrt_block_free_orig(mems, r);
	}
}

void
as_storage_record_open_mem(as_storage_rd* rd)
{
	drv_mems* mems = (drv_mems*)rd->ns->storage_private;

	rd->mem = &mems->mems[rd->r->file_id];
}

int
as_storage_record_load_bins_mem(as_storage_rd* rd)
{
	if (as_record_is_binless(rd->r)) {
		rd->n_bins = 0;
		return 0; // no need to read device
	}

	// If record hasn't been read, read it - sets rd->flat_bins and
	// rd->flat_n_bins.
	if (! rd->flat && read_record(rd, false) != 0) {
		cf_warning(AS_DRV_MEM, "load_bins: failed mem_read_record()");
		return -AS_ERR_UNKNOWN;
	}

	int result = as_flat_unpack_bins(rd->ns, rd->flat_bins, rd->flat_end,
			rd->flat_n_bins, rd->bins);

	if (result == AS_OK) {
		rd->n_bins = rd->flat_n_bins;
	}

	return result;
}

bool
as_storage_record_load_key_mem(as_storage_rd* rd)
{
	// If record hasn't been read, read it - sets rd->key_size and rd->key.
	if (! rd->flat && read_record(rd, false) != 0) {
		cf_warning(AS_DRV_MEM, "get_key: failed mem_read_record()");
		return false;
	}

	return true;
}

bool
as_storage_record_load_pickle_mem(as_storage_rd* rd)
{
	if (read_record(rd, true) != 0) {
		return false;
	}

	size_t sz = rd->flat_end - (const uint8_t*)rd->flat;

	rd->pickle = cf_malloc(sz);
	rd->pickle_sz = (uint32_t)sz;

	memcpy(rd->pickle, rd->flat, sz);

	((as_flat_record*)rd->pickle)->n_rblocks = SIZE_TO_N_RBLOCKS(sz);

	return true;
}

bool
as_storage_record_load_raw_mem(as_storage_rd* rd, bool leave_encrypted)
{
	as_namespace* ns = rd->ns;

	if (leave_encrypted) {
		cf_warning(AS_DRV_MEM, "{%s} storage-engine memory doesn't encrypt memory",
				ns->name);
	}

	as_record* r = rd->r;
	drv_mem* mem = rd->mem;

	uint64_t record_offset = RBLOCK_ID_TO_OFFSET(r->rblock_id);
	uint32_t record_size = N_RBLOCKS_TO_SIZE(r->n_rblocks);
	uint64_t record_end_offset = record_offset + record_size;

	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(record_offset);

	bool ok = true;

	if (wblock_id >= mem->n_wblocks || wblock_id < mem->first_wblock_id) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD bad offset %lu", ns->name,
				&r->keyd, record_offset);
		ok = false;
	}

	if (record_size < DRV_RECORD_MIN_SIZE) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD bad record size %u",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (record_end_offset > WBLOCK_ID_TO_OFFSET(wblock_id + 1)) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD record size %u crosses wblock boundary",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (! ok) {
		return false;
	}

	const as_flat_record* flat =
			(const as_flat_record*)(mem->mem_base_addr + record_offset);

	rd->flat = flat;

	sanity_check_flat(mem, r, record_offset, flat);
	// Continue even on failure...

	// Does not exclude the mark!
	rd->flat_end = (const uint8_t*)flat + record_size;

	return true;
}

void
as_storage_defrag_sweep_mem(as_namespace* ns)
{
	cf_info(AS_DRV_MEM, "{%s} sweeping all devices for wblocks to defrag ...",
			ns->name);

	drv_mems* mems = (drv_mems*)ns->storage_private;

	for (int i = 0; i < mems->n_mems; i++) {
		as_incr_uint32(&mems->mems[i].defrag_sweep);
	}
}

void
as_storage_load_regime_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	ns->eventual_regime = mems->generic->prefix.eventual_regime;
	ns->rebalance_regime = ns->eventual_regime;
}

void
as_storage_save_regime_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	cf_mutex_lock(&mems->flush_lock);

	mems->generic->prefix.eventual_regime = ns->eventual_regime;

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		write_header(mem, (uint8_t*)mems->generic,
				(uint8_t*)&mems->generic->prefix.eventual_regime,
				sizeof(mems->generic->prefix.eventual_regime));
	}

	cf_mutex_unlock(&mems->flush_lock);
}

void
as_storage_load_roster_generation_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	ns->roster_generation = mems->generic->prefix.roster_generation;
}

void
as_storage_save_roster_generation_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	// Normal for this to not change, cleaner to check here versus outside.
	if (ns->roster_generation == mems->generic->prefix.roster_generation) {
		return;
	}

	cf_mutex_lock(&mems->flush_lock);

	mems->generic->prefix.roster_generation = ns->roster_generation;

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		write_header(mem, (uint8_t*)mems->generic,
				(uint8_t*)&mems->generic->prefix.roster_generation,
				sizeof(mems->generic->prefix.roster_generation));
	}

	cf_mutex_unlock(&mems->flush_lock);
}

void
as_storage_load_pmeta_mem(as_namespace* ns, as_partition* p)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	drv_pmeta* pmeta = &mems->generic->pmeta[p->id];

	p->version = pmeta->version;
}

void
as_storage_save_pmeta_mem(as_namespace* ns, const as_partition* p)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	drv_pmeta* pmeta = &mems->generic->pmeta[p->id];

	cf_mutex_lock(&mems->flush_lock);

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		write_header(mem, (uint8_t*)mems->generic, (uint8_t*)pmeta,
				sizeof(*pmeta));
	}

	cf_mutex_unlock(&mems->flush_lock);
}

void
as_storage_cache_pmeta_mem(as_namespace* ns, const as_partition* p)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	drv_pmeta* pmeta = &mems->generic->pmeta[p->id];

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;
}

void
as_storage_flush_pmeta_mem(as_namespace* ns, uint32_t start_pid,
		uint32_t n_partitions)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	drv_pmeta* pmeta = &mems->generic->pmeta[start_pid];

	cf_mutex_lock(&mems->flush_lock);

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		write_header(mem, (uint8_t*)mems->generic, (uint8_t*)pmeta,
				sizeof(drv_pmeta) * n_partitions);
	}

	cf_mutex_unlock(&mems->flush_lock);
}

void
as_storage_stats_mem(as_namespace* ns, uint32_t* avail_pct,
		uint64_t* used_bytes)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;

	if (avail_pct != NULL) {
		*avail_pct = 100;

		// Find the device with the lowest available percent.
		for (int i = 0; i < mems->n_mems; i++) {
			drv_mem* mem = &mems->mems[i];
			uint32_t pct =
					(uint32_t)((available_size(mem) * 100) / mem->file_size);

			if (pct < *avail_pct) {
				*avail_pct = pct;
			}
		}
	}

	if (used_bytes != NULL) {
		uint64_t sz = 0;

		for (int i = 0; i < mems->n_mems; i++) {
			sz += mems->mems[i].inuse_size;
		}

		*used_bytes = sz;
	}
}

void
as_storage_device_stats_mem(const as_namespace* ns, uint32_t device_ix,
		storage_device_stats* stats)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	drv_mem* mem = &mems->mems[device_ix];

	stats->used_sz = mem->inuse_size;
	stats->n_free_wblocks = num_free_wblocks(mem);

	stats->n_read_errors = 0; // not used

	stats->write_q_sz = 0; // not used
	stats->n_writes = 0;
	stats->n_partial_writes = 0;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		stats->n_writes += mem->current_mwbs[c].n_wblock_writes;
		stats->n_partial_writes += mem->current_mwbs[c].n_wblock_partial_writes;
	}

	stats->defrag_q_sz = cf_queue_sz(mem->defrag_wblock_q);
	stats->n_defrag_reads = mem->n_defrag_wblock_reads;
	stats->n_defrag_writes = mem->n_defrag_wblock_writes;
	stats->n_defrag_partial_writes = mem->n_defrag_wblock_partial_writes;

	stats->shadow_write_q_sz = mem->mwb_shadow_q != NULL ?
			cf_queue_sz(mem->mwb_shadow_q) : 0;
}

void
as_storage_ticker_stats_mem(as_namespace* ns)
{
	histogram_dump(ns->device_read_size_hist);
	histogram_dump(ns->device_write_size_hist);

	drv_mems* mems = (drv_mems*)ns->storage_private;

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		if (mem->hist_shadow_write) {
			histogram_dump(mem->hist_shadow_write);
		}
	}
}

void
as_storage_dump_wb_summary_mem(const as_namespace* ns, bool verbose)
{
	drv_mems* mems = ns->storage_private;

	uint32_t n_free = 0;
	uint32_t n_reserved = 0;
	uint32_t n_used = 0;
	uint32_t n_defrag = 0;
	uint32_t n_emptying = 0;
	uint32_t n_pristine = 0;

	uint32_t n_short_lived = 0;

	uint32_t n_zero_used_sz = 0;

	linear_hist* h =
			linear_hist_create("", LINEAR_HIST_SIZE, 0, WBLOCK_SZ, 100);


	for (uint32_t d = 0; d < mems->n_mems; d++) {
		drv_mem* mem = &mems->mems[d];

		uint32_t d_free = 0;
		uint32_t d_reserved = 0;
		uint32_t d_used = 0;
		uint32_t d_defrag = 0;
		uint32_t d_emptying = 0;
		uint32_t d_pristine = 0;
		uint32_t d_short_lived = 0;
		uint32_t d_zero_used_sz = 0;

		for (uint32_t i = mem->first_wblock_id; i < mem->n_wblocks; i++) {
			mem_wblock_state* wblock_state = &mem->wblock_state[i];

			// Treat wblocks beyond pristine_wblock_id as pristine regardless of state.
			if (i >= mem->pristine_wblock_id) {
				d_pristine++;
				n_pristine++;
				continue;
			}

			switch (wblock_state->state) {
			case WBLOCK_STATE_FREE:
				d_free++;
				n_free++;
				break;
			case WBLOCK_STATE_RESERVED:
				d_reserved++;
				n_reserved++;
				break;
			case WBLOCK_STATE_USED:
				d_used++;
				n_used++;
				break;
			case WBLOCK_STATE_DEFRAG:
				d_defrag++;
				n_defrag++;
				break;
			case WBLOCK_STATE_EMPTYING:
				d_emptying++;
				n_emptying++;
				break;
			default:
				cf_warning(AS_DRV_MEM, "bad wblock state %u",
						wblock_state->state);
				break;
			}

			if (wblock_state->short_lived) {
				d_short_lived++;
				n_short_lived++;
			}

			uint32_t inuse_sz = as_load_uint32(&wblock_state->inuse_sz);

			if (inuse_sz == 0) {
				d_zero_used_sz++;
				n_zero_used_sz++;
			}
			else {
				linear_hist_insert_data_point(h, inuse_sz);
			}
		}

		if (verbose) {
			cf_info(AS_DRV_MEM, "WB: device %s: pristine:%u reserved:%u used:%u defrag:%u emptying:%u free:%u", mem->name,
				d_pristine, d_reserved, d_used, d_defrag, d_emptying, d_free);

			if (d_short_lived != 0) {
				cf_info(AS_DRV_MEM, "WB: device %s: short-lived:%u", mem->name, d_short_lived);
			}

			cf_info(AS_DRV_MEM, "WB: device %s: zero-used-sz:%u", mem->name, d_zero_used_sz);
		}
	}

	cf_info(AS_DRV_MEM, "WB: namespace %s", ns->name);
	cf_info(AS_DRV_MEM, "WB: wblocks by state - pristine:%u reserved:%u used:%u defrag:%u emptying:%u free:%u",
			n_pristine, n_reserved, n_used, n_defrag, n_emptying, n_free);

	if (n_short_lived != 0) {
		cf_info(AS_DRV_MEM, "WB: short-lived wblocks - %u", n_short_lived);
	}

	cf_dyn_buf_define(db);

	// Not bothering with more suitable linear_hist API ... use what's there.
	linear_hist_save_info(h);
	linear_hist_get_info(h, &db);
	cf_dyn_buf_append_char(&db, '\0');

	cf_info(AS_DRV_MEM, "WB: wblocks with zero used-sz - %u", n_zero_used_sz);
	cf_info(AS_DRV_MEM, "WB: wblocks by (non-zero) used-sz - %s", db.buf);

	cf_dyn_buf_free(&db);
	linear_hist_destroy(h);
}

void
as_storage_histogram_clear_mem(as_namespace* ns)
{
	drv_mems* mems = (drv_mems*)ns->storage_private;
	histogram_scale scale = as_config_histogram_scale();

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		if (mem->hist_shadow_write) {
			histogram_rescale(mem->hist_shadow_write, scale);
		}
	}
}


//==========================================================
// Local helpers - startup control.
//

static void
init_shadow_devices(as_namespace* ns)
{
	size_t mems_size = sizeof(drv_mems) +
			(ns->n_storage_shadows * sizeof(drv_mem));
	drv_mems* mems = cf_malloc(mems_size);

	ns->storage_private = (void*)mems;

	memset(mems, 0, mems_size);
	mems->n_mems = (int)ns->n_storage_shadows;
	mems->ns = ns;

	size_t min_file_size = UINT64_MAX;

	for (uint32_t i = 0; i < ns->n_storage_shadows; i++) {
		drv_mem *mem = &mems->mems[i];

		mem->shadow_name = ns->storage_shadows[i];

		// Note - can't configure commit-to-device and disable-odsync.
		mem->open_flag = O_RDWR | O_DIRECT |
				(ns->storage_disable_odsync ? 0 : O_DSYNC);

		int fd = open(mem->shadow_name, mem->open_flag);

		if (fd == -1) {
			cf_crash(AS_DRV_MEM, "unable to open device %s: %s",
					mem->shadow_name, cf_strerror(errno));
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		mem->file_size = check_file_size(size, "usable device");

		if (mem->file_size < min_file_size) {
			min_file_size = mem->file_size;
		}

		mem->io_min_size = find_io_min_size(fd, mem->shadow_name);

		close(fd);

		cf_info(AS_DRV_MEM, "opened device %s: usable size %lu, io-min-size %lu",
				mem->shadow_name, mem->file_size, mem->io_min_size);
	}

	if (ns->stripes != NULL) {
		if (min_file_size < ns->stripe_sz) {
			cf_crash(AS_DRV_MEM, "devices are smaller than memory stripes");
		}

		if (min_file_size > ns->stripe_sz) {
			cf_warning(AS_DRV_MEM, "trimming minimum size to memory size %lu",
					ns->stripe_sz);

			min_file_size = ns->stripe_sz;
		}
	}

	for (uint32_t i = 0; i < ns->n_storage_shadows; i++) {
		drv_mem *mem = &mems->mems[i];

		if (mem->file_size > min_file_size) {
			cf_warning(AS_DRV_MEM, "device %s: trimming to minimum size %lu",
					mem->shadow_name, min_file_size);

			mem->file_size = min_file_size;
		}
	}

	ns->drives_size = min_file_size * ns->n_storage_shadows;
}

static void
init_shadow_files(as_namespace* ns)
{
	size_t mems_size = sizeof(drv_mems) +
			(ns->n_storage_shadows * sizeof(drv_mem));
	drv_mems* mems = cf_malloc(mems_size);

	ns->storage_private = (void*)mems;

	memset(mems, 0, mems_size);
	mems->n_mems = (int)ns->n_storage_shadows;
	mems->ns = ns;

	size_t rounded_file_size = check_file_size(ns->storage_filesize, "file");

	if (ns->stripes != NULL) {
		if (rounded_file_size < ns->stripe_sz) {
			cf_crash(AS_DRV_MEM, "files are smaller than memory stripes");
		}

		if (rounded_file_size > ns->stripe_sz) {
			cf_warning(AS_DRV_MEM, "trimming file size to memory size %lu",
					ns->stripe_sz);

			rounded_file_size = ns->stripe_sz;
		}
	}

	for (uint32_t i = 0; i < ns->n_storage_shadows; i++) {
		drv_mem* mem = &mems->mems[i];

		mem->shadow_name = ns->storage_shadows[i];

		// Note - can't configure commit-to-device and disable-odsync.
		uint32_t direct_flags =
				O_DIRECT | (ns->storage_disable_odsync ? 0 : O_DSYNC);

		mem->open_flag = O_RDWR |
				(ns->storage_commit_to_device || ns->storage_direct_files ?
						direct_flags : 0);

		// Validate that file can be opened, create it if it doesn't exist.
		int fd = open(mem->shadow_name, mem->open_flag | O_CREAT,
				cf_os_base_perms());

		if (fd == -1) {
			cf_crash(AS_DRV_MEM, "unable to open file %s: %s", mem->shadow_name,
					cf_strerror(errno));
		}

		mem->file_size = rounded_file_size;
		mem->io_min_size = LO_IO_MIN_SIZE;

		// Truncate will grow or shrink the file to the correct size.
		if (ftruncate(fd, (off_t)mem->file_size) != 0) {
			cf_crash(AS_DRV_MEM, "unable to truncate file: errno %d", errno);
		}

		close(fd);

		cf_info(AS_DRV_MEM, "opened file %s: usable size %lu", mem->shadow_name,
				mem->file_size);
	}

	ns->drives_size = rounded_file_size * ns->n_storage_shadows;
}

static void
init_memory_only(as_namespace* ns)
{
	uint32_t n_stripes = (uint32_t)
			((ns->storage_data_size + MAX_STRIPE_SZ - 1) / MAX_STRIPE_SZ);

	if (n_stripes < MIN_N_STRIPES) {
		n_stripes = MIN_N_STRIPES;
	}

	size_t stripe_sz = ns->storage_data_size / n_stripes;
	size_t unusable_size = (stripe_sz - DRV_HEADER_SIZE) % WBLOCK_SZ;

	if (unusable_size != 0) {
		cf_info(AS_DRV_MEM, "memory stripe size must be header size %u + multiple of %u, rounding down",
				DRV_HEADER_SIZE, WBLOCK_SZ);
		stripe_sz -= unusable_size;
	}

	if (ns->stripes != NULL && stripe_sz != ns->stripe_sz) {
		cf_crash(AS_DRV_MEM, "existing memory stripe size differs from config");
	}

	size_t mems_size = sizeof(drv_mems) + (n_stripes * sizeof(drv_mem));
	drv_mems* mems = cf_malloc(mems_size);

	ns->storage_private = (void*)mems;

	memset(mems, 0, mems_size);
	mems->n_mems = (int)n_stripes;
	mems->ns = ns;

	for (uint32_t i = 0; i < n_stripes; i++) {
		drv_mem* mem = &mems->mems[i];

		mem->file_size = stripe_sz;
	}

	ns->drives_size = stripe_sz * n_stripes;
}

static uint64_t
check_file_size(uint64_t file_size, const char* tag)
{
	cf_assert(sizeof(off_t) > 4, AS_DRV_MEM, "this OS supports only 32-bit (4g) files - compile with 64 bit offsets");

	if (file_size > DRV_HEADER_SIZE) {
		off_t unusable_size = (file_size - DRV_HEADER_SIZE) % WBLOCK_SZ;

		if (unusable_size != 0) {
			cf_info(AS_DRV_MEM, "%s size must be header size %u + multiple of %u, rounding down",
					tag, DRV_HEADER_SIZE, WBLOCK_SZ);
			file_size -= unusable_size;
		}

		if (file_size > AS_STORAGE_MAX_DEVICE_SIZE) {
			cf_warning(AS_DRV_MEM, "%s size must be <= %ld, trimming original size %ld",
					tag, AS_STORAGE_MAX_DEVICE_SIZE, file_size);
			file_size = AS_STORAGE_MAX_DEVICE_SIZE;
		}
	}

	if (file_size <= DRV_HEADER_SIZE) {
		cf_crash(AS_DRV_MEM, "%s size %ld must be greater than header size %d",
				tag, file_size, DRV_HEADER_SIZE);
	}

	return file_size;
}

static uint64_t
find_io_min_size(int fd, const char* shadow_name)
{
	uint8_t *buf = cf_valloc(HI_IO_MIN_SIZE);
	size_t read_sz = LO_IO_MIN_SIZE;

	while (read_sz <= HI_IO_MIN_SIZE) {
		if (pread_all(fd, (void*)buf, read_sz, 0)) {
			cf_free(buf);
			return read_sz;
		}

		read_sz <<= 1; // LO_IO_MIN_SIZE and HI_IO_MIN_SIZE are powers of 2
	}

	cf_crash(AS_DRV_MEM, "%s: read failed at all sizes from %u to %u bytes",
			shadow_name, LO_IO_MIN_SIZE, HI_IO_MIN_SIZE);

	return 0;
}

static void
init_synchronous(drv_mems* mems)
{
	uint64_t random = 0;

	while (random == 0) {
		random = cf_get_rand64();
	}

	int n_mems = mems->n_mems;
	as_namespace* ns = mems->ns;

	drv_header* headers[n_mems];
	int first_used = -1;

	// Check all the headers. Pick one as the representative.
	for (int i = 0; i < n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		headers[i] = read_header(mem);

		if (! headers[i]) {
			headers[i] = init_header(ns, mem);
		}
		else if (first_used < 0) {
			first_used = i;
		}

		// Note - here so we have stripe names for log (set in read_header()).
		wblock_init(mem);
	}

	cleanup_unmatched_stripes(ns);

	clear_encryption_keys(ns);

	if (first_used < 0) {
		// Shouldn't find all fresh headers here during warm restart.
		if (! ns->cold_start) {
			// There's no going back to cold start now - do so the harsh way.
			cf_crash(AS_DRV_MEM, "{%s} found all %d devices fresh during warm restart",
					ns->name, n_mems);
		}

		cf_info(AS_DRV_MEM, "{%s} found all %d devices fresh, initializing to random %lu",
				ns->name, n_mems, random);

		mems->generic = cf_valloc(ROUND_UP_GENERIC);
		memcpy(mems->generic, &headers[0]->generic, ROUND_UP_GENERIC);

		mems->generic->prefix.n_devices = n_mems;
		mems->generic->prefix.random = random;

		for (int i = 0; i < n_mems; i++) {
			headers[i]->unique.device_id = (uint32_t)i;
		}

		drv_adjust_sc_version_flags(ns, mems->generic->pmeta, true, false);

		flush_header(mems, headers);

		for (int i = 0; i < n_mems; i++) {
			cf_free(headers[i]);
		}

		as_truncate_list_cenotaphs(ns); // all will show as cenotaph
		as_truncate_done_startup(ns);

		mems->all_fresh = true; // won't need to scan devices

		return;
	}

	// At least one device is not fresh. Check that all non-fresh devices match.

	uint32_t n_fresh_drives = 0;
	bool non_commit_drive = false;
	drv_prefix* prefix_first = &headers[first_used]->generic.prefix;

	memset(mems->device_translation, -1, sizeof(mems->device_translation));

	for (int i = 0; i < n_mems; i++) {
		drv_mem* mem = &mems->mems[i];
		drv_prefix* prefix_i = &headers[i]->generic.prefix;
		uint32_t old_device_id = headers[i]->unique.device_id;

		headers[i]->unique.device_id = (uint32_t)i;

		// Skip fresh devices.
		if (prefix_i->random == 0) {
			cf_info(AS_DRV_MEM, "{%s} device %s is empty", ns->name,
					mem->name);
			n_fresh_drives++;
			continue;
		}

		init_pristine_wblock_id(mem, headers[i]->unique.pristine_offset);

		mems->device_translation[old_device_id] = (int8_t)i;

		if (prefix_first->random != prefix_i->random) {
			cf_crash(AS_DRV_MEM, "{%s} drive set with unmatched headers - devices %s & %s have different signatures",
					ns->name, mems->mems[first_used].name, mem->name);
		}

		if (prefix_first->n_devices != prefix_i->n_devices) {
			cf_crash(AS_DRV_MEM, "{%s} drive set with unmatched headers - devices %s & %s have different device counts",
					ns->name, mems->mems[first_used].name, mem->name);
		}

		if ((prefix_i->flags & DRV_HEADER_FLAG_TRUSTED) == 0) {
			cf_info(AS_DRV_MEM, "{%s} device %s prior shutdown not clean",
					ns->name, mem->name);
			ns->dirty_restart = true;
		}

		if ((prefix_i->flags & DRV_HEADER_FLAG_COMMIT_TO_DEVICE) == 0) {
			non_commit_drive = true;
		}
	}

	// Drive set OK - fix up header set.
	mems->generic = cf_valloc(ROUND_UP_GENERIC);
	memcpy(mems->generic, &headers[first_used]->generic, ROUND_UP_GENERIC);

	mems->generic->prefix.n_devices = n_mems; // may have added/removed drives
	mems->generic->prefix.random = random;
	mems->generic->prefix.flags &= ~DRV_HEADER_FLAG_TRUSTED;

	flush_flags(mems);

	drv_adjust_sc_version_flags(ns, mems->generic->pmeta,
			n_mems < prefix_first->n_devices + n_fresh_drives,
			ns->dirty_restart && non_commit_drive);

	flush_header(mems, headers);
	flush_final_cfg(ns);

	for (int i = 0; i < n_mems; i++) {
		cf_free(headers[i]);
	}

	uint32_t now = as_record_void_time_get();

	// Sanity check void-times during startup.
	ns->startup_max_void_time = now + MAX_ALLOWED_TTL;

	// Cache booleans indicating whether partitions are owned or not. Also
	// restore tree-ids - note that absent partitions do have tree-ids.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		drv_pmeta* pmeta = &mems->generic->pmeta[pid];

		mems->get_state_from_storage[pid] =
				as_partition_version_has_data(&pmeta->version);
		ns->partitions[pid].tree_id = pmeta->tree_id;
	}

	// Warm restart.
	if (! ns->cold_start) {
		as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap
		resume_devices(mems);

		return; // warm restart is done
	}

	// Cold start - we can now create our partition trees.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		if (mems->get_state_from_storage[pid]) {
			as_partition* p = &ns->partitions[pid];

			p->tree = as_index_tree_create(&ns->tree_shared, p->tree_id,
					as_partition_tree_done, (void*)p);

			as_set_index_create_all(ns, p->tree);
		}
	}

	// Initialize the cold start expiration and eviction machinery.
	cf_mutex_init(&ns->cold_start_evict_lock);
	ns->cold_start_now = now;
}

static drv_header*
read_header(drv_mem* mem)
{
	as_namespace* ns = mem->ns;

	drv_header* header = cf_malloc(sizeof(drv_header));
	drv_header* shadow_header = NULL;

	if (mem->shadow_name != NULL) {
		size_t read_size = BYTES_UP_TO_IO_MIN(mem, sizeof(drv_header));

		shadow_header = cf_valloc(read_size);

		int fd = shadow_fd_get(mem);

		if (! pread_all(fd, (void*)shadow_header, read_size, 0)) {
			cf_crash(AS_DRV_MEM, "%s: read failed: errno %d (%s)",
					mem->shadow_name, errno, cf_strerror(errno));
		}

		shadow_fd_put(mem, fd);
	}

	resume_or_create_stripe(mem, shadow_header); // also fills mem->name

	ns->storage_devices[ns->n_storage_stripes++] = mem->name;

	bool cold_start_shadow = ns->cold_start && mem->shadow_name != NULL;
	const char* drv_name;

	if (cold_start_shadow) {
		drv_name = mem->shadow_name;
		memcpy(header, shadow_header, sizeof(drv_header));
	}
	else {
		drv_name = mem->name;
		memcpy(header, mem->mem_base_addr, sizeof(drv_header));
	}

	if (shadow_header != NULL) {
		cf_free(shadow_header);
	}

	drv_prefix* prefix = &header->generic.prefix;

	// Normal path for a fresh drive.
	if (prefix->magic != DRV_HEADER_MAGIC) {
		if (! cf_memeq(header, 0, sizeof(drv_header))) {
			cf_crash(AS_DRV_MEM, "%s: not an Aerospike device but not erased - check config or erase device",
					drv_name);
		}

		cf_detail(AS_DRV_MEM, "%s: zero magic - fresh device", drv_name);
		cf_free(header);
		return NULL;
	}

	if (prefix->version != DRV_VERSION) {
		if (prefix->version == 3) { // mem never had 2 or 1
			cf_crash(AS_DRV_MEM, "%s: Aerospike device has old format - must erase device to upgrade",
					drv_name);
		}

		cf_crash(AS_DRV_MEM, "%s: unknown version %u", drv_name,
				prefix->version);
	}

	if (strcmp(prefix->namespace, ns->name) != 0) {
		cf_crash(AS_DRV_MEM, "%s: previous namespace %s now %s - check config or erase device",
				drv_name, prefix->namespace, ns->name);
	}

	if (prefix->n_devices > AS_STORAGE_MAX_DEVICES) {
		cf_crash(AS_DRV_MEM, "%s: bad n-devices %u", drv_name,
				prefix->n_devices);
	}

	if (prefix->random == 0) {
		cf_crash(AS_DRV_MEM, "%s: random signature is 0", drv_name);
	}

	if (prefix->write_block_size == 0 ||
			WBLOCK_SZ % prefix->write_block_size != 0) {
		cf_crash(AS_DRV_MEM, "%s: can't change write-block-size from %u to %u",
				drv_name, prefix->write_block_size, WBLOCK_SZ);
	}

	if (header->unique.device_id >= AS_STORAGE_MAX_DEVICES) {
		cf_crash(AS_DRV_MEM, "%s: bad device-id %u", drv_name,
				header->unique.device_id);
	}

	// If shadow shut down cleanly, cold start off mem if it's there.
	// Note - before header_validate_cfg() which might write to header.
	if (cold_start_shadow && (prefix->flags & DRV_HEADER_FLAG_TRUSTED) != 0) {
		// Note - we save pristine_offset to shadow for storage-engine memory -
		// no need to tamper with it here.

		// If the mem header matches, we can use it for cold start.
		if (memcmp(mem->mem_base_addr, header, sizeof(drv_header)) == 0) {
			mem->cold_start_local = true;
		}
	}

	header_validate_cfg(ns, mem, header);

	if (header->unique.pristine_offset != 0 &&
			(header->unique.pristine_offset < DRV_HEADER_SIZE ||
					header->unique.pristine_offset > mem->file_size)) {
		cf_crash(AS_DRV_MEM, "%s: bad pristine offset %lu", drv_name,
				header->unique.pristine_offset);
	}

	prefix->write_block_size = WBLOCK_SZ;

	return header;
}

static drv_header*
init_header(as_namespace *ns, drv_mem *mem)
{
	drv_header* header = cf_malloc(sizeof(drv_header));

	memset(header, 0, sizeof(drv_header));

	drv_prefix* prefix = &header->generic.prefix;

	// Set non-zero common fields.
	prefix->magic = DRV_HEADER_MAGIC;
	prefix->version = DRV_VERSION;
	strcpy(prefix->namespace, ns->name);
	prefix->write_block_size = WBLOCK_SZ;

	header_init_cfg(ns, mem, header);

	return header;
}

static void
wblock_init(drv_mem* mem)
{
	uint32_t n_wblocks = (uint32_t)(mem->file_size / WBLOCK_SZ);

	cf_info(AS_DRV_MEM, "%s has %u wblocks of size %u", mem->name, n_wblocks,
			WBLOCK_SZ);

	mem->n_wblocks = n_wblocks;
	mem->wblock_state = cf_malloc(n_wblocks * sizeof(mem_wblock_state));

	// Device header wblocks' inuse_sz will (also) be 0 but that doesn't matter.
	for (uint32_t i = 0; i < n_wblocks; i++) {
		mem_wblock_state* p_wblock_state = &mem->wblock_state[i];

		p_wblock_state->inuse_sz = 0;
		cf_mutex_init(&p_wblock_state->LOCK);
		p_wblock_state->mwb = NULL;
		p_wblock_state->state = WBLOCK_STATE_FREE;
		p_wblock_state->short_lived = false;
		p_wblock_state->n_vac_dests = 0;
	}
}

static void
flush_header(drv_mems* mems, drv_header** headers)
{
	uint8_t* buf = cf_valloc(DRV_HEADER_SIZE);

	memset(buf, 0, DRV_HEADER_SIZE);
	memcpy(buf, mems->generic, sizeof(drv_generic));

	for (int i = 0; i < mems->n_mems; i++) {
		memcpy(buf + DRV_OFFSET_UNIQUE, &headers[i]->unique,
				sizeof(drv_unique));

		write_header(&mems->mems[i], buf, buf, DRV_HEADER_SIZE);
	}

	cf_free(buf);
}

// Not called for fresh devices, but called in all (warm/cold) starts.
static void
init_pristine_wblock_id(drv_mem* mem, uint64_t offset)
{
	if (offset == 0) {
		// Legacy device with data - flag to scan and find id on warm restart.
		mem->pristine_wblock_id = 0;
		return;
	}

	// Round up, in case write-block-size was increased.
	mem->pristine_wblock_id = (offset + (WBLOCK_SZ - 1)) / WBLOCK_SZ;
}

static void
start_loading_records(drv_mems* mems, cf_queue* complete_q)
{
	as_namespace* ns = mems->ns;

	drv_mrt_create_cold_start_hash(ns);

	ns->loading_records = true;

	void* p = cf_rc_alloc(1);

	for (int i = 1; i < mems->n_mems; i++) {
		cf_rc_reserve(p);
	}

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];
		mem_load_records_info* lri = cf_malloc(sizeof(mem_load_records_info));

		lri->mems = mems;
		lri->mem = mem;
		lri->complete_q = complete_q;
		lri->complete_rc = p;

		cf_thread_create_transient(run_mem_cold_start, (void*)lri);
	}
}

static void
load_wblock_queues(drv_mems* mems)
{
	cf_info(AS_DRV_MEM, "{%s} loading free & defrag queues", mems->ns->name);

	// Split this task across multiple threads.
	cf_tid tids[mems->n_mems];

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		tids[i] = cf_thread_create_joinable(run_load_queues, (void*)mem);
	}

	for (int i = 0; i < mems->n_mems; i++) {
		cf_thread_join(tids[i]);
	}
	// Now we're single-threaded again.

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		cf_info(AS_DRV_MEM, "%s init wblocks: pristine-id %u pristine %u free-q %u, defrag-q %u",
				mem->name,
				mem->pristine_wblock_id, num_pristine_wblocks(mem),
				cf_queue_sz(mem->free_wblock_q),
				cf_queue_sz(mem->defrag_wblock_q));
	}
}

// Thread "run" function to create and load a device's (wblock) free & defrag
// queues at startup. Sorts defrag-eligible wblocks so the most depleted ones
// are at the head of the defrag queue.
static void*
run_load_queues(void* pv_data)
{
	drv_mem* mem = (drv_mem*)pv_data;

	mem->free_wblock_q = cf_queue_create(sizeof(uint32_t), true);
	mem->defrag_wblock_q = cf_queue_create(sizeof(uint32_t), true);

	as_namespace* ns = mem->ns;
	uint32_t lwm_pct = ns->storage_defrag_lwm_pct;
	uint32_t lwm_size = ns->defrag_lwm_size;
	defrag_pen pens[lwm_pct];

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_init(&pens[n]);
	}

	uint32_t first_id = mem->first_wblock_id;
	uint32_t end_id = mem->pristine_wblock_id;

	// TODO - paranoia - remove eventually.
	cf_assert(end_id >= first_id && end_id <= mem->n_wblocks, AS_DRV_MEM,
			"%s bad pristine-wblock-id %u", mem->name, end_id);

	for (uint32_t wblock_id = first_id; wblock_id < end_id; wblock_id++) {
		uint32_t inuse_sz = mem->wblock_state[wblock_id].inuse_sz;

		if (inuse_sz == 0) {
			// Cold start may produce an empty short-lived wblock.
			mem->wblock_state[wblock_id].short_lived = false;

			// Faster than using push_wblock_to_free_q() here...
			cf_queue_push(mem->free_wblock_q, &wblock_id);
		}
		else if (inuse_sz < lwm_size &&
				! mem->wblock_state[wblock_id].short_lived) {
			defrag_pen_add(&pens[(inuse_sz * lwm_pct) / lwm_size], wblock_id);
		}
		else {
			mem->wblock_state[wblock_id].state = WBLOCK_STATE_USED;
		}
	}

	defrag_pens_dump(pens, lwm_pct, mem->name);

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_transfer(&pens[n], mem);
		defrag_pen_destroy(&pens[n]);
	}

	mem->n_defrag_wblock_reads = (uint64_t)cf_queue_sz(mem->defrag_wblock_q);

	return NULL;
}

static void
defrag_pen_init(defrag_pen* pen)
{
	pen->n_ids = 0;
	pen->capacity = DEFRAG_PEN_INIT_CAPACITY;
	pen->ids = pen->stack_ids;
}

static void
defrag_pen_destroy(defrag_pen* pen)
{
	if (pen->ids != pen->stack_ids) {
		cf_free(pen->ids);
	}
}

static void
defrag_pen_add(defrag_pen* pen, uint32_t wblock_id)
{
	if (pen->n_ids == pen->capacity) {
		if (pen->capacity == DEFRAG_PEN_INIT_CAPACITY) {
			pen->capacity <<= 2;
			pen->ids = cf_malloc(pen->capacity * sizeof(uint32_t));
			memcpy(pen->ids, pen->stack_ids, sizeof(pen->stack_ids));
		}
		else {
			pen->capacity <<= 1;
			pen->ids = cf_realloc(pen->ids, pen->capacity * sizeof(uint32_t));
		}
	}

	pen->ids[pen->n_ids++] = wblock_id;
}

static void
defrag_pen_transfer(defrag_pen* pen, drv_mem* mem)
{
	// For speed, "customize" instead of using push_wblock_to_defrag_q()...
	for (uint32_t i = 0; i < pen->n_ids; i++) {
		uint32_t wblock_id = pen->ids[i];

		mem->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(mem->defrag_wblock_q, &wblock_id);
	}
}

static void
defrag_pens_dump(defrag_pen pens[], uint32_t n_pens, const char* mem_name)
{
	char buf[2048];
	uint32_t n = 0;
	int pos = sprintf(buf, "%u", pens[n++].n_ids);

	while (n < n_pens) {
		pos += sprintf(buf + pos, ",%u", pens[n++].n_ids);
	}

	cf_info(AS_DRV_MEM, "%s init defrag profile: %s", mem_name, buf);
}

static void
start_maintenance_threads(drv_mems* mems)
{
	cf_info(AS_DRV_MEM, "{%s} starting device maintenance threads",
			mems->ns->name);

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		cf_thread_create_detached(run_mem_maintenance, (void*)mem);
	}
}

static void
start_write_threads(drv_mems* mems)
{
	cf_info(AS_DRV_MEM, "{%s} starting write threads", mems->ns->name);

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		if (mem->shadow_name != NULL) {
			mem->shadow_tid =
					cf_thread_create_joinable(run_shadow, (void*)mem);
		}
	}
}

static void
start_defrag_threads(drv_mems* mems)
{
	cf_info(AS_DRV_MEM, "{%s} starting defrag threads", mems->ns->name);

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		cf_thread_create_detached(run_defrag, (void*)mem);
	}
}


//==========================================================
// Local helpers - cold start.
//

static void*
run_mem_cold_start(void* udata)
{
	mem_load_records_info* lri = (mem_load_records_info*)udata;
	drv_mem* mem = lri->mem;
	drv_mems* mems = lri->mems;
	cf_queue* complete_q = lri->complete_q;
	void* complete_rc = lri->complete_rc;

	cf_free(lri);

	cold_start_sweep_device(mems, mem);

	if (cf_rc_release(complete_rc) == 0) {
		// All drives are done reading.

		as_namespace* ns = mems->ns;

		if (drv_cold_start_sweeps_done(ns)) {
			ns->loading_records = false;
			cold_start_drop_cenotaphs(ns);

			cf_mutex_destroy(&ns->cold_start_evict_lock);

			as_truncate_list_cenotaphs(ns);
			as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap

			cold_start_set_unrepl_stat(ns);
		}

		void* _t = NULL;

		cf_queue_push(complete_q, &_t);
		cf_rc_free(complete_rc);
	}

	return NULL;
}

// Not static - called by split function.
void
cold_start_sweep(drv_mems* mems, drv_mem* mem)
{
	if (mem->shadow_name != NULL && ! mem->cold_start_local) {
		cf_info(AS_DRV_MEM, "device %s: reading backing device %s to load index",
				mem->name, mem->shadow_name);
	}
	else {
		cf_info(AS_DRV_MEM, "device %s: reading device to load index",
				mem->name);
	}

	bool read_shadow = mem->shadow_name != NULL && ! mem->cold_start_local;
	int fd = read_shadow ? shadow_fd_get(mem) : -1;

	// Loop over all wblocks, unless we encounter 10 contiguous unused wblocks.

	mem->sweep_wblock_id = mem->first_wblock_id;

	uint64_t file_offset = DRV_HEADER_SIZE;
	uint32_t n_unused_wblocks = 0;

	while (file_offset < mem->file_size && n_unused_wblocks < 10) {
		uint8_t* buf = mem->mem_base_addr + file_offset;

		if (read_shadow) {
			mem_mprotect(buf, WBLOCK_SZ, PROT_READ | PROT_WRITE);

			if (! pread_all(fd, buf, WBLOCK_SZ, (off_t)file_offset)) {
				cf_crash(AS_DRV_MEM, "%s: read failed: errno %d (%s)",
						mem->shadow_name, errno, cf_strerror(errno));
			}
		}

		uint32_t indent = 0; // current offset within wblock, in bytes

		while (indent < WBLOCK_SZ) {
			as_flat_record* flat = (as_flat_record*)&buf[indent];

			if (read_shadow) {
				decrypt_record(mem, file_offset + indent, flat);
			}

			// Look for record magic.
			if (flat->magic != AS_FLAT_MAGIC) {
				// Should always find a record at beginning of used wblock. if
				// not, we've likely encountered the unused part of the device.
				if (indent == 0) {
					n_unused_wblocks++;
					break; // try next wblock
				}
				// else - keep looking for magic - necessary for
				// commit-to-device without shadow, or increased
				// write-block-size (switching from storage-engine device).

				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			if (n_unused_wblocks != 0) {
				cf_warning(AS_DRV_MEM, "%s: found used wblock after skipping %u unused",
						mem->name, n_unused_wblocks);

				n_unused_wblocks = 0; // restart contiguous count
			}

			uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
			uint32_t next_indent = indent + record_size;

			if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
				cf_warning(AS_DRV_MEM, "%s: bad record size %u", mem->name,
						record_size);
				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			// Found a record - try to add it to the index.
			cold_start_add_record(mems, mem, flat,
					OFFSET_TO_RBLOCK_ID(file_offset + indent), record_size);

			indent = next_indent;
		}

		file_offset += WBLOCK_SZ;
		mem->sweep_wblock_id++;

		if (read_shadow) {
			mem_mprotect(buf, WBLOCK_SZ, PROT_READ);
		}
	}

	mem->pristine_wblock_id = mem->sweep_wblock_id - n_unused_wblocks;

	mem->sweep_wblock_id = (uint32_t)(mem->file_size / WBLOCK_SZ);

	if (read_shadow) {
		shadow_fd_put(mem, fd);
	}

	cf_info(AS_DRV_MEM, "device %s: read complete: UNIQUE %lu (REPLACED %lu) (OLDER %lu) (EXPIRED %lu) (EVICTED %lu) (UNOWNED %lu) (DROPPED %lu) (UNPARSABLE %lu) records",
			mem->name, mem->record_add_unique_counter,
			mem->record_add_replace_counter, mem->record_add_older_counter,
			mem->record_add_expired_counter, mem->record_add_evicted_counter,
			mem->record_add_unowned_counter, mem->record_add_dropped_counter,
			mem->record_add_unparsable_counter);
}

static void
cold_start_add_record(drv_mems* mems, drv_mem* mem,
		const as_flat_record* flat, uint64_t rblock_id, uint32_t record_size)
{
	uint32_t pid = as_partition_getid(&flat->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! mems->get_state_from_storage[pid]) {
		mem->record_add_unowned_counter++;
		return;
	}

	as_namespace* ns = mems->ns;
	as_partition* p_partition = &ns->partitions[pid];

	// Includes round rblock padding, so may not literally exclude the mark.
	const uint8_t* end = (const uint8_t*)flat + record_size - END_MARK_SZ;

	as_flat_opt_meta opt_meta = { { 0 } };

	const uint8_t* p_read = as_flat_unpack_record_meta(flat, end, &opt_meta);

	if (! p_read) {
		cf_warning(AS_DRV_MEM, "bad metadata for %pD", &flat->keyd);
		mem->record_add_unparsable_counter++;
		return;
	}

	if (opt_meta.void_time > ns->startup_max_void_time) {
		cf_warning(AS_DRV_MEM, "bad void-time for %pD", &flat->keyd);
		mem->record_add_unparsable_counter++;
		return;
	}

	const uint8_t* cb_end = NULL;

	if (! as_flat_decompress_buffer(&opt_meta.cm, WBLOCK_SZ, &p_read, &end,
			&cb_end)) {
		cf_warning(AS_DRV_MEM, "bad compressed data for %pD", &flat->keyd);
		mem->record_add_unparsable_counter++;
		return;
	}

	const uint8_t* exact_end = as_flat_check_packed_bins(p_read, end,
			opt_meta.n_bins);

	if (exact_end == NULL) {
		cf_warning(AS_DRV_MEM, "bad flat record %pD", &flat->keyd);
		mem->record_add_unparsable_counter++;
		return;
	}

	if (! drv_check_end_mark(cb_end == NULL ? exact_end : cb_end, flat)) {
		cf_warning(AS_DRV_MEM, "bad end marker for %pD", &flat->keyd);
		mem->record_add_unparsable_counter++;
		return;
	}

	// Ignore record if it was in a dropped tree.
	if (flat->tree_id != p_partition->tree_id) {
		mem->record_add_dropped_counter++;
		return;
	}

	// Ignore records that were truncated.
	if (as_truncate_lut_is_truncated(flat->last_update_time, ns,
			opt_meta.set_name, opt_meta.set_name_len)) {
		return;
	}

	// If eviction is necessary, evict previously added records closest to
	// expiration. (If evicting, this call will block for a long time.) This
	// call may also update the cold start threshold void-time.
	if (! as_cold_start_evict_if_needed(ns)) {
		cf_crash(AS_DRV_MEM, "hit stop-writes limit before drive scan completed");
	}

	// Get/create the record from/in the appropriate index tree.
	as_index_ref r_ref;
	int rv = as_record_get_create(p_partition->tree, &flat->keyd, &r_ref, ns);

	if (rv < 0) {
		cf_crash(AS_DRV_MEM, "{%s} can't add record to index", ns->name);
	}

	bool is_create = rv == 1;

	as_index* r = r_ref.r;

	if (! is_create) {
		// Record already existed. Ignore this one if existing record is newer.
		if (prefer_existing_record(ns, flat, opt_meta.void_time, r)) {
			cold_start_fill_orig(mem, flat, rblock_id, &opt_meta,
					p_partition->tree, &r_ref);
			cold_start_adjust_cenotaph(ns, flat, opt_meta.void_time, r);
			as_record_done(&r_ref, ns);
			mem->record_add_older_counter++;
			return;
		}
	}
	// The record we're now reading is the latest version (so far) ...

	// Skip records that have expired.
	if (opt_meta.void_time != 0 && ns->cold_start_now > opt_meta.void_time) {
		if (! is_create) {
			drv_cold_start_remove_from_set_index(ns, p_partition->tree, &r_ref);
		}

		as_index_delete(p_partition->tree, &flat->keyd);
		as_record_done(&r_ref, ns);
		mem->record_add_expired_counter++;
		return;
	}

	// Skip records that were evicted.
	if (opt_meta.void_time != 0 && ns->evict_void_time > opt_meta.void_time &&
			drv_is_set_evictable(ns, &opt_meta)) {
		if (! is_create) {
			drv_cold_start_remove_from_set_index(ns, p_partition->tree, &r_ref);
		}

		as_index_delete(p_partition->tree, &flat->keyd);
		as_record_done(&r_ref, ns);
		mem->record_add_evicted_counter++;
		return;
	}

	// We'll keep the record we're now reading ...

	if (is_create) {
		// Set record's set-id.
		if (opt_meta.set_name != NULL) {
			as_index_set_set_w_len(r, ns, opt_meta.set_name,
					opt_meta.set_name_len, false);
		}

		drv_cold_start_record_create(ns, flat, &opt_meta, p_partition->tree,
				&r_ref);

		mem->record_add_unique_counter++;
	}
	else {
		cold_start_record_update(mems, flat, &opt_meta, p_partition->tree,
				&r_ref);

		mem->record_add_replace_counter++;
	}

	// Store or drop the key according to the props we read.
	as_record_finalize_key(r, opt_meta.key, opt_meta.key_size);

	// Set/reset the record's last-update-time, generation, and void-time.
	r->last_update_time = flat->last_update_time;
	r->generation = flat->generation;
	r->void_time = opt_meta.void_time;

	// Update maximum void-time.
	as_setmax_uint32(&p_partition->max_void_time, r->void_time);

	// Set/reset the records's replication state and XDR-write status.
	cold_start_init_repl_state(ns, r);
	cold_start_init_xdr_state(flat, r);

	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(rblock_id);

	if (is_mrt_provisional(r) || is_mrt_monitor_write(ns, r)) {
		mem->wblock_state[wblock_id].short_lived = true;
	}

	mem->inuse_size += record_size;
	mem->wblock_state[wblock_id].inuse_sz += record_size;

	// Set/reset the record's storage information.
	r->file_id = mem->file_id;
	r->rblock_id = rblock_id;

	as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
			DELTA_N_RBLOCKS_TO_SIZE(flat->n_rblocks, r->n_rblocks));

	r->n_rblocks = flat->n_rblocks;

	as_record_done(&r_ref, ns);
}

static bool
prefer_existing_record(const as_namespace* ns, const as_flat_record* flat,
		uint32_t block_void_time, const as_index* r)
{
	int result = as_record_resolve_conflict(cold_start_policy(ns),
			r->generation, r->last_update_time,
			flat->generation, flat->last_update_time);

	if (result != 0) {
		return result == -1; // -1 means block record < existing record
	}

	// Finally, compare void-times. Note that defragged records will generate
	// identical copies on drive, so they'll get here and return true.
	return r->void_time == 0 ||
			(block_void_time != 0 && block_void_time <= r->void_time);
}


//==========================================================
// Local helpers - shutdown.
//

static void
set_pristine_offset(drv_mems* mems)
{
	cf_mutex_lock(&mems->flush_lock);

	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];
		drv_header* header = (drv_header*)&mem->mem_base_addr[0];

		header->unique.pristine_offset =
				(uint64_t)mem->pristine_wblock_id * WBLOCK_SZ;

		// Persisted offset is never used at cold start, but will be used on a
		// warm restart after switching from storage-engine memory to device.
		if (mem->shadow_name != NULL) {
			aligned_write_to_shadow(mem, (uint8_t*)header,
					(uint8_t*)&header->unique.pristine_offset,
					sizeof(header->unique.pristine_offset));
		}
	}

	cf_mutex_unlock(&mems->flush_lock);
}

static void
set_trusted(drv_mems* mems)
{
	cf_mutex_lock(&mems->flush_lock);

	mems->generic->prefix.flags |= DRV_HEADER_FLAG_TRUSTED;
	flush_flags(mems);

	cf_mutex_unlock(&mems->flush_lock);
}


//==========================================================
// Local helpers - read record.
//

static int
read_record(as_storage_rd* rd, bool pickle_only)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;
	drv_mem* mem = rd->mem;

	uint64_t record_offset = RBLOCK_ID_TO_OFFSET(r->rblock_id);
	uint32_t record_size = N_RBLOCKS_TO_SIZE(r->n_rblocks);
	uint64_t record_end_offset = record_offset + record_size;

	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(record_offset);

	bool ok = true;

	if (wblock_id >= mem->n_wblocks || wblock_id < mem->first_wblock_id) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD bad offset %lu",
				ns->name, &r->keyd, record_offset);
		ok = false;
	}

	if (record_size < DRV_RECORD_MIN_SIZE) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD bad record size %u",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (record_end_offset > WBLOCK_ID_TO_OFFSET(wblock_id + 1)) {
		cf_warning(AS_DRV_MEM, "{%s} read: digest %pD record size %u crosses wblock boundary",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (! ok) {
		return -1;
	}

	const as_flat_record* flat =
			(const as_flat_record*)(mem->mem_base_addr + record_offset);

	rd->flat = flat;

	as_incr_uint32(&ns->n_reads_from_device);

	if (! sanity_check_flat(mem, r, record_offset, flat)) {
		return -1;
	}

	as_flat_opt_meta opt_meta = { { 0 } };

	// Includes round rblock padding, so may not literally exclude the mark.
	// (Is set exactly to mark below, if skipping or decompressing bins.)
	rd->flat_end = (const uint8_t*)flat + record_size - END_MARK_SZ;

	rd->flat_bins = as_flat_unpack_record_meta(flat, rd->flat_end, &opt_meta);

	if (rd->flat_bins == NULL) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad record metadata",
				ns->name, mem->name, &r->keyd);
		return -1;
	}

	rd->flat_n_bins = (uint16_t)opt_meta.n_bins;

	if (pickle_only) {
		if (! as_flat_skip_bins(&opt_meta.cm, rd)) {
			cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad bin data",
					ns->name, mem->name, &r->keyd);
			return -1;
		}

		return 0;
	}

	if (! as_flat_decompress_bins(&opt_meta.cm, rd)) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad compressed data",
				ns->name, mem->name, &r->keyd);
		return -1;
	}

	if (opt_meta.key != NULL) {
		rd->key_size = opt_meta.key_size;
		rd->key = opt_meta.key;
	}
	// else - if updating record without key, leave rd (msg) key to be stored.

	return 0;
}

static bool
sanity_check_flat(const drv_mem* mem, const as_record* r,
		uint64_t record_offset, const as_flat_record* flat)
{
	as_namespace *ns = mem->ns;
	bool ok = true;

	if (flat->magic != AS_FLAT_MAGIC) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad magic 0x%x offset %lu",
				ns->name, mem->name, &r->keyd, flat->magic, record_offset);
		ok = false;
	}

	if (flat->n_rblocks != r->n_rblocks) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad n-rblocks %u expecting %u",
				ns->name, mem->name, &r->keyd, flat->n_rblocks, r->n_rblocks);
		ok = false;
	}

	if (cf_digest_compare(&flat->keyd, &r->keyd) != 0) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD but found flat digest %pD",
				ns->name, mem->name, &r->keyd, &flat->keyd);
		ok = false;
	}

	if (flat->xdr_write != r->xdr_write) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad xdr-write %u expecting %u",
				ns->name, mem->name, &r->keyd, flat->xdr_write, r->xdr_write);
		ok = false;
	}

	if (flat->tree_id != r->tree_id) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad tree-id %u expecting %u",
				ns->name, mem->name, &r->keyd, flat->tree_id, r->tree_id);
		ok = false;
	}

	if (flat->generation != r->generation) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad generation %u expecting %u",
				ns->name, mem->name, &r->keyd, flat->generation,
				r->generation);
		ok = false;
	}

	if (flat->last_update_time != r->last_update_time) {
		cf_warning(AS_DRV_MEM, "{%s} read %s: digest %pD bad lut %lu expecting %lu",
				ns->name, mem->name, &r->keyd,
				(uint64_t)flat->last_update_time,
				(uint64_t)r->last_update_time);
		ok = false;
	}

	return ok;
}


//==========================================================
// Local helpers - write record.
//

// Not static - called by split function.
int
write_record(as_storage_rd* rd)
{
	as_record* r = rd->r;

	drv_mem* old_mem = NULL;
	uint64_t old_rblock_id = 0;
	uint32_t old_n_rblocks = 0;

	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id)) {
		// Replacing an old record.
		old_mem = rd->mem;
		old_rblock_id = r->rblock_id;
		old_n_rblocks = r->n_rblocks;
	}

	drv_mems* mems = (drv_mems*)rd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	rd->mem = &mems->mems[mem_get_file_id(mems, &r->keyd)];

	cf_assert(rd->mem, AS_DRV_MEM, "{%s} null mem", rd->ns->name);

	int rv = write_bins(rd);

	if (rv == 0 && old_mem) {
		if (! is_first_mrt(rd)) {
			block_free(old_mem, old_rblock_id, old_n_rblocks, "mem-write");
		}
	}

	if (rv == 0) {
		mrt_rd_block_free_orig(mems, rd);
	}

	return rv;
}

// Not static - called by split function.
int
buffer_bins(as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;
	as_record* r = rd->r;
	drv_mem* mem = rd->mem;

	uint32_t flat_sz;
	uint32_t limit_sz;

	if (rd->pickle == NULL) {
		flat_sz = as_flat_record_size(rd);
		limit_sz = drv_max_record_size(ns, r);
	}
	else {
		flat_sz = rd->orig_pickle_sz;
		limit_sz = WBLOCK_SZ;
	}

	uint32_t flat_w_mark_sz = flat_sz + END_MARK_SZ;

	if (flat_w_mark_sz > limit_sz) {
		cf_detail(AS_DRV_MEM, "{%s} write: size %u - rejecting %pD", ns->name,
				flat_w_mark_sz, &r->keyd);
		return -AS_ERR_RECORD_TOO_BIG;
	}

	as_flat_record* flat;

	if (rd->pickle == NULL) {
		flat = as_flat_compress_bins_and_pack_record(rd, WBLOCK_SZ, false, true,
				&flat_w_mark_sz);
		flat_sz = flat_w_mark_sz - END_MARK_SZ;
	}
	else {
		flat = (as_flat_record*)rd->pickle;

		// Limit check used orig size, but from here on use compressed size.
		flat_sz = rd->pickle_sz;
		flat_w_mark_sz = flat_sz + END_MARK_SZ;

		// Tree IDs are node-local - can't use those sent from other nodes.
		flat->tree_id = r->tree_id;
	}

	// Note - this is the only place where rounding size (up to a  multiple of
	// RBLOCK_SIZE) is really necessary.
	uint32_t write_sz = SIZE_UP_TO_RBLOCK_SIZE(flat_w_mark_sz);

	// Reserve the portion of the current mwb where this record will be written.

	current_mwb* cur_mwb = &mem->current_mwbs[rd->which_current_swb];

	cf_mutex_lock(&cur_mwb->lock);

	mem_write_block* mwb = cur_mwb->mwb;

	if (! mwb) {
		mwb = mwb_get(mem, false);
		cur_mwb->mwb = mwb;

		if (! mwb) {
			cf_ticker_warning(AS_DRV_MEM, "{%s} out of space", ns->name);
			cf_mutex_unlock(&cur_mwb->lock);
			return -AS_ERR_OUT_OF_SPACE;
		}

		set_wblock_flags(rd, mwb);

		if (mem->shadow_name == NULL) {
			prepare_for_first_write(mwb);
		}
	}

	mem_write_block* old_mwb = NULL;

	// Check if there's enough space in current buffer - if not, enqueue it to
	// be flushed to device, and grab a new buffer.
	if (write_sz > WBLOCK_SZ - mwb->pos) {
		if (mem->shadow_name != NULL) {
			// Enqueue the buffer, to be flushed to device.
			push_wblock_to_shadow_q(mem, mwb);
		}
		else {
			old_mwb = mwb; // stash for release outside lock
		}

		cur_mwb->n_wblock_writes++;

		// Get the new buffer.
		mwb = mwb_get(mem, false);
		cur_mwb->mwb = mwb;

		if (! mwb) {
			cf_ticker_warning(AS_DRV_MEM, "{%s} out of space", ns->name);
			cf_mutex_unlock(&cur_mwb->lock);

			// Outside lock to not block other threads trying to write to new mwb.
			release_old_mwb(mem, old_mwb);

			return -AS_ERR_OUT_OF_SPACE;
		}

		set_wblock_flags(rd, mwb);

		if (mem->shadow_name == NULL) {
			prepare_for_first_write(mwb);
		}
	}

	// There's enough space - save the position where this record will be
	// written, and advance mwb->pos for the next writer.

	uint32_t mwb_pos = mwb->pos;

	mwb->pos += write_sz;

	as_incr_uint32(&mwb->n_writers);

	cf_mutex_unlock(&cur_mwb->lock);
	// May now write this record concurrently with others in this mwb.

	// Flatten data into the block.

	uint32_t n_rblocks = ROUNDED_SIZE_TO_N_RBLOCKS(write_sz);

	if (rd->pickle != NULL) {
		flat->n_rblocks = n_rblocks;
	}

	as_flat_record* flat_in_mwb = (as_flat_record*)&mwb->base_addr[mwb_pos];

	if (flat == NULL) {
		as_flat_pack_record(rd, n_rblocks, false, flat_in_mwb);
	}
	else {
		memcpy(flat_in_mwb, flat, flat_sz);
	}

	drv_add_end_mark((uint8_t*)flat_in_mwb + flat_sz, flat_in_mwb);

	// Make a pickle if needed.
	if (rd->keep_pickle) {
		rd->pickle_sz = flat_sz;
		rd->pickle = cf_malloc(flat_sz);
		memcpy(rd->pickle, flat_in_mwb, flat_sz);

		if (write_sz - flat_sz >= RBLOCK_SIZE) {
			((as_flat_record*)rd->pickle)->n_rblocks--;
		}
	}

	uint64_t write_offset = WBLOCK_ID_TO_OFFSET(mwb->wblock_id) + mwb_pos;

	r->file_id = mem->file_id;
	r->rblock_id = OFFSET_TO_RBLOCK_ID(write_offset);

	as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
			DELTA_N_RBLOCKS_TO_SIZE(n_rblocks, r->n_rblocks));

	r->n_rblocks = n_rblocks;

	as_add_uint64(&mem->inuse_size, (int64_t)write_sz);
	as_add_uint32(&mem->wblock_state[mwb->wblock_id].inuse_sz,
			(int32_t)write_sz);

	// We are finished writing to the buffer.
	as_decr_uint32_rls(&mwb->n_writers);

	if (ns->storage_benchmarks_enabled) {
		histogram_insert_raw(ns->device_write_size_hist, write_sz);
	}

	// Outside lock to not block other threads trying to write to new mwb.
	release_old_mwb(mem, old_mwb);

	return 0;
}


//==========================================================
// Local helpers - write and recycle wblocks.
//

static void*
run_shadow(void* arg)
{
	drv_mem* mem = (drv_mem*)arg;

	while (mem->running_shadow || cf_queue_sz(mem->mwb_shadow_q) != 0) {
		mem_write_block* mwb;

		if (CF_QUEUE_OK != cf_queue_pop(mem->mwb_shadow_q, &mwb, 100)) {
			continue;
		}

		// Sanity checks (optional).
		write_sanity_checks(mem, mwb);

		// Flush to the shadow device.
		shadow_flush_mwb(mem, mwb);

		// If this mwb was a defrag destination, release the sources.
		mwb_release_all_vacated_wblocks(mwb);

		mwb_release(mem, mwb);
		as_decr_uint32(&mem->ns->n_wblocks_to_flush);
	}

	return NULL;
}

static void
write_sanity_checks(drv_mem* mem, mem_write_block* mwb)
{
	mem_wblock_state* p_wblock_state = &mem->wblock_state[mwb->wblock_id];

	cf_assert(p_wblock_state->mwb == mwb, AS_DRV_MEM,
			"device %s: wblock-id %u mwb not consistent while writing",
			mem->name, mwb->wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_RESERVED, AS_DRV_MEM,
			"device %s: wblock-id %u state %u off write-q", mem->name,
			mwb->wblock_id, p_wblock_state->state);
}

// Not static - called by split function.
void
block_free(drv_mem* mem, uint64_t rblock_id, uint32_t n_rblocks, char* msg)
{
	// Determine which wblock we're reducing used size in.
	uint64_t start_offset = RBLOCK_ID_TO_OFFSET(rblock_id);
	uint32_t size = N_RBLOCKS_TO_SIZE(n_rblocks);
	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(start_offset);
	uint32_t end_wblock_id = OFFSET_TO_WBLOCK_ID(start_offset + size - 1);

	cf_assert(size >= DRV_RECORD_MIN_SIZE, AS_DRV_MEM,
			"%s: %s: freeing bad size %u rblock_id %lu", mem->name, msg, size,
			rblock_id);

	cf_assert(start_offset >= DRV_HEADER_SIZE &&
			wblock_id < mem->n_wblocks && wblock_id == end_wblock_id,
			AS_DRV_MEM, "%s: %s: freeing bad range rblock_id %lu n_rblocks %u",
			mem->name, msg, rblock_id, n_rblocks);

	as_add_uint64(&mem->inuse_size, -(int64_t)size);

	mem_wblock_state* p_wblock_state = &mem->wblock_state[wblock_id];

	cf_mutex_lock(&p_wblock_state->LOCK);

	int64_t resulting_inuse_sz =
			(int32_t)as_aaf_uint32(&p_wblock_state->inuse_sz, -(int32_t)size);

	cf_assert(resulting_inuse_sz >= 0 &&
			resulting_inuse_sz < (int64_t)WBLOCK_SZ, AS_DRV_MEM,
			"%s: %s: wblock %d %s, subtracted %d now %ld", mem->name, msg,
			wblock_id, resulting_inuse_sz < 0 ? "over-freed" : "bad inuse_sz",
			(int32_t)size, resulting_inuse_sz);

	if (p_wblock_state->state == WBLOCK_STATE_USED) {
		if (resulting_inuse_sz == 0) {
			p_wblock_state->short_lived = false;

			as_incr_uint64(&mem->n_wblock_direct_frees);
			push_wblock_to_free_q(mem, wblock_id);
		}
		else if (resulting_inuse_sz < mem->ns->defrag_lwm_size) {
			if (! p_wblock_state->short_lived) {
				push_wblock_to_defrag_q(mem, wblock_id);
			}
		}
	}
	else if (p_wblock_state->state == WBLOCK_STATE_EMPTYING) {
		cf_assert(! p_wblock_state->short_lived, AS_DRV_MEM,
				"short-lived wblock in emptying state");

		if (resulting_inuse_sz == 0) {
			push_wblock_to_free_q(mem, wblock_id);
		}
	}

	cf_mutex_unlock(&p_wblock_state->LOCK);
}

static void
push_wblock_to_defrag_q(drv_mem* mem, uint32_t wblock_id)
{
	if (mem->defrag_wblock_q) { // null until devices are loaded at startup
		mem->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(mem->defrag_wblock_q, &wblock_id);
		as_incr_uint64(&mem->n_defrag_wblock_reads);
	}
}

static void
push_wblock_to_free_q(drv_mem* mem, uint32_t wblock_id)
{
	// Can get here before queue created, e.g. cold start replacing records.
	if (mem->free_wblock_q == NULL) {
		return;
	}

	cf_assert(wblock_id < mem->n_wblocks, AS_DRV_MEM,
			"pushing bad wblock_id %d to free_wblock_q", (int32_t)wblock_id);

	mem->wblock_state[wblock_id].state = WBLOCK_STATE_FREE;
	cf_queue_push(mem->free_wblock_q, &wblock_id);
}


//==========================================================
// Local helpers - write to header.
//

// Not static - called by split function.
void
write_header(drv_mem* mem, const uint8_t* header, const uint8_t* from,
		size_t size)
{
	memcpy(&mem->mem_base_addr[from - header], from, size);

	if (mem->shadow_name != NULL) {
		aligned_write_to_shadow(mem, header, from, size);
	}
}

static void
aligned_write_to_shadow(drv_mem* mem, const uint8_t* header,
		const uint8_t* from, size_t size)
{
	off_t offset = from - header;

	off_t flush_offset = BYTES_DOWN_TO_IO_MIN(mem, offset);
	off_t flush_end_offset = BYTES_UP_TO_IO_MIN(mem, offset + size);

	const uint8_t* flush = header + flush_offset;
	size_t flush_sz = flush_end_offset - flush_offset;

	int fd = shadow_fd_get(mem);

	if (! pwrite_all(fd, flush, flush_sz, flush_offset)) {
		cf_crash(AS_DRV_MEM, "%s: DEVICE FAILED write: errno %d (%s)",
				mem->shadow_name, errno, cf_strerror(errno));
	}

	shadow_fd_put(mem, fd);
}

static void
flush_flags(drv_mems* mems)
{
	for (int i = 0; i < mems->n_mems; i++) {
		drv_mem* mem = &mems->mems[i];

		write_header(mem, (uint8_t*)mems->generic,
				(uint8_t*)&mems->generic->prefix.flags,
				sizeof(mems->generic->prefix.flags));
	}
}


//==========================================================
// Local helpers - defrag.
//

static void*
run_defrag(void* pv_data)
{
	drv_mem* mem = (drv_mem*)pv_data;
	as_namespace* ns = mem->ns;
	uint32_t wblock_id;

	while (true) {
		uint32_t q_min = as_load_uint32(&ns->storage_defrag_queue_min);

		if (q_min == 0) {
			cf_queue_pop(mem->defrag_wblock_q, &wblock_id, CF_QUEUE_FOREVER);
		}
		else {
			if (cf_queue_sz(mem->defrag_wblock_q) <= q_min) {
				usleep(1000 * 50);
				continue;
			}

			cf_queue_pop(mem->defrag_wblock_q, &wblock_id, CF_QUEUE_NOWAIT);
		}

		defrag_wblock(mem, wblock_id);

		uint32_t sleep_us = as_load_uint32(&ns->storage_defrag_sleep);

		if (sleep_us != 0) {
			usleep(sleep_us);
		}

		while (ns->n_wblocks_to_flush > ns->storage_max_write_q + 100) {
			usleep(1000);
		}
	}

	return NULL;
}

static int
defrag_wblock(drv_mem* mem, uint32_t wblock_id)
{
	int record_count = 0;

	mem_wblock_state* p_wblock_state = &mem->wblock_state[wblock_id];

	cf_assert(p_wblock_state->n_vac_dests == 0, AS_DRV_MEM,
			"n-vacations not 0 beginning defrag wblock");

	if (mem->shadow_name != NULL) {
		// Make sure this can't decrement to 0 while defragging this wblock.
		p_wblock_state->n_vac_dests = 1;
	}

	if (as_load_uint32(&p_wblock_state->inuse_sz) == 0) {
		as_incr_uint64(&mem->n_wblock_defrag_io_skips);
		goto Finished;
	}

	uint64_t file_offset = WBLOCK_ID_TO_OFFSET(wblock_id);
	const uint8_t* mem_buf = mem->mem_base_addr + file_offset;

	uint32_t indent = 0; // current offset within the wblock, in bytes

	while (indent < WBLOCK_SZ &&
			as_load_uint32(&p_wblock_state->inuse_sz) != 0) {
		const as_flat_record* flat = (const as_flat_record*)&mem_buf[indent];

		if (flat->magic != AS_FLAT_MAGIC) {
			// The first record must have magic.
			if (indent == 0) {
				cf_warning(AS_DRV_MEM, "%s: no magic at beginning of used wblock %d",
						mem->name, wblock_id);
				break;
			}
			// else - keep looking for magic - necessary for commit-to-device
			// without shadow, or increased write-block-size (switching from
			// storage-engine device).

			indent += RBLOCK_SIZE;
			continue;
		}

		uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
		uint32_t next_indent = indent + record_size;

		if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
			cf_warning(AS_DRV_MEM, "%s: bad record size %u", mem->name,
					record_size);
			indent += RBLOCK_SIZE;
			continue; // try next rblock
		}

		// Found a good record, move it if it's current.
		int rv = record_defrag(mem, wblock_id, flat,
				OFFSET_TO_RBLOCK_ID(file_offset + indent));

		if (rv == 0) {
			record_count++;
		}

		indent = next_indent;
	}

Finished:

	// Note - usually wblock's inuse_sz is 0 here, but may legitimately be non-0
	// e.g. if a dropped partition's tree is not done purging. In this case, we
	// may have found deleted records in the wblock whose used-size contribution
	// has not yet been subtracted.

	release_vacated_wblock(mem, wblock_id, p_wblock_state);

	return record_count;
}

static int
record_defrag(drv_mem* mem, uint32_t wblock_id, const as_flat_record* flat,
		uint64_t rblock_id)
{
	as_namespace* ns = mem->ns;
	as_partition_reservation rsv;
	uint32_t pid = as_partition_getid(&flat->keyd);

	as_partition_reserve(ns, pid, &rsv);

	int rv;
	as_index_ref r_ref;
	bool found = 0 == as_record_get(rsv.tree, &flat->keyd, &r_ref);

	if (found) {
		as_index* r = r_ref.r;

		if ((r = drv_current_record(ns, r, mem->file_id, rblock_id)) != NULL) {
			if (r->generation != flat->generation) {
				cf_warning(AS_DRV_MEM, "device %s defrag: rblock_id %lu generation mismatch (%u:%u) %pD",
						mem->name, rblock_id, r->generation, flat->generation,
						&r->keyd);
			}

			if (r->n_rblocks != flat->n_rblocks) {
				cf_warning(AS_DRV_MEM, "device %s defrag: rblock_id %lu n_blocks mismatch (%u:%u) %pD",
						mem->name, rblock_id, r->n_rblocks, flat->n_rblocks,
						&r->keyd);
			}

			defrag_move_record(mem, wblock_id, flat, r);

			rv = 0; // record was in index tree and current - moved it
		}
		else {
			rv = -1; // record was in index tree - presumably was overwritten
		}

		as_record_done(&r_ref, ns);
	}
	else {
		rv = -2; // record was not in index tree - presumably was deleted
	}

	as_partition_release(&rsv);

	return rv;
}

static void
defrag_move_record(drv_mem* src_mem, uint32_t src_wblock_id,
		const as_flat_record* flat, as_index* r)
{
	uint64_t old_rblock_id = r->rblock_id;
	uint32_t old_n_rblocks = r->n_rblocks;

	as_namespace* ns = src_mem->ns;
	drv_mems* mems = (drv_mems*)ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	drv_mem* mem = &mems->mems[mem_get_file_id(mems, &flat->keyd)];

	cf_assert(mem, AS_DRV_MEM, "{%s} null mem", ns->name);

	uint32_t mem_n_rblocks = flat->n_rblocks;
	uint32_t write_size = N_RBLOCKS_TO_SIZE(mem_n_rblocks);

	cf_mutex_lock(&mem->defrag_lock);

	mem_write_block* mwb = mem->defrag_mwb;

	if (! mwb) {
		mwb = mwb_get(mem, true);
		mem->defrag_mwb = mwb;

		if (! mwb) {
			cf_warning(AS_DRV_MEM, "defrag_move_record: couldn't get mwb");
			cf_mutex_unlock(&mem->defrag_lock);
			return;
		}
	}

	// Check if there's enough space in defrag buffer - if not, enqueue it to be
	// flushed to device, and grab a new buffer.
	if (write_size > WBLOCK_SZ - mwb->pos) {
		if (mem->shadow_name != NULL) {
			// Enqueue the buffer, to be flushed to device.
			push_wblock_to_shadow_q(mem, mwb);
		}
		else {
			memset(&mwb->base_addr[mwb->pos], 0, WBLOCK_SZ - mwb->pos);
			mem_mprotect(mwb->base_addr, WBLOCK_SZ, PROT_READ);
			mwb_release(mem, mwb);
		}

		mem->n_defrag_wblock_writes++;

		// Get the new buffer.
		while ((mwb = mwb_get(mem, true)) == NULL) {
			// If we got here, we used all our reserve wblocks, but the wblocks
			// we defragged must still have non-zero inuse_sz. Must wait for
			// those to become free.
			cf_ticker_warning(AS_DRV_MEM, "{%s} defrag: drive %s totally full - waiting for vacated wblocks to be freed",
					mem->ns->name, mem->name);

			usleep(10 * 1000);
		}

		mem->defrag_mwb = mwb;
	}

	memcpy(&mwb->base_addr[mwb->pos], flat, write_size);

	uint64_t write_offset = WBLOCK_ID_TO_OFFSET(mwb->wblock_id) + mwb->pos;

	r->file_id = mem->file_id;
	r->rblock_id = OFFSET_TO_RBLOCK_ID(write_offset);
	r->n_rblocks = mem_n_rblocks;

	mwb->pos += write_size;

	as_add_uint64(&mem->inuse_size, (int64_t)write_size);
	as_add_uint32(&mem->wblock_state[mwb->wblock_id].inuse_sz,
			(int32_t)write_size);

	if (mem->shadow_name != NULL &&
			// If we just defragged into a new destination mwb, count it.
			mwb_add_unique_vacated_wblock(mwb, src_mem->file_id,
					src_wblock_id)) {
		mem_wblock_state* p_wblock_state =
				&src_mem->wblock_state[src_wblock_id];

		as_incr_uint32(&p_wblock_state->n_vac_dests);
	}

	cf_mutex_unlock(&mem->defrag_lock);

	block_free(src_mem, old_rblock_id, old_n_rblocks, "defrag-write");
}

static void
release_vacated_wblock(drv_mem* mem, uint32_t wblock_id,
		mem_wblock_state* p_wblock_state)
{
	cf_assert(p_wblock_state->mwb == NULL, AS_DRV_MEM,
			"device %s: wblock-id %u mwb not null while defragging",
			mem->name, wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_DEFRAG, AS_DRV_MEM,
			"device %s: wblock-id %u state %u releasing vacation destination",
			mem->name, wblock_id, p_wblock_state->state);

	cf_assert(! p_wblock_state->short_lived, AS_DRV_MEM,
			"device %s: wblock-id %u short-lived wblock in defrag state",
			mem->name, wblock_id);

	if (mem->shadow_name != NULL) {
		uint32_t n_vac_dests =
				as_aaf_uint32_rls(&p_wblock_state->n_vac_dests, -1);

		cf_assert(n_vac_dests != (uint32_t)-1, AS_DRV_MEM,
				"device %s: wblock-id %u vacation destinations underflow",
				mem->name, wblock_id);

		if (n_vac_dests != 0) {
			return;
		}
		// else - all wblocks we defragged into have been flushed.
	}

	as_fence_acq();

	cf_mutex_lock(&p_wblock_state->LOCK);

	if (p_wblock_state->inuse_sz == 0) {
		push_wblock_to_free_q(mem, wblock_id);
	}
	else {
		p_wblock_state->state = WBLOCK_STATE_EMPTYING;
	}

	cf_mutex_unlock(&p_wblock_state->LOCK);
}


//==========================================================
// Local helpers - maintenance.
//

static void*
run_mem_maintenance(void* udata)
{
	drv_mem* mem = (drv_mem*)udata;
	as_namespace* ns = mem->ns;

	uint64_t prev_n_total_writes = 0;
	uint64_t prev_n_defrag_reads = 0;
	uint64_t prev_n_defrag_writes = 0;
	uint64_t prev_n_defrag_io_skips = 0;
	uint64_t prev_n_direct_frees = 0;
	uint64_t prev_n_tomb_raider_reads = 0;

	uint64_t prev_n_writes_flush[N_CURRENT_SWBS] = { 0 };

	uint64_t prev_n_defrag_writes_flush = 0;

	uint64_t now = cf_getus();
	uint64_t next = now + MAX_INTERVAL;

	uint64_t prev_log_stats = now;
	uint64_t prev_free_mwbs = now;
	uint64_t prev_flush[N_CURRENT_SWBS];
	uint64_t prev_defrag_flush = now;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		prev_flush[c] = now;
	}

	// If any job's (initial) interval is less than MAX_INTERVAL and we want it
	// done on its interval the first time through, add a next_time() call for
	// that job here to adjust 'next'. (No such jobs for now.)

	uint64_t sleep_us = next - now;

	while (true) {
		usleep((uint32_t)sleep_us);

		now = cf_getus();
		next = now + MAX_INTERVAL;

		if (now >= prev_log_stats + LOG_STATS_INTERVAL) {
			log_stats(mem, &prev_n_total_writes, &prev_n_defrag_reads,
					&prev_n_defrag_writes, &prev_n_defrag_io_skips,
					&prev_n_direct_frees, &prev_n_tomb_raider_reads);
			prev_log_stats = now;
			next = next_time(now, LOG_STATS_INTERVAL, next);
		}

		if (now >= prev_free_mwbs + FREE_SWBS_INTERVAL) {
			free_mwbs(mem);
			prev_free_mwbs = now;
			next = next_time(now, FREE_SWBS_INTERVAL, next);
		}

		if (mem->shadow_name != NULL && ! ns->storage_commit_to_device) {
			uint64_t flush_max_us = as_load_uint64(&ns->storage_flush_max_us);

			if (flush_max_us != 0) {
				for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
					if (now >= prev_flush[c] + flush_max_us) {
						flush_current_mwb(mem, c, &prev_n_writes_flush[c]);
						prev_flush[c] = now;
						next = next_time(now, flush_max_us, next);
					}
				}
			}
		}

		if (mem->shadow_name != NULL) {
			static const uint64_t DEFRAG_FLUSH_MAX_US = 3UL * 1000 * 1000;

			if (now >= prev_defrag_flush + DEFRAG_FLUSH_MAX_US) {
				flush_defrag_mwb(mem, &prev_n_defrag_writes_flush);
				prev_defrag_flush = now;
				next = next_time(now, DEFRAG_FLUSH_MAX_US, next);
			}
		}

		if (mem->defrag_sweep != 0) {
			// May take long enough to mess up other jobs' schedules, but it's a
			// very rare manually-triggered intervention.
			defrag_sweep(mem);
			as_decr_uint32(&mem->defrag_sweep);
		}

		now = cf_getus(); // refresh in case jobs took significant time
		sleep_us = next > now ? next - now : 1;
	}

	return NULL;
}

static void
log_stats(drv_mem* mem, uint64_t* p_prev_n_total_writes,
		uint64_t* p_prev_n_defrag_reads, uint64_t* p_prev_n_defrag_writes,
		uint64_t* p_prev_n_defrag_io_skips, uint64_t* p_prev_n_direct_frees,
		uint64_t* p_prev_n_tomb_raider_reads)
{
	uint64_t n_defrag_reads = as_load_uint64(&mem->n_defrag_wblock_reads);
	uint64_t n_defrag_writes = as_load_uint64(&mem->n_defrag_wblock_writes);

	uint64_t n_total_writes = n_defrag_writes;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		n_total_writes += mem->current_mwbs[c].n_wblock_writes;
	}

	uint64_t n_defrag_partial_writes = 0;
	uint64_t n_total_partial_writes = 0;

	if (mem->shadow_name != NULL) {
		n_defrag_partial_writes =
					as_load_uint64(&mem->n_defrag_wblock_partial_writes);
		n_total_partial_writes = n_defrag_partial_writes;

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			n_total_partial_writes +=
					mem->current_mwbs[c].n_wblock_partial_writes;
		}
	}

	uint64_t n_defrag_io_skips =
			as_load_uint64(&mem->n_wblock_defrag_io_skips);
	uint64_t n_direct_frees = as_load_uint64(&mem->n_wblock_direct_frees);

	float total_write_rate = (float)(n_total_writes - *p_prev_n_total_writes) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_read_rate = (float)(n_defrag_reads - *p_prev_n_defrag_reads) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_write_rate =
			(float)(n_defrag_writes - *p_prev_n_defrag_writes) /
			(float)LOG_STATS_INTERVAL_sec;

	float defrag_io_skip_rate =
			(float)(n_defrag_io_skips - *p_prev_n_defrag_io_skips) /
			(float)LOG_STATS_INTERVAL_sec;
	float direct_free_rate = (float)(n_direct_frees - *p_prev_n_direct_frees) /
			(float)LOG_STATS_INTERVAL_sec;

	uint64_t n_tomb_raider_reads = mem->n_tomb_raider_reads;
	char tomb_raider_str[64];

	*tomb_raider_str = 0;

	if (n_tomb_raider_reads != 0) {
		if (*p_prev_n_tomb_raider_reads > n_tomb_raider_reads) {
			*p_prev_n_tomb_raider_reads = 0;
		}

		float tomb_raider_read_rate =
				(float)(n_tomb_raider_reads - *p_prev_n_tomb_raider_reads) /
				(float)LOG_STATS_INTERVAL_sec;

		sprintf(tomb_raider_str, " tomb-raider-read (%lu,%.1f)",
				n_tomb_raider_reads, tomb_raider_read_rate);
	}

	char shadow_str[64];
	char pf_str[128];

	*shadow_str = 0;
	*pf_str = 0;

	if (mem->shadow_name != NULL) {
		sprintf(shadow_str, " write-q %u", cf_queue_sz(mem->mwb_shadow_q));
		sprintf(pf_str, " partial-writes %lu defrag-partial-writes %lu",
				n_total_partial_writes, n_defrag_partial_writes);
	}

	uint32_t free_wblock_q_sz = cf_queue_sz(mem->free_wblock_q);
	uint32_t n_pristine_wblocks = num_pristine_wblocks(mem);
	uint32_t n_free_wblocks = free_wblock_q_sz + n_pristine_wblocks;

	cf_info(AS_DRV_MEM, "{%s} %s: used-bytes %lu free-wblocks %u write (%lu,%.1f) defrag-q %u defrag-read (%lu,%.1f) defrag-write (%lu,%.1f)%s%s",
			mem->ns->name, mem->name,
			mem->inuse_size, n_free_wblocks,
			n_total_writes, total_write_rate,
			cf_queue_sz(mem->defrag_wblock_q), n_defrag_reads,
					defrag_read_rate,
			n_defrag_writes, defrag_write_rate,
			shadow_str, tomb_raider_str);

	cf_detail(AS_DRV_MEM, "{%s} %s: free-wblocks (%u,%u) defrag-io-skips (%lu,%.1f) direct-frees (%lu,%.1f)%s",
			mem->ns->name, mem->name,
			free_wblock_q_sz, n_pristine_wblocks,
			n_defrag_io_skips, defrag_io_skip_rate,
			n_direct_frees, direct_free_rate,
			pf_str);

	*p_prev_n_total_writes = n_total_writes;
	*p_prev_n_defrag_reads = n_defrag_reads;
	*p_prev_n_defrag_writes = n_defrag_writes;
	*p_prev_n_defrag_io_skips = n_defrag_io_skips;
	*p_prev_n_direct_frees = n_direct_frees;
	*p_prev_n_tomb_raider_reads = n_tomb_raider_reads;

	if (n_free_wblocks == 0) {
		cf_warning(AS_DRV_MEM, "device %s: out of storage space", mem->name);
	}
}

static uint64_t
next_time(uint64_t now, uint64_t job_interval, uint64_t next)
{
	uint64_t next_job = now + job_interval;

	return next_job < next ? next_job : next;
}

static void
free_mwbs(drv_mem* mem)
{
	// Try to recover mwbs, 16 at a time, down to 16.
	for (uint32_t i = 0; i < 16 && cf_queue_sz(mem->mwb_free_q) > 16; i++) {
		mem_write_block* mwb;

		if (CF_QUEUE_OK !=
				cf_queue_pop(mem->mwb_free_q, &mwb, CF_QUEUE_NOWAIT)) {
			break;
		}

		mwb_destroy(mwb);
	}
}

static void
flush_current_mwb(drv_mem* mem, uint8_t which, uint64_t* p_prev_n_writes)
{
	current_mwb* cur_mwb = &mem->current_mwbs[which];
	uint64_t n_writes = as_load_uint64(&cur_mwb->n_wblock_writes);

	// If there's an active write load, we don't need to flush.
	if (n_writes != *p_prev_n_writes) {
		*p_prev_n_writes = n_writes;
		return;
	}

	cf_mutex_lock(&cur_mwb->lock);

	n_writes = as_load_uint64(&cur_mwb->n_wblock_writes);

	// Must check under the lock, could be racing a current mwb just queued.
	if (n_writes != *p_prev_n_writes) {
		cf_mutex_unlock(&cur_mwb->lock);

		*p_prev_n_writes = n_writes;
		return;
	}

	// Flush the current mwb if it isn't empty, and has been written to since
	// last flushed.

	mem_write_block* mwb = cur_mwb->mwb;

	uint64_t write_offset = 0;
	size_t write_sz = 0;
	uint8_t* buf = NULL;

	if (mwb != NULL && mwb->pos != mwb->flush_pos) {
		mem_wait_writers_done(mwb);

		write_offset = WBLOCK_ID_TO_OFFSET(mwb->wblock_id) + mwb->flush_pos;
		write_sz = mwb->pos - mwb->flush_pos;

		buf = encrypt_wblock(mwb, write_offset) + mwb->flush_pos;

		mwb->flush_pos = mwb->pos;

		as_incr_uint32(&mwb->n_writers);
	}

	cf_mutex_unlock(&cur_mwb->lock);

	if (write_offset != 0) {
		// Flush it.
		shadow_flush_buf(mem, buf, write_offset, write_sz);
		as_decr_uint32_rls(&mwb->n_writers);

		cur_mwb->n_wblock_partial_writes++;
	}
}

static void
flush_defrag_mwb(drv_mem* mem, uint64_t* p_prev_n_defrag_writes)
{
	uint64_t n_defrag_writes = as_load_uint64(&mem->n_defrag_wblock_writes);

	// If there's an active defrag load, we don't need to flush.
	if (n_defrag_writes != *p_prev_n_defrag_writes) {
		*p_prev_n_defrag_writes = n_defrag_writes;
		return;
	}

	cf_mutex_lock(&mem->defrag_lock);

	n_defrag_writes = as_load_uint64(&mem->n_defrag_wblock_writes);

	// Must check under the lock, could be racing a current mwb just queued.
	if (n_defrag_writes != *p_prev_n_defrag_writes) {
		cf_mutex_unlock(&mem->defrag_lock);

		*p_prev_n_defrag_writes = n_defrag_writes;
		return;
	}

	// Flush the defrag mwb if it isn't empty, and has been written to since
	// last flushed.

	mem_write_block* mwb = mem->defrag_mwb;

	if (mwb != NULL && mwb->n_vacated != 0) {
		uint64_t write_offset = WBLOCK_ID_TO_OFFSET(mwb->wblock_id) +
				mwb->flush_pos;
		size_t write_sz = mwb->pos - mwb->flush_pos;

		uint8_t* buf = encrypt_wblock(mwb, write_offset) + mwb->flush_pos;

		mwb->flush_pos = mwb->pos;

		shadow_flush_buf(mem, buf, write_offset, write_sz);

		mem->n_defrag_wblock_partial_writes++;

		// The whole point - free source wblocks, sets n_vacated to 0.
		mwb_release_all_vacated_wblocks(mwb);
	}

	cf_mutex_unlock(&mem->defrag_lock);
}

// Check all wblocks to load a device's defrag queue at runtime. Triggered only
// when defrag-lwm-pct is increased by manual intervention.
static void
defrag_sweep(drv_mem* mem)
{
	uint32_t first_id = mem->first_wblock_id;
	uint32_t end_id = mem->n_wblocks;
	uint32_t n_queued = 0;

	for (uint32_t wblock_id = first_id; wblock_id < end_id; wblock_id++) {
		mem_wblock_state* p_wblock_state = &mem->wblock_state[wblock_id];

		cf_mutex_lock(&p_wblock_state->LOCK);

		if (p_wblock_state->state == WBLOCK_STATE_USED &&
				p_wblock_state->inuse_sz < mem->ns->defrag_lwm_size &&
				! p_wblock_state->short_lived) {
			push_wblock_to_defrag_q(mem, wblock_id);
			n_queued++;
		}

		cf_mutex_unlock(&p_wblock_state->LOCK);
	}

	cf_info(AS_DRV_MEM, "... %s sweep queued %u wblocks for defrag",
			mem->name, n_queued);
}


//==========================================================
// Local helpers - mwb class.
//

static mem_write_block*
mwb_create(drv_mem* mem)
{
	mem_write_block* mwb = cf_calloc(1, sizeof(mem_write_block));

	if (mem->shadow_name != NULL) {
		mwb->vacated_capacity = VACATED_CAPACITY_STEP;
		mwb->vacated_wblocks =
				cf_malloc(sizeof(vacated_wblock) * mwb->vacated_capacity);
	}

	return mwb;
}

static void
mwb_destroy(mem_write_block* mwb)
{
	if (mwb->vacated_wblocks != NULL) {
		cf_free(mwb->vacated_wblocks);
	}

	// Note - encrypted_buf will have been freed.

	cf_free(mwb);
}

static void
mwb_reset(mem_write_block* mwb)
{
	mwb->flush_pos = 0;
	mwb->wblock_id = STORAGE_INVALID_WBLOCK;
	mwb->pos = 0;
	// Note - encrypted_buf will have been freed and NULL'd.
}

// Not static - called by split function.
void
mwb_release(drv_mem* mem, mem_write_block* mwb)
{
	uint32_t wblock_id = mwb->wblock_id;
	mem_wblock_state* wblock_state = &mem->wblock_state[wblock_id];

	cf_assert(mwb == wblock_state->mwb, AS_DRV_MEM,
			"releasing wrong mwb! %p (%d) != %p (%d), thread %d",
			mwb, (int32_t)mwb->wblock_id, wblock_state->mwb,
			(int32_t)wblock_state->mwb->wblock_id, cf_thread_sys_tid());

	cf_assert(wblock_state->state == WBLOCK_STATE_RESERVED, AS_DRV_MEM,
			"device %s: wblock-id %u state %u on mwb release", mem->name,
			wblock_id, wblock_state->state);

	cf_mutex_lock(&wblock_state->LOCK);

	mwb_reset(wblock_state->mwb);
	cf_queue_push(mwb->mem->mwb_free_q, &mwb);

	wblock_state->mwb = NULL;

	if (wblock_state->inuse_sz == 0) {
		wblock_state->short_lived = false;

		as_incr_uint64(&mem->n_wblock_direct_frees);
		push_wblock_to_free_q(mem, wblock_id);
	}
	else if (wblock_state->inuse_sz < mem->ns->defrag_lwm_size) {
		if (! wblock_state->short_lived) {
			push_wblock_to_defrag_q(mem, wblock_id);
		}
		else {
			wblock_state->state = WBLOCK_STATE_USED;
		}
	}
	else {
		wblock_state->state = WBLOCK_STATE_USED;
	}

	cf_mutex_unlock(&wblock_state->LOCK);
}

// Not static - called by split function.
mem_write_block*
mwb_get(drv_mem* mem, bool use_reserve)
{
	if (! use_reserve && num_free_wblocks(mem) <=
			// Records never change stripes if memory-only - 1 should work.
			// (Assuming data-size no more than 8 x 2T = 16T.)
			(mem->shadow_name != NULL ? DRV_DEFRAG_RESERVE : 1)) {
		return NULL;
	}

	mem_write_block* mwb;

	if (CF_QUEUE_OK != cf_queue_pop(mem->mwb_free_q, &mwb, CF_QUEUE_NOWAIT)) {
		mwb = mwb_create(mem);
		mwb->n_writers = 0;
		mwb->flush_pos = 0;
		mwb->mem = mem;
		mwb->wblock_id = STORAGE_INVALID_WBLOCK;
		mwb->pos = 0;
	}

	// Find a device block to write to.
	if (cf_queue_pop(mem->free_wblock_q, &mwb->wblock_id, CF_QUEUE_NOWAIT) !=
			CF_QUEUE_OK && ! pop_pristine_wblock_id(mem, &mwb->wblock_id)) {
		cf_queue_push(mem->mwb_free_q, &mwb);
		return NULL;
	}

	mwb->base_addr = mem->mem_base_addr + (uint64_t)mwb->wblock_id * WBLOCK_SZ;

	mem_mprotect(mwb->base_addr, WBLOCK_SZ, PROT_READ | PROT_WRITE);

	mem_wblock_state* p_wblock_state = &mem->wblock_state[mwb->wblock_id];

	uint32_t inuse_sz = as_load_uint32(&p_wblock_state->inuse_sz);

	cf_assert(inuse_sz == 0, AS_DRV_MEM,
			"device %s: wblock-id %u inuse-size %u off free-q", mem->name,
			mwb->wblock_id, inuse_sz);

	cf_assert(p_wblock_state->mwb == NULL, AS_DRV_MEM,
			"device %s: wblock-id %u mwb not null off free-q", mem->name,
			mwb->wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_FREE, AS_DRV_MEM,
			"device %s: wblock-id %u state %u off free-q", mem->name,
			mwb->wblock_id, p_wblock_state->state);

	p_wblock_state->mwb = mwb;
	p_wblock_state->state = WBLOCK_STATE_RESERVED;

	return mwb;
}

static bool
pop_pristine_wblock_id(drv_mem* mem, uint32_t* wblock_id)
{
	uint32_t id;

	while ((id = as_load_uint32(&mem->pristine_wblock_id)) < mem->n_wblocks) {
		if (as_cas_uint32(&mem->pristine_wblock_id, id, id + 1)) {
			*wblock_id = id;
			return true;
		}
	}

	return false; // out of space
}

static bool
mwb_add_unique_vacated_wblock(mem_write_block* mwb, uint32_t src_file_id,
		uint32_t src_wblock_id)
{
	for (uint32_t i = 0; i < mwb->n_vacated; i++) {
		vacated_wblock* vw = &mwb->vacated_wblocks[i];

		if (vw->wblock_id == src_wblock_id && vw->file_id == src_file_id) {
			return false; // already present
		}
	}

	if (mwb->n_vacated == mwb->vacated_capacity) {
		mwb->vacated_capacity += VACATED_CAPACITY_STEP;
		mwb->vacated_wblocks = cf_realloc(mwb->vacated_wblocks,
				sizeof(vacated_wblock) * mwb->vacated_capacity);
	}

	mwb->vacated_wblocks[mwb->n_vacated].file_id = src_file_id;
	mwb->vacated_wblocks[mwb->n_vacated].wblock_id = src_wblock_id;
	mwb->n_vacated++;

	return true; // added to list
}

static void
mwb_release_all_vacated_wblocks(mem_write_block* mwb)
{
	drv_mems* mems = (drv_mems*)mwb->mem->ns->storage_private;

	for (uint32_t i = 0; i < mwb->n_vacated; i++) {
		vacated_wblock* vw = &mwb->vacated_wblocks[i];

		drv_mem* src_mem = &mems->mems[vw->file_id];
		mem_wblock_state* wblock_state =
				&src_mem->wblock_state[vw->wblock_id];

		release_vacated_wblock(src_mem, vw->wblock_id, wblock_state);
	}

	mwb->n_vacated = 0;
}


//==========================================================
// Local helpers - persistence utilities.
//

// Not static - called by split function.
void
mem_mprotect(void* addr, size_t len, int prot)
{
	if (mprotect(addr, len, prot) < 0) {
		cf_crash(AS_DRV_MEM, "mprotect(%p, %zu, %d) failed: %d (%s)", addr,
				len, prot, errno, cf_strerror(errno));
	}
}

static void
prepare_for_first_write(mem_write_block* mwb)
{
	memset(mwb->base_addr, 0, WBLOCK_SZ);

	as_flat_record* first = (as_flat_record*)mwb->base_addr;

	first->magic = AS_FLAT_MAGIC;
}


//==========================================================
// Local helpers - shadow utilities.
//

// Not static - called by split function.
int
shadow_fd_get(drv_mem* mem)
{
	int fd = -1;
	int rv = cf_queue_pop(mem->shadow_fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(mem->shadow_name, mem->open_flag, cf_os_base_perms());

		if (-1 == fd) {
			cf_crash(AS_DRV_MEM, "%s: DEVICE FAILED open: errno %d (%s)",
					mem->shadow_name, errno, cf_strerror(errno));
		}
	}

	return fd;
}

static void
shadow_fd_put(drv_mem* mem, int fd)
{
	cf_queue_push(mem->shadow_fd_q, (void*)&fd);
}

static void
shadow_flush_mwb(drv_mem* mem, mem_write_block* mwb)
{
	memset(&mwb->base_addr[mwb->pos], 0, WBLOCK_SZ - mwb->pos);
	mem_wait_writers_done(mwb);
	mem_mprotect(mwb->base_addr, WBLOCK_SZ, PROT_READ);

	uint64_t write_offset =
			WBLOCK_ID_TO_OFFSET(mwb->wblock_id) + mwb->flush_pos;

	uint8_t* buf = encrypt_wblock(mwb, write_offset);

	// Clean the end of the buffer before flushing.
	if (mwb->encrypted_buf != NULL && mwb->pos < WBLOCK_SZ) {
		memset(&mwb->encrypted_buf[mwb->pos], 0, WBLOCK_SZ - mwb->pos);
	}

	shadow_flush_buf(mem, buf + mwb->flush_pos, write_offset,
			WBLOCK_SZ - mwb->flush_pos);

	if (mwb->encrypted_buf != NULL) {
		cf_free(mwb->encrypted_buf);
		mwb->encrypted_buf = NULL;
	}
}

static void
shadow_flush_buf(drv_mem* mem, const uint8_t* buf, off_t write_offset,
		uint64_t write_sz)
{
	uint32_t flush_sz = as_load_uint32(&mem->ns->storage_flush_size);

	uint64_t flush_offset = BYTES_DOWN_TO_FLUSH(flush_sz, write_offset);
	uint64_t flush_end = BYTES_UP_TO_FLUSH(flush_sz, write_offset + write_sz);
	const uint8_t* flush = buf - (write_offset - flush_offset);

	int fd = shadow_fd_get(mem);

	while (flush_offset < flush_end) {
		uint64_t start_ns = mem->ns->storage_benchmarks_enabled ?
				cf_getns() : 0;

		if (! pwrite_all(fd, flush, flush_sz, (off_t)flush_offset)) {
			cf_crash(AS_DRV_MEM, "%s: DEVICE FAILED write: errno %d (%s)",
					mem->shadow_name, errno, cf_strerror(errno));
		}

		if (start_ns != 0) {
			histogram_insert_data_point(mem->hist_shadow_write, start_ns);
		}

		flush += flush_sz;
		flush_offset += flush_sz;
	}

	shadow_fd_put(mem, fd);
}
