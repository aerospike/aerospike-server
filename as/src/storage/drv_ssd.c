/*
 * drv_ssd.c
 *
 * Copyright (C) 2009-2023 Aerospike, Inc.
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

#include "storage/drv_ssd.h"

#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h> // for BLKGETSIZE64
#include <sys/ioctl.h>
#include <sys/param.h> // for MAX()

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
#include "hist.h"
#include "linear_hist.h"
#include "log.h"
#include "os.h"
#include "pool.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/index.h"
#include "base/nsup.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"
#include "transaction/mrt_utils.h"
#include "transaction/rw_utils.h"


//==========================================================
// Constants.
//

// TODO - could decrease this as number of drives increases?
#define MAX_POOL_FDS 512 // power of 2 (rounds up anyway)

#define WRITE_IN_PLACE 1


//==========================================================
// Miscellaneous utility functions.
//

// Get an open file descriptor from the pool, or a fresh one if necessary.
int
ssd_fd_get(drv_ssd *ssd)
{
	while (true) {
		int fd = cf_pool_int32_pop(&ssd->fd_pool);

		if (fd != -1) {
			return fd;
		}

		uint32_t n_fds = as_load_uint32(&ssd->n_fds);

		if (n_fds == MAX_POOL_FDS) {
			cf_ticker_warning(AS_DRV_SSD, "%s: fd pool full", ssd->name);
			sched_yield();
			continue;
		}

		if (! as_cas_uint32(&ssd->n_fds, n_fds, n_fds + 1)) {
			continue;
		}

		fd = open(ssd->name, ssd->open_flag, cf_os_base_perms());

		if (fd < 0) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		return fd;
	}
}


int
ssd_fd_cache_get(drv_ssd *ssd)
{
	while (true) {
		int fd = cf_pool_int32_pop(&ssd->fd_cache_pool);

		if (fd != -1) {
			return fd;
		}

		uint32_t n_fds = as_load_uint32(&ssd->n_cache_fds);

		if (n_fds == MAX_POOL_FDS) {
			cf_ticker_warning(AS_DRV_SSD, "%s: cache fd pool full", ssd->name);
			sched_yield();
			continue;
		}

		if (! as_cas_uint32(&ssd->n_cache_fds, n_fds, n_fds + 1)) {
			continue;
		}

		fd = open(ssd->name, ssd->open_flag & ~(O_DIRECT | O_DSYNC),
				cf_os_base_perms());

		if (fd < 0) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		return fd;
	}
}


int
ssd_shadow_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->shadow_fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->shadow_name, ssd->open_flag, cf_os_base_perms());

		if (fd < 0) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->shadow_name, errno, cf_strerror(errno));
		}
	}

	return fd;
}


// Save an open file descriptor in the pool
void
ssd_fd_put(drv_ssd *ssd, int fd)
{
	cf_pool_int32_push(&ssd->fd_pool, fd);
}


static inline void
ssd_fd_cache_put(drv_ssd *ssd, int fd)
{
	cf_pool_int32_push(&ssd->fd_cache_pool, fd);
}


static inline void
ssd_shadow_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->shadow_fd_q, (void*)&fd);
}


// Decide which device a record belongs on.
static inline uint32_t
ssd_get_file_id(drv_ssds *ssds, cf_digest *keyd)
{
	return *(uint32_t*)&keyd->digest[DIGEST_STORAGE_BASE_BYTE] % ssds->n_ssds;
}


// Put a wblock on the write queue, to be flushed.
static inline void
push_wblock_to_write_q(drv_ssd* ssd, const ssd_write_buf* swb)
{
	as_incr_uint32(&ssd->ns->n_wblocks_to_flush);
	cf_queue_push(ssd->swb_write_q, &swb);
}


// Put a wblock on the free queue for reuse.
static inline void
push_wblock_to_free_q(drv_ssd *ssd, uint32_t wblock_id)
{
	// Can get here before queue created, e.g. cold start replacing records.
	if (ssd->free_wblock_q == NULL) {
		return;
	}

	cf_assert(wblock_id < ssd->n_wblocks, AS_DRV_SSD,
			"pushing bad wblock_id %d to free_wblock_q", (int32_t)wblock_id);

	ssd->wblock_state[wblock_id].state = WBLOCK_STATE_FREE;
	cf_queue_push(ssd->free_wblock_q, &wblock_id);
}


// Put a wblock on the defrag queue.
static inline void
push_wblock_to_defrag_q(drv_ssd *ssd, uint32_t wblock_id)
{
	if (ssd->defrag_wblock_q) { // null until devices are loaded at startup
		ssd->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
		as_incr_uint64(&ssd->n_defrag_wblock_reads);
	}
}


static inline bool
pop_pristine_wblock_id(drv_ssd *ssd, uint32_t* wblock_id)
{
	uint32_t id;

	while ((id = as_load_uint32(&ssd->pristine_wblock_id)) < ssd->n_wblocks) {
		if (as_cas_uint32(&ssd->pristine_wblock_id, id, id + 1)) {
			*wblock_id = id;
			return true;
		}
	}

	return false; // out of space
}


static inline uint32_t
num_pristine_wblocks(const drv_ssd *ssd)
{
	return ssd->n_wblocks - ssd->pristine_wblock_id;
}


static inline uint32_t
num_free_wblocks(const drv_ssd *ssd)
{
	return cf_queue_sz(ssd->free_wblock_q) + num_pristine_wblocks(ssd);
}


// Available contiguous size.
static inline uint64_t
available_size(drv_ssd *ssd)
{
	// Note - returns 100% available during cold start, to make it irrelevant in
	// cold start eviction threshold check.

	return ssd->free_wblock_q != NULL ?
			(uint64_t)num_free_wblocks(ssd) * WBLOCK_SZ : ssd->file_size;
}


void
ssd_release_vacated_wblock(drv_ssd *ssd, uint32_t wblock_id,
		ssd_wblock_state* p_wblock_state)
{
	cf_assert(p_wblock_state->swb == NULL, AS_DRV_SSD,
			"device %s: wblock-id %u swb not null while defragging",
			ssd->name, wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_DEFRAG, AS_DRV_SSD,
			"device %s: wblock-id %u state %u releasing vacation destination",
			ssd->name, wblock_id, p_wblock_state->state);

	cf_assert(! p_wblock_state->short_lived, AS_DRV_SSD,
			"device %s: wblock-id %u short-lived wblock in defrag state",
			ssd->name, wblock_id);

	uint32_t n_vac_dests = as_aaf_uint32_rls(&p_wblock_state->n_vac_dests, -1);

	cf_assert(n_vac_dests != (uint32_t)-1, AS_DRV_SSD,
			"device %s: wblock-id %u vacation destinations underflow",
			ssd->name, wblock_id);

	if (n_vac_dests != 0) {
		return;
	}
	// else - all wblocks we defragged into have been flushed.

	as_fence_acq();

	cf_mutex_lock(&p_wblock_state->LOCK);

	if (p_wblock_state->inuse_sz == 0) {
		push_wblock_to_free_q(ssd, wblock_id);
	}
	else {
		p_wblock_state->state = WBLOCK_STATE_EMPTYING;
	}

	cf_mutex_unlock(&p_wblock_state->LOCK);
}


//------------------------------------------------
// ssd_write_buf "swb" methods.
//

#define VACATED_CAPACITY_STEP 128 // allocate in 1K chunks

static inline ssd_write_buf*
swb_create(drv_ssd *ssd)
{
	ssd_write_buf *swb = (ssd_write_buf*)cf_malloc(sizeof(ssd_write_buf));

	swb->buf = cf_valloc(WBLOCK_SZ);
	swb->encrypted_buf = NULL;

	swb->n_vacated = 0;
	swb->vacated_capacity = VACATED_CAPACITY_STEP;
	swb->vacated_wblocks =
			cf_malloc(sizeof(vacated_wblock) * swb->vacated_capacity);

	return swb;
}

static inline void
swb_destroy(ssd_write_buf *swb)
{
	cf_free(swb->vacated_wblocks);
	cf_free(swb->buf);
	// Note - encrypted_buf will have been freed.

	cf_free(swb);
}

static inline void
swb_reset(ssd_write_buf *swb)
{
	swb->use_post_write_q = false;
	swb->flush_pos = 0;
	swb->wblock_id = STORAGE_INVALID_WBLOCK;
	swb->pos = 0;
	// Note - encrypted_buf will have been freed and NULL'd.
}

#define swb_reserve(_swb) as_incr_uint32(&(_swb)->rc)

static inline void
swb_check_and_reserve(ssd_wblock_state *wblock_state, ssd_write_buf **p_swb)
{
	cf_mutex_lock(&wblock_state->LOCK);

	if (wblock_state->swb != NULL) {
		*p_swb = wblock_state->swb;
		swb_reserve(*p_swb);
	}

	cf_mutex_unlock(&wblock_state->LOCK);
}

static inline void
swb_release(ssd_write_buf *swb)
{
	uint32_t rc = as_aaf_uint32_rls(&swb->rc, -1);

	cf_assert(rc != (uint32_t)-1, AS_DRV_SSD, "swb ref-count underflow");

	if (rc == 0) {
		// Note - as_fence_acq() not needed - no speculative read concerns here.

		swb_reset(swb);

		// Put the swb back on the free queue for reuse.
		cf_queue_push(swb->ssd->swb_free_q, &swb);
	}
}

static inline void
swb_dereference_and_release(drv_ssd *ssd, ssd_write_buf *swb)
{
	uint32_t wblock_id = swb->wblock_id;
	ssd_wblock_state *wblock_state = &ssd->wblock_state[wblock_id];

	cf_assert(swb == wblock_state->swb, AS_DRV_SSD,
			"releasing wrong swb! %p (%u) != %p (%u), thread %d",
			swb, wblock_id, wblock_state->swb, wblock_state->swb->wblock_id,
			cf_thread_sys_tid());

	cf_assert(wblock_state->state == WBLOCK_STATE_RESERVED, AS_DRV_SSD,
			"device %s: wblock-id %u state %u on swb release", ssd->name,
			wblock_id, wblock_state->state);

	cf_mutex_lock(&wblock_state->LOCK);

	swb_release(wblock_state->swb);
	wblock_state->swb = NULL;

	if (wblock_state->inuse_sz == 0) {
		wblock_state->short_lived = false;

		as_incr_uint64(&ssd->n_wblock_direct_frees);
		push_wblock_to_free_q(ssd, wblock_id);
	}
	else if (wblock_state->inuse_sz < ssd->ns->defrag_lwm_size) {
		if (! wblock_state->short_lived) {
			push_wblock_to_defrag_q(ssd, wblock_id);
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

ssd_write_buf *
swb_get(drv_ssd *ssd, bool use_reserve)
{
	if (! use_reserve && num_free_wblocks(ssd) <= DRV_DEFRAG_RESERVE) {
		return NULL;
	}

	ssd_write_buf *swb;

	if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
		swb = swb_create(ssd);
		swb->rc = 0;
		swb->n_writers = 0;
		swb->use_post_write_q = false;
		swb->flush_pos = 0;
		swb->ssd = ssd;
		swb->wblock_id = STORAGE_INVALID_WBLOCK;
		swb->pos = 0;
	}

	// Find a device block to write to.
	if (cf_queue_pop(ssd->free_wblock_q, &swb->wblock_id, CF_QUEUE_NOWAIT) !=
			CF_QUEUE_OK && ! pop_pristine_wblock_id(ssd, &swb->wblock_id)) {
		cf_queue_push(ssd->swb_free_q, &swb);
		return NULL;
	}

	cf_assert(swb->rc == 0, AS_DRV_SSD,
			"device %s: wblock-id %u swb rc %u off free-q", ssd->name,
			swb->wblock_id, swb->rc);

	swb->rc = 1;

	ssd_wblock_state* p_wblock_state = &ssd->wblock_state[swb->wblock_id];

	uint32_t inuse_sz = as_load_uint32(&p_wblock_state->inuse_sz);

	cf_assert(inuse_sz == 0, AS_DRV_SSD,
			"device %s: wblock-id %u inuse-size %u off free-q", ssd->name,
			swb->wblock_id, inuse_sz);

	cf_assert(p_wblock_state->swb == NULL, AS_DRV_SSD,
			"device %s: wblock-id %u swb not null off free-q", ssd->name,
			swb->wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_FREE, AS_DRV_SSD,
			"device %s: wblock-id %u state %u off free-q", ssd->name,
			swb->wblock_id, p_wblock_state->state);

	p_wblock_state->swb = swb;
	p_wblock_state->state = WBLOCK_STATE_RESERVED;

	return swb;
}

bool
swb_add_unique_vacated_wblock(ssd_write_buf* swb, uint32_t src_file_id,
		uint32_t src_wblock_id)
{
	for (uint32_t i = 0; i < swb->n_vacated; i++) {
		vacated_wblock *vw = &swb->vacated_wblocks[i];

		if (vw->wblock_id == src_wblock_id && vw->file_id == src_file_id) {
			return false; // already present
		}
	}

	if (swb->n_vacated == swb->vacated_capacity) {
		swb->vacated_capacity += VACATED_CAPACITY_STEP;
		swb->vacated_wblocks = cf_realloc(swb->vacated_wblocks,
				sizeof(vacated_wblock) * swb->vacated_capacity);
	}

	swb->vacated_wblocks[swb->n_vacated].file_id = src_file_id;
	swb->vacated_wblocks[swb->n_vacated].wblock_id = src_wblock_id;
	swb->n_vacated++;

	return true; // added to list
}

void
swb_release_all_vacated_wblocks(ssd_write_buf* swb)
{
	drv_ssds *ssds = (drv_ssds *)swb->ssd->ns->storage_private;

	for (uint32_t i = 0; i < swb->n_vacated; i++) {
		vacated_wblock *vw = &swb->vacated_wblocks[i];

		drv_ssd *src_ssd = &ssds->ssds[vw->file_id];
		ssd_wblock_state* wblock_state = &src_ssd->wblock_state[vw->wblock_id];

		ssd_release_vacated_wblock(src_ssd, vw->wblock_id, wblock_state);
	}

	swb->n_vacated = 0;
}

//
// END - ssd_write_buf "swb" methods.
//------------------------------------------------


// Reduce wblock's used size, if result is 0 put it in the "free" pool, if it's
// below the defrag threshold put it in the defrag queue.
void
ssd_block_free(drv_ssd *ssd, uint64_t rblock_id, uint32_t n_rblocks, char *msg)
{
	// Determine which wblock we're reducing used size in.
	uint64_t start_offset = RBLOCK_ID_TO_OFFSET(rblock_id);
	uint32_t size = N_RBLOCKS_TO_SIZE(n_rblocks);
	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(start_offset);
	uint32_t end_wblock_id = OFFSET_TO_WBLOCK_ID(start_offset + size - 1);

	cf_assert(size >= DRV_RECORD_MIN_SIZE, AS_DRV_SSD,
			"%s: %s: freeing bad size %u rblock_id %lu", ssd->name, msg, size,
			rblock_id);

	cf_assert(start_offset >= DRV_HEADER_SIZE &&
			wblock_id < ssd->n_wblocks && wblock_id == end_wblock_id,
			AS_DRV_SSD, "%s: %s: freeing bad range rblock_id %lu n_rblocks %u",
			ssd->name, msg, rblock_id, n_rblocks);

	as_add_uint64(&ssd->inuse_size, -(int64_t)size);

	ssd_wblock_state *p_wblock_state = &ssd->wblock_state[wblock_id];

	cf_mutex_lock(&p_wblock_state->LOCK);

	int64_t resulting_inuse_sz =
			(int32_t)as_aaf_uint32(&p_wblock_state->inuse_sz, -(int32_t)size);

	cf_assert(resulting_inuse_sz >= 0 &&
			resulting_inuse_sz < (int64_t)WBLOCK_SZ, AS_DRV_SSD,
			"%s: %s: wblock %d %s, subtracted %d now %ld", ssd->name, msg,
			wblock_id, resulting_inuse_sz < 0 ? "over-freed" : "bad inuse_sz",
			(int32_t)size, resulting_inuse_sz);

	if (p_wblock_state->state == WBLOCK_STATE_USED) {
		if (resulting_inuse_sz == 0) {
			p_wblock_state->short_lived = false;

			as_incr_uint64(&ssd->n_wblock_direct_frees);
			push_wblock_to_free_q(ssd, wblock_id);
		}
		else if (resulting_inuse_sz < ssd->ns->defrag_lwm_size) {
			if (! p_wblock_state->short_lived) {
				push_wblock_to_defrag_q(ssd, wblock_id);
			}
		}
	}
	else if (p_wblock_state->state == WBLOCK_STATE_EMPTYING) {
		cf_assert(! p_wblock_state->short_lived, AS_DRV_SSD,
				"short-lived wblock in emptying state");

		if (resulting_inuse_sz == 0) {
			push_wblock_to_free_q(ssd, wblock_id);
		}
	}

	cf_mutex_unlock(&p_wblock_state->LOCK);
}


void
defrag_move_record(drv_ssd *src_ssd, uint32_t src_wblock_id,
		as_flat_record *flat, as_index *r)
{
	uint64_t old_rblock_id = r->rblock_id;
	uint32_t old_n_rblocks = r->n_rblocks;

	drv_ssds *ssds = (drv_ssds*)src_ssd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	drv_ssd *ssd = &ssds->ssds[ssd_get_file_id(ssds, &flat->keyd)];

	cf_assert(ssd, AS_DRV_SSD, "{%s} null ssd", ssds->ns->name);

	uint32_t ssd_n_rblocks = flat->n_rblocks;
	uint32_t write_size = N_RBLOCKS_TO_SIZE(ssd_n_rblocks);

	cf_mutex_lock(&ssd->defrag_lock);

	ssd_write_buf *swb = ssd->defrag_swb;

	if (! swb) {
		swb = swb_get(ssd, true);
		ssd->defrag_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "defrag_move_record: couldn't get swb");
			cf_mutex_unlock(&ssd->defrag_lock);
			return;
		}
	}

	// Check if there's enough space in defrag buffer - if not, enqueue it to be
	// flushed to device, and grab a new buffer.
	if (write_size > WBLOCK_SZ - swb->pos) {
		// Enqueue the buffer, to be flushed to device.
		push_wblock_to_write_q(ssd, swb);
		ssd->n_defrag_wblock_writes++;

		// Get the new buffer.
		while ((swb = swb_get(ssd, true)) == NULL) {
			// If we got here, we used all our reserve wblocks, but the wblocks
			// we defragged must still have non-zero inuse_sz. Must wait for
			// those to become free.
			cf_ticker_warning(AS_DRV_SSD, "{%s} defrag: drive %s totally full - waiting for vacated wblocks to be freed",
					ssd->ns->name, ssd->name);

			usleep(10 * 1000);
		}

		ssd->defrag_swb = swb;
	}

	memcpy(swb->buf + swb->pos, (const uint8_t*)flat, write_size);

	uint64_t write_offset = WBLOCK_ID_TO_OFFSET(swb->wblock_id) + swb->pos;

	r->file_id = ssd->file_id;
	r->rblock_id = OFFSET_TO_RBLOCK_ID(write_offset);
	r->n_rblocks = ssd_n_rblocks;

	swb->pos += write_size;

	as_add_uint64(&ssd->inuse_size, (int64_t)write_size);
	as_add_uint32(&ssd->wblock_state[swb->wblock_id].inuse_sz,
			(int32_t)write_size);

	// If we just defragged into a new destination swb, count it.
	if (swb_add_unique_vacated_wblock(swb, src_ssd->file_id, src_wblock_id)) {
		ssd_wblock_state* p_wblock_state =
				&src_ssd->wblock_state[src_wblock_id];

		as_incr_uint32(&p_wblock_state->n_vac_dests);
	}

	cf_mutex_unlock(&ssd->defrag_lock);

	ssd_block_free(src_ssd, old_rblock_id, old_n_rblocks, "defrag-write");
}


int
ssd_record_defrag(drv_ssd *ssd, uint32_t wblock_id, as_flat_record *flat,
		uint64_t rblock_id)
{
	as_namespace *ns = ssd->ns;
	as_partition_reservation rsv;
	uint32_t pid = as_partition_getid(&flat->keyd);

	as_partition_reserve(ns, pid, &rsv);

	int rv;
	as_index_ref r_ref;
	bool found = 0 == as_record_get(rsv.tree, &flat->keyd, &r_ref);

	if (found) {
		as_index *r = r_ref.r;

		if ((r = drv_current_record(ns, r, ssd->file_id, rblock_id)) != NULL) {
			if (r->generation != flat->generation) {
				cf_warning(AS_DRV_SSD, "device %s defrag: rblock_id %lu generation mismatch (%u:%u) %pD",
						ssd->name, rblock_id, r->generation, flat->generation,
						&r->keyd);
			}

			if (r->n_rblocks != flat->n_rblocks) {
				cf_warning(AS_DRV_SSD, "device %s defrag: rblock_id %lu n_blocks mismatch (%u:%u) %pD",
						ssd->name, rblock_id, r->n_rblocks, flat->n_rblocks,
						&r->keyd);
			}

			defrag_move_record(ssd, wblock_id, flat, r);

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


int
ssd_defrag_wblock(drv_ssd *ssd, uint32_t wblock_id, uint8_t *read_buf)
{
	int record_count = 0;

	as_namespace* ns = ssd->ns;
	ssd_wblock_state* p_wblock_state = &ssd->wblock_state[wblock_id];

	cf_assert(p_wblock_state->n_vac_dests == 0, AS_DRV_SSD,
			"n-vacations not 0 beginning defrag wblock");

	// Make sure this can't decrement to 0 while defragging this wblock.
	p_wblock_state->n_vac_dests = 1;

	if (as_load_uint32(&p_wblock_state->inuse_sz) == 0) {
		as_incr_uint64(&ssd->n_wblock_defrag_io_skips);
		goto Finished;
	}

	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = WBLOCK_ID_TO_OFFSET(wblock_id);

	uint32_t flush_sz = as_load_uint32(&ns->storage_flush_size);
	uint64_t offset = file_offset;
	uint64_t end_offset = offset + WBLOCK_SZ;
	uint8_t* at = read_buf;

	while (offset < end_offset) {
		uint64_t start_ns = ns->storage_benchmarks_enabled ? cf_getns() : 0;

		if (! pread_all(fd, at, flush_sz, (off_t)offset)) {
			cf_warning(AS_DRV_SSD, "%s: read failed: errno %d (%s)", ssd->name,
					errno, cf_strerror(errno));
			close(fd);
			as_decr_uint32(&ssd->n_fds);
			as_incr_uint64(&ssd->n_read_errors);
			goto Finished;
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_large_block_read, start_ns);
		}

		at += flush_sz;
		offset += flush_sz;

		uint32_t sleep_us = as_load_uint32(&ns->storage_defrag_sleep);

		if (sleep_us != 0) {
			usleep(sleep_us);
		}
	}

	ssd_fd_put(ssd, fd);

	bool prefetch = cf_arenax_want_prefetch(ns->arena);

	if (prefetch) {
		ssd_prefetch_wblock(ssd, file_offset, read_buf);
	}

	uint32_t indent = 0; // current offset within the wblock, in bytes

	while (indent < WBLOCK_SZ &&
			as_load_uint32(&p_wblock_state->inuse_sz) != 0) {
		as_flat_record *flat = (as_flat_record*)&read_buf[indent];

		if (! prefetch) {
			ssd_decrypt(ssd, file_offset + indent, flat);
		}

		if (flat->magic != AS_FLAT_MAGIC) {
			// First block must have magic.
			if (indent == 0) {
				cf_warning(AS_DRV_SSD, "%s: no magic at beginning of used wblock %d",
						ssd->name, wblock_id);
				break;
			}

			// Later blocks may have no magic, just skip to next block.
			indent += RBLOCK_SIZE;
			continue;
		}

		uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
		uint32_t next_indent = indent + record_size;

		if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
			cf_warning(AS_DRV_SSD, "%s: bad record size %u", ssd->name,
					record_size);
			indent += RBLOCK_SIZE;
			continue; // try next rblock
		}

		// Found a good record, move it if it's current.
		int rv = ssd_record_defrag(ssd, wblock_id, flat,
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

	ssd_release_vacated_wblock(ssd, wblock_id, p_wblock_state);

	return record_count;
}


// Thread "run" function to service a device's defrag queue.
void*
run_defrag(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;
	as_namespace *ns = ssd->ns;
	uint32_t wblock_id;
	uint8_t *read_buf = cf_valloc(WBLOCK_SZ);

	while (true) {
		uint32_t q_min = as_load_uint32(&ns->storage_defrag_queue_min);

		if (q_min == 0) {
			cf_queue_pop(ssd->defrag_wblock_q, &wblock_id, CF_QUEUE_FOREVER);
		}
		else {
			if (cf_queue_sz(ssd->defrag_wblock_q) <= q_min) {
				usleep(1000 * 50);
				continue;
			}

			cf_queue_pop(ssd->defrag_wblock_q, &wblock_id, CF_QUEUE_NOWAIT);
		}

		ssd_defrag_wblock(ssd, wblock_id, read_buf);

		while (ns->n_wblocks_to_flush > ns->storage_max_write_q + 128) {
			usleep(1000);
		}
	}

	return NULL;
}


void
ssd_start_defrag_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} starting defrag threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_thread_create_detached(run_defrag, (void*)ssd);
	}
}


//------------------------------------------------
// defrag_pen class.
//

#define DEFRAG_PEN_INIT_CAPACITY (8 * 1024)

typedef struct defrag_pen_s {
	uint32_t n_ids;
	uint32_t capacity;
	uint32_t *ids;
	uint32_t stack_ids[DEFRAG_PEN_INIT_CAPACITY];
} defrag_pen;

static void
defrag_pen_init(defrag_pen *pen)
{
	pen->n_ids = 0;
	pen->capacity = DEFRAG_PEN_INIT_CAPACITY;
	pen->ids = pen->stack_ids;
}

static void
defrag_pen_destroy(defrag_pen *pen)
{
	if (pen->ids != pen->stack_ids) {
		cf_free(pen->ids);
	}
}

static void
defrag_pen_add(defrag_pen *pen, uint32_t wblock_id)
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
defrag_pen_transfer(defrag_pen *pen, drv_ssd *ssd)
{
	// For speed, "customize" instead of using push_wblock_to_defrag_q()...
	for (uint32_t i = 0; i < pen->n_ids; i++) {
		uint32_t wblock_id = pen->ids[i];

		ssd->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
	}
}

static void
defrag_pens_dump(defrag_pen pens[], uint32_t n_pens, const char* ssd_name)
{
	char buf[2048];
	uint32_t n = 0;
	int pos = sprintf(buf, "%u", pens[n++].n_ids);

	while (n < n_pens) {
		pos += sprintf(buf + pos, ",%u", pens[n++].n_ids);
	}

	cf_info(AS_DRV_SSD, "%s init defrag profile: %s", ssd_name, buf);
}

//
// END - defrag_pen class.
//------------------------------------------------


// Thread "run" function to create and load a device's (wblock) free & defrag
// queues at startup. Sorts defrag-eligible wblocks so the most depleted ones
// are at the head of the defrag queue.
void*
run_load_queues(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;

	ssd->free_wblock_q = cf_queue_create(sizeof(uint32_t), true);
	ssd->defrag_wblock_q = cf_queue_create(sizeof(uint32_t), true);

	as_namespace *ns = ssd->ns;
	uint32_t lwm_pct = ns->storage_defrag_lwm_pct;
	uint32_t lwm_size = ns->defrag_lwm_size;
	defrag_pen pens[lwm_pct];

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_init(&pens[n]);
	}

	uint32_t first_id = ssd->first_wblock_id;
	uint32_t end_id = ssd->pristine_wblock_id;

	// TODO - paranoia - remove eventually.
	cf_assert(end_id >= first_id && end_id <= ssd->n_wblocks, AS_DRV_SSD,
			"%s bad pristine-wblock-id %u", ssd->name, end_id);

	for (uint32_t wblock_id = first_id; wblock_id < end_id; wblock_id++) {
		uint32_t inuse_sz = ssd->wblock_state[wblock_id].inuse_sz;

		if (inuse_sz == 0) {
			// Cold start may produce an empty short-lived wblock.
			ssd->wblock_state[wblock_id].short_lived = false;

			// Faster than using push_wblock_to_free_q() here...
			cf_queue_push(ssd->free_wblock_q, &wblock_id);
		}
		else if (inuse_sz < lwm_size &&
				! ssd->wblock_state[wblock_id].short_lived) {
			defrag_pen_add(&pens[(inuse_sz * lwm_pct) / lwm_size], wblock_id);
		}
		else {
			ssd->wblock_state[wblock_id].state = WBLOCK_STATE_USED;
		}
	}

	defrag_pens_dump(pens, lwm_pct, ssd->name);

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_transfer(&pens[n], ssd);
		defrag_pen_destroy(&pens[n]);
	}

	ssd->n_defrag_wblock_reads = (uint64_t)cf_queue_sz(ssd->defrag_wblock_q);

	return NULL;
}


void
ssd_load_wblock_queues(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} loading free & defrag queues", ssds->ns->name);

	// Split this task across multiple threads.
	cf_tid tids[ssds->n_ssds];

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		tids[i] = cf_thread_create_joinable(run_load_queues, (void*)ssd);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		cf_thread_join(tids[i]);
	}
	// Now we're single-threaded again.

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_info(AS_DRV_SSD, "%s init wblocks: pristine-id %u pristine %u free-q %u, defrag-q %u",
				ssd->name, ssd->pristine_wblock_id, num_pristine_wblocks(ssd),
				cf_queue_sz(ssd->free_wblock_q),
				cf_queue_sz(ssd->defrag_wblock_q));
	}
}


void
ssd_wblock_init(drv_ssd *ssd)
{
	uint32_t n_wblocks = (uint32_t)(ssd->file_size / WBLOCK_SZ);

	cf_info(AS_DRV_SSD, "%s has %u wblocks of size %u", ssd->name, n_wblocks,
			WBLOCK_SZ);

	ssd->n_wblocks = n_wblocks;
	ssd->wblock_state = cf_malloc(n_wblocks * sizeof(ssd_wblock_state));

	// Device header wblocks' inuse_sz will (also) be 0 but that doesn't matter.
	for (uint32_t i = 0; i < n_wblocks; i++) {
		ssd_wblock_state * p_wblock_state = &ssd->wblock_state[i];

		p_wblock_state->inuse_sz = 0;
		cf_mutex_init(&p_wblock_state->LOCK);
		p_wblock_state->swb = NULL;
		p_wblock_state->state = WBLOCK_STATE_FREE;
		p_wblock_state->short_lived = false;
		p_wblock_state->n_vac_dests = 0;
	}
}


//==========================================================
// Record reading utilities.
//

static bool
sanity_check_flat(const drv_ssd *ssd, const as_record *r,
		uint64_t record_offset, const as_flat_record *flat)
{
	as_namespace *ns = ssd->ns;
	bool ok = true;

	if (flat->magic != AS_FLAT_MAGIC) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad magic 0x%x offset %lu",
				ns->name, ssd->name, &r->keyd, flat->magic, record_offset);
		ok = false;
	}

	if (flat->n_rblocks != r->n_rblocks) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad n-rblocks %u expecting %u",
				ns->name, ssd->name, &r->keyd, flat->n_rblocks, r->n_rblocks);
		ok = false;
	}

	if (cf_digest_compare(&flat->keyd, &r->keyd) != 0) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD but found flat digest %pD",
				ns->name, ssd->name, &r->keyd, &flat->keyd);
		ok = false;
	}

	if (flat->xdr_write != r->xdr_write) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad xdr-write %u expecting %u",
				ns->name, ssd->name, &r->keyd, flat->xdr_write, r->xdr_write);
		ok = false;
	}

	if (flat->tree_id != r->tree_id) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad tree-id %u expecting %u",
				ns->name, ssd->name, &r->keyd, flat->tree_id, r->tree_id);
		ok = false;
	}

	if (flat->generation != r->generation) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad generation %u expecting %u",
				ns->name, ssd->name, &r->keyd, flat->generation, r->generation);
		ok = false;
	}

	if (flat->last_update_time != r->last_update_time) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad lut %lu expecting %lu",
				ns->name, ssd->name, &r->keyd, (uint64_t)flat->last_update_time,
				(uint64_t)r->last_update_time);
		ok = false;
	}

	return ok;
}


int
ssd_read_record(as_storage_rd *rd, bool pickle_only)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;
	drv_ssd *ssd = rd->ssd;

	uint64_t record_offset = RBLOCK_ID_TO_OFFSET(r->rblock_id);
	uint32_t record_size = N_RBLOCKS_TO_SIZE(r->n_rblocks);
	uint64_t record_end_offset = record_offset + record_size;

	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(record_offset);

	bool ok = true;

	if (wblock_id >= ssd->n_wblocks || wblock_id < ssd->first_wblock_id) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD bad offset %lu", ns->name,
				&r->keyd, record_offset);
		ok = false;
	}

	if (record_size < DRV_RECORD_MIN_SIZE) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD bad record size %u",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (record_end_offset > WBLOCK_ID_TO_OFFSET(wblock_id + 1)) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD record size %u crosses wblock boundary",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (! ok) {
		return -1;
	}

	uint8_t *read_buf = NULL;
	as_flat_record *flat = NULL;

	ssd_write_buf *swb = NULL;

	swb_check_and_reserve(&ssd->wblock_state[wblock_id], &swb);

	if (swb != NULL) {
		// Data is in write buffer, so read it from there.
		as_incr_uint32(&ns->n_reads_from_cache);

		read_buf = cf_malloc(record_size);
		flat = (as_flat_record*)read_buf;

		int swb_offset = record_offset - WBLOCK_ID_TO_OFFSET(wblock_id);
		memcpy(read_buf, swb->buf + swb_offset, record_size);
		swb_release(swb);

		if (! sanity_check_flat(ssd, r, record_offset, flat)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD failed read from buffer",
					ns->name, ssd->name, &r->keyd);
			cf_free(read_buf);
			return -1;
		}
	}
	else {
		// Normal case - data is read from device.
		as_incr_uint32(&ns->n_reads_from_device);

		uint64_t read_offset = BYTES_DOWN_TO_IO_MIN(ssd, record_offset);
		uint64_t read_end_offset = BYTES_UP_TO_IO_MIN(ssd, record_end_offset);
		size_t read_size = read_end_offset - read_offset;
		uint64_t record_buf_indent = record_offset - read_offset;

		read_buf = cf_valloc(read_size);

		int fd = rd->read_page_cache ? ssd_fd_cache_get(ssd) : ssd_fd_get(ssd);

		uint64_t start_ns = ns->storage_benchmarks_enabled ? cf_getns() : 0;
		uint64_t start_us = as_health_sample_device_read() ? cf_getus() : 0;

		if (! pread_all(fd, read_buf, read_size, (off_t)read_offset)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD IO failed errno %d (%s) size %lu",
					ns->name, ssd->name, &r->keyd, errno, cf_strerror(errno),
					read_size);
			cf_free(read_buf);
			close(fd);
			as_decr_uint32(rd->read_page_cache ?
					&ssd->n_cache_fds : &ssd->n_fds);
			as_incr_uint64(&ssd->n_read_errors);
			return -1;
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_read, start_ns);
		}

		as_health_add_device_latency(ns->ix, r->file_id, start_us);

		if (rd->read_page_cache) {
			ssd_fd_cache_put(ssd, fd);
		}
		else {
			ssd_fd_put(ssd, fd);
		}

		flat = (as_flat_record*)(read_buf + record_buf_indent);
		ssd_decrypt_whole(ssd, record_offset, r->n_rblocks, flat);

		if (! sanity_check_flat(ssd, r, record_offset, flat)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD failed read directly from device",
					ns->name, ssd->name, &r->keyd);
			cf_free(read_buf);
			return -1;
		}

		if (ns->storage_benchmarks_enabled) {
			histogram_insert_raw(ns->device_read_size_hist, read_size);
		}
	}

	rd->flat = flat;
	rd->read_buf = read_buf; // no need to free read_buf on error now

	as_flat_opt_meta opt_meta = { { 0 } };

	// Includes round rblock padding, so may not literally exclude the mark.
	// (Is set exactly to mark below, if skipping or decompressing bins.)
	rd->flat_end = (const uint8_t*)flat + record_size - END_MARK_SZ;

	rd->flat_bins = as_flat_unpack_record_meta(flat, rd->flat_end, &opt_meta);

	if (rd->flat_bins == NULL) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad record metadata",
				ns->name, ssd->name, &r->keyd);
		return -1;
	}

	rd->flat_n_bins = (uint16_t)opt_meta.n_bins;

	if (pickle_only) {
		if (! as_flat_skip_bins(&opt_meta.cm, rd)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad bin data",
					ns->name, ssd->name, &r->keyd);
			return -1;
		}

		return 0;
	}

	if (! as_flat_decompress_bins(&opt_meta.cm, rd)) {
		cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD bad compressed data",
				ns->name, ssd->name, &r->keyd);
		return -1;
	}

	if (opt_meta.key != NULL) {
		rd->key_size = opt_meta.key_size;
		rd->key = opt_meta.key;
	}
	// else - if updating record without key, leave rd (msg) key to be stored.

	return 0;
}


//==========================================================
// Storage API implementation: reading records.
//

int
as_storage_record_load_bins_ssd(as_storage_rd *rd)
{
	if (as_record_is_binless(rd->r)) {
		rd->n_bins = 0;
		return 0; // no need to read device
	}

	// If record hasn't been read, read it - sets rd->flat_bins and
	// rd->flat_n_bins.
	if (! rd->flat && ssd_read_record(rd, false) != 0) {
		cf_warning(AS_DRV_SSD, "load_bins: failed ssd_read_record()");
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
as_storage_record_load_key_ssd(as_storage_rd *rd)
{
	// If record hasn't been read, read it - sets rd->key_size and rd->key.
	if (! rd->flat && ssd_read_record(rd, false) != 0) {
		cf_warning(AS_DRV_SSD, "get_key: failed ssd_read_record()");
		return false;
	}

	return true;
}


bool
as_storage_record_load_pickle_ssd(as_storage_rd *rd)
{
	if (ssd_read_record(rd, true) != 0) {
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
as_storage_record_load_raw_ssd(as_storage_rd *rd, bool leave_encrypted)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;
	drv_ssd *ssd = rd->ssd;

	uint64_t record_offset = RBLOCK_ID_TO_OFFSET(r->rblock_id);
	uint32_t record_size = N_RBLOCKS_TO_SIZE(r->n_rblocks);
	uint64_t record_end_offset = record_offset + record_size;

	uint32_t wblock_id = OFFSET_TO_WBLOCK_ID(record_offset);

	bool ok = true;

	if (wblock_id >= ssd->n_wblocks || wblock_id < ssd->first_wblock_id) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD bad offset %lu", ns->name,
				&r->keyd, record_offset);
		ok = false;
	}

	if (record_size < DRV_RECORD_MIN_SIZE) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD bad record size %u",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (record_end_offset > WBLOCK_ID_TO_OFFSET(wblock_id + 1)) {
		cf_warning(AS_DRV_SSD, "{%s} read: digest %pD record size %u crosses wblock boundary",
				ns->name, &r->keyd, record_size);
		ok = false;
	}

	if (! ok) {
		return false;
	}

	uint8_t *read_buf = NULL;
	as_flat_record *flat = NULL;

	ssd_write_buf *swb = NULL;

	swb_check_and_reserve(&ssd->wblock_state[wblock_id], &swb);

	if (swb != NULL) {
		// Data is in write buffer, so read it from there.

		read_buf = cf_malloc(record_size);
		flat = (as_flat_record*)read_buf;

		int swb_offset = record_offset - WBLOCK_ID_TO_OFFSET(wblock_id);
		memcpy(read_buf, swb->buf + swb_offset, record_size);
		swb_release(swb);

		if (! sanity_check_flat(ssd, r, record_offset, flat)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD failed read from buffer",
					ns->name, ssd->name, &r->keyd);
			// Continue even on failure...
		}
	}
	else {
		// Normal case - data is read from device.

		uint64_t read_offset = BYTES_DOWN_TO_IO_MIN(ssd, record_offset);
		uint64_t read_end_offset = BYTES_UP_TO_IO_MIN(ssd, record_end_offset);
		size_t read_size = read_end_offset - read_offset;
		uint64_t record_buf_indent = record_offset - read_offset;

		read_buf = cf_valloc(read_size);

		int fd = rd->read_page_cache ? ssd_fd_cache_get(ssd) : ssd_fd_get(ssd);

		if (! pread_all(fd, read_buf, read_size, (off_t)read_offset)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD IO failed errno %d (%s) size %lu",
					ns->name, ssd->name, &r->keyd, errno, cf_strerror(errno),
					read_size);
			cf_free(read_buf);
			close(fd);
			as_decr_uint32(rd->read_page_cache ?
					&ssd->n_cache_fds : &ssd->n_fds);
			as_incr_uint64(&ssd->n_read_errors);
			return false;
		}

		if (rd->read_page_cache) {
			ssd_fd_cache_put(ssd, fd);
		}
		else {
			ssd_fd_put(ssd, fd);
		}

		flat = (as_flat_record*)(read_buf + record_buf_indent);

		if (! leave_encrypted) {
			ssd_decrypt_whole(ssd, record_offset, r->n_rblocks, flat);
		}

		if (! sanity_check_flat(ssd, r, record_offset, flat)) {
			cf_warning(AS_DRV_SSD, "{%s} read %s: digest %pD failed read directly from device",
					ns->name, ssd->name, &r->keyd);
			// Continue even on failure...
		}
	}

	rd->flat = flat;
	rd->read_buf = read_buf; // no need to free read_buf on error now

	// Does not exclude the mark!
	rd->flat_end = (const uint8_t*)flat + record_size;

	return true;
}


//==========================================================
// Record writing utilities.
//

static inline void
ssd_wait_writers_done(ssd_write_buf* swb)
{
	while (swb->n_writers != 0) {
		as_arch_pause();
	}

	as_fence_acq();
}


void
ssd_flush_buf(drv_ssd *ssd, const uint8_t *buf, uint64_t write_offset,
		size_t write_sz)
{
	uint32_t flush_sz = as_load_uint32(&ssd->ns->storage_flush_size);

	uint64_t flush_offset = BYTES_DOWN_TO_FLUSH(flush_sz, write_offset);
	uint64_t flush_end = BYTES_UP_TO_FLUSH(flush_sz, write_offset + write_sz);
	const uint8_t *flush = buf - (write_offset - flush_offset);

	int fd = ssd_fd_get(ssd);

	while (flush_offset < flush_end) {
		uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ?
				cf_getns() : 0;

		if (! pwrite_all(fd, flush, flush_sz, (off_t)flush_offset)) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_write, start_ns);
		}

		flush += flush_sz;
		flush_offset += flush_sz;
	}

	ssd_fd_put(ssd, fd);
}


void
ssd_shadow_flush_buf(drv_ssd *ssd, const uint8_t *buf, off_t write_offset,
		size_t write_sz)
{
	uint32_t flush_sz = as_load_uint32(&ssd->ns->storage_flush_size);

	uint64_t flush_offset = BYTES_DOWN_TO_FLUSH(flush_sz, write_offset);
	uint64_t flush_end = BYTES_UP_TO_FLUSH(flush_sz, write_offset + write_sz);
	const uint8_t *flush = buf - (write_offset - flush_offset);

	int fd = ssd_shadow_fd_get(ssd);

	while (flush_offset < flush_end) {
		uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ?
				cf_getns() : 0;

		if (! pwrite_all(fd, flush, flush_sz, flush_offset)) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
					ssd->shadow_name, errno, cf_strerror(errno));
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_shadow_write, start_ns);
		}

		flush += flush_sz;
		flush_offset += flush_sz;
	}

	ssd_shadow_fd_put(ssd, fd);
}


void
ssd_flush_swb(drv_ssd *ssd, ssd_write_buf *swb)
{
	ssd_wait_writers_done(swb);

	uint64_t write_offset =
			WBLOCK_ID_TO_OFFSET(swb->wblock_id) + swb->flush_pos;

	uint8_t *buf = ssd_encrypt_wblock(swb, write_offset);

	// Clean the end of the buffer before flushing.
	if (swb->pos < WBLOCK_SZ) {
		memset(buf + swb->pos, 0, WBLOCK_SZ - swb->pos);
	}

	ssd_flush_buf(ssd, buf + swb->flush_pos, write_offset,
			WBLOCK_SZ - swb->flush_pos);
}


void
ssd_shadow_flush_swb(drv_ssd *ssd, ssd_write_buf *swb)
{
	uint64_t write_offset =
			WBLOCK_ID_TO_OFFSET(swb->wblock_id) + swb->flush_pos;

	uint8_t *buf = swb->encrypted_buf != NULL ? swb->encrypted_buf : swb->buf;

	ssd_shadow_flush_buf(ssd, buf + swb->flush_pos, write_offset,
			WBLOCK_SZ - swb->flush_pos);
}


void
ssd_write_sanity_checks(drv_ssd *ssd, ssd_write_buf *swb)
{
	ssd_wblock_state* p_wblock_state = &ssd->wblock_state[swb->wblock_id];

	cf_assert(p_wblock_state->swb == swb, AS_DRV_SSD,
			"device %s: wblock-id %u swb not consistent while writing",
			ssd->name, swb->wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_RESERVED, AS_DRV_SSD,
			"device %s: wblock-id %u state %u off write-q", ssd->name,
			swb->wblock_id, p_wblock_state->state);
}


void
ssd_post_write(drv_ssd *ssd, ssd_write_buf *swb)
{
	// We're done with this (whether or not there's a post-write queue).
	if (swb->encrypted_buf != NULL) {
		cf_free(swb->encrypted_buf);
		swb->encrypted_buf = NULL;
	}

	if (swb->use_post_write_q && ssd->ns->post_write_q_limit != 0) {
		// Transfer swb to post-write queue.
		cf_queue_push(ssd->post_write_q, &swb);
	}
	else {
		swb_dereference_and_release(ssd, swb);
	}

	if (ssd->post_write_q) {
		// Release post-write queue swbs if we're over the limit.
		while (cf_queue_sz(ssd->post_write_q) > ssd->ns->post_write_q_limit) {
			ssd_write_buf* cached_swb;

			if (CF_QUEUE_OK != cf_queue_pop(ssd->post_write_q, &cached_swb,
					CF_QUEUE_NOWAIT)) {
				// Should never happen.
				cf_warning(AS_DRV_SSD, "device %s: post-write queue pop failed",
						ssd->name);
				break;
			}

			swb_dereference_and_release(ssd, cached_swb);
		}
	}
}


// Thread "run" function that flushes write buffers to device.
void *
run_write(void *arg)
{
	drv_ssd *ssd = (drv_ssd*)arg;

	while (ssd->running || cf_queue_sz(ssd->swb_write_q) != 0) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_write_q, &swb, 100)) {
			continue;
		}

		// Sanity checks (optional).
		ssd_write_sanity_checks(ssd, swb);

		// Flush to the device.
		ssd_flush_swb(ssd, swb);

		if (ssd->shadow_name) {
			// Queue for shadow device write.
			cf_queue_push(ssd->swb_shadow_q, &swb);
		}
		else {
			// If this swb was a defrag destination, release the sources.
			swb_release_all_vacated_wblocks(swb);

			// Transfer to post-write queue, or release swb, as appropriate.
			ssd_post_write(ssd, swb);

			as_decr_uint32(&ssd->ns->n_wblocks_to_flush);
		}
	} // infinite event loop waiting for block to write

	return NULL;
}


// Thread "run" function that flushes write buffers to shadow device.
void *
run_shadow(void *arg)
{
	drv_ssd *ssd = (drv_ssd*)arg;

	while (ssd->running_shadow || cf_queue_sz(ssd->swb_shadow_q) != 0) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_shadow_q, &swb, 100)) {
			continue;
		}

		// Sanity checks (optional).
		ssd_write_sanity_checks(ssd, swb);

		// Flush to the shadow device.
		ssd_shadow_flush_swb(ssd, swb);

		// If this swb was a defrag destination, release the sources.
		swb_release_all_vacated_wblocks(swb);

		// Transfer to post-write queue, or release swb, as appropriate.
		ssd_post_write(ssd, swb);

		as_decr_uint32(&ssd->ns->n_wblocks_to_flush);
	}

	return NULL;
}


void
ssd_start_write_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} starting write threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->write_tid = cf_thread_create_joinable(run_write, (void*)ssd);

		if (ssd->shadow_name) {
			ssd->shadow_tid = cf_thread_create_joinable(run_shadow, (void*)ssd);
		}
	}
}


int
ssd_buffer_bins(as_storage_rd *rd)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;
	drv_ssd *ssd = rd->ssd;

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
		cf_detail(AS_DRV_SSD, "{%s} write: size %u - rejecting %pD", ns->name,
				flat_w_mark_sz, &r->keyd);
		return -AS_ERR_RECORD_TOO_BIG;
	}

	as_flat_record *flat;

	if (rd->pickle == NULL) {
		flat = as_flat_compress_bins_and_pack_record(rd, WBLOCK_SZ, false, true,
				&flat_w_mark_sz);
		flat_sz = flat_w_mark_sz - END_MARK_SZ;
	}
	else {
		flat = (as_flat_record *)rd->pickle;

		// Limit check used orig size, but from here on use compressed size.
		flat_sz = rd->pickle_sz;
		flat_w_mark_sz = flat_sz + END_MARK_SZ;

		// Tree IDs are node-local - can't use those sent from other nodes.
		flat->tree_id = r->tree_id;
	}

	// Note - this is the only place where rounding size (up to a  multiple of
	// RBLOCK_SIZE) is really necessary.
	uint32_t write_sz = SIZE_UP_TO_RBLOCK_SIZE(flat_w_mark_sz);

	// Reserve the portion of the current swb where this record will be written.

	current_swb *cur_swb = &ssd->current_swbs[rd->which_current_swb];

	cf_mutex_lock(&cur_swb->lock);

	ssd_write_buf *swb = cur_swb->swb;

	if (! swb) {
		swb = swb_get(ssd, false);
		cur_swb->swb = swb;

		if (! swb) {
			cf_ticker_warning(AS_DRV_SSD, "{%s} out of space", ns->name);
			cf_mutex_unlock(&cur_swb->lock);
			return -AS_ERR_OUT_OF_SPACE;
		}

		ssd_set_wblock_flags(rd, swb);
	}

	// Check if there's enough space in current buffer - if not, enqueue it to
	// be flushed to device, and grab a new buffer.
	if (write_sz > WBLOCK_SZ - swb->pos) {
		// Enqueue the buffer, to be flushed to device.
		push_wblock_to_write_q(ssd, swb);
		cur_swb->n_wblock_writes++;

		// Get the new buffer.
		swb = swb_get(ssd, false);
		cur_swb->swb = swb;

		if (! swb) {
			cf_ticker_warning(AS_DRV_SSD, "{%s} out of space", ns->name);
			cf_mutex_unlock(&cur_swb->lock);
			return -AS_ERR_OUT_OF_SPACE;
		}

		ssd_set_wblock_flags(rd, swb);
	}

	uint32_t n_rblocks = ROUNDED_SIZE_TO_N_RBLOCKS(write_sz);

	if (rd->pickle != NULL) {
		flat->n_rblocks = n_rblocks;
	}

	uint32_t swb_pos;
	int rv = 0;

	if (! is_first_mrt(rd) && n_rblocks == r->n_rblocks &&
			swb->wblock_id == RBLOCK_ID_TO_WBLOCK_ID(r->rblock_id) &&
			ssd->file_id == r->file_id &&
			(swb_pos = RBLOCK_ID_TO_POS(r->rblock_id)) >= swb->flush_pos) {
		// Stored size is unchanged, previous version is in this buffer, and
		// hasn't been flushed - just overwrite at the previous position.
		rv = WRITE_IN_PLACE;
	}
	else {
		// There's enough space - save the position where this record will be
		// written, and advance swb->pos for the next writer.
		swb_pos = swb->pos;
		swb->pos += write_sz;
	}

	as_incr_uint32(&swb->n_writers);

	cf_mutex_unlock(&cur_swb->lock);
	// May now write this record concurrently with others in this swb.

	// Flatten data into the block.

	as_flat_record *flat_in_swb = (as_flat_record*)&swb->buf[swb_pos];

	if (flat == NULL) {
		as_flat_pack_record(rd, n_rblocks, false, flat_in_swb);
	}
	else {
		memcpy(flat_in_swb, flat, flat_sz);
	}

	drv_add_end_mark((uint8_t*)flat_in_swb + flat_sz, flat_in_swb);

	// Make a pickle if needed.
	if (rd->keep_pickle) {
		rd->pickle_sz = flat_sz;
		rd->pickle = cf_malloc(flat_sz);
		memcpy(rd->pickle, flat_in_swb, flat_sz);

		if (write_sz - flat_sz >= RBLOCK_SIZE) {
			((as_flat_record*)rd->pickle)->n_rblocks--;
		}
	}

	uint64_t write_offset = WBLOCK_ID_TO_OFFSET(swb->wblock_id) + swb_pos;

	if (rv != WRITE_IN_PLACE) {
		r->file_id = ssd->file_id;
		r->rblock_id = OFFSET_TO_RBLOCK_ID(write_offset);

		as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
				DELTA_N_RBLOCKS_TO_SIZE(n_rblocks, r->n_rblocks));

		r->n_rblocks = n_rblocks;

		as_add_uint64(&ssd->inuse_size, (int64_t)write_sz);
		as_add_uint32(&ssd->wblock_state[swb->wblock_id].inuse_sz,
				(int32_t)write_sz);
	}

	// We are finished writing to the buffer.
	as_decr_uint32_rls(&swb->n_writers);

	if (ns->storage_benchmarks_enabled) {
		histogram_insert_raw(ns->device_write_size_hist, write_sz);
	}

	return rv;
}


int
ssd_write(as_storage_rd *rd)
{
	as_record *r = rd->r;

	drv_ssd *old_ssd = NULL;
	uint64_t old_rblock_id = 0;
	uint32_t old_n_rblocks = 0;

	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id)) {
		// Replacing an old record.
		old_ssd = rd->ssd;
		old_rblock_id = r->rblock_id;
		old_n_rblocks = r->n_rblocks;
	}

	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	rd->ssd = &ssds->ssds[ssd_get_file_id(ssds, &r->keyd)];

	cf_assert(rd->ssd, AS_DRV_SSD, "{%s} null ssd", rd->ns->name);

	int rv = ssd_write_bins(rd);

	if (rv == 0 && old_ssd) {
		if (! is_first_mrt(rd)) {
			ssd_block_free(old_ssd, old_rblock_id, old_n_rblocks, "ssd-write");
		}
	}
	else if (rv == WRITE_IN_PLACE) {
		rv = 0; // no need to free old block - it's reused
	}

	if (rv == 0) {
		ssd_mrt_rd_block_free_orig(ssds, rd);
	}

	return rv;
}


//==========================================================
// Storage statistics utilities.
//

void
as_storage_dump_wb_summary_ssd(const as_namespace* ns)
{
	drv_ssds* ssds = ns->storage_private;

	uint32_t n_free = 0;
	uint32_t n_reserved = 0;
	uint32_t n_used = 0;
	uint32_t n_defrag = 0;
	uint32_t n_emptying = 0;

	uint32_t n_short_lived = 0;

	uint32_t n_zero_used_sz = 0;

	linear_hist* h =
			linear_hist_create("", LINEAR_HIST_SIZE, 0, WBLOCK_SZ, 100);

	for (uint32_t d = 0; d < ssds->n_ssds; d++) {
		drv_ssd* ssd = &ssds->ssds[d];

		for (uint32_t i = ssd->first_wblock_id; i < ssd->n_wblocks; i++) {
			ssd_wblock_state* wblock_state = &ssd->wblock_state[i];

			switch (wblock_state->state) {
			case WBLOCK_STATE_FREE:
				n_free++;
				break;
			case WBLOCK_STATE_RESERVED:
				n_reserved++;
				break;
			case WBLOCK_STATE_USED:
				n_used++;
				break;
			case WBLOCK_STATE_DEFRAG:
				n_defrag++;
				break;
			case WBLOCK_STATE_EMPTYING:
				n_emptying++;
				break;
			default:
				cf_warning(AS_DRV_SSD, "bad wblock state %u",
						wblock_state->state);
				break;
			}

			if (wblock_state->short_lived) {
				n_short_lived++;
			}

			uint32_t inuse_sz = as_load_uint32(&wblock_state->inuse_sz);

			if (inuse_sz == 0) {
				n_zero_used_sz++;
			}
			else {
				linear_hist_insert_data_point(h, inuse_sz);
			}
		}
	}

	cf_info(AS_DRV_SSD, "WB: namespace %s", ns->name);
	cf_info(AS_DRV_SSD, "WB: wblocks by state - free:%u reserved:%u used:%u defrag:%u emptying:%u",
			n_free, n_reserved, n_used, n_defrag, n_emptying);

	if (n_short_lived != 0) {
		cf_info(AS_DRV_SSD, "WB: short-lived wblocks - %u", n_short_lived);
	}

	cf_dyn_buf_define(db);

	// Not bothering with more suitable linear_hist API ... use what's there.
	linear_hist_save_info(h);
	linear_hist_get_info(h, &db);
	cf_dyn_buf_append_char(&db, '\0');

	cf_info(AS_DRV_SSD, "WB: wblocks with zero used-sz - %u", n_zero_used_sz);
	cf_info(AS_DRV_SSD, "WB: wblocks by (non-zero) used-sz - %s", db.buf);

	cf_dyn_buf_free(&db);
	linear_hist_destroy(h);
}


//==========================================================
// Per-device background jobs.
//

#define LOG_STATS_INTERVAL_sec 20

void
ssd_log_stats(drv_ssd *ssd, uint64_t *p_prev_n_total_writes,
		uint64_t *p_prev_n_defrag_reads, uint64_t *p_prev_n_defrag_writes,
		uint64_t *p_prev_n_defrag_io_skips, uint64_t *p_prev_n_direct_frees,
		uint64_t *p_prev_n_tomb_raider_reads)
{
	uint64_t n_defrag_reads = as_load_uint64(&ssd->n_defrag_wblock_reads);
	uint64_t n_defrag_writes = as_load_uint64(&ssd->n_defrag_wblock_writes);
	uint64_t n_defrag_partial_writes =
			as_load_uint64(&ssd->n_defrag_wblock_partial_writes);

	uint64_t n_total_writes = n_defrag_writes;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		n_total_writes += ssd->current_swbs[c].n_wblock_writes;
	}

	uint64_t n_total_partial_writes = n_defrag_partial_writes;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		n_total_partial_writes += ssd->current_swbs[c].n_wblock_partial_writes;
	}

	uint64_t n_defrag_io_skips = as_load_uint64(&ssd->n_wblock_defrag_io_skips);
	uint64_t n_direct_frees = as_load_uint64(&ssd->n_wblock_direct_frees);

	float total_write_rate = (float)(n_total_writes - *p_prev_n_total_writes) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_read_rate = (float)(n_defrag_reads - *p_prev_n_defrag_reads) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_write_rate = (float)(n_defrag_writes - *p_prev_n_defrag_writes) /
			(float)LOG_STATS_INTERVAL_sec;

	float defrag_io_skip_rate = (float)(n_defrag_io_skips - *p_prev_n_defrag_io_skips) /
			(float)LOG_STATS_INTERVAL_sec;
	float direct_free_rate = (float)(n_direct_frees - *p_prev_n_direct_frees) /
			(float)LOG_STATS_INTERVAL_sec;

	uint64_t n_tomb_raider_reads = ssd->n_tomb_raider_reads;
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

	*shadow_str = 0;

	if (ssd->shadow_name) {
		sprintf(shadow_str, " shadow-write-q %u",
				cf_queue_sz(ssd->swb_shadow_q));
	}

	uint32_t free_wblock_q_sz = cf_queue_sz(ssd->free_wblock_q);
	uint32_t n_pristine_wblocks = num_pristine_wblocks(ssd);
	uint32_t n_free_wblocks = free_wblock_q_sz + n_pristine_wblocks;

	cf_info(AS_DRV_SSD, "{%s} %s: used-bytes %lu free-wblocks %u write-q %u write (%lu,%.1f) defrag-q %u defrag-read (%lu,%.1f) defrag-write (%lu,%.1f)%s%s",
			ssd->ns->name, ssd->name,
			ssd->inuse_size, n_free_wblocks,
			cf_queue_sz(ssd->swb_write_q),
			n_total_writes, total_write_rate,
			cf_queue_sz(ssd->defrag_wblock_q), n_defrag_reads, defrag_read_rate,
			n_defrag_writes, defrag_write_rate,
			shadow_str, tomb_raider_str);

	cf_detail(AS_DRV_SSD, "{%s} %s: free-wblocks (%u,%u) defrag-io-skips (%lu,%.1f) direct-frees (%lu,%.1f) partial-writes %lu defrag-partial-writes %lu",
			ssd->ns->name, ssd->name,
			free_wblock_q_sz, n_pristine_wblocks,
			n_defrag_io_skips, defrag_io_skip_rate,
			n_direct_frees, direct_free_rate,
			n_total_partial_writes, n_defrag_partial_writes);

	*p_prev_n_total_writes = n_total_writes;
	*p_prev_n_defrag_reads = n_defrag_reads;
	*p_prev_n_defrag_writes = n_defrag_writes;
	*p_prev_n_defrag_io_skips = n_defrag_io_skips;
	*p_prev_n_direct_frees = n_direct_frees;
	*p_prev_n_tomb_raider_reads = n_tomb_raider_reads;

	if (n_free_wblocks == 0) {
		cf_warning(AS_DRV_SSD, "device %s: out of storage space", ssd->name);
	}
}


void
ssd_free_swbs(drv_ssd *ssd)
{
	// Try to recover swbs, 16 at a time, down to 16.
	for (uint32_t i = 0; i < 16 && cf_queue_sz(ssd->swb_free_q) > 16; i++) {
		ssd_write_buf* swb;

		if (CF_QUEUE_OK !=
				cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
			break;
		}

		swb_destroy(swb);
	}
}


void
ssd_flush_current_swb(drv_ssd *ssd, uint8_t which, uint64_t *p_prev_n_writes)
{
	current_swb *cur_swb = &ssd->current_swbs[which];
	uint64_t n_writes = as_load_uint64(&cur_swb->n_wblock_writes);

	// If there's an active write load, we don't need to flush.
	if (n_writes != *p_prev_n_writes) {
		*p_prev_n_writes = n_writes;
		return;
	}

	cf_mutex_lock(&cur_swb->lock);

	n_writes = as_load_uint64(&cur_swb->n_wblock_writes);

	// Must check under the lock, could be racing a current swb just queued.
	if (n_writes != *p_prev_n_writes) {

		cf_mutex_unlock(&cur_swb->lock);

		*p_prev_n_writes = n_writes;
		return;
	}

	// Flush the current swb if it isn't empty, and has been written to since
	// last flushed.

	ssd_write_buf *swb = cur_swb->swb;

	uint64_t write_offset = 0;
	size_t write_sz = 0;
	uint8_t *buf = NULL;

	if (swb != NULL && swb->pos != swb->flush_pos) {
		ssd_wait_writers_done(swb);

		write_offset = WBLOCK_ID_TO_OFFSET(swb->wblock_id) + swb->flush_pos;
		write_sz = swb->pos - swb->flush_pos;

		buf = ssd_encrypt_wblock(swb, write_offset) + swb->flush_pos;

		swb->flush_pos = swb->pos;

		as_incr_uint32(&swb->n_writers);
	}

	cf_mutex_unlock(&cur_swb->lock);

	if (write_offset != 0) {
		// Flush it.
		ssd_flush_buf(ssd, buf, write_offset, write_sz);

		if (ssd->shadow_name != NULL) {
			ssd_shadow_flush_buf(ssd, buf, write_offset, write_sz);
		}

		as_decr_uint32_rls(&swb->n_writers);

		cur_swb->n_wblock_partial_writes++;
	}
}


void
ssd_flush_defrag_swb(drv_ssd *ssd, uint64_t *p_prev_n_defrag_writes)
{
	uint64_t n_defrag_writes = as_load_uint64(&ssd->n_defrag_wblock_writes);

	// If there's an active defrag load, we don't need to flush.
	if (n_defrag_writes != *p_prev_n_defrag_writes) {
		*p_prev_n_defrag_writes = n_defrag_writes;
		return;
	}

	cf_mutex_lock(&ssd->defrag_lock);

	n_defrag_writes = as_load_uint64(&ssd->n_defrag_wblock_writes);

	// Must check under the lock, could be racing a current swb just queued.
	if (n_defrag_writes != *p_prev_n_defrag_writes) {

		cf_mutex_unlock(&ssd->defrag_lock);

		*p_prev_n_defrag_writes = n_defrag_writes;
		return;
	}

	// Flush the defrag swb if it isn't empty, and has been written to since
	// last flushed.

	ssd_write_buf *swb = ssd->defrag_swb;

	if (swb != NULL && swb->n_vacated != 0) {
		uint64_t write_offset = WBLOCK_ID_TO_OFFSET(swb->wblock_id) +
				swb->flush_pos;
		size_t write_sz = swb->pos - swb->flush_pos;

		uint8_t *buf = ssd_encrypt_wblock(swb, write_offset) + swb->flush_pos;

		swb->flush_pos = swb->pos;

		ssd_flush_buf(ssd, buf, write_offset, write_sz);

		if (ssd->shadow_name != NULL) {
			ssd_shadow_flush_buf(ssd, buf, write_offset, write_sz);
		}

		ssd->n_defrag_wblock_partial_writes++;

		// The whole point - free source wblocks, sets n_vacated to 0.
		swb_release_all_vacated_wblocks(swb);
	}

	cf_mutex_unlock(&ssd->defrag_lock);
}


// Check all wblocks to load a device's defrag queue at runtime. Triggered only
// when defrag-lwm-pct is increased by manual intervention.
void
ssd_defrag_sweep(drv_ssd *ssd)
{
	uint32_t first_id = ssd->first_wblock_id;
	uint32_t end_id = ssd->n_wblocks;
	uint32_t n_queued = 0;

	for (uint32_t wblock_id = first_id; wblock_id < end_id; wblock_id++) {
		ssd_wblock_state *p_wblock_state = &ssd->wblock_state[wblock_id];

		cf_mutex_lock(&p_wblock_state->LOCK);

		if (p_wblock_state->state == WBLOCK_STATE_USED &&
				p_wblock_state->inuse_sz < ssd->ns->defrag_lwm_size &&
				! p_wblock_state->short_lived) {
			push_wblock_to_defrag_q(ssd, wblock_id);
			n_queued++;
		}

		cf_mutex_unlock(&p_wblock_state->LOCK);
	}

	cf_info(AS_DRV_SSD, "... %s sweep queued %u wblocks for defrag", ssd->name,
			n_queued);
}


static inline uint64_t
next_time(uint64_t now, uint64_t job_interval, uint64_t next)
{
	uint64_t next_job = now + job_interval;

	return next_job < next ? next_job : next;
}


// All in microseconds since we're using usleep().
#define MAX_INTERVAL		(1000 * 1000)
#define LOG_STATS_INTERVAL	(1000 * 1000 * LOG_STATS_INTERVAL_sec)
#define FREE_SWBS_INTERVAL	(1000 * 1000 * 20)

// Thread "run" function to perform various background jobs per device.
void *
run_ssd_maintenance(void *udata)
{
	drv_ssd *ssd = (drv_ssd*)udata;
	as_namespace *ns = ssd->ns;

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
	uint64_t prev_free_swbs = now;
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
			ssd_log_stats(ssd, &prev_n_total_writes, &prev_n_defrag_reads,
					&prev_n_defrag_writes, &prev_n_defrag_io_skips,
					&prev_n_direct_frees, &prev_n_tomb_raider_reads);
			prev_log_stats = now;
			next = next_time(now, LOG_STATS_INTERVAL, next);
		}

		if (now >= prev_free_swbs + FREE_SWBS_INTERVAL) {
			ssd_free_swbs(ssd);
			prev_free_swbs = now;
			next = next_time(now, FREE_SWBS_INTERVAL, next);
		}

		uint64_t flush_max_us = ssd_flush_max_us(ns);

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			if (flush_max_us != 0 && now >= prev_flush[c] + flush_max_us) {
				ssd_flush_current_swb(ssd, c, &prev_n_writes_flush[c]);
				prev_flush[c] = now;
				next = next_time(now, flush_max_us, next);
			}
		}

		static const uint64_t DEFRAG_FLUSH_MAX_US = 3UL * 1000 * 1000; // 3 sec

		if (now >= prev_defrag_flush + DEFRAG_FLUSH_MAX_US) {
			ssd_flush_defrag_swb(ssd, &prev_n_defrag_writes_flush);
			prev_defrag_flush = now;
			next = next_time(now, DEFRAG_FLUSH_MAX_US, next);
		}

		if (ssd->defrag_sweep != 0) {
			// May take long enough to mess up other jobs' schedules, but it's a
			// very rare manually-triggered intervention.
			ssd_defrag_sweep(ssd);
			as_decr_uint32(&ssd->defrag_sweep);
		}

		now = cf_getus(); // refresh in case jobs took significant time
		sleep_us = next > now ? next - now : 1;
	}

	return NULL;
}


void
ssd_start_maintenance_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} starting device maintenance threads",
			ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		cf_thread_create_detached(run_ssd_maintenance, (void*)ssd);
	}
}


//==========================================================
// Device header utilities.
//

drv_header *
ssd_read_header(drv_ssd *ssd)
{
	as_namespace *ns = ssd->ns;

	bool use_shadow = ns->cold_start && ssd->shadow_name;

	const char *ssd_name;
	int fd;
	size_t read_size;

	if (use_shadow) {
		ssd_name = ssd->shadow_name;
		fd = ssd_shadow_fd_get(ssd);
		read_size = BYTES_UP_TO_SHADOW_IO_MIN(ssd, sizeof(drv_header));
	}
	else {
		ssd_name = ssd->name;
		fd = ssd_fd_get(ssd);
		read_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(drv_header));
	}

	drv_header *header = cf_valloc(read_size);

	if (! pread_all(fd, (void*)header, read_size, 0)) {
		cf_crash(AS_DRV_SSD, "%s: read failed: errno %d (%s)", ssd_name, errno,
				cf_strerror(errno));
	}

	drv_prefix *prefix = &header->generic.prefix;

	// Normal path for a fresh drive.
	if (prefix->magic != DRV_HEADER_MAGIC) {
		if (! cf_memeq(header, 0, read_size)) {
			cf_crash(AS_DRV_SSD, "%s: not an Aerospike device but not erased - check config or erase device",
					ssd_name);
		}

		cf_detail(AS_DRV_SSD, "%s: zero magic - fresh device", ssd_name);
		cf_free(header);
		use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
		return NULL;
	}

	if (prefix->version != DRV_VERSION) {
		if (prefix->version == 3) { // 2 & 1 weren't at this location on device
			cf_crash(AS_DRV_SSD, "%s: Aerospike device has old format - must erase device to upgrade",
					ssd_name);
		}

		cf_crash(AS_DRV_SSD, "%s: unknown version %u", ssd_name,
				prefix->version);
	}

	if (strcmp(prefix->namespace, ns->name) != 0) {
		cf_crash(AS_DRV_SSD, "%s: previous namespace %s now %s - check config or erase device",
				ssd_name, prefix->namespace, ns->name);
	}

	if (prefix->n_devices > AS_STORAGE_MAX_DEVICES) {
		cf_crash(AS_DRV_SSD, "%s: bad n-devices %u", ssd_name,
				prefix->n_devices);
	}

	if (prefix->random == 0) {
		cf_crash(AS_DRV_SSD, "%s: random signature is 0", ssd_name);
	}

	if (prefix->write_block_size == 0 ||
			WBLOCK_SZ % prefix->write_block_size != 0) {
		cf_crash(AS_DRV_SSD, "%s: can't change write-block-size from %u to %u",
				ssd_name, prefix->write_block_size, WBLOCK_SZ);
	}

	if (header->unique.device_id >= AS_STORAGE_MAX_DEVICES) {
		cf_crash(AS_DRV_SSD, "%s: bad device-id %u", ssd_name,
				header->unique.device_id);
	}

	// If shadow shut down cleanly, cold start off local drive if it's there.
	// Note - before ssd_header_validate_cfg() which might write to header.
	if (use_shadow && (prefix->flags & DRV_HEADER_FLAG_TRUSTED) != 0) {
		int local_fd = ssd_fd_get(ssd);
		size_t local_read_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(drv_header));
		drv_header *local_header = cf_valloc(local_read_size);

		if (! pread_all(local_fd, (void*)local_header, local_read_size, 0)) {
			cf_crash(AS_DRV_SSD, "%s: local header read failed: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		// Shadow value can be different - exclude from comparison.
		local_header->unique.pristine_offset = header->unique.pristine_offset;

		size_t sz = local_read_size < read_size ? local_read_size : read_size;

		// If the local device header matches, we can use it for cold start.
		if (memcmp(local_header, header, sz) == 0) {
			ssd->cold_start_local = true;
		}

		cf_free(local_header);
		ssd_fd_put(ssd, local_fd);
	}

	ssd_header_validate_cfg(ns, ssd, header);

	if (header->unique.pristine_offset != 0 && // can be 0 on shadow
			(header->unique.pristine_offset < DRV_HEADER_SIZE ||
					header->unique.pristine_offset > ssd->file_size)) {
		cf_crash(AS_DRV_SSD, "%s: bad pristine offset %lu", ssd_name,
				header->unique.pristine_offset);
	}

	prefix->write_block_size = WBLOCK_SZ;

	use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);

	return header;
}


drv_header *
ssd_init_header(as_namespace *ns, drv_ssd *ssd)
{
	drv_header *header = cf_malloc(sizeof(drv_header));

	memset(header, 0, sizeof(drv_header));

	drv_prefix *prefix = &header->generic.prefix;

	// Set non-zero common fields.
	prefix->magic = DRV_HEADER_MAGIC;
	prefix->version = DRV_VERSION;
	strcpy(prefix->namespace, ns->name);
	prefix->write_block_size = WBLOCK_SZ;

	ssd_header_init_cfg(ns, ssd, header);

	return header;
}


void
ssd_empty_header(int fd, const char* device_name)
{
	void *h = cf_valloc(DRV_HEADER_SIZE);

	memset(h, 0, DRV_HEADER_SIZE);

	if (! pwrite_all(fd, h, DRV_HEADER_SIZE, 0)) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				device_name, errno, cf_strerror(errno));
	}

	cf_free(h);
}


void
ssd_write_header(drv_ssd *ssd, uint8_t *header, uint8_t *from, size_t size)
{
	off_t offset = from - header;

	off_t flush_offset = BYTES_DOWN_TO_IO_MIN(ssd, offset);
	off_t flush_end_offset = BYTES_UP_TO_IO_MIN(ssd, offset + size);

	uint8_t *flush = header + flush_offset;
	size_t flush_sz = flush_end_offset - flush_offset;

	int fd = ssd_fd_get(ssd);

	if (! pwrite_all(fd, (void*)flush, flush_sz, flush_offset)) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	ssd_fd_put(ssd, fd);

	if (! ssd->shadow_name) {
		return;
	}

	flush_offset = BYTES_DOWN_TO_SHADOW_IO_MIN(ssd, offset);
	flush_end_offset = BYTES_UP_TO_SHADOW_IO_MIN(ssd, offset + size);

	flush = header + flush_offset;
	flush_sz = flush_end_offset - flush_offset;

	fd = ssd_shadow_fd_get(ssd);

	if (! pwrite_all(fd, (void*)flush, flush_sz, flush_offset)) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	ssd_shadow_fd_put(ssd, fd);
}


//==========================================================
// Cold start utilities.
//

bool
prefer_existing_record(const as_namespace* ns, const as_flat_record* flat,
		uint32_t block_void_time, const as_index* r)
{
	int result = as_record_resolve_conflict(ssd_cold_start_policy(ns),
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


// Add a record just read from drive to the index, if all is well.
void
ssd_cold_start_add_record(drv_ssds* ssds, drv_ssd* ssd,
		const as_flat_record* flat, uint64_t rblock_id, uint32_t record_size)
{
	uint32_t pid = as_partition_getid(&flat->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! ssds->get_state_from_storage[pid]) {
		return;
	}

	as_namespace* ns = ssds->ns;
	as_partition* p_partition = &ns->partitions[pid];

	// Includes round rblock padding, so may not literally exclude the mark.
	const uint8_t* end = (const uint8_t*)flat + record_size - END_MARK_SZ;

	as_flat_opt_meta opt_meta = { { 0 } };

	const uint8_t* p_read = as_flat_unpack_record_meta(flat, end, &opt_meta);

	if (! p_read) {
		cf_warning(AS_DRV_SSD, "bad metadata for %pD", &flat->keyd);
		ssd->record_add_unparsable_counter++;
		return;
	}

	if (opt_meta.void_time > ns->startup_max_void_time) {
		cf_warning(AS_DRV_SSD, "bad void-time for %pD", &flat->keyd);
		ssd->record_add_unparsable_counter++;
		return;
	}

	const uint8_t* cb_end = NULL;

	if (! as_flat_decompress_buffer(&opt_meta.cm, WBLOCK_SZ, &p_read, &end,
			&cb_end)) {
		cf_warning(AS_DRV_SSD, "bad compressed data for %pD", &flat->keyd);
		ssd->record_add_unparsable_counter++;
		return;
	}

	const uint8_t* exact_end = as_flat_check_packed_bins(p_read, end,
			opt_meta.n_bins);

	if (exact_end == NULL) {
		cf_warning(AS_DRV_SSD, "bad flat record %pD", &flat->keyd);
		ssd->record_add_unparsable_counter++;
		return;
	}

	if (! drv_check_end_mark(cb_end == NULL ? exact_end : cb_end, flat)) {
		cf_warning(AS_DRV_SSD, "bad end marker for %pD", &flat->keyd);
		ssd->record_add_unparsable_counter++;
		return;
	}

	// Ignore record if it was in a dropped tree.
	if (flat->tree_id != p_partition->tree_id) {
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
		cf_crash(AS_DRV_SSD, "hit stop-writes limit before drive scan completed");
	}

	// Get/create the record from/in the appropriate index tree.
	as_index_ref r_ref;
	int rv = as_record_get_create(p_partition->tree, &flat->keyd, &r_ref, ns);

	if (rv < 0) {
		cf_crash(AS_DRV_SSD, "{%s} can't add record to index", ns->name);
	}

	bool is_create = rv == 1;

	as_index* r = r_ref.r;

	if (! is_create) {
		// Record already existed. Ignore this one if existing record is newer.
		if (prefer_existing_record(ns, flat, opt_meta.void_time, r)) {
			ssd_cold_start_fill_orig(ssd, flat, rblock_id, &opt_meta,
					p_partition->tree, &r_ref);
			ssd_cold_start_adjust_cenotaph(ns, flat, opt_meta.void_time, r);
			as_record_done(&r_ref, ns);
			ssd->record_add_older_counter++;
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
		ssd->record_add_expired_counter++;
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
		ssd->record_add_evicted_counter++;
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

		ssd->record_add_unique_counter++;
	}
	else {
		ssd_cold_start_record_update(ssds, flat, &opt_meta, p_partition->tree,
				&r_ref);

		ssd->record_add_replace_counter++;
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
	ssd_cold_start_init_repl_state(ns, r);
	ssd_cold_start_init_xdr_state(flat, r);

	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(rblock_id);

	if (is_mrt_provisional(r) || is_mrt_monitor_write(ns, r)) {
		ssd->wblock_state[wblock_id].short_lived = true;
	}

	ssd->inuse_size += record_size;
	ssd->wblock_state[wblock_id].inuse_sz += record_size;

	// Set/reset the record's storage information.
	r->file_id = ssd->file_id;
	r->rblock_id = rblock_id;

	as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
			DELTA_N_RBLOCKS_TO_SIZE(flat->n_rblocks, r->n_rblocks));

	r->n_rblocks = flat->n_rblocks;

	as_record_done(&r_ref, ns);
}


// Sweep through a storage device to rebuild the index.
void
ssd_cold_start_sweep(drv_ssds *ssds, drv_ssd *ssd)
{
	if (ssd->shadow_name != NULL && ! ssd->cold_start_local) {
		cf_info(AS_DRV_SSD, "device %s: reading shadow device %s to load index",
				ssd->name, ssd->shadow_name);
	}
	else {
		cf_info(AS_DRV_SSD, "device %s: reading device to load index",
				ssd->name);
	}

	uint8_t *buf = cf_valloc(WBLOCK_SZ);

	bool read_shadow = ssd->shadow_name && ! ssd->cold_start_local;
	const char *read_ssd_name = read_shadow ? ssd->shadow_name : ssd->name;
	int fd = read_shadow ? ssd_shadow_fd_get(ssd) : ssd_fd_get(ssd);
	int write_fd = read_shadow ? ssd_fd_get(ssd) : -1;

	// Loop over all wblocks, unless we encounter 10 contiguous unused wblocks.

	ssd->sweep_wblock_id = ssd->first_wblock_id;

	uint64_t file_offset = DRV_HEADER_SIZE;
	uint32_t n_unused_wblocks = 0;

	bool prefetch = cf_arenax_want_prefetch(ssd->ns->arena);

	while (file_offset < ssd->file_size && n_unused_wblocks < 10) {
		if (! pread_all(fd, buf, WBLOCK_SZ, (off_t)file_offset)) {
			cf_crash(AS_DRV_SSD, "%s: read failed: errno %d (%s)",
					read_ssd_name, errno, cf_strerror(errno));
		}

		if (read_shadow && ! pwrite_all(write_fd, (void*)buf, WBLOCK_SZ,
				(off_t)file_offset)) {
			cf_crash(AS_DRV_SSD, "%s: write failed: errno %d (%s)", ssd->name,
					errno, cf_strerror(errno));
		}

		if (prefetch) {
			ssd_prefetch_wblock(ssd, file_offset, buf);
		}

		uint32_t indent = 0; // current offset within wblock, in bytes

		while (indent < WBLOCK_SZ) {
			as_flat_record *flat = (as_flat_record*)&buf[indent];

			if (! prefetch) {
				ssd_decrypt(ssd, file_offset + indent, flat);
			}

			// Look for record magic.
			if (flat->magic != AS_FLAT_MAGIC) {
				// Should always find a record at beginning of used wblock. if
				// not, we've likely encountered the unused part of the device.
				if (indent == 0) {
					n_unused_wblocks++;
					break; // try next wblock
				}
				// else - nothing more in this wblock, but keep looking for
				// magic - necessary if we want to be able to increase
				// write-block-size across restarts.

				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			if (n_unused_wblocks != 0) {
				cf_warning(AS_DRV_SSD, "%s: found used wblock after skipping %u unused",
						ssd->name, n_unused_wblocks);

				n_unused_wblocks = 0; // restart contiguous count
			}

			uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
			uint32_t next_indent = indent + record_size;

			if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
				cf_warning(AS_DRV_SSD, "%s: bad record size %u", ssd->name,
						record_size);
				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			// Found a record - try to add it to the index.
			ssd_cold_start_add_record(ssds, ssd, flat,
					OFFSET_TO_RBLOCK_ID(file_offset + indent), record_size);

			indent = next_indent;
		}

		file_offset += WBLOCK_SZ;
		ssd->sweep_wblock_id++;
	}

	ssd->pristine_wblock_id = ssd->sweep_wblock_id - n_unused_wblocks;

	ssd->sweep_wblock_id = (uint32_t)(ssd->file_size / WBLOCK_SZ);

	if (fd != -1) {
		read_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
	}

	if (write_fd != -1) {
		ssd_fd_put(ssd, write_fd);
	}

	cf_free(buf);

	cf_info(AS_DRV_SSD, "device %s: read complete: UNIQUE %lu (REPLACED %lu) (OLDER %lu) (EXPIRED %lu) (EVICTED %lu) (UNPARSABLE %lu) records",
			ssd->name, ssd->record_add_unique_counter,
			ssd->record_add_replace_counter, ssd->record_add_older_counter,
			ssd->record_add_expired_counter,
			ssd->record_add_evicted_counter,
			ssd->record_add_unparsable_counter);
}


// Thread "run" function to read a storage device and rebuild the index.
void *
run_ssd_cold_start(void *udata)
{
	ssd_load_records_info *lri = (ssd_load_records_info*)udata;
	drv_ssd *ssd = lri->ssd;
	drv_ssds *ssds = lri->ssds;
	cf_queue *complete_q = lri->complete_q;
	void *complete_rc = lri->complete_rc;

	cf_free(lri);

	ssd_cold_start_sweep_device(ssds, ssd);

	if (cf_rc_release(complete_rc) == 0) {
		// All drives are done reading.

		as_namespace* ns = ssds->ns;

		if (drv_cold_start_sweeps_done(ns)) {
			ns->loading_records = false;
			ssd_cold_start_drop_cenotaphs(ns);

			cf_mutex_destroy(&ns->cold_start_evict_lock);

			as_truncate_list_cenotaphs(ns);
			as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap

			ssd_cold_start_set_unrepl_stat(ns);
		}

		void *_t = NULL;

		cf_queue_push(complete_q, &_t);
		cf_rc_free(complete_rc);
	}

	return NULL;
}


void
start_loading_records(drv_ssds *ssds, cf_queue *complete_q)
{
	as_namespace *ns = ssds->ns;

	drv_mrt_create_cold_start_hash(ns);

	ns->loading_records = true;

	void *p = cf_rc_alloc(1);

	for (int i = 1; i < ssds->n_ssds; i++) {
		cf_rc_reserve(p);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		ssd_load_records_info *lri = cf_malloc(sizeof(ssd_load_records_info));

		lri->ssds = ssds;
		lri->ssd = ssd;
		lri->complete_q = complete_q;
		lri->complete_rc = p;

		cf_thread_create_transient(run_ssd_cold_start, (void*)lri);
	}
}


//==========================================================
// Sindex startup utilities.
//

#define PROGRESS_RESOLUTION 1000

static void* run_si_startup(void* udata);
static void si_startup_sweep(drv_ssds* ssds, drv_ssd* ssd);
static void si_startup_do_record(drv_ssds* ssds, drv_ssd* ssd,
		as_flat_record* flat, uint64_t rblock_id, uint32_t record_size);

static void*
run_si_startup(void* udata)
{
	drv_ssd* ssd = (drv_ssd*)udata;
	drv_ssds* ssds = (drv_ssds*)ssd->ns->storage_private;

	// Reuse counter - may expire more records while building sindex.
	ssd->record_add_expired_counter = 0;

	// Reuse counter - now for progress in sindex ticker.
	ssd->record_add_unique_counter = 0;

	si_startup_sweep(ssds, ssd);

	cf_info(AS_DRV_SSD, "device %s: sindex sweep complete: expired %lu",
			ssd->name, ssd->record_add_expired_counter);

	return NULL;
}

static void
si_startup_sweep(drv_ssds* ssds, drv_ssd* ssd)
{
	uint8_t* buf = cf_valloc(WBLOCK_SZ);
	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = DRV_HEADER_SIZE;

	bool prefetch = cf_arenax_want_prefetch(ssd->ns->arena);

	// Loop over all wblocks (excluding header) in device.
	for (ssd->sweep_wblock_id = ssd->first_wblock_id;
			ssd->sweep_wblock_id < ssd->n_wblocks;
			ssd->sweep_wblock_id++, file_offset += WBLOCK_SZ) {

		// Don't read unused wblocks.
		if (ssd->wblock_state[ssd->sweep_wblock_id].inuse_sz == 0) {
			continue;
		}

		if (! pread_all(fd, buf, WBLOCK_SZ, (off_t)file_offset)) {
			cf_crash(AS_DRV_SSD, "%s: read failed: errno %d (%s)", ssd->name,
					errno, cf_strerror(errno));
		}

		if (prefetch) {
			ssd_prefetch_wblock(ssd, file_offset, buf);
		}

		uint32_t indent = 0; // current offset within the wblock, in bytes

		while (indent < WBLOCK_SZ) {
			as_flat_record* flat = (as_flat_record*)&buf[indent];

			if (! prefetch) {
				ssd_decrypt(ssd, file_offset + indent, flat);
			}

			// Look for record magic.
			if (flat->magic != AS_FLAT_MAGIC) {
				// Should always find a record at beginning of used wblock.
				cf_assert(indent != 0, AS_DRV_SSD, "%s: no magic at beginning of used wblock %u",
						ssd->name, ssd->sweep_wblock_id);

				// Nothing more in this wblock, but keep looking for magic -
				// necessary for increased write-block-size (upgrade from
				// pre-7.1) across restarts.
				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
			uint32_t next_indent = indent + record_size;

			if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
				cf_warning(AS_DRV_SSD, "%s: bad record size %u", ssd->name,
						record_size);
				indent += RBLOCK_SIZE;
				continue; // try next rblock
			}

			// Found a record - verify it's in the index, and process it.
			si_startup_do_record(ssds, ssd, flat,
					OFFSET_TO_RBLOCK_ID(file_offset + indent), record_size);

			indent = next_indent;
		}
	}

	ssd_fd_put(ssd, fd);
	cf_free(buf);
}

static void
si_startup_do_record(drv_ssds* ssds, drv_ssd* ssd, as_flat_record* flat,
		uint64_t rblock_id, uint32_t record_size)
{
	uint32_t pid = as_partition_getid(&flat->keyd);

	// Ignore records in trees that we don't own.
	if (! ssds->get_state_from_storage[pid]) {
		return;
	}

	as_namespace* ns = ssds->ns;

	// Includes round rblock padding, so may not literally exclude the mark.
	const uint8_t* end = (const uint8_t*)flat + record_size - END_MARK_SZ;

	as_flat_opt_meta opt_meta = { { 0 } };

	const uint8_t* p_read = as_flat_unpack_record_meta(flat, end, &opt_meta);

	if (! p_read) {
		cf_warning(AS_DRV_SSD, "bad metadata for %pD", &flat->keyd);
		return;
	}

	if (! as_flat_decompress_buffer(&opt_meta.cm, WBLOCK_SZ, &p_read, &end,
			NULL)) {
		cf_warning(AS_DRV_SSD, "bad compressed data for %pD", &flat->keyd);
		return;
	}

	if (! ns->cold_start &&
			as_flat_check_packed_bins(p_read, end, opt_meta.n_bins) == NULL) {
		cf_warning(AS_DRV_SSD, "bad flat record %pD", &flat->keyd);
		return;
	}

	as_partition* p = &ns->partitions[pid];

	as_index_ref r_ref;

	if (as_record_get(p->tree, &flat->keyd, &r_ref) != 0) {
		return; // record not in index, move along
	}

	as_record* r = read_r(ns, r_ref.r, false); // is not an MRT

	if (r == NULL) {
		as_record_done(&r_ref, ns);
		return; // provisional without original, not indexed
	}

	// From here, r may be the orig_r if flat is a provisional.

	if (! as_record_is_live(r)) {
		as_record_done(&r_ref, ns);
		return; // tombstone, not indexed
	}

	if (r->file_id != ssd->file_id || r->rblock_id != rblock_id) {
		as_record_done(&r_ref, ns);
		return; // not the current version of this record
	}
	// else - found the current version, load it into memory.

	// Sanity check current version metadata.
	if (r->n_rblocks != flat->n_rblocks ||
			r->last_update_time != flat->last_update_time ||
			r->generation != flat->generation ||
			r->xdr_write != flat->xdr_write ||
			r->xdr_tombstone != opt_meta.extra_flags.xdr_tombstone ||
			r->xdr_nsup_tombstone != opt_meta.extra_flags.xdr_nsup_tombstone ||
			r->xdr_bin_cemetery != opt_meta.extra_flags.xdr_bin_cemetery ||
			r->void_time != opt_meta.void_time) {
		cf_warning(AS_DRV_SSD, "metadata mismatch - removing %pD", &flat->keyd);
		as_set_index_delete_live(ns, p->tree, r, r_ref.r_h);
		as_index_delete(p->tree, &flat->keyd);
		as_record_done(&r_ref, ns);
		return;
	}

	if (++ssd->record_add_unique_counter == PROGRESS_RESOLUTION) {
		as_add_uint64(&ns->si_n_recs_checked, PROGRESS_RESOLUTION);
		ssd->record_add_unique_counter = 0;
	}

	// Skip records that have expired since resuming the index.
	if (as_record_is_expired(r) && ! is_mrt_original(r)) {
		// AER-6363 - use live for "six months" in case of tombstone with TTL.
		as_set_index_delete_live(ns, p->tree, r, r_ref.r_h);
		as_index_delete(p->tree, &flat->keyd);
		as_record_done(&r_ref, ns);
		ssd->record_add_expired_counter++;
		return;
	}

	if (! set_has_sindex(r, ns)) {
		as_record_done(&r_ref, ns);
		return; // not in a sindex
	}

	// Load bins and particles, load sindex. We are NOT data-in-memory!

	uint16_t n_bins = (uint16_t)opt_meta.n_bins;
	as_bin bins[n_bins];

	if (as_flat_unpack_bins(ns, p_read, end, n_bins, bins) < 0) {
		cf_crash(AS_DRV_SSD, "unpack bins failed");
	}

	update_sindex(ns, &r_ref, NULL, 0, bins, n_bins);

	as_record_done(&r_ref, ns);
}


//==========================================================
// Generic startup utilities.
//

static void
ssd_flush_header(drv_ssds *ssds, drv_header **headers)
{
	uint8_t* buf = cf_valloc(DRV_HEADER_SIZE);

	memset(buf, 0, DRV_HEADER_SIZE);
	memcpy(buf, ssds->generic, sizeof(drv_generic));

	for (int i = 0; i < ssds->n_ssds; i++) {
		memcpy(buf + DRV_OFFSET_UNIQUE, &headers[i]->unique,
				sizeof(drv_unique));

		ssd_write_header(&ssds->ssds[i], buf, buf, DRV_HEADER_SIZE);
	}

	cf_free(buf);
}


// Not called for fresh devices, but called in all (warm/cold) starts.
static void
ssd_init_pristine_wblock_id(drv_ssd *ssd, uint64_t offset)
{
	if (offset == 0) {
		// Legacy device with data - flag to scan and find id on warm restart.
		ssd->pristine_wblock_id = 0;
		return;
	}

	// Round up, in case write-block-size was increased.
	ssd->pristine_wblock_id = (offset + (WBLOCK_SZ - 1)) / WBLOCK_SZ;
}


void
ssd_init_synchronous(drv_ssds *ssds)
{
	uint64_t random = 0;

	while (random == 0) {
		random = cf_get_rand64();
	}

	int n_ssds = ssds->n_ssds;
	as_namespace *ns = ssds->ns;

	drv_header *headers[n_ssds];
	int first_used = -1;

	// Check all the headers. Pick one as the representative.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		headers[i] = ssd_read_header(ssd);

		if (! headers[i]) {
			headers[i] = ssd_init_header(ns, ssd);
		}
		else if (first_used < 0) {
			first_used = i;
		}
	}

	ssd_clear_encryption_keys(ns);

	if (first_used < 0) {
		// Shouldn't find all fresh headers here during warm restart.
		if (! ns->cold_start) {
			// There's no going back to cold start now - do so the harsh way.
			cf_crash(AS_DRV_SSD, "{%s} found all %d devices fresh during warm restart",
					ns->name, n_ssds);
		}

		cf_info(AS_DRV_SSD, "{%s} found all %d devices fresh, initializing to random %lu",
				ns->name, n_ssds, random);

		ssds->generic = cf_valloc(ROUND_UP_GENERIC);
		memcpy(ssds->generic, &headers[0]->generic, ROUND_UP_GENERIC);

		ssds->generic->prefix.n_devices = n_ssds;
		ssds->generic->prefix.random = random;

		for (int i = 0; i < n_ssds; i++) {
			headers[i]->unique.device_id = (uint32_t)i;
		}

		drv_adjust_sc_version_flags(ns, ssds->generic->pmeta, true, false);

		ssd_flush_header(ssds, headers);

		for (int i = 0; i < n_ssds; i++) {
			cf_free(headers[i]);
		}

		as_truncate_list_cenotaphs(ns); // all will show as cenotaph
		as_truncate_done_startup(ns);

		ssds->all_fresh = true; // won't need to scan devices

		return;
	}

	// At least one device is not fresh. Check that all non-fresh devices match.

	uint32_t n_fresh_drives = 0;
	bool non_commit_drive = false;
	drv_prefix *prefix_first = &headers[first_used]->generic.prefix;

	memset(ssds->device_translation, -1, sizeof(ssds->device_translation));

	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		drv_prefix *prefix_i = &headers[i]->generic.prefix;
		uint32_t old_device_id = headers[i]->unique.device_id;

		headers[i]->unique.device_id = (uint32_t)i;

		// Skip fresh devices.
		if (prefix_i->random == 0) {
			cf_info(AS_DRV_SSD, "{%s} device %s is empty", ns->name, ssd->name);
			n_fresh_drives++;
			continue;
		}

		ssd_init_pristine_wblock_id(ssd, headers[i]->unique.pristine_offset);

		ssds->device_translation[old_device_id] = (int8_t)i;

		if (prefix_first->random != prefix_i->random) {
			cf_crash(AS_DRV_SSD, "{%s} drive set with unmatched headers - devices %s & %s have different signatures",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (prefix_first->n_devices != prefix_i->n_devices) {
			cf_crash(AS_DRV_SSD, "{%s} drive set with unmatched headers - devices %s & %s have different device counts",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if ((prefix_i->flags & DRV_HEADER_FLAG_TRUSTED) == 0) {
			cf_info(AS_DRV_SSD, "{%s} device %s prior shutdown not clean",
					ns->name, ssd->name);
			ns->dirty_restart = true;
		}

		if ((prefix_i->flags & DRV_HEADER_FLAG_COMMIT_TO_DEVICE) == 0) {
			non_commit_drive = true;
		}
	}

	// Drive set OK - fix up header set.
	ssds->generic = cf_valloc(ROUND_UP_GENERIC);
	memcpy(ssds->generic, &headers[first_used]->generic, ROUND_UP_GENERIC);

	ssds->generic->prefix.n_devices = n_ssds; // may have added/removed drives
	ssds->generic->prefix.random = random;
	ssds->generic->prefix.flags &= ~DRV_HEADER_FLAG_TRUSTED;

	drv_adjust_sc_version_flags(ns, ssds->generic->pmeta,
			n_ssds < prefix_first->n_devices + n_fresh_drives,
			ns->dirty_restart && non_commit_drive);

	ssd_flush_header(ssds, headers);
	ssd_flush_final_cfg(ns);

	for (int i = 0; i < n_ssds; i++) {
		cf_free(headers[i]);
	}

	uint32_t now = as_record_void_time_get();

	// Sanity check void-times during startup.
	ns->startup_max_void_time = now + MAX_ALLOWED_TTL;

	// Cache booleans indicating whether partitions are owned or not. Also
	// restore tree-ids - note that absent partitions do have tree-ids.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		drv_pmeta *pmeta = &ssds->generic->pmeta[pid];

		ssds->get_state_from_storage[pid] =
				as_partition_version_has_data(&pmeta->version);
		ns->partitions[pid].tree_id = pmeta->tree_id;
	}

	// Warm restart.
	if (! ns->cold_start) {
		as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap
		ssd_resume_devices(ssds);

		return; // warm restart is done
	}

	// Cold start - we can now create our partition trees.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		if (ssds->get_state_from_storage[pid]) {
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


static uint64_t
check_file_size(as_namespace *ns, uint64_t file_size, const char *tag)
{
	cf_assert(sizeof(off_t) > 4, AS_DRV_SSD, "this OS supports only 32-bit (4g) files - compile with 64 bit offsets");

	if (file_size > DRV_HEADER_SIZE) {
		off_t unusable_size = (file_size - DRV_HEADER_SIZE) % WBLOCK_SZ;

		if (unusable_size != 0) {
			cf_info(AS_DRV_SSD, "%s size must be header size %u + multiple of %u, rounding down",
					tag, DRV_HEADER_SIZE, WBLOCK_SZ);
			file_size -= unusable_size;
		}

		if (file_size > AS_STORAGE_MAX_DEVICE_SIZE) {
			cf_warning(AS_DRV_SSD, "%s size must be <= %ld, trimming original size %ld",
					tag, AS_STORAGE_MAX_DEVICE_SIZE, file_size);
			file_size = AS_STORAGE_MAX_DEVICE_SIZE;
		}
	}

	if (file_size <= DRV_HEADER_SIZE) {
		cf_crash(AS_DRV_SSD, "%s size %ld must be greater than header size %d",
				tag, file_size, DRV_HEADER_SIZE);
	}

	return file_size;
}


static uint64_t
find_io_min_size(int fd, const char *ssd_name)
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

	cf_crash(AS_DRV_SSD, "%s: read failed at all sizes from %u to %u bytes",
			ssd_name, LO_IO_MIN_SIZE, HI_IO_MIN_SIZE);

	return 0;
}


void
ssd_init_devices(as_namespace *ns, drv_ssds **ssds_p)
{
	size_t ssds_size = sizeof(drv_ssds) +
			(ns->n_storage_devices * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = (int)ns->n_storage_devices;
	ssds->ns = ns;

	// Raw device-specific initialization of drv_ssd structures.
	for (uint32_t i = 0; i < ns->n_storage_devices; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_devices[i];

		// Note - can't configure commit-to-device and disable-odsync.
		ssd->open_flag = O_RDWR | O_DIRECT |
				(ns->storage_disable_odsync ? 0 : O_DSYNC);

		int fd = open(ssd->name, ssd->open_flag);

		if (fd == -1) {
			cf_crash(AS_DRV_SSD, "unable to open device %s: %s", ssd->name,
					cf_strerror(errno));
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		ssd->file_size = check_file_size(ns, size, "usable device");
		ssd->io_min_size = find_io_min_size(fd, ssd->name);

		if (ns->cold_start && ns->storage_cold_start_empty) {
			ssd_empty_header(fd, ssd->name);

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->name);
		}

		close(fd);

		ns->drives_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened device %s: usable size %lu, io-min-size %lu",
				ssd->name, ssd->file_size, ssd->io_min_size);
	}

	*ssds_p = ssds;
}


void
ssd_init_shadow_devices(as_namespace *ns, drv_ssds *ssds)
{
	if (ns->n_storage_shadows == 0) {
		// No shadows - a normal deployment.
		return;
	}

	// Check shadow devices.
	for (uint32_t i = 0; i < ns->n_storage_shadows; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->shadow_name = ns->storage_shadows[i];

		int fd = open(ssd->shadow_name, ssd->open_flag);

		if (fd == -1) {
			cf_crash(AS_DRV_SSD, "unable to open shadow device %s: %s",
					ssd->shadow_name, cf_strerror(errno));
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		if (size < ssd->file_size) {
			cf_crash(AS_DRV_SSD, "shadow device %s is smaller than main device - %lu < %lu",
					ssd->shadow_name, size, ssd->file_size);
		}

		ssd->shadow_io_min_size = find_io_min_size(fd, ssd->shadow_name);

		if (ns->cold_start && ns->storage_cold_start_empty) {
			ssd_empty_header(fd, ssd->shadow_name);

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->shadow_name);
		}

		close(fd);

		cf_info(AS_DRV_SSD, "shadow device %s is compatible with main device, shadow-io-min-size %lu",
				ssd->shadow_name, ssd->shadow_io_min_size);
	}
}


void
ssd_init_files(as_namespace *ns, drv_ssds **ssds_p)
{
	size_t ssds_size = sizeof(drv_ssds) +
			(ns->n_storage_files * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = (int)ns->n_storage_files;
	ssds->ns = ns;

	// File-specific initialization of drv_ssd structures.
	for (uint32_t i = 0; i < ns->n_storage_files; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_devices[i];

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (unlink(ssd->name) == 0) {
				cf_info(AS_DRV_SSD, "cold-start-empty - removed %s", ssd->name);
			}
			else if (errno == ENOENT) {
				cf_info(AS_DRV_SSD, "cold-start-empty - no file %s", ssd->name);
			}
			else {
				cf_crash(AS_DRV_SSD, "failed remove: errno %d", errno);
			}
		}

		// Note - can't configure commit-to-device and disable-odsync.
		uint32_t direct_flags =
				O_DIRECT | (ns->storage_disable_odsync ? 0 : O_DSYNC);

		ssd->open_flag = O_RDWR |
				(ns->storage_commit_to_device || ns->storage_direct_files ?
						direct_flags : 0);

		// Validate that file can be opened, create it if it doesn't exist.
		int fd = open(ssd->name, ssd->open_flag | O_CREAT, cf_os_base_perms());

		if (fd == -1) {
			cf_crash(AS_DRV_SSD, "unable to open file %s: %s", ssd->name,
					cf_strerror(errno));
		}

		ssd->file_size = check_file_size(ns, ns->storage_filesize, "file");
		ssd->io_min_size = LO_IO_MIN_SIZE;

		// Truncate will grow or shrink the file to the correct size.
		if (ftruncate(fd, (off_t)ssd->file_size) != 0) {
			cf_crash(AS_DRV_SSD, "unable to truncate file: errno %d", errno);
		}

		close(fd);

		ns->drives_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened file %s: usable size %lu", ssd->name,
				ssd->file_size);
	}

	*ssds_p = ssds;
}


void
ssd_init_shadow_files(as_namespace *ns, drv_ssds *ssds)
{
	if (ns->n_storage_shadows == 0) {
		// No shadows - a normal deployment.
		return;
	}

	// Check shadow files.
	for (uint32_t i = 0; i < ns->n_storage_shadows; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->shadow_name = ns->storage_shadows[i];

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (unlink(ssd->shadow_name) == 0) {
				cf_info(AS_DRV_SSD, "cold-start-empty - removed %s",
						ssd->shadow_name);
			}
			else if (errno == ENOENT) {
				cf_info(AS_DRV_SSD, "cold-start-empty - no shadow file %s",
						ssd->shadow_name);
			}
			else {
				cf_crash(AS_DRV_SSD, "failed remove: errno %d", errno);
			}
		}

		// Validate that file can be opened, create it if it doesn't exist.
		int fd = open(ssd->shadow_name, ssd->open_flag | O_CREAT,
				cf_os_base_perms());

		if (fd == -1) {
			cf_crash(AS_DRV_SSD, "unable to open shadow file %s: %s",
					ssd->shadow_name, cf_strerror(errno));
		}

		// Truncate will grow or shrink the file to the correct size.
		if (ftruncate(fd, (off_t)ssd->file_size) != 0) {
			cf_crash(AS_DRV_SSD, "unable to truncate file: errno %d", errno);
		}

		ssd->shadow_io_min_size = LO_IO_MIN_SIZE;

		close(fd);

		cf_info(AS_DRV_SSD, "shadow file %s is initialized", ssd->shadow_name);
	}
}


//==========================================================
// Generic shutdown utilities.
//

static void
ssd_set_pristine_offset(drv_ssds *ssds)
{
	// Round down to nearest multiple of HI_IO_MIN_SIZE - for simplicity, using
	// HI_IO_MIN_SIZE to allocate once outside the loop.
	off_t offset = offsetof(drv_header, unique.pristine_offset) &
			-(uint64_t)HI_IO_MIN_SIZE;

	// pristine_offset is a uint64_t, must sit within HI_IO_MIN_SIZE of offset.
	drv_unique *header_unique = cf_valloc(HI_IO_MIN_SIZE);

	cf_mutex_lock(&ssds->flush_lock);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		int fd = ssd_fd_get(ssd);

		if (! pread_all(fd, (void *)header_unique, HI_IO_MIN_SIZE, offset)) {
			cf_crash(AS_DRV_SSD, "%s: read failed: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		header_unique->pristine_offset =
				(uint64_t)ssd->pristine_wblock_id * WBLOCK_SZ;

		if (! pwrite_all(fd, (void *)header_unique, HI_IO_MIN_SIZE, offset)) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}

		ssd_fd_put(ssd, fd);

		// Skip shadow - persisted offset never used at cold start.
	}

	cf_mutex_unlock(&ssds->flush_lock);

	cf_free(header_unique);
}


static void
ssd_set_trusted(drv_ssds *ssds)
{
	cf_mutex_lock(&ssds->flush_lock);

	ssds->generic->prefix.flags |= DRV_HEADER_FLAG_TRUSTED;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd_write_header(ssd, (uint8_t *)ssds->generic,
				(uint8_t *)&ssds->generic->prefix.flags,
				sizeof(ssds->generic->prefix.flags));
	}

	cf_mutex_unlock(&ssds->flush_lock);
}


//==========================================================
// Storage API implementation: startup, shutdown, etc.
//

void
as_storage_init_ssd(as_namespace *ns)
{
	drv_ssds *ssds;

	if (ns->n_storage_devices != 0) {
		ssd_init_devices(ns, &ssds);
		ssd_init_shadow_devices(ns, ssds);
	}
	else {
		ssd_init_files(ns, &ssds);
		ssd_init_shadow_files(ns, ssds);
	}

	g_unique_data_size += ns->drives_size / (2 * ns->cfg_replication_factor);

	cf_mutex_init(&ssds->flush_lock);

	// The queue limit is more efficient to work with.
	ns->storage_max_write_q = (uint32_t)
			(ssds->n_ssds * ns->storage_max_write_cache / WBLOCK_SZ);

	// The queue limit is more efficient to work with. Queue is per-device.
	ns->post_write_q_limit = (uint32_t)
			(ns->storage_post_write_cache / WBLOCK_SZ);

	// Minimize how often we recalculate this.
	ns->defrag_lwm_size = (WBLOCK_SZ * ns->storage_defrag_lwm_pct) / 100;

	ns->storage_private = (void*)ssds;

	char histname[HISTOGRAM_NAME_SIZE];

	snprintf(histname, sizeof(histname), "{%s}-device-read-size", ns->name);
	ns->device_read_size_hist = histogram_create(histname, HIST_SIZE);

	snprintf(histname, sizeof(histname), "{%s}-device-write-size", ns->name);
	ns->device_write_size_hist = histogram_create(histname, HIST_SIZE);

	uint32_t first_wblock_id = DRV_HEADER_SIZE / WBLOCK_SZ;
	histogram_scale scale = as_config_histogram_scale();

	// Finish initializing drv_ssd structures (non-zero-value members).
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->ns = ns;
		ssd->file_id = i;

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			cf_mutex_init(&ssd->current_swbs[c].lock);
		}

		cf_mutex_init(&ssd->defrag_lock);

		ssd->running = true;

		if (ssd->shadow_name) {
			ssd->running_shadow = true;
		}

		// Some (non-dynamic) config shortcuts:
		ssd->first_wblock_id = first_wblock_id;

		// Non-fresh devices will initialize this appropriately later.
		ssd->pristine_wblock_id = first_wblock_id;

		ssd_wblock_init(ssd);

		// Note: free_wblock_q, defrag_wblock_q created after loading devices.

		cf_pool_int32_init(&ssd->fd_pool, MAX_POOL_FDS, -1);
		cf_pool_int32_init(&ssd->fd_cache_pool, MAX_POOL_FDS, -1);

		if (ssd->shadow_name) {
			ssd->shadow_fd_q = cf_queue_create(sizeof(int), true);
		}

		ssd->swb_write_q = cf_queue_create(sizeof(void*), true);

		if (ssd->shadow_name) {
			ssd->swb_shadow_q = cf_queue_create(sizeof(void*), true);
		}

		ssd->swb_free_q = cf_queue_create(sizeof(void*), true);

		// TODO - hide the storage_commit_to_device usage.
		ssd->post_write_q = cf_queue_create(sizeof(void*),
				ns->storage_commit_to_device);

		snprintf(histname, sizeof(histname), "{%s}-%s-read", ns->name, ssd->name);
		ssd->hist_read = histogram_create(histname, scale);

		snprintf(histname, sizeof(histname), "{%s}-%s-large-block-read", ns->name, ssd->name);
		ssd->hist_large_block_read = histogram_create(histname, scale);

		snprintf(histname, sizeof(histname), "{%s}-%s-write", ns->name, ssd->name);
		ssd->hist_write = histogram_create(histname, scale);

		if (ssd->shadow_name) {
			snprintf(histname, sizeof(histname), "{%s}-%s-shadow-write", ns->name, ssd->name);
			ssd->hist_shadow_write = histogram_create(histname, scale);
		}

		ssd_init_commit(ssd);
	}

	// Will load headers and, if warm restart, resume persisted index.
	ssd_init_synchronous(ssds);
}


void
as_storage_load_ssd(as_namespace *ns, cf_queue *complete_q)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	// If devices have data, and it's cold start, scan devices.
	if (! ssds->all_fresh && ns->cold_start) {
		// Fire off threads to scan devices to build index and/or load record
		// data into memory - will signal completion when threads are all done.
		start_loading_records(ssds, complete_q);
		return;
	}
	// else - fresh devices or warm restart, this namespace is ready to roll.

	void *_t = NULL;

	cf_queue_push(complete_q, &_t);
}


void
as_storage_load_ticker_ssd(const as_namespace *ns)
{
	char buf[1024];
	int pos = 0;
	const drv_ssds *ssds = (const drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		const drv_ssd *ssd = &ssds->ssds[i];
		uint32_t pct = (uint32_t)
				((ssd->sweep_wblock_id * 100UL) / (ssd->file_size / WBLOCK_SZ));

		pos += sprintf(buf + pos, "%u,", pct);
	}

	if (pos != 0) {
		buf[pos - 1] = '\0'; // chomp last comma
	}

	if (drv_mrt_2nd_pass_load_ticker(ns, buf)) {
		return;
	}

	if (ns->n_tombstones == 0) {
		cf_info(AS_DRV_SSD, "{%s} loaded: objects %lu device-pcts (%s)",
				ns->name, ns->n_objects, buf);
	}
	else {
		cf_info(AS_DRV_SSD, "{%s} loaded: objects %lu tombstones %lu device-pcts (%s)",
				ns->name, ns->n_objects, ns->n_tombstones, buf);
	}
}


void
as_storage_sindex_build_all_ssd(as_namespace* ns)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	cf_tid tids[ssds->n_ssds];

	for (int i = 0; i < ssds->n_ssds; i++) {
		tids[i] = cf_thread_create_joinable(run_si_startup, &ssds->ssds[i]);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		cf_thread_join(tids[i]);
	}
}


void
as_storage_activate_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	ssd_load_wblock_queues(ssds);

	ssd_start_maintenance_threads(ssds);
	ssd_start_write_threads(ssds);

	if (ns->storage_defrag_startup_minimum != 0) {
		// Allow defrag to go full speed during startup - restore configured
		// setting when startup is done.
		ns->saved_defrag_sleep = ns->storage_defrag_sleep;
		ns->storage_defrag_sleep = 0;
	}

	ssd_start_defrag_threads(ssds);
}


bool
as_storage_wait_for_defrag_ssd(as_namespace *ns)
{
	if (ns->storage_defrag_startup_minimum == 0) {
		return false; // nothing to do - don't wait
	}

	uint32_t avail_pct;

	as_storage_stats_ssd(ns, &avail_pct, NULL);

	if (avail_pct >= ns->storage_defrag_startup_minimum) {
		// Restore configured defrag throttling values.
		ns->storage_defrag_sleep = ns->saved_defrag_sleep;
		return false; // done - don't wait
	}
	// else - not done - wait.

	cf_info(AS_DRV_SSD, "{%s} wait-for-defrag: avail-pct %u wait-for %u ...",
			ns->name, avail_pct, ns->storage_defrag_startup_minimum);

	return true;
}


// Note that this is *NOT* the counterpart to as_storage_record_create()!
// That would be as_storage_record_close_ssd(). This is what gets called when a
// record is destroyed, to dereference storage.
void
as_storage_destroy_record_ssd(as_namespace *ns, as_record *r)
{
	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id) && r->n_rblocks != 0) {
		drv_ssds *ssds = (drv_ssds*)ns->storage_private;
		drv_ssd *ssd = &ssds->ssds[r->file_id];

		ssd_block_free(ssd, r->rblock_id, r->n_rblocks, "destroy");

		as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
				-(int64_t)N_RBLOCKS_TO_SIZE(r->n_rblocks));

		r->rblock_id = 0;
		r->n_rblocks = 0;

		ssd_mrt_block_free_orig(ssds, r);
	}
}


//==========================================================
// Storage API implementation: as_storage_rd cycle.
//

void
as_storage_record_open_ssd(as_storage_rd *rd)
{
	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	rd->ssd = &ssds->ssds[rd->r->file_id];
}


// These are near the top of this file:
//		as_storage_record_get_n_bins_ssd()
//		as_storage_record_read_ssd()
//		as_storage_particle_read_all_ssd()
//		as_storage_particle_read_and_size_all_ssd()


//==========================================================
// Storage API implementation: storage capacity monitoring.
//

void
as_storage_defrag_sweep_ssd(as_namespace *ns)
{
	cf_info(AS_DRV_SSD, "{%s} sweeping all devices for wblocks to defrag ...", ns->name);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		as_incr_uint32(&ssds->ssds[i].defrag_sweep);
	}
}


//==========================================================
// Storage API implementation: data in device headers.
//

void
as_storage_load_regime_ssd(as_namespace *ns)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	ns->eventual_regime = ssds->generic->prefix.eventual_regime;
	ns->rebalance_regime = ns->eventual_regime;
}


void
as_storage_save_regime_ssd(as_namespace *ns)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	cf_mutex_lock(&ssds->flush_lock);

	ssds->generic->prefix.eventual_regime = ns->eventual_regime;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		ssd_write_header(ssd, (uint8_t*)ssds->generic,
				(uint8_t*)&ssds->generic->prefix.eventual_regime,
				sizeof(ssds->generic->prefix.eventual_regime));
	}

	cf_mutex_unlock(&ssds->flush_lock);
}


void
as_storage_load_roster_generation_ssd(as_namespace *ns)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	ns->roster_generation = ssds->generic->prefix.roster_generation;
}


void
as_storage_save_roster_generation_ssd(as_namespace *ns)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	// Normal for this to not change, cleaner to check here versus outside.
	if (ns->roster_generation == ssds->generic->prefix.roster_generation) {
		return;
	}

	cf_mutex_lock(&ssds->flush_lock);

	ssds->generic->prefix.roster_generation = ns->roster_generation;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		ssd_write_header(ssd, (uint8_t*)ssds->generic,
				(uint8_t*)&ssds->generic->prefix.roster_generation,
				sizeof(ssds->generic->prefix.roster_generation));
	}

	cf_mutex_unlock(&ssds->flush_lock);
}


void
as_storage_load_pmeta_ssd(as_namespace *ns, as_partition *p)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	drv_pmeta *pmeta = &ssds->generic->pmeta[p->id];

	p->version = pmeta->version;
}


void
as_storage_save_pmeta_ssd(as_namespace *ns, const as_partition *p)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	drv_pmeta *pmeta = &ssds->generic->pmeta[p->id];

	cf_mutex_lock(&ssds->flush_lock);

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd_write_header(ssd, (uint8_t*)ssds->generic, (uint8_t*)pmeta,
				sizeof(*pmeta));
	}

	cf_mutex_unlock(&ssds->flush_lock);
}


void
as_storage_cache_pmeta_ssd(as_namespace *ns, const as_partition *p)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	drv_pmeta *pmeta = &ssds->generic->pmeta[p->id];

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;
}


void
as_storage_flush_pmeta_ssd(as_namespace *ns, uint32_t start_pid,
		uint32_t n_partitions)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	drv_pmeta *pmeta = &ssds->generic->pmeta[start_pid];

	cf_mutex_lock(&ssds->flush_lock);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd_write_header(ssd, (uint8_t*)ssds->generic, (uint8_t*)pmeta,
				sizeof(drv_pmeta) * n_partitions);
	}

	cf_mutex_unlock(&ssds->flush_lock);
}


//==========================================================
// Storage API implementation: statistics.
//

void
as_storage_stats_ssd(as_namespace *ns, uint32_t *avail_pct,
		uint64_t *used_bytes)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (avail_pct != NULL) {
		*avail_pct = 100;

		// Find the device with the lowest available percent.
		for (int i = 0; i < ssds->n_ssds; i++) {
			drv_ssd *ssd = &ssds->ssds[i];
			uint32_t pct =
					(uint32_t)((available_size(ssd) * 100) / ssd->file_size);

			if (pct < *avail_pct) {
				*avail_pct = pct;
			}
		}
	}

	if (used_bytes != NULL) {
		uint64_t sz = 0;

		for (int i = 0; i < ssds->n_ssds; i++) {
			sz += ssds->ssds[i].inuse_size;
		}

		*used_bytes = sz;
	}
}


void
as_storage_device_stats_ssd(const struct as_namespace_s *ns, uint32_t device_ix,
		storage_device_stats *stats)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	drv_ssd *ssd = &ssds->ssds[device_ix];

	stats->used_sz = ssd->inuse_size;
	stats->n_free_wblocks = num_free_wblocks(ssd);

	stats->n_read_errors = ssd->n_read_errors;

	stats->write_q_sz = cf_queue_sz(ssd->swb_write_q);
	stats->n_writes = 0;
	stats->n_partial_writes = 0;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		stats->n_writes += ssd->current_swbs[c].n_wblock_writes;
		stats->n_partial_writes += ssd->current_swbs[c].n_wblock_partial_writes;
	}

	stats->defrag_q_sz = cf_queue_sz(ssd->defrag_wblock_q);
	stats->n_defrag_reads = ssd->n_defrag_wblock_reads;
	stats->n_defrag_writes = ssd->n_defrag_wblock_writes;
	stats->n_defrag_partial_writes = ssd->n_defrag_wblock_partial_writes;

	stats->shadow_write_q_sz = ssd->swb_shadow_q != NULL ?
			cf_queue_sz(ssd->swb_shadow_q) : 0;
}


void
as_storage_ticker_stats_ssd(as_namespace *ns)
{
	histogram_dump(ns->device_read_size_hist);
	histogram_dump(ns->device_write_size_hist);

	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_dump(ssd->hist_read);
		histogram_dump(ssd->hist_large_block_read);
		histogram_dump(ssd->hist_write);

		if (ssd->hist_shadow_write) {
			histogram_dump(ssd->hist_shadow_write);
		}
	}
}


void
as_storage_histogram_clear_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	histogram_scale scale = as_config_histogram_scale();

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_rescale(ssd->hist_read, scale);
		histogram_rescale(ssd->hist_large_block_read, scale);
		histogram_rescale(ssd->hist_write, scale);

		if (ssd->hist_shadow_write) {
			histogram_rescale(ssd->hist_shadow_write, scale);
		}
	}
}


//==========================================================
// Shutdown.
//

void
as_storage_shutdown_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			current_swb* cur_swb = &ssd->current_swbs[c];

			// Stop the maintenance thread from (also) flushing the swbs.
			cf_mutex_lock(&cur_swb->lock);

			ssd_write_buf* swb = cur_swb->swb;

			// Flush current swb by pushing it to write-q.
			if (swb != NULL) {
				push_wblock_to_write_q(ssd, swb);
				cur_swb->swb = NULL;
			}
		}

		// Stop the maintenance thread from (also) flushing the defrag swb.
		cf_mutex_lock(&ssd->defrag_lock);

		// Flush defrag swb by pushing it to write-q.
		if (ssd->defrag_swb != NULL) {
			push_wblock_to_write_q(ssd, ssd->defrag_swb);
			ssd->defrag_swb = NULL;
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->running = false;
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_thread_join(ssd->write_tid);

		// At this point nothing more can be added to the shadow queue.
		ssd->running_shadow = false;
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (ssd->shadow_name != NULL) {
			cf_thread_join(ssd->shadow_tid);
		}
	}

	ssd_set_pristine_offset(ssds);
	ssd_set_trusted(ssds);
}
