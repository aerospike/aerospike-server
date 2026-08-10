/*
 * drv_common.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include "storage/drv_common.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_queue.h"

#include "dynbuf.h"
#include "hist.h"
#include "linear_hist.h"
#include "log.h"

#include "base/datamodel.h"
#include "storage/flat.h"

//==========================================================
// Public API - shared code between storage engines.
//

void
drv_defrag_pen_init(defrag_pen* pen)
{
	pen->n_ids = 0;
	pen->capacity = DRV_DEFRAG_PEN_INIT_CAPACITY;
	pen->ids = pen->stack_ids;
}

void
drv_defrag_pen_destroy(defrag_pen* pen)
{
	if (pen->ids != pen->stack_ids) {
		cf_free(pen->ids);
	}
}

void
drv_defrag_pen_add(defrag_pen* pen, uint32_t wblock_id)
{
	if (pen->n_ids == pen->capacity) {
		if (pen->capacity == DRV_DEFRAG_PEN_INIT_CAPACITY) {
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

bool
drv_is_set_evictable(const as_namespace* ns, const as_flat_opt_meta* opt_meta)
{
	if (! opt_meta->set_name) {
		return true;
	}

	as_set* p_set;

	if (cf_vmapx_get_by_name_w_len(ns->p_sets_vmap, opt_meta->set_name,
				opt_meta->set_name_len, (void**)&p_set) != CF_VMAPX_OK) {
		return true;
	}

	return ! p_set->eviction_disabled;
}

bool
pread_all(int fd, void* buf, size_t size, off_t offset)
{
	ssize_t result;

	while ((result = pread(fd, buf, size, offset)) != (ssize_t)size) {
		if (result < 0) {
			return false; // let the caller log errors
		}

		if (result == 0) { // should only happen if caller passed 0 size
			errno = EINVAL;
			return false;
		}

		buf += result;
		offset += result;
		size -= result;
	}

	return true;
}

bool
pwrite_all(int fd, const void* buf, size_t size, off_t offset)
{
	ssize_t result;

	while ((result = pwrite(fd, buf, size, offset)) != (ssize_t)size) {
		if (result < 0) {
			return false; // let the caller log errors
		}

		if (result == 0) { // should only happen if caller passed 0 size
			errno = EINVAL;
			return false;
		}

		buf += result;
		offset += result;
		size -= result;
	}

	return true;
}

void
drv_push_wblock_to_write_q(as_namespace* ns, cf_queue* write_q,
		const drv_write_buffer* wb)
{
	as_incr_uint32(&ns->n_wblocks_to_flush);
	cf_queue_push(write_q, &wb);
}

bool
drv_pop_pristine_wblock_id(uint32_t* pristine_wblock_id, uint32_t n_wblocks,
		uint32_t* wblock_id)
{
	uint32_t id;

	while ((id = as_load_uint32(pristine_wblock_id)) < n_wblocks) {
		if (as_cas_uint32(pristine_wblock_id, id, id + 1)) {
			*wblock_id = id;
			return true;
		}
	}

	return false; // out of space
}

uint32_t
drv_num_pristine_wblocks(uint32_t n_wblocks, uint32_t pristine_wblock_id)
{
	return n_wblocks - pristine_wblock_id;
}

uint32_t
drv_num_free_wblocks(const drv_wblock_pool* pool)
{
	return cf_queue_sz(pool->free_wblock_q) +
			drv_num_pristine_wblocks(pool->n_wblocks, *pool->pristine_wblock_id);
}

uint64_t
drv_available_size(const drv_wblock_pool* pool, uint64_t file_size)
{
	// Note - returns 100% available during cold start, to make it irrelevant
	// in cold start eviction threshold check.
	return pool->free_wblock_q != NULL
			? (uint64_t)drv_num_free_wblocks(pool) * WBLOCK_SZ
			: file_size;
}

uint64_t
drv_next_time(uint64_t now, uint64_t job_interval, uint64_t next)
{
	uint64_t next_job = now + job_interval;

	return next_job < next ? next_job : next;
}

void
drv_dump_wb_summary(cf_log_context log_ctx, const as_namespace* ns,
		bool verbose, const drv_dev_view* devs, uint32_t n_devs)
{
	uint32_t n_free = 0;
	uint32_t n_reserved = 0;
	uint32_t n_used = 0;
	uint32_t n_defrag = 0;
	uint32_t n_emptying = 0;
	uint32_t n_pristine = 0;

	uint32_t n_short_lived = 0;

	uint32_t n_zero_used_sz = 0;

	linear_hist* h = linear_hist_create("", LINEAR_HIST_SIZE, 0, WBLOCK_SZ, 100);

	for (uint32_t d = 0; d < n_devs; d++) {
		const drv_dev_view* dev = &devs[d];

		uint32_t d_free = 0;
		uint32_t d_reserved = 0;
		uint32_t d_used = 0;
		uint32_t d_defrag = 0;
		uint32_t d_emptying = 0;
		uint32_t d_pristine = 0;
		uint32_t d_short_lived = 0;
		uint32_t d_zero_used_sz = 0;

		for (uint32_t i = dev->first_wblock_id; i < dev->n_wblocks; i++) {
			const drv_wblock_state* wblock_state = &dev->wblock_state[i];

			// Treat wblocks beyond pristine_wblock_id as pristine regardless
			// of state.
			if (i >= dev->pristine_wblock_id) {
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
				cf_warning(log_ctx, "bad wblock state %u", wblock_state->state);
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
			cf_info(log_ctx,
					"WB: device %s: pristine:%u reserved:%u used:%u defrag:%u emptying:%u free:%u",
					dev->name, d_pristine, d_reserved, d_used, d_defrag,
					d_emptying, d_free);

			if (d_short_lived != 0) {
				cf_info(log_ctx, "WB: device %s: short-lived:%u", dev->name,
						d_short_lived);
			}

			cf_info(log_ctx, "WB: device %s: zero-used-sz:%u", dev->name,
					d_zero_used_sz);
		}
	}

	cf_info(log_ctx, "WB: namespace %s", ns->name);
	cf_info(log_ctx,
			"WB: wblocks by state - pristine:%u reserved:%u used:%u defrag:%u emptying:%u free:%u",
			n_pristine, n_reserved, n_used, n_defrag, n_emptying, n_free);

	if (n_short_lived != 0) {
		cf_info(log_ctx, "WB: short-lived wblocks - %u", n_short_lived);
	}

	cf_dyn_buf_define(db);

	// Not bothering with more suitable linear_hist API ... use what's there.
	linear_hist_save_info(h);
	linear_hist_get_info(h, &db);
	cf_dyn_buf_append_char(&db, '\0');

	cf_info(log_ctx, "WB: wblocks with zero used-sz - %u", n_zero_used_sz);
	cf_info(log_ctx, "WB: wblocks by (non-zero) used-sz - %s", db.buf);

	cf_dyn_buf_free(&db);
	linear_hist_destroy(h);
}

bool
drv_wb_add_unique_vacated_wblock(drv_write_buffer* wb, uint32_t src_file_id,
		uint32_t src_wblock_id)
{
	for (uint32_t i = 0; i < wb->n_vacated; i++) {
		vacated_wblock* vw = &wb->vacated_wblocks[i];

		if (vw->wblock_id == src_wblock_id && vw->file_id == src_file_id) {
			return false;
		}
	}

	if (wb->n_vacated == wb->vacated_capacity) {
		wb->vacated_capacity += DRV_WB_VACATED_CAPACITY_STEP;
		wb->vacated_wblocks = cf_realloc(wb->vacated_wblocks,
				sizeof(vacated_wblock) * wb->vacated_capacity);
	}

	wb->vacated_wblocks[wb->n_vacated].file_id = src_file_id;
	wb->vacated_wblocks[wb->n_vacated].wblock_id = src_wblock_id;
	wb->n_vacated++;

	return true;
}

void
drv_wb_release_all_vacated_wblocks(drv_write_buffer* wb,
		drv_wb_release_vacated_one_fn release_one, void* udata)
{
	for (uint32_t i = 0; i < wb->n_vacated; i++) {
		vacated_wblock* vw = &wb->vacated_wblocks[i];

		release_one(udata, vw->file_id, vw->wblock_id);
	}

	wb->n_vacated = 0;
}

drv_write_buffer*
drv_wb_get(cf_log_context log_ctx, const char* dev_name, bool use_reserve,
		uint32_t reserve_threshold, cf_queue* wb_free_q,
		const drv_wblock_pool* pool, drv_dev dev, drv_wb_create_fn create_fn,
		drv_wb_post_claim_fn post_claim_fn)
{
	if (! use_reserve && drv_num_free_wblocks(pool) <= reserve_threshold) {
		return NULL;
	}

	drv_write_buffer* wb;

	if (CF_QUEUE_OK != cf_queue_pop(wb_free_q, &wb, CF_QUEUE_NOWAIT)) {
		wb = create_fn(dev);
	}

	if (cf_queue_pop(pool->free_wblock_q, &wb->wblock_id, CF_QUEUE_NOWAIT) !=
					CF_QUEUE_OK &&
			! drv_pop_pristine_wblock_id(pool->pristine_wblock_id,
					pool->n_wblocks, &wb->wblock_id)) {
		cf_queue_push(wb_free_q, &wb);
		return NULL;
	}

	cf_assert(post_claim_fn != NULL, log_ctx,
			"device %s: post_claim_fn required", dev_name);

	post_claim_fn(wb, dev, wb->wblock_id);

	drv_wblock_state* p_wblock_state = &pool->wblock_state[wb->wblock_id];

	uint32_t inuse_sz = as_load_uint32(&p_wblock_state->inuse_sz);

	cf_assert(inuse_sz == 0, log_ctx,
			"device %s: wblock-id %u inuse-size %u off free-q", dev_name,
			wb->wblock_id, inuse_sz);

	cf_assert(p_wblock_state->wb == NULL, log_ctx,
			"device %s: wblock-id %u wb not null off free-q", dev_name,
			wb->wblock_id);

	cf_assert(p_wblock_state->state == WBLOCK_STATE_FREE, log_ctx,
			"device %s: wblock-id %u state %u off free-q", dev_name,
			wb->wblock_id, p_wblock_state->state);

	p_wblock_state->wb = wb;
	p_wblock_state->state = WBLOCK_STATE_RESERVED;

	return wb;
}

//==========================================================
// Local helpers - write to header.
//

// Push a byte range of the in-memory header image out to every device - one
// prefix field for the regime and roster writers, one drv_pmeta or a run of
// them for the pmeta writers. Takes no lock - callers that need one already
// hold flush_lock across their mutation and this fan-out.
//
// The device array and the writer slots come from <engine>_init_common_devs(),
// which nothing forces a new allocation path to call. Skipping it leaves the
// memset(0) state - devs null, n_devices 0, slots null - where this loop would
// write no header at all and report nothing, so assert instead of persisting
// silently. The assert sits outside the loop, and the four save/flush helpers
// that reach it run per rebalance, never per record.
static void
write_generic_header_bytes(cf_log_context log_ctx, const drv_devices_common* dc,
		drv_write_header_fn write_fn, const uint8_t* from, size_t size)
{
	cf_assert(dc->devs != NULL && dc->n_devices > 0 && write_fn != NULL,
			log_ctx, "{%s} header write before init", dc->ns->name);

	for (int i = 0; i < dc->n_devices; i++) {
		write_fn(dc->devs[i], (const uint8_t*)dc->generic, from, size);
	}
}

//==========================================================
// Public API - device array setup.
//

void
drv_init_common_devs(drv_devices_common* dc, int n_devices,
		drv_write_header_fn write_header, drv_write_header_fn write_header_atomic)
{
	dc->n_devices = n_devices;
	dc->devs = cf_malloc((size_t)n_devices * sizeof(drv_dev));

	dc->write_header = write_header;
	dc->write_header_atomic = write_header_atomic;
}

//==========================================================
// Public API - namespace header / pmeta persistence.
//

void
drv_load_regime(cf_log_context log_ctx, drv_devices_common* dc)
{
	cf_assert(dc->generic != NULL, log_ctx, "{%s} header read before init",
			dc->ns->name);

	as_namespace* ns = dc->ns;

	ns->eventual_regime = dc->generic->prefix.eventual_regime;
	ns->rebalance_regime = ns->eventual_regime;
}

void
drv_load_roster_generation(cf_log_context log_ctx, drv_devices_common* dc)
{
	cf_assert(dc->generic != NULL, log_ctx, "{%s} header read before init",
			dc->ns->name);

	dc->ns->roster_generation = dc->generic->prefix.roster_generation;
}

void
drv_load_pmeta(cf_log_context log_ctx, const drv_devices_common* dc,
		as_partition* p)
{
	cf_assert(p->id < AS_PARTITIONS, log_ctx, "{%s} bad pmeta pid %u",
			dc->ns->name, p->id);

	const drv_pmeta* pmeta = &dc->generic->pmeta[p->id];

	p->version = pmeta->version;
}

void
drv_cache_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		const as_partition* p)
{
	cf_assert(p->id < AS_PARTITIONS, log_ctx, "{%s} bad pmeta pid %u",
			dc->ns->name, p->id);

	drv_pmeta* pmeta = &dc->generic->pmeta[p->id];

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;
}

void
drv_save_regime(cf_log_context log_ctx, drv_devices_common* dc)
{
	cf_mutex_lock(&dc->flush_lock);

	dc->generic->prefix.eventual_regime = dc->ns->eventual_regime;

	write_generic_header_bytes(log_ctx, dc, dc->write_header,
			(const uint8_t*)&dc->generic->prefix.eventual_regime,
			sizeof(dc->generic->prefix.eventual_regime));

	cf_mutex_unlock(&dc->flush_lock);
}

void
drv_save_roster_generation(cf_log_context log_ctx, drv_devices_common* dc)
{
	// Normal for this to not change, cleaner to check here versus outside.
	if (dc->ns->roster_generation == dc->generic->prefix.roster_generation) {
		return;
	}

	cf_mutex_lock(&dc->flush_lock);

	dc->generic->prefix.roster_generation = dc->ns->roster_generation;

	write_generic_header_bytes(log_ctx, dc, dc->write_header,
			(const uint8_t*)&dc->generic->prefix.roster_generation,
			sizeof(dc->generic->prefix.roster_generation));

	cf_mutex_unlock(&dc->flush_lock);
}

void
drv_save_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		const as_partition* p)
{
	cf_assert(p->id < AS_PARTITIONS, log_ctx, "{%s} bad pmeta pid %u",
			dc->ns->name, p->id);

	drv_pmeta* pmeta = &dc->generic->pmeta[p->id];

	cf_mutex_lock(&dc->flush_lock);

	pmeta->version = p->version;
	pmeta->tree_id = p->tree_id;

	write_generic_header_bytes(log_ctx, dc, dc->write_header_atomic,
			(const uint8_t*)pmeta, sizeof(*pmeta));

	cf_mutex_unlock(&dc->flush_lock);
}

void
drv_flush_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		uint32_t start_pid, uint32_t n_partitions)
{
	// Not 'start_pid + n_partitions <= AS_PARTITIONS' - that sum is unsigned
	// and wraps, which would let a huge start_pid through.
	cf_assert(n_partitions <= AS_PARTITIONS &&
					start_pid <= AS_PARTITIONS - n_partitions,
			log_ctx, "{%s} bad pmeta range %u + %u", dc->ns->name, start_pid,
			n_partitions);

	drv_pmeta* pmeta = &dc->generic->pmeta[start_pid];

	cf_mutex_lock(&dc->flush_lock);

	write_generic_header_bytes(log_ctx, dc, dc->write_header_atomic,
			(const uint8_t*)pmeta, sizeof(drv_pmeta) * n_partitions);

	cf_mutex_unlock(&dc->flush_lock);
}
