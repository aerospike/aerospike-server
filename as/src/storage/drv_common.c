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
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "dynbuf.h"
#include "hist.h"
#include "linear_hist.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/nsup.h"
#include "base/truncate.h"
#include "storage/flat.h"
#include "transaction/mrt_utils.h"

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

// The edition-neutral half of drv_cold_start_adopt_set() - the halves live in
// drv_common_ce.c and drv_common_ee.c.
//
// Returns true if the element already has a set, which the caller must then
// leave alone - the first set swept wins, as on the create path.
bool
drv_cold_start_set_already_assigned(cf_log_context log_ctx, as_namespace* ns,
		const as_flat_record* flat, const as_flat_opt_meta* opt_meta,
		const as_index* r)
{
	if (as_index_get_set_id(r) == INVALID_SET_ID) {
		return false;
	}

	const char* set_name = as_index_get_set_name(r, ns);

	if (set_name == NULL ||
			strncmp(set_name, opt_meta->set_name, opt_meta->set_name_len) != 0 ||
			set_name[opt_meta->set_name_len] != 0) {
		// Takes a writer that reuses a digest across two set names, which the
		// server permits. Note - the on-device name is not null-terminated.
		cf_warning(log_ctx,
				"{%s} %pD on-device set %.*s does not match indexed set %s",
				ns->name, &flat->keyd, (int)opt_meta->set_name_len,
				opt_meta->set_name, set_name == NULL ? "(null)" : set_name);
	}

	return true;
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

//==========================================================
// Local helpers - cold-start record ingest.
//

static bool
drv_cold_start_prefer_existing(const as_namespace* ns,
		const as_flat_record* flat, uint32_t block_void_time, const as_index* r)
{
	int result = as_record_resolve_conflict(drv_cold_start_policy(ns),
			r->generation, r->last_update_time, flat->generation,
			flat->last_update_time);

	if (result != 0) {
		return result == -1; // -1 means block record < existing record
	}

	// Finally, compare void-times. Note that defragged records will generate
	// identical copies on drive, so they'll get here and return true.
	return r->void_time == 0 ||
			(block_void_time != 0 && block_void_time <= r->void_time);
}

// The reporting half of drv_cold_start_set_already_assigned(), split out so that
// function reads as the predicate its name promises. Reaching the warning needs
// a writer that reuses one digest across two set names, which the server permits
// - the set name is checked against the index only on update, and the digest is
// client-supplied and never recomputed.
static void
warn_if_set_mismatch(cf_log_context log_ctx, as_namespace* ns,
		const as_flat_record* flat, const as_flat_opt_meta* opt_meta,
		const as_index* r)
{
	const char* set_name = as_index_get_set_name(r, ns);

	if (set_name == NULL ||
			strncmp(set_name, opt_meta->set_name, opt_meta->set_name_len) != 0 ||
			set_name[opt_meta->set_name_len] != 0) {
		// Note - the on-device name is not null-terminated, which is why the
		// compare is length-bounded and the terminator is checked separately.
		cf_warning(log_ctx,
				"{%s} %pD on-device set %.*s does not match indexed set %s",
				ns->name, &flat->keyd, (int)opt_meta->set_name_len,
				opt_meta->set_name, set_name == NULL ? "(null)" : set_name);
	}
}

//==========================================================
// Public API - cold-start record ingest.
//

// The edition-neutral half of drv_cold_start_adopt_set() - the halves live in
// drv_common_ce.c and drv_common_ee.c.
//
// Returns true if the element already has a set, which the caller must then
// leave alone - the first set swept wins, as on the create path. A version
// naming a different set is reported rather than applied; see
// warn_if_set_mismatch().
bool
drv_cold_start_set_already_assigned(cf_log_context log_ctx, as_namespace* ns,
		const as_flat_record* flat, const as_flat_opt_meta* opt_meta,
		const as_index* r)
{
	if (as_index_get_set_id(r) == INVALID_SET_ID) {
		return false;
	}

	warn_if_set_mismatch(log_ctx, ns, flat, opt_meta, r);

	return true;
}

void
drv_cold_start_add_record(const drv_cold_start_add_ops* ops,
		const as_flat_record* flat, uint64_t rblock_id, uint32_t record_size)
{
	uint32_t pid = as_partition_getid(&flat->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! ops->common->get_state_from_storage[pid]) {
		ops->counters->unowned++;
		return;
	}

	as_namespace* ns = ops->common->ns;
	as_partition* p_partition = &ns->partitions[pid];

	// Includes round rblock padding, so may not literally exclude the mark.
	// PMEM writes no end mark (end_mark_sz == 0), so its bins run to record_size.
	const uint8_t* end = (const uint8_t*)flat + record_size - ops->end_mark_sz;

	as_flat_opt_meta opt_meta = { { 0 } };

	const uint8_t* p_read = as_flat_unpack_record_meta(flat, end, &opt_meta);

	if (! p_read) {
		cf_warning(ops->log_ctx, "bad metadata for %pD", &flat->keyd);
		ops->counters->unparsable++;
		return;
	}

	if (opt_meta.void_time > ns->startup_max_void_time) {
		cf_warning(ops->log_ctx, "bad void-time for %pD", &flat->keyd);
		ops->counters->unparsable++;
		return;
	}

	const uint8_t* cb_end = NULL;

	if (! as_flat_decompress_buffer(&opt_meta.cm, WBLOCK_SZ, &p_read, &end,
				&cb_end)) {
		cf_warning(ops->log_ctx, "bad compressed data for %pD", &flat->keyd);
		ops->counters->unparsable++;
		return;
	}

	const uint8_t* exact_end =
			as_flat_check_packed_bins(p_read, end, opt_meta.n_bins);

	if (exact_end == NULL) {
		if (ops->bad_packed_bins_fn != NULL) {
			ops->bad_packed_bins_fn(&ops->dev, flat, record_size, rblock_id);
		}
		else {
			cf_warning(ops->log_ctx, "bad flat record %pD", &flat->keyd);
		}
		ops->counters->unparsable++;
		return;
	}

	if (ops->end_mark_sz != 0 &&
			! drv_check_end_mark(cb_end == NULL ? exact_end : cb_end, flat)) {
		if (ops->bad_end_mark_fn != NULL) {
			ops->bad_end_mark_fn(&ops->dev, flat, record_size, rblock_id);
		}
		else {
			cf_warning(ops->log_ctx, "bad end marker for %pD", &flat->keyd);
		}
		ops->counters->unparsable++;
		return;
	}

	// Ignore record if it was in a dropped tree.
	if (flat->tree_id != p_partition->tree_id) {
		ops->counters->dropped++;
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
		cf_crash(ops->log_ctx,
				"hit stop-writes limit before drive scan completed");
	}

	// Get/create the record from/in the appropriate index tree.
	as_index_ref r_ref;
	int rv = as_record_get_create(p_partition->tree, &flat->keyd, &r_ref, ns);

	if (rv < 0) {
		cf_crash(ops->log_ctx, "{%s} can't add record to index", ns->name);
	}

	bool is_create = rv == 1;

	as_index* r = r_ref.r;

	if (! is_create) {
		// Record already existed. Ignore this one if existing record is newer.
		if (drv_cold_start_prefer_existing(ns, flat, opt_meta.void_time, r)) {
			ops->fill_orig_fn(&ops->dev, flat, rblock_id, &opt_meta,
					p_partition->tree, &r_ref);
			drv_cold_start_adjust_cenotaph(ns, flat, opt_meta.void_time, r);
			as_record_done(&r_ref, ns);
			ops->counters->older++;
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
		ops->counters->expired++;
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
		ops->counters->evicted++;
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

		ops->counters->unique++;
	}
	else {
		// Before the update, so its set stats and set index use the new set-id.
		if (opt_meta.set_name != NULL) {
			drv_cold_start_adopt_set(ops->log_ctx, ns, flat, &opt_meta,
					p_partition->tree, &r_ref);
		}

		ops->record_update_fn(ops->devs, flat, &opt_meta, p_partition->tree,
				&r_ref);

		ops->counters->replace++;
	}

	// Store or drop the key according to the props we read.
	as_record_finalize_key(r, opt_meta.key, opt_meta.key_size);

	// Set/reset the record's last-update-time, generation, and void-time.
	r->last_update_time = flat->last_update_time;
	r->generation = flat->generation;
	r->void_time = opt_meta.void_time;

	// Update maximum void-time.
	as_setmax_uint32(&p_partition->max_void_time, r->void_time);

	// Set/reset the record's replication state and XDR-write status.
	drv_cold_start_init_repl_state(ns, r);
	drv_cold_start_init_xdr_state(flat, r);

	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(rblock_id);

	if (is_mrt_provisional(r) || is_mrt_monitor_write(ns, r)) {
		ops->dev.wblock_state[wblock_id].short_lived = true;
	}

	*ops->dev.inuse_size += record_size;
	ops->dev.wblock_state[wblock_id].inuse_sz += record_size;

	// Set/reset the record's storage information.
	r->file_id = ops->dev.file_id;
	r->rblock_id = rblock_id;

	as_namespace_adjust_set_data_used_bytes(ns, as_index_get_set_id(r),
			DELTA_N_RBLOCKS_TO_SIZE(flat->n_rblocks, r->n_rblocks));

	r->n_rblocks = flat->n_rblocks;

	as_record_done(&r_ref, ns);
}

//==========================================================
// Public API - defrag (shared index-tree check).
//

int
drv_record_defrag(cf_log_context log_ctx, const char* dev_name, as_namespace* ns,
		int file_id, uint32_t wblock_id, const as_flat_record* flat,
		uint64_t rblock_id, drv_defrag_move_fn move_fn, drv_dev dev)
{
	as_partition_reservation rsv;
	uint32_t pid = as_partition_getid(&flat->keyd);

	as_partition_reserve(ns, pid, &rsv);

	int rv;
	as_index_ref r_ref;
	bool found = 0 == as_record_get(rsv.tree, &flat->keyd, &r_ref);

	if (found) {
		as_index* r = r_ref.r;

		if ((r = drv_current_record(ns, r, file_id, rblock_id)) != NULL) {
			if (r->generation != flat->generation) {
				cf_warning(log_ctx,
						"device %s defrag: rblock_id %lu generation mismatch (%u:%u) %pD",
						dev_name, rblock_id, r->generation, flat->generation,
						&r->keyd);
			}

			if (r->n_rblocks != flat->n_rblocks) {
				cf_warning(log_ctx,
						"device %s defrag: rblock_id %lu n_blocks mismatch (%u:%u) %pD",
						dev_name, rblock_id, r->n_rblocks, flat->n_rblocks,
						&r->keyd);
			}

			move_fn(dev, wblock_id, flat, r);

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

//==========================================================
// Public API - defrag wblock record scan.
//

int
drv_defrag_scan_wblock_records(cf_log_context log_ctx, const char* dev_name,
		const uint8_t* buf, uint64_t wblock_base_offset, uint32_t wblock_id,
		const drv_wblock_state* p_wblock_state, drv_dev dev,
		uint32_t benign_magic, bool stop_at_gap,
		drv_defrag_prepare_record_fn prepare_fn,
		drv_defrag_on_record_fn on_record_fn)
{
	int record_count = 0;
	uint32_t indent = 0;

	while (indent < WBLOCK_SZ && as_load_uint32(&p_wblock_state->inuse_sz) != 0) {
		const as_flat_record* flat = prepare_fn != NULL
				? prepare_fn(dev, wblock_base_offset + (uint64_t)indent,
						  &buf[indent])
				: (const as_flat_record*)&buf[indent];

		if (flat->magic != AS_FLAT_MAGIC) {
			// The first record must have magic - except an engine's benign
			// magic, e.g. PMEM's crash-recovery dirty records.
			if ((benign_magic == 0 || flat->magic != benign_magic) &&
					indent == 0) {
				cf_warning(log_ctx, "%s: no magic at beginning of used wblock %d",
						dev_name, wblock_id);
				break;
			}

			// Later records may have no magic - stop at the first gap or keep
			// looking for magic, per engine (see header comment).
			if (stop_at_gap) {
				break;
			}

			indent += RBLOCK_SIZE;
			continue;
		}

		uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);
		uint32_t next_indent = indent + record_size;

		if (record_size < DRV_RECORD_MIN_SIZE || next_indent > WBLOCK_SZ) {
			cf_warning(log_ctx, "%s: bad record size %u", dev_name, record_size);
			indent += RBLOCK_SIZE;
			continue; // try next rblock
		}

		// Found a good record, move it if it's current.
		if (on_record_fn(dev, wblock_id, flat,
					OFFSET_TO_RBLOCK_ID(wblock_base_offset + (uint64_t)indent)) ==
				0) {
			record_count++;
		}

		indent = next_indent;
	}

	return record_count;
}

//==========================================================
// Public API - defrag thread loop.
//

void
drv_run_defrag_loop(const as_namespace* ns, cf_queue* defrag_wblock_q,
		drv_dev dev, drv_defrag_process_wblock_fn process_wblock,
		uint8_t* read_buf, uint32_t max_write_q_extra, bool sleep_after_wblock)
{
	uint32_t wblock_id;

	while (true) {
		uint32_t q_min = as_load_uint32(&ns->storage_defrag_queue_min);

		if (q_min == 0) {
			cf_queue_pop(defrag_wblock_q, &wblock_id, CF_QUEUE_FOREVER);
		}
		else {
			if (cf_queue_sz(defrag_wblock_q) <= q_min) {
				usleep(1000 * 50);
				continue;
			}

			cf_queue_pop(defrag_wblock_q, &wblock_id, CF_QUEUE_NOWAIT);
		}

		process_wblock(dev, wblock_id, read_buf);

		if (sleep_after_wblock) {
			uint32_t sleep_us = as_load_uint32(&ns->storage_defrag_sleep);

			if (sleep_us != 0) {
				usleep(sleep_us);
			}
		}

		while (ns->n_wblocks_to_flush >
				ns->storage_max_write_q + max_write_q_extra) {
			usleep(1000);
		}
	}
}

//==========================================================
// Public API - device maintenance thread.
//

void
drv_run_maintenance_loop(drv_dev dev, const as_namespace* ns,
		uint32_t* defrag_sweep_req, const drv_maintenance_ops* ops)
{
	uint64_t prev_n_total_writes = 0;
	uint64_t prev_n_defrag_reads = 0;
	uint64_t prev_n_defrag_writes = 0;
	uint64_t prev_n_defrag_io_skips = 0;
	uint64_t prev_n_direct_frees = 0;
	uint64_t prev_n_tomb_raider_reads = 0;

	uint64_t prev_n_writes_flush[N_CURRENT_SWBS] = { 0 };

	uint64_t prev_n_defrag_writes_flush = 0;

	uint64_t now = cf_getus();
	uint64_t next = now + DRV_MAINT_MAX_INTERVAL_US;

	uint64_t prev_log_stats = now;
	uint64_t prev_free_pool = now;
	uint64_t prev_flush[N_CURRENT_SWBS];
	uint64_t prev_defrag_flush = now;

	for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
		prev_flush[c] = now;
	}

	// If any job's (initial) interval is less than DRV_MAINT_MAX_INTERVAL_US
	// and we want it done on its interval the first time through, add a
	// drv_next_time() call for that job here to adjust 'next'. (No such jobs
	// for now.)

	uint64_t sleep_us = next - now;

	while (true) {
		usleep((uint32_t)sleep_us);

		now = cf_getus();
		next = now + DRV_MAINT_MAX_INTERVAL_US;

		if (now >= prev_log_stats + DRV_MAINT_LOG_STATS_INTERVAL_US) {
			ops->log_stats_fn(dev, &prev_n_total_writes, &prev_n_defrag_reads,
					&prev_n_defrag_writes, &prev_n_defrag_io_skips,
					&prev_n_direct_frees, &prev_n_tomb_raider_reads);
			prev_log_stats = now;
			next = drv_next_time(now, DRV_MAINT_LOG_STATS_INTERVAL_US, next);
		}

		if (now >= prev_free_pool + DRV_MAINT_FREE_POOL_INTERVAL_US) {
			ops->free_pool_fn(dev);
			prev_free_pool = now;
			next = drv_next_time(now, DRV_MAINT_FREE_POOL_INTERVAL_US, next);
		}

		uint64_t flush_max_us = ops->flush_max_us_fn(dev, ns);

		for (uint8_t c = 0; c < N_CURRENT_SWBS; c++) {
			if (flush_max_us != 0 && now >= prev_flush[c] + flush_max_us) {
				ops->flush_current_fn(dev, c, &prev_n_writes_flush[c]);
				prev_flush[c] = now;
				next = drv_next_time(now, flush_max_us, next);
			}
		}

		if (ops->flush_defrag_fn != NULL &&
				now >= prev_defrag_flush + DRV_MAINT_DEFRAG_FLUSH_INTERVAL_US) {
			ops->flush_defrag_fn(dev, &prev_n_defrag_writes_flush);
			prev_defrag_flush = now;
			next = drv_next_time(now, DRV_MAINT_DEFRAG_FLUSH_INTERVAL_US, next);
		}

		// Sweep may take long enough to mess up other jobs' schedules, but
		// it's a very rare manually-triggered intervention.
		if (*defrag_sweep_req != 0) {
			ops->defrag_sweep_fn(dev);
			as_decr_uint32(defrag_sweep_req);
		}

		// Refresh in case jobs took significant time.
		now = cf_getus();
		sleep_us = next > now ? next - now : 1;
	}
}
