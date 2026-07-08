/*
 * drv_common.h
 *
 * Copyright (C) 2008-2024 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "log.h"

#include "fabric/partition.h"
#include "storage/flat.h"
#include "storage/storage.h"

//==========================================================
// Forward declarations.
//

struct as_flat_opt_meta_s;
struct as_index_s;
struct as_index_ref_s;
struct as_index_tree_s;
struct as_namespace_s;

//==========================================================
// Typedefs & constants.
//

#define DRV_HEADER_MAGIC (0x4349747275730322L)
#define DRV_VERSION 4
// DRV_VERSION history:
// 1 - original
// 2 - minimum storage increment (RBLOCK_SIZE) from 512 to 128 bytes
// 3 - total overhaul including changed magic and moved version
// 4 - added end-mark and switched encryption-at-rest key processing

// Device header flags.
#define DRV_HEADER_FLAG_TRUSTED 0x01
#define DRV_HEADER_FLAG_SINGLE_BIN 0x02
#define DRV_HEADER_FLAG_ENCRYPTED 0x04
#define DRV_HEADER_FLAG_CP 0x08
#define DRV_HEADER_FLAG_COMMIT_TO_DEVICE 0x10

// DRV_HEADER_SIZE must be a power of 2 and >= MAX_WRITE_BLOCK_SIZE.
// Do NOT change DRV_HEADER_SIZE!
#define DRV_HEADER_SIZE (8 * 1024 * 1024)

// Size rounding needed for sanity checking.
#define DRV_RECORD_MIN_SIZE                                                    \
	(((uint32_t)sizeof(as_flat_record) + (RBLOCK_SIZE - 1)) & -RBLOCK_SIZE)

#define DRV_DEFRAG_RESERVE 8

// Used when determining a device's io_min_size.
#define LO_IO_MIN_SIZE 512
#define HI_IO_MIN_SIZE 4096

typedef struct drv_prefix_s {
	uint64_t magic;
	uint32_t version;
	char namespace[32];
	uint32_t n_devices;
	uint64_t random; // identify matching set of devices
	uint32_t flags;
	uint32_t write_block_size;
	uint32_t eventual_regime;
	uint32_t unused; // was eviction threshold pre-4.5.1
	uint32_t roster_generation;
} drv_prefix;

// Because we pad explicitly:
COMPILER_ASSERT(sizeof(drv_prefix) <= HI_IO_MIN_SIZE);

// TODO - deal with the name and the name of as_storage_info_set/get!
typedef struct drv_pmeta_s {
	as_partition_version version;
	uint8_t tree_id;
	uint8_t unused[7];
} drv_pmeta;

// Make sure a drv_pmeta never unnecessarily crosses an IO size boundary.
COMPILER_ASSERT((sizeof(drv_pmeta) & (sizeof(drv_pmeta) - 1)) == 0);

typedef struct drv_generic_s {
	drv_prefix prefix;
	uint8_t pad_prefix[HI_IO_MIN_SIZE - sizeof(drv_prefix)];
	drv_pmeta pmeta[AS_PARTITIONS];
} drv_generic;

typedef struct drv_unique_s {
	uint32_t device_id;
	uint32_t unused;
	uint8_t encrypted_key[64];
	uint8_t canary[16];
	uint64_t pristine_offset;
} drv_unique;

typedef struct drv_atomic_s {
	size_t size;
	off_t offset;
	uint8_t data[(128 * 1024) - (sizeof(size_t) + sizeof(off_t))];
} drv_atomic;

COMPILER_ASSERT(sizeof(drv_atomic) == 128 * 1024);

#define ROUND_UP_GENERIC                                                       \
	((sizeof(drv_generic) + (HI_IO_MIN_SIZE - 1)) & -HI_IO_MIN_SIZE)

#define ROUND_UP_UNIQUE                                                        \
	((sizeof(drv_atomic) + (HI_IO_MIN_SIZE - 1)) & -HI_IO_MIN_SIZE)

typedef struct drv_header_s {
	drv_generic generic;
	uint8_t pad_generic[ROUND_UP_GENERIC - sizeof(drv_generic)];
	drv_unique unique;
	uint8_t pad_unique[ROUND_UP_UNIQUE - sizeof(drv_unique)];
	drv_atomic atomic;
} drv_header;

COMPILER_ASSERT(sizeof(drv_header) <= DRV_HEADER_SIZE);

COMPILER_ASSERT(offsetof(drv_header, generic) == 0);
COMPILER_ASSERT(offsetof(drv_header, generic.prefix) == 0);

#define DRV_OFFSET_UNIQUE (offsetof(drv_header, unique))

typedef struct vacated_wblock_s {
	uint32_t file_id;
	uint32_t wblock_id;
} vacated_wblock;

#define STORAGE_INVALID_WBLOCK 0xFFFFffff

// Really just means "was set", doesn't fully validate...
#define STORAGE_RBLOCK_IS_VALID(__x) ((__x) != 0)

#define WBLOCK_STATE_FREE 0
#define WBLOCK_STATE_RESERVED 1
#define WBLOCK_STATE_USED 2
#define WBLOCK_STATE_DEFRAG 3
#define WBLOCK_STATE_EMPTYING 4

#define DRV_DEFRAG_PEN_INIT_CAPACITY (8 * 1024)

typedef struct defrag_pen_s {
	uint32_t n_ids;
	uint32_t capacity;
	uint32_t* ids;
	uint32_t stack_ids[DRV_DEFRAG_PEN_INIT_CAPACITY];
} defrag_pen;

//------------------------------------------------
// Unified write-buffer / wblock-state types.
//
// Each engine's per-wblock state struct, write-buffer struct, and current-wb
// struct begins with a corresponding "drv_*" base. Engine code accesses the
// common fields via "wb->base.field"; engine-specific fields stay on the
// engine's extension struct.
//

// Forward declaration so drv_wblock_state can hold a typed pointer.
struct drv_write_buffer_s;

// Per-engine device structs, forward-declared for the typed back-pointer union
// on drv_write_buffer below. Each is defined in its engine's header/source
// (drv_pmem_s is EE-only; in CE it stays incomplete, used only as a pointer).
struct drv_ssd_s;
struct drv_mem_s;
struct drv_pmem_s;

// Typed back-pointer to the per-engine device struct that owns a write buffer.
// A union of the concrete engine types (not void*) so engine code reads a typed
// member instead of casting; also passed to drv_wb_get so shared code stays
// void*-free. All arms are pointers, so it occupies one pointer. NOTE: no
// discriminant -- callers must use the arm matching the owning engine (via
// mwb_dev/swb_dev/pwb_dev).
typedef union drv_dev_u {
	struct drv_ssd_s* ssd;
	struct drv_mem_s* mem;
	struct drv_pmem_s* pmem;
} drv_dev;

// Common base for ssd_write_buf / mem_write_block / pmem_write_block. Layout
// is intentionally a strict prefix of every engine's write-buffer struct so
// shared code can cast safely. Engine-specific fields (e.g., SSD's "buf",
// MEM/PMEM's "base_addr", PMEM's "dirty") live in the engine extension.
typedef struct drv_write_buffer_s {
	uint32_t rc;
	uint32_t n_writers; // number of concurrent writers
	uint32_t flush_pos; // pos on last flush
	uint32_t n_vacated;
	uint32_t vacated_capacity;
	vacated_wblock* vacated_wblocks;
	// Typed back-pointer to the owning device (see drv_dev above). One pointer,
	// so the struct layout is unchanged.
	drv_dev dev;
	uint32_t wblock_id;
	uint32_t pos;
} drv_write_buffer;

// Common base for ssd_wblock_state / mem_wblock_state / pmem_wblock_state.
// All three engines have identical fields here, so this is a clean unification
// (a typedef in each engine's header points the engine name at this base; no
// extension struct is needed for wblock state).
typedef struct drv_wblock_state_s {
	uint32_t inuse_sz; // number of bytes currently used in the wblock
	cf_mutex lock; // transactions, write_worker, and defrag all are interested in wblock_state
	drv_write_buffer* wb; // pending writes for the wblock, also treated as a cache for reads
	uint8_t state;
	bool short_lived; // relevant for enterprise edition only
	uint32_t n_vac_dests; // number of wblocks into which this wblock defragged
} drv_wblock_state;

// Common base for current_swb / current_mwb / current_pwb. SSD/MEM extend with
// encrypted_commits and n_wblock_partial_writes; PMEM has only the base.
typedef struct drv_current_wb_s {
	cf_mutex lock; // lock protects writes to wb
	drv_write_buffer* wb; // wb currently being filled by writes
	uint64_t n_wblock_writes; // total number of wbs added to the write-q by writes
} drv_current_wb;

//==========================================================
// Public API - shared code between storage engines.
//

void drv_defrag_pen_init(defrag_pen* pen);
void drv_defrag_pen_destroy(defrag_pen* pen);
void drv_defrag_pen_add(defrag_pen* pen, uint32_t wblock_id);

void drv_adjust_sc_version_flags(struct as_namespace_s* ns, drv_pmeta* pmeta,
		bool wiped_drives, bool dirty);
void drv_mrt_create_cold_start_hash(struct as_namespace_s* ns);
bool drv_load_needs_2nd_pass(const struct as_namespace_s* ns);
bool drv_mrt_2nd_pass_load_ticker(const struct as_namespace_s* ns,
		const char* pcts);
void drv_cold_start_remove_from_set_index(struct as_namespace_s* ns,
		struct as_index_tree_s* tree, struct as_index_ref_s* r_ref);
void drv_cold_start_record_create(struct as_namespace_s* ns,
		const struct as_flat_record_s* flat,
		const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s* tree,
		struct as_index_ref_s* r_ref);
bool drv_is_set_evictable(const struct as_namespace_s* ns,
		const struct as_flat_opt_meta_s* opt_meta);
bool drv_cold_start_sweeps_done(struct as_namespace_s* ns);
struct as_index_s* drv_current_record(struct as_namespace_s* ns,
		struct as_index_s* r, int file_id, uint64_t rblock_id);
uint32_t drv_max_record_size(const struct as_namespace_s* ns,
		const struct as_index_s* r);

bool pread_all(int fd, void* buf, size_t size, off_t offset);
bool pwrite_all(int fd, const void* buf, size_t size, off_t offset);

//
// Wblock queue helpers - shared across all storage engines. Each engine passes
// its own queue and wblock-state array, so the algorithm lives in one place.
// Helpers that log also take the engine's facility, so log routing (AS_DRV_SSD /
// AS_DRV_MEM / AS_DRV_PMEM) is preserved.
//

// The per-device wblock allocation state that travels together through the
// shared write-buffer helpers. Bundled so callers pass one pointer instead of
// the same four arguments repeated. pristine_wblock_id is a pointer because
// drv_wb_get advances it via compare-and-swap; the read-only helpers only
// dereference it.
// Allocation state only - the fields drv_wb_get reads to hand out a wblock.
// defrag_wblock_q is deliberately excluded: it is a routing/output queue that
// write-done pushes to, never a source for allocation.
typedef struct drv_wblock_pool_s {
	cf_queue* free_wblock_q;
	uint32_t* pristine_wblock_id;
	uint32_t n_wblocks;
	drv_wblock_state* wblock_state;
} drv_wblock_pool;

// Build a drv_wblock_pool view from any engine's device struct. One definition
// works for drv_mem / drv_ssd / drv_pmem alike because all three name these
// fields identically - it can't be an ordinary function since the device structs
// are distinct types. Statement-expression + __typeof__ so _dev is evaluated
// exactly once (safe for DRV_WBLOCK_POOL(next_dev()) etc.).
#define DRV_WBLOCK_POOL(_dev)                                                  \
	__extension__({                                                            \
		__typeof__(_dev) _d = (_dev);                                          \
		(drv_wblock_pool){                                                     \
			.free_wblock_q = _d->free_wblock_q,                                \
			.pristine_wblock_id = &_d->pristine_wblock_id,                     \
			.n_wblocks = _d->n_wblocks,                                        \
			.wblock_state = _d->wblock_state,                                  \
		};                                                                     \
	})

// Push a wblock onto its device's free queue. Sets the wblock's state to
// FREE first; safe to call before the queue exists (cold start).
// static inline (in the header) so it collapses into per-record callers (e.g.
// PMEM block_free) under -O3 without LTO.
static inline void
drv_push_wblock_to_free_q(cf_log_context log_ctx, cf_queue* free_wblock_q,
		drv_wblock_state* wblock_states, uint32_t n_wblocks, uint32_t wblock_id)
{
	// Can get here before queue created, e.g. cold start replacing records.
	if (free_wblock_q == NULL) {
		return;
	}

	cf_assert(wblock_id < n_wblocks, log_ctx,
			"pushing bad wblock_id %d to free_wblock_q", (int32_t)wblock_id);

	wblock_states[wblock_id].state = WBLOCK_STATE_FREE;
	cf_queue_push(free_wblock_q, &wblock_id);
}

// Push a wblock onto the defrag queue and bump the per-device defrag-read
// counter. No-op until the queue exists.
// static inline (in the header) - see drv_push_wblock_to_free_q above.
static inline void
drv_push_wblock_to_defrag_q(cf_queue* defrag_wblock_q,
		drv_wblock_state* wblock_states, uint32_t wblock_id,
		uint64_t* n_defrag_wblock_reads)
{
	// null until devices are loaded at startup.
	if (defrag_wblock_q == NULL) {
		return;
	}

	wblock_states[wblock_id].state = WBLOCK_STATE_DEFRAG;
	cf_queue_push(defrag_wblock_q, &wblock_id);
	as_incr_uint64(n_defrag_wblock_reads);
}

// Push a filled wblock onto its device's write/shadow queue to be flushed,
// bumping the namespace's pending-flush counter. write_q is the engine's
// concrete queue (swb_write_q / pwb_write_q / mwb_shadow_q).
void drv_push_wblock_to_write_q(struct as_namespace_s* ns, cf_queue* write_q,
		const drv_write_buffer* wb);

// Atomically claim the next wblock id from the pristine region. Returns
// false when the device is out of pristine wblocks.
bool drv_pop_pristine_wblock_id(uint32_t* pristine_wblock_id,
		uint32_t n_wblocks, uint32_t* wblock_id);

// Number of wblocks still in the pristine (never-written) region.
uint32_t drv_num_pristine_wblocks(uint32_t n_wblocks,
		uint32_t pristine_wblock_id);

// Total free wblocks: free queue + pristine region.
uint32_t drv_num_free_wblocks(const drv_wblock_pool* pool);

// Available contiguous bytes. Returns full file_size during cold start so
// eviction-threshold checks are effectively disabled then.
uint64_t drv_available_size(const drv_wblock_pool* pool, uint64_t file_size);

// Take the earlier of (now + job_interval) and (next). Used by per-engine
// maintenance loops to schedule the next wakeup.
uint64_t drv_next_time(uint64_t now, uint64_t job_interval, uint64_t next);

//
// Wblock summary dump - shared across all storage engines.
//
// Each engine's "dump_wb_summary" walks per-device wblock_state arrays and
// prints a one-line summary per device plus a namespace-level histogram.
// The walks are byte-identical except for log facility and the per-device
// array shape; the engine builds a tiny drv_dev_view array on the stack
// and hands it to drv_dump_wb_summary.
//

typedef struct drv_dev_view_s {
	const char* name;
	uint32_t first_wblock_id;
	uint32_t n_wblocks;
	uint32_t pristine_wblock_id;
	const drv_wblock_state* wblock_state;
} drv_dev_view;

void drv_dump_wb_summary(cf_log_context log_ctx, const struct as_namespace_s* ns,
		bool verbose, const drv_dev_view* devs, uint32_t n_devs);

//
// Write-buffer helpers - shared across storage engines.
//

// Initial size & growth step of wb->vacated_wblocks - batches the realloc.
#define DRV_WB_VACATED_CAPACITY_STEP 128

bool drv_wb_add_unique_vacated_wblock(drv_write_buffer* wb,
		uint32_t src_file_id, uint32_t src_wblock_id);

typedef void (*drv_wb_release_vacated_one_fn)(void* udata, uint32_t file_id,
		uint32_t wblock_id);

void drv_wb_release_all_vacated_wblocks(drv_write_buffer* wb,
		drv_wb_release_vacated_one_fn release_one, void* udata);

// static inline (in the header) so it collapses into per-record callers (PMEM
// block_free / pwb_release) under -O3 without LTO. inuse_sz is passed in rather
// than re-loaded so per-record callers reuse the value they already hold.
static inline void
drv_wb_wblock_write_done(cf_log_context log_ctx, uint32_t wblock_id,
		uint32_t inuse_sz, cf_queue* free_wblock_q, cf_queue* defrag_wblock_q,
		drv_wblock_state* wblock_states, uint32_t n_wblocks,
		uint64_t* n_defrag_wblock_reads, uint32_t defrag_lwm_size,
		uint64_t* n_wblock_direct_frees)
{
	// Derived here rather than passed in so it can't diverge from the element
	// the queue helpers re-index as wblock_states[wblock_id].
	drv_wblock_state* ws = &wblock_states[wblock_id];

	if (inuse_sz == 0) {
		ws->short_lived = false;

		as_incr_uint64(n_wblock_direct_frees);
		drv_push_wblock_to_free_q(log_ctx, free_wblock_q, wblock_states,
				n_wblocks, wblock_id);
	}
	else if (inuse_sz < defrag_lwm_size) {
		if (! ws->short_lived) {
			drv_push_wblock_to_defrag_q(defrag_wblock_q, wblock_states,
					wblock_id, n_defrag_wblock_reads);
		}
		else {
			ws->state = WBLOCK_STATE_USED;
		}
	}
	else {
		ws->state = WBLOCK_STATE_USED;
	}
}

// Per-engine callbacks for drv_wb_get. Each takes the engine's device via the
// typed drv_dev union (caller sets the arm matching its engine). When each fires:
//   create_fn     - REQUIRED. Allocates AND initializes a fresh write buffer for
//                   this engine. Runs only when the free queue is empty (a reused
//                   buffer was already reset on release), so it must leave the wb
//                   in the same state a freshly-acquired buffer expects.
//   post_claim_fn - REQUIRED. Runs every acquisition, after a wblock id is
//                   claimed; maps/positions the buffer over that wblock (MEM/PMEM)
//                   and/or takes the engine's reference on it (SSD). Asserted
//                   non-NULL: every engine needs it, and omitting it corrupts
//                   silently (SSD use-after-free / MEM/PMEM stale base_addr)
//                   rather than failing at acquisition time.
typedef drv_write_buffer* (*drv_wb_create_fn)(drv_dev dev);
typedef void (*drv_wb_post_claim_fn)(drv_write_buffer* wb, drv_dev dev,
		uint32_t wblock_id);

drv_write_buffer* drv_wb_get(cf_log_context log_ctx, const char* dev_name,
		bool use_reserve, uint32_t reserve_threshold, cf_queue* wb_free_q,
		const drv_wblock_pool* pool, drv_dev dev, drv_wb_create_fn create_fn,
		drv_wb_post_claim_fn post_claim_fn);

//
// Conversions between offsets and rblocks.
//

// Convert byte offset to rblock_id, as long as offset is already a multiple of
// rblock size.
static inline uint64_t
OFFSET_TO_RBLOCK_ID(uint64_t offset)
{
	return offset >> LOG_2_RBLOCK_SIZE;
}

// Convert rblock_id to byte offset.
static inline uint64_t
RBLOCK_ID_TO_OFFSET(uint64_t rblock_id)
{
	return rblock_id << LOG_2_RBLOCK_SIZE;
}

//
// Conversions between bytes/rblocks and wblocks.
//

// Convert byte offset to wblock_id.
static inline uint32_t
OFFSET_TO_WBLOCK_ID(uint64_t offset)
{
	return (uint32_t)(offset / WBLOCK_SZ);
}

// Convert wblock_id to byte offset.
static inline uint64_t
WBLOCK_ID_TO_OFFSET(uint32_t wblock_id)
{
	return (uint64_t)wblock_id * WBLOCK_SZ;
}

// Convert rblock_id to wblock_id.
static inline uint32_t
RBLOCK_ID_TO_WBLOCK_ID(uint64_t rblock_id)
{
	return (uint32_t)((rblock_id << LOG_2_RBLOCK_SIZE) / WBLOCK_SZ);
}

// Convert rblock_id to 'pos' or 'indent' within wblock.
static inline uint32_t
RBLOCK_ID_TO_POS(uint64_t rblock_id)
{
	return (uint32_t)((rblock_id << LOG_2_RBLOCK_SIZE) % WBLOCK_SZ);
}

//
// Round to flush quanta.
//

// Round bytes down to a multiple of flush size.
static inline uint64_t
BYTES_DOWN_TO_FLUSH(uint64_t flush_sz, uint64_t bytes)
{
	return bytes & -flush_sz;
}

// Round bytes up to a multiple of flush size.
static inline uint64_t
BYTES_UP_TO_FLUSH(uint64_t flush_sz, uint64_t bytes)
{
	return (bytes + (flush_sz - 1)) & -flush_sz;
}

//
// End-mark utilities.
//

static inline uint32_t
drv_make_end_mark(const as_flat_record* flat)
{
	// Hash digest and LUT.
	uint32_t hash = cf_wyhash32((const uint8_t*)&flat->keyd, 25);

	// Reserve a bit for signature flag.
	return cf_swap_to_le32(hash & 0x7FFFffff);
}

static inline void
drv_add_end_mark(uint8_t* mark, const as_flat_record* flat)
{
	*(uint32_t*)mark = drv_make_end_mark(flat);
}

static inline bool
drv_check_end_mark(const uint8_t* mark, const as_flat_record* flat)
{
	return *(uint32_t*)mark == drv_make_end_mark(flat);
}
