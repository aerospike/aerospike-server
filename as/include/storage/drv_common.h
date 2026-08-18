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

#include "base/datamodel.h"
#include "fabric/partition.h"
#include "storage/flat.h"
#include "storage/storage.h"

//==========================================================
// Storage ops tables.
//
// Defined by each engine next to its own functions, consumed only by
// as_storage_bind_ops(). Declared here rather than in storage.h so they stay
// invisible to the ~90 translation units that reach storage.h through
// base/datamodel.h.
//

extern const as_storage_ops as_storage_ops_mem;
extern const as_storage_ops as_storage_ops_ssd;
extern const as_storage_ops as_storage_ops_pmem;

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

// The biggest write through PMEM's atomic header path is the whole pmeta
// array, from drv_flush_pmeta. write_header_atomic stages that range in
// drv_atomic.data before applying it, and stages it whole - so the array has
// to fit. Only the atomic path stages: the plain writer copies straight to the
// device, so flush_header's DRV_HEADER_SIZE write is not bounded by this.
// Stated here because the two sizes are set in different structs, and the
// staging copy does not check.
COMPILER_ASSERT(sizeof(drv_pmeta) * AS_PARTITIONS <=
		sizeof(((drv_atomic*)NULL)->data));

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

// Typed handle to a per-engine device struct. A union of the concrete engine
// types (not void*) so engine code reads a typed member instead of casting.
// Used for the write buffer's owning device (via mwb_dev/swb_dev/pwb_dev), and
// as the device parameter of drv_wb_get and of the defrag and maintenance
// hooks, whose call sites name the arm directly - (drv_dev){ .ssd = ssd }. All
// arms are pointers, so it occupies one pointer. NOTE: no discriminant -- the
// arm a callee reads must be the one its caller set.
typedef union drv_dev_u {
	struct drv_ssd_s* ssd;
	struct drv_mem_s* mem;
	struct drv_pmem_s* pmem;
} drv_dev;

// Per-engine per-namespace device-array structs, forward-declared for the typed
// handle union below. Each is defined in its engine's header/source (drv_pmems_s
// is EE-only; in CE it stays incomplete, used only as a pointer).
struct drv_ssds_s;
struct drv_mems_s;
struct drv_pmems_s;

// Typed handle to a per-engine device-array struct, for passing one through
// shared code back to an engine callback without a cast in either direction.
// Same shape and caveat as drv_dev above: all arms are pointers, there is no
// discriminant, so callers must use the arm matching their engine.
typedef union drv_devs_u {
	struct drv_ssds_s* ssds;
	struct drv_mems_s* mems;
	struct drv_pmems_s* pmems;
} drv_devs;

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
// The shared cold-start helpers below have a community body (drv_shared_ce.c)
// as well as an enterprise one (drv_shared_ee.c), so they are declared here.
// Enterprise-only cold-start helpers - no community body at all - are declared
// in drv_common_ee.h, which describes what each of its entries does in CE.
conflict_resolution_pol drv_cold_start_policy(const struct as_namespace_s* ns);
void drv_cold_start_adjust_cenotaph(const struct as_namespace_s* ns,
		const struct as_flat_record_s* flat, uint32_t block_void_time,
		struct as_index_s* r);
void drv_cold_start_init_repl_state(const struct as_namespace_s* ns,
		struct as_index_s* r);
void drv_cold_start_set_unrepl_stat(struct as_namespace_s* ns);
void drv_cold_start_init_xdr_state(const struct as_flat_record_s* flat,
		struct as_index_s* r);

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

// What the shared cold-start path needs about one device - both the ingest body
// and the fill-orig hook. 'ns' is the owning namespace, the same pointer as the
// ops' common->ns, carried here because the hooks get only this struct.
//
// The remaining fields must all describe the SAME device, which is why they
// travel as one argument - exploded, a caller could pair one device's file_id
// with another's wblock_state, which the unified wblock-state type leaves
// indistinguishable to the compiler.
typedef struct drv_cold_start_dev_s {
	struct as_namespace_s* ns;
	int file_id;
	uint64_t* inuse_size;
	drv_wblock_state* wblock_state;
} drv_cold_start_dev;

// Per-device tally of what cold start did with each record it read. The shared
// body bumps at most one member per record - truncated records are dropped
// without a tally. The ssd engine also borrows
// 'expired' and 'unique' for its sindex startup sweep (run_si_startup), which
// runs per device after cold start has finished with it.
typedef struct drv_cold_start_counters_s {
	uint64_t older; // records not inserted due to better existing one
	uint64_t expired; // records not inserted due to expiration
	uint64_t evicted; // records not inserted due to eviction
	uint64_t replace; // records reinserted
	uint64_t unique; // records inserted
	uint64_t unowned; // records not inserted due to unowned partition
	uint64_t dropped; // records not inserted due to dropped tree
	uint64_t unparsable; // unparsable records not inserted
} drv_cold_start_counters;

// Build a drv_cold_start_dev view from any engine's device struct - same idiom,
// and the same reason for it, as DRV_WBLOCK_POOL above.
#define DRV_COLD_START_DEV(_dev)                                               \
	__extension__({                                                            \
		__typeof__(_dev) _d = (_dev);                                          \
		(drv_cold_start_dev){                                                  \
			.ns = _d->ns,                                                      \
			.file_id = _d->file_id,                                            \
			.inuse_size = &_d->inuse_size,                                     \
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

//==========================================================
// Namespace storage state common to all engines.
//
// drv_ssds, drv_mems, and EE drv_pmems each embed this struct as their 'common'
// member. It is a named member, not a positional one - nothing may depend on
// it being first, and no code casts an engine struct to it. Engines pass
// &<engine>->common to the shared helpers below, so no shared code needs to
// know the engine structs, and a field added here is picked up by all three
// engines automatically.
//
// What belongs here versus what an engine should project per call: use a
// per-call view (drv_dev_view, DRV_WBLOCK_POOL) for anything the engine can
// cheaply assemble at the call site, and put state here only when it must
// outlive a call or be established once at init - devs, flush_lock, the writer
// slots - or when a cross-engine rule must live in shared code rather than be
// restated by each engine, such as "pmeta is written through the atomic
// writer".
//

// Publishes bytes [from, from + size) of the in-memory header image to the
// device's own header, persisting them where the engine has backing storage -
// a memory-only namespace has no device, so there its write is a copy into the
// stripe's header and nothing is made durable. 'header' must point at the
// image of device offset 0, so the offset to write at is 'from - header'.
// Implementations may write a larger, I/O-min aligned region containing that
// range - so the caller's image must be allocated out to the I/O-min-rounded
// end of it, not merely to 'from + size'. HI_IO_MIN_SIZE is the ceiling every
// engine's io-min is found within (see find_io_min_size), which is why
// drv_generic images are allocated ROUND_UP_GENERIC rather than
// sizeof(drv_generic).
//
// Engines supply this via drv_devices_common below; the drv_dev arm they read
// must be their own, which init guarantees by setting devs and the writers
// together.
typedef void (*drv_write_header_fn)(drv_dev dev, const uint8_t* header,
		const uint8_t* from, size_t size);

typedef struct drv_devices_common_s {
	// Set by <engine>_init_devices/_init_files, before anything else here.
	struct as_namespace_s* ns;

	// The in-memory image of the header's generic region, shared by all
	// devices. Allocated by <engine>_init_synchronous, i.e. after devs and the
	// writers - every helper below dereferences it, so none are callable
	// before that.
	drv_generic* generic;

	// Used only at startup, to determine whether to load a record. Populated
	// by <engine>_init_synchronous and read during cold-start sweep. Kept
	// resident for the life of the namespace rather than allocated and freed
	// around startup - deliberately, so there is no second lifetime here that
	// could disagree with the enclosing struct's.
	bool get_state_from_storage[AS_PARTITIONS];

	// Indexed by previous device-id to get new device-id. -1 means device is
	// "fresh" or absent. Used only at startup to fix index elements' file-id;
	// populated by <engine>_init_synchronous, read during warm-restart resume.
	// Resident for the same reason as above.
	int8_t device_translation[AS_STORAGE_MAX_DEVICES];

	// Used only at startup, set true if all devices are fresh.
	bool all_fresh;

	cf_mutex flush_lock;

	// The count, the handle array and the writers are written together by
	// drv_init_common_devs(), so they cannot disagree with each other - the
	// shared header helpers iterate devs[0 .. n_devices) and write through the
	// slots, and no caller assembles a handle array per call.
	//
	// Agreement with the enclosing allocation stays the caller's job: each
	// <engine>_init_* sizes its drv_<engine> array in a cf_malloc expression
	// and passes the same count here, so the two have to be derived from one
	// expression or devs[i] ends up pointing past the block. Nothing enforces
	// that the call happened either, so the fan-out asserts the array is set
	// and non-empty - skipping init leaves the memset(0) state, in which every
	// save would be a silent no-op and persist no header at all.
	int n_devices;
	drv_dev* devs;

	// PMEM needs both - its pmeta writes go through the atomic path; MEM and
	// SSD point both at their single writer.
	drv_write_header_fn write_header;
	drv_write_header_fn write_header_atomic;
} drv_devices_common;

//
// Device array setup.
//

// Establish the count, the handle array and the two writer slots in one write,
// so they cannot disagree. Engines call this from their own
// <engine>_init_common_devs(), which then fills each devs[i] with its own arm -
// the only part that needs the engine struct.
void drv_init_common_devs(drv_devices_common* dc, int n_devices,
		drv_write_header_fn write_header,
		drv_write_header_fn write_header_atomic);

//
// Namespace header / pmeta helpers.
//

// Every helper takes the caller's facility - engines pass their own AS_DRV_*
// literal, so a crash still names the engine even though these bodies are
// shared and the per-engine wrappers tail-call into them. The pmeta helpers
// crash on a bad partition index; the four save helpers crash on an unwired
// device array; the load helpers crash on a header image that does not exist
// yet.
//
// Locking - the half of the contract a caller cannot derive from the
// signatures. drv_save_regime, drv_save_roster_generation and drv_save_pmeta
// hold flush_lock across both the field they mutate and the fan-out to every
// device; drv_flush_pmeta mutates nothing and holds it across the fan-out
// alone. The four load/cache helpers take nothing, and that is deliberate
// rather than an oversight.
//
// flush_lock serializes device writes and nothing else. It does not protect
// generic->pmeta[pid] against drv_cache_pmeta, which mutates the same bytes
// unlocked - a cache landing between a flush's lock acquisition and its
// fan-out publishes a partially updated record. Serializing per-partition
// pmeta mutation is the caller's job, and today as_partition.lock does it,
// except on paths that run only with migrations disallowed.
//
// Thread identity is not the invariant, so do not reason from it: these
// already run on the exchange thread (rebalance), migrate threads (emigrate
// and immigrate done), and the info thread (revive).

void drv_load_regime(cf_log_context log_ctx, drv_devices_common* dc);
void drv_load_roster_generation(cf_log_context log_ctx, drv_devices_common* dc);
void drv_load_pmeta(cf_log_context log_ctx, const drv_devices_common* dc,
		struct as_partition_s* p);
void drv_cache_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		const struct as_partition_s* p);

void drv_save_regime(cf_log_context log_ctx, drv_devices_common* dc);
void drv_save_roster_generation(cf_log_context log_ctx, drv_devices_common* dc);
void drv_save_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		const struct as_partition_s* p);
void drv_flush_pmeta(cf_log_context log_ctx, drv_devices_common* dc,
		uint32_t start_pid, uint32_t n_partitions);

//
// Cold-start record ingest (shared MEM/SSD/PMEM path; the engine callbacks
// supply any enterprise behavior).
//

// Both hooks take the same drv_cold_start_dev the shared body reads, built once
// per sweep by the engine. Everything either hook needs is in it - the engine
// device struct itself is not required, so neither is a void*.
typedef void (*drv_cold_start_fill_orig_fn)(const drv_cold_start_dev* dev,
		const struct as_flat_record_s* flat, uint64_t rblock_id,
		const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s* tree,
		struct as_index_ref_s* r_ref);

typedef void (*drv_cold_start_record_update_fn)(drv_devs devs,
		const struct as_flat_record_s* flat,
		const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s* tree,
		struct as_index_ref_s* r_ref);

typedef void (*drv_cold_start_bad_flat_detail_fn)(const drv_cold_start_dev* dev,
		const struct as_flat_record_s* flat, uint32_t record_size,
		uint64_t rblock_id);

typedef struct drv_cold_start_add_ops_s {
	cf_log_context log_ctx;
	drv_devices_common* common;
	// Typed handle to the owning engine's device-array struct, for callbacks
	// that need it back (record_update_fn).
	drv_devs devs;
	// The per-device fields the shared body and both engine hooks read. Built
	// once per sweep and passed to the hooks by address, so no engine rebuilds
	// it per record. Grouped, for the reason drv_cold_start_dev exists -
	// exploded across this struct, a caller could pair one device's file_id
	// with another's wblock_state.
	drv_cold_start_dev dev;
	// This device's ingest tally - the shared body bumps exactly one member per
	// record. One pointer rather than eight, so the engine cannot cross-wire
	// them.
	drv_cold_start_counters* counters;
	// fill_orig_fn and record_update_fn are required - the shared body calls
	// them unguarded. The two bad_* hooks are optional: NULL selects the shared
	// body's own shorter warning instead of the engine's detailed one.
	drv_cold_start_fill_orig_fn fill_orig_fn;
	drv_cold_start_record_update_fn record_update_fn;
	drv_cold_start_bad_flat_detail_fn bad_packed_bins_fn;
	drv_cold_start_bad_flat_detail_fn bad_end_mark_fn;
	// Trailing end-mark size: END_MARK_SZ for SSD/MEM, 0 for PMEM (which writes
	// no end mark). Controls how the bins' end is bounded and whether the end
	// mark is verified - see drv_cold_start_add_record().
	uint32_t end_mark_sz;
} drv_cold_start_add_ops;

void drv_cold_start_add_record(const drv_cold_start_add_ops* ops,
		const struct as_flat_record_s* flat, uint64_t rblock_id,
		uint32_t record_size);

//
// Defrag (shared index-tree check; engine supplies move).
//

// 'dev' is the engine's own device struct, passed as the typed drv_dev handle -
// same convention as drv_wb_get. Shared code never dereferences it; it only
// hands it back to the engine's callback.
typedef void (*drv_defrag_move_fn)(drv_dev dev, uint32_t src_wblock_id,
		const struct as_flat_record_s* flat, struct as_index_s* r);

int drv_record_defrag(cf_log_context log_ctx, const char* dev_name,
		struct as_namespace_s* ns, int file_id, uint32_t wblock_id,
		const struct as_flat_record_s* flat, uint64_t rblock_id,
		drv_defrag_move_fn move_fn, drv_dev dev);

// Returns the record view to inspect for the slot at buf - either buf itself,
// a decrypted copy in an engine-owned buffer, or a sentinel record carrying
// only an effective magic (see PMEM's decrypt_flat).
//
// The scan itself never writes through buf. A hook may, but only when its own
// engine passed a buffer that engine owns - which this signature cannot
// express, because what buf points at differs per caller:
//
//   SSD  - a private cf_valloc staging buffer (ssd_defrag_wblock), so
//          ssd_defrag_prepare_record decrypts in place.
//   MEM  - mem_base_addr + offset, the live stripe. Passes no hook.
//   PMEM - pmem_base_addr + offset, live persistent memory. decrypt_flat
//          returns buf unchanged, a thread-buffer copy, or a sentinel, and
//          never writes through buf.
//
// So the const is load-bearing, not decoration: for two of the three callers
// buf is live storage, and a new hook that casts it away the way SSD's does
// would corrupt data rather than decrypt a scratch copy.
typedef const struct as_flat_record_s* (*drv_defrag_prepare_record_fn)(drv_dev dev,
		uint64_t abs_offset, const uint8_t* buf);

typedef int (*drv_defrag_on_record_fn)(drv_dev dev, uint32_t wblock_id,
		const struct as_flat_record_s* flat, uint64_t rblock_id);

// benign_magic: a magic value that is legitimate at the start of a used wblock
// and must not warn/abort the scan, or 0 for none - PMEM passes
// AS_FLAT_MAGIC_DIRTY (crash-recovery dirty records are routine there); MEM/SSD
// pass 0, so any bad leading magic warns and aborts as their scans always did.
// stop_at_gap: stop at the first non-record slot instead of skip-scanning
// the rest of the wblock - PMEM passes true when not commit-to-device
// (contiguous fill means the first gap is end-of-data); MEM/SSD pass false
// (must keep looking - e.g. commit-to-device without shadow, or increased
// write-block-size).
// Three of these arguments are only distinguishable by position, so state what
// they must be - none of the three swaps produces a diagnostic:
//   - (wblock_base_offset, wblock_id) is the byte offset of THIS wblock and its
//     index; reversed, the offset reads as 0 and every rblock_id derived from it
//     addresses the wrong extent.
//   - p_wblock_state is the state of THIS wblock -
//     &<dev>->wblock_state[wblock_id], not the device's array. Its inuse_sz is
//     the scan's stop condition, so a wblock_id paired with another wblock's
//     state scans the wrong extent silently.
//   - (benign_magic, stop_at_gap) is a magic value then a bool; all three
//     engines pass 0 and false respectively, which are the same two tokens in
//     either order.
// prepare_fn is optional - NULL means the record is read as-is. on_record_fn is
// required and called unguarded.
int drv_defrag_scan_wblock_records(cf_log_context log_ctx, const char* dev_name,
		const uint8_t* buf, uint64_t wblock_base_offset, uint32_t wblock_id,
		const drv_wblock_state* p_wblock_state, drv_dev dev,
		uint32_t benign_magic, bool stop_at_gap,
		drv_defrag_prepare_record_fn prepare_fn,
		drv_defrag_on_record_fn on_record_fn);

typedef void (*drv_defrag_process_wblock_fn)(drv_dev dev, uint32_t wblock_id,
		uint8_t* read_buf);

void drv_run_defrag_loop(const struct as_namespace_s* ns,
		cf_queue* defrag_wblock_q, drv_dev dev,
		drv_defrag_process_wblock_fn process_wblock, uint8_t* read_buf,
		uint32_t max_write_q_extra, bool sleep_after_wblock);

//
// Device maintenance thread (periodic jobs; engine callbacks).
//

#define DRV_MAINT_MAX_INTERVAL_US (1000 * 1000)
#define DRV_MAINT_LOG_STATS_INTERVAL_SEC 20
#define DRV_MAINT_LOG_STATS_INTERVAL_US                                        \
	(1000 * 1000 * DRV_MAINT_LOG_STATS_INTERVAL_SEC)
#define DRV_MAINT_FREE_POOL_INTERVAL_US (1000 * 1000 * 20)
#define DRV_MAINT_DEFRAG_FLUSH_INTERVAL_US (3UL * 1000 * 1000)

typedef void (*drv_maint_log_stats_fn)(drv_dev dev,
		uint64_t* p_prev_n_total_writes, uint64_t* p_prev_n_defrag_reads,
		uint64_t* p_prev_n_defrag_writes, uint64_t* p_prev_n_defrag_io_skips,
		uint64_t* p_prev_n_direct_frees, uint64_t* p_prev_n_tomb_raider_reads);

typedef void (*drv_maint_free_pool_fn)(drv_dev dev);

typedef uint64_t (*drv_maint_flush_max_us_fn)(drv_dev dev,
		const struct as_namespace_s* ns);

typedef void (*drv_maint_flush_current_fn)(drv_dev dev, uint8_t which,
		uint64_t* p_prev_n_writes_flush);

typedef void (*drv_maint_flush_defrag_fn)(drv_dev dev,
		uint64_t* p_prev_n_defrag_writes_flush);

typedef void (*drv_maint_defrag_sweep_fn)(drv_dev dev);

// Every member is required except flush_defrag_fn, which may be NULL - MEM has
// no defrag buffer to flush without a shadow. The shared loop calls the rest
// unguarded.
typedef struct drv_maintenance_ops_s {
	drv_maint_log_stats_fn log_stats_fn;
	drv_maint_free_pool_fn free_pool_fn;
	drv_maint_flush_max_us_fn flush_max_us_fn;
	drv_maint_flush_current_fn flush_current_fn;
	drv_maint_flush_defrag_fn flush_defrag_fn;
	// Only does the sweep - the shared loop owns the test-and-decrement of the
	// request flag.
	drv_maint_defrag_sweep_fn defrag_sweep_fn;
} drv_maintenance_ops;

// defrag_sweep_req is the device's sweep-request flag, passed separately
// because SSD and PMEM share one file-scope const ops table across all their
// devices - only device-independent function pointers can live in there.
void drv_run_maintenance_loop(drv_dev dev, const struct as_namespace_s* ns,
		uint32_t* defrag_sweep_req, const drv_maintenance_ops* ops);

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
// IO size alignment.
//

// Round bytes down to a multiple of a minimum IO operation size. Takes the size
// rather than a device so one definition serves every engine, and either a
// device's io_min_size or its shadow_io_min_size.
//
// The uint64_t parameter keeps the mask 64-bit. A 32-bit unsigned io_min would
// negate to a 32-bit mask that zero-extends, clearing the top half of bytes -
// with 512, an offset of 5 GiB would round down to 1 GiB.
static inline uint64_t
drv_bytes_down_to_io_min(uint64_t io_min, uint64_t bytes)
{
	return bytes & -io_min;
}

// Round bytes up to a multiple of a minimum IO operation size.
static inline uint64_t
drv_bytes_up_to_io_min(uint64_t io_min, uint64_t bytes)
{
	return (bytes + (io_min - 1)) & -io_min;
}

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
