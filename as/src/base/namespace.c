/*
 * namespace.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hash_math.h"

#include "dynbuf.h"
#include "fault.h"
#include "hist.h"
#include "linear_hist.h"
#include "vmapx.h"
#include "xmem.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "fabric/roster.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//


//==========================================================
// Globals.
//


//==========================================================
// Forward declarations.
//

static void append_set_props(as_set *p_set, cf_dyn_buf *db);


//==========================================================
// Inlines & macros.
//

static inline uint32_t
ns_name_hash(char *name)
{
	uint32_t hv = cf_hash_fnv32((const uint8_t *)name, strlen(name));

	// Don't collide with a ns-id.
	if (hv <= AS_NAMESPACE_SZ) {
		hv += AS_NAMESPACE_SZ;
	}

	return hv;
}


//==========================================================
// Public API.
//

as_namespace *
as_namespace_create(char *name)
{
	cf_assert_nostack(strlen(name) < AS_ID_NAMESPACE_SZ,
			AS_NAMESPACE, "{%s} namespace name too long (max length is %u)",
			name, AS_ID_NAMESPACE_SZ - 1);

	cf_assert_nostack(g_config.n_namespaces < AS_NAMESPACE_SZ,
			AS_NAMESPACE, "too many namespaces (max is %u)", AS_NAMESPACE_SZ);

	uint32_t namehash = ns_name_hash(name);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (strcmp(ns->name, name) == 0) {
			cf_crash_nostack(AS_NAMESPACE, "{%s} duplicate namespace", name);
		}

		// Check for CE also, in case deployment later becomes EE with XDR.
		if (ns->namehash == namehash) {
			cf_crash_nostack(AS_XDR, "{%s} {%s} namespace name hashes collide",
					ns->name, name);
		}
	}

	as_namespace *ns = cf_malloc(sizeof(as_namespace));

	g_config.namespaces[g_config.n_namespaces++] = ns;

	// Set all members 0/NULL/false to start with.
	memset(ns, 0, sizeof(as_namespace));

	strcpy(ns->name, name);
	ns->id = g_config.n_namespaces; // note that id is 1-based
	ns->namehash = namehash;

	ns->jem_arena = cf_alloc_create_arena();
	cf_info(AS_NAMESPACE, "{%s} uses JEMalloc arena %d", name, ns->jem_arena);

	ns->cold_start = false; // try warm or cool restart unless told not to
	ns->arena = NULL; // can't create the arena until the configuration has been done

	//--------------------------------------------
	// Non-0/NULL/false configuration defaults.
	//

	ns->xmem_type = CF_XMEM_TYPE_UNDEFINED;

	ns->cfg_replication_factor = 2;
	ns->replication_factor = 0; // gets set on rebalance

	ns->sets_enable_xdr = true; // ship all the sets by default
	ns->ns_allow_nonxdr_writes = true; // allow nonxdr writes by default
	ns->ns_allow_xdr_writes = true; // allow xdr writes by default
	cf_vector_pointer_init(&ns->xdr_dclist_v, 3, 0);

	ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF;
	ns->evict_hist_buckets = 10000; // for 30 day TTL, bucket width is 4 minutes 20 seconds
	ns->evict_tenths_pct = 5; // default eviction amount is 0.5%
	ns->hwm_disk_pct = 50; // evict when device usage exceeds 50%
	ns->hwm_memory_pct = 60; // evict when memory usage exceeds 50% of namespace memory-size
	ns->index_stage_size = 1024L * 1024L * 1024L; // 1G
	ns->migrate_order = 5;
	ns->migrate_retransmit_ms = 1000 * 5; // 5 seconds
	ns->migrate_sleep = 1;
	ns->nsup_hist_period = 60 * 60; // 1 hour
	ns->nsup_period = 2 * 60; // 2 minutes
	ns->n_nsup_threads = 1;
	ns->read_consistency_level = AS_READ_CONSISTENCY_LEVEL_PROTO;
	ns->stop_writes_pct = 90; // stop writes when 90% of either memory or disk is used
	ns->tomb_raider_eligible_age = 60 * 60 * 24; // 1 day
	ns->tomb_raider_period = 60 * 60 * 24; // 1 day
	ns->transaction_pending_limit = 20;
	ns->tree_shared.n_sprigs = NUM_LOCK_PAIRS; // can't be less than number of lock pairs, 256 per partition
	ns->write_commit_level = AS_WRITE_COMMIT_LEVEL_PROTO;

	ns->mounts_hwm_pct = 80; // evict when persisted index usage exceeds 80%

	ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
	ns->storage_data_in_memory = true;
	// Note - default true is consistent with AS_STORAGE_ENGINE_MEMORY, but
	// cfg.c will set default false for AS_STORAGE_ENGINE_SSD.

	ns->storage_scheduler_mode = NULL; // null indicates default is to not change scheduler mode
	ns->storage_write_block_size = 1024 * 1024;
	ns->storage_defrag_lwm_pct = 50; // defrag if occupancy of block is < 50%
	ns->storage_defrag_sleep = 1000; // sleep this many microseconds between each wblock
	ns->storage_defrag_startup_minimum = 10; // defrag until >= 10% disk is writable before joining cluster
	ns->storage_encryption = AS_ENCRYPTION_AES_128;
	ns->storage_flush_max_us = 1000 * 1000; // wait this many microseconds before flushing inactive current write buffer (0 = never)
	ns->storage_max_write_cache = 1024 * 1024 * 64;
	ns->storage_min_avail_pct = 5; // stop writes when < 5% disk is writable
	ns->storage_post_write_queue = 256; // number of wblocks per device used as post-write cache
	ns->storage_tomb_raider_sleep = 1000; // sleep this many microseconds between each device read

	ns->sindex_num_partitions = DEFAULT_PARTITIONS_PER_INDEX;

	ns->geo2dsphere_within_strict = true;
	ns->geo2dsphere_within_min_level = 1;
	ns->geo2dsphere_within_max_level = 30;
	ns->geo2dsphere_within_max_cells = 12;
	ns->geo2dsphere_within_level_mod = 1;
	ns->geo2dsphere_within_earth_radius_meters = 6371000;  // Wikipedia, mean

	return ns;
}


void
as_namespaces_init(bool cold_start_cmd, uint32_t instance)
{
	as_namespaces_setup(cold_start_cmd, instance);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		// Done with temporary sets configuration array.
		if (ns->sets_cfg_array) {
			cf_free(ns->sets_cfg_array);
		}

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_init(ns, pid);
		}

		as_namespace_finish_setup(ns, instance);

		as_truncate_init(ns);
		as_sindex_init(ns);
	}

	as_roster_init_smd();
	as_truncate_init_smd();
	as_sindex_init_smd(); // before as_storage_init() populates the indexes
}


bool
as_namespace_configure_sets(as_namespace *ns)
{
	for (uint32_t i = 0; i < ns->sets_cfg_count; i++) {
		uint32_t idx;
		cf_vmapx_err result = cf_vmapx_put_unique(ns->p_sets_vmap,
				ns->sets_cfg_array[i].name, &idx);

		if (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS) {
			as_set* p_set = NULL;

			if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx,
					(void**)&p_set)) != CF_VMAPX_OK) {
				// Should be impossible - just verified idx.
				cf_crash(AS_NAMESPACE, "vmap error %d", result);
			}

			// Transfer configurable metadata.
			p_set->stop_writes_count = ns->sets_cfg_array[i].stop_writes_count;
			p_set->disable_eviction = ns->sets_cfg_array[i].disable_eviction;
			p_set->enable_xdr = ns->sets_cfg_array[i].enable_xdr;
		}
		else {
			// Maybe exceeded max sets allowed, but try failing gracefully.
			cf_warning(AS_NAMESPACE, "vmap error %d", result);
			return false;
		}
	}

	return true;
}


as_namespace *
as_namespace_get_byname(char *name)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (strcmp(ns->name, name) == 0) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_byid(uint32_t id)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (id == ns->id) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bybuf(uint8_t *buf, size_t len)
{
	if (len >= AS_ID_NAMESPACE_SZ) {
		return NULL;
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace *ns = g_config.namespaces[ns_ix];

		if (memcmp(buf, ns->name, len) == 0 && ns->name[len] == 0) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bymsgfield(as_msg_field *fp)
{
	return as_namespace_get_bybuf(fp->data, as_msg_field_get_value_sz(fp));
}


const char *
as_namespace_get_set_name(as_namespace *ns, uint16_t set_id)
{
	// Note that set_id is 1-based, but cf_vmap index is 0-based.
	// (This is because 0 in the index structure means 'no set'.)

	if (set_id == INVALID_SET_ID) {
		return NULL;
	}

	as_set *p_set;

	return cf_vmapx_get_by_index(ns->p_sets_vmap, (uint32_t)set_id - 1,
			(void**)&p_set) == CF_VMAPX_OK ? p_set->name : NULL;
}


uint16_t
as_namespace_get_set_id(as_namespace *ns, const char *set_name)
{
	uint32_t idx;

	return cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) == CF_VMAPX_OK ?
			(uint16_t)(idx + 1) : INVALID_SET_ID;
}


// At the moment this is only used by the enterprise build security feature.
uint16_t
as_namespace_get_create_set_id(as_namespace *ns, const char *set_name)
{
	if (! set_name) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "null set name");
		return INVALID_SET_ID;
	}

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx);

	if (result == CF_VMAPX_OK) {
		return (uint16_t)(idx + 1);
	}

	if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		result = cf_vmapx_put_unique(ns->p_sets_vmap, set_name, &idx);

		if (result == CF_VMAPX_ERR_NAME_EXISTS) {
			return (uint16_t)(idx + 1);
		}

		if (result == CF_VMAPX_ERR_BAD_PARAM) {
			cf_warning(AS_NAMESPACE, "set name %s too long", set_name);
			return INVALID_SET_ID;
		}

		if (result == CF_VMAPX_ERR_FULL) {
			cf_warning(AS_NAMESPACE, "can't add %s (at sets limit)", set_name);
			return INVALID_SET_ID;
		}

		if (result != CF_VMAPX_OK) {
			// Currently, remaining errors are all some form of out-of-memory.
			cf_warning(AS_NAMESPACE, "can't add %s (%d)", set_name, result);
			return INVALID_SET_ID;
		}

		return (uint16_t)(idx + 1);
	}

	// Should be impossible.
	cf_warning(AS_NAMESPACE, "unexpected error %d", result);
	return INVALID_SET_ID;
}


int
as_namespace_set_set_w_len(as_namespace *ns, const char *set_name, size_t len,
		uint16_t *p_set_id, bool apply_restrictions)
{
	as_set *p_set;

	if (as_namespace_get_create_set_w_len(ns, set_name, len, &p_set,
			p_set_id) != 0) {
		return -1;
	}

	if (apply_restrictions && as_set_stop_writes(p_set)) {
		return -2;
	}

	cf_atomic64_incr(&p_set->n_objects);

	return 0;
}


int
as_namespace_get_create_set_w_len(as_namespace *ns, const char *set_name,
		size_t len, as_set **pp_set, uint16_t *p_set_id)
{
	cf_assert(set_name, AS_NAMESPACE, "null set name");
	cf_assert(len != 0, AS_NAMESPACE, "empty set name");

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index_w_len(ns->p_sets_vmap, set_name,
			len, &idx);

	if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		// Special case handling for name too long.
		if (len >= AS_SET_NAME_MAX_SIZE) {
			char bad_name[AS_SET_NAME_MAX_SIZE];

			memcpy(bad_name, set_name, AS_SET_NAME_MAX_SIZE - 1);
			bad_name[AS_SET_NAME_MAX_SIZE - 1] = 0;

			cf_warning(AS_NAMESPACE, "set name %s... too long", bad_name);
			return -1;
		}

		result = cf_vmapx_put_unique_w_len(ns->p_sets_vmap, set_name, len,
				&idx);

		// Since this function can be called via many functions simultaneously.
		// Need to handle race, So handle CF_VMAPX_ERR_NAME_EXISTS.
		if (result == CF_VMAPX_ERR_FULL) {
			cf_warning(AS_NAMESPACE, "at set names limit, can't add set");
			return -1;
		}

		if (result != CF_VMAPX_OK && result != CF_VMAPX_ERR_NAME_EXISTS) {
			cf_warning(AS_NAMESPACE, "error %d, can't add set", result);
			return -1;
		}
	}
	else if (result != CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected error %d", result);
		return -1;
	}

	if (pp_set) {
		if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx,
				(void**)pp_set)) != CF_VMAPX_OK) {
			// Should be impossible - just verified idx.
			cf_warning(AS_NAMESPACE, "unexpected error %d", result);
			return -1;
		}
	}

	if (p_set_id) {
		*p_set_id = (uint16_t)(idx + 1);
	}

	return 0;
}


as_set *
as_namespace_get_set_by_name(as_namespace *ns, const char *set_name)
{
	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) != CF_VMAPX_OK) {
		return NULL;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible - just verified idx.
		cf_crash(AS_NAMESPACE, "unexpected vmap error");
	}

	return p_set;
}


as_set *
as_namespace_get_set_by_id(as_namespace *ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return NULL;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected - record with set-id not in vmap");
		return NULL;
	}

	return p_set;
}


as_set *
as_namespace_get_record_set(as_namespace *ns, const as_record *r)
{
	return as_namespace_get_set_by_id(ns, as_index_get_set_id(r));
}


void
as_namespace_get_set_info(as_namespace *ns, const char *set_name,
		cf_dyn_buf *db)
{
	as_set *p_set;

	if (set_name) {
		if (cf_vmapx_get_by_name(ns->p_sets_vmap, set_name, (void**)&p_set) ==
				CF_VMAPX_OK) {
			append_set_props(p_set, db);
		}

		return;
	}

	for (uint32_t idx = 0; idx < cf_vmapx_count(ns->p_sets_vmap); idx++) {
		if (cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set) ==
				CF_VMAPX_OK) {
			cf_dyn_buf_append_string(db, "ns=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_string(db, "set=");
			cf_dyn_buf_append_string(db, p_set->name);
			cf_dyn_buf_append_char(db, ':');
			append_set_props(p_set, db);
		}
	}
}


void
as_namespace_adjust_set_memory(as_namespace *ns, uint16_t set_id,
		int64_t delta_bytes)
{
	if (set_id == INVALID_SET_ID) {
		return;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) !=
			CF_VMAPX_OK) {
		cf_warning(AS_NAMESPACE, "set-id %u - failed vmap get", set_id);
		return;
	}

	if (cf_atomic64_add(&p_set->n_bytes_memory, delta_bytes) < 0) {
		cf_warning(AS_NAMESPACE, "set-id %u - negative memory!", set_id);
	}
}


void
as_namespace_release_set_id(as_namespace *ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) !=
			CF_VMAPX_OK) {
		return;
	}

	if (cf_atomic64_decr(&p_set->n_objects) < 0) {
		cf_warning(AS_NAMESPACE, "set-id %u - negative objects!", set_id);
	}
}


void
as_namespace_get_bins_info(as_namespace *ns, cf_dyn_buf *db, bool show_ns)
{
	if (show_ns) {
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');
	}

	if (ns->single_bin) {
		cf_dyn_buf_append_string(db, "[single-bin]");
	}
	else {
		uint32_t bin_count = cf_vmapx_count(ns->p_bin_name_vmap);

		cf_dyn_buf_append_string(db, "bin_names=");
		cf_dyn_buf_append_uint32(db, bin_count);
		cf_dyn_buf_append_string(db, ",bin_names_quota=");
		cf_dyn_buf_append_uint32(db, BIN_NAMES_QUOTA);

		for (uint16_t i = 0; i < (uint16_t)bin_count; i++) {
			cf_dyn_buf_append_char(db, ',');
			cf_dyn_buf_append_string(db, as_bin_get_name_from_id(ns, i));
		}
	}

	if (show_ns) {
		cf_dyn_buf_append_char(db, ';');
	}
}


// e.g. for ttl:
// units=seconds:hist-width=2582800:bucket-width=25828:buckets=0,0,0 ...
//
// e.g. for object-size-linear:
// units=bytes:hist-width=131072:bucket-width=128:buckets=16000,8000,0,0 ...
//
// e.g. for object-size:
// units=bytes:[64-128)=16000:[128-256)=8000 ...
void
as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name,
		cf_dyn_buf *db)
{
	if (set_name == NULL || set_name[0] == 0) {
		if (strcmp(hist_name, "ttl") == 0) {
			linear_hist_get_info(ns->ttl_hist, db);
		}
		else if (strcmp(hist_name, "object-size") == 0) {
			if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
				histogram_get_info(ns->obj_size_log_hist, db);
			}
			else {
				cf_dyn_buf_append_string(db, "hist-not-applicable");
			}
		}
		else if (strcmp(hist_name, "object-size-linear") == 0) {
			if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
				linear_hist_get_info(ns->obj_size_lin_hist, db);
			}
			else {
				cf_dyn_buf_append_string(db, "hist-not-applicable");
			}
		}
		else {
			cf_dyn_buf_append_string(db, "error-unknown-hist-name");
		}

		return;
	}

	uint16_t set_id = as_namespace_get_set_id(ns, set_name);

	if (set_id == INVALID_SET_ID) {
		cf_dyn_buf_append_string(db, "error-unknown-set-name");
		return;
	}

	if (strcmp(hist_name, "ttl") == 0) {
		if (ns->set_ttl_hists[set_id]) {
			linear_hist_get_info(ns->set_ttl_hists[set_id], db);
		}
		else {
			cf_dyn_buf_append_string(db, "hist-unavailable");
		}
	}
	else if (strcmp(hist_name, "object-size") == 0) {
		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			if (ns->set_obj_size_log_hists[set_id]) {
				histogram_get_info(ns->set_obj_size_log_hists[set_id], db);
			}
			else {
				cf_dyn_buf_append_string(db, "hist-unavailable");
			}
		}
		else {
			cf_dyn_buf_append_string(db, "hist-not-applicable");
		}
	}
	else if (strcmp(hist_name, "object-size-linear") == 0) {
		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			if (ns->set_obj_size_lin_hists[set_id]) {
				linear_hist_get_info(ns->set_obj_size_lin_hists[set_id], db);
			}
			else {
				cf_dyn_buf_append_string(db, "hist-unavailable");
			}
		}
		else {
			cf_dyn_buf_append_string(db, "hist-not-applicable");
		}
	}
	else {
		cf_dyn_buf_append_string(db, "error-unknown-hist-name");
	}
}


//==========================================================
// Local helpers.
//

static void
append_set_props(as_set *p_set, cf_dyn_buf *db)
{
	// Statistics:

	cf_dyn_buf_append_string(db, "objects=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_objects));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "tombstones=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_tombstones));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "memory_data_bytes=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_bytes_memory));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "truncate_lut=");
	cf_dyn_buf_append_uint64(db, p_set->truncate_lut);
	cf_dyn_buf_append_char(db, ':');

	// Configuration:

	cf_dyn_buf_append_string(db, "stop-writes-count=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->stop_writes_count));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "set-enable-xdr=");

	if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_TRUE) {
		cf_dyn_buf_append_string(db, "true");
	}
	else if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_FALSE) {
		cf_dyn_buf_append_string(db, "false");
	}
	else if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_DEFAULT) {
		cf_dyn_buf_append_string(db, "use-default");
	}
	else {
		cf_dyn_buf_append_uint32(db, cf_atomic32_get(p_set->enable_xdr));
	}

	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "disable-eviction=");
	cf_dyn_buf_append_bool(db, IS_SET_EVICTION_DISABLED(p_set));
	cf_dyn_buf_append_char(db, ';');
}
