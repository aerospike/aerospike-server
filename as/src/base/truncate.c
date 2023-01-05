/*
 * truncate.c
 *
 * Copyright (C) 2017-2021 Aerospike, Inc.
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

#include "base/truncate.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/set_index.h"
#include "base/smd.h"
#include "sindex/gc.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

typedef struct truncate_job_s {
	as_namespace* ns;
	as_set* p_set;
	uint16_t set_id;
	bool use_set_index;
	uint64_t lut;
	uint32_t n_threads;
	uint32_t pid;
	uint64_t n_deleted;
} truncate_job;

typedef struct truncate_reduce_cb_info_s {
	as_namespace* ns;
	as_set* p_set;
	uint16_t set_id;
	uint64_t lut;
	as_index_tree* tree;
	uint64_t n_deleted;
} truncate_reduce_cb_info;

// Includes 1 for delimiter and 1 for null-terminator.
#define TRUNCATE_KEY_SIZE (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE)

// Detect excessive clock skew for warning purposes only.
static const uint64_t WARN_CLOCK_SKEW_MS = 1000UL * 5;


//==========================================================
// Forward declarations.
//

static bool truncate_smd_conflict_cb(const as_smd_item* existing_item, const as_smd_item* new_item);
static void truncate_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);

static void truncate_action_do(as_namespace* ns, const char* set_name, uint64_t lut);
static void truncate_action_undo(as_namespace* ns, const char* set_name);
static void* run_truncate(void* arg);
static bool truncate_reduce_cb(as_index_ref* r_ref, void* udata);


//==========================================================
// Inlines & macros.
//

static inline uint64_t
lut_from_smd(const as_smd_item* item)
{
	return strtoul(item->value, NULL, 10);
}


//==========================================================
// Public API.
//

void
as_truncate_init(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		truncate_startup_hash_init(ns);
	}

	as_smd_module_load(AS_SMD_MODULE_TRUNCATE, truncate_smd_accept_cb,
			truncate_smd_conflict_cb, NULL);
}

// SMD key is "ns-name|set-name" or "ns-name".
// SMD value is last-update-time as decimal string.
void
as_truncate_cmd(const char* ns_name, const char* set_name, const char* lut_str,
		cf_dyn_buf* db)
{
	char smd_key[TRUNCATE_KEY_SIZE];

	strcpy(smd_key, ns_name);

	if (set_name != NULL) {
		char* p_write = smd_key + strlen(ns_name);

		*p_write++ = TOK_DELIMITER;
		strcpy(p_write, set_name);
	}

	uint64_t now = cf_clepoch_milliseconds();
	uint64_t lut;

	if (lut_str == NULL) {
		// Use a last-update-time threshold of now.
		lut = now;

		cf_info(AS_TRUNCATE, "{%s} got command to truncate to now (%lu)",
				smd_key, lut);
	}
	else {
		uint64_t utc_nanosec = strtoul(lut_str, NULL, 0);

		// Last update time as human-readable UTC seconds.
		// TODO - make generic utility?
		char utc_sec[64] = { 0 };
		time_t utc_time = utc_nanosec / 1000000000;
		struct tm utc_tm;

		if (cf_log_is_using_local_time()) {
			localtime_r(&utc_time, &utc_tm);
			strftime(utc_sec, sizeof(utc_sec), "%b %d %Y %T GMT%z", &utc_tm);
		}
		else {
			gmtime_r(&utc_time, &utc_tm);
			strftime(utc_sec, sizeof(utc_sec), "%b %d %Y %T %Z", &utc_tm);
		}

		lut = cf_clepoch_ms_from_utc_ns(utc_nanosec);

		if (lut == 0) {
			cf_warning(AS_TRUNCATE, "command lut %s (%s) would truncate to 0",
					lut_str, utc_sec);
			cf_dyn_buf_append_string(db, "ERROR::would-truncate-to-0");
			return;
		}

		if (lut > now) {
			cf_warning(AS_TRUNCATE, "command lut %s (%s) is in the future",
					lut_str, utc_sec);
			cf_dyn_buf_append_string(db, "ERROR::would-truncate-in-the-future");
			return;
		}

		cf_info(AS_TRUNCATE, "{%s} got command to truncate to %s (%lu)",
				smd_key, utc_sec, lut);
	}

	char smd_value[13 + 1]; // 0xFFffffFFFF (40 bits) is 13 decimal characters

	sprintf(smd_value, "%lu", lut);

	// Broadcast the truncate command to all nodes (including this one).
	if (! as_smd_set_blocking(AS_SMD_MODULE_TRUNCATE, smd_key, smd_value, 0)) {
		cf_warning(AS_TRUNCATE, "timeout truncating %s to %lu", smd_key, lut);
		cf_dyn_buf_append_string(db, "ERROR::timeout");
		return;
	}

	cf_dyn_buf_append_string(db, "ok");
}

// SMD key is "ns-name|set-name" or "ns-name".
void
as_truncate_undo_cmd(const char* ns_name, const char* set_name, cf_dyn_buf* db)
{
	char smd_key[TRUNCATE_KEY_SIZE];

	strcpy(smd_key, ns_name);

	if (set_name != NULL) {
		char* p_write = smd_key + strlen(ns_name);

		*p_write++ = TOK_DELIMITER;
		strcpy(p_write, set_name);
	}

	cf_info(AS_TRUNCATE, "{%s} got command to undo truncate", smd_key);

	// Broadcast the truncate-undo command to all nodes (including this one).
	if (! as_smd_delete_blocking(AS_SMD_MODULE_TRUNCATE, smd_key, 0)) {
		cf_warning(AS_INFO, "{%s} timeout during undo truncate", smd_key);
		cf_dyn_buf_append_string(db, "ERROR::timeout");
		return;
	}

	cf_dyn_buf_append_string(db, "ok");
}

bool
as_truncate_now_is_truncated(struct as_namespace_s* ns, uint16_t set_id)
{
	uint64_t now = cf_clepoch_milliseconds();

	if (now < ns->truncate_lut) {
		return true;
	}

	as_set* p_set = as_namespace_get_set_by_id(ns, set_id);

	return p_set != NULL ? now < p_set->truncate_lut : false;
}

bool
as_truncate_record_is_truncated(const as_record* r, as_namespace* ns)
{
	if (r->last_update_time < ns->truncate_lut) {
		return true;
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	return p_set != NULL ? r->last_update_time < p_set->truncate_lut : false;
}


//==========================================================
// Local helpers - SMD callbacks.
//

static bool
truncate_smd_conflict_cb(const as_smd_item* existing_item,
		const as_smd_item* new_item)
{
	return lut_from_smd(new_item) > lut_from_smd(existing_item);
}

static void
truncate_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		as_smd_item* item = cf_vector_get_ptr(items, i);

		const char* ns_name = item->key;
		const char* tok = strchr(ns_name, TOK_DELIMITER);

		uint32_t ns_len = tok ? (uint32_t)(tok - ns_name) : strlen(ns_name);
		as_namespace* ns = as_namespace_get_bybuf((uint8_t*)ns_name, ns_len);

		if (ns == NULL) {
			cf_detail(AS_TRUNCATE, "skipping invalid ns");
			continue;
		}

		const char* set_name = tok ? tok + 1 : NULL;

		if (item->value != NULL) {
			uint64_t lut = lut_from_smd(item);

			if (accept_type == AS_SMD_ACCEPT_OPT_START) {
				truncate_action_startup(ns, set_name, lut);
			}
			else {
				truncate_action_do(ns, set_name, lut);
			}
		}
		else {
			truncate_action_undo(ns, set_name);
		}
	}
}


//==========================================================
// Local helpers - SMD callbacks' helpers.
//

static void
truncate_action_do(as_namespace* ns, const char* set_name, uint64_t lut)
{
	uint64_t now = cf_clepoch_milliseconds();

	if (lut > now + WARN_CLOCK_SKEW_MS) {
		cf_warning(AS_TRUNCATE, "lut is %lu ms in the future - clock skew?",
				lut - now);
	}

	as_set* p_set = NULL;
	uint16_t set_id = INVALID_SET_ID;

	uint32_t n_threads = as_load_uint32(&ns->n_truncate_threads);

	if (set_name != NULL) {
		set_id = as_namespace_get_set_id(ns, set_name);
		p_set = as_namespace_get_set_by_id(ns, set_id);

		if (p_set == NULL) {
			cf_info(AS_TRUNCATE, "{%s|%s} truncate for nonexistent set",
					ns->name, set_name);
			return;
		}

		if (lut <= p_set->truncate_lut) {
			cf_info(AS_TRUNCATE, "{%s|%s} truncate lut %lu <= vmap lut %lu",
					ns->name, set_name, lut, p_set->truncate_lut);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s|%s} truncating to %lu on %u threads",
				ns->name, set_name, lut, n_threads);

		p_set->truncate_lut = lut;
		p_set->truncating = true;
	}
	else {
		if (lut <= ns->truncate_lut) {
			cf_info(AS_TRUNCATE, "{%s} truncate lut %lu <= ns lut %lu",
					ns->name, lut, ns->truncate_lut);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s} truncating to %lu on %u threads", ns->name,
				lut, n_threads);

		ns->truncate_lut = lut;
		ns->truncating = true;
	}

	// Truncate to new last-update-time.

	truncate_job* jobi = cf_malloc(sizeof(truncate_job));

	*jobi = (truncate_job){
			.ns = ns,
			.p_set = p_set,
			.set_id = set_id,
			.use_set_index = p_set != NULL ? p_set->n_tombstones == 0 : false,
			.lut = lut,
			.n_threads = n_threads
	};

	for (uint32_t i = 0; i < n_threads; i++) {
		cf_thread_create_transient(run_truncate, jobi);
	}
}

static void
truncate_action_undo(as_namespace* ns, const char* set_name)
{
	if (set_name != NULL) {
		as_set* p_set = as_namespace_get_set_by_name(ns, set_name);

		if (p_set == NULL) {
			cf_info(AS_TRUNCATE, "{%s|%s} undo truncate for nonexistent set",
					ns->name, set_name);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s|%s} undoing truncate - was to %lu", ns->name,
				set_name, p_set->truncate_lut);

		p_set->truncate_lut = 0;
		p_set->truncating = false;
	}
	else {
		cf_info(AS_TRUNCATE, "{%s} undoing truncate - was to %lu", ns->name,
				ns->truncate_lut);

		ns->truncate_lut = 0;
		ns->truncating = false;
	}
}

static void*
run_truncate(void* arg)
{
	truncate_job* jobi = (truncate_job*)arg;
	as_namespace* ns = jobi->ns;
	as_set* p_set = jobi->p_set;
	uint64_t lut = jobi->lut;

	truncate_reduce_cb_info cbi = {
			.ns = ns,
			.p_set = jobi->p_set,
			.set_id = jobi->set_id,
			.lut = lut
	};

	uint32_t pid;

	while ((pid = as_faa_uint32(&jobi->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		as_index_tree* tree = rsv.tree;

		if (tree == NULL) {
			as_partition_release(&rsv);
			continue;
		}

		cbi.tree = tree;

		if (! (jobi->use_set_index && as_set_index_reduce(ns, tree,
				jobi->set_id, NULL, truncate_reduce_cb, (void*)&cbi))) {
			as_index_reduce(tree, truncate_reduce_cb, (void*)&cbi);
		}

		as_partition_release(&rsv);

		as_add_uint64(&jobi->n_deleted, (int64_t)cbi.n_deleted);
	}

	if (as_aaf_uint32(&jobi->n_threads, -1) == 0) {
		if (p_set != NULL) {
			if (p_set->truncate_lut == lut) {
				cf_info(AS_TRUNCATE, "{%s|%s} done truncate to %lu deleted %lu",
						ns->name, p_set->name, lut, jobi->n_deleted);

				p_set->truncating = false;
			}
			else {
				cf_info(AS_TRUNCATE, "{%s|%s} abandoned truncate to %lu deleted %lu - truncate-lut is now %lu",
						ns->name, p_set->name, lut, jobi->n_deleted,
						p_set->truncate_lut);
			}
		}
		else {
			if (ns->truncate_lut == lut) {
				cf_info(AS_TRUNCATE, "{%s} done truncate to %lu deleted %lu",
						ns->name, lut, jobi->n_deleted);

				ns->truncating = false;
			}
			else {
				cf_info(AS_TRUNCATE, "{%s} abandoned truncate to %lu deleted %lu - truncate-lut is now %lu",
						ns->name, lut, jobi->n_deleted, ns->truncate_lut);
			}
		}

		cf_free(jobi);
	}

	return NULL;
}

static bool
truncate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_record* r = r_ref->r;
	truncate_reduce_cb_info* cbi = (truncate_reduce_cb_info*)udata;
	as_namespace* ns = cbi->ns;
	as_set* p_set = cbi->p_set;
	as_index_tree* tree = cbi->tree;

	if (p_set != NULL) {
		if (p_set->truncate_lut != cbi->lut) {
			as_record_done(r_ref, ns);
			return false;
		}

		if (as_index_get_set_id(r) == cbi->set_id &&
				r->last_update_time < cbi->lut) {
			cbi->n_deleted++;

			if (ns->storage_data_in_memory ||
					ns->pi_xmem_type == CF_XMEM_TYPE_FLASH) {
				remove_from_sindex(ns, r_ref);
			}

			as_set_index_delete_live(ns, tree, r, r_ref->r_h);
			as_index_delete(tree, &r->keyd);
		}
	}
	else {
		if (ns->truncate_lut != cbi->lut) {
			as_record_done(r_ref, ns);
			return false;
		}

		if (r->last_update_time < cbi->lut) {
			cbi->n_deleted++;

			if (ns->storage_data_in_memory ||
					ns->pi_xmem_type == CF_XMEM_TYPE_FLASH) {
				remove_from_sindex(ns, r_ref);
			}

			as_set_index_delete_live(ns, tree, r, r_ref->r_h);
			as_index_delete(tree, &r->keyd);
		}
	}

	as_record_done(r_ref, ns);
	as_sindex_gc_record_throttle(ns);

	return true;
}
