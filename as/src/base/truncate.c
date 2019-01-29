/*
 * truncate.c
 *
 * Copyright (C) 2017-2018 Aerospike, Inc.
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

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/smd.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

typedef struct truncate_reduce_cb_info_s {
	as_namespace* ns;
	as_index_tree* tree;
	int64_t n_deleted;
} truncate_reduce_cb_info;

static const uint32_t NUM_TRUNCATE_THREADS = 4;

// Includes 1 for delimiter and 1 for null-terminator.
#define TRUNCATE_KEY_SIZE (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE)

// System metadata key format token.
#define TOK_DELIMITER ('|')

// Detect excessive clock skew for warning purposes only.
static const uint64_t WARN_CLOCK_SKEW_MS = 1000UL * 5;


//==========================================================
// Forward declarations.
//

static bool truncate_smd_conflict_cb(const as_smd_item* existing_item, const as_smd_item* new_item);
static void truncate_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);

static void truncate_action_do(as_namespace* ns, const char* set_name, uint64_t lut);
static void truncate_action_undo(as_namespace* ns, const char* set_name);
static void truncate_all(as_namespace* ns);
static void* run_truncate(void* arg);
static void truncate_finish(as_namespace* ns);
static void truncate_reduce_cb(as_index_ref* r_ref, void* udata);


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
as_truncate_init(as_namespace* ns)
{
	truncate_startup_hash_init(ns);

	ns->truncate.state = TRUNCATE_IDLE;
	cf_mutex_init(&ns->truncate.state_lock);
}

void
as_truncate_init_smd()
{
	as_smd_module_load(AS_SMD_MODULE_TRUNCATE, truncate_smd_accept_cb,
			truncate_smd_conflict_cb, NULL);
}

// SMD key is "ns-name|set-name" or "ns-name".
// SMD value is last-update-time as decimal string.
bool
as_truncate_cmd(const char* ns_name, const char* set_name, const char* lut_str)
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

		if (cf_fault_is_using_local_time()) {
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
			return false;
		}

		if (lut > now) {
			cf_warning(AS_TRUNCATE, "command lut %s (%s) is in the future",
					lut_str, utc_sec);
			return false;
		}

		cf_info(AS_TRUNCATE, "{%s} got command to truncate to %s (%lu)",
				smd_key, utc_sec, lut);
	}

	char smd_value[13 + 1]; // 0xFFffffFFFF (40 bits) is 13 decimal characters

	sprintf(smd_value, "%lu", lut);

	// Broadcast the truncate command to all nodes (including this one).
	return as_smd_set_blocking(AS_SMD_MODULE_TRUNCATE, smd_key, smd_value, 0);
}

// SMD key is "ns-name|set-name" or "ns-name".
bool
as_truncate_undo_cmd(const char* ns_name, const char* set_name)
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
	return as_smd_delete_blocking(AS_SMD_MODULE_TRUNCATE, smd_key, 0);
}

bool
as_truncate_now_is_truncated(struct as_namespace_s* ns, uint16_t set_id)
{
	uint64_t now = cf_clepoch_milliseconds();

	if (now < ns->truncate.lut) {
		return true;
	}

	as_set* p_set = as_namespace_get_set_by_id(ns, set_id);

	return p_set != NULL ? now < p_set->truncate_lut : false;
}

bool
as_truncate_record_is_truncated(const as_record* r, as_namespace* ns)
{
	if (r->last_update_time < ns->truncate.lut) {
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

	if (set_name != NULL) {
		as_set* p_set = as_namespace_get_set_by_name(ns, set_name);

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

		cf_info(AS_TRUNCATE, "{%s|%s} truncating to %lu", ns->name, set_name,
				lut);

		p_set->truncate_lut = lut;
	}
	else {
		if (lut <= ns->truncate.lut) {
			cf_info(AS_TRUNCATE, "{%s} truncate lut %lu <= ns lut %lu",
					ns->name, lut, ns->truncate.lut);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s} truncating to %lu", ns->name, lut);

		ns->truncate.lut = lut;
	}

	// Truncate to new last-update-time.

	cf_mutex_lock(&ns->truncate.state_lock);

	switch (ns->truncate.state) {
	case TRUNCATE_IDLE:
		cf_info(AS_TRUNCATE, "{%s} starting truncate", ns->name);
		truncate_all(ns);
		break;
	case TRUNCATE_RUNNING:
		cf_info(AS_TRUNCATE, "{%s} flagging truncate to restart", ns->name);
		ns->truncate.state = TRUNCATE_RESTART;
		break;
	case TRUNCATE_RESTART:
		cf_info(AS_TRUNCATE, "{%s} truncate already will restart", ns->name);
		break;
	default:
		cf_crash(AS_TRUNCATE, "bad truncate state %d", ns->truncate.state);
		break;
	}

	cf_mutex_unlock(&ns->truncate.state_lock);
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
	}
	else {
		cf_info(AS_TRUNCATE, "{%s} undoing truncate - was to %lu", ns->name,
				ns->truncate.lut);

		ns->truncate.lut = 0;
	}
}

// Called under truncate lock.
static void
truncate_all(as_namespace* ns)
{
	// TODO - skipping sindex deletion shortcut - can't do that if we want to
	// keep writing through set truncates. Is this ok?

	ns->truncate.state = TRUNCATE_RUNNING;
	cf_atomic32_set(&ns->truncate.n_threads_running, NUM_TRUNCATE_THREADS);
	cf_atomic32_set(&ns->truncate.pid, -1);

	cf_atomic64_set(&ns->truncate.n_records_this_run, 0);

	for (uint32_t i = 0; i < NUM_TRUNCATE_THREADS; i++) {
		cf_thread_create_detached(run_truncate, (void*)ns);
	}
}

static void*
run_truncate(void* arg)
{
	as_namespace* ns = (as_namespace*)arg;
	uint32_t pid;

	while ((pid = (uint32_t)cf_atomic32_incr(&ns->truncate.pid)) <
			AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(ns, pid, &rsv);

		truncate_reduce_cb_info cb_info = { .ns = ns, .tree = rsv.tree };

		as_index_reduce(rsv.tree, truncate_reduce_cb, (void*)&cb_info);
		as_partition_release(&rsv);

		cf_atomic64_add(&ns->truncate.n_records_this_run, cb_info.n_deleted);
	}

	truncate_finish(ns);

	return NULL;
}

static void
truncate_finish(as_namespace* ns)
{
	if (cf_atomic32_decr(&ns->truncate.n_threads_running) == 0) {
		cf_mutex_lock(&ns->truncate.state_lock);

		ns->truncate.n_records += ns->truncate.n_records_this_run;

		cf_info(AS_TRUNCATE, "{%s} truncated records (%lu,%lu)", ns->name,
				ns->truncate.n_records_this_run, ns->truncate.n_records);

		switch (ns->truncate.state) {
		case TRUNCATE_RUNNING:
			cf_info(AS_TRUNCATE, "{%s} done truncate", ns->name);
			ns->truncate.state = TRUNCATE_IDLE;
			break;
		case TRUNCATE_RESTART:
			cf_info(AS_TRUNCATE, "{%s} restarting truncate", ns->name);
			truncate_all(ns);
			break;
		case TRUNCATE_IDLE:
		default:
			cf_crash(AS_TRUNCATE, "bad truncate state %d", ns->truncate.state);
			break;
		}

		cf_mutex_unlock(&ns->truncate.state_lock);
	}
}

static void
truncate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_record* r = r_ref->r;
	truncate_reduce_cb_info* cb_info = (truncate_reduce_cb_info*)udata;
	as_namespace* ns = cb_info->ns;

	if (r->last_update_time < ns->truncate.lut) {
		cb_info->n_deleted++;
		record_delete_adjust_sindex(r, ns);
		as_index_delete(cb_info->tree, &r->keyd);
		as_record_done(r_ref, ns);
		return;
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	// Delete records not updated since their set's threshold last-update-time.
	if (p_set != NULL && r->last_update_time < p_set->truncate_lut) {
		cb_info->n_deleted++;
		record_delete_adjust_sindex(r, ns);
		as_index_delete(cb_info->tree, &r->keyd);
	}

	as_record_done(r_ref, ns);
}
