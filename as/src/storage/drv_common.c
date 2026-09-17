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

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/flat.h"

//==========================================================
// Public API - shared code between storage engines.
//

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
