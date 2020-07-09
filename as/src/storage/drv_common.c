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
#include <unistd.h>

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

	as_set *p_set;

	if (cf_vmapx_get_by_name_w_len(ns->p_sets_vmap, opt_meta->set_name,
			opt_meta->set_name_len, (void**)&p_set) != CF_VMAPX_OK) {
		return true;
	}

	return ! IS_SET_EVICTION_DISABLED(p_set);
}

void
drv_apply_opt_meta(as_record* r, as_namespace* ns,
		const as_flat_opt_meta* opt_meta)
{
	// Set record's set-id. (If it already has one, assume they're the same.)
	if (as_index_get_set_id(r) == INVALID_SET_ID && opt_meta->set_name) {
		as_index_set_set_w_len(r, ns, opt_meta->set_name,
				opt_meta->set_name_len, false);
	}

	// Store or drop the key according to the props we read.
	as_record_finalize_key(r, ns, opt_meta->key, opt_meta->key_size);
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
