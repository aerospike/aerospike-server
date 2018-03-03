/*
 * olock.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

/*
 * An object lock system allows fewer locks to be created
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include <citrusleaf/cf_digest.h>

#include <cf_mutex.h>


typedef struct olock_s {
	uint32_t n_locks;
	uint32_t mask;
	cf_mutex locks[];
} olock;

void olock_lock(olock *ol, cf_digest *d);
void olock_vlock(olock *ol, cf_digest *d, cf_mutex **vlock);
void olock_unlock(olock *ol, cf_digest *d);
olock *olock_create(uint32_t n_locks, bool mutex);
void olock_destroy(olock *o);

extern olock *g_record_locks;
