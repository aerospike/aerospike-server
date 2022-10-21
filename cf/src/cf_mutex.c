/*
 * cf_mutex.c
 *
 * Copyright (C) 2017-2022 Aerospike, Inc.
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

#include <cf_mutex.h>

#include <linux/futex.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"

#include "log.h"


//==========================================================
// Typedefs & constants.
//

#define FUTEX_SPIN_MAX 100


//==========================================================
// Inlines & macros.
//

inline static void
sys_futex(void* uaddr, int op, int val)
{
	syscall(SYS_futex, uaddr, op, val, NULL, NULL, 0);
}

#define unlikely(__expr) __builtin_expect(!! (__expr), 0)
#define likely(__expr) __builtin_expect(!! (__expr), 1)


//==========================================================
// Public API - cf_mutex.
//

void
cf_mutex_lock(cf_mutex* m)
{
	uint32_t zero = 0;

	if (likely(as_cas_acq((uint32_t*)m, &zero, 1))) {
		return; // was not locked
	}

	if (as_load_rlx(&m->u32) == 2) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}

	while (as_fas_acq((uint32_t*)m, 2) != 0) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}
}

void
cf_mutex_unlock(cf_mutex* m)
{
	uint32_t check = as_fas_rls((uint32_t*)m, 0);

	if (unlikely(check == 2)) {
		sys_futex(m, FUTEX_WAKE_PRIVATE, 1);
	}
	else if (unlikely(check == 0)) {
		cf_crash(CF_MISC, "cf_mutex_unlock() on already unlocked mutex");
	}
}

// Return true if lock success.
bool
cf_mutex_trylock(cf_mutex* m)
{
	uint32_t zero = 0;

	if (likely(as_cas_acq((uint32_t*)m, &zero, 1))) {
		return true; // was not locked
	}

	return false;
}

void
cf_mutex_lock_spin(cf_mutex* m)
{
	int i = 0;

	while (true) {
		uint32_t zero = 0;

		if (as_cas_acq((uint32_t*)m, &zero, 1)) {
			return; // was not locked
		}

		for (; i < FUTEX_SPIN_MAX; i++) {
			if (as_load_rlx((uint32_t*)m) == 0) {
				break;
			}

			as_arch_pause();
		}

		if (i == FUTEX_SPIN_MAX) {
			break;
		}
	}

	if (m->u32 == 2) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}

	while (as_fas_acq((uint32_t*)m, 2) != 0) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}
}

void
cf_mutex_unlock_spin(cf_mutex* m)
{
	uint32_t check = as_fas_rls((uint32_t*)m, 0);

	if (unlikely(check == 2)) {
		// Spin and hope someone takes the lock.
		for (int i = 0; i < FUTEX_SPIN_MAX; i++) {
			// Try to hand off contended condition.
			if (as_load_rlx(&m->u32) != 0) {
				uint32_t v = 1;

				if (as_cas_rlx((uint32_t*)m, &v, 2)) {
					return; // someone else took the lock
				}

				if (v == 0) {
					break; // failed to hand off
				}
			}

			as_arch_pause();
		}

		sys_futex(m, FUTEX_WAKE_PRIVATE, 1);
	}
	else if (unlikely(check == 0)) {
		cf_crash(CF_MISC, "cf_mutex_unlock_spin() on already unlocked mutex");
	}
}


//==========================================================
// Public API - cf_condition.
//

void
cf_condition_wait(cf_condition* c, cf_mutex* m)
{
	uint32_t seq = c->seq;

	cf_mutex_unlock(m);
	sys_futex(&c->seq, FUTEX_WAIT_PRIVATE, seq);
	cf_mutex_lock(m);
}

void
cf_condition_signal(cf_condition* c)
{
	__sync_fetch_and_add(&c->seq, 1);
	sys_futex(&c->seq, FUTEX_WAKE_PRIVATE, 1);
}
