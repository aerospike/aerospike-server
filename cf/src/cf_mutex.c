/*
 * cf_mutex.c
 *
 * Copyright (C) 2017 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#include <linux/futex.h>
#include <sys/syscall.h>

#include "fault.h"


//==========================================================
// Typedefs & constants.
//

#define FUTEX_SPIN_MAX 100


//==========================================================
// Inlines & macros.
//

inline static void
sys_futex(void *uaddr, int op, int val)
{
	syscall(SYS_futex, uaddr, op, val, NULL, NULL, 0);
}

#define xchg(__ptr, __val) __sync_lock_test_and_set(__ptr, __val)
#define cmpxchg(__ptr, __cmp, __set) __sync_val_compare_and_swap(__ptr, __cmp, __set)
#define cpu_relax() asm volatile("pause\n": : :"memory")
#define unlikely(__expr) __builtin_expect(!! (__expr), 0)
#define likely(__expr) __builtin_expect(!! (__expr), 1)


//==========================================================
// Public API - cf_mutex.
//

void
cf_mutex_lock(cf_mutex *m)
{
	if (likely(cmpxchg((uint32_t *)m, 0, 1) == 0)) {
		return; // was not locked
	}

	if (m->u32 == 2) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}

	while (xchg((uint32_t *)m, 2) != 0) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}
}

void
cf_mutex_unlock(cf_mutex *m)
{
	uint32_t check = xchg((uint32_t *)m, 0);

	if (unlikely(check == 2)) {
		sys_futex(m, FUTEX_WAKE_PRIVATE, 1);
	}
	else if (unlikely(check == 0)) {
		cf_crash(CF_MISC, "cf_mutex_unlock() on already unlocked mutex");
	}
}

// Return true if lock success.
bool
cf_mutex_trylock(cf_mutex *m)
{
	if (cmpxchg((uint32_t *)m, 0, 1) == 0) {
		return true; // was not locked
	}

	return false;
}

void
cf_mutex_lock_spin(cf_mutex *m)
{
	for (int i = 0; i < FUTEX_SPIN_MAX; i++) {
		if (cmpxchg((uint32_t *)m, 0, 1) == 0) {
			return; // was not locked
		}

		cpu_relax();
	}

	if (m->u32 == 2) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}

	while (xchg((uint32_t *)m, 2) != 0) {
		sys_futex(m, FUTEX_WAIT_PRIVATE, 2);
	}
}

void
cf_mutex_unlock_spin(cf_mutex *m)
{
	uint32_t check = xchg((uint32_t *)m, 0);

	if (unlikely(check == 2)) {
		// Spin and hope someone takes the lock.
		for (int i = 0; i < FUTEX_SPIN_MAX; i++) {
			if (m->u32 != 0) {
				if (cmpxchg((uint32_t *)m, 1, 2) == 0) {
					break;
				}

				return; // someone else took the lock
			}

			cpu_relax();
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
cf_condition_wait(cf_condition *c, cf_mutex *m)
{
	uint32_t seq = c->seq;

	cf_mutex_unlock(m);
	sys_futex(&c->seq, FUTEX_WAIT_PRIVATE, seq);
	cf_mutex_lock(m);
}

void
cf_condition_signal(cf_condition *c)
{
	__sync_fetch_and_add(&c->seq, 1);
	sys_futex(&c->seq, FUTEX_WAKE_PRIVATE, 1);
}
