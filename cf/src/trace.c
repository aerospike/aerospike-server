/*
 * trace.c
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include "trace.h"

#include <dlfcn.h>
#include <execinfo.h>
#include <link.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "backtrace.h"

#include "log.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

extern char __executable_start;
extern char __etext;


//==========================================================
// Globals.
//

struct backtrace_state* g_bt_state;


//==========================================================
// Forward declarations.
//

static void error_cb(void* data, const char* msg, int errnum);
static int pcinfo_cb(void* data, uintptr_t pc, const char* file, int lineno, const char* func);
static void syminfo_cb(void* data, uintptr_t pc, const char* sn, uintptr_t sv, uintptr_t ss);


//==========================================================
// Public API.
//

void
cf_trace_init(void)
{
	void* tmp = NULL;

	// Force dynamic linker to resolve backtrace() now, and not when we crash!
	backtrace(&tmp, 1);

	g_bt_state = backtrace_create_state(NULL, 1, error_cb, NULL);
}

// For now, just a wrapper.
int
cf_backtrace(void** buf, int sz)
{
	return backtrace(buf, sz);
}

uint64_t
cf_strip_aslr(const void* addr)
{
	const void* start = &__executable_start;
	const void* end = &__etext;
	uint64_t aslr_offset = _r_debug.r_map->l_addr;

	return addr >= start && addr < end ?
			(uint64_t)addr - aslr_offset : (uint64_t)addr;
}

// Caller must ensure buf can't overflow.
void
cf_addr_to_sym_str(char* buf, const void* addr)
{
	Dl_info info;

	if (dladdr(addr, &info) == 0 || info.dli_fname == NULL ||
			info.dli_fname[0] == '\0') {
		strcpy(buf, "invalid address");
		return;
	}

	buf = stpcpy(buf, info.dli_fname);
	*buf++ = '(';

	char* start = buf;

	backtrace_pcinfo(g_bt_state, (uintptr_t)addr, pcinfo_cb, error_cb, &buf);

	if (buf == start) {
		backtrace_syminfo(g_bt_state, (uintptr_t)addr, syminfo_cb, error_cb,
				&buf);
	}
	else {
		buf--; // remove trailing space
	}

	*buf++ = ')';
	*buf = '\0';
}


//==========================================================
// Local helpers.
//

static void
error_cb(void* data, const char* msg, int errnum)
{
	(void)data;

	cf_warning(AS_AS, "libbacktrace error %d: %s", errnum, msg);
}

static int
pcinfo_cb(void* data, uintptr_t pc, const char* file, int lineno,
		const char* func)
{
	(void)pc;

	char* buf = *(char**)data;

	if (func != NULL) {
		buf = stpcpy(buf, func);
		*buf++ = ' ';
	}

	if (file != NULL) {
		buf = stpcpy(buf, file);

		if (lineno != 0) {
			buf += sprintf(buf, ":%d", lineno);
		}

		*buf++ = ' ';
	}

	*(char**)data = buf;

	return 0;
}

static void
syminfo_cb(void* data, uintptr_t pc, const char* sn, uintptr_t sv, uintptr_t ss)
{
	(void)pc;
	(void)sv;
	(void)ss;

	if (sn == NULL) {
		return;
	}

	char* buf = *(char**)data;

	buf = stpcpy(buf, sn);

	*(char**)data = buf;
}
