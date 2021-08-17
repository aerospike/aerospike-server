/*
 * log.h
 *
 * Copyright (C) 2019-2021 Aerospike, Inc.
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

#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

// Must be in sync with cf_log_context_strings.
typedef enum {
	CF_MISC,

	CF_ALLOC,
	CF_ARENAX,
	CF_HARDWARE,
	CF_MSG,
	CF_OS,
	CF_RBUFFER,
	CF_SOCKET,
	CF_TLS,
	CF_VAULT,
	CF_VMAPX,
	CF_XMEM,

	AS_AGGR,
	AS_APPEAL,
	AS_AS,
	AS_AUDIT,
	AS_BATCH,
	AS_BIN,
	AS_CFG,
	AS_CLUSTERING,
	AS_DRV_PMEM,
	AS_DRV_SSD,
	AS_EXCHANGE,
	AS_EXP,
	AS_FABRIC,
	AS_FLAT,
	AS_GEO,
	AS_HB,
	AS_HEALTH,
	AS_HLC,
	AS_INDEX,
	AS_INFO,
	AS_INFO_PORT,
	AS_JOB,
	AS_MIGRATE,
	AS_MON,
	AS_NAMESPACE,
	AS_NSUP,
	AS_PARTICLE,
	AS_PARTITION,
	AS_PAXOS,
	AS_PROTO,
	AS_PROXY,
	AS_PROXY_DIVERT, // special detail context
	AS_QUERY,
	AS_RECORD,
	AS_ROSTER,
	AS_RW,
	AS_RW_CLIENT, // special detail context
	AS_SCAN,
	AS_SECURITY,
	AS_SERVICE,
	AS_SERVICE_LIST,
	AS_SINDEX,
	AS_SKEW,
	AS_SMD,
	AS_STORAGE,
	AS_TRUNCATE,
	AS_TSVC,
	AS_UDF,
	AS_XDR,
	AS_XDR_CLIENT,

	CF_LOG_N_CONTEXTS
} cf_log_context;

// FIXME - reorder?
typedef enum {
	CF_CRITICAL,
	CF_WARNING,
	CF_INFO,
	CF_DEBUG,
	CF_DETAIL,

	CF_LOG_N_LEVELS
} cf_log_level;

typedef struct cf_log_sink_s cf_log_sink;

#define CF_LOG_OPEN_FLAGS (O_WRONLY | O_CREAT | O_APPEND)


//==========================================================
// Public API - compile-time helpers.
//

// Use COMPILER_ASSERT() for compile-time verification.
//
// Usage does not add any compiled code, or cost anything at runtime. When the
// evaluated expression is false, it causes a compile error which will draw
// attention to the relevant line.
//
// e.g.
// COMPILER_ASSERT(sizeof(my_int_array) / sizeof(int) == MY_INT_ARRAY_SIZE);
//
#define CGLUE(a, b) a##b
#define CVERIFY(expr, c) typedef char CGLUE(assert_failed_, c)[(expr) ? 1 : -1]
#define COMPILER_ASSERT(expr) CVERIFY(expr, __COUNTER__)

// Use ARRAY_ASSERT() as a shortcut of above example.
#define ARRAY_ASSERT(array, count) \
	COMPILER_ASSERT(sizeof(array) / sizeof((array)[0]) == count)

// Use CF_MUST_CHECK with declarations to force caller to handle return value.
//
// e.g.
// CF_MUST_CHECK int my_function();
//
#define CF_MUST_CHECK __attribute__((warn_unused_result))

// Use CF_IGNORE_ERROR() as caller to override CF_MUST_CHECK in declaration.
//
// e.g.
// CF_IGNORE_ERROR(my_function());
//
#define CF_IGNORE_ERROR(x) ((void)((x) == 12345))

// Use CF_NEVER_FAILS() as caller to assert that returned value is not negative.
//
// e.g.
// CF_NEVER_FAILS(my_function());
//
#define CF_NEVER_FAILS(x) \
do { \
	if ((x) < 0) { \
		cf_crash(CF_MISC, "this cannot happen..."); \
	} \
} while (false)


//==========================================================
// Public API - config wrappers.
//

void cf_log_use_local_time(bool use);
bool cf_log_is_using_local_time(void);

void cf_log_use_millis(bool use);
bool cf_log_is_using_millis(void);


//==========================================================
// Public API - manage logging.
//

void cf_log_init(bool early_verbose);
cf_log_sink* cf_log_init_sink(const char* path);
bool cf_log_init_level(cf_log_sink* sink, const char* context_str, const char* level_str);
void cf_log_activate_sinks(void);
bool cf_log_set_level(uint32_t id, const char* context_str, const char* level_str);
void cf_log_get_sinks(cf_dyn_buf* db);
void cf_log_get_level(uint32_t id, const char* context_str, cf_dyn_buf* db);
void cf_log_get_all_levels(uint32_t id, cf_dyn_buf* db);
bool cf_log_check_level(cf_log_context context, cf_log_level level);
void cf_log_rotate(void);


//==========================================================
// Public API - write to log.
//

void cf_log_write(cf_log_context context, cf_log_level level,
		const char* file_name, int line, const char* format, ...)
		__attribute__ ((format (printf, 5, 6)));

void cf_log_write_no_return(int sig, cf_log_context context,
		const char* file_name, int line, const char* format, ...)
		__attribute__ ((noreturn, format (printf, 5, 6)));

extern cf_log_level g_most_verbose_levels[];

// This is ONLY to keep Eclipse happy without having to tell it __FILENAME__ is
// defined. The make process will define it via the -D mechanism.
#ifndef __FILENAME__
#define __FILENAME__ ""
#endif

#define cf_crash(context, ...) \
	cf_log_write_no_return(SIGUSR1, (context), __FILENAME__, __LINE__, \
			__VA_ARGS__)

#define cf_crash_nostack(context, ...) \
	cf_log_write_no_return(SIGINT, (context), __FILENAME__, __LINE__, \
			__VA_ARGS__)

#define cf_assert(expr, context, ...) \
	do { \
		if (! (expr)) { \
			cf_crash(context, __VA_ARGS__); \
		} \
	} while (false)

#define cf_assert_nostack(expr, context, ...) \
	do { \
		if (! (expr)) { \
			cf_crash_nostack(context, __VA_ARGS__); \
		} \
	} while (false)

#define cf_log_filter(level, context, ...) \
	do { \
		if ((level) <= g_most_verbose_levels[(context)]) { \
			cf_log_write((context), (level), __FILENAME__, __LINE__, \
					__VA_ARGS__); \
		} \
	} while (false)

#define cf_warning(...) cf_log_filter(CF_WARNING, __VA_ARGS__)
#define cf_info(...) cf_log_filter(CF_INFO, __VA_ARGS__)
#define cf_debug(...) cf_log_filter(CF_DEBUG, __VA_ARGS__)
#define cf_detail(...) cf_log_filter(CF_DETAIL, __VA_ARGS__)

#define cf_strerror(err) ({ \
	char _buf[200]; \
	strerror_r(err, _buf, sizeof(_buf)); \
})


//==========================================================
// Public API - ticker logging.
//

void cf_log_write_cache(cf_log_context context, cf_log_level level,
		const char* file_name, int line, const char* format, ...)
		__attribute__ ((format (printf, 5, 6)));

void cf_log_dump_cache(void);

#define cf_log_filter_cache(level, context, ...) \
	do { \
		if ((level) <= g_most_verbose_levels[(context)]) { \
			cf_log_write_cache((context), (level), __FILENAME__, __LINE__, \
					__VA_ARGS__); \
		} \
	} while (false)

#define cf_ticker_warning(...) cf_log_filter_cache(CF_WARNING, __VA_ARGS__)
#define cf_ticker_info(...) cf_log_filter_cache(CF_INFO, __VA_ARGS__)
#define cf_ticker_debug(...) cf_log_filter_cache(CF_DEBUG, __VA_ARGS__)
#define cf_ticker_detail(...) cf_log_filter_cache(CF_DETAIL, __VA_ARGS__)


//==========================================================
// Public API - stack trace.
//

void cf_log_stack_trace(void* ctx);
uint64_t cf_log_strip_aslr(const void* addr);
