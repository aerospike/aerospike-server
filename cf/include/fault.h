/*
 * fault.h
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

#pragma once

#include <alloca.h>
#include <execinfo.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucontext.h>
#include "dynbuf.h"


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
#define CVERIFY(expr, counter) typedef char CGLUE(compiler_assert_failed_, counter)[(expr) ? 1 : -1]
#define COMPILER_ASSERT(expr) CVERIFY(expr, __COUNTER__)

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
} while (false);

// Use CF_ZSTR_DEFINE() to null-terminate strings conveniently.
//
// e.g.
// CF_ZSTR_DEFINE(zstr, 40, ns_name, name_sz);
// cf_warning(AS_NAMESPACE, "got namespace %s", zstr);
//
#define CF_ZSTR_DEFINE(zstr, max_sz, str, sz) \
		char zstr[max_sz]; \
		size_t zstr##len = sz < max_sz ? sz : max_sz - 1; \
		memcpy(zstr, str, zstr##len); \
		zstr[zstr##len] = 0;

// Use CF_ZSTRxx() to null-terminate strings conveniently. Useful especially as
// cf_detail & cf_debug parameters where there's no cost unless the log level
// is enabled. (Cost may be more than CF_ZSTR_DEFINE() due to copying struct on
// function return.)
//
// e.g.
// cf_debug(AS_NAMESPACE, "got namespace %s", CF_ZSTR64(ns_name, name_sz));
//

typedef struct cf_zstr64_s {
	char s[64];
} cf_zstr64;

typedef struct cf_zstr1k_s {
	char s[1024];
} cf_zstr1k;

static inline cf_zstr64
cf_null_terminate_64(const char *str, size_t sz)
{
	cf_zstr64 zstr;
	size_t len = sz < sizeof(zstr.s) ? sz : sizeof(zstr.s) - 1;

	memcpy(zstr.s, str, len);
	zstr.s[len] = 0;

	return zstr;
}

static inline cf_zstr1k
cf_null_terminate_1k(const char *str, size_t sz)
{
	cf_zstr1k zstr;
	size_t len = sz < sizeof(zstr.s) ? sz : sizeof(zstr.s) - 1;

	memcpy(zstr.s, str, len);
	zstr.s[len] = 0;

	return zstr;
}

#define CF_ZSTR64(str, sz) (cf_null_terminate_64((const char *)str, sz).s)
#define CF_ZSTR1K(str, sz) (cf_null_terminate_1k((const char *)str, sz).s)


/* SYNOPSIS
 * Fault scoping
 *
 * Faults are identified by a context and severity.  The context describes where
 * the fault occurred, and the severity determines the required action.
 *
 * Examples:
 *    cf_info(CF_MISC, "important message: %s", my_msg);
 *    cf_crash(CF_MISC, "doom!");
 *    cf_assert(my_test, CF_MISC, "gloom!");
 */

/* cf_fault_context
 * NB: if you add or remove entries from this enum, you must also change
 * the corresponding strings structure in fault.c */
typedef enum {
	CF_MISC,

	CF_ALLOC,
	CF_ARENAX,
	CF_HARDWARE,
	CF_MSG,
	CF_RBUFFER,
	CF_SOCKET,
	CF_TLS,
	CF_VMAPX,
	CF_XMEM,

	AS_AGGR,
	AS_APPEAL,
	AS_AS,
	AS_BATCH,
	AS_BIN,
	AS_CFG,
	AS_CLUSTERING,
	AS_DRV_SSD,
	AS_EXCHANGE,
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
	AS_PREDEXP,
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
	AS_XDR_HTTP,
	CF_FAULT_CONTEXT_UNDEF
} cf_fault_context;

extern char *cf_fault_context_strings[];

/* cf_fault_severity
 *     CRITICAL            fatal runtime panics
 *     WARNING             runtime errors
 *     INFO                informational or advisory messages
 *     DEBUG               debugging messages
 *     DETAIL              detailed debugging messages
 */
typedef enum {
	CF_CRITICAL = 0,
	CF_WARNING = 1,
	CF_INFO = 2,
	CF_DEBUG = 3,
	CF_DETAIL = 4,
	CF_FAULT_SEVERITY_UNDEF = 5
} cf_fault_severity;

/* cf_fault_sink
 * An endpoint (sink) for a flow of fault messages */
typedef struct cf_fault_sink {
	int fd;
	char *path;
	int limit[CF_FAULT_CONTEXT_UNDEF];
} cf_fault_sink;

#define CF_FAULT_SINKS_MAX 8

/**
 * When we want to dump out some binary data (like a digest, a bit string
 * or a buffer), we want to be able to specify how we'll display the data.
 * We expect this list to grow over time, as more binary representations
 * are needed. (2014_03_20 tjl).
 */
typedef enum {
	CF_DISPLAY_HEX_DIGEST,	 	// Show Special Case DIGEST in Packed Hex
	CF_DISPLAY_HEX_SPACED, 		// Show binary value in regular spaced hex
	CF_DISPLAY_HEX_PACKED, 	    // Show binary value in packed hex
	CF_DISPLAY_HEX_COLUMNS,		// Show binary value in Column Oriented Hex
	CF_DISPLAY_BASE64,		    // Show binary value in Base64
	CF_DISPLAY_BITS_SPACED,		// Show binary value in a spaced bit string
	CF_DISPLAY_BITS_COLUMNS		// Show binary value in Column Oriented Bits
} cf_display_type;


/* Function declarations */

// note: passing a null sink sets for all currently known sinks
extern int cf_fault_sink_addcontext(cf_fault_sink *s, char *context, char *severity);
extern cf_fault_sink *cf_fault_sink_add(char *path);

extern cf_fault_sink *cf_fault_sink_hold(char *path);
extern bool cf_fault_console_is_held();
extern int cf_fault_sink_activate_all_held();
extern int cf_fault_sink_get_fd_list(int *fds);

extern int cf_fault_sink_strlist(cf_dyn_buf *db); // pack all contexts into a string - using ids
extern int cf_fault_sink_context_all_strlist(int sink_id, cf_dyn_buf *db);
extern int cf_fault_sink_context_strlist(int sink_id, char *context, cf_dyn_buf *db);

extern cf_fault_sink *cf_fault_sink_get_id(int id);

extern void cf_fault_sink_logroll(void);

extern void cf_fault_use_local_time(bool val);
extern bool cf_fault_is_using_local_time();

extern void cf_fault_log_millis(bool log_millis);
extern bool cf_fault_is_logging_millis();

// TODO: Rework cf_display_type-based logging to have a more useful
// output format, instead of having this separate function.
extern void cf_fault_hex_dump(const char *title, const void *data, size_t len);

extern cf_fault_severity cf_fault_filter[];

// Define the mechanism that we'll use to write into the Server Log.
// cf_fault_event() is "regular" logging
extern void cf_fault_event(const cf_fault_context,
		const cf_fault_severity severity, const char *file_name,
		const int line, const char *msg, ...)
		__attribute__ ((format (printf, 5, 6)));

// cf_fault_event2() is for advanced logging, where we want to print some
// binary object (often a digest).
extern void cf_fault_event2(const cf_fault_context,
		const cf_fault_severity severity, const char *file_name, const int line,
		const void *mem_ptr, size_t len, cf_display_type dt, const char *msg, ...)
		__attribute__ ((format (printf, 8, 9)));

extern void cf_fault_event_nostack(const cf_fault_context,
		const cf_fault_severity severity, const char *fn, const int line,
		const char *msg, ...)
		__attribute__ ((format (printf, 5, 6)));

// For now there's only one cache, dumped by the ticker.
extern void cf_fault_cache_event(cf_fault_context context,
		cf_fault_severity severity, const char *file_name, int line,
		char *msg, ...)
		__attribute__ ((format (printf, 5, 6)));

// This is ONLY to keep Eclipse happy without having to tell it __FILENAME__ is
// defined. The make process will define it via the -D mechanism.
#ifndef __FILENAME__
#define __FILENAME__ ""
#endif

// The "regular" version.
#define cf_assert(a, context, __msg, ...) \
		((a) ? (void)0 : \
			cf_fault_event((context), CF_CRITICAL, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__))

// The "no stack" versions.
#define cf_assert_nostack(a, context, __msg, ...) \
		((a) ? (void)0 : \
			cf_fault_event_nostack((context), CF_CRITICAL, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__))
#define cf_crash_nostack(context, __msg, ...) \
		cf_fault_event_nostack((context), CF_CRITICAL, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__)

#define MAX_BACKTRACE_DEPTH 50

extern char __executable_start;

#define PRINT_SIGNAL_CONTEXT(_ctx) \
do { \
	ucontext_t *uc = _ctx; \
	mcontext_t *mc = &uc->uc_mcontext; \
	uint64_t *gregs = (uint64_t *)&mc->gregs[0]; \
	\
	char regs[1000]; \
	\
	snprintf(regs, sizeof(regs), \
		"rax %016lx rbx %016lx rcx %016lx rdx %016lx rsi %016lx rdi %016lx " \
		"rbp %016lx rsp %016lx r8 %016lx r9 %016lx r10 %016lx r11 %016lx " \
		"r12 %016lx r13 %016lx r14 %016lx r15 %016lx rip %016lx", \
		gregs[REG_RAX], gregs[REG_RBX], gregs[REG_RCX], gregs[REG_RDX], \
		gregs[REG_RSI], gregs[REG_RDI], gregs[REG_RBP], gregs[REG_RSP], \
		gregs[REG_R8], gregs[REG_R9], gregs[REG_R10], gregs[REG_R11], \
		gregs[REG_R12], gregs[REG_R13], gregs[REG_R14], gregs[REG_R15], \
		gregs[REG_RIP]); \
	\
	cf_fault_event(AS_AS, CF_WARNING, __FILENAME__, __LINE__, \
			"stacktrace: registers: %s", regs); \
	\
	void *bt[MAX_BACKTRACE_DEPTH]; \
	char trace[MAX_BACKTRACE_DEPTH * 20]; \
	\
	int sz = backtrace(bt, MAX_BACKTRACE_DEPTH); \
	int off = 0; \
	\
	for (int i = 0; i < sz; i++) { \
		off += snprintf(trace + off, sizeof(trace) - off, " 0x%lx", \
				(uint64_t)bt[i]); \
	} \
	\
	cf_fault_event(AS_AS, CF_WARNING, __FILENAME__, __LINE__, \
			"stacktrace: found %d frames:%s offset 0x%lx", sz, trace, \
			(uint64_t)&__executable_start); \
	\
	char **syms = backtrace_symbols(bt, sz); \
	\
	if (syms) { \
		for (int i = 0; i < sz; i++) { \
			cf_fault_event(AS_AS, CF_WARNING, __FILENAME__, __LINE__, \
					"stacktrace: frame %d: %s", i, syms[i]); \
		} \
	} \
	else { \
		cf_fault_event(AS_AS, CF_WARNING, __FILENAME__, __LINE__, \
				"stacktrace: found no symbols"); \
	} \
} while (0);

// The "regular" versions.
#define __SEVLOG(severity, context, __msg, ...) \
		(severity > cf_fault_filter[context] ? \
				(void)0 : \
				cf_fault_event((context), severity, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__))

#define cf_crash(context, __msg, ...) \
		cf_fault_event((context), CF_CRITICAL, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__)

#define cf_warning(...) __SEVLOG(CF_WARNING, ##__VA_ARGS__)
#define cf_info(...) __SEVLOG(CF_INFO, ##__VA_ARGS__)
#define cf_debug(...) __SEVLOG(CF_DEBUG, ##__VA_ARGS__)
#define cf_detail(...) __SEVLOG(CF_DETAIL, ##__VA_ARGS__)

// In addition to the existing LOG calls, we will now add a new mechanism
// that will the ability to print out a BINARY ARRAY, in a general manner, at
// the end of the passed in PRINT STRING.
// This is a general mechanism that can be used to express a binary array as
// a hex or Base64 value, but we'll often use it to print a full Digest Value,
// in either Hex format or Base64 format.
#define __BINARY_SEVLOG(severity, context, ptr, len, DT, __msg, ...) \
		(severity > cf_fault_filter[context] ? \
				(void)0 : \
				cf_fault_event2((context), severity, __FILENAME__, __LINE__, ptr, len, DT, (__msg), ##__VA_ARGS__))

#define cf_crash_binary(context, ptr, len, DT, __msg, ...) \
		cf_fault_event2((context), CF_CRITICAL, __FILENAME__, __LINE__, ptr, len, DT, (__msg), ##__VA_ARGS__)

#define cf_warning_binary(...) __BINARY_SEVLOG(CF_WARNING, ##__VA_ARGS__)
#define cf_info_binary(...) __BINARY_SEVLOG(CF_INFO, ##__VA_ARGS__)
#define cf_debug_binary(...) __BINARY_SEVLOG(CF_DEBUG, ##__VA_ARGS__)
#define cf_detail_binary(...) __BINARY_SEVLOG(CF_DETAIL, ##__VA_ARGS__)

// This set of log calls specifically handles DIGEST values.
#define __DIGEST_SEVLOG(severity, context, ptr,__msg, ...) \
		(severity > cf_fault_filter[context] ? \
				(void)0 : \
				cf_fault_event2((context), severity, __FILENAME__, __LINE__, ptr, 20, CF_DISPLAY_HEX_DIGEST, (__msg), ##__VA_ARGS__))

#define cf_crash_digest(context, ptr,__msg, ...) \
		cf_fault_event2((context), CF_CRITICAL, __FILENAME__, __LINE__, ptr, 20, CF_DISPLAY_HEX_DIGEST, (__msg), ##__VA_ARGS__)

#define cf_warning_digest(...)  __DIGEST_SEVLOG(CF_WARNING, ##__VA_ARGS__)
#define cf_info_digest(...)  __DIGEST_SEVLOG(CF_INFO, ##__VA_ARGS__)
#define cf_debug_digest(...)  __DIGEST_SEVLOG(CF_DEBUG, ##__VA_ARGS__)
#define cf_detail_digest(...)  __DIGEST_SEVLOG(CF_DETAIL, ##__VA_ARGS__)

// _GNU_SOURCE gives us a strerror_r() that returns (char *).
#define cf_strerror(err) strerror_r(err, (char *)alloca(200), 200)

/* cf_context_at_severity
 * Return whether the given context is set to this severity level or higher. */
extern bool cf_context_at_severity(const cf_fault_context context, const cf_fault_severity severity);

extern void cf_fault_init();

int generate_packed_hex_string(const void *mem_ptr, uint32_t len, char* output);

// For now there's only one cache, dumped by the ticker.
extern void cf_fault_dump_cache();

#define cf_dump_ticker_cache() cf_fault_dump_cache()

#define __CACHE_SEVLOG(severity, context, __msg, ...) \
		(severity > cf_fault_filter[context] ? \
				(void)0 : \
				cf_fault_cache_event((context), severity, __FILENAME__, __LINE__, (__msg), ##__VA_ARGS__))

#define cf_ticker_warning(...) __CACHE_SEVLOG(CF_WARNING, ##__VA_ARGS__)
#define cf_ticker_info(...) __CACHE_SEVLOG(CF_INFO, ##__VA_ARGS__)
#define cf_ticker_debug(...) __CACHE_SEVLOG(CF_DEBUG, ##__VA_ARGS__)
#define cf_ticker_detail(...) __CACHE_SEVLOG(CF_DETAIL, ##__VA_ARGS__)
