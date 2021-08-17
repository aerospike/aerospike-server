/*
 * log.c
 *
 * Copyright (C) 2019-2020 Aerospike, Inc.
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

#include "log.h"

#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <link.h>
#include <printf.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "aerospike/as_log.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "os.h"
#include "shash.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

struct cf_log_sink_s {
	const char* path;
	int fd;
	cf_log_level levels[CF_LOG_N_CONTEXTS];
};

#define MAX_SINKS 8

static const char* context_strings[] = {
		"misc",

		"alloc",
		"arenax",
		"hardware",
		"msg",
		"os",
		"rbuffer",
		"socket",
		"tls",
		"vault",
		"vmapx",
		"xmem",

		"aggr",
		"appeal",
		"as",
		"audit",
		"batch",
		"bin",
		"config",
		"clustering",
		"drv_pmem",
		"drv_ssd",
		"exchange",
		"exp",
		"fabric",
		"flat",
		"geo",
		"hb",
		"health",
		"hlc",
		"index",
		"info",
		"info-port",
		"job",
		"migrate",
		"mon",
		"namespace",
		"nsup",
		"particle",
		"partition",
		"paxos",
		"proto",
		"proxy",
		"proxy-divert",
		"query",
		"record",
		"roster",
		"rw",
		"rw-client",
		"scan",
		"security",
		"service",
		"service-list",
		"sindex",
		"skew",
		"smd",
		"storage",
		"truncate",
		"tsvc",
		"udf",
		"xdr",
		"xdr-client"
};

ARRAY_ASSERT(context_strings, CF_LOG_N_CONTEXTS);

static const char* level_strings[] = {
		"CRITICAL",
		"WARNING",
		"INFO",
		"DEBUG",
		"DETAIL"
};

ARRAY_ASSERT(level_strings, CF_LOG_N_LEVELS);

#define CACHE_MSG_MAX_SIZE 128

typedef struct cf_log_cache_hkey_s {
	// Members most likely to be unique come first:
	int line;
	cf_log_context context;
	const char* file_name;
	cf_log_level level;
	char msg[CACHE_MSG_MAX_SIZE];
} __attribute__((__packed__)) cf_log_cache_hkey;

#define MAX_LOG_STRING_SZ 1024
#define MAX_LOG_STRING_LEN (MAX_LOG_STRING_SZ - 1) // room for \n instead of \0

#define MAX_ADJUSTED_STRING_SZ 2048 // trust adjusted format won't exceed this

#define MAX_BACKTRACE_DEPTH 50

extern char __executable_start;
extern char __etext;


//==========================================================
// Globals.
//

cf_log_level g_most_verbose_levels[CF_LOG_N_CONTEXTS];

static bool g_use_local_time = false;
static bool g_use_millis = false;

static cf_shash* g_ticker_hash;
static bool g_sinks_activated = false;
static uint32_t g_n_sinks = 0;
static cf_log_sink g_sinks[MAX_SINKS];


//==========================================================
// Forward declarations.
//

static void set_most_verbose_level(cf_log_context context, cf_log_level level);
static uint32_t cache_hash_fn(const void *key);
static bool get_context(const char* context_str, cf_log_context* context);
static bool get_level(const char* level_str, cf_log_level* level);
static void update_most_verbose_level(cf_log_context context);
static void log_write(cf_log_context context, cf_log_level level, const char* file_name, int line, const char* format, va_list argp);
static int sprintf_now(char* buf);
static int cache_reduce_fn(const void* key, void* data, void* udata);

static void register_custom_conversions(void);
static int digest_fn(FILE* stream, const struct printf_info* info, const void* const* args);
static int digest_arginfo_fn(const struct printf_info* info, size_t n, int* argtypes, int* size);
static int hex_fn(FILE* stream, const struct printf_info* info, const void* const* args);
static int hex_arginfo_fn(const struct printf_info* info, size_t n, int* argtypes, int* size);
static void adjust_format(const char* format, char* adjusted);


//==========================================================
// Inlines & macros.
//

static inline const char*
level_tag(cf_log_level level)
{
	// FIXME - we wanted to manipulate CRITICAL?
	return level_strings[level];
}

static inline void
write_all(int fd, const char* buf, size_t sz)
{
	while (sz != 0) {
		ssize_t sz_wr = write(fd, buf, sz);

		if (sz_wr == 0) {
			fprintf(stderr, "zero-size log write to %d\n", fd);
			break;
		}

		if (sz_wr < 0) {
			fprintf(stderr, "log write to %d failed: %d (%s)\n", fd, errno,
					cf_strerror(errno));
			break;
		}

		buf += sz_wr;
		sz -= (size_t)sz_wr;
	}
}


//==========================================================
// Public API - config wrappers.
//

void
cf_log_use_local_time(bool use)
{
	g_use_local_time = use;
}

bool
cf_log_is_using_local_time(void)
{
	return g_use_local_time;
}

void
cf_log_use_millis(bool use)
{
	g_use_millis = use;
}

bool
cf_log_is_using_millis(void)
{
	return g_use_millis;
}


//==========================================================
// Public API - manage logging.
//

void
cf_log_init(bool early_verbose)
{
	cf_log_level level = early_verbose ? CF_DETAIL : CF_WARNING;

	for (cf_log_context context = 0; context < CF_LOG_N_CONTEXTS; context++) {
		set_most_verbose_level(context, level);
	}

	g_ticker_hash = cf_shash_create(cache_hash_fn, sizeof(cf_log_cache_hkey),
			sizeof(uint32_t), 256, CF_SHASH_MANY_LOCK);

	register_custom_conversions();
}

cf_log_sink*
cf_log_init_sink(const char* path)
{
	if (g_n_sinks >= MAX_SINKS) {
		cf_crash_nostack(CF_MISC, "can't configure > %d log sinks", MAX_SINKS);
	}

	cf_log_sink* sink = &g_sinks[g_n_sinks];

	sink->path = path != NULL ? cf_strdup(path) : NULL;

	// Set default for all contexts.
	for (cf_log_context context = 0; context < CF_LOG_N_CONTEXTS; context++) {
		sink->levels[context] = CF_CRITICAL;
	}

	g_n_sinks++;

	return sink;
}

bool
cf_log_init_level(cf_log_sink* sink, const char* context_str,
		const char* level_str)
{
	cf_log_level level;

	if (! get_level(level_str, &level)) {
		return false;
	}

	if (strcasecmp(context_str, "any") == 0) {
		for (int i = 0; i < CF_LOG_N_CONTEXTS; i++) {
			sink->levels[i] = level;
		}

		return true;
	}

	cf_log_context context;

	if (! get_context(context_str, &context)) {
		return false;
	}

	sink->levels[context] = level;

	return true;
}

void
cf_log_activate_sinks(void)
{
	for (uint32_t i = 0; i < g_n_sinks; i++) {
		cf_log_sink* sink = &g_sinks[i];

		if (sink->path == NULL) {
			sink->fd = STDERR_FILENO;
			continue;
		}

		sink->fd = open(sink->path, CF_LOG_OPEN_FLAGS, cf_os_log_perms());

		if (sink->fd < 0) {
			cf_crash_nostack(CF_MISC, "can't open %s: %d (%s)", sink->path,
					errno, cf_strerror(errno));
		}
	}

	for (cf_log_context context = 0; context < CF_LOG_N_CONTEXTS; context++) {
		update_most_verbose_level(context);
	}

	g_sinks_activated = true;
}

bool
cf_log_set_level(uint32_t id, const char* context_str, const char* level_str)
{
	if (id >= g_n_sinks) {
		cf_warning(CF_MISC, "invalid log sink id %u", id);
		return false;
	}

	cf_log_level level;

	if (! get_level(level_str, &level)) {
		cf_warning(CF_MISC, "invalid log level %s", level_str);
		return false;
	}

	cf_log_context context;
	cf_log_sink* sink = &g_sinks[id];

	if (strcmp(context_str, "any") == 0) {
		for (context = 0; context < CF_LOG_N_CONTEXTS; context++) {
			sink->levels[context] = level;
			update_most_verbose_level(context);
		}

		return true;
	}

	if (! get_context(context_str, &context)) {
		cf_warning(CF_MISC, "invalid log context %s", context_str);
		return false;
	}

	sink->levels[context] = level;
	update_most_verbose_level(context);

	return true;
}

void
cf_log_get_sinks(cf_dyn_buf* db)
{
	for (uint32_t i = 0; i < g_n_sinks; i++) {
		const char* path = g_sinks[i].path;

		cf_dyn_buf_append_uint32(db, i);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, path != NULL ? path : "stderr");
		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_chomp(db);
}

void
cf_log_get_level(uint32_t id, const char* context_str, cf_dyn_buf* db)
{
	if (id >= g_n_sinks) {
		cf_warning(CF_MISC, "bad sink id %u", id);
		cf_dyn_buf_append_string(db, "ERROR::bad-id");
		return;
	}

	cf_log_context context;

	if (! get_context(context_str, &context)) {
		cf_warning(CF_MISC, "bad context %s", context_str);
		cf_dyn_buf_append_string(db, "ERROR::bad-context");
		return;
	}

	cf_dyn_buf_append_string(db, context_str);
	cf_dyn_buf_append_char(db, ':');
	cf_dyn_buf_append_string(db, level_strings[g_sinks[id].levels[context]]);
}

void
cf_log_get_all_levels(uint32_t id, cf_dyn_buf* db)
{
	if (id >= g_n_sinks) {
		cf_warning(CF_MISC, "bad sink id %u", id);
		cf_dyn_buf_append_string(db, "ERROR::bad-id");
		return;
	}

	const cf_log_sink* sink = &g_sinks[id];

	for (uint32_t i = 0; i < CF_LOG_N_CONTEXTS; i++) {
		cf_dyn_buf_append_string(db, context_strings[i]);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, level_strings[sink->levels[i]]);
		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_chomp(db);
}

bool
cf_log_check_level(cf_log_context context, cf_log_level level)
{
	return level <= g_most_verbose_levels[context];
}

void
cf_log_rotate(void)
{
	fprintf(stderr, "rotating log files\n");

	for (uint32_t i = 0; i < g_n_sinks; i++) {
		cf_log_sink* sink = &g_sinks[i];

		if (sink->path == NULL) {
			continue;
		}

		static cf_mutex lock = CF_MUTEX_INIT;

		cf_mutex_lock(&lock); // so concurrent SIGHUPs can't double-close

		int old_fd = sink->fd;

		sink->fd = open(sink->path, CF_LOG_OPEN_FLAGS, cf_os_log_perms());

		usleep(1000); // threads may be interrupted while writing to old fd
		close(old_fd);

		cf_mutex_unlock(&lock);
	}
}


//==========================================================
// Public API - write to log.
//

void
cf_log_write(cf_log_context context, cf_log_level level, const char* file_name,
		int line, const char* format, ...)
{
	va_list argp;
	va_start(argp, format);

	log_write(context, level, file_name, line, format, argp);

	va_end(argp);
}

void
cf_log_write_no_return(int sig, cf_log_context context, const char* file_name,
		int line, const char* format, ...)
{
	va_list argp;
	va_start(argp, format);

	log_write(context, CF_CRITICAL, file_name, line, format, argp);

	va_end(argp);

	raise(sig); // signal will be handled on same thread

	// To satisfy compiler, don't return.
	while (true) {
		;
	}
}


//==========================================================
// Public API - ticker logging.
//

void
cf_log_write_cache(cf_log_context context, cf_log_level level,
		const char* file_name, int line, const char* format, ...)
{
	cf_log_cache_hkey key = {
			.line = line,
			.context = context,
			.file_name = file_name,
			.level = level,
			.msg = { 0 } // must pad hash keys
	};

	size_t limit = sizeof(key.msg) - 1; // truncate leaving null-terminator

	va_list argp;

	va_start(argp, format);
	vsnprintf(key.msg, limit, format, argp);
	va_end(argp);

	while (true) {
		uint32_t* valp = NULL;
		cf_mutex* lockp = NULL;

		if (cf_shash_get_vlock(g_ticker_hash, &key, (void**)&valp, &lockp) ==
				CF_SHASH_OK) {
			// Already in hash - increment count and don't log it.
			(*valp)++;
			cf_mutex_unlock(lockp);
			break;
		}
		// else - not found, add it to hash and log it.

		uint32_t initv = 1;

		if (cf_shash_put_unique(g_ticker_hash, &key, &initv) ==
				CF_SHASH_ERR_FOUND) {
			continue; // other thread beat us to it - loop around and get it
		}

		cf_log_write(context, level, file_name, line, "%s", key.msg);
		break;
	}
}

void
cf_log_dump_cache(void)
{
	cf_shash_reduce(g_ticker_hash, cache_reduce_fn, NULL);
}


//==========================================================
// Public API - stack trace.
//

void
cf_log_stack_trace(void* ctx)
{
	ucontext_t* uc = (ucontext_t*)ctx;
	mcontext_t* mc = (mcontext_t*)&uc->uc_mcontext;
	uint64_t* gregs = (uint64_t*)&mc->gregs[0];

	char regs[1000];

	sprintf(regs,
		"rax %016lx rbx %016lx rcx %016lx rdx %016lx rsi %016lx rdi %016lx "
		"rbp %016lx rsp %016lx r8 %016lx r9 %016lx r10 %016lx r11 %016lx "
		"r12 %016lx r13 %016lx r14 %016lx r15 %016lx rip %016lx",
		gregs[REG_RAX], gregs[REG_RBX], gregs[REG_RCX], gregs[REG_RDX],
		gregs[REG_RSI], gregs[REG_RDI], gregs[REG_RBP], gregs[REG_RSP],
		gregs[REG_R8], gregs[REG_R9], gregs[REG_R10], gregs[REG_R11],
		gregs[REG_R12], gregs[REG_R13], gregs[REG_R14], gregs[REG_R15],
		gregs[REG_RIP]);

	cf_warning(AS_AS, "stacktrace: registers: %s", regs);

	void* bt[MAX_BACKTRACE_DEPTH];
	char trace[MAX_BACKTRACE_DEPTH * 20];

	int n_frames = backtrace(bt, MAX_BACKTRACE_DEPTH);
	int off = 0;

	for (int i = 0; i < n_frames; i++) {
		off += sprintf(trace + off, " 0x%lx", cf_log_strip_aslr(bt[i]));
	}

	cf_warning(AS_AS, "stacktrace: found %d frames:%s offset 0x%lx", n_frames,
			trace, _r_debug.r_map->l_addr);

	char** syms = backtrace_symbols(bt, n_frames);

	if (syms == NULL) {
		cf_warning(AS_AS, "stacktrace: found no symbols");
		return;
	}

	for (int i = 0; i < n_frames; i++) {
		cf_warning(AS_AS, "stacktrace: frame %d: %s", i, syms[i]);
	}
}

uint64_t
cf_log_strip_aslr(const void* addr)
{
	void* start = &__executable_start;
	void* end = &__etext;
	uint64_t aslr_offset = _r_debug.r_map->l_addr;

	return addr >= start && addr < end ?
			(uint64_t)addr - aslr_offset : (uint64_t)addr;
}


//==========================================================
// Local helpers - logging.
//

static void
set_most_verbose_level(cf_log_context context, cf_log_level level)
{
	g_most_verbose_levels[context] = level;

	// UDF logging relies on the common as_log facility. Set as_log_level
	// whenever AS_UDF level changes.
	if (context == AS_UDF) {
		as_log_set_level((as_log_level)level);
	}
}

static uint32_t
cache_hash_fn(const void *key)
{
	return (uint32_t)((const cf_log_cache_hkey*)key)->line +
			*(uint32_t*)((const cf_log_cache_hkey*)key)->msg;
}

static bool
get_context(const char* context_str, cf_log_context* context)
{
	for (cf_log_context i = 0; i < CF_LOG_N_CONTEXTS; i++) {
		if (strcasecmp(context_strings[i], context_str) == 0) {
			*context = i;
			return true;
		}
	}

	return false;
}

static bool
get_level(const char* level_str, cf_log_level* level)
{
	for (cf_log_level i = 0; i < CF_LOG_N_LEVELS; i++) {
		if (strcasecmp(level_strings[i], level_str) == 0) {
			*level = i;
			return true;
		}
	}

	return false;
}

static void
update_most_verbose_level(cf_log_context context)
{
	cf_log_level level = CF_CRITICAL;

	for (uint32_t i = 0; i < g_n_sinks; i++) {
		if (g_sinks[i].levels[context] > level) {
			level = g_sinks[i].levels[context];
		}
	}

	set_most_verbose_level(context, level);
}

static void
log_write(cf_log_context context, cf_log_level level, const char* file_name,
		int line, const char* format, va_list argp)
{
	char buf[MAX_LOG_STRING_SZ];

	int pos = sprintf_now(buf);

	pos += sprintf(buf + pos, "%s (%s): (%s:%d) ", level_tag(level),
			context_strings[context], file_name, line);

	char adjusted[MAX_ADJUSTED_STRING_SZ];

	adjust_format(format, adjusted);

	pos += vsnprintf(buf + pos, MAX_LOG_STRING_LEN - (size_t)pos, adjusted,
			argp);

	if (pos > MAX_LOG_STRING_LEN) {
		pos = MAX_LOG_STRING_LEN;
	}

	buf[pos++] = '\n';

	if (! g_sinks_activated) {
		write_all(STDERR_FILENO, buf, (size_t)pos);
		return;
	}

	for (uint32_t i = 0; i < g_n_sinks; i++) {
		cf_log_sink* sink = &g_sinks[i];

		if (level > sink->levels[context]) {
			continue;
		}

		write_all(sink->fd, buf, (size_t)pos);
	}
}

static int
sprintf_now(char* buf)
{
	// Guard against reentrance since localtime_r() and gmtime_r() call
	// allocation functions which may fail and write log lines.
	static __thread bool g_reentrant = false;

	if (g_reentrant) {
		return 0;
	}

	g_reentrant = true;

	struct timeval now_tv;
	gettimeofday(&now_tv, NULL);

	struct tm now_tm;

	if (g_use_local_time) {
		localtime_r(&now_tv.tv_sec, &now_tm);
	}
	else {
		gmtime_r(&now_tv.tv_sec, &now_tm);
	}

	int pos = (int)strftime(buf, 999, "%b %d %Y %T", &now_tm);

	if (g_use_millis) {
		pos += sprintf(buf + pos, ".%03ld", now_tv.tv_usec / 1000);
	}

	if (g_use_local_time) {
		pos += (int)strftime(buf + pos, 999, " GMT%z: ", &now_tm);
	}
	else {
		strcpy(buf + pos, " GMT: ");
		pos += 6;
	}

	g_reentrant = false;

	return pos;
}

static int
cache_reduce_fn(const void* key, void* data, void* udata)
{
	(void)udata;

	uint32_t* count = (uint32_t*)data;

	if (*count == 0) {
		return CF_SHASH_REDUCE_DELETE;
	}

	const cf_log_cache_hkey* hkey = (const cf_log_cache_hkey*)key;

	cf_log_write(hkey->context, hkey->level, hkey->file_name, hkey->line,
			"(repeated:%u) %s", *count, hkey->msg);

	*count = 0;

	return CF_SHASH_OK;
}


//==========================================================
// Local helpers - custom conversions.
//

static void
register_custom_conversions(void)
{
	register_printf_specifier('D', digest_fn, digest_arginfo_fn);
	register_printf_specifier('H', hex_fn, hex_arginfo_fn);
}

static int
digest_fn(FILE* stream, const struct printf_info* info, const void* const* args)
{
	(void)info;

	const cf_digest* d = *(const cf_digest**)args[0];

	return fprintf(stream,
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
			d->digest[0],  d->digest[1],  d->digest[2],  d->digest[3],
			d->digest[4],  d->digest[5],  d->digest[6],  d->digest[7],
			d->digest[8],  d->digest[9],  d->digest[10], d->digest[11],
			d->digest[12], d->digest[13], d->digest[14], d->digest[15],
			d->digest[16], d->digest[17], d->digest[18], d->digest[19]);
}

static int
digest_arginfo_fn(const struct printf_info* info, size_t n, int* argtypes,
		int* size)
{
	(void)info;

	if (n != 0) {
		argtypes[0] = PA_POINTER;
		size[0] = sizeof(const cf_digest*);
	}

	return 1;
}

static int
hex_fn(FILE* stream, const struct printf_info* info, const void* const* args)
{
	if (info->width == 0) {
		return -1;
	}

	const uint8_t* p = *(const uint8_t**)args[0];
	int len = 0;

	for (int i = 0; i < info->width; i += 16) {
		len += fprintf(stream, "%06x:", i);

		int k;

		for (k = 0; k < 16 && i + k < info->width; k++) {
			len += fprintf(stream, " %02x", p[i + k]);
		}

		while (k++ < 16) {
			len += fprintf(stream, "   ");
		}

		fputc(' ', stream);
		len++;

		for (k = 0; k < 16 && i + k < info->width; k++) {
			char c = (char)p[i + k];

			fputc(c >= 32 && c <= 126 ? c : '.', stream);
			len++;
		}

		if (i + k < info->width) {
			fputc('\n', stream);
			len++;
		}
	}

	return len;
}

static int
hex_arginfo_fn(const struct printf_info* info, size_t n, int* argtypes,
		int* size)
{
	(void)info;

	if (n != 0) {
		argtypes[0] = PA_POINTER;
		size[0] = sizeof(const uint8_t*);
	}

	return 1;
}

static void
adjust_format(const char* format, char* adjusted)
{
	uint32_t from = 0;
	uint32_t to = 0;

	while (format[from] != '\0') {
		if (format[from] == '%') {
			if (format[from + 1] == 'p' && format[from + 2] == 'D') {
				from += 2;
				adjusted[to++] = '%';
			}
			else if (format[from + 1] == '*' && format[from + 2] == 'p' &&
					format[from + 3] == 'H') {
				from += 3;
				adjusted[to++] = '%';
				adjusted[to++] = '*';
			}
		}

		adjusted[to++] = format[from++];
	}

	adjusted[to] = '\0';
}
