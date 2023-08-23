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
#include <fcntl.h>
#include <printf.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include "aerospike/as_log.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "os.h"
#include "shash.h"
#include "trace.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

struct cf_log_sink_s {
	const char* path;
	int fd;
	int facility;    // for syslog only
	const char* tag; // for syslog only
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
		"secrets",
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
		"batch-sub",
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
		"key-busy",
		"migrate",
		"namespace",
		"nsup",
		"particle",
		"partition",
		"proto",
		"proxy",
		"proxy-divert",
		"query",
		"record",
		"roster",
		"rw",
		"rw-client",
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

typedef struct facility_code_s {
	const char* name;
	int code;
} facility_code;

static const facility_code facility_codes[] = {
		{ "auth", LOG_AUTH },
		{ "authpriv", LOG_AUTHPRIV },
		{ "cron", LOG_CRON },
		{ "daemon", LOG_DAEMON },
		{ "ftp", LOG_FTP },
		{ "kern", LOG_KERN },
		{ "lpr", LOG_LPR },
		{ "mail", LOG_MAIL },
		{ "news", LOG_NEWS },
		{ "syslog", LOG_SYSLOG },
		{ "user", LOG_USER },
		{ "uucp", LOG_UUCP },
		{ "local0", LOG_LOCAL0 },
		{ "local1", LOG_LOCAL1 },
		{ "local2", LOG_LOCAL2 },
		{ "local3", LOG_LOCAL3 },
		{ "local4", LOG_LOCAL4 },
		{ "local5", LOG_LOCAL5 },
		{ "local6", LOG_LOCAL6 },
		{ "local7", LOG_LOCAL7 }
};

#define N_FACILITY_CODES (sizeof(facility_codes) / sizeof(facility_code))

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

#define SYSLOG_TIME_SZ 15


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
static bool g_use_syslog = false;


//==========================================================
// Forward declarations.
//

static void set_most_verbose_level(cf_log_context context, cf_log_level level);
static uint32_t cache_hash_fn(const void *key);
static bool get_context(const char* context_str, cf_log_context* context);
static bool get_level(const char* level_str, cf_log_level* level);
static void update_most_verbose_level(cf_log_context context);
static void log_write(cf_log_context context, cf_log_level level, const char* file_name, int line, const char* format, va_list argp);
static uint32_t get_log_fds(cf_log_context context, cf_log_level level, int* fds);
static int sprintf_now(char* buf);
static bool write_all(int fd, const char* buf, size_t sz);
static int32_t syslog_socket(const char* path);
static void syslog_write(cf_log_context context, cf_log_level level, const char* file_name, int line, const char* format, va_list argp);
static uint32_t get_syslog_sinks(cf_log_context context, cf_log_level level, cf_log_sink** sinks);
static void syslog_sprintf_now(char* buf);
static int syslog_level(cf_log_level level);
static void syslog_write_sink(cf_log_sink* sink, int sys_level, const char* time, const char* buf, size_t buf_sz);
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
			sizeof(uint32_t), 256, true);

	register_custom_conversions();
}

cf_log_sink*
cf_log_init_sink(const char* path, int facility, const char* tag)
{
	if (g_n_sinks >= MAX_SINKS) {
		cf_crash_nostack(CF_MISC, "can't configure > %d log sinks", MAX_SINKS);
	}

	cf_log_sink* sink = &g_sinks[g_n_sinks];

	sink->path = path != NULL ? cf_strdup(path) : NULL;
	sink->facility = facility;

	if (tag != NULL) {
		sink->tag = cf_strdup(tag);
		g_use_syslog = true;
	}
	else {
		sink->tag = NULL;
	}

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

bool
cf_log_init_facility(cf_log_sink* sink, const char* facility_str)
{
	if (*facility_str == '\0') {
		return true; // will use the default
	}

	for (uint32_t i = 0; i < N_FACILITY_CODES; i++) {
		if (strcmp(facility_codes[i].name, facility_str) == 0) {
			sink->facility = facility_codes[i].code;
			return true;
		}
	}

	return false;
}

void
cf_log_init_path(cf_log_sink* sink, const char* path)
{
	if (*path != '\0') {
		cf_free((void*)sink->path);
		sink->path = cf_strdup(path);
	}
	// else - will use the default.
}

void
cf_log_init_tag(cf_log_sink* sink, const char* tag)
{
	if (*tag != '\0') {
		cf_free((void*)sink->tag);
		sink->tag = cf_strdup(tag);
	}
	// else - will use the default.
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

		if (sink->tag == NULL) {
			sink->fd = open(sink->path, CF_LOG_OPEN_FLAGS, cf_os_log_perms());

			if (sink->fd < 0) {
				cf_crash_nostack(CF_MISC, "can't open %s: %d (%s)", sink->path,
						errno, cf_strerror(errno));
			}

			continue;
		}

		sink->fd = syslog_socket(sink->path);

		if (sink->fd < 0) {
			cf_crash_nostack(CF_MISC, "can't connect to %s: %d (%s)",
					sink->path, errno, cf_strerror(errno));
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

		if (sink->path == NULL || sink->tag != NULL) {
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
	syslog_write(context, level, file_name, line, format, argp);

	va_end(argp);
}

void
cf_log_write_no_return(int sig, cf_log_context context, const char* file_name,
		int line, const char* format, ...)
{
	va_list argp;
	va_start(argp, format);

	log_write(context, CF_CRITICAL, file_name, line, format, argp);
	syslog_write(context, CF_CRITICAL, file_name, line, format, argp);

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

	va_list argp;
	va_start(argp, format);

	char adjusted[MAX_ADJUSTED_STRING_SZ];

	adjust_format(format, adjusted);
	vsnprintf(key.msg, sizeof(key.msg) - 1, adjusted, argp);

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
cf_log_stack_trace(void)
{
	void* bt[MAX_BACKTRACE_DEPTH];
	int n_frames = cf_backtrace(bt, MAX_BACKTRACE_DEPTH);

	for (int i = 0; i < n_frames; i++) {
		char sym_str[SYM_STR_MAX_SZ];

		cf_addr_to_sym_str(sym_str, bt[i]);
		cf_warning(AS_AS, "stacktrace: frame %d: %s", i, sym_str);
	}
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
	int fds[g_n_sinks + 1]; // +1 for g_n_sinks == 0 && ! g_sinks_activated
	uint32_t n_fds = get_log_fds(context, level, fds);

	if (n_fds == 0) {
		return;
	}

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

	for (uint32_t i = 0; i < n_fds; i++) {
		write_all(fds[i], buf, (size_t)pos);
	}
}

static uint32_t
get_log_fds(cf_log_context context, cf_log_level level, int* fds)
{
	if (! g_sinks_activated) {
		fds[0] = STDERR_FILENO;
		return 1;
	}

	uint32_t n_fds = 0;

	for (uint32_t i = 0; i < g_n_sinks; i++) {
		cf_log_sink* sink = &g_sinks[i];

		if (sink->tag == NULL && level <= sink->levels[context]) {
			fds[n_fds++] = sink->fd;
		}
	}

	return n_fds;
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

	int pos = (int)strftime(buf, MAX_LOG_STRING_SZ, "%b %d %Y %T", &now_tm);

	if (g_use_millis) {
		pos += sprintf(buf + pos, ".%03ld", now_tv.tv_usec / 1000);
	}

	if (g_use_local_time) {
		pos += (int)strftime(buf + pos, MAX_LOG_STRING_SZ - (size_t)pos,
				" GMT%z: ", &now_tm);
	}
	else {
		strcpy(buf + pos, " GMT: ");
		pos += 6;
	}

	g_reentrant = false;

	return pos;
}

static bool
write_all(int fd, const char* buf, size_t sz)
{
	while (sz != 0) {
		ssize_t sz_wr = write(fd, buf, sz);

		if (sz_wr == 0) {
			fprintf(stderr, "zero-size log write to %d\n", fd);
			return false;
		}

		if (sz_wr < 0) {
			fprintf(stderr, "log write to %d failed: %d (%s)\n", fd, errno,
					cf_strerror(errno));
			return false;
		}

		buf += sz_wr;
		sz -= (size_t)sz_wr;
	}

	return true;
}

static int32_t
syslog_socket(const char* path)
{
	struct sockaddr_un addr;

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, path);

	int32_t fd = socket(AF_UNIX, SOCK_DGRAM, 0);

	if (fd < 0) {
		return -1;
	}

	if (connect(fd, &addr, sizeof(addr)) >= 0) {
		return fd;
	}

	close(fd);
	fd = socket(AF_UNIX, SOCK_STREAM, 0);

	if (connect(fd, &addr, sizeof(addr)) >= 0) {
		return fd;
	}

	close(fd);
	return -1;
}

static void
syslog_write(cf_log_context context, cf_log_level level, const char* file_name,
		int line, const char* format, va_list argp)
{
	cf_log_sink* sinks[g_n_sinks];
	uint32_t n_sinks = get_syslog_sinks(context, level, sinks);

	if (n_sinks == 0) {
		return;
	}

	char time[SYSLOG_TIME_SZ];

	syslog_sprintf_now(time);

	char buf[MAX_LOG_STRING_SZ];

	int pos = sprintf(buf, "%s (%s): (%s:%d) ", level_tag(level),
			context_strings[context], file_name, line);

	char adjusted[MAX_ADJUSTED_STRING_SZ];

	adjust_format(format, adjusted);

	pos += vsnprintf(buf + pos, MAX_LOG_STRING_LEN - (size_t)pos, adjusted,
			argp);

	if (pos > MAX_LOG_STRING_LEN) {
		pos = MAX_LOG_STRING_LEN;
	}

	int sys_level = syslog_level(level);

	for (uint32_t i = 0; i < n_sinks; i++) {
		syslog_write_sink(sinks[i], sys_level, time, buf, (size_t)pos);
	}
}

static uint32_t
get_syslog_sinks(cf_log_context context, cf_log_level level,
		cf_log_sink** sinks)
{
	if (! g_use_syslog || ! g_sinks_activated) {
		return 0;
	}

	uint32_t n_sinks = 0;

	for (uint32_t i = 0; i < g_n_sinks; i++) {
		cf_log_sink* sink = &g_sinks[i];

		if (sink->tag != NULL && level <= sink->levels[context]) {
			sinks[n_sinks++] = sink;
		}
	}

	return n_sinks;
}

static void
syslog_sprintf_now(char* buf)
{
	// Guard against reentrance since localtime_r() and gmtime_r() call
	// allocation functions which may fail and write log lines.
	static __thread bool g_reentrant = false;

	if (g_reentrant) {
		memcpy(buf, "Jan  1 00:00:00", SYSLOG_TIME_SZ);
		return;
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

	strftime(buf, SYSLOG_TIME_SZ, "%b %e %T", &now_tm);

	g_reentrant = false;
}

static int
syslog_level(cf_log_level level)
{
	switch (level) {
	case CF_CRITICAL:
		return LOG_EMERG;
	case CF_WARNING:
		return LOG_WARNING;
	case CF_INFO:
		return LOG_INFO;
	default:
		return LOG_DEBUG;
	}
}

static void
syslog_write_sink(cf_log_sink* sink, int sys_level, const char* time,
		const char* buf, size_t buf_sz)
{
	int priority = (sink->facility << 3) | sys_level;
	size_t tag_len = strlen(sink->tag);
	char dgram[1 + 11 + 1 + SYSLOG_TIME_SZ + 1 + tag_len + 2 + buf_sz + 1];

	size_t pos = (size_t)sprintf(dgram, "<%d>", priority);

	memcpy(dgram + pos, time, SYSLOG_TIME_SZ);
	pos += SYSLOG_TIME_SZ;

	*(dgram + pos++) = ' ';

	memcpy(dgram + pos, sink->tag, tag_len);
	pos += tag_len;

	*(dgram + pos++) = ':';
	*(dgram + pos++) = ' ';

	memcpy(dgram + pos, buf, buf_sz);
	pos += buf_sz;

	*(dgram + pos++) = '\0';

	for (uint32_t tries = 0; tries < 2; tries++) {
		if (sink->fd < 0) {
			sink->fd = syslog_socket(sink->path);

			if (sink->fd < 0) {
				fprintf(stderr, "can't connect to %s: %d (%s)\n", sink->path,
						errno, cf_strerror(errno));
				break;
			}
		}

		int32_t opt;
		socklen_t len = sizeof(opt);
		int32_t rv = getsockopt(sink->fd, SOL_SOCKET, SO_TYPE, &opt, &len);

		cf_assert(rv == 0, CF_MISC, "can't get type of socket to %s\n",
				sink->path);

		if (write_all(sink->fd, dgram, opt == SOCK_STREAM ? pos : pos - 1)) {
			break;
		}

		close(sink->fd);
		sink->fd = -1;
	}
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
