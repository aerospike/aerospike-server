/*
 * fault.c
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

#include "fault.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

#include "aerospike/as_log.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"

#include "cf_mutex.h"
#include "shash.h"


/*
 *  Maximum length for logging binary (i.e., hexadecimal or bit string) data.
 */
#define MAX_BINARY_BUF_SZ (64 * 1024)

// TODO - do we really need O_NONBLOCK for log sinks?
#define SINK_OPEN_FLAGS (O_WRONLY | O_CREAT | O_NONBLOCK | O_APPEND)
#define SINK_OPEN_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

#define SINK_REOPEN_FLAGS (O_WRONLY | O_CREAT | O_NONBLOCK | O_TRUNC)

/* cf_fault_context_strings, cf_fault_severity_strings, cf_fault_scope_strings
 * Strings describing fault states */

/* MUST BE KEPT IN SYNC WITH FAULT.H */

char *cf_fault_context_strings[] = {
		"misc",

		"alloc",
		"arenax",
		"hardware",
		"msg",
		"rbuffer",
		"socket",
		"tls",
		"vmapx",
		"xmem",

		"aggr",
		"appeal",
		"as",
		"batch",
		"bin",
		"config",
		"clustering",
		"drv_ssd",
		"exchange",
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
		"predexp",
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
		"xdr-client",
		"xdr-http"
};

COMPILER_ASSERT(sizeof(cf_fault_context_strings) / sizeof(char*) == CF_FAULT_CONTEXT_UNDEF);

static const char *cf_fault_severity_strings[] = {
		"CRITICAL",
		"WARNING",
		"INFO",
		"DEBUG",
		"DETAIL"
};

COMPILER_ASSERT(sizeof(cf_fault_severity_strings) / sizeof(const char*) == CF_FAULT_SEVERITY_UNDEF);

cf_fault_sink cf_fault_sinks[CF_FAULT_SINKS_MAX];
cf_fault_severity cf_fault_filter[CF_FAULT_CONTEXT_UNDEF];
int cf_fault_sinks_inuse = 0;
int num_held_fault_sinks = 0;

cf_shash *g_ticker_hash = NULL;
#define CACHE_MSG_MAX_SIZE 128

typedef struct cf_fault_cache_hkey_s {
	// Members most likely to be unique come first:
	int					line;
	cf_fault_context	context;
	const char			*file_name;
	cf_fault_severity	severity;
	char				msg[CACHE_MSG_MAX_SIZE];
} __attribute__((__packed__)) cf_fault_cache_hkey;

bool g_use_local_time = false;

static bool g_log_millis = false;

// Filter stderr logging at this level when there are no sinks:
#define NO_SINKS_LIMIT CF_WARNING

static inline const char*
severity_tag(cf_fault_severity severity)
{
	return severity == CF_CRITICAL ?
			"FAILED ASSERTION" : cf_fault_severity_strings[severity];
}

/* cf_context_at_severity
 * Return whether the given context is set to this severity level or higher. */
bool
cf_context_at_severity(const cf_fault_context context, const cf_fault_severity severity)
{
	return (severity <= cf_fault_filter[context]);
}

static inline void
cf_fault_set_severity(const cf_fault_context context, const cf_fault_severity severity)
{
	cf_fault_filter[context] = severity;

	// UDF logging relies on the common as_log facility.
	// Set as_log_level whenever AS_UDF severity changes.
	if (context == AS_UDF && severity < CF_FAULT_SEVERITY_UNDEF) {
		as_log_set_level((as_log_level)severity);
	}
}

static inline uint32_t
cache_hash_fn(const void *key)
{
	return (uint32_t)((const cf_fault_cache_hkey*)key)->line +
			*(uint32_t*)((const cf_fault_cache_hkey*)key)->msg;
}

/* cf_fault_init
 * This code MUST be the first thing executed by main(). */
void
cf_fault_init()
{
	// Initialize the fault filter.
	for (int j = 0; j < CF_FAULT_CONTEXT_UNDEF; j++) {
		// We start with no sinks, so let's be in-sync with that.
		cf_fault_set_severity(j, NO_SINKS_LIMIT);
	}

	// Create the ticker hash.
	g_ticker_hash = cf_shash_create(cache_hash_fn, sizeof(cf_fault_cache_hkey),
			sizeof(uint32_t), 256, CF_SHASH_MANY_LOCK);
}


/* cf_fault_sink_add
 * Register an sink for faults */
cf_fault_sink *
cf_fault_sink_add(char *path)
{
	cf_fault_sink *s;

	if ((CF_FAULT_SINKS_MAX - 1) == cf_fault_sinks_inuse)
		return(NULL);

	s = &cf_fault_sinks[cf_fault_sinks_inuse++];
	s->path = cf_strdup(path);
	if (0 == strncmp(path, "stderr", 6))
		s->fd = 2;
	else {
		if (-1 == (s->fd = open(path, SINK_OPEN_FLAGS, SINK_OPEN_MODE))) {
			cf_fault_sinks_inuse--;
			return(NULL);
		}
	}

	for (int i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++)
		s->limit[i] = CF_INFO;

	return(s);
}


/* cf_fault_sink_hold
 * Register but don't activate a sink for faults - return sink object pointer on
 * success, NULL on failure. Only use at startup when parsing config file. After
 * all sinks are registered, activate via cf_fault_sink_activate_all_held(). */
cf_fault_sink *
cf_fault_sink_hold(char *path)
{
	if (num_held_fault_sinks >= CF_FAULT_SINKS_MAX) {
		cf_warning(CF_MISC, "too many fault sinks");
		return NULL;
	}

	cf_fault_sink *s = &cf_fault_sinks[num_held_fault_sinks];

	s->path = cf_strdup(path);

	// If a context is not added, its runtime default will be CF_INFO.
	for (int i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++) {
		s->limit[i] = CF_INFO;
	}

	num_held_fault_sinks++;

	return s;
}


/* cf_fault_console_is_held
 * Return whether the console is held.
 */
bool
cf_fault_console_is_held()
{
	for (int i = 0; i < num_held_fault_sinks; i++) {
		cf_fault_sink *s = &cf_fault_sinks[i];
		if (!strcmp(s->path, "stderr")) {
			return true;
		}
	}

	return false;
}


static void
fault_filter_adjust(cf_fault_sink *s, cf_fault_context ctx)
{
	// Don't adjust filter while adding contexts during config file parsing.
	if (cf_fault_sinks_inuse == 0) {
		return;
	}

	// Fault filter must allow logs at a less critical severity.
	if (s->limit[ctx] > cf_fault_filter[ctx]) {
		cf_fault_set_severity(ctx, s->limit[ctx]);
	}
	// Fault filter might be able to become stricter - check all sinks.
	else if (s->limit[ctx] < cf_fault_filter[ctx]) {
		cf_fault_severity severity = CF_CRITICAL;

		for (int i = 0; i < cf_fault_sinks_inuse; i++) {
			cf_fault_sink *t = &cf_fault_sinks[i];

			if (t->limit[ctx] > severity) {
				severity = t->limit[ctx];
			}
		}

		cf_fault_set_severity(ctx, severity);
	}
}


/* cf_fault_sink_activate_all_held
 * Activate all sinks on hold - return 0 on success, -1 on failure. Only use
 * once at startup, after parsing config file. On failure there's no cleanup,
 * assumes caller will stop the process. */
int
cf_fault_sink_activate_all_held()
{
	for (int i = 0; i < num_held_fault_sinks; i++) {
		if (cf_fault_sinks_inuse >= CF_FAULT_SINKS_MAX) {
			// In case this isn't first sink, force logging as if no sinks:
			cf_fault_sinks_inuse = 0;
			cf_warning(CF_MISC, "too many fault sinks");
			return -1;
		}

		cf_fault_sink *s = &cf_fault_sinks[i];

		// "Activate" the sink.
		if (0 == strncmp(s->path, "stderr", 6)) {
			s->fd = 2;
		}
		else if (-1 == (s->fd = open(s->path, SINK_OPEN_FLAGS, SINK_OPEN_MODE))) {
			// In case this isn't first sink, force logging as if no sinks:
			cf_fault_sinks_inuse = 0;
			cf_warning(CF_MISC, "can't open %s: %s", s->path, cf_strerror(errno));
			return -1;
		}

		cf_fault_sinks_inuse++;

		// Adjust the fault filter to the runtime levels.
		for (int j = 0; j < CF_FAULT_CONTEXT_UNDEF; j++) {
			fault_filter_adjust(s, (cf_fault_context)j);
		}
	}

	return 0;
}


/* cf_fault_sink_get_fd_list
 * Fill list with all active sink fds, excluding stderr - return list count. */
int
cf_fault_sink_get_fd_list(int *fds)
{
	int num_open_fds = 0;

	for (int i = 0; i < cf_fault_sinks_inuse; i++) {
		cf_fault_sink *s = &cf_fault_sinks[i];

		// Exclude stderr.
		if (s->fd > 2 && 0 != strncmp(s->path, "stderr", 6)) {
			fds[num_open_fds++] = s->fd;
		}
	}

	return num_open_fds;
}


static int
cf_fault_sink_addcontext_all(char *context, char *severity)
{
	for (int i = 0; i < cf_fault_sinks_inuse; i++) {
		cf_fault_sink *s = &cf_fault_sinks[i];
		int rv = cf_fault_sink_addcontext(s, context, severity);
		if (rv != 0)	return(rv);
	}
	return(0);
}


int
cf_fault_sink_addcontext(cf_fault_sink *s, char *context, char *severity)
{
	if (s == 0) 		return(cf_fault_sink_addcontext_all(context, severity));

	cf_fault_context ctx = CF_FAULT_CONTEXT_UNDEF;
	cf_fault_severity sev = CF_FAULT_SEVERITY_UNDEF;

	for (int i = 0; i < CF_FAULT_SEVERITY_UNDEF; i++) {
		if (0 == strncasecmp(cf_fault_severity_strings[i], severity, strlen(severity)))
			sev = (cf_fault_severity)i;
	}
	if (CF_FAULT_SEVERITY_UNDEF == sev)
		return(-1);

	if (0 == strncasecmp(context, "any", 3)) {
		for (int i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++) {
			s->limit[i] = sev;
			fault_filter_adjust(s, (cf_fault_context)i);
		}
	} else {
		for (int i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++) {
		//strncasecmp only compared the length of context passed in the 3rd argument and as cf_fault_context_strings has info and info port,
		//So when you try to set info to debug it will set info-port to debug . Just forcing it to check the length from cf_fault_context_strings
			if (0 == strncasecmp(cf_fault_context_strings[i], context, strlen(cf_fault_context_strings[i])))
				ctx = (cf_fault_context)i;
		}
		if (CF_FAULT_CONTEXT_UNDEF == ctx)
			return(-1);

		s->limit[ctx] = sev;
		fault_filter_adjust(s, ctx);
	}

	return(0);
}


void
cf_fault_use_local_time(bool val)
{
	g_use_local_time = val;
}

bool
cf_fault_is_using_local_time()
{
	return g_use_local_time;
}

void
cf_fault_log_millis(bool log_millis)
{
		g_log_millis = log_millis;
}

bool
cf_fault_is_logging_millis()
{
	return g_log_millis;
}

int
cf_sprintf_now(char* mbuf, size_t limit)
{
	struct tm nowtm;

	if (cf_fault_is_logging_millis()) {
		// Logging milli seconds as well.
		struct timeval curTime;
		gettimeofday(&curTime, NULL);
		int millis = curTime.tv_usec / 1000;
		int pos = 0;
		if (g_use_local_time) {
			localtime_r(&curTime.tv_sec, &nowtm);
			pos = strftime(mbuf, limit, "%b %d %Y %T.", &nowtm);
			pos +=
			  snprintf(mbuf + pos, limit - pos, "%03d", millis);
			pos +=
			  strftime(mbuf + pos, limit - pos, " GMT%z: ", &nowtm);
			return pos;
		} else {
			gmtime_r(&curTime.tv_sec, &nowtm);
			pos = strftime(mbuf, limit, "%b %d %Y %T.", &nowtm);
			pos +=
			  snprintf(mbuf + pos, limit - pos, "%03d", millis);
			pos +=
			  strftime(mbuf + pos, limit - pos, " %Z: ", &nowtm);
			return pos;
		}
	}

	// Logging only seconds.
	time_t now = time(NULL);

	if (g_use_local_time) {
		localtime_r(&now, &nowtm);
		return strftime(mbuf, limit, "%b %d %Y %T GMT%z: ", &nowtm);
	} else {
		gmtime_r(&now, &nowtm);
		return strftime(mbuf, limit, "%b %d %Y %T %Z: ", &nowtm);
	}
}

/* cf_fault_event
 * Respond to a fault */
void
cf_fault_event(const cf_fault_context context, const cf_fault_severity severity,
		const char *file_name, const int line, const char *msg, ...)
{
	va_list argp;
	char mbuf[1024];
	size_t pos;


	/* Make sure there's always enough space for the \n\0. */
	size_t limit = sizeof(mbuf) - 2;

	/* Set the timestamp */
	pos = cf_sprintf_now(mbuf, limit);

	/* Set the context/scope/severity tag */
	pos += snprintf(mbuf + pos, limit - pos, "%s (%s): ", severity_tag(severity), cf_fault_context_strings[context]);

	/*
	 * snprintf() and vsnprintf() will not write more than the size specified,
	 * but they return the size that would have been written without truncation.
	 * These checks make sure there's enough space for the final \n\0.
	 */
	if (pos > limit) {
		pos = limit;
	}

	/* Set the location: filename and line number */
	if (file_name) {
		pos += snprintf(mbuf + pos, limit - pos, "(%s:%d) ", file_name, line);
	}

	if (pos > limit) {
		pos = limit;
	}

	/* Append the message */
	va_start(argp, msg);
	pos += vsnprintf(mbuf + pos, limit - pos, msg, argp);
	va_end(argp);

	if (pos > limit) {
		pos = limit;
	}

	pos += snprintf(mbuf + pos, 2, "\n");

	/* Route the message to the correct destinations */
	if (0 == cf_fault_sinks_inuse) {
		/* If no fault sinks are defined, use stderr for important messages */
		if (severity <= NO_SINKS_LIMIT)
			fprintf(stderr, "%s", mbuf);
	} else {
		for (int i = 0; i < cf_fault_sinks_inuse; i++) {
			if ((severity <= cf_fault_sinks[i].limit[context]) || (CF_CRITICAL == severity)) {
				if (0 >= write(cf_fault_sinks[i].fd, mbuf, pos)) {
					// this is OK for a bit in case of a HUP. It's even better to queue the buffers and apply them
					// after the hup. TODO.
					fprintf(stderr, "internal failure in fault message write: %s\n", cf_strerror(errno));
				}
			}
		}
	}

	/* Critical errors */
	if (CF_CRITICAL == severity) {
		fflush(NULL);

		// Our signal handler will log a stack trace.
		raise(SIGUSR1);
	}
} // end cf_fault_event()


/**
 * Generate a Packed Hex String Representation of the binary string.
 * e.g. 0xfc86e83a6d6d3024659e6fe48c351aaaf6e964a5
 * The value is preceeded by a "0x" to denote Hex (which allows it to be
 * used in other contexts as a hex number).
 */
int
generate_packed_hex_string(const void *mem_ptr, uint32_t len, char* output)
{
	uint8_t *d = (uint8_t *) mem_ptr;
	char* p = output;
	char* startp = p; // Remember where we started.

	*p++ = '0';
	*p++ = 'x';

	for (uint32_t i = 0; i < len; i++) {
		sprintf(p, "%02x", d[i]);
		p += 2;
	}
	*p++ = 0; // Null terminate the output buffer.
	return (int) (p - startp); // show how much space we used.
} // end generate_packed_hex_string()


/**
 * Generate a Spaced Hex String Representation of the binary string.
 * e.g. fc 86 e8 3a 6d 6d 30 24 65 9e 6f e4 8c 35 1a aa f6 e9 64 a5
 */
int
generate_spaced_hex_string(const void *mem_ptr, uint32_t len, char* output)
{
	uint8_t *d = (uint8_t *) mem_ptr;
	char* p = output;
	char* startp = p; // Remember where we started.

	for (uint32_t i = 0; i < len; i++) {
		sprintf(p, "%02x ", d[i]); // Notice the space after the 02x.
		p += 3;
	}
	*p++ = 0; // Null terminate the output buffer.
	return (int) (p - startp); // show how much space we used.
} // end generate_spaced_hex_string()


/**
 * Generate a Column Hex String Representation of the binary string.
 * The Columns will be four two-byte values, with spaces between the bytes:
 * fc86 e83a 6d6d 3024
 * 659e 6fe4 8c35 1aaa
 * f6e9 64a5
 */
int
generate_column_hex_string(const void *mem_ptr, uint32_t len, char* output)
{
	uint8_t *d = (uint8_t *) mem_ptr;
	char* p = output;
	uint32_t i;
	char* startp = p; // Remember where we started.

	*p++ = '\n'; // Start out on a new line

	for (i = 0; i < len; i++) {
		sprintf(p, "%02x ", d[i]); // Two chars and a space
		p += 3;
		if ((i+1) % 8 == 0 && i != 0) {
			*p++ = '\n';  // add a line return
		}
	}
	*p++ = '\n'; // Finish with a new line
	*p++ = 0; // Null terminate the output buffer.
	return (int) (p - startp); // show how much space we used.
} // end generate_column_hex_string()


/**
 * Generate a Base64 String Representation of the binary string.
 * Base64 encoding converts three octets into four 6-bit encoded characters.
 * So, the string 8-bit bytes are broken down into 6 bit values, each of which
 * is then converted into a base64 value.
 * So, for example, the string "Man" :: M[77: 0x4d)] a[97(0x61)] n[110(0x6e)]
 * Bits: (4)0100 (d)1101 (6)0110 (1)0001 (6)0110 (e)1110
 * Base 64 bits: 010011     010110     000101    101110
 * Base 64 Rep:  010011(19) 010110(22) 000101(5) 101110(46)
 * Base 64 Chars:     T(19)      W(22)      F(5)      u(46)
 * and so this string is converted into the Base 64 string: "TWFu"
 */
int generate_base64_string(const void *mem_ptr, uint32_t len, char output_buf[])
{
	uint32_t encoded_len = cf_b64_encoded_len(len);
	// TODO - check that output_buf is big enough, and/or truncate.

	cf_b64_encode((const uint8_t*)mem_ptr, len, output_buf);

	output_buf[encoded_len] = 0; // null-terminate

	return (int)(encoded_len + 1); // bytes we used, including null-terminator
} // end generate_base64_hex_string()


/**
 * Generate a BIT representation with spaces between the four bit groups.
 * Print the bits left to right (big to small).
 * This is assuming BIG ENDIAN representation (most significant bit is left).
 */
int generate_4spaced_bits_string(const void *mem_ptr, uint32_t len, char* output)
{
	uint8_t *d = (uint8_t *) mem_ptr;
	char* p = output;
	uint8_t uint_val;
	uint8_t mask = 0x80; // largest single bit value in a byte
	char* startp = p; // Remember where we started.

	// For each byte in the string
	for (uint32_t i = 0; i < len; i++) {
		uint_val = d[i];
		for (int j = 0; j < 8; j++) {
			sprintf(p, "%1d", ((uint_val << j) & mask));
			p++;
			// Add a space after every 4th bit
			if ( (j+1) % 4 == 0 ) *p++ = ' ';
		}
	}
	*p++ = 0; // Null terminate the output buffer.
	return (int) (p - startp); // show how much space we used.
} // end generate_4spaced_bits_string()

/**
 * Generate a BIT representation of columns with spaces between the
 * four bit groups.  Columns will be 8 columns of 4 bits.
 * (1 32 bit word per row)
 */
int generate_column_bits_string(const void *mem_ptr, uint32_t len, char* output)
{
	uint8_t *d = (uint8_t *) mem_ptr;
	char* p = output;
	uint8_t uint_val;
	uint8_t mask = 0x80; // largest single bit value in a byte
	char* startp = p; // Remember where we started.

	// Start on a new line
	*p++ = '\n';

	// For each byte in the string
	for (uint32_t i = 0; i < len; i++) {
		uint_val = d[i];
		for (int j = 0; j < 8; j++) {
			sprintf(p, "%1d", ((uint_val << j) & mask));
			p++;
			// Add a space after every 4th bit
			if ((j + 1) % 4 == 0) *p++ = ' ';
		}
		// Add a line return after every 4th byte
		if ((i + 1) % 4 == 0) *p++ = '\n';
	}
	*p++ = 0; // Null terminate the output buffer.
	return (int) (p - startp); // show how much space we used.
} // end generate_column_bits_string()


/* cf_fault_event -- TWO:  Expand on the LOG ability by being able to
 * print the contents of a BINARY array if we're passed a valid ptr (not NULL).
 * We will print the array according to "format".
 * Parms:
 * (*) scope: The module family (e.g. AS_RW, AS_UDF...)
 * (*) severify: The scope severity (e.g. INFO, DEBUG, DETAIL)
 * (*) file_name: Ptr to the FILE generating the call
 * (*) line: The function (really, the FILE) line number of the source call
 * (*) mem_ptr: Ptr to memory location of binary array (or NULL)
 * (*) len: Length of the binary string
 * (*) format: The single char showing the format (e.g. 'D', 'B', etc)
 * (*) msg: The format msg string
 * (*) ... : The variable set of parameters the correspond to the msg string.
 *
 * NOTE: We will eventually merge this function with the original cf_fault_event()
 **/
void
cf_fault_event2(const cf_fault_context context,
		const cf_fault_severity severity, const char *file_name, const int line,
		const void *mem_ptr, size_t len, cf_display_type dt, const char *msg, ...)
{
	va_list argp;
	char mbuf[MAX_BINARY_BUF_SZ];
	size_t pos;

	char binary_buf[MAX_BINARY_BUF_SZ];

	// Arbitrarily limit output to a fixed maximum length.
	if (len > MAX_BINARY_BUF_SZ) {
		len = MAX_BINARY_BUF_SZ;
	}
	char * labelp = NULL; // initialize to quiet build warning

	/* Make sure there's always enough space for the \n\0. */
	size_t limit = sizeof(mbuf) - 2;

	/* Set the timestamp */
	pos = cf_sprintf_now(mbuf, limit);

	// If we're given a valid MEMORY POINTER for a binary value, then
	// compute the string that corresponds to the bytes.
	if (mem_ptr) {
		switch (dt) {
		case CF_DISPLAY_HEX_DIGEST:
			labelp = "Digest";
			generate_packed_hex_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_HEX_SPACED:
			labelp = "HexSpaced";
			generate_spaced_hex_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_HEX_PACKED:
			labelp = "HexPacked";
			generate_packed_hex_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_HEX_COLUMNS:
			labelp = "HexColumns";
			generate_column_hex_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_BASE64:
			labelp = "Base64";
			generate_base64_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_BITS_SPACED:
			labelp = "BitsSpaced";
			generate_4spaced_bits_string(mem_ptr, len, binary_buf);
			break;
		case CF_DISPLAY_BITS_COLUMNS:
			labelp = "BitsColumns";
			generate_column_bits_string(mem_ptr, len, binary_buf);
			break;
		default:
			labelp = "Unknown Format";
			binary_buf[0] = 0; // make sure it's null terminated.
			break;

		} // end switch
	} // if binary data is present

	/* Set the context/scope/severity tag */
	pos += snprintf(mbuf + pos, limit - pos, "%s (%s): ",
			severity_tag(severity),
			cf_fault_context_strings[context]);

	/*
	 * snprintf() and vsnprintf() will not write more than the size specified,
	 * but they return the size that would have been written without truncation.
	 * These checks make sure there's enough space for the final \n\0.
	 */
	if (pos > limit) {
		pos = limit;
	}

	/* Set the location: filename and line number */
	if (file_name) {
		pos += snprintf(mbuf + pos, limit - pos, "(%s:%d) ", file_name, line);
	}

	// Check for overflow (see above).
	if (pos > limit) {
		pos = limit;
	}

	/* Append the message */
	va_start(argp, msg);
	pos += vsnprintf(mbuf + pos, limit - pos, msg, argp);
	va_end(argp);

	// Check for overflow (see above).
	if (pos > limit) {
		pos = limit;
	}

	// Append our final BINARY string, if present (some might pass in NULL).
	if ( mem_ptr ) {
		pos += snprintf(mbuf + pos, limit - pos, "<%s>:%s", labelp, binary_buf);
	}

	// Check for overflow (see above).
	if (pos > limit) {
		pos = limit;
	}

	pos += snprintf(mbuf + pos, 2, "\n");

	/* Route the message to the correct destinations */
	if (0 == cf_fault_sinks_inuse) {
		/* If no fault sinks are defined, use stderr for critical messages */
		if (CF_CRITICAL == severity)
			fprintf(stderr, "%s", mbuf);
	} else {
		for (int i = 0; i < cf_fault_sinks_inuse; i++) {
			if ((severity <= cf_fault_sinks[i].limit[context]) || (CF_CRITICAL == severity)) {
				if (0 >= write(cf_fault_sinks[i].fd, mbuf, pos)) {
					// this is OK for a bit in case of a HUP. It's even better to queue the buffers and apply them
					// after the hup. TODO.
					fprintf(stderr, "internal failure in fault message write: %s\n", cf_strerror(errno));
				}
			}
		}
	}

	/* Critical errors */
	if (CF_CRITICAL == severity) {
		fflush(NULL);

		// Our signal handler will log a stack trace.
		raise(SIGUSR1);
	}
}


void
cf_fault_event_nostack(const cf_fault_context context,
		const cf_fault_severity severity, const char *fn, const int line,
		const char *msg, ...)
{
	va_list argp;
	char mbuf[1024];
	time_t now;
	struct tm nowtm;
	size_t pos;

	/* Make sure there's always enough space for the \n\0. */
	size_t limit = sizeof(mbuf) - 2;

	/* Set the timestamp */
	now = time(NULL);

	if (g_use_local_time) {
		localtime_r(&now, &nowtm);
		pos = strftime(mbuf, limit, "%b %d %Y %T GMT%z: ", &nowtm);
	}
	else {
		gmtime_r(&now, &nowtm);
		pos = strftime(mbuf, limit, "%b %d %Y %T %Z: ", &nowtm);
	}

	/* Set the context/scope/severity tag */
	pos += snprintf(mbuf + pos, limit - pos, "%s (%s): ", severity_tag(severity), cf_fault_context_strings[context]);

	/*
	 * snprintf() and vsnprintf() will not write more than the size specified,
	 * but they return the size that would have been written without truncation.
	 * These checks make sure there's enough space for the final \n\0.
	 */
	if (pos > limit) {
		pos = limit;
	}

	/* Set the location */
	if (fn)
		pos += snprintf(mbuf + pos, limit - pos, "(%s:%d) ", fn, line);

	if (pos > limit) {
		pos = limit;
	}

	/* Append the message */
	va_start(argp, msg);
	pos += vsnprintf(mbuf + pos, limit - pos, msg, argp);
	va_end(argp);

	if (pos > limit) {
		pos = limit;
	}

	pos += snprintf(mbuf + pos, 2, "\n");

	/* Route the message to the correct destinations */
	if (0 == cf_fault_sinks_inuse) {
		/* If no fault sinks are defined, use stderr for important messages */
		if (severity <= NO_SINKS_LIMIT)
			fprintf(stderr, "%s", mbuf);
	} else {
		for (int i = 0; i < cf_fault_sinks_inuse; i++) {
			if ((severity <= cf_fault_sinks[i].limit[context]) || (CF_CRITICAL == severity)) {
				if (0 >= write(cf_fault_sinks[i].fd, mbuf, pos)) {
					// this is OK for a bit in case of a HUP. It's even better to queue the buffers and apply them
					// after the hup. TODO.
					fprintf(stderr, "internal failure in fault message write: %s\n", cf_strerror(errno));
				}
			}
		}
	}

	/* Critical errors */
	if (CF_CRITICAL == severity) {
		fflush(NULL);

		// these signals don't throw stack traces in our system
		raise(SIGINT);
	}
}


int
cf_fault_sink_strlist(cf_dyn_buf *db)
{
	for (int i = 0; i < cf_fault_sinks_inuse; i++) {
		cf_dyn_buf_append_int(db, i);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db,cf_fault_sinks[i].path);
		cf_dyn_buf_append_char(db, ';');
	}
	cf_dyn_buf_chomp(db);
	return(0);
}


extern void
cf_fault_sink_logroll(void)
{
	fprintf(stderr, "cf_fault: rolling log files\n");
	for (int i = 0; i < cf_fault_sinks_inuse; i++) {
		cf_fault_sink *s = &cf_fault_sinks[i];
		if ((0 != strncmp(s->path, "stderr", 6)) && (s->fd > 2)) {
			int old_fd = s->fd;

			// Note - we use O_TRUNC, so we assume the file has been
			// moved/copied elsewhere, or we're ok losing it.
			s->fd = open(s->path, SINK_REOPEN_FLAGS, SINK_OPEN_MODE);

			usleep(1000); // threads may be interrupted while writing to old fd
			close(old_fd);
		}
	}
}


cf_fault_sink *cf_fault_sink_get_id(int id)
{
	if (id > cf_fault_sinks_inuse)	return(0);
	return ( &cf_fault_sinks[id] );
}

int
cf_fault_sink_context_all_strlist(int sink_id, cf_dyn_buf *db)
{
	// get the sink
	if (sink_id > cf_fault_sinks_inuse)	return(-1);
	cf_fault_sink *s = &cf_fault_sinks[sink_id];

	for (int i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++) {
		cf_dyn_buf_append_string(db, cf_fault_context_strings[i]);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_string(db, cf_fault_severity_strings[s->limit[i]]);
		cf_dyn_buf_append_char(db, ';');
	}
	cf_dyn_buf_chomp(db);
	return(0);
}

int
cf_fault_sink_context_strlist(int sink_id, char *context, cf_dyn_buf *db)
{
	// get the sink
	if (sink_id > cf_fault_sinks_inuse)	return(-1);
	cf_fault_sink *s = &cf_fault_sinks[sink_id];

	// get the severity
	int i;
	for (i = 0; i < CF_FAULT_CONTEXT_UNDEF; i++) {
		if (0 == strcmp(cf_fault_context_strings[i],context))
			break;
	}
	if (i == CF_FAULT_CONTEXT_UNDEF) {
		cf_dyn_buf_append_string(db, context);
		cf_dyn_buf_append_string(db, ":unknown");
		return(0);
	}

	// get the string
	cf_dyn_buf_append_string(db, context);
	cf_dyn_buf_append_char(db, ':');
	cf_dyn_buf_append_string(db, cf_fault_severity_strings[s->limit[i]]);
	return(0);
}


static int
cf_fault_cache_reduce_fn(const void *key, void *data, void *udata)
{
	uint32_t *count = (uint32_t*)data;

	if (*count == 0) {
		return CF_SHASH_REDUCE_DELETE;
	}

	const cf_fault_cache_hkey *hkey = (const cf_fault_cache_hkey*)key;

	cf_fault_event(hkey->context, hkey->severity, hkey->file_name, hkey->line,
			"(repeated:%u) %s", *count, hkey->msg);

	*count = 0;

	return CF_SHASH_OK;
}


// For now there's only one cache, dumped by the ticker.
void
cf_fault_dump_cache()
{
	cf_shash_reduce(g_ticker_hash, cf_fault_cache_reduce_fn, NULL);
}


// For now there's only one cache, dumped by the ticker.
void
cf_fault_cache_event(cf_fault_context context, cf_fault_severity severity,
		const char *file_name, int line, char *msg, ...)
{
	cf_fault_cache_hkey key = {
			.line = line,
			.context = context,
			.file_name = file_name,
			.severity = severity,
			.msg = { 0 } // must pad hash keys
	};

	size_t limit = sizeof(key.msg) - 1; // truncate leaving null-terminator

	va_list argp;

	va_start(argp, msg);
	vsnprintf(key.msg, limit, msg, argp);
	va_end(argp);

	while (true) {
		uint32_t *valp = NULL;
		cf_mutex *lockp = NULL;

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

		cf_fault_event(context, severity, file_name, line, "%s", key.msg);
		break;
	}
}

void
cf_fault_hex_dump(const char *title, const void *data, size_t len)
{
	const uint8_t *data8 = data;
	char line[8 + 3 * 16 + 17];
	size_t k;

	cf_info(CF_MISC, "hex dump - %s", title);

	for (size_t i = 0; i < len; i += k) {
		sprintf(line, "%06zx:                                                                 ", i);

		for (k = 0; i + k < len && k < 16; ++k) {
			char num[3];
			uint8_t d = data8[i + k];
			sprintf(num, "%02x", d);
			line[8 + 3 *  k + 0] = num[0];
			line[8 + 3 *  k + 1] = num[1];
			line[8 + 3 * 16 + k] = d >= 32 && d <= 126 ? d : '.';
		}

		cf_info(CF_MISC, "%s", line);
	}
}
