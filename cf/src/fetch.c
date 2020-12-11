/*
 * fetch.c
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include "fetch.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"

#include "log.h"
#include "vault.h"


//==========================================================
// Typedefs & constants.
//

#define CONFIGURED_FILE_MAX_SIZE (10 * 1024 * 1024)

#define ENV_PATH_PREFIX "env:"
#define ENV_PATH_PREFIX_LEN (sizeof(ENV_PATH_PREFIX) - 1)

#define ENV_B64_PATH_PREFIX "env-b64:"
#define ENV_B64_PATH_PREFIX_LEN (sizeof(ENV_B64_PATH_PREFIX) - 1)

static const char TRAILING_NEWLINE[] = "\n\r";


//==========================================================
// Forward declarations.
//

static uint8_t* fetch_env_bytes(const char* path, size_t* size_r);
static uint8_t* fetch_env_b64_bytes(const char* path, size_t* size_r);
static uint8_t* fetch_bytes_from_file(const char* file_path, size_t* size_r);


//==========================================================
// Inlines & macros.
//

static inline bool
is_env_path(const char* path)
{
	return strncmp(path, ENV_PATH_PREFIX, ENV_PATH_PREFIX_LEN) == 0;
}

static inline bool
is_env_b64_path(const char* path)
{
	return strncmp(path, ENV_B64_PATH_PREFIX, ENV_B64_PATH_PREFIX_LEN) == 0;
}


//==========================================================
// Public API.
//

bool
cf_fetch_is_env_path(const char* path)
{
	return is_env_path(path) || is_env_b64_path(path);
}

// Caller must cf_free return value when done.
uint8_t*
cf_fetch_bytes(const char* path, size_t* size_r)
{
	cf_assert(path != NULL, CF_MISC, "fetch with null path");

	if (is_env_path(path)) {
		return fetch_env_bytes(path, size_r);
	}

	if (is_env_b64_path(path)) {
		return fetch_env_b64_bytes(path, size_r);
	}

	if (cf_vault_is_vault_path(path)) {
		if (! cf_vault_is_configured()) {
			return NULL;
		}

		return cf_vault_fetch_bytes(path, size_r);
	}

	return fetch_bytes_from_file(path, size_r);
}

// Caller must cf_free return value when done.
char*
cf_fetch_string(const char* path)
{
	size_t len;
	uint8_t* buf = cf_fetch_bytes(path, &len);

	if (buf == NULL) {
		return NULL;
	}

	// Strip newlines from the end. It's common for inadvertent newlines to be
	// appended when editing files to insert content - try to forgive this.
	while (strchr(TRAILING_NEWLINE, buf[len - 1]) != NULL) {
		len--;

		if (len == 0) {
			cf_warning(CF_MISC, "empty string");
			cf_free(buf);
			return NULL;
		}
	}

	buf[len] = '\0';

	// Make sure there are no inadvertent null bytes in the string.
	if (strlen((char*)buf) != len) {
		cf_warning(CF_MISC, "string contains null byte");
		cf_free(buf);
		return NULL;
	}

	return (char*)buf;
}


//==========================================================
// Local helpers.
//

// Caller must cf_free return value when done.
static uint8_t*
fetch_env_bytes(const char* path, size_t* size_r)
{
	char* val = getenv(path + ENV_PATH_PREFIX_LEN);

	if (val == NULL || *val == '\0') {
		cf_warning(CF_MISC, "missing or empty variable for %s", path);
		return NULL;
	}

	*size_r = strlen(val);

	return (uint8_t*)cf_strdup(val);
}

// Caller must cf_free return value when done.
static uint8_t*
fetch_env_b64_bytes(const char* path, size_t* size_r)
{
	char* val = getenv(path + ENV_B64_PATH_PREFIX_LEN);

	if (val == NULL || *val == '\0') {
		cf_warning(CF_MISC, "missing or empty variable for %s", path);
		return NULL;
	}

	uint32_t len = (uint32_t)strlen(val);
	uint32_t size = cf_b64_decoded_buf_size(len);
	uint8_t* buf = cf_malloc(size + 1); // +1 for null terminator

	if (! cf_b64_validate_and_decode(val, len, buf, &size)) {
		cf_warning(CF_MISC, "invalid b64 variable for %s", path);
		cf_free(buf);
		return NULL;
	}

	*size_r = size;

	return buf;
}

// Caller must cf_free return value when done.
static uint8_t*
fetch_bytes_from_file(const char* file_path, size_t* size_r)
{
	int fd = open(file_path, O_RDONLY);

	if (fd == -1) {
		cf_warning(CF_MISC, "unable to open file %s: %s", file_path,
				cf_strerror(errno));
		return NULL;
	}

	off_t size = lseek(fd, 0, SEEK_END);

	if (size == -1) {
		cf_warning(CF_MISC, "unable to seek to end of file %s: %s", file_path,
				cf_strerror(errno));
		close(fd);
		return NULL;
	}

	if (size == 0) {
		cf_warning(CF_MISC, "empty file %s", file_path);
		close(fd);
		return NULL;
	}

	if (size > CONFIGURED_FILE_MAX_SIZE) {
		cf_warning(CF_MISC, "file %s too big %zu", file_path, size);
		close(fd);
		return NULL;
	}

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_warning(CF_MISC, "unable to seek to start of file %s: %s", file_path,
				cf_strerror(errno));
		close(fd);
		return NULL;
	}

	// Extra byte - if this is a string, caller will add '\0'.
	uint8_t* buf = cf_malloc(size + 1);

	uint8_t* at = buf;
	size_t bytes_left = (size_t)size;

	while (bytes_left > 0) {
		ssize_t rv = read(fd, at, bytes_left);

		if (rv <= 0) {
			cf_warning(CF_MISC, "unable to read file %s: %zd %s", file_path, rv,
					rv != 0 ? cf_strerror(errno) : "");
			cf_free(buf);
			close(fd);
			return NULL;
		}

		at += rv;
		bytes_left -= rv;
	}

	close(fd);

	*size_r = (size_t)size;
	return buf;
}
