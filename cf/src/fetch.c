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
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"

#include "log.h"
#include "vault.h"


//==========================================================
// Typedefs & constants.
//

#define CONFIGURED_FILE_MAX_SIZE (10 * 1024 * 1024)

static const char TRAILING_NEWLINE[] = "\n\r";


//==========================================================
// Forward declarations.
//

static uint8_t* fetch_bytes_from_file(const char* file_path, size_t* size_r);


//==========================================================
// Public API.
//

// Caller must cf_free return value when done.
uint8_t*
cf_fetch_bytes(const char* path, size_t* size_r)
{
	cf_assert(path != NULL, CF_MISC, "fetch with null path");

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
