/*
 * os.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike,),Inc. under one or more contributor
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

#include "os.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dynbuf.h"
#include "log.h"

#include "warnings.h"


//==========================================================
// Globals.
//

static bool g_use_group_perms = false;


//==========================================================
// Forward declarations.
//

static cf_os_file_res cf_os_read_any_file(const char** paths, uint32_t n_paths, void* buf, size_t* limit);

static void check_min_free_kbytes(cf_dyn_buf* db, uint64_t max_alloc_sz);
static void check_swappiness(cf_dyn_buf* db);
static void check_thp(cf_dyn_buf* db);
static void check_zone_reclaim_mode(cf_dyn_buf* db);


//==========================================================
// Inlines & macros.
//

#define check_failed(_db, _name, _msg, ...) \
	do { \
		cf_warning(CF_OS, "failed " _name " check - " _msg, ##__VA_ARGS__); \
		cf_dyn_buf_append_string(_db, _name); \
		cf_dyn_buf_append_char(_db, ','); \
	} \
	while (false)


//==========================================================
// Public API - file permissions.
//

void
cf_os_use_group_perms(bool use)
{
	g_use_group_perms = use;

	if (use) {
		umask((mode_t)(S_IROTH | S_IWOTH)); // but leaves GRP on
	}
	else {
		umask((mode_t)(S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH));
	}
}

bool
cf_os_is_using_group_perms(void)
{
	return g_use_group_perms;
}


//==========================================================
// Public API - read system files.
//

cf_os_file_res
cf_os_read_file(const char* path, void* buf, size_t* limit)
{
	cf_detail(CF_OS, "reading file %s with buffer size %zu", path, *limit);

	int32_t fd = open(path, O_RDONLY);

	if (fd < 0) {
		if (errno == ENOENT) {
			cf_detail(CF_OS, "file %s not found", path);
			return CF_OS_FILE_RES_NOT_FOUND;
		}

		cf_warning(CF_OS, "error while opening file %s for reading: %d (%s)",
				path, errno, cf_strerror(errno));

		return CF_OS_FILE_RES_ERROR;
	}

	size_t total = 0;

	while (total < *limit) {
		cf_detail(CF_OS, "reading %zd byte(s) at offset %zu", *limit - total,
				total);

		ssize_t len = read(fd, (uint8_t *)buf + total, *limit - total);

		CF_NEVER_FAILS(len);

		if (len == 0) {
			cf_detail(CF_OS, "EOF");
			break;
		}

		total += (size_t)len;
	}

	cf_detail(CF_OS, "read %zu byte(s) from file %s", total, path);

	bool res;

	if (total == *limit) {
		cf_warning(CF_OS, "read buffer too small for file %s", path);
		res = CF_OS_FILE_RES_ERROR;
	}
	else {
		res = CF_OS_FILE_RES_OK;
		*limit = total;
	}

	CF_NEVER_FAILS(close(fd));

	return res;
}

cf_os_file_res
cf_os_read_int_from_file(const char* path, int64_t* val)
{
	cf_detail(CF_OS, "reading value from file %s", path);

	char buf[100];
	size_t limit = sizeof(buf);
	cf_os_file_res res = cf_os_read_file(path, buf, &limit);

	if (res != CF_OS_FILE_RES_OK) {
		return res;
	}

	buf[limit - 1] = '\0';

	cf_detail(CF_OS, "parsing value \"%s\"", buf);

	char *end;
	int64_t x = strtol(buf, &end, 10);

	if (*end != '\0') {
		cf_warning(CF_OS, "invalid value \"%s\" in %s", buf, path);
		return CF_OS_FILE_RES_ERROR;
	}

	*val = x;

	return CF_OS_FILE_RES_OK;
}


//==========================================================
// Public API - best practices.
//

void
cf_os_best_practices_check(cf_dyn_buf* db, uint64_t max_alloc_sz)
{
	check_min_free_kbytes(db, max_alloc_sz);
	check_thp(db);
	check_swappiness(db);
	check_zone_reclaim_mode(db);
}


//==========================================================
// Local helpers - read system files.
//

cf_os_file_res
cf_os_read_any_file(const char** paths, uint32_t n_paths, void* buf,
		size_t* limit)
{
	for (uint32_t i = 0; i < n_paths; i++) {
		cf_os_file_res res = cf_os_read_file(paths[i], buf, limit);

		if (res == CF_OS_FILE_RES_OK || res == CF_OS_FILE_RES_ERROR) {
			return res;
		}
	}

	return CF_OS_FILE_RES_NOT_FOUND;
}


//==========================================================
// Local helpers - best practices.
//

static void
check_min_free_kbytes(cf_dyn_buf* db, uint64_t max_alloc_sz)
{
	static const char* path = "/proc/sys/vm/min_free_kbytes";
	uint64_t minimum_kb = (max_alloc_sz / 1024) + (100 * 1024);
	uint64_t min_free_kbytes;

	switch (cf_os_read_int_from_file(path, (int64_t*)&min_free_kbytes)) {
	case CF_OS_FILE_RES_OK:
		if (min_free_kbytes < minimum_kb) {
			check_failed(db, "min-free-kbytes",
					"min_free_kbytes should be at least %lu", minimum_kb);
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '%s'", path);
	}
}

static void
check_swappiness(cf_dyn_buf* db)
{
	static const char* path = "/proc/sys/vm/swappiness";
	int64_t swappiness;

	switch (cf_os_read_int_from_file(path, &swappiness)) {
	case CF_OS_FILE_RES_OK:
		if (swappiness != 0) {
			check_failed(db, "swappiness", "swappiness not set to 0");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '%s'", path);
	}
}

static void
check_thp(cf_dyn_buf* db)
{
	static const char* enabled_paths[] = {
			"/sys/kernel/mm/transparent_hugepage/enabled",
			"/sys/kernel/mm/redhat_transparent_hugepage/enabled"
	};
	uint32_t n_enabled_paths = sizeof(enabled_paths) / sizeof(char*);

	char buf[128];
	size_t limit = sizeof(buf) - 1;

	switch (cf_os_read_any_file(enabled_paths, n_enabled_paths, buf, &limit)) {
	case CF_OS_FILE_RES_OK:
		buf[limit] = '\0';
		if (strstr(buf, "[never]") != NULL) {
			return;
		}
		if (strstr(buf, "[madvise]") == NULL) {
			check_failed(db, "thp-enabled",
					"THP enabled not set to either 'never' or 'madvise'");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		cf_detail(CF_OS, "unable to find '/sys/kernel/mm/{redhat_,}transparent_hugepage/enabled'");
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '/sys/kernel/mm/{redhat_,}transparent_hugepage/enabled'");
	}

	static const char* defrag_paths[] = {
			"/sys/kernel/mm/transparent_hugepage/defrag",
			"/sys/kernel/mm/redhat_transparent_hugepage/defrag"
	};
	uint32_t n_defrag_paths = sizeof(defrag_paths) / sizeof(char*);
	limit = sizeof(buf) - 1;

	switch (cf_os_read_any_file(defrag_paths, n_defrag_paths, buf, &limit)) {
	case CF_OS_FILE_RES_OK:
		buf[limit] = '\0';
		if (strstr(buf, "[never]") == NULL &&
				strstr(buf, "[madvise]") == NULL) {
			check_failed(db, "thp-defrag",
					"THP defrag not set to either 'never' or 'madvise'");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		cf_detail(CF_OS, "unable to find '/sys/kernel/mm/{redhat_,}transparent_hugepage/defrag'");
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '/sys/kernel/mm/{redhat_,}transparent_hugepage/defrag'");
	}
}

static void
check_zone_reclaim_mode(cf_dyn_buf* db)
{
	static const char* path = "/proc/sys/vm/zone_reclaim_mode";
	int64_t mode;

	switch (cf_os_read_int_from_file(path, &mode)) {
	case CF_OS_FILE_RES_OK:
		if (mode != 0) {
			check_failed(db, "zone-reclaim-mode",
					"zone_reclaim_mode not set to 0");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '%s'", path);
	}
}
