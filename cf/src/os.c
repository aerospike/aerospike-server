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
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cf_defer.h"
#include "dynbuf.h"
#include "log.h"

#include "warnings.h"

//==========================================================
// Globals.
//

static bool g_use_group_perms = false;
static cf_os_test_read_file_fn g_mem_read_file_fn = cf_os_read_file;

//==========================================================
// Forward declarations.
//

static cf_os_file_res cf_os_read_any_file(const char** paths, uint32_t n_paths,
		void* buf, size_t* limit);

static inline cf_os_file_res mem_read_file(const char* path, void* buf,
		size_t* limit);

static void check_thp(cf_dyn_buf* db);

static bool not_mountinfo_next_field(const char** cursor, const char** field,
		size_t* field_len);

static bool mountinfo_parse_root_and_mount_point(const char* line,
		char* mount_root, size_t mount_root_sz, char* mount_point,
		size_t mount_point_sz);

static bool mountinfo_has_v1_controller(const char* super_options,
		const char* controller);

//==========================================================
// Inlines & Macros
//

static inline cf_os_file_res
mem_read_file(const char* path, void* buf, size_t* limit)
{
	return g_mem_read_file_fn(path, buf, limit);
}

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

void
cf_os_set_mem_read_file_fn_for_test(cf_os_test_read_file_fn fn)
{
	g_mem_read_file_fn = fn == NULL ? cf_os_read_file : fn;
}

//==========================================================
// Private API - get memory info
static void
close_fd_deferred(int32_t* fd)
{
	if (*fd >= 0) {
		close(*fd);
	}
}

static void
sys_mem_info(uint64_t* free_mem_kbytes, uint32_t* free_mem_pct,
		uint64_t* thp_mem_kbytes)
{
	*free_mem_kbytes = 0;
	*free_mem_pct = 0;
	*thp_mem_kbytes = 0;

	char buf[4096] = { 0 };
	size_t limit = 0;

	if (g_mem_read_file_fn != NULL) {
		limit = sizeof(buf) - 1;

		if (g_mem_read_file_fn("/proc/meminfo", buf, &limit) !=
				CF_OS_FILE_RES_OK) {
			cf_warning(AS_INFO, "failed to read /proc/meminfo");
			return;
		}

		buf[limit] = '\0';
	}
	else {
		DEFER_ATTR(close_fd_deferred)
		int32_t fd = open("/proc/meminfo", O_RDONLY);

		if (fd < 0) {
			cf_warning(AS_INFO, "failed to open /proc/meminfo: %d", errno);
			return;
		}

		limit = sizeof(buf);
		size_t total = 0;

		while (total < limit) {
			ssize_t len = read(fd, buf + total, limit - total);

			if (len < 0) {
				cf_warning(AS_INFO, "couldn't read /proc/meminfo: %d", errno);
				return;
			}

			if (len == 0) {
				break; // EOF
			}

			total += (size_t)len;
		}

		if (total == limit) {
			cf_warning(AS_INFO, "/proc/meminfo exceeds %zu bytes", limit);
			return;
		}

		limit = total;
	}

	uint64_t mem_total = 0;
	uint64_t mem_available = 0;
	uint64_t anon_huge_pages = 0;

	char* cur = buf;
	char* save_ptr = NULL;

	// We split each line into two fields separated by ':'. strtoul() will
	// safely ignore the spaces and 'kB' (if present).
	while (true) {
		char* name_tok = strtok_r(cur, ":", &save_ptr);

		if (name_tok == NULL) {
			break; // no more lines
		}

		cur = NULL; // all except first name_tok use NULL
		char* value_tok = strtok_r(NULL, "\r\n", &save_ptr);

		if (value_tok == NULL) {
			cf_warning(AS_INFO, "/proc/meminfo line missing value token");
			return;
		}

		if (strcmp(name_tok, "MemTotal") == 0) {
			mem_total = strtoul(value_tok, NULL, 0);
		}
		else if (strcmp(name_tok, "MemAvailable") == 0) {
			mem_available = strtoul(value_tok, NULL, 0);
		}
		else if (strcmp(name_tok, "AnonHugePages") == 0) {
			anon_huge_pages = strtoul(value_tok, NULL, 0);
		}
	}

	*free_mem_kbytes = mem_available;
	*free_mem_pct = mem_total == 0 ? 0 : (mem_available * 100) / mem_total;
	*thp_mem_kbytes = anon_huge_pages;
}

static bool
not_mountinfo_next_field(const char** cursor, const char** field,
		size_t* field_len)
{
	while (**cursor == ' ') {
		(*cursor)++;
	}

	if (**cursor == '\0') {
		return true;
	}

	*field = *cursor;

	while (**cursor != '\0' && **cursor != ' ') {
		(*cursor)++;
	}

	*field_len = (size_t)(*cursor - *field);
	return false;
}

static bool
mountinfo_parse_root_and_mount_point(const char* line, char* mount_root,
		size_t mount_root_sz, char* mount_point, size_t mount_point_sz)
{
	const char* cursor = line;
	const char* field;
	size_t field_len;

	for (int i = 0; i < 3; i++) {
		if (not_mountinfo_next_field(&cursor, &field, &field_len)) {
			return false;
		}
	}

	if (not_mountinfo_next_field(&cursor, &field, &field_len) ||
			field_len >= mount_root_sz) {
		return false;
	}

	memcpy(mount_root, field, field_len);
	mount_root[field_len] = '\0';

	if (not_mountinfo_next_field(&cursor, &field, &field_len) ||
			field_len >= mount_point_sz) {
		return false;
	}

	memcpy(mount_point, field, field_len);
	mount_point[field_len] = '\0';
	return true;
}

static bool
mountinfo_has_v1_controller(const char* super_options, const char* controller)
{
	if (super_options == NULL || controller == NULL || controller[0] == '\0') {
		return false;
	}

	size_t controller_len = strlen(controller);
	const char* cur = super_options;

	while (cur[0] != '\0') {
		const char* next = strchr(cur, ',');
		size_t tok_len = next == NULL ? strlen(cur) : (size_t)(next - cur);

		if (tok_len == controller_len &&
				strncmp(cur, controller, controller_len) == 0) {
			return true;
		}

		if (next == NULL) {
			break;
		}

		cur = next + 1;
	}

	return false;
}

static int
find_cgroup_path(const char* rscctrl, char* mount_pth_buf, size_t mount_buf_len,
		char* root_pth_buf, size_t root_buf_len)
{
	char mount_point[PATH_MAX] = { 0 };
	char mount_root[PATH_MAX] = { 0 };
	bool v2_found = false;

	cf_dyn_buf db;
	cf_dyn_buf_init_heap(&db, 16 * 1024);

	cf_os_file_res res;
	size_t limit;

	while (true) {
		limit = db.alloc_sz - 1;
		res = mem_read_file("/proc/self/mountinfo", db.buf, &limit);

		if (res == CF_OS_FILE_RES_OK) {
			db.buf[limit] = '\0';
			break;
		}

		if (db.alloc_sz < 1024 * 1024) { // Limit to 1MB
			cf_dyn_buf_reserve(&db, db.alloc_sz * 2, NULL);
		}
		else {
			cf_dyn_buf_free(&db);
			return 0;
		}
	}

	int found_mount = 0;
	char* saveptr_line;
	for (char* line = strtok_r((char*)db.buf, "\n", &saveptr_line);
			line != NULL; line = strtok_r(NULL, "\n", &saveptr_line)) {

		// mountinfo format:
		// mount_id parent_id major:minor root mount_point mount_options [optional_fields] - fs_type mount_source super_options
		char* dash = strstr(line, " - ");

		if (! dash) {
			continue;
		}

		char fs_type[64] = { 0 };

		if (sscanf(dash + 3, "%63s", fs_type) != 1) {
			continue;
		}

		bool is_v2 = (strcmp(fs_type, "cgroup2") == 0);
		bool is_v1 = (strcmp(fs_type, "cgroup") == 0);

		const char* super_options = NULL;

		if (is_v1) {
			super_options = dash + 3;

			for (int i = 0; i < 2; i++) {
				while (*super_options != '\0' && *super_options != ' ') {
					super_options++;
				}

				while (*super_options == ' ') {
					super_options++;
				}
			}

			if (*super_options == '\0') {
				super_options = NULL;
			}
		}

		if (is_v2 ||
				(is_v1 && mountinfo_has_v1_controller(super_options, rscctrl))) {
			// Extract root and mount_point from the beginning of the line
			// Fields 1-3 are integers/major:minor, 4 is root, 5 is mount_point
			if (mountinfo_parse_root_and_mount_point(line, mount_root,
						sizeof(mount_root), mount_point, sizeof(mount_point))) {
				found_mount = 1;
				v2_found = is_v2;
				break;
			}
		}
	}

	cf_dyn_buf_free(&db);

	if (found_mount) {
		snprintf(mount_pth_buf, mount_buf_len, "%s", mount_point);

		if (root_pth_buf != NULL && root_buf_len != 0) {
			snprintf(root_pth_buf, root_buf_len, "%s", mount_root);
		}

		return v2_found ? 2 : 1;
	}

	if (root_pth_buf != NULL && root_buf_len != 0) {
		root_pth_buf[0] = '\0';
	}

	return 0;
}

static bool
find_cgroup_path_v2(char* pth_buf, size_t buf_len, const char* mount_root)
{
	char buf[PATH_MAX] = { 0 };
	size_t limit = sizeof(buf) - 1;
	cf_os_file_res res = mem_read_file("/proc/self/cgroup", buf, &limit);

	if (res != CF_OS_FILE_RES_OK) {
		return false;
	}

	buf[limit] = '\0';
	char* saveptr_line;

	for (char* line = strtok_r(buf, "\n", &saveptr_line); line != NULL;
			line = strtok_r(NULL, "\n", &saveptr_line)) {
		if (strncmp(line, "0::", 3) != 0) {
			continue;
		}

		const char* full_path = line + 3;

		if (*full_path == '\0') {
			full_path = "/";
		}

		if (mount_root != NULL && mount_root[0] != '\0' &&
				strcmp(mount_root, "/") != 0) {
			size_t root_len = strlen(mount_root);

			if (strcmp(full_path, mount_root) == 0) {
				snprintf(pth_buf, buf_len, "/");
				return true;
			}

			if (strncmp(full_path, mount_root, root_len) == 0 &&
					(full_path[root_len] == '/' || full_path[root_len] == '\0')) {
				const char* rel_path = full_path + root_len;

				if (*rel_path == '\0') {
					rel_path = "/";
				}

				snprintf(pth_buf, buf_len, "%s", rel_path);
				return true;
			}

			cf_detail(CF_OS, "cgroup v2 path %s is outside mount root %s",
					full_path, mount_root);
			return false;
		}

		snprintf(pth_buf, buf_len, "%s", full_path);
		return true;
	}

	return false;
}

static bool
find_smallest_cgroup_limit_v2(const char* mount_path,
		const char* leaf_cgroup_path, uint64_t* limit_bytes,
		char* limit_dir_pth, size_t limit_dir_pth_sz)
{
	char rel_path[PATH_MAX] = "/";
	uint64_t smallest_limit = 0;
	bool found = false;

	long page_size = sysconf(_SC_PAGESIZE);

	if (page_size <= 0) {
		page_size = 4096;
	}

	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/mm/memcontrol.c#L4240
	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/include/linux/page_counter.h#L46-L50
	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/mm/page_counter.c#L272
	uint64_t max_value =
			((uint64_t)LONG_MAX / (uint64_t)page_size) * (uint64_t)page_size;

	if (leaf_cgroup_path != NULL && leaf_cgroup_path[0] != '\0') {
		snprintf(rel_path, sizeof(rel_path), "%s", leaf_cgroup_path);
	}

	while (true) {
		char dir_path[PATH_MAX] = { 0 };
		char limit_path[PATH_MAX] = { 0 };
		char cg_limit[64] = { 0 };

		if (strcmp(rel_path, "/") == 0) {
			snprintf(dir_path, sizeof(dir_path), "%s", mount_path);
		}
		else {
			snprintf(dir_path, sizeof(dir_path), "%s%s", mount_path, rel_path);
		}

		snprintf(limit_path, sizeof(limit_path), "%s/memory.max", dir_path);

		size_t limit_buf_sz = sizeof(cg_limit) - 1;
		cf_os_file_res cg_max_read =
				mem_read_file(limit_path, cg_limit, &limit_buf_sz);

		if (cg_max_read == CF_OS_FILE_RES_OK) {
			cg_limit[limit_buf_sz] = '\0';
			uint64_t parsed_limit = strtoull(cg_limit, NULL, 10);
			bool not_max = strncmp(cg_limit, "max", 3) != 0 &&
					parsed_limit != max_value;
			bool update_smallest_limit = ! found || parsed_limit < smallest_limit;
			if (not_max && update_smallest_limit) {
				smallest_limit = parsed_limit;
				found = true;
				snprintf(limit_dir_pth, limit_dir_pth_sz, "%s", dir_path);
			}
		}

		if (strcmp(rel_path, "/") == 0) {
			break;
		}

		char* slash = strrchr(rel_path, '/');

		if (slash == NULL || slash == rel_path) {
			snprintf(rel_path, sizeof(rel_path), "/");
		}
		else {
			*slash = '\0';
		}
	}

	if (! found) {
		if (limit_dir_pth_sz != 0) {
			limit_dir_pth[0] = '\0';
		}

		return false;
	}

	*limit_bytes = smallest_limit;
	return true;
}

static bool
cgroup_mem_info(uint64_t host_free_mem_kbytes, uint64_t* free_mem_kbytes,
		uint32_t* free_mem_pct)
{
	char mount_path[PATH_MAX] = "/sys/fs/cgroup";
	char mount_root[PATH_MAX] = "/";
	char base_path[PATH_MAX] = { 0 };
	int version = find_cgroup_path("memory", mount_path, sizeof(mount_path),
			mount_root, sizeof(mount_root));

	cf_detail(CF_OS, "cgroup version: %d", version);

	if (version == 0) {
		cf_detail(CF_OS, "no cgroup path found for memory");
	}

	if (version == 2) {
		char cgroup_path[PATH_MAX] = { 0 };

		if (find_cgroup_path_v2(cgroup_path, sizeof(cgroup_path), mount_root)) {
			uint64_t smallest_limit = 0;

			if (find_smallest_cgroup_limit_v2(mount_path, cgroup_path,
						&smallest_limit, base_path, sizeof(base_path))) {
				cf_detail(CF_OS, "smallest cgroup v2 limit %lu found at %s",
						smallest_limit, base_path);
			}
			else if (strcmp(cgroup_path, "/") == 0) {
				snprintf(base_path, sizeof(base_path), "%s", mount_path);
			}
			else {
				snprintf(base_path, sizeof(base_path), "%s%s", mount_path,
						cgroup_path);
			}
		}
		else {
			snprintf(base_path, sizeof(base_path), "%s", mount_path);
		}
	}
	else {
		snprintf(base_path, sizeof(base_path), "%s", mount_path);
	}

	char nested_limit_pth[PATH_MAX] = { 0 };
	char nested_used_pth[PATH_MAX] = { 0 };

	if (version == 2) {
		snprintf(nested_limit_pth, sizeof(nested_limit_pth), "%s/memory.max",
				base_path);
		snprintf(nested_used_pth, sizeof(nested_used_pth), "%s/memory.current",
				base_path);
	}
	else {
		snprintf(nested_limit_pth, sizeof(nested_limit_pth),
				"%s/memory.limit_in_bytes", base_path);
		snprintf(nested_used_pth, sizeof(nested_used_pth),
				"%s/memory.usage_in_bytes", base_path);
	}

	*free_mem_kbytes = 0;
	*free_mem_pct = 0;

	cf_detail(CF_OS, "nested_limit_pth: %s", nested_limit_pth);
	cf_detail(CF_OS, "nested_used_pth: %s", nested_used_pth);

	char cg_limit[64] = { 0 };
	char cg_used[64] = { 0 };
	const char* cg_limit_path[] = {
		// try v2 first (common)
		nested_limit_pth,
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	};
	const char* cg_used_path[] = {
		nested_used_pth,
		"/sys/fs/cgroup/memory.current",
		"/sys/fs/cgroup/memory/memory.usage_in_bytes",
	};
	uint64_t cg_used_bytes = 0;
	uint64_t cg_limit_bytes = 0;

	long page_size = sysconf(_SC_PAGESIZE);

	if (page_size <= 0) {
		page_size = 4096;
	}

	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/mm/memcontrol.c#L4240
	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/include/linux/page_counter.h#L46-L50
	// https://github.com/torvalds/linux/blob/2d1373e4246da3b58e1df058374ed6b101804e07/mm/page_counter.c#L272
	uint64_t max_value =
			((uint64_t)LONG_MAX / (uint64_t)page_size) * (uint64_t)page_size;

	for (uint32_t i = 0; i < sizeof(cg_limit_path) / sizeof(const char*); i++) {
		size_t limit_buf_sz = sizeof(cg_limit) - 1;
		cf_os_file_res cg_max_read =
				mem_read_file(cg_limit_path[i], cg_limit, &limit_buf_sz);

		size_t used_buf_sz = sizeof(cg_used) - 1;
		cf_os_file_res cg_used_read =
				mem_read_file(cg_used_path[i], cg_used, &used_buf_sz);

		if (cg_max_read == CF_OS_FILE_RES_OK &&
				cg_used_read == CF_OS_FILE_RES_OK) {
			cg_used[used_buf_sz] = '\0';
			cg_limit[limit_buf_sz] = '\0';
			cg_limit[strcspn(cg_limit, "\n")] = '\0';

			uint64_t parsed_limit = strtoull(cg_limit, NULL, 10);

			if (strcmp(cg_limit, "max") == 0 || parsed_limit == max_value) {
				return false; // unlimited memory
			}

			cg_used_bytes = strtoull(cg_used, NULL, 10);
			cg_limit_bytes = strtoull(cg_limit, NULL, 10);

			break;
		}
	}

	if (cg_limit_bytes == 0) {
		return false;
	}

	if (cg_used_bytes > cg_limit_bytes) {
		cf_warning(CF_OS, "used bytes %lu > limit bytes %lu", cg_used_bytes,
				cg_limit_bytes);
		return false;
	}

	uint64_t cg_free_bytes = (cg_limit_bytes - cg_used_bytes);

	if (cg_free_bytes == 0) {
		cf_warning(CF_OS,
				"zero available cg memory: used bytes %lu == limit bytes %lu",
				cg_used_bytes, cg_limit_bytes);
		*free_mem_pct = 0;
		*free_mem_kbytes = 0;
		return true;
	}

	uint64_t effective_free_kbytes = cg_free_bytes / 1024;

	if (host_free_mem_kbytes < effective_free_kbytes) {
		effective_free_kbytes = host_free_mem_kbytes;
	}

	uint64_t cg_limit_kbytes = cg_limit_bytes / 1024;

	if (cg_limit_kbytes == 0) {
		return false;
	}

	*free_mem_kbytes = effective_free_kbytes;
	*free_mem_pct = (effective_free_kbytes * 100) / cg_limit_kbytes;

	if (*free_mem_pct <= 10) {
		cf_warning(CF_OS,
				"cgroup memory available %lu kb is <= 10 percent of detected cgroup memory limit %lu kb. enable the cgroup-mem-tracking config option to stop writes here",
				*free_mem_kbytes, cg_limit_kbytes);
	}

	return true;
}

//==========================================================
// Public API - get memory info

void
get_mem_info(bool cgroup_mode, uint64_t* free_mem_kbytes,
		uint32_t* free_mem_pct, uint64_t* host_free_mem_kbytes,
		uint32_t* host_free_mem_pct, uint64_t* thp_mem_kbytes)
{
	uint64_t host_thp_kbytes = 0;

	sys_mem_info(host_free_mem_kbytes, host_free_mem_pct, &host_thp_kbytes);

	uint64_t cgroup_free_mem_kbytes = 0;
	uint32_t cgroup_free_mem_pct = 0;

	bool cgroup_usable = cgroup_mem_info(*host_free_mem_kbytes,
			&cgroup_free_mem_kbytes, &cgroup_free_mem_pct);

	if (cgroup_mode && ! cgroup_usable) {
		cf_detail(CF_OS,
				"cgroup-mem-tracking enabled but no usable cgroup found, falling back to host memory tracking");
	}
	if (cgroup_mode && cgroup_usable) {
		*free_mem_kbytes = cgroup_free_mem_kbytes;
		*free_mem_pct = cgroup_free_mem_pct;
		*thp_mem_kbytes = host_thp_kbytes;
	}
	else {
		*free_mem_kbytes = *host_free_mem_kbytes;
		*free_mem_pct = *host_free_mem_pct;
		*thp_mem_kbytes = host_thp_kbytes;
	}
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

		ssize_t len = read(fd, (uint8_t*)buf + total, *limit - total);

		CF_NEVER_FAILS(len);

		if (len == 0) {
			cf_detail(CF_OS, "EOF");
			break;
		}

		total += (size_t)len;
	}

	cf_detail(CF_OS, "read %zu byte(s) from file %s", total, path);

	cf_os_file_res res;

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

	char* end;
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
cf_os_best_practices_checks(cf_dyn_buf* db, uint64_t max_alloc_sz)
{
	cf_os_best_practices_check("max-map-count", "/proc/sys/vm/max_map_count",
			262144, INT64_MAX, db);
	cf_os_best_practices_check("min-free-kbytes", "/proc/sys/vm/min_free_kbytes",
			((int64_t)max_alloc_sz / 1024) + (100 * 1024), INT64_MAX, db);
	check_thp(db);
	cf_os_best_practices_check("swappiness", "/proc/sys/vm/swappiness", 0, 0, db);
	cf_os_best_practices_check("zone-reclaim-mode",
			"/proc/sys/vm/zone_reclaim_mode", 0, 0, db);
	cf_os_best_practices_check("somaxconn", "/proc/sys/net/core/somaxconn",
			4096, INT64_MAX, db);

	cf_os_best_practices_checks_ee(db);
}

void
cf_os_best_practices_check(const char* name, const char* path, int64_t min,
		int64_t max, cf_dyn_buf* db)
{
	int64_t value;

	switch (cf_os_read_int_from_file(path, &value)) {
	case CF_OS_FILE_RES_OK:
		if (min == max && value != min) {
			os_check_failed(db, name, "%s not set to %ld", name, min);
		}
		else if (value < min) {
			os_check_failed(db, name, "%s should be at least %ld", name, min);
		}
		else if (value > max) {
			os_check_failed(db, name, "%s should be at most %ld", name, max);
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS, "error reading '%s'", path);
	}
}

//==========================================================
// Local helpers - read system files.
//

cf_os_file_res
cf_os_read_any_file(const char** paths, uint32_t n_paths, void* buf, size_t* limit)
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
			os_check_failed(db, "thp-enabled",
					"THP enabled not set to either 'never' or 'madvise'");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		cf_detail(CF_OS,
				"unable to find '/sys/kernel/mm/{redhat_,}transparent_hugepage/enabled'");
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS,
				"error reading '/sys/kernel/mm/{redhat_,}transparent_hugepage/enabled'");
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
		if (strstr(buf, "[never]") == NULL && strstr(buf, "[madvise]") == NULL) {
			os_check_failed(db, "thp-defrag",
					"THP defrag not set to either 'never' or 'madvise'");
		}
		break;
	case CF_OS_FILE_RES_NOT_FOUND:
		cf_detail(CF_OS,
				"unable to find '/sys/kernel/mm/{redhat_,}transparent_hugepage/defrag'");
		break;
	case CF_OS_FILE_RES_ERROR:
	default:
		cf_crash_nostack(CF_OS,
				"error reading '/sys/kernel/mm/{redhat_,}transparent_hugepage/defrag'");
	}
}
