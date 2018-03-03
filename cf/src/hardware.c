/*
 * hardware.c
 *
 * Copyright (C) 2016-2017 Aerospike, Inc.
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

#include "hardware.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syscall.h>
#include <unistd.h>

#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <linux/ethtool.h>
#include <linux/if.h>
#include <linux/limits.h>
#include <linux/mempolicy.h>
#include <linux/sockios.h>

#include "daemon.h"
#include "fault.h"
#include "socket.h"

#include "citrusleaf/alloc.h"

#include "warnings.h"

// Only available in Linux kernel version 3.19 and later; but we'd like to
// allow compilation with older kernel headers.
#if !defined SO_INCOMING_CPU
#define SO_INCOMING_CPU 49
#endif

#define INVALID_INDEX ((uint16_t)-1)
#define POLICY_SCRIPT "/etc/aerospike/irqbalance-ban.sh"

#define MEM_PAGE_SIZE (4096L)

typedef enum {
	FILE_RES_OK,
	FILE_RES_NOT_FOUND,
	FILE_RES_ERROR
} file_res;

typedef enum {
	CHECK_PROC_PRESENT,
	CHECK_PROC_PRESENT_NO_ARG,
	CHECK_PROC_ABSENT
} check_proc_res;

typedef uint16_t os_numa_node_index;
typedef uint16_t os_package_index;
typedef uint16_t os_core_index;

typedef uint16_t irq_number;

typedef struct {
	uint16_t n_irqs;
	irq_number irqs[CPU_SETSIZE];
	uint16_t per_cpu;
} irq_list;

static cpu_set_t g_os_cpus_online;
static cpu_set_t g_numa_node_os_cpus_online[CPU_SETSIZE];

static uint16_t g_n_numa_nodes;
static uint16_t g_n_cores;
static uint16_t g_n_os_cpus;
static uint16_t g_n_cpus;
static uint16_t g_n_irq_cpus;

static os_numa_node_index g_numa_node_index_to_os_numa_node_index[CPU_SETSIZE];
static cf_topo_os_cpu_index g_core_index_to_os_cpu_index[CPU_SETSIZE];
static cf_topo_os_cpu_index g_cpu_index_to_os_cpu_index[CPU_SETSIZE];
static cf_topo_cpu_index g_os_cpu_index_to_cpu_index[CPU_SETSIZE];

static cf_topo_numa_node_index g_i_numa_node;

static file_res
read_file(const char *path, void *buff, size_t *limit)
{
	cf_detail(CF_HARDWARE, "reading file %s with buffer size %zu", path, *limit);
	int32_t fd = open(path, O_RDONLY);

	if (fd < 0) {
		if (errno == ENOENT) {
			cf_detail(CF_HARDWARE, "file %s not found", path);
			return FILE_RES_NOT_FOUND;
		}

		cf_warning(CF_HARDWARE, "error while opening file %s for reading: %d (%s)",
				path, errno, cf_strerror(errno));
		return FILE_RES_ERROR;
	}

	size_t total = 0;

	while (total < *limit) {
		cf_detail(CF_HARDWARE, "reading %zd byte(s) at offset %zu", *limit - total, total);
		ssize_t len = read(fd, (uint8_t *)buff + total, *limit - total);
		CF_NEVER_FAILS(len);

		if (len == 0) {
			cf_detail(CF_HARDWARE, "EOF");
			break;
		}

		total += (size_t)len;
	}

	cf_detail(CF_HARDWARE, "read %zu byte(s) from file %s", total, path);
	file_res res;

	if (total == *limit) {
		cf_warning(CF_HARDWARE, "read buffer too small for file %s", path);
		res = FILE_RES_ERROR;
	}
	else {
		res = FILE_RES_OK;
		*limit = total;
	}

	CF_NEVER_FAILS(close(fd));
	return res;
}

static file_res
write_file(const char *path, const void *buff, size_t limit)
{
	cf_detail(CF_HARDWARE, "writing file %s with buffer size %zu", path, limit);
	int32_t fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

	if (fd < 0) {
		if (errno == ENOENT) {
			cf_detail(CF_HARDWARE, "file %s not found", path);
			return FILE_RES_NOT_FOUND;
		}

		cf_warning(CF_HARDWARE, "error while opening file %s for writing: %d (%s)",
				path, errno, cf_strerror(errno));
		return FILE_RES_ERROR;
	}

	size_t total = 0;

	while (total < limit) {
		cf_detail(CF_HARDWARE, "writing %zd byte(s) at offset %zu", limit - total, total);
		ssize_t len = write(fd, (uint8_t *)buff + total, limit - total);
		CF_NEVER_FAILS(len);
		total += (size_t)len;
	}

	cf_detail(CF_HARDWARE, "done writing");
	CF_NEVER_FAILS(close(fd));
	return FILE_RES_OK;
}

static void
write_file_safe(const char *path, const void *buff, size_t limit)
{
	if (write_file(path, buff, limit) != FILE_RES_OK) {
		cf_crash(CF_HARDWARE, "write failed unexpectedly");
	}
}

static DIR *
opendir_safe(const char *path)
{
	DIR *dir = opendir(path);

	if (dir == NULL) {
		cf_crash(CF_HARDWARE, "error while opening directory: %d (%s)",
				errno, cf_strerror(errno));
	}

	return dir;
}

static int32_t
readdir_safe(DIR *dir, struct dirent *ent)
{
	while (true) {
		errno = 0;
		struct dirent *tmp = readdir(dir);

		if (tmp == NULL) {
			if (errno != 0) {
				cf_crash(CF_HARDWARE, "error while reading directory: %d (%s)",
						errno, cf_strerror(errno));
			}

			return -1;
		}

		if (strcmp(tmp->d_name, ".") == 0 || strcmp(tmp->d_name, "..") == 0) {
			continue;
		}

		memcpy(ent, tmp, sizeof(struct dirent));
		return 0;
	}
}

static void
closedir_safe(DIR *dir)
{
	if (closedir(dir) < 0) {
		cf_crash(CF_HARDWARE, "error while closing PCI device directory: %d (%s)",
				errno, cf_strerror(errno));
	}
}

static bool
path_exists(const char *path)
{
	struct stat stat_info;

	if (stat(path, &stat_info) < 0) {
		if (errno == ENOENT) {
			return false;
		}

		cf_crash(CF_HARDWARE, "error while checking for path %s: %d (%s)",
				path, errno, cf_strerror(errno));
	}

	return true;
}

static void
set_mempolicy_safe(uint32_t mode, uint64_t *node_mask, size_t max_node)
{
	if (syscall(__NR_set_mempolicy, mode, node_mask, max_node) < 0) {
		cf_crash(CF_HARDWARE, "set_mempolicy() system call failed: %d (%s)",
				errno, cf_strerror(errno));
	}
}

static void
migrate_pages_safe(pid_t pid, size_t max_node, uint64_t *from_mask, uint64_t *to_mask)
{
	int64_t res = syscall(__NR_migrate_pages, pid, max_node, from_mask, to_mask);

	if (res < 0) {
		cf_crash(CF_HARDWARE, "migrate_pages() syscall failed: %d (%s)",
				errno, cf_strerror(errno));
	}

	if (res > 0) {
		cf_warning(CF_HARDWARE, "could not NUMA-migrate %" PRId64 " page(s)", res);
	}
}

static void
mask_to_string(cpu_set_t *mask, char *buff, size_t limit)
{
	cf_topo_os_cpu_index max;

	for (max = CPU_SETSIZE - 1; max > 0; --max) {
		if (CPU_ISSET(max, mask)) {
			break;
		}
	}

	int32_t words = max / 32 + 1;
	size_t size = (size_t)words * 9;

	if (size > limit) {
		cf_crash(CF_HARDWARE, "CPU mask buffer overflow: %zu vs. %zu", size, limit);
	}

	for (int32_t i = words - 1; i >= 0; --i) {
		uint32_t val = 0;

		for (int32_t k = 0; k < 32; ++k) {
			if (CPU_ISSET((size_t)(i * 32 + k), mask)) {
				val |= 1u << k;
			}
		}

		snprintf(buff, limit, "%08x", val);

		if (i > 0) {
			buff[8] = ',';
		}

		buff += 9;
		limit -= 9;
	}
}

static file_res
read_index(const char *path, uint16_t *val)
{
	cf_detail(CF_HARDWARE, "reading index from file %s", path);
	char buff[100];
	size_t limit = sizeof(buff);
	file_res res = read_file(path, buff, &limit);

	if (res != FILE_RES_OK) {
		return res;
	}

	buff[limit - 1] = '\0';
	cf_detail(CF_HARDWARE, "parsing index \"%s\"", buff);

	char *end;
	uint64_t x = strtoul(buff, &end, 10);

	if (*end != '\0' || x >= CPU_SETSIZE) {
		cf_warning(CF_HARDWARE, "invalid index \"%s\" in %s", buff, path);
		return FILE_RES_ERROR;
	}

	*val = (uint16_t)x;
	return FILE_RES_OK;
}

static file_res
read_list(const char *path, cpu_set_t *mask)
{
	cf_detail(CF_HARDWARE, "reading list from file %s", path);
	char buff[1000];
	size_t limit = sizeof(buff);
	file_res res = read_file(path, buff, &limit);

	if (res != FILE_RES_OK) {
		return res;
	}

	buff[limit - 1] = '\0';
	cf_detail(CF_HARDWARE, "parsing list \"%s\"", buff);

	CPU_ZERO(mask);
	char *walker = buff;

	while (true) {
		char *delim;
		uint64_t from = strtoul(walker, &delim, 10);
		uint64_t thru;

		if (*delim == ',' || *delim == '\0'){
			thru = from;
		}
		else if (*delim == '-') {
			walker = delim + 1;
			thru = strtoul(walker, &delim, 10);
		}
		else {
			cf_warning(CF_HARDWARE, "invalid list \"%s\" in %s", buff, path);
			return FILE_RES_ERROR;
		}

		if (from >= CPU_SETSIZE || thru >= CPU_SETSIZE || from > thru) {
			cf_warning(CF_HARDWARE, "invalid list \"%s\" in %s", buff, path);
			return FILE_RES_ERROR;
		}

		cf_detail(CF_HARDWARE, "marking %d through %d", (int32_t)from, (int32_t)thru);

		for (size_t i = from; i <= thru; ++i) {
			CPU_SET(i, mask);
		}

		if (*delim == '\0') {
			break;
		}

		walker = delim + 1;
	}

	char buff2[1000];
	mask_to_string(mask, buff2, sizeof(buff2));
	cf_detail(CF_HARDWARE, "list \"%s\" -> mask %s", buff, buff2);

	return FILE_RES_OK;
}

static void
detect(cf_topo_numa_node_index a_numa_node)
{
	if (a_numa_node == INVALID_INDEX) {
		cf_detail(CF_HARDWARE, "detecting online CPUs");
	}
	else {
		cf_detail(CF_HARDWARE, "detecting online CPUs on NUMA node %hu", a_numa_node);
	}

	if (read_list("/sys/devices/system/cpu/online", &g_os_cpus_online) != FILE_RES_OK) {
		cf_crash(CF_HARDWARE, "error while reading list of online CPUs");
	}

	cf_detail(CF_HARDWARE, "learning CPU topology");

	cf_topo_numa_node_index os_numa_node_index_to_numa_node_index[CPU_SETSIZE];

	for (int32_t i = 0; i < CPU_SETSIZE; ++i) {
		CPU_ZERO(&g_numa_node_os_cpus_online[i]);

		g_core_index_to_os_cpu_index[i] = INVALID_INDEX;
		g_cpu_index_to_os_cpu_index[i] = INVALID_INDEX;
		g_os_cpu_index_to_cpu_index[i] = INVALID_INDEX;

		os_numa_node_index_to_numa_node_index[i] = INVALID_INDEX;
		g_numa_node_index_to_os_numa_node_index[i] = INVALID_INDEX;
	}

	cpu_set_t covered_numa_nodes;
	cpu_set_t covered_cores[CPU_SETSIZE]; // One mask per package.

	CPU_ZERO(&covered_numa_nodes);

	for (int32_t i = 0; i < CPU_SETSIZE; ++i) {
		CPU_ZERO(&covered_cores[i]);
	}

	g_n_numa_nodes = 0;
	g_n_cores = 0;
	g_n_os_cpus = 0;
	g_n_cpus = 0;
	char path[1000];
	bool no_numa = false;

	// Loop through all CPUs in the system by looping through OS CPU indexes.

	for (g_n_os_cpus = 0; g_n_os_cpus < CPU_SETSIZE; ++g_n_os_cpus) {
		cf_detail(CF_HARDWARE, "querying OS CPU index %hu", g_n_os_cpus);

		// Let's look at the CPU's package.

		snprintf(path, sizeof(path),
				"/sys/devices/system/cpu/cpu%hu/topology/physical_package_id",
				g_n_os_cpus);
		os_package_index i_os_package;
		file_res res = read_index(path, &i_os_package);

		// The entry doesn't exist. We've processed all available CPUs. Stop
		// looping through the CPUs.

		if (res == FILE_RES_NOT_FOUND) {
			break;
		}

		if (res != FILE_RES_OK) {
			cf_crash(CF_HARDWARE, "error while reading OS package index from %s", path);
		}

		cf_detail(CF_HARDWARE, "OS package index is %hu", i_os_package);

		// Only consider CPUs that are actually in use.

		if (!CPU_ISSET(g_n_os_cpus, &g_os_cpus_online)) {
			cf_detail(CF_HARDWARE, "OS CPU index %hu is offline", g_n_os_cpus);
			continue;
		}

		// Let's look at the CPU's underlying core. In Hyper Threading systems,
		// two (logical) CPUs share one (physical) core.

		snprintf(path, sizeof(path),
				"/sys/devices/system/cpu/cpu%hu/topology/core_id",
				g_n_os_cpus);
		os_core_index i_os_core;
		res = read_index(path, &i_os_core);

		if (res != FILE_RES_OK) {
			cf_crash(CF_HARDWARE, "error while reading OS core index from %s", path);
		}

		cf_detail(CF_HARDWARE, "OS core index is %hu", i_os_core);

		// Consider a core when we see it for the first time. In other words, we
		// consider the first Hyper Threading peer of each core to be that core.

		bool new_core;

		if (CPU_ISSET(i_os_core, &covered_cores[i_os_package])) {
			cf_detail(CF_HARDWARE, "core (%hu, %hu) already covered", i_os_core, i_os_package);
			new_core = false;
		}
		else {
			cf_detail(CF_HARDWARE, "core (%hu, %hu) is new", i_os_core, i_os_package);
			new_core = true;
			CPU_SET(i_os_core, &covered_cores[i_os_package]);
		}

		// Identify the NUMA node of the current CPU. We simply look for the
		// current CPU's topology info subtree in each NUMA node's subtree.
		// Specifically, we look for the current CPU's "core_id" entry.

		os_numa_node_index i_os_numa_node;

		for (i_os_numa_node = 0; i_os_numa_node < CPU_SETSIZE; ++i_os_numa_node) {
			snprintf(path, sizeof(path),
					"/sys/devices/system/cpu/cpu%hu/node%hu/cpu%hu/topology/core_id",
					g_n_os_cpus, i_os_numa_node, g_n_os_cpus);
			uint16_t dummy;
			res = read_index(path, &dummy);

			// We found the NUMA node that has the current CPU in its subtree.

			if (res == FILE_RES_OK) {
				break;
			}

			if (res != FILE_RES_NOT_FOUND) {
				cf_crash(CF_HARDWARE, "error while reading core number from %s", path);
			}
		}

		// Some Docker installations seem to not have any NUMA information
		// in /sys. In this case, assume a system with a single NUMA node.

		if (i_os_numa_node == CPU_SETSIZE) {
			cf_detail(CF_HARDWARE, "OS CPU index %hu does not have a NUMA node", g_n_os_cpus);
			no_numa = true;
			i_os_numa_node = 0;
		}

		cf_detail(CF_HARDWARE, "OS NUMA node index is %hu", i_os_numa_node);

		// Again, just like with cores, we consider a NUMA node when we encounter
		// it for the first time.

		bool new_numa_node;

		if (CPU_ISSET(i_os_numa_node, &covered_numa_nodes)) {
			cf_detail(CF_HARDWARE, "OS NUMA node index %hu already covered", i_os_numa_node);
			new_numa_node = false;
		}
		else {
			cf_detail(CF_HARDWARE, "OS NUMA node index %hu is new", i_os_numa_node);
			new_numa_node = true;
			CPU_SET(i_os_numa_node, &covered_numa_nodes);

			// For now, we only support a 64-bit bitmask (= one uint64_t).

			if (i_os_numa_node >= 64) {
				cf_crash(CF_HARDWARE, "OS NUMA node index %hu too high", i_os_numa_node);
			}
		}

		// Now we know that the CPU is online and we know, whether it is in a newly
		// seen core (new_core) and/or a newly seen NUMA node (new_numa_node).

		cf_topo_numa_node_index i_numa_node;

		if (new_numa_node) {
			i_numa_node = g_n_numa_nodes;
			++g_n_numa_nodes;
			os_numa_node_index_to_numa_node_index[i_os_numa_node] = i_numa_node;
			g_numa_node_index_to_os_numa_node_index[i_numa_node] = i_os_numa_node;
			cf_detail(CF_HARDWARE, "OS NUMA node index %hu -> new NUMA node index %hu",
					i_os_numa_node, i_numa_node);
		}
		else {
			i_numa_node = os_numa_node_index_to_numa_node_index[i_os_numa_node];
			cf_detail(CF_HARDWARE, "OS NUMA node index %hu -> existing NUMA node index %hu",
					i_os_numa_node, i_numa_node);
		}

		cf_detail(CF_HARDWARE, "OS CPU index %hu on NUMA node index %hu", g_n_os_cpus, i_numa_node);
		CPU_SET(g_n_os_cpus, &g_numa_node_os_cpus_online[i_numa_node]);

		// If we're in NUMA mode and the CPU isn't on the NUMA mode that we're
		// running on, then ignore the CPU.

		if (a_numa_node != INVALID_INDEX && a_numa_node != i_numa_node) {
			cf_detail(CF_HARDWARE, "skipping unwanted NUMA node index %hu", i_numa_node);
			continue;
		}

		// If the CPU is a new core, then map a new core index to the OS CPU index.

		if (new_core) {
			g_core_index_to_os_cpu_index[g_n_cores] = g_n_os_cpus;
			cf_detail(CF_HARDWARE, "core index %hu -> OS CPU index %hu", g_n_cores, g_n_os_cpus);
			++g_n_cores;
		}

		// Map the OS CPU index to a new CPU index and vice versa.

		g_os_cpu_index_to_cpu_index[g_n_os_cpus] = g_n_cpus;
		g_cpu_index_to_os_cpu_index[g_n_cpus] = g_n_os_cpus;

		cf_detail(CF_HARDWARE, "OS CPU index %hu <-> CPU index %hu", g_n_os_cpus, g_n_cpus);
		++g_n_cpus;
	}

	if (g_n_os_cpus == CPU_SETSIZE) {
		cf_crash(CF_HARDWARE, "too many CPUs");
	}

	if (a_numa_node != INVALID_INDEX && no_numa) {
		cf_warning(CF_HARDWARE, "no NUMA information found in /sys");
	}

	g_i_numa_node = a_numa_node;
}

static void
pin_to_numa_node(cf_topo_numa_node_index a_numa_node)
{
	cf_info(CF_HARDWARE, "pinning to NUMA node %hu", a_numa_node);

	// Move the current thread (and all of its future descendants) to the CPUs
	// on the selected NUMA node.

	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);

	for (cf_topo_cpu_index i_cpu = 0; i_cpu < g_n_cpus; ++i_cpu) {
		cf_topo_os_cpu_index i_os_cpu = g_cpu_index_to_os_cpu_index[i_cpu];
		CPU_SET(i_os_cpu, &cpu_set);
	}

	char buff[1000];
	mask_to_string(&cpu_set, buff, sizeof(buff));
	cf_detail(CF_HARDWARE, "NUMA node %hu CPU mask: %s", a_numa_node, buff);

	if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) < 0) {
		cf_crash(CF_HARDWARE, "error while pinning thread to NUMA node %hu: %d (%s)",
				a_numa_node, errno, cf_strerror(errno));
	}

	// Force future memory allocations to the selected NUMA node.

	os_numa_node_index i_os_numa_node = g_numa_node_index_to_os_numa_node_index[a_numa_node];
	uint64_t to_mask = 1UL << i_os_numa_node;
	cf_detail(CF_HARDWARE, "NUMA node mask (to): %016" PRIx64, to_mask);

	// Unlike select(), we have to pass "number of valid bits + 1".
	set_mempolicy_safe(MPOL_BIND, &to_mask, 65);

	// Make sure we can migrate shared memory that we later attach and map.
	cf_process_holdcap();
}

static uint32_t
pick_random(uint32_t limit)
{
	static __thread uint64_t state = 0;

	if (state == 0) {
		state = (uint64_t)syscall(SYS_gettid);
	}

	state = state * 6364136223846793005 + 1;

	if (state == 0) {
		state = 1;
	}

	return (uint32_t)((state >> 32) % limit);
}

uint16_t
cf_topo_count_cores(void)
{
	return g_n_cores;
}

uint16_t
cf_topo_count_cpus(void)
{
	return g_n_cpus;
}

static cf_topo_cpu_index
os_cpu_index_to_cpu_index(cf_topo_os_cpu_index i_os_cpu)
{
	cf_detail(CF_HARDWARE, "translating OS CPU index %hu", i_os_cpu);

	if (i_os_cpu >= g_n_os_cpus) {
		cf_crash(CF_HARDWARE, "invalid OS CPU index %hu", i_os_cpu);
	}

	cf_topo_cpu_index i_cpu = g_os_cpu_index_to_cpu_index[i_os_cpu];

	if (i_cpu == INVALID_INDEX) {
		cf_detail(CF_HARDWARE, "foreign OS CPU index %hu", i_os_cpu);
	}
	else {
		cf_detail(CF_HARDWARE, "CPU index is %hu", i_cpu);
	}

	return i_cpu;
}

cf_topo_cpu_index
cf_topo_current_cpu(void)
{
	cf_detail(CF_HARDWARE, "getting current OS CPU index");
	int32_t os = sched_getcpu();

	if (os < 0) {
		cf_crash(CF_HARDWARE, "error while getting OS CPU index: %d (%s)",
				errno, cf_strerror(errno));
	}

	return os_cpu_index_to_cpu_index((cf_topo_os_cpu_index)os);
}

cf_topo_cpu_index
cf_topo_socket_cpu(const cf_socket *sock)
{
	cf_detail(CF_HARDWARE, "determining CPU index for socket FD %d", CSFD(sock));

	int32_t os;
	socklen_t len = sizeof(os);

	if (getsockopt(sock->fd, SOL_SOCKET, SO_INCOMING_CPU, &os, &len) < 0) {
		cf_crash(CF_SOCKET, "error while determining incoming OS CPU index: %d (%s)",
				errno, cf_strerror(errno));
	}

	cf_detail(CF_HARDWARE, "OS CPU index is %d", os);
	cf_topo_cpu_index i_cpu = os_cpu_index_to_cpu_index((cf_topo_os_cpu_index)os);

	// 1. The incoming connection was handled on the wrong NUMA node. In this case,
	// pick a random CPU on the correct NUMA node.

	if (i_cpu == INVALID_INDEX) {
		i_cpu = (cf_topo_cpu_index)pick_random(g_n_cpus);
		cf_detail(CF_HARDWARE, "picking random CPU index %hu", i_cpu);
		return i_cpu;
	}

	// 2. The incoming connection was handled on a CPU that doesn't get any NIC
	// interrupts. This should not happen for connections from other machines, but
	// it does happen for connections from the local machine, because they don't
	// go through the NIC hardware. In this case, pick a random CPU.

	if (i_cpu >= g_n_irq_cpus) {
		i_cpu = (cf_topo_cpu_index)pick_random(g_n_cpus);
		cf_detail(CF_HARDWARE, "randomizing unexpected CPU index >%hu to %hu",
				g_n_irq_cpus - 1, i_cpu);
		return i_cpu;
	}

	// 3. Otherwise, redistribute. The first g_n_irq_cpus CPUs out of a total of
	// g_n_cpus CPUs get NIC interrupts. Suppose we have 2 NIC queues and 8 CPUs,
	// i.e., that g_n_irq_cpus == 2 and g_n_cpus == 8. We want to redistribute
	// evenly across the 8 CPUs, i.e., each CPU should be picked with a probability
	// of 0.125.

	// We're currently running on one of the 2 CPUs that get NIC interrupts, on
	// either with a probability of p1 = 0.5. We want to stay on the current CPU
	// with a probability of p2 = g_n_irq_cpus / g_n_cpus == 2 / 8 == 0.25, which
	// yields the desired total probability of p1 * p2 = 0.5 * 0.25 = 0.125.

	if (pick_random(100000) < g_n_irq_cpus * (uint32_t)100000 / g_n_cpus) {
		cf_detail(CF_HARDWARE, "staying on CPU index %hu", i_cpu);
		return i_cpu;
	}

	// 4. Otherwise, if we switch CPUs, then we jump to a CPU that doesn't receive
	// NIC interrupts, i.e., one of the remaining 6 CPUs [2 .. 8] in our example.
	// This reaches each CPU with a probability of (1 - p2) / 6 = 0.125.

	i_cpu = (cf_topo_cpu_index)(g_n_irq_cpus +
			pick_random((uint32_t)g_n_cpus - (uint32_t)g_n_irq_cpus));
	cf_detail(CF_HARDWARE, "redirecting to CPU index %hu", i_cpu);
	return i_cpu;
}

static void
pin_to_os_cpu(cf_topo_os_cpu_index i_os_cpu)
{
	cf_detail(CF_HARDWARE, "pinning to OS CPU index %hu", i_os_cpu);

	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(i_os_cpu, &cpu_set);

	if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) < 0) {
		cf_crash(CF_HARDWARE, "error while pinning thread to OS CPU %hu: %d (%s)",
				i_os_cpu, errno, cf_strerror(errno));
	}
}

void
cf_topo_pin_to_core(cf_topo_core_index i_core)
{
	cf_detail(CF_HARDWARE, "pinning to core index %hu", i_core);

	if (i_core >= g_n_cores) {
		cf_crash(CF_HARDWARE, "invalid core index %hu", i_core);
	}

	pin_to_os_cpu(g_core_index_to_os_cpu_index[i_core]);
}

void
cf_topo_pin_to_cpu(cf_topo_cpu_index i_cpu)
{
	cf_detail(CF_HARDWARE, "pinning to CPU index %hu", i_cpu);

	if (i_cpu >= g_n_cpus) {
		cf_crash(CF_HARDWARE, "invalid CPU index %hu", i_cpu);
	}

	pin_to_os_cpu(g_cpu_index_to_os_cpu_index[i_cpu]);
}

static check_proc_res
check_proc(const char *name, int32_t argc, const char *argv[])
{
	cf_detail(CF_HARDWARE, "looking for process %s", name);

	for (int32_t i = 0; i < argc; ++i) {
		cf_detail(CF_HARDWARE, "argv[%d]: %s", i, argv[i]);
	}

	DIR *dir = opendir_safe("/proc");
	struct dirent ent;
	char cmd[10000];
	size_t limit;
	bool found = false;

	while (readdir_safe(dir, &ent) >= 0) {
		bool numeric = true;

		for (int32_t i = 0; ent.d_name[i] != 0; ++i) {
			if (!isascii(ent.d_name[i]) || !isdigit(ent.d_name[i])) {
				numeric = false;
				break;
			}
		}

		if (!numeric) {
			continue;
		}

		char path[100];
		snprintf(path, sizeof(path), "/proc/%s/cmdline", ent.d_name);

		limit = sizeof(cmd) - 1;
		file_res rfr = read_file(path, cmd, &limit);

		// Can legitimately happen, if the process has exited in the meantime.
		if (rfr == FILE_RES_NOT_FOUND) {
			continue;
		}

		if (rfr == FILE_RES_ERROR) {
			cf_crash(CF_HARDWARE, "error while reading file %s", path);
		}

		if (limit > 0 && cmd[limit - 1] != 0) {
			cmd[limit] = 0;
		}

		const char *name2 = strrchr(cmd, '/');

		if (name2 != NULL) {
			++name2;
		}
		else {
			name2 = cmd;
		}

		if (strcmp(name2, name) == 0) {
			found = true;
			break;
		}
	}

	closedir_safe(dir);

	if (!found) {
		cf_detail(CF_HARDWARE, "process %s absent", name);
		return CHECK_PROC_ABSENT;
	}

	cf_detail(CF_HARDWARE, "process %s is %s", name, cmd);

	if (argc > 0) {
		int32_t i_arg = 0;

		for (size_t off = strlen(cmd) + 1; off < limit; off += strlen(cmd + off) + 1) {
			cf_detail(CF_HARDWARE, "checking argument %s against %s", cmd + off, argv[i_arg]);

			if (strcmp(cmd + off, argv[i_arg]) == 0) {
				++i_arg;

				if (i_arg >= argc) {
					break;
				}
			}
			else {
				i_arg = 0;
			}
		}

		if (i_arg >= argc) {
			cf_detail(CF_HARDWARE, "process %s present with argument", name);
			return CHECK_PROC_PRESENT;
		}
	}

	cf_detail(CF_HARDWARE, "process %s present", name);
	return CHECK_PROC_PRESENT_NO_ARG;
}

static uint16_t
interface_queues(const char *if_name, const char *format)
{
	uint16_t n_queues = 0;

	while (true) {
		char path[1000];
		snprintf(path, sizeof(path), format, if_name, n_queues);
		cf_detail(CF_HARDWARE, "checking for path %s", path);

		if (!path_exists(path)) {
			cf_detail(CF_HARDWARE, "path not found");
			break;
		}

		++n_queues;
	}

	cf_assert(n_queues != 0, CF_HARDWARE, "interface %s has no queues", if_name);

	return n_queues;
}

static uint16_t
interface_rx_queues(const char *if_name)
{
	cf_detail(CF_HARDWARE, "getting receive queues for interface %s", if_name);
	return interface_queues(if_name, "/sys/class/net/%s/queues/rx-%hu");
}

static uint16_t
interface_tx_queues(const char *if_name)
{
	cf_detail(CF_HARDWARE, "getting transmit queues for interface %s", if_name);
	return interface_queues(if_name, "/sys/class/net/%s/queues/tx-%hu");
}

static int
comp_irq_number(const void *lhs, const void *rhs)
{
	return *(irq_number *)lhs - *(irq_number *)rhs;
}

static void
interface_irqs(const char *if_name, irq_list *irqs)
{
	cf_detail(CF_HARDWARE, "getting IRQs for interface %s", if_name);

	DIR *dir = opendir_safe("/sys/bus/pci/devices");
	struct dirent ent;
	char path[PATH_MAX];
	bool found = false;

	while (readdir_safe(dir, &ent) >= 0) {
		snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/net/%s/ifindex",
				ent.d_name, if_name);
		bool exists = path_exists(path);

		if (!exists) {
			for (int32_t i = 0; i < 100; ++i) {
				snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/virtio%d/net/%s/ifindex",
						ent.d_name, i, if_name);
				exists = path_exists(path);

				if (exists) {
					break;
				}
			}
		}

		if (!exists) {
			continue;
		}

		snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/msi_irqs", ent.d_name);

		if (!path_exists(path)) {
			cf_crash(CF_HARDWARE, "interface %s does not support MSIs", if_name);
		}

		cf_detail(CF_HARDWARE, "interface %s is %s", if_name, ent.d_name);
		found = true;
		break;
	}

	closedir_safe(dir);

	if (!found) {
		cf_crash(CF_HARDWARE, "interface %s does not have a PCI device entry", if_name);
	}

	dir = opendir_safe(path);
	int32_t count = 0;
	irq_number irq_nums[CPU_SETSIZE];

	while (readdir_safe(dir, &ent) >= 0) {
		char *end;
		uint64_t tmp = strtoul(ent.d_name, &end, 10);

		if (*end != 0 || tmp > 65535) {
			cf_crash(CF_HARDWARE, "invalid IRQ number %s in %s", ent.d_name, path);
		}

		if (count >= CPU_SETSIZE) {
			cf_crash(CF_HARDWARE, "too many IRQs in %s", path);
		}

		cf_detail(CF_HARDWARE, "interface %s has IRQ %hu", if_name, (irq_number)tmp);
		irq_nums[count] = (irq_number)tmp;
		++count;
	}

	closedir_safe(dir);

	// Sort IRQ numbers, so that RX and TX interrupts pair up nicely when
	// populating irqs->irqs[].
	qsort(irq_nums, (size_t)count, sizeof(irq_number), comp_irq_number);

	char actions[count][100];
	memset(actions, 0, sizeof(actions));

	FILE *fh = fopen("/proc/interrupts", "r");

	if (fh == NULL) {
		cf_crash(CF_HARDWARE, "error while opening /proc/interrupts");
	}

	int32_t line_no = 0;
	char line[25000];

	while (fgets(line, sizeof(line), fh) != NULL) {
		++line_no;

		if (line_no == 1) {
			continue;
		}

		int32_t i = 0;

		while (line[i] == ' ') {
			++i;
		}

		irq_number irq_num = 0;

		while (line[i] >= '0' && line[i] <= '9') {
			irq_num = (irq_number)(irq_num * 10 + line[i] - '0');
			++i;
		}

		if (line[i] != ':') {
			continue;
		}

		while (line[i] != 0 && line[i] != '\n') {
			++i;
		}

		line[i] = 0;

		while (i >= 0 && line[i] != ' ') {
			--i;
		}

		char *action = line + i + 1;

		if (strlen(action) >= sizeof(actions[0])) {
			cf_crash(CF_HARDWARE, "oversize action in line %d in /proc/interrupts: %s",
					line_no, action);
		}

		cf_detail(CF_HARDWARE, "IRQ %hu has action %s", irq_num, action);

		for (i = 0; i < count; ++i) {
			if (irq_nums[i] == irq_num) {
				int32_t m = 0;

				// Remove any digits, so that the queue index goes away and all queues
				// look alike. Also, normalize to lower case. For example:
				//
				//   "i40e-em1-TxRx-0" -> "ie-em-txrx-"
				//   "i40e-em1-TxRx-1" -> "ie-em-txrx-"
				//   ...

				for (int32_t k = 0; action[k] != 0; ++k) {
					if (action[k] < '0' || action[k] > '9') {
						actions[i][m] = (char)tolower((uint8_t)action[k]);
						++m;
					}
				}

				actions[i][m] = 0;
				cf_detail(CF_HARDWARE, "action pattern is %s", actions[i]);
				break;
			}
		}
	}

	fclose(fh);

	int32_t n_groups = 0;
	int32_t group_sizes[count];
	int32_t group_extra[count];
	int32_t action_groups[count];
	int32_t inactive_group = -1;

	for (int32_t i = 0; i < count; ++i) {
		group_sizes[i] = 0;
		group_extra[i] = 0;
		action_groups[i] = -1;
	}

	// Group by action pattern.

	for (int32_t i = 0; i < count; ++i) {
		if (action_groups[i] >= 0) {
			continue;
		}

		action_groups[i] = n_groups;
		++group_sizes[n_groups];

		if (actions[i][0] == 0) {
			inactive_group = n_groups;
			cf_detail(CF_HARDWARE, "inactive IRQs in new group %d", n_groups);
		}
		else {
			cf_detail(CF_HARDWARE, "new group %d: %s", n_groups, actions[i]);
		}

		for (int32_t k = i + 1; k < count; ++k) {
			if (strcmp(actions[i], actions[k]) == 0) {
				action_groups[k] = n_groups;
				++group_sizes[n_groups];
			}
		}

		cf_detail(CF_HARDWARE, "group %d has %d member(s)", n_groups, group_sizes[n_groups]);

		// Prefer groups whose action patterns have "rx", "tx", "input", or "output" in them.

		if (strstr(actions[i], "rx") != NULL || strstr(actions[i], "tx") != NULL ||
				strstr(actions[i], "input") != NULL || strstr(actions[i], "output") != NULL) {
			cf_detail(CF_HARDWARE, "preferring group %d", n_groups);
			group_extra[n_groups] = 1;
		}

		++n_groups;
	}

	// Find the two largest groups.

	int32_t a = -1;
	int32_t b = -1;

	for (int32_t i = 0; i < n_groups; ++i) {
		if (i != inactive_group &&
				(a < 0 || group_sizes[i] + group_extra[i] > group_sizes[a] + group_extra[a])) {
			a = i;
		}
	}

	if (a < 0) {
		cf_crash(CF_HARDWARE, "no active interrupts for interface %s", if_name);
	}

	for (int32_t i = 0; i < n_groups; ++i) {
		if (i != inactive_group && i != a &&
				(b < 0 || group_sizes[i] + group_extra[i] > group_sizes[b] + group_extra[b])) {
			b = i;
		}
	}

	cf_detail(CF_HARDWARE, "largest groups: %d, %d", a, b);

	// If the two largest groups have an equal number of members, then we assume
	// that it's a NIC with separate RX and TX queue IRQs.

	if (b >= 0 && group_sizes[a] == group_sizes[b]) {
		cf_detail(CF_HARDWARE, "assuming %d separate RX and TX queue IRQ(s)",
				group_sizes[a] + group_sizes[b]);
		int32_t ia = 0;
		int32_t ib = 0;

		// Make RX and TX queue IRQs take turns in the IRQ list.

		for (int32_t k = 0; k < count; ++k) {
			if (action_groups[k] == a) {
				irqs->irqs[ia * 2] = irq_nums[k];
				cf_detail(CF_HARDWARE, "irqs[%d] = %hu", ia * 2, irq_nums[k]);
				++ia;
			}
			else if (action_groups[k] == b) {
				irqs->irqs[ib * 2 + 1] = irq_nums[k];
				cf_detail(CF_HARDWARE, "irqs[%d] = %hu", ib * 2 + 1, irq_nums[k]);
				++ib;
			}
		}

		irqs->n_irqs = (uint16_t)(group_sizes[a] + group_sizes[b]);

		// Send pairs of two consecutive IRQs in the IRQ list (= the RX and the
		// TX queue IRQ of a given NIC queue pair) to the same CPU.

		irqs->per_cpu = 2;
		return;
	}

	// Otherwise, we assume that it's a NIC with combined RX and TX queue IRQs
	// and that the largest group contains these IRQs.

	cf_detail(CF_HARDWARE, "assuming %d combined RX and TX queue IRQ(s)", group_sizes[a]);
	int32_t ia = 0;

	for (int32_t k = 0; k < count; ++k) {
		if (action_groups[k] == a) {
			irqs->irqs[ia] = irq_nums[k];
			cf_detail(CF_HARDWARE, "irqs[%d] = %hu", ia, irq_nums[k]);
			++ia;
		}
	}

	irqs->n_irqs = (uint16_t)group_sizes[a];

	// Send each IRQ in the IRQ list to a different CPU.

	irqs->per_cpu = 1;
}

static void
pin_irq(irq_number i_irq, cf_topo_os_cpu_index i_os_cpu)
{
	cf_detail(CF_HARDWARE, "pinning IRQ number %hu to OS CPU index %hu", i_irq, i_os_cpu);

	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(i_os_cpu, &mask);

	char mask_str[200];
	mask_to_string(&mask, mask_str, sizeof(mask_str));
	cf_detail(CF_HARDWARE, "CPU mask is %s", mask_str);

	char path[1000];
	snprintf(path, sizeof(path), "/proc/irq/%hu/smp_affinity", i_irq);

	if (write_file(path, mask_str, strlen(mask_str)) != FILE_RES_OK) {
		cf_crash(CF_HARDWARE, "error while pinning IRQ, path %s", path);
	}
}

static cf_topo_os_cpu_index
fix_os_cpu_index(cf_topo_os_cpu_index i_os_cpu, const cpu_set_t *online)
{
	while (true) {
		if (i_os_cpu >= g_n_os_cpus) {
			i_os_cpu = 0;
		}

		if (CPU_ISSET(i_os_cpu, online)) {
			return i_os_cpu;
		}

		++i_os_cpu;
	}
}

static void
config_steering(const char *format, const char *if_name, uint16_t n_queues, bool enable)
{
	uint16_t i_queue;
	cpu_set_t masks[n_queues];

	for (i_queue = 0; i_queue < n_queues; ++i_queue) {
		CPU_ZERO(&masks[i_queue]);
	}

	if (enable) {
		i_queue = 0;

		for (cf_topo_os_cpu_index i_os_cpu = 0; i_os_cpu < g_n_os_cpus; ++i_os_cpu) {
			if (CPU_ISSET(i_os_cpu, &g_os_cpus_online)) {
				CPU_SET(i_os_cpu, &masks[i_queue % n_queues]);
				++i_queue;
			}
		}
	}

	for (i_queue = 0; i_queue < n_queues; ++i_queue) {
		char path[1000];
		snprintf(path, sizeof(path), format, if_name, i_queue);
		cf_detail(CF_HARDWARE, "path is %s", path);

		char mask_str[200];
		mask_to_string(&masks[i_queue], mask_str, sizeof(mask_str));
		cf_detail(CF_HARDWARE, "CPU mask is %s", mask_str);

		write_file_safe(path, mask_str, strlen(mask_str));
	}
}

static void
enable_xps(const char *if_name)
{
	cf_detail(CF_HARDWARE, "enabling XPS for interface %s", if_name);
	uint16_t n_queues = interface_tx_queues(if_name);
	config_steering("/sys/class/net/%s/queues/tx-%hu/xps_cpus", if_name, n_queues, true);
}

static void
disable_rps(const char *if_name)
{
	cf_detail(CF_HARDWARE, "disabling RPS for interface %s", if_name);
	uint16_t n_queues = interface_rx_queues(if_name);
	config_steering("/sys/class/net/%s/queues/rx-%hu/rps_cpus", if_name, n_queues, false);
}

static void
config_rfs(const char *if_name, bool enable)
{
	cf_detail(CF_HARDWARE, "%s RFS for interface %s", enable ? "enabling" : "disabling", if_name);

	uint16_t n_queues = interface_rx_queues(if_name);
	uint32_t sz_glob = enable ? 1000000 : 0;
	uint32_t sz_queue = sz_glob / n_queues;

	cf_detail(CF_HARDWARE, "global size is %u, per-queue size is %u", sz_glob, sz_queue);

	char string[50];
	snprintf(string, sizeof(string), "%u", sz_glob);
	write_file_safe("/proc/sys/net/core/rps_sock_flow_entries", string, strlen(string));

	snprintf(string, sizeof(string), "%u", sz_queue);

	for (uint16_t i_queue = 0; i_queue < n_queues; ++i_queue) {
		char path[1000];
		snprintf(path, sizeof(path), "/sys/class/net/%s/queues/rx-%hu/rps_flow_cnt",
				if_name, i_queue);
		write_file_safe(path, string, strlen(string));
	}
}

static void
enable_coalescing(const char *if_name)
{
	cf_detail(CF_HARDWARE, "enabling interrupt coalescing for interface %s", if_name);
	int32_t sock = socket(AF_INET, SOCK_DGRAM, 0);

	if (sock < 0) {
		cf_crash(CF_HARDWARE, "error while create ethtool socket: %d (%s)", errno, cf_strerror(errno));
	}

	struct ifreq req;
	memset(&req, 0, sizeof(req));

	if (strlen(if_name) > IFNAMSIZ - 1) {
		cf_crash(CF_HARDWARE, "invalid interface name %s", if_name);
	}

	strcpy(req.ifr_name, if_name);
	struct ethtool_coalesce coal = { .cmd = ETHTOOL_GCOALESCE };
	req.ifr_data = &coal;

	if (ioctl(sock, SIOCETHTOOL, &req) < 0) {
		if (errno == EOPNOTSUPP) {
			cf_detail(CF_HARDWARE, "interface %s does not support ETHTOOL_GCOALESCE", if_name);
			goto cleanup1;
		}

		cf_crash(CF_HARDWARE, "error while getting interface settings: %d (%s)",
				errno, cf_strerror(errno));
	}

	cf_detail(CF_HARDWARE, "current interface settings: adaptive = %u, usecs = %u",
			coal.use_adaptive_rx_coalesce, coal.rx_coalesce_usecs);

	if (coal.use_adaptive_rx_coalesce != 0 || coal.rx_coalesce_usecs >= 100) {
		cf_detail(CF_HARDWARE, "leaving interface settings untouched");
		goto cleanup1;
	}

	cf_detail(CF_HARDWARE, "adjusting interface settings");
	coal = (struct ethtool_coalesce){
		.cmd = ETHTOOL_SCOALESCE,
		.rx_coalesce_usecs = 100 // .1 ms for now, which adds .05 ms to a request on average.
	};

	if (ioctl(sock, SIOCETHTOOL, &req) < 0) {
		if (errno == EOPNOTSUPP) {
			cf_detail(CF_HARDWARE, "interface %s does not support ETHTOOL_SCOALESCE", if_name);
			goto cleanup1;
		}

		cf_crash(CF_HARDWARE, "error while adjusting interface settings: %d (%s)",
				errno, cf_strerror(errno));
	}

cleanup1:
	CF_NEVER_FAILS(close(sock));
}

static void
check_irqbalance(void)
{
	cf_detail(CF_HARDWARE, "checking irqbalance");

	check_proc_res res = check_proc("irqbalance", 1, (const char *[]){
		"--policyscript=" POLICY_SCRIPT
	});

	if (res == CHECK_PROC_PRESENT_NO_ARG) {
		res = check_proc("irqbalance", 2, (const char *[]){
			"--policyscript",
			POLICY_SCRIPT
		});
	}

	if (res == CHECK_PROC_PRESENT_NO_ARG) {
		res = check_proc("irqbalance", 1, (const char *[]){
			"-l" POLICY_SCRIPT
		});
	}

	if (res == CHECK_PROC_PRESENT_NO_ARG) {
		res = check_proc("irqbalance", 2, (const char *[]){
			"-l",
			POLICY_SCRIPT
		});
	}

	if (res == CHECK_PROC_PRESENT_NO_ARG) {
		cf_crash_nostack(CF_HARDWARE, "please disable irqbalance or run it with the Aerospike policy script, /etc/aerospike/irqbalance-ban.sh");
	}
}

static void
config_interface(const char *if_name, bool rfs, irq_list *irqs)
{
	uint16_t n_irq_cpus = 0;
	cf_topo_os_cpu_index i_os_cpu = fix_os_cpu_index(0, &g_os_cpus_online);

	for (uint16_t i = 0; i < irqs->n_irqs; ++i) {
		pin_irq(irqs->irqs[i], i_os_cpu);

		if (i % irqs->per_cpu == irqs->per_cpu - 1) {
			++n_irq_cpus;
			i_os_cpu = fix_os_cpu_index((cf_topo_os_cpu_index)(i_os_cpu + 1), &g_os_cpus_online);
		}
	}

	cf_detail(CF_HARDWARE, "interface %s with %hu RX interrupt(s)", if_name, n_irq_cpus);

	if (g_n_irq_cpus == 0) {
		g_n_irq_cpus = n_irq_cpus;
	}
	else if (n_irq_cpus != g_n_irq_cpus) {
		cf_crash(CF_HARDWARE, "interface %s with inconsistent number of RX interrupts: %hu vs. %hu",
				if_name, n_irq_cpus, g_n_irq_cpus);
	}

	disable_rps(if_name);
	config_rfs(if_name, rfs);
	enable_xps(if_name);

	// Redistributing packets with RFS causes inter-CPU interrupts, which increases
	// the interrupt load on the machine. For low-end systems, make sure that
	// interrupt coalescing is enabled.
	//
	// We consider a machine low-end, if we handle interrupts on 25% or less of the
	// available CPUs (i.e., if the number of NIC queues is 25% or less of the number
	// of available CPUs) and it has fewer than 4 NIC queues.
	//
	// Better (i.e., NUMA) machines typically come with adaptive interrupt coalescing
	// enabled by default. That's why we only do this here and not in the NUMA case.

	if (rfs && n_irq_cpus <= g_n_cpus / 4 && n_irq_cpus < 4) {
		enable_coalescing(if_name);
	}
}

static void
config_interface_numa(const char *if_name, irq_list *irqs)
{
	uint16_t n_irq_cpus = 0;
	cf_topo_os_cpu_index i_os_cpu[g_n_numa_nodes];
	uint16_t i_numa_node;

	for (i_numa_node = 0; i_numa_node < g_n_numa_nodes; ++i_numa_node) {
		i_os_cpu[i_numa_node] = fix_os_cpu_index(0, &g_numa_node_os_cpus_online[i_numa_node]);
	}

	i_numa_node = 0;

	// This configures the IRQs for all NUMA nodes. If multiple asd processes are
	// running, each process does this, but each does it identically. Hence there
	// isn't any conflict.

	for (uint16_t i = 0; i < irqs->n_irqs; ++i) {
		char mask_str[200];
		mask_to_string(&g_numa_node_os_cpus_online[i_numa_node], mask_str, sizeof(mask_str));
		cf_detail(CF_HARDWARE, "NUMA node index %hu CPU mask is %s", i_numa_node, mask_str);

		pin_irq(irqs->irqs[i], i_os_cpu[i_numa_node]);

		if (i % irqs->per_cpu == irqs->per_cpu - 1) {
			// Only count CPUs on our NUMA node.

			if (i_numa_node == g_i_numa_node) {
				++n_irq_cpus;
			}

			i_os_cpu[i_numa_node] =
					fix_os_cpu_index((cf_topo_os_cpu_index)(i_os_cpu[i_numa_node] + 1),
					&g_numa_node_os_cpus_online[i_numa_node]);
			i_numa_node = (uint16_t)((i_numa_node + 1) % g_n_numa_nodes);
		}
	}

	cf_detail(CF_HARDWARE, "interface %s with %hu RX interrupt(s) on NUMA node %hu",
			if_name, n_irq_cpus, g_i_numa_node);

	if (g_n_irq_cpus == 0) {
		g_n_irq_cpus = n_irq_cpus;
	}
	else if (n_irq_cpus != g_n_irq_cpus) {
		cf_crash(CF_HARDWARE, "interface %s with inconsistent number of RX interrupts: %hu vs. %hu",
				if_name, n_irq_cpus, g_n_irq_cpus);
	}

	disable_rps(if_name);
	config_rfs(if_name, true);
	enable_xps(if_name);
}

static void
optimize_interface(const char *if_name)
{
	cf_detail(CF_HARDWARE, "optimizing interface %s", if_name);
	uint16_t n_queues = interface_rx_queues(if_name);
	irq_list irqs;
	interface_irqs(if_name, &irqs);

	cf_info(CF_HARDWARE, "detected %hu NIC receive queue(s), %hu interrupt(s) for %s",
			n_queues, irqs.n_irqs, if_name);

	// We either expect one interrupt per RX queue (shared with TX) or two
	// interrupts per RX queue (one RX, one TX).

	uint16_t n_irq_cpus = irqs.n_irqs / irqs.per_cpu;

	if (n_irq_cpus != n_queues) {
		cf_crash(CF_HARDWARE, "suspicious NIC interrupt count %hu with %hu NIC receive queue(s)",
				irqs.n_irqs, n_queues);
	}

	if (n_irq_cpus == g_n_cpus) {
		if (g_i_numa_node != INVALID_INDEX) {
			cf_detail(CF_HARDWARE, "setting up for a fancy interface with NUMA");
			config_interface_numa(if_name, &irqs);
		}
		else {
			cf_detail(CF_HARDWARE, "setting up for a fancy interface, no NUMA");
			config_interface(if_name, false, &irqs);
		}
	}
	else {
		if (n_irq_cpus <= g_n_cpus / 4) {
			cf_warning(CF_HARDWARE, "%s has very few NIC queues; only %hu out of %hu CPUs handle(s) NIC interrupts",
					if_name, n_irq_cpus, g_n_cpus);
		}

		if (g_i_numa_node != INVALID_INDEX) {
			cf_detail(CF_HARDWARE, "setting up for a lame interface with NUMA");
			config_interface_numa(if_name, &irqs);
		}
		else {
			cf_detail(CF_HARDWARE, "setting up for a lame interface, no NUMA");
			config_interface(if_name, true, &irqs);
		}
	}
}

static void
check_socket_cpu(void)
{
	int32_t fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

	if (fd < 0) {
		cf_crash(CF_SOCKET, "error while creating UDP test socket: %d (%s)",
				errno, cf_strerror(errno));
	}

	int32_t val = -1;

	if (setsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &val, sizeof(val)) < 0) {
		if (errno == ENOPROTOOPT) {
			cf_crash_nostack(CF_SOCKET, "CPU pinning requires Linux kernel 3.19 or later");
		}

		cf_crash(CF_SOCKET, "error while testing for SO_INCOMING_CPU: %d (%s)",
				errno, cf_strerror(errno));
	}

	CF_NEVER_FAILS(close(fd));
}

void
cf_topo_config(cf_topo_auto_pin auto_pin, cf_topo_numa_node_index a_numa_node,
		const cf_addr_list *addrs)
{
	// Detect the NUMA topology.

	switch (auto_pin) {
	case CF_TOPO_AUTO_PIN_NONE:
	case CF_TOPO_AUTO_PIN_CPU:
		detect(INVALID_INDEX);
		break;

	case CF_TOPO_AUTO_PIN_NUMA:
		detect(a_numa_node);

		// Clamp the given NUMA node index to the valid range. We can only do this
		// after we know what g_n_numa_nodes is, which is initialized by the above
		// call to detect().

		if (a_numa_node >= g_n_numa_nodes) {
			cf_topo_numa_node_index orig = a_numa_node;
			a_numa_node = (cf_topo_numa_node_index)(a_numa_node % g_n_numa_nodes);
			cf_detail(CF_HARDWARE, "invalid NUMA node index: %hu, clamping to %hu", orig, a_numa_node);
			detect(a_numa_node);
		}

		break;
	}

	// If we don't do any pinning, then we're done after NUMA topology detection.

	if (auto_pin == CF_TOPO_AUTO_PIN_NONE) {
		return;
	}

	// Make sure that we are running on Linux 3.19 or later.

	check_socket_cpu();

	// Reconfigure the client-facing network interface(s).

	check_irqbalance();

	if (addrs->n_addrs == 0) {
		cf_crash_nostack(CF_HARDWARE, "auto-pinning requires binding the service to one or more network interfaces");
	}

	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {
		const char *if_name = addrs->addrs[i];

		if (!cf_inter_is_inter_name(if_name)) {
			cf_crash_nostack(CF_HARDWARE, "auto-pinning requires binding the service to network interfaces; \"%s\" isn't a network interface",
					if_name);
		}

		char *exp_names[100];
		uint32_t n_exp = sizeof(exp_names) / sizeof(exp_names[0]);
		cf_inter_expand_bond(if_name, exp_names, &n_exp);

		for (uint32_t k = 0; k < n_exp; ++k) {
			optimize_interface(exp_names[k]);
			cf_free(exp_names[k]);
		}
	}

	// If we don't do NUMA pinning, then we're done after setting up the
	// client-facing network interface(s).

	if (auto_pin == CF_TOPO_AUTO_PIN_CPU) {
		return;
	}

	// NUMA pinning.

	pin_to_numa_node(a_numa_node);
}

void
cf_topo_force_map_memory(const uint8_t *from, size_t size)
{
	if (g_i_numa_node == INVALID_INDEX || size == 0) {
		return;
	}

	cf_assert(from, CF_HARDWARE, "invalid cf_topo_force_map_memory() call");

	// Read one byte per memory page to force otherwise lazy mapping.

	const uint8_t *start = (const uint8_t *)
			(((int64_t)from + (MEM_PAGE_SIZE - 1)) & -MEM_PAGE_SIZE);
	const uint8_t *end = from + size;
	const volatile uint8_t *p_byte;

	// In case 'from' was not page-aligned, take care of the partial page.
	if (start > from) {
		p_byte = from;
		p_byte[0];
	}

	for (p_byte = start; p_byte < end; p_byte += MEM_PAGE_SIZE) {
		p_byte[0];
	}
}

void
cf_topo_migrate_memory(void)
{
	if (g_i_numa_node == INVALID_INDEX) {
		return;
	}

	// Migrate existing memory allocations to the selected NUMA node.

	os_numa_node_index i_os_numa_node = g_numa_node_index_to_os_numa_node_index[g_i_numa_node];
	uint64_t to_mask = 1UL << i_os_numa_node;
	cf_detail(CF_HARDWARE, "NUMA node mask (to): %016" PRIx64, to_mask);

	uint64_t from_mask = 0;

	for (cf_topo_numa_node_index i_numa_node = 0; i_numa_node < g_n_numa_nodes; ++i_numa_node) {
		i_os_numa_node = g_numa_node_index_to_os_numa_node_index[i_numa_node];
		from_mask |= 1u << i_os_numa_node;
	}

	from_mask &= ~to_mask;
	cf_detail(CF_HARDWARE, "NUMA node mask (from): %016" PRIx64, from_mask);

	if (from_mask != 0) {
		cf_info(CF_HARDWARE, "migrating shared memory to local NUMA node - this may take a bit");
		// Unlike select(), we have to pass "number of valid bits + 1".
		migrate_pages_safe(0, 65, &from_mask, &to_mask);
	}

	// We had kept capabilities so we could do this migrate - revoke them now.
	cf_process_clearcap();
}

void
cf_topo_info(void)
{
	if (g_i_numa_node == INVALID_INDEX) {
		cf_info(CF_HARDWARE, "detected %hu CPU(s), %hu core(s), %hu NUMA node(s)",
				g_n_cpus, g_n_cores, g_n_numa_nodes);
	}
	else {
		cf_info(CF_HARDWARE, "detected %hu CPU(s), %hu core(s) on NUMA node %hu of %hu",
				g_n_cpus, g_n_cores, g_i_numa_node, g_n_numa_nodes);
	}
}
