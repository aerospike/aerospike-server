/*
 * hardware.h
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

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <socket.h>

typedef enum {
	CF_TOPO_AUTO_PIN_NONE,
	CF_TOPO_AUTO_PIN_CPU,
	CF_TOPO_AUTO_PIN_NUMA
} cf_topo_auto_pin;

typedef uint16_t cf_topo_os_cpu_index;

typedef uint16_t cf_topo_numa_node_index;
typedef uint16_t cf_topo_core_index;
typedef uint16_t cf_topo_cpu_index;

#define CF_TOPO_INVALID_INDEX ((cf_topo_numa_node_index)-1)

void cf_topo_config(cf_topo_auto_pin auto_pin, cf_topo_numa_node_index a_numa_node,
		const cf_addr_list *addrs);
void cf_topo_force_map_memory(const uint8_t *from, size_t size);
void cf_topo_migrate_memory(void);
void cf_topo_info(void);

uint16_t cf_topo_count_cores(void);
uint16_t cf_topo_count_cpus(void);

cf_topo_cpu_index cf_topo_current_cpu(void);
cf_topo_cpu_index cf_topo_socket_cpu(const cf_socket *sock);

void cf_topo_pin_to_core(cf_topo_core_index i_core);
void cf_topo_pin_to_cpu(cf_topo_cpu_index i_cpu);

#define CF_STORAGE_MAX_PHYS 100

typedef struct cf_storage_device_s {
	char *dev_path;
	uint32_t n_phys;

	struct {
		char *dev_path;
		cf_topo_numa_node_index numa_node;
		int32_t nvme_age;
	} phys[CF_STORAGE_MAX_PHYS];
} cf_storage_device_info;

cf_storage_device_info *cf_storage_get_device_info(const char *path);
void cf_storage_set_scheduler(const char *path, const char *sched);
int64_t cf_storage_file_system_size(const char *path);

void cf_page_cache_dirty_limits(void);

bool cf_mount_is_local(const char *path);
