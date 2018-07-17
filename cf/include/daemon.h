/*
 * daemon.h
 *
 * Copyright (C) 2017 Aerospike, Inc.
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
#include <sys/types.h>

void cf_process_daemonize(int *fd_ignore_list, int list_size);
void cf_process_privsep(uid_t uid, gid_t gid);
void cf_process_add_startup_cap(int cap);
void cf_process_add_runtime_cap(int cap);
void cf_process_drop_startup_caps(void);
bool cf_process_has_cap(int cap);
void cf_process_enable_cap(int cap);
void cf_process_disable_cap(int cap);
