/*
 * skew_monitor.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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
#include <stdint.h>

#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"

/**
 * Initialize skew monitor.
 */
void
as_skew_monitor_init();

/**
 * Return the current estimate of the clock skew in the cluster.
 */
uint64_t
as_skew_monitor_skew();

/**
 * Return the currently estimated outliers from our cluster.
 * Outliers should have space to hold at least AS_CLUSTER_SZ nodes.
 */
uint32_t
as_skew_monitor_outliers(cf_vector* outliers);

/**
 * Print skew outliers to a dynamic buffer.
 */
uint32_t
as_skew_monitor_outliers_append(cf_dyn_buf* db);

/**
 * Print skew monitor info to a dynamic buffer.
 */
void
as_skew_monitor_info(cf_dyn_buf* db);

/**
 * Dump some debugging information to the logs.
 */
void
as_skew_monitor_dump();
