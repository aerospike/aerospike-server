/*
 * cfg_tree_wrapper.h
 *
 * Copyright (C) 2025 Aerospike, Inc.
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

#ifdef __cplusplus
extern "C" {
#endif

#include "base/cfg.h"

// Opaque pointer to the C++ CFGTree object.
typedef void* cfg_tree_t;

typedef enum { CFG_FORMAT_YAML = 0 } cfg_format_t;

cfg_tree_t cfg_tree_create(const char* config_file, const char* schema_file,
		cfg_format_t format);
void cfg_tree_destroy(cfg_tree_t cfg_tree);
int cfg_tree_validate(cfg_tree_t cfg_tree);
char* cfg_tree_dump(cfg_tree_t cfg_tree);
int cfg_tree_apply_config(cfg_tree_t cfg_tree, as_config* config);
void cfg_tree_free_string(char* str);
const char* cfg_tree_get_last_error(void);

#ifdef __cplusplus
}
#endif