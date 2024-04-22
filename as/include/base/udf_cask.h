/*
 * udf_cask.h
 *
 * Copyright (C) 2013-2021 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "dynbuf.h"


//==========================================================
// Public API.
//

// Startup.
void udf_cask_init(void);

// Info commands.
void udf_cask_info_clear_cache(const char* name, const char* params, cf_dyn_buf* out);
void udf_cask_info_get(const char* name, const char* params, cf_dyn_buf* out);
void udf_cask_info_list(const char* name, const char* params, cf_dyn_buf* out);
void udf_cask_info_put(const char* name, const char* params, cf_dyn_buf* out);
void udf_cask_info_remove(const char* name, const char* params, cf_dyn_buf* out);
