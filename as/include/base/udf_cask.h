/*
 * udf_cask.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"

#include "base/thr_info.h"


// UDF Types
#define AS_UDF_TYPE_LUA 0
#define MAX_UDF_CONTENT_LENGTH (1024 * 1024) //(1MB)

extern char *as_udf_type_name[];

//------------------------------------------------
// Register function
void udf_cask_init();

//------------------------------------------------
// these functions are "as_info_command" format
// and called directly from there.
// therefore they have the same calling convention

int udf_cask_info_clear_cache(char * name, char * params, cf_dyn_buf * out);

int udf_cask_info_get(char * name, char * params, cf_dyn_buf * out);

int udf_cask_info_put(char * name, char * params, cf_dyn_buf * out);

int udf_cask_info_remove(char * name, char * params, cf_dyn_buf * out);

int udf_cask_info_reconfigure(char * name, char * params, cf_dyn_buf * buf);

int udf_cask_info_list(char *name, cf_dyn_buf * out);

//------------------------------------------------
// these are called by the modules that need to run UDFs

// called by a module to get the data associated with a udf (the file contents)
// this will be a reference count (rc_alloc) pointer and must be dereferenced by the caller
int udf_cask_get_udf(char *module, char *udf_type, uint8_t **buf , size_t *buf_len );

// called by a module to get the data associated with a udf (the fully qualified file name)
// caller passes in a max-size string buffer that gets filled out (null terminated)
int udf_cask_get_udf_filename(char *module, char *udf_type, char *filename );

