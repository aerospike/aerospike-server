/*
 * xmem.h
 *
 * Copyright (C) 2018 Aerospike, Inc.
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
// Typedefs & constants.
//

typedef enum {
	CF_XMEM_TYPE_UNDEFINED = -1,

	CF_XMEM_TYPE_SHMEM = 0,
	CF_XMEM_TYPE_PMEM,
	CF_XMEM_TYPE_FLASH,

	CF_NUM_XMEM_TYPES
} cf_xmem_type;

#define CF_XMEM_MAX_MOUNTS 16
