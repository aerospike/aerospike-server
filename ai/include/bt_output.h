/*
 * bt_output.h
 *
 * Copyright (C) 2013 Aerospike, Inc.
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
/*
 *  SYNOPSIS
 *    This file provides declarations for the B-Tree output functions.
 */

#pragma once

#include <stdio.h>

#include "bt.h"

void bt_dump_info(FILE *fp, bt *btr);
void bt_dumptree(FILE *fp, bt *btr, bool is_index, bool verbose);
