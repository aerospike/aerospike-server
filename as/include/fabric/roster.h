/*
 * roster.h
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

//==========================================================
// Includes.
//

#include <stdbool.h>

#include "node.h"

#include "fabric/partition_balance.h"


//==========================================================
// Public API.
//

void as_roster_init_smd();
bool as_roster_set_nodes_cmd(const char* ns_name, const char* nodes);


//==========================================================
// Inlines and macros.
//

// Format is: <node-id-hex-str>:<rack-id-decimal-str>,
#define ROSTER_STRING_ELE_LEN ((sizeof(cf_node) * 2) + 1 + MAX_RACK_ID_LEN + 1)

// In string lists, separate node-id and rack-id with this character.
#define ROSTER_ID_PAIR_SEPARATOR '@'
