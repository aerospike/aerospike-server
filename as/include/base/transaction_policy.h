/*
 * transaction_policy.h
 *
 * Copyright (C) 2014-2016 Aerospike, Inc.
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
	// Server config override value only - means use policy sent by client.
	AS_READ_CONSISTENCY_LEVEL_PROTO = -1,

	// Must match AS_POLICY_CONSISTENCY_LEVEL_ONE in C Client v3 as_policy.h.
	// Ignore duplicates - i.e. don't duplicate resolve.
	AS_READ_CONSISTENCY_LEVEL_ONE,

	// Must match AS_POLICY_CONSISTENCY_LEVEL_ALL in C Client v3 as_policy.h.
	// Involve all duplicates in the operation - i.e. duplicate resolve.
	AS_READ_CONSISTENCY_LEVEL_ALL,
} as_read_consistency_level;

typedef enum {
	// Server config override value only - means use policy sent by client.
	AS_WRITE_COMMIT_LEVEL_PROTO = -1,

	// Must match AS_POLICY_COMMIT_LEVEL_ALL in C Client v3 as_policy.h.
	// Respond to client only after successfully committing all replicas.
	AS_WRITE_COMMIT_LEVEL_ALL,

	// Must match AS_POLICY_COMMIT_LEVEL_MASTER in C Client v3 as_policy.h.
	// Respond to client after successfully committing the master replica.
	AS_WRITE_COMMIT_LEVEL_MASTER,
} as_write_commit_level;


//==========================================================
// Public API - macros.
//

//------------------------------------------------
// Extract levels from an as_msg.
//

// Not a strict check: both bits == 0 means ONE, anything else means ALL.
#define PROTO_CONSISTENCY_LEVEL(asmsg) \
	((((asmsg).info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B0) == 0 && \
	  ((asmsg).info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B1) == 0) ? \
			AS_READ_CONSISTENCY_LEVEL_ONE : AS_READ_CONSISTENCY_LEVEL_ALL)

// Not a strict check: both bits == 0 means ALL, anything else means MASTER.
#define PROTO_COMMIT_LEVEL(asmsg) \
	((((asmsg).info3 & AS_MSG_INFO3_COMMIT_LEVEL_B0) == 0 && \
	  ((asmsg).info3 & AS_MSG_INFO3_COMMIT_LEVEL_B1) == 0) ? \
			AS_WRITE_COMMIT_LEVEL_ALL : AS_WRITE_COMMIT_LEVEL_MASTER)

//------------------------------------------------
// Get levels for a transaction with reservation.
//

// Determine read consistency level for a transaction based on everything.
#define TR_READ_CONSISTENCY_LEVEL(tr) \
	(tr->rsv.ns->read_consistency_level == AS_READ_CONSISTENCY_LEVEL_PROTO ? \
		PROTO_CONSISTENCY_LEVEL(tr->msgp->msg) : \
		tr->rsv.ns->read_consistency_level)

// Determine write commit level for a transaction based on everything.
#define TR_WRITE_COMMIT_LEVEL(tr) \
	(tr->rsv.ns->write_commit_level == AS_WRITE_COMMIT_LEVEL_PROTO ? \
		PROTO_COMMIT_LEVEL(tr->msgp->msg) : \
		tr->rsv.ns->write_commit_level)

//------------------------------------------------
// Get levels without need of reservation.
//

// Same as above, for use before tr->rsv has been made.
#define READ_CONSISTENCY_LEVEL(ns, asmsg) \
	(ns->read_consistency_level == AS_READ_CONSISTENCY_LEVEL_PROTO ? \
		PROTO_CONSISTENCY_LEVEL(asmsg) : \
		ns->read_consistency_level)

//------------------------------------------------
// Get config override values' names.
//

#define NS_READ_CONSISTENCY_LEVEL_NAME() \
	(ns->read_consistency_level == AS_READ_CONSISTENCY_LEVEL_PROTO ? \
		"off" : (ns->read_consistency_level == AS_READ_CONSISTENCY_LEVEL_ONE ? \
			"one" : "all"))

#define NS_WRITE_COMMIT_LEVEL_NAME() \
	(ns->write_commit_level == AS_WRITE_COMMIT_LEVEL_PROTO ? \
		"off" : (ns->write_commit_level == AS_WRITE_COMMIT_LEVEL_ALL ? \
			"all" : "master"))
