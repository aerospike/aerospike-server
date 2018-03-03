/*
 * system_metadata.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 *    The System Metadata module provides a mechanism for synchronizing
 *    module metadata cluster-wide.  While each module is responsible
 *    for the interpretation of its own metadata, the System Metadata
 *    module provides persistence and automatic distribution of changes
 *    to that opaque metadata.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "node.h"


/* Declare Public System Metadata Types */


/*
 *  Type for actions to perform upon metadata items.
 */
typedef enum as_smd_action_e {
	AS_SMD_ACTION_SET,       // Add or modify this metadata item
	AS_SMD_ACTION_DELETE     // Delete this metadata item
} as_smd_action_t;

/*
 *  Type for an item of metadata.
 */
typedef struct as_smd_item_s {
	cf_node node_id;         // Originating node ID
	as_smd_action_t action;  // Action to perform on this metadata item
	char *module_name;       // Module name of the item
	char *key;               // Key of the metadata item
	char *value;             // Value of the metadata item
	uint32_t generation;     // Metadata generation counter
	uint64_t timestamp;      // Time metadata last modified
} as_smd_item_t;

/*
 *  Type for a list of metadata items for a particular node.
 */
typedef struct as_smd_item_list_s {
	size_t num_items;        // Number of metadata items
	as_smd_item_t *item[];   // Array of pointers to metadata items
} as_smd_item_list_t;

/*
 *  Opaque type representing the state of the System Metadata module.
 */
typedef struct as_smd_s as_smd_t;

/*
 *  SMD is a singleton, though many class methods are passed an object pointer.
 */
extern as_smd_t *g_smd;

/*
 *  Type for mutually-disjoint flag values passed by SMD to the module's accept callback
 *   via the "accept_opt" argument specifying the originator of the operation.
 */
typedef enum as_smd_accept_option_e {
	AS_SMD_ACCEPT_OPT_CREATE  = (1 << 0),  // Module creation-time accept event
	AS_SMD_ACCEPT_OPT_MERGE   = (1 << 1),  // Post-cluster state change merge
	AS_SMD_ACCEPT_OPT_API     = (1 << 2)   // User-initiated set/delete metadata via SMD API
} as_smd_accept_option_t;

/*
 *  Size of the key to be used during a majority consensus merge operation.
 *  (Ideally this would be a module-supplied parameter rather than a constant.)
 */
#define AS_SMD_MAJORITY_CONSENSUS_KEYSIZE  (1024)


/* Callback Function Types. */


/*
 *  Callback function type for getting metadata items.
 */
typedef int (*as_smd_get_cb)(char *module, as_smd_item_list_t *items, void *udata);

/*
 *  Callback function type for metadata merge policy functions.
 *    Resolve action executed on Paxos principal node to determine the cluster-wide "truth."
 *    Default merge policy:  union
 *    Alternative merge policies:  highest generation, latest timestamp
 *    Configurable via registering a per-module callback function.
 */
typedef int (*as_smd_merge_cb)(const char *module, as_smd_item_list_t **item_list_out, as_smd_item_list_t **item_lists_in, size_t num_lists, void *udata);

/*
 *  Callback function type for metadata merge item conflict resolution functions.
 *    Use only if not using custom as_smd_merge_cb
 *    Default item conflict resolution picks greater SMD generation/timestamp
 *    Configurable via registering a per-module callback function.
 *    Return true to choose existing_item, false to choose new_item.
 */
typedef bool (*as_smd_conflict_cb)(char *module, as_smd_item_t *existing_item, as_smd_item_t *new_item, void *udata);

/*
 *  Callback function type for metadata acceptance policy functions.
 *    The accept callback is executed to commit a metadata change, with
 *     the accept option specifying the originator of the accept action as follows:
 *       1). OPT_CREATE:  When a module has been created and its persisted metadata has been restored.
 *       2). OPT_MERGE:   When all cluster nodes receive and accept the truth from the Paxos principal.
 *       3). OPT_API:     When metadata is set via the API or restored from persistence, handled locally
 *                          prior to cluster formation, otherwise proxied via the Paxos principal.
 *    Configurable via registering a per-module callback function.
 */
typedef int (*as_smd_accept_cb)(char *module, as_smd_item_list_t *items, void *udata, uint32_t accept_opt);

/*
 *  Callback function type for metadata acceptance pre-check policy function.
 *    When a user-initiated metadata change operation is requested via the SMD API,
 *    the validity of operation and arguments is first checked on the Paxos principal
 *    to decide whether this operation should be sent to all cluster nodes.
 *    Configurable via registering a per-module callback function.
 */
typedef int (*as_smd_can_accept_cb)(char* module, as_smd_item_t *item, void *udata);


/* Constructor and destructor functions for metadata item list objects passed to/from the callback functions. */


/*
 *  Create an empty list of reference-counted metadata items.
 */
as_smd_item_list_t *as_smd_item_list_create(size_t num_items);

/*
 *  Release a list of reference-counted metadata items.
 */
void as_smd_item_list_destroy(as_smd_item_list_t *items);


/* System Metadata Module Startup / Shutdown */


/*
 *  Initialize the single global System Metadata module.
 */
as_smd_t *as_smd_init(void);

/*
 *  Start the System Metadata module to begin receiving Paxos state change events.
 */
int as_smd_start(as_smd_t *smd);

/*
 *  Terminate the System Metadata module.
 */
int as_smd_shutdown(as_smd_t *smd);


/* Metadata Manipulation */


/*
 *  Create a container for the named module's metadata and register the policy callback functions.
 *  (Pass a NULL callback function pointer to select the default policy.)
 */
int as_smd_create_module(char *module,
						 as_smd_merge_cb merge_cb, void *merge_udata,
						 as_smd_conflict_cb conflict_cb, void *conflict_udata,
						 as_smd_accept_cb accept_cb, void *accept_udata,
						 as_smd_can_accept_cb can_accept_cb, void *can_accept_udata);

/*
 *  Destroy the container for the named module's metadata, releasing all of its metadata.
 */
int as_smd_destroy_module(char *module);

/*
 *  Add a new, or modify an existing, metadata item in an existing module.
 */
int as_smd_set_metadata(char *module, char *key, char *value);

/*
 *  Delete an existing metadata item from an existing module.
 */
int as_smd_delete_metadata(char *module, char *key);

/*
 *  Retrieve metadata item(s.) (Pass NULL for module and/or key for "all".)
 */
int as_smd_get_metadata(char *module, char *key, as_smd_get_cb cb, void *udata);


/* Info Command Functions */


/*
 *  Print info. about the System Metadata state to the log.
 *  (Verbose true prints detailed info. about the metadata values.)
 */
void as_smd_dump(bool verbose);

/*
 *  Manipulate the System Metadata and log the result.
 */
void as_smd_info_cmd(char *cmd, cf_node node_id, char *module, char *key, char *value);


/* Pre-Defined Callback Policy Functions. */


/*
 *  Merge callback function implementing the majority consensus merge policy.
 */
int as_smd_majority_consensus_merge(const char *module, as_smd_item_list_t **item_list_out,
									as_smd_item_list_t **item_lists_in, size_t num_lists, void *udata);
