/*
 * system_metadata.c
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

#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>

#include "aerospike/as_hashmap.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_stringmap.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_rchash.h"

#include "msg.h"
#include "shash.h"

#include "base/cfg.h"
#include "base/secondary_index.h"
#include "base/system_metadata.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "jansson.h"


/*
**                                 System Metadata Theory of Operation
**                                 ===================================
**
**   Overview:
**   ---------
**
**   The System Metadata (SMD) module provides the means for an Aerospike cluster to manage and
**   automatically and consistently distribute data describing the state of any number of modules
**   within each of the cluster nodes.  This data is called "system metadata."  System metadata
**   is managed on a module-by-module basis, where each registered module has a set of zero or
**   more SMD items.  An SMD item has properties describing the item (module name, key, value
**   generation, and modification timestamp.)  The contents (value) of an SMD item is opaque to
**   the SMD module itself.  At creation time, modules may register policy callback functions
**   to perform the actions of merging and accepting metadata updates, or else select the system
**   default policy for these operations.
**
**   Initialization:
**   ---------------
**
**   Prior to use, the System Metadata module must first be initialized by calling "as_smd_init()"
**   to create the SMD internal data structures and launch a captive thread to process all
**   incoming system metadata events.  During this phase, all system metadata operations will be
**   handled locally on each node.
**
**   Once all server components have been initialized, SMD may be started via "as_smd_start()".
**   At this point, SMD will begin handling cluster state change events and begin
**   communicating with SMD in other cluster nodes via SMD fabric messages to synchronize
**   system metadata cluster-wide.  Fabric transactions are used guarantee message delivery
**   succeeds or fails atomically, with re-try handled automatically at fabric level.
**
**   The System Metadata module may be terminated using "as_smd_shutdown()", which de-registers
**   the SMD fabric message type and causes the captive thread to exit.  At this point, it is
**   permissible to re-initialize (and then re-start) the System Metadata module again.
**
**   Life Cycle of System Metadata:
**   ------------------------------
**
**   For a server component to use System Metadata, the component must first create its SMD
**   module.  The SMD API names modules via a name string which must be unique within the
**   server.  Calling "as_smd_create_module()" will create a container object in SMD to
**   hold the module's metadata and register any supplied policy callback functions provided by
**   the component.  To release the component's SMD module, call "as_smd_destroy_module()".
**
**   After a module has been created, new metadata items may be added, or existing items may
**   be modified, using "as_smd_set_metadata()".  Existing metadata items may be removed using
**   "as_smd_delete_metadata()".  Metadata may be searched using "as_smd_get_metadata()", which
**   can return one or more items for one or more modules, depending upon the item list passed
**   in, and sends the search results to a user-supplied callback function.
**
**   Each module's metadata is automatically persisted via serialization (in JSON format) to a file
**   upon each accepted metadata item change and also when the module is destroyed.  When a module
**   is created (usually at server start-up time), if an existing SMD file is found for the module,
**   its contents will be loaded in as the initial values of the module's metadata.
**
**   System Metadata Policy Callback Functions:
**   ------------------------------------------
**
**   There are three SMD policy callback functions a module may register.  If NULL is passed
**   for a callback function pointer in "as_smd_module_create()", the system default policy
**   will be selected for that operation.  All policy callbacks are executed in the context
**   of the SMD thread.
**
**   The SMD policy callbacks operate as follows:
**
**     1). The Merge Callback ("as_smd_merge_cb()"):  When a cluster state change occurs,
**          each module's Merge callback will be executed on the SMD principal to create a new,
**          unified view of each module's metadata.  The system default merge policy is to simply
**          form a union of all nodes' metadata items for the given module, taking the latest
**          version of metadata items with duplicate keys, chosen first by highest generation
**          and second by highest timestamp.
**
**     2). The Accept Callback ("as_smd_accept_cb()"):  When a modules SMD item(s) are changed,
**          or when a module is created and fully restored from persistence, the Accept callback
**          will be invoked on every node to commit the change, with the originator of the accept
**          event passed as the accept option parameter value.
**
**          This callback will be invoked in three distinct cases:
**
**          First, when a module is created and its persisted metadata (if any) has been fully
**          restored, this callback will be invoked with the OPT_CREATE accept option and a
**          NULL item list.  This event is the proper point for synchronizing with any other
**          thread(s) who depend upon the given module being fully initialized.
**
**          Second, after the the SMD principal has determined the merged metadata for a
**          module, it will distribute the new metadata to all cluster nodes (including itself)
**          for processing via the Accept callback with the OPT_MERGE accept option and an item
**          list of length 0 or greater.  The system default accept policy is simply to replace
**          any preexisting metadata items for the module with the received metadata items.
**          Modules will generally, however, define their own Accept callback to take actions
**          based upon the changed metadata, such as creating secondary indexes or defining new
**          User Defined Functions (UDFs.)
**
**          Third, when a metadata item is set or deleted via the SMD API (or at module creation
**          time, via restoration from persisted state), the Accept callback will be invoked with
**          the OPT_API accept option and an item list of length 1.  Note that at system start-up
**          time, prior to cluster formation, the metadata change will be handled locally.  Once
**          a cluster has been joined, however, each metadata change event will be proxied to
**          the SMD principal, who will forward it to every cluster node (including itself)
**          for acceptance.
**
**     3). The Can Accept Callback ("as_smd_can_accept_cb()"):  When the SMD principal
**          receives a metadata change request (set or delete), it will first attempt to
**          validate the request via any registered Can Accept callback.  If the callback
**          exists, it must return non-zero for the item to be processed.  Otherwise the item
**          will be rejected.
**
**   Threading Structure:
**   --------------------
**
**   The System Metadata module relies on a single, captive thread to handle all incoming SMD
**   fabric messages, public SMD API operations, and to invoke module's registered policy
**   callbacks.  Single-thread access means no locking of SMD data structures is necessary.
**
**   The SMD thread waits on a queue for messages from either the local node (created and sent
**   via the System Metadata API functions) or from other cluster nodes (via System Metadata
**   fabric messages.)
**
**   Initially the System Metadata module is inactive until the "as_smd_init()" function launches
**   the System Metadata thread.  At this point, only node-local SMD commands and events will be
**   processed.  When "as_smd_start()" is called, a START message will be sent telling the SMD
**   thread to also begin receiving SMD events for cluster state change notifications
**   and from other cluster nodes via SMD fabric messages.  SMD will now perform the full
**   policy callback processing as describe above.  The System Metadata module will be running
**   until the "as_smd_shutdown()" function sends a SHUTDOWN message, upon receipt of which the
**   System Metadata thread will exit cleanly.
**
**   Internal Messaging Structure:
**   -----------------------------
**
**   Each public SMD API function invocation corresponds to an event message being sent to the
**   System Metadata thread via its message queue for processing.  Internal command messages
**   (those not generated by API calls) are also sent via the message queue to handle cluster
**   state change events, incoming SMD fabric messages, and other internal utility functions.
**
**   Each event is defined by an event type, options bits, and a metadata item (which may be
**   NULL or partially populated, depending upon the command type.)
**
**   The SMD command message types are:
**
**    1). INIT / START / SHUTDOWN:  These messages correspond to the APIs controlling the
**          running of the SMD subsystem itself and its captive thread.
**
**    2). CREATE_MODULE / DESTROY_MODULE:  These messages create and destroy module objects
**          containing metadata items.
**
**    3). SET_METADATA / DELETE_METADATA / GET_METADATA:  The SMD API sends these messages to
**          set, delete, and get metadata items.
**
**    4). INTERNAL:  This message type is used for non-API "internal" events such as the event
**          triggered by a cluster state change notification, incoming SMD fabric
**          messages from other nodes, or to dump info. about the state of system metadata to
**          the system log.
**
**   Debugging Utilities:
**   --------------------
**
**   The state of the System Metadata module can be logged using the "dump-smd:" Info command:
**
**     dump-smd:[verbose={"true"|"false"}]   (Default: "false".)
**
**   The optional option "verbose" parameter may be set to "true" to log additional detailed
**   information about the system metadata, such as information about all modules' metadata items.
**
**   System Metadata may be directly manipulated using the "smd:" Info command:
**
**     smd:cmd=<SMDCommand>[;module=<String>;node=<HexNodeID>;key=<String>;value=<String>]
**
**   where <SMDCommand> is one of:  {create|destroy|set|delete|get|init|start|shutdown}, and:
**    - The "init", "start", and "shutdown" commands take no parameters;
**    - The "create" and "destroy" commands require a "module" parameter;
**    - The "set" command requires "key" and "value", the "delete" command only requires "key";
**    - The "get" command can take "module", "key" and "node" parameters, which if specified as
**       empty, e.g., "module=;key=", will perform a wildcard metadata item retrieval.
**
**   Open Issues:
**   ------------
**
**   The SMD API currently provides no mechanism for notifying the caller whether (or when)
**   the request has succeeded (or failed.)  The challenge is that in general the asynchronous
**   event may be triggered on a remote node, e.g., the SMD principal.  Support for an optional
**   callback for this purpose (per-module or per-API call) may be added in the future.
**
*/


/* Define constants. */


/* Maximum length for System Metadata persistence files. */
#define MAX_PATH_LEN  (1024)

/* Time in milliseconds to wait for an incoming message. */
#define AS_SMD_WAIT_INTERVAL_MS  (1000)

/* Time in milliseconds for System Metadata proxy transactions to the SMD principal. */
#define AS_SMD_TRANSACT_TIMEOUT_MS  (1000)

#define SMD_MAX_STACK_MODULES 128
#define SMD_MAX_STACK_NUM_ITEMS (1 << 14)

/* Declare Private Types */


/*
 *  Type for System Metadata command option flags.
 */
typedef enum as_smd_cmd_opt_e {
	AS_SMD_CMD_OPT_NONE           = 0x00,
	AS_SMD_CMD_OPT_DUMP_SMD       = 0x01,
	AS_SMD_CMD_OPT_VERBOSE        = 0x02
} as_smd_cmd_opt_t;

/*
 *  Types of API commands sent to the System Metadata module.
 */
typedef enum as_smd_cmd_type_e {
	AS_SMD_CMD_INIT,              // System Metadata API initialization
	AS_SMD_CMD_START,             // System Metadata start receiving cluster state changes
	AS_SMD_CMD_CREATE_MODULE,     // Metadata container creation
	AS_SMD_CMD_DESTROY_MODULE,    // Metadata container destruction
	AS_SMD_CMD_SET_METADATA,      // Add new, or modify existing, metadata item
	AS_SMD_CMD_DELETE_METADATA,   // Existing metadata item deletion
	AS_SMD_CMD_GET_METADATA,      // Get single metadata item
	AS_SMD_CMD_CLUSTER_CHANGED,   // Cluster state change
	AS_SMD_CMD_INTERNAL,          // System Metadata system internal command
	AS_SMD_CMD_SHUTDOWN           // System Metadata shut down
} as_smd_cmd_type_t;

/*
 *  Name of the given System Metadata API command type.
 */
#define AS_SMD_CMD_TYPE_NAME(cmd)  (AS_SMD_CMD_INIT == cmd ? "INIT" : \
									(AS_SMD_CMD_START == cmd ? "START" : \
									 (AS_SMD_CMD_CREATE_MODULE == cmd ? "CREATE" : \
									  (AS_SMD_CMD_DESTROY_MODULE == cmd ? "DESTROY" : \
									   (AS_SMD_CMD_SET_METADATA == cmd ? "SET" : \
										(AS_SMD_CMD_DELETE_METADATA == cmd ? "DELETE" : \
										 (AS_SMD_CMD_GET_METADATA == cmd ? "GET" : \
										  (AS_SMD_CMD_CLUSTER_CHANGED == cmd ? "CLUSTER" : \
										   (AS_SMD_CMD_INTERNAL == cmd ? "INTERNAL" : \
											(AS_SMD_CMD_SHUTDOWN == cmd ? "SHUTDOWN" : "<UNKNOWN>"))))))))))

/*
 *  Type for System Metadata event messages sent via the API.
 */
typedef struct as_smd_cmd_s {
	as_smd_cmd_type_t type;              // System Metadata command type
	uint32_t options;                    // Bit vector of event options of type "as_smd_cmd_opt_t"
	as_smd_item_t *item;                 // Metadata item associated with this event (only relevant fields are set)
	void *a, *b, *c, *d, *e, *f, *g, *h; // Generic storage for command parameters.
} as_smd_cmd_t;

/*
 *  Types of operation messages handled by the System Metadata module, received as msg events.
 */
typedef enum as_smd_msg_op_e {
	AS_SMD_MSG_OP_SET_ITEM,                 // Add a new, or modify an existing, metadata item
	AS_SMD_MSG_OP_DELETE_ITEM,              // Delete an existing metadata item (must already exist)  [[Deprecated]]
	AS_SMD_MSG_OP_MY_CURRENT_METADATA,      // Current metadata sent from a node to the principal
	AS_SMD_MSG_OP_ACCEPT_THIS_METADATA,     // New blessed metadata sent from the principal to a node
	AS_SMD_MSG_OP_SET_FROM_PR               // Accept item (OPT_API) from principal.
} as_smd_msg_op_t;

/*
 *  Name of the given System Metadata message operation.
 */
#define AS_SMD_MSG_OP_NAME(op)  (AS_SMD_MSG_OP_SET_ITEM == op ? "SET_ITEM" : \
								 (AS_SMD_MSG_OP_DELETE_ITEM == op ? "DELETE_ITEM" : \
								  (AS_SMD_MSG_OP_MY_CURRENT_METADATA == op ? "MY_CURRENT_METADATA" : \
								   (AS_SMD_MSG_OP_ACCEPT_THIS_METADATA == op ? "ACCEPT_THIS_METADATA" : \
									(AS_SMD_MSG_OP_SET_FROM_PR == op ? "SET_FROM_PR" : "<UNKNOWN>")))))

/*
 *  Name of the given System Metadata action.
 */
#define AS_SMD_ACTION_NAME(action)  (AS_SMD_ACTION_SET == action ? "SET" : \
									 (AS_SMD_ACTION_DELETE == action ? "DELETE" : "<UNKNOWN>"))


/* Define API Command / Message Type / Callback Action Correspondence Macros. */


/*
 *  Message operation corresponding to the given API command type.
 *   (Default to SET_ITEM for the unknown case.)
 */
#define CMD_TYPE2MSG_OP(cmd)  (AS_SMD_CMD_SET_METADATA == cmd ? AS_SMD_MSG_OP_SET_ITEM : \
							   (AS_SMD_CMD_DELETE_METADATA == cmd ? AS_SMD_MSG_OP_DELETE_ITEM : AS_SMD_MSG_OP_SET_ITEM))

/*
 *  API action corresponding to the given message operation.
 *   (Default to SET for the unknown case.)
 */
#define MSG_OP2ACTION(op)  (AS_SMD_MSG_OP_SET_ITEM == op ? AS_SMD_ACTION_SET : \
							(AS_SMD_MSG_OP_DELETE_ITEM == op ? AS_SMD_ACTION_DELETE : AS_SMD_ACTION_SET))

/*
 *  Type for System Metadata messages transmitted via the fabric.
 */
typedef struct as_smd_msg_s {
	as_smd_msg_op_t op;         // System Metadata operation
	uint64_t cluster_key;       // Sending node's cluster key
	cf_node node_id;            // Sending node's ID
	char *module_name;          // Name of the module.
	uint32_t num_items;         // Number of metadata items
	as_smd_item_list_t *items;  // List of metadata items associated with this message (only relevant fields are set)
	uint32_t options;           // Message options (originator)
} as_smd_msg_t;

/*
 *  Types of events sent to and processed by the System Metadata thread.
 */
typedef enum as_smd_event_type_e {
	AS_SMD_CMD,                 // SMD API command
	AS_SMD_MSG,                 // SMD fabric message
} as_smd_event_type_t;

/*
 *  Type for an event object handled by the System Metadata system.
 *     An event can either be an API command or a message transmitted via the fabric.
 */
typedef struct as_smd_event_s {
	as_smd_event_type_t type;   // Selector determining event type (command or message)
	union {
		as_smd_cmd_t cmd;       // SMD command event sent via the SMD API
		as_smd_msg_t msg;       // SMD message event sent via fabric
	} u;
} as_smd_event_t;

/*
 *  Type for the key for items in the external metadata hash table: node_id, key_len, key (flexible array member, sized by key_len.)
 */
typedef struct as_smd_external_item_key_s {
	cf_node node_id;            // ID of the source cluster node.
	size_t key_len;             // Length of the key string.
	char key[];                 // Flexible array member for the null-terminated key string.
} as_smd_external_item_key_t;

typedef enum {
	AS_SMD_MSG_TRID,
	AS_SMD_MSG_ID,
	AS_SMD_MSG_CLUSTER_KEY,
	AS_SMD_MSG_OP,
	AS_SMD_MSG_NUM_ITEMS, // deprecated
	AS_SMD_MSG_ACTION, // deprecated
	AS_SMD_MSG_MODULE, // deprecated
	AS_SMD_MSG_KEY, // deprecated
	AS_SMD_MSG_VALUE, // deprecated
	AS_SMD_MSG_GENERATION, // deprecated
	AS_SMD_MSG_TIMESTAMP,
	AS_SMD_MSG_MODULE_NAME,
	AS_SMD_MSG_OPTIONS, // deprecated

	AS_SMD_MSG_MODULE_LIST,
	AS_SMD_MSG_MODULE_COUNTS,
	AS_SMD_MSG_KEY_LIST,
	AS_SMD_MSG_VALUE_LIST,
	AS_SMD_MSG_GEN_LIST,

	AS_SMD_MSG_SINGLE_KEY,
	AS_SMD_MSG_SINGLE_VALUE,
	AS_SMD_MSG_SINGLE_GENERATION,
	AS_SMD_MSG_SINGLE_TIMESTAMP,

	NUM_SMD_FIELDS
} smd_msg_fields;

#define AS_SMD_MSG_V2_IDENTIFIER  0x123B

/*
 *  Define the template for System Metadata messages.
 *
 *  System Metadata message structure:
 *     0). Transaction ID - UINT64 (Required for Fabric Transact.)
 *     1). System Metadata Protocol Version Identifier - (uint32_t <==> UINT32)  [Only V2 for now.]
 *     2). Cluster Key - (uint64_t <==> UINT64)
 *     3). Operation - (uint32_t <==> UINT32)
 *     4). Number of items - (uint32_t <==> UINT32)
 *     5). Action[] - Array of (uint32_t <==> UINT32)
 *     6). Module[] - Array of (char * <==> STR)
 *     7). Key[] - Array of (char * <==> STR)
 *     8). Value[] - Array of (char * <==> STR)
 *     9). Generation[] - Array of (uint32_t <==> UINT32)
 *     10). Timestamp[] - Array of (uint64_t <==> UINT64)
 *     11). Module Name - (char * <==> STR)
 *     12). Options - (uint32_t <==> UINT32)
 */
static const msg_template as_smd_msg_template[] = {
	{ AS_SMD_MSG_TRID, M_FT_UINT64 },              // Transaction ID for Fabric Transact
	{ AS_SMD_MSG_ID, M_FT_UINT32 },                // Version of the System Metadata protocol
	{ AS_SMD_MSG_CLUSTER_KEY, M_FT_UINT64 },       // Cluster key corresponding to msg contents
	{ AS_SMD_MSG_OP, M_FT_UINT32 },                // Metadata operation
	{ AS_SMD_MSG_NUM_ITEMS, M_FT_UINT32 },         // Number of metadata items
	{ AS_SMD_MSG_ACTION, M_FT_ARRAY_UINT32 },      // Metadata action array
	{ AS_SMD_MSG_MODULE, M_FT_ARRAY_STR },         // Metadata module array
	{ AS_SMD_MSG_KEY, M_FT_ARRAY_STR },            // Metadata key array
	{ AS_SMD_MSG_VALUE, M_FT_ARRAY_STR },          // Metadata value array
	{ AS_SMD_MSG_GENERATION, M_FT_ARRAY_UINT32 },  // Metadata generation array
	{ AS_SMD_MSG_TIMESTAMP, M_FT_ARRAY_UINT64 },   // Metadata timestamp array
	{ AS_SMD_MSG_MODULE_NAME, M_FT_STR },          // Name of module the message is from or else NULL if from all.
	{ AS_SMD_MSG_OPTIONS, M_FT_UINT32 },           // Option flags specifying the originator of the message (i.e., MERGE/API)

	{ AS_SMD_MSG_MODULE_LIST, M_FT_MSGPACK },
	{ AS_SMD_MSG_MODULE_COUNTS, M_FT_MSGPACK },
	{ AS_SMD_MSG_KEY_LIST, M_FT_MSGPACK },
	{ AS_SMD_MSG_VALUE_LIST, M_FT_MSGPACK },
	{ AS_SMD_MSG_GEN_LIST, M_FT_MSGPACK },

	{ AS_SMD_MSG_SINGLE_KEY, M_FT_STR },
	{ AS_SMD_MSG_SINGLE_VALUE, M_FT_STR },
	{ AS_SMD_MSG_SINGLE_GENERATION, M_FT_UINT32 },
	{ AS_SMD_MSG_SINGLE_TIMESTAMP, M_FT_UINT64 },
};

COMPILER_ASSERT(sizeof(as_smd_msg_template) / sizeof(msg_template) == NUM_SMD_FIELDS);

#define AS_SMD_MSG_SCRATCH_SIZE 64 // accommodate module name

/*
 *  State of operation of the System Metadata module.
 */
typedef enum as_smd_state_e {
	AS_SMD_STATE_IDLE,                     // Not initialized yet
	AS_SMD_STATE_INITIALIZED,              // Ready to receive API calls
	AS_SMD_STATE_RUNNING,                  // Normal operation:  Receiving cluster state changes
	AS_SMD_STATE_EXITING                   // Shutting down
} as_smd_state_t;

/*
 *  Name of the given System Metadata state.
 */
#define AS_SMD_STATE_NAME(state)  (AS_SMD_STATE_IDLE == state ? "IDLE" : \
								   (AS_SMD_STATE_INITIALIZED == state ? "INITIALIZED" : \
									(AS_SMD_STATE_RUNNING == state ? "RUNNING" : \
									 (AS_SMD_STATE_EXITING == state ? "EXITING" : "UNKNOWN"))))

#define SMD_PENDING_MERGE_TIMEOUT_SEC 30

typedef struct smd_pending_merge_s {
	as_smd_msg_t m;
	uint64_t expire;
} smd_pending_merge;

/*
 *  Internal representation of the state of the System Metadata module.
 */
struct as_smd_s {

	// System Metadata thread ID.
	pthread_t thr_id;

	// System Metadata thread attributes.
	pthread_attr_t thr_attr;

	// Is the System Metadata module up and running?
	as_smd_state_t state;

	// Hash table mapping module name (char *) ==> module object (as_smd_module_t *).
	cf_rchash *modules;

	// Message queue for receiving System Metadata messages.
	cf_queue *msgq;

	// Scoreboard of what cluster nodes the SMD principal has received metadata from:  cf_node ==> cf_shash *.
	cf_shash *scoreboard;

	cf_queue pending_merge_queue; // elements are (smd_pending_merge)
};

/*
 *  Type representing a module and holding all metadata for the module.
 */
typedef struct as_smd_module_s {

	// Name of this module.
	char *module;

	// This module's merge metadata callback function (or NULL if none.)
	as_smd_merge_cb merge_cb;

	// User data for the merge metadata callback (or NULL if none.)
	void *merge_udata;

	// This module's item conflict resolution callback function (or NULL if none.)
	as_smd_conflict_cb conflict_cb;

	// User data for the item conflict resolution callback (or NULL if none.)
	void *conflict_udata;

	// This module's accept metadata callback function (or NULL if none.)
	as_smd_accept_cb accept_cb;

	// User data for the accept metadata callback (or NULL if none.)
	void *accept_udata;

	// This module's user_op validation callback (or NULL if none.)
	as_smd_can_accept_cb can_accept_cb;

	// User data for the user_op validation callback (or NULL if none.)
	void *can_accept_udata;

	// Parsed JSON representation of the module's metadata.
	json_t *json;

	// Hash table of metadata registered by this node mapping key (char *) ==> metadata item (as_smd_item_t *).
	cf_rchash *my_metadata;

	// Hash table of metadata received from all external nodes mapping key (as_smd_external_item_key_t *) ==> metadata item (as_smd_item_t *).
	cf_rchash *external_metadata;

	// Does the module need to be persisted?
	bool dirty;
} as_smd_module_t;


/* Define macros. */


/*
 *  Free and set to NULL a pointer if non-NULL.
 */
#define CF_FREE_AND_NULLIFY(ptr) \
	if (ptr) {                   \
		cf_free(ptr);            \
		ptr = NULL;              \
	}

/*
 *  Free members of a metadata item if non-NULL.
 */
#define RELEASE_ITEM_MEMBERS(ptr)          \
	CF_FREE_AND_NULLIFY(ptr->module_name); \
	CF_FREE_AND_NULLIFY(ptr->key);         \
	CF_FREE_AND_NULLIFY(ptr->value);


/* Function forward references. */


static int as_smd_module_persist(as_smd_module_t *module_obj);
void *as_smd_thr(void *arg);


/* Globals. */

as_smd_t *g_smd;

static uint64_t g_cluster_key;
static uint32_t g_cluster_size;
static cf_node g_succession[AS_CLUSTER_SZ];

static void as_smd_destroy_event(as_smd_event_t *evt);

/* Get SMD's principal node */


static inline cf_node as_smd_principal()
{
	return g_succession[0];
}


/* Internal message passing functions. */


/*
 *  Allocate a System Metadata cmd event object to handle API commands.
 *  (Note:  Using 0 for "node_id" is shorthand for the current node.)
 *
 *  Release using "as_smd_destroy_event()".
 */
static as_smd_event_t *as_smd_create_cmd_event(as_smd_cmd_type_t type, ...)
{
	as_smd_event_t *evt = NULL;
	as_smd_item_t *item = NULL;

	// In Commands:  Internal
	uint32_t options = 0;

	// (Always zero.)
	cf_node node_id = 0;

	// In Commands:  Create / Destroy / Set / Delete / Get
	char *module = NULL;

	// In Commands:  Set / Delete / Get
	char *key = NULL;

	// In Commands:  Set
	char *value = NULL;
	uint32_t generation = 0;
	uint64_t timestamp = 0UL;

	// In Commands:  Create
	as_smd_merge_cb merge_cb = NULL;
	void *merge_udata = NULL;
	as_smd_conflict_cb conflict_cb = NULL;
	void *conflict_udata = NULL;
	as_smd_accept_cb accept_cb = NULL;
	void *accept_udata = NULL;
	as_smd_can_accept_cb can_accept_cb = NULL;
	void *can_accept_udata = NULL;

	// In Commands:  Get
	as_smd_get_cb get_cb = NULL;
	void *get_udata = NULL;

	// In Command:  Cluster-changed
	uint64_t cluster_key = 0;
	uint32_t cluster_size = 0;
	cf_node *succession = NULL;

	// Handle variable arguments.
	va_list args;
	va_start(args, type);
	switch (type) {
		case AS_SMD_CMD_INIT:
		case AS_SMD_CMD_START:
		case AS_SMD_CMD_SHUTDOWN:
			// (No additional arguments.)
			break;

		case AS_SMD_CMD_CREATE_MODULE:
			module = va_arg(args, char *);
			merge_cb = va_arg(args, as_smd_merge_cb);
			merge_udata = va_arg(args, void *);
			conflict_cb = va_arg(args, as_smd_conflict_cb);
			conflict_udata = va_arg(args, void *);
			accept_cb = va_arg(args, as_smd_accept_cb);
			accept_udata = va_arg(args, void *);
			can_accept_cb = va_arg(args, as_smd_can_accept_cb);
			can_accept_udata = va_arg(args, void *);
			break;

		case AS_SMD_CMD_DESTROY_MODULE:
			module = va_arg(args, char *);
			break;

		case AS_SMD_CMD_SET_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			value = va_arg(args, char *);
			generation = va_arg(args, uint32_t);
			timestamp = va_arg(args, uint64_t);
			break;

		case AS_SMD_CMD_DELETE_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			break;

		case AS_SMD_CMD_GET_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			get_cb = va_arg(args, as_smd_get_cb);
			get_udata = va_arg(args, void *);
			break;

		case AS_SMD_CMD_CLUSTER_CHANGED:
			cf_debug(AS_SMD, "At event creation for cluster state change");
			cluster_key = va_arg(args, uint64_t);
			cluster_size = va_arg(args, uint32_t);
			succession = va_arg(args, cf_node *);
			break;

		case AS_SMD_CMD_INTERNAL:
			options = va_arg(args, uint32_t);
			break;
	}
	va_end(args);

	// Allocate an event object and initialize it as a command.
	evt = (as_smd_event_t *) cf_calloc(1, sizeof(as_smd_event_t));
	evt->type = AS_SMD_CMD;
	as_smd_cmd_t *cmd = &(evt->u.cmd);
	cmd->type = type;
	cmd->options = options;

	// Only events with the module specified will create a cmd containing a metadata item.
	if (module) {
		// Create the metadata item.
		// [NB: Reference-counted for insertion in metadata "rchash" table.]
		item = (as_smd_item_t *) cf_rc_alloc(sizeof(as_smd_item_t));
		memset(item, 0, sizeof(as_smd_item_t));

		cmd->item = item;

		// Set the originating node ID.
		// (Note:  Using 0 for "node_id" is shorthand for the current node.)
		item->node_id = (!node_id ? g_config.self_node : node_id);

		item->action = MSG_OP2ACTION(CMD_TYPE2MSG_OP(type));

		// Populate the item with duplicated metadata
		// (Note:  The caller is responsible for releasing any dynamically-allocated values passed in.)

		if (module) {
			item->module_name = cf_strdup(module);
		}

		if (key) {
			item->key = cf_strdup(key);
		}

		if (value) {
			size_t value_len = strlen(value) + 1;
			item->value = (char *) cf_malloc(value_len);
			strncpy(item->value, value, value_len);
		}

		item->generation = generation;

		item->timestamp = timestamp;
	}

	// Store the policy callback information generically.
	if (AS_SMD_CMD_CREATE_MODULE == type) {
		cmd->a = merge_cb;
		cmd->b = merge_udata;
		cmd->c = conflict_cb;
		cmd->d = conflict_udata;
		cmd->e = accept_cb;
		cmd->f = accept_udata;
		cmd->g = can_accept_cb;
		cmd->h = can_accept_udata;
	} else if (AS_SMD_CMD_GET_METADATA == type) {
		cmd->a = get_cb;
		cmd->b = get_udata;
	} else if (AS_SMD_CMD_CLUSTER_CHANGED == type) {
		cmd->a = (void *)cluster_key;
		cmd->b = (void *)(uint64_t)cluster_size;
		cmd->c = succession;
	}

	return evt;
}

static bool
smd_msg_read_items(as_smd_msg_t *sm, const msg *m, const cf_vector *mod_vec,
		const uint32_t *counts, cf_vector *key_vec, cf_vector *value_vec,
		uint32_t *gen_list)
{
	if (! msg_msgpack_list_get_buf_array_presized(m, AS_SMD_MSG_KEY_LIST,
			key_vec)) {
		cf_warning(AS_SMD, "KEY_LIST invalid");
		return false;
	}

	msg_msgpack_list_get_buf_array_presized(m, AS_SMD_MSG_VALUE_LIST,
			value_vec);

	uint32_t check = sm->num_items;

	if (! msg_msgpack_list_get_uint32_array(m, AS_SMD_MSG_GEN_LIST, gen_list,
			&check) || check != sm->num_items) {
		cf_warning(AS_SMD, "GEN_LIST invalid with count %u num_items %u", check, sm->num_items);
		return false;
	}

	if (msg_get_uint64_array_count(m, AS_SMD_MSG_TIMESTAMP, &check) != 0 ||
			check != sm->num_items) {
		cf_warning(AS_SMD, "TIMESTAMP invalid with count %u num_items %u", check, sm->num_items);
		return false;
	}

	sm->items = as_smd_item_list_create(sm->num_items);

	uint32_t msg_idx = 0;

	for (uint32_t i = 0; i < cf_vector_size(mod_vec); i++) {
		const msg_buf_ele *p_mod = cf_vector_getp((cf_vector *)mod_vec, i);

		for (uint32_t j = 0; j < counts[i]; j++) {
			as_smd_item_t *item = sm->items->item[msg_idx];

			item->node_id = sm->node_id;
			item->module_name = cf_strndup((const char *)p_mod->ptr, p_mod->sz);

			const msg_buf_ele *p_key = cf_vector_getp(key_vec, msg_idx);
			const msg_buf_ele *p_value = (msg_idx < cf_vector_size(value_vec)) ?
					cf_vector_getp(value_vec, msg_idx) : NULL;

			if (! p_key->ptr) {
				cf_warning(AS_SMD, "invalid packed key at %u/%u", msg_idx, sm->num_items);
				return false;
			}

			item->key = cf_strndup((const char *)p_key->ptr, p_key->sz);
			item->value = (p_value && p_value->ptr) ?
					cf_strndup((const char *)p_value->ptr, p_value->sz) : NULL;

			item->generation = gen_list[msg_idx];
			msg_get_uint64_array(m, AS_SMD_MSG_TIMESTAMP, msg_idx,
					&item->timestamp);

			item->action = item->value ?
					AS_SMD_ACTION_SET : AS_SMD_ACTION_DELETE;

			msg_idx++;
		}
	}

	return true;
}

// New message protocol.
static bool
smd_new_create_msg_event(as_smd_msg_t *sm, cf_node node_id, msg *m)
{
	uint32_t counts[SMD_MAX_STACK_MODULES];
	cf_vector_define(mod_vec, sizeof(msg_buf_ele), SMD_MAX_STACK_MODULES, 0);

	if (sm->op == AS_SMD_MSG_OP_ACCEPT_THIS_METADATA) {
		sm->options = AS_SMD_ACCEPT_OPT_MERGE;
	}
	else if (sm->op == AS_SMD_MSG_OP_SET_FROM_PR) {
		sm->op = AS_SMD_MSG_OP_ACCEPT_THIS_METADATA;
		sm->options = AS_SMD_ACCEPT_OPT_API;
	}

	if (sm->module_name) {
		// Check single item optimized packing.
		char *key;

		if (msg_get_str(m, AS_SMD_MSG_SINGLE_KEY, &key, NULL,
				MSG_GET_DIRECT) == 0) {
			sm->num_items = 1;

			sm->items = as_smd_item_list_create(1);

			as_smd_item_t *item = sm->items->item[0];

			item->node_id = node_id;
			item->module_name = cf_strdup(sm->module_name);
			item->key = cf_strdup(key);
			msg_get_str(m, AS_SMD_MSG_SINGLE_VALUE, &item->value, NULL,
					MSG_GET_COPY_MALLOC);
			msg_get_uint32(m, AS_SMD_MSG_SINGLE_GENERATION, &item->generation);
			msg_get_uint64(m, AS_SMD_MSG_SINGLE_TIMESTAMP, &item->timestamp);
			item->action = item->value ?
					AS_SMD_ACTION_SET : AS_SMD_ACTION_DELETE;

			return true;
		}

		if (! msg_msgpack_container_get_count(m, AS_SMD_MSG_KEY_LIST,
				&sm->num_items) || sm->num_items == 0) {
			sm->items = as_smd_item_list_create(0);
			return true;
		}

		msg_buf_ele ele = {
				.sz = (uint32_t)strlen(sm->module_name),
				.ptr = (uint8_t *)sm->module_name
		};

		cf_vector_append(&mod_vec, &ele);
		counts[0] = sm->num_items;
	}
	else {
		if (! msg_msgpack_container_get_count(m, AS_SMD_MSG_KEY_LIST,
				&sm->num_items) || sm->num_items == 0) {
			sm->items = as_smd_item_list_create(0);
			return true;
		}

		if (! msg_msgpack_list_get_buf_array_presized(m, AS_SMD_MSG_MODULE_LIST,
				&mod_vec)) {
			cf_warning(AS_SMD, "MODULE_LIST invalid");
			return false;
		}

		if (cf_vector_size(&mod_vec) == 0) {
			cf_warning(AS_SMD, "MODULE_LIST zero module names with num_items %u", sm->num_items);
			return false;
		}

		uint32_t check = SMD_MAX_STACK_MODULES;

		if (! msg_msgpack_list_get_uint32_array(m, AS_SMD_MSG_MODULE_COUNTS,
				counts, &check) ||
				check != cf_vector_size(&mod_vec)) {
			cf_warning(AS_SMD, "MODULE_COUNTS invalid with counts %u vector_size(mod_vec) %u", check, cf_vector_size(&mod_vec));
			return false;
		}

		uint32_t total_check = 0;

		for (uint32_t i = 0; i < cf_vector_size(&mod_vec); i++) {
			total_check += counts[i];
		}

		if (total_check != sm->num_items) {
			cf_warning(AS_SMD, "MODULE_COUNTS total %u does not match num_items %u", total_check, sm->num_items);
			return false;
		}
	}

	if (sm->num_items < SMD_MAX_STACK_NUM_ITEMS) {
		uint32_t gen_list[sm->num_items];
		cf_vector_define(key_vec, sizeof(msg_buf_ele), sm->num_items, 0);
		cf_vector_define(value_vec, sizeof(msg_buf_ele), sm->num_items, 0);

		return smd_msg_read_items(sm, m, &mod_vec, counts, &key_vec, &value_vec,
				gen_list);
	}

	cf_vector key_vec;
	cf_vector value_vec;
	uint32_t *gen_list = cf_malloc(sizeof(uint32_t) * sm->num_items);

	cf_vector_init(&key_vec, sizeof(msg_buf_ele), sm->num_items, 0);
	cf_vector_init(&value_vec, sizeof(msg_buf_ele), sm->num_items, 0);

	bool ret = smd_msg_read_items(sm, m, &mod_vec, counts, &key_vec, &value_vec,
			gen_list);

	cf_vector_destroy(&key_vec);
	cf_vector_destroy(&value_vec);
	cf_free(gen_list);

	return ret;
}

/*
 *  Allocate a System Metadata msg event object to handle an incoming SMD fabric msg.
 *
 *  Release using "as_smd_destroy_event()".
 */
static as_smd_event_t *
as_smd_old_create_msg_event(as_smd_msg_op_t op, cf_node node_id, msg *msg)
{
	as_smd_event_t *evt = NULL;
	int e = 0;

	// Allocate an event object and initialize it as a msg.
	evt = (as_smd_event_t *) cf_calloc(1, sizeof(as_smd_event_t));
	evt->type = AS_SMD_MSG;
	as_smd_msg_t *smd_msg = &(evt->u.msg);

	smd_msg->op = op;
	smd_msg->node_id = node_id;

	if ((e = msg_get_uint64(msg, AS_SMD_MSG_CLUSTER_KEY, &(smd_msg->cluster_key)))) {
		cf_warning(AS_SMD, "failed to get cluster key from System Metadata fabric msg (err %d)", e);
		cf_free(evt);
		return 0;
	}

	if ((e = msg_get_str(msg, AS_SMD_MSG_MODULE_NAME, &(smd_msg->module_name), 0, MSG_GET_COPY_MALLOC))) {
		cf_debug(AS_SMD, "failed to get module name from System Metadata fabric msg (err %d)", e);
	}

	if (msg_get_uint32(msg, AS_SMD_MSG_NUM_ITEMS, &smd_msg->num_items) != 0) {
		if (! smd_new_create_msg_event(smd_msg, node_id, msg)) {
			as_smd_destroy_event(evt);
			return NULL;
		}

		return evt;
	}

	as_smd_destroy_event(evt);
	return NULL;
}


/* Memory release functions for object types passed to the callback functions. */


/*
 *  Release a reference-counted metadata item.
 *  (Note:  This is *not* a public API.)
 */
static void as_smd_item_destroy(as_smd_item_t *item)
{
	if (item) {
		if (!cf_rc_release(item)) {
			RELEASE_ITEM_MEMBERS(item);
			cf_rc_free(item);
		}
	}
}

/*
 *  Allocate an empty list of to contain metadata items.
 *  (Note:  This is *not* a public API.)
 */
static as_smd_item_list_t *as_smd_item_list_alloc(size_t num_items)
{
	as_smd_item_list_t *item_list = (as_smd_item_list_t *)
			cf_malloc(sizeof(as_smd_item_list_t) + num_items * sizeof(as_smd_item_t *));

	item_list->num_items = num_items;
	memset(item_list->item, 0, num_items * sizeof(as_smd_item_t *));

	return item_list;
}

/*
 *  Create an empty list of reference-counted metadata items.
 *  (Note:  This is a public API for creating merge callback function arguments.)
 */
as_smd_item_list_t *as_smd_item_list_create(size_t num_items)
{
	as_smd_item_list_t *item_list = as_smd_item_list_alloc(num_items);

	// Use num_items to count the number of successfully allocated items.
	item_list->num_items = 0;
	for (int i = 0; i < num_items; i++) {
		item_list->item[i] = (as_smd_item_t *) cf_rc_alloc(sizeof(as_smd_item_t));
		memset(item_list->item[i], 0, sizeof(as_smd_item_t));
		item_list->num_items++;
	}

	return item_list;
}

/*
 *  Release a list of reference-counted metadata items.
 *  (Note:  This is a public API for releasing merge callback function arguments.)
 */
void as_smd_item_list_destroy(as_smd_item_list_t *items)
{
	if (items) {
		for (int i = 0; i < items->num_items; i++) {
			as_smd_item_destroy(items->item[i]);
			items->item[i] = NULL;
		}
		cf_free(items);
	}
}

/*
 *  Release a System Metadata event object (either a cmd or a msg.)
 */
static void as_smd_destroy_event(as_smd_event_t *evt)
{
	if (evt) {
		if (AS_SMD_CMD == evt->type) {
			as_smd_cmd_t *cmd = &(evt->u.cmd);

			// Give back the item reference if necessary.
			as_smd_item_destroy(cmd->item);
			cmd->item = NULL;
		} else if (AS_SMD_MSG == evt->type) {
			as_smd_msg_t *msg = &(evt->u.msg);

			// Release the module name.
			if (msg->module_name) {
				cf_free(msg->module_name);
				msg->module_name = NULL;
			}

			// Release the msg item list.
			as_smd_item_list_destroy(msg->items);
			msg->num_items = 0;
			msg->items = NULL;
		} else {
			cf_warning(AS_SMD, "not destroying unknown type of System Metadata event (%d)", evt->type);
			return;
		}

		// Release the event itself.
		cf_free(evt);
	} else {
		cf_warning(AS_SMD, "not freeing NULL System Metadata event");
	}
}

/*
 *  Send an event to the System Metadata thread via the message queue.
 */
static int as_smd_send_event(as_smd_t *smd, as_smd_event_t *evt)
{
	if (!smd) {
		cf_warning(AS_SMD, "System Metadata is not initialized ~~ Not sending event!");
		as_smd_destroy_event(evt);
		return -1;
	}

	cf_queue_push(smd->msgq, &evt);

	return 0;
}


/* System Metadata Module Init / Start / Shutdown API */


/*
 *  Free a module object from the modules rchash table.
 */
static void modules_rchash_destructor_fn(void *object)
{
	as_smd_module_t *module_obj = (as_smd_module_t *) object;

	cf_debug(AS_SMD, "mrdf(%p) [module \"%s\"] called!", object, module_obj->module);

	// Ensure that the module's callbacks cannot be called again.
	module_obj->merge_cb = module_obj->merge_udata = NULL;
	module_obj->conflict_cb = module_obj->conflict_udata = NULL;
	module_obj->accept_cb = module_obj->accept_udata = NULL;
	module_obj->can_accept_cb = module_obj->can_accept_udata = NULL;

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	// Free the module's name.
	CF_FREE_AND_NULLIFY(module_obj->module);

	// Free both of the module's metadata hash tables.
	cf_rchash_destroy(module_obj->my_metadata);
	cf_rchash_destroy(module_obj->external_metadata);
}

/*
 *  Free a metadata item from the metadata rchash table.
 */
static void metadata_rchash_destructor_fn(void *object)
{
	as_smd_item_t *item = (as_smd_item_t *) object;

	cf_debug(AS_SMD, "mdrdf(%p) [key \"%s\"] called!", object, item->key);

	// Free up the members of the item.
	RELEASE_ITEM_MEMBERS(item);
}

/*
 *  Handle a cluster state change event notification from as_exchange.
 */
static void as_smd_cluster_state_changed_fn(const as_exchange_cluster_changed_event *event, void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;

	cf_debug(AS_SMD, "Received cluster state changed event!");

	size_t succession_size = event->cluster_size * sizeof(cf_node);
	cf_node *succession = cf_malloc(succession_size);

	memcpy(succession, event->succession, succession_size);

	// Send a Cluster Changed command to the System Metadata thread.
	as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_CLUSTER_CHANGED, event->cluster_key, event->cluster_size, succession));
}

/*
 *  Create and initialize a System Metadata module. (Local method for now.)
 */
static as_smd_t *as_smd_create(void)
{
	as_smd_t *smd = (as_smd_t *) cf_calloc(1, sizeof(as_smd_t));

	// Go to the not yet initialized state.
	smd->state = AS_SMD_STATE_IDLE;

	// Create the System Metadata modules hash table.
	cf_rchash_create(&(smd->modules), cf_rchash_fn_fnv32, modules_rchash_destructor_fn, 0, 127, CF_RCHASH_BIG_LOCK);

	// Create the scoreboard hash table.
	smd->scoreboard = cf_shash_create(cf_shash_fn_ptr, sizeof(cf_node), sizeof(cf_shash *), 127, CF_SHASH_BIG_LOCK);

	// Create the System Metadata message queue.
	smd->msgq = cf_queue_create(sizeof(as_smd_event_t *), true);

	cf_queue_init(&smd->pending_merge_queue, sizeof(smd_pending_merge), 128, false);

	// Create the System Metadata thread.

	if (pthread_attr_init(&(smd->thr_attr))) {
		cf_crash(AS_SMD, "failed to initialize the System Metadata thread attributes");
	}

	if (pthread_create(&(smd->thr_id), &(smd->thr_attr), as_smd_thr, smd)) {
		cf_crash(AS_SMD, "failed to create the System Metadata thread");
	}

	// Send an INIT message to the System Metadata thread.
	if (as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_INIT))) {
		cf_crash(AS_SMD, "failed to send INIT message to System Metadata thread");
	}

	return smd;
}

/*
 *  Initialize the single global System Metadata module.
 */
as_smd_t *as_smd_init(void)
{
	// This is here only because we happen to use the absence of the old
	// sindex SMD files as proof of a proper live jump from v3 to v5. We'll
	// need to keep this around for a long time - perhaps move it to a
	// better place when SMD is overhauled.

	char smd_path[MAX_PATH_LEN];
	char smd_save_path[MAX_PATH_LEN];

	snprintf(smd_path, MAX_PATH_LEN, "%s/smd/%s.smd", g_config.work_directory, OLD_SINDEX_MODULE);
	snprintf(smd_save_path, MAX_PATH_LEN, "%s.save", smd_path);

	struct stat buf;
	bool both_gone =
			stat(smd_path, &buf) != 0 && errno == ENOENT &&
			stat(smd_save_path, &buf) != 0 && errno == ENOENT;

	if (! both_gone) {
		cf_crash_nostack(AS_SMD,
				"Aerospike server was not properly switched to paxos-protocol v5 - "
				"see Aerospike documentation http://www.aerospike.com/docs/operations/upgrade/cluster_to_3_13");
	}

	if (! g_smd) {
		g_smd = as_smd_create();
	} else {
		cf_warning(AS_SMD, "System Metadata is already initialized");
	}

	return g_smd;
}

/*
 *  Convert an incoming fabric message into the corresponding msg event and post it to the System Metadata message queue.
 */
static int as_smd_msgq_push(cf_node node_id, msg *msg, void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;

	cf_debug(AS_SMD, "asmp():  Receiving a System Metadata message from node %016lX", node_id);

	// Make sure System Metadata is running before processing msg.
	if (smd && smd->state != AS_SMD_STATE_RUNNING) {
		cf_warning(AS_SMD, "System Metadata not initialized ~~ Ignoring incoming fabric msg!");
		return -1;
	}

	// Verify the System Metadata fabric protocol version.
	uint32_t version;
	int e = msg_get_uint32(msg, AS_SMD_MSG_ID, &version);
	if (0 > e) {
		cf_warning(AS_SMD, "failed to get protocol version from System Metadata fabric msg");
		return -1;
	} else if (AS_SMD_MSG_V2_IDENTIFIER != version) {
		cf_warning(AS_SMD, "received System Metadata fabric msg for unknown protocol version (read: %d ; expected: %d) ~~ Ignoring message!",
				   version, AS_SMD_MSG_V2_IDENTIFIER);
		return -1;
	}

	// Extract the operation from the incoming fabric msg.
	uint32_t op = 0;
	msg_get_uint32(msg, AS_SMD_MSG_OP, &op);

	cf_debug(AS_SMD, "Operation received %s", AS_SMD_MSG_OP_NAME(op));

	// Create a System Metadata msg event object and populate it from the fabric msg.
	as_smd_event_t *evt = as_smd_old_create_msg_event(op, node_id, msg);

	cf_assert(evt, AS_SMD, "failed to create a System Metadata msg event");

	// Send the msg event to the System Metadata thread.
	return as_smd_send_event(smd, evt);
}

/*
 *  Receiver function for System Metadata fabric transactions.
 */
static int as_smd_transact_recv_fn(cf_node node_id, msg *msg, void *transact_data, void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;
	int retval = 0;

	cf_debug(AS_SMD, "astrf():  node %016lX (%s) received SMD transaction from node %016lX (%s)",
			 g_config.self_node, (as_smd_principal() == g_config.self_node ? "SMD principal" : "regular node"),
			 node_id, (as_smd_principal() == node_id ? "SMD principal" : "regular node"));

	// Send the received msg to the System Metadata thread.
	if ((retval = as_smd_msgq_push(node_id, msg, smd))) {
		cf_warning(AS_SMD, "failed to push received transact msg (retval %d)", retval);
	}

	// Complete the transaction by replying to the received msg.
	msg_reset(msg);
	as_fabric_transact_reply(msg, transact_data);

	return retval;
}

/*
 *  Start the System Metadata module to begin receiving cluster state change events.
 */
int as_smd_start(as_smd_t *smd)
{
	// Register System Metadata fabric transact message type.
	if (as_fabric_transact_register(M_TYPE_SMD, as_smd_msg_template,
			sizeof(as_smd_msg_template), AS_SMD_MSG_SCRATCH_SIZE,
			as_smd_transact_recv_fn, smd)) {
		cf_crash(AS_SMD, "Failed to register System Metadata fabric transact msg type!");
	}

	// Register to receive cluster state changed events.
	as_exchange_register_listener(as_smd_cluster_state_changed_fn, (void *)smd);

	// Send a START message to the System Metadata thread.
	int retval = 0;
	if ((retval = as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_START)))) {
		cf_crash(AS_SMD, "failed to send START message to System Metadata thread");
	}

	return retval;
}

/*
 *  Terminate the System Metadata module.
 */
int as_smd_shutdown(as_smd_t *smd)
{
	// Send a SHUTDOWN message to the System Metadata thread.
	return as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_SHUTDOWN));
}


/*
 *  Public System Metadata Manipulation API Functions:
 *   These functions are executed in the context of a module using System Metadata.
 */


/*
 *  Create a container for the named module's metadata and register the policy callback functions.
 *  (Pass a NULL callback function pointer to select the default policy.)
 */
int as_smd_create_module(char *module,
						 as_smd_merge_cb merge_cb, void *merge_udata,
						 as_smd_conflict_cb conflict_cb, void *conflict_udata,
						 as_smd_accept_cb accept_cb, void *accept_udata,
						 as_smd_can_accept_cb can_accept_cb, void *can_accept_udata)
{
	// Send a CREATE command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_CREATE_MODULE, module,
							 merge_cb, merge_udata, conflict_cb, conflict_udata,
							 accept_cb, accept_udata, can_accept_cb, can_accept_udata));
}

/*
 *  Destroy the container for the named module's metadata, releasing all of its metadata.
 */
int as_smd_destroy_module(char *module)
{
	// Send a DESTROY command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_DESTROY_MODULE, module));
}

/*
 *  Add a new, or modify an existing, metadata item in an existing module.
 */
int as_smd_set_metadata(char *module, char *key, char *value)
{
	// Send an SET command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_SET_METADATA, module, key, value, 0, 0UL));
}

/*
 *  Add a new, or modify an existing, metadata item (with generation and timestamp) in an existing module.
 *  (Note:  This is an internal-only function, not available via the public SMD API.)
 */
int as_smd_set_metadata_gen_ts(char *module, char *key, char *value, uint32_t generation, uint64_t timestamp)
{
	// Send an SET command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_SET_METADATA, module, key, value, generation, timestamp));
}

/*
 *  Delete an existing metadata item from an existing module.
 */
int as_smd_delete_metadata(char *module, char *key)
{
	// Send a DELETE command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_DELETE_METADATA, module, key));
}

/*
 *  Retrieve metadata item(s.) (Pass NULL for module and/or key for "all".)
 */
int as_smd_get_metadata(char *module, char *key, as_smd_get_cb cb, void *udata)
{
	// Send a GET command to the System Metadata thread.
	return as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_GET_METADATA, module, key, cb, udata));
}


/*
 *  Info Command Functions:
 *   These functions are executed in the context of the Info system.
 */


/*
 *  Reduce function to print a single metadata item.
 */
static int as_smd_metadata_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;

	cf_info(AS_SMD, "%016lX\t\"%s\"\t\"%s\"\t\"%s\"\t%u\t\t%lu", item->node_id, item->module_name, item->key, item->value, item->generation, item->timestamp);

	return 0;
}

/*
 *  Reduce function to print info. about a single System Metadata module.
 */
static int as_smd_dump_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	const char *module = (const char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	int *module_num = (int *) udata;
	int num_items = 0;

	cf_info(AS_SMD, "Module %d: \"%s\" [\"%s\"]: ", *module_num++, module, module_obj->module);
	cf_info(AS_SMD, "merge cb: %p", module_obj->merge_cb);
	cf_info(AS_SMD, "merge udata: %p", module_obj->merge_udata);
	cf_info(AS_SMD, "conflict cb: %p", module_obj->conflict_cb);
	cf_info(AS_SMD, "conflict udata: %p", module_obj->conflict_udata);
	cf_info(AS_SMD, "accept cb: %p", module_obj->accept_cb);
	cf_info(AS_SMD, "accept udata: %p", module_obj->accept_udata);
	cf_info(AS_SMD, "can accept cb: %p", module_obj->can_accept_cb);
	cf_info(AS_SMD, "can accept udata: %p", module_obj->can_accept_udata);

	cf_info(AS_SMD, "My Metadata:");
	cf_info(AS_SMD, "number of metadata items: %d", num_items = cf_rchash_get_size(module_obj->my_metadata));
	if (num_items) {
		cf_info(AS_SMD, "Node ID\t\tModule\tKey\tValue\t\tGeneration\tTimestamp");
		cf_rchash_reduce(module_obj->my_metadata, as_smd_metadata_reduce_fn, NULL);
	}

	cf_info(AS_SMD, "External Metadata:");
	cf_info(AS_SMD, "number of metadata items: %d", num_items = cf_rchash_get_size(module_obj->external_metadata));
	if (num_items) {
		cf_info(AS_SMD, "Node ID\t\tModule\tKey\tValue\t\tGeneration\tTimestamp");
		cf_rchash_reduce(module_obj->external_metadata, as_smd_metadata_reduce_fn, NULL);
	}

	return 0;
}

/*
 *  Print info. about the System Metadata state to the log.
 *  (Verbose event option prints detailed info. about the metadata values.)
 */
void as_smd_dump_metadata(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	// Print info. about the System Metadata system.
	cf_info(AS_SMD, "System Metadata Status:");
	cf_info(AS_SMD, "-----------------------");
	cf_info(AS_SMD, "thr_id: 0x%lx", smd->thr_id);
	cf_info(AS_SMD, "thr_attr: %p", &smd->thr_attr);
	cf_info(AS_SMD, "state: %s", AS_SMD_STATE_NAME(smd->state));
	cf_info(AS_SMD, "number of modules: %d", cf_rchash_get_size(smd->modules));
	cf_info(AS_SMD, "number of pending messages in queue: %d", cf_queue_sz(smd->msgq));

	// If verbose, dump info. about the metadata itself.
	if (cmd->options & AS_SMD_CMD_OPT_VERBOSE) {
		int module_num = 0;
		cf_rchash_reduce(smd->modules, as_smd_dump_reduce_fn, &module_num);
	}
}

/*
 *  Print info. about the System Metadata state to the log.
 *  (Verbose true prints detailed info. about the metadata values.)
 */
void as_smd_dump(bool verbose)
{
	// Send an INTERNAL + DUMP_SMD + verbosity command to the System Metadata thread.
	as_smd_send_event(g_smd, as_smd_create_cmd_event(AS_SMD_CMD_INTERNAL,
					  (AS_SMD_CMD_OPT_DUMP_SMD | (verbose ? AS_SMD_CMD_OPT_VERBOSE : 0))));
}

/*
 *  Callback used to receive System Metadata items requested via the Info SMD "get" command.
 */
static int as_smd_info_get_fn(char *module, as_smd_item_list_t *items, void *udata)
{
	for (int i = 0; i < items->num_items; i++) {
		as_smd_item_t *item = items->item[i];
		cf_info(AS_SMD, "SMD Info get metadata item[%d]:  module \"%s\" ; key \"%s\" ; value \"%s\" ; generation %u ; timestamp %lu",
				i, item->module_name, item->key, item->value, item->generation, item->timestamp);
	}

	return 0;
}

/*
 *  Manipulate the System Metadata and log the result.
 */
void as_smd_info_cmd(char *cmd, cf_node node_id, char *module, char *key, char *value)
{
	int retval = 0;

	// Invoke the appropriate System Metadata API function.

	if (!strcmp(cmd, "create")) {
		if ((retval = as_smd_create_module(module, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))) {
			cf_warning(AS_SMD, "System Metadata create module \"%s\" failed (retval %d)", module, retval);
		}
	} else if (!strcmp(cmd, "destroy")) {
		if ((retval = as_smd_destroy_module(module))) {
			cf_warning(AS_SMD, "System Metadata destroy module \"%s\" failed (retval %d)", module, retval);
		}
	} else if (!strcmp(cmd, "set")) {
		if (((retval = as_smd_set_metadata(module, key, value)))) {
			cf_warning(AS_SMD, "System Metadata set item: module: \"%s\" key: \"%s\" value: \"%s\" failed (retval %d)", module, key, value, retval);
		}
	} else if (!strcmp(cmd, "delete")) {
		if (((retval = as_smd_delete_metadata(module, key)))) {
			cf_warning(AS_SMD, "System Metadata delete item: module: \"%s\" key: \"%s\" failed (retval %d)", module, key, retval);
		}
	} else if (!strcmp(cmd, "get")) {
		if ((retval = as_smd_get_metadata(module, key, as_smd_info_get_fn, NULL))) {
			cf_warning(AS_SMD, "System Metadata get node: %016lX module: \"%s\" key: \"%s\" failed (retval %d)", node_id, module, key, retval);
		}
	} else if (!strcmp(cmd, "init")) {
		as_smd_init();
	} else if (!strcmp(cmd, "start")) {
		if (g_smd) {
			if ((retval = as_smd_start(g_smd))) {
				cf_warning(AS_SMD, "System Metadata start up failed (retval %d)", retval);
			}
		} else {
			cf_warning(AS_SMD, "System Metadata is not initialized");
		}
	} else if (!strcmp(cmd, "shutdown")) {
		if (g_smd) {
			as_smd_shutdown(g_smd);
		} else {
			cf_warning(AS_SMD, "System Metadata is not initialized");
		}
	} else {
		cf_warning(AS_SMD, "unknown System Metadata command: \"%s\"", cmd);
	}
}


/*
 *  System Metadata Internals:
 *   These functions are executed in the context of the System Metadata thread,
 *   except for the fabric callbacks.
 */


/* Metadata persistence functions. */


/*
 *  Read in metadata for the given module from the standard location.
 *  Return:  0 if successful, -1 otherwise.
 */
static int as_smd_read(char *module, json_t **module_smd)
{
	int retval = 0;
	json_t *root = NULL;

	char smd_path[MAX_PATH_LEN];
	size_t load_flags = JSON_REJECT_DUPLICATES;
	json_error_t json_error;

	snprintf(smd_path, MAX_PATH_LEN, "%s/smd/%s.smd", g_config.work_directory, module);

	// Check if the persisted metadata file exists before attempting to read it.
	struct stat buf;
	if (!stat(smd_path, &buf)) {
		if (!(root = json_load_file(smd_path, load_flags, &json_error))) {
			cf_warning(AS_SMD, "failed to load System Metadata for module \"%s\" from file \"%s\" with JSON error: %s ; source: %s ; line: %d ; column: %d ; position: %d",
					   module, smd_path, json_error.text, json_error.source, json_error.line, json_error.column, json_error.position);
			retval = -1;
		}
	} else {
		cf_debug(AS_SMD, "failed to read persisted System Metadata file \"%s\" for module \"%s\": %s (%d)", smd_path, module, cf_strerror(errno), errno);
	}

	if (module_smd) {
		*module_smd = root;
	}

	return retval;
}

/*
 *  Write out metadata for the given module to the the standard location.
 *  Return:  0 if successful, -1 otherwise.
 *
 *  Note:  Any pre-existing file will be saved prior to write for
 *          manual recovery in case of system failure.
 */
static int as_smd_write(char *module, json_t *module_smd)
{
	int retval = 0;

	char smd_path[MAX_PATH_LEN];
	char smd_save_path[MAX_PATH_LEN];
	size_t dump_flags = JSON_INDENT(3) | JSON_ENSURE_ASCII | JSON_PRESERVE_ORDER;

	snprintf(smd_path, MAX_PATH_LEN, "%s/smd/%s.smd", g_config.work_directory, module);
	snprintf(smd_save_path, MAX_PATH_LEN, "%s.save", smd_path);

	if (json_dump_file(module_smd, smd_save_path, dump_flags) < 0) {
		cf_warning(AS_SMD, "failed to dump System Metadata for module \"%s\" to file \"%s\": %s (%d)", module, smd_path, cf_strerror(errno), errno);
		return -1;
	}

	if (rename(smd_save_path, smd_path) != 0) {
		cf_warning(AS_SMD, "error on renaming existing metadata file \"%s\": %s (%d)", smd_save_path, cf_strerror(errno), errno);
		return -1;
	}

	return retval;
}

/*
 *  Load persisted System Metadata for the given module:
 *    Read the module's JSON file (if it exists) and add each metadata found therein.
 *    Return:   The number of metadata items restored (which may be 0) if reading
 *               the metadata file was successful, -1 otherwise.
 */
static int as_smd_module_restore(as_smd_module_t *module_obj)
{
	int retval = 0;

	// Load the module's metadata (if persisted.)
	if ((retval = as_smd_read(module_obj->module, &(module_obj->json)))) {
		cf_warning(AS_SMD, "failed to read persisted System Metadata for module \"%s\"", module_obj->module);
		return -1;
	}

	size_t num_items = json_array_size(module_obj->json);
	for (int i = 0; i < num_items; i++) {
		json_t *json_item = json_array_get(module_obj->json, i);

		if (!json_is_object(json_item)) {
			// Warn and skip the bad item.
			cf_warning(AS_SMD, "non-JSON object %d of type %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, json_typeof(json_item), module_obj->module);
			continue;
		}

		size_t num_fields = json_object_size(json_item);
		if (5 != num_fields) {
			// Warn if the item doesn't have the right number of fields.
			cf_warning(AS_SMD, "wrong number of fields %zu (expected 5) for object %d in persisted System Metadata for module \"%s\"", num_fields, i, module_obj->module);
		}

		char *module = (char *) json_string_value(json_object_get(json_item, "module"));
		if (!module) {
			cf_warning(AS_SMD, "missing \"module\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		} else if (strcmp(module_obj->module, module)) {
			cf_warning(AS_SMD, "incorrect module \"%s\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", module, i, module_obj->module);
			continue;
		}

		char *key = (char *) json_string_value(json_object_get(json_item, "key"));
		if (!key) {
			cf_warning(AS_SMD, "missing \"key\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		}

		char *value = (char *) json_string_value(json_object_get(json_item, "value"));
		if (!value) {
			cf_warning(AS_SMD, "missing \"value\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		}

		// [Note:  Should really use uint32_t, but Jansson integers are longs.]
		uint64_t generation = 1;
		json_t *generation_obj = json_object_get(json_item, "generation");
		if (!generation_obj) {
			cf_warning(AS_SMD, "missing \"generation\" for object %d in persisted System Metadata for module \"%s\" ~~ Using 1!", i, module_obj->module);
		} else {
			if (0 == (generation = json_integer_value(generation_obj))) {
				cf_warning(AS_SMD, "bad \"generation\" for object %d in persisted System Metadata for module \"%s\" ~~ Using 1!", i, module_obj->module);
				generation = 1;
			}
		}

		uint64_t timestamp = cf_getms();
		json_t *timestamp_obj = json_object_get(json_item, "timestamp");
		if (!timestamp_obj) {
			cf_warning(AS_SMD, "missing \"timestamp\" for object %d in persisted System Metadata for module \"%s\" ~~ Using now!", i, module_obj->module);
		} else {
			if (0 == (timestamp = json_integer_value(timestamp_obj))) {
				cf_warning(AS_SMD, "bad \"timestamp\" for object %d in persisted System Metadata for module \"%s\" ~~ Using now!", i, module_obj->module);
				timestamp = cf_getms();
			}
		}

		// Send the item metadata add command.
		as_smd_set_metadata_gen_ts(module, key, value, generation, timestamp);

		// Another metadata item was successfully restored.
		retval++;
	}

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	return retval;
}

/*
 *  Serialize a single metadata item into a JSON object and add it to the array passed in via "udata".
 */
static int as_smd_serialize_into_json_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	json_t *array = (json_t *) udata;
	json_t *metadata_obj = NULL;

	// Create an empty JSON object to hold the
	if (!(metadata_obj = json_object())) {
		cf_warning(AS_SMD, "failed to create JSON object to serialize metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
		return 0;
	}

	// Add each of the item's properties to the JSON object.
	int e = 0;
	e += json_object_set_new(metadata_obj, "module", json_string(item->module_name));
	e += json_object_set_new(metadata_obj, "key", json_string(item->key));
	e += json_object_set_new(metadata_obj, "value", json_string(item->value));
	e += json_object_set_new(metadata_obj, "generation", json_integer(item->generation));
	e += json_object_set_new(metadata_obj, "timestamp", json_integer(item->timestamp));

	if (e) {
		cf_warning(AS_SMD, "failed to serialize fields of metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
	} else {
		if (json_array_append_new(array, metadata_obj)) {
			cf_warning(AS_SMD, "failed to add to array metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
		}
	}

	return 0;
}

/*
 *  Store persistently System Metadata for the given module:
 *    Convert each of the module's metadata items into a JSON object and write an array of the results to the module's JSON file.
 */
static int as_smd_module_persist(as_smd_module_t *module_obj)
{
	int retval = 0;

	// Avoid unnecessary writes.
	if (!module_obj->dirty) {
		return retval;
	}

	if (module_obj->json) {
		cf_warning(AS_SMD, "module \"%s\" JSON is unexpectedly non-NULL (rc %zu) ~~ Nulling!", module_obj->module, module_obj->json->refcount);
		json_decref(module_obj->json);
		module_obj->json = NULL;
	}

	// Create an empty JSON array.
	if (!(module_obj->json = json_array())) {
		cf_warning(AS_SMD, "failed to create JSON array for persisting module \"%s\"", module_obj->module);
		return -1;
	}

	// Walk the module's metadata hash table and create a JSON array of objects, one for each item.
	cf_rchash_reduce(module_obj->my_metadata, as_smd_serialize_into_json_reduce_fn, module_obj->json);

	// Store the module's metadata persistently if necessary.
	if (module_obj->json && (retval = as_smd_write(module_obj->module, module_obj->json))) {
		cf_warning(AS_SMD, "failed to write persisted System Metadata file for module \"%s\"", module_obj->module);
		retval = -1;
	} else {
		// The module's SMD has been persisted.
		module_obj->dirty = false;
	}

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	return retval;
}

/*
 *  Create a metadata container for the given module.
 */
static int as_smd_module_create(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	as_smd_module_t *module_obj;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - creating module \"%s\"", item->module_name);

	// Verify the module does not yet exist.
	if (CF_RCHASH_OK == (retval = cf_rchash_get(smd->modules, item->module_name, strlen(item->module_name) + 1, (void **) &module_obj))) {
		// (Note:  This is not a problem ~~ May have come over the wire.)
		cf_detail(AS_SMD, "System Metadata module \"%s\" already exists", item->module_name);

		// Give back the reference.
		cf_rc_release(module_obj);

		return retval;
	}

	// Create the module object.
	// [NB:  Reference-counted for insertion in modules "rchash" table.]
	module_obj = (as_smd_module_t *) cf_rc_alloc(sizeof(as_smd_module_t));
	memset(module_obj, 0, sizeof(as_smd_module_t));

	// Set the module's name.
	module_obj->module = cf_strdup(item->module_name);

	// Create the module's local metadata hash table.
	cf_rchash_create(&(module_obj->my_metadata), cf_rchash_fn_fnv32, metadata_rchash_destructor_fn, 0, 127, CF_RCHASH_BIG_LOCK);

	// Create the module's external metadata hash table.
	cf_rchash_create(&(module_obj->external_metadata), cf_rchash_fn_fnv32, metadata_rchash_destructor_fn, 0, 127, CF_RCHASH_BIG_LOCK);

	// Add the module to the modules hash table.
	if (CF_RCHASH_OK != (retval = cf_rchash_put_unique(smd->modules, item->module_name, strlen(item->module_name) + 1, module_obj))) {
		cf_crash(AS_SMD, "failed to add System Metadata module \"%s\" to modules table (retval %d)", item->module_name, retval);
	}

	// Set the callback functions and their respective user data.
	module_obj->merge_cb = cmd->a;
	module_obj->merge_udata = cmd->b;
	module_obj->conflict_cb = cmd->c;
	module_obj->conflict_udata = cmd->d;
	module_obj->accept_cb = cmd->e;
	module_obj->accept_udata = cmd->f;
	module_obj->can_accept_cb = cmd->g;
	module_obj->can_accept_udata = cmd->h;

	int num_items = as_smd_module_restore(module_obj);
	if (0 > num_items) {
		cf_warning(AS_SMD, "failed to restore persisted System Metadata for module \"%s\"", item->module_name);
	}

	// Set an empty metadata item, signifying the completion of module creation,
	//  including the restoration of zero or more persisted metadata items.
	//  (Will trigger an Accept callback with the OPT_CREATE accept option.)
	if ((retval = as_smd_set_metadata(module_obj->module, NULL, NULL))) {
		cf_warning(AS_SMD, "failed to send SMD module \"%s\" creation complete event", module_obj->module);
	}

	return retval;
}

/*
 *  Find or create a System Metadata module object.
 *  The name if the module can be at two places
 *  1. With each item
 *  2. At the item_list level
 *
 *  First preference is to get the information from the specific item
 *  If the item is NULL, get the information from the item_list.
 */
static as_smd_module_t *
as_smd_module_get(as_smd_t *smd, as_smd_item_t *item, as_smd_msg_t *msg)
{
	as_smd_module_t *module_obj = NULL;
	int retval = 0;

	char *module_name = NULL;

	// First check for a given message with the module name set.
	if (msg && msg->module_name) {
		cf_debug(AS_SMD, "asmg():  Name of module from message: \"%s\"", module_name);
		module_name = msg->module_name;
	}
	else if (item && item->module_name) {
		// Next, see if an item is passed and it has module name set.  This takes precedence.
		module_name = item->module_name;
		cf_debug(AS_SMD, "asmg():  Name of module from the item: \"%s\"", module_name);
	}
	else {
		// If the message, item, and item_list are NULL, we cannot do anything.
		cf_debug(AS_SMD, "asmg():  No module name found!");
		return NULL;
	}

	if (CF_RCHASH_OK != (retval = cf_rchash_get(smd->modules, module_name, strlen(module_name) + 1, (void **) &module_obj))) {
		as_smd_cmd_t cmd;
		as_smd_item_t fakeitem;
		// Could not find the module object corresponding to the module name. Create one.
		// Note:  No policy callback will be set if the module is created on-the-fly.
		//
		// Ideally, we should not land into this situation at all.
		// All the legal module objects should get created upfront
		// TODO : Should we not throw a warning/crash here and not create a new module ???
		memset(&cmd, 0, sizeof(as_smd_cmd_t));
		fakeitem.module_name = module_name;	// Only the module name is used. All the callback pointers will be NULL.
		cmd.type = AS_SMD_CMD_CREATE_MODULE;
		cmd.item = &fakeitem;
		if ((retval = as_smd_module_create(smd, &cmd))) {
			cf_warning(AS_SMD, "failed to create System Metadata module \"%s\" (rv %d)", module_name, retval);
		} else {
			cf_debug(AS_SMD, "created System Metadata module \"%s\" on-the-fly", module_name);

			if (CF_RCHASH_OK != (retval = cf_rchash_get(smd->modules, module_name, strlen(module_name) + 1, (void **) &module_obj))) {
				cf_crash(AS_SMD, "failed to get System Metadata module \"%s\" after creation (rv %d)", module_name, retval);
			}
		}
	}

	return module_obj;
}

/*
 *  Destroy a metadata container for the given module after releasing all contained metadata.
 */
static int as_smd_module_destroy(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - destroying module \"%s\"", item->module_name);

	// Remove the module's object from the hash table.
	if (CF_RCHASH_OK != (retval = cf_rchash_delete(smd->modules, item->module_name, strlen(item->module_name) + 1))) {
		cf_warning(AS_SMD, "failed to delete System Metadata module \"%s\" (retval %d)", item->module_name, retval);
		return retval;
	}

	return retval;
}

static void
smd_msg_fill_items(msg *m, as_smd_item_t **items, uint32_t num_items,
		cf_vector *key_vec, cf_vector *value_vec, uint32_t *gen_list)
{
	uint32_t value_count = 0;

	msg_set_uint64_array_size(m, AS_SMD_MSG_TIMESTAMP, num_items);

	for (uint32_t i = 0; i < num_items; i++) {
		msg_buf_ele key_ele = {
				.sz = (uint32_t)strlen(items[i]->key),
				.ptr = (uint8_t *)items[i]->key
		};

		cf_vector_append(key_vec, &key_ele);

		msg_buf_ele value_ele = {
				.ptr = (uint8_t *)items[i]->value
		};

		if (items[i]->value) {
			value_ele.sz = (uint32_t)strlen(items[i]->value);
			value_count++;
		}

		cf_vector_append(value_vec, &value_ele);

		gen_list[i] = items[i]->generation;
		msg_set_uint64_array(m, AS_SMD_MSG_TIMESTAMP, i, items[i]->timestamp);
	}

	msg_msgpack_list_set_buf(m, AS_SMD_MSG_KEY_LIST, key_vec);

	if (value_count != 0) {
		msg_msgpack_list_set_buf(m, AS_SMD_MSG_VALUE_LIST, value_vec);
	}

	msg_msgpack_list_set_uint32(m, AS_SMD_MSG_GEN_LIST, gen_list, num_items);
}

// New message protocol.
static msg *
smd_create_msg(as_smd_msg_op_t op, as_smd_item_t **items, uint32_t num_items,
		const char *module_name, uint32_t accept_opt)
{
	msg *m = as_fabric_msg_get(M_TYPE_SMD);

	msg_set_uint32(m, AS_SMD_MSG_ID, AS_SMD_MSG_V2_IDENTIFIER);
	msg_set_uint64(m, AS_SMD_MSG_CLUSTER_KEY, g_cluster_key);

	if (op == AS_SMD_MSG_OP_ACCEPT_THIS_METADATA &&
			(accept_opt & AS_SMD_ACCEPT_OPT_API) != 0) {
		op = AS_SMD_MSG_OP_SET_FROM_PR;
	}
	else if (op == AS_SMD_MSG_OP_DELETE_ITEM) {
		op = AS_SMD_MSG_OP_SET_ITEM;
	}

	msg_set_uint32(m, AS_SMD_MSG_OP, op);

	if (module_name) {
		msg_set_str(m, AS_SMD_MSG_MODULE_NAME, module_name, MSG_SET_COPY);

		// Single item optimized packing.
		if (num_items == 1) {
			msg_set_str(m, AS_SMD_MSG_SINGLE_KEY, items[0]->key,
					MSG_SET_COPY);

			if (items[0]->value) {
				msg_set_str(m, AS_SMD_MSG_SINGLE_VALUE, items[0]->value,
						MSG_SET_COPY);
			}

			if (items[0]->generation != 0) {
				msg_set_uint32(m, AS_SMD_MSG_SINGLE_GENERATION,
						items[0]->generation);
			}

			if (items[0]->timestamp != 0) {
				msg_set_uint64(m, AS_SMD_MSG_SINGLE_TIMESTAMP,
						items[0]->timestamp);
			}

			return m;
		}
	}

	if (num_items == 0) {
		return m;
	}

	if (! module_name) {
		uint32_t mod_max = cf_rchash_get_size(g_smd->modules);
		uint32_t mod_counts[mod_max];
		uint32_t count = 0;
		const char *prev = NULL;
		cf_vector_define(mod_vec, sizeof(msg_buf_ele), mod_max, 0);

		// Assume same item module names are clustered together.
		for (uint32_t i = 0; i < num_items; i++) {
			if (count != 0 && strcmp(prev, items[i]->module_name) == 0) {
				mod_counts[count - 1]++;
				continue;
			}

			msg_buf_ele ele = {
					.sz = (uint32_t)strlen(items[i]->module_name),
					.ptr = (uint8_t *)items[i]->module_name
			};

			cf_vector_append(&mod_vec, &ele);
			prev = items[i]->module_name;

			cf_assert(count < mod_max, AS_SMD, "unexpected item module name ordering");

			mod_counts[count++] = 1;
		}

		msg_msgpack_list_set_buf(m, AS_SMD_MSG_MODULE_LIST, &mod_vec);
		msg_msgpack_list_set_uint32(m, AS_SMD_MSG_MODULE_COUNTS, mod_counts,
				count);
	}

	if (num_items < SMD_MAX_STACK_NUM_ITEMS) {
		uint32_t gen_list[num_items];
		cf_vector_define(key_vec, sizeof(msg_buf_ele), num_items, 0);
		cf_vector_define(value_vec, sizeof(msg_buf_ele), num_items, 0);

		smd_msg_fill_items(m, items, num_items, &key_vec, &value_vec, gen_list);
	}
	else {
		cf_vector key_vec;
		cf_vector value_vec;
		uint32_t *gen_list = cf_malloc(sizeof(uint32_t) * num_items);

		if (cf_vector_init(&key_vec, sizeof(msg_buf_ele), num_items, 0) != 0) {
			cf_crash(AS_SMD, "cf_vector_init");
		}

		if (cf_vector_init(&value_vec, sizeof(msg_buf_ele), num_items, 0) !=
				0) {
			cf_crash(AS_SMD, "cf_vector_init");
		}

		smd_msg_fill_items(m, items, num_items, &key_vec, &value_vec, gen_list);

		cf_vector_destroy(&key_vec);
		cf_vector_destroy(&value_vec);
		cf_free(gen_list);
	}

	return m;
}

/*
 *  Get or create a new System Metadata fabric msg to perform the given operation on the given metadata items.
 */
static msg *
as_smd_msg_get(as_smd_msg_op_t op, as_smd_item_t **item, size_t num_items, const char *module_name, uint32_t accept_opt)
{
	// TODO - collapse - don't need two functions any more.
	return smd_create_msg(op, item, (uint32_t)num_items, module_name, accept_opt);
}

/*
 *  Callback for fabric transact responses, both when forwarding metadata change commands to the SMD principal
 *   and when receiving message events from the SMD principal.
 *
 *   Note:  This function is currently shared between all System Metadata transactions, which works for now
 *           since the different transaction types don't require separate completion processing.
 */
static int transact_complete_fn(msg *response, void *udata, int fabric_err)
{
//	as_smd_t *smd = (as_smd_t *) udata; // (Not used.)

	if (!response) {
		cf_warning(AS_SMD, "Null response message passed in transaction complete!");
		return -1;
	}

	as_fabric_msg_put(response);

	if (AS_FABRIC_SUCCESS != fabric_err) {
		cf_warning(AS_SMD, "System Metadata transaction failed with fabric error %d", fabric_err);
		return -1;
	}

	return 0;
}

static void
smd_fabric_send(cf_node node_id, msg *m)
{
	if (node_id == g_config.self_node) {
		as_smd_msgq_push(node_id, m, g_smd);
		as_fabric_msg_put(m);
		return;
	}

	as_fabric_transact_start(node_id, m, AS_SMD_TRANSACT_TIMEOUT_MS,
			transact_complete_fn, NULL);
}

/*
 *  Send the metadata item change message to the SMD principal.
 */
static int as_smd_proxy_to_principal(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	if (as_smd_principal() == (cf_node)0) {
		cf_warning(AS_SMD, "failed to get the SMD principal node ~~ Not proxying SMD msg");
		return -1;
	}

	msg *msg = NULL;

	cf_debug(AS_SMD, "forwarding %s metadata request to SMD principal node %016lX", AS_SMD_MSG_OP_NAME(op), as_smd_principal());

	// Get an existing (or create a new) System Metadata fabric msg for the appropriate operation and metadata item.
	size_t num_items = 1;
	if (!(msg = as_smd_msg_get(op, &item, num_items, item->module_name, AS_SMD_ACCEPT_OPT_API))) {
		cf_warning(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start for module \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name);
		return -1;
	}

	smd_fabric_send(as_smd_principal(), msg);

	return 0;
}

/*
 *  Locally change a metadata item.
 */
static int as_smd_metadata_change_local(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	int retval = 0;

	as_smd_module_t *module_obj = NULL;

	cf_debug(AS_SMD, "System Metadata thread - locally %s'ing metadata: node %016lX ; action %s ; module \"%s\" ; key \"%s\"",
			 AS_SMD_MSG_OP_NAME(op), item->node_id, AS_SMD_ACTION_NAME(item->action), item->module_name, item->key);

	// Find the module's object.
	if (CF_RCHASH_OK != (retval = cf_rchash_get(smd->modules, item->module_name, strlen(item->module_name) + 1, (void **) &module_obj))) {
		cf_warning(AS_SMD, "failed to find System Metadata module \"%s\" (retval %d)", item->module_name, retval);
		return retval;
	}

	if (AS_SMD_ACTION_DELETE == item->action) {
		// Delete the metadata from the module's local metadata hash table.
		if (CF_RCHASH_OK != (retval = cf_rchash_delete(module_obj->my_metadata, item->key, strlen(item->key) + 1))) {
			cf_warning(AS_SMD, "failed to delete key \"%s\" from System Metadata module \"%s\" (retval %d)", item->key, item->module_name, retval);
		}
	} else if (item->key) {
		// Handle the Set case:

		// Select metadata local hash table for incoming metadata.
		cf_rchash *metadata_hash = module_obj->my_metadata;

		// The length of the key string includes the NULL terminator.
		uint32_t key_len = strlen(item->key) + 1;

		// If the item is local, simply use the key string within the item.
		void *key = item->key;

		// Default to generation 1.
		if (!item->generation) {
			item->generation = 1;
		}

		// Default timestamp to now.
		if (!item->timestamp) {
			item->timestamp = cf_clepoch_milliseconds();
		}

		// Add new, replace or keep existing, metadata in the module's metadata hash table.

		as_smd_item_t *existing_item;
		bool existing_wins = false;

		if (CF_RCHASH_OK == cf_rchash_get(metadata_hash, key, key_len, (void **)&existing_item)) {
			existing_wins = (existing_item->generation > item->generation) ||
					((existing_item->generation == item->generation) &&
					 (existing_item->timestamp > item->timestamp));
			as_smd_item_destroy(existing_item);
		}

		if (! existing_wins) {
			// Add reference to item for storage in the hash table.
			// (Note:  One reference to the item will be released by the thread when it releases the containing command.)
			cf_rc_reserve(item);
			cf_rchash_put(metadata_hash, key, key_len, item);
		}
	} else {
		cf_debug(AS_SMD, "(not setting empty metadata item for module \"%s\")", module_obj->module);
	}

	// Give back the module reference.
	cf_rc_release(module_obj);

	return retval;
}

/*
 *  Handle a metadata change request by proxying to SMD principal or short-circuiting locally during node start-up.
 */
static int as_smd_metadata_change(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	int retval = 0;

	if ((AS_SMD_STATE_RUNNING == smd->state) && item->key) {
		// Forward to SMD principal.
		// [Ideally, would re-try or at least notify (via an as-yet nonexistent mechanism) upon failure.]
		return as_smd_proxy_to_principal(smd, op, item);
	} else {
		// Short-circuit to handle change locally when this node is starting up
		// or when an initially-empty module is being created, as indicated by NULL item key (and value.)

		cf_debug(AS_SMD, "handling metadata change type %s locally: module \"%s\" ; key \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name, item->key);

		if ((retval = as_smd_metadata_change_local(smd, op, item))) {
			cf_warning(AS_SMD, "failed to %s a metadata item locally: module \"%s\" ; key \"%s\" ; value \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name, item->key, item->value);
		}

		uint32_t accept_opt = AS_SMD_ACCEPT_OPT_API;
		as_smd_item_list_t *item_list = NULL;

		if (!item->key) {
			// Empty key (and value) indicates creation of an initially-empty module.
			accept_opt = AS_SMD_ACCEPT_OPT_CREATE;
		} else {
			// While restoring pass this info to the module as well.  This is needed
			// at the boot to make sure metadata init is done before the data init is done.
			item_list = as_smd_item_list_alloc(1);
			item_list->item[0] = item;
		}

		as_smd_module_t *module_obj = as_smd_module_get(smd, item, NULL);

		// At the end of module creation, SMD will be persisted.
		if (AS_SMD_ACCEPT_OPT_CREATE == accept_opt) {
			module_obj->dirty = true;
		}

		if (module_obj->accept_cb) {
			// Invoke the module's registered accept policy callback function.
			(module_obj->accept_cb)(module_obj->module, item_list, module_obj->accept_udata, accept_opt);
		}

		// Persist the accepted metadata for this module.
		if (as_smd_module_persist(module_obj)) {
			cf_warning(AS_SMD, "failed to persist accepted metadata for module \"%s\"", module_obj->module);
		}

		cf_rc_release(module_obj);

		if (item_list) {
			cf_free(item_list);
		}
	}

	return retval;
}

/*
 *  Type representing the state of a metadata get request.
 */
typedef struct as_smd_metadata_get_state_s {
	size_t num_items;                   // Number of matching items.
	as_smd_item_t *item;                // Item to compare with each item.
	as_smd_item_list_t *item_list;      // List of matching items.
	cf_rchash_reduce_fn reduce_fn;         // Reduce function to apply to matching items.
} as_smd_metadata_get_state_t;

/*
 *  Reduce function to count one metadata item.
 */
static int as_smd_count_matching_item_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;

	// Count each matching item.
	if (!strcmp(get_state->item->key, "") || !strcmp(get_state->item->key, item->key)) {
		get_state->num_items += 1;
	}

	return 0;
}

/*
 *  Reduce function to return a single metadata option, if it matches the pattern.
 */
static int as_smd_metadata_get_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;
	as_smd_item_list_t *item_list = get_state->item_list;

	// Add each matching item to the list.
	if (!strcmp(get_state->item->key, "") || !strcmp(get_state->item->key, item->key)) {
		cf_rc_reserve(item);
		item_list->item[item_list->num_items] = item;
		item_list->num_items += 1;
	}

	return 0;
}

/*
 *  Reduce function to perform a given reduce function on each matching module.
 */
static int as_smd_matching_module_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	const char *module = (const char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;

	// Perform the given reduce function on matching module's metadata.
	if (!strcmp(get_state->item->module_name, "") || !strcmp(get_state->item->module_name, module)) {
		cf_rchash_reduce(module_obj->my_metadata, get_state->reduce_fn, get_state);
	}

	return 0;
}

/*
 *  Search for metadata according to the given search criteria.
 *  The incoming item's module and/or key can be NULL to perform a wildcard match.
 */
static int as_smd_metadata_get(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - get metadata: module \"%s\" ; node %016lX ; key \"%s\"", item->module_name, item->node_id, item->key);

	// Extract the user's callback function and user data.
	as_smd_get_cb get_cb = cmd->a;
	void *get_udata = cmd->b;

	if (!get_cb) {
		cf_warning(AS_SMD, "no System Metadata get callback supplied ~~ Ignoring get metadata request!");
		return -1;
	}

	as_smd_metadata_get_state_t get_state;
	get_state.num_items = 0;
	get_state.item = item;
	get_state.item_list = NULL;
	get_state.reduce_fn = as_smd_count_matching_item_reduce_fn;

	// Count the number of matching items.
	cf_rchash_reduce(smd->modules, as_smd_matching_module_reduce_fn, &get_state);

	// Allocate a list of sufficient size for the get result.
	as_smd_item_list_t *item_list = as_smd_item_list_alloc(get_state.num_items);
	get_state.item_list = item_list;

	// (Note:  Use num_items to count the position for each metadata item.)
	item_list->num_items = 0;

	// Add matching items to the list.
	get_state.reduce_fn = as_smd_metadata_get_reduce_fn;
	cf_rchash_reduce(smd->modules, as_smd_matching_module_reduce_fn, &get_state);

	// Invoke the user's callback function.
	(get_cb)(item->module_name, item_list, get_udata);

	// Release the item list.
	as_smd_item_list_destroy(item_list);

	return retval;
}

/*
 *  Cleanly release all System Metadata resources.
 */
static void as_smd_terminate(as_smd_t *smd)
{
	cf_debug(AS_SMD, "SMD Terminate called");

	// After this is NULLed out, no more messages will be sent to the System Metadata queue.
	g_smd = NULL;

	// De-register the System Metadata fabric transact message type.
	// [Note:  Don't need to remove the handler, simply drop the msg in the handler function.]
//	as_fabric_transact_register(M_TYPE_SMD, NULL, 0, NULL, NULL);

	// Go to the not started up yet state.
	smd->state = AS_SMD_STATE_IDLE;

	// Destroy the message queue.
	cf_queue_destroy(smd->msgq);

	// Release the scoreboard hash table.
	cf_shash_destroy(smd->scoreboard);

	// Release the modules hash table.
	cf_rchash_destroy(smd->modules);

	// Release the System Metadata object.
	cf_free(smd);
}

/*
 *  Reduce function to count one metadata item.
 */
static int as_smd_count_item_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
//	as_smd_item_t *item = (as_smd_item_t *) object; // (Not used.)
	size_t *num_items = (size_t *) udata;

	*num_items += 1;

	return 0;
}

/*
 *  Reduce function to count metadata items in one module.
 */
static int as_smd_module_count_items_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	size_t *num_items = (size_t *) udata;

	// Increase the running total by the count the number of metadata items in this module.
	cf_rchash_reduce(module_obj->my_metadata, as_smd_count_item_reduce_fn, num_items);

	return 0;
}

/*
 *  Reduce function to serialize one metadata item.
 */
static int as_smd_item_serialize_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	// Add a this metadata item to the list.
	cf_rc_reserve(item);
	item_list->item[item_list->num_items] = item;
	item_list->num_items += 1;

	return 0;
}

/*
 *  Reduce function to serialize all of a module's metadata items.
 */
static int as_smd_module_serialize_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	// Serialize all of this module's metadata items.
	cf_rchash_reduce(module_obj->my_metadata, as_smd_item_serialize_reduce_fn, item_list);

	return 0;
}

static int as_smd_receive_metadata(as_smd_t *smd, as_smd_msg_t *smd_msg);

static void
smd_expire_pending_merges()
{
	if (cf_queue_sz(&g_smd->pending_merge_queue) == 0) {
		return;
	}

	smd_pending_merge item;
	uint64_t now = cf_getms();

	while (cf_queue_pop(&g_smd->pending_merge_queue, &item, CF_QUEUE_NOWAIT) ==
			CF_QUEUE_OK) {
		if (item.expire > now) {
			cf_queue_push_head(&g_smd->pending_merge_queue, &item);
			break;
		}

		cf_free(item.m.module_name);
		as_smd_item_list_destroy(item.m.items);
	}
}

static void
smd_process_pending_merges()
{
	uint64_t now = cf_getms();
	smd_pending_merge item;
	int count = cf_queue_sz(&g_smd->pending_merge_queue);

	for (int i = 0; i < count; i++) {
		cf_queue_pop(&g_smd->pending_merge_queue, &item, CF_QUEUE_NOWAIT);

		if (item.m.cluster_key == g_cluster_key) {
			as_smd_receive_metadata(g_smd, &item.m);
		}
		else if (item.expire > now) {
			cf_queue_push(&g_smd->pending_merge_queue, &item);
			continue;
		}

		cf_free(item.m.module_name);
		as_smd_item_list_destroy(item.m.items);
	}
}

/*
 *  Handle a cluster state changed message.
 *  This function collects all metadata items in this node, from all the module,
 *  currently (UDF, SINDEX) and sends it to the SMD principal for merging the metadata.
 */
static void as_smd_cluster_changed(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	cf_debug(AS_SMD, "System Metadata thread received cluster state changed cmd event!");

	g_cluster_key = (uint64_t)cmd->a;
	g_cluster_size = (uint32_t)(uint64_t)cmd->b;
	memcpy(g_succession, cmd->c, g_cluster_size * sizeof(cf_node));

	cf_free(cmd->c);

	// Determine the number of metadata items to be sent.
	size_t num_items = 0;
	cf_rchash_reduce(smd->modules, as_smd_module_count_items_reduce_fn, &num_items);

	cf_debug(AS_SMD, "sending %zu serialized metadata items to the SMD principal", num_items);

	// Copy all reference-counted metadata item pointers from the hash table into an item list.
	// (Note:  Even if this node has no metadata items, we must still send a message to the principal.)
	as_smd_item_list_t *item_list = as_smd_item_list_alloc(num_items);
	// (Note:  Use num_items to count the position for each serialized metadata item.)
	item_list->num_items = 0;
	cf_rchash_reduce(smd->modules, as_smd_module_serialize_reduce_fn, item_list);

	cf_debug(AS_SMD, "aspc():  num_items = %zu (%zu)", item_list->num_items, num_items);

	// Build a System Metadata fabric msg containing serialized metadata from the item list.
	msg *msg = NULL;
	as_smd_msg_op_t my_smd_op = AS_SMD_MSG_OP_MY_CURRENT_METADATA;
	if (!(msg = as_smd_msg_get(my_smd_op, item_list->item, item_list->num_items, NULL, 0))) {
		cf_crash(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start", AS_SMD_MSG_OP_NAME(my_smd_op));
	}

	// The metadata has been copied into the fabric msg and can now be released.
	as_smd_item_list_destroy(item_list);

	smd_fabric_send(as_smd_principal(), msg);

	smd_process_pending_merges();
}

/*
 *  Destroy a node's scoreboard hash table mapping module to metadata item count.
 */
static int as_smd_scoreboard_reduce_delete_fn(const void *key, void *data, void *udata)
{
	cf_node node_id = (cf_node) key;
	cf_shash *module_item_count_hash = *((cf_shash **) data);

	cf_debug(AS_SMD, "destroying module item count hash for node %016lX", node_id);

	cf_shash_destroy(module_item_count_hash);

	return CF_SHASH_REDUCE_DELETE;
}

/*
 *  Remove the metadata item from the hash table.
 */
static int as_smd_reduce_delete_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	return CF_RCHASH_REDUCE_DELETE;
}

/*
 *  Delete all of this module's external metadata items.
 */
static int as_smd_delete_external_metadata_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_t *smd = (as_smd_t *) udata;

	cf_rchash_reduce(module_obj->external_metadata, as_smd_reduce_delete_fn, smd);
	cf_debug(AS_SMD, "All the entries in the scoreboard have been deleted");

	return 0;
}

/*
 *  Clear out the temporary state used to merge metadata upon cluster state change.
 */
static void as_smd_clear_scoreboard(as_smd_t *smd)
{
	cf_shash_reduce(smd->scoreboard, as_smd_scoreboard_reduce_delete_fn, smd);
	cf_rchash_reduce(smd->modules, as_smd_delete_external_metadata_reduce_fn, smd);
}

/*
 *  Apply a metadata change locally using the registered merge policy, defaulting to union.
 */
static int as_smd_apply_metadata_change(as_smd_t *smd, as_smd_module_t *module_obj, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
	cf_debug(AS_SMD, "System Metadata thread - applying metadata %s change: item 0:  module \"%s\" ; key \"%s\" ; value \"%s\" ; action %d",
			 AS_SMD_MSG_OP_NAME(smd_msg->op), module_obj->module, item->key, item->value, item->action);

	// [Note:  Only 1 item should ever be changed via this path.]
	if (1 != smd_msg->num_items) {
		cf_crash(AS_SMD, "unexpected number of metadata items being changed: %d != 1", smd_msg->num_items);
	}

#if 0
	if (module_obj->merge_cb) {
		// Invoke the module's registered merge policy callback function.
		(module_obj->merge_cb)(module_obj->module, smd_msg->item, NULL, module_obj->merge_udata);
	} else {
#endif
		cf_debug(AS_SMD, "asamc():  num_items %d", smd_msg->num_items);

		// By default, simply perform a union operation on an item-by-item basis.
		for (int i = 0; i < smd_msg->num_items; i++) {
			item = smd_msg->items->item[i];
			if (module_obj->can_accept_cb) {
				int ret = (module_obj->can_accept_cb)(module_obj->module, item, module_obj->can_accept_udata);
				if (ret != 0) {
					cf_debug(AS_SMD, "SMD principal rejected the user operation with error code %s", as_sindex_err_str(ret));
					continue;
				} else {
					cf_debug(AS_SMD, "SMD principal validity check succeeded.");
				}
			}

			// Default timestamp to now.
			if (!item->timestamp) {
				item->timestamp = cf_clepoch_milliseconds();
			}

			cf_debug(AS_SMD, "asamc():  processing item %d: module \"%s\" key \"%s\" action %s gen %u ts %lu", i, item->module_name, item->key, AS_SMD_ACTION_NAME(item->action), item->generation, item->timestamp);

			// Perform the appropriate union operation.

			as_smd_item_t *existing_item = NULL;
			if (CF_RCHASH_OK == cf_rchash_get(module_obj->my_metadata, item->key, strlen(item->key) + 1, (void **) &existing_item)) {
				cf_debug(AS_SMD, "asamc():  Old item exists.");
			} else {
				cf_debug(AS_SMD, "asamc():  Old item does not exist.");

				if (AS_SMD_ACTION_DELETE == item->action) {
					cf_debug(AS_SMD, "deleting a non-extant item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
				}
			}

			if (!existing_item) {
				// For delete, if item already doesn't exist, there's nothing to do.
				if (AS_SMD_ACTION_DELETE == item->action) {
					continue;
				} else {
					// Otherwise, default to generation 1.
					if (!item->generation) {
						item->generation = 1;
					}
				}
			}

			// Choose the most up-to-date item data.
			if (existing_item && (AS_SMD_ACTION_DELETE != item->action)) {
				// Default to the next generation.
				if (!item->generation) {
					item->generation = existing_item->generation + 1;
				}

				// Choose the newest first by the highest generation and second by the highest timestamp.
				if ((existing_item->generation > item->generation) ||
						((existing_item->generation == item->generation) && (existing_item->timestamp > item->timestamp))) {

					cf_debug(AS_SMD, "old item is newer");

					// If the existing item is newer, skip the incoming item.
					cf_rc_release(existing_item);
					continue;
				} else {
					// Otherwise, advance the generation.
					item->generation = existing_item->generation + 1;

					cf_debug(AS_SMD, "New items is newer:  Going to gen %u ts %lu", item->generation, item->timestamp);
				}
				cf_rc_release(existing_item);
				existing_item = NULL;
			}

			// For each member of the succession list,
			//   Generate a new SMD fabric msg sharing the properties of the incoming msg event.
			//   Start a transaction to send the msg out to the node.
			//   The transaction recv function performs the accept metadata function locally.

			for (uint32_t i = 0; i < g_cluster_size; i++) {
				msg *msg = NULL;
				cf_node node_id = g_succession[i];
				as_smd_msg_op_t accept_op = AS_SMD_MSG_OP_ACCEPT_THIS_METADATA;
				if (!(msg = as_smd_msg_get(accept_op, smd_msg->items->item, smd_msg->num_items, module_obj->module, AS_SMD_ACCEPT_OPT_API))) {
					cf_warning(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start ~~ Skipping node %016lX!",
							   AS_SMD_MSG_OP_NAME(accept_op), node_id);
					continue;
				}

				smd_fabric_send(node_id, msg);
			}
		}
#if 0
	}
#endif

	return retval;
}

/*
 *  Increment hash table value by the given delta, starting from zero if not found, and return the new total.
 */
static int as_smd_shash_incr(cf_shash *ht, as_smd_module_t *module_obj, size_t delta)
{
	size_t count = 0;

	if (CF_SHASH_OK != cf_shash_get(ht, &module_obj, &count)) {
		// If not found, start at zero.
		count = 0;
	}

	count += delta;

	cf_shash_put(ht, &module_obj, &count);

	cf_debug(AS_SMD, "incrementing metadata item count for module \"%s\" to %zu", module_obj->module, count);

	return count;
}

/*
 *  Add the metadata items from this msg to the appropriate modules' external hash tables.
 */
static cf_shash *as_smd_store_metadata_by_module(as_smd_t *smd, as_smd_msg_t *smd_msg)
{
	as_smd_item_list_t *items = smd_msg->items;
	cf_shash *module_item_count_hash = cf_shash_create(cf_shash_fn_ptr, sizeof(as_smd_module_t *), sizeof(size_t), 19, CF_SHASH_BIG_LOCK);

	for (int i = 0; i < items->num_items; i++) {
		as_smd_item_t *item = items->item[i];

		// Find the appropriate module's external hash table for this item.
		as_smd_module_t *module_obj = NULL;
		if (! (module_obj = as_smd_module_get(smd, item, NULL))) {
			cf_warning(AS_SMD, "failed to get System Metadata module \"%s\" ~~ Skipping item!", item->module_name);
			continue;
		}

		// The length of the key string includes the NULL terminator.
		uint32_t key_len = strlen(item->key) + 1;
		uint32_t stack_key_len = sizeof(as_smd_external_item_key_t) + key_len;

		as_smd_external_item_key_t *stack_key = alloca(stack_key_len);
		if (!stack_key) {
			cf_crash(AS_SMD, "Failed to allocate stack key of size %d bytes!", stack_key_len);
		}
		stack_key->node_id = item->node_id;
		stack_key->key_len = key_len;
		memcpy(&(stack_key->key), item->key, key_len);

		// Warn if the item is already present.
		as_smd_item_t *old_item = NULL;
		cf_rchash *metadata_hash = module_obj->external_metadata;
		if (CF_RCHASH_OK == cf_rchash_get(metadata_hash, stack_key, stack_key_len, (void **) &old_item)) {
			cf_warning(AS_SMD, "found existing metadata item: node: %016lX module: \"%s\" key: \"%s\" value: \"%s\" ~~ Replacing with value: \"%s\"!",
					   item->node_id, item->module_name, item->key, old_item->value, item->value);
			// Give back the item reference.
			cf_rc_release(old_item);
		}

		// Add reference to item for storage in the hash table.
		// (Note:  One reference to the item will be released by the thread when it releases the containing msg.)
		cf_rc_reserve(item);

		// Insert the new metadata into the module's external metadata hash table, replacing any previous contents.
		cf_rchash_put(metadata_hash, stack_key, stack_key_len, item);

		cf_debug(AS_SMD, "Stored metadata by module for item %d: module \"%s\" ; key \"%s\"", i, module_obj->module, stack_key->key);
		// Increment the number of items for this module in this node's hash table.
		as_smd_shash_incr(module_item_count_hash, module_obj, 1);

		// Give back the module reference.
		cf_rc_release(module_obj);
	}

	return module_item_count_hash;
}

typedef struct smd_ext_item_search_s {
	cf_node node_id;
	as_smd_item_list_t *item_list;
	uint32_t count;
} smd_ext_item_search;

static int
smd_ext_items_fn(const void *key, uint32_t keylen, void *obj, void *udata)
{
	const as_smd_external_item_key_t *extkey =
			(const as_smd_external_item_key_t *)key;
	as_smd_item_t *item = (as_smd_item_t *)obj;
	smd_ext_item_search *search = (smd_ext_item_search *)udata;

	if (extkey->node_id == search->node_id) {
		cf_rc_reserve(item);
		search->item_list->item[search->item_list->num_items] = item;
		search->item_list->num_items++;
		cf_debug(AS_SMD, "For the node \"%016lX\", num_items is %zu", extkey->node_id, search->item_list->num_items);
	}

	return 0;
}

static int
smd_ext_items_count_fn(const void *key, uint32_t keysz, void *obj, void *udata)
{
	const as_smd_external_item_key_t *extkey =
			(const as_smd_external_item_key_t *)key;
	smd_ext_item_search *search = (smd_ext_item_search *)udata;

	if (extkey->node_id == search->node_id) {
		search->count++;
	}

	return 0;
}

/*
 *  Reduce function to create a list of metadata items from an rchash table.
 */
static int as_smd_list_items_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
//	char *item_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	cf_debug(AS_SMD, "adding to item list item: node: %016lX ; module: \"%s\" ; key: \"%s\"", item->node_id, item->module_name, item->key);
	cf_debug(AS_SMD, "item list: %p", item_list);
	cf_debug(AS_SMD, "item list length: %zu", item_list->num_items);

	cf_rc_reserve(item);

	item_list->item[item_list->num_items] = item;
	item_list->num_items += 1;

	return 0;
}

/*
 *  Invoke the merge policy callback function for this module.
 */
static int as_smd_invoke_merge_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata)
{
	const char *module = (const char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;

	cf_debug(AS_SMD, "invoking merge policy for module \"%s\"", module);

	as_smd_item_list_t *item_list_out = NULL;
	as_smd_item_list_t *item_lists_in[g_cluster_size];
	int list_num = (int)g_cluster_size;

	for (uint32_t i = 0; i < g_cluster_size; i++) {
		smd_ext_item_search search = {
				.node_id = g_succession[i]
		};

		cf_rchash_reduce(module_obj->external_metadata, smd_ext_items_count_fn,
				&search);
		item_lists_in[i] = as_smd_item_list_alloc(search.count);

		if (search.count != 0) {
			search.item_list = item_lists_in[i];
			item_lists_in[i]->num_items = 0;
			cf_rchash_reduce(module_obj->external_metadata, smd_ext_items_fn,
					&search);
		}
	}

	// Merge the metadata item lists for this module.
	if (module_obj->merge_cb) {
		// Invoke the module's registered merge policy callback function.
		(module_obj->merge_cb)(module, &item_list_out, item_lists_in, list_num, module_obj->merge_udata);
	} else {
		cf_debug(AS_SMD, "no merge cb registered ~~ performing default merge policy: union");

		// No merge policy registered ~~ Default to union.
		cf_rchash *merge_hash = NULL;
		cf_rchash_create(&merge_hash, cf_rchash_fn_fnv32, metadata_rchash_destructor_fn, 0, 127, 0);

		// Run through all metadata items in all node's lists.
		for (int i = 0; i < list_num; i++) {
			if (item_lists_in[i]) {
				for (int j = 0; j < item_lists_in[i]->num_items; j++) {
					as_smd_item_t *new_item = item_lists_in[i]->item[j];
					uint32_t key_len = strlen(new_item->key) + 1;

					// Look for an existing items with this key.
					as_smd_item_t *existing_item = NULL;
					if (CF_RCHASH_OK != cf_rchash_get(merge_hash, new_item->key, key_len, (void **) &existing_item)) {
						// If not found, insert this item.
						cf_rc_reserve(new_item);
						cf_rchash_put(merge_hash, new_item->key, key_len, new_item);
					} else {
						// Otherwise, choose a winner.
						bool existing_wins;

						if (module_obj->conflict_cb) {
							// Use registered callback to determine winner.
							existing_wins = (module_obj->conflict_cb)((char *)module, existing_item, new_item, module_obj->conflict_udata);
						} else {
							// Otherwise, choose a winner first by the highest generation and second by the highest timestamp.
							existing_wins = (existing_item->generation > new_item->generation) ||
									((existing_item->generation == new_item->generation) &&
									 (existing_item->timestamp > new_item->timestamp));
						}

						// Leave existing item in hash, or replace existing item
						// with new item (put releases existing item).
						if (! existing_wins) {
							cf_rc_reserve(new_item);
							cf_rchash_put(merge_hash, new_item->key, key_len, new_item);
						}

						as_smd_item_destroy(existing_item); // for cf_rchash_get
					}
				}
			}
		}

		// Create a merged items list.
		size_t num_items = cf_rchash_get_size(merge_hash);
		item_list_out = as_smd_item_list_alloc(num_items);

		// Populate the merged items list from the hash table.
		// (Note:  Use num_items to count the position for each metadata item.)
		item_list_out->num_items = 0;
		cf_rchash_reduce(merge_hash, as_smd_list_items_reduce_fn, item_list_out);
		cf_rchash_destroy(merge_hash);
	}

	// Sent out a merged metadata msg via fabric transaction to every cluster node.
	msg *msg = NULL;
	as_smd_msg_op_t merge_op = AS_SMD_MSG_OP_ACCEPT_THIS_METADATA;
	for (uint32_t i = 0; i < g_cluster_size; i++) {
		cf_node node_id = g_succession[i];
		if (!(msg = as_smd_msg_get(merge_op, item_list_out->item, item_list_out->num_items, module, AS_SMD_ACCEPT_OPT_MERGE))) {
			cf_crash(AS_SMD, "failed to get a System Metadata fabric msg for operation %s", AS_SMD_MSG_OP_NAME(merge_op));
		}

		smd_fabric_send(node_id, msg);
	}

	// Release the item lists.
	for (int i = 0; i < list_num; i++) {
		as_smd_item_list_destroy(item_lists_in[i]);
	}

	// Release the merged items list.
	as_smd_item_list_destroy(item_list_out);

	return 0;
}

static void
smd_add_pending_merge(as_smd_msg_t *sm)
{
	smd_pending_merge add = {
			.m = *sm,
			.expire = cf_getms() + SMD_PENDING_MERGE_TIMEOUT_SEC * 1000
	};

	// Steal memory from original.
	sm->items = NULL;
	sm->module_name = NULL;

	cf_queue_push(&g_smd->pending_merge_queue, &add);
}

/*
 *  Receive a node's metadata on the SMD principal to be combined via the registered merge policy.
 */
static int as_smd_receive_metadata(as_smd_t *smd, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	// Only the SMD principal receives other node's metadata.
	if (g_config.self_node != as_smd_principal()) {
		if (smd_msg->cluster_key != g_cluster_key) {
			smd_add_pending_merge(smd_msg);
		}

		cf_debug(AS_SMD, "non-principal node %016lX received metadata from node %016lX", g_config.self_node, smd_msg->node_id);
		return -1;
	}

	cf_debug(AS_SMD, "System Metadata thread - received %d metadata items from node %016lX", smd_msg->num_items, smd_msg->node_id);

	if (g_cluster_key != smd_msg->cluster_key) {
		smd_add_pending_merge(smd_msg);
		cf_debug(AS_SMD, "received SMD with non-current cluster key (%016lx != %016lx) from node %016lX -> Pending",
				 smd_msg->cluster_key, g_cluster_key, smd_msg->node_id);
		return -1;
	}

	// Store the all of the metadata items received from this node in the appropriate module's external metadata hash table.
	// And return the item counts by module in a hash table.
	cf_shash *module_item_count_hash = NULL;
	if (!(module_item_count_hash = as_smd_store_metadata_by_module(smd, smd_msg))) {
		cf_crash(AS_SMD, "failed to store metadata by module from node %016lX", smd_msg->node_id);
	}

	// If something is already there, its obsolete, so release it.
	cf_shash *prev_module_item_count_hash = NULL;
	if (CF_SHASH_OK == cf_shash_get(smd->scoreboard, &(smd_msg->node_id), &prev_module_item_count_hash)) {
		cf_debug(AS_SMD, "found an obsolete module item count hash for node %016lX ~~ Deleting!", smd_msg->node_id);
		if (CF_SHASH_OK != cf_shash_delete(smd->scoreboard, &(smd_msg->node_id))) {
			cf_warning(AS_SMD, "failed to delete obsolete module item count hash for node %016lX", smd_msg->node_id);
		}
		cf_shash_destroy(prev_module_item_count_hash);
	}

	// Note that this node has provided its metadata for this cluster state change.
	if (CF_SHASH_OK != cf_shash_put_unique(smd->scoreboard, &(smd_msg->node_id), &module_item_count_hash)) {
		cf_warning(AS_SMD, "failed to put unique node %016lX into System Metadata scoreboard hash table", smd_msg->node_id);
	}

	// Merge the metadata when all nodes have reported in.
	if (cf_shash_get_size(smd->scoreboard) == g_cluster_size) {
		cf_debug(AS_SMD, "received metadata from all %u cluster nodes ~~ invoking merge policies", g_cluster_size);

		cf_debug(AS_SMD, "Invoking merge reduce in SMD principal");
		// Invoke the merge policy for each module and send the results to all nodes.
		cf_rchash_reduce(smd->modules, as_smd_invoke_merge_reduce_fn, smd);

		// Clear out the state used to notify cluster nodes of the new metadata.
		as_smd_clear_scoreboard(smd);
	} else if (cf_shash_get_size(smd->scoreboard) > g_cluster_size) {
		// Cluster is unstable.
		// While one node is coming up, one of other nodes has gone down.
		// e.g Consider 3 node cluster. Add new node. Cluster size is 4.
		// SMD principal has received information from 3 nodes and waiting for fourth node.
		// So score board size is 3.
		// But now two node has gone down. Cluster size is reduced to 2.
		as_smd_clear_scoreboard(smd);
	} else {
		cf_debug(AS_SMD, "Cluster size = %u and smd->scoreboard size = %d ", g_cluster_size, cf_shash_get_size(smd->scoreboard));
	}

	return retval;
}

static int metadata_local_deleteall_fn(const void *key, uint32_t key_len, void *object, void *udata)
{
	return CF_RCHASH_REDUCE_DELETE;
}

/*
 *  Accept a metadata change from the SMD principal using the registered accept policy.
 */
static int as_smd_accept_metadata(as_smd_t *smd, as_smd_module_t *module_obj, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	// There will be:
	//    0 items when, after the merge, no valid metadata items were found according to the merge algorithm.
	//    1 item when the user issues a set/delete metadata API call to a specific module (e.g., SINDEX, UDF.)
	// >= 1 items when, after the merge, a non-empty list of items is valid according to the merge algorithm.
	if (smd_msg->items->num_items) {
		as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
		cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: %zu items: item 0: module \"%s\" ; key \"%s\" ; value \"%s\"",
				 AS_SMD_MSG_OP_NAME(smd_msg->op), smd_msg->items->num_items, module_obj->module, item->key, item->value);
	} else {
		// Allow empty item list for merge and module create.
		if (smd_msg->options & (AS_SMD_ACCEPT_OPT_MERGE | AS_SMD_ACCEPT_OPT_CREATE)) {
			cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: Zero items coming from merge", AS_SMD_MSG_OP_NAME(smd_msg->op));
		} else {
			cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: Zero items ~~ Returning!", AS_SMD_MSG_OP_NAME(smd_msg->op));
			return retval;
		}
	}

	cf_debug(AS_SMD, "accepting replacement metadata from incoming System Metadata msg");

#if 1 // DEBUG
	// It should never be null. Being defensive to bail out just in case.
	if (!module_obj) {
		cf_crash(AS_SMD, "SMD module NULL in accept metadata!");
	}
#endif

	// In case of merge (after cluster state change) drop the existing local metadata definitions
	// This is done to clean up some metadata, which could have been dropped during the merge
	if (smd_msg->options & AS_SMD_ACCEPT_OPT_MERGE) {
		cf_rchash_reduce(module_obj->my_metadata, metadata_local_deleteall_fn, NULL);
	}

	for (int i = 0; i < smd_msg->items->num_items; i++) {
		as_smd_item_t *item = smd_msg->items->item[i];
		if ((retval = as_smd_metadata_change_local(smd, smd_msg->op, item))) {
			cf_warning(AS_SMD, "failed to perform the default accept replace local metadata operation %s (rv %d) for item %d: module \"%s\" ; key \"%s\" ; value \"%s\"",
					   AS_SMD_MSG_OP_NAME(smd_msg->op), retval, i, item->module_name, item->key, item->value);
		}
	}

	// Accept the metadata item list for this module.
	if (module_obj->accept_cb) {
		// Invoke the module's registered accept policy callback function.
		cf_debug(AS_SMD, "Calling accept callback with OPT_MERGE for module %s with nitems %zu", smd_msg->module_name, smd_msg->items->num_items);
		(module_obj->accept_cb)(module_obj->module, smd_msg->items, module_obj->accept_udata, smd_msg->options);
	}

	// SMD should now be persisted.
	module_obj->dirty = true;

	// Persist the accepted metadata for this module.
	if (as_smd_module_persist(module_obj)) {
		cf_warning(AS_SMD, "failed to persist accepted metadata for module \"%s\"", module_obj->module);
	}

	return retval;
}

static uint32_t key2idx_get_index(as_hashmap *map, const char *key)
{
	const as_integer *i = as_stringmap_get_integer((as_map *)map, key);

	if (i) {
		return (uint32_t)as_integer_get(i);
	}

	uint32_t new_index = as_hashmap_size(map);

	as_stringmap_set_int64((as_map *)map, key, (int64_t)new_index);

	return new_index;
}

int as_smd_majority_consensus_merge(const char *module, as_smd_item_list_t **merged_list,
									as_smd_item_list_t **lists_to_merge, size_t num_list, void *udata)
{
	typedef struct {
		as_smd_item_t *item; // does not hold ref to item
		uint32_t count;
	} merge_item;

	cf_vector merge_list;
	as_hashmap key2idx;

	as_hashmap_init(&key2idx, 1024);
	cf_vector_init(&merge_list, sizeof(merge_item), 1024, 0);

	for(size_t i = 0; i < num_list; i++) {
		size_t num_items = lists_to_merge[i]->num_items;

		for (size_t j = 0; j < num_items; j++) {
			as_smd_item_t *item = lists_to_merge[i]->item[j];
			uint32_t idx = key2idx_get_index(&key2idx, item->key);

			if (idx >= cf_vector_size(&merge_list)) {
				merge_item mitem = {
						.item = item,
						.count = 1
				};

				cf_vector_append(&merge_list, &mitem);
				continue;
			}

			merge_item *p_mitem = (merge_item *)cf_vector_getp(&merge_list, idx);
			bool existing_wins = (p_mitem->item->generation > item->generation) ||
					((p_mitem->item->generation == item->generation) &&
							(p_mitem->item->timestamp > item->timestamp));

			if (! existing_wins) {
				p_mitem->item = item;
			}

			p_mitem->count++;
		}
	}

	as_hashmap_destroy(&key2idx);
	*merged_list = as_smd_item_list_alloc(cf_vector_size(&merge_list));

	uint32_t majority_count = ((uint32_t)num_list + 1) / 2;

	for (uint32_t i = 0; i < cf_vector_size(&merge_list); i++) {
		merge_item *p_mitem = (merge_item *)cf_vector_getp(&merge_list, i);

		if (p_mitem->count >= majority_count) {
			cf_rc_reserve(p_mitem->item);
			(*merged_list)->item[i] = p_mitem->item;
		}
		else {
			as_smd_item_t *item = (as_smd_item_t *)cf_rc_alloc(sizeof(as_smd_item_t));

			memset(item, 0, sizeof(as_smd_item_t));
			item->action = AS_SMD_ACTION_DELETE;
			item->key = cf_strdup(p_mitem->item->key);
			item->generation = p_mitem->item->generation + 1;
			item->timestamp = cf_clepoch_milliseconds();
			(*merged_list)->item[i] = item;
		}
	}

	cf_vector_destroy(&merge_list);

	return 0;
}

/*
 *  Process an SMD event, which may be either an SMD API command or an incoming SMD fabric msg.
 */
static void as_smd_process_event (as_smd_t *smd, as_smd_event_t *evt)
{
	if (AS_SMD_CMD == evt->type) {

		/***** Handle SMD API Command Event *****/

		as_smd_cmd_t *cmd = &(evt->u.cmd);

		cf_debug(AS_SMD, "SMD thread received command: \"%s\" ; options: 0x%08x", AS_SMD_CMD_TYPE_NAME(cmd->type), cmd->options);

		if (cmd->item) {
			cf_debug(AS_SMD, "SMD event item: node %016lX ; module \"%s\" ; key \"%s\" ; value %p ; generation %u ; timestamp %zu",
					 cmd->item->node_id, cmd->item->module_name, cmd->item->key, cmd->item->value, cmd->item->generation, cmd->item->timestamp);
		}

		switch (cmd->type) {
			case AS_SMD_CMD_INIT:
				smd->state = AS_SMD_STATE_INITIALIZED;
				break;

			case AS_SMD_CMD_START:
				smd->state = AS_SMD_STATE_RUNNING;
				break;

			case AS_SMD_CMD_CREATE_MODULE:
				as_smd_module_create(smd, cmd);
				break;

			case AS_SMD_CMD_DESTROY_MODULE:
				as_smd_module_destroy(smd, cmd);
				break;

			case AS_SMD_CMD_SET_METADATA:
			case AS_SMD_CMD_DELETE_METADATA:
				as_smd_metadata_change(smd, CMD_TYPE2MSG_OP(cmd->type), cmd->item);
				break;

			case AS_SMD_CMD_GET_METADATA:
				as_smd_metadata_get(smd, cmd);
				break;

			case AS_SMD_CMD_CLUSTER_CHANGED:
				as_smd_cluster_changed(smd, cmd);
				break;

			case AS_SMD_CMD_INTERNAL:
				if (cmd->options & AS_SMD_CMD_OPT_DUMP_SMD) {
					as_smd_dump_metadata(smd, cmd);
				} else {
					cf_warning(AS_SMD, "Unknown System Metadata internal event options received: 0x%08x ~~ Ignoring event!", cmd->options);
				}
				break;

			case AS_SMD_CMD_SHUTDOWN:
				smd->state = AS_SMD_STATE_EXITING;
				break;

			default:
				cf_crash(AS_SMD, "received unknown System Metadata event type %d", cmd->type);
				break;
		}
	} else if (AS_SMD_MSG == evt->type) {

		/***** Handle SMD Fabric Transaction Message Event *****/

		as_smd_msg_t *msg = &(evt->u.msg);
		as_smd_item_t *item = NULL;

		if (msg->num_items) {
			item = msg->items->item[0]; // (Only log the fist item.)
			cf_debug(AS_SMD, "SMD thread received fabric msg event with op %s item: item 0: node %016lX module \"%s\" ; key \"%s\" ; value \"%s\"",
					 AS_SMD_MSG_OP_NAME(msg->op), item->node_id, item->module_name, item->key, item->value);
		} else {
			cf_debug(AS_SMD, "SMD thread received fabric msg event with op %s [Zero metadata items]", AS_SMD_MSG_OP_NAME(msg->op));
			if ((AS_SMD_MSG_OP_SET_ITEM == msg->op) || (AS_SMD_MSG_OP_DELETE_ITEM == msg->op)) {
				cf_crash(AS_SMD, "SMD thread received invalid empty metadata items list from node %016lX for message %s",
						 msg->node_id, AS_SMD_MSG_OP_NAME(msg->op));
			}
		}

		// Find (or create) the module's object.
		as_smd_module_t *module_obj = as_smd_module_get(smd, (msg->num_items > 0 ? msg->items->item[0] : NULL), msg);

		switch (msg->op) {
			case AS_SMD_MSG_OP_SET_ITEM:
			case AS_SMD_MSG_OP_DELETE_ITEM:
				as_smd_apply_metadata_change(smd, module_obj, msg);
				break;

			case AS_SMD_MSG_OP_MY_CURRENT_METADATA:
				as_smd_receive_metadata(smd, msg);
				break;

			case AS_SMD_MSG_OP_ACCEPT_THIS_METADATA:
			case AS_SMD_MSG_OP_SET_FROM_PR:
				as_smd_accept_metadata(smd, module_obj, msg);
				break;
		}

		if (module_obj) {
			// Give back the reference.
			cf_rc_release(module_obj);
		}
	} else {
		// This should never happen.
		cf_warning(AS_SMD, "received unknown type of System Metadata event (%d)", evt->type);
	}
}

/*
 *  Thread to handle all System Metadata events, incoming via the API or the fabric.
 */
void *as_smd_thr(void *arg)
{
	as_smd_t *smd = (as_smd_t *) arg;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread created");

	// Receive incoming messages via the message queue.
	// Process each message.
	// Destroy the message after processing.

	for ( ; smd->state != AS_SMD_STATE_EXITING ; ) {

		as_smd_event_t *evt = NULL;

		if ((retval = cf_queue_pop(smd->msgq, &evt, AS_SMD_WAIT_INTERVAL_MS))) {
			if (CF_QUEUE_ERR == retval) {
				cf_warning(AS_SMD, "failed to pop an event (retval %d)", retval);
			}
		}

		if (CF_QUEUE_EMPTY == retval) {
			// [Could handle any periodic / background events here when there's nothing else to do.]
			cf_detail(AS_SMD, "System Metadata thread - received timeout event");
			smd_expire_pending_merges();
		} else {
			as_smd_process_event(smd, evt);

			// Release the event message.
			as_smd_destroy_event(evt);
		}
	}

	// Release System Metadata resources.
	as_smd_terminate(smd);

	// Exit the System Metadata thread.
	return NULL;
}
