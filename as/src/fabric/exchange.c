/*
 * exchange.c
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include "fabric/exchange.h"

#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/param.h> // For MAX() and MIN().

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "shash.h"
#include "socket.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/stats.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"

/*
 * Overview
 * ========
 * Cluster data exchange state machine. Exchanges per namespace partition
 * version exchange for now, after evey cluster change.
 *
 * State transition diagram
 * ========================
 * The exchange state transition diagram responds to three events
 * 	1. Incoming message
 * 	2. Timer event
 * 	3. Clustering module's cluster change event.
 *
 * There are four states
 * 	1. Rest - the exchange is complete with all exchanged data committed.
 * 	2. Exchanging - the cluster has changed since the last commit and new data
 * exchange is in progress.
 * 	3. Ready to commit - this node has send its exchange data to all cluster
 * members, received corresponding acks and also exchange data from all cluster
 * members.
 * 	4. Orphaned - this node is an orphan. After a timeout blocks client
 * transactions.
 *
 * Exchange starts by being in the orphaned state.
 *
 * Code organization
 * =================
 *
 * There are different sections for each state. Each state has a dispatcher
 * which delegates the event handing to a state specific function.
 *
 * Locks
 * =====
 *  1. g_exchanage_lock - protected the exchange state machine.
 *  2. g_exchange_info_lock - prevents update of exchanged data for a round while
 * exchange is in progress
 *  3. g_exchange_commited_cluster_lock - a higher
 * granularity, committed cluster r/w lock,used to allow read access to
 * committed cluster data even while exchange is busy and holding on to the
 * global exchange state machine lock.
 *  4. g_external_event_publisher_lock - ensure order of external events
 * published is maintained.
 *
 * Lock order
 * ==========
 * Locks to be obtained in the order mentioned above and relinquished in the
 * reverse order.
 */

/*
 * ----------------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------------
 */

/**
 * Exchange protocol version information.
 */
#define AS_EXCHANGE_PROTOCOL_IDENTIFIER 1

/**
 * A soft limit for the maximum cluster size. Meant to be optimize hash and list
 * data structures and not as a limit on the number of nodes.
 */
#define AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT 200

/**
 * A soft limit for the maximum number of unique vinfo's in a namespace. Meant
 * to be optimize hash and list data structures and not as a limit on the number
 * of vinfos processed.
 */
#define AS_EXCHANGE_UNIQUE_VINFO_MAX_SIZE_SOFT 200

/**
 * Average number of partitions for a version information. Used as initial
 * allocation size for every unique vinfo, hence a smaller value.
 */
#define AS_EXCHANGE_VINFO_NUM_PIDS_AVG 1024

/**
 * Maximum event listeners.
 */
#define AS_EXTERNAL_EVENT_LISTENER_MAX 7

/*
 * ----------------------------------------------------------------------------
 * Exchange data format for namespaces payload
 * ----------------------------------------------------------------------------
 */

/**
 * Partition data exchanged for each unique vinfo for a namespace.
 */
typedef struct as_exchange_vinfo_payload_s
{
	/**
	 * The partition vinfo.
	 */
	as_partition_version vinfo;

	/**
	 * Count of partitions having this vinfo.
	 */
	uint32_t num_pids;

	/**
	 * Partition having this vinfo.
	 */
	uint16_t pids[];
}__attribute__((__packed__)) as_exchange_vinfo_payload;

/**
 * Information exchanged for a single namespace.
 */
typedef struct as_exchange_ns_vinfos_payload_s
{
	/**
	 * Count of version infos.
	 */
	uint32_t num_vinfos;

	/**
	 * Parition version information for each unique version.
	 */
	as_exchange_vinfo_payload vinfos[];
}__attribute__((__packed__)) as_exchange_ns_vinfos_payload;

/**
 * Received data stored per node, per namespace, before actual commit.
 */
typedef struct as_exchange_node_namespace_data_s
{
	/**
	 * Mapped local namespace.
	 */
	as_namespace* local_namespace;

	/**
	 * Partition versions for this namespace. This field is reused across
	 * exchange rounds and may not be null even if the local namespace is null.
	 */
	as_exchange_ns_vinfos_payload* partition_versions;

	/**
	 * Sender's rack id.
	 */
	uint32_t rack_id;

	/**
	 * Sender's roster generation.
	 */
	uint32_t roster_generation;

	/**
	 * Sender's roster count.
	 */
	uint32_t roster_count;

	/**
	 * Sending node's roster for this namespace.
	 */
	cf_node* roster;

	/**
	 * Sending node's roster rack-ids for this namespace.
	 */
	cf_node* roster_rack_ids;

	/**
	 * Sender's eventual regime for this namespace.
	 */
	uint32_t eventual_regime;

	/**
	 * Sender's rebalance regime for this namespace.
	 */
	uint32_t rebalance_regime;

	/**
	 * Sender's rebalance flags for this namespace.
	 */
	uint32_t rebalance_flags;
} as_exchange_node_namespace_data;

/**
 * Exchanged data for a single node.
 */
typedef struct as_exchange_node_data_s
{
	/**
	 * Used by exchange listeners during upgrades for compatibility purposes.
	 */
	uint32_t compatibility_id;

	/**
	 * Number of sender's namespaces that have a matching local namespace.
	 */
	uint32_t num_namespaces;

	/**
	 * Data for sender's namespaces having a matching local namespace.
	 */
	as_exchange_node_namespace_data namespace_data[AS_NAMESPACE_SZ];
} as_exchange_node_data;

/*
 * ----------------------------------------------------------------------------
 * Exchange internal data structures
 * ----------------------------------------------------------------------------
 */

/**
 * Exchange subsystem status.
 */
typedef enum
{
	AS_EXCHANGE_SYS_STATE_UNINITIALIZED,
	AS_EXCHANGE_SYS_STATE_RUNNING,
	AS_EXCHANGE_SYS_STATE_SHUTTING_DOWN,
	AS_EXCHANGE_SYS_STATE_STOPPED
} as_exchange_sys_state;

/**
 * Exchange message types.
 */
typedef enum
{
	/**
	 * Exchange data for one node.
	 */
	AS_EXCHANGE_MSG_TYPE_DATA,

	/**
	 * Ack on receipt of exchanged data.
	 */
	AS_EXCHANGE_MSG_TYPE_DATA_ACK,

	/**
	 * Not used.
	 */
	AS_EXCHANGE_MSG_TYPE_DATA_NACK,

	/**
	 * The source is ready to commit exchanged information.
	 */
	AS_EXCHANGE_MSG_TYPE_READY_TO_COMMIT,

	/**
	 * Message from the principal asking all nodes to commit the exchanged
	 * information.
	 */
	AS_EXCHANGE_MSG_TYPE_COMMIT,

	/**
	 * Sentinel value for exchange message types.
	 */
	AS_EXCHANGE_MSG_TYPE_SENTINEL
} as_exchange_msg_type;

/**
 * Internal exchange event type.
 */
typedef enum
{
	/**
	 * Cluster change event.
	 */
	AS_EXCHANGE_EVENT_CLUSTER_CHANGE,

	/**
	 * Timer event.
	 */
	AS_EXCHANGE_EVENT_TIMER,

	/**
	 * Incoming message event.
	 */
	AS_EXCHANGE_EVENT_MSG,
} as_exchange_event_type;

/**
 * Internal exchange event.
 */
typedef struct as_exchange_event_s
{
	/**
	 * The type of the event.
	 */
	as_exchange_event_type type;

	/**
	 * Message for incoming message events.
	 */
	msg* msg;

	/**
	 * Source for incoming message events.
	 */
	cf_node msg_source;

	/**
	 * Clustering event instance for clustering events.
	 */
	as_clustering_event* clustering_event;
} as_exchange_event;

/**
 * Exchange subsystem state in the state transition diagram.
 */
typedef enum as_exchange_state_s
{
	/**
	 * Exchange subsystem is at rest will all data exchanged synchronized and
	 * committed.
	 */
	AS_EXCHANGE_STATE_REST,

	/**
	 * Data exchange is in progress.
	 */
	AS_EXCHANGE_STATE_EXCHANGING,

	/**
	 * Data exchange is complete and this node is ready to commit data.
	 */
	AS_EXCHANGE_STATE_READY_TO_COMMIT,

	/**
	 * Self node is orphaned.
	 */
	AS_EXCHANGE_STATE_ORPHANED
} as_exchange_state;

/**
 * State for a single node in the succession list.
 */
typedef struct as_exchange_node_state_s
{
	/**
	 * Inidicates if peer node has acknowledged send from self.
	 */
	bool send_acked;

	/**
	 * Inidicates if self node has received data from this peer.
	 */
	bool received;

	/**
	 * Inidicates if this peer node is ready to commit. Only relevant and used
	 * by the current principal.
	 */
	bool is_ready_to_commit;

	/**
	 * Exchange data received from this peer node. Member variables may be heap
	 * allocated and hence should be freed carefully while discarding this
	 * structure instance.
	 */
	as_exchange_node_data* data;
} as_exchange_node_state;

/**
 * State maintained by the exchange subsystem.
 */
typedef struct as_exchange_s
{
	/**
	 * Exchange subsystem status.
	 */
	as_exchange_sys_state sys_state;

	/**
	 * Exchange state in the state transition diagram.
	 */
	as_exchange_state state;

	/**
	 * Time when this node's exchange data was sent out.
	 */
	cf_clock send_ts;

	/**
	 * Time when this node's ready to commit was sent out.
	 */
	cf_clock ready_to_commit_send_ts;

	/**
	 * Thread id of the timer event generator.
	 */
	pthread_t timer_tid;

	/**
	 * Nodes that are not yet ready to commit.
	 */
	cf_vector ready_to_commit_pending_nodes;

	/**
	 * Current cluster key.
	 */
	as_cluster_key cluster_key;

	/**
	 * Exchange's copy of the succession list.
	 */
	cf_vector succession_list;

	/**
	 * The principal node in current succession list. Always the first node.
	 */
	cf_node principal;

	/**
	 * Used by exchange listeners during upgrades for compatibility purposes.
	 */
	uint32_t compatibility_ids[AS_CLUSTER_SZ];

	/**
	 * Used by exchange listeners during upgrades for compatibility purposes.
	 */
	uint32_t min_compatibility_id;

	/**
	 * Committed cluster generation.
	 */
	uint64_t committed_cluster_generation;

	/**
	 * Last committed cluster key.
	 */
	as_cluster_key committed_cluster_key;

	/**
	 * Last committed cluster size - size of the succession list.
	 */
	uint32_t committed_cluster_size;

	/**
	 * Last committed exchange's succession list.
	 */
	cf_vector committed_succession_list;

	/**
	 * The principal node in the committed succession list. Always the first
	 * node.
	 */
	cf_node committed_principal;

	/**
	 * The time this node entered orphan state.
	 */
	cf_clock orphan_state_start_time;

	/**
	 * Indicates if transactions have already been blocked in the orphan state.
	 */
	bool orphan_state_are_transactions_blocked;

	/**
	 * Will have an as_exchange_node_state entry for every node in the
	 * succession list.
	 */
	cf_shash* nodeid_to_node_state;

	/**
	 * Self node's partition version payload for current round.
	 */
	cf_dyn_buf self_data_dyn_buf[AS_NAMESPACE_SZ];

	/**
	 * This node's exchange data fabric message to send for current round.
	 */
	msg* data_msg;
} as_exchange;

/**
 * Internal storage for external event listeners.
 */
typedef struct as_exchange_event_listener_s
{
	/**
	 * The listener's calback function.
	 */
	as_exchange_cluster_changed_cb event_callback;

	/**
	 * The listeners user data object passed back as is to the callback
	 * function.
	 */
	void* udata;
} as_exchange_event_listener;

/**
 * External event publisher state.
 */
typedef struct as_exchange_external_event_publisher_s
{
	/**
	 * State of the external event publisher.
	 */
	as_exchange_sys_state sys_state;

	/**
	 * Inidicates if there is an event to publish.
	 */
	bool event_queued;

	/**
	 * The pending event to publish.
	 */
	as_exchange_cluster_changed_event to_publish;

	/**
	 * The static succession list published with the message.
	 */
	cf_vector published_succession_list;

	/**
	 * Conditional variable to signal a pending event.
	 */
	pthread_cond_t is_pending;

	/**
	 * Thread id of the publisher thread.
	 */
	pthread_t event_publisher_tid;

	/**
	 * Mutex to protect the conditional variable.
	 */
	pthread_mutex_t is_pending_mutex;

	/**
	 * External event listeners.
	 */
	as_exchange_event_listener event_listeners[AS_EXTERNAL_EVENT_LISTENER_MAX];

	/**
	 * Event listener count.
	 */
	uint32_t event_listener_count;
} as_exchange_external_event_publisher;

/*
 * ----------------------------------------------------------------------------
 * Externs
 * ----------------------------------------------------------------------------
 */
void
as_skew_monitor_update();

/*
 * ----------------------------------------------------------------------------
 * Globals
 * ----------------------------------------------------------------------------
 */

/**
 * Singleton exchange state all initialized to zero.
 */
static as_exchange g_exchange = { 0 };

/**
 * Rebalance flags.
 */
typedef enum
{
	AS_EXCHANGE_REBALANCE_FLAG_UNIFORM = 0x01,
	AS_EXCHANGE_REBALANCE_FLAG_QUIESCE = 0x02
} as_exchange_rebalance_flags;

/**
 * The fields in the exchange message. Should never change the order or elements
 * in between.
 */
typedef enum
{
	AS_EXCHANGE_MSG_ID,
	AS_EXCHANGE_MSG_TYPE,
	AS_EXCHANGE_MSG_CLUSTER_KEY,
	AS_EXCHANGE_MSG_NAMESPACES,
	AS_EXCHANGE_MSG_NS_PARTITION_VERSIONS,
	AS_EXCHANGE_MSG_NS_RACK_IDS,
	AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS,
	AS_EXCHANGE_MSG_NS_ROSTERS,
	AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS,
	AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES,
	AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES,
	AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS,
	AS_EXCHANGE_MSG_COMPATIBILITY_ID,

	NUM_EXCHANGE_MSG_FIELDS
} as_exchange_msg_fields;

/**
 * Exchange message template.
 */
static const msg_template exchange_msg_template[] = {
		{ AS_EXCHANGE_MSG_ID, M_FT_UINT32 },
		{ AS_EXCHANGE_MSG_TYPE, M_FT_UINT32 },
		{ AS_EXCHANGE_MSG_CLUSTER_KEY, M_FT_UINT64 },
		{ AS_EXCHANGE_MSG_NAMESPACES, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_PARTITION_VERSIONS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_RACK_IDS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_ROSTERS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS, M_FT_MSGPACK },
		{ AS_EXCHANGE_MSG_COMPATIBILITY_ID, M_FT_UINT32 }
};

COMPILER_ASSERT(sizeof(exchange_msg_template) / sizeof(msg_template) ==
		NUM_EXCHANGE_MSG_FIELDS);

/**
 * Global lock to set or get exchanged info from other threads.
 */
pthread_mutex_t g_exchanged_info_lock = PTHREAD_MUTEX_INITIALIZER;

/**
 * Global lock to serialize all reads and writes to the exchange state.
 */
pthread_mutex_t g_exchange_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * Acquire a lock on the exchange subsystem.
 */
#define EXCHANGE_LOCK()							\
({												\
	pthread_mutex_lock (&g_exchange_lock);		\
	LOCK_DEBUG("locked in %s", __FUNCTION__);	\
})

/**
 * Relinquish the lock on the exchange subsystem.
 */
#define EXCHANGE_UNLOCK()							\
({													\
	pthread_mutex_unlock (&g_exchange_lock);		\
	LOCK_DEBUG("unLocked in %s", __FUNCTION__);		\
})

/**
 * Global lock to set or get committed exchange cluster. This is a lower
 * granularity lock to allow read access to committed cluster even while
 * exchange is busy for example rebalancing under the exchange lock.
 */
pthread_rwlock_t g_exchange_commited_cluster_lock = PTHREAD_RWLOCK_INITIALIZER;

/**
 * Acquire a read lock on the committed exchange cluster.
 */
#define EXCHANGE_COMMITTED_CLUSTER_RLOCK()						\
({																\
	pthread_rwlock_rdlock (&g_exchange_commited_cluster_lock);	\
	LOCK_DEBUG("committed data locked in %s", __FUNCTION__);	\
})

/**
 * Acquire a write lock on the committed exchange cluster.
 */
#define EXCHANGE_COMMITTED_CLUSTER_WLOCK()						\
({																\
	pthread_rwlock_wrlock (&g_exchange_commited_cluster_lock);	\
	LOCK_DEBUG("committed data locked in %s", __FUNCTION__);	\
})

/**
 * Relinquish the lock on the committed exchange cluster.
 */
#define EXCHANGE_COMMITTED_CLUSTER_UNLOCK()						\
({																\
	pthread_rwlock_unlock (&g_exchange_commited_cluster_lock);	\
	LOCK_DEBUG("committed data unLocked in %s", __FUNCTION__);	\
})

/**
 * Singleton external events publisher.
 */
static as_exchange_external_event_publisher g_external_event_publisher;

/**
 * The fat lock for all clustering events listener changes.
 */
static pthread_mutex_t g_external_event_publisher_lock =
		PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * Acquire a lock on the event publisher.
 */
#define EXTERNAL_EVENT_PUBLISHER_LOCK()						\
({															\
	pthread_mutex_lock (&g_external_event_publisher_lock);	\
	LOCK_DEBUG("publisher locked in %s", __FUNCTION__);		\
})

/**
 * Relinquish the lock on the external event publisher.
 */
#define EXTERNAL_EVENT_PUBLISHER_UNLOCK()						\
({																\
	pthread_mutex_unlock (&g_external_event_publisher_lock);	\
	LOCK_DEBUG("publisher unLocked in %s", __FUNCTION__);		\
})

/*
 * ----------------------------------------------------------------------------
 * Logging macros.
 * ----------------------------------------------------------------------------
 */

/**
 * Used to limit potentially long log lines. Includes space for NULL terminator.
 */
#define CRASH(format, ...) cf_crash(AS_EXCHANGE, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_EXCHANGE, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_EXCHANGE, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_EXCHANGE, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_EXCHANGE, format, ##__VA_ARGS__)
#define LOG(severity, format, ...)			\
({											\
	switch (severity) {						\
	case CF_CRITICAL:						\
		CRASH(format, ##__VA_ARGS__);		\
		break;								\
	case CF_WARNING:						\
		WARNING(format, ##__VA_ARGS__);		\
		break;								\
	case CF_INFO:							\
		INFO(format, ##__VA_ARGS__);		\
		break;								\
	case CF_DEBUG:							\
		DEBUG(format, ##__VA_ARGS__);		\
		break;								\
	case CF_DETAIL:							\
		DETAIL(format, ##__VA_ARGS__);		\
		break;								\
	default:								\
		break;								\
	}										\
})

/**
 * Size of the (per-namespace) self payload dynamic buffer.
 */
#define AS_EXCHANGE_SELF_DYN_BUF_SIZE() (AS_EXCHANGE_UNIQUE_VINFO_MAX_SIZE_SOFT		\
		* ((AS_EXCHANGE_VINFO_NUM_PIDS_AVG * sizeof(uint16_t))						\
				+ sizeof(as_partition_version)))

/**
 * Scratch size for exchange messages.
 * TODO: Compute this properly.
 */
#define AS_EXCHANGE_MSG_SCRATCH_SIZE 2048

#ifdef LOCK_DEBUG_ENABLED
#define LOCK_DEBUG(format, ...) DEBUG(format, ##__VA_ARGS__)
#else
#define LOCK_DEBUG(format, ...)
#endif

/**
 * Timer event generation interval.
 */
#define EXCHANGE_TIMER_TICK_INTERVAL() (75)

/**
 * Minimum timeout interval for sent exchange data.
 */
#define EXCHANGE_SEND_MIN_TIMEOUT() (MAX(75, as_hb_tx_interval_get() / 2))

/**
 * Maximum timeout interval for sent exchange data.
 */
#define EXCHANGE_SEND_MAX_TIMEOUT() (30000)

/**
 * Timeout for receiving commit message after transitioning to ready to commit.
 */
#define EXCHANGE_READY_TO_COMMIT_TIMEOUT() (EXCHANGE_SEND_MIN_TIMEOUT())

/**
 * Send timeout is a step function with this value as the interval for each
 * step.
 */
#define EXCHANGE_SEND_STEP_INTERVAL()							\
(MAX(EXCHANGE_SEND_MIN_TIMEOUT(), as_hb_tx_interval_get()))

/**
 * Check if exchange is initialized.
 */
#define EXCHANGE_IS_INITIALIZED()						\
({														\
	EXCHANGE_LOCK();									\
	bool initialized = (g_exchange.sys_state			\
			!= AS_EXCHANGE_SYS_STATE_UNINITIALIZED);	\
	EXCHANGE_UNLOCK();									\
	initialized;										\
})

/**
 * * Check if exchange is running.
 */
#define EXCHANGE_IS_RUNNING()											\
({																		\
	EXCHANGE_LOCK();													\
	bool running = (EXCHANGE_IS_INITIALIZED()							\
			&& g_exchange.sys_state == AS_EXCHANGE_SYS_STATE_RUNNING);	\
	EXCHANGE_UNLOCK();													\
	running;															\
})

/**
 * Create temporary stack variables.
 */
#define TOKEN_PASTE(x, y) x##y
#define STACK_VAR(x, y) TOKEN_PASTE(x, y)

/**
 * Convert a vector to a stack allocated array.
 */
#define cf_vector_to_stack_array(vector_p, nodes_array_p, num_nodes_p)	\
({																		\
	*num_nodes_p = cf_vector_size(vector_p);							\
	if (*num_nodes_p > 0) {												\
		*nodes_array_p = alloca(sizeof(cf_node) * (*num_nodes_p));		\
		for (int i = 0; i < *num_nodes_p; i++) {						\
			cf_vector_get(vector_p, i, &(*nodes_array_p)[i]);			\
		}																\
	}																	\
	else {																\
		*nodes_array_p = NULL;											\
	}																	\
})

/**
 * Create and initialize a lockless stack allocated vector to initially sized to
 * store cluster node number of elements.
 */
#define cf_vector_stack_create(value_type)											\
({																					\
	cf_vector * STACK_VAR(vector, __LINE__) = (cf_vector*)alloca(					\
			sizeof(cf_vector));														\
	size_t buffer_size = AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT							\
			* sizeof(value_type);													\
	void* STACK_VAR(buff, __LINE__) = alloca(buffer_size); cf_vector_init_smalloc(	\
			STACK_VAR(vector, __LINE__), sizeof(value_type),						\
			(uint8_t*)STACK_VAR(buff, __LINE__), buffer_size,						\
			VECTOR_FLAG_INITZERO);													\
	STACK_VAR(vector, __LINE__);													\
})

/*
 * ----------------------------------------------------------------------------
 * Vector functions to be moved to cf_vector
 * ----------------------------------------------------------------------------
 */

/**
 * Convert a vector to an array.
 * FIXME: return pointer to the internal vector storage.
 */
static cf_node*
vector_to_array(cf_vector* vector)
{
	return (cf_node*)vector->vector;
}

/**
 * Clear / delete all entries in a vector.
 */
static void
vector_clear(cf_vector* vector)
{
	cf_vector_delete_range(vector, 0, cf_vector_size(vector));
}

/**
 * Find the index of an element in the vector. Equality is based on mem compare.
 *
 * @param vector the source vector.
 * @param element the element to find.
 * @return the index if the element is found, -1 otherwise.
 */
static int
vector_find(cf_vector* vector, const void* element)
{
	int element_count = cf_vector_size(vector);
	size_t value_len = VECTOR_ELEM_SZ(vector);
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* src_element = cf_vector_getp(vector, i);
		if (src_element) {
			if (memcmp(element, src_element, value_len) == 0) {
				return i;
			}
		}
	}
	return -1;
}

/**
 * Copy all elements form the source vector to the destination vector to the
 * destination vector. Assumes the source and destination vector are not being
 * modified while the copy operation is in progress.
 *
 * @param dest the destination vector.
 * @param src the source vector.
 * @return the number of elements copied.
 */
static int
vector_copy(cf_vector* dest, cf_vector* src)
{
	int element_count = cf_vector_size(src);
	int copied_count = 0;
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* src_element = cf_vector_getp(src, i);
		if (src_element) {
			cf_vector_append(dest, src_element);
			copied_count++;
		}
	}
	return copied_count;
}

/**
 * Generate a hash code for a blob using Jenkins hash function.
 */
static uint32_t
exchange_blob_hash(const uint8_t* value, size_t value_size)
{
	uint32_t hash = 0;
	for (int i = 0; i < value_size; ++i) {
		hash += value[i];
		hash += (hash << 10);
		hash ^= (hash >> 6);
	}
	hash += (hash << 3);
	hash ^= (hash >> 11);
	hash += (hash << 15);

	return hash;
}

/**
 * Generate a hash code for a mesh node key.
 */
static uint32_t
exchange_vinfo_shash(const void* value)
{
	return exchange_blob_hash((const uint8_t*)value,
			sizeof(as_partition_version));
}

/*
 * ----------------------------------------------------------------------------
 * Clustering external event publisher
 * ----------------------------------------------------------------------------
 */

/**
 * * Check if event publisher is running.
 */
static bool
exchange_external_event_publisher_is_running()
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();
	bool running = g_external_event_publisher.sys_state
			== AS_EXCHANGE_SYS_STATE_RUNNING;
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
	return running;
}

/**
 * Initialize the event publisher.
 */
static void
exchange_external_event_publisher_init()
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();
	memset(&g_external_event_publisher, 0, sizeof(g_external_event_publisher));
	cf_vector_init(&g_external_event_publisher.published_succession_list,
			sizeof(cf_node),
			AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);

	pthread_mutex_init(&g_external_event_publisher.is_pending_mutex, NULL);
	pthread_cond_init(&g_external_event_publisher.is_pending, NULL);
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
}

/**
 * Register a clustering event listener.
 */
static void
exchange_external_event_listener_register(
		as_exchange_cluster_changed_cb event_callback, void* udata)
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();

	if (g_external_event_publisher.event_listener_count
			>= AS_EXTERNAL_EVENT_LISTENER_MAX) {
		CRASH("cannot register more than %d event listeners",
				AS_EXTERNAL_EVENT_LISTENER_MAX);
	}

	g_external_event_publisher.event_listeners[g_external_event_publisher.event_listener_count].event_callback =
			event_callback;
	g_external_event_publisher.event_listeners[g_external_event_publisher.event_listener_count].udata =
			udata;
	g_external_event_publisher.event_listener_count++;

	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
}

/**
 * Wakeup the publisher thread.
 */
static void
exchange_external_event_publisher_thr_wakeup()
{
	pthread_mutex_lock(&g_external_event_publisher.is_pending_mutex);
	pthread_cond_signal(&g_external_event_publisher.is_pending);
	pthread_mutex_unlock(&g_external_event_publisher.is_pending_mutex);
}

/**
 * Queue up and external event to publish.
 */
static void
exchange_external_event_queue(as_exchange_cluster_changed_event* event)
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();
	memcpy(&g_external_event_publisher.to_publish, event,
			sizeof(g_external_event_publisher.to_publish));

	vector_clear(&g_external_event_publisher.published_succession_list);
	if (event->succession) {
		// Use the static list for the published event, so that the input event
		// object can be destroyed irrespective of when the it is published.
		for (int i = 0; i < event->cluster_size; i++) {
			cf_vector_append(
					&g_external_event_publisher.published_succession_list,
					&event->succession[i]);
		}
		g_external_event_publisher.to_publish.succession = vector_to_array(
				&g_external_event_publisher.published_succession_list);

	}
	else {
		g_external_event_publisher.to_publish.succession = NULL;
	}

	g_external_event_publisher.event_queued = true;

	EXTERNAL_EVENT_PUBLISHER_UNLOCK();

	// Wake up the publisher thread.
	exchange_external_event_publisher_thr_wakeup();
}

/**
 * Publish external events if any are pending.
 */
static void
exchange_external_events_publish()
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();

	if (g_external_event_publisher.event_queued) {
		g_external_event_publisher.event_queued = false;
		for (uint32_t i = 0;
				i < g_external_event_publisher.event_listener_count; i++) {
			(g_external_event_publisher.event_listeners[i].event_callback)(
					&g_external_event_publisher.to_publish,
					g_external_event_publisher.event_listeners[i].udata);
		}
	}
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
}

/**
 * External event publisher thread.
 */
static void*
exchange_external_event_publisher_thr(void* arg)
{
	pthread_mutex_lock(&g_external_event_publisher.is_pending_mutex);

	while (true) {
		pthread_cond_wait(&g_external_event_publisher.is_pending,
				&g_external_event_publisher.is_pending_mutex);
		if (exchange_external_event_publisher_is_running()) {
			exchange_external_events_publish();
		}
		else {
			// Publisher stopped, exit the tread.
			break;
		}
	}

	return NULL;
}

/**
 * Start the event publisher.
 */
static void
exchange_external_event_publisher_start()
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state = AS_EXCHANGE_SYS_STATE_RUNNING;
	g_external_event_publisher.event_publisher_tid =
			cf_thread_create_joinable(exchange_external_event_publisher_thr,
					NULL);
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
}

/**
 * Stop the event publisher.
 */
static void
external_event_publisher_stop()
{
	EXTERNAL_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state = AS_EXCHANGE_SYS_STATE_SHUTTING_DOWN;
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();

	exchange_external_event_publisher_thr_wakeup();
	cf_thread_join(g_external_event_publisher.event_publisher_tid);

	EXTERNAL_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state = AS_EXCHANGE_SYS_STATE_STOPPED;
	g_external_event_publisher.event_queued = false;
	EXTERNAL_EVENT_PUBLISHER_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Node state related
 * ----------------------------------------------------------------------------
 */

/**
 * Initialize node state.
 */
static void
exchange_node_state_init(as_exchange_node_state* node_state)
{
	memset(node_state, 0, sizeof(*node_state));

	node_state->data = cf_calloc(1, sizeof(as_exchange_node_data));
}

/**
 * Reset node state.
 */
static void
exchange_node_state_reset(as_exchange_node_state* node_state)
{
	node_state->send_acked = false;
	node_state->received = false;
	node_state->is_ready_to_commit = false;

	node_state->data->num_namespaces = 0;
	for (int i = 0; i < AS_NAMESPACE_SZ; i++) {
		node_state->data->namespace_data[i].local_namespace = NULL;
	}
}

/**
 * Destroy node state.
 */
static void
exchange_node_state_destroy(as_exchange_node_state* node_state)
{
	for (int i = 0; i < AS_NAMESPACE_SZ; i++) {
		if (node_state->data->namespace_data[i].partition_versions) {
			cf_free(node_state->data->namespace_data[i].partition_versions);
		}

		if (node_state->data->namespace_data[i].roster) {
			cf_free(node_state->data->namespace_data[i].roster);
		}

		if (node_state->data->namespace_data[i].roster_rack_ids) {
			cf_free(node_state->data->namespace_data[i].roster_rack_ids);
		}
	}

	cf_free(node_state->data);
}

/**
 * Reduce function to match node -> node state hash to the succession list.
 * Should always be invoked under a lock over the main hash.
 */
static int
exchange_node_states_reset_reduce(const void* key, void* data, void* udata)
{
	const cf_node* node = (const cf_node*)key;
	as_exchange_node_state* node_state = (as_exchange_node_state*)data;

	int node_index = vector_find(&g_exchange.succession_list, node);
	if (node_index < 0) {
		// Node not in succession list
		exchange_node_state_destroy(node_state);
		return CF_SHASH_REDUCE_DELETE;
	}

	exchange_node_state_reset(node_state);
	return CF_SHASH_OK;
}

/**
 * Adjust the nodeid_to_node_state hash to have an entry for every node in the
 * succession list with state reset for a new round of exchange. Removes entries
 * not in the succession list.
 */
static void
exchange_node_states_reset()
{
	EXCHANGE_LOCK();

	// Fix existing entries by reseting entries in succession and removing
	// entries not in succession list.
	cf_shash_reduce(g_exchange.nodeid_to_node_state,
			exchange_node_states_reset_reduce, NULL);

	// Add missing entries.
	int succession_length = cf_vector_size(&g_exchange.succession_list);

	as_exchange_node_state temp_state;
	for (int i = 0; i < succession_length; i++) {
		cf_node nodeid;

		cf_vector_get(&g_exchange.succession_list, i, &nodeid);
		if (cf_shash_get(g_exchange.nodeid_to_node_state, &nodeid, &temp_state)
				== CF_SHASH_ERR_NOT_FOUND) {
			exchange_node_state_init(&temp_state);

			cf_shash_put(g_exchange.nodeid_to_node_state, &nodeid, &temp_state);
		}
	}

	EXCHANGE_UNLOCK();
}

/**
 * Reduce function to find nodes that had not acked self node's exchange data.
 */
static int
exchange_nodes_find_send_unacked_reduce(const void* key, void* data,
		void* udata)
{
	const cf_node* node = (const cf_node*)key;
	as_exchange_node_state* node_state = (as_exchange_node_state*)data;
	cf_vector* unacked = (cf_vector*)udata;

	if (!node_state->send_acked) {
		cf_vector_append(unacked, node);
	}
	return CF_SHASH_OK;
}

/**
 * Find nodes that have not acked self node's exchange data.
 */
static void
exchange_nodes_find_send_unacked(cf_vector* unacked)
{
	cf_shash_reduce(g_exchange.nodeid_to_node_state,
			exchange_nodes_find_send_unacked_reduce, unacked);
}

/**
 * Reduce function to find peer nodes from whom self node has not received
 * exchange data.
 */
static int
exchange_nodes_find_not_received_reduce(const void* key, void* data,
		void* udata)
{
	const cf_node* node = (const cf_node*)key;
	as_exchange_node_state* node_state = (as_exchange_node_state*)data;
	cf_vector* not_received = (cf_vector*)udata;

	if (!node_state->received) {
		cf_vector_append(not_received, node);
	}
	return CF_SHASH_OK;
}

/**
 * Find peer nodes from whom self node has not received exchange data.
 */
static void
exchange_nodes_find_not_received(cf_vector* not_received)
{
	cf_shash_reduce(g_exchange.nodeid_to_node_state,
			exchange_nodes_find_not_received_reduce, not_received);
}

/**
 * Reduce function to find peer nodes that are not ready to commit.
 */
static int
exchange_nodes_find_not_ready_to_commit_reduce(const void* key, void* data,
		void* udata)
{
	const cf_node* node = (const cf_node*)key;
	as_exchange_node_state* node_state = (as_exchange_node_state*)data;
	cf_vector* not_ready_to_commit = (cf_vector*)udata;

	if (!node_state->is_ready_to_commit) {
		cf_vector_append(not_ready_to_commit, node);
	}
	return CF_SHASH_OK;
}

/**
 * Find peer nodes that are not ready to commit.
 */
static void
exchange_nodes_find_not_ready_to_commit(cf_vector* not_ready_to_commit)
{
	cf_shash_reduce(g_exchange.nodeid_to_node_state,
			exchange_nodes_find_not_ready_to_commit_reduce,
			not_ready_to_commit);
}

/**
 * Update the node state for a node.
 */
static void
exchange_node_state_update(cf_node nodeid, as_exchange_node_state* node_state)
{
	cf_shash_put(g_exchange.nodeid_to_node_state, &nodeid, node_state);
}

/**
 * Get state of a node from the hash. If not found crash because this entry
 * should be present in the hash.
 */
static void
exchange_node_state_get_safe(cf_node nodeid, as_exchange_node_state* node_state)
{
	if (cf_shash_get(g_exchange.nodeid_to_node_state, &nodeid, node_state)
			== CF_SHASH_ERR_NOT_FOUND) {
		CRASH(
				"node entry for node %"PRIx64"  missing from node state hash", nodeid);
	}
}

/*
 * ----------------------------------------------------------------------------
 * Message related
 * ----------------------------------------------------------------------------
 */

/**
 * Fill compulsary fields in a message common to all message types.
 */
static void
exchange_msg_src_fill(msg* msg, as_exchange_msg_type type)
{
	EXCHANGE_LOCK();
	msg_set_uint32(msg, AS_EXCHANGE_MSG_ID, AS_EXCHANGE_PROTOCOL_IDENTIFIER);
	msg_set_uint64(msg, AS_EXCHANGE_MSG_CLUSTER_KEY, g_exchange.cluster_key);
	msg_set_uint32(msg, AS_EXCHANGE_MSG_TYPE, type);
	EXCHANGE_UNLOCK();
}

/**
 * Get the msg buffer from a pool and fill in all compulsory fields.
 * @return the msg buff with compulsory fields filled in.
 */
static msg*
exchange_msg_get(as_exchange_msg_type type)
{
	msg* msg = as_fabric_msg_get(M_TYPE_EXCHANGE);
	exchange_msg_src_fill(msg, type);
	return msg;
}

/**
 * Return the message buffer back to the pool.
 */
static void
exchange_msg_return(msg* msg)
{
	as_fabric_msg_put(msg);
}

/**
 * Get message id.
 */
static int
exchange_msg_id_get(msg* msg, uint32_t* msg_id)
{
	if (msg_get_uint32(msg, AS_EXCHANGE_MSG_ID, msg_id) != 0) {
		return -1;
	}
	return 0;
}

/**
 * Get message type.
 */
static int
exchange_msg_type_get(msg* msg, as_exchange_msg_type* msg_type)
{
	if (msg_get_uint32(msg, AS_EXCHANGE_MSG_TYPE, msg_type) != 0) {
		return -1;
	}
	return 0;
}

/**
 * Get message cluster key.
 */
static int
exchange_msg_cluster_key_get(msg* msg, as_cluster_key* cluster_key)
{
	if (msg_get_uint64(msg, AS_EXCHANGE_MSG_CLUSTER_KEY, cluster_key) != 0) {
		return -1;
	}
	return 0;
}

/**
 * Set data payload for a message.
 */
static void
exchange_msg_data_payload_set(msg* msg)
{
	uint32_t ns_count = g_config.n_namespaces;

	cf_vector_define(namespace_list, sizeof(msg_buf_ele), ns_count, 0);
	cf_vector_define(partition_versions, sizeof(msg_buf_ele), ns_count, 0);
	uint32_t rack_ids[ns_count];

	bool have_roster = false;
	bool have_roster_rack_ids = false;
	uint32_t roster_generations[ns_count];
	cf_vector_define(rosters, sizeof(msg_buf_ele), ns_count, 0);
	cf_vector_define(rosters_rack_ids, sizeof(msg_buf_ele), ns_count, 0);

	bool have_regimes = false;
	uint32_t eventual_regimes[ns_count];
	uint32_t rebalance_regimes[ns_count];

	bool have_rebalance_flags = false;
	uint32_t rebalance_flags[ns_count];

	memset(rebalance_flags, 0, sizeof(rebalance_flags));

	msg_set_uint32(msg, AS_EXCHANGE_MSG_COMPATIBILITY_ID,
			AS_EXCHANGE_COMPATIBILITY_ID);

	for (uint32_t ns_ix = 0; ns_ix < ns_count; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		msg_buf_ele ns_ele = {
			.sz = (uint32_t)strlen(ns->name),
			.ptr = (uint8_t*)ns->name
		};

		msg_buf_ele pv_ele = {
			.sz = (uint32_t)g_exchange.self_data_dyn_buf[ns_ix].used_sz,
			.ptr = g_exchange.self_data_dyn_buf[ns_ix].buf
		};

		msg_buf_ele rn_ele = {
			.sz = (uint32_t)(ns->smd_roster_count * sizeof(cf_node)),
			.ptr = (uint8_t*)ns->smd_roster
		};

		msg_buf_ele rri_ele = {
			.sz = (uint32_t)(ns->smd_roster_count * sizeof(uint32_t)),
			.ptr = (uint8_t*)ns->smd_roster_rack_ids
		};

		cf_vector_append(&namespace_list, &ns_ele);
		cf_vector_append(&partition_versions, &pv_ele);
		rack_ids[ns_ix] = ns->rack_id;

		if (ns->smd_roster_generation != 0) {
			have_roster = true;

			if (! have_roster_rack_ids) {
				for (uint32_t n = 0; n < ns->smd_roster_count; n++) {
					if (ns->smd_roster_rack_ids[n] != 0) {
						have_roster_rack_ids = true;
						break;
					}
				}
			}
		}

		roster_generations[ns_ix] = ns->smd_roster_generation;
		cf_vector_append(&rosters, &rn_ele);
		cf_vector_append(&rosters_rack_ids, &rri_ele);

		eventual_regimes[ns_ix] = ns->eventual_regime;
		rebalance_regimes[ns_ix] = ns->rebalance_regime;

		if (eventual_regimes[ns_ix] != 0 || rebalance_regimes[ns_ix] != 0) {
			have_regimes = true;
		}

		if (ns->cfg_prefer_uniform_balance) {
			rebalance_flags[ns_ix] |= AS_EXCHANGE_REBALANCE_FLAG_UNIFORM;
		}

		if (ns->pending_quiesce) {
			rebalance_flags[ns_ix] |= AS_EXCHANGE_REBALANCE_FLAG_QUIESCE;
		}

		if (rebalance_flags[ns_ix] != 0) {
			have_rebalance_flags = true;
		}
	}

	msg_msgpack_list_set_buf(msg, AS_EXCHANGE_MSG_NAMESPACES, &namespace_list);
	msg_msgpack_list_set_buf(msg, AS_EXCHANGE_MSG_NS_PARTITION_VERSIONS,
			&partition_versions);
	msg_msgpack_list_set_uint32(msg, AS_EXCHANGE_MSG_NS_RACK_IDS, rack_ids,
			ns_count);

	if (have_roster) {
		msg_msgpack_list_set_uint32(msg, AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS,
				roster_generations, ns_count);
		msg_msgpack_list_set_buf(msg, AS_EXCHANGE_MSG_NS_ROSTERS, &rosters);

		if (have_roster_rack_ids) {
			msg_msgpack_list_set_buf(msg, AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS,
					&rosters_rack_ids);
		}
	}

	if (have_regimes) {
		msg_msgpack_list_set_uint32(msg, AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES,
				eventual_regimes, ns_count);
		msg_msgpack_list_set_uint32(msg, AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES,
				rebalance_regimes, ns_count);
	}

	if (have_rebalance_flags) {
		msg_msgpack_list_set_uint32(msg, AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS,
				rebalance_flags, ns_count);
	}
}

/**
 * Check sanity of an incoming message. If this check passes the message is
 * guaranteed to have valid protocol identifier, valid type and valid matching
 * cluster key with source node being a part of the cluster.
 * @return 0 if the message in valid, -1 if the message is invalid and should be
 * ignored.
 */
static bool
exchange_msg_is_sane(cf_node source, msg* msg)
{
	uint32_t id = 0;
	if (exchange_msg_id_get(msg, &id) != 0||
	id != AS_EXCHANGE_PROTOCOL_IDENTIFIER) {
		DEBUG(
				"received exchange message with mismatching identifier - expected %u but was  %u",
				AS_EXCHANGE_PROTOCOL_IDENTIFIER, id);
		return false;
	}

	as_exchange_msg_type msg_type = 0;

	if (exchange_msg_type_get(msg, &msg_type) != 0
			|| msg_type >= AS_EXCHANGE_MSG_TYPE_SENTINEL) {
		WARNING("received exchange message with invalid message type  %u",
				msg_type);
		return false;
	}

	EXCHANGE_LOCK();
	as_cluster_key current_cluster_key = g_exchange.cluster_key;
	bool is_in_cluster = vector_find(&g_exchange.succession_list, &source) >= 0;
	EXCHANGE_UNLOCK();

	if (!is_in_cluster) {
		DEBUG("received exchange message from node %"PRIx64" not in cluster",
				source);
		return false;
	}

	as_cluster_key incoming_cluster_key = 0;
	if (exchange_msg_cluster_key_get(msg, &incoming_cluster_key) != 0
			|| (current_cluster_key != incoming_cluster_key)
			|| current_cluster_key == 0) {
		DEBUG("received exchange message with mismatching cluster key - expected %"PRIx64" but was  %"PRIx64,
				current_cluster_key, incoming_cluster_key);
		return false;
	}

	return true;
}

/**
 * Send a message over fabric.
 *
 * @param msg the message to send.
 * @param dest the desination node.
 * @param error_msg the error message.
 */
static void
exchange_msg_send(msg* msg, cf_node dest, char* error_msg)
{
	if (as_fabric_send(dest, msg, AS_FABRIC_CHANNEL_CTRL)) {
		// Fabric will not return the message to the pool. Do it ourself.
		exchange_msg_return(msg);
		WARNING("%s (dest:%"PRIx64")", error_msg, dest);
	}
}

/**
 * Send a message over to a list of destination nodes.
 *
 * @param msg the message to send.
 * @param dests the node list to send the message to.
 * @param num_dests the number of destination nodes.
 * @param error_msg the error message.
 */
static void
exchange_msg_send_list(msg* msg, cf_node* dests, int num_dests, char* error_msg)
{
	if (as_fabric_send_list(dests, num_dests, msg, AS_FABRIC_CHANNEL_CTRL)
			!= 0) {
		// Fabric will not return the message to the pool. Do it ourself.
		exchange_msg_return(msg);
		as_clustering_log_cf_node_array(CF_WARNING, AS_EXCHANGE, error_msg,
				dests, num_dests);
	}
}

/**
 * Send a commit message to a destination node.
 * @param dest the destination node.
 */
static void
exchange_commit_msg_send(cf_node dest)
{
	msg* commit_msg = exchange_msg_get(AS_EXCHANGE_MSG_TYPE_COMMIT);
	DEBUG("sending commit message to node %"PRIx64, dest);
	exchange_msg_send(commit_msg, dest, "error sending commit message");
}

/**
 * Send a commit message to a list of destination nodes.
 * @param dests the destination nodes.
 * @param num_dests the number of destination nodes.
 */
static void
exchange_commit_msg_send_all(cf_node* dests, int num_dests)
{
	msg* commit_msg = exchange_msg_get(AS_EXCHANGE_MSG_TYPE_COMMIT);
	as_clustering_log_cf_node_array(CF_DEBUG, AS_EXCHANGE,
			"sending commit message to nodes:", dests, num_dests);
	exchange_msg_send_list(commit_msg, dests, num_dests,
			"error sending commit message");
}

/**
 * Send ready to commit message to the principal.
 */
static void
exchange_ready_to_commit_msg_send()
{
	EXCHANGE_LOCK();
	g_exchange.ready_to_commit_send_ts = cf_getms();
	cf_node principal = g_exchange.principal;
	EXCHANGE_UNLOCK();

	msg* ready_to_commit_msg = exchange_msg_get(
			AS_EXCHANGE_MSG_TYPE_READY_TO_COMMIT);
	DEBUG("sending ready to commit message to node %"PRIx64, principal);
	exchange_msg_send(ready_to_commit_msg, principal,
			"error sending ready to commit message");
}

/**
 * Send exchange data to all nodes that have not acked the send.
 */
static void
exchange_data_msg_send_pending_ack()
{
	EXCHANGE_LOCK();
	g_exchange.send_ts = cf_getms();

	cf_node* unacked_nodes;
	int num_unacked_nodes;
	cf_vector* unacked_nodes_vector = cf_vector_stack_create(cf_node);

	exchange_nodes_find_send_unacked(unacked_nodes_vector);
	cf_vector_to_stack_array(unacked_nodes_vector, &unacked_nodes,
			&num_unacked_nodes);

	cf_vector_destroy(unacked_nodes_vector);

	if (!num_unacked_nodes) {
		goto Exit;
	}

	// FIXME - temporary assert, until we're sure.
	cf_assert(g_exchange.data_msg != NULL, AS_EXCHANGE, "payload not built");

	as_clustering_log_cf_node_array(CF_DEBUG, AS_EXCHANGE,
			"sending exchange data to nodes:", unacked_nodes,
			num_unacked_nodes);

	msg_incr_ref(g_exchange.data_msg);

	exchange_msg_send_list(g_exchange.data_msg, unacked_nodes,
			num_unacked_nodes, "error sending exchange data");
Exit:
	EXCHANGE_UNLOCK();
}

/**
 * Send a commit message to a destination node.
 * @param dest the destination node.
 */
static void
exchange_data_ack_msg_send(cf_node dest)
{
	msg* ack_msg = exchange_msg_get(AS_EXCHANGE_MSG_TYPE_DATA_ACK);
	DEBUG("sending data ack message to node %"PRIx64, dest);
	exchange_msg_send(ack_msg, dest, "error sending data ack message");
}

/*
 * ----------------------------------------------------------------------------
 * Data payload related
 * ----------------------------------------------------------------------------
 */

/**
 * Add a pid to the namespace hash for the input vinfo.
 */
static void
exchange_namespace_hash_pid_add(cf_shash* ns_hash, as_partition_version* vinfo,
		uint16_t pid)
{
	if (as_partition_version_is_null(vinfo)) {
		// Ignore NULL vinfos.
		return;
	}

	cf_vector* pid_vector;

	// Append the hash.
	if (cf_shash_get(ns_hash, vinfo, &pid_vector) != CF_SHASH_OK) {
		// We are seeing this vinfo for the first time.
		pid_vector = cf_vector_create(sizeof(uint16_t),
		AS_EXCHANGE_VINFO_NUM_PIDS_AVG, 0);
		cf_shash_put(ns_hash, vinfo, &pid_vector);
	}

	cf_vector_append(pid_vector, &pid);
}

/**
 * Destroy the pid vector for each vinfo.
 */
static int
exchange_namespace_hash_destroy_reduce(const void* key, void* data, void* udata)
{
	cf_vector* pid_vector = *(cf_vector**)data;
	cf_vector_destroy(pid_vector);
	return CF_SHASH_REDUCE_DELETE;
}

/**
 * Serialize each vinfo and accumulated pids to the input buffer.
 */
static int
exchange_namespace_hash_serialize_reduce(const void* key, void* data,
		void* udata)
{
	const as_partition_version* vinfo = (const as_partition_version*)key;
	cf_vector* pid_vector = *(cf_vector**)data;
	cf_dyn_buf* dyn_buf = (cf_dyn_buf*)udata;

	// Append the vinfo.
	cf_dyn_buf_append_buf(dyn_buf, (uint8_t*)vinfo, sizeof(*vinfo));

	// Append the count of pids.
	uint32_t num_pids = cf_vector_size(pid_vector);
	cf_dyn_buf_append_buf(dyn_buf, (uint8_t*)&num_pids, sizeof(num_pids));

	// Append each pid.
	for (int i = 0; i < num_pids; i++) {
		uint16_t* pid = cf_vector_getp(pid_vector, i);
		cf_dyn_buf_append_buf(dyn_buf, (uint8_t*)pid, sizeof(*pid));
	}

	return CF_SHASH_OK;
}

/**
 * Append namespace payload, in as_exchange_namespace_payload format, for a
 * namespace to the dynamic buffer.
 *
 * @param ns the namespace.
 * @param dyn_buf the dynamic buffer.
 */
static void
exchange_data_namespace_payload_add(as_namespace* ns, cf_dyn_buf* dyn_buf)
{
	// A hash from each unique non null vinfo to a vector of partition ids
	// having the vinfo.
	cf_shash* ns_hash = cf_shash_create(exchange_vinfo_shash,
			sizeof(as_partition_version), sizeof(cf_vector*),
			AS_EXCHANGE_UNIQUE_VINFO_MAX_SIZE_SOFT, 0);

	as_partition* partitions = ns->partitions;

	// Populate the hash with one entry for each vinfo
	for (int i = 0; i < AS_PARTITIONS; i++) {
		as_partition_version* current_vinfo = &partitions[i].version;
		exchange_namespace_hash_pid_add(ns_hash, current_vinfo, i);
	}

	// We are ready to populate the dyn buffer with this ns's data.
	DEBUG("namespace %s has %d unique vinfos", ns->name,
			cf_shash_get_size(ns_hash));

	// Append the vinfo count.
	uint32_t num_vinfos = cf_shash_get_size(ns_hash);
	cf_dyn_buf_append_buf(dyn_buf, (uint8_t*)&num_vinfos, sizeof(num_vinfos));

	// Append vinfos and partitions.
	cf_shash_reduce(ns_hash, exchange_namespace_hash_serialize_reduce, dyn_buf);

	// Destroy the intermediate hash and the pid vectors.
	cf_shash_reduce(ns_hash, exchange_namespace_hash_destroy_reduce, NULL);

	cf_shash_destroy(ns_hash);
}

/**
 * Prepare the exchanged data payloads for current exchange round.
 */
static void
exchange_data_payload_prepare()
{
	EXCHANGE_LOCK();

	// Block / abort migrations and freeze the partition version infos.
	as_partition_balance_disallow_migrations();
	as_partition_balance_synchronize_migrations();

	// Ensure ns->smd_roster is synchronized exchanged partition versions.
	pthread_mutex_lock(&g_exchanged_info_lock);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// May change flags on partition versions we're about to exchange.
		as_partition_balance_protect_roster_set(ns);

		// Append payload for each namespace.

		// TODO - add API to reset dynbuf?
		g_exchange.self_data_dyn_buf[ns_ix].used_sz = 0;

		exchange_data_namespace_payload_add(ns,
				&g_exchange.self_data_dyn_buf[ns_ix]);
	}

	g_exchange.data_msg = exchange_msg_get(AS_EXCHANGE_MSG_TYPE_DATA);
	exchange_msg_data_payload_set(g_exchange.data_msg);

	pthread_mutex_unlock(&g_exchanged_info_lock);

	EXCHANGE_UNLOCK();
}

/**
 * Indicates if the per-namespace fields in an incoming data message are valid.
 *
 * @return number of namespaces.
 */
static uint32_t
exchange_data_msg_get_num_namespaces(as_exchange_event* msg_event)
{
	uint32_t num_namespaces_sent = 0;
	uint32_t num_namespace_elements_sent = 0;

	if (!msg_msgpack_list_get_count(msg_event->msg,
			AS_EXCHANGE_MSG_NAMESPACES, &num_namespaces_sent)
			|| num_namespaces_sent > AS_NAMESPACE_SZ) {
		WARNING("received invalid namespaces from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (!msg_msgpack_list_get_count(msg_event->msg,
			AS_EXCHANGE_MSG_NS_PARTITION_VERSIONS, &num_namespace_elements_sent)
			|| num_namespaces_sent != num_namespace_elements_sent) {
		WARNING("received invalid partition versions from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (!msg_msgpack_list_get_count(msg_event->msg,
			AS_EXCHANGE_MSG_NS_RACK_IDS, &num_namespace_elements_sent)
			|| num_namespaces_sent != num_namespace_elements_sent) {
		WARNING("received invalid cluster groups from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid roster generations from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTERS)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_ROSTERS,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid rosters from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid rosters-rack-ids from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid eventual regimes from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid rebalance regimes from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS)
			&& (!msg_msgpack_list_get_count(msg_event->msg,
					AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS,
					&num_namespace_elements_sent)
					|| num_namespaces_sent != num_namespace_elements_sent)) {
		WARNING("received invalid rebalance flags from node %"PRIx64,
				msg_event->msg_source);
		return 0;
	}

	return num_namespaces_sent;
}

/**
 * Basic validation for incoming namespace payload.
 * Validates that
 * 	1. Number of vinfos < AS_PARTITIONS.
 * 	2. Each partition is between 0 and AS_PARTITIONS.
 * 	3. Namespaces payload does not exceed payload_end_ptr.
 *
 * @param ns_payload pointer to start of the namespace payload.
 * @param ns_payload_size the size of the input namespace payload.
 * @return true if this is a valid payload.
 */
static bool
exchange_namespace_payload_is_valid(as_exchange_ns_vinfos_payload* ns_payload,
		uint32_t ns_payload_size)
{
	// Pointer past the last byte in the payload.
	uint8_t* payload_end_ptr = (uint8_t*)ns_payload + ns_payload_size;

	if ((uint8_t*)ns_payload->vinfos > payload_end_ptr) {
		return false;
	}

	if (ns_payload->num_vinfos > AS_PARTITIONS) {
		return false;
	}

	uint8_t* read_ptr = (uint8_t*)ns_payload->vinfos;

	for (uint32_t i = 0; i < ns_payload->num_vinfos; i++) {
		if (read_ptr >= payload_end_ptr) {
			return false;
		}

		as_exchange_vinfo_payload* vinfo_payload =
				(as_exchange_vinfo_payload*)read_ptr;

		if ((uint8_t*)vinfo_payload->pids > payload_end_ptr) {
			return false;
		}

		if (vinfo_payload->num_pids > AS_PARTITIONS) {
			return false;
		}

		size_t pids_size = vinfo_payload->num_pids * sizeof(uint16_t);

		if ((uint8_t*)vinfo_payload->pids + pids_size > payload_end_ptr) {
			return false;
		}

		for (uint32_t j = 0; j < vinfo_payload->num_pids; j++) {
			if (vinfo_payload->pids[j] >= AS_PARTITIONS) {
				return false;
			}
		}

		read_ptr += sizeof(as_exchange_vinfo_payload) + pids_size;
	}

	if (read_ptr != payload_end_ptr) {
		// There are unaccounted for extra bytes in the payload.
		return false;
	}

	return true;
}

/*
 * ----------------------------------------------------------------------------
 * Common across all states
 * ----------------------------------------------------------------------------
 */

/**
 * Update committed exchange cluster data.
 * @param cluster_key the new cluster key.
 * @param succession the new succession. Can be NULL, for orphan state.
 */
static void
exchange_commited_cluster_update(as_cluster_key cluster_key,
								 cf_vector* succession)
{
	EXCHANGE_COMMITTED_CLUSTER_WLOCK();
	g_exchange.committed_cluster_key = cluster_key;
	vector_clear(&g_exchange.committed_succession_list);

	if (succession && cf_vector_size(succession) > 0) {
		vector_copy(&g_exchange.committed_succession_list,
					&g_exchange.succession_list);
		g_exchange.committed_cluster_size = cf_vector_size(succession);
		cf_vector_get(succession, 0, &g_exchange.committed_principal);
	}
	else {
		g_exchange.committed_cluster_size = 0;
		g_exchange.committed_principal = 0;
	}

	g_exchange.committed_cluster_generation++;
	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();
}

/**
 * Indicates if self node is the cluster principal.
 */
static bool
exchange_self_is_principal()
{
	EXCHANGE_LOCK();
	bool is_principal = (g_config.self_node == g_exchange.principal);
	EXCHANGE_UNLOCK();
	return is_principal;
}

/**
 * Dump exchange state.
 */
static void
exchange_dump(cf_fault_severity severity, bool verbose)
{
	EXCHANGE_LOCK();
	cf_vector* node_vector = cf_vector_stack_create(cf_node);

	char* state_str = "";
	switch (g_exchange.state) {
	case AS_EXCHANGE_STATE_REST:
		state_str = "rest";
		break;
	case AS_EXCHANGE_STATE_EXCHANGING:
		state_str = "exchanging";
		break;
	case AS_EXCHANGE_STATE_READY_TO_COMMIT:
		state_str = "ready to commit";
		break;
	case AS_EXCHANGE_STATE_ORPHANED:
		state_str = "orphaned";
		break;
	}

	LOG(severity, "EXG: state: %s", state_str);

	if (g_exchange.state == AS_EXCHANGE_STATE_ORPHANED) {
		LOG(severity, "EXG: client transactions blocked: %s",
				g_exchange.orphan_state_are_transactions_blocked ?
						"true" : "false");
		LOG(severity, "EXG: orphan since: %"PRIu64"(millis)",
				cf_getms() - g_exchange.orphan_state_start_time);
	}
	else {
		LOG(severity, "EXG: cluster key: %"PRIx64, g_exchange.cluster_key);
		as_clustering_log_cf_node_vector(severity, AS_EXCHANGE,
				"EXG: succession:", &g_exchange.succession_list);

		if (verbose) {
			vector_clear(node_vector);
			exchange_nodes_find_send_unacked(node_vector);
			as_clustering_log_cf_node_vector(severity, AS_EXCHANGE,
					"EXG: send pending:", node_vector);

			vector_clear(node_vector);
			exchange_nodes_find_not_received(node_vector);
			as_clustering_log_cf_node_vector(severity, AS_EXCHANGE,
					"EXG: receive pending:", node_vector);

			if (exchange_self_is_principal()) {
				vector_clear(node_vector);
				exchange_nodes_find_not_ready_to_commit(node_vector);
				as_clustering_log_cf_node_vector(severity, AS_EXCHANGE,
						"EXG: ready to commit pending:", node_vector);
			}
		}
	}

	cf_vector_destroy(node_vector);
	EXCHANGE_UNLOCK();
}

/**
 * Reset state for new round of exchange, while reusing as mush heap allocated
 * space for exchanged data.
 * @param new_succession_list new succession list. Can be NULL for orphaned
 * state.
 * @param new_cluster_key 0 for orphaned state.
 */
static void
exchange_reset_for_new_round(cf_vector* new_succession_list,
		as_cluster_key new_cluster_key)
{
	EXCHANGE_LOCK();
	vector_clear(&g_exchange.succession_list);
	g_exchange.principal = 0;

	if (new_succession_list && cf_vector_size(new_succession_list) > 0) {
		vector_copy(&g_exchange.succession_list, new_succession_list);
		// Set the principal node.
		cf_vector_get(&g_exchange.succession_list, 0, &g_exchange.principal);
	}

	// Reset accumulated node states.
	exchange_node_states_reset();

	g_exchange.cluster_key = new_cluster_key;

	if (g_exchange.data_msg) {
		as_fabric_msg_put(g_exchange.data_msg);
		g_exchange.data_msg = NULL;
	}

	EXCHANGE_UNLOCK();
}

/**
 * Commit exchange state to reflect self node being an orphan.
 */
static void
exchange_orphan_commit()
{
	EXCHANGE_LOCK();
	exchange_commited_cluster_update(0, NULL);
	WARNING("blocking client transactions in orphan state!");
	as_partition_balance_revert_to_orphan();
	g_exchange.orphan_state_are_transactions_blocked = true;
	EXCHANGE_UNLOCK();
}

/**
 * Receive an orphaned event and abort current round.
 */
static void
exchange_orphaned_handle(as_clustering_event* orphaned_event)
{
	DEBUG("got orphaned event");

	EXCHANGE_LOCK();

	if (g_exchange.state != AS_EXCHANGE_STATE_REST
			&& g_exchange.state != AS_EXCHANGE_STATE_ORPHANED) {
		INFO("aborting partition exchange with cluster key %"PRIx64,
				g_exchange.cluster_key);
	}

	g_exchange.state = AS_EXCHANGE_STATE_ORPHANED;
	exchange_reset_for_new_round(NULL, 0);

	// Stop ongoing migrations if any.
	as_partition_balance_disallow_migrations();
	as_partition_balance_synchronize_migrations();

	// Update the time this node got into orphan state.
	g_exchange.orphan_state_start_time = cf_getms();

	// Potentially temporary orphan state. We will timeout and commit orphan
	// state if this persists for long.
	g_exchange.orphan_state_are_transactions_blocked = false;

	EXCHANGE_UNLOCK();
}

/**
 * Receive a cluster change event and start a new data exchange round.
 */
static void
exchange_cluster_change_handle(as_clustering_event* clustering_event)
{
	EXCHANGE_LOCK();

	DEBUG("got cluster change event");

	if (g_exchange.state != AS_EXCHANGE_STATE_REST
			&& g_exchange.state != AS_EXCHANGE_STATE_ORPHANED) {
		INFO("aborting partition exchange with cluster key %"PRIx64,
				g_exchange.cluster_key);
	}

	exchange_reset_for_new_round(clustering_event->succession_list,
			clustering_event->cluster_key);

	g_exchange.state = AS_EXCHANGE_STATE_EXCHANGING;

	INFO("data exchange started with cluster key %"PRIx64,
			g_exchange.cluster_key);

	// Prepare the data payload.
	exchange_data_payload_prepare();

	EXCHANGE_UNLOCK();

	exchange_data_msg_send_pending_ack();
}

/**
 * Handle a cluster change event.
 * @param cluster_change_event the cluster change event.
 */
static void
exchange_clustering_event_handle(as_exchange_event* exchange_clustering_event)
{
	as_clustering_event* clustering_event =
			exchange_clustering_event->clustering_event;

	switch (clustering_event->type) {
	case AS_CLUSTERING_ORPHANED:
		exchange_orphaned_handle(clustering_event);
		break;
	case AS_CLUSTERING_CLUSTER_CHANGED:
		exchange_cluster_change_handle(clustering_event);
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Orphan state event handling
 * ----------------------------------------------------------------------------
 */

/**
 * The wait time in orphan state after which client transactions and transaction
 * related interactions (e.g. valid partition map publishing) should be blocked.
 */
static uint32_t
exchange_orphan_transaction_block_timeout()
{
	return (uint32_t)as_clustering_quantum_interval()
			* AS_EXCHANGE_REVERT_ORPHAN_INTERVALS;
}

/**
 * Handle the timer event and if we have been an orphan for too long, block
 * client transactions.
 */
static void
exchange_orphan_timer_event_handle()
{
	uint32_t timeout = exchange_orphan_transaction_block_timeout();
	EXCHANGE_LOCK();
	if (!g_exchange.orphan_state_are_transactions_blocked
			&& g_exchange.orphan_state_start_time + timeout < cf_getms()) {
		exchange_orphan_commit();
	}
	EXCHANGE_UNLOCK();
}

/**
 * Event processing in the orphan state.
 */
static void
exchange_orphan_event_handle(as_exchange_event* event)
{
	switch (event->type) {
	case AS_EXCHANGE_EVENT_CLUSTER_CHANGE:
		exchange_clustering_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_TIMER:
		exchange_orphan_timer_event_handle();
		break;
	default:
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Rest state event handling
 * ----------------------------------------------------------------------------
 */

/**
 * Process a message event when in rest state.
 */
static void
exchange_rest_msg_event_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	if (!exchange_msg_is_sane(msg_event->msg_source, msg_event->msg)) {
		goto Exit;
	}

	as_exchange_msg_type msg_type;
	exchange_msg_type_get(msg_event->msg, &msg_type);

	if (exchange_self_is_principal()
			&& msg_type == AS_EXCHANGE_MSG_TYPE_READY_TO_COMMIT) {
		// The commit message did not make it to the source node, hence it send
		// us the ready to commit message. Resend the commit message.
		DEBUG("received a ready to commit message from %"PRIx64,
				msg_event->msg_source);
		exchange_commit_msg_send(msg_event->msg_source);
	}
	else {
		DEBUG(
				"rest state received unexpected mesage of type %d from node %"PRIx64,
				msg_type, msg_event->msg_source);

	}

Exit:

	EXCHANGE_UNLOCK();
}

/**
 * Event processing in the rest state.
 */
static void
exchange_rest_event_handle(as_exchange_event* event)
{
	switch (event->type) {
	case AS_EXCHANGE_EVENT_CLUSTER_CHANGE:
		exchange_clustering_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_MSG:
		exchange_rest_msg_event_handle(event);
		break;
	default:
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Exchanging state event handling
 * ----------------------------------------------------------------------------
 */

/**
 * Commit namespace payload for a node.
 * Assumes the namespace vinfo and succession list have been zero set before.
 */
static void
exchange_namespace_payload_pre_commit_for_node(cf_node node,
		as_exchange_node_namespace_data* namespace_data)
{
	as_namespace* ns = namespace_data->local_namespace;

	uint32_t sl_ix = ns->cluster_size++;

	ns->succession[sl_ix] = node;

	as_exchange_ns_vinfos_payload* ns_payload =
			namespace_data->partition_versions;
	uint8_t* read_ptr = (uint8_t*)ns_payload->vinfos;

	for (int i = 0; i < ns_payload->num_vinfos; i++) {
		as_exchange_vinfo_payload* vinfo_payload =
				(as_exchange_vinfo_payload*)read_ptr;

		for (int j = 0; j < vinfo_payload->num_pids; j++) {
			memcpy(&ns->cluster_versions[sl_ix][vinfo_payload->pids[j]],
					&vinfo_payload->vinfo, sizeof(vinfo_payload->vinfo));
		}

		read_ptr += sizeof(as_exchange_vinfo_payload)
				+ vinfo_payload->num_pids * sizeof(uint16_t);
	}

	ns->rack_ids[sl_ix] = namespace_data->rack_id;

	if (namespace_data->roster_generation > ns->roster_generation) {
		ns->roster_generation = namespace_data->roster_generation;
		ns->roster_count = namespace_data->roster_count;

		memcpy(ns->roster, namespace_data->roster,
				ns->roster_count * sizeof(cf_node));

		if (namespace_data->roster_rack_ids) {
			memcpy(ns->roster_rack_ids, namespace_data->roster_rack_ids,
					ns->roster_count * sizeof(uint32_t));
		}
		else {
			memset(ns->roster_rack_ids, 0, ns->roster_count * sizeof(uint32_t));
		}
	}

	if (ns->eventual_regime != 0 &&
			namespace_data->eventual_regime > ns->eventual_regime) {
		ns->eventual_regime = namespace_data->eventual_regime;
	}

	ns->rebalance_regimes[sl_ix] = namespace_data->rebalance_regime;

	// Prefer uniform balance only if all nodes prefer it.
	if ((namespace_data->rebalance_flags &
			AS_EXCHANGE_REBALANCE_FLAG_UNIFORM) == 0) {
		ns->prefer_uniform_balance = false;
	}

	bool is_node_quiesced = (namespace_data->rebalance_flags &
			AS_EXCHANGE_REBALANCE_FLAG_QUIESCE) != 0;

	if (node == g_config.self_node) {
		ns->is_quiesced = is_node_quiesced;
	}

	ns->quiesced[sl_ix] = is_node_quiesced;
}

/**
 * Commit exchange data for a given node.
 */
static void
exchange_data_pre_commit_for_node(cf_node node, uint32_t ix)
{
	EXCHANGE_LOCK();
	as_exchange_node_state node_state;
	exchange_node_state_get_safe(node, &node_state);

	g_exchange.compatibility_ids[ix] = node_state.data->compatibility_id;

	if (node_state.data->compatibility_id < g_exchange.min_compatibility_id) {
		g_exchange.min_compatibility_id = node_state.data->compatibility_id;
	}

	for (uint32_t i = 0; i < node_state.data->num_namespaces; i++) {
		exchange_namespace_payload_pre_commit_for_node(node,
				&node_state.data->namespace_data[i]);
	}

	EXCHANGE_UNLOCK();
}

/**
 * Check that there's not a mixture of AP and CP nodes in any namespace.
 */
static bool
exchange_data_pre_commit_ap_cp_check()
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		cf_node ap_node = (cf_node)0;
		cf_node cp_node = (cf_node)0;

		for (uint32_t n = 0; n < ns->cluster_size; n++) {
			if (ns->rebalance_regimes[n] == 0) {
				ap_node = ns->succession[n];
			}
			else {
				cp_node = ns->succession[n];
			}
		}

		if (ap_node != (cf_node)0 && cp_node != (cf_node)0) {
			WARNING("{%s} has mixture of AP and SC nodes - for example %lx is AP and %lx is SC",
					ns->name, ap_node, cp_node);
			return false;
		}
	}

	return true;
}

/**
 * Pre commit namespace data anticipating a successful commit from the
 * principal. This pre commit is to ensure regime advances in cp mode to cover
 * the case where the principal commits exchange data but the commit to a
 * non-principal is lost.
 */
static bool
exchange_exchanging_pre_commit()
{
	EXCHANGE_LOCK();
	pthread_mutex_lock(&g_exchanged_info_lock);

	memset(g_exchange.compatibility_ids, 0,
			sizeof(g_exchange.compatibility_ids));
	g_exchange.min_compatibility_id = UINT32_MAX;

	// Reset exchange data for all namespaces.
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];
		memset(ns->succession, 0, sizeof(ns->succession));

		// Assuming zero to represent "null" partition.
		memset(ns->cluster_versions, 0, sizeof(ns->cluster_versions));

		memset(ns->rack_ids, 0, sizeof(ns->rack_ids));

		ns->is_quiesced = false;
		memset(ns->quiesced, 0, sizeof(ns->quiesced));

		ns->roster_generation = 0;
		ns->roster_count = 0;
		memset(ns->roster, 0, sizeof(ns->roster));
		memset(ns->roster_rack_ids, 0, sizeof(ns->roster_rack_ids));

		// Note - not clearing ns->eventual_regime - prior non-0 value means CP.
		// Note - not clearing ns->rebalance_regime - it's not set here.
		memset(ns->rebalance_regimes, 0, sizeof(ns->rebalance_regimes));

		ns->cluster_size = 0;

		// Any node that does not prefer uniform balance will set this false.
		ns->prefer_uniform_balance = true;
	}

	// Fill the namespace partition version info in succession list order.
	int num_nodes = cf_vector_size(&g_exchange.succession_list);
	for (int i = 0; i < num_nodes; i++) {
		cf_node node;
		cf_vector_get(&g_exchange.succession_list, i, &node);
		exchange_data_pre_commit_for_node(node, i);
	}

	// Collected all exchanged data - do final configuration consistency checks.
	if (!exchange_data_pre_commit_ap_cp_check()) {
		WARNING("abandoned exchange - fix configuration conflict");
		pthread_mutex_unlock(&g_exchanged_info_lock);
		EXCHANGE_UNLOCK();
		return false;
	}

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		if (ns->eventual_regime != 0) {
			ns->eventual_regime += 2;

			as_storage_save_regime(ns);

			INFO("{%s} eventual-regime %u ready", ns->name,
					ns->eventual_regime);
		}
	}

	pthread_mutex_unlock(&g_exchanged_info_lock);
	EXCHANGE_UNLOCK();

	return true;
}

/**
 * Check to see if all exchange data is sent and received. If so switch to
 * ready_to_commit state.
 */
static void
exchange_exchanging_check_switch_ready_to_commit()
{
	EXCHANGE_LOCK();

	cf_vector* node_vector = cf_vector_stack_create(cf_node);

	if (g_exchange.state == AS_EXCHANGE_STATE_REST
			|| g_exchange.cluster_key == 0) {
		goto Exit;
	}

	exchange_nodes_find_send_unacked(node_vector);
	if (cf_vector_size(node_vector) > 0) {
		// We still have unacked exchange send messages.
		goto Exit;
	}

	vector_clear(node_vector);
	exchange_nodes_find_not_received(node_vector);
	if (cf_vector_size(node_vector) > 0) {
		// We still haven't received exchange messages from all nodes in the
		// succession list.
		goto Exit;
	}

	if (!exchange_exchanging_pre_commit()) {
		// Pre-commit failed. We are not ready to commit.
		goto Exit;
	}

	g_exchange.state = AS_EXCHANGE_STATE_READY_TO_COMMIT;

	DEBUG("ready to commit exchange data for cluster key %"PRIx64,
			g_exchange.cluster_key);

Exit:
	cf_vector_destroy(node_vector);

	if (g_exchange.state == AS_EXCHANGE_STATE_READY_TO_COMMIT) {
		exchange_ready_to_commit_msg_send();
	}

	EXCHANGE_UNLOCK();
}

/**
 * Handle incoming data message.
 *
 * Assumes the message has been checked for sanity.
 */
static void
exchange_exchanging_data_msg_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	DEBUG("received exchange data from node %"PRIx64, msg_event->msg_source);

	as_exchange_node_state node_state;
	exchange_node_state_get_safe(msg_event->msg_source, &node_state);

	if (!node_state.received) {
		node_state.data->compatibility_id = 0;
		msg_get_uint32(msg_event->msg, AS_EXCHANGE_MSG_COMPATIBILITY_ID,
				&node_state.data->compatibility_id);

		uint32_t num_namespaces_sent = exchange_data_msg_get_num_namespaces(
				msg_event);

		if (num_namespaces_sent == 0) {
			WARNING("ignoring invalid exchange data from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		cf_vector_define(namespace_list, sizeof(msg_buf_ele),
				num_namespaces_sent, 0);
		cf_vector_define(partition_versions, sizeof(msg_buf_ele),
				num_namespaces_sent, 0);
		uint32_t rack_ids[num_namespaces_sent];

		uint32_t roster_generations[num_namespaces_sent];
		cf_vector_define(rosters, sizeof(msg_buf_ele), num_namespaces_sent, 0);
		cf_vector_define(rosters_rack_ids, sizeof(msg_buf_ele),
				num_namespaces_sent, 0);

		memset(roster_generations, 0, sizeof(roster_generations));

		uint32_t eventual_regimes[num_namespaces_sent];
		uint32_t rebalance_regimes[num_namespaces_sent];
		uint32_t rebalance_flags[num_namespaces_sent];

		memset(eventual_regimes, 0, sizeof(eventual_regimes));
		memset(rebalance_regimes, 0, sizeof(rebalance_regimes));
		memset(rebalance_flags, 0, sizeof(rebalance_flags));

		if (!msg_msgpack_list_get_buf_array_presized(msg_event->msg,
				AS_EXCHANGE_MSG_NAMESPACES, &namespace_list)) {
			WARNING("received invalid namespaces from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		if (!msg_msgpack_list_get_buf_array_presized(msg_event->msg,
				AS_EXCHANGE_MSG_NS_PARTITION_VERSIONS, &partition_versions)) {
			WARNING("received invalid partition versions from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		uint32_t num_rack_ids = num_namespaces_sent;

		if (!msg_msgpack_list_get_uint32_array(msg_event->msg,
				AS_EXCHANGE_MSG_NS_RACK_IDS, rack_ids, &num_rack_ids)) {
			WARNING("received invalid cluster groups from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		uint32_t num_roster_generations = num_namespaces_sent;

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS)
				&& !msg_msgpack_list_get_uint32_array(msg_event->msg,
						AS_EXCHANGE_MSG_NS_ROSTER_GENERATIONS,
						roster_generations, &num_roster_generations)) {
			WARNING("received invalid roster generations from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTERS)
				&& !msg_msgpack_list_get_buf_array_presized(msg_event->msg,
						AS_EXCHANGE_MSG_NS_ROSTERS, &rosters)) {
			WARNING("received invalid rosters from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS)
				&& !msg_msgpack_list_get_buf_array_presized(msg_event->msg,
						AS_EXCHANGE_MSG_NS_ROSTERS_RACK_IDS,
						&rosters_rack_ids)) {
			WARNING("received invalid rosters-rack-ids from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		uint32_t num_eventual_regimes = num_namespaces_sent;

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES)
				&& !msg_msgpack_list_get_uint32_array(msg_event->msg,
						AS_EXCHANGE_MSG_NS_EVENTUAL_REGIMES, eventual_regimes,
						&num_eventual_regimes)) {
			WARNING("received invalid eventual regimes from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		uint32_t num_rebalance_regimes = num_namespaces_sent;

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES)
				&& !msg_msgpack_list_get_uint32_array(msg_event->msg,
						AS_EXCHANGE_MSG_NS_REBALANCE_REGIMES, rebalance_regimes,
						&num_rebalance_regimes)) {
			WARNING("received invalid rebalance regimes from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		uint32_t num_rebalance_flags = num_namespaces_sent;

		if (msg_is_set(msg_event->msg, AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS)
				&& !msg_msgpack_list_get_uint32_array(msg_event->msg,
						AS_EXCHANGE_MSG_NS_REBALANCE_FLAGS, rebalance_flags,
						&num_rebalance_flags)) {
			WARNING("received invalid rebalance flags from node %"PRIx64,
					msg_event->msg_source);
			goto Exit;
		}

		node_state.data->num_namespaces = 0;

		for (uint32_t i = 0; i < num_namespaces_sent; i++) {
			msg_buf_ele* namespace_name_element = cf_vector_getp(
					&namespace_list, i);

			// Find a match for the namespace.
			as_namespace* matching_namespace = as_namespace_get_bybuf(
					namespace_name_element->ptr, namespace_name_element->sz);

			if (!matching_namespace) {
				continue;
			}

			as_exchange_node_namespace_data* namespace_data =
					&node_state.data->namespace_data[node_state.data->num_namespaces];
			node_state.data->num_namespaces++;

			namespace_data->local_namespace = matching_namespace;
			namespace_data->rack_id = rack_ids[i];
			namespace_data->roster_generation = roster_generations[i];
			namespace_data->eventual_regime = eventual_regimes[i];
			namespace_data->rebalance_regime = rebalance_regimes[i];
			namespace_data->rebalance_flags = rebalance_flags[i];

			// Copy partition versions.
			msg_buf_ele* partition_versions_element = cf_vector_getp(
					&partition_versions, i);

			if (!exchange_namespace_payload_is_valid(
					(as_exchange_ns_vinfos_payload*)partition_versions_element->ptr,
					partition_versions_element->sz)) {
				WARNING(
						"received invalid partition versions for namespace %s from node %"PRIx64,
						matching_namespace->name, msg_event->msg_source);
				goto Exit;
			}

			namespace_data->partition_versions = cf_realloc(
					namespace_data->partition_versions,
					partition_versions_element->sz);

			memcpy(namespace_data->partition_versions,
					partition_versions_element->ptr,
					partition_versions_element->sz);

			// Copy rosters.
			// TODO - make this piece a utility function?
			if (namespace_data->roster_generation == 0) {
				namespace_data->roster_count = 0;
			}
			else {
				msg_buf_ele* roster_ele = cf_vector_getp(&rosters, i);

				namespace_data->roster_count = roster_ele->sz / sizeof(cf_node);

				if (namespace_data->roster_count == 0
						|| namespace_data->roster_count > AS_CLUSTER_SZ
						|| roster_ele->sz % sizeof(cf_node) != 0) {
					WARNING(
							"received invalid roster for namespace %s from node %"PRIx64,
							matching_namespace->name, msg_event->msg_source);
					goto Exit;
				}

				namespace_data->roster = cf_realloc(namespace_data->roster,
						roster_ele->sz);

				memcpy(namespace_data->roster, roster_ele->ptr, roster_ele->sz);

				uint32_t rri_ele_sz = 0;

				if (cf_vector_size(&rosters_rack_ids) != 0) {
					msg_buf_ele* rri_ele = cf_vector_getp(&rosters_rack_ids, i);

					if (rri_ele->sz != 0) {
						rri_ele_sz = rri_ele->sz;

						if (rri_ele_sz
								!= namespace_data->roster_count
										* sizeof(uint32_t)) {
							WARNING(
									"received invalid roster-rack-ids for namespace %s from node %"PRIx64,
									matching_namespace->name,
									msg_event->msg_source);
							goto Exit;
						}

						namespace_data->roster_rack_ids = cf_realloc(
								namespace_data->roster_rack_ids, rri_ele_sz);

						memcpy(namespace_data->roster_rack_ids, rri_ele->ptr,
								rri_ele_sz);
					}
				}

				if (rri_ele_sz == 0 && namespace_data->roster_rack_ids) {
					cf_free(namespace_data->roster_rack_ids);
					namespace_data->roster_rack_ids = NULL;
				}
			}
		}

		// Mark exchange data received from the source.
		node_state.received = true;
		exchange_node_state_update(msg_event->msg_source, &node_state);
	}
	else {
		// Duplicate pinfo received. Ignore.
		INFO("received duplicate exchange data from node %"PRIx64,
				msg_event->msg_source);
	}

	// Send an acknowledgement.
	exchange_data_ack_msg_send(msg_event->msg_source);

	// Check if we can switch to ready to commit state.
	exchange_exchanging_check_switch_ready_to_commit();

Exit:
	EXCHANGE_UNLOCK();
}

/**
 * Handle incoming data ack message.
 *
 * Assumes the message has been checked for sanity.
 */
static void
exchange_exchanging_data_ack_msg_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	DEBUG("received exchange data ack from node %"PRIx64,
			msg_event->msg_source);

	as_exchange_node_state node_state;
	exchange_node_state_get_safe(msg_event->msg_source, &node_state);

	if (!node_state.send_acked) {
		// Mark send as acked in the node state.
		node_state.send_acked = true;
		exchange_node_state_update(msg_event->msg_source, &node_state);
	}
	else {
		// Duplicate ack. Ignore.
		DEBUG("received duplicate data ack from node %"PRIx64,
				msg_event->msg_source);
	}

	// We might have send and received all partition info. Check for completion.
	exchange_exchanging_check_switch_ready_to_commit();

	EXCHANGE_UNLOCK();
}

/**
 * Process a message event when in exchanging state.
 */
static void
exchange_exchanging_msg_event_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	if (!exchange_msg_is_sane(msg_event->msg_source, msg_event->msg)) {
		goto Exit;
	}

	as_exchange_msg_type msg_type;
	exchange_msg_type_get(msg_event->msg, &msg_type);

	switch (msg_type) {
	case AS_EXCHANGE_MSG_TYPE_DATA:
		exchange_exchanging_data_msg_handle(msg_event);
		break;
	case AS_EXCHANGE_MSG_TYPE_DATA_ACK:
		exchange_exchanging_data_ack_msg_handle(msg_event);
		break;
	default:
		DEBUG(
				"exchanging state received unexpected mesage of type %d from node %"PRIx64,
				msg_type, msg_event->msg_source);
	}
Exit:
	EXCHANGE_UNLOCK();
}

/**
 * Process a message event when in exchanging state.
 */
static void
exchange_exchanging_timer_event_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();
	bool send_data = false;

	cf_clock now = cf_getms();

	// The timeout is a "linear" step function, where the timeout is constant
	// for the step interval.
	cf_clock min_timeout = EXCHANGE_SEND_MIN_TIMEOUT();
	cf_clock max_timeout = EXCHANGE_SEND_MAX_TIMEOUT();
	uint32_t step_interval = EXCHANGE_SEND_STEP_INTERVAL();
	cf_clock timeout = MAX(min_timeout,
			MIN(max_timeout,
					min_timeout
							* ((now - g_exchange.send_ts) / step_interval)));

	if (g_exchange.send_ts + timeout < now) {
		send_data = true;
	}

	EXCHANGE_UNLOCK();

	if (send_data) {
		exchange_data_msg_send_pending_ack();
	}
}

/**
 * Event processing in the exchanging state.
 */
static void
exchange_exchanging_event_handle(as_exchange_event* event)
{
	switch (event->type) {
	case AS_EXCHANGE_EVENT_CLUSTER_CHANGE:
		exchange_clustering_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_MSG:
		exchange_exchanging_msg_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_TIMER:
		exchange_exchanging_timer_event_handle(event);
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Ready_To_Commit state event handling
 * ----------------------------------------------------------------------------
 */

/**
 * Handle incoming ready to commit message.
 *
 * Assumes the message has been checked for sanity.
 */
static void
exchange_ready_to_commit_rtc_msg_handle(as_exchange_event* msg_event)
{
	if (!exchange_self_is_principal()) {
		WARNING(
				"non-principal self received ready to commit message from %"PRIx64" - ignoring",
				msg_event->msg_source);
		return;
	}

	EXCHANGE_LOCK();

	DEBUG("received ready to commit from node %"PRIx64, msg_event->msg_source);

	as_exchange_node_state node_state;
	exchange_node_state_get_safe(msg_event->msg_source, &node_state);

	if (!node_state.is_ready_to_commit) {
		// Mark as ready to commit in the node state.
		node_state.is_ready_to_commit = true;
		exchange_node_state_update(msg_event->msg_source, &node_state);
	}
	else {
		// Duplicate ready to commit received. Ignore.
		INFO("received duplicate ready to commit message from node %"PRIx64,
				msg_event->msg_source);
	}

	cf_vector* node_vector = cf_vector_stack_create(cf_node);
	exchange_nodes_find_not_ready_to_commit(node_vector);

	if (cf_vector_size(node_vector) <= 0) {
		// Send a commit message to all nodes in succession list.
		cf_node* node_list = NULL;
		int num_node_list = 0;
		cf_vector_to_stack_array(&g_exchange.succession_list, &node_list,
				&num_node_list);
		exchange_commit_msg_send_all(node_list, num_node_list);
	}

	cf_vector_destroy(node_vector);

	EXCHANGE_UNLOCK();
}

/**
 * Commit accumulated exchange data.
 */
static void
exchange_data_commit()
{
	EXCHANGE_LOCK();

	INFO("data exchange completed with cluster key %"PRIx64,
			g_exchange.cluster_key);

	// Exchange is done, use the current cluster details as the committed
	// cluster details.
	exchange_commited_cluster_update(g_exchange.cluster_key,
			&g_exchange.succession_list);

	// Force an update of the skew, to ensure new nodes if any have been checked
	// for skew.
	as_skew_monitor_update();

	// Must cover partition balance since it may manipulate ns->cluster_size.
	pthread_mutex_lock(&g_exchanged_info_lock);
	as_partition_balance();
	pthread_mutex_unlock(&g_exchanged_info_lock);

	EXCHANGE_UNLOCK();
}

/**
 * Handle incoming data ack message.
 *
 * Assumes the message has been checked for sanity.
 */
static void
exchange_ready_to_commit_commit_msg_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	if (msg_event->msg_source != g_exchange.principal) {
		WARNING(
				"ignoring commit message from node %"PRIx64" - expected message from %"PRIx64,
				msg_event->msg_source, g_exchange.principal);
		goto Exit;
	}

	INFO("received commit command from principal node %"PRIx64,
			msg_event->msg_source);

	// Commit exchanged data.
	exchange_data_commit();

	// Move to the rest state.
	g_exchange.state = AS_EXCHANGE_STATE_REST;

	// Queue up a cluster change event for downstream sub systems.
	as_exchange_cluster_changed_event cluster_change_event;

	EXCHANGE_COMMITTED_CLUSTER_RLOCK();
	cluster_change_event.cluster_key = g_exchange.committed_cluster_key;
	cluster_change_event.succession = vector_to_array(
			&g_exchange.committed_succession_list);
	cluster_change_event.cluster_size = g_exchange.committed_cluster_size;
	exchange_external_event_queue(&cluster_change_event);
	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();

Exit:
	EXCHANGE_UNLOCK();
}

/**
 * Handle incoming data message in ready to commit stage.
 *
 * Assumes the message has been checked for sanity.
 */
static void
exchange_ready_to_commit_data_msg_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	DEBUG("received exchange data from node %"PRIx64, msg_event->msg_source);

	// The source must have missed self node's data ack. Send an
	// acknowledgement.
	exchange_data_ack_msg_send(msg_event->msg_source);

	EXCHANGE_UNLOCK();
}

/**
 * Process a message event when in ready_to_commit state.
 */
static void
exchange_ready_to_commit_msg_event_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	if (!exchange_msg_is_sane(msg_event->msg_source, msg_event->msg)) {
		goto Exit;
	}

	as_exchange_msg_type msg_type;
	exchange_msg_type_get(msg_event->msg, &msg_type);

	switch (msg_type) {
	case AS_EXCHANGE_MSG_TYPE_READY_TO_COMMIT:
		exchange_ready_to_commit_rtc_msg_handle(msg_event);
		break;
	case AS_EXCHANGE_MSG_TYPE_COMMIT:
		exchange_ready_to_commit_commit_msg_handle(msg_event);
		break;
	case AS_EXCHANGE_MSG_TYPE_DATA:
		exchange_ready_to_commit_data_msg_handle(msg_event);
		break;
	default:
		DEBUG(
				"ready to commit state received unexpected message of type %d from node %"PRIx64,
				msg_type, msg_event->msg_source);
	}
Exit:
	EXCHANGE_UNLOCK();
}

/**
 * Process a message event when in ready_to_commit state.
 */
static void
exchange_ready_to_commit_timer_event_handle(as_exchange_event* msg_event)
{
	EXCHANGE_LOCK();

	if (g_exchange.ready_to_commit_send_ts + EXCHANGE_READY_TO_COMMIT_TIMEOUT()
			< cf_getms()) {
		// Its been a while since ready to commit has been sent to the
		// principal, retransmit it so that the principal gets it this time and
		// supplies a commit message.
		exchange_ready_to_commit_msg_send();
	}
	EXCHANGE_UNLOCK();
}

/**
 * Event processing in the ready_to_commit state.
 */
static void
exchange_ready_to_commit_event_handle(as_exchange_event* event)
{
	switch (event->type) {
	case AS_EXCHANGE_EVENT_CLUSTER_CHANGE:
		exchange_clustering_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_MSG:
		exchange_ready_to_commit_msg_event_handle(event);
		break;
	case AS_EXCHANGE_EVENT_TIMER:
		exchange_ready_to_commit_timer_event_handle(event);
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Exchange core subsystem
 * ----------------------------------------------------------------------------
 */

/**
 * Dispatch an exchange event inline to the relevant state handler.
 */
static void
exchange_event_handle(as_exchange_event* event)
{
	EXCHANGE_LOCK();

	switch (g_exchange.state) {
	case AS_EXCHANGE_STATE_REST:
		exchange_rest_event_handle(event);
		break;
	case AS_EXCHANGE_STATE_EXCHANGING:
		exchange_exchanging_event_handle(event);
		break;
	case AS_EXCHANGE_STATE_READY_TO_COMMIT:
		exchange_ready_to_commit_event_handle(event);
		break;
	case AS_EXCHANGE_STATE_ORPHANED:
		exchange_orphan_event_handle(event);
		break;
	}

	EXCHANGE_UNLOCK();
}

/**
 * Exchange timer event generator thread, to help with retries and retransmits
 * across all states.
 */
static void*
exchange_timer_thr(void* arg)
{
	as_exchange_event timer_event;
	memset(&timer_event, 0, sizeof(timer_event));
	timer_event.type = AS_EXCHANGE_EVENT_TIMER;

	while (EXCHANGE_IS_RUNNING()) {
		// Wait for a while and retry.
		usleep(EXCHANGE_TIMER_TICK_INTERVAL() * 1000);
		exchange_event_handle(&timer_event);
	}
	return NULL;
}

/**
 * Handle incoming messages from fabric.
 */
static int
exchange_fabric_msg_listener(cf_node source, msg* msg, void* udata)
{
	if (!EXCHANGE_IS_RUNNING()) {
		// Ignore this message.
		DEBUG("exchange stopped - ignoring message from %"PRIx64, source);
		goto Exit;
	}

	as_exchange_event msg_event;
	memset(&msg_event, 0, sizeof(msg_event));
	msg_event.type = AS_EXCHANGE_EVENT_MSG;
	msg_event.msg = msg;
	msg_event.msg_source = source;

	exchange_event_handle(&msg_event);
Exit:
	as_fabric_msg_put(msg);
	return 0;
}

/**
 * Listener for cluster change events from clustering layer.
 */
void
exchange_clustering_event_listener(as_clustering_event* event)
{
	if (!EXCHANGE_IS_RUNNING()) {
		// Ignore this message.
		DEBUG("exchange stopped - ignoring cluster change event");
		return;
	}

	as_exchange_event clustering_event;
	memset(&clustering_event, 0, sizeof(clustering_event));
	clustering_event.type = AS_EXCHANGE_EVENT_CLUSTER_CHANGE;
	clustering_event.clustering_event = event;

	// Dispatch the event.
	exchange_event_handle(&clustering_event);
}

/**
 * Initialize the template to be used for exchange messages.
 */
static void
exchange_msg_init()
{
	// Register fabric exchange msg type with no processing function.
	as_fabric_register_msg_fn(M_TYPE_EXCHANGE, exchange_msg_template,
			sizeof(exchange_msg_template), AS_EXCHANGE_MSG_SCRATCH_SIZE,
			exchange_fabric_msg_listener, NULL);
}

/**
 * Initialize exchange subsystem.
 */
static void
exchange_init()
{
	if (EXCHANGE_IS_INITIALIZED()) {
		return;
	}

	EXCHANGE_LOCK();

	memset(&g_exchange, 0, sizeof(g_exchange));

	// Start in the orphaned state.
	g_exchange.state = AS_EXCHANGE_STATE_ORPHANED;
	g_exchange.orphan_state_start_time = cf_getms();
	g_exchange.orphan_state_are_transactions_blocked = true;

	// Initialize the adjacencies.
	g_exchange.nodeid_to_node_state = cf_shash_create(cf_nodeid_shash_fn,
			sizeof(cf_node), sizeof(as_exchange_node_state),
			AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT, 0);

	cf_vector_init(&g_exchange.succession_list, sizeof(cf_node),
	AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);

	EXCHANGE_COMMITTED_CLUSTER_WLOCK();
	cf_vector_init(&g_exchange.committed_succession_list, sizeof(cf_node),
	AS_EXCHANGE_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);
	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();

	// Initialize exchange fabric messaging.
	exchange_msg_init();

	// Initialize self exchange data dynamic buffers.
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		cf_dyn_buf_init_heap(&g_exchange.self_data_dyn_buf[ns_ix],
			AS_EXCHANGE_SELF_DYN_BUF_SIZE());
	}

	// Initialize external event publishing.
	exchange_external_event_publisher_init();

	// Get partition versions from storage.
	as_partition_balance_init();

	DEBUG("exchange module initialized");

	EXCHANGE_UNLOCK();
}

/**
 * Stop exchange subsystem.
 */
static void
exchange_stop()
{
	if (!EXCHANGE_IS_RUNNING()) {
		WARNING("exchange is already stopped");
		return;
	}

	// Unguarded state, but this should be ok.
	g_exchange.sys_state = AS_EXCHANGE_SYS_STATE_SHUTTING_DOWN;

	cf_thread_join(g_exchange.timer_tid);

	EXCHANGE_LOCK();

	g_exchange.sys_state = AS_EXCHANGE_SYS_STATE_STOPPED;

	DEBUG("exchange module stopped");

	EXCHANGE_UNLOCK();

	external_event_publisher_stop();
}

/**
 * Start the exchange subsystem.
 */
static void
exchange_start()
{
	EXCHANGE_LOCK();

	if (EXCHANGE_IS_RUNNING()) {
		// Shutdown the exchange subsystem.
		exchange_stop();
	}

	g_exchange.sys_state = AS_EXCHANGE_SYS_STATE_RUNNING;

	g_exchange.timer_tid = cf_thread_create_joinable(exchange_timer_thr,
			(void*)&g_exchange);

	DEBUG("exchange module started");

	EXCHANGE_UNLOCK();

	exchange_external_event_publisher_start();
}

/*
 * ----------------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------------
 */
/**
 * Initialize exchange subsystem.
 */
void
as_exchange_init()
{
	exchange_init();
}

/**
 * Start exchange subsystem.
 */
void
as_exchange_start()
{
	exchange_start();
}

/**
 * Stop exchange subsystem.
 */
void
as_exchange_stop()
{
}

/**
 * Register to receive cluster-changed events.
 * TODO - may replace with simple static list someday.
 */
void
as_exchange_register_listener(as_exchange_cluster_changed_cb cb, void* udata)
{
	exchange_external_event_listener_register(cb, udata);
}

/**
 * Dump exchange state to log.
 */
void
as_exchange_dump(bool verbose)
{
	exchange_dump(CF_INFO, verbose);
}

/**
 * Member-access method.
 */
uint64_t
as_exchange_cluster_key()
{
	return (uint64_t)g_exchange.committed_cluster_key;
}

/**
 * Member-access method.
 */
uint32_t
as_exchange_cluster_size()
{
	return g_exchange.committed_cluster_size;
}

/**
 * Copy over the committed succession list.
 * Ensure the input vector has enough capacity.
 */
void
as_exchange_succession(cf_vector* succession)
{
	EXCHANGE_COMMITTED_CLUSTER_RLOCK();
	vector_copy(succession, &g_exchange.committed_succession_list);
	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();
}

/**
 * Return the committed succession list. For internal use within the scope
 * exchange_data_commit function call.
 */
cf_node*
as_exchange_succession_unsafe()
{
	return vector_to_array(&g_exchange.committed_succession_list);
}

/**
 * Return the committed succession list as a string in a dyn-buf.
 */
void
as_exchange_info_get_succession(cf_dyn_buf* db)
{
	EXCHANGE_COMMITTED_CLUSTER_RLOCK();

	cf_node* nodes = vector_to_array(&g_exchange.committed_succession_list);

	for (uint32_t i = 0; i < g_exchange.committed_cluster_size; i++) {
		cf_dyn_buf_append_uint64_x(db, nodes[i]);
		cf_dyn_buf_append_char(db, ',');
	}

	if (g_exchange.committed_cluster_size != 0) {
		cf_dyn_buf_chomp(db);
	}

	// Always succeeds.
	cf_dyn_buf_append_string(db, "\nok");

	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();
}

/**
 * Member-access method.
 */
cf_node
as_exchange_principal()
{
	return g_exchange.committed_principal;
}

/**
 * Used by exchange listeners during upgrades for compatibility purposes.
 */
uint32_t*
as_exchange_compatibility_ids(void)
{
	return (uint32_t*)g_exchange.compatibility_ids;
}

/**
 * Used by exchange listeners during upgrades for compatibility purposes.
 */
uint32_t
as_exchange_min_compatibility_id(void)
{
	return g_exchange.min_compatibility_id;
}

/**
 * Exchange cluster state output for info calls.
 */
void as_exchange_cluster_info(cf_dyn_buf* db) {
	EXCHANGE_COMMITTED_CLUSTER_RLOCK();
	info_append_uint32(db, "cluster_size", g_exchange.committed_cluster_size);
	info_append_uint64_x(db, "cluster_key", g_exchange.committed_cluster_key);
	info_append_uint64(db, "cluster_generation", g_exchange.committed_cluster_generation);
	info_append_uint64_x(db, "cluster_principal", g_exchange.committed_principal);
	EXCHANGE_COMMITTED_CLUSTER_UNLOCK();
}

/**
 * Lock before setting or getting exchanged info from non-exchange thread.
 */
void
as_exchange_info_lock()
{
	pthread_mutex_lock(&g_exchanged_info_lock);
}

/**
 * Unlock after setting or getting exchanged info from non-exchange thread.
 */
void
as_exchange_info_unlock()
{
	pthread_mutex_unlock(&g_exchanged_info_lock);
}
