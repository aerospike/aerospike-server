/*
 * hb.c
 *
 * Copyright (C) 2012-2017 Aerospike, Inc.
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

#include "fabric/hb.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/param.h>
#include <sys/types.h>
#include <zlib.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "dns.h"
#include "fault.h"
#include "node.h"
#include "shash.h"
#include "socket.h"

#include "base/cfg.h"
#include "base/health.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "fabric/endpoint.h"
#include "fabric/fabric.h"
#include "fabric/partition_balance.h"

/*
 * Overview
 * ========
 * The heartbeat subsystem is a core clustering module that discovers nodes in
 * the cluster and monitors connectivity to them. This subsystem maintains an
 * "adjacency list", which is the list of nodes deemed to be alive and connected
 * at any instance in time.
 *
 * The heartbeat subsystem is divided into three sub modules
 * 	1. Config
 * 	2. Channel
 * 	3. Mesh
 * 	4. Main
 *
 * Config
 * ------
 * This sub module deals with overall heartbeat subsystem configuration and
 * dynamic updates to configuration.
 *
 * Channel
 * -------
 * This sub module is responsible for maintaining a channel between this node
 * and all known nodes. The channel sub module provides the ability to broadcast
 * or uni cast messages to known nodes.
 *
 * Other modules interact with the channel sub module primarily through events
 * raised by the channel sub module. The events help other sub modules infer
 * connectivity status to known nodes and react to incoming heartbeat message
 * from other nodes.
 *
 * Depending on the configured mode (mesh. multicast) the channels between this
 * node and other nodes could be
 *  1. TCP and hence unicast. One per pair of nodes.
 *  2. Multicast with UDP. One per cluster.
 *
 * Mesh
 * ----
 * This sub module is responsible for discovering cluster members. New nodes are
 * discovered via adjacency lists published in their heartbeats of know nodes.
 * The mesh module boots up using configured seed nodes.
 *
 * Main
 * ----
 * This sub module orchestrates other modules and hence main. Its primary
 * responsibility is to maintain the adjacency list.
 *
 * Heartbeat messages
 * ==================
 *
 * Every heartbeat message contains
 * 	1. the source node's nodeid
 * 	2. the source node's published ip address
 * 	3. the source node's published port.
 *
 * There are the following types of heartbeat messages
 * 	1. Pulse - messages sent at periodic intervals. Will contain current
 * 	adjacency lists
 * 	2. Info request - message sent in the mesh mode, to a known mesh node,
 * 	in order to get ip address and port of a newly discovered node.
 * 	3. Info reply - message sent in response to an info request. Returns
 * 	the node's ip address and port.
 *
 * Message conventions
 * -------------------
 * 1. Published adjacency will always contain the source node.
 *
 * Design philosophy
 * =================
 *
 * Locking vs single threaded event loop.
 * --------------------------------------
 * This first cut leans toward using locks instead of single threaded event
 * loops to protect critical data. The choice is driven by the fact that
 * synchronous external and inter-sub module interaction looked like more work
 * with single threaded event loops. The design chooses simplicity over
 * performance given the lower volumes of events that need to be processed here
 * as compared to the transaction processing code. The locks are coarse, one per
 * sub module and re-entrant. They are used generously and no function makes an
 * assumption of locks prior locks being held.
 *
 * Inter-module interactions in some cases are via synchronous function calls,
 * which run the risk of deadlocks. For now, deadlocks should not happen.
 * However, if this ideology complicates code, inter-module interaction will be
 * rewritten to use asynchronous event queues.
 *
 * Locking policy
 * ==============
 *
 * 1. Lock as much as you can. The locks are re-entrant. This is not a critical
 * 	  high volume code path, and hence correctness with simplicity is preferred.
 * 	  Any read / write access to module state should be under a lock.
 * 2. Preventing deadlocks
 * 	  a. The enforced lock order is
 * 		 1. Protocol lock (SET_PROTOCOL_LOCK) Uses to ensure protocol set is
 *  atomic.
 * 		 2. Main module (HB_LOCK)
 * 		 3. Mesh and multicast modules (MESH_LOCK)
 * 		 4. Channel (CHANNEL_LOCK)
 * 		 5. Config (HB_CONFIG_LOCK)
 * 	   Always make sure every thread acquires locks in this order ONLY. In terms
 *  of functions calls only lower numbered modules can call functions from the
 *  higher numbered modules while holding their onto their locks.
 * 3. Events raised / messages passed to listeners should be outside the
 * 	  module's lock.
 *
 * Guidelines for message plugins
 * ==============================
 * The parse data functions should NOT hold any locks and thus avert deadlocks.
 *
 * TODO
 * ====
 * 1. Extend to allow hostnames in mesh mode across the board.
 */

/*
 * ----------------------------------------------------------------------------
 * Macros
 * ----------------------------------------------------------------------------
 */

/*
 * ----------------------------------------------------------------------------
 * Channel
 * ----------------------------------------------------------------------------
 */

/**
 * Size of the poll events set.
 */
#define POLL_SZ 1024

/**
 * The number of bytes for the message length on the wire.
 */
#define MSG_WIRE_LENGTH_SIZE 4

/**
 * Channel idle interval after which check for inactive channel is triggered.
 */
#define CHANNEL_IDLE_CHECK_PERIOD (CHANNEL_NODE_READ_IDLE_TIMEOUT() / 2)

/**
 * A channel times out if there is no msg received from a node in this interval.
 * Set to a fraction of node timeout so that a new channel could be set up to
 * recover from a potentially bad connection before the node times out.
 */
#define CHANNEL_NODE_READ_IDLE_TIMEOUT()					\
(PULSE_TRANSMIT_INTERVAL()									\
		* MAX(2, config_max_intervals_missed_get() / 3))

/**
 * Acquire a lock on the entire channel sub module.
 */
#define CHANNEL_LOCK() (pthread_mutex_lock(&g_channel_lock))

/**
 * Relinquish the lock on the entire channel sub module.
 */
#define CHANNEL_UNLOCK() (pthread_mutex_unlock(&g_channel_lock))

/*
 * ----------------------------------------------------------------------------
 * Mesh and Multicast
 * ----------------------------------------------------------------------------
 */

/**
 * Read write timeout (in ms).
 */
#define MESH_RW_TIMEOUT 5

/**
 * Size of the network header.
 *
 *  Maximum size of IPv4 header - 20 bytes (assuming no variable length fields)
 *  Fixed size of IPv6 header - 40 bytes (assuming no extension headers)
 *  Maximum size of TCP header - 60 Bytes
 *  Size of UDP header (fixed) - 8 bytes
 *  So maximum size of empty TCP datagram - 60 + 20 = 80 bytes
 *  So maximum size of empty IPv4 UDP datagram - 20 + 8 = 28 bytes
 *  So maximum size of empty IPv6 UDP datagram - 40 + 8 = 48 bytes
 *
 * Being conservative and assuming 30 bytes for IPv4 UDP header and 50 bytes for
 * IPv6 UDP header.
 */
#define UDP_HEADER_SIZE_MAX 50

/**
 * Expected ratio - (input size) / (compressed size). Assuming 40% decrease in
 * size after compression.
 */
#define MSG_COMPRESSION_RATIO (1.0 / 0.60)

/**
 * Mesh timeout for pending nodes.
 */
#define MESH_PENDING_TIMEOUT (CONNECT_TIMEOUT())

/**
 * Mesh inactive timeout after which a mesh node will be forgotten.
 */
#define MESH_INACTIVE_TIMEOUT (10 * HB_NODE_TIMEOUT())

/**
 * Mesh timeout for getting the endpoint for a node after which this node will
 * be forgotten.
 */
#define MESH_ENDPOINT_UNKNOWN_TIMEOUT (HB_NODE_TIMEOUT())

/**
 * Intervals at which mesh tender runs.
 */
#define MESH_TEND_INTERVAL (PULSE_TRANSMIT_INTERVAL())

/**
 * Intervals at which attempts to resolve unresolved seed hostname will be made.
 */
#define MESH_SEED_RESOLVE_ATTEMPT_INTERVAL() (HB_NODE_TIMEOUT())

/**
 * Intervals at which conflict checks is enabled.
 */
#define MESH_CONFLICT_CHECK_INTERVAL() (5 * HB_NODE_TIMEOUT())

/**
 * Duration for which conflicts are checked.
 */
#define MESH_CONFLICT_CHECK_DURATION() (MESH_CONFLICT_CHECK_INTERVAL() / 5)

/**
 * Acquire a lock on the entire mesh sub module.
 */
#define MESH_LOCK() (pthread_mutex_lock(&g_mesh_lock))

/**
 * Relinquish the lock on the entire mesh sub module.
 */
#define MESH_UNLOCK() (pthread_mutex_unlock(&g_mesh_lock))

/**
 * Acquire a lock on the entire multicast sub module.
 */
#define MULTICAST_LOCK() (pthread_mutex_lock(&g_multicast_lock))

/**
 * Relinquish the lock on the entire multicast sub module.
 */
#define MULTICAST_UNLOCK() (pthread_mutex_unlock(&g_multicast_lock))

/*
 * ----------------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------------
 */

/**
 * The identifier for heartbeat protocol version 3.
 */
#define HB_PROTOCOL_V3_IDENTIFIER 0x6864

/**
 * Maximum length of hb protocol string.
 */
#define HB_PROTOCOL_STR_MAX_LEN 16

/**
 * Default allocation size for plugin data.
 */
#define HB_PLUGIN_DATA_DEFAULT_SIZE 128

/**
 * Block size for allocating node plugin data. Ensure the allocation is in
 * multiples of 128 bytes, allowing expansion to 16 nodes without reallocating.
 */
#define HB_PLUGIN_DATA_BLOCK_SIZE 128

/**
 * Message scratch size for v3 HB messages. To accommodate 64 node cluster.
 */
#define AS_HB_MSG_SCRATCH_SIZE 1024

/**
 * A soft limit for the maximum cluster size. Meant to be optimize hash and list
 * data structures and not as a limit on the number of nodes.
 */
#define AS_HB_CLUSTER_MAX_SIZE_SOFT 200

/**
 * Maximum event listeners.
 */
#define AS_HB_EVENT_LISTENER_MAX 7

/**
 * Maximum permissible cluster-name mismatch per node.
 */
#define CLUSTER_NAME_MISMATCH_MAX 2

/**
 * Timeout for deeming a node dead based on received heartbeats.
 */
#define HB_NODE_TIMEOUT()											\
((config_max_intervals_missed_get() * config_tx_interval_get()))

/**
 * Intervals at which heartbeats are send.
 */
#define PULSE_TRANSMIT_INTERVAL()							\
(MAX(config_tx_interval_get(), AS_HB_TX_INTERVAL_MS_MIN))

/**
 * Intervals at which adjacency tender runs.
 */
#define ADJACENCY_TEND_INTERVAL (PULSE_TRANSMIT_INTERVAL())

/**
 * Intervals at which adjacency tender runs in anticipation of addtional node
 * depart events.
 */
#define ADJACENCY_FAST_TEND_INTERVAL (MIN(ADJACENCY_TEND_INTERVAL, 10))

/**
 * Acquire a lock on the external event publisher.
 */
#define EXTERNAL_EVENT_PUBLISH_LOCK()					\
(pthread_mutex_lock(&g_external_event_publish_lock))

/**
 * Relinquish the lock on the external event publisher.
 */
#define EXTERNAL_EVENT_PUBLISH_UNLOCK()					\
(pthread_mutex_unlock(&g_external_event_publish_lock))

/**
 * Acquire a lock on the heartbeat main module.
 */
#define HB_LOCK() (pthread_mutex_lock(&g_hb_lock))

/**
 * Relinquish the lock on the  heartbeat main module.
 */
#define HB_UNLOCK() (pthread_mutex_unlock(&g_hb_lock))

/**
 * Weightage of current latency over current moving average. For now weigh
 * recent values heavily over older values.
 */
#define ALPHA (0.65)

/*
 * ----------------------------------------------------------------------------
 * Common
 * ----------------------------------------------------------------------------
 */

/**
 * The default MTU for multicast in case device discovery fails.
 */
#define DEFAULT_MIN_MTU 1500

/**
 * Maximum memory size allocated on the call stack.
 */
#define STACK_ALLOC_LIMIT (16 * 1024)

/**
 * Max string length for an endpoint list converted to a string.
 */
#define ENDPOINT_LIST_STR_SIZE 1024

/**
 * A hard limit on the buffer size for parsing incoming messages.
 */
#define MSG_BUFFER_MAX_SIZE (10 * 1024 * 1024)

#ifndef ASC
#define ASC (2 << 2)
#endif

/**
 * Connection initiation timeout, Capped at 100 ms.
 */
#define CONNECT_TIMEOUT() (MIN(100, config_tx_interval_get()))

/**
 * Allocate a buffer for heart beat messages. Larger buffers are heap allocated
 * to prevent stack overflows.
 */
#define MSG_BUFF_ALLOC(size) (										\
		(size) <= MSG_BUFFER_MAX_SIZE ?								\
				(((size) > STACK_ALLOC_LIMIT) ?						\
						cf_malloc(size) : alloca(size)) : NULL)

/**
 * Allocate a buffer for heart beat messages. Larger buffers are heap allocated
 * to prevent stack overflows. Crashes the process on failure to allocate the
 * buffer.
 */
#define MSG_BUFF_ALLOC_OR_DIE(size, crash_msg, ...)		\
({														\
	uint8_t* retval = MSG_BUFF_ALLOC((size));			\
	if (!retval) {										\
		CRASH(crash_msg, ##__VA_ARGS__);				\
	}													\
	retval;												\
})

/**
 * Free the buffer allocated by MSG_BUFF_ALLOC
 */
#define MSG_BUFF_FREE(buffer, size)								\
if (((size) > STACK_ALLOC_LIMIT) && buffer) {cf_free(buffer);}

/**
 * Acquire a lock on the entire config sub module.
 */
#define HB_CONFIG_LOCK() (pthread_mutex_lock(&g_hb_config_lock))

/**
 * Relinquish the lock on the entire config sub module.
 */
#define HB_CONFIG_UNLOCK() (pthread_mutex_unlock(&g_hb_config_lock))

/**
 * Acquire a lock while setting heartbeat protocol dynamically.
 */
#define SET_PROTOCOL_LOCK() (pthread_mutex_lock(&g_set_protocol_lock))

/**
 * Relinquish the lock after setting heartbeat protocol dynamically.
 */
#define SET_PROTOCOL_UNLOCK() (pthread_mutex_unlock(&g_set_protocol_lock))

/**
 * Logging macros.
 */
#define CRASH(format, ...) cf_crash(AS_HB, format, ##__VA_ARGS__)
#define CRASH_NOSTACK(format, ...) cf_crash_nostack(AS_HB, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_HB, format, ##__VA_ARGS__)
#define TICKER_WARNING(format, ...)					\
cf_ticker_warning(AS_HB, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_HB, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_HB, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_HB, format, ##__VA_ARGS__)
#define ASSERT(expression, message, ...)				\
if (!(expression)) {WARNING(message, ##__VA_ARGS__);}

/*
 * ----------------------------------------------------------------------------
 * Private internal data structures
 * ----------------------------------------------------------------------------
 */

/*
 * ----------------------------------------------------------------------------
 * Common
 * ----------------------------------------------------------------------------
 */

/**
 * Heartbeat subsystem state.
 */
typedef enum
{
	AS_HB_STATUS_UNINITIALIZED,
	AS_HB_STATUS_RUNNING,
	AS_HB_STATUS_SHUTTING_DOWN,
	AS_HB_STATUS_STOPPED
} as_hb_status;

/*
 * ----------------------------------------------------------------------------
 * Mesh related
 * ----------------------------------------------------------------------------
 */

/**
 * Mesh node status enum.
 */
typedef enum
{
	/**
	 * The mesh node has an active channel.
	 */
	AS_HB_MESH_NODE_CHANNEL_ACTIVE,

	/**
	 * The mesh node is waiting for an active channel.
	 */
	AS_HB_MESH_NODE_CHANNEL_PENDING,

	/**
	 * The mesh node does not have an active channel.
	 */
	AS_HB_MESH_NODE_CHANNEL_INACTIVE,

	/**
	 * The ip address and port for this node are not yet known.
	 */
	AS_HB_MESH_NODE_ENDPOINT_UNKNOWN,

	/**
	 * The sentinel value. Should be the last in the enum.
	 */
	AS_HB_MESH_NODE_STATUS_SENTINEL
} as_hb_mesh_node_status;

/**
 * The info payload for a single node.
 */
typedef struct as_hb_mesh_info_reply_s
{
	/**
	 * The nodeid of the node for which info reply is sent.
	 */
	cf_node nodeid;

	/**
	 * The advertised endpoint list for this node. List to allow variable size
	 * endpoint list. Always access as reply.endpoints[0].
	 */
	as_endpoint_list endpoint_list[];
}__attribute__((__packed__)) as_hb_mesh_info_reply;

/**
 * Mesh tend reduce function udata.
 */
typedef struct as_hb_mesh_tend_reduce_udata_s
{
	/**
	 * The new endpoint lists to connect to. Each list has endpoints for s
	 * single remote peer.
	 */
	as_endpoint_list** to_connect;

	/**
	 * The capacity of the to connect array.
	 */
	size_t to_connect_capacity;

	/**
	 * The count of endpoints to connect.
	 */
	size_t to_connect_count;

	/**
	 * Pointers to seeds that need matching.
	 */
	cf_vector* inactive_seeds_p;
} as_hb_mesh_tend_reduce_udata;

/**
 * Mesh endpoint search udata.
 */
typedef struct
{
	/**
	 * The endpoint to search.
	 */
	cf_sock_addr* to_search;

	/**
	 * Indicates is a match is found.
	 */
	bool found;
} as_hb_endpoint_list_addr_find_udata;

/**
 * Mesh endpoint list search udata.
 */
typedef struct as_hb_mesh_endpoint_list_reduce_udata_s
{
	/**
	 * The endpoint to search.
	 */
	as_endpoint_list* to_search;

	/**
	 * Indicates is a match is found.
	 */
	bool found;

	/**
	 * The matched key if found.
	 */
	cf_node* matched_nodeid;
} as_hb_mesh_endpoint_list_reduce_udata;

/**
 * Information maintained for configured mesh seed nodes.
 */
typedef struct as_hb_mesh_seed_s
{
	/**
	 * The name / ip address of this seed mesh host.
	 */
	char seed_host_name[DNS_NAME_MAX_SIZE];

	/**
	 * The port of this seed mesh host.
	 */
	cf_ip_port seed_port;

	/**
	 * Identifies TLS mesh seed hosts.
	 */
	bool seed_tls;

	/**
	 * The heap allocated end point list for this seed host resolved usiung the
	 * seeds hostname.
	 * Will be null if the endpoint list cannot be resolved.
	 */
	as_endpoint_list* resolved_endpoint_list;

	/**
	 * Timestamp when the seed hostname was resolved into the endpoint list.
	 * Used to perform periodic refresh of the endpoint list.
	 */
	cf_clock resolved_endpoint_list_ts;

	/**
	 * The state of this seed in terms of established channel.
	 */
	as_hb_mesh_node_status status;

	/**
	 * The last time the state of this node was updated.
	 */
	cf_clock last_status_updated;

	/**
	 * The node id for a matching mesh node entry. A zero will indicate that
	 * there exists no matching mesh node entry.
	 */
	cf_node mesh_nodeid;

	/**
	 * Timestamp indicating when the matching mesh node's endpoint was updated.
	 * Used to detect endpoint changes to the matching mesh node entry if it
	 * exists.
	 */
	as_hlc_timestamp mesh_node_endpoint_change_ts;
} as_hb_mesh_seed;

/**
 * Information maintained for discovered mesh end points.
 */
typedef struct as_hb_mesh_node_s
{
	/**
	 * The heap allocated end point list for this mesh host. Should be freed
	 * once the last mesh entry is removed from the mesh state.
	 */
	as_endpoint_list* endpoint_list;

	/**
	 * Timestamp when the mesh node was last updated.
	 */
	as_hlc_timestamp endpoint_change_ts;

	/**
	 * The state of this node in terms of established channel.
	 */
	as_hb_mesh_node_status status;

	/**
	 * The last time the state of this node was updated.
	 */
	cf_clock last_status_updated;

	/**
	 * The time this node's channel become inactive.
	 */
	cf_clock inactive_since;
} as_hb_mesh_node;

/**
 * State maintained for the mesh mode.
 */
typedef struct as_hb_mesh_state_s
{
	/**
	 * The sockets on which this instance accepts heartbeat tcp connections.
	 */
	cf_sockets listening_sockets;

	/**
	 * Indicates if the published endpoint list is ipv4 only.
	 */
	bool published_endpoint_list_ipv4_only;

	/**
	 * The published endpoint list.
	 */
	as_endpoint_list* published_endpoint_list;

	/**
	 * Mesh seed data.
	 */
	cf_vector seeds;

	/**
	 * A map from an cf_node _key to a mesh node.
	 */
	cf_shash* nodeid_to_mesh_node;

	/**
	 * Thread id for the mesh tender thread.
	 */
	pthread_t mesh_tender_tid;

	/**
	 * The status of the mesh module.
	 */
	as_hb_status status;

	/**
	 * The mtu on the listening device. This is extrapolated to all nodes and
	 * paths in the cluster. This limits the cluster size possible.
	 */
	int min_mtu;

	/**
	 * Indicates if new nodes are discovered. Optimization to start mesh tend
	 * earlier than normal tend interval on discovering new nodes.
	 */
	bool nodes_discovered;
} as_hb_mesh_state;

/*
 * ----------------------------------------------------------------------------
 * Multicast data structures
 * ----------------------------------------------------------------------------
 */

/**
 * State maintained for the multicast mode.
 */
typedef struct as_hb_multicast_state_s
{
	/**
	 * The sockets associated with multicast mode.
	 */
	cf_mserv_cfg cfg;

	/**
	 * Multicast listening sockets.
	 */
	cf_sockets listening_sockets;

	/**
	 * The mtu on the listening device. This is extrapolated to all nodes and
	 * paths in the cluster. This limits the cluster size possible.
	 */
	int min_mtu;
} as_hb_multicast_state;

/*
 * ----------------------------------------------------------------------------
 * Channel state
 * ----------------------------------------------------------------------------
 */

/**
 * The type of a channel event.
 */
typedef enum
{
	/**
	 * The endpoint has a channel tx/rx channel associated with it.
	 */
	AS_HB_CHANNEL_NODE_CONNECTED,

	/**
	 * The endpoint had a tx/rx channel that went down.
	 */
	AS_HB_CHANNEL_NODE_DISCONNECTED,

	/**
	 * A message was received on a connected channel. The message in the event,
	 * is guaranteed to have passed basic sanity check like have protocol id,
	 * type and source nodeid.
	 */
	AS_HB_CHANNEL_MSG_RECEIVED,

	/**
	 * Channel found node whose cluster name does not match.
	 */
	AS_HB_CHANNEL_CLUSTER_NAME_MISMATCH
} as_hb_channel_event_type;

/**
 * Status for reads from a channel.
 */
typedef enum
{
	/**
	 * The message was read successfully and parser.
	 */
	AS_HB_CHANNEL_MSG_READ_SUCCESS,

	/**
	 * The message read successfully but parsing failed.
	 */
	AS_HB_CHANNEL_MSG_PARSE_FAIL,

	/**
	 * The message read failed network io.
	 */
	AS_HB_CHANNEL_MSG_CHANNEL_FAIL,

	/**
	 * Sentinel default value.
	 */
	AS_HB_CHANNEL_MSG_READ_UNDEF
} as_hb_channel_msg_read_status;

typedef struct
{
	/**
	 * The endpoint address to search channel by.
	 */
	as_endpoint_list* endpoint_list;

	/**
	 * Indicates if the endpoint was found.
	 */
	bool found;

	/**
	 * The matching socket, if found.
	 */
	cf_socket* socket;
} as_hb_channel_endpoint_reduce_udata;

typedef struct
{
	/**
	 * The endpoint address to search channel by.
	 */
	cf_sock_addr* addr_to_search;

	/**
	 * Indicates if the endpoint was found.
	 */
	bool found;
} as_hb_channel_endpoint_iterate_udata;

typedef struct
{
	/**
	 * The message buffer to send.
	 */
	uint8_t* buffer;

	/**
	 * The buffer length.
	 */
	size_t buffer_len;
} as_hb_channel_buffer_udata;

/**
 * A channel represents a medium to send and receive messages.
 */
typedef struct as_hb_channel_s
{
	/**
	 * Indicates if this channel is a multicast channel.
	 */
	bool is_multicast;

	/**
	 * Indicates if this channel is inbound. Not relevant for multicast
	 * channels.
	 */
	bool is_inbound;

	/**
	 * The id of the associated node. In mesh / unicast case this will initially
	 * be zero and filled in when the nodeid for the node at the other end is
	 * learnt. In multicast case this will be zero.
	 */
	cf_node nodeid;

	/**
	 * The address of the peer. Will always be specified for outbound channels.
	 */
	cf_sock_addr endpoint_addr;

	/**
	 * The last time a message was received from this node.
	 */
	cf_clock last_received;

	/**
	 * Time when this channel won a socket resolution. Zero if this channel
	 * never won resolution. In compatibility mode with older code its possible
	 * we will keep allowing the same socket to win and enter an infinite loop
	 * of closing the sockets.
	 */
	cf_clock resolution_win_ts;
} as_hb_channel;

/**
 * State maintained per heartbeat channel.
 */
typedef struct as_hb_channel_state_s
{
	/**
	 * The poll handle. All IO wait across all heartbeat connections happens on
	 * this handle.
	 */
	cf_poll poll;

	/**
	 * Channel status.
	 */
	as_hb_status status;

	/**
	 * Maps a socket to an as_hb_channel.
	 */
	cf_shash* socket_to_channel;

	/**
	 * Maps a nodeid to a channel specific node data structure. This association
	 * will be made only on receiving the first heartbeat message from the node
	 * on a channel.
	 */
	cf_shash* nodeid_to_socket;

	/**
	 * Sockets accumulated by the channel tender to close at the end of every
	 * epoll loop.
	 */
	cf_queue socket_close_queue;

	/**
	 * The sockets on which heartbeat subsystem listens.
	 */
	cf_sockets* listening_sockets;

	/**
	 * Clock to keep track of last time idle connections were checked.
	 */
	cf_clock last_channel_idle_check;

	/**
	 * Enables / disables publishing channel events. Events should be disabled
	 * only when the state changes are temporary / transient and hence would not
	 * change the overall channel state from an external perspective.
	 */
	bool events_enabled;

	/**
	 * Events are batched and published to reduce cluster transitions. Queue of
	 * unpublished heartbeat events.
	 */
	cf_queue events_queue;

	/**
	 * Thread id for the socket tender thread.
	 */
	pthread_t channel_tender_tid;
} as_hb_channel_state;

/**
 * Entry queued up for socket close.
 */
typedef struct as_hb_channel_socket_close_entry_s
{
	/**
	 * The node for which this event was generated.
	 */
	cf_socket* socket;
	/**
	 * Indicates if this close is a remote close.
	 */
	bool is_remote;
	/**
	 * True if close of this entry should generate a disconnect event.
	 */
	bool raise_close_event;
} as_hb_channel_socket_close_entry;

/**
 * An event generated by the channel sub module.
 */
typedef struct as_hb_channel_event_s
{
	/**
	 * The channel event type.
	 */
	as_hb_channel_event_type type;

	/**
	 * The node for which this event was generated.
	 */
	cf_node nodeid;

	/**
	 * The received message if any over this endpoint. Valid for incoming
	 * message type event. The message if not NULL never be edited or copied
	 * over.
	 */
	msg* msg;

	/**
	 * The hlc timestamp for message receipt.
	 */
	as_hlc_msg_timestamp msg_hlc_ts;
} as_hb_channel_event;

/*
 * ----------------------------------------------------------------------------
 * Main sub module state
 * ----------------------------------------------------------------------------
 */

/**
 * Heartbeat message types.
 */
typedef enum
{
	AS_HB_MSG_TYPE_PULSE,
	AS_HB_MSG_TYPE_INFO_REQUEST,
	AS_HB_MSG_TYPE_INFO_REPLY,
	AS_HB_MSG_TYPE_COMPRESSED
} as_hb_msg_type;

/**
 * Events published by the heartbeat subsystem.
 */
typedef enum
{
	AS_HB_INTERNAL_NODE_ARRIVE,
	AS_HB_INTERNAL_NODE_DEPART,
	AS_HB_INTERNAL_NODE_EVICT,
	AS_HB_INTERNAL_NODE_ADJACENCY_CHANGED
} as_hb_internal_event_type;

/**
 * State maintained by the heartbeat subsystem for the selected mode.
 */
typedef struct as_hb_mode_state_s
{
	/**
	 * The mesh / multicast state.
	 */
	union
	{
		as_hb_mesh_state mesh_state;
		as_hb_multicast_state multicast_state;
	};
} as_hb_mode_state;

/**
 * Plugin data iterate reduce udata.
 */
typedef struct
{
	/**
	 * The plugin id.
	 */
	as_hb_plugin_id pluginid;

	/**
	 * The iterate function.
	 */
	as_hb_plugin_data_iterate_fn iterate_fn;

	/**
	 * The udata for the iterate function.
	 */
	void* udata;
} as_hb_adjacecny_iterate_reduce_udata;

/**
 * Information tracked for an adjacent nodes.
 */
typedef struct as_hb_adjacent_node_s
{
	/**
	 * The heart beat protocol version.
	 */
	uint32_t protocol_version;

	/**
	 * The remote node's
	 */
	as_endpoint_list* endpoint_list;

	/**
	 * Used to cycle between the two copies of plugin data.
	 */
	int plugin_data_cycler;

	/**
	 * Plugin specific data accumulated by the heartbeat subsystem. The data is
	 * heap allocated and should be destroyed the moment this element entry is
	 * unused. There are two copies of the plugin data, one the current copy and
	 * one the previous copy. Previous copy is used to generate data change
	 * notifications.
	 */
	as_hb_plugin_node_data plugin_data[AS_HB_PLUGIN_SENTINEL][2];

	/**
	 * The monotonic local time node information was last updated.
	 */
	cf_clock last_updated_monotonic_ts;

	/**
	 * HLC timestamp for the last pulse message.
	 */
	as_hlc_msg_timestamp last_msg_hlc_ts;

	/**
	 * Track number of consecutive cluster-name mismatches.
	 */
	uint32_t cluster_name_mismatch_count;

	/**
	 * Moving average of the latency in ms.
	 */
	uint64_t avg_latency;

	/**
	 * A shift register tracking change of endpoints. On receipt of a heartbeat,
	 * if source node's endpoints change 1 is inserted at the LSB, else 0 is
	 * inserted at the LSB.
	 */
	uint64_t endpoint_change_tracker;
} as_hb_adjacent_node;

/**
 * Internal storage for external event listeners.
 */
typedef struct as_hb_event_listener_s
{
	/**
	 * Registered callback function.
	 */
	as_hb_event_fn event_callback;

	/**
	 * Arguments for the listeners.
	 */
	void* udata;
} as_hb_event_listener;

/**
 * Heartbeat subsystem internal state.
 */
typedef struct as_hb_s
{
	/**
	 * The status of the subsystem.
	 */
	as_hb_status status;

	/**
	 * The adjacency dictionary. The key is the nodeid. The value is an instance
	 * of as_hb_adjacent_node.
	 */
	cf_shash* adjacency;

	/**
	 * The probation dictionary having nodes that display unexpected behavior.
	 * Nodeids under probation and adjacency hash are always exclusive. The key
	 * is the nodeid. The value is an instance of as_hb_adjacent_node.
	 */
	cf_shash* on_probation;

	/**
	 * Temporary nodeid to index hash used to compute nodes to evict from a
	 * clique.
	 */
	cf_shash* nodeid_to_index;

	/**
	 * The mode specific state.
	 */
	as_hb_mode_state mode_state;

	/**
	 * The channel state.
	 */
	as_hb_channel_state channel_state;

	/**
	 * Self node accumulated stats used primarily to detect duplicate node-ids.
	 */
	as_hb_adjacent_node self_node;

	/**
	 * Indicates self node-id has duplicates.
	 */
	bool self_is_duplicate;

	/**
	 * Monotonic timestamp of when a self duplicate was detected.
	 */
	cf_clock self_duplicate_detected_ts;

	/**
	 * The plugin dictionary. The key is the as_hb_plugin entry and the value an
	 * instance of as_hb_plugin.
	 */
	as_hb_plugin plugins[AS_HB_PLUGIN_SENTINEL];

	/**
	 * Thread id for the transmitter thread.
	 */
	pthread_t transmitter_tid;

	/**
	 * Thread id for the thread expiring nodes from the adjacency list.
	 */
	pthread_t adjacency_tender_tid;
} as_hb;

/**
 * Registered heartbeat listeners.
 */
typedef struct as_hb_external_events_s
{
	/**
	 * Events are batched and published. Queue of unpublished heartbeat events.
	 */
	cf_queue external_events_queue;

	/**
	 * Count of event listeners.
	 */
	int event_listener_count;

	/**
	 * External event listeners.
	 */
	as_hb_event_listener event_listeners[AS_HB_EVENT_LISTENER_MAX];
} as_hb_external_events;

/**
 * Shash reduce function to read current adjacency list.
 */
typedef struct as_hb_adjacency_reduce_udata_s
{
	/**
	 * The target adjacency list.
	 */
	cf_node* adj_list;

	/**
	 * Count of elements in the adjacency list.
	 */
	int adj_count;
} as_hb_adjacency_reduce_udata;

/**
 * Udata for finding nodes in the adjacency list not in the input succession
 * list.
 */
typedef struct
{
	/**
	 * Number of events generated.
	 */
	int event_count;

	/**
	 * List of generated events.
	 */
	as_hb_event_node* events;

	/**
	 * Limit on number of generated events.
	 */
	int max_events;

	/**
	 * Current succession list.
	 */
	cf_node* succession;

	/**
	 * Number of nodes in succession list.
	 */
	int succession_size;
} as_hb_find_new_nodes_reduce_udata;

/**
 * Shash reduce function to read current adjacency list.
 */
typedef struct as_hb_adjacency_tender_udata_s
{
	/**
	 * The list of expired nodes.
	 */
	cf_node* dead_nodes;

	/**
	 * Count of elements in the dead node list.
	 */
	int dead_node_count;

	/**
	 * The list of evicted nodes , e.g. due to cluster name mismatch.
	 */
	cf_node* evicted_nodes;

	/**
	 * Count of elements in the evicted node list.
	 */
	int evicted_node_count;
} as_hb_adjacency_tender_udata;

/**
 * Udata for tip clear.
 */
typedef struct as_hb_mesh_tip_clear_udata_s
{
	/**
	 * Host IP or DNS name to be cleared from seed list.
	 */
	char host[DNS_NAME_MAX_SIZE];

	/**
	 * Listening port of the host.
	 */
	int port;

	/**
	 * Number of IP addresses to match.
	 */
	uint32_t n_addrs;

	/**
	 * IP addresses to match.
 	*/
	cf_ip_addr* addrs;

	/**
	 * Node id if a specific node-id needs to be removed as well.
	 */
	cf_node nodeid;

	/**
	 * Tip-clear status
	 */
	bool entry_deleted;
} as_hb_mesh_tip_clear_udata;

/**
 * Convert endpoint list to string in a process function.
 */
typedef struct endpoint_list_to_string_udata_s
{
	/**
	 * The endpoint list in string format.
	 */
	char* endpoint_list_str;

	/**
	 * The size of enpoint list.
	 */
	size_t endpoint_list_str_capacity;
} endpoint_list_to_string_udata;

/**
 * Udata to fill an endpoint list into a message.
 */
typedef struct endpoint_list_to_msg_udata_s
{
	/**
	 * The target message.
	 */
	msg* msg;

	/**
	 * Indicates if we are running in mesh mode.
	 */
	bool is_mesh;
} endpoint_list_to_msg_udata;

/**
 * Udata to test if this endpoint list overlaps with other endpoint list.
 */
typedef struct endpoint_list_equal_check_udata_s
{
	/**
	 * The endpoint list of the new node.
	 */
	as_endpoint_list* other;

	/**
	 * Output. Indicates if the lists are equal.
	 */
	bool are_equal;
} endpoint_list_equal_check_udata;

/**
 * Endpoint list process function.
 * @param endpoint current endpoint in the iteration.
 * @param udata udata passed through from the invoker of the iterate function.
 */
typedef void
(*endpoint_list_process_fn)(const as_endpoint_list* endpoint_list, void* udata);

/**
 * Seed host list reduce udata.
 */
typedef struct as_hb_seed_host_list_udata_s
{
	/**
	 * The buffer to receive the list.
	 */
	cf_dyn_buf* db;

	/**
	 * Selects TLS seed nodes.
	 */
	bool tls;
} as_hb_seed_host_list_udata;

/*
 * ----------------------------------------------------------------------------
 * Globals
 * ----------------------------------------------------------------------------
 */

/**
 * Global heartbeat instance.
 */
static as_hb g_hb;

/**
 * Global heartbeat events listener instance.
 */
static as_hb_external_events g_hb_event_listeners;

/**
 * The big fat lock for all external event publishing. This ensures that a batch
 * of external events are published atomically to preserve the order of external
 * events.
 */
static pthread_mutex_t g_external_event_publish_lock =
		PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * Global lock to serialize all read and writes to the heartbeat subsystem.
 */
static pthread_mutex_t g_hb_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The big fat lock for all channel state.
 */
static pthread_mutex_t g_channel_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The big fat lock for all mesh state.
 */
static pthread_mutex_t g_mesh_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The big fat lock for all multicast state.
 */
static pthread_mutex_t g_multicast_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The global lock for all heartbeat configuration.
 */
static pthread_mutex_t g_hb_config_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The lock used while setting heartbeat protocol.
 */
static pthread_mutex_t g_set_protocol_lock =
		PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * Message templates for heartbeat messages.
 */
static msg_template g_hb_msg_template[] = {

{ AS_HB_MSG_ID, M_FT_UINT32 },

{ AS_HB_MSG_TYPE, M_FT_UINT32 },

{ AS_HB_MSG_NODE, M_FT_UINT64 },

{ AS_HB_MSG_CLUSTER_NAME, M_FT_STR },

{ AS_HB_MSG_HLC_TIMESTAMP, M_FT_UINT64 },

{ AS_HB_MSG_ENDPOINTS, M_FT_BUF },

{ AS_HB_MSG_COMPRESSED_PAYLOAD, M_FT_BUF },

{ AS_HB_MSG_INFO_REQUEST, M_FT_BUF },

{ AS_HB_MSG_INFO_REPLY, M_FT_BUF },

{ AS_HB_MSG_FABRIC_DATA, M_FT_BUF },

{ AS_HB_MSG_HB_DATA, M_FT_BUF },

{ AS_HB_MSG_PAXOS_DATA, M_FT_BUF },

{ AS_HB_MSG_SKEW_MONITOR_DATA, M_FT_UINT64 } };

/*
 * ----------------------------------------------------------------------------
 * Private internal function forward declarations.
 * ----------------------------------------------------------------------------
 */

static void info_append_addrs(cf_dyn_buf *db, const char *name, const cf_addr_list *list);
static uint32_t round_up_pow2(uint32_t v);
static int vector_find(cf_vector* vector, const void* element);

static void endpoint_list_copy(as_endpoint_list** dest, as_endpoint_list* src);
static void endpoint_list_to_string_process(const as_endpoint_list* endpoint_list, void* udata);
static void endpoint_list_equal_process(const as_endpoint_list* endpoint_list, void* udata);

static int msg_compression_threshold(int mtu);
static int msg_endpoint_list_get(msg* msg, as_endpoint_list** endpoint_list);
static int msg_id_get(msg* msg, uint32_t* id);
static int msg_nodeid_get(msg* msg, cf_node* nodeid);
static int msg_send_hlc_ts_get(msg* msg, as_hlc_timestamp* send_ts);
static int msg_type_get(msg* msg, as_hb_msg_type* type);
static int msg_cluster_name_get(msg* msg, char** cluster_name);
static int msg_node_list_get(msg* msg, int field_id, cf_node** adj_list, size_t* adj_length);
static int msg_adjacency_get(msg* msg, cf_node** adj_list, size_t* adj_length);
static void msg_node_list_set(msg* msg, int field_id, cf_node* node_list, size_t node_length);
static void msg_adjacency_set(msg* msg, cf_node* adj_list, size_t adj_length);
static int msg_info_reply_get(msg* msg, as_hb_mesh_info_reply** reply, size_t* reply_count);
static void msg_published_endpoints_fill(const as_endpoint_list* published_endpoint_list, void* udata);
static void msg_src_fields_fill(msg* msg);
static void msg_type_set(msg* msg, as_hb_msg_type msg_type);

static int config_mcsize();
static const cf_serv_cfg* config_bind_cfg_get();
static const cf_mserv_cfg* config_multicast_group_cfg_get();
static uint32_t config_tx_interval_get();
static void config_tx_interval_set(uint32_t new_interval);
static uint32_t config_override_mtu_get();
static void config_override_mtu_set(uint32_t mtu);
static uint32_t config_max_intervals_missed_get();
static void config_max_intervals_missed_set(uint32_t new_max);
static unsigned char config_multicast_ttl_get();
static as_hb_protocol config_protocol_get();
static void config_protocol_set(as_hb_protocol new_protocol);
static cf_node config_self_nodeid_get();
static as_hb_mode config_mode_get();
static void config_bind_serv_cfg_expand(const cf_serv_cfg* bind_cfg, cf_serv_cfg* published_cfg, bool ipv4_only);
static bool config_binding_is_valid(char** error, as_hb_protocol protocol);

static void channel_init_channel(as_hb_channel* channel);
static void channel_event_init(as_hb_channel_event* event);
static bool channel_is_running();
static bool channel_is_stopped();
static uint32_t channel_win_grace_ms();
static void channel_events_enabled_set(bool enabled);
static bool channel_are_events_enabled();
static void channel_event_queue(as_hb_channel_event* event);
static void channel_event_publish_pending();
static int channel_get_channel(cf_socket* socket, as_hb_channel* result);
static void channel_socket_shutdown(cf_socket* socket);
static int channel_socket_get(cf_node nodeid, cf_socket** socket);
static bool channel_cf_sockets_contains(cf_sockets* sockets, cf_socket* to_find);
static void channel_socket_destroy(cf_socket* sock);
static void channel_socket_close(cf_socket* socket, bool remote_close, bool raise_close_event);
static void channel_sockets_close(cf_vector* sockets);
static void channel_socket_close_queue(cf_socket* socket, bool is_remote_close, bool raise_close_event);
static void channel_socket_close_pending();
static void channel_socket_register(cf_socket* socket, bool is_multicast, bool is_inbound, cf_sock_addr* endpoint_addr);
static void channel_accept_connection(cf_socket* lsock);
static as_hb_channel_msg_read_status channel_compressed_message_parse(msg* msg, void* buffer, int buffer_content_len);
static void channel_endpoint_find_iterate_fn(const as_endpoint* endpoint, void* udata);
static int channel_endpoint_search_reduce(const void* key, void* data, void* udata);
static bool channel_endpoint_is_connected(as_endpoint_list* endpoint_list);
static as_hb_channel_msg_read_status channel_multicast_msg_read(cf_socket* socket, msg* msg);
static as_hb_channel_msg_read_status channel_mesh_msg_read(cf_socket* socket, msg* msg);
static void channel_node_attach(cf_socket* socket, as_hb_channel* channel, cf_node nodeid);
static bool channel_socket_should_live(cf_socket* socket, as_hb_channel* channel);
static cf_socket* channel_socket_resolve(cf_socket* socket1, cf_socket* socket2);
static int channel_msg_sanity_check(as_hb_channel_event* msg_event);
static int channel_msg_event_process(cf_socket* socket, as_hb_channel_event* event);
static void channel_msg_read(cf_socket* socket);
static void channel_channels_idle_check();
void* channel_tender(void* arg);
static bool channel_mesh_endpoint_filter(const as_endpoint* endpoint, void* udata);
static void channel_mesh_channel_establish(as_endpoint_list** endpoint_lists, int endpoint_list_count);
static int channel_node_disconnect(cf_node nodeid);
static void channel_mesh_listening_socks_register(cf_sockets* listening_sockets);
static void channel_mesh_listening_socks_deregister(cf_sockets* listening_sockets);
static void channel_multicast_listening_socks_register(cf_sockets* listening_sockets);
static void channel_multicast_listening_socks_deregister(cf_sockets* listening_sockets);
static void channel_init();
static void channel_start();
static int channel_sockets_get_reduce(const void* key, void* data, void* udata);
static void channel_stop();
static int channel_mesh_msg_send(cf_socket* socket, uint8_t* buff, size_t buffer_length);
static int channel_multicast_msg_send(cf_socket* socket, uint8_t* buff, size_t buffer_length);
static bool channel_msg_is_compression_required(msg* msg, int wire_size, int mtu);
static int channel_msg_buffer_size_get(int wire_size, int mtu);
static size_t channel_msg_buffer_fill(msg* original_msg, int wire_size, int mtu, uint8_t* buffer, size_t buffer_len);
static int channel_msg_unicast(cf_node dest, msg* msg);
static int channel_msg_broadcast_reduce(const void* key, void* data, void* udata);
static int channel_msg_broadcast(msg* msg);
static void channel_clear();
static int channel_dump_reduce(const void* key, void* data, void* udata);
static void channel_dump(bool verbose);

static bool mesh_is_running();
static bool mesh_is_stopped();
static void mesh_published_endpoints_process(endpoint_list_process_fn process_fn, void* udata);
static const char* mesh_node_status_string(as_hb_mesh_node_status status);
static int mesh_seed_delete_unsafe(int seed_index);
static int mesh_seed_find_unsafe(char* host, int port);
static void mesh_tend_udata_capacity_ensure(as_hb_mesh_tend_reduce_udata* tend_reduce_udata, int mesh_node_count);
static void mesh_node_status_change(as_hb_mesh_node* mesh_node, as_hb_mesh_node_status new_status);
static void mesh_listening_sockets_close();
static void mesh_seed_host_list_get(cf_dyn_buf* db, bool tls);
static void mesh_seed_inactive_refresh_get_unsafe(cf_vector* inactive_seeds_p);
static void mesh_stop();
static int mesh_tend_reduce(const void* key, void* data, void* udata);
void* mesh_tender(void* arg);
static void mesh_node_destroy(as_hb_mesh_node* mesh_node);
static void mesh_endpoint_addr_find_iterate(const as_endpoint* endpoint, void* udata);
static bool mesh_node_is_discovered(cf_node nodeid);
static bool mesh_node_endpoint_list_is_valid(cf_node nodeid);
static int mesh_node_get(cf_node nodeid, as_hb_mesh_node* mesh_node);
static void mesh_channel_on_node_disconnect(as_hb_channel_event* event);
static bool mesh_node_check_fix_self_msg(as_hb_channel_event* event);
static void mesh_node_data_update(as_hb_channel_event* event);
static int mesh_info_reply_sizeof(as_hb_mesh_info_reply* reply, int reply_count, size_t* reply_size);
static void mesh_nodes_send_info_reply(cf_node dest, as_hb_mesh_info_reply* reply, size_t reply_count);
static msg* mesh_info_msg_init(as_hb_msg_type msg_type);
static void mesh_nodes_send_info_request(msg* in_msg, cf_node dest, cf_node* to_discover, size_t to_discover_count);
static void mesh_channel_on_pulse(msg* msg);
static void mesh_channel_on_info_request(msg* msg);
static void mesh_channel_on_info_reply(msg* msg);
static int mesh_tip(char* host, int port, bool tls);
static void mesh_channel_event_process(as_hb_channel_event* event);
static void mesh_init();
static int mesh_free_node_data_reduce(const void* key, void* data, void* udata);
static int mesh_tip_clear_reduce(const void* key, void* data, void* udata);
static int mesh_peer_endpoint_reduce(const void* key, void* data, void* udata);
static void mesh_clear();
static void mesh_listening_sockets_open();
static void mesh_start();
static int mesh_dump_reduce(const void* key, void* data, void* udata);
static void mesh_dump(bool verbose);

static void multicast_init();
static void multicast_clear();
static void multicast_listening_sockets_open();
static void multicast_start();
static void multicast_listening_sockets_close();
static void multicast_stop();
static void multicast_dump(bool verbose);
static int multicast_supported_cluster_size_get();

static bool hb_is_initialized();
static bool hb_is_running();
static bool hb_is_stopped();
static void hb_mode_init();
static void hb_mode_start();
static int hb_mtu();
static void hb_msg_init();
static uint32_t hb_protocol_identifier_get();
static cf_clock hb_node_depart_time(cf_clock detect_time);
static bool hb_is_mesh();
static void hb_event_queue(as_hb_internal_event_type event_type, const cf_node* nodes, int node_count);
static void hb_event_publish_pending();
static int hb_adjacency_free_data_reduce(const void* key, void* data, void* udata);
static void hb_clear();
static int hb_adjacency_iterate_reduce(const void* key, void* data, void* udata);
static void hb_plugin_set_fn(msg* msg);
static void hb_plugin_parse_data_fn(msg* msg, cf_node source, as_hb_plugin_node_data* prev_plugin_data, as_hb_plugin_node_data* plugin_data);
static msg* hb_msg_get();
static void hb_msg_return(msg* msg);
static void hb_plugin_msg_fill(msg* msg);
static void hb_plugin_msg_parse(msg* msg, as_hb_adjacent_node* adjacent_node, as_hb_plugin* plugins, bool plugin_data_changed[]);
static void hb_plugin_init();
void* hb_transmitter(void* arg);
static int hb_adjacent_node_get(cf_node nodeid, as_hb_adjacent_node* adjacent_node);
static void hb_adjacent_node_plugin_data_get(as_hb_adjacent_node* adjacent_node, as_hb_plugin_id plugin_id, void** plugin_data, size_t* plugin_data_size);
static void hb_adjacent_node_adjacency_get(as_hb_adjacent_node* adjacent_node, cf_node** adjacency_list, size_t* adjacency_length);
static bool hb_node_has_expired(cf_node nodeid, as_hb_adjacent_node* adjacent_node);
static bool hb_self_is_duplicate();
static void hb_self_duplicate_update();
static void hb_adjacent_node_destroy(as_hb_adjacent_node* adjacent_node);
static int hb_adjacency_tend_reduce(const void* key, void* data, void* udata);
void* hb_adjacency_tender(void* arg);
static void hb_tx_start();
static void hb_tx_stop();
static void hb_adjacency_tender_start();
static void hb_adjacency_tender_stop();
static void hb_init();
static void hb_start();
static void hb_stop();
static void hb_plugin_register(as_hb_plugin* plugin);
static bool hb_msg_is_obsolete(as_hb_channel_event* event, as_hlc_timestamp send_ts);
static void hb_endpoint_change_tracker_update(uint64_t* tracker, bool endpoint_changed);
static bool hb_endpoint_change_tracker_is_normal(uint64_t tracker);
static bool hb_endpoint_change_tracker_has_changed(uint64_t tracker);
static int hb_adjacent_node_update(as_hb_channel_event* msg_event, as_hb_adjacent_node* adjacent_node, bool plugin_data_changed[]);
static bool hb_node_can_consider_adjacent(as_hb_adjacent_node* adjacent_node);
static void hb_channel_on_self_pulse(as_hb_channel_event* msg_event);
static void hb_channel_on_pulse(as_hb_channel_event* msg_event);
static void hb_channel_on_msg_rcvd(as_hb_channel_event* event);
static void hb_handle_cluster_name_mismatch(as_hb_channel_event* event);
static void hb_channel_event_process(as_hb_channel_event* event);
static void hb_mode_dump(bool verbose);
static int hb_dump_reduce(const void* key, void* data, void* udata);
static void hb_dump(bool verbose);
static void hb_adjacency_graph_invert(cf_vector* nodes, uint8_t** inverted_graph);
static void hb_maximal_clique_evict(cf_vector* nodes, cf_vector* nodes_to_evict);
static int hb_plugin_data_iterate_reduce(const void* key, void* data, void* udata);
static void hb_plugin_data_iterate_all(as_hb_plugin_id pluginid,
		as_hb_plugin_data_iterate_fn iterate_fn, void* udata);

/*
 * ----------------------------------------------------------------------------
 * Public functions.
 * ----------------------------------------------------------------------------
 */
/**
 * Initialize the heartbeat subsystem.
 */
void
as_hb_init()
{
	// Initialize hb subsystem.
	hb_init();

	// Add the mesh seed nodes.
	// Using one time seed config outside the config module.
	if (hb_is_mesh()) {
		for (int i = 0; i < AS_CLUSTER_SZ; i++) {
			if (g_config.hb_config.mesh_seed_addrs[i]) {
				mesh_tip(g_config.hb_config.mesh_seed_addrs[i],
						g_config.hb_config.mesh_seed_ports[i],
						g_config.hb_config.mesh_seed_tls[i]);
			}
			else {
				break;
			}
		}
	}
}

/**
 * Start the heartbeat subsystem.
 */
void
as_hb_start()
{
	hb_start();
}

/**
 * Shut down the heartbeat subsystem.
 */
void
as_hb_shutdown()
{
	hb_stop();
}

/**
 * Indicates if self node is a duplicate
 */
bool
as_hb_self_is_duplicate()
{
	return hb_self_is_duplicate();
}

/**
 * Free the data structures of heart beat.
 */
void
as_hb_destroy()
{
	// Destroy the main module.
	hb_clear();
}

/**
 * Return a string representation of a heartbeat protocol type.
 *
 * @param protocol for which the string is computed
 * @param protocol_s string representation of protocol
 */
void
as_hb_protocol_get_s(as_hb_protocol protocol, char* protocol_s)
{
	char *str;
	switch (protocol) {
	case AS_HB_PROTOCOL_V3:
		str = "v3";
		break;
	case AS_HB_PROTOCOL_NONE:
		str = "none";
		break;
	case AS_HB_PROTOCOL_RESET:
		str = "reset";
		break;
	default:
		str = "undefined";
	}

	sprintf(protocol_s, "%s", str);
}

/**
 * Set heartbeat protocol version.
 */
as_hb_protocol
as_hb_protocol_get()
{
	return config_protocol_get();
}

/**
 * Set heartbeat protocol version.
 */
int
as_hb_protocol_set(as_hb_protocol new_protocol)
{
	SET_PROTOCOL_LOCK();
	int rv = 0;
	if (config_protocol_get() == new_protocol) {
		INFO("no heartbeat protocol change needed");
		rv = 0;
		goto Exit;
	}
	char old_protocol_s[HB_PROTOCOL_STR_MAX_LEN];
	char new_protocol_s[HB_PROTOCOL_STR_MAX_LEN];
	as_hb_protocol_get_s(config_protocol_get(), old_protocol_s);
	as_hb_protocol_get_s(new_protocol, new_protocol_s);
	switch (new_protocol) {
	case AS_HB_PROTOCOL_V3:
		if (hb_is_running()) {
			INFO("disabling current heartbeat protocol %s", old_protocol_s);
			hb_stop();
		}
		INFO("setting heartbeat protocol version number to %s", new_protocol_s);
		config_protocol_set(new_protocol);
		hb_start();
		INFO("heartbeat protocol version set to %s", new_protocol_s);
		break;

	case AS_HB_PROTOCOL_NONE:
		INFO("setting heartbeat protocol version to none");
		hb_stop();
		config_protocol_set(new_protocol);
		INFO("heartbeat protocol set to none");
		break;

	case AS_HB_PROTOCOL_RESET:
		if (config_protocol_get() == AS_HB_PROTOCOL_NONE) {
			INFO("heartbeat messaging disabled ~~ not resetting");
			rv = -1;
			goto Exit;
		}

		// NB: "protocol" is never actually set to "RESET" ~~
		// it is simply a trigger for the reset action.
		INFO("resetting heartbeat messaging");

		hb_stop();

		hb_clear();

		hb_start();

		break;

	default:
		WARNING("unknown heartbeat protocol version number: %d", new_protocol);
		rv = -1;
		goto Exit;
	}

Exit:
	SET_PROTOCOL_UNLOCK();
	return rv;
}

/**
 * Register a heartbeat plugin.
 */
void
as_hb_plugin_register(as_hb_plugin* plugin)
{
	if (!hb_is_initialized()) {
		WARNING(
				"main heartbeat module uninitialized - not registering the plugin");
		return;
	}
	hb_plugin_register(plugin);
}

/**
 * Register a heartbeat node event listener.
 */
void
as_hb_register_listener(as_hb_event_fn event_callback, void* udata)
{
	if (!hb_is_initialized()) {
		WARNING(
				"main heartbeat module uninitialized - not registering the listener");
		return;
	}

	HB_LOCK();

	if (g_hb_event_listeners.event_listener_count >=
	AS_HB_EVENT_LISTENER_MAX) {
		CRASH("cannot register more than %d event listeners",
				AS_HB_EVENT_LISTENER_MAX);
	}

	g_hb_event_listeners.event_listeners[g_hb_event_listeners.event_listener_count].event_callback =
			event_callback;
	g_hb_event_listeners.event_listeners[g_hb_event_listeners.event_listener_count].udata =
			udata;
	g_hb_event_listeners.event_listener_count++;

	HB_UNLOCK();
}

/**
 * Validate heartbeat config.
 */
void
as_hb_config_validate()
{
	char *error;
	// Validate clustering and heartbeat version compatibility.
	as_hb_protocol hb_protocol = config_protocol_get();

	if (hb_protocol != AS_HB_PROTOCOL_V3
			&& hb_protocol != AS_HB_PROTOCOL_NONE) {
		CRASH_NOSTACK("clustering protocol v5 requires hearbeat version v3");
	}

	if (!config_binding_is_valid(&error, hb_protocol)) {
		CRASH_NOSTACK("%s", error);
	}
}

/**
 * Override the computed MTU for the network interface used by heartbeat.
 */
void
as_hb_override_mtu_set(int mtu)
{
	config_override_mtu_set(mtu);
}

/**
 * Get the heartbeat pulse transmit interval.
 */
uint32_t
as_hb_tx_interval_get()
{
	return config_tx_interval_get();
}

/**
 * Set the heartbeat pulse transmit interval.
 */
int
as_hb_tx_interval_set(uint32_t new_interval)
{
	if (new_interval < AS_HB_TX_INTERVAL_MS_MIN
			|| new_interval > AS_HB_TX_INTERVAL_MS_MAX) {
		WARNING("heartbeat interval must be >= %u and <= %u - ignoring %u",
				AS_HB_TX_INTERVAL_MS_MIN, AS_HB_TX_INTERVAL_MS_MAX,
				new_interval);
		return (-1);
	}
	config_tx_interval_set(new_interval);
	return (0);
}

/**
 * Get the maximum number of missed heartbeat intervals after which a node is
 * considered expired.
 */
uint32_t
as_hb_max_intervals_missed_get()
{
	return config_max_intervals_missed_get();
}

/**
 * Set the maximum number of missed heartbeat intervals after which a node is
 * considered expired.
 */
int
as_hb_max_intervals_missed_set(uint32_t new_max)
{
	if (new_max < AS_HB_MAX_INTERVALS_MISSED_MIN) {
		WARNING("heartbeat timeout must be >= %u - ignoring %u",
				AS_HB_MAX_INTERVALS_MISSED_MIN, new_max);
		return (-1);
	}
	config_max_intervals_missed_set(new_max);
	return (0);
}

/**
 * Get the timeout interval to consider a node dead / expired in milliseconds if
 * no heartbeat pulse messages are received.
 */
uint32_t
as_hb_node_timeout_get()
{
	return HB_NODE_TIMEOUT();
}

/**
 * Populate the buffer with heartbeat configuration.
 */
void
as_hb_info_config_get(cf_dyn_buf* db)
{
	if (hb_is_mesh()) {
		info_append_string(db, "heartbeat.mode", "mesh");
		info_append_addrs(db, "heartbeat.address", &g_config.hb_serv_spec.bind);
		info_append_uint32(db, "heartbeat.port",
				(uint32_t)g_config.hb_serv_spec.bind_port);
		info_append_addrs(db, "heartbeat.tls-address",
				&g_config.hb_tls_serv_spec.bind);
		info_append_uint32(db, "heartbeat.tls-port",
				g_config.hb_tls_serv_spec.bind_port);
		info_append_string_safe(db, "heartbeat.tls-name",
				g_config.hb_tls_serv_spec.tls_our_name);
		mesh_seed_host_list_get(db, true);
	}
	else {
		info_append_string(db, "heartbeat.mode", "multicast");
		info_append_addrs(db, "heartbeat.address", &g_config.hb_serv_spec.bind);
		info_append_addrs(db, "heartbeat.multicast-group",
				&g_config.hb_multicast_groups);
		info_append_uint32(db, "heartbeat.port",
				(uint32_t)g_config.hb_serv_spec.bind_port);
	}

	info_append_uint32(db, "heartbeat.interval", config_tx_interval_get());
	info_append_uint32(db, "heartbeat.timeout",
			config_max_intervals_missed_get());

	info_append_int(db, "heartbeat.mtu", hb_mtu());

	char protocol_s[HB_PROTOCOL_STR_MAX_LEN];
	as_hb_protocol_get_s(config_protocol_get(), protocol_s);

	info_append_string(db, "heartbeat.protocol", protocol_s);
}

/**
 * Populate heartbeat endpoints.
 */
void
as_hb_info_endpoints_get(cf_dyn_buf* db)
{
	const cf_serv_cfg *cfg = config_bind_cfg_get();

	if (cfg->n_cfgs == 0) {
		// Will never happen in practice.
		return;
	}

	info_append_int(db, "heartbeat.port", g_config.hb_serv_spec.bind_port);

	char *string = as_info_bind_to_string(cfg, CF_SOCK_OWNER_HEARTBEAT);
	info_append_string(db, "heartbeat.addresses", string);
	cf_free(string);

	info_append_int(db, "heartbeat.tls-port",
			g_config.hb_tls_serv_spec.bind_port);

	string = as_info_bind_to_string(cfg, CF_SOCK_OWNER_HEARTBEAT_TLS);
	info_append_string(db, "heartbeat.tls-addresses", string);
	cf_free(string);

	if (hb_is_mesh()) {
		MESH_LOCK();
		cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
				mesh_peer_endpoint_reduce, db);
		MESH_UNLOCK();
	}
	else {
		// Output multicast groups.
		const cf_mserv_cfg* multicast_cfg = config_multicast_group_cfg_get();
		if (multicast_cfg->n_cfgs == 0) {
			return;
		}

		cf_dyn_buf_append_string(db, "heartbeat.multicast-groups=");
		uint32_t count = 0;
		for (uint32_t i = 0; i < multicast_cfg->n_cfgs; ++i) {
			if (count > 0) {
				cf_dyn_buf_append_char(db, ',');
			}

			cf_dyn_buf_append_string(db,
					cf_ip_addr_print(&multicast_cfg->cfgs[i].addr));
			++count;
		}
		cf_dyn_buf_append_char(db, ';');
	}
}

/**
 * Generate a string for listening address and port in format ip_address:port
 * and return the heartbeat mode.
 *
 * @param mode (output) current heartbeat subsystem mode.
 * @param addr_port (output) listening ip address and port formatted as
 * ip_address:port
 * @param addr_port_capacity the capacity of the addr_port input.
 */
void
as_hb_info_listen_addr_get(as_hb_mode* mode, char* addr_port,
		size_t addr_port_capacity)
{
	*mode = hb_is_mesh() ? AS_HB_MODE_MESH : AS_HB_MODE_MULTICAST;
	if (hb_is_mesh()) {
		endpoint_list_to_string_udata udata;
		udata.endpoint_list_str = addr_port;
		udata.endpoint_list_str_capacity = addr_port_capacity;
		mesh_published_endpoints_process(endpoint_list_to_string_process,
				&udata);
	}
	else {
		const cf_mserv_cfg* multicast_cfg = config_multicast_group_cfg_get();

		char* write_ptr = addr_port;
		int remaining = addr_port_capacity;

		// Ensure we leave space for the terminating NULL delimiter.
		for (int i = 0; i < multicast_cfg->n_cfgs && remaining > 1; i++) {
			cf_sock_addr temp;
			cf_ip_addr_copy(&multicast_cfg->cfgs[i].addr, &temp.addr);
			temp.port = multicast_cfg->cfgs[i].port;
			int rv = cf_sock_addr_to_string(&temp, write_ptr, remaining);
			if (rv <= 0) {
				// We exhausted the write buffer.
				// Ensure NULL termination.
				addr_port[addr_port_capacity - 1] = 0;
				return;
			}

			write_ptr += rv;
			remaining -= rv;

			if (i != multicast_cfg->n_cfgs - 1 && remaining > 1) {
				*write_ptr = ',';
				write_ptr++;
				remaining--;
			}
		}

		// Ensure NULL termination.
		*write_ptr = 0;
	}
}

/**
 * Populate the buffer with duplicate nodeids.
 */
void
as_hb_info_duplicates_get(cf_dyn_buf* db)
{
	cf_dyn_buf_append_string(db, "cluster_duplicate_nodes=");

	HB_LOCK();
	bool self_is_duplicate = hb_self_is_duplicate();
	int num_probation = cf_shash_get_size(g_hb.on_probation);
	cf_node duplicate_list[num_probation + 1];

	if (!self_is_duplicate && num_probation == 0) {
		cf_dyn_buf_append_string(db, "null");
		goto Exit;
	}

	as_hb_adjacency_reduce_udata probation_reduce_udata = { duplicate_list, 0 };

	cf_shash_reduce(g_hb.on_probation, hb_adjacency_iterate_reduce,
			&probation_reduce_udata);

	if (hb_self_is_duplicate()) {
		duplicate_list[probation_reduce_udata.adj_count++] =
				config_self_nodeid_get();
	}

	int num_duplicates = probation_reduce_udata.adj_count;
	qsort(duplicate_list, num_duplicates, sizeof(cf_node),
			cf_node_compare_desc);

	for (int i = 0; i < num_duplicates; i++) {
		cf_dyn_buf_append_uint64_x(db, duplicate_list[i]);
		cf_dyn_buf_append_char(db, ',');
	}
	cf_dyn_buf_chomp(db);

Exit:
	HB_UNLOCK();
	cf_dyn_buf_append_char(db, ';');
}

/*
 * -----------------------------------------------------------------
 * Mesh mode public API
 * -----------------------------------------------------------------
 */

/**
 * Add an aerospike instance from the mesh seed list.
 */
int
as_hb_mesh_tip(char* host, int port, bool tls)
{
	if (!hb_is_mesh()) {
		WARNING("tip not applicable for multicast");
		return (-1);
	}

	return mesh_tip(host, port, tls);
}

/**
 * Remove a mesh node instance from the mesh list.
 */
int
as_hb_mesh_tip_clear(char* host, int port)
{
	if (!hb_is_mesh()) {
		WARNING("tip clear not applicable for multicast");
		return (-1);
	}

	if (host == NULL || host[0] == 0
			|| strnlen(host, DNS_NAME_MAX_SIZE) == DNS_NAME_MAX_SIZE) {
		WARNING("invalid tip clear host:%s or port:%d", host, port);
		return (-1);
	}

	MESH_LOCK();
	DETAIL("executing tip clear for %s:%d", host, port);

	// FIXME: Remove the mesh host entry and close channel was done to meet
	// AER-5241 ???
	// tip-clear is not a mechanism to throw a connected node out of the
	// cluster.
	// We should not be required to use this mechanism now.
	// tip-clear should only be used to cleanup seed list after decommisioning
	// an ip.
	cf_ip_addr addrs[CF_SOCK_CFG_MAX];
	uint32_t n_addrs = CF_SOCK_CFG_MAX;

	as_hb_mesh_tip_clear_udata mesh_tip_clear_reduce_udata;
	strcpy(mesh_tip_clear_reduce_udata.host, host);
	mesh_tip_clear_reduce_udata.port = port;
	mesh_tip_clear_reduce_udata.entry_deleted = false;
	mesh_tip_clear_reduce_udata.nodeid = 0;

	if (cf_ip_addr_from_string_multi(host, addrs, &n_addrs) != 0) {
		n_addrs = 0;
	}

	mesh_tip_clear_reduce_udata.addrs = addrs;
	mesh_tip_clear_reduce_udata.n_addrs = n_addrs;

	int seed_index = mesh_seed_find_unsafe(host, port);
	if (seed_index >= 0) {
		as_hb_mesh_seed* seed = cf_vector_getp(
				&g_hb.mode_state.mesh_state.seeds, seed_index);
		mesh_tip_clear_reduce_udata.nodeid = seed->mesh_nodeid;
	}

	// Refresh the mapping between the seeds and the mesh hosts.
	mesh_seed_inactive_refresh_get_unsafe (NULL);
	cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
			mesh_tip_clear_reduce, &mesh_tip_clear_reduce_udata);

	// Remove the seed entry in case we do not find a matching mesh entry.
	// Will happen trivially if this seed could not be connected.
	mesh_tip_clear_reduce_udata.entry_deleted =
			mesh_tip_clear_reduce_udata.entry_deleted
					|| mesh_seed_delete_unsafe(
							mesh_seed_find_unsafe(host, port)) == 0;

	MESH_UNLOCK();
	return mesh_tip_clear_reduce_udata.entry_deleted ? 0 : -1;
}

/**
 * Clear the entire mesh list.
 */
int
as_hb_mesh_tip_clear_all(uint32_t* cleared)
{
	if (!hb_is_mesh()) {
		WARNING("tip clear not applicable for multicast");
		return (-1);
	}

	MESH_LOCK();
	*cleared = cf_shash_get_size(
			g_hb.mode_state.mesh_state.nodeid_to_mesh_node);

	// Refresh the mapping between the seeds and the mesh hosts.
	mesh_seed_inactive_refresh_get_unsafe(NULL);
	cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
			mesh_tip_clear_reduce, NULL);

	// Remove all entries that did not have a matching mesh endpoint.
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		if (mesh_seed_delete_unsafe(i) == 0) {
			i--;
			element_count--;
		}
		else {
			// Should not happen in practice.
			as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
			CRASH("error deleting mesh seed entry %s:%d", seed->seed_host_name,
					seed->seed_port);
		}
	}

	MESH_UNLOCK();
	return (0);
}

/**
 * Read the plugin data for a node in the adjacency list. The plugin_data->data
 * input param should be pre allocated and plugin_data->data_capacity should
 * indicate its capacity.
 *
 * @param nodeid the node id
 * @param pluginid the plugin identifier.
 * @param plugin_data (input/output) on success plugin_data->data will be the
 * plugin's data for the node and plugin_data->data_size will be the data size.
 * node. NULL if there is no plugin data.
 * @praram msg_hlc_ts  (output) if not NULL will be filled with the timestamp of
 * when the hb message for this data was received.
 * @param recv_monotonic_ts (output) if not NULL will be filled with monotonic
 * wall clock receive timestamp for this plugin data.
 * @return 0 on success and -1 on error, where errno will be set to	 ENOENT if
 * there is no entry for this node and ENOMEM if the input plugin data's
 * capacity is less than plugin's data. In ENOMEM case plugin_data->data_size
 * will be set to the required capacity.
 */
int
as_hb_plugin_data_get(cf_node nodeid, as_hb_plugin_id plugin,
		as_hb_plugin_node_data* plugin_data, as_hlc_msg_timestamp* msg_hlc_ts,
		cf_clock* recv_monotonic_ts)
{
	int rv = 0;

	HB_LOCK();

	as_hb_adjacent_node adjacent_node;
	if (hb_adjacent_node_get(nodeid, &adjacent_node) != 0) {
		rv = -1;
		plugin_data->data_size = 0;
		errno = ENOENT;
		goto Exit;
	}

	as_hb_plugin_node_data* plugin_data_internal =
			&adjacent_node.plugin_data[plugin][adjacent_node.plugin_data_cycler
					% 2];

	if (plugin_data_internal->data && plugin_data_internal->data_size) {
		// Set the plugin data size
		plugin_data->data_size = plugin_data_internal->data_size;

		if (plugin_data_internal->data_size > plugin_data->data_capacity) {
			rv = -1;
			errno = ENOMEM;
			goto Exit;
		}

		// Copy over the stored copy of the plugin data.
		memcpy(plugin_data->data, plugin_data_internal->data,
				plugin_data_internal->data_size);

		// Copy the message timestamp.
		if (msg_hlc_ts) {
			memcpy(msg_hlc_ts, &adjacent_node.last_msg_hlc_ts,
					sizeof(as_hlc_msg_timestamp));
		}

		if (recv_monotonic_ts) {
			*recv_monotonic_ts = adjacent_node.last_updated_monotonic_ts;
		}

		rv = 0;
	}
	else {
		// No plugin data set.
		plugin_data->data_size = 0;
		if (recv_monotonic_ts) {
			*recv_monotonic_ts = 0;
		}
		if (msg_hlc_ts) {
			memset(msg_hlc_ts, 0, sizeof(as_hlc_msg_timestamp));
		}
		rv = 0;
	}

Exit:
	HB_UNLOCK();
	return rv;
}

/**
 * Call the iterate method on plugin data for all nodes in the input vector. The
 * iterate function will be invoked for all nodes in the input vector even if
 * they are not in the adjacency list or they have no plugin data. Plugin data
 * will be NULL with size zero in such cases.
 *
 * @param nodes the iterate on.
 * @param plugin the plugin identifier.
 * @param iterate_fn the iterate function invoked for plugin data for every
 * node.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration.
 * NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
void
as_hb_plugin_data_iterate(cf_vector* nodes, as_hb_plugin_id plugin,
		as_hb_plugin_data_iterate_fn iterate_fn, void* udata)

{
	HB_LOCK();

	int size = cf_vector_size(nodes);

	for (int i = 0; i < size; i++) {
		cf_node* nodeid = cf_vector_getp(nodes, i);

		if (nodeid == NULL || *nodeid == 0) {
			continue;
		}

		as_hb_adjacent_node nodeinfo;

		if (hb_adjacent_node_get(*nodeid, &nodeinfo) == 0) {
			size_t data_size = 0;
			void* data = NULL;

			hb_adjacent_node_plugin_data_get(&nodeinfo, plugin, &data,
					&data_size);

			iterate_fn(*nodeid, data, data_size,
					nodeinfo.last_updated_monotonic_ts,
					&nodeinfo.last_msg_hlc_ts, udata);
		}
		else {
			// This node is not known to the heartbeat subsystem.
			iterate_fn(*nodeid, NULL, 0, 0, NULL, udata);
		}
	}

	HB_UNLOCK();
}

/**
 * Call the iterate method on all nodes in current adjacency list. Note plugin
 * data can still be NULL if the plugin data failed to parse the plugin data.
 *
 * @param pluginid the plugin identifier.
 * @param iterate_fn the iterate function invoked for plugin data for every
 * node.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration.
 * NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
void
as_hb_plugin_data_iterate_all(as_hb_plugin_id pluginid,
		as_hb_plugin_data_iterate_fn iterate_fn, void* udata)
{
	hb_plugin_data_iterate_all(pluginid, iterate_fn, udata);
}

/**
 * Log the state of the heartbeat module.
 */
void
as_hb_dump(bool verbose)
{
	INFO("Heartbeat Dump:");

	as_hb_mode mode;
	char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
	as_hb_info_listen_addr_get(&mode, endpoint_list_str,
			sizeof(endpoint_list_str));

	// Dump the config.
	INFO("HB Mode: %s (%d)",
			(mode == AS_HB_MODE_MULTICAST ?
					"multicast" :
					(mode == AS_HB_MODE_MESH ? "mesh" : "undefined")), mode);

	INFO("HB Addresses: {%s}", endpoint_list_str);
	INFO("HB MTU: %d", hb_mtu());

	INFO("HB Interval: %d", config_tx_interval_get());
	INFO("HB Timeout: %d", config_max_intervals_missed_get());
	char protocol_s[HB_PROTOCOL_STR_MAX_LEN];
	as_hb_protocol_get_s(config_protocol_get(), protocol_s);
	INFO("HB Protocol: %s (%d)", protocol_s, config_protocol_get());

	// dump mode specific state.
	hb_mode_dump(verbose);

	// Dump the channel state.
	channel_dump(verbose);

	// Dump the adjacency list.
	hb_dump(verbose);
}

/**
 * Indicates if a node is alive.
 */
bool
as_hb_is_alive(cf_node nodeid)
{
	bool is_alive;
	HB_LOCK();

	as_hb_adjacent_node adjacent_node;
	is_alive = (nodeid == config_self_nodeid_get())
			|| (hb_adjacent_node_get(nodeid, &adjacent_node) == 0);

	HB_UNLOCK();
	return is_alive;
}

/**
 * Compute the nodes to evict from the input nodes so that remaining nodes form
 * a clique, based on adjacency lists. Self nodeid is never considered for
 * eviction.
 *
 * @param nodes input cf_node vector.
 * @param nodes_to_evict output cf_node clique array, that is initialized.
 */
void
as_hb_maximal_clique_evict(cf_vector* nodes, cf_vector* nodes_to_evict)
{
	hb_maximal_clique_evict(nodes, nodes_to_evict);
}

/**
 * Read the hlc timestamp for the message.
 * Note: A protected API for the sole benefit of skew monitor.
 *
 * @param msg the incoming message.
 * @param send_ts the output hlc timestamp.
 * @return 0 if the time stamp could be parsed -1 on failure.
 */
int
as_hb_msg_send_hlc_ts_get(msg* msg, as_hlc_timestamp* send_ts)
{
	return msg_send_hlc_ts_get(msg, send_ts);
}

/*
 * ----------------------------------------------------------------------------
 * Common sub module.
 * ----------------------------------------------------------------------------
 */

/*
 * ----------------------------------------------------------------------------
 * Utility
 * ----------------------------------------------------------------------------
 */

/**
 * Round up input int to the nearest power of two.
 */
static uint32_t
round_up_pow2(uint32_t v)
{
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v++;
	return v;
}

/**
 * Generate a hash code for a cf_socket.
 */
static uint32_t
hb_socket_hash_fn(const void* key)
{
	const cf_socket** socket = (const cf_socket**)key;
	return cf_hash_jen32((const uint8_t*)socket, sizeof(cf_socket*));
}

/**
 * Reduce function to delete all entries in a map
 */
static int
hb_delete_all_reduce(const void* key, void* data, void* udata)
{
	return CF_SHASH_REDUCE_DELETE;
}

/*
 * ----------------------------------------------------------------------------
 * Info call related
 * ----------------------------------------------------------------------------
 */

/**
 * Append a address spec to a cf_dyn_buf.
 */
static void
info_append_addrs(cf_dyn_buf *db, const char *name, const cf_addr_list *list)
{
	for (uint32_t i = 0; i < list->n_addrs; ++i) {
		info_append_string(db, name, list->addrs[i]);
	}
}

/*
 * ----------------------------------------------------------------------------
 * Vector operations
 * ----------------------------------------------------------------------------
 */

/**
 * TODO: Move this to cf_vector.
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
	size_t value_len = cf_vector_element_size(vector);
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

/*
 * ----------------------------------------------------------------------------
 * Endpoint list related
 * ----------------------------------------------------------------------------
 */

/**
 * Copy an endpoint list to the destination, while possible reallocating the
 * destination space.
 * @param dest the double pointer to the destination list, because it might need
 * reallocation to accommodate a larger source list.
 * @param src the source endpoint list.
 */
static void
endpoint_list_copy(as_endpoint_list** dest, as_endpoint_list* src)
{
	size_t src_size;

	if (as_endpoint_list_sizeof(src, &src_size) != 0) {
		// Bad endpoint list passed.
		CRASH("invalid adjacency list passed for copying");
	}

	*dest = cf_realloc(*dest, src_size);

	memcpy(*dest, src, src_size);
}

/**
 * Process function to convert endpoint list to a string.
 */
static void
endpoint_list_to_string_process(const as_endpoint_list* endpoint_list,
		void* udata)
{
	endpoint_list_to_string_udata* to_string_udata =
			(endpoint_list_to_string_udata*)udata;
	as_endpoint_list_to_string(endpoint_list,
			to_string_udata->endpoint_list_str,
			to_string_udata->endpoint_list_str_capacity);
}

/**
 * Process function to check if endpoint lists overlap.
 */
static void
endpoint_list_equal_process(const as_endpoint_list* endpoint_list, void* udata)
{
	endpoint_list_equal_check_udata* equal_udata =
			(endpoint_list_equal_check_udata*)udata;

	equal_udata->are_equal = equal_udata->are_equal
			|| as_endpoint_lists_are_equal(endpoint_list, equal_udata->other);
}

/*
 * ----------------------------------------------------------------------------
 * Messge related
 * ----------------------------------------------------------------------------
 */

/**
 * The size of a buffer beyond which compression should be applied. For now set
 * to 60% of the interface mtu.
 */
static int
msg_compression_threshold(int mtu)
{
	return (int)(mtu * 0.6);
}

/**
 * Read advertised endpoint list from an incoming message.
 * @param msg the incoming message.
 * @param endpoint_list the output endpoint. The endpoint_list will point to
 * input message.
 * internal location and should not be freed.
 * @return 0 on success -1 on failure.
 */
static int
msg_endpoint_list_get(msg* msg, as_endpoint_list** endpoint_list)
{
	size_t endpoint_list_size;
	if (msg_get_buf(msg, AS_HB_MSG_ENDPOINTS, (uint8_t**)endpoint_list,
			&endpoint_list_size, MSG_GET_DIRECT) != 0) {
		return -1;
	}

	size_t parsed_size;
	if (as_endpoint_list_nsizeof(*endpoint_list, &parsed_size,
			endpoint_list_size) || parsed_size != endpoint_list_size) {
		return -1;
	}
	return 0;
}

/**
 * Read the protocol identifier for this heartbeat message. These functions can
 * get called multiple times for a single message. Hence they do not increment
 * error counters.
 *
 * @param msg the incoming message.
 * @param id the output id.
 * @return 0 if the id could be parsed -1 on failure.
 */
static int
msg_id_get(msg* msg, uint32_t* id)
{
	if (msg_get_uint32(msg, AS_HB_MSG_ID, id) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Read the source nodeid for a node. These functions can get called multiple
 * times for a single message. Hence they do not increment error counters.
 * @param msg the incoming message.
 * @param nodeid the output nodeid.
 * @return 0 if the nodeid could be parsed -1 on failure.
 */
static int
msg_nodeid_get(msg* msg, cf_node* nodeid)
{
	if (msg_get_uint64(msg, AS_HB_MSG_NODE, nodeid) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Read the HLC send timestamp for the message. These functions can get called
 * multiple times for a single message. Hence they do not increment error
 * counters.
 * @param msg the incoming message.
 * @param send_ts the output hlc timestamp.
 * @return 0 if the time stamp could be parsed -1 on failure.
 */
static int
msg_send_hlc_ts_get(msg* msg, as_hlc_timestamp* send_ts)
{
	if (msg_get_uint64(msg, AS_HB_MSG_HLC_TIMESTAMP, send_ts) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Read the message type.  These functions can get called multiple times for a
 * single message. Hence they do not increment error counters.
 * @param msg the incoming message.
 * @param type the output message type.
 * @return 0 if the type could be parsed -1 on failure.
 */
static int
msg_type_get(msg* msg, as_hb_msg_type* type)
{
	if (msg_get_uint32(msg, AS_HB_MSG_TYPE, type) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Read the cluster name.
 * @param msg the incoming message.
 * @param cluster name of the output message type.
 * @return 0 if the cluster name could be parsed -1 on failure.
 */
static int
msg_cluster_name_get(msg* msg, char** cluster_name)
{
	if (msg_get_str(msg, AS_HB_MSG_CLUSTER_NAME, cluster_name,
			MSG_GET_DIRECT) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Get a pointer to a node list in the message.
 *
 * @param msg the incoming message.
 * @param field_id the field id.
 * @param adj_list output. on success will point to the adjacency list in the
 * message.
 * @para adj_length output. on success will contain the length of the adjacency
 * list.
 * @return 0 on success. -1 if the adjacency list is absent.
 */
static int
msg_node_list_get(msg* msg, int field_id, cf_node** adj_list,
		size_t* adj_length)
{
	if (msg_get_buf(msg, field_id, (uint8_t**)adj_list, adj_length,
			MSG_GET_DIRECT) != 0) {
		return -1;
	}

	// correct adjacency list length.
	*adj_length /= sizeof(cf_node);

	return 0;
}

/**
 * Get a pointer to the adjacency list in the message.
 *
 * @param msg the incoming message.
 * @param adj_list output. on success will point to the adjacency list in the
 * message.
 * @para adj_length output. on success will contain the length of the adjacency
 * list.
 * @return 0 on success. -1 if the adjacency list is absent.
 */
static int
msg_adjacency_get(msg* msg, cf_node** adj_list, size_t* adj_length)
{
	return msg_node_list_get(msg, AS_HB_MSG_HB_DATA, adj_list, adj_length);
}

/**
 * Set a node list on an outgoing messages for a field.
 *
 * @param msg the outgoing message.
 * @param field_id the id of the list field.
 * @param node_list the adjacency list to set.
 * @para node_length the length of the adjacency list.
 */
static void
msg_node_list_set(msg* msg, int field_id, cf_node* node_list,
		size_t node_length)
{
	msg_set_buf(msg, field_id, (uint8_t*)node_list,
			sizeof(cf_node) * node_length, MSG_SET_COPY);
}

/**
 * Set the adjacency list on an outgoing messages.
 *
 * @param msg the outgoing message.
 * @param adj_list the adjacency list to set.
 * @para adj_length the length of the adjacency list.
 */
static void
msg_adjacency_set(msg* msg, cf_node* adj_list, size_t adj_length)
{
	msg_node_list_set(msg, AS_HB_MSG_HB_DATA, adj_list, adj_length);
}

/**
 * Set the info reply on an outgoing messages.
 *
 * @param msg the outgoing message.
 * @param response the response list to set.
 * @para response_count the length of the response list.
 */
static void
msg_info_reply_set(msg* msg, as_hb_mesh_info_reply* response,
		size_t response_count)
{
	size_t response_size = 0;
	if (mesh_info_reply_sizeof(response, response_count, &response_size)) {
		CRASH("error setting info reply on msg");
	}

	msg_set_buf(msg, AS_HB_MSG_INFO_REPLY, (uint8_t*)response, response_size,
			MSG_SET_COPY);

	return;
}

/**
 * Get a pointer to the info reply list in the message.
 *
 * @param msg the incoming message.
 * @param reply output. on success will point to the reply list in the message.
 * @param reply_count output. on success will contain the length of the reply
 * list.
 * @return 0 on success. -1 if the reply list is absent.
 */
static int
msg_info_reply_get(msg* msg, as_hb_mesh_info_reply** reply, size_t* reply_count)
{
	size_t reply_size;
	if (msg_get_buf(msg, AS_HB_MSG_INFO_REPLY, (uint8_t**)reply, &reply_size,
			MSG_GET_DIRECT) != 0) {
		return -1;
	}

	*reply_count = 0;

	// Go over reply and compute the count of replies and also validate the
	// endpoint lists.
	uint8_t* start_ptr = (uint8_t*)*reply;
	int64_t remaining_size = reply_size;

	while (remaining_size > 0) {
		as_hb_mesh_info_reply* reply_ptr = (as_hb_mesh_info_reply*)start_ptr;
		remaining_size -= sizeof(as_hb_mesh_info_reply);
		start_ptr += sizeof(as_hb_mesh_info_reply);
		if (remaining_size <= 0) {
			// Incomplete / garbled info reply message.
			*reply_count = 0;
			return -1;
		}

		size_t endpoint_list_size = 0;
		if (as_endpoint_list_nsizeof(reply_ptr->endpoint_list,
				&endpoint_list_size, remaining_size) != 0) {
			// Incomplete / garbled info reply message.
			*reply_count = 0;
			return -1;
		}

		remaining_size -= endpoint_list_size;
		start_ptr += endpoint_list_size;
		(*reply_count)++;
	}

	return 0;
}

/**
 * Fill a message with an endpoint list.
 */
static void
msg_published_endpoints_fill(const as_endpoint_list* published_endpoint_list,
		void* udata)
{
	endpoint_list_to_msg_udata* to_msg_udata =
			(endpoint_list_to_msg_udata*)udata;
	msg* msg = to_msg_udata->msg;
	bool is_mesh = to_msg_udata->is_mesh;

	if (!published_endpoint_list) {
		if (is_mesh) {
			// Something is messed up. Except for v3 multicast,
			// published list should not be empty.
			WARNING("published endpoint list is empty");
		}
		return;
	}

	// Makes sense only for mesh.
	if (is_mesh && published_endpoint_list) {
		// Set the source address
		size_t endpoint_list_size = 0;
		as_endpoint_list_sizeof(published_endpoint_list, &endpoint_list_size);
		msg_set_buf(msg, AS_HB_MSG_ENDPOINTS,
				(uint8_t*)published_endpoint_list, endpoint_list_size,
				MSG_SET_COPY);
	}
}

/**
 * Fill source fields for the message.
 * @param msg the message to fill the source fields into.
 */
static void
msg_src_fields_fill(msg* msg)
{
	bool is_mesh = hb_is_mesh();

	// Set the hb protocol id / version.
	msg_set_uint32(msg, AS_HB_MSG_ID, hb_protocol_identifier_get());

	// Set the source node.
	msg_set_uint64(msg, AS_HB_MSG_NODE, config_self_nodeid_get());

	endpoint_list_to_msg_udata udata;
	udata.msg = msg;
	udata.is_mesh = is_mesh;

	if (is_mesh) {
		// Endpoint list only valid for mesh mode.
		mesh_published_endpoints_process(msg_published_endpoints_fill, &udata);
	}

	// Set the send hlc timestamp
	msg_set_uint64(msg, AS_HB_MSG_HLC_TIMESTAMP, as_hlc_timestamp_now());
}

/**
 * Set the type for an outgoing message.
 * @param msg the outgoing message.
 * @param msg_type the type to set.
 */
static void
msg_type_set(msg* msg, as_hb_msg_type msg_type)
{
	// Set the message type.
	msg_set_uint32(msg, AS_HB_MSG_TYPE, msg_type);
}

/*
 * ----------------------------------------------------------------------------
 * Config sub module.
 * ----------------------------------------------------------------------------
 */

/**
 * Get mcsize.
 */
static int
config_mcsize()
{
	int mode_cluster_size = 0;
	if (hb_is_mesh()) {
		// Only bounded by available memory. But let's say its infinite.
		mode_cluster_size = INT_MAX;
	}
	else {
		mode_cluster_size = multicast_supported_cluster_size_get();
	}

	// Ensure we are always upper bounded by the absolute max cluster size.
	int supported_cluster_size = MIN(ASC, mode_cluster_size);

	DETAIL("supported cluster size %d", supported_cluster_size);
	return supported_cluster_size;
}

/**
 * Get the binding addresses for the heartbeat subsystem.
 */
static const cf_serv_cfg*
config_bind_cfg_get()
{
	// Not protected by config_lock because it is not changed.
	return &g_config.hb_config.bind_cfg;
}

/**
 * Get the multicast groups for the multicast mode.
 */
static const cf_mserv_cfg*
config_multicast_group_cfg_get()
{
	// Not protected by config_lock. Never updated after config parsing..
	return &g_config.hb_config.multicast_group_cfg;
}

/**
 * Get the heartbeat pulse transmit interval.
 */
static uint32_t
config_tx_interval_get()
{
	HB_CONFIG_LOCK();
	uint32_t interval = g_config.hb_config.tx_interval;
	HB_CONFIG_UNLOCK();
	return interval;
}

/**
 * Set the heartbeat pulse transmit interval.
 */
static void
config_tx_interval_set(uint32_t new_interval)
{
	HB_CONFIG_LOCK();
	INFO("changing value of interval from %d to %d ",
			g_config.hb_config.tx_interval, new_interval);
	g_config.hb_config.tx_interval = new_interval;
	HB_CONFIG_UNLOCK();
}

/**
 * Get the heartbeat pulse transmit interval.
 */
static uint32_t
config_override_mtu_get()
{
	HB_CONFIG_LOCK();
	uint32_t override_mtu = g_config.hb_config.override_mtu;
	HB_CONFIG_UNLOCK();
	return override_mtu;
}

/**
 * Set the heartbeat pulse transmit interval.
 */
static void
config_override_mtu_set(uint32_t mtu)
{
	HB_CONFIG_LOCK();
	INFO("changing value of override mtu from %d to %d ",
			g_config.hb_config.override_mtu, mtu);
	g_config.hb_config.override_mtu = mtu;
	HB_CONFIG_UNLOCK();
	INFO("max supported cluster size is %d", config_mcsize());
}

/**
 * Get the maximum number of missed heartbeat intervals after which a node is
 * considered expired.
 */
static uint32_t
config_max_intervals_missed_get()
{
	uint32_t rv = 0;
	HB_CONFIG_LOCK();
	rv = g_config.hb_config.max_intervals_missed;
	HB_CONFIG_UNLOCK();
	return rv;
}

/**
 * Get the number intervals endpoints should be tracked for.
 */
static uint32_t
config_endpoint_track_intervals_get()
{
	// Allow a grace period of half heartbeat timeout, but lower bounded to at
	// least 3.
	return MAX(3, config_max_intervals_missed_get() / 2);
}

/**
 * Get the maximum number of allowed changes, per endpoint track intervals.
 */
static uint32_t
config_endpoint_changes_allowed_get()
{
	// Allow no change to the endpoint list for now.
	return 0;
}

/**
 * Set the maximum number of missed heartbeat intervals after which a node is
 * considered expired.
 */
static void
config_max_intervals_missed_set(uint32_t new_max)
{
	HB_CONFIG_LOCK();
	INFO("changing value of timeout from %d to %d ",
			g_config.hb_config.max_intervals_missed, new_max);
	g_config.hb_config.max_intervals_missed = new_max;
	HB_CONFIG_UNLOCK();
}

/**
 * Return ttl for multicast packets. Set to zero for default TTL.
 */
static unsigned char
config_multicast_ttl_get()
{
	return g_config.hb_config.multicast_ttl;
}

/**
 * Return the current heartbeat protocol.
 */
static as_hb_protocol
config_protocol_get()
{
	as_hb_protocol rv = 0;
	HB_CONFIG_LOCK();
	rv = g_config.hb_config.protocol;
	HB_CONFIG_UNLOCK();
	return rv;
}

/**
 * Return the current heartbeat protocol.
 */
static void
config_protocol_set(as_hb_protocol new_protocol)
{
	HB_CONFIG_LOCK();
	g_config.hb_config.protocol = new_protocol;
	HB_CONFIG_UNLOCK();
}

/**
 * The nodeid for this node.
 */
static cf_node
config_self_nodeid_get()
{
	// Not protected by config_lock. Never updated after config parsing..
	return g_config.self_node;
}

/**
 * Return the heartbeat subsystem mode.
 */
static as_hb_mode
config_mode_get()
{
	// Not protected by config_lock. Never updated after config parsing..
	return g_config.hb_config.mode;
}

/**
 * Expand "any" binding addresses to actual interface addresses.
 * @param bind_cfg the binding configuration.
 * @param published_cfg (output) the server configuration to expand.
 * @param ipv4_only indicates if only legacy addresses should be allowed.
 */
static void
config_bind_serv_cfg_expand(const cf_serv_cfg* bind_cfg,
		cf_serv_cfg* published_cfg, bool ipv4_only)
{
	cf_serv_cfg_init(published_cfg);
	cf_sock_cfg sock_cfg;

	for (int i = 0; i < bind_cfg->n_cfgs; i++) {
		cf_sock_cfg_copy(&bind_cfg->cfgs[i], &sock_cfg);

		// Expand "any" address to all interfaces.
		if (cf_ip_addr_is_any(&sock_cfg.addr)) {
			cf_ip_addr all_addrs[CF_SOCK_CFG_MAX];
			uint32_t n_all_addrs = CF_SOCK_CFG_MAX;
			if (cf_inter_get_addr_all(all_addrs, &n_all_addrs) != 0) {
				WARNING("error getting all interface addresses");
				n_all_addrs = 0;
			}

			for (int j = 0; j < n_all_addrs; j++) {
				// Skip local address if any is specified.
				if (cf_ip_addr_is_local(&all_addrs[j])
						|| (ipv4_only && !cf_ip_addr_is_legacy(&all_addrs[j]))) {
					continue;
				}

				cf_ip_addr_copy(&all_addrs[j], &sock_cfg.addr);
				if (cf_serv_cfg_add_sock_cfg(published_cfg, &sock_cfg)) {
					CRASH("error initializing published address list");
				}
			}

			// TODO: Does not look like the right warning or the right message.
			if (published_cfg->n_cfgs == 0) {
				WARNING(
						"no network interface addresses detected for heartbeat access");
			}
		}
		else {
			if (ipv4_only && !cf_ip_addr_is_legacy(&bind_cfg->cfgs[i].addr)) {
				continue;
			}

			if (cf_serv_cfg_add_sock_cfg(published_cfg, &sock_cfg)) {
				CRASH("error initializing published address list");
			}
		}
	}
}

/**
 * Checks if the heartbeat binding configuration is valid.
 * @param error pointer to a static error message if validation fails, else will
 * be set to NULL.
 */
static bool
config_binding_is_valid(char** error, as_hb_protocol protocol)
{
	const cf_serv_cfg* bind_cfg = config_bind_cfg_get();
	const cf_mserv_cfg* multicast_group_cfg = config_multicast_group_cfg_get();

	if (hb_is_mesh()) {
		if (bind_cfg->n_cfgs == 0) {
			// Should not happen in practice.
			*error = "no bind addresses found for heartbeat";
			return false;
		}

		// Ensure we have a valid port for all bind endpoints.
		for (int i = 0; i < bind_cfg->n_cfgs; i++) {
			if (bind_cfg->cfgs[i].port == 0) {
				*error = "invalid mesh listening port";
				return false;
			}
		}

		cf_serv_cfg publish_serv_cfg;
		cf_serv_cfg_init(&publish_serv_cfg);

		if (multicast_group_cfg->n_cfgs != 0) {
			*error =
					"invalid config option: multicast-group not supported in mesh mode";
			return false;
		}
	}
	else {
		const cf_mserv_cfg* multicast_group_cfg =
				config_multicast_group_cfg_get();

		if (multicast_group_cfg->n_cfgs == 0) {
			*error = "no multicast groups specified";
			return false;
		}

		// Ensure multicast groups have valid ports.
		// TODO: We could check if the address is valid multicast.
		for (int i = 0; i < multicast_group_cfg->n_cfgs; i++) {
			if (multicast_group_cfg->cfgs[i].port == 0) {
				*error = "invalid multicast port";
				return false;
			}
		}

		if (g_config.hb_config.mesh_seed_addrs[0]) {
			*error =
					"invalid config option: mesh-seed-address-port not supported for multicast mode";
			return false;
		}

		cf_serv_cfg publish_serv_cfg;
		cf_serv_cfg_init(&publish_serv_cfg);
	}

	*error = NULL;
	return true;
}

/*
 * ----------------------------------------------------------------------------
 * Channel sub module.
 * ----------------------------------------------------------------------------
 */

/**
 * Initialize the channel structure.
 */
static void
channel_init_channel(as_hb_channel* channel)
{
	memset(channel, 0, sizeof(as_hb_channel));
	cf_ip_addr_set_any(&channel->endpoint_addr.addr);
}

/**
 * Initialize the channel event structure.
 */
static void
channel_event_init(as_hb_channel_event* event)
{
	memset(event, 0, sizeof(as_hb_channel_event));
}

/**
 * Is channel running.
 */
static bool
channel_is_running()
{
	CHANNEL_LOCK();
	bool retval =
			(g_hb.channel_state.status == AS_HB_STATUS_RUNNING) ? true : false;
	CHANNEL_UNLOCK();
	return retval;
}

/**
 * Is channel stopped.
 */
static bool
channel_is_stopped()
{
	CHANNEL_LOCK();
	bool retval =
			(g_hb.channel_state.status == AS_HB_STATUS_STOPPED) ? true : false;
	CHANNEL_UNLOCK();
	return retval;
}

/**
 * Keep a winning socket as a winner for at least this amount of time to prevent
 * constant flip flopping and give the winning socket a chance to send
 * heartbeats.
 */
static uint32_t
channel_win_grace_ms()
{
	return 3 * config_tx_interval_get();
}

/**
 * Enable / disable events.
 */
static void
channel_events_enabled_set(bool enabled)
{
	CHANNEL_LOCK();
	g_hb.channel_state.events_enabled = enabled;
	CHANNEL_UNLOCK();
}

/**
 * Know if events are enabled.
 */
static bool
channel_are_events_enabled()
{
	bool result;
	CHANNEL_LOCK();
	result = g_hb.channel_state.events_enabled;
	CHANNEL_UNLOCK();
	return result;
}

/**
 * Discard an event that has been processed.
 */
static void
channel_event_discard(as_hb_channel_event* event)
{
	// Free the message structure for message received events.
	if (event->type == AS_HB_CHANNEL_MSG_RECEIVED) {
		hb_msg_return(event->msg);
	}
}

/**
 * Queues a channel event for publishing by the channel tender.
 */
static void
channel_event_queue(as_hb_channel_event* event)
{
	if (!channel_are_events_enabled()) {
		channel_event_discard(event);
		DETAIL(
				"events disabled. Ignoring event of type %d with nodeid %" PRIx64,
				event->type, event->nodeid);
		return;
	}

	DETAIL("queuing channel event of type %d for node %" PRIx64, event->type,
			event->nodeid);
	cf_queue_push(&g_hb.channel_state.events_queue, event);
}

/**
 * Publish queued up channel events. Should be called outside a channel lock to
 * prevent deadlocks.
 */
static void
channel_event_publish_pending()
{
	// No channel lock here to prevent deadlocks.
	as_hb_channel_event event;
	while (cf_queue_pop(&g_hb.channel_state.events_queue, &event, 0)
			== CF_QUEUE_OK) {
		// Nothing elaborate, using hardcoded list of event recipients.
		mesh_channel_event_process(&event);
		hb_channel_event_process(&event);

		channel_event_discard(&event);
	}
}

/**
 * Return the endpoint associated with this socket if it exists.
 *
 * @param socket the socket to query for.
 * @param result the output result.
 * @return 0 if the socket was found and the result value is filled. -1 if a
 * mapping for the socket could not be found.
 */
static int
channel_get_channel(cf_socket* socket, as_hb_channel* result)
{
	int status;
	CHANNEL_LOCK();

	if (cf_shash_get(g_hb.channel_state.socket_to_channel, &socket, result)
			== CF_SHASH_OK) {
		status = 0;
	}
	else {
		status = -1;
	}

	CHANNEL_UNLOCK();
	return status;
}

/**
 * Shutdown a channel socket without closing, forcing the channel tender to
 * cleanup associated data structures.
 */
static void
channel_socket_shutdown(cf_socket* socket)
{
	cf_socket_shutdown(socket);
}

/**
 * Return the socket associated with this node.
 * Returns 0 on success and -1 if there is no socket attached to this node.
 */
static int
channel_socket_get(cf_node nodeid, cf_socket** socket)
{
	int rv = -1;
	CHANNEL_LOCK();
	if (cf_shash_get(g_hb.channel_state.nodeid_to_socket, &nodeid, socket)
			== CF_SHASH_ERR_NOT_FOUND) {
		rv = -1;
	}
	else {
		rv = 0;
	}

	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Indicate if a socket is present in a sockets list.
 */
static bool
channel_cf_sockets_contains(cf_sockets* sockets, cf_socket* to_find)
{
	for (int i = 0; i < sockets->n_socks; i++) {
		if (&sockets->socks[i] == to_find) {
			return true;
		}
	}

	return false;
}

/**
 * Destroy an allocated socket.
 */
static void
channel_socket_destroy(cf_socket* sock)
{
	cf_socket_close(sock);
	cf_socket_term(sock);
	cf_free(sock);
}

/**
 * Close a channel socket. Precondition is that the socket is registered with
 * the channel module using channel_socket_register.
 */
static void
channel_socket_close(cf_socket* socket, bool remote_close,
		bool raise_close_event)
{
	if (remote_close) {
		DEBUG("remote close: fd %d event", CSFD(socket));
	}

	CHANNEL_LOCK();

	if (channel_cf_sockets_contains(g_hb.channel_state.listening_sockets,
			socket)) {
		// Listening sockets will be closed by the mode (mesh/multicast
		// ) modules.
		goto Exit;
	}

	// Clean up data structures.
	as_hb_channel channel;
	int status = channel_get_channel(socket, &channel);

	if (status == 0) {
		if (channel.nodeid != 0) {
			cf_socket* node_socket;
			if (channel_socket_get(channel.nodeid, &node_socket) == 0
					&& node_socket == socket) {
				// Remove associated node for this socket.
				cf_shash_delete(g_hb.channel_state.nodeid_to_socket,
						&channel.nodeid);

				if (!channel.is_multicast && raise_close_event) {
					as_hb_channel_event event;
					channel_event_init(&event);

					// Notify others that this node is no longer connected.
					event.type = AS_HB_CHANNEL_NODE_DISCONNECTED;
					event.nodeid = channel.nodeid;
					event.msg = NULL;

					channel_event_queue(&event);
				}
			}
		}

		DETAIL("removed channel associated with fd %d polarity %s Type: %s",
				CSFD(socket), channel.is_inbound ? "inbound" : "outbound",
				channel.is_multicast ? "multicast" : "mesh");
		// Remove associated channel.
		cf_shash_delete(g_hb.channel_state.socket_to_channel, &socket);
	}
	else {
		// Will only happen if we are closing this socket twice. Cannot
		// deference the underlying fd because the socket has been freed.
		WARNING("found a socket %p without an associated channel", socket);
		goto Exit;
	}

	static int32_t err_ok[] = { ENOENT, EBADF, EPERM };
	int32_t err = cf_poll_delete_socket_forgiving(g_hb.channel_state.poll,
			socket, sizeof(err_ok) / sizeof(int32_t), err_ok);

	if (err == ENOENT) {
		// There is no valid code path where epoll ctl should fail.
		CRASH("unable to remove fd %d from epoll fd list: %s", CSFD(socket),
				cf_strerror(errno));
		goto Exit;
	}

	cf_atomic_int_incr(&g_stats.heartbeat_connections_closed);
	DEBUG("closing channel with fd %d", CSFD(socket));

	channel_socket_destroy(socket);

Exit:
	CHANNEL_UNLOCK();
}

/**
 * Close multiple sockets. Should be invoked only by channel stop.
 * @param sockets the vector consisting of sockets to be closed.
 */
static void
channel_sockets_close(cf_vector* sockets)
{
	uint32_t socket_count = cf_vector_size(sockets);
	for (int index = 0; index < socket_count; index++) {
		cf_socket* socket;
		if (cf_vector_get(sockets, index, &socket) != 0) {
			WARNING("error finding the fd %d to be deleted", CSFD(socket));
			continue;
		}
		channel_socket_close(socket, false, true);
	}
}

/**
 * Queues a socket for closing by the channel tender. Should be used by all code
 * paths other than the channel stop code path.
 */
static void
channel_socket_close_queue(cf_socket* socket, bool is_remote_close,
		bool raise_close_event)
{
	as_hb_channel_socket_close_entry close_entry = {
		socket,
		is_remote_close,
		raise_close_event };
	DETAIL("queuing close of fd %d", CSFD(socket));
	cf_queue_push(&g_hb.channel_state.socket_close_queue, &close_entry);
}

/**
 * Close queued up sockets.
 */
static void
channel_socket_close_pending()
{
	// No channel lock required here.
	as_hb_channel_socket_close_entry close_entry;
	while (cf_queue_pop(&g_hb.channel_state.socket_close_queue, &close_entry, 0)
			== CF_QUEUE_OK) {
		channel_socket_close(close_entry.socket, close_entry.is_remote,
				close_entry.raise_close_event);
	}
}

/**
 * Register a new socket.
 *
 * @param socket the socket.
 * @param is_multicast indicates if this socket is a multicast socket.
 * @param is_inbound indicates if this socket is an inbound / outbound.
 * @param endpoint peer endpoint this socket connects to. Will be NULL for
 * inbound sockets.
 */
static void
channel_socket_register(cf_socket* socket, bool is_multicast, bool is_inbound,
		cf_sock_addr* endpoint_addr)
{
	CHANNEL_LOCK();

	as_hb_channel channel;
	channel_init_channel(&channel);

	// This socket should not be part of the socket to channel map.
	ASSERT(channel_get_channel(socket, &channel) == -1,
			"error the channel already exists for fd %d", CSFD(socket));

	channel.is_multicast = is_multicast;
	channel.is_inbound = is_inbound;
	channel.last_received = cf_getms();

	if (endpoint_addr) {
		memcpy(&channel.endpoint_addr, endpoint_addr, sizeof(*endpoint_addr));
	}

	// Add socket to poll list
	cf_poll_add_socket(g_hb.channel_state.poll, socket,
			EPOLLIN | EPOLLERR | EPOLLRDHUP, socket);

	cf_shash_put(g_hb.channel_state.socket_to_channel, &socket, &channel);

	DEBUG("channel created for fd %d - polarity %s type: %s", CSFD(socket),
			channel.is_inbound ? "inbound" : "outbound",
			channel.is_multicast ? "multicast" : "mesh");

	CHANNEL_UNLOCK();
}

/**
 * Accept an incoming tcp connection. For now this is relevant only to the mesh
 * mode.
 * @param lsock the listening socket that received the connection.
 */
static void
channel_accept_connection(cf_socket* lsock)
{
	if (!hb_is_mesh()) {
		// We do not accept connections in non mesh modes.
		return;
	}

	cf_socket csock;
	cf_sock_addr caddr;

	if (cf_socket_accept(lsock, &csock, &caddr) < 0) {
		if ((errno == EMFILE) || (errno == ENFILE) || (errno == ENOMEM)
				|| (errno == ENOBUFS)) {
			TICKER_WARNING(
					"failed to accept heartbeat connection due to error : %s",
					cf_strerror(errno));
			// We are in an extreme situation where we ran out of system
			// resources (file/mem). We should rather lie low and not do too
			// much activity. So, sleep. We should not sleep too long as this
			// same function is supposed to send heartbeat also.
			usleep(MAX(AS_HB_TX_INTERVAL_MS_MIN, 1) * 1000);
			return;
		}
		else {
			// TODO: Find what there errors are.
			WARNING("accept failed: %s", cf_strerror(errno));
			return;
		}
	}

	// Update the stats to reflect to a new connection opened.
	cf_atomic_int_incr(&g_stats.heartbeat_connections_opened);

	char caddr_str[DNS_NAME_MAX_SIZE];
	cf_sock_addr_to_string_safe(&caddr, caddr_str, sizeof(caddr_str));
	DEBUG("new connection from %s", caddr_str);

	cf_sock_cfg *cfg = lsock->cfg;

	if (cfg->owner == CF_SOCK_OWNER_HEARTBEAT_TLS) {
		tls_socket_prepare_server(g_config.hb_config.tls, &csock);

		if (tls_socket_accept_block(&csock) != 1) {
			WARNING("heartbeat TLS server handshake with %s failed", caddr_str);
			cf_socket_close(&csock);
			cf_socket_term(&csock);

			cf_atomic_int_incr(&g_stats.heartbeat_connections_closed);
			return;
		}
	}

	// Allocate a new socket.
	cf_socket* sock = cf_malloc(sizeof(cf_socket));
	cf_socket_init(sock);
	cf_socket_copy(&csock, sock);

	// Register this socket with the channel subsystem.
	channel_socket_register(sock, false, true, NULL);
}

/**
 * Parse compressed buffer into a message.
 *
 * @param msg the input parsed compressed message and also the output heartbeat
 * message.
 * @param buffer the input buffer.
 * @param buffer_content_len the length of the content in the buffer.
 * @return the status of parsing the message.
 */
static as_hb_channel_msg_read_status
channel_compressed_message_parse(msg* msg, void* buffer, int buffer_content_len)
{
	// This is a direct pointer inside the buffer parameter. No allocation
	// required.
	uint8_t* compressed_buffer = NULL;
	size_t compressed_buffer_length = 0;
	int parsed = AS_HB_CHANNEL_MSG_PARSE_FAIL;
	void* uncompressed_buffer = NULL;
	size_t uncompressed_buffer_length = 0;

	if (msg_get_buf(msg, AS_HB_MSG_COMPRESSED_PAYLOAD, &compressed_buffer,
			&compressed_buffer_length, MSG_GET_DIRECT) != 0) {
		parsed = AS_HB_CHANNEL_MSG_PARSE_FAIL;
		goto Exit;
	}

	// Assume compression ratio of 3. We will expand the buffer if needed.
	uncompressed_buffer_length = round_up_pow2(3 * compressed_buffer_length);

	// Keep trying till we allocate enough memory for the uncompressed buffer.
	while (true) {
		uncompressed_buffer = MSG_BUFF_ALLOC_OR_DIE(uncompressed_buffer_length,
				"error allocating memory size %zu for decompressing message",
				uncompressed_buffer_length);

		int uncompress_rv = uncompress(uncompressed_buffer,
				&uncompressed_buffer_length, compressed_buffer,
				compressed_buffer_length);

		if (uncompress_rv == Z_OK) {
			// Decompression was successful.
			break;
		}

		if (uncompress_rv == Z_BUF_ERROR) {
			// The uncompressed buffer is not large enough. Free current buffer
			// and allocate a new buffer.
			MSG_BUFF_FREE(uncompressed_buffer, uncompressed_buffer_length);

			// Give uncompressed buffer more space.
			uncompressed_buffer_length *= 2;
			continue;
		}

		// Decompression failed. Clean up and exit.
		parsed = AS_HB_CHANNEL_MSG_PARSE_FAIL;
		goto Exit;
	}

	// Reset the message to prepare for parsing the uncompressed buffer. We have
	// no issues losing the compressed buffer because we have an uncompressed
	// copy.
	msg_reset(msg);

	// Parse the uncompressed buffer.
	parsed =
			msg_parse(msg, uncompressed_buffer, uncompressed_buffer_length) ?
					AS_HB_CHANNEL_MSG_READ_SUCCESS :
					AS_HB_CHANNEL_MSG_PARSE_FAIL;

	if (parsed == AS_HB_CHANNEL_MSG_READ_SUCCESS) {
		// Copying the buffer content to ensure that the message and the buffer
		// can have separate life cycles and we never get into races. The
		// frequency of heartbeat messages is low enough to make this not matter
		// much unless we have massive clusters.
		msg_preserve_all_fields(msg);
	}

Exit:
	MSG_BUFF_FREE(uncompressed_buffer, uncompressed_buffer_length);
	return parsed;
}

/**
 * Parse the buffer into a message.
 *
 * @param msg the output heartbeat message.
 * @param buffer the input buffer.
 * @param buffer_content_len the length of the content in the buffer.
 * @return the status of parsing the message.
 */
static as_hb_channel_msg_read_status
channel_message_parse(msg* msg, void* buffer, int buffer_content_len)
{
	// Peek into the buffer to get hold of the message type.
	msg_type type = 0;
	uint32_t msg_size = 0;
	if (! msg_parse_hdr(&msg_size, &type, (uint8_t*)buffer, buffer_content_len)
			|| type != msg->type) {
		// Pre check because msg_parse considers this a warning but this would
		// be common when protocol version between nodes do not match.
		DEBUG("message type mismatch - expected:%d received:%d", msg->type,
				type);
		return AS_HB_CHANNEL_MSG_PARSE_FAIL;
	}

	bool parsed = msg_parse(msg, buffer, buffer_content_len);

	if (parsed) {
		if (msg_is_set(msg, AS_HB_MSG_COMPRESSED_PAYLOAD)) {
			// This is a compressed message.
			return channel_compressed_message_parse(msg, buffer,
					buffer_content_len);
		}

		// This is an uncompressed message. Copying the buffer content to ensure
		// that the message and the buffer can have separate life cycles and we
		// never get into races. The frequency of heartbeat messages is low
		// enough to make this not matter much unless we have massive clusters.
		msg_preserve_all_fields(msg);
	}

	return parsed ?
			AS_HB_CHANNEL_MSG_READ_SUCCESS : AS_HB_CHANNEL_MSG_PARSE_FAIL;
}

/**
 * Iterate over a endpoint list and see if there is a matching socket address.
 */
static void
channel_endpoint_find_iterate_fn(const as_endpoint* endpoint, void* udata)
{
	cf_sock_addr sock_addr;
	as_hb_channel_endpoint_iterate_udata* iterate_data =
			(as_hb_channel_endpoint_iterate_udata*)udata;
	if (as_endpoint_to_sock_addr(endpoint, &sock_addr) != 0) {
		return;
	}

	if (cf_sock_addr_is_any(&sock_addr)) {
		return;
	}

	iterate_data->found = iterate_data->found
			|| (cf_sock_addr_compare(&sock_addr, iterate_data->addr_to_search)
					== 0);
}

/**
 * Reduce function to find a matching endpoint.
 */
static int
channel_endpoint_search_reduce(const void* key, void* data, void* udata)
{
	cf_socket** socket = (cf_socket**)key;
	as_hb_channel* channel = (as_hb_channel*)data;
	as_hb_channel_endpoint_reduce_udata* endpoint_reduce_udata =
			(as_hb_channel_endpoint_reduce_udata*)udata;

	as_hb_channel_endpoint_iterate_udata iterate_udata;
	iterate_udata.addr_to_search = &channel->endpoint_addr;
	iterate_udata.found = false;

	as_endpoint_list_iterate(endpoint_reduce_udata->endpoint_list,
			channel_endpoint_find_iterate_fn, &iterate_udata);

	if (iterate_udata.found) {
		endpoint_reduce_udata->found = true;
		endpoint_reduce_udata->socket = *socket;
		// Stop the reduce, we have found a match.
		return CF_SHASH_ERR_FOUND;
	}

	return CF_SHASH_OK;
}

/**
 * Indicates if any endpoint from the input endpoint list is already connected.
 * @param endpoint_list the endpoint list to check.
 * @return true if at least one endpoint is already connected to, false
 * otherwise.
 */
static bool
channel_endpoint_is_connected(as_endpoint_list* endpoint_list)
{
	CHANNEL_LOCK();
	// Linear search. This will in practice not be a very frequent operation.
	as_hb_channel_endpoint_reduce_udata udata;
	memset(&udata, 0, sizeof(udata));
	udata.endpoint_list = endpoint_list;

	cf_shash_reduce(g_hb.channel_state.socket_to_channel,
			channel_endpoint_search_reduce, &udata);

	CHANNEL_UNLOCK();
	return udata.found;
}

/**
 * Read a message from the multicast socket.
 *
 * @param socket the multicast socket to read from.
 * @param msg the message to read into.
 *
 * @return the status the read operation.
 */
static as_hb_channel_msg_read_status
channel_multicast_msg_read(cf_socket* socket, msg* msg)
{
	CHANNEL_LOCK();

	as_hb_channel_msg_read_status rv = AS_HB_CHANNEL_MSG_READ_UNDEF;

	int buffer_len = MAX(hb_mtu(), STACK_ALLOC_LIMIT);
	uint8_t* buffer = MSG_BUFF_ALLOC(buffer_len);

	if (!buffer) {
		WARNING(
				"error allocating space for multicast recv buffer of size %d on fd %d",
				buffer_len, CSFD(socket));
		goto Exit;
	}

	cf_sock_addr from;

	int num_rcvd = cf_socket_recv_from(socket, buffer, buffer_len, 0, &from);

	if (num_rcvd <= 0) {
		DEBUG("multicast packed read failed on fd %d", CSFD(socket));
		rv = AS_HB_CHANNEL_MSG_CHANNEL_FAIL;
		goto Exit;
	}

	rv = channel_message_parse(msg, buffer, num_rcvd);
	if (rv != AS_HB_CHANNEL_MSG_READ_SUCCESS) {
		goto Exit;
	}

	rv = AS_HB_CHANNEL_MSG_READ_SUCCESS;

Exit:
	MSG_BUFF_FREE(buffer, buffer_len);

	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Read a message from the a tcp mesh socket.
 *
 * @param socket the tcp socket to read from.
 * @param msg the message to read into.
 *
 * @return status of the read operation.
 */
static as_hb_channel_msg_read_status
channel_mesh_msg_read(cf_socket* socket, msg* msg)
{
	CHANNEL_LOCK();

	uint32_t buffer_len = 0;
	uint8_t* buffer = NULL;

	as_hb_channel_msg_read_status rv = AS_HB_CHANNEL_MSG_READ_UNDEF;
	uint8_t len_buff[MSG_WIRE_LENGTH_SIZE];

	if (cf_socket_recv_all(socket, len_buff, MSG_WIRE_LENGTH_SIZE, 0,
	MESH_RW_TIMEOUT) < 0) {
		WARNING("mesh size recv failed fd %d : %s", CSFD(socket),
				cf_strerror(errno));
		rv = AS_HB_CHANNEL_MSG_CHANNEL_FAIL;
		goto Exit;
	}

	buffer_len = ntohl(*((uint32_t*)len_buff)) + 6;

	buffer = MSG_BUFF_ALLOC(buffer_len);

	if (!buffer) {
		WARNING(
				"error allocating space for mesh recv buffer of size %d on fd %d",
				buffer_len, CSFD(socket));
		goto Exit;
	}

	memcpy(buffer, len_buff, MSG_WIRE_LENGTH_SIZE);

	if (cf_socket_recv_all(socket, buffer + MSG_WIRE_LENGTH_SIZE,
			buffer_len - MSG_WIRE_LENGTH_SIZE, 0, MESH_RW_TIMEOUT) < 0) {
		DETAIL("mesh recv failed fd %d : %s", CSFD(socket), cf_strerror(errno));
		rv = AS_HB_CHANNEL_MSG_CHANNEL_FAIL;
		goto Exit;
	}

	DETAIL("mesh recv success fd %d message size %d", CSFD(socket), buffer_len);

	rv = channel_message_parse(msg, buffer, buffer_len);

Exit:
	MSG_BUFF_FREE(buffer, buffer_len);

	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Associate a socket with a nodeid and notify listeners about a node being
 * connected, effective only for mesh channels.
 *
 * For multicast channels this function is a no-op. The reason being additional
 * machinery would be required to clean up the node to channel mapping on node
 * expiry.
 *
 * @param socket the socket.
 * @param channel the channel to associate.
 * @param nodeid the nodeid associated with this socket.
 */
static void
channel_node_attach(cf_socket* socket, as_hb_channel* channel, cf_node nodeid)
{
	// For now node to socket mapping is not maintained for multicast channels.
	if (channel->is_multicast) {
		return;
	}

	CHANNEL_LOCK();

	// Update the node information for the channel.
	// This is the first time this node has a connection. Record the mapping.
	cf_shash_put(g_hb.channel_state.nodeid_to_socket, &nodeid, &socket);

	channel->nodeid = nodeid;
	cf_shash_put(g_hb.channel_state.socket_to_channel, &socket, channel);

	DEBUG("attached fd %d to node %" PRIx64, CSFD(socket), nodeid);

	CHANNEL_UNLOCK();

	// Publish an event to let know that a new node has a channel now.
	as_hb_channel_event node_connected_event;
	channel_event_init(&node_connected_event);
	node_connected_event.nodeid = nodeid;
	node_connected_event.type = AS_HB_CHANNEL_NODE_CONNECTED;
	channel_event_queue(&node_connected_event);
}

/**
 * Indicates if a channel should be allowed to continue to win and live because
 * of a winning grace period.
 */
static bool
channel_socket_should_live(cf_socket* socket, as_hb_channel* channel)
{
	if (channel->resolution_win_ts > 0
			&& channel->resolution_win_ts + channel_win_grace_ms()
					> cf_getms()) {
		// Losing socket was a previous winner. Allow it time to do some work
		// before knocking it off.
		INFO("giving %d unresolved fd some grace time", CSFD(socket));
		return true;
	}
	return false;
}

/**
 * Selects one out give two sockets connected to same remote node. The algorithm
 * is deterministic and ensures the remote node also chooses a socket that drops
 * the same connection.
 *
 * @param socket1 one of the sockets
 * @param socket2 one of the sockets
 * @return resolved socket on success, NULL if resolution fails.
 */
static cf_socket*
channel_socket_resolve(cf_socket* socket1, cf_socket* socket2)
{
	cf_socket* rv = NULL;
	CHANNEL_LOCK();

	DEBUG("resolving between fd %d and %d", CSFD(socket1), CSFD(socket2));

	as_hb_channel channel1;
	if (channel_get_channel(socket1, &channel1) < 0) {
		// Should not happen in practice.
		WARNING("resolving fd %d without channel", CSFD(socket1));
		rv = socket2;
		goto Exit;
	}

	as_hb_channel channel2;
	if (channel_get_channel(socket2, &channel2) < 0) {
		// Should not happen in practice.
		WARNING("resolving fd %d without channel", CSFD(socket2));
		rv = socket1;
		goto Exit;
	}

	if (channel_socket_should_live(socket1, &channel1)) {
		rv = socket1;
		goto Exit;
	}

	if (channel_socket_should_live(socket2, &channel2)) {
		rv = socket2;
		goto Exit;
	}

	cf_node remote_nodeid =
			channel1.nodeid != 0 ? channel1.nodeid : channel2.nodeid;

	if (remote_nodeid == 0) {
		// Should not happen in practice.
		WARNING("remote node id unknown for fds %d and %d", CSFD(socket1),
				CSFD(socket2));
		rv = NULL;
		goto Exit;
	}

	// Choose the socket with the highest acceptor nodeid.
	cf_node acceptor_nodeid1 =
			channel1.is_inbound ? config_self_nodeid_get() : remote_nodeid;
	cf_node acceptor_nodeid2 =
			channel2.is_inbound ? config_self_nodeid_get() : remote_nodeid;

	as_hb_channel* winner_channel = NULL;
	cf_socket* winner_socket = NULL;
	if (acceptor_nodeid1 > acceptor_nodeid2) {
		winner_channel = &channel1;
		winner_socket = socket1;
	}
	else if (acceptor_nodeid1 < acceptor_nodeid2) {
		winner_channel = &channel2;
		winner_socket = socket2;
	}
	else {
		// Both connections have the same acceptor. Should not happen in
		// practice. Despair and report resolution failure.
		INFO(
				"found redundant connections to same node, fds %d %d - choosing at random",
				CSFD(socket1), CSFD(socket2));

		if (cf_getms() % 2 == 0) {
			winner_channel = &channel1;
			winner_socket = socket1;
		}
		else {
			winner_channel = &channel2;
			winner_socket = socket2;
		}
	}

	cf_clock now = cf_getms();
	if (winner_channel->resolution_win_ts == 0) {
		winner_channel->resolution_win_ts = now;
		// Update the winning count of the winning channel in the channel data
		// structures.
		cf_shash_put(g_hb.channel_state.socket_to_channel, &winner_socket,
				winner_channel);
	}

	if (winner_channel->resolution_win_ts > now + channel_win_grace_ms()) {
		// The winner has been winning a lot, most likely the other side has us
		// with a seed address different from our published address.
		//
		// Break the cycle here and choose the loosing channel as the winner.
		INFO("breaking socket resolve loop dropping winning fd %d",
				CSFD(winner_socket));
		winner_channel = (winner_channel == &channel1) ? &channel2 : &channel1;
		winner_socket = (socket1 == winner_socket) ? socket2 : socket1;
	}

	rv = winner_socket;

Exit:
	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Basic sanity check for a message.
 * @param msg_event the message event.
 * @return 0 if the message passes basic sanity tests. -1 on failure.
 */
static int
channel_msg_sanity_check(as_hb_channel_event* msg_event)
{
	msg* msg = msg_event->msg;
	uint32_t id = 0;

	as_hb_msg_type type = 0;
	cf_node src_nodeid = 0;

	int rv = 0;

	if (msg_nodeid_get(msg, &src_nodeid) != 0) {
		TICKER_WARNING("received message without a source node");
		rv = -1;
	}

	// Validate the fact that we have a valid source nodeid.
	if (src_nodeid == 0) {
		// Event nodeid is zero. Not a valid source nodeid. This will happen in
		// compatibility mode if the info request from a new node arrives before
		// the pulse message. Can be ignored.
		TICKER_WARNING("received a message from node with unknown nodeid");
		rv = -1;
	}

	if (msg_id_get(msg, &id) != 0) {
		TICKER_WARNING(
				"received message without heartbeat protocol identifier from node %" PRIx64,
				src_nodeid);
		rv = -1;
	}
	else {
		DETAIL(
				"received message with heartbeat protocol identifier %d from node %" PRIx64,
				id, src_nodeid);

		// Ignore the message if the protocol of the incoming message does not
		// match.
		if (id != hb_protocol_identifier_get()) {
			TICKER_WARNING(
					"received message with different heartbeat protocol identifier from node %" PRIx64,
					src_nodeid);
			rv = -1;
		}
	}

	if (msg_type_get(msg, &type) != 0) {
		TICKER_WARNING(
				"received message without message type from node %" PRIx64,
				src_nodeid);
		rv = -1;
	}

	as_endpoint_list* endpoint_list;
	if (hb_is_mesh()) {
		// Check only applies to v3 mesh.
		// v3 multicast protocol does not advertise endpoint list.
		if (msg_endpoint_list_get(msg, &endpoint_list) != 0
				|| endpoint_list->n_endpoints <= 0) {
			TICKER_WARNING(
					"received message without address/port from node %" PRIx64,
					src_nodeid);
			rv = -1;
		}
	}

	as_hlc_timestamp send_ts;
	if (msg_send_hlc_ts_get(msg, &send_ts) != 0) {
		TICKER_WARNING("received message without HLC time from node %" PRIx64,
				src_nodeid);
		rv = -1;
	}

	if (type == AS_HB_MSG_TYPE_PULSE) {
		char* remote_cluster_name = NULL;
		if (msg_cluster_name_get(msg, &remote_cluster_name) != 0) {
			remote_cluster_name = "";
		}

		if (!as_config_cluster_name_matches(remote_cluster_name)) {
			// Generate cluster-name mismatch event.
			as_hb_channel_event mismatch_event;
			channel_event_init(&mismatch_event);

			// Notify hb about cluster-name mismatch.
			mismatch_event.type = AS_HB_CHANNEL_CLUSTER_NAME_MISMATCH;
			mismatch_event.nodeid = src_nodeid;
			mismatch_event.msg = NULL;
			memcpy(&mismatch_event.msg_hlc_ts, &msg_event->msg_hlc_ts,
					sizeof(msg_event->msg_hlc_ts));

			channel_event_queue(&mismatch_event);

			TICKER_WARNING("ignoring message from %"PRIX64" with different cluster name(%s)",
					src_nodeid, remote_cluster_name[0] == '\0' ? "null" : remote_cluster_name );
			rv = -1;
		}
	}

	DETAIL("received message of type %d from node %" PRIx64, type, src_nodeid);

	return rv;
}

/**
 * Process incoming message to possibly update channel state.
 *
 * @param socket the socket on which the message is received.
 * @param event the message wrapped around in a channel event.
 * @return 0 if the message can be further processed, -1 if the message should
 * be discarded.
 */
static int
channel_msg_event_process(cf_socket* socket, as_hb_channel_event* event)
{
	// Basic sanity check for the inbound message.
	if (channel_msg_sanity_check(event) != 0) {
		DETAIL("sanity check failed for message on fd %d", CSFD(socket));
		return -1;
	}

	int rv = -1;
	CHANNEL_LOCK();

	as_hb_channel channel;
	if (channel_get_channel(socket, &channel) < 0) {
		// This is a bug and should not happen. Be paranoid and try fixing it ?
		WARNING("received a message on an unregistered fd %d - closing the fd",
				CSFD(socket));
		channel_socket_close_queue(socket, false, true);
		rv = -1;
		goto Exit;
	}

	if (channel.is_multicast) {
		rv = 0;
		goto Exit;
	}

	cf_node nodeid = event->nodeid;

	if (channel.nodeid != 0 && channel.nodeid != nodeid) {
		// The event nodeid does not match previously know event id. Something
		// seriously wrong here.
		WARNING("received a message from node with incorrect nodeid - expected %" PRIx64 " received %" PRIx64 "on fd %d",
				channel.nodeid, nodeid, CSFD(socket));
		rv = -1;
		goto Exit;
	}

	// Update the last received time for this node
	channel.last_received = cf_getms();

	cf_shash_put(g_hb.channel_state.socket_to_channel, &socket, &channel);

	cf_socket* existing_socket;
	int get_result = cf_shash_get(g_hb.channel_state.nodeid_to_socket, &nodeid,
			&existing_socket);

	if (get_result == CF_SHASH_ERR_NOT_FOUND) {
		// Associate this socket with the node.
		channel_node_attach(socket, &channel, nodeid);
	}
	else if (existing_socket != socket) {
		// Somehow the other node and this node discovered each other together
		// both connected via two tcp connections. Choose one and close the
		// other.
		cf_socket* resolved = channel_socket_resolve(socket, existing_socket);

		if (!resolved) {
			DEBUG(
					"resolving between fd %d and %d failed - closing both connections",
					CSFD(socket), CSFD(existing_socket));

			// Resolution failed. Should not happen but there is a window where
			// the same node initiated two connections.
			// Close both connections and try again.
			channel_socket_close_queue(socket, false, true);
			channel_socket_close_queue(existing_socket, false, true);

			// Nothing wrong with the message. Let it through.
			rv = 0;
			goto Exit;
		}

		DEBUG("resolved fd %d between redundant fd %d and %d for node %" PRIx64,
				CSFD(resolved), CSFD(socket), CSFD(existing_socket), nodeid);

		if (resolved == existing_socket) {
			// The node to socket mapping is correct, just close this socket and
			// this node will  still be connected to the remote node. Do not
			// raise any event for this closure.
			channel_socket_close_queue(socket, false, false);
		}
		else {
			// We need to close the existing socket. Disable channel events
			// because we make the node appear to be not connected. Do not raise
			// any event for this closure.
			channel_socket_close_queue(existing_socket, false, false);
			// Associate this socket with the node.
			channel_node_attach(socket, &channel, nodeid);
		}
	}

	rv = 0;

Exit:
	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Read a message from a socket that has data.
 * @param socket the socket having data to be read.
 */
static void
channel_msg_read(cf_socket* socket)
{
	CHANNEL_LOCK();

	as_hb_channel_msg_read_status status;
	as_hb_channel channel;

	bool free_msg = true;

	msg* msg = hb_msg_get();

	if (channel_get_channel(socket, &channel) != 0) {
		// Would happen if the channel was closed in the same epoll loop.
		DEBUG("error the channel does not exist for fd %d", CSFD(socket));
		goto Exit;
	}

	if (channel.is_multicast) {
		status = channel_multicast_msg_read(socket, msg);
	}
	else {
		status = channel_mesh_msg_read(socket, msg);
	}

	switch (status) {
	case AS_HB_CHANNEL_MSG_READ_SUCCESS: {
		break;
	}

	case AS_HB_CHANNEL_MSG_PARSE_FAIL: {
		TICKER_WARNING("unable to parse heartbeat message on fd %d",
				CSFD(socket));
		goto Exit;
	}

	case AS_HB_CHANNEL_MSG_CHANNEL_FAIL:	// Falling through
	default: {
		DEBUG("could not read message from fd %d", CSFD(socket));
		if (!channel.is_multicast) {
			// Shut down only mesh socket.
			channel_socket_shutdown(socket);
		}
		goto Exit;
	}
	}

	as_hb_channel_event event;
	channel_event_init(&event);

	if (msg_get_uint64(msg, AS_HB_MSG_NODE, &event.nodeid) < 0) {
		// Node id missing from the message. Assume this message to be corrupt.
		TICKER_WARNING("message with invalid nodeid received on fd %d",
				CSFD(socket));
		goto Exit;
	}

	event.msg = msg;
	event.type = AS_HB_CHANNEL_MSG_RECEIVED;

	// Update hlc and store update message timestamp for the event.
	as_hlc_timestamp send_ts = 0;
	msg_send_hlc_ts_get(msg, &send_ts);
	as_hlc_timestamp_update(event.nodeid, send_ts, &event.msg_hlc_ts);

	// Process received message to update channel state.
	if (channel_msg_event_process(socket, &event) == 0) {
		// The message needs to be delivered to the listeners. Prevent a free.
		free_msg = false;
		channel_event_queue(&event);
	}

Exit:
	CHANNEL_UNLOCK();

	// release the message.
	if (free_msg) {
		hb_msg_return(msg);
	}
}

/**
 * Reduce function to remove faulty channels / nodes. Shutdown associated socket
 * to have channel tender cleanup.
 */
static int
channel_channels_tend_reduce(const void* key, void* data, void* udata)
{
	cf_socket** socket = (cf_socket**)key;
	as_hb_channel* channel = (as_hb_channel*)data;

	DETAIL("tending channel fd %d for node %" PRIx64 " - last received %" PRIu64 " endpoint %s",
			CSFD(*socket), channel->nodeid, channel->last_received,
			cf_sock_addr_print(&channel->endpoint_addr));

	if (channel->last_received + CHANNEL_NODE_READ_IDLE_TIMEOUT()
			< cf_getms()) {
		// Shutdown associated socket if it is not a multicast socket.
		if (!channel->is_multicast) {
			DEBUG("channel shutting down idle fd %d for node %" PRIx64 " - last received %" PRIu64 " endpoint %s",
					CSFD(*socket), channel->nodeid, channel->last_received,
					cf_sock_addr_print(&channel->endpoint_addr));
			channel_socket_shutdown(*socket);
		}
	}

	return CF_SHASH_OK;
}

/**
 * Tend channel specific node information to remove channels that are faulty (or
 * TODO: attached to misbehaving nodes).
 */
static void
channel_channels_idle_check()
{
	CHANNEL_LOCK();

	cf_clock now = cf_getms();
	if (g_hb.channel_state.last_channel_idle_check + CHANNEL_IDLE_CHECK_PERIOD
			<= now) {
		cf_shash_reduce(g_hb.channel_state.socket_to_channel,
				channel_channels_tend_reduce, NULL);
		g_hb.channel_state.last_channel_idle_check = now;
	}

	CHANNEL_UNLOCK();
}

/**
 * Socket tending thread. Manages heartbeat receive as well.
 */
void*
channel_tender(void* arg)
{
	DETAIL("channel tender started");

	while (channel_is_running()) {
		cf_poll_event events[POLL_SZ];
		int32_t nevents = cf_poll_wait(g_hb.channel_state.poll, events, POLL_SZ,
				AS_HB_TX_INTERVAL_MS_MIN);

		DETAIL("tending channel");

		for (int32_t i = 0; i < nevents; i++) {
			cf_socket* socket = events[i].data;
			if (channel_cf_sockets_contains(
					g_hb.channel_state.listening_sockets, socket)
					&& hb_is_mesh()) {
				// Accept a new connection.
				channel_accept_connection(socket);
			}
			else if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
				channel_socket_close_queue(socket, true, true);
			}
			else if (events[i].events & EPOLLIN) {
				// Read a message for the socket that is ready.
				channel_msg_read(socket);
			}
		}

		// Tend channels to discard stale channels.
		channel_channels_idle_check();

		// Close queued up socket.
		channel_socket_close_pending();

		// Publish pending events. Should be outside channel lock.
		channel_event_publish_pending();

		DETAIL("done tending channel");
	}

	DETAIL("channel tender shut down");
	return NULL;
}

/*
 * ----------------------------------------------------------------------------
 * Channel public API
 * ----------------------------------------------------------------------------
 */

/**
 * Filter out endpoints not matching this node's capabilities.
 */
static bool
channel_mesh_endpoint_filter(const as_endpoint* endpoint, void* udata)
{
	if ((cf_ip_addr_legacy_only())
			&& endpoint->addr_type == AS_ENDPOINT_ADDR_TYPE_IPv6) {
		return false;
	}

	// If we don't offer TLS, then we won't connect via TLS, either.
	if (g_config.hb_tls_serv_spec.bind_port == 0
			&& as_endpoint_capability_is_supported(endpoint,
					AS_ENDPOINT_TLS_MASK)) {
		return false;
	}

	return true;
}

/**
 * Try and connect to a set of endpoint_lists.
 */
static void
channel_mesh_channel_establish(as_endpoint_list** endpoint_lists,
		int endpoint_list_count)
{
	for (int i = 0; i < endpoint_list_count; i++) {
		char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
		as_endpoint_list_to_string(endpoint_lists[i], endpoint_list_str,
				sizeof(endpoint_list_str));

		if (channel_endpoint_is_connected(endpoint_lists[i])) {
			DEBUG(
					"duplicate endpoint connect request - ignoring endpoint list {%s}",
					endpoint_list_str);
			continue;
		}

		DEBUG("attempting to connect mesh host at {%s}", endpoint_list_str);

		cf_socket* sock = (cf_socket*)cf_malloc(sizeof(cf_socket));

		const as_endpoint* connected_endpoint = as_endpoint_connect_any(
				endpoint_lists[i], channel_mesh_endpoint_filter, NULL,
				CONNECT_TIMEOUT(), sock);

		if (connected_endpoint) {
			cf_atomic_int_incr(&g_stats.heartbeat_connections_opened);

			cf_sock_addr endpoint_addr;
			memset(&endpoint_addr, 0, sizeof(endpoint_addr));
			cf_ip_addr_set_any(&endpoint_addr.addr);
			if (as_endpoint_to_sock_addr(connected_endpoint, &endpoint_addr)
					!= 0) {
				// Should never happen in practice.
				WARNING("error converting endpoint to socket address");
				channel_socket_destroy(sock);
				sock = NULL;

				cf_atomic_int_incr(&g_stats.heartbeat_connections_closed);
				continue;
			}

			if (as_endpoint_capability_is_supported(connected_endpoint,
					AS_ENDPOINT_TLS_MASK)) {
				tls_socket_prepare_client(g_config.hb_config.tls, sock);

				if (tls_socket_connect_block(sock) != 1) {
					WARNING("heartbeat TLS client handshake with {%s} failed",
							endpoint_list_str);
					channel_socket_destroy(sock);
					sock = NULL;

					cf_atomic_int_incr(&g_stats.heartbeat_connections_closed);
					return;
				}
			}

			channel_socket_register(sock, false, false, &endpoint_addr);
		}
		else {
			TICKER_WARNING("could not create heartbeat connection to node {%s}",
					endpoint_list_str);
			if (sock) {
				cf_free(sock);
				sock = NULL;
			}
		}
	}
}

/**
 * Disconnect a node from the channel list.
 * @param nodeid the nodeid of the node whose channel should be disconnected.
 * @return 0 if the node had a channel and was disconnected. -1 otherwise.
 */
static int
channel_node_disconnect(cf_node nodeid)
{
	int rv = -1;

	CHANNEL_LOCK();

	cf_socket* socket;
	if (channel_socket_get(nodeid, &socket) != 0) {
		// not found
		rv = -1;
		goto Exit;
	}

	DEBUG("disconnecting the channel attached to node %" PRIx64, nodeid);

	channel_socket_close_queue(socket, false, true);

	rv = 0;

Exit:
	CHANNEL_UNLOCK();

	return rv;
}

/**
 * Register mesh listening sockets.
 */
static void
channel_mesh_listening_socks_register(cf_sockets* listening_sockets)
{
	CHANNEL_LOCK();
	g_hb.channel_state.listening_sockets = listening_sockets;

	cf_poll_add_sockets(g_hb.channel_state.poll,
			g_hb.channel_state.listening_sockets,
			EPOLLIN | EPOLLERR | EPOLLHUP);
	cf_socket_show_server(AS_HB, "mesh heartbeat",
			g_hb.channel_state.listening_sockets);

	// We do not need a separate channel to cover this socket because IO will
	// not happen on these sockets.
	CHANNEL_UNLOCK();
}

/**
 * Deregister mesh listening socket from epoll event.
 * @param socket the listening socket socket.
 */
static void
channel_mesh_listening_socks_deregister(cf_sockets* listening_sockets)
{
	CHANNEL_LOCK();
	cf_poll_delete_sockets(g_hb.channel_state.poll, listening_sockets);
	CHANNEL_UNLOCK();
}

/**
 * Register the multicast listening socket.
 * @param socket the listening socket.
 * @param endpoint the endpoint on which multicast io happens.
 */
static void
channel_multicast_listening_socks_register(cf_sockets* listening_sockets)
{
	CHANNEL_LOCK();
	g_hb.channel_state.listening_sockets = listening_sockets;

	// Create a new multicast channel for each multicast socket.
	for (uint32_t i = 0;
			i < g_hb.mode_state.multicast_state.listening_sockets.n_socks;
			++i) {
		channel_socket_register(&g_hb.channel_state.listening_sockets->socks[i],
				true, false, NULL);
	}

	cf_socket_mcast_show(AS_HB, "multicast heartbeat",
			g_hb.channel_state.listening_sockets);
	CHANNEL_UNLOCK();
}

/**
 * Deregister multicast listening socket from epoll event.
 * @param socket the listening socket socket.
 */
static void
channel_multicast_listening_socks_deregister(cf_sockets* listening_sockets)
{
	CHANNEL_LOCK();
	cf_poll_delete_sockets(g_hb.channel_state.poll, listening_sockets);
	CHANNEL_UNLOCK();
}

/**
 * Initialize the channel sub module.
 */
static void
channel_init()
{
	CHANNEL_LOCK();

	// Disable events till initialization is complete.
	channel_events_enabled_set(false);

	// Initialize unpublished event queue.
	cf_queue_init(&g_hb.channel_state.events_queue, sizeof(as_hb_channel_event),
	AS_HB_CLUSTER_MAX_SIZE_SOFT, true);

	// Initialize sockets to close queue.
	cf_queue_init(&g_hb.channel_state.socket_close_queue,
			sizeof(as_hb_channel_socket_close_entry),
			AS_HB_CLUSTER_MAX_SIZE_SOFT, true);

	// Initialize the nodeid to socket hash.
	g_hb.channel_state.nodeid_to_socket = cf_shash_create(cf_nodeid_shash_fn,
			sizeof(cf_node), sizeof(cf_socket*), AS_HB_CLUSTER_MAX_SIZE_SOFT,
			0);

	// Initialize the socket to channel state hash.
	g_hb.channel_state.socket_to_channel = cf_shash_create(hb_socket_hash_fn,
			sizeof(cf_socket*), sizeof(as_hb_channel),
			AS_HB_CLUSTER_MAX_SIZE_SOFT, 0);

	g_hb.channel_state.status = AS_HB_STATUS_STOPPED;

	CHANNEL_UNLOCK();
}

/**
 * Start channel sub module. Kicks off the channel tending thread.
 */
static void
channel_start()
{
	CHANNEL_LOCK();

	if (channel_is_running()) {
		WARNING("heartbeat channel already started");
		goto Exit;
	}

	// create the epoll socket.
	cf_poll_create(&g_hb.channel_state.poll);

	DEBUG("created epoll fd %d", CEFD(g_hb.channel_state.poll));

	// Disable events till initialization is complete.
	channel_events_enabled_set(false);

	// Data structures have been initialized.
	g_hb.channel_state.status = AS_HB_STATUS_RUNNING;

	// Initialization complete enable events.
	channel_events_enabled_set(true);

	// Start the channel tender.
	g_hb.channel_state.channel_tender_tid =
			cf_thread_create_joinable(channel_tender, (void*)&g_hb);

Exit:
	CHANNEL_UNLOCK();
}

/**
 * Get all sockets.
 */
static int
channel_sockets_get_reduce(const void* key, void* data, void* udata)
{
	cf_vector* sockets = (cf_vector*)udata;
	cf_vector_append(sockets, key);
	return CF_SHASH_OK;
}

/**
 * Stop the channel sub module called on hb_stop.
 */
static void
channel_stop()
{
	if (!channel_is_running()) {
		WARNING("heartbeat channel already stopped");
		return;
	}

	DEBUG("stopping the channel");

	// Unguarded state change but this should be OK.
	g_hb.channel_state.status = AS_HB_STATUS_SHUTTING_DOWN;

	// Wait for the channel tender thread to finish.
	cf_thread_join(g_hb.channel_state.channel_tender_tid);

	CHANNEL_LOCK();

	cf_vector sockets;
	cf_socket buff[cf_shash_get_size(g_hb.channel_state.socket_to_channel)];
	cf_vector_init_smalloc(&sockets, sizeof(cf_socket*), (uint8_t*)buff,
			sizeof(buff), VECTOR_FLAG_INITZERO);

	cf_shash_reduce(g_hb.channel_state.socket_to_channel,
			channel_sockets_get_reduce, &sockets);

	channel_sockets_close(&sockets);

	// Disable events.
	channel_events_enabled_set(false);

	cf_vector_destroy(&sockets);

	// Close epoll socket.
	cf_poll_destroy(g_hb.channel_state.poll);
	EFD(g_hb.channel_state.poll) = -1;

	// Disable the channel thread.
	g_hb.channel_state.status = AS_HB_STATUS_STOPPED;

	DEBUG("channel Stopped");

	CHANNEL_UNLOCK();
}

/**
 * Send heartbeat protocol message retries in case of EAGAIN and EWOULDBLOCK
 * @param socket the socket to send the buffer over.
 * @param buff the data buffer.
 * @param buffer_length the number of bytes in the buffer to send.
 * @return 0 on successful send -1 on failure
 */
static int
channel_mesh_msg_send(cf_socket* socket, uint8_t* buff, size_t buffer_length)
{
	CHANNEL_LOCK();
	int rv;

	if (cf_socket_send_all(socket, buff, buffer_length, 0,
	MESH_RW_TIMEOUT) < 0) {
		as_hb_channel channel;
		if (channel_get_channel(socket, &channel) == 0) {
			// Would happen if the channel was closed in the same epoll loop.
			TICKER_WARNING("sending mesh message to %"PRIx64" on fd %d failed : %s",
					channel.nodeid, CSFD(socket), cf_strerror(errno));
		}
		else {
			TICKER_WARNING("sending mesh message on fd %d failed : %s",
					CSFD(socket), cf_strerror(errno));
		}

		channel_socket_shutdown(socket);
		rv = -1;
	}
	else {
		rv = 0;
	}

	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Send heartbeat protocol message retries in case of EAGAIN and EWOULDBLOCK
 * @param socket the socket to send the buffer over.
 * @param buff the data buffer.
 * @param buffer_length the number of bytes in the buffer to send.
 * @return 0 on successful send -1 on failure
 */
static int
channel_multicast_msg_send(cf_socket* socket, uint8_t* buff,
		size_t buffer_length)
{
	CHANNEL_LOCK();
	int rv = 0;
	DETAIL("sending udp heartbeat to fd %d: msg size %zu", CSFD(socket),
			buffer_length);

	int mtu = hb_mtu();
	if (buffer_length > mtu) {
		TICKER_WARNING("mtu breach, sending udp heartbeat to fd %d: mtu %d",
				CSFD(socket), mtu);
	}

	cf_msock_cfg* socket_cfg = (cf_msock_cfg*)(socket->cfg);
	cf_sock_addr dest;
	dest.port = socket_cfg->port;
	cf_ip_addr_copy(&socket_cfg->addr, &dest.addr);

	if (cf_socket_send_to(socket, buff, buffer_length, 0, &dest) < 0) {
		TICKER_WARNING("multicast message send failed on fd %d %s",
				CSFD(socket), cf_strerror(errno));
		rv = -1;
	}
	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Indicates if this msg requires compression.
 */
static bool
channel_msg_is_compression_required(msg* msg, int wire_size, int mtu)
{
	return wire_size > msg_compression_threshold(mtu);
}

/**
 * Estimate the size of the buffer required to fill out the serialized message.
 * @param msg the input message.
 * @param mtu the underlying network mtu.
 * @return the size of the buffer required.
 */
static int
channel_msg_buffer_size_get(int wire_size, int mtu)
{
	return round_up_pow2(MAX(wire_size, compressBound(wire_size)));
}

/**
 * Fills the buffer with the serialized message.
 * @param original_msg the original message to serialize.
 * @param wire_size the message wire size.
 * @param mtu the underlying network mtu.
 * @param buffer the destination buffer.
 * @param buffer_len the buffer length.
 *
 * @return length of the serialized message.
 */
static size_t
channel_msg_buffer_fill(msg* original_msg, int wire_size, int mtu,
		uint8_t* buffer, size_t buffer_len)
{
	// This is output by msg_to_wire. Using a separate variable so that we do
	// not lose the actual buffer length needed for compression later on.
	size_t msg_size = msg_to_wire(original_msg, buffer);

	if (channel_msg_is_compression_required(original_msg, msg_size, mtu)) {
		// Compression is required.
		const size_t compressed_buffer_len = buffer_len;
		uint8_t* compressed_buffer = MSG_BUFF_ALLOC_OR_DIE(
				compressed_buffer_len,
				"error allocating memory size %zu for compressing message",
				compressed_buffer_len);

		size_t compressed_msg_size = compressed_buffer_len;
		int compress_rv = compress2(compressed_buffer, &compressed_msg_size,
				buffer, wire_size, Z_BEST_COMPRESSION);

		if (compress_rv == Z_BUF_ERROR) {
			// Compression result going to be larger than original input buffer.
			// Skip compression and try to send the message as is.
			DETAIL(
					"skipping compression - compressed size larger than input size %zu",
					msg_size);
		}
		else {
			msg* temp_msg = hb_msg_get();

			msg_set_buf(temp_msg, AS_HB_MSG_COMPRESSED_PAYLOAD,
					compressed_buffer, compressed_msg_size, MSG_SET_COPY);
			msg_size = msg_to_wire(temp_msg, buffer);

			hb_msg_return(temp_msg);
		}

		MSG_BUFF_FREE(compressed_buffer, compressed_buffer_len);

	}

	return msg_size;
}

/**
 * Send a message to a destination node.
 */
static int
channel_msg_unicast(cf_node dest, msg* msg)
{
	size_t buffer_len = 0;
	uint8_t* buffer = NULL;
	if (!hb_is_mesh()) {
		// Can't send a unicast message in the multicast mode.
		WARNING("ignoring sending unicast message in multicast mode");
		return -1;
	}

	CHANNEL_LOCK();

	int rv = -1;
	cf_socket* connected_socket;

	if (channel_socket_get(dest, &connected_socket) != 0) {
		DEBUG("failing message send to disconnected node %" PRIx64, dest);
		rv = -1;
		goto Exit;
	}

	// Read the message to a buffer.
	int mtu = hb_mtu();
	int wire_size = msg_get_wire_size(msg);
	buffer_len = channel_msg_buffer_size_get(wire_size, mtu);
	buffer =
			MSG_BUFF_ALLOC_OR_DIE(buffer_len,
					"error allocating memory size %zu for sending message to node %" PRIx64,
					buffer_len, dest);

	size_t msg_size = channel_msg_buffer_fill(msg, wire_size, mtu, buffer,
			buffer_len);

	// Send over the buffer.
	rv = channel_mesh_msg_send(connected_socket, buffer, msg_size);

Exit:
	MSG_BUFF_FREE(buffer, buffer_len);
	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Shash reduce function to walk over the socket to channel hash and broadcast
 * the message in udata.
 */
static int
channel_msg_broadcast_reduce(const void* key, void* data, void* udata)
{
	CHANNEL_LOCK();
	cf_socket** socket = (cf_socket**)key;
	as_hb_channel* channel = (as_hb_channel*)data;
	as_hb_channel_buffer_udata* buffer_udata =
			(as_hb_channel_buffer_udata*)udata;

	if (!channel->is_multicast) {
		DETAIL(
				"broadcasting message of length %zu on channel %d assigned to node %" PRIx64,
				buffer_udata->buffer_len, CSFD(*socket), channel->nodeid);

		channel_mesh_msg_send(*socket, buffer_udata->buffer,
				buffer_udata->buffer_len);
	}
	else {
		channel_multicast_msg_send(*socket, buffer_udata->buffer,
				buffer_udata->buffer_len);
	}

	CHANNEL_UNLOCK();

	return CF_SHASH_OK;
}

/**
 * Broadcast a message over all channels.
 */
static int
channel_msg_broadcast(msg* msg)
{
	CHANNEL_LOCK();

	int rv = -1;

	// Read the message to a buffer.
	int mtu = hb_mtu();
	int wire_size = msg_get_wire_size(msg);
	size_t buffer_len = channel_msg_buffer_size_get(wire_size, mtu);
	uint8_t* buffer = MSG_BUFF_ALLOC_OR_DIE(buffer_len,
			"error allocating memory size %zu for sending broadcast message",
			buffer_len);

	as_hb_channel_buffer_udata udata;
	udata.buffer = buffer;

	// Note this is the length of buffer to send.
	udata.buffer_len = channel_msg_buffer_fill(msg, wire_size, mtu, buffer,
			buffer_len);

	cf_shash_reduce(g_hb.channel_state.socket_to_channel,
			channel_msg_broadcast_reduce, &udata);

	MSG_BUFF_FREE(buffer, buffer_len);
	CHANNEL_UNLOCK();
	return rv;
}

/**
 * Clear all channel state.
 */
static void
channel_clear()
{
	if (!channel_is_stopped()) {
		WARNING("attempted channel clear without stopping the channel");
		return;
	}

	CHANNEL_LOCK();

	// Free the unpublished event queue.
	cf_queue_delete_all(&g_hb.channel_state.events_queue);

	// Delete nodeid to socket hash.
	cf_shash_reduce(g_hb.channel_state.nodeid_to_socket, hb_delete_all_reduce,
	NULL);

	// Delete the socket_to_channel hash.
	cf_shash_reduce(g_hb.channel_state.socket_to_channel, hb_delete_all_reduce,
	NULL);

	DETAIL("cleared channel information");
	CHANNEL_UNLOCK();
}

/**
 * Reduce function to dump channel node info to log file.
 */
static int
channel_dump_reduce(const void* key, void* data, void* udata)
{
	cf_socket** socket = (cf_socket**)key;
	as_hb_channel* channel = (as_hb_channel*)data;

	INFO("\tHB Channel (%s): node-id %" PRIx64 " fd %d endpoint %s polarity %s last-received %" PRIu64,
			channel->is_multicast ? "multicast" : "mesh", channel->nodeid,
			CSFD(*socket), (cf_sock_addr_is_any(&channel->endpoint_addr))
			? "unknown"
			: cf_sock_addr_print(&channel->endpoint_addr),
			channel->is_inbound ? "inbound" : "outbound",
			channel->last_received);

	return CF_SHASH_OK;
}

/**
 * Dump channel state to logs.
 * @param verbose enables / disables verbose logging.
 */
static void
channel_dump(bool verbose)
{
	CHANNEL_LOCK();

	INFO("HB Channel Count %d",
			cf_shash_get_size(g_hb.channel_state.socket_to_channel));

	if (verbose) {
		cf_shash_reduce(g_hb.channel_state.socket_to_channel,
				channel_dump_reduce, NULL);
	}

	CHANNEL_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Mesh sub module.
 * ----------------------------------------------------------------------------
 */

/**
 * Is mesh running.
 */
static bool
mesh_is_running()
{
	MESH_LOCK();
	bool retval =
			(g_hb.mode_state.mesh_state.status == AS_HB_STATUS_RUNNING) ?
					true : false;
	MESH_UNLOCK();
	return retval;
}

/**
 * Is mesh stopped.
 */
static bool
mesh_is_stopped()
{
	MESH_LOCK();
	bool retval =
			(g_hb.mode_state.mesh_state.status == AS_HB_STATUS_STOPPED) ?
					true : false;
	MESH_UNLOCK();
	return retval;
}

/**
 * Refresh	the mesh published endpoint list.
 * @return 0 on successful list creation, -1 otherwise.
 */
static int
mesh_published_endpoint_list_refresh()
{
	int rv = -1;
	MESH_LOCK();

	// TODO: Add interface addresses change detection logic here as well.
	if (g_hb.mode_state.mesh_state.published_endpoint_list != NULL
			&& g_hb.mode_state.mesh_state.published_endpoint_list_ipv4_only
					== cf_ip_addr_legacy_only()) {
		rv = 0;
		goto Exit;
	}

	// The global flag has changed, refresh the published address list.
	if (g_hb.mode_state.mesh_state.published_endpoint_list) {
		// Free the obsolete list.
		cf_free(g_hb.mode_state.mesh_state.published_endpoint_list);
	}

	const cf_serv_cfg* bind_cfg = config_bind_cfg_get();
	cf_serv_cfg published_cfg;

	config_bind_serv_cfg_expand(bind_cfg, &published_cfg,
			g_hb.mode_state.mesh_state.published_endpoint_list_ipv4_only);

	g_hb.mode_state.mesh_state.published_endpoint_list =
			as_endpoint_list_from_serv_cfg(&published_cfg);

	if (!g_hb.mode_state.mesh_state.published_endpoint_list) {
		CRASH("error initializing mesh published address list");
	}

	g_hb.mode_state.mesh_state.published_endpoint_list_ipv4_only =
			cf_ip_addr_legacy_only();

	rv = 0;

	char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
	as_endpoint_list_to_string(
			g_hb.mode_state.mesh_state.published_endpoint_list,
			endpoint_list_str, sizeof(endpoint_list_str));
	INFO("updated heartbeat published address list to {%s}", endpoint_list_str);

Exit:
	MESH_UNLOCK();
	return rv;
}

/**
 * Read the published endpoint list via a callback. The call back pattern is to
 * prevent access to the published list outside the mesh lock.
 * @param process_fn the list process function. The list passed to the process
 * function can be NULL.
 * @param udata passed as is to the process function.
 */
static void
mesh_published_endpoints_process(endpoint_list_process_fn process_fn,
		void* udata)
{
	MESH_LOCK();

	as_endpoint_list* rv = NULL;
	if (mesh_published_endpoint_list_refresh()) {
		WARNING("error creating mesh published endpoint list");
		rv = NULL;
	}
	else {
		rv = g_hb.mode_state.mesh_state.published_endpoint_list;
	}

	(process_fn)(rv, udata);

	MESH_UNLOCK();
}

/**
 * Convert mesh status to a string.
 */
static const char*
mesh_node_status_string(as_hb_mesh_node_status status)
{
	static char* status_str[] = {
		"active",
		"pending",
		"inactive",
		"endpoint-unknown" };

	if (status >= AS_HB_MESH_NODE_STATUS_SENTINEL) {
		return "corrupted";
	}
	return status_str[status];
}

/**
 * Change the state of a mesh node. Note: memset the mesh_nodes to zero before
 * calling state change for the first time.
 */
static void
mesh_seed_status_change(as_hb_mesh_seed* seed,
		as_hb_mesh_node_status new_status)
{
	seed->status = new_status;
	seed->last_status_updated = cf_getms();
}

/**
 * Destroy a mesh seed node.
 */
static void
mesh_seed_destroy(as_hb_mesh_seed* seed)
{
	MESH_LOCK();
	if (seed->resolved_endpoint_list) {
		cf_free(seed->resolved_endpoint_list);
		seed->resolved_endpoint_list = NULL;
	}
	MESH_UNLOCK();
}

static void
mesh_seed_dns_resolve_cb(bool is_resolved, const char* hostname,
		const cf_ip_addr *addrs, uint32_t n_addrs, void *udata)
{
	MESH_LOCK();
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);

		if ((strncmp(seed->seed_host_name, hostname,
				sizeof(seed->seed_host_name)) != 0)
				|| seed->resolved_endpoint_list != NULL) {
			continue;
		}

		cf_serv_cfg temp_serv_cfg;
		cf_serv_cfg_init(&temp_serv_cfg);

		cf_sock_cfg sock_cfg;
		cf_sock_cfg_init(&sock_cfg,
				seed->seed_tls ?
						CF_SOCK_OWNER_HEARTBEAT_TLS : CF_SOCK_OWNER_HEARTBEAT);
		sock_cfg.port = seed->seed_port;

		for (int i = 0; i < n_addrs; i++) {
			cf_ip_addr_copy(&addrs[i], &sock_cfg.addr);
			if (cf_serv_cfg_add_sock_cfg(&temp_serv_cfg, &sock_cfg)) {
				CRASH("error initializing resolved address list");
			}

			DETAIL("resolved mesh node hostname %s to %s", seed->seed_host_name,
					cf_ip_addr_print(&addrs[i]));
		}

		seed->resolved_endpoint_list = as_endpoint_list_from_serv_cfg(
				&temp_serv_cfg);
	}

	MESH_UNLOCK();
}

/**
 * Fill the endpoint list for a mesh seed using the mesh seed hostname and port.
 * returns the
 * @param mesh_node the mesh node
 * @return 0 on success. -1 if a valid endpoint list does not exist and it could
 * not be generated.
 */
static int
mesh_seed_endpoint_list_fill(as_hb_mesh_seed* seed)
{
	if (seed->resolved_endpoint_list != NULL
			&& seed->resolved_endpoint_list->n_endpoints > 0) {
		// A valid endpoint list already exists. For now we resolve only once.
		return 0;
	}

	cf_clock now = cf_getms();
	if (now
			< seed->resolved_endpoint_list_ts
					+ MESH_SEED_RESOLVE_ATTEMPT_INTERVAL()) {
		// We have just resolved this seed entry unsuccessfully. Don't try again
		// for sometime.
		return -1;
	}

	// Resolve and get all IPv4/IPv6 ip addresses asynchronously.
	seed->resolved_endpoint_list_ts = now;
	cf_ip_addr_from_string_multi_a(seed->seed_host_name,
			mesh_seed_dns_resolve_cb, NULL);
	return -1;
}

/**
 * Find a mesh seed in the seed list that has an overlapping endpoint and return
 * an internal pointer. Assumes this function is called within mesh lock to
 * prevent invalidating the returned index after function return.
 *
 * @param endpoint_list the	 endpoint list to find the endpoint by.
 * @return index to matching seed entry if found, else -1
 */
static int
mesh_seed_endpoint_list_overlapping_find_unsafe(as_endpoint_list* endpoint_list)
{
	MESH_LOCK();

	int match_index = -1;
	if (!endpoint_list) {
		// Null / empty endpoint list.
		goto Exit;
	}
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);

		// Ensure the seed hostname is resolved.
		mesh_seed_endpoint_list_fill(seed);

		if (as_endpoint_lists_are_overlapping(endpoint_list,
				seed->resolved_endpoint_list, true)) {
			match_index = i;
			break;
		}
	}

Exit:
	MESH_UNLOCK();
	return match_index;
}

/**
 * Remove a seed entry from the seed list.
 * Assumes this function is called within mesh lock to prevent invalidating the
 * used index during a function call.
 * @param seed_index the index of the seed element.
 * @return 0 on success -1 on failure.
 */
static int
mesh_seed_delete_unsafe(int seed_index)
{
	int rv = -1;
	MESH_LOCK();
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	if (seed_index >= 0) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, seed_index);
		mesh_seed_destroy(seed);
		rv = cf_vector_delete(seeds, seed_index);
		if (rv == 0) {
			INFO("removed mesh seed host:%s port %d", seed->seed_host_name,
					seed->seed_port);
		}
	}
	MESH_UNLOCK();
	return rv;
}

/**
 * Find a mesh seed in the seed list with exactly matching hostname and port.
 * Assumes this function is called within mesh lock to prevent invalidating the
 * returned index after function return.
 *
 * @param host the seed hostname
 * @param port the seed port
 * @return index to matching seed entry if found, else -1
 */
static int
mesh_seed_find_unsafe(char* host, int port)
{
	MESH_LOCK();

	int match_index = -1;
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
		if (strncmp(seed->seed_host_name, host, sizeof(seed->seed_host_name))
				== 0 && seed->seed_port == port) {
			match_index = i;
			break;
		}
	}

	MESH_UNLOCK();
	return match_index;
}

/**
 * Endure mesh tend udata has enough space for current mesh nodes.
 */
static void
mesh_tend_udata_capacity_ensure(as_hb_mesh_tend_reduce_udata* tend_reduce_udata,
		int mesh_node_count)
{
	// Ensure capacity for nodes to connect.
	if (tend_reduce_udata->to_connect_capacity < mesh_node_count) {
		uint32_t alloc_size = round_up_pow2(
				mesh_node_count * sizeof(as_endpoint_list*));
		int old_capacity = tend_reduce_udata->to_connect_capacity;
		tend_reduce_udata->to_connect_capacity = alloc_size
				/ sizeof(as_endpoint_list*);
		tend_reduce_udata->to_connect = cf_realloc(
				tend_reduce_udata->to_connect, alloc_size);

		// NULL out newly allocated elements.
		for (int i = old_capacity; i < tend_reduce_udata->to_connect_capacity;
				i++) {
			tend_reduce_udata->to_connect[i] = NULL;
		}
	}
}

/**
 * Change the state of a mesh node. Note: memset the mesh_nodes to zero before
 * calling state change for the first time.
 */
static void
mesh_node_status_change(as_hb_mesh_node* mesh_node,
		as_hb_mesh_node_status new_status)
{
	as_hb_mesh_node_status old_status = mesh_node->status;
	mesh_node->status = new_status;

	if ((new_status != AS_HB_MESH_NODE_CHANNEL_ACTIVE
			&& old_status == AS_HB_MESH_NODE_CHANNEL_ACTIVE)
			|| mesh_node->last_status_updated == 0) {
		mesh_node->inactive_since = cf_getms();
	}
	mesh_node->last_status_updated = cf_getms();
	return;
}

/**
 * Close mesh listening sockets.
 */
static void
mesh_listening_sockets_close()
{
	MESH_LOCK();
	INFO("closing mesh heartbeat sockets");
	cf_sockets_close(&g_hb.mode_state.mesh_state.listening_sockets);
	DEBUG("closed mesh heartbeat sockets");
	MESH_UNLOCK();
}

/**
 * Populate the buffer with mesh seed list.
 */
static void
mesh_seed_host_list_get(cf_dyn_buf* db, bool tls)
{
	if (!hb_is_mesh()) {
		return;
	}

	MESH_LOCK();

	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
		const char* info_key =
				seed->seed_tls ?
						"heartbeat.tls-mesh-seed-address-port=" :
						"heartbeat.mesh-seed-address-port=";

		cf_dyn_buf_append_string(db, info_key);
		cf_dyn_buf_append_string(db, seed->seed_host_name);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_uint32(db, seed->seed_port);
		cf_dyn_buf_append_char(db, ';');
	}

	MESH_UNLOCK();
}

/**
 * Checks if the match between a mesh seed and a mesh node is valid.
 * The matching would be invalid if the mesh node's endpoint has been updated
 * after the match was made or there has been no match.
 */
static bool
mesh_seed_mesh_node_check(as_hb_mesh_seed* seed)
{
	if (seed->status != AS_HB_MESH_NODE_CHANNEL_ACTIVE) {
		return false;
	}

	as_hb_mesh_node node;
	if (mesh_node_get(seed->mesh_nodeid, &node) != 0) {
		// The matched node has vanished.
		return false;
	}

	return seed->mesh_node_endpoint_change_ts == node.endpoint_change_ts;
}

/**
 * Refresh the matching between seeds and mesh nodes and get inactive seeds.
 * Should be invoked under a mesh lock to ensure the validity of returned
 * pointers.
 * @param inactive_seeds_p output vector of inactive seed pointers. Can be NULL
 * if inactive nodes need not be returned.
 */
static void
mesh_seed_inactive_refresh_get_unsafe(cf_vector* inactive_seeds_p)
{
	MESH_LOCK();

	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	if (inactive_seeds_p) {
		cf_vector_clear(inactive_seeds_p);
	}

	// Mark seeds that do not have a matching mesh node and transitively do not
	// have a matching channel.
	cf_clock now = cf_getms();
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
		if (mesh_seed_mesh_node_check(seed)) {
			continue;
		}

		seed->mesh_nodeid = 0;
		seed->mesh_node_endpoint_change_ts = 0;

		// The mesh node is being connected. Skip.
		if (seed->status == AS_HB_MESH_NODE_CHANNEL_PENDING) {
			if (seed->last_status_updated + MESH_PENDING_TIMEOUT > now) {
				// Spare the pending seeds, since we are attempting to connect
				// to the seed host.
				continue;
			}

			// Flip to inactive if we have been in pending state for a long
			// time.
			mesh_seed_status_change(seed, AS_HB_MESH_NODE_CHANNEL_INACTIVE);
		}

		if (seed->status != AS_HB_MESH_NODE_CHANNEL_PENDING) {
			mesh_seed_status_change(seed, AS_HB_MESH_NODE_CHANNEL_INACTIVE);
			if (inactive_seeds_p) {
				cf_vector_append(inactive_seeds_p, &seed);
			}
		}
	}

	MESH_UNLOCK();
}

/**
 * Match input seeds to a mesh node using its endpoint address and
 */
static void
mesh_seeds_mesh_node_match_update(cf_vector* inactive_seeds_p,
		as_hb_mesh_node* mesh_node, cf_node mesh_nodeid)
{
	if (mesh_node->status
			== AS_HB_MESH_NODE_ENDPOINT_UNKNOWN|| mesh_node->endpoint_list == NULL) {
		return;
	}

	int element_count = cf_vector_size(inactive_seeds_p);
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		as_hb_mesh_seed* seed = *(as_hb_mesh_seed**)cf_vector_getp(
				inactive_seeds_p, i);
		if (as_endpoint_lists_are_overlapping(seed->resolved_endpoint_list,
				mesh_node->endpoint_list, true)) {
			// We found a matching mesh node for the seed, flip its status to
			// active.
			seed->mesh_nodeid = mesh_nodeid;
			seed->mesh_node_endpoint_change_ts = mesh_node->endpoint_change_ts;
			mesh_seed_status_change(seed, AS_HB_MESH_NODE_CHANNEL_ACTIVE);
			DEBUG("seed entry %s:%d connected", seed->seed_host_name,
					seed->seed_port);
		}
	}
}

/**
 * Determines if a mesh entry should be connected to or expired and deleted.
 */
static int
mesh_tend_reduce(const void* key, void* data, void* udata)
{
	MESH_LOCK();

	int rv = CF_SHASH_OK;
	cf_node nodeid = *(cf_node*)key;
	as_hb_mesh_node* mesh_node = (as_hb_mesh_node*)data;
	as_hb_mesh_tend_reduce_udata* tend_reduce_udata =
			(as_hb_mesh_tend_reduce_udata*)udata;

	DETAIL("tending mesh node %"PRIx64" with status %s", nodeid,
			mesh_node_status_string(mesh_node->status));

	mesh_seeds_mesh_node_match_update(tend_reduce_udata->inactive_seeds_p,
			mesh_node, nodeid);

	if (mesh_node->status == AS_HB_MESH_NODE_CHANNEL_ACTIVE) {
		// The mesh node is connected. Skip.
		goto Exit;
	}

	cf_clock now = cf_getms();

	if (!mesh_node->endpoint_list) {
		// Will happen if node discover and disconnect happen close together.
		mesh_node_status_change(mesh_node, AS_HB_MESH_NODE_ENDPOINT_UNKNOWN);
	}

	if (mesh_node->inactive_since + MESH_INACTIVE_TIMEOUT <= now) {
		DEBUG("mesh forgetting node %" PRIx64" because it could not be connected since %" PRIx64,
				nodeid, mesh_node->inactive_since);
		rv = CF_SHASH_REDUCE_DELETE;
		goto Exit;
	}

	if (mesh_node->status == AS_HB_MESH_NODE_ENDPOINT_UNKNOWN) {
		if (mesh_node->last_status_updated + MESH_ENDPOINT_UNKNOWN_TIMEOUT
				> now) {
			DEBUG("mesh forgetting node %"PRIx64" ip address/port undiscovered since %"PRIu64,
					nodeid, mesh_node->last_status_updated);

			rv = CF_SHASH_REDUCE_DELETE;
		}
		// Skip connecting with a node with unknown endpoint.
		goto Exit;
	}

	if (mesh_node->status == AS_HB_MESH_NODE_CHANNEL_PENDING) {
		// The mesh node is being connected. Skip.
		if (mesh_node->last_status_updated + MESH_PENDING_TIMEOUT > now) {
			goto Exit;
		}

		// Flip to inactive if we have been in pending state for a long time.
		mesh_node_status_change(mesh_node, AS_HB_MESH_NODE_CHANNEL_INACTIVE);
	}

	// Channel for this node is inactive. Prompt the channel sub module to
	// connect to this node.
	if (tend_reduce_udata->to_connect_count
			>= tend_reduce_udata->to_connect_capacity) {
		// New nodes found but we are out of capacity. Ultra defensive coding.
		// This will never happen under the locks.
		WARNING("skipping connecting to node %" PRIx64" - not enough memory allocated",
				nodeid);
		goto Exit;
	}

	endpoint_list_copy(
			&tend_reduce_udata->to_connect[tend_reduce_udata->to_connect_count],
			mesh_node->endpoint_list);
	tend_reduce_udata->to_connect_count++;

	// Flip status to pending.
	mesh_node_status_change(mesh_node, AS_HB_MESH_NODE_CHANNEL_PENDING);

Exit:
	if (rv == CF_SHASH_REDUCE_DELETE) {
		// Clear all internal allocated memory.
		mesh_node_destroy(mesh_node);
	}

	MESH_UNLOCK();

	return rv;
}

/**
 * Add inactive seeds to to_connect array.
 * Should be invoked under mesh lock to prevent invalidating the array of seed
 * node pointers.
 * @param seed_p vector of seed pointers.
 * @param tend reduce udata having the to connect endpoint list.
 */
void
mesh_seeds_inactive_add_to_connect(cf_vector* seeds_p,
		as_hb_mesh_tend_reduce_udata* tend_reduce_udata)
{
	MESH_LOCK();
	int element_count = cf_vector_size(seeds_p);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = *(as_hb_mesh_seed**)cf_vector_getp(seeds_p, i);
		if (seed->status != AS_HB_MESH_NODE_CHANNEL_INACTIVE) {
			continue;
		}

		// Channel for this node is inactive. Prompt the channel sub module to
		// connect to this node.
		if (tend_reduce_udata->to_connect_count
			>= tend_reduce_udata->to_connect_capacity) {
			// New nodes found but we are out of capacity. Ultra defensive
			// coding.
			// This will never happen under the locks.
			WARNING(
				"skipping connecting to %s:%d - not enough memory allocated",
				seed->seed_host_name, seed->seed_port);
			return;
		}

		// Ensure the seed hostname is resolved.
		if (mesh_seed_endpoint_list_fill(seed) != 0) {
			continue;
		}

		endpoint_list_copy(
				&tend_reduce_udata->to_connect[tend_reduce_udata->to_connect_count],
				seed->resolved_endpoint_list);
		tend_reduce_udata->to_connect_count++;

		// Flip status to pending.
		mesh_seed_status_change(seed, AS_HB_MESH_NODE_CHANNEL_PENDING);
	}
	MESH_UNLOCK();
}

/**
 * Tends the mesh host list, to discover and remove nodes. Should never invoke a
 * channel call while holding a mesh lock.
 */
void*
mesh_tender(void* arg)
{
	DETAIL("mesh tender started");
	// Figure out which nodes need to be connected to.
	// collect nodes to connect to and remove dead nodes.
	as_hb_mesh_tend_reduce_udata tend_reduce_udata = { NULL, 0, 0 };

	// Vector of pointer to inactive seeds.
	cf_vector inactive_seeds_p;
	cf_vector_init(&inactive_seeds_p, sizeof(as_hb_mesh_seed*),
	AS_HB_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);

	cf_clock last_time = 0;

	while (hb_is_mesh() && mesh_is_running()) {
		cf_clock curr_time = cf_getms();

		// Unlocked access but this should be alright Set the discovered flag.
		bool nodes_discovered = g_hb.mode_state.mesh_state.nodes_discovered;
		if ((curr_time - last_time) < MESH_TEND_INTERVAL && !nodes_discovered) {
			// Interval has not been reached for sending heartbeats
			usleep(MIN(AS_HB_TX_INTERVAL_MS_MIN, (last_time +
			MESH_TEND_INTERVAL) - curr_time) * 1000);
			continue;
		}
		last_time = curr_time;

		DETAIL("tending mesh list");

		MESH_LOCK();
		// Unset the discovered flag.
		g_hb.mode_state.mesh_state.nodes_discovered = false;

		// Update the list of inactive seeds.
		mesh_seed_inactive_refresh_get_unsafe(&inactive_seeds_p);

		// Make sure the udata has enough capacity.
		int connect_count_max = cf_shash_get_size(
				g_hb.mode_state.mesh_state.nodeid_to_mesh_node)
				+ cf_vector_size(&inactive_seeds_p);
		mesh_tend_udata_capacity_ensure(&tend_reduce_udata, connect_count_max);

		tend_reduce_udata.to_connect_count = 0;
		tend_reduce_udata.inactive_seeds_p = &inactive_seeds_p;
		cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
				mesh_tend_reduce, &tend_reduce_udata);

		// Add inactive seeds for connection.
		mesh_seeds_inactive_add_to_connect(&inactive_seeds_p,
				&tend_reduce_udata);

		MESH_UNLOCK();

		// Connect can be time consuming, especially in failure cases.
		// Connect outside of the mesh lock and prevent hogging the lock.
		if (tend_reduce_udata.to_connect_count > 0) {
			// Try connecting the newer nodes.
			channel_mesh_channel_establish(tend_reduce_udata.to_connect,
					tend_reduce_udata.to_connect_count);
		}

		DETAIL("done tending mesh list");
	}

	if (tend_reduce_udata.to_connect) {
		// Free space allocated for endpoint lists.
		for (int i = 0; i < tend_reduce_udata.to_connect_capacity; i++) {
			if (tend_reduce_udata.to_connect[i]) {
				cf_free(tend_reduce_udata.to_connect[i]);
			}
		}
		cf_free(tend_reduce_udata.to_connect);
	}

	cf_vector_destroy(&inactive_seeds_p);

	DETAIL("mesh tender shut down");
	return NULL;
}

/**
 * Add or update a mesh node to mesh node list.
 */
static void
mesh_node_add_update(cf_node nodeid, as_hb_mesh_node* mesh_node)
{
	MESH_LOCK();
	cf_shash_put(g_hb.mode_state.mesh_state.nodeid_to_mesh_node, &nodeid,
			mesh_node);
	MESH_UNLOCK();
}

/**
 * Destroy a mesh node.
 */
static void
mesh_node_destroy(as_hb_mesh_node* mesh_node)
{
	MESH_LOCK();
	if (mesh_node->endpoint_list) {
		cf_free(mesh_node->endpoint_list);
		mesh_node->endpoint_list = NULL;
	}
	MESH_UNLOCK();
}

/**
 * Endpoint list iterate function find endpoint matching sock addr.
 */
static void
mesh_endpoint_addr_find_iterate(const as_endpoint* endpoint, void* udata)
{
	cf_sock_addr endpoint_addr;
	if (as_endpoint_to_sock_addr(endpoint, &endpoint_addr) != 0) {
		return;
	}

	as_hb_endpoint_list_addr_find_udata* endpoint_reduce_udata =
			(as_hb_endpoint_list_addr_find_udata*)udata;

	if (cf_sock_addr_compare(&endpoint_addr, endpoint_reduce_udata->to_search)
			== 0) {
		endpoint_reduce_udata->found = true;
	}
}

/**
 * Indicates if a give node is discovered.
 * @param nodeid the input nodeid.
 * @return true if discovered, false otherwise.
 */
static bool
mesh_node_is_discovered(cf_node nodeid)
{
	if (nodeid == config_self_nodeid_get()) {
		// Assume this node knows itself.
		return true;
	}

	as_hb_mesh_node mesh_node;
	return mesh_node_get(nodeid, &mesh_node) == 0;
}

/**
 * Indicates if a give node has a valid endpoint list.
 * @param nodeid the input nodeid.
 * @return true if node has valid endpoint list, false otherwise.
 */
static bool
mesh_node_endpoint_list_is_valid(cf_node nodeid)
{
	if (nodeid == config_self_nodeid_get()) {
		// Assume this node knows itself.
		return true;
	}

	as_hb_mesh_node mesh_node;
	return mesh_node_get(nodeid, &mesh_node) == 0
			&& mesh_node.status != AS_HB_MESH_NODE_ENDPOINT_UNKNOWN
			&& mesh_node.endpoint_list;
}

/**
 * Get the mesh node associated with this node.
 * @param nodeid the nodeid to search for.
 * @param is_real_nodeid indicates if the query is for a real or fake nodeid.
 * @param mesh_node the output mesh node.
 * @return 0 on success -1 if there is mesh node attached.
 */
static int
mesh_node_get(cf_node nodeid, as_hb_mesh_node* mesh_node)
{
	int rv = -1;

	MESH_LOCK();
	if (cf_shash_get(g_hb.mode_state.mesh_state.nodeid_to_mesh_node, &nodeid,
			mesh_node) == CF_SHASH_OK) {
		rv = 0;
	}
	else {
		// The node not found.
		rv = -1;
	}
	MESH_UNLOCK();
	return rv;
}

/**
 * Handle the event when the channel reports a node as disconnected.
 */
static void
mesh_channel_on_node_disconnect(as_hb_channel_event* event)
{
	MESH_LOCK();

	as_hb_mesh_node mesh_node;
	if (mesh_node_get(event->nodeid, &mesh_node) != 0) {
		// Again should not happen in practice. But not really bad.
		DEBUG("unknown mesh node disconnected %" PRIx64, event->nodeid);
		goto Exit;
	}

	DEBUG("mesh setting node %" PRIx64" status as inactive on loss of channel",
			event->nodeid);

	// Mark this node inactive and move on. Mesh tender should remove this node
	// after it has been inactive for a while.
	mesh_node_status_change(&mesh_node, AS_HB_MESH_NODE_CHANNEL_INACTIVE);

	// Update the mesh entry.
	mesh_node_add_update(event->nodeid, &mesh_node);

Exit:
	MESH_UNLOCK();
}

/**
 * Check and fix the case where we received a self incoming message probably
 * because one of our non loop back interfaces was used as a seed address.
 *
 * @return true if this message is a self message, false otherwise.
 */
static bool
mesh_node_check_fix_self_msg(as_hb_channel_event* event)
{
	if (event->nodeid == config_self_nodeid_get()) {
		// Handle self message. Will happen if the seed node address on this
		// node does not match the listen / publish address.
		as_endpoint_list* msg_endpoint_list;
		msg_endpoint_list_get(event->msg, &msg_endpoint_list);

		MESH_LOCK();

		// Check if this node has published an endpoint list matching self node.
		endpoint_list_equal_check_udata udata = { 0 };
		udata.are_equal = false;
		udata.other = msg_endpoint_list;
		mesh_published_endpoints_process(endpoint_list_equal_process, &udata);

		if (udata.are_equal) {
			// Definitely pulse message from self node.
			int self_seed_index =
					mesh_seed_endpoint_list_overlapping_find_unsafe(
							msg_endpoint_list);
			if (self_seed_index >= 0) {
				as_hb_mesh_seed* self_seed = cf_vector_getp(
						&g_hb.mode_state.mesh_state.seeds, self_seed_index);
				INFO("removing self seed entry host:%s port:%d",
						self_seed->seed_host_name, self_seed->seed_port);
				as_hb_mesh_tip_clear(self_seed->seed_host_name,
						self_seed->seed_port);
			}
		}
		MESH_UNLOCK();
		return true;
	}
	return false;
}

/**
 * Update mesh node status based on an incoming message.
 */
static void
mesh_node_data_update(as_hb_channel_event* event)
{
	if (mesh_node_check_fix_self_msg(event)) {
		// Message from self, can be ignored.
		return;
	}

	MESH_LOCK();
	as_hb_mesh_node existing_mesh_node = { 0 };
	as_endpoint_list* msg_endpoint_list = NULL;
	msg_endpoint_list_get(event->msg, &msg_endpoint_list);

	// Search for existing entry.
	bool needs_update = mesh_node_get(event->nodeid, &existing_mesh_node) != 0;

	// Update the endpoint list to be the message endpoint list if the seed ip
	// list and the published ip list differ
	if (!as_endpoint_lists_are_equal(existing_mesh_node.endpoint_list,
			msg_endpoint_list)) {
		char endpoint_list_str1[ENDPOINT_LIST_STR_SIZE];
		endpoint_list_str1[0] = 0;

		as_endpoint_list_to_string(existing_mesh_node.endpoint_list,
				endpoint_list_str1, sizeof(endpoint_list_str1));

		char endpoint_list_str2[ENDPOINT_LIST_STR_SIZE];
		as_endpoint_list_to_string(msg_endpoint_list, endpoint_list_str2,
				sizeof(endpoint_list_str2));

		if (existing_mesh_node.endpoint_list) {
			INFO("for node %"PRIx64" updating mesh endpoint address from {%s} to {%s}",event->nodeid,
					endpoint_list_str1, endpoint_list_str2);
		}

		// Update the endpoints.
		endpoint_list_copy(&existing_mesh_node.endpoint_list,
				msg_endpoint_list);
		existing_mesh_node.endpoint_change_ts = as_hlc_timestamp_now();

		needs_update = true;
	}

	if (existing_mesh_node.status != AS_HB_MESH_NODE_CHANNEL_ACTIVE) {
		// Update status to active.
		mesh_node_status_change(&existing_mesh_node,
				AS_HB_MESH_NODE_CHANNEL_ACTIVE);
		needs_update = true;
	}

	if (needs_update) {
		// Apply the update.
		mesh_node_add_update(event->nodeid, &existing_mesh_node);
	}

	MESH_UNLOCK();
}

/**
 * Return the in memory and on wire size of an info reply array.
 * @param reply the info reply.
 * @param reply_count the number of replies.
 * @param reply_size the wire size of the message.
 * @return 0 on successful reply count computation, -1 otherwise,
 */
static int
mesh_info_reply_sizeof(as_hb_mesh_info_reply* reply, int reply_count,
		size_t* reply_size)
{
	// Go over reply and compute the count of replies and also validate the
	// endpoint lists.
	uint8_t* start_ptr = (uint8_t*)reply;
	*reply_size = 0;

	for (int i = 0; i < reply_count; i++) {
		as_hb_mesh_info_reply* reply_ptr = (as_hb_mesh_info_reply*)start_ptr;
		*reply_size += sizeof(as_hb_mesh_info_reply);
		start_ptr += sizeof(as_hb_mesh_info_reply);

		size_t endpoint_list_size = 0;
		if (as_endpoint_list_sizeof(&reply_ptr->endpoint_list[0],
				&endpoint_list_size)) {
			// Incomplete / garbled info reply message.
			*reply_size = 0;
			return -1;
		}

		*reply_size += endpoint_list_size;
		start_ptr += endpoint_list_size;
	}

	return 0;
}

/**
 * Send a info reply in reply to an info request.
 * @param dest the destination node to send the info reply to.
 * @param reply array of node ids and endpoints
 * @param reply_count the count of replies.
 */
static void
mesh_nodes_send_info_reply(cf_node dest, as_hb_mesh_info_reply* reply,
		size_t reply_count)
{
	// Create the discover message.
	msg* msg = mesh_info_msg_init(AS_HB_MSG_TYPE_INFO_REPLY);

	// Set the reply.
	msg_info_reply_set(msg, reply, reply_count);

	DEBUG("sending info reply to node %" PRIx64, dest);

	// Send the info reply.
	if (channel_msg_unicast(dest, msg) != 0) {
		TICKER_WARNING("error sending info reply message to node %" PRIx64,
				dest);
	}

	hb_msg_return(msg);
}

/**
 * Initialize the info request msg buffer
 */
static msg*
mesh_info_msg_init(as_hb_msg_type msg_type)
{
	msg* msg = hb_msg_get();
	msg_src_fields_fill(msg);
	msg_type_set(msg, msg_type);
	return msg;
}

/**
 * Send a info request for all undiscovered nodes.
 * @param dest the destination node to send the discover message to.
 * @param to_discover array of node ids to discover.
 * @param to_discover_count the count of nodes in the array.
 */
static void
mesh_nodes_send_info_request(msg* in_msg, cf_node dest, cf_node* to_discover,
		size_t to_discover_count)
{
	// Create the discover message.
	msg* info_req = mesh_info_msg_init(AS_HB_MSG_TYPE_INFO_REQUEST);

	// Set the list of nodes to discover.
	msg_node_list_set(info_req, AS_HB_MSG_INFO_REQUEST, to_discover,
			to_discover_count);

	DEBUG("sending info request to node %" PRIx64, dest);

	// Send the info request.
	if (channel_msg_unicast(dest, info_req) != 0) {
		TICKER_WARNING("error sending info request message to node %" PRIx64,
				dest);
	}
	hb_msg_return(info_req);
}

/**
 * Handle an incoming pulse message to discover new neighbours.
 */
static void
mesh_channel_on_pulse(msg* msg)
{
	cf_node* adj_list;
	size_t adj_length;

	cf_node source;

	// Channel has validated the source. Don't bother checking here.
	msg_nodeid_get(msg, &source);
	if (msg_adjacency_get(msg, &adj_list, &adj_length) != 0) {
		// Adjacency list absent.
		WARNING("received message from %" PRIx64" without adjacency list",
				source);
		return;
	}

	cf_node to_discover[adj_length];
	size_t num_to_discover = 0;

	// TODO: Track already queried nodes so that we do not retry immediately.
	// Will need a separate state, pending query.
	MESH_LOCK();

	// Try and discover new nodes from this message's adjacency list.
	for (int i = 0; i < adj_length; i++) {
		if (!mesh_node_is_discovered(adj_list[i])) {
			DEBUG("discovered new mesh node %" PRIx64, adj_list[i]);

			as_hb_mesh_node new_node;
			memset(&new_node, 0, sizeof(new_node));
			mesh_node_status_change(&new_node,
					AS_HB_MESH_NODE_ENDPOINT_UNKNOWN);

			// Add as a new node
			mesh_node_add_update(adj_list[i], &new_node);
		}

		if (!mesh_node_endpoint_list_is_valid(adj_list[i])) {
			to_discover[num_to_discover++] = adj_list[i];
		}
	}

	MESH_UNLOCK();

	// Discover these nodes outside a lock.
	if (num_to_discover) {
		mesh_nodes_send_info_request(msg, source, to_discover, num_to_discover);
	}
}

/**
 * Handle an incoming info message.
 */
static void
mesh_channel_on_info_request(msg* msg)
{
	cf_node* query_nodeids;
	size_t query_count;

	cf_node source;
	msg_nodeid_get(msg, &source);

	if (msg_node_list_get(msg, AS_HB_MSG_INFO_REQUEST, &query_nodeids,
			&query_count) != 0) {
		TICKER_WARNING("got an info request without query nodes from %" PRIx64,
				source);
		return;
	}

	MESH_LOCK();

	// Compute the entire response size.
	size_t reply_size = 0;

	for (int i = 0; i < query_count; i++) {
		as_hb_mesh_node mesh_node;

		if (mesh_node_get(query_nodeids[i], &mesh_node) == 0) {
			if (mesh_node.status != AS_HB_MESH_NODE_ENDPOINT_UNKNOWN
					&& mesh_node.endpoint_list) {
				size_t endpoint_list_size = 0;
				as_endpoint_list_sizeof(mesh_node.endpoint_list,
						&endpoint_list_size);
				reply_size += sizeof(as_hb_mesh_info_reply)
						+ endpoint_list_size;
			}
		}
	}

	as_hb_mesh_info_reply* replies = alloca(reply_size);
	uint8_t* reply_ptr = (uint8_t*)replies;
	size_t reply_count = 0;

	DEBUG("received info request from node : %" PRIx64, source);
	DEBUG("preparing a reply for %zu requests", query_count);

	for (int i = 0; i < query_count; i++) {
		as_hb_mesh_node mesh_node;

		DEBUG("mesh received info request for node %" PRIx64, query_nodeids[i]);

		if (mesh_node_get(query_nodeids[i], &mesh_node) == 0) {
			if (mesh_node.status != AS_HB_MESH_NODE_ENDPOINT_UNKNOWN
					&& mesh_node.endpoint_list) {
				as_hb_mesh_info_reply* reply = (as_hb_mesh_info_reply*)reply_ptr;

				reply->nodeid = query_nodeids[i];

				size_t endpoint_list_size = 0;
				as_endpoint_list_sizeof(mesh_node.endpoint_list,
						&endpoint_list_size);

				memcpy(&reply->endpoint_list[0], mesh_node.endpoint_list,
						endpoint_list_size);

				reply_ptr += sizeof(as_hb_mesh_info_reply) + endpoint_list_size;

				reply_count++;
			}
		}
	}

	MESH_UNLOCK();

	// Send the reply
	if (reply_count > 0) {
		mesh_nodes_send_info_reply(source, replies, reply_count);
	}
}

/**
 * Handle an incoming info reply.
 */
static void
mesh_channel_on_info_reply(msg* msg)
{
	as_hb_mesh_info_reply* reply = NULL;
	size_t reply_count = 0;
	cf_node source = 0;
	msg_nodeid_get(msg, &source);
	if (msg_info_reply_get(msg, &reply, &reply_count) != 0
			|| reply_count == 0) {
		TICKER_WARNING(
				"got an info reply from without query nodes from %" PRIx64,
				source);
		return;
	}

	DEBUG("received info reply from node %" PRIx64, source);

	MESH_LOCK();

	uint8_t *start_ptr = (uint8_t*)reply;
	for (int i = 0; i < reply_count; i++) {
		as_hb_mesh_info_reply* reply_ptr = (as_hb_mesh_info_reply*)start_ptr;
		as_hb_mesh_node existing_node;
		if (mesh_node_get(reply_ptr->nodeid, &existing_node) != 0) {
			// Somehow the node was removed from the mesh hash. Maybe a timeout.
			goto NextReply;
		}

		// Update the state of this node.
		if (existing_node.status == AS_HB_MESH_NODE_ENDPOINT_UNKNOWN) {
			// Update the endpoint.
			endpoint_list_copy(&existing_node.endpoint_list,
					reply_ptr->endpoint_list);

			mesh_node_status_change(&existing_node,
					AS_HB_MESH_NODE_CHANNEL_INACTIVE);
			// Set the discovered flag.
			g_hb.mode_state.mesh_state.nodes_discovered = true;

			char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
			as_endpoint_list_to_string(existing_node.endpoint_list,
					endpoint_list_str, sizeof(endpoint_list_str));

			DEBUG("for node %" PRIx64" discovered endpoints {%s}",
					reply_ptr->nodeid, endpoint_list_str);

			// Update the hash.
			mesh_node_add_update(reply_ptr->nodeid, &existing_node);
		}

	NextReply:
		start_ptr += sizeof(as_hb_mesh_info_reply);
		size_t endpoint_list_size = 0;
		as_endpoint_list_sizeof(reply_ptr->endpoint_list, &endpoint_list_size);
		start_ptr += endpoint_list_size;
	}

	MESH_UNLOCK();
}

/**
 * Handle the case when a message is received on a channel.
 */
static void
mesh_channel_on_msg_rcvd(as_hb_channel_event* event)
{
	// Update the mesh node status.
	mesh_node_data_update(event);

	as_hb_msg_type msg_type;
	msg_type_get(event->msg, &msg_type);

	switch (msg_type) {
	case AS_HB_MSG_TYPE_PULSE:	// A pulse message. Try and discover new nodes.
		mesh_channel_on_pulse(event->msg);
		break;
	case AS_HB_MSG_TYPE_INFO_REQUEST:	// Send back an info reply.
		mesh_channel_on_info_request(event->msg);
		break;
	case AS_HB_MSG_TYPE_INFO_REPLY:	// Update the list of mesh nodes, if this is an undiscovered node.
		mesh_channel_on_info_reply(event->msg);
		break;
	default:
		WARNING("received a message of unknown type from");
		// Ignore other messages.
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Mesh public API
 * ----------------------------------------------------------------------------
 */

/**
 * Add a host / port to the mesh seed list.
 * @param host the seed node hostname / ip address
 * @param port the seed node port.
 * @param tls indicates TLS support.
 * @return CF_SHASH_OK, CF_SHASH_ERR, CF_SHASH_ERR_FOUND.
 */
static int
mesh_tip(char* host, int port, bool tls)
{
	MESH_LOCK();

	int rv = -1;
	as_hb_mesh_seed new_seed = { { 0 } };

	// Check validity of hostname and port.
	int hostname_len = strnlen(host, DNS_NAME_MAX_SIZE);
	if (hostname_len <= 0 || hostname_len == DNS_NAME_MAX_SIZE) {
		// Invalid hostname.
		WARNING("mesh seed host %s exceeds allowed %d characters", host,
				DNS_NAME_MAX_LEN);
		goto Exit;
	}
	if (port <= 0 || port > USHRT_MAX) {
		WARNING("mesh seed port %s:%d exceeds should be between 0 to %d", host,
				port, USHRT_MAX);
		goto Exit;
	}

	// Check if we already have a match for this seed.
	if (mesh_seed_find_unsafe(host, port) >= 0) {
		WARNING("mesh seed host %s:%d already in seed list", host, port);
		goto Exit;
	}

	mesh_seed_status_change(&new_seed, AS_HB_MESH_NODE_CHANNEL_INACTIVE);
	strcpy(new_seed.seed_host_name, host);
	new_seed.seed_port = port;
	new_seed.seed_tls = tls;

	cf_vector_append(&g_hb.mode_state.mesh_state.seeds, &new_seed);

	INFO("added new mesh seed %s:%d", host, port);
	rv = 0;

Exit:
	if (rv != 0) {
		// Ensure endpoint allocated space is freed.
		mesh_seed_destroy(&new_seed);
	}

	MESH_UNLOCK();
	return rv;
}

/**
 * Handle a channel event on an endpoint.
 */
static void
mesh_channel_event_process(as_hb_channel_event* event)
{
	// Skip if we are not in mesh mode.
	if (!hb_is_mesh()) {
		return;
	}

	MESH_LOCK();
	switch (event->type) {
	case AS_HB_CHANNEL_NODE_CONNECTED:
		// Ignore this event. The subsequent message event will be use for
		// determining mesh node active status.
		break;
	case AS_HB_CHANNEL_NODE_DISCONNECTED:
		mesh_channel_on_node_disconnect(event);
		break;
	case AS_HB_CHANNEL_MSG_RECEIVED:
		mesh_channel_on_msg_rcvd(event);
		break;
	case AS_HB_CHANNEL_CLUSTER_NAME_MISMATCH:	// Ignore this event. HB module will handle it.
		break;
	}

	MESH_UNLOCK();
}

/**
 * Initialize mesh mode data structures.
 */
static void
mesh_init()
{
	if (!hb_is_mesh()) {
		return;
	}

	MESH_LOCK();

	g_hb.mode_state.mesh_state.status = AS_HB_STATUS_STOPPED;

	// Initialize the mesh node hash.
	g_hb.mode_state.mesh_state.nodeid_to_mesh_node = cf_shash_create(
			cf_nodeid_shash_fn, sizeof(cf_node), sizeof(as_hb_mesh_node),
			AS_HB_CLUSTER_MAX_SIZE_SOFT, 0);

	// Initialize the seed list.
	cf_vector_init(&g_hb.mode_state.mesh_state.seeds, sizeof(as_hb_mesh_seed),
	AS_HB_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);

	MESH_UNLOCK();
}

/**
 * Delete the shash entries only if they are not seed entries.
 */
static int
mesh_free_node_data_reduce(const void* key, void* data, void* udata)
{
	as_hb_mesh_node* mesh_node = (as_hb_mesh_node*)data;
	mesh_node_destroy(mesh_node);
	return CF_SHASH_REDUCE_DELETE;
}

/**
 * Remove a host / port from the mesh list.
 */
static int
mesh_tip_clear_reduce(const void* key, void* data, void* udata)
{
	int rv = CF_SHASH_OK;

	MESH_LOCK();

	cf_node nodeid = *(cf_node*)key;
	as_hb_mesh_node* mesh_node = (as_hb_mesh_node*)data;
	as_hb_mesh_tip_clear_udata* tip_clear_udata =
			(as_hb_mesh_tip_clear_udata*)udata;

	if (tip_clear_udata == NULL || nodeid == tip_clear_udata->nodeid) {
		// Handling tip clear all or clear of a specific node.
		rv = CF_SHASH_REDUCE_DELETE;
		goto Exit;
	}

	// See if the address matches any one of the endpoints in the node's
	// endpoint list.
	for (int i = 0; i < tip_clear_udata->n_addrs; i++) {
		cf_sock_addr sock_addr;
		cf_ip_addr_copy(&tip_clear_udata->addrs[i], &sock_addr.addr);
		sock_addr.port = tip_clear_udata->port;
		as_hb_endpoint_list_addr_find_udata udata;
		udata.found = false;
		udata.to_search = &sock_addr;

		as_endpoint_list_iterate(mesh_node->endpoint_list,
				mesh_endpoint_addr_find_iterate, &udata);

		if (udata.found) {
			rv = CF_SHASH_REDUCE_DELETE;
			goto Exit;
		}
	}

	// Not found by endpoint.
	rv = CF_SHASH_OK;

Exit:
	if (rv == CF_SHASH_REDUCE_DELETE) {
		char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
		as_endpoint_list_to_string(mesh_node->endpoint_list, endpoint_list_str,
				sizeof(endpoint_list_str));

		// Find all seed entries matching this mesh entry and delete them.
		cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
		int element_count = cf_vector_size(seeds);
		for (int i = 0; i < element_count; i++) {
			as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
			if (seed->mesh_nodeid != nodeid) {
				// Does not match this mesh entry.
				continue;
			}
			if (mesh_seed_delete_unsafe(i) == 0) {
				i--;
				element_count--;
			}
			else {
				// Should not happen in practice.
				CRASH("error deleting mesh seed entry %s:%d",
						seed->seed_host_name, seed->seed_port);
			}
		}

		if (channel_node_disconnect(nodeid) != 0) {
			WARNING("unable to disconnect the channel to node %" PRIx64,
					nodeid);
		}

		mesh_node_destroy(mesh_node);
		if (tip_clear_udata != NULL) {
			tip_clear_udata->entry_deleted = true;
		}
	}

	MESH_UNLOCK();
	return rv;
}

/**
 * Output Heartbeat endpoints of peers.
 */
static int
mesh_peer_endpoint_reduce(const void* key, void* data, void* udata)
{
	int rv = CF_SHASH_OK;
	MESH_LOCK();
	cf_node nodeid = *(cf_node*)key;
	as_hb_mesh_node* mesh_node = (as_hb_mesh_node*)data;
	cf_dyn_buf* db = (cf_dyn_buf*)udata;

	cf_dyn_buf_append_string(db, "heartbeat.peer=");
	cf_dyn_buf_append_string(db, "node-id=");
	cf_dyn_buf_append_uint64_x(db, nodeid);
	cf_dyn_buf_append_string(db, ":");
	as_endpoint_list_info(mesh_node->endpoint_list, db);
	cf_dyn_buf_append_string(db, ";");

	MESH_UNLOCK();
	return rv;
}

/**
 * Free the mesh mode data structures.
 */
static void
mesh_clear()
{
	if (!mesh_is_stopped()) {
		WARNING(
				"attempted clearing mesh module without stopping it - skip mesh clear!");
		return;
	}

	MESH_LOCK();
	// Delete the elements from the map.
	cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
			mesh_free_node_data_reduce, NULL);

	// Reset the seeds to inactive state
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	for (int i = 0; i < element_count; i++) {
		// Should not happen in practice.
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
		seed->mesh_nodeid = 0;
		mesh_seed_status_change(seed, AS_HB_MESH_NODE_CHANNEL_INACTIVE);
	}

	MESH_UNLOCK();
}

/**
 * Open mesh listening socket. Crashes if open failed.
 */
static void
mesh_listening_sockets_open()
{
	MESH_LOCK();

	const cf_serv_cfg* bind_cfg = config_bind_cfg_get();

	// Compute min MTU across all binding interfaces.
	int min_mtu = -1;
	char addr_string[DNS_NAME_MAX_SIZE];
	for (uint32_t i = 0; i < bind_cfg->n_cfgs; ++i) {
		const cf_sock_cfg* sock_cfg = &bind_cfg->cfgs[i];
		cf_ip_addr_to_string_safe(&sock_cfg->addr, addr_string,
				sizeof(addr_string));

		INFO("initializing mesh heartbeat socket: %s:%d", addr_string,
				sock_cfg->port);

		int bind_interface_mtu =
				!cf_ip_addr_is_any(&sock_cfg->addr) ?
						cf_inter_mtu(&sock_cfg->addr) : cf_inter_min_mtu();

		if (min_mtu == -1 || min_mtu > bind_interface_mtu) {
			min_mtu = bind_interface_mtu;
		}
	}

	if (cf_socket_init_server((cf_serv_cfg*)bind_cfg,
			&g_hb.mode_state.mesh_state.listening_sockets) != 0) {
		CRASH("couldn't initialize unicast heartbeat sockets");
	}

	for (uint32_t i = 0;
			i < g_hb.mode_state.mesh_state.listening_sockets.n_socks; ++i) {
		DEBUG("opened mesh heartbeat socket: %d",
				CSFD(&g_hb.mode_state.mesh_state.listening_sockets.socks[i]));
	}

	if (min_mtu == -1) {
		WARNING("error getting the min MTU - using the default %d",
				DEFAULT_MIN_MTU);
		min_mtu = DEFAULT_MIN_MTU;
	}

	g_hb.mode_state.mesh_state.min_mtu = min_mtu;
	INFO("mtu of the network is %d", min_mtu);

	MESH_UNLOCK();
}

/**
 * Start mesh threads.
 */
static void
mesh_start()
{
	if (!hb_is_mesh()) {
		return;
	}

	MESH_LOCK();

	mesh_listening_sockets_open();
	channel_mesh_listening_socks_register(
			&g_hb.mode_state.mesh_state.listening_sockets);

	g_hb.mode_state.mesh_state.status = AS_HB_STATUS_RUNNING;

	// Start the mesh tender thread.
	g_hb.mode_state.mesh_state.mesh_tender_tid =
			cf_thread_create_joinable(mesh_tender, (void*)&g_hb);

	MESH_UNLOCK();
}

/**
 * Stop the mesh module.
 */
static void
mesh_stop()
{
	if (!mesh_is_running()) {
		WARNING("mesh is already stopped");
		return;
	}

	// Unguarded state, but this should be OK.
	g_hb.mode_state.mesh_state.status = AS_HB_STATUS_SHUTTING_DOWN;

	// Wait for the channel tender thread to finish.
	cf_thread_join(g_hb.mode_state.mesh_state.mesh_tender_tid);

	MESH_LOCK();

	channel_mesh_listening_socks_deregister(
			&g_hb.mode_state.mesh_state.listening_sockets);

	mesh_listening_sockets_close();

	g_hb.mode_state.mesh_state.status = AS_HB_STATUS_STOPPED;

	// Clear allocated state if any.
	if (g_hb.mode_state.mesh_state.published_endpoint_list) {
		cf_free(g_hb.mode_state.mesh_state.published_endpoint_list);
		g_hb.mode_state.mesh_state.published_endpoint_list = NULL;
	}

	MESH_UNLOCK();
}

/**
 * Reduce function to dump mesh node info to log file.
 */
static int
mesh_dump_reduce(const void* key, void* data, void* udata)
{
	cf_node nodeid = *(cf_node*)key;
	as_hb_mesh_node* mesh_node = (as_hb_mesh_node*)data;

	char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
	as_endpoint_list_to_string(mesh_node->endpoint_list, endpoint_list_str,
			sizeof(endpoint_list_str));

	INFO("\tHB Mesh Node: node-id %" PRIx64" status %s last-updated %" PRIu64 " endpoints {%s}",
			nodeid, mesh_node_status_string(mesh_node->status),
			mesh_node->last_status_updated, endpoint_list_str);

	return CF_SHASH_OK;
}

/**
 * Dump mesh state to logs.
 * @param verbose enables / disables verbose logging.
 */
static void
mesh_dump(bool verbose)
{
	if (!hb_is_mesh() || !verbose) {
		return;
	}

	MESH_LOCK();
	cf_vector* seeds = &g_hb.mode_state.mesh_state.seeds;
	int element_count = cf_vector_size(seeds);
	INFO("HB Seed Count %d", element_count);
	for (int i = 0; i < element_count; i++) {
		as_hb_mesh_seed* seed = cf_vector_getp(seeds, i);
		char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
		as_endpoint_list_to_string(seed->resolved_endpoint_list,
				endpoint_list_str, sizeof(endpoint_list_str));
		INFO("\tHB Mesh Seed: host %s port %d node-id %" PRIx64" status %s endpoints {%s}",
				seed->seed_host_name, seed->seed_port, seed->mesh_nodeid, mesh_node_status_string(seed->status),
				endpoint_list_str);
	}

	INFO("HB Mesh Nodes Count %d", cf_shash_get_size(g_hb.mode_state.mesh_state.nodeid_to_mesh_node));
	cf_shash_reduce(g_hb.mode_state.mesh_state.nodeid_to_mesh_node,
			mesh_dump_reduce, NULL);
	MESH_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Multicast sub module.
 * ----------------------------------------------------------------------------
 */

/**
 * Initialize multicast data structures.
 */
static void
multicast_init()
{
}

/**
 * Clear multicast data structures.
 */
static void
multicast_clear()
{
	// Free multicast data structures. Nothing to do.
}

/**
 * Open multicast sockets. Crashes if open failed.
 */
static void
multicast_listening_sockets_open()
{
	MULTICAST_LOCK();

	const cf_mserv_cfg* mserv_cfg = config_multicast_group_cfg_get();

	// Compute min MTU across all binding interfaces.
	int min_mtu = -1;
	char addr_string[DNS_NAME_MAX_SIZE];
	for (uint32_t i = 0; i < mserv_cfg->n_cfgs; ++i) {
		const cf_msock_cfg* sock_cfg = &mserv_cfg->cfgs[i];
		cf_ip_addr_to_string_safe(&sock_cfg->addr, addr_string,
				sizeof(addr_string));

		INFO("initializing multicast heartbeat socket: %s:%d", addr_string,
				sock_cfg->port);

		int bind_interface_mtu =
				!cf_ip_addr_is_any(&sock_cfg->if_addr) ?
						cf_inter_mtu(&sock_cfg->if_addr) : cf_inter_min_mtu();

		if (min_mtu == -1 || min_mtu > bind_interface_mtu) {
			min_mtu = bind_interface_mtu;
		}
	}

	if (cf_socket_mcast_init((cf_mserv_cfg*)mserv_cfg,
			&g_hb.mode_state.multicast_state.listening_sockets) != 0) {
		CRASH("couldn't initialize multicast heartbeat socket: %s",
				cf_strerror(errno));
	}

	for (uint32_t i = 0;
			i < g_hb.mode_state.multicast_state.listening_sockets.n_socks;
			++i) {
		DEBUG("opened multicast socket %d",
				CSFD(
						&g_hb.mode_state.multicast_state.listening_sockets.socks[i]));
	}

	if (min_mtu == -1) {
		WARNING("error getting the min mtu - using the default %d",
				DEFAULT_MIN_MTU);
		min_mtu = DEFAULT_MIN_MTU;
	}

	g_hb.mode_state.multicast_state.min_mtu = min_mtu;

	INFO("mtu of the network is %d", min_mtu);
	MULTICAST_UNLOCK();
}

/**
 * Start multicast module.
 */
static void
multicast_start()
{
	MULTICAST_LOCK();
	multicast_listening_sockets_open();
	channel_multicast_listening_socks_register(
			&g_hb.mode_state.multicast_state.listening_sockets);
	MULTICAST_UNLOCK();
}

/**
 * Close multicast listening socket.
 */
static void
multicast_listening_sockets_close()
{
	MULTICAST_LOCK();
	INFO("closing multicast heartbeat sockets");
	cf_sockets_close(&g_hb.mode_state.multicast_state.listening_sockets);
	DEBUG("closed multicast heartbeat socket");
	MULTICAST_UNLOCK();
}

/**
 * Stop Multicast.
 */
static void
multicast_stop()
{
	MULTICAST_LOCK();
	channel_multicast_listening_socks_deregister(
			&g_hb.mode_state.multicast_state.listening_sockets);
	multicast_listening_sockets_close();

	MULTICAST_UNLOCK();
}

/**
 * Dump multicast state to logs.
 * @param verbose enables / disables verbose logging.
 */
static void
multicast_dump(bool verbose)
{
	if (hb_is_mesh()) {
		return;
	}

	// Mode is multicast.
	INFO("HB Multicast TTL: %d", config_multicast_ttl_get());
}

/**
 * Find the maximum cluster size based on MTU of the network.
 *
 * num_nodes is computed so that
 *
 * MTU = compression_factor(fixed_size +	 num_nodesper_node_size)
 * where,
 * fixed_size = udp_header_size + msg_header_size +
 * sigma(per_plugin_fixed_size)
 * per_node_size = sigma(per_plugin_per_node_size).
 */
static int
multicast_supported_cluster_size_get()
{
	// Calculate the fixed size for a UDP packet and the message header.
	size_t msg_fixed_size = msg_get_template_fixed_sz(g_hb_msg_template,
			sizeof(g_hb_msg_template) / sizeof(msg_template));

	size_t msg_plugin_per_node_size = 0;

	for (int i = 0; i < AS_HB_PLUGIN_SENTINEL; i++) {
		// Adding plugin specific fixed size
		msg_fixed_size += g_hb.plugins[i].wire_size_fixed;
		// Adding plugin specific per node size.
		msg_plugin_per_node_size += g_hb.plugins[i].wire_size_per_node;
	}

	// TODO: Compute the max cluster size using max storage per node in cluster
	// and the min mtu.
	int supported_cluster_size = MAX(1,
			(((hb_mtu() - UDP_HEADER_SIZE_MAX) * MSG_COMPRESSION_RATIO)
					- msg_fixed_size) / msg_plugin_per_node_size);

	return supported_cluster_size;
}

/*
 * ----------------------------------------------------------------------------
 * Heartbeat main sub module.
 * ----------------------------------------------------------------------------
 */

/**
 * Is Main module initialized.
 */
static bool
hb_is_initialized()
{
	HB_LOCK();
	bool retval = (g_hb.status != AS_HB_STATUS_UNINITIALIZED) ? true : false;
	HB_UNLOCK();
	return retval;
}

/**
 * Is Main module running.
 */
static bool
hb_is_running()
{
	HB_LOCK();
	bool retval = (g_hb.status == AS_HB_STATUS_RUNNING) ? true : false;
	HB_UNLOCK();
	return retval;
}

/**
 * Is Main module stopped.
 */
static bool
hb_is_stopped()
{
	HB_LOCK();
	bool retval = (g_hb.status == AS_HB_STATUS_STOPPED) ? true : false;
	HB_UNLOCK();
	return retval;
}

/**
 * Initialize the mode specific data structures.
 */
static void
hb_mode_init()
{
	if (hb_is_mesh()) {
		mesh_init();
	}
	else {
		multicast_init();
	}
}

/**
 * Start mode specific threads..
 */
static void
hb_mode_start()
{
	if (hb_is_mesh()) {
		mesh_start();
	}
	else {
		multicast_start();
	}
}

/**
 * The MTU for underlying network.
 */
static int
hb_mtu()
{
	int __mtu = config_override_mtu_get();
	if (!__mtu) {
		__mtu = hb_is_mesh() ?
				g_hb.mode_state.mesh_state.min_mtu :
				g_hb.mode_state.multicast_state.min_mtu;
		__mtu = __mtu > 0 ? __mtu : DEFAULT_MIN_MTU;
	}
	return __mtu;
}

/**
 * Initialize the template to be used for heartbeat messages.
 */
static void
hb_msg_init()
{
	// Register fabric heartbeat msg type with no processing function:
	// This permits getting / putting heartbeat msgs to be moderated via an idle
	// msg queue.
	as_fabric_register_msg_fn(M_TYPE_HEARTBEAT, g_hb_msg_template,
			sizeof(g_hb_msg_template),
			AS_HB_MSG_SCRATCH_SIZE, 0, 0);
}

/**
 * Get hold of current heartbeat protocol version
 */
static uint32_t
hb_protocol_identifier_get()
{
	return HB_PROTOCOL_V3_IDENTIFIER;
}

/**
 * Node depart event time estimate. Assumes node departed timeout milliseconds
 * before the detection.
 */
static cf_clock
hb_node_depart_time(cf_clock detect_time)
{
	return (detect_time - HB_NODE_TIMEOUT());
}

/**
 * Indicates if mode is mesh.
 */
static bool
hb_is_mesh()
{
	return (config_mode_get() == AS_HB_MODE_MESH);
}

/**
 * Publish an event to subsystems listening to heart beat events.
 */
static void
hb_event_queue(as_hb_internal_event_type event_type, const cf_node* nodes,
		int node_count)
{
	// Lock-less because the queue is thread safe and we do not use heartbeat
	// state here.
	for (int i = 0; i < node_count; i++) {
		as_hb_event_node event;
		event.nodeid = nodes[i];
		event.event_detected_time = cf_getms();

		switch (event_type) {
		case AS_HB_INTERNAL_NODE_ARRIVE:
			event.evt = AS_HB_NODE_ARRIVE;
			event.event_time = event.event_detected_time;
			as_health_add_node_counter(event.nodeid, AS_HEALTH_NODE_ARRIVALS);
			break;
		case AS_HB_INTERNAL_NODE_DEPART:
			event.evt = AS_HB_NODE_DEPART;
			event.event_time = hb_node_depart_time(event.event_detected_time);
			break;
		case AS_HB_INTERNAL_NODE_EVICT:
			event.evt = AS_HB_NODE_DEPART;
			event.event_time = event.event_detected_time;
			break;
		case AS_HB_INTERNAL_NODE_ADJACENCY_CHANGED:
			event.evt = AS_HB_NODE_ADJACENCY_CHANGED;
			event.event_time = event.event_detected_time;
			break;
		}

		DEBUG("queuing event of type %d for node %" PRIx64, event.evt,
				event.nodeid);
		cf_queue_push(&g_hb_event_listeners.external_events_queue, &event);
	}
}

/**
 * Publish all pending events. Should be invoked outside hb locks.
 */
static void
hb_event_publish_pending()
{
	EXTERNAL_EVENT_PUBLISH_LOCK();
	int num_events = cf_queue_sz(&g_hb_event_listeners.external_events_queue);
	if (num_events <= 0) {
		// Events need not be published.
		goto Exit;
	}

	as_hb_event_node events[AS_HB_CLUSTER_MAX_SIZE_SOFT];
	int published_count = 0;
	while (published_count < AS_HB_CLUSTER_MAX_SIZE_SOFT
			&& cf_queue_pop(&g_hb_event_listeners.external_events_queue,
					&events[published_count], 0) == CF_QUEUE_OK) {
		published_count++;
	}

	if (published_count) {
		// Assuming that event listeners are not registered after system init,
		// no locks here.
		DEBUG("publishing %d heartbeat events", published_count);
		for (int i = 0; i < g_hb_event_listeners.event_listener_count; i++) {
			(g_hb_event_listeners.event_listeners[i].event_callback)(
					published_count, events,
					g_hb_event_listeners.event_listeners[i].udata);
		}
	}

Exit:
	EXTERNAL_EVENT_PUBLISH_UNLOCK();
}

/**
 * Delete the heap allocated data while iterating through the hash and deleting
 * entries.
 */
static int
hb_adjacency_free_data_reduce(const void* key, void* data, void* udata)
{
	as_hb_adjacent_node* adjacent_node = (as_hb_adjacent_node*)data;

	const cf_node* nodeid = (const cf_node*)key;

	hb_adjacent_node_destroy(adjacent_node);

	// Send event depart to for this node
	hb_event_queue(AS_HB_INTERNAL_NODE_DEPART, nodeid, 1);

	return CF_SHASH_REDUCE_DELETE;
}

/**
 * Clear the heartbeat data structures.
 */
static void
hb_clear()
{
	if (!hb_is_stopped()) {
		WARNING("attempted to clear heartbeat module without stopping it");
		return;
	}

	HB_LOCK();

	// Free the plugin data and delete adjacent nodes.
	cf_shash_reduce(g_hb.adjacency, hb_adjacency_free_data_reduce, NULL);
	cf_shash_reduce(g_hb.on_probation, hb_adjacency_free_data_reduce, NULL);
	hb_adjacent_node_destroy(&g_hb.self_node);
	memset(&g_hb.self_node, 0, sizeof(g_hb.self_node));

	HB_UNLOCK();

	// Publish node departed events for the removed nodes.
	hb_event_publish_pending();

	// Clear the mode module.
	if (hb_is_mesh()) {
		mesh_clear();
	}
	else {
		multicast_clear();
	}

	channel_clear();
}

/**
 * Reduce function to get hold of current adjacency list.
 */
static int
hb_adjacency_iterate_reduce(const void* key, void* data, void* udata)
{
	const cf_node* nodeid = (const cf_node*)key;
	as_hb_adjacency_reduce_udata* adjacency_reduce_udata =
			(as_hb_adjacency_reduce_udata*)udata;

	adjacency_reduce_udata->adj_list[adjacency_reduce_udata->adj_count] =
			*nodeid;
	adjacency_reduce_udata->adj_count++;

	return CF_SHASH_OK;
}

/**
 * Plugin function to set heartbeat adjacency list into a pulse message.
 */
static void
hb_plugin_set_fn(msg* msg)
{
	HB_LOCK();

	cf_node adj_list[cf_shash_get_size(g_hb.adjacency)];
	as_hb_adjacency_reduce_udata adjacency_reduce_udata = { adj_list, 0 };

	cf_shash_reduce(g_hb.adjacency, hb_adjacency_iterate_reduce,
			&adjacency_reduce_udata);

	HB_UNLOCK();

	// Populate adjacency list.
	msg_adjacency_set(msg, adj_list, adjacency_reduce_udata.adj_count);

	// Set cluster name.
	char cluster_name[AS_CLUSTER_NAME_SZ];
	as_config_cluster_name_get(cluster_name);

	if (cluster_name[0] != '\0') {
		msg_set_str(msg, AS_HB_MSG_CLUSTER_NAME, cluster_name, MSG_SET_COPY);
	}
}

/**
 * Plugin function that parses adjacency list out of a heartbeat pulse message.
 */
static void
hb_plugin_parse_data_fn(msg* msg, cf_node source,
		as_hb_plugin_node_data* prev_plugin_data,
		as_hb_plugin_node_data* plugin_data)
{
	size_t adj_length = 0;
	cf_node* adj_list = NULL;

	if (msg_adjacency_get(msg, &adj_list, &adj_length) != 0) {
		// Store a zero length adjacency list. Should not have happened.
		WARNING("received heartbeat without adjacency list %" PRIx64, source);
		adj_length = 0;
	}

	// The guess can be larger for older protocols which also include self node
	// in the adjacency list.
	int guessed_data_size = (adj_length * sizeof(cf_node));

	if (guessed_data_size > plugin_data->data_capacity) {
		// Round up to nearest multiple of block size to prevent very frequent
		// reallocation.
		size_t data_capacity = ((guessed_data_size + HB_PLUGIN_DATA_BLOCK_SIZE
				- 1) /
		HB_PLUGIN_DATA_BLOCK_SIZE) *
		HB_PLUGIN_DATA_BLOCK_SIZE;

		// Reallocate since we have outgrown existing capacity.
		plugin_data->data = cf_realloc(plugin_data->data, data_capacity);
		plugin_data->data_capacity = data_capacity;
	}

	cf_node* dest_list = (cf_node*)(plugin_data->data);

	size_t final_list_length = 0;
	for (size_t i = 0; i < adj_length; i++) {
		if (adj_list[i] == source) {
			// Skip the source node.
			continue;
		}
		dest_list[final_list_length++] = adj_list[i];
	}

	plugin_data->data_size = (final_list_length * sizeof(cf_node));
}

/**
 * Get the msg buffer from a pool based on the protocol under use.
 * @return the msg buff
 */
static msg*
hb_msg_get()
{
	return as_fabric_msg_get(M_TYPE_HEARTBEAT);
}

/**
 * Return the message buffer back to the pool.
 */
static void
hb_msg_return(msg* msg)
{
	as_fabric_msg_put(msg);
}

/**
 * Fill the outgoing pulse message with plugin specific data.
 *
 * Note: The set functions would be acquiring their locks. This function should
 * never directly use nor have a call stack under HB_LOCK.
 *
 * @param msg the outgoing pulse message.
 */
static void
hb_plugin_msg_fill(msg* msg)
{
	for (int i = 0; i < AS_HB_PLUGIN_SENTINEL; i++) {
		if (g_hb.plugins[i].set_fn) {
			(g_hb.plugins[i].set_fn)(msg);
		}
	}
}

/**
 * Parse fields from the message into plugin specific data.
 * @param msg the outgoing pulse message.
 * @param adjacent_node the node from which this message was received.
 * @param plugin_data_changed (output) array whose ith entry is set to true if
 * ith plugin's data changed, false otherwise. Should be large enough to hold
 * flags for all plugins.
 */
static void
hb_plugin_msg_parse(msg* msg, as_hb_adjacent_node* adjacent_node,
		as_hb_plugin* plugins, bool plugin_data_changed[])
{
	cf_node source;
	adjacent_node->plugin_data_cycler++;

	msg_nodeid_get(msg, &source);
	for (int i = 0; i < AS_HB_PLUGIN_SENTINEL; i++) {
		plugin_data_changed[i] = false;
		if (plugins[i].parse_fn) {
			as_hb_plugin_node_data* curr_data =
					&adjacent_node->plugin_data[i][adjacent_node->plugin_data_cycler
							% 2];

			as_hb_plugin_node_data* prev_data =
					&adjacent_node->plugin_data[i][(adjacent_node->plugin_data_cycler
							+ 1) % 2];

			// Ensure there is a preallocated data pointer.
			if (curr_data->data == NULL) {
				curr_data->data = cf_malloc(HB_PLUGIN_DATA_DEFAULT_SIZE);
				curr_data->data_capacity = HB_PLUGIN_DATA_DEFAULT_SIZE;
				curr_data->data_size = 0;
			}

			if (prev_data->data == NULL) {
				prev_data->data = cf_malloc(HB_PLUGIN_DATA_DEFAULT_SIZE);
				prev_data->data_capacity = HB_PLUGIN_DATA_DEFAULT_SIZE;
				prev_data->data_size = 0;
			}

			// Parse message data into current data.
			(plugins[i]).parse_fn(msg, source, prev_data, curr_data);

			if (!plugins[i].change_listener) {
				// No change listener configured. Skip detecting change.
				continue;
			}

			size_t curr_data_size = curr_data->data_size;
			void* curr_data_blob = curr_data_size ? curr_data->data : NULL;

			size_t prev_data_size = prev_data->data_size;
			void* prev_data_blob = prev_data_size ? prev_data->data : NULL;

			if (prev_data_blob == curr_data_blob) {
				// Old and new data both NULL or both point to the same memory
				// location.
				plugin_data_changed[i] = false;
				continue;
			}

			if (prev_data_size != curr_data_size || prev_data_blob == NULL
					|| curr_data_blob == NULL) {
				// Plugin data definitely changed, as the data sizes differ or
				// exactly one of old or new data pointers is NULL.
				plugin_data_changed[i] = true;
				continue;
			}

			// The data sizes match at this point and neither values are NULL.
			plugin_data_changed[i] = memcmp(prev_data_blob, curr_data_blob,
					curr_data_size) != 0;
		}
	}
}

/**
 * Adjacency list for an adjacent node changed.
 */
static void
hb_plugin_data_change_listener(cf_node changed_node_id)
{
	hb_event_queue(AS_HB_INTERNAL_NODE_ADJACENCY_CHANGED, &changed_node_id, 1);
}

/**
 * Initialize the plugin specific data structures.
 */
static void
hb_plugin_init()
{
	memset(&g_hb.plugins, 0, sizeof(g_hb.plugins));

	// Be cute. Register self as a plugin.
	as_hb_plugin self_plugin;
	memset(&self_plugin, 0, sizeof(self_plugin));
	self_plugin.id = AS_HB_PLUGIN_HB;
	self_plugin.wire_size_fixed = 0;
	self_plugin.wire_size_per_node = sizeof(cf_node);
	self_plugin.set_fn = hb_plugin_set_fn;
	self_plugin.parse_fn = hb_plugin_parse_data_fn;
	self_plugin.change_listener = hb_plugin_data_change_listener;
	hb_plugin_register(&self_plugin);
}

/**
 * Transmits heartbeats at fixed intervals.
 */
void*
hb_transmitter(void* arg)
{
	DETAIL("heartbeat transmitter started");

	cf_clock last_time = 0;

	while (hb_is_running()) {
		cf_clock curr_time = cf_getms();

		if ((curr_time - last_time) < PULSE_TRANSMIT_INTERVAL()) {
			// Interval has not been reached for sending heartbeats
			usleep(MIN(AS_HB_TX_INTERVAL_MS_MIN, (last_time +
			PULSE_TRANSMIT_INTERVAL()) - curr_time) * 1000);
			continue;
		}

		last_time = curr_time;

		// Construct the pulse message.
		msg* msg = hb_msg_get();

		msg_src_fields_fill(msg);
		msg_type_set(msg, AS_HB_MSG_TYPE_PULSE);

		// Have plugins fill their data into the heartbeat pulse message.
		hb_plugin_msg_fill(msg);

		// Broadcast the heartbeat to all known recipients.
		channel_msg_broadcast(msg);

		// Return the msg back to the fabric.
		hb_msg_return(msg);

		DETAIL("done sending pulse message");
	}

	DETAIL("heartbeat transmitter stopped");
	return NULL;
}

/**
 * Get hold of adjacent node information given its nodeid.
 * @param nodeid the nodeid.
 * @param adjacent_node the output node information.
 * @return 0 on success, -1 on failure.
 */
static int
hb_adjacent_node_get(cf_node nodeid, as_hb_adjacent_node* adjacent_node)
{
	int rv = -1;
	HB_LOCK();

	if (cf_shash_get(g_hb.adjacency, &nodeid, adjacent_node) == CF_SHASH_OK) {
		rv = 0;
	}

	HB_UNLOCK();
	return rv;
}

/**
 * Get hold of an on-probation node information given its nodeid.
 * @param nodeid the nodeid.
 * @param adjacent_node the output node information.
 * @return 0 on success, -1 on failure.
 */
static int
hb_on_probation_node_get(cf_node nodeid, as_hb_adjacent_node* adjacent_node)
{
	int rv = -1;
	HB_LOCK();

	if (cf_shash_get(g_hb.on_probation, &nodeid, adjacent_node)
			== CF_SHASH_OK) {
		rv = 0;
	}

	HB_UNLOCK();
	return rv;
}

/**
 * Read the plugin data from an adjacent node.
 * @param adjacent_node the adjacent node.
 * @param plugin_data (output) will be null if this node has no plugin data.
 * Else will point to the plugin data.
 * @param plugin_data_size (output) the size of the plugin data.
 */
static void
hb_adjacent_node_plugin_data_get(as_hb_adjacent_node* adjacent_node,
		as_hb_plugin_id plugin_id, void** plugin_data, size_t* plugin_data_size)
{
	*plugin_data_size =
			adjacent_node->plugin_data[plugin_id][adjacent_node->plugin_data_cycler
					% 2].data_size;

	*plugin_data =
			*plugin_data_size ?
					(cf_node*)(adjacent_node->plugin_data[plugin_id][adjacent_node->plugin_data_cycler
							% 2].data) : NULL;
}

/**
 * Get adjacency list for an adjacent node.
 */
static void
hb_adjacent_node_adjacency_get(as_hb_adjacent_node* adjacent_node,
		cf_node** adjacency_list, size_t* adjacency_length)
{
	hb_adjacent_node_plugin_data_get(adjacent_node, AS_HB_PLUGIN_HB,
			(void**)adjacency_list, adjacency_length);
	(*adjacency_length) /= sizeof(cf_node);
}

/**
 * Indicates if a give node has expired and should be removed from the adjacency
 * list.
 */
static bool
hb_node_has_expired(cf_node nodeid, as_hb_adjacent_node* adjacent_node)
{
	if (nodeid == config_self_nodeid_get()) {
		return false;
	}

	HB_LOCK();

	cf_clock now = cf_getms();

	bool expired = adjacent_node->last_updated_monotonic_ts + HB_NODE_TIMEOUT()
			< now;

	HB_UNLOCK();
	return expired;
}

/**
 * Indicates if self node has duplicate ids.
 */
static bool
hb_self_is_duplicate(){
	HB_LOCK();
	bool self_is_duplicate = g_hb.self_is_duplicate;
	HB_UNLOCK();
	return self_is_duplicate;
}

/**
 * Updates the self is duplicate flag.
 */
static void
hb_self_duplicate_update()
{
	cf_clock now = cf_getms();
	HB_LOCK();
	if (g_hb.self_is_duplicate) {
		uint32_t duplicate_block_interval =
			config_endpoint_track_intervals_get()
			* config_tx_interval_get();
		if (g_hb.self_duplicate_detected_ts + duplicate_block_interval <= now) {
			// We have not seen duplicates for the endpoint change tracking
			// interval. Mark ourself as non-duplicate.
			g_hb.self_is_duplicate = false;
		}
	}
	HB_UNLOCK();
}

/**
 * Free up space occupied by plugin data from adjacent node.
 */
static void
hb_adjacent_node_destroy(as_hb_adjacent_node* adjacent_node)
{
	HB_LOCK();
	for (int i = 0; i < AS_HB_PLUGIN_SENTINEL; i++) {
		as_hb_plugin_node_data* curr_plugin_data = adjacent_node->plugin_data[i];
		for (int j = 0; j < 2; j++) {
			if (curr_plugin_data[j].data) {
				cf_free(curr_plugin_data[j].data);
				curr_plugin_data[j].data = NULL;
			}

			curr_plugin_data[j].data_capacity = 0;
			curr_plugin_data[j].data_size = 0;
		}
	}

	if (adjacent_node->endpoint_list) {
		// Free the endpoint list.
		cf_free(adjacent_node->endpoint_list);
		adjacent_node->endpoint_list = NULL;
	}

	HB_UNLOCK();
}

/**
 * Tend reduce function that removes expired nodes from adjacency list.
 */
static int
hb_adjacency_tend_reduce(const void* key, void* data, void* udata)
{
	cf_node nodeid = *(const cf_node*)key;
	as_hb_adjacent_node* adjacent_node = (as_hb_adjacent_node*)data;
	as_hb_adjacency_tender_udata* adjacency_tender_udata =
			(as_hb_adjacency_tender_udata*)udata;

	int rv = CF_SHASH_OK;
	bool cluster_name_mismatch = adjacent_node->cluster_name_mismatch_count
			> CLUSTER_NAME_MISMATCH_MAX;
	if (hb_node_has_expired(nodeid, adjacent_node) || cluster_name_mismatch) {
		INFO("node expired %" PRIx64" %s", nodeid, cluster_name_mismatch ? "(cluster name mismatch)" : "");
		if (cluster_name_mismatch) {
			adjacency_tender_udata->evicted_nodes[adjacency_tender_udata->evicted_node_count++] =
					nodeid;
		}
		else {
			adjacency_tender_udata->dead_nodes[adjacency_tender_udata->dead_node_count++] =
					nodeid;
		}

		// Free plugin data as well.
		hb_adjacent_node_destroy(adjacent_node);

		rv = CF_SHASH_REDUCE_DELETE;
	}

	return rv;
}

/**
 * Tend reduce function that removes expired nodes from the probationary list.
 */
static int
hb_on_probation_tend_reduce(const void* key, void* data, void* udata)
{
	cf_node nodeid = *(const cf_node*)key;
	as_hb_adjacent_node* adjacent_node = (as_hb_adjacent_node*)data;

	int rv = CF_SHASH_OK;
	if (hb_node_has_expired(nodeid, adjacent_node)) {
		DEBUG("on-probation node %" PRIx64 " expired", nodeid);
		// Free plugin data as well.
		hb_adjacent_node_destroy(adjacent_node);
		rv = CF_SHASH_REDUCE_DELETE;
	}
	return rv;
}

/**
 * Tends the adjacency list. Removes nodes that expire.
 */
void*
hb_adjacency_tender(void* arg)
{
	DETAIL("adjacency tender started");

	cf_clock last_time = 0;
	cf_clock last_depart_time = 0;

	while (hb_is_running()) {
		cf_clock curr_time = cf_getms();
		uint32_t adjacency_tend_interval = ADJACENCY_TEND_INTERVAL;
		// Interval after node depart where we tend faster to detect additional
		// node departures.
		uint32_t fast_check_interval = 2 * config_tx_interval_get();
		if (last_depart_time + fast_check_interval > curr_time) {
			adjacency_tend_interval = ADJACENCY_FAST_TEND_INTERVAL;
		}

		hb_self_duplicate_update();

		if ((curr_time - last_time) < adjacency_tend_interval) {
			// Publish any pendng events.
			hb_event_publish_pending();

			// Interval has not been reached for sending heartbeats
			usleep(
					MIN(AS_HB_TX_INTERVAL_MS_MIN,
							(last_time + adjacency_tend_interval) - curr_time)
							* 1000);
			continue;
		}

		last_time = curr_time;

		DETAIL("tending adjacency list");

		HB_LOCK();
		cf_node dead_nodes[cf_shash_get_size(g_hb.adjacency)];
		cf_node evicted_nodes[cf_shash_get_size(g_hb.adjacency)];
		as_hb_adjacency_tender_udata adjacency_tender_udata;
		adjacency_tender_udata.dead_nodes = dead_nodes;
		adjacency_tender_udata.dead_node_count = 0;
		adjacency_tender_udata.evicted_nodes = evicted_nodes;
		adjacency_tender_udata.evicted_node_count = 0;

		cf_shash_reduce(g_hb.adjacency, hb_adjacency_tend_reduce,
				&adjacency_tender_udata);

		if (adjacency_tender_udata.dead_node_count > 0) {
			last_depart_time = curr_time;
			// Queue events for dead nodes.
			hb_event_queue(AS_HB_INTERNAL_NODE_DEPART, dead_nodes,
					adjacency_tender_udata.dead_node_count);
		}

		if (adjacency_tender_udata.evicted_node_count > 0) {
			last_depart_time = curr_time;
			// Queue events for evicted nodes.
			hb_event_queue(AS_HB_INTERNAL_NODE_EVICT, evicted_nodes,
					adjacency_tender_udata.evicted_node_count);
		}

		// Expire nodes from the on-probation list.
		cf_shash_reduce(g_hb.on_probation, hb_on_probation_tend_reduce, NULL);
		HB_UNLOCK();

		// See if we have pending events to publish.
		hb_event_publish_pending();

		DETAIL("done tending adjacency list");
	}

	DETAIL("adjacency tender shut down");
	return NULL;
}

/**
 * Start the transmitter thread.
 */
static void
hb_tx_start()
{
	// Start the transmitter thread.
	g_hb.transmitter_tid = cf_thread_create_joinable(hb_transmitter,
			(void*)&g_hb);
}

/**
 * Stop the transmitter thread.
 */
static void
hb_tx_stop()
{
	DETAIL("waiting for the transmitter thread to stop");
	// Wait for the adjacency tender thread to stop.
	cf_thread_join(g_hb.transmitter_tid);
}

/**
 * Start the transmitter thread.
 */
static void
hb_adjacency_tender_start()
{
	// Start the transmitter thread.
	g_hb.adjacency_tender_tid = cf_thread_create_joinable(hb_adjacency_tender,
			(void*)&g_hb);
}

/**
 * Stop the adjacency tender thread.
 */
static void
hb_adjacency_tender_stop()
{
	// Wait for the adjacency tender thread to stop.
	cf_thread_join(g_hb.adjacency_tender_tid);
}

/**
 * Initialize the heartbeat subsystem.
 */
static void
hb_init()
{
	if (hb_is_initialized()) {
		WARNING("heartbeat main module is already initialized");
		return;
	}

	// Operate under a lock. Let's be paranoid everywhere.
	HB_LOCK();

	// Initialize the heartbeat data structure.
	memset(&g_hb, 0, sizeof(g_hb));

	// Initialize the adjacency hash.
	g_hb.adjacency = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node),
			sizeof(as_hb_adjacent_node), AS_HB_CLUSTER_MAX_SIZE_SOFT, 0);

	// Initialize the on_probation hash.
	g_hb.on_probation = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node),
			sizeof(as_hb_adjacent_node), AS_HB_CLUSTER_MAX_SIZE_SOFT, 0);

	// Initialize the temporary hash to map nodeid to index.
	g_hb.nodeid_to_index = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node),
			sizeof(int), AS_HB_CLUSTER_MAX_SIZE_SOFT, 0);

	// Initialize unpublished event queue.
	cf_queue_init(&g_hb_event_listeners.external_events_queue,
			sizeof(as_hb_event_node),
			AS_HB_CLUSTER_MAX_SIZE_SOFT, true);

	// Initialize the mode specific state.
	hb_mode_init();

	// Initialize the plugin functions.
	hb_plugin_init();

	// Initialize IO channel subsystem.
	channel_init();

	g_hb.status = AS_HB_STATUS_STOPPED;

	HB_UNLOCK();
}

/**
 * Start the heartbeat subsystem.
 */
static void
hb_start()
{
	// Operate under a lock. Let's be paranoid everywhere.
	HB_LOCK();

	if (hb_is_running()) {
		// Shutdown the heartbeat subsystem.
		hb_stop();
	}

	g_hb.status = AS_HB_STATUS_RUNNING;

	// Initialize the heartbeat message templates. Called from here because
	// fabric needs to be initialized for this call to succeed. Fabric init
	// happens after heartbeat init.
	hb_msg_init();

	// Initialize channel sub module.
	channel_start();

	// Start the mode sub module
	hb_mode_start();

	// Start heart beat transmitter.
	hb_tx_start();

	// Start heart beat adjacency tender.
	hb_adjacency_tender_start();

	HB_UNLOCK();
}

/**
 * Shut down the heartbeat subsystem.
 */
static void
hb_stop()
{
	if (!hb_is_running()) {
		WARNING("heartbeat is already stopped");
		return;
	}

	HB_LOCK();
	g_hb.status = AS_HB_STATUS_SHUTTING_DOWN;
	HB_UNLOCK();

	// Publish pending events. Should not delay any events.
	hb_event_publish_pending();

	// Shutdown mode.
	if (hb_is_mesh()) {
		mesh_stop();
	}
	else {
		multicast_stop();
	}

	// Wait for the threads to shut down.
	hb_tx_stop();

	hb_adjacency_tender_stop();

	// Stop channels.
	channel_stop();

	g_hb.status = AS_HB_STATUS_STOPPED;
}

/**
 * Register a plugin with the heart beat system.
 */
static void
hb_plugin_register(as_hb_plugin* plugin)
{
	HB_LOCK();
	memcpy(&g_hb.plugins[plugin->id], plugin, sizeof(as_hb_plugin));
	HB_UNLOCK();
}

/**
 * Check if the heartbeat recieved is duplicate or stale.
 */
static bool
hb_msg_is_obsolete(as_hb_channel_event* event, as_hlc_timestamp last_send_ts)
{
	if (as_hlc_timestamp_order_get(event->msg_hlc_ts.send_ts, last_send_ts)
			== AS_HLC_HAPPENS_BEFORE) {
		// Received a delayed heartbeat send before the current heartbeat.
		return true;
	}
	return false;
}

/**
 * Update the tracker with endpoint change status.
 */
static void
hb_endpoint_change_tracker_update(uint64_t* tracker, bool endpoint_changed)
{
	*tracker = *tracker << 1;
	if (endpoint_changed) {
		(*tracker)++;
	}
}

/**
 * Indicates if endpoint changes for this node are normal.
 */
static bool
hb_endpoint_change_tracker_is_normal(uint64_t tracker)
{
	if (tracker == 0) {
		// Normal and healthy case.
		return true;
	}

	uint32_t num_intervals_to_track = MIN(64,
			config_endpoint_track_intervals_get());
	uint64_t mask = ~(~(uint64_t)0 << num_intervals_to_track);

	// Ignore older history.
	tracker &= mask;

	int flip_count = 0;
	static int nibblebits[] = { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };
	for (; tracker != 0; tracker >>= 4) {
		flip_count += nibblebits[tracker & 0x0f];
	}

	return flip_count <= config_endpoint_changes_allowed_get();
}


/**
 * Indicates if the change tracker just changed.
 */
static bool
hb_endpoint_change_tracker_has_changed(uint64_t tracker)
{
	return tracker % 2;
}

/**
 * Update adjacent node data on receiving a valid pulse message.
 *
 * @return 0 if the update was successfully applied, -1 if the update should be
 * rejected.
 */
static int
hb_adjacent_node_update(as_hb_channel_event* msg_event,
		as_hb_adjacent_node* adjacent_node, bool plugin_data_changed[])
{
	msg* msg = msg_event->msg;

	cf_node source = 0;
	// Channel has validated the source. Don't bother checking here.
	msg_nodeid_get(msg, &source);

	msg_id_get(msg, &adjacent_node->protocol_version);

	as_hlc_timestamp send_ts = adjacent_node->last_msg_hlc_ts.send_ts;

	if (hb_endpoint_change_tracker_has_changed(
			adjacent_node->endpoint_change_tracker)) {
		// Allow a little more slack for obsolete checking because the two nodes
		// might not have matching send timestamps.
		send_ts = as_hlc_timestamp_subtract_ms(send_ts,
				config_tx_interval_get());
	}

	if (hb_msg_is_obsolete(msg_event, send_ts)) {
		WARNING("ignoring delayed heartbeat - expected timestamp less than %" PRIu64" but was  %" PRIu64 " from node: %" PRIx64,
				send_ts,
				msg_event->msg_hlc_ts.send_ts, source);
		return -1;
	}

	// Populate plugin data.
	hb_plugin_msg_parse(msg, adjacent_node, g_hb.plugins, plugin_data_changed);

	// Get the ip address.
	as_endpoint_list* msg_endpoint_list;
	if (msg_endpoint_list_get(msg, &msg_endpoint_list) == 0
			&& !as_endpoint_lists_are_equal(adjacent_node->endpoint_list,
					msg_endpoint_list)) {
		// Update the endpoints.
		endpoint_list_copy(&adjacent_node->endpoint_list, msg_endpoint_list);
	}

	// Update the last updated time.
	adjacent_node->last_updated_monotonic_ts = cf_getms();
	memcpy(&adjacent_node->last_msg_hlc_ts, &msg_event->msg_hlc_ts,
			sizeof(adjacent_node->last_msg_hlc_ts));

	// Update the latency.
	int64_t latency = as_hlc_timestamp_diff_ms(msg_event->msg_hlc_ts.send_ts,
			msg_event->msg_hlc_ts.recv_ts);
	latency = latency < 0 ? -latency : latency;
	adjacent_node->avg_latency = ALPHA * latency
			+ (1 - ALPHA) * adjacent_node->avg_latency;

	// Reset the cluster-name mismatch counter to zero.
	adjacent_node->cluster_name_mismatch_count = 0;

	// Check if fabric endpoints have changed.
	as_hb_plugin_node_data* curr_data =
			&adjacent_node->plugin_data[AS_HB_PLUGIN_FABRIC][adjacent_node->plugin_data_cycler
					% 2];

	as_hb_plugin_node_data* prev_data =
			&adjacent_node->plugin_data[AS_HB_PLUGIN_FABRIC][(adjacent_node->plugin_data_cycler
					+ 1) % 2];

	as_endpoint_list* curr_fabric_endpoints =
			as_fabric_hb_plugin_get_endpoint_list(curr_data);
	as_endpoint_list* prev_fabric_endpoints =
			as_fabric_hb_plugin_get_endpoint_list(prev_data);

	// Endpoints changed if this is not the first update and if the endpoint
	// lists do not match.
	bool endpoints_changed = prev_fabric_endpoints != NULL
			&& !as_endpoint_lists_are_equal(curr_fabric_endpoints,
					prev_fabric_endpoints);

	if (endpoints_changed) {
		char curr_fabric_endpoints_str[ENDPOINT_LIST_STR_SIZE];
		char prev_fabric_endpoints_str[ENDPOINT_LIST_STR_SIZE];

		as_endpoint_list_to_string(curr_fabric_endpoints,
				curr_fabric_endpoints_str, sizeof(curr_fabric_endpoints_str));
		as_endpoint_list_to_string(prev_fabric_endpoints,
				prev_fabric_endpoints_str, sizeof(prev_fabric_endpoints_str));

		TICKER_WARNING("node: %"PRIx64" fabric endpoints changed from {%s} to {%s}", source, prev_fabric_endpoints_str, curr_fabric_endpoints_str);
	}

	hb_endpoint_change_tracker_update(&adjacent_node->endpoint_change_tracker,
			endpoints_changed);

	return 0;
}

/**
 * Indicates if a node can be considered adjacent, based on accumulated
 * statistics.
 */
static bool
hb_node_can_consider_adjacent(as_hb_adjacent_node* adjacent_node)
{
	return hb_endpoint_change_tracker_is_normal(
			adjacent_node->endpoint_change_tracker);
}

/**
 * Process a pulse from source having our node-id.
 */
static void
hb_channel_on_self_pulse(as_hb_channel_event* msg_event)
{
	bool plugin_data_changed[AS_HB_PLUGIN_SENTINEL] = { 0 };

	HB_LOCK();
	if (hb_adjacent_node_update(msg_event, &g_hb.self_node, plugin_data_changed)
			!= 0) {
		goto Exit;
	}

	as_hb_plugin_node_data* curr_data =
			&g_hb.self_node.plugin_data[AS_HB_PLUGIN_FABRIC][g_hb.self_node.plugin_data_cycler
					% 2];
	as_endpoint_list* curr_fabric_endpoints =
			as_fabric_hb_plugin_get_endpoint_list(curr_data);

	if (!as_fabric_is_published_endpoint_list(curr_fabric_endpoints)) {
		// Mark self as having duplicate node-id.
		g_hb.self_is_duplicate = true;
		g_hb.self_duplicate_detected_ts = cf_getms();

		// Found another node with duplicate node-id.
		char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
		as_endpoint_list_to_string(curr_fabric_endpoints, endpoint_list_str,
				sizeof(endpoint_list_str));
		TICKER_WARNING("duplicate node-id: %" PRIx64 " with fabric endpoints {%s}", config_self_nodeid_get(), endpoint_list_str);
	}
	else {
		cf_atomic_int_incr(&g_stats.heartbeat_received_self);
	}

Exit:
	HB_UNLOCK();
}

/**
 * Process an incoming pulse message.
 */
static void
hb_channel_on_pulse(as_hb_channel_event* msg_event)
{
	msg* msg = msg_event->msg;
	cf_node source;

	// Print cluster breach only once per second.
	static cf_clock last_cluster_breach_print = 0;

	// Channel has validated the source. Don't bother checking here.
	msg_nodeid_get(msg, &source);

	if (source == config_self_nodeid_get()) {
		hb_channel_on_self_pulse(msg_event);
		// Ignore self heartbeats.
		return;
	}

	HB_LOCK();

	as_hb_adjacent_node adjacent_node = { 0 };

	bool plugin_data_changed[AS_HB_PLUGIN_SENTINEL] = { 0 };
	bool is_in_adjacency = (hb_adjacent_node_get(source, &adjacent_node) == 0);
	bool should_be_on_probation = false;

	if (!is_in_adjacency) {
		hb_on_probation_node_get(source, &adjacent_node);
	}

	// Update the adjacent node with contents of the message.
	if (hb_adjacent_node_update(msg_event, &adjacent_node, plugin_data_changed)
			!= 0) {
		// Update rejected.
		goto Exit;
	}

	// Check if this node needs to be on probation.
	should_be_on_probation = !hb_node_can_consider_adjacent(&adjacent_node);

	cf_atomic_int_incr(&g_stats.heartbeat_received_foreign);

	bool is_new = !should_be_on_probation && !is_in_adjacency;

	if (is_new) {
		int mcsize = config_mcsize();
		// Note: adjacency list does not contain self node hence
		// (mcsize - 1) in the check.
		if (cf_shash_get_size(g_hb.adjacency) >= (mcsize - 1)) {
			if (last_cluster_breach_print != (cf_getms() / 1000L)) {
				WARNING("ignoring node: %" PRIx64" - exceeding maximum supported cluster size %d",
						source, mcsize);
				last_cluster_breach_print = cf_getms() / 1000L;
			}
			goto Exit;
		}
	}

	// Move the node to appropriate hash.
	cf_shash_put(should_be_on_probation ? g_hb.on_probation : g_hb.adjacency,
			&source, &adjacent_node);

	// Maintain mutual exclusion between adjacency and on_probation hashes.
	cf_shash_delete(should_be_on_probation ? g_hb.adjacency : g_hb.on_probation,
			&source);

	if (is_new) {
		// Publish event if this is a new node.
		INFO("node arrived %" PRIx64, source);
		hb_event_queue(AS_HB_INTERNAL_NODE_ARRIVE, &source, 1);
	}
	else if (should_be_on_probation && is_in_adjacency) {
		// This node needs to be on probation, most likely due to duplicate
		// node-ids.
		WARNING("node expired %" PRIx64" - potentially duplicate node-id", source);
		hb_event_queue(AS_HB_INTERNAL_NODE_DEPART, &source, 1);
	}

Exit:
	HB_UNLOCK();

	// Publish any pending node arrival events.
	hb_event_publish_pending();

	if (!should_be_on_probation) {
		// Call plugin change listeners outside of a lock to prevent deadlocks.
		for (int i = 0; i < AS_HB_PLUGIN_SENTINEL; i++) {
			if (plugin_data_changed[i] && g_hb.plugins[i].change_listener) {
				// Notify that data for this plugin for the source node has
				// changed.
				DETAIL("plugin data for node %" PRIx64" changed for plugin %d",
						source, i);
				(g_hb.plugins[i]).change_listener(source);
			}
		}
	}
}

/**
 * Process an incoming heartbeat message.
 */
static void
hb_channel_on_msg_rcvd(as_hb_channel_event* event)
{
	msg* msg = event->msg;
	as_hb_msg_type type;
	msg_type_get(msg, &type);

	switch (type) {
	case AS_HB_MSG_TYPE_PULSE:	// A pulse message. Update the adjacent node data.
		hb_channel_on_pulse(event);
		break;
	default:	// Ignore other messages.
		break;
	}
}

/**
 * Increase the cluster-name mismatch counter the node.
 */
static void
hb_handle_cluster_name_mismatch(as_hb_channel_event* event)
{
	HB_LOCK();

	as_hb_adjacent_node adjacent_node;
	memset(&adjacent_node, 0, sizeof(adjacent_node));

	if (hb_adjacent_node_get(event->nodeid, &adjacent_node) != 0) {
		// Node does not exist in the adjacency list
		goto Exit;
	}

	if (hb_msg_is_obsolete(event, adjacent_node.last_msg_hlc_ts.send_ts)) {
		WARNING("ignoring delayed heartbeat - expected timestamp less than %" PRIu64" but was  %" PRIu64 " from node: %" PRIx64,
				adjacent_node.last_msg_hlc_ts.send_ts,
				event->msg_hlc_ts.send_ts, event->nodeid);
		goto Exit;
	}

	// Update the cluster_name_mismatch counter.
	adjacent_node.cluster_name_mismatch_count++;
	cf_shash_put(g_hb.adjacency, &event->nodeid, &adjacent_node);
Exit:
	HB_UNLOCK();
}

/**
 * Process channel events.
 */
static void
hb_channel_event_process(as_hb_channel_event* event)
{
	// Deal with pulse messages here.
	switch (event->type) {
	case AS_HB_CHANNEL_MSG_RECEIVED:
		hb_channel_on_msg_rcvd(event);
		break;
	case AS_HB_CHANNEL_CLUSTER_NAME_MISMATCH:
		hb_handle_cluster_name_mismatch(event);
		break;
	default:	// Ignore channel active and inactive events. Rather rely on the adjacency
	// tender to expire nodes.
		break;
	}
}

/**
 * Dump hb mode state to logs.
 * @param verbose enables / disables verbose logging.
 */
static void
hb_mode_dump(bool verbose)
{
	if (hb_is_mesh()) {
		mesh_dump(verbose);
	}
	else {
		multicast_dump(verbose);
	}
}

/**
 * Reduce function to dump hb node info to log file.
 */
static int
hb_dump_reduce(const void* key, void* data, void* udata)
{
	const cf_node* nodeid = (const cf_node*)key;
	as_hb_adjacent_node* adjacent_node = (as_hb_adjacent_node*)data;

	char endpoint_list_str[ENDPOINT_LIST_STR_SIZE];
	as_endpoint_list_to_string(adjacent_node->endpoint_list, endpoint_list_str,
			sizeof(endpoint_list_str));

	INFO("\tHB %s Node: node-id %" PRIx64" protocol %" PRIu32" endpoints {%s} last-updated %" PRIu64 " latency-ms %" PRIu64 ,
			(char*)udata,
			*nodeid, adjacent_node->protocol_version, endpoint_list_str,
			adjacent_node->last_updated_monotonic_ts, adjacent_node->avg_latency);

	return CF_SHASH_OK;
}

/**
 * Dump hb state to logs.
 * @param verbose enables / disables verbose logging.
 */
static void
hb_dump(bool verbose)
{
	HB_LOCK();

	INFO("HB Adjacency Size: %d", cf_shash_get_size(g_hb.adjacency));

	if (verbose) {
		cf_shash_reduce(g_hb.adjacency, hb_dump_reduce, "Adjacent");
	}

	if (cf_shash_get_size(g_hb.on_probation)) {
		INFO("HB On-probation Size: %d", cf_shash_get_size(g_hb.on_probation));

		if (verbose) {
			cf_shash_reduce(g_hb.on_probation, hb_dump_reduce, "On-probation");
		}
	}

	HB_UNLOCK();
}

/**
 * Compute a complement / inverted adjacency graph for input nodes such that
 * entry
 *
 * inverted_graph[i][j] = 0 iff node[i] and node[j] are in each others adjacency
 * lists. That is they have a bidirectional network link active between them.
 *
 * else
 *
 * inverted_graph[i][j] > 0 iff there is no link or a unidirectional link
 * between them.
 *
 *
 * @param nodes the input vector of nodes.
 * @param inverted_graph (output) a (num_nodes x num_nodes ) 2D byte array.
 */
static void
hb_adjacency_graph_invert(cf_vector* nodes, uint8_t** inverted_graph)
{
	HB_LOCK();
	int num_nodes = cf_vector_size(nodes);

	for (int i = 0; i < num_nodes; i++) {
		for (int j = 0; j < num_nodes; j++) {
			inverted_graph[i][j] = 2;
		}
		cf_node nodeid = 0;
		cf_vector_get(nodes, i, &nodeid);
		cf_shash_put(g_hb.nodeid_to_index, &nodeid, &i);
	}

	cf_node self_nodeid = config_self_nodeid_get();
	int self_node_index = -1;
	cf_shash_get(g_hb.nodeid_to_index, &self_nodeid, &self_node_index);

	for (int i = 0; i < num_nodes; i++) {
		// Mark the node connected from itself, i.e, disconnected in the
		// inverted graph.
		inverted_graph[i][i] = 0;

		cf_node node = *(cf_node*)cf_vector_getp(nodes, i);
		as_hb_adjacent_node node_info;

		if (hb_adjacent_node_get(node, &node_info) == 0) {
			if (self_node_index >= 0) {
				// Self node will not have plugin data. But the fact that this
				// node has an adjacent node indicates that is is in our
				// adjacency list. Adjust the graph.
				inverted_graph[i][self_node_index]--;
				inverted_graph[self_node_index][i]--;
			}

			cf_node* adjacency_list = NULL;
			size_t adjacency_length = 0;
			hb_adjacent_node_adjacency_get(&node_info, &adjacency_list, &adjacency_length);

			for (int j = 0; j < adjacency_length; j++) {
				int other_node_index = -1;
				cf_shash_get(g_hb.nodeid_to_index, &adjacency_list[j],
						&other_node_index);
				if (other_node_index < 0) {
					// This node is not in the input set of nodes.
					continue;
				}

				if (i != other_node_index) {
					inverted_graph[i][other_node_index]--;
					inverted_graph[other_node_index][i]--;
				}
			}
		}
	}

	// Cleanup the temporary hash.
	cf_shash_delete_all(g_hb.nodeid_to_index);

	HB_UNLOCK();
}

/**
 * Compute the nodes to evict from the input nodes so that remaining nodes form
 * a clique, based on adjacency lists using minimal vertex cover.
 *
 * The minimal vertex cover on this graph is the set of nodes that should be
 * removed to result in  a clique on the remaining nodes. This implementation is
 * an approximation of the minimal vertex cover. The notion is to keep removing
 * vertices having the highest degree until there are no more edges remaining.
 * The heuristic gets rid of the more problematic nodes first.
 *
 * @param nodes input cf_node vector.
 * @param nodes_to_evict output cf_node clique array, that is initialized.
 */
static void
hb_maximal_clique_evict(cf_vector* nodes, cf_vector* nodes_to_evict)
{
	int num_nodes = cf_vector_size(nodes);

	if (num_nodes == 0) {
		// Nothing to do.
		return;
	}

	int graph_alloc_size = sizeof(uint8_t) * num_nodes * num_nodes;
	void* graph_data = MSG_BUFF_ALLOC(graph_alloc_size);

	if (!graph_data) {
		CRASH("error allocating space for clique finding data structure");
	}

	uint8_t* inverted_graph[num_nodes];
	inverted_graph[0] = graph_data;
	for (int i = 1; i < num_nodes; i++) {
		inverted_graph[i] = *inverted_graph + num_nodes * i;
	}

	hb_adjacency_graph_invert(nodes, inverted_graph);

	// Count the number of edges in the inverted graph. These edges are the ones
	// that need to be removed so that the remaining nodes form a clique in the
	// adjacency graph. Also for performance get hold of the self node index in
	// the nodes vector.
	int edge_count = 0;
	int self_node_index = -1;
	for (int i = 0; i < num_nodes; i++) {
		cf_node node = 0;
		cf_vector_get(nodes, i, &node);
		if (node == config_self_nodeid_get()) {
			self_node_index = i;
		}

		for (int j = 0; j < num_nodes; j++) {
			if (inverted_graph[i][j]) {
				edge_count++;
			}
		}
	}

	cf_vector_delete_range(nodes_to_evict, 0,
			cf_vector_size(nodes_to_evict) - 1);

	// Since we always decide to retain self node, first get rid of all nodes
	// having missing links to self node.
	if (self_node_index >= 0) {
		for (int i = 0; i < num_nodes; i++) {
			if (inverted_graph[self_node_index][i]
					|| inverted_graph[i][self_node_index]) {
				cf_node to_evict = 0;
				cf_vector_get(nodes, i, &to_evict);
				DEBUG("marking node %" PRIx64" for clique based eviction",
						to_evict);

				cf_vector_append(nodes_to_evict, &to_evict);

				// Remove all edges attached to the removed node.
				for (int j = 0; j < num_nodes; j++) {
					if (inverted_graph[i][j]) {
						inverted_graph[i][j] = 0;
						edge_count--;
					}
					if (inverted_graph[j][i]) {
						inverted_graph[j][i] = 0;
						edge_count--;
					}
				}
			}
		}
	}

	while (edge_count > 0) {
		// Find vertex with highest degree.
		cf_node max_degree_node = 0;
		int max_degree_node_idx = -1;
		int max_degree = 0;

		for (int i = 0; i < num_nodes; i++) {
			cf_node to_evict = 0;
			cf_vector_get(nodes, i, &to_evict);

			if (vector_find(nodes_to_evict, &to_evict) >= 0) {
				// We have already decided to evict this node.
				continue;
			}

			if (to_evict == config_self_nodeid_get()) {
				// Do not evict self.
				continue;
			}

			// Get the degree of this node.
			int degree = 0;
			for (int j = 0; j < num_nodes; j++) {
				if (inverted_graph[i][j]) {
					degree++;
				}
			}

			DETAIL("inverted degree for node %" PRIx64" is %d",
					to_evict, degree);

			// See if this node has a higher degree. On ties choose the node
			// with a smaller nodeid
			if (degree > max_degree
					|| (degree == max_degree && max_degree_node > to_evict)) {
				max_degree = degree;
				max_degree_node = to_evict;
				max_degree_node_idx = i;
			}
		}

		if (max_degree_node_idx < 0) {
			// We are done no node to evict.
			break;
		}

		DEBUG("marking node %" PRIx64" with degree %d for clique based eviction",
				max_degree_node, max_degree);

		cf_vector_append(nodes_to_evict, &max_degree_node);

		// Remove all edges attached to the removed node.
		for (int i = 0; i < num_nodes; i++) {
			if (inverted_graph[max_degree_node_idx][i]) {
				inverted_graph[max_degree_node_idx][i] = 0;
				edge_count--;
			}
			if (inverted_graph[i][max_degree_node_idx]) {
				inverted_graph[i][max_degree_node_idx] = 0;
				edge_count--;
			}
		}
	}

	MSG_BUFF_FREE(graph_data, graph_alloc_size);
}

/**
 * Reduce function to iterate over plugin data for all adjacent nodes.
 */
static int
hb_plugin_data_iterate_reduce(const void* key, void* data, void* udata)
{
	const cf_node* nodeid = (const cf_node*)key;
	as_hb_adjacent_node* adjacent_node = (as_hb_adjacent_node*)data;
	as_hb_adjacecny_iterate_reduce_udata* reduce_udata =
			(as_hb_adjacecny_iterate_reduce_udata*)udata;

	size_t plugin_data_size =
			adjacent_node->plugin_data[reduce_udata->pluginid][adjacent_node->plugin_data_cycler
					% 2].data_size;
	void* plugin_data =
			plugin_data_size ?
					adjacent_node->plugin_data[reduce_udata->pluginid][adjacent_node->plugin_data_cycler
							% 2].data : NULL;

	reduce_udata->iterate_fn(*nodeid, plugin_data, plugin_data_size,
			adjacent_node->last_updated_monotonic_ts,
			&adjacent_node->last_msg_hlc_ts, reduce_udata->udata);

	return CF_SHASH_OK;
}

/**
 * Call the iterate method on all nodes in current adjacency list. Note plugin
 * data can still be NULL if the plugin data failed to parse the plugin data.
 *
 * @param pluginid the plugin identifier.
 * @param iterate_fn the iterate function invoked for plugin data forevery node.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration. NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
static void
hb_plugin_data_iterate_all(as_hb_plugin_id pluginid,
		as_hb_plugin_data_iterate_fn iterate_fn, void* udata)
{
	HB_LOCK();

	as_hb_adjacecny_iterate_reduce_udata reduce_udata;
	reduce_udata.pluginid = pluginid;
	reduce_udata.iterate_fn = iterate_fn;
	reduce_udata.udata = udata;
	cf_shash_reduce(g_hb.adjacency, hb_plugin_data_iterate_reduce,
			&reduce_udata);

	HB_UNLOCK();
}
