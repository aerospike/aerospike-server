/*
 * hb.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_vector.h"

#include "msg.h"
#include "socket.h"
#include "tls.h"

#include "fabric/hlc.h"

/**
 * Maximum number of nodes in a cluster.
 */
#ifndef AS_CLUSTER_SZ
#define AS_CLUSTER_SZ 8
#endif

/**
 * Minimum heartbeat interval.
 */
#define AS_HB_TX_INTERVAL_MS_MIN 50

/**
 * Maximum heartbeat interval. (10 mins)
 */
#define AS_HB_TX_INTERVAL_MS_MAX 600000

/**
 * Minimum max-intervals-missed.
 */
#define AS_HB_MAX_INTERVALS_MISSED_MIN 3

/**
 * Heartbeat modes.
 */
typedef enum as_hb_mode_enum
{
	AS_HB_MODE_UNDEF,
	AS_HB_MODE_MULTICAST,
	AS_HB_MODE_MESH
} as_hb_mode;

/**
 * Heartbeat protocol versions.
 */
typedef enum as_hb_protocol_enum
{
	AS_HB_PROTOCOL_UNDEF,
	AS_HB_PROTOCOL_NONE,
	AS_HB_PROTOCOL_RESET,
	AS_HB_PROTOCOL_V3
} as_hb_protocol;

/**
 * Events published by the heartbeat subsystem.
 */
typedef enum
{
	AS_HB_NODE_ARRIVE,
	AS_HB_NODE_DEPART,
	AS_HB_NODE_ADJACENCY_CHANGED,
	AS_HB_NODE_EVENT_SENTINEL
} as_hb_event_type;

/**
 * A plugin that is publishing and receiving data via the heartbeat subsystem.
 * The heartbeat outgoing message buffer will be populated and parsed in the
 * order of this enum.
 */
typedef enum
{
	/**
	 * The heartbeat subsystem itself.
	 */
	AS_HB_PLUGIN_HB,
	/**
	 * The older clustering subsystem.
	 * TODO: Use only one plugin id and register differently based on the
	 * clustering version.
	 */
	AS_HB_PLUGIN_FABRIC,
	/**
	 * The clustering subsystem.
	 */
	AS_HB_PLUGIN_CLUSTERING,
	/**
	 * The skew monitor.
	 */
	AS_HB_PLUGIN_SKEW_MONITOR,
	/**
	 * Dummy sentinel enum value. Should be the last.
	 */
	AS_HB_PLUGIN_SENTINEL
} as_hb_plugin_id;

/**
 * The fields in the heartbeat message.
 * New field additions only at the end.
 */
typedef enum
{
	/**
	 * HB protocol identifier.
	 */
	AS_HB_MSG_ID,

	/**
	 * HB subsystem message type.
	 */
	AS_HB_MSG_TYPE,

	/**
	 * HB message source.
	 */
	AS_HB_MSG_NODE,

	/**
	 * Cluster Name.
	 */
	AS_HB_MSG_CLUSTER_NAME,

	/**
	 * HLC timestamp.
	 */
	AS_HB_MSG_HLC_TIMESTAMP,

	/**
	 * Heartbeats endpoints advertised by this node.
	 */
	AS_HB_MSG_ENDPOINTS,

	/**
	 * Payload for compressed messages.
	 */
	AS_HB_MSG_COMPRESSED_PAYLOAD,

	/**
	 * Mesh info request.
	 */
	AS_HB_MSG_INFO_REQUEST,

	/**
	 * Mesh info reply.
	 */
	AS_HB_MSG_INFO_REPLY,

	/*
	 * ---- Plugin data fields. Potentially extensible  ----
	 */
	/**
	 * Fabric  data advertised by this node. Placed close to hb endpoints to
	 * help compression, because it would most likely match with hb endpoints.
	 */
	AS_HB_MSG_FABRIC_DATA,

	/**
	 * Valid only for pulse messages, has adjacency list and clusterid.
	 */
	AS_HB_MSG_HB_DATA,

	/**
	 * Contains the cluster key and succession list.
	 */
	AS_HB_MSG_PAXOS_DATA,

	/**
	 * Local physical clock monotonic timestamp for when the message was sent.
	 */
	AS_HB_MSG_SKEW_MONITOR_DATA
} as_hb_msg_fields;

/**
 * Heartbeat subsystem configuration.
 */
typedef struct as_hb_config_s
{
	/**
	 * Mode of operation. Mesh or Multicast for now.
	 */
	as_hb_mode mode;

	/**
	 * Binding interface config.
	 */
	cf_serv_cfg bind_cfg;

	/**
	 * Global TLS configuration.
	 */

	cf_tls_info *tls;

	/**
	 * Multicast mode only config for multicast groups.
	 */
	cf_mserv_cfg multicast_group_cfg;

	/**
	 * The interval at which heartbeat pulse messages are sent in milliseconds.
	 */
	uint32_t tx_interval;

	/**
	 * Max number of missed heartbeat intervals after which a node is considered
	 * expired.
	 */
	uint32_t max_intervals_missed;

	/**
	 * The ttl for multicast packets. Set to zero for default TTL.
	 */
	uint8_t multicast_ttl;

	/**
	 * HB protocol to use.
	 */
	as_hb_protocol protocol;

	/**
	 * Set to a value > 0 to override the MTU read from the network interface.
	 */
	uint32_t override_mtu;

	/**
	 * Mesh seeds from config file.
	 * Only used for during config parsing and initialization.
	 */
	char* mesh_seed_addrs[AS_CLUSTER_SZ];
	int mesh_seed_ports[AS_CLUSTER_SZ];
	bool mesh_seed_tls[AS_CLUSTER_SZ];

} as_hb_config;

/**
 * Heartbeat published event structure.
 */
typedef struct as_hb_event_node_s
{
	/**
	 * The type of the event.
	 */
	as_hb_event_type evt;

	/**
	 * The event nodeid.
	 */
	cf_node nodeid;

	/**
	 * The monotonic timestamp when this event happened.
	 */
	cf_clock event_time;

	/**
	 * The monotonic timestamp when this event was detected. Will differ from
	 * event_time for node depart events.
	 */
	cf_clock event_detected_time;
} as_hb_event_node;

/**
 * A hook to allow plugin to publish its data as a part of the heartbeat
 * message.
 */
typedef void (*as_hb_plugin_set_data_fn)(msg* hb_message);

/**
 * Data stored for an adjacent node for a plugin.
 */
typedef struct as_hb_plugin_node_data_s
{
	/**
	 * Heap allocated node specific data blob for this plugin.
	 */
	void* data;

	/**
	 * The size of the stored data.
	 */
	size_t data_size;

	/**
	 * The capacity of the allocated data structure.
	 */
	size_t data_capacity;
} as_hb_plugin_node_data;

/**
 * A function to parse plugin data for a node into an in memory object. Should
 * be fast and never acquire locks.
 *
 * The parameter plugin_data->data will always point to a pre-allocated memory
 * location. plugin_data->data_capacity will indicate the capacity of this
 * memory. Implementations should reuse this pre-allocated data blob to avoid
 * the overhead of heap  allocations. If current data capacity is greater than
 * the new data size please invoke cf_realloc and get a new block for current
 * data and update plugin_data->data and plugin_data->data_capacity accordingly.
 *
 * This function should always update data_size correctly before returning. Set
 * plugin_data->data_size = 0 for no plugin data.
 *
 * @param hb_message the heartbeat message.
 * @param source the source node.
 * @param plugin_data_prev plugin data structure from the previous heartbeat to
 * be used to accumulate historical data.
 * Field plugin_data_prev->data_size will be zero if this the first heartbeat
 * from the source.
 * @param plugin_data (output) plugin data structure to output parsed data.
 */
typedef void (*as_hb_plugin_parse_data_fn)(msg* hb_message, cf_node source, as_hb_plugin_node_data* plugin_data_prev, as_hb_plugin_node_data* plugin_data);

/**
 * A listener for detecting changes to this plugin's data for a particular node.
 * Does not supply old and new values of the data, because does not seem to be
 * required currently and to keep implementation simple.
 *
 * @param node the node whose plugin data changed.
 */
typedef void (*as_hb_plugin_data_changed_fn)(cf_node nodeid);

/**
 * A plugin allows a module to pushing and read data with heartbeat pulse
 * messages.
 */
typedef struct as_hb_plugin_s
{
	/**
	 * The plugin id.
	 */
	as_hb_plugin_id id;

	/**
	 * Fixed plugin data size on wire.
	 */
	size_t wire_size_fixed;

	/**
	 * Additional plugin data size on wire per node in the adjacency list.
	 */
	size_t wire_size_per_node;

	/**
	 * The function which adds this plugin's data to the pulse message. Can be
	 * NULL. This function can hold the plugin module's locks.
	 */
	as_hb_plugin_set_data_fn set_fn;

	/**
	 * A function will parses and reads this plugins data from an incoming
	 * message. Can be NULL. This function SHOULD NOT hold the plugin module's
	 * locks to prevent deadlocks.
	 */
	as_hb_plugin_parse_data_fn parse_fn;

	/**
	 * A function invoked when plugin data for a particular node changed.
	 * Can be NULL. This function can hold the plugin module's locks.
	 */
	as_hb_plugin_data_changed_fn change_listener;
} as_hb_plugin;

/*
 * -----------------------------------------------------------------
 * HB subsystem public API
 * -----------------------------------------------------------------
 */

void as_hb_init();

void as_hb_start();

void as_hb_shutdown();

bool as_hb_self_is_duplicate();

bool as_hb_node_is_adjacent(cf_node nodeid);

typedef void (*as_hb_event_fn)(int nevents, as_hb_event_node* events, void* udata);

void as_hb_register_listener(as_hb_event_fn event_callback, void* udata);

void as_hb_dump(bool verbose);

as_hb_protocol as_hb_protocol_get();

int as_hb_protocol_set(as_hb_protocol protocol);

uint32_t as_hb_node_timeout_get();

void as_hb_override_mtu_set(int mtu);

uint32_t as_hb_tx_interval_get();

int as_hb_tx_interval_set(uint32_t new_interval);

uint32_t as_hb_max_intervals_missed_get();

int as_hb_max_intervals_missed_set(uint32_t new_max);

uint32_t as_hb_node_timeout_get();

bool as_hb_max_cluster_size_isvalid(uint32_t max_cluster_size);

/*
 * -----------------------------------------------------------------
 * HB plugin subsystem public API.
 * -----------------------------------------------------------------
 */

void as_hb_plugin_register(as_hb_plugin* plugin);

bool as_hb_is_alive(cf_node nodeid);

void as_hb_config_validate();

void as_hb_maximal_clique_evict(cf_vector* nodes, cf_vector* nodes_to_evict);

int as_hb_plugin_data_get(cf_node nodeid, as_hb_plugin_id plugin, as_hb_plugin_node_data* plugin_data, as_hlc_msg_timestamp* msg_hlc_ts, cf_clock* recv_monotonic_ts);

typedef void (*as_hb_plugin_data_iterate_fn)(cf_node nodeid, void* plugin_data, size_t plugin_data_size, cf_clock recv_monotonic_ts, as_hlc_msg_timestamp* msg_hlc_ts, void* udata);

void as_hb_plugin_data_iterate(cf_vector* nodes, as_hb_plugin_id plugin, as_hb_plugin_data_iterate_fn iterate_fn, void* udata);

void as_hb_plugin_data_iterate_all(as_hb_plugin_id plugin, as_hb_plugin_data_iterate_fn iterate_fn, void* udata);

/*
 * -----------------------------------------------------------------
 * Info public API
 * -----------------------------------------------------------------
 */

void as_hb_info_config_get(cf_dyn_buf* db);

void as_hb_info_endpoints_get(cf_dyn_buf* db);

void as_hb_info_listen_addr_get(as_hb_mode* mode, char* addr_port, size_t addr_port_capacity);

void as_hb_info_duplicates_get(cf_dyn_buf* db);

/*
 * -----------------------------------------------------------------
 * Mesh mode public API
 * -----------------------------------------------------------------
 */

int as_hb_mesh_tip(char* host, int port, bool tls);

int as_hb_mesh_tip_clear(char* host, int port);

int as_hb_mesh_tip_clear_all(uint32_t* cleared);

void as_hb_config_validate();
