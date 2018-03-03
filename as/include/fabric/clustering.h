/*
 * clustering.h
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

/*
 * Aerospike cluster formation v5 based on paxos.
 * Complete discussion of the algorithm can be found
 * https://docs.google.com/document/d/1u-27aeZD9no9wiWgt1_BsTSg_6ewG9VBI2sYA0g01BE/edit#
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_vector.h"

#include "fault.h"

#include "fabric/hlc.h"

/*
 * ----------------------------------------------------------------------------
 * Public data structures.
 * ----------------------------------------------------------------------------
 */
/**
 * Aerospike cluster key.
 */
typedef uint64_t as_cluster_key;

/**
 * Aerospike clustering protocol identifier.
 */
typedef uint32_t as_cluster_proto_identifier;

/**
 * Configuration for the clustering algorithm.
 */
typedef struct as_clustering_config_s
{
	/**
	 * The smallest allowed  cluster size.
	 */
	uint32_t cluster_size_min;

	/**
	 * Indicates if clique based eviction is enabled.
	 */
	bool clique_based_eviction_enabled;

	/**
	 * Current protocol identifier.
	 */
	as_cluster_proto_identifier protocol_identifier;

} as_clustering_config;

/**
 * The clustering protocol versions.
 */
typedef enum as_clustering_protocol_version
{
	AS_CLUSTERING_PROTOCOL_UNDEF,
	AS_CLUSTERING_PROTOCOL_NONE,
	AS_CLUSTERING_PROTOCOL_V1,
	AS_CLUSTERING_PROTOCOL_V2,
	AS_CLUSTERING_PROTOCOL_V3,
	AS_CLUSTERING_PROTOCOL_V4,
	AS_CLUSTERING_PROTOCOL_V5
} as_clustering_protocol_version;

/**
 * Clustering event type.
 */
typedef enum as_clustering_event_type_e
{
	/**
	 * Cluster membership for this node changed.
	 */
	AS_CLUSTERING_CLUSTER_CHANGED,

	/**
	 * This node became an orphan node.
	 */
	AS_CLUSTERING_ORPHANED
} as_clustering_event_type;

/**
 * Clustering event type.
 */
typedef enum as_clustering_event_qualifier_e
{
	/**
	 * The default qualifier for cases where a qualifier is not applicable.
	 */
	AS_CLUSTERING_QUALIFIER_NA,

	/**
	 * Cluster membership lost since the principal evicted this node or is no
	 * longer reachable or the cluster is invalid. Relevant only for orphaned
	 * event.
	 */
	AS_CLUSTERING_MEMBERSHIP_LOST,

	/**
	 * This node became an orphan node in order to attempt a merge. Relevant
	 * only for orphaned event.
	 */
	AS_CLUSTERING_ATTEMPTING_MERGE,
} as_clustering_event_qualifier;

/**
 * Clustering event.
 */
typedef struct as_clustering_event_s
{
	/**
	 * The clustering event type.
	 */
	as_clustering_event_type type;

	/**
	 * The clustering event qualifier.
	 */
	as_clustering_event_qualifier qualifier;

	/**
	 * The cluster key. Will be non-zero if this is a cluster change event.
	 */
	as_cluster_key cluster_key;

	/**
	 * The new succession list. It will not be empty if this is a cluster change
	 * event.
	 *
	 * The allocated space will be freed once the event processing is complete.
	 * Listeners should always create a copy of this list, if it needs to be
	 * used later on by the listener.
	 */
	cf_vector* succession_list;
} as_clustering_event;

/*
 * ----------------------------------------------------------------------------
 * Public API.
 * ----------------------------------------------------------------------------
 */
/**
 * Initialize clustering subsystem.
 */
void
as_clustering_init();

/**
 * Start clustering subsystem.
 */
void
as_clustering_start();

/**
 * Stop clustering subsystem.
 */
void
as_clustering_stop();

/**
 * Reform the cluster with the same succession list.This would trigger the
 * generation of new partition info and the cluster would get a new cluster key.
 *
 * @return 0 if new clustering round started, -1 otherwise.
 */
int
as_clustering_cluster_reform();

/**
 * Return the quantum interval, i.e., the interval at which cluster change
 * decisions are taken. The unit is milliseconds.
 */
uint64_t
as_clustering_quantum_interval();

/**
 * Log a vector of node-ids at input severity spliting long vectors over
 * multiple lines. The call might not work if the vector is not protected
 * against multi-threaded access.
 *
 * @param context the logging context.
 * @param severity the log severity.
 * @param file_name the source file name for the log line.
 * @param line the source file line number for the log line.
 * @param message the message prefix for each log line. Message and node list
 * will be separated with a space. Can be NULL for no prefix.
 * @param nodes the vector of nodes.
 */
void
as_clustering_cf_node_vector_event(cf_fault_severity severity,
		cf_fault_context context, char* file_name, int line, char* message,
		cf_vector* nodes);

/**
 * Log an array of node-ids at input severity spliting long vectors over
 * multiple lines. The call might not work if the array is not protected against
 * multi-threaded access.
 *
 * @param context the logging context.
 * @param severity the log severity.
 * @param file_name the source file name for the log line.
 * @param line the source file line number for the log line.
 * @param message the message prefix for each log line. Message and node list
 * will be separated with a space. Can be NULL for no prefix.
 * @param nodes the array of nodes.
 * @param node_count the count of nodes in the array.
 */
void
as_clustering_cf_node_array_event(cf_fault_severity severity,
		cf_fault_context context, char* file_name, int line, char* message,
		cf_node* nodes, int node_count);

/**
 * Log a vector of node-ids at input severity spliting long vectors over
 * multiple lines. The call might not work if the vector is not protected
 * against multi-threaded access.
 *
 * @param context the logging context.
 * @param severity the log severity.
 * @param message the message prefix for each log line. Message and node list
 * will be separated with a space. Can be NULL for no prefix.
 * @param nodes the vector of nodes.
 */
#define as_clustering_log_cf_node_vector(severity, context, message, nodes)					\
	as_clustering_cf_node_vector_event(severity, context, __FILENAME__,	\
									   __LINE__, message, nodes)

/**
 * Log an array of node-ids at input severity spliting long vectors over
 * multiple lines. The call might not work if the array is not protected against
 * multi-threaded access.
 *
 * @param context the logging context.
 * @param severity the log severity.
 * @param message the message prefix for each log line. Message and node list
 * will be separated with a space. Can be NULL for no prefix.
 * @param nodes the array of nodes.
 * @param node_count the count of nodes in the array.
 */
#define as_clustering_log_cf_node_array(severity, context, message, nodes,	\
		node_count)															\
as_clustering_cf_node_array_event(severity, context, __FILENAME__,			\
		__LINE__, message, nodes, node_count);


/*
 * ---- Clustering info command functions. ----
 */
/**
 * If false means than either this node is orphaned, or is undergoing a cluster
 * change.
 */
bool
as_clustering_has_integrity();

/**
 * Indicates if self node is orphaned.
 */
bool
as_clustering_is_orphan();

/**
 * Dump clustering state to the log.
 */
void
as_clustering_dump(bool verbose);

/**
 * Set the min cluster size.
 */
int
as_clustering_cluster_size_min_set(uint32_t new_cluster_size_min);
