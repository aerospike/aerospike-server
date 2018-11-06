/*
 * clustering.c
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

#include "fabric/clustering.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/param.h> // For MAX() and MIN().

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_random.h"

#include "cf_thread.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "shash.h"

#include "base/cfg.h"
#include "fabric/fabric.h"
#include "fabric/hlc.h"

/*
 * Overview
 * ========
 * Clustering v5 implementation based on the design at
 * https://aerospike.atlassian.net/wiki/pages/viewpage.action?spaceKey=DEV&title=Central+Wiki%3A++Clustering+V5
 *
 * Public and private view of the cluster
 * =======================================
 * This clustering algorithm introduces an orphan state, in which this node is
 * not part of a cluster, but is looking to form/join a cluster. During this
 * transitionary phase, the public view of the cluster the tuple, <cluster_key,
 * succession_list), does not change from the last view. However the internal
 * view, which is published along with the heartbeat messages, is set to <0,
 * []>.
 *
 * This ensures clients continue to function, (maybe with errors), during the
 * transition from orphan to part of a cluster state. This is in line with the
 * clustering v4 and prior behaviour.
 *
 * TODO: (revise)
 *
 * Deviations from paxos
 * =====================
 *
 * Accepted value
 * ---------------
 *
 * Accepted value is not send along with accept and accepted message. The latest
 * accepted value overwrites the previous value at a node. In paxos if a node
 * has already accepted a value, it is send back to the proposer who should use
 * the value with highest proposal id as the final value. The proposer generates
 * the final consensus value as the succession list with the nodes that have
 * both returned promise and accepted replies.
 *
 * This is not safe in terms of achieveing a single paxos value, however it is
 * safe in that nodes courted by other principals will get filtered out during
 * paxos and not require additional paxos rounds.
 *
 * It is still possible that the final consensus succession list might has a few
 * nodes moving out owing to a neighboring principal. However the faulty node
 * check in the next quantum interval will fix this.
 *
 * Quorum
 * ------
 * The prepare phase uses a majority quorum for the promise messages, to speed
 * through the paxos round. However the accept phase uses a complete / full
 * quorum for accepted messages. This helps with ensuring that when a node
 * generartes a cluster change event all cluster member have applied the current
 * cluster membership.
 *
 * Design
 * ======
 * The clustering sub-system with rest of Aerospike via input event notification
 * (primarily heartbeat events) and output events notifications (primary cluster
 * change notifications).
 *
 * The subsystem is driven by internal events (that also encapsulate external
 * input event notifications) like timer, quantum interval start, adjaceny
 * changed, message received, etc.
 *
 * The clustering-v5 subsystem is further organized as the following sub-modules
 * each of which reacts to the above mentioned events based on individual state
 * transition diagrams.
 *
 * 	1. Timer
 * 	2. Quantum interval generator
 * 	3. Paxos proposer
 * 	4. Paxos acceptor
 * 	5. Register
 * 	6. External event publisher
 * 	7. Internal event dispatcher
 * 	8. Clustering main
 *
 * The sub modules also interact with each other via inline internal event
 * dispatch and handling.
 *
 * Timer
 * -----
 * Generates timer events that serve as the internal tick/clock for the
 * clustering-v5 sub system. Other sub-modules use the timer events to drive
 * actions to be performed at fixed intervals, for e.g. message retransmits.
 *
 * Quantum interval generator
 * --------------------------
 * Generates quantum interval start events, at which cluster change decision are
 * taken.
 *
 * Paxos proposer
 * --------------
 * The paxos proposer proposes a cluster change. The node may or may not be the
 * eventual principal for the cluster.
 *
 * Paxos acceptor
 * --------------
 * Participates in voting for a proposal. A paxos proposer is also necessarily
 * an accetor in this design.
 *
 * Register
 * --------
 * Holds current cluster membership and cluster key. It is responsible for
 * ensuring all cluster members have their registers in sync before publishing
 * an external cluster change event.
 *
 * External event publisher
 * ------------------------
 * Generate and publishes external events or cluster changes. Runs as a separate
 * thread to prevent interference and potential deadlocks with the clustering
 * subsystem.
 *
 * Internal event dispatcher
 * -------------------------
 * Dispatches internal events to current function based in the event type and
 * current state.
 *
 * Clustering main
 * ---------------
 * Monitors the cluster and triggers cluster changes.
 *
 * State transitions
 * =================
 * TODO: diagrams for each sub-module
 *
 * Message send rules
 * ==================
 * Message send should preferably be outside the main clustering lock and should
 * not be followed by any state change in the same function. This is because
 * fabric relays messages to self inline in the send call itself which can lead
 * to corruption if the message handler involves a state change as well or can
 * result in the message handler seeing inconsistent partially updated state.
 */

/*
 * ----------------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------------
 */

/**
 * A soft limit for the maximum cluster size. Meant to be optimize hash and list
 * data structures and not as a limit on the number of nodes.
 */
#define AS_CLUSTERING_CLUSTER_MAX_SIZE_SOFT 200

/**
 * Timer event generation interval.
 */
#define CLUSTERING_TIMER_TICK_INTERVAL 75

/**
 * Maximum time paxos round would take for completion. 3 RTTs paxos message
 * exchanges and 1 RTT as a buffer.
 */
#define PAXOS_COMPLETION_TIME_MAX (4 * network_rtt_max())

/**
 * Maximum quantum interval duration, should be at least two heartbeat
 * intervals, to ensure there is at least one exchange of clustering information
 * over heartbeats.
 */
#define QUANTUM_INTERVAL_MAX MAX(5000, 2 * as_hb_tx_interval_get())

/**
 * Block size for allocating node plugin data. Ensure the allocation is in
 * multiples of 128 bytes, allowing expansion to 16 nodes without reallocating.
 */
#define HB_PLUGIN_DATA_BLOCK_SIZE 128

/**
 * Scratch size for clustering messages.
 *
 * TODO: Compute this properly.
 */
#define AS_CLUSTERING_MSG_SCRATCH_SIZE 1024

/**
 * Majority value for preferred principal to be selected for move. Use tow
 * thirds as the majority value.
 */
#define AS_CLUSTERING_PREFERRRED_PRINCIPAL_MAJORITY (2 / 3)

/*
 * ----------------------------------------------------------------------------
 * Paxos data structures
 * ----------------------------------------------------------------------------
 */

/**
 * Paxos sequence number. We will use the hybrid logical clock timestamp as
 * sequence numbers, to ensure node restarts do not reset the sequence number
 * back to zero and sequence numbers are monotoniocally increasing. A sequence
 * number value of zero is invalid.
 */
typedef as_hlc_timestamp as_paxos_sequence_number;

/**
 * Paxos proposal identifier.
 * Note: The nodeid can be skipped when sending the proposal id over the wire
 * and can be inferred from the source duirng paxos message exchanges.
 */
typedef struct as_paxos_proposal_id_s
{
	/**
	 * The sequence number.
	 */
	as_paxos_sequence_number sequence_number;

	/**
	 * The proposing node's nodeid to break ties.
	 */
	cf_node src_nodeid;
} as_paxos_proposal_id;

/**
 * The proposed cluster membership.
 */
typedef struct as_paxos_proposed_value_s
{
	/**
	 * The cluster key.
	 */
	as_cluster_key cluster_key;

	/**
	 * The succession list.
	 */
	cf_vector succession_list;
} as_paxos_proposed_value;

/**
 * Paxos acceptor state.
 */
typedef enum
{
	/**
	 * Acceptor is idel with no active paxos round.
	 */
	AS_PAXOS_ACCEPTOR_STATE_IDLE,

	/**
	 * Acceptor has received and acked a promise message.
	 */
	AS_PAXOS_ACCEPTOR_STATE_PROMISED,

	/**
	 * Acceptor has received and accepted an accept message from a proposer.
	 */
	AS_PAXOS_ACCEPTOR_STATE_ACCEPTED
} as_paxos_acceptor_state;

/**
 * Data tracked by the node in the role of a paxos acceptor.
 * All nodes are paxos acceptors.
 */
typedef struct as_paxos_acceptor_s
{
	/**
	 * The paxos acceptor state.
	 */
	as_paxos_acceptor_state state;

	/**
	 * Monotonic timestamp when the first message for current proposal was
	 * received from the proposer.
	 */
	cf_clock acceptor_round_start;

	/**
	 * Monotonic timestamp when the promise message was sent.
	 */
	cf_clock promise_send_time;

	/**
	 * Monotonic timestamp when the promise message was sent.
	 */
	cf_clock accepted_send_time;

	/**
	 * Id of the last proposal, promised or accepted by this node.
	 */
	as_paxos_proposal_id last_proposal_received_id;
} as_paxos_acceptor;

/**
 * State of a paxos proposer.
 */
typedef enum as_paxos_proposer_state_e
{
	/**
	 * Paxos proposer is idle. No pending paxos rounds.
	 */
	AS_PAXOS_PROPOSER_STATE_IDLE,

	/**
	 * Paxos proposer sent out a prepare message.
	 */
	AS_PAXOS_PROPOSER_STATE_PREPARE_SENT,

	/**
	 * Paxos proposer has sent out an accept message.
	 */
	AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT
} as_paxos_proposer_state;

/**
 * Data tracked by the node in the role of a paxos proposer. The proposer node
 * may or may not be the current or eventual principal.
 */
typedef struct as_paxos_proposer_s
{
	/**
	 * The state of the proposer.
	 */
	as_paxos_proposer_state state;

	/**
	 * The sequence number / id for the last proposed paxos value.
	 */
	as_paxos_sequence_number sequence_number;

	/**
	 * The proposed cluster value.
	 */
	as_paxos_proposed_value proposed_value;

	/**
	 * The time current paxos round was started.
	 */
	cf_clock paxos_round_start_time;

	/**
	 * The time current proposal's prepare message was sent.
	 */
	cf_clock prepare_send_time;

	/**
	 * The time current proposal's accept message was sent.
	 */
	cf_clock accept_send_time;

	/**
	 * The time current proposal's learn message was sent.
	 */
	cf_clock learn_send_time;

	/**
	 * Indicates if learn message needs retransmit.
	 */
	bool learn_retransmit_needed;

	/**
	 * The set of acceptor nodes including self.
	 */
	cf_vector acceptors;

	/**
	 * Set of nodeids that send out a promise response to the current prepare
	 * message.
	 */
	cf_vector promises_received;

	/**
	 * Set of nodeids that send out an accepted response to the current accept
	 * message.
	 */
	cf_vector accepted_received;
} as_paxos_proposer;

/**
 * Result of paxos round start call.
 */
typedef enum as_paxos_start_result_e
{
	/**
	 * Paxos round started successfully.
	 */
	AS_PAXOS_RESULT_STARTED,

	/**
	 * cluster size is less than minimum required cluster size.
	 */
	AS_PAXOS_RESULT_CLUSTER_TOO_SMALL,

	/**
	 * Paxos round already in progress. Paxos not started.
	 */
	AS_PAXOS_RESULT_ROUND_RUNNING
} as_paxos_start_result;

/**
 * Node clustering status.
 */
typedef enum
{
	/**
	 * Peer node is orphaned.
	 */
	AS_NODE_ORPHAN,

	/**
	 * Peer node has a cluster assigned.
	 */
	AS_NODE_CLUSTER_ASSIGNED,

	/**
	 * Peer node status is unknown.
	 */
	AS_NODE_UNKNOWN
} as_clustering_peer_node_state;

/*
 * ----------------------------------------------------------------------------
 * Clustering data structures
 * ----------------------------------------------------------------------------
 */

/**
 * Clustering message types.
 */
typedef enum
{
	/*
	 * ---- Clustering management messages ----
	 */
	AS_CLUSTERING_MSG_TYPE_JOIN_REQUEST,
	AS_CLUSTERING_MSG_TYPE_JOIN_REJECT,
	AS_CLUSTERING_MSG_TYPE_MERGE_MOVE,
	AS_CLUSTERING_MSG_TYPE_CLUSTER_CHANGE_APPLIED,

	/*
	 * ---- Paxos messages ----
	 */
	AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE,
	AS_CLUSTERING_MSG_TYPE_PAXOS_PROMISE,
	AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE_NACK,
	AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT,
	AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPTED,
	AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT_NACK,
	AS_CLUSTERING_MSG_TYPE_PAXOS_LEARN,
} as_clustering_msg_type;

/**
 * The fields in the clustering message.
 */
typedef enum
{
	/**
	 * Clustering message identifier.
	 */
	AS_CLUSTERING_MSG_ID,

	/**
	 * Clustering message type.
	 */
	AS_CLUSTERING_MSG_TYPE,

	/**
	 * The source node send timestamp.
	 */
	AS_CLUSTERING_MSG_HLC_TIMESTAMP,

	/**
	 * The paxos sequence number. Not all messages will have this.
	 */
	AS_CLUSTERING_MSG_SEQUENCE_NUMBER,

	/**
	 * The proposed cluster key. Only part of the paxos accept message.
	 */
	AS_CLUSTERING_MSG_CLUSTER_KEY,

	/**
	 * The proposed succession list. Only part of the paxos accept message.
	 */
	AS_CLUSTERING_MSG_SUCCESSION_LIST,

	/**
	 * The proposed principal relevant only to cluster move commands, which will
	 * merge two well formed paxos clusters.
	 */
	AS_CLUSTERING_MSG_PROPOSED_PRINCIPAL,

	/**
	 * Sentinel value to keep track of the number of message fields.
	 */
	AS_CLUSTERING_MGS_SENTINEL
} as_clustering_msg_field;

/**
 * Internal clustering event type.
 */
typedef enum
{
	/**
	 * Timer event.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_TIMER,

	/**
	 * Incoming message event.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_MSG,

	/**
	 * A join request was accepted.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_JOIN_REQUEST_ACCEPTED,

	/**
	 * Indicates the start of a quantum interval.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_QUANTUM_INTERVAL_START,

	/**
	 * Indicates that self node's cluster membership changed.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_CHANGED,

	/**
	 * Indicates that self node's cluster membership has been synced across all
	 * cluster members.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_SYNCED,

	/**
	 * Indicates that self node has been marked as an orphan.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_REGISTER_ORPHANED,

	/**
	 * Indicates an incoming heartbeat event.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_HB,

	/**
	 * Indicates that plugin data for a node has changed.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_HB_PLUGIN_DATA_CHANGED,

	/**
	 * The paxos round being accepted succeeded and the proposed value should be
	 * committed.
	 * This implies that all the proposed cluster members have all agreed on the
	 * proposed cluster key and the proposed cluster membership.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_PAXOS_ACCEPTOR_SUCCESS,

	/**
	 * The last paxos round being accepted failed.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_PAXOS_ACCEPTOR_FAIL,

	/**
	 * The paxos round proposed by this node.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_PAXOS_PROPOSER_SUCCESS,

	/**
	 * The last paxos round proposed failed.
	 */
	AS_CLUSTERING_INTERNAL_EVENT_PAXOS_PROPOSER_FAIL,
} as_clustering_internal_event_type;

/**
 * An event used internally by the clustering subsystem.
 */
typedef struct as_clustering_internal_event_s
{
	/**
	 * The event type.
	 */
	as_clustering_internal_event_type type;

	/**
	 * The event qualifier.
	 */
	as_clustering_event_qualifier qualifier;

	/*
	 * ----- Quantum interval start event related fields
	 */
	/**
	 * Indicates if this quantum interval start can be skipped by the event
	 * handler.
	 */
	bool quantum_interval_is_skippable;

	/*
	 * ----- Message event related fields.
	 */
	/**
	 * The source node id.
	 */
	cf_node msg_src_nodeid;

	/**
	 * Incoming message type.
	 */
	as_clustering_msg_type msg_type;

	/**
	 * The hlc timestamp for message receipt.
	 */
	as_hlc_msg_timestamp msg_hlc_ts;

	/**
	 * Local monotonic received timestamp.
	 */
	cf_clock msg_recvd_ts;

	/**
	 * The received message.
	 */
	msg* msg;

	/*
	 * ----- HB event related fields.
	 */
	/**
	 * Number of heartbeat events.
	 */
	int hb_n_events;

	/**
	 * Heartbeat events.
	 */
	as_hb_event_node* hb_events;

	/*
	 * ----- HB plugin data changed event related fields.
	 */
	/**
	 * Node id of the node whose plugin data has changed.
	 */
	cf_node plugin_data_changed_nodeid;

	/**
	 * Node's plugin data.
	 */
	as_hb_plugin_node_data* plugin_data;

	/**
	 * The hlc timestamp for message receipt.
	 */
	as_hlc_msg_timestamp plugin_data_changed_hlc_ts;

	/**
	 * Local monotonic received timestamp.
	 */
	cf_clock plugin_data_changed_ts;

	/*
	 * ----- Join request handled related fields.
	 */
	cf_node join_request_source_nodeid;

	/*
	 * ----- Paxos success related fields.
	 */
	/**
	 * New succession list.
	 */
	cf_vector *new_succession_list;

	/**
	 * New cluster key.
	 */
	as_cluster_key new_cluster_key;

	/**
	 * New paxos sequence number.
	 */
	as_paxos_sequence_number new_sequence_number;
} as_clustering_internal_event;

/**
 * The clustering timer state.
 */
typedef struct as_clustering_timer_s
{
	/**
	 * The timer thread id.
	 */
	pthread_t timer_tid;
} as_clustering_timer;

/**
 * Clustering subsystem state.
 */
typedef enum
{
	AS_CLUSTERING_SYS_STATE_UNINITIALIZED,
	AS_CLUSTERING_SYS_STATE_RUNNING,
	AS_CLUSTERING_SYS_STATE_SHUTTING_DOWN,
	AS_CLUSTERING_SYS_STATE_STOPPED
} as_clustering_sys_state;

/**
 * Type of quantum interval fault. Ensure the vtable in quantum iterval table is
 * updated for each type.
 */
typedef enum as_clustering_quantum_fault_type_e
{
	/**
	 * A new node arrived.
	 */
	QUANTUM_FAULT_NODE_ARRIVED,

	/**
	 * A node not our principal departed from the cluster.
	 */
	QUANTUM_FAULT_NODE_DEPARTED,

	/**
	 * We are in a cluster and out principal departed.
	 */
	QUANTUM_FAULT_PRINCIPAL_DEPARTED,

	/**
	 * A member node's adjacency list has changed.
	 */
	QUANTUM_FAULT_PEER_ADJACENCY_CHANGED,

	/**
	 * Join request accepted.
	 */
	QUANTUM_FAULT_JOIN_ACCEPTED,

	/**
	 * We have seen a principal who might send us a merge request.
	 */
	QUANTUM_FAULT_INBOUND_MERGE_CANDIDATE_SEEN,

	/**
	 * A node in our cluster has been orphaned.
	 */
	QUANTUM_FAULT_CLUSTER_MEMBER_ORPHANED,

	/**
	 * Sentinel value. Should be the last in the enum.
	 */
	QUANTUM_FAULT_TYPE_SENTINEL
} as_clustering_quantum_fault_type;

/**
 * Fault information for for first fault event detected in a quantum interval.
 */
typedef struct as_clustering_quantum_fault_s
{
	/**
	 * First time the fault event was detected in current quantum based on
	 * monotonic clock. Should be initialized to zero at quantum start / end.
	 */
	cf_clock event_ts;

	/**
	 * Last time the fault event was detected in current quantum based on
	 * monotonic clock. Should be initialized to zero at quantum start / end.
	 */
	cf_clock last_event_ts;
} as_clustering_quantum_fault;

/**
 * Function to determine the minimum wait time after given fault happens.
 */
typedef uint32_t
(as_clustering_quantum_fault_wait_fn)(as_clustering_quantum_fault* fault);

/**
 * Vtable for different types of faults.
 */
typedef struct as_clustering_quantum_fault_vtable_s
{
	/**
	 * String used to log this fault type.
	 */
	char *fault_log_str;

	/**
	 * Function providing the wait time for this fault type.
	 */
	as_clustering_quantum_fault_wait_fn* wait_fn;
} as_clustering_quantum_fault_vtable;

/**
 * Generates quantum intervals.
 */
typedef struct as_clustering_quantum_interval_generator_s
{
	/**
	 * Quantum interval fault vtable.
	 */
	as_clustering_quantum_fault_vtable vtable[QUANTUM_FAULT_TYPE_SENTINEL];

	/**
	 * Quantum interval faults.
	 */
	as_clustering_quantum_fault fault[QUANTUM_FAULT_TYPE_SENTINEL];

	/**
	 * Time quantum interval last started.
	 */
	cf_clock last_quantum_start_time;

	/**
	 * For quantum interval being skippable respect the last quantum interval
	 * since quantum_interval() will be affected by changes to hb config.
	 */
	uint32_t last_quantum_interval;

	/**
	 * Indicates if current quantum interval should be postponed.
	 */
	bool is_interval_postponed;
} as_clustering_quantum_interval_generator;

/**
 * State of the clustering register.
 */
typedef enum
{
	/**
	 * The register contents are in synced with all cluster members.
	 */
	AS_CLUSTERING_REGISTER_STATE_SYNCED,

	/**
	 * The register contents are being synced with other cluster members.
	 */
	AS_CLUSTERING_REGISTER_STATE_SYNCING
} as_clustering_register_state;

/**
 * Stores current cluster key and succession list and generates external events.
 */
typedef struct as_clustering_register_s
{
	/**
	 * The register state.
	 */
	as_clustering_register_state state;

	/**
	 * Current cluster key.
	 */
	as_cluster_key cluster_key;

	/**
	 * Current succession list.
	 */
	cf_vector succession_list;

	/**
	 * Indicates if this node has transitioned to orphan state after being in a
	 * valid cluster.
	 */
	bool has_orphan_transitioned;

	/**
	 * The sequence number for the current cluster.
	 */
	as_paxos_sequence_number sequence_number;

	/**
	 * Nodes pending sync.
	 */
	cf_vector sync_pending;

	/**
	 * Nodes that send a sync applied for an unexpected cluster. Store it in
	 * case this is an imminent cluster change we will see in the future. All
	 * the nodes in this vector have sent the same cluster key and the same
	 * succession list.
	 */
	cf_vector ooo_change_applied_received;

	/**
	 * Cluster key sent by nodes in ooo_change_applied_received vector.
	 */
	as_cluster_key ooo_cluster_key;

	/**
	 * Succession sent by nodes in ooo_change_applied_received vector.
	 */
	cf_vector ooo_succession_list;

	/**
	 * Timestamp of the first ooo change applied message.
	 */
	as_hlc_timestamp ooo_hlc_timestamp;

	/**
	 * The time cluster last changed.
	 */
	as_hlc_timestamp cluster_modified_hlc_ts;

	/**
	 * The monotonic clock time cluster last changed.
	 */
	cf_clock cluster_modified_time;

	/**
	 * The last time the register sync was checked in the syncing state.
	 */
	cf_clock last_sync_check_time;
} as_clustering_register;

/**
 * * Clustering state.
 */
typedef enum
{
	/**
	 * Self node is not part of a cluster.
	 */
	AS_CLUSTERING_STATE_ORPHAN,

	/**
	 * Self node is not part of a cluster.
	 */
	AS_CLUSTERING_STATE_PRINCIPAL,

	/**
	 * Self node is part of a cluster but not the principal.
	 */
	AS_CLUSTERING_STATE_NON_PRINCIPAL
} as_clustering_state;

/**
 * Clustering state maintained by this node.
 */
typedef struct as_clustering_s
{

	/**
	 * Clustering submodule state, indicates if the clustering sub system is
	 * running, stopped or initialized.
	 */
	as_clustering_sys_state sys_state;

	/**
	 * Simple view of whether or not the cluster is well-formed.
	 */
	bool has_integrity;

	/**
	 * Clustering relevant state, e.g. orphan, principal, non-principal.
	 */
	as_clustering_state state;

	/**
	 * The preferred principal is a node such that removing current principal
	 * and making said node new principal will lead to a larger cluster. This is
	 * updated in the non-principal state at each quantum interval and is sent
	 * out with each heartbeat pulse.
	 */
	cf_node preferred_principal;

	/**
	 * Pending join requests.
	 */
	cf_vector pending_join_requests;

	/**
	 * The monotonic clock time when this node entered orphan state.
	 * Will be set to zero when the node is not an orphan.
	 */
	cf_clock orphan_state_start_time;

	/**
	 * Time when the last move command was sent.
	 */
	cf_clock move_cmd_issue_time;

	/**
	 * Hash from nodes whom join request was sent to the time the join request
	 * was send . Used to prevent sending join request too quickly to the same
	 * principal again and again.
	 */
	cf_shash* join_request_blackout;

	/**
	 * The principal to which the last join request was sent.
	 */
	cf_node last_join_request_principal;

	/**
	 * The time at which the last join request was sent, to track and timeout
	 * join requests.
	 */
	cf_clock last_join_request_sent_time;

	/**
	 * The time at which the last join request was retransmitted, to track and
	 * retransmit join requests.
	 */
	cf_clock last_join_request_retransmit_time;
} as_clustering;

/**
 * Result of sending out a join request.
 */
typedef enum as_clustering_join_request_result_e
{
	/**
	 *
	 * Join request was sent out.
	 */
	AS_CLUSTERING_JOIN_REQUEST_SENT,

	/**
	 *
	 * Join request was attempted, but sending failed.
	 */
	AS_CLUSTERING_JOIN_REQUEST_SEND_FAILED,

	/**
	 * Join request already pending. A new join request was not sent.
	 */
	AS_CLUSTERING_JOIN_REQUEST_PENDING,

	/**
	 * No neighboring principals present to send the join request.
	 */
	AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS
} as_clustering_join_request_result;

/**
 * External event publisher state.
 */
typedef struct as_clustering_external_event_publisher_s
{
	/**
	 * State of the external event publisher.
	 */
	as_clustering_sys_state sys_state;

	/**
	 * Inidicates if there is an event to publish.
	 */
	bool event_queued;

	/**
	 * The pending event to publish.
	 */
	as_clustering_event to_publish;

	/**
	 * The static succession list published with the message.
	 */
	cf_vector published_succession_list;

	/**
	 * Conditional variable to signal pending event to publish.
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
} as_clustering_external_event_publisher;

/*
 * ----------------------------------------------------------------------------
 * Forward declarations
 * ----------------------------------------------------------------------------
 */
static void
internal_event_dispatch(as_clustering_internal_event* event);
static bool
clustering_is_our_principal(cf_node nodeid);
static bool
clustering_is_principal();
static bool
clustering_is_cluster_member(cf_node nodeid);

/*
 * ----------------------------------------------------------------------------
 * Non-public hooks to exchange subsystem.
 * ----------------------------------------------------------------------------
 */
extern void
exchange_clustering_event_listener(as_clustering_event* event);

/*
 * ----------------------------------------------------------------------------
 * Timer, timeout values and intervals
 *
 * All values should be multiples of timer tick interval.
 * ----------------------------------------------------------------------------
 */

/**
 * Timer tick interval, which should be a GCD of all clustering intervals.
 */
static uint32_t
timer_tick_interval()
{
	return CLUSTERING_TIMER_TICK_INTERVAL;
}

/**
 * Maximum network latency for the cluster.
 */
static uint32_t
network_latency_max()
{
	return g_config.fabric_latency_max_ms;
}

/**
 * Maximum network rtt for the cluster.
 */
static uint32_t
network_rtt_max()
{
	return 2 * network_latency_max();
}

/**
 * Quantum interval in milliseconds.
 */
static uint32_t
quantum_interval()
{
	uint32_t std_quantum_interval = MIN(QUANTUM_INTERVAL_MAX,
			as_hb_node_timeout_get()
					+ 2 * (as_hb_tx_interval_get() + network_latency_max()));

	// Ensure we give paxos enough time to complete.
	return MAX(PAXOS_COMPLETION_TIME_MAX, std_quantum_interval);
}

/**
 * Maximum number of times quantum interval start can be skipped.
 */
static uint32_t
quantum_interval_skip_max()
{
	return 2;
}

/**
 * Interval at which register sync is checked.
 */
static uint32_t
register_sync_check_interval()
{
	return MAX(network_rtt_max(), as_hb_tx_interval_get());
}

/**
 * Timeout for a join request, should definitely be larger than a quantum
 * interval to prevent the requesting node from making new requests before the
 * current requested principal node can finish the paxos round.
 */
static uint32_t
join_request_timeout()
{
	// Allow for
	// 	- 1 quantum interval, where our request lands just after the potential
	// principal's quantum interval start.
	// 	- 0.5 quantum intervals to give time for a paxos round to finish
	// 	- (quantum_interval_skip_max -1) intervals if the principal had to skip
	// quantum intervals.
	return (uint32_t)(
			(1 + 0.5 + (quantum_interval_skip_max() - 1)) * quantum_interval());
}

/**
 * Timeout for a retransmitting a join request.
 */
static uint32_t
join_request_retransmit_timeout()
{
	return (uint32_t)(MIN(as_hb_tx_interval_get() / 2, quantum_interval() / 2));
}

/**
 * The interval at which a node checks to see if it should join a cluster.
 */
static uint32_t
join_cluster_check_interval()
{
	return timer_tick_interval();
}

/**
 * Blackout period for join requests to a particular principal to prevent
 * bombarding it with join requests. Should be less than join_request_timeout().
 */
static uint32_t
join_request_blackout_interval()
{
	return MIN(join_request_timeout(),
			MIN(quantum_interval() / 2, 2 * as_hb_tx_interval_get()));
}

/**
 * Blackout period after sending a move command, during which join requests will
 * be rejected.
 */
static uint32_t
join_request_move_reject_interval()
{
	// Wait for one quantum interval before accepting join requests after
	// sending a move command.
	return quantum_interval();
}

/**
 * Maximum tolerable join request transmission delay in milliseconds. Join
 * requests delayed by more than this amount will not be accepted.
 */
static uint32_t
join_request_accept_delay_max()
{
	// Join request is considered stale / delayed if the (received hlc timestamp
	// - send hlc timestamp) > this value;
	return (2 * as_hb_tx_interval_get() + network_latency_max());
}

/**
 * Timeout in milliseconds for a paxos proposal. Give a paxos round two thirds
 * of an interval to timeout.
 * A paxos round should definitely timeout before the next quantum interval, so
 * that it does not delay cluster convergence.
 */
static uint32_t
paxos_proposal_timeout()
{
	return MAX(quantum_interval() / 2, network_rtt_max());
}

/**
 * Timeout in milliseconds after which a paxos message is retransmitted.
 */
static uint32_t
paxos_msg_timeout()
{
	return MAX(MIN(quantum_interval() / 4, 100), network_rtt_max());
}

/**
 * Maximum amount of time a node will be in orphan state. After this timeout the
 * node will try forming a new cluster even if there are other adjacent
 * clusters/nodes visible.
 */
static uint32_t
clustering_orphan_timeout()
{
	return UINT_MAX;
}

/*
 * ----------------------------------------------------------------------------
 * Stack allocation
 * ----------------------------------------------------------------------------
 */

/**
 * Maximum memory size allocated on the call stack.
 */
#define STACK_ALLOC_LIMIT() (16 * 1024)

/**
 * Allocate a buffer on stack if possible. Larger buffers are heap allocated to
 * prevent stack overflows.
 */
#define BUFFER_ALLOC_OR_DIE(size)									\
(((size) > STACK_ALLOC_LIMIT()) ? cf_malloc(size) : alloca(size))

/**
 * Free the buffer allocated by BUFFER_ALLOC
 */
#define BUFFER_FREE(buffer, size)									\
if (((size) > STACK_ALLOC_LIMIT()) && buffer) {cf_free(buffer);}

/*
 * ----------------------------------------------------------------------------
 * Logging
 * ----------------------------------------------------------------------------
 */
#define LOG_LENGTH_MAX() (800)
#define CRASH(format, ...) cf_crash(AS_CLUSTERING, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_CLUSTERING, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_CLUSTERING, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_CLUSTERING, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_CLUSTERING, format, ##__VA_ARGS__)

#define ASSERT(expression, message, ...)				\
if (!(expression)) {WARNING(message, ##__VA_ARGS__);}

#define log_cf_node_array(message, nodes, node_count, severity)		\
as_clustering_log_cf_node_array(severity, AS_CLUSTERING, message,	\
								 		nodes, node_count)
#define log_cf_node_vector(message, nodes, severity) \
	as_clustering_log_cf_node_vector(severity, AS_CLUSTERING, message,	\
										nodes)

/*
 * ----------------------------------------------------------------------------
 * Vector functions
 * ----------------------------------------------------------------------------
 */

/**
 * Clear / delete all entries in a vector.
 */
static void
vector_clear(cf_vector* vector)
{
	cf_vector_delete_range(vector, 0, cf_vector_size(vector));
}

/**
 * Create temporary stack variables.
 */
#define TOKEN_PASTE(x, y) x##y
#define STACK_VAR(x, y) TOKEN_PASTE(x, y)

/**
 * Initialize a lockless vector, initially sized to  store cluster node number
 * of elements.
 */
#define vector_lockless_init(vectorp, value_type)							\
({																			\
	cf_vector_init(vectorp, sizeof(value_type),								\
			AS_CLUSTERING_CLUSTER_MAX_SIZE_SOFT, VECTOR_FLAG_INITZERO);		\
})

/**
 * Create and initialize a lockless stack allocated vector to initially sized to
 * store cluster node number of elements.
 */
#define vector_stack_lockless_create(value_type)									\
({																					\
	cf_vector * STACK_VAR(vector, __LINE__) = (cf_vector*)alloca(					\
			sizeof(cf_vector));														\
	size_t buffer_size = AS_CLUSTERING_CLUSTER_MAX_SIZE_SOFT						\
			* sizeof(value_type);													\
	void* STACK_VAR(buff, __LINE__) = alloca(buffer_size); cf_vector_init_smalloc(	\
			STACK_VAR(vector, __LINE__), sizeof(value_type),						\
			(uint8_t*)STACK_VAR(buff, __LINE__), buffer_size,						\
			VECTOR_FLAG_INITZERO);													\
	STACK_VAR(vector, __LINE__);													\
})

/**
 * Check two vector for equality. Two vector are euql if they have the same
 * number of elements and corresponding elements are equal. For now simple
 * memory compare is used to compare elements. Assumes the vectors are not
 * accessed by other threads during this operation.
 *
 * @param v1 the first vector to compare.
 * @param v2 the second vector to compare.
 * @return true if the vectors are true, false otherwise.
 */
static bool
vector_equals(cf_vector* v1, cf_vector* v2)
{
	int v1_count = cf_vector_size(v1);
	int v2_count = cf_vector_size(v2);
	int v1_elem_sz = VECTOR_ELEM_SZ(v1);
	int v2_elem_sz = VECTOR_ELEM_SZ(v2);

	if (v1_count != v2_count || v1_elem_sz != v2_elem_sz) {
		return false;
	}

	for (int i = 0; i < v1_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* v1_element = cf_vector_getp(v1, i);
		void* v2_element = cf_vector_getp(v2, i);

		if (v1_element == v2_element) {
			// Same reference or both are NULL.
			continue;
		}

		if (v1_element == NULL || v2_element == NULL) {
			// Exactly one reference is NULL.
			return false;
		}

		if (memcmp(v1_element, v2_element, v1_elem_sz) != 0) {
			return false;
		}
	}

	return true;
}

/**
 * Find the index of an element in the vector. Equality is based on mem compare.
 *
 * @param vector the source vector.
 * @param element the element to find.
 * @return the index if the element is found, -1 otherwise.
 */
static int
vector_find(cf_vector* vector, void* element)
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
 * Copy all elements form the source vector to the destination vector only if
 * they do not exist in the destination vector. Assumes the source and
 * destination vector are not being modified while the copy operation is in
 * progress.
 *
 * @param dest the destination vector.
 * @param src the source vector.
 * @return the number of elements copied.
 */
static int
vector_copy_unique(cf_vector* dest, cf_vector* src)
{
	int element_count = cf_vector_size(src);
	int copied_count = 0;
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* src_element = cf_vector_getp(src, i);
		if (src_element) {
			cf_vector_append_unique(dest, src_element);
			copied_count++;
		}
	}
	return copied_count;
}

/**
 * Sorts in place the elements in the vector using the inout comparator function
 * and retains only unique elements. Assumes the source vector is not being
 * modified while the sort operation is in progress.
 *
 * @param src the source vector.
 * @return comparator the comparator function, which must return an integer less
 * than, equal to, or greater than zero if the first argument is  considered  to
 * be  respectively  less  than,  equal  to, or greater than the second
 */
static void
vector_sort_unique(cf_vector* src, int
(*comparator)(const void*, const void*))
{
	int element_count = cf_vector_size(src);
	size_t value_len = VECTOR_ELEM_SZ(src);
	size_t array_size = element_count * value_len;
	void* element_array = BUFFER_ALLOC_OR_DIE(array_size);

	// A lame approach to sorting. Copying the elements to an array and invoking
	// qsort.
	uint8_t* next_element_ptr = element_array;
	int array_element_count = 0;
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* src_element = cf_vector_getp(src, i);
		if (src_element) {
			memcpy(next_element_ptr, src_element, value_len);
			next_element_ptr += value_len;
			array_element_count++;
		}
	}

	qsort(element_array, array_element_count, value_len, comparator);

	vector_clear(src);
	next_element_ptr = element_array;
	for (int i = 0; i < array_element_count; i++) {
		cf_vector_append_unique(src, next_element_ptr);
		next_element_ptr += value_len;
	}

	BUFFER_FREE(element_array, array_size);
	return;
}

/**
 * Remove all elements from the to_remove vector present in the target vector.
 * Equality is based on simple mem compare.
 *
 * @param target the target vector being modified.
 * @param to_remove the vector whose elements must be removed from the target.
 * @return the number of elements removed.
 */
static int
vector_subtract(cf_vector* target, cf_vector* to_remove)
{
	int element_count = cf_vector_size(to_remove);
	int removed_count = 0;
	for (int i = 0; i < element_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		void* to_remove_element = cf_vector_getp(to_remove, i);
		if (to_remove_element) {
			int found_at = 0;
			while ((found_at = vector_find(target, to_remove_element)) >= 0) {
				cf_vector_delete(target, found_at);
				removed_count++;
			}
		}
	}

	return removed_count;
}

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
 * Copy elements in a vector to an array.
 * @param array the destination array. Should be large enough to hold the number
 * all elements in the vector.
 * @param src the source vector.
 * @param element_count the number of elements to copy from the source vector.
 */
static void
vector_array_cpy(void* array, cf_vector* src, int element_count)
{
	uint8_t* element_ptr = array;
	int element_size = VECTOR_ELEM_SZ(src);
	for (int i = 0; i < element_count; i++) {
		cf_vector_get(src, i, element_ptr);
		element_ptr += element_size;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Globals
 * ----------------------------------------------------------------------------
 */

/**
 * The big fat lock for all clustering state.
 */
static pthread_mutex_t g_clustering_lock =
		PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * The fat lock for all clustering events listener changes.
 */
static pthread_mutex_t g_clustering_event_publisher_lock =
		PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
 * Debugging lock acquition.
 * #define LOCK_DEBUG_ENABLED 1
 */
#ifdef LOCK_DEBUG_ENABLED
#define LOCK_DEBUG(format, ...) DEBUG(format, ##__VA_ARGS__)
#else
#define LOCK_DEBUG(format, ...)
#endif

/**
 * Acquire a lock on the clustering module.
 */
#define CLUSTERING_LOCK()						\
({												\
	pthread_mutex_lock (&g_clustering_lock);	\
	LOCK_DEBUG("locked in %s", __FUNCTION__);	\
})

/**
 * Relinquish the lock on the clustering module.
 */
#define CLUSTERING_UNLOCK()							\
({													\
	pthread_mutex_unlock (&g_clustering_lock);		\
	LOCK_DEBUG("unLocked in %s", __FUNCTION__);		\
})

/**
 * Acquire a lock on the clustering publisher.
 */
#define CLUSTERING_EVENT_PUBLISHER_LOCK()						\
({																\
	pthread_mutex_lock (&g_clustering_event_publisher_lock);	\
	LOCK_DEBUG("publisher locked in %s", __FUNCTION__);			\
})

/**
 * Relinquish the lock on the clustering publisher.
 */
#define CLUSTERING_EVENT_PUBLISHER_UNLOCK()						\
({																\
	pthread_mutex_unlock (&g_clustering_event_publisher_lock);	\
	LOCK_DEBUG("publisher unLocked in %s", __FUNCTION__);		\
})

/**
 * Singleton timer.
 */
static as_clustering_timer g_timer;

/**
 * Singleton external events publisher.
 */
static as_clustering_external_event_publisher g_external_event_publisher;

/**
 * Singleton cluster register to store this node's cluster membership.
 */
static as_clustering_register g_register;

/**
 * Singleton clustrering state all initialized to zero.
 */
static as_clustering g_clustering = { 0 };

/**
 * Singleton paxos proposer.
 */
static as_paxos_proposer g_proposer;

/**
 * Singleton paxos acceptor.
 */
static as_paxos_acceptor g_acceptor;

/**
 * Singleton quantum interval generator.
 */
static as_clustering_quantum_interval_generator g_quantum_interval_generator;

/**
 * Message template for heart beat messages.
 */
static msg_template g_clustering_msg_template[] = {

{ AS_CLUSTERING_MSG_ID, M_FT_UINT32 },

{ AS_CLUSTERING_MSG_TYPE, M_FT_UINT32 },

{ AS_CLUSTERING_MSG_HLC_TIMESTAMP, M_FT_UINT64 },

{ AS_CLUSTERING_MSG_SEQUENCE_NUMBER, M_FT_UINT64 },

{ AS_CLUSTERING_MSG_CLUSTER_KEY, M_FT_UINT64 },

{ AS_CLUSTERING_MSG_SUCCESSION_LIST, M_FT_BUF },

{ AS_CLUSTERING_MSG_PROPOSED_PRINCIPAL, M_FT_UINT64 }

};

/*
 * ----------------------------------------------------------------------------
 * Clustering life cycle
 * ----------------------------------------------------------------------------
 */

/**
 * Check if clustering is initialized.
 */
static bool
clustering_is_initialized()
{
	CLUSTERING_LOCK();
	bool initialized = (g_clustering.sys_state
			!= AS_CLUSTERING_SYS_STATE_UNINITIALIZED);
	CLUSTERING_UNLOCK();
	return initialized;
}

/**
 * * Check if clustering is running.
 */
static bool
clustering_is_running()
{
	CLUSTERING_LOCK();
	bool running = g_clustering.sys_state == AS_CLUSTERING_SYS_STATE_RUNNING;
	CLUSTERING_UNLOCK();
	return running;
}

/*
 * ----------------------------------------------------------------------------
 * Config related functions
 * ----------------------------------------------------------------------------
 */

/**
 * The nodeid for this node.
 */
static cf_node
config_self_nodeid_get()
{
	return g_config.self_node;
}

/*
 * ----------------------------------------------------------------------------
 * Compatibility mode functions
 * ----------------------------------------------------------------------------
 */

/**
 * Return current protocol version identifier.
 */
as_cluster_proto_identifier
clustering_protocol_identifier_get()
{
	return 0x707C;
}

/**
 * Compare clustering protocol versions for compatibility.
 */
bool
clustering_versions_are_compatible(as_cluster_proto_identifier v1,
		as_cluster_proto_identifier v2)
{
	return v1 == v2;
}

/*
 * ----------------------------------------------------------------------------
 * Timer event generator
 *
 * TODO: Can be abstracted out as a single scheduler single utility across
 * modules.
 * ----------------------------------------------------------------------------
 */

static void
timer_init()
{
	CLUSTERING_LOCK();
	memset(&g_timer, 0, sizeof(g_timer));
	CLUSTERING_UNLOCK();
}

/**
 * Clustering timer event generator thread, to help with retries and retransmits
 * across all states.
 */
static void*
timer_thr(void* arg)
{
	as_clustering_internal_event timer_event;
	memset(&timer_event, 0, sizeof(timer_event));
	timer_event.type = AS_CLUSTERING_INTERNAL_EVENT_TIMER;

	while (clustering_is_running()) {
		// Wait for a while and retry.
		internal_event_dispatch(&timer_event);
		usleep(timer_tick_interval() * 1000);
	}

	return NULL;
}

/**
 * Start the timer.
 */
static void
timer_start()
{
	CLUSTERING_LOCK();
	g_timer.timer_tid = cf_thread_create_joinable(timer_thr, NULL);
	CLUSTERING_UNLOCK();
}

/**
 * Stop the timer.
 */
static void
timer_stop()
{
	CLUSTERING_LOCK();
	cf_thread_join(g_timer.timer_tid);
	CLUSTERING_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Heartbeat subsystem interfacing
 * ----------------------------------------------------------------------------
 */

/*
 * The structure of data clustring subsystem pushes with in hb pulse messages
 * and retains as plugin data is as follows.
 *
 * Each row occupies 4 bytes.
 *
 * V5 heartbeat wire payload structure.
 * ===============================
 *
 * ------------|-------------|------------|------------|
 * |         Clustering Protocol identifier            |
 * |---------------------------------------------------|
 * |                                                   |
 * |-------- Cluster Key ------------------------------|
 * |                                                   |
 * |---------------------------------------------------|
 * |                                                   |
 * |-------- Paxos sequence number --------------------|
 * |                                                   |
 * |---------------------------------------------------|
 * |                                                   |
 * |-------- Preferred principal ----------------------|
 * |                                                   |
 * |---------------------------------------------------|
 * |         Length of succession list                 |
 * |---------------------------------------------------|
 * |                                                   |
 * |-------- Succ. Node id 0 --------------------------|
 * |                                                   |
 * |---------------------------------------------------|
 * |                                                   |
 * |-------- Succ. Node id 1 --------------------------|
 * |                                                   |
 * |---------------------------------------------------|
 * |                        .                          |
 * |                        .                          |
 *
 *
 * Cluster key and succession lists helps with detecting cluster integrity,
 * Plain clusterkey should be good enough but matching succession lists adds to
 * another level of safety (may not be required but being cautious).
 *
 * For orpahned node cluster key and length of succession list are set to zero.
 *
 * The parsed hb pluging data is just the same as the wire payload structure.
 * The plugin code ensure invalid content will never be parsed as plugin data to
 * memory. The direct implication is that if plugin data is not NULL,
 * required fields
 * - Clustering protocol identifier
 * - Cluster key
 * - Succession list length will always be present when read back from the
 * heartbeat subsystem and the succession list will be consistent with the
 * succession list length.
 */

/**
 * Read plugin data from hb layer for a node, using stack allocated space.
 * Will attempt a max of 3 attempts before crashing.
 * plugin_data_p->data_size will be zero and plugin_data_p->data will be NULL if
 * an entry for the node does not exist.
 */
#define clustering_hb_plugin_data_get(nodeid, plugin_data_p,				\
		hb_msg_hlc_ts_p, msg_recv_ts_p)										\
({																			\
	(plugin_data_p)->data_capacity = 1024;									\
	int tries_remaining = 3;												\
	bool enoent = false;													\
	bool rv = -1;															\
	while (tries_remaining--) {												\
		(plugin_data_p)->data = alloca((plugin_data_p)->data_capacity);		\
		if (as_hb_plugin_data_get(nodeid, AS_HB_PLUGIN_CLUSTERING,			\
				plugin_data_p, hb_msg_hlc_ts_p, msg_recv_ts_p) == 0) {		\
			rv = 0;															\
			break;															\
		}																	\
		if (errno == ENOENT) {												\
			enoent = true;													\
			break;															\
		}																	\
		if (errno == ENOMEM) {												\
			(plugin_data_p)->data_capacity = (plugin_data_p)->data_size;	\
		}																	\
	}																		\
	if (rv != 0 && !enoent && tries_remaining < 0) {						\
		CRASH("error allocating space for paxos hb plugin data");			\
	}																		\
	if (enoent) {															\
		(plugin_data_p)->data_size = 0;										\
		(plugin_data_p)->data = NULL;										\
	}																		\
	rv;																		\
})

/**
 * Get a pointer to the protocol identifier inside plugin data. Will be NULL if
 * plugin data is null or there are not enough bytes in the data to hold the
 * identifier.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to the protocol identifier on success, NULL on failure.
 */
static as_cluster_proto_identifier*
clustering_hb_plugin_proto_get(void* plugin_data, size_t plugin_data_size)
{
	if (plugin_data == NULL
			|| plugin_data_size < sizeof(as_cluster_proto_identifier)) {
		// The data does not hold valid data or there is no cluster key and or
		// succession list is missing.
		return NULL;
	}

	return (as_cluster_proto_identifier*)plugin_data;
}

/**
 * Retrieves the cluster key from clustering hb plugin data.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to the cluster key on success, NULL on failure.
 */
static as_cluster_key*
clustering_hb_plugin_cluster_key_get(void* plugin_data, size_t plugin_data_size)
{
	uint8_t* proto = (uint8_t*)clustering_hb_plugin_proto_get(plugin_data,
			plugin_data_size);
	if (proto == NULL) {
		// The data does not hold valid data.
		return NULL;
	}

	if ((uint8_t*)plugin_data + plugin_data_size
			< proto + sizeof(as_cluster_proto_identifier)
					+ sizeof(as_cluster_key)) {
		// Not enough bytes for cluster key.
		return NULL;
	}

	return (as_cluster_key*)(proto + sizeof(as_cluster_proto_identifier));
}

/**
 * Retrieves the sequence number from clustering hb plugin data.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to the sequence number on success, NULL on failure.
 */
static as_paxos_sequence_number*
clustering_hb_plugin_sequence_number_get(void* plugin_data,
		size_t plugin_data_size)
{
	uint8_t* cluster_key = (uint8_t*)clustering_hb_plugin_cluster_key_get(
			plugin_data, plugin_data_size);
	if (cluster_key == NULL) {
		// The data does not hold valid data or there is no cluster key.
		return NULL;
	}

	if ((uint8_t*)plugin_data + plugin_data_size
			< cluster_key + sizeof(as_cluster_key)
					+ sizeof(as_paxos_sequence_number)) {
		// Not enough bytes for succession list length.
		return NULL;
	}

	return (as_paxos_sequence_number*)(cluster_key + sizeof(as_cluster_key));
}

/**
 * Retrieves the preferred principal from clustering hb plugin data.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to the preferred principal on success, NULL on failure.
 */
static cf_node*
clustering_hb_plugin_preferred_principal_get(void* plugin_data,
		size_t plugin_data_size)
{
	uint8_t* sequence_number_p =
			(uint8_t*)clustering_hb_plugin_sequence_number_get(plugin_data,
					plugin_data_size);
	if (sequence_number_p == NULL) {
		// The data does not hold valid data or there is no sequence number.
		return NULL;
	}

	if ((uint8_t*)plugin_data + plugin_data_size
			< sequence_number_p + sizeof(as_paxos_sequence_number)
					+ sizeof(cf_node)) {
		// Not enough bytes for preferred principal.
		return NULL;
	}

	return (as_paxos_sequence_number*)(sequence_number_p
			+ sizeof(as_paxos_sequence_number));
}

/**
 * Retrieves the succession list length pointer from clustering hb plugin data.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to succession list length on success, NULL on failure.
 */
static uint32_t*
clustering_hb_plugin_succession_length_get(void* plugin_data,
		size_t plugin_data_size)
{
	uint8_t* preferred_principal_p =
			(uint8_t*)clustering_hb_plugin_preferred_principal_get(plugin_data,
					plugin_data_size);
	if (preferred_principal_p == NULL) {
		// The data does not hold valid data or there is no preferred principal
		// and or succession list is missing.
		return NULL;
	}

	if ((uint8_t*)plugin_data + plugin_data_size
			< preferred_principal_p + sizeof(cf_node) + sizeof(uint32_t)) {
		// Not enough bytes for succession list length.
		return NULL;
	}

	return (uint32_t*)(preferred_principal_p + sizeof(cf_node));
}

/**
 * Retrieves the pointer to the first node in the succession list.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to first node in succession list on success, NULL on failure
 * or if the succession list is empty.
 */
static cf_node*
clustering_hb_plugin_succession_get(void* plugin_data, size_t plugin_data_size)
{
	uint8_t* succession_list_length_p =
			(uint8_t*)clustering_hb_plugin_succession_length_get(plugin_data,
					plugin_data_size);
	if (succession_list_length_p == NULL) {
		// The data does not hold valid data or there is no cluster key and or
		// succession list is missing.
		return NULL;
	}

	if (*(uint32_t*)succession_list_length_p == 0) {
		// Empty succession list.
		return NULL;
	}

	if ((uint8_t*)plugin_data + plugin_data_size
			< succession_list_length_p + sizeof(uint32_t)
					+ (sizeof(cf_node) * (*(uint32_t*)succession_list_length_p))) {
		// Not enough bytes for succession list length.
		return NULL;
	}

	return (cf_node*)(succession_list_length_p + sizeof(uint32_t));
}

/**
 * Validate the correctness of plugin data. By ensuring all required fields are
 * present and the succession list matches the provided length.
 * @param plugin_data can be NULL.
 * @param plugin_data_size the size of plugin data.
 * @return pointer to first node in succession list on success, NULL on failure.
 */
static bool
clustering_hb_plugin_data_is_valid(void* plugin_data, size_t plugin_data_size)
{
	void* proto_identifier_p = clustering_hb_plugin_proto_get(plugin_data,
			plugin_data_size);
	if (proto_identifier_p == NULL) {
		DEBUG("plugin data missing protocol identifier");
		return false;
	}

	as_cluster_proto_identifier current_proto_identifier =
			clustering_protocol_identifier_get();
	if (!clustering_versions_are_compatible(current_proto_identifier,
			*(as_cluster_proto_identifier*)proto_identifier_p)) {
		DEBUG("protocol versions incompatible - expected %"PRIx32" but was: %"PRIx32,
				current_proto_identifier,
				*(as_cluster_proto_identifier*)proto_identifier_p);
		return false;
	}

	void* cluster_key_p = clustering_hb_plugin_cluster_key_get(plugin_data,
			plugin_data_size);
	if (cluster_key_p == NULL) {
		DEBUG("plugin data missing cluster key");
		return false;
	}

	void* sequence_number_p = clustering_hb_plugin_sequence_number_get(
			plugin_data, plugin_data_size);
	if (sequence_number_p == NULL) {
		DEBUG("plugin data missing sequence number");
		return false;
	}

	void* preferred_principal_p = clustering_hb_plugin_preferred_principal_get(
			plugin_data, plugin_data_size);
	if (preferred_principal_p == NULL) {
		DEBUG("plugin data missing preferred principal");
		return false;
	}

	uint32_t* succession_list_length_p =
			(void*)clustering_hb_plugin_succession_length_get(plugin_data,
					plugin_data_size);
	if (succession_list_length_p == NULL) {
		DEBUG("plugin data missing succession list length");
		return false;
	}

	void* succession_list_p = clustering_hb_plugin_succession_get(plugin_data,
			plugin_data_size);

	if (*succession_list_length_p > 0 && succession_list_p == NULL) {
		DEBUG("succession list length %d, but succession list is empty",
				*succession_list_length_p);
		return false;
	}

	return true;
}

/**
 * Determines if the plugin data with hb subsystem is old to be ignored.
 * ALL access to plugin data should be vetted through this function.  The plugin
 * data is obsolete if it was send before the current cluster state or has a
 * version mismatch.
 *
 * This is detemined by comparing the plugin data hb message hlc timestamp and
 * monotonic timestamps with the cluster formation hlc and monotonic times.
 *
 * @param cluster_modified_hlc_ts the hlc timestamp when current cluster change
 * happened. Sent to avoid locking in this function.
 * @param cluster_modified_time  the monotonic timestamp when current cluster
 * change happened. Sento to avoid locking in this function.
 * @param plugin_data the plugin data.
 * @param plugin_data_size the size of plugin data.
 * @param msg_recv_ts the monotonic timestamp for plugin data receive.
 * @param hb_msg_hlc_ts the hlc timestamp for plugin data receive.
 * @return true if plugin data is obsolete, false otherwise.
 */
static bool
clustering_hb_plugin_data_is_obsolete(as_hlc_timestamp cluster_modified_hlc_ts,
		cf_clock cluster_modified_time, void* plugin_data,
		size_t plugin_data_size, cf_clock msg_recv_ts,
		as_hlc_msg_timestamp* hb_msg_hlc_ts)
{
	if (!clustering_hb_plugin_data_is_valid(plugin_data, plugin_data_size)) {
		// Plugin data is invalid. Assume it to be obsolete.
		// Seems like a redundant check but required in case clustering protocol
		// was switched to an incompatible version.
		return true;
	}

	if (as_hlc_send_timestamp_order(cluster_modified_hlc_ts, hb_msg_hlc_ts)
			!= AS_HLC_HAPPENS_BEFORE) {
		// Cluster formation time after message send or the order is unknown,
		// assume cluster formation is after message send. the caller should
		// ignore this message.
		return true;
	}

	// HB data should be atleast after cluster formation time + one hb interval
	// to send out our cluster state + one network delay for our information to
	// reach the remote node + one hb interval for the other node to send out
	// the his updated state + one network delay for the updated state to reach
	// us.
	if (cluster_modified_time + 2 * as_hb_tx_interval_get()
			+ 2 * g_config.fabric_latency_max_ms > msg_recv_ts) {
		return true;
	}

	return false;
}

/**
 * Indicates if the plugin data for a node indicates that it is an orphan node.
 */
static as_clustering_peer_node_state
clustering_hb_plugin_data_node_status(void* plugin_data,
		size_t plugin_data_size)
{
	if (!clustering_hb_plugin_data_is_valid(plugin_data, plugin_data_size)) {
		// Either we have not hb channel to this node or it has sen invalid
		// plugin data. Assuming the cluster state is unknown.
		return AS_NODE_UNKNOWN;
	}

	as_cluster_key* cluster_key = clustering_hb_plugin_cluster_key_get(
			plugin_data, plugin_data_size);

	if (*cluster_key == 0) {
		return AS_NODE_ORPHAN;
	}

	// Redundant paranoid check.
	uint32_t* succession_list_length_p =
			clustering_hb_plugin_succession_length_get(plugin_data,
					plugin_data_size);

	if (*succession_list_length_p == 0) {
		return AS_NODE_ORPHAN;
	}

	return AS_NODE_CLUSTER_ASSIGNED;
}

/**
 * Push clustering payload into a heartbeat pulse message. The payload format is
 * as described above.
 */
static void
clustering_hb_plugin_set_fn(msg* msg)
{
	if (!clustering_is_initialized()) {
		// Clustering not initialized. Send no data at all.
		return;
	}

	CLUSTERING_LOCK();

	uint32_t cluster_size = cf_vector_size(&g_register.succession_list);

	size_t payload_size =
		// For the paxos version identifier
		sizeof(uint32_t)
			// For cluster key
			+ sizeof(as_cluster_key)
				// For sequence number
				+ sizeof(as_paxos_sequence_number)
					// For preferred principal
					+ sizeof(cf_node)
						// For succession list length.
						+ sizeof(uint32_t)
							// For succession list.
							+ (sizeof(cf_node) * cluster_size);

	uint8_t* payload = alloca(payload_size);

	uint8_t* current_field_p = payload;

	// Set the paxos protocol identifier.
	uint32_t protocol = clustering_protocol_identifier_get();
	memcpy(current_field_p, &protocol, sizeof(protocol));
	current_field_p += sizeof(protocol);

	// Set cluster key.
	memcpy(current_field_p, &g_register.cluster_key,
			sizeof(g_register.cluster_key));
	current_field_p += sizeof(g_register.cluster_key);

	// Set the sequence number.
	memcpy(current_field_p, &g_register.sequence_number,
			sizeof(g_register.sequence_number));
	current_field_p += sizeof(g_register.sequence_number);

	// Set the preferred principal.
	memcpy(current_field_p, &g_clustering.preferred_principal,
			sizeof(g_clustering.preferred_principal));
	current_field_p += sizeof(g_clustering.preferred_principal);

	// Set succession length
	memcpy(current_field_p, &cluster_size, sizeof(cluster_size));
	current_field_p += sizeof(cluster_size);

	// Copy over the succession list.
	cf_node* succession = (cf_node*)(current_field_p);
	for (int i = 0; i < cluster_size; i++) {
		cf_vector_get(&g_register.succession_list, i, &succession[i]);
	}

	msg_set_buf(msg, AS_HB_MSG_PAXOS_DATA, payload, payload_size, MSG_SET_COPY);

	CLUSTERING_UNLOCK();
}

/**
 * Plugin parse function that copies the msg payload verbatim to a plugin data.
 */
static void
clustering_hb_plugin_parse_data_fn(msg* msg, cf_node source,
		as_hb_plugin_node_data* prev_plugin_data,
		as_hb_plugin_node_data* plugin_data)
{
	// Lockless check to prevent deadlocks.
	if (g_clustering.sys_state == AS_CLUSTERING_SYS_STATE_UNINITIALIZED) {
		// Ignore this heartbeat.
		plugin_data->data_size = 0;
		return;
	}

	void* payload;
	size_t payload_size;

	if (msg_get_buf(msg, AS_HB_MSG_PAXOS_DATA, (uint8_t**)&payload,
					&payload_size, MSG_GET_DIRECT) != 0) {
		cf_ticker_warning(AS_CLUSTERING,
				"received empty clustering payload in heartbeat pulse from node %"PRIx64,
				source);
		plugin_data->data_size = 0;
		return;
	}

	// Validate and retain only valid plugin data.
	if (!clustering_hb_plugin_data_is_valid(payload, payload_size)) {
		cf_ticker_warning(AS_CLUSTERING,
				"received invalid clustering payload in heartbeat pulse from node %"PRIx64,
				source);
		plugin_data->data_size = 0;
		return;
	}

	if (payload_size > plugin_data->data_capacity) {
		// Round up to nearest multiple of block size to prevent very frequent
		// reallocation.
		size_t data_capacity = ((payload_size + HB_PLUGIN_DATA_BLOCK_SIZE - 1)
				/ HB_PLUGIN_DATA_BLOCK_SIZE) * HB_PLUGIN_DATA_BLOCK_SIZE;

		// Reallocate since we have outgrown existing capacity.
		plugin_data->data = cf_realloc(plugin_data->data, data_capacity);
		plugin_data->data_capacity = data_capacity;
	}

	plugin_data->data_size = payload_size;
	memcpy(plugin_data->data, payload, payload_size);
}

/**
 * Check if the input succession list from hb plugin data matches, with a
 * succession list vector.
 * @param succession_list the first succession list.
 * @param succession_list_length the length of the succession list.
 * @param succession_list_vector the second succession list as a vector. Should
 * be protected from multithreaded access while this function is running.
 * @return true if the succcession lists are equal, false otherwise.
 */
bool
clustering_hb_succession_list_matches(cf_node* succession_list,
		uint32_t succession_list_length, cf_vector* succession_list_vector)
{
	if (succession_list_length != cf_vector_size(succession_list_vector)) {
		return false;
	}

	for (uint32_t i = 0; i < succession_list_length; i++) {
		cf_node* vector_element = cf_vector_getp(succession_list_vector, i);
		if (vector_element == NULL || *vector_element != succession_list[i]) {
			return false;
		}
	}
	return true;
}

/*
 * ----------------------------------------------------------------------------
 * Quantum interval generator
 * ----------------------------------------------------------------------------
 */

/**
 * Time taken for the effect of a fault to get propogated via HB.
 */
static uint32_t
quantum_interval_hb_fault_comm_delay()
{
	return as_hb_tx_interval_get() + network_latency_max();
}

/**
 * Quantum wait time after node arrived event.
 */
static uint32_t
quantum_interval_node_arrived_wait_time(as_clustering_quantum_fault* fault)
{
	return MIN(quantum_interval(),
			(fault->last_event_ts - fault->event_ts) / 2
					+ 2 * quantum_interval_hb_fault_comm_delay()
					+ quantum_interval() / 2);
}

/**
 * Quantum wait time after node departs.
 */
static uint32_t
quantum_interval_node_departed_wait_time(as_clustering_quantum_fault* fault)
{
	return MIN(quantum_interval(),
			as_hb_node_timeout_get()
					+ 2 * quantum_interval_hb_fault_comm_delay()
					+ quantum_interval() / 4);
}

/**
 * Quantum wait time after a peer nodes adjacency changed.
 */
static uint32_t
quantum_interval_peer_adjacency_changed_wait_time(
		as_clustering_quantum_fault* fault)
{
	return MIN(quantum_interval(), quantum_interval_hb_fault_comm_delay());
}

/**
 * Quantum wait time after accepting a join request.
 */
static uint32_t
quantum_interval_join_accepted_wait_time(as_clustering_quantum_fault* fault)
{
	// Ensure we wait for atleast one heartbeat interval to receive the latest
	// heartbeat after the last join request and for other nodes to send their
	// join requests as well.
	return MIN(quantum_interval(),
			(fault->last_event_ts - fault->event_ts)
					+ join_cluster_check_interval() + network_latency_max()
					+ as_hb_tx_interval_get());
}

/**
 * Quantum wait time after principal node departs.
 */
static uint32_t
quantum_interval_principal_departed_wait_time(
		as_clustering_quantum_fault* fault)
{
	// Anticipate an incoming join request from other orphaned cluster members.
	return MIN(quantum_interval(),
			as_hb_node_timeout_get()
					+ 2 * quantum_interval_hb_fault_comm_delay()
					+ MAX(quantum_interval() / 4,
							quantum_interval_join_accepted_wait_time(fault)));
}

/**
 * Quantum wait time after seeing a cluster that might send us a join request.
 */
static uint32_t
quantum_interval_inbound_merge_candidate_wait_time(
		as_clustering_quantum_fault* fault)
{
	return quantum_interval();
}

/**
 * Quantum wait time after a cluster member has been orphaned.
 */
static uint32_t
quantum_interval_member_orphaned_wait_time(as_clustering_quantum_fault* fault)
{
	return quantum_interval();
}

/**
 * Marks the current quantum interval as skipped. A kludge to allow quantum to
 * allow quantum interval generator to mark quantum intervals as postponed.
 */
static void
quantum_interval_mark_postponed()
{
	CLUSTERING_LOCK();
	g_quantum_interval_generator.is_interval_postponed = true;
	CLUSTERING_UNLOCK();
}

/**
 * Update the vtable for a fault.
 */
static void
quantum_interval_vtable_update(as_clustering_quantum_fault_type type,
		char *fault_log_str, as_clustering_quantum_fault_wait_fn wait_fn)
{
	CLUSTERING_LOCK();
	g_quantum_interval_generator.vtable[type].fault_log_str = fault_log_str;
	g_quantum_interval_generator.vtable[type].wait_fn = wait_fn;
	CLUSTERING_UNLOCK();
}

/**
 * Initialize quantum interval generator.
 */
static void
quantum_interval_generator_init()
{
	CLUSTERING_LOCK();
	memset(&g_quantum_interval_generator, 0,
			sizeof(g_quantum_interval_generator));
	g_quantum_interval_generator.last_quantum_start_time = cf_getms();
	g_quantum_interval_generator.last_quantum_interval = quantum_interval();

	// Initialize the vtable.
	quantum_interval_vtable_update(QUANTUM_FAULT_NODE_ARRIVED, "node arrived",
			quantum_interval_node_arrived_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_NODE_DEPARTED, "node departed",
			quantum_interval_node_departed_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_PRINCIPAL_DEPARTED,
			"principal departed",
			quantum_interval_principal_departed_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_PEER_ADJACENCY_CHANGED,
			"peer adjacency changed",
			quantum_interval_peer_adjacency_changed_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_JOIN_ACCEPTED,
			"join request accepted", quantum_interval_join_accepted_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_INBOUND_MERGE_CANDIDATE_SEEN,
			"merge candidate seen",
			quantum_interval_inbound_merge_candidate_wait_time);
	quantum_interval_vtable_update(QUANTUM_FAULT_CLUSTER_MEMBER_ORPHANED,
			"member orphaned", quantum_interval_member_orphaned_wait_time);

	CLUSTERING_UNLOCK();
}

/**
 * Get the earliest possible monotonic clock time the next quantum interval can
 * start.
 *
 * Start quantum interval after the last update to any one of adjacency,
 * pending_join_requests , neighboring_principals. The heuristic is that these
 * should be stable to initiate cluster merge / join or cluster formation
 * requests.
 */
static cf_clock
quantum_interval_earliest_start_time()
{
	CLUSTERING_LOCK();
	cf_clock fault_event_time = 0;
	for (int i = 0; i < QUANTUM_FAULT_TYPE_SENTINEL; i++) {
		if (g_quantum_interval_generator.fault[i].event_ts) {
			fault_event_time = MAX(fault_event_time,
					g_quantum_interval_generator.fault[i].event_ts
							+ g_quantum_interval_generator.vtable[i].wait_fn(
									&g_quantum_interval_generator.fault[i]));
		}

		DETAIL("Fault:%s event_ts:%"PRIu64,
				g_quantum_interval_generator.vtable[i].fault_log_str,
				g_quantum_interval_generator.fault[i].event_ts);
	}

	DETAIL("Last Quantum interval:%"PRIu64,
			g_quantum_interval_generator.last_quantum_start_time);

	cf_clock start_time = g_quantum_interval_generator.last_quantum_start_time
			+ quantum_interval();
	if (fault_event_time) {
		// Ensure we have at least 1/2 quantum interval of separation between
		// quantum intervals to give chance to multiple fault events that  are
		// resonably close in time.
		start_time = MAX(
				g_quantum_interval_generator.last_quantum_start_time
						+ quantum_interval() / 2, fault_event_time);
	}
	CLUSTERING_UNLOCK();

	return start_time;
}

/**
 * Reset quantum interval fault.
 * @param fault_type the fault type.
 */
static void
quantum_interval_fault_reset(as_clustering_quantum_fault_type fault_type)
{
	CLUSTERING_LOCK();
	memset(&g_quantum_interval_generator.fault[fault_type], 0,
			sizeof(g_quantum_interval_generator.fault[fault_type]));
	CLUSTERING_UNLOCK();
}

/**
 * Update a fault event based on the current fault ts.
 * @param fault the fault to update.
 * @param fault_ts the new fault timestamp
 * @param src_nodeid the fault causing nodeid, 0 if the nodeid is not known.
 */
static void
quantum_interval_fault_update(as_clustering_quantum_fault_type fault_type,
		cf_clock fault_ts, cf_node src_nodeid)
{
	CLUSTERING_LOCK();
	as_clustering_quantum_fault* fault =
			&g_quantum_interval_generator.fault[fault_type];
	if (fault->event_ts == 0
			|| fault_ts - fault->event_ts > quantum_interval() / 2) {
		// Fault event detected first time in this quantum or we are seeing the
		// effect of a different event more than half quantum apart.
		fault->event_ts = fault_ts;
		DETAIL("updated '%s' fault with ts %"PRIu64" for node %"PRIx64,
				g_quantum_interval_generator.vtable[fault_type].fault_log_str, fault_ts, src_nodeid);
	}

	fault->last_event_ts = fault_ts;
	CLUSTERING_UNLOCK();
}

/**
 * Reset the state for the next quantum interval.
 */
static void
quantum_interval_generator_reset(cf_clock last_quantum_start_time)
{
	CLUSTERING_LOCK();
	if (!g_quantum_interval_generator.is_interval_postponed) {
		// Update last quantum interval.
		g_quantum_interval_generator.last_quantum_interval = MAX(0,
				last_quantum_start_time
						- g_quantum_interval_generator.last_quantum_start_time);

		g_quantum_interval_generator.last_quantum_start_time =
				last_quantum_start_time;
		for (int i = 0; i < QUANTUM_FAULT_TYPE_SENTINEL; i++) {
			quantum_interval_fault_reset(i);
		}
	}
	g_quantum_interval_generator.is_interval_postponed = false;

	CLUSTERING_UNLOCK();
}

/**
 * Handle timer event and generate a quantum internal event if required.
 */
static void
quantum_interval_generator_timer_event_handle(
		as_clustering_internal_event* timer_event)
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();

	cf_clock earliest_quantum_start_time =
			quantum_interval_earliest_start_time();

	cf_clock expected_quantum_start_time =
			g_quantum_interval_generator.last_quantum_start_time
					+ g_quantum_interval_generator.last_quantum_interval;

	// Provide a buffer for current quantum interval to finish gracefully as
	// long as it is less than half a quantum interval.
	cf_clock quantum_wait_buffer = MIN(
			earliest_quantum_start_time > expected_quantum_start_time ?
					earliest_quantum_start_time - expected_quantum_start_time :
					0, g_quantum_interval_generator.last_quantum_interval / 2);

	// Fire quantum interval start event if it is time, or if we have skipped
	// quantum interval start for more that the max skip number of intervals.
	// Add a buffer of wait time to ensure we wait a bit more if we can cover
	// the waiting time.
	bool is_skippable = g_quantum_interval_generator.last_quantum_start_time
			+ (quantum_interval_skip_max() + 1)
					* g_quantum_interval_generator.last_quantum_interval
			+ quantum_wait_buffer > now;
	bool fire_quantum_event = earliest_quantum_start_time <= now
			|| !is_skippable;
	CLUSTERING_UNLOCK();

	if (fire_quantum_event) {
		as_clustering_internal_event timer_event;
		memset(&timer_event, 0, sizeof(timer_event));
		timer_event.type = AS_CLUSTERING_INTERNAL_EVENT_QUANTUM_INTERVAL_START;
		timer_event.quantum_interval_is_skippable = is_skippable;
		internal_event_dispatch(&timer_event);

		// Reset for next interval generation.
		quantum_interval_generator_reset(now);
	}
}

/**
 * Check if the interval generator has seen an adjacency fault in the current
 * quantum interval.
 * @return true if the quantum interval generator has seen an adjacency fault,
 * false otherwise.
 */
static bool
quantum_interval_is_adjacency_fault_seen()
{
	CLUSTERING_LOCK();
	bool is_fault_seen =
			g_quantum_interval_generator.fault[QUANTUM_FAULT_NODE_ARRIVED].event_ts
					|| g_quantum_interval_generator.fault[QUANTUM_FAULT_NODE_DEPARTED].event_ts
					|| g_quantum_interval_generator.fault[QUANTUM_FAULT_PRINCIPAL_DEPARTED].event_ts;
	CLUSTERING_UNLOCK();
	return is_fault_seen;
}

/**
 * Check if the interval generator has seen a peer node adjacency changed fault
 * in current quantum interval.
 * @return true if the quantum interval generator has seen a peer node adjacency
 * changed fault,
 * false otherwise.
 */
static bool
quantum_interval_is_peer_adjacency_fault_seen()
{
	CLUSTERING_LOCK();
	bool is_fault_seen =
			g_quantum_interval_generator.fault[QUANTUM_FAULT_PEER_ADJACENCY_CHANGED].event_ts;
	CLUSTERING_UNLOCK();
	return is_fault_seen;
}

/**
 * Update the fault time for this quantum on self heartbeat adjacency list
 * change.
 */
static void
quantum_interval_generator_hb_event_handle(
		as_clustering_internal_event* hb_event)
{
	CLUSTERING_LOCK();

	cf_clock min_event_time[AS_HB_NODE_EVENT_SENTINEL];
	cf_clock min_event_node[AS_HB_NODE_EVENT_SENTINEL];

	memset(min_event_time, 0, sizeof(min_event_time));
	memset(min_event_node, 0, sizeof(min_event_node));

	as_hb_event_node* events = hb_event->hb_events;
	for (int i = 0; i < hb_event->hb_n_events; i++) {
		if (min_event_time[events[i].evt] == 0
				|| min_event_time[events[i].evt] > events[i].event_time) {
			min_event_time[events[i].evt] = events[i].event_time;
			min_event_node[events[i].evt] = events[i].nodeid;
		}

		if (events[i].evt == AS_HB_NODE_DEPART
				&& clustering_is_our_principal(events[i].nodeid)) {
			quantum_interval_fault_update(QUANTUM_FAULT_PRINCIPAL_DEPARTED,
					events[i].event_time, events[i].nodeid);
		}
	}

	for (int i = 0; i < AS_HB_NODE_EVENT_SENTINEL; i++) {
		if (min_event_time[i]) {
			switch (i) {
			case AS_HB_NODE_ARRIVE:
				quantum_interval_fault_update(QUANTUM_FAULT_NODE_ARRIVED,
						min_event_time[i], min_event_node[i]);
				break;
			case AS_HB_NODE_DEPART:
				quantum_interval_fault_update(QUANTUM_FAULT_NODE_DEPARTED,
						min_event_time[i], min_event_node[i]);
				break;
			case AS_HB_NODE_ADJACENCY_CHANGED:
				if (clustering_is_cluster_member(min_event_node[i])) {
					quantum_interval_fault_update(
							QUANTUM_FAULT_PEER_ADJACENCY_CHANGED,
							min_event_time[i], min_event_node[i]);
				}
				break;
			default:
				break;
			}

		}
	}
	CLUSTERING_UNLOCK();
}

/**
 * Update the fault time for this quantum on clustering information for an
 * adjacent node change. Assumes the node's plugin data is not obsolete.
 */
static void
quantum_interval_generator_hb_plugin_data_changed_handle(
		as_clustering_internal_event* change_event)
{
	CLUSTERING_LOCK();

	if (clustering_hb_plugin_data_is_obsolete(
			g_register.cluster_modified_hlc_ts,
			g_register.cluster_modified_time, change_event->plugin_data->data,
			change_event->plugin_data->data_size,
			change_event->plugin_data_changed_ts,
			&change_event->plugin_data_changed_hlc_ts)) {
		// The plugin data is obsolete. Can't take decisions based on it.
		goto Exit;
	}

	// Get the changed node's succession list, cluster key. All the fields
	// should be present since the obsolete check also checked for fields being
	// valid.
	cf_node* succession_list_p = clustering_hb_plugin_succession_get(
			change_event->plugin_data->data,
			change_event->plugin_data->data_size);
	uint32_t* succession_list_length_p =
			clustering_hb_plugin_succession_length_get(
					change_event->plugin_data->data,
					change_event->plugin_data->data_size);

	if (*succession_list_length_p > 0
			&& !clustering_is_our_principal(succession_list_p[0])
			&& clustering_is_principal()) {
		if (succession_list_p[0] < config_self_nodeid_get()) {
			// We are seeing a new principal who could potentially merge with
			// this cluster.
			if (g_quantum_interval_generator.fault[QUANTUM_FAULT_INBOUND_MERGE_CANDIDATE_SEEN].event_ts
					!= 1) {
				quantum_interval_fault_update(
						QUANTUM_FAULT_INBOUND_MERGE_CANDIDATE_SEEN, cf_getms(),
						change_event->plugin_data_changed_nodeid);
			}
		}
		else {
			// We see a cluster with higher nodeid and most probably we will not
			// be the principal of the merged cluster. Reset the fault
			// timestamp, however set it to 1 to differentiate between no fault
			// and a fault to be ingnored in this quantum interval. A value of 1
			// for practical purposes will never push the quantum interval
			// forward.
			quantum_interval_fault_update(
					QUANTUM_FAULT_INBOUND_MERGE_CANDIDATE_SEEN, 1,
					change_event->plugin_data_changed_nodeid);
		}
	}
	else {
		if (clustering_is_principal() && *succession_list_length_p == 0
				&& vector_find(&g_register.succession_list,
						&change_event->plugin_data_changed_nodeid) >= 0) {
			// One of our cluster members switched to orphan state. Most likely
			// a quick restart.
			quantum_interval_fault_update(QUANTUM_FAULT_CLUSTER_MEMBER_ORPHANED,
					cf_getms(), change_event->plugin_data_changed_nodeid);
		}
		else {
			// A node becoming an orphan node or seeing a succession with our
			// principal does not mean we have seen a new cluster.
		}
	}
Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Update the fault time for this quantum on self heartbeat adjacency list
 * change.
 */
static void
quantum_interval_generator_join_request_accepted_handle(
		as_clustering_internal_event* join_request_event)
{
	quantum_interval_fault_update(QUANTUM_FAULT_JOIN_ACCEPTED, cf_getms(),
			join_request_event->join_request_source_nodeid);
}

/**
 * Dispatch internal clustering events for the quantum interval generator.
 */
static void
quantum_interval_generator_event_dispatch(as_clustering_internal_event* event)
{
	switch (event->type) {
	case AS_CLUSTERING_INTERNAL_EVENT_TIMER:
		quantum_interval_generator_timer_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_HB:
		quantum_interval_generator_hb_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_HB_PLUGIN_DATA_CHANGED:
		quantum_interval_generator_hb_plugin_data_changed_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_JOIN_REQUEST_ACCEPTED:
		quantum_interval_generator_join_request_accepted_handle(event);
		break;
	default:
		break;
	}
}

/**
 * Start quantum interval generator.
 */
static void
quantum_interval_generator_start()
{
	CLUSTERING_LOCK();
	g_quantum_interval_generator.last_quantum_start_time = cf_getms();
	CLUSTERING_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Clustering common
 * ----------------------------------------------------------------------------
 */

/**
 * Generate a new random and most likely a unique cluster key.
 * @param current_cluster_key current cluster key to prevent collision.
 * @return randomly generated cluster key.
 */
static as_cluster_key
clustering_cluster_key_generate(as_cluster_key current_cluster_key)
{
	// Generate one uuid and use this for the cluster key
	as_cluster_key cluster_key = 0;

	// Generate a non-zero cluster key that fits in 6 bytes.
	while ((cluster_key = (cf_get_rand64() >> 16)) == 0
			|| cluster_key == current_cluster_key) {
		;
	}

	return cluster_key;
}

/**
 * Indicates if this node is an orphan. A node is deemed orphan if it is not a
 * memeber of any cluster.
 */
static bool
clustering_is_orphan()
{
	CLUSTERING_LOCK();

	bool is_orphan = cf_vector_size(&g_register.succession_list) <= 0
			|| g_register.cluster_key == 0;

	CLUSTERING_UNLOCK();

	return is_orphan;
}

/**
 * Return the principal node for current cluster.
 * @param principal (output) the current principal for the cluster.
 * @return 0 if there is a valid principal, -1 if the node is in orphan state
 * and there is no valid principal.
 */
static int
clustering_principal_get(cf_node* principal)
{
	CLUSTERING_LOCK();
	int rv = -1;

	if (cf_vector_get(&g_register.succession_list, 0, principal) == 0) {
		rv = 0;
	}

	CLUSTERING_UNLOCK();

	return rv;
}

/**
 * Indicates if this node is the principal for its cluster.
 */
static bool
clustering_is_principal()
{
	CLUSTERING_LOCK();
	cf_node current_principal;

	bool is_principal = clustering_principal_get(&current_principal) == 0
			&& current_principal == config_self_nodeid_get();

	CLUSTERING_UNLOCK();

	return is_principal;
}

/**
 * Indicates if input node is this node's principal. Input node can be self node
 * as well.
 */
static bool
clustering_is_our_principal(cf_node nodeid)
{
	CLUSTERING_LOCK();
	cf_node current_principal;

	bool is_principal = clustering_principal_get(&current_principal) == 0
			&& current_principal == nodeid;

	CLUSTERING_UNLOCK();

	return is_principal;
}

/**
 * Indicates if a node is our cluster member.
 */
static bool
clustering_is_cluster_member(cf_node nodeid)
{
	CLUSTERING_LOCK();
	bool is_member = vector_find(&g_register.succession_list, &nodeid) >= 0;
	CLUSTERING_UNLOCK();
	return is_member;
}

/**
 * Indicates if the input node is present in a succession list.
 * @param nodeid the nodeid to search.
 * @param succession_list the succession list.
 * @param succession_list_length the length of the succession list.
 * @return true if the node is present in the succession list, false otherwise.
 */
static bool
clustering_is_node_in_succession(cf_node nodeid, cf_node* succession_list,
		int succession_list_length)
{
	for (int i = 0; i < succession_list_length; i++) {
		if (succession_list[i] == nodeid) {
			return true;
		}
	}

	return false;
}

/**
 * Indicates if the input node can be accepted as this a paxos proposer. We can
 * accept the new node as our principal if we are in the orphan state or if the
 * input node is already our principal.
 *
 * Note: In case we send a join request to a node with a lower node id,  input
 * node's nodeid can be less than our nodeid. This is still valid as the
 * proposer who will hand over the principalship to us once paxos round is over.
 *
 * @param nodeid the nodeid of the proposer to check.
 * @return true if this input node is an acceptable proposer.
 */
static bool
clustering_can_accept_as_proposer(cf_node nodeid)
{
	return clustering_is_orphan() || clustering_is_our_principal(nodeid);
}

/**
 * Plugin data iterate function that finds and collects neighboring principals,
 * excluding current principal if any .
 */
static void
clustering_neighboring_principals_find(cf_node nodeid, void* plugin_data,
		size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	cf_vector* neighboring_principals = (cf_vector*)udata;

	CLUSTERING_LOCK();

	// For determining neighboring principal it is alright if this data is
	// within two heartbeat intervals. So obsolete check has the timestamps as
	// zero. This way we will not reject principals that have nothing to do with
	// our cluster changes.
	if (recv_monotonic_ts + 2 * as_hb_tx_interval_get() >= cf_getms()
			&& !clustering_hb_plugin_data_is_obsolete(0, 0, plugin_data,
					plugin_data_size, recv_monotonic_ts, msg_hlc_ts)) {
		cf_node* succession_list = clustering_hb_plugin_succession_get(
				plugin_data, plugin_data_size);

		uint32_t* succession_list_length_p =
				clustering_hb_plugin_succession_length_get(plugin_data,
						plugin_data_size);

		if (succession_list != NULL && succession_list_length_p != NULL
				&& *succession_list_length_p > 0
				&& succession_list[0] != config_self_nodeid_get()) {
			cf_vector_append_unique(neighboring_principals,
					&succession_list[0]);
		}
	}
	else {
		DETAIL(
				"neighboring principal check skipped - found obsolete plugin data for node %"PRIx64,
				nodeid);
	}

	CLUSTERING_UNLOCK();
}

/**
 * Get a list of adjacent principal nodes ordered by descending nodeids.
 */
static void
clustering_neighboring_principals_get(cf_vector* neighboring_principals)
{
	CLUSTERING_LOCK();

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and releases.
	as_hb_plugin_data_iterate_all(AS_HB_PLUGIN_CLUSTERING,
			clustering_neighboring_principals_find, neighboring_principals);

	vector_sort_unique(neighboring_principals, cf_node_compare_desc);

	CLUSTERING_UNLOCK();
}

/**
 * Find dead nodes in current succession list.
 */
static void
clustering_dead_nodes_find(cf_vector* dead_nodes)
{
	CLUSTERING_LOCK();

	cf_vector* succession_list_p = &g_register.succession_list;
	int succession_list_count = cf_vector_size(succession_list_p);
	for (int i = 0; i < succession_list_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		cf_node cluster_member_nodeid = *((cf_node*)cf_vector_getp(
				succession_list_p, i));

		if (!as_hb_is_alive(cluster_member_nodeid)) {
			cf_vector_append(dead_nodes, &cluster_member_nodeid);
		}
	}

	CLUSTERING_UNLOCK();
}

/**
 * Indicates if a node is faulty. A node in the succecssion list deemed faulty
 * - if the node is alive and it reports to be an orphan or is part of some
 * other cluster.
 * - if the node is alive its clustering protocol identifier does not match this
 * node's clustering protocol identifier.
 */
static bool
clustering_node_is_faulty(cf_node nodeid)
{
	if (nodeid == config_self_nodeid_get()) {
		// Self node is never faulty wrt clustering.
		return false;
	}

	CLUSTERING_LOCK();
	bool is_faulty = false;
	as_hlc_msg_timestamp hb_msg_hlc_ts;
	cf_clock msg_recv_ts = 0;
	as_hb_plugin_node_data plugin_data = { 0 };

	if (clustering_hb_plugin_data_get(nodeid, &plugin_data, &hb_msg_hlc_ts,
			&msg_recv_ts) != 0
			|| clustering_hb_plugin_data_is_obsolete(
					g_register.cluster_modified_hlc_ts,
					g_register.cluster_modified_time, plugin_data.data,
					plugin_data.data_size, msg_recv_ts, &hb_msg_hlc_ts)) {
		INFO(
				"faulty check skipped - found obsolete plugin data for node %"PRIx64,
				nodeid);
		is_faulty = false;
		goto Exit;
	}

	// We have clustering data from the node after the current cluster change.
	// Compare protocol identifier, clusterkey, and succession.
	as_cluster_proto_identifier* proto_p = clustering_hb_plugin_proto_get(
			plugin_data.data, plugin_data.data_size);

	if (proto_p == NULL
			|| !clustering_versions_are_compatible(*proto_p,
					clustering_protocol_identifier_get())) {
		DEBUG("for node %"PRIx64" protocol version mismatch - expected: %"PRIx32" but was : %"PRIx32,
				nodeid, clustering_protocol_identifier_get(),
				proto_p != NULL ? *proto_p : 0);
		is_faulty = true;
		goto Exit;
	}

	as_cluster_key* cluster_key_p = clustering_hb_plugin_cluster_key_get(
			plugin_data.data, plugin_data.data_size);
	if (cluster_key_p == NULL || *cluster_key_p != g_register.cluster_key) {
		DEBUG("for node %"PRIx64" cluster key mismatch - expected: %"PRIx64" but was : %"PRIx64,
				nodeid, g_register.cluster_key, cluster_key_p != NULL ? *cluster_key_p : 0);
		is_faulty = true;
		goto Exit;
	}

	// Check succession list just to be sure.
	// We have clustering data from the node after the current cluster change.
	cf_node* succession_list = clustering_hb_plugin_succession_get(
			plugin_data.data, plugin_data.data_size);

	uint32_t* succession_list_length_p =
			clustering_hb_plugin_succession_length_get(plugin_data.data,
					plugin_data.data_size);

	if (succession_list == NULL || succession_list_length_p == NULL
			|| !clustering_hb_succession_list_matches(succession_list,
					*succession_list_length_p, &g_register.succession_list)) {
		INFO("for node %"PRIx64" succession list mismatch", nodeid);

		log_cf_node_vector("self succession list:", &g_register.succession_list,
				CF_INFO);

		if (succession_list) {
			log_cf_node_array("node succession list:", succession_list,
					succession_list && succession_list_length_p ?
							*succession_list_length_p : 0, CF_INFO);
		}
		else {
			INFO("node succession list: (empty)");
		}

		is_faulty = true;
		goto Exit;
	}

Exit:
	CLUSTERING_UNLOCK();
	return is_faulty;
}

/**
 * Find "faulty" nodes in current succession list.
 */
static void
clustering_faulty_nodes_find(cf_vector* faulty_nodes)
{
	CLUSTERING_LOCK();

	if (clustering_is_orphan()) {
		goto Exit;
	}

	cf_vector* succession_list_p = &g_register.succession_list;
	int succession_list_count = cf_vector_size(succession_list_p);
	for (int i = 0; i < succession_list_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		cf_node cluster_member_nodeid = *((cf_node*)cf_vector_getp(
				succession_list_p, i));
		if (clustering_node_is_faulty(cluster_member_nodeid)) {
			cf_vector_append(faulty_nodes, &cluster_member_nodeid);
		}
	}

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Indicates if a node is in sync with this node's cluster. A node in the
 * succecssion list is deemed in sync if the node is alive and it reports to be
 * in the same cluster via its heartbeats.
 */
static bool
clustering_node_is_sync(cf_node nodeid)
{
	if (nodeid == config_self_nodeid_get()) {
		// Self node is always in sync wrt clustering.
		return true;
	}

	CLUSTERING_LOCK();
	bool is_sync = false;
	as_hlc_msg_timestamp hb_msg_hlc_ts;
	cf_clock msg_recv_ts = 0;
	as_hb_plugin_node_data plugin_data = { 0 };
	bool data_exists =
	clustering_hb_plugin_data_get(nodeid, &plugin_data, &hb_msg_hlc_ts,
			&msg_recv_ts) == 0;

	// Latest valid plugin data is ok as long as other checks are met. Hence the
	// timestamps are zero.
	if (!data_exists || msg_recv_ts + 2 * as_hb_tx_interval_get() < cf_getms()
			|| clustering_hb_plugin_data_is_obsolete(0, 0, plugin_data.data,
					plugin_data.data_size, msg_recv_ts, &hb_msg_hlc_ts)) {
		is_sync = false;
		goto Exit;
	}

	// We have clustering data from the node after the current cluster change.
	// Compare protocol identifier, clusterkey, and succession.
	as_cluster_proto_identifier* proto_p = clustering_hb_plugin_proto_get(
			plugin_data.data, plugin_data.data_size);

	if (proto_p == NULL
			|| !clustering_versions_are_compatible(*proto_p,
					clustering_protocol_identifier_get())) {
		DEBUG(
				"for node %"PRIx64" protocol version mismatch - expected: %"PRIx32" but was : %"PRIx32,
				nodeid, clustering_protocol_identifier_get(),
				proto_p != NULL ? *proto_p : 0);
		is_sync = false;
		goto Exit;
	}

	as_cluster_key* cluster_key_p = clustering_hb_plugin_cluster_key_get(
			plugin_data.data, plugin_data.data_size);
	if (cluster_key_p == NULL || *cluster_key_p != g_register.cluster_key) {
		DEBUG(
				"for node %"PRIx64" cluster key mismatch - expected: %"PRIx64" but was : %"PRIx64,
				nodeid, g_register.cluster_key, cluster_key_p != NULL ? *cluster_key_p : 0);
		is_sync = false;
		goto Exit;
	}

	// Check succession list just to be sure.
	// We have clustering data from the node after the current cluster change.
	cf_node* succession_list = clustering_hb_plugin_succession_get(
			plugin_data.data, plugin_data.data_size);

	uint32_t* succession_list_length_p =
			clustering_hb_plugin_succession_length_get(plugin_data.data,
					plugin_data.data_size);

	if (succession_list == NULL || succession_list_length_p == NULL
			|| !clustering_hb_succession_list_matches(succession_list,
					*succession_list_length_p, &g_register.succession_list)) {
		DEBUG("for node %"PRIx64" succession list mismatch", nodeid);

		log_cf_node_vector("self succession list:", &g_register.succession_list,
				CF_DEBUG);

		if (succession_list) {
			log_cf_node_array("node succession list:", succession_list,
					succession_list && succession_list_length_p ?
							*succession_list_length_p : 0, CF_DEBUG);
		}
		else {
			DEBUG("node succession list: (empty)");
		}

		is_sync = false;
		goto Exit;
	}

	is_sync = true;

Exit:
	CLUSTERING_UNLOCK();
	return is_sync;
}

/**
 * Find orphan nodes using clustering data for each node in the heartbeat's
 * adjacency list.
 */
static void
clustering_orphan_nodes_find(cf_node nodeid, void* plugin_data,
		size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	cf_vector* orphans = udata;

	CLUSTERING_LOCK();

	// For determining orphan it is alright if this data is within two heartbeat
	// intervals. So obsolete check has the timestamps as zero.
	if (recv_monotonic_ts + 2 * as_hb_tx_interval_get() >= cf_getms()
			&& !clustering_hb_plugin_data_is_obsolete(0, 0, plugin_data,
					plugin_data_size, recv_monotonic_ts, msg_hlc_ts)) {
		if (clustering_hb_plugin_data_node_status(plugin_data, plugin_data_size)
				== AS_NODE_ORPHAN) {
			cf_vector_append(orphans, &nodeid);
		}

	}
	else {
		DETAIL(
				"orphan check skipped - found obsolete plugin data for node %"PRIx64,
				nodeid);
	}

	CLUSTERING_UNLOCK();
}

/**
 * Get a list of neighboring nodes that are orphans. Does not include self node.
 */
static void
clustering_neighboring_orphans_get(cf_vector* neighboring_orphans)
{
	CLUSTERING_LOCK();

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and release.
	as_hb_plugin_data_iterate_all(AS_HB_PLUGIN_CLUSTERING,
			clustering_orphan_nodes_find, neighboring_orphans);

	CLUSTERING_UNLOCK();
}

/**
 * Find neighboring nodes using clustering data for each node in the heartbeat's
 * adjacency list.
 */
static void
clustering_neighboring_nodes_find(cf_node nodeid, void* plugin_data,
		size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	cf_vector* nodes = udata;
	cf_vector_append(nodes, &nodeid);
}

/**
 * Get a list of all neighboring nodes. Does not include self node.
 */
static void
clustering_neighboring_nodes_get(cf_vector* neighboring_nodes)
{
	CLUSTERING_LOCK();

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and release.
	as_hb_plugin_data_iterate_all(AS_HB_PLUGIN_CLUSTERING,
			clustering_neighboring_nodes_find, neighboring_nodes);

	CLUSTERING_UNLOCK();
}

/**
 * Evict nodes not forming a clique from the succession list.
 */
static uint32_t
clustering_succession_list_clique_evict(cf_vector* succession_list,
		char* evict_msg)
{
	uint32_t num_evicted = 0;
	if (g_config.clustering_config.clique_based_eviction_enabled) {
		// Remove nodes that do not form a clique.
		cf_vector* evicted_nodes = vector_stack_lockless_create(cf_node);
		as_hb_maximal_clique_evict(succession_list, evicted_nodes);
		num_evicted = cf_vector_size(evicted_nodes);
		log_cf_node_vector(evict_msg, evicted_nodes,
				num_evicted > 0 ? CF_INFO : CF_DEBUG);

		vector_subtract(succession_list, evicted_nodes);
		cf_vector_destroy(evicted_nodes);
	}
	return num_evicted;
}

/*
 * ----------------------------------------------------------------------------
 * Clustering network message functions
 * ----------------------------------------------------------------------------
 */

/**
 * Fill common source node specific fields for the message.
 * @param msg the message to fill the source fields into.
 */
static void
msg_src_fields_fill(msg* msg)
{
	// Set the hb protocol id / version.
	msg_set_uint32(msg, AS_CLUSTERING_MSG_ID,
			clustering_protocol_identifier_get());

	// Set the send timestamp
	msg_set_uint64(msg, AS_CLUSTERING_MSG_HLC_TIMESTAMP,
			as_hlc_timestamp_now());
}

/**
 * Read the protocol identifier for this clustering message. These functions can
 * get called multiple times for a single message. Hence they do not increment
 * error counters.
 * @param msg the incoming message.
 * @param id the output id.
 * @return 0 if the type could be parsed -1 on failure.
 */
static int
msg_proto_id_get(msg* msg, uint32_t* id)
{
	if (msg_get_uint32(msg, AS_CLUSTERING_MSG_ID, id) != 0) {
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
msg_type_get(msg* msg, as_clustering_msg_type* type)
{
	if (msg_get_uint32(msg, AS_CLUSTERING_MSG_TYPE, type) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Set the type for an outgoing message.
 * @param msg the outgoing message.
 * @param msg_type the type to set.
 */
static void
msg_type_set(msg* msg, as_clustering_msg_type msg_type)
{
	// Set the message type.
	msg_set_uint32(msg, AS_CLUSTERING_MSG_TYPE, msg_type);
}

/**
 * Read the proposed principal field from the message.
 * @param msg the incoming message.
 * @param nodeid the output nodeid.
 * @return 0 if the type could be parsed -1 on failure.
 */
static int
msg_proposed_principal_get(msg* msg, cf_node* nodeid)
{
	if (msg_get_uint64(msg, AS_CLUSTERING_MSG_PROPOSED_PRINCIPAL, nodeid)
			!= 0) {
		return -1;
	}

	return 0;
}

/**
 * Set the proposed principal field in the message.
 * @param msg the outgoing message.
 * @param nodeid the proposed principal nodeid.
 */
static void
msg_proposed_principal_set(msg* msg, cf_node nodeid)
{
	msg_set_uint64(msg, AS_CLUSTERING_MSG_PROPOSED_PRINCIPAL, nodeid);
}

/**
 * Read the HLC send timestamp for the message. These functions can get called
 * multiple times for a single message. Hence they do not increment error
 * counters.
 * @param msg the incoming message.
 * @param send_ts the output hls timestamp.
 * @return 0 if the type could be parsed -1 on failure.
 */
static int
msg_send_ts_get(msg* msg, as_hlc_timestamp* send_ts)
{
	if (msg_get_uint64(msg, AS_CLUSTERING_MSG_HLC_TIMESTAMP, send_ts) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Set the sequence number for an outgoing message.
 * @param msg the outgoing message.
 * @param sequence_number the sequence number to set.
 */
static void
msg_sequence_number_set(msg* msg, as_paxos_sequence_number sequence_number)
{
	// Set the message type.
	msg_set_uint64(msg, AS_CLUSTERING_MSG_SEQUENCE_NUMBER, sequence_number);
}

/**
 * Read sequence number from the message.
 * @param msg the incoming message.
 * @param sequence_number the output sequence number.
 * @return 0 if the sequence number could be parsed -1 on failure.
 */
static int
msg_sequence_number_get(msg* msg, as_paxos_sequence_number* sequence_number)
{
	if (msg_get_uint64(msg, AS_CLUSTERING_MSG_SEQUENCE_NUMBER, sequence_number)
			!= 0) {
		return -1;
	}

	return 0;
}

/**
 * Set the cluster key for an outgoing message field.
 * @param msg the outgoing message.
 * @param cluster_key the cluster key to set.
 * @param field the field to set the cluster key to.
 */
static void
msg_cluster_key_field_set(msg* msg, as_cluster_key cluster_key,
		as_clustering_msg_field field)
{
	msg_set_uint64(msg, field, cluster_key);
}

/**
 * Set the cluster key for an outgoing message.
 * @param msg the outgoing message.
 * @param cluster_key the cluster key to set.
 */
static void
msg_cluster_key_set(msg* msg, as_cluster_key cluster_key)
{
	msg_cluster_key_field_set(msg, cluster_key, AS_CLUSTERING_MSG_CLUSTER_KEY);
}

/**
 * Read cluster key from a message field.
 * @param msg the incoming message.
 * @param cluster_key the output cluster key.
 * @param field the field to set the cluster key to.
 * @return 0 if the cluster key could be parsed -1 on failure.
 */
static int
msg_cluster_key_field_get(msg* msg, as_cluster_key* cluster_key,
		as_clustering_msg_field field)
{
	if (msg_get_uint64(msg, field, cluster_key) != 0) {
		return -1;
	}

	return 0;
}

/**
 * Read cluster key from the message.
 * @param msg the incoming message.
 * @param cluster_key the output cluster key.
 * @return 0 if the cluster key could be parsed -1 on failure.
 */
static int
msg_cluster_key_get(msg* msg, as_cluster_key* cluster_key)
{
	return msg_cluster_key_field_get(msg, cluster_key,
			AS_CLUSTERING_MSG_CLUSTER_KEY);
}

/**
 * Set the succession list for an outgoing message in a particular field.
 * @param msg the outgoing message.
 * @param succession_list the succession list to set.
 * @param field the field to set for the succession list.
 */
static void
msg_succession_list_field_set(msg* msg, cf_vector* succession_list,
		as_clustering_msg_field field)

{
	int num_elements = cf_vector_size(succession_list);
	size_t buffer_size = num_elements * sizeof(cf_node);
	cf_node* succession_buffer = (cf_node*)BUFFER_ALLOC_OR_DIE(buffer_size);

	for (int i = 0; i < num_elements; i++) {
		cf_vector_get(succession_list, i, &succession_buffer[i]);
	}

	msg_set_buf(msg, field, (uint8_t*)succession_buffer, buffer_size,
			MSG_SET_COPY);

	BUFFER_FREE(succession_buffer, buffer_size);
}

/**
 * Set the succession list for an outgoing message.
 * @param msg the outgoing message.
 * @param succession_list the succession list to set.
 */
static void
msg_succession_list_set(msg* msg, cf_vector* succession_list)
{
	int num_elements = cf_vector_size(succession_list);
	if (num_elements <= 0) {
		// Empty succession list being sent. Definitely wrong.Something is amiss
		// let it through. The receiver will reject it anyways.
		WARNING("setting empty succession list");
		return;
	}

	msg_succession_list_field_set(msg, succession_list,
			AS_CLUSTERING_MSG_SUCCESSION_LIST);
}

/**
 * Read succession list from a message field.
 * @param msg the incoming message.
 * @param succession_list the output succession list.
 * @param field the field to read from.
 * @return 0 if the succession list could be parsed -1 on failure.
 */
static int
msg_succession_list_field_get(msg* msg, cf_vector* succession_list,
		as_clustering_msg_field field)
{
	vector_clear(succession_list);
	cf_node* succession_buffer;
	size_t buffer_size;
	if (msg_get_buf(msg, field, (uint8_t**)&succession_buffer, &buffer_size,
			MSG_GET_DIRECT) != 0) {
		// Empty succession list should not be allowed.
		return -1;
	}

	// Correct adjacency list length.
	int num_elements = buffer_size / sizeof(cf_node);

	for (int i = 0; i < num_elements; i++) {
		cf_vector_append(succession_list, &succession_buffer[i]);
	}

	vector_sort_unique(succession_list, cf_node_compare_desc);

	return 0;
}

/**
 * Read succession list from the message.
 * @param msg the incoming message.
 * @param succession_list the output succession list.
 * @return 0 if the succession list could be parsed -1 on failure.
 */
static int
msg_succession_list_get(msg* msg, cf_vector* succession_list)
{
	return msg_succession_list_field_get(msg, succession_list,
			AS_CLUSTERING_MSG_SUCCESSION_LIST);
}

/**
 * Get the paxos proposal id for message event.
 * @param event the message event.
 * @param proposal_id the paxos proposal id.
 * @return 0 if the type could be parsed -1 on failure.
 */
static int
msg_event_proposal_id_get(as_clustering_internal_event* event,
		as_paxos_proposal_id* proposal_id)
{
	if (msg_sequence_number_get(event->msg, &proposal_id->sequence_number)
			!= 0) {
		return -1;
	}
	proposal_id->src_nodeid = event->msg_src_nodeid;
	return 0;
}

/**
 * Get a network message object from the message pool with all common fields for
 * clustering, like protocol identifier, and hlc timestamp filled in.
 * @param type the type of the message.
 */
static msg*
msg_pool_get(as_clustering_msg_type type)
{
	msg* msg = as_fabric_msg_get(M_TYPE_CLUSTERING);
	msg_src_fields_fill(msg);
	msg_type_set(msg, type);
	return msg;
}

/**
 * Return a message back to the message pool.
 */
static void
msg_pool_return(msg* msg)
{
	as_fabric_msg_put(msg);
}

/**
 * Determines if the received message is old to be ignored.
 *
 * This is detemined by comparing the message hlc timestamp and monotonic
 * timestamps with the cluster formation hlc and monotonic times.
 *
 * @param cluster_modified_hlc_ts the hlc timestamp when for current cluster
 * change happened. Sent to avoid locking in this function.
 * @param cluster_modified_time  the monotonic timestamp when for current
 * cluster change happened. Sento to avoid locking in this function.
 * @param msg_recv_ts the monotonic timestamp for plugin data receive.
 * @param msg_hlc_ts the hlc timestamp for plugin data receive.
 * @return true if plugin data is obsolete, false otherwise.
 */
bool
msg_is_obsolete(as_hlc_timestamp cluster_modified_hlc_ts,
		cf_clock cluster_modified_time, cf_clock msg_recv_ts,
		as_hlc_msg_timestamp* msg_hlc_ts)
{
	if (as_hlc_send_timestamp_order(cluster_modified_hlc_ts, msg_hlc_ts)
			!= AS_HLC_HAPPENS_BEFORE) {
		// Cluster formation time after message send or the order is unknown,
		// assume cluster formation is after message received.
		// The caller should ignore this message.
		return true;
	}

	// MSG should be atleast after cluster formation time + one hb interval to
	// send out our cluster state + one network delay for our information to
	// reach the remote node + one hb for the other node to send out the his
	// updated state +
	// one network delay for the updated  state to reach us.
	if (cluster_modified_time + 2 * as_hb_tx_interval_get()
			+ 2 * g_config.fabric_latency_max_ms > msg_recv_ts) {
		return true;
	}

	return false;
}

/**
 * Send a message to all input nodes. This is best effort some sends could fail.
 * The message will be returned back to the pool.
 * @param msg the message to send.
 * @param nodes the nodes to send the message to.
 * @return 0 on successfu queueing of message (does not imply guaranteed
 * delivery), -1 if the message could not be queued.
 */
static int
msg_node_send(msg* msg, cf_node node)
{
	int rv = as_fabric_send(node, msg, AS_FABRIC_CHANNEL_CTRL);
	if (rv) {
		// Fabric did not clean up the message, return it back to the message
		// pool.
		msg_pool_return(msg);
	}
	return rv;
}

/**
 * Send a message to all input nodes. This is best effort some sends could fail.
 * The message will be returned back to the pool.
 * @param msg the message to send.
 * @param nodes the nodes to send the message to.
 * @return the number of nodes the message was sent to. Does not imply
 * guaranteed receipt by these nodes however.
 */
static int
msg_nodes_send(msg* msg, cf_vector* nodes)
{
	int node_count = cf_vector_size(nodes);
	int sent_count = 0;

	if (node_count <= 0) {
		return sent_count;
	}

	int alloc_size = node_count * sizeof(cf_node);
	cf_node* send_list = (cf_node*)BUFFER_ALLOC_OR_DIE(alloc_size);

	vector_array_cpy(send_list, nodes, node_count);

	if (as_fabric_send_list(send_list, node_count, msg, AS_FABRIC_CHANNEL_CTRL)
			!= 0) {
		// Fabric did not clean up the message, return it back to the message
		// pool.
		msg_pool_return(msg);
	}

	BUFFER_FREE(send_list, alloc_size);
	return sent_count;
}

/*
 * ----------------------------------------------------------------------------
 * Paxos common
 * ----------------------------------------------------------------------------
 */

/**
 * Compare paxos proposal ids. Compares the sequence numbers, ties in sequence
 * number are broken by nodeids.
 *
 * @param id1 the first identifier.
 * @param id2 the second identifier.
 *
 * @return 0 if id1 equals id2, 1 if id1 > id2 and -1 if id1 < id2.
 */
static int
paxos_proposal_id_compare(as_paxos_proposal_id* id1, as_paxos_proposal_id* id2)
{
	if (id1->sequence_number != id2->sequence_number) {
		return id1->sequence_number > id2->sequence_number ? 1 : -1;
	}

	// Sequence numbers match, compare nodeids.
	if (id1->src_nodeid != id2->src_nodeid) {
		return id1->src_nodeid > id2->src_nodeid ? 1 : -1;
	}

	// Node id and sequence numbers match.
	return 0;
}

/*
 * ----------------------------------------------------------------------------
 * Paxos proposer
 * ----------------------------------------------------------------------------
 */

/**
 * Dump paxos proposer state to logs.
 */
static void
paxos_proposer_dump(bool verbose)
{
	CLUSTERING_LOCK();

	// Output paxos proposer state.
	switch (g_proposer.state) {
	case AS_PAXOS_PROPOSER_STATE_IDLE:
		INFO("CL: paxos proposer: idle");
		break;
	case AS_PAXOS_PROPOSER_STATE_PREPARE_SENT:
		INFO("CL: paxos proposer: prepare sent");
		break;
	case AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT:
		INFO("CL: paxos proposer: accept sent");
		break;
	}

	if (verbose) {
		if (g_proposer.state != AS_PAXOS_PROPOSER_STATE_IDLE) {
			INFO("CL: paxos proposal start time: %"PRIu64" now: %"PRIu64,
					g_proposer.paxos_round_start_time, cf_getms());
			INFO("CL: paxos proposed cluster key: %"PRIx64,
					g_proposer.proposed_value.cluster_key);
			INFO("CL: paxos proposed sequence: %"PRIu64,
					g_proposer.sequence_number);
			log_cf_node_vector("CL: paxos proposed succession:",
					&g_proposer.proposed_value.succession_list, CF_INFO);
			log_cf_node_vector("CL: paxos promises received:",
					&g_proposer.promises_received, CF_INFO);
			log_cf_node_vector("CL: paxos accepted received:",
					&g_proposer.accepted_received, CF_INFO);
		}
	}

	CLUSTERING_UNLOCK();
}

/**
 * Reset state on failure of a paxos round.
 */
static void
paxos_proposer_reset()
{
	CLUSTERING_LOCK();

	// Flipping state to idle to indicate paxos round is over.
	g_proposer.state = AS_PAXOS_PROPOSER_STATE_IDLE;
	memset(&g_proposer.sequence_number, 0, sizeof(g_proposer.sequence_number));

	g_proposer.proposed_value.cluster_key = 0;
	vector_clear(&g_proposer.proposed_value.succession_list);

	vector_clear(&g_proposer.acceptors);

	DETAIL("paxos round over for proposal id %"PRIx64":%"PRIu64,
			config_self_nodeid_get(), g_proposer.sequence_number);

	CLUSTERING_UNLOCK();
}

/**
 * Invoked to fail an ongoing paxos proposal.
 */
static void
paxos_proposer_fail()
{
	// Cleanup state for the paxos round.
	paxos_proposer_reset();

	as_clustering_internal_event paxos_fail_event;
	memset(&paxos_fail_event, 0, sizeof(paxos_fail_event));
	paxos_fail_event.type = AS_CLUSTERING_INTERNAL_EVENT_PAXOS_PROPOSER_FAIL;

	internal_event_dispatch(&paxos_fail_event);
}

/**
 * Indicates if a paxos proposal from self node is active.
 */
static bool
paxos_proposer_proposal_is_active()
{
	CLUSTERING_LOCK();
	bool rv = g_proposer.state != AS_PAXOS_PROPOSER_STATE_IDLE;
	CLUSTERING_UNLOCK();
	return rv;
}

/**
 * Send paxos prepare message current list of acceptor nodes.
 */
static void
paxos_proposer_prepare_send()
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE);

	CLUSTERING_LOCK();

	// Set the sequence number
	msg_sequence_number_set(msg, g_proposer.sequence_number);

	log_cf_node_vector("paxos prepare message sent to:", &g_proposer.acceptors,
			CF_DEBUG);

	g_proposer.prepare_send_time = cf_getms();

	cf_vector* acceptors = vector_stack_lockless_create(cf_node);
	vector_copy(acceptors, &g_proposer.acceptors);

	CLUSTERING_UNLOCK();

	// Sent the message to the acceptors.
	msg_nodes_send(msg, acceptors);
	cf_vector_destroy(acceptors);
}

/**
 * Send paxos accept message current list of acceptor nodes.
 */
static void
paxos_proposer_accept_send()
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT);

	CLUSTERING_LOCK();

	// Set the sequence number
	msg_sequence_number_set(msg, g_proposer.sequence_number);

	// Skip send of the proposed value for accept, since we do not use it. Learn
	// message is the only way a consensus value is sent out.
	log_cf_node_vector("paxos accept message sent to:", &g_proposer.acceptors,
			CF_DEBUG);

	g_proposer.accept_send_time = cf_getms();

	cf_vector* acceptors = vector_stack_lockless_create(cf_node);
	vector_copy(acceptors, &g_proposer.acceptors);

	CLUSTERING_UNLOCK();

	// Sent the message to the acceptors.
	msg_nodes_send(msg, acceptors);
	cf_vector_destroy(acceptors);
}

/**
 * Send paxos learn message current list of acceptor nodes.
 */
static void
paxos_proposer_learn_send()
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_LEARN);

	CLUSTERING_LOCK();

	// Set the sequence number
	msg_sequence_number_set(msg, g_proposer.sequence_number);

	// Set the cluster key
	msg_cluster_key_set(msg, g_proposer.proposed_value.cluster_key);

	// Set the succession list
	msg_succession_list_set(msg, &g_proposer.proposed_value.succession_list);

	log_cf_node_vector("paxos learn message sent to:", &g_proposer.acceptors,
			CF_DEBUG);

	g_proposer.learn_send_time = cf_getms();

	cf_vector* acceptors = vector_stack_lockless_create(cf_node);
	vector_copy(acceptors, &g_proposer.acceptors);

	CLUSTERING_UNLOCK();

	// Sent the message to the acceptors.
	msg_nodes_send(msg, acceptors);
	cf_vector_destroy(acceptors);
}

/**
 * Handle an incoming paxos promise message.
 */
static void
paxos_proposer_promise_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	msg* msg = event->msg;

	DEBUG("received paxos promise from node %"PRIx64, src_nodeid);

	CLUSTERING_LOCK();
	if (g_proposer.state != AS_PAXOS_PROPOSER_STATE_PREPARE_SENT) {
		// We are not in the prepare phase. Reject this message.
		DEBUG("ignoring paxos promise from node %"PRIx64" - we are not in prepare phase",
				src_nodeid);
		goto Exit;
	}

	if (vector_find(&g_proposer.acceptors, &src_nodeid) < 0) {
		WARNING("ignoring paxos promise from node %"PRIx64" - it is not in acceptor list",
				src_nodeid);
		goto Exit;
	}

	as_paxos_sequence_number sequence_number = 0;
	if (msg_sequence_number_get(msg, &sequence_number) != 0) {
		WARNING("ignoring paxos promise from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		goto Exit;
	}

	if (sequence_number != g_proposer.sequence_number) {
		// Not a matching promise message. Ignore.
		INFO("ignoring paxos promise from node %"PRIx64" because its proposal id %"PRIu64" does not match expected id %"PRIu64,
				src_nodeid, sequence_number,
				g_proposer.sequence_number);
		goto Exit;
	}

	cf_vector_append_unique(&g_proposer.promises_received, &src_nodeid);

	int promised_count = cf_vector_size(&g_proposer.promises_received);
	int acceptor_count = cf_vector_size(&g_proposer.acceptors);

	// Use majority quorum to move on.
	if (promised_count >= 1 + (acceptor_count / 2)) {
		// We have quorum number of promises. go ahead to the accept phase.
		g_proposer.state = AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT;
		paxos_proposer_accept_send();
	}

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming paxos prepare nack message.
 */
static void
paxos_proposer_prepare_nack_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	msg* msg = event->msg;

	DEBUG("received paxos prepare nack from node %"PRIx64, src_nodeid);

	CLUSTERING_LOCK();
	if (g_proposer.state != AS_PAXOS_PROPOSER_STATE_PREPARE_SENT) {
		// We are not in the prepare phase. Reject this message.
		INFO("ignoring paxos prepare nack from node %"PRIx64" - we are not in prepare phase",
				src_nodeid);
		goto Exit;
	}

	if (vector_find(&g_proposer.acceptors, &src_nodeid) < 0) {
		WARNING("ignoring paxos prepare nack from node %"PRIx64" - it is not in acceptor list",
				src_nodeid);
		goto Exit;
	}

	as_paxos_sequence_number sequence_number = 0;
	if (msg_sequence_number_get(msg, &sequence_number) != 0) {
		WARNING("ignoring paxos prepare nack from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		goto Exit;
	}

	if (sequence_number != g_proposer.sequence_number) {
		// Not a matching prepare nack message. Ignore.
		INFO("ignoring paxos prepare nack from node %"PRIx64" because its proposal id %"PRIu64" does not match expected id %"PRIu64,
				src_nodeid, sequence_number,
				g_proposer.sequence_number);
		goto Exit;
	}

	INFO(
			"aborting current paxos proposal because of a prepare nack from node %"PRIx64,
			src_nodeid);
	paxos_proposer_fail();

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Invoked when all acceptors have accepted the proposal.
 */
static void
paxos_proposer_success()
{
	CLUSTERING_LOCK();

	// Set the proposer to back idle state.
	g_proposer.state = AS_PAXOS_PROPOSER_STATE_IDLE;

	// Send out learn message and enable retransmits of learn message.
	g_proposer.learn_retransmit_needed = true;
	paxos_proposer_learn_send();

	// Retain the sequence_number, cluster key and succession list for
	// retransmits of the learn message.
	as_clustering_internal_event paxos_success_event;
	memset(&paxos_success_event, 0, sizeof(paxos_success_event));
	paxos_success_event.type =
			AS_CLUSTERING_INTERNAL_EVENT_PAXOS_PROPOSER_SUCCESS;

	CLUSTERING_UNLOCK();
}

/**
 * Indicates if the proposer can accept, accepted messages.
 */
static bool
paxos_proposer_can_accept_accepted(cf_node src_nodeid, msg* msg)
{
	bool rv = false;

	CLUSTERING_LOCK();
	// We also allow accepted messages in the idle state to deal with a loss of
	// the learn message.
	if (g_proposer.state != AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT
			&& g_proposer.state != AS_PAXOS_PROPOSER_STATE_IDLE) {
		// We are not in the accept phase. Reject this message.
		DEBUG("ignoring paxos accepted from node %"PRIx64" - we are not in accept phase. Actual phase %d",
				src_nodeid, g_proposer.state);
		goto Exit;
	}

	if (vector_find(&g_proposer.acceptors, &src_nodeid) < 0) {
		WARNING("ignoring paxos accepted from node %"PRIx64" - it is not in acceptor list",
				src_nodeid);
		goto Exit;
	}

	as_paxos_sequence_number sequence_number = 0;
	if (msg_sequence_number_get(msg, &sequence_number) != 0) {
		WARNING("ignoring paxos accepted from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		goto Exit;
	}

	if (sequence_number != g_proposer.sequence_number) {
		// Not a matching accepted message. Ignore.
		INFO("ignoring paxos accepted from node %"PRIx64" because its proposal id %"PRIu64" does not match expected id %"PRIu64,
				src_nodeid, sequence_number,
				g_proposer.sequence_number);
		goto Exit;
	}

	if (g_proposer.proposed_value.cluster_key == g_register.cluster_key
			&& vector_equals(&g_proposer.proposed_value.succession_list,
					&g_register.succession_list)) {
		// The register is already synced for this proposal. We can ignore this
		// accepted message.
		INFO("ignoring paxos accepted from node %"PRIx64" because its proposal id %"PRIu64" is a duplicate",
				src_nodeid, sequence_number
		);
		goto Exit;
	}

	rv = true;
Exit:
	CLUSTERING_UNLOCK();
	return rv;
}

/**
 * Handle an incoming paxos accepted message.
 */
static void
paxos_proposer_accepted_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	msg* msg = event->msg;

	DEBUG("received paxos accepted from node %"PRIx64, src_nodeid);

	if (!paxos_proposer_can_accept_accepted(src_nodeid, msg)) {
		return;
	}

	CLUSTERING_LOCK();

	cf_vector_append_unique(&g_proposer.accepted_received, &src_nodeid);

	int accepted_count = cf_vector_size(&g_proposer.accepted_received);
	int acceptor_count = cf_vector_size(&g_proposer.acceptors);

	// Use a simple quorum, all acceptors should accept for success.
	if (accepted_count == acceptor_count) {
		// This is the point after which the succession list will not change for
		// this paxos round. Ensure that we meet the minimum cluster size
		// criterion.
		int cluster_size = cf_vector_size(
				&g_proposer.proposed_value.succession_list);
		if (cluster_size < g_config.clustering_config.cluster_size_min) {
			WARNING(
					"failing paxos round - the remaining number of nodes %d is less than minimum cluster size %d",
					cluster_size, g_config.clustering_config.cluster_size_min);
			// Fail paxos.
			paxos_proposer_fail();
			goto Exit;
		}

		// We have quorum number of accepted nodes. The proposal succeeded.
		paxos_proposer_success();
	}

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming paxos accept nack message.
 */
static void
paxos_proposer_accept_nack_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	msg* msg = event->msg;

	DEBUG("received paxos accept nack from node %"PRIx64, src_nodeid);

	CLUSTERING_LOCK();
	if (g_proposer.state != AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT) {
		// We are not in the accept phase. Reject this message.
		INFO("ignoring paxos accept nack from node %"PRIx64" - we are not in accept phase",
				src_nodeid);
		goto Exit;
	}

	if (vector_find(&g_proposer.acceptors, &src_nodeid) < 0) {
		WARNING("ignoring paxos accept nack from node %"PRIx64" - it is not in acceptor list",
				src_nodeid);
		goto Exit;
	}

	as_paxos_sequence_number sequence_number = 0;
	if (msg_sequence_number_get(msg, &sequence_number) != 0) {
		WARNING("ignoring paxos accept nack from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		goto Exit;
	}

	if (sequence_number != g_proposer.sequence_number) {
		// Not a matching accept nack message. Ignore.
		INFO("ignoring paxos accept nack from node %"PRIx64"because its proposal id %"PRIu64" does not match expected id %"PRIu64,
				src_nodeid, sequence_number,
				g_proposer.sequence_number);
		goto Exit;
	}

	INFO(
			"aborting current paxos proposal because of an accept nack from node %"PRIx64,
			src_nodeid);
	paxos_proposer_fail();

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming message.
 */
static void
paxos_proposer_msg_event_handle(as_clustering_internal_event* msg_event)
{
	switch (msg_event->msg_type) {
	case AS_CLUSTERING_MSG_TYPE_PAXOS_PROMISE:
		paxos_proposer_promise_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE_NACK:
		paxos_proposer_prepare_nack_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPTED:
		paxos_proposer_accepted_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT_NACK:
		paxos_proposer_accept_nack_handle(msg_event);
		break;
	default:	// Other message types are not of interest.
		break;
	}
}

/**
 * Handle heartbeat event.
 */
static void
paxos_proposer_hb_event_handle(as_clustering_internal_event* hb_event)
{
	if (!paxos_proposer_proposal_is_active()) {
		return;
	}

	CLUSTERING_LOCK();
	for (int i = 0; i < hb_event->hb_n_events; i++) {
		if (hb_event->hb_events[i].evt == AS_HB_NODE_DEPART) {
			cf_node departed_node = hb_event->hb_events[i].nodeid;
			if (vector_find(&g_proposer.acceptors, &departed_node)) {
				// One of the acceptors has departed. Abort the paxos proposal.
				INFO("paxos acceptor %"PRIx64" departed - aborting current paxos proposal", departed_node);
				paxos_proposer_fail();
				break;
			}
		}
	}
	CLUSTERING_UNLOCK();
}

/**
 * Check and retransmit prepare message if paxos promise messages have not yet
 * being received.
 */
static void
paxos_proposer_prepare_check_retransmit()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	if (g_proposer.state == AS_PAXOS_PROPOSER_STATE_PREPARE_SENT
			&& g_proposer.prepare_send_time + paxos_msg_timeout() < now) {
		paxos_proposer_prepare_send();
	}
	CLUSTERING_UNLOCK();
}

/**
 * Check and retransmit accept message if paxos accepted has yet being received.
 */
static void
paxos_proposer_accept_check_retransmit()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	if (g_proposer.state == AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT
			&& g_proposer.accept_send_time + paxos_msg_timeout() < now) {
		paxos_proposer_accept_send();
	}
	CLUSTERING_UNLOCK();
}

/**
 * Check and retransmit learn message if all acceptors have not applied the
 * current cluster change.
 */
static void
paxos_proposer_learn_check_retransmit()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	bool learn_timedout = g_proposer.learn_retransmit_needed
			&& (g_proposer.state == AS_PAXOS_PROPOSER_STATE_IDLE)
			&& (g_proposer.proposed_value.cluster_key != 0)
			&& (g_proposer.learn_send_time + paxos_msg_timeout() < now);

	if (learn_timedout) {
		// If the register is not synced, most likely the learn message did not
		// make it through, retransmit the learn message to move the paxos
		// acceptor forward and start register sync.
		INFO("retransmitting paxos learn message");
		paxos_proposer_learn_send();
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle a timer event and retransmit messages if required.
 */
static void
paxos_proposer_timer_event_handle()
{
	CLUSTERING_LOCK();
	switch (g_proposer.state) {
	case AS_PAXOS_PROPOSER_STATE_IDLE:
		paxos_proposer_learn_check_retransmit();
		break;
	case AS_PAXOS_PROPOSER_STATE_PREPARE_SENT:
		paxos_proposer_prepare_check_retransmit();
		break;
	case AS_PAXOS_PROPOSER_STATE_ACCEPT_SENT:
		paxos_proposer_accept_check_retransmit();
		break;
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle register getting synched.
 */
static void
paxos_proposer_register_synched()
{
	CLUSTERING_LOCK();
	// Register synched we no longer need learn messages to be retransmitted.
	g_proposer.learn_retransmit_needed = false;
	CLUSTERING_UNLOCK();
}

/**
 * Initialize paxos proposer state.
 */
static void
paxos_proposer_init()
{
	CLUSTERING_LOCK();
	// Memset to zero which ensures that all proposer state variables have zero
	// which is the correct initial value for elements other that contained
	// vectors and status.
	memset(&g_proposer, 0, sizeof(g_proposer));

	// Initialize the proposer state.
	// No paxos round running, so the state has to be idle.
	g_proposer.state = AS_PAXOS_PROPOSER_STATE_IDLE;

	// Set the current acceptor list to be empty.
	vector_lockless_init(&g_proposer.acceptors, cf_node);

	// Set the current promises received node list to empty.
	vector_lockless_init(&g_proposer.promises_received, cf_node);

	// Set the current accepted received node list to empty.
	vector_lockless_init(&g_proposer.accepted_received, cf_node);

	// Initialize the proposed value.
	vector_lockless_init(&g_proposer.proposed_value.succession_list, cf_node);
	g_proposer.proposed_value.cluster_key = 0;

	CLUSTERING_UNLOCK();
}

/**
 * Log paxos results.
 */
static void
paxos_result_log(as_paxos_start_result result, cf_vector* new_succession_list)
{
	CLUSTERING_LOCK();
	switch (result) {
	case AS_PAXOS_RESULT_STARTED: {
		// Running check required because paxos round finished for single node
		// cluster by this time.
		if (paxos_proposer_proposal_is_active()) {
			INFO("paxos round started - cluster key: %"PRIx64,
					g_proposer.proposed_value.cluster_key);
			log_cf_node_vector("paxos round started - succession list:",
					&g_proposer.proposed_value.succession_list, CF_INFO);
		}
		break;
	}

	case AS_PAXOS_RESULT_CLUSTER_TOO_SMALL: {
		WARNING(
				"paxos round aborted - new cluster size %d less than min cluster size %d",
				cf_vector_size(new_succession_list),
				g_config.clustering_config.cluster_size_min);
		break;
	}

	case AS_PAXOS_RESULT_ROUND_RUNNING: {
		// Should never happen in practice. Let the old round finish or timeout.
		WARNING(
				"older paxos round still running - should have finished by now");
	}
	}

	CLUSTERING_UNLOCK();
}

/**
 * Start a new paxos round.
 *
 * @param new_succession_list the new succession list.
 * @param acceptor_list the list of nodes to use for paxos acceptors.
 * @param current_cluster_key the current cluster key
 * @param current_succession_list the current succession list, can be null if
 * this node is an orphan.
 */
static as_paxos_start_result
paxos_proposer_proposal_start(cf_vector* new_succession_list,
		cf_vector* acceptor_list)
{
	if (cf_vector_size(new_succession_list)
			< g_config.clustering_config.cluster_size_min) {
		// Fail paxos.
		return AS_PAXOS_RESULT_CLUSTER_TOO_SMALL;
	}

	CLUSTERING_LOCK();

	as_paxos_start_result result;
	if (paxos_proposer_proposal_is_active()) {
		result = AS_PAXOS_RESULT_ROUND_RUNNING;
		goto Exit;
	}

	// Update state to prepare.
	g_proposer.state = AS_PAXOS_PROPOSER_STATE_PREPARE_SENT;

	g_proposer.sequence_number = as_hlc_timestamp_now();

	g_proposer.paxos_round_start_time = cf_getms();

	// Populate the proposed value struct with new succession list and a new
	// cluster key.
	vector_clear(&g_proposer.proposed_value.succession_list);
	vector_copy(&g_proposer.proposed_value.succession_list,
			new_succession_list);
	g_proposer.proposed_value.cluster_key = clustering_cluster_key_generate(
			g_register.cluster_key);

	// Remember the acceptors for this paxos round.
	vector_clear(&g_proposer.acceptors);
	vector_copy(&g_proposer.acceptors, acceptor_list);

	// Clear the promise received and accepted received vectors for this new
	// round.
	vector_clear(&g_proposer.promises_received);
	vector_clear(&g_proposer.accepted_received);

	paxos_proposer_prepare_send();

	result = AS_PAXOS_RESULT_STARTED;

Exit:
	CLUSTERING_UNLOCK();

	return result;
}

/**
 * Paxos proposer monitor to detect and cleanup long running and most likely
 * failed paxos rounds.
 */
static void
paxos_proposer_monitor()
{
	CLUSTERING_LOCK();
	if (paxos_proposer_proposal_is_active()) {
		if (g_proposer.paxos_round_start_time + paxos_proposal_timeout()
				<= cf_getms()) {
			// Paxos round is running and has timed out.
			// Consider paxos round failed.
			INFO("paxos round timed out for proposal id %"PRIx64":%"PRIu64,
					config_self_nodeid_get(),
					g_proposer.sequence_number);
			paxos_proposer_fail();
		}
	}
	CLUSTERING_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Paxos acceptor
 * ----------------------------------------------------------------------------
 */

/**
 * Dump paxos acceptor state to logs.
 */
static void
paxos_acceptor_dump(bool verbose)
{
	CLUSTERING_LOCK();

	// Output paxos acceptor state.
	switch (g_acceptor.state) {
	case AS_PAXOS_ACCEPTOR_STATE_IDLE:
		INFO("CL: paxos acceptor: idle");
		break;
	case AS_PAXOS_ACCEPTOR_STATE_PROMISED:
		INFO("CL: paxos acceptor: promised");
		break;
	case AS_PAXOS_ACCEPTOR_STATE_ACCEPTED:
		INFO("CL: paxos acceptor: accepted");
		break;
	}

	if (verbose) {
		if (g_acceptor.state != AS_PAXOS_ACCEPTOR_STATE_IDLE) {
			INFO("CL: paxos acceptor start time: %"PRIu64" now: %"PRIu64,
					g_acceptor.acceptor_round_start, cf_getms());
			INFO("CL: paxos acceptor proposal id: (%"PRIx64":%"PRIu64")",
					g_acceptor.last_proposal_received_id.src_nodeid,
					g_acceptor.last_proposal_received_id.sequence_number);
			INFO("CL: paxos acceptor promised time: %"PRIu64" now: %"PRIu64,
					g_acceptor.promise_send_time, cf_getms());
			INFO("CL: paxos acceptor accepted time: %"PRIu64" now: %"PRIu64,
					g_acceptor.accepted_send_time, cf_getms());
		}
	}

	CLUSTERING_UNLOCK();
}

/**
 * Reset the acceptor for the next round.
 */
static void
paxos_acceptor_reset()
{
	CLUSTERING_LOCK();
	g_acceptor.state = AS_PAXOS_ACCEPTOR_STATE_IDLE;
	g_acceptor.acceptor_round_start = 0;
	g_acceptor.promise_send_time = 0;
	g_acceptor.accepted_send_time = 0;
	CLUSTERING_UNLOCK();
}

/**
 * Invoked to fail an ongoing paxos proposal.
 */
static void
paxos_acceptor_fail()
{
	// Cleanup state for the paxos round.
	paxos_acceptor_reset();

	as_clustering_internal_event paxos_fail_event;
	memset(&paxos_fail_event, 0, sizeof(paxos_fail_event));
	paxos_fail_event.type = AS_CLUSTERING_INTERNAL_EVENT_PAXOS_ACCEPTOR_FAIL;

	internal_event_dispatch(&paxos_fail_event);
}

/**
 * Invoked on success of an ongoing paxos proposal.
 */
static void
paxos_acceptor_success(as_cluster_key cluster_key, cf_vector* succession_list,
		as_paxos_sequence_number sequence_number)
{
	// Cleanup state for the paxos round.
	paxos_acceptor_reset();

	as_clustering_internal_event paxos_success_event;
	memset(&paxos_success_event, 0, sizeof(paxos_success_event));
	paxos_success_event.type =
			AS_CLUSTERING_INTERNAL_EVENT_PAXOS_ACCEPTOR_SUCCESS;
	paxos_success_event.new_succession_list = succession_list;
	paxos_success_event.new_cluster_key = cluster_key;
	paxos_success_event.new_sequence_number = sequence_number;

	internal_event_dispatch(&paxos_success_event);
}

/**
 * Send paxos promise message to the proposer node.
 * @param dest  the destination node.
 * @param sequence_number the sequence number from the incoming message.
 */
static void
paxos_acceptor_promise_send(cf_node dest,
		as_paxos_sequence_number sequence_number)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_PROMISE);

	msg_sequence_number_set(msg, sequence_number);

	DEBUG("paxos promise message sent to node %"PRIx64" with proposal id (%"PRIx64":%"PRIu64")", dest, dest, sequence_number);

	CLUSTERING_LOCK();
	g_acceptor.promise_send_time = cf_getms();
	CLUSTERING_UNLOCK();

	// Send the message to the proposer.
	msg_node_send(msg, dest);
}

/**
 * Send paxos prepare nack message to the proposer.
 * @param dest  the destination node.
 * @param sequence_number the sequence number from the incoming message.
 */
static void
paxos_acceptor_prepare_nack_send(cf_node dest,
		as_paxos_sequence_number sequence_number)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE_NACK);

	msg_sequence_number_set(msg, sequence_number);

	DEBUG("paxos prepare nack message sent to node %"PRIx64" with proposal id (%"PRIx64":%"PRIu64")", dest, dest, sequence_number);

	// Send the message to the proposer.
	msg_node_send(msg, dest);
}

/**
 * Send paxos accepted message to the proposer node.
 * @param dest  the destination node.
 * @param sequence_number the sequence number from the incoming message.
 */
static void
paxos_acceptor_accepted_send(cf_node dest,
		as_paxos_sequence_number sequence_number)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPTED);

	msg_sequence_number_set(msg, sequence_number);

	DEBUG("paxos accepted message sent to node %"PRIx64" with proposal id (%"PRIx64":%"PRIu64")", dest, dest, sequence_number);

	CLUSTERING_LOCK();
	g_acceptor.accepted_send_time = cf_getms();
	CLUSTERING_UNLOCK();

	// Send the message to the proposer.
	msg_node_send(msg, dest);
}

/**
 * Send paxos accept nack message to the proposer.
 * @param dest  the destination node.
 * @param sequence_number the sequence number from the incoming message.
 */
static void
paxos_acceptor_accept_nack_send(cf_node dest,
		as_paxos_sequence_number sequence_number)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT_NACK);

	msg_sequence_number_set(msg, sequence_number);

	DEBUG("paxos accept nack message sent to node %"PRIx64" with proposal id (%"PRIx64":%"PRIu64")", dest, dest, sequence_number);

	// Send the message to the proposer.
	msg_node_send(msg, dest);
}

/**
 * Check if the incoming prepare can be promised.
 */
static bool
paxos_acceptor_prepare_can_promise(cf_node src_nodeid,
		as_paxos_proposal_id* proposal_id)
{
	if (!clustering_can_accept_as_proposer(src_nodeid)) {
		INFO("ignoring paxos prepare from node %"PRIx64" because it cannot be a principal",
				src_nodeid);
		return false;
	}

	bool can_promise = false;
	CLUSTERING_LOCK();
	int comparison = paxos_proposal_id_compare(proposal_id,
			&g_acceptor.last_proposal_received_id);

	switch (g_acceptor.state) {
	case AS_PAXOS_ACCEPTOR_STATE_IDLE:
	case AS_PAXOS_ACCEPTOR_STATE_ACCEPTED: {
		// Allow only higher valued proposal to prevent replays and also to
		// ensure convergence in the face of competing proposals.
		can_promise = comparison > 0;
	}
		break;
	case AS_PAXOS_ACCEPTOR_STATE_PROMISED: {
		// We allow for replays of the prepare message as well so that the
		// proposer can receive a promise for this node's lost promise message.
		can_promise = comparison >= 0;
	}
		break;
	}

	CLUSTERING_UNLOCK();

	return can_promise;
}

/**
 * Handle an incoming paxos prepare message.
 */
static void
paxos_acceptor_prepare_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	DEBUG("received paxos prepare from node %"PRIx64, src_nodeid);

	as_paxos_proposal_id proposal_id = { 0 };
	if (msg_event_proposal_id_get(event, &proposal_id) != 0) {
		INFO("ignoring paxos prepare from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		return;
	}

	if (!paxos_acceptor_prepare_can_promise(src_nodeid, &proposal_id)) {
		INFO("ignoring paxos prepare from node %"PRIx64" with obsolete proposal id (%"PRIx64":%"PRIu64")", proposal_id.src_nodeid, proposal_id.src_nodeid, proposal_id.sequence_number);
		paxos_acceptor_prepare_nack_send(src_nodeid,
				proposal_id.sequence_number);
		return;
	}

	CLUSTERING_LOCK();

	bool is_new_proposal = paxos_proposal_id_compare(&proposal_id,
			&g_acceptor.last_proposal_received_id) != 0;

	if (is_new_proposal) {
		// Remember this to be the last proposal id we received.
		memcpy(&g_acceptor.last_proposal_received_id, &proposal_id,
				sizeof(proposal_id));

		// Update the round start time.
		g_acceptor.acceptor_round_start = cf_getms();

		// Switch to promised state.
		g_acceptor.state = AS_PAXOS_ACCEPTOR_STATE_PROMISED;
	}
	else {
		// This is a retransmit or delayed message in which case we do not
		// update the state.
		// If we have already accepted this proposal, we would want to remain in
		// accepted state.
	}

	// The proposal is promised. Send back a paxos promise.
	paxos_acceptor_promise_send(src_nodeid, proposal_id.sequence_number);

	CLUSTERING_UNLOCK();
}

/**
 * Check if the incoming accept can be accepted.
 */
static bool
paxos_acceptor_accept_can_accept(cf_node src_nodeid,
		as_paxos_proposal_id* proposal_id)
{
	if (!clustering_can_accept_as_proposer(src_nodeid)) {
		INFO("ignoring paxos accept from node %"PRIx64" because it cannot be a principal",
				src_nodeid);
		return false;
	}

	bool can_accept = false;
	CLUSTERING_LOCK();
	int comparison = paxos_proposal_id_compare(proposal_id,
			&g_acceptor.last_proposal_received_id);

	switch (g_acceptor.state) {
	case AS_PAXOS_ACCEPTOR_STATE_IDLE:
	case AS_PAXOS_ACCEPTOR_STATE_PROMISED:
	case AS_PAXOS_ACCEPTOR_STATE_ACCEPTED: {
		// We allow for replays of the accept message as well, so that the
		// proposer can receive an accepted for this node's lost accepted
		// message.
		can_accept = comparison >= 0;
	}
		break;
	}

	CLUSTERING_UNLOCK();

	return can_accept;
}

/**
 * Handle an incoming paxos accept message.
 */
static void
paxos_acceptor_accept_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;

	DEBUG("received paxos accept from node %"PRIx64, src_nodeid);

	// Its ok to proceed even is paxos is running, because this could be a
	// competing proposal and the winner will be decided by paxos sequence
	// number.
	as_paxos_proposal_id proposal_id = { 0 };
	if (msg_event_proposal_id_get(event, &proposal_id) != 0) {
		INFO("ignoring paxos accept from node %"PRIx64" with invalid proposal id",
				src_nodeid);
		return;
	}

	if (!paxos_acceptor_accept_can_accept(src_nodeid, &proposal_id)) {
		INFO("ignoring paxos accept from node %"PRIx64" with obsolete proposal id (%"PRIx64":%"PRIu64")", proposal_id.src_nodeid, proposal_id.src_nodeid, proposal_id.sequence_number);
		paxos_acceptor_accept_nack_send(src_nodeid,
				proposal_id.sequence_number);
		return;
	}

	CLUSTERING_LOCK();

	bool is_new_proposal = paxos_proposal_id_compare(&proposal_id,
			&g_acceptor.last_proposal_received_id) != 0;

	if (is_new_proposal) {
		// This node has missed the prepare message, but received the accept
		// message. This is alright.

		// Remember this to be the last proposal id we received.
		memcpy(&g_acceptor.last_proposal_received_id, &proposal_id,
				sizeof(proposal_id));

		// Mark this as the start of the acceptor paxos round.
		g_acceptor.acceptor_round_start = cf_getms();
	}

	g_acceptor.state = AS_PAXOS_ACCEPTOR_STATE_ACCEPTED;
	// The proposal is accepted. Send back a paxos accept.
	paxos_acceptor_accepted_send(src_nodeid, proposal_id.sequence_number);

	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming paxos learn message.
 */
static void
paxos_acceptor_learn_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;
	msg* msg = event->msg;

	DEBUG("received paxos learn from node %"PRIx64, src_nodeid);

	if (!clustering_can_accept_as_proposer(src_nodeid)) {
		INFO("ignoring learn message from a non-principal node %"PRIx64" because we are already in a cluster",
				src_nodeid);
		return;
	}

	// Its ok to proceed even if paxos is running, because this could be a
	// competing proposal and the winner was decided by paxos sequence number.
	as_paxos_proposal_id proposal_id = { 0 };
	if (msg_event_proposal_id_get(event, &proposal_id) != 0) {
		INFO("ignoring paxos learn from node %"PRIx64"with invalid proposal id",
				src_nodeid);
		return;
	}

	CLUSTERING_LOCK();

	if (g_acceptor.state != AS_PAXOS_ACCEPTOR_STATE_ACCEPTED) {
		INFO(
				"ignoring paxos learn from node %"PRIx64" - proposal id (%"PRIx64":%"PRIu64") we are already in a cluster",
				src_nodeid, proposal_id.src_nodeid,
				proposal_id.sequence_number);
		goto Exit;
	}

	if (paxos_proposal_id_compare(&proposal_id,
			&g_acceptor.last_proposal_received_id) != 0) {
		// We have not promised nor accepted this proposal,
		// ignore the learn message.
		INFO(
				"ignoring paxos learn from node %"PRIx64" - proposal id (%"PRIx64":%"PRIu64") mismatches current proposal id (%"PRIx64":%"PRIu64")",
				src_nodeid, proposal_id.src_nodeid,
				proposal_id.sequence_number,
				g_acceptor.last_proposal_received_id.src_nodeid,
				g_acceptor.last_proposal_received_id.sequence_number);
		goto Exit;
	}

	as_cluster_key new_cluster_key = 0;
	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);

	if (msg_cluster_key_get(msg, &new_cluster_key) != 0) {
		INFO("ignoring paxos learn from node %"PRIx64" without cluster key",
				src_nodeid);
		goto Exit_destory_succession;
	}

	if (msg_succession_list_get(msg, new_succession_list) != 0) {
		INFO("ignoring paxos learn from node %"PRIx64" without succession list",
				src_nodeid);
		goto Exit_destory_succession;
	}

	if (new_cluster_key == g_register.cluster_key) {
		if (!vector_equals(new_succession_list, &g_register.succession_list)) {
			// We have the same cluster key repeated for a new round. Should
			// never happen.
			CRASH("duplicate cluster key %"PRIx64" generated for different paxos rounds - disastrous", new_cluster_key);
		}

		INFO("ignoring duplicate paxos learn from node %"PRIx64, src_nodeid);
		goto Exit_destory_succession;
	}

	// Paxos round converged, apply the new cluster configuration.
	paxos_acceptor_success(new_cluster_key, new_succession_list,
			proposal_id.sequence_number);

Exit_destory_succession:
	cf_vector_destroy(new_succession_list);

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming message.
 */
static void
paxos_acceptor_msg_event_handle(as_clustering_internal_event *msg_event)
{
	switch (msg_event->msg_type) {
	case AS_CLUSTERING_MSG_TYPE_PAXOS_PREPARE:
		paxos_acceptor_prepare_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_PAXOS_ACCEPT:
		paxos_acceptor_accept_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_PAXOS_LEARN:
		paxos_acceptor_learn_handle(msg_event);
		break;
	default:	// Other message types are not of interest.
		break;
	}
}

/**
 * Check and retransmit promise message if paxos proposer has not moved ahead
 * and send back an accept message.
 */
static void
paxos_acceptor_promise_check_retransmit()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	if (g_acceptor.state == AS_PAXOS_ACCEPTOR_STATE_PROMISED
			&& g_acceptor.promise_send_time + paxos_msg_timeout() < now) {
		paxos_acceptor_promise_send(
				g_acceptor.last_proposal_received_id.src_nodeid,
				g_acceptor.last_proposal_received_id.sequence_number);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Check and retransmit accepted message if paxos proposer has not send back a
 * learn message.
 */
static void
paxos_acceptor_accepted_check_retransmit()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	if (g_acceptor.state == AS_PAXOS_ACCEPTOR_STATE_ACCEPTED
			&& g_acceptor.accepted_send_time + paxos_msg_timeout() < now) {
		paxos_acceptor_accepted_send(
				g_acceptor.last_proposal_received_id.src_nodeid,
				g_acceptor.last_proposal_received_id.sequence_number);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle a timer event and retransmit messages if required.
 */
static void
paxos_acceptor_timer_event_handle()
{
	CLUSTERING_LOCK();
	switch (g_acceptor.state) {
	case AS_PAXOS_ACCEPTOR_STATE_IDLE: {
		// No retransmitts required.
		break;
	}
	case AS_PAXOS_ACCEPTOR_STATE_PROMISED:
		paxos_acceptor_promise_check_retransmit();
		break;
	case AS_PAXOS_ACCEPTOR_STATE_ACCEPTED:
		paxos_acceptor_accepted_check_retransmit();
		break;
	}

	CLUSTERING_UNLOCK();
}

/**
 * Initialize paxos acceptor state.
 */
static void
paxos_acceptor_init()
{
	CLUSTERING_LOCK();
	// Memset to zero which ensures that all acceptor state variables have zero
	// which is the correct initial value for elements other that contained
	// vectors and status.
	memset(&g_acceptor, 0, sizeof(g_acceptor));
	g_acceptor.state = AS_PAXOS_ACCEPTOR_STATE_IDLE;
	CLUSTERING_UNLOCK();
}

/**
 * Paxos acceptor monitor to detect and cleanup long running and most likely
 * failed paxos rounds.
 */
static void
paxos_acceptor_monitor()
{
	CLUSTERING_LOCK();
	if (g_acceptor.state != AS_PAXOS_ACCEPTOR_STATE_IDLE
			&& g_acceptor.acceptor_round_start + paxos_proposal_timeout()
					<= cf_getms()) {
		// Paxos round is running and has timed out.
		// Consider paxos round failed.
		INFO("paxos round timed out for proposal id %"PRIx64":%"PRIu64,
				config_self_nodeid_get(),
				g_proposer.sequence_number);
		paxos_acceptor_fail();
	}
	CLUSTERING_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Paxos lifecycle and common event handling
 * ----------------------------------------------------------------------------
 */

/**
 * Paxos monitor to detect and cleanup long running and most likely failed paxos
 * rounds.
 */
static void
paxos_monitor()
{
	paxos_proposer_monitor();
	paxos_acceptor_monitor();
}

/**
 * Handle an incoming timer event.
 */
static void
paxos_timer_event_handle()
{
	// Acceptor retransmits handled here.
	paxos_acceptor_timer_event_handle();

	// Proposer retransmits handled here.
	paxos_proposer_timer_event_handle();

	// Invoke Paxos monitor to timeout long running paxos rounds.
	paxos_monitor();
}

/**
 * Handle incoming messages.
 */
static void
paxos_msg_event_handle(as_clustering_internal_event* msg_event)
{
	paxos_acceptor_msg_event_handle(msg_event);
	paxos_proposer_msg_event_handle(msg_event);
}

/**
 * Handle heartbeat event.
 */
static void
paxos_hb_event_handle(as_clustering_internal_event* hb_event)
{
	paxos_proposer_hb_event_handle(hb_event);
}

/**
 * Dispatch clustering events.
 */
static void
paxos_event_dispatch(as_clustering_internal_event* event)
{
	switch (event->type) {
	case AS_CLUSTERING_INTERNAL_EVENT_TIMER:
		paxos_timer_event_handle();
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_MSG:
		paxos_msg_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_HB:
		paxos_hb_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_SYNCED:
		paxos_proposer_register_synched();
	default:	// Not of interest for paxos.
		break;
	}
}

/**
 * Initialize paxos proposer and acceptor data structures.
 */
static void
paxos_init()
{
	paxos_proposer_init();
	paxos_acceptor_init();
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
external_event_publisher_is_running()
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();
	bool running = g_external_event_publisher.sys_state
			== AS_CLUSTERING_SYS_STATE_RUNNING;
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();
	return running;
}

/**
 * Initialize the event publisher.
 */
static void
external_event_publisher_init()
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();
	memset(&g_external_event_publisher, 0, sizeof(g_external_event_publisher));
	vector_lockless_init(&g_external_event_publisher.published_succession_list,
			cf_node);

	pthread_mutex_init(&g_external_event_publisher.is_pending_mutex, NULL);
	pthread_cond_init(&g_external_event_publisher.is_pending, NULL);
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();
}

/**
 * Wakeup the publisher thread.
 */
static void
external_event_publisher_thr_wakeup()
{
	pthread_mutex_lock(&g_external_event_publisher.is_pending_mutex);
	pthread_cond_signal(&g_external_event_publisher.is_pending);
	pthread_mutex_unlock(&g_external_event_publisher.is_pending_mutex);
}

/**
 * Queue up and external event to publish.
 */
static void
external_event_queue(as_clustering_event* event)
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();
	memcpy(&g_external_event_publisher.to_publish, event,
			sizeof(g_external_event_publisher.to_publish));

	vector_clear(&g_external_event_publisher.published_succession_list);
	if (event->succession_list) {
		// Use the static list for the published event, so that the input event
		// object can be destroyed irrespective of when the it is published.
		vector_copy(&g_external_event_publisher.published_succession_list,
				event->succession_list);
		g_external_event_publisher.to_publish.succession_list =
				&g_external_event_publisher.published_succession_list;

	}

	g_external_event_publisher.event_queued = true;

	CLUSTERING_EVENT_PUBLISHER_UNLOCK();

	// Wake up the publisher thread.
	external_event_publisher_thr_wakeup();
}

/**
 * Publish external events if any are pending.
 */
static void
external_events_publish()
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();

	if (g_external_event_publisher.event_queued) {
		g_external_event_publisher.event_queued = false;
		exchange_clustering_event_listener(
				&g_external_event_publisher.to_publish);
	}
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();
}

/**
 * External event publisher thread.
 */
static void*
external_event_publisher_thr(void* arg)
{
	pthread_mutex_lock(&g_external_event_publisher.is_pending_mutex);

	while (true) {
		pthread_cond_wait(&g_external_event_publisher.is_pending,
				&g_external_event_publisher.is_pending_mutex);
		if (external_event_publisher_is_running()) {
			external_events_publish();
		}
		else {
			// Publisher stopped, exit the tread.
			break;
		}
	}

	pthread_mutex_unlock(&g_external_event_publisher.is_pending_mutex);
	return NULL;
}

/**
 * Start the event publisher.
 */
static void
external_event_publisher_start()
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state = AS_CLUSTERING_SYS_STATE_RUNNING;
	g_external_event_publisher.event_publisher_tid =
			cf_thread_create_joinable(external_event_publisher_thr, NULL);
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();
}

/**
 * Stop the event publisher.
 */
static void
external_event_publisher_stop()
{
	CLUSTERING_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state =
			AS_CLUSTERING_SYS_STATE_SHUTTING_DOWN;
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();

	external_event_publisher_thr_wakeup();
	cf_thread_join(g_external_event_publisher.event_publisher_tid);

	CLUSTERING_EVENT_PUBLISHER_LOCK();
	g_external_event_publisher.sys_state = AS_CLUSTERING_SYS_STATE_STOPPED;
	g_external_event_publisher.event_queued = false;
	CLUSTERING_EVENT_PUBLISHER_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Clustering register
 * ----------------------------------------------------------------------------
 */

/**
 * Dump register state to logs.
 */
static void
register_dump(bool verbose)
{
	CLUSTERING_LOCK();

	// Output register state.
	switch (g_register.state) {
	case AS_CLUSTERING_REGISTER_STATE_SYNCED:
		INFO("CL: register: synced");
		break;
	case AS_CLUSTERING_REGISTER_STATE_SYNCING:
		INFO("CL: register: syncing");
		break;
	}

	// Cluster state details.
	INFO("CL: cluster changed at: %"PRIu64" now: %"PRIu64,
			g_register.cluster_modified_time, cf_getms());

	INFO("CL: cluster key: %"PRIx64, g_register.cluster_key);
	INFO("CL: cluster sequence: %"PRIu64, g_register.sequence_number);
	INFO("CL: cluster size: %d", cf_vector_size(&g_register.succession_list));

	if (verbose) {
		log_cf_node_vector("CL: succession:", &g_register.succession_list,
				CF_INFO);
	}

	CLUSTERING_UNLOCK();
}

/**
 * Initialize the register.
 */
static void
register_init()
{
	CLUSTERING_LOCK();
	memset(&g_register, 0, sizeof(g_register));
	vector_lockless_init(&g_register.succession_list, cf_node);
	vector_lockless_init(&g_register.sync_pending, cf_node);
	vector_lockless_init(&g_register.ooo_change_applied_received, cf_node);
	vector_lockless_init(&g_register.ooo_succession_list, cf_node);

	// We are in the orphan state but that will be considered as sync state.
	g_register.state = AS_CLUSTERING_REGISTER_STATE_SYNCED;
	CLUSTERING_UNLOCK();
}

/**
 * Returns true if register sync is pending.
 */
static bool
register_is_sycn_pending()
{
	CLUSTERING_LOCK();
	bool sync_pending = cf_vector_size(&g_register.sync_pending) > 0;
	log_cf_node_vector("pending register sync:", &g_register.sync_pending,
			CF_DETAIL);
	CLUSTERING_UNLOCK();
	return sync_pending;
}

/**
 * Check if the register is synced across the cluster and move to sync state if
 * it is synced.
 */
static void
register_check_and_switch_synced()
{
	CLUSTERING_LOCK();
	if (!register_is_sycn_pending()
			&& g_register.state != AS_CLUSTERING_REGISTER_STATE_SYNCED) {
		g_register.state = AS_CLUSTERING_REGISTER_STATE_SYNCED;
		// Generate internal cluster changed synced.
		as_clustering_internal_event cluster_synced;
		memset(&cluster_synced, 0, sizeof(cluster_synced));
		cluster_synced.type =
				AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_SYNCED;
		internal_event_dispatch(&cluster_synced);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Update register to become an orphan node.
 */
static void
register_become_orphan(as_clustering_event_qualifier qualifier)
{
	CLUSTERING_LOCK();
	g_register.state = AS_CLUSTERING_REGISTER_STATE_SYNCED;
	g_register.cluster_key = 0;
	g_register.sequence_number = 0;
	g_register.has_orphan_transitioned = true;
	g_clustering.has_integrity = false;
	vector_clear(&g_register.succession_list);
	vector_clear(&g_register.sync_pending);

	g_register.cluster_modified_time = cf_getms();
	g_register.cluster_modified_hlc_ts = as_hlc_timestamp_now();

	// Queue internal orphaned event.
	as_clustering_internal_event orphaned_event;
	memset(&orphaned_event, 0, sizeof(orphaned_event));
	orphaned_event.type = AS_CLUSTERING_INTERNAL_EVENT_REGISTER_ORPHANED;
	orphaned_event.qualifier = qualifier;
	internal_event_dispatch(&orphaned_event);

	CLUSTERING_UNLOCK();

	INFO("moved self node to orphan state");
}

/**
 * Handle timer event in the syncing state.
 */
static void
register_syncing_timer_event_handle()
{
	CLUSTERING_LOCK();
	cf_clock now = cf_getms();
	if (g_register.last_sync_check_time + register_sync_check_interval()
			> now) {
		// Give more time before checking for sync.
		goto Exit;
	}

	if (register_is_sycn_pending()) {
		// Update pending nodes based on heartbeat status.
		int num_pending = cf_vector_size(&g_register.sync_pending);
		for (int i = 0; i < num_pending; i++) {
			cf_node pending;
			cf_vector_get(&g_register.sync_pending, i, &pending);
			if (clustering_node_is_sync(pending)) {
				cf_vector_delete(&g_register.sync_pending, i);

				// Compensate the index for the delete.
				i--;

				// Adjust vector size.
				num_pending--;
			}
		}
	}

	register_check_and_switch_synced();

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Send cluster change applied message to all cluster members.
 */
static void
register_cluster_change_applied_msg_send()
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_CLUSTER_CHANGE_APPLIED);

	CLUSTERING_LOCK();

	// Set the cluster key.
	msg_cluster_key_set(msg, g_register.cluster_key);

	// Set the succession list.
	msg_succession_list_set(msg, &g_register.succession_list);

	log_cf_node_vector("cluster change applied message sent to:",
			&g_register.succession_list, CF_DEBUG);

	cf_vector* members = vector_stack_lockless_create(cf_node);
	vector_copy(members, &g_register.succession_list);

	CLUSTERING_UNLOCK();

	// Sent the message to the cluster members.
	msg_nodes_send(msg, members);
	cf_vector_destroy(members);
}

/**
 * Validate cluster state. For now ensure the cluster size is greater than the
 * min cluster size.
 */
static void
register_validate_cluster()
{
	CLUSTERING_LOCK();
	int cluster_size = cf_vector_size(&g_register.succession_list);
	if (!clustering_is_orphan()
			&& cluster_size < g_config.clustering_config.cluster_size_min) {
		WARNING(
				"cluster size %d less than required minimum size %d - switching to orphan state",
				cluster_size, g_config.clustering_config.cluster_size_min);
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle a timer event for the register.
 */
static void
register_timer_event_handle()
{
	CLUSTERING_LOCK();
	switch (g_register.state) {
	case AS_CLUSTERING_REGISTER_STATE_SYNCED:
		register_validate_cluster();
		break;
	case AS_CLUSTERING_REGISTER_STATE_SYNCING:
		register_syncing_timer_event_handle();
		break;
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle paxos round succeeding.
 */
static void
register_paxos_acceptor_success_handle(
		as_clustering_internal_event* paxos_success_event)
{
	CLUSTERING_LOCK();

	g_register.has_orphan_transitioned = false;

	g_register.cluster_key = paxos_success_event->new_cluster_key;
	g_register.sequence_number = paxos_success_event->new_sequence_number;

	vector_clear(&g_register.succession_list);
	vector_copy(&g_register.succession_list,
			paxos_success_event->new_succession_list);

	// Update the timestamps as the register has changed its contents.
	g_register.cluster_modified_time = cf_getms();
	g_register.cluster_modified_hlc_ts = as_hlc_timestamp_now();

	// Initialize pending list with all cluster members.
	g_register.state = AS_CLUSTERING_REGISTER_STATE_SYNCING;
	vector_clear(&g_register.sync_pending);
	vector_copy(&g_register.sync_pending, &g_register.succession_list);
	register_cluster_change_applied_msg_send();

	if (g_register.cluster_key == g_register.ooo_cluster_key
			&& vector_equals(&g_register.succession_list,
					&g_register.ooo_succession_list)) {
		// We have already received change applied message from these node
		// account for them.
		vector_subtract(&g_register.sync_pending,
				&g_register.ooo_change_applied_received);
	}
	vector_clear(&g_register.ooo_change_applied_received);
	vector_clear(&g_register.ooo_succession_list);
	g_register.ooo_cluster_key = 0;
	g_register.ooo_hlc_timestamp = 0;

	INFO("applied new cluster key %"PRIx64,
			paxos_success_event->new_cluster_key);
	log_cf_node_vector("applied new succession list",
			&g_register.succession_list, CF_INFO);
	INFO("applied cluster size %d",
			cf_vector_size(&g_register.succession_list));

	as_clustering_internal_event cluster_changed;
	memset(&cluster_changed, 0, sizeof(cluster_changed));
	cluster_changed.type =
			AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_CHANGED;
	internal_event_dispatch(&cluster_changed);

	// Send change appied message. Its alright even if they are out of order.
	register_cluster_change_applied_msg_send();

	CLUSTERING_UNLOCK();
}

/**
 * Handle incoming cluster change applied message.
 */
static void
register_cluster_change_applied_msg_handle(
		as_clustering_internal_event* msg_event)
{
	CLUSTERING_LOCK();
	as_cluster_key msg_cluster_key = 0;
	msg_cluster_key_get(msg_event->msg, &msg_cluster_key);
	cf_vector *msg_succession_list = vector_stack_lockless_create(cf_node);
	msg_succession_list_get(msg_event->msg, msg_succession_list);
	as_hlc_timestamp msg_hlc_timestamp = 0;
	msg_send_ts_get(msg_event->msg, &msg_hlc_timestamp);

	DEBUG("received cluster change applied message from node %"PRIx64,
			msg_event->msg_src_nodeid);
	if (g_register.cluster_key == msg_cluster_key
			&& vector_equals(&g_register.succession_list,
					msg_succession_list)) {
		// This is a matching change applied message.
		int found_at = 0;
		if ((found_at = vector_find(&g_register.sync_pending,
				&msg_event->msg_src_nodeid)) >= 0) {
			// Remove from the pending list.
			cf_vector_delete(&g_register.sync_pending, found_at);
		}

	}
	else if (g_register.ooo_cluster_key == msg_cluster_key
			&& vector_equals(&g_register.ooo_succession_list,
					msg_succession_list)) {
		DEBUG("received ooo cluster change applied message from node %"PRIx64" with cluster key %"PRIx64, msg_event->msg_src_nodeid, msg_cluster_key);
		cf_vector_append_unique(&g_register.ooo_change_applied_received,
				&msg_event->msg_src_nodeid);

	}
	else if (g_register.ooo_hlc_timestamp < msg_hlc_timestamp) {
		// Prefer a later version of OOO message.
		g_register.ooo_cluster_key = msg_cluster_key;
		g_register.ooo_hlc_timestamp = msg_hlc_timestamp;
		vector_clear(&g_register.ooo_succession_list);
		vector_copy(&g_register.ooo_succession_list, msg_succession_list);
		vector_clear(&g_register.ooo_change_applied_received);
		cf_vector_append_unique(&g_register.ooo_change_applied_received,
				&msg_event->msg_src_nodeid);
		DEBUG("received ooo cluster change applied message from node %"PRIx64" with cluster key %"PRIx64, msg_event->msg_src_nodeid, msg_cluster_key);
	}
	else {
		INFO(
				"ignoring cluster mismatching change applied message from node %"PRIx64,
				msg_event->msg_src_nodeid);
	}
	cf_vector_destroy(msg_succession_list);
	register_check_and_switch_synced();
	CLUSTERING_UNLOCK();
}

/**
 * Handle incoming message.
 */
static void
register_msg_event_handle(as_clustering_internal_event* msg_event)
{
	CLUSTERING_LOCK();
	as_clustering_msg_type type;
	msg_type_get(msg_event->msg, &type);

	if (type == AS_CLUSTERING_MSG_TYPE_CLUSTER_CHANGE_APPLIED) {
		register_cluster_change_applied_msg_handle(msg_event);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Dispatch internal events to the register.
 */
static void
register_event_dispatch(as_clustering_internal_event* event)
{
	switch (event->type) {
	case AS_CLUSTERING_INTERNAL_EVENT_TIMER:
		register_timer_event_handle();
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_PAXOS_ACCEPTOR_SUCCESS:
		register_paxos_acceptor_success_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_MSG:
		register_msg_event_handle(event);
		break;
	default:	// Not of interest for the register.
		break;
	}
}

/*
 * ----------------------------------------------------------------------------
 * Clustering core (triggers cluster changes)
 * ----------------------------------------------------------------------------
 */

/**
 * Send a join reject message to destination node.
 */
static void
clustering_join_reject_send(cf_node dest)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_JOIN_REJECT);

	DETAIL("sent join reject to node %"PRIx64, dest);

	// Sent the message to the acceptors.
	msg_node_send(msg, dest);
}

/**
 * Send cluster join reject message to all nodes in the vector.
 */
static void
clustering_join_requests_reject(cf_vector* rejected_nodes)
{
	int rejected_node_count = cf_vector_size(rejected_nodes);
	for (int i = 0; i < rejected_node_count; i++) {
		// No null check required since we are iterating under a lock and within
		// vector bounds.
		cf_node requesting_nodeid = *((cf_node*)cf_vector_getp(rejected_nodes,
				i));

		// Send the reject message.
		clustering_join_reject_send(requesting_nodeid);
	}
}

/**
 * Send join reject message for all pending join requests.
 */
static void
clustering_join_requests_reject_all()
{
	CLUSTERING_LOCK();

	cf_vector* rejected_nodes = vector_stack_lockless_create(cf_node);
	vector_copy_unique(rejected_nodes, &g_clustering.pending_join_requests);

	vector_clear(&g_clustering.pending_join_requests);

	CLUSTERING_UNLOCK();

	clustering_join_requests_reject(rejected_nodes);

	cf_vector_destroy(rejected_nodes);
}

/**
 * Send a join request to a principal.
 * @param new_principal the destination principal node.
 * @return 0 on successful message queue, -1 on failure.
 */
static int
clustering_join_request_send(cf_node new_principal)
{
	int rv = -1;
	CLUSTERING_LOCK();

	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_JOIN_REQUEST);

	DETAIL("sending cluster join request to node %"PRIx64, new_principal);

	if (msg_node_send(msg, new_principal) == 0) {
		cf_clock now = cf_getms();
		cf_shash_put(g_clustering.join_request_blackout, &new_principal, &now);

		g_clustering.last_join_request_principal = new_principal;
		g_clustering.last_join_request_sent_time =
				g_clustering.last_join_request_retransmit_time = cf_getms();

		INFO("sent cluster join request to %"PRIx64, new_principal);
		rv = 0;
	}

	// Send early reject to all nodes that have send us a join request in the
	// orphan state, because self node is not going to become a principal node.
	// This allows the requesting nodes to send requests to other
	// (potential)principals.
	clustering_join_requests_reject_all();

	CLUSTERING_UNLOCK();
	return rv;
}

/**
 * Retransmit a join request to a previously attmepted principal.
 * @param last_join_request_principal the principal to retransmit to.
 */
static void
clustering_join_request_retransmit(cf_node last_join_request_principal)
{
	CLUSTERING_LOCK();
	cf_node new_principal = g_clustering.last_join_request_principal;
	g_clustering.last_join_request_retransmit_time = cf_getms();
	CLUSTERING_UNLOCK();

	if (new_principal != last_join_request_principal) {
		// The last attempted principal has changed. Don't retransmit.
		return;
	}

	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_JOIN_REQUEST);
	DETAIL("re-sending cluster join request to node %"PRIx64, new_principal);
	if (msg_node_send(msg, new_principal) == 0) {
		DEBUG("re-sent cluster join request to %"PRIx64, new_principal);
	}
}

/**
 *  Remove nodes for which join requests are blocked.
 *
 * @param  requestees the nodes considered for join requests.
 * @param target the result with requestees that are not blocked.
 */
static void
clustering_join_request_filter_blocked(cf_vector* requestees, cf_vector* target)
{
	CLUSTERING_LOCK();
	cf_clock last_sent;
	int requestee_count = cf_vector_size(requestees);
	for (int i = 0; i < requestee_count; i++) {
		cf_node requestee;
		cf_vector_get(requestees, i, &requestee);
		if (cf_shash_get(g_clustering.join_request_blackout, &requestee,
				&last_sent) != CF_SHASH_OK) {
			// The requestee is not marked for blackout
			cf_vector_append(target, &requestee);
		}
	}
	CLUSTERING_UNLOCK();
}

/**
 * Send a cluster join request to a neighboring principal. If
 * preferred_principal is set and it is an eligible neighboring principal, a
 * request is sent to that principal, else this function cycles among eligible
 * neighboring principals at each call.
 *
 * A request will not be sent if there is no neighboring principal.
 *
 * @param preferred_principal the preferred principal to join. User zero if
 * there is no preference.
 * @return 0 if the join request was send or there is one in progress. -1 if
 * there are no principals to try and send the join request.
 */
static as_clustering_join_request_result
clustering_principal_join_request_attempt(cf_node preferred_principal)
{
	CLUSTERING_LOCK();

	as_clustering_join_request_result rv = AS_CLUSTERING_JOIN_REQUEST_SENT;
	cf_vector* neighboring_principals = vector_stack_lockless_create(cf_node);
	cf_vector* eligible_principals = vector_stack_lockless_create(cf_node);

	// Get list of neighboring principals.
	clustering_neighboring_principals_get(neighboring_principals);
	if (cf_vector_size(neighboring_principals) == 0) {
		DEBUG("no neighboring principal found - not sending join request");
		rv = AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS;
		goto Exit;
	}

	clustering_join_request_filter_blocked(neighboring_principals,
			eligible_principals);

	if (cf_vector_size(eligible_principals) == 0) {
		DETAIL("no eligible principals found to make a join request");
		// This principal is still in the blackout list. Do not send a request.
		rv = AS_CLUSTERING_JOIN_REQUEST_PENDING;
		goto Exit;
	}

	int next_join_request_principal_index = -1;

	// We have some well-formed neighboring clusters, try and join them
	if (preferred_principal != 0) {
		int preferred_principal_index = vector_find(eligible_principals,
				&preferred_principal);
		if (preferred_principal_index >= 0) {
			DETAIL("sending join request to preferred principal %"PRIx64,
					preferred_principal);

			// Update the index of the principal to try.
			next_join_request_principal_index = preferred_principal_index;
		}
	}

	if (next_join_request_principal_index == -1) {
		// Choose the first entry, since we have no valid preferred principal.
		next_join_request_principal_index = 0;
		if (g_clustering.last_join_request_principal != 0) {
			// Choose the node after the current principal. If the current
			// principal is not found we start at index 0 else the next index.
			next_join_request_principal_index = vector_find(eligible_principals,
					&g_clustering.last_join_request_principal) + 1;
		}
	}

	// Forget the fact that a join request is pending for a principal.
	g_clustering.last_join_request_principal = 0;

	cf_node* principal_to_try = cf_vector_getp(eligible_principals,
			next_join_request_principal_index
					% cf_vector_size(eligible_principals));

	if (principal_to_try) {
		rv = clustering_join_request_send(*principal_to_try) == 0 ?
				AS_CLUSTERING_JOIN_REQUEST_SENT :
				AS_CLUSTERING_JOIN_REQUEST_SEND_FAILED;

	}
	else {
		DEBUG("no neighboring principal found - not sending join request");
		rv = AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS;
	}

Exit:
	if (rv != AS_CLUSTERING_JOIN_REQUEST_SENT) {
		// Forget the last principal we sent the join request to.
		g_clustering.last_join_request_principal = 0;
		g_clustering.last_join_request_sent_time = 0;
	}

	CLUSTERING_UNLOCK();

	cf_vector_destroy(neighboring_principals);
	cf_vector_destroy(eligible_principals);

	return rv;
}

/**
 * Send a cluster join request to a neighboring orphan who this node thinks will
 * be best suited to form a new cluster.
 */
static as_clustering_join_request_result
clustering_orphan_join_request_attempt()
{
	CLUSTERING_LOCK();

	// Get list of neighboring orphans.
	cf_vector* orphans = vector_stack_lockless_create(cf_node);
	clustering_neighboring_orphans_get(orphans);

	// Get filtered list of orphans.
	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);
	clustering_join_request_filter_blocked(orphans, new_succession_list);

	log_cf_node_vector("neighboring orphans for join request:",
			new_succession_list, CF_DEBUG);

	// Add self node.
	cf_node self_nodeid = config_self_nodeid_get();
	cf_vector_append_unique(new_succession_list, &self_nodeid);

	clustering_succession_list_clique_evict(new_succession_list,
			"clique based evicted nodes for potential cluster:");

	// Sort the new succession list.
	vector_sort_unique(new_succession_list, cf_node_compare_desc);

	as_clustering_join_request_result rv =
			AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS;

	if (cf_vector_size(new_succession_list) > 0) {
		cf_node new_principal = *((cf_node*)cf_vector_getp(new_succession_list,
				0));
		if (new_principal == config_self_nodeid_get()) {
			// No need to send self a join request.
			goto Exit;
		}
		else {
			rv = clustering_join_request_send(new_principal) == 0 ?
					AS_CLUSTERING_JOIN_REQUEST_SENT :
					AS_CLUSTERING_JOIN_REQUEST_SEND_FAILED;
		}
	}

Exit:
	cf_vector_destroy(new_succession_list);
	cf_vector_destroy(orphans);

	CLUSTERING_UNLOCK();
	return rv;
}

/**
 * Remove nodes from the blackout hash once they have been in the list for
 * greater than the blackout period.
 */
int
clustering_join_request_blackout_tend_reduce(const void* key, void* data,
		void* udata)
{
	cf_clock* join_request_send_time = (cf_clock*)data;
	if (*join_request_send_time + join_request_blackout_interval()
			< cf_getms()) {
		return CF_SHASH_REDUCE_DELETE;
	}
	return CF_SHASH_OK;
}

/**
 * Tend the join request blackout data structure to remove blacked out
 * principals.
 */
static void
clustering_join_request_blackout_tend()
{
	CLUSTERING_LOCK();
	cf_shash_reduce(g_clustering.join_request_blackout,
			clustering_join_request_blackout_tend_reduce, NULL);
	CLUSTERING_UNLOCK();
}

/**
 * Send a cluster join request to a neighboring principal if one exists, else if
 * there are no neighboring principals, send a join request to a neighboring
 * orphan node if this node thinks it will win paxos and become the new
 * principal.
 */
static as_clustering_join_request_result
clustering_join_request_attempt()
{
	clustering_join_request_blackout_tend();

	CLUSTERING_LOCK();
	cf_node last_join_request_principal =
			g_clustering.last_join_request_principal;
	cf_clock last_join_request_sent_time =
			g_clustering.last_join_request_sent_time;
	cf_clock last_join_request_retransmit_time =
			g_clustering.last_join_request_retransmit_time;
	CLUSTERING_UNLOCK();

	// Check if the outgoing join request has timed out.
	if (last_join_request_principal
			&& as_hb_is_alive(last_join_request_principal)) {
		if (last_join_request_sent_time + join_request_timeout() > cf_getms()) {
			if (last_join_request_retransmit_time
					+ join_request_retransmit_timeout() < cf_getms()) {
				// Re-transmit join request to the same principal, to cover the
				// case where the previous join request was lost.
				clustering_join_request_retransmit(last_join_request_principal);
			}
			// Wait for the principal to respond. do nothing
			DETAIL(
					"join request to principal %"PRIx64" pending - not attempting new join request",
					last_join_request_principal);

			return AS_CLUSTERING_JOIN_REQUEST_PENDING;
		}
		// Timeout joining a principal. Choose a different principal.
		INFO("join request timed out for principal %"PRIx64,
				last_join_request_principal);

	}

	// Try sending a join request to a neighboring principal.
	as_clustering_join_request_result rv =
			clustering_principal_join_request_attempt(0);

	if (rv != AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS) {
		// There are valid principals around. Don't send a request to
		// neighboring orphan nodes.
		return rv;
	}

	// Send a join request to an orphan node, best suited to be the new
	// principal.
	return clustering_orphan_join_request_attempt();
}

/**
 * Try to become a principal and start a new cluster.
 */
static void
clustering_cluster_form()
{
	ASSERT(clustering_is_orphan(),
			"should not attempt forming new cluster when not an orphan node");

	CLUSTERING_LOCK();
	bool paxos_proposal_started = false;
	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);
	cf_vector* expected_succession_list = vector_stack_lockless_create(cf_node);
	cf_vector* orphans = vector_stack_lockless_create(cf_node);

	clustering_neighboring_orphans_get(orphans);
	vector_copy(new_succession_list, orphans);

	log_cf_node_vector("neighboring orphans for cluster formation:",
			new_succession_list,
			cf_vector_size(new_succession_list) > 0 ? CF_INFO : CF_DEBUG);
	log_cf_node_vector("pending join requests:",
			&g_clustering.pending_join_requests,
			cf_vector_size(&g_clustering.pending_join_requests) > 0 ?
					CF_INFO : CF_DEBUG);

	// Add self node.
	cf_node self_nodeid = config_self_nodeid_get();
	cf_vector_append_unique(new_succession_list, &self_nodeid);

	clustering_succession_list_clique_evict(new_succession_list,
			"clique based evicted nodes at cluster formation:");

	// Sort the new succession list.
	vector_sort_unique(new_succession_list, cf_node_compare_desc);

	cf_vector_append(expected_succession_list, &self_nodeid);
	vector_copy_unique(expected_succession_list,
			&g_clustering.pending_join_requests);
	// Sort the expected succession list.
	vector_sort_unique(expected_succession_list, cf_node_compare_desc);
	// The result should match the pending join requests exactly to consider the
	// new succession list.
	if (!vector_equals(expected_succession_list, new_succession_list)) {
		log_cf_node_vector(
				"skipping forming cluster - cannot form new cluster from pending join requests",
				&g_clustering.pending_join_requests, CF_INFO);
		goto Exit;
	}

	if (cf_vector_size(orphans) > 0
			&& cf_vector_size(new_succession_list) == 1) {
		log_cf_node_vector(
				"skipping forming cluster - there are neighboring orphans that cannot be clustered with",
				orphans, CF_INFO);
		goto Exit;
	}

	if (cf_vector_size(new_succession_list) > 0) {
		cf_node new_principal = *((cf_node*)cf_vector_getp(new_succession_list,
				0));
		if (new_principal == config_self_nodeid_get()) {
			log_cf_node_vector(
					"principal node - forming new cluster with succession list:",
					new_succession_list, CF_INFO);

			as_paxos_start_result result = paxos_proposer_proposal_start(
					new_succession_list, new_succession_list);

			// Log paxos result.
			paxos_result_log(result, new_succession_list);

			paxos_proposal_started = (result == AS_PAXOS_RESULT_STARTED);
		}
		else {
			INFO("skipping cluster formation - a new potential principal %"PRIx64" exists",
					new_principal);
		}
	}

Exit:
	// Compute list of rejected nodes.
	if (paxos_proposal_started) {
		// Nodes in set (pending_join - new succession list) could not be
		// accomodated and should receive a join reject.
		vector_subtract(&g_clustering.pending_join_requests,
				new_succession_list);
	}
	else {
		// Reject all pending join requests. Will happen below.
	}

	cf_vector* rejected_nodes = vector_stack_lockless_create(cf_node);
	vector_copy_unique(rejected_nodes, &g_clustering.pending_join_requests);

	// Clear the pending join requests
	vector_clear(&g_clustering.pending_join_requests);

	// Send reject messages to rejected nodes.
	clustering_join_requests_reject(rejected_nodes);

	cf_vector_destroy(rejected_nodes);

	cf_vector_destroy(orphans);
	cf_vector_destroy(expected_succession_list);
	cf_vector_destroy(new_succession_list);

	CLUSTERING_UNLOCK();
}

/**
 * Try to join a cluster if there is a neighboring one,
 * else try to form one.
 */
static void
clustering_join_or_form_cluster()
{
	ASSERT(clustering_is_orphan(),
			"should not attempt forming new cluster when not an orphan node");

	if (paxos_proposer_proposal_is_active()) {
		// There is an active paxos round with this node as the proposed
		// principal.
		// Skip join cluster attempt and give current paxos round a chance to
		// form the cluster.
		return;
	}

	CLUSTERING_LOCK();

	// TODO (Discuss this): after some timeout and exhausting all neighboring
	// principals, become a single node cluster / try our own cluster. This
	// might not be required. Nonetheless discuss and figure this  out. Current
	// behaviour is form new cluster after a timeout.

	// A node is orphan for too long if it has attempted a join request which
	// timedout and its in orphan state for a while.
	bool orphan_for_too_long = (clustering_orphan_timeout()
			+ g_clustering.orphan_state_start_time) < cf_getms()
			&& g_clustering.last_join_request_principal
			&& g_clustering.last_join_request_sent_time + join_request_timeout()
					< cf_getms();

	if (orphan_for_too_long
			|| clustering_join_request_attempt()
					== AS_CLUSTERING_JOIN_REQUEST_NO_PRINCIPALS) {
		// No neighboring principal found or we have been orphan for too long,
		// try and form a new cluster.
		clustering_cluster_form();
	}
	else {
		// A join request sent successfully or pending. Wait for the new
		// principal to respond.

		// We are not going to be a principal node in this quantum, reject all
		// pending join requests.
		clustering_join_requests_reject_all();
	}

	CLUSTERING_UNLOCK();
}

/**
 * Get a list of nodes that need to be added to current succession list from
 * pending join requests. Bascially filters out node that are not orphans.
 */
static void
clustering_nodes_to_add_get(cf_vector* nodes_to_add)
{
	CLUSTERING_LOCK();

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and release.
	as_hb_plugin_data_iterate(&g_clustering.pending_join_requests,
			AS_HB_PLUGIN_CLUSTERING, clustering_orphan_nodes_find,
			nodes_to_add);

	CLUSTERING_UNLOCK();
}

/**
 * Handle quantum interval start in the orphan state. Try and join / form a
 * cluster.
 */
static void
clustering_orphan_quantum_interval_start_handle()
{
	if (!as_hb_self_is_duplicate()) {
		// Try to join a cluster or form a new one.
		clustering_join_or_form_cluster();
	}
}

/**
 * Send a cluster move command to all nodes in the input list.
 *
 * @param candidate_principal the principal to which the other nodes should try
 * and join after receiving the move command.
 * @param cluster_key current cluster key for receiver validation.
 * @param nodeids the nodes to send move command to.
 */
static void
clustering_cluster_move_send(cf_node candidate_principal,
		as_cluster_key cluster_key, cf_vector* nodeids)
{
	msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_MERGE_MOVE);

	// Set the proposed principal.
	msg_proposed_principal_set(msg, candidate_principal);

	// Set cluster key for message validation.
	msg_cluster_key_set(msg, cluster_key);

	log_cf_node_vector("cluster merge move command sent to:", nodeids,
			CF_DEBUG);

	// Sent the message to the acceptors.
	msg_nodes_send(msg, nodeids);
}

/**
 * Update preferred principal votes using hb plugin data.
 */
static void
clustering_principal_preferred_principal_votes_count(cf_node nodeid,
		void* plugin_data, size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	// A hash from each unique non null vinfo to a vector of partition ids
	// having the vinfo.
	cf_shash* preferred_principal_votes = (cf_shash*)udata;

	CLUSTERING_LOCK();
	if (!clustering_hb_plugin_data_is_obsolete(
			g_register.cluster_modified_hlc_ts,
			g_register.cluster_modified_time, plugin_data, plugin_data_size,
			recv_monotonic_ts, msg_hlc_ts)) {
		cf_node* preferred_principal_p =
				clustering_hb_plugin_preferred_principal_get(plugin_data,
						plugin_data_size);

		int current_votes = 0;
		if (cf_shash_get(preferred_principal_votes, preferred_principal_p,
				&current_votes) == CF_SHASH_OK) {
			current_votes++;
		}
		else {
			// We are seeing this preferred principal for the first time.
			current_votes = 0;
		}

		cf_shash_put(preferred_principal_votes, preferred_principal_p,
				&current_votes);
	}
	else {
		DETAIL(
				"preferred principal voting skipped - found obsolete plugin data for node %"PRIx64,
				nodeid);
	}
	CLUSTERING_UNLOCK();
}

/**
 * Get the preferred majority principal.
 */
static int
clustering_principal_preferred_principal_majority_find(const void* key,
		void* data, void* udata)
{

	const cf_node* current_preferred_principal = (const cf_node*)key;
	int current_preferred_principal_votes = *(int*)data;
	cf_node* majority_preferred_principal = (cf_node*)udata;

	CLUSTERING_LOCK();
	int preferred_principal_majority =
			(int)ceil(
					cf_vector_size(
							&g_register.succession_list) * AS_CLUSTERING_PREFERRRED_PRINCIPAL_MAJORITY);
	bool is_majority = current_preferred_principal_votes
			>= preferred_principal_majority;
	CLUSTERING_UNLOCK();

	if (is_majority) {
		*majority_preferred_principal = *current_preferred_principal;
		// Majority found, halt reduce.
		return CF_SHASH_ERR_FOUND;
	}

	return CF_SHASH_OK;
}

/**
 * Get preferred principal based on a majority of non-principal's preferred
 * principals.
 * @return the preferred principal nodeid if there is a majority, else zero.
 */
static cf_node
clustering_principal_majority_preferred_principal_get()
{
	// A hash from each unique non null vinfo to a vector of partition ids
	// having the vinfo.
	cf_shash* preferred_principal_votes = cf_shash_create(cf_nodeid_shash_fn,
			sizeof(cf_node), sizeof(int), AS_CLUSTERING_CLUSTER_MAX_SIZE_SOFT,
			0);

	CLUSTERING_LOCK();

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and release.
	as_hb_plugin_data_iterate(&g_register.succession_list,
			AS_HB_PLUGIN_CLUSTERING,
			clustering_principal_preferred_principal_votes_count,
			preferred_principal_votes);

	// Find the majority preferred principal.
	cf_node preferred_principal = 0;
	cf_shash_reduce(preferred_principal_votes,
			clustering_principal_preferred_principal_majority_find,
			&preferred_principal);

	CLUSTERING_UNLOCK();

	cf_shash_destroy(preferred_principal_votes);

	DETAIL("preferred principal is %"PRIx64, preferred_principal);

	return preferred_principal;
}

/**
 * Indicates if this node is a principal and its cluster can be merged with this
 * principal node's cluster.
 *
 * @param nodeid the candidate nodeid.
 * @param node_succession_list the candidate node's succession list.
 * @param node_succession_list_length the length of the node's succession list.
 * @return true if current node can be merged with this node's cluster.
 */
bool
clustering_is_merge_candidate(cf_node nodeid, cf_node* node_succession_list,
		int node_succession_list_length)
{
	if (node_succession_list_length <= 0 || node_succession_list[0] != nodeid) {
		// Not a principal node. Ignore.
		return false;
	}

	if (nodeid < config_self_nodeid_get()) {
		// Has a smaller nodeid. Ignore. This node will merge with our cluster.
		return false;
	}

	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);

	CLUSTERING_LOCK();
	vector_copy_unique(new_succession_list, &g_register.succession_list);
	CLUSTERING_UNLOCK();

	bool is_candidate = false;

	// Node is the principal of its cluster. Create the new succession list.
	for (int i = 0; i < node_succession_list_length; i++) {
		cf_vector_append_unique(new_succession_list, &node_succession_list[i]);
	}

	int expected_cluster_size = cf_vector_size(new_succession_list);

	// Find and evict the nodes that  are not well connected.
	clustering_succession_list_clique_evict(new_succession_list,
			"clique based evicted nodes at cluster merge:");
	int new_cluster_size = cf_vector_size(new_succession_list);

	// If no nodes need to be evicted then the merge is fine.
	is_candidate = (expected_cluster_size == new_cluster_size);

	// Exit:
	cf_vector_destroy(new_succession_list);

	return is_candidate;
}

/**
 * HB plugin iterate function to find principals that this node's cluster can be
 * merged with.
 */
static void
clustering_merge_candiate_find(cf_node nodeid, void* plugin_data,
		size_t plugin_data_size, cf_clock recv_monotonic_ts,
		as_hlc_msg_timestamp* msg_hlc_ts, void* udata)
{
	cf_node* candidate_principal = (cf_node*)udata;

	CLUSTERING_LOCK();

	if (!clustering_hb_plugin_data_is_obsolete(
			g_register.cluster_modified_hlc_ts,
			g_register.cluster_modified_time, plugin_data, plugin_data_size,
			recv_monotonic_ts, msg_hlc_ts)) {
		uint32_t* other_succession_list_length =
				clustering_hb_plugin_succession_length_get(plugin_data,
						plugin_data_size);

		cf_node* other_succession_list = clustering_hb_plugin_succession_get(
				plugin_data, plugin_data_size);

		if (other_succession_list != NULL
				&& clustering_is_merge_candidate(nodeid, other_succession_list,
						*other_succession_list_length)
				&& *candidate_principal < nodeid) {
			DETAIL("principal node %"PRIx64" potential candidate for cluster merge", nodeid);
			*candidate_principal = nodeid;
		}

	}
	else {
		DETAIL(
				"merge check skipped - found obsolete plugin data for node %"PRIx64,
				nodeid);
	}

	CLUSTERING_UNLOCK();
}

/**
 * Attempt to move to the majority preferred principal.
 *
 * @return 0 if the move to preferred principal was attempted, -1 otherwise.
 */
static int
clustering_preferred_principal_move()
{
	cf_node preferred_principal =
			clustering_principal_majority_preferred_principal_get();

	if (preferred_principal == 0
			|| preferred_principal == config_self_nodeid_get()) {
		return -1;
	}

	cf_vector* succession_list = vector_stack_lockless_create(cf_node);
	as_cluster_key cluster_key = 0;
	CLUSTERING_LOCK();
	vector_copy(succession_list, &g_register.succession_list);
	cluster_key = g_register.cluster_key;
	// Update the time move command was sent.
	g_clustering.move_cmd_issue_time = cf_getms();
	CLUSTERING_UNLOCK();

	INFO("majority nodes find %"PRIx64" to be a better principal - sending move command to all cluster members",
			preferred_principal);
	clustering_cluster_move_send(preferred_principal, cluster_key,
			succession_list);
	cf_vector_destroy(succession_list);

	return 0;
}

/**
 * Attempt to merge with a larger adjacent cluster is the resulting cluster will
 * form a clique.
 *
 * @return 0 if a merge is attempted, -1 otherwise.
 */
static int
clustering_merge_attempt()
{
	int rv = -1;
	CLUSTERING_LOCK();
	cf_vector* succession_list = vector_stack_lockless_create(cf_node);
	vector_copy(succession_list, &g_register.succession_list);
	as_cluster_key cluster_key = g_register.cluster_key;
	cf_node candidate_principal = 0;

	// Use a single iteration over the clustering data received via the
	// heartbeats instead of individual calls to get a consistent view and avoid
	// small lock and release.
	as_hb_plugin_data_iterate_all(AS_HB_PLUGIN_CLUSTERING,
			clustering_merge_candiate_find, &candidate_principal);

	CLUSTERING_UNLOCK();

	if (candidate_principal == 0) {
		DEBUG("no cluster merge candidates found");
		rv = -1;
		goto Exit;
	}

	// Send a move command to all nodes in the succession list. Need not switch
	// to orphan state immediately, this node will receive the move command too
	// and will handle the move accordingly.
	INFO("this cluster can merge with cluster with principal %"PRIx64" - sending move command to all cluster members",
			candidate_principal);
	clustering_cluster_move_send(candidate_principal, cluster_key,
			succession_list);
	rv = 0;
Exit:
	cf_vector_destroy(succession_list);
	return rv;
}

/**
 * Handle quantum interval start when self node is the principal of its cluster.
 */
static void
clustering_principal_quantum_interval_start_handle(
		as_clustering_internal_event* event)
{
	DETAIL("principal node quantum wakeup");

	if (as_hb_self_is_duplicate()) {
		// Cluster is in a bad shape and self node has a duplicate node-id.
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
		return;
	}

	CLUSTERING_LOCK();
	bool paxos_proposal_started = false;

	cf_vector* dead_nodes = vector_stack_lockless_create(cf_node);
	clustering_dead_nodes_find(dead_nodes);

	log_cf_node_vector("dead nodes at quantum start:", dead_nodes,
			cf_vector_size(dead_nodes) > 0 ? CF_INFO : CF_DEBUG);

	cf_vector* faulty_nodes = vector_stack_lockless_create(cf_node);
	clustering_faulty_nodes_find(faulty_nodes);

	log_cf_node_vector("faulty nodes at quantum start:", faulty_nodes,
			cf_vector_size(faulty_nodes) > 0 ? CF_INFO : CF_DEBUG);

	// Having dead node or faulty nodes is a sign of cluster integrity breach.
	// New nodes should not count as integrity breach.
	g_clustering.has_integrity = cf_vector_size(faulty_nodes) == 0
			&& cf_vector_size(dead_nodes) == 0;

	cf_vector* new_nodes = vector_stack_lockless_create(cf_node);
	clustering_nodes_to_add_get(new_nodes);
	log_cf_node_vector("join requests at quantum start:", new_nodes,
			cf_vector_size(new_nodes) > 0 ? CF_INFO : CF_DEBUG);

	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);
	vector_copy_unique(new_succession_list, &g_register.succession_list);
	vector_subtract(new_succession_list, dead_nodes);
	vector_subtract(new_succession_list, faulty_nodes);
	vector_copy_unique(new_succession_list, new_nodes);

	// Add self node. We should not miss self in the succession list, but be
	// doubly sure.
	cf_node self_nodeid = config_self_nodeid_get();
	cf_vector_append_unique(new_succession_list, &self_nodeid);

	vector_sort_unique(new_succession_list, cf_node_compare_desc);
	uint32_t num_evicted = clustering_succession_list_clique_evict(
			new_succession_list,
			"clique based evicted nodes at quantum start:");

	if (event->quantum_interval_is_skippable && cf_vector_size(dead_nodes) != 0
			&& !quantum_interval_is_adjacency_fault_seen()) {
		// There is an imminent adjacency fault that has not been seen by the
		// quantum interval generator, lets not take any action.
		DEBUG("adjacency fault imminent - skipping quantum interval handling");
		quantum_interval_mark_postponed();
		goto Exit;
	}

	if (event->quantum_interval_is_skippable && num_evicted != 0
			&& !quantum_interval_is_peer_adjacency_fault_seen()) {
		// There is an imminent adjacency fault that has not been seen by the
		// quantum interval generator, lets not take any action.
		DEBUG(
				"peer adjacency fault imminent - skipping quantum interval handling");
		quantum_interval_mark_postponed();
		goto Exit;
	}

	if (cf_vector_size(faulty_nodes) == 0 && cf_vector_size(dead_nodes) == 0) {
		// We might have only pending join requests. Attempt a move to a
		// preferred principal or a merge before trying to add new nodes.
		if (clustering_preferred_principal_move() == 0
				|| clustering_merge_attempt() == 0) {
			goto Exit;
		}
	}

	if (vector_equals(new_succession_list, &g_register.succession_list)
			&& cf_vector_size(faulty_nodes) == 0) {
		// There is no change in the succession list and also there are no
		// faulty nodes. If there are faulty nodes they have probably restarted
		// quickly, in which case a new cluster transition with the same
		// succession list is required.
		goto Exit;
	}

	if (cf_vector_size(faulty_nodes) != 0
			&& cf_vector_size(new_succession_list) == 1) {
		// This node most likely lost time (slept/paused) and the rest of the
		// cluster reformed. Its best to go to the orphan state and start from
		// there instead of moving to a single node cluster and again eventually
		// forming a larger cluster.
		WARNING(
				"all cluster members are part of different cluster - changing state to orphan");
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
		goto Exit;
	}

	// Start a new paxos round.
	log_cf_node_vector("current succession list", &g_register.succession_list,
			CF_DEBUG);

	log_cf_node_vector("proposed succession list", new_succession_list,
			CF_DEBUG);
	DEBUG("proposed cluster size %d", cf_vector_size(new_succession_list));

	as_paxos_start_result result = paxos_proposer_proposal_start(
			new_succession_list, new_succession_list);

	// Log paxos result.
	paxos_result_log(result, new_succession_list);

	// TODO: Should we move to orphan state if there are not enough nodes in the
	// cluster.
	// Tentatively yes....
	if (result == AS_PAXOS_RESULT_CLUSTER_TOO_SMALL) {
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
	}

	paxos_proposal_started = (result == AS_PAXOS_RESULT_STARTED);
Exit:
	// Although these are stack vectors the contents can be heap allocated on
	// resize. Destroy call is prudent.
	cf_vector_destroy(dead_nodes);
	cf_vector_destroy(faulty_nodes);
	cf_vector_destroy(new_nodes);
	cf_vector_destroy(new_succession_list);

	// Compute list of rejected nodes.
	if (paxos_proposal_started) {
		// Nodes in set (pending_join - new succession list) could not be
		// accomodated and should receive a join reject.
		vector_subtract(&g_clustering.pending_join_requests,
				new_succession_list);
	}
	else {
		// Nodes in set (pending_join - current succession list) could not be
		// accomodated and should receive a join reject.
		vector_subtract(&g_clustering.pending_join_requests,
				&g_register.succession_list);

	}

	cf_vector* rejected_nodes = vector_stack_lockless_create(cf_node);
	vector_copy_unique(rejected_nodes, &g_clustering.pending_join_requests);

	// Clear the pending join requests
	vector_clear(&g_clustering.pending_join_requests);

	// Send reject messages to rejected nodes.
	clustering_join_requests_reject(rejected_nodes);

	cf_vector_destroy(rejected_nodes);

	CLUSTERING_UNLOCK();
}

/**
 * Check for and handle eviction by self node's principal.
 *
 * @param principal_plugin_data the pluging data for the principal.
 * @param plugin_data_hlc_ts the hlc timestamp when the plugin data was
 * received.
 * @param plugin_data_ts the monotonic clock timestamp when the plugin data was
 * recvied.
 */
static void
clustering_non_principal_evicted_check(cf_node principal_nodeid,
		as_hb_plugin_node_data* principal_plugin_data,
		as_hlc_msg_timestamp* plugin_data_hlc_ts, cf_clock plugin_data_ts)
{
	CLUSTERING_LOCK();
	bool is_evicted = false;

	if (!as_hb_is_alive(principal_nodeid)) {
		is_evicted = true;
		goto Exit;
	}

	if (!clustering_is_our_principal(principal_nodeid)
			|| clustering_hb_plugin_data_is_obsolete(
					g_register.cluster_modified_hlc_ts,
					g_register.cluster_modified_time,
					principal_plugin_data->data,
					principal_plugin_data->data_size, plugin_data_ts,
					plugin_data_hlc_ts)) {
		// The plugin data is obsolete. Can't take decisions based on it.
		goto Exit;
	}

	// Get the changed node's succession list, cluster key. All the fields
	// should be present since the obsolete check also checked for fields being
	// valid.
	cf_node* succession_list_p = clustering_hb_plugin_succession_get(
			principal_plugin_data->data, principal_plugin_data->data_size);
	uint32_t* succession_list_length_p =
			clustering_hb_plugin_succession_length_get(
					principal_plugin_data->data,
					principal_plugin_data->data_size);

	// Check if we have been evicted.
	if (!clustering_is_node_in_succession(config_self_nodeid_get(),
			succession_list_p, *succession_list_length_p)) {
		is_evicted = true;
	}

Exit:
	if (is_evicted) {
		// This node has been evicted from the cluster.
		WARNING("evicted from cluster by principal node %"PRIx64"- changing state to orphan",
				principal_nodeid);
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
	}

	CLUSTERING_UNLOCK();
}

/**
 * Monitor plugin data change events for evictions.
 */
static void
clustering_non_principal_hb_plugin_data_changed_handle(
		as_clustering_internal_event* change_event)
{
	clustering_non_principal_evicted_check(
			change_event->plugin_data_changed_nodeid, change_event->plugin_data,
			&change_event->plugin_data_changed_hlc_ts,
			change_event->plugin_data_changed_ts);
}

/**
 * Update the preferred principal in the non-principal mode.
 */
static void
clustering_non_principal_preferred_principal_update()
{
	cf_node current_principal = 0;
	if (clustering_principal_get(&current_principal) != 0
			|| current_principal == 0) {
		// We are an orphan.
		return;
	}

	cf_vector* new_succession_list = vector_stack_lockless_create(cf_node);

	clustering_neighboring_nodes_get(new_succession_list);
	cf_node self_nodeid = config_self_nodeid_get();
	cf_vector_append(new_succession_list, &self_nodeid);

	clustering_succession_list_clique_evict(new_succession_list,
			"clique based evicted nodes while updating preferred principal:");

	// Sort the new succession list.
	vector_sort_unique(new_succession_list, cf_node_compare_desc);

	cf_node preferred_principal = 0;
	int new_cluster_size = cf_vector_size(new_succession_list);
	if (new_cluster_size > 0) {
		if (vector_find(new_succession_list, &current_principal) < 0) {
			cf_vector_get(new_succession_list, 0, &preferred_principal);
		}
	}

	CLUSTERING_LOCK();
	if (preferred_principal != 0
			&& g_clustering.preferred_principal != preferred_principal) {
		INFO("preferred principal updated to %"PRIx64,
				g_clustering.preferred_principal);
	}
	g_clustering.preferred_principal = preferred_principal;

	cf_vector_destroy(new_succession_list);
	CLUSTERING_UNLOCK();
}

/**
 * Handle quantum interval start in the non principal state.
 */
static void
clustering_non_principal_quantum_interval_start_handle()
{
	// Reject all accumulated join requests since we are no longer a principal.
	clustering_join_requests_reject_all();

	if (as_hb_self_is_duplicate()) {
		// Cluster is in a bad shape and self node has a duplicate node-id.
		register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
		return;
	}

	// Update the preferred principal.
	clustering_non_principal_preferred_principal_update();

	// Check if we have been evicted.
	cf_node principal = 0;

	if (clustering_principal_get(&principal) != 0) {
		WARNING("could not get principal for self node");
		return;
	}

	as_hlc_msg_timestamp plugin_data_hlc_ts;
	cf_clock plugin_data_ts = 0;
	as_hb_plugin_node_data plugin_data = { 0 };

	if (clustering_hb_plugin_data_get(principal, &plugin_data,
			&plugin_data_hlc_ts, &plugin_data_ts) != 0) {
		plugin_data_ts = 0;
		memset(&plugin_data, 0, sizeof(plugin_data));
	}

	clustering_non_principal_evicted_check(principal, &plugin_data,
			&plugin_data_hlc_ts, plugin_data_ts);
}

/**
 * Handle quantum interval start.
 */
static void
clustering_quantum_interval_start_handle(as_clustering_internal_event* event)
{
	CLUSTERING_LOCK();

	// Dispatch based on state.
	switch (g_clustering.state) {
	case AS_CLUSTERING_STATE_ORPHAN:
		clustering_orphan_quantum_interval_start_handle();
		break;
	case AS_CLUSTERING_STATE_PRINCIPAL:
		clustering_principal_quantum_interval_start_handle(event);
		break;
	case AS_CLUSTERING_STATE_NON_PRINCIPAL:
		clustering_non_principal_quantum_interval_start_handle();
	default:
		break;
	}

	CLUSTERING_UNLOCK();
}

/**
 * Handle a timer event in the orphan state.
 */
static void
clustering_orphan_timer_event_handle()
{
	// Attempt a join request.
	DETAIL("attempting join request from orphan state");
	clustering_join_request_attempt();
}

/**
 * Handle a timer event for the clustering module.
 */
static void
clustering_timer_event_handle()
{
	CLUSTERING_LOCK();

	// Dispatch based on state.
	switch (g_clustering.state) {
	case AS_CLUSTERING_STATE_ORPHAN:
		clustering_orphan_timer_event_handle();
		break;
	default:
		break;
	}

	CLUSTERING_UNLOCK();
}

/**
 * Check if the incoming message is sane to be proccessed further.
 */
static bool
clustering_message_sanity_check(cf_node src_nodeid, msg* msg)
{
	as_cluster_proto_identifier proto;
	if (msg_proto_id_get(msg, &proto) != 0) {
		WARNING(
				"received message with no clustering protocol identifier from node %"PRIx64,
				src_nodeid);
		return false;
	}

	return clustering_versions_are_compatible(proto,
			clustering_protocol_identifier_get());
}

/**
 * Handle an incoming join request. We do not bother with older replay's for
 * join requests because the pending request are cleanup during new cluster
 * formation.
 */
static void
clustering_join_request_handle(as_clustering_internal_event* msg_event)
{
	cf_node src_nodeid = msg_event->msg_src_nodeid;
	DEBUG("received cluster join request from node %"PRIx64, src_nodeid);
	bool fire_quantum_event = false;

	CLUSTERING_LOCK();

	cf_clock now = cf_getms();

	if (g_clustering.move_cmd_issue_time + join_request_move_reject_interval()
			> now) {
		// We have just send out a move request. Reject this join request.
		INFO("ignoring join request from node %"PRIx64" since we have just issued a move command",
				src_nodeid);
		clustering_join_reject_send(src_nodeid);
		goto Exit;
	}

	if ((!clustering_is_principal() && !clustering_is_orphan())
			|| g_clustering.last_join_request_sent_time + join_request_timeout()
					>= cf_getms()) {
		// Can't handle a join request this node is not the principal right now
		// or this node is trying to join another cluster.
		msg* msg = msg_pool_get(AS_CLUSTERING_MSG_TYPE_JOIN_REJECT);

		DETAIL("sent join reject to node %"PRIx64, msg_event->msg_src_nodeid);

		// Sent the message to the acceptors.
		msg_node_send(msg, msg_event->msg_src_nodeid);

		goto Exit;
	}

	if (vector_find(&g_clustering.pending_join_requests, &src_nodeid) >= 0) {
		DEBUG("ignoring join request from node %"PRIx64" since a request is already pending",
				src_nodeid);
		goto Exit;
	}

	// Check if we are receiving a stale or very delayed join request.
	int64_t message_delay_estimate = as_hlc_timestamp_diff_ms(
			as_hlc_timestamp_now(), msg_event->msg_hlc_ts.send_ts);
	if (message_delay_estimate < 0
			|| message_delay_estimate > join_request_accept_delay_max()) {
		INFO("ignoring stale join request from node %"PRIx64" - delay estimate %lu(ms) ",
				src_nodeid, message_delay_estimate);
		goto Exit;
	}

	// Add this request to the pending queue.
	cf_vector_append_unique(&g_clustering.pending_join_requests, &src_nodeid);

	// Generate a join request accepted event for the quantum interval
	// generator.
	as_clustering_internal_event join_request_event;
	memset(&join_request_event, 0, sizeof(join_request_event));
	join_request_event.type =
			AS_CLUSTERING_INTERNAL_EVENT_JOIN_REQUEST_ACCEPTED;
	join_request_event.join_request_source_nodeid = src_nodeid;
	internal_event_dispatch(&join_request_event);
	fire_quantum_event = true;

	INFO("accepted join request from node %"PRIx64, src_nodeid);

Exit:
	CLUSTERING_UNLOCK();

	if (fire_quantum_event) {
		internal_event_dispatch(&join_request_event);
	}
}

/**
 * Handle an incoming join reject.
 */
static void
clustering_join_reject_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;

	DEBUG("received cluster join reject from node %"PRIx64, src_nodeid);

	CLUSTERING_LOCK();

	if (!clustering_is_orphan()) {
		// Already part of a cluster. Ignore the reject.
		INFO(
				"already part of a cluster - ignoring join reject from node %"PRIx64,
				src_nodeid);
		goto Exit;
	}

	if (paxos_proposer_proposal_is_active()) {
		// This node is attempting to form a new cluster.
		INFO(
				"already trying to form a cluster - ignoring join reject from node %"PRIx64,
				src_nodeid);
		goto Exit;
	}

	if (g_clustering.last_join_request_principal == src_nodeid) {
		// This node had requested the source principal for cluster membership
		// which was rejected. Try and join a different cluster.

		// This join request should not be considered as pending, so reset the
		// join request sent time.
		g_clustering.last_join_request_sent_time = 0;
		g_clustering.last_join_request_principal = 0;
		clustering_join_request_attempt();
	}

Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming merge move command. Basically this node switched to orphan
 * state and sends a join request to the principal listed in the merge move.
 */
static void
clustering_merge_move_handle(as_clustering_internal_event* event)
{
	cf_node src_nodeid = event->msg_src_nodeid;

	DEBUG("received cluster merge move from node %"PRIx64, src_nodeid);

	CLUSTERING_LOCK();

	as_cluster_key msg_cluster_key = 0;
	msg_cluster_key_get(event->msg, &msg_cluster_key);

	if (clustering_is_orphan()) {
		// Already part of a cluster. Ignore the reject.
		INFO(
				"already orphan node - ignoring merge move command from node %"PRIx64,
				src_nodeid);
		goto Exit;
	}

	if (msg_is_obsolete(g_register.cluster_modified_hlc_ts,
			g_register.cluster_modified_time, event->msg_recvd_ts,
			&event->msg_hlc_ts) || !clustering_is_our_principal(src_nodeid)
			|| paxos_proposer_proposal_is_active()
			|| msg_cluster_key != g_register.cluster_key) {
		INFO("ignoring cluster merge move from node %"PRIx64, src_nodeid);
		goto Exit;
	}

	// Madril simulation black lists current principal so that we do not end up
	// joining him again immediately. However the check for obsolete data should
	// make that check from madril redundant.
	cf_node new_principal = 0;

	if (msg_proposed_principal_get(event->msg, &new_principal) != 0) {
		// Move command does not have the proposed principal
		WARNING(
				"received merge move command without a proposed principal. Will join the first available principal");
		new_principal = 0;
	}

	// Switch to orphan cluster state so that we move to the new principal.
	register_become_orphan (AS_CLUSTERING_ATTEMPTING_MERGE);

	// Send a join request to a the new principal
	clustering_principal_join_request_attempt(new_principal);
Exit:
	CLUSTERING_UNLOCK();
}

/**
 * Handle an incoming message.
 */
static void
clustering_msg_event_handle(as_clustering_internal_event* msg_event)
{
	// Delegate handling based on message type.
	switch (msg_event->msg_type) {
	case AS_CLUSTERING_MSG_TYPE_JOIN_REQUEST:
		clustering_join_request_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_JOIN_REJECT:
		clustering_join_reject_handle(msg_event);
		break;
	case AS_CLUSTERING_MSG_TYPE_MERGE_MOVE:
		clustering_merge_move_handle(msg_event);
		break;
	default:	// Non cluster management messages.
		break;
	}
}

/**
 * Fabric msg listener that generates an internal message event and dispatches
 * it to the sub system.
 */
static int
clustering_fabric_msg_listener(cf_node msg_src_nodeid, msg* msg, void* udata)
{
	if (!clustering_is_running()) {
		// Ignore fabric messages when clustering is not running.
		WARNING("clustering stopped - ignoring message from node %"PRIx64,
				msg_src_nodeid);
		goto Exit;
	}

	// Sanity check.
	if (!clustering_message_sanity_check(msg_src_nodeid, msg)) {
		WARNING("invalid mesage received from node %"PRIx64, msg_src_nodeid);
		goto Exit;
	}

	as_clustering_internal_event msg_event;
	memset(&msg_event, 0, sizeof(msg_event));
	msg_event.type = AS_CLUSTERING_INTERNAL_EVENT_MSG;

	msg_event.msg_src_nodeid = msg_src_nodeid;

	// Update hlc and store update message timestamp for the event.
	as_hlc_timestamp send_ts = 0;
	msg_send_ts_get(msg, &send_ts);
	as_hlc_timestamp_update(msg_event.msg_src_nodeid, send_ts,
			&msg_event.msg_hlc_ts);

	msg_event.msg = msg;
	msg_event.msg_recvd_ts = cf_getms();
	msg_type_get(msg, &msg_event.msg_type);

	internal_event_dispatch(&msg_event);

Exit:
	as_fabric_msg_put(msg);
	return 0;
}

/**
 * Handle register cluster changed.
 */
static void
clustering_register_cluster_changed_handle()
{
	CLUSTERING_LOCK();

	if (paxos_proposer_proposal_is_active()) {
		paxos_proposer_fail();
	}

	if (clustering_is_principal()) {
		g_clustering.state = AS_CLUSTERING_STATE_PRINCIPAL;
	}
	else {
		g_clustering.state = AS_CLUSTERING_STATE_NON_PRINCIPAL;
		// We are a non-principal. Reject all pending join requests.
		clustering_join_requests_reject_all();
	}

	g_clustering.preferred_principal = 0;
	g_clustering.last_join_request_principal = 0;
	g_clustering.move_cmd_issue_time = 0;

	CLUSTERING_UNLOCK();
}

/**
 * Handle register synced events. Basically this means it is safe to publish the
 * cluster changed event to external sub systems.
 */
static void
clustering_register_cluster_synced_handle(as_clustering_internal_event* event)
{
	CLUSTERING_LOCK();

	// Queue the cluster change event for publishing.
	as_clustering_event cluster_change_event;
	cluster_change_event.type = AS_CLUSTERING_CLUSTER_CHANGED;
	cluster_change_event.qualifier = event->qualifier;
	cluster_change_event.cluster_key = g_register.cluster_key;
	cluster_change_event.succession_list = &g_register.succession_list;
	external_event_queue(&cluster_change_event);

	g_clustering.has_integrity = true;

	CLUSTERING_UNLOCK();
}

/**
 * Handle the register going to orphaned state.
 */
static void
clustering_register_orphaned_handle(as_clustering_internal_event* event)
{
	CLUSTERING_LOCK();
	g_clustering.state = AS_CLUSTERING_STATE_ORPHAN;
	g_clustering.orphan_state_start_time = cf_getms();
	g_clustering.preferred_principal = 0;

	// Queue the cluster change event for publishing.
	as_clustering_event orphaned_event;
	orphaned_event.type = AS_CLUSTERING_ORPHANED;
	orphaned_event.qualifier = event->qualifier;
	orphaned_event.cluster_key = 0;
	orphaned_event.succession_list = NULL;
	external_event_queue(&orphaned_event);
	CLUSTERING_UNLOCK();
}

/**
 * Handle hb plugin data change by dispatching it based on clustering change.
 */
static void
clustering_hb_plugin_data_changed_event_handle(
		as_clustering_internal_event* change_event)
{
	CLUSTERING_LOCK();
	switch (g_clustering.state) {
	case AS_CLUSTERING_STATE_NON_PRINCIPAL:
		clustering_non_principal_hb_plugin_data_changed_handle(change_event);
		break;
	default:
		break;
	}
	CLUSTERING_UNLOCK();
}

/**
 * Handle heartbeat event.
 */
static void
clustering_hb_event_handle(as_clustering_internal_event* hb_event)
{
	for (int i = 0; i < hb_event->hb_n_events; i++) {
		if (hb_event->hb_events[i].evt == AS_HB_NODE_DEPART
				&& clustering_is_our_principal(hb_event->hb_events[i].nodeid)) {
			// Our principal is no longer visible.
			INFO("principal node %"PRIx64" departed - switching to orphan state",
					hb_event->hb_events[i].nodeid);
			register_become_orphan (AS_CLUSTERING_MEMBERSHIP_LOST);
		}
	}
}

/**
 * Handle the fail of a paxos proposal started by the self node.
 */
static void
clustering_paxos_proposer_fail_handle()
{
	// Send reject to all pending join requesters.
	clustering_join_requests_reject_all();
}

/**
 * Clustering module event handler.
 */
static void
clustering_event_handle(as_clustering_internal_event* event)
{
	// Lock to enusure the entire event handling is atomic and parallel events
	// events (hb/fabric)  do not interfere.
	CLUSTERING_LOCK();

	switch (event->type) {
	case AS_CLUSTERING_INTERNAL_EVENT_TIMER:
		clustering_timer_event_handle();
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_QUANTUM_INTERVAL_START:
		clustering_quantum_interval_start_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_HB:
		clustering_hb_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_HB_PLUGIN_DATA_CHANGED:
		clustering_hb_plugin_data_changed_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_MSG:
		clustering_msg_event_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_REGISTER_ORPHANED:
		clustering_register_orphaned_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_CHANGED:
		clustering_register_cluster_changed_handle();
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_REGISTER_CLUSTER_SYNCED:
		clustering_register_cluster_synced_handle(event);
		break;
	case AS_CLUSTERING_INTERNAL_EVENT_PAXOS_PROPOSER_FAIL:	// Send reject message to all
		clustering_paxos_proposer_fail_handle();
		break;
	default:	// Not of interest for main clustering module.
		break;
	}

	CLUSTERING_UNLOCK();
}

/**
 * Initialize the template to be used for clustering messages.
 */
static void
clustering_msg_init()
{
	// Register fabric clustering msg type with no processing function:
	// This permits getting / putting clustering msgs to be moderated via an
	// idle msg queue.
	as_fabric_register_msg_fn(M_TYPE_CLUSTERING, g_clustering_msg_template,
			sizeof(g_clustering_msg_template), AS_CLUSTERING_MSG_SCRATCH_SIZE,
			clustering_fabric_msg_listener, NULL);
}

/**
 * Change listener that updates the first time in current quantum.
 */
static void
clustering_hb_plugin_data_change_listener(cf_node changed_node_id)
{
	if (!clustering_is_running()) {
		return;
	}

	DETAIL("cluster information change detected for node %"PRIx64,
			changed_node_id);

	as_hb_plugin_node_data plugin_data;
	as_clustering_internal_event change_event;
	memset(&change_event, 0, sizeof(change_event));
	change_event.type = AS_CLUSTERING_INTERNAL_EVENT_HB_PLUGIN_DATA_CHANGED;
	change_event.plugin_data_changed_nodeid = changed_node_id;
	change_event.plugin_data = &plugin_data;

	if (clustering_hb_plugin_data_get(changed_node_id, &plugin_data,
			&change_event.plugin_data_changed_hlc_ts,
			&change_event.plugin_data_changed_ts) != 0) {
		// Not possible. We should be able to read the plugin data that changed.
		return;
	}
	internal_event_dispatch(&change_event);
}

/**
 * Listen to external heartbeat event and dispatch an internal heartbeat event.
 */
static void
clustering_hb_event_listener(int n_events, as_hb_event_node* hb_node_events,
		void* udata)
{
	if (!clustering_is_running()) {
		return;
	}

	// Wrap the events in an internal event and dispatch.
	as_clustering_internal_event hb_event;
	memset(&hb_event, 0, sizeof(hb_event));
	hb_event.type = AS_CLUSTERING_INTERNAL_EVENT_HB;
	hb_event.hb_n_events = n_events;
	hb_event.hb_events = hb_node_events;

	internal_event_dispatch(&hb_event);
}

/**
 * Reform the cluster with the same succession list.This would trigger the
 * generation of new partition info and the cluster would get a new cluster key.
 *
 * @return 0 if new clustering round started, 1 if not principal, -1 otherwise.
 */
static int
clustering_cluster_reform()
{
	int rv = -1;
	CLUSTERING_LOCK();

	cf_vector* dead_nodes = vector_stack_lockless_create(cf_node);
	clustering_dead_nodes_find(dead_nodes);

	log_cf_node_vector("recluster: dead nodes - ", dead_nodes,
			cf_vector_size(dead_nodes) > 0 ? CF_INFO : CF_DEBUG);

	cf_vector* faulty_nodes = vector_stack_lockless_create(cf_node);
	clustering_faulty_nodes_find(faulty_nodes);

	log_cf_node_vector("recluster: faulty nodes - ", faulty_nodes,
			cf_vector_size(faulty_nodes) > 0 ? CF_INFO : CF_DEBUG);

	cf_vector* new_nodes = vector_stack_lockless_create(cf_node);
	clustering_nodes_to_add_get(new_nodes);
	log_cf_node_vector("recluster: pending join requests - ", new_nodes,
			cf_vector_size(new_nodes) > 0 ? CF_INFO : CF_DEBUG);

	if (!clustering_is_running() || !clustering_is_principal()
			|| cf_vector_size(dead_nodes) > 0
			|| cf_vector_size(faulty_nodes) > 0
			|| cf_vector_size(new_nodes) > 0) {
		INFO(
				"recluster: skipped - principal %s dead_nodes %d faulty_nodes %d new_nodes %d",
				clustering_is_principal() ? "true" : "false",
				cf_vector_size(dead_nodes), cf_vector_size(faulty_nodes),
				cf_vector_size(new_nodes));

		if (!clustering_is_principal()) {
			// Common case - command will likely be sent to all nodes.
			rv = 1;
		}

		goto Exit;
	}

	cf_vector* succession_list = vector_stack_lockless_create(cf_node);
	vector_copy(succession_list, &g_register.succession_list);

	log_cf_node_vector(
			"recluster: principal node - reforming new cluster with succession list:",
			succession_list, CF_INFO);

	as_paxos_start_result result = paxos_proposer_proposal_start(
			succession_list, succession_list);

	// Log paxos result.
	paxos_result_log(result, succession_list);

	rv = (result == AS_PAXOS_RESULT_STARTED) ? 0 : -1;

	if (rv == -1) {
		INFO("recluster: skipped");
	}
	else {
		INFO("recluster: triggered...");
	}

	cf_vector_destroy(succession_list);

Exit:
	cf_vector_destroy(dead_nodes);
	cf_vector_destroy(faulty_nodes);
	cf_vector_destroy(new_nodes);
	CLUSTERING_UNLOCK();
	return rv;
}

/**
 * Initialize clustering subsystem.
 */
static void
clustering_init()
{
	if (clustering_is_initialized()) {
		return;
	}

	CLUSTERING_LOCK();
	memset(&g_clustering, 0, sizeof(g_clustering));

	// Start out as an orphan cluster.
	g_clustering.state = AS_CLUSTERING_STATE_ORPHAN;
	g_clustering.orphan_state_start_time = cf_getms();

	g_clustering.join_request_blackout = cf_shash_create(cf_nodeid_shash_fn,
			sizeof(cf_node), sizeof(cf_clock),
			AS_CLUSTERING_CLUSTER_MAX_SIZE_SOFT, 0);

	vector_lockless_init(&g_clustering.pending_join_requests, cf_node);

	// Register as a plugin with the heartbeat subsystem.
	as_hb_plugin clustering_plugin;
	memset(&clustering_plugin, 0, sizeof(clustering_plugin));

	clustering_plugin.id = AS_HB_PLUGIN_CLUSTERING;
	// Includes the size for the protocol version, the cluster key, the paxos
	// sequence number for current cluster and the preferred principal.
	clustering_plugin.wire_size_fixed = sizeof(uint32_t)
			+ sizeof(as_cluster_key) + sizeof(as_paxos_sequence_number)
			+ sizeof(cf_node);
	// Size of the node in succession list.
	clustering_plugin.wire_size_per_node = sizeof(cf_node);
	clustering_plugin.set_fn = clustering_hb_plugin_set_fn;
	clustering_plugin.parse_fn = clustering_hb_plugin_parse_data_fn;
	clustering_plugin.change_listener =
			clustering_hb_plugin_data_change_listener;

	as_hb_plugin_register(&clustering_plugin);

	// Register as hb event listener
	as_hb_register_listener(clustering_hb_event_listener, NULL);

	// Initialize fabric message pool.
	clustering_msg_init();

	// Initialize external event publisher.
	external_event_publisher_init();

	// Initialize the register.
	register_init();

	// Initialize timer.
	timer_init();

	// Initialize the quantum interval generator
	quantum_interval_generator_init();

	// Initialize paxos.
	paxos_init();

	g_clustering.sys_state = AS_CLUSTERING_SYS_STATE_STOPPED;

	DETAIL("clustering module initialized");

	CLUSTERING_UNLOCK();
}

/**
 * Start the clustering sub-system.
 */
static void
clustering_start()
{
	if (clustering_is_running()) {
		return;
	}

	CLUSTERING_LOCK();
	g_clustering.sys_state = AS_CLUSTERING_SYS_STATE_RUNNING;
	CLUSTERING_UNLOCK();

	// Start quantum interval generator.
	quantum_interval_generator_start();

	// Start the timer.
	timer_start();

	// Start the external event publisher.
	external_event_publisher_start();
}

/**
 * Stop the clustering sub-system.
 */
static void
clustering_stop()
{
	if (!clustering_is_running()) {
		return;
	}

	CLUSTERING_LOCK();
	g_clustering.sys_state = AS_CLUSTERING_SYS_STATE_SHUTTING_DOWN;
	CLUSTERING_UNLOCK();

	// Stop the timer.
	timer_stop();

	// Stop the external event publisher.
	external_event_publisher_stop();

	CLUSTERING_LOCK();
	g_clustering.sys_state = AS_CLUSTERING_SYS_STATE_STOPPED;
	CLUSTERING_UNLOCK();
}

/**
 * Dump clustering state to logs.
 */
static void
clustering_dump(bool verbose)
{
	if (!clustering_is_running()) {
		INFO("CL: stopped");
		return;
	}

	paxos_proposer_dump(verbose);
	paxos_acceptor_dump(verbose);
	register_dump(verbose);

	CLUSTERING_LOCK();

	switch (g_clustering.state) {
	case AS_CLUSTERING_STATE_ORPHAN:
		INFO("CL: state: orphan");
		break;
	case AS_CLUSTERING_STATE_PRINCIPAL:
		INFO("CL: state: principal");
		break;
	case AS_CLUSTERING_STATE_NON_PRINCIPAL:
		INFO("CL: state: non-principal");
		break;
	}

	INFO("CL: %s",
			g_clustering.has_integrity ? "has integrity" : "integrity fault");
	cf_node current_principal;
	if (clustering_principal_get(&current_principal) != 0) {
		if (g_clustering.preferred_principal != current_principal) {
			INFO("CL: preferred principal %"PRIx64,
					g_clustering.preferred_principal);
		}
	}

	if (g_clustering.state == AS_CLUSTERING_STATE_ORPHAN) {
		INFO("CL: join request sent to principal %"PRIx64,
				g_clustering.last_join_request_principal);
		INFO("CL: join request sent time: %"PRIu64" now: %"PRIu64 ,
				g_clustering.last_join_request_sent_time, cf_getms());
	}

	if (verbose) {
		log_cf_node_vector("CL: pending join requests:",
				&g_clustering.pending_join_requests, CF_INFO);
	}

	CLUSTERING_UNLOCK();
}

/*
 * ----------------------------------------------------------------------------
 * Internal event dispatcher
 * ----------------------------------------------------------------------------
 */

/**
 * Simple dispatcher for events. The order of dispatch is from lower (less
 * dependent) to higher (more dependent) sub-modules.
 */
static void
internal_event_dispatch(as_clustering_internal_event* event)
{
	// Sub-module dispatch.
	quantum_interval_generator_event_dispatch(event);
	paxos_event_dispatch(event);
	register_event_dispatch(event);

	// Dispatch to the main clustering module.
	clustering_event_handle(event);
}

/*
 * ----------------------------------------------------------------------------
 * Public API.
 * ----------------------------------------------------------------------------
 */

/**
 *
 * Initialize clustering subsystem.
 */
void
as_clustering_init()
{
	clustering_init();
}

/**
 * Start clustering subsystem.
 */
void
as_clustering_start()
{
	clustering_start();
}

/**
 * Stop clustering subsystem.
 */
void
as_clustering_stop()
{
	clustering_stop();
}

/**
 * Reform the cluster with the same succession list.This would trigger the
 * generation of new partition info and the cluster would get a new cluster key.
 *
 * @return 0 if new clustering round started, -1 otherwise.
 */
int
as_clustering_cluster_reform()
{
	return clustering_cluster_reform();
}

/**
 * Return the quantum interval, i.e., the interval at which cluster change
 * decisions are taken. The unit is milliseconds.
 */
uint64_t
as_clustering_quantum_interval()
{
	return quantum_interval();
}

/**
 * TEMPORARY - used by paxos only.
 */
void
as_clustering_set_integrity(bool has_integrity)
{
	g_clustering.has_integrity = has_integrity;
}

/*
 * ----------------------------------------------------------------------------
 * Clustering info command functions.
 * ----------------------------------------------------------------------------
 */

/**
 * If false means than either this node is orphaned, or is undergoing a cluster
 * change.
 */
bool
as_clustering_has_integrity()
{
	return g_clustering.has_integrity;
}

/**
 * Indicates if self node is orphaned.
 */
bool
as_clustering_is_orphan()
{
	return clustering_is_orphan();
}

/**
 * Dump clustering state to the log.
 */
void
as_clustering_dump(bool verbose)
{
	clustering_dump(verbose);
}

/**
 * Set the min cluster size.
 */
int
as_clustering_cluster_size_min_set(uint32_t new_cluster_size_min)
{
	CLUSTERING_LOCK();
	int rv = 0;
	uint32_t cluster_size = cf_vector_size(&g_register.succession_list);
	if (clustering_is_orphan() || cluster_size >= new_cluster_size_min) {
		INFO("changing value of min-cluster-size from %u to %u",
				g_config.clustering_config.cluster_size_min,
				new_cluster_size_min);
		g_config.clustering_config.cluster_size_min = new_cluster_size_min;
	}
	else {
		WARNING(
				"min-cluster-size %d should be <= current cluster size %d - ignoring",
				new_cluster_size_min, cluster_size);
		rv = -1;
	}
	CLUSTERING_UNLOCK();
	return rv;
}

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
		cf_vector* nodes)
{
	as_clustering_cf_node_array_event(severity, context, file_name, line,
			message, vector_to_array(nodes), cf_vector_size(nodes));
}

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
		cf_node* nodes, int node_count)
{
	if (!cf_context_at_severity(context, severity) && severity != CF_DETAIL) {
		return;
	}

	// Also account the space following the nodeid.
	int node_str_len = 2 * (sizeof(cf_node)) + 1;

	int message_length = 0;
	char copied_message[LOG_LENGTH_MAX()];

	if (message) {
		// Limit the message length to allow at least one node to fit in the log
		// line. Accounting for the separator between message and node list.
		message_length = MIN(strnlen(message, LOG_LENGTH_MAX() - 1),
		LOG_LENGTH_MAX() - 1 - node_str_len) + 1;

		// Truncate the message.
		strncpy(copied_message, message, message_length);
		message = copied_message;
	}

	// Allow for the NULL terminator.
	int nodes_per_line = (LOG_LENGTH_MAX() - message_length - 1) / node_str_len;
	nodes_per_line = MAX(1, nodes_per_line);

	// Have a buffer large enough to accomodate the message and nodes per line.
	char log_buffer[message_length + (nodes_per_line * node_str_len) + 1];	// For the NULL terminator.
	int output_node_count = 0;

	// Marks the start of the nodeid list in the log line buffer.
	char* node_buffer_start = log_buffer;
	if (message) {
		node_buffer_start += sprintf(log_buffer, "%s ", message);
	}

	for (int i = 0; i < node_count;) {
		char* buffer = node_buffer_start;

		for (int j = 0; j < nodes_per_line && i < node_count; j++) {
			buffer += sprintf(buffer, "%"PRIx64" ", nodes[i]);
			output_node_count++;
			i++;
		}

		// Overwrite the space from the last node on the log line only if there
		// is atleast one node output
		if (buffer != node_buffer_start) {
			*(buffer - 1) = 0;
			cf_fault_event(context, severity, file_name, line, "%s",
					log_buffer);
		}
	}

	// Handle the empty vector case.
	if (output_node_count == 0) {
		sprintf(node_buffer_start, "(empty)");
		cf_fault_event(context, severity, file_name, line, "%s", log_buffer);
	}
}
