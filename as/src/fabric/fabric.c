/*
 * fabric.c
 *
 * Copyright (C) 2008-2018 Aerospike, Inc.
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

//   Object Management:
//   ------------------
//
//	 Node and FC objects are reference counted. Correct book keeping on object
//	 references are vital to system operations.
//
//   Holders of FC references:
//     (1) node->fc_hash
//     (2) node->send_idle_fc_queue
//     (3) (epoll_event ev).data.ptr
//
//	 For sending, (2) and (3) are mutually exclusive.
//	 Refs between (2) and (3) are passed virtually whenever possible, without
//	 needing to explicitly call reserve/release.
//	 (3) takes ref on rearm.
//	 (3) gives ref to calling thread when epoll triggers, due to ONESHOT.
//	     Thread will either rearm or give ref to (2). Never do both.
//
//	 FCs are created in two methods: fabric_node_connect(), run_fabric_accept()
//
//   Holders of Node references:
//     * fc->node
//     * g_fabric.node_hash


//==========================================================
// Includes.
//

#include "fabric/fabric.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_vector.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "rchash.h"
#include "shash.h"
#include "socket.h"
#include "tls.h"

#include "base/cfg.h"
#include "base/health.h"
#include "base/stats.h"
#include "fabric/endpoint.h"
#include "fabric/hb.h"


//==========================================================
// Typedefs & constants.
//

#define FABRIC_SEND_MEM_SZ			(1024) // bytes
#define FABRIC_BUFFER_MEM_SZ		(1024 * 1024) // bytes
#define FABRIC_BUFFER_MAX_SZ		(128 * 1024 * 1024) // used simply for validation
#define FABRIC_EPOLL_SEND_EVENTS	16
#define FABRIC_EPOLL_RECV_EVENTS	1

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	FS_FIELD_NODE,
	FS_UNUSED1, // used to be FS_ADDR
	FS_UNUSED2, // used to be FS_PORT
	FS_UNUSED3, // used to be FS_ANV
	FS_UNUSED4, // used to be FS_ADDR_EX
	FS_CHANNEL,

	NUM_FS_FIELDS
} fs_msg_fields;

static const msg_template fabric_mt[] = {
		{ FS_FIELD_NODE, M_FT_UINT64 },
		{ FS_UNUSED1, M_FT_UINT32 },
		{ FS_UNUSED2, M_FT_UINT32 },
		{ FS_UNUSED3, M_FT_BUF },
		{ FS_UNUSED4, M_FT_BUF },
		{ FS_CHANNEL, M_FT_UINT32 },
};

COMPILER_ASSERT(sizeof(fabric_mt) / sizeof(msg_template) == NUM_FS_FIELDS);

#define FS_MSG_SCRATCH_SIZE 0

#define DEFAULT_EVENTS (EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLONESHOT)

// Block size for allocating fabric hb plugin data.
#define HB_PLUGIN_DATA_BLOCK_SIZE 128

typedef struct fabric_recv_thread_pool_s {
	cf_vector threads;
	cf_poll poll;
	uint32_t pool_id;
} fabric_recv_thread_pool;

typedef struct send_entry_s {
	struct send_entry_s *next;
	uint32_t id;
	uint32_t count;
	cf_poll poll;
} send_entry;

typedef struct fabric_state_s {
	as_fabric_msg_fn	msg_cb[M_TYPE_MAX];
	void 				*msg_udata[M_TYPE_MAX];

	fabric_recv_thread_pool recv_pool[AS_FABRIC_N_CHANNELS];

	cf_mutex			send_lock;
	send_entry			*sends;
	send_entry			*send_head;

	cf_mutex			node_hash_lock;
	cf_rchash			*node_hash; // key is cf_node, value is (fabric_node *)
} fabric_state;

typedef struct fabric_node_s {
	cf_node 	node_id; // remote node
	bool		live; // set to false on shutdown
	uint32_t	connect_count[AS_FABRIC_N_CHANNELS];
	bool		connect_full;

	cf_mutex	connect_lock;

	cf_mutex	fc_hash_lock;
	cf_shash	*fc_hash; // key is (fabric_connection *), value unused

	cf_mutex	send_idle_fc_queue_lock;
	cf_queue	send_idle_fc_queue[AS_FABRIC_N_CHANNELS];

	cf_queue	send_queue[AS_FABRIC_N_CHANNELS];

	uint8_t		send_counts[];
} fabric_node;

typedef struct fabric_connection_s {
	cf_socket sock;
	cf_sock_addr peer;
	fabric_node *node;

	bool failed;
	bool started_via_connect;

	bool			s_cork_bypass;
	int				s_cork;
	uint8_t			s_buf[FABRIC_SEND_MEM_SZ];
	struct iovec	*s_iov;
	size_t			s_iov_count;
	uint32_t		s_sz;
	uint32_t		s_msg_sz;
	msg				*s_msg_in_progress;
	size_t			s_count;

	uint8_t			*r_bigbuf;
	uint8_t			r_buf[FABRIC_BUFFER_MEM_SZ + sizeof(msg_hdr)];
	uint32_t		r_rearm_count;
	msg_type		r_type;
	uint32_t		r_sz;
	uint32_t		r_buf_sz;
	uint64_t		benchmark_time;

	// The send_ptr != NULL means that the FC's sock has registered with
	// send_poll. This is needed because epoll's API doesn't allow registering
	// a socket without event triggers (ERR and HUP are enabled even when
	// unspecified).
	send_entry *send_ptr;
	fabric_recv_thread_pool *pool;

	uint64_t s_bytes;
	uint64_t s_bytes_last;
	uint64_t r_bytes;
	uint64_t r_bytes_last;
} fabric_connection;

typedef struct node_list_s {
	uint32_t count;
	cf_node nodes[AS_CLUSTER_SZ]; // must support the maximum cluster size.
} node_list;

const char *CHANNEL_NAMES[] = {
		[AS_FABRIC_CHANNEL_RW]   = "rw",
		[AS_FABRIC_CHANNEL_CTRL] = "ctrl",
		[AS_FABRIC_CHANNEL_BULK] = "bulk",
		[AS_FABRIC_CHANNEL_META] = "meta",
};

COMPILER_ASSERT(sizeof(CHANNEL_NAMES) / sizeof(const char *) ==
		AS_FABRIC_N_CHANNELS);

const bool channel_nagle[] = {
		[AS_FABRIC_CHANNEL_RW]   = false,
		[AS_FABRIC_CHANNEL_CTRL] = false,
		[AS_FABRIC_CHANNEL_BULK] = true,
		[AS_FABRIC_CHANNEL_META] = false,
};

COMPILER_ASSERT(sizeof(channel_nagle) / sizeof(bool) == AS_FABRIC_N_CHANNELS);


//==========================================================
// Globals.
//

cf_serv_cfg g_fabric_bind = { .n_cfgs = 0 };
cf_tls_info *g_fabric_tls;

static fabric_state g_fabric;
static cf_poll g_accept_poll;

static as_endpoint_list *g_published_endpoint_list;
static bool g_published_endpoint_list_ipv4_only;

// Max connections formed via connect. Others are formed via accept.
static uint32_t g_fabric_connect_limit[AS_FABRIC_N_CHANNELS];


//==========================================================
// Forward declarations.
//

// Support functions.
static void send_entry_insert(send_entry **se_pp, send_entry *se);

static void fabric_published_serv_cfg_fill(const cf_serv_cfg *bind_cfg, cf_serv_cfg *published_cfg, bool ipv4_only);
static bool fabric_published_endpoints_refresh(void);

// fabric_node
static fabric_node *fabric_node_create(cf_node node_id);
static fabric_node *fabric_node_get(cf_node node_id);
static fabric_node *fabric_node_get_or_create(cf_node node_id);
static fabric_node *fabric_node_pop(cf_node node_id);
static int fabric_node_disconnect_reduce_fn(const void *key, void *data, void *udata);
static void fabric_node_disconnect(cf_node node_id);

static fabric_connection *fabric_node_connect(fabric_node *node, uint32_t ch);
static int fabric_node_send(fabric_node *node, msg *m, as_fabric_channel channel);
static void fabric_node_connect_all(fabric_node *node);
static void fabric_node_destructor(void *pnode);
inline static void fabric_node_reserve(fabric_node *node);
inline static void fabric_node_release(fabric_node *node);
static bool fabric_node_add_connection(fabric_node *node, fabric_connection *fc);
static uint8_t fabric_node_find_min_send_count(const fabric_node *node);
static bool fabric_node_is_connect_full(const fabric_node *node);

static int fabric_get_node_list_fn(const void *key, uint32_t keylen, void *data, void *udata);
static uint32_t fabric_get_node_list(node_list *nl);

// fabric_connection
fabric_connection *fabric_connection_create(cf_socket *sock, cf_sock_addr *peer);
static bool fabric_connection_accept_tls(fabric_connection *fc);
static bool fabric_connection_connect_tls(fabric_connection *fc);
inline static void fabric_connection_reserve(fabric_connection *fc);
static void fabric_connection_release(fabric_connection *fc);
inline static cf_node fabric_connection_get_id(const fabric_connection *fc);

inline static void fabric_connection_cork(fabric_connection *fc);
inline static void fabric_connection_uncork(fabric_connection *fc);
static void fabric_connection_send_assign(fabric_connection *fc);
static void fabric_connection_send_unassign(fabric_connection *fc);
inline static void fabric_connection_recv_rearm(fabric_connection *fc);
inline static void fabric_connection_send_rearm(fabric_connection *fc);
static void fabric_connection_disconnect(fabric_connection *fc);
static void fabric_connection_set_keepalive_options(fabric_connection *fc);

static void fabric_connection_reroute_msg(fabric_connection *fc);
static bool fabric_connection_send_progress(fabric_connection *fc);
static bool fabric_connection_process_writable(fabric_connection *fc);

static bool fabric_connection_process_fabric_msg(fabric_connection *fc, const msg *m);
static bool fabric_connection_read_fabric_msg(fabric_connection *fc);

static bool fabric_connection_process_msg(fabric_connection *fc, bool do_rearm);
static bool fabric_connection_process_readable(fabric_connection *fc);

// fabric_recv_thread_pool
static void fabric_recv_thread_pool_init(fabric_recv_thread_pool *pool, uint32_t size, uint32_t pool_id);
static void fabric_recv_thread_pool_set_size(fabric_recv_thread_pool *pool, uint32_t size);
static void fabric_recv_thread_pool_add_fc(fabric_recv_thread_pool *pool, fabric_connection *fc);

// fabric_endpoint
static bool fabric_endpoint_list_get(cf_node nodeid, as_endpoint_list *endpoint_list, size_t *endpoint_list_size);
static bool fabric_connect_endpoint_filter(const as_endpoint *endpoint, void *udata);

// Thread functions.
static void *run_fabric_recv(void *arg);
static void *run_fabric_send(void *arg);
static void *run_fabric_accept(void *arg);

// Ticker helpers.
static int fabric_rate_node_reduce_fn(const void *key, uint32_t keylen, void *data, void *udata);
static int fabric_rate_fc_reduce_fn(const void *key, void *data, void *udata);

// Heartbeat.
static void fabric_hb_plugin_set_fn(msg *m);
static void fabric_hb_plugin_parse_data_fn(msg *m, cf_node source, as_hb_plugin_node_data *prev_plugin_data,  as_hb_plugin_node_data *plugin_data);
static void fabric_heartbeat_event(int nevents, as_hb_event_node *events, void *udata);


//==========================================================
// Public API.
//

//------------------------------------------------
// msg
//

// Log information about existing "msg" objects and queues.
void
as_fabric_msg_queue_dump()
{
	cf_info(AS_FABRIC, "All currently-existing msg types:");

	int total_q_sz = 0;
	int total_alloced_msgs = 0;

	for (int i = 0; i < M_TYPE_MAX; i++) {
		int num_of_type = cf_atomic_int_get(g_num_msgs_by_type[i]);

		total_alloced_msgs += num_of_type;

		if (num_of_type) {
			cf_info(AS_FABRIC, "alloc'd = %d", num_of_type);
		}
	}

	int num_msgs = cf_atomic_int_get(g_num_msgs);

	if (abs(num_msgs - total_alloced_msgs) > 2) {
		cf_warning(AS_FABRIC, "num msgs (%d) != total alloc'd msgs (%d)", num_msgs, total_alloced_msgs);
	}

	cf_info(AS_FABRIC, "Total num. msgs = %d ; Total num. queued = %d ; Delta = %d", num_msgs, total_q_sz, num_msgs - total_q_sz);
}

//------------------------------------------------
// as_fabric
//

void
as_fabric_init()
{
	for (uint32_t i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		g_fabric_connect_limit[i] = g_config.n_fabric_channel_fds[i];

		fabric_recv_thread_pool_init(&g_fabric.recv_pool[i],
				g_config.n_fabric_channel_recv_threads[i], i);
	}

	cf_mutex_init(&g_fabric.send_lock);

	as_fabric_register_msg_fn(M_TYPE_FABRIC, fabric_mt, sizeof(fabric_mt),
			FS_MSG_SCRATCH_SIZE, NULL, NULL);

	cf_mutex_init(&g_fabric.node_hash_lock);

	g_fabric.node_hash = cf_rchash_create(cf_nodeid_rchash_fn,
			fabric_node_destructor, sizeof(cf_node), 128, 0);

	g_published_endpoint_list = NULL;
	g_published_endpoint_list_ipv4_only = cf_ip_addr_legacy_only();

	if (! fabric_published_endpoints_refresh()) {
		cf_crash(AS_FABRIC, "error creating fabric published endpoint list");
	}

	as_hb_plugin fabric_plugin;

	memset(&fabric_plugin, 0, sizeof(fabric_plugin));
	fabric_plugin.id = AS_HB_PLUGIN_FABRIC;
	fabric_plugin.wire_size_fixed = 0; // includes the size for the protocol version
	as_endpoint_list_sizeof(g_published_endpoint_list,
			&fabric_plugin.wire_size_fixed);
	fabric_plugin.wire_size_per_node = 0; // size per node node in succession list
	fabric_plugin.set_fn = fabric_hb_plugin_set_fn;
	fabric_plugin.parse_fn = fabric_hb_plugin_parse_data_fn;
	fabric_plugin.change_listener = NULL;
	as_hb_plugin_register(&fabric_plugin);

	as_hb_register_listener(fabric_heartbeat_event, &g_fabric);
}

void
as_fabric_start()
{
	g_fabric.sends =
			cf_malloc(sizeof(send_entry) * g_config.n_fabric_send_threads);
	g_fabric.send_head = g_fabric.sends;

	cf_info(AS_FABRIC, "starting %u fabric send threads", g_config.n_fabric_send_threads);

	for (int i = 0; i < g_config.n_fabric_send_threads; i++) {
		cf_poll_create(&g_fabric.sends[i].poll);
		g_fabric.sends[i].id = i;
		g_fabric.sends[i].count = 0;
		g_fabric.sends[i].next = g_fabric.sends + i + 1;

		cf_thread_create_detached(run_fabric_send, (void*)&g_fabric.sends[i]);
	}

	g_fabric.sends[g_config.n_fabric_send_threads - 1].next = NULL;

	for (uint32_t i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		cf_info(AS_FABRIC, "starting %u fabric %s channel recv threads", g_config.n_fabric_channel_recv_threads[i], CHANNEL_NAMES[i]);

		fabric_recv_thread_pool_set_size(&g_fabric.recv_pool[i],
				g_config.n_fabric_channel_recv_threads[i]);
	}

	cf_info(AS_FABRIC, "starting fabric accept thread");

	cf_thread_create_detached(run_fabric_accept, NULL);
}

void
as_fabric_set_recv_threads(as_fabric_channel channel, uint32_t count)
{
	g_config.n_fabric_channel_recv_threads[channel] = count;

	fabric_recv_thread_pool_set_size(&g_fabric.recv_pool[channel], count);
}

int
as_fabric_send(cf_node node_id, msg *m, as_fabric_channel channel)
{
	m->benchmark_time = g_config.fabric_benchmarks_enabled ? cf_getns() : 0;

	if (g_config.self_node == node_id) {
		cf_assert(g_fabric.msg_cb[m->type], AS_FABRIC, "m->type %d not registered", m->type);
		(g_fabric.msg_cb[m->type])(node_id, m, g_fabric.msg_udata[m->type]);

		return AS_FABRIC_SUCCESS;
	}

	fabric_node *node = fabric_node_get(node_id);
	int ret = fabric_node_send(node, m, channel);

	if (node) {
		fabric_node_release(node); // from fabric_node_get
	}

	return ret;
}

int
as_fabric_send_list(const cf_node *nodes, uint32_t node_count, msg *m,
		as_fabric_channel channel)
{
	cf_assert(nodes && node_count != 0, AS_FABRIC, "nodes list null or empty");

	// TODO - if we implement an out-of-scope response when sending to self,
	// remove this deferral.
	bool send_self = false;

	for (uint32_t i = 0; i < node_count; i++) {
		if (nodes[i] == g_config.self_node) {
			send_self = true;
			continue;
		}

		msg_incr_ref(m);

		int ret = as_fabric_send(nodes[i], m, channel);

		if (ret != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
			return ret; // caller releases main reference on failure
		}
	}

	if (send_self) {
		// Shortcut - use main reference for fabric.
		return as_fabric_send(g_config.self_node, m, channel);
	}

	as_fabric_msg_put(m); // release main reference

	return AS_FABRIC_SUCCESS;
}

int
as_fabric_retransmit(cf_node node_id, msg *m, as_fabric_channel channel)
{
	// This function assumes the sender holds only a single reference to the
	// msg. Do not use this function when there may be more than one reference
	// to an unsent msg.

	if (cf_rc_count(m) > 1) {
		// Msg should already be in the fabric queue - success.
		return AS_FABRIC_SUCCESS;
	}

	msg_incr_ref(m);

	int err = as_fabric_send(node_id, m, channel);

	if (err != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
		return err;
	}

	return AS_FABRIC_SUCCESS;
}

// TODO - make static registration
void
as_fabric_register_msg_fn(msg_type type, const msg_template *mt, size_t mt_sz,
		size_t scratch_sz, as_fabric_msg_fn msg_cb, void *msg_udata)
{
	msg_type_register(type, mt, mt_sz, scratch_sz);

	g_fabric.msg_cb[type] = msg_cb;
	g_fabric.msg_udata[type] = msg_udata;
}

void
as_fabric_info_peer_endpoints_get(cf_dyn_buf *db)
{
	node_list nl;
	fabric_get_node_list(&nl);

	for (uint32_t i = 0; i < nl.count; i++) {
		if (nl.nodes[i] == g_config.self_node) {
			continue;
		}

		fabric_node *node = fabric_node_get(nl.nodes[i]);

		if (! node) {
			cf_info(AS_FABRIC, "\tnode %lx not found in hash although reported available", nl.nodes[i]);
			continue;
		}

		size_t endpoint_list_capacity = 1024;
		bool retry = true;

		while (true) {
			uint8_t stack_mem[endpoint_list_capacity];
			as_endpoint_list *endpoint_list = (as_endpoint_list *)stack_mem;

			if (! fabric_endpoint_list_get(node->node_id, endpoint_list,
					&endpoint_list_capacity)) {
				if (errno == ENOENT) {
					// No entry present for this node in heartbeat.
					cf_detail(AS_FABRIC, "could not get endpoint list for %lx", node->node_id);
					break;
				}

				if (! retry) {
					break;
				}

				retry = false;
				continue;
			}

			cf_dyn_buf_append_string(db, "fabric.peer=");
			cf_dyn_buf_append_string(db, "node-id=");
			cf_dyn_buf_append_uint64_x(db, node->node_id);
			cf_dyn_buf_append_string(db, ":");
			as_endpoint_list_info(endpoint_list, db);
			cf_dyn_buf_append_string(db, ";");
			break;
		}

		fabric_node_release(node);
	}
}

bool
as_fabric_is_published_endpoint_list(const as_endpoint_list *list)
{
	return as_endpoint_lists_are_equal(g_published_endpoint_list, list);
}

// Used by heartbeat subsystem only, for duplicate node-id detection.
as_endpoint_list *
as_fabric_hb_plugin_get_endpoint_list(as_hb_plugin_node_data *plugin_data)
{
	return (plugin_data && plugin_data->data_size != 0) ?
			(as_endpoint_list *)plugin_data->data : NULL;
}

void
as_fabric_rate_capture(fabric_rate *rate)
{
	cf_mutex_lock(&g_fabric.node_hash_lock);
	cf_rchash_reduce(g_fabric.node_hash, fabric_rate_node_reduce_fn, rate);
	cf_mutex_unlock(&g_fabric.node_hash_lock);
}

void
as_fabric_dump(bool verbose)
{
	node_list nl;
	fabric_get_node_list(&nl);

	cf_info(AS_FABRIC, " Fabric Dump: nodes known %d", nl.count);

	for (uint32_t i = 0; i < nl.count; i++) {
		if (nl.nodes[i] == g_config.self_node) {
			cf_info(AS_FABRIC, "\tnode %lx is self", nl.nodes[i]);
			continue;
		}

		fabric_node *node = fabric_node_get(nl.nodes[i]);

		if (! node) {
			cf_info(AS_FABRIC, "\tnode %lx not found in hash although reported available", nl.nodes[i]);
			continue;
		}

		cf_mutex_lock(&node->fc_hash_lock);
		cf_info(AS_FABRIC, "\tnode %lx fds {via_connect={h=%d m=%d l=%d} all=%d} live %d q {h=%d m=%d l=%d}",
				node->node_id,
				node->connect_count[AS_FABRIC_CHANNEL_CTRL],
				node->connect_count[AS_FABRIC_CHANNEL_RW],
				node->connect_count[AS_FABRIC_CHANNEL_BULK],
				cf_shash_get_size(node->fc_hash), node->live,
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_CTRL]),
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_RW]),
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_BULK]));
		cf_mutex_unlock(&node->fc_hash_lock);

		fabric_node_release(node); // node_get
	}
}


//==========================================================
// Support functions.
//

static void
send_entry_insert(send_entry **se_pp, send_entry *se)
{
	while (*se_pp && se->count > (*se_pp)->count) {
		se_pp = &(*se_pp)->next;
	}

	se->next = *se_pp;
	*se_pp = se;
}

// Get addresses to publish as serv config. Expand "any" addresses.
static void
fabric_published_serv_cfg_fill(const cf_serv_cfg *bind_cfg,
		cf_serv_cfg *published_cfg, bool ipv4_only)
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
				cf_warning(AS_FABRIC, "error getting all interface addresses");
				n_all_addrs = 0;
			}

			for (int j = 0; j < n_all_addrs; j++) {
				// Skip local address if any is specified.
				if (cf_ip_addr_is_local(&all_addrs[j]) ||
						(ipv4_only && ! cf_ip_addr_is_legacy(&all_addrs[j]))) {
					continue;
				}

				cf_ip_addr_copy(&all_addrs[j], &sock_cfg.addr);

				if (cf_serv_cfg_add_sock_cfg(published_cfg, &sock_cfg)) {
					cf_crash(AS_FABRIC, "error initializing published address list");
				}
			}
		}
		else {
			if (ipv4_only && ! cf_ip_addr_is_legacy(&bind_cfg->cfgs[i].addr)) {
				continue;
			}

			if (cf_serv_cfg_add_sock_cfg(published_cfg, &sock_cfg)) {
				cf_crash(AS_FABRIC, "error initializing published address list");
			}
		}
	}
}

// Refresh the fabric published endpoint list.
// Return true on success.
static bool
fabric_published_endpoints_refresh()
{
	if (g_published_endpoint_list &&
			g_published_endpoint_list_ipv4_only == cf_ip_addr_legacy_only()) {
		return true;
	}

	// The global flag has changed, refresh the published address list.
	if (g_published_endpoint_list) {
		// Free the obsolete list.
		cf_free(g_published_endpoint_list);
	}

	cf_serv_cfg published_cfg;
	fabric_published_serv_cfg_fill(&g_fabric_bind, &published_cfg,
			g_published_endpoint_list_ipv4_only);

	g_published_endpoint_list = as_endpoint_list_from_serv_cfg(&published_cfg);
	cf_assert(g_published_endpoint_list, AS_FABRIC, "error initializing mesh published address list");

	g_published_endpoint_list_ipv4_only = cf_ip_addr_legacy_only();

	if (g_published_endpoint_list->n_endpoints == 0) {
		if (g_published_endpoint_list_ipv4_only) {
			cf_warning(AS_FABRIC, "no IPv4 addresses configured for fabric");
		}
		else {
			cf_warning(AS_FABRIC, "no addresses configured for fabric");
		}

		return false;
	}

	char endpoint_list_str[512];
	as_endpoint_list_to_string(g_published_endpoint_list, endpoint_list_str,
			sizeof(endpoint_list_str));

	cf_info(AS_FABRIC, "updated fabric published address list to {%s}", endpoint_list_str);

	return true;
}


//==========================================================
// fabric_node
//

static fabric_node *
fabric_node_create(cf_node node_id)
{
	size_t size = sizeof(fabric_node) +
			(sizeof(uint8_t) * g_config.n_fabric_send_threads);
	fabric_node *node = cf_rc_alloc(size);

	memset(node, 0, size);

	node->node_id = node_id;
	node->live = true;

	cf_mutex_init(&node->send_idle_fc_queue_lock);

	for (int i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		cf_queue_init(&node->send_idle_fc_queue[i], sizeof(fabric_connection *),
				CF_QUEUE_ALLOCSZ, false);

		cf_queue_init(&node->send_queue[i], sizeof(msg *), CF_QUEUE_ALLOCSZ,
				true);
	}

	cf_mutex_init(&node->connect_lock);
	cf_mutex_init(&node->fc_hash_lock);

	node->fc_hash = cf_shash_create(cf_shash_fn_ptr,
			sizeof(fabric_connection *), 0, 32, 0);

	cf_detail(AS_FABRIC, "fabric_node_create(%lx) node %p", node_id, node);

	return node;
}

static fabric_node *
fabric_node_get(cf_node node_id)
{
	fabric_node *node;

	cf_mutex_lock(&g_fabric.node_hash_lock);
	int rv = cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node);
	cf_mutex_unlock(&g_fabric.node_hash_lock);

	if (rv != CF_RCHASH_OK) {
		return NULL;
	}

	return node;
}

static fabric_node *
fabric_node_get_or_create(cf_node node_id)
{
	fabric_node *node;

	cf_mutex_lock(&g_fabric.node_hash_lock);

	if (cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node) == CF_RCHASH_OK) {
		cf_mutex_unlock(&g_fabric.node_hash_lock);

		fabric_node_connect_all(node);

		return node;
	}

	node = fabric_node_create(node_id);

	if (cf_rchash_put_unique(g_fabric.node_hash, &node_id, sizeof(cf_node),
			node) != CF_RCHASH_OK) {
		cf_crash(AS_FABRIC, "fabric_node_get_or_create(%lx)", node_id);
	}

	fabric_node_reserve(node); // for return

	cf_mutex_unlock(&g_fabric.node_hash_lock);

	fabric_node_connect_all(node);

	return node;
}

static fabric_node *
fabric_node_pop(cf_node node_id)
{
	fabric_node *node = NULL;

	cf_mutex_lock(&g_fabric.node_hash_lock);

	if (cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node) == CF_RCHASH_OK) {
		if (cf_rchash_delete(g_fabric.node_hash, &node_id, sizeof(node_id)) !=
				CF_RCHASH_OK) {
			cf_crash(AS_FABRIC, "fabric_node_pop(%lx)", node_id);
		}
	}

	cf_mutex_unlock(&g_fabric.node_hash_lock);

	return node;
}

static int
fabric_node_disconnect_reduce_fn(const void *key, void *data, void *udata)
{
	fabric_connection *fc = *(fabric_connection **)key;

	cf_assert(fc, AS_FABRIC, "fc == NULL, don't put NULLs into fc_hash");
	cf_socket_shutdown(&fc->sock);
	fabric_connection_release(fc); // for delete from node->fc_hash

	return CF_SHASH_REDUCE_DELETE;
}

static void
fabric_node_disconnect(cf_node node_id)
{
	fabric_node *node = fabric_node_pop(node_id);

	if (! node) {
		cf_warning(AS_FABRIC, "fabric_node_disconnect(%lx) not connected", node_id);
		return;
	}

	cf_info(AS_FABRIC, "fabric_node_disconnect(%lx)", node_id);

	cf_mutex_lock(&node->fc_hash_lock);

	node->live = false;
	// Clean up all fc's attached to this node.
	cf_shash_reduce(node->fc_hash, fabric_node_disconnect_reduce_fn, NULL);

	cf_mutex_unlock(&node->fc_hash_lock);

	cf_mutex_lock(&node->send_idle_fc_queue_lock);

	for (int i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		while (true) {
			fabric_connection *fc;

			int rv = cf_queue_pop(&node->send_idle_fc_queue[i], &fc,
					CF_QUEUE_NOWAIT);

			if (rv != CF_QUEUE_OK) {
				break;
			}

			fabric_connection_send_unassign(fc);
			fabric_connection_release(fc);
		}
	}

	cf_mutex_unlock(&node->send_idle_fc_queue_lock);

	fabric_node_release(node); // from fabric_node_pop()
}

static fabric_connection *
fabric_node_connect(fabric_node *node, uint32_t ch)
{
	cf_detail(AS_FABRIC, "fabric_node_connect(%p, %u)", node, ch);

	cf_mutex_lock(&node->connect_lock);

	uint32_t fds = node->connect_count[ch] + 1;

	if (fds > g_fabric_connect_limit[ch]) {
		cf_mutex_unlock(&node->connect_lock);
		return NULL;
	}

	cf_socket sock;
	cf_sock_addr addr;
	size_t endpoint_list_capacity = 1024;
	int tries_remaining = 3;

	while (tries_remaining--) {
		uint8_t endpoint_list_mem[endpoint_list_capacity];
		as_endpoint_list *endpoint_list = (as_endpoint_list *)endpoint_list_mem;

		if (fabric_endpoint_list_get(node->node_id, endpoint_list,
				&endpoint_list_capacity)) {
			char endpoint_list_str[1024];

			as_endpoint_list_to_string(endpoint_list, endpoint_list_str,
					sizeof(endpoint_list_str));
			cf_detail(AS_FABRIC, "fabric_node_connect(%p, %u) node_id %lx with endpoints {%s}", node, ch, node->node_id, endpoint_list_str);

			// Initiate connect to the remote endpoint.
			const as_endpoint *connected_endpoint = as_endpoint_connect_any(
					endpoint_list, fabric_connect_endpoint_filter, NULL, 0,
					&sock);

			if (! connected_endpoint) {
				cf_detail(AS_FABRIC, "fabric_node_connect(%p, %u) node_id %lx failed for endpoints {%s}", node, ch, node->node_id, endpoint_list_str);
				cf_mutex_unlock(&node->connect_lock);
				return NULL;
			}

			as_endpoint_to_sock_addr(connected_endpoint, &addr);

			if (as_endpoint_capability_is_supported(connected_endpoint,
					AS_ENDPOINT_TLS_MASK)) {
				tls_socket_prepare_client(g_fabric_tls, &sock);
			}

			break; // read success
		}

		if (errno == ENOENT) {
			// No entry present for this node in heartbeat.
			cf_detail(AS_FABRIC, "fabric_node_connect(%p, %u) unknown remote node %lx", node, ch, node->node_id);
			cf_mutex_unlock(&node->connect_lock);
			return NULL;
		}

		// The list capacity was not enough. Retry with suggested list size.
	}

	if (tries_remaining < 0) {
		cf_warning(AS_FABRIC,"fabric_node_connect(%p, %u) List get error for remote node %lx", node, ch, node->node_id);
		cf_mutex_unlock(&node->connect_lock);
		return NULL;
	}

	msg *m = as_fabric_msg_get(M_TYPE_FABRIC);

	cf_atomic64_incr(&g_stats.fabric_connections_opened);
	as_health_add_node_counter(node->node_id, AS_HEALTH_NODE_FABRIC_FDS);

	msg_set_uint64(m, FS_FIELD_NODE, g_config.self_node);
	msg_set_uint32(m, FS_CHANNEL, ch);
	m->benchmark_time = g_config.fabric_benchmarks_enabled ? cf_getns() : 0;

	fabric_connection *fc = fabric_connection_create(&sock, &addr);

	fc->s_msg_in_progress = m;
	fc->started_via_connect = true;
	fc->pool = &g_fabric.recv_pool[ch];

	if (! fabric_node_add_connection(node, fc)) {
		fabric_connection_release(fc);
		cf_mutex_unlock(&node->connect_lock);
		return NULL;
	}

	node->connect_count[ch]++;
	node->connect_full = fabric_node_is_connect_full(node);

	cf_mutex_unlock(&node->connect_lock);

	return fc;
}

static int
fabric_node_send(fabric_node *node, msg *m, as_fabric_channel channel)
{
	if (! node || ! node->live) {
		return AS_FABRIC_ERR_NO_NODE;
	}

	while (true) {
		// Sync with fabric_connection_process_writable() to avoid non-empty
		// send_queue with every fc being in send_idle_fc_queue.
		cf_mutex_lock(&node->send_idle_fc_queue_lock);

		fabric_connection *fc;
		int rv = cf_queue_pop(&node->send_idle_fc_queue[(int)channel], &fc,
				CF_QUEUE_NOWAIT);

		if (rv != CF_QUEUE_OK) {
			cf_queue_push(&node->send_queue[(int)channel], &m);
			cf_mutex_unlock(&node->send_idle_fc_queue_lock);

			if (! node->connect_full) {
				fabric_node_connect_all(node);
			}

			break;
		}

		cf_mutex_unlock(&node->send_idle_fc_queue_lock);

		if ((! cf_socket_exists(&fc->sock)) || fc->failed) {
			fabric_connection_send_unassign(fc);
			fabric_connection_release(fc); // send_idle_fc_queue
			continue;
		}

		fc->s_msg_in_progress = m;

		// Wake up.
		if (fc->send_ptr) {
			fabric_connection_send_rearm(fc); // takes fc ref
		}
		else {
			fabric_connection_send_assign(fc); // takes fc ref
		}

		break;
	}

	return AS_FABRIC_SUCCESS;
}

static void
fabric_node_connect_all(fabric_node *node)
{
	if (! node->live) {
		return;
	}

	for (uint32_t ch = 0; ch < AS_FABRIC_N_CHANNELS; ch++) {
		uint32_t n = g_fabric_connect_limit[ch] - node->connect_count[ch];

		for (uint32_t i = 0; i < n; i++) {
			fabric_connection *fc = fabric_node_connect(node, ch);

			if (! fc) {
				break;
			}

			// TLS connections are one-way. Outgoing connections are for
			// outgoing data.
			if (fc->sock.state == CF_SOCKET_STATE_NON_TLS) {
				fabric_recv_thread_pool_add_fc(&g_fabric.recv_pool[ch], fc);
				cf_detail(AS_FABRIC, "{%16lX, %u} activated", fabric_connection_get_id(fc), fc->sock.fd);

				if (channel_nagle[ch]) {
					fc->s_cork_bypass = true;
					cf_socket_enable_nagle(&fc->sock);
				}
			}
			else {
				fc->s_cork_bypass = true;
			}

			// Takes the remaining ref for send_poll and idle queue.
			fabric_connection_send_assign(fc);
		}
	}
}

static void
fabric_node_destructor(void *pnode)
{
	fabric_node *node = (fabric_node *)pnode;
	cf_detail(AS_FABRIC, "fabric_node_destructor(%p)", node);

	for (int i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		// send_idle_fc_queue section.
		cf_assert(cf_queue_sz(&node->send_idle_fc_queue[i]) == 0, AS_FABRIC, "send_idle_fc_queue not empty as expected");
		cf_queue_destroy(&node->send_idle_fc_queue[i]);

		// send_queue section.
		while (true) {
			msg *m;

			if (cf_queue_pop(&node->send_queue[i], &m, CF_QUEUE_NOWAIT) !=
					CF_QUEUE_OK) {
				break;
			}

			as_fabric_msg_put(m);
		}

		cf_queue_destroy(&node->send_queue[i]);
	}

	cf_mutex_destroy(&node->send_idle_fc_queue_lock);

	// connection_hash section.
	cf_assert(cf_shash_get_size(node->fc_hash) == 0, AS_FABRIC, "fc_hash not empty as expected");
	cf_shash_destroy(node->fc_hash);

	cf_mutex_destroy(&node->fc_hash_lock);
}

inline static void
fabric_node_reserve(fabric_node *node) {
	cf_rc_reserve(node);
}

inline static void
fabric_node_release(fabric_node *node)
{
	if (cf_rc_release(node) == 0) {
		fabric_node_destructor(node);
		cf_rc_free(node);
	}
}

static bool
fabric_node_add_connection(fabric_node *node, fabric_connection *fc)
{
	cf_mutex_lock(&node->fc_hash_lock);

	if (! node->live) {
		cf_mutex_unlock(&node->fc_hash_lock);
		return false;
	}

	fabric_node_reserve(node);
	fc->node = node;

	fabric_connection_set_keepalive_options(fc);
	fabric_connection_reserve(fc); // for put into node->fc_hash

	uint8_t value = 0;
	int rv = cf_shash_put_unique(node->fc_hash, &fc, &value);

	cf_assert(rv == CF_SHASH_OK, AS_FABRIC, "fabric_node_add_connection(%p, %p) failed to add with rv %d", node, fc, rv);

	cf_mutex_unlock(&node->fc_hash_lock);

	return true;
}

static uint8_t
fabric_node_find_min_send_count(const fabric_node *node)
{
	uint8_t min = node->send_counts[0];

	for (uint32_t i = 1; i < g_config.n_fabric_send_threads; i++) {
		if (node->send_counts[i] < min) {
			min = node->send_counts[i];
		}
	}

	return min;
}

static bool
fabric_node_is_connect_full(const fabric_node *node)
{
	for (int ch = 0; ch < AS_FABRIC_N_CHANNELS; ch++) {
		if (node->connect_count[ch] < g_fabric_connect_limit[ch]) {
			return false;
		}
	}

	return true;
}


static int
fabric_get_node_list_fn(const void *key, uint32_t keylen, void *data,
		void *udata)
{
	node_list *nl = (node_list *)udata;

	if (nl->count == AS_CLUSTER_SZ) {
		return 0;
	}

	nl->nodes[nl->count] = *(const cf_node *)key;
	nl->count++;

	return 0;
}

// Get a list of all the nodes - use a dynamic array, which requires inline.
static uint32_t
fabric_get_node_list(node_list *nl)
{
	nl->count = 1;
	nl->nodes[0] = g_config.self_node;

	cf_mutex_lock(&g_fabric.node_hash_lock);
	cf_rchash_reduce(g_fabric.node_hash, fabric_get_node_list_fn, nl);
	cf_mutex_unlock(&g_fabric.node_hash_lock);

	return nl->count;
}


//==========================================================
// fabric_connection
//

fabric_connection *
fabric_connection_create(cf_socket *sock, cf_sock_addr *peer)
{
	fabric_connection *fc = cf_rc_alloc(sizeof(fabric_connection));

	memset(fc, 0, sizeof(fabric_connection));

	cf_socket_copy(sock, &fc->sock);
	cf_sock_addr_copy(peer, &fc->peer);

	fc->r_type = M_TYPE_FABRIC;

	return fc;
}

static bool
fabric_connection_accept_tls(fabric_connection *fc)
{
	int32_t tls_ev = tls_socket_accept(&fc->sock);

	if (tls_ev == EPOLLERR) {
		cf_warning(AS_FABRIC, "fabric TLS server handshake with %s failed", cf_sock_addr_print(&fc->peer));
		return false;
	}

	if (tls_ev == 0) {
		tls_socket_must_not_have_data(&fc->sock, "fabric server handshake");
		tls_ev = EPOLLIN;
	}

	cf_poll_modify_socket(g_accept_poll, &fc->sock,
			tls_ev | EPOLLERR | EPOLLHUP | EPOLLRDHUP, fc);
	return true;
}

static bool
fabric_connection_connect_tls(fabric_connection *fc)
{
	int32_t tls_ev = tls_socket_connect(&fc->sock);

	if (tls_ev == EPOLLERR) {
		cf_warning(AS_FABRIC, "fabric TLS client handshake with %s failed", cf_sock_addr_print(&fc->peer));
		return false;
	}

	if (tls_ev == 0) {
		tls_socket_must_not_have_data(&fc->sock, "fabric client handshake");
		tls_ev = EPOLLOUT;
	}

	cf_poll_modify_socket(fc->send_ptr->poll, &fc->sock,
			tls_ev | DEFAULT_EVENTS, fc);
	return true;
}

inline static void
fabric_connection_reserve(fabric_connection *fc)
{
	cf_rc_reserve(fc);
}

static void
fabric_connection_release(fabric_connection *fc)
{
	if (cf_rc_release(fc) == 0) {
		if (fc->s_msg_in_progress) {
			// First message (s_count == 0) is initial M_TYPE_FABRIC message
			// and does not need to be saved.
			if (! fc->started_via_connect || fc->s_count != 0) {
				cf_queue_push(&fc->node->send_queue[fc->pool->pool_id],
						&fc->s_msg_in_progress);
			}
			else {
				as_fabric_msg_put(fc->s_msg_in_progress);
			}
		}

		if (fc->node) {
			fabric_node_release(fc->node);
			fc->node = NULL;
		}
		else {
			cf_detail(AS_FABRIC, "releasing fc %p not attached to a node", fc);
		}

		cf_socket_close(&fc->sock);
		cf_socket_term(&fc->sock);
		cf_atomic64_incr(&g_stats.fabric_connections_closed);

		cf_free(fc->r_bigbuf);
		cf_rc_free(fc);
	}
}

inline static cf_node
fabric_connection_get_id(const fabric_connection *fc)
{
	if (fc->node) {
		return fc->node->node_id;
	}

	return 0;
}

inline static void
fabric_connection_cork(fabric_connection *fc)
{
	if (fc->s_cork == 1 || fc->s_cork_bypass) {
		return;
	}

	fc->s_cork = 1;
	cf_socket_set_cork(&fc->sock, fc->s_cork);
}

inline static void
fabric_connection_uncork(fabric_connection *fc)
{
	if (fc->s_cork == 0 || fc->s_cork_bypass) {
		return;
	}

	fc->s_cork = 0;
	cf_socket_set_cork(&fc->sock, fc->s_cork);
}

// epoll takes the reference of fc.
static void
fabric_connection_send_assign(fabric_connection *fc)
{
	cf_mutex_lock(&g_fabric.send_lock);

	send_entry **pp = &g_fabric.send_head;
	uint8_t min = fabric_node_find_min_send_count(fc->node);

	while (true) {
		uint32_t send_id = (*pp)->id;

		if (fc->node->send_counts[send_id] == min) {
			break;
		}

		cf_assert((*pp)->next, AS_FABRIC, "fabric_connection_send_assign() invalid send_count state");

		pp = &(*pp)->next;
	}

	send_entry *se = *pp;

	se->count++;
	fc->node->send_counts[se->id]++;

	if (se->next && se->next->count < se->count) {
		*pp = se->next;
		send_entry_insert(pp, se);
	}

	fc->send_ptr = se;

	cf_mutex_unlock(&g_fabric.send_lock);

	cf_poll_add_socket(se->poll, &fc->sock, EPOLLOUT | DEFAULT_EVENTS, fc);
}

static void
fabric_connection_send_unassign(fabric_connection *fc)
{
	cf_mutex_lock(&g_fabric.send_lock);

	if (! fc->send_ptr) {
		cf_mutex_unlock(&g_fabric.send_lock);
		return;
	}

	send_entry **pp = &g_fabric.send_head;
	send_entry *se = fc->send_ptr;

	while (*pp != se) {
		cf_assert((*pp)->next, AS_FABRIC, "fabric_connection_send_unassign() invalid send_count state");

		pp = &(*pp)->next;
	}

	cf_assert(se->count != 0 || fc->node->send_counts[se->id] != 0, AS_FABRIC, "invalid send_count accounting se %p id %u count %u node send_count %u",
			se, se->id, se->count, fc->node->send_counts[se->id]);

	se->count--;
	fc->node->send_counts[se->id]--;

	*pp = se->next;
	send_entry_insert(&g_fabric.send_head, se);

	fc->send_ptr = NULL;

	cf_mutex_unlock(&g_fabric.send_lock);
}

inline static void
fabric_connection_recv_rearm(fabric_connection *fc)
{
	fc->r_rearm_count++;
	cf_poll_modify_socket(fc->pool->poll, &fc->sock,
			EPOLLIN | DEFAULT_EVENTS, fc);
}

// epoll takes the reference of fc.
inline static void
fabric_connection_send_rearm(fabric_connection *fc)
{
	cf_poll_modify_socket(fc->send_ptr->poll, &fc->sock,
			EPOLLOUT | DEFAULT_EVENTS, fc);
}

static void
fabric_connection_disconnect(fabric_connection *fc)
{
	fc->failed = true;
	cf_socket_shutdown(&fc->sock);

	fabric_node *node = fc->node;

	if (! node) {
		return;
	}

	cf_mutex_lock(&node->fc_hash_lock);

	if (cf_shash_delete(node->fc_hash, &fc) != CF_SHASH_OK) {
		cf_detail(AS_FABRIC, "fc %p is not in (node %p)->fc_hash", fc, node);
		cf_mutex_unlock(&node->fc_hash_lock);
		return;
	}

	cf_mutex_unlock(&node->fc_hash_lock);

	if (fc->started_via_connect) {
		cf_mutex_lock(&node->connect_lock);

		cf_atomic32_decr(&node->connect_count[fc->pool->pool_id]);
		node->connect_full = false;

		cf_mutex_unlock(&node->connect_lock);
	}

	cf_mutex_lock(&node->send_idle_fc_queue_lock);

	if (cf_queue_delete(&node->send_idle_fc_queue[fc->pool->pool_id], &fc,
			true) == CF_QUEUE_OK) {
		fabric_connection_send_unassign(fc);
		fabric_connection_release(fc); // for delete from send_idle_fc_queue
	}

	cf_mutex_unlock(&node->send_idle_fc_queue_lock);

	cf_detail(AS_FABRIC, "fabric_connection_disconnect(%p) {pool=%u id=%lx fd=%u}",
			fc, fc->pool ? fc->pool->pool_id : 0,
			node ? node->node_id : (cf_node)0, fc->sock.fd);

	fabric_connection_release(fc); // for delete from node->fc_hash
}

static void
fabric_connection_set_keepalive_options(fabric_connection *fc)
{
	if (g_config.fabric_keepalive_enabled) {
		cf_socket_keep_alive(&fc->sock, g_config.fabric_keepalive_time,
				g_config.fabric_keepalive_intvl,
				g_config.fabric_keepalive_probes);
	}
}

static void
fabric_connection_reroute_msg(fabric_connection *fc)
{
	if (! fc->s_msg_in_progress) {
		return;
	}

	// Don't reroute initial M_TYPE_FABRIC message.
	if ((fc->started_via_connect && fc->s_count == 0) ||
			fabric_node_send(fc->node, fc->s_msg_in_progress,
					fc->pool->pool_id) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(fc->s_msg_in_progress);
	}

	fc->s_msg_in_progress = NULL;
}

static void
fabric_connection_incr_iov(fabric_connection *fc, uint32_t sz)
{
	while (sz != 0) {
		if (sz <= fc->s_iov->iov_len) {
			fc->s_iov->iov_base = (uint8_t *)fc->s_iov->iov_base + sz;
			fc->s_iov->iov_len -= sz;
			return;
		}

		cf_assert(fc->s_iov_count != 0, AS_PARTICLE, "fc->s_iov_count == 0");
		sz -= fc->s_iov->iov_len;
		fc->s_iov->iov_len = 0;
		fc->s_iov++;
		fc->s_iov_count--;
	}
}

static bool
fabric_connection_send_progress(fabric_connection *fc)
{
	if (fc->s_msg_sz == 0) { // new msg
		msg *m = fc->s_msg_in_progress;

		fc->s_iov = (struct iovec *)fc->s_buf;
		fc->s_iov_count = msg_to_iov_buf(m, fc->s_buf, sizeof(fc->s_buf),
				&fc->s_msg_sz);
		fc->s_sz = 0;

		if (m->benchmark_time != 0) {
			m->benchmark_time = histogram_insert_data_point(
					g_stats.fabric_send_init_hists[fc->pool->pool_id],
					m->benchmark_time);
		}
	}

	struct msghdr sendhdr = {
			.msg_iov = fc->s_iov,
			.msg_iovlen = fc->s_iov_count
	};

	int32_t send_sz = cf_socket_send_msg(&fc->sock, &sendhdr, 0);

	if (send_sz < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			return false;
		}

		send_sz = 0; // treat as sending 0
	}

	if (fc->s_msg_in_progress->benchmark_time != 0) {
		fc->s_msg_in_progress->benchmark_time = histogram_insert_data_point(
				g_stats.fabric_send_fragment_hists[fc->pool->pool_id],
				fc->s_msg_in_progress->benchmark_time);
	}

	fc->s_sz += send_sz;
	fc->s_bytes += send_sz;

	if (fc->s_sz == fc->s_msg_sz) { // complete send
		as_fabric_msg_put(fc->s_msg_in_progress);
		fc->s_msg_in_progress = NULL;
		fc->s_msg_sz = 0;
		fc->s_count++;
	}
	else { // partial send
		fabric_connection_incr_iov(fc, (uint32_t)send_sz);
	}

	return true;
}

// Must rearm or place into idle queue on success.
static bool
fabric_connection_process_writable(fabric_connection *fc)
{
	fabric_node *node = fc->node;
	uint32_t pool = fc->pool->pool_id;

	if (! fc->s_msg_in_progress) {
		// TODO - Change to load op when atomic API is ready.
		// Also should be rare or not even happen in x86_64.
		cf_warning(AS_FABRIC, "fc(%p)->s_msg_in_progress NULL on entry", fc);
		return false;
	}

	fabric_connection_cork(fc);

	while (true) {
		if (! fabric_connection_send_progress(fc)) {
			return false;
		}

		if (fc->s_msg_in_progress) {
			fabric_connection_send_rearm(fc);
			return true;
		}

		if (cf_queue_pop(&node->send_queue[pool], &fc->s_msg_in_progress,
				CF_QUEUE_NOWAIT) != CF_QUEUE_OK) {
			break;
		}
	}

	fabric_connection_uncork(fc);

	if (! fc->node->live || fc->failed) {
		return false;
	}

	// Try with bigger lock block to sync with as_fabric_send().
	cf_mutex_lock(&node->send_idle_fc_queue_lock);

	if (! fc->node->live || fc->failed) {
		cf_mutex_unlock(&node->send_idle_fc_queue_lock);
		return false;
	}

	if (cf_queue_pop(&node->send_queue[pool], &fc->s_msg_in_progress,
			CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		cf_queue_push(&node->send_idle_fc_queue[pool], &fc);
		cf_mutex_unlock(&node->send_idle_fc_queue_lock);
		return true;
	}

	cf_mutex_unlock(&node->send_idle_fc_queue_lock);

	fabric_connection_send_rearm(fc);

	return true;
}

// Return true on success.
static bool
fabric_connection_process_fabric_msg(fabric_connection *fc, const msg *m)
{
	cf_poll_delete_socket(g_accept_poll, &fc->sock);

	cf_node node_id;

	if (msg_get_uint64(m, FS_FIELD_NODE, &node_id) != 0) {
		cf_warning(AS_FABRIC, "process_fabric_msg: failed to read M_TYPE_FABRIC node");
		return false;
	}

	cf_detail(AS_FABRIC, "process_fabric_msg: M_TYPE_FABRIC from node %lx", node_id);

	fabric_node *node = fabric_node_get_or_create(node_id);

	if (! fabric_node_add_connection(node, fc)) {
		fabric_node_release(node); // from cf_rchash_get
		return false;
	}

	uint32_t pool_id = AS_FABRIC_N_CHANNELS; // illegal value

	msg_get_uint32(m, FS_CHANNEL, &pool_id);

	if (pool_id >= AS_FABRIC_N_CHANNELS) {
		fabric_node_release(node); // from cf_rchash_get
		return false;
	}

	fc->r_sz = 0;
	fc->r_buf_sz = 0;

	// fc->pool needs to be set before placing into send_idle_fc_queue.
	fabric_recv_thread_pool_add_fc(&g_fabric.recv_pool[pool_id], fc);

	// TLS connections are one-way. Incoming connections are for
	// incoming data.
	if (fc->sock.state == CF_SOCKET_STATE_NON_TLS) {
		if (channel_nagle[pool_id]) {
			fc->s_cork_bypass = true;
			cf_socket_enable_nagle(&fc->sock);
		}

		cf_mutex_lock(&node->send_idle_fc_queue_lock);

		if (node->live && ! fc->failed) {
			fabric_connection_reserve(fc); // for send poll & idleQ

			if (cf_queue_pop(&node->send_queue[pool_id], &fc->s_msg_in_progress,
					CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
				cf_queue_push(&node->send_idle_fc_queue[pool_id], &fc);
			}
			else {
				fabric_connection_send_assign(fc);
			}
		}

		cf_mutex_unlock(&node->send_idle_fc_queue_lock);
	}
	else {
		fc->s_cork_bypass = true;
	}

	fabric_node_release(node); // from cf_rchash_get
	fabric_connection_release(fc); // from g_accept_poll

	return true;
}

static bool
fabric_connection_read_fabric_msg(fabric_connection *fc)
{
	while (true) {
		int32_t recv_sz = cf_socket_recv(&fc->sock, fc->r_buf + fc->r_sz,
				sizeof(msg_hdr) + fc->r_buf_sz - fc->r_sz, 0);

		if (recv_sz < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_warning(AS_FABRIC, "fabric_connection_read_fabric_msg() recv_sz %d errno %d %s", recv_sz, errno, cf_strerror(errno));
				return false;
			}

			break;
		}

		if (recv_sz == 0) {
			cf_detail(AS_FABRIC, "fabric_connection_read_fabric_msg(%p) recv_sz 0 r_msg_sz %u", fc, fc->r_buf_sz);
			return false;
		}

		fc->r_sz += recv_sz;
		fc->r_bytes += recv_sz;

		if (fc->r_buf_sz == 0) {
			if (fc->r_sz < sizeof(msg_hdr)) {
				tls_socket_must_not_have_data(&fc->sock, "partial fabric read");
				break;
			}

			msg_parse_hdr(&fc->r_buf_sz, &fc->r_type, fc->r_buf, fc->r_sz);

			if (fc->r_buf_sz >= sizeof(fc->r_buf)) {
				cf_warning(AS_FABRIC, "r_msg_sz > sizeof(fc->r_membuf) %zu", sizeof(fc->r_buf));
				return false;
			}

			if (fc->r_buf_sz != 0) {
				continue;
			}
		}

		if (fc->r_sz < sizeof(msg_hdr) + fc->r_buf_sz) {
			tls_socket_must_not_have_data(&fc->sock, "partial fabric read");
			break;
		}

		tls_socket_must_not_have_data(&fc->sock, "full fabric read");

		if (fc->r_type != M_TYPE_FABRIC) {
			cf_warning(AS_FABRIC, "fabric_connection_read_fabric_msg() expected type M_TYPE_FABRIC(%d) got type %d", M_TYPE_FABRIC, fc->r_type);
			return false;
		}

		msg *m = as_fabric_msg_get(M_TYPE_FABRIC);

		if (! msg_parse_fields(m, fc->r_buf + sizeof(msg_hdr), fc->r_buf_sz)) {
			cf_warning(AS_FABRIC, "msg_parse failed for fc %p", fc);
			as_fabric_msg_put(m);
			return false;
		}

		bool ret = fabric_connection_process_fabric_msg(fc, m);

		as_fabric_msg_put(m);

		return ret;
	}

	return true;
}

// Return true on success.
// Must have re-armed on success if do_rearm == true.
static bool
fabric_connection_process_msg(fabric_connection *fc, bool do_rearm)
{
	cf_assert(fc->node, AS_FABRIC, "process_msg: no node assigned");

	uint32_t read_ahead_sz = fc->r_sz - fc->r_buf_sz;
	uint32_t mem_sz = fc->r_buf_sz;
	uint8_t *p_bigbuf = fc->r_bigbuf; // malloc handoff

	fc->r_bigbuf = NULL;

	if (p_bigbuf) {
		fc->r_sz = read_ahead_sz;
		mem_sz = 0;
	}

	while (read_ahead_sz > sizeof(msg_hdr)) {
		read_ahead_sz -= sizeof(msg_hdr);

		uint32_t *ptr = (uint32_t *)(fc->r_buf + mem_sz);
		uint32_t sz = cf_swap_from_be32(*ptr);

		if (read_ahead_sz < sz) {
			break;
		}

		mem_sz += sizeof(msg_hdr) + sz;
		read_ahead_sz -= sz;
	}

	uint8_t stack_mem[mem_sz + 1]; // +1 to account for mem_sz == 0

	memcpy(stack_mem, fc->r_buf, mem_sz);
	fc->r_sz -= mem_sz;
	memmove(fc->r_buf, fc->r_buf + mem_sz, fc->r_sz);

	uint8_t *buf_ptr = p_bigbuf;

	if (! buf_ptr) {
		buf_ptr = stack_mem;
		mem_sz -= fc->r_buf_sz;
	}

	buf_ptr += sizeof(msg_hdr);

	// Save some state for after re-arm.
	cf_node node = fc->node->node_id;
	uint64_t bt = fc->benchmark_time;
	uint32_t ch = fc->pool->pool_id;
	msg_type type = fc->r_type;
	uint32_t msg_sz = fc->r_buf_sz - sizeof(msg_hdr);

	fc->r_buf_sz = 0;

	if (do_rearm) {
		// Re-arm for next message (possibly handled in another thread).
		fabric_connection_recv_rearm(fc); // do not use fc after this point
	}

	while (true) {
		if (! msg_type_is_valid(type)) {
			cf_warning(AS_FABRIC, "failed to create message for type %u (max %u)", type, M_TYPE_MAX);
			cf_free(p_bigbuf);
			return false;
		}

		msg *m = as_fabric_msg_get(type);

		if (! msg_parse_fields(m, buf_ptr, msg_sz)) {
			cf_warning(AS_FABRIC, "msg_parse_fields failed for fc %p", fc);
			as_fabric_msg_put(m);
			cf_free(p_bigbuf);
			return false;
		}

		if (g_fabric.msg_cb[m->type]) {
			(g_fabric.msg_cb[m->type])(node, m, g_fabric.msg_udata[m->type]);

			if (bt != 0) {
				histogram_insert_data_point(g_stats.fabric_recv_cb_hists[ch],
						bt);
			}
		}
		else {
			cf_warning(AS_FABRIC, "process_msg: could not deliver message type %d", m->type);
			as_fabric_msg_put(m);
		}

		if (p_bigbuf) {
			cf_free(p_bigbuf);
			p_bigbuf = NULL;
			buf_ptr = stack_mem;
		}
		else {
			buf_ptr += msg_sz;
		}

		if (mem_sz < sizeof(msg_hdr)) {
			cf_assert(mem_sz == 0, AS_FABRIC, "process_msg: stack_sz left %u != 0", mem_sz);
			break;
		}

		msg_parse_hdr(&msg_sz, &type, buf_ptr, mem_sz);
		buf_ptr += sizeof(msg_hdr);
		mem_sz -= sizeof(msg_hdr) + msg_sz;
	}

	return true;
}

// Return true on success.
// Must have re-armed on success.
static bool
fabric_connection_process_readable(fabric_connection *fc)
{
	size_t recv_all = 0;

	while (true) {
		int32_t recv_sz;

		if (! fc->r_bigbuf) {
			recv_sz = cf_socket_recv(&fc->sock, fc->r_buf + fc->r_sz,
					sizeof(fc->r_buf) - fc->r_sz, 0);
		}
		else {
			struct iovec iov[2] = {
					{
							.iov_base = fc->r_bigbuf + fc->r_sz,
							.iov_len = fc->r_buf_sz - fc->r_sz
					},
					{
							.iov_base = fc->r_buf,
							.iov_len = sizeof(fc->r_buf) // read ahead
					}
			};

			struct msghdr recvhdr = {
					.msg_iov = iov,
					.msg_iovlen = 2
			};

			recv_sz = cf_socket_recv_msg(&fc->sock, &recvhdr, 0);
		}

		if (recv_sz < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_warning(AS_FABRIC, "fabric_connection_process_readable() recv_sz %d msg_sz %u errno %d %s", recv_sz, fc->r_buf_sz, errno, cf_strerror(errno));
				return false;
			}

			break;
		}
		else if (recv_sz == 0) {
			cf_detail(AS_FABRIC, "fabric_connection_process_readable(%p) recv_sz 0 msg_sz %u", fc, fc->r_buf_sz);
			return false;
		}

		fc->r_sz += recv_sz;
		fc->r_bytes += recv_sz;
		recv_all += recv_sz;

		if (fc->r_buf_sz == 0) {
			fc->benchmark_time = g_config.fabric_benchmarks_enabled ?
					cf_getns() : 0;

			if (fc->r_sz < sizeof(msg_hdr)) {
				break;
			}

			msg_parse_hdr(&fc->r_buf_sz, &fc->r_type, fc->r_buf, fc->r_sz);
			fc->r_buf_sz += sizeof(msg_hdr);

			if (fc->r_buf_sz > sizeof(fc->r_buf)) {
				if (fc->r_buf_sz > FABRIC_BUFFER_MAX_SZ) {
					cf_warning(AS_FABRIC, "fabric_connection_process_readable(%p) invalid msg_size %u remote 0x%lx", fc, fc->r_buf_sz, fabric_connection_get_id(fc));
					return false;
				}

				fc->r_bigbuf = cf_malloc(fc->r_buf_sz);
				memcpy(fc->r_bigbuf, fc->r_buf, fc->r_sz); // fc->r_sz < r_msg_sz here
			}
		}

		if (fc->r_sz < fc->r_buf_sz) {
			if (fc->benchmark_time != 0) {
				fc->benchmark_time = histogram_insert_data_point(
						g_stats.fabric_recv_fragment_hists[fc->pool->pool_id],
						fc->benchmark_time);
			}

			break;
		}

		bool do_rearm =
				recv_all > (size_t)g_config.fabric_recv_rearm_threshold ||
				fc->r_buf_sz > g_config.fabric_recv_rearm_threshold;

		if (! fabric_connection_process_msg(fc, do_rearm)) {
			return false;
		}

		if (do_rearm) {
			// Already rearmed.
			return true;
		}
	}

	fabric_connection_recv_rearm(fc);
	return true;
}


//==========================================================
// fabric_recv_thread_pool
//

static void
fabric_recv_thread_pool_init(fabric_recv_thread_pool *pool, uint32_t size,
		uint32_t pool_id)
{
	cf_vector_init(&pool->threads, sizeof(cf_tid), size, 0);
	cf_poll_create(&pool->poll);
	pool->pool_id = pool_id;
}

// Called only at startup or under set-config lock. Caller has checked size.
static void
fabric_recv_thread_pool_set_size(fabric_recv_thread_pool *pool, uint32_t size)
{
	while (size < cf_vector_size(&pool->threads)) {
		cf_tid tid;

		cf_vector_pop(&pool->threads, &tid);
		cf_thread_cancel(tid);
	}

	while (size > cf_vector_size(&pool->threads)) {
		cf_tid tid = cf_thread_create_detached(run_fabric_recv, (void*)pool);

		cf_vector_append(&pool->threads, &tid);
	}
}

static void
fabric_recv_thread_pool_add_fc(fabric_recv_thread_pool *pool,
		fabric_connection *fc)
{
	fabric_connection_reserve(fc); // extra ref for poll
	fc->pool = pool;

	uint32_t recv_events = EPOLLIN | DEFAULT_EVENTS;

	cf_poll_add_socket(pool->poll, &fc->sock, recv_events, fc);
}


//==========================================================
// fabric_endpoint
//

// Get the endpoint list to connect to the remote node.
// Returns true on success where errno will be set to  ENOENT if there is no
// endpoint list could be obtained for this node and ENOMEM if the input
// endpoint_list_size is less than actual size. Var endpoint_list_size will be
// updated with the required capacity.
static bool
fabric_endpoint_list_get(cf_node nodeid, as_endpoint_list *endpoint_list,
		size_t *endpoint_list_size)
{
	as_hb_plugin_node_data plugin_data = {
			.data_capacity = *endpoint_list_size,
			.data = endpoint_list,
			.data_size = 0,
	};

	if (as_hb_plugin_data_get(nodeid, AS_HB_PLUGIN_FABRIC, &plugin_data, NULL,
			NULL) == 0) {
		return plugin_data.data_size != 0;
	}

	if (errno == ENOENT) {
		return false;
	}

	// Not enough allocated memory.
	*endpoint_list_size = plugin_data.data_size;

	return false;
}

// Filter out endpoints not matching this node's capabilities.
static bool
fabric_connect_endpoint_filter(const as_endpoint *endpoint, void *udata)
{
	if (cf_ip_addr_legacy_only() &&
			endpoint->addr_type == AS_ENDPOINT_ADDR_TYPE_IPv6) {
		return false;
	}

	// If we don't offer TLS, then we won't connect via TLS, either.
	if (g_config.tls_fabric.bind_port == 0 &&
			as_endpoint_capability_is_supported(endpoint,
					AS_ENDPOINT_TLS_MASK)) {
		return false;
	}

	return true;
}


//==========================================================
// Thread functions.
//

static void *
run_fabric_recv(void *arg)
{
	cf_thread_disable_cancel();

	fabric_recv_thread_pool *pool = (fabric_recv_thread_pool *)arg;
	static int worker_id_counter = 0;
	uint64_t worker_id = worker_id_counter++;
	cf_poll poll = pool->poll;

	cf_detail(AS_FABRIC, "run_fabric_recv() created index %lu", worker_id);

	while (true) {
		cf_thread_test_cancel();

		cf_poll_event events[FABRIC_EPOLL_RECV_EVENTS];
		int32_t n = cf_poll_wait(poll, events, FABRIC_EPOLL_RECV_EVENTS, -1);

		for (int32_t i = 0; i < n; i++) {
			fabric_connection *fc = events[i].data;

			if (fc->node && ! fc->node->live) {
				fabric_connection_disconnect(fc);
				fabric_connection_release(fc);
				continue;
			}

			// Handle remote close, socket errors.
			// Also triggered by call to cf_socket_shutdown(fc->sock), but only
			// first call.
			// Not triggered by cf_socket_close(fc->sock), which automatically
			// does EPOLL_CTL_DEL.
			if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
				cf_detail(AS_FABRIC, "%lu: epoll : error, will close: fc %p fd %d errno %d signal {err:%d, hup:%d, rdhup:%d}",
						worker_id,
						fc, CSFD(&fc->sock), errno,
						((events[i].events & EPOLLERR) ? 1 : 0),
						((events[i].events & EPOLLHUP) ? 1 : 0),
						((events[i].events & EPOLLRDHUP) ? 1 : 0));
				fabric_connection_disconnect(fc);
				fabric_connection_release(fc);
				continue;
			}

			cf_assert(events[i].events == EPOLLIN, AS_FABRIC, "epoll not setup correctly for %p", fc);
			uint32_t rearm_count = fc->r_rearm_count;

			if (! fabric_connection_process_readable(fc)) {
				fabric_connection_disconnect(fc);

				if (rearm_count == fc->r_rearm_count) {
					fabric_connection_release(fc);
				}

				continue;
			}
		}
	}

	return NULL;
}

static void *
run_fabric_send(void *arg)
{
	send_entry *se = (send_entry *)arg;
	cf_poll poll = se->poll;

	cf_detail(AS_FABRIC, "run_fabric_send() fd %d id %u", poll.fd, se->id);

	while (true) {
		cf_poll_event events[FABRIC_EPOLL_SEND_EVENTS];
		int32_t n = cf_poll_wait(poll, events, FABRIC_EPOLL_SEND_EVENTS, -1);

		for (int32_t i = 0; i < n; i++) {
			fabric_connection *fc = events[i].data;

			if (fc->node && ! fc->node->live) {
				fabric_connection_disconnect(fc);
				fabric_connection_send_unassign(fc);
				fabric_connection_release(fc);
				continue;
			}

			// Handle remote close, socket errors. Also triggered by call to
			// cf_socket_shutdown(fb->sock), but only first call. Not triggered
			// by cf_socket_close(fb->sock), which automatically EPOLL_CTL_DEL.
			if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
				cf_detail(AS_FABRIC, "epoll : error, will close: fc %p fd %d errno %d signal {err:%d, hup:%d, rdhup:%d}",
						fc, CSFD(&fc->sock), errno,
						((events[i].events & EPOLLERR) ? 1 : 0),
						((events[i].events & EPOLLHUP) ? 1 : 0),
						((events[i].events & EPOLLRDHUP) ? 1 : 0));
				fabric_connection_disconnect(fc);
				fabric_connection_send_unassign(fc);
				fabric_connection_reroute_msg(fc);
				fabric_connection_release(fc);
				continue;
			}

			if (tls_socket_needs_handshake(&fc->sock)) {
				if (! fabric_connection_connect_tls(fc)) {
					fabric_connection_disconnect(fc);
					fabric_connection_send_unassign(fc);
					fabric_connection_reroute_msg(fc);
					fabric_connection_release(fc);
				}

				continue;
			}

			cf_assert(events[i].events == EPOLLOUT, AS_FABRIC, "epoll not setup correctly for %p", fc);

			if (! fabric_connection_process_writable(fc)) {
				fabric_connection_disconnect(fc);
				fabric_connection_send_unassign(fc);
				fabric_connection_reroute_msg(fc);
				fabric_connection_release(fc);
				continue;
			}
		}
	}

	return 0;
}

static void *
run_fabric_accept(void *arg)
{
	cf_sockets sockset;

	if (cf_socket_init_server(&g_fabric_bind, &sockset) < 0) {
		cf_crash(AS_FABRIC, "Could not create fabric listener socket - check configuration");
	}

	cf_poll_create(&g_accept_poll);
	cf_poll_add_sockets(g_accept_poll, &sockset, EPOLLIN | EPOLLERR | EPOLLHUP);
	cf_socket_show_server(AS_FABRIC, "fabric", &sockset);

	while (true) {
		// Accept new connections on the service socket.
		cf_poll_event events[64];
		int32_t n = cf_poll_wait(g_accept_poll, events, 64, -1);

		for (int32_t i = 0; i < n; i++) {
			cf_socket *ssock = events[i].data;

			if (cf_sockets_has_socket(&sockset, ssock)) {
				cf_socket csock;
				cf_sock_addr sa;

				if (cf_socket_accept(ssock, &csock, &sa) < 0) {
					if (errno == EMFILE) {
						cf_warning(AS_FABRIC, "low on file descriptors");
						continue;
					}
					else {
						cf_crash(AS_FABRIC, "cf_socket_accept: %d %s", errno, cf_strerror(errno));
					}
				}

				cf_detail(AS_FABRIC, "fabric_accept: accepting new sock %d", CSFD(&csock));
				cf_atomic64_incr(&g_stats.fabric_connections_opened);

				fabric_connection *fc = fabric_connection_create(&csock, &sa);

				cf_sock_cfg *cfg = ssock->cfg;

				if (cfg->owner == CF_SOCK_OWNER_FABRIC_TLS) {
					tls_socket_prepare_server(g_fabric_tls, &fc->sock);
				}

				uint32_t events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
				cf_poll_add_socket(g_accept_poll, &fc->sock, events, fc);
			}
			else {
				fabric_connection *fc = events[i].data;

				if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
					fabric_connection_release(fc);
					continue;
				}

				if (tls_socket_needs_handshake(&fc->sock)) {
					if (! fabric_connection_accept_tls(fc)) {
						fabric_connection_release(fc);
					}

					continue;
				}

				if (! fabric_connection_read_fabric_msg(fc)) {
					fabric_connection_release(fc);
					continue;
				}
			}
		}
	}

	return 0;
}

static int
fabric_rate_node_reduce_fn(const void *key, uint32_t keylen, void *data,
		void *udata)
{
	fabric_node *node = (fabric_node *)data;
	fabric_rate *rate = (fabric_rate *)udata;

	cf_mutex_lock(&node->fc_hash_lock);
	cf_shash_reduce(node->fc_hash, fabric_rate_fc_reduce_fn, rate);
	cf_mutex_unlock(&node->fc_hash_lock);

	return 0;
}

static int
fabric_rate_fc_reduce_fn(const void *key, void *data, void *udata)
{
	fabric_connection *fc = *(fabric_connection **)key;
	fabric_rate *rate = (fabric_rate *)udata;

	if (! fc->pool) {
		return 0;
	}

	uint32_t pool_id = fc->pool->pool_id;
	uint64_t r_bytes = fc->r_bytes;
	uint64_t s_bytes = fc->s_bytes;

	rate->r_bytes[pool_id] += r_bytes - fc->r_bytes_last;
	rate->s_bytes[pool_id] += s_bytes - fc->s_bytes_last;

	fc->r_bytes_last = r_bytes;
	fc->s_bytes_last = s_bytes;

	return 0;
}


//==========================================================
// Heartbeat.
//

// Set the fabric advertised endpoints.
static void
fabric_hb_plugin_set_fn(msg *m)
{
	if (m->type == M_TYPE_HEARTBEAT_V2) {
		// In v1 and v2 fabric does not advertise its endpoints and they
		// do not support plugged in data.
		return;
	}

	if (! fabric_published_endpoints_refresh()) {
		cf_warning(AS_FABRIC, "No publish addresses found for fabric.");
		return;
	}

	size_t payload_size = 0;

	if (as_endpoint_list_sizeof(
			g_published_endpoint_list, &payload_size) != 0) {
		cf_crash(AS_FABRIC, "Error getting endpoint list size for published addresses.");
	}

	msg_set_buf(m, AS_HB_MSG_FABRIC_DATA, (uint8_t *)g_published_endpoint_list,
			payload_size, MSG_SET_COPY);
}

// Plugin function that parses succession list out of a heartbeat pulse message.
static void
fabric_hb_plugin_parse_data_fn(msg *m, cf_node source,
		as_hb_plugin_node_data *prev_plugin_data,
		as_hb_plugin_node_data *plugin_data)
{
	if (m->type == M_TYPE_HEARTBEAT_V2) {
		plugin_data->data_size = 0;
		return;
	}

	uint8_t *payload = NULL;
	size_t payload_size = 0;

	if (msg_get_buf(m, AS_HB_MSG_FABRIC_DATA, &payload, &payload_size,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_FABRIC, "Unable to read fabric published endpoint list from heartbeat from node %lx", source);
		return;
	}

	if (payload_size > plugin_data->data_capacity) {
		// Round up to nearest multiple of block size to prevent very frequent
		// reallocation.
		size_t data_capacity = ((payload_size + HB_PLUGIN_DATA_BLOCK_SIZE - 1) /
				HB_PLUGIN_DATA_BLOCK_SIZE) * HB_PLUGIN_DATA_BLOCK_SIZE;

		// Reallocate since we have outgrown existing capacity.
		plugin_data->data = cf_realloc(plugin_data->data, data_capacity);

		plugin_data->data_capacity = data_capacity;
	}

	plugin_data->data_size = payload_size;

	memcpy(plugin_data->data, payload, payload_size);
}

// Function is called when a new node created or destroyed on the heartbeat
// system.
// This will insert a new element in the hashtable that keeps track of all TCP
// connections.
static void
fabric_heartbeat_event(int nevents, as_hb_event_node *events, void *udata)
{
	if ((nevents < 1) || (nevents > AS_CLUSTER_SZ) || ! events) {
		cf_warning(AS_FABRIC, "fabric: received event count of %d", nevents);
		return;
	}

	for (int i = 0; i < nevents; i++) {
		switch (events[i].evt) {
		case AS_HB_NODE_ARRIVE: {
				fabric_node *node = fabric_node_get_or_create(events[i].nodeid);
				fabric_node_release(node); // for node_get_or_create()

				cf_info(AS_FABRIC, "fabric: node %lx arrived", events[i].nodeid);
			}
			break;
		case AS_HB_NODE_DEPART:
			cf_info(AS_FABRIC, "fabric: node %lx departed", events[i].nodeid);
			fabric_node_disconnect(events[i].nodeid);
			break;
		case AS_HB_NODE_ADJACENCY_CHANGED:
			// Not relevant to fabric.
			break;
		default:
			cf_warning(AS_FABRIC, "fabric: received unknown event type %d %lx", events[i].evt, events[i].nodeid);
			break;
		}
	}
}
