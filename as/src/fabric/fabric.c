/*
 * fabric.c
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
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
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_rchash.h"
#include "citrusleaf/cf_vector.h"

#include "fault.h"
#include "msg.h"
#include "node.h"
#include "shash.h"
#include "socket.h"
#include "tls.h"

#include "base/cfg.h"
#include "base/stats.h"
#include "fabric/endpoint.h"
#include "fabric/hb.h"


//==========================================================
// Typedefs & constants.
//

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

#define FS_MSG_SCRATCH_SIZE	128

#define DEFAULT_EVENTS (EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLONESHOT)

// Block size for allocating fabric hb plugin data.
#define HB_PLUGIN_DATA_BLOCK_SIZE	128

typedef struct fabric_recv_thread_pool_s {
	cf_vector			threads;
	cf_poll				poll;
	uint32_t			pool_id;
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

	cf_queue			msg_pool_queue[M_TYPE_MAX]; // a pool of reusable msgs
	cf_vector			fb_free;

	fabric_recv_thread_pool recv_pool[AS_FABRIC_N_CHANNELS];

	pthread_mutex_t		send_lock;
	send_entry			*sends;
	send_entry			*send_head;

	pthread_mutex_t		node_hash_lock;
	cf_rchash			*node_hash; // key is cf_node, value is (fabric_node *)
} fabric_state;

typedef struct fabric_buffer_s {
	uint8_t 		*buf;
	uint8_t			*progress;
	const uint8_t	*end;
	uint8_t			membuf[FABRIC_BUFFER_MEM_SZ];
} fabric_buffer;

typedef struct fabric_node_s {
	cf_node 	node_id; // remote node
	bool		live; // set to false on shutdown
	uint32_t	connect_count[AS_FABRIC_N_CHANNELS];
	bool		connect_full;

	pthread_mutex_t		connect_lock;

	pthread_mutex_t		fc_hash_lock;
	cf_shash			*fc_hash; // key is (fabric_connection *), value unused

	pthread_mutex_t		send_idle_fc_queue_lock;
	cf_queue			send_idle_fc_queue[AS_FABRIC_N_CHANNELS];

	cf_queue			send_queue[AS_FABRIC_N_CHANNELS];

	uint8_t	send_counts[];
} fabric_node;

typedef struct fabric_connection_s {
	cf_socket sock;
	cf_sock_addr peer;
	fabric_node *node;

	bool failed;
	bool started_via_connect;

	fabric_buffer	s_buf;
	msg				*s_msg_in_progress;
	size_t			s_count;

	fabric_buffer	*r_buf_in_progress;
	uint32_t		r_msg_size;
	msg_type		r_type;
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

// fabric_buffer
static fabric_buffer *fabric_buffer_create(size_t sz);
static void fabric_buffer_init(fabric_buffer *fb, size_t sz);
static void fabric_buffer_destroy(fabric_buffer *fb);
inline static void fabric_buffer_free_extra(fabric_buffer *fb);
inline static bool fabric_buffer_resize(fabric_buffer *fb, size_t sz);

// fabric_connection
fabric_connection *fabric_connection_create(cf_socket *sock, cf_sock_addr *peer);
static bool fabric_connection_accept_tls(fabric_connection *fc);
static bool fabric_connection_connect_tls(fabric_connection *fc);
inline static void fabric_connection_reserve(fabric_connection *fc);
static void fabric_connection_release(fabric_connection *fc);
inline static cf_node fabric_connection_get_id(const fabric_connection *fc);

static void fabric_connection_send_assign(fabric_connection *fc);
static void fabric_connection_send_unassign(fabric_connection *fc);
inline static void fabric_connection_recv_rearm(fabric_connection *fc);
inline static void fabric_connection_send_rearm(fabric_connection *fc);
static void fabric_connection_disconnect(fabric_connection *fc);
static void fabric_connection_set_keepalive_options(fabric_connection *fc);

static void fabric_connection_reroute_msg(fabric_connection *fc);
static void fabric_connection_send_progress(fabric_connection *fc, bool is_last);
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
static void run_fabric_recv_cleanup(void *arg);
static void *run_fabric_send(void *arg);
static void *run_fabric_accept(void *arg);

// Ticker helpers.
static int fabric_rate_node_reduce_fn(const void *key, uint32_t keylen, void *data, void *udata);
static int fabric_rate_fc_reduce_fn(const void *key, void *data, void *udata);

// Heartbeat.
static void fabric_hb_plugin_set_fn(msg *m);
static void fabric_hb_plugin_parse_data_fn(msg *m, cf_node source, as_hb_plugin_node_data *plugin_data);
static void fabric_heartbeat_event(int nevents, as_hb_event_node *events, void *udata);


//==========================================================
// Public API.
//

//------------------------------------------------
// msg
//

msg *
as_fabric_msg_get(msg_type type)
{
	if (type >= M_TYPE_MAX) {
		return NULL;
	}

	msg *m = NULL;

	if (cf_queue_pop(&g_fabric.msg_pool_queue[type], &m, CF_QUEUE_NOWAIT) !=
			CF_QUEUE_OK) {
		m = msg_create(type);
	}
	else {
		msg_incr_ref(m);
	}

	return m;
}

void
as_fabric_msg_put(msg *m)
{
	int cnt = cf_rc_release(m);

	if (cnt == 0) {
		msg_reset(m);

		if (cf_queue_sz(&g_fabric.msg_pool_queue[m->type]) > 128) {
			msg_put(m);
		}
		else {
			cf_queue_push(&g_fabric.msg_pool_queue[m->type], &m);
		}
	}
	else if (cnt < 0) {
		msg_dump(m, "extra put");
		cf_crash(AS_FABRIC, "extra put for msg type %d", m->type);
	}
}

// Log information about existing "msg" objects and queues.
void
as_fabric_msg_queue_dump()
{
	cf_info(AS_FABRIC, "All currently-existing msg types:");

	int total_q_sz = 0;
	int total_alloced_msgs = 0;

	for (int i = 0; i < M_TYPE_MAX; i++) {
		int q_sz = cf_queue_sz(&g_fabric.msg_pool_queue[i]);
		int num_of_type = cf_atomic_int_get(g_num_msgs_by_type[i]);

		total_alloced_msgs += num_of_type;

		if (q_sz || num_of_type) {
			cf_info(AS_FABRIC, "|msgq[%d]| = %d ; alloc'd = %d", i, q_sz, num_of_type);
			total_q_sz += q_sz;
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

int
as_fabric_init()
{
	for (uint32_t i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		g_fabric_connect_limit[i] = g_config.n_fabric_channel_fds[i];

		fabric_recv_thread_pool_init(&g_fabric.recv_pool[i],
				g_config.n_fabric_channel_recv_threads[i], i);
	}

	pthread_mutex_init(&g_fabric.send_lock, 0);

	as_fabric_register_msg_fn(M_TYPE_FABRIC, fabric_mt, sizeof(fabric_mt),
			FS_MSG_SCRATCH_SIZE, NULL, NULL);

	pthread_mutex_init(&g_fabric.node_hash_lock, 0);

	cf_rchash_create(&g_fabric.node_hash, cf_nodeid_rchash_fn,
			fabric_node_destructor, sizeof(cf_node), 128, 0);

	for (int i = 0; i < M_TYPE_MAX; i++) {
		cf_queue_init(&g_fabric.msg_pool_queue[i], sizeof(msg *),
				CF_QUEUE_ALLOCSZ, true);
	}

	cf_vector_init(&g_fabric.fb_free, sizeof(fabric_buffer *), 64,
			VECTOR_FLAG_BIGLOCK);

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

	as_fabric_transact_init();

	return 0;
}

int
as_fabric_start()
{
	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	g_fabric.sends =
			cf_malloc(sizeof(send_entry) * g_config.n_fabric_send_threads);
	g_fabric.send_head = g_fabric.sends;

	cf_info(AS_FABRIC, "starting %u fabric send threads", g_config.n_fabric_send_threads);

	for (int i = 0; i < g_config.n_fabric_send_threads; i++) {
		cf_poll_create(&g_fabric.sends[i].poll);
		g_fabric.sends[i].id = i;
		g_fabric.sends[i].count = 0;
		g_fabric.sends[i].next = g_fabric.sends + i + 1;

		if (pthread_create(&thread, &attrs, run_fabric_send,
				&g_fabric.sends[i]) != 0) {
			cf_crash(AS_FABRIC, "could not create fabric send thread");
		}
	}

	g_fabric.sends[g_config.n_fabric_send_threads - 1].next = NULL;

	for (uint32_t i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		cf_info(AS_FABRIC, "starting %u fabric %s channel recv threads", g_config.n_fabric_channel_recv_threads[i], CHANNEL_NAMES[i]);

		fabric_recv_thread_pool_set_size(&g_fabric.recv_pool[i],
				g_config.n_fabric_channel_recv_threads[i]);
	}

	cf_info(AS_FABRIC, "starting fabric accept thread");

	if (pthread_create(&thread, &attrs, run_fabric_accept, NULL) != 0) {
		cf_crash(AS_FABRIC, "could not create fabric accept thread");
	}

	return 0;
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
	if (! nodes) {
		node_list nl;

		fabric_get_node_list(&nl);
		return as_fabric_send_list(nl.nodes, nl.count, m, channel);
	}

	int ret = AS_FABRIC_SUCCESS;

	for (uint32_t i = 0; i < node_count; i++) {
		msg_incr_ref(m);

		if ((ret = as_fabric_send(nodes[i], m, channel)) != AS_FABRIC_SUCCESS) {
			// Leave the reference for the sake of caller.
			break;
		}
	}

	as_fabric_msg_put(m); // release main reference

	return ret;
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
	pthread_mutex_lock(&g_fabric.node_hash_lock);
	cf_rchash_reduce(g_fabric.node_hash, fabric_rate_node_reduce_fn, rate);
	pthread_mutex_unlock(&g_fabric.node_hash_lock);
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

		pthread_mutex_lock(&node->fc_hash_lock);
		cf_info(AS_FABRIC, "\tnode %lx fds {via_connect={h=%d m=%d l=%d} all=%d} live %d q {h=%d m=%d l=%d}",
				node->node_id,
				node->connect_count[AS_FABRIC_CHANNEL_CTRL],
				node->connect_count[AS_FABRIC_CHANNEL_RW],
				node->connect_count[AS_FABRIC_CHANNEL_BULK],
				cf_shash_get_size(node->fc_hash), node->live,
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_CTRL]),
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_RW]),
				cf_queue_sz(&node->send_queue[AS_FABRIC_CHANNEL_BULK]));
		pthread_mutex_unlock(&node->fc_hash_lock);

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

	if (pthread_mutex_init(&node->send_idle_fc_queue_lock, NULL) != 0) {
		cf_crash(AS_FABRIC, "fabric_node_create(%lx) failed to init send_idle_fc_queue_lock", node_id);
	}

	for (int i = 0; i < AS_FABRIC_N_CHANNELS; i++) {
		cf_queue_init(&node->send_idle_fc_queue[i], sizeof(fabric_connection *),
				CF_QUEUE_ALLOCSZ, false);

		cf_queue_init(&node->send_queue[i], sizeof(msg *), CF_QUEUE_ALLOCSZ,
				true);
	}

	if (pthread_mutex_init(&node->connect_lock, NULL) != 0) {
		cf_crash(AS_FABRIC, "fabric_node_create(%lx) failed to init connect_lock", node_id);
	}

	if (pthread_mutex_init(&node->fc_hash_lock, NULL) != 0) {
		cf_crash(AS_FABRIC, "fabric_node_create(%lx) failed to init fc_hash_lock", node_id);
	}

	node->fc_hash = cf_shash_create(cf_shash_fn_ptr,
			sizeof(fabric_connection *), 0, 32, 0);

	cf_detail(AS_FABRIC, "fabric_node_create(%lx) node %p", node_id, node);

	return node;
}

static fabric_node *
fabric_node_get(cf_node node_id)
{
	fabric_node *node;

	pthread_mutex_lock(&g_fabric.node_hash_lock);
	int rv = cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node);
	pthread_mutex_unlock(&g_fabric.node_hash_lock);

	if (rv != CF_RCHASH_OK) {
		return NULL;
	}

	return node;
}

static fabric_node *
fabric_node_get_or_create(cf_node node_id)
{
	fabric_node *node;

	pthread_mutex_lock(&g_fabric.node_hash_lock);

	if (cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node) == CF_RCHASH_OK) {
		pthread_mutex_unlock(&g_fabric.node_hash_lock);

		fabric_node_connect_all(node);

		return node;
	}

	node = fabric_node_create(node_id);

	if (cf_rchash_put_unique(g_fabric.node_hash, &node_id, sizeof(cf_node),
			node) != CF_RCHASH_OK) {
		cf_crash(AS_FABRIC, "fabric_node_get_or_create(%lx)", node_id);
	}

	fabric_node_reserve(node); // for return

	pthread_mutex_unlock(&g_fabric.node_hash_lock);

	fabric_node_connect_all(node);

	return node;
}

static fabric_node *
fabric_node_pop(cf_node node_id)
{
	fabric_node *node = NULL;

	pthread_mutex_lock(&g_fabric.node_hash_lock);

	if (cf_rchash_get(g_fabric.node_hash, &node_id, sizeof(cf_node),
			(void **)&node) == CF_RCHASH_OK) {
		if (cf_rchash_delete(g_fabric.node_hash, &node_id, sizeof(node_id)) !=
				CF_RCHASH_OK) {
			cf_crash(AS_FABRIC, "fabric_node_pop(%lx)", node_id);
		}
	}

	pthread_mutex_unlock(&g_fabric.node_hash_lock);

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

	pthread_mutex_lock(&node->fc_hash_lock);

	node->live = false;
	// Clean up all fc's attached to this node.
	cf_shash_reduce(node->fc_hash, fabric_node_disconnect_reduce_fn, NULL);

	pthread_mutex_unlock(&node->fc_hash_lock);

	pthread_mutex_lock(&node->send_idle_fc_queue_lock);

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

	pthread_mutex_unlock(&node->send_idle_fc_queue_lock);

	fabric_node_release(node); // from fabric_node_pop()
}

static fabric_connection *
fabric_node_connect(fabric_node *node, uint32_t ch)
{
	cf_detail(AS_FABRIC, "fabric_node_connect(%p, %u)", node, ch);

	pthread_mutex_lock(&node->connect_lock);

	uint32_t fds = node->connect_count[ch] + 1;

	if (fds > g_fabric_connect_limit[ch]) {
		pthread_mutex_unlock(&node->connect_lock);
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
				pthread_mutex_unlock(&node->connect_lock);
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
			pthread_mutex_unlock(&node->connect_lock);
			return NULL;
		}

		// The list capacity was not enough. Retry with suggested list size.
	}

	if (tries_remaining < 0) {
		cf_warning(AS_FABRIC,"fabric_node_connect(%p, %u) List get error for remote node %lx", node, ch, node->node_id);
		pthread_mutex_unlock(&node->connect_lock);
		return NULL;
	}

	msg *m = as_fabric_msg_get(M_TYPE_FABRIC);

	cf_atomic64_incr(&g_stats.fabric_connections_opened);
	msg_set_uint64(m, FS_FIELD_NODE, g_config.self_node);
	msg_set_uint32(m, FS_CHANNEL, ch);
	m->benchmark_time = g_config.fabric_benchmarks_enabled ? cf_getns() : 0;

	fabric_connection *fc = fabric_connection_create(&sock, &addr);

	fc->s_msg_in_progress = m;
	fc->started_via_connect = true;
	fc->pool = &g_fabric.recv_pool[ch];

	if (! fabric_node_add_connection(node, fc)) {
		fabric_connection_release(fc);
		pthread_mutex_unlock(&node->connect_lock);
		return NULL;
	}

	node->connect_count[ch]++;
	node->connect_full = fabric_node_is_connect_full(node);

	pthread_mutex_unlock(&node->connect_lock);

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
		pthread_mutex_lock(&node->send_idle_fc_queue_lock);

		fabric_connection *fc;
		int rv = cf_queue_pop(&node->send_idle_fc_queue[(int)channel], &fc,
				CF_QUEUE_NOWAIT);

		if (rv != CF_QUEUE_OK) {
			cf_queue_push(&node->send_queue[(int)channel], &m);
			pthread_mutex_unlock(&node->send_idle_fc_queue_lock);

			if (! node->connect_full) {
				fabric_node_connect_all(node);
			}

			break;
		}

		pthread_mutex_unlock(&node->send_idle_fc_queue_lock);

		if ((! cf_socket_exists(&fc->sock)) || fc->failed) {
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

	pthread_mutex_destroy(&node->send_idle_fc_queue_lock);

	// connection_hash section.
	cf_assert(cf_shash_get_size(node->fc_hash) == 0, AS_FABRIC, "fc_hash not empty as expected");
	cf_shash_destroy(node->fc_hash);

	pthread_mutex_destroy(&node->fc_hash_lock);
}

inline static void
fabric_node_reserve(fabric_node *node) {
	cf_rc_reserve(node);
}

inline static void
fabric_node_release(fabric_node *node)
{
	int cnt = cf_rc_release(node);

	if (cnt == 0) {
		fabric_node_destructor(node);
		cf_rc_free(node);
	}
	else if (cnt < 0) {
		cf_crash(AS_FABRIC, "fabric_node_release(%p) extra call", node);
	}
}

static bool
fabric_node_add_connection(fabric_node *node, fabric_connection *fc)
{
	pthread_mutex_lock(&node->fc_hash_lock);

	if (! node->live) {
		pthread_mutex_unlock(&node->fc_hash_lock);
		return false;
	}

	fabric_node_reserve(node);
	fc->node = node;

	fabric_connection_set_keepalive_options(fc);
	fabric_connection_reserve(fc); // for put into node->fc_hash

	uint8_t value = 0;
	int rv = cf_shash_put_unique(node->fc_hash, &fc, &value);

	cf_assert(rv == CF_SHASH_OK, AS_FABRIC, "fabric_node_add_connection(%p, %p) failed to add with rv %d", node, fc, rv);

	pthread_mutex_unlock(&node->fc_hash_lock);

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

	pthread_mutex_lock(&g_fabric.node_hash_lock);
	cf_rchash_reduce(g_fabric.node_hash, fabric_get_node_list_fn, nl);
	pthread_mutex_unlock(&g_fabric.node_hash_lock);

	return nl->count;
}


//==========================================================
// fabric_buffer
//

static fabric_buffer *
fabric_buffer_create(size_t sz)
{
	fabric_buffer *fb;

	if (cf_vector_pop(&g_fabric.fb_free, &fb) != 0) {
		fb = cf_malloc(sizeof(fabric_buffer));
	}

	fabric_buffer_init(fb, sz);

	return fb;
}

static void
fabric_buffer_init(fabric_buffer *fb, size_t sz)
{
	if (sz > FABRIC_BUFFER_MEM_SZ) {
		fb->buf = (uint8_t *)cf_malloc(sz);
	}
	else {
		fb->buf = fb->membuf;
	}

	fb->progress = fb->buf;
	fb->end = fb->buf + sz;
}

static void
fabric_buffer_destroy(fabric_buffer *fb)
{
	fabric_buffer_free_extra(fb);

	if (cf_vector_size(&g_fabric.fb_free) > 64) {
		cf_free(fb);
	}
	else if (cf_vector_append(&g_fabric.fb_free, &fb) != 0) {
		cf_crash(AS_FABRIC, "push into %p failed on fb %p", &g_fabric.fb_free, fb);
	}
}

inline static void
fabric_buffer_free_extra(fabric_buffer *fb)
{
	if (fb->buf != fb->membuf) {
		cf_free(fb->buf);
	}
}

// Resize fb after we know the msg_size.
inline static bool
fabric_buffer_resize(fabric_buffer *fb, size_t sz)
{
	if (sz > FABRIC_BUFFER_MEM_SZ) {
		if (sz > FABRIC_BUFFER_MAX_SZ) {
			return false;
		}

		cf_assert(fb->buf == fb->membuf, AS_FABRIC, "function misuse");

		size_t old_sz = fb->progress - fb->membuf;

		fb->buf = (uint8_t *)cf_malloc(sz);

		memcpy(fb->buf, fb->membuf, old_sz);
		fb->progress = fb->buf + old_sz;
	}

	fb->end = fb->buf + sz;
	return true;
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

	fc->r_buf_in_progress = fabric_buffer_create(sizeof(msg_hdr));
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
	int cnt = cf_rc_release(fc);

	if (cnt == 0) {
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

		fabric_buffer_destroy(fc->r_buf_in_progress);
		fabric_buffer_free_extra(&fc->s_buf);

		cf_rc_free(fc);
	}
	else if (cnt < 0) {
		cf_crash(AS_FABRIC, "extra fabric_connection_release %p", fc);
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

// epoll takes the reference of fc.
static void
fabric_connection_send_assign(fabric_connection *fc)
{
	pthread_mutex_lock(&g_fabric.send_lock);

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

	pthread_mutex_unlock(&g_fabric.send_lock);

	cf_poll_add_socket(se->poll, &fc->sock, EPOLLOUT | DEFAULT_EVENTS, fc);
}

static void
fabric_connection_send_unassign(fabric_connection *fc)
{
	pthread_mutex_lock(&g_fabric.send_lock);

	if (! fc->send_ptr) {
		pthread_mutex_unlock(&g_fabric.send_lock);
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

	pthread_mutex_unlock(&g_fabric.send_lock);
}

inline static void
fabric_connection_recv_rearm(fabric_connection *fc)
{
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

	pthread_mutex_lock(&node->fc_hash_lock);

	if (cf_shash_delete(node->fc_hash, &fc) != CF_SHASH_OK) {
		cf_detail(AS_FABRIC, "fc %p is not in (node %p)->fc_hash", fc, node);
		pthread_mutex_unlock(&node->fc_hash_lock);
		return;
	}

	pthread_mutex_unlock(&node->fc_hash_lock);

	if (fc->started_via_connect) {
		pthread_mutex_lock(&node->connect_lock);

		cf_atomic32_decr(&node->connect_count[fc->pool->pool_id]);
		node->connect_full = false;

		pthread_mutex_unlock(&node->connect_lock);
	}

	pthread_mutex_lock(&node->send_idle_fc_queue_lock);

	if (cf_queue_delete(&node->send_idle_fc_queue[fc->pool->pool_id], &fc,
			true) == CF_QUEUE_OK) {
		fabric_connection_release(fc); // for delete from send_idle_fc_queue
	}

	pthread_mutex_unlock(&node->send_idle_fc_queue_lock);

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
fabric_connection_send_progress(fabric_connection *fc, bool is_last)
{
	uint8_t *send_progress;
	size_t send_full;

	if (fc->s_buf.buf) {
		// Partially sent msg.
		send_progress = fc->s_buf.progress;
		send_full = fc->s_buf.end - send_progress;
	}
	else {
		// Fresh msg.
		msg *m = fc->s_msg_in_progress;

		send_full = msg_get_wire_size(m);
		fabric_buffer_init(&fc->s_buf, send_full);

		send_progress = fc->s_buf.progress;
		msg_to_wire(m, send_progress);

		if (m->benchmark_time != 0) {
			m->benchmark_time = histogram_insert_data_point(
					g_stats.fabric_send_init_hists[fc->pool->pool_id],
					m->benchmark_time);
		}
	}

	int32_t flags = MSG_NOSIGNAL | (is_last ? 0 : MSG_MORE);
	int32_t send_sz = cf_socket_send(&fc->sock, send_progress, send_full,
			flags);

	if (send_sz < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			fc->failed = true;
			cf_socket_write_shutdown(&fc->sock);
			return;
		}

		send_sz = 0; // treat as sending 0
	}

	if (fc->s_msg_in_progress->benchmark_time != 0) {
		fc->s_msg_in_progress->benchmark_time = histogram_insert_data_point(
				g_stats.fabric_send_fragment_hists[fc->pool->pool_id],
				fc->s_msg_in_progress->benchmark_time);
	}

	fc->s_bytes += send_sz;

	if ((size_t)send_sz == send_full) {
		// Complete send.
		as_fabric_msg_put(fc->s_msg_in_progress);
		fc->s_msg_in_progress = NULL;
		fabric_buffer_free_extra(&fc->s_buf);
		fc->s_buf.buf = NULL;
		fc->s_count++;
	}
	else {
		// Partial send.
		fc->s_buf.progress += send_sz;
	}
}

// Must rearm or place into idle queue on success.
static bool
fabric_connection_process_writable(fabric_connection *fc)
{
	// Strategy with MSG_MORE to prevent small packets during migration.
	// Case 1 - socket buffer not full:
	//    Send all messages except last with MSG_MORE. Last message flushes
	//    buffer.
	// Case 2 - socket buffer full:
	//    All messages get sent with MSG_MORE but because buffer full, small
	//    packets still won't happen.
	fabric_node *node = fc->node;
	uint32_t pool = fc->pool->pool_id;

	if (! fc->s_msg_in_progress) {
		// TODO - Change to load op when atomic API is ready.
		// Also should be rare or not even happen in x86_64.
		cf_warning(AS_FABRIC, "fc(%p)->s_msg_in_progress NULL on entry", fc);
		return false;
	}

	while (fc->s_msg_in_progress) {
		msg *pending = NULL;

		cf_queue_pop(&node->send_queue[pool], &pending, CF_QUEUE_NOWAIT);
		fabric_connection_send_progress(fc, ! pending);

		if (fc->s_msg_in_progress) {
			if (pending) {
				cf_queue_push_head(&node->send_queue[pool], &pending);
			}

			fabric_connection_send_rearm(fc);
			return true;
		}

		fc->s_msg_in_progress = pending;
	}

	if (! fc->node->live || fc->failed) {
		return false;
	}

	// Try with bigger lock block to sync with as_fabric_send().
	pthread_mutex_lock(&node->send_idle_fc_queue_lock);

	if (! fc->node->live || fc->failed) {
		pthread_mutex_unlock(&node->send_idle_fc_queue_lock);
		return false;
	}

	if (cf_queue_pop(&node->send_queue[pool], &fc->s_msg_in_progress,
			CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		cf_queue_push(&node->send_idle_fc_queue[pool], &fc);
		pthread_mutex_unlock(&node->send_idle_fc_queue_lock);
		return true;
	}

	pthread_mutex_unlock(&node->send_idle_fc_queue_lock);

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

	fabric_buffer_free_extra(fc->r_buf_in_progress);
	fabric_buffer_init(fc->r_buf_in_progress, sizeof(msg_hdr));
	fc->r_msg_size = 0;

	// fc->pool needs to be set before placing into send_idle_fc_queue.
	fabric_recv_thread_pool_add_fc(&g_fabric.recv_pool[pool_id], fc);

	// TLS connections are one-way. Incoming connections are for
	// incoming data.
	if (fc->sock.state == CF_SOCKET_STATE_NON_TLS) {
		pthread_mutex_lock(&node->send_idle_fc_queue_lock);

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

		pthread_mutex_unlock(&node->send_idle_fc_queue_lock);
	}

	fabric_node_release(node); // from cf_rchash_get
	fabric_connection_release(fc); // from g_accept_poll

	return true;
}

static bool
fabric_connection_read_fabric_msg(fabric_connection *fc)
{
	fabric_buffer *fb = fc->r_buf_in_progress;

	while (true) {
		size_t recv_full = fb->end - fb->progress;
		int32_t recv_sz = cf_socket_recv(&fc->sock, fb->progress, recv_full, 0);

		if (recv_sz < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_warning(AS_FABRIC, "fabric_connection_read_fabric_msg() recv_sz %d errno %d %s", recv_sz, errno, cf_strerror(errno));
				return false;
			}

			break;
		}

		if (recv_sz == 0) {
			cf_detail(AS_FABRIC, "fabric_connection_read_fabric_msg(%p) fb=%p recv_sz == 0 / %zu", fc, fb, recv_full);
			return false;
		}

		fb->progress += recv_sz;
		fc->r_bytes += recv_sz;

		if ((size_t)recv_sz < recv_full) {
			tls_socket_must_not_have_data(&fc->sock, "partial fabric read");
			break;
		}

		if (fc->r_msg_size == 0) {
			size_t hdr_sz = fb->progress - fb->buf;

			if (msg_get_initial(
					&fc->r_msg_size, &fc->r_type, fb->buf, hdr_sz) != 0) {
				cf_crash(AS_FABRIC, "fb->end was not initialized correctly");
			}

			if (! fabric_buffer_resize(fb, fc->r_msg_size)) {
				cf_warning(AS_FABRIC, "fabric_connection_read_fabric_msg(%p) invalid msg_size %u remote 0x%lx", fc, fc->r_msg_size, fabric_connection_get_id(fc));
				return false;
			}

			continue;
		}

		tls_socket_must_not_have_data(&fc->sock, "full fabric read");

		if (fc->r_type != M_TYPE_FABRIC) {
			cf_warning(AS_FABRIC, "fabric_connection_read_fabric_msg() expected type M_TYPE_FABRIC(%d) got type %d", M_TYPE_FABRIC, fc->r_type);
			return false;
		}

		msg *m = as_fabric_msg_get(M_TYPE_FABRIC);

		if (msg_parse(m, fb->buf, fc->r_msg_size) != 0) {
			cf_warning(AS_FABRIC, "msg_parse failed for fc %p fb %p", fc, fb);
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
// Must have re-armed on success.
static bool
fabric_connection_process_msg(fabric_connection *fc, bool do_rearm)
{
	msg *m = as_fabric_msg_get(fc->r_type);

	if (! m) {
		cf_warning(AS_FABRIC, "Failed to create message for type %d (max %d)", fc->r_type, M_TYPE_MAX);
		return false;
	}

	fabric_buffer *fb = fc->r_buf_in_progress;

	if (msg_parse(m, fb->buf, fc->r_msg_size) != 0) {
		cf_warning(AS_FABRIC, "msg_parse failed for fc %p fb %p", fc, fb);
		as_fabric_msg_put(m);
		return false;
	}

	cf_assert(fc->node, AS_FABRIC, "process_msg: no node assigned");

	// Save some state for after re-arm.
	cf_node node = fc->node->node_id;
	uint64_t bt = fc->benchmark_time;
	uint32_t ch = fc->pool->pool_id;

	fc->r_msg_size = 0;

	if (do_rearm) {
		// Re-arm for next message (possibly handled in another thread).
		fc->r_buf_in_progress = fabric_buffer_create(sizeof(msg_hdr));
		fabric_connection_recv_rearm(fc); // do not use fc after this point
	}

	if (g_fabric.msg_cb[m->type]) {
		(g_fabric.msg_cb[m->type])(node, m, g_fabric.msg_udata[m->type]);

		if (bt != 0) {
			histogram_insert_data_point(g_stats.fabric_recv_cb_hists[ch], bt);
		}
	}
	else {
		cf_warning(AS_FABRIC, "process_msg: could not deliver message type %d", m->type);
		as_fabric_msg_put(m);
	}

	if (do_rearm) {
		fabric_buffer_destroy(fb);
	}

	return true;
}

// Return true on success.
// Must have re-armed on success.
static bool
fabric_connection_process_readable(fabric_connection *fc)
{
	fabric_buffer *fb = fc->r_buf_in_progress;
	size_t recv_all = 0;

	while (true) {
		size_t recv_full = fb->end - fb->progress;
		int32_t recv_sz = cf_socket_recv(&fc->sock, fb->progress, recv_full, 0);

		if (recv_sz < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_warning(AS_FABRIC, "fabric_connection_process_readable() recv_sz %d errno %d %s", recv_sz, errno, cf_strerror(errno));
				return false;
			}

			break;
		}

		if (recv_sz == 0) {
			cf_detail(AS_FABRIC, "fabric_connection_process_readable(%p) fb=%p recv_sz == 0 / %zu", fc, fb, recv_full);
			return false;
		}

		fb->progress += recv_sz;
		fc->r_bytes += recv_sz;
		recv_all += recv_sz;

		if (fc->r_msg_size == 0) {
			fc->benchmark_time = g_config.fabric_benchmarks_enabled ?
					cf_getns() : 0;
		}

		if ((size_t)recv_sz < recv_full) {
			if (fc->benchmark_time != 0) {
				fc->benchmark_time = histogram_insert_data_point(
						g_stats.fabric_recv_fragment_hists[fc->pool->pool_id],
						fc->benchmark_time);
			}

			break;
		}

		if (fc->r_msg_size == 0) {
			size_t hdr_sz = fb->progress - fb->buf;

			if (msg_get_initial(
					&fc->r_msg_size, &fc->r_type, fb->buf, hdr_sz) != 0) {
				cf_crash(AS_FABRIC, "fb->end was not initialized correctly");
			}

			if (! fabric_buffer_resize(fb, fc->r_msg_size)) {
				cf_warning(AS_FABRIC, "fabric_connection_process_readable(%p) invalid msg_size %u remote 0x%lx", fc, fc->r_msg_size, fabric_connection_get_id(fc));
				return false;
			}

			continue;
		}

		bool do_rearm = recv_all > (size_t)g_config.fabric_recv_rearm_threshold;

		if (! fabric_connection_process_msg(fc, do_rearm)) {
			return false;
		}

		if (do_rearm) {
			// Already rearmed.
			return true;
		}

		fabric_buffer_free_extra(fc->r_buf_in_progress);
		fabric_buffer_init(fc->r_buf_in_progress, sizeof(msg_hdr));
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
	cf_vector_init(&pool->threads, sizeof(pthread_t), size, 0);
	cf_poll_create(&pool->poll);
	pool->pool_id = pool_id;
}

// Called only at startup or under set-config lock. Caller has checked size.
static void
fabric_recv_thread_pool_set_size(fabric_recv_thread_pool *pool, uint32_t size)
{
	while (size < cf_vector_size(&pool->threads)) {
		pthread_t th;
		cf_vector_pop(&pool->threads, &th);
		pthread_cancel(th);
	}

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	while (size > cf_vector_size(&pool->threads)) {
		if (pthread_create(&thread, &attrs, run_fabric_recv, pool) != 0) {
			cf_crash(AS_FABRIC, "could not create fabric recv thread");
		}

		cf_vector_append(&pool->threads, &thread);
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
	int oldstate;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);

	fabric_recv_thread_pool *pool = (fabric_recv_thread_pool *)arg;
	static int worker_id_counter = 0;
	uint64_t worker_id = worker_id_counter++;
	cf_poll poll = pool->poll;

	cf_detail(AS_FABRIC, "run_fabric_recv() created index %lu", worker_id);

	pthread_cleanup_push(run_fabric_recv_cleanup, (void *)worker_id);

	while (true) {
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
		pthread_testcancel();
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);

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

			if (! fabric_connection_process_readable(fc)) {
				fabric_connection_disconnect(fc);
				fabric_connection_release(fc);
				continue;
			}
		}
	}

	pthread_cleanup_pop(0);
	return NULL;
}

static void
run_fabric_recv_cleanup(void *arg)
{
	uint64_t worker_id = (uint64_t)arg;

	cf_detail(AS_FABRIC, "run_fabric_recv() canceling index %lu", worker_id);
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

	pthread_mutex_lock(&node->fc_hash_lock);
	cf_shash_reduce(node->fc_hash, fabric_rate_fc_reduce_fn, rate);
	pthread_mutex_unlock(&node->fc_hash_lock);

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

	if (msg_set_buf(m, AS_HB_MSG_FABRIC_DATA,
			(uint8_t *)g_published_endpoint_list, payload_size,
			MSG_SET_COPY) != 0) {
		cf_crash(AS_FABRIC, "Error setting succession list on msg.");
	}
}

// Plugin function that parses succession list out of a heartbeat pulse message.
static void
fabric_hb_plugin_parse_data_fn(msg *m, cf_node source,
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


//==============================================================================
// Fabric transact.
//

//==========================================================
// Constants and typedefs.
//

typedef enum {
	TRANSACT_CODE_REQUEST = 1,
	TRANSACT_CODE_RESPONSE = 2,
} transact_code;

// Operation to be performed on transaction in retransmission hash.
typedef enum {
	TRANSACT_OP_TIMEOUT = 1,
	TRANSACT_OP_RETRANSMIT = 2,
} transact_op;

typedef struct fabric_transact_xmit_s {
	uint64_t		tid;
	cf_node 		node_id;
	msg				*m;
	pthread_mutex_t	lock;

	uint64_t		deadline_ms;
	uint64_t		retransmit_ms;
	int 			retransmit_wait;

	as_fabric_transact_complete_fn cb;
	void			*udata;
} fabric_transact_xmit;

typedef struct fabric_transact_recv_s {
	cf_node 	node_id; // where it came from
	uint64_t	tid; // inbound tid
} fabric_transact_recv;

typedef struct transact_recv_key_s {
	uint64_t	tid;
	cf_node 	node_id;
} __attribute__ ((__packed__)) transact_recv_key;

typedef struct ll_fabric_transact_xmit_element_s {
	cf_ll_element ll_e;
	int op;
	uint64_t tid;
} ll_fabric_transact_xmit_element;


//==========================================================
// Globals.
//

static cf_atomic64 g_fabric_transact_tid = 0;
static cf_rchash *g_fabric_transact_xmit_hash = NULL;
static as_fabric_transact_recv_fn fabric_transact_recv_cb[M_TYPE_MAX] = { 0 };
static void *fabric_transact_recv_udata[M_TYPE_MAX] = { 0 };


//==========================================================
// Forward declarations and inlines.
//

static void fabric_transact_xmit_destructor(void *object);
static void fabric_transact_xmit_release(fabric_transact_xmit *ft);
static int fabric_transact_msg_fn(cf_node node_id, msg *m, void *udata);
static void *run_fabric_transact(void *arg);
static void ll_ftx_destructor_fn(cf_ll_element *e);
static int fabric_transact_xmit_reduce_fn(const void *key, uint32_t keylen, void *o, void *udata);
static int ll_ftx_reduce_fn(cf_ll_element *le, void *udata);

inline static transact_code
tid_code_get(uint64_t tid)
{
	return (transact_code)(tid >> 56);
}

inline static uint64_t
tid_code_set(uint64_t tid, transact_code code)
{
	return tid | (((uint64_t)code) << 56);
}

inline static uint64_t
tid_code_clear(uint64_t tid)
{
	return tid & 0xFFffffFFFFffff;
}


//==========================================================
// Public API.
//

void
as_fabric_transact_init()
{
	cf_rchash_create(&g_fabric_transact_xmit_hash, cf_rchash_fn_u32,
			fabric_transact_xmit_destructor, sizeof(uint64_t), 64,
			CF_RCHASH_MANY_LOCK);

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	if (pthread_create(&thread, &attrs, run_fabric_transact, NULL) != 0) {
		cf_crash(AS_FABRIC, "could not create fabric transact thread");
	}
}

void
as_fabric_transact_start(cf_node dest, msg *m, int timeout_ms,
		as_fabric_transact_complete_fn cb, void *udata)
{
	// TODO - could check it against the list of global message ids.

	if (msg_field_get_type(m, 0) != M_FT_UINT64) {
		// error
		cf_warning(AS_FABRIC, "as_fabric_transact: first field must be int64");
		(cb)(NULL, udata, AS_FABRIC_ERR_UNKNOWN);
		return;
	}

	fabric_transact_xmit *ft = cf_rc_alloc(sizeof(fabric_transact_xmit));

	ft->tid = cf_atomic64_incr(&g_fabric_transact_tid);
	ft->node_id = dest;
	ft->m = m;

	pthread_mutex_init(&ft->lock, NULL);
	uint64_t now = cf_getms();

	ft->deadline_ms = now + timeout_ms;
	ft->retransmit_wait = 10; // 10 ms start
	ft->retransmit_ms = now + ft->retransmit_wait; // hard start at 10 milliseconds
	ft->cb = cb;
	ft->udata = udata;

	uint64_t xmit_tid = tid_code_set(ft->tid, TRANSACT_CODE_REQUEST);

	// Set message tid.
	msg_set_uint64(m, 0, xmit_tid);

	// Put will take the reference, need to keep one around for the send.
	cf_rc_reserve(ft);
	cf_rchash_put(g_fabric_transact_xmit_hash, &ft->tid, sizeof(ft->tid), ft);

	// Transmit the initial message.
	msg_incr_ref(m);

	if (as_fabric_send(ft->node_id, ft->m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	fabric_transact_xmit_release(ft);

	return;
}

// Registers all of this message type as a
// transaction type message, which means the main message.
int
as_fabric_transact_register(msg_type type, const msg_template *mt, size_t mt_sz,
		size_t scratch_sz, as_fabric_transact_recv_fn cb, void *udata)
{
	// Put details in the global structure.
	fabric_transact_recv_cb[type] = cb;
	fabric_transact_recv_udata[type] = udata;

	// Register my internal callback with the main message callback.
	as_fabric_register_msg_fn(type, mt, mt_sz, scratch_sz,
			fabric_transact_msg_fn, NULL);

	return 0;
}

int
as_fabric_transact_reply(msg *m, void *transact_data)
{
	fabric_transact_recv *ftr = (fabric_transact_recv *)transact_data;

	// This is a response - overwrite tid with response code etc.
	uint64_t xmit_tid = tid_code_set(ftr->tid, TRANSACT_CODE_RESPONSE);
	msg_set_uint64(m, 0, xmit_tid);

	if (as_fabric_send(ftr->node_id, m, AS_FABRIC_CHANNEL_META) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	return 0;
}


//==========================================================
// Local helpers - various initializers and destructors.
//

static void
fabric_transact_xmit_release(fabric_transact_xmit *ft)
{
	if (cf_rc_release(ft) == 0) {
		fabric_transact_xmit_destructor(ft);
		cf_rc_free(ft);
	}
}

// Received a message. Could be a response to an outgoing message, or a new
// incoming transaction message.
static int
fabric_transact_msg_fn(cf_node node_id, msg *m, void *udata)
{
	// Assume m->type is correct.

	// Received a message, make sure we have a registered callback.
	if (fabric_transact_recv_cb[m->type] == 0) {
		cf_warning(AS_FABRIC, "transact: received message for transact with bad type %d, internal error", m->type);
		as_fabric_msg_put(m); // return to pool unexamined
		return 0;
	}

	// Check to see that we have an outstanding request (only cb once!).
	uint64_t tid = 0;

	if (msg_get_uint64(m, 0 /*field_id*/, &tid) != 0) {
		cf_warning(AS_FABRIC, "transact_msg: received message with no tid");
		as_fabric_msg_put(m);
		return 0;
	}

	transact_code code = tid_code_get(tid);
	tid = tid_code_clear(tid);

	// If it's a response, check against what you sent.
	if (code == TRANSACT_CODE_RESPONSE) {
		fabric_transact_xmit *ft;

		if (cf_rchash_get(g_fabric_transact_xmit_hash, &tid, sizeof(tid),
				(void **)&ft) != CF_RCHASH_OK) {
			cf_detail(AS_FABRIC, "transact_msg: {%lu} no fabric transmit structure in global hash", tid);
			as_fabric_msg_put(m);
			return 0;
		}

		if (cf_rchash_delete(g_fabric_transact_xmit_hash, &tid, sizeof(tid)) ==
				CF_RCHASH_ERR_NOT_FOUND) {
			cf_detail(AS_FABRIC, "transact_msg: {%lu} concurrent thread has already removed transaction", tid);
			fabric_transact_xmit_release(ft);
			as_fabric_msg_put(m);
			return 0;
		}

		pthread_mutex_lock(&ft->lock);

		// Make sure we haven't notified some other way, then notify caller.
		if (ft->cb) {
			(ft->cb)(m, ft->udata, AS_FABRIC_SUCCESS);
			ft->cb = NULL;
		}

		pthread_mutex_unlock(&ft->lock);

		// This will often be the final release.
		fabric_transact_xmit_release(ft);
	}
	else if (code == TRANSACT_CODE_REQUEST) {
		fabric_transact_recv *ftr = cf_malloc(sizeof(fabric_transact_recv));

		ftr->tid = tid; // has already been cleared
		ftr->node_id = node_id;

		// Notify caller - they will likely respond inline.
		(*fabric_transact_recv_cb[m->type])(node_id, m, ftr,
				fabric_transact_recv_udata[m->type]);
		cf_free(ftr);
	}
	else {
		cf_warning(AS_FABRIC, "transact_msg: {%lu} bad code on incoming message: %d", tid, code);
		as_fabric_msg_put(m);
	}

	return 0;
}

static void
fabric_transact_xmit_destructor(void *object)
{
	fabric_transact_xmit *ft = object;
	as_fabric_msg_put(ft->m);
}

// Long running thread for transaction maintenance.
static void *
run_fabric_transact(void *arg)
{
	// Create a list of transactions to be processed in each pass.
	cf_ll ll_fabric_transact_xmit;
	// Initialize list to empty list.
	// This list is processed by single thread. No need of a lock.
	cf_ll_init(&ll_fabric_transact_xmit, &ll_ftx_destructor_fn, false);

	while (true) {
		usleep(10000); // 10 ms for now

		// Visit each entry in g_fabric_transact_xmit_hash and select entries to
		// be retransmitted or timed out. Add that transaction id (tid) in the
		// linked list 'll_fabric_transact_xmit'.
		cf_rchash_reduce(g_fabric_transact_xmit_hash,
				fabric_transact_xmit_reduce_fn,
				(void *)&ll_fabric_transact_xmit);

		if (cf_ll_size(&ll_fabric_transact_xmit)) {
			// There are transactions to be processed.
			// Process each transaction in list.
			cf_ll_reduce(&ll_fabric_transact_xmit, true /*forward*/,
					ll_ftx_reduce_fn, NULL);
		}
	}

	return 0;
}

static void
ll_ftx_destructor_fn(cf_ll_element *e)
{
	cf_free(e);
}

static int
fabric_transact_xmit_reduce_fn(const void *key, uint32_t keylen, void *o,
		void *udata)
{
	fabric_transact_xmit *ftx = (fabric_transact_xmit *)o;
	int op = 0;

	uint64_t now = cf_getms();

	pthread_mutex_lock(&ftx->lock);

	if (now > ftx->deadline_ms) {
		// Expire and remove transactions that are timed out.
		// Need to call application: we've timed out.
		op = (int)TRANSACT_OP_TIMEOUT;
	}
	else if (now > ftx->retransmit_ms) {
		// Retransmit, update time counters, etc.
		ftx->retransmit_ms = now + ftx->retransmit_wait;
		ftx->retransmit_wait *= 2;
		op = (int)TRANSACT_OP_RETRANSMIT;
	}

	if (op > 0) {
		// Add the transaction in linked list of transactions to be processed.
		// Process such transactions *outside* retransmit hash lock, because...
		//
		// Fabric short circuits the message to self by directly calling
		// receiver function of corresponding module. Receiver constructs
		// "reply" and hands over to fabric to deliver.
		//
		// On receiving "reply", fabric removes original message, for which
		// this is a reply, from retransmit hash.
		//
		// "fabric_transact_xmit_reduce_fn" is invoked by reduce_delete, which
		// holds the lock over corresponding hash (here "retransmit hash"). If
		// the message, sent by this function, is short circuited by fabric,
		// the same thread will again try to get lock over "retransmit hash",
		// resulting in deadlock.

		cf_ll *ll_fabric_transact_xmit = (cf_ll *)udata;

		// Create new node for list.
		ll_fabric_transact_xmit_element *ll_ftx_ele =
				(ll_fabric_transact_xmit_element *)
				cf_malloc(sizeof(ll_fabric_transact_xmit_element));

		ll_ftx_ele->tid = ftx->tid;
		ll_ftx_ele->op = op;
		// Append into list.
		cf_ll_append(ll_fabric_transact_xmit, (cf_ll_element *)ll_ftx_ele);
	}

	pthread_mutex_unlock(&ftx->lock);

	return 0;
}

static int
ll_ftx_reduce_fn(cf_ll_element *le, void *udata)
{
	const ll_fabric_transact_xmit_element *ll_ftx_ele =
			(const ll_fabric_transact_xmit_element *)le;
	fabric_transact_xmit *ftx;
	uint64_t tid = ll_ftx_ele->tid;

	// cf_rchash_get increments ref count on transaction ftx.
	int rv = cf_rchash_get(g_fabric_transact_xmit_hash, &tid, sizeof(tid),
			(void **)&ftx);

	if (rv != 0) {
		cf_warning(AS_FABRIC, "No fabric transmit structure in global hash for fabric transaction-id %lu", tid);
		return CF_LL_REDUCE_DELETE;
	}

	if (ll_ftx_ele->op == (int)TRANSACT_OP_TIMEOUT) {
		// Call application: we've timed out.
		if (ftx->cb) {
			(ftx->cb)(0, ftx->udata, AS_FABRIC_ERR_TIMEOUT);
			ftx->cb = NULL;
		}

		cf_detail(AS_FABRIC, "fabric transact: %lu timed out", tid);
		// cf_rchash_delete removes ftx from hash and decrements ref count.
		cf_rchash_delete(g_fabric_transact_xmit_hash, &tid, sizeof(tid));
		// It should be final release of transaction ftx. On final release, it
		// also decrements message ref count, taken by initial fabric_send().
		fabric_transact_xmit_release(ftx);
	}
	else if (ll_ftx_ele->op == (int)TRANSACT_OP_RETRANSMIT) {
		if (ftx->m) {
			msg_incr_ref(ftx->m);

			msg *m = ftx->m;
			cf_node node = ftx->node_id;

			if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_META) != 0) {
				cf_detail(AS_FABRIC, "fabric: transact: %lu retransmit send failed", tid);
				as_fabric_msg_put(m);
			}
			else {
				cf_detail(AS_FABRIC, "fabric: transact: %lu retransmit send success", tid);
			}
		}

		// Decrement ref count, incremented by cf_rchash_get.
		fabric_transact_xmit_release(ftx);
	}

	// Remove it from linked list.
	return CF_LL_REDUCE_DELETE;
}
