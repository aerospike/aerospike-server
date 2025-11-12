/*
 * service.c
 *
 * Copyright (C) 2018-2020 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "base/service.h"

#include <errno.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <zlib.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "epoll_queue.h"
#include "hardware.h"
#include "log.h"
#include "shash.h"
#include "socket.h"
#include "tls.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "fabric/partition.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define N_EVENTS 1024

#define XDR_WRITE_BUFFER_SIZE (5 * 1024 * 1024)
#define XDR_READ_BUFFER_SIZE (15 * 1024 * 1024)

#define MAX_ADMIN_CONNECTIONS 100

typedef struct thread_ctx_s {
	cf_topo_cpu_index i_cpu;
	cf_mutex* lock;
	cf_poll poll;
	cf_epoll_queue trans_q;
} thread_ctx;


//==========================================================
// Globals.
//

as_service_access g_access = {
	.service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.alt_service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.tls_service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.alt_tls_service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.admin = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.tls_admin = { .addrs = { .n_addrs = 0 }, .port = 0 }
};

cf_serv_cfg g_service_bind = { .n_cfgs = 0 };
cf_tls_info* g_tls_service;
cf_serv_cfg g_admin_bind = { .n_cfgs = 0 };
cf_tls_info* g_tls_admin;

static cf_sockets g_sockets;
static cf_sockets g_admin_sockets;

static cf_poll g_admin_poll;

static cf_mutex g_thread_locks[MAX_SERVICE_THREADS];
static thread_ctx* g_thread_ctxs[MAX_SERVICE_THREADS];

static cf_mutex g_reaper_lock = CF_MUTEX_INIT;
static uint32_t g_n_slots;
static as_file_handle** g_file_handles;
static cf_queue g_free_slots;

static cf_mutex g_user_agents_db_lock = CF_MUTEX_INIT;
static cf_dyn_buf g_user_agents_db;


//==========================================================
// Forward declarations.
//

// Setup.
static void create_service_thread(uint32_t sid);
static void create_admin_thread(void);
static void add_localhost(cf_serv_cfg* serv_cfg, cf_sock_owner owner);

// Accept client connections.
static void* run_accept(void* udata);

// Assign connections to threads.
static void accept_service_connection(cf_sock_cfg* cfg, cf_socket* csock, cf_sock_addr* caddr);
static void accept_admin_connection(cf_sock_cfg* cfg, cf_socket* csock, cf_sock_addr* caddr);

static void assign_service_socket(as_file_handle* fd_h);
static void assign_admin_socket(as_file_handle* fd_h);

static uint32_t select_sid(void);
static uint32_t select_sid_pinned(cf_topo_cpu_index i_cpu);
static uint32_t select_sid_adq(cf_topo_napi_id id);
static uint32_t select_sid_keyd(const cf_digest* keyd);
static void schedule_redistribution(void);

// Demarshal requests.
static void* run_service(void* udata);
static void* run_admin(void* udata);

static bool handle_client_io_event(as_file_handle* fd_h, uint32_t mask, uint64_t events_ns);
static void stop_service(thread_ctx* ctx);
static void delete_file_handle(as_file_handle* fd_h);
static void release_file_handle(as_file_handle* fd_h);
static bool process_readable(as_file_handle* fd_h);
static void start_transaction(as_file_handle* fd_h);
static void config_xdr_socket(cf_socket* sock);

// Reap idle and bad connections.
static void start_reaper(void);
static void* run_reaper(void* udata);

// Transaction queue.
static bool start_internal_transaction(thread_ctx* ctx);

// User agent handling.
static uint32_t ua_hash_fn(const void* key);
static int ua_reduce_fn(const void* key, void* value, void* udata);
static void ua_increment_count(cf_shash* uah, const user_agent_key* key);


//==========================================================
// Inlines & macros.
//

static inline as_file_handle*
init_file_handle(cf_socket* sock, cf_sock_addr* caddr, cf_poll_data_type type)
{
	as_file_handle* fd_h = cf_rc_alloc(sizeof(as_file_handle));

	*fd_h = (as_file_handle) {
		.poll_data_type = type,
		.last_used = cf_getns(),
		.proto_unread = sizeof(as_proto),
		.security_filter = as_security_filter_create(),
	};

	cf_sock_addr_to_string_safe(caddr, fd_h->client, sizeof(fd_h->client));
	cf_socket_copy(sock, &fd_h->sock);

	return fd_h;
}

static inline void
rearm(as_file_handle* fd_h, uint32_t events)
{
	cf_poll_modify_socket(fd_h->poll, &fd_h->sock,
			events | EPOLLONESHOT | EPOLLRDHUP, fd_h);
}


//==========================================================
// Public API.
//

void
as_service_init(void)
{
	cf_dyn_buf_init_heap(&g_user_agents_db, 256 * 1024);

	// Create epoll instances and service threads.

	cf_info(AS_SERVICE, "starting %u service threads",
			g_config.n_service_threads);

	for (uint32_t i = 0; i < MAX_SERVICE_THREADS; i++) {
		cf_mutex_init(&g_thread_locks[i]);
	}

	for (uint32_t i = 0; i < g_config.n_service_threads; i++) {
		create_service_thread(i);
	}
}

void
as_service_start(void)
{
	start_reaper();

	// Create listening sockets.

	if (! g_config.service_localhost_disabled) {
		add_localhost(&g_service_bind, CF_SOCK_OWNER_SERVICE);
		add_localhost(&g_service_bind, CF_SOCK_OWNER_SERVICE_TLS);
	}

	if (cf_socket_init_server(&g_service_bind, &g_sockets) < 0) {
		cf_crash(AS_SERVICE, "couldn't initialize service socket");
	}

	cf_socket_show_server(AS_SERVICE, "client", &g_sockets);

	// Create accept thread.

	cf_info(AS_SERVICE, "starting accept thread");

	cf_thread_create_detached(run_accept, NULL);
}

void
as_service_set_threads(uint32_t n_threads)
{
	uint32_t old_n_threads = g_config.n_service_threads;

	if (n_threads > old_n_threads) {
		for (uint32_t sid = old_n_threads; sid < n_threads; sid++) {
			create_service_thread(sid);
		}

		g_config.n_service_threads = n_threads;

		schedule_redistribution();
	}
	else if (n_threads < old_n_threads) {
		g_config.n_service_threads = n_threads;

		for (uint32_t sid = n_threads; sid < old_n_threads; sid++) {
			cf_mutex_lock(&g_thread_locks[sid]);

			thread_ctx* ctx = g_thread_ctxs[sid];

			cf_detail(AS_SERVICE, "sending terminator sid %u ctx %p", sid, ctx);

			as_transaction tr;
			as_transaction_init_head(&tr, NULL, NULL);

			cf_epoll_queue_push(&ctx->trans_q, &tr);
			g_thread_ctxs[sid] = NULL;

			cf_mutex_unlock(&g_thread_locks[sid]);
		}
	}
}

bool
as_service_set_proto_fd_max(uint32_t val)
{
	struct rlimit rl;

	if (getrlimit(RLIMIT_NOFILE, &rl) < 0) {
		cf_crash(AS_SERVICE, "getrlimit() failed: %s", cf_strerror(errno));
	}

	if (val > (uint32_t)rl.rlim_cur) {
		cf_warning(AS_SERVICE, "can't set proto-fd-max %u > system limit %lu",
				val, rl.rlim_cur);
		return false;
	}

	if (val <= g_n_slots) {
		g_config.n_proto_fd_max = val;
		return true; // never shrink slots
	}

	size_t old_sz = g_n_slots * sizeof(as_file_handle*);
	size_t new_sz = val * sizeof(as_file_handle*);

	cf_mutex_lock(&g_reaper_lock);

	g_file_handles = cf_realloc(g_file_handles, new_sz);
	memset((uint8_t*)g_file_handles + old_sz, 0, new_sz - old_sz);

	for (uint32_t i = g_n_slots; i < val; i++) {
		cf_queue_push(&g_free_slots, &i);
	}

	g_n_slots = val;

	cf_mutex_unlock(&g_reaper_lock);

	g_config.n_proto_fd_max = val; // set *after* expanding slots

	return true;
}

void
as_service_rearm(as_file_handle* fd_h)
{
	if (fd_h->move_me) {
		cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
		assign_service_socket(fd_h); // rearms (EPOLLIN)

		fd_h->move_me = false;
		return;
	}

	rearm(fd_h, EPOLLIN);
}

void
as_service_enqueue_internal(as_transaction* tr)
{
	while (true) {
		uint32_t sid = as_config_is_cpu_pinned() ?
				select_sid_pinned(cf_topo_current_cpu()) : select_sid();

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			cf_epoll_queue_push(&ctx->trans_q, tr);
			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}

void
as_service_enqueue_internal_keyd(as_transaction* tr)
{
	while (true) {
		uint32_t sid = select_sid_keyd(&tr->keyd);

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			cf_epoll_queue_push(&ctx->trans_q, tr);
			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}

void
as_service_get_user_agents(cf_dyn_buf* db)
{
	cf_mutex_lock(&g_user_agents_db_lock);
	cf_dyn_buf_append_buf(db, g_user_agents_db.buf, g_user_agents_db.used_sz);
	cf_mutex_unlock(&g_user_agents_db_lock);
}

void
as_admin_init(void)
{
	if (g_admin_bind.n_cfgs == 0) {
		return;
	}

	create_admin_thread();
}

void
as_admin_start(void)
{
	if (g_admin_bind.n_cfgs == 0) {
		return;
	}

	if (! g_config.admin_localhost_disabled) {
		add_localhost(&g_admin_bind, CF_SOCK_OWNER_ADMIN);
		add_localhost(&g_admin_bind, CF_SOCK_OWNER_ADMIN_TLS);
	}

	if (cf_socket_init_server(&g_admin_bind, &g_admin_sockets) < 0) {
		cf_crash(AS_SERVICE, "couldn't initialize admin socket");
	}

	cf_socket_show_server(AS_SERVICE, "admin", &g_admin_sockets);
}


//==========================================================
// Local helpers - setup.
//

static void
create_service_thread(uint32_t sid)
{
	thread_ctx* ctx = cf_malloc(sizeof(thread_ctx));

	cf_detail(AS_SERVICE, "starting sid %u ctx %p", sid, ctx);

	if (as_config_is_cpu_pinned()) {
		ctx->i_cpu = (cf_topo_cpu_index)(sid % cf_topo_count_cpus());
	}

	ctx->lock = &g_thread_locks[sid];
	cf_poll_create(&ctx->poll);
	cf_epoll_queue_init(&ctx->trans_q, AS_TRANSACTION_HEAD_SIZE, 64);

	cf_thread_create_transient(run_service, ctx);

	cf_mutex_lock(&g_thread_locks[sid]);

	g_thread_ctxs[sid] = ctx;

	cf_mutex_unlock(&g_thread_locks[sid]);
}

static void
create_admin_thread(void)
{
	cf_poll_create(&g_admin_poll);

	cf_thread_create_detached(run_admin, &g_admin_poll);
}

static void
add_localhost(cf_serv_cfg* serv_cfg, cf_sock_owner owner)
{
	// Localhost will only be added to the addresses, if we're not yet listening
	// on wildcard ("any") or localhost.

	cf_ip_port port = 0;

	for (uint32_t i = 0; i < serv_cfg->n_cfgs; i++) {
		if (serv_cfg->cfgs[i].owner != owner) {
			continue;
		}

		port = serv_cfg->cfgs[i].port;

		if (cf_ip_addr_is_any(&serv_cfg->cfgs[i].addr) ||
				cf_ip_addr_is_local(&serv_cfg->cfgs[i].addr)) {
			return;
		}
	}

	if (port == 0) {
		return;
	}

	cf_sock_cfg sock_cfg;

	cf_sock_cfg_init(&sock_cfg, owner);
	sock_cfg.port = port;
	cf_ip_addr_set_local(&sock_cfg.addr);

	if (cf_serv_cfg_add_sock_cfg(serv_cfg, &sock_cfg) < 0) {
		cf_crash(AS_SERVICE, "couldn't add localhost listening address");
	}
}


//==========================================================
// Local helpers - accept client connections.
//

static void*
run_accept(void* udata)
{
	(void)udata;

	cf_poll poll;
	cf_poll_create(&poll);

	cf_poll_add_sockets(poll, &g_sockets, EPOLLIN);
	cf_poll_add_sockets(poll, &g_admin_sockets, EPOLLIN);

	while (true) {
		cf_poll_event events[N_EVENTS];
		int32_t n_events = cf_poll_wait(poll, events, N_EVENTS, -1);

		cf_assert(n_events >= 0, AS_SERVICE, "unexpected EINTR");

		for (uint32_t i = 0; i < (uint32_t)n_events; i++) {
			cf_socket* ssock = events[i].data;
			cf_socket csock;
			cf_sock_addr caddr;

			if (cf_socket_accept(ssock, &csock, &caddr) < 0) {
				if (errno == EMFILE || errno == ENFILE) {
					cf_ticker_warning(AS_SERVICE, "out of file descriptors");
					continue;
				}

				cf_crash(AS_SERVICE, "accept() failed: %d (%s)", errno,
						cf_strerror(errno));
			}

			cf_sock_cfg* cfg = ssock->cfg;

			if (cfg->owner == CF_SOCK_OWNER_SERVICE ||
					cfg->owner == CF_SOCK_OWNER_SERVICE_TLS) {
				accept_service_connection(cfg, &csock, &caddr);
			}
			else {
				accept_admin_connection(cfg, &csock, &caddr);
			}
		}
	}

	return NULL;
}


//==========================================================
// Local helpers - assign client connections to threads.
//

static void
accept_service_connection(cf_sock_cfg* cfg, cf_socket* csock,
		cf_sock_addr* caddr)
{
	// Ensure that proto_connections_closed is read first.
	uint64_t n_closed =
			as_load_uint64(&g_stats.proto_connections_closed);
	// TODO - ARM TSO plugin - will need barrier.
	uint64_t n_opened =
			as_load_uint64(&g_stats.proto_connections_opened);

	if (n_opened - n_closed >= g_config.n_proto_fd_max) {
		cf_ticker_warning(AS_SERVICE,
				"refusing client connection - proto-fd-max %u",
				g_config.n_proto_fd_max);

		cf_socket_close(csock);
		cf_socket_term(csock);
		return;
	}

	cf_socket_keep_alive(csock, 60, 60, 2);

	if (cfg->owner == CF_SOCK_OWNER_SERVICE_TLS) {
		tls_socket_prepare_server(csock, g_tls_service);
	}

	// Ref for epoll instance.
	as_file_handle* fd_h = init_file_handle(csock, caddr,
			CF_POLL_DATA_CLIENT_IO);

	cf_rc_reserve(fd_h); // ref for reaper

	cf_mutex_lock(&g_reaper_lock);

	uint32_t slot;

	if (cf_queue_pop(&g_free_slots, &slot, CF_QUEUE_NOWAIT) !=
			CF_QUEUE_OK) {
		cf_crash(AS_SERVICE, "cannot get free slot");
	}

	g_file_handles[slot] = fd_h;

	cf_mutex_unlock(&g_reaper_lock);

	assign_service_socket(fd_h); // arms (EPOLLIN)

	as_incr_uint64(&g_stats.proto_connections_opened);
}

static void
accept_admin_connection(cf_sock_cfg* cfg, cf_socket* csock, cf_sock_addr* caddr)
{
	// Ensure that admin_connections_closed is read first.
	uint64_t n_closed =
			as_load_uint64(&g_stats.admin_connections_closed);
	// TODO - ARM TSO plugin - will need barrier.
	uint64_t n_opened =
			as_load_uint64(&g_stats.admin_connections_opened);

	if (n_opened - n_closed >= MAX_ADMIN_CONNECTIONS) {
		cf_ticker_warning(AS_SERVICE,
				"refusing admin connection - breached connection limit of %u",
				MAX_ADMIN_CONNECTIONS);

		cf_socket_close(csock);
		cf_socket_term(csock);
		return;
	}

	cf_socket_keep_alive(csock, 60, 60, 2);

	if (cfg->owner == CF_SOCK_OWNER_ADMIN_TLS) {
		tls_socket_prepare_server(csock, g_tls_admin);
	}

	// Ref for epoll instance.
	as_file_handle* fd_h = init_file_handle(csock, caddr,
			CF_POLL_DATA_ADMIN_IO);

	assign_admin_socket(fd_h); // arms (EPOLLIN)

	as_incr_uint64(&g_stats.admin_connections_opened);
}

static void
assign_service_socket(as_file_handle* fd_h)
{
	while (true) {
		uint32_t sid;

		switch (g_config.auto_pin) {
		case CF_TOPO_AUTO_PIN_NONE:
			sid = select_sid();
			break;
		case CF_TOPO_AUTO_PIN_CPU:
		case CF_TOPO_AUTO_PIN_NUMA:
			sid = select_sid_pinned(cf_topo_socket_cpu(&fd_h->sock));
			break;
		case CF_TOPO_AUTO_PIN_ADQ:
			sid = select_sid_adq(cf_topo_socket_napi_id(&fd_h->sock));
			break;
		default:
			cf_crash(AS_SERVICE, "bad auto-pin %d", g_config.auto_pin);
			return;
		}

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			fd_h->poll = ctx->poll;

			cf_poll_add_socket(fd_h->poll, &fd_h->sock,
					EPOLLIN | EPOLLONESHOT | EPOLLRDHUP, fd_h);

			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}

static void
assign_admin_socket(as_file_handle* fd_h)
{
	fd_h->poll = g_admin_poll;

	cf_poll_add_socket(fd_h->poll, &fd_h->sock,
			EPOLLIN | EPOLLONESHOT | EPOLLRDHUP, fd_h);
}

static uint32_t
select_sid(void)
{
	static uint32_t rr = 0;

	return rr++ % g_config.n_service_threads;
}

static uint32_t
select_sid_pinned(cf_topo_cpu_index i_cpu)
{
	static uint32_t rr[CPU_SETSIZE] = { 0 };

	uint16_t n_cpus = cf_topo_count_cpus();
	uint32_t threads_per_cpu = g_config.n_service_threads / n_cpus;

	uint32_t thread_ix = rr[i_cpu]++ % threads_per_cpu;

	return (thread_ix * n_cpus) + i_cpu;
}

static uint32_t
select_sid_adq(cf_topo_napi_id id)
{
	return id == 0 ? select_sid() : id % g_config.n_service_threads;
}

static uint32_t
select_sid_keyd(const cf_digest* keyd)
{
	return as_partition_getid(keyd) % g_config.n_service_threads;
}

static void
schedule_redistribution(void)
{
	cf_mutex_lock(&g_reaper_lock);

	uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

	for (uint32_t i = 0; n_remaining != 0; i++) {
		as_file_handle* fd_h = g_file_handles[i];

		if (fd_h != NULL) {
			fd_h->move_me = true;
			n_remaining--;
		}
	}

	cf_mutex_unlock(&g_reaper_lock);
}


//==========================================================
// Local helpers - demarshal client requests.
//

static void*
run_service(void* udata)
{
	thread_ctx* ctx = (thread_ctx*)udata;

	cf_detail(AS_SERVICE, "running ctx %p", ctx);

	if (as_config_is_cpu_pinned()) {
		cf_topo_pin_to_cpu(ctx->i_cpu);
	}

	cf_poll poll = ctx->poll;
	cf_epoll_queue* trans_q = &ctx->trans_q;

	cf_poll_add_fd(poll, trans_q->event_fd, EPOLLIN, trans_q);
	as_xdr_init_poll(poll);

	while (true) {
		cf_poll_event events[N_EVENTS];
		int32_t n_events = cf_poll_wait(poll, events, N_EVENTS, -1);
		uint64_t events_ns = cf_getns();

		for (uint32_t i = 0; i < (uint32_t)n_events; i++) {
			uint32_t mask = events[i].events;
			void* data = events[i].data;

			uint8_t type = *(uint8_t*)data;

			if (type == CF_POLL_DATA_EPOLL_QUEUE) {
				cf_assert(mask == EPOLLIN, AS_SERVICE,
						"unexpected event: 0x%0x", mask);

				if (start_internal_transaction(ctx)) {
					continue;
				}

				stop_service(ctx);

				return NULL;
			}

			if (type == CF_POLL_DATA_XDR_IO) {
				as_xdr_io_event(mask, data);
				continue;
			}

			if (type == CF_POLL_DATA_XDR_TIMER) {
				as_xdr_timer_event(events, n_events, i);
				continue;
			}
			// else - type == CF_POLL_DATA_CLIENT_IO

			as_file_handle* fd_h = data;

			if (! handle_client_io_event(fd_h, mask, events_ns)) {
				continue;
			}

			// Note that epoll cannot trigger again for this file handle during
			// the transaction. We'll rearm at the end of the transaction.
			start_transaction(fd_h);
		}
	}

	return NULL;
}

static void*
run_admin(void* udata)
{
	cf_poll poll = *(cf_poll*)udata;

	while (true) {
		cf_poll_event events[N_EVENTS];
		int32_t n_events = cf_poll_wait(poll, events, N_EVENTS, -1);
		uint64_t events_ns = cf_getns();

		for (uint32_t i = 0; i < (uint32_t)n_events; i++) {
			uint32_t mask = events[i].events;
			as_file_handle* fd_h = events[i].data;

			if (! handle_client_io_event(fd_h, mask, events_ns)) {
				continue;
			}

			uint8_t type = fd_h->proto->type;

			if (type != PROTO_TYPE_INFO && type != PROTO_TYPE_SECURITY) {
				cf_warning(AS_SERVICE, "from %s - expected info or security type on admin port, got %u",
						fd_h->client, type);
				delete_file_handle(fd_h);
				continue;
			}

			// Note that epoll cannot trigger again for this file handle during
			// the transaction. We'll rearm at the end of the transaction.
			start_transaction(fd_h);
		}
	}

	return NULL;
}

static bool
handle_client_io_event(as_file_handle* fd_h, uint32_t mask, uint64_t events_ns)
{
	if ((mask & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) != 0) {
		delete_file_handle(fd_h);
		return false;
	}

	if (tls_socket_needs_handshake(&fd_h->sock)) {
		int32_t tls_ev = tls_socket_accept(&fd_h->sock);

		if (tls_ev == EPOLLERR) {
			delete_file_handle(fd_h);
			return false;
		}

		if (tls_ev == 0) {
			tls_socket_must_not_have_data(&fd_h->sock, "client handshake");
			tls_ev = EPOLLIN;
		}

		rearm(fd_h, (uint32_t)tls_ev);
		return false;
	}

	if (fd_h->proto == NULL && fd_h->proto_unread == sizeof(as_proto)) {
		// Overload last_used for request start time. Note - latency
		// will include unrelated events ahead of this one.
		fd_h->last_used = events_ns;
	}

	if (! process_readable(fd_h)) {
		delete_file_handle(fd_h);
		return false;
	}

	tls_socket_must_not_have_data(&fd_h->sock, "full client read");

	if (fd_h->proto_unread != 0) {
		rearm(fd_h, EPOLLIN);
		return false;
	}

	return true;
}

static void
stop_service(thread_ctx* ctx)
{
	cf_detail(AS_SERVICE, "stopping ctx %p", ctx);

	as_xdr_shutdown_poll();
	as_xdr_cleanup_tl_stats();

	while (true) {
		bool any_in_transaction = false;

		cf_mutex_lock(&g_reaper_lock);

		uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

		for (uint32_t i = 0; n_remaining != 0; i++) {
			as_file_handle* fd_h = g_file_handles[i];

			if (fd_h == NULL) {
				continue;
			}

			n_remaining--;

			// Ignore, if another thread's or INVALID_POLL.
			if (! cf_poll_equal(fd_h->poll, ctx->poll)) {
				continue;
			}

			// Don't transfer during TLS handshake - might need EPOLLOUT.
			if (tls_socket_needs_handshake(&fd_h->sock)) {
				delete_file_handle(fd_h);
				continue;
			}

			if (fd_h->in_transaction != 0) {
				any_in_transaction = true;
				continue;
			}

			cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
			assign_service_socket(fd_h); // keeps armed (EPOLLIN)
		}

		cf_mutex_unlock(&g_reaper_lock);

		if (! any_in_transaction) {
			break;
		}

		sleep(1);
	}

	cf_poll_destroy(ctx->poll);
	cf_epoll_queue_destroy(&ctx->trans_q);

	cf_detail(AS_SERVICE, "stopped ctx %p", ctx);

	cf_free(ctx);
}

static void
delete_file_handle(as_file_handle* fd_h)
{
	cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
	fd_h->poll = INVALID_POLL;
	fd_h->reap_me = true;
	release_file_handle(fd_h);
}

static void
release_file_handle(as_file_handle* proto_fd_h)
{
	if (cf_rc_release(proto_fd_h) != 0) {
		return;
	}

	cf_socket_close(&proto_fd_h->sock);
	cf_socket_term(&proto_fd_h->sock);

	if (proto_fd_h->proto != NULL) {
		cf_free(proto_fd_h->proto);
	}

	if (proto_fd_h->security_filter != NULL) {
		as_security_filter_destroy(proto_fd_h->security_filter);
	}

	if (proto_fd_h->poll_data_type == CF_POLL_DATA_CLIENT_IO) {
		as_incr_uint64_rls(&g_stats.proto_connections_closed);
	}
	else {
		as_incr_uint64_rls(&g_stats.admin_connections_closed);
	}

	cf_rc_free(proto_fd_h);
}

static bool
process_readable(as_file_handle* fd_h)
{
	uint8_t* end = fd_h->proto == NULL ?
			(uint8_t*)&fd_h->proto_hdr + sizeof(as_proto) : // header
			fd_h->proto->body + fd_h->proto->sz; // body

	while (true) {
		int32_t sz = cf_socket_recv(&fd_h->sock, end - fd_h->proto_unread,
				fd_h->proto_unread, 0);

		if (sz < 0) {
			return errno == EAGAIN || errno == EWOULDBLOCK;
		}

		if (sz == 0) {
			return false;
		}

		fd_h->proto_unread -= (uint64_t)sz;

		if (fd_h->proto_unread != 0) {
			continue; // drain socket (and OpenSSL's internal buffer) dry
		}

		if (fd_h->proto != NULL) {
			return true; // done with entire request
		}
		// else - switch from header to body.

		// Check for a TLS ClientHello arriving at a non-TLS socket. Heuristic:
		//   - tls[0] == ContentType.handshake (22)
		//   - tls[1] == ProtocolVersion.major (3)
		//   - tls[5] == HandshakeType.client_hello (1)

		uint8_t* tls = (uint8_t*)&fd_h->proto_hdr;

		if (tls[0] == 22 && tls[1] == 3 && tls[5] == 1) {
			cf_warning(AS_SERVICE, "ignoring TLS connection from %s",
					fd_h->client);
			return false;
		}

		// For backward compatibility, allow version 0 with security messages.
		if (fd_h->proto_hdr.version != PROTO_VERSION &&
				! (fd_h->proto_hdr.version == 0 &&
						fd_h->proto_hdr.type == PROTO_TYPE_SECURITY)) {
			cf_warning(AS_SERVICE, "unsupported proto version %d from %s",
					fd_h->proto_hdr.version, fd_h->client);
			return false;
		}

		if (! as_proto_is_valid_type(&fd_h->proto_hdr)) {
			cf_warning(AS_SERVICE, "unsupported proto type %d from %s",
					fd_h->proto_hdr.type, fd_h->client);
			return false;
		}

		as_proto_swap(&fd_h->proto_hdr);

		if (fd_h->proto_hdr.sz > PROTO_SIZE_MAX) {
			cf_warning(AS_SERVICE, "invalid proto size %lu from %s",
					(uint64_t)fd_h->proto_hdr.sz, fd_h->client);
			return false;
		}

		fd_h->proto = cf_malloc(sizeof(as_proto) + fd_h->proto_hdr.sz);
		memcpy(fd_h->proto, &fd_h->proto_hdr, sizeof(as_proto));

		fd_h->proto_unread = fd_h->proto->sz;
		end = fd_h->proto->body + fd_h->proto->sz;
	}
}

static void
start_transaction(as_file_handle* fd_h)
{
	// as_end_of_transaction() rearms then decrements, so this may be > 1.
	as_incr_uint32(&fd_h->in_transaction);

	uint64_t start_ns = fd_h->last_used;
	as_proto* proto = fd_h->proto;

	fd_h->proto = NULL;
	fd_h->proto_unread = sizeof(as_proto);

	if (proto->type == PROTO_TYPE_INFO) {
		as_info_transaction it = {
			.fd_h = fd_h,
			.proto = proto,
			.start_time = start_ns
		};

		as_info(&it);
		return;
	}

	as_transaction tr;
	as_transaction_init_head(&tr, NULL, (cl_msg*)proto);

	tr.origin = FROM_CLIENT;
	tr.from.proto_fd_h = fd_h;
	tr.start_time = start_ns;

	if (proto->type == PROTO_TYPE_SECURITY) {
		as_security_transact(&tr);
		return;
	}

	if (proto->type == PROTO_TYPE_AS_MSG_COMPRESSED) {
		uint32_t result = as_proto_uncompress((as_comp_proto*)proto,
				(as_proto**)&tr.msgp);

		if (result != AS_OK) {
			as_transaction_demarshal_error(&tr, result);
			return;
		}

		cf_free(proto);
	}

	if (as_transaction_is_xdr(&tr) && ! fd_h->is_xdr) {
		config_xdr_socket(&fd_h->sock);
		fd_h->is_xdr = true;
	}

	if (tr.msgp->msg.info1 & AS_MSG_INFO1_BATCH) {
		as_batch_queue_task(&tr);
		return;
	}

	if (! as_transaction_prepare(&tr, true)) {
		as_transaction_demarshal_error(&tr, AS_ERR_PARAMETER);
		return;
	}

	as_tsvc_process_transaction(&tr);
}

static void
config_xdr_socket(cf_socket* sock)
{
	cf_socket_set_receive_buffer(sock, XDR_READ_BUFFER_SIZE);
	cf_socket_set_send_buffer(sock, XDR_WRITE_BUFFER_SIZE);
	cf_socket_set_window(sock, XDR_READ_BUFFER_SIZE);
	cf_socket_enable_nagle(sock);
}


//==========================================================
// Local helpers - reap idle and bad connections.
//

static void
start_reaper(void)
{
	g_n_slots = g_config.n_proto_fd_max;
	g_file_handles = cf_calloc(g_n_slots, sizeof(as_file_handle*));

	cf_queue_init(&g_free_slots, sizeof(uint32_t), g_n_slots, false);

	for (uint32_t i = 0; i < g_n_slots; i++) {
		cf_queue_push(&g_free_slots, &i);
	}

	cf_info(AS_SERVICE, "starting reaper thread");

	cf_thread_create_detached(run_reaper, NULL);
}

static void*
run_reaper(void* udata)
{
	(void)udata;
	user_agent_key unknownkey = { .size = 12, .b64data = "dW5rbm93bg==" };
	cf_shash* ua_hash = cf_shash_create(ua_hash_fn, sizeof(user_agent_key),
		sizeof(uint32_t), 512, false);

	while (true) {
		sleep(1);

		bool security_refresh = as_security_should_refresh();

		uint64_t kill_ns = (uint64_t)g_config.proto_fd_idle_ms * 1000000;
		uint64_t now_ns = cf_getns();

		cf_mutex_lock(&g_reaper_lock);

		uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

		for (uint32_t i = 0; n_remaining != 0; i++) {
			as_file_handle* fd_h = g_file_handles[i];

			if (fd_h == NULL) {
				continue;
			}

			n_remaining--;

			if (security_refresh) {
				as_security_refresh(fd_h);
			}

			// reap_me overrides in_transaction.
			if (fd_h->reap_me) {
				g_file_handles[i] = NULL;
				cf_queue_push_head(&g_free_slots, &i);
				release_file_handle(fd_h);
				continue;
			}

			if (! fd_h->in_transaction && kill_ns != 0 &&
					fd_h->last_used + kill_ns < now_ns) {
				cf_socket_shutdown(&fd_h->sock); // will trigger epoll errors

				g_file_handles[i] = NULL;
				cf_queue_push_head(&g_free_slots, &i);
				release_file_handle(fd_h);

				g_stats.reaper_count++;
				continue;
			}

			if (fd_h->user_agent.size > 0) {
				ua_increment_count(ua_hash, &fd_h->user_agent);
			}
			else if (fd_h->called_features) {
				ua_increment_count(ua_hash, &unknownkey);
			}
		}

		cf_mutex_unlock(&g_reaper_lock);

		cf_mutex_lock(&g_user_agents_db_lock);
		g_user_agents_db.used_sz = 0;
		cf_shash_reduce(ua_hash, ua_reduce_fn, &g_user_agents_db);
		cf_mutex_unlock(&g_user_agents_db_lock);
	}

	return NULL;
}


//==========================================================
// Local helpers - transaction queue.
//

static bool
start_internal_transaction(thread_ctx* ctx)
{
	as_transaction tr;

	cf_mutex_lock(ctx->lock);

	if (! cf_epoll_queue_pop(&ctx->trans_q, &tr)) {
		cf_crash(AS_SERVICE, "unable to pop from transaction queue");
	}

	cf_mutex_unlock(ctx->lock);

	if (tr.msgp == NULL) {
		return false;
	}

	as_tsvc_process_transaction(&tr);

	return true;
}

static void
ua_increment_count(cf_shash* uah, const user_agent_key* key)
{
	uint32_t count = 1;
	if (cf_shash_get(uah, key, &count) == CF_SHASH_OK) {
		count++;
	}
	cf_shash_put(uah, key, &count);
}

static uint32_t
ua_hash_fn(const void* key)
{
	const user_agent_key* ua_key = (const user_agent_key*)key;
	return cf_wyhash32(ua_key->b64data, ua_key->size);
}

static int
ua_reduce_fn(const void* key, void* value, void* udata)
{
	const user_agent_key* ua_key = (const user_agent_key*)key;
	const uint32_t* ua_count = (const uint32_t*)value;
	cf_dyn_buf* db = (cf_dyn_buf*)udata;

	cf_dyn_buf_append_string(db, "user-agent=");
	cf_dyn_buf_append_buf(db, ua_key->b64data, ua_key->size);
	cf_dyn_buf_append_char(db, ':');
	cf_dyn_buf_append_string(db, "count=");
	cf_dyn_buf_append_uint32(db, *ua_count);
	cf_dyn_buf_append_char(db, ';');

	return CF_SHASH_REDUCE_DELETE;
}
