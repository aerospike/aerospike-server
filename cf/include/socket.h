/*
 * socket.h
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

#pragma once

#include <alloca.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "fault.h"
#include "msg.h"
#include "node.h"

// Use forward declaration instead of including openssl/ssl.h here.
struct ssl_st;

#define CF_SOCKET_TIMEOUT 10000
#define CF_SOCK_CFG_MAX 250

// Accesses the socket file descriptor as an rvalue, i.e., the socket file descriptor
// cannot be modified.
#define CSFD(sock) ((int32_t)((sock)->fd))

// CSFD() for epoll file descriptors.
#define CEFD(poll) ((int32_t)(poll).fd)

// Like CEFD(), but produces an lvalue, i.e., the epoll file descriptor can be modified.
#define EFD(poll) ((poll).fd)

#define cf_ip_addr_print(_addr) ({ \
	char *_tmp = alloca(250); \
	cf_ip_addr_to_string_safe(_addr, _tmp, 250); \
	_tmp; \
})

#define cf_ip_addr_print_multi(_addrs, _n_addrs) ({ \
	char *_tmp = alloca(2500); \
	cf_ip_addr_to_string_multi_safe(_addrs, _n_addrs, _tmp, 2500); \
	_tmp; \
})

#define cf_ip_port_print(_port) ({ \
	char *_tmp = alloca(25); \
	cf_ip_port_to_string_safe(_port, _tmp, 25); \
	_tmp; \
})

#define cf_sock_addr_print(_addr) ({ \
	char *_tmp = alloca(250); \
	cf_sock_addr_to_string_safe(_addr, _tmp, 250); \
	_tmp; \
})

typedef struct cf_ip_addr_s {
	sa_family_t family;

	union {
		struct in_addr v4;
		struct in6_addr v6;
	};
} cf_ip_addr;

typedef uint16_t cf_ip_port;

typedef struct cf_addr_list_s {
	uint32_t n_addrs;
	const char *addrs[CF_SOCK_CFG_MAX];
} cf_addr_list;

typedef struct cf_serv_spec_s {
	cf_ip_port bind_port;
	cf_addr_list bind;
	cf_ip_port std_port;
	cf_addr_list std;
	cf_ip_port alt_port;
	cf_addr_list alt;
	char *tls_our_name;
	uint32_t n_tls_peer_names;
	char *tls_peer_names[CF_SOCK_CFG_MAX];
} cf_serv_spec;

typedef struct cf_sock_addr_s {
	cf_ip_addr addr;
	cf_ip_port port;
} cf_sock_addr;

typedef enum {
	CF_SOCKET_STATE_NON_TLS,
	CF_SOCKET_STATE_TLS_HANDSHAKE,
	CF_SOCKET_STATE_TLS_READY
} cf_socket_state;

typedef struct cf_socket_s {
	int32_t fd;
	cf_socket_state state;
	void *cfg;
	struct ssl_st *ssl;
} cf_socket;

typedef struct cf_sockets_s {
	uint32_t n_socks;
	cf_socket socks[CF_SOCK_CFG_MAX];
} cf_sockets;

typedef enum {
	CF_SOCK_OWNER_SERVICE,
	CF_SOCK_OWNER_SERVICE_TLS,
	CF_SOCK_OWNER_HEARTBEAT,
	CF_SOCK_OWNER_HEARTBEAT_TLS,
	CF_SOCK_OWNER_FABRIC,
	CF_SOCK_OWNER_FABRIC_TLS,
	CF_SOCK_OWNER_INFO,
	CF_SOCK_OWNER_XDR,
	CF_SOCK_OWNER_INVALID
} cf_sock_owner;

typedef struct cf_sock_cfg_s {
	cf_sock_owner owner;
	cf_ip_port port;
	cf_ip_addr addr;
} cf_sock_cfg;

typedef struct cf_serv_cfg_s {
	uint32_t n_cfgs;
	cf_sock_cfg cfgs[CF_SOCK_CFG_MAX];
} cf_serv_cfg;

typedef struct cf_poll_s {
	int32_t fd;
} __attribute__((packed)) cf_poll;

// This precisely matches the epoll_event struct.
typedef struct cf_poll_event_s {
	uint32_t events;
	void *data;
} __attribute__((packed)) cf_poll_event;

typedef struct cf_msock_cfg_s {
	cf_sock_owner owner;
	cf_ip_port port;
	cf_ip_addr addr;
	cf_ip_addr if_addr;
	uint8_t ttl;
} cf_msock_cfg;

typedef struct cf_mserv_cfg_s {
	uint32_t n_cfgs;
	cf_msock_cfg cfgs[CF_SOCK_CFG_MAX];
} cf_mserv_cfg;

void cf_socket_set_advertise_ipv6(bool advertise);
bool cf_socket_advertises_ipv6(void);

CF_MUST_CHECK int32_t cf_ip_addr_from_string(const char *string, cf_ip_addr *addr);
CF_MUST_CHECK int32_t cf_ip_addr_from_string_multi(const char *string, cf_ip_addr *addrs, uint32_t *n_addrs);
CF_MUST_CHECK int32_t cf_ip_addr_to_string(const cf_ip_addr *addr, char *string, size_t size);
void cf_ip_addr_to_string_safe(const cf_ip_addr *addr, char *string, size_t size);
CF_MUST_CHECK int32_t cf_ip_addr_to_string_multi(const cf_ip_addr *addrs, uint32_t n_addrs, char *string, size_t size);
void cf_ip_addr_to_string_multi_safe(const cf_ip_addr *addrs, uint32_t n_addrs, char *string, size_t size);
CF_MUST_CHECK int32_t cf_ip_addr_from_binary(const uint8_t *binary, size_t size, cf_ip_addr *addr);
CF_MUST_CHECK int32_t cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size);
void cf_ip_addr_to_rack_aware_id(const cf_ip_addr *addr, uint32_t *id);

CF_MUST_CHECK int32_t cf_ip_addr_compare(const cf_ip_addr *lhs, const cf_ip_addr *rhs);
void cf_ip_addr_copy(const cf_ip_addr *from, cf_ip_addr *to);
void cf_ip_addr_sort(cf_ip_addr *addrs, uint32_t n_addrs);

bool cf_ip_addr_is_dns_name(const char *string);
bool cf_ip_addr_str_is_legacy(const char *string);
bool cf_ip_addr_is_legacy(const cf_ip_addr *addr);
bool cf_ip_addr_legacy_only(void);

void cf_ip_addr_set_local(cf_ip_addr *addr);
CF_MUST_CHECK bool cf_ip_addr_is_local(const cf_ip_addr *addr);

void cf_ip_addr_set_any(cf_ip_addr *addr);
CF_MUST_CHECK bool cf_ip_addr_is_any(const cf_ip_addr *addr);

CF_MUST_CHECK int32_t cf_ip_port_from_string(const char *string, cf_ip_port *port);
CF_MUST_CHECK int32_t cf_ip_port_to_string(cf_ip_port port, char *string, size_t size);
void cf_ip_port_to_string_safe(cf_ip_port port, char *string, size_t size);
CF_MUST_CHECK int32_t cf_ip_port_from_binary(const uint8_t *binary, size_t size, cf_ip_port *port);
CF_MUST_CHECK int32_t cf_ip_port_to_binary(cf_ip_port port, uint8_t *binary, size_t size);
void cf_ip_port_from_node_id(cf_node id, cf_ip_port *port);

CF_MUST_CHECK int32_t cf_sock_addr_from_string(const char *string, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_sock_addr_to_string(const cf_sock_addr *addr, char *string, size_t size);
void cf_sock_addr_to_string_safe(const cf_sock_addr *addr, char *string, size_t size);
CF_MUST_CHECK int32_t cf_sock_addr_from_binary(const uint8_t *binary, size_t size, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_sock_addr_to_binary(const cf_sock_addr *addr, uint8_t *binary, size_t size);

CF_MUST_CHECK int32_t cf_sock_addr_from_host_port(const char *host, cf_ip_port port, cf_sock_addr *addr);
void cf_sock_addr_from_addr_port(const cf_ip_addr *ip_addr, cf_ip_port port, cf_sock_addr *addr);

CF_MUST_CHECK int32_t cf_sock_addr_compare(const cf_sock_addr *lhs, const cf_sock_addr *rhs);
void cf_sock_addr_copy(const cf_sock_addr *from, cf_sock_addr *to);

void cf_sock_addr_from_native(const struct sockaddr *native, cf_sock_addr *addr);
void cf_sock_addr_to_native(const cf_sock_addr *addr, struct sockaddr *native);

void cf_sock_addr_set_any(cf_sock_addr *addr);
CF_MUST_CHECK bool cf_sock_addr_is_any(const cf_sock_addr *addr);

void cf_sock_cfg_init(cf_sock_cfg *cfg, cf_sock_owner owner);
void cf_sock_cfg_copy(const cf_sock_cfg *from, cf_sock_cfg *to);

void cf_serv_cfg_init(cf_serv_cfg *cfg);
CF_MUST_CHECK int32_t cf_serv_cfg_add_sock_cfg(cf_serv_cfg *serv_cfg, const cf_sock_cfg *sock_cfg);

void cf_sockets_init(cf_sockets *socks);
CF_MUST_CHECK bool cf_sockets_has_socket(const cf_sockets *socks, const cf_socket *sock);
void cf_sockets_close(cf_sockets *socks);

void cf_fd_disable_blocking(int32_t fd);

void cf_socket_disable_blocking(cf_socket *sock);
void cf_socket_enable_blocking(cf_socket *sock);
void cf_socket_disable_nagle(cf_socket *sock);
void cf_socket_enable_nagle(cf_socket *sock);
void cf_socket_keep_alive(cf_socket *sock, int32_t idle, int32_t interval, int32_t count);
void cf_socket_set_send_buffer(cf_socket *sock, int32_t size);
void cf_socket_set_receive_buffer(cf_socket *sock, int32_t size);
void cf_socket_set_window(cf_socket *sock, int32_t size);

void cf_socket_init(cf_socket *sock);
bool cf_socket_exists(cf_socket *sock);

static inline void cf_socket_copy(const cf_socket *from, cf_socket *to)
{
	to->fd = from->fd;
	to->state = from->state;
	to->cfg = from->cfg;
	to->ssl = from->ssl;
}

CF_MUST_CHECK int32_t cf_socket_init_server(cf_serv_cfg *cfg, cf_sockets *socks);
void cf_socket_show_server(cf_fault_context cont, const char *tag, const cf_sockets *socks);
CF_MUST_CHECK int32_t cf_socket_init_client(cf_sock_cfg *cfg, int32_t timeout, cf_socket *sock);

CF_MUST_CHECK int32_t cf_socket_accept(cf_socket *lsock, cf_socket *sock, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_socket_remote_name(const cf_socket *sock, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_socket_local_name(const cf_socket *sock, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_socket_available(cf_socket *sock);

CF_MUST_CHECK int32_t cf_socket_recv_from(cf_socket *sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_socket_recv(cf_socket *sock, void *buff, size_t size, int32_t flags);
CF_MUST_CHECK int32_t cf_socket_send_to(cf_socket *sock, const void *buff, size_t size, int32_t flags, const cf_sock_addr *addr);
CF_MUST_CHECK int32_t cf_socket_send(cf_socket *sock, const void *buff, size_t size, int32_t flags);

CF_MUST_CHECK int32_t cf_socket_recv_from_all(cf_socket *sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr, int32_t timeout);
CF_MUST_CHECK int32_t cf_socket_recv_all(cf_socket *sock, void *buff, size_t size, int32_t flags, int32_t timeout);
CF_MUST_CHECK int32_t cf_socket_send_to_all(cf_socket *sock, const void *buff, size_t size, int32_t flags, const cf_sock_addr *addr, int32_t timeout);
CF_MUST_CHECK int32_t cf_socket_send_all(cf_socket *sock, const void *buff, size_t size, int32_t flags, int32_t timeout);

void cf_socket_write_shutdown(cf_socket *sock);
void cf_socket_shutdown(cf_socket *sock);
void cf_socket_close(cf_socket *sock);
void cf_socket_drain_close(cf_socket *sock);
void cf_socket_term(cf_socket *sock);

void cf_msock_cfg_init(cf_msock_cfg *cfg, cf_sock_owner owner);
void cf_msock_cfg_copy(const cf_msock_cfg *from, cf_msock_cfg *to);

void cf_mserv_cfg_init(cf_mserv_cfg *cfg);
CF_MUST_CHECK int32_t cf_mserv_cfg_add_msock_cfg(cf_mserv_cfg *serv_cfg, const cf_msock_cfg *sock_cfg);
CF_MUST_CHECK int32_t cf_mserv_cfg_add_combo(cf_mserv_cfg *serv_cfg, cf_sock_owner owner, cf_ip_port port, cf_ip_addr *addr, cf_ip_addr *if_addr, uint8_t ttl);

CF_MUST_CHECK int32_t cf_socket_mcast_init(cf_mserv_cfg *cfg, cf_sockets *socks);
void cf_socket_mcast_show(cf_fault_context cont, const char *tag, const cf_sockets *socks);
CF_MUST_CHECK int32_t cf_socket_mcast_set_inter(cf_socket *sock, const cf_ip_addr *iaddr);
CF_MUST_CHECK int32_t cf_socket_mcast_set_ttl(cf_socket *sock, int32_t ttl);
CF_MUST_CHECK int32_t cf_socket_mcast_join_group(cf_socket *sock, const cf_ip_addr *iaddr, const cf_ip_addr *gaddr);

void cf_poll_create(cf_poll *poll);
void cf_poll_add_fd(cf_poll poll, int32_t fd, uint32_t events, void *data);
void cf_poll_add_socket(cf_poll poll, const cf_socket *sock, uint32_t events, void *data);
CF_MUST_CHECK int32_t cf_poll_modify_socket_forgiving(cf_poll poll, const cf_socket *sock, uint32_t events, void *data, uint32_t n_err_ok, int32_t *err_ok);
CF_MUST_CHECK int32_t cf_poll_delete_socket_forgiving(cf_poll poll, const cf_socket *sock, uint32_t n_err_ok, int32_t *err_ok);
void cf_poll_add_sockets(cf_poll poll, cf_sockets *socks, uint32_t events);
void cf_poll_delete_sockets(cf_poll poll, cf_sockets *socks);
CF_MUST_CHECK int32_t cf_poll_wait(cf_poll poll, cf_poll_event *events, int32_t limit, int32_t timeout);
void cf_poll_destroy(cf_poll poll);

static inline void cf_poll_modify_socket(cf_poll poll, const cf_socket *sock, uint32_t events, void *data)
{
	CF_IGNORE_ERROR(cf_poll_modify_socket_forgiving(poll, sock, events, data, 0, NULL));
}

static inline void cf_poll_delete_socket(cf_poll poll, const cf_socket *sock)
{
	CF_IGNORE_ERROR(cf_poll_delete_socket_forgiving(poll, sock, 0, NULL));
}

CF_MUST_CHECK int32_t cf_inter_get_addr_all(cf_ip_addr *addrs, uint32_t *n_addrs);
CF_MUST_CHECK int32_t cf_inter_get_addr_all_legacy(cf_ip_addr *addrs, uint32_t *n_addrs);
CF_MUST_CHECK int32_t cf_inter_get_addr_def(cf_ip_addr *addrs, uint32_t *n_addrs);
CF_MUST_CHECK int32_t cf_inter_get_addr_def_legacy(cf_ip_addr *addrs, uint32_t *n_addrs);
CF_MUST_CHECK int32_t cf_inter_get_addr_name(cf_ip_addr *addrs, uint32_t *n_addrs, const char *if_name);
bool cf_inter_is_inter_name(const char *if_name);
CF_MUST_CHECK int32_t cf_inter_addr_to_index_and_name(const cf_ip_addr *addr, int32_t *index, char **name);
void cf_inter_expand_bond(const char *if_name, char **out_names, uint32_t *n_out);
CF_MUST_CHECK int32_t cf_inter_mtu(const cf_ip_addr *inter_addr);
CF_MUST_CHECK int32_t cf_inter_min_mtu(void);
bool cf_inter_detect_changes(cf_ip_addr *addrs, uint32_t *n_addrs, uint32_t limit);
bool cf_inter_detect_changes_legacy(cf_ip_addr *addrs, uint32_t *n_addrs, uint32_t limit);

CF_MUST_CHECK int32_t cf_node_id_get(cf_ip_port port, const char *if_hint, cf_node *id);

#if defined CF_SOCKET_PRIVATE
CF_MUST_CHECK size_t cf_socket_addr_len(const struct sockaddr* sa);
CF_MUST_CHECK int32_t cf_socket_parse_netlink(bool allow_v6, uint32_t family, uint32_t flags,
		const void *data, size_t len, cf_ip_addr *addr);
void cf_socket_fix_client(cf_socket *sock);
void cf_socket_fix_bind(cf_serv_cfg *serv_cfg);
void cf_socket_fix_server(cf_socket *sock);
#endif
