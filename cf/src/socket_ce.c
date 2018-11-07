/*
 * socket_ce.c
 *
 * Copyright (C) 2016-2018 Aerospike, Inc.
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

#define CF_SOCKET_PRIVATE
#include "socket.h"

#include <errno.h>
#include <netdb.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "dns.h"
#include "fault.h"

#include "citrusleaf/alloc.h"

addrinfo g_cf_ip_addr_dns_hints = { .ai_flags = 0, .ai_family = AF_INET };

static char *
safe_strndup(const char *string, size_t length)
{
	char *res = cf_strndup(string, length);

	if (res == NULL) {
		cf_crash(CF_SOCKET, "Out of memory");
	}

	return res;
}

void
cf_socket_set_advertise_ipv6(bool advertise)
{
	cf_warning(CF_SOCKET, "'advertise-ipv6' is relevant for enterprise only");
}

bool
cf_socket_advertises_ipv6(void)
{
	return false;
}

int32_t
cf_ip_addr_from_addrinfo(const char *name, const addrinfo *info,
		cf_ip_addr *addrs, uint32_t *n_addrs)
{
	uint32_t i = 0;

	for (const addrinfo *walker = info; walker != NULL;
			walker = walker->ai_next) {
		if (walker->ai_socktype == SOCK_STREAM) {
			if (i >= *n_addrs) {
				cf_warning(CF_SOCKET, "Too many IP addresses for '%s'", name);
				return -1;
			}

			struct sockaddr_in *sai = (struct sockaddr_in *)walker->ai_addr;
			addrs[i].v4 = sai->sin_addr;
			++i;
		}
	}

	if (i == 0) {
		cf_warning(CF_SOCKET, "No valid addresses for '%s'", name);
		return -1;
	}

	cf_ip_addr_sort(addrs, i);
	*n_addrs = i;
	return 0;
}

bool
cf_ip_addr_str_is_legacy(const char *string)
{
	(void)string;
	return true;
}

bool
cf_ip_addr_is_legacy(const cf_ip_addr* addr)
{
	(void)addr;
	return true;
}

bool
cf_ip_addr_legacy_only(void)
{
	return true;
}

int32_t
cf_ip_addr_to_string(const cf_ip_addr *addr, char *string, size_t size)
{
	if (inet_ntop(AF_INET, &addr->v4, string, size) == NULL) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	return strlen(string);
}

int32_t
cf_ip_addr_from_binary(const uint8_t *binary, size_t size, cf_ip_addr *addr)
{
	if (size != 4) {
		cf_debug(CF_SOCKET, "Input buffer size incorrect.");
		return -1;
	}

	memcpy(&addr->v4, binary, 4);
	return 4;
}

int32_t
cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size)
{
	if (size < 4) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	memcpy(binary, &addr->v4, 4);
	return 4;
}

void
cf_ip_addr_to_rack_aware_id(const cf_ip_addr *addr, uint32_t *id)
{
	*id = ntohl(addr->v4.s_addr);
}

int32_t
cf_ip_addr_compare(const cf_ip_addr *lhs, const cf_ip_addr *rhs)
{
	return memcmp(&lhs->v4, &rhs->v4, 4);
}

void
cf_ip_addr_copy(const cf_ip_addr *from, cf_ip_addr *to)
{
	to->v4 = from->v4;
}

void
cf_ip_addr_set_local(cf_ip_addr *addr)
{
	addr->v4.s_addr = htonl(0x7f000001);
}

bool
cf_ip_addr_is_local(const cf_ip_addr *addr)
{
	return (ntohl(addr->v4.s_addr) & 0xff000000) == 0x7f000000;
}

void
cf_ip_addr_set_any(cf_ip_addr *addr)
{
	addr->v4.s_addr = 0;
}

bool
cf_ip_addr_is_any(const cf_ip_addr *addr)
{
	return addr->v4.s_addr == 0;
}

int32_t
cf_sock_addr_to_string(const cf_sock_addr *addr, char *string, size_t size)
{
	int32_t total = 0;
	int32_t count = cf_ip_addr_to_string(&addr->addr, string, size);

	if (count < 0) {
		return -1;
	}

	total += count;

	if (size - total < 2) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	string[total++] = ':';
	string[total] = 0;

	count = cf_ip_port_to_string(addr->port, string + total, size - total);

	if (count < 0) {
		return -1;
	}

	total += count;
	return total;
}

int32_t
cf_sock_addr_from_string(const char *string, cf_sock_addr *addr)
{
	int32_t res = -1;
	const char *colon = strchr(string, ':');

	if (colon == NULL) {
		cf_warning(CF_SOCKET, "Missing ':' in socket address '%s'", string);
		goto cleanup0;
	}

	const char *host = safe_strndup(string, colon - string);

	if (cf_ip_addr_from_string(host, &addr->addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid host address '%s' in socket address '%s'", host, string);
		goto cleanup1;
	}

	if (cf_ip_port_from_string(colon + 1, &addr->port) < 0) {
		cf_warning(CF_SOCKET, "Invalid port '%s' in socket address '%s'", colon + 1, string);
		goto cleanup1;
	}

	res = 0;

cleanup1:
	cf_free((void *)host);

cleanup0:
	return res;
}

void
cf_sock_addr_from_native(const struct sockaddr *native, cf_sock_addr *addr)
{
	if (native->sa_family != AF_INET) {
		cf_crash(CF_SOCKET, "Invalid address family: %d", native->sa_family);
	}

	struct sockaddr_in *sai = (struct sockaddr_in *)native;
	addr->addr.v4 = sai->sin_addr;
	addr->port = ntohs(sai->sin_port);
}

void
cf_sock_addr_to_native(const cf_sock_addr *addr, struct sockaddr *native)
{
	struct sockaddr_in *sai = (struct sockaddr_in *)native;
	memset(sai, 0, sizeof(struct sockaddr_in));
	sai->sin_family = AF_INET;
	sai->sin_addr = addr->addr.v4;
	sai->sin_port = htons(addr->port);
}

int32_t
cf_mserv_cfg_add_combo(cf_mserv_cfg *serv_cfg, cf_sock_owner owner, cf_ip_port port,
		cf_ip_addr *addr, cf_ip_addr *if_addr, uint8_t ttl)
{
	cf_msock_cfg sock_cfg;
	cf_msock_cfg_init(&sock_cfg, owner);
	sock_cfg.port = port;
	cf_ip_addr_copy(addr, &sock_cfg.addr);
	cf_ip_addr_copy(if_addr, &sock_cfg.if_addr);
	sock_cfg.ttl = ttl;

	return cf_mserv_cfg_add_msock_cfg(serv_cfg, &sock_cfg);
}

int32_t
cf_socket_mcast_set_inter(cf_socket *sock, const cf_ip_addr *iaddr)
{
	struct ip_mreqn mr;
	memset(&mr, 0, sizeof(mr));
	mr.imr_address = iaddr->v4;

	if (setsockopt(sock->fd, IPPROTO_IP, IP_MULTICAST_IF, &mr, sizeof(mr)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_MULTICAST_IF) failed on FD %d: %d (%s)",
				sock->fd, errno, cf_strerror(errno));
		return -1;
	}

	return 0;
}

int32_t
cf_socket_mcast_set_ttl(cf_socket *sock, int32_t ttl)
{
	if (setsockopt(sock->fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_MULTICAST_TTL) failed on FD %d: %d (%s)",
				sock->fd, errno, cf_strerror(errno));
		return -1;
	}

	return 0;
}

int32_t
cf_socket_mcast_join_group(cf_socket *sock, const cf_ip_addr *iaddr, const cf_ip_addr *gaddr)
{
	struct ip_mreqn mr;
	memset(&mr, 0, sizeof(mr));

	if (!cf_ip_addr_is_any(iaddr)) {
		mr.imr_address = iaddr->v4;
	}

	mr.imr_multiaddr = gaddr->v4;

	if (setsockopt(sock->fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mr, sizeof(mr)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_ADD_MEMBERSHIP) failed on FD %d: %d (%s)",
				sock->fd, errno, cf_strerror(errno));
		return -1;
	}

#ifdef IP_MULTICAST_ALL
	// Only receive traffic from multicast groups this socket actually joins.
	// Note: Bind address filtering takes precedence, so this is simply an extra level of
	// restriction.
	static const int32_t no = 0;

	if (setsockopt(sock->fd, IPPROTO_IP, IP_MULTICAST_ALL, &no, sizeof(no)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_MULTICAST_ALL) failed on FD %d: %d (%s)",
				sock->fd, errno, cf_strerror(errno));
		return -1;
	}
#endif

	return 0;
}

size_t
cf_socket_addr_len(const struct sockaddr *sa)
{
	switch (sa->sa_family) {
	case AF_INET:
		return sizeof(struct sockaddr_in);

	default:
		cf_crash(CF_SOCKET, "Invalid address family: %d", sa->sa_family);
		return 0;
	}
}

int32_t
cf_socket_parse_netlink(bool allow_ipv6, uint32_t family, uint32_t flags,
		const void *data, size_t len, cf_ip_addr *addr)
{
	(void)allow_ipv6;
	(void)flags;

	if (family != AF_INET || len != 4) {
		return -1;
	}

	memcpy(&addr->v4, data, 4);
	return 0;
}

void
cf_socket_fix_client(cf_socket *sock)
{
	(void)sock;
}

void
cf_socket_fix_bind(cf_serv_cfg *serv_cfg)
{
	(void)serv_cfg;
}

void
cf_socket_fix_server(cf_socket *sock)
{
	(void)sock;
}
