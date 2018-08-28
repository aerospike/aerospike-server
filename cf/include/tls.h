/*
 * tls.h
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

#pragma once

#include "socket.h"

struct cf_tls_info_s;
typedef struct cf_tls_info_s cf_tls_info;

typedef struct cf_tls_spec_s {
	char *ca_file;
	char *ca_path;
	char *cert_blacklist;
	char *cert_file;
	char *cipher_suite;
	char *key_file;
	char *key_file_password;
	char *pw_string;
	char *name;
	char *protocols;
} cf_tls_spec;

void tls_check_init();

void tls_cleanup();
void tls_thread_cleanup();

void tls_socket_init(cf_socket *sock);
void tls_socket_term(cf_socket *sock);
int tls_socket_shutdown(cf_socket *sock);
void tls_socket_close(cf_socket *sock);

char *tls_read_password(const char *path);
cf_tls_info *tls_config_server_context(cf_tls_spec *tspec, bool auth_client, uint32_t n_peer_names, char **peer_names);
cf_tls_info *tls_config_intra_context(cf_tls_spec *tspec, const char *which);

void tls_socket_prepare_server(cf_tls_info *info, cf_socket *sock);
void tls_socket_prepare_client(cf_tls_info *info, cf_socket *sock);

static inline bool tls_socket_needs_handshake(cf_socket *sock)
{
	return sock->state == CF_SOCKET_STATE_TLS_HANDSHAKE;
}

void tls_socket_must_not_have_data(cf_socket *sock, const char *caller);

int tls_socket_accept(cf_socket *sock);
int tls_socket_connect(cf_socket *sock);
int tls_socket_accept_block(cf_socket *sock);
int tls_socket_connect_block(cf_socket *sock);

int tls_socket_recv(cf_socket *sock, void *buf, size_t sz, int32_t flags,
					uint64_t timeout_msec);

int tls_socket_send(cf_socket *sock, void const *buf, size_t sz, int32_t flags,
					uint64_t timeout_msec);

int tls_socket_pending(cf_socket *sock);
