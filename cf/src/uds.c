/*
 * uds.c
 *
 * Copyright (C) 2023 Aerospike, Inc.
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

#include "uds.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "log.h"

#include "warnings.h"


//==========================================================
// Public API.
//

bool
cf_uds_connect(char* path, cf_uds* sock)
{
	// Create a Unix domain socket.
	int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);

	if (sockfd == -1) {
		cf_warning(CF_SOCKET, "error while creating socket for %s: %d (%s)",
				path, errno, cf_strerror(errno));
		return false;
	}

	struct sockaddr_un addr;

	if (strlen(path) >= sizeof(addr.sun_path)) {
		cf_warning(CF_SOCKET, "uds path %s must be < %lu characters", path,
				sizeof(addr.sun_path));
		return false;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, path);

	if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
		cf_ticker_warning(CF_SOCKET, "error while connect to %s: %d (%s)",
				path, errno, cf_strerror(errno));
		return false;
	}

	sock->fd = sockfd;

	return true;
}

int32_t
cf_uds_send_all(cf_uds* sock, const void* buf, size_t size, int32_t flags)
{
	uint8_t* start = (uint8_t*)buf;
	size_t off = 0;

	while (off < size) {
		ssize_t send_sz = send(sock->fd, start + off, size - off, flags);

		if (send_sz == -1) {
			cf_warning(CF_SOCKET, "failed to send complete buffer: %d (%s)",
					errno, cf_strerror(errno));
			return -1;
		}

		off += (size_t)send_sz;
	}

	return 0;
}

int32_t
cf_uds_recv_all(cf_uds* sock, void* buf, size_t size, int32_t flags)
{
	uint8_t* start = (uint8_t*)buf;
	size_t off = 0;

	while (off < size) {
		ssize_t recv_sz = recv(sock->fd, start + off, size - off, flags);

		if (recv_sz == -1) {
			cf_warning(CF_SOCKET, "failed to receive complete buffer: %d (%s)",
					errno, cf_strerror(errno));
			return -1;
		}

		if (recv_sz == 0) {
			cf_warning(CF_SOCKET, "connection closed before receiving complete buffer");
			return -1;
		}

		off += (size_t)recv_sz;
	}

	return 0;
}
