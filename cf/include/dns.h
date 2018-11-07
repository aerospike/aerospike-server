/*
 * dns.h
 *
 * Copyright (C) 2018 Aerospike, Inc.
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

#include <limits.h>
#include <netdb.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>

#define DNS_NAME_MAX_LEN 255
#define DNS_NAME_MAX_SIZE (DNS_NAME_MAX_LEN + 1)

#define CF_MUST_CHECK __attribute__((warn_unused_result))

typedef struct addrinfo addrinfo;

/**
 * Callback after asynchronous name resolution.
 *
 * @param status 0 iff the name resolution succeeded, else an error code.
 * @param hostname the hostname being resolved.
 * @param addrs the result of dns resolution. Should be freed using cf_dns_free
 * once the addresses are no longer required. Will not be NULL when status is
 * zero.
 * @param udata udata passed to the asynchronous resolve function.
 */
typedef void (*cf_dns_resolve_cb)(const int status, const char* hostname, addrinfo* addrs, void* udata);

void cf_dns_init();
void cf_dns_resolve_a(const char* hostname, addrinfo* hints, cf_dns_resolve_cb cb,
		void* udata);
CF_MUST_CHECK int cf_dns_resolve(const char* hostname, addrinfo* hints, addrinfo** addrs);
const char* cf_dns_strerror(int errcode);
void cf_dns_free(addrinfo* addrs);
