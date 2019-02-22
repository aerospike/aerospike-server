/*
 * dns.c
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

#include "dns.h"

#include <errno.h>

#include "citrusleaf/cf_queue.h"

#include "cf_thread.h"
#include "fault.h"

/*
 * ----------------------------------------------------------------------------
 * Constants.
 * ----------------------------------------------------------------------------
 */
/**
 * Number of resolver threads.
 */
#define CF_DNS_NUM_RESOLVERS 2

/**
 * Initial size of the request queue.
 */
#define CF_DNS_REQ_QUEUE_INIT_SIZE 10

/**
 * The logging context.
 */
#define CF_DNS_LOG_CONTEXT CF_SOCKET

/*
 * ----------------------------------------------------------------------------
 * Private data structures.
 * ----------------------------------------------------------------------------
 */

/**
 * Internal representation of a resolve request.
 */
typedef struct cf_dns_resolve_req_s
{
	/**
	 * The hostname to resolve.
	 */
	char hostname[DNS_NAME_MAX_SIZE + 1];

	/**
	 * Name resolution criteria for resolution.
	 */
	addrinfo hints;

	/**
	 * The call back function to invoke.
	 */
	cf_dns_resolve_cb cb;

	/**
	 * Userdata to be passed on to the callback function.
	 */
	void* udata;
} cf_dns_resolve_req;

/*
 * ----------------------------------------------------------------------------
 * Globals.
 * ----------------------------------------------------------------------------
 */
static cf_queue g_req_queue;

/*
 * ----------------------------------------------------------------------------
 * Private functions.
 * ----------------------------------------------------------------------------
 */

/**
 * DNS resolver thread.
 */
static void*
cf_dns_resolve_worker(void* arg)
{
	cf_dns_resolve_req req = { { 0 } };

	while (cf_queue_pop(&g_req_queue, &req, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		addrinfo *addrs = NULL;
		int status = getaddrinfo(req.hostname, NULL, &req.hints, &addrs);
		req.cb(status, req.hostname, addrs, req.udata);
	}

	return NULL;
}

/*
 * ----------------------------------------------------------------------------
 * Public functions.
 * ----------------------------------------------------------------------------
 */

/**
 * Start the dns resolver threads for asynchronous processing.
 */
void
cf_dns_init()
{
	// Initialize the request queue.
	cf_queue_init(&g_req_queue, sizeof(cf_dns_resolve_req),
			CF_DNS_REQ_QUEUE_INIT_SIZE, true);

	// Start the resolver threads.
	for (int i = 0; i < CF_DNS_NUM_RESOLVERS; i++) {
		cf_thread_create_detached(cf_dns_resolve_worker, NULL);
	}
}

/**
 * Resolves hostname to ip addresses asynchronously.
 *
 * @param hostname the hostname/devicename/ip address to resolve. Should be of
 * length less than or equal to DNS_NAME_MAX_SIZE.
 * @param hints criteria for selecting addresses. Can be NULL.
 * @param cb the call back invoked after resolution, either successful or
 * unsuccessful.
 * @param udata user define data passed to the callback. Can be NULL. Is not
 * freed, the caller shoud be own the lifecycle of udata.
 */
void
cf_dns_resolve_a(const char* hostname, addrinfo* hints, cf_dns_resolve_cb cb,
		void* udata)
{
	// Create a new resolution request.
	cf_dns_resolve_req req = { { 0 } };
	strncpy(req.hostname, hostname, sizeof(req.hostname));
	req.hostname[DNS_NAME_MAX_SIZE] = 0;
	memcpy(&req.hints, hints, sizeof(req.hints));
	req.cb = cb;
	req.udata = udata;

	// Queue the request.
	cf_queue_push(&g_req_queue, &req);
}

/**
 * Resolve hostname to ip addresses.
 *
 * @param hostname the hostname/devicename/ip address to resolve.
 * @param hints criteria for selecting addresses. Can be NULL.
 * @param addrs the result of resolution on success. If call is successful
 * should be freed after use using cf_dns_free.
 * @return 0 on success, EAI_* error code otherwise.
 */
CF_MUST_CHECK int
cf_dns_resolve(const char* hostname, addrinfo* hints, addrinfo** addrs)
{
	return getaddrinfo(hostname, NULL, hints, addrs);
}

/**
 * Convert dns error code to string.
 * @param errcode the error code not equal to zero.
 */
const char*
cf_dns_strerror(int errcode)
{
	return gai_strerror(errcode);
}

/**
 * Free space allocated for resolved addresses.
 *
 * @param addrs the addresses to free up. Can be NULL.
 */
void
cf_dns_free(addrinfo* addrs)
{
	if (addrs) {
		freeaddrinfo(addrs);
	}
}
