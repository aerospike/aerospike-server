/*
 * endpoint.h
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

/*
 * Overview
 * ========
 *
 * An endpoint captures all information needed by a peer node to establish a
 * connection to a service (e.g. fabric or heartbeat). The key difference
 * between an endpoint and socket API's cf_sock_cfg, is that cf_sock_cfg captures
 * all information needed by a service to start a server socket and accept
 * connections on it, whereas an endpoint captures all information a peer needs
 * to connect to the service. These two complementary structures overlap in
 * information content, however cf_sock_cfg will carry server side configuration
 * values (e.g. TLS configuration), which are irrelevant for the client using
 * this service. Also an endpoint structure is oriented to be advertised over
 * the wire.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "socket.h"

/**
 * Indicates if this endpoint supports TLS.
 */
#define AS_ENDPOINT_TLS_MASK 0x01

/**
 * Endpoint address type.
 */
typedef enum
{
	/**
	 * Undefined address type.
	 */
	AS_ENDPOINT_ADDR_TYPE_UNDEF,
	/**
	 * IPv4 address.
	 */
	AS_ENDPOINT_ADDR_TYPE_IPv4,
	/**
	 * IPv6 address.
	 */
	AS_ENDPOINT_ADDR_TYPE_IPv6,
	/**
	 * Sentinel value.
	 */
	AS_ENDPOINT_ADDR_TYPE_SENTINEL
} as_endpoint_addr_type;

/**
 * An endpoint definition.
 */
typedef struct as_endpoint_s
{
	/**
	 *  Bit field of capabilities. currently carries only tls enabled flag.
	 */
	uint8_t capabilities;

	/**
	 * The type of the address.
	 */
	uint8_t addr_type;

	/**
	 * The endpoint port.
	 */
	uint16_t port;

	/**
	 * The network formatted and ordered IPv4 / IPv6 address (or string if
	 * we decide to support dns names). The size of this field depends on
	 * the address type.
	 */
	uint8_t addr[];
}__attribute__((__packed__)) as_endpoint;

/**
 * A list of endpoints.
 */
typedef struct as_endpoint_list_s
{
	/**
	 * The number of endpoints contained in the list. Max of 255.
	 */
	uint8_t n_endpoints;

	/**
	 * The list of endpoints.
	 */
	as_endpoint endpoints[];
}__attribute__((__packed__)) as_endpoint_list;

/**
 * Iterate function for iterating over endpoints in an endpoint list.
 * @param endpoint current endpoint in the iteration.
 * @param udata udata passed through from the invoker of the iterate function.
 */
typedef void
	(*as_endpoint_iterate_fn)(const as_endpoint* endpoint, void* udata);

/**
 * Filter function for an endpoints in an endpoint list.
 * @param endpoint current endpoint in the iteration.
 * @param udata udata passed through from the invoker of the filter function.
 * @return should return true if this endpoint passes the filter, false if it
 * fails the filter.
 */
typedef bool
	(*as_endpoint_filter_fn)(const as_endpoint* endpoint, void* udata);

/**
 * Get the sizeof an endpoint. Accounts for variable size of the address field.
 * @return the size of the endpoint address. Zero if the endpoint address is
 * invalid.
 */
size_t
as_endpoint_sizeof(const as_endpoint* endpoint);

/**
 * Enable a capability on an endpoint given its mask.
 * @param endpoint the endpoint.
 * @param capability_mask the capability mask.
 */
void
as_endpoint_capability_enable(as_endpoint* endpoint, uint8_t capability_mask);

/**
 * Disable a capability on an endpoint given its mask.
 * @param endpoint the endpoint.
 * @param capability_mask the capability mask.
 */
void
as_endpoint_capability_disable(as_endpoint* endpoint, uint8_t capability_mask);

/**
 * Connect to an endpoint.
 *
 * @param endpoint the peer endpoint to connect to.
 * @param owner the socket owner module.
 * @param timeout the overall connect timeout.
 * @param sock (output) will be populated if connections is successful.
 * @return -1 on success, 0 on failure.
 */
int
as_endpoint_connect(const as_endpoint* endpoint, int32_t timeout, cf_socket* sock);

/**
 * Connect to the best matching endpoint in the endpoint list.
 *
 * @param endpoint_list the list of endpoints.
 * @param filter_fn filter function to discard incompatible endpoints. Can be
 * NULL.
 * @param filter_udata udata passed on as is to the filter function.
 * @param timeout the overall connect timeout.
 * @param sock (output) will be populated if connection is successful.
 * @return the connected endpoint on success, NULL if no endpoint count be
 * connected.
 */
const as_endpoint*
as_endpoint_connect_any(const as_endpoint_list* endpoint_list,
	as_endpoint_filter_fn filter_fn, void* filter_udata, int32_t timeout, cf_socket* sock);
/**
 * Convert a socket configuration to an endpoint inplace.
 * @return a heap allocated, converted endpoint. Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
void
as_endpoint_from_sock_cfg_fill(const cf_sock_cfg* src, as_endpoint* endpoint);

/**
 * Convert a socket configuration to an endpoint.
 * @return a heap allocated, converted endpoint. Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
as_endpoint*
as_endpoint_from_sock_cfg(const cf_sock_cfg* src);

/**
 * Convert an endpoint to a cf_sock_addr.
 * @param endpoint the source endpoint.
 * @param sock_addr the target socket address.
 */
int
as_endpoint_to_sock_addr(const as_endpoint* endpoint, cf_sock_addr* sock_addr);

/**
 * Indicates if an endpoint supports listed capabilities.
 * @return true if the endpoint supports the input capability.
 */
bool
as_endpoint_capability_is_supported(const as_endpoint* endpoint, uint8_t capability_mask);

/**
 * Iterate over endpoints in an endpoint list and invoke the iterate function
 * for each endpoint.
 * @param iterate_fn the iterate function invoked for each endpoint in the list.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration.
 * NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
void
as_endpoint_list_iterate(const as_endpoint_list* endpoint_list, as_endpoint_iterate_fn iterate_fn,
	void* udata);

/**
 * Return the in memory size in bytes of the endpoint list.
 * @param endpoint_list the endpoint list.
 * @param size (output) the size of the list on success.
 * @return 0 on successful size calculation, -1 otherwise.
 */
int
as_endpoint_list_sizeof(const as_endpoint_list* endpoint_list, size_t* size);

/**
 * Return the in memory size in bytes of the endpoint list, but abort if the
 * size of the read exceeds the input size.
 * @param endpoint_list the endpoint list.
 * @param size (output) the size of the list on success.
 * @param size_max the maximum size until which parsing will be attempted.
 * @return 0 on successful size calculation, -1 otherwise.
 */
int
as_endpoint_list_nsizeof(const as_endpoint_list* endpoint_list, size_t* size, size_t size_max);

/**
 * Convert a server configuration to an endpoint list in place into the
 * destination endpoint list.
 * @param serv_cfg source server configuration.
 * @param endpoint_list destination endpoint list.
 */
void
as_endpoint_list_from_serv_cfg_fill(const cf_serv_cfg* serv_cfg, as_endpoint_list* endpoint_list);

/**
 * Convert a server configuration to an endpoint list.
 * @param serv_cfg server configuration.
 * @return a heap allocated endpoint list.  Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
as_endpoint_list*
as_endpoint_list_from_serv_cfg(const cf_serv_cfg* serv_cfg);

/**
 * Compare two endpoint lists for equality.
 * @param list1 the first. NULL allowed.
 * @param list2 the second list. NULL allowed.
 * @return true iff the lists are equals, false otherwise.
 */
bool
as_endpoint_lists_are_equal(const as_endpoint_list* list1, const as_endpoint_list* list2);

/**
 * Check if two lists overlap in at least one endpoint.
 * @param list1 the first. NULL allowed.
 * @param list2 the second list. NULL allowed.
 * @param ignore_capabilities set to true if the overlap match should ignore
 * node capabilities, false if capabilities should also be matched.
 * @return true iff the lists are overlap, false otherwise.
 */
bool
as_endpoint_lists_are_overlapping(const as_endpoint_list* list1, const as_endpoint_list* list2,
	bool ignore_capabilities);

/**
 * Convert an endpoint list to a string.
 * @param endpoint_list the input list. NULL allowed.
 * @param buffer the output buffer.
 * @buffer_capacity the capacity of the output buffer.
 * @return the number of characters printed (excluding the null  byte  used  to
 end  output  to strings)
 */
int
as_endpoint_list_to_string(const as_endpoint_list* endpoint_list, char* buffer,
	size_t buffer_capacity);

/**
 * Convert an endpoint list to a string matching capabilities.
 * @param endpoint_list the input list. NULL allowed.
 * @param buffer the output buffer.
 * @param buffer_capacity the capacity of the output buffer.
 * @param capability_mask specifies which bit to match.
 * @param capabilities specifies capabilities to be match for.
 * @return the number of characters printed (excluding the null  byte  used  to
 * end output to strings)
 */
int
as_endpoint_list_to_string_match_capabilities(
		const as_endpoint_list* endpoint_list, char* buffer,
		size_t buffer_capacity, uint8_t capability_mask, uint8_t capabilities);

/**
 * Populate dyn buf with endpoints info.
 * @param endpoint_list the input list. NULL allowed.
 * @param db the dynamic buffer.
 */
void
as_endpoint_list_info(const as_endpoint_list* endpoint_list, cf_dyn_buf* db);
