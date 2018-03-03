/*
 * endpoint.c
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

#include "fabric/endpoint.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "citrusleaf/alloc.h"

#include "fault.h"
#include "socket.h"

#include "base/cfg.h"

/*----------------------------------------------------------------------------
 * Private internal data structures.
 *----------------------------------------------------------------------------*/
typedef struct as_endpoint_collect_udata_s
{
	/**
	 * Collected endpoint pointers.
	 */
	const as_endpoint** endpoints;

	/**
	 * Collected endpoint count.
	 */
	uint32_t collected_count;
} as_endpoint_collect_udata;

typedef struct as_endpoint_to_string_udata_s
{
	/**
	 * Current write pointer.
	 */
	char* write_ptr;

	/**
	 * buffer remaining capacity.
	 */
	size_t buffer_remaining;

	/**
	 * Number of endpoints converted.
	 */
	uint32_t endpoints_converted;

	/**
	 * Capabilities of endpoint.
	 */
	uint8_t capabilities;

	/**
	 * Capability mask. Set to 0 to match all the endpoints.
	 */
	uint8_t capability_mask;
} as_endpoint_to_string_udata;

typedef struct as_endpoint_list_overlap_udata_s
{
	/**
	 * Indicates if there was an overlap.
	 */
	bool overlapped;

	/**
	 * Indicates if endpoint capabilities should be ignored.
	 */
	bool ignore_capabilities;

	/**
	 * The other list to compare.
	 */
	const as_endpoint_list* other;
} as_endpoint_list_overlap_udata;

typedef struct as_endpoint_list_endpoint_find_udata_s
{
	/**
	 * Indicates if there was an overlap.
	 */
	bool match_found;

	/**
	 * Indicates if endpoint capabilities should be ignored.
	 */
	bool ignore_capabilities;

	/**
	 * The other list to compare.
	 */
	const as_endpoint* to_find;
} as_endpoint_list_endpoint_find_udata;

/*----------------------------------------------------------------------------
 * Private internal function forward declarations.
 *----------------------------------------------------------------------------*/
static bool endpoint_addr_type_is_valid(uint8_t type);
static size_t endpoint_addr_binary_size(uint8_t type);
static size_t endpoint_sizeof_by_addr_type(uint8_t addr_type);
static as_endpoint* endpoint_allocate(uint8_t addr_type);
static void endpoint_collect_iterate_fn(const as_endpoint* endpoint, void* udata);
static void endpoint_to_string_iterate(const as_endpoint* endpoint, void* udata);
static uint8_t endpoint_addr_type_from_cf_ip_addr(const cf_ip_addr* addr);
static void endpoint_from_sock_cfg(const cf_sock_cfg* src, as_endpoint* endpoint);
static void endpoint_list_overlap_iterate(const as_endpoint* endpoint, void* udata);
static void endpoint_list_find_iterate(const as_endpoint* endpoint, void* udata);

static bool endpoints_are_equal(const as_endpoint* endpoint1, const as_endpoint* endpoint2, const bool ignore_capabilities);
static void endpoints_preference_sort(const as_endpoint* endpoints[], size_t n_endpoints);

/*----------------------------------------------------------------------------
 * Public API.
 *----------------------------------------------------------------------------*/

/**
 * Get the sizeof an endpoint. Accounts for variable size of the address field.
 * @return the size of the endpoint address. Zero if the endpoint address is
 * invalid.
 */
size_t
as_endpoint_sizeof(const as_endpoint* endpoint)
{
	return endpoint_sizeof_by_addr_type(endpoint->addr_type);
}

/**
 * Enable a capability on an endpoint given its mask.
 * @param endpoint the endpoint.
 * @param capability_mask the capability mask.
 */
void
as_endpoint_capability_enable(as_endpoint* endpoint, uint8_t capability_mask)
{
	endpoint->capabilities |= capability_mask;
}

/**
 * Disable a capability on an endpoint given its mask.
 * @param endpoint the endpoint.
 * @param capability_mask the capability mask.
 */
void
as_endpoint_capability_disable(as_endpoint* endpoint, uint8_t capability_mask)
{
	endpoint->capabilities &= ~capability_mask;
}

/**
 * Connect to an endpoint.
 *
 * @param endpoint the peer endpoint to connect to.
 * @param timeout the overall connect timeout.
 * @param sock (output) will be populated if connections is successful.
 * @return -1 on success, 0 on failure.
 */
int
as_endpoint_connect(const as_endpoint* endpoint, int32_t timeout, cf_socket* sock)
{
	if (!endpoint_addr_type_is_valid(endpoint->addr_type)) {
		return -1;
	}

	cf_sock_cfg cfg;
	cf_sock_cfg_init(&cfg, CF_SOCK_OWNER_INVALID);
	cfg.port = endpoint->port;
	if (cf_ip_addr_from_binary(endpoint->addr, endpoint_addr_binary_size(endpoint->addr_type),
		&cfg.addr) <= 0) {
		return -1;
	}

	int rv = cf_socket_init_client(&cfg, timeout, sock);

	// Reset the client sock config, because the config is a stack pointer.
	sock->cfg = NULL;
	return rv;
}

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
	as_endpoint_filter_fn filter_fn, void* filter_udata, int32_t timeout, cf_socket* sock)
{
	if (endpoint_list->n_endpoints == 0) {
		return NULL;
	}

	const as_endpoint* ordered_endpoints[endpoint_list->n_endpoints];
	const as_endpoint* rv = NULL;

	as_endpoint_collect_udata collect_udata;
	collect_udata.endpoints = ordered_endpoints;
	collect_udata.collected_count = 0;

	// Collect all endpoints in a pointer array.
	as_endpoint_list_iterate(endpoint_list, endpoint_collect_iterate_fn, &collect_udata);

	// Sort by descending preference.
	endpoints_preference_sort(ordered_endpoints, endpoint_list->n_endpoints);

	// TODO: Timeout individual connect or have the caller adjust based on
	// number of endpoints
	for (uint8_t i = 0; i < endpoint_list->n_endpoints; i++) {
		if (filter_fn && !(filter_fn)(ordered_endpoints[i], filter_udata)) {
			continue;
		}

		// Try this potential candidate.
		if (as_endpoint_connect(ordered_endpoints[i], timeout, sock) == 0) {
			// Connect succeeded.
			rv = ordered_endpoints[i];
			break;
		}
	}

	return rv;
}

/**
 * Convert a socket configuration to an endpoint in place.
 * @return a heap allocated, converted endpoint. Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
void
as_endpoint_from_sock_cfg_fill(const cf_sock_cfg* src, as_endpoint* endpoint)
{
	endpoint_from_sock_cfg(src, endpoint);
}

/**
 * Convert a socket configuration to an endpoint.
 * @return a heap allocated, converted endpoint. Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
as_endpoint*
as_endpoint_from_sock_cfg(const cf_sock_cfg* src)
{
	uint8_t addr_type = endpoint_addr_type_from_cf_ip_addr(&src->addr);
	as_endpoint* endpoint = endpoint_allocate(addr_type);
	endpoint_from_sock_cfg(src, endpoint);
	return endpoint;
}

/**
 * Convert an endpoint to a cf_sock_addr.
 * @param endpoint the source endpoint.
 * @param sock_addr the target socket address.
 * @return 0 on success, -1 on failure.
 */
int
as_endpoint_to_sock_addr(const as_endpoint* endpoint, cf_sock_addr* sock_addr)
{
	sock_addr->port = endpoint->port;
	return
		cf_ip_addr_from_binary(endpoint->addr, endpoint_addr_binary_size(endpoint->addr_type),
			&sock_addr->addr) > 0 ? 0 : -1;
}

/**
 * Indicates if an endpoint supports listed capabilities.
 * @return true if the endpoint supports the input capability.
 */
bool
as_endpoint_capability_is_supported(const as_endpoint* endpoint, uint8_t capability_mask)
{
	return (endpoint->capabilities & capability_mask) > 0;
}

/**
 * Return the in memory size in bytes of the endpoint list.
 * @param endpoint_list the endpoint list.
 * @param size (output) the size of the list on success.
 * @return 0 on successful size calculation, -1 otherwise.
 */
int
as_endpoint_list_sizeof(const as_endpoint_list* endpoint_list, size_t* size)
{
	return as_endpoint_list_nsizeof(endpoint_list, size, SIZE_MAX);
}

/**
 * Return the in memory size in bytes of the endpoint list, but abort if the
 * size of the read exceeds the input size.
 * @param endpoint_list the endpoint list.
 * @param size (output) the size of the list on success.
 * @param size_max the maximum size until which parsing will be attempted.
 * @return 0 on successful size calculation, -1 otherwise.
 */
int
as_endpoint_list_nsizeof(const as_endpoint_list* endpoint_list, size_t* size, size_t size_max)
{
	if (!endpoint_list) {
		return 0;
	}

	*size = sizeof(as_endpoint_list);

	uint8_t* endpoint_ptr = (uint8_t*) endpoint_list->endpoints;
	for (int i = 0; i < endpoint_list->n_endpoints; i++) {
		size_t endpoint_size = as_endpoint_sizeof((as_endpoint*)endpoint_ptr);
		if (endpoint_size == 0) {
			// Invalid endpoint. Signal error
			*size = 0;
			return -1;
		}

		if (*size + endpoint_size > size_max) {
			*size = 0;
			return -1;
		}

		*size += endpoint_size;
		endpoint_ptr += endpoint_size;
	}

	return 0;
}

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
as_endpoint_list_iterate(const as_endpoint_list* endpoint_list,
	const as_endpoint_iterate_fn iterate_fn, void* udata)
{
	if(!endpoint_list) {
		return;
	}

	uint8_t* endpoint_ptr = (uint8_t*) endpoint_list->endpoints;

	for (int i = 0; i < endpoint_list->n_endpoints; i++) {
		if (iterate_fn) {
			(iterate_fn)((as_endpoint*) endpoint_ptr, udata);
		}
		endpoint_ptr += as_endpoint_sizeof((as_endpoint*) endpoint_ptr);
	}
}

/**
 * Convert a server configuration to an endpoint list in place into the
 * destination endpoint list.
 * @param serv_cfg source server configuration.
 * @param endpoint_list destination endpoint list.
 */
void
as_endpoint_list_from_serv_cfg_fill(const cf_serv_cfg* serv_cfg, as_endpoint_list* endpoint_list)
{
	endpoint_list->n_endpoints = serv_cfg->n_cfgs;

	uint8_t* endpoint_ptr = (uint8_t*) &endpoint_list->endpoints[0];
	for (int i = 0; i < serv_cfg->n_cfgs; i++) {
		as_endpoint* endpoint = (as_endpoint*) endpoint_ptr;
		endpoint_from_sock_cfg(&serv_cfg->cfgs[i], endpoint);
		endpoint_ptr += as_endpoint_sizeof(endpoint);
	}
}

/**
 * Convert a server configuration to an endpoint list.
 * @param serv_cfg server configuration.
 * @return a heap allocated endpoint list.  Should be freed using cf_free
 * once the endpoint is no longer needed.
 */
as_endpoint_list*
as_endpoint_list_from_serv_cfg(const cf_serv_cfg* serv_cfg)
{
	size_t result_size = sizeof(as_endpoint_list);
	for (int i = 0; i < serv_cfg->n_cfgs; i++) {
		result_size += endpoint_sizeof_by_addr_type(
			endpoint_addr_type_from_cf_ip_addr(&serv_cfg->cfgs[i].addr));
	}

	as_endpoint_list* endpoint_list = (as_endpoint_list*) cf_malloc(result_size);

	as_endpoint_list_from_serv_cfg_fill(serv_cfg, endpoint_list);

	return endpoint_list;
}

/**
 * Compare two endpoint lists for equality.
 * @param list1 the first. NULL allowed.
 * @param list2 the second list. NULL allowed.
 * @return true iff the lists are equals, false otherwise.
 */
bool
as_endpoint_lists_are_equal(const as_endpoint_list* list1, const as_endpoint_list* list2)
{
	if (list1 == list2) {
		return true;
	}

	if (!list1 || !list2) {
		return false;
	}

	size_t size1;
	if (as_endpoint_list_sizeof(list1, &size1) != 0) {
		return false;
	}

	size_t size2;
	if (as_endpoint_list_sizeof(list2, &size2) != 0) {
		return false;
	}

	if (size1 != size2) {
		return false;
	}

	return memcmp(list1, list2, size1) == 0;
}

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
	bool ignore_capabilities)
{
	if (list1 == list2) {
		return true;
	}

	if (!list1 || !list2) {
		return false;
	}

	as_endpoint_list_overlap_udata udata;
	udata.overlapped = false;
	udata.other = list2;
	udata.ignore_capabilities = ignore_capabilities;

	as_endpoint_list_iterate(list1, endpoint_list_overlap_iterate, &udata);

	return udata.overlapped;
}

/**
 * Convert an endpoint list to a string.
 * @param endpoint_list the input list. NULL allowed.
 * @param buffer the output buffer.
 * @param buffer_capacity the capacity of the output buffer.
 * @return the number of characters printed (excluding the null  byte  used  to
 * end  output to strings)
 */
int
as_endpoint_list_to_string(const as_endpoint_list* endpoint_list, char* buffer,
		size_t buffer_capacity)
{
	return as_endpoint_list_to_string_match_capabilities(endpoint_list, buffer,
			buffer_capacity, 0, 0);
}

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
		size_t buffer_capacity, uint8_t capability_mask, uint8_t capabilities)
{
	if (!endpoint_list) {
		buffer[0] = 0;
		return 0;
	}

	as_endpoint_to_string_udata udata = { 0 };
	udata.write_ptr = buffer;
	udata.buffer_remaining = buffer_capacity;
	udata.capabilities = capabilities;
	udata.capability_mask = capability_mask;
	as_endpoint_list_iterate(endpoint_list, endpoint_to_string_iterate, &udata);

	if (udata.endpoints_converted) {
		if (udata.endpoints_converted != endpoint_list->n_endpoints) {
			// Truncation has happened. Add ellipses.
			if (udata.buffer_remaining > 4) {
				udata.buffer_remaining -= sprintf(udata.write_ptr, "...");
			}
		}
		else {
			// Remove the dangling comma from the last endpoint.
			udata.write_ptr--;
			udata.buffer_remaining++;
		}
	}

	// Ensure NULL termination.
	*udata.write_ptr = 0;

	return buffer_capacity - udata.buffer_remaining;
}

/**
 * Populate dyn buf with endpoints info
 * @param endpoint_list the input list. NULL allowed.
 * @param db the dynamic buffer.
 */
void
as_endpoint_list_info(const as_endpoint_list* endpoint_list, cf_dyn_buf* db)
{
	size_t endpoint_list_size = 0;
	as_endpoint_list_sizeof(endpoint_list, &endpoint_list_size);
	// 4 chars for delimiters, 50 chars for ipv6 ip and port, rounded to 64
	size_t endpoint_list_str_size = 64 * endpoint_list_size;

	char endpoint_list_str[endpoint_list_str_size];
	as_endpoint_list_to_string_match_capabilities(endpoint_list,
			endpoint_list_str, sizeof(endpoint_list_str), AS_ENDPOINT_TLS_MASK,
			0);

	cf_dyn_buf_append_string(db, "endpoint=");
	if (endpoint_list_str[0] != '\0') {
		cf_dyn_buf_append_string(db, endpoint_list_str);
	}
	cf_dyn_buf_append_string(db, ":");

	as_endpoint_list_to_string_match_capabilities(endpoint_list,
			endpoint_list_str, sizeof(endpoint_list_str), AS_ENDPOINT_TLS_MASK,
			AS_ENDPOINT_TLS_MASK);

	cf_dyn_buf_append_string(db, "endpoint-tls=");
	if (endpoint_list_str[0] != '\0') {
		cf_dyn_buf_append_string(db, endpoint_list_str);
	}

}

/*----------------------------------------------------------------------------
 * Private internal functions.
 *----------------------------------------------------------------------------*/
/**
 * Indicates if input address type is valid.
 */
static bool
endpoint_addr_type_is_valid(uint8_t type)
{
	return type > AS_ENDPOINT_ADDR_TYPE_UNDEF && type < AS_ENDPOINT_ADDR_TYPE_SENTINEL;
}

/**
 * Get the size of the binary for input address type.
 * TODO: Move to socket API. Not if we support DNS names.
 */
static size_t
endpoint_addr_binary_size(uint8_t type)
{
	return (type == AS_ENDPOINT_ADDR_TYPE_IPv4) ? 4 : 16;
}

/**
 * Return the sizeof endpoint give its address type.
 */
static size_t
endpoint_sizeof_by_addr_type(uint8_t addr_type)
{
	return sizeof(as_endpoint) + endpoint_addr_binary_size(addr_type);
}

/**
 * Convert cf_ip address to endpoint address type.
 */
static uint8_t
endpoint_addr_type_from_cf_ip_addr(const cf_ip_addr* addr)
{
	return cf_ip_addr_is_legacy(addr) ? AS_ENDPOINT_ADDR_TYPE_IPv4 : AS_ENDPOINT_ADDR_TYPE_IPv6;
}

/**
 * Heap allocate an endpoint.
 */
static as_endpoint*
endpoint_allocate(uint8_t addr_type)
{
	return cf_malloc(endpoint_sizeof_by_addr_type(addr_type));
}

/**
 * Convert a socket to an endpoint.
 */
static void
endpoint_from_sock_cfg(const cf_sock_cfg* src, as_endpoint* endpoint)
{
	endpoint->addr_type =
		cf_ip_addr_is_legacy(&src->addr) ? AS_ENDPOINT_ADDR_TYPE_IPv4 : AS_ENDPOINT_ADDR_TYPE_IPv6;
	endpoint->port = src->port;

	// We will have allocated correct binary size.
	CF_IGNORE_ERROR(
		cf_ip_addr_to_binary(&src->addr, endpoint->addr,
			endpoint_addr_binary_size(endpoint->addr_type)));

	endpoint->capabilities = (src->owner == CF_SOCK_OWNER_HEARTBEAT_TLS ||
		src->owner == CF_SOCK_OWNER_FABRIC_TLS) ? AS_ENDPOINT_TLS_MASK : 0;
}

/**
 * Generate a hash for an endpoint, but salted with the a random tie breaker to
 * generate random looking shuffles for "equal" endpoints. This is jenkins
 * one-at-a-time hash of the tie breaker concatenated with the endpoint.
 */
static uint32_t
endpoint_sort_hash(const as_endpoint* endpoint, int tie_breaker)
{
	uint32_t hash = 0;

	// Hash the nodeid.
	uint8_t* key = (uint8_t*)&tie_breaker;
	for (int i = 0; i < sizeof(tie_breaker); ++i) {
		hash += *key;
		hash += (hash << 10);
		hash ^= (hash >> 6);
		key++;
	}

	// Hash the endpoint value.
	size_t endpoint_size = as_endpoint_sizeof(endpoint);
	key = (uint8_t*)endpoint;
	for (int i = 0; i < endpoint_size; ++i) {
		hash += *key;
		hash += (hash << 10);
		hash ^= (hash >> 6);
		key++;
	}

	hash += (hash << 3);
	hash ^= (hash >> 11);
	hash += (hash << 15);
	return hash;
}

/**
 * Comparator to sort endpoints in descending order of preference.
 */
static int
endpoint_preference_compare(const void* e1, const void* e2, void* arg)
{
	const as_endpoint* endpoint1 = *(as_endpoint**)e1;
	const as_endpoint* endpoint2 = *(as_endpoint**)e2;
	int tie_breaker = *((int*)arg);

	// Prefer TLS over clear text.
	bool endpoint1_is_tls = as_endpoint_capability_is_supported(endpoint1, AS_ENDPOINT_TLS_MASK);

	bool endpoint2_is_tls = as_endpoint_capability_is_supported(endpoint2, AS_ENDPOINT_TLS_MASK);

	if (endpoint1_is_tls != endpoint2_is_tls) {
		return endpoint1_is_tls ? -1 : 1;
	}

	// If TLS capabilities match prefer IPv6.
	bool endpoint1_is_ipv6 = endpoint1->addr_type == AS_ENDPOINT_ADDR_TYPE_IPv6;
	bool endpoint2_is_ipv6 = endpoint2->addr_type == AS_ENDPOINT_ADDR_TYPE_IPv6;

	if (endpoint1_is_ipv6 != endpoint2_is_ipv6) {
		return endpoint1_is_ipv6 ? -1 : 1;
	}

	// Used tie breaker parameter to salt the hashes for load balancing.
	return endpoint_sort_hash(endpoint1, tie_breaker) -
		endpoint_sort_hash(endpoint2, tie_breaker);
}

/**
 * Sort endpoints in place in descending order of preference.
 * @param endpoints array of endpoint pointers.
 */
static void
endpoints_preference_sort(const as_endpoint* endpoints[], size_t n_endpoints)
{
	// Random tie breaker to load balance between two equivalent endpoints.
	int tie_breaker = rand();

	qsort_r(endpoints, n_endpoints, sizeof(as_endpoint*),
		endpoint_preference_compare, &tie_breaker);
}

/**
 * Iterate and collect all endpoint addresses in passed in udata.
 */
static void
endpoint_collect_iterate_fn(const as_endpoint* endpoint, void* udata)
{
	as_endpoint_collect_udata* endpoints_data = (as_endpoint_collect_udata*) udata;
	endpoints_data->endpoints[endpoints_data->collected_count++] = endpoint;
}

/**
 * Iterate over endpoints and convert them to strings.
 */
static void
endpoint_to_string_iterate(const as_endpoint* endpoint, void* udata)
{
	as_endpoint_to_string_udata* to_string_data =
			(as_endpoint_to_string_udata*)udata;

	if ((endpoint->capabilities & to_string_data->capability_mask)
			!= (to_string_data->capabilities & to_string_data->capability_mask)) {
		// skip as the capabilities do not match
		to_string_data->endpoints_converted++;
		return;
	}

	char address_buffer[1024];
	int capacity = sizeof(address_buffer);
	char* endpoint_str_ptr = address_buffer;

	cf_sock_addr temp_addr;
	if (cf_ip_addr_from_binary(endpoint->addr,
			endpoint_addr_binary_size(endpoint->addr_type), &temp_addr.addr)
			<= 0) {
		return;
	}

	int rv = 0;
	if (endpoint->port) {
		temp_addr.port = endpoint->port;
		rv = cf_sock_addr_to_string(&temp_addr, endpoint_str_ptr, capacity);
		if (rv <= 0) {
			return;
		}

		capacity -= rv;
		endpoint_str_ptr += rv;
		rv = snprintf(endpoint_str_ptr, capacity, ",");
	}
	else {
		// Skip port and tls capabilities.
		rv = cf_ip_addr_to_string(&temp_addr.addr, endpoint_str_ptr, capacity);
		if (rv <= 0) {
			return;
		}

		capacity -= rv;
		endpoint_str_ptr += rv;
		rv = snprintf(endpoint_str_ptr, capacity, ",");
	}

	if (rv == capacity) {
		// Output truncated. Abort.
		return;
	}

	int to_write = strnlen(address_buffer, sizeof(address_buffer));

	// Ensure we leave space for the NULL terminator.
	if (to_write + 1 <= to_string_data->buffer_remaining) {
		sprintf(to_string_data->write_ptr, "%s", address_buffer);
		to_string_data->buffer_remaining -= to_write;
		to_string_data->write_ptr += to_write;
		to_string_data->endpoints_converted++;
	}
}

/**
 * Compare two endpoints for equality.
 * @param endpoint1 the first. NULL allowed.
 * @param endpoint2 the second endpoint. NULL allowed.
 * @param ignore_capabilities indicates if endpoint capabilities should be
 * ignored.
 * @return true iff the endpoints are equals, false otherwise.
 */
static bool
endpoints_are_equal(const as_endpoint* endpoint1, const as_endpoint* endpoint2,
	bool ignore_capabilities)
{
	if (endpoint1 == endpoint2) {
		return true;
	}

	if (!endpoint1 || !endpoint2) {
		return false;
	}

	size_t size1 = as_endpoint_sizeof(endpoint1);
	if (!size1) {
		return false;
	}

	size_t size2 = as_endpoint_sizeof(endpoint2);
	if (!size2) {
		return false;
	}

	if (size1 != size2) {
		return false;
	}

	return (ignore_capabilities || endpoint1->capabilities == endpoint2->capabilities)
		&& endpoint1->port == endpoint2->port && endpoint1->addr_type == endpoint2->addr_type
		&& memcmp(endpoint1->addr, endpoint2->addr, endpoint_addr_binary_size(endpoint1->addr_type)) == 0;
}

/**
 * Iterate function to find an overlap.
 */
static void
endpoint_list_overlap_iterate(const as_endpoint* endpoint, void* udata)
{
	as_endpoint_list_overlap_udata* overlap_udata = (as_endpoint_list_overlap_udata*) udata;
	as_endpoint_list_endpoint_find_udata find_udata;
	find_udata.match_found = false;
	find_udata.ignore_capabilities = overlap_udata->ignore_capabilities;
	find_udata.to_find = endpoint;

	as_endpoint_list_iterate(overlap_udata->other, endpoint_list_find_iterate, &find_udata);

	overlap_udata->overlapped |= find_udata.match_found;
}

/**
 * Iterate function to search for an endpoint.
 */
static void
endpoint_list_find_iterate(const as_endpoint* endpoint, void* udata)
{
	as_endpoint_list_endpoint_find_udata* find_udata = (as_endpoint_list_endpoint_find_udata*) udata;

	const as_endpoint* to_find = find_udata->to_find;
	if (!to_find) {
		return;
	}

	find_udata->match_found |= endpoints_are_equal(endpoint, to_find,
		find_udata->ignore_capabilities);
}
