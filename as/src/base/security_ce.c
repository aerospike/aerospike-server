/*
 * security_stubs.c
 *
 * Copyright (C) 2014-2018 Aerospike, Inc.
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

#include "base/security.h"
#include "base/security_config.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"

#include "fault.h"
#include "socket.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"


//==========================================================
// Public API.
//

// Security is an enterprise feature - here, do nothing.
void
as_security_init(void)
{
}

// Security is an enterprise feature - here, allow all operations.
uint8_t
as_security_check(const as_file_handle* fd_h, as_sec_perm perm)
{
	return AS_OK;
}

// Security is an enterprise feature - here, allow all operations.
bool
as_security_check_data_op(as_transaction* tr, as_namespace* ns,
		as_sec_perm perm)
{
	return true;
}

// Security is an enterprise feature - here, there's no filter.
void*
as_security_filter_create(void)
{
	return NULL;
}

// Security is an enterprise feature - here, there's no filter.
void
as_security_filter_destroy(void* pv_filter)
{
}

// Security is an enterprise feature - here, do nothing.
void
as_security_log(const as_file_handle* fd_h, uint8_t result, as_sec_perm perm,
		const char* action, const char* detail)
{
}

// Security is an enterprise feature - here, never need to refresh.
bool
as_security_should_refresh(void)
{
	return false;
}

// Security is an enterprise feature - shouldn't get here.
void
as_security_refresh(as_file_handle* fd_h)
{
	cf_crash(AS_SECURITY, "CE build called as_security_refresh()");
}

// Security is an enterprise feature. If we receive a security message from a
// client here, quickly return AS_SEC_ERR_NOT_SUPPORTED. The client may choose
// to continue using this (unsecured) socket.
void
as_security_transact(as_transaction* tr)
{
	// We don't need the request, since we're ignoring it.
	cf_free(tr->msgp);
	tr->msgp = NULL;

	// Set up a simple response with a single as_sec_msg that has no fields.
	size_t resp_size = sizeof(as_proto) + sizeof(as_sec_msg);
	uint8_t resp[resp_size];

	// Fill out the as_proto fields.
	as_proto* p_resp_proto = (as_proto*)resp;

	p_resp_proto->version = PROTO_VERSION;
	p_resp_proto->type = PROTO_TYPE_SECURITY;
	p_resp_proto->sz = sizeof(as_sec_msg);

	// Switch to network byte order.
	as_proto_swap(p_resp_proto);

	uint8_t* p_proto_body = resp + sizeof(as_proto);

	memset((void*)p_proto_body, 0, sizeof(as_sec_msg));

	// Fill out the relevant as_sec_msg fields.
	as_sec_msg* p_sec_msg = (as_sec_msg*)p_proto_body;

	p_sec_msg->scheme = AS_SEC_MSG_SCHEME;
	p_sec_msg->result = AS_SEC_ERR_NOT_SUPPORTED;

	// Send the complete response.
	cf_socket *sock = &tr->from.proto_fd_h->sock;

	if (cf_socket_send_all(sock, resp, resp_size, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		cf_warning(AS_SECURITY, "fd %d send failed, errno %d",
				CSFD(sock), errno);
		as_end_of_transaction_force_close(tr->from.proto_fd_h);
		tr->from.proto_fd_h = NULL;
		return;
	}

	as_end_of_transaction_ok(tr->from.proto_fd_h);
	tr->from.proto_fd_h = NULL;
}


//==========================================================
// Public API - security configuration.
//

// Security is an enterprise feature - here, do nothing.
void
as_security_config_check()
{
}

// Security is an enterprise feature - here, do nothing.
void
as_security_config_log_scope(uint32_t sink, const char* ns_name,
		const char* set_name)
{
}
