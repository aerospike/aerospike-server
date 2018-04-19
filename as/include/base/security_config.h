/*
 * security_config.h
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

// Syslog "local" facilities.
typedef enum {
	AS_SYSLOG_NONE		= -1,
	AS_SYSLOG_MIN		= 0,
	AS_SYSLOG_MAX		= 7,

	// May configure any facility from "local0" to "local7".
	AS_SYSLOG_LOCAL0	= 0,
	AS_SYSLOG_LOCAL1	= 1,
	AS_SYSLOG_LOCAL2	= 2,
	AS_SYSLOG_LOCAL3	= 3,
	AS_SYSLOG_LOCAL4	= 4,
	AS_SYSLOG_LOCAL5	= 5,
	AS_SYSLOG_LOCAL6	= 6,
	AS_SYSLOG_LOCAL7	= 7,
} as_sec_syslog_local;

// Security-related reporting sink bit-field flags.
#define AS_SEC_SINK_LOG		0x1
#define AS_SEC_SINK_SYSLOG	0x2

// Security-related reporting sinks as bit-fields.
typedef struct as_sec_report_s {
	uint32_t	authentication;
	uint32_t	data_op;
	uint32_t	sys_admin;
	uint32_t	user_admin;
	uint32_t	violation;
} as_sec_report;

// Hopefully nobody really needs this many.
#define MAX_ROLE_QUERY_PATTERNS 64

// EVP_MD methods for LDAP session token encryption.
typedef enum {
	AS_LDAP_EVP_SHA_256, // current default
	AS_LDAP_EVP_SHA_512,

	AS_LDAP_NUM_EVP_MDS
} as_sec_ldap_evp_md;

// Security configuration.
typedef struct as_sec_config_s {
	bool				ldap_enabled;
	bool				security_enabled;
	uint32_t			n_ldap_login_threads;
	uint32_t			privilege_refresh_period;	// (seconds)
	as_sec_report		report;						// reporting sinks
	as_sec_syslog_local	syslog_local;				// syslog local facility

	// LDAP scope configuration.
	bool 				ldap_tls_disabled;
	uint32_t			ldap_polling_period;		// (seconds)
	char*				ldap_query_base_dn;
	char*				ldap_query_user_dn;
	char*				ldap_query_user_password_file;
	char* 				ldap_role_query_base_dn;
	char*				ldap_role_query_patterns[MAX_ROLE_QUERY_PATTERNS + 1];
	bool				ldap_role_query_search_ou;
	char*				ldap_server;
	uint32_t			ldap_session_ttl;			// (seconds)
	char*				ldap_tls_ca_file;			// set unless tls disabled
	as_sec_ldap_evp_md	ldap_token_hash_method;
	char*				ldap_user_dn_pattern;
	char*				ldap_user_query_pattern;

	// Derived from config.
	char*				ldap_query_user_password;
} as_sec_config;

#define PRIVILEGE_REFRESH_PERIOD_MIN	10
#define PRIVILEGE_REFRESH_PERIOD_MAX	(60 * 60 * 24)
#define LDAP_POLLING_PERIOD_MIN			0 // zero means don't poll
#define LDAP_POLLING_PERIOD_MAX			(60 * 60 * 24)
#define LDAP_SESSION_TTL_MIN			120
#define LDAP_SESSION_TTL_MAX			(60 * 60 * 24 * 10)


//==========================================================
// Public API.
//

void as_security_config_check();
void as_security_config_log_scope(uint32_t sink, const char* ns_name,
		const char* set_name);
