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

// Security-related reporting.
typedef struct as_sec_report_s {
	bool authentication;
	bool data_op;
	bool sys_admin;
	bool user_admin;
	bool violation;
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
	bool				security_configured;			// indirect config
	bool				quotas_enabled;
	uint32_t			privilege_refresh_period;	// (seconds)
	uint32_t			session_ttl;				// (seconds)
	uint32_t			tps_weight;

	// LDAP scope configuration.
	bool				ldap_configured;				// indirect config
	bool 				ldap_tls_disabled;
	uint32_t			n_ldap_login_threads;
	uint32_t			ldap_polling_period;		// (seconds)
	char*				ldap_query_base_dn;
	char*				ldap_query_user_dn;
	char*				ldap_query_user_password_file;
	char* 				ldap_role_query_base_dn;
	char*				ldap_role_query_patterns[MAX_ROLE_QUERY_PATTERNS + 1];
	bool				ldap_role_query_search_ou;
	char*				ldap_server;
	char*				ldap_tls_ca_file;			// set unless tls disabled
	as_sec_ldap_evp_md	ldap_token_hash_method;
	char*				ldap_user_dn_pattern;
	char*				ldap_user_query_pattern;

	// log scope configuration.
	as_sec_report		report;
} as_sec_config;

#define PRIVILEGE_REFRESH_PERIOD_MIN	10
#define PRIVILEGE_REFRESH_PERIOD_MAX	(60 * 60 * 24)
#define TPS_WEIGHT_MIN					2
#define TPS_WEIGHT_MAX					20

#define LDAP_POLLING_PERIOD_MAX			(60 * 60 * 24)
#define SECURITY_SESSION_TTL_MIN		120
#define SECURITY_SESSION_TTL_MAX		(60 * 60 * 24 * 10)


//==========================================================
// Public API.
//

void as_security_config_log_scope(const char* ns_name, const char* set_name);
void as_security_config_log_role(const char* role);
void as_security_config_log_user(const char* user);
