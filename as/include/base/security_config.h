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

// Security configuration.
typedef struct as_sec_config_s {
	bool				security_enabled;
	uint32_t			privilege_refresh_period;	// (seconds)
	as_sec_report		report;						// reporting sinks
	as_sec_syslog_local	syslog_local;				// syslog local facility
} as_sec_config;


//==========================================================
// Public API.
//

void as_security_config_check();
void as_security_config_log_scope(uint32_t sink, const char* ns_name,
		const char* set_name);
