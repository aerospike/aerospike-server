/*
 * ael_diag.h
 *
 * Copyright (C) 2026 Aerospike, Inc.
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

typedef enum {
	AEL_SEV_ERROR = 0,
} ael_severity;

typedef struct {
	uint32_t offset;
	uint32_t sz;
	ael_severity severity;
	const char* msg;
	bool owned; // msg was cf_malloc'd by ael_diag_addf; freed by list_destroy
} ael_diag;

#define AEL_DIAG_MAX 8

typedef struct {
	ael_diag entries[AEL_DIAG_MAX];
	uint32_t count;
} ael_diag_list;

static inline void
ael_diag_add(ael_diag_list* list, ael_severity sev, uint32_t offset,
		uint32_t byte_sz, const char* msg)
{
	if (list->count >= AEL_DIAG_MAX) {
		return;
	}

	list->entries[list->count++] = (ael_diag){
		.offset = offset, .sz = byte_sz, .severity = sev, .msg = msg
	};
}

static inline bool
ael_diag_has_error(const ael_diag_list* list)
{
	for (uint32_t i = 0; i < list->count; i++) {
		if (list->entries[i].severity == AEL_SEV_ERROR) {
			return true;
		}
	}

	return false;
}

// Formatted diagnostic: the message is cf_malloc'd (owned) and freed by
// ael_diag_list_destroy. For messages that interpolate token text, type names,
// or counts; ael_diag_add stays for string literals.
void ael_diag_addf(ael_diag_list* list, ael_severity sev, uint32_t offset,
		uint32_t byte_sz, const char* fmt, ...)
		__attribute__((format(printf, 5, 6)));

// Append a copy of `src` at the given span, duplicating an owned message so the
// source and destination lists can each be destroyed independently.
void ael_diag_add_copy(ael_diag_list* list, const ael_diag* src,
		uint32_t offset, uint32_t byte_sz);

// Free the owned (ael_diag_addf) messages in the list; a no-op for lists that
// hold only string literals.
void ael_diag_list_destroy(ael_diag_list* list);
