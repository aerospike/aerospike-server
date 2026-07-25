/*
 * ael_diag.c
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

//==========================================================
// Includes.
//

#include "exp/ael_diag.h"

#include <stdarg.h>
#include <stdio.h>

#include "enhanced_alloc.h"

void
ael_diag_addf(ael_diag_list* list, ael_severity sev, uint32_t offset,
		uint32_t byte_sz, const char* fmt, ...)
{
	if (list->count >= AEL_DIAG_MAX) {
		return;
	}

	va_list ap;

	va_start(ap, fmt);
	int n = vsnprintf(NULL, 0, fmt, ap);
	va_end(ap);

	if (n < 0) {
		return; // encoding error -- drop rather than emit a partial message
	}

	char* buf = cf_malloc((size_t)n + 1);

	va_start(ap, fmt);
	vsnprintf(buf, (size_t)n + 1, fmt, ap);
	va_end(ap);

	list->entries[list->count++] = (ael_diag){
		.offset = offset, .sz = byte_sz, .severity = sev, .msg = buf, .owned = true
	};
}

void
ael_diag_add_copy(ael_diag_list* list, const ael_diag* src, uint32_t offset,
		uint32_t byte_sz)
{
	if (list->count >= AEL_DIAG_MAX) {
		return;
	}

	// Each list owns its own copy of an owned message, so both can be destroyed.
	const char* msg = src->owned ? cf_strdup(src->msg) : src->msg;

	list->entries[list->count++] = (ael_diag){ .offset = offset,
		.sz = byte_sz,
		.severity = src->severity,
		.msg = msg,
		.owned = src->owned };
}

void
ael_diag_list_destroy(ael_diag_list* list)
{
	for (uint32_t i = 0; i < list->count; i++) {
		if (list->entries[i].owned) {
			cf_free((void*)list->entries[i].msg);
			list->entries[i].owned = false;
		}
	}
}
