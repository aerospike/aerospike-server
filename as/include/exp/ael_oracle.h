/*
 * ael_oracle.h
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

//==========================================================
// Expected-token oracle for AEL completion tooling.
//
// Built ONLY into the ael-lsp tool (-DAEL_ORACLE); the production server never
// compiles these paths. ael_can_accept / ael_num_tokens are defined in the
// generated parser (ael_parser.y %code, where the lemon statics are in scope).
// The Parse* entry points are lemon's own -- declared here because the
// generated ael_parser.h exposes only the TOK_* token codes, not the driver
// API. A completion driver (ael_complete.c) uses these to run a
// check-before-feed parse up to the cursor and read the accepted next-token set.
//

#include <stddef.h>

#include "exp/ael_lexer.h" // token_value
#include "exp/parse_context.h" // parse_context

// Is terminal `tok` (1 .. ael_num_tokens() - 1) a legal next token for the
// opaque yyParser* `parser` in its current state? Snapshots and probes without
// mutating the parser.
int ael_can_accept(void* parser, int tok);
int ael_num_tokens(void);

// Lemon's opaque parser entry points, used to drive a check-before-feed parse
// from outside the generated translation unit.
void* ParseAlloc(void* (*malloc_fn)(size_t));
void Parse(void* parser, int major, token_value minor, parse_context* ctx);
void ParseFree(void* parser, void (*free_fn)(void*));
