/*
 * predexp.h
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
 * predicate expression declarations
 *
 */

#pragma once

#include "base/datamodel.h"
#include "base/index.h"

// A "compiled" predicate expression
typedef struct predexp_eval_base_s predexp_eval_t;

// A named variable
typedef struct predexp_var_s as_predexp_var_t;

// Arguments to predicate expressions
typedef struct predexp_args_s {
	as_namespace*		ns;		// always present
	as_record*			md;		// always present
	as_predexp_var_t*	vl;		// always present
	as_storage_rd*		rd;		// NULL during metadata phase
} predexp_args_t;

extern predexp_eval_t* predexp_build(as_msg_field* pfp);

// Called with NULL rd
extern bool predexp_matches_metadata(predexp_eval_t* eval,
									 predexp_args_t* argsp);

// Called with both ndx and rd.
extern bool predexp_matches_record(predexp_eval_t* eval,
								   predexp_args_t* argsp);

extern void predexp_destroy(predexp_eval_t* eval);
