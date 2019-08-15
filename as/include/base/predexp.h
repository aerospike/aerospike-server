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

typedef enum {
	PREDEXP_FALSE = 0,		// matching nodes only
	PREDEXP_TRUE = 1,		// matching nodes only
	PREDEXP_UNKNOWN = 2,	// matching nodes only

	// Private - never returned by pulbic API.
	PREDEXP_VALUE = 3,		// value nodes only
	PREDEXP_NOVALUE = 4		// value nodes only
} predexp_retval_t;

predexp_eval_t* predexp_build(as_msg_field* pfp);

// Called with NULL rd
predexp_retval_t predexp_matches_metadata(predexp_eval_t* eval, predexp_args_t* argsp);

// Called with both ndx and rd.
bool predexp_matches_record(predexp_eval_t* eval, predexp_args_t* argsp);

void predexp_destroy(predexp_eval_t* eval);
