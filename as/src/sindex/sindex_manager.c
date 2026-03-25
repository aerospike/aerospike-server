/*
 * sindex_manager.c
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
#include "sindex/sindex_manager.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "base/proto.h"
#include "base/smd.h"
#include "base/set_index.h"
#include "base/thr_info.h"

#include "sindex/sindex.h"

#include "shash.h"
#include "dynbuf.h"
#include "log.h"
#include "vector.h"


//==========================================================
// Typedefs & constants.
//


//==========================================================
// Globals.
//

#define TOK_CHAR_DELIMITER '|'

pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;


//==========================================================
// Forward declarations.
//

static void find_sindex_key(const cf_vector* items, void* udata);
static void defn_hash_put(as_sindex* si);
static void defn_hash_delete(as_sindex* si);
static void defn_hash_destroy_cb(cf_ll_element* ele);
static bool compare_buf(const uint8_t* buf1, uint32_t buf1_sz, const uint8_t* buf2, uint32_t buf2_sz);
static void defn_hash_generate_key(const char* bin_name, const uint8_t* exp_buf, uint32_t exp_buf_sz, defn_hash_key* key);
static uint32_t defn_hash_fn(const void* key);
static void def_free(as_sindex_def* def);
static void build_smd_key(const char* ns_name, const char* set_name, const char* bin_name, const char* cdt_ctx, const char* exp, as_sindex_type itype, as_particle_type ktype, char* smd_key);
static as_sindex_type itype_from_smd_char(char c);
static as_particle_type ktype_from_smd_char(char c);
static char itype_to_smd_char(as_sindex_type itype);
static char ktype_to_smd_char(as_particle_type ktype);
static inline bool is_set_index_smd_key(const char* smd_key);
static void sindex_list(as_info_cmd_args* args);
static void set_index_list(as_info_cmd_args* args);
static bool sindex_exists(const as_namespace* ns, const char* iname);


//==========================================================
// Public API.
//

void
as_sindex_manager_init(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];
		
		ns->sindex_defn_hash = cf_shash_create(defn_hash_fn,
				sizeof(defn_hash_key), sizeof(cf_ll*), 256 , false);

		ns->sindex_iname_hash = cf_shash_create(cf_shash_fn_zstr,
				INAME_MAX_SZ, sizeof(as_sindex*), 256, false);
		ns->n_sindexes = 0;
	}
	
	as_sindex_init();

	pthread_rwlockattr_t rwattr;

	pthread_rwlockattr_init(&rwattr);
	pthread_rwlockattr_setkind_np(&rwattr,
			PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

	pthread_rwlock_init(&g_sindex_rwlock, &rwattr);

	// Register the unified SMD callback for both secondary and set indexes
	as_smd_module_load(AS_SMD_MODULE_SINDEX, as_sindex_manager_smd_accept_cb, 
			NULL,NULL);
	
	as_sindex_resume_check();
}

void
as_sindex_manager_start(void)
{
	as_set_index_start();   // starts set-index population
	as_sindex_start();     	// starts sindex GC threads
}

// TODO:- (daud) carve out parse functions for set index and secondary index.
void
as_sindex_manager_sindex_create(as_info_cmd_args* args)
{
	const char* params = args->params;
	cf_dyn_buf* db = args->db;
	info_param_result rv;

	// Common params for both set index and secondary index.
	char idx_name_str[INAME_MAX_SZ] = { 0 };
	int idx_name_len = sizeof(idx_name_str);
	rv = as_info_parameter_get(params, "indexname", idx_name_str, &idx_name_len);
	if (! as_info_required_param_is_ok(db, "indexname", idx_name_str, rv)) {
		return;
	}

	char ns_str[AS_ID_NAMESPACE_SZ] = { 0 };
	int ns_len = sizeof(ns_str);
	rv = as_info_param_get_namespace_ns(params, ns_str, &ns_len);
	if (! as_info_required_param_is_ok(db, "namespace", ns_str, rv)) {
		return;
	}

	char itype_str[INDEXTYPE_MAX_SZ] = { 0 };
	int itype_len = sizeof(itype_str);
	rv = as_info_parameter_get(params, "indextype", itype_str, &itype_len);
	bool is_set_ix = (rv == INFO_PARAM_OK &&
			as_sindex_itype_from_string(itype_str) == AS_SINDEX_ITYPE_SET);

	char set_str[AS_SET_NAME_MAX_SIZE] = { 0 };
	char* p_set_str = NULL;
	char smd_key[SINDEX_SMD_KEY_MAX_SZ] = { 0 };
	int set_len;

	const char* set_name = NULL;
	const char* bin_name_arg = NULL;
	const char* cdt_ctx_arg = NULL;
	const char* exp_arg = NULL;
	as_sindex_type itype = AS_SINDEX_ITYPE_DEFAULT;
	as_particle_type ktype = AS_PARTICLE_TYPE_NULL;

	if (is_set_ix) {
		// Set index: reject sindex-only params; require set.
		char ctx_b64[CTX_B64_MAX_SZ];
		int ctx_b64_len = sizeof(ctx_b64);
		if (as_info_parameter_get(params, "context", ctx_b64, &ctx_b64_len) == 
				INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create: context parameter is not supported for set index");
			as_info_respond_error(db, AS_ERR_PARAMETER, "context parameter is not supported for set index");
			return;
		}

		char indexdata_str[INDEXDATA_MAX_SZ];
		int indexdata_len = sizeof(indexdata_str);
		if (as_info_parameter_get(params, "indexdata", 
					indexdata_str, &indexdata_len) == INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create: indexdata parameter is not supported for set index");
			as_info_respond_error(db, AS_ERR_PARAMETER, "indexdata parameter is not supported for set index");
			return;
		}

		char ktype_str[KTYPE_MAX_SZ];
		int ktype_len = sizeof(ktype_str);
		if (as_info_parameter_get(params, "type", ktype_str, &ktype_len) == 
				INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create: type parameter is not supported for set index");
			as_info_respond_error(db, AS_ERR_PARAMETER, "type parameter is not supported for set index");
			return;
		}

		char bin_name[AS_BIN_NAME_MAX_SZ];
		int bin_name_len = sizeof(bin_name);
		if (as_info_parameter_get(params, "bin", bin_name, &bin_name_len) == 
				INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create: bin parameter is not supported for set index");
			as_info_respond_error(db, AS_ERR_PARAMETER, "bin parameter is not supported for set index");
			return;
		}

		char exp_b64[EXP_B64_MAX_SZ];
		int exp_b64_len = sizeof(exp_b64);
		if (as_info_parameter_get(params, "exp", exp_b64, &exp_b64_len) == 
				INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create: exp parameter is not supported for set index");
			as_info_respond_error(db, AS_ERR_PARAMETER, "exp parameter is not supported for set index");
			return;
		}
		
		set_len = sizeof(set_str);
		rv = as_info_parameter_get(params, "set", set_str, &set_len);
		if (! as_info_required_param_is_ok(db, "set", set_str, rv)) {
			return;
		}
	
		as_namespace* ns = as_namespace_get_byname(ns_str);
		if (ns == NULL) {
			cf_warning(AS_INFO, "sindex-create: namespace '%s' not found", 
						ns_str);
			as_info_respond_error(db, AS_ERR_PARAMETER, "namespace not found");
			return;
		}

		as_set* p_set = NULL;
		uint16_t set_id = INVALID_SET_ID;
		if (as_namespace_get_create_set_w_len(ns, set_str, strlen(set_str), 
				&p_set, &set_id) != 0) {
			cf_warning(AS_INFO, "sindex-create: failed to get/create set '%s' in namespace '%s'", 
						set_str, ns_str);
			as_info_respond_error(db, AS_ERR_PARAMETER, "failed to get/create set");
			return;
		}

		set_name = set_str;
		itype = AS_SINDEX_ITYPE_SET;
		ktype = AS_PARTICLE_TYPE_MAX;
	}
	else {
		set_len = sizeof(set_str);
		rv = as_info_parameter_get(params, "set", set_str, &set_len);
		rv = as_info_optional_param_is_ok(db, "set", set_str, rv);

		if (rv == INFO_PARAM_FAIL_REPLIED) {
			return;
		}

		if (rv == INFO_PARAM_OK) {
			p_set_str = set_str;
		}

		char ctx_b64[CTX_B64_MAX_SZ];
		int ctx_b64_len = sizeof(ctx_b64);
		const char* p_cdt_ctx = NULL;

		rv = as_info_parameter_get(params, "context", ctx_b64, &ctx_b64_len);
		rv = as_info_optional_param_is_ok(db, "context", ctx_b64, rv);

		if (rv == INFO_PARAM_FAIL_REPLIED) {
			return;
		}

		if (rv == INFO_PARAM_OK) {
			uint8_t* buf;
			int32_t buf_sz = as_sindex_cdt_ctx_b64_decode(ctx_b64, ctx_b64_len,
					&buf);

			if (buf_sz > 0) {
				cf_free(buf);
			}
			else if (buf_sz < 0) {
				switch (buf_sz) {
				case -1:
					cf_warning(AS_INFO, "sindex-create %s: 'context' invalid base64",
							idx_name_str);
					as_info_respond_error(db, AS_ERR_PARAMETER,
							"'context' invalid base64");
					return;
				case -2:
					cf_warning(AS_INFO, "sindex-create %s: 'context' invalid cdt context",
							idx_name_str);
					as_info_respond_error(db, AS_ERR_PARAMETER,
							"'context' invalid cdt context");
					return;
				case -3:
					cf_warning(AS_INFO, "sindex-create %s: 'context' not normalized msgpack",
							idx_name_str);
					as_info_respond_error(db, AS_ERR_PARAMETER,
							"'context' not normalized msgpack");
					return;
				default:
					cf_crash(AS_INFO, "unreachable");
				}
			}

			p_cdt_ctx = ctx_b64;
		}

		int indtype_len = sizeof(itype_str);

		rv = as_info_parameter_get(params, "indextype", itype_str,
				&indtype_len);
		rv = as_info_optional_param_is_ok(db, "indextype", itype_str, rv);

		if (rv == INFO_PARAM_FAIL_REPLIED) {
			return;
		}

		if (rv == INFO_PARAM_OK_NOT_FOUND) {
			// If not specified, the index type is DEFAULT.
			itype = AS_SINDEX_ITYPE_DEFAULT;
		}
		else {
			itype = as_sindex_itype_from_string(itype_str);

			if (itype == AS_SINDEX_N_ITYPES) {
				cf_warning(AS_INFO, "sindex-create %s: bad 'indextype' '%s'",
						idx_name_str, itype_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"bad 'indextype' - must be one of 'default', 'list', 'mapkeys', 'mapvalues'");
				return;
			}
		}

		char bin_name[AS_BIN_NAME_MAX_SZ] = { 0 };
		int bin_name_len = sizeof(bin_name);
		const char* p_bin_name = NULL;
		char exp_b64[EXP_B64_MAX_SZ] = { 0 };
		int exp_b64_len = sizeof(exp_b64);
		const char* p_exp = NULL;
		char indexdata_str[INDEXDATA_MAX_SZ] = { 0 };
		int indexdata_len = sizeof(indexdata_str);
		char ktype_str[KTYPE_MAX_SZ] = { 0 };
		int ktype_len = sizeof(ktype_str);
		char* p_ktype_str = NULL;

		info_param_result indexdata_rv = as_info_parameter_get(params, "indexdata",
				indexdata_str, &indexdata_len);
		indexdata_rv = as_info_optional_param_is_ok(db, "indexdata", 
					indexdata_str, indexdata_rv);

		if (indexdata_rv == INFO_PARAM_FAIL_REPLIED) {
			return;
		}

		info_param_result type_rv = as_info_parameter_get(params, "type", 
				ktype_str, &ktype_len);
		type_rv = as_info_optional_param_is_ok(db, "type", ktype_str, type_rv);

		if (type_rv == INFO_PARAM_FAIL_REPLIED) {
			return;
		}

		if (indexdata_rv == INFO_PARAM_OK_NOT_FOUND &&
				type_rv == INFO_PARAM_OK_NOT_FOUND) {
			cf_warning(AS_INFO, "sindex-create %s: both 'indexdata' and 'type' are missing",
					idx_name_str);
			as_info_respond_error(db, AS_ERR_PARAMETER, "both 'indexdata' and 'type' are missing");
			return;
		}

		if (indexdata_rv == INFO_PARAM_OK && type_rv == INFO_PARAM_OK) {
			cf_warning(AS_INFO, "sindex-create %s: both 'indexdata' and 'type' are specified",
					idx_name_str);
			as_info_respond_error(db, AS_ERR_PARAMETER, "both 'indexdata' and 'type' are specified");
			return;
		}

		if (type_rv == INFO_PARAM_OK) {
			// New protocol - type=<type>[;bin=<name>][;exp=<base64-exp>]
			ktype = as_sindex_ktype_from_string(ktype_str);

			if (ktype == AS_PARTICLE_TYPE_BAD) {
				cf_warning(AS_INFO, "sindex-create %s: bad 'type' '%s'",
						idx_name_str, ktype_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"bad 'type' - must be one of 'numeric', 'string', 'blob', 'geo2dsphere'");
				return;
			}

			p_ktype_str = ktype_str;

			rv = as_info_parameter_get(params, "bin", bin_name, &bin_name_len);

			uint32_t bin_rv = as_info_optional_param_is_ok(db, "bin", bin_name, 
						rv);

			if (bin_rv == INFO_PARAM_FAIL_REPLIED) {
				return;
			}

			if (bin_rv == INFO_PARAM_OK) {
				p_bin_name = bin_name;
			}

			rv = as_info_parameter_get(params, "exp", exp_b64, &exp_b64_len);

			uint32_t exp_rv = as_info_optional_param_is_ok(db, "exp", exp_b64, 
						rv);

			if (exp_rv == INFO_PARAM_FAIL_REPLIED) {
				return;
			}

			if (bin_rv == INFO_PARAM_OK && exp_rv == INFO_PARAM_OK) {
				cf_warning(AS_INFO, "sindex-create %s: both 'bin' and 'exp' are specified",
						idx_name_str);
				as_info_respond_error(db, AS_ERR_PARAMETER, "both 'bin' and 'exp' are specified");
				return;
			}

			if (bin_rv == INFO_PARAM_OK_NOT_FOUND &&
					exp_rv == INFO_PARAM_OK_NOT_FOUND) {
				cf_warning(AS_INFO, "sindex-create %s: both 'bin' and 'exp' are missing",
						idx_name_str);
				as_info_respond_error(db, AS_ERR_PARAMETER, "both 'bin' and 'exp' are missing");
				return;
			}

			if (exp_rv == INFO_PARAM_OK) {
				if (p_cdt_ctx != NULL) {
					cf_warning(AS_INFO, "sindex-create %s: both 'context' and 'exp' are specified",
							idx_name_str);
					as_info_respond_error(db, AS_ERR_PARAMETER, "both 'context' and 'exp' are specified");
					return;
				}

				uint8_t exp_type = AS_PARTICLE_TYPE_BAD;

				if (! as_sindex_validate_exp(exp_b64, &exp_type, db)) {
					return;
				}

				if (! as_sindex_validate_exp_type(idx_name_str, itype, ktype,
						exp_type, db)) {
					return;
				}

				p_exp = exp_b64;
			}
		}
		else {
			// Old protocol - indexdata=bin-name,keytype

			as_info_warn_deprecated("'indexdata' parameter of 'sindex-create' info command is deprecated - use 'bin' and 'type' instead");

			p_bin_name = indexdata_str;

			p_ktype_str = strchr(indexdata_str, ',');

			if (p_ktype_str == NULL) {
				cf_warning(AS_INFO, "sindex-create %s: 'indexdata' missing bin type",
						idx_name_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"'indexdata' missing bin type");
				return;
			}

			*p_ktype_str++ = '\0';

			if (p_bin_name[0] == '\0') {
				cf_warning(AS_INFO, "sindex-create %s: 'indexdata' missing bin name",
						idx_name_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"'indexdata' missing bin name");
				return;
			}

			if (strlen(p_bin_name) >= AS_BIN_NAME_MAX_SZ) {
				cf_warning(AS_INFO, "sindex-create %s: 'indexdata' bin name too long",
						idx_name_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"'indexdata' bin name too long");
				return;
			}

			ktype = as_sindex_ktype_from_string(p_ktype_str);

			if (ktype == AS_PARTICLE_TYPE_BAD) {
				cf_warning(AS_INFO, "sindex-create %s: bad 'indexdata' bin type '%s'",
						idx_name_str, p_ktype_str);
				as_info_respond_error(db, AS_ERR_PARAMETER,
						"bad 'indexdata' bin type - must be one of 'numeric', 'string', 'blob', 'geo2dsphere'");
				return;
			}
		}

		if (itype == AS_SINDEX_ITYPE_MAPKEYS &&
				ktype != AS_PARTICLE_TYPE_INTEGER &&
				ktype != AS_PARTICLE_TYPE_STRING &&
				ktype != AS_PARTICLE_TYPE_BLOB) {
			cf_warning(AS_INFO, "sindex-create %s: bad 'indexdata' bin type '%s' for 'indextype' 'mapkeys'",
					idx_name_str, p_ktype_str);
			as_info_respond_error(db, AS_ERR_PARAMETER,
					"bad 'indexdata' bin type for 'indextype' 'mapkeys' - must be one of 'numeric', 'string', 'blob'");
			return;
		}

		set_name = p_set_str;
		bin_name_arg = p_bin_name;
		cdt_ctx_arg = p_cdt_ctx;
		exp_arg = p_exp;
	}

	build_smd_key(ns_str, set_name, bin_name_arg, cdt_ctx_arg,
			exp_arg, itype, ktype, smd_key);

	cf_info(AS_INFO, "sindex-create: request received for %s:%s via info",
			ns_str, idx_name_str);

	find_sindex_key_udata fsk = {
			.ns_name = ns_str,
			.index_name = idx_name_str,
			.smd_key = smd_key
	};

	as_smd_get_all(AS_SMD_MODULE_SINDEX, find_sindex_key, &fsk);

	if (fsk.found_key != NULL) {
		if (strcmp(fsk.found_key, smd_key) != 0) {
			cf_free(fsk.found_key);
			cf_warning(AS_INFO, "sindex-create %s:%s: 'indexname' already exists with different definition",
					ns_str, idx_name_str);
			as_info_respond_error(db, AS_ERR_SINDEX_FOUND,
					"'indexname' already exists with different definition");
			return;
		}

		cf_free(fsk.found_key);
		cf_info(AS_INFO, "sindex-create %s:%s: 'indexname' and defintion already exists",
				ns_str, idx_name_str);
		as_info_respond_ok(db);
		return;
	}

	if (fsk.n_name_matches > 1) {
		cf_warning(AS_INFO, "sindex-create %s:%s: 'indexname' already exists with %u definitions - rename(s) required",
				ns_str, idx_name_str, fsk.n_name_matches);
		as_info_respond_error(db, AS_ERR_SINDEX_FOUND,
				"'indexname' already exists with multiple definitions");
		return;
	}

	// Allow index renaming even if there are MAX_N_SINDEXES sindexes.
	if (! is_set_ix && ! fsk.has_smd_key && fsk.n_sindexes >= MAX_N_SINDEXES) {
		cf_warning(AS_INFO, "sindex-create %s:%s: already at sindex definition limit",
				ns_str, idx_name_str);
		as_info_respond_error(db, AS_ERR_SINDEX_MAX_COUNT,
				"already at sindex definition limit");
		return;
	}

	if (! as_smd_set_blocking(AS_SMD_MODULE_SINDEX, smd_key, idx_name_str, 0)) {
		cf_warning(AS_INFO, "sindex-create: timeout while creating %s:%s in SMD",
				ns_str, idx_name_str);
		as_info_respond_error(db, AS_ERR_TIMEOUT, "timeout");
		return;
	}

	as_info_respond_ok(db);
}

void
as_sindex_manager_sindex_delete(as_info_cmd_args* args)
{
	const char* params = args->params;
	cf_dyn_buf* db = args->db;

	// Command format:
	// sindex-delete:ns=usermap;set=demo;indexname=um_state

	char index_name_str[INAME_MAX_SZ];
	int index_name_len = sizeof(index_name_str);
	info_param_result rv = as_info_parameter_get(params, "indexname", 
			index_name_str,	&index_name_len);

	rv = as_info_required_param_is_ok(db, "indexname", index_name_str, rv);

	if (rv == INFO_PARAM_FAIL_REPLIED) {
		return;
	}

	char ns_str[AS_ID_NAMESPACE_SZ];
	int ns_len = sizeof(ns_str);

	rv = as_info_param_get_namespace_ns(params, ns_str, &ns_len);

	if (! as_info_required_param_is_ok(db, "namespace", ns_str, rv)) {
		return;
	}

	cf_info(AS_INFO, "sindex-delete: request received for %s:%s via info",
			ns_str, index_name_str);

	find_sindex_key_udata fsk = {
			.ns_name = ns_str,
			.index_name = index_name_str
	};

	as_smd_get_all(AS_SMD_MODULE_SINDEX, find_sindex_key, &fsk);

	if (fsk.found_key == NULL) {
		if (fsk.n_name_matches == 0) {
			cf_info(AS_INFO, "sindex-delete: 'indexname' %s not found",
					fsk.index_name);
			as_info_respond_ok(db);
			return;
		}

		cf_warning(AS_INFO, "sindex-delete: 'indexname' %s not unique - found %u matches - rename(s) required",
				fsk.index_name, fsk.n_name_matches);
		as_info_respond_error(db, AS_ERR_SINDEX_FOUND,
				"'indexname' is not unique");
		return;
	}

	cf_info(AS_INFO, "sindex-delete: deleting %s in SMD", fsk.found_key);
	if (! as_smd_delete_blocking(AS_SMD_MODULE_SINDEX, fsk.found_key, 0)) {
		cf_free(fsk.found_key);
		cf_warning(AS_INFO, "sindex-delete: timeout while dropping %s:%s in SMD",
				ns_str, index_name_str);
		as_info_respond_error(db, AS_ERR_TIMEOUT, "timeout");
		return;
	}

	cf_free(fsk.found_key);

	as_info_respond_ok(db);
}

static bool
sindex_exists(const as_namespace* ns, const char* iname)
{
	SINDEX_GRLOCK();

	bool exists = as_si_by_iname(ns, iname) != NULL;

	SINDEX_GRUNLOCK();

	return exists;
}

void
as_sindex_manager_sindex_exists(struct as_info_cmd_args_s* args)
{
	const char* params = args->params;
	cf_dyn_buf* db = args->db;

	// Command format:
	// sindex-exists:ns=usermap;indexname=um_state

	char index_name_str[INAME_MAX_SZ];
	int index_name_len = sizeof(index_name_str);
	info_param_result rv = as_info_parameter_get(params, "indexname", 
			index_name_str, &index_name_len);

	rv = as_info_required_param_is_ok(db, "indexname", index_name_str, rv);

	if (rv == INFO_PARAM_FAIL_REPLIED) {
		return;
	}

	char ns_str[AS_ID_NAMESPACE_SZ];
	int ns_len = sizeof(ns_str);
	as_namespace* ns = NULL;

	rv = as_info_param_get_namespace_ns(params, ns_str, &ns_len);

	if (! info_param_required_local_namespace_is_ok(db, ns_str, &ns, rv)) {
		return;
	}

	cf_dyn_buf_append_string(db, sindex_exists(ns, index_name_str) ?
			"true" : "false");
}

bool
as_sindex_manager_stats_str(as_namespace* ns, char* iname, cf_dyn_buf* db)
{
	SINDEX_GRLOCK();

	as_sindex* si = as_si_by_iname(ns, iname);

	if (si == NULL) {
		cf_warning(AS_SINDEX, "SINDEX STAT : sindex %s not found", iname);
		SINDEX_GRUNLOCK();
		return false;
	}

	if (si->itype != AS_SINDEX_ITYPE_SET) {
		bool result = as_sindex_stats_str(si, db);
		SINDEX_GRUNLOCK();
		return result;
	}

	uint16_t set_id = si->set_id;

	SINDEX_GRUNLOCK();

	as_set* p_set = as_namespace_get_set_by_id(ns, set_id);
	
	return as_set_index_stats_str(ns, p_set, db);
}

static void
sindex_list(as_info_cmd_args* args)
{
	const char* params = args->params;
	cf_dyn_buf* db = args->db;

	char ns_str[128];
	int ns_len = sizeof(ns_str);
	info_param_result rv = as_info_param_get_namespace_ns(params, ns_str, 
			&ns_len);
	as_namespace* ns = NULL;

	rv = info_param_optional_local_namespace_is_ok(db, ns_str, &ns, rv);

	if (rv == INFO_PARAM_FAIL_REPLIED) {
		return;
	}

	char b64_str[6];
	int b64_len = sizeof(b64_str);
	bool b64 = false;

	rv = as_info_parameter_get(params, "b64", b64_str, &b64_len);
	rv = as_info_optional_param_is_ok(db, "b64", b64_str, rv);

	if (rv == INFO_PARAM_FAIL_REPLIED) {
		return;
	}

	if (rv == INFO_PARAM_OK) {
		if (strcmp(b64_str, "true") == 0) {
			b64 = true;
		}
		else if (strcmp(b64_str, "false") == 0) {
			b64 = false;
		}
		else {
			cf_warning(AS_INFO, "b64 value invalid");
			as_info_respond_error(db, AS_ERR_PARAMETER, "bad b64");
			return;
		}
	}

	if (ns == NULL) {
		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_sindex_list_str(g_config.namespaces[ns_ix], b64, db);
		}

		cf_dyn_buf_chomp_char(db, ';');
	}
	else {
		as_sindex_list_str(ns, b64, db);
		cf_dyn_buf_chomp_char(db, ';');
	}
}

static int
set_index_list_reduce_fn(const void* key, void* value, void* udata)
{
	(void)key;
	cf_dyn_buf* db = (cf_dyn_buf*)udata;
	as_sindex* si = *(as_sindex**)value;

	if (si->itype != AS_SINDEX_ITYPE_SET) {
		return CF_SHASH_OK;
	}

	cf_dyn_buf_append_string(db, "ns=");
	cf_dyn_buf_append_string(db, si->ns->name);
	cf_dyn_buf_append_string(db, ":indexname=");
	cf_dyn_buf_append_string(db, si->iname);
	cf_dyn_buf_append_string(db, ":set=");
	cf_dyn_buf_append_string(db, si->set_name);
	cf_dyn_buf_append_string(db, ":indextype=");
	cf_dyn_buf_append_string(db, as_sindex_type_names[si->itype]);

	cf_dyn_buf_append_char(db, ';');

	return CF_SHASH_OK;
}

static void
set_index_list_str(const as_namespace* ns, cf_dyn_buf* db)
{
	SINDEX_GRLOCK();

	cf_shash_reduce(ns->sindex_iname_hash, set_index_list_reduce_fn, db);

	SINDEX_GRUNLOCK();
}

static void
set_index_list(as_info_cmd_args* args)
{
	const char* params = args->params;
	cf_dyn_buf* db = args->db;

	char ns_str[128];
	int ns_len = sizeof(ns_str);
	info_param_result rv = as_info_param_get_namespace_ns(params, ns_str, 
			&ns_len);
	as_namespace* ns = NULL;

	rv = info_param_optional_local_namespace_is_ok(db, ns_str, &ns, rv);

	if (rv == INFO_PARAM_FAIL_REPLIED) {
		return;
	}

	if (ns == NULL) {
		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			set_index_list_str(g_config.namespaces[ns_ix], db);
		}
	}
	else {
		set_index_list_str(ns, db);
	}

	cf_dyn_buf_chomp_char(db, ';');
}

void
as_sindex_manager_list_str(struct as_info_cmd_args_s* args)
{
	sindex_list(args);
	cf_dyn_buf_append_char(args->db, ';');
	set_index_list(args);
	cf_dyn_buf_chomp_char(args->db, ';');
}

void
as_sindex_manager_handle_smd_item(const as_smd_item* item, 
		as_smd_accept_type accept_type)
{
	as_sindex_def def = { 0 };

	if (! as_sindex_manager_smd_item_to_def(item->key, item->value, &def)) {
		return;
	}

	if (item->value != NULL) {
		as_sindex_manager_smd_create(&def, accept_type == 
				AS_SMD_ACCEPT_OPT_START);
	}
	else {
		as_sindex_manager_smd_drop(&def);
	}

	def_free(&def);
}

void
as_sindex_manager_smd_accept_cb(const cf_vector* items, 
		as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		const as_smd_item* item = cf_vector_get_ptr(items, i);

		as_sindex_manager_handle_smd_item(item, accept_type);
	}
}

void
as_sindex_manager_smd_create(as_sindex_def* def, bool startup)
{
	if (def->itype == AS_SINDEX_ITYPE_SET) {
		smd_set_index_create(def);
	}
	else {
		smd_sindex_create(def, startup);
	}
}

void
as_sindex_manager_smd_drop(as_sindex_def* def)
{
	if (def->itype == AS_SINDEX_ITYPE_SET) {
		smd_set_index_drop(def);
	}
	else {
		smd_sindex_drop(def);
	}
}

bool
as_sindex_manager_smd_item_to_def(const char* smd_key, const char* smd_value, 
		as_sindex_def* def)
{
	const char* read = smd_key;
	const char* tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - namespace name missing delimiter");
		return false;
	}

	uint32_t ns_name_len = (uint32_t)(tok - read);
	def->ns = as_namespace_get_bybuf((const uint8_t*)read, ns_name_len);

	if (def->ns == NULL) {
		cf_detail(AS_SINDEX, "skipping invalid namespace %.*s", ns_name_len, 
				read);
		return false;
	}

	read = tok + 1;
	tok = strchr(read, TOK_CHAR_DELIMITER);

	if (tok == NULL) {
		cf_warning(AS_SINDEX, "smd - set name missing delimiter");
		return false;
	}

	uint32_t set_name_len = (uint32_t)(tok - read);

	if (set_name_len >= AS_SET_NAME_MAX_SIZE) {
		cf_warning(AS_SINDEX, "smd - set name too long");
		return false;
	}

	if (set_name_len != 0) {
		memcpy(def->set_name, read, set_name_len);
		def->set_name[set_name_len] = 0;
	}

	read = tok + 1;

	// For set index, smd key is ns|set|S. So remainder is exactly "S".
	if (read[0] == 'S' && read[1] == '\0') {
		if (set_name_len == 0) {
			cf_warning(AS_SINDEX, "smd - set name required for set index");
			return false;
		}

		def->itype = AS_SINDEX_ITYPE_SET;
		def->ktype = AS_PARTICLE_TYPE_MAX; // non-zero placeholder
		strcpy(def->bin_name, SET_INDEX_DUMMY_BIN_NAME);
	}
	else {
		// Secondary index: parse bin_name, optional c/e, itype, ktype.
		tok = strchr(read, TOK_CHAR_DELIMITER);

		if (tok == NULL) {
			cf_warning(AS_SINDEX, "smd - bin name missing delimiter");
			return false;
		}

		uint32_t bin_name_len = (uint32_t)(tok - read);

		memcpy(def->bin_name, read, bin_name_len);
		def->bin_name[bin_name_len] = 0;

		read = tok + 1;
		tok = strchr(read, TOK_CHAR_DELIMITER);

		const char* ctx_start = NULL;
		uint32_t ctx_len = 0;
		const char* exp_start = NULL;
		uint32_t exp_len = 0;

		if (*read == 'c') {
			if (tok == NULL) {
				cf_warning(AS_SINDEX, "smd - context missing delimiter");
				return false;
			}

			ctx_start = read + 1;
			ctx_len = (uint32_t)(tok - ctx_start);

			if (ctx_len >= CTX_B64_MAX_SZ) {
				cf_warning(AS_SINDEX, "smd - context too long");
				return false;
			}

			read = tok + 1;
			tok = strchr(read, TOK_CHAR_DELIMITER);
		}
		else if (*read == 'e') {
			if (bin_name_len != 0) {
				cf_warning(AS_SINDEX, "smd - both bin name and expression specified");
				return false;
			}

			if (tok == NULL) {
				cf_warning(AS_SINDEX, "smd - expression missing delimiter");
				return false;
			}

			exp_start = read + 1;
			exp_len = (uint32_t)(tok - exp_start);

			if (exp_len >= EXP_B64_MAX_SZ) {
				cf_warning(AS_SINDEX, "smd - expression too long");
				return false;
			}

			read = tok + 1;
			tok = strchr(read, TOK_CHAR_DELIMITER);
		}

		if (exp_start == NULL &&
				(bin_name_len == 0 || bin_name_len >= AS_BIN_NAME_MAX_SZ)) {
			cf_warning(AS_SINDEX, "smd - bad bin name");
			return false;
		}

		if (tok == NULL) {
			cf_warning(AS_SINDEX, "smd - itype missing delimiter");
			return false;
		}

		if (tok - read != 1) {
			cf_warning(AS_SINDEX, "smd - itype not single char");
			return false;
		}

		def->itype = itype_from_smd_char(*read);

		if (def->itype == AS_SINDEX_N_ITYPES) {
			cf_warning(AS_SINDEX, "smd - bad itype");
			return false;
		}

		read = tok + 1;

		if (*(read + 1) != '\0') {
			cf_warning(AS_SINDEX, "smd - ktype not single char");
			return false;
		}

		def->ktype = ktype_from_smd_char(*read);

		if (def->ktype == AS_PARTICLE_TYPE_BAD) {
			cf_warning(AS_SINDEX, "smd - bad ktype");
			return false;
		}

		if (def->itype == AS_SINDEX_ITYPE_MAPKEYS &&
				def->ktype != AS_PARTICLE_TYPE_INTEGER &&
				def->ktype != AS_PARTICLE_TYPE_STRING &&
				def->ktype != AS_PARTICLE_TYPE_BLOB) {
			cf_warning(AS_SINDEX, "smd - bad ktype for itype 'mapkeys'");
			return false;
		}

		if (ctx_start != NULL) {
			char* ctx_b64 = cf_malloc(ctx_len + 1);
			memcpy(ctx_b64, ctx_start, ctx_len);
			ctx_b64[ctx_len] = '\0';
			def->ctx_b64 = ctx_b64;
		}
		else if (exp_start != NULL) {
			char* exp_b64 = cf_malloc(exp_len + 1);
			memcpy(exp_b64, exp_start, exp_len);
			exp_b64[exp_len] = '\0';
			def->exp_b64 = exp_b64;
		}
	}

	if (smd_value != NULL) {
		if (strlen(smd_value) >= INAME_MAX_SZ) {
			cf_warning(AS_SINDEX, "smd - iname too long");
			return false;
		}
		
		strcpy(def->iname, smd_value);
	}

	return true;
}

void
as_rename_sindex(as_sindex* si, const char* iname)
{
	as_namespace* ns = si->ns;

	cf_shash_delete(ns->sindex_iname_hash, si->iname);
	cf_shash_put(ns->sindex_iname_hash, iname, &si);

	memcpy(si->iname, iname, INAME_MAX_SZ); // keep iname 0-padded
}

void
as_add_sindex(as_sindex* si)
{
	as_namespace* ns = si->ns;

	defn_hash_put(si);
	cf_shash_put(ns->sindex_iname_hash, si->iname, &si);
	if (si->itype != AS_SINDEX_ITYPE_SET) {
		ns->n_sindexes++;
	}
}

void
as_delete_sindex(as_sindex* si)
{
	as_namespace* ns = si->ns;

	if (si->itype != AS_SINDEX_ITYPE_SET) {
		ns->n_sindexes--;
	}

	defn_hash_delete(si);
	cf_shash_delete(ns->sindex_iname_hash, si->iname);
}

as_sindex*
as_si_by_iname(const as_namespace* ns, const char* iname)
{
	size_t iname_len = strlen(iname);

	if (iname_len == 0 || iname_len >= INAME_MAX_SZ) {
		cf_warning(AS_SINDEX, "bad index name size %zu", iname_len);
		return NULL;
	}

	char padded_iname[INAME_MAX_SZ] = { 0 };

	strcpy(padded_iname, iname);

	as_sindex* si = NULL;

	cf_shash_get(ns->sindex_iname_hash, padded_iname, &si);

	return si;
}

as_sindex*
as_si_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name,
		as_particle_type ktype, as_sindex_type itype, const uint8_t* exp_buf,
		uint32_t exp_buf_sz, const uint8_t* ctx_buf, uint32_t ctx_buf_sz)
{
	cf_ll* si_ll = as_si_list_by_defn(ns, set_id, bin_name, exp_buf, exp_buf_sz);

	if (si_ll == NULL) {
		return NULL;
	}

	cf_ll_element* ele = cf_ll_get_head(si_ll);

	while (ele != NULL) {
		defn_hash_ele* prop_ele = (defn_hash_ele*)ele;
		as_sindex* si = prop_ele->si;

		if (si->ktype == ktype && si->itype == itype &&
				compare_buf(si->exp_buf, si->exp_buf_sz, exp_buf, exp_buf_sz) &&
				compare_buf(si->ctx_buf, si->ctx_buf_sz, ctx_buf, ctx_buf_sz)) {
			return si;
		}

		ele = ele->next;
	}

	return NULL;
}

cf_ll*
as_si_list_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name,
		const uint8_t* exp_buf, uint32_t exp_buf_sz)
{
	defn_hash_key key = { .set_id = set_id };

	defn_hash_generate_key(bin_name, exp_buf, exp_buf_sz, &key);

	cf_ll* si_ll = NULL;

	cf_shash_get(ns->sindex_defn_hash, &key, &si_ll);

	return si_ll;
}


//==========================================================
// Local helpers.
//

static void
find_sindex_key(const cf_vector* items, void* udata)
{
	find_sindex_key_udata* fsk = (find_sindex_key_udata*)udata;
	uint32_t ns_name_len = strlen(fsk->ns_name);

	fsk->found_key = NULL;
	fsk->n_name_matches = 0;
	fsk->n_sindexes = 0;
	fsk->n_setindexes = 0;
	fsk->has_smd_key = false;

	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		as_smd_item* item = cf_vector_get_ptr(items, i);

		if (item->value == NULL) {
			continue; // ignore tombstones
		}

		const char* smd_ns_name_end = strchr(item->key, '|');

		if (smd_ns_name_end == NULL) {
			cf_warning(AS_INFO, "unexpected sindex key format '%s'", item->key);
			continue;
		}

		uint32_t smd_ns_name_len = smd_ns_name_end - item->key;

		if (smd_ns_name_len != ns_name_len ||
				memcmp(item->key, fsk->ns_name, ns_name_len) != 0) {
			continue;
		}

		bool is_set_index = is_set_index_smd_key(item->key);

		if (is_set_index) {
			fsk->n_setindexes++;
		}
		else {
			fsk->n_sindexes++;
		}

		if (fsk->smd_key != NULL && strcmp(fsk->smd_key, item->key) == 0) {
			fsk->has_smd_key = true;
			fsk->smd_key = NULL; // can only be one
		}

		if (strcmp(fsk->index_name, item->value) != 0) {
			continue;
		}

		fsk->n_name_matches++;

		if (fsk->n_name_matches == 1) {
			fsk->found_key = strdup(item->key);
		}
		else {
			cf_free(fsk->found_key); // only return when unique
			fsk->found_key = NULL;
		}
	}
}

static void
defn_hash_put(as_sindex* si)
{
	as_namespace* ns = si->ns;

	defn_hash_key key = { .set_id = si->set_id };

	defn_hash_generate_key(si->bin_name, si->exp_buf, si->exp_buf_sz, &key);

	cf_ll* si_ll = NULL;

	int rv = cf_shash_get(ns->sindex_defn_hash, &key, &si_ll);

	if (rv == CF_SHASH_ERR_NOT_FOUND) {
		si_ll = cf_malloc(sizeof(cf_ll));
		cf_ll_init(si_ll, defn_hash_destroy_cb, false);
		cf_shash_put(ns->sindex_defn_hash, &key, &si_ll);
	}

	defn_hash_ele* ele = cf_malloc(sizeof(defn_hash_ele));

	ele->si = si;
	cf_ll_append(si_ll, (cf_ll_element*)ele);
}

static void
defn_hash_delete(as_sindex* si)
{
	as_namespace* ns = si->ns;

	defn_hash_key key = { .set_id = si->set_id };

	defn_hash_generate_key(si->bin_name, si->exp_buf, si->exp_buf_sz, &key);

	cf_ll* si_ll = NULL;

	cf_shash_get(ns->sindex_defn_hash, &key, &si_ll);

	cf_ll_element* ele = cf_ll_get_head(si_ll);

	while (ele != NULL) {
		defn_hash_ele* prop_ele = (defn_hash_ele*)ele;

		if (prop_ele->si == si) {
			cf_ll_delete(si_ll, ele);

			// If the list size becomes 0, delete the entry from the hash.
			if (cf_ll_size(si_ll) == 0) {
				cf_shash_delete(ns->sindex_defn_hash, &key);
			}

			return;
		}

		ele = ele->next;
	}
}

static void
defn_hash_destroy_cb(cf_ll_element* ele)
{
	cf_free(ele);
}

static bool
compare_buf(const uint8_t* buf1, uint32_t buf1_sz,
		const uint8_t* buf2, uint32_t buf2_sz)
{
	if (buf1 == NULL && buf2 == NULL) {
		return true;
	}

	if (buf1 != NULL && buf2 != NULL && buf1_sz == buf2_sz &&
			memcmp(buf1, buf2, buf2_sz) == 0) {
		return true;
	}

	return false;
}

static void
defn_hash_generate_key(const char* bin_name, const uint8_t* exp_buf,
		uint32_t exp_buf_sz, defn_hash_key* key)
{
	if (bin_name[0] != '\0') {
		strcpy(key->bin_name, bin_name);
	}
	else {
		uint64_t hash = cf_wyhash64(exp_buf, exp_buf_sz);
		memcpy(key->bin_name, &hash, sizeof(hash));
		key->bin_name[AS_BIN_NAME_MAX_SZ - 1] = 'e'; // unique identity for exp
	}
}

static uint32_t
defn_hash_fn(const void* key)
{
	return cf_wyhash32((const uint8_t*)key, sizeof(defn_hash_key));
}

static void
def_free(as_sindex_def* def)
{
	if (def->ctx_b64 != NULL) {
		cf_free(def->ctx_b64);
	}

	if (def->exp_b64 != NULL) {
		cf_free(def->exp_b64);
	}
}

static void
build_smd_key(const char* ns_name, const char* set_name,
		const char* bin_name, const char* cdt_ctx, const char* exp,
		as_sindex_type itype, as_particle_type ktype, char* smd_key)
{
	if (itype == AS_SINDEX_ITYPE_SET) {
		// For set index: 
		// ns|set|S

		sprintf(smd_key, "%s|%s|%c", ns_name,
				set_name != NULL ? set_name : "",
				itype_to_smd_char(itype));
	} else {
		// ns-name|<set-name>|bin-name|itype|ktype
		// ns-name|<set-name>|bin-name|c<base64>|itype|ktype
		// ns-name|<set-name>||e<base64>|itype|ktype

		sprintf(smd_key, "%s|%s|%s%s%s|%c|%c",
			ns_name,
			set_name != NULL ? set_name : "",
			// "" is illegal as a bin-name for si's & XDR bin shipping.
			bin_name != NULL ? bin_name : "",
			// 'e' prefix makes node reject entries with exp on downgrade.
			// 'c' prefix makes node reject entries with ctx on downgrade.
			exp != NULL ? "|e" : cdt_ctx != NULL ? "|c" : "",
			exp != NULL ? exp : cdt_ctx != NULL ? cdt_ctx : "",
			// |e and |c can't conflict with itype.
			itype_to_smd_char(itype),
			ktype_to_smd_char(ktype));
	}
}

static as_sindex_type
itype_from_smd_char(char c)
{
	switch (c) {
	case '.': return AS_SINDEX_ITYPE_DEFAULT;
	case 'L': return AS_SINDEX_ITYPE_LIST;
	case 'K': return AS_SINDEX_ITYPE_MAPKEYS;
	case 'V': return AS_SINDEX_ITYPE_MAPVALUES;
	case 'S': return AS_SINDEX_ITYPE_SET;
	default:
		cf_warning(AS_SINDEX, "invalid smd type %c", c);
		return AS_SINDEX_N_ITYPES;
	}
}

static as_particle_type
ktype_from_smd_char(char c)
{
	switch (c) {
	case 'I': return AS_PARTICLE_TYPE_INTEGER;
	case 'S': return AS_PARTICLE_TYPE_STRING;
	case 'B': return AS_PARTICLE_TYPE_BLOB;
	case 'G': return AS_PARTICLE_TYPE_GEOJSON;
	default:
		cf_warning(AS_SINDEX, "invalid smd ktype %c", c);
		return AS_PARTICLE_TYPE_BAD;
	}
}


static char
itype_to_smd_char(as_sindex_type itype)
{
	switch (itype) {
	case AS_SINDEX_ITYPE_DEFAULT: return '.';
	case AS_SINDEX_ITYPE_LIST: return 'L';
	case AS_SINDEX_ITYPE_MAPKEYS: return 'K';
	case AS_SINDEX_ITYPE_MAPVALUES: return 'V';
	case AS_SINDEX_ITYPE_SET: return 'S';
	default:
		cf_crash(AS_SINDEX, "invalid type %d", itype);
	}
	return '\0';
}

static char
ktype_to_smd_char(as_particle_type ktype)
{
	switch (ktype) {
	case AS_PARTICLE_TYPE_INTEGER: return 'I';
	case AS_PARTICLE_TYPE_STRING: return 'S';
	case AS_PARTICLE_TYPE_BLOB: return 'B';
	case AS_PARTICLE_TYPE_GEOJSON: return 'G';
	default:
		cf_crash(AS_SINDEX, "invalid ktype %d", ktype);
	}
	return '\0';
}

static inline bool
is_set_index_smd_key(const char* smd_key)
{
	int del_cnt = 0;

	for (const char* p = smd_key; *p; p++) {
		if (*p == '|') {
			del_cnt++;
		}
	}

	size_t n = strlen(smd_key);

	return del_cnt == 2 && smd_key[n - 1] == 'S';
}
