/*
 * sindex.c
 *
 * Copyright (C) 2022-2023 Aerospike, Inc.
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

#include "sindex/sindex.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_ll.h"

#include "arenax.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"
#include "shash.h"
#include "vector.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/smd.h"
#include "geospatial/geospatial.h"
#include "sindex/gc.h"
#include "sindex/populate.h"
#include "sindex/sindex_tree.h"
#include "storage/storage.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define TOK_CHAR_DELIMITER '|'

typedef struct as_sindex_def_s {
	as_namespace* ns;
	char iname[INAME_MAX_SZ];
	char set_name[AS_SET_NAME_MAX_SIZE];
	char bin_name[AS_BIN_NAME_MAX_SZ];
	as_particle_type ktype;
	as_sindex_type itype;
	char* ctx_b64;
	char* exp_b64;
} as_sindex_def;

typedef struct exp_def_s {
	as_exp* exp; // built exp points to buf
	uint8_t* buf;
	int32_t buf_sz;
	cf_vector* bin_names;
} exp_def;

typedef struct defn_hash_key_s {
	uint16_t set_id;
	char bin_name[AS_BIN_NAME_MAX_SZ]; // will be hash of exp for SI on exp
} defn_hash_key;

typedef struct defn_hash_ele_s {
	cf_ll_element ele;
	as_sindex* si;
} defn_hash_ele;

typedef bool (*add_ktype_from_msgpack_fn)(msgpack_in* val, as_sindex_bin* sbin);

static const char* sindex_itypes[] = {
		"default",
		"list",
		"mapkeys",
		"mapvalues"
};

ARRAY_ASSERT(sindex_itypes, AS_SINDEX_N_ITYPES);

#define CARDINALITY_PERIOD 3600


//==========================================================
// Globals.
//

pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;


//==========================================================
// Forward declarations.
//

static void as_sindex_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);
static bool smd_item_to_def(const char* smd_key, const char* smd_value, as_sindex_def* def);
static void smd_create(as_sindex_def* def, bool startup);
static void smd_drop(as_sindex_def* def);
static void rename_sindex(as_sindex* si, const char* iname);
static void add_sindex(as_sindex* si);
static void delete_sindex(as_sindex* si);
static bool parse_exp(const char* exp_b64, exp_def* e_def_r);
static void free_exp_def(exp_def* e_def);

static uint32_t defn_hash_fn(const void* key);
static void defn_hash_generate_key(const char* bin_name, const uint8_t* exp_buf, uint32_t exp_buf_sz, defn_hash_key* key);
static void defn_hash_put(as_sindex* si);
static void defn_hash_delete(as_sindex* si);
static void defn_hash_destroy_cb(cf_ll_element* ele);

static uint32_t populate_sbins(as_namespace* ns, uint16_t set_id, const as_bin* b, as_sindex_bin* sbins, as_sindex_op op);
static uint32_t populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbin, as_sindex_op op);

static cf_ll* si_list_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name, const uint8_t* exp_buf, uint32_t exp_buf_sz);
static as_sindex* si_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name, as_particle_type ktype, as_sindex_type itype, const uint8_t* exp_buf, uint32_t exp_buf_sz, const uint8_t* ctx_buf, uint32_t ctx_buf_sz);
static as_sindex* si_by_iname(const as_namespace* ns, const char* iname);
static bool compare_buf(const uint8_t* buf1, uint32_t buf1_sz, const uint8_t* buf2, uint32_t buf2_sz);

static bool sbin_from_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin);
static bool sbin_from_simple_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin);
static bool sbin_from_cdt_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin);

static void add_value_to_sbin(as_sindex_bin* sbin, int64_t val);

static bool add_listvalues_foreach(msgpack_in* element, void* udata);
static bool add_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata);
static bool add_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata);

static void add_long_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_string_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_blob_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);
static void add_geojson_from_msgpack(msgpack_in* element, as_sindex_bin* sbin);

static char const* ktype_str(as_particle_type ktype);
static as_particle_type ktype_from_smd_char(char c);
static char ktype_to_smd_char(as_particle_type ktype);
static as_sindex_type itype_from_smd_char(char c);
static char itype_to_smd_char(as_sindex_type itype);

static void* run_cardinality(void* udata);


//==========================================================
// Inlines & macros.
//

static inline void
add_keytype_from_msgpack(as_particle_type ktype, msgpack_in* element,
		as_sindex_bin* sbin)
{
	switch (ktype) {
	case AS_PARTICLE_TYPE_INTEGER:
		add_long_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_STRING:
		add_string_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_BLOB:
		add_blob_from_msgpack(element, sbin);
		break;
	case AS_PARTICLE_TYPE_GEOJSON:
		add_geojson_from_msgpack(element, sbin);
		break;
	default:
		break;
	}
}

static inline void
as_sindex_def_free(as_sindex_def* def)
{
	if (def->ctx_b64 != NULL) {
		cf_free(def->ctx_b64);
	}

	if (def->exp_b64 != NULL) {
		cf_free(def->exp_b64);
	}
}

static inline void
init_sbin(as_sindex_bin* sbin, as_sindex_op op, as_sindex* si)
{
	*sbin = (as_sindex_bin){ .si = si, .op = op };
}

static inline void
sbin_free(as_sindex_bin* sbin)
{
	if (sbin->values != NULL) {
		cf_free(sbin->values);
	}
}


//==========================================================
// Public API - startup.
//

void
as_sindex_init(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->sindex_defn_hash = cf_shash_create(defn_hash_fn,
				sizeof(defn_hash_key), sizeof(cf_ll*), MAX_N_SINDEXES, false);

		ns->sindex_iname_hash = cf_shash_create(cf_shash_fn_zstr,
				INAME_MAX_SZ, sizeof(as_sindex*), MAX_N_SINDEXES, false);

		as_sindex_gc_ns_init(ns);
	}

	pthread_rwlockattr_t rwattr;

	pthread_rwlockattr_init(&rwattr);
	pthread_rwlockattr_setkind_np(&rwattr,
			PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

	pthread_rwlock_init(&g_sindex_rwlock, &rwattr);

	as_smd_module_load(AS_SMD_MODULE_SINDEX, as_sindex_smd_accept_cb, NULL,
			NULL);

	as_sindex_resume_check();
}

void
as_sindex_load(void)
{
	as_sindex_populate_startup();
}

void
as_sindex_start(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_thread_create_detached(as_sindex_run_gc, ns);
		cf_thread_create_detached(run_cardinality, ns);
	}
}


//==========================================================
// Public API - populate sindexes.
//

void
as_sindex_put_all_rd(as_namespace* ns, as_storage_rd* rd, as_index_ref* r_ref)
{
	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si != NULL && ! si->readable && (si->set_id == INVALID_SET_ID ||
				si->set_id == as_index_get_set_id(rd->r))) {
			as_sindex_put_rd(si, rd, r_ref);
		}
	}
}

void
as_sindex_put_rd(as_sindex* si, as_storage_rd* rd, as_index_ref* r_ref)
{
	as_bin* b = NULL;
	as_bin rb;
	as_record* r = r_ref->r;

	if (si->exp == NULL) {
		b = as_bin_get_live(rd, si->bin_name);

		if (b == NULL) {
			return;
		}
	}
	else {
		uint16_t set_id = si->set_id;

		if (set_id != INVALID_SET_ID && set_id != as_index_get_set_id(r)) {
			return;
		}

		as_bin_set_empty(&rb);

		as_exp_ctx ctx_rd = {
				.ns = si->ns,
				.r = r,
				.rd = rd
		};

		if (! as_exp_eval(si->exp, &ctx_rd, &rb, NULL)) {
			return;
		}

		b = &rb;
	}

	as_sindex_bin sbin;

	init_sbin(&sbin, AS_SINDEX_OP_INSERT, si);

	if (sbin_from_bin(si, b, &sbin)) {
		// Mark record for sindex before insertion.
		as_index_set_in_sindex(r);

		as_sindex_update_by_sbin(&sbin, 1, r_ref->r_h);
		sbin_free(&sbin);
	}

	if (si->exp != NULL) {
		as_bin_particle_destroy(&rb);
	}
}


//==========================================================
// Public API - modify sindexes from writes/deletes.
//

uint32_t
as_sindex_populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbins,
		as_sindex_op op)
{
	return populate_sbin_si(si, b, sbins, op);
}

uint32_t
as_sindex_populate_sbins(as_namespace* ns, uint16_t set_id, const as_bin* b,
		as_sindex_bin* sbins, as_sindex_op op)
{
	if (as_bin_is_tombstone(b)) {
		return 0;
	}

	uint32_t n_populated = populate_sbins(ns, set_id, b, sbins, op);

	if (set_id != INVALID_SET_ID) {
		n_populated += populate_sbins(ns, INVALID_SET_ID, b,
				sbins + n_populated, op);
	}

	return n_populated;
}

void
as_sindex_update_by_sbin(as_sindex_bin* sbins, uint32_t n_sbins,
		cf_arenax_handle r_h)
{
	// Deletes before inserts - a sindex key can recur with different op.

	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		if (sbin->op == AS_SINDEX_OP_DELETE) {
			for (uint32_t j = 0; j < sbin->n_values; j++) {
				int64_t bval = j == 0 ? sbin->val : sbin->values[j];

				as_sindex_tree_delete(sbin->si, bval, r_h);
			}
		}
	}

	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		if (sbin->op == AS_SINDEX_OP_INSERT) {
			for (uint32_t j = 0; j < sbin->n_values; j++) {
				int64_t bval = j == 0 ? sbin->val : sbin->values[j];

				as_sindex_tree_put(sbin->si, bval, r_h);
			}
		}
	}
}

void
as_sindex_sbin_free_all(as_sindex_bin* sbins, uint32_t n_sbins)
{
	for (uint32_t i = 0; i < n_sbins; i++) {
		as_sindex_bin* sbin = &sbins[i];

		as_sindex_release(sbin->si);
		sbin_free(sbin);
	}
}


//==========================================================
// Public API - lookup.
//

as_sindex*
as_sindex_lookup_by_defn(const as_namespace* ns, uint16_t set_id,
		const char* bin_name, as_particle_type ktype, as_sindex_type itype,
		const uint8_t* exp_buf, uint32_t exp_buf_sz, const uint8_t* ctx_buf,
		uint32_t ctx_buf_sz)
{
	SINDEX_GRLOCK();

	as_sindex* si = si_by_defn(ns, set_id, bin_name, ktype, itype, exp_buf,
			exp_buf_sz, ctx_buf, ctx_buf_sz);

	if (si == NULL && set_id != INVALID_SET_ID) {
		si = si_by_defn(ns, INVALID_SET_ID, bin_name, ktype, itype, exp_buf,
				exp_buf_sz, ctx_buf, ctx_buf_sz);
	}

	if (si == NULL || si->dropped) {
		SINDEX_GRUNLOCK();
		return NULL;
	}

	as_sindex_job_reserve(si);

	SINDEX_GRUNLOCK();

	return si;
}

as_sindex*
as_sindex_lookup_by_iname(const as_namespace* ns, const char* iname)
{
	SINDEX_GRLOCK();

	as_sindex* si = si_by_iname(ns, iname);

	if (si == NULL || si->dropped) {
		SINDEX_GRUNLOCK();
		return NULL;
	}

	as_sindex_job_reserve(si);

	SINDEX_GRUNLOCK();

	return si;
}


//==========================================================
// Public API - info & stats.
//

as_particle_type
as_sindex_ktype_from_string(const char* ktype_str)
{
	if (strcasecmp(ktype_str, "numeric") == 0) {
		return AS_PARTICLE_TYPE_INTEGER;
	}

	if (strcasecmp(ktype_str, "string") == 0) {
		return AS_PARTICLE_TYPE_STRING;
	}

	if (strcasecmp(ktype_str, "blob") == 0) {
		return AS_PARTICLE_TYPE_BLOB;
	}

	if (strcasecmp(ktype_str, "geo2dsphere") == 0) {
		return AS_PARTICLE_TYPE_GEOJSON;
	}

	cf_warning(AS_SINDEX, "invalid key type %s", ktype_str);

	return AS_PARTICLE_TYPE_BAD;
}

as_sindex_type
as_sindex_itype_from_string(const char* itype_str) {
	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_DEFAULT]) == 0) {
		return AS_SINDEX_ITYPE_DEFAULT;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_LIST]) == 0) {
		return AS_SINDEX_ITYPE_LIST;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_MAPKEYS]) == 0) {
		return AS_SINDEX_ITYPE_MAPKEYS;
	}

	if (strcasecmp(itype_str, sindex_itypes[AS_SINDEX_ITYPE_MAPVALUES]) == 0) {
		return AS_SINDEX_ITYPE_MAPVALUES;
	}

	return AS_SINDEX_N_ITYPES;
}

bool
as_sindex_exists(const as_namespace* ns, const char* iname)
{
	SINDEX_GRLOCK();

	bool exists = si_by_iname(ns, iname) != NULL;

	SINDEX_GRUNLOCK();

	return exists;
}

bool
as_sindex_stats_str(as_namespace* ns, char* iname, cf_dyn_buf* db)
{
	SINDEX_GRLOCK();

	as_sindex* si = si_by_iname(ns, iname);

	if (si == NULL) {
		cf_warning(AS_SINDEX, "SINDEX STAT : sindex %s not found", iname);
		SINDEX_GRUNLOCK();
		return false;
	}

	info_append_uint64(db, "entries", as_sindex_tree_n_keys(si));
	info_append_uint64(db, "used_bytes", as_sindex_tree_mem_size(si));

	info_append_uint64(db, "entries_per_bval", si->keys_per_bval);
	info_append_uint64(db, "entries_per_rec", si->keys_per_rec);

	info_append_uint32(db, "load_pct", si->populate_pct);
	info_append_uint64(db, "load_time", si->load_time);

	info_append_uint64(db, "stat_gc_recs", si->n_gc_cleaned);

	cf_dyn_buf_chomp(db);

	SINDEX_GRUNLOCK();

	return true;
}

void
as_sindex_list_str(const as_namespace* ns, bool b64, cf_dyn_buf* db)
{
	SINDEX_GRLOCK();

	for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
		as_sindex* si = ns->sindexes[i];

		if (si == NULL) {
			continue;
		}

		cf_dyn_buf_append_string(db, "ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_string(db, ":indexname=");
		cf_dyn_buf_append_string(db, si->iname);
		cf_dyn_buf_append_string(db, ":set=");
		cf_dyn_buf_append_string(db, si->set_name[0] != '\0' ?
				si->set_name : "null");
		cf_dyn_buf_append_string(db, ":bin=");
		cf_dyn_buf_append_string(db, si->bin_name[0] != '\0' ?
				si->bin_name : "null");
		cf_dyn_buf_append_string(db, ":type=");
		cf_dyn_buf_append_string(db, ktype_str(si->ktype));
		cf_dyn_buf_append_string(db, ":indextype=");
		cf_dyn_buf_append_string(db, sindex_itypes[si->itype]);
		cf_dyn_buf_append_string(db, ":context=");

		if (si->ctx_buf == NULL) {
			cf_dyn_buf_append_string(db, "null");
		}
		else {
			if (b64) {
				cf_dyn_buf_append_string(db, si->ctx_b64);
			}
			else {
				cdt_ctx_to_dynbuf(si->ctx_buf, si->ctx_buf_sz, db);
			}
		}

		cf_dyn_buf_append_string(db, ":exp=");

		if (si->exp == NULL) {
			cf_dyn_buf_append_string(db, "null");
		}
		else {
			if (b64) {
				cf_dyn_buf_append_string(db, si->exp_b64);
			}
			else {
				as_exp_display(si->exp, db);
			}
		}

		if (si->readable) {
			cf_dyn_buf_append_string(db, ":state=RW");
		}
		else {
			cf_dyn_buf_append_string(db, ":state=WO");
		}

		cf_dyn_buf_append_char(db, ';');
	}

	SINDEX_GRUNLOCK();
}

void
as_sindex_build_smd_key(const char* ns_name, const char* set_name,
		const char* bin_name, const char* cdt_ctx, const char* exp,
		as_sindex_type itype, as_particle_type ktype, char* smd_key)
{
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

int32_t
as_sindex_cdt_ctx_b64_decode(const char* ctx_b64, uint32_t ctx_b64_len,
		uint8_t** buf_r)
{
	uint32_t buf_sz = cf_b64_decoded_buf_size(ctx_b64_len);
	uint32_t buf_sz_out;
	uint8_t* buf;

	buf = cf_malloc(buf_sz);

	if (! cf_b64_validate_and_decode(ctx_b64, ctx_b64_len, buf, &buf_sz_out)) {
		cf_free(buf);
		return -1;
	}

	msgpack_vec vecs[1];
	msgpack_in_vec mv = {
			.n_vecs = 1,
			.vecs = vecs
	};

	vecs[0].buf = buf;
	vecs[0].buf_sz = buf_sz_out;
	vecs[0].offset = 0;

	if (! cdt_context_read_check_peek(&mv)) {
		cf_free(buf);
		return -2;
	}

	bool was_modified;

	buf_sz = msgpack_compactify(buf, (uint32_t)buf_sz, &was_modified);

	if (buf_sz == 0) {
		cf_free(buf);
		return -2;
	}

	if (was_modified) {
		cf_free(buf);
		return -3;
	}

	*buf_r = buf;

	return (int32_t)buf_sz_out;
}

int32_t
as_sindex_exp_b64_decode(const char* exp_b64, uint32_t exp_b64_len,
		uint8_t** buf_r) {
	uint32_t buf_sz = cf_b64_decoded_buf_size(exp_b64_len);
	uint8_t *buf;
	uint32_t buf_sz_out;

	buf = cf_malloc(buf_sz);

	if (! cf_b64_validate_and_decode(exp_b64, exp_b64_len, buf, &buf_sz_out)) {
		cf_free(buf);
		return -1;
	}

	*buf_r = buf;

	return (int32_t)buf_sz_out;
}

bool
as_sindex_validate_exp(const char* exp_b64)
{
	exp_def e_def = { 0 };

	if (! parse_exp(exp_b64, &e_def)) {
		return false;
	}

	free_exp_def(&e_def);

	return true;
}


//==========================================================
// Local helpers - create, delete, rename sindexes.
//

static void
as_sindex_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		const as_smd_item* item = cf_vector_get_ptr(items, i);
		as_sindex_def def = { 0 };

		if (! smd_item_to_def(item->key, item->value, &def)) {
			continue;
		}

		if (item->value != NULL) {
			smd_create(&def, accept_type == AS_SMD_ACCEPT_OPT_START);
		}
		else {
			smd_drop(&def);
		}

		as_sindex_def_free(&def);
	}
}

static bool
smd_item_to_def(const char* smd_key, const char* smd_value, as_sindex_def* def)
{
	// ns-name|<set-name>|bin-name|itype|ktype
	// ns-name|<set-name>|bin-name|c<cdt-context>|itype|ktype
	// ns-name|<set-name>||e<exp-b64>|itype|ktype

	const char* read = smd_key;
	const char* tok = strchr(read, TOK_CHAR_DELIMITER);

	if (! tok) {
		cf_warning(AS_SINDEX, "smd - namespace name missing delimiter");
		return false;
	}

	uint32_t ns_name_len = (uint32_t)(tok - read);

	def->ns = as_namespace_get_bybuf((const uint8_t*)read, ns_name_len);

	if (def->ns == NULL) { // normal if namespace is not on this node
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
	// else - set_name remains empty - ok.

	read = tok + 1;
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

		// Skip context parsing for now to avoid malloc (parsed at the end).
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

		// Skip expression parsing for now to avoid malloc (parsed at the end).
		read = tok + 1;
		tok = strchr(read, TOK_CHAR_DELIMITER);
	}

	// Validate bin name only if it is a non-expression sindex.
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

	// Handle sindex name (SMD value) if it's there.

	if (smd_value != NULL) {
		if (strlen(smd_value) >= INAME_MAX_SZ) {
			cf_warning(AS_SINDEX, "smd - iname too long");
			return false;
		}

		strcpy(def->iname, smd_value);
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

	return true;
}

static void
smd_create(as_sindex_def* def, bool startup)
{
	SINDEX_GWLOCK();

	as_namespace* ns = def->ns;

	as_sindex* cur_si = si_by_iname(ns, def->iname);

	if (cur_si != NULL) {
		// For now, no special treatment if definition matches.
		cf_warning(AS_SINDEX, "SINDEX CREATE: iname already in use - ignoring %s",
				def->iname);
		SINDEX_GWUNLOCK();
		return;
	}

	as_set* p_set = NULL;
	uint16_t set_id = INVALID_SET_ID;

	if (def->set_name[0] != '\0' && as_namespace_get_create_set_w_len(ns,
			def->set_name, strlen(def->set_name), &p_set, &set_id) != 0) {
		cf_warning(AS_SINDEX, "SINDEX CREATE : failed get-create set %s",
				def->set_name);
		SINDEX_GWUNLOCK();
		return;
	}

	uint8_t* ctx_buf = NULL;
	int32_t ctx_buf_sz = 0;
	exp_def e_def = { 0 };

	if (def->ctx_b64 != NULL) {
		ctx_buf_sz = as_sindex_cdt_ctx_b64_decode(def->ctx_b64,
				(uint32_t)strlen(def->ctx_b64), &ctx_buf);

		if (ctx_buf_sz < 0) {
			cf_warning(AS_SINDEX, "SINDEX CREATE: invalid cdt context decode result %d",
					ctx_buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}
	else if (def->exp_b64 != NULL && ! parse_exp(def->exp_b64, &e_def)) {
			SINDEX_GWUNLOCK();
			return;
	}

	if ((cur_si = si_by_defn(ns, set_id, def->bin_name, def->ktype, def->itype,
			e_def.buf, (uint32_t)e_def.buf_sz, ctx_buf, (uint32_t)ctx_buf_sz))
			!= NULL) {
		cf_info(AS_SINDEX, "SINDEX CREATE: renaming %s to %s", cur_si->iname,
				def->iname);
		rename_sindex(cur_si, def->iname);
		SINDEX_GWUNLOCK();

		if (ctx_buf != NULL) {
			cf_free(ctx_buf);
		}

		free_exp_def(&e_def);
		return;
	}

	if (as_sindex_n_sindexes(ns) == MAX_N_SINDEXES) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: at sindex limit - ignoring %s",
				def->iname);
		SINDEX_GWUNLOCK();

		if (ctx_buf != NULL) {
			cf_free(ctx_buf);
		}

		free_exp_def(&e_def);
		return;
	}

	cf_info(AS_SINDEX, "SINDEX CREATE: request received for %s:%s via smd",
			ns->name, def->iname);

	as_sindex* si = cf_rc_alloc(sizeof(as_sindex));

	*si = (as_sindex){
			.ns = ns,
			.set_id = set_id,
			.ktype = def->ktype,
			.itype = def->itype,
			.ctx_b64 = def->ctx_b64,
			.ctx_buf = ctx_buf,
			.ctx_buf_sz = (uint32_t)ctx_buf_sz,
			.exp = e_def.exp,
			.exp_b64 = def->exp_b64,
			.exp_buf = e_def.buf,
			.exp_buf_sz = (uint32_t)e_def.buf_sz,
			.exp_bin_names = e_def.bin_names,
			.n_btrees = AS_PARTITIONS
	};

	strcpy(si->iname, def->iname);
	strcpy(si->set_name, def->set_name);
	strcpy(si->bin_name, def->bin_name);

	// These are now owned by si - don't free outside.
	def->ctx_b64 = NULL;
	def->exp_b64 = NULL;

	if (ns->flat_sindexes == NULL) {
		add_to_sindexes(si);
		as_sindex_tree_create(si);
	}
	else {
		// Also inserts si in sindexes array, and marks si readable if so.
		as_sindex_tree_resume(si);
	}

	add_sindex(si);

	if (def->set_name[0] == '\0') {
		ns->n_setless_sindexes++;
	}
	else {
		p_set->n_sindexes++;
	}

	// Startup has its own mechanism to populate.
	if (startup) {
		SINDEX_GWUNLOCK();
		return;
	}

	as_fence_seq();

	if (p_set == NULL ? ns->n_objects == 0 : p_set->n_objects == 0) {
		// Shortcut if the set is empty.

		si->readable = true;
		si->populate_pct = 100;

		cf_info(AS_SINDEX, "{%s} empty sindex %s ready", ns->name, si->iname);
	}
	else {
		as_sindex_populate_add(si);
	}

	SINDEX_GWUNLOCK();
}

static void
smd_drop(as_sindex_def* def)
{
	SINDEX_GWLOCK();

	as_namespace* ns = def->ns;
	uint16_t set_id = INVALID_SET_ID;

	if (def->set_name[0] != '\0' && (set_id =
			as_namespace_get_set_id(ns, def->set_name)) == INVALID_SET_ID) {
		cf_warning(AS_SINDEX, "SINDEX DROP: set '%s' not found", def->set_name);
		SINDEX_GWUNLOCK();
		return;
	}

	uint8_t* ctx_buf = NULL;
	int32_t ctx_buf_sz = 0;
	char* ctx_b64 = def->ctx_b64;
	uint8_t* exp_buf = NULL;
	int32_t exp_buf_sz = 0;
	char* exp_b64 = def->exp_b64;

	if (ctx_b64 != NULL) {
		ctx_buf_sz = as_sindex_cdt_ctx_b64_decode(ctx_b64,
				(uint32_t) strlen(ctx_b64), &ctx_buf);

		if (ctx_buf_sz < 0) {
			cf_warning(AS_SINDEX, "SINDEX DROP: invalid cdt context decode result %d",
					ctx_buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}
	else if (exp_b64 != NULL) {
		exp_buf_sz = as_sindex_exp_b64_decode(exp_b64,
				(uint32_t)strlen(exp_b64), &exp_buf);

		int32_t buf_sz = exp_buf_sz;

		if (buf_sz < 0) {
			cf_warning(AS_SINDEX, "SINDEX DROP: invalid expression decode result %d",
					buf_sz);
			SINDEX_GWUNLOCK();
			return;
		}
	}

	as_sindex* si = si_by_defn(ns, set_id, def->bin_name, def->ktype,
			def->itype, exp_buf, (uint32_t)exp_buf_sz, ctx_buf,
			(uint32_t)ctx_buf_sz);

	if (ctx_buf != NULL) {
		cf_free(ctx_buf);
	}

	if (exp_buf != NULL) {
		cf_free(exp_buf);
	}

	if (si == NULL) {
		cf_warning(AS_SINDEX, "SINDEX DROP: defn not found");
		SINDEX_GWUNLOCK();
		return;
	}

	si->dropped = true; // allow queries, populate, GC, collect-stats to abort

	SINDEX_GWUNLOCK();

	cf_info(AS_SINDEX, "SINDEX DROP: request received for %s:%s via smd",
			ns->name, si->iname);

	// Wait for queries etc. to be done with this sindex.
	while (si->n_jobs != 0) {
		usleep(100);
	}

	as_fence_acq();

	// At this point, no queries etc. can operate on this sindex. It's safe to
	// remove it and allow transactions to vacate/recycle references in the
	// sindex without harming the queries etc. (See AER-6611.)

	SINDEX_GWLOCK();

	drop_from_sindexes(si); // must precede bin_bitmap_clear()

	delete_sindex(si);

	if (def->set_name[0] == '\0') {
		ns->n_setless_sindexes--;
	}
	else {
		as_set* p_set = as_namespace_get_set_by_name(ns, def->set_name);

		p_set->n_sindexes--;
	}

	SINDEX_GWUNLOCK();

	// Release original rc-alloc ref-count.
	as_sindex_release(si);
}

static void
rename_sindex(as_sindex* si, const char* iname)
{
	as_namespace* ns = si->ns;

	cf_shash_delete(ns->sindex_iname_hash, si->iname);
	cf_shash_put(ns->sindex_iname_hash, iname, &si);

	memcpy(si->iname, iname, INAME_MAX_SZ); // keep iname 0-padded
}

static void
add_sindex(as_sindex* si)
{
	as_namespace* ns = si->ns;

	defn_hash_put(si);
	cf_shash_put(ns->sindex_iname_hash, si->iname, &si);
}

static void
delete_sindex(as_sindex* si)
{
	as_namespace* ns = si->ns;

	defn_hash_delete(si);
	cf_shash_delete(ns->sindex_iname_hash, si->iname);
}

static bool
parse_exp(const char* exp_b64, exp_def* e_def_r)
{
	uint8_t* buf = NULL;
	int32_t buf_sz = as_sindex_exp_b64_decode(exp_b64,
			(uint32_t)strlen(exp_b64), &buf);

	if (buf_sz < 0) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression decode result %d",
				buf_sz);
		return false;
	}

	cf_vector* bin_names = cf_vector_create(AS_BIN_NAME_MAX_SZ, 10, 0);
	as_exp* exp = as_exp_build_buf(buf, (uint32_t)buf_sz, false, bin_names);

	if (exp == NULL) {
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression %s", exp_b64);
		cf_free(buf);
		cf_vector_destroy(bin_names);
		return false;
	}

	bool unsupported_exp = false;

	if ((exp->flags & AS_EXP_HAS_NON_DIGEST_META) != 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression %s - has non-digest metadata",
				exp_b64);
	}

	if ((exp->flags & AS_EXP_HAS_REC_KEY) != 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression %s - has record key",
				exp_b64);
	}

	if ((exp->flags & AS_EXP_HAS_DIGEST_MOD) == 0 &&
			cf_vector_size(bin_names) == 0) {
		unsupported_exp = true;
		cf_warning(AS_SINDEX, "SINDEX CREATE: invalid expression %s - needs digest modifier or bins",
				exp_b64);
	}

	if (unsupported_exp) {
		as_exp_destroy(exp);
		cf_free(buf);
		cf_vector_destroy(bin_names);
		return false;
	}

	// Transfer responsibility of freeing to caller.
	*e_def_r = (exp_def){
		.exp = exp,
		.buf = buf,
		.buf_sz = buf_sz,
		.bin_names = bin_names
	};

	return true;
}

static void
free_exp_def(exp_def* e_def)
{
	if (e_def->exp != NULL) {
		as_exp_destroy(e_def->exp);
	}

	if (e_def->buf != NULL) {
		cf_free(e_def->buf);
	}

	if (e_def->bin_names != NULL) {
		cf_vector_destroy(e_def->bin_names);
	}
}


//==========================================================
// Local helpers - set+(bin-name or exp) hash.
//

static uint32_t
defn_hash_fn(const void* key)
{
	return cf_wyhash32((const uint8_t*)key, sizeof(defn_hash_key));
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


//==========================================================
// Local helpers - populate sbin.
//

static uint32_t
populate_sbins(as_namespace* ns, uint16_t set_id, const as_bin* b,
		as_sindex_bin* sbins, as_sindex_op op)
{
	cf_ll* si_ll = si_list_by_defn(ns, set_id, b->name, NULL, 0);

	if (si_ll == NULL) {
		return 0;
	}

	uint32_t n_populated = 0;
	cf_ll_element* ele = cf_ll_get_head(si_ll);

	while (ele != NULL) {
		defn_hash_ele* si_ele = (defn_hash_ele*)ele;
		as_sindex* si = si_ele->si;

		n_populated += populate_sbin_si(si, b, &sbins[n_populated], op);

		ele = ele->next;
	}

	return n_populated;
}

static uint32_t
populate_sbin_si(as_sindex* si, const as_bin* b, as_sindex_bin* sbin,
		as_sindex_op op)
{
	init_sbin(sbin, op, si);

	if (sbin_from_bin(si, b, sbin)) {
		as_sindex_reserve(si);
		// Release & free will happen once sbin is updated in sindex tree.
		return 1;
	}

	sbin_free(sbin);

	return 0;
}


//==========================================================
// Local helpers - sindex lookup.
//

static cf_ll*
si_list_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name,
		const uint8_t* exp_buf, uint32_t exp_buf_sz)
{
	defn_hash_key key = { .set_id = set_id };

	defn_hash_generate_key(bin_name, exp_buf, exp_buf_sz, &key);

	cf_ll* si_ll = NULL;

	cf_shash_get(ns->sindex_defn_hash, &key, &si_ll);

	return si_ll;
}

static as_sindex*
si_by_defn(const as_namespace* ns, uint16_t set_id, const char* bin_name,
		as_particle_type ktype, as_sindex_type itype, const uint8_t* exp_buf,
		uint32_t exp_buf_sz, const uint8_t* ctx_buf, uint32_t ctx_buf_sz)
{
	cf_ll* si_ll = si_list_by_defn(ns, set_id, bin_name, exp_buf, exp_buf_sz);

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

static as_sindex*
si_by_iname(const as_namespace* ns, const char* iname)
{
	size_t iname_len = strlen(iname);

	if (iname_len == 0 || iname_len >= INAME_MAX_SZ) {
		cf_warning(AS_SINDEX, "bad index name size %zu", iname_len);
		return false;
	}

	char padded_iname[INAME_MAX_SZ] = { 0 };

	strcpy(padded_iname, iname);

	as_sindex* si = NULL;

	cf_shash_get(ns->sindex_iname_hash, padded_iname, &si);

	return si;
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


//==========================================================
// Local helpers - sbins from bins.
//

static bool
sbin_from_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	as_particle_type type = as_bin_get_particle_type(b);
	as_bin ctx_bin = { 0 };

	if (si->ctx_buf != NULL) {
		if (type != AS_PARTICLE_TYPE_LIST && type != AS_PARTICLE_TYPE_MAP) {
			return false;
		}

		if (! as_bin_cdt_get_by_context(b, si->ctx_buf, si->ctx_buf_sz,
				&ctx_bin)) {
			return false;
		}

		type = as_bin_get_particle_type(&ctx_bin);

		if (type == AS_PARTICLE_TYPE_GEOJSON &&
				! as_bin_cdt_context_geojson_parse(&ctx_bin)) {
			return false;
		}

		b = &ctx_bin;
	}

	bool rv;

	switch(si->itype) {
	case AS_SINDEX_ITYPE_DEFAULT:
		rv = (type == si->ktype && sbin_from_simple_bin(si, b, sbin));
		break;
	case AS_SINDEX_ITYPE_LIST:
		rv = (type == AS_PARTICLE_TYPE_LIST && sbin_from_cdt_bin(si, b, sbin));
		break;
	case AS_SINDEX_ITYPE_MAPKEYS:
	case AS_SINDEX_ITYPE_MAPVALUES:
		rv = (type == AS_PARTICLE_TYPE_MAP && sbin_from_cdt_bin(si, b, sbin));
		break;
	default:
		cf_crash(AS_SINDEX, "invalid index type %d", si->itype);
	}

	if (si->ctx_buf != NULL) {
		as_bin_particle_destroy(&ctx_bin);
	}

	return rv;
}

static bool
sbin_from_simple_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	as_particle_type type = as_bin_get_particle_type(b);

	if (type == AS_PARTICLE_TYPE_INTEGER) {
		add_value_to_sbin(sbin, as_bin_particle_integer_value(b));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_STRING) {
		char* str;
		uint32_t len = as_bin_particle_string_ptr(b, &str);

		if (len > MAX_STRING_KSIZE) {
			cf_ticker_warning(AS_SINDEX, "failed sindex on bin %s - string longer than %u",
					si->bin_name, MAX_STRING_KSIZE);
			return false;
		}

		add_value_to_sbin(sbin, as_sindex_string_to_bval(str, len));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_BLOB) {
		uint8_t* blob;
		uint32_t sz = as_bin_particle_blob_ptr(b, &blob);

		if (sz > MAX_BLOB_KSIZE) {
			cf_ticker_warning(AS_SINDEX, "failed sindex on bin %s - blob longer than %u",
					si->bin_name, MAX_BLOB_KSIZE);
			return false;
		}

		add_value_to_sbin(sbin, as_sindex_blob_to_bval(blob, sz));

		return true;
	}

	if (type == AS_PARTICLE_TYPE_GEOJSON) {
		// GeoJSON is like AS_PARTICLE_TYPE_STRING when reading the value and
		// AS_PARTICLE_TYPE_INTEGER for adding the result to the index.
		uint64_t* cells;
		size_t ncells = as_bin_particle_geojson_cellids(b, &cells);

		if (ncells == 0) {
			// Empty coordinate arrays are "null objects".
			return false;
		}

		for (size_t ndx = 0; ndx < ncells; ++ndx) {
			add_value_to_sbin(sbin, (int64_t)cells[ndx]);
		}

		return true;
	}

	cf_crash(AS_SINDEX, "invalid bin type %d", type);
	return false;
}

static bool
sbin_from_cdt_bin(as_sindex* si, const as_bin* b, as_sindex_bin* sbin)
{
	switch (si->itype) {
	case AS_SINDEX_ITYPE_LIST:
		as_bin_list_foreach(b, add_listvalues_foreach, sbin);
		break;
	case AS_SINDEX_ITYPE_MAPKEYS:
		as_bin_map_foreach(b, add_mapkeys_foreach, sbin);
		break;
	case AS_SINDEX_ITYPE_MAPVALUES:
		as_bin_map_foreach(b, add_mapvalues_foreach, sbin);
		break;
	default:
		cf_crash(AS_SINDEX, "unexpected");
	}

	return sbin->n_values != 0;
}


//==========================================================
// Local helpers - value to sbin.
//

static void
add_value_to_sbin(as_sindex_bin* sbin, int64_t val)
{
	// If this is the first value, assign the value to the embedded field.
	if (sbin->n_values == 0) {
		sbin->val = val;
		sbin->n_values++;
		return;
	}

	if (sbin->values == NULL) {
		sbin->capacity = 32;
		sbin->values = cf_malloc(sbin->capacity * sizeof(int64_t));

		// Note - as used now, copied val is superfluous, we never look at it.
		sbin->values[0] = sbin->val;
	}
	else if (sbin->capacity == sbin->n_values) {
		sbin->capacity = 2 * sbin->capacity;
		sbin->values = cf_realloc(sbin->values,
				sbin->capacity * sizeof(int64_t));
	}

	sbin->values[sbin->n_values++] = val;
}


//==========================================================
// Local helpers - msgpack to sbin - iterator callbacks.
//

static bool
add_listvalues_foreach(msgpack_in* element, void* udata)
{
	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, element, sbin);

	return true;
}

static bool
add_mapkeys_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)val;

	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, key, sbin);

	return true;
}

static bool
add_mapvalues_foreach(msgpack_in* key, msgpack_in* val, void* udata)
{
	(void)key;

	as_sindex_bin* sbin = (as_sindex_bin*)udata;

	add_keytype_from_msgpack(sbin->si->ktype, val, sbin);

	return true;
}


//==========================================================
// Local helpers - msgpack to sbin - convert to ktypes.
//

static void
add_long_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	int64_t v;

	if (! msgpack_get_int64(element, &v)) {
		return;
	}

	add_value_to_sbin(sbin, v);
}

static void
add_string_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t str_sz;
	const uint8_t* str = msgpack_get_bin(element, &str_sz);

	if (str_sz == 0 || str == NULL || *str != AS_PARTICLE_TYPE_STRING) {
		return;
	}

	// Skip as_bytes type.
	str++;
	str_sz--;

	add_value_to_sbin(sbin, as_sindex_string_to_bval((const char*)str, str_sz));
}

static void
add_blob_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t blob_sz;
	const uint8_t* blob = msgpack_get_bin(element, &blob_sz);

	if (blob_sz == 0 || blob == NULL || *blob != AS_PARTICLE_TYPE_BLOB) {
		return;
	}

	// Skip as_bytes type.
	blob++;
	blob_sz--;

	add_value_to_sbin(sbin, as_sindex_blob_to_bval(blob, blob_sz));
}

static void
add_geojson_from_msgpack(msgpack_in* element, as_sindex_bin* sbin)
{
	uint32_t json_sz;
	const uint8_t* json = msgpack_get_bin(element, &json_sz);

	if (json_sz == 0 || json == NULL || *json != AS_PARTICLE_TYPE_GEOJSON) {
		return;
	}

	// Skip as_bytes type.
	json++;
	json_sz--;

	uint64_t cellid;
	geo_region_t region;

	if (! as_geojson_parse(NULL, (const char*)json, json_sz, &cellid,
			&region)) {
		return;
	}

	if (cellid != 0) { // POINT
		add_value_to_sbin(sbin, (int64_t)cellid);
	}
	else { // REGION
		uint32_t ncells;
		uint64_t outcells[MAX_REGION_CELLS];

		if (! geo_region_cover(NULL, region, MAX_REGION_CELLS, outcells, NULL,
				NULL, &ncells)) {
			cf_warning(AS_SINDEX, "geo_region_cover failed");
			geo_region_destroy(region);
			return;
		}

		geo_region_destroy(region);

		for (uint32_t i = 0; i < ncells; i++) {
			add_value_to_sbin(sbin, (int64_t)outcells[i]);
		}
	}
}


//==========================================================
// Local helpers - type utilities.
//

static char const*
ktype_str(as_particle_type ktype)
{
	switch (ktype) {
	case AS_PARTICLE_TYPE_INTEGER: return "numeric";
	case AS_PARTICLE_TYPE_STRING: return "string";
	case AS_PARTICLE_TYPE_BLOB: return "blob";
	case AS_PARTICLE_TYPE_GEOJSON: return "geo2dsphere";
	default:
		cf_crash(AS_SINDEX, "invalid ktype %d", ktype);
	}

	return NULL;
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

static as_sindex_type
itype_from_smd_char(char c)
{
	switch (c) {
	case '.': return AS_SINDEX_ITYPE_DEFAULT;
	case 'L': return AS_SINDEX_ITYPE_LIST;
	case 'K': return AS_SINDEX_ITYPE_MAPKEYS;
	case 'V': return AS_SINDEX_ITYPE_MAPVALUES;
	default:
		cf_warning(AS_SINDEX, "invalid smd type %c", c);
		return AS_SINDEX_N_ITYPES; // since there's no named illegal value
	}
}

// 'c' (context) and 'e' (expression) are reserved in the smd key.
static char
itype_to_smd_char(as_sindex_type itype)
{
	switch (itype) {
	case AS_SINDEX_ITYPE_DEFAULT: return '.';
	case AS_SINDEX_ITYPE_LIST: return 'L';
	case AS_SINDEX_ITYPE_MAPKEYS: return 'K';
	case AS_SINDEX_ITYPE_MAPVALUES: return 'V';
	default:
		cf_crash(AS_SINDEX, "invalid type %d", itype);
	}

	return '\0';
}


//==========================================================
// Local helpers - stats.
//

static void*
run_cardinality(void* udata)
{
	as_namespace* ns = (as_namespace*)udata;

	while (true) {
		for (uint32_t i = 0; i < MAX_N_SINDEXES; i++) {
			SINDEX_GRLOCK();

			as_sindex* si = ns->sindexes[i];

			if (si == NULL || si->dropped || ! si->readable) {
				SINDEX_GRUNLOCK();
				continue;
			}

			as_sindex_job_reserve(si);

			SINDEX_GRUNLOCK();

			as_sindex_tree_collect_cardinality(si);

			as_sindex_job_release(si);
		}

		sleep(CARDINALITY_PERIOD);
	}

	return NULL;
}
