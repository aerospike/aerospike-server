/*
 * udf_cask.c
 *
 * Copyright (C) 2012-2020 Aerospike, Inc.
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

#include "base/udf_cask.h"

#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "jansson.h"

#include "aerospike/as_module.h"
#include "aerospike/mod_lua.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_crypto.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "log.h"

#include "base/cfg.h"
#include "base/smd.h"
#include "base/thr_info.h"


//==========================================================
// Typedefs & constants.
//

#define LUA_TYPE_STR "LUA"

#define MAX_USER_PATH_SZ 256 // from g_config.mod_lua.user_path
#define MAX_FILE_NAME_SZ 128
#define MAX_FILE_PATH_SZ (MAX_USER_PATH_SZ + MAX_FILE_NAME_SZ)

#define MAX_UDF_CONTENT_LENGTH (1024 * 1024)

typedef struct udf_get_data_s {
	cf_dyn_buf* db;
	bool done;
} udf_get_data_t;


//==========================================================
// Forward declarations.
//

// SMD callbacks.
static void udf_cask_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type);
static void udf_cask_smd_get_all_cb(const cf_vector* items, void* udata);

// File utilities.
static uint8_t* file_read(const char* filename, size_t* p_sz, int* err);
static bool file_write(const char* filename, const uint8_t* buf, size_t sz);
static void file_remove(const char* filename);


//==========================================================
// Inlines & macros.
//

static inline bool
is_valid_udf_type(const char* type)
{
	return strcmp(type, LUA_TYPE_STR) == 0;
}


//==========================================================
// Public API - startup.
//

void
udf_cask_init(void)
{
	// Delete existing files in user path on startup.
	DIR* dir = opendir(g_config.mod_lua.user_path);

	if (dir == NULL) {
		cf_crash(AS_UDF, "could not open udf directory %s: %s",
				g_config.mod_lua.user_path, cf_strerror(errno));
	}

	struct dirent* entry = NULL;

	while ((entry = readdir(dir)) != NULL) {
		// readdir() also reads "." and ".." entries.
		if (strcmp(entry->d_name, ".") == 0 ||
				strcmp(entry->d_name, "..") == 0) {
			continue;
		}

		char file_path[MAX_USER_PATH_SZ + strlen(entry->d_name) + 1];

		sprintf(file_path, "%s/%s", g_config.mod_lua.user_path, entry->d_name);

		if (remove(file_path) != 0) {
			cf_warning(AS_UDF, "failed to remove file %s: %s)", file_path,
					cf_strerror(errno));
		}
	}

	closedir(dir);

	as_smd_module_load(AS_SMD_MODULE_UDF, udf_cask_smd_accept_cb, NULL, NULL);
}


//==========================================================
// Public API - info commands.
//

int
udf_cask_info_clear_cache(char* name, char* params, cf_dyn_buf* out)
{
	cf_debug(AS_UDF, "UDF CASK INFO CLEAR CACHE");

	// Command format:
	// "udf-clear-cache:"

	mod_lua_wrlock(&mod_lua);

	as_module_event e = { .type = AS_MODULE_EVENT_CLEAR_CACHE };

	as_module_update(&mod_lua, &e);

	mod_lua_unlock(&mod_lua);

	cf_dyn_buf_append_string(out, "ok");

	return 0;
}

int
udf_cask_info_get(char* name, char* params, cf_dyn_buf* out)
{
	cf_debug(AS_UDF, "UDF CASK INFO GET");

	// Command format:
	// "udf-get:filename=<name>"

	char filename[MAX_FILE_NAME_SZ];
	int filename_len = (int)sizeof(filename);

	if (as_info_parameter_get(params, "filename", filename,
			&filename_len) != 0) {
		cf_warning(AS_UDF, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	mod_lua_rdlock(&mod_lua);

	size_t buf_sz;
	int err = 0; // quiet older distros
	uint8_t* buf = file_read(filename, &buf_sz, &err);

	mod_lua_unlock(&mod_lua);

	if (buf != NULL) {
		// The "content" field is the raw file base-64 encoded.

		size_t content_len = cf_b64_encoded_len(buf_sz);
		uint8_t* content = cf_malloc(content_len);

		cf_b64_encode(buf, buf_sz, (char*)content);

		// Response format:
		// "type=<type>;content=<base-64-encoded-lua-code>"

		// Note - removed "gen" field (unused by client) in 5.8.

		cf_dyn_buf_append_string(out, "type=");
		cf_dyn_buf_append_string(out, LUA_TYPE_STR);
		cf_dyn_buf_append_string(out, ";content=");
		cf_dyn_buf_append_buf(out, content, content_len);
		cf_dyn_buf_append_string(out, ";");

		cf_free(buf);
		cf_free(content);
	}
	else {
		switch (err) {
		case 1:
			cf_dyn_buf_append_string(out, "error=not_found");
			break;
		case 2:
			cf_dyn_buf_append_string(out, "error=empty");
			break;
		default:
			cf_dyn_buf_append_string(out, "error=unknown_error");
			break;
		}
	}

	return 0;
}

int
udf_cask_info_list(char* name, cf_dyn_buf* out)
{
	cf_debug(AS_UDF, "UDF CASK INFO LIST");

	// Command format:
	// "udf-list"

	udf_get_data_t get_data = { .db = out, .done = false };

	// Collects a list with format:
	// "filename=<name>;hash=<hex-string-hash>;type=<type>; ... "

	// The "hash" field is the SMD value SHA1-hashed, as a hex string. The
	// SMD value is a JSON-formatted object, within which is base-64-encoded
	// LUA code (i.e. base-64-encoded raw file content). Note - this is
	// different to what the client generates in the "udf-get" command.

	as_smd_get_all(AS_SMD_MODULE_UDF, udf_cask_smd_get_all_cb, &get_data);

	return 0;
}

int
udf_cask_info_put(char* name, char* params, cf_dyn_buf* out)
{
	cf_debug(AS_UDF, "UDF CASK INFO PUT");

	// Command format:
	// "udf-put:filename=<name>;content-len=<len>[;udf-type=<type>];content=<base-64-encoded-lua-code>"

	char filename[MAX_FILE_NAME_SZ];
	int filename_len = (int)sizeof(filename);

	if (as_info_parameter_get(params, "filename", filename,
			&filename_len) != 0) {
		cf_warning(AS_UDF, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	char* dot = strchr(filename, '.');

	if (dot == NULL || dot == filename || strlen(dot) == 1) {
		// Invalid - no dot, OR dot at beginning, OR dot at end.
		cf_warning(AS_UDF, "filename must have an extension");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	char content_len[10 + 1];
	int content_len_len = (int)sizeof(content_len);

	if (as_info_parameter_get(params, "content-len", content_len,
			&content_len_len) != 0) {
		cf_warning(AS_UDF, "invalid or missing content-len");
		cf_dyn_buf_append_string(out, "error=invalid_content_len");
		return 0;
	}

	char udf_type[16];
	int udf_type_len = (int)sizeof(udf_type);

	int pg = as_info_parameter_get(params, "udf-type", udf_type, &udf_type_len);

	if (pg == -1) {
		// Default is LUA.
		strcpy(udf_type, LUA_TYPE_STR);
	}
	else if (pg == -2 || ! is_valid_udf_type(udf_type)) {
		cf_warning(AS_UDF, "invalid udf-type %s", udf_type);
		cf_dyn_buf_append_string(out, "error=invalid_udf_type");
		return 0;
	}

	uint32_t len32;

	if (cf_str_atoi_u32(content_len, &len32) != 0 || len32 % 4 != 0) {
		cf_warning(AS_UDF, "invalid content-len %s", udf_type);
		cf_dyn_buf_append_string(out, "error=invalid_content_len");
		return 0;
	}

	uint32_t sz32 = len32 + 1;

	char* content = (char*)cf_malloc((size_t)sz32);
	int i_content_len = (int)sz32;

	if (as_info_parameter_get(params, "content", content,
			&i_content_len) != 0 || (uint32_t)i_content_len != len32) {
		cf_warning(AS_UDF, "invalid or missing content");
		cf_dyn_buf_append_string(out, "error=invalid_content");
		cf_free(content);
		return 0;
	}

	uint32_t encoded_len = len32;
	uint32_t decoded_len = cf_b64_decoded_buf_size(encoded_len);

	if (decoded_len > MAX_UDF_CONTENT_LENGTH) {
		cf_warning(AS_UDF, "lua file size:%d > 1MB", decoded_len);
		cf_dyn_buf_append_string(out, "error=invalid_udf_content_len,>1M");
		cf_free(content);
		return 0;
	}

	char* decoded_str = cf_malloc(decoded_len);

	if (! cf_b64_validate_and_decode(content, encoded_len,
			(uint8_t*)decoded_str, &decoded_len) ) {
		cf_warning(AS_UDF, "invalid base64 content");
		cf_dyn_buf_append_string(out, "error=invalid_base64_content");
		cf_free(decoded_str);
		cf_free(content);
		return 0;
	}

	as_module_error err;
	int rv = as_module_validate(&mod_lua, NULL, filename, decoded_str,
			decoded_len, &err);

	cf_free(decoded_str);

	if (rv != 0) {
		cf_warning(AS_UDF, "udf-put: compile error: [%s:%d] %s", err.file,
				err.line, err.message);

		cf_dyn_buf_append_string(out, "error=compile_error");
		cf_dyn_buf_append_string(out, ";file=");
		cf_dyn_buf_append_string(out, err.file);
		cf_dyn_buf_append_string(out, ";line=");
		cf_dyn_buf_append_uint32(out, err.line);

		uint32_t message_len = strlen(err.message);
		uint32_t enc_message_len = cf_b64_encoded_len(message_len);
		char enc_message[enc_message_len];

		cf_b64_encode((const uint8_t*)err.message, message_len, enc_message);

		cf_dyn_buf_append_string(out, ";message=");
		cf_dyn_buf_append_buf(out, (uint8_t*)enc_message, enc_message_len);

		cf_free(content);
		return 0;
	}

	json_t* udf_obj = json_object();

	int e = 0;

	e += json_object_set_new(udf_obj, "content64", json_string(content));
	e += json_object_set_new(udf_obj, "type", json_string(udf_type));
	e += json_object_set_new(udf_obj, "name", json_string(filename));

	cf_free(content);

	if (e != 0) {
		cf_warning(AS_UDF, "could not encode UDF object");
		cf_dyn_buf_append_string(out, "error=json_error");
		json_decref(udf_obj);
		return 0;
	}

	char* udf_obj_str = json_dumps(udf_obj, 0);

	json_decref(udf_obj);

	cf_debug(AS_UDF, "created json object %s", udf_obj_str);

	if (as_smd_set_blocking(AS_SMD_MODULE_UDF, filename, udf_obj_str, 0)) {
		cf_info(AS_UDF, "UDF module %s/%s registered",
				g_config.mod_lua.user_path, filename);
		cf_dyn_buf_append_string(out, "ok");
	}
	else {
		cf_warning(AS_UDF, "UDF module %s/%s registration timed out",
				g_config.mod_lua.user_path, filename);
		cf_dyn_buf_append_string(out, "error=timeout");
	}

	cf_free(udf_obj_str);

	return 0;
}

int
udf_cask_info_remove(char* name, char* params, cf_dyn_buf* out)
{
	cf_debug(AS_UDF, "UDF CASK INFO REMOVE");

	// Command format:
	// "udf-remove:filename=<name>"

	char filename[MAX_FILE_NAME_SZ];
	int filename_len = (int)sizeof(filename);

	if (as_info_parameter_get(params, "filename", filename,
			&filename_len) != 0) {
		cf_warning(AS_UDF, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	char file_path[strlen(g_config.mod_lua.user_path) + 1 + filename_len + 1];

	sprintf(file_path, "%s/%s", g_config.mod_lua.user_path, filename);

	if (! as_smd_delete_blocking(AS_SMD_MODULE_UDF, filename, 0)) {
		cf_warning(AS_UDF, "UDF module %s remove timed out", file_path);
		cf_dyn_buf_append_string(out, "error=timeout");
		return -1;
	}

	cf_info(AS_UDF, "UDF module %s removed", file_path);
	cf_dyn_buf_append_string(out, "ok");

	return 0;
}


//==========================================================
// Local helpers - SMD callbacks.
//

static void
udf_cask_smd_accept_cb(const cf_vector* items, as_smd_accept_type accept_type)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		as_smd_item* item = cf_vector_get_ptr(items, i);

		if (strlen(item->key) >= MAX_FILE_NAME_SZ) {
			cf_warning(AS_UDF, "filename %s too long - ignoring", item->key);
			continue;
		}

		// Remove (never at startup).
		if (item->value == NULL) {
			cf_assert(accept_type != AS_SMD_ACCEPT_OPT_START, AS_UDF,
					"got smd tombstone at startup");

			mod_lua_wrlock(&mod_lua);

			file_remove(item->key);

			as_module_event e = {
					.type = AS_MODULE_EVENT_FILE_REMOVE,
					.data.filename = item->key
			};

			as_module_update(&mod_lua, &e);

			mod_lua_unlock(&mod_lua);

			continue;
		}
		// else - set (startup and runtime).

		json_error_t json_error;
		json_t* item_obj = json_loads(item->value, 0, &json_error);

		if (item_obj == NULL) {
			cf_warning(AS_UDF, "failed to parse UDF \"%s\" with JSON error: %s ; source: %s ; line: %d ; column: %d ; position: %d",
					item->key, json_error.text, json_error.source,
					json_error.line, json_error.column, json_error.position);
			continue;
		}

		json_t* content64_obj = json_object_get(item_obj, "content64");
		const char* content = json_string_value(content64_obj);

		uint32_t encoded_len = strlen(content);
		uint32_t decoded_len = cf_b64_decoded_buf_size(encoded_len);
		char* decoded_str = cf_malloc(decoded_len);

		if (! cf_b64_validate_and_decode(content, encoded_len,
				(uint8_t*)decoded_str, &decoded_len)) {
			cf_warning(AS_UDF, "invalid base64 content for %s", item->key);
			cf_free(decoded_str);
			json_decref(item_obj);
			continue;
		}

		mod_lua_wrlock(&mod_lua);

		bool rv = file_write(item->key, (uint8_t*)decoded_str, decoded_len);

		cf_free(decoded_str);
		json_decref(item_obj);

		if (rv) {
			as_module_event e = {
					.type = AS_MODULE_EVENT_FILE_ADD,
					.data.filename = item->key
			};

			as_module_update(&mod_lua, &e);
		}

		mod_lua_unlock(&mod_lua);
	}
}

static void
udf_cask_smd_get_all_cb(const cf_vector* items, void* udata)
{
	udf_get_data_t* get_data = (udf_get_data_t*)udata;
	cf_dyn_buf* out = get_data->db;

	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		as_smd_item* item = cf_vector_get_ptr(items, i);

		if (item->value == NULL) { // TODO - do we get SMD tombstones here?
			continue;
		}

		cf_dyn_buf_append_string(out, "filename=");
		cf_dyn_buf_append_string(out, item->key);
		cf_dyn_buf_append_string(out, ",");

		unsigned char hash[CF_SHA_DIGEST_LENGTH];

		cf_SHA1((uint8_t*)item->value, strlen(item->value), hash);

		char hex_buf[(CF_SHA_DIGEST_LENGTH * 2) + 1];
		char* at = hex_buf;

		for (uint32_t j = 0; j < CF_SHA_DIGEST_LENGTH; j++) {
			at += sprintf(at, "%02x", hash[i]);
		}

		cf_dyn_buf_append_string(out, "hash=");
		cf_dyn_buf_append_buf(out, (uint8_t*)hex_buf, CF_SHA_DIGEST_LENGTH * 2);
		cf_dyn_buf_append_string(out, ",type=");
		cf_dyn_buf_append_string(out, LUA_TYPE_STR);
		cf_dyn_buf_append_string(out, ";");
	}

	get_data->done = true;
}


//==========================================================
// Local helpers - file utilities.
//

static uint8_t*
file_read(const char* filename, size_t* p_sz, int* err)
{
	char file_path[MAX_FILE_PATH_SZ];

	sprintf(file_path, "%s/%s", g_config.mod_lua.user_path, filename);

	FILE* file = fopen(file_path, "r");

	if (file == NULL) {
		cf_warning(AS_UDF, "failed to open file %s: %s", file_path,
				cf_strerror(errno));
		*err = 1;
		return NULL;
	}

	if (fseek(file, 0, SEEK_END) != 0) {
		cf_crash(AS_UDF, "failed file seek for %s: %s", file_path,
				cf_strerror(errno));
	}

	long int rv = ftell(file);

	if (rv < 0) {
		cf_crash(AS_UDF, "failed file tell for %s: %s", file_path,
				cf_strerror(errno));
	}

	if (rv == 0) {
		cf_warning(AS_UDF, "file %s is empty", file_path);
		fclose(file);
		*err = 2;
		return NULL;
	}

	if (fseek(file, 0, SEEK_SET) != 0) {
		cf_crash(AS_UDF, "failed file seek for %s: %s", file_path,
				cf_strerror(errno));
	}

	size_t sz = (size_t)rv;
	uint8_t* buf = cf_malloc(sz);

	if (fread(buf, sz, 1, file) != 1) {
		cf_warning(AS_UDF, "failed file read for %s: %s", file_path,
				cf_strerror(errno));
		cf_free(buf);
		fclose(file);
		*err = -1;
		return NULL;
	}

	fclose(file);
	*p_sz = sz;

	return buf;
}

static bool
file_write(const char* filename, const uint8_t* buf, size_t sz)
{
	char file_path[MAX_FILE_PATH_SZ];

	sprintf(file_path, "%s/%s", g_config.mod_lua.user_path, filename);

	FILE* file = fopen(file_path, "w");

	if (file == NULL) {
		cf_warning(AS_UDF, "failed to open file %s: %s", file_path,
				cf_strerror(errno));
		return false;
	}

	if (fwrite(buf, sizeof(char), sz, file) != sz) {
		cf_warning(AS_UDF, "failed file write for %s: %s", file_path,
				cf_strerror(errno));
		fclose(file);
		return false;
	}

	fclose(file);

	return true;
}

static void
file_remove(const char* filename)
{
	char file_path[MAX_FILE_PATH_SZ];

	sprintf(file_path, "%s/%s", g_config.mod_lua.user_path, filename);

	unlink(file_path);
}
