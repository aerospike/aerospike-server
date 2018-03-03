/*
 * udf_cast.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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

#include "base/udf_cask.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/sha.h>

#include "jansson.h"

#include "aerospike/as_module.h"
#include "aerospike/mod_lua.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_crypto.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/thr_info.h"
#include "base/system_metadata.h"
#include <sys/stat.h>

char udf_smd_module_name[] = "UDF";
char *as_udf_type_name[] = {"LUA", 0};

static bool g_udf_smd_loaded = false;

static int file_read(char *, uint8_t **, size_t *, unsigned char *);
static int file_write(char *, uint8_t *, size_t, unsigned char *);
static int file_remove(char *);
static int file_generation(char *, uint8_t *, size_t, unsigned char *);

static inline int file_resolve(char * filepath, char * filename, char * ext) {

	char *  p               = filepath;
	char *  user_path       = g_config.mod_lua.user_path;
	size_t  user_path_len   = strlen(user_path);
	int     filename_len    = strlen(filename);

	memcpy(p, user_path, sizeof(char) * user_path_len);
	p += user_path_len;

	memcpy(p, "/", 1);
	p += 1;

	memcpy(p, filename, filename_len);
	p += filename_len;

	if ( ext ) {
		int ext_len = strlen(ext);
		memcpy(p, ext, ext_len);
		p += ext_len;
	}

	p[0] = '\0';

	return 0;
}

static int file_read(char * filename, uint8_t ** content, size_t * content_len, unsigned char * hash) {

	char    filepath[256]   = {0};
	char    line[1024]      = {0};
	size_t  line_len        = sizeof(line);

	file_resolve(filepath, filename, NULL);

	cf_dyn_buf_define(buf);

	FILE *file = fopen(filepath, "r");

	if ( file ) {

		while( fgets(line, line_len, file) != NULL ) {
			cf_dyn_buf_append_string(&buf, line);
		}

		fclose(file);
		file = NULL;

		if ( buf.used_sz > 0 ) {

			char *src = cf_dyn_buf_strdup(&buf);

			file_generation(filepath, (uint8_t *)src, buf.used_sz, hash);

			uint32_t src_len = (uint32_t)buf.used_sz;
			uint32_t out_size = cf_b64_encoded_len(src_len);

			*content = (uint8_t *)cf_malloc(out_size);
			*content_len = out_size;

			cf_b64_encode((const uint8_t*)src, src_len, (char*)(*content));

			cf_free(src);
			src = NULL;

			return 0;
		}

		*content = NULL;
		*content_len = 0;
		return 2;
	}

	*content = NULL;
	*content_len = 0;
	return 1;
}

static int file_write(char * filename, uint8_t * content, size_t content_len, unsigned char * hash) {

	char    filepath[256]   = {0};

	file_resolve(filepath, filename, NULL);

	FILE *file = fopen(filepath, "w");

	if (file == NULL) {
		cf_warning(AS_UDF, "could not open udf put to %s: %s", filepath, cf_strerror(errno));
		return -1;
	}
	int r = fwrite(content, sizeof(char), content_len, file);
	if (r <= 0) {
		cf_warning(AS_UDF, "could not write file %s: %d", filepath, r);
		fclose(file);
		return -1;
	}

	fclose(file);
	file = NULL;

	file_generation(filepath, content, content_len, hash);

	return 0;
}

static int file_remove(char * filename) {
	char filepath[256] = {0};
	file_resolve(filepath, filename, NULL);
	unlink(filepath);
	return 0;
}

static int file_generation(char * filename, uint8_t * content, size_t content_len, unsigned char * hash) {
	unsigned char sha1[128] = {0};
	int len = 20;
	SHA1((const unsigned char *) content, (unsigned long) content_len, (unsigned char *) sha1);
	cf_b64_encode(sha1, len, (char*)hash);
	hash[cf_b64_encoded_len(len)] = 0;
	return 0;
}

// return -1 if not found otherwise the index in as_udf_type_name
static int udf_type_getid(char *type) {
	int index = 0;
	while (as_udf_type_name[index]) {
		if (strcmp( type, as_udf_type_name[index]) == 0 ) {
			return(index);
		}
		index++;
	}
	return(-1);
}

/*
 * Type for user data passed to the get metadata callback.
 */
typedef struct udf_get_data_s {
	cf_dyn_buf *db;        // DynBuf for output.
	pthread_cond_t *cv;    // Condition variable for signaling callback completion.
	pthread_mutex_t *mt;   // Mutex protecting the condition variable.
	bool done;             // Has the callback finished?
} udf_get_data_t;

/*
 * UDF SMD get metadata items callback.
 */
static int udf_cask_get_metadata_cb(char *module, as_smd_item_list_t *items, void *udata)
{
	udf_get_data_t *p_get_data = (udf_get_data_t *) udata;
	cf_dyn_buf *out = p_get_data->db;

	unsigned char   hash[SHA_DIGEST_LENGTH];
	// hex string to be returned to the client
	unsigned char   sha1_hex_buff[CF_SHA_HEX_BUFF_LEN];
	// Currently just return directly for LUA
	uint8_t udf_type = AS_UDF_TYPE_LUA;

	for (int index = 0; index < items->num_items; index++) {
		as_smd_item_t *item = items->item[index];
		cf_debug(AS_UDF, "UDF metadata item[%d]:  module \"%s\" ; key \"%s\" ; value \"%s\" ; generation %u ; timestamp %lu",
				 index, item->module_name, item->key, item->value, item->generation, item->timestamp);
		cf_dyn_buf_append_string(out, "filename=");
		cf_dyn_buf_append_buf(out, (uint8_t *)item->key, strlen(item->key));
		cf_dyn_buf_append_string(out, ",");
		SHA1((uint8_t *)item->value, strlen(item->value), hash);

		// Convert to a hexadecimal string
		cf_convert_sha1_to_hex(hash, sha1_hex_buff);
		cf_dyn_buf_append_string(out, "hash=");
		cf_dyn_buf_append_buf(out, sha1_hex_buff, CF_SHA_HEX_BUFF_LEN);
		cf_dyn_buf_append_string(out, ",type=");
		cf_dyn_buf_append_string(out, as_udf_type_name[udf_type]);
		cf_dyn_buf_append_string(out, ";");
	}

	pthread_mutex_lock(p_get_data->mt);

	p_get_data->done = true;
	int retval = pthread_cond_signal(p_get_data->cv);
	if (retval) {
		cf_warning(AS_UDF, "pthread_cond_signal failed (rv %d)", retval);
	}

	pthread_mutex_unlock(p_get_data->mt);

	return retval;
}

/*
 *  Implementation of the "udf-list" Info. Command.
 */
int udf_cask_info_list(char *name, cf_dyn_buf *out)
{
	cf_debug(AS_UDF, "UDF CASK INFO LIST");

	pthread_mutex_t get_data_mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t get_data_cond_var = PTHREAD_COND_INITIALIZER;

	udf_get_data_t get_data;
	get_data.db = out;
	get_data.cv = &get_data_cond_var;
	get_data.mt = &get_data_mutex;
	get_data.done = false;

	pthread_mutex_lock(&get_data_mutex);

	int retval = as_smd_get_metadata(udf_smd_module_name, "", udf_cask_get_metadata_cb, &get_data);
	if (!retval) {
		do { // [Note:  Loop protects against spurious wakeups.]
			if ((retval = pthread_cond_wait(&get_data_cond_var, &get_data_mutex))) {
				cf_warning(AS_UDF, "pthread_cond_wait failed (rv %d)", retval);
				break;
			}
		} while (!get_data.done);
	} else {
		cf_warning(AS_UDF, "failed to get UDF metadata (rv %d)", retval);
	}

	pthread_mutex_unlock(&get_data_mutex);

	pthread_mutex_destroy(&get_data_mutex);
	pthread_cond_destroy(&get_data_cond_var);

	return retval;
}

/*
 * Reading local directory to get specific module item's contents.
 * In future if needed we can change this to reading from smd metadata. 
 */
int udf_cask_info_get(char *name, char * params, cf_dyn_buf * out) {

	int                 resp                = 0;
	char                filename[128]       = {0};
	int                 filename_len        = sizeof(filename);
	uint8_t *           content             = NULL;
	size_t              content_len         = 0;
	unsigned char       content_gen[256]    = {0};
	uint8_t             udf_type            = AS_UDF_TYPE_LUA;

	cf_debug(AS_INFO, "UDF CASK INFO GET");

	// get (required) script filename
	if ( as_info_parameter_get(params, "filename", filename, &filename_len) ) {
		cf_info(AS_INFO, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	mod_lua_rdlock(&mod_lua);
	// read the script from filesystem
	resp = file_read(filename, &content, &content_len, content_gen);
	mod_lua_unlock(&mod_lua);
	if ( resp ) {
		switch ( resp ) {
			case 1 : {
				cf_dyn_buf_append_string(out, "error=not_found");
				break;
			}
			case 2 : {
				cf_dyn_buf_append_string(out, "error=empty");
				break;
			}
			default : {
				cf_dyn_buf_append_string(out, "error=unknown_error");
				break; // complier complains without a break;
			}
		}
	}
	else {
		// put back the result
		cf_dyn_buf_append_string(out, "gen=");
		cf_dyn_buf_append_string(out, (char *) content_gen);
		cf_dyn_buf_append_string(out, ";type=");
		cf_dyn_buf_append_string(out, as_udf_type_name[udf_type]);
		cf_dyn_buf_append_string(out, ";content=");
		cf_dyn_buf_append_buf(out, content, content_len);
		cf_dyn_buf_append_string(out, ";");
	}

	if ( content ) {
		cf_free(content);
		content = NULL;
	}

	return 0;
}

// An info put call will call system metadata
//
// Data is reflected into json as an object with the following fields
// which can be added to later if necessary, for example, instead of using
// the specific data, it could include the URL to the data
//
// key - name of the UDF file
//
// content64 - base64 encoded data
// type - language to execute
// name - reptition of the name, same as the key

int udf_cask_info_put(char *name, char * params, cf_dyn_buf * out) {

	cf_debug(AS_INFO, "UDF CASK INFO PUT");

	int					rc 					= 0;
	char                filename[128]       = {0};
	int                 filename_len        = sizeof(filename);
	// Content_len from the client and its expected size
	char                content_len[32]     = {0};
	int 		        clen		        = sizeof(content_len);
	// Udf content from the client and its expected length
	char	 		    *udf_content        = NULL;
	int 		        udf_content_len    = 0;
	// Udf type from the client and its expected size
	char 		         type[8]            = {0};
	int 		         type_len 	        = sizeof(type);

	// get (required) script filename
	char *tmp_char;

	if ( as_info_parameter_get(params, "filename", filename, &filename_len)
			|| !(tmp_char = strchr(filename, '.'))               // No extension in filename
			|| tmp_char == filename                              // '.' at the begining of filename
			|| strlen (tmp_char) <= 1) {                         // '.' in filename, but no extnsion e.g. "abc."
		cf_info(AS_INFO, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
		return 0;
	}

	if ( as_info_parameter_get(params, "content-len", content_len, &(clen)) ) {
		cf_info(AS_INFO, "invalid or missing content-len");
		cf_dyn_buf_append_string(out, "error=invalid_content_len");
		return 0;
	}

	if ( as_info_parameter_get(params, "udf-type", type, &type_len) ) {
		// Replace with DEFAULT IS LUA
		strcpy(type, as_udf_type_name[0]);
	}

	// check type field
	if (-1 == udf_type_getid(type)) {
		cf_info(AS_INFO, "invalid or missing udf-type : %s not valid", type);
		cf_dyn_buf_append_string(out, "error=invalid_udf_type");
		return 0;
	}

	// get b64 encoded script
	udf_content_len = atoi(content_len) + 1;
	udf_content = (char *) cf_malloc(udf_content_len);

	// cf_info(AS_UDF, "content_len = %s", content_len);
	// cf_info(AS_UDF, "udf_content_len = %d", udf_content_len);


	// get (required) script content - base64 encoded here.
	if ( as_info_parameter_get(params, "content", udf_content, &(udf_content_len)) ) {
		cf_info(AS_UDF, "invalid content");
		cf_dyn_buf_append_string(out, "error=invalid_content");
		cf_free(udf_content);
		return 0;
	}

	// base 64 decode it
	uint32_t encoded_len = strlen(udf_content);
	uint32_t decoded_len = cf_b64_decoded_buf_size(encoded_len) + 1;
	
	// Don't allow UDF file size > 1MB 
	if ( decoded_len > MAX_UDF_CONTENT_LENGTH) {
		cf_info(AS_INFO, "lua file size:%d > 1MB", decoded_len);
		cf_dyn_buf_append_string(out, "error=invalid_udf_content_len, lua file size > 1MB");
		cf_free(udf_content);
		return 0;
	}

	char * decoded_str = cf_malloc(decoded_len);

	if ( ! cf_b64_validate_and_decode(udf_content, encoded_len, (uint8_t*)decoded_str, &decoded_len) ) {
		cf_info(AS_UDF, "invalid base64 content %s", filename);
		cf_dyn_buf_append_string(out, "error=invalid_base64_content");
		cf_free(decoded_str);
		cf_free(udf_content);
		return 0;
	}

	decoded_str[decoded_len] = '\0';

	as_module_error err;
	rc = as_module_validate(&mod_lua, NULL, filename, decoded_str, decoded_len, &err);

	cf_free(decoded_str);
	decoded_str = NULL;
	decoded_len = 0;

	if ( rc ) {
		cf_warning(AS_UDF, "udf-put: compile error: [%s:%d] %s", err.file, err.line, err.message);
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
		cf_dyn_buf_append_buf(out, (uint8_t *)enc_message, enc_message_len);

		cf_free(udf_content);
		return 0;
	}

	// Create an empty JSON object
	json_t *udf_obj = 0;
	if (!(udf_obj = json_object())) {
		cf_warning(AS_UDF, "failed to create JSON array for receiving UDF");
		cf_free(udf_content);
		return -1;
	}
	int e = 0;
	e += json_object_set_new(udf_obj, "content64", json_string(udf_content));
	e += json_object_set_new(udf_obj, "type", json_string(type));
	e += json_object_set_new(udf_obj, "name", json_string(filename));

	cf_free(udf_content);

	if (e) {
		cf_warning(AS_UDF, "could not encode UDF object, error %d", e);
		json_decref(udf_obj);
		return(-1);
	}
	// make it into a string, yet another buffer copy
	char *udf_obj_str = json_dumps(udf_obj, 0/*flags*/);
	json_decref(udf_obj);
	udf_obj = 0;

	cf_debug(AS_UDF, "created json object %s", udf_obj_str);

	// how do I know whether to call create or add?
	e = as_smd_set_metadata(udf_smd_module_name, filename, udf_obj_str);
	if (e) {
		cf_warning(AS_UDF, "could not add UDF metadata, error %d", e);
		cf_free(udf_obj_str);
		return(-1);
	}

	cf_info(AS_UDF, "UDF module '%s' (%s/%s) registered", filename, g_config.mod_lua.user_path, filename);

	// free the metadata
	cf_free(udf_obj_str);
	udf_obj_str = 0;

	return 0;
}

int udf_cask_info_remove(char *name, char * params, cf_dyn_buf * out) {

	char    filename[128]   = {0};
	int     filename_len    = sizeof(filename);
	char file_path[1024]	= {0};
	struct stat buf;

	cf_debug(AS_INFO, "UDF CASK INFO REMOVE");

	// get (required) script filename
	if ( as_info_parameter_get(params, "filename", filename, &filename_len) ) {
		cf_info(AS_UDF, "invalid or missing filename");
		cf_dyn_buf_append_string(out, "error=invalid_filename");
	}

	// now check if such a file-name exists :
	if (!g_config.mod_lua.user_path)
	{
		return -1;
	}

	snprintf(file_path, 1024, "%s/%s", g_config.mod_lua.user_path, filename);

	cf_debug(AS_INFO, " Lua file removal full-path is : %s \n", file_path);

	if (stat(file_path, &buf) != 0) {
		cf_info(AS_UDF, "failed to read file from : %s, error : %s", file_path, cf_strerror(errno));
		cf_dyn_buf_append_string(out, "error=file_not_found");
		return -1;
	}

	as_smd_delete_metadata(udf_smd_module_name, filename);

	// this is what an error would look like
	//    cf_dyn_buf_append_string(out, "error=");
	//    cf_dyn_buf_append_int(out, resp);

	cf_dyn_buf_append_string(out, "ok");

	cf_info(AS_UDF, "UDF module '%s' (%s) removed", filename, file_path);

	return 0;
}

/*
 *  Clear out the Lua cache.
 */
int udf_cask_info_clear_cache(char *name, char * params, cf_dyn_buf * out)
{
	cf_debug(AS_INFO, "UDF CASK INFO CLEAR CACHE");

	mod_lua_wrlock(&mod_lua);

	as_module_event e = {
		.type = AS_MODULE_EVENT_CLEAR_CACHE
	};
	as_module_update(&mod_lua, &e);

	mod_lua_unlock(&mod_lua);

	cf_dyn_buf_append_string(out, "ok");

	return 0;
}

/**
 * (Re-)Configure UDF modules
 */
int udf_cask_info_configure(char *name, char * params, cf_dyn_buf * buf) {
	as_module_configure(&mod_lua, &g_config.mod_lua);
	return 0;
}

//
// take a current list and return the new list
// Validates that items are correct? or is that done with the add?
// How do you signal that there are no changes between the current list and the new list?

int
udf_cask_smd_merge_fn (char *module, as_smd_item_list_t **item_list_out, as_smd_item_list_t **item_lists_in, size_t num_lists, void *udata)
{
	cf_debug(AS_UDF, "UDF CASK merge function");

	// (For now, just send back an empty metadata item list.)
	as_smd_item_list_t *item_list = as_smd_item_list_create(0);
	*item_list_out = item_list;

	return(0);
}

// This function must take the current "view of the world" and
// make the local store the same as that.

int
udf_cask_smd_accept_fn(char *module, as_smd_item_list_t *items, void *udata, uint32_t accept_opt)
{
	if (accept_opt & AS_SMD_ACCEPT_OPT_CREATE) {
		cf_debug(AS_UDF, "(doing nothing in UDF accept cb for module creation)");
		g_udf_smd_loaded = true;
		return 0;
	}

	cf_debug(AS_UDF, "UDF CASK accept fn : n items %zu", items->num_items);

	// For each item in the list, see if the current version
	// is different from the curretly stored version
	// and if the new item is new, write to the storage directory
	for (int i = 0; i < items->num_items ; i++) {

		as_smd_item_t *item = items->item[i];

		if (item->action == AS_SMD_ACTION_SET) {

			json_error_t json_error;
			json_t *item_obj = json_loads(item->value, 0 /*flags*/, &json_error);
			if (!item_obj) {
				cf_warning(AS_UDF, "failed to parse UDF \"%s\" with JSON error: %s ; source: %s ; line: %d ; column: %d ; position: %d",
						   item->key, json_error.text, json_error.source, json_error.line, json_error.column, json_error.position);
				continue;
			}

			/*item->key is name */
			json_t *content64_obj = json_object_get(item_obj, "content64");
			const char *content64_str = json_string_value(content64_obj);

			// base 64 decode it
			uint32_t encoded_len = strlen(content64_str);
			uint32_t decoded_len = cf_b64_decoded_buf_size(encoded_len) + 1;
			char *content_str = cf_malloc(decoded_len);

			if (! cf_b64_validate_and_decode(content64_str, encoded_len, (uint8_t*)content_str, &decoded_len)) {
				cf_info(AS_UDF, "invalid script on accept, will not register %s", item->key);
				cf_free(content_str);
				json_decref(item_obj);
				continue;
			}

			content_str[decoded_len] = 0;

			cf_debug(AS_UDF, "pushing to %s, %d bytes [%s]", item->key, decoded_len, content_str);
			mod_lua_wrlock(&mod_lua);

			// content_gen is actually a hash. Not sure if it's filled out or what.
			unsigned char       content_gen[256]    = {0};
			int e = file_write(item->key, (uint8_t *) content_str, decoded_len, content_gen);
			cf_free(content_str);
			json_decref(item_obj);
			if ( e ) {
				mod_lua_unlock(&mod_lua);
				cf_info(AS_UDF, "invalid script on accept, will not register %s", item->key);
				continue;
			}
			// Update the cache
			as_module_event ame = {
				.type           = AS_MODULE_EVENT_FILE_ADD,
				.data.filename  = item->key
			};
			as_module_update(&mod_lua, &ame);
			mod_lua_unlock(&mod_lua);
		}
		else if (item->action == AS_SMD_ACTION_DELETE) {
			cf_debug(AS_UDF, "received DELETE SMD action %d key %s", item->action, item->key);

			mod_lua_wrlock(&mod_lua);
			file_remove(item->key);

			// fixes potential cache issues
			as_module_event e = {
				.type           = AS_MODULE_EVENT_FILE_REMOVE,
				.data.filename  = item->key
			};
			as_module_update(&mod_lua, &e);

			mod_lua_unlock(&mod_lua);

		}
		else {
			cf_info(AS_UDF, "received unknown SMD action %d", item->action);
		}
	}

	return(0);
}


void
udf_cask_init()
{
	// Have to delete the existing files in the user path on startup
	struct dirent   * entry         = NULL;
	// opendir(NULL) seg-faults
	if (!g_config.mod_lua.user_path)
	{
		cf_crash(AS_UDF, "cask init: null mod-lua user-path");
	}

	DIR *dir = opendir(g_config.mod_lua.user_path);
	if ( dir == 0 ) {
		cf_crash(AS_UDF, "cask init: could not open udf directory %s: %s", g_config.mod_lua.user_path, cf_strerror(errno));
	}
	while ( (entry = readdir(dir))) {
		// readdir also reads "." and ".." entries.
		if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, ".."))
		{
			char fn[1024];
			snprintf(fn, sizeof(fn), "%s/%s", g_config.mod_lua.user_path, entry->d_name);
			int rem_rv = remove(fn);
			if (rem_rv != 0) {
				cf_warning(AS_UDF, "Failed to remove the file %s. Error %d", fn, errno);
			}
		}
	}
	closedir(dir);

	// as_smd_create_module(udf_smd_module_name, udf_cask_smd_merge_fn, 0, udf_cask_smd_accept_fn, 0);
	// take the default merge function
	if (as_smd_create_module(udf_smd_module_name, 0, 0, 0, 0, udf_cask_smd_accept_fn, 0, 0, 0)) {
		cf_crash(AS_UDF, "failed to create SMD module \"%s\"", udf_smd_module_name);
	}

	while (! g_udf_smd_loaded) {
		usleep(1000);
	}

	// there may be existing data. Read it and populate the local file system.
}
