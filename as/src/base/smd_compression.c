/*
 * smd_compression.c
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

#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "log.h"

#include "base/proto.h"
#include "base/smd.h"
#include "base/zstd_wire.h"

#define SMD_FULL_ZSTD_THRESHOLD 1024
// SMD payloads (UDFs, sindex definitions, roster lists, etc.) can grow well
// past the 8 MiB record-size cap that zstd_wire_decompress_buffer enforces.
// Use the server message-size bound - PROTO_SIZE_MAX (128 MiB) - so compressed
// full-sync receive rejects payloads that could not have been sent
// uncompressed over the normal wire. This deliberately supersedes the design
// note's 64 MiB starting point: parity with the uncompressed bound means
// enabling compression can never make a previously-deliverable full sync
// undeliverable. The declared content size does not drive the allocation -
// the decoder grows its output from small as real content materializes - so a
// tiny frame declaring 128 MiB cannot force a full-cap allocation up front;
// actual decompressed bytes (and the item-count bounds check below) bound the
// transient cost on the SMD thread.
#define SMD_FULL_ZSTD_MAX_DECOMPRESSED_SIZE PROTO_SIZE_MAX

// Sentinel value_len indicating a NULL value (delete tombstone).
#define SMD_NULL_VALUE_SENTINEL UINT32_MAX

// Per-item fixed overhead: key_len + value_len + generation + timestamp.
#define SMD_ITEM_FIXED_OVERHEAD (4 + 4 + 4 + 8)

static bool smd_item_list_pack_bin(const cf_vector* items, uint8_t** buf_r,
		size_t* sz_r);
static bool smd_item_list_parse_bin(const uint8_t* buf, size_t sz,
		cf_vector* items_r);
static void smd_item_list_parse_reset(cf_vector* items_r);
static void smd_item_destroy(as_smd_item* item);

bool
as_smd_compress_items(const cf_vector* items, uint8_t** buf_r, size_t* sz_r,
		size_t* orig_sz_r, int32_t level)
{
	uint8_t* packed = NULL;
	size_t packed_sz = 0;

	if (orig_sz_r != NULL) {
		*orig_sz_r = 0;
	}

	bool packed_ok = smd_item_list_pack_bin(items, &packed, &packed_sz);

	if (orig_sz_r != NULL) {
		*orig_sz_r = packed_sz;
	}

	if (! packed_ok) {
		*buf_r = NULL;
		*sz_r = 0;
		return false;
	}

	void* compressed = NULL;
	size_t compressed_sz =
			zstd_wire_compress_buffer(&compressed, packed, packed_sz, level);
	cf_free(packed);

	if (compressed_sz == 0 || compressed_sz >= packed_sz) {
		cf_free(compressed);
		*buf_r = NULL;
		*sz_r = 0;
		return false;
	}

	*buf_r = (uint8_t*)compressed;
	*sz_r = compressed_sz;
	return true;
}

bool
as_smd_decompress_items(const uint8_t* buf, size_t sz, cf_vector* items_r)
{
	if (items_r == NULL) {
		return false;
	}

	void* uncompressed = NULL;
	size_t uncompressed_sz = zstd_wire_decompress_buffer_capped(&uncompressed,
			buf, sz, SMD_FULL_ZSTD_MAX_DECOMPRESSED_SIZE);

	if (uncompressed_sz == 0) {
		return false;
	}

	bool rv = smd_item_list_parse_bin((const uint8_t*)uncompressed,
			uncompressed_sz, items_r);
	cf_free(uncompressed);
	return rv;
}

void
as_smd_items_destroy(cf_vector* items)
{
	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		smd_item_destroy((as_smd_item*)cf_vector_get_ptr(items, i));
	}

	cf_vector_destroy(items);
}

static bool
smd_item_list_pack_bin(const cf_vector* items, uint8_t** buf_r, size_t* sz_r)
{
	*buf_r = NULL;
	*sz_r = 0;

	uint32_t count = cf_vector_size(items);

	if (count == 0) {
		return false;
	}

	// Compute total packed size: 4-byte header + per-item fixed overhead +
	// variable key/value bytes.
	size_t packed_sz = sizeof(uint32_t);

	for (uint32_t i = 0; i < count; i++) {
		const as_smd_item* item = (const as_smd_item*)cf_vector_get_ptr(items, i);

		packed_sz += SMD_ITEM_FIXED_OVERHEAD;
		packed_sz += strlen(item->key);

		if (item->value != NULL) {
			packed_sz += strlen(item->value);
		}
	}

	*sz_r = packed_sz;

	if (packed_sz < SMD_FULL_ZSTD_THRESHOLD) {
		return false;
	}

	uint8_t* packed = (uint8_t*)cf_malloc(packed_sz);
	uint8_t* at = packed;

	uint32_t count_be = cf_swap_to_be32(count);
	memcpy(at, &count_be, sizeof(count_be));
	at += sizeof(count_be);

	for (uint32_t i = 0; i < count; i++) {
		const as_smd_item* item = (const as_smd_item*)cf_vector_get_ptr(items, i);

		uint32_t key_len = (uint32_t)strlen(item->key);
		uint32_t key_len_be = cf_swap_to_be32(key_len);
		memcpy(at, &key_len_be, sizeof(key_len_be));
		at += sizeof(key_len_be);
		memcpy(at, item->key, key_len);
		at += key_len;

		uint32_t value_len = item->value == NULL ? SMD_NULL_VALUE_SENTINEL
												 : (uint32_t)strlen(item->value);
		uint32_t value_len_be = cf_swap_to_be32(value_len);
		memcpy(at, &value_len_be, sizeof(value_len_be));
		at += sizeof(value_len_be);

		if (item->value != NULL) {
			memcpy(at, item->value, value_len);
			at += value_len;
		}

		uint32_t gen_be = cf_swap_to_be32(item->generation);
		memcpy(at, &gen_be, sizeof(gen_be));
		at += sizeof(gen_be);

		uint64_t ts_be = cf_swap_to_be64(item->timestamp);
		memcpy(at, &ts_be, sizeof(ts_be));
		at += sizeof(ts_be);
	}

	cf_assert((size_t)(at - packed) == packed_sz, AS_SMD,
			"smd pack size mismatch: %zu vs %zu", (size_t)(at - packed),
			packed_sz);

	*buf_r = packed;
	return true;
}

static bool
smd_item_list_parse_bin(const uint8_t* buf, size_t sz, cf_vector* items_r)
{
	if (sz < sizeof(uint32_t)) {
		cf_warning(AS_SMD, "compressed smd payload too small (%zu)", sz);
		return false;
	}

	const uint8_t* at = buf;
	const uint8_t* end = buf + sz;

	uint32_t count_be;
	memcpy(&count_be, at, sizeof(count_be));
	at += sizeof(count_be);
	uint32_t count = cf_swap_from_be32(count_be);

	// Reject an implausible item count before allocating. Each item needs at
	// least SMD_ITEM_FIXED_OVERHEAD bytes on the wire, so a count larger than
	// the remaining buffer could possibly describe is corrupt or hostile.
	// Without this guard a bogus count is passed straight to cf_vector_init,
	// which eagerly cf_malloc(count * sizeof(ptr)) - a peer-controlled count
	// can force a multi-GB allocation (node abort) and the 32-bit count*ele_sz
	// product can wrap and under-allocate.
	size_t max_count = (size_t)(end - at) / SMD_ITEM_FIXED_OVERHEAD;

	if (count > max_count) {
		cf_warning(AS_SMD,
				"implausible smd item count %u (max %zu for %zu payload bytes)",
				count, max_count, sz);
		return false;
	}

	cf_vector_init(items_r, sizeof(as_smd_item*), count, 0);

	for (uint32_t i = 0; i < count; i++) {
		if ((size_t)(end - at) < sizeof(uint32_t)) {
			cf_warning(AS_SMD, "truncated key length");
			smd_item_list_parse_reset(items_r);
			return false;
		}

		uint32_t key_len_be;
		memcpy(&key_len_be, at, sizeof(key_len_be));
		at += sizeof(key_len_be);
		uint32_t key_len = cf_swap_from_be32(key_len_be);

		if (key_len == SMD_NULL_VALUE_SENTINEL || (size_t)(end - at) < key_len) {
			cf_warning(AS_SMD, "invalid key length %u", key_len);
			smd_item_list_parse_reset(items_r);
			return false;
		}

		const char* key_ptr = (const char*)at;
		at += key_len;

		if ((size_t)(end - at) < sizeof(uint32_t)) {
			cf_warning(AS_SMD, "truncated value length");
			smd_item_list_parse_reset(items_r);
			return false;
		}

		uint32_t value_len_be;
		memcpy(&value_len_be, at, sizeof(value_len_be));
		at += sizeof(value_len_be);
		uint32_t value_len = cf_swap_from_be32(value_len_be);

		const char* value_ptr = NULL;
		uint32_t value_bytes = 0;

		if (value_len != SMD_NULL_VALUE_SENTINEL) {
			if ((size_t)(end - at) < value_len) {
				cf_warning(AS_SMD, "truncated value");
				smd_item_list_parse_reset(items_r);
				return false;
			}

			value_ptr = (const char*)at;
			value_bytes = value_len;
			at += value_len;
		}

		if ((size_t)(end - at) < sizeof(uint32_t) + sizeof(uint64_t)) {
			cf_warning(AS_SMD, "truncated item trailer");
			smd_item_list_parse_reset(items_r);
			return false;
		}

		uint32_t gen_be;
		memcpy(&gen_be, at, sizeof(gen_be));
		at += sizeof(gen_be);

		uint64_t ts_be;
		memcpy(&ts_be, at, sizeof(ts_be));
		at += sizeof(ts_be);

		as_smd_item* item = (as_smd_item*)cf_malloc(sizeof(as_smd_item));

		item->key = cf_strndup(key_ptr, key_len);
		item->value = value_ptr == NULL ? NULL
										: cf_strndup(value_ptr, value_bytes);
		item->generation = cf_swap_from_be32(gen_be);
		item->timestamp = cf_swap_from_be64(ts_be);

		cf_vector_append_ptr(items_r, item);
	}

	if (at != end) {
		cf_warning(AS_SMD, "trailing bytes in compressed smd payload (%zu)",
				(size_t)(end - at));
		smd_item_list_parse_reset(items_r);
		return false;
	}

	return true;
}

// On parse failure, free any items already pushed and zero the vector struct.
// cf_vector_destroy() frees v->eles but does NOT clear the pointer or flags,
// so a second destroy (from the caller's smd_op_destroy() -> item_vec_destroy())
// would double-free. Zeroing leaves the second destroy a safe no-op.
static void
smd_item_list_parse_reset(cf_vector* items_r)
{
	as_smd_items_destroy(items_r);
	memset(items_r, 0, sizeof(*items_r));
}

static void
smd_item_destroy(as_smd_item* item)
{
	if (item != NULL) {
		cf_free(item->key);
		cf_free(item->value);
		cf_free(item);
	}
}
