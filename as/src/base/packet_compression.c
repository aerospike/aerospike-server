/*
 * packet_compression.c
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

#include <stdint.h>
#include <stdlib.h>
#include <zlib.h>

#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/packet_compression.h"
#include "base/proto.h"

#define STACK_BUF_SZ (1024 * 16)

/**
 * Function to decompress the given data
 * Expected arguments
 * @param type			Type of compression
 * @param length		Length of buffer to be decompressed
 * @param buf			Pointer to buffer to be decompressed
 * @param out_buf_len	Length of buffer to hold decompressed data
 * @param out_buf		Pointer to buffer to hold decompressed data
 * @return 0 if successful
 */
int
as_decompress(compression_type type, size_t buf_len, const uint8_t *buf, size_t *out_buf_len, uint8_t *out_buf)
{
	int ret_value = -1;
	cf_debug(AS_COMPRESSION, "In as_decompress");
	switch (type) {
		case COMPRESSION_ZLIB: {
			// manual convert to match types just in case
			uLongf converted_out_buf_len = *out_buf_len;
			// zlib api to decompress the data
			ret_value = uncompress(out_buf, &converted_out_buf_len, buf, (uLongf) buf_len);
			*out_buf_len = converted_out_buf_len;
			break;
		}
		default:
			cf_warning(AS_COMPRESSION, "Unknown as_proto compression type: %d", type);
			break;
	}
	cf_debug(AS_COMPRESSION, "Returned as_decompress : %d", ret_value);
	return ret_value;
}

/**
 * Function to get back decompressed packet from PROTO_TYPE_AS_MSG_COMPRESSED packet
 * Packet :  Header - Original size of message - Compressed message
 * @param buf			Pointer to PROTO_TYPE_AS_MSG_COMPRESSED packet. - Input
 * @param output_packet	Pointer holding address of decompressed packet. - Output
 */
int
as_packet_decompression(uint8_t *buf, uint8_t **output_packet, size_t *output_packet_size)
{
	int ret_value = -1;
	as_comp_proto *as_comp_protop = (as_comp_proto *) buf;

	cf_debug(AS_COMPRESSION, "In as_packet_decompression");

	if (as_comp_protop->proto.type != PROTO_TYPE_AS_MSG_COMPRESSED)	{
		cf_warning(AS_COMPRESSION, "as_packet_decompression : Invalid input data : type received %d != PROTO_TYPE_AS_MSG_COMPRESSED (%d)",
				   as_comp_protop->proto.type, PROTO_TYPE_AS_MSG_COMPRESSED);
		cf_warning(AS_COMPRESSION, "Returned as_packet_decompression : %d", ret_value);
		return ret_value;
	}

#if 0 // enable this when byte swap also fixed on client side
	as_comp_protop->org_sz = cf_swap_from_be64(as_comp_protop->org_sz);
#endif
	size_t decompressed_as_packet_sz = as_comp_protop->org_sz;
	// sanity check for client supplied size
	if (decompressed_as_packet_sz > PROTO_SIZE_MAX) {
		// the closest error for this case is "input data was corrupted or incomplete"
		return Z_DATA_ERROR;
	}

	size_t buf_sz = as_comp_protop->proto.sz - 8;
	buf += sizeof(as_comp_proto);
	uint8_t *decompressed_packet = cf_malloc(decompressed_as_packet_sz);
	ret_value = as_decompress(COMPRESSION_ZLIB, buf_sz, buf, &decompressed_as_packet_sz, decompressed_packet);
	if (ret_value) {
		cf_free(decompressed_packet);
	} else {
		*output_packet = decompressed_packet;
		if (output_packet_size) {
			*output_packet_size = decompressed_as_packet_sz;
		}
	}
	cf_debug(AS_COMPRESSION, "Returned as_packet_decompression : %d", ret_value);
	return (ret_value);
}

/*
 * Function to compress the given data
 * Expected arguments
 * 1. Type of compression
 *  1 for zlib
 * 2. Length of buffer to be compressed - mandatory
 * 3. Pointer to buffer to be compressed - mandatory
 * 4. Length of buffer to hold compressed data - mandatory
 * 5. Pointer to buffer to hold compressed data - mandatory
 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
 */
int
as_compress(int argc, uint8_t *argv[])
{
#define MANDATORY_NO_ARGUMENTS 5
	int compression_type;
	uint8_t *buf;
	size_t *buf_len;
	uint8_t *out_buf;
	size_t *out_buf_len;
	int compression_level;
	int ret_value = 0;

	cf_debug(AS_COMPRESSION, "In as_compress");

	if (argc < MANDATORY_NO_ARGUMENTS)
	{
		// Insufficient arguments
		cf_debug(AS_COMPRESSION, "as_compress : In sufficient arguments\n");
		cf_debug(AS_COMPRESSION, "Returned as_compress : -1");
		return -1;
	}

	compression_type = *argv[0];
	buf_len = (size_t *) argv[1];
	buf = argv[2];
	out_buf_len = (size_t *) argv[3];
	out_buf = argv[4];

	compression_level = (argc > MANDATORY_NO_ARGUMENTS) ? (*argv[MANDATORY_NO_ARGUMENTS + 1]) : Z_DEFAULT_COMPRESSION;

	switch (compression_type)
	{
		case COMPRESSION_ZLIB:
			// zlib api to compress the data
			ret_value = compress2(out_buf, out_buf_len, buf, *buf_len, compression_level);
			break;
	}
	cf_debug(AS_COMPRESSION, "Returned as_compress : %d", ret_value);
	return ret_value;
}

/*
 * Function to create packet to send compressed data.
 * Packet :  Header - Original size of message - Compressed message.
 * Input : buf - Pointer to data to be compressed. - Input
 *     buf_sz - Size of the data to be compressed. - Input
 *     compressed_packet : Pointer holding address of compressed packet. - Output
 *     compressed_as_packet_sz : Size of the compressed packet. - Output
 */
int
as_packet_compression(uint8_t *buf, size_t buf_sz, uint8_t **compressed_packet, size_t *compressed_as_packet_sz)
{
	uint8_t *tmp_buf;
	uint8_t wr_stack_buf[STACK_BUF_SZ];
	uint8_t *wr_buf = wr_stack_buf;
	size_t  wr_buf_sz = sizeof(wr_stack_buf);
	cf_debug(AS_COMPRESSION, "In as_packet_compression");

	/* Compress the data using client API for compression.
	 * Expected arguments
	 * 1. Type of compression
	 *  1 for zlib
	 * 2. Length of buffer to be compressed - mandatory
	 * 3. Pointer to buffer to be compressed - mandatory
	 * 4. Length of buffer to hold compressed data - mandatory
	 * 5. Pointer to buffer to hold compressed data - mandatory
	 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
	 */
	uint8_t *argv[5];
	int argc = 5;
	int compression_type = COMPRESSION_ZLIB;
	argv[0] = (uint8_t *)&compression_type;
	argv[1] = (uint8_t *)&buf_sz;
	argv[2] = buf;
	argv[3] = (uint8_t *)&wr_buf_sz;
	argv[4] = wr_buf;

	if (as_compress(argc, argv))
	{
		compressed_packet = NULL;
		compressed_as_packet_sz = 0;
		cf_debug(AS_COMPRESSION, "Returned as_packet_compression : -1");
		return -1;
	}

	// Allocate buffer to hold new packet
	*compressed_as_packet_sz = sizeof(as_comp_proto) + wr_buf_sz;
	*compressed_packet = (uint8_t *) cf_calloc(*compressed_as_packet_sz, 1);
	if(!*compressed_packet)
	{
		cf_debug(AS_COMPRESSION, "as_packet_compression : failed to allocte memory");
		cf_debug(AS_COMPRESSION, "Returned as_packet_compression : -1");
		return -1;
	}
	// Construct the packet for compressed data.
	as_comp_proto *as_comp_protop = (as_comp_proto *) *compressed_packet;
	as_comp_protop->proto.version = PROTO_VERSION;
	as_comp_protop->proto.type = PROTO_TYPE_AS_MSG_COMPRESSED;
	as_comp_protop->proto.sz = *compressed_as_packet_sz - 8;
	as_proto *proto = (as_proto *) *compressed_packet;
	as_proto_swap(proto);
	as_comp_protop->org_sz = buf_sz;

	tmp_buf = *compressed_packet +  sizeof(as_comp_proto);
	memcpy(tmp_buf, wr_buf, wr_buf_sz);

	cf_debug(AS_COMPRESSION, "Returned as_packet_compression : 0");
	return 0;
}
