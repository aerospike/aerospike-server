/*
 * hll.c
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

//#include "base/hll.h"

#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <endian.h> // cf_byte_order.h
#include <sys/param.h> // for MIN()

//#include "citrusleaf/alloc.h" // for calloc
//#include "citrusleaf/cf_byte_order.h"

//#include <bits.h>

//#include "warnings.h"

#include <stdio.h> // FIXME remove me
#include <stdlib.h> // FIXME remove me (for calloc)

//==========================================================
// FIXME - remove
//


#define cf_swap_to_be16(_n) htobe16(_n)
#define cf_swap_from_be16(_n) be16toh(_n)

#define cf_swap_to_be64(_n) htobe64(_n)
#define cf_swap_from_be64(_n) be64toh(_n)


// Returns number of trailing zeros in a uint64_t, 64 for x == 0.
static inline uint32_t
cf_lsb64(uint64_t x)
{
	if (x == 0) {
		return 64;
	}

	return (uint32_t)__builtin_ctzll(x);
}


//==========================================================
// Typedefs & constants.
//

typedef struct hll_s {
	uint8_t n_index_bits; // n_registers = 1 << index_bits (range 4 to 16)
	uint8_t n_minhash_bits; // r in https://arxiv.org/pdf/1710.08436.pdf
	uint8_t registers[];
} __attribute__ ((__packed__)) hll_t;

#define MIN_INDEX_BITS 4
#define MAX_INDEX_BITS 16
#define HLL_BITS 6
#define HLL_MAX_VALUE (1 << HLL_BITS)
#define MIN_MINHASH_BITS 4
#define MAX_MINHASH_BITS (64 - HLL_BITS - 7)
#define MAX_INDEX_AND_MINHASH_BITS 64


//==========================================================
// Forward declarations.
//

static size_t hmh_required_sz(uint8_t index_bits, uint8_t minhash_bits);
static size_t registers_sz(uint8_t index_bits, uint8_t minhash_bits);
static bool hmh_init(hll_t* hmh, uint8_t index_bits, uint8_t minhash_bits);
static void hmh_insert(hll_t* hmh, size_t buf_sz, const uint8_t* buf);
static void hmh_hash(const hll_t* hmh, const uint8_t* element, size_t value_sz, uint16_t* register_ix, uint64_t* value);
static uint64_t get_register(const hll_t* hmh, uint32_t r);
static void set_register(hll_t* hmh, uint32_t r, uint64_t value);
static uint64_t hmh_estimate_cardinality(const hll_t* hmh);
static uint8_t unpack_register_hll_val(const hll_t* hmh, uint64_t value);
static double hmh_tau(double val);
static double hmh_sigma(double val);
static double hmh_alpha(const hll_t* hmh);
static bool hmh_estimate_union_cardinality(uint32_t n_hmhs, const hll_t** hmhs, uint64_t* result);
static void hmh_compatible_template(uint32_t n_hmhs, const hll_t** hmhs, hll_t* template);
static void hmh_union(hll_t* hmhunion, const hll_t* hmh);
static void hll_union(hll_t* hllunion, const hll_t* hmh);
static bool hmh_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs, uint64_t* result);
static bool hll_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs, uint64_t* result);
static bool hmh_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs, double* result);
static bool hmh_has_data(const hll_t* hmh);
static void hmh_intersect_one(hll_t* intersect_hmh, const hll_t* hmh);
static uint32_t hmh_n_used_registers(const hll_t* hmh);
static double hmh_jaccard_estimate_collisions(const hll_t* template, uint64_t card0, uint64_t card1);
static bool hll_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs, double* result);

void fail(const char* msg);

///////// murmurhash
#define ROTL(x, r) ((x << r) | (x >> (64 - r)))

static inline uint64_t
fmix64(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccd;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53;
	k ^= k >> 33;

	return k;
}

void
MurmurHash3_x64_128(const uint8_t* key, const size_t len, uint8_t* out)
{
	const uint8_t* data = (const uint8_t*)key;
	const size_t nblocks = len / 16;

	uint64_t h1 = 0;
	uint64_t h2 = 0;

	const uint64_t c1 = 0x87c37b91114253d5;
	const uint64_t c2 = 0x4cf5ad432745937f;

	//----------
	// body

	const uint64_t* blocks = (const uint64_t*)(data);

	for(size_t i = 0; i < nblocks; i++) {
		uint64_t k1 = blocks[i * 2 + 0];
		uint64_t k2 = blocks[i * 2 + 1];

		k1 *= c1;
		k1 = ROTL(k1, 31);
		k1 *= c2; h1 ^= k1;

		h1 = ROTL(h1, 27);
		h1 += h2;
		h1 = h1 * 5+0x52dce729;

		k2 *= c2;
		k2 = ROTL(k2, 33);
		k2 *= c1; h2 ^= k2;

		h2 = ROTL(h2, 31);
		h2 += h1;
		h2 = h2 * 5+0x38495ab5;
	}

	//----------
	// tail

	const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

	uint64_t k1 = 0;
	uint64_t k2 = 0;

	switch(len & 15) {
	case 15:
		k2 ^= ((uint64_t)tail[14]) << 48;
		// No break.
	case 14:
		k2 ^= ((uint64_t)tail[13]) << 40;
		// No break.
	case 13:
		k2 ^= ((uint64_t)tail[12]) << 32;
		// No break.
	case 12:
		k2 ^= ((uint64_t)tail[11]) << 24;
		// No break.
	case 11:
		k2 ^= ((uint64_t)tail[10]) << 16;
		// No break.
	case 10:
		k2 ^= ((uint64_t)tail[9]) << 8;
		// No break.
	case  9:
		k2 ^= ((uint64_t)tail[8]) << 0;

		k2 *= c2;
		k2 = ROTL(k2, 33);
		k2 *= c1;
		h2 ^= k2;

		// No break.
	case  8:
		k1 ^= ((uint64_t)tail[7]) << 56;
		// No break.
	case  7:
		k1 ^= ((uint64_t)tail[6]) << 48;
		// No break.
	case  6:
		k1 ^= ((uint64_t)tail[5]) << 40;
		// No break.
	case  5:
		k1 ^= ((uint64_t)tail[4]) << 32;
		// No break.
	case  4:
		k1 ^= ((uint64_t)tail[3]) << 24;
		// No break.
	case  3:
		k1 ^= ((uint64_t)tail[2]) << 16;
		// No break.
	case  2:
		k1 ^= ((uint64_t)tail[1]) << 8;
		// No break.
	case  1:
		k1 ^= ((uint64_t)tail[0]) << 0;

		k1 *= c1;
		k1 = ROTL(k1, 31);
		k1 *= c2;
		h1 ^= k1;
	}

	//----------
	// finalization

	h1 ^= len;
	h2 ^= len;

	h1 += h2;
	h2 += h1;

	h1 = fmix64(h1);
	h2 = fmix64(h2);

	h1 += h2;
	h2 += h1;

	((uint64_t*)out)[0] = h1;
	((uint64_t*)out)[1] = h2;
}

///////// murmurhash end


//==========================================================
// Public API.
//



//==========================================================
// Local helpers.
//

static size_t
hmh_required_sz(uint8_t index_bits, uint8_t minhash_bits)
{
	size_t sz = registers_sz(index_bits, minhash_bits);

	if (sz == - 1) {
		return -1;
	}

	return sizeof(hll_t) + sz;
}

static size_t
registers_sz(uint8_t index_bits, uint8_t minhash_bits)
{
	if (index_bits < MIN_INDEX_BITS ||
			index_bits > MAX_INDEX_BITS) {
		return -1;
	}

	if (minhash_bits > 0 && (minhash_bits < MIN_MINHASH_BITS ||
			minhash_bits > MAX_MINHASH_BITS)) {
		return -1; // minhash_bits may be either 0 or 4 to 64
	}

	if (index_bits + minhash_bits > MAX_INDEX_AND_MINHASH_BITS) {
		return -1; // index_bits and minhash_bits come from one uint64_t.
	}

	return ((HLL_BITS + minhash_bits) * (1 << index_bits) + 7) / 8;
}

static bool
hmh_init(hll_t* hmh, uint8_t index_bits, uint8_t minhash_bits)
{
	hmh->n_index_bits = index_bits;
	hmh->n_minhash_bits = minhash_bits;

	size_t sz = registers_sz(index_bits, minhash_bits);

	if (sz == -1) {
		return false;
	}

	memset(hmh->registers, 0, sz);

	return true;
}

static void
hmh_insert(hll_t* hmh, size_t buf_sz, const uint8_t* buf)
{
	uint16_t register_ix;
	uint64_t new_value;

	hmh_hash(hmh, buf, buf_sz, &register_ix, &new_value);

	uint64_t cur_value = get_register(hmh, register_ix);

	if (cur_value < new_value) {
		set_register(hmh, register_ix, new_value);
	}
}

static void
hmh_hash(const hll_t* hmh, const uint8_t* element, size_t value_sz,
		uint16_t* register_ix, uint64_t* value)
{
	uint64_t minhash_mask = ((uint64_t)1 << hmh->n_minhash_bits) - 1;
	uint8_t hash[16];

	MurmurHash3_x64_128(element, value_sz, hash);

	uint64_t* hash_64 = (uint64_t*)hash;
	*register_ix = (uint16_t)(hash_64[0] >> (64 - hmh->n_index_bits));

	uint64_t minhash_val = hash_64[0] & minhash_mask;
	uint8_t hll_val = cf_lsb64(hash_64[1]) + 1;

	*value = ((uint64_t)hll_val << hmh->n_minhash_bits) | minhash_val;
}

static uint64_t
get_register(const hll_t* hmh, uint32_t r) {
	uint32_t n_bits = HLL_BITS + hmh->n_minhash_bits;
	uint32_t bit_offset = n_bits * r;
	uint32_t byte_offset = bit_offset / 8;
	uint32_t value_offset = bit_offset % 8;

	uint32_t n_registers = 1 << hmh->n_index_bits;
	uint32_t bit_end = n_bits * n_registers;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;
	uint32_t l_bit = bit_offset % 8;

	if (byte_offset > max_offset) {
		l_bit += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	uint64_t* dwords = (uint64_t*)(hmh->registers + byte_offset);
	uint64_t value = cf_swap_from_be64(dwords[0]);
	uint32_t shift_bits = 64 - (l_bit + n_bits);
	uint64_t mask = ((uint64_t)1 << n_bits) - 1;

	return (value >> shift_bits) & mask;
}

static void
set_register(hll_t* hmh, uint32_t r, uint64_t value) {
	uint32_t n_bits = HLL_BITS + hmh->n_minhash_bits;
	uint32_t bit_offset = n_bits * r;
	uint32_t byte_offset = bit_offset / 8;
	uint64_t mask = (((uint64_t)1 << n_bits) - 1) << (64 - n_bits);
	uint32_t n_registers = 1 << hmh->n_index_bits;
	uint32_t bit_end = n_bits * n_registers;
	uint32_t byte_end = (bit_end + 7) / 8;
	uint32_t max_offset = byte_end - 8;
	uint32_t shift_bits = bit_offset % 8;

	if (byte_offset > max_offset) {
		shift_bits += (byte_offset - max_offset) * 8;
		byte_offset = max_offset;
	}

	mask >>= shift_bits;
	value <<= (64 - n_bits);
	value >>= shift_bits;

	uint64_t* dwords = (uint64_t*)(hmh->registers + byte_offset);

	dwords[0] = cf_swap_to_be64((cf_swap_from_be64(dwords[0]) & ~mask) | value);
}

// @misc{ertl2017new,
//     title={New cardinality estimation algorithms for HyperLogLog sketches},
//     author={Otmar Ertl},
//     year={2017},
//     eprint={1702.01284},
//     archivePrefix={arXiv},
//     primaryClass={cs.DS}
// }
// Algorithm 6
static uint64_t
hmh_estimate_cardinality(const hll_t* hmh)
{
	uint32_t n_registers = 1 << hmh->n_index_bits;
	uint32_t c[HLL_MAX_VALUE + 1] = {0}; // q_bits + 1

	for (uint32_t r = 0; r < n_registers; r++) {
		c[unpack_register_hll_val(hmh, get_register(hmh, r))]++;
	}

	// TODO - evaluate if allowing q_bits to be 64 was a good choice.
	double z = n_registers * hmh_tau(c[HLL_MAX_VALUE] / (double)n_registers);

	for (uint32_t k = HLL_MAX_VALUE; k > 0; k--) {
		z = 0.5 * (z + c[k]);
	}

	z += n_registers * hmh_sigma(c[0] / (double)n_registers);

	return (uint64_t)(llroundl(hmh_alpha(hmh) * n_registers * n_registers / z));
}

static uint8_t
unpack_register_hll_val(const hll_t* hmh, uint64_t value)
{
	uint64_t mask = ((uint64_t)1 << hmh->n_minhash_bits) - 1;
	return (uint8_t)(value >> hmh->n_minhash_bits);
}

static double
hmh_tau(double val)
{
	if (val == 0.0 || val == 1.0) {
		return 0.0;
	}

	double z_prime;
	double y = 1.0;
	double z = 1 - val;

	do {
		val = sqrt(val);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - val, 2) * y;
	} while(z_prime != z);

	return z / 3;
}

static double
hmh_sigma(double val)
{
	if (val == 1.0) {
		return INFINITY;
	}

	double z_prime;
	double y = 1;
	double z = val;

	do {
		val *= val;
		z_prime = z;
		z += val * y;
		y += y;
	} while(z_prime != z);

	return z;
}

static double
hmh_alpha(const hll_t* hmh)
{
	switch (hmh->n_index_bits) {
	case 4:
		return 0.673;
	case 5:
		return 0.697;
	case 6:
		return 0.709;
	}

	return 0.7213 / (1.0 + 1.079 / (1 << hmh->n_index_bits));
}

static bool
hmh_estimate_union_cardinality(uint32_t n_hmhs, const hll_t** hmhs,
		uint64_t* result)
{
	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);
	size_t sz = hmh_required_sz(template.n_index_bits, template.n_minhash_bits);

	if (sz == -1) {
		return false;
	}

	uint8_t buf[sz];
	hll_t* hmhunion = (hll_t*)buf;

	hmh_init(hmhunion, template.n_index_bits, template.n_minhash_bits);

	for (uint32_t i = 0; i < n_hmhs; i++) {
		hmh_union(hmhunion, hmhs[i]);
	}

	*result = hmh_estimate_cardinality(hmhunion);

	return true;
}

static void
hmh_compatible_template(uint32_t n_hmhs, const hll_t** hmhs, hll_t* template)
{
	uint8_t index_bits = hmhs[0]->n_index_bits;
	uint64_t minhash_bits = hmhs[0]->n_minhash_bits;

	for (uint32_t i = 1; i < n_hmhs; i++) {
		const hll_t* hmh = hmhs[i];

		if (index_bits > hmh->n_index_bits) {
			index_bits = hmh->n_index_bits;
		}

		if (minhash_bits != 0 && minhash_bits != hmh->n_minhash_bits) {
			minhash_bits = 0;
		}
	}

	*template = (hll_t){
		.n_index_bits = index_bits,
		.n_minhash_bits = minhash_bits
	};
}

static void
hmh_union(hll_t* hmhunion, const hll_t* hmh)
{
	if (hmhunion->n_minhash_bits == 0) {
		return hll_union(hmhunion, hmh);
	}

	// XXX - INV: hmhunion.index_bits <= hmh.index_bits

	uint32_t n_registers = 1 << hmhunion->n_index_bits;
	uint32_t max_registers = 1 << hmh->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t small_r = r & register_mask;
		uint64_t v0 = get_register(hmhunion, small_r);
		uint64_t v1 = get_register(hmh, r);

		set_register(hmhunion, small_r, v0 > v1 ? v0 : v1);
	}
}

static void
hll_union(hll_t* hllunion, const hll_t* hmh)
{
	// XXX - INV: hmhunion.index_bits <= hmh.index_bits

	uint32_t n_registers = 1 << hllunion->n_index_bits;
	uint32_t max_registers = 1 << hmh->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t small_r = r & register_mask;
		uint8_t h0 = unpack_register_hll_val(hllunion, get_register(hllunion,
				small_r));
		uint8_t h1 = unpack_register_hll_val(hmh, get_register(hmh, r));

		set_register(hllunion, small_r, h0 > h1 ? h0 : h1);
	}
}

static bool
hmh_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs,
		uint64_t* result)
{
	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);

	if (template.n_minhash_bits == 0) {
		return hll_estimate_intersect_cardinality(n_hmhs, hmhs, result);
	}

	uint64_t cu;

	if (! hmh_estimate_union_cardinality(n_hmhs, hmhs, &cu)) {
		return false; // incompatible
	}

	double j;

	if (! hmh_estimate_similarity(n_hmhs, hmhs, &j)) {
		return false; // incompatible
	}

	*result = llround(cu * j);

	return true;
}

static bool
hll_estimate_intersect_cardinality(uint32_t n_hmhs, const hll_t** hmhs,
		uint64_t* result)
{
	uint64_t cu;

	if (! hmh_estimate_union_cardinality(n_hmhs, hmhs, &cu)) {
		return false; // incompatible
	}

	uint64_t sum_hmhs = 0;

	for (uint32_t i = 0; i < n_hmhs; i++) {
		uint64_t c = hmh_estimate_cardinality(hmhs[i]);

		if (sum_hmhs + c < sum_hmhs) {
			sum_hmhs = UINT64_MAX;
			// TODO - warn in server.
			break;
		}

		sum_hmhs += c;
	}

	*result = sum_hmhs > cu ? sum_hmhs - cu : 0;

	return true;
}

static bool
hmh_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs, double* result)
{
	hll_t template;

	hmh_compatible_template(n_hmhs, hmhs, &template);

	if (template.n_minhash_bits == 0) {
		return hll_estimate_similarity(n_hmhs, hmhs, result);
	}

	bool one_has_data = false;
	bool one_is_empty = false;

	for (uint32_t h = 0; h < n_hmhs; h++) {
		if (hmh_has_data(hmhs[h])) {
			one_has_data = true;
		}
		else {
			one_is_empty = true;
		}
	}

	if (one_is_empty) {
		*result = one_has_data ? 0.0 : 1.0;
		return true;
	}

	size_t sz = hmh_required_sz(template.n_index_bits, template.n_minhash_bits);

	if (sz == -1) {
		fail("ASSERT");
	}

	uint8_t agg_buf[sz];
	hll_t* agg_hmh = (hll_t*)agg_buf;
	hmh_init(agg_hmh, template.n_index_bits, template.n_minhash_bits);

	for (uint32_t h = 0; h < n_hmhs; h++) {
		hmh_union(agg_hmh, hmhs[h]);
	}

	uint32_t n_union = hmh_n_used_registers(agg_hmh);

	hmh_init(agg_hmh, template.n_index_bits, template.n_minhash_bits);
	hmh_union(agg_hmh, hmhs[0]);

	for (uint32_t h = 1; h < n_hmhs; h++) {
		hmh_intersect_one(agg_hmh, hmhs[h]);
	}

	uint32_t n_intersect = hmh_n_used_registers(agg_hmh);

	double ec = 0.0;

	if (n_hmhs == 2) {
		uint64_t card0 = hmh_estimate_cardinality(hmhs[0]);
		uint64_t card1 = hmh_estimate_cardinality(hmhs[1]);

		// XXX - if n sets is == 2 then estimate collisions.
		ec = hmh_jaccard_estimate_collisions(&template, card0, card1);
	}

	*result = (n_intersect - ec) / (double)n_union;

	if (*result < 0.0) {
		*result = 0.0;
	}

	return true;
}

static bool
hmh_has_data(const hll_t* hmh) {
	uint32_t n_dwords = (1 << hmh->n_index_bits) / 8;
	uint64_t* dwords = (uint64_t*)hmh->registers;

	for (uint32_t i = 0; i < n_dwords; i++) {
		if (dwords[i] != 0) {
			return true;
		}
	}

	return false;
}

static void
hmh_intersect_one(hll_t* intersect_hmh, const hll_t* hmh)
{
	// XXX - INV: hmhunion.index_bits <= hmh.index_bits
	uint32_t n_registers = 1 << intersect_hmh->n_index_bits;
	uint32_t max_registers = 1 << hmh->n_index_bits;
	uint32_t register_mask = n_registers - 1;

	for (uint32_t r = 0; r < max_registers; r++ ) {
		uint32_t small_r = r & register_mask;
		uint64_t v0 = get_register(intersect_hmh, small_r);
		uint64_t v1 = get_register(hmh, r);

		set_register(intersect_hmh, small_r, v0 == v1 ? v0 : 0);
	}
}

static uint32_t
hmh_n_used_registers(const hll_t* hmh)
{
	uint32_t n_registers = 1 << hmh->n_index_bits;
	uint32_t count = 0;

	for (uint32_t r = 0; r < n_registers; r++ ) {
		if (get_register(hmh, r)) {
			count++;
		}
	}

	return count;
}

static double
hmh_jaccard_estimate_collisions(const hll_t* template, uint64_t card0,
		uint64_t card1)
{
	double cp = 0.0;
	uint32_t n_registers = 1 << template->n_index_bits;
	uint32_t n_hll_buckets = 1 << HLL_BITS;
	double b1;
	double b2;

	// Note that i must be signed.
	for (int32_t i = 1; i <= n_hll_buckets; i++) {
		if (i != n_hll_buckets) {
			b1 = pow(2, -i);
			b2 = pow(2, -i + 1);
		}
		else {
			b1 = 0.0;
			b2 = pow(2, -i + 1);
		}

		b1 /= n_registers;
		b2 /= n_registers;

		double pr_x = pow(1 - b1, card0) - pow(1 - b2, card0);
		double pr_y = pow(1 - b1, card1) - pow(1 - b2, card1);

		cp += pr_x * pr_y;
	}

	return cp * n_registers / pow(2, template->n_minhash_bits);
}

static bool
hll_estimate_similarity(uint32_t n_hmhs, const hll_t** hmhs, double* result)
{
	uint64_t intersect_estimate;

	if (! hll_estimate_intersect_cardinality(n_hmhs, hmhs, &intersect_estimate)) {
		return false;
	}

	uint64_t union_estimate;

	if (! hmh_estimate_union_cardinality(n_hmhs, hmhs, &union_estimate)) {
		return false;
	}

	*result = intersect_estimate / (double)union_estimate;

	return true;
}


/////////////////////////////// TESTING

#include <stdlib.h> // atoi
#include <string.h>

char* RUN_MODE = "";
uint32_t N_INDEX_BITS = 0;
uint32_t N_MINHASH_BITS = 16;
uint32_t LOG_N_KEYS = 0;
char* PREFIX = "";

void fail(const char* msg)
{
	printf("FAIL: %s\n", msg);
	*(int*)0 = 0;
}

typedef struct hmh_hash_triple_s {
	uint16_t register_ix;
	uint64_t minhash_val; // bits to go into the sub-bucket
	uint8_t hll_val;
} hmh_hash_triple;

static uint64_t
pack_register(const hll_t* hmh, const hmh_hash_triple triple)
{
	return ((uint64_t)triple.hll_val << hmh->n_minhash_bits) | triple.minhash_val;
}

static hmh_hash_triple
unpack_register(const hll_t* hmh, uint64_t value)
{
	uint64_t mask = ((uint64_t)1 << hmh->n_minhash_bits) - 1;
	hmh_hash_triple triple;

	triple.minhash_val = value & mask;
	triple.hll_val = (uint8_t)(value >> hmh->n_minhash_bits);

	return triple;
}

void
test_hmh_get_set(void)
{
	size_t sz = hmh_required_sz(N_INDEX_BITS, N_MINHASH_BITS);

	if (sz == -1) {
		fail("bad sz.");
	}

	uint8_t buf[sz];
	hll_t* hmh = (hll_t*)buf;
	hmh_init(hmh, N_INDEX_BITS, N_MINHASH_BITS);
	uint32_t max_value = 1 << HLL_BITS;
	uint64_t max_minhash_val = 1 << N_MINHASH_BITS;
	uint32_t n_registers = 1 << N_INDEX_BITS;
	uint32_t hll_val = 0;
	uint64_t minhash_val = 0;

	for (uint32_t r = 0; r < n_registers; r++) {
		hll_val = (hll_val + 1) % max_value;
		minhash_val = (minhash_val + 1) % max_minhash_val;

		hmh_hash_triple triple = (hmh_hash_triple){
			.hll_val = hll_val, .minhash_val = minhash_val};
		uint64_t value = pack_register(hmh, triple);

		///
		set_register(hmh, r, value);
		uint64_t res = get_register(hmh, r);

		if (res != value) {
			fail("bad load and/or store.");
		}

		///

		triple = unpack_register(hmh, value);

		if (hll_val != triple.hll_val || minhash_val != triple.minhash_val) {
			fail("bad unpack/pack.");
		}
	}

	hll_val = 0;
	minhash_val = 0;

	// Check for corruption.
	for (uint32_t r = 0; r < n_registers; r++) {
		hll_val = (hll_val + 1) % max_value;
		minhash_val = (minhash_val + 1) % max_minhash_val;

		hmh_hash_triple triple = (hmh_hash_triple){
			.hll_val = hll_val, .minhash_val = minhash_val};

		uint64_t value = pack_register(hmh, triple);

		///

		uint64_t res = get_register(hmh, r);

		if (res != value) {
			fail("bad load and/or store.");
		}
	}

	hll_val = 0;
	minhash_val = 0;

	for (uint32_t r = n_registers - 1; r > 0; r--) {
		hll_val = (hll_val + 1) % max_value;
		minhash_val = (minhash_val + 1) % max_minhash_val;

		hmh_hash_triple triple = (hmh_hash_triple){
			.hll_val = hll_val, .minhash_val = minhash_val};
		uint64_t value = pack_register(hmh, triple);

		///

		set_register(hmh, r, value);
		uint64_t res = get_register(hmh, r);

		if (res != value) {
			fail("bad load and/or store.");
		}
	}

	hll_val = 0;
	minhash_val = 0;

	// Check for corruption.
	for (uint32_t r = n_registers - 1; r > 0; r--) {
		hll_val = (hll_val + 1) % max_value;
		minhash_val = (minhash_val + 1) % max_minhash_val;

		hmh_hash_triple triple = (hmh_hash_triple){
			.hll_val = hll_val, .minhash_val = minhash_val};
		uint64_t value = pack_register(hmh, triple);

		///

		uint64_t res = get_register(hmh, r);

		if (res != value) {
			fail("bad load and/or store.");
		}
	}

	printf("test_get_set: passed\n");
}

void
fill_hll(hll_t* hmh, uint32_t start)
{
	char key[100];

	for (uint32_t i = start; i < LOG_N_KEYS + start; i++) {
		int len = sprintf(key, "%u", i);
		hmh_insert(hmh, len, key);
	}
}

double
calc_rel_error(uint64_t estimate, uint64_t expected)
{
	double res;

	if (expected == 0) {
		res = estimate == 0 ? 0.0 : 9999.0;
	}
	else if (estimate > expected) {
		res = (estimate - expected) / (double)expected;
	}
	else {
		res = (expected - estimate) / (double)expected;
	}

	return 100 * res;
}

uint64_t
calc_abs_error(uint64_t estimate, uint64_t expected)
{
	double res;

	if (estimate > expected) {
		res = estimate - expected;
	}
	else {
		res = expected - estimate;
	}

	return res;
}

double
calc_abs_error_d(double estimate, double expected)
{
	double res;

	if (estimate > expected) {
		res = estimate - expected;
	}
	else {
		res = expected - estimate;
	}

	return res;
}

double
calc_rel_error_d(double estimate, double expected)
{
	double res;

	if (expected == 0) {
		res = estimate == 0 ? 0.0 : 9999.0;
	}
	else if (estimate > expected) {
		res = (estimate - expected) / expected;
	}
	else {
		res = (expected - estimate) / expected;
	}

	return 100 * res;
}

void
test_hmh_card(void)
{
	size_t sz = hmh_required_sz(N_INDEX_BITS, N_MINHASH_BITS);

	if (sz == -1) {
		fail("bad sz");
	}

	uint8_t buf[sz];
	hll_t* hmh = (hll_t*)buf;

	hmh_init(hmh, N_INDEX_BITS, N_MINHASH_BITS);
	fill_hll(hmh, 0);

	uint64_t expected = LOG_N_KEYS;
	uint64_t hll_est = hmh_estimate_cardinality(hmh);
	double hll_err = calc_rel_error(hll_est, expected);
	uint64_t hybrid_est = hll_est;
	double hybrid_err = calc_rel_error(hybrid_est, expected);

	printf("test cardinality: expected %lu hll_est (%lu %f) hybrid_est (%lu %f)\n",
			expected, hll_est, hll_err, hybrid_est, hybrid_err);
}

void
test_hmh_jaccard(void)
{
	size_t sz = hmh_required_sz(N_INDEX_BITS, N_MINHASH_BITS);

	if (sz == -1) {
		fail("bad sz");
	}

	const uint32_t n_hmhs = 2;

	uint8_t buf[n_hmhs][sz];
	hll_t* hmhs[] = {(hll_t*)buf[0], (hll_t*)buf[1]};

	hmh_init(hmhs[0], N_INDEX_BITS, N_MINHASH_BITS);
	hmh_init(hmhs[1], N_INDEX_BITS, N_MINHASH_BITS);
	fill_hll(hmhs[0], 0);
	fill_hll(hmhs[1], LOG_N_KEYS / 2);

	uint64_t union_estimate;

	if (! hmh_estimate_union_cardinality(n_hmhs, (const hll_t**)hmhs, &union_estimate)) {
		fail("tried to union incompatible hlls?");
	}

	uint64_t union_expected = LOG_N_KEYS / 2 + LOG_N_KEYS;
	double error = calc_rel_error(union_estimate, union_expected);

	printf("test union cardinality: expected %lu estimate %lu error %f%%\n",
			union_expected, union_estimate, error);

	uint64_t intersect_estimate;

	if (! hll_estimate_intersect_cardinality(n_hmhs, (const hll_t**)hmhs, &intersect_estimate)) {
		fail("tried to intersect incompatible hlls?");
	}

	uint64_t intersect_expected = LOG_N_KEYS / 2;
	error = calc_rel_error(intersect_estimate, intersect_expected);

	printf("test intersect cardinality: expected %lu estimate %lu error %f%%\n",
			intersect_expected, intersect_estimate, error);

	double jaccard = intersect_estimate / (double)union_estimate;

	printf("jaccard index: estimate %f\n", jaccard);

	double better_jac;

	if (! hmh_estimate_similarity(n_hmhs, (const hll_t**)hmhs, &better_jac)) {
		fail("tried to jaccard index incompatible hlls");
	}

	printf("better jaccard index: estimate %f\n", better_jac);
}

void
test_mode(int argc, char* argv[])
{
	test_hmh_get_set();
	test_hmh_card();
	test_hmh_jaccard();
}

bool
is_pow2(uint64_t n) {
	return ((n - 1) & n) == 0;
}

void
eval_card_mode(int argc, char* argv[])
{
	size_t sz = hmh_required_sz(N_INDEX_BITS, N_MINHASH_BITS);

	if (sz == -1) {
		fail("bad sz");
	}

	uint8_t buf[sz];
	hll_t* hmh = (hll_t*)buf;
	hmh_init(hmh, N_INDEX_BITS, N_MINHASH_BITS);

	char key[100];
	uint32_t i = 0;
	int32_t exp = -1;

	printf("head:\texp\tactual\test\terr\n");

	for (; i < LOG_N_KEYS; i++) {
		if (is_pow2(i)) {
			uint64_t est = hmh_estimate_cardinality(hmh);
			double err = calc_rel_error(est, i);

			printf("card:\t%d\t%u\t%lu\t%f\n", exp++, i, est, err);
			fflush(0);
		}

		int len = sprintf(key, "%s|%u", PREFIX, i);
		hmh_insert(hmh, len, key);
	}

	uint64_t est = hmh_estimate_cardinality(hmh);
	double err = calc_rel_error(est, i);

	printf("card:\t%d\t%u\t%lu\t%f\n", exp, i, est, err);
}

void
print_result(const char* tag, int32_t exp, double intersect_pct,
		uint64_t actual, uint64_t est)
{
	uint64_t abs_err = calc_abs_error(est, actual);
	double rel_err = calc_rel_error(est, actual);

	printf("%s:\t%d\t%f\t%lu\t%lu\t%lu\t%f\n",
			tag, exp, intersect_pct, actual, est, abs_err, rel_err);
}

void
print_result_d(const char* tag, int32_t exp, double intersect_pct,
		double actual, double est)
{
	double abs_err = calc_abs_error_d(est, actual);
	double rel_err = calc_rel_error_d(est, actual);

	printf("%s:\t%d\t %f\t%f\t%f\t%f\t%f\n",
			tag, exp, intersect_pct, actual, est, abs_err, rel_err);
}

void
eval_union_intersect(int argc, char* argv[])
{
	static const uint32_t overlap[] = {0, 99, 20, 10, 4, 2};
	static const double intersect_pct[] = {0.00, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.00};
	static const char* intersect_str[] = {"00", "01", "05", "10", "25", "50", "75", "90", "95", "99", "100"};
	const uint32_t n_overlaps = sizeof(overlap) / sizeof(uint32_t);
	const uint32_t n_sets = sizeof(intersect_pct) / sizeof(double);
	uint32_t icounts[n_sets];
	uint32_t ocounts[n_sets];
	memset(icounts, 0, sizeof(uint32_t) * n_sets);
	memset(ocounts, 0, sizeof(uint32_t) * n_sets);

	size_t sz = hmh_required_sz(N_INDEX_BITS, N_MINHASH_BITS);

	if (sz == -1) {
		fail("bad sz");
	}

	uint8_t buf[n_sets][sz];
	hll_t* hmh[n_sets];
	uint8_t small_buf[n_overlaps][sz];
	hll_t* small_hmh[n_sets];

	for (uint32_t h = 0; h < n_sets; h++) {
		hmh[h] = (hll_t*)buf[h];
		hmh_init(hmh[h], N_INDEX_BITS, N_MINHASH_BITS);

		if (h < n_overlaps) {
			small_hmh[h] = (hll_t*)small_buf[h];
			hmh_init(small_hmh[h], N_INDEX_BITS, N_MINHASH_BITS);
		}
	}

	uint32_t primary_h = n_sets - 1;
	hll_t* primary = hmh[primary_h];
	const uint32_t n_hmhs = 2;
	hll_t* hmhs[] = {primary, (hll_t*)NULL};
	char key[100];
	char okey[100];
	int32_t exp = -1;

	printf("head:\texp\tintersect\tactual\test\tabs_err\trel_err\n");

	// One extra loop to print the last group.
	for (uint32_t i = 0; i < LOG_N_KEYS + 1; i++) {
		if (is_pow2(i)) {
			for (uint32_t h = 0; h < n_sets; h++) {
				uint64_t est = hmh_estimate_cardinality(hmh[h]);
				print_result("card", exp, intersect_pct[h], i, est);
			}

//			for (uint32_t h = 0; h < n_sets; h++) {
//				printf("actual:\t%u\t%f\t%u\t%u\t%u\t%u\n",
//						exp, intersect_pct[h], i, icounts[h], ocounts[h],
//						icounts[h] + ocounts[h]);
//			}

			for (uint32_t h = 0; h < n_sets; h++) {
				uint64_t actual = i + ocounts[h];
				uint64_t est;

				hmhs[1] = hmh[h];

				if (! hmh_estimate_union_cardinality(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result("union", exp, intersect_pct[h], actual, est);
			}

			for (uint32_t h = 0; h < n_overlaps; h++) {
				uint64_t actual = i;
				uint64_t est;

				hmhs[1] = small_hmh[h];

				if (! hmh_estimate_union_cardinality(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result("union_subset", exp, intersect_pct[h], actual,
						est);
			}

			for (uint32_t h = 0; h < n_sets; h++) {
				if (i == 0) {
					break;
				}

				double actual = icounts[h] / (double)i;
				double est;

				hmhs[1] = hmh[h];

				if (! hmh_estimate_similarity(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result_d("similarity", exp, intersect_pct[h], actual,
						est);
			}

			for (uint32_t h = 0; h < n_overlaps; h++) {
				if (i == 0) {
					break;
				}

				double actual = icounts[h] / (double)i;
				double est;

				hmhs[1] = small_hmh[h];

				if (! hmh_estimate_similarity(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result_d("similarity_subset", exp, intersect_pct[h],
						actual, est);
			}

			for (uint32_t h = 0; h < n_sets; h++) {
				uint64_t actual = icounts[h];
				uint64_t est;

				hmhs[1] = hmh[h];

				if (! hmh_estimate_intersect_cardinality(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result("intersect", exp, intersect_pct[h], actual, est);
			}

			for (uint32_t h = 0; h < n_overlaps; h++) {
				uint64_t actual = icounts[h];
				uint64_t est;

				hmhs[1] = small_hmh[h];

				if (! hmh_estimate_intersect_cardinality(
						n_hmhs, (const hll_t**)hmhs, &est)) {
					fail("estimation failure!");
				}

				print_result("intersect_subset", exp, intersect_pct[h], actual,
						est);
			}

			exp++;

			fflush(0);
		}

		int len = sprintf(key, "%s|%u", PREFIX, i);
		uint32_t h = 0;

		for (; h < n_overlaps; h++) {
			int olen = sprintf(okey, "%s|%s|%u", intersect_str[h], PREFIX, i);

			if (overlap[h] != 0 && i % overlap[h] == 0) {
				icounts[h]++;
				hmh_insert(hmh[h], len, key);
				hmh_insert(small_hmh[h], len, key);
			}
			else {
				ocounts[h]++;
				hmh_insert(hmh[h], olen, okey);
			}
		}

		for (; h < n_sets; h++) {
			int olen = sprintf(okey, "%s|%s|%u", intersect_str[h], PREFIX, i);
			uint32_t o = (n_overlaps - ((h % n_overlaps) + 1)) - 1;
			//printf("o %u pct %s\n", o, intersect_str[h]);

			if (overlap[o] == 0 || i % overlap[o] != 0) {
				icounts[h]++;
				hmh_insert(hmh[h], len, key);
			}
			else {
				ocounts[h]++;
				hmh_insert(hmh[h], olen, okey);
			}
		}
	}
}

int
main(int argc, char* argv[])
{
	uint32_t argc_max = 5;

	if (argc != argc_max + 1) {
		printf("syntax:\thll mode n_index_bits n_minhash_bits log_n_keys prefix\n");
		return 1;
	}

	uint32_t i = 1;

	RUN_MODE = argv[i++];
	N_INDEX_BITS = atoi(argv[i++]);
	N_MINHASH_BITS = atoi(argv[i++]);
	LOG_N_KEYS = 1 << atoi(argv[i++]);
	PREFIX = argv[i++];

	printf("params:\tmode %s n_index_bits %u n_minhash_bits %u log_n_keys %u prefix %s\n",
			RUN_MODE, N_INDEX_BITS, N_MINHASH_BITS, LOG_N_KEYS, PREFIX);

	if (i != argc_max + 1) {
		fail("didn't process all args.");
	}

	static const char* TEST_MODE = "test";
	static const char* EVAL_CARD_MODE = "eval-card";
	static const char* EVAL_UI = "eval-ui";

	size_t mode_len = strlen(RUN_MODE);

	if (strcmp(RUN_MODE, TEST_MODE) == 0) {
		test_mode(argc, argv);
	}
	else if (strcmp(RUN_MODE, EVAL_CARD_MODE) == 0) {
		eval_card_mode(argc, argv);
	}
	else if (strcmp(RUN_MODE, EVAL_UI) == 0) {
		eval_union_intersect(argc, argv);
	}
	else {
		printf("invalid 'mode'\n");
		return 1;
	}

	return 0;
}
