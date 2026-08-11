/*
 * replica_write.c
 *
 * Copyright (C) 2016-2026 Aerospike, Inc.
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

#include "transaction/replica_write.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "enhanced_alloc.h"
#include "log.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "base/zstd_wire.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/replica_write_test_support.h" // helpers kept non-static for the unit tests
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"

//==========================================================
// Forward declarations.
//

static as_namespace* repl_write_get_namespace_or_nack(cf_node node, msg* m,
		const char* tag);
static uint32_t pack_info_bits(as_transaction* tr);
static void fill_repl_write_message(msg* m, rw_request* rw, as_transaction* tr);
static void send_repl_write_ack(cf_node node, msg* m, uint32_t result);
static void send_repl_write_ack_w_digest(cf_node node, msg* m, uint32_t result,
		const cf_digest* keyd);
static uint32_t parse_result_code(msg* m);
static void drop_replica(as_partition_reservation* rsv, cf_digest* keyd);

//==========================================================
// Public API.
//

void
repl_write_make_message(rw_request* rw, as_transaction* tr)
{
	if (rw->dest_msg) {
		as_fabric_msg_put(rw->dest_msg);
	}

	// Cache info bits on the initial build. Re-builds during
	// delta-fallback pass a zero-initialized synthetic transaction whose
	// origin/flags would silently drop bits like RW_INFO_NO_REPL_ACK.
	// pack_info_bits is idempotent on the same tr, so re-computing here on
	// the initial call is fine; subsequent rebuilds reuse the cached value.
	if (! rw->repl_info_bits_set) {
		rw->repl_info_bits = pack_info_bits(tr);
		rw->repl_info_bits_set = true;
	}

	rw->dest_msg = as_fabric_msg_get(M_TYPE_RW);
	fill_repl_write_message(rw->dest_msg, rw, tr);
}

void
repl_write_setup_rw(rw_request* rw, as_transaction* tr,
		repl_write_done_cb repl_write_cb, timeout_done_cb timeout_cb)
{
	rw->msgp = tr->msgp;
	tr->msgp = NULL;

	rw->msg_fields = tr->msg_fields;
	rw->origin = tr->origin;
	rw->from_flags = tr->from_flags;

	rw->from.any = tr->from.any;
	rw->from_data.any = tr->from_data.any;
	tr->from.any = NULL;

	rw->start_time = tr->start_time;
	rw->benchmark_time = tr->benchmark_time;

	as_partition_reservation_copy(&rw->rsv, &tr->rsv);
	// Hereafter, rw_request must release reservation - happens in destructor.

	rw->end_time = tr->end_time;
	rw->flags = tr->flags;
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;
	rw->last_update_time = tr->last_update_time;

	rw->repl_write_cb = repl_write_cb;
	rw->timeout_cb = timeout_cb;

	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}

	// Allow retransmit thread to destroy rw_request as soon as we unlock.
	as_store_bool_rls(&rw->is_set_up, true);

	if (as_health_sample_replica_write()) {
		rw->repl_start_us = cf_getus();
	}

	// Gated like repl_start_us above it - a clock read per write (and per
	// retransmit, in repl_write_reset_rw) is not free at write throughput. The
	// consumer in repl_write_handle_ack() requires the same flag, so an
	// unsampled write's 0 is never read as a timestamp. Read once here rather
	// than at both ends: the flag is dynamic, and a flip between setup and ack
	// would otherwise pair a histogram insert with a zero start.
	rw->repl_start_ns = tr->rsv.ns->repl_benchmarks_enabled ? cf_getns() : 0;
}

void
repl_write_reset_rw(rw_request* rw, as_transaction* tr, repl_write_done_cb cb)
{
	// Reset rw->from.any which was set null in tr setup.
	rw->from.any = tr->from.any;

	// Needed for response to origin.
	rw->flags = tr->flags;
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;
	rw->last_update_time = tr->last_update_time;

	rw->repl_write_cb = cb;

	// TODO - is this better than not resetting? Note - xmit_ms not volatile.
	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (uint32_t i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}

	if (as_health_sample_replica_write()) {
		rw->repl_start_us = cf_getus();
	}

	rw->repl_start_ns = tr->rsv.ns->repl_benchmarks_enabled ? cf_getns() : 0;
}

void
repl_write_reset_replicas(rw_request* rw)
{
	cf_node nodes[AS_CLUSTER_SZ];
	uint32_t n_nodes = as_partition_get_other_replicas(rw->rsv.p, nodes);

	if (n_nodes == rw->n_dest_nodes &&
			memcmp(nodes, rw->dest_nodes, n_nodes * sizeof(cf_node)) == 0) {
		return; // almost always - replica destinations unchanged
	}

	if (! sufficient_replica_destinations(rw->rsv.ns, n_nodes)) {
		return; // don't change destinations - time out if not enough replicas
	}
	// else - use new replica destinations. Note - could be same nodes just
	// reordered, but not worth detecting this.

	// Initialize or preserve completion status.

	define_deferred_array(complete, bool, n_nodes);

	for (uint32_t n = 0; n < n_nodes; n++) {
		complete[n] = false;

		for (uint32_t old_n = 0; old_n < rw->n_dest_nodes; old_n++) {
			if (nodes[n] == rw->dest_nodes[old_n]) {
				complete[n] = rw->dest_complete[old_n];
				break;
			}
		}
	}

	// Install the new list of replica destinations.

	rw->n_dest_nodes = n_nodes;

	for (uint32_t n = 0; n < n_nodes; n++) {
		rw->dest_nodes[n] = nodes[n];
		rw->dest_complete[n] = complete[n];
	}
}

static uint32_t
apply_delta_replication(as_partition_reservation* rsv, cf_digest* keyd,
		const uint8_t* delta, size_t delta_sz, uint32_t base_generation,
		uint64_t base_lut, as_remote_record* rr)
{
	as_namespace* ns = rsv->ns;
	as_index_ref r_ref;
	if (as_record_get(rsv->tree, keyd, &r_ref) != 0) {
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_no_record);
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	as_record* r = r_ref.r;

	if (r->generation != base_generation || r->last_update_time != base_lut) {
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_version);
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	as_storage_rd rd;
	as_storage_record_open(ns, r, &rd);

	// Sampled before the load below, which is what may reach storage. This is
	// the one replica-write mode that can pay a prole read - the plain and
	// compressed handlers never touch the old record - so it is counted
	// separately, otherwise the extra read IOPS are unattributable.
	//
	// The engine is the whole test, deliberately: rd was just opened, and
	// as_storage_record_open() zeroes rd.flat while no engine's open sets it
	// (each sets only its own union member), so rd.flat is NULL here on every
	// engine and the load always goes to the record's backing store. On an
	// in-memory namespace that store is the arena - no I/O - which is why the
	// engine check has to be here at all: counting those would inverse the very
	// signal this stat exists to give ("is delta-zstd costing me read IOPS?").
	// If this sample ever moves after a load, it needs an rd.flat == NULL
	// conjunct again to stay honest.
	bool load_hit_device = ns->storage_type != AS_STORAGE_ENGINE_MEMORY;
	int load_rv = as_storage_rd_lazy_load_bins(&rd, NULL);

	if (load_hit_device) {
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_apply_device_reads);
	}

	if (load_rv < 0) {
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_other);
		// Return RECORD_VERSION_MISMATCH (not AS_ERR_UNKNOWN) so the master
		// retransmits the full pickle. AS_ERR_UNKNOWN would mark the dest
		// complete-but-failed and silently leave the replica stale.
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	if (rd.flat != NULL && rd.flat->is_compressed == 1) {
		// Local stored flat is storage-compressed: the bin load decompressed
		// into a separate buffer and repointed rd.flat_end there, so
		// old_flat_sz below would span two unrelated allocations. The
		// compressed bytes wouldn't match the master's canonical base anyway.
		// Reject so the master retransmits the full pickle.
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_other);
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	uint32_t old_flat_sz = rd.flat_end - (const uint8_t*)rd.flat;

	if (old_flat_sz < sizeof(as_flat_record)) {
		// Corrupt/short local flat - don't zero a tree_id past the buffer end
		// in as_flat_make_delta_canonical(); fall back to a full pickle.
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_other);
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	// Strip our local tree_id from the source flat so it matches the master's
	// canonicalised dictionary input (see compute_delta_for_replication +
	// as_flat_make_delta_canonical).
	uint8_t* canonical_flat = cf_malloc(old_flat_sz);
	as_flat_make_delta_canonical(canonical_flat, rd.flat, old_flat_sz);

	rr->pickle_sz = zstd_wire_apply_patch((void**)&rr->pickle, canonical_flat,
			old_flat_sz, delta, delta_sz);

	cf_free(canonical_flat);

	if (rr->pickle_sz == 0) {
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_apply);
		return AS_ERR_RECORD_VERSION_MISMATCH; // triggers fallback to full pickle
	}

	if (! as_flat_unpack_remote_record_meta(ns, rr)) {
		cf_warning(AS_RW, "delta replication: failed to unpack metadata");
		cf_free(rr->pickle);
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_other);
		// Same rationale as the lazy_load_bins failure above: force a
		// retransmit rather than silently dropping the replica write.
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	// The partition was reserved with the message digest, but the commit below
	// keys off rr->keyd unpacked from the reconstructed pickle. They can only
	// diverge if something slipped past the codec checksum - fail closed
	// (forcing the full-pickle fallback) rather than apply into the wrong
	// partition's tree.
	if (memcmp(rr->keyd, keyd, sizeof(cf_digest)) != 0) {
		cf_warning(AS_RW, "delta replication: digest mismatch");
		cf_free(rr->pickle);
		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);
		as_incr_uint64(&ns->repl_wire_comp_stat.delta_reject_other);
		return AS_ERR_RECORD_VERSION_MISMATCH;
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	uint32_t result = (uint32_t)as_record_replace_if_better(rr);

	cf_free(rr->pickle);
	return result;
}

void
repl_write_delta_handle_op(cf_node node, msg* m)
{
	as_remote_record rr = { .via = VIA_REPLICATION, .src = node };
	as_namespace* ns = repl_write_get_namespace_or_nack(node, m, "delta");

	if (ns == NULL) {
		return; // already acked and consumed m
	}

	uint8_t* delta;
	size_t delta_sz;

	if (msg_get_buf(m, RW_FIELD_RECORD, &delta, &delta_sz, MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "delta: no delta buffer");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	uint32_t base_generation;
	uint64_t base_lut;

	if (msg_get_uint32(m, RW_FIELD_GENERATION, &base_generation) != 0 ||
			msg_get_uint64(m, RW_FIELD_LAST_UPDATE_TIME, &base_lut) != 0) {
		cf_warning(AS_RW, "delta: missing base version");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	msg_get_uint32(m, RW_FIELD_REGIME, &rr.regime);
	cf_digest* keyd;
	size_t keyd_size;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, &keyd_size,
				MSG_GET_DIRECT) != 0 ||
			keyd_size != CF_DIGEST_KEY_SZ) {
		cf_warning(AS_RW, "delta: invalid digest");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	if (as_storage_overloaded(ns, 192, "replica delta")) {
		send_repl_write_ack_w_digest(node, m, AS_ERR_DEVICE_OVERLOAD, keyd);
		return;
	}

	as_partition_reservation rsv;
	uint32_t result =
			as_partition_reserve_replica(ns, as_partition_getid(keyd), &rsv);

	if (result != AS_OK) {
		send_repl_write_ack_w_digest(node, m, result, keyd);
		return;
	}
	rr.rsv = &rsv;

	// MRT writes to an existing record need the original pickle to resolve;
	// the master appends it on the MRT_BLOCKED retransmit. Without this read
	// the delta op loops MRT_BLOCKED until the transaction times out.
	repl_write_get_orig_pickle(m, &rr);

	result = apply_delta_replication(&rsv, keyd, delta, delta_sz,
			base_generation, base_lut, &rr);

	as_partition_release(&rsv);
	send_repl_write_ack_w_digest(node, m, result, keyd);
	// Message released by fabric layer
}

void
repl_write_compressed_handle_op(cf_node node, msg* m)
{
	cf_digest* keyd = msg_get_digest(m, RW_FIELD_DIGEST);
	as_namespace* ns = repl_write_get_namespace_or_nack(node, m, "compressed");

	if (ns == NULL) {
		return; // already acked and consumed m
	}

	as_remote_record rr = { .via = VIA_REPLICATION, .src = node };

	msg_get_uint32(m, RW_FIELD_REGIME, &rr.regime);

	uint8_t* compressed;
	size_t compressed_sz;

	if (msg_get_buf(m, RW_FIELD_RECORD, &compressed, &compressed_sz,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_compressed_handle_op: no record");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	if (keyd == NULL) {
		cf_warning(AS_RW, "repl_write_compressed_handle_op: no digest");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	// Check storage overload BEFORE doing the expensive zstd decompress
	// and the meta-unpack work. The delta handler already does this; the
	// non-compressed handler does too. A peer ack-storm during overload
	// must not force us into full decode for records we'd reject anyway.
	if (as_storage_overloaded(ns, 192, "replica write")) {
		send_repl_write_ack_w_digest(node, m, AS_ERR_DEVICE_OVERLOAD, keyd);
		return;
	}

	// Reserve BEFORE decompressing, on the message digest - the delta handler
	// does the same. Decompressing first meant expanding up to 8 MiB of
	// peer-supplied bytes, plus the meta-unpack, before establishing that this
	// node is even a replica for the partition the digest maps to: a peer could
	// spend our CPU and memory on records we were always going to refuse. The
	// reservation is now held across the decode, which is what the delta path
	// already does (it holds it across both a decode and a device read), so this
	// is strictly the lighter of the two.
	as_partition_reservation rsv;
	uint32_t result =
			as_partition_reserve_replica(ns, as_partition_getid(keyd), &rsv);

	if (result != AS_OK) {
		send_repl_write_ack_w_digest(node, m, result, keyd);
		return;
	}

	rr.pickle_sz = zstd_wire_decompress_buffer((void**)&rr.pickle, compressed,
			compressed_sz);

	if (rr.pickle_sz == 0) {
		cf_warning(AS_RW,
				"repl_write_compressed_handle_op: bad compressed record");
		as_partition_release(&rsv);
		// Return RECORD_VERSION_MISMATCH (not AS_ERR_UNKNOWN) so the master
		// falls back to a full uncompressed pickle. AS_ERR_UNKNOWN is not
		// retransmittable, so it would mark the dest complete-but-failed and
		// silently leave the replica stale; retransmitting the same compressed
		// payload (which is deterministically un-decodable) would loop forever.
		// The master's repl_write_needs_pickle_fallback() switches off
		// compression on this code.
		send_repl_write_ack_w_digest(node, m, AS_ERR_RECORD_VERSION_MISMATCH,
				keyd);
		return;
	}

	repl_write_get_orig_pickle(m, &rr);

	if (! as_flat_unpack_remote_record_meta(ns, &rr)) {
		cf_warning(AS_RW, "repl_write_compressed_handle_op: bad record");
		cf_free(rr.pickle);
		as_partition_release(&rsv);
		// Same rationale as the decompress-failure path above: force a
		// full-pickle fallback rather than silently dropping the replica write.
		send_repl_write_ack_w_digest(node, m, AS_ERR_RECORD_VERSION_MISMATCH,
				keyd);
		return;
	}

	// as_flat_unpack_remote_record_meta() points rr.keyd at the digest inside
	// the decompressed payload, but we reserved on the message digest. They must
	// agree: reserving one partition and then applying a record belonging to
	// another would write outside the reservation. Well-formed senders always
	// match (fill_repl_write_message() copies tr->keyd into RW_FIELD_DIGEST),
	// so a mismatch is a corrupt or hostile payload - refuse it the same way as
	// a failed unpack, which makes the master fall back to a full pickle.
	if (cf_digest_compare(rr.keyd, keyd) != 0) {
		cf_warning(AS_RW,
				"repl_write_compressed_handle_op: payload digest != msg digest");
		cf_free(rr.pickle);
		as_partition_release(&rsv);
		send_repl_write_ack_w_digest(node, m, AS_ERR_RECORD_VERSION_MISMATCH,
				keyd);
		return;
	}

	rr.rsv = &rsv;

	result = (uint32_t)as_record_replace_if_better(&rr);

	as_partition_release(&rsv);
	send_repl_write_ack_w_digest(node, m, result, rr.keyd);
	cf_free(rr.pickle);
}

void
repl_write_handle_op(cf_node node, msg* m)
{
	as_namespace* ns = repl_write_get_namespace_or_nack(node, m, "repl-write");

	if (ns == NULL) {
		return; // already acked and consumed m
	}

	cf_digest* keyd;
	size_t keyd_size;

	// Handle drops.
	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, &keyd_size,
				MSG_GET_DIRECT) == 0) {
		if (keyd_size != CF_DIGEST_KEY_SZ) {
			cf_warning(AS_RW, "repl_write_handle_op: invalid digest");
			send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
			return;
		}

		as_partition_reservation rsv;
		uint32_t result =
				as_partition_reserve_replica(ns, as_partition_getid(keyd), &rsv);

		if (result == AS_OK) {
			drop_replica(&rsv, keyd);
			as_partition_release(&rsv);
		}

		send_repl_write_ack(node, m, result);

		return;
	}
	// else - flat record, including tombstone.

	as_remote_record rr = { .via = VIA_REPLICATION, .src = node };

	msg_get_uint32(m, RW_FIELD_REGIME, &rr.regime);

	if (msg_get_buf(m, RW_FIELD_RECORD, &rr.pickle, &rr.pickle_sz,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no record");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	repl_write_get_orig_pickle(m, &rr);

	if (! as_flat_unpack_remote_record_meta(ns, &rr)) {
		cf_warning(AS_RW, "repl_write_handle_op: bad record");
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return;
	}

	// Replica writes are the last thing cut off when storage is backed up.
	if (as_storage_overloaded(ns, 192, "replica write")) {
		send_repl_write_ack_w_digest(node, m, AS_ERR_DEVICE_OVERLOAD, rr.keyd);
		return;
	}

	as_partition_reservation rsv;
	uint32_t result =
			as_partition_reserve_replica(ns, as_partition_getid(rr.keyd), &rsv);

	if (result != AS_OK) {
		send_repl_write_ack_w_digest(node, m, result, rr.keyd);
		return;
	}

	rr.rsv = &rsv;

	result = (uint32_t)as_record_replace_if_better(&rr);

	as_partition_release(&rsv);
	send_repl_write_ack_w_digest(node, m, result, rr.keyd);
}

// Read the optional MRT original-record pickle (RW_FIELD_ORIG_RECORD) from a
// replica-write message into rr. The field is present only on a retransmit
// after the prole asked for it by acking AS_ERR_MRT_BLOCKED (see
// repl_write_with_orig / repl_write_should_retransmit_replicas); on a first
// send it is absent, so a failed get is normal and just leaves rr->orig_pickle
// NULL. Every replica handler that calls as_record_replace_if_better - plain,
// compressed, and delta - must call this first, or an MRT write to an existing
// record never resolves and the master retransmits until the MRT times out.
void
repl_write_get_orig_pickle(msg* m, as_remote_record* rr)
{
	msg_get_buf(m, RW_FIELD_ORIG_RECORD, &rr->orig_pickle, &rr->orig_pickle_sz,
			MSG_GET_DIRECT);
}

// Whether a successful replica-write ack should credit ns bytes_saved. Gated on
// wire_compressed_op_sent (what fill_repl_write_message actually put on the
// wire), NOT use_delta/use_compressed (what was computed) - otherwise a
// rolling-upgrade compat-gated plain send would credit savings for a record
// that went out uncompressed. Pure so the rule is unit-testable.
//
// Note - bytes_saved is a lower bound: both inputs are single per-rw_request
// scalars, so on a multi-replica send where one replica applies the compressed
// op and another forces a pickle fallback, the fallback zeroes the pending
// savings and a later AS_OK ack from the replica that DID apply it credits
// nothing (ack-order dependent). We accept the undercount rather than track
// per-destination state.
bool
repl_write_ack_credits_bytes_saved(bool wire_compressed_op_sent,
		uint32_t result_code, uint64_t per_dest_bytes_saved)
{
	return result_code == AS_OK && wire_compressed_op_sent &&
			per_dest_bytes_saved != 0;
}

bool
repl_write_needs_pickle_fallback(const rw_request* rw, uint32_t result_code)
{
	// A delta or compressed replica op the peer could not apply/decode is
	// reported as AS_ERR_RECORD_VERSION_MISMATCH (see apply_delta_replication
	// and repl_write_compressed_handle_op). Both must fall back to a plain
	// full pickle: re-sending the same delta (against a base the replica does
	// not have) or the same compressed payload (deterministically
	// un-decodable) would otherwise retransmit forever.
	return result_code == AS_ERR_RECORD_VERSION_MISMATCH &&
			(rw->use_delta || rw->use_compressed);
}

void
repl_write_handle_ack(cf_node node, msg* m)
{
	uint32_t ns_ix;

	if (msg_get_uint32(m, RW_FIELD_NS_IX, &ns_ix) != 0) {
		cf_warning(AS_RW, "repl-write ack: no ns-ix");
		as_fabric_msg_put(m);
		return;
	}

	cf_digest* keyd = msg_get_digest(m, RW_FIELD_DIGEST);

	if (keyd == NULL) {
		cf_warning(AS_RW, "repl-write ack: no or bad digest");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t tid;

	if (msg_get_uint32(m, RW_FIELD_TID, &tid) != 0) {
		cf_warning(AS_RW, "repl-write ack: no tid");
		as_fabric_msg_put(m);
		return;
	}

	rw_request_hkey hkey = { ns_ix, *keyd };
	rw_request* rw = rw_request_hash_get(&hkey);

	if (! rw) {
		// Extra ack, after rw_request is already gone.
		as_fabric_msg_put(m);
		return;
	}

	cf_mutex_lock(&rw->lock);

	if (rw->tid != tid || rw->repl_write_complete) {
		// Extra ack - rw_request is newer transaction for same digest, or ack
		// is arriving after rw_request was aborted.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (! rw->from.any) {
		// Lost race against timeout in retransmit thread.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	// Find remote node in replicas list.
	int i = index_of_node(rw->dest_nodes, rw->n_dest_nodes, node);

	if (i == -1) {
		cf_detail(AS_RW, "repl-write ack: from non-dest node %lx", node);
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (rw->dest_complete[i]) {
		// Extra ack for this replica write.
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	uint32_t result_code = parse_result_code(m);

	if (repl_write_needs_pickle_fallback(rw, result_code)) {
		as_incr_uint64(&rw->rsv.ns->repl_wire_comp_stat.fallback_count);
		rw->use_delta = false;
		rw->use_compressed = false;
		if (rw->delta != NULL) {
			cf_free(rw->delta);
			rw->delta = NULL;
		}
		if (rw->compressed != NULL) {
			cf_free(rw->compressed);
			rw->compressed = NULL;
		}
		// Pending savings no longer apply — the rebuilt plain message saves
		// nothing on subsequent acks.
		rw->per_dest_bytes_saved = 0;

		// Synthetic as_transaction for the fallback rebuild. rsv and keyd
		// are required by fill_repl_write_message; repl_info_bits were
		// cached on the initial build (see repl_write_make_message).
		as_transaction tr_stack;
		memset(&tr_stack, 0, sizeof(tr_stack));
		tr_stack.rsv = rw->rsv;
		tr_stack.keyd = rw->keyd;
		repl_write_make_message(rw, &tr_stack);
		rw->xmit_ms = 0;
	}

	// If it makes sense, retransmit replicas. Note - rw->dest_complete[i] not
	// yet set true, so that retransmit will go to this remote node.
	if (repl_write_should_retransmit_replicas(rw, result_code)) {
		cf_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	rw->dest_complete[i] = true;

	// Credit per-destination bytes saved. Only AS_OK acks of a message
	// that actually went out delta/compressed contribute — after a delta-
	// fallback rebuild, or when the rolling-upgrade compat gate forced a plain
	// send, wire_compressed_op_sent is false and nothing was saved on the wire.
	// per_dest_bytes_saved is zero unless compute_delta_for_replication or
	// compute_compression_for_replication set it.
	if (repl_write_ack_credits_bytes_saved(rw->wire_compressed_op_sent,
				result_code, rw->per_dest_bytes_saved)) {
		as_add_uint64(&rw->rsv.ns->repl_wire_comp_stat.bytes_saved,
				rw->per_dest_bytes_saved);
	}

	as_health_add_ns_latency(node, ns_ix, AS_HEALTH_NS_REPL_LAT,
			rw->repl_start_us);

	// Dedicated repl-latency histogram (separate from the sampled
	// as_health stat above). Gated on enable-benchmarks-repl so
	// production clusters pay nothing; histogram itself is always
	// created so the flag flips dynamically.
	as_namespace* ns_for_hist = rw->rsv.ns;
	if (ns_for_hist->repl_benchmarks_enabled && rw->repl_start_ns != 0) {
		histogram_insert_data_point(ns_for_hist->repl_write_hist,
				rw->repl_start_ns);
	}

	for (uint32_t j = 0; j < rw->n_dest_nodes; j++) {
		if (! rw->dest_complete[j]) {
			// Still haven't heard from all replicas.
			cf_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			as_fabric_msg_put(m);
			return;
		}
	}

	// Success for all replicas.
	repl_write_send_confirmation(rw);

	// Continuation runs on a fabric thread - re-arm error-detail verbosity from
	// the client's info4 bits (carried in rw->msgp), mirroring the service
	// thread.
	as_error_msg_arm_from_msgp(rw->msgp);

	rw->repl_write_cb(rw);

	// Keep the no-leak property local to this fabric handler. Disarm, not
	// clear: fabric receive threads are pooled and rw_msg_cb dispatches
	// unarmed work (repl_write_handle_op, dup_res_handle_request) onto the
	// same thread, and that work authors storage details of its own. Leaving
	// the tier armed would let it do so on behalf of whichever client this
	// thread last served.
	as_error_msg_disarm();

	rw->repl_write_complete = true;

	cf_mutex_unlock(&rw->lock);
	rw_request_hash_delete(&hkey, rw);
	rw_request_release(rw);
	as_fabric_msg_put(m);
}

//==========================================================
// Local helpers.
//

// Shared preamble for all three replica-write receive handlers: resolve the
// message's namespace, or NACK and consume m. Returns NULL when it has already
// acked - the caller must just return. One copy of the policy so a change to it
// (a different result code during a rolling upgrade, an extra log field) is one
// edit rather than three that have to agree.
static as_namespace*
repl_write_get_namespace_or_nack(cf_node node, msg* m, const char* tag)
{
	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
				MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "%s: no namespace", tag);
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return NULL;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (ns == NULL) {
		cf_warning(AS_RW, "%s: invalid namespace", tag);
		send_repl_write_ack(node, m, AS_ERR_UNKNOWN);
		return NULL;
	}

	return ns;
}

static uint32_t
pack_info_bits(as_transaction* tr)
{
	uint32_t info = 0;

	if (respond_on_master_complete(tr)) {
		info |= RW_INFO_NO_REPL_ACK;
	}

	return info;
}

static void
fill_repl_write_message(msg* m, rw_request* rw, as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;

	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_IX, ns->ix);
	msg_set_uint32(m, RW_FIELD_TID, rw->tid);

	if (rw->pickle != NULL) {
		repl_write_add_regime(m, tr);

		// Delta and compressed ops were added in compatibility id
		// RW_WIRE_COMPRESSION_COMPATIBILITY_ID. During a rolling upgrade an
		// older peer would silently drop these ops and the master would
		// retransmit indefinitely, so only emit them once the whole cluster
		// has caught up.
		bool peers_support_wire_compression = as_exchange_min_compatibility_id() >=
				RW_WIRE_COMPRESSION_COMPATIBILITY_ID;

		if (peers_support_wire_compression && rw->use_delta && rw->delta != NULL) {
			msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE_DELTA);
			msg_set_buf(m, RW_FIELD_RECORD, rw->delta, rw->delta_sz,
					MSG_SET_COPY);
			msg_set_uint32(m, RW_FIELD_GENERATION, rw->base_generation);
			msg_set_uint64(m, RW_FIELD_LAST_UPDATE_TIME, rw->base_lut);
			msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
					MSG_SET_COPY);
			rw->wire_compressed_op_sent = true;
		}
		else if (peers_support_wire_compression && rw->use_compressed &&
				rw->compressed != NULL) {
			msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE_COMPRESSED);
			msg_set_buf(m, RW_FIELD_RECORD, rw->compressed, rw->compressed_sz,
					MSG_SET_COPY);
			msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
					MSG_SET_COPY);
			rw->wire_compressed_op_sent = true;
		}
		else {
			// Plain RW_OP_REPL_WRITE is terminal. It is reached on the initial
			// send (no wire compression), on a delta/compressed fallback rebuild
			// after repl_write_needs_pickle_fallback() cleared
			// use_delta/use_compressed, or when the rolling-upgrade compat gate
			// forces plain despite a computed compressed payload. In every case
			// nothing went out compressed, so clear the sent-flag — otherwise
			// repl_write_handle_ack() would credit bytes_saved for an
			// uncompressed message. Hand off the pickle to fabric to preserve
			// the pre-wire-compression zero-copy fast path.
			msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE);
			msg_set_buf(m, RW_FIELD_RECORD, rw->pickle, rw->pickle_sz,
					MSG_SET_HANDOFF_MALLOC);
			rw->pickle = NULL; // fabric owns the buffer now
			rw->wire_compressed_op_sent = false;
		}
	}
	else {
		msg_set_uint32(m, RW_FIELD_OP, RW_OP_REPL_WRITE);
		msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
				MSG_SET_COPY);
	}

	// Use the cached repl_info_bits (set on the initial build in
	// repl_write_make_message). On delta-fallback retransmit tr is a
	// zero-initialized synthetic, so pack_info_bits(tr) here would lose
	// the bits the initial send carried.
	uint32_t info = rw->repl_info_bits;

	if (info != 0) {
		msg_set_uint32(m, RW_FIELD_INFO, info);
	}
}

static void
send_repl_write_ack(cf_node node, msg* m, uint32_t result)
{
	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if ((info & RW_INFO_NO_REPL_ACK) != 0) {
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 3, RW_FIELD_NS_IX, RW_FIELD_DIGEST, RW_FIELD_TID);

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static void
send_repl_write_ack_w_digest(cf_node node, msg* m, uint32_t result,
		const cf_digest* keyd)
{
	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	if ((info & RW_INFO_NO_REPL_ACK) != 0) {
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 2, RW_FIELD_NS_IX, RW_FIELD_TID);

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);
	msg_set_buf(m, RW_FIELD_DIGEST, (const uint8_t*)keyd, sizeof(cf_digest),
			MSG_SET_COPY);

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}

static uint32_t
parse_result_code(msg* m)
{
	uint32_t result_code;

	if (msg_get_uint32(m, RW_FIELD_RESULT, &result_code) != 0) {
		cf_warning(AS_RW, "repl-write ack: no result_code");
		return AS_ERR_UNKNOWN;
	}

	return result_code;
}

static void
drop_replica(as_partition_reservation* rsv, cf_digest* keyd)
{
	// Shortcut pointers & flags.
	as_namespace* ns = rsv->ns;
	as_index_tree* tree = rsv->tree;

	as_index_ref r_ref;

	if (as_record_get(tree, keyd, &r_ref) != 0) {
		return; // not found is ok from master's perspective.
	}

	// MRT PARANOIA - can we encounter a provisional here?
	cf_assert(r_ref.r->orig_h == 0, AS_RW, "unexpected - dropped provisional");

	if (ns->storage_type != AS_STORAGE_ENGINE_SSD ||
			ns->pi_xmem_type == CF_XMEM_TYPE_FLASH) {
		remove_from_sindex(ns, &r_ref);
	}

	// Note - may find a tombstone here if replica missed a generation.
	as_set_index_delete_live(ns, tree, r_ref.r, r_ref.r_h);
	as_index_delete(tree, keyd);
	as_record_done(&r_ref, ns);
}
