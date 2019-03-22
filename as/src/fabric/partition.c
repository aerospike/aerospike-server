/*
 * partition.c
 *
 * Copyright (C) 2008-2018 Aerospike, Inc.
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

#include "fabric/partition.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_b64.h"

#include "cf_mutex.h"
#include "fault.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "fabric/partition_balance.h"


//==========================================================
// Forward declarations.
//

cf_node find_best_node(const as_partition* p, bool is_read);
void accumulate_replica_stats(const as_partition* p, uint64_t* p_n_objects, uint64_t* p_n_tombstones);
void partition_reserve_lockfree(as_partition* p, as_namespace* ns, as_partition_reservation* rsv);
char partition_descriptor(const as_partition* p);
int partition_get_replica_self_lockfree(const as_namespace* ns, uint32_t pid);


//==========================================================
// Public API.
//

void
as_partition_init(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	// Note - as_partition has been zeroed since it's a member of as_namespace.
	// Set non-zero members.

	cf_mutex_init(&p->lock);

	p->id = pid;

	if (ns->cold_start) {
		return; // trees are created later when we know which ones we own
	}

	p->tree = as_index_tree_resume(&ns->tree_shared, ns->xmem_trees, pid,
			as_partition_tree_done, (void*)p);
}

void
as_partition_shutdown(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	while (true) {
		cf_mutex_lock(&p->lock);

		as_index_tree* tree = p->tree;
		as_index_tree_reserve(tree);

		cf_mutex_unlock(&p->lock);

		// Must come outside partition lock, since transactions may take
		// partition lock under the record (sprig) lock.
		as_index_tree_block(tree);

		// If lucky, this remains locked and we complete shutdown.
		cf_mutex_lock(&p->lock);

		if (tree == p->tree) {
			break; // lucky - same tree we blocked
		}

		// Bad luck - blocked a tree that just got switched, block the new one.
		cf_mutex_unlock(&p->lock);
	}
}

void
as_partition_freeze(as_partition* p)
{
	p->working_master = (cf_node)0;

	p->n_nodes = 0;
	p->n_replicas = 0;
	p->n_dupl = 0;

	p->pending_emigrations = 0;
	p->pending_lead_emigrations = 0;
	p->pending_immigrations = 0;

	p->n_witnesses = 0;
}

// Get a list of all nodes (excluding self) that are replicas for a specified
// partition: place the list in *nv and return the number of nodes found.
uint32_t
as_partition_get_other_replicas(as_partition* p, cf_node* nv)
{
	uint32_t n_other_replicas = 0;

	cf_mutex_lock(&p->lock);

	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		// Don't ever include yourself.
		if (p->replicas[repl_ix] == g_config.self_node) {
			continue;
		}

		// Copy the node ID into the user-supplied vector.
		nv[n_other_replicas++] = p->replicas[repl_ix];
	}

	cf_mutex_unlock(&p->lock);

	return n_other_replicas;
}

cf_node
as_partition_writable_node(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (p->n_replicas == 0) {
		// This partition is unavailable.
		cf_mutex_unlock(&p->lock);
		return (cf_node)0;
	}

	cf_node best_node = find_best_node(p, false);

	cf_mutex_unlock(&p->lock);

	return best_node;
}

// If this node is an eventual master, return the acting master, else return 0.
cf_node
as_partition_proxyee_redirect(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	cf_node node = (cf_node)0;

	if (g_config.self_node == p->replicas[0] &&
			g_config.self_node != p->working_master) {
		node = p->working_master;
	}

	cf_mutex_unlock(&p->lock);

	return node;
}

void
as_partition_get_replicas_master_str(cf_dyn_buf* db)
{
	size_t db_sz = db->used_sz;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_buf(db, (uint8_t*)ns->replica_maps[0].b64map,
				sizeof(ns->replica_maps[0].b64map));
		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}

void
as_partition_get_replicas_all_str(cf_dyn_buf* db, bool include_regime)
{
	size_t db_sz = db->used_sz;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		if (include_regime) {
			cf_dyn_buf_append_uint32(db, ns->rebalance_regime);
			cf_dyn_buf_append_char(db, ',');
		}

		uint32_t repl_factor = ns->replication_factor;

		// If we haven't rebalanced yet, report 1 column with no ownership.
		if (repl_factor == 0) {
			repl_factor = 1;
		}

		cf_dyn_buf_append_uint32(db, repl_factor);

		for (uint32_t repl_ix = 0; repl_ix < repl_factor; repl_ix++) {
			cf_dyn_buf_append_char(db, ',');
			cf_dyn_buf_append_buf(db,
					(uint8_t*)&ns->replica_maps[repl_ix].b64map,
					sizeof(ns->replica_maps[repl_ix].b64map));
		}

		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}

void
as_partition_get_replica_stats(as_namespace* ns, repl_stats* p_stats)
{
	memset(p_stats, 0, sizeof(repl_stats));

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition* p = &ns->partitions[pid];

		cf_mutex_lock(&p->lock);

		if (g_config.self_node == p->working_master) {
			accumulate_replica_stats(p,
					&p_stats->n_master_objects,
					&p_stats->n_master_tombstones);
		}
		else if (find_self_in_replicas(p) >= 0) { // -1 if not
			accumulate_replica_stats(p,
					&p_stats->n_prole_objects,
					&p_stats->n_prole_tombstones);
		}
		else {
			accumulate_replica_stats(p,
					&p_stats->n_non_replica_objects,
					&p_stats->n_non_replica_tombstones);
		}

		cf_mutex_unlock(&p->lock);
	}
}

// TODO - what if partition is unavailable?
void
as_partition_reserve(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	partition_reserve_lockfree(p, ns, rsv);

	cf_mutex_unlock(&p->lock);
}

int
as_partition_reserve_replica(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	if (! is_self_replica(p)) {
		cf_mutex_unlock(&p->lock);
		return AS_ERR_CLUSTER_KEY_MISMATCH;
	}

	partition_reserve_lockfree(p, ns, rsv);

	cf_mutex_unlock(&p->lock);

	return AS_OK;
}

// Returns:
//  0 - reserved - node parameter returns self node
// -1 - not reserved - node parameter returns other "better" node
// -2 - not reserved - node parameter not filled - partition is unavailable
int
as_partition_reserve_write(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	// If this partition is frozen, return.
	if (p->n_replicas == 0) {
		if (node) {
			*node = (cf_node)0;
		}

		cf_mutex_unlock(&p->lock);
		return -2;
	}

	cf_node best_node = find_best_node(p, false);

	if (node) {
		*node = best_node;
	}

	// If this node is not the appropriate one, return.
	if (best_node != g_config.self_node) {
		cf_mutex_unlock(&p->lock);
		return -1;
	}

	partition_reserve_lockfree(p, ns, rsv);

	cf_mutex_unlock(&p->lock);

	return 0;
}

// Returns:
//  0 - reserved - node parameter returns self node
// -1 - not reserved - node parameter returns other "better" node
// -2 - not reserved - node parameter not filled - partition is unavailable
int
as_partition_reserve_read_tr(as_namespace* ns, uint32_t pid, as_transaction* tr,
		cf_node* node)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	// Handle unavailable partition.
	if (p->n_replicas == 0) {
		int result = partition_reserve_unavailable(ns, p, tr, node);

		if (result == 0) {
			partition_reserve_lockfree(p, ns, &tr->rsv);
		}

		cf_mutex_unlock(&p->lock);
		return result;
	}

	cf_node best_node = find_best_node(p,
			! partition_reserve_promote(ns, p, tr));

	if (node) {
		*node = best_node;
	}

	// If this node is not the appropriate one, return.
	if (best_node != g_config.self_node) {
		cf_mutex_unlock(&p->lock);
		return -1;
	}

	partition_reserve_lockfree(p, ns, &tr->rsv);

	cf_mutex_unlock(&p->lock);

	return 0;
}

// Reserves all query-able partitions.
// Returns the number of partitions reserved.
int
as_partition_prereserve_query(as_namespace* ns, bool can_partition_query[],
		as_partition_reservation rsv[])
{
	int reserved = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		if (as_partition_reserve_query(ns, pid, &rsv[pid])) {
			can_partition_query[pid] = false;
		}
		else {
			can_partition_query[pid] = true;
			reserved++;
		}
	}

	return reserved;
}

// Reserve a partition for query.
// Return value 0 means the reservation was taken, -1 means not.
int
as_partition_reserve_query(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	return as_partition_reserve_write(ns, pid, rsv, NULL);
}

// Obtain a partition reservation for XDR reads. Succeeds, if we are sync or
// zombie for the partition.
// TODO - what if partition is unavailable?
int
as_partition_reserve_xdr_read(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	as_partition* p = &ns->partitions[pid];

	cf_mutex_lock(&p->lock);

	int res = -1;

	if (as_partition_version_has_data(&p->version)) {
		partition_reserve_lockfree(p, ns, rsv);
		res = 0;
	}

	cf_mutex_unlock(&p->lock);

	return res;
}

void
as_partition_reservation_copy(as_partition_reservation* dst,
		as_partition_reservation* src)
{
	dst->ns = src->ns;
	dst->p = src->p;
	dst->tree = src->tree;
	dst->regime = src->regime;
	dst->n_dupl = src->n_dupl;

	if (dst->n_dupl != 0) {
		memcpy(dst->dupl_nodes, src->dupl_nodes, sizeof(cf_node) * dst->n_dupl);
	}
}

void
as_partition_release(as_partition_reservation* rsv)
{
	as_index_tree_release(rsv->tree);
}

void
as_partition_advance_tree_id(as_partition* p, const char* ns_name)
{
	uint32_t n_hanging;

	// Find first available tree-id past current one. Should be very next one.
	for (n_hanging = 0; n_hanging < MAX_NUM_TREE_IDS; n_hanging++) {
		p->tree_id = (p->tree_id + 1) & TREE_ID_MASK;

		uint64_t id_mask = 1UL << p->tree_id;

		if ((p->tree_ids_used & id_mask) == 0) {
			// Claim tree-id. Claim is relinquished when tree is destroyed.
			p->tree_ids_used |= id_mask;
			break;
		}
	}

	// If no available tree-ids, just stop. Should never happen.
	if (n_hanging == MAX_NUM_TREE_IDS) {
		cf_crash(AS_PARTITION, "{%s} pid %u has %u dropped trees hanging",
				ns_name, p->id, n_hanging);
	}

	// Too many hanging trees - ref-count leak? Offer chance to warm restart.
	if (n_hanging > MAX_NUM_TREE_IDS / 2) {
		cf_warning(AS_PARTITION, "{%s} pid %u has %u dropped trees hanging",
				ns_name, p->id, n_hanging);
	}
}

// Callback made when dropped as_index_tree is finally destroyed.
void
as_partition_tree_done(uint8_t id, void* udata)
{
	as_partition* p = (as_partition*)udata;

	cf_mutex_lock(&p->lock);

	// Relinquish tree-id.
	p->tree_ids_used &= ~(1UL << id);

	cf_mutex_unlock(&p->lock);
}

void
as_partition_getinfo_str(cf_dyn_buf* db)
{
	size_t db_sz = db->used_sz;

	cf_dyn_buf_append_string(db, "namespace:partition:state:n_replicas:replica:"
			"n_dupl:working_master:emigrates:lead_emigrates:immigrates:records:"
			"tombstones:regime:version:final_version;");

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			cf_mutex_lock(&p->lock);

			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, pid);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_char(db, partition_descriptor(p));
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->n_replicas);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, find_self_in_replicas(p));
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->n_dupl);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->working_master);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->pending_emigrations);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->pending_lead_emigrations);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->pending_immigrations);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, as_index_tree_size(p->tree));
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, p->n_tombstones);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->regime);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_string(db, VERSION_AS_STRING(&p->version));
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_string(db, VERSION_AS_STRING(&p->final_version));

			cf_dyn_buf_append_char(db, ';');

			cf_mutex_unlock(&p->lock);
		}
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db); // take back the final ';'
	}
}


//==========================================================
// Public API - client view replica maps.
//

void
client_replica_maps_create(as_namespace* ns)
{
	uint32_t size = sizeof(client_replica_map) * ns->cfg_replication_factor;

	ns->replica_maps = cf_malloc(size);
	memset(ns->replica_maps, 0, size);

	for (uint32_t repl_ix = 0; repl_ix < ns->cfg_replication_factor;
			repl_ix++) {
		client_replica_map* repl_map = &ns->replica_maps[repl_ix];

		cf_mutex_init(&repl_map->write_lock);

		cf_b64_encode((uint8_t*)repl_map->bitmap,
				(uint32_t)sizeof(repl_map->bitmap), (char*)repl_map->b64map);
	}
}

void
client_replica_maps_clear(as_namespace* ns)
{
	memset(ns->replica_maps, 0,
			sizeof(client_replica_map) * ns->cfg_replication_factor);

	for (uint32_t repl_ix = 0; repl_ix < ns->cfg_replication_factor;
			repl_ix++) {
		client_replica_map* repl_map = &ns->replica_maps[repl_ix];

		cf_b64_encode((uint8_t*)repl_map->bitmap,
				(uint32_t)sizeof(repl_map->bitmap), (char*)repl_map->b64map);
	}
}

bool
client_replica_maps_update(as_namespace* ns, uint32_t pid)
{
	uint32_t byte_i = pid >> 3;
	uint32_t byte_chunk = (byte_i / 3);
	uint32_t chunk_bitmap_offset = byte_chunk * 3;
	uint32_t chunk_b64map_offset = byte_chunk << 2;

	uint32_t bytes_from_end = CLIENT_BITMAP_BYTES - chunk_bitmap_offset;
	uint32_t input_size = bytes_from_end > 3 ? 3 : bytes_from_end;

	int replica = partition_get_replica_self_lockfree(ns, pid); // -1 if not
	uint8_t set_mask = 0x80 >> (pid & 0x7);
	bool changed = false;

	for (int repl_ix = 0; repl_ix < (int)ns->cfg_replication_factor;
			repl_ix++) {
		client_replica_map* repl_map = &ns->replica_maps[repl_ix];

		volatile uint8_t* mbyte = repl_map->bitmap + byte_i;
		bool owned = replica == repl_ix;
		bool is_set = (*mbyte & set_mask) != 0;
		bool needs_update = (owned && ! is_set) || (! owned && is_set);

		if (! needs_update) {
			continue;
		}

		volatile uint8_t* bitmap_chunk = repl_map->bitmap + chunk_bitmap_offset;
		volatile char* b64map_chunk = repl_map->b64map + chunk_b64map_offset;

		cf_mutex_lock(&repl_map->write_lock);

		*mbyte ^= set_mask;
		cf_b64_encode((uint8_t*)bitmap_chunk, input_size, (char*)b64map_chunk);

		cf_mutex_unlock(&repl_map->write_lock);

		changed = true;
	}

	return changed;
}

bool
client_replica_maps_is_partition_queryable(const as_namespace* ns, uint32_t pid)
{
	uint32_t byte_i = pid >> 3;

	const client_replica_map* repl_map = ns->replica_maps;
	const volatile uint8_t* mbyte = repl_map->bitmap + byte_i;

	uint8_t set_mask = 0x80 >> (pid & 0x7);

	return (*mbyte & set_mask) != 0;
}


//==========================================================
// Local helpers.
//

// Find best node to handle read/write. Called within partition lock.
cf_node
find_best_node(const as_partition* p, bool is_read)
{
	// Working master (final or acting) returns self, eventual master returns
	// acting master. Others don't have p->working_master set.
	if (p->working_master != (cf_node)0) {
		return p->working_master;
	}

	if (is_read && p->pending_immigrations == 0 &&
			find_self_in_replicas(p) > 0) {
		return g_config.self_node; // may read from prole that's got everything
	}

	return p->replicas[0]; // final master as a last resort
}

void
accumulate_replica_stats(const as_partition* p, uint64_t* p_n_objects,
		uint64_t* p_n_tombstones)
{
	int64_t n_tombstones = (int64_t)p->n_tombstones;
	int64_t n_objects = (int64_t)as_index_tree_size(p->tree) - n_tombstones;

	*p_n_objects += n_objects > 0 ? (uint64_t)n_objects : 0;
	*p_n_tombstones += (uint64_t)n_tombstones;
}

void
partition_reserve_lockfree(as_partition* p, as_namespace* ns,
		as_partition_reservation* rsv)
{
	as_index_tree_reserve(p->tree);

	rsv->ns = ns;
	rsv->p = p;
	rsv->tree = p->tree;
	rsv->regime = p->regime;
	rsv->n_dupl = p->n_dupl;

	if (rsv->n_dupl != 0) {
		memcpy(rsv->dupl_nodes, p->dupls, sizeof(cf_node) * rsv->n_dupl);
	}
}

char
partition_descriptor(const as_partition* p)
{
	if (find_self_in_replicas(p) >= 0) { // -1 if not
		return p->pending_immigrations == 0 ? 'S' : 'D';
	}

	if (as_partition_version_is_null(&p->version)) {
		return 'A';
	}

	return as_partition_version_has_data(&p->version) ? 'Z' : 'X';
}

int
partition_get_replica_self_lockfree(const as_namespace* ns, uint32_t pid)
{
	const as_partition* p = &ns->partitions[pid];

	if (g_config.self_node == p->working_master) {
		return 0;
	}

	int self_n = find_self_in_replicas(p); // -1 if not

	if (self_n > 0 && p->pending_immigrations == 0 &&
			// Check self_n < n_repl only because n_repl could be out-of-sync
			// with (less than) partition's replica list count.
			self_n < (int)ns->replication_factor) {
		return self_n;
	}

	return -1; // not a replica
}
