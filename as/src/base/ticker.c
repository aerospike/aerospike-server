/*
 * ticker.c
 *
 * Copyright (C) 2016-2019 Aerospike, Inc.
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

#include "base/ticker.h"

#include <malloc.h>
#include <mcheck.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/param.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "hist.h"
#include "hist_track.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/secondary_index.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/thr_sindex.h"
#include "base/thr_tsvc.h"
#include "fabric/clustering.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/partition.h"
#include "fabric/skew_monitor.h"
#include "storage/storage.h"
#include "transaction/proxy.h"
#include "transaction/rw_request_hash.h"


//==========================================================
// Forward declarations.
//

extern bool g_shutdown_started;

void* run_ticker(void* arg);
void log_ticker_frame(uint64_t delta_time);

void log_line_clock();
void log_line_system_memory();
void log_line_in_progress();
void log_line_fds();
void log_line_heartbeat();
void log_fabric_rate(uint64_t delta_time);
void log_line_early_fail();
void log_line_batch_index();

void log_line_objects(as_namespace* ns, uint64_t n_objects,
		repl_stats* mp);
void log_line_tombstones(as_namespace* ns, uint64_t n_tombstones,
		repl_stats* mp);
void log_line_appeals(as_namespace* ns);
void log_line_migrations(as_namespace* ns);
void log_line_memory_usage(as_namespace* ns, size_t total_mem, size_t index_mem,
		size_t sindex_mem, size_t data_mem);
void log_line_persistent_index_usage(as_namespace* ns, size_t used_size);
void log_line_device_usage(as_namespace* ns);

void log_line_client(as_namespace* ns);
void log_line_xdr_client(as_namespace* ns);
void log_line_from_proxy(as_namespace* ns);
void log_line_xdr_from_proxy(as_namespace* ns);
void log_line_batch_sub(as_namespace* ns);
void log_line_from_proxy_batch_sub(as_namespace* ns);
void log_line_scan(as_namespace* ns);
void log_line_query(as_namespace* ns);
void log_line_udf_sub(as_namespace* ns);
void log_line_retransmits(as_namespace* ns);
void log_line_re_repl(as_namespace* ns);
void log_line_special_errors(as_namespace* ns);

void dump_global_histograms();
void dump_namespace_histograms(as_namespace* ns);


//==========================================================
// Public API.
//

void
as_ticker_start()
{
	cf_thread_create_detached(run_ticker, NULL);
}


//==========================================================
// Local helpers.
//

void*
run_ticker(void* arg)
{
	uint64_t last_time = cf_getns();

	while (true) {
		sleep(1); // wake up every second to check

		uint64_t curr_time = cf_getns();
		uint64_t delta_time = curr_time - last_time;

		if (delta_time < (uint64_t)g_config.ticker_interval * 1000000000) {
			continue;
		}

		last_time = curr_time;

		// Reduce likelihood of ticker frames showing after shutdown signal.
		if (g_shutdown_started) {
			break;
		}

		log_ticker_frame(delta_time);
	}

	return NULL;
}

void
log_ticker_frame(uint64_t delta_time)
{
	cf_info(AS_INFO, "NODE-ID %lx CLUSTER-SIZE %u",
			g_config.self_node,
			as_exchange_cluster_size()
			);

	log_line_clock();
	log_line_system_memory();
	log_line_in_progress();
	log_line_fds();
	log_line_heartbeat();
	log_fabric_rate(delta_time);
	log_line_early_fail();
	log_line_batch_index();

	dump_global_histograms();

	size_t total_ns_memory_inuse = 0;

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		uint64_t n_objects = ns->n_objects;
		uint64_t n_tombstones = ns->n_tombstones;

		size_t index_used = (n_objects + n_tombstones) * sizeof(as_index);

		size_t index_mem = as_namespace_index_persisted(ns) ? 0 : index_used;
		size_t sindex_mem = ns->n_bytes_sindex_memory;
		size_t data_mem = ns->n_bytes_memory;
		size_t total_mem = index_mem + sindex_mem + data_mem;

		total_ns_memory_inuse += total_mem;

		repl_stats mp;
		as_partition_get_replica_stats(ns, &mp);

		log_line_objects(ns, n_objects, &mp);
		log_line_tombstones(ns, n_tombstones, &mp);
		log_line_appeals(ns);
		log_line_migrations(ns);
		log_line_memory_usage(ns, total_mem, index_mem, sindex_mem, data_mem);
		log_line_persistent_index_usage(ns, index_used);
		log_line_device_usage(ns);

		log_line_client(ns);
		log_line_xdr_client(ns);
		log_line_from_proxy(ns);
		log_line_xdr_from_proxy(ns);
		log_line_batch_sub(ns);
		log_line_from_proxy_batch_sub(ns);
		log_line_scan(ns);
		log_line_query(ns);
		log_line_udf_sub(ns);
		log_line_retransmits(ns);
		log_line_re_repl(ns);
		log_line_special_errors(ns);

		dump_namespace_histograms(ns);
	}

	if (g_config.fabric_dump_msgs) {
		as_fabric_msg_queue_dump();
	}

	cf_dump_ticker_cache();
}

void
log_line_clock()
{
	cf_dyn_buf_define_size(outliers_db, 17 * AS_CLUSTER_SZ);
	uint32_t num_outliers = as_skew_monitor_outliers_append(&outliers_db);

	if (num_outliers != 0) {
		cf_dyn_buf_append_char(&outliers_db, 0);

		cf_info(AS_INFO, "   cluster-clock: skew-ms %lu outliers (%s)",
				as_skew_monitor_skew(),
				outliers_db.buf
				);
	}
	else {
		cf_info(AS_INFO, "   cluster-clock: skew-ms %lu",
				as_skew_monitor_skew()
				);
	}

	cf_dyn_buf_free(&outliers_db);
}

void
log_line_system_memory()
{
	uint64_t free_mem;
	uint32_t free_pct;

	sys_mem_info(&free_mem, &free_pct);

	size_t allocated_kbytes;
	size_t active_kbytes;
	size_t mapped_kbytes;
	double efficiency_pct;

	cf_alloc_heap_stats(&allocated_kbytes, &active_kbytes, &mapped_kbytes,
			&efficiency_pct, NULL);

	cf_info(AS_INFO, "   system-memory: free-kbytes %lu free-pct %d heap-kbytes (%lu,%lu,%lu) heap-efficiency-pct %.1lf",
			free_mem / 1024,
			free_pct,
			allocated_kbytes, active_kbytes, mapped_kbytes,
			efficiency_pct
			);
}

void
log_line_in_progress()
{
	cf_info(AS_INFO, "   in-progress: tsvc-q %d info-q %d rw-hash %u proxy-hash %u tree-gc-q %d",
			as_tsvc_queue_get_size(),
			as_info_queue_get_size(),
			rw_request_hash_count(),
			as_proxy_hash_count(),
			as_index_tree_gc_queue_size()
			);
}

void
log_line_fds()
{
	uint64_t n_proto_fds_opened = g_stats.proto_connections_opened;
	uint64_t n_proto_fds_closed = g_stats.proto_connections_closed;
	uint64_t n_hb_fds_opened = g_stats.heartbeat_connections_opened;
	uint64_t n_hb_fds_closed = g_stats.heartbeat_connections_closed;
	uint64_t n_fabric_fds_opened = g_stats.fabric_connections_opened;
	uint64_t n_fabric_fds_closed = g_stats.fabric_connections_closed;

	uint64_t n_proto_fds_open = n_proto_fds_opened - n_proto_fds_closed;
	uint64_t n_hb_fds_open = n_hb_fds_opened - n_hb_fds_closed;
	uint64_t n_fabric_fds_open = n_fabric_fds_opened - n_fabric_fds_closed;

	cf_info(AS_INFO, "   fds: proto (%lu,%lu,%lu) heartbeat (%lu,%lu,%lu) fabric (%lu,%lu,%lu)",
			n_proto_fds_open, n_proto_fds_opened, n_proto_fds_closed,
			n_hb_fds_open, n_hb_fds_opened, n_hb_fds_closed,
			n_fabric_fds_open, n_fabric_fds_opened, n_fabric_fds_closed
			);
}

void
log_line_heartbeat()
{
	cf_info(AS_INFO, "   heartbeat-received: self %lu foreign %lu",
			g_stats.heartbeat_received_self, g_stats.heartbeat_received_foreign
			);
}

void
log_fabric_rate(uint64_t delta_time)
{
	fabric_rate rate = { { 0 } };

	as_fabric_rate_capture(&rate);

	uint64_t dt_sec = delta_time / 1000000000;

	if (dt_sec < 1) {
		dt_sec = 1;
	}

	g_stats.fabric_bulk_s_rate = rate.s_bytes[AS_FABRIC_CHANNEL_BULK] / dt_sec;
	g_stats.fabric_bulk_r_rate = rate.r_bytes[AS_FABRIC_CHANNEL_BULK] / dt_sec;
	g_stats.fabric_ctrl_s_rate = rate.s_bytes[AS_FABRIC_CHANNEL_CTRL] / dt_sec;
	g_stats.fabric_ctrl_r_rate = rate.r_bytes[AS_FABRIC_CHANNEL_CTRL] / dt_sec;
	g_stats.fabric_meta_s_rate = rate.s_bytes[AS_FABRIC_CHANNEL_META] / dt_sec;
	g_stats.fabric_meta_r_rate = rate.r_bytes[AS_FABRIC_CHANNEL_META] / dt_sec;
	g_stats.fabric_rw_s_rate = rate.s_bytes[AS_FABRIC_CHANNEL_RW] / dt_sec;
	g_stats.fabric_rw_r_rate = rate.r_bytes[AS_FABRIC_CHANNEL_RW] / dt_sec;

	cf_info(AS_INFO, "   fabric-bytes-per-second: bulk (%lu,%lu) ctrl (%lu,%lu) meta (%lu,%lu) rw (%lu,%lu)",
			g_stats.fabric_bulk_s_rate, g_stats.fabric_bulk_r_rate,
			g_stats.fabric_ctrl_s_rate, g_stats.fabric_ctrl_r_rate,
			g_stats.fabric_meta_s_rate, g_stats.fabric_meta_r_rate,
			g_stats.fabric_rw_s_rate, g_stats.fabric_rw_r_rate
			);
}

void
log_line_early_fail()
{
	uint64_t n_demarshal = g_stats.n_demarshal_error;
	uint64_t n_tsvc_client = g_stats.n_tsvc_client_error;
	uint64_t n_tsvc_from_proxy = g_stats.n_tsvc_from_proxy_error;
	uint64_t n_tsvc_batch_sub = g_stats.n_tsvc_batch_sub_error;
	uint64_t n_tsvc_from_proxy_batch_sub = g_stats.n_tsvc_from_proxy_batch_sub_error;
	uint64_t n_tsvc_udf_sub = g_stats.n_tsvc_udf_sub_error;

	if ((n_demarshal |
			n_tsvc_client |
			n_tsvc_from_proxy |
			n_tsvc_batch_sub |
			n_tsvc_from_proxy_batch_sub |
			n_tsvc_udf_sub) == 0) {
		return;
	}

	cf_info(AS_INFO, "   early-fail: demarshal %lu tsvc-client %lu tsvc-from-proxy %lu tsvc-batch-sub %lu tsvc-from-proxy-batch-sub %lu tsvc-udf-sub %lu",
			n_demarshal,
			n_tsvc_client,
			n_tsvc_from_proxy,
			n_tsvc_batch_sub,
			n_tsvc_from_proxy_batch_sub,
			n_tsvc_udf_sub
			);
}

void
log_line_batch_index()
{
	uint64_t n_complete = g_stats.batch_index_complete;
	uint64_t n_error = g_stats.batch_index_errors;
	uint64_t n_timeout = g_stats.batch_index_timeout;

	if ((n_complete | n_error | n_timeout) == 0) {
		return;
	}

	cf_info(AS_INFO, "   batch-index: batches (%lu,%lu,%lu)",
			n_complete, n_error, n_timeout
			);
}

void
log_line_objects(as_namespace* ns, uint64_t n_objects, repl_stats* mp)
{
	// TODO - show if all 0's ???
	cf_info(AS_INFO, "{%s} objects: all %lu master %lu prole %lu non-replica %lu",
			ns->name,
			n_objects,
			mp->n_master_objects,
			mp->n_prole_objects,
			mp->n_non_replica_objects
			);
}

void
log_line_tombstones(as_namespace* ns, uint64_t n_tombstones, repl_stats* mp)
{
	if ((n_tombstones |
			mp->n_master_tombstones |
			mp->n_prole_tombstones |
			mp->n_non_replica_tombstones) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} tombstones: all %lu master %lu prole %lu non-replica %lu",
			ns->name,
			n_tombstones,
			mp->n_master_tombstones,
			mp->n_prole_tombstones,
			mp->n_non_replica_tombstones
			);
}

void
log_line_appeals(as_namespace* ns)
{
	int64_t remaining_tx = (int64_t)ns->appeals_tx_remaining;
	int64_t active_tx = (int64_t)ns->appeals_tx_active;
	int64_t active_rx = (int64_t)ns->appeals_rx_active;

	if (remaining_tx > 0 || active_tx > 0 || active_rx > 0) {
		cf_info(AS_INFO, "{%s} appeals: remaining-tx %ld active (%ld,%ld)",
				ns->name,
				remaining_tx, active_tx, active_rx
				);
	}
}

void
log_line_migrations(as_namespace* ns)
{
	int64_t initial_tx = (int64_t)ns->migrate_tx_partitions_initial;
	int64_t initial_rx = (int64_t)ns->migrate_rx_partitions_initial;
	int64_t remaining_tx = (int64_t)ns->migrate_tx_partitions_remaining;
	int64_t remaining_rx = (int64_t)ns->migrate_rx_partitions_remaining;
	int64_t initial = initial_tx + initial_rx;
	int64_t remaining = remaining_tx + remaining_rx;

	if (initial > 0 && remaining > 0) {
		float complete_pct = (1 - ((float)remaining / (float)initial)) * 100;

		cf_info(AS_INFO, "{%s} migrations: remaining (%ld,%ld,%ld) active (%ld,%ld,%ld) complete-pct %0.2f",
				ns->name,
				remaining_tx, remaining_rx, ns->migrate_signals_remaining,
				ns->migrate_tx_partitions_active, ns->migrate_rx_partitions_active, ns->migrate_signals_active,
				complete_pct
				);
	}
	else {
		cf_info(AS_INFO, "{%s} migrations: complete", ns->name);
	}
}

void
log_line_memory_usage(as_namespace* ns, size_t total_mem, size_t index_mem,
		size_t sindex_mem, size_t data_mem)
{
	double mem_used_pct = (double)(total_mem * 100) / (double)ns->memory_size;

	if (ns->storage_data_in_memory) {
		cf_info(AS_INFO, "{%s} memory-usage: total-bytes %lu index-bytes %lu sindex-bytes %lu data-bytes %lu used-pct %.2lf",
				ns->name,
				total_mem,
				index_mem,
				sindex_mem,
				data_mem,
				mem_used_pct
				);
	}
	else {
		cf_info(AS_INFO, "{%s} memory-usage: total-bytes %lu index-bytes %lu sindex-bytes %lu used-pct %.2lf",
				ns->name,
				total_mem,
				index_mem,
				sindex_mem,
				mem_used_pct
				);
	}
}

void
log_line_persistent_index_usage(as_namespace* ns, size_t used_size)
{
	if (ns->xmem_type == CF_XMEM_TYPE_PMEM) {
		uint64_t used_pct = used_size * 100 / ns->mounts_size_limit;

		cf_info(AS_INFO, "{%s} index-pmem-usage: used-bytes %lu used-pct %lu",
				ns->name,
				used_size,
				used_pct
				);
	}
	else if (ns->xmem_type == CF_XMEM_TYPE_FLASH) {
		uint64_t used_pct = used_size * 100 / ns->mounts_size_limit;

		cf_info(AS_INFO, "{%s} index-flash-usage: used-bytes %lu used-pct %lu",
				ns->name,
				used_size,
				used_pct
				);
	}
}

void
log_line_device_usage(as_namespace* ns)
{
	if (ns->storage_type != AS_STORAGE_ENGINE_SSD) {
		return;
	}

	int available_pct;
	uint64_t inuse_disk_bytes;
	as_storage_stats(ns, &available_pct, &inuse_disk_bytes);

	if (ns->storage_data_in_memory) {
		cf_info(AS_INFO, "{%s} device-usage: used-bytes %lu avail-pct %d",
				ns->name,
				inuse_disk_bytes,
				available_pct
				);
	}
	else {
		uint32_t n_reads_from_cache = ns->n_reads_from_cache;
		uint32_t n_total_reads = ns->n_reads_from_device + n_reads_from_cache;

		cf_atomic32_set(&ns->n_reads_from_device, 0);
		cf_atomic32_set(&ns->n_reads_from_cache, 0);

		ns->cache_read_pct =
				(float)(100 * n_reads_from_cache) /
				(float)(n_total_reads == 0 ? 1 : n_total_reads);

		cf_info(AS_INFO, "{%s} device-usage: used-bytes %lu avail-pct %d cache-read-pct %.2f",
				ns->name,
				inuse_disk_bytes,
				available_pct,
				ns->cache_read_pct
				);
	}
}

void
log_line_client(as_namespace* ns)
{
	uint64_t n_tsvc_error = ns->n_client_tsvc_error;
	uint64_t n_tsvc_timeout = ns->n_client_tsvc_timeout;
	uint64_t n_proxy_complete = ns->n_client_proxy_complete;
	uint64_t n_proxy_error = ns->n_client_proxy_error;
	uint64_t n_proxy_timeout = ns->n_client_proxy_timeout;
	uint64_t n_read_success = ns->n_client_read_success;
	uint64_t n_read_error = ns->n_client_read_error;
	uint64_t n_read_timeout = ns->n_client_read_timeout;
	uint64_t n_read_not_found = ns->n_client_read_not_found;
	uint64_t n_write_success = ns->n_client_write_success;
	uint64_t n_write_error = ns->n_client_write_error;
	uint64_t n_write_timeout = ns->n_client_write_timeout;
	uint64_t n_delete_success = ns->n_client_delete_success;
	uint64_t n_delete_error = ns->n_client_delete_error;
	uint64_t n_delete_timeout = ns->n_client_delete_timeout;
	uint64_t n_delete_not_found = ns->n_client_delete_not_found;
	uint64_t n_udf_complete = ns->n_client_udf_complete;
	uint64_t n_udf_error = ns->n_client_udf_error;
	uint64_t n_udf_timeout = ns->n_client_udf_timeout;
	uint64_t n_lang_read_success = ns->n_client_lang_read_success;
	uint64_t n_lang_write_success = ns->n_client_lang_write_success;
	uint64_t n_lang_delete_success = ns->n_client_lang_delete_success;
	uint64_t n_lang_error = ns->n_client_lang_error;

	if ((n_tsvc_error | n_tsvc_timeout |
			n_proxy_complete | n_proxy_error | n_proxy_timeout |
			n_read_success | n_read_error | n_read_timeout | n_read_not_found |
			n_write_success | n_write_error | n_write_timeout |
			n_delete_success | n_delete_error | n_delete_timeout | n_delete_not_found |
			n_udf_complete | n_udf_error | n_udf_timeout |
			n_lang_read_success | n_lang_write_success | n_lang_delete_success | n_lang_error) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} client: tsvc (%lu,%lu) proxy (%lu,%lu,%lu) read (%lu,%lu,%lu,%lu) write (%lu,%lu,%lu) delete (%lu,%lu,%lu,%lu) udf (%lu,%lu,%lu) lang (%lu,%lu,%lu,%lu)",
			ns->name,
			n_tsvc_error, n_tsvc_timeout,
			n_proxy_complete, n_proxy_error, n_proxy_timeout,
			n_read_success, n_read_error, n_read_timeout, n_read_not_found,
			n_write_success, n_write_error, n_write_timeout,
			n_delete_success, n_delete_error, n_delete_timeout, n_delete_not_found,
			n_udf_complete, n_udf_error, n_udf_timeout,
			n_lang_read_success, n_lang_write_success, n_lang_delete_success, n_lang_error
			);
}

void
log_line_xdr_client(as_namespace* ns)
{
	uint64_t n_write_success = ns->n_xdr_client_write_success;
	uint64_t n_write_error = ns->n_xdr_client_write_error;
	uint64_t n_write_timeout = ns->n_xdr_client_write_timeout;
	uint64_t n_delete_success = ns->n_xdr_client_delete_success;
	uint64_t n_delete_error = ns->n_xdr_client_delete_error;
	uint64_t n_delete_timeout = ns->n_xdr_client_delete_timeout;
	uint64_t n_delete_not_found = ns->n_xdr_client_delete_not_found;

	if ((n_write_success | n_write_error | n_write_timeout |
			n_delete_success | n_delete_error | n_delete_timeout | n_delete_not_found) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} xdr-client: write (%lu,%lu,%lu) delete (%lu,%lu,%lu,%lu)",
			ns->name,
			n_write_success, n_write_error, n_write_timeout,
			n_delete_success, n_delete_error, n_delete_timeout, n_delete_not_found
			);
}

void
log_line_from_proxy(as_namespace* ns)
{
	uint64_t n_tsvc_error = ns->n_from_proxy_tsvc_error;
	uint64_t n_tsvc_timeout = ns->n_from_proxy_tsvc_timeout;
	uint64_t n_read_success = ns->n_from_proxy_read_success;
	uint64_t n_read_error = ns->n_from_proxy_read_error;
	uint64_t n_read_timeout = ns->n_from_proxy_read_timeout;
	uint64_t n_read_not_found = ns->n_from_proxy_read_not_found;
	uint64_t n_write_success = ns->n_from_proxy_write_success;
	uint64_t n_write_error = ns->n_from_proxy_write_error;
	uint64_t n_write_timeout = ns->n_from_proxy_write_timeout;
	uint64_t n_delete_success = ns->n_from_proxy_delete_success;
	uint64_t n_delete_error = ns->n_from_proxy_delete_error;
	uint64_t n_delete_timeout = ns->n_from_proxy_delete_timeout;
	uint64_t n_delete_not_found = ns->n_from_proxy_delete_not_found;
	uint64_t n_udf_complete = ns->n_from_proxy_udf_complete;
	uint64_t n_udf_error = ns->n_from_proxy_udf_error;
	uint64_t n_udf_timeout = ns->n_from_proxy_udf_timeout;
	uint64_t n_lang_read_success = ns->n_from_proxy_lang_read_success;
	uint64_t n_lang_write_success = ns->n_from_proxy_lang_write_success;
	uint64_t n_lang_delete_success = ns->n_from_proxy_lang_delete_success;
	uint64_t n_lang_error = ns->n_from_proxy_lang_error;

	if ((n_tsvc_error | n_tsvc_timeout |
			n_read_success | n_read_error | n_read_timeout | n_read_not_found |
			n_write_success | n_write_error | n_write_timeout |
			n_delete_success | n_delete_error | n_delete_timeout | n_delete_not_found |
			n_udf_complete | n_udf_error | n_udf_timeout |
			n_lang_read_success | n_lang_write_success | n_lang_delete_success | n_lang_error) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} from-proxy: tsvc (%lu,%lu) read (%lu,%lu,%lu,%lu) write (%lu,%lu,%lu) delete (%lu,%lu,%lu,%lu) udf (%lu,%lu,%lu) lang (%lu,%lu,%lu,%lu)",
			ns->name,
			n_tsvc_error, n_tsvc_timeout,
			n_read_success, n_read_error, n_read_timeout, n_read_not_found,
			n_write_success, n_write_error, n_write_timeout,
			n_delete_success, n_delete_error, n_delete_timeout, n_delete_not_found,
			n_udf_complete, n_udf_error, n_udf_timeout,
			n_lang_read_success, n_lang_write_success, n_lang_delete_success, n_lang_error
			);
}

void
log_line_xdr_from_proxy(as_namespace* ns)
{
	uint64_t n_write_success = ns->n_xdr_from_proxy_write_success;
	uint64_t n_write_error = ns->n_xdr_from_proxy_write_error;
	uint64_t n_write_timeout = ns->n_xdr_from_proxy_write_timeout;
	uint64_t n_delete_success = ns->n_xdr_from_proxy_delete_success;
	uint64_t n_delete_error = ns->n_xdr_from_proxy_delete_error;
	uint64_t n_delete_timeout = ns->n_xdr_from_proxy_delete_timeout;
	uint64_t n_delete_not_found = ns->n_xdr_from_proxy_delete_not_found;

	if ((n_write_success | n_write_error | n_write_timeout |
			n_delete_success | n_delete_error | n_delete_timeout | n_delete_not_found) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} xdr-from-proxy: write (%lu,%lu,%lu) delete (%lu,%lu,%lu,%lu)",
			ns->name,
			n_write_success, n_write_error, n_write_timeout,
			n_delete_success, n_delete_error, n_delete_timeout, n_delete_not_found
			);
}

void
log_line_batch_sub(as_namespace* ns)
{
	uint64_t n_tsvc_error = ns->n_batch_sub_tsvc_error;
	uint64_t n_tsvc_timeout = ns->n_batch_sub_tsvc_timeout;
	uint64_t n_proxy_complete = ns->n_batch_sub_proxy_complete;
	uint64_t n_proxy_error = ns->n_batch_sub_proxy_error;
	uint64_t n_proxy_timeout = ns->n_batch_sub_proxy_timeout;
	uint64_t n_read_success = ns->n_batch_sub_read_success;
	uint64_t n_read_error = ns->n_batch_sub_read_error;
	uint64_t n_read_timeout = ns->n_batch_sub_read_timeout;
	uint64_t n_read_not_found = ns->n_batch_sub_read_not_found;

	if ((n_tsvc_error | n_tsvc_timeout |
			n_proxy_complete | n_proxy_error | n_proxy_timeout |
			n_read_success | n_read_error | n_read_timeout | n_read_not_found) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} batch-sub: tsvc (%lu,%lu) proxy (%lu,%lu,%lu) read (%lu,%lu,%lu,%lu)",
			ns->name,
			n_tsvc_error, n_tsvc_timeout,
			n_proxy_complete, n_proxy_error, n_proxy_timeout,
			n_read_success, n_read_error, n_read_timeout, n_read_not_found
			);
}

void
log_line_from_proxy_batch_sub(as_namespace* ns)
{
	uint64_t n_tsvc_error = ns->n_from_proxy_batch_sub_tsvc_error;
	uint64_t n_tsvc_timeout = ns->n_from_proxy_batch_sub_tsvc_timeout;
	uint64_t n_read_success = ns->n_from_proxy_batch_sub_read_success;
	uint64_t n_read_error = ns->n_from_proxy_batch_sub_read_error;
	uint64_t n_read_timeout = ns->n_from_proxy_batch_sub_read_timeout;
	uint64_t n_read_not_found = ns->n_from_proxy_batch_sub_read_not_found;

	if ((n_tsvc_error | n_tsvc_timeout |
			n_read_success | n_read_error | n_read_timeout | n_read_not_found) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} from-proxy-batch-sub: tsvc (%lu,%lu) read (%lu,%lu,%lu,%lu)",
			ns->name,
			n_tsvc_error, n_tsvc_timeout,
			n_read_success, n_read_error, n_read_timeout, n_read_not_found
			);
}

void
log_line_scan(as_namespace* ns)
{
	uint64_t n_basic_complete = ns->n_scan_basic_complete;
	uint64_t n_basic_error = ns->n_scan_basic_error;
	uint64_t n_basic_abort = ns->n_scan_basic_abort;
	uint64_t n_aggr_complete = ns->n_scan_aggr_complete;
	uint64_t n_aggr_error = ns->n_scan_aggr_error;
	uint64_t n_aggr_abort = ns->n_scan_aggr_abort;
	uint64_t n_udf_bg_complete = ns->n_scan_udf_bg_complete;
	uint64_t n_udf_bg_error = ns->n_scan_udf_bg_error;
	uint64_t n_udf_bg_abort = ns->n_scan_udf_bg_abort;

	if ((n_basic_complete | n_basic_error | n_basic_abort |
			n_aggr_complete | n_aggr_error | n_aggr_abort |
			n_udf_bg_complete | n_udf_bg_error | n_udf_bg_abort) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} scan: basic (%lu,%lu,%lu) aggr (%lu,%lu,%lu) udf-bg (%lu,%lu,%lu)",
			ns->name,
			n_basic_complete, n_basic_error, n_basic_abort,
			n_aggr_complete, n_aggr_error, n_aggr_abort,
			n_udf_bg_complete, n_udf_bg_error, n_udf_bg_abort
			);
}

void
log_line_query(as_namespace* ns)
{
	uint64_t n_basic_success = ns->n_lookup_success;
	uint64_t n_basic_failure = ns->n_lookup_errs + ns->n_lookup_abort;
	uint64_t n_aggr_success = ns->n_agg_success;
	uint64_t n_aggr_failure = ns->n_agg_errs + ns->n_agg_abort;
	uint64_t n_udf_bg_success = ns->n_query_udf_bg_success;
	uint64_t n_udf_bg_failure = ns->n_query_udf_bg_failure;

	if ((n_basic_success | n_basic_failure |
			n_aggr_success | n_aggr_failure |
			n_udf_bg_success | n_udf_bg_failure) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} query: basic (%lu,%lu) aggr (%lu,%lu) udf-bg (%lu,%lu)",
			ns->name,
			n_basic_success, n_basic_failure,
			n_aggr_success, n_aggr_failure,
			n_udf_bg_success, n_udf_bg_failure
			);
}

void
log_line_udf_sub(as_namespace* ns)
{
	uint64_t n_tsvc_error = ns->n_udf_sub_tsvc_error;
	uint64_t n_tsvc_timeout = ns->n_udf_sub_tsvc_timeout;
	uint64_t n_udf_complete = ns->n_udf_sub_udf_complete;
	uint64_t n_udf_error = ns->n_udf_sub_udf_error;
	uint64_t n_udf_timeout = ns->n_udf_sub_udf_timeout;
	uint64_t n_lang_read_success = ns->n_udf_sub_lang_read_success;
	uint64_t n_lang_write_success = ns->n_udf_sub_lang_write_success;
	uint64_t n_lang_delete_success = ns->n_udf_sub_lang_delete_success;
	uint64_t n_lang_error = ns->n_udf_sub_lang_error;

	if ((n_tsvc_error | n_tsvc_timeout |
			n_udf_complete | n_udf_error | n_udf_timeout |
			n_lang_read_success | n_lang_write_success | n_lang_delete_success | n_lang_error) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} udf-sub: tsvc (%lu,%lu) udf (%lu,%lu,%lu) lang (%lu,%lu,%lu,%lu)",
			ns->name,
			n_tsvc_error, n_tsvc_timeout,
			n_udf_complete, n_udf_error, n_udf_timeout,
			n_lang_read_success, n_lang_write_success, n_lang_delete_success, n_lang_error
			);
}

void
log_line_retransmits(as_namespace* ns)
{
	uint64_t n_migrate_record_retransmits = ns->migrate_record_retransmits;
	uint64_t n_all_read_dup_res = ns->n_retransmit_all_read_dup_res;
	uint64_t n_all_write_dup_res = ns->n_retransmit_all_write_dup_res;
	uint64_t n_all_write_repl_write = ns->n_retransmit_all_write_repl_write;
	uint64_t n_all_delete_dup_res = ns->n_retransmit_all_delete_dup_res;
	uint64_t n_all_delete_repl_write = ns->n_retransmit_all_delete_repl_write;
	uint64_t n_all_udf_dup_res = ns->n_retransmit_all_udf_dup_res;
	uint64_t n_all_udf_repl_write = ns->n_retransmit_all_udf_repl_write;
	uint64_t n_all_batch_sub_dup_res = ns->n_retransmit_all_batch_sub_dup_res;
	uint64_t n_udf_sub_dup_res = ns->n_retransmit_udf_sub_dup_res;
	uint64_t n_udf_sub_repl_write = ns->n_retransmit_udf_sub_repl_write;

	if ((n_migrate_record_retransmits |
			n_all_read_dup_res |
			n_all_write_dup_res | n_all_write_repl_write |
			n_all_delete_dup_res | n_all_delete_repl_write |
			n_all_udf_dup_res | n_all_udf_repl_write |
			n_all_batch_sub_dup_res |
			n_udf_sub_dup_res | n_udf_sub_repl_write) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} retransmits: migration %lu all-read %lu all-write (%lu,%lu) all-delete (%lu,%lu) all-udf (%lu,%lu) all-batch-sub %lu udf-sub (%lu,%lu)",
			ns->name,
			n_migrate_record_retransmits,
			n_all_read_dup_res,
			n_all_write_dup_res, n_all_write_repl_write,
			n_all_delete_dup_res, n_all_delete_repl_write,
			n_all_udf_dup_res, n_all_udf_repl_write,
			n_all_batch_sub_dup_res,
			n_udf_sub_dup_res, n_udf_sub_repl_write
			);
}

void
log_line_re_repl(as_namespace* ns)
{
	uint64_t n_re_repl_success = ns->n_re_repl_success;
	uint64_t n_re_repl_error = ns->n_re_repl_error;
	uint64_t n_re_repl_timeout = ns->n_re_repl_timeout;

	if ((n_re_repl_success | n_re_repl_error | n_re_repl_timeout) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} re-repl: all-triggers (%lu,%lu,%lu)",
			ns->name,
			n_re_repl_success, n_re_repl_error, n_re_repl_timeout
			);
}

void
log_line_special_errors(as_namespace* ns)
{
	uint64_t n_fail_key_busy = ns->n_fail_key_busy;
	uint64_t n_fail_record_too_big = ns->n_fail_record_too_big;

	if ((n_fail_key_busy |
			n_fail_record_too_big) == 0) {
		return;
	}

	cf_info(AS_INFO, "{%s} special-errors: key-busy %lu record-too-big %lu",
			ns->name,
			n_fail_key_busy,
			n_fail_record_too_big
			);
}

void
dump_global_histograms()
{
	if (g_stats.batch_index_hist_active) {
		histogram_dump(g_stats.batch_index_hist);
	}

	if (g_config.info_hist_enabled) {
		histogram_dump(g_stats.info_hist);
	}

	if (g_config.svc_benchmarks_enabled) {
		histogram_dump(g_stats.svc_demarshal_hist);
		histogram_dump(g_stats.svc_queue_hist);
	}

	if (g_config.fabric_benchmarks_enabled) {
		histogram_dump(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_BULK]);
		histogram_dump(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_BULK]);
		histogram_dump(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_BULK]);
		histogram_dump(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_BULK]);
		histogram_dump(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_CTRL]);
		histogram_dump(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_CTRL]);
		histogram_dump(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_CTRL]);
		histogram_dump(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_CTRL]);
		histogram_dump(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_META]);
		histogram_dump(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_META]);
		histogram_dump(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_META]);
		histogram_dump(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_META]);
		histogram_dump(g_stats.fabric_send_init_hists[AS_FABRIC_CHANNEL_RW]);
		histogram_dump(g_stats.fabric_send_fragment_hists[AS_FABRIC_CHANNEL_RW]);
		histogram_dump(g_stats.fabric_recv_fragment_hists[AS_FABRIC_CHANNEL_RW]);
		histogram_dump(g_stats.fabric_recv_cb_hists[AS_FABRIC_CHANNEL_RW]);
	}

	as_query_histogram_dumpall();
}

void
dump_namespace_histograms(as_namespace* ns)
{
	if (ns->read_hist_active) {
		cf_hist_track_dump(ns->read_hist);
	}

	if (ns->read_benchmarks_enabled) {
		histogram_dump(ns->read_start_hist);
		histogram_dump(ns->read_restart_hist);
		histogram_dump(ns->read_dup_res_hist);
		histogram_dump(ns->read_repl_ping_hist);
		histogram_dump(ns->read_local_hist);
		histogram_dump(ns->read_response_hist);
	}

	if (ns->write_hist_active) {
		cf_hist_track_dump(ns->write_hist);
	}

	if (ns->write_benchmarks_enabled) {
		histogram_dump(ns->write_start_hist);
		histogram_dump(ns->write_restart_hist);
		histogram_dump(ns->write_dup_res_hist);
		histogram_dump(ns->write_master_hist);
		histogram_dump(ns->write_repl_write_hist);
		histogram_dump(ns->write_response_hist);
	}

	if (ns->udf_hist_active) {
		cf_hist_track_dump(ns->udf_hist);
	}

	if (ns->udf_benchmarks_enabled) {
		histogram_dump(ns->udf_start_hist);
		histogram_dump(ns->udf_restart_hist);
		histogram_dump(ns->udf_dup_res_hist);
		histogram_dump(ns->udf_master_hist);
		histogram_dump(ns->udf_repl_write_hist);
		histogram_dump(ns->udf_response_hist);
	}

	if (ns->query_hist_active) {
		cf_hist_track_dump(ns->query_hist);
	}

	if (ns->query_rec_count_hist_active) {
		histogram_dump(ns->query_rec_count_hist);
	}

	if (ns->proxy_hist_enabled) {
		histogram_dump(ns->proxy_hist);
	}

	if (ns->batch_sub_benchmarks_enabled) {
		histogram_dump(ns->batch_sub_start_hist);
		histogram_dump(ns->batch_sub_restart_hist);
		histogram_dump(ns->batch_sub_dup_res_hist);
		histogram_dump(ns->batch_sub_repl_ping_hist);
		histogram_dump(ns->batch_sub_read_local_hist);
		histogram_dump(ns->batch_sub_response_hist);
	}

	if (ns->udf_sub_benchmarks_enabled) {
		histogram_dump(ns->udf_sub_start_hist);
		histogram_dump(ns->udf_sub_restart_hist);
		histogram_dump(ns->udf_sub_dup_res_hist);
		histogram_dump(ns->udf_sub_master_hist);
		histogram_dump(ns->udf_sub_repl_write_hist);
		histogram_dump(ns->udf_sub_response_hist);
	}

	if (ns->re_repl_hist_active) {
		histogram_dump(ns->re_repl_hist);
	}

	if (ns->storage_benchmarks_enabled) {
		as_storage_ticker_stats(ns);
	}

	as_sindex_histogram_dumpall(ns);
}
