/*
 * exchange.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "dynbuf.h"
#include "node.h"
#include "vector.h"

/*
 * ----------------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------------
 */

/**
 * Used by exchange listeners during upgrades for compatibility purposes.
 *
 *  1 - 4.5.1 - for SMD upgrade.
 *  2 - 4.5.2 - for AER-6035 (AP uniform-balance + quiesce bug).
 *  3 - 4.5.3 - for new pickle format.
 *  4 - 4.7.0 - for AER-6128 (AP uniform-balance empty master selection bug).
 *  5 - 4.7.0.3 - for AER-6143 (SC quiesce with non-roster nodes bug).
 *  6 - 4.9.0 - for XDR compatibility with 5.0.0+.
 *  7 - 5.0.0 - for XDR compatibility with 4.9.0.
 *  8 - 5.2.0.17 & 5.3.0.8 - for (stripping) bin metadata and tombstones.
 *  9 - 5.4.0.3 - for (stripping) bin src-id or bin metadata and tombstones.
 * 10 - 5.5.0 - for converting bin cemeteries to regular tombstones.
 * 11 - 6.0.0 - for AER-6487 (revived nodes) & AER-6513 (storage end mark).
 */
#define AS_EXCHANGE_COMPATIBILITY_ID 11

/**
 * Number of quantum intervals in orphan state after which client transactions
 * will be blocked.
 */
#define AS_EXCHANGE_REVERT_ORPHAN_INTERVALS 5

/*
 * ----------------------------------------------------------------------------
 * Typedefs.
 * ----------------------------------------------------------------------------
 */

/**
 * Exchange event raised for every well-formed cluster change, after exchange
 * concludes successfully.
 */
typedef struct as_exchange_cluster_changed_event_s
{
	/**
	 * The new cluster key.
	 */
	uint64_t cluster_key;

	/**
	 * The new cluster size.
	 */
	uint32_t cluster_size;

	/**
	 * The new succession list.
	 */
	cf_node* succession;
} as_exchange_cluster_changed_event;

/**
 * Cluster change event call back function for cluster changed event listeners.
 */
typedef void
(*as_exchange_cluster_changed_cb)(
		const as_exchange_cluster_changed_event* event, void* udata);

/*
 * ----------------------------------------------------------------------------
 * Public API.
 * ----------------------------------------------------------------------------
 */
/**
 * Initialize exchange subsystem.
 */
void
as_exchange_init();

/**
 * Start exchange subsystem.
 */
void
as_exchange_start();

/**
 * Shut down exchange subsystem.
 */
void
as_exchange_shutdown();

/**
 * Register to receive cluster-changed events.
 * TODO - may replace with simple static list someday.
 */
void
as_exchange_register_listener(as_exchange_cluster_changed_cb cb, void* udata);

/**
 * Dump exchange state to log.
 */
void
as_exchange_dump(bool verbose);

/**
 * Member-access method.
 */
uint64_t
as_exchange_cluster_key();

/**
 * Member-access method.
 */
uint32_t
as_exchange_cluster_size();

/**
 * Copy over the committed succession list.
 * Ensure the input vector has enough capacity.
 */
void
as_exchange_succession(cf_vector* succession);

/**
 * Return the committed succession list as a string in a dyn-buf.
 */
void
as_exchange_info_get_succession(cf_dyn_buf* db);

/**
 * Member-access method.
 */
cf_node
as_exchange_principal();

/**
 * Used by exchange listeners during upgrades for compatibility purposes.
 */
uint32_t*
as_exchange_compatibility_ids(void);

/**
 * Used during upgrades for compatibility purposes.
 */
uint32_t
as_exchange_min_compatibility_id(void);

/**
 * Used during upgrades for compatibility purposes.
 */
uint32_t
as_exchange_max_compatibility_id(void);

/**
 * Output exchange cluster state for info.
 */
void as_exchange_cluster_info(cf_dyn_buf* db);

/**
 * Lock before setting or getting exchanged info from non-exchange thread.
 */
void
as_exchange_info_lock();

/**
 * Unlock after setting or getting exchanged info from non-exchange thread.
 */
void
as_exchange_info_unlock();
