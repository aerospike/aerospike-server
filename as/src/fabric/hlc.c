/*
 * hlc.c
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include "fabric/hlc.h"

#include <math.h>
#include <sys/param.h> // For MAX() and MIN().

#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_atomic.h"

#include "fault.h"

#include "base/cfg.h"

/*
 * Overview
 * ========
 * Hybrid logical clock as described in
 * "Logical Physical Clocks and Consistent Snapshots in Globally Distributed
 * Databases" available at http://www.cse.buffalo.edu/tech-reports/2014-04.pdf.
 *
 * Relies on a global 64 bit variable that has the logical time.
 * The 48 MSBs include the physical component of the timestamp and the least
 * significant 16 bits include the logical component. 48 bits for milliseconds
 * since epoch gives us (8925 - years elapsed since epoch today) years before
 * wrap around.
 *
 * The notion of HLC is to bound the skew between the logical clock and phsycial
 * clock. This requires rejecting updates to the clock from nodes with large
 * clock skews. We DO NOT do that yet and print a warning instead. The current
 * envisioned usage is a global monotonically increasing timestamp. Should be
 * fixed if we are to use it as a surrogate for wall clock.
 *
 * Guarantees
 * ==========
 * 1. Monotonically increasing. (Wraps around after ~8900 years). Service
 * restarts might break the monotonicity, however the new clock will leapfrog
 * the hlc value before the restart eventually.
 * 2. as_hlc_timestamp_update call after every message receipt will ensure the
 * message (send hlc ts)  < (message receive hlc ts).
 * 3. A fixed local timestamp will eventually be marked as happened before a
 * remote message. This is an important requirement. For example, in paxos the
 * local cluster change timestamp should have happened before some incoming
 * heartbeat. The ordering system should not always return a
 * AS_HLC_ORDER_INDETERMINATE for a fixed local timestamp and a new message
 * received.
 *
 * Not guaranteed (requires hlc persistence across service restarts)
 * ==============
 * 1. On service restart the HLC clock will not start where it left off, however
 * it will eventually leapfrog the older value. Fixing this requires persistence
 * which is not implemented. eventually leapfrogging is alright for all current
 * requirements.
 * 2. If a as_hlc_msg_timestamp is persisted and compared with a current running
 * value, the result may not be correct.
 *
 *
 * Requirements
 * ============
 * Subsystems that reply on hlc should have their network messages timestamped
 * with hlc timestamps and should invoke the as_hlc_timestamp_update on receipt
 * of every message. This will ensure the hlc are in sync across the cluster and
 * (send hlc ts)  < (message receive hlc ts).
 */

/**
 * Global timestamp with current hlc value.
 */
static as_hlc_timestamp g_now;

/**
 * Previous value of the physical component.
 */
static cf_atomic64 g_prev_physical_component;

/**
 * Previous value of the wall clock, when the physical component changed.
 */
static cf_atomic64 g_prev_wall_clock;

/*
 * ----------------------------------------------------------------------------
 * Globals.
 * ----------------------------------------------------------------------------
 */
/**
 * Mask for the physical component of a hlc timestamp.
 */
#define PHYSICAL_TS_MASK 0xffffffffffff0000

/**
 * Mask for logical component of a hls timestamp.
 */
#define LOGICAL_TS_MASK 0x000000000000ffff

/**
 * Inform when HLC jumps by more than this amount.
 */
#define HLC_JUMP_WARN 5000

/**
 * Logging macros.
 */
#define CRASH(format, ...) cf_crash(AS_HLC, format, ##__VA_ARGS__)
#define WARNING(format, ...) cf_warning(AS_HLC, format, ##__VA_ARGS__)
#define INFO(format, ...) cf_info(AS_HLC, format, ##__VA_ARGS__)
#define DEBUG(format, ...) cf_debug(AS_HLC, format, ##__VA_ARGS__)
#define DETAIL(format, ...) cf_detail(AS_HLC, format, ##__VA_ARGS__)
#define ASSERT(expression, message, ...)				\
if (!(expression)) {WARNING(message, __VA_ARGS__);}

/*
 * ----------------------------------------------------------------------------
 * Forward declarations.
 * ----------------------------------------------------------------------------
 */
static cf_clock
hlc_wall_clock_get();
static as_hlc_timestamp
hlc_ts_get();
static bool
hlc_ts_set(as_hlc_timestamp old_value, as_hlc_timestamp new_value,
		cf_node source);
static cf_clock
hlc_physical_ts_get(as_hlc_timestamp hlc_ts);
static uint16_t
hlc_logical_ts_get(as_hlc_timestamp hlc_ts);
static void
hlc_physical_ts_set(as_hlc_timestamp* hlc_ts, cf_clock physical_ts);
static void
hlc_physical_ts_on_set(cf_clock physical_ts, cf_clock wall_clock_now);
static void
hlc_logical_ts_set(as_hlc_timestamp* hlc_ts, uint16_t logical_ts);
static void
hlc_logical_ts_incr(uint16_t* logical_ts, cf_clock* physical_ts,
		cf_clock wall_clock_now);

/*
 * ----------------------------------------------------------------------------
 * Public API.
 * ----------------------------------------------------------------------------
 */
/**
 * Initialize hybrid logical clock.
 */
void
as_hlc_init()
{
	g_now = 0;
	g_prev_physical_component = 0;
	g_prev_wall_clock = 0;
}

/**
 * Return the physical component of a hlc timstamp
 * @param hlc_ts the hybrid logical clock timestamp.
 */
cf_clock
as_hlc_physical_ts_get(as_hlc_timestamp hlc_ts)
{
	return hlc_physical_ts_get(hlc_ts);
}

/**
 * Return a hlc timestamp representing the hlc time "now". The notion is to make
 * the minimum increment to the hlc timestamp necessary.
 */
as_hlc_timestamp
as_hlc_timestamp_now()
{
	// Keep trying till an atomic operation succeeds. Looks like a tight loop
	// but even with reasonable contention should not take more then a few
	// iterations to succeed.
	while (true) {
		as_hlc_timestamp current_hlc_ts = hlc_ts_get();

		// Initialize the new physical and logical values to current values.
		cf_clock new_hlc_physical_ts = hlc_physical_ts_get(current_hlc_ts);
		uint16_t new_hlc_logical_ts = hlc_logical_ts_get(current_hlc_ts);

		cf_clock wall_clock_physical_ts = hlc_wall_clock_get();

		if (new_hlc_physical_ts >= wall_clock_physical_ts) {
			// The HLC physical component is greater than the physical wall
			// time. Advance the logical timestamp.
			hlc_logical_ts_incr(&new_hlc_logical_ts, &new_hlc_physical_ts,
					wall_clock_physical_ts);
		}
		else {
			// The wall clock is greater, use this as the physical component and
			// reset the logical timestamp.
			new_hlc_physical_ts = wall_clock_physical_ts;
			new_hlc_logical_ts = 0;
		}

		as_hlc_timestamp new_hlc_ts = 0;

		hlc_physical_ts_set(&new_hlc_ts, new_hlc_physical_ts);
		hlc_logical_ts_set(&new_hlc_ts, new_hlc_logical_ts);

		if (hlc_ts_set(current_hlc_ts, new_hlc_ts, g_config.self_node)) {
			hlc_physical_ts_on_set(new_hlc_physical_ts, wall_clock_physical_ts);
			DETAIL("changed HLC value from %" PRIu64 " to %" PRIu64,
					current_hlc_ts, new_hlc_ts);
			return new_hlc_ts;
		}
	}
}

/**
 * Update the HLC on receipt of a remote message. The notion is to adjust this
 * node's hlc to ensure the receive hlc ts > the send hlc ts.
 *
 * @param source for debugging and tracking only.
 * @param send_timestamp the hlc timestamp when this message was sent.
 * @param recv_timestamp (output) the message receive timestamp which will be
 * populated. Can be NULL in which case it will be ignored.
 */
void
as_hlc_timestamp_update(cf_node source, as_hlc_timestamp send_ts,
		as_hlc_msg_timestamp* msg_ts)
{
	cf_clock send_ts_physical_ts = hlc_physical_ts_get(send_ts);
	uint16_t send_ts_logical_ts = hlc_logical_ts_get(send_ts);

	// Keep trying till an atomic operation succeeds. Looks like a tight loop
	// but even with reasonable contention should not take more then a few
	// iterations to succeed.
	while (true) {
		as_hlc_timestamp current_hlc_ts = hlc_ts_get();

		cf_clock current_hlc_physical_ts = hlc_physical_ts_get(current_hlc_ts);
		uint16_t current_hlc_logical_ts = hlc_logical_ts_get(current_hlc_ts);

		cf_clock wall_clock_physical_ts = hlc_wall_clock_get();

		cf_clock new_hlc_physical_ts = MAX(
				MAX(current_hlc_physical_ts, send_ts_physical_ts),
				wall_clock_physical_ts);
		uint16_t new_hlc_logical_ts = 0;

		if (new_hlc_physical_ts == current_hlc_physical_ts
				&& new_hlc_physical_ts == send_ts_physical_ts) {
			// There is no change in the physical components of peer and local
			// hlc clocks. Set logical component to max of the two values and
			// increment.
			new_hlc_logical_ts = MAX(current_hlc_logical_ts,
					send_ts_logical_ts);
			hlc_logical_ts_incr(&new_hlc_logical_ts, &new_hlc_physical_ts,
					wall_clock_physical_ts);
		}
		else if (new_hlc_physical_ts == current_hlc_physical_ts) {
			// The physical component of the send timestamp is smaller than our
			// current physical component. We just need to increment the logical
			// component.
			new_hlc_logical_ts = current_hlc_ts;
			hlc_logical_ts_incr(&new_hlc_logical_ts, &new_hlc_physical_ts,
					wall_clock_physical_ts);
		}
		else if (new_hlc_physical_ts == send_ts_physical_ts) {
			// Current physical component is lesser than the incoming physical
			// component. We need to ensure that the updated logical component
			// is greater than the send logical component.
			new_hlc_logical_ts = send_ts_logical_ts;
			hlc_logical_ts_incr(&new_hlc_logical_ts, &new_hlc_physical_ts,
					wall_clock_physical_ts);
		}
		else {
			// Our physical clock is greater than current physical component and
			// the send physical component. We can reset the logical clock to
			// zero and still maintain the send and receive ordering.
			new_hlc_logical_ts = 0;
		}

		as_hlc_timestamp new_hlc_ts = 0;

		hlc_physical_ts_set(&new_hlc_ts, new_hlc_physical_ts);
		hlc_logical_ts_set(&new_hlc_ts, new_hlc_logical_ts);

		if (hlc_ts_set(current_hlc_ts, new_hlc_ts, source)) {
			hlc_physical_ts_on_set(new_hlc_physical_ts, wall_clock_physical_ts);
			DETAIL("message received from node %" PRIx64 " with HLC %" PRIu64 " - changed HLC value from %" PRIu64 " to %" PRIu64,
					source, send_ts, current_hlc_ts, new_hlc_ts);
			if (msg_ts) {
				msg_ts->send_ts = send_ts;
				msg_ts->recv_ts = new_hlc_ts;
			}
			return;
		}
	}
}

/**
 * Return the difference in milliseconds between two hlc timestamps. Note this
 * difference may be greater than or equal to (but never less than)
 * the physical wall call difference, because HLC can have non linear jumps,
 * whenever the clock is adjusted. The difference should be used as an estimate
 * rather than an absolute difference.
 * For e.g. use the difference to check that the real time difference is most
 * some number of milliseconds. However do not use this for interval statistics
 * or to check if the difference in time is at least some number of
 * milliseconds.
 *
 * @param ts1 the first timestamp.
 * @param ts2 the seconds timestamp.
 * @return ts1 - ts2 in milliseconds. if ts1 < ts2 the result is negative,
 * else it is positive or zero.
 */
int64_t
as_hlc_timestamp_diff_ms(as_hlc_timestamp ts1, as_hlc_timestamp ts2)
{
	int64_t diff = 0;
	if (ts1 >= ts2) {
		diff = hlc_physical_ts_get(ts1) - hlc_physical_ts_get(ts2);
	}
	else {
		diff = -(hlc_physical_ts_get(ts2) - hlc_physical_ts_get(ts1));
	}

	return diff;
}

/**
 * Orders a local timestamp and remote message send timestamp.
 *
 * @param local_ts the local timestamp.
 * @param msg_ts message receive timestamp containing the remote send and the
 * local receive timestamp.
 * @return the order between the local and the message timestamp.
 */
as_hlc_timestamp_order
as_hlc_send_timestamp_order(as_hlc_timestamp local_ts,
		as_hlc_msg_timestamp* msg_ts)
{
	if (local_ts > msg_ts->recv_ts) {
		// The local event happened after the local message received timestamp
		// and therefore after the remote send as well.
		return AS_HLC_HAPPENS_AFTER;
	}

	// Compute the unceratinty window around the local receive timestamp.
	uint64_t offset = abs(msg_ts->send_ts - msg_ts->recv_ts);

	if (local_ts > (msg_ts->recv_ts - offset)) {
		// Local timestamp is in the uncertainty window. We cannot tell the
		// order.
		return AS_HLC_ORDER_INDETERMINATE;
	}

	cf_clock local_physical_ts = hlc_physical_ts_get(local_ts);
	cf_clock recv_physical_ts = hlc_physical_ts_get(msg_ts->recv_ts);

	if ((recv_physical_ts - local_physical_ts)
			< g_config.fabric_latency_max_ms) {
		// Consider the max network delay worth of time to also be part of the
		// uncertainty window.
		return AS_HLC_ORDER_INDETERMINATE;
	}

	return AS_HLC_HAPPENS_BEFORE;
}

/**
 * Orders two timestamp generated by the same node / process.
 *
 * @param ts1 the first timestamp.
 * @param ts2 the second timestamp.
 * @return AS_HLC_HAPPENS_BEFORE if ts1 happens before ts2 else
 * AS_HLC_HAPPENS_AFTER if ts1 happens after ts2  else
 * AS_HLC_ORDER_INDETERMINATE.
 */
as_hlc_timestamp_order
as_hlc_timestamp_order_get(as_hlc_timestamp ts1, as_hlc_timestamp ts2)
{
	if (ts1 < ts2) {
		return AS_HLC_HAPPENS_BEFORE;
	}
	else if (ts1 > ts2) {
		return AS_HLC_HAPPENS_AFTER;
	}

	return AS_HLC_ORDER_INDETERMINATE;
}

/**
 * Subtract milliseconds worth of time from the timestamp.
 * @param timestamp the input timestamp.
 * @param ms the number of milliseconds to subtract.
 */
as_hlc_timestamp
as_hlc_timestamp_subtract_ms(as_hlc_timestamp timestamp, int ms)
{
	cf_clock physical_ts = hlc_physical_ts_get(timestamp);
	uint16_t logical_ts = hlc_logical_ts_get(timestamp);
	physical_ts -= ms;
	as_hlc_timestamp new_hlc_ts = 0;

	hlc_physical_ts_set(&new_hlc_ts, physical_ts);
	hlc_logical_ts_set(&new_hlc_ts, logical_ts);
	return new_hlc_ts;
}

/**
 * Dump some debugging information to the logs.
 */
void
as_hlc_dump(bool verbose)
{
	as_hlc_timestamp now = as_hlc_timestamp_now();
	cf_clock current_hlc_physical_ts = hlc_physical_ts_get(now);
	uint16_t current_hlc_logical_ts = hlc_logical_ts_get(now);

	INFO("HLC Ts:%" PRIu64 " HLC Physical Ts:%" PRIu64 " HLC Logical Ts:%d Wall Clock:%" PRIu64,
			now, current_hlc_physical_ts, current_hlc_logical_ts,
			hlc_wall_clock_get());
}

/*
 * ----------------------------------------------------------------------------
 * Private functions.
 * ----------------------------------------------------------------------------
 */

/**
 * Return this node's wall clock.
 */
static cf_clock
hlc_wall_clock_get()
{
	// Unix timestamps will be 48 bits for a reasonable future. We will use only
	// 48 bits.
	return cf_clock_getabsolute();
}

/**
 * Return the physical component of a hlc timstamp
 * @param hlc_ts the hybrid logical clock timestamp.
 */
static cf_clock
hlc_physical_ts_get(as_hlc_timestamp hlc_ts)
{
	return hlc_ts >> 16;
}

/**
 * Return the logical component of a hlc timstamp
 * @param hlc_ts the hybrid logical clock timestamp.
 */
static uint16_t
hlc_logical_ts_get(as_hlc_timestamp hlc_ts)
{
	return (uint16_t)(hlc_ts & LOGICAL_TS_MASK);
}

/**
 * Set the physical component of a hlc timestamp. 16 LSBs of the input physical
 * timestamp will be ignored.
 * @param hlc_ts the timestamp
 * @param physical_ts the physical timestamp whose value should be set into the
 * hls timestamp.
 */
static void
hlc_physical_ts_set(as_hlc_timestamp* hlc_ts, cf_clock physical_ts)
{
	*hlc_ts = (*hlc_ts & LOGICAL_TS_MASK) | (physical_ts << 16);
}

/**
 * Handle setting updating the physical component of the hlc timestamp.
 */
static void
hlc_physical_ts_on_set(cf_clock physical_ts, cf_clock wall_clock_now)
{
	if (g_prev_physical_component != physical_ts) {
		g_prev_physical_component = physical_ts;
		g_prev_wall_clock = wall_clock_now;
	}
}

/**
 * Increment the logical timestamp and deal with a wrap around by incrementing
 * the physical timestamp and ensure physical component moves at least at the
 * rate of the wall clock to ensure hlc can be used as a crude measure of time
 * intervals.
 */
static void
hlc_logical_ts_incr(uint16_t* logical_ts, cf_clock* physical_ts,
		cf_clock wall_clock_now)
{
	(*logical_ts)++;
	if (*logical_ts == 0) {
		(*physical_ts)++;
	}
	cf_clock physical_component_diff = *physical_ts - g_prev_physical_component;
	cf_clock wall_clock_diff =
			(wall_clock_now > g_prev_wall_clock) ?
					wall_clock_now - g_prev_wall_clock : 0;
	if (physical_component_diff < wall_clock_diff) {
		*physical_ts += wall_clock_diff - physical_component_diff;
	}
}

/**
 * Set the logical component of a hlc timestamp.
 * @param hlc_ts the timestamp
 * @param logical_ts the logical timestamp whose value should be set into the
 * hls timestamp.
 */
static void
hlc_logical_ts_set(as_hlc_timestamp* hlc_ts, uint16_t logical_ts)
{
	*hlc_ts = (*hlc_ts & PHYSICAL_TS_MASK) | (((uint64_t)logical_ts));
}

/**
 * Get current value for the global timestamp atomically.
 *
 * @param new_value the new value for the global timestamp.
 * @return true on successful set, false on failure to do an atomic set.
 */
static as_hlc_timestamp
hlc_ts_get()
{
	return ck_pr_load_64(&g_now);
}

/**
 * Set a new value for the global timestamp atomically.
 *
 * @param old_value the old value.
 * @param new_value the new value for the global timestamp.
 * @param source the source node that caused this jump update in HLC, self if we
 * are advancing the clock, peer node if advance is caused on message receipt.
 * @return true on successful set, false on failure to do an atomic set.
 */
static bool
hlc_ts_set(as_hlc_timestamp old_value, as_hlc_timestamp new_value,
		cf_node source)
{
	// Default to ck atomic check and set.
	cf_clock jump = hlc_physical_ts_get(new_value)
			- hlc_physical_ts_get(old_value);
	if (jump > HLC_JUMP_WARN && old_value > 0) {
		INFO("HLC jumped by %"PRIu64" ms cause:%"PRIx64" old:%"PRIu64" new:%"PRIu64, jump, source, old_value, new_value);
	}
	return ck_pr_cas_64(&g_now, old_value, new_value);
}
