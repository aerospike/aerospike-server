/*
 * index_checkpoint.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>

//==========================================================
// Forward declarations.
//

struct as_info_cmd_args_s;
struct as_namespace_s;

//==========================================================
// Public API.
//
// Index-checkpoint surfaces invoked from shared code (thr_info.c, as.c). The
// feature is enterprise-only: these are implemented in index_checkpoint_ee.c
// and stubbed in index_checkpoint_ce.c, so no checkpoint logic compiles into a
// CE binary. The info commands are also gated 'ee_only' in the command table,
// so the CE stubs are never reached.
//

// 'checkpoint-save' / 'checkpoint-status' info-command handlers.
void as_index_checkpoint_save_cmd(struct as_info_cmd_args_s* args);
void as_index_checkpoint_status_cmd(struct as_info_cmd_args_s* args);

// Called from as.c AFTER cf_process_privsep(), alongside the other post-privsep directory
// checks. EE: if index-checkpoint-path is set, verify it is writable by, and owned by, the
// effective (service) uid - the identity-dependent checks, grouped in as.c with the work/lua/
// smd dir validations (the identity-independent checks run in cfg_post_process, which also runs
// after privsep - the split is co-location, not a privilege difference). CE: no-op.
void as_index_checkpoint_validate_path_writable(void);

// Called at the end of as.c's clean-shutdown sequence. EE: if a checkpoint was
// requested, log and park for a bounded time (serving the info/service listener until
// the orchestrator's SIGTERM, or until the save's 'timeout' elapses and the node exits
// on its own). CE: no-op (returns immediately).
void as_index_checkpoint_park_on_shutdown(void);

// True once 'checkpoint-save' has been triggered - the node is heading into
// (or already in) the (timeout-bounded) checkpoint park, where the info port stays up
// but only the two checkpoint commands ('checkpoint-status' and a re-issued, idempotent
// 'checkpoint-save') are safe to serve. thr_info.c gates the info/service listener on
// this. EE: reflects the trigger flag. CE: always false (no feature).
bool as_index_checkpoint_in_shutdown(void);

// True once the main thread has ENTERED the post-checkpoint park (after a clean
// shutdown). The SIGTERM/SIGINT handlers use it to distinguish "reap the parked node"
// (exit) from a second signal arriving during an ordinary in-flight shutdown (which must
// NOT exit early - that would abort storage shutdown before headers are TRUSTED). EE:
// reflects the park flag. CE: always false (no feature).
bool as_index_checkpoint_is_parked(void);

// True if a 'checkpoint-save' ran this shutdown and FAILED for one or more namespaces.
// The exit paths (as.c's normal exit and the SIGTERM reap in signal.c) OR this into the
// process exit status so a failed checkpoint reports non-zero - otherwise an orchestrator
// scripting on $? is told the checkpoint succeeded and replaces the pod, which then
// cold-starts with no warning. EE: scans per-namespace ckpt_state. CE: always false.
bool as_index_checkpoint_any_failed(void);

// Called by a SIGTERM/SIGINT handler that lost the shutdown CAS: records that a reap
// signal arrived, so if a checkpoint shutdown is still heading into its park (not yet
// parked), the park honors it and exits immediately instead of holding the full timeout.
// EE: sets the reap flag. CE: no-op (no feature).
void as_index_checkpoint_note_reap_signal(void);

// Re-derive ns->index_checkpoint_path (the single gate every checkpoint decision reads)
// from ns->skip_checkpoint and the global 'index-checkpoint-path'. Called at config
// post-process (boot) and whenever 'skip-checkpoint' is set dynamically, so the derivation
// has one home. EE: derives the path; CE: no-op (no checkpoint feature).
void as_index_checkpoint_apply_skip(struct as_namespace_s* ns);

// Called from as.c at go-live - AFTER as_namespaces_setup() hydrated the checkpoints and
// BEFORE the node joins the cluster or serves any transaction (as_fabric_start /
// as_service_start). EE: delete-on-consume - delete the on-disk checkpoint dir "<ns>" of
// every namespace that hydrated FROM it (ns->ckpt_hydrated), so a later restart can never
// re-adopt it over now-diverged live data (single-copy model); that delete is durable before
// any write can be taken, and is FATAL if it cannot complete. A folder we ignored or could
// not use never went live, so it is left on disk: for a shadowless namespace a real recovery
// fallback, re-decided on a later boot; for a durably-backed one only retained bytes, since
// storage init regenerates the backing's 'random' and the stamped id can never match again.
// Either way the next successful checkpoint-save removes it. CE: no-op (no feature).
void as_index_checkpoint_delete_on_startup(void);
