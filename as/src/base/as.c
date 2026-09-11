/*
 * as.c
 *
 * Copyright (C) 2008-2023 Aerospike, Inc.
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

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <syscall.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"

#include "cf_thread.h"
#include "daemon.h"
#include "dns.h"
#include "fips.h"
#include "hardware.h"
#include "log.h"
#include "os.h"
#include "tls.h"
#include "trace.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/health.h"
#include "base/index.h"
#include "base/index_checkpoint.h"
#include "base/json_init.h"
#include "base/masking.h"
#include "base/mrt_monitor.h"
#include "base/nsup.h"
#include "base/security.h"
#include "base/service.h"
#include "base/smd.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/ticker.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/clustering.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "fabric/roster.h"
#include "fabric/service_list.h"
#include "fabric/skew_monitor.h"
#include "query/query_manager.h"
#include "sindex/sindex.h"
#include "sindex/sindex_manager.h"
#include "storage/storage.h"
#include "transaction/proxy.h"
#include "transaction/rw_request_hash.h"
#include "transaction/udf.h"

//==========================================================
// Typedefs & constants.
//

// Schema file hash - can be overridden at compile time via -DAS_SCHEMA_HASH="..."
#ifndef AS_SCHEMA_HASH
#define AS_SCHEMA_HASH "unknown"
#endif

// The AS_PREVIEW_FEAT_* bit flags for --preview live in cfg.h (single source, so
// the config layer can gate a preview feature by name). Each bit must be nonzero:
// as_preview_features_parse() uses 0 as its "invalid list" sentinel.

// getopt_long value for --preview. It has no short form, so use a sentinel
// above the printable-char range instead of a letter.
#define OPT_PREVIEW 1000

// Known preview features, keyed by name. This is the source the startup
// error's "valid features" list is generated from, so adding a feature here is
// enough to keep that diagnostic correct.
static const struct {
	const char* name;
	uint32_t flag;
} PREVIEW_FEATS[] = {
	{ "yaml-config", AS_PREVIEW_FEAT_YAML_CONFIG },
	{ "index-checkpoint", AS_PREVIEW_FEAT_INDEX_CHECKPOINT },
};

// The preview features enabled at startup (parsed from the command line). Stored
// so the config layer can gate a preview feature by name; see
// as_preview_feature_enabled().
static uint32_t g_enabled_preview_features = 0;

// Command line options for the Aerospike server.
static const struct option CMD_OPTS[] = {
	{ "help", no_argument, NULL, 'h' },
	{ "version", no_argument, NULL, 'v' },
	{ "config-file", required_argument, NULL, 'f' },
	{ "schema-file", required_argument, NULL, 's' },
	{ "foreground", no_argument, NULL, 'd' },
	{ "fgdaemon", no_argument, NULL, 'F' },
	{ "early-verbose", no_argument, NULL, 'e' },
	{ "cold-start", no_argument, NULL, 'c' },
	{ "instance", required_argument, NULL, 'n' },
	{ "preview", required_argument, NULL, OPT_PREVIEW },
	{ NULL, 0, NULL, 0 },
};

static const char HELP[] =
		"\n"
		"asd informative command-line options:\n"
		"\n"
		"--help"
		"\n"
		"Print this message and exit.\n"
		"\n"
		"--version"
		"\n"
		"Print edition and build version information and exit.\n"
		"\n"
		"asd runtime command-line options:\n"
		"\n"
		"--config-file <file>"
		"\n"
		"Specify the location of the Aerospike server config file. If this option is not\n"
		"specified, the default location /etc/aerospike/aerospike.conf is used.\n"
		"\n"
		"--schema-file <file>"
		"\n"
		"Specify the location of the Aerospike server schema file. If this option is not\n"
		"specified, the default location /opt/aerospike/schema/aerospike_config_schema.json is used.\n"
		"\n"
		"--foreground"
		"\n"
		"Specify that Aerospike not be daemonized. This is useful for running Aerospike\n"
		"in gdb. Alternatively, add 'run-as-daemon false' in the service context of the\n"
		"Aerospike config file.\n"
		"\n"
		"--fgdaemon"
		"\n"
		"Specify that Aerospike is to be run as a \"new-style\" (foreground) daemon. This\n"
		"is useful for running Aerospike under systemd or Docker.\n"
		"\n"
		"--early-verbose"
		"\n"
		"Show verbose logging before config parsing.\n"
		"\n"
		"--cold-start"
		"\n"
		"(Enterprise edition only.) At startup, force the Aerospike server to read all\n"
		"records from storage devices to rebuild the index.\n"
		"\n"
		"--instance <0-15>"
		"\n"
		"(Enterprise edition only.) If running multiple instances of Aerospike on one\n"
		"machine (not recommended), each instance must be uniquely designated via this\n"
		"option.\n"
		"\n"
		"--preview <feature>[,<feature>...]"
		"\n"
		"Enable specific preview features by name, as a comma-separated list\n"
		"(e.g. yaml-config). An unknown feature name causes startup to fail and the\n"
		"valid feature names to be printed.\n"
		"\n";

static const char USAGE[] = "\n"
							"asd informative command-line options:\n"
							"[--help]\n"
							"[--version]\n"
							"\n"
							"asd runtime command-line options:\n"
							"[--config-file <file>] "
							"[--schema-file <file>] "
							"[--foreground] "
							"[--fgdaemon] "
							"[--early-verbose] "
							"[--cold-start] "
							"[--instance <0-15>] "
							"[--preview <feature,...>] \n";

static const char DEFAULT_CONFIG_FILE[] = "/etc/aerospike/aerospike.conf";
static const char DEFAULT_SCHEMA_FILE[] =
		"/opt/aerospike/schema/aerospike_config_schema.json";

static const char SMD_DIR_NAME[] = "/smd";

//==========================================================
// Globals.
//

// Not cf_mutex, which won't tolerate unlock if already unlocked.
pthread_mutex_t g_main_deadlock = PTHREAD_MUTEX_INITIALIZER;

// Set to 1 by the main thread once startup is complete. Read by the signal
// handlers and the checkpoint-save info command (which must refuse to touch
// g_main_deadlock before it exists), so accessed via as_*_uint32 atomics.
uint32_t g_startup_complete = 0;
// Claimed exactly once (0 -> 1, via as_cas_uint32) by whichever thread first
// initiates shutdown - a signal handler or the checkpoint-save info thread -
// so only that thread unlocks g_main_deadlock. A non-atomic bool would let two
// threads both pass the check and both unlock the about-to-be-destroyed mutex.
uint32_t g_shutdown_started = 0;
// Set (0 -> 1) if as_storage_shutdown() reported a failure, so the SIGTERM/SIGINT reap
// handlers - which cannot see as_run()'s stack-local storage_ok - fold it into the process
// exit status exactly as the normal exit does. Stored before the park's release-store of
// s_parked, so the reaper's acquire-load of s_parked already makes it visible.
uint32_t g_storage_shutdown_failed = 0;

//==========================================================
// Forward declarations.
//

// signal.c doesn't have header file.
extern void as_signal_setup(void);

static void preview_features_str(char* buf, size_t cap);
static void write_pidfile(char* pidfile);
static void validate_directory(const char* path, const char* log_tag);
static void validate_smd_directory(void);
static void verify_schema_file(const char* schema_file);

//==========================================================
// Public API - Aerospike server entry point.
//

int
as_run(int argc, char** argv)
{
	g_start_sec = cf_get_seconds();

	int opt;
	int opt_i;
	const char* config_file = DEFAULT_CONFIG_FILE;
	const char* schema_file = DEFAULT_SCHEMA_FILE;
	bool run_in_foreground = false;
	bool new_style_daemon = false;
	bool early_verbose = false;
	bool cold_start_cmd = false;
	uint32_t instance = 0;
	uint32_t preview_features = 0;

	// Parse command line options.
	while ((opt = getopt_long(argc, argv, "", CMD_OPTS, &opt_i)) != -1) {
		switch (opt) {
		case 'h':
			// printf() since we want stdout and don't want cf_log's prefix.
			printf("%s\n", HELP);
			return 0;
		case 'v':
			// printf() since we want stdout and don't want cf_log's prefix.
			printf("%s build %s\n", aerospike_build_type, aerospike_build_id);
			return 0;
		case 'f':
			config_file = cf_strdup(optarg);
			break;
		case 's':
			schema_file = cf_strdup(optarg);
			break;
		case 'F':
			// As a "new-style" daemon(*), asd runs in the foreground and
			// ignores the following configuration items:
			//  - user ('user')
			//	- group ('group')
			//  - PID file ('pidfile')
			//
			// If ignoring configuration items, or if the 'console' sink is not
			// specified, warnings will appear in stderr.
			//
			// (*) http://0pointer.de/public/systemd-man/daemon.html#New-Style%20Daemons
			run_in_foreground = true;
			new_style_daemon = true;
			break;
		case 'd':
			run_in_foreground = true;
			break;
		case 'e':
			early_verbose = true;
			break;
		case 'c':
			cold_start_cmd = true;
			break;
		case 'n': {
			// The instance id occupies a 4-bit field in every xmem key (0-15); a
			// larger value overflows into the key's magic byte and mis-routes every
			// segment. Parse strictly (cf_log isn't initialized yet): reject an empty
			// arg, non-numeric or trailing junk ("abc" must NOT read as 0), a negative
			// (strtoul silently wraps it), an out-of-range magnitude, or > 15.
			char* end = NULL;
			errno = 0;
			unsigned long v = strtoul(optarg, &end, 0);
			if (optarg[0] == '\0' || end == optarg || *end != '\0' ||
					errno != 0 || strchr(optarg, '-') != NULL || v > 15) {
				fprintf(stderr,
						"invalid --instance '%s'; valid range is 0-15\n%s\n",
						optarg, USAGE);
				return 1;
			}
			instance = (uint32_t)v;
			break;
		}
		case OPT_PREVIEW: {
			// A valid list always sets at least one bit, so 0 means failure
			// (unknown feature name or effectively empty list).
			uint32_t features = as_preview_features_parse(optarg);
			if (features == 0) {
				char valid[256];
				preview_features_str(valid, sizeof(valid));
				// fprintf() since cf_log isn't initialized yet.
				fprintf(stderr,
						"invalid --preview value; valid features: %s\n%s\n",
						valid, USAGE);
				return 1;
			}
			preview_features |= features;
			break;
		}
		default:
			// fprintf() since we don't want cf_log's prefix.
			fprintf(stderr, "%s\n", USAGE);
			return 1;
		}
	}

	// Initializations before config parsing.
	cf_log_init(early_verbose);
	cf_alloc_init();
	cf_trace_init();
	cf_thread_init();
	as_signal_setup();
	cf_fips_init();
	cf_tls_init();

	// Set all fields in the global runtime configuration instance. This parses
	// the configuration file, and creates as_namespace objects. (Return value
	// is a shortcut pointer to the global runtime configuration instance.)
	as_config* c = NULL;

	// Publish the enabled set before as_config_init() (which runs config
	// post-processing) so the config layer can gate a preview feature by name.
	as_preview_features_set(preview_features);

	if ((preview_features & AS_PREVIEW_FEAT_YAML_CONFIG) != 0) {
		// Verify that the schema file hasn't been modified since installation.
		verify_schema_file(schema_file);
		// The config is assumed to be in yaml format when yaml-config is enabled.
		c = as_config_init_yaml(config_file, schema_file);
	}
	else {
		c = as_config_init(config_file);
	}

	// Detect NUMA topology and, if requested, prepare for CPU and NUMA pinning.
	cf_topo_config(c->auto_pin, (cf_topo_numa_node_index)instance,
			&c->service.bind);

	// Perform privilege separation as necessary. If configured user & group
	// don't have root privileges, all resources created or reopened past this
	// point must be set up so that they are accessible without root privileges.
	// If not, the process will self-terminate with (hopefully!) a log message
	// indicating which resource is not set up properly.
	cf_process_privsep(c->uid, c->gid);

	//
	// All resources such as files, devices, and shared memory must be created
	// or reopened below this line! (The configuration file is the only thing
	// that must be opened above, in order to parse the user & group.)
	//==========================================================================

	// Activate log sinks. Up to this point, 'cf_' log output goes to stderr,
	// filtered according to early_verbose. After this point, 'cf_' log output
	// will appear in all log file sinks specified in configuration, with
	// specified filtering. If console sink is specified in configuration, 'cf_'
	// log output will continue going to stderr, but filtering will switch to
	// that specified in console sink configuration.
	cf_log_activate_sinks();

	// Daemonize asd if specified. After daemonization, output to stderr will no
	// longer appear in terminal. Instead, check /tmp/aerospike-console.<pid>
	// for console output.
	if (! run_in_foreground && c->run_as_daemon) {
		cf_process_daemonize();
	}

	// Log which build this is - should be the first line in the log file.
	cf_info(AS_AS, "<><><><><><><><><><>  %s build %s  <><><><><><><><><><>",
			aerospike_build_type, aerospike_build_id);
	cf_log_version(CF_INFO, AS_AS, "starting");

	// Includes echoing the configuration file to log.
	as_config_post_process(c, config_file);

	// If we allocated a non-default config file name, free it.
	if (config_file != DEFAULT_CONFIG_FILE) {
		cf_free((void*)config_file);
	}

	// If we allocated a non-default schema file name, free it.
	if (schema_file != DEFAULT_SCHEMA_FILE) {
		cf_free((void*)schema_file);
	}

	// Write the pid file, if specified.
	if (! new_style_daemon) {
		write_pidfile(c->pidfile);
	}
	else {
		if (c->pidfile != NULL) {
			cf_warning(AS_AS, "will not write PID file in new-style daemon mode");
		}
	}

	// Check that required directories are set up properly.
	validate_directory(c->work_directory, "work");
	validate_directory(c->mod_lua.user_path, "Lua user");
	validate_smd_directory();

	// The euid-dependent index-checkpoint-path checks (writable by / owned by the service
	// uid) run here, after privsep, with the other directory validations. (cfg_post_process
	// also runs after privsep - as the service uid, not root - so either site would see the
	// right euid; this is just the natural home next to validate_directory().)
	as_index_checkpoint_validate_path_writable();

	// Initialize subsystems. At this point we're allocating local resources,
	// starting worker threads, etc. (But no communication with other server
	// nodes or clients yet.)

	as_json_init(); // Jansson JSON API used by System Metadata
	as_index_tree_gc_init(); // thread to purge dropped index trees
	as_nsup_init(); // load previous evict-void-time(s)
	as_xdr_init(); // load persisted last-ship-time(s)
	as_roster_init(); // load roster-related SMD

	// Set up namespaces. Each namespace decides here whether it will do a warm
	// or cold start. Index arenas, set and bin name vmaps are initialized.
	// Index-checkpoint (EE): the boot verdict runs INSIDE here (drv_mem_find_stripes and the
	// CKPT_* fail-stops). It MUST stay ahead of as_storage_init() below - the durably-backed
	// gate peeks the backing's 'random', which storage init REGENERATES on open, so a reorder
	// would silently stop every device namespace from ever hydrating (no serve, no error) - and
	// ahead of the go-live delete further down, which would otherwise delete an un-decided
	// checkpoint. (Nothing serves until as_service_start(), well below either step.)
	as_namespaces_setup(cold_start_cmd, instance);

	// These load SMD involving sets/bins, needed during storage init/load.
	as_sindex_manager_init();
	as_truncate_init();

	// Initialize namespaces. Partition structures and index tree structures are
	// initialized.
	as_namespaces_init(cold_start_cmd, instance);

	// Relevant for enterprise edition only.
	as_mrt_monitor_init();

	// Initialize the storage system. For warm restarts, this includes fully
	// resuming persisted indexes.
	as_storage_init();
	// ... This could block for minutes ....................

	// For warm restarts, fully resume persisted sindexes.
	as_sindex_resume();
	// ... This could block for minutes ....................

	// Migrate memory to correct NUMA node (includes resumed index arenas).
	cf_topo_migrate_memory();

	// Drop capabilities that we kept only for initialization.
	cf_process_drop_startup_caps();

	// For cold starts, this does full drive scans. (Also populates
	// storage-engine memory & pmem namespaces' secondary indexes.)
	as_storage_load();
	// ... This could block for hours ......................

	// Populate storage-engine device namespaces' secondary indexes.
	as_sindex_load();
	// ... This could block for a while ....................

	// The defrag subsystem starts operating here. Wait for enough available
	// storage.
	as_storage_activate();
	// ... This could block for a while ....................

	cf_info(AS_AS, "initializing services...");

	cf_dns_init(); // DNS resolver
	as_security_init(); // security features
	as_service_init(); // server may process internal transactions
	as_admin_init(); // admin connection handling
	as_hb_init(); // inter-node heartbeat
	as_skew_monitor_init(); // clock skew monitor
	as_fabric_init(); // inter-node communications
	as_exchange_init(); // initialize the cluster exchange subsystem
	as_clustering_init(); // clustering-v5 start
	as_service_list_init(); // service list handling
	as_info_init(); // info transaction handling
	as_migrate_init(); // move data between nodes
	as_proxy_init(); // do work on behalf of others
	as_rw_init(); // read & write service
	as_query_manager_init(); // query transaction handling
	as_udf_init(); // user-defined functions
	as_batch_init(); // batch transaction handling

	// Delete every namespace's CONSUMED on-disk checkpoint NOW, durably - AFTER
	// as_namespaces_setup() hydrated from it and storage is up, but BEFORE the node joins the
	// cluster or serves any transaction (that begins in the *_start block just below:
	// as_fabric_start / as_service_start). Delete-on-consume: a folder we did not hydrate from
	// never went live, so it is left as a recovery fallback and the next checkpoint-save removes
	// it. Defrag may already be relocating records at this point, but it never drops them and
	// the node is not yet reachable, so nothing has diverged from the checkpoint. Once the node
	// can take writes it diverges and must never re-adopt a folder it consumed; deleting that
	// one here - as late as possible, but before any divergence - closes the silent-rollback
	// window (single-copy model). A boot that dies before this point re-hydrates the
	// still-present checkpoint, which is correct: nothing was served, so nothing diverged.
	as_index_checkpoint_delete_on_startup();

	// Start subsystems. At this point we may begin communicating with other
	// cluster nodes, and ultimately with clients.

	as_sindex_manager_start(); // sindex gc and set-index population threads
	cf_tls_start(); // starts tls certificate refresh thread
	as_security_start(); // starts security threads
	as_smd_start(); // enables receiving cluster state change events
	as_health_start(); // starts before fabric and hb to capture them
	as_fabric_start(); // may send & receive fabric messages
	as_xdr_start(); // XDR should start before it joins other nodes
	as_hb_start(); // start inter-node heartbeat
	as_exchange_start(); // start the cluster exchange subsystem
	as_clustering_start(); // clustering-v5 start
	as_nsup_start(); // may send evict-void-time(s) to other nodes
	as_admin_start(); // admin should start before service
	as_service_start(); // will now accept client and admin transactions
	as_ticker_start(); // only after everything else is started

	// Relevant for enterprise edition only.
	as_storage_start_tomb_raider();

	// Log a service-ready message.
	cf_info(AS_AS, "service ready: soon there will be cake!");

	//--------------------------------------------
	// Startup is done. This thread will now wait
	// quietly for a shutdown signal.
	//

	// Stop this thread from finishing. Intentionally deadlocking on a mutex is
	// a remarkably efficient way to do this.
	pthread_mutex_lock(&g_main_deadlock);
	as_store_uint32(&g_startup_complete, 1);
	pthread_mutex_lock(&g_main_deadlock);

	// When the service is running, you are here (deadlocked) - the signals that
	// stop the service (yes, these signals always occur in this thread) will
	// unlock the mutex, allowing us to continue.

	// A claimant (signal handler / info thread) already won the claim and unlocked
	// us; store is idempotent, kept for clarity.
	as_store_uint32(&g_shutdown_started, 1);
	pthread_mutex_unlock(&g_main_deadlock);
	// Do NOT pthread_mutex_destroy(&g_main_deadlock): the claimant may be an info
	// thread still returning from its cross-thread unlock, and destroying a mutex
	// referenced by another thread is undefined. The process _exit()s just below,
	// so destroying it bought nothing.

	//--------------------------------------------
	// Received a shutdown signal.
	//

	cf_info(AS_AS, "initiating clean shutdown ...");

	// If this node was not quiesced and storage shutdown takes very long (e.g.
	// flushing pmem index), best to get kicked out of the cluster quickly.
	as_hb_shutdown();

	// Block partition rebalance to prevent new (non-null) partition trees from
	// being swizzled in.
	as_exchange_shutdown();

	// Make sure committed SMD files are in sync with SMD callback activity.
	as_smd_shutdown();

	bool storage_ok = as_storage_shutdown(instance);

	if (! storage_ok) {
		// Publish for the reap handlers (signal.c) - see g_storage_shutdown_failed.
		as_store_uint32(&g_storage_shutdown_failed, 1);
		cf_warning(AS_AS, "failed clean shutdown");
	}
	else {
		cf_info(AS_AS, "finished clean shutdown - exiting");
	}

	// Index checkpoint (enterprise): if a 'checkpoint-save' was issued, the
	// index (and memory-ns data) was copied during as_storage_shutdown(). Park
	// here serving the info/service listener for 'checkpoint-status' until SIGTERM
	// reaps us (sig_handle_term -> _exit once g_shutdown_started) - even if storage
	// shutdown reported a failure, so an operator polling checkpoint-status sees
	// the per-namespace result instead of the process simply vanishing. Returns
	// immediately (and is a no-op in CE) when no checkpoint was requested.
	as_index_checkpoint_park_on_shutdown();

	// If shutdown was totally clean (all threads joined) we could just return,
	// but for now we exit to make sure all threads die. A FAILED checkpoint must exit
	// non-zero too: as_storage_shutdown returns true even when a per-namespace
	// checkpoint failed, so without this an orchestrator scripting on $? is told the
	// save succeeded and replaces the pod, which then cold-starts with no warning.
	bool clean_exit = storage_ok && ! as_index_checkpoint_any_failed();
#ifdef DOPROFILE
	exit(clean_exit ? 0 : 1); // exit() so profile build dumps gmon.out
#else
	_exit(clean_exit ? 0 : 1);
#endif

	return 0;
}

//==========================================================
// Public API.
//

bool
as_preview_feature_enabled(uint32_t flag)
{
	return (g_enabled_preview_features & flag) != 0;
}

void
as_preview_features_set(uint32_t features)
{
	g_enabled_preview_features = features;
}

//==========================================================
// Local helpers.
//

// Format the comma-separated list of all known preview feature names into
// buf (e.g. "yaml-config"), so the startup-error diagnostic is built from the
// single PREVIEW_FEATS source rather than a duplicated literal.
static void
preview_features_str(char* buf, size_t cap)
{
	if (cap != 0) {
		buf[0] = '\0'; // always a valid string, even if the table is empty
	}

	size_t off = 0;

	for (size_t i = 0; i < sizeof(PREVIEW_FEATS) / sizeof(PREVIEW_FEATS[0]); i++) {
		int n = snprintf(buf + off, cap - off, "%s%s", i == 0 ? "" : ", ",
				PREVIEW_FEATS[i].name);

		if (n < 0 || (size_t)n >= cap - off) {
			break; // truncated - won't happen for the small known set
		}

		off += (size_t)n;
	}
}

// Parse a comma-separated list of preview feature names into a bit mask.
// Returns 0 if any token is not a known feature or if the list is effectively
// empty; a valid list always sets at least one bit, so the caller treats 0 as a
// startup failure.
uint32_t
as_preview_features_parse(const char* arg)
{
	uint32_t features = 0;
	char* dup = cf_strdup(arg); // strtok_r mutates - don't touch optarg/argv
	char* save = NULL;

	for (char* tok = strtok_r(dup, ",", &save); tok != NULL;
			tok = strtok_r(NULL, ",", &save)) {
		// Trim leading & trailing spaces/tabs so "yaml-config, x" works.
		while (*tok == ' ' || *tok == '\t') {
			tok++;
		}

		char* end = tok + strlen(tok);

		while (end > tok && (end[-1] == ' ' || end[-1] == '\t')) {
			end--;
		}

		*end = '\0';

		if (*tok == '\0') {
			continue; // skip empty tokens, e.g. from a trailing comma
		}

		uint32_t flag = 0;

		for (size_t i = 0; i < sizeof(PREVIEW_FEATS) / sizeof(PREVIEW_FEATS[0]);
				i++) {
			if (strcmp(tok, PREVIEW_FEATS[i].name) == 0) {
				flag = PREVIEW_FEATS[i].flag;
				break;
			}
		}

		if (flag == 0) {
			cf_free(dup); // unknown feature name
			return 0;
		}

		features |= flag;
	}

	cf_free(dup);

	return features; // 0 here means an empty list - caller treats as failure
}

static void
write_pidfile(char* pidfile)
{
	if (pidfile == NULL) {
		// If there's no pid file specified in the config file, just move on.
		return;
	}

	// Note - the directory the pid file is in must already exist.

	remove(pidfile);

	int pid_fd = open(pidfile, O_CREAT | O_RDWR, cf_os_log_perms());

	if (pid_fd < 0) {
		cf_crash_nostack(AS_AS, "failed to open pid file %s: %s", pidfile,
				cf_strerror(errno));
	}

	char pidstr[16];
	sprintf(pidstr, "%u\n", (uint32_t)getpid());

	// If we can't access this resource, just log a warning and continue -
	// it is not critical to the process.
	if (write(pid_fd, pidstr, strlen(pidstr)) == -1) {
		cf_warning(AS_AS, "failed write to pid file %s: %s", pidfile,
				cf_strerror(errno));
	}

	close(pid_fd);
}

static void
validate_directory(const char* path, const char* log_tag)
{
	struct stat buf;

	if (stat(path, &buf) != 0) {
		cf_crash_nostack(AS_AS, "%s directory '%s' is not set up properly: %s",
				log_tag, path, cf_strerror(errno));
	}
	else if (! S_ISDIR(buf.st_mode)) {
		cf_crash_nostack(AS_AS,
				"%s directory '%s' is not set up properly: Not a directory",
				log_tag, path);
	}
}

static void
validate_smd_directory(void)
{
	size_t len = strlen(g_config.work_directory);
	define_deferred_array(smd_path, char, len + sizeof(SMD_DIR_NAME));

	strcpy(smd_path, g_config.work_directory);
	strcpy(smd_path + len, SMD_DIR_NAME);
	validate_directory(smd_path, "system metadata");
}

static void
verify_schema_file(const char* schema_file)
{
	cf_detail(AS_AS, "verifying schema file '%s', expecting hash: %s",
			schema_file, AS_SCHEMA_HASH);

	// Compute the SHA-256 hash of the schema file at runtime
	FILE* fp = fopen(schema_file, "rb");

	if (fp == NULL) {
		cf_crash_nostack(AS_AS,
				"schema file '%s' not found: %s - skipping integrity check",
				schema_file, cf_strerror(errno));
		return;
	}

	EVP_MD_CTX* mdctx = EVP_MD_CTX_new();

	if (mdctx == NULL) {
		cf_crash_nostack(AS_AS,
				"failed to create hash context for schema verification");
		fclose(fp);
		return;
	}

	if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
		cf_crash_nostack(AS_AS,
				"failed to initialize hash for schema verification");
		EVP_MD_CTX_free(mdctx);
		fclose(fp);
		return;
	}

	unsigned char buffer[8192];
	size_t bytes_read;

	while ((bytes_read = fread(buffer, 1, sizeof(buffer), fp)) > 0) {
		if (EVP_DigestUpdate(mdctx, buffer, bytes_read) != 1) {
			cf_crash_nostack(AS_AS,
					"failed to update hash for schema verification");
			EVP_MD_CTX_free(mdctx);
			fclose(fp);
			return;
		}
	}

	fclose(fp);

	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int hash_len;

	if (EVP_DigestFinal_ex(mdctx, hash, &hash_len) != 1) {
		cf_crash_nostack(AS_AS,
				"failed to finalize hash for schema verification");
		EVP_MD_CTX_free(mdctx);
		return;
	}

	EVP_MD_CTX_free(mdctx);

	// Convert hash to hex string
	char hash_str[65]; // SHA-256 is 32 bytes = 64 hex chars + null terminator

	for (unsigned int i = 0; i < hash_len; i++) {
		sprintf(&hash_str[i * 2], "%02x", hash[i]);
	}

	hash_str[64] = '\0';

	// Compare with the build-time hash
	if (strcmp(hash_str, AS_SCHEMA_HASH) != 0) {
		cf_warning(AS_AS, "unofficial configuration schema file detected: '%s'",
				schema_file);
		cf_warning(AS_AS, "expected hash: %s", AS_SCHEMA_HASH);
		cf_warning(AS_AS, "actual hash:   %s", hash_str);
		cf_warning(AS_AS,
				"using an unofficial configuration schema may result in unexpected behavior");
		cf_warning(AS_AS, "continuing with unofficial schema");
	}
	else {
		cf_info(AS_AS, "schema file integrity verified: %s", schema_file);
	}
}
