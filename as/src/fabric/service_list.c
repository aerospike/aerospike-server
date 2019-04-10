/*
 * service_list.c
 *
 * Copyright (C) 2017-2018 Aerospike, Inc.
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

#include "fabric/service_list.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/time.h>

#include "cf_mutex.h"
#include "cf_str.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "shash.h"
#include "socket.h"

#include "base/security.h"
#include "base/service.h"
#include "base/thr_info.h"

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "fabric/clustering.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"

#include "warnings.h"


//==========================================================
// Typedefs & constants.
//

#define HASH_STR_SZ 50

#define DEFAULT_PORT "3000"

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	FIELD_OP,
	FIELD_GEN,
	FIELD_SERV,
	FIELD_SERV_ALT,
	FIELD_CLEAR_STD,
	FIELD_TLS_STD,
	FIELD_CLEAR_ALT,
	FIELD_TLS_ALT,
	FIELD_TLS_NAME,

	NUM_FIELDS
} services_msg_field;

#define OP_UPDATE		0
#define OP_ACK			1
#define OP_UPDATE_REQ	2

// got_update  |  got_ack  |  Action on update thread
// ------------+-----------+------------------------------------------------
// false       |  false    |  Send OP_UPDATE_REQ
//             |           |  We don't know the peer and it doesn't know us.
//             |           |
// false       |  true     |  Send OP_UPDATE_REQ
//             |           |  We don't know the peer, but it knows us.
//             |           |
// true        |  false    |  Send OP_UPDATE
//             |           |  We know the peer, but it doesn't know us.
//             |           |
// true        |  true     |  None
//             |           |  We know each other.

typedef struct peer_s {
	bool		present;		// peer is a current peer (vs. an alumnus)

	bool		got_update;		// we saw an update from this peer
	bool		got_ack;		// we saw an acknowledgment from this peer
	uint64_t	retrans_at_ms;	// time of next retransmission for this peer

	uint64_t	in_gen;			// generation of last incoming change

	char		*serv;			// goes into "services"
	char		*serv_alt;		// goes into "services-alternate"

	char		*clear_std;		// goes into "peers-clear-std"
	char		*tls_std;		// goes into "peers-tls-std"
	char		*clear_alt;		// goes into "peers-clear-alt"
	char		*tls_alt;		// goes into "peers-tls-alt"
	char		*tls_name;		// peer's TLS name
} peer_t;

// Maps the given peer_t to a field in the peer_t.
typedef char **(*proj_t)(peer_t *p);

typedef struct filter_s {
	bool		tls;			// when set, include the TLS name
	bool		present;		// when set, exclude alumni
	uint64_t	since;			// base generation for a delta build
} filter_t;

// Builds an info value in g_info. Reduces g_peers, applies the given filter,
// and selects the peer_t field given by the projection function.
typedef void (*build_t)(cf_dyn_buf *db, proj_t proj, const filter_t *filter);

// How to turn the peer_t entries (g_peers) into an info value (g_info).
typedef struct peer_val_s {
	const char	*key;			// info value's key name, e.g., "services"
	proj_t		proj;			// projection function for the underlying
								// field in peer_t
	build_t		build;			// build function to update the info value
	const filter_t
				*filter;		// filter passed to the build function
} peer_val_t;

// How to turn our own services (g_local) into an info value (g_info).
typedef struct local_val_s {
	const char	*key;			// key identifier
	proj_t		proj;			// projection function that selects the
								// corresponding field from g_local
} local_val_t;

// Links fabric message fields to their peer_t fields.
typedef struct field_proj_s {
	int32_t		field;			// message field identifier (FIELD_*)
	proj_t		proj;			// projection function that selects the
								// corresponding field from a given peer_t
} field_proj_t;

// Context for building an info value. Passed to the build function.
typedef struct print_par_s {
	cf_dyn_buf	*db;			// the buffer to print to
	proj_t		proj;			// the field to print
	const char	*strip;			// what to strip from the end of the field
	bool		tls;			// when set, includes the TLS name
	bool		present;		// when set, excludes alumni
	uint64_t	since;			// the base generation for a delta build
	uint32_t	count;			// the number of already printed fields
} print_par_t;

// Context for the update thread's g_peers reduce.
typedef struct update_ctx_s {
	uint64_t	now_ms;			// current time
	uint64_t	retrans_at_ms;	// minimum of retransmission times, across all
								// peer_t entries in g_peers
} update_ctx_t;

static const msg_template MSG_TEMP[] = {
	{ FIELD_OP,			M_FT_UINT32	},
	{ FIELD_GEN,		M_FT_UINT32	},
	{ FIELD_SERV,		M_FT_STR	},
	{ FIELD_SERV_ALT,	M_FT_STR	},
	{ FIELD_CLEAR_STD,	M_FT_STR	},
	{ FIELD_TLS_STD,	M_FT_STR	},
	{ FIELD_CLEAR_ALT,	M_FT_STR	},
	{ FIELD_TLS_ALT,	M_FT_STR	},
	{ FIELD_TLS_NAME,	M_FT_STR	}
};

COMPILER_ASSERT(sizeof(MSG_TEMP) / sizeof(msg_template) == NUM_FIELDS);

#define SCRATCH_SZ 512
#define RETRANS_INTERVAL_MS 1000

static const filter_t PEERS_CLEAR = {
	.tls = false,	.present = true,	.since = 0
};

static const filter_t PEERS_TLS = {
	.tls = true,	.present = true,	.since = 0
};

static const filter_t ALUMNI_CLEAR = {
	.tls = false,	.present = false,	.since = 0
};

static const filter_t ALUMNI_TLS = {
	.tls = true,	.present = false,	.since = 0
};


//==========================================================
// Forward declarations.
//

// Peer management.

static char **proj_serv(peer_t *p);			// maps p to &p->serv
static char **proj_serv_alt(peer_t *p);		// maps p to &p->serv_alt
static char **proj_clear_std(peer_t *p);	// etc.
static char **proj_tls_std(peer_t *p);
static char **proj_clear_alt(peer_t *p);
static char **proj_tls_alt(peer_t *p);
static char **proj_tls_name(peer_t *p);

static peer_t *create_peer(cf_node node);
static peer_t *find_peer(cf_node node);
static void dump_peer(cf_node node, const peer_t *p);
static void set_present(const cf_node *nodes, uint32_t n_nodes, bool present);

static int32_t purge_alumni_reduce(const void *key, void *data, void *udata);
static void purge_alumni(void);

static void handle_cluster_change(const as_exchange_cluster_changed_event *ev,
		void *udata);
static int32_t handle_fabric_message(cf_node node, msg *m, void *udata);

static int32_t update_reduce(const void *key, void *data, void *udata);
static void *run_update_thread(void *udata);
static void wake_up(void);

// Local node information.

static char *print_list(const as_service_endpoint *endp, uint32_t limit,
		char sep, bool legacy);
static void enum_addrs(cf_addr_list *addrs);
static void free_addrs(cf_addr_list *addrs);
static void populate_local(void);

// Info value management.

static void set_info_val(const char *key, const char *val);
static void print_info_val(const char *key, cf_dyn_buf *db);

static int32_t build_peers_reduce(const void *key, void *data, void *udata);
static void build_peers(cf_dyn_buf *db, proj_t proj, const filter_t *filter);

static int32_t build_services_reduce(const void *key, void *data, void *udata);
static void build_services(cf_dyn_buf *db, proj_t proj, const filter_t *filter);

static void build_gen(cf_dyn_buf *db, proj_t proj, const filter_t *filter);

static void recalc(void);

// Miscellaneous.

static const char *op_str(uint32_t op);
static char *strip_suff(const char *in, const char *suff, char *out);


//==========================================================
// Inlines & macros.
//

#define ARRAY_COUNT(_a) (sizeof(_a) / (sizeof((_a)[0])))


//==========================================================
// Function tables.
//

static const peer_val_t PEER_VALS[] = {
//	key						proj			build			filter
	{ "peers-generation",	NULL,			build_gen,		NULL			},
	{ "peers-clear-std",	proj_clear_std,	build_peers,	&PEERS_CLEAR	},
	{ "peers-clear-alt",	proj_clear_alt,	build_peers,	&PEERS_CLEAR	},
	{ "peers-tls-std",		proj_tls_std,	build_peers,	&PEERS_TLS		},
	{ "peers-tls-alt",		proj_tls_alt,	build_peers,	&PEERS_TLS		},
	{ "alumni-clear-std",	proj_clear_std,	build_peers,	&ALUMNI_CLEAR	},
	{ "alumni-tls-std",		proj_tls_std,	build_peers,	&ALUMNI_TLS		},
	{ "services",			proj_serv,		build_services,	&PEERS_CLEAR	},
	{ "services-alternate",	proj_serv_alt,	build_services,	&PEERS_CLEAR	},
	{ "services-alumni",	proj_serv,		build_services,	&ALUMNI_CLEAR	}
};

static const local_val_t LOCAL_VALS[] = {
	{ "service-clear-std",	proj_clear_std	},
	{ "service-clear-alt",	proj_clear_alt	},
	{ "service-tls-std",	proj_tls_std	},
	{ "service-tls-alt",	proj_tls_alt	},
	{ "service",			proj_serv		}
};

static const field_proj_t FIELD_PROJS[] = {
	{ FIELD_CLEAR_STD,	proj_clear_std	},
	{ FIELD_CLEAR_ALT,	proj_clear_alt	},
	{ FIELD_TLS_STD,	proj_tls_std	},
	{ FIELD_TLS_ALT,	proj_tls_alt	},
	{ FIELD_TLS_NAME,	proj_tls_name	},
	{ FIELD_SERV,		proj_serv		},
	{ FIELD_SERV_ALT,	proj_serv_alt	}
};


//==========================================================
// Globals.
//

// Counts incoming peer changes.
static uint64_t g_in_gen = 0;

// Locking order: g_peers_lock before g_info_lock.

static cf_mutex g_peers_lock = CF_MUTEX_INIT;
static pthread_rwlock_t g_info_lock = PTHREAD_RWLOCK_INITIALIZER;

// These hash tables are created without locks.

static cf_shash *g_peers;
static cf_shash *g_info;

// "Peer" that holds our information.
static peer_t g_local;

// Signals the update thread.
static cf_queue g_wake_up;


//==========================================================
// Public API.
//

void
as_service_list_init(void)
{
	cf_detail(AS_SERVICE_LIST, "initializing service list");

	// These hash tables are created without locks.

	g_info = cf_shash_create(cf_shash_fn_zstr, HASH_STR_SZ,
			sizeof(char *), 32, 0);

	g_peers = cf_shash_create(cf_nodeid_shash_fn, sizeof(cf_node),
			sizeof(peer_t *), AS_CLUSTER_SZ, 0);

	cf_queue_init(&g_wake_up, sizeof(uint8_t), 1, true);

	populate_local();
	recalc();

	as_fabric_register_msg_fn(M_TYPE_INFO, MSG_TEMP, sizeof(MSG_TEMP),
			SCRATCH_SZ, handle_fabric_message, NULL);

	as_exchange_register_listener(handle_cluster_change, NULL);
	cf_thread_create_detached(run_update_thread, NULL);
}

int32_t
as_service_list_dynamic(char *key, cf_dyn_buf *db)
{
	cf_detail(AS_SERVICE_LIST, "handling info value %s", key);

	if (strcmp(key, "services-alumni-reset") == 0) {
		purge_alumni();
		cf_dyn_buf_append_string(db, "ok");
		return 0;
	}

	print_info_val(key, db);
	return 0;
}

int32_t
as_service_list_command(char *key, char *par, cf_dyn_buf *db)
{
	cf_detail(AS_SERVICE_LIST, "handling info command %s %s", key, par);

	uint64_t since = 0;

	// Hack to avoid generic parameter parsing for now, no error checking ...
	static const char prefix[] = "generation=";
	static const size_t prefix_len = sizeof(prefix) - 1;

	if (strncmp(par, prefix, prefix_len) == 0) {
		since = strtoul(par + prefix_len, NULL, 10);
	}

	// Find the build and projection functions for the given key.

	const peer_val_t *peer_val;

	for (uint32_t i = 0; i < ARRAY_COUNT(PEER_VALS); ++i) {
		peer_val = PEER_VALS + i;

		if (strcmp(peer_val->key, key) == 0) {
			break;
		}
	}

	// Build the info value directly into the given db, instead of
	// storing it in g_info as recalc() does.

	const filter_t *val_par = peer_val->filter;
	cf_mutex_lock(&g_peers_lock);

	peer_val->build(db, peer_val->proj, &(filter_t){
		.tls = val_par->tls, .present = false, .since = since
	});

	cf_mutex_unlock(&g_peers_lock);
	return 0;
}


//==========================================================
// Local helpers - peer management.
//

static char **
proj_serv(peer_t *p)
{
	return &p->serv;
}

static char **
proj_serv_alt(peer_t *p)
{
	return &p->serv_alt;
}

static char **
proj_clear_std(peer_t *p)
{
	return &p->clear_std;
}

static char **
proj_tls_std(peer_t *p)
{
	return &p->tls_std;
}

static char **
proj_clear_alt(peer_t *p)
{
	return &p->clear_alt;
}

static char **
proj_tls_alt(peer_t *p)
{
	return &p->tls_alt;
}

static char **
proj_tls_name(peer_t *p)
{
	return &p->tls_name;
}

static peer_t *
create_peer(cf_node node)
{
	cf_detail(AS_SERVICE_LIST, "new peer %lx", node);
	peer_t *p = cf_calloc(1, sizeof(peer_t));

	p->present = false;

	p->got_update = false;
	p->got_ack = false;
	p->retrans_at_ms = 0;

	p->in_gen = 0;

	p->serv = cf_strdup("");
	p->serv_alt = cf_strdup("");

	p->clear_std = cf_strdup("");
	p->tls_std = cf_strdup("");
	p->clear_alt = cf_strdup("");
	p->tls_alt = cf_strdup("");
	p->tls_name = cf_strdup("");

	int32_t res = cf_shash_put_unique(g_peers, &node, &p);

	cf_assert(res == CF_SHASH_OK, AS_SERVICE_LIST,
			"cf_shash_put_unique() failed: %d", res);

	cf_detail(AS_SERVICE_LIST, "added peer %lx", node);
	return p;
}

static peer_t *
find_peer(cf_node node)
{
	cf_detail(AS_SERVICE_LIST, "finding peer %lx", node);
	peer_t *p;
	int32_t res = cf_shash_get(g_peers, &node, &p);

	switch (res) {
	case CF_SHASH_OK:
		return p;

	case CF_SHASH_ERR_NOT_FOUND:
		return create_peer(node);

	default:
		cf_crash(AS_SERVICE_LIST, "cf_shash_get() failed: %d", res);
		return NULL; // not reached
	}
}

static void
dump_peer(cf_node node, const peer_t *p)
{
	cf_detail(AS_SERVICE_LIST, "--------------------- peer change %016lx",
			node);

	cf_detail(AS_SERVICE_LIST, "present       %d", (int32_t)p->present);
	cf_detail(AS_SERVICE_LIST, "got_update    %d", (int32_t)p->got_update);
	cf_detail(AS_SERVICE_LIST, "got_ack       %d", (int32_t)p->got_ack);
	cf_detail(AS_SERVICE_LIST, "retrans_at_ms %d",
			(int32_t)(p->retrans_at_ms % 1000000));
	cf_detail(AS_SERVICE_LIST, "in_gen        %lu", p->in_gen);
	cf_detail(AS_SERVICE_LIST, "serv          %s", p->serv);
	cf_detail(AS_SERVICE_LIST, "serv_alt      %s", p->serv_alt);
	cf_detail(AS_SERVICE_LIST, "clear_std     %s", p->clear_std);
	cf_detail(AS_SERVICE_LIST, "tls_std       %s", p->tls_std);
	cf_detail(AS_SERVICE_LIST, "clear_alt     %s", p->clear_alt);
	cf_detail(AS_SERVICE_LIST, "tls_alt       %s", p->tls_alt);
	cf_detail(AS_SERVICE_LIST, "tls_name      %s", p->tls_name);
}

static void
set_present(const cf_node *nodes, uint32_t n_nodes, bool present)
{
	for (uint32_t i = 0; i < n_nodes; ++i) {
		peer_t *p = find_peer(nodes[i]);

		p->present = present;

		if (present && p->got_update && p->got_ack) {
			p->retrans_at_ms = 0;
		}

		p->in_gen = ++g_in_gen;

		dump_peer(nodes[i], p);
	}
}

static int32_t
purge_alumni_reduce(const void *key, void *data, void *udata)
{
	cf_node node = *(const cf_node *)key;
	peer_t *p = *(peer_t **)data;
	(void)udata;

	cf_detail(AS_SERVICE_LIST, "visiting node %lx", node);

	if (p->present) {
		cf_detail(AS_SERVICE_LIST, "node present");
		return CF_SHASH_OK;
	}

	cf_detail(AS_SERVICE_LIST, "deleting alumnus");
	++g_in_gen;
	return CF_SHASH_REDUCE_DELETE;
}

static void
purge_alumni(void)
{
	cf_detail(AS_SERVICE_LIST, "purging alumni");
	cf_mutex_lock(&g_peers_lock);

	uint64_t old_gen = g_in_gen;

	cf_shash_reduce(g_peers, purge_alumni_reduce, NULL);

	if (g_in_gen > old_gen) {
		recalc();
	}

	cf_mutex_unlock(&g_peers_lock);
}

// handle_cluster_change() is authoritative for who's currently a peer and
// who's just an alumnus, i.e., a former peer. It does two things:
//
//   1. It adds previously unknown nodes to g_peers.
//
//   2. It manages the peer_t::present field: true = peer, false = alumnus.
//
// The other peer_t fields are managed by handle_fabric_message().

static void
handle_cluster_change(const as_exchange_cluster_changed_event *ev, void *udata)
{
	cf_detail(AS_SERVICE_LIST, "------------------ cluster change %016lx",
			ev->cluster_key);
	(void)udata;

	// The previous succession list.

	static cf_node suc_old[AS_CLUSTER_SZ];
	static uint32_t sz_old = 0;

	// Remove ourselves from the new succession list.

	cf_node suc_new[AS_CLUSTER_SZ];
	uint32_t sz_new = 0;

	for (uint32_t i = 0; i < ev->cluster_size; ++i) {
		if (ev->succession[i] == g_config.self_node) {
			continue;
		}

		suc_new[sz_new] = ev->succession[i];
		++sz_new;
	}

	as_clustering_log_cf_node_array(CF_DETAIL, AS_SERVICE_LIST, "new peers",
			suc_new, (int32_t)sz_new);
	as_clustering_log_cf_node_array(CF_DETAIL, AS_SERVICE_LIST, "old peers",
			suc_old, (int32_t)sz_old);

	cf_node add[AS_CLUSTER_SZ];
	uint32_t n_add = 0;

	cf_node rem[AS_CLUSTER_SZ];
	uint32_t n_rem = 0;

	uint32_t i_old = 0;
	uint32_t i_new = 0;

	// Calculate the differences between the old and the new peers.

	// This assumes that a succession list contains the node IDs in
	// descending order.

	while (i_old < sz_old || i_new < sz_new) {
		cf_node node_old = i_old < sz_old ? suc_old[i_old] : 0;
		cf_node node_new = i_new < sz_new ? suc_new[i_new] : 0;

		// Old succession list skipped ahead of new succession list.
		// (Or we hit the end of the old succession list.)

		if (node_old < node_new) {
			add[n_add] = node_new;
			++n_add;
			++i_new;
			continue;
		}

		// New succession list skipped ahead of old succession list.
		// (Or we hit the end of the new succession list.)

		if (node_new < node_old) {
			rem[n_rem] = node_old;
			++n_rem;
			++i_old;
			continue;
		}

		++i_old;
		++i_new;
	}

	as_clustering_log_cf_node_array(CF_DETAIL, AS_SERVICE_LIST, "peers add",
			add, (int32_t)n_add);
	as_clustering_log_cf_node_array(CF_DETAIL, AS_SERVICE_LIST, "peers rem",
			rem, (int32_t)n_rem);

	if (n_add + n_rem > 0) {
		cf_mutex_lock(&g_peers_lock);

		set_present(add, n_add, true);
		set_present(rem, n_rem, false);
		recalc();

		cf_mutex_unlock(&g_peers_lock);
		wake_up();
	}

	// Next time, new succession list will be the old succession list.

	for (uint32_t i = 0; i < sz_new; ++i) {
		suc_old[i] = suc_new[i];
	}

	sz_old = sz_new;
}

// handle_fabric_message() manages what we know about another node. It does
// two things:
//
//   1. It adds previously unknown nodes to g_peers.
//
//   2. It manages all peer_t fields, except peer_t::present. It doesn't care
//      whether a node is currently a peer (present = true) or just an alumnus
//      (present = false). It blindly updates the peer_t fields, always.
//
// peer_t::present is managed by handle_cluster_change().

static int32_t
handle_fabric_message(cf_node node, msg *m, void *udata)
{
	(void)udata;
	cf_detail(AS_SERVICE_LIST, "------------------ fabric message %016lx",
			node);

	// Get operation and generation.

	uint32_t op;

	if (msg_get_uint32(m, FIELD_OP, &op) < 0) {
		cf_warning(AS_SERVICE_LIST, "op-less service message from node %lx",
				node);
		as_fabric_msg_put(m);
		return 0;
	}

	uint32_t gen;

	if (msg_get_uint32(m, FIELD_GEN, &gen) < 0) {
		cf_warning(AS_SERVICE_LIST, "gen-less service message from node %lx",
				node);
		as_fabric_msg_put(m);
		return 0;
	}

	cf_detail(AS_SERVICE_LIST, "op %s gen %u", op_str(op), gen);

	cf_mutex_lock(&g_peers_lock);

	peer_t *p = find_peer(node);
	bool change = false;

	if (op == OP_ACK) {
		cf_detail(AS_SERVICE_LIST, "OP_ACK %u from %lx", gen, node);

		// Set peer_t::ack.

		change = change || !p->got_ack;
		p->got_ack = true;

		if (p->present && p->got_update) {
			change = change || p->retrans_at_ms != 0;
			p->retrans_at_ms = 0;
		}

		if (change) {
			dump_peer(node, p);
		}

		cf_mutex_unlock(&g_peers_lock);
		as_fabric_msg_put(m);

		return 0;
	}

	if (op != OP_UPDATE && op != OP_UPDATE_REQ) {
		cf_warning(AS_SERVICE_LIST, "invalid service list op %d from node %lx",
				op, node);

		cf_mutex_unlock(&g_peers_lock);
		as_fabric_msg_put(m);

		return 0;
	}

	if (op == OP_UPDATE) {
		cf_detail(AS_SERVICE_LIST, "OP_UPDATE from %lx", node);
	}
	else {
		cf_detail(AS_SERVICE_LIST, "OP_UPDATE_REQ from %lx", node);

		// Clear peer_t::ack.

		change = change || p->got_ack;
		p->got_ack = false;

		wake_up();
	}

	// Set peer_t::update.

	change = change || !p->got_update;
	p->got_update = true;

	if (p->present && p->got_ack) {
		change = change || p->retrans_at_ms != 0;
		p->retrans_at_ms = 0;
	}

	// Populate peer_t from message fields.

	for (uint32_t i = 0; i < ARRAY_COUNT(FIELD_PROJS); ++i) {
		char **to = FIELD_PROJS[i].proj(p);
		char *old = *to;

		// We follow the convention of the old code, which omits
		// empty fields from the fabric message. So let's be prepared
		// for missing fields!

		if (msg_get_str(m, FIELD_PROJS[i].field, to, MSG_GET_COPY_MALLOC) < 0) {
			*to = cf_strdup("");
		}

		change = change || strcmp(*to, old) != 0;
		cf_free(old);
	}

	if (change) {
		p->in_gen = ++g_in_gen;
		dump_peer(node, p);
		recalc();
	}

	// Send ACK.

	cf_detail(AS_SERVICE_LIST, "sending OP_ACK to %lx", node);

	msg_preserve_fields(m, 1, FIELD_GEN);
	msg_set_uint32(m, FIELD_OP, OP_ACK);

	int32_t res = as_fabric_send(node, m, AS_FABRIC_CHANNEL_CTRL);

	if (res != AS_FABRIC_SUCCESS) {
		cf_warning(AS_SERVICE_LIST, "error while sending OP_ACK to %lx: %d",
				node, res);
		cf_mutex_unlock(&g_peers_lock);
		as_fabric_msg_put(m);
		return 0;
	}

	cf_mutex_unlock(&g_peers_lock);

	// No as_fabric_msg_put(), since we reused the original message to
	// send the ACK.

	return 0;
}

static int32_t
update_reduce(const void *key, void *data, void *udata)
{
	cf_node node = *(const cf_node *)key;
	peer_t *p = *(peer_t **)data;
	update_ctx_t *ctx = udata;

	cf_detail(AS_SERVICE_LIST,
			"updating %lx - present %d got_update %d got_ack %d", node,
			(int32_t)p->present, (int32_t)p->got_update, (int32_t)p->got_ack);

	// If it's an alumnus, don't update.

	if (!p->present) {
		cf_detail(AS_SERVICE_LIST, "skipping alumnus");
		return CF_SHASH_OK;
	}

	// If we don't need anything from the peer and the peer doesn't need
	// anything from us (i.e., it acknowledged), we're done.

	if (p->got_update && p->got_ack) {
		cf_detail(AS_SERVICE_LIST, "nothing to be done");
		return CF_SHASH_OK;
	}

	// If it's not yet time to transmit, then don't. Also calculate
	// ctx->retrans_at_ms as the minimum across all peers.

	if (p->retrans_at_ms != 0) {
		cf_detail(AS_SERVICE_LIST, "retrans_at_ms %d now_ms %d",
				(int32_t)(p->retrans_at_ms % 1000000),
				(int32_t)(ctx->now_ms % 1000000));

		if (ctx->now_ms < p->retrans_at_ms) {
			cf_detail(AS_SERVICE_LIST, "not yet");

			if (ctx->retrans_at_ms == 0 ||
					p->retrans_at_ms < ctx->retrans_at_ms) {
				ctx->retrans_at_ms = p->retrans_at_ms;
			}

			return CF_SHASH_OK;
		}
	}
	else {
		cf_detail(AS_SERVICE_LIST, "no retrans");
	}

	// If we never got an update from a peer, request one from the peer
	// (OP_UPDATE_REQ); otherwise don't (OP_UPDATE).

	// This is handy after a restart. The peers won't notice that we restarted
	// and won't send us an update. So, we ask them by sending OP_UPDATE_REQ.

	uint32_t op = p->got_update ? OP_UPDATE : OP_UPDATE_REQ;

	// Compose outgoing message.

	msg *m = as_fabric_msg_get(M_TYPE_INFO);
	msg_set_uint32(m, FIELD_OP, op);

	// We don't support dynamically changing interface configurations any
	// longer, so we'll only ever have generation 1.

	msg_set_uint32(m, FIELD_GEN, 1);

	// Populate fields from g_local.

	for (uint32_t i = 0; i < ARRAY_COUNT(FIELD_PROJS); ++i) {
		char **from = FIELD_PROJS[i].proj(&g_local);

		// We follow the convention of the old code, which omits empty fields
		// from the fabric message.

		if ((*from)[0] != 0) {
			msg_set_str(m, FIELD_PROJS[i].field, *from, MSG_SET_COPY);
		}
	}

	// Send fabric message.

	cf_detail(AS_SERVICE_LIST, "sending %s to %lx", op_str(op), node);

	int32_t res = as_fabric_send(node, m, AS_FABRIC_CHANNEL_CTRL);

	if (res == AS_FABRIC_ERR_NO_NODE) {
		cf_detail(AS_SERVICE_LIST, "unknown node %lx", node);
		as_fabric_msg_put(m);
	}
	else if (res != AS_FABRIC_SUCCESS) {
		cf_warning(AS_SERVICE_LIST, "error while sending %s to %lx: %d",
				op_str(op), node, res);
		as_fabric_msg_put(m);
	}

	p->retrans_at_ms = ctx->now_ms + RETRANS_INTERVAL_MS;

	if (ctx->retrans_at_ms == 0 || p->retrans_at_ms < ctx->retrans_at_ms) {
		ctx->retrans_at_ms = p->retrans_at_ms;
	}

	cf_detail(AS_SERVICE_LIST, "retrans_at_ms %d now_ms %d",
			(int32_t)(p->retrans_at_ms % 1000000),
			(int32_t)(ctx->now_ms % 1000000));

	return CF_SHASH_OK;
}

static void *
run_update_thread(void *udata)
{
	(void)udata;

	while (true) {
		update_ctx_t ctx = {
			.now_ms = cf_getms(), .retrans_at_ms = 0
		};

		cf_mutex_lock(&g_peers_lock);

		cf_detail(AS_SERVICE_LIST,
				"----------------------------------------- updating");
		cf_shash_reduce(g_peers, update_reduce, &ctx);

		cf_mutex_unlock(&g_peers_lock);

		int32_t wait;

		if (ctx.retrans_at_ms == 0) {
			wait = CF_QUEUE_FOREVER;
			cf_detail(AS_SERVICE_LIST, "sleeping forever");
		}
		else {
			wait = (int32_t)(ctx.retrans_at_ms - ctx.now_ms);
			cf_detail(AS_SERVICE_LIST, "sleeping %d ms", wait);
		}

		uint8_t dummy;
		cf_queue_pop(&g_wake_up, &dummy, wait);
	}

	return NULL; // not reached
}

static void
wake_up(void)
{
	cf_detail(AS_SERVICE_LIST, "waking up update thread");

	static uint8_t dummy = 0;
	cf_queue_push(&g_wake_up, &dummy);
}


//==========================================================
// Local helpers - local node information.
//

static char *
print_list(const as_service_endpoint *endp, uint32_t limit, char sep,
		bool legacy)
{
	cf_detail(AS_SERVICE_LIST, "printing list - count %u port %hu limit %u "
			"legacy %d", endp->addrs.n_addrs, endp->port, limit,
			(int32_t)legacy);

	if (endp->port == 0) {
		cf_detail(AS_SERVICE_LIST, "service inactive");
		return NULL;
	}

	legacy = legacy || cf_ip_addr_legacy_only();

	cf_dyn_buf_define(db);
	uint32_t n_out = 0;

	for (uint32_t i = 0; i < endp->addrs.n_addrs &&
			(limit == 0 || n_out < limit); ++i) {
		cf_detail(AS_SERVICE_LIST, "adding %s", endp->addrs.addrs[i]);

		if (legacy && !cf_ip_addr_str_is_legacy(endp->addrs.addrs[i])) {
			cf_detail(AS_SERVICE_LIST, "skipping non-legacy");
			continue;
		}

		if (n_out > 0) {
			cf_dyn_buf_append_char(&db, sep);
		}

		if (cf_ip_addr_is_dns_name(endp->addrs.addrs[i])) {
			cf_dyn_buf_append_string(&db, endp->addrs.addrs[i]);
			cf_dyn_buf_append_char(&db, ':');
			cf_dyn_buf_append_string(&db, cf_ip_port_print(endp->port));
		}
		else {
			cf_sock_addr addr;
			CF_NEVER_FAILS(cf_sock_addr_from_host_port(endp->addrs.addrs[i],
					endp->port, &addr));
			cf_dyn_buf_append_string(&db, cf_sock_addr_print(&addr));
		}

		++n_out;
	}

	char *str = n_out > 0 ? cf_dyn_buf_strdup(&db) : NULL;

	cf_dyn_buf_free(&db);
	return str;
}

static void
enum_addrs(cf_addr_list *addrs)
{
	cf_ip_addr bin_addrs[CF_SOCK_CFG_MAX];
	uint32_t n_bin_addrs = CF_SOCK_CFG_MAX;

	if (cf_inter_get_addr_all(bin_addrs, &n_bin_addrs) < 0) {
		cf_crash(AS_SERVICE_LIST, "address enumeration failed");
	}

	addrs->n_addrs = 0;

	for (uint32_t i = 0; i < n_bin_addrs; ++i) {
		if (cf_ip_addr_is_local(bin_addrs + i)) {
			continue;
		}

		char addr_str[250];
		cf_ip_addr_to_string_safe(bin_addrs + i, addr_str, sizeof(addr_str));

		addrs->addrs[addrs->n_addrs] = cf_strdup(addr_str);
		++addrs->n_addrs;
	}
}

static void
free_addrs(cf_addr_list *addrs)
{
	for (uint32_t i = 0; i < addrs->n_addrs; ++i) {
		cf_free((char *)addrs->addrs[i]);
	}

	addrs->n_addrs = 0;
}

static void
populate_local(void)
{
	cf_detail(AS_SERVICE_LIST, "populating local info");

	// Populate from access addresses.

	g_local.serv = print_list(&g_access.service, 0, ';', true);
	g_local.serv_alt = print_list(&g_access.alt_service, 1, ';', true);

	g_local.clear_std = print_list(&g_access.service, 0, ',', false);
	g_local.tls_std = print_list(&g_access.tls_service, 0, ',', false);
	g_local.clear_alt = print_list(&g_access.alt_service, 0, ',', false);
	g_local.tls_alt = print_list(&g_access.alt_tls_service, 0, ',', false);

	// Alternate lists default to no addresses.

	if (g_local.serv_alt == NULL) {
		g_local.serv_alt = "";
	}

	if (g_local.clear_alt == NULL) {
		g_local.clear_alt = "";
	}

	if (g_local.tls_alt == NULL) {
		g_local.tls_alt = "";
	}

	// Standard lists default to all interface addresses.

	as_service_endpoint endp;
	enum_addrs(&endp.addrs);

	// Don't test g_local.serv, which is also NULL in IPv6-only setups.
	if (g_local.clear_std == NULL) {
		endp.port = g_access.service.port;
		g_local.serv = print_list(&endp, 0, ';', true);
	}

	if (g_local.clear_std == NULL) {
		endp.port = g_access.service.port;
		g_local.clear_std = print_list(&endp, 0, ',', false);
	}

	if (g_local.tls_std == NULL) {
		endp.port = g_access.tls_service.port;
		g_local.tls_std = print_list(&endp, 0, ',', false);
	}

	free_addrs(&endp.addrs);

	// Take care of unused (port == 0) standard lists, which are
	// still NULL at this point.

	if (g_local.serv == NULL) {
		g_local.serv = "";
	}

	if (g_local.clear_std == NULL) {
		g_local.clear_std = "";
	}

	if (g_local.tls_std == NULL) {
		g_local.tls_std = "";
	}

	// Finally, the TLS name.

	g_local.tls_name = g_config.tls_service.tls_our_name;

	if (g_local.tls_name == NULL) {
		g_local.tls_name = "";
	}

	// Populate info values from g_local.

	cf_detail(AS_SERVICE_LIST, "populating info values");

	for (uint32_t i = 0; i < ARRAY_COUNT(LOCAL_VALS); ++i) {
		const char *key = LOCAL_VALS[i].key;
		const char *val = *(LOCAL_VALS[i].proj(&g_local));
		set_info_val(key, val);
	}

	dump_peer(0, &g_local);
}


//==========================================================
// Local helpers - info value management.
//

static void
set_info_val(const char *key, const char *val)
{
	cf_detail(AS_SERVICE_LIST, "info val %s <- %s", key, val);

	char hash_str[HASH_STR_SZ];
	strncpy(hash_str, key, HASH_STR_SZ); // pads with \0

	// Remove existing value.

	char *val_old;
	int32_t res = cf_shash_get_and_delete(g_info, hash_str, &val_old);

	switch (res) {
	case CF_SHASH_OK:
		cf_free(val_old);
		break;
	case CF_SHASH_ERR_NOT_FOUND:
		break;
	default:
		cf_crash(AS_SERVICE_LIST, "cf_shash_get_and_delete() failed: %d", res);
		break;
	}

	// Set new value.

	char *val_dup = cf_strdup(val);
	res = cf_shash_put_unique(g_info, hash_str, &val_dup);

	cf_assert(res == CF_SHASH_OK, AS_SERVICE_LIST,
			"cf_shash_put_unique() failed: %d", res);
}

static void
print_info_val(const char *key, cf_dyn_buf *db)
{
	pthread_rwlock_rdlock(&g_info_lock);

	char hash_str[HASH_STR_SZ];
	strncpy(hash_str, key, HASH_STR_SZ); // pads with \0

	char *val;
	int32_t res = cf_shash_get(g_info, hash_str, &val);

	cf_assert(res == CF_SHASH_OK, AS_SERVICE_LIST, "cf_shash_get() failed: %d",
			res);

	cf_dyn_buf_append_string(db,  val);
	cf_detail(AS_SERVICE_LIST, "info val %s -> %s", key, val);

	pthread_rwlock_unlock(&g_info_lock);
}

static int32_t
build_peers_reduce(const void *key, void *data, void *udata)
{
	cf_node node = *(const cf_node *)key;
	peer_t *p = *(peer_t **)data;
	print_par_t *par = udata;

	cf_detail(AS_SERVICE_LIST, "visiting node %lx", node);

	// Skip alumnus, if alumni excluded.

	if (par->present && !p->present) {
		cf_detail(AS_SERVICE_LIST, "node absent");
		return CF_SHASH_OK;
	}

	// Skip, if unchanged since the given delta cut-off generation.

	if (p->in_gen <= par->since) {
		cf_detail(AS_SERVICE_LIST, "no recent change");
		return CF_SHASH_OK;
	}

	// Read selected field from peer_t.

	const char *field = *(par->proj(p));

	if (field[0] == 0) {
		cf_detail(AS_SERVICE_LIST, "field empty");
		return CF_SHASH_OK;
	}

	// Append field value.

	cf_detail(AS_SERVICE_LIST, "adding %s", field);
	cf_dyn_buf *db = par->db;

	if (par->count > 0) {
		cf_dyn_buf_append_char(db, ',');
	}

	char node_str[17];
	cf_str_itoa_u64(node, node_str, 16);

	cf_dyn_buf_append_char(db, '[');
	cf_dyn_buf_append_string(db, node_str);
	cf_dyn_buf_append_char(db, ',');

	// (a) Typical case. Not a delta query, or a delta query but
	//     not an alumnus. Report normally, i.e., as
	//
	//     [<node-id>,<tls-name>,[addr-port-1, addr-port-2, ...]]

	if (par->since == 0 || p->present) {
		if (par->tls) {
			cf_dyn_buf_append_string(db, p->tls_name);
		}

		cf_dyn_buf_append_char(db, ',');
		cf_dyn_buf_append_char(db, '[');

		char buff[strlen(field) + 1];
		char *pref = strip_suff(field, par->strip, buff);
		cf_detail(AS_SERVICE_LIST, "stripped %s", pref);
		cf_dyn_buf_append_string(db, pref);

		cf_dyn_buf_append_char(db, ']');
	}

	// (b) Delta query and an alumnus. Include alumnus as
	//
	//    [<node-id>,,]
	//
	//    in response to indicate that the node with ID <node-id>
	//    (= the alumnus) went away.

	else {
		cf_dyn_buf_append_char(db, ',');
	}

	cf_dyn_buf_append_char(db, ']');

	++par->count;
	return CF_SHASH_OK;
}

static void
build_peers(cf_dyn_buf *db, proj_t proj, const filter_t *filter)
{
	cf_dyn_buf_append_uint64(db, g_in_gen);
	cf_dyn_buf_append_char(db, ',');

	cf_dyn_buf_append_string(db, DEFAULT_PORT);
	cf_dyn_buf_append_char(db, ',');

	cf_dyn_buf_append_char(db, '[');

	print_par_t print_par = {
		.db = db,
		.proj = proj,
		.strip = ":" DEFAULT_PORT,
		.tls = filter->tls,
		.present = filter->present,
		.since = filter->since,
		.count = 0
	};

	cf_shash_reduce(g_peers, build_peers_reduce, &print_par);

	cf_dyn_buf_append_char(db, ']');
}

static int32_t
build_services_reduce(const void *key, void *data, void *udata)
{
	cf_node node = *(const cf_node *)key;
	peer_t *p = *(peer_t **)data;
	print_par_t *par = udata;

	cf_detail(AS_SERVICE_LIST, "visiting node %lx", node);

	// Skip alumnus, if alumni excluded.

	if (par->present && !p->present) {
		cf_detail(AS_SERVICE_LIST, "node absent");
		return CF_SHASH_OK;
	}

	// Read selected field from peer_t.

	const char *field = *(par->proj(p));

	if (field[0] == 0) {
		cf_detail(AS_SERVICE_LIST, "field empty");
		return CF_SHASH_OK;
	}

	// Append field value.

	cf_detail(AS_SERVICE_LIST, "adding %s", field);
	cf_dyn_buf *db = par->db;

	if (par->count > 0) {
		cf_dyn_buf_append_char(db, ';');
	}

	cf_dyn_buf_append_string(db, field);
	++par->count;

	return CF_SHASH_OK;
}

static void
build_services(cf_dyn_buf *db, proj_t proj, const filter_t *filter)
{
	print_par_t print_par = {
		.db = db,
		.proj = proj,
		.strip = NULL,
		.tls = false,
		.present = filter->present,
		.since = 0,
		.count = 0
	};

	cf_shash_reduce(g_peers, build_services_reduce, &print_par);
}

static void
build_gen(cf_dyn_buf *db, proj_t proj, const filter_t *filter)
{
	(void)proj;
	(void)filter;

	cf_dyn_buf_append_uint64(db, g_in_gen);
}

static void
recalc(void)
{
	cf_detail(AS_SERVICE_LIST, "recalculating info values");
	pthread_rwlock_wrlock(&g_info_lock);

	// Loop through all info values.

	for (uint32_t i = 0; i < ARRAY_COUNT(PEER_VALS); ++i) {
		const peer_val_t *peer_val = PEER_VALS + i;

		// Skip, if info value isn't refreshable.

		if (peer_val->build == NULL) {
			continue;
		}

		cf_detail(AS_SERVICE_LIST, "recalculating %s", peer_val->key);

		// Build the info value into a temporary db.

		cf_dyn_buf_define(db);

		peer_val->build(&db, peer_val->proj, peer_val->filter);
		char *val = cf_dyn_buf_strdup(&db);

		cf_dyn_buf_free(&db);

		// Store the info value in g_info.

		if (val == NULL) {
			set_info_val(peer_val->key, "");
		}
		else {
			set_info_val(peer_val->key, val);
			cf_free(val);
		}
	}

	pthread_rwlock_unlock(&g_info_lock);
}


//==========================================================
// Local helpers - miscellaneous.
//

static const char *
op_str(uint32_t op)
{
	switch (op) {
	case OP_UPDATE:
		return "OP_UPDATE";
	case OP_ACK:
		return "OP_ACK";
	case OP_UPDATE_REQ:
		return "OP_UPDATE_REQ";
	default:
		return "OP_???";
	}
}

// Strip a given suffix off the elements of a comma-separated list. With suffix
// ":xxx", for example, the following would happen:
//
//   aaa:xxx,bbb:yyy,ccc:xxx,ddd:xxx -> aaa,bbb:yyy,ccc,ddd
//
// Used to strip the default port off "host:port" lists.

static char *
strip_suff(const char *in, const char *suff, char *out)
{
	size_t in_len = strlen(in);
	size_t suff_len = strlen(suff);

	size_t i_in = in_len;
	size_t i_out = in_len;

	out[i_out] = 0;

	while (i_in >= suff_len) {
		if (memcmp(in + i_in - suff_len, suff, suff_len) == 0) {
			i_in -= suff_len;
		}

		while (i_in > 0) {
			out[--i_out] = in[--i_in];

			if (in[i_in] == ',') {
				break;
			}
		}
	}

	return out + i_out;
}
