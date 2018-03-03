/*
 * monitor.c
 *
 * Copyright (C) 2013-2016 Aerospike, Inc.
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

/*
 *  Aerospike Long Running Job Monitoring interface
 *
 *  This file implements the generic interface for the long running jobs
 *  in Aerospike like query / scan / batch etc. The idea is to able to see
 *  what is going on in the system.
 *
 *  Each module which needs to show up in the monitoring needs to register
 *  and implement the interfaces.
 */

#include <stdlib.h>
#include <stdio.h>

#include "base/secondary_index.h"
#include "base/monitor.h"
#include "base/scan.h"
#include "base/thr_sindex.h"


#define AS_MON_MAX_MODULE 10

// Indexed by as_mon_module_slot - keep in sync.
const char * AS_MON_MODULES[] = {
		"query",
		"scan",
		"sindex-builder"
};

// functional declaration
int    as_mon_populate_jobstat(as_mon_jobstat * stat, cf_dyn_buf *db);
static as_mon * g_as_mon_module[AS_MON_MAX_MODULE];
static uint32_t g_as_mon_curr_mod_count;
int    as_mon_register(const char *module);

/*
 * This is called to init the mon subsystem.
 */
int
as_mon_init()
{
	g_as_mon_curr_mod_count = 0;
	as_mon_register(AS_MON_MODULES[QUERY_MOD]);
	as_mon_register(AS_MON_MODULES[SCAN_MOD]);
	as_mon_register(AS_MON_MODULES[SBLD_MOD]);

	// TODO: Add more stuff if there is any locks needs some stats needed etc etc ...
	return AS_MON_OK;
}

as_mon *
as_mon_get_module(const char * module)
{
	as_mon_module_slot mod;
	if (strcmp(module, AS_MON_MODULES[QUERY_MOD]) == 0) {
		mod = QUERY_MOD;
	}
	else if (strcmp(module, AS_MON_MODULES[SCAN_MOD]) == 0) {
		mod = SCAN_MOD;
	}
	else if (strcmp(module, AS_MON_MODULES[SBLD_MOD]) == 0) {
		mod = SBLD_MOD;
	}
	else {
		return NULL;
	}

	return g_as_mon_module[mod];
}

/*
 * The call to register a module to be tracked under as mon interface
 * Returns -
 * 		AS_MON_OK    - On successful registartion.
 * 		AS_MON_ERROR - failure
 */
int
as_mon_register(const char *module)
{
	if (!module) return AS_MON_ERR;
	as_mon *mon_obj = (as_mon *) cf_rc_alloc(sizeof(as_mon));
	as_mon_cb *cb = cf_malloc(sizeof(as_mon_cb));
	as_mon_module_slot mod;

	if(!strcmp(module, AS_MON_MODULES[QUERY_MOD])) {
		cb->get_jobstat     = as_query_get_jobstat;
		cb->get_jobstat_all = as_query_get_jobstat_all;

		cb->set_priority    = as_query_set_priority;
		cb->kill            = as_query_kill;
		cb->suspend         = NULL;
		cb->set_pendingmax  = NULL;
		cb->set_maxinflight = NULL;
		cb->set_maxpriority = NULL;
		mod = QUERY_MOD;
	}
	else if (!strcmp(module, AS_MON_MODULES[SCAN_MOD]))
	{
		cb->get_jobstat     = as_scan_get_jobstat;
		cb->get_jobstat_all = as_scan_get_jobstat_all;

		cb->set_priority    = as_scan_change_job_priority;
		cb->kill            = as_scan_abort;
		cb->suspend         = NULL;
		cb->set_pendingmax  = NULL;
		cb->set_maxinflight = NULL;
		cb->set_maxpriority = NULL;
		mod = SCAN_MOD;
	}
	else if (!strcmp(module, AS_MON_MODULES[SBLD_MOD]))
	{
		cb->get_jobstat     = as_sbld_get_jobstat;
		cb->get_jobstat_all = as_sbld_get_jobstat_all;

		cb->set_priority    = NULL;
		cb->kill            = as_sbld_abort;
		cb->suspend         = NULL;
		cb->set_pendingmax  = NULL;
		cb->set_maxinflight = NULL;
		cb->set_maxpriority = NULL;
		mod = SBLD_MOD;
	}
	else {
		cf_warning(AS_MON, "wrong module parameter.");
		return AS_MON_ERR;
	}
	// Setup mon object
	mon_obj->type  = cf_strdup(module);
	memcpy(&mon_obj->cb, cb, sizeof(as_mon_cb));

	g_as_mon_curr_mod_count++;
	g_as_mon_module[mod] = mon_obj;
	return AS_MON_OK;
}

/*
 * Calls the callback function to kill a job.
 *
 * Returns
 * 		AS_MON_OK - On success.
 * 		AS_MON_ERR - on failure.
 *
 */
int
as_mon_killjob(const char *module, uint64_t id, cf_dyn_buf *db)
{
	int retval = AS_MON_ERR;
	as_mon * mon_object = as_mon_get_module(module);

	if (!mon_object) {
		cf_warning(AS_MON, "Failed to find module %s", module);
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\" not found");
		return retval;
	}

	if (mon_object->cb.kill) {
		retval = mon_object->cb.kill(id);

		if (retval == AS_MON_OK) {
			cf_dyn_buf_append_string(db, "OK");
		}
		else {
			cf_dyn_buf_append_string(db, "ERROR:");
			cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
			cf_dyn_buf_append_string(db, ":job not active");
		}
	}
	else {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":kill-job not supported for module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\"");
	}
	return retval;
}

/*
 * Calls the callback function to set priority of a job.
 *
 * Returns
 * 		AS_MON_OK - On success.
 * 		AS_MON_ERR - on failure.
 *
 */
int
as_mon_set_priority(const char *module, uint64_t id, uint32_t priority, cf_dyn_buf *db)
{
	if (priority == 0) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":priority value must be greater than zero");
		return AS_MON_ERR;
	}
	int retval = AS_MON_ERR;
	as_mon * mon_object = as_mon_get_module(module);

	if (!mon_object) {
		cf_warning(AS_MON, "Failed to find module %s", module);
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\" not found");
		return retval;
	}

	if (mon_object->cb.set_priority) {
		retval = mon_object->cb.set_priority(id, priority);

		if (retval == AS_MON_OK) {
			cf_dyn_buf_append_string(db, "OK");
		}
		else {
			cf_dyn_buf_append_string(db, "ERROR:");
			cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
			cf_dyn_buf_append_string(db, ":job not active");
		}
	}
	else {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":set-priority not supported for module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\"");
	}
	return retval;
}

/*
 * Calls the callback function to populate the stat of a particular job.
 *
 * Returns
 * 		AS_MON_OK - On success.
 * 		AS_MON_ERR - on failure.
 *
 */
int
as_mon_populate_jobstat(as_mon_jobstat * job_stat, cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "trid=");
	cf_dyn_buf_append_uint64(db, job_stat->trid);

	if (job_stat->job_type[0]) {
		cf_dyn_buf_append_string(db, ":job-type=");
		cf_dyn_buf_append_string(db, job_stat->job_type);
	}

	cf_dyn_buf_append_string(db, ":ns=");
	cf_dyn_buf_append_string(db, job_stat->ns);

	if (job_stat->set[0]) {
		cf_dyn_buf_append_string(db, ":set=");
		cf_dyn_buf_append_string(db, job_stat->set);
	}

	cf_dyn_buf_append_string(db, ":priority=");
	cf_dyn_buf_append_uint32(db, job_stat->priority);

	if (job_stat->status[0]) {
		cf_dyn_buf_append_string(db, ":status=");
		cf_dyn_buf_append_string(db, job_stat->status);
	}

	char progress_pct[8];
	sprintf(progress_pct, "%.2f", job_stat->progress_pct);

	cf_dyn_buf_append_string(db, ":job-progress=");
	cf_dyn_buf_append_string(db, progress_pct);

	cf_dyn_buf_append_string(db, ":run-time=");
	cf_dyn_buf_append_uint64(db, job_stat->run_time);

	cf_dyn_buf_append_string(db, ":time-since-done=");
	cf_dyn_buf_append_uint64(db, job_stat->time_since_done);

	cf_dyn_buf_append_string(db, ":recs-read=");
	cf_dyn_buf_append_uint64(db, job_stat->recs_read);

	cf_dyn_buf_append_string(db, ":net-io-bytes=");
	cf_dyn_buf_append_uint64(db, job_stat->net_io_bytes);

	//	char cpu_data[100];
	//	sprintf(cpu_data, "%f", job_stat->cpu);
	//	cf_dyn_buf_append_string(db, cpu_data);

	if (job_stat->jdata[0]) {
		cf_dyn_buf_append_string(db, job_stat->jdata);
	}

	return AS_MON_OK;
}

static int
as_mon_get_jobstat_reduce_fn(as_mon *mon_object, cf_dyn_buf *db)
{
	int size = 0;
	as_mon_jobstat * job_stats = NULL;
	if (mon_object->cb.get_jobstat_all) {
		job_stats = mon_object->cb.get_jobstat_all(&size);
	}

	// return OK to go to next module
	if (!job_stats) return AS_MON_OK;

	as_mon_jobstat * job;
	job = job_stats;

	for (int i = 0; i < size; i++) {
		cf_dyn_buf_append_string(db, "module=");
		cf_dyn_buf_append_string(db, mon_object->type);
		cf_dyn_buf_append_string(db, ":");
		as_mon_populate_jobstat(job, db);
		cf_dyn_buf_append_string(db, ";");
		job++;
	}
	cf_free(job_stats);
	return AS_MON_OK;
}

/*
 * This is called when the info call is triggered to get the info
 * about all the jobs.
 *
 * parameter:
 *     @db: in/out which gets populated. Each module stats is colon separated
 *          key:value and each module info is semicolon separated.
 *          e.g module:query:cpu:<val>:mem:<val>;module:query:cpu:<val>:mem:<val>;
 *
 * returns: 0 in case of success
 *          negative value in case of failure
 */
int
as_mon_get_jobstat_all(const char *module, cf_dyn_buf *db)
{
	bool found_module = false;
	for (int i = 0; i < g_as_mon_curr_mod_count; i++) {
		if ((module && !strcmp(g_as_mon_module[i]->type, module))
				|| (!module)) {
			as_mon_get_jobstat_reduce_fn(g_as_mon_module[i], db);
			if (module) {
				found_module = true;
			}
		}
	}

	if (module && !found_module) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\" not found");
	}
	else {
		cf_dyn_buf_chomp(db);
	}
	return 0;
}

/*
 * This is called when the info call is triggered to get the info
 * about a particular job in particular module.
 *
 * parameter:
 *     @db: in/out which gets populated. Each module stats is colon separated
 *          key:value and each module info is semicolon separated.
 *          e.g module:query:cpu:<val>:mem:<val>;module:query:cpu:<val>:mem:<val>;
 *
 * returns: 0 in case of success
 *          negative value in case of failure
 */
int
as_mon_get_jobstat(const char *module, uint64_t id, cf_dyn_buf *db)
{
	int      retval     = AS_MON_ERR;
	as_mon * mon_object = as_mon_get_module(module);;

	if (!mon_object) {
		cf_warning(AS_MON, "Failed to find module %s", module);
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\" not found");
		return retval;
	}

	as_mon_jobstat * job_stat = NULL;
	if (mon_object->cb.get_jobstat) {
		job_stat = mon_object->cb.get_jobstat(id);
	}
	else {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":get-job not supported for module \"");
		cf_dyn_buf_append_string(db, module);
		cf_dyn_buf_append_string(db, "\"");
		return retval;
	}

	if (job_stat) {
		retval = as_mon_populate_jobstat(job_stat, db);
		cf_free(job_stat);
	}
	else {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOT_FOUND);
		cf_dyn_buf_append_string(db, ":job not found");
	}
	return retval;
}

/*
 * Manipulates the monitor system.
 * Add, delete, reinit the modules.
 *
 */

void
as_mon_info_cmd(const char *module, char *cmd, uint64_t trid, uint32_t value, cf_dyn_buf *db)
{
	if (module == NULL) {
		as_mon_get_jobstat_all(NULL, db);
		return;
	}

	if (cmd == NULL) {
		as_mon_get_jobstat_all(module, db);
		return;
	}

	if (!strcmp(cmd, "get-job")) {
		as_mon_get_jobstat(module, trid, db);
	}
	else if (!strcmp(cmd, "kill-job")) {
		as_mon_killjob(module, trid, db);
	}
	else if (!strcmp(cmd, "set-priority")) {
		as_mon_set_priority(module, trid, value, db);
	}
	else {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":unrecognized command \"");
		cf_dyn_buf_append_string(db, cmd);
		cf_dyn_buf_append_string(db, "\"");
	}
}
