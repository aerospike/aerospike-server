/*
 * daemon.c
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
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
 * process utilities
 */

#include "daemon.h"

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/capability.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "fault.h"

extern int capset(cap_user_header_t header, cap_user_data_t data);


static bool g_hold_caps = false;
static bool g_clear_caps = false;


void
cf_process_privsep(uid_t uid, gid_t gid)
{
	if (0 != getuid() || (uid == getuid() && gid == getgid())) {
		return;
	}

	// If appropriate, make all capabilities survive the UID/GID switch.
	if (g_hold_caps) {
		if (0 > prctl(PR_SET_KEEPCAPS, 1, 0, 0, 0)) {
			cf_crash(CF_MISC, "prctl: %s", cf_strerror(errno));
		}

		g_clear_caps = true;
	}

	// Drop all auxiliary groups.
	if (0 > setgroups(0, (const gid_t *)0)) {
		cf_crash(CF_MISC, "setgroups: %s", cf_strerror(errno));
	}

	// Change privileges.
	if (0 > setgid(gid)) {
		cf_crash(CF_MISC, "setgid: %s", cf_strerror(errno));
	}

	if (0 > setuid(uid)) {
		cf_crash(CF_MISC, "setuid: %s", cf_strerror(errno));
	}
}


// TODO - if we get more customers of this API, we could switch to either using
// a 'hold counter', or a more involved scheme where individual capabilities can
// be kept and revoked.

void
cf_process_holdcap(void)
{
	g_hold_caps = true;
}


void
cf_process_clearcap(void)
{
	if (! g_clear_caps) {
		return;
	}

	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_2
	};

	struct __user_cap_data_struct cap_data[2] = { { 0 } };

	if (0 > capset(&cap_head, cap_data)) {
		cf_crash(CF_MISC, "capset: %s", cf_strerror(errno));
	}
}


// Daemonize the server - fork a new child process and exit the parent process.
// Close all the file descriptors opened except the ones specified in the
// fd_ignore_list. Redirect console messages to a file.
void
cf_process_daemonize(int *fd_ignore_list, int list_size)
{
	int FD, j;
	char cfile[128];
	pid_t p;

	// Fork ourselves, then let the parent expire.
	if (-1 == (p = fork())) {
		cf_crash(CF_MISC, "couldn't fork: %s", cf_strerror(errno));
	}

	if (0 != p) {
		// Prefer _exit() over exit(), as we don't want the parent to
		// do any cleanups.
		_exit(0);
	}

	// Get a new session.
	if (-1 == setsid()) {
		cf_crash(CF_MISC, "couldn't set session: %s", cf_strerror(errno));
	}

	// Drop all the file descriptors except the ones in fd_ignore_list.
	for (int i = getdtablesize(); i > 2; i--) {
		for (j = 0; j < list_size; j++) {
			if (fd_ignore_list[j] == i) {
				break;
			}
		}

		if (j ==  list_size) {
			close(i);
		}
	}

	// Open a temporary file for console message redirection.
	snprintf(cfile, 128, "/tmp/aerospike-console.%d", getpid());

	if (-1 == (FD = open(cfile, O_WRONLY|O_CREAT|O_APPEND, S_IRUSR|S_IWUSR))) {
		cf_crash(CF_MISC, "couldn't open console redirection file %s: %s", cfile, cf_strerror(errno));
	}

	if (-1 == chmod(cfile, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)) {
		cf_crash(CF_MISC, "couldn't set mode on console redirection file %s: %s", cfile, cf_strerror(errno));
	}

	// Redirect stdout, stderr, and stdin to the console file.
	for (int i = 0; i < 3; i++) {
		if (-1 == dup2(FD, i)) {
			cf_crash(CF_MISC, "couldn't duplicate FD: %s", cf_strerror(errno));
		}
	}
}
