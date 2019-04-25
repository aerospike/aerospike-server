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
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/capability.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "fault.h"

extern int capset(cap_user_header_t header, cap_user_data_t data);
extern int capget(cap_user_header_t header, cap_user_data_t data);


static uint32_t g_startup_caps[2] = { 0, 0 }; // held during startup
static uint32_t g_runtime_caps[2] = { 0, 0 }; // held forever
static bool g_kept_caps = false;


void
cf_process_privsep(uid_t uid, gid_t gid)
{
	uid_t curr_uid = geteuid();
	gid_t curr_gid = getegid();

	// In case people use the setuid/setgid bit on the asd executable.
	CF_NEVER_FAILS(setuid(curr_uid));
	CF_NEVER_FAILS(setgid(curr_gid));

	// To see this log message, change NO_SINKS_LIMIT in fault.c.
	cf_info(AS_AS, "user %d, group %d -> user %d, group %d", curr_uid, curr_gid,
			uid, gid);

	// Not started as root: may switch neither user nor group.

	if (curr_uid != 0) {
		if (uid != (uid_t)-1 && uid != curr_uid) {
			cf_crash_nostack(CF_MISC,
					"insufficient privileges to switch user to %d", uid);
		}

		if (gid != (gid_t)-1 && gid != curr_gid) {
			cf_crash_nostack(CF_MISC,
					"insufficient privileges to switch group to %d", gid);
		}

		return;
	}

	// Started as root and staying root: may switch group.

	if (uid == (uid_t)-1 || uid == 0) {
		if (gid == (gid_t)-1) {
			return;
		}

		if (setgroups(0, (const gid_t *)0) < 0) {
			cf_crash(CF_MISC, "setgroups: %s", cf_strerror(errno));
		}

		if (setgid(gid) < 0) {
			cf_crash(CF_MISC, "setgid: %s", cf_strerror(errno));
		}

		return;
	}

	// Started as root and not staying root: switch user (group), keep caps.

	uint32_t caps[2] = {
		g_startup_caps[0] | g_runtime_caps[0],
		g_startup_caps[1] | g_runtime_caps[1]
	};

	// If required, make capabilities survive the UID/GID switch.

	if (caps[0] != 0 || caps[1] != 0) {
		if (prctl(PR_SET_KEEPCAPS, 1, 0, 0, 0) < 0) {
			cf_crash(CF_MISC, "prctl: %s", cf_strerror(errno));
		}

		g_kept_caps = true;
	}

	if (gid != (gid_t)-1) {
		if (setgroups(0, (const gid_t *)0) < 0) {
			cf_crash(CF_MISC, "setgroups: %s", cf_strerror(errno));
		}

		if (setgid(gid) < 0) {
			cf_crash(CF_MISC, "setgid: %s", cf_strerror(errno));
		}
	}

	if (setuid(uid) < 0) {
		cf_crash(CF_MISC, "setuid: %s", cf_strerror(errno));
	}

	// If we made the capabilities survive, reduce them to the desired set.

	if (! g_kept_caps) {
		return;
	}

	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_3
	};

	struct __user_cap_data_struct cap_data[2] = {
		{ .permitted = caps[0], .effective = caps[0] },
		{ .permitted = caps[1], .effective = caps[1] }
	};

	if (capset(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capset: %s", cf_strerror(errno));
	}
}


void
cf_process_add_startup_cap(int cap)
{
	g_startup_caps[cap >> 5] = 1 << (cap & 0x1f);
}


void
cf_process_add_runtime_cap(int cap)
{
	g_runtime_caps[cap >> 5] = 1 << (cap & 0x1f);
}


void
cf_process_drop_startup_caps(void)
{
	if (! g_kept_caps) {
		return;
	}

	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_3
	};

	struct __user_cap_data_struct cap_data[2] = {
		{ .permitted = g_runtime_caps[0] },
		{ .permitted = g_runtime_caps[1] }
	};

	if (capset(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capset: %s", cf_strerror(errno));
	}
}


bool
cf_process_has_cap(int cap)
{
	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_3
	};

	struct __user_cap_data_struct cap_data[2];

	if (capget(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capget: %s", cf_strerror(errno));
	}

	uint32_t perm = cap_data[cap >> 5].permitted;

	return (perm & (1 << (cap & 0x1f))) != 0;
}


void
cf_process_enable_cap(int cap)
{
	if (! g_kept_caps) {
		return;
	}

	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_3
	};

	struct __user_cap_data_struct cap_data[2];

	if (capget(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capget: %s", cf_strerror(errno));
	}

	cap_data[cap >> 5].effective |= 1 << (cap & 0x1f);

	if (capset(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capset: %s", cf_strerror(errno));
	}
}


void
cf_process_disable_cap(int cap)
{
	if (! g_kept_caps) {
		return;
	}

	struct __user_cap_header_struct cap_head = {
		.version = _LINUX_CAPABILITY_VERSION_3
	};

	struct __user_cap_data_struct cap_data[2];

	if (capget(&cap_head, cap_data) < 0) {
		cf_crash(CF_MISC, "capget: %s", cf_strerror(errno));
	}

	cap_data[cap >> 5].effective &= ~(1 << (cap & 0x1f));

	if (capset(&cap_head, cap_data) < 0) {
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
