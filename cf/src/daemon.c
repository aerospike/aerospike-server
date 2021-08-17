/*
 * daemon.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

#include "log.h"
#include "os.h"

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
// Redirect console messages to a file.
void
cf_process_daemonize(void)
{
	pid_t child = fork();

	if (child < 0) {
		cf_crash(CF_MISC, "failed fork: %d (%s)", errno, cf_strerror(errno));
	}

	if (child != 0) {
		// Prefer _exit() over exit() - prevent cleanups by parent.
		_exit(0);
	}
	// else - in child.

	// Get a new session.
	if (setsid() < 0) {
		cf_crash(CF_MISC, "failed setsid: %d (%s)", errno, cf_strerror(errno));
	}

	char path[64];

	sprintf(path, "/tmp/aerospike-console.%d", getpid());

	int fd = open(path, CF_LOG_OPEN_FLAGS, cf_os_log_perms());

	if (fd < 0) {
		cf_crash(CF_MISC, "failed to open %s: %d (%s)", path, errno,
				cf_strerror(errno));
	}

	if (dup2(fd, STDOUT_FILENO) < 0) {
		cf_crash(CF_MISC, "failed to redirect stdout: %d (%s)", errno,
				cf_strerror(errno));
	}

	if (dup2(fd, STDERR_FILENO) < 0) {
		cf_crash(CF_MISC, "failed to redirect stderr: %d (%s)", errno,
				cf_strerror(errno));
	}

	close(fd);

	fd = open("/dev/null", O_RDONLY);

	if (fd < 0) {
		cf_crash(CF_MISC, "failed to open /dev/null: %d (%s)", errno,
				cf_strerror(errno));
	}

	if (dup2(fd, STDIN_FILENO) < 0) {
		cf_crash(CF_MISC, "failed to redirect stdin: %d (%s)", errno,
				cf_strerror(errno));
	}

	close(fd);
}
