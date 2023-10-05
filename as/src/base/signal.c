/*
 * signal.c
 *
 * Copyright (C) 2010-2023 Aerospike, Inc.
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

#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cf_thread.h"
#include "enhanced_alloc.h"
#include "log.h"


//==========================================================
// Typedefs & constants.
//

typedef void (*action_t)(int sig, siginfo_t* info, void* ctx);

// String constants in version.c, generated by make.
extern const char aerospike_build_type[];
extern const char aerospike_build_id[];
extern const char aerospike_build_os[];
extern const char aerospike_build_arch[];
extern const char aerospike_build_sha[];
extern const char aerospike_build_ee_sha[];


//==========================================================
// Globals.
//

// The mutex that the main function deadlocks on after starting the service.
extern pthread_mutex_t g_main_deadlock;
extern bool g_startup_complete;


//==========================================================
// Local helpers.
//

static inline void
set_action(int sig_num, action_t act)
{
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));

	sa.sa_sigaction = act;
	sigemptyset(&sa.sa_mask);
	// SA_SIGINFO prefers sa_sigaction over sa_handler.
	sa.sa_flags = SA_RESTART | SA_SIGINFO;

	if (sigaction(sig_num, &sa, NULL) < 0) {
		cf_crash(AS_AS, "could not register signal handler for %d", sig_num);
	}
}

static inline void
set_handler(int sig_num, sighandler_t hand)
{
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));

	sa.sa_handler = hand;
	sigemptyset(&sa.sa_mask);
	// No SA_SIGINFO; use sa_handler.
	sa.sa_flags = SA_RESTART;

	if (sigaction(sig_num, &sa, NULL) < 0) {
		cf_crash(AS_AS, "could not register signal handler for %d", sig_num);
	}
}

static inline void
reraise_signal(int sig_num)
{
	if (getpid() == 1) {
		cf_warning(AS_AS, "pid 1 received signal %d - exiting", sig_num);
		_exit(1);
	}

	set_handler(sig_num, SIG_DFL);
	raise(sig_num);
}

static inline void
log_abort(const char* signal)
{
	cf_warning(AS_AS, "%s received, aborting %s build %s os %s arch %s sha %.7s%s%.7s",
			signal, aerospike_build_type, aerospike_build_id,
			aerospike_build_os, aerospike_build_arch,
			aerospike_build_sha,
			*aerospike_build_ee_sha == '\0' ? "" : " ee-sha ",
			*aerospike_build_ee_sha == '\0' ? "" : aerospike_build_ee_sha);
}

static void
log_memory(void* ctx)
{
	ucontext_t* uc = (ucontext_t*)ctx;
	mcontext_t* mc = (mcontext_t*)&uc->uc_mcontext;
#if defined __x86_64__
	uint64_t* gregs = (uint64_t*)&mc->gregs[0];

	static const char* names[16] = {
		"rax", "rbx", "rcx", "rdx", "rsi", "rdi", "rbp", "rsp", "r8", "r9",
		"r10", "r11", "r12", "r13", "r14", "r15"
	};

	uint64_t values[16] = {
		gregs[REG_RAX], gregs[REG_RBX], gregs[REG_RCX], gregs[REG_RDX],
		gregs[REG_RSI], gregs[REG_RDI], gregs[REG_RBP], gregs[REG_RSP],
		gregs[REG_R8], gregs[REG_R9], gregs[REG_R10], gregs[REG_R11],
		gregs[REG_R12], gregs[REG_R13], gregs[REG_R14], gregs[REG_R15]
	};

	uint32_t n_regs = 16;
#elif defined __aarch64__
	uint64_t* regs = (uint64_t*)&mc->regs[0];
	uint64_t sp = (uint64_t)mc->sp;

	static const char* names[32] = {
		"x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10",
		"x11", "x12", "x13", "x14", "x15", "x16", "x17", "x18", "x19", "x20",
		"x21", "x22", "x23", "x24", "x25", "x26", "x27", "x28", "x29", "x30",
		"x31"
	};

	uint64_t values[32];

	for (uint32_t i = 0; i < 31; i++) {
		values[i] = regs[i];
	}

	values[31] = sp;

	uint32_t n_regs = 32;
#endif

	for (uint32_t i = 0; i < n_regs; i++) {
		if (values[i] < 64) {
			continue;
		}

		void* p = (void*)((values[i] & -16ul) - 64);
		size_t sz = 64 + 16 + 64;

		cf_trim_region_to_mapped(&p, &sz);

		if (sz == 0) {
			continue;
		}

		char buff[3 * sz + 1];

		for (uint32_t k = 0; k < sz; k++) {
			sprintf(buff + 3 * k, " %02x", ((uint8_t*)p)[k]);
		}

		cf_warning(AS_AS, "stacktrace: memory %s 0x%lx%s", names[i],
			(uint64_t)p, buff);
	}
}


//==========================================================
// Signal handlers.
//

// We get here on some crashes.
static void
as_sig_handle_abort(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGABRT");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

static void
as_sig_handle_bus(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGBUS");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

// Floating point exception.
static void
as_sig_handle_fpe(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGFPE");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

// This signal is our cue to roll the log.
static void
as_sig_handle_hup(int sig_num, siginfo_t* info, void* ctx)
{
	cf_info(AS_AS, "SIGHUP received, rolling log");

	cf_log_rotate();
}

// We get here on some crashes.
static void
as_sig_handle_ill(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGILL");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

// We get here on cf_crash_nostack(), cf_assert_nostack(), or Ctrl-C when
// running in foreground.
static void
as_sig_handle_int(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGINT");

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(1);
	}

	pthread_mutex_unlock(&g_main_deadlock);
}

// We get here if we intentionally trigger the signal.
static void
as_sig_handle_quit(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGQUIT");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

// We get here on some crashes.
static void
as_sig_handle_segv(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort("SIGSEGV");
	cf_log_stack_trace(ctx);
	log_memory(ctx);
	reraise_signal(sig_num);
}

// We get here on normal shutdown.
static void
as_sig_handle_term(int sig_num, siginfo_t* info, void* ctx)
{
	cf_info(AS_AS, "SIGTERM received, shutting down %s build %s os %s arch %s sha %.7s%s%.7s",
			aerospike_build_type, aerospike_build_id,
			aerospike_build_os, aerospike_build_arch,
			aerospike_build_sha,
			*aerospike_build_ee_sha == '\0' ? "" : " ee-sha ",
			*aerospike_build_ee_sha == '\0' ? "" : aerospike_build_ee_sha);

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(0);
	}

	pthread_mutex_unlock(&g_main_deadlock);
}

// We get here on cf_crash() and cf_assert().
static void
as_sig_handle_usr1(int sig_num, siginfo_t* info, void* ctx)
{
	(void)ctx;

	log_abort("SIGUSR1");
	cf_log_stack_trace(&g_crash_ctx);
	log_memory(&g_crash_ctx);
	reraise_signal(SIGABRT);
}


//==========================================================
// Public API.
//

void
as_signal_setup()
{
	set_action(SIGABRT, as_sig_handle_abort);
	set_action(SIGBUS, as_sig_handle_bus);
	set_action(SIGFPE, as_sig_handle_fpe);
	set_action(SIGHUP, as_sig_handle_hup);
	set_action(SIGILL, as_sig_handle_ill);
	set_action(SIGINT, as_sig_handle_int);
	set_action(SIGQUIT, as_sig_handle_quit);
	set_action(SIGSEGV, as_sig_handle_segv);
	set_action(SIGTERM, as_sig_handle_term);
	set_action(SIGUSR1, as_sig_handle_usr1);
	set_action(SIGUSR2, cf_thread_traces_action);

	// Block SIGPIPE signal when there is some error while writing to pipe. The
	// write() call will return with a normal error which we can handle.
	set_handler(SIGPIPE, SIG_IGN);
}
