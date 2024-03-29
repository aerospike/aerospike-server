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

#include <inttypes.h>
#include <link.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucontext.h>
#include <unistd.h>

#include "cf_thread.h"
#include "enhanced_alloc.h"
#include "log.h"
#include "trace.h"

#include "warnings.h"


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

// Use our own - sys_siglist[] deprecated in glibc 2.31.
static const char* const signal_str[] = {
	[SIGINT] = "INT",
	[SIGQUIT] = "QUIT",
	[SIGILL] = "ILL",
	[SIGABRT] = "ABRT",
	[SIGBUS] = "BUS",
	[SIGFPE] = "FPE",
	[SIGUSR1] = "USR1",
	[SIGSEGV] = "SEGV"
};


//==========================================================
// Globals.
//

// The mutex that the main function deadlocks on after starting the service.
extern pthread_mutex_t g_main_deadlock;
extern bool g_startup_complete;


//==========================================================
// Forward declarations.
//

static void set_action(int sig_num, action_t act);
static void set_handler(int sig_num, sighandler_t hand);

static void sig_trace_and_reraise(int sig_num, siginfo_t* info, void* ctx);
static void sig_handle_hup(int sig_num, siginfo_t* info, void* ctx);
static void sig_handle_int(int sig_num, siginfo_t* info, void* ctx);
static void sig_handle_term(int sig_num, siginfo_t* info, void* ctx);

static void log_abort(int sig_num);
static void log_siginfo(const siginfo_t* info);
static const char* siginfo_code_str(const siginfo_t* info);
static void log_stack_trace(void* ctx);
static void log_memory(void* ctx);


//==========================================================
// Public API.
//

void as_signal_setup(void); // no signal.h, appease GCC

void
as_signal_setup(void)
{
	set_action(SIGABRT, sig_trace_and_reraise);
	set_action(SIGBUS, sig_trace_and_reraise);
	set_action(SIGFPE, sig_trace_and_reraise);
	set_action(SIGHUP, sig_handle_hup);
	set_action(SIGILL, sig_trace_and_reraise);
	set_action(SIGINT, sig_handle_int);
	set_action(SIGQUIT, sig_trace_and_reraise); // intentional trigger
	set_action(SIGSEGV, sig_trace_and_reraise);
	set_action(SIGTERM, sig_handle_term);
	set_action(SIGUSR1, sig_trace_and_reraise); // cf_crash() & cf_assert()
	set_action(SIGUSR2, cf_thread_traces_action);

	// Block SIGPIPE signal when there is some error while writing to pipe. The
	// write() call will return with a normal error which we can handle.
	set_handler(SIGPIPE, SIG_IGN);
}


//==========================================================
// Local helpers - set signal handlers.
//

static void
set_action(int sig_num, action_t act)
{
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));

	sa.sa_sigaction = act;
	sigemptyset(&sa.sa_mask);
	// SA_SIGINFO prefers sa_sigaction over sa_handler.
	sa.sa_flags = SA_RESTART | SA_SIGINFO;

	if (sigaction(sig_num, &sa, NULL) < 0) {
		cf_warning(AS_AS, "could not register signal handler for %d", sig_num);
		_exit(1);
	}
}

static void
set_handler(int sig_num, sighandler_t hand)
{
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));

	sa.sa_handler = hand;
	sigemptyset(&sa.sa_mask);
	// No SA_SIGINFO; use sa_handler.
	sa.sa_flags = SA_RESTART;

	if (sigaction(sig_num, &sa, NULL) < 0) {
		cf_warning(AS_AS, "could not register signal handler for %d", sig_num);
		_exit(1);
	}
}


//==========================================================
// Local helpers - signal handlers.
//

static void
sig_trace_and_reraise(int sig_num, siginfo_t* info, void* ctx)
{
	log_abort(sig_num);
	log_siginfo(info);

	if (g_crash_ctx_valid) {
		ctx = &g_crash_ctx;
	}

	log_stack_trace(ctx);
	log_memory(ctx);

	if (getpid() == 1) { // can happen in containers
		cf_warning(AS_AS, "pid 1 received %s - exiting", signal_str[sig_num]);
		_exit(1);
	}

	if (sig_num == SIGUSR1) {
		sig_num = SIGABRT;
	}

	set_handler(sig_num, SIG_DFL);
	raise(sig_num);
}

// This signal is our cue to roll the log.
static void
sig_handle_hup(int sig_num, siginfo_t* info, void* ctx)
{
	(void)sig_num;
	(void)info;
	(void)ctx;

	cf_info(AS_AS, "SIGHUP received, rolling log");

	cf_log_rotate();
}

// We get here on cf_crash_nostack(), cf_assert_nostack(), or Ctrl-C when
// running in foreground.
static void
sig_handle_int(int sig_num, siginfo_t* info, void* ctx)
{
	(void)info;
	(void)ctx;

	log_abort(sig_num);

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(1);
	}

	pthread_mutex_unlock(&g_main_deadlock);
}

// We get here on normal shutdown.
static void
sig_handle_term(int sig_num, siginfo_t* info, void* ctx)
{
	(void)sig_num;
	(void)info;
	(void)ctx;

	cf_info(AS_AS, "SIGTERM received, shutting down %s build %s os %s arch %s sha %.7s%s%.7s",
			aerospike_build_type, aerospike_build_id, aerospike_build_os,
			aerospike_build_arch, aerospike_build_sha,
			*aerospike_build_ee_sha == '\0' ? "" : " ee-sha ",
			*aerospike_build_ee_sha == '\0' ? "" : aerospike_build_ee_sha);

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(0);
	}

	pthread_mutex_unlock(&g_main_deadlock);
}


//==========================================================
// Local helpers - logging.
//

static void
log_abort(int sig_num)
{
	cf_warning(AS_AS, "SIG%s received, aborting %s build %s os %s arch %s sha %.7s%s%.7s",
			signal_str[sig_num], aerospike_build_type, aerospike_build_id,
			aerospike_build_os, aerospike_build_arch, aerospike_build_sha,
			*aerospike_build_ee_sha == '\0' ? "" : " ee-sha ",
			*aerospike_build_ee_sha == '\0' ? "" : aerospike_build_ee_sha);
}

static void
log_siginfo(const siginfo_t* info)
{
	char buf[512];
	char* at = buf;

	at += sprintf(at, "si_code %s (%d)", siginfo_code_str(info), info->si_code);

	if (info->si_signo == SIGILL || info->si_signo == SIGBUS ||
			info->si_signo == SIGFPE || info->si_signo == SIGSEGV) {
		at += sprintf(at, " si_addr 0x%016lx", (uintptr_t)info->si_addr);
	}

	if (info->si_code == SI_USER) {
		at += sprintf(at, " si_uid %d si_pid %u", info->si_uid, info->si_pid);
	}

	if (info->si_signo == SIGBUS && (info->si_code == BUS_MCEERR_AR ||
			info->si_code == BUS_MCEERR_AO)) {
		at += sprintf(at, " si_addr_lsb 0x%04x", info->si_addr_lsb);
	}

	if (info->si_signo == SIGSEGV && info->si_code == SEGV_BNDERR) {
		sprintf(at, " si_lower 0x%016lx si_upper 0x%016lx",
				(uintptr_t)info->si_lower, (uintptr_t)info->si_upper);
	}

	cf_warning(AS_AS, "%s", buf);
}

static const char*
siginfo_code_str(const siginfo_t* info)
{
	// Strings placed corresponding to the numeric value of the constant. See:
	// https://github.com/torvalds/linux/blob/master/include/uapi/asm-generic/siginfo.h
	static const char* const cs_all[] = { "SI_USER", "SI_QUEUE", "SI_TIMER",
			"SI_MESGQ", "SI_ASYNCIO", "SI_SIGIO", "SI_TKILL" };
	static const char* const cs_ill[] = { 0, "ILL_ILLOPC", "ILL_ILLOPN",
			"ILL_ILLADR", "ILL_ILLTRP", "ILL_PRVOPC", "ILL_COPROC",
			"ILL_BADSTK", "ILL_BADIADDR" };
	static const char* const cs_bus[] = { 0, "BUS_ADRALN", "BUS_ADRERR",
			"BUS_OBJERR", "BUS_MCEERR_AR", "BUS_MCEERR_AO" };
	static const char* const cs_fpe[] = { 0, "FPE_INTDIV", "FPE_INTOVF",
			"FPE_FLTDIV", "FPE_FLTOVF", "FPE_FLTUND", "FPE_FLTRES",
			"FPE_FLTINV", "FPE_FLTSUB" };
	static const char* const cs_segv[] = { 0, "SEGV_MAPERR", "SEGV_ACCERR",
			"SEGV_BNDERR", "SEGV_PKUERR" };
	static const char* const* sig_cs[] = {
			[SIGILL] = cs_ill,
			[SIGBUS] = cs_bus,
			[SIGFPE] = cs_fpe,
			[SIGSEGV] = cs_segv
	};
	static const int32_t cs_len[] = {
			[SIGILL] = sizeof(cs_ill) / sizeof(char*),
			[SIGBUS] = sizeof(cs_bus) / sizeof(char*),
			[SIGFPE] = sizeof(cs_fpe) / sizeof(char*),
			[SIGSEGV] = sizeof(cs_segv) / sizeof(char*)
	};

	if (info->si_code == SI_KERNEL) {
		return "SI_KERNEL";
	}

	if (info->si_code <= 0) {
		int32_t code = -info->si_code;

		return code < (int32_t)(sizeof(cs_all) / sizeof(char*)) ?
				cs_all[code] : "(unimpl)";
	}

	if (info->si_signo == SIGILL || info->si_signo == SIGBUS ||
			info->si_signo == SIGFPE || info->si_signo == SIGSEGV) {
		int32_t signo = info->si_signo;
		int32_t code = info->si_code;

		return code < cs_len[signo] ? sig_cs[signo][code] : "(unimpl)";
	}

	// Only if si_signo is SIGTRAP, SIGCHLD, SIGIO/SIGPOLL, or SIGSYS.
	cf_warning(AS_AS, "called from unexpected signal handler");
	return "(unimpl)";
}

// Unlike cf_log_stack_trace(), includes registers and addresses.
static void
log_stack_trace(void* ctx)
{
	ucontext_t* uc = (ucontext_t*)ctx;
	mcontext_t* mc = (mcontext_t*)&uc->uc_mcontext;
	char reg_str[1000];
#if defined __x86_64__
	uint64_t* gregs = (uint64_t*)&mc->gregs[0];

	sprintf(reg_str,
		"rax %016lx rbx %016lx rcx %016lx rdx %016lx rsi %016lx rdi %016lx "
		"rbp %016lx rsp %016lx r8 %016lx r9 %016lx r10 %016lx r11 %016lx "
		"r12 %016lx r13 %016lx r14 %016lx r15 %016lx rip %016lx",
		gregs[REG_RAX], gregs[REG_RBX], gregs[REG_RCX], gregs[REG_RDX],
		gregs[REG_RSI], gregs[REG_RDI], gregs[REG_RBP], gregs[REG_RSP],
		gregs[REG_R8], gregs[REG_R9], gregs[REG_R10], gregs[REG_R11],
		gregs[REG_R12], gregs[REG_R13], gregs[REG_R14], gregs[REG_R15],
		gregs[REG_RIP]);
#elif defined __aarch64__
	uint64_t fault = (uint64_t)mc->fault_address;
	uint64_t* regs = (uint64_t*)&mc->regs[0];
	uint64_t sp = (uint64_t)mc->sp;
	uint64_t pc = (uint64_t)mc->pc;

	char* p = reg_str;

	p += sprintf(p, "fault %016lx ", fault);

	for (uint32_t i = 0; i < 31; i++) {
		p += sprintf(p, "x%u %016lx ", i, regs[i]);
	}

	sprintf(p, "x31 %016lx pc %016lx", sp, pc);
#endif

	cf_warning(AS_AS, "stacktrace: registers: %s", reg_str);

	void* bt[MAX_BACKTRACE_DEPTH];
	int n_frames = cf_backtrace(bt, MAX_BACKTRACE_DEPTH);

	char trace[MAX_BACKTRACE_DEPTH * 20];
	char* at = trace;

	for (int i = 0; i < n_frames; i++) {
		at += sprintf(at, " 0x%lx", cf_strip_aslr(bt[i]));
	}

	cf_warning(AS_AS, "stacktrace: found %d frames:%s offset 0x%lx", n_frames,
			trace, _r_debug.r_map->l_addr);

	for (int i = 0; i < n_frames; i++) {
		char sym_str[SYM_STR_MAX_SZ];

		cf_addr_to_sym_str(sym_str, bt[i]);
		cf_warning(AS_AS, "stacktrace: frame %d: %s [0x%lx]", i, sym_str,
				cf_strip_aslr(bt[i]));
	}
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

		char buf[3 * sz + 1];

		for (uint32_t k = 0; k < sz; k++) {
			sprintf(buf + 3 * k, " %02x", ((uint8_t*)p)[k]);
		}

		cf_warning(AS_AS, "stacktrace: memory %s 0x%lx%s", names[i],
			(uint64_t)p, buf);
	}
}
