// vim: set noet ts=4 sw=4:

/*
 * tso.cc
 *
 * Copyright (C) 2022 Aerospike, Inc.
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

#include <gcc-plugin.h>
#include <plugin-version.h>

#include <context.h>
#include <tree.h>
#include <tree-pass.h>
#include <print-tree.h>
#include <line-map.h>

#include <gimple.h>
#include <gimple-expr.h>
#include <gimple-iterator.h>
#include <gimple-walk.h>
#include <gimple-pretty-print.h>

#include <basic-block.h>

#pragma GCC diagnostic warning "-Wshadow"
#pragma GCC diagnostic warning "-Wcast-align"
#pragma GCC diagnostic warning "-Wcast-qual"
#pragma GCC diagnostic warning "-Wconversion"
#pragma GCC diagnostic warning "-Wsign-conversion"
#pragma GCC diagnostic warning "-Wmissing-declarations"
#pragma GCC diagnostic warning "-Wredundant-decls"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>


//==========================================================
// Typedefs & constants.
//

#define DEBUG 0
#define COUNT_BARRIER "cf_tso_count_barrier"
#define RELAXED 0
#define SEQ_CST 5

class tso_1 : public gimple_opt_pass
{
public:
	tso_1(const pass_data& pd) : gimple_opt_pass(pd, g) {}
	virtual tso_1* clone() override;
	virtual unsigned int execute(function* f) override;
};

class tso_2 : public gimple_opt_pass
{
public:
	tso_2(const pass_data& pd) : gimple_opt_pass(pd, g) {}
	virtual tso_2* clone() override;
	virtual unsigned int execute(function* f) override;
};


//==========================================================
// Globals.
//

int plugin_is_GPL_compatible;

static bool g_enable = true;
static std::set<std::string> g_excluded;
static bool g_track_deps = true;
static bool g_fix_asm = true;
static bool g_fix_built_in = true;
static bool g_profiling = false;


//==========================================================
// Forward declarations.
//

static bool handle_argument(const plugin_argument* arg);
static bool add_excluded(const std::string& path);
static std::string strip_line(const std::string& line);
static bool set_boolean(bool& b, const std::string& value);
static std::string strip_path(const std::string& path);
static bool skip_function(const std::string& top_file, const std::string& file,
		const std::string& ident);
static bool is_excluded(const std::string& key);
static bool gimple_needs_ordering(const gimple* st);
static bool tree_needs_ordering(const tree t);
static bool gimple_depends(const gimple* st, std::vector<tree>& deps, bool ordered);
static bool tree_contains(const tree t, const tree x);
static void insert_barrier(gimple_stmt_iterator& gsi, location_t loc);
static gcall* make_barrier(void);
static gcall* make_profiling_call(void);
static bool is_barrier(const gimple* st);
static void neutralize_barrier(gimple* st);
#if DEBUG > 0
static void debug_func(const std::string& top_file, const std::string& file,
		const std::string& ident);
static void debug_stmt(/* const */ gimple* st, const std::vector<tree>& deps);
static void debug_stmt(/* const */ gimple* st);
#endif
#if DEBUG > 1
static void debug_ops(const gimple* st);
#endif


//==========================================================
// Public API.
//

int
plugin_init(plugin_name_args* info, plugin_gcc_version* ver)
{
	if (! plugin_default_version_check (ver, &gcc_version)) {
		return 1;
	}

	for (int i = 0; i < info->argc; ++i) {
		plugin_argument* arg = info->argv + i;

		if (! handle_argument(arg)) {
			return 1;
		}
	}

	pass_data pd_1 = {
		.type = GIMPLE_PASS,
		.name = "tso_1",
		.optinfo_flags = OPTGROUP_NONE,
		.tv_id = TV_NONE,
		.properties_required = PROP_gimple_lcf,
		.properties_provided = 0,
		.properties_destroyed = 0,
		.todo_flags_start = 0,
		.todo_flags_finish = 0
	};

	pass_data pd_2 = {
		.type = GIMPLE_PASS,
		.name = "tso_2",
		.optinfo_flags = OPTGROUP_NONE,
		.tv_id = TV_NONE,
		.properties_required = PROP_cfg,
		.properties_provided = 0,
		.properties_destroyed = 0,
		.todo_flags_start = 0,
		.todo_flags_finish = 0
	};

	register_pass_info pi_1 = {
		.pass = new tso_1(pd_1),
		.reference_pass_name = "cfg",
		.ref_pass_instance_number = 1,
		.pos_op = PASS_POS_INSERT_BEFORE
	};

	register_pass_info pi_2 = {
		.pass = new tso_2(pd_2),
		.reference_pass_name = "sanopt",
		.ref_pass_instance_number = 1,
		.pos_op = PASS_POS_INSERT_AFTER
	};

	register_callback(info->base_name, PLUGIN_PASS_MANAGER_SETUP, NULL, &pi_1);
	register_callback(info->base_name, PLUGIN_PASS_MANAGER_SETUP, NULL, &pi_2);

	return 0;
}

tso_1*
tso_1::clone() {
	return this;
}

unsigned int
tso_1::execute(function* fn)
{
	tree decl = fn->decl;
	const std::string top_path{main_input_filename};
	const std::string& top_file = strip_path(top_path);
	const std::string path{DECL_SOURCE_FILE(decl)};
	const std::string& file = strip_path(path);
	const std::string ident{fndecl_name(decl)};

#if DEBUG > 0
	debug_func(top_file, file, ident);
#endif

	if (! g_enable || skip_function(top_file, file, ident)) {
		return 0;
	}

	gimple_seq& body = fn->gimple_body;
	gimple_stmt_iterator gsi = gsi_start(body);
	std::vector<tree> deps;

	while (! gsi_end_p(gsi)) {
		gimple* st = gsi_stmt(gsi);

		if (gimple_has_substatements(st)) {
			std::cerr << "substatements not supported" << std::endl;
			::abort();
		}

#if DEBUG > 0
		debug_stmt(st, deps);
#endif

		bool ordered = gimple_needs_ordering(st);
		bool depends = g_track_deps && gimple_depends(st, deps, ordered);

		if (ordered) {
			if (depends) {
#if DEBUG > 0
				std::cerr << "  depends" << std::endl;
#endif
			}
			else {
				location_t loc = gimple_location(st);

				insert_barrier(gsi, loc);
#if DEBUG > 0
				std::cerr << "  barrier" << std::endl;
#endif
			}
		}

#if DEBUG > 1
		debug_ops(st);
#endif

		gsi_next(&gsi);
	}

	return 0;
}

tso_2*
tso_2::clone() {
	return this;
}

unsigned int
tso_2::execute(function* fn)
{
	tree decl = fn->decl;
	const std::string top_path{main_input_filename};
	const std::string& top_file = strip_path(top_path);
	const std::string path{DECL_SOURCE_FILE(decl)};
	const std::string& file = strip_path(path);
	const std::string ident{fndecl_name(decl)};

#if DEBUG > 0
	debug_func(top_file, file, ident);
#endif

	if (! g_enable || skip_function(top_file, file, ident)) {
		return 0;
	}

	basic_block bb;

	FOR_ALL_BB_FN(bb, fn) {
		gimple_stmt_iterator gsi = gsi_start_nondebug_bb(bb);
		bool safe = false;

		while (! gsi_end_p(gsi)) {
			gimple* st = gsi_stmt(gsi);

#if DEBUG > 0
			debug_stmt(st);
#endif

			if (is_barrier(st)) {
				if (safe) {
					neutralize_barrier(st);
#if DEBUG > 0
					std::cerr << "  neutralize" << std::endl;
#endif
				}
				else {
					safe = true;
#if DEBUG > 0
					std::cerr << "  keep" << std::endl;
#endif
				}
			}
			else {
				safe = false;
			}

			gsi_next_nondebug(&gsi);
		}
	}

	return 0;
}

//==========================================================
// Local helpers.
//

static bool
handle_argument(const plugin_argument* arg)
{
	const std::string key{arg->key};
	const std::string value{arg->value};

	if (key == "enable") {
		return set_boolean(g_enable, value);
	}

	if (key == "exclude") {
		return add_excluded(value);
	}

	if (key == "track-deps") {
		return set_boolean(g_track_deps, value);
	}

	if (key == "fix-asm") {
		return set_boolean(g_fix_asm, value);
	}

	if (key == "fix-built-in") {
		return set_boolean(g_fix_built_in, value);
	}

	if (key == "profiling") {
		return set_boolean(g_profiling, value);
	}

	std::cerr << "invalid plugin argument \"" << key << "\"" << std::endl;
	return false;
}

static bool
add_excluded(const std::string& path)
{
	std::ifstream ifs{path};

	if (! ifs.is_open()) {
		std::cerr << "failed to open \"" << path << "\"" << std::endl;
		return false;
	}

	std::string line;

	while (std::getline(ifs, line)) {
		const std::string& stripped = strip_line(line);

		if (stripped.size() != 0) {
			g_excluded.insert(stripped);
		}
	}

	return true;
}

static std::string
strip_line(const std::string& line)
{
	size_t i_comment = line.find('#');
	const std::string& no_comment = line.substr(0, i_comment);
	size_t i_begin = no_comment.find_first_not_of(" \t");

	if (i_begin == std::string::npos) {
		return "";
	}

	size_t i_end = no_comment.find_last_not_of(" \t");

	return std::string{no_comment.substr(i_begin, i_end + 1 - i_begin)};
}

static bool
set_boolean(bool& b, const std::string& value)
{
	if (value == "on" || value == "yes" || value == "1") {
		b = true;
		return true;
	}

	if (value == "off" || value == "no" || value == "0") {
		b = false;
		return true;
	}

	std::cerr << "invalid Boolean \"" << value << "\"" << std::endl;
	return false;
}

static std::string
strip_path(const std::string& path)
{
	size_t i = path.rfind('/');

	return i == std::string::npos ?
			path : path.substr(i + 1, std::string::npos);
}

static bool
skip_function(const std::string& top_file, const std::string& file,
		const std::string& ident)
{
	if (ident == COUNT_BARRIER) {
		return true;
	}

	if (is_excluded(file) || is_excluded(ident)) {
		return true;
	}

	if (is_excluded(file + ":" + ident)) {
		return true;
	}

	if (file == top_file) {
		return false;
	}

	if (is_excluded(top_file)) {
		return true;
	}

	if (is_excluded(top_file + ":" + file)) {
		return true;
	}

	if (is_excluded(top_file + ":" + ident)) {
		return true;
	}

	if (is_excluded(top_file + ":" + file + ":" + ident)) {
		return true;
	}

	return false;
}

static
bool is_excluded(const std::string& key)
{
	return g_excluded.find(key) != g_excluded.end();
}

static bool
gimple_needs_ordering(const gimple* st)
{
	enum gimple_code code = gimple_code(st);

	if (code == GIMPLE_ASM && g_fix_asm) {
		return true;
	}

	if (code == GIMPLE_CALL && g_fix_built_in) {
		tree decl = gimple_call_fndecl(st);

		if (decl != NULL_TREE && DECL_BUILT_IN_CLASS(decl) != NOT_BUILT_IN) {
			return true;
		}

		// fall through
	}

	unsigned n_ops = gimple_num_ops(st);

	for (unsigned i = 0; i < n_ops; ++i) {
		tree op = gimple_op(st, i);

		if (tree_needs_ordering(op)) {
			return true;
		}
	}

	return false;
}

static bool
tree_needs_ordering(const tree t)
{
	if (t == NULL_TREE) {
		return false;
	}

	tree_code code = TREE_CODE(t);

	if (TREE_CODE_CLASS(code) == tcc_reference) {
		switch (code) {
		case COMPONENT_REF:
		case BIT_FIELD_REF:
		case ARRAY_REF:
		case VIEW_CONVERT_EXPR:
			// recurse - check operands for pointer dereferences and globals
			break;

		case ARRAY_RANGE_REF:
		case REALPART_EXPR:
		case IMAGPART_EXPR:
		case INDIRECT_REF:
		case TARGET_MEM_REF:
			std::cerr << "unsupported tree code " << code << std::endl;
			::abort();

		case MEM_REF:
			// pointer dereference - done
			return true;

		default:
			std::cerr << "unknown tree code " << code << std::endl;
			::abort();
		}
	}
	else if (code == ADDR_EXPR) {
		// & operator - done
		// (otherwise taking the address of a global variable - an ADDR_EXPR
		// on a VAR_DECL with TREE_STATIC() - would result in a barrier)
		return false;
	}
	else if (code == VAR_DECL && TREE_STATIC(t)) {
		// global - done
		return true;
	}
	// else - recurse

	int n_ops = TREE_OPERAND_LENGTH(t);

	for (int i = 0; i < n_ops; ++i) {
		tree op = TREE_OPERAND(t, i);

		if (tree_needs_ordering(op)) {
			return true;
		}
	}

	return false;
}

static bool
gimple_depends(const gimple* st, std::vector<tree>& deps, bool ordered)
{
	enum gimple_code code = gimple_code(st);

	if (code != GIMPLE_ASSIGN) {
		deps.clear();
		return false;
	}

	tree lhs = gimple_assign_lhs(st);
	tree rhs1 = gimple_assign_rhs1(st);
	tree rhs2 = gimple_assign_rhs2(st); // optional, may be NULL_TREE
	tree rhs3 = gimple_assign_rhs3(st); // optional, may be NULL_TREE

	bool depends = false;

	for (tree& dep : deps) {
		// deps only contains SSA_NAMEs - if something in deps appears on the
		// LHS, then we can be sure that it is being read (by definition of SSA,
		// each SSA_NAME is only written to once, which is what got it into
		// deps, so subsequent occurrences must be read occurrences)
		if (tree_contains(lhs, dep) || tree_contains(rhs1, dep) ||
				tree_contains(rhs2, dep) || tree_contains(rhs3, dep)) {
			depends = true;
			break;
		}
	}

	if (ordered) {
		deps.clear();
	}

	if ((ordered || depends) && TREE_CODE(lhs) == SSA_NAME) {
		deps.push_back(lhs);
	}

	return depends;
}

static bool
tree_contains(const tree t, const tree x)
{
	if (t == NULL_TREE) {
		return false;
	}

	if (t == x) {
		return true;
	}

	int n_ops = TREE_OPERAND_LENGTH(t);

	for (int i = 0; i < n_ops; ++i) {
		tree op = TREE_OPERAND(t, i);

		if (tree_contains(op, x)) {
			return true;
		}
	}

	return false;
}

static void
insert_barrier(gimple_stmt_iterator& gsi, location_t loc)
{
	gcall* b = g_profiling ? make_profiling_call() : make_barrier();

	gimple_set_location(b, loc);
	gsi_insert_before(&gsi, b, GSI_SAME_STMT);
}

static gcall*
make_barrier(void)
{
	tree decl = builtin_decl_explicit(BUILT_IN_ATOMIC_THREAD_FENCE);
	tree order = build_int_cst(integer_type_node, SEQ_CST);
	gcall* call = gimple_build_call(decl, 1, order);

	return call;
}

static gcall*
make_profiling_call(void)
{
	tree type = build_function_type_list(void_type_node, NULL_TREE);
	tree decl = build_fn_decl(COUNT_BARRIER, type);
	gcall* call = gimple_build_call(decl, 0);

	return call;
}

static bool
is_barrier(const gimple* st)
{
	if (! is_gimple_call(st)) {
		return false;
	}

	tree decl = builtin_decl_explicit(BUILT_IN_ATOMIC_THREAD_FENCE);

	if (gimple_call_fndecl(st) != decl) {
		return false;
	}

	if (gimple_call_num_args(st) != 1) {
		std::cerr << "unexpected number of arguments" << std::endl;
		::abort();
	}

	tree order = gimple_call_arg(st, 0);

	if (TREE_CODE(order) != INTEGER_CST) {
		std::cerr << "non-constant memory order argument" << std::endl;
		::abort();
	}

	if (tree_to_uhwi(order) != SEQ_CST) {
		return false;
	}

	return true;
}

static void
neutralize_barrier(gimple* st)
{
	tree order = build_int_cst(integer_type_node, RELAXED);

	gimple_call_set_arg(st, 0, order);
}

#if DEBUG > 0
static void
debug_func(const std::string& top_file, const std::string& file,
		const std::string& ident)
{
	size_t sz = ident.size() + file.size() + top_file.size() + 3;
	size_t n = sz > 70 ? 10 : 80 - sz;

	std::cerr << std::string(n, '-') << " " << top_file << ":" << file <<
			":" << ident << std::endl;
}

static void
debug_stmt(/* const */ gimple* st, const std::vector<tree>& deps)
{
	enum gimple_code code = gimple_code(st);

	std::cerr << "- " << code;

	for (const tree& dep : deps) {
		std::cerr << " _" << SSA_NAME_VERSION(dep);
	}

	std::cerr << " | ";
	debug_gimple_stmt(st);
}

static void
debug_stmt(/* const */ gimple* st)
{
	enum gimple_code code = gimple_code(st);

	std::cerr << "- " << code;

	std::cerr << " | ";
	debug_gimple_stmt(st);
}
#endif

#if DEBUG > 1
static void
debug_ops(const gimple* st)
{
	unsigned n_ops = gimple_num_ops(st);

	for (unsigned i = 0; i < n_ops; ++i) {
		tree op = gimple_op(st, i);

		std::cerr << "op " << i << std::endl;
		debug_tree(op);
	}
}
#endif
