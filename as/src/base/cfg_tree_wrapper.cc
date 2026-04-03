/*
 * cfg_tree_wrapper.cc
 *
 * Copyright (C) 2025 Aerospike, Inc.
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

#include "base/cfg_tree_wrapper.h"

#include <cstring>
#include <string>

#include "base/cfg_tree.hpp"
#include "base/cfg_tree_handlers.hpp"

// Thread-local storage for error messages (only for C wrapper)
thread_local std::string last_error_message;

// Helper function to convert C++ cfg_format to C cfg_format_t
cfg_format
cpp_format_from_c(cfg_format_t c_format)
{
	switch (c_format) {
	case CFG_FORMAT_YAML:
		return cfg_format::YAML;
	default:
		throw std::invalid_argument("Invalid configuration format");
	}
}

// Helper function to set error message (only for C wrapper)
void
set_last_error(const std::string& error)
{
	last_error_message = error;
}

// Helper function to clear error message (only for C wrapper)
void
clear_last_error()
{
	last_error_message.clear();
}

extern "C" {

cfg_tree_t
cfg_tree_create(const char* config_file, const char* schema_file,
		cfg_format_t format)
{
	if (config_file == nullptr || schema_file == nullptr) {
		set_last_error("Config file and schema file paths cannot be null");
		return nullptr;
	}

	try {
		clear_last_error();
		cfg_format cpp_format = cpp_format_from_c(format);
		CFGTree* tree = new CFGTree(std::string(config_file),
				std::string(schema_file), cpp_format);

		return static_cast<cfg_tree_t>(tree);
	}
	catch (const std::exception& e) {
		set_last_error(std::string("Failed to create CFGTree: ") + e.what());
		return nullptr;
	}
}

void
cfg_tree_destroy(cfg_tree_t cfg_tree)
{
	if (cfg_tree != nullptr) {
		CFGTree* tree = static_cast<CFGTree*>(cfg_tree);
		delete tree;
	}
}

int
cfg_tree_validate(cfg_tree_t cfg_tree)
{
	if (cfg_tree == nullptr) {
		set_last_error("CFGTree instance is null");
		return -1;
	}

	try {
		clear_last_error();
		CFGTree* tree = static_cast<CFGTree*>(cfg_tree);
		tree->validate();
	}
	catch (const std::exception& e) {
		set_last_error(e.what());
		return -1;
	}

	return 0;
}

char*
cfg_tree_dump(cfg_tree_t cfg_tree)
{
	if (cfg_tree == nullptr) {
		set_last_error("CFGTree instance is null");
		return nullptr;
	}

	try {
		clear_last_error();
		CFGTree* tree = static_cast<CFGTree*>(cfg_tree);
		std::string json_str = tree->dump();

		// Allocate C string and copy data
		char* result = new char[json_str.length() + 1];
		std::strcpy(result, json_str.c_str());
		return result;
	}
	catch (const std::exception& e) {
		set_last_error(std::string("Failed to dump configuration: ") + e.what());
		return nullptr;
	}
}

int
cfg_tree_apply_config(cfg_tree_t cfg_tree, as_config* config)
{
	if (cfg_tree == nullptr) {
		set_last_error("CFGTree instance is null");
		return -1;
	}

	if (config == nullptr) {
		set_last_error("as_config pointer is null");
		return -1;
	}

	try {
		clear_last_error();
		CFGTree* tree = static_cast<CFGTree*>(cfg_tree);
		tree->apply_config(config); // Throws on error
		return 0; // Success
	}
	catch (const cfg_handlers::config_error& e) {
		set_last_error(std::string("Configuration error: ") + e.what());
		return -1;
	}
	catch (const std::exception& e) {
		set_last_error(std::string("Failed to apply configuration: ") + e.what());
		return -1;
	}
}

void
cfg_tree_free_string(char* str)
{
	if (str != nullptr) {
		delete[] str;
	}
}

const char*
cfg_tree_get_last_error(void)
{
	if (last_error_message.empty()) {
		return nullptr;
	}

	return last_error_message.c_str();
}

} // extern "C"