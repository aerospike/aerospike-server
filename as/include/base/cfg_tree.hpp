/*
* cfg_tree.hpp
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

#pragma once

//==========================================================
// Includes.
//

#include <array>
#include <functional>
#include <stdexcept>
#include <vector>

#include "yaml-cpp/yaml.h"

#include "base/cfg_tree_handlers.hpp"
#include "nlohmann/json-schema.hpp"
#include "nlohmann/json.hpp"

// includes from the server C codebase
extern "C" {
#include "base/datamodel.h"
}

//==========================================================
// Forward declarations.
//

class CFGTree;

// Forward declare as_config to avoid circular includes
struct as_config_s;
typedef struct as_config_s as_config;

//==========================================================
// Typedefs & constants.
//

enum class cfg_format { YAML };

//==========================================================
// Public API.
//

class CFGTree
{
public:
	CFGTree(const std::string& config_file, const std::string& schema_file,
			cfg_format format);
	~CFGTree();
	std::string dump() const;
	void validate();

	// Apply configuration to as_config struct - throws on error
	void apply_config(as_config* config);

	// Helper method to get JSON value by path
	static bool get_json_value(const std::string& path,
			const nlohmann::json& source, nlohmann::json& result);

private:
	nlohmann::json json_tree;
	std::string config_file;
	std::string config_data;
	std::string schema_file;
	nlohmann::json_schema::json_validator validator;

	void parse_yaml_data();
	void read_config_file();
	void load_schema();
	// TODO:void apply_schema_defaults();

	// Utility methods
	void* get_field_ptr(void* config, size_t offset) const;
};
