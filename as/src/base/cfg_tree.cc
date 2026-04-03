/*
 * cfg_tree_handlers.cc
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

//==========================================================
// Includes.
//
#include "base/cfg_tree.hpp"

#include <algorithm> // For std::transform
#include <cctype> // For std::tolower
#include <fstream>
#include <limits> // For std::numeric_limits
#include <string>

#include "base/cfg_tree_handlers.hpp"
#include "nlohmann/json-schema.hpp"
#include "nlohmann/json.hpp"

// TODO: vault.h has the 'namespace' field which conflicts with C++ keyword
// Need to find a proper solution for this
// extern "C" {
// #include "vault.h"
// }

//==========================================================
// Forward declarations.
//

std::string read_file(const std::string& path);
json convert_yaml_to_json(const YAML::Node& node);

//==========================================================
// Typedefs & constants.
//

#define NO_OFFSET 0

// Macro to handle C++ keyword conflicts in offsetof
// #define VAULT_NAMESPACE_OFFSET offsetof(cf_vault_config, namespace)

using json = nlohmann::json;
using json_validator = nlohmann::json_schema::json_validator;

//==========================================================
// Public API.
//

CFGTree::CFGTree(const std::string& config_file, const std::string& schema_file,
		cfg_format format)
{
	this->config_file = config_file;
	this->schema_file = schema_file;
	read_config_file();
	load_schema();

	switch (format) {
	case cfg_format::YAML:
		parse_yaml_data();
		break;
	default:
		throw std::invalid_argument("Invalid configuration file format");
	}

	// Apply schema defaults after parsing to fill in missing optional fields
	// TODO: re-enable this when we have delt with the schema emitting
	// defaults for every field.
	// apply_schema_defaults();
}

CFGTree::~CFGTree() {}

// TODO: re-enable this when we have delt with the default patch
// producing every possible field in the schema.
// void
// CFGTree::apply_schema_defaults()
// {
//     try {
//         // Custom error handler that doesn't throw exceptions
//         // This allows us to collect default values even if some required fields are missing
//         class default_collecting_error_handler :
//                 public nlohmann::json_schema::basic_error_handler {
//         public:
//             void
//             error(const nlohmann::json::json_pointer& /*ptr*/,
//                     const nlohmann::json& /*instance*/,
//                     const std::string& /*message*/) override
//             {
//                 // Don't call the base error method to avoid setting error flag
//                 // We want to collect defaults even if validation has errors
//             }
//         };

//         default_collecting_error_handler err;
//         json patch = validator.validate(json_tree, err);

//         // Apply the patch with default values (only adds missing fields, preserves existing)
//         json_tree = json_tree.patch(patch);
//     }
//     catch (const std::exception& e) {
//         // If schema defaults fail, continue with the original tree
//         // This ensures that configuration parsing doesn't fail due to schema default issues
//         std::cerr << "Warning: Could not apply schema defaults: " << e.what()
//                 << std::endl;
//     }
// }

void
CFGTree::validate()
{
	if (json_tree.empty()) {
		throw std::runtime_error("Config data is not set");
	}

	try {
		validator.validate(json_tree);
	}
	catch (const std::exception& e) {
		std::string error_msg = "Validation error: " + std::string(e.what());
		// Validation errors from JSON schema library include newlines
		// which mess with logging output so we remove them here.
		std::replace(error_msg.begin(), error_msg.end(), '\n', ' ');
		throw std::runtime_error(error_msg);
	}
}

std::string
CFGTree::dump() const
{
	return json_tree.dump();
}

// NEW: Configuration application methods

void
CFGTree::apply_config(as_config* config)
{
	if (config == NULL) {
		throw std::invalid_argument("as_config pointer is null");
	}

	cfg_handlers::apply_config(config, json_tree);
}

// misc helpers

void*
CFGTree::get_field_ptr(void* config, size_t offset) const
{
	return reinterpret_cast<char*>(config) + offset;
}

//==========================================================
// Private methods.
//

void
CFGTree::parse_yaml_data()
{
	if (config_data.empty()) {
		throw std::runtime_error("Config data is not set");
	}

	// NOTE: YAML::Load() only loads the first document in the stream.
	// if there is more than one it will miss the rest.
	// we only expect one document in an Aerospike config file so this is ok for now.
	YAML::Node root = YAML::Load(config_data);
	json_tree = convert_yaml_to_json(root);
}

void
CFGTree::read_config_file()
{
	config_data = read_file(config_file);
}

void
CFGTree::load_schema()
{
	if (schema_file.empty()) {
		throw std::runtime_error("Schema file is not set");
	}

	auto schema_string = read_file(schema_file);
	auto schema_json = json::parse(schema_string);

	validator = json_validator(NULL,
			nlohmann::json_schema::default_string_format_check);

	validator.set_root_schema(schema_json);
}

//==========================================================
// Local helpers.
//

std::string
read_file(const std::string& path)
{
	std::ifstream file(path);

	if (! file.is_open()) {
		throw std::runtime_error("Failed to open file: " + path);
	}

	std::ostringstream ss;
	ss << file.rdbuf();

	if (file.fail() && ! file.eof()) {
		throw std::runtime_error("Failed to read file: " + path);
	}

	return ss.str();
}

//==========================================================
// YAML to JSON conversion.
//

// json is move assignable so we can return the json object from the function.
// without too much overhead (I hope).
json
convert_yaml_to_json(const YAML::Node& node)
{
	if (node.IsScalar()) {
		// Try to parse the strings from yaml-cpp
		// and convert them to the types that json schema expects.

		// First check for null values
		if (node.IsNull()) {
			throw std::runtime_error("Null value not supported");
		}

		// Quoted values (",') have the tag "!"
		// per the yaml spec "YAML processors should resolve nodes having the "!" non-specific tag as
		// "tag:yaml.org,2002:seq", "tag:yaml.org,2002:map" or "tag:yaml.org,2002:str" depending on their kind."
		if (node.Tag() == "!") {
			return json(node.Scalar());
		}

		// Try boolean
		try {
			bool bool_val = node.as<bool>();
			// Only return as bool if it is a valid yaml boolean
			std::string scalar_str = node.Scalar();
			// BUG: read_consistency_level_override "on" gets parsed as a bool instead of a string...
			// for now restrict booleans to true/false
			std::transform(scalar_str.begin(), scalar_str.end(),
					scalar_str.begin(), ::tolower);

			if (scalar_str == "true" || scalar_str == "false") {
				return json(bool_val);
			}
		}
		catch (const YAML::BadConversion&) {
		}

		// Try integer
		std::string scalar_str = node.Scalar();

		if (scalar_str.find('.') == std::string::npos &&
				scalar_str.find('e') == std::string::npos &&
				scalar_str.find('E') == std::string::npos) {

			// Try different integer types based on size
			try {
				long long int_val = node.as<long long>();
				// Check if it fits in a standard int
				if (int_val >= std::numeric_limits<int>::min() &&
						int_val <= std::numeric_limits<int>::max()) {
					return json(static_cast<int>(int_val));
				}
				else {
					return json(int_val);
				}
			}
			catch (const YAML::BadConversion&) {
			}
		}

		// Try floating point
		try {
			double double_val = node.as<double>();
			std::string scalar_str = node.Scalar();

			if (scalar_str.find('.') != std::string::npos ||
					scalar_str.find('e') != std::string::npos ||
					scalar_str.find('E') != std::string::npos) {
				return json(double_val);
			}
		}
		catch (const YAML::BadConversion&) {
		}

		// Default to string
		return json(node.Scalar());
	}
	else if (node.IsSequence()) {
		json json_array = json::array();

		for (const auto& item : node) {
			json_array.push_back(convert_yaml_to_json(item));
		}

		return json_array;
	}
	else if (node.IsMap()) {
		json json_object = json::object();

		for (const auto& item : node) {
			if (json_object.contains(item.first.Scalar())) {
				throw std::runtime_error("Duplicate key: " + item.first.Scalar());
			}

			if (! item.first.IsScalar()) {
				throw std::runtime_error("Invalid key type: " +
						std::to_string(item.first.Type()));
			}

			json_object[item.first.Scalar()] = convert_yaml_to_json(item.second);
		}

		return json_object;
	}
	else {
		throw std::runtime_error("Unsupported YAML node type: " +
				std::to_string(node.Type()));
	}
}