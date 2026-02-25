//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: openlineage_extension.cpp
// Description: Main extension entry point and registration logic.
//              Registers configuration options, pragma functions, and optimizer hooks.
//===----------------------------------------------------------------------===//

#define DUCKDB_EXTENSION_MAIN

#include "openlineage_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/database.hpp"
#include "openlineage_optimizer.hpp"
#include "lineage_client.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Configuration Setters
//===--------------------------------------------------------------------===//
// These functions are called when users execute SET statements or configure
// the extension via the configuration API.

/// @brief Callback for setting the OpenLineage backend URL.
static void SetOpenLineageUrl(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetUrl(parameter.GetValue<string>());
}

/// @brief Callback for setting the OpenLineage API key.
static void SetOpenLineageApiKey(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetApiKey(parameter.GetValue<string>());
}

/// @brief Callback for setting the OpenLineage namespace.
static void SetOpenLineageNamespace(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetNamespace(parameter.GetValue<string>());
}

/// @brief Callback for enabling/disabling debug mode.
static void SetOpenLineageDebug(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetDebug(parameter.GetValue<bool>());
}

//===--------------------------------------------------------------------===//
// Extension Loading
//===--------------------------------------------------------------------===//

/// @brief Internal extension loading logic.
/// @param loader The extension loader providing access to the database.
/// @note Registers pragma functions, configuration options, and optimizer hooks.
static void LoadInternal(ExtensionLoader &loader) {
	// Register configuration options that users can set via SET statements
	auto &config = loader.GetDatabaseInstance().config;

	// SET openlineage_url = 'http://...'
	config.AddExtensionOption("openlineage_url", "URL of the OpenLineage backend", LogicalType::VARCHAR, Value(""),
	                          SetOpenLineageUrl);

	// SET openlineage_api_key = 'your-key'
	config.AddExtensionOption("openlineage_api_key", "API Key for OpenLineage backend", LogicalType::VARCHAR, Value(""),
	                          SetOpenLineageApiKey);

	// SET openlineage_namespace = 'my-namespace'
	config.AddExtensionOption("openlineage_namespace", "Namespace for OpenLineage events", LogicalType::VARCHAR,
	                          Value("duckdb"), SetOpenLineageNamespace);

	// SET openlineage_debug = true
	config.AddExtensionOption("openlineage_debug", "Enable debug logging for OpenLineage events", LogicalType::BOOLEAN,
	                          Value(false), SetOpenLineageDebug);

	// Register the optimizer extension that injects lineage tracking
	OptimizerExtension extension;
	extension.pre_optimize_function = OpenLineageOptimizer::PreOptimize;
	config.optimizer_extensions.push_back(extension);
}

//===--------------------------------------------------------------------===//
// Extension Class Implementation
//===--------------------------------------------------------------------===//

void OpenlineageExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string OpenlineageExtension::Name() {
	return "openlineage";
}

std::string OpenlineageExtension::Version() const {
#ifdef EXT_VERSION_OPENLINEAGE
	return EXT_VERSION_OPENLINEAGE;
#else
	return "";
#endif
}

} // namespace duckdb

//===--------------------------------------------------------------------===//
// C Extension Entry Point
//===--------------------------------------------------------------------===//
// This function is called by DuckDB when the extension is loaded.

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openlineage, loader) {
	duckdb::LoadInternal(loader);
}
}
