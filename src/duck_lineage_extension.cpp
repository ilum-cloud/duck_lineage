//===----------------------------------------------------------------------===//
// DuckDB DuckLineage Extension
//
// File: duck_lineage_extension.cpp
// Description: Main extension entry point and registration logic.
//              Registers configuration options, pragma functions, and optimizer hooks.
//===----------------------------------------------------------------------===//

#define DUCKDB_EXTENSION_MAIN

#include "duck_lineage_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/database.hpp"
#include "duck_lineage_optimizer.hpp"
#include "lineage_client.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Configuration Setters
//===--------------------------------------------------------------------===//
// These functions are called when users execute SET statements or configure
// the extension via the configuration API.

/// @brief Callback for setting the OpenLineage backend URL.
static void SetDuckLineageUrl(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetUrl(parameter.GetValue<string>());
}

/// @brief Callback for setting the OpenLineage API key.
static void SetDuckLineageApiKey(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetApiKey(parameter.GetValue<string>());
}

/// @brief Callback for setting the OpenLineage namespace.
static void SetDuckLineageNamespace(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetNamespace(parameter.GetValue<string>());
}

/// @brief Callback for enabling/disabling debug mode.
static void SetDuckLineageDebug(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetDebug(parameter.GetValue<bool>());
}

/// @brief Callback for setting max retries for HTTP requests.
static void SetDuckLineageMaxRetries(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetMaxRetries(parameter.GetValue<int64_t>());
}

/// @brief Callback for setting max queue size for pending events.
static void SetDuckLineageMaxQueueSize(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetMaxQueueSize(parameter.GetValue<int64_t>());
}

/// @brief Callback for setting HTTP request timeout in seconds.
static void SetDuckLineageTimeout(ClientContext &context, SetScope scope, Value &parameter) {
	LineageClient::Get().SetTimeout(parameter.GetValue<int64_t>());
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

	// SET duck_lineage_url = 'http://...'
	config.AddExtensionOption("duck_lineage_url", "URL of the OpenLineage backend", LogicalType::VARCHAR, Value(""),
	                          SetDuckLineageUrl);

	// SET duck_lineage_api_key = 'your-key'
	config.AddExtensionOption("duck_lineage_api_key", "API Key for OpenLineage backend", LogicalType::VARCHAR, Value(""),
	                          SetDuckLineageApiKey);

	// SET duck_lineage_namespace = 'my-namespace'
	config.AddExtensionOption("duck_lineage_namespace", "Namespace for OpenLineage events", LogicalType::VARCHAR,
	                          Value("duckdb"), SetDuckLineageNamespace);

	// SET duck_lineage_debug = true
	config.AddExtensionOption("duck_lineage_debug", "Enable debug logging for OpenLineage events", LogicalType::BOOLEAN,
	                          Value(false), SetDuckLineageDebug);

	// SET duck_lineage_max_retries = 3
	config.AddExtensionOption("duck_lineage_max_retries", "Maximum retry attempts for failed HTTP requests",
	                          LogicalType::BIGINT, Value::BIGINT(3), SetDuckLineageMaxRetries);

	// SET duck_lineage_max_queue_size = 10000
	config.AddExtensionOption("duck_lineage_max_queue_size", "Maximum number of events to queue before dropping",
	                          LogicalType::BIGINT, Value::BIGINT(10000), SetDuckLineageMaxQueueSize);

	// SET duck_lineage_timeout = 10
	config.AddExtensionOption("duck_lineage_timeout", "HTTP request timeout in seconds", LogicalType::BIGINT,
	                          Value::BIGINT(10), SetDuckLineageTimeout);

	// Register the optimizer extension that injects lineage tracking
	OptimizerExtension extension;
	extension.pre_optimize_function = DuckLineageOptimizer::PreOptimize;
	config.optimizer_extensions.push_back(extension);
}

//===--------------------------------------------------------------------===//
// Extension Class Implementation
//===--------------------------------------------------------------------===//

void DuckLineageExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckLineageExtension::Name() {
	return "duck_lineage";
}

std::string DuckLineageExtension::Version() const {
#ifdef EXT_VERSION_DUCK_LINEAGE
	return EXT_VERSION_DUCK_LINEAGE;
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

DUCKDB_CPP_EXTENSION_ENTRY(duck_lineage, loader) {
	duckdb::LoadInternal(loader);
}
}
