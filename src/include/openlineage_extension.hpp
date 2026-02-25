//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: openlineage_extension.hpp
// Description: Main extension entry point for the OpenLineage integration.
//              Registers configuration options, pragma functions, and the
//              optimizer extension that injects lineage tracking.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

/// @class OpenlineageExtension
/// @brief Main extension class for DuckDB OpenLineage integration.
///
/// This extension adds OpenLineage support to DuckDB, enabling automatic
/// lineage tracking for queries. It registers:
/// - Configuration options (openlineage_url, openlineage_api_key, etc.)
/// - A test pragma function (PRAGMA openlineage_test)
/// - An optimizer extension that injects lineage tracking into query plans
///
/// The extension automatically tracks data lineage for queries and sends
/// OpenLineage events (START, COMPLETE, FAIL) to a configured backend.
class OpenlineageExtension : public Extension {
public:
	/// @brief Load the extension into the database.
	/// @param db The extension loader providing access to the database instance.
	/// @note Registers all configuration options, functions, and optimizer hooks.
	void Load(ExtensionLoader &db) override;

	/// @brief Get the extension name.
	/// @return "openlineage"
	std::string Name() override;

	/// @brief Get the extension version.
	/// @return Version string (defined by EXT_VERSION_OPENLINEAGE macro).
	std::string Version() const override;
};

} // namespace duckdb
