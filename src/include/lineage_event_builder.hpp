//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_event_builder.hpp
// Description: Factory class for building OpenLineage JSON events.
//              Provides a fluent interface for creating START, COMPLETE, and
//              FAIL events with facets, datasets, and metadata.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace duckdb {

using json = nlohmann::json;

// Forward declaration
struct CatalogInfo;

/// @class LineageEventBuilder
/// @brief Factory class for constructing OpenLineage JSON events.
///
/// Provides a builder pattern for creating OpenLineage events with proper
/// structure and validation. Supports:
/// - Event types: START, COMPLETE, FAIL
/// - Run metadata (runId, facets)
/// - Job metadata (namespace, name, facets)
/// - Input/Output datasets with facets
/// - Parent run relationships
/// - Error information for FAIL events
///
/// Example usage:
/// @code
///   auto event = LineageEventBuilder::CreateStart()
///                    .WithRunId(run_id)
///                    .WithEventTime(timestamp)
///                    .WithJob(namespace, job_name)
///                    .AddJobFacet_Sql(query)
///                    .AddOutputDataset(namespace, table_name, schema)
///                    .AddOutputDatasetsFacet_Statistics(row_count)
///                    .Build();
/// @endcode
class LineageEventBuilder {
public:
	/// @brief Event types supported by OpenLineage
	enum class EventType {
		START,    ///< Query execution started
		COMPLETE, ///< Query execution completed successfully
		FAIL      ///< Query execution failed
	};

	//===--------------------------------------------------------------------===//
	// Factory Methods
	//===--------------------------------------------------------------------===//

	/// @brief Create a builder for a START event
	/// @return New builder instance configured for START event
	static LineageEventBuilder CreateStart();

	/// @brief Create a builder for a COMPLETE event
	/// @return New builder instance configured for COMPLETE event
	static LineageEventBuilder CreateComplete();

	/// @brief Create a builder for a FAIL event
	/// @return New builder instance configured for FAIL event
	static LineageEventBuilder CreateFail();

	//===--------------------------------------------------------------------===//
	// Base Event Metadata (Required)
	//===--------------------------------------------------------------------===//

	/// @brief Set the run ID for this execution
	/// @param run_id Unique identifier (typically UUID)
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithRunId(const std::string &run_id);

	/// @brief Set the event timestamp
	/// @param event_time ISO 8601 formatted timestamp
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithEventTime(const std::string &event_time);

	/// @brief Set the job namespace and name
	/// @param namespace_ Namespace for the job (e.g., "duckdb")
	/// @param name Job name (e.g., SHA-256 hash of query)
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithJob(const std::string &namespace_, const std::string &name);

	//===--------------------------------------------------------------------===//
	// Job Facets
	//===--------------------------------------------------------------------===//

	/// @brief Add SQL facet to job metadata
	/// @param query SQL query text
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddJobFacet_Sql(const std::string &query);

	//===--------------------------------------------------------------------===//
	// Run Facets
	//===--------------------------------------------------------------------===//

	/// @brief Add parent run facet for orchestration integration
	/// @param parent_run_id UUID of the parent run
	/// @param parent_namespace Optional parent job namespace
	/// @param parent_name Optional parent job name
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddRunFacet_Parent(const std::string &parent_run_id, const std::string &parent_namespace = "",
	                                        const std::string &parent_name = "");

	/// @brief Add error message facet for FAIL events
	/// @param message Error description
	/// @param programming_language Language of the failed code (e.g., "SQL")
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddRunFacet_ErrorMessage(const std::string &message,
	                                              const std::string &programming_language = "SQL");

	/// @brief Add processing engine facet to identify the execution engine
	/// @param version Processing engine version (required)
	/// @param name Optional processing engine name (e.g., "DuckDB")
	/// @param duck_lineage_adapter_version Optional DuckLineage adapter version
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddRunFacet_ProcessingEngine(const std::string &version, const std::string &name = "",
	                                                  const std::string &duck_lineage_adapter_version = "");

	//===--------------------------------------------------------------------===//
	// Datasets
	//===--------------------------------------------------------------------===//

	/// @brief Set input datasets (bulk)
	/// @param inputs JSON array of input datasets
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithInputs(const json &inputs);

	/// @brief Set output datasets (bulk)
	/// @param outputs JSON array of output datasets
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithOutputs(const json &outputs);

	/// @brief Add a single input dataset
	/// @param namespace_ Dataset namespace (e.g., database path)
	/// @param name Dataset name (e.g., table name)
	/// @param schema_fields JSON array of field objects with name and type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddInputDataset(const std::string &namespace_, const std::string &name,
	                                     const json &schema_fields);

	/// @brief Add a single output dataset
	/// @param namespace_ Dataset namespace (e.g., database path)
	/// @param name Dataset name (e.g., table name)
	/// @param schema_fields JSON array of field objects with name and type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDataset(const std::string &namespace_, const std::string &name,
	                                      const json &schema_fields);

	//===--------------------------------------------------------------------===//
	// Dataset Facets
	//===--------------------------------------------------------------------===//

	/// @brief Add DataSource facet to an input dataset
	/// @param namespace_ Dataset namespace
	/// @param name Dataset name
	/// @param schema_fields JSON array of field objects with name and type
	/// @param catalog_path Physical catalog path to preserve location information
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddInputDatasetWithFacet_DataSource(const std::string &namespace_, const std::string &name,
	                                                         const json &schema_fields,
	                                                         const std::string &catalog_path);

	/// @brief Add DataSource facet to an output dataset
	/// @param namespace_ Dataset namespace
	/// @param name Dataset name
	/// @param schema_fields JSON array of field objects with name and type
	/// @param catalog_path Physical catalog path to preserve location information
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetWithFacet_DataSource(const std::string &namespace_, const std::string &name,
	                                                          const json &schema_fields,
	                                                          const std::string &catalog_path);

	/// @brief Add OutputStatistics facets to all output datasets
	/// @param row_count Number of rows produced
	/// @param size_in_bytes Optional size in bytes
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetsFacet_Statistics(idx_t row_count, idx_t size_in_bytes = 0);

	/// @brief Add LifecycleStateChange facet to an output dataset
	/// @param dataset_namespace Dataset namespace
	/// @param dataset_name Dataset name
	/// @param lifecycle_state The lifecycle state change (CREATE, DROP, ALTER, OVERWRITE, RENAME, TRUNCATE)
	/// @param previous_namespace Previous namespace (for RENAME operations only)
	/// @param previous_name Previous name (for RENAME operations only)
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetFacet_LifecycleStateChange(const std::string &dataset_namespace,
	                                                                const std::string &dataset_name,
	                                                                const std::string &lifecycle_state,
	                                                                const std::string &previous_namespace = "",
	                                                                const std::string &previous_name = "");

	/// @brief Add Symlinks facet with identifiers to an input dataset
	/// @param dataset_namespace Dataset namespace
	/// @param dataset_name Dataset name
	/// @param identifier_namespace Identifier namespace
	/// @param identifier_name Identifier name
	/// @param identifier_type Identifier type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddInputDatasetFacet_SymlinksIdentifiers(const std::string &dataset_namespace,
	                                                              const std::string &dataset_name,
	                                                              const std::string &identifier_namespace,
	                                                              const std::string &identifier_name,
	                                                              const std::string &identifier_type);

	/// @brief Add Symlinks facet with identifiers to an output dataset
	/// @param dataset_namespace Dataset namespace
	/// @param dataset_name Dataset name
	/// @param identifier_namespace Identifier namespace
	/// @param identifier_name Identifier name
	/// @param identifier_type Identifier type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetFacet_SymlinksIdentifiers(const std::string &dataset_namespace,
	                                                               const std::string &dataset_name,
	                                                               const std::string &identifier_namespace,
	                                                               const std::string &identifier_name,
	                                                               const std::string &identifier_type);

	/// @brief Add DatasetType facet to an input dataset
	/// @param dataset_namespace Dataset namespace
	/// @param dataset_name Dataset name
	/// @param dataset_type Dataset type (e.g., "FILE", "STREAM", "MODEL", "TABLE", "VIEW")
	/// @param sub_type Optional sub-type within the dataset type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddInputDatasetFacet_DatasetType(const std::string &dataset_namespace,
	                                                      const std::string &dataset_name,
	                                                      const std::string &dataset_type,
	                                                      const std::string &sub_type = "");

	/// @brief Add DatasetType facet to an output dataset
	/// @param dataset_namespace Dataset namespace
	/// @param dataset_name Dataset name
	/// @param dataset_type Dataset type (e.g., "FILE", "STREAM", "MODEL", "TABLE", "VIEW")
	/// @param sub_type Optional sub-type within the dataset type
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetFacet_DatasetType(const std::string &dataset_namespace,
	                                                       const std::string &dataset_name,
	                                                       const std::string &dataset_type,
	                                                       const std::string &sub_type = "");

	/// @brief Add an input dataset with DataSource and Catalog facets
	/// @param namespace_ Dataset namespace
	/// @param name Dataset name
	/// @param schema_fields JSON array of field objects with name and type
	/// @param catalog_path Physical catalog path for DataSource facet
	/// @param catalog_info Catalog metadata for Catalog facet
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddInputDatasetWithFacets(const std::string &namespace_, const std::string &name,
	                                               const json &schema_fields, const std::string &catalog_path,
	                                               const CatalogInfo &catalog_info);

	/// @brief Add an output dataset with DataSource and Catalog facets
	/// @param namespace_ Dataset namespace
	/// @param name Dataset name
	/// @param schema_fields JSON array of field objects with name and type
	/// @param catalog_path Physical catalog path for DataSource facet
	/// @param catalog_info Catalog metadata for Catalog facet
	/// @return Reference to this builder for chaining
	LineageEventBuilder &AddOutputDatasetWithFacets(const std::string &namespace_, const std::string &name,
	                                                const json &schema_fields, const std::string &catalog_path,
	                                                const CatalogInfo &catalog_info);

	//===--------------------------------------------------------------------===//
	// Producer Information
	//===--------------------------------------------------------------------===//

	/// @brief Set custom producer URL (defaults to extension URL)
	/// @param producer_url URL identifying the producer
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithProducer(const std::string &producer_url);

	/// @brief Set custom schema URL (defaults to OpenLineage 2.0.2)
	/// @param schema_url URL to OpenLineage schema
	/// @return Reference to this builder for chaining
	LineageEventBuilder &WithSchemaURL(const std::string &schema_url);

	//===--------------------------------------------------------------------===//
	// Helper Methods
	//===--------------------------------------------------------------------===//

	/// @brief Create a dataset JSON object with schema facet
	/// @param namespace_ Dataset namespace (e.g., database path)
	/// @param name Dataset name (e.g., table name)
	/// @param schema_fields JSON array of field objects with name and type
	/// @return JSON object representing an OpenLineage dataset
	static json CreateDataset(const std::string &namespace_, const std::string &name, const json &schema_fields);

	/// @brief Create a schema field JSON object
	/// @param field_name Name of the field
	/// @param field_type Type of the field
	/// @return JSON object representing a schema field
	static json CreateSchemaField(const std::string &field_name, const std::string &field_type);

	//===--------------------------------------------------------------------===//
	// Build
	//===--------------------------------------------------------------------===//

	/// @brief Build the final JSON event
	/// @return JSON object representing the OpenLineage event
	/// @throws std::runtime_error if required fields are missing
	json Build() const;

	/// @brief Build and serialize to JSON string
	/// @param indent Number of spaces for indentation (0 for compact)
	/// @return JSON string representation
	std::string BuildString(int indent = 0) const;

private:
	// Private constructor - use factory methods
	explicit LineageEventBuilder(EventType type);

	// Validate that all required fields are set
	void Validate() const;

	// Internal state
	EventType event_type;
	json event_json;
	bool has_run_id = false;
	bool has_event_time = false;
	bool has_job = false;

	// Default constants
	static constexpr const char *DEFAULT_PRODUCER = "https://github.com/Ilum/duckdb-openlineage";
	static constexpr const char *DEFAULT_SCHEMA_URL =
	    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
	static constexpr const char *SQL_FACET_PRODUCER = "https://github.com/Ilum/duckdb-openlineage";
	static constexpr const char *SQL_FACET_SCHEMA = "https://openlineage.io/spec/job/sql/1-0-0.json";
	static constexpr const char *PARENT_FACET_SCHEMA = "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json";
	static constexpr const char *ERROR_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json";
	static constexpr const char *PROCESSING_ENGINE_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json";
	static constexpr const char *OUTPUT_STATS_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json";
	static constexpr const char *DATASET_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json";
	static constexpr const char *SYMLINKS_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json";
	static constexpr const char *DATASOURCE_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json";
	static constexpr const char *CATALOG_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json";
	static constexpr const char *LIFECYCLE_STATE_CHANGE_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json";
	static constexpr const char *DATASET_TYPE_FACET_SCHEMA =
	    "https://openlineage.io/spec/facets/1-0-0/DatasetTypeDatasetFacet.json";
};

} // namespace duckdb
