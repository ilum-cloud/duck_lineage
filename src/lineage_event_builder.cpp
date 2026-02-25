//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_event_builder.cpp
// Description: Implementation of the LineageEventBuilder factory class.
//===----------------------------------------------------------------------===//

#include "lineage_event_builder.hpp"
#include "lineage_utils.hpp"
#include <stdexcept>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Static member definitions (required for C++11 constexpr)
//===--------------------------------------------------------------------===//

constexpr const char *LineageEventBuilder::DEFAULT_PRODUCER;
constexpr const char *LineageEventBuilder::DEFAULT_SCHEMA_URL;
constexpr const char *LineageEventBuilder::SQL_FACET_PRODUCER;
constexpr const char *LineageEventBuilder::SQL_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::PARENT_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::ERROR_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::PROCESSING_ENGINE_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::OUTPUT_STATS_SCHEMA;
constexpr const char *LineageEventBuilder::DATASET_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::DATASOURCE_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::CATALOG_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::LIFECYCLE_STATE_CHANGE_SCHEMA;
constexpr const char *LineageEventBuilder::DATASET_TYPE_FACET_SCHEMA;
constexpr const char *LineageEventBuilder::SYMLINKS_FACET_SCHEMA;

//===--------------------------------------------------------------------===//
// Factory Methods
//===--------------------------------------------------------------------===//

LineageEventBuilder LineageEventBuilder::CreateStart() {
	return LineageEventBuilder(EventType::START);
}

LineageEventBuilder LineageEventBuilder::CreateComplete() {
	return LineageEventBuilder(EventType::COMPLETE);
}

LineageEventBuilder LineageEventBuilder::CreateFail() {
	return LineageEventBuilder(EventType::FAIL);
}

//===--------------------------------------------------------------------===//
// Private Constructor
//===--------------------------------------------------------------------===//

LineageEventBuilder::LineageEventBuilder(EventType type) : event_type(type) {
	// Initialize with event type
	const char *type_str = nullptr;
	switch (type) {
	case EventType::START:
		type_str = "START";
		break;
	case EventType::COMPLETE:
		type_str = "COMPLETE";
		break;
	case EventType::FAIL:
		type_str = "FAIL";
		break;
	}

	event_json = {{"eventType", type_str}, {"producer", DEFAULT_PRODUCER}, {"schemaURL", DEFAULT_SCHEMA_URL}};
}

//===--------------------------------------------------------------------===//
// Core Metadata
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::WithRunId(const std::string &run_id) {
	if (!event_json.contains("run")) {
		event_json["run"] = json::object();
	}
	event_json["run"]["runId"] = run_id;
	has_run_id = true;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddInputDatasetFacet_SymlinksIdentifiers(
    const std::string &dataset_namespace, const std::string &dataset_name, const std::string &identifier_namespace,
    const std::string &identifier_name, const std::string &identifier_type) {
	// Find the input dataset to add the facet to
	if (!event_json.contains("inputs") || event_json["inputs"].empty()) {
		return *this;
	}

	for (auto &input : event_json["inputs"]) {
		if (input["namespace"] == dataset_namespace && input["name"] == dataset_name) {
			// Ensure facets object exists
			if (!input.contains("facets")) {
				input["facets"] = json::object();
			}

			json symlinks_facet = input["facets"].value(
			    "symlinks", json::object({{"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", SYMLINKS_FACET_SCHEMA}}));
			json identifiers = symlinks_facet.value("identifiers", json::array());
			identifiers.push_back(
			    {{"namespace", identifier_namespace}, {"name", identifier_name}, {"type", identifier_type}});
			symlinks_facet["identifiers"] = identifiers;

			input["facets"]["symlinks"] = symlinks_facet;
			break;
		}
	}

	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetFacet_SymlinksIdentifiers(
    const std::string &dataset_namespace, const std::string &dataset_name, const std::string &identifier_namespace,
    const std::string &identifier_name, const std::string &identifier_type) {
	// Find the output dataset to add the facet to
	if (!event_json.contains("outputs") || event_json["outputs"].empty()) {
		return *this;
	}

	for (auto &output : event_json["outputs"]) {
		if (output["namespace"] == dataset_namespace && output["name"] == dataset_name) {
			// Ensure facets object exists
			if (!output.contains("facets")) {
				output["facets"] = json::object();
			}

			json symlinks_facet = output["facets"].value(
			    "symlinks", json::object({{"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", SYMLINKS_FACET_SCHEMA}}));
			json identifiers = symlinks_facet.value("identifiers", json::array());
			identifiers.push_back(
			    {{"namespace", identifier_namespace}, {"name", identifier_name}, {"type", identifier_type}});
			symlinks_facet["identifiers"] = identifiers;

			output["facets"]["symlinks"] = symlinks_facet;
			break;
		}
	}

	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddInputDatasetFacet_DatasetType(const std::string &dataset_namespace,
                                                                           const std::string &dataset_name,
                                                                           const std::string &dataset_type,
                                                                           const std::string &sub_type) {
	// Find the input dataset to add the facet to
	if (!event_json.contains("inputs") || event_json["inputs"].empty()) {
		return *this;
	}

	for (auto &input : event_json["inputs"]) {
		if (input["namespace"] == dataset_namespace && input["name"] == dataset_name) {
			// Ensure facets object exists
			if (!input.contains("facets")) {
				input["facets"] = json::object();
			}

			// Build datasetType facet
			json dataset_type_facet = {{"_producer", SQL_FACET_PRODUCER},
			                           {"_schemaURL", DATASET_TYPE_FACET_SCHEMA},
			                           {"datasetType", dataset_type}};

			// Add optional subType if provided
			if (!sub_type.empty()) {
				dataset_type_facet["subType"] = sub_type;
			}

			input["facets"]["datasetType"] = dataset_type_facet;
			break;
		}
	}

	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetFacet_DatasetType(const std::string &dataset_namespace,
                                                                            const std::string &dataset_name,
                                                                            const std::string &dataset_type,
                                                                            const std::string &sub_type) {
	// Find the output dataset to add the facet to
	if (!event_json.contains("outputs") || event_json["outputs"].empty()) {
		return *this;
	}

	for (auto &output : event_json["outputs"]) {
		if (output["namespace"] == dataset_namespace && output["name"] == dataset_name) {
			// Ensure facets object exists
			if (!output.contains("facets")) {
				output["facets"] = json::object();
			}

			// Build datasetType facet
			json dataset_type_facet = {{"_producer", SQL_FACET_PRODUCER},
			                           {"_schemaURL", DATASET_TYPE_FACET_SCHEMA},
			                           {"datasetType", dataset_type}};

			// Add optional subType if provided
			if (!sub_type.empty()) {
				dataset_type_facet["subType"] = sub_type;
			}

			output["facets"]["datasetType"] = dataset_type_facet;
			break;
		}
	}

	return *this;
}

LineageEventBuilder &LineageEventBuilder::WithEventTime(const std::string &event_time) {
	event_json["eventTime"] = event_time;
	has_event_time = true;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::WithJob(const std::string &namespace_, const std::string &name) {
	event_json["job"] = {{"namespace", namespace_}, {"name", name}};
	has_job = true;
	return *this;
}

//===--------------------------------------------------------------------===//
// Job Facets
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::AddJobFacet_Sql(const std::string &query) {
	// Ensure job exists
	if (!event_json.contains("job")) {
		event_json["job"] = json::object();
	}

	// Ensure facets object exists
	if (!event_json["job"].contains("facets")) {
		event_json["job"]["facets"] = json::object();
	}

	// Add SQL facet
	event_json["job"]["facets"]["sql"] = {
	    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", SQL_FACET_SCHEMA}, {"query", query}};

	return *this;
}

//===--------------------------------------------------------------------===//
// Run Facets
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::AddRunFacet_Parent(const std::string &parent_run_id,
                                                             const std::string &parent_namespace,
                                                             const std::string &parent_name) {
	// Ensure run exists
	if (!event_json.contains("run")) {
		event_json["run"] = json::object();
	}

	// Ensure facets object exists
	if (!event_json["run"].contains("facets")) {
		event_json["run"]["facets"] = json::object();
	}

	// Build parent facet
	json parent_facet = {
	    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", PARENT_FACET_SCHEMA}, {"run", {{"runId", parent_run_id}}}};

	// Add parent job information if provided
	if (!parent_namespace.empty() && !parent_name.empty()) {
		parent_facet["job"] = {{"namespace", parent_namespace}, {"name", parent_name}};
	}

	event_json["run"]["facets"]["parent"] = parent_facet;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddRunFacet_ErrorMessage(const std::string &message,
                                                                   const std::string &programming_language) {
	// Ensure run exists
	if (!event_json.contains("run")) {
		event_json["run"] = json::object();
	}

	// Ensure facets object exists
	if (!event_json["run"].contains("facets")) {
		event_json["run"]["facets"] = json::object();
	}

	// Add error message facet
	event_json["run"]["facets"]["errorMessage"] = {{"_producer", SQL_FACET_PRODUCER},
	                                               {"_schemaURL", ERROR_FACET_SCHEMA},
	                                               {"message", message},
	                                               {"programmingLanguage", programming_language}};

	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddRunFacet_ProcessingEngine(const std::string &version,
                                                                       const std::string &name,
                                                                       const std::string &openlineage_adapter_version) {
	// Ensure run exists
	if (!event_json.contains("run")) {
		event_json["run"] = json::object();
	}

	// Ensure facets object exists
	if (!event_json["run"].contains("facets")) {
		event_json["run"]["facets"] = json::object();
	}

	// Build processing engine facet
	json processing_engine_facet = {
	    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", PROCESSING_ENGINE_FACET_SCHEMA}, {"version", version}};

	// Add optional fields if provided
	if (!name.empty()) {
		processing_engine_facet["name"] = name;
	}
	if (!openlineage_adapter_version.empty()) {
		processing_engine_facet["openlineageAdapterVersion"] = openlineage_adapter_version;
	}

	event_json["run"]["facets"]["processing_engine"] = processing_engine_facet;
	return *this;
}

//===--------------------------------------------------------------------===//
// Datasets
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::WithInputs(const json &inputs) {
	event_json["inputs"] = inputs;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::WithOutputs(const json &outputs) {
	event_json["outputs"] = outputs;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddInputDataset(const std::string &namespace_, const std::string &name,
                                                          const json &schema_fields) {
	// Ensure inputs array exists
	if (!event_json.contains("inputs")) {
		event_json["inputs"] = json::array();
	}

	// Create and add the dataset
	event_json["inputs"].push_back(CreateDataset(namespace_, name, schema_fields));
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddInputDatasetWithFacet_DataSource(const std::string &namespace_,
                                                                              const std::string &name,
                                                                              const json &schema_fields,
                                                                              const std::string &catalog_path) {
	// Ensure inputs array exists
	if (!event_json.contains("inputs")) {
		event_json["inputs"] = json::array();
	}

	// Create the dataset with schema facet
	json dataset = CreateDataset(namespace_, name, schema_fields);

	// Add DataSource facet to preserve catalog path information
	if (!catalog_path.empty()) {
		if (!dataset.contains("facets")) {
			dataset["facets"] = json::object();
		}
		// Format the path properly (handles postgres connection strings, local files, etc.)
		string uri = FormatDatabasePath(catalog_path);
		dataset["facets"]["dataSource"] = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", DATASOURCE_FACET_SCHEMA}, {"name", uri}, {"uri", uri}};
	}

	event_json["inputs"].push_back(dataset);
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDataset(const std::string &namespace_, const std::string &name,
                                                           const json &schema_fields) {
	// Ensure outputs array exists
	if (!event_json.contains("outputs")) {
		event_json["outputs"] = json::array();
	}

	// Create and add the dataset
	event_json["outputs"].push_back(CreateDataset(namespace_, name, schema_fields));
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetWithFacet_DataSource(const std::string &namespace_,
                                                                               const std::string &name,
                                                                               const json &schema_fields,
                                                                               const std::string &catalog_path) {
	// Ensure outputs array exists
	if (!event_json.contains("outputs")) {
		event_json["outputs"] = json::array();
	}

	// Create the dataset with schema facet
	json dataset = CreateDataset(namespace_, name, schema_fields);

	// Add DataSource facet to preserve catalog path information
	if (!catalog_path.empty()) {
		if (!dataset.contains("facets")) {
			dataset["facets"] = json::object();
		}
		// Format the path properly (handles postgres connection strings, local files, etc.)
		string uri = FormatDatabasePath(catalog_path);
		dataset["facets"]["dataSource"] = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", DATASOURCE_FACET_SCHEMA}, {"name", uri}, {"uri", uri}};
	}

	event_json["outputs"].push_back(dataset);
	return *this;
}

//===--------------------------------------------------------------------===//
// Dataset Facets
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetsFacet_Statistics(idx_t row_count, idx_t size_in_bytes) {
	// Only add statistics if outputs exist and for successful queries
	if (!event_json.contains("outputs") || event_json["outputs"].empty()) {
		return *this;
	}

	// Skip for FAIL events
	if (event_type == EventType::FAIL) {
		return *this;
	}

	// Add statistics to each output dataset
	for (auto &output : event_json["outputs"]) {
		// Ensure outputFacets exists
		if (!output.contains("outputFacets")) {
			output["outputFacets"] = json::object();
		}

		// Build statistics facet
		json stats_facet = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", OUTPUT_STATS_SCHEMA}, {"rowCount", row_count}};

		// Add size if provided
		if (size_in_bytes > 0) {
			stats_facet["sizeInBytes"] = size_in_bytes;
		}

		output["outputFacets"]["outputStatistics"] = stats_facet;
	}

	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddInputDatasetWithFacets(const std::string &namespace_,
                                                                    const std::string &name, const json &schema_fields,
                                                                    const std::string &catalog_path,
                                                                    const CatalogInfo &catalog_info) {
	// Ensure inputs array exists
	if (!event_json.contains("inputs")) {
		event_json["inputs"] = json::array();
	}

	// Create the dataset with schema facet
	json dataset = CreateDataset(namespace_, name, schema_fields);

	// Ensure facets object exists
	if (!dataset.contains("facets")) {
		dataset["facets"] = json::object();
	}

	// Add DataSource facet if catalog path is provided
	if (!catalog_path.empty()) {
		// Format the path properly (handles postgres connection strings, local files, etc.)
		string uri = FormatDatabasePath(catalog_path);
		dataset["facets"]["dataSource"] = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", DATASOURCE_FACET_SCHEMA}, {"name", uri}, {"uri", uri}};
	}

	// Add Catalog facet - only include fields that are available
	json catalog_facet = {{"_producer", SQL_FACET_PRODUCER},
	                      {"_schemaURL", CATALOG_FACET_SCHEMA},
	                      {"framework", catalog_info.framework},
	                      {"type", catalog_info.type},
	                      {"name", catalog_info.name}};

	// Add optional fields if they are present (non-empty)
	if (!catalog_info.metadata_uri.empty()) {
		catalog_facet["metadataUri"] = catalog_info.metadata_uri;
	}
	if (!catalog_info.warehouse_uri.empty()) {
		catalog_facet["warehouseUri"] = catalog_info.warehouse_uri;
	}
	if (!catalog_info.source.empty()) {
		catalog_facet["source"] = catalog_info.source;
	}

	dataset["facets"]["catalog"] = catalog_facet;

	event_json["inputs"].push_back(dataset);
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetWithFacets(const std::string &namespace_,
                                                                     const std::string &name, const json &schema_fields,
                                                                     const std::string &catalog_path,
                                                                     const CatalogInfo &catalog_info) {
	// Ensure outputs array exists
	if (!event_json.contains("outputs")) {
		event_json["outputs"] = json::array();
	}

	// Create the dataset with schema facet
	json dataset = CreateDataset(namespace_, name, schema_fields);

	// Ensure facets object exists
	if (!dataset.contains("facets")) {
		dataset["facets"] = json::object();
	}

	// Add DataSource facet if catalog path is provided
	if (!catalog_path.empty()) {
		// Format the path properly (handles postgres connection strings, local files, etc.)
		string uri = FormatDatabasePath(catalog_path);
		dataset["facets"]["dataSource"] = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", DATASOURCE_FACET_SCHEMA}, {"name", uri}, {"uri", uri}};
	}

	// Add Catalog facet - only include fields that are available
	json catalog_facet = {{"_producer", SQL_FACET_PRODUCER},
	                      {"_schemaURL", CATALOG_FACET_SCHEMA},
	                      {"framework", catalog_info.framework},
	                      {"type", catalog_info.type},
	                      {"name", catalog_info.name}};

	// Add optional fields if they are present (non-empty)
	if (!catalog_info.metadata_uri.empty()) {
		catalog_facet["metadataUri"] = catalog_info.metadata_uri;
	}
	if (!catalog_info.warehouse_uri.empty()) {
		catalog_facet["warehouseUri"] = catalog_info.warehouse_uri;
	}
	if (!catalog_info.source.empty()) {
		catalog_facet["source"] = catalog_info.source;
	}

	dataset["facets"]["catalog"] = catalog_facet;

	event_json["outputs"].push_back(dataset);
	return *this;
}

LineageEventBuilder &LineageEventBuilder::AddOutputDatasetFacet_LifecycleStateChange(
    const std::string &dataset_namespace, const std::string &dataset_name, const std::string &lifecycle_state,
    const std::string &previous_namespace, const std::string &previous_name) {
	// Validate lifecycle state
	if (lifecycle_state != "CREATE" && lifecycle_state != "DROP" && lifecycle_state != "ALTER" &&
	    lifecycle_state != "OVERWRITE" && lifecycle_state != "RENAME" && lifecycle_state != "TRUNCATE") {
		throw std::runtime_error("Invalid lifecycle state: " + lifecycle_state);
	}

	// Find the output dataset to add the facet to
	if (!event_json.contains("outputs") || event_json["outputs"].empty()) {
		return *this;
	}

	for (auto &output : event_json["outputs"]) {
		if (output["namespace"] == dataset_namespace && output["name"] == dataset_name) {
			// Ensure facets object exists
			if (!output.contains("facets")) {
				output["facets"] = json::object();
			}

			// Build lifecycle state change facet
			json lifecycle_facet = {{"_producer", SQL_FACET_PRODUCER},
			                        {"_schemaURL", LIFECYCLE_STATE_CHANGE_SCHEMA},
			                        {"lifecycleStateChange", lifecycle_state}};

			// Add previousIdentifier for RENAME operations
			if (lifecycle_state == "RENAME" && !previous_namespace.empty() && !previous_name.empty()) {
				lifecycle_facet["previousIdentifier"] = {{"namespace", previous_namespace}, {"name", previous_name}};
			}

			output["facets"]["lifecycleStateChange"] = lifecycle_facet;
			break;
		}
	}

	return *this;
}

//===--------------------------------------------------------------------===//
// Producer Information
//===--------------------------------------------------------------------===//

LineageEventBuilder &LineageEventBuilder::WithProducer(const std::string &producer_url) {
	event_json["producer"] = producer_url;
	return *this;
}

LineageEventBuilder &LineageEventBuilder::WithSchemaURL(const std::string &schema_url) {
	event_json["schemaURL"] = schema_url;
	return *this;
}

//===--------------------------------------------------------------------===//
// Helper Methods
//===--------------------------------------------------------------------===//

json LineageEventBuilder::CreateDataset(const std::string &namespace_, const std::string &name,
                                        const json &schema_fields) {
	json dataset = {{"namespace", namespace_}, {"name", name}};

	// Add schema facet if fields are provided
	if (!schema_fields.empty()) {
		if (!dataset.contains("facets")) {
			dataset["facets"] = json::object();
		}
		dataset["facets"]["schema"] = {
		    {"_producer", SQL_FACET_PRODUCER}, {"_schemaURL", DATASET_FACET_SCHEMA}, {"fields", schema_fields}};
	}

	return dataset;
}

json LineageEventBuilder::CreateSchemaField(const std::string &field_name, const std::string &field_type) {
	return json {{"name", field_name}, {"type", field_type}};
}

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//

void LineageEventBuilder::Validate() const {
	if (!has_run_id) {
		throw std::runtime_error("LineageEventBuilder: runId is required");
	}
	if (!has_event_time) {
		throw std::runtime_error("LineageEventBuilder: eventTime is required");
	}
	if (!has_job) {
		throw std::runtime_error("LineageEventBuilder: job information is required");
	}
}

json LineageEventBuilder::Build() const {
	Validate();
	return event_json;
}

std::string LineageEventBuilder::BuildString(int indent) const {
	Validate();
	return event_json.dump(indent);
}

} // namespace duckdb
