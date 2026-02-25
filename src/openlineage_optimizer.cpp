//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: openlineage_optimizer.cpp
// Description: Optimizer extension that analyzes query plans to extract lineage.
//              Traverses the logical plan to identify input/output datasets,
//              extracts schema information, and injects lineage tracking.
//===----------------------------------------------------------------------===//

#include "openlineage_optimizer.hpp"
#include "logical_lineage_sentinel.hpp"
#include "lineage_client.hpp"
#include "lineage_utils.hpp"
#include "lineage_event_builder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include <nlohmann/json.hpp>
#include <unordered_set>

namespace duckdb {

using json = nlohmann::json;

//===--------------------------------------------------------------------===//
// Helper Structures for Deduplication
//===--------------------------------------------------------------------===

/// @brief Simple struct to track datasets for deduplication
struct DatasetKey {
	string namespace_;
	string name;

	bool operator==(const DatasetKey &other) const {
		return namespace_ == other.namespace_ && name == other.name;
	}
};

/// @brief Hash function for DatasetKey
struct DatasetKeyHash {
	size_t operator()(const DatasetKey &k) const {
		return std::hash<string>()(k.namespace_) ^ (std::hash<string>()(k.name) << 1);
	}
};

using DatasetSet = unordered_set<DatasetKey, DatasetKeyHash>;

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

/// @brief Read a tag value from a catalog entry with fallback to empty string.
/// @param entry The catalog entry to read from (can be null).
/// @param tag_name The tag name to read.
/// @return The tag value, or empty string if tag not found or entry is null.
static string ReadTag(optional_ptr<CatalogEntry> entry, const string &tag_name) {
	if (entry && entry->tags.contains(tag_name)) {
		return entry->tags[tag_name];
	}
	return "";
}

/// @brief Get the database path from a catalog for use as a namespace.
/// @param catalog The catalog to extract the path from.
/// @return The database path, or "duckdb_memory" for in-memory/system catalogs.
/// @note Used as the namespace in OpenLineage dataset identifiers.
static string GetCatalogPath(Catalog &catalog) {
	try {
		// Special handling for system and temporary catalogs
		if (catalog.IsSystemCatalog() || catalog.IsTemporaryCatalog()) {
			return "duckdb_memory";
		}

		// Get the database file path
		string path = catalog.GetDBPath();
		if (path.empty() || path == ":memory:") {
			return "duckdb_memory";
		}

		return path;
	} catch (...) {
		// Fallback in case of any errors
		return "default";
	}
}

//===--------------------------------------------------------------------===//
// Query Node Visitor for Input Extraction
//===--------------------------------------------------------------------===//

// Forward declarations
static void ExtractInputsFromTableRef(ClientContext &context, TableRef &ref, LineageEventBuilder &builder);

/// @brief Helper function to extract input datasets from a QueryNode tree
/// @param context Client context for accessing catalog
/// @param node The query node to traverse
/// @param builder Event builder to add datasets to
static void ExtractInputsFromQueryNode(ClientContext &context, QueryNode &node, LineageEventBuilder &builder) {
	// Handle different types of query nodes
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select_node = node.Cast<SelectNode>();

		// Process the FROM clause
		if (select_node.from_table) {
			ExtractInputsFromTableRef(context, *select_node.from_table, builder);
		}

		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = node.Cast<SetOperationNode>();
		for (auto &child : setop_node.children) {
			if (child) {
				ExtractInputsFromQueryNode(context, *child, builder);
			}
		}
		break;
	}
	default:
		// For other node types, skip for now
		break;
	}
}

// Forward declaration
static void ExtractTableNamesFromQueryNode(ClientContext &context, QueryNode &node, unordered_set<string> &table_names);

/// @brief Helper function to extract table names from a TableRef tree
/// @param context Client context for accessing catalog
/// @param ref The table reference to traverse
/// @param table_names Output set to populate with table names
static void ExtractTableNamesFromTableRef(ClientContext &context, TableRef &ref, unordered_set<string> &table_names) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref.Cast<BaseTableRef>();
		// Resolve the actual catalog entry to get the correct fully qualified name
		try {
			// Try to look up the table entry to determine its actual catalog and schema
			optional_ptr<CatalogEntry> entry = nullptr;
			string actual_catalog_name = base_table.catalog_name;
			string actual_schema_name = base_table.schema_name;

			// If catalog is not specified, try to find the table in available catalogs
			if (actual_catalog_name.empty()) {
				// Try common catalog names
				vector<string> catalog_names = {"memory", "hms", "hive", "hive_metastore"};
				for (const auto &catalog_name : catalog_names) {
					try {
						vector<string> schemas_to_try = {"main", "default", ""};
						for (const auto &schema_name : schemas_to_try) {
							try {
								EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base_table.table_name);
								auto test_entry = Catalog::GetEntry(context, catalog_name, schema_name, lookup_info,
								                                    OnEntryNotFound::RETURN_NULL);
								if (test_entry && (test_entry->type == CatalogType::TABLE_ENTRY ||
								                   test_entry->type == CatalogType::VIEW_ENTRY)) {
									entry = test_entry;
									actual_catalog_name = catalog_name;
									actual_schema_name = schema_name.empty() ? "main" : schema_name;
									goto found_entry;
								}
							} catch (...) {
							}
						}
					} catch (...) {
					}
				}
			} else {
				// Catalog is specified, use it
				actual_schema_name = actual_schema_name.empty() ? "main" : actual_schema_name;
			}
		found_entry:;

			// Build fully qualified table name
			if (!actual_catalog_name.empty()) {
				string fully_qualified = actual_catalog_name + "." + actual_schema_name + "." + base_table.table_name;
				table_names.insert(fully_qualified);
			} else {
				// Fallback: use the catalog/schema from the BaseTableRef
				string catalog_name = base_table.catalog_name.empty() ? "memory" : base_table.catalog_name;
				string schema_name = base_table.schema_name.empty() ? "main" : base_table.schema_name;
				string fully_qualified = catalog_name + "." + schema_name + "." + base_table.table_name;
				table_names.insert(fully_qualified);
			}
		} catch (...) {
			// Fallback: use the catalog/schema from the BaseTableRef
			string catalog_name = base_table.catalog_name.empty() ? "memory" : base_table.catalog_name;
			string schema_name = base_table.schema_name.empty() ? "main" : base_table.schema_name;
			string fully_qualified = catalog_name + "." + schema_name + "." + base_table.table_name;
			table_names.insert(fully_qualified);
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			ExtractTableNamesFromTableRef(context, *join.left, table_names);
		}
		if (join.right) {
			ExtractTableNamesFromTableRef(context, *join.right, table_names);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			ExtractTableNamesFromQueryNode(context, *subquery.subquery->node, table_names);
		}
		break;
	}
	default:
		break;
	}
}

/// @brief Helper function to extract table names from a QueryNode tree
/// @param context Client context for accessing catalog
/// @param node The query node to traverse
/// @param table_names Output set to populate with table names
static void ExtractTableNamesFromQueryNode(ClientContext &context, QueryNode &node,
                                           unordered_set<string> &table_names) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select_node = node.Cast<SelectNode>();
		if (select_node.from_table) {
			ExtractTableNamesFromTableRef(context, *select_node.from_table, table_names);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = node.Cast<SetOperationNode>();
		for (auto &child : setop_node.children) {
			if (child) {
				ExtractTableNamesFromQueryNode(context, *child, table_names);
			}
		}
		break;
	}
	default:
		break;
	}
}

/// @brief Helper function to extract input datasets from a TableRef tree
/// @param context Client context for accessing catalog
/// @param ref The table reference to traverse
/// @param builder Event builder to add datasets to
static void ExtractInputsFromTableRef(ClientContext &context, TableRef &ref, LineageEventBuilder &builder) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref.Cast<BaseTableRef>();

		// Get the actual catalog from the context if not specified
		// BaseTableRef is from the parsed query, so catalog_name may be empty
		// Use pointer to allow reassignment when searching for HMS catalog
		Catalog *catalog_ptr = &Catalog::GetCatalog(context, base_table.catalog_name);
		string catalog_name = catalog_ptr->GetName();
		string catalog_type = catalog_ptr->GetCatalogType();
		string schema_name = base_table.schema_name.empty() ? "main" : base_table.schema_name;
		string db_path = GetCatalogPath(*catalog_ptr);

		// Build the fully qualified table name
		string fully_qualified_name = catalog_name + "." + schema_name + "." + base_table.table_name;

		// Check if this is an HMS table by catalog type
		bool is_hms_table = (catalog_type == "hive_metastore");

		CatalogInfo catalog_info;

		// Variable to store the HMS table entry we found (for later tag reading)
		optional_ptr<CatalogEntry> found_hms_entry = nullptr;

		// If not initially found as HMS, search all catalogs to find the table
		// This handles cases like "SELECT * FROM hms.table" where catalog is not specified
		if (!is_hms_table) {
			// Try to find the table in any catalog by searching
			// Use the database's attached databases to find catalogs
			auto &db = DatabaseInstance::GetDatabase(context);

			// Get all attached database managers which contain catalogs
			vector<string> catalog_names;
			try {
				// Try to get catalog through different methods
				// Method 1: Check if there's an extension or similar with HMS
				// We'll query the context for attached databases
				auto &attached_dbs = db.GetDatabaseManager();

				// Try common HMS catalog names
				vector<string> hms_catalog_names = {"hms", "hive", "hive_metastore"};
				for (const auto &hms_name : hms_catalog_names) {
					try {
						auto &test_catalog = Catalog::GetCatalog(context, hms_name);
						string test_cat_type = test_catalog.GetCatalogType();

						if (test_cat_type == "hive_metastore") {
							// Try to get the table entry from this catalog
							// Try multiple approaches to find the table
							optional_ptr<CatalogEntry> test_entry = nullptr;
							string found_schema = schema_name;

							// Approach 1: Try with the original schema name from the query
							{
								EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base_table.table_name);
								test_entry = Catalog::GetEntry(context, hms_name, schema_name, lookup_info,
								                               OnEntryNotFound::RETURN_NULL);
							}

							// Approach 2: Try with 'default' schema (common in HMS)
							if (!test_entry && schema_name != "default") {
								EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base_table.table_name);
								test_entry = Catalog::GetEntry(context, hms_name, "default", lookup_info,
								                               OnEntryNotFound::RETURN_NULL);
								if (test_entry) {
									found_schema = "default";
								}
							}

							// Approach 3: Try with empty schema (let HMS decide)
							if (!test_entry) {
								EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base_table.table_name);
								test_entry =
								    Catalog::GetEntry(context, hms_name, "", lookup_info, OnEntryNotFound::RETURN_NULL);
								if (test_entry) {
									found_schema = "";
								}
							}

							bool entry_found = (test_entry && test_entry->type == CatalogType::TABLE_ENTRY);

							if (entry_found) {
								// Found it! This is an HMS table
								is_hms_table = true;
								catalog_ptr = &test_catalog;
								catalog_name = hms_name;
								catalog_type = "hive_metastore";
								// Update schema_name to the one that actually worked
								schema_name = found_schema.empty() ? "hms" : found_schema;
								fully_qualified_name = catalog_name + "." + schema_name + "." + base_table.table_name;
								db_path = GetCatalogPath(*catalog_ptr);
								// Save the entry for later use
								found_hms_entry = test_entry;
								break;
							}
						}
					} catch (...) {
					}

					if (is_hms_table) {
						break;
					}
				}
			} catch (...) {
			}
		}

		// Get reference from pointer for easier use
		Catalog &catalog = *catalog_ptr;

		if (is_hms_table) {
			// For HMS tables, mark the catalog info appropriately
			catalog_info.type = "hms";
			catalog_info.name = catalog_name;
			catalog_info.source = "hms";
			catalog_info.framework = "hive";

			// Use the table entry we already found in the search loop
			auto table_entry = found_hms_entry;

			// Extract schema information
			json fields = json::array();
			if (table_entry && table_entry->type == CatalogType::TABLE_ENTRY) {
				auto &table = table_entry->Cast<TableCatalogEntry>();
				for (auto &col : table.GetColumns().Logical()) {
					fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
				}
			}

			// Try to get HMS metadata from tags (inter-extension communication)
			string storage_location;
			string hms_table_type;
			string hms_input_format;
			string hms_output_format;
			string hms_serialization_lib;
			string hms_dataset_namespace;
			string hms_dataset_name;

			// Read HMS metadata tags using helper function
			storage_location = ReadTag(table_entry, "hms_storage_location");
			hms_table_type = ReadTag(table_entry, "hms_table_type");
			hms_input_format = ReadTag(table_entry, "hms_input_format");
			hms_output_format = ReadTag(table_entry, "hms_output_format");
			hms_serialization_lib = ReadTag(table_entry, "hms_serialization_lib");

			// Determine namespace and dataset name based on storage location
			if (!storage_location.empty()) {
				// Parse storage location to extract namespace and dataset name
				string normalized_location = storage_location;

				// Strip __PLACEHOLDER__ suffix if present (Delta tables sometimes have this)
				if (StringUtil::Contains(normalized_location, "-__PLACEHOLDER__")) {
					auto placeholder_pos = normalized_location.find("-__PLACEHOLDER__");
					normalized_location = normalized_location.substr(0, placeholder_pos);
				}

				// Convert s3a:// to s3://
				if (StringUtil::StartsWith(normalized_location, "s3a://")) {
					normalized_location = "s3://" + normalized_location.substr(6);
				}

				// Normalize storage location as namespace
				if (normalized_location.back() == '/') {
					normalized_location.pop_back();
				}

				// Split by '/' to extract namespace components
				// Path format: s3://bucket/subpath1/subpath2/.../database/table_name
				// We strip from the end: table_name, then database name
				// Everything remaining becomes the namespace
				auto segments = StringUtil::Split(normalized_location, '/');
				if (segments.size() >= 3) {
					// Keep track of stripped segments to build the name
					vector<string> stripped_segments;

					// Strip table name from end (last segment if it contains the table name)
					if (!segments.empty() && segments.size() > 2) {
						// Check if last segment contains or matches the table name
						string last_segment = segments.back();
						if (last_segment == base_table.table_name ||
						    StringUtil::Contains(last_segment, base_table.table_name)) {
							stripped_segments.insert(stripped_segments.begin(), last_segment);
							segments.pop_back();
						}
					}

					// Strip database/schema name from end (if it looks like a database name)
					// Database names often end with .db or match the schema_name
					if (!segments.empty() && segments.size() > 2) {
						string last_segment = segments.back();
						// Check if it looks like a database name (ends with .db or matches schema)
						if (StringUtil::EndsWith(last_segment, ".db") ||
						    (!schema_name.empty() && last_segment == schema_name)) {
							stripped_segments.insert(stripped_segments.begin(), last_segment);
							segments.pop_back();
						}
					}

					// Reconstruct namespace from remaining segments
					// First 3 segments are protocol ("s3:"), empty ("", from //), bucket
					hms_dataset_namespace = segments[0] + "//" + segments[2];

					// Add any remaining middle segments to namespace
					if (segments.size() > 3) {
						vector<string> namespace_segments;
						for (size_t i = 3; i < segments.size(); i++) {
							namespace_segments.push_back(segments[i]);
						}
						if (!namespace_segments.empty()) {
							hms_dataset_namespace += "/" + StringUtil::Join(namespace_segments, "/");
						}
					}

					// Name is what was stripped (database/table_name)
					hms_dataset_name = StringUtil::Join(stripped_segments, "/");
				} else {
					// Fallback for unexpected path format
					hms_dataset_namespace = normalized_location;
					hms_dataset_name = base_table.table_name;
				}
			} else {
				// Fallback: use catalog name as namespace
				hms_dataset_namespace = catalog_name;     // e.g., "hms"
				hms_dataset_name = base_table.table_name; // e.g., "transactions"
			}

			// Update catalog_info source to match the namespace
			catalog_info.source = hms_dataset_namespace;

			// For HMS tables, pass the normalized namespace as db_path (not the raw storage_location)
			builder.AddInputDatasetWithFacets(hms_dataset_namespace, hms_dataset_name, fields, hms_dataset_namespace,
			                                  catalog_info);

			// Add DatasetType facet with format information in sub_type
			string dataset_subtype = hms_input_format.empty() ? "" : hms_input_format;
			builder.AddInputDatasetFacet_DatasetType(hms_dataset_namespace, hms_dataset_name, "TABLE", dataset_subtype);
		} else {
			// For regular DuckDB tables, use the original logic
			catalog_info = ExtractCatalogInfo(catalog);

			// For views, we don't have the schema information at this parsing stage
			// Add input dataset without schema (it's optional)
			json fields = json::array(); // Empty schema - we're in the parsing phase, not binding

			builder.AddInputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
			                                  db_path, catalog_info);

			// Add DatasetType facet to distinguish as TABLE
			builder.AddInputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
			                                         "TABLE");
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			ExtractInputsFromTableRef(context, *join.left, builder);
		}
		if (join.right) {
			ExtractInputsFromTableRef(context, *join.right, builder);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			ExtractInputsFromQueryNode(context, *subquery.subquery->node, builder);
		}
		break;
	}
	default:
		// For other table ref types (empty, show, table_function, etc.), skip
		break;
	}
}

//===--------------------------------------------------------------------===//
// Plan Visitor for Lineage Extraction
//===--------------------------------------------------------------------===//

/// @class LineagePlanVisitor
/// @brief Visitor that traverses the logical plan to extract input/output datasets.
///
/// This visitor performs a depth-first traversal of the query plan and identifies:
/// - Input datasets (from LogicalGet operators - table reads, including HMS scans)
/// - Output datasets (from LogicalInsert, LogicalCreateTable, and LogicalCreateView operators)
///
/// For each dataset, it extracts:
/// - Namespace (database path)
/// - Table/View name
/// - Schema (column names and types)
class LineagePlanVisitor {
public:
	ClientContext &context;                         ///< Client context for accessing catalog information
	LineageEventBuilder &builder;                   ///< Event builder to add datasets to
	const unordered_set<string> &view_dependencies; ///< Fully qualified names of view dependencies (to skip)
	const unordered_set<string>
	    &view_dependency_tables; ///< Just table names of view dependencies (for flexible matching)

	explicit LineagePlanVisitor(ClientContext &context, LineageEventBuilder &builder,
	                            const unordered_set<string> &view_dependencies,
	                            const unordered_set<string> &view_dependency_tables)
	    : context(context), builder(builder), view_dependencies(view_dependencies),
	      view_dependency_tables(view_dependency_tables) {
	}

	/// @brief Recursively visit an operator and all its children.
	/// @param op The logical operator to visit.
	void Visit(LogicalOperator &op) {
		VisitOperator(op);
		// Recursively visit all children in the operator tree
		for (auto &child : op.children) {
			Visit(*child);
		}
	}

	/// @brief Process a single operator to extract lineage information.
	/// @param op The logical operator to analyze.
	/// @note Handles LogicalGet (reads), LogicalInsert (writes), LogicalCreateTable (creates), and LogicalCreateView
	/// (creates).
	void VisitOperator(LogicalOperator &op) {
		// ===== Handle table reads (SELECT FROM table) =====
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();

			// Check if this is a regular table scan
			if (get.GetTable()) {
				// Check if this is a VIEW - if so, extract its dependencies instead of treating it as a table
				if (get.GetTable()->type == CatalogType::VIEW_ENTRY) {
					// This is a view - add the view as an input dataset
					auto &view_entry = get.GetTable()->Cast<ViewCatalogEntry>();

					// Get the database path to store as metadata
					string db_path = GetCatalogPath(get.GetTable()->catalog);

					// Extract catalog information for the catalog facet
					CatalogInfo catalog_info = ExtractCatalogInfo(get.GetTable()->catalog);

					// Get fully qualified view name: catalog.schema.view
					string fully_qualified_name = GetFullyQualifiedTableName(
					    get.GetTable()->catalog, get.GetTable()->ParentSchema(), get.GetTable()->name);

					// Extract schema information from the view
					json fields = json::array();
					for (auto &col : get.GetTable()->GetColumns().Logical()) {
						fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
					}

					// Add the view as an input dataset
					builder.AddInputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
					                                  db_path, catalog_info);

					// Add DatasetType facet to mark this as a VIEW
					builder.AddInputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
					                                         "VIEW");

					return; // Don't process this LogicalGet further - we've handled the view
				}

				// Check if this table is a dependency of a referenced view
				// If so, skip it since we only want to show the view, not its underlying tables
				string fully_qualified_table = GetFullyQualifiedTableName(
				    get.GetTable()->catalog, get.GetTable()->ParentSchema(), get.GetTable()->name);
				if (view_dependencies.count(fully_qualified_table) > 0) {
					// This table is accessed through a view, skip it
					return;
				}

				// This is a regular TABLE - process it normally
				// Get the database path to store as metadata
				string db_path = GetCatalogPath(get.GetTable()->catalog);

				// Extract catalog information for the catalog facet
				CatalogInfo catalog_info = ExtractCatalogInfo(get.GetTable()->catalog);

				// Get fully qualified table name: catalog.schema.table
				string fully_qualified_name = GetFullyQualifiedTableName(
				    get.GetTable()->catalog, get.GetTable()->ParentSchema(), get.GetTable()->name);

				// Extract schema information (column names and types)
				json fields = json::array();
				for (auto &col : get.GetTable()->GetColumns().Logical()) {
					fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
				}

				// Add input dataset with both DataSource and Catalog facets
				builder.AddInputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
				                                  db_path, catalog_info);

				// Add DatasetType facet to distinguish between TABLE and VIEW
				builder.AddInputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
				                                         "TABLE");
			} else {
				// Check for HMS scan function - use tags from catalog instead of parsing function name
				if (StringUtil::StartsWith(get.function.name, "hms_scan##")) {
					// Parse the function name to extract catalog, schema, and table name
					// Function name format: hms_scan##URL_ENCODED(catalog.schema.table)
					string encoded_name = get.function.name.substr(10); // Remove "hms_scan##"

					// URL decode to get fully qualified table name
					string fully_qualified_name = StringUtil::URLDecode(encoded_name);

					// Parse catalog.schema.table
					string catalog_name, schema_name, table_name;
					auto first_dot = fully_qualified_name.find('.');
					auto last_dot = fully_qualified_name.rfind('.');

					if (first_dot != string::npos && last_dot != string::npos && first_dot != last_dot) {
						catalog_name = fully_qualified_name.substr(0, first_dot);
						schema_name = fully_qualified_name.substr(first_dot + 1, last_dot - first_dot - 1);
						table_name = fully_qualified_name.substr(last_dot + 1);
					} else {
						table_name = fully_qualified_name;
						catalog_name = "hms";
					}

					// Check if this HMS table is a dependency of a referenced view
					// Use flexible matching: check both fully qualified name and just table name
					bool should_skip = view_dependencies.count(fully_qualified_name) > 0 ||
					                   view_dependency_tables.count(table_name) > 0;

					if (should_skip) {
						// This table is accessed through a view, skip it
						return;
					}

					// Try to get the table entry from catalog to read tags
					string storage_location;
					string hms_input_format;
					optional_ptr<CatalogEntry> table_entry = nullptr;

					// Try to look up the table entry in the catalog
					try {
						// Try multiple schema approaches
						vector<string> schemas_to_try = {schema_name};
						if (!schema_name.empty() && schema_name != "default") {
							schemas_to_try.push_back("default");
						}
						schemas_to_try.push_back("");

						for (const auto &test_schema : schemas_to_try) {
							try {
								EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, table_name);
								auto test_entry = Catalog::GetEntry(context, catalog_name, test_schema, lookup_info,
								                                    OnEntryNotFound::RETURN_NULL);
								if (test_entry && test_entry->type == CatalogType::TABLE_ENTRY) {
									table_entry = test_entry;
									break;
								}
							} catch (...) {
							}
						}
					} catch (...) {
					}

					// Read metadata from tags if table entry was found
					if (table_entry) {
						if (table_entry->tags.contains("hms_storage_location")) {
							storage_location = table_entry->tags["hms_storage_location"];
						}
						if (table_entry->tags.contains("hms_input_format")) {
							hms_input_format = table_entry->tags["hms_input_format"];
						}
					}

					// Extract schema information from returned types
					json fields = json::array();
					for (size_t i = 0; i < get.names.size(); i++) {
						if (i < get.returned_types.size()) {
							fields.push_back(
							    LineageEventBuilder::CreateSchemaField(get.names[i], get.returned_types[i].ToString()));
						}
					}

					// Determine namespace and name for OpenLineage
					string dataset_namespace = "file";
					string dataset_name = table_name;
					if (!storage_location.empty()) {
						// Normalize storage location
						string normalized_location = storage_location;

						// Strip __PLACEHOLDER__ suffix if present (Delta tables sometimes have this)
						if (StringUtil::Contains(normalized_location, "-__PLACEHOLDER__")) {
							auto placeholder_pos = normalized_location.find("-__PLACEHOLDER__");
							normalized_location = normalized_location.substr(0, placeholder_pos);
						}

						if (StringUtil::StartsWith(normalized_location, "s3a://")) {
							normalized_location = "s3://" + normalized_location.substr(6);
						}
						if (normalized_location.back() == '/') {
							normalized_location.pop_back();
						}

						// Parse storage location to extract namespace
						// Path format: s3://bucket/subpath1/subpath2/.../database/table_name
						// We strip from the end: table_name, then database name
						// Everything remaining becomes the namespace
						auto segments = StringUtil::Split(normalized_location, '/');
						if (segments.size() >= 3) {
							// Keep track of stripped segments to build the name
							vector<string> stripped_segments;

							// Strip table name from end (last segment if it contains the table name)
							string last_segment = segments.back();
							if (last_segment == table_name || StringUtil::Contains(last_segment, table_name)) {
								stripped_segments.insert(stripped_segments.begin(), last_segment);
								segments.pop_back();
							}

							// Strip database/schema name from end (if it looks like a database name)
							// Database names often end with .db or match the schema_name
							if (!segments.empty() && segments.size() > 2) {
								last_segment = segments.back();
								if (StringUtil::EndsWith(last_segment, ".db") ||
								    (!schema_name.empty() && last_segment == schema_name)) {
									stripped_segments.insert(stripped_segments.begin(), last_segment);
									segments.pop_back();
								}
							}

							// Reconstruct namespace from remaining segments
							// First 3 segments are protocol ("s3:"), empty ("", from //), bucket
							dataset_namespace = segments[0] + "//" + segments[2];

							// Add any remaining middle segments to namespace
							if (segments.size() > 3) {
								vector<string> namespace_segments;
								for (size_t i = 3; i < segments.size(); i++) {
									namespace_segments.push_back(segments[i]);
								}
								if (!namespace_segments.empty()) {
									dataset_namespace += "/" + StringUtil::Join(namespace_segments, "/");
								}
							}

							// Name is what was stripped (database/table_name)
							dataset_name = StringUtil::Join(stripped_segments, "/");
						} else {
							// Fallback for unexpected path format
							dataset_namespace = normalized_location;
						}
					}

					CatalogInfo catalog_info;
					catalog_info.type = "hms";
					catalog_info.name = catalog_name;
					catalog_info.source = dataset_namespace; // Source should match namespace
					catalog_info.framework = "hive";

					// Add input dataset with HMS metadata
					// Pass the normalized namespace as db_path (not the raw storage_location)
					builder.AddInputDatasetWithFacets(dataset_namespace, dataset_name, fields, dataset_namespace,
					                                  catalog_info);

					// Add DatasetType facet with format information
					string dataset_subtype = hms_input_format.empty() ? "" : hms_input_format;
					builder.AddInputDatasetFacet_DatasetType(dataset_namespace, dataset_name, "TABLE", dataset_subtype);

					return; // Done processing this operator
				}
				// This is a table function (e.g., read_csv_auto, read_parquet)
				// Try to extract file paths from MultiFileBindData
				if (get.bind_data) {
					try {
						// Attempt to cast to MultiFileBindData
						auto *multi_file_data = dynamic_cast<MultiFileBindData *>(get.bind_data.get());
						if (multi_file_data && multi_file_data->file_list) {
							// Get the file paths from the file list
							auto file_paths = multi_file_data->file_list->GetAllFiles();

							// Extract schema information from the returned types
							json fields = json::array();
							for (size_t i = 0; i < get.names.size(); i++) {
								if (i < get.returned_types.size()) {
									fields.push_back(LineageEventBuilder::CreateSchemaField(
									    get.names[i], get.returned_types[i].ToString()));
								}
							}

							// Add each file as an input dataset
							for (const auto &file_info : file_paths) {
								string file_path = file_info.path;

								// Use "file" as namespace for file-based inputs
								string file_namespace = "file";

								// Add the file as an input dataset
								// Using simplified approach without catalog facets for files
								builder.AddInputDataset(file_namespace, file_path, fields);
							}
						}
					} catch (...) {
						// If casting fails, this is a different type of table function
						// We can ignore it for now
					}
				}
			}

			// ===== Handle table inserts (INSERT INTO table) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_INSERT) {
			auto &insert = op.Cast<LogicalInsert>();
			// Get the database path to store as metadata
			string db_path = GetCatalogPath(insert.table.catalog);

			// Extract catalog information for the catalog facet
			CatalogInfo catalog_info = ExtractCatalogInfo(insert.table.catalog);

			// Get fully qualified table name: catalog.schema.table
			string fully_qualified_name =
			    GetFullyQualifiedTableName(insert.table.catalog, insert.table.ParentSchema(), insert.table.name);

			// Extract schema information
			json fields = json::array();
			for (auto &col : insert.table.GetColumns().Logical()) {
				fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
			}

			// Add output dataset with both DataSource and Catalog facets
			builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
			                                   db_path, catalog_info);

			// Add DatasetType facet to distinguish this as a TABLE
			builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
			                                          "TABLE");

			// Add lifecycle state change facet
			// For INSERT, we use OVERWRITE as it modifies the dataset
			builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
			                                                   fully_qualified_name, "OVERWRITE");

			// ===== Handle table creation (CREATE TABLE) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
			auto &create = op.Cast<LogicalCreateTable>();
			// Get the database path to store as metadata
			string db_path = GetCatalogPath(create.schema.catalog);

			// Extract catalog information for the catalog facet
			CatalogInfo catalog_info = ExtractCatalogInfo(create.schema.catalog);

			// Get fully qualified table name: catalog.schema.table
			string fully_qualified_name =
			    GetFullyQualifiedTableName(create.schema.catalog, create.schema, create.info->Base().table);

			// Extract schema information from the CREATE TABLE definition
			json fields = json::array();
			for (auto &col : create.info->Base().columns.Logical()) {
				fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
			}

			// Add output dataset with both DataSource and Catalog facets
			builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
			                                   db_path, catalog_info);

			// Add DatasetType facet to distinguish this as a TABLE
			builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
			                                          "TABLE");

			// Add lifecycle state change facet (CREATE)
			builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
			                                                   fully_qualified_name, "CREATE");

			// ===== Handle view creation (CREATE VIEW) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_CREATE_VIEW) {
			auto &create = op.Cast<LogicalCreate>();
			// Cast info to CreateViewInfo
			if (create.info && create.info->type == CatalogType::VIEW_ENTRY) {
				auto &view_info = create.info->Cast<CreateViewInfo>();

				// Get the database path to store as metadata
				string db_path = GetCatalogPath(create.schema->catalog);

				// Extract catalog information for the catalog facet
				CatalogInfo catalog_info = ExtractCatalogInfo(create.schema->catalog);

				// Get fully qualified view name: catalog.schema.view
				string fully_qualified_name =
				    GetFullyQualifiedTableName(create.schema->catalog, *create.schema, view_info.view_name);

				// Extract schema information from the view definition
				// The view's schema is defined by the names and types in CreateViewInfo
				json fields = json::array();
				for (size_t i = 0; i < view_info.names.size(); i++) {
					string col_name = view_info.names[i];
					string col_type = (i < view_info.types.size()) ? view_info.types[i].ToString() : "UNKNOWN";
					fields.push_back(LineageEventBuilder::CreateSchemaField(col_name, col_type));
				}

				// Add output dataset with both DataSource and Catalog facets
				builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
				                                   db_path, catalog_info);

				// Add DatasetType facet to distinguish this as a VIEW
				builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
				                                          "VIEW");

				// Add lifecycle state change facet (CREATE)
				builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
				                                                   fully_qualified_name, "CREATE");

				// Extract inputs from the view's query by traversing the QueryNode tree
				// We need to manually bind the tables referenced in the view
				if (view_info.query && view_info.query->node) {
					ExtractInputsFromQueryNode(context, *view_info.query->node, builder);
				}
			}

			// ===== Handle table/view drop (DROP TABLE / DROP VIEW) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_DROP) {
			auto &simple = op.Cast<LogicalSimple>();
			if (simple.info && simple.info->info_type == ParseInfoType::DROP_INFO) {
				auto &drop_info = simple.info->Cast<DropInfo>();

				// Handle table and view drops
				if (drop_info.type == CatalogType::TABLE_ENTRY || drop_info.type == CatalogType::VIEW_ENTRY) {
					// Get catalog and schema from drop info
					auto &catalog = Catalog::GetCatalog(context, drop_info.catalog);
					string db_path = GetCatalogPath(catalog);
					CatalogInfo catalog_info = ExtractCatalogInfo(catalog);

					// Build fully qualified table/view name
					string fully_qualified_name;
					if (!drop_info.catalog.empty()) {
						fully_qualified_name = drop_info.catalog + "." + drop_info.schema + "." + drop_info.name;
					} else {
						fully_qualified_name = drop_info.schema + "." + drop_info.name;
					}

					// For DROP, we don't have schema information available
					json fields = json::array();

					// Determine dataset type based on what's being dropped
					string dataset_type = (drop_info.type == CatalogType::VIEW_ENTRY) ? "VIEW" : "TABLE";

					// Add output dataset
					builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name,
					                                   fields, db_path, catalog_info);

					// Add DatasetType facet to distinguish between TABLE and VIEW
					builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
					                                          dataset_type);

					// Add lifecycle state change facet (DROP)
					builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
					                                                   fully_qualified_name, "DROP");
				}
			}

			// ===== Handle table/view alterations (ALTER TABLE / ALTER VIEW) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_ALTER) {
			auto &simple = op.Cast<LogicalSimple>();
			if (simple.info && simple.info->info_type == ParseInfoType::ALTER_INFO) {
				auto &alter_info = simple.info->Cast<AlterInfo>();

				// Handle table and view alterations
				if (alter_info.GetCatalogType() == CatalogType::TABLE_ENTRY ||
				    alter_info.GetCatalogType() == CatalogType::VIEW_ENTRY) {
					// Get catalog and schema from alter info
					auto &catalog = Catalog::GetCatalog(context, alter_info.catalog);
					string db_path = GetCatalogPath(catalog);
					CatalogInfo catalog_info_obj = ExtractCatalogInfo(catalog);

					// Build fully qualified table/view name
					string fully_qualified_name;
					if (!alter_info.catalog.empty()) {
						fully_qualified_name = alter_info.catalog + "." + alter_info.schema + "." + alter_info.name;
					} else {
						fully_qualified_name = alter_info.schema + "." + alter_info.name;
					}

					// For ALTER, we don't have schema information available
					json fields = json::array();

					// Determine dataset type based on what's being altered
					string dataset_type = (alter_info.GetCatalogType() == CatalogType::VIEW_ENTRY) ? "VIEW" : "TABLE";

					// Determine if this is a RENAME operation (only for tables)
					if (alter_info.GetCatalogType() == CatalogType::TABLE_ENTRY &&
					    alter_info.type == AlterType::ALTER_TABLE) {
						auto &alter_table_info = alter_info.Cast<AlterTableInfo>();

						if (alter_table_info.alter_table_type == AlterTableType::RENAME_TABLE) {
							auto &rename_info = alter_table_info.Cast<RenameTableInfo>();

							// Build the new fully qualified name
							string new_fully_qualified_name;
							if (!alter_info.catalog.empty()) {
								new_fully_qualified_name =
								    alter_info.catalog + "." + alter_info.schema + "." + rename_info.new_table_name;
							} else {
								new_fully_qualified_name = alter_info.schema + "." + rename_info.new_table_name;
							}

							// Add output dataset with the new name
							builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(),
							                                   new_fully_qualified_name, fields, db_path,
							                                   catalog_info_obj);

							// Add DatasetType facet to distinguish this as a TABLE
							builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(),
							                                          new_fully_qualified_name, dataset_type);

							// Add lifecycle state change facet (RENAME) with previous identifier
							builder.AddOutputDatasetFacet_LifecycleStateChange(
							    LineageClient::Get().GetNamespace(), new_fully_qualified_name, "RENAME",
							    LineageClient::Get().GetNamespace(), fully_qualified_name);
						} else {
							// For other ALTER operations (ADD COLUMN, DROP COLUMN, etc.)
							builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(),
							                                   fully_qualified_name, fields, db_path, catalog_info_obj);

							// Add DatasetType facet to distinguish this as a TABLE
							builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(),
							                                          fully_qualified_name, dataset_type);

							// Add lifecycle state change facet (ALTER)
							builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
							                                                   fully_qualified_name, "ALTER");
						}
					} else {
						// For non-table ALTER operations (views, sequences, etc.)
						builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name,
						                                   fields, db_path, catalog_info_obj);

						// Add DatasetType facet to distinguish between TABLE and VIEW
						builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(),
						                                          fully_qualified_name, dataset_type);

						// Add lifecycle state change facet (ALTER)
						builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
						                                                   fully_qualified_name, "ALTER");
					}
				}
			}

			// ===== Handle UPDATE (OVERWRITE semantics) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_UPDATE) {
			auto &update = op.Cast<LogicalUpdate>();
			// Get the database path to store as metadata
			string db_path = GetCatalogPath(update.table.catalog);

			// Extract catalog information for the catalog facet
			CatalogInfo catalog_info = ExtractCatalogInfo(update.table.catalog);

			// Get fully qualified table name: catalog.schema.table
			string fully_qualified_name =
			    GetFullyQualifiedTableName(update.table.catalog, update.table.ParentSchema(), update.table.name);

			// Extract schema information
			json fields = json::array();
			for (auto &col : update.table.GetColumns().Logical()) {
				fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
			}

			// Add output dataset with both DataSource and Catalog facets
			builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
			                                   db_path, catalog_info);

			// Add DatasetType facet to distinguish this as a TABLE
			builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
			                                          "TABLE");

			// Add lifecycle state change facet (OVERWRITE for UPDATE operations)
			builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
			                                                   fully_qualified_name, "OVERWRITE");

			// ===== Handle DELETE (partial data removal) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_DELETE) {
			auto &del = op.Cast<LogicalDelete>();
			// Get the database path to store as metadata
			string db_path = GetCatalogPath(del.table.catalog);

			// Extract catalog information for the catalog facet
			CatalogInfo catalog_info = ExtractCatalogInfo(del.table.catalog);

			// Get fully qualified table name: catalog.schema.table
			string fully_qualified_name =
			    GetFullyQualifiedTableName(del.table.catalog, del.table.ParentSchema(), del.table.name);

			// Extract schema information
			json fields = json::array();
			for (auto &col : del.table.GetColumns().Logical()) {
				fields.push_back(LineageEventBuilder::CreateSchemaField(col.Name(), col.Type().ToString()));
			}

			// Add output dataset with both DataSource and Catalog facets
			builder.AddOutputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name, fields,
			                                   db_path, catalog_info);

			// Add DatasetType facet to distinguish this as a TABLE
			builder.AddOutputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(), fully_qualified_name,
			                                          "TABLE");

			// Note: DELETE operations modify data but don't fit cleanly into lifecycle states
			// We'll use OVERWRITE to indicate data modification
			builder.AddOutputDatasetFacet_LifecycleStateChange(LineageClient::Get().GetNamespace(),
			                                                   fully_qualified_name, "OVERWRITE");

			// ===== Handle COPY TO FILE (export to files) =====
		} else if (op.type == LogicalOperatorType::LOGICAL_COPY_TO_FILE) {
			auto &copy_to = op.Cast<LogicalCopyToFile>();

			// Determine output schema: prefer explicit expected_types/names if available
			json fields = json::array();
			if (!copy_to.names.empty() && !copy_to.expected_types.empty() &&
			    copy_to.names.size() == copy_to.expected_types.size()) {
				for (size_t i = 0; i < copy_to.names.size(); i++) {
					fields.push_back(
					    LineageEventBuilder::CreateSchemaField(copy_to.names[i], copy_to.expected_types[i].ToString()));
				}
			} else if (!op.children.empty()) {
				// Fallback: infer schema from child output types when explicit names/types are unavailable
				auto &child = *op.children[0];
				for (size_t i = 0; i < child.types.size(); i++) {
					string col_name = "col_" + std::to_string(i + 1);
					fields.push_back(LineageEventBuilder::CreateSchemaField(col_name, child.types[i].ToString()));
				}
			}

			// Use "file" namespace and the configured file path as dataset name
			// Note: for multi-file outputs (thread/partitioned), DuckDB manages actual paths.
			// We record the base path specified in the COPY TO command.
			string file_namespace = "file";
			string dataset_name = copy_to.file_path.empty() ? "unknown" : copy_to.file_path;

			builder.AddOutputDataset(file_namespace, dataset_name, fields);

			// Mark lifecycle: exporting typically creates or overwrites the target file
			builder.AddOutputDatasetFacet_LifecycleStateChange(file_namespace, dataset_name, "OVERWRITE");
		}
	}
};

//===--------------------------------------------------------------------===//
// Optimizer Hook Implementation
//===--------------------------------------------------------------------===//

// Forward declarations
static void FindViewReferences(ClientContext &context, QueryNode &node, unordered_set<string> &referenced_views);
static void FindViewReferencesInExpression(ClientContext &context, ParsedExpression &expr,
                                           unordered_set<string> &referenced_views);

/// @brief Helper function to find view references in a table reference tree
/// @param context Client context for accessing catalog
/// @param ref The table reference to traverse
/// @param referenced_views Output set to populate with view names
static void FindViewReferencesInTableRef(ClientContext &context, TableRef &ref,
                                         unordered_set<string> &referenced_views) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref.Cast<BaseTableRef>();

		// Check if this table reference is actually a view
		try {
			// Build list of catalogs to try
			vector<string> catalog_names;
			if (!base_table.catalog_name.empty()) {
				// Catalog specified, use it
				catalog_names.push_back(base_table.catalog_name);
			} else {
				// Catalog not specified - first try empty (let DuckDB decide)
				catalog_names.push_back("");

				// Also try to discover available catalogs
				try {
					auto &db = DatabaseInstance::GetDatabase(context);
					auto &db_manager = db.GetDatabaseManager();

					// Try to get catalogs
					// Try common catalog names that might exist
					vector<string> common_catalogs = {"memory", "temp"};
					for (const auto &common_cat : common_catalogs) {
						try {
							Catalog::GetCatalog(context, common_cat);
							catalog_names.push_back(common_cat);
						} catch (...) {
							// Catalog doesn't exist, skip
						}
					}

					// Try attached databases (HMS, etc.)
					// We'll check specific ones that might have views
					vector<string> extension_catalogs = {"hms", "hive", "hive_metastore"};
					for (const auto &ext_cat : extension_catalogs) {
						try {
							Catalog::GetCatalog(context, ext_cat);
							catalog_names.push_back(ext_cat);
						} catch (...) {
							// Catalog doesn't exist, skip
						}
					}
				} catch (...) {
					// Fallback to common catalog names
					catalog_names = {"", "memory", "temp"};
				}
			}

			// Build list of schemas to try
			vector<string> schemas_to_try;
			if (!base_table.schema_name.empty()) {
				// Schema specified, try it first, then empty
				schemas_to_try.push_back(base_table.schema_name);
				schemas_to_try.push_back("");
			} else {
				// Schema not specified - start with empty (let DuckDB use its search path)
				schemas_to_try.push_back("");
			}

			// Try all catalog/schema combinations
			bool found = false;
			for (const auto &catalog_name : catalog_names) {
				if (found)
					break;

				// Build schema list for this specific catalog
				vector<string> schemas_for_this_catalog;
				if (!base_table.schema_name.empty()) {
					// Schema specified, try it first, then empty
					schemas_for_this_catalog.push_back(base_table.schema_name);
					schemas_for_this_catalog.push_back("");
				} else {
					// Schema not specified - start with empty (let DuckDB use its search path)
					schemas_for_this_catalog.push_back("");

					// Try to discover schemas from this catalog
					if (!catalog_name.empty()) {
						try {
							auto &catalog = Catalog::GetCatalog(context, catalog_name);

							// Try to access common schema names to see which exist
							vector<string> common_schemas = {"main", "default", "temp", "information_schema"};
							for (const auto &schema : common_schemas) {
								try {
									catalog.GetSchema(context, schema);
									// Only add if not already in list
									if (std::find(schemas_for_this_catalog.begin(), schemas_for_this_catalog.end(),
									              schema) == schemas_for_this_catalog.end()) {
										schemas_for_this_catalog.push_back(schema);
									}
								} catch (...) {
									// Schema doesn't exist in this catalog, skip
								}
							}
						} catch (...) {
							// Schema discovery failed
						}
					}
				}

				for (const auto &schema_name : schemas_for_this_catalog) {
					try {
						EntryLookupInfo lookup_info(CatalogType::VIEW_ENTRY, base_table.table_name);
						auto entry = Catalog::GetEntry(context, catalog_name, schema_name, lookup_info,
						                               OnEntryNotFound::RETURN_NULL);

						if (entry && entry->type == CatalogType::VIEW_ENTRY) {
							// This is a view reference!
							referenced_views.insert(base_table.table_name);
							found = true;
							return; // Found it, no need to check other schemas/catalogs
						}
					} catch (...) {
						// Continue to next schema/catalog
					}
				}
			}
		} catch (...) {
			// Not a view or catalog lookup failed, ignore
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			FindViewReferencesInTableRef(context, *join.left, referenced_views);
		}
		if (join.right) {
			FindViewReferencesInTableRef(context, *join.right, referenced_views);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			FindViewReferences(context, *subquery.subquery->node, referenced_views);
		}
		break;
	}
	default:
		break;
	}
}

/// @brief Helper function to find view references in a query node tree
/// @param context Client context for accessing catalog
/// @param node The query node to traverse
/// @param referenced_views Output set to populate with view names
static void FindViewReferences(ClientContext &context, QueryNode &node, unordered_set<string> &referenced_views) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select_node = node.Cast<SelectNode>();
		if (select_node.from_table) {
			FindViewReferencesInTableRef(context, *select_node.from_table, referenced_views);
		}

		// Also check subqueries in the SELECT list (SELECT expressions)
		// This handles queries like: SELECT (SELECT ... FROM view) AS col
		for (auto &expr : select_node.select_list) {
			// Traverse the expression tree to find subqueries
			try {
				// Try to cast to subquery expression
				if (expr->type == ExpressionType::SUBQUERY) {
					auto &subquery_expr = expr->Cast<SubqueryExpression>();
					if (subquery_expr.subquery && subquery_expr.subquery->node) {
						FindViewReferences(context, *subquery_expr.subquery->node, referenced_views);
					}
				}
			} catch (...) {
				// Not a subquery, continue
			}
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = node.Cast<SetOperationNode>();
		for (auto &child : setop_node.children) {
			if (child) {
				FindViewReferences(context, *child, referenced_views);
			}
		}
		break;
	}
	default:
		break;
	}
}

void OpenLineageOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// Early exit if no plan to optimize
	if (!plan) {
		return;
	}

	// Try to get the query via input.context.GetCurrentQuery();
	// Due to bugs in DuckDB (https://github.com/duckdb/duckdb-java/issues/549), this may try to dereference a null
	// pointer internally So we wrap in try-catch to avoid crashing
	string query;
	try {
		query = input.context.GetCurrentQuery();
	} catch (...) {
		Printer::Print("Warning: Failed to get current query from context.");
		return;
	}

	if (query.empty()) {
		return;
	}

	// ===== Parse the query to detect view references BEFORE view expansion =====
	// We need to track which views are referenced so we can add them to lineage
	// DuckDB expands views during binding, which happens before PreOptimize,
	// so by the time we see the plan, the view references are already gone.
	// Solution: Parse the original SQL to find view references.
	Parser parser;
	try {
		parser.ParseQuery(query);
	} catch (...) {
		// If parsing fails, continue normally
	}

	// Track views that were referenced in the original query
	unordered_set<string> referenced_views;
	if (!parser.statements.empty()) {
		auto &statement = parser.statements[0];
		if (statement->type == StatementType::SELECT_STATEMENT) {
			auto &select_stmt = statement->Cast<SelectStatement>();
			if (select_stmt.node) {
				// Traverse the query node tree to find view references
				FindViewReferences(input.context, *select_stmt.node, referenced_views);
			}
		}
	}

	// Extract dependencies from referenced views so we can skip them during plan traversal
	// We store both fully qualified names and just table names for flexible matching
	unordered_set<string> view_dependencies;      // Tables that are dependencies of referenced views
	unordered_set<string> view_dependency_tables; // Just table names for flexible matching

	for (const auto &view_name : referenced_views) {
		// Build list of catalogs to try (similar to detection logic)
		vector<string> catalog_names;

		// First try empty catalog
		catalog_names.push_back("");

		// Then try to discover available catalogs
		try {
			auto &db = DatabaseInstance::GetDatabase(input.context);

			// Try common catalogs
			vector<string> common_catalogs = {"memory", "temp"};
			for (const auto &common_cat : common_catalogs) {
				try {
					Catalog::GetCatalog(input.context, common_cat);
					catalog_names.push_back(common_cat);
				} catch (...) {
					// Catalog doesn't exist
				}
			}

			// Try extension catalogs
			vector<string> extension_catalogs = {"hms", "hive", "hive_metastore"};
			for (const auto &ext_cat : extension_catalogs) {
				try {
					Catalog::GetCatalog(input.context, ext_cat);
					catalog_names.push_back(ext_cat);
				} catch (...) {
					// Catalog doesn't exist
				}
			}
		} catch (...) {
			// Use fallback
			catalog_names = {"", "memory"};
		}

		bool found_view_def = false;
		for (const auto &catalog_name : catalog_names) {
			if (found_view_def)
				break;

			// Build schema list for this specific catalog
			vector<string> schemas_for_this_catalog;
			// Always start with empty (let DuckDB decide)
			schemas_for_this_catalog.push_back("");

			// Try to discover schemas from this catalog
			if (!catalog_name.empty()) {
				try {
					auto &catalog = Catalog::GetCatalog(input.context, catalog_name);

					vector<string> common_schemas = {"main", "default", "temp", "information_schema"};
					for (const auto &schema : common_schemas) {
						try {
							catalog.GetSchema(input.context, schema);
							schemas_for_this_catalog.push_back(schema);
						} catch (...) {
							// Schema doesn't exist
						}
					}
				} catch (...) {
					// Schema discovery failed
				}
			}

			for (const auto &schema_name : schemas_for_this_catalog) {
				try {
					EntryLookupInfo lookup_info(CatalogType::VIEW_ENTRY, view_name);
					auto entry = Catalog::GetEntry(input.context, catalog_name, schema_name, lookup_info,
					                               OnEntryNotFound::RETURN_NULL);

					if (entry && entry->type == CatalogType::VIEW_ENTRY) {
						auto &view_entry = entry->Cast<ViewCatalogEntry>();
						if (view_entry.query && view_entry.query->node) {
							// Extract tables from view definition and add to view_dependencies
							ExtractTableNamesFromQueryNode(input.context, *view_entry.query->node, view_dependencies);
							// Also extract just table names for flexible matching
							ExtractTableNamesFromQueryNode(input.context, *view_entry.query->node,
							                               view_dependency_tables);
						}
						found_view_def = true;
						break;
					}
				} catch (...) {
					// Continue to next schema
				}
			}
		}
	}

	// Extract just the table names from fully qualified names for flexible matching
	for (const auto &fully_qualified : view_dependencies) {
		auto last_dot = fully_qualified.rfind('.');
		if (last_dot != string::npos) {
			string table_name = fully_qualified.substr(last_dot + 1);
			view_dependency_tables.insert(table_name);
		}
	}

	// Generate unique identifiers for this query execution
	string jobName = GenerateJobName(*plan, query); // Readable job name from plan analysis
	string runId = GenerateUUID();                  // Unique run ID for this execution
	string eventTime = GetCurrentISOTime();         // Current timestamp in ISO 8601 format

	// Build the START event using the factory builder
	auto builder = LineageEventBuilder::CreateStart();
	builder.WithRunId(runId)
	    .WithEventTime(eventTime)
	    .WithJob(LineageClient::Get().GetNamespace(), jobName)
	    .AddJobFacet_Sql(query)
	    .AddRunFacet_ProcessingEngine(DuckDB::LibraryVersion(), "DuckDB");

	// ===== Add views that were referenced in the query =====
	for (const auto &view_name : referenced_views) {
		// Look up the view in the catalog using a robust discovery approach
		bool found_view = false;

		// Build catalog list to try
		vector<string> catalog_names;
		catalog_names.push_back(""); // Try empty first

		// Discover available catalogs
		try {
			vector<string> common_catalogs = {"memory", "temp"};
			for (const auto &common_cat : common_catalogs) {
				try {
					Catalog::GetCatalog(input.context, common_cat);
					catalog_names.push_back(common_cat);
				} catch (...) {
				}
			}

			vector<string> extension_catalogs = {"hms", "hive", "hive_metastore"};
			for (const auto &ext_cat : extension_catalogs) {
				try {
					Catalog::GetCatalog(input.context, ext_cat);
					catalog_names.push_back(ext_cat);
				} catch (...) {
				}
			}
		} catch (...) {
			catalog_names = {"", "memory"};
		}

		// Try all combinations
		for (const auto &catalog_name : catalog_names) {
			if (found_view)
				break;

			// Build schema list for this specific catalog
			vector<string> schemas_for_this_catalog;
			// Always start with empty
			schemas_for_this_catalog.push_back("");

			// Try to discover schemas from this catalog
			if (!catalog_name.empty()) {
				try {
					auto &catalog = Catalog::GetCatalog(input.context, catalog_name);

					vector<string> common_schemas = {"main", "default", "temp", "information_schema"};
					for (const auto &schema : common_schemas) {
						try {
							catalog.GetSchema(input.context, schema);
							schemas_for_this_catalog.push_back(schema);
						} catch (...) {
							// Schema doesn't exist
						}
					}
				} catch (...) {
					// Schema discovery failed
				}
			}

			for (const auto &schema_name : schemas_for_this_catalog) {
				try {
					EntryLookupInfo lookup_info(CatalogType::VIEW_ENTRY, view_name);
					auto entry = Catalog::GetEntry(input.context, catalog_name, schema_name, lookup_info,
					                               OnEntryNotFound::RETURN_NULL);

					if (entry && entry->type == CatalogType::VIEW_ENTRY) {
						// This is a view! Add it to lineage
						auto &view_entry = entry->Cast<ViewCatalogEntry>();
						auto &catalog = Catalog::GetCatalog(input.context, catalog_name);

						// Get the database path to store as metadata
						string db_path = GetCatalogPath(catalog);

						// Extract catalog information for the catalog facet
						CatalogInfo catalog_info = ExtractCatalogInfo(catalog);

						// Get fully qualified view name: catalog.schema.view
						string fully_qualified_name;
						if (!schema_name.empty()) {
							fully_qualified_name = catalog_name + "." + schema_name + "." + view_entry.name;
						} else {
							fully_qualified_name = catalog_name + "." + view_entry.name;
						}

						// Extract schema information from the view
						json fields = json::array();
						auto column_info = view_entry.GetColumnInfo();
						if (column_info) {
							for (size_t i = 0; i < column_info->names.size(); i++) {
								fields.push_back(LineageEventBuilder::CreateSchemaField(
								    column_info->names[i], column_info->types[i].ToString()));
							}
						}

						// Add the view as an input dataset
						builder.AddInputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name,
						                                  fields, db_path, catalog_info);

						// Add DatasetType facet to mark this as a VIEW
						builder.AddInputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(),
						                                         fully_qualified_name, "VIEW");

						found_view = true;
						break;
					}
				} catch (...) {
					// Continue to next schema
				}
			}
		}

		if (!found_view) {
			try {
				// Try to get the view from the entry's parent schema if available
				EntryLookupInfo lookup_info(CatalogType::VIEW_ENTRY, view_name);
				auto entry = Catalog::GetEntry(input.context, "", "", lookup_info, OnEntryNotFound::RETURN_NULL);

				if (entry && entry->type == CatalogType::VIEW_ENTRY) {
					// This is a view! Add it to lineage
					auto &view_entry = entry->Cast<ViewCatalogEntry>();

					// Try to get the catalog from the parent schema
					Catalog *catalog_ptr = nullptr;
					try {
						catalog_ptr = &Catalog::GetCatalog(input.context, "");
					} catch (...) {
						// If that fails, try memory catalog
						try {
							catalog_ptr = &Catalog::GetCatalog(input.context, "memory");
						} catch (...) {
						}
					}

					if (catalog_ptr) {
						auto &catalog = *catalog_ptr;

						// Get the database path to store as metadata
						string db_path = GetCatalogPath(catalog);

						// Extract catalog information for the catalog facet
						CatalogInfo catalog_info = ExtractCatalogInfo(catalog);

						// Get fully qualified view name: catalog.schema.view
						string catalog_name = catalog.GetName();
						string fully_qualified_name = catalog_name + "." + view_entry.name;

						// Extract schema information from the view
						json fields = json::array();
						auto column_info = view_entry.GetColumnInfo();
						if (column_info) {
							for (size_t i = 0; i < column_info->names.size(); i++) {
								fields.push_back(LineageEventBuilder::CreateSchemaField(
								    column_info->names[i], column_info->types[i].ToString()));
							}
						}

						// Add the view as an input dataset
						builder.AddInputDatasetWithFacets(LineageClient::Get().GetNamespace(), fully_qualified_name,
						                                  fields, db_path, catalog_info);

						// Add DatasetType facet to mark this as a VIEW
						builder.AddInputDatasetFacet_DatasetType(LineageClient::Get().GetNamespace(),
						                                         fully_qualified_name, "VIEW");

						found_view = true;
					}
				}
			} catch (...) {
				// View lookup failed, skip it
			}
		}
	}

	// Traverse the logical plan to extract input/output datasets
	LineagePlanVisitor visitor(input.context, builder, view_dependencies, view_dependency_tables);
	visitor.Visit(*plan);

	// ===== Check for parent run context from environment =====
	const char *parent_run_id_env = std::getenv("OPENLINEAGE_PARENT_RUN_ID");
	if (parent_run_id_env) {
		const char *parent_job_ns = std::getenv("OPENLINEAGE_PARENT_JOB_NAMESPACE");
		const char *parent_job_name = std::getenv("OPENLINEAGE_PARENT_JOB_NAME");

		std::string parent_ns = parent_job_ns ? parent_job_ns : "";
		std::string parent_name = parent_job_name ? parent_job_name : "";

		builder.AddRunFacet_Parent(parent_run_id_env, parent_ns, parent_name);
	}

	// Build the START event
	json event = builder.Build();

	// ===== Deduplicate inputs by namespace+name =====
	if (event.contains("inputs") && event["inputs"].is_array()) {
		DatasetSet seen_inputs;
		json::const_iterator it = event["inputs"].begin();
		while (it != event["inputs"].end()) {
			const auto &input = *it;
			if (input.contains("namespace") && input.contains("name")) {
				DatasetKey key {input["namespace"], input["name"]};
				if (seen_inputs.count(key) > 0) {
					// Duplicate found, remove it
					it = event["inputs"].erase(it);
					continue;
				}
				seen_inputs.insert(key);
			}
			++it;
		}
	}

	// Send the deduplicated event
	LineageClient::Get().SendEvent(event.dump());

	// Extract outputs from the event for the sentinel
	json outputs = event.contains("outputs") ? event["outputs"] : json::array();

	// ===== Inject the lineage sentinel into the plan =====
	// Wrap the entire plan with a LogicalLineageSentinel operator
	// This will track execution and emit COMPLETE/FAIL events
	auto sentinel = make_uniq<LogicalLineageSentinel>(std::move(plan), runId, jobName, query, outputs);

	// Resolve output types (propagate from child)
	sentinel->ResolveOperatorTypes();

	// Replace the plan with the sentinel-wrapped version
	plan = std::move(sentinel);
}

} // namespace duckdb
