//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_utils.cpp
// Description: Implementation of utility functions for job name generation.
//===----------------------------------------------------------------------===//

#include "lineage_utils.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

std::string GetFullyQualifiedTableName(Catalog &catalog, SchemaCatalogEntry &schema, const std::string &table_name) {
	// Build fully qualified name: catalog.schema.table
	const std::string &catalog_name = catalog.GetName();
	const std::string &schema_name = schema.name;

	// Format as catalog.schema.table
	return catalog_name + "." + schema_name + "." + table_name;
}

std::string SanitizeJobNamePart(const std::string &str) {
	if (str.empty()) {
		return str;
	}

	std::string result;
	result.reserve(str.size());
	bool last_was_underscore = false;

	// Characters that should be replaced with underscore
	auto is_separator = [](char c) {
		return c == '_' || c == '-' || c == ' ' || c == '.' || c == ',' || c == ';';
	};

	for (char c : str) {
		if (std::isalnum(static_cast<unsigned char>(c))) {
			result += c;
			last_was_underscore = false;
		} else if (is_separator(c) && !last_was_underscore && !result.empty()) {
			result += '_';
			last_was_underscore = true;
		}
	}

	// Remove trailing underscore
	if (!result.empty() && result.back() == '_') {
		result.pop_back();
	}

	return result;
}

// Helper functions to analyze operator types in a plan
static bool ContainsOperatorType(const LogicalOperator &plan, LogicalOperatorType target_type) {
	if (plan.type == target_type) {
		return true;
	}
	for (const auto &child : plan.children) {
		if (ContainsOperatorType(*child, target_type)) {
			return true;
		}
	}
	return false;
}

// Check if any of multiple operator types exist in the plan
static bool ContainsAnyOperatorType(const LogicalOperator &plan, const std::vector<LogicalOperatorType> &types) {
	for (auto type : types) {
		if (ContainsOperatorType(plan, type)) {
			return true;
		}
	}
	return false;
}

static size_t CountOperatorType(const LogicalOperator &plan, LogicalOperatorType target_type) {
	size_t count = (plan.type == target_type) ? 1 : 0;
	for (const auto &child : plan.children) {
		count += CountOperatorType(*child, target_type);
	}
	return count;
}

std::string InferStatementType(const LogicalOperator &plan) {
	// Lookup table for DML/DDL operator types
	static const unordered_map<LogicalOperatorType, string> operator_type_names = {
	    {LogicalOperatorType::LOGICAL_INSERT, "INSERT"},
	    {LogicalOperatorType::LOGICAL_DELETE, "DELETE"},
	    {LogicalOperatorType::LOGICAL_UPDATE, "UPDATE"},
	    {LogicalOperatorType::LOGICAL_MERGE_INTO, "MERGE"},
	    {LogicalOperatorType::LOGICAL_CREATE_TABLE, "CREATE_TABLE"},
	    {LogicalOperatorType::LOGICAL_CREATE_INDEX, "CREATE_INDEX"},
	    {LogicalOperatorType::LOGICAL_CREATE_VIEW, "CREATE_VIEW"},
	    {LogicalOperatorType::LOGICAL_CREATE_SCHEMA, "CREATE_SCHEMA"},
	    {LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, "CREATE_SEQUENCE"},
	    {LogicalOperatorType::LOGICAL_CREATE_MACRO, "CREATE_MACRO"},
	    {LogicalOperatorType::LOGICAL_CREATE_TYPE, "CREATE_TYPE"},
	    {LogicalOperatorType::LOGICAL_DROP, "DROP"},
	    {LogicalOperatorType::LOGICAL_ALTER, "ALTER"},
	    {LogicalOperatorType::LOGICAL_COPY_TO_FILE, "COPY"},
	    {LogicalOperatorType::LOGICAL_VACUUM, "VACUUM"},
	    {LogicalOperatorType::LOGICAL_EXPLAIN, "EXPLAIN"},
	    {LogicalOperatorType::LOGICAL_ATTACH, "ATTACH"},
	    {LogicalOperatorType::LOGICAL_DETACH, "DETACH"},
	    {LogicalOperatorType::LOGICAL_EXPORT, "EXPORT"}};

	// Check if it's a DML/DDL operation
	auto it = operator_type_names.find(plan.type);
	if (it != operator_type_names.end()) {
		return it->second;
	}

	// For SELECT queries, analyze the plan structure to determine the type

	// Check for write operations in children (e.g., CREATE TABLE AS SELECT)
	for (const auto &child : plan.children) {
		if (child->type == LogicalOperatorType::LOGICAL_INSERT ||
		    child->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
			return InferStatementType(*child);
		}
	}

	// Analyze SELECT query characteristics
	const auto join_types = {LogicalOperatorType::LOGICAL_JOIN, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
	                         LogicalOperatorType::LOGICAL_CROSS_PRODUCT, LogicalOperatorType::LOGICAL_ASOF_JOIN};

	bool has_aggregate = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	bool has_join = ContainsAnyOperatorType(plan, join_types);
	bool has_window = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_WINDOW);
	bool has_cte = ContainsAnyOperatorType(
	    plan, {LogicalOperatorType::LOGICAL_MATERIALIZED_CTE, LogicalOperatorType::LOGICAL_RECURSIVE_CTE});
	bool has_union = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_UNION);
	bool has_except = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_EXCEPT);
	bool has_intersect = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_INTERSECT);
	bool has_distinct = ContainsOperatorType(plan, LogicalOperatorType::LOGICAL_DISTINCT);

	// Build a more specific query type
	std::string query_type = "SELECT";

	// Prioritize the most significant features
	if (has_union || has_except || has_intersect) {
		if (has_union)
			query_type = "SELECT_UNION";
		else if (has_except)
			query_type = "SELECT_EXCEPT";
		else
			query_type = "SELECT_INTERSECT";
	} else if (has_cte) {
		query_type = "SELECT_CTE";
	} else if (has_aggregate && has_join) {
		query_type = "SELECT_AGG_JOIN";
	} else if (has_aggregate) {
		query_type = "SELECT_AGG";
	} else if (has_join) {
		size_t join_count = CountOperatorType(plan, LogicalOperatorType::LOGICAL_JOIN) +
		                    CountOperatorType(plan, LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		query_type = (join_count > 1) ? "SELECT_MULTIJOIN" : "SELECT_JOIN";
	} else if (has_window) {
		query_type = "SELECT_WINDOW";
	} else if (has_distinct) {
		query_type = "SELECT_DISTINCT";
	}

	return query_type;
}

void ExtractTableNames(const LogicalOperator &plan, std::vector<std::string> &table_names, size_t max_tables) {
	if (table_names.size() >= max_tables) {
		return;
	}

	// Extract table name based on operator type
	if (plan.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan.Cast<LogicalGet>();
		if (get.GetTable()) {
			table_names.push_back(get.GetTable()->name);
		}
	} else if (plan.type == LogicalOperatorType::LOGICAL_INSERT) {
		auto &insert = plan.Cast<LogicalInsert>();
		table_names.push_back(insert.table.name);
	} else if (plan.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		auto &create = plan.Cast<LogicalCreateTable>();
		if (create.info) {
			table_names.push_back(create.info->Base().table);
		}
	} else if (plan.type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = plan.Cast<LogicalDelete>();
		table_names.push_back(del.table.name);
	} else if (plan.type == LogicalOperatorType::LOGICAL_UPDATE) {
		auto &update = plan.Cast<LogicalUpdate>();
		table_names.push_back(update.table.name);
	}

	// Recursively process children
	for (const auto &child : plan.children) {
		if (table_names.size() >= max_tables) {
			break;
		}
		ExtractTableNames(*child, table_names, max_tables);
	}
}

std::string GenerateJobName(const LogicalOperator &plan, const std::string &query, size_t max_length) {
	// Get statement type
	std::string stmt_type = InferStatementType(plan);

	// Extract table names
	std::vector<std::string> table_names;
	ExtractTableNames(plan, table_names, 3);

	// Calculate hash of the query for uniqueness (always include this)
	// This ensures that only identical queries get merged into the same job entity
	std::string hash = CalculateSHA256(query);
	std::string short_hash = hash.substr(0, 8);

	// Build the job name
	std::string job_name = stmt_type;

	// Add table names if available (but reserve space for hash)
	size_t max_prefix_length = max_length - 9; // Reserve 9 chars for "_" + 8-char hash
	for (const auto &table : table_names) {
		std::string sanitized = SanitizeJobNamePart(table);
		if (!sanitized.empty()) {
			std::string tentative_name = job_name + "_" + sanitized;
			if (tentative_name.length() <= max_prefix_length) {
				job_name = tentative_name;
			} else {
				// Can't fit more tables, stop adding
				break;
			}
		}
	}

	// Always append the hash to ensure unique identification
	job_name += "_" + short_hash;

	return job_name;
}

// Helper function to trim leading and trailing whitespace
static std::string Trim(const std::string &str) {
	if (str.empty()) {
		return str;
	}
	size_t start = str.find_first_not_of(" \t\r\n");
	if (start == string::npos) {
		return "";
	}
	size_t end = str.find_last_not_of(" \t\r\n");
	return str.substr(start, end - start + 1);
}

// Helper function to detect if a string is a PostgreSQL connection string
static bool IsPostgresConnectionString(const std::string &str) {
	// Check for explicit postgres: prefix
	constexpr const char *POSTGRES_PREFIX = "postgres:";
	if (str.find(POSTGRES_PREFIX) == 0) {
		return true;
	}
	// Check for postgres connection parameters (key=value pairs with host AND port)
	// Require at least host= and port= to reduce false positives on file paths
	size_t host_pos = str.find("host=");
	size_t port_pos = str.find("port=");
	if (host_pos != string::npos && port_pos != string::npos) {
		// Additional check: ensure they look like key=value pairs (not substrings)
		// by verifying they're at start or preceded by space
		bool valid_host = (host_pos == 0 || str[host_pos - 1] == ' ');
		bool valid_port = (port_pos == 0 || str[port_pos - 1] == ' ');
		return valid_host && valid_port;
	}
	return false;
}

// Helper function to parse PostgreSQL connection string and convert to proper URI
static std::string ParsePostgresConnectionString(const std::string &conn_str) {
	// Expected format: postgres:dbname=<name> host=<host> port=<port> user=<user> password=<pass>
	// OR just: dbname=<name> host=<host> port=<port> user=<user> password=<pass>
	string dbname;
	string host;
	string port;

	// Remove postgres: prefix if present
	string work_str = conn_str;
	constexpr const char *POSTGRES_PREFIX = "postgres:";
	if (work_str.find(POSTGRES_PREFIX) == 0) {
		work_str = work_str.substr(strlen(POSTGRES_PREFIX));
	}

	// Parse key=value pairs (space-separated)
	size_t pos = 0;
	while (pos < work_str.length()) {
		// Skip leading whitespace
		pos = work_str.find_first_not_of(" \t", pos);
		if (pos == string::npos) {
			break;
		}

		// Find the next space or end
		size_t space_pos = work_str.find(' ', pos);
		string token = (space_pos == string::npos) ? work_str.substr(pos) : work_str.substr(pos, space_pos - pos);
		pos = (space_pos == string::npos) ? work_str.length() : space_pos + 1;

		// Parse key=value
		size_t eq_pos = token.find('=');
		if (eq_pos != string::npos && eq_pos < token.length() - 1) {
			string key = Trim(token.substr(0, eq_pos));
			string value = Trim(token.substr(eq_pos + 1));

			if (key == "dbname") {
				dbname = value;
			} else if (key == "host") {
				host = value;
			} else if (key == "port") {
				port = value;
			}
		}
	}

	// Build postgres:// URI (only if we have a host)
	if (!host.empty()) {
		string uri = "postgres://" + host;
		if (!port.empty()) {
			uri += ":" + port;
		}
		if (!dbname.empty()) {
			uri += "/" + dbname;
		}
		return uri;
	}

	// Fallback: return original string if parsing failed
	return conn_str;
}

// Helper function to format a database path for use in metadata_uri
static std::string FormatMetadataPath(const std::string &db_path) {
	if (db_path.empty()) {
		return db_path;
	}

	// Check if it's a PostgreSQL connection string
	if (IsPostgresConnectionString(db_path)) {
		return ParsePostgresConnectionString(db_path);
	}

	// Check if it already has a URI scheme (contains ://)
	if (db_path.find("://") != string::npos) {
		return db_path;
	}

	// Check for other connection string formats (mysql:, sqlite:, etc.)
	if (db_path.find(':') != string::npos && db_path.find(':') < db_path.find('/')) {
		return db_path;
	}

	// Otherwise, treat it as a local file path
	return "file://" + db_path;
}

CatalogInfo ExtractCatalogInfo(Catalog &catalog) {
	CatalogInfo info;
	info.framework = "duckdb";
	info.source = "duckdb";
	info.name = catalog.GetName();

	// Handle system and temporary catalogs first
	if (catalog.IsSystemCatalog()) {
		info.type = "system";
		info.metadata_uri = "duckdb:system";
		return info;
	}

	if (catalog.IsTemporaryCatalog()) {
		info.type = "temporary";
		info.metadata_uri = "duckdb:memory";
		return info;
	}

	// Get database path; default to memory if unavailable
	string db_path;
	bool in_memory = true;
	try {
		db_path = catalog.GetDBPath();
		in_memory = catalog.InMemory();
	} catch (...) {
		// Assume special catalog
	}

	// Handle in-memory databases
	if (in_memory || db_path.empty() || db_path == ":memory:") {
		info.type = "memory";
		info.metadata_uri = "duckdb:memory";
		return info;
	}

	// Handle file-based and extension catalogs
	try {
		auto &attached_db = catalog.GetAttached();
		string catalog_type = catalog.GetCatalogType();

		if (catalog_type == "duckdb") {
			// Regular DuckDB database file
			info.type = attached_db.IsInitialDatabase() ? "native"
			            : attached_db.IsReadOnly()      ? "attached_readonly"
			                                            : "attached";
			info.metadata_uri = FormatMetadataPath(db_path);
			info.warehouse_uri = info.metadata_uri;
		} else if (catalog_type == "ducklake") {
			// DuckLake catalog
			info.type = "ducklake";
			info.metadata_uri = FormatMetadataPath(db_path);
			// warehouse_uri remains empty for DuckLake
		} else {
			// Extension-based catalog (Postgres, Iceberg, Delta Lake, etc.)
			info.type = catalog_type;
			info.metadata_uri = FormatMetadataPath(db_path);
		}
	} catch (...) {
		// Fallback to conservative defaults
		info.type = "unknown";
		info.metadata_uri = "";
		info.warehouse_uri = "";
	}

	return info;
}

std::string FormatDatabasePath(const std::string &catalog_path) {
	if (catalog_path.empty() || catalog_path == "duckdb_memory" || catalog_path == ":memory:") {
		return catalog_path;
	}

	// Check if it's a PostgreSQL connection string (with or without postgres: prefix)
	if (IsPostgresConnectionString(catalog_path)) {
		return ParsePostgresConnectionString(catalog_path);
	}

	// Check if it already has a URI scheme (contains ://)
	if (catalog_path.find("://") != std::string::npos) {
		return catalog_path;
	}

	// Otherwise, treat it as a local file path
	return "file://" + catalog_path;
}

} // namespace duckdb
