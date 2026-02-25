//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_utils.hpp
// Description: Utility functions for the OpenLineage extension.
//              Provides cryptographic hashing, UUID generation, and timestamp
//              formatting utilities used throughout the extension.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include <openssl/evp.h>
#include <string>
#include <string_view>
#include <sstream>
#include <iomanip>
#include <random>
#include <algorithm>
#include <cctype>
#include <vector>
#include <chrono>
#include <cstdint>
#include <unordered_map>

namespace duckdb {

class Catalog;

/// @brief Holds catalog metadata for OpenLineage catalog facet
/// @note Optional fields use empty strings to indicate absence (C++11 compatible)
struct CatalogInfo {
	// Required fields
	std::string framework; // e.g., "duckdb"
	std::string type;      // e.g., "native", "attached", "memory"
	std::string name;      // catalog name

	// Optional fields (empty string = not available)
	std::string metadata_uri;  // URI to metadata/catalog
	std::string warehouse_uri; // URI to data warehouse
	std::string source;        // e.g., "duckdb"
};

/// @brief Calculate the SHA-256 hash of a string using OpenSSL.
/// @param str Input string to hash.
/// @return Hexadecimal string representation of the SHA-256 hash (64 characters).
/// @throws std::runtime_error if OpenSSL operations fail.
/// @note Used to generate deterministic job names from SQL queries.
static std::string CalculateSHA256(const std::string &str) {
	unsigned char hash[EVP_MAX_MD_SIZE];
	unsigned int hash_len = 0;

	EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
	if (!mdctx) {
		throw std::runtime_error("Failed to create EVP_MD_CTX");
	}

	if (EVP_DigestInit_ex(mdctx, EVP_sha256(), nullptr) != 1) {
		EVP_MD_CTX_free(mdctx);
		throw std::runtime_error("Failed to initialize SHA-256 digest");
	}

	if (EVP_DigestUpdate(mdctx, str.c_str(), str.size()) != 1) {
		EVP_MD_CTX_free(mdctx);
		throw std::runtime_error("Failed to update SHA-256 digest");
	}

	if (EVP_DigestFinal_ex(mdctx, hash, &hash_len) != 1) {
		EVP_MD_CTX_free(mdctx);
		throw std::runtime_error("Failed to finalize SHA-256 digest");
	}

	EVP_MD_CTX_free(mdctx);

	std::stringstream ss;
	ss << std::hex << std::setfill('0');
	for (unsigned int i = 0; i < hash_len; i++) {
		ss << std::setw(2) << static_cast<unsigned int>(hash[i]);
	}
	return ss.str();
}

/// @brief Generate a random UUID version 7 (RFC 9562).
/// @return String representation of the UUID in the format:
///         xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
///         where 7 indicates version 7 and y is one of 8, 9, A, or B.
/// @note Uses timestamp-based generation for natural sorting and uniqueness.
/// @note Format: 48-bit timestamp (ms) + 12-bit random + 62-bit random
/// @note Used to generate unique run IDs for OpenLineage events.
static std::string GenerateUUID() {
	static std::random_device rd;
	static std::mt19937 gen(rd());
	static std::uniform_int_distribution<uint64_t> dis64(0, UINT64_MAX);
	static std::uniform_int_distribution<uint32_t> dis32(0, UINT32_MAX);

	// Get current timestamp in milliseconds since Unix epoch
	auto now = std::chrono::system_clock::now();
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	uint64_t timestamp = static_cast<uint64_t>(ms);

	// Generate random bits for the rest of the UUID
	uint64_t rand_a = dis64(gen);
	uint64_t rand_b = dis64(gen);

	// Build UUID v7:
	// timestamp_ms (48 bits) | rand_a (12 bits) | version (4 bits) | rand_b (12 bits) | variant (2 bits) | rand_c (62
	// bits)

	// First 48 bits: timestamp in milliseconds
	uint64_t time_hi = (timestamp >> 16) & 0xFFFFFFFF;
	uint64_t time_lo = timestamp & 0xFFFF;

	// Next 12 bits: random + 4 bits version (7)
	uint64_t rand_and_version = ((rand_a & 0x0FFF) << 4) | 0x7;

	// Next 14 bits: 2 bits variant (10) + 12 bits random
	uint64_t variant_and_rand = 0x8000 | (rand_b & 0x0FFF);

	// Last 48 bits: random
	uint64_t rand_final = dis64(gen) & 0xFFFFFFFFFFFF;

	std::stringstream ss;
	ss << std::hex << std::setfill('0');

	// Format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
	ss << std::setw(8) << time_hi;
	ss << "-";
	ss << std::setw(4) << time_lo;
	ss << "-";
	ss << std::setw(4) << rand_and_version;
	ss << "-";
	ss << std::setw(4) << variant_and_rand;
	ss << "-";
	ss << std::setw(12) << rand_final;

	return ss.str();
}

/// @brief Get the current timestamp in ISO 8601 format with UTC timezone.
/// @return String in the format: YYYY-MM-DDTHH:MM:SS.ffffffZ
///         where T separates date and time, and Z indicates UTC timezone.
/// @note Uses DuckDB's internal Timestamp type for consistency.
/// @note Used for eventTime fields in OpenLineage events.
static std::string GetCurrentISOTime() {
	auto now = Timestamp::GetCurrentTimestamp();
	std::string ts = Timestamp::ToString(now);
	auto space_pos = ts.find(' ');
	if (space_pos != std::string::npos) {
		ts[space_pos] = 'T';
	}
	ts += "Z";
	return ts;
}

/// @brief Extract the base path from a full table storage location.
/// @param location The full storage location path (e.g., "s3://bucket/schema/table").
/// @param schema_name The schema name of the table.
/// @param table_name The table name.
/// @return The base path with schema and table name removed (e.g., "s3://bucket").
/// @note Handles cases where schema may be "default" or omitted.
/// @note Assumes the path is in the format: base_path/schema/table or base_path/table.
static string extract_base_path(const string &location, const string &schema_name, const string &table_name) {
	string path = location;
	if (!(StringUtil::StartsWith(path, "s3a://") || StringUtil::StartsWith(path, "s3://"))) {
		return location; // only handle s3-style URIs here
	}

	bool is_s3a = StringUtil::StartsWith(path, "s3a://");
	string prefix = is_s3a ? "s3a://" : "s3://";
	string no_scheme = path.substr(prefix.size());

	// Split into segments
	vector<string> parts;
	size_t start = 0;
	while (start < no_scheme.size()) {
		auto pos = no_scheme.find('/', start);
		if (pos == string::npos) {
			parts.push_back(no_scheme.substr(start));
			break;
		}
		parts.push_back(no_scheme.substr(start, pos - start));
		start = pos + 1;
	}

	if (parts.empty()) {
		return prefix; // weird, return scheme only
	}

	// Try to strip trailing table and schema segments if they match
	// e.g. .../warehouse/.../<schema>/<table> -> keep up to warehouse
	bool stripped = false;
	// If last segment equals table_name, pop it
	if (!table_name.empty() && parts.size() > 1 && parts.back() == table_name) {
		parts.pop_back();
		stripped = true;
	}
	// If new last segment equals schema_name, pop it
	if (!schema_name.empty() && parts.size() > 1 && parts.back() == schema_name) {
		parts.pop_back();
		stripped = true;
	}

	// If we stripped at least one of schema/table, return the remaining prefix as data root
	if (stripped) {
		string joined = prefix;
		for (size_t i = 0; i < parts.size(); i++) {
			if (i) {
				joined += "/";
			}
			joined += parts[i];
		}
		if (!StringUtil::EndsWith(joined, "/")) {
			joined += "/";
		}
		return joined;
	}

	// Fallback: return bucket or bucket+first-segment (likely warehouse root)
	if (parts.size() == 1) {
		return prefix + parts[0] + "/";
	}
	// join bucket + first segment
	string result = prefix + parts[0] + "/" + parts[1];
	if (!StringUtil::EndsWith(result, "/")) {
		result += "/";
	}
	return result;
}

/// @brief Get a fully qualified table name from catalog entry components.
/// @param catalog The catalog containing the table.
/// @param schema The schema containing the table.
/// @param table_name The name of the table.
/// @return Fully qualified name in the format: catalog.schema.table
/// @note Used to generate fully qualified dataset names for OpenLineage events.
std::string GetFullyQualifiedTableName(Catalog &catalog, SchemaCatalogEntry &schema, const std::string &table_name);

/// @brief Sanitize a string for use in a job name by replacing invalid characters.
/// @param str Input string to sanitize.
/// @return Sanitized string with only alphanumeric characters, underscores, and hyphens.
/// @note Replaces spaces and other characters with underscores, removes consecutive underscores.
std::string SanitizeJobNamePart(const std::string &str);

/// @brief Infer the statement type from a logical plan's root operator.
/// @param plan The logical operator plan to analyze.
/// @return String representing the statement type (e.g., "SELECT", "INSERT", "CREATE_TABLE").
/// @note Examines the plan structure to determine the primary operation type.
std::string InferStatementType(const LogicalOperator &plan);

/// @brief Extract table names from a logical plan by traversing the operator tree.
/// @param plan The logical operator plan to analyze.
/// @param table_names Output vector to collect table names.
/// @param max_tables Maximum number of table names to collect (default: 3).
/// @note Recursively visits the plan to find table references from GET, INSERT, CREATE operations.
void ExtractTableNames(const LogicalOperator &plan, std::vector<std::string> &table_names, size_t max_tables = 3);

/// @brief Generate a readable job name from a logical plan.
/// @param plan The logical operator plan to analyze.
/// @param query The SQL query text (used as fallback for hash component).
/// @param max_length Maximum length of the generated job name (default: 64).
/// @return Human-readable job name in format: STATEMENT_TYPE_table1_table2[_hash]
/// @note Combines statement type, table names, and optionally a short hash for uniqueness.
/// @example "SELECT_customers_orders", "INSERT_sales", "CREATE_TABLE_users"
std::string GenerateJobName(const LogicalOperator &plan, const std::string &query, size_t max_length = 64);

/// @brief Extract catalog information from a DuckDB catalog for OpenLineage catalog facet.
/// @param catalog The DuckDB catalog to extract information from.
/// @return CatalogInfo structure with catalog metadata (best-effort extraction).
/// @note Only populates fields that can be reliably determined from the catalog.
/// @note Framework is always "duckdb", type varies (native/attached/memory), name from catalog.
CatalogInfo ExtractCatalogInfo(Catalog &catalog);

/// @brief Format a database path for use in OpenLineage facets.
/// @param catalog_path The raw database path from the catalog.
/// @return Properly formatted URI or path for OpenLineage.
/// @note Handles PostgreSQL connection strings, local files, and URIs.
/// @note Converts "postgres:dbname=X host=Y port=Z" to "postgres://Y:Z/X"
/// @note Prepends "file://" to local file paths that don't have a scheme.
std::string FormatDatabasePath(const std::string &catalog_path);

} // namespace duckdb
