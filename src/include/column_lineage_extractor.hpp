//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: column_lineage_extractor.hpp
// Description: Extracts column-level lineage from DuckDB logical plans.
//              Performs a bottom-up traversal of the plan to map each output
//              column to its source table columns, producing the OpenLineage
//              columnLineage dataset facet.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/main/client_context.hpp"
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

/// @brief Represents a source column from a specific dataset.
struct SourceColumn {
	string dataset_namespace;
	string dataset_name; // fully qualified (catalog.schema.table)
	string column_name;

	bool operator==(const SourceColumn &other) const {
		return dataset_namespace == other.dataset_namespace && dataset_name == other.dataset_name &&
		       column_name == other.column_name;
	}
};

/// @brief Hash function for SourceColumn (for deduplication in sets).
struct SourceColumnHash {
	size_t operator()(const SourceColumn &sc) const {
		return std::hash<string>()(sc.dataset_namespace) ^ (std::hash<string>()(sc.dataset_name) << 1) ^
		       (std::hash<string>()(sc.column_name) << 2);
	}
};

/// @brief Lineage information for a single column binding.
struct BindingLineage {
	vector<SourceColumn> sources;
	bool is_direct = true;
};

/// @brief Represents column lineage for a single output column.
struct ColumnLineageField {
	string output_column_name;
	vector<SourceColumn> input_fields;
	string transformation_type; // "DIRECT" or "INDIRECT"
};

/// @brief Key: packed (table_index << 32 | column_index)
using BindingLineageMap = unordered_map<uint64_t, BindingLineage>;

/// @class ColumnLineageExtractor
/// @brief Extracts column-level lineage from a DuckDB logical plan.
///
/// Performs a bottom-up traversal of the logical plan tree, building a map
/// from each ColumnBinding to its source table columns. After traversal,
/// resolves the output columns of the top-level operator to produce
/// OpenLineage columnLineage field entries.
class ColumnLineageExtractor {
public:
	explicit ColumnLineageExtractor(ClientContext &context);

	/// @brief Build the binding-to-source lineage map by traversing the plan.
	/// @param op The root logical operator to traverse.
	void BuildLineageMap(LogicalOperator &op);

	/// @brief Extract column lineage for a specific output dataset.
	/// @param op The root logical operator (to get output column bindings/names).
	/// @param dataset_namespace The namespace of the output dataset.
	/// @param dataset_name The name of the output dataset.
	/// @return Vector of ColumnLineageField entries for each resolvable output column.
	vector<ColumnLineageField> ExtractOutputColumnLineage(LogicalOperator &op, const string &dataset_namespace,
	                                                      const string &dataset_name);

private:
	ClientContext &context;
	BindingLineageMap lineage_map;

	/// @brief Pack a ColumnBinding into a uint64_t key.
	static uint64_t PackBinding(const ColumnBinding &binding);

	/// @brief Recursively traverse the plan bottom-up, populating lineage_map.
	void TraversePlan(LogicalOperator &op);

	// Operator-specific handlers
	void HandleGet(LogicalOperator &op);
	void HandleProjection(LogicalOperator &op);
	void HandleFilter(LogicalOperator &op);
	void HandleJoin(LogicalOperator &op);
	void HandleAggregate(LogicalOperator &op);
	void HandleSetOperation(LogicalOperator &op);
	void HandleWindow(LogicalOperator &op);
	void HandleDefaultPassthrough(LogicalOperator &op);

	/// @brief Resolve an expression tree to find all source columns it references.
	/// @param expr The expression to resolve.
	/// @return BindingLineage with all resolved source columns.
	BindingLineage ResolveExpression(Expression &expr);

	/// @brief Deduplicate source columns in a BindingLineage.
	static void DeduplicateSources(BindingLineage &lineage);
};

} // namespace duckdb
