//===----------------------------------------------------------------------===//
// DuckDB DuckLineage Extension
//
// File: duck_lineage_optimizer.hpp
// Description: Optimizer extension that analyzes query plans and injects
//              lineage tracking. Runs during the pre-optimization phase to
//              extract input/output datasets and wrap the plan with a sentinel.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

/// @class DuckLineageOptimizer
/// @brief Optimizer extension that injects lineage tracking into query plans.
///
/// This class provides a pre-optimization hook that:
/// 1. Analyzes the logical plan to identify input and output datasets
/// 2. Extracts schema information from table operations
/// 3. Generates OpenLineage START events with query metadata
/// 4. Wraps the plan with a LogicalLineageSentinel to track completion
///
/// The optimizer extracts lineage from:
/// - LogicalGet: Input tables being read
/// - LogicalInsert: Output tables being written
/// - LogicalCreateTable: New tables being created
///
/// It respects parent run context from environment variables for integration
/// with external orchestration systems.
class DuckLineageOptimizer {
public:
	/// @brief Pre-optimization hook that injects lineage tracking.
	/// @param input Optimizer input containing the client context.
	/// @param plan The logical query plan to analyze and modify.
	/// @note Generates a START event and wraps the plan in a LogicalLineageSentinel.
	/// @note If the query is empty or plan is null, does nothing.
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
