//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: physical_lineage_sentinel.hpp
// Description: Physical operator that tracks query execution and emits
//              OpenLineage completion events. Acts as a pass-through operator
//              that counts rows and sends COMPLETE/FAIL events on destruction.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include <chrono>
#include <nlohmann/json.hpp>

namespace duckdb {

using json = nlohmann::json;

/// @class PhysicalLineageSentinel
/// @brief Physical operator that tracks execution and emits lineage events.
///
/// This operator is a pass-through that doesn't modify data but tracks execution:
/// - Counts rows processed during execution
/// - Records execution timing
/// - Emits COMPLETE event on successful completion (in destructor)
/// - Emits FAIL event if an exception occurs (detected in destructor)
///
/// The operator sits at the root of the physical plan and all data flows through it.
/// It uses the destructor to emit final events, ensuring events are sent even if
/// query execution is interrupted.
///
/// Thread Safety: The operator's global state tracks row counts atomically to
/// support parallel execution.
class PhysicalLineageSentinel : public PhysicalOperator {
public:
	/// @brief Type identifier for this custom physical operator.
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	/// @brief Construct a physical lineage sentinel operator.
	/// @param physical_plan The physical plan (used by base class).
	/// @param types Output column types (inherited from child).
	/// @param estimated_cardinality Estimated number of rows.
	/// @param run_id Unique run identifier (UUID).
	/// @param job_name Job identifier (SHA-256 of query).
	/// @param query Original SQL query text.
	/// @param outputs JSON array of output datasets.
	PhysicalLineageSentinel(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                        string run_id, string job_name, string query, json outputs);

	/// @brief Destructor that emits COMPLETE or FAIL event.
	/// @note Detects exceptions via std::uncaught_exception() to determine event type.
	/// @note Includes row count statistics in the completion event.
	~PhysicalLineageSentinel() override;

	// ===== Lineage Metadata =====
	string run_id;   ///< Unique ID for this query run
	string job_name; ///< Job identifier (hash of query)
	string query;    ///< Original SQL query text
	json outputs;    ///< JSON array of output datasets

	// ===== Operator Interface =====

	/// @brief Execute the operator by passing through data and counting rows.
	/// @param context Execution context.
	/// @param input Input data chunk from child operator.
	/// @param chunk Output data chunk (referenced to input).
	/// @param gstate Global operator state (tracks row counts).
	/// @param state Local operator state (unused).
	/// @return Always returns NEED_MORE_INPUT to continue processing.
	/// @note This operator is transparent - it simply references the input to output.
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	/// @brief Indicate that this operator supports parallel execution.
	/// @return Always returns true.
	/// @note Global state uses atomic counters to safely aggregate row counts.
	bool ParallelOperator() const override {
		return true;
	}

	/// @brief Indicate that this is not a source operator.
	/// @return Always returns false.
	/// @note This operator passes through data from its child.
	bool IsSource() const override {
		return false;
	}

public:
	// ===== State Structures =====

	/// @struct SentinelGlobalState
	/// @brief Global state shared across all threads executing this operator.
	///
	/// Tracks aggregate statistics for the entire query execution:
	/// - Total number of rows processed across all threads
	/// - Start time for calculating execution duration
	struct SentinelGlobalState : public GlobalOperatorState {
		atomic<idx_t> rows_processed; ///< Atomic counter of total rows (thread-safe)
		std::chrono::time_point<std::chrono::high_resolution_clock> start_time; ///< Query start time

		/// @brief Initialize global state with zero rows and current timestamp.
		SentinelGlobalState() : rows_processed(0), start_time(std::chrono::high_resolution_clock::now()) {
		}
	};

	/// @struct SentinelSourceState
	/// @brief Source state (unused, kept for potential future extensions).
	struct SentinelSourceState : public GlobalSourceState {
		// Reserved for future use
	};

	/// @brief Create global operator state for tracking execution statistics.
	/// @param context The client context.
	/// @return Unique pointer to SentinelGlobalState.
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;

	/// @brief Create local operator state (default implementation).
	/// @param context The execution context.
	/// @return Unique pointer to basic OperatorState.
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
};

} // namespace duckdb
