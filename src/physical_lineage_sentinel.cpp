//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: physical_lineage_sentinel.cpp
// Description: Implementation of the PhysicalLineageSentinel operator.
//              Tracks query execution and emits COMPLETE/FAIL events with
//              execution statistics (row counts, timing, error information).
//===----------------------------------------------------------------------===//

#include "physical_lineage_sentinel.hpp"
#include "lineage_client.hpp"
#include "lineage_utils.hpp"
#include "lineage_event_builder.hpp"
#include <nlohmann/json.hpp>
#include <iostream>
#include <exception>
#include <cstdlib>

namespace duckdb {

using json = nlohmann::json;

//===--------------------------------------------------------------------===//
// Constructor / Destructor
//===--------------------------------------------------------------------===//

PhysicalLineageSentinel::PhysicalLineageSentinel(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                 idx_t estimated_cardinality, string run_id, string job_name,
                                                 string query, json outputs)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      run_id(std::move(run_id)), job_name(std::move(job_name)), query(std::move(query)), outputs(std::move(outputs)) {
}

PhysicalLineageSentinel::~PhysicalLineageSentinel() {
	// Determine if the query succeeded or failed
	// std::uncaught_exception() returns true if we're in the middle of exception handling
	bool success = true;
	if (std::uncaught_exception()) {
		success = false;
	}

	// Retrieve the total row count from the global state
	idx_t rows = 0;
	// op_state is managed by the PhysicalOperator base class
	// The destructor of the derived class runs before the base class,
	// so op_state should still be valid if it was initialized
	if (op_state) {
		auto &state = op_state->Cast<SentinelGlobalState>();
		rows = state.rows_processed;
	}

	// Determine the event type based on success/failure
	// std::string eventType = success ? "COMPLETE" : "FAIL";

	try {
		// Generate the current timestamp for the event
		string eventTime = GetCurrentISOTime();

		// Build the COMPLETE or FAIL event using the factory builder
		auto builder = success ? LineageEventBuilder::CreateComplete() : LineageEventBuilder::CreateFail();

		builder.WithRunId(run_id)
		    .WithEventTime(eventTime)
		    .WithJob(LineageClient::Get().GetNamespace(), job_name)
		    .WithOutputs(outputs);

		if (!query.empty()) {
			builder.AddJobFacet_Sql(query);
		}

		// Add output statistics for successful queries
		if (success) {
			builder.AddOutputDatasetsFacet_Statistics(rows);
		}
		// ===== Add parent run facet if environment variables are set =====
		const char *parent_run_id_env = std::getenv("OPENLINEAGE_PARENT_RUN_ID");
		if (parent_run_id_env) {
			const char *parent_job_ns = std::getenv("OPENLINEAGE_PARENT_JOB_NAMESPACE");
			const char *parent_job_name = std::getenv("OPENLINEAGE_PARENT_JOB_NAME");

			std::string parent_ns = parent_job_ns ? parent_job_ns : "";
			std::string parent_name = parent_job_name ? parent_job_name : "";

			builder.AddRunFacet_Parent(parent_run_id_env, parent_ns, parent_name);
		}

		// ===== Add error message facet on failure =====
		if (!success) {
			builder.AddRunFacet_ErrorMessage("Query execution failed (details unavailable in destructor)", "SQL");
		}
		// Build and send the event
		json event = builder.Build();
		LineageClient::Get().SendEvent(event.dump());
	} catch (...) {
		// Suppress all exceptions in the destructor to prevent crashes
		// Destructors should never throw exceptions
		std::cerr << "OpenLineage Extension: Error sending COMPLETE/FAIL event" << '\n';
	}
}

//===--------------------------------------------------------------------===//
// State Management
//===--------------------------------------------------------------------===//

unique_ptr<GlobalOperatorState> PhysicalLineageSentinel::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<SentinelGlobalState>();
}

unique_ptr<OperatorState> PhysicalLineageSentinel::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<OperatorState>();
}

//===--------------------------------------------------------------------===//
// Execution
//===--------------------------------------------------------------------===//

OperatorResultType PhysicalLineageSentinel::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
	auto &sentinel_state = gstate.Cast<SentinelGlobalState>();

	// Pass through the data unchanged (reference, not copy)
	chunk.Reference(input);

	// Atomically increment the row counter
	// This is thread-safe as the counter is atomic
	sentinel_state.rows_processed += chunk.size();

	// Request more input from the child operator
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
