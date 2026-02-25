//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: logical_lineage_sentinel.cpp
// Description: Implementation of the LogicalLineageSentinel operator.
//              Handles conversion from logical to physical plan representation.
//===----------------------------------------------------------------------===//

#include "logical_lineage_sentinel.hpp"
#include "physical_lineage_sentinel.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Physical Plan Creation
//===--------------------------------------------------------------------===//

PhysicalOperator &LogicalLineageSentinel::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// Step 1: Create the physical plan for the child operator
	// This recursively converts the entire logical plan tree to physical
	auto &child_physical = planner.CreatePlan(*children[0]);

	// Step 2: Create our PhysicalLineageSentinel operator
	// The planner's Make method uses the planner's allocator for memory management
	auto &physical_sentinel =
	    planner.Make<PhysicalLineageSentinel>(types, estimated_cardinality, run_id, job_name, query, outputs);

	// Step 3: Attach the child's physical plan as our child
	// This maintains the operator tree structure in the physical plan
	physical_sentinel.children.push_back(child_physical);

	return physical_sentinel;
}

//===--------------------------------------------------------------------===//
// Serialization (Not Implemented)
//===--------------------------------------------------------------------===//
// Note: Serialization is not required for this extension as query plans
// are not persisted. These methods throw NotImplementedException if called.

void LogicalLineageSentinel::Serialize(Serializer &serializer) const {
	// Serialization not needed for lineage tracking
	throw NotImplementedException("LogicalLineageSentinel serialization not implemented");
}

unique_ptr<LogicalOperator> LogicalLineageSentinel::Deserialize(Deserializer &deserializer) {
	// Deserialization not needed for lineage tracking
	throw NotImplementedException("LogicalLineageSentinel deserialization not implemented");
}

} // namespace duckdb
