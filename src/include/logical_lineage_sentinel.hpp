//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: logical_lineage_sentinel.hpp
// Description: Logical operator that acts as a sentinel in the query plan.
//              Injects lineage tracking into the logical plan without modifying
//              query semantics. Converts to PhysicalLineageSentinel during
//              physical planning.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include <nlohmann/json.hpp>

namespace duckdb {

using json = nlohmann::json;

/// @class LogicalLineageSentinel
/// @brief Logical operator injected into the query plan to track lineage.
///
/// This operator is inserted at the root of the logical plan by the optimizer.
/// It passes through all data unchanged but carries metadata about the query
/// (run ID, job name, SQL text, output datasets) that will be used to emit
/// OpenLineage events during execution.
///
/// The operator is transparent to query execution - it doesn't modify the
/// result set or query semantics, only adds lineage tracking.
class LogicalLineageSentinel : public LogicalExtensionOperator {
public:
	/// @brief Default constructor for deserialization.
	LogicalLineageSentinel() {
	}

	/// @brief Create a sentinel operator with lineage metadata.
	/// @param child The child operator (original query plan root).
	/// @param run_id Unique identifier for this query execution (UUID).
	/// @param job_name Identifier for the job (SHA-256 hash of query).
	/// @param query The original SQL query text.
	/// @param outputs JSON array of output datasets from the query.
	explicit LogicalLineageSentinel(unique_ptr<LogicalOperator> child, string run_id, string job_name, string query,
	                                json outputs)
	    : run_id(std::move(run_id)), job_name(std::move(job_name)), query(std::move(query)),
	      outputs(std::move(outputs)) {
		children.push_back(std::move(child));
	}

	// ===== Lineage Metadata =====
	string run_id;   ///< Unique ID for this query run (UUID format)
	string job_name; ///< Job identifier (SHA-256 of query)
	string query;    ///< Original SQL query text
	json outputs;    ///< JSON array of output datasets

	/// @brief Convert this logical operator to a physical operator.
	/// @param context The client context.
	/// @param planner The physical plan generator.
	/// @return Reference to the created PhysicalLineageSentinel operator.
	/// @note Creates a PhysicalLineageSentinel and passes the child's physical plan.
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	/// @brief Serialize this operator (not implemented).
	/// @param serializer The serializer.
	/// @throws NotImplementedException
	void Serialize(Serializer &serializer) const override;

	/// @brief Deserialize this operator (not implemented).
	/// @param deserializer The deserializer.
	/// @return Unique pointer to the deserialized operator.
	/// @throws NotImplementedException
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	/// @brief Get column bindings from the child operator.
	/// @return Column bindings from the child (pass-through).
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	/// @brief Resolve output types by inheriting from the child operator.
	/// @note This ensures the sentinel is transparent and doesn't change output types.
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

} // namespace duckdb
