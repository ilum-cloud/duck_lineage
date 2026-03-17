//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: column_lineage_extractor.cpp
// Description: Implementation of column-level lineage extraction from logical
//              plans. Traverses operators bottom-up, mapping each ColumnBinding
//              to its source table columns.
//===----------------------------------------------------------------------===//

#include "column_lineage_extractor.hpp"
#include "lineage_client.hpp"
#include "lineage_utils.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/tableref/bound_pivotref.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include <unordered_set>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constructor
//===--------------------------------------------------------------------===//

ColumnLineageExtractor::ColumnLineageExtractor(ClientContext &context) : context(context) {
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

uint64_t ColumnLineageExtractor::PackBinding(const ColumnBinding &binding) {
	return (static_cast<uint64_t>(binding.table_index) << 32) | static_cast<uint64_t>(binding.column_index);
}

void ColumnLineageExtractor::DeduplicateSources(BindingLineage &lineage) {
	unordered_set<SourceColumn, SourceColumnHash> seen;
	vector<SourceColumn> unique_sources;
	for (auto &src : lineage.sources) {
		if (seen.insert(src).second) {
			unique_sources.push_back(src);
		}
	}
	lineage.sources = std::move(unique_sources);
}

//===--------------------------------------------------------------------===//
// Public API
//===--------------------------------------------------------------------===//

void ColumnLineageExtractor::BuildLineageMap(LogicalOperator &op) {
	lineage_map.clear();
	TraversePlan(op);
}

/// @brief Walk down through "sink" operators (INSERT, CREATE, COPY_TO, etc.)
/// to find the operator that actually produces the data being written.
static LogicalOperator *FindDataProducingOperator(LogicalOperator &op) {
	LogicalOperator *current = &op;
	while (current) {
		switch (current->type) {
		case LogicalOperatorType::LOGICAL_INSERT:
		case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		case LogicalOperatorType::LOGICAL_DELETE:
		case LogicalOperatorType::LOGICAL_UPDATE:
			if (!current->children.empty()) {
				current = current->children[0].get();
				continue;
			}
			return current;
		default:
			return current;
		}
	}
	return &op;
}

/// @brief Extract column names from a data-producing operator.
static vector<string> GetOperatorColumnNames(LogicalOperator &op) {
	vector<string> names;

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		for (auto &expr : proj.expressions) {
			names.push_back(expr->GetName());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto &column_ids = get.GetColumnIds();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].IsRowIdColumn() || column_ids[i].IsVirtualColumn()) {
				names.push_back("__rowid");
				continue;
			}
			auto col_idx = column_ids[i].GetPrimaryIndex();
			if (col_idx < get.names.size()) {
				names.push_back(get.names[col_idx]);
			} else {
				names.push_back("col" + to_string(i));
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op.Cast<LogicalAggregate>();
		for (auto &group : agg.groups) {
			names.push_back(group->GetName());
		}
		for (auto &expr : agg.expressions) {
			names.push_back(expr->GetName());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PIVOT: {
		auto &pivot = op.Cast<LogicalPivot>();
		// Group columns from child
		if (!op.children.empty()) {
			auto child_names = GetOperatorColumnNames(*op.children[0]);
			for (idx_t i = 0; i < pivot.bound_pivot.group_count && i < child_names.size(); i++) {
				names.push_back(child_names[i]);
			}
		}
		// Pivoted columns: value_aggname pattern
		for (auto &val : pivot.bound_pivot.pivot_values) {
			for (auto &agg : pivot.bound_pivot.aggregates) {
				names.push_back(val + "_" + agg->GetName());
			}
		}
		// Pad if needed
		while (names.size() < pivot.bound_pivot.types.size()) {
			names.push_back("col" + to_string(names.size()));
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		if (!op.children.empty()) {
			names = GetOperatorColumnNames(*op.children[0]);
		}
		for (auto &expr : op.expressions) {
			names.push_back(expr->GetName());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		// For set operations, use the left child's column names
		if (!op.children.empty()) {
			names = GetOperatorColumnNames(*op.children[0]);
		}
		if (names.size() < op.types.size()) {
			for (idx_t i = names.size(); i < op.types.size(); i++) {
				names.push_back("col" + to_string(i));
			}
		}
		break;
	}
	default: {
		// Fallback: for passthrough operators (filter, order, limit, distinct, etc.),
		// delegate to the child operator to get proper column names.
		// Use GetColumnBindings().size() for output width since op.types may not
		// be resolved yet (we run in the optimizer hook before full type resolution).
		auto output_width = op.GetColumnBindings().size();
		if (output_width == 0) {
			output_width = op.types.size();
		}

		if (!op.children.empty()) {
			// For LOGICAL_MATERIALIZED_CTE, children[1] is the main query (output),
			// children[0] is the CTE definition. Use children[1] for column names.
			idx_t name_child = 0;
			if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op.children.size() > 1) {
				name_child = 1;
			}
			names = GetOperatorColumnNames(*op.children[name_child]);
			// Trim or pad to match this operator's output width
			while (names.size() > output_width && output_width > 0) {
				names.pop_back();
			}
			while (names.size() < output_width) {
				names.push_back("col" + to_string(names.size()));
			}
		} else {
			for (idx_t i = 0; i < output_width; i++) {
				names.push_back("col" + to_string(i));
			}
		}
		break;
	}
	}

	return names;
}

vector<ColumnLineageField> ColumnLineageExtractor::ExtractOutputColumnLineage(LogicalOperator &op,
                                                                              const string &dataset_namespace,
                                                                              const string &dataset_name) {
	vector<ColumnLineageField> result;

	// Find the operator that produces the data being written
	LogicalOperator *data_op = FindDataProducingOperator(op);
	if (!data_op) {
		return result;
	}

	auto bindings = data_op->GetColumnBindings();
	if (bindings.empty()) {
		return result;
	}

	// Get column names from the data-producing operator
	auto col_names = GetOperatorColumnNames(*data_op);

	for (idx_t i = 0; i < bindings.size(); i++) {
		auto key = PackBinding(bindings[i]);
		auto it = lineage_map.find(key);
		if (it == lineage_map.end() || it->second.sources.empty()) {
			continue;
		}

		ColumnLineageField field;
		field.output_column_name = i < col_names.size() ? col_names[i] : "col" + to_string(i);
		field.input_fields = it->second.sources;
		field.transformation_type = it->second.is_direct ? "DIRECT" : "INDIRECT";
		result.push_back(std::move(field));
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Plan Traversal
//===--------------------------------------------------------------------===//

void ColumnLineageExtractor::TraversePlan(LogicalOperator &op) {
	// Bottom-up: recurse into children first
	for (auto &child : op.children) {
		TraversePlan(*child);
	}

	// Now handle this operator
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		HandleGet(op);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		HandleProjection(op);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		HandleFilter(op);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		HandleJoin(op);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		HandleAggregate(op);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		HandleSetOperation(op);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		HandleWindow(op);
		break;
	case LogicalOperatorType::LOGICAL_PIVOT:
		HandlePivot(op);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		HandleUnnest(op);
		break;
	default:
		HandleDefaultPassthrough(op);
		break;
	}
}

//===--------------------------------------------------------------------===//
// Operator Handlers
//===--------------------------------------------------------------------===//

void ColumnLineageExtractor::HandleGet(LogicalOperator &op) {
	auto &get = op.Cast<LogicalGet>();

	// Skip views - their underlying tables will be in the plan
	if (get.GetTable() && get.GetTable()->type == CatalogType::VIEW_ENTRY) {
		return;
	}

	string dataset_ns;
	string dataset_name;
	vector<string> column_names;

	if (get.GetTable()) {
		// Table scan: resolve namespace and fully qualified name from catalog
		try {
			string data_path = "";
			try {
				auto &catalog = get.GetTable()->catalog;
				if (!catalog.IsSystemCatalog() && !catalog.IsTemporaryCatalog()) {
					auto &attached_db = catalog.GetAttached();
					if (attached_db.tags.contains("data_path")) {
						data_path = attached_db.tags["data_path"];
						if (!data_path.empty() && data_path.back() == '/') {
							data_path.pop_back();
						}
					}
				}
			} catch (...) {
			}

			if (!data_path.empty()) {
				dataset_ns = data_path;
			} else {
				dataset_ns = LineageClient::Get().GetNamespace();
			}
		} catch (...) {
			dataset_ns = LineageClient::Get().GetNamespace();
		}

		try {
			dataset_name = GetFullyQualifiedTableName(get.GetTable()->catalog, get.GetTable()->ParentSchema(),
			                                          get.GetTable()->name);
		} catch (...) {
			dataset_name = get.GetTable()->name;
		}

		// Get the table columns for name lookup
		auto &table_columns = get.GetTable()->GetColumns();
		for (auto &col : table_columns.Logical()) {
			column_names.push_back(col.Name());
		}
	} else {
		// File scan (read_csv, read_parquet, etc.) or table function
		dataset_ns = "file";

		// Try to extract file path from MultiFileBindData (same pattern as optimizer)
		if (get.bind_data) {
			try {
				auto *multi_file_data = dynamic_cast<MultiFileBindData *>(get.bind_data.get());
				if (multi_file_data && multi_file_data->file_list) {
					auto file_paths = multi_file_data->file_list->GetPaths();
					if (!file_paths.empty()) {
						dataset_name = file_paths[0].path;
					}
				}
			} catch (...) {
			}
		}
		if (dataset_name.empty()) {
			dataset_name = get.function.name;
		}

		// Use get.names for column name lookup
		column_names = get.names;
	}

	// Map each column binding to its source
	auto &column_ids = get.GetColumnIds();
	auto bindings = get.GetColumnBindings();

	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i >= column_ids.size()) {
			break;
		}

		// Skip row ID and virtual columns
		if (column_ids[i].IsRowIdColumn() || column_ids[i].IsVirtualColumn()) {
			continue;
		}

		auto col_idx = column_ids[i].GetPrimaryIndex();

		// Get the column name
		string col_name;
		if (col_idx < column_names.size()) {
			col_name = column_names[col_idx];
		} else {
			col_name = "col" + to_string(col_idx);
		}

		SourceColumn src;
		src.dataset_namespace = dataset_ns;
		src.dataset_name = dataset_name;
		src.column_name = col_name;

		BindingLineage lineage;
		lineage.sources.push_back(src);
		lineage.is_direct = true;

		lineage_map[PackBinding(bindings[i])] = std::move(lineage);
	}
}

void ColumnLineageExtractor::HandleProjection(LogicalOperator &op) {
	auto &proj = op.Cast<LogicalProjection>();
	auto bindings = proj.GetColumnBindings();

	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		if (i >= bindings.size()) {
			break;
		}

		auto resolved = ResolveExpression(*proj.expressions[i]);
		DeduplicateSources(resolved);

		if (!resolved.sources.empty()) {
			lineage_map[PackBinding(bindings[i])] = std::move(resolved);
		}
	}
}

void ColumnLineageExtractor::HandleFilter(LogicalOperator &op) {
	auto &filter = op.Cast<LogicalFilter>();

	// Filter passes through child bindings, optionally remapped by projection_map
	if (!op.children.empty()) {
		auto child_bindings = op.children[0]->GetColumnBindings();
		auto my_bindings = filter.GetColumnBindings();

		for (idx_t i = 0; i < my_bindings.size(); i++) {
			// Determine which child binding this maps to
			idx_t child_idx = i;
			if (!filter.projection_map.empty() && i < filter.projection_map.size()) {
				child_idx = filter.projection_map[i];
			}

			if (child_idx < child_bindings.size()) {
				auto child_key = PackBinding(child_bindings[child_idx]);
				auto it = lineage_map.find(child_key);
				if (it != lineage_map.end()) {
					lineage_map[PackBinding(my_bindings[i])] = it->second;
				}
			}
		}
	}
}

void ColumnLineageExtractor::HandleJoin(LogicalOperator &op) {
	// Joins merge bindings from left and right children
	// With projection maps, only a subset may be exposed
	if (op.children.size() < 2) {
		HandleDefaultPassthrough(op);
		return;
	}

	auto left_bindings = op.children[0]->GetColumnBindings();
	auto right_bindings = op.children[1]->GetColumnBindings();

	// Check for projection maps on join operators
	vector<idx_t> left_projection_map;
	vector<idx_t> right_projection_map;

	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &join = op.Cast<LogicalJoin>();
		left_projection_map = join.left_projection_map;
		right_projection_map = join.right_projection_map;
	}

	auto my_bindings = op.GetColumnBindings();

	// Build the combined binding list: left (possibly projected) + right (possibly projected)
	vector<ColumnBinding> source_bindings;
	if (left_projection_map.empty()) {
		for (auto &b : left_bindings) {
			source_bindings.push_back(b);
		}
	} else {
		for (auto idx : left_projection_map) {
			if (idx < left_bindings.size()) {
				source_bindings.push_back(left_bindings[idx]);
			}
		}
	}

	if (right_projection_map.empty()) {
		for (auto &b : right_bindings) {
			source_bindings.push_back(b);
		}
	} else {
		for (auto idx : right_projection_map) {
			if (idx < right_bindings.size()) {
				source_bindings.push_back(right_bindings[idx]);
			}
		}
	}

	// Map my bindings to source bindings
	for (idx_t i = 0; i < my_bindings.size() && i < source_bindings.size(); i++) {
		auto source_key = PackBinding(source_bindings[i]);
		auto it = lineage_map.find(source_key);
		if (it != lineage_map.end()) {
			lineage_map[PackBinding(my_bindings[i])] = it->second;
		}
	}
}

void ColumnLineageExtractor::HandleAggregate(LogicalOperator &op) {
	auto &agg = op.Cast<LogicalAggregate>();
	auto bindings = agg.GetColumnBindings();

	// Groups come first, then aggregates
	idx_t group_count = agg.groups.size();

	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i < group_count) {
			// Group expressions - resolve to find source columns
			auto resolved = ResolveExpression(*agg.groups[i]);
			DeduplicateSources(resolved);
			if (!resolved.sources.empty()) {
				lineage_map[PackBinding(bindings[i])] = std::move(resolved);
			}
		} else {
			// Aggregate expressions (SUM, COUNT, etc.)
			idx_t agg_idx = i - group_count;
			if (agg_idx < agg.expressions.size()) {
				auto resolved = ResolveExpression(*agg.expressions[agg_idx]);
				DeduplicateSources(resolved);
				if (!resolved.sources.empty()) {
					lineage_map[PackBinding(bindings[i])] = std::move(resolved);
				}
			}
		}
	}
}

void ColumnLineageExtractor::HandleSetOperation(LogicalOperator &op) {
	auto my_bindings = op.GetColumnBindings();

	if (op.children.size() < 2) {
		return;
	}

	auto left_bindings = op.children[0]->GetColumnBindings();
	auto right_bindings = op.children[1]->GetColumnBindings();

	// For UNION/INTERSECT/EXCEPT, merge corresponding columns from both children
	for (idx_t i = 0; i < my_bindings.size(); i++) {
		BindingLineage merged;

		// Add sources from left child
		if (i < left_bindings.size()) {
			auto left_key = PackBinding(left_bindings[i]);
			auto it = lineage_map.find(left_key);
			if (it != lineage_map.end()) {
				for (auto &src : it->second.sources) {
					merged.sources.push_back(src);
				}
				merged.is_direct = merged.is_direct && it->second.is_direct;
			}
		}

		// Add sources from right child
		if (i < right_bindings.size()) {
			auto right_key = PackBinding(right_bindings[i]);
			auto it = lineage_map.find(right_key);
			if (it != lineage_map.end()) {
				for (auto &src : it->second.sources) {
					merged.sources.push_back(src);
				}
				merged.is_direct = merged.is_direct && it->second.is_direct;
			}
		}

		DeduplicateSources(merged);
		if (!merged.sources.empty()) {
			lineage_map[PackBinding(my_bindings[i])] = std::move(merged);
		}
	}
}

void ColumnLineageExtractor::HandleWindow(LogicalOperator &op) {
	auto &window = op.Cast<LogicalWindow>();

	// Window passes through child bindings, then appends window expressions
	if (!op.children.empty()) {
		auto child_bindings = op.children[0]->GetColumnBindings();
		auto my_bindings = op.GetColumnBindings();

		// First N bindings are pass-through from child
		for (idx_t i = 0; i < child_bindings.size() && i < my_bindings.size(); i++) {
			auto child_key = PackBinding(child_bindings[i]);
			auto it = lineage_map.find(child_key);
			if (it != lineage_map.end()) {
				lineage_map[PackBinding(my_bindings[i])] = it->second;
			}
		}

		// Remaining bindings are window expressions
		for (idx_t i = child_bindings.size(); i < my_bindings.size(); i++) {
			idx_t expr_idx = i - child_bindings.size();
			if (expr_idx < window.expressions.size()) {
				auto resolved = ResolveExpression(*window.expressions[expr_idx]);
				DeduplicateSources(resolved);
				if (!resolved.sources.empty()) {
					lineage_map[PackBinding(my_bindings[i])] = std::move(resolved);
				}
			}
		}
	}
}

void ColumnLineageExtractor::HandlePivot(LogicalOperator &op) {
	auto &pivot = op.Cast<LogicalPivot>();
	auto my_bindings = pivot.GetColumnBindings();

	if (op.children.empty()) {
		return;
	}
	auto child_bindings = op.children[0]->GetColumnBindings();

	idx_t group_count = pivot.bound_pivot.group_count;

	for (idx_t i = 0; i < my_bindings.size(); i++) {
		if (i < group_count) {
			// Group columns: pass through from child (1:1 positional)
			if (i < child_bindings.size()) {
				auto child_key = PackBinding(child_bindings[i]);
				auto it = lineage_map.find(child_key);
				if (it != lineage_map.end()) {
					lineage_map[PackBinding(my_bindings[i])] = it->second;
				}
			}
		} else {
			// Pivoted aggregate columns: resolve aggregate expression
			idx_t pivot_col = i - group_count;
			idx_t agg_count = pivot.bound_pivot.aggregates.size();
			if (agg_count > 0) {
				idx_t agg_idx = pivot_col % agg_count;
				if (agg_idx < pivot.bound_pivot.aggregates.size()) {
					auto resolved = ResolveExpression(*pivot.bound_pivot.aggregates[agg_idx]);
					DeduplicateSources(resolved);
					resolved.is_direct = false; // aggregation is always indirect
					if (!resolved.sources.empty()) {
						lineage_map[PackBinding(my_bindings[i])] = std::move(resolved);
					}
				}
			}
		}
	}
}

void ColumnLineageExtractor::HandleUnnest(LogicalOperator &op) {
	auto &unnest = op.Cast<LogicalUnnest>();
	(void)unnest; // Used for Cast validation

	if (op.children.empty()) {
		return;
	}
	auto child_bindings = op.children[0]->GetColumnBindings();
	auto my_bindings = op.GetColumnBindings();

	// First N bindings: pass-through from child
	for (idx_t i = 0; i < child_bindings.size() && i < my_bindings.size(); i++) {
		auto child_key = PackBinding(child_bindings[i]);
		auto it = lineage_map.find(child_key);
		if (it != lineage_map.end()) {
			lineage_map[PackBinding(my_bindings[i])] = it->second;
		}
	}

	// Remaining bindings: unnest expressions
	for (idx_t i = child_bindings.size(); i < my_bindings.size(); i++) {
		idx_t expr_idx = i - child_bindings.size();
		if (expr_idx < op.expressions.size()) {
			auto resolved = ResolveExpression(*op.expressions[expr_idx]);
			DeduplicateSources(resolved);
			resolved.is_direct = false; // unnesting transforms the structure
			if (!resolved.sources.empty()) {
				lineage_map[PackBinding(my_bindings[i])] = std::move(resolved);
			}
		}
	}
}

void ColumnLineageExtractor::HandleDefaultPassthrough(LogicalOperator &op) {
	// Default: pass through child bindings unchanged
	if (op.children.empty()) {
		return;
	}

	auto my_bindings = op.GetColumnBindings();
	auto child_bindings = op.children[0]->GetColumnBindings();

	for (idx_t i = 0; i < my_bindings.size() && i < child_bindings.size(); i++) {
		auto child_key = PackBinding(child_bindings[i]);
		auto it = lineage_map.find(child_key);
		if (it != lineage_map.end()) {
			lineage_map[PackBinding(my_bindings[i])] = it->second;
		}
	}
}

//===--------------------------------------------------------------------===//
// Expression Resolution
//===--------------------------------------------------------------------===//

BindingLineage ColumnLineageExtractor::ResolveExpression(Expression &expr) {
	BindingLineage result;

	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		auto key = PackBinding(col_ref.binding);
		auto it = lineage_map.find(key);
		if (it != lineage_map.end()) {
			result = it->second;
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		for (auto &child : func.children) {
			auto child_lineage = ResolveExpression(*child);
			for (auto &src : child_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && child_lineage.is_direct;
		}
		break;
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &agg = expr.Cast<BoundAggregateExpression>();
		for (auto &child : agg.children) {
			auto child_lineage = ResolveExpression(*child);
			for (auto &src : child_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && child_lineage.is_direct;
		}
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		result = ResolveExpression(*cast.child);
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &oper = expr.Cast<BoundOperatorExpression>();
		for (auto &child : oper.children) {
			auto child_lineage = ResolveExpression(*child);
			for (auto &src : child_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && child_lineage.is_direct;
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &check : case_expr.case_checks) {
			auto when_lineage = ResolveExpression(*check.when_expr);
			for (auto &src : when_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && when_lineage.is_direct;
			auto then_lineage = ResolveExpression(*check.then_expr);
			for (auto &src : then_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && then_lineage.is_direct;
		}
		if (case_expr.else_expr) {
			auto else_lineage = ResolveExpression(*case_expr.else_expr);
			for (auto &src : else_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && else_lineage.is_direct;
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = expr.Cast<BoundWindowExpression>();
		for (auto &child : window_expr.children) {
			auto child_lineage = ResolveExpression(*child);
			for (auto &src : child_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && child_lineage.is_direct;
		}
		for (auto &partition : window_expr.partitions) {
			auto part_lineage = ResolveExpression(*partition);
			for (auto &src : part_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && part_lineage.is_direct;
		}
		for (auto &order : window_expr.orders) {
			auto order_lineage = ResolveExpression(*order.expression);
			for (auto &src : order_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && order_lineage.is_direct;
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = expr.Cast<BoundUnnestExpression>();
		result = ResolveExpression(*unnest_expr.child);
		result.is_direct = false;
		break;
	}
	case ExpressionClass::BOUND_CONSTANT: {
		// Constants have no column lineage
		break;
	}
	default: {
		// Fallback: use ExpressionIterator to find column refs in unknown expression types
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			auto child_lineage = ResolveExpression(child);
			for (auto &src : child_lineage.sources) {
				result.sources.push_back(src);
			}
			result.is_direct = result.is_direct && child_lineage.is_direct;
		});
		break;
	}
	}

	return result;
}

} // namespace duckdb
