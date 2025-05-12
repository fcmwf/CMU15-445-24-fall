//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer.h
//
// Identification: src/include/optimizer/optimizer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  auto OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  std::string PlanTypeToString(PlanType type) {
  switch (type) {
    case PlanType::SeqScan: return "SeqScan";
    case PlanType::IndexScan: return "IndexScan";
    case PlanType::Insert: return "Insert";
    case PlanType::Update: return "Update";
    case PlanType::Delete: return "Delete";
    case PlanType::Aggregation: return "Aggregation";
    case PlanType::Limit: return "Limit";
    case PlanType::NestedLoopJoin: return "NestedLoopJoin";
    case PlanType::NestedIndexJoin: return "NestedIndexJoin";
    case PlanType::HashJoin: return "HashJoin";
    case PlanType::Filter: return "Filter";
    case PlanType::Values: return "Values";
    case PlanType::Projection: return "Projection";
    case PlanType::Sort: return "Sort";
    case PlanType::TopN: return "TopN";
    case PlanType::TopNPerGroup: return "TopNPerGroup";
    case PlanType::MockScan: return "MockScan";
    case PlanType::InitCheck: return "InitCheck";
    case PlanType::Window: return "Window";
    default: return "Unknown PlanType";
  }
}
  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
