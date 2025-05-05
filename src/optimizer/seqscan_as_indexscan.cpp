//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */

auto ParserEqualExpression(const ComparisonExpression *expr, std::vector<AbstractExpressionRef> &predicate, AbstractExpressionRef &filter_predicate) -> bool{
  if(expr->comp_type_ != ComparisonType::Equal){
    return false;
  }
  const auto *left = expr->GetChildAt(0).get();
  const auto *right = expr->GetChildAt(1).get();

  const ColumnValueExpression *col_expr = dynamic_cast<const ColumnValueExpression *>(left);
  const ConstantValueExpression *const_expr = dynamic_cast<const ConstantValueExpression *>(right);
  if (!col_expr || !const_expr) {
    col_expr = dynamic_cast<const ColumnValueExpression *>(right);
    const_expr = dynamic_cast<const ConstantValueExpression *>(left);
    if (!col_expr || !const_expr) {
      return false;
    }
  }
  if(filter_predicate == nullptr){
    filter_predicate = std::make_shared<ColumnValueExpression>(*col_expr);
    predicate.push_back(std::make_shared<ConstantValueExpression>(*const_expr));
    return true;
  }
  if(dynamic_cast<const ColumnValueExpression *>(filter_predicate.get())->GetColIdx() != col_expr->GetColIdx()){
    return false;
  }
  predicate.push_back(std::make_shared<ConstantValueExpression>(*const_expr));
  return true;
}
auto ParserOrExpression(const LogicExpression *expr, std::vector<AbstractExpressionRef> &predicate, AbstractExpressionRef &filter_predicate) -> bool{
  auto left = expr->GetChildAt(0);
  auto right = expr->GetChildAt(1);
  // process left child
  if(dynamic_cast<const LogicExpression *>(left.get()) != nullptr) {
    if(dynamic_cast<const LogicExpression *>(left.get())->logic_type_ != LogicType::Or){
      return false;
    }
    if(!ParserOrExpression(dynamic_cast<const LogicExpression *>(left.get()), predicate, filter_predicate)){
      return false;
    }
  }else{
    if(!ParserEqualExpression(dynamic_cast<const ComparisonExpression *>(left.get()), predicate, filter_predicate)){
      return false;
    }
  }
  // process right child
  if(dynamic_cast<const LogicExpression *>(right.get()) != nullptr) {
    if(dynamic_cast<const LogicExpression *>(right.get())->logic_type_ != LogicType::Or){
      return false;
    }
    if(!ParserOrExpression(dynamic_cast<const LogicExpression *>(right.get()), predicate, filter_predicate)){
      return false;
    }
  }else{
    if(!ParserEqualExpression(dynamic_cast<const ComparisonExpression *>(right.get()), predicate, filter_predicate)){
      return false;
    }
  }
  return true;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 优化所有子节点
  std::vector<AbstractPlanNodeRef> new_children;
  for (const auto &child : plan->GetChildren()) {
    new_children.push_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(new_children);

  // 仅优化 SeqScan + 有 filter 的节点
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  if (seq_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  auto *expr = seq_plan.filter_predicate_.get();
  std::vector<AbstractExpressionRef> keys;
  AbstractExpressionRef filter_predicate = nullptr;
  // 判断是否是 OR (v1 = x OR v1 = y)
  auto logic_expr = dynamic_cast<const LogicExpression *>(expr);
  if(logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      return optimized_plan;
    }else{
      if(!ParserOrExpression(logic_expr, keys, filter_predicate)){
        return optimized_plan;
      }
      auto index_column_id = dynamic_cast<const ColumnValueExpression *>(filter_predicate.get())->GetColIdx();
      std::vector<std::shared_ptr<IndexInfo>> indexes = catalog_.GetTableIndexes(seq_plan.table_name_);
      bool col_is_index = std::find_if(indexes.begin(), indexes.end(),
                          [&index_column_id](const std::shared_ptr<IndexInfo> &index_info) {
                              return  index_info->index_oid_ == index_column_id;
                          }) != indexes.end();
      if(!col_is_index){
        return optimized_plan;
      }
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_plan.table_oid_, index_column_id,
                                                filter_predicate, keys);
    }
  }
  keys.clear();
  filter_predicate = nullptr;
  auto Com_expr = dynamic_cast<const ComparisonExpression*>(expr);
  if(Com_expr!=nullptr && ParserEqualExpression(Com_expr,keys,filter_predicate)){
    std::cout << "column and value" << std::endl;
    std::cout << dynamic_cast<const ColumnValueExpression *>(filter_predicate.get())->ToString() << std::endl;
    std::cout << dynamic_cast<const ConstantValueExpression *>(keys[0].get())->ToString() << std::endl;
    std::cout << optimized_plan->output_schema_->ToString() << std::endl;
    auto index_column_id = dynamic_cast<const ColumnValueExpression *>(filter_predicate.get())->GetColIdx();
    std::vector<std::shared_ptr<IndexInfo>> indexes = catalog_.GetTableIndexes(seq_plan.table_name_);
    bool col_is_index = std::find_if(indexes.begin(), indexes.end(),
                        [&index_column_id](const std::shared_ptr<IndexInfo> &index_info) {
                            return  index_info->index_oid_ == index_column_id;
                        }) != indexes.end();
    if(!col_is_index){
      return optimized_plan;
    }
    return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_plan.table_oid_, index_column_id,
                                              filter_predicate, keys);
  }
  return optimized_plan;
}

}  // namespace bustub
