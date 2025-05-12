//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ProcessExp(std::vector<AbstractExpressionRef> &left_key_expressions,
                           std::vector<AbstractExpressionRef> &right_key_expressions,
                           AbstractExpression* exp) -> bool{
    if(auto logic_exp = dynamic_cast<LogicExpression*>(exp);logic_exp != nullptr){
      if(logic_exp->logic_type_ != LogicType::And){
        return false;
      }
      if(!ProcessExp(left_key_expressions, right_key_expressions, logic_exp->GetChildAt(0).get())){
        return false;
      }
      if(!ProcessExp(left_key_expressions, right_key_expressions, logic_exp->GetChildAt(1).get())){
        return false;
      }
      return true;
    }
    if(auto cmp_exp = dynamic_cast<ComparisonExpression*>(exp);cmp_exp != nullptr){
        if(cmp_exp->comp_type_!=ComparisonType::Equal){
            return false;
        }
        auto left_col = dynamic_cast<ColumnValueExpression*>(cmp_exp->GetChildAt(0).get());
        auto right_col = dynamic_cast<ColumnValueExpression*>(cmp_exp->GetChildAt(1).get());
        if(left_col == nullptr || right_col == nullptr){
          return false;
        }
        BUSTUB_ASSERT(left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1 ||
                      left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0,
                      "left and right column should be from different tables");
        if(left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1){
            left_key_expressions.push_back(cmp_exp->GetChildAt(0));
            right_key_expressions.push_back(cmp_exp->GetChildAt(1));
        }else{
            left_key_expressions.push_back(cmp_exp->GetChildAt(1));
            right_key_expressions.push_back(cmp_exp->GetChildAt(0));
        }
        return true;
    }
    return false;
}        

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    if(!ProcessExp(left_key_expressions, right_key_expressions, nlj_plan.Predicate().get())){
        return optimized_plan;
    }
    return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), std::move(left_key_expressions),
        std::move(right_key_expressions), nlj_plan.GetJoinType());
  }
  return optimized_plan; 
}

}  // namespace bustub
