//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
      tree_{dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
}

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple t;
    RID r;
    auto index_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
    auto expr = dynamic_cast<const ColumnValueExpression *>( plan_->KeyPredicate().get());
    while (child_executor_->Next(&t, &r)) {
        auto key = t.GetValue(&child_executor_->GetOutputSchema(), expr->GetColIdx());
        std::vector<RID> result;
        Tuple key_tuple = Tuple({key}, &exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_);
        tree_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
        for (auto &pair: result) {
            auto res = index_info->table_->GetTuple(pair);
            std::vector<Value> values;
            values.reserve(
                plan_->InnerTableSchema().GetColumnCount() + child_executor_->GetOutputSchema().GetColumnCount());
            for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
                values.push_back(t.GetValue(&child_executor_->GetOutputSchema(), i));
            }
            for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
                values.push_back(res.second.GetValue(&plan_->InnerTableSchema(), i));
            }
            *tuple = Tuple{values, &GetOutputSchema()};
            return true;
        }
        if (plan_->GetJoinType() == JoinType::LEFT) {
            std::vector<Value> values;
            values.reserve(
                plan_->InnerTableSchema().GetColumnCount() + child_executor_->GetOutputSchema().GetColumnCount());
            for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
                values.push_back(t.GetValue(&child_executor_->GetOutputSchema(), i));
            }
            for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
                values.push_back(
                    ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
            }
            *tuple = Tuple{values, &GetOutputSchema()};
            return true;
        }
    }
    return false;
}

}  // namespace bustub
