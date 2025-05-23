//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"


namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> &&left_child,
                                    std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child(std::move(left_child)),
      right_child(std::move(right_child)) {
    if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
        // Note for 2023 Spring: You ONLY need to implement left join and inner join.
        throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
    left_child->Init();
    right_child->Init();
    Tuple t;
    RID r;

    while (right_child->Next(&t, &r)) {
        size_t cur_hash = 0;
        for (const auto &expr: plan_->RightJoinKeyExpressions()) {
            auto k = expr->Evaluate(&t, right_child->GetOutputSchema());
            if (!k.IsNull())
                cur_hash = HashUtil::CombineHashes(cur_hash, HashUtil::HashValue(&k));
        }
        ht_[cur_hash].push_back(t);
    }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    RID r;
    while (idx >= 0 || left_child->Next(&left_tuple, &r)) {
        size_t cur_hash = 0;
        for (const auto &expr: plan_->LeftJoinKeyExpressions()) {
            auto k = expr->Evaluate(&left_tuple, left_child->GetOutputSchema());
            if (!k.IsNull())
                cur_hash = HashUtil::CombineHashes(cur_hash, HashUtil::HashValue(&k));
        }
        if (ht_.count(cur_hash)) {
            for (size_t j = (idx >= 0 ? idx : 0); j < ht_[cur_hash].size(); j++) {

                bool founded = true;
                int ld = 0;
                for (const auto &expr: plan_->RightJoinKeyExpressions()) {
                    auto k = expr->Evaluate(&ht_[cur_hash][j], right_child->GetOutputSchema());
                    if (!k.IsNull()) {
                        auto res = k.CompareEquals(
                            plan_->LeftJoinKeyExpressions()[ld++]->Evaluate(&left_tuple,
                                                                            left_child->GetOutputSchema()));
                        if (res != CmpBool::CmpTrue) {
                            founded = false;
                            break;
                        }
                    }
                }
                if (founded) {
                    std::vector<Value> values;
                    values.reserve(
                        left_child->GetOutputSchema().GetColumnCount() +
                        right_child->GetOutputSchema().GetColumnCount());
                    for (uint32_t i = 0; i < left_child->GetOutputSchema().GetColumnCount(); ++i) {
                        values.push_back(left_tuple.GetValue(&left_child->GetOutputSchema(), i));
                    }
                    for (uint32_t i = 0; i < right_child->GetOutputSchema().GetColumnCount(); ++i) {
                        values.push_back(ht_[cur_hash][j].GetValue(&right_child->GetOutputSchema(), i));
                    }

                    *tuple = Tuple{values, &GetOutputSchema()};
                    idx = j + 1;
                    return true;
                }

            }

        }
        if (idx == -1 && JoinType::LEFT == plan_->GetJoinType()) {
            std::vector<Value> values;
            values.reserve(
                left_child->GetOutputSchema().GetColumnCount() + right_child->GetOutputSchema().GetColumnCount());
            for (uint32_t i = 0; i < left_child->GetOutputSchema().GetColumnCount(); ++i) {
                values.push_back(left_tuple.GetValue(&left_child->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_child->GetOutputSchema().GetColumnCount(); ++i) {
                values.push_back(
                    ValueFactory::GetNullValueByType(right_child->GetOutputSchema().GetColumn(i).GetType()));
            }

            *tuple = Tuple{values, &GetOutputSchema()};
            return true;
        }
        idx = -1;

    }
    return false;
}

}  // namespace bustub
