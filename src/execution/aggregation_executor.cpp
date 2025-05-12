//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
      aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
      child_executor_->Init();
      aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(!insert_done_){
      Tuple t;
      RID r;
      while (child_executor_->Next(&t, &r)) {
          aht_->InsertCombine(MakeAggregateKey(&t), MakeAggregateValue(&t));
      }
      if (aht_->Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
          aht_->Initial();
      }
      aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
      insert_done_ = true;
    }
    if (*aht_iterator_ == aht_->End()) {
        return false;
    }
    std::vector<Value> values;
    values.insert(values.end(), aht_iterator_->Key().group_bys_.begin(), aht_iterator_->Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_->Val().aggregates_.begin(), aht_iterator_->Val().aggregates_.end());
    *tuple = Tuple{values, &GetOutputSchema()};
    // std::cout << "tuple: " << tuple->ToString(&GetOutputSchema()) << std::endl;
    ++(*aht_iterator_);
    return true;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
