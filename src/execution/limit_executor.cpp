//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),  plan_(plan), child_executor_(std::move(child_executor)) {
    limit_ = plan_->GetLimit();
}

/** Initialize the limit */
void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

/**
 * Yield the next tuple from the limit.
 * @param[out] tuple The next tuple produced by the limit
 * @param[out] rid The next tuple RID produced by the limit
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(count_ >= limit_) {
        return false;
    }
    if(child_executor_->Next(tuple, rid)) {
        count_++;
        return true;
    }
    return false;
}
  
}  // namespace bustub
