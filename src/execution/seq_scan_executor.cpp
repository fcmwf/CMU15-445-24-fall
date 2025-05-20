//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator()) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  while (!iter_.IsEnd()) {
    auto tuple_info = iter_.GetTuple();
    auto rid = iter_.GetRID();
    // std::cout << "rid: " << rid.ToString();
    // std::cout << "tuple: " << tuple_info.second.ToString(&GetOutputSchema()) << std::endl;
    auto res = CollectUndoLogs(rid, tuple_info.first, tuple_info.second, 
                              exec_ctx_->GetTransactionManager()->GetUndoLink(rid),
                              exec_ctx_->GetTransaction(),
                              exec_ctx_->GetTransactionManager());
    if(res.has_value()){
        auto tuple_opt = ReconstructTuple(&GetOutputSchema(), tuple_info.second, tuple_info.first, *res);
        if (tuple_opt.has_value()) {
          // std::cout << "retrive tuple: " << tuple_opt.value().ToString(&GetOutputSchema()) << std::endl;
          auto tuple = tuple_opt.value();
          if (plan_->filter_predicate_ != nullptr) {
            auto value = plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema());
            if (value.IsNull() || !value.GetAs<bool>()) {
              ++iter_;
              continue;
            }
          }
          vec_.emplace_back(tuple_opt.value(), rid);
        }
    }
    ++iter_;
    }
    cursor_ = 0;
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == vec_.size()) {
    return false;
  }
  *tuple = vec_[cursor_].first;
  *rid = vec_[cursor_].second;
  // std::cout << std::endl << "tuple: " << tuple->ToString(&plan_->OutputSchema()) << std::endl;
  cursor_++;
  return true;
}

}  // namespace bustub
