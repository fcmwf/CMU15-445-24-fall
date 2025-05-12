//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), 
      plan_(plan),
      tree_{dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())}, 
      iter_{tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {
  tableInfo = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  auto lock_mode = LockManager::LockMode::INTENTION_SHARED;
  if (GetExecutorContext()->IsDelete())
    lock_mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      exec_ctx_->GetLockManager()->LockTable(GetExecutorContext()->GetTransaction(),
                                            lock_mode, 
                                            tableInfo->oid_);
    }
  } catch (const TransactionAbortException &e) {
    LOG_ERROR("TransactionAbortException: %s", e.what());
    throw ExecutionException("lock failed");
  }
  is_end_ = false;
  retrive_flag_ = false;
  iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(is_end_){
      return false;
    }
    if(plan_->filter_predicate_ != nullptr){  // point lookup
       if(!retrive_flag_){
          auto index_col = dynamic_cast<const ColumnValueExpression*>(plan_->filter_predicate_.get())->GetColIdx();
          auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
          auto table_index = exec_ctx_->GetCatalog()->GetIndex(index_col);
          for(auto& val : plan_->pred_keys_){
            auto key = val->Evaluate(nullptr,Schema({}));
            auto key_schema = exec_ctx_->GetCatalog()->GetIndex(index_col)->key_schema_;
            Tuple tuple_key{{key}, &key_schema};
            std::vector<RID> rids;
            table_index->index_->ScanKey(tuple_key, &rids, exec_ctx_->GetTransaction());
            rids_.insert(rids_.end(), std::make_move_iterator(rids.begin()), std::make_move_iterator(rids.end()));
          }
          retrive_flag_ = true;
          if(rids_.size()==0){
            is_end_ = true;
            return false;
          }
          *rid = rids_.back();
          rids_.pop_back();
          *tuple = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid).second;
          return true;
       }else{
            while(!rids_.empty()){
              *rid = rids_.back();
              rids_.pop_back();
              *tuple = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid).second;
              return true;
            }
            is_end_ = true;
            return false;
       }
    }
    if (!iter_.IsEnd()) {      // range scan
      // std::cout << "iter_ is not end" << std::endl;
        const auto pair = *iter_;
        auto res = tableInfo->table_->GetTuple(pair.second);
        *rid = pair.second;
        *tuple = res.second;
        ++iter_;
        return true;
    }
    is_end_ = true;
    return false;
}

}  // namespace bustub

 

 