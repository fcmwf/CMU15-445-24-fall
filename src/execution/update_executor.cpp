//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (is_end_) {
      return false;
    }
    Tuple origin_tuple{};
    RID emit_rid;
    int32_t update_count = 0;
    auto table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
    while(child_executor_->Next(&origin_tuple, &emit_rid)){
          // std::cout << "update rid: " << emit_rid ;
          auto tuple_info = table_info_->table_->GetTuple(emit_rid);
          auto tran_temp_ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
          //If a tuple is being modified by an uncommitted transaction, no other transactions are allowed to modify it.
          if (tuple_info.first.ts_ >= TXN_START_ID && tuple_info.first.ts_ != tran_temp_ts) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("w-w conflict in delete");
          }
          // the tuple is updated by anthor transaction before this transaction start
          if (tuple_info.first.ts_ < TXN_START_ID && tuple_info.first.ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("w-w conflict in delete");
          }

          //get update tuple
          std::vector<Value> values;
          values.reserve(plan_->target_expressions_.size());
          for (const auto &expr : plan_->target_expressions_) {
            values.push_back(expr->Evaluate(&origin_tuple, child_executor_->GetOutputSchema()));
          } 
          Tuple update_tuple{values, &child_executor_->GetOutputSchema()};
          // std::cout << "updated tuple: " << update_tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;
          auto prev_link = exec_ctx_->GetTransactionManager()->GetUndoLink(emit_rid);
          if (tuple_info.first.ts_ < TXN_START_ID) {
              // not self modification
              // update undolog
              auto undo_log = GenerateNewUndoLog(&child_executor_->GetOutputSchema(),&origin_tuple,&update_tuple,
                                                tuple_info.first.ts_, exec_ctx_->GetTransactionManager()->GetUndoLink(emit_rid).value_or(UndoLink{}));
              auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
              exec_ctx_->GetTransactionManager()->UpdateUndoLink(emit_rid, undo_link);
          }else{
              // self_modification
              if(prev_link.has_value() && prev_link.value().prev_txn_ == exec_ctx_->GetTransaction()->GetTransactionId()){
              // update undolog
                auto prev_undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(prev_link.value());
                auto new_undo_log = GenerateUpdatedUndoLog(&child_executor_->GetOutputSchema(), &origin_tuple, &update_tuple, prev_undo_log);
                exec_ctx_->GetTransaction()->ModifyUndoLog(prev_link.value().prev_log_idx_, new_undo_log);
              }
          }
          auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
          BUSTUB_ENSURE(indexes.size() <= 1, "not only one index");
          if(indexes.size()==1) {
              auto index = indexes[0];
              index->index_->DeleteEntry(
                  origin_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), 
                  emit_rid,
                  exec_ctx_->GetTransaction());
              index->index_->InsertEntry(
                  update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), 
                  emit_rid,
                  exec_ctx_->GetTransaction());
          }
          table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, 
                                                  update_tuple, emit_rid);
          exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, emit_rid);
          update_count++;
    }
    is_end_ = true;
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(TypeId::INTEGER, update_count);
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
}


}  // namespace bustub
