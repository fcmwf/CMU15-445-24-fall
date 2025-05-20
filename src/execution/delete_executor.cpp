//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  is_end_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(is_end_) {
    return false;
  }
  Tuple delete_tuple{};
  RID emit_rid;
  int32_t delete_count = 0;
  auto table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  while(child_executor_->Next(&delete_tuple, &emit_rid)){
      // std::cout << "delete rid:" << emit_rid.ToString();
      // std::cout << "delete tuple:" << delete_tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;
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
      auto prev_link = exec_ctx_->GetTransactionManager()->GetUndoLink(emit_rid);
      if (tuple_info.first.ts_ < TXN_START_ID) {
          // not self modification  
          // update undolog
          UndoLog undo_log;
          if(tuple_info.first.is_deleted_){
            undo_log = {true, {}, {},
                        tuple_info.first.ts_, exec_ctx_->GetTransactionManager()->GetUndoLink(emit_rid).value_or(UndoLink{})};
          }else{
            undo_log = {false ,std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(), true), delete_tuple,
                        tuple_info.first.ts_, exec_ctx_->GetTransactionManager()->GetUndoLink(emit_rid).value_or(UndoLink{})};
          }
          auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
          exec_ctx_->GetTransactionManager()->UpdateUndoLink(emit_rid, undo_link);
          // std::cout << "log: " << undo_log.is_deleted_ << " " << undo_log.ts_ << std::endl;
          // std::cout << "delete tuple: " << delete_tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;
          // std::cout << "delete rid: " << emit_rid.ToString();
      }else{
          // self modification
          if(prev_link.has_value() && prev_link.value().prev_txn_ == exec_ctx_->GetTransaction()->GetTransactionId()){
              auto prev_undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(prev_link.value());
              auto new_undo_log = GenerateUpdatedUndoLog(&child_executor_->GetOutputSchema(), &delete_tuple, nullptr, prev_undo_log);
              exec_ctx_->GetTransaction()->ModifyUndoLog(prev_link.value().prev_log_idx_, new_undo_log);
          }
      }
      // std::cout << "temp_ts: " << exec_ctx_->GetTransaction()->GetTransactionTempTs() << std::endl;
      table_info_->table_->UpdateTupleMeta({exec_ctx_->GetTransaction()->GetTransactionTempTs(), true},emit_rid);
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, emit_rid);

      // auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      // std::for_each(indexs.begin(), indexs.end(), 
      //               [&table_info_, &delete_tuple, &emit_rid, &exec_ctx = exec_ctx_](const std::shared_ptr<bustub::IndexInfo> &i) {
      //                 i->index_->DeleteEntry(delete_tuple.KeyFromTuple(table_info_->schema_ , 
      //                                                                   i->key_schema_, 
      //                                                                   i->index_->GetKeyAttrs()), 
      //                                       emit_rid,
      //                                       exec_ctx->GetTransaction());
      //               });
      delete_count++;
  }
  is_end_ = true;
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
