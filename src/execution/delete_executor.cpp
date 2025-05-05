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

#include "execution/executors/delete_executor.h"

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
  try {
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                           LockManager::LockMode::INTENTION_EXCLUSIVE,
                                           plan_->GetTableOid());
  } catch (TransactionAbortException e) {
    throw ExecutionException("delete get table lock failed");
  }
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
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());

  while(child_executor_->Next(&delete_tuple, &emit_rid)){
    table_info->table_->UpdateTupleMeta(TupleMeta{0,true}, emit_rid);
    auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    std::for_each(indexs.begin(), indexs.end(), 
                  [&table_info, &delete_tuple, &emit_rid, &exec_ctx = exec_ctx_](const std::shared_ptr<bustub::IndexInfo> &i) {
                    i->index_->DeleteEntry(delete_tuple.KeyFromTuple(table_info->schema_ , 
                                                                      i->key_schema_, 
                                                                      i->index_->GetKeyAttrs()), 
                                          emit_rid,
                                          exec_ctx->GetTransaction());
                  });
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
