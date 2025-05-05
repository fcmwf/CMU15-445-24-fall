//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      plan_ = plan;
      child_executor_ = std::move(child_executor);
}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  try {
      GetExecutorContext()->GetLockManager()->LockTable(GetExecutorContext()->GetTransaction(),
                                                        LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                        plan_->GetTableOid());
  } catch (TransactionAbortException e) {
      throw ExecutionException("insert get table lock failed");
  }
}
/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) return false;
  Tuple to_insert_tuple{};
  RID emit_rid;
  int32_t insert_count = 0;
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto txn = GetExecutorContext()->GetTransaction();

  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
          auto r = table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionId(),false},
                                                   to_insert_tuple,
                                                   exec_ctx_->GetLockManager(), 
                                                   exec_ctx_->GetTransaction(),
                                                   plan_->GetTableOid());

          auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

          std::for_each(index.begin(), index.end(),
                        [&to_insert_tuple, &r, &table_info, &exec_ctx = exec_ctx_](const std::shared_ptr<IndexInfo> &i) {
                            i->index_->InsertEntry(
                                to_insert_tuple.KeyFromTuple(table_info->schema_, i->key_schema_, i->index_->GetKeyAttrs()), 
                                *r,
                                exec_ctx->GetTransaction());
                        });

          if (r != std::nullopt) {
              insert_count++;
          }
          BUSTUB_ENSURE(r != std::nullopt, "Sequential insertion cannot fail");
      }
      
      is_end_ = true;
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumnCount());
      values.emplace_back(TypeId::INTEGER, insert_count);
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;

}

}  // namespace bustub
