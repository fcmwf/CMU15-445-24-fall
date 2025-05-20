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
#include "type/value_factory.h"
#include "concurrency/transaction_manager.h"



namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
// InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
//                                std::unique_ptr<AbstractExecutor> &&child_executor)
//     : AbstractExecutor(exec_ctx) {
//       plan_ = plan;
//       child_executor_ = std::move(child_executor);
// }
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

/** Initialize the insert */
void InsertExecutor::Init() {
  // child_executor_->Init();
  // try {
  //     GetExecutorContext()->GetLockManager()->LockTable(GetExecutorContext()->GetTransaction(),
  //                                                       LockManager::LockMode::INTENTION_EXCLUSIVE,
  //                                                       plan_->GetTableOid());
  // } catch (TransactionAbortException e) {
  //     throw ExecutionException("insert get table lock failed");
  // }
  child_executor_->Init();
  Column col("num", INTEGER);
  std::vector<Column> vec{col};
  schema_ = std::make_shared<const Schema>(vec);
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
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID in_rid;
  Tuple in_tuple;
  while (true) {
    bool status = child_executor_->Next(&in_tuple, &in_rid);
    if (!status) {
      if (is_end_) {
        return false;
      }
      is_end_ = true;
      std::vector<Value> values;
      values.push_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple(values, schema_.get());
      return true;
    }

    // check the tuple already in index?
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    BUSTUB_ENSURE(indexes.size() <= 1, "not only one index");
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      std::vector<RID> result;
      index_info->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
      BUSTUB_ENSURE(result.size() == 1, "index more than one tuple");
      if (!result.empty()) {
        *rid = result[0];
        auto tuple_info = table_info_->table_->GetTuple(*rid);
        // if the tuple in tableheap is deleted and the deleted ts smaller than current txn read ts, than still insert
        // success
        // todo is it needed to handle the self modification case?
        if (tuple_info.first.is_deleted_ && tuple_info.first.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
          // get the versioninfo and lock by set in_process to true
          auto version_link_opt = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
          if (!version_link_opt.has_value()) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert conflict");
          }
          auto version_link = version_link_opt.value();
          // update the undolog chain
          std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount());
          auto undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(
              UndoLog{true, modified_fields, Tuple(), tuple_info.first.ts_, version_link});
          exec_ctx_->GetTransactionManager()->UpdateUndoLink(*rid, undo_link);
          // update tableheap
          table_info_->table_->UpdateTupleInPlace({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
                                                  in_tuple, *rid);
          exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);
        }
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("insert conflict");
      }
    }

    auto rid_opt =
        table_info_->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, in_tuple);

    if (rid_opt.has_value()) {
      *rid = rid_opt.value();
    }
    // std::cout << "rid info:" << rid->ToString() << std::endl;
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, *rid);
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      bool res = index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      if (!res) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("insert conflict");
      }
    }
    nums_++;
  }
}
// auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if (is_end_) return false;
//   Tuple to_insert_tuple{};
//   RID emit_rid;
//   int32_t insert_count = 0;
//   auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
//   auto txn = GetExecutorContext()->GetTransaction();

//   while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {
//           auto r = table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionId(),false},
//                                                    to_insert_tuple,
//                                                    exec_ctx_->GetLockManager(), 
//                                                    exec_ctx_->GetTransaction(),
//                                                    plan_->GetTableOid());

//           auto index = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

//           std::for_each(index.begin(), index.end(),
//                         [&to_insert_tuple, &r, &table_info, &exec_ctx = exec_ctx_](const std::shared_ptr<IndexInfo> &i) {
//                             i->index_->InsertEntry(
//                                 to_insert_tuple.KeyFromTuple(table_info->schema_, i->key_schema_, i->index_->GetKeyAttrs()), 
//                                 *r,
//                                 exec_ctx->GetTransaction());
//                         });

//           if (r != std::nullopt) {
//               insert_count++;
//           }
//           BUSTUB_ENSURE(r != std::nullopt, "Sequential insertion cannot fail");
//       }
      
//       is_end_ = true;
//       std::vector<Value> values{};
//       values.reserve(GetOutputSchema().GetColumnCount());
//       values.emplace_back(TypeId::INTEGER, insert_count);
//       *tuple = Tuple{values, &GetOutputSchema()};
//       return true;

// }

}  // namespace bustub
