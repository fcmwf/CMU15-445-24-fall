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

#include "execution/executors/update_executor.h"

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
  try{
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                           LockManager::LockMode::INTENTION_EXCLUSIVE,
                                           plan_->GetTableOid());
  } catch (TransactionAbortException e) {
      throw ExecutionException("delete get table lock failed");
  }
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
    bool update_flag = false;
    RID first_update_rid;
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
    while(child_executor_->Next(&origin_tuple, &emit_rid)){
          if(update_flag && first_update_rid == emit_rid){
            break;
          }
          std::vector<Value> values;
          values.reserve(plan_->target_expressions_.size());
          for (const auto &expr : plan_->target_expressions_) {
            values.push_back(expr->Evaluate(&origin_tuple, child_executor_->GetOutputSchema()));
          } 
          Tuple update_tuple{values, &child_executor_->GetOutputSchema()};
          table_info->table_->UpdateTupleMeta(TupleMeta{0,true}, origin_tuple.GetRid());
          auto r = table_info->table_->InsertTuple(TupleMeta{0,false},
                                                    update_tuple,
                                                    exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(),
                                                    plan_->GetTableOid());

          if(!r.has_value()){
            throw ExecutionException("update insert failed");
          }
          auto index = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
          for (auto &index_info : index) {
            index_info->index_->DeleteEntry(origin_tuple.KeyFromTuple(table_info->schema_, 
                                                                      index_info->key_schema_, 
                                                                      index_info->index_->GetKeyAttrs()),
                                            origin_tuple.GetRid(),
                                            exec_ctx_->GetTransaction());
            index_info->index_->InsertEntry(update_tuple.KeyFromTuple(table_info->schema_, 
                                                                      index_info->key_schema_, 
                                                                      index_info->index_->GetKeyAttrs()),
                                            *r,
                                            exec_ctx_->GetTransaction());
            }
          update_count++;
          if(!update_flag){
            first_update_rid = *r;
            update_flag = true;
          }
    }
    is_end_ = true;
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(TypeId::INTEGER, update_count);
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
}


}  // namespace bustub
