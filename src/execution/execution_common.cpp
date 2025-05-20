//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)), schema_({}) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const Tuple &tuple_a, const Tuple &tuple_b) const -> bool {   
    // return true if type==asc && a < b. 
    //             Or type==desc && a > b.
    auto entry_a = GenerateSortKey(tuple_a);
    auto entry_b = GenerateSortKey(tuple_b);
    BUSTUB_ASSERT(entry_a.size() == entry_b.size() && entry_a.size() == order_bys_.size(),
                  "The size of sort key and order by should be the same.");
    for (size_t i = 0; i < order_bys_.size(); i++) {
        BUSTUB_ASSERT(entry_a[i].GetTypeId() == entry_b[i].GetTypeId(),
                      "The type of sort key should be the same.");
        if(entry_a[i].CompareEquals(entry_b[i]) == CmpBool::CmpTrue) {
            continue;
        }
        CmpBool cmp;
        switch(order_bys_[i].first){
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
                cmp = entry_a[i].CompareLessThan(entry_b[i]);
                BUSTUB_ASSERT(cmp != CmpBool::CmpNull, "The comparison should not be null.");
                return cmp == CmpBool::CmpTrue;
                break;
            case OrderByType::DESC:
                cmp = entry_a[i].CompareGreaterThan(entry_b[i]);
                BUSTUB_ASSERT(cmp != CmpBool::CmpNull, "The comparison should not be null.");
                return cmp == CmpBool::CmpTrue;
                break;
            default:
                throw  Exception("Invalid order by type.");
        }
    }
    return true;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto TupleComparator::GenerateSortKey(const Tuple &tuple) const -> SortKey {
  SortKey sort_key{};
  for(auto& col: order_bys_) {
    auto value = col.second->Evaluate(&tuple, schema_);
    sort_key.push_back(value);
  }
  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */
auto ConstructValue(const Schema *schema, const Tuple &base_tuple) -> std::vector<Value> {
  std::vector<Value> vec;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    vec.push_back(base_tuple.GetValue(schema, i));
  }
  return vec;
}
/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty() && base_meta.is_deleted_) {
    return std::nullopt;
  }
  std::vector<Value> vec = ConstructValue(schema, base_tuple);
  bool flag = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    flag = log.is_deleted_;
    std::vector<Column> cols;
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        cols.push_back(schema->GetColumn(i));
      }
    }
    auto log_schema = Schema(cols);
    size_t col_idx = 0;
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
      }
    }
  }
  if (flag) {
    return std::nullopt;
  }
  return std::make_optional<Tuple>(vec, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
    
    auto read_ts = txn->GetReadTs();
    auto commit_ts = base_meta.ts_;
    // std::cout << "read_ts: " << read_ts << std::endl;
    // std::cout << "commit_ts: " << commit_ts << std::endl;
    // base tuple is the most recent data relative to the read timestamp. 
    if (commit_ts <= read_ts ) {
        // std::cout << "case 1" << std::endl;
        return std::vector<UndoLog>{};
    }

    // base tuple is commited by self
    if(commit_ts >= TXN_START_ID && txn->GetTransactionTempTs() == commit_ts){
        // std::cout << "case 2" << std::endl;
        return std::vector<UndoLog>{};
    }
    // std::cout << "case 3" << std::endl;
    std::vector<UndoLog> undo_logs{};
    // base tuple is commited by other txn
    if(!undo_link.has_value()){  
        return std::nullopt;
    }
    UndoLink link = undo_link.value();
    if(!link.IsValid()){
        return std::nullopt;
    }
    auto log = txn_mgr->GetUndoLog(link);
    // std::cout << "log.ts: " << log.ts_ << std::endl;
    while(log.ts_ >= read_ts){
        undo_logs.push_back(log);
        link = log.prev_version_;
        if(!link.IsValid()){
          break;
        }
        log = txn_mgr->GetUndoLog(link);
        // std::cout << "log.ts: " << log.ts_ << std::endl;
    }
    if(log.ts_ > read_ts){
      return std::nullopt;
    }
    if(log.ts_ == read_ts){
      return {undo_logs};
    }
    if((undo_logs.size()>0 && undo_logs.back().ts_ != read_ts) || (undo_logs.size()==0)){
      undo_logs.push_back(log);
    }
    // std::cout << undo_logs.begin()->ts_ << std::endl;
    return {undo_logs};
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
    if(base_tuple==nullptr){
      return UndoLog{true,{},{},ts,prev_version};
    }
    if(target_tuple==nullptr){
      BUSTUB_ASSERT(base_tuple!=nullptr,"base tuple should not be null");
      return UndoLog{false,std::vector<bool>(schema->GetColumnCount(),true),*base_tuple,ts,prev_version};
    }
    std::vector<bool> modified_fields;
    std::vector<Value> values;
    std::vector<Column> cols;
    for(uint32_t i=0; i<schema->GetColumnCount(); i++){
        Value base = base_tuple->GetValue(schema,i);
        Value target = target_tuple->GetValue(schema,i);
        if(!base.CompareExactlyEquals(target)){
            modified_fields.push_back(true);
            values.push_back(base);
            cols.push_back(schema->GetColumn(i));
        }else{
            modified_fields.push_back(false);
        }
    }
    Schema target_schema{cols};
    Tuple tuple{values,&target_schema};
    // std::cout << "base tuple: " << base_tuple->ToString(schema) << std::endl;
    // std::cout << "target tuple: " << target_tuple->ToString(schema) << std::endl;
    // std::cout << "change tuple: " << tuple.ToString(&target_schema) << std::endl;
    return UndoLog{false,modified_fields,tuple,ts,prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
    Tuple base{};
    bool is_null = false;
    if(base_tuple==nullptr){
        base = log.tuple_;
    }else{
        auto res = ReconstructTuple(schema,*base_tuple,{0,false},{log});
        if(res.has_value()){
          base = *res;
        }else{
          is_null = true;
        }
    }
    return GenerateNewUndoLog(schema, is_null?nullptr:&base, target_tuple, log.ts_, log.prev_version_);      
}

auto TsToString(timestamp_t ts) {
  if (ts >= TXN_START_ID) {
    return "txn" + fmt::to_string(ts - TXN_START_ID);
  }
  return fmt::to_string(ts);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  // fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println("");
  // auto iter = table_heap->MakeIterator();
  // while (!iter.IsEnd()) {
  //   RID rid = iter.GetRID();
  //   auto tuple_info = iter.GetTuple();
  //   fmt::println("RID={}/{} ts={} {} tuple={} ", std::to_string(rid.GetPageId()), std::to_string(rid.GetSlotNum()),
  //                TsToString(tuple_info.first.ts_), tuple_info.first.is_deleted_ ? "<del marker>" : "",
  //                tuple_info.second.ToString(&table_info->schema_));
  //   auto undo_link = txn_mgr->GetUndoLink(rid);
  //   std::vector<Value> vec;
  //   for (size_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
  //     vec.push_back(tuple_info.second.GetValue(&table_info->schema_, i));
  //   }
  //   if (undo_link.has_value()) {
  //     if (txn_mgr->txn_map_.count(undo_link->prev_txn_) == 0) {
  //       ++iter;
  //       continue;
  //     }
  //     auto log = txn_mgr->GetUndoLog(undo_link.value());
  //     std::vector<Column> cols;
  //     for (size_t i = 0; i < log.modified_fields_.size(); i++) {
  //       if (log.modified_fields_[i]) {
  //         cols.push_back(table_info->schema_.GetColumn(i));
  //         // vec[i] = log.tuple_.GetValue(log.tuple_, i);
  //       }
  //     }
  //     auto log_schema = Schema(cols);
  //     size_t col_idx = 0;
  //     for (size_t i = 0; i < log.modified_fields_.size(); i++) {
  //       if (log.modified_fields_[i]) {
  //         // cols.push_back(schema->GetColumn(i));
  //         vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
  //       }
  //     }
  //     fmt::println("   {}@{} {} tuple={} ts={}", TsToString(undo_link->prev_txn_),
  //                  std::to_string(undo_link->prev_log_idx_), log.is_deleted_ ? "<del marker>" : "",
  //                  Tuple(vec, &table_info->schema_).ToString(&table_info->schema_), std::to_string(log.ts_));
  //     while (log.prev_version_.IsValid()) {
  //       if (txn_mgr->txn_map_.count(log.prev_version_.prev_txn_) == 0) {
  //         break;
  //       }
  //       log = txn_mgr->GetUndoLog(log.prev_version_);
  //       std::vector<Column> cols;
  //       for (size_t i = 0; i < log.modified_fields_.size(); i++) {
  //         if (log.modified_fields_[i]) {
  //           cols.push_back(table_info->schema_.GetColumn(i));
  //           // vec[i] = log.tuple_.GetValue(log.tuple_, i);
  //         }
  //       }
  //       auto log_schema = Schema(cols);
  //       size_t col_idx = 0;
  //       for (size_t i = 0; i < log.modified_fields_.size(); i++) {
  //         if (log.modified_fields_[i]) {
  //           // cols.push_back(schema->GetColumn(i));
  //           vec[i] = log.tuple_.GetValue(&log_schema, col_idx++);
  //         }
  //       }
  //       fmt::println("   txn{}@{} {} tuple={} ts={}", TsToString(undo_link->prev_txn_),
  //                    std::to_string(undo_link->prev_log_idx_), log.is_deleted_ ? "<del marker>" : "",
  //                    Tuple(vec, &table_info->schema_).ToString(&table_info->schema_), std::to_string(log.ts_));
  //     }
  //   }
  //   ++iter;
  // }
  // fmt::println("");
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
