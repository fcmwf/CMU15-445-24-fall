//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"
#define DEBUG_SORT


namespace bustub {

  
template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {
      cmp_.SetSchema(plan->OutputSchema());
      std::string log_name = "merge.log";
      {
        std::ofstream merge_log(log_name, std::ios::out | std::ios::trunc);
      }
      merge_log.open(log_name, std::ios::out | std::ios::app);
      if (!merge_log.is_open()) {
          std::cerr << "Failed to open file in append mode!" << std::endl;
      }
    }


void Insert(std::ofstream *merge_log, ExecutorContext *exec_ctx_ ,WritePageGuard& guard, const Tuple& tuple, std::vector<page_id_t>& pages, page_id_t &output_page){
    if(!guard.AsMut<SortPage>()->InsertTuple(tuple)){
      // guard.As<SortPage>()->PageInfo(merge_log);
      pages.push_back(output_page);
      output_page = exec_ctx_->GetBufferPoolManager()->NewPage();
      *merge_log << "output page: " << output_page << std::endl;
      guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page);
      guard.AsMut<SortPage>()->Init(tuple.GetLength());
      guard.AsMut<SortPage>()->InsertTuple(tuple);
    }
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
    if(is_init_){
      if(runs_.empty()) {is_end_ = true;}
      else {iter_ = runs_[0].Begin(); }
      return;
    }
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    page_id_t page = exec_ctx_->GetBufferPoolManager()->NewPage();
    merge_log << "new page: " << page << std::endl;
    WritePageGuard write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(page);
    std::vector<Tuple> tuples;
    size_t capacity_;
    while(child_executor_->Next(&tuple, &rid)) {
      tuple.SetRid(rid);
      capacity_ = (BUSTUB_PAGE_SIZE - SORTPAGE_HEADER_SIZE) / (tuple.GetLength()+sizeof(page_id_t)+sizeof(uint32_t));
      if(tuples.size() >= capacity_) {
          std::sort(tuples.begin(), tuples.end(), cmp_);
          // {
          //   merge_log << "after sort" << std::endl;
          //   for(auto&t:tuples){
          //     merge_log << t.ToString(&plan_->OutputSchema()) << std::endl;
          //   }
          // }
          auto sortpage = write_guard.AsMut<SortPage>();
          sortpage->Init(tuple.GetLength());
          // sortpage->PageInfo(merge_log);
          for_each(tuples.begin(), tuples.end(), [&](const Tuple & tuple){ sortpage->InsertTuple(tuple); });
          tuples.clear();
          runs_.emplace_back(std::vector<page_id_t>{page},exec_ctx_->GetBufferPoolManager(),&merge_log);
          page = exec_ctx_->GetBufferPoolManager()->NewPage();
          merge_log << "page full, new page: " << page << std::endl;
          write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(page);
          tuples.push_back(tuple);
      }else{
          tuples.push_back(tuple);
      }
    }
    if(!tuples.empty()){
        // std::cout << "page: " << page << std::endl;
        write_guard.AsMut<SortPage>()->Init(tuples[0].GetLength());
        std::sort(tuples.begin(), tuples.end(), cmp_);
        // write_guard.As<SortPage>()->PageInfo(&merge_log);
        for(auto& t: tuples){
            // merge_log << t.ToString(&plan_->OutputSchema()) << std::endl;
            write_guard.AsMut<SortPage>()->InsertTuple(t);
        }
        write_guard.Drop();
        runs_.emplace_back(std::vector<page_id_t>{page},exec_ctx_->GetBufferPoolManager(),&merge_log);
    }

    merge_log << "capacity: " << capacity_ << std::endl;
    merge_log << "runs size: " << runs_.size() << std::endl;

    // RunsPrint();
    // external_merge
    std::vector<MergeSortRun> new_runs_{};
    while(runs_.size()>1){
        merge_log << "cur runs size: " << runs_.size() << std::endl;
      for(size_t i=0; i+1<runs_.size(); i+=2){
        merge_log << "Merge " << i << " and " << i+1 << std::endl;
        std::vector<page_id_t> pages;
        page_id_t output_page = exec_ctx_->GetBufferPoolManager()->NewPage();  // output buffer
        merge_log << "output page: " << output_page << std::endl;
        write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page);
        auto iter_a = runs_[i].Begin();
        auto iter_b = runs_[i+1].Begin();
        write_guard.AsMut<SortPage>()->Init((*iter_a).GetLength());
        while(iter_a != runs_[i].End() && iter_b != runs_[i+1].End()){
          Tuple insert_tuple;
          if(cmp_(*iter_a, *iter_b)){
            insert_tuple = *iter_a;
            ++iter_a;
          }else{
            insert_tuple = *iter_b;
            ++iter_b;
          }
          merge_log << "tuple: " << insert_tuple.ToString(&plan_->OutputSchema()) << std::endl;
          Insert(&merge_log, exec_ctx_, write_guard, insert_tuple, pages, output_page);
        }
        while(iter_a != runs_[i].End()){
          merge_log << "tuple: " << (*iter_a).ToString(&plan_->OutputSchema()) << std::endl;
          Insert(&merge_log, exec_ctx_, write_guard, *iter_a, pages, output_page);
          ++iter_a;
        }
        while(iter_b != runs_[i+1].End()){
          merge_log << "tuple: " << (*iter_b).ToString(&plan_->OutputSchema()) << std::endl;
          Insert(&merge_log, exec_ctx_, write_guard, *iter_b, pages, output_page);
          ++iter_b;
        }
        pages.push_back(output_page);
        new_runs_.emplace_back(pages, exec_ctx_->GetBufferPoolManager(),&merge_log);
      }
      if(runs_.size() % 2 == 1){
        new_runs_.emplace_back(runs_.back().GetPages(), exec_ctx_->GetBufferPoolManager(),&merge_log);
      }
      write_guard.Drop();
      for_each(runs_.begin(),runs_.end(),[](MergeSortRun &run){ run.DeletePages(); });
      runs_ = std::move(new_runs_);
    }
    merge_log << "external sort finish" << std::endl;
    if(runs_.empty()) {is_end_ = true;}
    else {iter_ = runs_[0].Begin(); }
    is_init_ = true;
}


/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
    if(is_end_){
        return false;
    }
    while(iter_ != runs_[0].End()){
        *tuple = *iter_;
        merge_log << "get tuple: " << tuple->ToString(&plan_->OutputSchema()) << std::endl;
        ++iter_;
        return true;
    }
    return false;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
