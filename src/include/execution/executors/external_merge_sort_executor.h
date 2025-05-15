//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
static constexpr uint64_t SORTPAGE_HEADER_SIZE = 8;
/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Spring 2025.
 */
class SortPage {
 public:
  SortPage() = default;

  void Init(uint16_t tuple_size) {
    tuple_size_ = tuple_size;
    num_tuples_ = 0;
    capacity_ = (BUSTUB_PAGE_SIZE - SORTPAGE_HEADER_SIZE) / (tuple_size + sizeof(page_id_t) + sizeof(uint32_t));
  }
  bool InsertTuple(const Tuple &tuple) {
    if (IsFull()) {
      return false;
    }
    page_id_t page_id = tuple.GetRid().GetPageId();
    uint32_t slot_num = tuple.GetRid().GetSlotNum();
    memcpy(tuple_start + num_tuples_*(tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)), &page_id, sizeof(page_id_t));
    memcpy(tuple_start + num_tuples_*(tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)) + sizeof(page_id_t), &slot_num, sizeof(uint32_t));
    memcpy(tuple_start + num_tuples_*(tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)) + sizeof(page_id_t)+sizeof(uint32_t), tuple.GetData(), tuple_size_);
    num_tuples_++;
    return true;
  }

  Tuple GetTuple(size_t index) const {
    BUSTUB_ASSERT(index < num_tuples_, "Index out of bounds");
    page_id_t page_id_;
    uint32_t slot_num_;
    memcpy(&page_id_, tuple_start + index * (tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)), sizeof(page_id_t));
    memcpy(&slot_num_, tuple_start + index * (tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)) + sizeof(page_id_t), sizeof(uint32_t));
    std::vector<char> datas(tuple_size_);
    RID rid(page_id_, slot_num_);
    memcpy(datas.data(), tuple_start + index * (tuple_size_+sizeof(page_id_t)+sizeof(uint32_t)) + sizeof(page_id_t) + sizeof(uint32_t), tuple_size_);
    Tuple tuple(rid, datas.data(), tuple_size_);
    return tuple;
  }

  bool IndexExist(size_t index) const {
    return index < num_tuples_;
  }

  bool IsFull() const { return num_tuples_ >= capacity_; }
  size_t NumTuples() const { return num_tuples_; }

  void PageInfo( std::ofstream *merge_log) const {
    *merge_log << "tuple size: " << tuple_size_ << std::endl;
    *merge_log << "tuple num: " << num_tuples_ << std::endl;
    *merge_log << "capacity: " << capacity_ << std::endl;
  }
 private:
  uint16_t tuple_size_{0};     // 每个元组的大小（字节）
  uint16_t num_tuples_{0};     // 当前页面中已存的元组数
  uint16_t capacity_{0};       // 可插入的元组数
  char tuple_start[0];
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, std::ofstream *merge_log) : pages_(std::move(pages)), bpm_(bpm), merge_log(merge_log) {}
  void DeletePages() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
  }
  auto GetPageCount() const -> size_t { return pages_.size(); }


/** Iterator for iterating on the sorted tuples in one run. */
class Iterator {
  friend class MergeSortRun;

  public:
  Iterator() = default;
  ~Iterator(){
    page_guard_.Drop();
  }
  /**
   * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
   * next sort page.
   */
  auto operator=(Iterator &&that) noexcept -> Iterator &{
    if (this == &that) {
      return *this;
    }
    run_ = that.run_;
    page_index_ = that.page_index_;  
    page_guard_ = std::move(that.page_guard_);
    page_ = that.page_;  
    tuple_index_ = that.tuple_index_;  
    return *this;
  }

  auto operator++() -> Iterator & {
    tuple_index_++;
    if(page_->IndexExist(tuple_index_)){
      return *this;
    }
    page_index_++;
    tuple_index_ = 0;
    if(page_index_ >= run_->GetPageCount()){
      return *this;
    }
    page_guard_ = run_->GetBufferPoolManager()->ReadPage(run_->GetPage(page_index_));
    page_ = page_guard_.As<SortPage>();
    *(run_->merge_log) << "iterator page: " << run_->GetPage(page_index_) << std::endl;
    page_->PageInfo(run_->merge_log);
    return *this;
  }

  /**
   * Dereference the iterator to get the current tuple in the sorted run that the iterator is
   * pointing to.
   */
  auto operator*() -> Tuple {
    // std::cout << "page_index: " << page_index_ << " tuple_index: " << tuple_index_ << std::endl;
    // page_->PageInfo();
    BUSTUB_ASSERT(page_ != nullptr && page_->IndexExist(tuple_index_),
                  "Iterator is not valid");
    return page_->GetTuple(tuple_index_);
  }

  /**
   * Checks whether two iterators are pointing to the same tuple in the same sorted run.
   */
  auto operator==(const Iterator &other) const -> bool {
    return run_ == other.run_ && page_index_ == other.page_index_ && tuple_index_ == other.tuple_index_;
  }

  /**
   * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
   * on different sorted runs.
   */
  auto operator!=(const Iterator &other) const -> bool {
    return run_ != other.run_ || page_index_ != other.page_index_ || tuple_index_ != other.tuple_index_;
  }

  auto IteratorToString() const -> std::string {
    return "Iterator: page_index = " + std::to_string(page_index_) + ", tuple_index = " + std::to_string(tuple_index_);
  }

  private:
  explicit Iterator(const MergeSortRun *run) : run_(run) {
    page_index_ = 0;
    page_guard_ = run_->GetBufferPoolManager()->ReadPage(run_->GetPage(page_index_));
    page_ = page_guard_.As<SortPage>();
    // std::cout << "iterator init" << std::endl;
    *(run_->merge_log) << "iterator page: " << run_->GetPage(page_index_) << std::endl;
    page_->PageInfo(run_->merge_log);
    BUSTUB_ASSERT(page_ != nullptr, "Page is null");
  }
  explicit Iterator(const MergeSortRun *run, bool end) : run_(run) {
    page_index_ = run_->GetPageCount();
    tuple_index_ = 0;
  }
  /** The sorted run that the iterator is iterating on. */
  const MergeSortRun *run_;
  size_t page_index_{0};  
  ReadPageGuard page_guard_;
  const SortPage *page_{nullptr};  
  size_t tuple_index_{0};  
};

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() -> Iterator {
    return Iterator(this);
  }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() -> Iterator {
    return Iterator(this,true);
  }

  auto GetBufferPoolManager() const -> BufferPoolManager * { return bpm_; }
  auto GetPage(size_t index) const -> page_id_t {
    BUSTUB_ASSERT(index < pages_.size(), "Index out of bounds");
    return pages_[index];
  }
  auto GetPages() const -> std::vector<page_id_t>  { return pages_; }
 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
  std::ofstream *merge_log;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void RunsPrint(){
    int i = 0;
    for(auto &run : runs_){
      merge_log << "run " << i++ << " info :" << std::endl;
      merge_log << "sum page number: " << run.GetPageCount() << std::endl;
      for(size_t i=0; i<run.GetPageCount(); i++){
        merge_log << "page:" << run.GetPage(i) << std::endl;
      }
      auto iter = run.Begin();
      // std::cout << "begin: " << iter.IteratorToString() << std::endl;
      // std::cout << "end: " << run.End().IteratorToString() << std::endl;
      while(iter != run.End()){
        auto tuple = *iter;
        merge_log << tuple.ToString(&GetOutputSchema()) << std::endl;
        ++iter;
        // std::cout << "iter: " << iter.IteratorToString() << std::endl;
      }
    }
  }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  /** TODO(P3): You will want to add your own private members here. */
  std::vector<MergeSortRun> runs_;
  MergeSortRun::Iterator iter_{};
  std::ofstream merge_log;
  bool is_end_{false};
  bool is_init_{false};
};

}  // namespace bustub
