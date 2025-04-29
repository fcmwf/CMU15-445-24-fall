//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // define your own type here
  // you may define your own constructor based on your member variables
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  IndexIterator();
  explicit IndexIterator(BufferPoolManager *bufferPoolManager,  page_id_t page ,int node_index = 0) {
    bpm_ = bufferPoolManager;
    page_ = page;
    node_index_ = node_index;
  }
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType , const ValueType >;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return page_ == itr.page_ && node_index_ == itr.node_index_ && bpm_ == itr.bpm_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return page_ != itr.page_ || node_index_ != itr.node_index_ || bpm_ != itr.bpm_;
  }


 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t page_;
  int node_index_;
};

}  // namespace bustub
