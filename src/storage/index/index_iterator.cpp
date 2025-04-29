//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // Check if the current page is a leaf page
  if(page_ == INVALID_PAGE_ID) { return true; }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType , const ValueType > {
  BUSTUB_ASSERT(page_ != INVALID_PAGE_ID, "POINT TO THR END");
  ReadPageGuard read_guard = bpm_->ReadPage(page_);
  const LeafPage *node = reinterpret_cast<const LeafPage*>(read_guard.GetData());
  // std::cout << "node page id: " << node->GetPageId() << std::endl;
  // std::cout << "node key: " << node->KeyAt(node_index_) << std::endl;
  // std::cout << "node value: " << node->ValueAt(node_index_) << std::endl;
  // node->NodePrint();
  return std::make_pair(node->KeyAt(node_index_), node->ValueAt(node_index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(page_ != INVALID_PAGE_ID, "POINT TO THR END");
  ReadPageGuard read_guard = bpm_->ReadPage(page_);
  const LeafPage *node = reinterpret_cast<const LeafPage*>(read_guard.GetData());
  if(node->GetSize() - 1 > node_index_){   // next value is still in this page
    node_index_++;
  }else if(node->GetSize() - 1 == node_index_ && node->GetNextPageId() != INVALID_PAGE_ID){ // next value is in next page
    page_ = node->GetNextPageId();
    node_index_ = 0;
  }else{
    page_ = INVALID_PAGE_ID;
    node_index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
