//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <algorithm>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id,int max_size) {
  SetPageId(page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPrevPageId(INVALID_PAGE_ID);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrevPageId() const -> page_id_t {
  return prev_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrevPageId(page_id_t prev_page_id) {
  prev_page_id_ = prev_page_id;
}
/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS 
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "key index arguement error");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS 
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator& comparator) const -> int {
  for(int i=0; i<GetSize(); i++){
    if(comparator(key_array_[i], key) == 0){
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS 
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "value index arguement error");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType& key, const KeyComparator& comparator) const -> std::optional<ValueType>{
  auto res = std::lower_bound(key_array_, key_array_+GetSize(), key,[&comparator](const KeyType& key_array, const KeyType& key){
    return comparator(key_array,key) < 0;
  });
  if(res==key_array_+GetSize() || comparator(*res,key)!=0){
    return std::nullopt;
  }
  return rid_array_[res-key_array_];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool{
  // std::string temp_log_name = "temp.log";
  // std::ofstream temp_log(temp_log_name, std::ios::out | std::ios::app);
  auto target = std::lower_bound(key_array_, key_array_ + GetSize(), key, [&comparator](const KeyType &array_key, auto k) {
    return comparator(array_key, k) < 0;
  });
  if (target == key_array_ + GetSize()) {
    key_array_[GetSize()] = key;
    rid_array_[GetSize()] = value;
    ChangeSizeBy(1);
    return true;
  }
  if (comparator(key, *target) == 0) return false;
  auto d = target - key_array_;

  // temp_log << "insert key: " << key << std::endl;
  // temp_log << "insert value: " << value ;
  // temp_log << "insert pos: " << d << std::endl;
  // temp_log << "max size: " << GetMaxSize() << std::endl;
  // temp_log << "cur size: " << GetSize() << std::endl;
  // temp_log << "LEAF_PAGE_SLOT_CNT: " << LEAF_PAGE_SLOT_CNT << std::endl;


  std::move_backward(target, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(rid_array_ + d, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);

  key_array_[d] = key;
  rid_array_[d] = value;
  ChangeSizeBy(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool{
  auto value_pos = std::find_if(key_array_, key_array_ + GetSize(), [&comparator, &key](const KeyType &array_key) {
      return comparator(array_key, key) == 0; // 比较键值是否相等
  });
  if(value_pos==key_array_+GetSize()){   // not found
    return false;
  }
  std::move(value_pos + 1, key_array_ + GetSize(), value_pos);
  std::move(rid_array_ + (value_pos - key_array_) + 1, rid_array_ + GetSize(), rid_array_ + (value_pos - key_array_));
  ChangeSizeBy(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLeafData(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *other) {
  // std::cout << "Size: " << other->GetSize() << std::endl;
  for (int i = index; i < other->GetSize(); ++i) {
    // std::cout << "index = " << i << std::endl;
    key_array_[i-index] = other->key_array_[i];
    rid_array_[i-index] = other->rid_array_[i];
  }
  // std::cout << "success end" << std::endl;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
