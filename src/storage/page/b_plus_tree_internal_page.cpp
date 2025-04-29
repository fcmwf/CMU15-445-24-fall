//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, int max_size) {
  SetPageId(page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "key index arguement error");  // use index=0 when internal node should be splited
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "key index arguement error");
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "key index arguement error");
  page_id_array_[index] = value;
}

/**
 * @param value The value to search for
 * @return The index that corresponds to the specified value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int{
  for(int i=0; i<GetSize(); i++){
    if(page_id_array_[i] == value){
      return i;
    }
  }
  return -1;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // std::cout << "value at: " << index << std::endl;
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "key index arguement error");
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType& key, const KeyComparator& comparator) const -> ValueType{
  auto target = std::lower_bound(key_array_ + 1, key_array_ + GetSize(), key, [&comparator](const auto &key_array, auto k) {
    return comparator(key_array, k) < 0;
  });
  if (target == key_array_ + GetSize()) {  
    return ValueAt(GetSize()-1);
  }
  if (comparator(*target, key) == 0) return ValueAt(target - key_array_);
  return ValueAt(target - key_array_ - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  auto target = std::lower_bound(key_array_ + 1, key_array_ + GetSize(), key, [&comparator](const KeyType &key_arry, auto k) {
    return comparator(key_arry, k) < 0;
  });
  if (target == key_array_ + GetSize()) {
    key_array_[GetSize()] = key;
    page_id_array_[GetSize()] = value;
    ChangeSizeBy(1);
    return;
  }
  auto d = target - key_array_;
  std::move_backward(target, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(page_id_array_+d, page_id_array_+GetSize(), page_id_array_ + GetSize() + 1);
  key_array_[d] = key;
  page_id_array_[d] = value;
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert_begin(const KeyType &key, const ValueType &value ) {
  std::move_backward(key_array_, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(page_id_array_, page_id_array_+GetSize(), page_id_array_ + GetSize() + 1);
  SetKeyAt(1,key);
  SetValueAt(0,value);
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert_back(const KeyType &key, const ValueType &value){
  key_array_[GetSize()] = key;
  page_id_array_[GetSize()] = value;
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyInternalData(int index, B_PLUS_TREE_INTERNAL_PAGE_TYPE *other) {
  for (int i = index; i < other->GetSize(); ++i) {
    key_array_[i-index] = other->key_array_[i];
    page_id_array_[i-index] = other->page_id_array_[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const ValueType &value) -> bool{
  int value_pos = ValueIndex(value);
  // std::cout << "Remove value: " << value << std::endl;
  // std::cout << "value_pos: " << value_pos << std::endl;

  if(value_pos == GetSize()){   // not found
    return false;
  }
  std::move(key_array_ + value_pos + 1, key_array_ + GetSize(), key_array_ + value_pos);
  std::move(page_id_array_ + value_pos + 1, page_id_array_ + GetSize(), page_id_array_ + value_pos);
  ChangeSizeBy(-1);
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
