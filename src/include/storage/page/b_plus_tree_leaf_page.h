//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define PAGE_ID_SIZE sizeof(page_id_t)
#define LEAF_PAGE_SLOT_CNT ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - 2*PAGE_ID_SIZE) / (sizeof(KeyType) + sizeof(ValueType))) - 1
        // page分配4096字节 如果按照 ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(KeyType) + sizeof(ValueType))) == 255 算，根本分配不了这么多空间
/**
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(page_id_t page_id, int max_size = LEAF_PAGE_SLOT_CNT);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto GetPrevPageId() const -> page_id_t;
  void SetPrevPageId(page_id_t prev_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto KeyIndex(const KeyType &key, const KeyComparator& comparator) const -> int;
  auto ValueAt(int index) const -> ValueType;
  auto Lookup(const KeyType& key, const KeyComparator& comparator) const -> std::optional<ValueType>;
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;
  auto Remove(const KeyType &key, const KeyComparator &comparator) -> bool;
  void CopyLeafData(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *other);
  void NodePrint(std::ostream & stream) const{
    stream << "leaf " << GetPageId() << " content: " << std::endl;
    for(int i=0; i<GetSize(); i++){
      stream << key_array_[i] << " ";
    }
    stream << std::endl;

    for(int i=0; i<GetSize(); i++){
      stream << rid_array_[i] << " ";
    }
    stream << "next: " << GetNextPageId() << " prev: " << GetPrevPageId() <<  std::endl;
    stream << "parent: " << GetParentId() << std::endl;
  }
  /**
   * @brief For test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_{INVALID_PAGE_ID};
  page_id_t prev_page_id_{INVALID_PAGE_ID};
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
