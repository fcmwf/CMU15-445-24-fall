//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.h
//
// Identification: src/include/storage/index/b_plus_tree.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>
#include <tuple>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;
enum class PageParser{Borrow,Merge};
/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;
  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  std::unordered_map<page_id_t, BPlusTreePage*> write_nodes_;  // node that locked and may be write
  bool SetPageParent(page_id_t page_id, page_id_t parent_id) {
    if(write_nodes_.count(page_id)){
      write_nodes_[page_id]->SetParentId(parent_id);
      return true;
    }
    return false;
  } 
};


#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
enum class Operation {SEARCH, INSERT, DELETE};
// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;
  
  // helper function
  auto FindLeaf(const KeyType &key , Context &ctx, const Operation &mode ) -> page_id_t;
  auto SplitLeafNode(page_id_t page_id, Context &ctx) -> page_id_t;
  auto SplitInternalNode(page_id_t page_id,  Context &ctx) -> page_id_t;
  void InsertLeaf2Parent(const KeyType &key, page_id_t old_page, page_id_t new_page, Context &ctx);
  void InsertInternalPage2Parent(page_id_t page_id, Context &ctx);

  // Leaf process
  void LeafMerge(page_id_t left_leaf, page_id_t right_leaf, Context &ctx);
  void LeafBorrow(page_id_t page_id, page_id_t sib_id, bool direct, Context &ctx);
  auto LeafGetSibling(page_id_t page_id, Context &ctx) -> std::vector<std::tuple<page_id_t,bool,bool>>;

  // Internal Process
  void InternalProcess(page_id_t page_id, Context &ctx);
  auto InternalGetSibling(page_id_t page_id, Context &ctx) -> std::vector<std::tuple<page_id_t,bool,bool>>;
  auto InternalMerge(page_id_t leaf_page, page_id_t right_page, Context &ctx) -> page_id_t;
  void InternalBorrow(page_id_t page_id, page_id_t sib_id, bool direct, Context &ctx) ;

  auto SiblingParser(std::vector<std::tuple<page_id_t,bool,bool>> &sibling_list,
    const PageParser &parser) -> std::optional<std::tuple<page_id_t,bool>>;
  auto GetMostLeftKey(const page_id_t &page, Context &ctx) -> KeyType;

  bool IsRootPage(page_id_t page_id){ return page_id==root_page_id_; }
  void SetRootPage(page_id_t page_id){ root_page_id_ = page_id; }
    // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);

  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
  
  page_id_t root_page_id_;
  std::shared_mutex mutex_;

  std::ofstream insert_log;
  std::ofstream delete_log;
  std::ofstream temp_log;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
