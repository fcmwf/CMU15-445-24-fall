//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <filesystem>
#include <functional>
#include <future>  // NOLINT
#include <thread>  // NOLINT
#include <fcntl.h>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // std::string file_name = "insert_tree.txt";
  // // std::string log_name = std::to_string(thread_itr)+"log.txt";
  // {
  //   std::ofstream outFile(file_name, std::ios::out | std::ios::trunc);
  //   // 文件流会在离开作用域时自动关闭
  // }
  // std::ofstream outFile(file_name, std::ios::app);
  for (auto key : keys) {
    // outFile << "insert key: " << key << std::endl;
    // outFile << "before insert: " << std::endl 
    //         << tree->DrawBPlusTree() << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid);
    // outFile << "after insert: " << std::endl 
    //         << tree->DrawBPlusTree() << std::endl;
  }
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;

  std::string file_name = std::to_string(thread_itr)+"tree.txt";
  // std::string log_name = std::to_string(thread_itr)+"log.txt";
  {
    std::ofstream outFile(file_name, std::ios::out | std::ios::trunc);
    // 文件流会在离开作用域时自动关闭
  }
  std::ofstream outFile(file_name, std::ios::app);

  // int fd = open(log_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  // dup2(fd, STDOUT_FILENO);

  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid);
      // outFile << "insert key: " << key << std::endl;
      // if(key%10 == 0 || key%10 == 1){
      //   outFile << "insert key: " << key << std::endl;
      // }
      // outFile << tree->DrawBPlusTree(); // 将内容追加到文件末尾
      // outFile << "--------------------------------------------" << std::endl;
    }
  }

  outFile.close();                 // 关闭文件
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;

  // std::string file_name =  "delete_tree.txt";
  // {
  //   std::ofstream outFile(file_name, std::ios::out | std::ios::trunc);
  //   // 文件流会在离开作用域时自动关闭
  // }
  // std::ofstream outFile(file_name, std::ios::app);

  for (auto key : remove_keys) {
    // outFile << "delete key: " << key << std::endl;
    // outFile << "before delete: " << std::endl 
    //         << tree->DrawBPlusTree() << std::endl;
    index_key.SetFromInteger(key);
    
    tree->Remove(index_key);
    // outFile << "after delete: " << std::endl 
    //         <<  tree->DrawBPlusTree() << std::endl;
  }
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;

  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key);
    }
  }
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result);
    ASSERT_EQ(res, true);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0], rid);
  }
}

const size_t NUM_ITERS = 20;
const size_t MIXTEST_NUM_ITERS = 20;
static const size_t BPM_SIZE = 50;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    std::cout << "Test1 round: " << iter << std::endl;
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 50;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(2, InsertHelper, &tree, keys);

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      ASSERT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      auto location = pair.second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    ASSERT_EQ(current_key, keys.size() + 1);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void InsertTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    std::cout << "Test2 round: " << iter << std::endl;
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 50;
    // int64_t scale_factor = 10;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

    std::ofstream outFile("tree.txt");
    outFile << tree.DrawBPlusTree();
    outFile.close();

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      // std::cout << "query key: " << key << std::endl;
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      ASSERT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      auto location = pair.second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    ASSERT_EQ(current_key, keys.size() + 1);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
  
}

void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5};

    InsertHelper(&tree, keys);
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    // std::cout << tree.DrawBPlusTree() << std::endl;
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;

    std::vector<int64_t> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    // std::cout << tree.DrawBPlusTree() << std::endl;
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    int64_t start_key = 2;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      auto location = pair.second;
      // std::cout << "key: " << pair.first << std::endl;
      // std::cout << "value: " << pair.second << std::endl;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    ASSERT_EQ(size, 1);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&tree, keys);
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    // std::cout << tree.DrawBPlusTree() << std::endl;
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    // std::cout << tree.DrawBPlusTree() << std::endl;
    // std::cout << "----------------------------------------------------------------------------------------------------"
    //           << std::endl;
    int64_t start_key = 7;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      auto location = pair.second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    ASSERT_EQ(size, 4);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < MIXTEST_NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    int64_t sieve = 2;  // divide evenly
    int64_t total_keys = 100;
    for (int64_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete);

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 8;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(tasks[i % tasks.size()], i);
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    std::cout << "finish" << std::endl;
    std::string file_name = "tree.txt";
    { std::ofstream outFile(file_name, std::ios::out | std::ios::trunc);}
    std::ofstream outFile(file_name, std::ios::app);
    outFile << tree.DrawBPlusTree();
    int64_t size = 0;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      ASSERT_EQ((pair.first).ToString(), for_insert[size]);
      size++;
    }

    ASSERT_EQ(size, for_insert.size());

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest2Call() {
  for (size_t iter = 0; iter < MIXTEST_NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManagerUnlimitedMemory();
    auto *bpm = new BufferPoolManager(BPM_SIZE, disk_manager);

    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

    // Add perserved_keys
    std::vector<int64_t> perserved_keys;
    std::vector<int64_t> dynamic_keys;
    int64_t total_keys = 1000;
    int64_t sieve = 5;
    for (int64_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.push_back(i);
      } else {
        dynamic_keys.push_back(i);
      }
    }
    InsertHelper(&tree, perserved_keys);

    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys); };
    auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(tasks[i % tasks.size()], i);
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // Check all reserved keys exist
    size = 0;

    for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      if ((pair.first).ToString() % sieve == 0) {
        size++;
      }
    }

    ASSERT_EQ(size, perserved_keys.size());

    delete disk_manager;
    delete bpm;
  }
}

TEST(BPlusTreeConcurrentTest,  InsertTest1) {  // NOLINT
  InsertTest1Call();
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {  // NOLINT
  // auto start = std::chrono::high_resolution_clock::now();
  InsertTest2Call();
  // auto end = std::chrono::high_resolution_clock::now();
  // std::chrono::duration<double> elapsed = end - start;
  // std::cout << "程序运行时间: " << elapsed.count() << " 秒" << std::endl;
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {  // NOLINT
  DeleteTest1Call();
}

TEST(BPlusTreeConcurrentTest,  DeleteTest2) {  // NOLINT
  DeleteTest2Call();
}

TEST(BPlusTreeConcurrentTest,  MixTest1) {  // NOLINT
  MixTest1Call();
}

TEST(BPlusTreeConcurrentTest,  MixTest2) {  // NOLINT
  MixTest2Call();
}
}  // namespace bustub
