//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_concurrent_test.cpp
//
// Identification: test/container/hash_table_concurrent_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
// NOLINTNEXTLINE
#include <chrono>
#include <cstdio>
#include <functional>
// NOLINTNEXTLINE
#include <future>
#include <iostream>
// NOLINTNEXTLINE
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.emplace_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    hash_table->Insert(nullptr, key, value);
  }
  EXPECT_NE(keys[0], keys[1]);
}

// helper function to seperate insert
void InsertHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Insert(nullptr, key, value);
    }
  }
}

// helper function to delete
void DeleteHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : remove_keys) {
    int value = key;
    hash_table->Remove(nullptr, key, value);
  }
}

// helper function to seperate delete
void DeleteHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Remove(nullptr, key, value);
    }
  }
}

void LookupHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    std::vector<int> result;
    bool res = hash_table->GetValue(nullptr, key, &result);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

const size_t NUM_ITERS = 100;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 100;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelper, &hash_table, keys);

    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      hash_table.GetValue(nullptr, key, &result);
      EXPECT_EQ(result.size(), 1);

      int value = key;
      EXPECT_EQ(result[0], value);
    }

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void InsertTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 1000;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelperSplit, &hash_table, keys, 2);

    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      hash_table.GetValue(nullptr, key, &result);
      EXPECT_EQ(result.size(), 1);

      int value = key;
      EXPECT_EQ(result[0], value);
    }

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5};
    InsertHelper(&hash_table, keys, 1);

    std::vector<int> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, 1, DeleteHelper, &hash_table, remove_keys);

    int size = 0;
    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&hash_table, keys, 1);

    std::vector<int> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, 1, DeleteHelperSplit, &hash_table, remove_keys, 2);

    int size = 0;
    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(21, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // first, populate index
    std::vector<int> for_insert;
    std::vector<int> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.emplace_back(i);
      } else {
        for_delete.emplace_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&hash_table, for_delete, 1);

    auto insert_task = [&](int tid) { InsertHelper(&hash_table, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&hash_table, for_delete, tid); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int size = 0;
    std::vector<int> result;
    for (auto key : for_insert) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, for_insert.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(13, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // Add perserved_keys
    std::vector<int> perserved_keys;
    std::vector<int> dynamic_keys;
    size_t total_keys = 300;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.emplace_back(i);
      } else {
        dynamic_keys.emplace_back(i);
      }
    }
    InsertHelper(&hash_table, perserved_keys, 1);
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&hash_table, dynamic_keys, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&hash_table, dynamic_keys, tid); };
    auto lookup_task = [&](int tid) { LookupHelper(&hash_table, perserved_keys, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // Check all reserved keys exist
    size = 0;
    std::vector<int> result;
    for (auto key : perserved_keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, perserved_keys.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

/*
 * Score: 5
 * Description: Concurrently insert a set of keys.
 */
TEST(HashTableConcurrentTest, InsertTest1) {
  TEST_TIMEOUT_BEGIN
  InsertTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Split the concurrent insert test to multiple threads
 * without overlap.
 */
TEST(HashTableConcurrentTest, InsertTest2) {
  TEST_TIMEOUT_BEGIN
  InsertTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 5
 * Description: Concurrently delete a set of keys.
 */
TEST(HashTableConcurrentTest, DeleteTest1) {
  TEST_TIMEOUT_BEGIN
  DeleteTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Split the concurrent delete task to multiple threads
 * without overlap.
 */
TEST(HashTableConcurrentTest, DeleteTest2) {
  TEST_TIMEOUT_BEGIN
  DeleteTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(HashTableConcurrentTest2, MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

/*
 * Score: 10
 * Description: Insert a set of keys. Concurrently insert and delete
 * a different set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(HashTableConcurrentTest2, MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

}  // namespace bustub
