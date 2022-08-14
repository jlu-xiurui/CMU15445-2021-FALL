//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_hash_table_scale_test.cpp
//
// Identification: test/container/grading_hash_table_scale_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
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

void ConcurrentScaleTest() {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(13, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

  // Create header_page
  page_id_t page_id;
  bpm->NewPage(&page_id, nullptr);

  // Add perserved_keys
  std::vector<int> perserved_keys;
  std::vector<int> dynamic_keys;
  size_t total_keys = 200000;
  size_t sieve = 10;
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

void ScaleTestCall() {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("foo_pk", bpm, IntComparator(), HashFunction<int>());

  int num_keys = 100000;  // index can fit around 225k int-int pairs

  // Create header_page
  page_id_t page_id;
  bpm->NewPage(&page_id, nullptr);

  //  insert all the keys
  for (int i = 0; i < num_keys; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove half the keys
  for (int i = 0; i < num_keys / 2; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  try to find the removed half
  for (int i = 0; i < num_keys / 2; i++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  //  insert to the 2nd half as duplicates
  for (int i = num_keys / 2; i < num_keys; i++) {
    ht.Insert(nullptr, i, i + 1);
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  get all the duplicates
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove the last duplicates inserted
  for (int i = num_keys / 2; i < num_keys; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i + 1));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  query everything
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove the rest of the remaining keys
  for (int i = num_keys / 2; i < num_keys; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Failed to insert " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  query everything
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key: " << i << std::endl;
  }

  //  Verify Merging Worked
  assert(ht.GetGlobalDepth() < 8);
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

/*
 * Score: 5
 * Description: Insert 200k keys to verify the table capacity
 */
TEST(HashTableScaleTest, ScaleTest) { ScaleTestCall(); }

/*
 * Score: 5
 * Description: Same as MixTest2 but with 100k integer keys
 * and only runs 1 iteration.
 */
TEST(HashTableScaleTest, ConcurrentScaleTest) {
  TEST_TIMEOUT_BEGIN
  ConcurrentScaleTest();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

}  // namespace bustub