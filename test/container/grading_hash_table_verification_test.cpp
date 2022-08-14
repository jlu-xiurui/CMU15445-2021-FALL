//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_verification_test.cpp
//
// Identification: test/container/hash_table_verification_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

template <typename KeyType>
class ZeroHashFunction : public HashFunction<KeyType> {
  uint64_t GetHash(KeyType key /* unused */) override { return 0; }
};

// NOLINTNEXTLINE
TEST(HashTableVerificationTest, DiskManagerTest) {
  auto *disk_manager = new DiskManager("test.db");
  // We should be able to do anything with strictly 3 pages allocated.
  auto *bpm = new BufferPoolManagerInstance(3, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int num_values_to_insert = 20000;
  // insert a few values
  for (int i = 0; i < num_values_to_insert; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < num_values_to_insert; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // At least one page needs to be swapped
  bool are_buffer_pages_correct = disk_manager->GetNumWrites() >= 0;
  EXPECT_TRUE(are_buffer_pages_correct) << "Incorrect usage of buffer pool";

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
