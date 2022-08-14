//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a directory page from the BufferPoolManager
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_EQ(0, directory_page->GetGlobalDepth());
  directory_page->SetPageId(10);
  EXPECT_EQ(10, directory_page->GetPageId());
  directory_page->SetLSN(100);
  EXPECT_EQ(100, directory_page->GetLSN());

  // add a few hypothetical bucket pages
  for (unsigned i = 0; i < 8; i++) {
    directory_page->SetBucketPageId(i, i);
  }

  // check for correct bucket page IDs
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(i, directory_page->GetBucketPageId(i));
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    assert(bucket_page->Insert(i, i, IntComparator()));
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(i, bucket_page->KeyAt(i));
    EXPECT_EQ(i, bucket_page->ValueAt(i));
  }

  // remove a few pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // check for the flags
  for (unsigned i = 0; i < 15; i++) {
    if (i < 10) {
      EXPECT_TRUE(bucket_page->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(bucket_page->IsReadable(i));
      } else {
        EXPECT_TRUE(bucket_page->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(bucket_page->IsOccupied(i));
    }
  }

  // try to remove the already-removed pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(!bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, HashTablePageIntegratedTest) {
  size_t buffer_pool_size = 3;
  size_t hash_table_size = 500;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager);

  // setup: one directory page and two bucket pages
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  page_id_t bucket_page_id_1 = INVALID_PAGE_ID;
  page_id_t bucket_page_id_2 = INVALID_PAGE_ID;

  directory_page->IncrGlobalDepth();

  auto bucket_page_1 = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id_1, nullptr)->GetData());
  directory_page->SetLocalDepth(0, 0);

  auto bucket_page_2 = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id_2, nullptr)->GetData());

  directory_page->SetLocalDepth(0, 1);
  directory_page->SetLocalDepth(1, 1);
  directory_page->SetBucketPageId(0, bucket_page_id_1);
  directory_page->SetBucketPageId(1, bucket_page_id_2);

  // add to first bucket page until it is full
  size_t pairs_total_page_1 = hash_table_size / 2;
  for (unsigned i = 0; i < pairs_total_page_1; i++) {
    assert(bucket_page_1->Insert(i, i, IntComparator()));
  }

  // add to second bucket page until it is full
  size_t pairs_total_page_2 = hash_table_size / 2;
  for (unsigned i = 0; i < pairs_total_page_2; i++) {
    assert(bucket_page_2->Insert(i, i, IntComparator()));
  }

  // remove every other pair
  for (unsigned i = 0; i < pairs_total_page_1; i++) {
    if (i % 2 == 1) {
      bucket_page_1->Remove(i, i, IntComparator());
    }
  }

  for (unsigned i = 0; i < pairs_total_page_2; i++) {
    if (i % 2 == 1) {
      bucket_page_2->Remove(i, i, IntComparator());
    }
  }

  // check for the flags
  for (unsigned i = 0; i < pairs_total_page_1 + pairs_total_page_1 / 2; i++) {
    if (i < pairs_total_page_1) {
      EXPECT_TRUE(bucket_page_1->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(bucket_page_1->IsReadable(i));
      } else {
        EXPECT_TRUE(bucket_page_1->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(bucket_page_1->IsOccupied(i));
    }
  }

  for (unsigned i = 0; i < pairs_total_page_2 + pairs_total_page_2 / 2; i++) {
    if (i < pairs_total_page_2) {
      EXPECT_TRUE(bucket_page_2->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(bucket_page_2->IsReadable(i));
      } else {
        EXPECT_TRUE(bucket_page_2->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(bucket_page_2->IsOccupied(i));
    }
  }

  bpm->UnpinPage(bucket_page_id_1, true, nullptr);
  bpm->UnpinPage(bucket_page_id_2, true, nullptr);
  bpm->UnpinPage(directory_page_id, true, nullptr);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
