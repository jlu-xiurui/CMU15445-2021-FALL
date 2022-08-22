/**
 * grading_lock_manager_test_1.cpp
 */

#include <atomic>
#include <future>  //NOLINT
#include <random>
#include <thread>  //NOLINT

#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// --- Real tests ---
// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Basic shared lock test under READ_COMMITTED
void BasicTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Basic shared lock test under READ_UNCOMMITTED
void BasicTest3() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_UNCOMMITTED));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    for (const RID &rid : rids) {
      try {
        lock_mgr.LockShared(txns[txn_id], rid);
        CheckAborted(txns[txn_id]);
      } catch (TransactionAbortException &e) {
        CheckAborted(txns[txn_id]);
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Correct case

/****************************
 * Basic Tests (15 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score: 5
 * Description: Basic tests for LockShared and Unlock operations
 * on small amount of rids.
 */
TEST(LockManagerTest, BasicTest) {
  TEST_TIMEOUT_BEGIN
  for (size_t i = 0; i < NUM_ITERS; i++) {
    BasicTest1();
    BasicTest2();
    BasicTest3();
  }
  TEST_TIMEOUT_FAIL_END(1000 * 30)
}

}  // namespace bustub
