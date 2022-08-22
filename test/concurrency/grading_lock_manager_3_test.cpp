/**
 * grading_lock_manager_test.cpp
 */

#include <atomic>
#include <future>  //NOLINT
#include <random>
#include <thread>  //NOLINT

#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

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
// Basic single thread upgrade test
void UpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn(0);
  txn_mgr.Begin(&txn);

  bool res = lock_mgr.LockShared(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 1, 0);
  CheckGrowing(&txn);

  res = lock_mgr.LockUpgrade(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 1);
  CheckGrowing(&txn);

  res = lock_mgr.Unlock(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 0);
  CheckShrinking(&txn);

  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);
}

/****************************
 * Basic Tests (15 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score 5
 * Description: test lock upgrade.
 */
TEST(LockManagerTest, UpgradeLockTest) {
  TEST_TIMEOUT_BEGIN
  for (size_t i = 0; i < NUM_ITERS; i++) {
    UpgradeTest();
  }
  TEST_TIMEOUT_FAIL_END(1000 * 30)
}

}  // namespace bustub
