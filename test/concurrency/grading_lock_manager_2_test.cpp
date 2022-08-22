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
// Correct case
void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};
  bool res;
  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  res = lock_mgr.LockExclusive(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 0, 1);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 0, 2);

  res = lock_mgr.Unlock(txn, rid1);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 0);

  txn_mgr.Commit(txn);
  CheckCommitted(txn);

  delete txn;
}

// Incorrect case
void TwoPLTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockShared(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  try {
    lock_mgr.LockShared(txn, rid0);
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}

/****************************
 * Basic Tests (15 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score: 5
 * Description: Test two phase locking with
 * 1) Correct case
 * 2) Incorrect case: obtain lock after unlock operations
 */
TEST(LockManagerTest, TwoPLTest) {
  TEST_TIMEOUT_BEGIN
  for (size_t i = 0; i < NUM_ITERS; i++) {
    TwoPLTest1();
    TwoPLTest2();
  }
  TEST_TIMEOUT_FAIL_END(1000 * 30)
}

}  // namespace bustub
