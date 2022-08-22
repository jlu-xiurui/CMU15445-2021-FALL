/**
 * grading_lock_manager_test.cpp
 */

#include <atomic>
#include <random>

#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

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
void WoundWaitBasicTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  std::mutex id_mutex;
  size_t id_hold = 0;
  size_t id_wait = 10;
  size_t id_kill = 1;

  size_t num_wait = 10;
  size_t num_kill = 1;

  std::vector<std::thread> wait_threads;
  std::vector<std::thread> kill_threads;

  Transaction txn(id_hold);
  txn_mgr.Begin(&txn);
  lock_mgr.LockExclusive(&txn, rid0);
  lock_mgr.LockShared(&txn, rid1);

  auto wait_die_task = [&]() {
    id_mutex.lock();
    Transaction wait_txn(id_wait++);
    id_mutex.unlock();
    bool res;
    txn_mgr.Begin(&wait_txn);
    res = lock_mgr.LockShared(&wait_txn, rid1);
    EXPECT_TRUE(res);
    CheckGrowing(&wait_txn);
    CheckTxnLockSize(&wait_txn, 1, 0);
    try {
      res = lock_mgr.LockExclusive(&wait_txn, rid0);
      EXPECT_FALSE(res) << wait_txn.GetTransactionId() << "ERR";
    } catch (const TransactionAbortException &e) {
    } catch (const Exception &e) {
      EXPECT_TRUE(false) << "Test encountered exception" << e.what();
    }

    CheckAborted(&wait_txn);

    txn_mgr.Abort(&wait_txn);
  };

  // All transaction here should wait.
  for (size_t i = 0; i < num_wait; i++) {
    wait_threads.emplace_back(std::thread{wait_die_task});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // TODO(peijingx): guarantee all are waiting on LockExclusive
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto kill_task = [&]() {
    Transaction kill_txn(id_kill++);

    bool res;
    txn_mgr.Begin(&kill_txn);
    res = lock_mgr.LockShared(&kill_txn, rid1);
    EXPECT_TRUE(res);
    CheckGrowing(&kill_txn);
    CheckTxnLockSize(&kill_txn, 1, 0);

    res = lock_mgr.LockShared(&kill_txn, rid0);
    EXPECT_TRUE(res);
    CheckGrowing(&kill_txn);
    CheckTxnLockSize(&kill_txn, 2, 0);

    txn_mgr.Commit(&kill_txn);
    CheckCommitted(&kill_txn);
    CheckTxnLockSize(&kill_txn, 0, 0);
  };

  for (size_t i = 0; i < num_kill; i++) {
    kill_threads.emplace_back(std::thread{kill_task});
  }

  for (size_t i = 0; i < num_wait; i++) {
    wait_threads[i].join();
  }

  CheckGrowing(&txn);
  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);

  for (size_t i = 0; i < num_kill; i++) {
    kill_threads[i].join();
  }
}

// Two threads, check if one will abort.
void WoundWaitDeadlockTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr(&lock_mgr);
  RID rid0(0, 0);
  RID rid1(0, 1);
  Transaction txn1(1);
  txn_mgr.Begin(&txn1);

  bool res;
  res = lock_mgr.LockExclusive(&txn1, rid0);  // txn id 1 has lock on rid0
  EXPECT_TRUE(res);                           // should be granted
  CheckGrowing(&txn1);
  // This will imediately take the lock

  // Use promise and future to identify
  auto task = [&](std::promise<void> lock_acquired) {
    Transaction txn0(0);  // this transaction is older than txn1
    txn_mgr.Begin(&txn0);
    bool res;
    res = lock_mgr.LockExclusive(&txn0,
                                 rid1);  // we should grant it here and make sure that txn1 is aborted.
    EXPECT_TRUE(res);
    CheckGrowing(&txn0);
    res = lock_mgr.LockExclusive(&txn0, rid0);  // this should abort the main txn

    EXPECT_TRUE(res);
    CheckGrowing(&txn0);
    txn_mgr.Commit(&txn0);

    lock_acquired.set_value();
  };

  std::promise<void> lock_acquired;
  std::future<void> lock_acquired_future = lock_acquired.get_future();

  std::thread t{task, std::move(lock_acquired)};
  auto otask = [&](Transaction *tx) {
    while (tx->GetState() != TransactionState::ABORTED) {
    }
    txn_mgr.Abort(tx);
  };
  std::thread w{otask, &txn1};
  lock_acquired_future.wait();  // waiting for the thread to acquire exclusive lock for rid1
  // this should fail, and txn abort and release all the locks.

  t.join();
  w.join();
}

// Large number of lock and unlock operations.
void WoundWaitStressTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::mt19937 generator(time(nullptr));

  size_t num_rids = 100;
  size_t num_threads = 1000;

  std::vector<RID> rids;
  for (uint32_t i = 0; i < num_rids; i++) {
    RID rid{0, i};
    rids.push_back(rid);
  }

  // Task1 is to get shared lock until abort
  auto task1 = [&](int tid) {
    Transaction txn(tid);
    int num_shared = 0;
    int mod = 2;
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        bool res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_shared++;
        CheckTxnLockSize(&txn, num_shared, 0);
      }
    }
    CheckGrowing(&txn);
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        bool res = lock_mgr.Unlock(&txn, rids[i]);
        EXPECT_TRUE(res);  // unlock here should be ok
        CheckShrinking(&txn);
      }
    }
    CheckTxnLockSize(&txn, 0, 0);
  };

  // Task 2 is shared lock from the back
  auto task2 = [&](int tid) {
    Transaction txn(tid);
    int mod = 3;
    int num_shared = 0;
    for (int i = static_cast<int>(rids.size()) - 1; i >= 0; i--) {
      if (i % mod == 0) {
        bool res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_shared++;
        CheckTxnLockSize(&txn, num_shared, 0);
      }
    }
    CheckGrowing(&txn);
    txn_mgr.Commit(&txn);
    CheckTxnLockSize(&txn, 0, 0);
  };

  // Shared lock wants to upgrade
  auto task3 = [&](int tid) {
    Transaction txn(tid);
    int num_exclusive = 0;
    int mod = 6;
    bool res;
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckTxnLockSize(&txn, 0, num_exclusive);
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        CheckTxnLockSize(&txn, 1, num_exclusive);
        res = lock_mgr.LockUpgrade(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_exclusive++;
        CheckTxnLockSize(&txn, 0, num_exclusive);
        CheckGrowing(&txn);
      }
    }
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        res = lock_mgr.Unlock(&txn, rids[i]);

        EXPECT_TRUE(res);
        CheckShrinking(&txn);
        // A fresh RID here
        RID rid{tid, static_cast<uint32_t>(tid)};
        res = lock_mgr.LockShared(&txn, rid);
        EXPECT_FALSE(res);

        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
        CheckTxnLockSize(&txn, 0, 0);
        return;
      }
    }
  };

  // Exclusive lock and unlock
  auto task4 = [&](int tid) {
    Transaction txn(tid);
    // randomly pick
    int index = static_cast<int>(generator() % rids.size());
    bool res = lock_mgr.LockExclusive(&txn, rids[index]);
    if (res) {
      bool res = lock_mgr.Unlock(&txn, rids[index]);
      EXPECT_TRUE(res);
    } else {
      txn_mgr.Abort(&txn);
    }
    CheckTxnLockSize(&txn, 0, 0);
  };

  std::vector<std::function<void(int)>> tasks{task1, task2, task4};
  std::vector<std::thread> threads;
  // only one task3 to ensure success upgrade most of the time
  threads.emplace_back(std::thread{task3, num_threads / 2});
  for (size_t i = 0; i < num_threads; i++) {
    if (i != num_threads / 2) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
  }
  for (size_t i = 0; i < num_threads; i++) {
    // Threads might be returned already
    if (threads[i].joinable()) {
      threads[i].join();
    }
  }
}

// Basically a simple multithreaded version of basic upgrade test
void WoundUpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  Transaction txn(50);
  txn_mgr.Begin(&txn);
  RID rid(0, 0);
  lock_mgr.LockShared(&txn, rid);
  std::atomic<bool> finish_update(false);

  std::atomic<int> id_upgrade = 0;
  std::atomic<int> id_read = 1;

  auto read_task = [&](const std::shared_future<void> &unlock_future) {
    Transaction txn1(id_read++);

    txn_mgr.Begin(&txn1);
    lock_mgr.LockShared(&txn1, rid);
    unlock_future.wait();
    CheckAborted(&txn1);
    txn_mgr.Abort(&txn1);
  };

  auto kill_og = [&](Transaction *tx, const std::shared_future<void> &unlock_future) {
    unlock_future.wait();
    CheckAborted(tx);
    txn_mgr.Abort(tx);
  };

  auto upgrade_task = [&](std::promise<void> un) {
    Transaction txn2(id_upgrade);
    txn_mgr.Begin(&txn2);
    bool res = lock_mgr.LockShared(&txn2, rid);

    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn2, 1, 0);

    res = lock_mgr.LockUpgrade(&txn2, rid);
    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn2, 0, 1);

    un.set_value();
    finish_update = true;
    txn_mgr.Commit(&txn2);
  };

  std::promise<void> unlock;
  std::shared_future<void> unlock_future(unlock.get_future());
  std::vector<std::thread> threads;

  size_t num_threads = 30;
  for (size_t i = 1; i < num_threads; i++) {
    threads.emplace_back(std::thread{read_task, unlock_future});
  }
  threads.emplace_back(std::thread{kill_og, &txn, unlock_future});

  // wait enough time to ensure the read tasks have locked and are waiting
  // on future
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  threads.emplace_back(std::thread{upgrade_task, std::move(unlock)});
  for (auto &thread : threads) {
    thread.join();
  }
}

void FairnessTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn_3(3);
  txn_mgr.Begin(&txn_3);
  lock_mgr.LockExclusive(&txn_3, rid);

  std::atomic<size_t> num_ready = 0;
  std::atomic<size_t> read_index = 0;
  std::vector<int> reads_id{5, 4, 6};

  auto read_task = [&]() {
    Transaction txn(reads_id[read_index++]);
    txn_mgr.Begin(&txn);
    bool res = lock_mgr.LockShared(&txn, rid);
    num_ready++;
    EXPECT_TRUE(res);
    // wait for the write thread to abort this txn
    std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    CheckAborted(&txn);
    txn_mgr.Abort(&txn);
  };

  auto write_task = [&](int id, bool expected_res) {
    Transaction txn(id);
    txn_mgr.Begin(&txn);
    bool res = lock_mgr.LockExclusive(&txn, rid);
    EXPECT_EQ(res, expected_res);
  };

  // This might be flaky
  std::vector<std::thread> reads;
  // push first three
  for (size_t i = 0; i < reads_id.size(); i++) {
    reads.emplace_back(std::thread{read_task});
  }

  txn_mgr.Commit(&txn_3);
  CheckCommitted(&txn_3);
  // wait for all read_tasks to grab latch
  while (num_ready != reads_id.size()) {
    std::this_thread::yield();
  }

  std::thread write1{write_task, 1, true};  // should abort all the reads

  write1.join();
  // all reads should join back
  for (size_t i = 0; i < 3; i++) {
    reads[i].join();
  }
}

// Test write fairness
void FairnessTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn1(10);
  txn_mgr.Begin(&txn1);

  lock_mgr.LockExclusive(&txn1, rid);
  std::mutex mutex;
  std::vector<int> txn_ids;

  std::mutex index_mutex;
  size_t write_index = 0;

  std::vector<int> write_ids{7, 5, 6, 4, 9};
  std::vector<bool> write_expected{true, true, false, true, true};
  std::vector<bool> abort_expected{true, true, true, false, false};
  std::promise<void> up1;

  auto write_task = [&](const std::shared_future<void> &f) {
    index_mutex.lock();
    size_t index = write_index++;
    index_mutex.unlock();

    int id = write_ids[index];
    bool expected_res = write_expected[index];
    bool should_abort = abort_expected[index];

    Transaction txn(id);
    txn_mgr.Begin(&txn);
    bool res;
    try {
      res = lock_mgr.LockExclusive(&txn, rid);
      EXPECT_EQ(expected_res, res);
    } catch (const TransactionAbortException &e) {
      EXPECT_FALSE(expected_res);
      res = false;
    } catch (const Exception &e) {
      EXPECT_TRUE(false) << "Test encountered exception" << e.what();
    }

    if (res) {
      mutex.lock();
      txn_ids.push_back(id);
      mutex.unlock();
      f.wait();
      if (should_abort) {
        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
      } else {
        txn_mgr.Commit(&txn);
      }
    } else {
      f.wait();
      if (should_abort) {
        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
      }
    }
  };
  std::vector<std::thread> writes;
  std::shared_future<void> uf1(up1.get_future());

  for (size_t i = 0; i < write_ids.size(); i++) {
    writes.emplace_back(std::thread{write_task, uf1});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Make sure writer wounds all readers
  up1.set_value();

  // all abort thread should be joined back
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (!write_expected[i]) {
      writes[i].join();
    }
  }
  CheckAborted(&txn1);
  // Join back rest of threads
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (write_expected[i]) {
      writes[i].join();
    }
  }

  // we expect get results in order. Just to make sure with mutex
  size_t j = 0;
  mutex.lock();
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (write_expected[i]) {
      EXPECT_EQ(write_ids[i], txn_ids[j++]);
    }
  }
  mutex.unlock();
}

/****************************
 * Prevention Tests (55 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score 10
 * Description: Check basic case if later txn will
 * die when it's waiting for previous txn is also waiting
 */
TEST(LockManagerTest, WoundWaitTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundWaitBasicTest();
  }
}

/*
 * Score 10
 * Description: Large number of concurrent operations.
 * The main point for this test is to ensure no deadlock
 * happen (test won't hang).
 */
TEST(LockManagerTest, WoundWaitDeadlockTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundWaitDeadlockTest();
  }
}

/*
 * Score 10
 * Description: test lock upgrade.
 * 1) Test 1 is for single thread upgrade test
 * 2) Test 2 is for simple two threads upgrade test
 * 3) Test 3 has a queue with several read requests followed by write requests
 *    and one read request wants to upgrade. It will eventually get the lock
 *    after the all waiting write requests.
 *    Test 3 also tests if later txn won't be added into the wait queue
 *    if the queue has transactions with smaller tid.
 */
TEST(LockManagerTest, WoundUpgradeTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundUpgradeTest();
  }
}

/*
 * Score: 10
 * Description:
 * 1) Queue is fair for incoming read requests
 * 2) Queue is fair for incoming read and write requests
 */
TEST(LockManagerTest, WoundWaitFairnessTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    FairnessTest1();
    FairnessTest2();
  }
}

}  // namespace bustub
