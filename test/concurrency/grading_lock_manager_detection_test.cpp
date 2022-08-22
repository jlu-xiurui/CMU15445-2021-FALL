/**
 * grading_lock_manager_detection_test.cpp
 */

#include <atomic>
#include <future>  //NOLINT
#include <random>
#include <thread>  //NOLINT

#include "common/config.h"
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
/****************************
 * Graph and Detection Tests (35 pts)
 ****************************/

void BasicCycleTest() {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::seconds(5);
  TransactionManager txn_mgr{&lock_mgr};

  /*** Create 0->1->0 cycle ***/
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  EXPECT_EQ(2, lock_mgr.GetEdgeList().size());

  txn_id_t txn;
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  lock_mgr.RemoveEdge(1, 0);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));

  /*** Create 0->1->2->0 cycle ***/
  lock_mgr.AddEdge(1, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));

  lock_mgr.AddEdge(2, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(2, txn);

  lock_mgr.RemoveEdge(1, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
}

void EdgeTest() {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::seconds(5);

  TransactionManager txn_mgr{&lock_mgr};
  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(txn_ids), std::end(txn_ids), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

void MultipleCycleTest() {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::seconds(5);
  TransactionManager txn_mgr{&lock_mgr};

  /*** Create 0->1->0 cycle ***/
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  EXPECT_EQ(2, lock_mgr.GetEdgeList().size());

  txn_id_t txn;
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  /*** Create 2->3->4->2 cycle ***/
  lock_mgr.AddEdge(2, 3);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  lock_mgr.AddEdge(3, 4);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  lock_mgr.AddEdge(4, 2);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  EXPECT_EQ(5, lock_mgr.GetEdgeList().size());

  /*** Destroy 0->1->0 cycle ***/
  lock_mgr.RemoveEdge(1, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(txn, 4);
  EXPECT_EQ(4, lock_mgr.GetEdgeList().size());

  /*** Destroy 2->3->4->2 cycle ***/
  lock_mgr.RemoveEdge(4, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(3, lock_mgr.GetEdgeList().size());
}

void OverlappingCyclesTest() {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::seconds(5);

  TransactionManager txn_mgr{&lock_mgr};

  /*** Create 0->1->2->3->4->5->0 cycle ***/
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 2);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 5);
  lock_mgr.AddEdge(5, 0);
  EXPECT_EQ(6, lock_mgr.GetEdgeList().size());

  txn_id_t txn;
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(5, txn);

  /*** Create 2->6->7->2 cycle ***/
  lock_mgr.AddEdge(2, 6);
  lock_mgr.AddEdge(6, 7);
  lock_mgr.AddEdge(7, 2);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(5, txn);
  EXPECT_EQ(9, lock_mgr.GetEdgeList().size());

  /*** Destroy large cycle ***/
  lock_mgr.RemoveEdge(5, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(7, txn);
  EXPECT_EQ(8, lock_mgr.GetEdgeList().size());

  /*** Destroy small cycle ***/
  lock_mgr.RemoveEdge(7, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(7, lock_mgr.GetEdgeList().size());
}

void BasicDeadlockDetectionTest() {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::milliseconds(500);
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockExclusive(txn0, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    lock_mgr.LockExclusive(txn0, rid1);

    lock_mgr.Unlock(txn0, rid0);
    lock_mgr.Unlock(txn0, rid1);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockExclusive(txn1, rid1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    try {
      res = lock_mgr.LockExclusive(txn1, rid0);
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    } catch (TransactionAbortException &e) {
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    }
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}

void LargeDeadlockDetectionTest() {
  const int num_threads = 50;
  cycle_detection_interval = std::chrono::milliseconds(num_threads * 100);
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;

  // Create RIDs and Txns
  for (int i = 0; i < num_threads; i++) {
    rids.emplace_back(i, i);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  // Lock transactions in a large cycle
  // Txn i locks rids[i] and rids[(i+1)%num_threads]
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread([&, i] {
      // Sleep so previous threads can lock their things
      std::this_thread::sleep_for(std::chrono::milliseconds(i * num_threads));
      EXPECT_EQ(true, lock_mgr.LockExclusive(txns[i], rids[i]));
      EXPECT_EQ(TransactionState::GROWING, txns[i]->GetState());
      std::this_thread::sleep_for(std::chrono::milliseconds((i + 2) * num_threads * 5));

      // This will block
      bool res;
      if (i < num_threads - 1) {
        res = lock_mgr.LockExclusive(txns[i], rids[(i + 1) % num_threads]);
        EXPECT_EQ(true, res);
        lock_mgr.Unlock(txns[i], rids[i]);
        lock_mgr.Unlock(txns[i], rids[(i + 1) % num_threads]);
        EXPECT_EQ(TransactionState::SHRINKING, txns[i]->GetState());
        txn_mgr.Commit(txns[i]);
        EXPECT_EQ(TransactionState::COMMITTED, txns[i]->GetState());

      } else {
        assert(num_threads - 1 == i);

        try {
          res = lock_mgr.LockExclusive(txns[i], rids[(i + 1) % num_threads]);
          EXPECT_EQ(TransactionState::ABORTED, txns[i]->GetState());
          txn_mgr.Abort(txns[i]);
        } catch (TransactionAbortException &e) {
          EXPECT_EQ(TransactionState::ABORTED, txns[i]->GetState());
          txn_mgr.Abort(txns[i]);
        }
      }
    }));
  }

  // Join threads
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  // Delete transactions
  for (int i = 0; i < num_threads; i++) {
    delete txns[i];
  }
}

/*
 * Score 5
 * Description: Basic Cycle test
 */
TEST(LockManagerDetectionTest, BasicCycleTest) {
  TEST_TIMEOUT_BEGIN
  BasicCycleTest();
  TEST_TIMEOUT_FAIL_END(1000 * 20)
}
/*
 * Score 5
 * Description: Tests that random edges are added and can be found with GetEdgeList function
 */
TEST(LockManagerDetectionTest, EdgeTest) {
  TEST_TIMEOUT_BEGIN
  EdgeTest();
  TEST_TIMEOUT_FAIL_END(1000 * 60)
}
/*
 * Score 5
 * Description: Check they correctly victim transactions in the right order when multiple
 * cycles occur
 */
TEST(LockManagerDetectionTest, MultipleCycleTest) {
  TEST_TIMEOUT_BEGIN
  MultipleCycleTest();
  TEST_TIMEOUT_FAIL_END(1000 * 60)
}
/*
 * Score 5
 * Description: Simple two transaction deadlock detection test
 */
TEST(LockManagerDetectionTest, BasicDeadlockDetectionTest) {
  TEST_TIMEOUT_BEGIN
  BasicDeadlockDetectionTest();
  TEST_TIMEOUT_FAIL_END(1000 * 60)
}
/*
 * Score 5
 * Description: Check they correctly victim transactions in the right order when cycles overlap
 */

TEST(LockManagerDetectionTest, OverlappingCyclesTest) {
  TEST_TIMEOUT_BEGIN
  OverlappingCyclesTest();
  TEST_TIMEOUT_FAIL_END(1000 * 60)
}
/*
 * Score 10
 * Description: Check that they can handle large cycles by creating one big ring cycle
 */
TEST(LockManagerDetectionTest, LargeDeadlockDetectionTest) {
  TEST_TIMEOUT_BEGIN
  LargeDeadlockDetectionTest();
  TEST_TIMEOUT_FAIL_END(1000 * 60)
}
}  // namespace bustub
