//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <iostream>
#include <utility>
#include <vector>
namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id, LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;
  //Wound Wait : Kill all low priority transaction
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue) {
    if (request.lock_mode_ == LockMode::EXCLUSIVE) {
      if (request.txn_id_ > txn_id) {
        txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {
        can_grant = false;
      }
    }
    if (request.txn_id_ == txn_id) {
      request.granted_ = can_grant;
      break;
    }
  }
  if (is_kill) {
    cv.notify_all();
  }
  //Wait the lock
  while (!can_grant) {
    for (auto &request : request_queue) {
      if (request.lock_mode_ == LockMode::EXCLUSIVE &&
          txn_table_[request.txn_id_]->GetState() != TransactionState::ABORTED) {
        break;
      }
      if (request.txn_id_ == txn_id) {
        can_grant = true;
        request.granted_ = true;
      }
    }
    if (!can_grant) {
      cv.wait(lk);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;
  //Wound Wait
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue) {
    if (request.txn_id_ == txn_id) {
      request.granted_ = can_grant;
      break;
    }
    if (request.txn_id_ > txn_id) {
      txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
      is_kill = true;
    } else {
      can_grant = false;
    }
  }
  if (is_kill) {
    cv.notify_all();
  }
  //Wait lock
  while (!can_grant) {
    auto it = request_queue.begin();
    while (txn_table_[it->txn_id_]->GetState() == TransactionState::ABORTED) {
      ++it;
    }
    if (it->txn_id_ == txn_id) {
      can_grant = true;
      it->granted_ = true;
    }
    if (!can_grant) {
      cv.wait(lk);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  lock_request_queue.upgrading_ = txn_id;
  bool can_grant = false;
  while (!can_grant) {
    auto it = request_queue.begin();
    auto target = it;
    can_grant = true;
    bool is_kill = false;
    while (it != request_queue.end() && it->granted_) {
      if (it->txn_id_ == txn_id) {
        target = it;
      } else if (it->txn_id_ > txn_id) {
        txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } else {
        can_grant = false;
      }
      ++it;
    }
    if (is_kill) {
      cv.notify_all();
    }
    if (!can_grant) {
      cv.wait(lk);
    } else {
      target->lock_mode_ = LockMode::EXCLUSIVE;
      lock_request_queue.upgrading_ = INVALID_TXN_ID;
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while (it->txn_id_ != txn_id) {
    ++it;
  }

  request_queue.erase(it);
  cv.notify_all();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
