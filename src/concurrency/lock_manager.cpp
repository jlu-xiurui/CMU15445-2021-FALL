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

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetState() == TransactionState::ABORTED){
    return false;
  }
  if(txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  latch_.lock();
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id,LockMode::SHARED);
  bool can_grant = false;
  while(!can_grant){
    for(auto &request : request_queue){
      if(request.lock_mode_ == LockMode::EXCLUSIVE){
        break;
      }
      if(request.txn_id_ == txn_id){
        can_grant = true;
        request.granted_ = true;
      }
    }
    if(!can_grant){
      std::unique_lock<std::mutex> lk(latch_);
      cv.wait(lk);
    }
  }
  txn->GetSharedLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetState() == TransactionState::ABORTED){
    return false;
  }
  if(txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  latch_.lock();
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id,LockMode::EXCLUSIVE);
  bool can_grant = false;
  while(!can_grant){
    if(request_queue.front().txn_id_ == txn_id){
      can_grant = true;
      request_queue.front().granted_ = true;
    }
    if(!can_grant){
      std::unique_lock<std::mutex> lk(latch_);
      cv.wait(lk);
    }
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetState() == TransactionState::ABORTED){
    return false;
  }
  latch_.lock();
  auto &lock_request_queue = lock_table_[rid];
  if(lock_request_queue.upgrading_ != INVALID_TXN_ID){
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
  }
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  lock_request_queue.upgrading_ = txn_id;
  bool can_grant = false;
  while(!can_grant){
    auto it = request_queue.begin();
    if(it->txn_id_ == txn_id && next(it)->granted_ == false){
      can_grant = true;
      request_queue.front().granted_ = true;
      request_queue.front().lock_mode_ = LockMode::EXCLUSIVE;
    }
    if(!can_grant){
      std::unique_lock<std::mutex> lk(latch_);
      cv.wait(lk);
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetState() == TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }
  /*if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UNLOCK_ON_SHRINKING);
  }*/
  latch_.lock();
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while(it->txn_id_ != txn_id){
    ++it;
  }
  /*if(txn->GetState() == TransactionState::SHRINKING && it->lock_mode_ == LockMode::EXCLUSIVE && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UNLOCK_ON_SHRINKING);
  }*/
  request_queue.erase(it);
  cv.notify_all();
  latch_.unlock();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
