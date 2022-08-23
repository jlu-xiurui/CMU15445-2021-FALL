//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <iostream>
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {
  table_oid_t oid = plan->GetTableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  iter_ = table_info_->table_->Begin(exec_ctx->GetTransaction());
  end_ = table_info_->table_->End();
}

void SeqScanExecutor::Init() {
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *out_schema = this->GetOutputSchema();
  auto exec_ctx = GetExecutorContext();
  Transaction *txn = exec_ctx->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx->GetLockManager();
  Schema table_schema = table_info_->schema_;
  while (iter_ != end_) {
    Tuple table_tuple = *iter_;
    *rid = table_tuple.GetRid();
    if (txn->GetSharedLockSet()->count(*rid) == 0U && txn->GetExclusiveLockSet()->count(*rid) == 0U) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !lock_mgr->LockShared(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }
    std::vector<Value> values;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(&table_tuple, &table_schema));
    }
    *tuple = Tuple(values, out_schema);
    auto *predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(tuple, out_schema).GetAs<bool>()) {
      ++iter_;
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_mgr->Unlock(txn, *rid);
      }
      return true;
    }
    if (txn->GetSharedLockSet()->count(*rid) != 0U && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, *rid);
    }
    ++iter_;
  }
  return false;
}

}  // namespace bustub
