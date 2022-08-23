//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include <iostream>
#include "execution/executors/delete_executor.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx->GetLockManager();

  while (child_executor_->Next(tuple, rid)) {
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    } else {
      if (!lock_mgr->LockUpgrade(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }
    if (table_info_->table_->MarkDelete(*rid, txn)) {
      for (auto indexinfo : indexes_) {
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
        IndexWriteRecord iwr(*rid, table_info_->oid_, WType::DELETE, *tuple, *tuple, indexinfo->index_oid_,
                             exec_ctx->GetCatalog());
        txn->AppendIndexWriteRecord(iwr);
      }
    }
  }
  return false;
}
}  // namespace bustub
