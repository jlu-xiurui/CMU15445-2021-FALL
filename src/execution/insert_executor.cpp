//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "execution/executors/insert_executor.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  is_raw_ = plan->IsRawInsert();
  if (is_raw_) {
    size_ = plan->RawValues().size();
  }
  indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!is_raw_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto exec_ctx = GetExecutorContext();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx->GetLockManager();

  Tuple tmp_tuple;
  RID tmp_rid;
  if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
      const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);
      tmp_tuple = Tuple(raw_value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
        if (!lock_mgr->LockExclusive(txn, tmp_rid)) {
          txn_mgr->Abort(txn);
        }
        for (auto indexinfo : indexes_) {
          indexinfo->index_->InsertEntry(
              tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
              tmp_rid, txn);
          IndexWriteRecord iwr(*rid, table_info_->oid_, WType::INSERT, *tuple, *tuple, indexinfo->index_oid_,
                               exec_ctx->GetCatalog());
          txn->AppendIndexWriteRecord(iwr);
        }
      }
    }
    return false;
  }
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
      for (auto indexinfo : indexes_) {
        indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(),
                                                              indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
                                       tmp_rid, txn);
        txn->GetIndexWriteSet()->emplace_back(tmp_rid, table_info_->oid_, WType::INSERT, tmp_tuple, tmp_tuple,
                                              indexinfo->index_oid_, exec_ctx->GetCatalog());
      }
    }
  }
  return false;
}

}  // namespace bustub
