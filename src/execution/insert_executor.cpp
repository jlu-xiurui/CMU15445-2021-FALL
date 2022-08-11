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
  Transaction *txn = exec_ctx_->GetTransaction();
  if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
      const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);
      *tuple = Tuple(raw_value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(*tuple, rid, txn)) {
        for (auto indexinfo : indexes_) {
          indexinfo->index_->InsertEntry(*tuple, *rid, txn);
        }
      }
    }
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->InsertTuple(*tuple, rid, txn)) {
      for (auto indexinfo : indexes_) {
        indexinfo->index_->InsertEntry(*tuple, *rid, txn);
      }
    }
  }
  return false;
}

}  // namespace bustub
