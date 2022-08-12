//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// mock_scan_executor.cpp
//
// Identification: src/execution/mock_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/mock_scan_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

MockScanExecutor::MockScanExecutor(ExecutorContext *exec_ctx, const MockScanPlanNode *plan)
    : AbstractExecutor{exec_ctx}, plan_{plan}, cursor_{0} {}

void MockScanExecutor::Init() {
  // Reset the cursor
  cursor_ = 0;
}

bool MockScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (cursor_ == plan_->GetSize()) {
    // Scan complete
    return EXECUTOR_EXHAUSTED;
  }
  ++cursor_;
  *tuple = MakeDummyTuple();
  *rid = MakeDummyRID();
  plan_->IncrementPollCount();
  return EXECUTOR_ACTIVE;
}

/** @return A dummy tuple according to the output schema */
Tuple MockScanExecutor::MakeDummyTuple() const {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema()->GetColumnCount());
  for (const auto &column : GetOutputSchema()->GetColumns()) {
    values.push_back(ValueFactory::GetZeroValueByType(column.GetType()));
  }
  return Tuple{values, GetOutputSchema()};
}

RID MockScanExecutor::MakeDummyRID() const { return RID{0}; }

}  // namespace bustub
