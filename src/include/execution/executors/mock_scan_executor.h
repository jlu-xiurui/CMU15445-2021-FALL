//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// mock_scan_executor.h
//
// Identification: src/include/execution/executors/mock_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/mock_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The MockScanExecutor executor executes a sequential table scan.
 */
class MockScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  MockScanExecutor(ExecutorContext *exec_ctx, const MockScanPlanNode *plan);

  /** Initialize the mock scan. */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() const { return plan_->OutputSchema(); }

 private:
  /** @return A dummy tuple according to the output schema */
  Tuple MakeDummyTuple() const;

  /** @return A dummy RID value */
  RID MakeDummyRID() const;

 private:
  /** MockScanExecutor::Next() returns `true` when scan is incomplete */
  constexpr static const bool EXECUTOR_ACTIVE{true};

  /** MockScanExecutor::Next() returns `false` when scan is complete */
  constexpr static const bool EXECUTOR_EXHAUSTED{false};

  /** The plan node for the scan */
  const MockScanPlanNode *plan_;
  /** The cursor for the current mock scan */
  std::size_t cursor_;
};

}  // namespace bustub
