//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_distinct_executor_test.cpp
//
// Identification: test/execution/grading_distinct_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>

#include "execution/executors/distinct_executor.h"
#include "executor_test_util.h"  // NOLINT
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT

namespace bustub {

// Parameters for index construction
using KeyType = GenericKey<8>;
using ValueType = RID;
using ComparatorType = GenericComparator<8>;
using HashFunctionType = HashFunction<KeyType>;

/** Index creation parameters for a BIGINT key */
constexpr static const auto BIGINT_SIZE = 8;
using BigintKeyType = GenericKey<BIGINT_SIZE>;
using BigintValueType = RID;
using BigintComparatorType = GenericComparator<BIGINT_SIZE>;
using BigintHashFunctionType = HashFunction<BigintKeyType>;

#define GradingExecutorTest ExecutorTest

TEST_F(GradingExecutorTest, DistinctEmptyTable) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the distinct plan
  auto distinct_plan = std::make_unique<DistinctPlanNode>(out_schema, seq_scan_plan.get());

  // Execute sequential scan with DISTINCT
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(distinct_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT DISTINCT colA FROM test_7
TEST_F(GradingExecutorTest, DistinctWithoutDuplicates) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the distinct plan
  auto distinct_plan = std::make_unique<DistinctPlanNode>(out_schema, seq_scan_plan.get());

  // Execute sequential scan with DISTINCT
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(distinct_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results; all values in colA are unique
  ASSERT_EQ(result_set.size(), TEST7_SIZE);

  // Results are unordered
  std::vector<int64_t> results{};
  results.reserve(result_set.size());
  std::transform(result_set.cbegin(), result_set.cend(), std::back_inserter(results), [=](const Tuple &tuple) {
    return tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>();
  });
  std::sort(results.begin(), results.end());

  // Expect keys 0 - 99
  std::vector<int64_t> expected(result_set.size());
  std::iota(expected.begin(), expected.end(), 0);

  ASSERT_TRUE(std::equal(results.cbegin(), results.cend(), expected.cbegin()));
}

// SELECT DISTINCT colC FROM test_7
TEST_F(GradingExecutorTest, DistinctWithDuplicates) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
  auto &schema = table_info->schema_;

  auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
  auto *out_schema = MakeOutputSchema({{"colC", col_c}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the distinct plan
  auto distinct_plan = std::make_unique<DistinctPlanNode>(out_schema, seq_scan_plan.get());

  // Execute sequential scan with DISTINCT
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(distinct_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results; colC is cyclic on 0 - 9
  ASSERT_EQ(result_set.size(), 10);

  // Results are unordered
  std::vector<int32_t> results{};
  results.reserve(result_set.size());
  std::transform(result_set.cbegin(), result_set.cend(), std::back_inserter(results), [=](const Tuple &tuple) {
    return tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>();
  });
  std::sort(results.begin(), results.end());

  // Expect keys 0 - 9
  std::vector<int32_t> expected(result_set.size());
  std::iota(expected.begin(), expected.end(), 0);

  ASSERT_TRUE(std::equal(results.cbegin(), results.cend(), expected.cbegin()));
}

// SELECT DISTINCT colA, colC FROM test_7
TEST_F(GradingExecutorTest, DistinctMultipleColumns) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}, {"colC", col_c}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the distinct plan
  auto distinct_plan = std::make_unique<DistinctPlanNode>(out_schema, seq_scan_plan.get());

  // Execute sequential scan with DISTINCT
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(distinct_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results; addition of colA should make all rows distinct
  ASSERT_EQ(result_set.size(), TEST7_SIZE);

  // Results are unordered
  std::vector<std::pair<int64_t, int32_t>> results{};
  results.reserve(result_set.size());
  std::transform(result_set.cbegin(), result_set.cend(), std::back_inserter(results), [=](const Tuple &tuple) {
    const int64_t a = tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>();
    const int32_t c = tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>();
    return std::make_pair(a, c);
  });
  std::sort(
      results.begin(), results.end(),
      [](const std::pair<int64_t, int32_t> &a, const std::pair<int64_t, int32_t> &b) { return a.first < b.first; });

  for (std::size_t i = 0; i < results.size(); ++i) {
    const auto a = results[i].first;
    const auto c = results[i].second;
    ASSERT_EQ(static_cast<int64_t>(i), a);
    ASSERT_EQ(static_cast<int32_t>(i % 10), c);
  }
}

}  // namespace bustub
