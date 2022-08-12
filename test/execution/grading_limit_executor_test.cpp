//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_limit_executor_test.cpp
//
// Identification: test/execution/grading_limit_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "executor_test_util.h"  // NOLINT
#include "test_util.h"           // NOLINT

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

// SELECT colA, colB FROM test_3 LIMIT 10
TEST_F(GradingExecutorTest, BasicLimit) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the limit plan
  auto limit_plan = std::make_unique<LimitPlanNode>(out_schema, seq_scan_plan.get(), 10);

  // Execute sequential scan with limit
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(limit_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 10);
  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
  }
}

// SELECT colA, colB FROM empty_table LIMIT 10
TEST_F(GradingExecutorTest, LimitWithEmptyTable) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}});

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);

  // Construct the limit plan
  auto limit_plan = std::make_unique<LimitPlanNode>(out_schema, seq_scan_plan.get(), 10);

  // Execute sequential scan with limit and offset
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(limit_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT colA, colB FROM test_3 WHERE colB > 500 LIMIT 10
TEST_F(GradingExecutorTest, LimitWithNoneYieldedFromChild) {
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
  auto &schema = table_info->schema_;

  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});

  auto *const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));
  auto *predicate = MakeComparisonExpression(col_a, const500, ComparisonType::GreaterThan);

  // Construct sequential scan
  auto seq_scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, predicate, table_info->oid_);

  // Construct the limit plan
  auto limit_plan = std::make_unique<LimitPlanNode>(out_schema, seq_scan_plan.get(), 10);

  // Execute sequential scan with limit and offset
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(limit_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 0);
}

}  // namespace bustub
