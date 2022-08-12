//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_aggregation_executor_test.cpp
//
// Identification: test/execution/grading_aggregation_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <numeric>
#include <unordered_set>

#include "execution/executors/aggregation_executor.h"
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

// SELECT COUNT(colB) from test_3;
TEST_F(GradingExecutorTest, AggregationCount) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto cola_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colB", cola_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *count_b = MakeAggregateValueExpression(false, 0);

    agg_schema = MakeOutputSchema({{"countB", count_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{cola_b}, std::vector<AggregationType>{AggregationType::CountAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify the size of the result set
  ASSERT_EQ(result_set.size(), 1);

  // Count aggregation should include all tuples
  const auto countb_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("countB")).GetAs<int32_t>();
  ASSERT_EQ(countb_val, TEST3_SIZE);
}

// SELECT MIN(colB) from test_3;
TEST_F(GradingExecutorTest, AggregationMin) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto cola_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colB", cola_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *min_b = MakeAggregateValueExpression(false, 0);

    agg_schema = MakeOutputSchema({{"minB", min_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{cola_b}, std::vector<AggregationType>{AggregationType::MinAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify the size of the result set
  ASSERT_EQ(result_set.size(), 1);

  // Min aggregation should identify the minimum value for Column B in the table
  const auto min_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("minB")).GetAs<int32_t>();
  ASSERT_EQ(min_b_val, static_cast<int32_t>(0));
}

// SELECT MAX(colB) from test_3;
TEST_F(GradingExecutorTest, AggregationMax) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto cola_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colB", cola_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *max_b = MakeAggregateValueExpression(false, 0);

    agg_schema = MakeOutputSchema({{"maxB", max_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{cola_b}, std::vector<AggregationType>{AggregationType::MaxAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify the size of the result set
  ASSERT_EQ(result_set.size(), 1);

  // Max aggregation should identify the maximum value for Column B in the table
  const auto max_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("maxB")).GetAs<int32_t>();
  ASSERT_EQ(max_b_val, static_cast<int32_t>(TEST3_SIZE - 1));
}

// SELECT SUM(colB) from test_3;
TEST_F(GradingExecutorTest, AggregationSum) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto cola_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colB", cola_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *sum_b = MakeAggregateValueExpression(false, 0);

    agg_schema = MakeOutputSchema({{"sumB", sum_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{cola_b}, std::vector<AggregationType>{AggregationType::SumAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify the size of the result set
  ASSERT_EQ(result_set.size(), 1);

  // Sum aggregation should compute sum of all values in Column B
  const auto sum_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("sumB")).GetAs<int32_t>();
  ASSERT_EQ(sum_b_val, static_cast<int32_t>(TEST3_SIZE * (TEST3_SIZE - 1) / 2));
}

// SELECT COUNT(colB), SUM(colB), MIN(colB), MAX(colB) from test_3;
TEST_F(GradingExecutorTest, MultipleAggregationsOverSingleColumn) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto cola_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colB", cola_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *count_b = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sum_b = MakeAggregateValueExpression(false, 1);
    const AbstractExpression *min_b = MakeAggregateValueExpression(false, 2);
    const AbstractExpression *max_b = MakeAggregateValueExpression(false, 3);

    agg_schema = MakeOutputSchema({{"countB", count_b}, {"sumB", sum_b}, {"minB", min_b}, {"maxB", max_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{cola_b, cola_b, cola_b, cola_b},
        std::vector<AggregationType>{AggregationType::CountAggregate, AggregationType::SumAggregate,
                                     AggregationType::MinAggregate, AggregationType::MaxAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set.size(), 1);

  auto countb_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("countB")).GetAs<int32_t>();
  auto sum_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("sumB")).GetAs<int32_t>();
  auto min_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("minB")).GetAs<int32_t>();
  auto max_b_val = result_set[0].GetValue(agg_schema, agg_schema->GetColIdx("maxB")).GetAs<int32_t>();

  // Should count all tuples
  ASSERT_EQ(countb_val, TEST3_SIZE);

  // Should sum from 0 to TEST3_SIZE
  ASSERT_EQ(sum_b_val, static_cast<int32_t>(TEST3_SIZE * (TEST3_SIZE - 1) / 2));

  // Minimum should be 0
  ASSERT_EQ(min_b_val, static_cast<int32_t>(0));

  // Maximum should be TEST1_SIZE - 1
  ASSERT_EQ(max_b_val, static_cast<int32_t>(TEST3_SIZE - 1));
}

// SELECT COUNT(colB) FROM test_7 GROUP BY colC
TEST_F(GradingExecutorTest, AggregationCountWithGroupBy) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *cola_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *col_c = MakeColumnValueExpression(*scan_schema, 0, "colC");

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{col_c};

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{cola_b};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate};
    const AbstractExpression *count_b = MakeAggregateValueExpression(false, 0);

    // Create plan
    agg_schema = MakeOutputSchema({{"countB", count_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // colC has 10 possible values, should have 10 results
  ASSERT_EQ(result_set.size(), 10);

  // Should have count of 10 in each of the 10 groups
  for (const auto &result : result_set) {
    ASSERT_EQ(result.GetValue(agg_schema, agg_schema->GetColIdx("countB")).GetAs<int32_t>(), static_cast<int32_t>(10));
  }
}

// SELECT MIN(colB), colC FROM test_7 GROUP BY colC
TEST_F(GradingExecutorTest, AggregationMinWithGroupBy) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *col_c = MakeColumnValueExpression(*scan_schema, 0, "colC");

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{col_c};
    const AbstractExpression *groupby_c = MakeAggregateValueExpression(true, 0);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{col_b};
    std::vector<AggregationType> agg_types{AggregationType::MinAggregate};
    const AbstractExpression *min_b = MakeAggregateValueExpression(false, 0);

    // Create plan
    agg_schema = MakeOutputSchema({{"minB", min_b}, {"groupbyC", groupby_c}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // colC has 10 possible values, should have 10 results
  ASSERT_EQ(result_set.size(), 10);

  for (const auto &result : result_set) {
    const auto min_val = result.GetValue(agg_schema, agg_schema->GetColIdx("minB")).GetAs<int32_t>();
    const auto group_by = result.GetValue(agg_schema, agg_schema->GetColIdx("groupbyC")).GetAs<int32_t>();

    // Group by values range on [0, 9]
    ASSERT_GE(group_by, 0);
    ASSERT_LT(group_by, 10);

    // Column B is serial, so the minimum value for each group
    // is the first value encountered for that group
    ASSERT_EQ(min_val, group_by);
  }
}

// SELECT MAX(colB), colC FROM test_7 GROUP BY colC
TEST_F(GradingExecutorTest, AggregationMaxWithGroupBy) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *col_c = MakeColumnValueExpression(*scan_schema, 0, "colC");

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{col_c};
    const AbstractExpression *groupby_c = MakeAggregateValueExpression(true, 0);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{col_b};
    std::vector<AggregationType> agg_types{AggregationType::MaxAggregate};
    const AbstractExpression *max_b = MakeAggregateValueExpression(false, 0);

    // Create plan
    agg_schema = MakeOutputSchema({{"maxB", max_b}, {"groupbyC", groupby_c}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // colC has 10 possible values, should have 10 results
  ASSERT_EQ(result_set.size(), 10);

  for (const auto &result : result_set) {
    const auto max_val = result.GetValue(agg_schema, agg_schema->GetColIdx("maxB")).GetAs<int32_t>();
    const auto group_by = result.GetValue(agg_schema, agg_schema->GetColIdx("groupbyC")).GetAs<int32_t>();

    // Group by values range on [0, 9]
    ASSERT_GE(group_by, 0);
    ASSERT_LT(group_by, 10);

    // Column B is serial, so the maximum value for each group
    // is the last value encountered for that group
    const auto expected = TEST7_SIZE - result_set.size() + group_by;
    ASSERT_EQ(max_val, expected);
  }
}

// SELECT SUM(colB), colC FROM test_7 GROUP BY colC
TEST_F(GradingExecutorTest, AggregationSumWithGroupBy) {
  // Construct the sequential scan
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  // Construct the aggregation
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *col_c = MakeColumnValueExpression(*scan_schema, 0, "colC");

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{col_c};
    const AbstractExpression *groupby_c = MakeAggregateValueExpression(true, 0);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{col_b};
    std::vector<AggregationType> agg_types{AggregationType::SumAggregate};
    const AbstractExpression *sum_b = MakeAggregateValueExpression(false, 0);

    // Create plan
    agg_schema = MakeOutputSchema({{"sumB", sum_b}, {"groupbyC", groupby_c}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute the aggregation
  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // colC has 10 possible values, should have 10 results
  ASSERT_EQ(result_set.size(), 10);

  for (const auto &result : result_set) {
    const auto sum_val = result.GetValue(agg_schema, agg_schema->GetColIdx("sumB")).GetAs<int32_t>();
    const auto group_by = result.GetValue(agg_schema, agg_schema->GetColIdx("groupbyC")).GetAs<int32_t>();

    // Group by values range on [0, 9]
    ASSERT_GE(group_by, 0);
    ASSERT_LT(group_by, 10);

    // NOTE: can't wait for effing ranges, this should be so much easier
    std::vector<int> v(10);
    std::generate(v.begin(), v.end(), [n = group_by]() mutable {
      auto tmp = n;
      n += 10;
      return tmp;
    });
    const auto expected = std::accumulate(v.begin(), v.end(), 0);
    ASSERT_EQ(sum_val, expected);
  }
}

// SELECT COUNT(colA), colB FROM test_1 GROUP BY colB HAVING count(colA) > 100
TEST_F(GradingExecutorTest, AggregationWithGroupByAndHaving) {
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_a = MakeColumnValueExpression(*scan_schema, 0, "colA");
    const AbstractExpression *col_b = MakeColumnValueExpression(*scan_schema, 0, "colB");

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{col_b};
    const AbstractExpression *groupby_b = MakeAggregateValueExpression(true, 0);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{col_a};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate, AggregationType::SumAggregate};
    const AbstractExpression *count_a = MakeAggregateValueExpression(false, 0);

    // Make having clause
    const AbstractExpression *having = MakeComparisonExpression(
        count_a, MakeConstantValueExpression(ValueFactory::GetIntegerValue(100)), ComparisonType::GreaterThan);

    // Create plan
    agg_schema = MakeOutputSchema({{"countA", count_a}, {"colB", groupby_b}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), having, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute the aggregation
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  std::unordered_set<int32_t> encountered{};
  for (const auto &tuple : result_set) {
    // Should have countA > 100
    const auto count_a = tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>();
    ASSERT_GT(count_a, 100);

    // Should have unique colBs.
    const auto col_b = tuple.GetValue(agg_schema, agg_schema->GetColIdx("colB")).GetAs<int32_t>();
    ASSERT_EQ(encountered.count(col_b), 0);
    encountered.insert(col_b);

    // Sanity check: ColB should also be within [0, 10).
    ASSERT_GE(col_b, 0);
    ASSERT_LT(col_b, 10);
  }
}

// SELECT COUNT(colA), SUM(colA), MIN(colA), MAX(colA) from test_1;
TEST_F(GradingExecutorTest, AggregationIntegrated1) {
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    scan_schema = MakeOutputSchema({{"colA", col_a}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_a = MakeColumnValueExpression(*scan_schema, 0, "colA");
    const AbstractExpression *count_a = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sum_a = MakeAggregateValueExpression(false, 1);
    const AbstractExpression *min_a = MakeAggregateValueExpression(false, 2);
    const AbstractExpression *max_a = MakeAggregateValueExpression(false, 3);

    agg_schema = MakeOutputSchema({{"countA", count_a}, {"sumA", sum_a}, {"minA", min_a}, {"maxA", max_a}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, scan_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{col_a, col_a, col_a, col_a},
        std::vector<AggregationType>{AggregationType::CountAggregate, AggregationType::SumAggregate,
                                     AggregationType::MinAggregate, AggregationType::MaxAggregate});
  }

  // Execute the aggregation
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Should only have a single tuple in the result set
  ASSERT_EQ(result_set.size(), 1);

  const Tuple result = result_set.front();
  const auto count_a_val = result.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>();
  const auto sum_a_val = result.GetValue(agg_schema, agg_schema->GetColIdx("sumA")).GetAs<int32_t>();
  const auto min_a_val = result.GetValue(agg_schema, agg_schema->GetColIdx("minA")).GetAs<int32_t>();
  const auto max_a_val = result.GetValue(agg_schema, agg_schema->GetColIdx("maxA")).GetAs<int32_t>();

  // Should count all tuples
  ASSERT_EQ(count_a_val, TEST1_SIZE);

  // Should sum from 0 to TEST1_SIZE
  ASSERT_EQ(sum_a_val, TEST1_SIZE * (TEST1_SIZE - 1) / 2);

  // Minimum should be 0
  ASSERT_EQ(min_a_val, 0);

  // Maximum should be TEST1_SIZE - 1
  ASSERT_EQ(max_a_val, TEST1_SIZE - 1);
}

// SELECT COUNT(colA), colB, SUM(colC) FROM test_1 GROUP BY colB HAVING COUNT(colA) > 100;
TEST_F(GradingExecutorTest, AggregationIntegrated2) {
  const Schema *scan_schema;
  std::unique_ptr<AbstractPlanNode> scan_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto col_c = MakeColumnValueExpression(schema, 0, "colC");
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_);
  }

  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_a = MakeColumnValueExpression(*scan_schema, 0, "colA");
    const AbstractExpression *col_b = MakeColumnValueExpression(*scan_schema, 0, "colB");
    const AbstractExpression *col_c = MakeColumnValueExpression(*scan_schema, 0, "colC");

    // Make GROUP BY
    std::vector<const AbstractExpression *> group_by_cols{col_b};
    const AbstractExpression *groupby_b = MakeAggregateValueExpression(true, 0);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{col_a, col_c};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate, AggregationType::SumAggregate};
    const AbstractExpression *count_a = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sum_c = MakeAggregateValueExpression(false, 1);

    // Make HAVING clause
    const AbstractExpression *having = MakeComparisonExpression(
        count_a, MakeConstantValueExpression(ValueFactory::GetIntegerValue(100)), ComparisonType::GreaterThan);

    // Create plan
    agg_schema = MakeOutputSchema({{"countA", count_a}, {"colB", groupby_b}, {"sumC", sum_c}});
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, scan_plan.get(), having, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  std::unordered_set<int32_t> encountered{};
  for (const auto &tuple : result_set) {
    // Should have countA > 100
    ASSERT_GT(tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>(), 100);

    // Should have unique colBs.
    auto col_b = tuple.GetValue(agg_schema, agg_schema->GetColIdx("colB")).GetAs<int32_t>();
    ASSERT_EQ(encountered.count(col_b), 0);
    encountered.insert(col_b);

    // Sanity check: ColB should also be within [0, 10).
    ASSERT_GE(col_b, 0);
    ASSERT_LT(col_b, 10);
  }
}

}  // namespace bustub
