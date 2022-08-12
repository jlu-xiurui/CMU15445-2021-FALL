//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_executor_integrated_test.cpp
//
// Identification: test/execution/grading_executor_integrated_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "executor_test_util.h"  // NOLINT
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

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

// Scan -> Nested Loop Join -> Aggregation
TEST_F(GradingExecutorTest, Integrated1) {
  const Schema *table1_schema;
  std::unique_ptr<AbstractPlanNode> table1_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    table1_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    table1_scan = std::make_unique<SeqScanPlanNode>(table1_schema, nullptr, table_info->oid_);
  }

  const Schema *table3_schema;
  std::unique_ptr<AbstractPlanNode> table3_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    table3_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    table3_scan = std::make_unique<SeqScanPlanNode>(table3_schema, nullptr, table_info->oid_);
  }

  const Schema *join_schema;
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan;
  {
    // colA and colB have a tuple index of 0 because they are the left side of the join
    auto table1_cola = MakeColumnValueExpression(*table1_schema, 0, "colA");
    auto table1_colb = MakeColumnValueExpression(*table1_schema, 0, "colB");

    // col1 and col2 have a tuple index of 1 because they are the right side of the join
    auto table3_cola = MakeColumnValueExpression(*table3_schema, 1, "colA");
    auto table3_colb = MakeColumnValueExpression(*table3_schema, 1, "colB");

    auto predicate = MakeComparisonExpression(table1_cola, table3_cola, ComparisonType::Equal);
    join_schema = MakeOutputSchema({{"table1_colA", table1_cola},
                                    {"table1_colB", table1_colb},
                                    {"table3_colA", table3_cola},
                                    {"table3_colB", table3_colb}});
    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        join_schema, std::vector<const AbstractPlanNode *>{table1_scan.get(), table3_scan.get()}, predicate);
  }

  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *col_a = MakeColumnValueExpression(*join_schema, 0, "table1_colA");
    const AbstractExpression *count_a = MakeAggregateValueExpression(false, 0);
    const AbstractExpression *sum_a = MakeAggregateValueExpression(false, 1);
    const AbstractExpression *min_a = MakeAggregateValueExpression(false, 2);
    const AbstractExpression *max_a = MakeAggregateValueExpression(false, 3);

    agg_schema = MakeOutputSchema({{"countA", count_a}, {"sumA", sum_a}, {"minA", min_a}, {"maxA", max_a}});
    agg_plan = std::make_unique<AggregationPlanNode>(
        agg_schema, join_plan.get(), nullptr, std::vector<const AbstractExpression *>{},
        std::vector<const AbstractExpression *>{col_a, col_a, col_a, col_a},
        std::vector<AggregationType>{AggregationType::CountAggregate, AggregationType::SumAggregate,
                                     AggregationType::MinAggregate, AggregationType::MaxAggregate});
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(agg_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set.size(), 1);
  const Tuple tuple = result_set.front();

  auto count_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>();
  auto sum_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("sumA")).GetAs<int32_t>();
  auto min_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("minA")).GetAs<int32_t>();
  auto max_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("maxA")).GetAs<int32_t>();

  // Should count all tuples
  ASSERT_EQ(count_a_val, TEST3_SIZE);

  // Should sum from 0 to TEST3_SIZE
  ASSERT_EQ(sum_a_val, TEST3_SIZE * (TEST3_SIZE - 1) / 2);

  // Minimum should be 0
  ASSERT_EQ(min_a_val, 0);

  // Maximum should be TEST3_SIZE - 1
  ASSERT_EQ(max_a_val, TEST3_SIZE - 1);
}

// Insert -> Update -> Scan -> Hash Join
TEST_F(GradingExecutorTest, Integrated2) {
  // SELECT colA, colB FROM test_6;
  const Schema *table6_schema;
  std::unique_ptr<AbstractPlanNode> table6_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_6");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    table6_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    table6_scan = std::make_unique<SeqScanPlanNode>(table6_schema, nullptr, table_info->oid_);
  }

  // SELECT colA, colB FROM test_7;
  const Schema *table7_schema;
  std::unique_ptr<AbstractPlanNode> table7_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    table7_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    table7_scan = std::make_unique<SeqScanPlanNode>(table7_schema, nullptr, table_info->oid_);
  }

  // SELECT colA, colB from empty_table3;
  const Schema *empty_table_schema;
  std::unique_ptr<AbstractPlanNode> empty_table_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    empty_table_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    empty_table_scan = std::make_unique<SeqScanPlanNode>(empty_table_schema, nullptr, table_info->oid_);
  }

  // INSERT INTO empty_table3 SELECT colA, colB FROM table6;
  std::unique_ptr<InsertPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    insert_plan = std::make_unique<InsertPlanNode>(table6_scan.get(), table_info->oid_);
  }

  std::vector<Tuple> results{};

  // Execute the INSERT INTO ... SELECT
  GetExecutionEngine()->Execute(insert_plan.get(), &results, GetTxn(), GetExecutorContext());
  ASSERT_TRUE(results.empty());

  // UPDATE
  std::unique_ptr<AbstractPlanNode> update_plan;
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{};
  update_attrs.emplace(static_cast<uint32_t>(0), UpdateInfo{UpdateType::Add, 1});
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    update_plan = std::make_unique<UpdatePlanNode>(empty_table_scan.get(), table_info->oid_, update_attrs);
  }

  // Execute the update
  GetExecutionEngine()->Execute(update_plan.get(), &results, GetTxn(), GetExecutorContext());
  ASSERT_TRUE(results.empty());

  // SELECT empty_table3.colA, empty_table3.colB, table7.colA, table7.colB FROM empty_table3, test_7 WHERE
  // empty_table3.colA = test_7.colA;
  const Schema *join_schema;
  std::unique_ptr<HashJoinPlanNode> join_plan;
  {
    auto empty_cola = MakeColumnValueExpression(*empty_table_schema, 0, "colA");
    auto empty_colb = MakeColumnValueExpression(*empty_table_schema, 0, "colB");

    auto table7_cola = MakeColumnValueExpression(*table7_schema, 1, "colA");
    auto table7_colb = MakeColumnValueExpression(*table7_schema, 1, "colB");

    join_schema = MakeOutputSchema({{"empty_colA", empty_cola},
                                    {"empty_colB", empty_colb},
                                    {"table7_colA", table7_cola},
                                    {"table7_colB", table7_colb}});
    join_plan = std::make_unique<HashJoinPlanNode>(
        join_schema, std::vector<const AbstractPlanNode *>{empty_table_scan.get(), table7_scan.get()}, empty_cola,
        table7_cola);
  }

  GetExecutionEngine()->Execute(join_plan.get(), &results, GetTxn(), GetExecutorContext());

  ASSERT_EQ(TEST7_SIZE - 1, results.size());
  for (const auto &tuple : results) {
    const auto empty_cola = tuple.GetValue(join_schema, join_schema->GetColIdx("empty_colA")).GetAs<int64_t>();
    const auto empty_colb = tuple.GetValue(join_schema, join_schema->GetColIdx("empty_colB")).GetAs<int32_t>();
    const auto table7_cola = tuple.GetValue(join_schema, join_schema->GetColIdx("table7_colA")).GetAs<int64_t>();
    const auto table7_colb = tuple.GetValue(join_schema, join_schema->GetColIdx("table7_colB")).GetAs<int32_t>();

    ASSERT_EQ(empty_cola, table7_cola);
    ASSERT_EQ(empty_colb + 1, table7_colb);
  }
}

}  // namespace bustub
