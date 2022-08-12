//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_nested_loop_join_executor_test.cpp
//
// Identification: test/execution/grading_nested_loop_join_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/plans/mock_scan_plan.h"
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

// SELECT test_4.colA, test_4.colB, test_6.colA, test_6.colB FROM test_4 JOIN test_6 ON test_4.colA = test_6.colA
TEST_F(GradingExecutorTest, NestedLoopJoin) {
  // Construct sequential scan of table test_4
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_6
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_6");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema{};
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    // Columns from Table 4 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table4_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table4_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");

    // Columns from Table 6 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table6_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table6_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");

    auto predicate = MakeComparisonExpression(table4_col_a, table6_col_a, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table4_colA", table4_col_a},
                                   {"table4_colB", table4_col_b},
                                   {"table6_colA", table6_col_a},
                                   {"table6_colB", table6_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 100);

  for (const auto &tuple : result_set) {
    const auto t4_col_a = tuple.GetValue(out_schema, out_schema->GetColIdx("table4_colA")).GetAs<int64_t>();
    const auto t4_col_b = tuple.GetValue(out_schema, out_schema->GetColIdx("table4_colB")).GetAs<int32_t>();
    const auto t6_col_a = tuple.GetValue(out_schema, out_schema->GetColIdx("table6_colA")).GetAs<int64_t>();
    const auto t6_col_b = tuple.GetValue(out_schema, out_schema->GetColIdx("table6_colB")).GetAs<int32_t>();

    // Join keys should be equiavlent
    ASSERT_EQ(t4_col_a, t6_col_a);

    // In case of Table 4 and Table 6, corresponding columns also equal
    ASSERT_LT(t4_col_b, TEST4_SIZE);
    ASSERT_LT(t6_col_b, TEST6_SIZE);
    ASSERT_EQ(t4_col_b, t6_col_b);
  }
}

// SELECT test_5.colA, test_5.colB, test_4.colA, test_4.colB FROM test_5 JOIN test_4 ON test_5.colA = test_4.colA
TEST_F(GradingExecutorTest, NestedLoopJoinEmptyOuterTable) {
  // Construct sequential scan of table test_5
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_5");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_4
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema{};
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    // Columns from Table 4 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table5_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table5_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");

    // Columns from Table 6 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table4_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table4_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");

    auto *predicate = MakeComparisonExpression(table5_col_a, table4_col_a, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table5_colA", table5_col_a},
                                   {"table5_colB", table5_col_b},
                                   {"table4_colA", table4_col_a},
                                   {"table4_colB", table4_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Result set should be empty
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT test_4.colA, test_4.colB, test_5.colA, test_5.colB FROM test_4 JOIN test_5 ON test_4.colA = test_5.colA
TEST_F(GradingExecutorTest, NestedLoopJoinEmptyInnerTable) {
  // Construct sequential scan of table test_4
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_5
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_5");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema{};
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    // Columns from Table 4 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table4_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table4_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");

    // Columns from Table 6 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table5_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table5_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");

    auto *predicate = MakeComparisonExpression(table4_col_a, table5_col_a, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table4_colA", table4_col_a},
                                   {"table4_colB", table4_col_b},
                                   {"table5_colA", table5_col_a},
                                   {"table5_colB", table5_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Result set should be empty
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT test_7.colA, test_7.colB, test_8.colA, test_8.colB FROM test_7 JOIN test_8 ON test_7.colC = test_8.colB
TEST_F(GradingExecutorTest, NestedLoopJoinOuterTableDuplicateJoinKeys) {
  // Construct sequential scan of table test_7
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_8
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_8");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema{};
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    // Columns from Table 7 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table7_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table7_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");
    auto *table7_col_c = MakeColumnValueExpression(*out_schema1, 0, "colC");

    // Columns from Table 8 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table8_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table8_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");

    auto *predicate = MakeComparisonExpression(table7_col_c, table8_col_b, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table7_colA", table7_col_a},
                                   {"table7_colB", table7_col_b},
                                   {"table8_colA", table8_col_a},
                                   {"table8_colB", table8_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Table 7 contains 100 tuples, partitioned into 10 groups of
  // 10 that share a join key (colC); Table 8 contains 10 tuples,
  // with values for colB from 0 .. 9; for each outer tuple, we
  // should find exactly one inner tuple to match

  // Result set should be empty
  ASSERT_EQ(result_set.size(), TEST7_SIZE);
}

// SELECT test_8.colA, test_8.colB, test_7.colA, test_7.colB FROM test_8 JOIN test_7 ON test_8.colB = test_7.colC
TEST_F(GradingExecutorTest, NestedLoopJoinInnerTableDuplicateJoinKeys) {
  // Construct sequential scan of table test_8
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_8");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_7
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema{};
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    // Columns from Table 8 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table8_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table8_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");

    // Columns from Table 7 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table7_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table7_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");
    auto *table7_col_c = MakeColumnValueExpression(*out_schema2, 1, "colC");

    auto *predicate = MakeComparisonExpression(table8_col_b, table7_col_c, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table8_colA", table8_col_a},
                                   {"table8_colB", table8_col_b},
                                   {"table7_colA", table7_col_a},
                                   {"table7_colB", table7_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Table 7 contains 100 tuples, partitioned into 10 groups of
  // 10 that share a join key (colC); Table 8 contains 10 tuples,
  // with values for colB from 0 .. 9; for each outer tuple, we
  // should find exactly one inner tuple to match

  // Result set should be empty
  ASSERT_EQ(result_set.size(), TEST7_SIZE);
}

// SELECT test_1.colA, test_1.colB, test_3.colA, test_3.colA FROM test_1 JOIN test_3 ON test_1.colA = test_3.colA;
TEST_F(GradingExecutorTest, NestedLoopJoinIntegrated) {
  // SELECT colA, colB FROM test_1 WHERE colA < 50;
  const Schema *table1_schema;
  std::unique_ptr<AbstractPlanNode> table1_scan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
    auto predicate = MakeComparisonExpression(col_a, const50, ComparisonType::LessThan);
    table1_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    table1_scan = std::make_unique<SeqScanPlanNode>(table1_schema, predicate, table_info->oid_);
  }

  // SELECT colA, colB from test_3;
  const Schema *table3_schema;
  std::unique_ptr<AbstractPlanNode> table3_scan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
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

    // colA and colB have a tuple index of 1 because they are the right side of the join
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

  // Execute the JOIN
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 50);

  for (const auto &tuple : result_set) {
    const auto table1_cola = tuple.GetValue(join_schema, join_schema->GetColIdx("table1_colA")).GetAs<int32_t>();
    const auto table3_cola = tuple.GetValue(join_schema, join_schema->GetColIdx("table3_colA")).GetAs<int32_t>();
    ASSERT_EQ(table1_cola, table3_cola);
    ASSERT_LT(table1_cola, 50);
  }
}

TEST_F(GradingExecutorTest, NestedLoopJoinIOCost) {
  // The sizes of the individual mock tables
  const std::size_t scan0_size = 10;
  const std::size_t scan1_size = 10;

  // All all of the tuples have the same value
  const std::size_t expected_join_size = scan0_size * scan1_size;

  std::unique_ptr<Schema> scan0_schema;
  std::unique_ptr<MockScanPlanNode> scan0;
  {
    std::vector<Column> columns{Column{"colA", TypeId::INTEGER}, {Column{"colB", TypeId::INTEGER}}};
    scan0_schema = std::make_unique<Schema>(columns);
    scan0 = std::make_unique<MockScanPlanNode>(scan0_schema.get(), scan0_size);
  }

  std::unique_ptr<Schema> scan1_schema;
  std::unique_ptr<MockScanPlanNode> scan1;
  {
    std::vector<Column> columns{Column{"colA", TypeId::INTEGER}, {Column{"colB", TypeId::INTEGER}}};
    scan1_schema = std::make_unique<Schema>(columns);
    scan1 = std::make_unique<MockScanPlanNode>(scan1_schema.get(), scan1_size);
  }

  // Construct the join plan
  const Schema *out_schema;
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan{};
  {
    auto *table0_col_a = MakeColumnValueExpression(*scan0_schema, 0, "colA");
    auto *table0_col_b = MakeColumnValueExpression(*scan0_schema, 0, "colB");

    auto *table1_col_a = MakeColumnValueExpression(*scan1_schema, 1, "colA");
    auto *table1_col_b = MakeColumnValueExpression(*scan1_schema, 1, "colB");

    auto predicate = MakeComparisonExpression(table0_col_a, table1_col_a, ComparisonType::Equal);

    out_schema = MakeOutputSchema({{"table0_colA", table0_col_a},
                                   {"table0_colB", table0_col_b},
                                   {"table1_colA", table1_col_a},
                                   {"table1_colB", table1_col_b}});

    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_schema, std::vector<const AbstractPlanNode *>{scan0.get(), scan1.get()}, predicate);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  ASSERT_EQ(expected_join_size, result_set.size());

  // Scan 0 should only be polled once per tuple
  ASSERT_EQ(scan0_size, scan0->PollCount());
  // Scan 1 should be polled SCAN1_SIZE for each tuple in the outer table
  ASSERT_EQ(scan0_size * scan1_size, scan1->PollCount());
}

}  // namespace bustub
