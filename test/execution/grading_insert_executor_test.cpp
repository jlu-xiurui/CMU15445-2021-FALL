//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_insert_executor_test.cpp
//
// Identification: test/execution/insert_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include "executor_test_util.h"  // NOLINT
#include "storage/table/tuple.h"
#include "test_util.h"  // NOLINT

// TODO(Kyle): Found a strange phenomenon where index updates
// completely fail when we use INTEGER keys but work fine
// when BIGINT keys are used...

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

// INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)
TEST_F(GradingExecutorTest, RawInsert1) {
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  std::vector<Tuple> result_set{};

  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  GetExecutionEngine()->Execute(&insert_plan, &result_set, GetTxn(), GetExecutorContext());

  // InsertExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);

  // Iterate through table make sure that values were inserted.

  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Size
  ASSERT_EQ(result_set.size(), 3);

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 10);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 101);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 11);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 102);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 12);
}

// INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
TEST_F(GradingExecutorTest, RawInsert2) {
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  auto key_schema = ParseCreateStatement("a bigint");
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "empty_table2", table_info->schema_, *key_schema, {0}, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  // Execute the insert
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&insert_plan, nullptr, GetTxn(), GetExecutorContext());
  // Insert should not modify the result set
  ASSERT_EQ(0, result_set.size());

  // Iterate through table make sure that values were inserted

  result_set.clear();

  // SELECT * FROM empty_table2;
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Size
  ASSERT_EQ(3, result_set.size());

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // Get RID from index, fetch tuple and then compare
  std::vector<RID> rids{};
  for (size_t i = 0; i < result_set.size(); ++i) {
    // Scan the index for the RID(s) associated with the tuple
    index_info->index_->ScanKey(result_set[i], &rids, GetTxn());

    // Fetch the tuple from the table
    Tuple indexed_tuple;
    ASSERT_TRUE(table_info->table_->GetTuple(rids[i], &indexed_tuple, GetTxn()));

    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>());
  }
}

// INSERT INTO empty_table2 SELECT colA, colB FROM test_1
TEST_F(GradingExecutorTest, SelectInsertSequentialScan1) {
  std::vector<Tuple> result_set{};

  // Construct a sequential scan of test_1
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct an insertion plan
  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }

  // Insert into empty_table2 from sequential scan of test_1
  GetExecutionEngine()->Execute(insert_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // InsertExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);

  // Construct a sequential scan of empty_table2 (no longer empty)
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Now iterate through both tables, and make sure they have the same data
  std::vector<Tuple> result_set1{};
  std::vector<Tuple> result_set2{};
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set1, GetTxn(), GetExecutorContext());
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set2, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set1.size(), TEST1_SIZE);
  ASSERT_EQ(result_set2.size(), TEST1_SIZE);
  ASSERT_EQ(result_set1.size(), result_set2.size());

  for (size_t i = 0; i < result_set1.size(); ++i) {
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colB")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>());
  }
  ASSERT_EQ(result_set1.size(), 1000);
}

// INSERT INTO empty_table2 SELECT colA, colB FROM test_1 WHERE colA < 500
TEST_F(GradingExecutorTest, SelectInsertSequentialScan2) {
  std::vector<Tuple> result_set{};

  // Construct a sequential scan of test_1
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));
    auto predicate = MakeComparisonExpression(col_a, const500, ComparisonType::LessThan);
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  }

  // Construct an insertion plan
  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }

  // Insert into empty_table2 from sequential scan of test_1
  GetExecutionEngine()->Execute(insert_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // InsertExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);

  // Construct a sequential scan of empty_table2 (no longer empty)
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Now iterate through both tables, and make sure they have the same data
  std::vector<Tuple> result_set1;
  std::vector<Tuple> result_set2;
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set1, GetTxn(), GetExecutorContext());
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set2, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set1.size(), result_set2.size());
  for (size_t i = 0; i < result_set1.size(); ++i) {
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colB")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>());
  }
  ASSERT_EQ(result_set1.size(), 500);
}

// INSERT INTO empty_table3 SELECT colA, colB FROM test_4 WHERE colA >= 50
TEST_F(GradingExecutorTest, SelectInsertSequentialScan3) {
  const Schema *out_schema1;
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
    auto predicate = MakeComparisonExpression(col_a, const50, ComparisonType::GreaterThanOrEqual);
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, predicate, table_info->oid_);
  }

  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }

  // Construct an index on the target table
  auto key_schema = ParseCreateStatement("a bigint");
  const std::vector<std::uint32_t> key_attrs{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "empty_table3", GetExecutorContext()->GetCatalog()->GetTable("empty_table3")->schema_,
          *key_schema, key_attrs, BIGINT_SIZE, BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  // Execute the insert
  GetExecutionEngine()->Execute(insert_plan.get(), nullptr, GetTxn(), GetExecutorContext());

  // Now iterate through both tables, and make sure they have the same data
  const Schema *out_schema2;
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  std::vector<Tuple> result_set1{};
  std::vector<Tuple> result_set2{};
  GetExecutionEngine()->Execute(scan_plan1.get(), &result_set1, GetTxn(), GetExecutorContext());
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set2, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set1.size(), 50);
  ASSERT_EQ(result_set1.size(), result_set2.size());
  for (std::size_t i = 0; i < result_set1.size(); ++i) {
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int32_t>());
    ASSERT_EQ(result_set1[i].GetValue(out_schema1, out_schema1->GetColIdx("colB")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>());
  }

  // Ensure that the index is updated for the target table
  std::vector<RID> rids{};
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
  for (std::size_t i = 0; i < result_set2.size(); ++i) {
    // Construct the index key
    const Tuple key = result_set2[i].KeyFromTuple(table_info->schema_, *key_schema, key_attrs);

    // Scan the index for the key
    index_info->index_->ScanKey(key, &rids, GetTxn());
    ASSERT_EQ(i + 1, rids.size());

    // Get the same tuple from the table
    Tuple indexed_tuple;
    ASSERT_TRUE(table_info->table_->GetTuple(rids[i], &indexed_tuple, GetTxn()));

    // Compare
    ASSERT_EQ(indexed_tuple.GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int64_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colA")).GetAs<int64_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>(),
              result_set2[i].GetValue(out_schema2, out_schema2->GetColIdx("colB")).GetAs<int32_t>());
  }
}

// INSERT INTO empty_table3 VALUES (100, 10), (101, 11), (102, 12)
TEST_F(GradingExecutorTest, RawInsertWithIndex1) {
  std::vector<Tuple> result_set{};

  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetBigIntValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val2{ValueFactory::GetBigIntValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val3{ValueFactory::GetBigIntValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  // Create an index on the target table
  auto key_schema = std::unique_ptr<Schema>{ParseCreateStatement("a bigint")};
  auto key_attrs = std::vector<uint32_t>{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "empty_table3", table_info->schema_, *(key_schema.get()), key_attrs, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);
  auto *index = index_info->index_.get();

  // Execute the insertion plan
  GetExecutionEngine()->Execute(&insert_plan, &result_set, GetTxn(), GetExecutorContext());

  // InsertExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Iterate through table make sure that values were inserted

  // Create sequential scan plan
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  // Execute sequential scan
  GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 3);

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), 100);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 10);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), 101);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 11);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), 102);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 12);

  std::vector<RID> rids{};

  // Get RID from index, fetch tuple and then compare
  for (size_t i = 0; i < result_set.size(); ++i) {
    auto index_key =
        result_set[i].KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

    index->ScanKey(index_key, &rids, GetTxn());
    ASSERT_EQ(rids.size(), i + 1);

    Tuple indexed_tuple;
    ASSERT_TRUE(table_info->table_->GetTuple(rids[i], &indexed_tuple, GetTxn()));

    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>());
  }
}

// INSERT INTO empty_table3 (SELECT colA, colB FROM test_4)
TEST_F(GradingExecutorTest, SequentialInsertWithIndex) {
  std::vector<Tuple> result_set{};

  // Construct sequential scan plan
  const Schema *out_schema1{};
  std::unique_ptr<SeqScanPlanNode> scan_plan1{};
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct an insertion plan
  std::unique_ptr<InsertPlanNode> insert_plan{};
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }

  // Create an index on the target table
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
  auto key_schema = std::unique_ptr<Schema>{ParseCreateStatement("a bigint")};
  auto key_attrs = std::vector<uint32_t>{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "empty_table3", table_info->schema_, *key_schema, key_attrs, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  // Execute the insertion plan
  GetExecutionEngine()->Execute(insert_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // InsertExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Iterate through table make sure that values were inserted

  // Create sequential scan plan
  const Schema *out_schema2;
  std::unique_ptr<SeqScanPlanNode> scan_plan2{};
  {
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Execute sequential scan
  GetExecutionEngine()->Execute(scan_plan2.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), TEST4_SIZE);

  // Ensure the index contains all inserted tuples
  auto &table_schema = table_info->schema_;
  auto *index = index_info->index_.get();
  for (auto &tuple : result_set) {
    std::vector<RID> scanned{};
    const Tuple key = tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
    index->ScanKey(key, &scanned, GetTxn());
    ASSERT_EQ(1, scanned.size());
  }
}

}  // namespace bustub
