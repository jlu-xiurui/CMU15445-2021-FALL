//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_delete_executor_test.cpp
//
// Identification: test/execution/grading_delete_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"
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

TEST_F(GradingExecutorTest, DeleteEntireTable) {
  // Construct a sequential scan of the table
  const Schema *out_schema{};
  std::unique_ptr<AbstractPlanNode> scan_plan{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);
  }

  // Construct a deletion plan
  std::unique_ptr<AbstractPlanNode> delete_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    delete_plan = std::make_unique<DeletePlanNode>(scan_plan.get(), table_info->oid_);
  }

  std::vector<Tuple> result_set{};

  // Execute an initial sequential scan, ensure all expected tuples are present
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify all the tuples are present
  ASSERT_EQ(result_set.size(), TEST1_SIZE);
  result_set.clear();

  // Execute deletion of all tuples in the table
  GetExecutionEngine()->Execute(delete_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // DeleteExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Execute another sequential scan; no tuples should be present in the table
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 0);
}

TEST_F(GradingExecutorTest, DeleteEntireTableWithIndex) {
  // Construct a sequential scan of the table
  const Schema *out_schema{};
  std::unique_ptr<AbstractPlanNode> scan_plan{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);
  }

  // Construct a deletion plan
  std::unique_ptr<AbstractPlanNode> delete_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    delete_plan = std::make_unique<DeletePlanNode>(scan_plan.get(), table_info->oid_);
  }

  std::vector<Tuple> result_set{};

  // Execute an initial sequential scan, ensure all expected tuples are present
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  ASSERT_EQ(result_set.size(), TEST4_SIZE);

  // Construct an index on the populated table
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
  auto key_schema = std::unique_ptr<Schema>{ParseCreateStatement("a bigint")};
  auto key_attrs = std::vector<uint32_t>{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "test_4", table_info->schema_, *key_schema, key_attrs, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  // Ensure that all tuples are present in the index
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &table_schema = table_info->schema_;
    auto *index = index_info->index_.get();
    for (auto &tuple : result_set) {
      std::vector<RID> scanned{};
      const Tuple key = tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
      index->ScanKey(key, &scanned, GetTxn());
      ASSERT_EQ(1, scanned.size());
    }
  }

  // Execute deletion of all tuples in the table
  std::vector<Tuple> delete_result_set{};
  GetExecutionEngine()->Execute(delete_plan.get(), &delete_result_set, GetTxn(), GetExecutorContext());

  // DeleteExecutor should not modify the result set
  ASSERT_EQ(delete_result_set.size(), 0);
  delete_result_set.clear();

  // Execute another sequential scan; no tuples should be present in the table
  GetExecutionEngine()->Execute(scan_plan.get(), &delete_result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(delete_result_set.size(), 0);

  // The index for the table should now be empty
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &table_schema = table_info->schema_;
    auto *index = index_info->index_.get();
    // Iterate over the original result set which contains original tuples from table
    for (auto &tuple : result_set) {
      std::vector<RID> scanned{};
      const Tuple key = tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
      index->ScanKey(key, &scanned, GetTxn());
      ASSERT_TRUE(scanned.empty());
    }
  }
}

// Delete a single tuple from a table
TEST_F(GradingExecutorTest, DeleteSingleTuple) {
  // Construct a scan of the table that selects only a single tuple
  const Schema *scan_schema;
  std::unique_ptr<SeqScanPlanNode> scan_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
    auto *const0 = MakeConstantValueExpression(ValueFactory::GetBigIntValue(0));
    auto *predicate = MakeComparisonExpression(col_a, const0, ComparisonType::Equal);
    scan_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(scan_schema, predicate, table_info->oid_);
  }

  // Construct the delete plan
  std::unique_ptr<DeletePlanNode> delete_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    delete_plan = std::make_unique<DeletePlanNode>(scan_plan.get(), table_info->oid_);
  }

  // Construct an index on the target table
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
  auto key_schema = std::unique_ptr<Schema>{ParseCreateStatement("a bigint")};
  auto key_attrs = std::vector<uint32_t>{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "test_4", table_info->schema_, *key_schema, key_attrs, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  auto delete_txn = std::unique_ptr<Transaction>{GetTxnManager()->Begin()};
  auto delete_exec_ctx =
      std::make_unique<ExecutorContext>(delete_txn.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  // Execute the delete plan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(delete_plan.get(), &result_set, delete_txn.get(), delete_exec_ctx.get());
  ASSERT_EQ(result_set.size(), 0);

  GetTxnManager()->Commit(delete_txn.get());
  result_set.clear();

  // Now perform a full table scan
  std::unique_ptr<SeqScanPlanNode> full_scan;
  { full_scan = std::make_unique<SeqScanPlanNode>(scan_schema, nullptr, table_info->oid_); }

  GetExecutionEngine()->Execute(full_scan.get(), &result_set, GetTxn(), GetExecutorContext());
  EXPECT_EQ(result_set.size(), TEST4_SIZE - 1);

  // The deleted value should not be present in the result set
  for (const auto &tuple : result_set) {
    EXPECT_NE(tuple.GetValue(scan_schema, scan_schema->GetColIdx("colA")).GetAs<int64_t>(), static_cast<int64_t>(0));
  }

  // Ensure the deleted value is not present in the index
  auto *table = table_info->table_.get();
  auto &table_schema = table_info->schema_;
  auto *index = index_info->index_.get();
  // Iterate over the original result set which contains original tuples from table
  for (auto &tuple : result_set) {
    std::vector<RID> scanned{};
    const Tuple key = tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
    index->ScanKey(key, &scanned, GetTxn());
    ASSERT_EQ(1, scanned.size());

    Tuple queried{};
    table->GetTuple(scanned.front(), &queried, GetTxn());
    EXPECT_NE(tuple.GetValue(scan_schema, scan_schema->GetColIdx("colA")).GetAs<int64_t>(), static_cast<int64_t>(0));
  }
}

TEST_F(GradingExecutorTest, DeleteIntegrated) {
  // SELECT colA FROM test_1 WHERE colA < 50
  const Schema *output_schema;
  std::unique_ptr<SeqScanPlanNode> scan_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto const50 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(50));
    auto predicate = MakeComparisonExpression(col_a, const50, ComparisonType::LessThan);
    output_schema = MakeOutputSchema({{"colA", col_a}});
    scan_plan = std::make_unique<SeqScanPlanNode>(output_schema, predicate, table_info->oid_);
  }

  // Execute scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Result set size
  ASSERT_EQ(result_set.size(), 50);

  // Verify tuple contents
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(output_schema, output_schema->GetColIdx("colA")).GetAs<int32_t>() < 50);
  }

  result_set.clear();

  // DELETE FROM test_1 WHERE colA < 50
  std::unique_ptr<DeletePlanNode> delete_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
    delete_plan = std::make_unique<DeletePlanNode>(scan_plan.get(), table_info->oid_);
  }

  GetExecutionEngine()->Execute(delete_plan.get(), nullptr, GetTxn(), GetExecutorContext());

  // Delete should not modify the result set
  ASSERT_TRUE(result_set.empty());

  // Execute the scan again
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_TRUE(result_set.empty());
}

}  // namespace bustub
