//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_update_executor_test.cpp
//
// Identification: test/execution/grading_update_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include "execution/executors/update_executor.h"
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

// Addition update
TEST_F(GradingExecutorTest, UpdateTableAdd) {
  // Construct a sequential scan of the table
  std::unique_ptr<AbstractPlanNode> scan_plan{};
  const Schema *out_schema{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);
  }

  // Construct an update plan
  std::unique_ptr<AbstractPlanNode> update_plan{};
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{};
  update_attrs.emplace(static_cast<uint32_t>(1), UpdateInfo{UpdateType::Add, 1});
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    update_plan = std::make_unique<UpdatePlanNode>(scan_plan.get(), table_info->oid_, update_attrs);
  }

  std::vector<Tuple> result_set{};

  // Execute an initial sequential scan, ensure all expected tuples are present
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
  }

  result_set.clear();

  // Execute update for all tuples in the table
  GetExecutionEngine()->Execute(update_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // UpdateExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Execute another sequential scan; no tuples should be present in the table
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results after update
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i + 1));
  }
}

// Set update
TEST_F(GradingExecutorTest, UpdateTableSet) {
  // Construct a sequential scan of the table
  std::unique_ptr<AbstractPlanNode> scan_plan{};
  const Schema *out_schema{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);
  }

  // Construct an update plan
  std::unique_ptr<AbstractPlanNode> update_plan{};
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{};
  update_attrs.emplace(static_cast<uint32_t>(1), UpdateInfo{UpdateType::Set, 1337});
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
    update_plan = std::make_unique<UpdatePlanNode>(scan_plan.get(), table_info->oid_, update_attrs);
  }

  std::vector<Tuple> result_set{};

  // Execute an initial sequential scan, ensure all expected tuples are present
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
  }

  result_set.clear();

  // Execute update for all tuples in the table
  GetExecutionEngine()->Execute(update_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // UpdateExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Execute another sequential scan; no tuples should be present in the table
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results after update
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(1337));
  }
}

// Set update
TEST_F(GradingExecutorTest, UpdateTableSetWithIndex) {
  // Construct a sequential scan of the table
  const Schema *out_schema{};
  std::unique_ptr<SeqScanPlanNode> scan_plan{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    auto col_c = MakeColumnValueExpression(schema, 0, "colC");
    out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colC", col_c}});
    scan_plan = std::make_unique<SeqScanPlanNode>(out_schema, nullptr, table_info->oid_);
  }

  // Construct an update plan (set colB = 1337)
  std::unique_ptr<UpdatePlanNode> update_plan{};
  std::unordered_map<uint32_t, UpdateInfo> update_attrs{};
  update_attrs.emplace(static_cast<uint32_t>(1), UpdateInfo{UpdateType::Set, 1337});
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    update_plan = std::make_unique<UpdatePlanNode>(scan_plan.get(), table_info->oid_, update_attrs);
  }

  // Construct an index on the target table
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
  auto key_schema = std::unique_ptr<Schema>{ParseCreateStatement("a bigint")};
  auto key_attrs = std::vector<uint32_t>{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "test_4", table_info->schema_, *(key_schema.get()), key_attrs, BIGINT_SIZE,
          BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  std::vector<Tuple> result_set{};

  // Execute an initial sequential scan, ensure all expected tuples are present
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), static_cast<int64_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_LT(tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>(), static_cast<int32_t>(10));
  }

  result_set.clear();

  // Ensure the index contains the expected records
  auto &table_schema = table_info->schema_;
  auto *index = index_info->index_.get();
  for (auto &tuple : result_set) {
    std::vector<RID> scanned{};
    const Tuple key = tuple.KeyFromTuple(table_schema, *key_schema, key_attrs);
    index->ScanKey(key, &scanned, GetTxn());
    ASSERT_EQ(1, scanned.size());
  }

  // Execute update for all tuples in the table
  GetExecutionEngine()->Execute(update_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // UpdateExecutor should not modify the result set
  ASSERT_EQ(result_set.size(), 0);
  result_set.clear();

  // Execute another sequential scan
  GetExecutionEngine()->Execute(scan_plan.get(), &result_set, GetTxn(), GetExecutorContext());

  // Verify results after update
  ASSERT_EQ(result_set.size(), TEST3_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), static_cast<int64_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(1337));
    ASSERT_LT(tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>(), static_cast<int32_t>(10));
  }

  // Get RID from index, fetch tuple and then compare
  std::vector<RID> rids{};

  auto *table = table_info->table_.get();
  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto index_key = result_set[i].KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());

    index->ScanKey(index_key, &rids, GetTxn());
    ASSERT_EQ(rids.size(), i + 1);

    Tuple indexed_tuple{};
    ASSERT_TRUE(table->GetTuple(rids[i], &indexed_tuple, GetTxn()));

    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>());
    ASSERT_EQ(indexed_tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>(),
              result_set[i].GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>());
  }
}

TEST_F(GradingExecutorTest, UpdateIntegrated) {
  // INSERT INTO empty_table3 SELECT colA, colA FROM test_4;
  const Schema *out_schema1;
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colA", col_a}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  std::unique_ptr<AbstractPlanNode> insert_plan;
  {
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    insert_plan = std::make_unique<InsertPlanNode>(scan_plan1.get(), table_info->oid_);
  }

  // Execute the insert
  GetExecutionEngine()->Execute(insert_plan.get(), nullptr, GetTxn(), GetExecutorContext());

  // SELECT colA, colB from empty_table3;
  const Schema *out_schema2;
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Execute the initial scan
  std::vector<Tuple> scan_results{};
  GetExecutionEngine()->Execute(scan_plan2.get(), &scan_results, GetTxn(), GetExecutorContext());
  ASSERT_EQ(TEST4_SIZE, scan_results.size());

  // Create index for target table
  auto key_schema = ParseCreateStatement("a bigint");
  const std::vector<std::uint32_t> key_attrs{0};
  auto *index_info =
      GetExecutorContext()->GetCatalog()->CreateIndex<BigintKeyType, BigintValueType, BigintComparatorType>(
          GetTxn(), "index1", "empty_table3", GetExecutorContext()->GetCatalog()->GetTable("empty_table3")->schema_,
          *key_schema, key_attrs, BIGINT_SIZE, BigintHashFunctionType{});
  ASSERT_NE(Catalog::NULL_INDEX_INFO, index_info);

  // UPDATE empty_table3 SET colB = colB + 10;

  std::unique_ptr<AbstractPlanNode> update_plan;
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    std::unordered_map<uint32_t, UpdateInfo> update_attrs;
    update_attrs.insert(std::make_pair(1, UpdateInfo(UpdateType::Add, 10)));
    update_plan = std::make_unique<UpdatePlanNode>(scan_plan2.get(), table_info->oid_, update_attrs);
  }

  // Execute the update plan
  std::vector<Tuple> update_results{};
  GetExecutionEngine()->Execute(update_plan.get(), &update_results, GetTxn(), GetExecutorContext());
  ASSERT_EQ(0, update_results.size());

  // Compare the result sets
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
  const auto &schema = table_info->schema_;
  for (auto &tuple : scan_results) {
    // Construct the key from the result tuple
    const Tuple key = tuple.KeyFromTuple(table_info->schema_, *key_schema, key_attrs);

    // Scan the index for the key
    std::vector<RID> rids{};
    index_info->index_->ScanKey(key, &rids, GetTxn());
    ASSERT_EQ(1, rids.size());

    // Get the updated tuple from the table
    Tuple indexed_tuple;
    ASSERT_TRUE(table_info->table_->GetTuple(rids[0], &indexed_tuple, GetTxn()));

    const auto old_cola = tuple.GetValue(&schema, 0).GetAs<int64_t>();
    const auto old_colb = tuple.GetValue(&schema, 1).GetAs<int32_t>();
    const auto new_cola = indexed_tuple.GetValue(&schema, 0).GetAs<int64_t>();
    const auto new_colb = indexed_tuple.GetValue(&schema, 1).GetAs<int32_t>();

    // Compare values
    ASSERT_EQ(old_cola, new_cola);
    ASSERT_EQ(old_colb + 10, new_colb);
  }
}

// Sequential add updates with N transactions
TEST_F(GradingExecutorTest, SequentialUpdateAdd) {
  constexpr const auto n_tasks = 10UL;

  // construct a sequential scan of the table
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_3");
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "col_a");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  auto scan_txn1 = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ)};
  auto exec_ctx1 =
      std::make_unique<ExecutorContext>(scan_txn1.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  std::vector<Tuple> scan_results{};
  GetExecutionEngine()->Execute(&scan_plan, &scan_results, scan_txn1.get(), exec_ctx1.get());
  GetTxnManager()->Commit(scan_txn1.get());

  // Verify results
  ASSERT_EQ(scan_results.size(), TEST3_SIZE);
  for (auto i = 0UL; i < scan_results.size(); ++i) {
    auto &tuple = scan_results[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
  }

  scan_results.clear();

  auto task = [&]() {
    // make a new transaction for the task to run
    auto txn = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ)};
    auto exec_ctx =
        std::make_unique<ExecutorContext>(txn.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    // make a new execution engine to execute the operation
    auto exec_engine = std::make_unique<ExecutionEngine>(GetBPM(), GetTxnManager(), GetCatalog());

    // construct an update plan
    std::unordered_map<uint32_t, UpdateInfo> update_attrs{};
    update_attrs.emplace(static_cast<uint32_t>(1), UpdateInfo{UpdateType::Add, 1});
    UpdatePlanNode update_plan{&scan_plan, table_info->oid_, update_attrs};

    // execute update for all tuples in the table
    std::vector<Tuple> result_set{};
    EXPECT_TRUE(GetExecutionEngine()->Execute(&update_plan, &result_set, txn.get(), exec_ctx.get()));

    // UpdateExecutor should not modify the result set
    ASSERT_EQ(result_set.size(), 0);
    GetTxnManager()->Commit(txn.get());
  };

  // launch all update tasks
  std::vector<std::thread> tasks{};
  tasks.reserve(n_tasks);
  for (auto _ = 0UL; _ < n_tasks; ++_) {
    tasks.emplace_back(task);
    tasks.back().join();
  }

  // verify results with another sequential scan
  auto scan_txn2 = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ)};
  auto exec_ctx2 =
      std::make_unique<ExecutorContext>(scan_txn2.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  EXPECT_TRUE(GetExecutionEngine()->Execute(&scan_plan, &scan_results, scan_txn2.get(), exec_ctx2.get()));
  GetTxnManager()->Commit(scan_txn2.get());

  // verify results
  ASSERT_EQ(scan_results.size(), TEST3_SIZE);
  for (auto i = 0UL; i < scan_results.size(); ++i) {
    auto &tuple = scan_results[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(),
              static_cast<int32_t>(i + n_tasks));
  }
}

}  // namespace bustub
