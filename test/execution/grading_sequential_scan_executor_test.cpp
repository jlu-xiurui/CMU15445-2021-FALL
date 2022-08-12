//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_sequential_scan_executor_test.cpp
//
// Identification: test/execution/sequential_scan_executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
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

// SELECT colA, colB FROM test_1
TEST_F(GradingExecutorTest, SequentialScan) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *cola_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *out_schema = MakeOutputSchema({{"colA", cola_a}, {"colB", cola_b}});
  SeqScanPlanNode plan{out_schema, nullptr, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 1000);
}

// SELECT colA, colB FROM test_1 WHERE colA < 500
TEST_F(GradingExecutorTest, SequentialScanWithPredicate) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *cola_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));
  auto *predicate = MakeComparisonExpression(cola_a, const500, ComparisonType::LessThan);
  auto *out_schema = MakeOutputSchema({{"colA", cola_a}, {"colB", cola_b}});
  SeqScanPlanNode plan{out_schema, predicate, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() < 500);
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() < 10);
  }

  ASSERT_EQ(result_set.size(), 500);
}

// SELECT colA, colB FROM test_1 WHERE colA > 1000
TEST_F(GradingExecutorTest, SequentialScanWithZeroMatchPredicate) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *cola_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *const1000 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(1000));
  auto *predicate = MakeComparisonExpression(cola_a, const1000, ComparisonType::GreaterThan);
  auto *out_schema = MakeOutputSchema({{"colA", cola_a}, {"colB", cola_b}});
  SeqScanPlanNode plan{out_schema, predicate, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT colA FROM empty_table
TEST_F(GradingExecutorTest, SequentialScanEmptyTable) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table");
  Schema &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *out_schema = MakeOutputSchema({{"colA", cola_a}});
  SeqScanPlanNode plan{out_schema, nullptr, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 0);
}

// SELECT colA, colB, colC FROM test_7
TEST_F(GradingExecutorTest, SequentialScanCyclicColumn) {
  // Construct query plan
  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_7");
  auto &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *cola_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *col_c = MakeColumnValueExpression(schema, 0, "colC");
  auto *out_schema = MakeOutputSchema({{"colA", cola_a}, {"colB", cola_b}, {"colC", col_c}});
  SeqScanPlanNode plan{out_schema, nullptr, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), TEST7_SIZE);

  for (auto i = 0UL; i < result_set.size(); ++i) {
    auto &tuple = result_set[i];
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int64_t>(), static_cast<int64_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), static_cast<int32_t>(i));
    ASSERT_EQ(tuple.GetValue(out_schema, out_schema->GetColIdx("colC")).GetAs<int32_t>(),
              static_cast<int32_t>((i % 10)));
  }
}

// SELECT colA AS col1, colB AS col2 FROM test_1
TEST_F(GradingExecutorTest, SchemaChangeSequentialScan) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  Schema &schema = table_info->schema_;
  auto *cola_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *cola_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *out_schema = MakeOutputSchema({{"col1", cola_a}, {"col2", cola_b}});
  SeqScanPlanNode plan{out_schema, nullptr, table_info->oid_};

  // Execute sequential scan
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify results
  ASSERT_EQ(result_set.size(), 1000);
}

// SELECT colA FROM test_4
// READ_UNCOMMITTED isolation level
TEST_F(GradingExecutorTest, DISABLED_ConcurrentScanReadUncommitted) {
  constexpr const auto n_tasks = 10UL;
  auto task = [&]() {
    // make a new transaction for the task to run
    auto txn = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED)};
    auto exec_ctx =
        std::make_unique<ExecutorContext>(txn.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    // make a new execution engine to execute the operation
    auto exec_engine = std::make_unique<ExecutionEngine>(GetBPM(), GetTxnManager(), GetCatalog());

    // construct a query plan
    auto *table_info = exec_ctx->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto cola_a = AllocateColumnValueExpression(schema, 0, "colA");
    auto out_schema = AllocateOutputSchema({{"colA", cola_a.get()}});
    SeqScanPlanNode plan{out_schema.get(), nullptr, table_info->oid_};

    // execute sequential scan
    std::vector<Tuple> result_set{};
    EXPECT_TRUE(exec_engine->Execute(&plan, &result_set, txn.get(), exec_ctx.get()));
    GetTxnManager()->Commit(txn.get());

    const auto success = std::all_of(result_set.cbegin(), result_set.cend(), [&, c = 0LL](const Tuple &t) mutable {
      return t.GetValue(&schema, out_schema->GetColIdx("colA")).GetAs<int64_t>() == static_cast<int64_t>(c++);
    });
    EXPECT_EQ(result_set.size(), TEST4_SIZE);
    EXPECT_TRUE(success);
  };

  std::vector<std::thread> tasks{};
  tasks.reserve(n_tasks);
  for (auto _ = 0UL; _ < n_tasks; ++_) {
    tasks.emplace_back(task);
  }

  for (auto &task : tasks) {
    task.join();
  }
}

// SELECT colA FROM test_4
// READ_COMMITTED isolation level
TEST_F(GradingExecutorTest, DISABLED_ConcurrentScanReadCommitted) {
  constexpr const auto n_tasks = 10UL;
  auto task = [&]() {
    // make a new transaction for the task to run
    auto txn = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED)};
    auto exec_ctx =
        std::make_unique<ExecutorContext>(txn.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    // make a new execution engine to execute the operation
    auto exec_engine = std::make_unique<ExecutionEngine>(GetBPM(), GetTxnManager(), GetCatalog());

    // construct a query plan
    auto *table_info = exec_ctx->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto cola_a = AllocateColumnValueExpression(schema, 0, "colA");
    auto out_schema = AllocateOutputSchema({{"colA", cola_a.get()}});
    SeqScanPlanNode plan{out_schema.get(), nullptr, table_info->oid_};

    // execute sequential scan
    std::vector<Tuple> result_set{};
    EXPECT_TRUE(exec_engine->Execute(&plan, &result_set, txn.get(), exec_ctx.get()));
    GetTxnManager()->Commit(txn.get());

    const auto success = std::all_of(result_set.cbegin(), result_set.cend(), [&, c = 0LL](const Tuple &t) mutable {
      return t.GetValue(&schema, out_schema->GetColIdx("colA")).GetAs<int64_t>() == static_cast<int64_t>(c++);
    });
    EXPECT_EQ(result_set.size(), TEST4_SIZE);
    EXPECT_TRUE(success);
  };

  std::vector<std::thread> tasks{};
  tasks.reserve(n_tasks);
  for (auto _ = 0UL; _ < n_tasks; ++_) {
    tasks.emplace_back(task);
  }

  for (auto &task : tasks) {
    task.join();
  }
}

// SELECT colA FROM test_4
// REPEATABLE_READ isolation level
TEST_F(GradingExecutorTest, DISABLED_ConcurrentScanRepeatableRead) {
  constexpr const auto n_tasks = 10UL;
  auto task = [&]() {
    // make a new transaction for the task to run
    auto txn = std::unique_ptr<Transaction>{GetTxnManager()->Begin(nullptr, IsolationLevel::REPEATABLE_READ)};
    auto exec_ctx =
        std::make_unique<ExecutorContext>(txn.get(), GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    // make a new execution engine to execute the operation
    auto exec_engine = std::make_unique<ExecutionEngine>(GetBPM(), GetTxnManager(), GetCatalog());

    // construct a query plan
    auto *table_info = exec_ctx->GetCatalog()->GetTable("test_4");
    auto &schema = table_info->schema_;
    auto cola_a = AllocateColumnValueExpression(schema, 0, "colA");
    auto out_schema = AllocateOutputSchema({{"colA", cola_a.get()}});
    SeqScanPlanNode plan{out_schema.get(), nullptr, table_info->oid_};

    // execute sequential scan
    std::vector<Tuple> result_set{};
    EXPECT_TRUE(exec_engine->Execute(&plan, &result_set, txn.get(), exec_ctx.get()));
    GetTxnManager()->Commit(txn.get());

    const auto success = std::all_of(result_set.cbegin(), result_set.cend(), [&, c = 0LL](const Tuple &t) mutable {
      return t.GetValue(&schema, out_schema->GetColIdx("colA")).GetAs<int64_t>() == static_cast<int64_t>(c++);
    });
    EXPECT_EQ(result_set.size(), TEST4_SIZE);
    EXPECT_TRUE(success);
  };

  std::vector<std::thread> tasks{};
  tasks.reserve(n_tasks);
  for (auto _ = 0UL; _ < n_tasks; ++_) {
    tasks.emplace_back(task);
  }

  for (auto &task : tasks) {
    task.join();
  }
}

}  // namespace bustub
