/**
 * grading_transaction_test.cpp
 */

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

class GradingTransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManagerInstance>(2560, disk_manager_.get());
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    lock_manager_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ =
        std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();

    execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
  }

  // This function is called after every test.
  void TearDown() override {
    // Commit our transaction.
    txn_mgr_->Commit(txn_);
    // Shut down the disk manager and clean up the transaction.
    disk_manager_->ShutDown();
    remove("executor_test.db");
    delete txn_;
  };

  /** @return the executor context in our test class */
  ExecutorContext *GetExecutorContext() { return exec_ctx_.get(); }
  ExecutionEngine *GetExecutionEngine() { return execution_engine_.get(); }
  Transaction *GetTxn() { return txn_; }
  TransactionManager *GetTxnManager() { return txn_mgr_.get(); }
  Catalog *GetCatalog() { return catalog_.get(); }
  BufferPoolManager *GetBPM() { return bpm_.get(); }
  LockManager *GetLockManager() { return lock_manager_.get(); }

  // The below helper functions are useful for testing.

  const AbstractExpression *MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeConstantValueExpression(const Value &val) {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                     ComparisonType comp_type) {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

/****************************
 * Transaction Tests (25 pts)
 ****************************/

// NOLINTNEXTLINE
TEST_F(GradingTransactionTest, DirtyReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  auto key_schema = ParseCreateStatement("a bigint");

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());

  // Iterate through table to read the tuples.
  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  GetTxnManager()->Abort(txn1);
  delete txn1;

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // Size
  ASSERT_EQ(result_set.size(), 3);

  GetTxnManager()->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(GradingTransactionTest, UnrepeatableReadsTest) {
  // txn0: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: SELECT * FROM empty_table2;
  // txn2: UPDATE empty_table2 SET colA = colA+10
  // txn2 commit
  // txn1: SELECT * FROM empty_table2;

  auto txn0 = GetTxnManager()->Begin();
  auto exec_ctx0 = std::make_unique<ExecutorContext>(txn0, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx0->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn0, exec_ctx0.get());
  GetTxnManager()->Commit(txn0);
  delete txn0;

  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);

  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  std::unordered_map<uint32_t, UpdateInfo> update_attrs;
  update_attrs.insert(std::make_pair(0, UpdateInfo(UpdateType::Add, 10)));
  std::unique_ptr<AbstractPlanNode> update_plan;
  { update_plan = std::make_unique<UpdatePlanNode>(&scan_plan, table_info->oid_, update_attrs); }

  result_set.clear();
  GetExecutionEngine()->Execute(update_plan.get(), &result_set, txn2, exec_ctx2.get());

  GetTxnManager()->Commit(txn2);
  delete txn2;

  result_set.clear();
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 210);
  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 211);
  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 212);

  GetTxnManager()->Commit(txn1);
  delete txn1;
}

// NOLINTNEXTLINE
TEST_F(GradingTransactionTest, RepeatableReadsTest) {
  // txn0: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: SELECT * FROM empty_table2;
  // txn2: UPDATE empty_table2 SET colA = colA+10
  // txn1: SELECT * FROM empty_table2;

  auto txn0 = GetTxnManager()->Begin();
  auto exec_ctx0 = std::make_unique<ExecutorContext>(txn0, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx0->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn0, exec_ctx0.get());
  GetTxnManager()->Commit(txn0);
  delete txn0;

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::unordered_map<uint32_t, UpdateInfo> update_attrs;
  update_attrs.insert(std::make_pair(0, UpdateInfo(UpdateType::Add, 10)));
  std::unique_ptr<AbstractPlanNode> update_plan;
  { update_plan = std::make_unique<UpdatePlanNode>(&scan_plan, table_info->oid_, update_attrs); }

  std::thread t0([&] {
    std::vector<Tuple> result_set;
    GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());

    // First value
    ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    // Second value
    ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    // Third value
    ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    result_set.clear();
    GetExecutionEngine()->Execute(&scan_plan, &result_set, txn1, exec_ctx1.get());

    // First value
    ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    // Second value
    ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    // Third value
    ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);

    GetTxnManager()->Commit(txn1);
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    GetExecutionEngine()->Execute(update_plan.get(), nullptr, txn2, exec_ctx2.get());

    std::vector<Tuple> result_set;
    GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

    // First value
    ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 210);
    // Second value
    ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 211);
    // Third value
    ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 212);

    GetTxnManager()->Commit(txn2);
  });

  t0.join();
  t1.join();
  delete txn1;
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(GradingTransactionTest, DISABLED_IntegratedTest) {
  //  txn1 ->        scan -> join -> aggregate
  //  txn2 ->    delete one tuple -> commit
  //  txn3 -> scan

  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto txn3 = GetTxnManager()->Begin();
  auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  auto table_info2 = GetExecutorContext()->GetCatalog()->GetTable("test_2");

  std::unique_ptr<AbstractPlanNode> scan_plan1;
  const Schema *out_schema1;
  {
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  const Schema *out_schema2;
  {
    auto &schema = table_info2->schema_;
    auto col1 = MakeColumnValueExpression(schema, 0, "col1");
    auto col2 = MakeColumnValueExpression(schema, 0, "col2");
    out_schema2 = MakeOutputSchema({{"col1", col1}, {"col2", col2}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info2->oid_);
  }
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan;
  const Schema *out_final;
  {
    // colA and colB have a tuple index of 0 because they are the left side of the join
    auto col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");
    // col1 and col2 have a tuple index of 1 because they are the right side of the join
    auto col1 = MakeColumnValueExpression(*out_schema2, 1, "col1");
    auto col2 = MakeColumnValueExpression(*out_schema2, 1, "col2");
    std::vector<const AbstractExpression *> left_keys{col_a};
    std::vector<const AbstractExpression *> right_keys{col1};
    auto predicate = MakeComparisonExpression(col_a, col1, ComparisonType::Equal);
    out_final = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"col1", col1}, {"col2", col2}});
    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_final, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  std::unique_ptr<AbstractPlanNode> agg_plan;
  const Schema *agg_schema;
  {
    const AbstractExpression *col_a = MakeColumnValueExpression(*out_final, 0, "colA");
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

  std::unique_ptr<AbstractPlanNode> scan_delete_plan;
  const Schema *out_delete_schema;
  {
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto const1 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(1));
    auto predicate = MakeComparisonExpression(col_a, const1, ComparisonType::Equal);
    out_delete_schema = MakeOutputSchema({{"colA", col_a}});
    scan_delete_plan = std::make_unique<SeqScanPlanNode>(out_delete_schema, predicate, table_info->oid_);
  }
  std::unique_ptr<AbstractPlanNode> delete_plan;
  { delete_plan = std::make_unique<DeletePlanNode>(scan_delete_plan.get(), table_info->oid_); }

  std::unordered_map<uint32_t, UpdateInfo> update_attrs;
  update_attrs.insert(std::make_pair(0, UpdateInfo(UpdateType::Add, 10)));
  std::unique_ptr<AbstractPlanNode> update_plan;
  { update_plan = std::make_unique<UpdatePlanNode>(scan_plan1.get(), table_info->oid_, update_attrs); }

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    std::vector<Tuple> result_set1;
    GetExecutionEngine()->Execute(agg_plan.get(), &result_set1, txn1, exec_ctx1.get());

    ASSERT_EQ(result_set1.size(), 1);
    auto tuple = result_set1[0];
    auto count_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("countA")).GetAs<int32_t>();
    auto sum_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("sumA")).GetAs<int32_t>();
    auto min_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("minA")).GetAs<int32_t>();
    auto max_a_val = tuple.GetValue(agg_schema, agg_schema->GetColIdx("maxA")).GetAs<int32_t>();
    // Should count all tuples
    ASSERT_EQ(count_a_val, TEST2_SIZE - 1);
    // Should sum from 0 to TEST2_SIZE - 1 except for 1
    ASSERT_EQ(sum_a_val, TEST2_SIZE * (TEST2_SIZE - 1) / 2 - 1);
    // Minimum should be 0
    ASSERT_EQ(min_a_val, 0);
    // Maximum should be TEST2_SIZE - 1
    ASSERT_EQ(max_a_val, TEST2_SIZE - 1);
    GetTxnManager()->Commit(txn1);
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    GetExecutionEngine()->Execute(delete_plan.get(), nullptr, txn2, exec_ctx2.get());
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    GetTxnManager()->Commit(txn2);
  });

  std::thread t3([&] {
    std::vector<Tuple> result_set;
    GetExecutionEngine()->Execute(scan_plan1.get(), nullptr, txn3, exec_ctx3.get());

    int val = 0;
    for (const auto &res_tuple : result_set) {
      ASSERT_EQ(res_tuple.GetValue(out_schema1, out_schema1->GetColIdx("colA")).GetAs<int32_t>(), val++);
    }
    GetTxnManager()->Commit(txn3);
  });

  t1.join();
  t2.join();
  t3.join();
  delete txn1;
  delete txn2;
  delete txn3;
}

}  // namespace bustub
