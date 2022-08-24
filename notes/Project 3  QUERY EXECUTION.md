# Project 3 : QUERY EXECUTION

在关系数据库中，SQL语句将被转换为逻辑查询计划，并在进行查询优化后转化为物理查询计划，系统通过执行物理查询计划完成对应的语句功能。在本实验中，需要为`bustub`实现物理查询计划执行功能，包括顺序扫描、插入、删除、更改、连接、聚合以及`DISTINCT`和`LIMIT`。

## 查询计划执行

![figure1](C:\Users\xiurui1517\Desktop\计算机书单\15445\notes\project3_figure\figure1.png)

在关系型数据库中，物理查询计划在系统内部被组织成树的形式，并通过特定的查询处理模型（迭代器模型、生产者模型）进行执行。在本实验中所要实现的模型为迭代器模型，如上图所示，该模型的每个查询计划节点通过`NEXT()`方法得到其所需的下一个元组，直至`NEXT()`方法返回假。在执行流中，根节点的`NEXT()`方法最先被调用，其控制流向下传播直至叶节点。

在`bustub`中，每个查询计划节点`AbstractPlanNode`都被包含在执行器类`AbstractExecutor`中，用户通过执行器类调用查询计划的`Next()`方法及初始化`Init()`方法，而查询计划节点中则保存该操作所需的特有信息，如顺序扫描需要在节点中保存其所要扫描的表标识符、连接需要在节点中保存其子节点及连接的谓词。同时。执行器类中也包含`ExecutorContext`上下文信息，其代表了查询计划的全局信息，如事务、事务管理器、锁管理器等。

## SeqScanExecutor

`SeqScanExecutor`执行顺序扫描操作，其通过`Next()`方法顺序遍历其对应表中的所有元组，并将元组返回至调用者。在`bustub`中，所有与表有关的信息被包含在`TableInfo`中：

```C++
 40 struct TableInfo {
 41   /**
 42    * Construct a new TableInfo instance.
 43    * @param schema The table schema
 44    * @param name The table name
 45    * @param table An owning pointer to the table heap
 46    * @param oid The unique OID for the table
 47    */
 48   TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
 49       : schema_{std::move(schema)}, name_{std::move(name)}, table_{std::move(table)}, oid_{oid} {    }  
 50   /** The table schema */
 51   Schema schema_;
 52   /** The table name */
 53   const std::string name_;
 54   /** An owning pointer to the table heap */
 55   std::unique_ptr<TableHeap> table_;
 56   /** The table OID */
 57   const table_oid_t oid_;
 58 };
```

表中的实际元组储存在`TableHeap`中，其包含用于插入、查找、更改、删除元组的所有函数接口，并可以通过`TableIterator`迭代器顺序遍历其中的元组。在`SeqScanExecutor`中，为其增加`TableInfo`、及迭代器私有成员，用于访问表信息和遍历表。在`bustub`中，所有表都被保存在目录`Catalog`中，可以通过表标识符从中提取对应的`TableInfo`：

```c++
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {
  table_oid_t oid = plan->GetTableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  iter_ = table_info_->table_->Begin(exec_ctx->GetTransaction());
  end_ = table_info_->table_->End();
}
```

在`Init()`中，执行计划节点所需的初始化操作，在这里重新设定表的迭代器，使得查询计划可以重新遍历表：

```C++
void SeqScanExecutor::Init() {
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}
```

在`Next()`中，计划节点遍历表，并通过输入参数返回元组，当遍历结束时返回假：

```C++
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *out_schema = this->GetOutputSchema();
  Schema table_schema = table_info_->schema_;
  while (iter_ != end_) {
    Tuple table_tuple = *iter_;
    *rid = table_tuple.GetRid();
    std::vector<Value> values;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(&table_tuple, &table_schema));
    }
    *tuple = Tuple(values, out_schema);
    auto *predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(tuple, out_schema).GetAs<bool>()) {
      ++iter_;
      return true;
    }
    ++iter_;
  }
  return false;
}
```

在这里，通过迭代器`iter_`访问元组，当计划节点谓词`predicate`非空时，通过`predicate`的`Evaluate`方法评估当前元组是否满足谓词，如满足则返回，否则遍历下一个元组。值得注意的是，表中的元组应当以`out_schema`的模式被重组。在`bustub`中，所有查询计划节点的输出元组均通过`out_schema`中各列`Column`的`ColumnValueExpression`中的各种“`Evaluate`”方法构造，如`Evaluate`、`EvaluateJoin`、`EvaluateAggregate`。

对于具有特定`out_schema`的计划节点，其构造输出元组的方式为遍历`out_schema`中的`Column`，并通过`Column`中`ColumnValueExpression`的`Evaluate`方法提取表元组对应的行：

```C++
 36   auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value override {
 37     return tuple->GetValue(schema, col_idx_);
 38   }
```

可以看出，`Column`中保存了该列在表模式中的列号，`Evaluate`根据该列号从表元组中提取对应的列。

## InsertExecutor

在`InsertExecutor`中，其向特定的表中插入元组，元组的来源可能为其他计划节点或自定义的元组数组。其具体来源可通过`IsRawInsert()`提取。在构造函数中，提取其所要插入表的`TableInfo`，元组来源，以及与表中的所有索引：

```c++
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  is_raw_ = plan->IsRawInsert();
  if (is_raw_) {
    size_ = plan->RawValues().size();
  }
  indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}
```

在`Init`中，当其元组来源为其他计划节点时，执行对应计划节点的`Init()`方法：

```C++
void InsertExecutor::Init() {
  if (!is_raw_) {
    child_executor_->Init();
  }
}
```

在`Next()`中，根据不同的元组来源实施不同的插入策略：

```C++
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  Tuple tmp_tuple;
  RID tmp_rid;
  if (is_raw_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
      const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);
      tmp_tuple = Tuple(raw_value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
        for (auto indexinfo : indexes_) {
          indexinfo->index_->InsertEntry(
              tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
              tmp_rid, txn);
        }
      }
    }
    return false;
  }
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
      for (auto indexinfo : indexes_) {
        indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(),
                                                              indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
                                       tmp_rid, txn);
      }
    }
  }
  return false;
}
```

需要注意，`Insert`节点不应向外输出任何元组，所以其总是返回假，即所有的插入操作均应当在一次`Next`中被执行完成。当来源为自定义的元组数组时，根据表模式构造对应的元组，并插入表中；当来源为其他计划节点时，通过子节点获取所有元组并插入表。在插入过程中，应当使用`InsertEntry`更新表中的所有索引，`InsertEntry`的参数应由`KeyFromTuple`方法构造。

## UpdateExecutor与DeleteExecutor

`UpdateExecutor`与`DeleteExecutor`用于从特定的表中更新、删除元组，其实现方法与`InsertExecutor`相似，但其元组来源仅为其他计划节点：

```C++

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple src_tuple;
  auto *txn = this->GetExecutorContext()->GetTransaction();
  while (child_executor_->Next(&src_tuple, rid)) {
    *tuple = this->GenerateUpdatedTuple(src_tuple);
    if (table_info_->table_->UpdateTuple(*tuple, *rid, txn)) {
      for (auto indexinfo : indexes_) {
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
      }
    }
  }
  return false;
}
```

`UpdateExecutor::Next`中，利用`GenerateUpdatedTuple`方法将源元组更新为新元组，在更新索引时，删除表中与源元组对应的所有索引记录，并增加与新元组对应的索引记录。

```C++

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto *txn = this->GetExecutorContext()->GetTransaction();
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, txn)) {
      for (auto indexinfo : indexes_) {
        indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), indexinfo->key_schema_,
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, txn);
      }
    }
  }
  return false;
}
```

`DeleteExecutor`与上两执行器相似，不再赘述。

## NestedLoopJoinExecutor

`NestedLoopJoinExecutor`将两个子计划节点中的所有元组进行连接操作，每次调用`Next()`方法其向父节点返回一个符合连接谓词的连接元组：

```C++

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  buffer_.clear();
  const Schema *left_schema = plan_->GetLeftPlan()->OutputSchema();
  const Schema *right_schema = plan_->GetRightPlan()->OutputSchema();
  const Schema *out_schema = this->GetOutputSchema();
  Tuple left_tuple;
  Tuple right_tuple;
  RID rid;
  while (left_executor_->Next(&left_tuple, &rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &rid)) {
      auto *predicate = plan_->Predicate();
      if (predicate == nullptr ||
          predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        std::vector<Value> values;
        for (const auto &col : out_schema->GetColumns()) {
          values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
        }
        buffer_.emplace_back(values, out_schema);
      }
    }
  }
}
```

在这里，`Init()`函数完成所有的连接操作，并将得到的所有连接元组存放在缓冲区`buffer_`中。其通过子计划节点的`Next()`方法得到子计划节点的元组，通过双层循环遍历每一对元组组合，当内层计划节点返回假时调用其`Init()`使其初始化。在得到子计划节点元组后，如存在谓词，则调用谓词的`EvaluateJoin`验证其是否符合谓词。如不存在谓词或符合谓词，则通过调用`out_schema`各`Column`的`EvaluateJoin`得到输出元组，并将其置入`buffer_`。

```C++
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!buffer_.empty()) {
    *tuple = buffer_.back();
    buffer_.pop_back();
    return true;
  }
  return false;
}
```

在`Next()`中，仅需提取缓冲区内的元组即可。

## HashJoinExecutor

`HashJoinExecutor`使用基础哈希连接算法进行连接操作，其原理为将元组的连接键（即某些属性列的组合）作为哈希表的键，并使用其中一个子计划节点的元组构造哈希表。由于具有相同连接键的元组一定具有相同的哈希键值，因此另一个子计划节点中的元组仅需在该元组映射的桶中寻找可与其连接的元组，如下图所示：

![figure2](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project3_figure\figure2.png)

为了使得元组可以被插入哈希表，需要为元组的连接键设定对应的哈希函数，以及其连接键的比较方法：

```C++
struct HashJoinKey {
  Value value_;
  bool operator==(const HashJoinKey &other) const { return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; }
};

}  // namespace bustub
namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &key) const { return bustub::HashUtil::HashValue(&key.value_); }
};
```

对于哈希函数，使用`bustub`中内置的`HashUtil::HashValue`即可。在这里，通过阅读代码可以发现，`bustub`中连接操作的连接键仅由元组的一个属性列组成，因此在连接键中仅需存放单个属性列的具体值`Value`，而不需同聚合操作一样存放属性列的组合`Vector<Value>`。连接键通过`Value`的`CompareEquals`进行比较。

```C++
 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;

  std::unique_ptr<AbstractExecutor> right_child_;

  std::unordered_multimap<HashJoinKey, Tuple> hash_map_{};

  std::vector<Tuple> output_buffer_;
};
```

在`HashJoinExecutor`中，使用`unordered_multimap`直接存放对于连接键的元组，从原理上其与使用普通哈希表并遍历桶的过程上是等价的，但使用`multimap`会使实现代码更加简单。

```C++

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_map_.clear();
  output_buffer_.clear();
  Tuple left_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  RID rid;
  while (left_child_->Next(&left_tuple, &rid)) {
    HashJoinKey left_key;
    left_key.value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);
    hash_map_.emplace(left_key, left_tuple);
  }
}
```

在`Init()`中，`HashJoinExecutor`遍历左子计划节点的元组，以完成哈希表的构建操作。

```C++

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!output_buffer_.empty()) {
    *tuple = output_buffer_.back();
    output_buffer_.pop_back();
    return true;
  }
  Tuple right_tuple;
  const Schema *left_schema = left_child_->GetOutputSchema();
  const Schema *right_schema = right_child_->GetOutputSchema();
  const Schema *out_schema = GetOutputSchema();
  while (right_child_->Next(&right_tuple, rid)) {
    HashJoinKey right_key;
    right_key.value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);
    auto iter = hash_map_.find(right_key);
    uint32_t num = hash_map_.count(right_key);
    for (uint32_t i = 0; i < num; ++i, ++iter) {
      std::vector<Value> values;
      for (const auto &col : out_schema->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateJoin(&iter->second, left_schema, &right_tuple, right_schema));
      }
      output_buffer_.emplace_back(values, out_schema);
    }
    if (!output_buffer_.empty()) {
      *tuple = output_buffer_.back();
      output_buffer_.pop_back();
      return true;
    }
  }
  return false;
}
```

在`Next()`中，使用右子计划节点的元组作为"探针"，以寻找与其连接键相同的左子计划节点的元组。需要注意的是，一个右节点的元组可能有多个左节点的元组与其对应，但一次`Next()`操作仅返回一个结果连接元组。因此在一次`Next()`中应当将连接得到的所有结果元组存放在`output_buffer_`缓冲区中，使得在下一次`Next()`中仅需从缓冲区中提取结果元组，而不调用子节点的`Next()`方法。

## AggregationExecutor

`AggregationExecutor`实现聚合操作，其原理为使用哈希表将所有聚合键相同的元组映射在一起，以此统计所有聚合键元组的聚合信息：

```C++
 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;
  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_;
  /** Simple aggregation hash table */
  // TODO(Student): Uncomment SimpleAggregationHashTable aht_;
  SimpleAggregationHashTable hash_table_;
  /** Simple aggregation hash table iterator */
  // TODO(Student): Uncomment SimpleAggregationHashTable::Iterator aht_iterator_;
  SimpleAggregationHashTable::Iterator iter_;
};
```

`SimpleAggregationHashTable`为用于聚合操作的哈希表，其以聚合键（即元组的属性列组合）作为键，并以聚合的统计结果组合（聚合键相同元组的`SUM`、`AVG`、`MIN`等统计量的组合）作为值。其设置了用于遍历其元素的迭代器。

```C++

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      hash_table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(hash_table_.Begin()) {}

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    hash_table_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = hash_table_.Begin();
}
```

在`Init()`中，遍历子计划节点的元组，并构建哈希表及设置用于遍历该哈希表的迭代器。`InsertCombine`将当前聚合键的统计信息更新：

```c++
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }
```

```C++
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountAggregate:
          // Count increases by one.
          result->aggregates_[i] = result->aggregates_[i].Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::SumAggregate:
          // Sum increases by addition.
          result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          break;
        case AggregationType::MinAggregate:
          // Min is just the min.
          result->aggregates_[i] = result->aggregates_[i].Min(input.aggregates_[i]);
          break;
        case AggregationType::MaxAggregate:
          // Max is just the max.
          result->aggregates_[i] = result->aggregates_[i].Max(input.aggregates_[i]);
          break;
      }
    }
  }
```

在`Next()`中，使用迭代器遍历哈希表，如存在谓词，则使用谓词的`EvaluateAggregate`判断当前聚合键是否符合谓词，如不符合则继续遍历直到寻找到符合谓词的聚合键。

```C++
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != hash_table_.End()) {
    auto *having = plan_->GetHaving();
    const auto &key = iter_.Key().group_bys_;
    const auto &val = iter_.Val().aggregates_;
    if (having == nullptr || having->EvaluateAggregate(key, val).GetAs<bool>()) {
      std::vector<Value> values;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(key, val));
      }
      *tuple = Tuple(values, GetOutputSchema());
      ++iter_;
      return true;
    }
    ++iter_;
  }
  return false;
}
```

## LimitExecutor

`LimitExecutor`用于限制输出元组的数量，其计划节点中定义了具体的限制数量。其`Init()`应当调用子计划节点的`Init()`方法，并重置当前限制数量；`Next()`方法则将子计划节点的元组返回，直至限制数量为0。

```C++

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  limit_ = plan_->GetLimit();
}

void LimitExecutor::Init() {
  child_executor_->Init();
  limit_ = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (limit_ == 0 || !child_executor_->Next(tuple, rid)) {
    return false;
  }
  --limit_;
  return true;
}
```

### DistinctExecutor

`DistinctExecutor`用于去除相同的输入元组，并将不同的元组输出。在这里使用哈希表方法去重，哈希表的具体构建策略参照了聚合执行器中的`SimpleAggregationHashTable`：

```C++
namespace bustub {
struct DistinctKey {
  std::vector<Value> value_;
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.value_.size(); i++) {
      if (value_[i].CompareEquals(other.value_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &key) const {
    size_t curr_hash = 0;
    for (const auto &value : key.value_) {
      if (!value.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&value));
      }
    }
    return curr_hash;
  }
};
...

class DistinctExecutor : public AbstractExecutor {
...
  std::unordered_set<DistinctKey> set_;

  DistinctKey MakeKey(const Tuple *tuple) {
    std::vector<Value> values;
    const Schema *schema = GetOutputSchema();
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      values.emplace_back(tuple->GetValue(schema, i));
    }
    return {values};
  }
};
```

在实际运行中，使用哈希表去重即。`Init()`清空当前哈希表，并初始化子计划节点。`Next()`判断当前元组是否已经出现在哈希表中，如是则遍历下一个输入元组，如非则将该元组插入哈希表并返回：

```C++

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void DistinctExecutor::Init() {
  set_.clear();
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    auto key = MakeKey(tuple);
    if (set_.count(key) == 0U) {
      set_.insert(key);
      return true;
    }
  }
  return false;
}
```

## 实验结果

![figure3](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project3_figure\figure3.png)
