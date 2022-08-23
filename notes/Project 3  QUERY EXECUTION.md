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

在`Init()`中，执行计划节点所需的初始化操作，在这里重新设定表的迭代器，使得查询计划可以重新遍历表（但本实验中并不需要此功能）：

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

在这里，通过迭代器`iter_`访问元组，当计划节点谓词`predicate`非空时，通过`predicate`的`Evaluate`方法评估当前元组是否满足谓词，如满足则返回，否则遍历下一个元组。值得注意的是，表中的元组应当以`out_schema`的模式被重组。在`bustub`中，所有查询计划节点的输出元组均通过`AbstractExpression`中的各种“`Evaluate`”方法构造，如`Evaluate`、`EvaluateJoin`、`EvaluateAggregate`。

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
