# Project 4 : CONCURRENCY CONTROL

本实验将实现`bustub`中的锁管理器，其负责跟踪数据库中使用的元组级锁，以使得数据库支持并发的查询计划执行。

## TASK #1 - LOCK MANAGER + DEADLOCK PREVENTION

在本实验中，将使用两阶段锁策略实现具体的元组级锁，具体的锁定解锁策略应当由事务的隔离级别决定。当一个事务需要读取或写入元组时，其需要根据隔离级别尝试获得元组对应的读锁或写锁，并在适当的时刻将其释放。

### 事务及隔离级别

```C++
155 class Transaction {
...
257  private:
258   /** The current transaction state. */
259   TransactionState state_;
260   /** The isolation level of the transaction. */
261   IsolationLevel isolation_level_;
262   /** The thread ID, used in single-threaded transactions. */
263   std::thread::id thread_id_;
264   /** The ID of this transaction. */
265   txn_id_t txn_id_;
266 
267   /** The undo set of table tuples. */
268   std::shared_ptr<std::deque<TableWriteRecord>> table_write_set_;
269   /** The undo set of indexes. */
270   std::shared_ptr<std::deque<IndexWriteRecord>> index_write_set_;
271   /** The LSN of the last record written by the transaction. */
272   lsn_t prev_lsn_;
273 
274   /** Concurrent index: the pages that were latched during index operation. */
275   std::shared_ptr<std::deque<Page *>> page_set_;
276   /** Concurrent index: the page IDs that were deleted during index operation.*/
277   std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;
278 
279   /** LockManager: the set of shared-locked tuples held by this transaction. */
280   std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
281   /** LockManager: the set of exclusive-locked tuples held by this transaction. */
282   std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;
283 };
284 
```

`bustub`中事务由`Transaction`以及`TransactionManager`管理。`Transaction`中维护了事务的全部信息，包括事务ID、事务隔离级别、事务的状态（锁扩张、锁收缩、`COMMIT`及`ABORT`）、事务的元组修改记录及索引修改记录、事务的页面修改记录、以及事务当前所拥有的锁。

`TransactionManager`中进行事务的实际行为，如`BEGIN`、`COMMIT`、`ABORT`，并可以通过其获得对应ID的具体事务。

![figure1](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project4_figure\figure1.png)

事务的不同隔离级别将导致不同的可能并发异常情况，并在两阶段锁策略中通过不同的获取释放锁方式实现。

![figure2](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project4_figure\figure2.png)

### 死锁预防策略

在本实验中，将通过`Wound-Wait`策略实现死锁预防，其具体方法为：当优先级高的事务等待优先级低的事务的锁时，将优先级低的事务杀死；当优先级低的事务等待优先级高的事务的锁时，优先级低的事务将阻塞。在`bustub`中，事务的优先级通过其事务ID确定，越小的事务ID将代表更高的优先级。

### 锁请求表

![figure3](C:\Users\xiurui\Desktop\计算机书单\CMU15445\notes\project4_figure\figure3.png)

在锁管理器中，使用`lock table`管理锁，`lock table`是以元组ID为键，锁请求队列为值的哈希表。其中，锁请求中保存了请求该元组锁的事务ID、请求的元组锁类型、以及请求是否被许可；通过队列的方式保存锁保证了锁请求的先后顺序：

```C++
 38   class LockRequest {
 39    public:
 40     LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), gr    anted_(false) {}
 41 
 42     txn_id_t txn_id_;
 43     LockMode lock_mode_;
 44     bool granted_;
 45   };
 46 
 47   class LockRequestQueue {
 48    public:
 49     std::list<LockRequest> request_queue_;
 50     std::mutex latch_;
 51     // for notifying blocked transactions on this rid
 52     std::condition_variable cv_;
 53     // txn_id of an upgrading transaction (if any)
 54     txn_id_t upgrading_ = INVALID_TXN_ID;
 55   };
```

### LockShared

在`LockShared`中，事务`txn`请求元组ID为`rid`的读锁：

```C++
 20 auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
 21   if (txn->GetState() == TransactionState::ABORTED) {
 22     return false;
 23   }
 24   if (txn->GetState() == TransactionState::SHRINKING) {
 25     txn->SetState(TransactionState::ABORTED);
 26     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
 27   }
 28   if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
 29     txn->SetState(TransactionState::ABORTED);
 30     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCO    MMITTED);
 31   }
```

**20-31行**：进行前置判断，当事务的状态为`ABORT`时，直接返回假；如当前的事务状态为锁收缩时，调用获取锁函数将导致事务`ABORT`并抛出异常；当事务的隔离级别的`READ_UNCOMMITTED`时，其不应获取读锁，尝试获取读锁将导致`ABORT`并抛出异常。

```C++
 32   txn->SetState(TransactionState::GROWING);
 33   std::unique_lock<std::mutex> lk(latch_);
 34   auto &lock_request_queue = lock_table_[rid];
 35   auto &request_queue = lock_request_queue.request_queue_;
 36   auto &cv = lock_request_queue.cv_;
 37   auto txn_id = txn->GetTransactionId();
 38   request_queue.emplace_back(txn_id, LockMode::SHARED);
 39   txn->GetSharedLockSet()->emplace(rid);
 40   txn_table_[txn_id] = txn;
 41   //Wound Wait : Kill all low priority transaction
 42   bool can_grant = true;
 43   bool is_kill = false;
 44   for (auto &request : request_queue) {
 45     if (request.lock_mode_ == LockMode::EXCLUSIVE) {
 46       if (request.txn_id_ > txn_id) {
 47         txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
 48         is_kill = true;
 49       } else {
 50         can_grant = false;
 51       }
 52     }
 53     if (request.txn_id_ == txn_id) {
 54       request.granted_ = can_grant;
 55       break;
 56     }
 57   }
 58   if (is_kill) {
 59     cv.notify_all();
 60   }
```

**32-60行**：如前置判断通过，则将当前事务状态置为`GROWING`，并获得互斥锁保护锁管理器的数据结构。然后，获取对应元组ID的锁请求队列及其相关成员，并将当前事务的锁请求加入队列。在这里`txn_table_`为保存<事务ID、事务>的二元组。需要注意，在**该请求被加入队列**时将就应当调用`GetSharedLockSet()`将该元组ID加入事务的持有锁集合，使得该锁被杀死时能将该请求从队列中删除。

为了避免死锁，事务需要检查当前队列是否存在使得其阻塞的锁请求，如存在则判断当前事务的优先级是否高于该请求的事务，如是则杀死该事务；如非则将`can_grant`置为false表示事务将被阻塞。如该事务杀死了任何其他事务，则通过锁请求队列的条件变量`cv`唤醒其他等待锁的事务，使得被杀死的事务可以退出请求队列。

```C++
 61   //Wait the lock
 62   while (!can_grant) {
 63     for (auto &request : request_queue) {
 64       if (request.lock_mode_ == LockMode::EXCLUSIVE &&
 65           txn_table_[request.txn_id_]->GetState() != TransactionState::ABORTED) {
 66         break;
 67       }
 68       if (request.txn_id_ == txn_id) {
 69         can_grant = true;
 70         request.granted_ = true;
 71       }
 72     }
 73     if (!can_grant) {
 74       cv.wait(lk);
 75     }
 76     if (txn->GetState() == TransactionState::ABORTED) {
 77       throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
 78     }
 79   }
 80   return true;
 81 }
```

**61-81行**：如存在阻塞该事务的其他事务，且该事务不能将其杀死，则进入循环等待锁。在循环中，事务调用条件变量`cv`的`wait`阻塞自身，并原子地释放锁。每次当其被唤醒时，其检查：

1. 该事务是否被杀死，如是则抛出异常；

2. 队列中在该事务之前的锁请求是否存在活写锁（状态不为`ABORT`），如是则继续阻塞，如非则将该请求的`granted_`置为真，并返回。

### LockExclusive

`LockExclusive`使得事务`txn`尝试获得元组ID为`rid`的元组写锁。

```c++
 83 auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
 84   if (txn->GetState() == TransactionState::ABORTED) {
 85     return false;
 86   }
 87   if (txn->GetState() == TransactionState::SHRINKING) {
 88     txn->SetState(TransactionState::ABORTED);
 89     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
 90   }
```

**83-90行**：进行前置检查，如当前事务状态为`ABORT`返回假，如当前锁在收缩阶段，则将其状态置为`ABORT`并抛出异常。

```C++
 91   txn->SetState(TransactionState::GROWING);
 92   std::unique_lock<std::mutex> lk(latch_);
 93   auto &lock_request_queue = lock_table_[rid];
 94   auto &request_queue = lock_request_queue.request_queue_;
 95   auto &cv = lock_request_queue.cv_;
 96   auto txn_id = txn->GetTransactionId();
 97   request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);
 98   txn->GetExclusiveLockSet()->emplace(rid);
 99   txn_table_[txn_id] = txn;
100   //Wound Wait
101   bool can_grant = true;
102   bool is_kill = false;
103   for (auto &request : request_queue) {
104     if (request.txn_id_ == txn_id) {
105       request.granted_ = can_grant;
106       break;
107     }
108     if (request.txn_id_ > txn_id) {
109       txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
110       is_kill = true;
111     } else {
112       can_grant = false;
113     }
114   }
115   if (is_kill) {
116     cv.notify_all();
117   }
```

**91-117行**：更新事务状态，获取锁请求队列，并将该请求插入队列，以及将该锁加入事务的拥有锁集合。查询是否有将该事务锁请求阻塞的请求，当获取写锁时，队列中的任何一个锁请求都将造成其阻塞，当锁请求的事务优先级低时，将其杀死。如存在不能杀死的请求，则该事务将被阻塞。当杀死了任一事务时，将唤醒该锁等待队列的所有事务。

```C++
119   while (!can_grant) {
120     auto it = request_queue.begin();
121     while (txn_table_[it->txn_id_]->GetState() == TransactionState::ABORTED) {
122       ++it;
123     }
124     if (it->txn_id_ == txn_id) {
125       can_grant = true;
126       it->granted_ = true;
127     }
128     if (!can_grant) {
129       cv.wait(lk);
130     }
131     if (txn->GetState() == TransactionState::ABORTED) {
132       throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
133     }
134   }
135   return true;
136 }
```

**91-117行**：等待锁可用，每当事务被唤醒时，检查其是否被杀死，如被杀死则抛出异常；如未被杀死，则检查队列前是否有任意未被杀死的锁请求，如没有则获得锁并将锁请求`granted_`置为真。

### LockUpgrade

`LockUpgrade`用于将当前事务`txn`所拥有的元组ID为`rid`的读锁升级为写锁。

```C++
138 auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
139   if (txn->GetState() == TransactionState::ABORTED) {
140     return false;
141   }
142   std::unique_lock<std::mutex> lk(latch_);
143   auto &lock_request_queue = lock_table_[rid];
144   if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
145     txn->SetState(TransactionState::ABORTED);
146     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
147   }
148   auto &request_queue = lock_request_queue.request_queue_;
149   auto &cv = lock_request_queue.cv_;
150   auto txn_id = txn->GetTransactionId();
151   lock_request_queue.upgrading_ = txn_id;
```

**138-151行**：判断当前事务是否被杀死，以及该元组的锁请求序列是否已经存在等待升级锁的其他事务，如是则杀死事务并抛出异常。如通过检验，则将当前锁请求队列的`upgrading_`置为当前事务ID，以提示该队列存在一个等待升级锁的事务。

```C++
153   while (!can_grant) {
154     auto it = request_queue.begin();
155     auto target = it;
156     can_grant = true;
157     bool is_kill = false;
158     while (it != request_queue.end() && it->granted_) {
159       if (it->txn_id_ == txn_id) {
160         target = it;
161       } else if (it->txn_id_ > txn_id) {
162         txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
163         is_kill = true;
164       } else {
165         can_grant = false;
166       }
167       ++it;
168     }
169     if (is_kill) {
170       cv.notify_all();
171     } 
172     if (!can_grant) {
173       cv.wait(lk);
174     } else {
175       target->lock_mode_ = LockMode::EXCLUSIVE;
176       lock_request_queue.upgrading_ = INVALID_TXN_ID;
177     } 
178     if (txn->GetState() == TransactionState::ABORTED) {
179       throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
180     } 
181   } 
182   
183   txn->GetSharedLockSet()->erase(rid);
184   txn->GetExclusiveLockSet()->emplace(rid);
185   return true;

```

**152-184行**：在`Wound Wait`中并未提及有关更新锁的行为，在这里将其每次唤醒尝试升级锁视为一次写锁获取，即每次其尝试升级锁时都将杀死队列前方将其阻塞的事务。

其具体方法为，每次事务被唤醒时，先检查其是否被杀死，然后遍历锁请求队列在其前方的请求，如其优先级较低则将其杀死，如其优先级较高则将`can_grant`置为假，示意其将在之后被阻塞。如杀死任意一个事务，则唤醒其他事务。如`can_grant`为假则阻塞事务，如为真则更新锁请求的`lock_mode_`并将`upgrading_`初始化。

当升级成功时，更新事务的拥有锁集合。

### Unlock

`Unlock`函数使得事务`txn`释放元组ID为`rid`元组上的锁。

```c++
188 auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
189   if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
190     txn->SetState(TransactionState::SHRINKING);
191   }
192 
193   std::unique_lock<std::mutex> lk(latch_);
194   auto &lock_request_queue = lock_table_[rid];
195   auto &request_queue = lock_request_queue.request_queue_;
196   auto &cv = lock_request_queue.cv_;
197   auto txn_id = txn->GetTransactionId();
198   auto it = request_queue.begin();
199   while (it->txn_id_ != txn_id) {
200     ++it;
201   }
202 
203   request_queue.erase(it);
204   cv.notify_all();
205   txn->GetSharedLockSet()->erase(rid);
206   txn->GetExclusiveLockSet()->erase(rid);
207   return true;
208 }
```

需要注意，当事务隔离级别为`READ_COMMIT`时，事务获得的读锁将在使用完毕后立即释放，因此该类事务不符合2PL规则，为了程序的兼容性，在这里认为`READ_COMMIT`事务在`COMMIT`或`ABORT`之前始终保持`GROWING`状态，对于其他事务，将在调用`Unlock`时转变为`SHRINKING`状态。在释放锁时，遍历锁请求对列并删除对应事务的锁请求，然后唤醒其他事务，并在事务的锁集合中删除该锁。

## TASK #3 - CONCURRENT QUERY EXECUTION

在本部分中，需要为查询计划执行器的顺序扫描、插入、删除、更新计划的`NEXT()`方法提供元组锁的保护，使得上述计划可支持并发执行。

```C++
 33 bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
 34   const Schema *out_schema = this->GetOutputSchema();
 35   auto exec_ctx = GetExecutorContext();
 36   Transaction *txn = exec_ctx->GetTransaction();
 37   TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
 38   LockManager *lock_mgr = exec_ctx->GetLockManager();
 39   Schema table_schema = table_info_->schema_;
 40   while (iter_ != end_) {
 41     Tuple table_tuple = *iter_;
 42     *rid = table_tuple.GetRid();
 43     if (txn->GetSharedLockSet()->count(*rid) == 0U && txn->GetExclusiveLockSet()->count(*rid) == 0U) {
 44       if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !lock_mgr->LockShared(txn, *rid)) {
 45         txn_mgr->Abort(txn);
 46       }
 47     }
 48     std::vector<Value> values;
 49     for (const auto &col : GetOutputSchema()->GetColumns()) {
 50       values.emplace_back(col.GetExpr()->Evaluate(&table_tuple, &table_schema));
 51     }
 52     *tuple = Tuple(values, out_schema);
 53     auto *predicate = plan_->GetPredicate();
 54     if (predicate == nullptr || predicate->Evaluate(tuple, out_schema).GetAs<bool>()) {
 55       ++iter_;
 56       if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
 57         lock_mgr->Unlock(txn, *rid);
 58       }
 59       return true;
 60     }
 61     if (txn->GetSharedLockSet()->count(*rid) != 0U && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
 62       lock_mgr->Unlock(txn, *rid);
 63     }
 64     ++iter_;
 65   }
 66   return false;
 67 }
```

当`SeqScanExecutor`从表中获取元组时，需要在以下条件下为该元组加读锁，并当加锁失败时调用`Abort`杀死该元组：

1. 事务不拥有该元组的读锁或写锁（由于一个事务中可能多次访问同一元组）；
2. 事务的隔离等级不为`READ_UNCOMMITTED`。

在使用完毕元组后，需要在以下条件下为该元组解锁：

1. 事务拥有该元组的读锁（可能事务拥有的锁为写锁）；
2. 事务的隔离等级为`READ_COMMITTED`时，需要在使用完读锁后立即释放，在其他等级下时，锁将在`COMMIT`或`ABORT`中被释放。

```C++
 37 bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
 38   auto exec_ctx = GetExecutorContext();
 39   Transaction *txn = exec_ctx_->GetTransaction();
 40   TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
 41   LockManager *lock_mgr = exec_ctx->GetLockManager();
 42 
 43   Tuple tmp_tuple;
 44   RID tmp_rid;
 45   if (is_raw_) {
 46     for (uint32_t idx = 0; idx < size_; idx++) {
 47       const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);
 48       tmp_tuple = Tuple(raw_value, &table_info_->schema_);
 49       if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
 50         if (!lock_mgr->LockExclusive(txn, tmp_rid)) {
 51           txn_mgr->Abort(txn);
 52         }
 53         for (auto indexinfo : indexes_) {
 54           indexinfo->index_->InsertEntry(
 55               tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
 56               tmp_rid, txn);
 57           IndexWriteRecord iwr(*rid, table_info_->oid_, WType::INSERT, *tuple, *tuple, indexinfo->index_oid_,
 58                                exec_ctx->GetCatalog());
 59           txn->AppendIndexWriteRecord(iwr);
 60         }
 61       }
 62     }
 63     return false;
 64   }
 65   while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
 66     if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) {
 67       if (!lock_mgr->LockExclusive(txn, *rid)) {
 68         txn_mgr->Abort(txn);
 69       }
 70       for (auto indexinfo : indexes_) {
 71         indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(),
 72                                                               indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
 73                                        tmp_rid, txn);
 74         txn->GetIndexWriteSet()->emplace_back(tmp_rid, table_info_->oid_, WType::INSERT, tmp_tuple, tmp_tuple,
 75                                               indexinfo->index_oid_, exec_ctx->GetCatalog());
 76       }
 77     }
 78   }
 79   return false;
 80 }
```

对于`InsertExecutor`，其在将元组插入表后获得插入表的写锁。需要注意，当其元组来源为其他计划节点时，其来源元组与插入表的元组不是同一个元组。

```C++
 30 bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
 31   auto exec_ctx = GetExecutorContext();
 32   Transaction *txn = exec_ctx_->GetTransaction();
 33   TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
 34   LockManager *lock_mgr = exec_ctx->GetLockManager();
 35 
 36   while (child_executor_->Next(tuple, rid)) {
 37     if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
 38       if (!lock_mgr->LockExclusive(txn, *rid)) {
 39         txn_mgr->Abort(txn);
 40       }
 41     } else {
 42       if (!lock_mgr->LockUpgrade(txn, *rid)) {
 43         txn_mgr->Abort(txn);
 44       }
 45     }
 46     if (table_info_->table_->MarkDelete(*rid, txn)) {
 47       for (auto indexinfo : indexes_) {
 48         indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), i    ndexinfo->key_schema_,
 49                                                            indexinfo->index_->GetKeyAttrs()),
 50                                        *rid, txn);
 51         IndexWriteRecord iwr(*rid, table_info_->oid_, WType::DELETE, *tuple, *tuple, indexinfo->i    ndex_oid_,
 52                              exec_ctx->GetCatalog());
 53         txn->AppendIndexWriteRecord(iwr);
 54       }
 55     }
 56   }
 57   return false;
 58 }
```

对于`DeleteExecutor`和`UpdateExecutor`，在其获得子计划节点元组后，应当为该元组加读锁。需要注意的是，当事务隔离等级为`REPEATABLE_READ`时，其子计划节点拥有该元组的读锁，因此此时应该调用`LockUpgrade`升级锁，而不是获得新写锁。

## 实验结果

![figure4](E:\ubuntu\sharedoc\notes\project4_figure\figure4.png)
