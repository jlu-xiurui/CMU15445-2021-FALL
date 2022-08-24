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

